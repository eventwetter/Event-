import os
import json
import math
import time
import requests
from datetime import datetime, timezone, timedelta

import firebase_admin
from firebase_admin import credentials, firestore, messaging
import pytz

# ==============================================================================
#  EVENT WETTER  --  REGEN-FRÜHWARNUNG (Cronjob-Backend)
# ------------------------------------------------------------------------------
#  Prüft regelmäßig den GeoSphere-Austria-Nowcast im 20-km-Umkreis jedes
#  Events, für das ein Team-Mitglied den Regen-Push aktiviert hat, und
#  schickt bei erkanntem Niederschlag eine Push-Benachrichtigung mit
#  Entfernung, voraussichtlicher Eintreffzeit und einem Hinweis auf hohe
#  Intensität.
#
#  Die Erkennungslogik (Raster-Analyse mit Sternmuster-Fallback, Zellzug per
#  linearer Regression über die Onset-Zeiten pro Sektor, Alarm-Zustands-
#  maschine gegen Push-Spam) ist 1:1 von der Bergtouren-Wetter-App
#  übernommen (siehe deren check_weather.py) und hier auf einen ortsfesten
#  Punkt vereinfacht: kein Track, keine Tourtypen, keine Höhenzonen, kein
#  Gewitter-/Wind-Scoring - nur "kommt hier gleich Regen an?".
# ==============================================================================

if not firebase_admin._apps:
    cred_json = os.environ.get("FIREBASE_CREDENTIALS")
    if cred_json:
        cred = credentials.Certificate(json.loads(cred_json))
        firebase_admin.initialize_app(cred)
    else:
        firebase_admin.initialize_app()

db = firestore.client()
LOCAL_TZ = pytz.timezone('Europe/Vienna')

SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'EventWetter/1.0 (rain early warning)'})

GEOSPHERE_BASE = "https://dataset.api.hub.geosphere.at/v1"

RAIN_THRESHOLD = 0.02
MIN_SECTOR_KM = 2.0
MAX_SECTOR_KM = 20.0          # deckt sich mit dem 20-km-Radius im Live-Nowcast der App
HIGH_INTENSITY_MM = 4.0       # ab hier "hohe Intensität" in der Push-Meldung

STAGE_ETA_CLOSE_MIN = 35       # "steht unmittelbar bevor"
STAGE_ETA_MID_MIN = 80         # "zieht heran"

STAGE_RANK = {'stable': 0, 'early_warning': 1, 'update_mid': 2, 'update_close': 3, 'arrival': 4}
STAGE_COOLDOWN_S = {
    'early_warning': 90 * 60,
    'update_mid': 45 * 60,
    'update_close': 20 * 60,
    'arrival': 60 * 60,
}
ESCALATION_MIN_GAP_S = 5 * 60  # eine echte Verschärfung darf die Sperrfrist verkürzen


def http_json(url, timeout=15, retries=2):
    """Ein Request mit kurzem Retry. Gibt None statt einer Exception zurück, damit ein
    einzelner API-Aussetzer nicht den kompletten Lauf abbricht."""
    for attempt in range(retries + 1):
        try:
            res = SESSION.get(url, timeout=timeout)
            if res.status_code >= 500 and attempt < retries:
                time.sleep(1.2 * (attempt + 1))
                continue
            res.raise_for_status()
            return res.json()
        except Exception as e:
            if attempt >= retries:
                print(f"HTTP-Fehler ({url.split('?')[0]}): {e}")
                return None
            time.sleep(1.2 * (attempt + 1))
    return None


def calc_distance_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def direction_name(name):
    return {"N": "Norden", "NO": "Nordosten", "O": "Osten", "SO": "Südosten", "S": "Süden",
            "SW": "Südwesten", "W": "Westen", "NW": "Nordwesten"}.get(name, name)


def get_rain_description(amount):
    if amount < 0.2:
        return "Nieselregen"
    if amount < 2.0:
        return "leichter Regen"
    if amount < HIGH_INTENSITY_MM:
        return "moderater Regen"
    return "starker Regen"


def parse_iso_utc(value):
    """Robustes ISO-Parsing inklusive 'Z'-Suffix."""
    if not value:
        return None
    try:
        txt = str(value).strip()
        if txt.endswith('Z'):
            txt = txt[:-1] + '+00:00'
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# ------------------------------------------------------------------------------
# GeoSphere-Parser: Die API liefert die Zeitachse einmal auf oberster Ebene der
# FeatureCollection ("timestamps"), nicht zwingend pro Feature - beide Formen
# werden abgedeckt (der Endpunkt ist offiziell "prerelease" und kann sich noch
# ändern).
# ------------------------------------------------------------------------------
def gs_timestamps(payload, feature=None):
    if not isinstance(payload, dict):
        return []
    for key in ('timestamps', 'time', 'times'):
        v = payload.get(key)
        if isinstance(v, list) and v:
            return v
    if isinstance(feature, dict):
        props = feature.get('properties') or {}
        for key in ('timestamps', 'time', 'times'):
            v = props.get(key)
            if isinstance(v, list) and v:
                return v
        for pdata in (props.get('parameters') or {}).values():
            if isinstance(pdata, dict):
                for key in ('timestamps', 'time', 'times'):
                    v = pdata.get(key)
                    if isinstance(v, list) and v:
                        return v
    return []


def gs_values(feature, param='rr'):
    if not isinstance(feature, dict):
        return []
    params = (feature.get('properties') or {}).get('parameters') or {}
    entry = params.get(param) or params.get(param.upper()) or params.get(param.lower())
    if entry is None and len(params) == 1:
        entry = next(iter(params.values()))
    if not isinstance(entry, dict):
        return []
    data = entry.get('data')
    if isinstance(data, list) and data and isinstance(data[0], list):
        return []  # verschachtelte Grid-Matrix, hier nicht als Punktserie nutzbar
    return data or []


def gs_point(feature):
    geom = (feature or {}).get('geometry') or {}
    coords = geom.get('coordinates')
    if isinstance(coords, list) and len(coords) >= 2:
        try:
            return float(coords[1]), float(coords[0])  # lat, lon
        except (TypeError, ValueError):
            return None, None
    return None, None


def find_onset(times, values, threshold=RAIN_THRESHOLD):
    if not times or not values:
        return None
    n = min(len(times), len(values))
    for i in range(n):
        try:
            v = float(values[i] or 0)
        except (TypeError, ValueError):
            continue
        try:
            next_v = float(values[i + 1] or 0) if i + 1 < n else v
        except (TypeError, ValueError):
            next_v = v
        if v >= threshold and (next_v >= threshold or i == n - 1):
            if not times[i]:
                continue
            return {"time": times[i], "amount": v}
    return None


def bearing_to_sector(lat1, lon1, lat2, lon2):
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    x = math.sin(dlambda) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    bearing = (math.degrees(math.atan2(x, y)) + 360) % 360
    dirs = ['N', 'NO', 'O', 'SO', 'S', 'SW', 'W', 'NW']
    return dirs[round(bearing / 45) % 8]


def stage_from_eta(eta_minutes):
    if eta_minutes <= STAGE_ETA_CLOSE_MIN:
        return "update_close"
    if eta_minutes <= STAGE_ETA_MID_MIN:
        return "update_mid"
    return "early_warning"


# ------------------------------------------------------------------------------
# Primärer Weg: 1 Grid-Request über den ganzen 20-km-Umkreis, Zellzug per
# linearer Regression der Onset-Zeiten pro Himmelsrichtung.
# ------------------------------------------------------------------------------
def fetch_precip_grid(lat, lon, half_extent_km):
    dlat = half_extent_km / 111.32
    dlon = half_extent_km / (111.32 * max(.2, math.cos(math.radians(lat))))
    south, north = lat - dlat, lat + dlat
    west, east = lon - dlon, lon + dlon
    url = (f"{GEOSPHERE_BASE}/grid/forecast/nowcast-v1-15min-1km"
           f"?parameters=rr&bbox={south:.5f},{west:.5f},{north:.5f},{east:.5f}"
           f"&output_format=geojson")
    return http_json(url, timeout=20)


def analyze_precip_raster(lat, lon):
    payload = fetch_precip_grid(lat, lon, half_extent_km=MAX_SECTOR_KM)
    if not payload:
        return None
    features = payload.get('features') or []
    if not features:
        return None

    base_times = gs_timestamps(payload)
    cells = []
    for feat in features:
        f_lat, f_lon = gs_point(feat)
        if f_lat is None:
            continue
        times = base_times or gs_timestamps(payload, feat)
        vals = gs_values(feat, 'rr')
        if not times or not vals:
            continue
        cells.append({
            "lat": f_lat, "lon": f_lon,
            "dist": calc_distance_km(lat, lon, f_lat, f_lon),
            "onset": find_onset(times, vals)
        })
    if not cells:
        return None

    # 1) Niederschlag direkt am Event-Ort?
    center_cell = min(cells, key=lambda c: c["dist"])
    if center_cell["dist"] < 1.5 and center_cell["onset"]:
        o = center_cell["onset"]
        o_dt = parse_iso_utc(o["time"])
        eta_min = (o_dt - datetime.now(timezone.utc)).total_seconds() / 60 if o_dt else 0
        if eta_min <= 10:
            return {"stage": "arrival", "distance_km": 0, "time": o["time"],
                    "direction": None, "amount": o["amount"]}
        return {"stage": stage_from_eta(eta_min), "distance_km": 0, "time": o["time"],
                "direction": None, "amount": o["amount"]}

    # 2) Pro Sektor: Onset-Zeit über Entfernung linear fitten -> Geschwindigkeit & ETA
    sectors = {}
    for c in cells:
        if not (MIN_SECTOR_KM <= c["dist"] <= MAX_SECTOR_KM) or not c["onset"]:
            continue
        sec = bearing_to_sector(lat, lon, c["lat"], c["lon"])
        t_dt = parse_iso_utc(c["onset"]["time"])
        if not t_dt:
            continue
        sectors.setdefault(sec, []).append((c["dist"], t_dt.timestamp(), c["onset"].get("amount", 0)))

    now_epoch = datetime.now(timezone.utc).timestamp()
    candidates = []
    for sec, pts in sectors.items():
        if len(pts) < 3:
            continue
        n = len(pts)
        sum_d = sum(p[0] for p in pts)
        sum_t = sum(p[1] for p in pts)
        sum_dt = sum(p[0] * p[1] for p in pts)
        sum_dd = sum(p[0] * p[0] for p in pts)
        denom = n * sum_dd - sum_d * sum_d
        if abs(denom) < 1e-6:
            continue
        a = (n * sum_dt - sum_d * sum_t) / denom   # Sekunden pro km
        b = (sum_t - a * sum_d) / n                # ETA (epoch) bei Distanz 0
        if a >= 0:
            continue  # Onset muss mit steigender Entfernung frueher liegen
        speed_kmh = -3600.0 / a
        if not (10 <= speed_kmh <= 120):
            continue
        if b < now_epoch - 300 or b > now_epoch + 3 * 3600:
            continue

        nearest_dist = min(p[0] for p in pts)
        amount = max(p[2] for p in pts)
        stage = stage_from_eta((b - now_epoch) / 60.0)

        if max(abs(p[1] - (a * p[0] + b)) for p in pts) > 40 * 60:
            continue  # Feld zu zerrissen für ein brauchbares Frontmodell

        candidates.append({
            "stage": stage, "distance_km": nearest_dist,
            "time": datetime.fromtimestamp(b, timezone.utc).isoformat(),
            "direction": sec, "amount": amount, "speed": speed_kmh
        })

    if candidates:
        candidates.sort(key=lambda x: x["time"])
        return candidates[0]
    return None


LIVE_DIRS = [
    {"name": "N", "lat": 1, "lon": 0}, {"name": "NO", "lat": .7071, "lon": .7071},
    {"name": "O", "lat": 0, "lon": 1}, {"name": "SO", "lat": -.7071, "lon": .7071},
    {"name": "S", "lat": -1, "lon": 0}, {"name": "SW", "lat": -.7071, "lon": -.7071},
    {"name": "W", "lat": 0, "lon": -1}, {"name": "NW", "lat": .7071, "lon": -.7071}
]


def live_distance_point(lat, lon, dir_obj, km):
    d_lat = (km / 111.32) * dir_obj["lat"]
    d_lon = (km / (111.32 * max(.2, math.cos(lat * math.pi / 180)))) * dir_obj["lon"]
    return lat + d_lat, lon + d_lon


def analyze_precip_legacy(lat, lon):
    """Punkt-Fallback (Sternmuster), falls der Raster-Endpunkt ausfällt."""
    center_url = (f"{GEOSPHERE_BASE}/timeseries/forecast/nowcast-v1-15min-1km"
                  f"?lat_lon={lat:.5f},{lon:.5f}&parameters=rr&forecast_offset=0&output_format=geojson")
    c_res = http_json(center_url, timeout=12)
    if c_res:
        c_feats = c_res.get('features') or []
        if c_feats:
            c_times = gs_timestamps(c_res, c_feats[0])
            c_vals = gs_values(c_feats[0], 'rr')
            center_onset = find_onset(c_times, c_vals)
            if center_onset:
                return {"stage": "arrival", "distance_km": 0, "time": center_onset["time"],
                        "direction": None, "amount": center_onset["amount"]}

    points = []
    for d in LIVE_DIRS:
        for km in [4, 8, 12, 16, 20]:
            p_lat, p_lon = live_distance_point(lat, lon, d, km)
            points.append({"dir": d["name"], "km": km, "lat": p_lat, "lon": p_lon})

    pts_query = "&".join([f"lat_lon={p['lat']:.5f},{p['lon']:.5f}" for p in points])
    pts_url = (f"{GEOSPHERE_BASE}/timeseries/forecast/nowcast-v1-15min-1km"
               f"?{pts_query}&parameters=rr&forecast_offset=0&output_format=geojson")
    p_res = http_json(pts_url, timeout=20)
    if not p_res:
        return None

    features = p_res.get('features') or []
    base_times = gs_timestamps(p_res)
    grouped = {}
    for idx, p in enumerate(points):
        feat = features[idx] if idx < len(features) else {}
        times = base_times or gs_timestamps(p_res, feat)
        grouped.setdefault(p["dir"], {})[p["km"]] = {"times": times, "vals": gs_values(feat, 'rr')}

    now_epoch = datetime.now(timezone.utc).timestamp()
    candidates = []
    for dir_name, items in grouped.items():
        for outer_km, inner_km in [(20, 16), (16, 12), (12, 4)]:
            if outer_km not in items or inner_km not in items:
                continue
            o_out = find_onset(items[outer_km]["times"], items[outer_km]["vals"])
            o_in = find_onset(items[inner_km]["times"], items[inner_km]["vals"])
            if not o_out or not o_in:
                continue
            t_out_dt = parse_iso_utc(o_out["time"])
            t_in_dt = parse_iso_utc(o_in["time"])
            if not t_out_dt or not t_in_dt:
                continue
            dt = t_in_dt.timestamp() - t_out_dt.timestamp()
            if dt <= 0:
                continue
            speed_kmh = (outer_km - inner_km) / (dt / 3600)
            if not (10 <= speed_kmh <= 120):
                continue
            eta = t_in_dt.timestamp() + (inner_km / speed_kmh) * 3600
            if eta < now_epoch - 300 or eta > now_epoch + 3 * 3600:
                continue
            candidates.append({
                "stage": stage_from_eta((eta - now_epoch) / 60.0),
                "distance_km": inner_km,
                "time": datetime.fromtimestamp(eta, timezone.utc).isoformat(),
                "direction": dir_name,
                "amount": max(o_out.get("amount", 0), o_in.get("amount", 0)),
                "speed": speed_kmh
            })
    if candidates:
        candidates.sort(key=lambda x: x["time"])
        return candidates[0]
    return None


def analyze_precip(lat, lon):
    front = None
    try:
        front = analyze_precip_raster(lat, lon)
    except Exception as e:
        print(f"    Grid-Nowcast fehlgeschlagen, Fallback aktiv: {e}")
    if front is None:
        try:
            front = analyze_precip_legacy(lat, lon)
        except Exception as e:
            print(f"    Legacy-Nowcast fehlgeschlagen: {e}")
    return front


# ------------------------------------------------------------------------------
# Alarm-Zustandsmaschine gegen Push-Spam (1:1 aus der Bergtouren-App)
# ------------------------------------------------------------------------------
def build_alert_key(stage, amount, direction, eta_minutes):
    """Stabiler Vergleichsschlüssel OHNE Uhrzeit, damit normale Prognose-
    schwankungen keine neue Meldung auslösen."""
    intensity = 'niesel' if amount < 0.2 else ('leicht' if amount < 2.0 else 'stark')
    eta_bucket = int(max(0, eta_minutes) // 30)   # 30-Minuten-Raster
    return f"{stage}|{intensity}|{direction or '-'}|{eta_bucket}"


def should_send_alert(stage, key, last_key, last_state, last_ts, now_utc):
    if not key:
        return False, "kein Alarm"
    if key == last_key:
        return False, f"unverändert ({key})"

    rank_now = STAGE_RANK.get(stage, 0)
    rank_last = STAGE_RANK.get(last_state, 0)
    seit = (now_utc - last_ts).total_seconds() if last_ts else None

    if seit is None:
        return True, "erste Meldung"

    if rank_now > rank_last:
        if seit >= ESCALATION_MIN_GAP_S:
            return True, f"Verschärfung {last_state} -> {stage}"
        return False, f"Verschärfung, aber erst {int(seit / 60)} Min. seit letzter Meldung"

    cooldown = STAGE_COOLDOWN_S.get(stage, 45 * 60)
    if seit >= cooldown:
        return True, f"Sperrfrist abgelaufen ({int(seit / 60)} Min.)"
    return False, f"Sperrfrist läuft ({int(seit / 60)}/{cooldown // 60} Min., Stufe {stage})"


def is_dead_token(err):
    txt = str(err).lower()
    return any(k in txt for k in ["unregistered", "not found", "registration-token-not-registered",
                                  "invalid-registration-token"])


# 2. High-Priority Push (data-only, damit der Service Worker sie genau EINMAL
# anzeigt - siehe Kommentar in firebase-messaging-sw.js).
def send_push(title, body, token, tag='event-rain'):
    msg = messaging.Message(
        data={'title': title, 'body': body, 'tag': tag, 'click_url': './index.html'},
        token=token,
        android=messaging.AndroidConfig(priority='high', ttl=timedelta(hours=2), collapse_key=tag),
        webpush=messaging.WebpushConfig(
            headers={'Urgency': 'high', 'TTL': '7200'},
            data={'title': title, 'body': body, 'tag': tag, 'click_url': './index.html'}
        )
    )
    return messaging.send(msg)


def parse_event_window(sub):
    """Start-/Endzeitpunkt des Events als tz-aware datetimes (Europe/Vienna),
    inkl. Übernacht-Rollover - identisch zur Logik im Frontend (runCheck())."""
    try:
        datum = sub.get('datum')
        von = sub.get('von') or '00:00'
        bis = sub.get('bis') or '23:59'
        enddatum = sub.get('enddatum')
        start_dt = LOCAL_TZ.localize(datetime.fromisoformat(f"{datum}T{von}"))
        end_datum = enddatum or datum
        if not enddatum and bis <= von:
            end_naive = datetime.fromisoformat(f"{datum}T{bis}") + timedelta(days=1)
        else:
            end_naive = datetime.fromisoformat(f"{end_datum}T{bis}")
        end_dt = LOCAL_TZ.localize(end_naive)
        return start_dt, end_dt
    except Exception:
        return None, None


def run():
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(LOCAL_TZ)
    print(f"Lauf gestartet: {now_local:%d.%m.%Y %H:%M %Z}")

    stat = {'gesamt': 0, 'aktiv': 0, 'pushes': 0, 'fehler': 0}

    for doc in db.collection('event_subscriptions').stream():
        sub = doc.to_dict() or {}
        stat['gesamt'] += 1
        try:
            token = sub.get('token')
            lat, lon = sub.get('lat'), sub.get('lon')
            if sub.get('finished') or not token or lat is None or lon is None:
                continue

            start_dt, end_dt = parse_event_window(sub)
            if not start_dt or not end_dt:
                print(f"  {doc.id}: Zeitfenster nicht lesbar - übersprungen.")
                continue

            if now_local > end_dt:
                db.collection('event_subscriptions').document(doc.id).update({'finished': True})
                continue
            # Der Nowcast reicht ohnehin nur ca. 2-3h voraus - frueher ist er nicht relevant.
            if now_local < start_dt - timedelta(hours=3):
                continue

            lat, lon = float(lat), float(lon)
            last_alert_key = sub.get('last_alert_key', '')
            last_state = sub.get('last_weather_state', 'stable')
            last_alert_ts = parse_iso_utc(sub.get('last_alert_ts'))
            ort = sub.get('ortName') or 'dem Event-Ort'

            stat['aktiv'] += 1
            front = analyze_precip(lat, lon)

            # Relevanzfilter: schwacher Niesel weit in der Zukunft ist Rauschen,
            # keine Information.
            if front and front.get('stage') != 'arrival':
                arr = parse_iso_utc(front["time"])
                mins = (arr - now_utc).total_seconds() / 60 if arr else 0
                amt = front.get("amount", 0)
                if amt < 0.2 and mins > 60:
                    front = None
                elif mins > 180:
                    front = None

            update_data = {}
            if front:
                stage = front['stage']
                amount = front.get('amount', 0)
                dist_km = int(round(front.get('distance_km', 0)))
                direction = front.get('direction')
                arr_dt = parse_iso_utc(front['time'])
                mins_left = max(0, int((arr_dt - now_utc).total_seconds() / 60)) if arr_dt else 0
                arr_local = arr_dt.astimezone(LOCAL_TZ) if arr_dt else now_local
                clock_txt = arr_local.strftime("%H:%M") + " Uhr"
                rel_txt = ("unmittelbar" if mins_left <= 0 else
                           (f"in ca. {mins_left} Min." if mins_left < 60
                            else f"in ca. {mins_left / 60.0:.1f}".replace('.', ',') + " h"))
                dir_txt = f" aus {direction_name(direction)}" if direction else ""
                rain_desc = get_rain_description(amount)
                high = amount >= HIGH_INTENSITY_MM

                if stage == 'arrival':
                    emoji = '⚠️' if high else '🌧️'
                    title = f"{emoji} Niederschlag am Eventort – {ort}"
                    body = f"{rain_desc} hat den Ort jetzt erreicht ({clock_txt})."
                else:
                    emoji = '⚠️' if high else '🌦️'
                    title = f"{emoji} Niederschlag nähert sich – {ort}"
                    body = (f"{rain_desc}{dir_txt}, noch ca. {dist_km} km entfernt. "
                            f"Voraussichtliches Eintreffen: {clock_txt} ({rel_txt}).")
                if high:
                    body += " Hohe Intensität erwartet."

                alert_key = build_alert_key(stage, amount, direction, mins_left)
                send_it, grund = should_send_alert(stage, alert_key, last_alert_key,
                                                    last_state, last_alert_ts, now_utc)

                print(f"  {doc.id}: {ort} | {stage} | {dist_km} km | {clock_txt} | "
                      f"{amount:.2f} mm | senden={'ja' if send_it else 'nein'} ({grund})")

                update_data['last_weather_state'] = stage
                if send_it:
                    try:
                        send_push(title, body, token)
                        stat['pushes'] += 1
                        update_data['last_alert_key'] = alert_key
                        update_data['last_alert_ts'] = now_utc.isoformat()
                        update_data['last_alert_title'] = title
                    except Exception as fe:
                        print(f"    Push-Fehler: {fe}")
                        update_data.pop('last_weather_state', None)
                        if is_dead_token(fe):
                            print(f"    Token ungültig - wird entfernt ({doc.id}).")
                            db.collection('event_subscriptions').document(doc.id).update({'token': None})
                            continue
            else:
                if last_state != 'stable':
                    update_data['last_weather_state'] = 'stable'
                print(f"  {doc.id}: {ort} | kein Niederschlag im 20-km-Umkreis")

            if update_data:
                db.collection('event_subscriptions').document(doc.id).update(update_data)

        except Exception as e:
            stat['fehler'] += 1
            print(f"DEBUG Fehler bei {doc.id}: {e}")

    print(f"Lauf beendet: {stat['gesamt']} Abos, {stat['aktiv']} aktiv geprüft, "
          f"{stat['pushes']} Pushes, {stat['fehler']} Fehler.")
    if stat['gesamt'] == 0:
        print("Hinweis: Keine Abos vorhanden - noch niemand hat den Regen-Push aktiviert.")


if __name__ == "__main__":
    run()
