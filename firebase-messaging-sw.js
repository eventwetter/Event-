/* Service Worker für die Regen-Frühwarnung per Push (Firebase Cloud Messaging).
 * Muss im Wurzelverzeichnis der Seite liegen (gleiche Ebene wie index.html),
 * damit er die ganze Domain abdecken darf.
 *
 * Zeigt Benachrichtigungen NUR selbst an (data-only Nachrichten vom Backend,
 * siehe check_rain.py) - enthielte die Nachricht zusätzlich einen
 * "notification"-Block, würde das FCM-SDK sie zusätzlich selbst anzeigen und
 * jede Warnung erschiene doppelt.
 */

importScripts('https://www.gstatic.com/firebasejs/10.13.2/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.13.2/firebase-messaging-compat.js');

firebase.initializeApp({
  apiKey: "AIzaSyBOOMI7UkBsT5jNuY3KOcBcfDiU9os8XMs",
  authDomain: "event-wetter.firebaseapp.com",
  projectId: "event-wetter",
  storageBucket: "event-wetter.firebasestorage.app",
  messagingSenderId: "711746288105",
  appId: "1:711746288105:web:a8174a792b894098493378"
});

const messaging = firebase.messaging();

messaging.onBackgroundMessage((payload) => {
  const data = payload.data || {};
  const title = data.title || 'Event Wetter';
  const options = {
    body: data.body || '',
    icon: './applogo.png',
    badge: './applogo.png',
    tag: data.tag || 'event-rain',
    data: { click_url: data.click_url || './index.html' }
  };
  self.registration.showNotification(title, options);
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.click_url) || './index.html';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if ('focus' in client) return client.focus();
      }
      if (clients.openWindow) return clients.openWindow(url);
    })
  );
});
