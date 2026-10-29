// Cache only public installation icons. Private pages, API data and artifacts stay on the network.
const CACHE = 'experimenttracker-shell-v2';
const ICONS = ['/icon.svg', '/icon-192.png', '/icon-512.png', '/icon-maskable-512.png', '/apple-touch-icon.png'];
self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE).then(cache => cache.addAll(ICONS)));
  self.skipWaiting();
});
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(k => k.startsWith('experimenttracker-shell-') && k !== CACHE)
        .map(k => caches.delete(k))
    ))
  );
  self.clients.claim();
});
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin || event.request.method !== 'GET' || url.search) return;
  if (!ICONS.includes(url.pathname)) return;
  event.respondWith(caches.match(event.request).then(cached => cached || fetch(event.request)));
});

// Push payloads contain display text and an authenticated deep link, never credentials.
self.addEventListener('push', event => {
  event.waitUntil((async () => {
    let data;
    try { data = event.data?.json(); } catch { /* Show a visible fallback for malformed messages. */ }
    const id = typeof data?.experiment_id === 'string' && /^[a-f0-9-]{36}$/.test(data.experiment_id) ? data.experiment_id : null;
    const tag = id ? `experiment:${id}` : 'experimenttracker';
    let title = String(data?.title || 'ExperimentTracker').slice(0, 100);
    let body = String(data?.body || 'An experiment update is available.').slice(0, 240);
    let revision = Number.isSafeInteger(data?.revision) ? data.revision : -1;
    let url = '/';
    try {
      const link = new URL(data?.url || '/', self.location.origin);
      if (link.origin === self.location.origin && link.pathname === '/') url = link.pathname + link.search;
    } catch { /* Keep navigation on this app. */ }
    let alert = Boolean(data?.alert);
    // Delayed messages must not replace a newer visible state. Redisplay that
    // state quietly; each push still fulfills the userVisibleOnly promise.
    const visible = await self.registration.getNotifications({ tag });
    const newer = visible.find(n => Number(n.data?.revision) >= revision);
    if (newer) {
      title = newer.title; body = newer.body; revision = newer.data.revision;
      url = newer.data.url || '/'; alert = false;
    }
    await self.registration.showNotification(title, {
      body, tag, icon: '/icon-192.png', badge: '/icon-192.png',
      renotify: alert, silent: !alert,
      timestamp: typeof data?.timestamp === 'number' ? data.timestamp : Date.now(),
      data: { url, revision, experiment_id: id },
      actions: [{ action: 'open', title: 'Open experiment' }],
    });
  })());
});
self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil((async () => {
    const url = new URL(event.notification.data?.url || '/', self.location.origin);
    if (url.origin !== self.location.origin || url.pathname !== '/') return;
    for (const client of await self.clients.matchAll({ type: 'window', includeUncontrolled: true })) {
      if (new URL(client.url).origin !== self.location.origin) continue;
      await client.navigate(url.href);
      await client.focus();
      return;
    }
    await self.clients.openWindow(url.href);
  })());
});
