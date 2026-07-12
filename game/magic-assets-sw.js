self.addEventListener('install', event => event.waitUntil(self.skipWaiting()));
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));

self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);
  const badPath = '/game/Magic/graphics/';
  const index = url.pathname.indexOf(badPath);
  if (index < 0) return;

  const sitePrefix = url.pathname.slice(0, index);
  const filename = url.pathname.slice(index + badPath.length);
  url.pathname = `${sitePrefix}/graphics/${filename}`;

  event.respondWith(fetch(new Request(url.toString(), event.request)));
});
