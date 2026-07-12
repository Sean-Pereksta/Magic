self.addEventListener('install',event=>event.waitUntil(self.skipWaiting()));
self.addEventListener('activate',event=>event.waitUntil(self.clients.claim()));
self.addEventListener('fetch',event=>{
  const url=new URL(event.request.url);
  const bad='/game/Magic/graphics/';
  if(!url.pathname.includes(bad)) return;
  url.pathname=url.pathname.replace(bad,'/graphics/');
  event.respondWith(fetch(new Request(url.toString(),event.request)));
});
