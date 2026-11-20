'use strict';

const http = require('node:http');
const { randomBytes, timingSafeEqual } = require('node:crypto');

const join = '/__catnmice/join';
const status = '/__catnmice/status';
const access = '/__catnmice/access';
const sockets = { cat: ['/socket'], trashtactics: ['/api/play'], lastsignal: ['/mobile/ws', '/ws'] };

function equal(a, b) {
  const left = Buffer.from(a), right = Buffer.from(b);
  return left.length === right.length && timingSafeEqual(left, right);
}

function send(res, code, body, type = 'text/plain; charset=utf-8', headers = {}) {
  res.writeHead(code, { 'Cache-Control': 'no-store', 'Content-Type': type, 'Referrer-Policy': 'no-referrer',
    'X-Content-Type-Options': 'nosniff', ...headers });
  res.end(body);
}

function responseHeaders(gameId, req, upstream, localHost) {
  const headers = { ...upstream };
  const policy = headers['content-security-policy'], host = req.headers.host || '';
  // Last Signal explicitly lists its WebSocket origin for mobile Safari. The browser
  // connects to the gateway, though the native process generated its policy for loopback.
  if (gameId === 'lastsignal' && typeof policy === 'string' &&
      /^(?:[a-z0-9.-]+|\[[a-f0-9:]+\])(?::[0-9]{1,5})?$/i.test(host)) {
    const secure = /^[a-z0-9-]+\.trycloudflare\.com$/i.test(host) || req.socket.encrypted;
    headers['content-security-policy'] = policy.replace(' ws://' + localHost + ';',
      ' ' + (secure ? 'wss://' : 'ws://') + host + ';');
  }
  return headers;
}

// No third-party resources are loaded on the code entry page. The PIN is taken out of the URL
// before any request, including the redirect into the native game.
const pinPage = `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Join private server</title><main><h1>Join private server</h1><form><label>Four-digit code
<input id="pin" type="text" inputmode="numeric" pattern="[0-9]{4}" minlength="4" maxlength="4" required autofocus></label>
<button>Join server</button></form><p role="status"></p></main><script>
const input=document.querySelector('input'),form=document.querySelector('form'),message=document.querySelector('[role=status]');
const code=new URLSearchParams(location.hash.slice(1)).get('code');
history.replaceState(null,'',location.pathname+location.search);
async function enter(event){event.preventDefault();try{
const response=await fetch('/__catnmice/access',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code:input.value})});
if(response.ok) location.replace(location.pathname+location.search);
else {message.textContent=await response.text();input.select();}
}catch{message.textContent='The server could not be reached. Try again.'}}
form.addEventListener('submit',enter);
if(code&&/^[0-9]{4}$/.test(code)){input.value=code;form.requestSubmit();}
</script></html>`;

function snapshot(native) {
  if (!native || !['waiting', 'playing', 'finished'].includes(native.phase) ||
      !Number.isInteger(native.capacity) || native.capacity < 1 || native.capacity > 256 ||
      !Number.isInteger(native.players) || native.players < 0 || native.players > native.capacity) {
    throw Error('The game returned an invalid lobby status.');
  }
  const queued = native.queued ?? 0;
  if (!Number.isInteger(queued) || queued < 0 || queued > 1000) throw Error('Invalid queue length.');
  return { phase: native.phase, players: native.players, capacity: native.capacity, queued,
    joinable: (native.joinable ?? native.phase === 'waiting') === true &&
      native.phase !== 'finished' && native.players < native.capacity };
}

function createGateway({ adapter, gameId, hostId, name, visibility, getCode }) {
  const target = new URL(adapter.origin);
  if (target.protocol !== 'http:' || target.hostname !== '127.0.0.1' || !target.port ||
    target.username || target.password || target.pathname !== '/' || target.search || target.hash) throw Error('Expected a loopback game server.');
  const privateHost = visibility === 'code';
  const cookie = 'catnmice_managed_' + gameId;
  const secret = randomBytes(32).toString('hex');
  const transports = new Set();
  let attempts = 0, windowStart = 0;

  const getStatus = async () => ({ game: gameId, hostId, name, visibility, ...snapshot(await adapter.getStatus()) });
  const cookies = req => (req.headers.cookie || '').split(';').map(s => s.trim()).filter(Boolean);
  const admitted = req => !privateHost || cookies(req).some(s => equal(s, cookie + '=' + secret));
  function sameOrigin(req) {
    if (!req.headers.origin) return true; // Native clients can omit Origin.
    try {
      const origin = new URL(req.headers.origin);
      return ['https:', 'http:'].includes(origin.protocol) && origin.host === req.headers.host;
    } catch { return false; }
  }
  function options(req, upgrade = false) {
    // A public client cannot inject routing metadata or the gateway session into the native host.
    const headers = { ...req.headers };
    const remove = ['connection','proxy-authorization','proxy-authenticate','keep-alive','upgrade','te','trailer',
      'forwarded','via', ...(req.headers.connection || '').toLowerCase().split(',').map(s=>s.trim())];
    for (const h of Object.keys(headers)) if (remove.includes(h) || h.startsWith('x-forwarded-') || h.startsWith('cf-')) delete headers[h];
    headers.host = target.host;
    if (req.headers.origin) headers.origin = target.origin;
    const rest = cookies(req).filter(s => !s.startsWith(cookie + '=')).join('; ');
    if (rest) headers.cookie = rest;
    else delete headers.cookie;
    if (upgrade) { headers.connection = 'Upgrade'; headers.upgrade = 'websocket'; }
    return { hostname: target.hostname, port: target.port, path: req.url, method: req.method, headers };
  }
  async function grant(req, res) {
    if (!req.headers['content-type']?.startsWith('application/json')) return send(res, 415, 'Send a four-digit code.');
    if (Date.now() - windowStart >= 60_000) { windowStart = Date.now(); attempts = 0; }
    if (attempts >= 30) return send(res, 429, 'Too many attempts. Try again in one minute.');
    let body = '';
    for await (const chunk of req) {
      if (chunk.length + body.length > 128) return send(res, 413, 'Code request is too large.');
      body += chunk.toString();
    }
    let submitted;
    try { submitted = JSON.parse(body).code; } catch {}
    if (typeof submitted !== 'string' || !/^\d{4}$/.test(submitted) || !equal(submitted, getCode())) {
      attempts++;
      return send(res, 403, 'That code does not match this server.');
    }
    const secure = req.headers.host?.split(':')[0].endsWith('.trycloudflare.com') ? '; Secure' : '';
    return send(res, 204, '', 'text/plain', { 'Set-Cookie': cookie + '=' + secret + '; Path=/; HttpOnly; SameSite=Lax' + secure });
  }
  async function handle(req, res) {
    if (!req.url.startsWith('/') || req.url.startsWith('//') || req.url.includes('\\')) return send(res, 400, 'Invalid path.');
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname === status && req.method === 'GET') {
      return send(res, 200, JSON.stringify(await getStatus()), 'application/json', { 'Access-Control-Allow-Origin': '*' });
    }
    if (req.method !== 'GET' && req.method !== 'HEAD' && !sameOrigin(req)) return send(res, 403, 'Open the game on this server.');
    if (url.pathname === access && req.method === 'POST' && privateHost) return grant(req, res);
    if (url.pathname === join && req.method === 'GET') {
      if (!admitted(req)) return send(res, 200, pinPage, 'text/html; charset=utf-8', {
        'Content-Security-Policy': "default-src 'none'; script-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'"
      });
      if (!(await getStatus()).joinable) return send(res, 409, 'This match is full or in progress. Choose another server or try again after the match.');
      const dest = await adapter.joinPath();
      if (typeof dest !== 'string' || !/^\/(?!\/)/.test(dest) || /[\\\r\n]/.test(dest)) throw Error('Invalid native join path.');
      const location = new URL(dest, target);
      if (location.origin !== target.origin) throw Error('Invalid native join origin.');
      const displayName = (url.searchParams.get('name') || '').trim().slice(0,40);
      if (displayName) location.searchParams.set('name', displayName);
      return send(res, 302, '', 'text/plain', { Location: location.pathname + location.search + location.hash });
    }
    if (url.pathname.startsWith('/__catnmice/')) return send(res, 404, 'Unavailable.');
    if (!admitted(req)) return send(res, 403, 'Open the server invitation and enter its code.');
    if (gameId === 'lastsignal' && url.pathname !== '/mobile' && !url.pathname.startsWith('/mobile/'))
      return send(res, 404, 'Unavailable.');
    if (url.pathname.startsWith('/api/desktop/') ||
        (gameId === 'trashtactics' && url.pathname === '/api/lobbies' && req.method !== 'GET')) return send(res, 404, 'Unavailable.');
    const upstream = http.request(options(req), reply => {
      res.writeHead(reply.statusCode, responseHeaders(gameId, req, reply.headers, target.host));
      reply.once('error', () => res.destroy());
      reply.pipe(res);
    });
    upstream.once('error', () => {
      if (!res.headersSent && !res.destroyed) send(res, 502, 'The local game server could not be reached.');
      else res.destroy();
    });
    req.once('aborted', () => upstream.destroy());
    res.once('close', () => { if (!res.writableFinished) upstream.destroy(); });
    req.pipe(upstream);
  }
  const server = http.createServer((req, res) => {
    handle(req, res).catch(() => {
      if (!res.headersSent && !res.destroyed) send(res, 503, 'The game is preparing its lobby. Try again shortly.');
      else res.destroy();
    });
  });
  server.on('connection', socket => {
    transports.add(socket);
    socket.once('close', () => transports.delete(socket));
  });
  server.on('upgrade', (req, client, head) => {
    if (!sockets[gameId]?.includes(req.url.split('?')[0]) ||
        req.headers.upgrade?.toLowerCase() !== 'websocket' || !sameOrigin(req) || !admitted(req)) {
      return client.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
    }
    const upstream = http.request(options(req, true));
    client.once('error', () => upstream.destroy());
    client.once('close', () => upstream.destroy());
    upstream.once('error', () => client.destroy());
    upstream.once('response', reply => { reply.resume(); client.end('HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n'); });
    upstream.once('upgrade', (reply, peer, first) => {
      peer.once('error', () => client.destroy());
      peer.once('close', () => client.destroy());
      client.once('close', () => peer.destroy());
      client.write('HTTP/1.1 101 Switching Protocols\r\n');
      for (let i = 0; i < reply.rawHeaders.length; i += 2) client.write(reply.rawHeaders[i] + ': ' + reply.rawHeaders[i + 1] + '\r\n');
      client.write('\r\n');
      if (first.length) client.write(first);
      if (head.length) peer.write(head);
      peer.pipe(client).pipe(peer);
    });
    upstream.end();
  });
  return { getStatus, server, close() {
    for (const socket of transports) socket.destroy();
    return new Promise(resolve => server.close(resolve));
  } };
}

module.exports = { createGateway, snapshot, join, status, responseHeaders };
