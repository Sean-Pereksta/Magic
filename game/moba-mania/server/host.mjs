import { randomBytes, randomInt, randomUUID } from 'node:crypto';
import { createReadStream, existsSync } from 'node:fs';
import { stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { basename, extname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { HEROES } from '../data.mjs';
import { OnlineMatch } from '../online-match.mjs';

const socketPath = '/api/moba/online';
const joinPath = '/?online=1&hosted=1';
const slotsPerTeam = 3;
const capacity = slotsPerTeam * 2;
const observerLimitMs = 120_000;
const roundSummaryMs = 18_000;
const unoccupiedMatchMs = 120_000;
const maxPeers = 32;
const heroIds = Object.keys(HEROES);
const roleOrder = ['tank', 'damage', 'support', 'control'];
const defaultHero = 'arc';
const runtimeRoot = fileURLToPath(new URL('../', import.meta.url));
const mime = {
  '.html': 'text/html; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.png': 'image/png', '.svg': 'image/svg+xml', '.webp': 'image/webp', '.woff2': 'font/woff2',
  '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg'
};

function serve(packRoot) {
  const packagedEntry = resolve(packRoot, 'index.html');
  const entry = existsSync(packagedEntry) ? packagedEntry : resolve(packRoot, 'CatLanes.html');
  const runtime = resolve(packRoot, 'moba-mania');
  return async function serveGame(request, response) {
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      response.writeHead(405, { Allow: 'GET, HEAD' }).end();
      return;
    }
    let pathname;
    try { pathname = decodeURIComponent(new URL(request.url, 'http://localhost').pathname); }
    catch { response.writeHead(400).end(); return; }
    const isEntry = ['/', '/index.html', '/CatLanes.html', '/game/CatLanes.html'].includes(pathname);
    const isRuntime = /^\/(?:game\/)?moba-mania\/[A-Za-z0-9_-]+\.(?:mjs|js|css|png|svg|webp|woff2|mp3|ogg)$/.test(pathname);
    const isHubSignal = pathname === '/hub/play-signal.mjs';
    const isDirectory = pathname === '/shared/managed-servers.js';
    const extra = isHubSignal || isDirectory ? pathname.slice(1) : null;
    const file = isEntry ? entry : isRuntime ? resolve(runtime, basename(pathname)) :
      extra && resolve(existsSync(resolve(packRoot, extra)) ? packRoot : packRoot + '/..', extra);
    if (!file) { response.writeHead(404).end(); return; }
    try {
      const info = await stat(file);
      if (!info.isFile()) { response.writeHead(404).end(); return; }
      response.writeHead(200, {
        'Content-Type': mime[extname(file)], 'Content-Length': info.size,
        'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer'
      });
      if (request.method === 'HEAD') response.end();
      else createReadStream(file).on('error', () => response.destroy()).pipe(response);
    } catch { response.writeHead(404).end(); }
  };
}

function normalizeName(value, fallback, limit = 24) {
  return typeof value === 'string' ? value.replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, limit) || fallback : fallback;
}

function validHero(hero) { return typeof hero === 'string' && Object.hasOwn(HEROES, hero); }

function sameOrigin(request) {
  if (!request.headers.origin) return true;
  try {
    const origin = new URL(request.headers.origin);
    return ['http:', 'https:'].includes(origin.protocol) && origin.host === request.headers.host;
  } catch { return false; }
}

function send(peer, message) {
  if (peer.readyState !== 1) return;
  if (peer.bufferedAmount > 1024 * 1024) { peer.terminate(); return; }
  peer.send(JSON.stringify(message));
}

function sendError(peer, code, message) { send(peer, { type: 'error', code, message }); }

function roleOf(hero) { return HEROES[hero]?.specialty?.id || 'damage'; }

function draftTeam(seats) {
  const selected = new Set(seats.filter(Boolean).map(seat => seat.hero));
  const roles = seats.filter(Boolean).map(seat => roleOf(seat.hero));
  return seats.map(seat => {
    if (seat) return { playerId: seat.playerId, hero: seat.hero, name: seat.name };
    const unused = heroIds.filter(hero => !selected.has(hero));
    const needed = roleOrder.find(role => !roles.includes(role) && unused.some(hero => roleOf(hero) === role));
    const candidates = needed ? unused.filter(hero => roleOf(hero) === needed) : unused;
    const hero = candidates.length ? candidates[randomInt(candidates.length)] : defaultHero;
    selected.add(hero);
    roles.push(roleOf(hero));
    return { hero };
  });
}

export function createMobaServer({ packRoot = resolve(runtimeRoot, '..'), name } = {}) {
  const serverName = normalizeName(name, 'MOBA Mania', 40);
  const server = createServer(serve(packRoot));
  const sockets = new WebSocketServer({ noServer: true, maxPayload: 8192, perMessageDeflate: {
    serverNoContextTakeover: true, clientNoContextTakeover: true, threshold: 1024,
    concurrencyLimit: capacity, zlibDeflateOptions: { level: 3 }
  } });
  const players = new Map();
  const seats = Array.from({ length: 2 }, () => Array(slotsPerTeam).fill(null));
  const roomId = randomUUID();
  let phase = 'waiting', leader = null, match = null, roster = null, matchId = null;
  let nextRoundAt = 0, emptySince = 0, previousStep = performance.now(), snapshotAt = 0;
  let closePromise, ticker, heartbeat;

  function getStatus() {
    const connected = seats.flat().filter(Boolean).length;
    return { phase, players: connected, capacity, queued: 0, joinable: phase !== 'finished' && connected < capacity };
  }

  function lobby() {
    const teams = seats.map((teamSeats, team) => teamSeats.map((player, slot) => {
      if (player) return { playerId: player.playerId, name: player.name, team, slot, hero: player.hero, connected: true, bot: false };
      const hero = roster?.[team][slot].hero ?? null;
      return { name: hero ? `${HEROES[hero].name} (Bot)` : 'Open slot', team, slot, hero, bot: true, connected: false };
    }));
    const status = getStatus();
    return { id: roomId, name: serverName, phase, hostId: leader?.playerId ?? null, matchId, teams,
      players: teams.flat().filter(seat => !seat.bot), capacity, joinable: status.joinable };
  }

  function broadcastLobby() {
    const message = { type: 'lobby', state: lobby() };
    for (const peer of sockets.clients) send(peer, message);
  }

  function publishSnapshot(player) {
    if (!match || !player.peer) return;
    const state = match.snapshot(player.playerId);
    if (state) send(player.peer, { type: 'snapshot', state, matchId });
  }

  function openSeat(team, slot) {
    if (team !== undefined) {
      const seat = slot ?? seats[team].indexOf(null);
      return seat >= 0 && !seats[team][seat] ? [team, seat] : null;
    }
    const first = seats[0].filter(Boolean).length <= seats[1].filter(Boolean).length ? 0 : 1;
    return openSeat(first) || openSeat(1 - first);
  }

  function join(peer, message) {
    if (peer.player) return;
    const { team, slot } = message;
    if ((team !== undefined && team !== 0 && team !== 1) || (slot !== undefined &&
        (team === undefined || !Number.isInteger(slot) || slot < 0 || slot >= slotsPerTeam))) {
      return sendError(peer, 'invalid-seat', 'Choose an Azure or Rose slot.');
    }
    const prior = players.get(message.playerId);
    const resume = prior && typeof message.token === 'string' && message.token === prior.token ? prior : null;
    if ((message.playerId !== undefined || message.token !== undefined) && !resume) {
      return sendError(peer, 'resume-invalid', 'Your previous seat is unavailable. Choose an open seat.');
    }
    if (phase === 'finished' && !resume?.peer) return sendError(peer, 'round-ended', 'This round ended. Join when the lobby reopens.');
    if (message.hero !== undefined && phase === 'waiting' && !validHero(message.hero)) {
      return sendError(peer, 'invalid-hero', 'Pick a hero from the roster.');
    }
    const reserved = resume && !seats[resume.team][resume.slot] ? [resume.team, resume.slot] : null;
    const seat = resume?.peer ? [resume.team, resume.slot] : reserved || openSeat(team, slot);
    if (!seat) return sendError(peer, team === undefined ? 'full' : 'seat-taken',
      team === undefined ? 'This lobby already has six players.' : 'That slot was taken. Choose another slot.');
    const [nextTeam, nextSlot] = seat;
    const player = resume || {
      playerId: randomUUID(), token: randomBytes(24).toString('base64url'),
      name: normalizeName(message.name, 'Player'), hero: phase === 'waiting' ? message.hero || defaultHero : null, peer: null
    };
    if (phase === 'playing' && !player.peer && !match.takeover(player.playerId, nextTeam, nextSlot, player.name)) {
      return sendError(peer, 'seat-taken', 'That slot was taken. Choose another slot.');
    }
    for (const former of players.values()) {
      if (former !== player && !former.peer && former.team === nextTeam && former.slot === nextSlot) players.delete(former.playerId);
    }
    const previousPeer = player.peer;
    player.team = nextTeam;
    player.slot = nextSlot;
    player.peer = peer;
    if (roster) player.hero = roster[nextTeam][nextSlot].hero;
    players.set(player.playerId, player);
    seats[nextTeam][nextSlot] = player;
    peer.player = player;
    previousPeer?.close(4001, 'Session resumed');
    if (!leader?.peer) leader = player;
    emptySince = 0;
    send(peer, { type: 'welcome', playerId: player.playerId, token: player.token });
    broadcastLobby();
    publishSnapshot(player);
  }

  function depart(peer, forget = false) {
    const player = peer.player;
    if (!player || player.peer !== peer) return;
    peer.player = null;
    player.peer = null;
    seats[player.team][player.slot] = null;
    if (forget) players.delete(player.playerId);
    if (phase === 'playing') match.release(player.playerId);
    if (leader === player) leader = seats.flat().find(Boolean) ?? null;
    if (!leader) emptySince = performance.now();
    broadcastLobby();
  }

  function select(peer, player, { team = player.team, slot, hero = player.hero }) {
    if (phase !== 'waiting') return sendError(peer, 'match-started', 'Hero picks are locked during a match.');
    if (!validHero(hero)) return sendError(peer, 'invalid-hero', 'Pick a hero from the roster.');
    if ((team !== 0 && team !== 1) || (slot !== undefined && (!Number.isInteger(slot) || slot < 0 || slot >= slotsPerTeam))) {
      return sendError(peer, 'invalid-seat', 'Choose an Azure or Rose slot.');
    }
    const seat = team === player.team && (slot === undefined || slot === player.slot) ? [team, player.slot] : openSeat(team, slot);
    if (!seat) return sendError(peer, 'seat-taken', 'That slot was taken. Choose another slot.');
    const [nextTeam, nextSlot] = seat;
    if (player.team === nextTeam && player.slot === nextSlot && player.hero === hero) return;
    seats[player.team][player.slot] = null;
    player.team = nextTeam;
    player.slot = nextSlot;
    player.hero = hero;
    seats[nextTeam][nextSlot] = player;
    for (const former of players.values()) {
      if (!former.peer && former.team === nextTeam && former.slot === nextSlot) players.delete(former.playerId);
    }
    broadcastLobby();
  }

  function startRound(peer, player) {
    if (phase !== 'waiting' || player !== leader) return sendError(peer, 'cannot-start', 'Only the lobby host can start a waiting match.');
    roster = seats.map(draftTeam);
    matchId = randomUUID();
    match = new OnlineMatch({ roster, seed: randomInt(2 ** 32), matchId });
    phase = 'playing';
    previousStep = snapshotAt = performance.now();
    broadcastLobby();
    for (const joined of players.values()) publishSnapshot(joined);
  }

  function resetRound() {
    for (const player of players.values()) if (!player.peer) players.delete(player.playerId);
    phase = 'waiting';
    match = roster = matchId = null;
    nextRoundAt = emptySince = 0;
    for (const peer of sockets.clients) send(peer, { type: 'round-ended' });
    broadcastLobby();
  }

  function tick() {
    const now = performance.now();
    if ((phase === 'finished' && now >= nextRoundAt) ||
        (phase === 'playing' && emptySince && now - emptySince >= unoccupiedMatchMs)) {
      resetRound();
    }
    if (phase !== 'playing') return;
    match.advance(Math.min((now - previousStep) / 1000, .12));
    previousStep = now;
    if (match.game.winner !== null) {
      phase = 'finished';
      nextRoundAt = now + roundSummaryMs;
      broadcastLobby();
    }
    if (now - snapshotAt < 65 && phase === 'playing') return;
    snapshotAt = now;
    for (const player of players.values()) publishSnapshot(player);
  }

  server.on('upgrade', (request, socket, head) => {
    if (request.url.split('?')[0] !== socketPath || !sameOrigin(request) || sockets.clients.size >= maxPeers) {
      socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
      return;
    }
    sockets.handleUpgrade(request, socket, head, peer => sockets.emit('connection', peer));
  });

  sockets.on('connection', peer => {
    peer.connectedAt = performance.now();
    peer.lastPong = peer.connectedAt;
    let messages = 0, rateWindow = peer.connectedAt;
    peer.on('error', () => peer.terminate());
    peer.on('pong', () => { peer.lastPong = performance.now(); });
    peer.on('close', () => depart(peer));
    peer.on('message', (data, binary) => {
      const now = performance.now();
      if (now - rateWindow >= 1000) { rateWindow = now; messages = 0; }
      if (++messages > 160 || binary) { peer.close(1008, 'Message limit'); return; }
      let message;
      try { message = JSON.parse(data.toString()); } catch { return; }
      if (!message || typeof message !== 'object' || Array.isArray(message)) return;
      if (message.type === 'ping') {
        if (Number.isFinite(message.time)) send(peer, { type: 'pong', time: message.time });
        return;
      }
      if (message.type === 'join') return join(peer, message);
      const player = peer.player;
      if (!player || player.peer !== peer) return;
      switch (message.type) {
        case 'select': return select(peer, player, message);
        case 'start': return startRound(peer, player);
        case 'command': if (phase === 'playing') match.command(player.playerId, message.action); break;
        case 'leave': depart(peer, true); peer.close(1000, 'Left lobby'); break;
      }
    });
    send(peer, { type: 'lobby', state: lobby() });
  });

  function close() {
    if (closePromise) return closePromise;
    clearInterval(ticker);
    clearInterval(heartbeat);
    for (const peer of sockets.clients) peer.terminate();
    sockets.close();
    closePromise = new Promise(done => server.close(() => done()));
    server.closeAllConnections();
    return closePromise;
  }

  server.once('listening', () => {
    ticker = setInterval(tick, 25);
    heartbeat = setInterval(() => {
      const now = performance.now();
      for (const peer of sockets.clients) {
        if (now - peer.lastPong > 35_000 || (!peer.player && now - peer.connectedAt > observerLimitMs)) peer.terminate();
        else if (peer.readyState === 1) peer.ping();
      }
    }, 15_000);
  });
  return { server, getStatus, joinPath: () => joinPath, close };
}

export async function startMobaHost({ packRoot, name, signal, onFailure, port = 0 } = {}) {
  signal?.throwIfAborted();
  const room = createMobaServer({ packRoot, name });
  const { server } = room;
  let closing;
  function close() {
    signal?.removeEventListener('abort', aborted);
    closing ||= room.close();
    return closing;
  }
  function aborted() { void close(); }
  try {
    await new Promise((ready, fail) => {
      server.once('error', fail);
      server.listen(port, '127.0.0.1', () => { server.off('error', fail); ready(); });
    });
    signal?.throwIfAborted();
    signal?.addEventListener('abort', aborted, { once: true });
    server.on('error', error => { if (!closing) onFailure?.(error); });
    server.once('close', () => { if (!closing) onFailure?.(new Error('MOBA Mania host stopped.')); });
    return { origin: `http://127.0.0.1:${server.address().port}`, getStatus: room.getStatus, joinPath: room.joinPath, close };
  } catch (error) {
    await close();
    throw error;
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const host = await startMobaHost({ port: Number(process.env.MOBA_MANIA_PORT || 8828) });
  console.log('MOBA Mania online lobby: ' + host.origin + host.joinPath());
  process.once('SIGINT', () => void host.close());
  process.once('SIGTERM', () => void host.close());
}
