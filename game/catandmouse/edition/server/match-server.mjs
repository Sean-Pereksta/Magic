import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { createReadStream } from 'node:fs';
import { stat } from 'node:fs/promises';
import { dirname, extname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { createSimulation, isCompleteLoadout, normalizeLoadout, STEP_MS } from '../simulation.mjs';
import { PROTOCOL_VERSION } from '../directory-config.mjs';
import { displayName, readLobbySettings } from './hosting-config.mjs';

const editionRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const loadingMs = 20_000;
const countdownMs = 3_000;
const lobbyReconnectMs = 90_000;
const battleReconnectMs = 120_000;
const secret = () => randomBytes(24).toString('base64url');
const mimeTypes = {
  '.html': 'text/html; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.png': 'image/png',
  '.webp': 'image/webp',
  '.jpg': 'image/jpeg',
  '.json': 'application/json; charset=utf-8',
  '.ogg': 'audio/ogg',
  '.mp3': 'audio/mpeg',
  '.woff2': 'font/woff2',
  '.svg': 'image/svg+xml'
};

export function serveEdition(request, response) {
  if (!['GET', 'HEAD'].includes(request.method)) {
    response.writeHead(405).end();
    return;
  }
  let requested;
  try {
    requested = decodeURIComponent(new URL(request.url, 'http://localhost').pathname);
  } catch {
    response.writeHead(400).end();
    return;
  }
  if (requested === '/') requested = '/index.html';
  const file = resolve(editionRoot, '.' + requested);
  const type = mimeTypes[extname(file)];
  const pathIsPublic = /^\/[a-zA-Z0-9_-]+\.[a-zA-Z0-9]+$/.test(requested)
    || /^\/art\/[a-zA-Z0-9_./-]+$/.test(requested)
    || /^\/assets\/[a-zA-Z0-9_./-]+$/.test(requested);
  if (!type || !pathIsPublic || /(?:^|\/)\.\.?\//.test(requested)
    || !file.startsWith(editionRoot + sep)) {
    response.writeHead(404).end();
    return;
  }
  stat(file).then(info => {
    if (!info.isFile()) {
      response.writeHead(404).end();
      return;
    }
    response.writeHead(200, {
      'Content-Type': type,
      'Content-Length': info.size,
      'Cache-Control': 'no-cache',
      'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'no-referrer'
    });
    if (request.method === 'HEAD') response.end();
    else createReadStream(file).on('error', () => response.destroy()).pipe(response);
  }).catch(() => response.writeHead(404).end());
}

function send(socket, message) {
  if (socket?.readyState === 1) socket.send(JSON.stringify(message));
}

function fail(socket, code, message) {
  send(socket, { type: 'error', code, message });
}

export async function createMatchServer(options, { onLobbyChange = () => {}, desktopOrigin, rankedSeed, managed = false } = {}) {
  const ranked = rankedSeed !== undefined;
  const settings = readLobbySettings(ranked ? { ...options, startingWave: 1 } : options);
  let id = randomUUID();
  const hostToken = secret();
  const invite = secret();
  const listingToken = secret();
  const listingProof = createHash('sha256').update(listingToken).digest('hex');
  const players = new Map();
  let origin;
  let phase = 'waiting';
  let matchId = null;
  let included = new Set();
  let force = false;
  let loadDeadline = null;
  let countdownUntil = null;
  let simulation = null;
  let lastSnapshot = null;
  let resetAfter = 0;

  let host = managed ? null : {
    id: randomUUID(), name: settings.hostName, token: secret(), isHost: true,
    ready: false, loaded: false, loadout: normalizeLoadout(), socket: null, expiresAt: Infinity
  };
  if (host) players.set(host.id, host);

  function chooseLeader() {
    if (!managed || host?.socket) return;
    const next = [...players.values()].find(player => player.socket);
    if (!next) return;
    if (host) host.isHost = false;
    host = next;
    host.isHost = true;
  }

  function resetRoom() {
    for (const player of players.values()) {
      if (!player.socket) continue;
      send(player.socket, { type: 'round-ended' });
      player.socket.player = null;
      player.socket.close(1000, 'Match ended');
    }
    players.clear();
    included.clear();
    host = null;
    id = randomUUID();
    phase = 'waiting';
    matchId = loadDeadline = countdownUntil = simulation = lastSnapshot = null;
    resetAfter = 0;
    broadcastLobby();
  }

  function getLobby() {
    const roster = [...players.values()];
    const canStart = phase === 'waiting' && roster.length === settings.capacity
      && roster.every(player => player.socket && player.ready);
    return {
      id, ...settings, ...(managed && { managed: true }), phase, matchId, loadDeadline, countdownUntil, canStart,
      players: roster.map(({ id, name, isHost, ready, loaded, loadout, socket }) => ({
        id, name, isHost, ready, loaded, loadout, connected: Boolean(socket)
      }))
    };
  }

  function broadcastLobby() {
    const lobby = getLobby();
    for (const player of players.values()) send(player.socket, { type: 'lobby', lobby });
    onLobbyChange(lobby);
  }

  function prepare(player) {
    send(player.socket, {
      type: 'prepare', matchId, map: settings.map, difficulty: settings.difficulty, startingWave: settings.startingWave,
      deadline: loadDeadline
    });
  }

  function start(player, forced) {
    if (!player.isHost) {
      fail(player.socket, 'host-only', 'Only the host can start this lobby.');
      return;
    }
    if (phase !== 'waiting') {
      send(player.socket, { type: 'lobby', lobby: getLobby() });
      return;
    }
    if (!forced && !getLobby().canStart) {
      fail(player.socket, 'not-ready', 'Fill all seats and mark everyone ready before starting.');
      return;
    }
    force = forced;
    phase = 'loading';
    matchId = randomUUID();
    included = new Set([...players.values()].filter(entry => entry.socket).map(entry => entry.id));
    loadDeadline = Date.now() + loadingMs;
    for (const entry of players.values()) {
      entry.loaded = false;
      if (included.has(entry.id)) {
        entry.loadout = normalizeLoadout(entry.loadout);
        prepare(entry);
      }
    }
    broadcastLobby();
  }

  function resolveLoading(now) {
    const participants = [...included].map(playerId => players.get(playerId));
    const complete = player => Boolean(player?.loaded && player.socket);
    if (participants.every(complete)) {
      phase = 'countdown';
    } else if (now < loadDeadline) {
      return;
    } else if (force && complete(host)) {
      for (const player of participants) {
        if (complete(player)) continue;
        fail(player.socket, 'load-timeout', 'You missed the battlefield loading window. The host is starting without your seat.');
        player.socket?.close(4002, 'Battlefield loading timed out');
        players.delete(player.id);
        included.delete(player.id);
      }
      phase = 'countdown';
    } else {
      phase = 'waiting';
      matchId = null;
      included.clear();
      loadDeadline = null;
      for (const player of players.values()) {
        player.loaded = false;
        player.ready = false;
        fail(player.socket, 'start-retry', 'A player did not finish loading. The lobby is ready for another attempt.');
      }
      broadcastLobby();
      return;
    }
    countdownUntil = now + countdownMs;
    for (const player of players.values()) {
      if (included.has(player.id)) continue;
      fail(player.socket, 'started', 'The lobby started while your seat was disconnected.');
      player.socket?.close(4002, 'Lobby started');
      players.delete(player.id);
    }
    broadcastLobby();
  }

  function sendSnapshot(player, snapshot) {
    const { events, ...state } = snapshot;
    send(player.socket, { type: 'snapshot', state, events });
    if (player.socket) player.socket.needsSnapshot = false;
  }

  function broadcastSimulation(forceSnapshot = false, current = simulation.snapshot()) {
    if (forceSnapshot || !lastSnapshot) {
      for (const player of players.values()) sendSnapshot(player, current);
      lastSnapshot = current;
      return;
    }
    const { events, ...state } = current;
    const changes = {};
    for (const [key, value] of Object.entries(state)) {
      if (JSON.stringify(value) !== JSON.stringify(lastSnapshot[key])) changes[key] = value;
    }
    const previousEvent = lastSnapshot.events.at(-1)?.id ?? 0;
    const delta = JSON.stringify({
      type: 'delta', tick: current.tick, baseTick: lastSnapshot.tick, changes,
      events: events.filter(event => event.id > previousEvent)
    });
    for (const player of players.values()) {
      const socket = player.socket;
      if (socket?.readyState !== 1) continue;
      if (socket.bufferedAmount > 256_000) {
        socket.needsSnapshot = true;
      } else if (socket.needsSnapshot) {
        sendSnapshot(player, current);
      } else {
        socket.send(delta);
      }
    }
    lastSnapshot = current;
  }

  function join(socket, message) {
    if (message.version !== PROTOCOL_VERSION) {
      fail(socket, 'version', 'This room uses a different game version. Refresh the game and try again.');
      return;
    }
    if (message.roomId !== id) {
      fail(socket, 'room-changed', 'That exact room is no longer at this address.');
      return;
    }
    const returning = message.token && [...players.values()].find(player => player.token === message.token);
    let player = returning || (!managed && message.hostToken === hostToken ? host : null);
    if (player && !player.socket && Date.now() >= player.expiresAt) player = null;

    if (!player) {
      if (message.token) {
        fail(socket, 'seat-expired', 'Your previous seat expired.');
        return;
      }
      if (phase !== 'waiting') {
        fail(socket, 'started', 'This room has already locked admission for its match.');
        return;
      }
      if (settings.visibility === 'invite' && message.invite !== invite) {
        fail(socket, 'invite-required', 'This room requires a current guest invitation.');
        return;
      }
      if (players.size >= settings.capacity) {
        fail(socket, 'full', 'All seats in this room have been reserved.');
        return;
      }
      player = {
        id: randomUUID(), name: displayName(message.name), token: secret(), isHost: false,
        ready: false, loaded: false, loadout: normalizeLoadout(), socket: null, expiresAt: Infinity
      };
      players.set(player.id, player);
    }
    player.socket?.close(4001, 'This seat reconnected');
    player.socket = socket;
    player.expiresAt = Infinity;
    socket.player = player;
    chooseLeader();
    send(socket, { type: 'joined', token: player.token, playerId: player.id });
    broadcastLobby();
    if (phase === 'loading' && included.has(player.id)) prepare(player);
    if (lastSnapshot) sendSnapshot(player, lastSnapshot);
  }

  function handleMessage(socket, message) {
    const player = socket.player;
    if (!player) {
      if (message.type === 'join') join(socket, message);
      if (message.type === 'probe' && message.roomId === id && message.version === PROTOCOL_VERSION) {
        send(socket, { type: 'probe', roomId: id, version: PROTOCOL_VERSION });
      }
      return;
    }
    if (player.socket !== socket) return;
    switch (message.type) {
      case 'ready':
        if (phase !== 'waiting') break;
        if (message.ready && !isCompleteLoadout(message.loadout)) {
          fail(socket, 'loadout', 'Choose eight different build options before marking ready.');
          break;
        }
        player.loadout = normalizeLoadout(message.loadout);
        player.ready = message.ready === true;
        broadcastLobby();
        break;
      case 'start':
        start(player, message.force === true);
        break;
      case 'loaded':
        if (phase === 'loading' && message.matchId === matchId && included.has(player.id)) {
          player.loaded = true;
          broadcastLobby();
          resolveLoading(Date.now());
        }
        break;
      case 'resync':
        if (lastSnapshot) sendSnapshot(player, lastSnapshot);
        break;
      case 'input': {
        if (!included.has(player.id) || phase !== 'running'
          || typeof message.command?.type !== 'string') break;
        const result = simulation.command(player.id, message.command);
        if (result && !result.ok && message.command?.type !== 'move') {
          send(socket, { type: 'action-result', ok: false, code: result.code });
        }
        break;
      }
      case 'leave':
        socket.leftRoom = true;
        if (phase === 'waiting' && (!player.isHost || managed)) players.delete(player.id);
        socket.close(1000, 'Left lobby');
        break;
      default:
        break;
    }
  }

  const http = createServer((request, response) => {
    if (request.method === 'GET' && request.url?.split('?')[0] === '/health') {
      response.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store'
      }).end(JSON.stringify({ game: 'catandmouse', roomId: id, version: PROTOCOL_VERSION, listingProof }));
      return;
    }
    serveEdition(request, response);
  });
  const wss = new WebSocketServer({
    noServer: true, maxPayload: 4096, perMessageDeflate: false, maxPendingAutoPongs: 8
  });
  http.on('upgrade', (request, socket, head) => {
    const expectedHost = request.headers.host;
    const pageOrigin = request.headers.origin;
    if (request.url !== '/socket' || wss.clients.size >= 24
      || (pageOrigin && pageOrigin !== 'http://' + expectedHost && pageOrigin !== 'https://' + expectedHost
        && pageOrigin !== desktopOrigin)) {
      socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
      return;
    }
    wss.handleUpgrade(request, socket, head, client => wss.emit('connection', client));
  });
  wss.on('connection', socket => {
    let windowStart = Date.now();
    let messages = 0;
    const joinTimer = setTimeout(() => {
      if (!socket.player) socket.close(4000, 'Join timed out');
    }, 15_000);
    socket.alive = true;
    socket.on('pong', () => { socket.alive = true; });
    socket.on('error', () => {});
    socket.on('message', (buffer, binary) => {
      const now = Date.now();
      if (now - windowStart >= 1_000) {
        windowStart = now;
        messages = 0;
      }
      if (++messages > 60 || binary) {
        socket.close(1008, 'Unexpected message volume or format');
        return;
      }
      let message;
      try {
        message = JSON.parse(buffer.toString());
      } catch {
        fail(socket, 'format', 'The message could not be read.');
        return;
      }
      if (message && typeof message.type === 'string') handleMessage(socket, message);
    });
    socket.on('close', () => {
      clearTimeout(joinTimer);
      const player = socket.player;
      if (!player || player.socket !== socket) return;
      player.socket = null;
      chooseLeader();
      if (simulation) simulation.command(player.id, { type: 'move', dx: 0, dy: 0 });
      if (phase === 'waiting') player.ready = false;
      if (['countdown', 'running', 'finished'].includes(phase)) player.expiresAt = Date.now() + battleReconnectMs;
      else if (managed || !player.isHost) player.expiresAt = Date.now() + lobbyReconnectMs;
      if (managed && socket.leftRoom) player.expiresAt = 0;
      broadcastLobby();
    });
  });

  await new Promise((done, reject) => {
    http.once('error', reject);
    http.listen(0, '127.0.0.1', done);
  });
  origin = 'http://127.0.0.1:' + http.address().port;
  let due = performance.now() + STEP_MS;
  const tick = setInterval(() => {
    const now = Date.now();
    if (phase === 'loading') resolveLoading(now);
    if (phase === 'countdown' && now >= countdownUntil) {
      simulation = createSimulation({
        matchId, mapId: settings.map, difficulty: settings.difficulty, startingWave: settings.startingWave,
        ...(ranked ? { ranked, seed: rankedSeed } : {}),
        players: [...included].map(playerId => {
          const { id, name, loadout } = players.get(playerId);
          return { id, name, loadout };
        })
      });
      phase = 'running';
      due = performance.now() + STEP_MS;
      broadcastLobby();
      broadcastSimulation(true);
    }
    if (phase !== 'running') return;
    let steps = 0;
    const tickTime = performance.now();
    while (tickTime >= due && steps < 5) {
      simulation.step();
      due += STEP_MS;
      steps++;
    }
    if (steps === 5) due = Math.max(due, tickTime);
    if (!steps) return;
    if (simulation.status !== 'active') {
      phase = 'finished';
      if (managed) resetAfter = now + 4_000;
      broadcastSimulation(true);
      broadcastLobby();
    } else if (simulation.tick >= lastSnapshot.tick + 4) {
      broadcastSimulation();
    }
  }, Math.max(10, STEP_MS / 2));
  const heartbeat = setInterval(() => {
    for (const socket of wss.clients) {
      if (!socket.alive) socket.terminate();
      else {
        socket.alive = false;
        socket.ping();
      }
    }
  }, 15_000);
  const admissions = setInterval(() => {
    const now = Date.now();
    if (managed && phase !== 'waiting' && phase !== 'closed') {
      const abandoned = [...players.values()].every(player => !player.socket && now >= player.expiresAt);
      if (abandoned || (resetAfter && now >= resetAfter)) resetRoom();
      return;
    }
    if (phase !== 'waiting') return;
    let changed = false;
    for (const player of players.values()) {
      if (now < player.expiresAt) continue;
      players.delete(player.id);
      changed = true;
    }
    if (changed) broadcastLobby();
  }, 1_000);

  return {
    url: origin, get id() { return id; }, hostToken, invite, listingToken, getLobby,
    ...(ranked ? { seatToken: host.token } : {}),
    get score() { return ranked && phase === 'finished' ? simulation.rankedScore : null; },
    async close() {
      phase = 'closed';
      clearInterval(heartbeat);
      clearInterval(tick);
      clearInterval(admissions);
      for (const socket of wss.clients) {
        fail(socket, 'host-closed', 'The host ended this lobby.');
        socket.close(1001, 'Host closed');
      }
      const terminate = setTimeout(() => {
        for (const socket of wss.clients) socket.terminate();
        http.closeAllConnections();
      }, 500);
      try {
        await new Promise(done => wss.close(done));
        await new Promise(done => http.close(done));
      } finally {
        clearTimeout(terminate);
      }
    }
  };
}

if (process.send && process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const match = await createMatchServer(JSON.parse(process.env.CAT_MOUSE_ROOM_OPTIONS), {
    desktopOrigin: process.env.CAT_MOUSE_DESKTOP_ORIGIN,
    onLobbyChange: lobby => process.connected && process.send({ type: 'lobby', lobby })
  });
  let shuttingDown = false;
  const shutdown = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    await match.close();
    process.exit(0);
  };
  process.on('disconnect', shutdown);
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
  process.on('message', message => {
    if (message?.type === 'stop') shutdown();
  });
  process.send({
    type: 'listening', url: match.url, id: match.id, hostToken: match.hostToken,
    invite: match.invite, listingToken: match.listingToken, lobby: match.getLobby()
  });
}
