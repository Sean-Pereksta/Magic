import { fork, spawn } from 'node:child_process';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import { createServer } from 'node:http';
import { mkdir, readFile, unlink, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { CAT_DIRECTORY_URL, PROTOCOL_VERSION } from '../directory-config.mjs';
import { readLobbySettings } from './hosting-config.mjs';
import { serveEdition } from './match-server.mjs';
import { openTunnel } from './tunnel.mjs';

const serverRoot = dirname(fileURLToPath(import.meta.url));
const editionRoot = resolve(serverRoot, '..');
const argument = name => {
  const index = process.argv.indexOf('--' + name);
  return index < 0 ? null : process.argv[index + 1];
};
const appPort = Number(argument('app-port') ?? 38471);
const localOnly = process.argv.includes('--local-only');
const noOpen = process.argv.includes('--no-open');
const directoryUrl = (process.env.CAT_MOUSE_DIRECTORY_URL ?? CAT_DIRECTORY_URL).trim().replace(/\/+$/, '');
const cloudflared = argument('cloudflared') ?? join(editionRoot, 'runtime', 'cloudflared.exe');
const data = argument('data-dir') ?? join(process.env.LOCALAPPDATA || editionRoot, 'CatMouseLocal');
await mkdir(data, { recursive: true });
const sessionFile = join(data, 'desktop-' + appPort + '.json');
const desktop = randomBytes(32).toString('base64url');
const authorization = Buffer.from('Bearer ' + desktop);
const bind = (server, port = 0) => new Promise((done, reject) => {
  server.once('error', reject);
  server.listen(port, '127.0.0.1', done);
});
const origin = server => 'http://127.0.0.1:' + server.address().port;
const app = createServer(serveEdition);
try {
  await bind(app, appPort);
} catch (error) {
  if (error.code !== 'EADDRINUSE') throw error;
  const existing = JSON.parse(await readFile(sessionFile, 'utf8'));
  if (!Number.isInteger(existing.control) || !/^[\w-]{43}$/.test(existing.desktop)) throw error;
  const current = await fetch('http://127.0.0.1:' + existing.control + '/host', {
    headers: { Authorization: 'Bearer ' + existing.desktop }, signal: AbortSignal.timeout(1_500)
  });
  if (!current.ok) throw error;
  openApp(appPort, existing);
  process.exit(0);
}

let room = null;
let stage = 'idle';
let message = 'Choose your room settings.';
let stopping = false;
let directoryQueue = Promise.resolve();
let control;

function openApp(port, session) {
  const fragment = new URLSearchParams({ desktop: session.desktop, control: session.control });
  const url = 'http://127.0.0.1:' + port + '/#' + fragment;
  if (process.platform === 'win32' && !noOpen) {
    spawn('rundll32.exe', ['url.dll,FileProtocolHandler', url], {
      detached: true, windowsHide: true, stdio: 'ignore'
    }).unref();
  } else {
    console.log(JSON.stringify({ bootstrapUrl: url }));
  }
}

function status() {
  return {
    stage, message,
    lobbyId: room?.id ?? null,
    serverUrl: room?.url ?? null,
    publicUrl: room?.publicUrl ?? null,
    inviteUrl: room?.publicUrl ? room.publicUrl + '/#' + new URLSearchParams({
      room: room.id, invite: room.invite
    }) : null,
    hostToken: room?.hostToken ?? null,
    directoryStatus: room?.directoryStatus ?? null
  };
}

function directoryRequest(entry, method, body) {
  return fetch(directoryUrl + '/rooms/' + entry.id, {
    method,
    headers: { Authorization: 'Bearer ' + entry.listingToken, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
    redirect: 'error',
    signal: AbortSignal.timeout(6_000)
  }).then(result => {
    if (!result.ok) throw new Error('Directory returned ' + result.status);
  });
}

function syncDirectory(entry, renew = false) {
  if (entry.settings.visibility !== 'public' || localOnly || !directoryUrl) return;
  const visible = !entry.cancelled && Boolean(entry.publicUrl && entry.lobby?.phase === 'waiting'
    && entry.lobby.players.some(player => player.isHost && player.connected));
  const openSeats = visible ? entry.settings.capacity - entry.lobby.players.length : null;
  const signature = visible ? entry.publicUrl + '/' + openSeats : '';
  if (!renew && signature === entry.signature) return;
  entry.signature = signature;
  directoryQueue = directoryQueue.then(async () => {
    if (!entry.listingToken || signature !== entry.signature) return;
    try {
      if (visible) {
        const { roomName, hostName, map, difficulty, capacity } = entry.settings;
        await directoryRequest(entry, 'PUT', {
          public: true, roomName, hostName, map, difficulty, capacity, openSeats,
          serverUrl: entry.publicUrl, version: PROTOCOL_VERSION
        });
        entry.directoryStatus = 'published';
      } else if (entry.directoryStatus === 'published' || entry.directoryStatus === 'unavailable') {
        await directoryRequest(entry, 'DELETE');
        entry.directoryStatus = 'waiting';
      }
    } catch {
      entry.directoryStatus = 'unavailable';
    }
  });
}

function tunnelFor(entry) {
  if (localOnly) {
    stage = 'ready';
    message = 'Local room is ready for a loopback test.';
    return;
  }
  stage = 'starting';
  message = 'Opening a temporary public connection…';
  const tunnel = openTunnel({
    executable: cloudflared, serverUrl: entry.url, lobbyId: entry.id,
    onStage(next, detail) {
      if (room !== entry || entry.cancelled) return;
      stage = next;
      message = detail;
    },
    onDisconnect() {
      if (room !== entry || entry.cancelled) return;
      entry.publicUrl = null;
      stage = 'failed';
      message = 'The public connection closed. Retry to restore guest access.';
      syncDirectory(entry);
    }
  });
  entry.tunnel = tunnel;
  tunnel.verified.then(url => {
    if (room !== entry || entry.cancelled || entry.tunnel !== tunnel) return;
    entry.publicUrl = url;
    stage = 'ready';
    message = 'The public room address is ready.';
    syncDirectory(entry);
  }).catch(error => {
    if (room !== entry || entry.cancelled || entry.tunnel !== tunnel) return;
    stage = 'failed';
    message = error.message;
  });
}

function host(settings) {
  const entry = {
    settings, cancelled: false, signature: '', directoryStatus: settings.visibility !== 'public'
      ? 'private' : directoryUrl && !localOnly ? 'waiting' : 'disabled'
  };
  room = entry;
  stage = 'starting';
  message = 'Starting the local match server…';
  const child = fork(join(serverRoot, 'match-server.mjs'), [], {
    stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    windowsHide: true,
    env: {
      ...process.env, CAT_MOUSE_ROOM_OPTIONS: JSON.stringify(settings),
      CAT_MOUSE_DESKTOP_ORIGIN: origin(app)
    }
  });
  entry.child = child;
  child.on('message', update => {
    if (room !== entry || entry.cancelled) return;
    if (update.type === 'listening') {
      const { url, id, invite, hostToken, listingToken, lobby } = update;
      Object.assign(entry, { url, id, invite, hostToken, listingToken, lobby });
      tunnelFor(entry);
    } else if (update.type === 'lobby') {
      entry.lobby = update.lobby;
      syncDirectory(entry);
    }
  });
  const failure = () => {
    if (room !== entry || entry.cancelled) return;
    stage = 'failed';
    message = 'The local match server ended. Stop this room and start a new one.';
    entry.url = null;
    entry.publicUrl = null;
    entry.tunnel?.stop().catch(() => {});
    syncDirectory(entry);
  };
  child.on('error', failure);
  child.once('exit', failure);
}

function stopRoom() {
  const entry = room;
  if (!entry) return Promise.resolve();
  entry.stopTask ??= stopEntry(entry);
  return entry.stopTask;
}

async function stopEntry(entry) {
  entry.cancelled = true;
  stage = 'stopping';
  message = 'Closing the room…';
  syncDirectory(entry);
  if (entry.child.exitCode === null && entry.child.connected) {
    await new Promise(done => {
      const fallback = setTimeout(() => {
        entry.child.kill();
        done();
      }, 2_000);
      entry.child.once('exit', () => {
        clearTimeout(fallback);
        done();
      });
      entry.child.send({ type: 'stop' }, () => {});
    });
  }
  await Promise.all([
    directoryQueue,
    entry.tunnel?.stop().catch(() => {})
  ]);
  room = null;
  stage = 'idle';
  message = 'Choose your room settings.';
}

async function jsonBody(request) {
  const parts = [];
  let length = 0;
  for await (const part of request) {
    length += part.length;
    if (length > 2_048) throw new Error('The room settings are too large.');
    parts.push(part);
  }
  return JSON.parse(Buffer.concat(parts).toString());
}

control = createServer(async (request, response) => {
  const appOrigin = origin(app);
  response.setHeader('Access-Control-Allow-Origin', appOrigin);
  response.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  response.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  response.setHeader('Cache-Control', 'no-store');
  response.setHeader('Content-Type', 'application/json; charset=utf-8');
  const answer = (code, body) => response.writeHead(code).end(JSON.stringify(body));
  if (request.headers.origin && request.headers.origin !== appOrigin) return answer(403, { message: 'Open the installed game.' });
  if (request.method === 'OPTIONS') return response.writeHead(204).end();
  const supplied = Buffer.from(request.headers.authorization ?? '');
  if (supplied.length !== authorization.length || !timingSafeEqual(supplied, authorization)) {
    return answer(401, { message: 'Reopen Cat & Mouse from the Local Edition.' });
  }
  const route = request.url?.split('?')[0];
  try {
    if (route === '/host' && request.method === 'GET') return answer(200, status());
    if (route === '/host' && request.method === 'POST') {
      const settings = readLobbySettings(await jsonBody(request));
      if (room || stopping) return answer(409, { message: 'Close the current room before hosting another.' });
      host(settings);
      return answer(202, status());
    }
    if (route === '/host/retry' && request.method === 'POST') {
      if (!room?.url || stage !== 'failed') return answer(409, { message: 'There is no failed connection to retry.' });
      const entry = room;
      stage = 'starting';
      await entry.tunnel?.stop();
      if (room === entry && !entry.cancelled) tunnelFor(entry);
      return answer(202, status());
    }
    if (route === '/host' && request.method === 'DELETE') {
      void stopRoom();
      return answer(202, status());
    }
    if (route === '/app' && request.method === 'DELETE') {
      answer(202, { message: 'Closing Cat & Mouse.' });
      void shutdown();
      return;
    }
    return answer(404, { message: 'Unknown Local Edition action.' });
  } catch (error) {
    return answer(400, { message: error.message });
  }
});
await bind(control);
const session = { desktop, control: control.address().port };
await writeFile(sessionFile, JSON.stringify(session), { mode: 0o600 });
openApp(app.address().port, session);

const renew = setInterval(() => {
  if (room?.publicUrl && !room.cancelled) syncDirectory(room, true);
}, 20_000);
async function shutdown() {
  if (stopping) return;
  stopping = true;
  clearInterval(renew);
  await stopRoom();
  await unlink(sessionFile).catch(() => {});
  control.closeAllConnections();
  app.closeAllConnections();
  process.exit(0);
}
process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
