'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { inDir, writeJson } = require('./storage.cjs');

function managedEdition(port, type, entry, server, runtimeDir, adapter, nativeServer) {
  return {
    type, port, statusType: 'game-host-status', joinPath: '/__catnmice/join',
    files: {
      entry, server, manager: 'manager/supervisor.cjs',
      runtime: runtimeDir + '/node.exe', tunnel: runtimeDir + '/cloudflared.exe',
    },
    required: [adapter, nativeServer, 'manager/gateway.cjs', 'manager/community-directory.cjs', 'manager/community-tunnel.cjs'],
  };
}

const editions = {
  acornrally: {
    type: 'static', port: 8791, statusType: 'rally-host-status', joinPath: '/?room=join',
    files: {
      entry: 'index.html',
      server: 'server/lobby-supervisor.cjs',
      runtime: 'server/runtime/node.exe',
      tunnel: 'server/runtime/cloudflared.exe',
    },
    required: ['server/host.cjs', 'server/community-host.cjs', 'server/community-directory.cjs', 'server/community-tunnel.cjs',
      'server/community-poll.cjs', 'server/race-queue.cjs', 'race-sim.js', 'network.js'],
  },
  bannerlands: managedEdition(8792, 'static', 'game/index.html', 'game/source/desktop/launcher.mjs',
    'runtime', 'manager/adapter.cjs', 'game/source/multiplayer-server.mjs'),
  trashtactics: managedEdition(8793, 'static', 'game/trash-tactics/index.html', 'game/trash-tactics/hosting/launch.mjs',
    'hub-runtime', 'manager/adapter.mjs', 'game/trash-tactics/server.mjs'),
  cat: managedEdition(8794, 'static', 'game/catandmouse/edition/index.html', 'game/catandmouse/edition/server/launcher.mjs',
    'hub-runtime', 'manager/adapter.mjs', 'game/catandmouse/edition/server/match-server.mjs'),
  lastsignal: managedEdition(8795, 'native-windows', 'Last Signal.exe', 'manager/supervisor.cjs',
    'Hosting', 'manager/adapter.mjs', 'Mobile/index.html'),
};
const supportsServerManager = id => Object.hasOwn(editions, id);

function hasServerManagerDescriptor(id, spec) {
  return supportsServerManager(id) && spec?.type === editions[id].type && spec.hosting === true &&
    Object.entries(editions[id].files).every(([name, relative]) => spec[name] === relative);
}

function hasServerManagerPack(id, spec, root) {
  return hasServerManagerDescriptor(id, spec) && [...Object.values(editions[id].files), ...editions[id].required]
    .every(relative => fs.statSync(inDir(root, relative), { throwIfNoEntry: false })?.isFile());
}

function preferences(input, title) {
  const name = typeof input?.name === 'string' ? input.name.trim() : '';
  if (name.length > 40 || /[\u0000-\u001f\u007f]/.test(name)) throw Error('Use a server name up to 40 characters.');
  return { name: name || title, visibility: input?.visibility === 'code' ? 'code' : 'public' };
}

function internetLink(origin, joinPath) {
  try {
    const url = new URL(origin);
    if (url.protocol === 'https:' && !url.port && !url.username && !url.password && !url.search && !url.hash &&
        url.pathname === '/' && /^[a-z0-9-]+\.trycloudflare\.com$/i.test(url.hostname)) {
      return new URL(joinPath, url).href;
    }
  } catch {}
  return undefined;
}

const hostMetrics = {
  tunnelRttMs: 60_000, tunnelJitterMs: 60_000, tunnelProbeFailures: 1_000_000,
  tunnelProbeSamples: 1_000_000, eventLoopLagMs: 60_000, snapshotRateHz: 240,
  snapshotBytes: 10_000_000, skippedSnapshots: 1_000_000_000,
  playerCount: 8, readyPlayers: 8, expectedPlayers: 8,
};
const playerMetrics = {
  rttMs: 60_000, jitterMs: 60_000, snapshotAgeMs: 600_000,
  snapshotBacklog: 1_000_000_000, snapshotGapPct: 100, loadMs: 600_000, fps: 360,
};

function readMetrics(data, fields) {
  const metrics = {};
  for (const [key, max] of Object.entries(fields)) {
    const value = data[key];
    if (Number.isFinite(value) && value >= 0 && value <= max) metrics[key] = Math.round(value * 10) / 10;
  }
  return metrics;
}

function readLabels(data, fields, target) {
  for (const key of fields) {
    const value = data[key];
    if (typeof value === 'string' && /^[a-z][a-z -]{0,30}$/i.test(value)) target[key] = value;
  }
}

function readRallyDiagnostics(data) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return null;
  const result = readMetrics(data, hostMetrics);
  readLabels(data, ['phase', 'localStatus', 'tunnelStatus', 'directoryStatus'], result);
  if (Array.isArray(data.players)) {
    result.players = data.players.slice(0, 8)
      .filter(player => player && typeof player === 'object' && !Array.isArray(player))
      .map(player => {
        const row = readMetrics(player, playerMetrics);
        if (typeof player.name === 'string') row.name = player.name.replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 40);
        readLabels(player, ['loadState'], row);
        if (['websocket', 'poll', 'events'].includes(player.transport)) row.transport = player.transport;
        return row;
      });
  }
  return { ...result, sampledAt: Date.now() };
}

function createServerManager({ dataDir, packFor, onChange, onExit, spawnProcess = spawn }) {
  const configPath = path.join(dataDir, 'server-manager.json');
  let stored = {};
  try {
    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    if (config && typeof config === 'object' && !Array.isArray(config)) stored = config;
  } catch {}
  const running = new Map();
  const exitMessage = new Map();

  function status(game) {
    let config;
    try { config = preferences(stored[game.id], game.title); }
    catch { config = preferences(null, game.title); }
    const instance = running.get(game.id);
    return {
      id: game.id,
      ...config,
      ...instance?.status,
      status: instance?.status.status || 'offline',
      running: Boolean(instance),
      installed: Boolean(packFor(game.id)),
      available: hasServerManagerDescriptor(game.id, game.offline),
      message: instance?.status.message || exitMessage.get(game.id),
    };
  }

  function start(game, input) {
    const { id, title } = game;
    if (!supportsServerManager(id)) throw Error('Server Manager does not support this game.');
    if (running.has(id)) throw Error('This game already has a server running.');
    const pack = packFor(id);
    if (!pack || !hasServerManagerPack(id, pack.spec, pack.root)) throw Error('Install this game’s hosting pack before starting its server.');
    const config = preferences(input, title);
    const edition = editions[id];
    const data = path.join(dataDir, 'servers', id);
    const cloudflared = inDir(pack.root, pack.spec.tunnel);
    const env = { ...process.env, ...(edition.files.manager ? {
      CATNMICE_GAME_ID: id,
      CATNMICE_SERVER_NAME: config.name,
      CATNMICE_VISIBILITY: config.visibility,
      CATNMICE_PORT: String(edition.port),
      CATNMICE_DATA_DIR: data,
      CATNMICE_CLOUDFLARED: cloudflared,
      CATNMICE_PACK_ROOT: pack.root,
    } : {
      ACORN_RALLY_COMMUNITY: '1',
      ACORN_RALLY_SERVER_NAME: config.name,
      ACORN_RALLY_VISIBILITY: config.visibility,
      ACORN_RALLY_PORT: String(edition.port),
      ACORN_RALLY_DATA_DIR: data,
      ACORN_RALLY_CLOUDFLARED: cloudflared,
    }) };
    const next = { ...stored, [id]: config };
    writeJson(configPath, next);
    stored = next;
    const child = spawnProcess(inDir(pack.root, pack.spec.runtime), [inDir(pack.root, edition.files.manager || edition.files.server)], {
      cwd: pack.root, env, windowsHide: true, stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    });
    const instance = { child, status: { status: 'starting' }, stopping: false, closed: null };
    running.set(id, instance);
    exitMessage.delete(id);
    let launchFailed = false;
    let timeout;
    child.on('message', data => {
      if (instance.stopping) return;
      if (id === 'acornrally' && data?.type === 'rally-host-diagnostics') {
        const diagnostics = readRallyDiagnostics(data.diagnostics);
        if (diagnostics) instance.diagnostics = diagnostics;
        return;
      }
      if (data?.type !== edition.statusType ||
          !['starting', 'online', 'lan-only', 'offline'].includes(data.status)) return;
      const link = data.status === 'online' ? internetLink(data.origin, edition.joinPath) : undefined;
      const usableCode = link || (edition.files.manager && data.status === 'lan-only');
      const code = usableCode && config.visibility === 'code' && typeof data.code === 'string' && /^\d{4}$/.test(data.code)
        ? data.code : undefined;
      instance.status = {
        status: data.status, link: config.visibility === 'public' ? link : undefined, code,
        message: typeof data.message === 'string' ? data.message.slice(0, 250) : undefined,
      };
      onChange();
    });
    child.once('error', () => { launchFailed = true; });
    instance.closed = new Promise(resolve => child.once('close', code => {
      clearTimeout(timeout);
      running.delete(id);
      if (!instance.stopping) {
        exitMessage.set(id, code === 98 ? 'The local server port is already in use. Stop the other ' + title + ' host and try again.'
          : launchFailed ? 'Windows could not start the bundled game server. Repair the game pack.'
          : (instance.status.status === 'offline' && instance.status.message) || 'The server stopped. Start it again to reopen its lobby.');
      }
      onChange();
      onExit(id);
      resolve();
    }));
    instance.stop = () => {
      if (!instance.stopping) {
        instance.stopping = true;
        instance.status = { status: 'stopping' };
        timeout = setTimeout(() => child.kill(), 12_000);
        timeout.unref();
        if (child.connected) child.send({ type: 'stop' }, error => { if (error) child.kill(); });
        else child.kill();
        onChange();
      }
      return instance.closed;
    };
    onChange();
  }

  return {
    status,
    diagnostics(game) {
      if (game.id !== 'acornrally') throw Error('Connection diagnostics are available for Acorn Rally.');
      const instance = running.get(game.id);
      return {
        status: instance?.status.status || 'offline',
        message: instance?.status.message || exitMessage.get(game.id),
        metrics: instance?.stopping ? undefined : instance?.diagnostics,
      };
    },
    start,
    stop: id => running.get(id)?.stop(),
    stopAll: () => Promise.all([...running.values()].map(instance => instance.stop())),
    busy: id => id ? running.has(id) : running.size > 0,
    localLink(id) {
      const current = running.get(id);
      if (!current || !['online', 'lan-only'].includes(current.status.status)) throw Error('Wait for the game server to finish starting.');
      return 'http://127.0.0.1:' + editions[id].port + editions[id].joinPath;
    },
  };
}

module.exports = { createServerManager, hasServerManagerDescriptor, hasServerManagerPack, supportsServerManager };
