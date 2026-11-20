'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { inDir, writeJson } = require('./storage.cjs');

const editions = {
  acornrally: {
    port: 8791,
    files: {
      entry: 'index.html',
      server: 'server/lobby-supervisor.cjs',
      runtime: 'server/runtime/node.exe',
      tunnel: 'server/runtime/cloudflared.exe',
    },
    required: ['server/host.cjs', 'server/community-host.cjs', 'server/community-directory.cjs', 'server/community-tunnel.cjs',
      'server/community-poll.cjs', 'server/race-queue.cjs', 'race-sim.js', 'network.js'],
  },
};
const supportsServerManager = id => Object.hasOwn(editions, id);

function hasServerManagerDescriptor(id, spec) {
  return supportsServerManager(id) && spec?.type === 'static' && spec.hosting === true &&
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

function internetLink(origin) {
  try {
    const url = new URL(origin);
    if (url.protocol === 'https:' && !url.port && !url.username && !url.password && !url.search && !url.hash &&
        url.pathname === '/' && /^[a-z0-9-]+\.trycloudflare\.com$/i.test(url.hostname)) {
      url.searchParams.set('room', 'join');
      return url.href;
    }
  } catch {}
  return undefined;
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
    const env = {
      ...process.env,
      ACORN_RALLY_COMMUNITY: '1',
      ACORN_RALLY_SERVER_NAME: config.name,
      ACORN_RALLY_VISIBILITY: config.visibility,
      ACORN_RALLY_PORT: String(edition.port),
      ACORN_RALLY_DATA_DIR: path.join(dataDir, 'servers', id),
      ACORN_RALLY_CLOUDFLARED: inDir(pack.root, pack.spec.tunnel),
    };
    const next = { ...stored, [id]: config };
    writeJson(configPath, next);
    stored = next;
    const child = spawnProcess(inDir(pack.root, pack.spec.runtime), [inDir(pack.root, pack.spec.server)], {
      cwd: pack.root, env, windowsHide: true, stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    });
    const instance = { child, status: { status: 'starting' }, stopping: false, closed: null };
    running.set(id, instance);
    exitMessage.delete(id);
    let launchFailed = false;
    let timeout;
    child.on('message', data => {
      if (instance.stopping || data?.type !== 'rally-host-status' ||
          !['starting', 'online', 'lan-only', 'offline'].includes(data.status)) return;
      const link = data.status === 'online' ? internetLink(data.origin) : undefined;
      const code = link && config.visibility === 'code' && typeof data.code === 'string' && /^\d{4}$/.test(data.code)
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
        exitMessage.set(id, code === 98 ? 'The local server port is already in use. Stop the other Acorn Rally host and try again.'
          : launchFailed ? 'Windows could not start the bundled game server. Repair the game pack.'
          : 'The server stopped. Start it again to reopen its lobby.');
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
    start,
    stop: id => running.get(id)?.stop(),
    stopAll: () => Promise.all([...running.values()].map(instance => instance.stop())),
    busy: id => id ? running.has(id) : running.size > 0,
    localLink(id) {
      const current = running.get(id);
      if (!current || !['online', 'lan-only'].includes(current.status.status)) throw Error('Wait for the game server to finish starting.');
      return 'http://127.0.0.1:' + editions[id].port + '/?room=join';
    },
  };
}

module.exports = { createServerManager, hasServerManagerDescriptor, hasServerManagerPack, supportsServerManager };
