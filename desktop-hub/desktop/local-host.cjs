const fs = require('node:fs');
const { spawn } = require('node:child_process');
const { createInterface } = require('node:readline');
const { inDir } = require('./storage.cjs');

const editions = {
  bannerlands: {
    entry: 'game/index.html',
    server: 'game/source/desktop/launcher.mjs',
    runtime: 'runtime/node.exe',
    tunnel: 'runtime/cloudflared.exe',
  },
  cat: {
    entry: 'game/catandmouse/edition/index.html',
    server: 'game/catandmouse/edition/server/launcher.mjs',
    runtime: 'hub-runtime/node.exe',
    tunnel: 'hub-runtime/cloudflared.exe',
  },
  trashtactics: {
    entry: 'game/trash-tactics/index.html',
    server: 'game/trash-tactics/hosting/launch.mjs',
    runtime: 'hub-runtime/node.exe',
    tunnel: 'hub-runtime/cloudflared.exe',
  },
};

const usesLocalHost = id => Object.hasOwn(editions, id);

function hasLocalHostPack(id, spec, root) {
  return usesLocalHost(id) && spec?.type === 'static' && spec.hosting === true &&
    Object.entries(editions[id]).every(([name, relative]) =>
      spec[name] === relative && fs.statSync(inDir(root, relative), { throwIfNoEntry: false })?.isFile()
    );
}

function bootstrapUrl(id, mode, value, port) {
  const url = new URL(value);
  if (url.origin !== 'http://127.0.0.1:' + port ||
      url.pathname !== '/' || url.username || url.password || url.search) {
    throw Error('The local game service returned an unexpected launch address.');
  }
  const fragment = new URLSearchParams(url.hash.slice(1));
  const validToken = key => /^[\w-]{43}$/.test(fragment.get(key) || '');
  const valid = id === 'cat'
    ? validToken('desktop') && /^[1-9]\d{0,4}$/.test(fragment.get('control') || '') && Number(fragment.get('control')) <= 65535
    : id === 'bannerlands' ? validToken('desktop')
    : mode === 'host' ? validToken('host') : !url.hash;
  if (!valid) throw Error('The local game service returned invalid hosting controls.');
  if (id === 'trashtactics') url.searchParams.set('hub', '1');
  if (mode === 'host') url.searchParams.set('host', '1');
  if (mode === 'online') {
    if (id === 'bannerlands') {
      fragment.set('view', 'browse');
      url.hash = fragment.toString();
    } else url.searchParams.set('online', '1');
  }
  return url.href;
}

function quickInvite(value) {
  try {
    const url = new URL(value);
    if (url.protocol === 'https:' && !url.port && !url.username && !url.password &&
        /^[a-z0-9-]+\.trycloudflare\.com$/i.test(url.hostname) && url.pathname === '/') return url;
  } catch {}
  return null;
}

function catGuestInvite(value) {
  const url = quickInvite(value);
  if (!url || url.search) return null;
  const fragment = new URLSearchParams(url.hash.slice(1));
  if (!/^[a-z0-9_-]{8,100}$/i.test(fragment.get('room') || '') ||
      [...fragment.keys()].some(key => key !== 'room' && key !== 'invite') ||
      (fragment.has('invite') && !/^[a-z0-9_-]{1,100}$/i.test(fragment.get('invite')))) return null;
  return url.href;
}

function joinGuestInvite(value) {
  const cat = catGuestInvite(value);
  if (cat) return cat;
  const url = quickInvite(value);
  if (!url) return null;
  if (!url.hash && url.searchParams.size === 1 && /^[a-f0-9]{10}$/i.test(url.searchParams.get('room') || '')) return url.href;
  if (url.search) return null;
  const fragment = new URLSearchParams(url.hash.slice(1));
  if (fragment.size === 1 && /^[\w-]{43}$/.test(fragment.get('invite') || '')) return url.href;
  return null;
}

function startLocalHost({ id, mode, root, spec, port, data, onExit }) {
  if (!hasLocalHostPack(id, spec, root)) throw Error('Install this game’s Windows hosting pack.');
  const server = inDir(root, spec.server);
  const tunnel = inDir(root, spec.tunnel);
  const env = { ...process.env };
  const args = ['--no-open'];
  if (id === 'trashtactics') {
    env.TRASH_TACTICS_CLOUDFLARED = tunnel;
    env.TRASH_TACTICS_HOST_PORT = String(port);
    if (mode !== 'host') args.push('--local-only');
  } else {
    args.push('--cloudflared', tunnel, '--data-dir', data);
    if (id === 'cat') args.push('--app-port', String(port), '--no-reuse');
    else args.push('--port', String(port));
  }
  const child = spawn(inDir(root, spec.runtime), [server, ...args], {
    cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  child.stderr.resume();
  const lines = createInterface({ input: child.stdout });
  let stopping = false;
  let exited = false;
  let forceStop;
  const closed = new Promise(resolve => child.once('close', resolve));
  const close = () => {
    if (stopping || exited) return;
    stopping = true;
    if (child.connected) child.send({ type: 'stop' }, () => {});
    forceStop = setTimeout(() => { if (!exited) child.kill(); }, 12_000);
    forceStop.unref();
  };
  const ready = new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      close();
      reject(Error(mode === 'host' && id === 'trashtactics'
        ? 'Trash Tactics could not open its Cloudflare connection. Try Host Lobby again.'
        : 'The local game service took too long to start. Try opening the game again.'));
    }, mode === 'host' && id === 'trashtactics' ? 90_000 : 20_000);
    lines.on('line', line => {
      if (!line.startsWith('{')) return;
      try {
        const { bootstrapUrl: address } = JSON.parse(line);
        if (!address) return;
        const url = bootstrapUrl(id, mode, address, port);
        clearTimeout(timeout);
        resolve({ url, close });
      } catch (error) {
        clearTimeout(timeout);
        close();
        reject(error);
      }
    });
    child.once('error', () => {
      clearTimeout(timeout);
      reject(Error('The local game service could not launch. Repair the game pack in Catnmice Setup.'));
    });
    child.once('close', () => {
      exited = true;
      clearTimeout(timeout);
      clearTimeout(forceStop);
      lines.close();
      reject(Error(mode === 'host' && id === 'trashtactics'
        ? 'Trash Tactics could not start hosting. Try again and check your Cloudflare connection.'
        : 'The local game service could not start. Close other copies of the game and try again.'));
      onExit?.();
    });
  });
  return { ready, close, closed };
}

module.exports = { usesLocalHost, hasLocalHostPack, startLocalHost, catGuestInvite, joinGuestInvite };
