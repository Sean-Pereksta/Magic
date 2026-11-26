'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const { randomInt, randomUUID } = require('node:crypto');
const { monitorEventLoopDelay, performance } = require('node:perf_hooks');
const { setTimeout: delay } = require('node:timers/promises');
const { createGateway, join, status } = require('./gateway.cjs');

const games = new Set(['bannerlands', 'trashtactics', 'cat', 'lastsignal', 'mobamania']);
const wait = (ms, signal) => delay(ms, undefined, { signal });

function readConfig(env) {
  const gameId = env.CATNMICE_GAME_ID, publicPort = Number(env.CATNMICE_PORT);
  if (!games.has(gameId) || !Number.isInteger(publicPort) || publicPort < 1 || publicPort > 65535)
    throw Error('Invalid managed game or port.');
  const packRoot = env.CATNMICE_PACK_ROOT, dataDir = env.CATNMICE_DATA_DIR;
  if (!packRoot || !path.isAbsolute(packRoot) || !dataDir || !path.isAbsolute(dataDir)) throw Error('Missing game pack or data directory.');
  return { gameId, publicPort, packRoot, dataDir, cloudflared: env.CATNMICE_CLOUDFLARED,
    name: String(env.CATNMICE_SERVER_NAME || gameId).replace(/[\x00-\x1f\x7f]/g,'').trim().slice(0, 40) || gameId,
    visibility: env.CATNMICE_VISIBILITY === 'code' ? 'code' : 'public' };
}

async function loadAdapter(packRoot) {
  const folder = path.join(packRoot, 'manager');
  const common = path.join(folder, 'adapter.cjs');
  return fs.existsSync(common) ? require(common) : import(pathToFileURL(path.join(folder, 'adapter.mjs')).href);
}

function listen(server, port) {
  return new Promise((resolve, reject) => {
    const failed = error => { server.off('listening', ready); reject(error); };
    const ready = () => { server.off('error', failed); resolve(); };
    server.once('error', failed).once('listening', ready).listen(port, '0.0.0.0');
  });
}

function listingUnavailable(error) {
  if (error?.code === 'OPERATION_NOT_ALLOWED')
    return 'Enable Anonymous sign-in in Firebase Authentication for Magic Servers. Same-Wi-Fi play is available.';
  if (error?.status === 403)
    return 'Magic Servers denied the listing. Check the active Firestore rules. Same-Wi-Fi play is available.';
  if (error?.status === 429 || error?.code === 'RESOURCE_EXHAUSTED')
    return 'Magic Servers is limiting listing requests. The server will retry automatically. Same-Wi-Fi play is available.';
  return 'The public lobby listing could not be refreshed. Same-Wi-Fi play is available.';
}

async function advertise({ config, directory, hostId, getStatus, getCode, setCode, origin, signal, report }) {
  let lease;
  try {
    while (!signal.aborted) {
      let interval = 25_000;
      try {
        const record = { ...(await getStatus()), name: config.name, visibility: config.visibility, hostId, origin };
        if (signal.aborted) break;
        if (lease) lease = await directory.renew(record, lease);
        if (!lease && !signal.aborted) lease = await directory.reserve(record, getCode());
        if (signal.aborted) break;
        setCode(lease.code);
        report('online', { origin });
      } catch (error) {
        if (signal.aborted) break;
        report('lan-only', { message: listingUnavailable(error) });
        interval = 8000;
      }
      await wait(interval, signal);
    }
  } finally { if (lease) await directory.release(lease); }
}

async function runHost(config, stopSignal, reportToLauncher, dependencies = {}) {
  const controller = new AbortController();
  const signal = AbortSignal.any([stopSignal, controller.signal]);
  const fail = error => controller.abort(error instanceof Error ? error : Error('The native server stopped.'));
  const hostId = randomUUID();
  let code = String(randomInt(10_000)).padStart(4,'0'), adapter, gateway, tunnel;
  const getCode = () => code;
  const setCode = next => { code = next; };
  const startedAt = performance.now();
  const loopDelay = monitorEventLoopDelay({ resolution: 20 });
  let last, tunnelStatus = 'starting', directoryStatus = 'pending';

  async function sampleDiagnostics() {
    loopDelay.enable();
    while (!signal.aborted) {
      const lobby = await gateway.getStatus().catch(() => null);
      if (signal.aborted) break;
      const eventLoopLagMs = loopDelay.count ? Math.max(0, loopDelay.percentile(95) / 1e6 - 20) : undefined;
      loopDelay.reset();
      reportToLauncher({ type: 'game-host-diagnostics', diagnostics: {
        ...tunnel?.diagnostics?.(),
        localStatus: lobby ? 'ready' : 'unavailable',
        tunnelStatus, directoryStatus, eventLoopLagMs,
        phase: lobby?.phase, playerCount: lobby?.players, maxPlayers: lobby?.capacity,
        queuedPlayers: lobby?.queued, uptimeSeconds: (performance.now() - startedAt) / 1000,
      } });
      await wait(3000, signal);
    }
  }

  function report(status, info = {}) {
    const update = { type: 'game-host-status', status, ...info };
    if (config.visibility === 'code') update.code = code;
    const key = JSON.stringify(update);
    if (key !== last) { last = key; reportToLauncher(update); }
  }
  try {
    const module = dependencies.adapterModule || await loadAdapter(config.packRoot);
    adapter = await module.start({ ...config, signal, onFailure: fail });
    signal.throwIfAborted();
    gateway = createGateway({ ...config, adapter, hostId, getCode });
    await listen(gateway.server, config.publicPort);
    gateway.server.on('error', fail);
    signal.throwIfAborted();
    if (!config.cloudflared || !path.isAbsolute(config.cloudflared) || !fs.existsSync(config.cloudflared)) {
      tunnelStatus = 'unavailable';
      directoryStatus = 'disabled';
      report('lan-only', { message: 'The public tunnel is unavailable. Same-Wi-Fi play is available.' });
      void sampleDiagnostics().catch(() => {});
      while (!signal.aborted) await wait(86_400_000, signal);
      return;
    }
    const directory = (dependencies.directoryFactory || require('./community-directory.cjs').createFirebaseDirectory)({
      dataDir: config.dataDir, listingCollection: 'managedGameDirectories/' + config.gameId + '/lobbies',
      joinPath: join, listingExtras: record => ({ hostId: { stringValue: record.hostId }, joinable: { booleanValue: record.joinable } })
    });
    void sampleDiagnostics().catch(() => {});
    let retry = 1000;
    while (!signal.aborted) {
      tunnelStatus = 'starting';
      directoryStatus = 'pending';
      report('starting', { message: 'Opening the public game connection.' });
      try {
        tunnel = (dependencies.tunnelFactory || require('./community-tunnel.cjs').startQuickTunnel)({ executable: config.cloudflared, port: config.publicPort,
          probeId: hostId, probePath: status, matchesProbe: info => info?.game === config.gameId && info.hostId === hostId });
        const close = () => tunnel?.close();
        signal.addEventListener('abort', close, { once: true });
        try {
          const origin = await tunnel.ready;
          tunnelStatus = 'ready';
          retry = 1000;
          await advertise({ config, directory, hostId, getStatus: gateway.getStatus, getCode, setCode, origin,
            signal: AbortSignal.any([signal, tunnel.signal]),
            report(state, info) {
              directoryStatus = state === 'online' ? 'listed' : 'unavailable';
              report(state, info);
            },
          });
        } finally { signal.removeEventListener('abort', close); }
      } catch { /* A tunnel interruption must keep the game running on the local network. */ }
      finally { tunnel?.close(); tunnel = undefined; }
      if (!signal.aborted) {
        tunnelStatus = 'reconnecting';
        directoryStatus = 'pending';
        report('lan-only', { message: 'The public tunnel is reconnecting. Same-Wi-Fi play is available.' });
        await wait(retry, signal);
        retry = Math.min(30_000, retry * 2);
      }
    }
    signal.throwIfAborted();
  } finally {
    controller.abort();
    loopDelay.disable();
    tunnel?.close();
    try { await gateway?.close(); } finally { await adapter?.close(); }
  }
}

async function main() {
  const config = readConfig(process.env), stop = new AbortController();
  fs.mkdirSync(config.dataDir, { recursive: true });
  const halt = () => stop.abort();
  process.on('SIGINT', halt).on('SIGTERM', halt).on('disconnect', halt);
  process.on('message', msg => { if (msg?.type === 'stop') halt(); });
  const send = data => { if (process.connected) process.send(data, () => {}); };
  let delay = 1000;
  while (!stop.signal.aborted) {
    try {
      await runHost(config, stop.signal, state => { if (state.status === 'online') delay = 1000; send(state); });
    } catch (error) {
      if (stop.signal.aborted) break;
      if (error.code === 'EADDRINUSE') { process.exitCode = 98; break; }
      send({ type:'game-host-status', status:'starting', resetDiagnostics:true,
        message:'The native game server stopped. Restarting the lobby.' });
    }
    if (!stop.signal.aborted) await wait(delay, stop.signal).catch(() => {});
    delay = Math.min(30_000, delay * 2);
  }
  if (process.connected) process.disconnect();
}

if (require.main === module) main().catch(() => { process.exitCode = 1; if (process.connected) process.disconnect(); });
module.exports = { runHost, advertise, readConfig };
