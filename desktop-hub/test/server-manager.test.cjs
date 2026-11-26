'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { EventEmitter } = require('node:events');
const { test } = require('node:test');
const { createServerManager, hasServerManagerPack } = require('../desktop/server-manager.cjs');

test('managed games use their own pack, port, settings and join endpoint', async t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'catnmice-managed-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const fixtures = [
    ['bannerlands','game/index.html','game/source/desktop/launcher.mjs','runtime','adapter.cjs','game/source/multiplayer-server.mjs'],
    ['trashtactics','game/trash-tactics/index.html','game/trash-tactics/hosting/launch.mjs','hub-runtime','adapter.mjs','game/trash-tactics/server.mjs'],
    ['cat','game/catandmouse/edition/index.html','game/catandmouse/edition/server/launcher.mjs','hub-runtime','adapter.mjs','game/catandmouse/edition/server/match-server.mjs'],
    ['lastsignal','Last Signal.exe','manager/supervisor.cjs','Hosting','adapter.mjs','Mobile/index.html'],
    ['mobamania','index.html','moba-mania/server/host.mjs','hub-runtime','adapter.mjs','moba-mania/online-match.mjs'],
  ];
  const children = new Map(), packs = new Map(), ports = new Set();
  const manager = createServerManager({
    dataDir: path.join(root, 'private'), packFor: id => packs.get(id), onChange() {}, onExit() {},
    spawnProcess(executable, args, { cwd, env }) {
      assert.equal(cwd, env.CATNMICE_PACK_ROOT);
      assert.equal(executable, path.join(cwd, packs.get(env.CATNMICE_GAME_ID).spec.runtime));
      assert.deepEqual(args, [path.join(cwd, 'manager', 'supervisor.cjs')]);
      assert.equal(env.CATNMICE_DATA_DIR, path.join(root,'private','servers',env.CATNMICE_GAME_ID));
      assert.equal(env.CATNMICE_CLOUDFLARED, path.join(cwd, packs.get(env.CATNMICE_GAME_ID).spec.tunnel));
      if (env.CATNMICE_GAME_ID === 'mobamania') assert.equal(env.CATNMICE_PORT, '8796');
      ports.add(env.CATNMICE_PORT);
      const child = new EventEmitter();
      child.connected = true;
      child.send = (message, callback) => { assert.equal(message.type, 'stop'); callback(); child.emit('close', 0); };
      children.set(env.CATNMICE_GAME_ID, child);
      return child;
    },
  });
  for (const [id,entry,server,folder,adapter,gameFile] of fixtures) {
    const packRoot = path.join(root, id);
    const spec = { type: id === 'lastsignal' ? 'native-windows' : 'static', hosting: true,
      manager: 'manager/supervisor.cjs', entry, server, runtime: folder+'/node.exe', tunnel: folder+'/cloudflared.exe' };
    for (const rel of [entry, server, spec.runtime, spec.tunnel, spec.manager, gameFile, 'manager/'+adapter,
      'manager/gateway.cjs','manager/community-tunnel.cjs','manager/community-directory.cjs']) {
      const file = path.join(packRoot, rel);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, '');
    }
    assert.ok(hasServerManagerPack(id, spec, packRoot));
    assert.equal(hasServerManagerPack(id, { ...spec, manager: undefined }, packRoot), false);
    packs.set(id, { root: packRoot, spec });
    const game = { id, title: id, offline: spec };
    const visibility = id === 'lastsignal' ? 'code' : 'public';
    manager.start(game, { name: id+' community', visibility });
    children.get(id).emit('message', { type:'game-host-status', status:'online', origin:'https://'+id+'.trycloudflare.com', code:'0007' });
    assert.equal(manager.status(game).link, visibility === 'public' ? 'https://'+id+'.trycloudflare.com/__catnmice/join' : undefined);
    assert.equal(manager.status(game).code, visibility === 'code' ? '0007' : undefined);
    assert.equal(new URL(manager.localLink(id)).pathname, '/__catnmice/join');
    assert.equal(manager.diagnostics(game).diagnosticsState, 'starting');
    const maxPlayers = id === 'mobamania' ? 6 : 4;
    children.get(id).emit('message', { type:'game-host-diagnostics', diagnostics: {
      playerCount: 3, maxPlayers, tunnelRttMs: 121.4, directoryStatus: 'listed',
    } });
    assert.equal(manager.diagnostics(game).diagnosticsState, 'live');
    assert.equal(manager.diagnostics(game).metrics.playerCount, 3);
    assert.equal(manager.diagnostics(game).metrics.maxPlayers, maxPlayers);
    assert.equal(manager.diagnostics(game).metrics.tunnelRttMs, 121.4);
    assert.equal(manager.diagnostics(game).metrics.directoryStatus, 'listed');
    children.get(id).emit('message', { type:'game-host-status', status:'starting', resetDiagnostics:true });
    assert.equal(manager.diagnostics(game).metrics, undefined);
  }
  assert.equal(ports.size, fixtures.length);
  children.get('lastsignal').emit('message', { type:'game-host-status',status:'lan-only',code:'0008' });
  assert.equal(manager.status({id:'lastsignal'}).code,'0008');
  await manager.stop('cat');
  assert.equal(manager.busy('cat'), false);
  assert.equal(manager.busy('bannerlands'), true);
  await manager.stopAll();
  assert.equal(manager.busy(), false);
});

test('the launcher runs the packaged Acorn server and exposes only its assigned join details', async t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'catnmice-rally-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const packRoot = path.join(root, 'pack');
  const dataDir = path.join(root, 'private');
  const spec = {
    type: 'static', hosting: true, entry: 'index.html', server: 'server/lobby-supervisor.cjs',
    runtime: 'server/runtime/node.exe', tunnel: 'server/runtime/cloudflared.exe',
  };
  for (const relative of [spec.entry, spec.server, spec.runtime, spec.tunnel, 'server/host.cjs',
    'server/community-host.cjs', 'server/community-directory.cjs', 'server/community-tunnel.cjs',
    'server/community-poll.cjs','server/race-queue.cjs', 'race-sim.js', 'network.js']) {
    const file = path.join(packRoot, relative);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, '');
  }
  const pack = { root: packRoot, spec };
  const game = { id: 'acornrally', title: 'Acorn Rally', offline: spec };
  assert.equal(hasServerManagerPack(game.id, spec, packRoot), true);
  assert.equal(hasServerManagerPack(game.id, { ...spec, tunnel: '../outside.exe' }, packRoot), false);

  const processes = [];
  let changes = 0;
  let clock = 1_000_000;
  const options = {
    dataDir, packFor: () => pack, onChange: () => { changes++; }, onExit: () => {},
    now: () => clock,
    spawnProcess(executable, args, spawnOptions) {
      const child = new EventEmitter();
      child.connected = true;
      child.send = (message, callback) => {
        assert.deepEqual(message, { type: 'stop' });
        callback();
        child.emit('close', 0);
      };
      processes.push({ child, executable, args, env: spawnOptions.env });
      return child;
    },
  };
  const manager = createServerManager(options);
  assert.equal(manager.diagnostics(game).diagnosticsState, 'offline');
  assert.equal(manager.diagnostics(game).uptimeSeconds, undefined);
  manager.start(game, { name: 'Local cup', visibility: 'code' });
  assert.equal(manager.diagnostics(game).diagnosticsState, 'starting');
  assert.equal(manager.diagnostics(game).uptimeSeconds, 0);
  assert.throws(() => manager.start(game, {}), /already has/);
  const { child, env, executable, args } = processes[0];
  assert.equal(executable, path.join(packRoot, spec.runtime));
  assert.deepEqual(args, [path.join(packRoot, spec.server)]);
  assert.equal(env.ACORN_RALLY_CLOUDFLARED, path.join(packRoot, spec.tunnel));
  assert.equal(env.ACORN_RALLY_DATA_DIR, path.join(dataDir, 'servers', game.id));
  assert.equal(env.ACORN_RALLY_COMMUNITY, '1');
  assert.equal(env.ACORN_RALLY_VISIBILITY, 'code');
  assert.equal(env.ACORN_RALLY_SERVER_NAME, 'Local cup');
  assert.equal(env.ACORN_RALLY_PORT, '8791');
  assert.equal(manager.status(game).status, 'starting');
  child.emit('message', { type: 'rally-host-status', status: 'online', code: '0042', origin: 'https://untrusted.example' });
  assert.equal(manager.status(game).code, undefined);
  child.emit('message', { type: 'rally-host-status', status: 'online', code: '0042', origin: 'https://race-abc.trycloudflare.com' });
  assert.equal(manager.status(game).code, '0042');
  assert.equal(manager.status(game).link, undefined);
  assert.equal(manager.localLink(game.id), 'http://127.0.0.1:8791/?room=join');
  assert.equal(manager.diagnostics(game).metrics, undefined);
  // Older hosting packs dropped the telemetry at the supervisor. The launcher
  // must stop saying it is loading and expose that missing stream after startup.
  clock += 15_000;
  assert.equal(manager.diagnostics(game).diagnosticsState, 'unavailable');
  assert.equal(manager.diagnostics(game).uptimeSeconds, 15);
  const changesBeforeSample = changes;
  child.emit('message', { type: 'rally-host-diagnostics', diagnostics: {
    tunnelRttMs: 82.24, tunnelLatencyAgeMs: 1200, tunnelJitterMs: -1, eventLoopLagMs: Infinity,
    tunnelStatus: 'ready', directoryStatus: 'listed', expectedPlayers: 2, readyPlayers: 1, phase: 'loading',
    playerCount: 2, connectedPlayers: 1, maxPlayers: 8, queuedPlayers: 3, uptimeSeconds: 14.2,
    code: '0042', origin: 'https://race-abc.trycloudflare.com',
    players: [{ name: 'Player', loadState: 'ready', loadMs: 1800, rttMs: 105, snapshotGapPct: 2.5,
      fps: 60, transport: 'websocket', connected: true, latencyAgeMs: 300, id: 'private-id' },
      ...Array.from({ length: 9 }, () => ({ name: 'x'.repeat(60), loadState: '<invalid>', snapshotGapPct: 101, transport: 'other' }))],
  } });
  const { status, metrics } = manager.diagnostics(game);
  assert.equal(status, 'online');
  assert.equal(changes, changesBeforeSample);
  assert.equal(metrics.tunnelRttMs, 82.2);
  assert.equal(metrics.tunnelLatencyAgeMs, 1200);
  assert.equal(metrics.tunnelStatus, 'ready');
  assert.equal(metrics.phase, 'loading');
  assert.equal(metrics.readyPlayers, 1);
  assert.equal(metrics.expectedPlayers, 2);
  assert.equal(metrics.playerCount, 2);
  assert.equal(metrics.connectedPlayers, 1);
  assert.equal(metrics.maxPlayers, 8);
  assert.equal(metrics.queuedPlayers, 3);
  assert.equal(metrics.uptimeSeconds, 14.2);
  assert.equal(metrics.sampledAt, clock);
  assert.equal(manager.diagnostics(game).diagnosticsState, 'live');
  assert.equal(manager.diagnostics(game).sampleAgeMs, 0);
  for (const key of ['tunnelJitterMs','eventLoopLagMs','code','origin']) assert.equal(metrics[key], undefined);
  assert.equal(metrics.players.length, 8);
  assert.deepEqual(metrics.players[0], { name: 'Player', loadState: 'ready', loadMs: 1800, rttMs: 105,
    snapshotGapPct: 2.5, fps: 60, transport: 'websocket', connected: true, latencyAgeMs: 300 });
  assert.deepEqual(metrics.players[1], { name: 'x'.repeat(40) });
  clock += 12_001;
  assert.equal(manager.diagnostics(game).diagnosticsState, 'delayed');
  assert.equal(manager.diagnostics(game).sampleAgeMs, 12_001);
  assert.equal(manager.diagnostics(game).metrics, metrics);
  for (const invalid of [null, [], {}, { origin: 'private', playerCount: 99, uptimeSeconds: -1 }]) {
    child.emit('message', { type: 'rally-host-diagnostics', diagnostics: invalid });
  }
  assert.equal(manager.diagnostics(game).metrics, metrics);
  assert.equal(manager.diagnostics(game).diagnosticsState, 'delayed');
  child.emit('message', { type: 'rally-host-diagnostics', diagnostics: {
    playerCount: 0, connectedPlayers: 0, maxPlayers: 8, queuedPlayers: 0, uptimeSeconds: 27, players: [],
  } });
  assert.equal(manager.diagnostics(game).diagnosticsState, 'live');
  assert.equal(manager.diagnostics(game).sampleAgeMs, 0);
  assert.equal(manager.diagnostics(game).metrics.playerCount, 0);
  assert.equal(manager.diagnostics(game).metrics.queuedPlayers, 0);
  assert.deepEqual(manager.diagnostics(game).metrics.players, []);
  assert.equal(manager.diagnostics(game).metrics.tunnelRttMs, undefined);
  child.emit('message', { type: 'rally-host-status', status: 'offline' });
  assert.equal(manager.diagnostics(game).diagnosticsState, 'offline');
  assert.equal(manager.diagnostics(game).metrics, undefined);
  child.emit('message', { type: 'rally-host-status', status: 'lan-only' });
  assert.equal(manager.status(game).code, undefined);
  assert.equal(manager.diagnostics(game).status, 'lan-only');
  assert.equal(manager.diagnostics(game).metrics, undefined, 'A restarted host cannot reuse the old roster');
  await manager.stop(game.id);
  assert.equal(manager.busy(), false);
  assert.equal(manager.status(game).status, 'offline');
  assert.equal(manager.diagnostics(game).metrics, undefined);
  assert.equal(manager.diagnostics(game).diagnosticsState, 'offline');
  assert.equal(createServerManager(options).status(game).name, 'Local cup');
  assert.equal(createServerManager(options).status(game).visibility, 'code');

  manager.start(game, { visibility: 'public' });
  const publicChild = processes[1].child;
  assert.equal(manager.diagnostics(game).metrics, undefined);
  assert.equal(manager.diagnostics(game).diagnosticsState, 'starting');
  assert.equal(manager.diagnostics(game).uptimeSeconds, 0);
  child.emit('message', { type: 'rally-host-diagnostics', diagnostics: { playerCount: 8 } });
  assert.equal(manager.diagnostics(game).metrics, undefined);
  publicChild.emit('message', { type: 'rally-host-status', status: 'online', code: '1234', origin: 'https://race-def.trycloudflare.com' });
  assert.equal(manager.status(game).link, 'https://race-def.trycloudflare.com/?room=join');
  assert.equal(manager.status(game).code, undefined);
  await manager.stopAll();
  assert.equal(manager.busy(), false);
  assert.ok(changes > 5);
});
