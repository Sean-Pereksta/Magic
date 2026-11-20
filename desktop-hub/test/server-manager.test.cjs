'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { EventEmitter } = require('node:events');
const { test } = require('node:test');
const { createServerManager, hasServerManagerPack } = require('../desktop/server-manager.cjs');

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
  const options = {
    dataDir, packFor: () => pack, onChange: () => { changes++; }, onExit: () => {},
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
  manager.start(game, { name: 'Local cup', visibility: 'code' });
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
  child.emit('message', { type: 'rally-host-status', status: 'lan-only' });
  assert.equal(manager.status(game).code, undefined);
  await manager.stop(game.id);
  assert.equal(manager.busy(), false);
  assert.equal(manager.status(game).status, 'offline');
  assert.equal(createServerManager(options).status(game).name, 'Local cup');
  assert.equal(createServerManager(options).status(game).visibility, 'code');

  manager.start(game, { visibility: 'public' });
  const publicChild = processes[1].child;
  publicChild.emit('message', { type: 'rally-host-status', status: 'online', code: '1234', origin: 'https://race-def.trycloudflare.com' });
  assert.equal(manager.status(game).link, 'https://race-def.trycloudflare.com/?room=join');
  assert.equal(manager.status(game).code, undefined);
  await manager.stopAll();
  assert.equal(manager.busy(), false);
  assert.ok(changes > 5);
});
