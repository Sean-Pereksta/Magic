import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { access, readFile } from 'node:fs/promises';
import path from 'node:path';
import { createInterface } from 'node:readline';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';

if (!process.argv[2]) throw Error('Usage: node build/trash-host-smoke.mjs <installed launcher directory>');

async function waitFor(read, description, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const value = read();
    if (value) return value;
    await delay(20);
  }
  throw Error('Timed out waiting for ' + description);
}

async function startPack(t) {
  const content = path.resolve(process.argv[2], 'resources', 'app', 'content');
  const catalog = JSON.parse(await readFile(path.join(content, 'catalog.json'), 'utf8'));
  const spec = catalog.games.find(game => game.id === 'trashtactics')?.offline;
  assert.equal(spec?.hosting, true, 'Installer must include the Trash Tactics hosting edition');
  const pack = path.join(content, 'packs', 'trashtactics');
  await Promise.all([spec.entry, spec.server, spec.runtime, spec.tunnel].map(file => access(path.join(pack, file))));

  const child = fork(path.join(pack, spec.server), ['--no-open', '--local-only'], {
    execPath: path.join(pack, spec.runtime),
    execArgv: [],
    windowsHide: true,
    stdio: ['ignore', 'pipe', 'ignore', 'ipc'],
    env: { ...process.env, TRASH_TACTICS_HOST_PORT: '0' }
  });
  const exit = once(child, 'exit');
  exit.catch(() => {});
  t.after(async () => {
    const fallback = setTimeout(() => child.kill(), 5000);
    try {
      if (child.connected) child.send({ type: 'stop' }, () => {});
      assert.deepEqual(await exit, [0, null], 'Installed Trash Tactics host should exit cleanly');
    } finally {
      clearTimeout(fallback);
    }
  });
  let ready, error;
  child.on('error', failure => { error = failure; });
  createInterface(child.stdout).on('line', line => {
    if (line.startsWith('{')) ready = JSON.parse(line).bootstrapUrl;
  });
  const url = new URL(await waitFor(() => {
    if (error) throw error;
    if (child.exitCode !== null) throw Error('Installed Trash Tactics launcher stopped before becoming ready');
    return ready;
  }, 'installed Trash Tactics launcher', 18000));
  assert.equal(url.protocol, 'http:');
  assert.equal(url.hostname, '127.0.0.1');
  return url;
}

async function player(t, origin) {
  const endpoint = new URL('/api/play', origin);
  endpoint.protocol = 'ws:';
  const socket = new WebSocket(endpoint);
  const received = [];
  let failure;
  socket.addEventListener('message', event => received.push(JSON.parse(event.data)));
  socket.addEventListener('error', event => { failure = event.error ?? Error('WebSocket could not reach the installed host'); });
  t.after(() => socket.close());
  await waitFor(() => {
    if (failure) throw failure;
    return socket.readyState === WebSocket.OPEN;
  }, 'player connection');
  return {
    send: (type, value) => socket.send(JSON.stringify({ type, ...value })),
    next: (type, matches = () => true) => waitFor(() => {
      if (failure) throw failure;
      const problem = received.find(message => message.type === 'error');
      if (problem) throw Error(problem.message);
      const index = received.findIndex(message => message.type === type && matches(message));
      return index === -1 ? null : received.splice(index, 1)[0];
    }, 'player ' + type)
  };
}

test('installed Trash Tactics seats a host and guest and starts the same match', { timeout: 30000 }, async t => {
  const origin = await startPack(t);
  const health = await fetch(new URL('/api/health', origin), { signal: AbortSignal.timeout(5000) });
  assert.equal(health.status, 200);
  assert.equal((await health.json()).ok, true);
  const host = await player(t, origin);
  const guest = await player(t, origin);
  const [hostHello, guestHello] = await Promise.all([host.next('hello'), guest.next('hello')]);
  assert.equal(hostHello.version, guestHello.version);

  host.send('create', { name: 'Smoke Host', title: 'Installed test', map: 'desktop' });
  const home = await host.next('seat');
  assert.equal(home.team, 0);
  guest.send('join', { room: home.room, name: 'Smoke Guest' });
  const away = await guest.next('seat');
  assert.equal(away.team, 1);
  assert.equal(away.room, home.room);
  await host.next('room', value => value.seats.every(seat => seat?.connected));
  host.send('start');

  const players = [host, guest];
  const loading = await Promise.all(players.map(client => client.next('load')));
  assert.ok(loading.every(message => message.map === 'desktop'));
  for (const client of players) client.send('ready');
  const started = await Promise.all(players.map(client => client.next('start')));
  assert.ok(started.every(message => message.map === 'desktop'));
  const states = await Promise.all(players.map(client => client.next('state')));
  for (const [team, state] of states.entries()) {
    assert.equal(state.teams.length, 2);
    assert.ok(state.entities.some(entity => entity.team === team));
  }
});
