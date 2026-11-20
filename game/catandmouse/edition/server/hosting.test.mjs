import assert from 'node:assert/strict';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { STARTER_LOADOUT } from '../simulation.mjs';
import { PROTOCOL_VERSION } from '../directory-config.mjs';
import { createMatchServer } from './match-server.mjs';
import { readLobbySettings } from './hosting-config.mjs';

const options = {
  roomName: 'Flour Watch', hostName: 'Pip', map: 'flour-mill',
  difficulty: 'normal', capacity: 3, visibility: 'public'
};

test('starting wave defaults for older host options and admits only supported tiers', () => {
  assert.equal(readLobbySettings(options).startingWave, 1);
  for (const startingWave of [1, 5, 10]) assert.equal(readLobbySettings({ ...options, startingWave }).startingWave, startingWave);
  assert.throws(() => readLobbySettings({ ...options, startingWave: 50 }), /starting wave/);
  assert.throws(() => readLobbySettings({ ...options, startingWave: '5' }), /starting wave/);
});

async function peer(match) {
  const socket = new WebSocket(match.url.replace(/^http:/, 'ws:') + '/socket');
  const history = [];
  socket.addEventListener('message', event => history.push(JSON.parse(event.data)));
  await new Promise((resolve, reject) => {
    socket.addEventListener('open', resolve, { once: true });
    socket.addEventListener('error', reject, { once: true });
  });
  return {
    send: (type, values = {}) => socket.send(JSON.stringify({ type, ...values })),
    join(values = {}) {
      this.send('join', { roomId: match.id, version: PROTOCOL_VERSION, name: 'Guest', ...values });
    },
    close: () => socket.close(),
    async next(type, predicate = () => true, timeout = 5_000) {
      const until = Date.now() + timeout;
      while (Date.now() < until) {
        const index = history.findIndex(message => message.type === type && predicate(message));
        if (index !== -1) return history.splice(index, 1)[0];
        await delay(10);
      }
      throw new Error('Missing ' + type + ': ' + JSON.stringify(history.slice(-3)));
    }
  };
}

test('normal start locks admission, checks loading and resynchronizes a reclaimed seat', { timeout: 12_000 }, async t => {
  const match = await createMatchServer({ ...options, startingWave: 5 });
  t.after(() => match.close());
  assert.equal(match.getLobby().startingWave, 5);
  const host = await peer(match);
  const guestA = await peer(match);
  const guestB = await peer(match);
  const guestC = await peer(match);
  host.join({ hostToken: match.hostToken });
  await host.next('joined');
  guestA.join({ roomId: 'different-room' });
  await guestA.next('error', message => message.code === 'room-changed');
  guestA.join({ version: 'incompatible-version' });
  await guestA.next('error', message => message.code === 'version');
  guestA.join();
  const seatA = await guestA.next('joined');
  host.send('start');
  await host.next('error', message => message.code === 'not-ready');

  guestB.join();
  guestC.join();
  const finalSeat = await Promise.race([
    guestB.next('joined').then(() => ({ accepted: guestB, rejected: guestC })),
    guestC.next('joined').then(() => ({ accepted: guestC, rejected: guestB }))
  ]);
  await finalSeat.rejected.next('error', message => message.code === 'full');
  for (const player of [host, guestA, finalSeat.accepted]) player.send('ready', { ready: true, loadout: STARTER_LOADOUT });
  await host.next('lobby', message => message.lobby.canStart);
  host.send('start');
  host.send('start', { force: true });
  const prepared = await Promise.all([host, guestA, finalSeat.accepted].map(player => player.next('prepare')));
  assert.equal(new Set(prepared.map(message => message.matchId)).size, 1);
  assert.ok(prepared.every(message => message.startingWave === 5));
  finalSeat.rejected.join();
  await finalSeat.rejected.next('error', message => message.code === 'started');
  for (const player of [host, guestA, finalSeat.accepted]) player.send('loaded', { matchId: prepared[0].matchId });
  const snapshot = await guestA.next('snapshot');
  assert.equal(snapshot.state.matchId, prepared[0].matchId);
  assert.equal(snapshot.state.players.length, 3);
  assert.equal(snapshot.state.startingWave, 5);
  assert.equal(snapshot.state.wave, 4);
  assert.ok(snapshot.state.enemies.some(enemy => enemy.type === 'cat'));
  const delta = await guestA.next('delta');
  assert.equal(delta.baseTick, snapshot.state.tick);
  guestA.close();
  const reconnected = await peer(match);
  reconnected.join({ token: seatA.token });
  assert.equal((await reconnected.next('joined')).playerId, seatA.playerId);
  const resynced = await reconnected.next('snapshot');
  assert.equal(resynced.state.matchId, snapshot.state.matchId);
  assert.ok(resynced.state.tick >= delta.tick);
  const live = await reconnected.next('delta');
  assert.equal(live.baseTick, resynced.state.tick);
});

test('a forced start admits connected unready players with complete starter loadouts', { timeout: 8_000 }, async t => {
  const match = await createMatchServer({ ...options, visibility: 'invite', capacity: 4 });
  t.after(() => match.close());
  const host = await peer(match);
  const guest = await peer(match);
  host.join({ hostToken: match.hostToken });
  await host.next('joined');
  guest.join();
  await guest.next('error', message => message.code === 'invite-required');
  guest.join({ invite: match.invite });
  await guest.next('joined');
  guest.send('ready', { ready: false, loadout: ['wall'] });
  guest.send('start', { force: true });
  await guest.next('error', message => message.code === 'host-only');
  host.send('start', { force: true });
  const map = await host.next('prepare');
  await guest.next('prepare');
  host.send('loaded', { matchId: map.matchId });
  guest.send('loaded', { matchId: map.matchId });
  const room = await guest.next('lobby', message => message.lobby.phase === 'countdown');
  assert.equal(room.lobby.players.length, 2);
  assert.equal(room.lobby.players[1].loadout.length, 8);
  const snapshot = await guest.next('snapshot');
  assert.equal(snapshot.state.players.length, 2);
});

test('managed room transfers leadership and reopens after the last player leaves a match', { timeout: 10_000 }, async t => {
  const match = await createMatchServer(options, { managed: true });
  t.after(() => match.close());
  const original = match.id;
  assert.equal(match.getLobby().players.length, 0);
  const first = await peer(match);
  first.join({ name: 'Pip' });
  const leader = await first.next('joined');
  assert.equal(match.getLobby().players.find(player => player.id === leader.playerId).isHost, true);
  const second = await peer(match);
  second.join();
  const successor = await second.next('joined');
  second.send('start', { force: true });
  await second.next('error', message => message.code === 'host-only');
  first.send('leave');
  await second.next('lobby', ({ lobby }) => lobby.players.length === 1 && lobby.players[0].isHost);
  second.send('start', { force: true });
  const prepared = await second.next('prepare');
  second.send('loaded', { matchId: prepared.matchId });
  await second.next('snapshot');
  second.send('leave');
  for (let attempt = 0; match.id === original && attempt < 100; attempt++) await delay(25);
  assert.notEqual(match.id, original);
  assert.equal(match.getLobby().phase, 'waiting');
  assert.equal(match.getLobby().players.length, 0);
  const fresh = await peer(match);
  fresh.join({ roomId: original, token: successor.token });
  await fresh.next('error', message => message.code === 'room-changed');
  fresh.join();
  assert.notEqual((await fresh.next('joined')).playerId, successor.playerId);
});

test('force start excludes a guest who misses the bounded loading window', { timeout: 30_000 }, async t => {
  const match = await createMatchServer(options);
  t.after(() => match.close());
  const host = await peer(match);
  const guest = await peer(match);
  host.join({ hostToken: match.hostToken });
  await host.next('joined');
  guest.join();
  await guest.next('joined');
  host.send('start', { force: true });
  const map = await host.next('prepare');
  await guest.next('prepare');
  host.send('loaded', { matchId: map.matchId });
  await guest.next('error', message => message.code === 'load-timeout', 23_000);
  const started = await host.next('snapshot');
  assert.equal(started.state.players.length, 1);
});
