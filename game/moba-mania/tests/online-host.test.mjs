import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import http from 'node:http';
import { connect } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as wait } from 'node:timers/promises';
import { test } from 'node:test';
import { WebSocket } from 'ws';
import gatewayModule from '../../server-manager/host/gateway.cjs';
import { createMobaServer } from '../server/host.mjs';

const origin = 'http://moba.test';
const request = http.request.bind(http);

async function listen(server) {
  const id = 'moba-' + randomUUID();
  const pipe = process.platform === 'win32' ? '\\\\.\\pipe\\' + id : join(tmpdir(), id + '.sock');
  server.listen(pipe);
  await once(server, 'listening');
  return pipe;
}

function call(pipe, path, { method = 'GET', headers, body } = {}) {
  return new Promise((resolve, reject) => {
    const req = request({ socketPath: pipe, path, method, headers: { Host: 'moba.test', ...headers } }, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', bytes => { data += bytes; });
      res.once('end', () => resolve({ status: res.statusCode, headers: res.headers, data }));
    });
    req.once('error', reject);
    req.end(body);
  });
}

function openPeer(pipe, cookie) {
  return new WebSocket(origin.replace(/^http:/, 'ws:') + '/api/moba/online', {
    origin, headers: cookie ? { Cookie: cookie } : {}, createConnection: () => connect(pipe)
  });
}

async function peerAt(t, pipe, cookie) {
  const socket = openPeer(pipe, cookie);
  t.after(() => socket.terminate());
  const packets = [];
  socket.on('message', raw => packets.push(JSON.parse(raw)));
  await once(socket, 'open');
  return {
    socket, send: packet => socket.send(JSON.stringify(packet)),
    async next(type, matches = () => true) {
      for (let elapsed = 0; elapsed < 3500; elapsed += 15) {
        const index = packets.findIndex(packet => packet.type === type && matches(packet));
        if (index !== -1) return packets.splice(index, 1)[0];
        await wait(15);
      }
      assert.fail(`Did not receive ${type}; received ${packets.map(packet => packet.type).join(', ')}`);
    }
  };
}

test('managed lobby selects teams and heroes, fills with bots and live-joins a bot seat', async t => {
  const host = createMobaServer({ name: 'River server' });
  const hostPipe = await listen(host.server);
  const gateway = gatewayModule.createGateway({ adapter: { ...host, origin: 'http://127.0.0.1:4567' }, gameId: 'mobamania', hostId: 'test',
    name: 'River server', visibility: 'code', getCode: () => '0642' });
  const gatewayPipe = await listen(gateway.server);
  t.after(async () => { await gateway.close(); await host.close(); });
  // Exercise proxy upgrades with real transports even where the runner blocks TCP.
  t.mock.method(http, 'request', (options, callback) => request({
    ...options, ...(Number(options.port) === 4567 ? { socketPath: hostPipe } : {})
  }, callback));

  const declined = openPeer(gatewayPipe);
  const [decline] = await once(declined, 'error');
  assert.match(decline.message, /403/);
  const admitted = await call(gatewayPipe, '/__catnmice/access', {
    method: 'POST', headers: { 'Content-Type': 'application/json', Origin: origin }, body: JSON.stringify({ code: '0642' })
  });
  assert.equal(admitted.status, 204);
  const cookie = admitted.headers['set-cookie'][0].split(';')[0];
  const invitation = await call(gatewayPipe, '/__catnmice/join?name=Lyn', { headers: { Cookie: cookie } });
  assert.equal(invitation.status, 302);
  assert.equal(invitation.headers.location, '/?online=1&hosted=1&name=Lyn');
  const entry = await call(gatewayPipe, invitation.headers.location, { headers: { Cookie: cookie } });
  assert.equal(entry.status, 200);
  assert.match(entry.data, /moba-mania\/ui.mjs/);
  const asset = await call(gatewayPipe, '/game/moba-mania/ui.mjs', { method: 'HEAD', headers: { Cookie: cookie } });
  assert.match(asset.headers['content-type'], /^text\/javascript/);

  const first = await peerAt(t, gatewayPipe, cookie);
  const second = await peerAt(t, gatewayPipe, cookie);
  assert.equal(first.socket.extensions, 'permessage-deflate');
  const { state: empty } = await first.next('lobby');
  assert.deepEqual(empty.teams.map(team => team.length), [3, 3]);
  first.send({ type: 'join', name: 'Lyn', team: 0, slot: 0, hero: 'arc' });
  const firstId = (await first.next('welcome')).playerId;
  second.send({ type: 'join', name: 'Pip', team: 0, slot: 1, hero: { toString: null } });
  assert.equal((await second.next('error')).code, 'invalid-hero');
  second.send({ type: 'join', name: 'Pip', team: 0, slot: 1, hero: 'angel' });
  const secondIdentity = await second.next('welcome');
  const secondId = secondIdentity.playerId;
  second.send({ type: 'select', team: 1, slot: 2, hero: 'bulwark' });
  const { state: selection } = await first.next('lobby', ({ state }) => state.teams[1][2].playerId === secondId);
  assert.equal(selection.teams[1][2].hero, 'bulwark');
  assert.equal(selection.teams[0][0].hero, 'arc');
  assert.equal(selection.hostId, firstId);
  second.send({ type: 'start' });
  assert.equal((await second.next('error')).code, 'cannot-start');

  first.send({ type: 'start' });
  const { state: playing } = await first.next('lobby', ({ state }) => state.phase === 'playing');
  assert.equal(playing.teams.flat().filter(seat => seat.bot).length, 4);
  assert.ok(playing.teams.flat().every(seat => seat.hero));
  const frame1 = await first.next('snapshot');
  const frame2 = await second.next('snapshot');
  assert.equal(frame1.state.matchId, playing.matchId);
  assert.notEqual(frame1.state.playerId, frame2.state.playerId);
  assert.equal(frame1.state.units.find(unit => unit.id === frame1.state.playerId).heroId, 'arc');
  const live = JSON.parse((await call(gatewayPipe, '/__catnmice/status')).data);
  assert.deepEqual({ phase: live.phase, players: live.players, capacity: live.capacity, joinable: live.joinable },
    { phase: 'playing', players: 2, capacity: 6, joinable: true });

  second.socket.close();
  await first.next('lobby', ({ state }) => state.phase === 'playing' && state.teams[1][2].bot);
  const late = await peerAt(t, gatewayPipe, cookie);
  late.send({ type: 'join', name: 'Rue', team: 1, slot: 2, hero: 'arc' });
  await late.next('welcome');
  const frame3 = await late.next('snapshot');
  const controlled = frame3.state.units.find(unit => unit.id === frame3.state.playerId);
  assert.equal(controlled.id, frame2.state.playerId);
  assert.equal(controlled.heroId, 'bulwark');
  late.send({ type: 'command', action: { type: 'move', x: -1, y: 0 } });
  const moved = await late.next('snapshot', ({ state }) =>
    state.units.find(unit => unit.id === controlled.id).x < controlled.x);
  assert.ok(moved.state.time > frame3.state.time);
  const stale = await peerAt(t, gatewayPipe, cookie);
  stale.send({ ...secondIdentity, type: 'join' });
  assert.equal((await stale.next('error')).code, 'resume-invalid');
});

test('a lobby enforces three per team and six overall, and reconnects to the original seat', async t => {
  const host = createMobaServer();
  const pipe = await listen(host.server);
  t.after(() => host.close());
  const clients = [];
  for (let index = 0; index < 6; index++) {
    const client = await peerAt(t, pipe);
    client.send({ type: 'join', name: 'Player ' + (index + 1) });
    clients.push({ client, identity: await client.next('welcome') });
  }
  assert.deepEqual(host.getStatus(), { phase: 'waiting', players: 6, capacity: 6, queued: 0, joinable: false });
  const extra = await peerAt(t, pipe);
  extra.send({ type: 'join', team: 1 });
  assert.equal((await extra.next('error')).code, 'seat-taken');
  extra.send({ type: 'join' });
  assert.equal((await extra.next('error')).code, 'full');

  const { client, identity } = clients[0];
  client.socket.close();
  await extra.next('lobby', ({ state }) => state.players.length === 5);
  extra.send({ ...identity, type: 'join' });
  assert.equal((await extra.next('welcome')).playerId, identity.playerId);
  const { state } = await extra.next('lobby', ({ state }) => state.teams[0][0].playerId === identity.playerId);
  assert.equal(state.teams[0][0].hero, 'arc');
  assert.equal(state.players.length, 6);
});
