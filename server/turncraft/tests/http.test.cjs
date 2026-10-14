'use strict';
const test = require('node:test'),
  assert = require('node:assert/strict'),
  express = require('express');
const { createRouter } = require('../http.cjs');
const { createTurncraftService } = require('../service.cjs');
const {
  createStore
} = require('../../../game/turncraft/tests/helpers/authority-store.cjs');
async function server(t, { service } = {}) {
  const store = createStore({
    'factionForgeAccounts/owner': { verified: true, username: 'Owner' },
    'factionForgeNames/Owner': { uid: 'owner' },
    'factionForgeAccounts/peer': { verified: true, username: 'Peer' },
    'factionForgeNames/Peer': { uid: 'peer' }
  });
  let stamp = 100000;
  const auth = {
    verifyIdToken: async (token) => {
      if (!['owner', 'peer', 'guest'].includes(token))
        throw Object.assign(new Error('Invalid token'), {
          code: 'auth/invalid-id-token'
        });
      return { uid: token };
    }
  };
  const game =
    service || createTurncraftService({ db: store.db, auth, now: () => stamp });
  const app = express();
  app.use(
    '/api/turncraft',
    createRouter({ service: game, allowedOrigins: ['https://catnmice.com'] })
  );
  const listener = await new Promise((resolve) => {
    const value = app.listen(0, '127.0.0.1', () => resolve(value));
  });
  t.after(
    () =>
      new Promise((resolve) => {
        listener.closeAllConnections();
        listener.close(resolve);
      })
  );
  const base = `http://127.0.0.1:${listener.address().port}/api/turncraft`;
  async function call(
    path,
    {
      token = 'owner',
      origin = 'https://catnmice.com',
      method = 'GET',
      body,
      raw
    } = {}
  ) {
    return fetch(base + path, {
      method,
      headers: {
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(origin ? { Origin: origin } : {}),
        ...(body || raw ? { 'Content-Type': 'application/json' } : {})
      },
      ...(body || raw ? { body: raw || JSON.stringify(body) } : {})
    });
  }
  return { call, game, records: store.records, advance: (ms) => (stamp += ms) };
}
test('HTTP health, bearer identity, exact CORS, preflight and malformed requests', async (t) => {
  const f = await server(t);
  assert.equal((await f.call('/health', { token: null })).status, 200);
  assert.equal((await f.call('/profile', { token: null })).status, 401);
  assert.equal((await f.call('/profile', { token: 'bad' })).status, 401);
  assert.equal(
    (await f.call('/profile', { origin: 'https://attacker.test' })).status,
    403
  );
  const preflight = await f.call('/profile', {
    method: 'OPTIONS',
    token: null
  });
  assert.equal(preflight.status, 204);
  assert.equal(
    preflight.headers.get('Access-Control-Allow-Origin'),
    'https://catnmice.com'
  );
  assert.equal(
    (await f.call('/lobbies', { method: 'POST', raw: '{' })).status,
    400
  );
  assert.equal((await f.call('/profile')).status, 200);
  assert.equal(f.records.get('turncraftProfiles/owner').mmr, 1000);
});
test('HTTP custom rooms reject profile/host/winner injection and preserve readiness', async (t) => {
  const f = await server(t);
  const created = await f.call('/lobbies', {
    method: 'POST',
    body: { name: 'Room', visibility: 'public', settings: { maxPlayers: 2 } }
  });
  assert.equal(created.status, 200);
  const room = await created.json();
  assert.equal(
    (
      await f.call(`/lobbies/${room.id}/configure`, {
        method: 'POST',
        body: {
          settings: {
            mapId: 'classic',
            gameMode: 'annihilation',
            maxPlayers: 2,
            mmr: 4000
          }
        }
      })
    ).status,
    400
  );
  assert.equal(
    (
      await f.call('/lobbies/join', {
        token: 'peer',
        method: 'POST',
        body: { lobbyId: room.id }
      })
    ).status,
    200
  );
  assert.equal(
    (
      await f.call(`/lobbies/${room.id}/start`, {
        token: 'peer',
        method: 'POST',
        body: {}
      })
    ).status,
    403
  );
  assert.equal(
    (await f.call(`/lobbies/${room.id}/start`, { method: 'POST', body: {} }))
      .status,
    409
  );
  assert.equal(
    (
      await f.call('/progress/survival', {
        method: 'POST',
        body: {
          arenaId: 'amber-wastes',
          wave: 10,
          durationSeconds: 300,
          enemiesDefeated: 40,
          factionId: 'frontier',
          difficulty: 'medium',
          mmr: 9999
        }
      })
    ).status,
    400
  );
  assert.equal(
    (
      await f.call('/matches/fake/result', {
        method: 'POST',
        body: { winner: 'owner' }
      })
    ).status,
    404
  );
});
test('HTTP ranked queue uses real engine, accepts raw gameplay command, rejects seat/score claims and rates authenticated forfeit once', async (t) => {
  const f = await server(t);
  await f.call('/profile');
  await f.call('/profile', { token: 'peer' });
  assert.equal(
    (
      await f.call('/ranked/queue', {
        token: 'guest',
        method: 'POST',
        body: {}
      })
    ).status,
    403
  );
  assert.equal(
    (
      await f.call('/ranked/queue', {
        method: 'POST',
        body: { factionId: 'frontier' }
      })
    ).status,
    200
  );
  const matched = await (
    await f.call('/ranked/queue', {
      token: 'peer',
      method: 'POST',
      body: { factionId: 'brood' }
    })
  ).json();
  assert.equal(matched.status, 'matched');
  const state = await (await f.call(`/matches/${matched.matchId}`)).json();
  assert.equal(state.state.players.length, 2);
  const worker = state.state.entities.find(
    (e) => e.faction === 0 && e.kind === 'unit'
  );
  const command = { type: 'move', entityId: worker.id, x: 10, y: 8 };
  assert.equal(
    (
      await f.call(`/matches/${matched.matchId}/command`, {
        method: 'POST',
        body: { command, requestId: 'one' }
      })
    ).status,
    200
  );
  assert.equal(
    (
      await f.call(`/matches/${matched.matchId}/command`, {
        method: 'POST',
        body: { command: { ...command, slot: 1 }, requestId: 'two' }
      })
    ).status,
    400
  );
  assert.equal(
    (
      await f.call(`/matches/${matched.matchId}/command`, {
        method: 'POST',
        body: { command, requestId: 'three', winner: 'owner' }
      })
    ).status,
    400
  );
  assert.equal(
    (await f.call(`/matches/${matched.matchId}`, { token: 'guest' })).status,
    403
  );
  f.advance(30000);
  const result = await (
    await f.call(`/matches/${matched.matchId}/forfeit`, {
      token: 'peer',
      method: 'POST',
      body: {}
    })
  ).json();
  assert.equal(result.ratingReceipt.changes.owner.delta, 24);
  await f.call(`/matches/${matched.matchId}/forfeit`, {
    token: 'peer',
    method: 'POST',
    body: {}
  });
  assert.equal(f.records.get('turncraftProfiles/owner').rankedMatches, 1);
});
test('state polling budget is isolated from gameplay command budget', async (t) => {
  const service = {
    identity: async (token) => {
      if (!token) throw new Error('token');
      return { uid: token };
    },
    capabilities: () => ({ ranked: true }),
    authority: {
      state: async () => ({ state: {} }),
      command: async () => ({ accepted: true })
    }
  };
  const f = await server(t, { service });
  for (let n = 0; n < 120; n++)
    assert.equal((await f.call('/matches/active')).status, 200);
  assert.equal((await f.call('/matches/active')).status, 429);
  assert.equal(
    (
      await f.call('/matches/active/command', {
        method: 'POST',
        body: { command: { type: 'stop' }, requestId: 'raw' }
      })
    ).status,
    200
  );
});
