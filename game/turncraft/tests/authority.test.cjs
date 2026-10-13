'use strict';
const test = require('node:test'),
  assert = require('node:assert/strict');
const { createStore } = require('./helpers/authority-store.cjs');
const {
  createTurncraftService
} = require('../../../server/turncraft/service.cjs');
const {
  createRankedProcessor
} = require('../../../server/turncraft/ranked.cjs');
const {
  createSimulator,
  validateCommand
} = require('../../../server/turncraft/simulator.cjs');
const D = require('../../../server/turncraft/domain.cjs');
const a = {
    uid: 'alpha',
    username: 'Alpha',
    displayName: 'Alpha',
    linked: true
  },
  b = { uid: 'beta', username: 'Beta', displayName: 'Beta', linked: true },
  c = { uid: 'charlie', displayName: 'Guest', linked: false };
function fixture() {
  let stamp = 100000;
  const store = createStore({
    'factionForgeAccounts/alpha': { verified: true, username: 'Alpha' },
    'factionForgeNames/Alpha': { uid: 'alpha' },
    'factionForgeAccounts/beta': { verified: true, username: 'Beta' },
    'factionForgeNames/Beta': { uid: 'beta' },
    'users/Alpha': {
      otherGame: { coins: 50 },
      gameStats: { unrelated: { wins: 12 } }
    },
    'users/Beta': { otherGame: { coins: 90 } }
  });
  const auth = {
    verifyIdToken: async (token) => {
      if (token === 'bad')
        throw Object.assign(new Error('Bad token'), {
          code: 'auth/invalid-id-token'
        });
      return { uid: token };
    }
  };
  return {
    ...store,
    service: createTurncraftService({ db: store.db, auth, now: () => stamp }),
    advance: (ms) => (stamp += ms),
    now: () => stamp
  };
}
test('profile uses verified existing ownership and preserves every unrelated shared field', async () => {
  const f = fixture();
  assert.equal((await f.service.identity('alpha')).username, 'Alpha');
  assert.equal((await f.service.identity('charlie')).linked, false);
  await f.service.profile(a);
  assert.equal(f.records.get('users/Alpha').otherGame.coins, 50);
  assert.equal(f.records.get('users/Alpha').gameStats.unrelated.wins, 12);
  await f.service.profile(c);
  assert.equal(f.records.has('users/Guest'), false);
  await assert.rejects(f.service.identity(''), { code: 'UNAUTHENTICATED' });
});
test('personal progression validates mission order, merges best stars and never awards rating', async () => {
  const f = fixture();
  await f.service.profile(a);
  await assert.rejects(
    f.service.saveCampaign(a, {
      missionId: 'amber-wastes-2',
      regionId: 'amber-wastes',
      stars: 3
    }),
    { code: 'MISSION_LOCKED' }
  );
  for (let n = 1; n <= 4; n++)
    await f.service.saveCampaign(a, {
      missionId: `amber-wastes-${n}`,
      regionId: 'amber-wastes',
      stars: 2,
      optionalObjectives: ['salvage']
    });
  await f.service.saveCampaign(a, {
    missionId: 'amber-wastes-1',
    regionId: 'amber-wastes',
    stars: 1,
    optionalObjectives: []
  });
  const p = f.records.get('turncraftProfiles/alpha');
  assert.equal(p.mmr, 1000);
  assert.equal(p.rankedMatches, 0);
  assert.equal(p.campaign.completedMissions['amber-wastes-1'].stars, 2);
  assert.deepEqual(p.campaign.unlockedRegions, [
    'amber-wastes',
    'ironwood-basin'
  ]);
  await f.service.saveSurvival(a, {
    arenaId: 'amber-wastes',
    wave: 75,
    durationSeconds: 1000,
    enemiesDefeated: 3000,
    factionId: 'frontier',
    difficulty: 'hard'
  });
  assert.equal(
    f.records.get('turncraftProfiles/alpha').survival.recordsByMap[
      'amber-wastes_hard'
    ].verified,
    false
  );
  assert.equal(f.records.has('turncraftRankings/alpha'), false);
  await assert.rejects(
    f.service.saveSurvival(a, {
      arenaId: 'amber-wastes',
      wave: 75,
      durationSeconds: 1000,
      enemiesDefeated: 3000,
      factionId: 'frontier',
      difficulty: 'hard',
      mmr: 4000
    }),
    { code: 'INVALID_ARGUMENT' }
  );
});
test('private invites, UID seats, readiness, host-only start, late join and host departure', async () => {
  const f = fixture();
  const room = await f.service.createLobby(a, {
    name: 'Private',
    visibility: 'private',
    settings: { maxPlayers: 2 }
  });
  await assert.rejects(f.service.join(b, { lobbyId: room.id }), {
    code: 'INVITE_REQUIRED'
  });
  await f.service.join(b, { code: room.inviteCode });
  await assert.rejects(f.service.start(b, room.id), { code: 'HOST_ONLY' });
  await assert.rejects(f.service.start(a, room.id), { code: 'NOT_READY' });
  await f.service.player(a, room.id, { ready: true });
  await f.service.player(b, room.id, { factionId: 'brood', ready: true });
  const started = await f.service.start(a, room.id);
  assert.equal(started.status, 'started');
  assert.deepEqual(started.turncraftSeats, ['alpha', 'beta']);
  assert.equal(started.chessStarfrontReady.Beta.factionId, 'brood');
  await assert.rejects(f.service.join(c, { lobbyId: room.id }), {
    code: 'MATCH_STARTED'
  });
  await assert.rejects(f.service.start(a, room.id), { code: 'MATCH_STARTED' });
  const other = await f.service.createLobby(a, {});
  await f.service.join(b, { lobbyId: other.id });
  await f.service.leave(a, other.id);
  assert.equal(f.records.get(`lobbies/${other.id}`).hostUid, 'beta');
});
test('actual authoritative RTS starts two humans, processes legal commands, and restores checkpoints', () => {
  const engine = createSimulator({ clock: 100000, seed: 42 });
  const first = engine.start({
    players: [
      { name: 'Alpha', factionId: 'frontier' },
      { name: 'Beta', factionId: 'brood' }
    ]
  });
  assert.equal(first.players.length, 2);
  assert.ok(first.players.every((p) => p.ai === false));
  assert.ok(first.entities.some((e) => e.type === 'commandHub'));
  const worker = first.entities.find((e) => e.type === 'engineer');
  engine.command(
    0,
    validateCommand({ type: 'move', entityId: worker.id, x: 10, y: 8 })
  );
  engine.advance(105000);
  const restored = createSimulator({ checkpoint: engine.save() });
  assert.equal(restored.clock, 105000);
  assert.deepEqual(restored.snapshot().players, engine.snapshot().players);
  assert.equal(
    restored.snapshot().entities.find((e) => e.id === worker.id).x,
    engine.snapshot().entities.find((e) => e.id === worker.id).x
  );
  assert.throws(
    () =>
      validateCommand({ type: 'move', entityId: worker.id, x: Infinity, y: 8 }),
    { code: 'INVALID_ARGUMENT' }
  );
  assert.throws(() => validateCommand({ type: 'move', slot: 1 }), {
    code: 'INVALID_ARGUMENT'
  });
});
test('custom completion verifies persisted terminal snapshot; rematch uses a fresh room and cannot duplicate or grant rating', async () => {
  const f = fixture(),
    room = await f.service.createLobby(a, { requestId: 'idempotent-room' });
  assert.equal(
    (await f.service.createLobby(a, { requestId: 'idempotent-room' })).id,
    room.id
  );
  await f.service.join(b, { lobbyId: room.id });
  await f.service.player(a, room.id, { ready: true });
  await f.service.player(b, room.id, { ready: true });
  await f.service.start(a, room.id);
  const lobbyPath = `lobbies/${room.id}`,
    row = f.records.get(lobbyPath);
  row.chessStarfrontController = {
    ownerUid: 'alpha',
    ownerName: 'Alpha',
    epoch: 1,
    expiresAtMs: f.now() + 16000
  };
  f.records.set(lobbyPath, row);
  await assert.rejects(f.service.complete(b, room.id, {}), {
    code: 'HOST_ONLY'
  });
  await assert.rejects(f.service.complete(a, room.id, {}), {
    code: 'RESULT_NOT_READY'
  });
  await assert.rejects(f.service.complete(a, room.id, { winner: 'alpha' }), {
    code: 'INVALID_ARGUMENT'
  });
  f.records.set(`${lobbyPath}/chess_starfront/state`, {
    gameKey: 'chess_starfront',
    gameOver: true,
    controllerUid: 'alpha',
    controllerEpoch: 1,
    players: [
      { name: 'Alpha', factionId: 'frontier' },
      { name: 'Beta', factionId: 'ascendant' }
    ],
    matchStartedAt: 100000,
    matchResult: {
      winnerIdx: 0,
      finishedAt: 130000,
      reason: 'Every opposing command base fell.'
    }
  });
  const finished = await f.service.complete(a, room.id, {});
  assert.equal(finished.status, 'finished');
  assert.equal(finished.turncraftResult.winnerIdx, 0);
  assert.deepEqual(await f.service.complete(a, room.id, {}), finished);
  await assert.rejects(f.service.rematch(b, room.id, {}), {
    code: 'HOST_ONLY'
  });
  const copies = await Promise.all([
    f.service.rematch(a, room.id, {}),
    f.service.rematch(a, room.id, {})
  ]);
  assert.equal(copies[0].id, copies[1].id);
  assert.notEqual(copies[0].id, room.id);
  assert.equal(copies[0].status, 'waiting');
  assert.ok(Object.values(copies[0].turncraftPlayers).every((p) => !p.ready));
  assert.equal(f.records.get(lobbyPath).nextLobbyId, copies[0].id);
  assert.equal(
    f.records.has(`lobbies/${copies[0].id}/chess_starfront/state`),
    false
  );
  assert.equal(f.records.get('turncraftProfiles/alpha').rankedMatches, 0);
});
test('ranked queue authenticates two accounts; authoritative forfeit atomically rates once and preserves other games', async () => {
  const f = fixture();
  await f.service.profile(a);
  await f.service.profile(b);
  await assert.rejects(f.service.authority.queue(c, {}), {
    code: 'ACCOUNT_NOT_LINKED'
  });
  assert.equal(
    (await f.service.authority.queue(a, { factionId: 'frontier' })).status,
    'waiting'
  );
  const match = await f.service.authority.queue(b, { factionId: 'ascendant' });
  assert.equal(match.status, 'matched');
  assert.equal((await f.service.authority.queue(a, {})).matchId, match.matchId);
  await assert.rejects(f.service.authority.state(c, match.matchId), {
    code: 'NOT_A_MEMBER'
  });
  f.advance(30000);
  const result = await f.service.authority.forfeit(b, match.matchId);
  assert.equal(result.state.gameOver, true);
  assert.equal(result.ratingReceipt.outcome, 'p0');
  assert.deepEqual(result.ratingReceipt.deltas, [24, -24]);
  const duplicate = await Promise.all([
    f.service.authority.forfeit(b, match.matchId),
    f.service.authority.state(a, match.matchId)
  ]);
  assert.deepEqual(duplicate[0].ratingReceipt, result.ratingReceipt);
  assert.equal(f.records.get('turncraftProfiles/alpha').rankedMatches, 1);
  assert.equal(f.records.get('turncraftProfiles/beta').rankedLosses, 1);
  assert.equal(
    f.records.get('turncraftProfiles/alpha').activeRankedMatch,
    null
  );
  assert.equal(f.records.get('users/Alpha').gameStats.unrelated.wins, 12);
  assert.equal(f.records.get('users/Alpha').otherGame.coins, 50);
  assert.equal(f.records.get('turncraftRankings/alpha').mmr, 1024);
});
test('authority scopes commands to authenticated seats and rejects winner/score injection', async () => {
  const f = fixture();
  await f.service.profile(a);
  await f.service.profile(b);
  await f.service.authority.queue(a, {});
  const match = await f.service.authority.queue(b, {}),
    state = await f.service.authority.state(a, match.matchId),
    enemy = createSimulator({
      checkpoint: f.records.get('turncraftMatches/' + match.matchId).checkpoint
    })
      .snapshot()
      .entities.find((e) => e.faction === 1 && e.kind === 'unit');
  const command = { type: 'move', entityId: enemy.id, x: 20, y: 20 };
  const before = JSON.stringify(enemy);
  assert.ok(
    !state.state.entities.some((e) => e.id === enemy.id),
    'fog-hidden enemy actors must not reach the client'
  );
  assert.equal(state.state.players[1].minerals, 0);
  await f.service.authority.command(a, match.matchId, {
    command,
    requestId: 'own-seat-only'
  });
  const after = await f.service.authority.state(a, match.matchId);
  assert.equal(
    JSON.stringify(
      createSimulator({
        checkpoint: f.records.get('turncraftMatches/' + match.matchId)
          .checkpoint
      })
        .snapshot()
        .entities.find((e) => e.id === enemy.id)
    ),
    before
  );
  await assert.rejects(
    f.service.authority.command(a, match.matchId, {
      command,
      requestId: 'inject',
      winner: 'alpha'
    }),
    { code: 'INVALID_ARGUMENT' }
  );
  assert.equal(f.records.get('turncraftProfiles/alpha').rankedMatches, 0);
});
test('rating processor refuses missing verifier, practice/private/bot matches and fabricated results', async () => {
  const f = fixture(),
    processor = createRankedProcessor({ db: f.db, now: f.now });
  await assert.rejects(processor.recordTrustedResult('fake', {}), {
    code: 'RANKED_UNAVAILABLE'
  });
  f.records.set('turncraftMatches/fake', { ranked: false });
  await assert.rejects(
    createRankedProcessor({
      db: f.db,
      now: f.now,
      verifyEvidence: async () => ({ verified: true })
    }).recordTrustedResult('fake', {}),
    { code: 'INELIGIBLE_MATCH' }
  );
  assert.equal(D.rankFor(1000, 0), 'Unranked');
  assert.equal(D.rankFor(1750, 10), 'Master');
  const profiles = D.elo(D.newProfile(a, 0), D.newProfile(b, 0), 'draw', 100);
  assert.deepEqual(
    profiles.map((p) => p.lastDelta),
    [0, 0]
  );
});
