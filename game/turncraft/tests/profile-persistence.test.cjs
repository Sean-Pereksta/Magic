const {test} = require('node:test');
const assert = require('node:assert/strict');
const {pathToFileURL} = require('node:url');
const path = require('node:path');
const load = name => import(pathToFileURL(path.resolve(__dirname, '../shattered-front', name)));

function database() {
  const records = new Map(), auth = {currentUser: {uid: 'a'}};
  let tail = Promise.resolve(), writes = 0;
  const api = {
    doc: (db, ...parts) => parts.join('/'),
    runTransaction(db, operation) {
      const task = tail.then(async () => {
        const pending = [];
        const value = await operation({
          get: async ref => ({exists: () => records.has(ref), data: () => structuredClone(records.get(ref))}),
          set: (ref, data) => pending.push(() => records.set(ref, structuredClone(data))),
          update: (ref, patch) => pending.push(() => {
            const account = records.get(ref);
            for (const [field, value] of Object.entries(patch)) {
              const keys = field.split('.'); let target = account;
              for (const key of keys.slice(0, -1)) target = target[key] ||= {};
              target[keys.at(-1)] = structuredClone(value);
            }
          })
        });
        for (const write of pending) {write(); writes++;}
        return value;
      });
      tail = task.catch(() => {});
      return task;
    }
  };
  records.set('users/Alpha', {bio: 'Preserved', gameStats: {other: {wins: 7}, turncraft: {mmr: 1000}}});
  records.set('users/Beta', {gameStats: {turncraft: {mmr: 5}}});
  records.set('lobbies/duel', {gameType: 'turncraft', turncraftVersion: 1, ranked: true, status: 'finished', finishedAt: 1234000, members: {a: 'Alpha', b: 'Beta'}, turncraftSeats: ['a','b'], turncraftPlayers: {a: {name: 'Alpha', factionId: 'frontier'}, b: {name: 'Beta', factionId: 'brood'}}, settings: {maxPlayers: 2, fillAi: false, mapId: 'classic', gameMode: 'annihilation'}, turncraftResult: {winnerIdx: 0, players: [{name: 'Alpha', factionId: 'frontier'}, {name: 'Beta', factionId: 'brood'}], durationSeconds: 120}});
  return {db: {}, auth, api, records, writes: () => writes};
}

test('ranked history and rating survive reopen and simultaneous retries count once per participant', async () => {
  const {saveMatchResult} = await load('result-store.mjs'), state = database();
  const values = await Promise.all([saveMatchResult(state, 'duel', 'Alpha'), saveMatchResult(state, 'duel', 'Alpha')]);
  assert.equal(values.filter(value => value.wrote).length, 1);
  const saved = state.records.get('users/Alpha');
  assert.equal(saved.gameStats.turncraft.mmr, 1025);
  assert.equal(saved.gameStats.turncraft.rankedMatches, 1);
  assert.equal(saved.gameStats.turncraft.rankedWins, 1);
  assert.equal(saved.gameStats.turncraft.multiplayerMatches, 1);
  assert.equal(saved.bio, 'Preserved');
  assert.deepEqual(saved.gameStats.other, {wins: 7});
  state.auth.currentUser = {uid: 'b'};
  const loser = await saveMatchResult(state, 'duel', 'Beta');
  assert.equal(loser.history.mmrAfter, 0);
  assert.equal(loser.history.mmrDelta, -5);
  assert.equal(loser.profile.rankedLosses, 1);
  const reopened = await saveMatchResult({...state}, 'duel', 'Beta');
  assert.equal(reopened.wrote, false);
  assert.equal(state.records.get('users/Beta').gameStats.turncraft.rankedMatches, 1);
});

test('custom/draw history is separate from ranked and identity changes cannot write late receipts', async () => {
  const {saveMatchResult} = await load('result-store.mjs'), state = database();
  const room = state.records.get('lobbies/duel'); room.ranked = false; room.turncraftResult.winnerIdx = null;
  const saved = await saveMatchResult(state, 'duel', 'Alpha');
  assert.equal(saved.history.mode, 'custom'); assert.equal(saved.history.outcome, 'draw');
  assert.equal(saved.profile.multiplayerDraws, 1); assert.equal(saved.profile.mmr, 1000);
  assert.equal(saved.profile.rankedMatches, undefined);
  const changed = database();
  await assert.rejects(saveMatchResult({...changed, isCurrentAccount: () => false}, 'duel', 'Alpha'), /profile changed/);
  assert.equal(changed.writes(), 0);
  changed.records.set('users/Stranger', {gameStats: {}});
  await assert.rejects(saveMatchResult(changed, 'duel', 'Stranger'), /profile that entered/);
  assert.equal(changed.writes(), 0);
});

test('the same hub profile recovers an unsettled match after its Firebase auth UID is replaced', async () => {
  const {saveMatchResult} = await load('result-store.mjs'), state = database();
  state.auth.currentUser = {uid: 'replacement-auth-uid'};
  const saved = await saveMatchResult(state, 'duel', 'Alpha');
  assert.equal(saved.history.uid, 'a');
  assert.equal(saved.history.outcome, 'win');
  assert.equal(saved.profile.mmr, 1025);
  assert.equal((await saveMatchResult(state, 'duel', 'Alpha')).wrote, false);
});

test('solo losses, campaign stars and survival history persist with stable once-only run IDs', async () => {
  const {createProfileStore} = await load('profile-store.mjs'), {readProgress} = await load('progress.mjs');
  const state = database(), store = createProfileStore(state);
  const campaign = {resultId: 'campaign-win', missionId: 'amber-wastes-1', won: true, stars: 3, difficulty: 'hard', factionId: 'frontier'};
  await store.save('Alpha', 'campaign', campaign);
  await store.save('Alpha', 'campaign', {...campaign, resultId: 'campaign-loss', won: false, stars: 0});
  await store.save('Alpha', 'survival', {resultId: 'survival-run', arenaId: 'amber-wastes', factionId: 'brood', difficulty: 'medium', wave: 19});
  const before = state.writes();
  await store.save('Alpha', 'campaign', campaign);
  assert.equal(state.writes(), before);
  const profile = state.records.get('users/Alpha').gameStats.turncraft;
  assert.equal(profile.campaign.completedMissions['amber-wastes-1'].stars, 3);
  assert.equal(profile.survival.highestWave, 19);
  assert.equal(state.records.get('users/Alpha/turncraftSoloHistory/campaign-loss').outcome, 'loss');
  global.localStorage = {getItem: () => JSON.stringify({pendingResults: [{mode: 'campaign', missionId: 'amber-wastes-1', won: false, resultId: 'offline-loss'}]})};
  try { assert.equal(readProgress('profile-key').pendingResults[0].resultId, 'offline-loss'); }
  finally { delete global.localStorage; }
});
