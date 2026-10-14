const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const { initializeTestEnvironment, assertFails, assertSucceeds } = require('@firebase/rules-unit-testing');
const api = require('firebase/firestore');

(async () => {
  if (!process.env.FIRESTORE_EMULATOR_HOST) throw new Error('Use emulators:exec; this test never connects to production.');
  const env = await initializeTestEnvironment({
    projectId: 'demo-turncraft',
    firestore: { rules: fs.readFileSync(path.resolve(__dirname, '../../../firestore.rules'), 'utf8') }
  });
  const { createProfileStore } = await import(pathToFileURL(path.resolve(__dirname, '../shattered-front/profile-store.mjs')).href);
  const { MISSIONS, ARENAS } = await import(pathToFileURL(path.resolve(__dirname, '../shattered-front/campaign.mjs')).href);
  try {
    await env.clearFirestore();
    await env.withSecurityRulesDisabled(async (context) => {
      const db = context.firestore();
      await api.setDoc(api.doc(db, 'users', 'Commander'), {
        bio: 'Keep this', gameStats: { otherGame: { wins: 9 }, turncraft: { mmr: 1500, rankedMatches: 10 } }
      });
      await api.setDoc(api.doc(db, 'factionForgeNames', 'Commander'), { uid: 'owner' });
      await api.setDoc(api.doc(db, 'users', 'ExistingLegacyAccount'), { gameStats: { otherGame: { wins: 4 } } });
    });
    const db = env.authenticatedContext('owner').firestore();
    const store = createProfileStore({ db, api });
    const campaign = { missionId: MISSIONS[0].id, won: true, stars: 2, difficulty: 'medium', optionalObjectives: [] };
    const saved = await store.save('Commander', 'campaign', campaign);
    assert.equal(saved.wrote, true);
    assert.equal(saved.profile.campaign.completedMissions[campaign.missionId].stars, 2);
    assert.equal((await store.save('Commander', 'campaign', campaign)).wrote, false, 'queued replay does not write again');
    await Promise.all([
      store.save('Commander', 'campaign', { ...campaign, stars: 3 }),
      store.save('Commander', 'campaign', { ...campaign, stars: 1, difficulty: 'hard' })
    ]);
    let account = (await api.getDoc(api.doc(db, 'users', 'Commander'))).data();
    const mission = account.gameStats.turncraft.campaign.completedMissions[campaign.missionId];
    assert.equal(mission.stars, 3, 'concurrent saves keep best stars');
    assert.deepEqual(mission.ratingsByDifficulty, { medium: 3, hard: 1 });
    const survival = { arenaId: ARENAS[0].id, wave: 10, durationSeconds: 600, enemiesDefeated: 80, factionId: 'frontier', difficulty: 'medium' };
    await store.save('Commander', 'survival', survival);
    assert.equal((await store.save('Commander', 'survival', { ...survival, wave: 5 })).wrote, false, 'weaker survival run performs no write');
    await store.save('Commander', 'survival', { ...survival, wave: 12 });
    account = (await api.getDoc(api.doc(db, 'users', 'Commander'))).data();
    assert.equal(account.gameStats.turncraft.survival.recordsByMap[`${ARENAS[0].id}:medium`].wave, 12);
    assert.equal(account.gameStats.turncraft.achievements['survival-10'], true);
    assert.equal(account.gameStats.turncraft.mmr, 1500);
    assert.equal(account.gameStats.turncraft.rankedMatches, 10);
    assert.deepEqual(account.gameStats.otherGame, { wins: 9 });
    assert.equal(account.bio, 'Keep this');
    assert.equal((await store.save('ExistingLegacyAccount', 'campaign', campaign)).wrote, true, 'existing username account works without a new binding');
    const peerDb = env.authenticatedContext('peer').firestore();
    await assertFails(createProfileStore({ db: peerDb, api }).save('Commander', 'campaign', { ...campaign, difficulty: 'pro' }));
    await assertFails(api.updateDoc(api.doc(db, 'users', 'Commander'), { 'gameStats.turncraft.mmr': 9999 }));
    await assertFails(api.updateDoc(api.doc(db, 'users', 'Commander'), { 'gameStats.turncraft.rankedMatches': 100 }));
    await assertFails(api.updateDoc(api.doc(db, 'users', 'Commander'), { 'gameStats.turncraft': { campaign: {} } }));
    await assertFails(api.updateDoc(api.doc(db, 'users', 'Commander'), { 'gameStats.turncraft.survival.highestWave': 100001 }));
    await assertFails(api.updateDoc(api.doc(env.unauthenticatedContext().firestore(), 'users', 'Commander'), { 'gameStats.turncraft.achievements': {} }));
    await assertSucceeds(api.updateDoc(api.doc(db, 'users', 'Commander'), { 'gameStats.otherGame.wins': 10 }));
    await assert.rejects(store.save('MissingAccount', 'campaign', campaign), /existing CatnMice account/);
    await assert.rejects(store.save('Commander', 'campaign', { ...campaign, missionId: 'nonexistent' }), /does not exist/);
    console.log('Firestore personal saves: existing accounts, best results, concurrent merge, no-op replays, achievements, rating protection and unrelated profile fields passed.');
  } finally { await env.cleanup(); }
})().catch((error) => { console.error(error); process.exitCode = 1; });
