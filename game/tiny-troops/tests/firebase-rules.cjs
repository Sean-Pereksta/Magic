// Run against a local demo emulator, never the user's production project.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
const from = process.env.TINY_TROOPS_FIREBASE_TEST_MODULES ? createRequire(path.join(process.env.TINY_TROOPS_FIREBASE_TEST_MODULES, 'package.json')) : require;
const { initializeTestEnvironment, assertFails, assertSucceeds } = from('@firebase/rules-unit-testing');
const { doc, collection, setDoc, getDoc, getDocs, updateDoc, serverTimestamp, runTransaction } = from('firebase/firestore');
(async () => {
  const env = await initializeTestEnvironment({ projectId: 'demo-tiny-troops', firestore: { host: '127.0.0.1', port: Number(process.env.TINY_TROOPS_FIRESTORE_PORT) || 8089, rules: fs.readFileSync(path.resolve(__dirname, '../../../firestore.rules'), 'utf8') } });
  try {
    await env.clearFirestore();
    const signed = env.authenticatedContext('tiny-test').firestore(), other = env.authenticatedContext('second-device').firestore(), guest = env.unauthenticatedContext().firestore();
    const ref = database => doc(database, 'tiny_troops_profiles/my_army');
    const saved = { username: 'My Army', cleanUsername: 'my_army', password: 'game-code', saveVersion: 'tiny_troops_v1', updatedAtMs: 100, sequence: 1, reason: 'manual', createOnly: true, updatedAt: serverTimestamp(), state: { saveVersion: 'tiny_troops_v1', schema: 3, round: 601, squad: Array(20).fill(null), tt: { id: 'test-run' } } };
    await assertFails(setDoc(ref(guest), saved)); await assertFails(getDoc(ref(guest)));
    await assertSucceeds(setDoc(ref(signed), saved));
    assert.equal((await assertSucceeds(getDoc(ref(other)))).data().state.round, 601);
    await assertFails(getDocs(collection(signed, 'tiny_troops_profiles')));
    await assertSucceeds(runTransaction(other, async tx => { const r = ref(other), current = (await tx.get(r)).data(); tx.set(r, { ...current, sequence: 2, updatedAtMs: 200, updatedAt: serverTimestamp(), state: { ...current.state, round: 602 } }); }));
    await assertFails(updateDoc(ref(other), { password: 'different-code', updatedAtMs: 300, updatedAt: serverTimestamp() }));
    await assertFails(updateDoc(ref(other), { updatedAtMs: 50, updatedAt: serverTimestamp() }));
    await assertFails(updateDoc(ref(other), { state: { ...saved.state, squad: [] }, updatedAtMs: 300, updatedAt: serverTimestamp() }));
    await assertFails(updateDoc(ref(other), { unexpected: true, updatedAtMs: 300, updatedAt: serverTimestamp() }));
    assert.equal((await getDoc(ref(signed))).data().state.round, 602);
    const legacy = 'users/tiny-test/tiny_troops_profiles/legacy';
    await env.withSecurityRulesDisabled(ctx => setDoc(doc(ctx.firestore(), legacy), saved));
    await assertSucceeds(getDoc(doc(signed, legacy))); await assertFails(getDoc(doc(other, legacy)));
    await assertSucceeds(setDoc(doc(signed, 'tiny_troops_profiles/my_army/runs/run1'), { username: 'My Army', round: 601, kills: 20, points: 300, coins: 50, dateTime: new Date().toISOString(), runId: 'run1' }));
    const leaderboard = doc(signed, 'tiny_troops_meta/global_leaderboard');
    await assertSucceeds(setDoc(leaderboard, { top10: [], updatedAt: serverTimestamp() }));
    await assertSucceeds(getDoc(leaderboard)); await assertFails(updateDoc(leaderboard, { top10: Array(11).fill({}), updatedAt: serverTimestamp() }));
    await assertFails(getDoc(doc(guest, 'tiny_troops_meta/global_leaderboard')));
    await assertSucceeds(setDoc(doc(signed, 'gameStats/unchanged-game'), { plays: 1 }));
    console.log('PASS Firestore save/create/update/read, second-device lookup, stale/code/malformed-write rejection, legacy migration, scores, and unrelated game access');
  } finally { await env.cleanup(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
