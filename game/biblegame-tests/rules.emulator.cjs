'use strict';

// Run with firebase emulators:exec --project demo-biblegame --config
// firebase.biblegame-test.json --only firestore "node game/biblegame-tests/rules.emulator.cjs".
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { initializeTestEnvironment, assertSucceeds, assertFails } = require('@firebase/rules-unit-testing');
const { doc, collection, getDoc, getDocs, setDoc, updateDoc, deleteDoc, writeBatch, runTransaction, serverTimestamp, Timestamp, setLogLevel } = require('firebase/firestore');
setLogLevel('silent'); // Expected permission denials are asserted below.

const projectId = process.env.GCLOUD_PROJECT || 'demo-biblegame';
const [host, portText] = (process.env.FIRESTORE_EMULATOR_HOST || '127.0.0.1:8097').split(':');
const suffix = `${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
const lobbyId = `bible_rules_${suffix}`, sid = `match_${suffix}`;
const roster = Array.from({ length: 20 }, (_, i) => ({
  name: `RulesP${i}_${suffix}`, authUid: `rules_auth_${i}_${suffix}`, uid: `acct_rulesp${i}_${suffix}`, connectionId: `connection_${i}`
}));
const [alice, bob] = roster;
let env, count = 0;
const dbFor = p => env.authenticatedContext(p.authUid).firestore();
const sessionPath = `lobbies/${lobbyId}/bibleSessions/${sid}`;
const playerPath = p => `${sessionPath}/players/${p.uid}`;
const memberPath = p => `${sessionPath}/members/${p.authUid}`;
const basePlayer = p => ({ uid: p.uid, username: p.name, profileKey: p.name, authUid: p.authUid, connectionId: p.connectionId, score: 0, spectator: false, online: true, lastSeenAt: Timestamp.now(), joinedAt: Timestamp.now(), elo: 0 });
const baseMember = p => ({ playerUid: p.uid, profileKey: p.name, connectionId: p.connectionId, updatedAt: Timestamp.now() });
async function check(label, fn) { await fn(); count++; console.log(`ok ${count} - ${label}`); }
async function admin(fn) { return env.withSecurityRulesDisabled(async context => fn(context.firestore())); }
async function patchSession(patch) { return admin(db => updateDoc(doc(db, sessionPath), patch)); }

async function main() {
  env = await initializeTestEnvironment({
    projectId,
    firestore: { host, port: Number(portText), rules: fs.readFileSync(process.env.BIBLE_RULES_PATH || path.resolve(__dirname, '../../firestore.rules'), 'utf8') }
  });
  await admin(async db => {
    const batch = writeBatch(db);
    batch.set(doc(db, 'lobbies', lobbyId), { gameType: 'biblegame', host: alice.name, players: roster.map(p => p.name), status: 'started' });
    roster.forEach(p => batch.set(doc(db, 'users', p.name), { username: p.name, chatAuthUid: p.authUid, bibleGame: { elo: 0, gamesPlayed: 0 } }));
    // Reproduce the cross-profile restriction that previously aborted room results.
    batch.set(doc(db, 'factionForgeNames', bob.name), { uid: bob.authUid });
    await batch.commit();
  });
  const a = dbFor(alice), b = dbFor(bob), outsider = env.authenticatedContext(`outsider_${suffix}`).firestore();
  const sref = doc(a, sessionPath);

  await check('signed-out readers cannot access a Bible session', () => assertFails(getDoc(doc(env.unauthenticatedContext().firestore(), sessionPath))));
  await check('unlisted callers cannot create room controls', () => assertFails(setDoc(doc(outsider, 'lobbies', lobbyId, 'bibleGame', 'control'), {
    activeSessionId: sid, hostUid: alice.uid, hostName: alice.name, hostAuthUid: alice.authUid, hostProfileKey: alice.name, updatedAt: serverTimestamp()
  })));
  await check('host atomically creates session and active-session control', async () => {
    const batch = writeBatch(a);
    batch.set(sref, { sessionId: sid, status: 'voting', mode: null, hostUid: alice.uid, hostName: alice.name, hostAuthUid: alice.authUid, hostProfileKey: alice.name,
      phaseStartedAt: serverTimestamp(), durationMs: 60000, round: 0, roundKey: 0, participantUids: [], teamA: [], teamB: [], teamTotals: { A: 0, B: 0 }, teamLocks: { A: false, B: false },
      ratingsApplied: false, ratingResults: {}, createdAt: serverTimestamp(), updatedAt: serverTimestamp() });
    batch.set(doc(a, 'lobbies', lobbyId, 'bibleGame', 'control'), { activeSessionId: sid, hostUid: alice.uid, hostName: alice.name, hostAuthUid: alice.authUid, hostProfileKey: alice.name, updatedAt: serverTimestamp() });
    await assertSucceeds(batch.commit());
  });
  await check('players join with their own verified profile and connection index', async () => {
    for (const p of [alice, bob]) {
      const db = dbFor(p), batch = writeBatch(db);
      batch.set(doc(db, playerPath(p)), basePlayer(p));
      batch.set(doc(db, memberPath(p)), { ...baseMember(p), updatedAt: serverTimestamp() });
      await assertSucceeds(batch.commit());
    }
    await admin(async db => {
      const batch = writeBatch(db);
      roster.slice(2).forEach(p => { batch.set(doc(db, playerPath(p)), basePlayer(p)); batch.set(doc(db, memberPath(p)), baseMember(p)); });
      await batch.commit();
    });
  });
  await check('an opponent cannot overwrite another player', () => assertFails(updateDoc(doc(b, playerPath(alice)), { score: 500 })));
  await check('a caller cannot impersonate another username', () => assertFails(setDoc(doc(outsider, `${sessionPath}/players/fake`), { ...basePlayer(alice), uid: 'fake', authUid: `outsider_${suffix}` })));
  await check('own vote and book choice are accepted', async () => {
    await assertSucceeds(setDoc(doc(a, `${sessionPath}/votes/${alice.uid}`), { uid: alice.uid, connectionId: alice.connectionId, mode: 'teams', username: alice.name, updatedAt: serverTimestamp() }));
    await assertSucceeds(setDoc(doc(a, `${sessionPath}/bookChoices/${alice.uid}`), { uid: alice.uid, connectionId: alice.connectionId, draftBooks: ['Genesis'], updatedAt: serverTimestamp() }));
  });
  await check('stale connections cannot vote', () => assertFails(setDoc(doc(a, `${sessionPath}/votes/${alice.uid}`), { uid: alice.uid, connectionId: 'previous-tab', mode: 'speed', updatedAt: serverTimestamp() })));
  await check('starting a twenty-player room may reset participant state', async () => {
    const batch = writeBatch(a), ids = roster.map(p => p.uid);
    batch.update(sref, { status: 'playing', mode: 'teams', round: 1, roundKey: 1, participantUids: ids, teamA: ids.filter((_, i) => i % 2 === 0), teamB: ids.filter((_, i) => i % 2 === 1), phaseStartedAt: serverTimestamp(), durationMs: 75000, updatedAt: serverTimestamp() });
    roster.forEach(p => batch.update(doc(a, playerPath(p)), { score: 0, lastResult: null, lastAnsweredRoundKey: null, spectator: false }));
    await assertSucceeds(batch.commit());
  });
  await check('team A chat is private from team B and outsiders', async () => {
    const ref = doc(a, `${sessionPath}/chatTeamA/message1`);
    await assertSucceeds(setDoc(ref, { uid: alice.uid, connectionId: alice.connectionId, name: alice.name, text: 'Discuss Genesis here.', ts: serverTimestamp() }));
    await assertSucceeds(getDocs(collection(a, `${sessionPath}/chatTeamA`)));
    await assertFails(getDoc(doc(b, `${sessionPath}/chatTeamA/message1`)));
    await assertFails(getDocs(collection(b, `${sessionPath}/chatTeamA`)));
    await assertFails(getDoc(doc(outsider, `${sessionPath}/chatTeamA/message1`)));
    await assertFails(setDoc(doc(b, `${sessionPath}/chatTeamA/forbidden`), { uid: bob.uid, connectionId: bob.connectionId, name: bob.name, text: 'No.', ts: serverTimestamp() }));
  });
  await check('unknown Bible subcollections cannot use the legacy recursive allow', () => assertFails(setDoc(doc(a, `${sessionPath}/unrestricted/anything`), { score: 999 })));
  await check('voting and book changes stop once play starts', async () => {
    await assertFails(updateDoc(doc(a, `${sessionPath}/votes/${alice.uid}`), { mode: 'speed', updatedAt: serverTimestamp() }));
    await assertFails(updateDoc(doc(a, `${sessionPath}/bookChoices/${alice.uid}`), { draftBooks: ['John'], updatedAt: serverTimestamp() }));
  });
  const teamAnswer = (db, p, team, roundKey = 1) => {
    const batch = writeBatch(db);
    batch.set(doc(db, `${sessionPath}/submissions/team_${roundKey}_${team}`), { uid: p.uid, connectionId: p.connectionId, team, roundKey, points: 5, createdAt: serverTimestamp() });
    batch.update(doc(db, sessionPath), { [`teamTotals.${team}`]: 5, [`teamLocks.${team}`]: true, updatedAt: serverTimestamp() });
    batch.update(doc(db, playerPath(p)), { score: 5 });
    return batch.commit();
  };
  await check('a team cannot lock an answer during reading/discussion', () => assertFails(teamAnswer(a, alice, 'A')));
  await patchSession({ phaseStartedAt: Timestamp.fromMillis(Date.now() - 60000) });
  await check('a player cannot submit or alter the opposing team', () => assertFails(teamAnswer(b, bob, 'A')));
  await check('the current team may atomically lock one answer in its final window', () => assertSucceeds(teamAnswer(a, alice, 'A')));
  await check('the second team may lock independently without changing the first score', () => assertSucceeds(teamAnswer(b, bob, 'B')));
  await check('team submissions cannot be replaced or replayed', () => assertFails(teamAnswer(a, alice, 'A')));
  await patchSession({ roundKey: 2, teamLocks: { A: false, B: false }, teamTotals: { A: 0, B: 0 }, phaseStartedAt: Timestamp.fromMillis(Date.now() - 60000) });
  await check('a submission from an earlier round is rejected', () => assertFails(teamAnswer(a, alice, 'A', 1)));
  await patchSession({ phaseStartedAt: Timestamp.fromMillis(Date.now() - 90000) });
  await check('the server rejects an answer after the round deadline', () => assertFails(teamAnswer(a, alice, 'A', 2)));
  await patchSession({ mode: 'speed', durationMs: 90000, phaseStartedAt: Timestamp.fromMillis(Date.now() - 100000) });
  await check('expired speed mode rejects score and private-question writes', () => assertFails(updateDoc(doc(a, playerPath(alice)), { score: 10, questionKey: 2, currentQuestion: { id: 2 } })));
  await patchSession({ mode: 'category' });
  await check('expired category mode rejects a verse and score atomically', async () => {
    const batch = writeBatch(a);
    batch.set(doc(a, `${sessionPath}/submissions/2_${alice.uid}_category_123`), { uid: alice.uid, connectionId: alice.connectionId, roundKey: 2, verseId: 123, createdAt: serverTimestamp() });
    batch.update(doc(a, playerPath(alice)), { score: 6 });
    await assertFails(batch.commit());
  });
  await patchSession({ mode: 'teams', durationMs: 75000 });
  await check('a verified participant can advance an abandoned round', () => assertSucceeds(updateDoc(doc(b, sessionPath), { round: 3, roundKey: 3, phaseStartedAt: serverTimestamp(), teamLocks: { A: false, B: false }, updatedAt: serverTimestamp() })));
  await patchSession({ round: 4, roundKey: 4, teamTotals: { A: 0, B: 0 }, phaseStartedAt: Timestamp.fromMillis(Date.now() - 60000), currentQuestion: { id: 1, book: 'Genesis', chapter: 1, verse: 1, text: 'In the beginning.' } });
  await check('concurrent real transactions preserve the opposite team and retry safely', async () => {
    let readers = 0, release;
    const bothRead = new Promise(resolve => { release = resolve; });
    const answer = (db, p, team) => {
      let attempt = 0;
      return runTransaction(db, async tx => {
        const ref = doc(db, sessionPath), pref = doc(db, playerPath(p)), sub = doc(db, `${sessionPath}/submissions/team_4_${team}`);
        const [s, player, previous] = await Promise.all([tx.get(ref), tx.get(pref), tx.get(sub)]);
        if (++attempt === 1) { if (++readers === 2) release(); await bothRead; }
        if (previous.exists()) return;
        const current = s.data(), correct = current.currentQuestion;
        tx.set(sub, { uid: p.uid, connectionId: p.connectionId, team, roundKey: current.roundKey, submittedByName: p.name,
          guess: { book: correct.book, chapter: correct.chapter, verse: correct.verse }, correct, bookOK: true, chapterOK: true, verseOK: true, points: 5, full: true, createdAt: serverTimestamp() });
        // Full-map writes are denied during a race before the SDK can retry.
        // Own-team field paths preserve the already committed opposing answer.
        tx.update(ref, { [`teamTotals.${team}`]: current.teamTotals[team] + 5, [`teamLocks.${team}`]: true,
          ...(current.teamLocks[team === 'A' ? 'B' : 'A'] ? { roundResolvedAt: serverTimestamp() } : {}), updatedAt: serverTimestamp() });
        tx.update(pref, { score: player.data().score + 5 });
      });
    };
    await assertSucceeds(Promise.all([answer(a, alice, 'A'), answer(b, bob, 'B')]));
    const finished = (await getDoc(sref)).data();
    assert.deepEqual(finished.teamTotals, { A: 5, B: 5 });
    assert.deepEqual(finished.teamLocks, { A: true, B: true });
    assert.ok(finished.roundResolvedAt.toMillis() > 0);
  });
  await check('a new connection can take ownership; the old connection loses writes', async () => {
    const replacement = 'new-active-tab', batch = writeBatch(a);
    batch.update(doc(a, playerPath(alice)), { connectionId: replacement });
    batch.update(doc(a, memberPath(alice)), { connectionId: replacement, updatedAt: serverTimestamp() });
    await assertSucceeds(batch.commit());
    await assertFails(updateDoc(doc(a, playerPath(alice)), { connectionId: alice.connectionId, lastSeenAt: serverTimestamp() }));
    await assertFails(setDoc(doc(a, `${sessionPath}/chatTeamA/old-tab`), { uid: alice.uid, connectionId: alice.connectionId, name: alice.name, text: 'Old tab', ts: serverTimestamp() }));
    alice.connectionId = replacement;
  });

  await assertSucceeds(updateDoc(sref, { status: 'ended', durationMs: 0, endReason: 'completed', updatedAt: serverTimestamp() }));
  const results = Object.fromEntries(roster.map(p => [p.uid, { uid: p.uid, authUid: p.authUid, profileKey: p.name, username: p.name, oldElo: 0, newElo: 28, change: 28, outcome: 'win', rankTitle: 'Mustard Seed', rankEmoji: '🌱', rankColor: '#65a30d' }]));
  await check('twenty-player result finalization and durable inboxes fit rule access limits', async () => {
    const batch = writeBatch(a);
    batch.update(sref, { ratingsApplied: true, ratingResults: results, winnerUids: roster.map(p => p.uid), winnerNames: roster.map(p => p.name), resultSummary: 'Saved.', ratedAt: serverTimestamp(), updatedAt: serverTimestamp() });
    for (const p of roster) {
      batch.update(doc(a, playerPath(p)), { elo: 28, eloChange: 28, outcome: 'win', rankTitle: 'Mustard Seed', rankEmoji: '🌱', rankColor: '#65a30d' });
      batch.set(doc(a, 'bibleGameRatingInbox', p.authUid, 'matches', sid), { lobbyId, sessionId: sid, uid: p.uid, profileKey: p.name });
    }
    await assertSucceeds(batch.commit());
  });
  await check('finalized results cannot be applied twice by a controller', () => assertFails(updateDoc(sref, { ratingsApplied: true, resultSummary: 'Replayed', updatedAt: serverTimestamp() })));
  await check('a player can only read their own queued result', async () => {
    await assertSucceeds(getDocs(collection(b, 'bibleGameRatingInbox', bob.authUid, 'matches')));
    await assertFails(getDoc(doc(a, 'bibleGameRatingInbox', bob.authUid, 'matches', sid)));
  });
  await check('the controller no longer needs another game’s bound profile', () => assertFails(updateDoc(doc(a, 'users', bob.name), { bibleGameElo: 28 })));
  await check('the recipient atomically applies their own rating and immutable receipt', async () => {
    const batch = writeBatch(b);
    batch.update(doc(b, 'users', bob.name), { bibleGameElo: 28, bibleGameWins: 1, bibleGame: { elo: 28, gamesPlayed: 1, wins: 1, lastSessionId: sid } });
    batch.set(doc(b, 'users', bob.name, 'bibleGameResults', sid), { sessionId: sid, lobbyId, uid: bob.uid, appliedAt: serverTimestamp(), oldElo: 0, newElo: 28, change: 28, outcome: 'win' });
    batch.delete(doc(b, 'bibleGameRatingInbox', bob.authUid, 'matches', sid));
    await assertSucceeds(batch.commit());
    assert.equal((await getDoc(doc(b, 'users', bob.name))).data().bibleGameElo, 28);
  });
  await check('another user cannot create a result receipt and existing receipts cannot change', async () => {
    const receipt = { sessionId: sid, lobbyId, uid: bob.uid, appliedAt: serverTimestamp() };
    await assertFails(setDoc(doc(a, 'users', bob.name, 'bibleGameResults', `fake_${sid}`), receipt));
    await assertFails(updateDoc(doc(b, 'users', bob.name, 'bibleGameResults', sid), { newElo: 999 }));
    await assertFails(deleteDoc(doc(b, 'users', bob.name, 'bibleGameResults', sid)));
  });
  await check('unrelated lobby-backed games keep their existing subcollection access', async () => {
    const other = `other_${suffix}`;
    await admin(db => setDoc(doc(db, 'lobbies', other), { gameType: 'other', players: [] }));
    await assertSucceeds(setDoc(doc(a, 'lobbies', other, 'presence', 'example'), { online: true }));
  });
  console.log(`Bible multiplayer rules: ${count} checks passed.`);
}

main().catch(error => { console.error(error); process.exitCode = 1; }).finally(async () => { if (env) await env.cleanup(); });
