const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness, timestamp } = require('./harness.cjs');

function exactGuess(h) {
  h.el('bookInput').value = h.verse.book;
  h.el('chapInput').value = h.verse.chapter;
  h.el('verseInput').value = h.verse.verse;
  return { book: h.verse.book, chapter: h.verse.chapter, verse: h.verse.verse };
}

function makeFreshHost(h) {
  const hostId = h.run("stablePlayerUid('Bob')");
  const host = {
    uid: hostId, username: 'Bob', profileKey: 'Bob', authUid: 'auth-bob',
    connectionId: 'host-device', score: 0, online: true, lastSeenAt: timestamp(h.now())
  };
  h.docs.set(`${h.sessionPath}/players/${hostId}`, host);
  h.context.__host = host;
  h.run('PLAYERS.set(__host.uid,__host)');
  h.setSession({ hostUid: hostId, hostName: 'Bob', participantUids: [hostId, h.playerId] });
  assert.equal(h.run('driver()'), false);
  assert.equal(h.run('isFresh(PLAYERS.get(SESSION.hostUid))'), true);
  return hostId;
}

test('team controls follow reading, discussion, final, and expired boundaries without another snapshot', () => {
  const h = createHarness();
  const start = h.now();
  h.setSession({ mode: 'teams', phaseStartedAt: timestamp(start), durationMs: 75_000 });
  h.run('render()');
  assert.equal(h.el('submitBtn').disabled, true);
  assert.match(h.el('teamStage').textContent, /read/i);
  h.setNow(start + h.run('TEAM_READING_MS'));
  h.run('updateTimer()');
  assert.match(h.el('teamStage').textContent, /discuss/i);
  assert.equal(h.el('submitBtn').disabled, true);
  h.setNow(start + 75_000 - h.run('TEAM_FINAL_MS'));
  h.run('updateTimer()');
  assert.match(h.el('teamStage').textContent, /final/i);
  assert.equal(h.el('submitBtn').disabled, false);
  h.setNow(start + 75_000);
  h.run('updateTimer()');
  assert.equal(h.el('submitBtn').disabled, true);
});

test('a presence or score rerender preserves the player reference draft', () => {
  const h = createHarness();
  h.run('render()');
  exactGuess(h);
  h.setPlayer({ score: 3 });
  h.run('render()');
  assert.equal(h.el('bookInput').value, 'Genesis');
  assert.equal(Number(h.el('chapInput').value), 1);
  assert.equal(Number(h.el('verseInput').value), 1);
});

test('saving immediately disables the answer button and suppresses extra clicks through rerenders', async () => {
  const h = createHarness();
  h.run('render()');
  exactGuess(h);
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  h.beforeTransaction(() => gate);
  const first = h.run('submitStandard()');
  assert.equal(h.el('submitBtn').disabled, true);
  assert.match(h.el('submitBtn').textContent, /saving/i);
  h.run('render()');
  assert.equal(h.el('submitBtn').disabled, true);
  const second = h.run('submitStandard()');
  release();
  await Promise.all([first, second]);
  assert.equal(h.player().score, 5);
  assert.equal(h.writes.filter(write => write.path.includes('/submissions/')).length, 1);
  assert.match(h.el('submitNotice').textContent, /saved/i);
});

test('an interrupted save restores the button, preserves the draft, and allows a retry', async () => {
  const h = createHarness();
  h.context.console = { error() {} };
  h.run('render()');
  exactGuess(h);
  h.beforeTransaction(() => { throw Object.assign(new Error('Network unavailable'), { code: 'unavailable' }); });
  await h.el('submitBtn').onclick();
  assert.equal(h.writes.length, 0);
  assert.equal(h.el('submitBtn').disabled, false);
  assert.equal(h.el('bookInput').value, 'Genesis');
  assert.equal(Number(h.el('chapInput').value), 1);
  assert.equal(Number(h.el('verseInput').value), 1);
  assert.match(h.el('submitNotice').textContent, /connection interrupted/i);
  await h.el('submitBtn').onclick();
  assert.equal(h.player().score, 5);
  assert.match(h.el('submitNotice').textContent, /saved/i);
});

test('cooperative controls unlock on the timer and preserve an option across unrelated updates', () => {
  const h = createHarness();
  const start = h.now();
  h.setSession({ mode: 'coop', phaseStartedAt: timestamp(start), durationMs: 0 });
  h.run('render()');
  assert.equal(h.document.querySelectorAll('.mc-btn')[0].disabled, true);
  h.setNow(start + h.run('COOP_READING_MS'));
  h.run('updateTimer()');
  assert.equal(h.document.querySelectorAll('.mc-btn')[0].disabled, false);
  h.document.querySelectorAll('.mc-btn')[0].onclick();
  h.setPlayer({ score: 2 });
  h.run('render()');
  assert.equal(h.el('mcSubmitBtn').disabled, false);
  assert.equal(h.run('mcSelectedId'), h.verse.id);
});

test('cooperative options terminate with a narrow allowed verse pool', () => {
  const h = createHarness();
  const options = h.run("coopOptions(verses[0],['Genesis','John'])");
  assert.equal(options.length, 2);
  assert.equal(new Set(options.map(option => option.id)).size, 2);
  assert.ok(options.some(option => option.id === h.verse.id));
});

test('an old team answer cannot lock or score the next round', async () => {
  const h = createHarness();
  h.setSession({ mode: 'teams', phaseStartedAt: timestamp(h.now() - 60_000), durationMs: 75_000 });
  const guess = exactGuess(h);
  h.context.__guess = guess;
  h.beforeTransaction(() => h.setSession({ round: 2, roundKey: 2, currentQuestion: h.verse2, phaseStartedAt: timestamp(h.now()) }, { local: false }));
  await h.run('submitTeam(__guess)');
  assert.equal(h.session().teamLocks.A, false);
  assert.equal(h.session().teamTotals.A, 0);
  assert.equal(h.player().score, 0);
  assert.equal(h.writes.length, 0);
});

test('only one teammate may lock the answer in a round', async () => {
  const h = createHarness();
  h.setSession({ mode: 'teams', phaseStartedAt: timestamp(h.now() - 60_000), durationMs: 75_000 });
  h.context.__guess = exactGuess(h);
  await Promise.all([h.run('submitTeam(__guess)'), h.run('submitTeam(__guess)')]);
  assert.equal(h.session().teamLocks.A, true);
  assert.equal(h.session().teamTotals.A, 5);
  assert.equal(h.player().score, 5);
});

test('a locked Team A does not prevent Team B from submitting its own answer', async () => {
  const h = createHarness();
  h.setSession({
    mode: 'teams', phaseStartedAt: timestamp(h.now() - 60_000), durationMs: 75_000,
    teamA: ['bob'], teamB: [h.playerId],
    teamLocks: { A: true, B: false }, teamTotals: { A: 3, B: 0 }
  });
  h.context.__guess = exactGuess(h);
  await h.run('submitTeam(__guess)');
  assert.equal(h.session().teamLocks.A, true);
  assert.equal(h.session().teamLocks.B, true);
  assert.equal(h.session().teamTotals.A, 3);
  assert.equal(h.session().teamTotals.B, 5);
});

test('a team submission updates only its own fields and preserves the other team lock received after the click', async () => {
  const h = createHarness();
  h.setSession({ mode: 'teams', phaseStartedAt: timestamp(h.now() - 60_000), durationMs: 75_000 });
  h.context.__guess = exactGuess(h);
  h.beforeTransaction(() => h.setSession({
    teamTotals: { A: 0, B: 3 }, teamLocks: { A: false, B: true }
  }, { local: false }));
  await h.run('submitTeam(__guess)');
  assert.equal(h.session().teamTotals.A, 5);
  assert.equal(h.session().teamTotals.B, 3);
  assert.equal(h.session().teamLocks.A, true);
  assert.equal(h.session().teamLocks.B, true);
  assert.equal(h.session().roundResolvedAt.toMillis(), h.now());
  const update = h.writes.find(write => write.type === 'update' && write.path === h.sessionPath);
  assert.equal(update.data['teamTotals.A'], 5);
  assert.equal(update.data['teamLocks.A'], true);
  assert.equal(Object.hasOwn(update.data, 'teamTotals'), false);
  assert.equal(Object.hasOwn(update.data, 'teamLocks'), false);
  assert.equal(Object.hasOwn(update.data, 'teamTotals.B'), false);
  assert.equal(Object.hasOwn(update.data, 'teamLocks.B'), false);
});

test('an old cooperative choice cannot consume a life on the next question', async () => {
  const h = createHarness();
  h.setSession({ mode: 'coop', phaseStartedAt: timestamp(h.now() - 10_000), durationMs: 0 });
  h.run(`mcSelectedId=${h.verse.id}`);
  h.beforeTransaction(() => h.setSession({ round: 2, roundKey: 2, currentQuestion: h.verse2, phaseStartedAt: timestamp(h.now() - 10_000) }, { local: false }));
  await h.run('submitCoop()');
  assert.equal(h.session().sharedLives, 3);
  assert.equal(h.session().coopLocked, false);
  assert.equal(h.player().score, 0);
  assert.equal(h.writes.length, 0);
});

test('simultaneous cooperative answers change the shared lives only once', async () => {
  const h = createHarness();
  h.setSession({ mode: 'coop', phaseStartedAt: timestamp(h.now() - 10_000), durationMs: 0 });
  h.run(`mcSelectedId=${h.verse2.id}`);
  await Promise.all([h.run('submitCoop()'), h.run('submitCoop()')]);
  assert.equal(h.session().sharedLives, 2);
  assert.equal(h.session().coopLocked, true);
  assert.equal(h.writes.filter(write => write.path === h.sessionPath).length, 1);
});

test('two speed clicks for the displayed question cannot also answer the next question', async () => {
  const h = createHarness();
  h.setSession({ mode: 'speed', durationMs: 90_000 });
  h.context.__guess = exactGuess(h);
  await Promise.all([h.run('submitSpeed(__guess)'), h.run('submitSpeed(__guess)')]);
  assert.equal(h.player().questionKey, 2);
  assert.equal(h.player().score, 5);
  assert.equal(h.writes.filter(write => write.path === h.playerPath).length, 1);
});

for (const mode of ['traditional', 'speed', 'teams', 'category']) {
  test(`${mode} rejects an answer when its deadline expires before the transaction reads`, async () => {
    const h = createHarness();
    const durationMs = mode === 'teams' ? 75_000 : mode === 'speed' || mode === 'category' ? 90_000 : 40_000;
    h.setSession({ mode, phaseStartedAt: timestamp(h.now() - durationMs + 100), durationMs });
    h.context.__guess = exactGuess(h);
    h.el('categoryRefInput').value = 'Genesis 1:1';
    h.beforeTransaction(() => h.setNow(h.now() + 100));
    const method = { traditional: 'submitTraditional(__guess)', speed: 'submitSpeed(__guess)', teams: 'submitTeam(__guess)', category: 'submitCategory()' }[mode];
    await h.run(method);
    assert.equal(h.player().score, 0);
    assert.equal(h.writes.length, 0);
  });
}

test('a traditional player can score only once per round', async () => {
  const h = createHarness();
  h.context.__guess = exactGuess(h);
  await Promise.all([h.run('submitTraditional(__guess)'), h.run('submitTraditional(__guess)')]);
  assert.equal(h.player().score, 5);
  assert.equal(h.writes.filter(write => write.path.includes('/submissions/')).length, 1);
});

test('final round expiration is an atomic transaction and cannot end a newer round', async () => {
  const h = createHarness();
  h.setSession({ round: 6, roundKey: 6, phaseStartedAt: timestamp(h.now() - 40_000) });
  h.beforeTransaction(() => h.setSession({ round: 1, roundKey: 7, phaseStartedAt: timestamp(h.now()) }, { local: false }));
  await h.run('advanceRound()');
  assert.equal(h.session().status, 'playing');
  assert.equal(h.session().roundKey, 7);
  assert.equal(h.writes.length, 0);
});

test('two drivers may expire the final round only once', async () => {
  const h = createHarness();
  h.setSession({ round: 6, roundKey: 6, phaseStartedAt: timestamp(h.now() - 40_000) });
  await Promise.all([h.run('advanceRound()'), h.run('advanceRound()')]);
  assert.equal(h.session().status, 'ended');
  assert.equal(h.writes.length, 1);
  assert.equal(h.transactions.filter(transaction => transaction.length).length, 1);
});

test('a delayed round driver cannot advance before the exact deadline', async () => {
  const h = createHarness();
  h.setSession({ phaseStartedAt: timestamp(h.now() - 39_999), durationMs: 40_000 });
  await h.run('advanceRound()');
  assert.equal(h.session().round, 1);
  assert.equal(h.writes.length, 0);
});

test('a participant advances a frozen fresh host round only after the fallback grace', async () => {
  const h = createHarness();
  makeFreshHost(h);
  const due = h.now();
  h.setSession({ phaseStartedAt: timestamp(due - 40_000), durationMs: 40_000 });
  h.setNow(due + 1799); // 1500ms grace + 300ms for roster position 1.
  await h.run('drive()');
  assert.equal(h.session().roundKey, 1);
  assert.equal(h.writes.length, 0);
  h.setNow(due + 1800);
  await h.run('drive()');
  assert.equal(h.run('driver()'), false);
  assert.equal(h.run('isFresh(PLAYERS.get(SESSION.hostUid))'), true);
  assert.equal(h.session().round, 2);
  assert.equal(h.session().roundKey, 2);
  assert.equal(h.writes.filter(write => write.path === h.sessionPath).length, 1);
});

test('a participant unlocks a frozen host co-op reveal after grace and a racing driver cannot advance twice', async () => {
  const h = createHarness();
  makeFreshHost(h);
  const revealDue = h.now();
  const lockedAt = revealDue - h.run('COOP_REVEAL_MS');
  h.setSession({ mode: 'coop', durationMs: 0, coopLocked: true, coopLockedAt: timestamp(lockedAt) });
  h.setNow(revealDue + 1799);
  await h.run('drive()');
  assert.equal(h.session().coopLocked, true);
  assert.equal(h.writes.length, 0);
  h.setNow(revealDue + 1800);
  // The direct transition represents a second driver's attempt with the same
  // round snapshot; it must lose the transaction comparison after fallback wins.
  await Promise.all([h.run('drive()'), h.run('advanceCoop()')]);
  assert.equal(h.run('driver()'), false);
  assert.equal(h.run('isFresh(PLAYERS.get(SESSION.hostUid))'), true);
  assert.equal(h.session().coopLocked, false);
  assert.equal(h.session().round, 2);
  assert.equal(h.session().roundKey, 2);
  assert.equal(h.session().chosenIds.length, 2);
  assert.equal(h.writes.filter(write => write.path === h.sessionPath).length, 1);
});

test('fallback participants leave expired voting to the fresh host', async () => {
  const h = createHarness();
  makeFreshHost(h);
  h.setSession({ status: 'voting', mode: null, round: 0, roundKey: 0, phaseStartedAt: timestamp(h.now() - 30_000), durationMs: 30_000 });
  h.setNow(h.now() + 3000);
  await h.run('drive()');
  assert.equal(h.session().status, 'voting');
  assert.equal(h.writes.length, 0);
});

test('a stale Play Again request cannot replace a session already started by another driver', async () => {
  const h = createHarness();
  h.setSession({ status: 'ended', ratingsApplied: true });
  h.run('savedRatings.add(currentSessionId)');
  const controlPath = 'lobbies/test/bibleGame/control';
  h.beforeTransaction(() => h.docs.set(controlPath, { activeSessionId: 'already-new-session', hostUid: h.playerId }));
  await h.run('createFreshSession()');
  assert.equal(h.docs.get(controlPath).activeSessionId, 'already-new-session');
  assert.equal(h.writes.length, 0);
});

test('Play Again waits for stored ratings even when the local snapshot is ahead', async () => {
  const h = createHarness();
  h.setSession({ status: 'ended', ratingsApplied: true });
  h.run('savedRatings.add(currentSessionId)');
  h.setSession({ ratingsApplied: false }, { local: false });
  await h.run('createFreshSession()');
  assert.equal(h.docs.get('lobbies/test/bibleGame/control').activeSessionId, 'test-session');
  assert.equal(h.writes.length, 0);
});

test('another active connection cannot submit from a stale local snapshot', async () => {
  const h = createHarness();
  h.context.__guess = exactGuess(h);
  h.setPlayer({ connectionId: 'newer-device' }, { local: false });
  await h.run('submitTraditional(__guess)');
  assert.equal(h.player().score, 0);
  assert.equal(h.writes.length, 0);
});

test('a player removed from the roster before the transaction cannot submit', async () => {
  const h = createHarness();
  h.context.__guess = exactGuess(h);
  h.beforeTransaction(() => h.setSession({ participantUids: ['bob'] }, { local: false }));
  await h.run('submitTraditional(__guess)');
  assert.equal(h.player().score, 0);
  assert.equal(h.writes.length, 0);
});

test('a team change before the transaction cannot score the previous team', async () => {
  const h = createHarness();
  h.setSession({ mode: 'teams', phaseStartedAt: timestamp(h.now() - 60_000), durationMs: 75_000 });
  h.context.__guess = exactGuess(h);
  h.beforeTransaction(() => h.setSession({ teamA: ['bob'], teamB: [h.playerId] }, { local: false }));
  await h.run('submitTeam(__guess)');
  assert.equal(h.session().teamTotals.A, 0);
  assert.equal(h.session().teamLocks.A, false);
  assert.equal(h.player().score, 0);
  assert.equal(h.writes.length, 0);
});

test('a stale heartbeat or leave cannot reclaim or disconnect a newer connection', async () => {
  const h = createHarness();
  h.run('startHeartbeat()');
  h.setPlayer({ connectionId: 'newer-device' }, { local: false });
  for (const callback of h.intervals.values()) await callback();
  await h.run('markOffline()');
  assert.equal(h.player().connectionId, 'newer-device');
  assert.equal(h.player().online, true);
});

test('clock calibration is unchanged when a score snapshot repeats the same clock probe', async () => {
  const h = createHarness();
  h.run("currentSessionId='' ");
  await h.run("attachSession('test-session')");
  const probeClient = h.now() - 100;
  const me = { ...h.player(), clockProbe: timestamp(h.now() + 450), clockProbeClientMs: probeClient };
  h.emitCollection(`${h.sessionPath}/players`, [[h.playerId, me]]);
  const initial = h.run('serverClockOffsetMs');
  h.setNow(h.now() + 20_000);
  h.emitCollection(`${h.sessionPath}/players`, [[h.playerId, { ...me, score: 5 }]]);
  assert.equal(h.run('serverClockOffsetMs'), initial);
});

test('unacknowledged clock probes do not recalibrate the timer', async () => {
  const h = createHarness();
  h.run("currentSessionId='' ");
  await h.run("attachSession('test-session')");
  const me = { ...h.player(), clockProbe: timestamp(h.now() + 5000), clockProbeClientMs: h.now() };
  const initial = h.run('serverClockOffsetMs');
  h.emitCollection(`${h.sessionPath}/players`, [[h.playerId, me]], { hasPendingWrites: true });
  assert.equal(h.run('serverClockOffsetMs'), initial);
});

test('answer controls wait for authoritative game and player snapshots after reconnecting', async () => {
  const h = createHarness();
  h.run("currentSessionId='' ");
  await h.run("attachSession('test-session')");
  h.emitDoc(h.sessionPath);
  h.emitCollection(`${h.sessionPath}/players`, [[h.playerId, h.player()]], { fromCache: true });
  assert.equal(h.el('submitBtn').disabled, true);
  h.emitCollection(`${h.sessionPath}/players`, [[h.playerId, h.player()]]);
  assert.equal(h.el('submitBtn').disabled, false);
  h.emitDoc(h.sessionPath, h.session(), { fromCache: true });
  assert.equal(h.el('submitBtn').disabled, true);
  const writes = h.writes.length;
  exactGuess(h);
  await h.run('submitStandard()');
  assert.equal(h.writes.length, writes);
  h.emitDoc(h.sessionPath);
  assert.equal(h.el('submitBtn').disabled, false);
  h.run('navigator.onLine=false;updateTimer()');
  assert.equal(h.el('submitBtn').disabled, true);
  await h.run('submitStandard()');
  assert.equal(h.writes.length, writes);
});

test('changing sessions replaces listeners and ignores an obsolete session callback', async () => {
  const h = createHarness();
  h.run("currentSessionId='' ");
  await h.run("attachSession('test-session')");
  assert.equal(h.snapshotListenerCount(h.sessionPath), 1);
  const obsoleteCallback = h.captureDocListener(h.sessionPath);
  const newPath = 'lobbies/test/bibleSessions/new-session';
  h.docs.set(newPath, { ...h.session(), sessionId: 'new-session' });
  await h.run("attachSession('new-session')");
  assert.equal(h.snapshotListenerCount(h.sessionPath), 0);
  assert.equal(h.snapshotListenerCount(newPath), 1);
  h.emitDoc(newPath);
  // Firestore may have queued a callback before unsubscribe completed.
  obsoleteCallback({ ...h.session(), roundKey: 99 });
  assert.equal(h.run('currentSessionId'), 'new-session');
  assert.notEqual(h.run('SESSION.roundKey'), 99);
});

test('the match driver finalizes results without writing another account profile', async () => {
  const h = createHarness();
  const bobId = h.run("stablePlayerUid('Bob')");
  h.docs.set(`${h.sessionPath}/players/${bobId}`, {
    uid: bobId, username: 'Bob', profileKey: 'Bob', authUid: 'auth-bob',
    connectionId: 'bob-device', score: 3, elo: 0
  });
  h.docs.set('users/Bob', { username: 'Bob', chatAuthUid: 'auth-bob', bibleGame: { elo: 0 } });
  h.setSession({
    status: 'ended', mode: 'teams', ratingsApplied: false,
    participantUids: [h.playerId, bobId], teamA: [h.playerId], teamB: [bobId],
    teamTotals: { A: 5, B: 3 }
  });
  await h.run('applyRatings()');
  assert.equal(h.session().ratingsApplied, true);
  assert.equal(h.session().ratingResults[h.playerId].outcome, 'win');
  assert.equal(h.session().ratingResults[bobId].outcome, 'loss');
  assert.equal(h.writes.filter(write => write.path.startsWith('users/')).length, 0);
  assert.equal(h.docs.get('users/Bob').bibleGame.elo, 0);
});

test('each account saves only its own rating once, including after a refresh', async () => {
  const h = createHarness();
  const bobId = h.run("stablePlayerUid('Bob')");
  h.docs.set('users/Bob', { username: 'Bob', chatAuthUid: 'auth-bob', bibleGame: { elo: 50 } });
  h.setSession({ status: 'ended', ratingsApplied: true, ratingResults: {
    [h.playerId]: { uid: h.playerId, profileKey: 'Alice', outcome: 'win', opponentElo: 0 },
    [bobId]: { uid: bobId, profileKey: 'Bob', outcome: 'loss', opponentElo: 0 }
  } });
  await Promise.all([h.run('saveOwnRating()'), h.run('saveOwnRating()')]);
  h.run('savedRatings.clear()');
  await h.run('saveOwnRating()');
  const alice = h.docs.get('users/Alice').bibleGame;
  assert.equal(alice.gamesPlayed, 1);
  assert.equal(alice.wins, 1);
  assert.equal(h.docs.get('users/Bob').bibleGame.elo, 50);
  assert.equal(h.writes.filter(write => write.path === 'users/Alice').length, 1);
  assert.equal(h.writes.filter(write => write.path === 'users/Alice/bibleGameResults/test-session').length, 1);
  assert.equal(h.writes.filter(write => write.path.startsWith('users/Bob')).length, 0);
});

test('a rating receipt rejected for account ownership is not marked saved locally', async () => {
  const h = createHarness();
  h.setSession({ status: 'ended', ratingsApplied: true, ratingResults: {
    [h.playerId]: { uid: h.playerId, profileKey: 'Alice', outcome: 'win', opponentElo: 0 }
  } });
  h.docs.set('users/Alice', { username: 'Alice', chatAuthUid: 'different-auth', bibleGame: { elo: 0 } });
  await h.run('saveOwnRating()');
  assert.equal(h.writes.length, 0);
  assert.equal(h.run('savedRatings.has(currentSessionId)'), false);
  assert.equal(h.run('canStartAgain()'), false);
});

test('returning to a later lobby recovers an old rating once and removes its inbox entry', async () => {
  const h = createHarness();
  const oldSession = 'bible_older-match';
  const oldLobby = 'previous-lobby';
  const inboxPath = `bibleGameRatingInbox/auth-alice/matches/${oldSession}`;
  h.docs.set(inboxPath, { sessionId: oldSession, lobbyId: oldLobby, uid: h.playerId, profileKey: 'Alice' });
  h.docs.set(`lobbies/${oldLobby}/bibleSessions/${oldSession}`, {
    sessionId: oldSession, status: 'ended', mode: 'traditional', ratingsApplied: true,
    ratingResults: { [h.playerId]: { uid: h.playerId, profileKey: 'Alice', outcome: 'win', opponentElo: 0 } }
  });
  await h.run('recoverRatings()');
  const writesAfterRecovery = h.writes.length;
  await h.run('recoverRatings()');
  assert.equal(h.docs.get('users/Alice').bibleGame.gamesPlayed, 1);
  assert.equal(h.docs.get('users/Alice').bibleGame.wins, 1);
  assert.equal(h.docs.has(inboxPath), false);
  assert.equal(h.docs.get(`users/Alice/bibleGameResults/${oldSession}`).lobbyId, oldLobby);
  assert.equal(h.writes.length, writesAfterRecovery);
  assert.equal(h.run('currentSessionId'), 'test-session');
});

test('joining preserves profile statistics updated by a concurrent rating transaction', async () => {
  const h = createHarness();
  h.beforeTransaction(() => h.docs.set('users/Alice', {
    username: 'Alice', chatAuthUid: 'auth-alice',
    bibleGame: { elo: 125, gamesPlayed: 3, wins: 2, losses: 1 }
  }));
  await h.run('ensureProfile()');
  const profile = h.docs.get('users/Alice').bibleGame;
  assert.equal(profile.elo, 125);
  assert.equal(profile.gamesPlayed, 3);
  assert.equal(profile.wins, 2);
  assert.equal(profile.losses, 1);
});
