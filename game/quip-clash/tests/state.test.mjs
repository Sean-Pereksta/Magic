import test from 'node:test';
import assert from 'node:assert/strict';
import { MAX_PLAYERS, WRITE_MS, VOTE_MS } from '../core.mjs';
import {
  REVEAL_MS, SCORES_MS, phaseKey, deadlineOf, playerIds, assignmentsFor, answersFor, votersFor,
  createRoom, joinRoom, leaveRoom, startRoom, saveResponses, castVote, advanceRoom, replayRoom,
} from '../state.mjs';

const identities = count => Array.from({ length: count }, (_, index) => ({
  uid: `uid-${index}`, username: `account${index}`, displayName: `Player ${index}`,
}));
const setup = (count = 4, now = 1000) => {
  const people = identities(count);
  let room = createRoom(people[0], { name: 'Test room', seed: `seed-${count}`, now });
  for (const person of people.slice(1)) room = joinRoom(room, person);
  return { people, room };
};
const start = (count = 4, now = 1000) => {
  const match = setup(count, now);
  match.room = startRoom(match.room, match.people[0], now);
  return match;
};
const textFor = (room, person) => Object.fromEntries(assignmentsFor(room, person.uid).map(question => [question.id, `Funny answer by ${person.uid} for ${question.id}`]));
function lockEveryone(room, people, now) {
  for (const person of people) room = saveResponses(room, person, textFor(room, person), now, true);
  return room;
}
function voteEveryone(room, people, now) {
  const eligible = votersFor(room);
  for (const person of people.filter(person => eligible.includes(person.uid))) {
    const options = answersFor(room).filter(answer => answer.uid !== person.uid);
    room = castVote(room, person, options[eligible.indexOf(person.uid) % options.length].id, phaseKey(room), now);
  }
  return room;
}
const totalScores = room => room.roster.reduce((sum, person) => sum + person.score, 0);
const totalLedger = room => Object.values(room.receipts).reduce((sum, receipt) => sum + Object.values(receipt.awards).reduce((subtotal, points) => subtotal + points, 0), 0);

test('room membership uses account seats, preserves names, and enforces lobby/start limits', () => {
  const { people, room } = setup(MAX_PLAYERS);
  assert.equal(room.roster.length, MAX_PLAYERS);
  assert.deepEqual(playerIds(room), people.map(person => person.uid));
  assert.strictEqual(joinRoom(room, people[1]), room);
  assert.throws(() => joinRoom(room, identities(MAX_PLAYERS + 1).at(-1)), /full/);
  assert.throws(() => startRoom(room, people[1], 1000), /host/);
  const tooSmall = setup(3);
  assert.throws(() => startRoom(tooSmall.room, tooSmall.people[0], 1000), /at least 4/);
  const started = startRoom(room, people[0], 1000);
  assert.equal(started.phase, 'writing');
  assert.equal(started.phaseDurationMs, WRITE_MS);
  assert.equal(started.roster[2].displayName, people[2].displayName);
  assert.strictEqual(joinRoom(started, people[2]), started, 'Existing players can reconnect after start');
  assert.throws(() => joinRoom(started, { uid: 'new', username: 'new', displayName: 'New' }), /already started/);
});

test('waiting players can leave, host transfers, and started-game seats remain stable', () => {
  const { people, room } = setup(4);
  const left = leaveRoom(room, people[0]);
  assert.equal(left.hostUid, people[1].uid);
  assert.equal(left.host, people[1].username);
  assert.ok(!Object.hasOwn(left.members, people[0].uid));
  assert.equal(left.roster.length, 3);
  const active = start(4);
  assert.strictEqual(leaveRoom(active.room, active.people[0]), active.room);
  const lone = setup(1);
  const closed = leaveRoom(lone.room, lone.people[0]);
  assert.equal(closed.status, 'closed');
  assert.equal(closed.phase, 'closed');
});

test('switching site accounts on one anonymous Firebase device cannot create or inherit a seat', () => {
  const { people, room } = setup(4);
  const switched = { ...people[0], username: 'different-account', displayName: 'Different account' };
  assert.throws(() => joinRoom(room, switched), /already in the room with another account/);
  assert.equal(room.roster.length, 4);
  assert.throws(() => startRoom(room, switched, 1000), /Join/);
  const started = startRoom(room, people[0], 1000);
  assert.throws(() => joinRoom(started, switched), /already in the room with another account/);
  assert.equal(started.roster[0].displayName, people[0].displayName);
  assert.equal(started.roster[0].username, people[0].username);
  assert.throws(() => saveResponses(started, switched, textFor(started, switched), 1001, true), /Join/);
});

test('partial answers save independently, remain available after reconnect, and lock once', () => {
  const { people, room } = start(5);
  const person = people[0], assigned = assignmentsFor(room, person.uid);
  assert.equal(assigned.length, 2);
  const partial = saveResponses(room, person, { [assigned[0].id]: 'First saved answer' }, 1001);
  assert.equal(partial.responses[person.uid][assigned[0].id], 'First saved answer');
  assert.equal(partial.responses[person.uid][assigned[1].id], '');
  assert.ok(!partial.ready[person.uid]);
  assert.throws(() => saveResponses(partial, person, {}, 1002, true), /each prompt/);
  const reconnected = JSON.parse(JSON.stringify(partial));
  const complete = saveResponses(reconnected, person, { [assigned[1].id]: 'Second saved answer' }, 1003);
  assert.deepEqual(complete.responses[person.uid], {
    [assigned[0].id]: 'First saved answer', [assigned[1].id]: 'Second saved answer',
  });
  const locked = saveResponses(complete, person, {}, 1004, true);
  assert.equal(locked.ready[person.uid], true);
  assert.strictEqual(saveResponses(locked, person, { [assigned[0].id]: 'Trying to overwrite' }, 1005), locked);
  assert.equal(locked.responses[person.uid][assigned[0].id], 'First saved answer');
  assert.deepEqual(room.responses, {}, 'Saving does not mutate a prior snapshot');
  assert.throws(() => saveResponses(partial, people[1], { [assignmentsFor(partial, people[1].uid)[0].id]: 'x'.repeat(1001) }, 1002), /1001/);
});

test('writing requires locks for early completion and advances exactly at 90 seconds otherwise', () => {
  const { people, room } = start();
  let saved = room;
  for (const person of people) saved = saveResponses(saved, person, textFor(saved, person), 1001);
  assert.strictEqual(advanceRoom(saved, people[0], phaseKey(saved), 1002, 1002), saved);
  const locked = lockEveryone(saved, people, 1003);
  const early = advanceRoom(locked, people[2], phaseKey(locked), 1004, 1004);
  assert.equal(early.phase, 'voting');
  assert.equal(early.phaseDurationMs, VOTE_MS);
  assert.equal(deadlineOf(early), 1004 + VOTE_MS);
  assert.strictEqual(advanceRoom(room, people[0], phaseKey(room), 1000 + WRITE_MS - 1, 1234), room);
  const timed = advanceRoom(room, people[1], phaseKey(room), 1000 + WRITE_MS, 1000 + WRITE_MS);
  assert.equal(timed.phase, 'voting');
  assert.equal(timed.phaseStartedAt, 1000 + WRITE_MS);
  assert.throws(() => saveResponses(room, people[0], textFor(room, people[0]), 1000 + WRITE_MS, true), /Time is up/);
});

test('server timestamps determine deadlines, and pending timestamps never accept a timed action', () => {
  const { people, room } = start();
  const timestamp = { toMillis: () => 5000 };
  assert.equal(deadlineOf({ ...room, phaseStartedAt: timestamp }), 5000 + WRITE_MS);
  const pending = { ...room, phaseStartedAt: { pending: true } };
  assert.equal(deadlineOf(pending), null);
  assert.throws(() => saveResponses(pending, people[0], textFor(pending, people[0]), 5000), /Time is up/);
  assert.strictEqual(advanceRoom(pending, people[0], phaseKey(pending), 10_000_000, 10_000_000), pending);
});

test('only valid eligible votes lock, retried votes cannot change, and 20 seconds closes voting', () => {
  const { people, room } = start(5);
  const written = lockEveryone(room, people, 1001);
  const voting = advanceRoom(written, people[0], phaseKey(written), 1002, 1002);
  const author = people.find(person => voting.questions[0].authors.includes(person.uid));
  const voter = people.find(person => votersFor(voting).includes(person.uid));
  const options = answersFor(voting);
  assert.throws(() => castVote(voting, author, options[0].id, phaseKey(voting), 1003), /sitting out/);
  assert.throws(() => castVote(voting, voter, 'missing', phaseKey(voting), 1003), /another player/);
  const one = castVote(voting, voter, options[0].id, phaseKey(voting), 1003);
  assert.strictEqual(castVote(one, voter, options.at(-1).id, phaseKey(one), 1004), one);
  assert.strictEqual(advanceRoom(one, people[0], phaseKey(one), 1004, 1004), one);
  assert.strictEqual(advanceRoom(one, people[0], phaseKey(one), deadlineOf(one) - 1, 1234), one);
  const revealed = advanceRoom(one, people[3], phaseKey(one), deadlineOf(one), deadlineOf(one));
  assert.equal(revealed.phase, 'reveal');
  assert.equal(revealed.phaseDurationMs, REVEAL_MS);
  assert.equal(totalScores(revealed), 1000);
  assert.throws(() => castVote(voting, voter, options[0].id, phaseKey(voting), deadlineOf(voting)), /Time is up/);
  const all = voteEveryone(voting, people, 1003);
  assert.equal(advanceRoom(all, people[1], phaseKey(all), 1004, 1004).phase, 'reveal');
});

test('stale phase requests cannot vote on or advance a later matchup', () => {
  const { people, room } = start();
  const written = lockEveryone(room, people, 1001);
  const writingKey = phaseKey(written);
  let voting = advanceRoom(written, people[0], writingKey, 1002, 1002);
  assert.strictEqual(advanceRoom(voting, people[0], writingKey, 999999, 999999), voting);
  const firstKey = phaseKey(voting);
  const voted = voteEveryone(voting, people, 1003);
  const revealed = advanceRoom(voted, people[0], firstKey, 1004, 1004);
  voting = advanceRoom(revealed, people[0], phaseKey(revealed), 1005, 1005, true);
  assert.equal(voting.phase, 'voting');
  assert.equal(voting.questionIndex, 1);
  const voter = people.find(person => votersFor(voting).includes(person.uid));
  assert.throws(() => castVote(voting, voter, answersFor(voting)[0].id, firstKey, 1006), /already ended/);
  assert.strictEqual(advanceRoom(voting, people[0], firstKey, 999999, 999999), voting);
  assert.equal(Object.keys(voting.receipts).length, 1);
});

test('invalid or self ballots never satisfy early voting completion', () => {
  const { people, room } = start();
  const written = lockEveryone(room, people, 1001);
  const voting = advanceRoom(written, people[0], phaseKey(written), 1002, 1002);
  const invalid = { ...voting, votes: Object.fromEntries(votersFor(voting).map(uid => [uid, 'not-an-option'])) };
  assert.strictEqual(advanceRoom(invalid, people[0], phaseKey(invalid), 1003, 1003), invalid);
  const timedOut = advanceRoom(invalid, people[0], phaseKey(invalid), deadlineOf(invalid), deadlineOf(invalid));
  assert.equal(timedOut.phase, 'reveal');
  assert.equal(totalScores(timedOut), 0);
  const finalQuestion = { id: 'r3-q1', text: 'Final question', authors: people.map(person => person.uid) };
  const final = {
    ...voting, round: 3, questions: [finalQuestion],
    responses: Object.fromEntries(people.map(person => [person.uid, { [finalQuestion.id]: `${person.uid} responds` }])),
    votes: Object.fromEntries(people.map(person => [person.uid, `${finalQuestion.id}_${person.uid}`])),
  };
  assert.strictEqual(advanceRoom(final, people[0], phaseKey(final), 1003, 1003), final);
  assert.equal(totalScores(advanceRoom(final, people[0], phaseKey(final), deadlineOf(final), deadlineOf(final))), 0);
});

test('score receipts are idempotent across competing transitions and reconnect replay', () => {
  const { people, room } = start();
  const written = lockEveryone(room, people, 1001);
  const voting = voteEveryone(advanceRoom(written, people[0], phaseKey(written), 1002, 1002), people, 1003);
  const key = phaseKey(voting);
  const revealed = advanceRoom(voting, people[0], key, 1004, 1004);
  const receiptsBefore = JSON.stringify(revealed.receipts);
  const scoresBefore = revealed.roster.map(person => person.score);
  assert.strictEqual(advanceRoom(revealed, people[1], key, 1005, 1005), revealed);
  // A recovered voting state that already contains its receipt must reuse the ledger.
  const recovered = { ...JSON.parse(JSON.stringify(revealed)), phase: 'voting', phaseStartedAt: 1006, votes: {} };
  const rerevealed = advanceRoom(recovered, people[2], phaseKey(recovered), deadlineOf(recovered), deadlineOf(recovered));
  assert.equal(JSON.stringify(rerevealed.receipts), receiptsBefore);
  assert.deepEqual(rerevealed.roster.map(person => person.score), scoresBefore);
  assert.equal(totalScores(rerevealed), 1000);
  assert.equal(totalScores(rerevealed), totalLedger(rerevealed));
  assert.deepEqual(voting.receipts, {}, 'Scoring leaves the previous snapshot unchanged');
});

test('saved answers survive a writing timeout and missing responses receive no points', () => {
  const { people, room } = start(5);
  const target = room.questions[0], author = people.find(person => person.uid === target.authors[0]);
  const partial = saveResponses(room, author, { [target.id]: 'My saved but unlocked answer' }, 1001);
  const voting = advanceRoom(partial, people[2], phaseKey(partial), deadlineOf(partial), deadlineOf(partial));
  assert.deepEqual(answersFor(voting).map(answer => answer.uid), [author.uid]);
  assert.ok(!votersFor(voting).some(uid => target.authors.includes(uid)), 'Every assigned author sits out even after a missed response');
  const all = voteEveryone(voting, people, deadlineOf(partial) + 1);
  const revealed = advanceRoom(all, people[0], phaseKey(all), deadlineOf(partial) + 2, deadlineOf(partial) + 2);
  assert.equal(revealed.roster.find(person => person.uid === author.uid).score, 1000);
  for (const uid of target.authors.slice(1)) assert.equal(revealed.roster.find(person => person.uid === uid).score, 0);
});

test('empty matchups skip safely and reveal/scores can be continued by host or timed out by anyone', () => {
  const { people, room } = start();
  let current = advanceRoom(room, people[1], phaseKey(room), deadlineOf(room), deadlineOf(room));
  assert.deepEqual(votersFor(current), []);
  current = advanceRoom(current, people[1], phaseKey(current), current.phaseStartedAt + 1, current.phaseStartedAt + 1);
  assert.equal(current.phase, 'reveal');
  assert.equal(totalScores(current), 0);
  assert.strictEqual(advanceRoom(current, people[1], phaseKey(current), current.phaseStartedAt + 1, 123, true), current);
  assert.strictEqual(advanceRoom(current, people[0], phaseKey(current), deadlineOf(current) - 1, 123), current);
  const forced = advanceRoom(current, people[0], phaseKey(current), current.phaseStartedAt + 1, current.phaseStartedAt + 1, true);
  assert.equal(forced.phase, 'voting');
  assert.equal(forced.questionIndex, 1);
  while (current.phase !== 'scores') current = advanceRoom(current, people[2], phaseKey(current), deadlineOf(current), deadlineOf(current));
  assert.equal(current.phaseDurationMs, SCORES_MS);
  assert.strictEqual(advanceRoom(current, people[2], phaseKey(current), deadlineOf(current) - 1, 123, true), current);
  const nextRound = advanceRoom(current, people[2], phaseKey(current), deadlineOf(current), deadlineOf(current));
  assert.equal(nextRound.round, 2);
  assert.equal(nextRound.phase, 'writing');
});

for (const count of [4, 5, 7, 12]) {
  test(`complete ${count}-player match stays synced, conserves pools, finishes, and rematches cleanly`, () => {
    const { people, room } = start(count);
    let current = room, now = 1001, transitions = 0;
    while (current.phase !== 'finished') {
      assert.ok(++transitions < 150, 'Match should make finite progress');
      const before = current;
      if (current.phase === 'writing') {
        current = lockEveryone(current, people, now++);
        current = advanceRoom(current, people[1], phaseKey(current), now, now++);
      } else if (current.phase === 'voting') {
        current = voteEveryone(current, people, now++);
        current = advanceRoom(current, people[1], phaseKey(current), now, now++);
      } else {
        current = advanceRoom(current, people[0], phaseKey(current), now, now++, true);
      }
      assert.notStrictEqual(current, before);
      assert.equal(totalScores(current), totalLedger(current));
      for (const person of current.roster) {
        assert.ok(Number.isInteger(person.score) && person.score >= 0);
        assert.equal(person.displayName, people.find(player => player.uid === person.uid).displayName);
      }
    }
    const questionCount = count % 2 ? count - 1 : count;
    assert.equal(current.status, 'finished');
    assert.equal(current.round, 3);
    assert.equal(Object.keys(current.receipts).length, questionCount * 2 + 1);
    assert.equal(totalScores(current), questionCount * 3000 + 3000);
    assert.equal(new Set(current.usedPromptIds).size, current.usedPromptIds.length);
    const normal = Object.values(current.receipts).filter(receipt => receipt.round !== 3);
    for (const receipt of normal) for (const uid of receipt.question.authors) assert.ok(!Object.hasOwn(receipt.ballots, uid));
    const finalReceipt = Object.values(current.receipts).find(receipt => receipt.round === 3);
    assert.equal(finalReceipt.totalVotes, count);
    for (const [uid, id] of Object.entries(finalReceipt.ballots)) assert.notEqual(finalReceipt.answers.find(answer => answer.id === id).uid, uid);
    const oldKey = phaseKey(current);
    assert.throws(() => replayRoom(current, people[1], 'next-match', now), /host/);
    const rematch = replayRoom(current, people[0], 'next-match', now);
    assert.equal(rematch.phase, 'waiting');
    assert.equal(rematch.matchId, 'next-match');
    assert.deepEqual(rematch.receipts, {});
    assert.deepEqual(rematch.responses, {});
    assert.deepEqual(rematch.ready, {});
    assert.deepEqual(rematch.usedPromptIds, []);
    assert.equal(totalScores(rematch), 0);
    assert.deepEqual(rematch.roster.map(person => person.displayName), people.map(person => person.displayName));
    assert.strictEqual(advanceRoom(rematch, people[0], oldKey, now, now), rematch);
    assert.equal(startRoom(rematch, people[0], now).round, 1);
  });
}

test('a fully timed-out match completes all rounds without inventing points', () => {
  const { people, room } = start(5);
  let current = room, transitions = 0;
  while (current.phase !== 'finished') {
    assert.ok(++transitions < 100);
    current = advanceRoom(current, people[3], phaseKey(current), deadlineOf(current), deadlineOf(current));
    assert.equal(totalScores(current), 0);
  }
  assert.equal(Object.keys(current.receipts).length, 9);
  assert.ok(Object.values(current.receipts).every(receipt => receipt.totalVotes === 0));
});

test('an unjoined account cannot write, vote, advance, leave, start, or replay', () => {
  const { people, room } = start();
  const outsider = { uid: 'outside', username: 'outside', displayName: 'Outside' };
  assert.throws(() => saveResponses(room, outsider, {}, 1001), /Join/);
  assert.throws(() => castVote(room, outsider, 'answer', phaseKey(room), 1001), /Join/);
  assert.throws(() => advanceRoom(room, outsider, phaseKey(room), 1001, 1001), /Join/);
  assert.throws(() => leaveRoom(room, outsider), /Join/);
  assert.throws(() => startRoom(room, outsider, 1001), /Join/);
  assert.throws(() => replayRoom(room, outsider, 'seed', 1001), /Join/);
  assert.strictEqual(joinRoom(room, people[0]), room);
});
