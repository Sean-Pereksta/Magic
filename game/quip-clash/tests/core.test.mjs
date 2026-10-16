import test from "node:test";
import assert from "node:assert/strict";
import {
  MIN_PLAYERS, MAX_PLAYERS, MAX_ANSWER_LENGTH, WRITE_MS, VOTE_MS,
  buildQuestions, eligibleVoters, scoreQuestion, canFinishWriting, canFinishVoting, isValidAnswer,
} from "../core.mjs";
import { NORMAL_PROMPTS, FINAL_PROMPTS } from "../prompts.mjs";

const players = count => Array.from({ length: count }, (_, index) => `player-${String(index).padStart(2, "0")}`);
const question = { id: "r1-q1", text: "A funny thing.", authors: ["a", "b"] };
const four = ["a", "b", "c", "d"];
const options = [
  { id: "answer-a", uid: "a", text: "First answer" },
  { id: "answer-b", uid: "b", text: "Second answer" },
];

test("timers, player bounds, and long-answer limit match the game rules", () => {
  assert.equal(MIN_PLAYERS, 4);
  assert.equal(MAX_PLAYERS, 12);
  assert.equal(WRITE_MS, 90_000);
  assert.equal(VOTE_MS, 20_000);
  assert.equal(MAX_ANSWER_LENGTH, 1000);
  assert.ok(isValidAnswer("x".repeat(1000)));
  assert.ok(!isValidAnswer("x".repeat(1001)));
  assert.ok(!isValidAnswer(" \n\t "));
  assert.ok(!isValidAnswer(undefined));
});

for (let count = MIN_PLAYERS; count <= MAX_PLAYERS; count++) {
  test(`${count} players receive exactly two unique prompts with fair author groups`, () => {
    const ids = players(count);
    for (let seed = 0; seed < 60; seed++) {
      for (const round of [1, 2]) {
        const questions = buildQuestions(ids, round, seed);
        assert.equal(questions.length, count % 2 ? count - 1 : count);
        assert.equal(new Set(questions.map(item => item.id)).size, questions.length);
        assert.equal(new Set(questions.map(item => item.promptId)).size, questions.length);
        assert.equal(questions.filter(item => item.authors.length === 3).length, count % 2 ? 2 : 0);
        const opponentPairs = new Set();
        for (const item of questions) {
          assert.ok(item.text.length > 10);
          assert.equal(new Set(item.authors).size, item.authors.length);
          assert.ok(item.authors.length === 2 || item.authors.length === 3);
          for (let index = 0; index < item.authors.length; index++) {
            for (let other = index + 1; other < item.authors.length; other++) {
              const pair = [item.authors[index], item.authors[other]].sort().join("|");
              assert.ok(!opponentPairs.has(pair), "No pair of players faces each other twice in a round");
              opponentPairs.add(pair);
            }
          }
        }
        for (const uid of ids) assert.equal(questions.filter(item => item.authors.includes(uid)).length, 2);
      }
    }
  });
}

test("assignments are seeded, stable across player order, and avoid previous prompts", () => {
  const ids = players(9);
  const first = buildQuestions(ids, 1, "match-123");
  assert.deepEqual(first, buildQuestions([...ids].reverse(), 1, "match-123"));
  assert.notDeepEqual(first, buildQuestions(ids, 1, "another-match"));
  const used = first.map(item => item.promptId);
  const second = buildQuestions(ids, 2, "match-123", used);
  assert.ok(second.every(item => !used.includes(item.promptId)));
  assert.ok(first.every(item => item.id.startsWith("r1-")));
  assert.ok(second.every(item => item.id.startsWith("r2-")));
});

test("the original prompt bank is varied, uniquely identified, and large enough for repeated matches", () => {
  assert.ok(NORMAL_PROMPTS.length >= 80);
  assert.ok(FINAL_PROMPTS.length >= 20);
  const all = [...NORMAL_PROMPTS, ...FINAL_PROMPTS];
  assert.equal(new Set(all.map(item => item.id)).size, all.length);
  assert.equal(new Set(all.map(item => item.text)).size, all.length);
  assert.ok(all.every(item => Object.isFrozen(item)));
});

test("invalid lobby sizes, duplicate accounts, rounds, and exhausted banks are rejected", () => {
  for (const count of [0, 3, 13]) assert.throws(() => buildQuestions(players(count), 1, "seed"), RangeError);
  assert.throws(() => buildQuestions(["a", "b", "c", "c"], 1, "seed"), RangeError);
  assert.throws(() => buildQuestions(["a", "b", "c", null], 1, "seed"), TypeError);
  for (const round of [0, 4, "1"]) assert.throws(() => buildQuestions(four, round, "seed"), RangeError);
  assert.throws(() => buildQuestions(four, 1, "seed", NORMAL_PROMPTS.map(item => item.id)), RangeError);
});

test("the finale assigns one common prompt to every player at every allowed size", () => {
  for (let count = MIN_PLAYERS; count <= MAX_PLAYERS; count++) {
    const ids = players(count);
    const final = buildQuestions(ids, 3, "final");
    assert.equal(final.length, 1);
    assert.deepEqual([...final[0].authors].sort(), ids);
    assert.ok(final[0].promptId.startsWith("final-"));
    const submissions = Object.fromEntries(ids.map(uid => [uid, { [final[0].id]: `An answer from ${uid}` }]));
    assert.ok(canFinishWriting(final, submissions));
  }
});

test("writing completes only when every assigned response is present and valid", () => {
  const ids = players(7);
  const questions = buildQuestions(ids, 1, "ready");
  const submissions = Object.fromEntries(ids.map(uid => [uid, {}]));
  for (const item of questions) for (const uid of item.authors) submissions[uid][item.id] = `${uid} responds`;
  assert.ok(canFinishWriting(questions, submissions));
  const item = questions[0];
  const uid = item.authors[0];
  for (const invalid of [undefined, "", "  ", "x".repeat(1001)]) {
    assert.ok(!canFinishWriting(questions, { ...submissions, [uid]: { ...submissions[uid], [item.id]: invalid } }));
  }
  assert.ok(!canFinishWriting(questions, {}));
  assert.ok(!canFinishWriting([], submissions));
});

test("normal rounds exclude all authors, including all three authors on odd-player questions", () => {
  assert.deepEqual(eligibleVoters(question, four, 1, options), ["c", "d"]);
  const triple = { ...question, authors: ["a", "b", "c"] };
  assert.deepEqual(eligibleVoters(triple, [...four, "e"], 2, options), ["d", "e"]);
  const score = scoreQuestion(triple, [...options, { id: "answer-c", uid: "c", text: "Third" }],
    { a: "answer-b", b: "answer-c", c: "answer-a", d: "answer-a", e: "answer-c" }, [...four, "e"], 2);
  assert.equal(score.totalVotes, 2);
  assert.equal(score.awards.a, 1000);
  assert.equal(score.awards.b, 0);
  assert.equal(score.awards.c, 1000);
});

test("ballots from authors, unknown accounts, self votes, and invalid targets are ignored", () => {
  const score = scoreQuestion(question, options, {
    a: "answer-b", b: "answer-a", c: "answer-a", d: "missing", outsider: "answer-b",
  }, four, 1);
  assert.equal(score.totalVotes, 1);
  assert.deepEqual(score.awards, { a: 1000, b: 0, c: 0, d: 0 });
  assert.equal(score.answers[0].percent, 100);
  assert.ok(!canFinishVoting(question, options, { c: "answer-a", d: "missing" }, four, 1));
  assert.ok(canFinishVoting(question, options, { c: "answer-a", d: "answer-b" }, four, 1));
});

test("points double in round two and triple in the finale with no extra bonuses", () => {
  for (const round of [1, 2, 3]) {
    const current = round === 3 ? { ...question, authors: four } : question;
    const score = scoreQuestion(current, options, { c: "answer-a", d: "answer-b" }, four, round);
    assert.equal(score.pool, round * 1000);
    assert.equal(score.awards.a, round * 500);
    assert.equal(score.awards.b, round * 500);
    assert.equal(Object.values(score.awards).reduce((sum, value) => sum + value, 0), score.pool);
  }
});

test("largest remainders allocate every point and resolve ties by answer ID", () => {
  const ids = ["a", "b", "c", "d", "e", "f"];
  const triple = { ...question, authors: ["a", "b", "c"] };
  const answers = [...options, { id: "answer-c", uid: "c", text: "Third" }];
  const votes = { d: "answer-a", e: "answer-b", f: "answer-c" };
  const score = scoreQuestion(triple, answers, votes, ids, 1);
  assert.deepEqual(score.answers.map(answer => answer.points), [334, 333, 333]);
  assert.deepEqual(score, scoreQuestion(triple, [...answers].reverse(), new Map(Object.entries(votes).reverse()), [...ids].reverse(), 1));
  assert.equal(score.answers[0].percent, 100 / 3);
  const uneven = scoreQuestion(question, options, { c: "answer-a", d: "answer-a", e: "answer-b" }, ids, 1);
  assert.deepEqual(uneven.answers.map(answer => answer.points), [667, 333]);
});

test("missing and blank answers never become options or receive timeout points", () => {
  const invalid = [
    ...options.slice(0, 1),
    { id: "blank", uid: "b", text: "  " },
    { id: "outsider", uid: "c", text: "Not assigned" },
    { id: "long", uid: "b", text: "x".repeat(1001) },
  ];
  const score = scoreQuestion(question, invalid, { c: "blank", d: "answer-a" }, four, 1);
  assert.equal(score.answers.length, 1);
  assert.equal(score.totalVotes, 1);
  assert.equal(score.awards.b, 0);
  assert.equal(score.awards.a, 1000);
  const empty = scoreQuestion(question, [], { c: "missing" }, four, 1);
  assert.equal(empty.totalVotes, 0);
  assert.deepEqual(empty.awards, { a: 0, b: 0, c: 0, d: 0 });
});

test("zero valid votes awards zero even with submitted answers", () => {
  for (const round of [1, 2, 3]) {
    const current = round === 3 ? { ...question, authors: four } : question;
    const score = scoreQuestion(current, options, {}, four, round);
    assert.equal(score.totalVotes, 0);
    assert.ok(score.answers.every(answer => answer.points === 0 && answer.percent === 0));
    assert.ok(Object.values(score.awards).every(points => points === 0));
  }
});

test("finale participants can vote for other answers, and nonresponders can still vote", () => {
  const final = { ...question, authors: four };
  assert.deepEqual(eligibleVoters(final, four, 3, options), four);
  const score = scoreQuestion(final, options, { a: "answer-b", b: "answer-a", c: "answer-a", d: "answer-b" }, four, 3);
  assert.equal(score.totalVotes, 4);
  assert.deepEqual(score.awards, { a: 1500, b: 1500, c: 0, d: 0 });
  assert.ok(canFinishVoting(final, options, { a: "answer-b", b: "answer-a", c: "answer-a", d: "answer-b" }, four, 3));
  assert.ok(!canFinishVoting(final, options, { a: "answer-a", b: "answer-b", c: "answer-a", d: "answer-b" }, four, 3));
  assert.equal(scoreQuestion(final, options, { a: "answer-a", b: "answer-b" }, four, 3).totalVotes, 0);
});

test("a lone finale responder sits out; others can vote, and no available options finishes immediately", () => {
  const final = { ...question, authors: four };
  const only = options.slice(0, 1);
  assert.deepEqual(eligibleVoters(final, four, 3, only), ["b", "c", "d"]);
  assert.ok(canFinishVoting(final, only, { b: "answer-a", c: "answer-a", d: "answer-a" }, four, 3));
  assert.equal(scoreQuestion(final, only, { a: "answer-a", b: "answer-a" }, four, 3).awards.a, 3000);
  assert.deepEqual(eligibleVoters(final, four, 3, []), []);
  assert.ok(canFinishVoting(final, [], {}, four, 3));
});

test("duplicate option IDs or multiple answers from one author are rejected deterministically", () => {
  const duplicateId = [options[0], { ...options[1], id: options[0].id }];
  assert.deepEqual(scoreQuestion(question, duplicateId, { c: "answer-a" }, four, 1).answers, []);
  const duplicateAuthor = [...options, { id: "other-a", uid: "a", text: "Another" }];
  const score = scoreQuestion(question, duplicateAuthor, { c: "answer-a", d: "answer-b" }, four, 1);
  assert.equal(score.answers.length, 1);
  assert.equal(score.answers[0].uid, "b");
  assert.equal(score.awards.a, 0);
});

test("scoring is pure and repeatable so one immutable receipt can safely be reused", () => {
  const frozenQuestion = Object.freeze({ ...question, authors: Object.freeze([...question.authors]) });
  const frozenOptions = Object.freeze(options.map(answer => Object.freeze({ ...answer })));
  const votes = Object.freeze({ c: "answer-a", d: "answer-b" });
  const ids = Object.freeze([...four]);
  const receipt = scoreQuestion(frozenQuestion, frozenOptions, votes, ids, 1);
  assert.deepEqual(receipt, scoreQuestion(frozenQuestion, frozenOptions, votes, ids, 1));
  assert.deepEqual(frozenOptions, options);
  assert.deepEqual(votes, { c: "answer-a", d: "answer-b" });
  receipt.answers[0].points = -1;
  assert.equal(scoreQuestion(frozenQuestion, frozenOptions, votes, ids, 1).answers[0].points, 500);
});

test("full matches conserve points across all sizes and both paired rounds", () => {
  for (let count = MIN_PLAYERS; count <= MAX_PLAYERS; count++) {
    const ids = players(count);
    let used = [];
    const totals = Object.fromEntries(ids.map(uid => [uid, 0]));
    for (const round of [1, 2, 3]) {
      const questions = buildQuestions(ids, round, `match-${count}`, used);
      used = [...used, ...questions.map(item => item.promptId)];
      for (const item of questions) {
        const answers = item.authors.map(uid => ({ id: `${item.id}-${uid}`, uid, text: `${uid}'s answer` }));
        const voters = eligibleVoters(item, ids, round, answers);
        const ballots = Object.fromEntries(voters.map((uid, index) => {
          const choices = answers.filter(answer => answer.uid !== uid);
          return [uid, choices[index % choices.length].id];
        }));
        assert.ok(canFinishVoting(item, answers, ballots, ids, round));
        const result = scoreQuestion(item, answers, ballots, ids, round);
        assert.equal(result.totalVotes, voters.length);
        assert.equal(Object.values(result.awards).reduce((sum, value) => sum + value, 0), round * 1000);
        for (const uid of ids) totals[uid] += result.awards[uid];
      }
    }
    const normalQuestionCount = count % 2 ? count - 1 : count;
    assert.equal(Object.values(totals).reduce((sum, value) => sum + value, 0), normalQuestionCount * 3000 + 3000);
  }
});
