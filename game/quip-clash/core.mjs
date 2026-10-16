import { NORMAL_PROMPTS, FINAL_PROMPTS } from "./prompts.mjs";

export const MIN_PLAYERS = 4;
export const MAX_PLAYERS = 12;
export const MAX_ANSWER_LENGTH = 1000;
export const WRITE_MS = 90_000;
export const VOTE_MS = 20_000;

const compare = (a, b) => a < b ? -1 : a > b ? 1 : 0;
const own = (object, key) => Object.prototype.hasOwnProperty.call(object ?? {}, key);

function assertRound(round) {
  if (![1, 2, 3].includes(round)) throw new RangeError("Round must be 1, 2, or 3.");
}

function playerIds(players) {
  if (!Array.isArray(players) || players.some(uid => typeof uid !== "string" || !uid)) {
    throw new TypeError("Players must be an array of nonempty account IDs.");
  }
  return [...new Set(players)].sort(compare);
}

function randomFrom(seed) {
  let hash = 2166136261;
  for (const character of String(seed)) {
    hash ^= character.codePointAt(0);
    hash = Math.imul(hash, 16777619);
  }
  let state = hash >>> 0;
  return () => {
    state = (state + 0x6D2B79F5) >>> 0;
    let value = state;
    value = Math.imul(value ^ value >>> 15, value | 1);
    value ^= value + Math.imul(value ^ value >>> 7, value | 61);
    return ((value ^ value >>> 14) >>> 0) / 4294967296;
  };
}

function shuffled(values, random) {
  const result = [...values];
  for (let index = result.length - 1; index > 0; index--) {
    const other = Math.floor(random() * (index + 1));
    [result[index], result[other]] = [result[other], result[index]];
  }
  return result;
}

/** Assign two distinct prompts per player, or one shared prompt in the finale. */
export function buildQuestions(players, round, seed, usedPromptIds = []) {
  assertRound(round);
  const ids = playerIds(players);
  if (ids.length !== players.length) throw new RangeError("A player may only join once.");
  if (ids.length < MIN_PLAYERS || ids.length > MAX_PLAYERS) {
    throw new RangeError(`A match needs ${MIN_PLAYERS}–${MAX_PLAYERS} players.`);
  }
  const random = randomFrom(`${seed}|round:${round}`);
  const order = shuffled(ids, random);
  let groups;
  if (round === 3) {
    groups = [order];
  } else if (order.length % 2 === 0) {
    // A cycle gives every player two prompts without repeating an opponent.
    groups = order.map((uid, index) => [uid, order[(index + 1) % order.length]]);
  } else {
    // Two triples consume six slots; the remaining paths give everyone degree two.
    groups = [[order[0], order[1], order[2]], [order[0], order[3], order[4]]];
    const path = [order[1], ...order.slice(5), order[3]];
    for (let index = 0; index < path.length - 1; index++) groups.push([path[index], path[index + 1]]);
    groups.push([order[2], order[4]]);
  }
  const used = new Set(usedPromptIds);
  const bank = round === 3 ? FINAL_PROMPTS : NORMAL_PROMPTS;
  const prompts = shuffled(bank.filter(prompt => !used.has(prompt.id)), random);
  if (prompts.length < groups.length) throw new RangeError("There are not enough unused prompts for this round.");
  return shuffled(groups, random).map((authors, index) => ({
    id: `r${round}-q${index + 1}`,
    text: prompts[index].text,
    authors: [...authors],
    promptId: prompts[index].id,
  }));
}

export function isValidAnswer(text) {
  return typeof text === "string" && text.length <= MAX_ANSWER_LENGTH && text.trim().length > 0;
}

function validAnswers(question, answers, players) {
  const ids = new Set(players);
  const authors = new Set(question?.authors ?? []);
  const candidates = (Array.isArray(answers) ? answers : []).filter(answer =>
    answer && typeof answer.id === "string" && answer.id &&
    typeof answer.uid === "string" && ids.has(answer.uid) && authors.has(answer.uid) &&
    isValidAnswer(answer.text));
  const idCounts = new Map();
  const uidCounts = new Map();
  for (const answer of candidates) {
    idCounts.set(answer.id, (idCounts.get(answer.id) ?? 0) + 1);
    uidCounts.set(answer.uid, (uidCounts.get(answer.uid) ?? 0) + 1);
  }
  // Reject ambiguous options rather than letting array order select an answer.
  return candidates.filter(answer => idCounts.get(answer.id) === 1 && uidCounts.get(answer.uid) === 1)
    .map(answer => ({ id: answer.id, uid: answer.uid, text: answer.text.trim() }))
    .sort((a, b) => compare(a.id, b.id) || compare(a.uid, b.uid));
}

/** Normal-round authors sit out; finale players may vote only for another author. */
export function eligibleVoters(question, players, round, answers = []) {
  assertRound(round);
  const ids = playerIds(players);
  const authors = new Set(question?.authors ?? []);
  if (round !== 3) return ids.filter(uid => !authors.has(uid));
  const options = validAnswers(question, answers, ids);
  return ids.filter(uid => options.some(answer => answer.uid !== uid));
}

function ballotEntries(votes) {
  return votes instanceof Map ? [...votes.entries()] : Object.entries(votes ?? {});
}

function acceptedBallots(question, answers, votes, players, round) {
  const eligible = new Set(eligibleVoters(question, players, round, answers));
  const options = new Map(answers.map(answer => [answer.id, answer]));
  const accepted = new Map();
  for (const [uid, answerId] of ballotEntries(votes)) {
    const target = options.get(answerId);
    if (eligible.has(uid) && target && target.uid !== uid && !accepted.has(uid)) accepted.set(uid, answerId);
  }
  return accepted;
}

/** Divide a fixed pool by valid vote share using deterministic whole-point remainders. */
export function scoreQuestion(question, answers, votes, players, round) {
  assertRound(round);
  const ids = playerIds(players);
  const options = validAnswers(question, answers, ids);
  const ballots = acceptedBallots(question, options, votes, ids, round);
  const pool = round * 1000;
  const totalVotes = ballots.size;
  const counts = new Map(options.map(answer => [answer.id, 0]));
  for (const answerId of ballots.values()) counts.set(answerId, counts.get(answerId) + 1);
  const results = options.map(answer => {
    const count = counts.get(answer.id);
    return {
      ...answer,
      votes: count,
      points: totalVotes ? Math.floor(count * pool / totalVotes) : 0,
      percent: totalVotes ? count * 100 / totalVotes : 0,
    };
  });
  if (totalVotes) {
    let remaining = pool - results.reduce((total, answer) => total + answer.points, 0);
    const remainderOrder = [...results].sort((a, b) =>
      ((b.votes * pool) % totalVotes) - ((a.votes * pool) % totalVotes) ||
      compare(a.id, b.id) || compare(a.uid, b.uid));
    for (const answer of remainderOrder) {
      if (remaining-- <= 0) break;
      answer.points++;
    }
  }
  const awards = Object.fromEntries(ids.map(uid => [uid, 0]));
  for (const answer of results) awards[answer.uid] += answer.points;
  return { answers: results, awards, totalVotes, pool };
}

/** Submission maps are keyed first by account ID, then by question ID. */
export function canFinishWriting(questions, submissions) {
  return Array.isArray(questions) && questions.length > 0 && questions.every(question =>
    Array.isArray(question.authors) && question.authors.length > 0 && question.authors.every(uid =>
      own(submissions, uid) && own(submissions[uid], question.id) && isValidAnswer(submissions[uid][question.id])));
}

/** An invalid or self ballot never satisfies the early-completion condition. */
export function canFinishVoting(question, answers, votes, players, round) {
  assertRound(round);
  const ids = playerIds(players);
  const options = validAnswers(question, answers, ids);
  const eligible = eligibleVoters(question, ids, round, options);
  const accepted = acceptedBallots(question, options, votes, ids, round);
  return eligible.every(uid => accepted.has(uid));
}
