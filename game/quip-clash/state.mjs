import { buildQuestions, eligibleVoters, scoreQuestion, canFinishWriting, canFinishVoting, MIN_PLAYERS, MAX_PLAYERS, MAX_ANSWER_LENGTH, WRITE_MS, VOTE_MS } from './core.mjs';

export const REVEAL_MS = 6000;
export const SCORES_MS = 12000;
export const VERSION = 1;
export const phaseKey = room => `${room.matchId}:${room.round}:${room.phase}:${room.questionIndex}`;
export const deadlineOf = room => {
  const started = room.phaseStartedAt?.toMillis?.() ?? room.phaseStartedAt;
  return typeof started === 'number' ? started + room.phaseDurationMs : null;
};
export const playerIds = room => room.roster.map(p => p.uid);
export const assignmentsFor = (room, uid) => room.questions.filter(q => q.authors.includes(uid));
export function answersFor(room, question = room.questions[room.questionIndex]) {
  if (!question) return [];
  return question.authors.map(uid => ({ id: `${question.id}_${uid}`, uid, text: room.responses[uid]?.[question.id]?.trim() || '' })).filter(a => a.text);
}
export function votersFor(room) {
  const question = room.questions[room.questionIndex];
  return question && answersFor(room).length ? eligibleVoters(question, playerIds(room), room.round, answersFor(room)) : [];
}
export function createRoom(identity, { name, visibility = 'public', seed, now }) {
  return {
    gameType: 'quipclash', quipVersion: VERSION, name: String(name || `${identity.displayName}'s Quip Clash`).trim().slice(0, 60),
    status: 'waiting', visibility: visibility === 'private' ? 'private' : 'public',
    host: identity.username, hostUid: identity.uid, players: [identity.username], members: { [identity.uid]: identity.username },
    roster: [{ ...identity, score: 0 }], createdAt: now, updatedAt: now, matchId: seed,
    round: 0, phase: 'waiting', questionIndex: 0, questions: [], responses: {}, ready: {}, votes: {}, receipts: {}, usedPromptIds: [],
    phaseStartedAt: null, phaseDurationMs: 0, clockProbes: {},
  };
}
export function validateRoom(room) {
  if (!room || room.gameType !== 'quipclash' || room.quipVersion !== VERSION || !Array.isArray(room.roster)) throw new Error('This is not a Quip Clash room.');
}
function member(room, identity) {
  validateRoom(room);
  if (room.members[identity.uid] !== identity.username) throw new Error('Join this room before playing.');
}
function openPhase(room, phase, now) {
  if (room.phase !== phase) throw new Error('The game has moved on. Your screen is updating.');
  if (deadlineOf(room) === null || now >= deadlineOf(room)) throw new Error('Time is up for this phase.');
}
export function joinRoom(room, identity) {
  validateRoom(room);
  if (room.members[identity.uid] === identity.username) return room;
  // The site's accounts can share a device's anonymous Firebase identity.
  if (Object.hasOwn(room.members, identity.uid)) throw new Error('This device is already in the room with another account.');
  if (room.status !== 'waiting') throw new Error('This game has already started. Join the next game.');
  if (room.roster.length >= MAX_PLAYERS) throw new Error('This room is full (12 players).');
  if (room.roster.some(p => p.username.toLowerCase() === identity.username.toLowerCase())) throw new Error('Your account is already in this room on another device.');
  return { ...room, players: [...room.players, identity.username], members: { ...room.members, [identity.uid]: identity.username }, roster: [...room.roster, { ...identity, score: 0 }] };
}
export function leaveRoom(room, identity) {
  member(room, identity);
  if (room.status !== 'waiting') return room; // Keep started-game seats stable for reconnects and fair scoring.
  const roster = room.roster.filter(p => p.uid !== identity.uid), members = { ...room.members };
  delete members[identity.uid];
  return { ...room, roster, members, players: roster.map(p => p.username), hostUid: roster[0]?.uid || '', host: roster[0]?.username || '', status: roster.length ? 'waiting' : 'closed', phase: roster.length ? 'waiting' : 'closed' };
}
function beginRound(room, round, timestamp) {
  const questions = buildQuestions(playerIds(room), round, `${room.matchId}:${round}`, room.usedPromptIds);
  return { ...room, status: 'started', round, phase: 'writing', questions, questionIndex: 0, responses: {}, ready: {}, votes: {},
    usedPromptIds: [...room.usedPromptIds, ...questions.map(q => q.promptId)], phaseStartedAt: timestamp, phaseDurationMs: WRITE_MS };
}
export function startRoom(room, identity, timestamp) {
  member(room, identity);
  if (room.hostUid !== identity.uid) throw new Error('Only the host can start the game.');
  if (room.status !== 'waiting') throw new Error('This game has already started.');
  if (room.roster.length < MIN_PLAYERS) throw new Error('Gather at least 4 players to start.');
  return beginRound(room, 1, timestamp);
}
export function saveResponses(room, identity, input, now, lock = false) {
  member(room, identity); openPhase(room, 'writing', now);
  if (room.ready[identity.uid]) return room;
  const answers = {};
  for (const q of assignmentsFor(room, identity.uid)) {
    const value = input[q.id] ?? room.responses[identity.uid]?.[q.id] ?? '';
    if (typeof value !== 'string' || value.length > MAX_ANSWER_LENGTH) throw new Error(`Keep each answer under ${MAX_ANSWER_LENGTH + 1} characters.`);
    answers[q.id] = value;
  }
  if (lock && Object.values(answers).some(text => !text.trim())) throw new Error('Give each prompt an answer before locking in.');
  return { ...room, responses: { ...room.responses, [identity.uid]: answers }, ready: lock ? { ...room.ready, [identity.uid]: true } : room.ready };
}
export function castVote(room, identity, answerId, expectedKey, now) {
  member(room, identity); openPhase(room, 'voting', now);
  if (phaseKey(room) !== expectedKey) throw new Error('That matchup has already ended.');
  if (!votersFor(room).includes(identity.uid)) throw new Error('You are sitting out this matchup.');
  const answer = answersFor(room).find(a => a.id === answerId);
  if (!answer || answer.uid === identity.uid) throw new Error('Choose another player’s answer.');
  if (room.votes[identity.uid]) return room; // A retried request must never change a locked vote.
  return { ...room, votes: { ...room.votes, [identity.uid]: answerId } };
}
export function advanceRoom(room, identity, expectedKey, now, timestamp, force = false) {
  member(room, identity);
  if (phaseKey(room) !== expectedKey) return room;
  const deadline = deadlineOf(room), expired = deadline !== null && now >= deadline;
  if (room.phase === 'writing') {
    const everyoneReady = room.roster.every(p => room.ready[p.uid]) && canFinishWriting(room.questions, room.responses);
    if (!expired && !everyoneReady) return room;
    return { ...room, phase: 'voting', questionIndex: 0, votes: {}, phaseStartedAt: timestamp, phaseDurationMs: VOTE_MS };
  }
  if (room.phase === 'voting') {
    const voters = votersFor(room), question = room.questions[room.questionIndex];
    if (!expired && voters.length && !canFinishVoting(question, answersFor(room), room.votes, playerIds(room), room.round)) return room;
    const receiptKey = `r${room.round}_${question.id}`;
    const result = scoreQuestion(question, answersFor(room), room.votes, playerIds(room), room.round);
    const receipts = room.receipts[receiptKey] ? room.receipts : { ...room.receipts, [receiptKey]: { ...result, question, round: room.round, ballots: { ...room.votes } } };
    // Recompute from the immutable ledger rather than incrementing scores.
    const roster = room.roster.map(p => ({ ...p, score: Object.values(receipts).reduce((total, receipt) => total + (receipt.awards[p.uid] || 0), 0) }));
    return { ...room, receipts, roster, phase: 'reveal', phaseStartedAt: timestamp, phaseDurationMs: REVEAL_MS };
  }
  if (room.phase === 'reveal') {
    if (!expired && !(force && room.hostUid === identity.uid)) return room;
    if (room.questionIndex + 1 < room.questions.length) return { ...room, phase: 'voting', questionIndex: room.questionIndex + 1, votes: {}, phaseStartedAt: timestamp, phaseDurationMs: VOTE_MS };
    return { ...room, phase: room.round === 3 ? 'finished' : 'scores', status: room.round === 3 ? 'finished' : 'started', phaseStartedAt: timestamp, phaseDurationMs: room.round === 3 ? 0 : SCORES_MS };
  }
  if (room.phase === 'scores' && (expired || force && room.hostUid === identity.uid)) return beginRound(room, room.round + 1, timestamp);
  return room;
}
export function replayRoom(room, identity, seed, now) {
  member(room, identity);
  if (room.hostUid !== identity.uid || room.phase !== 'finished') throw new Error('The host can start a rematch after the final results.');
  return { ...createRoom(identity, { name: room.name, visibility: room.visibility, seed, now }), roster: room.roster.map(p => ({ ...p, score: 0 })), players: [...room.players], members: { ...room.members } };
}
