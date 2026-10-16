import { render, updateTimer } from './ui.mjs';
import { createClient } from './network.mjs';
import { answersFor, assignmentsFor, deadlineOf, phaseKey, votersFor } from './state.mjs';

const params = new URLSearchParams(location.search);
let identity, client, room, stopWatch, connected = false, error = '', message = '', busy = false;
let drafts = {}, draftKey = '', saveTimer, advancing = false, lastAttempt = 0, lastClockSync = 0, clockSyncing = false;
let saveQueue = Promise.resolve(), draftDirty = false, lastSaveAttempt = 0;
const session = {
  get(key) { try { return sessionStorage.getItem(key); } catch { return null; } },
  set(key, value) { try { sessionStorage.setItem(key, value); } catch { /* Browser storage is optional. */ } },
  remove(key) { try { sessionStorage.removeItem(key); } catch { /* Browser storage is optional. */ } },
};
function secondsLeft() { const deadline = room && deadlineOf(room); return deadline ? Math.max(0, Math.ceil((deadline - client.clock.now()) / 1000)) : 0; }
function roundDelta(uid) { return Object.values(room.receipts).filter(r => r.round === room.round).reduce((sum, r) => sum + (r.awards[uid] || 0), 0); }
function view(screen) {
  if (!room) return { screen: screen || (identity ? 'home' : 'loading'), identity, connected, error, message, busy, createName: params.get('name') || '' };
  const question = room.questions[room.questionIndex], result = question && room.receipts[`r${room.round}_${question.id}`];
  const leaderboard = room.roster.map(p => ({ ...p, delta: roundDelta(p.uid) })).sort((a, b) => b.score - a.score || a.displayName.localeCompare(b.displayName));
  const voting = room.phase === 'voting', eligible = votersFor(room), voted = Boolean(room.votes[identity.uid]);
  return {
    screen: screen || room.phase, identity, room: { ...room, players: room.roster, deadline: deadlineOf(room) },
    isHost: room.hostUid === identity.uid, connected, error, message, busy, secondsLeft: secondsLeft(),
    assignments: assignmentsFor(room, identity.uid).map(q => ({ ...q, answer: room.responses[identity.uid]?.[q.id] || '' })), draft: drafts,
    answers: (room.phase === 'reveal' && result ? result.answers : answersFor(room)).map(a => ({ ...a,
      displayName: room.phase === 'reveal' ? room.roster.find(p => p.uid === a.uid)?.displayName || 'Player' : undefined })),
    voted, eligible: eligible.includes(identity.uid), submitted: Boolean(room.ready[identity.uid]), leaderboard,
    progress: voting ? { done: eligible.filter(uid => room.votes[uid]).length, total: eligible.length } : { done: Object.keys(room.ready).length, total: room.roster.length },
  };
}
function draw(screen) { render(view(screen), actions); }
function explain(err) {
  const code = err?.code || '';
  if (code.includes('permission-denied')) return 'The database refused this action. Refresh and sign in through the lobby again.';
  if (code.includes('unavailable') || code.includes('deadline-exceeded') || code.includes('failed-precondition')) return 'Connection interrupted. Reconnect and try again; your saved answers are kept.';
  return err?.message || 'Something went wrong. Please try again.';
}
async function perform(work, success) {
  if (busy) return;
  busy = true; error = ''; draw();
  try { const value = await work(); if (success) await success(value); }
  catch (err) {
    // A clock-sync failure may follow a successful create/join. Keep that
    // committed room reachable instead of losing its newly reserved code.
    if (err.roomCode) await attach(err.roomCode);
    error = explain(err);
  }
  finally { busy = false; draw(); }
}
function loadDrafts() {
  const key = `${room.id}:${room.matchId}:${room.round}:${identity.uid}`;
  if (key === draftKey) return;
  draftKey = key; drafts = {};
  try { drafts = JSON.parse(session.get(`quip.draft:${key}`) || '{}'); } catch { drafts = {}; }
  if (!drafts || typeof drafts !== 'object' || Array.isArray(drafts)) drafts = {};
  drafts = { ...(room.responses[identity.uid] || {}), ...drafts };
  draftDirty = JSON.stringify(drafts) !== JSON.stringify(room.responses[identity.uid] || {});
}
function receive(next, synced) {
  if (next.status === 'closed') { error = 'This room has closed. Create a new one.'; detach(); draw('home'); return; }
  if (next.members?.[identity.uid] !== identity.username) { error = 'You are no longer in this room.'; detach(); draw('home'); return; }
  room = next; connected = synced && navigator.onLine && client.clock.synced; loadDrafts(); draw();
  if (connected) void maybeAdvance();
}
function detach() { stopWatch?.(); stopWatch = null; clearTimeout(saveTimer); room = null; draftKey = ''; drafts = {}; connected = navigator.onLine; }
async function attach(code) {
  stopWatch?.(); room = null; draftKey = ''; error = ''; message = ''; connected = true;
  session.set('quip.room', code);
  const url = new URL(location.href); url.search = ''; url.searchParams.set('lobbyId', code); history.replaceState(null, '', url);
  stopWatch = client.watch(code, receive, err => { connected = false; error = explain(err); draw(room ? undefined : 'error'); });
  lastClockSync = performance.now();
  const inviteId = params.get('inviteId');
  if (inviteId) {
    const { sdk, db } = await import('./firebase.mjs');
    // Invitations are consumed only once membership has committed successfully.
    try { await sdk.deleteDoc(sdk.doc(db, 'lobbyInvites', inviteId)); } catch { /* A stale invitation must not block playing. */ }
  }
}
async function persist(lock = false) {
  if (!room || room.phase !== 'writing' || room.ready[identity.uid]) return;
  const code = room.id, key = phaseKey(room), data = { ...drafts }, savedDraftKey = draftKey;
  lastSaveAttempt = performance.now();
  // Serial writes keep an older draft from overtaking a newer one on slow networks.
  const task = saveQueue.catch(() => {}).then(() => client.save(code, data, lock, key));
  saveQueue = task;
  await task;
  if (draftKey === savedDraftKey && JSON.stringify(drafts) === JSON.stringify(data)) draftDirty = false;
  if (room?.id === code && phaseKey(room) === key) { message = lock ? 'Answers locked in!' : 'Answers saved'; draw(); }
}
async function maybeAdvance(force = false) {
  if (!room || !connected || !client.clock.synced || advancing || !['writing', 'voting', 'reveal', 'scores'].includes(room.phase)) return;
  const now = performance.now();
  if (!force && now - lastAttempt < 1200) return;
  const expired = secondsLeft() === 0;
  const ready = room.phase === 'writing' && room.roster.every(p => room.ready[p.uid]);
  const voted = room.phase === 'voting' && votersFor(room).every(uid => room.votes[uid]);
  if (!expired && !ready && !voted && !force) return;
  advancing = true; lastAttempt = now;
  try { await client.advance(room.id, phaseKey(room), force); }
  catch (err) { error = explain(err); draw(); }
  finally { advancing = false; }
}
const actions = {
  create: name => perform(() => client.create({ name, visibility: params.get('visibility') || 'public' }), attach),
  join: code => perform(() => client.join(code), attach),
  start: () => perform(() => client.start(room.id)),
  leave: () => perform(async () => { if (room.phase === 'waiting') await client.leave(room.id); detach(); session.remove('quip.room'); history.replaceState(null, '', location.pathname); message = ''; }),
  submit: answers => perform(async () => { clearTimeout(saveTimer); drafts = { ...drafts, ...answers }; await persist(true); }),
  vote: answer => perform(() => client.vote(room.id, answer, phaseKey(room))),
  next: () => maybeAdvance(true),
  replay: () => perform(() => client.replay(room.id)),
  draft(questionId, text) {
    drafts[questionId] = text; draftDirty = true; session.set(`quip.draft:${draftKey}`, JSON.stringify(drafts)); message = 'Saving…';
    clearTimeout(saveTimer);
    const expected = draftKey;
    saveTimer = setTimeout(() => { if (expected === draftKey) void persist().catch(err => { error = explain(err); draw(); }); }, 250);
  },
  clearError() { error = ''; draw(); },
  async copyCode() {
    const link = new URL(location.pathname, location.origin); link.searchParams.set('lobbyId', room.id);
    try { await navigator.clipboard.writeText(link.href); message = 'Invite link copied!'; }
    catch { message = `Share room code: ${room.id}`; }
    draw();
  },
};
draw('loading');
async function initialize() {
  try {
    const { loadIdentity, sdk, db } = await import('./firebase.mjs');
    identity = await loadIdentity();
    if (!identity) { draw('account'); return; }
    client = createClient({ db, sdk, identity }); connected = navigator.onLine;
    const code = params.get('lobbyId') || (params.has('mode') ? '' : session.get('quip.room'));
    if (code) await actions.join(code);
    else draw('home');
  } catch (err) { error = explain(err); draw('error'); }
}
void initialize();
setInterval(() => {
  if (!room) return;
  updateTimer(secondsLeft());
  if (room.phase === 'writing' && draftDirty && connected && !room.ready[identity.uid] && performance.now() - lastSaveAttempt > 3000) void persist().catch(err => { error = explain(err); draw(); });
  void maybeAdvance();
  if (navigator.onLine && !clockSyncing && performance.now() - lastClockSync > (client.clock.synced && connected ? 60000 : 5000)) {
    clockSyncing = true; lastClockSync = performance.now();
    void client.syncClock(room.id).then(() => { connected = navigator.onLine; error = ''; draw(); }).catch(() => {}).finally(() => { clockSyncing = false; });
  }
}, 250);
window.addEventListener('offline', () => { connected = false; message = 'Reconnecting… Your typed answers are kept on this device.'; draw(); });
window.addEventListener('online', () => { if (!room) return; void client.syncClock(room.id).then(() => { connected = true; error = ''; message = ''; draw(); if (room.phase === 'writing') return persist(); }).catch(err => { error = explain(err); draw(); }); });
document.addEventListener('visibilitychange', () => {
  if (document.hidden && draftDirty && room?.phase === 'writing') void persist().catch(() => {});
  if (!document.hidden && room) void client.syncClock(room.id).then(() => { connected = navigator.onLine; return maybeAdvance(); }).catch(() => {});
});
