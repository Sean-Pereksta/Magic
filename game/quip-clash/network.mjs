import * as state from './state.mjs';

export function createClock() {
  let anchor = Date.now(), tick = performance.now(), synced = false;
  return { now: () => anchor + performance.now() - tick, get synced() { return synced; },
    calibrate(serverMs, sentTick, receivedTick) { anchor = serverMs + (receivedTick - sentTick) / 2; tick = receivedTick; synced = true; } };
}
export function createClient({ db, sdk, identity, clock = createClock() }) {
  const { doc, runTransaction, serverTimestamp, updateDoc, FieldPath, getDocFromServer, onSnapshot } = sdk;
  const ref = code => doc(db, 'lobbies', code);
  const seed = () => globalThis.crypto.randomUUID();
  function codeOf(input) {
    let code = String(input || '').trim();
    try { if (code.includes('://')) code = new URL(code).searchParams.get('lobbyId') || ''; } catch { /* Validation below reports malformed links. */ }
    if (!/^[A-Za-z0-9_-]{4,80}$/.test(code)) throw new Error('Enter a room code or an invitation link.');
    return code.length === 6 ? code.toUpperCase() : code;
  }
  async function transact(code, reduce) {
    return runTransaction(db, async tx => {
      const snapshot = await tx.get(ref(code));
      if (!snapshot.exists()) throw new Error('Room not found. Check the code and try again.');
      const before = snapshot.data(); state.validateRoom(before);
      const after = reduce(before, serverTimestamp());
      if (after !== before) tx.set(ref(code), { ...after, updatedAt: clock.now() });
      return code;
    }, { maxAttempts: 20 });
  }
  async function syncClock(code) {
    const sent = performance.now();
    await updateDoc(ref(code), new FieldPath('clockProbes', identity.uid), serverTimestamp());
    const snapshot = await getDocFromServer(ref(code));
    const received = performance.now(), probe = snapshot.data()?.clockProbes?.[identity.uid];
    if (!probe?.toMillis) throw new Error('Could not sync the room clock. Try reconnecting.');
    clock.calibrate(probe.toMillis(), sent, received);
  }
  return {
    clock, codeOf, syncClock,
    async create(options = {}) {
      for (let attempt = 0; attempt < 5; attempt++) {
        const bytes = new Uint8Array(6); crypto.getRandomValues(bytes);
        const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789', code = Array.from(bytes, b => alphabet[b % alphabet.length]).join('');
        const matchSeed = seed();
        const created = await runTransaction(db, async tx => {
          if ((await tx.get(ref(code))).exists()) return false;
          tx.set(ref(code), state.createRoom(identity, { ...options, seed: matchSeed, now: clock.now() })); return true;
        }, { maxAttempts: 20 });
        if (created) {
          try { await syncClock(code); } catch (error) { error.roomCode = code; throw error; }
          return code;
        }
      }
      throw new Error('Could not reserve a room code. Please try again.');
    },
    async join(input) {
      const code = codeOf(input); await transact(code, room => state.joinRoom(room, identity));
      try { await syncClock(code); } catch (error) { error.roomCode = code; throw error; }
      return code;
    },
    watch(code, onRoom, onError) { return onSnapshot(ref(code), { includeMetadataChanges: true }, snap => {
      if (!snap.exists()) { onError(new Error('This room is no longer available.')); return; }
      onRoom({ ...snap.data(), id: snap.id }, !snap.metadata.fromCache);
    }, onError); },
    start: code => transact(code, (room, timestamp) => state.startRoom(room, identity, timestamp)),
    save: (code, answers, lock = false, key) => transact(code, room => {
      if (key && state.phaseKey(room) !== key) return room;
      return state.saveResponses(room, identity, answers, clock.now(), lock);
    }),
    vote: (code, answer, key) => transact(code, room => state.castVote(room, identity, answer, key, clock.now())),
    advance: (code, key, force = false) => transact(code, (room, timestamp) => state.advanceRoom(room, identity, key, clock.now(), timestamp, force)),
    leave: code => transact(code, room => state.leaveRoom(room, identity)),
    replay: code => { const matchSeed = seed(); return transact(code, room => state.replayRoom(room, identity, matchSeed, clock.now())); },
  };
}
