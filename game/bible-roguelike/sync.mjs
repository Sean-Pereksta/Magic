import {normalizeName, VERSION} from './core.mjs';

export const GAME_TYPE = 'bibleroguelike';
export const PRESENCE_MS = 60000;
export function lobbyNames(lobby) {
  return (lobby?.players || []).map(p => typeof p === 'string' ? p : p?.username || p?.name || '').filter(Boolean);
}
function requireLobby(lobby, username) {
  if (lobby?.gameType !== GAME_TYPE || lobby.status !== 'started') throw new Error('Start a Roguelike game from the multiplayer lobby first.');
  if (!lobbyNames(lobby).some(n => normalizeName(n) === normalizeName(username))) throw new Error('You are no longer a member of this lobby.');
}

// Firebase is injected so the identical transaction code can be exercised with
// an optimistic transaction harness. No DOM, random rolls or effects in callbacks.
export function createCoopStore({api, db, gameId, username, uid, rules, newRun, now = () => Date.now()}) {
  const {doc,runTransaction,onSnapshot,setDoc,serverTimestamp} = api;
  const lobbyRef = doc(db,'lobbies',gameId), runRef = doc(db,'lobbies',gameId,'roguelike','run');
  let playerId = null, heartbeat = null, unsub = null, closed = false;
  const presenceRef = id => doc(db,'lobbies',gameId,'roguelikePresence',id);
  async function connect(onChange,onConnection) {
    // Generate the candidate outside retries. Its first committed roster is fixed.
    const candidate = newRun();
    const joined = await runTransaction(db,async tx => {
      const lobby = await tx.get(lobbyRef), run = await tx.get(runRef);
      if (!lobby.exists()) throw new Error('This lobby no longer exists.');
      requireLobby(lobby.data(),username);
      const state = run.exists() ? rules.upgradeRun(run.data()) : rules.create({...candidate,names:lobbyNames(lobby.data())});
      if (state.version !== VERSION) throw new Error('This run uses a different version. Return to the lobby.');
      const player = state.players.find(p => normalizeName(p.name) === normalizeName(username));
      if (!player) throw new Error('This run already has its roster. Rejoin with your original name or start a new lobby.');
      if (player.uid && player.uid !== uid) throw new Error('This player belongs to another signed-in session. Reopen the original session.');
      player.uid = uid;
      tx.set(runRef,state);
      tx.set(presenceRef(player.id),{uid,online:true,lastSeenAt:serverTimestamp()});
      return {state,playerId:player.id};
    });
    playerId = joined.playerId;
    if (closed) return joined;
    onChange(joined.state);
    unsub = onSnapshot(runRef,{includeMetadataChanges:true},snapshot => {
      if (!snapshot.exists()) { onConnection(false,'Run missing. Return to the lobby.'); return; }
      if (!snapshot.metadata.fromCache && !snapshot.metadata.hasPendingWrites) onChange(snapshot.data());
      onConnection(!snapshot.metadata.fromCache && !snapshot.metadata.hasPendingWrites,snapshot.metadata.fromCache ? 'Reconnecting…' : 'Party synchronized');
    },error => onConnection(false,error.message || 'Connection lost. Reopen the run to reconnect.'));
    const pulse = () => {
      if (closed) return;
      setDoc(presenceRef(playerId),{uid,online:true,lastSeenAt:serverTimestamp()}).catch(error => onConnection(false,error.message));
    };
    heartbeat = setInterval(pulse,20000);
    onConnection(true,'Party synchronized');
    return joined;
  }
  async function dispatch(action) {
    if (closed || !playerId) throw new Error('Reconnect before playing.');
    return runTransaction(db,async tx => {
      const lobby = await tx.get(lobbyRef), run = await tx.get(runRef);
      if (!lobby.exists() || !run.exists()) throw new Error('The lobby or run has closed.');
      requireLobby(lobby.data(),username);
      const state = run.data(), player = state.players.find(p => p.id === playerId);
      if (!player || player.uid !== uid || action.playerId !== playerId) throw new Error('This player session has changed. Reconnect.');
      let activeIds = state.players.map(p => p.id);
      if (action.type === 'continue') {
        // Idle/disconnected peers never require a host to release the next room.
        const presence = await Promise.all(state.players.map(p => tx.get(presenceRef(p.id))));
        const moment = now();
        activeIds = state.players.filter((p,i) => {
          if (p.id === playerId) return true;
          const data = presence[i].data(), seen = data?.lastSeenAt?.toMillis?.() || 0;
          return data?.online && moment - seen < PRESENCE_MS && lobbyNames(lobby.data()).some(n => normalizeName(n) === normalizeName(p.name));
        }).map(p => p.id);
      }
      const next = rules.reduce(state,action,{activeIds});
      if (next !== state) tx.set(runRef,next);
      return next;
    });
  }
  async function restart(expectedRunId) {
    const candidate = newRun();
    return runTransaction(db,async tx => {
      const lobby = await tx.get(lobbyRef), run = await tx.get(runRef);
      if (!lobby.exists() || !run.exists()) throw new Error('The lobby has closed.');
      requireLobby(lobby.data(),username);
      const old = run.data();
      if (old.id !== expectedRunId) return old; // Another teammate already restarted.
      if (old.phase !== 'ended') throw new Error('Finish the current run before starting another.');
      if (!old.players.some(p => p.id === playerId && p.uid === uid)) throw new Error('Reconnect before restarting.');
      const next = rules.create({...candidate,names:old.players.map(p => p.name)});
      next.players.forEach((p,i) => { p.uid = old.players[i].uid; });
      tx.set(runRef,next); return next;
    });
  }
  function close() {
    closed = true; clearInterval(heartbeat); unsub?.();
    if (playerId) void setDoc(presenceRef(playerId),{uid,online:false,lastSeenAt:serverTimestamp()}).catch(() => {});
  }
  return {connect,dispatch,restart,close,get playerId() { return playerId; }};
}
