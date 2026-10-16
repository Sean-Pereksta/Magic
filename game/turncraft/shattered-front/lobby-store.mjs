export const LOBBY_FACTIONS = Object.freeze(['frontier', 'ascendant', 'brood']);
import {LOBBY_MAPS, validateBattleSettings, battleParticipantCount} from './battle-config.mjs';
export {LOBBY_MAPS};
const MODES = ['annihilation', 'conquest', 'relic'];
const clone = value => JSON.parse(JSON.stringify(value));
const safeKey = value => String(value).replace(/[^a-zA-Z0-9_@.-]/g, '_').slice(0, 100);
function fail(ok, message, code = 'INVALID_ARGUMENT') {
  if (!ok) throw Object.assign(new Error(message), {code});
}
function id(value) {
  fail(typeof value === 'string' && /^[A-Za-z0-9_-]{1,100}$/.test(value), 'Invalid lobby code.');
  return value;
}
function label(value, fallback = 'Commander') {
  const text = typeof value === 'string' ? value.trim().replace(/[\u0000-\u001f]/g, '').slice(0, 80) : '';
  fail(!text.includes('/'), 'A player name cannot contain a slash.');
  return text || fallback;
}
function settings(value = {}) { return validateBattleSettings(value); }
function inviteCode() {
  return [...globalThis.crypto.getRandomValues(new Uint8Array(6))].map(value => value.toString(16).padStart(2, '0')).join('').toUpperCase();
}
function member(who, factionId = 'frontier') {
  fail(LOBBY_FACTIONS.includes(factionId), 'Choose an existing faction.');
  return {name: who.name, factionId, team: 'ffa', ready: false, avatar: who.avatar || null};
}

// Uses the same authenticated Firestore connection as Game Hub. A leased player
// simulates each battle; durable match receipts update the selected hub profile.
export function createLobbyStore({db, auth, api, getIdentity, now = Date.now}) {
  const roomRef = value => api.doc(db, 'lobbies', id(value));
  const secretRef = value => api.doc(db, 'lobbies', id(value), 'turncraft_private', 'invite');
  const inviteRef = code => api.doc(db, 'lobbies', 'turncraft', 'invites', code);
  const savedRanks = new Map();
  async function who() {
    const user = auth.currentUser, identity = await getIdentity?.() || {};
    fail(user && (!identity.uid || identity.uid === user.uid), 'Sign in to your CatnMice browser account first.', 'UNAUTHENTICATED');
    return {uid: user.uid, username: identity.username, name: label(identity.username || identity.displayName), avatar: typeof identity.avatar === 'string' && identity.avatar.startsWith('https://') ? identity.avatar : null};
  }
  async function decorate(row) {
    const turncraftPlayers = {...row.turncraftPlayers};
    await Promise.all(Object.entries(turncraftPlayers).map(async ([uid, player]) => {
      if (!savedRanks.has(player.name)) savedRanks.set(player.name,
        api.getDoc(api.doc(db, 'users', player.name)).then(profile => profile.data()?.gameStats?.turncraft || {}).catch(() => ({})));
      const saved = await savedRanks.get(player.name);
      turncraftPlayers[uid] = {...player, mmr: Number.isFinite(saved.mmr) ? saved.mmr : 1000, rankedMatches: Number.isInteger(saved.rankedMatches) ? saved.rankedMatches : 0};
    }));
    return {...row, turncraftPlayers};
  }
  async function get(value) {
    const user = await who(), snap = await api.getDoc(roomRef(value));
    fail(snap.exists() && snap.data().gameType === 'turncraft' && snap.data().turncraftVersion === 1, 'This TurnCraft room has closed.', 'NOT_FOUND');
    const row = snap.data();
    fail(!row.members[user.uid] || row.members[user.uid] === user.name, 'Reopen the Game Hub profile that joined this room.', 'PROFILE_CHANGED');
    fail(row.visibility === 'public' || row.members[user.uid], 'Join this private room with its invite code.', 'INVITE_REQUIRED');
    const secret = row.members[user.uid] ? await api.getDoc(secretRef(value)) : null;
    return decorate({id: value, ...row, ...(secret?.exists() ? secret.data() : {})});
  }
  async function browse(ranked = false) {
    await who();
    const snapshot = await api.getDocs(api.query(api.collection(db, 'lobbies'),
      api.where('gameType', '==', 'turncraft'), api.where('turncraftVersion', '==', 1),
      api.where('status', '==', 'waiting'), api.where('visibility', '==', 'public'),
      api.where('ranked', '==', ranked), api.limit(50)));
    return {lobbies: snapshot.docs.map(snap => ({id: snap.id, ...snap.data()})).filter(row => row.turncraftVersion === 1 && row.ranked === ranked && row.visibility === 'public' && row.status === 'waiting' && row.players.length < row.settings.maxPlayers).slice(0, 50)};
  }
  async function create(input = {}) {
    const user = await who(), value = id(input.requestId || globalThis.crypto.randomUUID()), code = inviteCode(), stamp = now();
    const ranked = input.ranked === true;
    fail(!ranked || user.username, 'Choose your Game Hub profile before playing ranked.');
    if (ranked) fail((await api.getDoc(api.doc(db, 'users', user.username))).exists(), 'Open your existing Game Hub profile before playing ranked.');
    const visibility = ranked ? 'public' : input.visibility || 'public';
    fail(['public', 'private'].includes(visibility), 'Choose public or private.');
    const row = {name: label(input.name, 'Shattered Front'), gameType: 'turncraft', turncraftVersion: 1, status: 'waiting', visibility, ranked, host: user.name, hostUid: user.uid, members: {[user.uid]: user.name}, players: [user.name], turncraftSeats: [user.uid], turncraftPlayers: {[user.uid]: member(user, input.factionId)}, settings: settings(ranked ? {...input.settings, maxPlayers: 2, fillAi: false} : input.settings), createdAt: stamp, updatedAt: stamp};
    await api.runTransaction(db, async tx => {
      const existing = await tx.get(roomRef(value));
      if (existing.exists()) {
        fail(existing.data().hostUid === user.uid, 'This room belongs to another commander.');
        return;
      }
      tx.set(roomRef(value), row);
      tx.set(secretRef(value), {inviteCode: code});
      tx.set(inviteRef(code), {lobbyId: value, createdAt: stamp, expiresAt: stamp + 86400000});
    });
    return get(value);
  }
  async function join(input) {
    const user = await who();
    let value = input.lobbyId, code = null;
    if (input.code) {
      code = String(input.code).trim().toUpperCase();
      fail(/^[A-F0-9]{12}$/.test(code), 'Enter the 12-character invite code.');
      const invite = await api.getDoc(inviteRef(code));
      fail(invite.exists() && invite.data().expiresAt > now(), 'This invitation has expired.', 'NOT_FOUND');
      value = invite.data().lobbyId;
    }
    await api.runTransaction(db, async tx => {
      const ref = roomRef(value), snap = await tx.get(ref);
      fail(snap.exists() && snap.data().gameType === 'turncraft' && snap.data().turncraftVersion === 1, 'This room has closed.', 'NOT_FOUND');
      const row = snap.data();
      if (row.members[user.uid]) {
        fail(row.members[user.uid] === user.name, 'Reopen the Game Hub profile that joined this room.', 'PROFILE_CHANGED');
        return;
      }
      fail(row.status === 'waiting', 'This match has already started.', 'MATCH_STARTED');
      fail(row.visibility === 'public' || code, 'Use this private room’s invite code.', 'INVITE_REQUIRED');
      fail(row.turncraftSeats.length < row.settings.maxPlayers, 'This room is full.', 'LOBBY_FULL');
      fail(!row.players.some(name => safeKey(name) === safeKey(user.name)), 'Another commander in this room uses that display name.', 'NAME_IN_USE');
      if (code) {
        const invite = await tx.get(inviteRef(code));
        fail(invite.exists() && invite.data().lobbyId === value && invite.data().expiresAt > now(), 'This invitation has expired.', 'INVITE_REQUIRED');
        tx.set(api.doc(db, 'lobbies', value, 'turncraft_joins', user.uid), {uid: user.uid, lobbyId: value, code, createdAt: now()});
      }
      tx.update(ref, {members: {...row.members, [user.uid]: user.name}, players: [...row.players, user.name], turncraftSeats: [...row.turncraftSeats, user.uid], turncraftPlayers: {...row.turncraftPlayers, [user.uid]: member(user, LOBBY_FACTIONS[row.players.length % 3])}, updatedAt: now()});
    });
    return get(value);
  }
  async function change(value, action, input = {}) {
    const user = await who();
    await api.runTransaction(db, async tx => {
      const ref = roomRef(value), snap = await tx.get(ref);
      fail(snap.exists() && snap.data().turncraftVersion === 1, 'This room has closed.', 'NOT_FOUND');
      const row = clone(snap.data());
      fail(row.members[user.uid], 'Join this room first.', 'NOT_A_MEMBER');
      if (action === 'leave') {
        if (row.status === 'finished') return; // Retain participants for delayed history claims.
        fail(['waiting', 'finished'].includes(row.status), 'Finish the active battle before leaving its room.', 'MATCH_STARTED');
        delete row.members[user.uid]; delete row.turncraftPlayers[user.uid];
        row.turncraftSeats = row.turncraftSeats.filter(uid => uid !== user.uid);
        row.players = row.turncraftSeats.map(uid => row.members[uid]);
        if (!row.players.length) row.status = 'closed';
        else if (row.hostUid === user.uid) {row.hostUid = row.turncraftSeats[0]; row.host = row.members[row.hostUid];}
      } else {
        fail(row.status === 'waiting', 'Match settings are locked after launch.', 'MATCH_STARTED');
        if (action === 'player') {
          const player = row.turncraftPlayers[user.uid];
          if (input.factionId !== undefined) {fail(LOBBY_FACTIONS.includes(input.factionId), 'Choose an existing faction.'); player.factionId = input.factionId; player.ready = false;}
          if (input.ready !== undefined) {fail(typeof input.ready === 'boolean', 'Choose a ready state.'); player.ready = input.ready;}
        } else if (action === 'configure') {
          fail(row.hostUid === user.uid, 'Only the host can configure this room.', 'HOST_ONLY');
          if (input.settings) row.settings = settings(row.ranked ? {...input.settings, maxPlayers: 2, fillAi: false} : input.settings);
          fail(row.settings.maxPlayers >= row.players.length, 'That limit is smaller than the current room.');
          if (input.visibility) {fail(['public', 'private'].includes(input.visibility), 'Choose public or private.'); row.visibility = input.visibility;}
          if (input.name) row.name = label(input.name);
          for (const player of Object.values(row.turncraftPlayers)) player.ready = false;
        } else if (action === 'start') {
          fail(row.hostUid === user.uid, 'Only the host can start this match.', 'HOST_ONLY');
          fail(row.players.length >= 2 && row.turncraftSeats.every(uid => row.turncraftPlayers[uid].ready), 'At least two commanders must be present and everyone must be ready.', 'NOT_READY');
          battleParticipantCount(row.settings, row.players.length);row.status = 'started'; row.startedAt = now();
          row.chessStarfrontReady = Object.fromEntries(row.turncraftSeats.map(uid => {const player = row.turncraftPlayers[uid]; return [safeKey(player.name), {username: player.name, factionId: player.factionId, objectiveVote: row.settings.gameMode, mapVote: row.settings.mapId, ready: true, readyAtMs: row.startedAt}];}));
        }
      }
      row.updatedAt = now(); tx.set(ref, row);
    });
    if (action === 'leave') return {status: 'left'};
    return get(value);
  }
  async function rematch(value) {
    const user = await who(), nextId = globalThis.crypto.randomUUID(), code = inviteCode(), stamp = now();
    let target;
    try { target = await api.runTransaction(db, async tx => {
      const ref = roomRef(value), snap = await tx.get(ref);
      fail(snap.exists(), 'This room has closed.', 'NOT_FOUND');
      const row = snap.data();
      fail(row.hostUid === user.uid, 'Only the host can request a rematch.', 'HOST_ONLY');
      fail(row.status === 'finished', 'Finish this match first.', 'MATCH_NOT_FINISHED');
      if (row.nextLobbyId) return row.nextLobbyId;
      const fresh = {name: row.name, gameType: 'turncraft', turncraftVersion: 1, status: 'waiting', visibility: row.visibility, ranked: row.ranked, host: row.host, hostUid: row.hostUid, members: row.members, players: row.players, turncraftSeats: row.turncraftSeats, turncraftPlayers: Object.fromEntries(Object.entries(row.turncraftPlayers).map(([uid, player]) => [uid, {...player, ready: false}])), settings: row.settings, rematchOf: value, createdAt: stamp, updatedAt: stamp};
      tx.set(roomRef(nextId), fresh); tx.set(secretRef(nextId), {inviteCode: code});
      tx.set(inviteRef(code), {lobbyId: nextId, createdAt: stamp, expiresAt: stamp + 86400000});
      tx.update(ref, {nextLobbyId: nextId, updatedAt: stamp}); return nextId;
    }); } catch (error) {
      // Rules reject a second concurrent pointer replacement before Firestore
      // retries its stale transaction. Recover only the same host's already
      // committed rematch; a denied attempt with no committed room still fails.
      const current = await api.getDoc(roomRef(value));
      if (!current.exists() || current.data().hostUid !== user.uid || current.data().status !== 'finished' || !current.data().nextLobbyId) throw error;
      target = current.data().nextLobbyId;
    }
    return get(target);
  }
  async function leaderboard() {
    await who();
    const snap = await api.getDocs(api.query(api.collection(db, 'users'), api.where('gameStats.turncraft.rankedMatches', '>', 0)));
    return {players: snap.docs.map(doc => ({username: doc.id, ...doc.data().gameStats.turncraft})).filter(p => Number.isFinite(p.mmr)).sort((a, b) => b.mmr - a.mmr).slice(0, 50)};
  }
  return {get, browse, create, join, decorate, player: (value, input) => change(value, 'player', input), configure: (value, input) => change(value, 'configure', input), start: value => change(value, 'start'), leave: value => change(value, 'leave'), rematch, leaderboard, complete: value => completeCustomMatch({db, auth, api}, value)};
}

export async function completeCustomMatch({db, auth, api}, value) {
  fail(auth.currentUser, 'Sign in again.', 'UNAUTHENTICATED'); id(value);
  return api.runTransaction(db, async tx => {
    const ref = api.doc(db, 'lobbies', value), snap = await tx.get(ref);
    fail(snap.exists() && snap.data().turncraftVersion === 1, 'This room has closed.', 'NOT_FOUND');
    const row = snap.data(), uid = auth.currentUser.uid;
    fail(row.members[uid], 'Only match participants can confirm this result.', 'NOT_A_MEMBER');
    if (row.status === 'finished') return {id: value, ...row};
    fail(row.status === 'started', 'Start this battle first.', 'MATCH_NOT_STARTED');
    fail(uid === row.hostUid || uid === row.chessStarfrontController?.ownerUid, 'The host or current controller must confirm this result.', 'HOST_ONLY');
    const state = await tx.get(api.doc(db, 'lobbies', value, 'chess_starfront', 'state')), result = state.data();
    fail(state.exists() && result.gameKey === 'chess_starfront' && result.gameOver === true && result.controllerUid === row.chessStarfrontController?.ownerUid && result.controllerEpoch === row.chessStarfrontController?.epoch && result.players?.length === battleParticipantCount(row.settings, row.players.length) && JSON.stringify(result.players.slice(0, row.players.length).map(p => p.name)) === JSON.stringify(row.players) && result.players.slice(row.players.length).every(p => p.ai === true && LOBBY_FACTIONS.includes(p.factionId)) && result.matchResult && (result.matchResult.winnerIdx === null || Number.isInteger(result.matchResult.winnerIdx) && result.matchResult.winnerIdx >= 0 && result.matchResult.winnerIdx < result.players.length), 'The final shared battlefield snapshot has not arrived yet.', 'RESULT_NOT_READY');
    const patch = {status: 'finished', finishedAt: Date.now(), turncraftResult: {winnerIdx: result.matchResult.winnerIdx ?? null, reason: String(result.matchResult.reason || '').slice(0, 200), players: result.players.map(p => ({name: p.name, factionId: p.factionId})), durationSeconds: Math.max(0, Math.floor((Number(result.matchResult.finishedAt) - Number(result.matchStartedAt || row.startedAt)) / 1000))}, updatedAt: Date.now()};
    tx.update(ref, patch); return {id: value, ...row, ...patch};
  });
}
