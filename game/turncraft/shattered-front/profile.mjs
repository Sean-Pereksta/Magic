import { firebaseConfig } from '../../../lobby/firebase-config.mjs';
import { assetUrl } from './assets.mjs';
import { createProfileStore } from './profile-store.mjs';
import { saveMatchResult } from './result-store.mjs';

export const RANKS = Object.freeze([
  { name: 'Bronze', min: 0 }, { name: 'Silver', min: 900 },
  { name: 'Gold', min: 1100 }, { name: 'Platinum', min: 1300 },
  { name: 'Diamond', min: 1500 }, { name: 'Master', min: 1750 }
]);
export function rankFor(mmr, matches = 0) {
  return Number(matches) > 0 ? [...RANKS].reverse().find((rank) => Number(mmr) >= rank.min)?.name || 'Bronze' : 'Unranked';
}
export function rankEmblem(profile = {}) {
  const rank = rankFor(profile.mmr, profile.rankedMatches).toLowerCase();
  return assetUrl(`rank.${rank}`, `ranks/${rank}.png`);
}
export function defaultProfile() {
  return {
    mmr: 1000, rank: 'Unranked', rankedMatches: 0, rankedWins: 0,
    rankedLosses: 0, rankedDraws: 0, provisionalMatchesRemaining: 10,
    highestMMR: 1000,
    campaign: { highestCompletedMission: 0, completedMissions: {}, unlockedRegions: ['amber-wastes'] },
    survival: { highestWave: 0, recordsByMap: {} }, achievements: {}
  };
}
const capabilities = {
  custom: true, ranked: true, globalSurvival: false,
  reason: 'Ranked duels, custom lobbies and profile saves use Firestore.'
};
let firebasePromise, authUnsubscribe, profileUnsubscribe, rebindProfile, watchStarting = false, watchRevision = 0;
let last = { identity: null, profile: defaultProfile(), capabilities };
const listeners = new Set();
let sawSelectedProfile = false;
export function selectedUsername() {
  let selected = null;
  try { selected = localStorage.getItem('gh.username'); } catch {}
  if (selected) sawSelectedProfile = true;
  const value = selected || (!sawSelectedProfile ? new URLSearchParams(globalThis.location?.search || '').get('username') : null);
  return typeof value === 'string' && value.trim() && !value.includes('/') ? value.trim() : null;
}
const username = selectedUsername;
export async function firebase() {
  if (!firebasePromise) firebasePromise = (async () => {
    const [appApi, authApi, storeApi] = await Promise.all([
      import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),
      import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),
      import('https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js')
    ]);
    const app = appApi.getApps().length ? appApi.getApp() : appApi.initializeApp(firebaseConfig);
    const auth = authApi.getAuth(app), db = storeApi.getFirestore(app);
    await auth.authStateReady();
    if (!auth.currentUser) await authApi.signInAnonymously(auth);
    return { app, auth, db, authApi, storeApi };
  })().catch((error) => { firebasePromise = null; throw error; });
  return firebasePromise;
}
function emit() { for (const callback of listeners) callback(last); }
function update(user, name, snapshot) {
  const account = snapshot?.exists() ? snapshot.data() : null;
  last = {
    identity: user ? {
      uid: user.uid, username: name,
      displayName: name || user.displayName || 'Commander',
      avatar: account?.photoURL || account?.avatar || user.photoURL || null,
      linked: Boolean(account)
    } : null,
    profile: { ...defaultProfile(), ...(account?.gameStats?.turncraft || {}) }, capabilities
  };
  emit();
  if (last.identity?.linked) void flushMatchResults(name);
  return last;
}
function failed(error) {
  last = { ...last, error: error.message };
  emit();
}
export async function getProfile() {
  const { auth, db, storeApi } = await firebase(), name = username();
  const snapshot = name ? await storeApi.getDoc(storeApi.doc(db, 'users', name)) : null;
  if (name !== username()) return last;
  return update(auth.currentUser, name, snapshot);
}
export async function getIdentity() { return (await getProfile()).identity; }
export async function getCapabilities() { return capabilities; }
async function startWatch() {
  if (authUnsubscribe || watchStarting || !listeners.size) return;
  watchStarting = true;
  try {
    const { auth, authApi, db, storeApi } = await firebase();
    if (!listeners.size) return;
    let observedUid, observedName;
    rebindProfile = () => {
      const user = auth.currentUser, name = username();
      if (observedUid === user?.uid && observedName === name) return;
      observedUid = user?.uid; observedName = name;
      const revision = ++watchRevision;
      profileUnsubscribe?.();
      profileUnsubscribe = null;
      update(user, name, null);
      if (user && name) profileUnsubscribe = storeApi.onSnapshot(
        storeApi.doc(db, 'users', name),
        (snapshot) => { if (revision === watchRevision) update(user, name, snapshot); },
        (error) => { if (revision === watchRevision) failed(error); }
      );
    };
    authUnsubscribe = authApi.onAuthStateChanged(auth, rebindProfile);
  } catch (error) { failed(error); }
  finally { watchStarting = false; }
}
function accountChanged(event) {
  if (event?.type === 'storage' && event.key && event.key !== 'gh.username') return;
  if (rebindProfile) rebindProfile();
  else void startWatch();
  if (last.identity?.linked) void flushMatchResults(username());
}
export function watchProfile(callback) {
  listeners.add(callback);
  if (listeners.size === 1) {
    globalThis.addEventListener?.('storage', accountChanged);
    globalThis.addEventListener?.('focus', accountChanged);
    globalThis.addEventListener?.('online', accountChanged);
    globalThis.document?.addEventListener('visibilitychange', accountChanged);
  }
  callback(last);
  void startWatch();
  return () => {
    listeners.delete(callback);
    if (!listeners.size) {
      profileUnsubscribe?.(); authUnsubscribe?.();
      profileUnsubscribe = authUnsubscribe = null;
      rebindProfile = null;
      watchRevision++;
      globalThis.removeEventListener?.('storage', accountChanged);
      globalThis.removeEventListener?.('focus', accountChanged);
      globalThis.removeEventListener?.('online', accountChanged);
      globalThis.document?.removeEventListener('visibilitychange', accountChanged);
    }
  };
}
async function save(mode, result, options = {}) {
  const name = options.username || last.identity?.username || username();
  const { auth, db, storeApi } = await firebase();
  if (!auth.currentUser || !name) throw new Error('Sign into your CatnMice account to sync progress.');
  if (name !== username()) {
    rebindProfile?.();
    throw new Error('Your account changed. Progress is kept with the account that played this run.');
  }
  const value = await createProfileStore({ db, api: storeApi }).save(name, mode, result);
  if (name !== username()) { rebindProfile?.(); return last; }
  return update(auth.currentUser, name, { exists: () => true, data: () => ({ ...value.account, gameStats: { ...value.account.gameStats, turncraft: value.profile } }) });
}
export function saveCampaign(result, options) { return save('campaign', result, options); }
export function saveSurvival(result, options) { return save('survival', result, options); }

export async function saveLobbyResult(lobbyId, name = username()) {
  if (name !== username()) throw new Error('Return with the Game Hub profile that played this match to save its result.');
  const connection = await firebase();
  const uid = connection.auth.currentUser?.uid;
  const value = await saveMatchResult({...connection, api: connection.storeApi, isCurrentAccount: () => username() === name && connection.auth.currentUser?.uid === uid}, lobbyId, name);
  try { localStorage.setItem(pendingMatchKey(name), JSON.stringify(pendingMatches(name).filter(id => id !== lobbyId))); } catch {}
  try { await connection.storeApi.deleteDoc(connection.storeApi.doc(connection.db, 'users', name, 'turncraftPending', lobbyId)); } catch {}
  return value;
}

const pendingMatchKey = name => `turncraft.pendingMatches.${encodeURIComponent(name)}`;
function pendingMatches(name) {
  try { const saved = JSON.parse(localStorage.getItem(pendingMatchKey(name)) || '[]'); return Array.isArray(saved) ? saved.filter(id => typeof id === 'string' && /^[A-Za-z0-9_-]{1,100}$/.test(id)) : []; } catch { return []; }
}
export async function rememberLobby(lobbyId, name = username()) {
  if (!name || name !== username() || !/^[A-Za-z0-9_-]{1,100}$/.test(lobbyId)) return;
  try { localStorage.setItem(pendingMatchKey(name), JSON.stringify([...new Set([...pendingMatches(name), lobbyId])].slice(-100))); } catch {}
  try {
    const {db, storeApi} = await firebase(), ref = storeApi.doc(db, 'users', name, 'turncraftPending', lobbyId);
    await storeApi.runTransaction(db, async tx => {
      const existing = await tx.get(ref);
      if (name !== username()) return;
      if (!existing.exists()) tx.set(ref, {lobbyId, username: name, createdAt: Date.now()});
    });
  } catch { /* The profile-scoped local queue remains available while offline. */ }
}
export async function forgetLobby(lobbyId, name = username()) {
  if (!name || name !== username()) return;
  try {
    const {db, storeApi} = await firebase();
    await storeApi.deleteDoc(storeApi.doc(db, 'users', name, 'turncraftPending', lobbyId));
    localStorage.setItem(pendingMatchKey(name), JSON.stringify(pendingMatches(name).filter(id => id !== lobbyId)));
  } catch { /* Finished matches remain queued until their receipt is saved. */ }
}
let flushingMatches = false;
async function flushMatchResults(name) {
  if (flushingMatches || !name || name !== username()) return;
  flushingMatches = true;
  try {
    let ids = pendingMatches(name);
    try {
      const {db, storeApi} = await firebase();
      const snapshot = await storeApi.getDocs(storeApi.query(storeApi.collection(db, 'users', name, 'turncraftPending'), storeApi.limit(100)));
      ids = [...new Set([...ids, ...snapshot.docs.map(doc => doc.id)])];
    } catch {}
    for (const id of ids) {
      if (name !== username()) break;
      try {
        const {db, storeApi} = await firebase();
        const room = await storeApi.getDoc(storeApi.doc(db, 'lobbies', id));
        if (name !== username()) break;
        if (!room.exists() || room.data().status === 'closed' || !room.data().players?.includes(name)) await forgetLobby(id, name);
        else await saveLobbyResult(id, name);
      } catch {}
    }
  }
  finally { flushingMatches = false; if (name !== username()) void flushMatchResults(username()); }
}

export async function getHistory(name = username()) {
  if (!name || name !== username()) return [];
  const {db, storeApi} = await firebase();
  const snapshots = await Promise.all(['turncraftHistory', 'turncraftSoloHistory'].map(collection => storeApi.getDocs(storeApi.query(storeApi.collection(db, 'users', name, collection), storeApi.orderBy('completedAt', 'desc'), storeApi.limit(30)))));
  return name === username() ? snapshots.flatMap(snapshot => snapshot.docs.map(doc => doc.data())).sort((a, b) => b.completedAt - a.completedAt).slice(0, 30) : [];
}

export async function getCheckpoint(name = username()) {
  if (!name || name !== username()) return null;
  const {db, storeApi} = await firebase();
  const snapshot = await storeApi.getDoc(storeApi.doc(db, 'users', name, 'turncraftSaves', 'active'));
  const saved = snapshot.data();
  if (!saved || saved.username !== name || name !== username()) return null;
  if (saved.packed === null) return {cleared: true, savedAt: saved.savedAt, completedRunId: saved.completedRunId || null, username: name};
  if (!saved.packed) return null;
  return JSON.parse(saved.packed);
}

export async function saveCheckpoint(checkpoint, name = username(), completedRunId = null) {
  if (!name || name !== username()) throw new Error('This checkpoint belongs to the profile that started the mission.');
  const {db, storeApi} = await firebase();
  const packed = checkpoint ? JSON.stringify(checkpoint) : null;
  if (packed && packed.length > 850000) throw new Error('The battlefield is too large for a cloud checkpoint.');
  const ref = storeApi.doc(db, 'users', name, 'turncraftSaves', 'active');
  // A slow write from an older checkpoint cannot overwrite a newer checkpoint.
  const savedAt = checkpoint?.savedAt || Date.now();
  await storeApi.runTransaction(db, async tx => {
    const existing = await tx.get(ref);
    if (name !== username()) throw new Error('Your selected profile changed before this checkpoint synced.');
    if (!checkpoint && completedRunId && existing.data()?.packed && JSON.parse(existing.data().packed).resultId !== completedRunId) return;
    if ((existing.data()?.savedAt || 0) > savedAt) return;
    tx.set(ref, {username: name, packed, savedAt, completedRunId: checkpoint ? null : completedRunId});
  });
}
