import { firebaseConfig } from '../../../lobby/firebase-config.mjs';
import { assetUrl } from './assets.mjs';
import { createProfileStore } from './profile-store.mjs';

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
  custom: true, ranked: false, globalSurvival: false,
  reason: 'Custom lobbies and account saves are available. Ranked matches are unavailable.'
};
let firebasePromise, authUnsubscribe, profileUnsubscribe, rebindProfile, watchStarting = false, watchRevision = 0;
let last = { identity: null, profile: defaultProfile(), capabilities };
const listeners = new Set();
const username = () => {
  try { return localStorage.getItem('gh.username') || null; } catch { return null; }
};
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
