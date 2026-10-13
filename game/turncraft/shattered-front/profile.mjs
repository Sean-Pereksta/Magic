import { firebaseConfig } from '../../../lobby/firebase-config.mjs';
import { assetUrl } from './assets.mjs';

export const RANKS = Object.freeze([
  { name: 'Bronze', min: 0 },
  { name: 'Silver', min: 900 },
  { name: 'Gold', min: 1100 },
  { name: 'Platinum', min: 1300 },
  { name: 'Diamond', min: 1500 },
  { name: 'Master', min: 1750 }
]);
export function rankFor(mmr, matches = 0) {
  return Number(matches) > 0
    ? [...RANKS].reverse().find((rank) => Number(mmr) >= rank.min)?.name ||
        'Bronze'
    : 'Unranked';
}
export function rankEmblem(profile = {}) {
  const rank = rankFor(profile.mmr, profile.rankedMatches);
  return assetUrl(
    `rank.${rank.toLowerCase()}`,
    `ranks/${rank.toLowerCase()}.png`
  );
}
export function defaultProfile() {
  return {
    mmr: 1000,
    rank: 'Unranked',
    rankedMatches: 0,
    rankedWins: 0,
    rankedLosses: 0,
    rankedDraws: 0,
    provisionalMatchesRemaining: 10,
    highestMMR: 1000,
    campaign: {
      highestCompletedMission: 0,
      completedMissions: {},
      unlockedRegions: ['amber-wastes']
    },
    survival: { highestWave: 0, recordsByMap: {} },
    achievements: {}
  };
}
const unavailable = {
  custom: false,
  ranked: false,
  globalSurvival: false,
  reason:
    'Cloud services are connecting. Your local campaign remains available.'
};
let firebasePromise = null,
  last = {
    identity: null,
    profile: defaultProfile(),
    capabilities: unavailable
  },
  listeners = new Set(),
  profileUnsubscribe = null,
  authUnsubscribe = null;
export async function firebase() {
  if (!firebasePromise)
    firebasePromise = (async () => {
      const [appApi, authApi, storeApi] = await Promise.all([
        import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),
        import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),
        import(
          'https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js'
        )
      ]);
      const app = appApi.getApps().length
          ? appApi.getApp()
          : appApi.initializeApp(firebaseConfig),
        auth = authApi.getAuth(app),
        db = storeApi.getFirestore(app);
      await auth.authStateReady();
      if (!auth.currentUser) await authApi.signInAnonymously(auth);
      return { app, auth, db, authApi, storeApi };
    })();
  return firebasePromise;
}
function apiBase() {
  let configured = globalThis.TURNCRAFT_API_BASE;
  try {
    configured = configured || localStorage.getItem('turncraft.apiBase');
  } catch {}
  const base = String(configured || '/api/turncraft').replace(/\/$/, '');
  const url = new URL(base, location.href);
  if (url.origin !== location.origin && url.protocol !== 'https:')
    throw new Error('The TurnCraft cloud service must use HTTPS.');
  return url.href.replace(/\/$/, '');
}
export async function request(
  path,
  { method = 'GET', body, timeoutMs = 15000 } = {}
) {
  const { auth } = await firebase();
  if (!auth.currentUser)
    throw new Error('Your CatnMice sign-in is unavailable.');
  const token = await auth.currentUser.getIdToken(),
    controller = new AbortController(),
    timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(apiBase() + path, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        ...(body ? { 'Content-Type': 'application/json' } : {})
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
      signal: controller.signal
    });
    let value;
    try {
      value = await response.json();
    } catch {
      throw new Error(
        'TurnCraft cloud services need to be deployed. Local play is available.'
      );
    }
    if (!response.ok) {
      const error = new Error(
        value.error?.message || 'Cloud services are temporarily unavailable.'
      );
      error.code = value.error?.code;
      throw error;
    }
    return value;
  } catch (error) {
    if (error.name === 'AbortError')
      throw new Error(
        'The cloud service is taking too long. Retry when your connection returns.'
      );
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
function emit() {
  for (const callback of listeners) callback(last);
}
export async function getIdentity() {
  return (await getProfile()).identity;
}
export async function getProfile() {
  const value = await request('/profile');
  last = { ...last, ...value };
  emit();
  return last;
}
export async function getCapabilities() {
  const result = await getProfile();
  return result.capabilities;
}
export function watchProfile(callback) {
  listeners.add(callback);
  callback(last);
  if (!authUnsubscribe)
    void firebase()
      .then(({ auth, authApi, db, storeApi }) => {
        if (authUnsubscribe || !listeners.size) return;
        authUnsubscribe = authApi.onAuthStateChanged(auth, (user) => {
          profileUnsubscribe?.();
          profileUnsubscribe = null;
          if (!user) {
            last = {
              identity: null,
              profile: defaultProfile(),
              capabilities: unavailable
            };
            emit();
            return;
          }
          last = {
            ...last,
            identity: {
              uid: user.uid,
              username: null,
              displayName:
                user.displayName ||
                localStorage.getItem('gh.username') ||
                'Commander',
              avatar: user.photoURL || null,
              linked: false
            }
          };
          profileUnsubscribe = storeApi.onSnapshot(
            storeApi.doc(db, 'turncraftProfiles', user.uid),
            (snapshot) => {
              if (snapshot.exists())
                last = { ...last, profile: snapshot.data() };
              emit();
            },
            () => {}
          );
          void getProfile().catch((error) => {
            last = {
              ...last,
              error: error.message,
              capabilities: { ...unavailable, reason: error.message }
            };
            emit();
          });
        });
      })
      .catch((error) => {
        last = { ...last, error: error.message };
        emit();
      });
  return () => {
    listeners.delete(callback);
    if (!listeners.size) {
      profileUnsubscribe?.();
      authUnsubscribe?.();
      profileUnsubscribe = authUnsubscribe = null;
    }
  };
}
export async function saveCampaign(result) {
  const body = {
    missionId: result.missionId,
    stars: Math.max(1, Math.min(3, Math.round(result.stars || 1))),
    optionalObjectives: result.optionalObjectives || [],
    regionId: result.regionId,
    difficulty: result.difficulty || 'medium'
  };
  const value = await request('/progress/campaign', { method: 'POST', body });
  last = { ...last, ...value };
  emit();
  return last;
}
export async function saveSurvival(result) {
  const body = {
    arenaId: result.arenaId,
    wave: Math.max(0, Math.floor(result.wave || 0)),
    durationSeconds: Math.max(0, Math.floor(result.durationSeconds || 0)),
    enemiesDefeated: Math.max(0, Math.floor(result.enemiesDefeated || 0)),
    factionId: result.factionId,
    difficulty: result.difficulty || 'medium',
    date: result.date || new Date().toISOString()
  };
  const value = await request('/progress/survival', { method: 'POST', body });
  last = { ...last, ...value };
  emit();
  return last;
}
