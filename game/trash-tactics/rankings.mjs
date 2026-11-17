import {STARTING_ELO} from './rank-tiers.mjs';

const PROJECT_ID = 'bible-game-246c0';
const RESULT_PATH = 'trashTacticsRankedResults';
const RATING_PATH = 'gameStats.trashTactics';
const INVALID_TOKEN = new Set([
  'auth/argument-error', 'auth/invalid-id-token', 'auth/id-token-expired',
  'auth/id-token-revoked', 'auth/user-disabled', 'auth/user-not-found'
]);

function rankError(code, message, status) {
  return Object.assign(new Error(message), {code, status});
}

function usernameIsValid(name) {
  return typeof name === 'string' && name === name.trim() && name.length > 0 && name.length <= 32 && !name.includes('/') && name !== '.' && name !== '..' && !/^__.*__$/.test(name);
}

function count(value) {
  return Number.isSafeInteger(value) && value >= 0 ? value : 0;
}

function readStats(account) {
  const saved = account?.gameStats?.trashTactics;
  const wins = count(saved?.wins), losses = count(saved?.losses), draws = count(saved?.draws);
  return {rating: Number.isSafeInteger(saved?.rating) && saved.rating >= 0 ? saved.rating : STARTING_ELO,
    wins, losses, draws, games: wins + losses + draws};
}

function publicProfile(snapshot) {
  const data = snapshot.data();
  const displayName = typeof data.displayName === 'string' ? data.displayName.trim().slice(0, 64) : '';
  return {username: snapshot.id, displayName: displayName || snapshot.id, ...readStats(data)};
}

export function createRankings({db, auth}) {
  const userRef = username => db.collection('users').doc(username);
  const bindingRef = uid => db.collection('factionForgeAccounts').doc(uid);
  const nameRef = username => db.collection('factionForgeNames').doc(username);
  const notLinked = () => rankError('ACCOUNT_NOT_LINKED', 'Ranked play requires a verified Catnmice profile. Ask the Catnmice operator to link your Firebase sign-in.', 403);

  async function identity(token) {
    if (typeof token !== 'string' || token.length === 0 || token.length > 8192) {
      throw rankError('AUTH_REQUIRED', 'Open the Catnmice Game Hub and sign in before joining ranked play.', 401);
    }

    let uid;
    try {
      ({uid} = await auth.verifyIdToken(token, true));
    } catch (error) {
      if (!INVALID_TOKEN.has(error.code)) throw error;
      throw rankError('AUTH_REQUIRED', 'Your Catnmice sign-in expired. Reopen the Game Hub and try again.', 401);
    }
    const binding = (await bindingRef(uid).get()).data();
    if (binding?.verified !== true || !usernameIsValid(binding.username)) throw notLinked();

    const [name, profile] = await Promise.all([nameRef(binding.username).get(), userRef(binding.username).get()]);
    if (name.data()?.uid !== uid || name.data()?.verified !== true || !profile.exists) throw notLinked();
    return {uid, ...publicProfile(profile)};
  }

  async function profile(username) {
    if (!usernameIsValid(username)) return null;
    const snapshot = await userRef(username).get();
    return snapshot.exists ? publicProfile(snapshot) : null;
  }

  async function leaderboard(limit = 25) {
    const size = Number.isInteger(limit) ? Math.max(1, Math.min(100, limit)) : 25;
    const snapshots = await db.collection('users').orderBy(`${RATING_PATH}.rating`, 'desc').limit(size).get();
    return snapshots.docs.map(publicProfile);
  }

  async function recordResult({id, players, winnerTeam}) {
    if (typeof id !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,119}$/.test(id) || !Array.isArray(players) || players.length !== 2 ||
      players.some(player => !usernameIsValid(player?.username) || typeof player.uid !== 'string' || !player.uid || player.uid.includes('/')) ||
      players[0].uid === players[1].uid || players[0].username === players[1].username || ![0, 1, null].includes(winnerTeam)) {
      throw rankError('INVALID_RESULT', 'The ranked match result is invalid.', 400);
    }

    const resultRef = db.collection(RESULT_PATH).doc(id);
    const completedAt = Date.now();
    return db.runTransaction(async tx => {
      const savedResult = await tx.get(resultRef);
      if (savedResult.exists) {
        const saved = savedResult.data();
        if (saved.winnerTeam !== winnerTeam || saved.players.some((row, team) => row.uid !== players[team].uid || row.username !== players[team].username)) {
          throw rankError('RESULT_CONFLICT', 'A different result is already saved for this match.', 409);
        }
        return saved;
      }

      const refs = players.map(player => userRef(player.username));
      const [profiles, bindings, names] = await Promise.all([
        Promise.all(refs.map(ref => tx.get(ref))),
        Promise.all(players.map(player => tx.get(bindingRef(player.uid)))),
        Promise.all(players.map(player => tx.get(nameRef(player.username))))
      ]);
      if (players.some((player, team) => {
        const binding = bindings[team].data(), name = names[team].data();
        return !profiles[team].exists || binding?.verified !== true || binding.username !== player.username ||
          name?.verified !== true || name.uid !== player.uid;
      })) throw notLinked();

      const [first, second] = profiles.map(snapshot => readStats(snapshot.data()));
      const expected = 1 / (1 + 10 ** ((second.rating - first.rating) / 400));
      const actual = winnerTeam === null ? 0.5 : Number(winnerTeam === 0);
      const delta = Math.min(second.rating, Math.max(-first.rating, Math.round(32 * (actual - expected))));
      const result = {id, winnerTeam, completedAt, players: [first, second].map((stats, team) => {
        const {uid, username} = players[team];
        const change = team === 0 ? delta : -delta;
        const rating = stats.rating + change;
        const outcome = winnerTeam === null ? 'draws' : team === winnerTeam ? 'wins' : 'losses';
        const next = {...stats, rating, [outcome]: stats[outcome] + 1, games: stats.games + 1, lastMatchId: id, lastPlayedAt: completedAt};
        tx.update(refs[team], RATING_PATH, next);
        return {uid, username, before: stats.rating, rating, change, wins: next.wins, losses: next.losses, draws: next.draws, games: next.games};
      })};
      tx.create(resultRef, result);
      return result;
    });
  }

  return {identity, profile, leaderboard, recordResult};
}

export async function createCatnmiceRankings() {
  const [{applicationDefault, getApps, initializeApp}, {getAuth}, {getFirestore}] = await Promise.all([
    import('firebase-admin/app'), import('firebase-admin/auth'), import('firebase-admin/firestore')
  ]);
  const name = 'trash-tactics-rankings';
  const app = getApps().find(app => app.name === name) || initializeApp({projectId: PROJECT_ID, credential: applicationDefault()}, name);
  return createRankings({db: getFirestore(app), auth: getAuth(app)});
}
