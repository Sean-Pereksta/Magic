import { createGame, applyAction, getActivePlayer, SPELLS, ENEMIES, DIFFICULTIES, COLS, TOWER_ROW, MAX_HAND } from './engine.mjs';

export const SAVE_KEY = 'spelltower.glade.v2';
export const INACTIVE_TURN_MS = 90000;
const clone = value => JSON.parse(JSON.stringify(value));
const validName = name => typeof name === 'string' && name.trim().length > 0 && name.length <= 32 && !['__proto__', 'prototype', 'constructor'].includes(name.toLowerCase()) && !/[\u0000-\u001f\u007f/]/.test(name);

/** Local play does not import, initialize, or wait for a network SDK. */
export function saveRun(storage, game) {
  if (!game || game.mode === 'online') return false;
  try {
    if (game.status !== 'playing') { storage.removeItem(SAVE_KEY); return true; }
    storage.setItem(SAVE_KEY, JSON.stringify({ version: 2, game }));
    return true;
  } catch { return false; }
}

export function loadRun(storage) {
  try {
    const raw = storage.getItem(SAVE_KEY);
    if (!raw || raw.length > 500000) return null;
    const saved = JSON.parse(raw), game = saved?.game;
    if (saved.version !== 2 || !validLocalRun(game)) return null;
    return game;
  } catch { return null; }
}

// A partial or outdated save must not get as far as the renderer or turn engine.
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const integer = (value, minimum = 0, maximum = Number.MAX_SAFE_INTEGER) => Number.isSafeInteger(value) && value >= minimum && value <= maximum;
const identifier = value => typeof value === 'string' && value.length > 0 && value.length <= 128;
const boardPoint = point => record(point) && integer(point.x, 0, COLS - 1) && integer(point.y, 0, TOWER_ROW - 1);
const groundTimers = ['wet', 'vines', 'burn', 'ice', 'mud', 'stone', 'steam', 'charged', 'poison'];
const foeTimers = ['wet', 'stun', 'frozen', 'rooted', 'burn', 'poison', 'slow', 'blinded', 'vulnerable'];

function validLocalRun(game) {
  if (!record(game) || game.version !== 2 || !['solo', 'hotseat'].includes(game.mode) || game.status !== 'playing' || typeof game.difficulty !== 'string' || !Object.hasOwn(DIFFICULTIES, game.difficulty)) return false;
  if (!Array.isArray(game.players) || !integer(game.players.length, 1, game.mode === 'solo' ? 1 : 4)
    || !game.players.every(validName)
    || new Set(game.players).size !== game.players.length) return false;
  if (!integer(game.activeIndex, 0, game.players.length - 1) || !integer(game.revision) || !integer(game.turn)
    || !integer(game.round, 1) || !integer(game.wave, 1, 10) || !integer(game.roundsInWave, 1, 3)
    || !integer(game.maxHP, 1) || !integer(game.hp, 1, game.maxHP) || !integer(game.maxMana, 1)
    || !integer(game.mana, 0, game.maxMana) || !integer(game.rng, 0, 0xffffffff)
    || !integer(game.idCounter) || !integer(game.kills) || !integer(game.combos)) return false;
  if (!Array.isArray(game.pendingPlayers) || !game.pendingPlayers.length
    || new Set(game.pendingPlayers).size !== game.pendingPlayers.length
    || !game.pendingPlayers.every(name => game.players.includes(name))
    || !game.pendingPlayers.includes(getActivePlayer(game))) return false;
  if (!record(game.hands) || Object.keys(game.hands).some(name => !game.players.includes(name))) return false;
  const ids = new Set();
  for (const name of game.players) {
    const hand = game.hands[name];
    if (!Array.isArray(hand) || hand.length > MAX_HAND) return false;
    for (const card of hand) {
      if (!record(card) || !identifier(card.id) || ids.has(card.id) || typeof card.key !== 'string' || !Object.hasOwn(SPELLS, card.key)
        || !integer(card.charges, 1, SPELLS[card.key].charges) || !integer(card.usedTurn, -1, game.turn)) return false;
      ids.add(card.id);
    }
  }
  if (!Array.isArray(game.enemies) || game.enemies.length > COLS * TOWER_ROW) return false;
  const occupied = new Set();
  for (const foe of game.enemies) {
    if (!boardPoint(foe) || !identifier(foe.id) || ids.has(foe.id) || typeof foe.key !== 'string' || !Object.hasOwn(ENEMIES, foe.key)
      || !integer(foe.maxHP, 1) || !integer(foe.hp, 1, foe.maxHP) || !integer(foe.speed, 1)
      || !integer(foe.damage, 1) || !integer(foe.burnPower) || !foeTimers.every(key => integer(foe[key]))) return false;
    const cell = `${foe.x},${foe.y}`;
    if (occupied.has(cell)) return false;
    occupied.add(cell); ids.add(foe.id);
  }
  if (!Array.isArray(game.spawnQueue) || game.spawnQueue.length > 32
    || !game.spawnQueue.every(item => record(item) && typeof item.key === 'string' && Object.hasOwn(ENEMIES, item.key))) return false;
  if (!record(game.terrain)) return false;
  for (const [cell, tile] of Object.entries(game.terrain)) {
    const match = /^(\d+),(\d+)$/.exec(cell);
    if (!match || cell !== `${Number(match[1])},${Number(match[2])}` || !boardPoint({ x: Number(match[1]), y: Number(match[2]) }) || !record(tile)
      || !groundTimers.some(key => integer(tile[key], 1))
      || Object.entries(tile).some(([key, value]) => ![...groundTimers, 'burnPower', 'strength'].includes(key) || !integer(value, 1))) return false;
  }
  if (!Array.isArray(game.events) || game.events.length > 72) return false;
  for (const event of game.events) {
    if (!record(event) || !identifier(event.id) || typeof event.type !== 'string' || typeof event.text !== 'string'
      || typeof event.element !== 'string' || !Number.isFinite(event.amount)
      || !integer(event.x, -1, COLS - 1) || !integer(event.y, -1, TOWER_ROW)
      || (event.cells !== undefined && (!Array.isArray(event.cells) || !event.cells.every(boardPoint)))) return false;
  }
  return true;
}

export function makeLocalNames(count, values = []) {
  const names = [];
  for (let i = 0; i < Math.min(4, Math.max(1, count)); i++) {
    let name = String(values[i] || `Mage ${i + 1}`).trim().slice(0, 26);
    if (!validName(name)) name = `Mage ${i + 1}`;
    const base = name;
    let suffix = 2;
    while (names.includes(name)) name = `${base} ${suffix++}`;
    names.push(name);
  }
  return names;
}

function lobbyPlayers(lobby, username) {
  if (lobby.gameType !== 'spelltower') throw new Error('This room belongs to a different game. Open Spell Tower from the lobby.');
  if (lobby.status !== 'started') throw new Error('Wait for the host to start Spell Tower from the lobby.');
  const players = Array.isArray(lobby.players) ? [...new Set(lobby.players)].filter(validName).slice(0, 8) : [];
  if (!players.includes(username)) throw new Error('You are no longer in this room. Rejoin Spell Tower from the lobby.');
  return players;
}

function roomSeed(id) {
  let seed = 2166136261;
  for (const char of id) seed = Math.imul(seed ^ char.charCodeAt(0), 16777619);
  return seed >>> 0;
}

/**
 * Firestore is injected so the real transaction/reconnection logic can be tested
 * without a public room. Snapshots only render; they never write back to state.
 * The existing authenticated lobby subcollection rules cover this path.
 */
export async function connectRoom({ sdk, db, gameId, username, onState, onConnection = () => {}, onError = () => {}, now = Date.now }) {
  if (!gameId || gameId.includes('/') || gameId.length > 160 || !validName(username)) throw new Error('Open an online Spell Tower room from the lobby.');
  const lobbyRef = sdk.doc(db, 'lobbies', gameId);
  const stateRef = sdk.doc(db, 'lobbies', gameId, 'spelltower', 'state');
  let closed = false, last = null, unsubscribers = [], reconcilePending = false;

  async function readBoth(tx) {
    const lobbySnap = await tx.get(lobbyRef), stateSnap = await tx.get(stateRef);
    if (!lobbySnap.exists()) throw new Error('This lobby has closed. Return to the arcade to start another.');
    const lobby = lobbySnap.data(), players = lobbyPlayers(lobby, username);
    const stored = stateSnap.exists() ? stateSnap.data() : null;
    if (stored && (stored.version !== 2 || !stored.game)) throw new Error('This room uses an older Spell Tower version. Create a fresh room.');
    return { lobby, players, stored };
  }

  async function reconcile() {
    if (closed || reconcilePending) return;
    reconcilePending = true;
    try {
      await sdk.runTransaction(db, async tx => {
        const { lobby, players, stored } = await readBoth(tx);
        const host = players.includes(lobby.host) ? lobby.host : players[0];
        if (!stored) {
          const game = createGame({ players, mode: 'online', seed: roomSeed(gameId), difficulty: lobby.spelltowerDifficulty || 'normal' });
          tx.set(stateRef, { version: 2, game, host, changedAt: now(), startedAt: now() });
          return;
        }
        let game = stored.game;
        // Only reconcile differences. A presence snapshot cannot create a write loop.
        for (const name of game.players.filter(name => !players.includes(name))) game = applyAction(game, name, { type: 'leave', player: name });
        for (const name of players.filter(name => !game.players.includes(name))) {
          if (game.status === 'playing') game = applyAction(game, name, { type: 'join', player: name });
        }
        if (game !== stored.game || host !== stored.host) tx.set(stateRef, { ...stored, game, host, changedAt: now() });
      });
    } finally { reconcilePending = false; }
  }

  await reconcile();
  if (closed) return;
  unsubscribers.push(sdk.onSnapshot(stateRef, { includeMetadataChanges: true }, snap => {
    if (closed || !snap.exists() || snap.metadata?.hasPendingWrites) return;
    const stored = snap.data();
    if (stored.version !== 2 || !stored.game) return;
    last = stored;
    onConnection(snap.metadata?.fromCache ? 'Reconnecting…' : 'Connected');
    onState(clone(stored.game), { host: stored.host, changedAt: stored.changedAt, startedAt: stored.startedAt, fromCache: !!snap.metadata?.fromCache });
  }, error => { if (!closed) { onConnection('Connection interrupted'); onError(error); } }));
  unsubscribers.push(sdk.onSnapshot(lobbyRef, snap => {
    if (closed) return;
    if (!snap.exists()) { onError(new Error('The lobby has closed.')); return; }
    if (!(snap.data().players || []).includes(username)) { onError(new Error('You have left this room. Return to the arcade to rejoin.')); return; }
    reconcile().catch(onError);
  }, onError));

  const session = {
    get metadata() { return last; },
    async dispatch(action, expectedRevision) {
      if (closed) throw new Error('This connection has closed.');
      return sdk.runTransaction(db, async tx => {
        const { stored } = await readBoth(tx);
        if (!stored) throw new Error('The match is still getting ready.');
        if (stored.game.revision !== expectedRevision) throw new Error('The battlefield changed. Check the updated board and try again.');
        if (['join', 'leave'].includes(action.type)) throw new Error('Use the lobby to change the party.');
        const game = applyAction(stored.game, username, action);
        tx.set(stateRef, { ...stored, game, changedAt: now() });
        return game;
      });
    },
    async restart() {
      if (closed) throw new Error('This connection has closed.');
      await sdk.runTransaction(db, async tx => {
        const { stored, players } = await readBoth(tx);
        if (!stored || stored.host !== username) throw new Error('The host can start the next adventure.');
        const game = createGame({ players, mode: 'online', seed: (now() ^ roomSeed(gameId)) >>> 0, difficulty: stored.game.difficulty });
        // A restart has a new run ID and a strictly newer revision for old queued actions.
        game.revision = stored.game.revision + 1;
        tx.set(stateRef, { ...stored, game, changedAt: now(), startedAt: now() });
      });
    },
    async skipInactive() {
      if (closed) throw new Error('This connection has closed.');
      await sdk.runTransaction(db, async tx => {
        const { stored } = await readBoth(tx);
        if (!stored || stored.host !== username) throw new Error('Only the host can pass an inactive turn.');
        if (now() - stored.changedAt < INACTIVE_TURN_MS) throw new Error('Wait 90 seconds without a move before passing this turn.');
        const active = getActivePlayer(stored.game);
        if (active === username) throw new Error('Use End turn to pass your own turn.');
        const game = applyAction(stored.game, active, { type: 'endTurn' });
        tx.set(stateRef, { ...stored, game, changedAt: now() });
      });
    },
    async leave() {
      if (closed) return;
      await sdk.runTransaction(db, async tx => {
        // A room deleted by another client or a seat already removed needs only
        // a local unsubscribe. Requiring membership here would trap the user.
        const lobbySnap = await tx.get(lobbyRef);
        if (!lobbySnap.exists()) return;
        const lobby = lobbySnap.data();
        if (lobby.gameType !== 'spelltower' || !Array.isArray(lobby.players) || !lobby.players.includes(username)) return;
        const stateSnap = await tx.get(stateRef);
        const stored = stateSnap.exists() ? stateSnap.data() : null;
        const players = [...new Set(lobby.players)].filter(validName).slice(0, 8);
        const remaining = players.filter(name => name !== username);
        if (!remaining.length) {
          tx.delete(stateRef);
          tx.delete(lobbyRef);
          return;
        }
        const host = remaining.includes(lobby.host) ? lobby.host : remaining[0];
        if (stored?.version === 2 && Array.isArray(stored.game?.players)) {
          const game = stored.game.players.includes(username) ? applyAction(stored.game, username, { type: 'leave', player: username }) : stored.game;
          tx.set(stateRef, { ...stored, game, host, changedAt: now() });
        }
        tx.update(lobbyRef, { players: remaining, host, updatedAt: now() });
      });
      session.close();
    },
    close() { closed = true; for (const unsubscribe of unsubscribers) unsubscribe(); unsubscribers = []; }
  };
  return session;
}
