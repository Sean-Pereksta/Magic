// server.js
// Grid Overlord — Node host server (authoritative sim)

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const admin = require('firebase-admin');
const path = require('path');
const fs = require('fs');

// ------------- FIREBASE ADMIN SETUP -------------
const serviceAccountPath = './serviceAccountKey.json';
let serviceAccount;

if (fs.existsSync(serviceAccountPath)) {
  serviceAccount = require(serviceAccountPath);
} else if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
} else {
  console.error('Firebase service account not found. Please provide serviceAccountKey.json or FIREBASE_SERVICE_ACCOUNT env var.');
  process.exit(1);
}

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

// ------------- EXPRESS + SOCKET.IO -------------
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

// Simple health route
app.get('/', (req, res) => {
  res.send('Grid Overlord Node server running');
});

// ------------- GAME CONSTANTS -------------
const BOARD_SIZE = 7;
const BATTLE_W = 10;
const BATTLE_H = 10;

const SPREAD_INTERVAL = 45;      // seconds
const INCOME_INTERVAL = 5;       // seconds
const ENEMY_BUILD_INTERVAL = 30; // seconds
const GAME_WIN_TIME = 20 * 60;   // seconds

const ECO_BONUS_PER_NODE = 1;
const ECO_NODE_CAPACITY = 80;
const ECO_DRAIN_PER_TICK = 2;

// Troops / Towers / Enemies same as client
const TROOPS = {
  rifle:   { key:"rifle",   name:"Rifleman",      emoji:"🔫", baseHp:12, dmg:3, range:3, cost:8 },
  phase:   { key:"phase",   name:"Phase Blade",   emoji:"🗡️", baseHp:18, dmg:4, range:1, cost:10 },
  bomber:  { key:"bomber",  name:"Photon Bomber", emoji:"💣", baseHp:10, dmg:3, range:3, splash:true, cost:12 },
  medic:   { key:"medic",   name:"Medic",         emoji:"🧑‍⚕️", baseHp:10, heal:3, range:2, cost:10 },
  sergeant:{ key:"sergeant",name:"Space Sergeant",emoji:"⭐", baseHp:14, dmg:2, range:2, buff:true, cost:14 },
  sniper:  { key:"sniper",  name:"Ion Sniper",    emoji:"🎯", baseHp:8,  dmg:5, range:5, cost:14 }
};

const TOWERS = {
  laser:  { key:"laser",  name:"Laser Tower",  emoji:"🔺", dmg:4, range:4, cost:14 },
  tesla:  { key:"tesla",  name:"Tesla Coil",   emoji:"⚡", dmg:3, range:3, chain:true, cost:16 },
  shield: { key:"shield", name:"Shield Tower", emoji:"🛡️", range:2, shield:true, cost:12 },
  signal: { key:"signal", name:"Signal Tower", emoji:"📡", range:2, buff:true, cost:16 }
};

const ENEMIES = {
  bug: {
    key: "bug",
    emoji: "🐜",
    baseHp: 10,
    dmg: 2,
    speed: 1,
    range: 1
  },
  spitter: {
    key: "spitter",
    emoji: "🪲",
    baseHp: 14,
    dmg: 3,
    speed: 1,
    range: 3
  },
  brute: {
    key: "brute",
    emoji: "🦂",
    baseHp: 26,
    dmg: 5,
    speed: 1,
    range: 1
  },
  behemoth: {
    key: "behemoth",
    emoji: "👾",
    baseHp: 40,
    dmg: 6,
    speed: 1,
    range: 1
  },
  hiveQueen: {
    key: "hiveQueen",
    emoji: "👑",
    baseHp: 90,
    dmg: 3,
    speed: 1,
    range: 2,
    isBoss: true
  },
  warper: {
    key: "warper",
    emoji: "🌀",
    baseHp: 75,
    dmg: 4,
    speed: 1,
    range: 3,
    isBoss: true
  }
};

// ------------- GAME STATE ON NODE -------------
/**
 * games[gameId] = {
 *   state: { timeSeconds, base, nodesState, status, winAtSeconds },
 *   players: Map<uid, { uid, name, socketId, money, incomeBonus, lastDropshipAt }>,
 *   loop: IntervalId
 * }
 */
const games = new Map();

// ------------- HELPERS -------------
function nodeKey(row, col) {
  return `${row}_${col}`;
}
function neighborsOf(row, col) {
  const dirs = [[1,0],[-1,0],[0,1],[0,-1]];
  const out = [];
  for (const [dr,dc] of dirs) {
    const nr = row + dr, nc = col + dc;
    if (nr >= 0 && nr < BOARD_SIZE && nc >= 0 && nc < BOARD_SIZE) {
      out.push(nodeKey(nr,nc));
    }
  }
  return out;
}

function gameLog(gameId, msg) {
  console.log(`[${gameId}] ${msg}`);
  io.to(gameId).emit('log', msg);
}

// Difficulty factor based on player count
function getDifficultyFactor(playerCount) {
  return 1 + (playerCount - 1) * 0.4;
}

// ------------- INITIAL STATE -------------
function createInitialState(gameId) {
  const center = Math.floor(BOARD_SIZE / 2);
  const nodesState = {};

  for (let r = 0; r < BOARD_SIZE; r++) {
    for (let c = 0; c < BOARD_SIZE; c++) {
      const id = nodeKey(r,c);
      const manhattan = Math.abs(r - center) + Math.abs(c - center);
      let owner;

      if (r === center && c === center) {
        owner = 'player';
      } else if (manhattan === 1) {
        owner = 'player';
      } else if (
        (r === 0 && (c === 1 || c === BOARD_SIZE - 2)) ||
        (r === BOARD_SIZE - 1 && (c === 1 || c === BOARD_SIZE - 2)) ||
        (c === 0 && (r === 1 || r === BOARD_SIZE - 2)) ||
        (c === BOARD_SIZE - 1 && (r === 1 || r === BOARD_SIZE - 2))
      ) {
        owner = 'enemy';
      } else {
        owner = 'neutral';
      }

      let type = 'normal';
      if (r === center && c === center) type = 'base';
      else if (manhattan === 2) type = 'eco';

      const battlefield = {
        width: BATTLE_W,
        height: BATTLE_H,
        troops: [],
        towers: [],
        aliens: [],
        spines: [],
        hitLines: []
      };

      // Base generator tower
      if (type === 'base') {
        const midY = Math.floor(BATTLE_H / 2);
        battlefield.towers.push({
          id: 'generator',
          x: 0,
          y: midY,
          kind: 'generator',
          hp: 120,
          maxHp: 120
        });
      }

      const ecoPool = (type === 'eco') ? ECO_NODE_CAPACITY : null;

      nodesState[id] = {
        id,
        row: r,
        col: c,
        owner,
        type,
        spreadChance: 0.33,
        pendingWave: false,
        ecoPool,
        battlefield
      };
    }
  }

  return {
    gameId,
    timeSeconds: 0,
    status: 'running',
    winAtSeconds: GAME_WIN_TIME,
    base: {
      hp: 120,
      maxHp: 120,
      level: 1,
      incomePerPlayer: 7,
      upgradeCost: 60
    },
    nodesState
  };
}

function createGame(gameId) {
  const state = createInitialState(gameId);
  const players = new Map();
  const loop = setInterval(() => tickGame(gameId), 1000);
  const game = { state, players, loop };
  games.set(gameId, game);
  return game;
}

function destroyGame(gameId) {
  const game = games.get(gameId);
  if (!game) return;
  clearInterval(game.loop);
  games.delete(gameId);
  console.log('Destroyed game', gameId);
}

function serializeGame(game) {
  return {
    ...game.state,
    players: Array.from(game.players.values()).map(p => ({
      uid: p.uid,
      name: p.name,
      money: p.money || 0,
      incomeBonus: p.incomeBonus || 0,
      lastDropshipAt: p.lastDropshipAt || 0
    }))
  };
}

// ------------- GAME TICK -------------
function tickGame(gameId) {
  const game = games.get(gameId);
  if (!game) return;

  const state = game.state;
  if (state.status !== 'running') return;

  state.timeSeconds = (state.timeSeconds || 0) + 1;
  const t = state.timeSeconds;

  // Lose / win checks are handled in simulateBattles,
  // but we can early bail on exceeded win time:
  if (t >= state.winAtSeconds && state.status === 'running') {
    state.status = 'won';
    gameLog(gameId, '✅ You survived the full duration! Victory.');
  }

  // Periodic events
  if (t % INCOME_INTERVAL === 0) {
    applyIncomeTick(gameId, state, game.players);
  }
  if (t % SPREAD_INTERVAL === 0) {
    applySpreadTick(gameId, state);
  }
  if (t % ENEMY_BUILD_INTERVAL === 0) {
    growEnemyForces(gameId, state, game.players.size || 1);
  }

  // Simulate all battlefields
  simulateBattles(gameId, state);

  // Broadcast state
  io.to(gameId).emit('state', serializeGame(game));
}

// ------------- CORE LOGIC: ECO INCOME -------------
function applyIncomeTick(gameId, state, players) {
  const base = state.base || { incomePerPlayer: 0 };
  const baseIncome = base.incomePerPlayer || 0;

  let ecoCount = 0;
  const depletedEcoKeys = [];

  const nodesState = state.nodesState;

  // Eco nodes drain while controlled; track which run out
  for (const key in nodesState) {
    const node = nodesState[key];
    if (!node) continue;
    if (node.type !== 'eco') continue;

    if (node.owner === 'player') {
      const currentPool =
        typeof node.ecoPool === 'number' ? node.ecoPool : ECO_NODE_CAPACITY;

      if (currentPool > 0) {
        ecoCount++;
      }

      const newPool = currentPool - ECO_DRAIN_PER_TICK;
      node.ecoPool = Math.max(0, newPool);

      if (newPool <= 0) {
        node.type = 'normal';
        depletedEcoKeys.push(key);
      }
    } else {
      // hold pool value, no drain if not player-owned
      node.ecoPool =
        typeof node.ecoPool === 'number' ? node.ecoPool : ECO_NODE_CAPACITY;
    }
  }

  const ecoBonus = ecoCount * ECO_BONUS_PER_NODE;

  const playerCount = players.size || 1;
  const difficultyFactor = getDifficultyFactor(playerCount);
  const rawIncome = baseIncome + ecoBonus;
  const incomePerPlayer = Math.max(
    0,
    Math.round(rawIncome / difficultyFactor)
  );

  if (incomePerPlayer > 0) {
    for (const [uid, p] of players.entries()) {
      const extra = p.incomeBonus || 0;
      p.money = (p.money || 0) + incomePerPlayer + extra;
    }
  }

  // Relocate any eco nodes that ran out to alien territory
  if (depletedEcoKeys.length) {
    const enemyCandidates = Object.keys(nodesState).filter((k) => {
      const n = nodesState[k];
      if (!n) return false;
      return n.owner === 'enemy' && n.type !== 'base' && n.type !== 'eco';
    });

    for (const spentKey of depletedEcoKeys) {
      if (!enemyCandidates.length) break;

      const idx = Math.floor(Math.random() * enemyCandidates.length);
      const newKey = enemyCandidates.splice(idx, 1)[0];
      const newNode = nodesState[newKey];
      const oldNode = nodesState[spentKey];

      if (!newNode) continue;

      newNode.type = 'eco';
      newNode.ecoPool = ECO_NODE_CAPACITY;

      if (oldNode) {
        gameLog(
          gameId,
          `💰 Eco node at ${oldNode.row + 1}-${oldNode.col + 1} is exhausted. New rich sector appears in alien territory at ${newNode.row + 1}-${newNode.col + 1}.`
        );
      } else {
        gameLog(
          gameId,
          '💰 An eco node has been exhausted and relocated to a new alien-controlled sector.'
        );
      }
    }
  }
}

// ------------- CORE LOGIC: SPREAD / PENDING WAVES -------------
function applySpreadTick(gameId, state) {
  const nodesState = state.nodesState;

  for (const key in nodesState) {
    const node = nodesState[key];
    if (!node) continue;
    if (node.owner === 'enemy') continue;

    const { row, col } = node;
    const neighbors = neighborsOf(row, col);
    const hasEnemyNeighbor = neighbors.some((nk) => {
      const nn = nodesState[nk];
      return nn && nn.owner === 'enemy';
    });
    if (!hasEnemyNeighbor) continue;

    let chance =
      typeof node.spreadChance === 'number' ? node.spreadChance : 0.33;
    const roll = Math.random();

    if (roll < chance) {
      node.spreadChance = 0.33;
      node.pendingWave = true;
      gameLog(
        gameId,
        `Alien pressure builds in node ${row + 1}-${col + 1} — wave incoming.`
      );
    } else {
      chance = Math.min(0.95, chance + 0.15);
      node.spreadChance = chance;
    }
  }
}

// ------------- ENEMY BUILDUP -------------
function spawnSpine(bf, x, y, variant) {
  const v = variant || 'piercer';
  const maxHp = v === 'piercer' ? 26 : 20;
  const dmg = v === 'piercer' ? 5 : 1;
  const poisonTicks = v === 'piercer' ? 6 : 2;
  const range = 3;

  bf.spines = bf.spines || [];
  bf.spines.push({
    id: `sp_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`,
    x,
    y,
    hp: maxHp,
    maxHp,
    range,
    dmg,
    poisonTicks,
    cooldown: 0,
    cooldownMax: 3,
    variant: v
  });
}

function ensureSpinesForEnemyNode(node, bf) {
  bf.spines = bf.spines || [];
  if (bf.spines.length > 0) return false;

  const mid = Math.floor(BATTLE_H / 2);
  const x = BATTLE_W - 3;
  const positions = [
    { x, y: mid - 2 },
    { x, y: mid },
    { x, y: mid + 2 }
  ].filter((p) => p.y >= 1 && p.y < BATTLE_H - 1);

  positions.forEach((p, i) => {
    const variant = i === 1 ? 'shredder' : 'piercer';
    spawnSpine(bf, p.x, p.y, variant);
  });

  return true;
}

function pickPrimaryAlienType(minutes, tier) {
  if (tier === 1) {
    if (minutes < 2) return 'bug';
    if (minutes < 4) return Math.random() < 0.6 ? 'bug' : 'spitter';
    return 'spitter';
  }
  if (tier === 2) {
    if (minutes < 6) return 'spitter';
    return Math.random() < 0.6 ? 'spitter' : 'brute';
  }
  if (minutes < 10) return 'brute';
  return Math.random() < 0.6 ? 'brute' : 'behemoth';
}

function pickSecondaryAlienType(minutes) {
  if (minutes < 3) return 'bug';
  if (minutes < 6) return 'spitter';
  if (minutes < 10) return 'brute';
  return 'behemoth';
}

function spawnAlienWaveForNode(node, bf, state) {
  const aliens = bf.aliens || [];
  const nowSec = state.timeSeconds || 0;
  const minutes = nowSec / 60;

  let baseTier =
    node.alienTier ?? node.threatTier ?? node.tier ?? 1;

  if (minutes > 8) baseTier += 3;
  else if (minutes > 5) baseTier += 2;
  else if (minutes > 2.5) baseTier += 1;

  const tier = Math.max(1, Math.min(5, baseTier));

  let theme;
  const r = Math.random();
  if (tier <= 2) {
    if (r < 0.6) theme = 'bugs';
    else theme = 'spitters';
  } else if (tier <= 4) {
    if (r < 0.35) theme = 'bugs';
    else if (r < 0.7) theme = 'spitters';
    else theme = 'brutes';
  } else {
    if (r < 0.25) theme = 'bugs';
    else if (r < 0.5) theme = 'spitters';
    else if (r < 0.8) theme = 'brutes';
    else theme = 'heavies';
  }

  let baseCount = 4 + tier * 2;
  baseCount += Math.floor((Math.random() - 0.5) * 3);
  baseCount = Math.max(3, baseCount);

  function spawnAlien(x, y, type) {
    const def = ENEMIES[type] || ENEMIES.bug;
    const hp = def.baseHp || 10;
    aliens.push({
      id: 'alien_' + type + '_' + Date.now() + '_' + Math.random().toString(16).slice(2, 6),
      x,
      y,
      type,
      hp,
      maxHp: hp
    });
  }

  let primaryType = 'bug';
  let supportTypes = [];

  if (theme === 'bugs') {
    primaryType = 'bug';
    supportTypes = ['spitter'];
  } else if (theme === 'spitters') {
    primaryType = 'spitter';
    supportTypes = ['bug'];
  } else if (theme === 'brutes') {
    primaryType = 'brute';
    supportTypes = ['bug', 'spitter'];
  } else if (theme === 'heavies') {
    primaryType = 'behemoth';
    supportTypes = ['brute', 'spitter'];
  }

  const entryX = BATTLE_W - 1;
  const lanes = [
    Math.floor(BATTLE_H * 0.2),
    Math.floor(BATTLE_H * 0.5),
    Math.floor(BATTLE_H * 0.8)
  ].map((y) => Math.max(0, Math.min(BATTLE_H - 1, y)));

  for (let i = 0; i < baseCount; i++) {
    const laneY = lanes[i % lanes.length];
    const jitterY = laneY + (Math.random() < 0.5 ? 0 : (Math.random() < 0.5 ? 1 : -1));
    const y = Math.max(0, Math.min(BATTLE_H - 1, jitterY));

    let type = primaryType;
    const rr = Math.random();

    if (rr < 0.8) {
      type = primaryType;
    } else {
      if (primaryType === 'bug') {
        type = Math.random() < 0.7 ? 'spitter' : (supportTypes[0] || 'spitter');
      } else if (primaryType === 'spitter') {
        type = Math.random() < 0.7 ? 'bug' : (supportTypes[0] || 'bug');
      } else if (primaryType === 'brute') {
        type = Math.random() < 0.7 ? 'spitter' : (supportTypes[0] || 'spitter');
      } else {
        type = Math.random() < 0.6 ? 'brute' : (supportTypes[0] || 'brute');
      }
    }

    spawnAlien(entryX, y, type);
  }

  let hiveChance = 0;
  let warpChance = 0;

  if (tier >= 3 && tier <= 4) {
    hiveChance = 0.18 + 0.04 * (tier - 3);
  } else if (tier >= 5) {
    hiveChance = 0.28;
    warpChance = 0.22;
  }

  hiveChance += Math.min(0.12, minutes * 0.01);
  warpChance += Math.min(0.10, Math.max(0, minutes - 6) * 0.01);

  function findBossLaneY() {
    return lanes[1] ?? lanes[0] ?? Math.floor(BATTLE_H / 2);
  }

  if (hiveChance > 0 && Math.random() < hiveChance) {
    const bossY = findBossLaneY();
    const bossX = BATTLE_W - 2;
    spawnAlien(bossX, bossY, 'hiveQueen');
  }

  if (warpChance > 0 && Math.random() < warpChance) {
    const bossY = findBossLaneY();
    const bossX = BATTLE_W - 3;
    spawnAlien(bossX, bossY, 'warper');
  }

  bf.aliens = aliens;
}

function growEnemyForces(gameId, state, playerCount) {
  const timeSeconds = state.timeSeconds || 0;
  const minutes = timeSeconds / 60;

  const difficultyFactor = getDifficultyFactor(playerCount);
  const maxAliensBase = (4 + Math.floor(minutes * 3)) * difficultyFactor;
  const maxSpinesBase = (2 + Math.floor(minutes / 4)) * difficultyFactor;

  const nodesState = state.nodesState;

  for (const key in nodesState) {
    const node = nodesState[key];
    if (!node) continue;
    if (node.owner !== 'enemy') continue;

    const bf = node.battlefield || {
      width: BATTLE_W,
      height: BATTLE_H,
      troops: [],
      towers: [],
      aliens: [],
      spines: [],
      hitLines: []
    };
    bf.aliens = bf.aliens || [];
    bf.spines = bf.spines || [];

    const maxAliens = Math.min(40, Math.round(maxAliensBase));
    const maxSpines = Math.min(8, Math.round(maxSpinesBase));
    let changed = false;

    if (bf.aliens.length < maxAliens) {
      const deficit = maxAliens - bf.aliens.length;
      const baseSpawn = 4 + Math.floor(minutes);
      const spawnCount = Math.min(
        Math.round(baseSpawn * difficultyFactor),
        deficit
      );

      const primaryType = pickPrimaryAlienType(
        minutes,
        minutes >= 10 ? 3 : minutes >= 5 ? 2 : 1
      );
      const secondaryType = pickSecondaryAlienType(minutes);

      for (let i = 0; i < spawnCount; i++) {
        const row = 1 + (i % (BATTLE_H - 2));
        const x = BATTLE_W - 1;

        let typeKey;
        const r = Math.random();

        if (r < 0.8) {
          typeKey = primaryType;
        } else {
          if (primaryType === 'bug') {
            typeKey = Math.random() < 0.7 ? 'spitter' : secondaryType;
          } else if (primaryType === 'spitter') {
            typeKey = Math.random() < 0.7 ? 'bug' : secondaryType;
          } else if (primaryType === 'brute') {
            typeKey = Math.random() < 0.7 ? 'spitter' : secondaryType;
          } else {
            typeKey = Math.random() < 0.6 ? 'brute' : secondaryType;
          }
        }

        const def = ENEMIES[typeKey] || ENEMIES.bug;
        bf.aliens.push({
          id: 'a_' + Date.now() + '_' + Math.random().toString(16).slice(2, 6),
          x,
          y: row,
          type: typeKey,
          hp: def.baseHp,
          maxHp: def.baseHp
        });
      }
      changed = true;
    }

    if (bf.spines.length < maxSpines) {
      const needed = maxSpines - bf.spines.length;
      for (let i = 0; i < needed; i++) {
        const x = BATTLE_W - 3 - (i % 2);
        const y = 1 + Math.floor(Math.random() * (BATTLE_H - 2));
        const variant = Math.random() < 0.5 ? 'piercer' : 'shredder';
        spawnSpine(bf, x, y, variant);
      }
      changed = true;
    }

    if (changed) {
      node.battlefield = bf;
    }
  }
}

// ------------- BATTLE SIM STEP HELPERS -------------
function troopVsAlienMultiplier(troopKind, alienType) {
  if ((troopKind === 'rifle' || troopKind === 'bomber') && alienType === 'bug') {
    return 1.75;
  }
  if (troopKind === 'phase' && alienType === 'spitter') {
    return 1.75;
  }
  if (troopKind === 'sniper' && alienType === 'brute') {
    return 1.75;
  }
  if (troopKind === 'sniper' && alienType === 'behemoth') {
    return 2.0;
  }
  return 1;
}

function simulateBattlefieldStep(node, bf, state) {
  const aliens = bf.aliens || [];
  const troops = bf.troops || [];
  const towers = bf.towers || [];
  const spines = bf.spines || [];
  let changed = false;

  const hasPlayerUnits =
    troops.some((t) => t.hp > 0) ||
    towers.some((tw) => tw.hp > 0);

  const occupiedAlienPositions = new Set();
  for (const a of aliens) {
    if (a.hp > 0) {
      occupiedAlienPositions.add(`${a.x}_${a.y}`);
    }
  }

  function inBounds(x, y) {
    return x >= 0 && x < BATTLE_W && y >= 0 && y < BATTLE_H;
  }
  function manhattan(x1, y1, x2, y2) {
    return Math.abs(x1 - x2) + Math.abs(y1 - y2);
  }
  function isOccupiedTile(x, y) {
    if (!inBounds(x, y)) return true;
    for (const t of troops) if (t.hp > 0 && t.x === x && t.y === y) return true;
    for (const tw of towers) if (tw.hp > 0 && tw.x === x && tw.y === y) return true;
    for (const a of aliens) if (a.hp > 0 && a.x === x && a.y === y) return true;
    for (const s of spines) if (s.hp > 0 && s.x === x && s.y === y) return true;
    return false;
  }

  // Poison tick on troops
  for (const t of troops) {
    if (t.poisonTicks && t.poisonTicks > 0) {
      t.hp -= 1;
      t.poisonTicks -= 1;
      changed = true;
    }
  }

  // Build shield and buff maps
  const shieldMap = {};
  const damageBuffMap = {};

  for (const tw of towers) {
    if (tw.hp <= 0) continue;
    const def = TOWERS[tw.kind];
    if (!def) continue;

    if (def.shield) {
      const range = def.range || 2;
      for (const t of troops) {
        if (t.hp <= 0) continue;
        const d = manhattan(tw.x, tw.y, t.x, t.y);
        if (d <= range) shieldMap[t.id] = 0.5;
      }
      for (const tw2 of towers) {
        if (tw2.hp <= 0 || tw2.id === tw.id) continue;
        const d = manhattan(tw.x, tw.y, tw2.x, tw2.y);
        if (d <= range) shieldMap[tw2.id] = 0.5;
      }
    }
    if (def.buff) {
      const range = def.range || 2;
      for (const t of troops) {
        if (t.hp <= 0) continue;
        const d = manhattan(tw.x, tw.y, t.x, t.y);
        if (d <= range) damageBuffMap[t.id] = 1.5;
      }
      for (const tw2 of towers) {
        if (tw2.hp <= 0 || tw2.id === tw.id) continue;
        const d = manhattan(tw.x, tw.y, tw2.x, tw2.y);
        if (d <= range) damageBuffMap[tw2.id] = 1.5;
      }
    }
  }

  // Also check sergeant buff
  for (const t of troops) {
    if (t.hp <= 0) continue;
    const def = TROOPS[t.kind];
    if (def && def.buff) {
      const range = def.range || 2;
      for (const ally of troops) {
        if (ally.hp <= 0 || ally.id === t.id) continue;
        const d = manhattan(t.x, t.y, ally.x, ally.y);
        if (d <= range) damageBuffMap[ally.id] = (damageBuffMap[ally.id] || 1) * 1.3;
      }
    }
  }

  // Alien movement
  for (const a of aliens) {
    if (a.hp <= 0) continue;
    const def = ENEMIES[a.type] || ENEMIES.bug;
    const speed = def.speed || 1;
    const attackRange = def.range || 1;

    // Boss cooldowns
    if (a.summonCooldown > 0) a.summonCooldown--;
    if (a.warpCooldown > 0) a.warpCooldown--;

    // Find nearest target
    let bestTarget = null, bestD = 999;
    for (const t of troops) {
      if (t.hp <= 0) continue;
      const d = manhattan(a.x, a.y, t.x, t.y);
      if (d < bestD) { bestD = d; bestTarget = t; }
    }
    for (const tw of towers) {
      if (tw.hp <= 0) continue;
      const d = manhattan(a.x, a.y, tw.x, tw.y);
      if (d < bestD) { bestD = d; bestTarget = tw; }
    }

    if (bestTarget && bestD > attackRange) {
      // Move towards target
      const dx = Math.sign(bestTarget.x - a.x);
      const dy = Math.sign(bestTarget.y - a.y);

      let moved = false;
      const tries = [[dx, 0], [0, dy], [dx, dy]];
      for (const [mx, my] of tries) {
        if (mx === 0 && my === 0) continue;
        const nx = a.x + mx;
        const ny = a.y + my;
        if (!isOccupiedTile(nx, ny)) {
          a.x = nx;
          a.y = ny;
          moved = true;
          changed = true;
          break;
        }
      }
    }

    // Boss abilities
    if (a.type === 'hiveQueen') {
      if ((a.summonCooldown || 0) <= 0) {
        const dirs = [
          [1,0],[-1,0],[0,1],[0,-1],
          [1,1],[1,-1],[-1,1],[-1,-1]
        ];
        let spawned = 0;
        for (const [dx,dy] of dirs) {
          if (spawned >= 3) break;
          const sx = a.x + dx;
          const sy = a.y + dy;
          if (!inBounds(sx,sy)) continue;
          if (isOccupiedTile(sx,sy)) continue;

          const bugDef = ENEMIES.bug;
          aliens.push({
            id: 'boss_bug_' + Date.now() + '_' + Math.random().toString(16).slice(2,6),
            x: sx,
            y: sy,
            type: 'bug',
            hp: bugDef.baseHp,
            maxHp: bugDef.baseHp
          });
          spawned++;
          changed = true;
        }
        if (spawned > 0) {
          a.summonCooldown = 6;
        }
      }
    }

    if (a.type === 'warper') {
      if (a.warpCooldown <= 0 && towers.some((tw) => tw.hp > 0)) {
        let bestTower = null, bestD = Infinity;
        for (const tw of towers) {
          if (tw.hp <= 0) continue;
          const d = manhattan(a.x, a.y, tw.x, tw.y);
          if (d < bestD) { bestD = d; bestTower = tw; }
        }
        if (bestTower) {
          const dirs = [
            [1,0],[-1,0],[0,1],[0,-1],
            [1,1],[1,-1],[-1,1],[-1,-1]
          ];
          let dest = null;
          for (const [dx,dy] of dirs) {
            const tx = bestTower.x + dx;
            const ty = bestTower.y + dy;
            if (!inBounds(tx,ty)) continue;
            if (isOccupiedTile(tx,ty)) continue;
            dest = {x:tx, y:ty};
            break;
          }
          if (dest) {
            a.x = dest.x;
            a.y = dest.y;
            a.warpCooldown = 8;
            changed = true;
          }
        }
      }
    }
  }

  // Troop movement - troops move towards enemies until in range
  for (const t of troops) {
    if (t.hp <= 0) continue;
    const def = TROOPS[t.kind] || TROOPS.rifle;
    const range = def.range || 2;

    if (t.kind === 'medic') {
      // Medics move towards injured allies
      let bestAlly = null, bestD = 999, bestPct = 1.1;
      for (const ally of troops) {
        if (ally.hp <= 0 || ally.id === t.id) continue;
        const pct = ally.hp / ally.maxHp;
        if (pct < 1) {
          const d = manhattan(t.x, t.y, ally.x, ally.y);
          // Prioritize most injured, then closest
          if (pct < bestPct || (pct === bestPct && d < bestD)) {
            bestPct = pct;
            bestD = d;
            bestAlly = ally;
          }
        }
      }

      // Move towards injured ally if not in healing range
      if (bestAlly && bestD > (def.range || 2)) {
        const dx = Math.sign(bestAlly.x - t.x);
        const dy = Math.sign(bestAlly.y - t.y);

        const tries = [[dx, 0], [0, dy], [dx, dy]];
        for (const [mx, my] of tries) {
          if (mx === 0 && my === 0) continue;
          const nx = t.x + mx;
          const ny = t.y + my;
          if (!isOccupiedTile(nx, ny)) {
            t.x = nx;
            t.y = ny;
            changed = true;
            break;
          }
        }
      }
    } else {
      // Combat troops move towards nearest enemy (alien or spine)
      let bestEnemy = null, bestD = 999;
      for (const a of aliens) {
        if (a.hp <= 0) continue;
        const d = manhattan(t.x, t.y, a.x, a.y);
        if (d < bestD) { bestD = d; bestEnemy = a; }
      }
      for (const s of spines) {
        if (s.hp <= 0) continue;
        const d = manhattan(t.x, t.y, s.x, s.y);
        if (d < bestD) { bestD = d; bestEnemy = s; }
      }

      // Move towards enemy if not in attack range
      if (bestEnemy && bestD > range) {
        const dx = Math.sign(bestEnemy.x - t.x);
        const dy = Math.sign(bestEnemy.y - t.y);

        const tries = [[dx, 0], [0, dy], [dx, dy]];
        for (const [mx, my] of tries) {
          if (mx === 0 && my === 0) continue;
          const nx = t.x + mx;
          const ny = t.y + my;
          if (!isOccupiedTile(nx, ny)) {
            t.x = nx;
            t.y = ny;
            changed = true;
            break;
          }
        }
      }
    }
  }

  // Hitlines: we just store them; client will render
  bf.hitLines = [];
  function addHitLine(kind, x1,y1,x2,y2) {
    bf.hitLines.push({ kind, x1,y1,x2,y2 });
  }

  function nearestEnemyFrom(x, y, range) {
    let best = null, bestD = 999, bestType = null;
    for (const a of aliens) {
      if (a.hp <= 0) continue;
      const d = Math.abs(a.x - x) + Math.abs(a.y - y);
      if (d <= range && d < bestD) {
        best = a; bestD = d; bestType = 'alien';
      }
    }
    for (const s of spines) {
      if (s.hp <= 0) continue;
      const d = Math.abs(s.x - x) + Math.abs(s.y - y);
      if (d <= range && d < bestD) {
        best = s; bestD = d; bestType = 'spine';
      }
    }
    return best ? { type: bestType, enemy: best } : null;
  }

  // Troops actions
  for (const t of troops) {
    if (t.hp <= 0) continue;
    const def = TROOPS[t.kind] || TROOPS.rifle;

    if (t.kind === 'medic') {
      let best = null, bestPct = 1.1;
      for (const ally of troops) {
        if (ally.hp <= 0) continue;
        const d = Math.abs(ally.x - t.x) + Math.abs(ally.y - t.y);
        if (d <= (def.range || 2)) {
          const pct = ally.hp / ally.maxHp;
          if (pct < bestPct) {
            bestPct = pct;
            best = ally;
          }
        }
      }
      if (best && best.hp < best.maxHp) {
        best.hp = Math.min(best.maxHp, best.hp + (def.heal || 3));
        changed = true;
      }
    } else {
      const range = def.range || 2;
      const targetInfo = nearestEnemyFrom(t.x, t.y, range);
      if (targetInfo) {
        let dmg = def.dmg || 2;
        const buff = damageBuffMap[t.id] || 1;
        dmg = Math.round(dmg * buff);

        if (def.splash) {
          const tx = targetInfo.enemy.x;
          const ty = targetInfo.enemy.y;
          addHitLine('player', t.x, t.y, tx, ty);
          for (const a of aliens) {
            const d = Math.abs(a.x - tx) + Math.abs(a.y - ty);
            if (d <= 1) {
              const mult = troopVsAlienMultiplier(t.kind, a.type || 'bug');
              const finalDmg = Math.round(dmg * mult);
              a.hp -= finalDmg;
              changed = true;
            }
          }
          for (const s of spines) {
            const d = Math.abs(s.x - tx) + Math.abs(s.y - ty);
            if (d <= 1) {
              s.hp -= dmg;
              changed = true;
            }
          }
        } else {
          let finalDmg = dmg;
          if (targetInfo.type === 'alien') {
            const alienType = targetInfo.enemy.type || 'bug';
            const mult = troopVsAlienMultiplier(t.kind, alienType);
            finalDmg = Math.round(dmg * mult);
          }
          addHitLine('player', t.x, t.y, targetInfo.enemy.x, targetInfo.enemy.y);
          targetInfo.enemy.hp -= finalDmg;
          changed = true;
        }
      }
    }
  }

  // Tower attacks
  for (const tw of towers) {
    if (tw.kind === 'generator') continue;
    const def = TOWERS[tw.kind] || TOWERS.laser;
    if (def.shield || def.buff) continue;

    const range = def.range || 3;
    const targetInfo = nearestEnemyFrom(tw.x, tw.y, range);
    if (!targetInfo) continue;

    let dmg = def.dmg || 2;
    const buff = damageBuffMap[tw.id] || 1;
    dmg = Math.round(dmg * buff);

    if (def.chain) {
      const tx = targetInfo.enemy.x;
      const ty = targetInfo.enemy.y;
      const chainTargets = [];
      for (const a of aliens) {
        if (a.hp <= 0) continue;
        const d = Math.abs(a.x - tx) + Math.abs(a.y - ty);
        if (d <= 1) chainTargets.push({ type: 'alien', ref: a });
      }
      for (const s of spines) {
        if (s.hp <= 0) continue;
        const d = Math.abs(s.x - tx) + Math.abs(s.y - ty);
        if (d <= 1) chainTargets.push({ type: 'spine', ref: s });
      }
      if (chainTargets.length === 0) {
        chainTargets.push({ type: targetInfo.type, ref: targetInfo.enemy });
      }
      chainTargets.slice(0, 3).forEach((tObj) => {
        addHitLine('player', tw.x, tw.y, tObj.ref.x, tObj.ref.y);
        tObj.ref.hp -= dmg;
      });
      changed = true;
    } else {
      addHitLine('player', tw.x, tw.y, targetInfo.enemy.x, targetInfo.enemy.y);
      targetInfo.enemy.hp -= dmg;
      changed = true;
    }
  }

  // Spine attacks
  for (const s of spines) {
    if (s.hp <= 0) continue;
    s.cooldown = (s.cooldown || 0) - 1;
    if (s.cooldown > 0) continue;

    const range = s.range || 3;
    const variant = s.variant || 'piercer';

    const inRangeTroops = [];
    const inRangeTowers = [];
    for (const t of troops) {
      if (t.hp <= 0) continue;
      const d = Math.abs(t.x - s.x) + Math.abs(t.y - s.y);
      if (d <= range) inRangeTroops.push(t);
    }
    for (const tw of towers) {
      if (tw.hp <= 0) continue;
      const d = Math.abs(tw.x - s.x) + Math.abs(tw.y - s.y);
      if (d <= range) inRangeTowers.push(tw);
    }

    if (variant === 'shredder') {
      let swung = false;
      const dmgLow = s.dmg || 1;
      for (const t of inRangeTroops) {
        let finalDmg = dmgLow;
        const shield = shieldMap[t.id] || 1;
        finalDmg = Math.max(1, Math.round(finalDmg * shield));
        t.hp -= finalDmg;
        addHitLine('enemy', s.x, s.y, t.x, t.y);
        swung = true;
      }
      for (const tw of inRangeTowers) {
        let finalDmg = dmgLow;
        const shield = shieldMap[tw.id] || 1;
        finalDmg = Math.max(1, Math.round(finalDmg * shield));
        tw.hp -= finalDmg;
        addHitLine('enemy', s.x, s.y, tw.x, tw.y);
        swung = true;
      }
      if (swung) {
        s.cooldown = s.cooldownMax || 3;
        changed = true;
      }
    } else {
      let best = null, bestD = 999, bestType = null;
      for (const t of inRangeTroops) {
        const d = Math.abs(t.x - s.x) + Math.abs(t.y - s.y);
        if (d < bestD) { best = t; bestD = d; bestType = 'troop'; }
      }
      for (const tw of inRangeTowers) {
        const d = Math.abs(tw.x - s.x) + Math.abs(tw.y - s.y);
        if (d < bestD) { best = tw; bestD = d; bestType = 'tower'; }
      }
      if (best) {
        let finalDmg = s.dmg || 3;
        const shield = shieldMap[best.id] || 1;
        finalDmg = Math.max(1, Math.round(finalDmg * shield));
        best.hp -= finalDmg;
        if (bestType === 'troop') {
          best.poisonTicks = Math.max(best.poisonTicks || 0, s.poisonTicks || 5);
        }
        addHitLine('enemy', s.x, s.y, best.x, best.y);
        s.cooldown = s.cooldownMax || 3;
        changed = true;
      }
    }
  }

  // Alien attacks
  for (const a of aliens) {
    if (a.hp <= 0) continue;
    const def = ENEMIES[a.type] || ENEMIES.bug;
    const dmg = def.dmg || 2;
    const attackRange = def.range || 1;

    let bestEntity = null, bestD = attackRange + 1, bestType = null;

    for (const t of troops) {
      if (t.hp <= 0) continue;
      const d = Math.abs(t.x - a.x) + Math.abs(t.y - a.y);
      if (d <= attackRange && d < bestD) {
        bestEntity = t; bestD = d; bestType = 'troop';
      }
    }
    for (const tw of towers) {
      if (tw.hp <= 0) continue;
      const d = Math.abs(tw.x - a.x) + Math.abs(tw.y - a.y);
      if (d <= attackRange && d < bestD) {
        bestEntity = tw; bestD = d; bestType = 'tower';
      }
    }

    if (bestEntity) {
      const shield = shieldMap[bestEntity.id] || 1;
      let finalDmg = Math.max(1, Math.round(dmg * shield));
      bestEntity.hp -= finalDmg;
      addHitLine('enemy', a.x, a.y, bestEntity.x, bestEntity.y);
      changed = true;
    }
  }

  // Cleanup deaths
  for (const a of aliens) { if (a.hp <= 0) a.dead = true; }
  bf.aliens = aliens.filter((a) => !a.dead);

  for (const s of spines) { if (s.hp <= 0) s.dead = true; }
  bf.spines = spines.filter((s) => !s.dead);

  for (const t of troops) { if (t.hp <= 0) t.dead = true; }
  bf.troops = troops.filter((t) => !t.dead);

  for (const tw of towers) { if (tw.hp <= 0) tw.dead = true; }
  bf.towers = towers.filter((tw) => !tw.dead);

  return { changed };
}

// ------------- SIMULATE ALL NODES -------------
function simulateBattles(gameId, state) {
  const nodesState = state.nodesState;
  let baseGeneratorHp = null;

  for (const key in nodesState) {
    const node = nodesState[key];
    if (!node) continue;

    let changed = false;
    const bf = node.battlefield || {
      width: BATTLE_W,
      height: BATTLE_H,
      troops: [],
      towers: [],
      aliens: [],
      spines: [],
      hitLines: []
    };

    bf.hitLines = [];

    if (node.owner === 'enemy') {
      if (ensureSpinesForEnemyNode(node, bf)) {
        changed = true;
      }
    }

    if (node.pendingWave) {
      spawnAlienWaveForNode(node, bf, state);
      node.pendingWave = false;
      changed = true;
    }

    const hadStuff =
      (bf.troops && bf.troops.length) ||
      (bf.towers && bf.towers.length) ||
      (bf.aliens && bf.aliens.length) ||
      (bf.spines && bf.spines.length);

    if (hadStuff) {
      const result = simulateBattlefieldStep(node, bf, state);
      if (result.changed) changed = true;
    }

    const hasPlayerStuff =
      (bf.troops && bf.troops.length) ||
      (bf.towers && bf.towers.length);
    const hasEnemyStuff =
      (bf.aliens && bf.aliens.length) ||
      (bf.spines && bf.spines.length);

    let newOwner;
    if (hasPlayerStuff && !hasEnemyStuff) newOwner = 'player';
    else if (!hasPlayerStuff && hasEnemyStuff) newOwner = 'enemy';
    else if (hasPlayerStuff && hasEnemyStuff) newOwner = 'contested';
    else newOwner = 'neutral';

    if (newOwner !== node.owner) {
      node.owner = newOwner;
      changed = true;
    }

    if (node.type === 'base') {
      const gen = (bf.towers || []).find((tw) => tw.kind === 'generator');
      baseGeneratorHp = gen ? gen.hp : 0;
    }

    if (changed) {
      node.battlefield = bf;
    }
  }

  if (baseGeneratorHp != null) {
    const newHp = Math.max(0, Math.round(baseGeneratorHp));
    state.base.hp = newHp;
    if (newHp <= 0 && state.status !== 'lost') {
      state.status = 'lost';
      gameLog(gameId, '⚠ Generator destroyed. Game over for all players.');
    }
  }
}

// ------------- CAN BUILD IN NODE? -------------
function canBuildInNode(state, nodeId) {
  const node = state.nodesState[nodeId];
  if (!node) return false;
  if (node.owner === 'player') return true;

  const { row, col } = node;
  const neigh = neighborsOf(row, col);
  for (const nk of neigh) {
    const n = state.nodesState[nk];
    if (n && n.owner === 'player') return true;
  }
  return false;
}

// ------------- PLAYER ACTIONS -------------
function handleBuildAction(gameId, game, uid, action) {
  const { nodeId, x, y, category, unitType } = action;
  const state = game.state;
  const player = game.players.get(uid);
  if (!player) return;

  const node = state.nodesState[nodeId];
  if (!node) {
    gameLog(gameId, 'Build failed: node missing.');
    return;
  }

  if (!canBuildInNode(state, nodeId)) {
    gameLog(gameId, 'Build failed: cannot build in this node.');
    return;
  }
  if (x > 2) {
    gameLog(gameId, 'Build failed: must build in first 3 columns.');
    return;
  }

  const def =
    category === 'troop'
      ? TROOPS[unitType]
      : TOWERS[unitType];

  if (!def) {
    gameLog(gameId, 'Build failed: unknown unit type.');
    return;
  }

  const cost = def.cost || 0;
  if ((player.money || 0) < cost) {
    gameLog(gameId, 'Build failed: not enough credits.');
    return;
  }

  const bf = node.battlefield || {
    width: BATTLE_W,
    height: BATTLE_H,
    troops: [],
    towers: [],
    aliens: [],
    spines: [],
    hitLines: []
  };

  const occupied =
    (bf.troops || []).some((t) => t.x === x && t.y === y) ||
    (bf.towers || []).some((tw) => tw.x === x && tw.y === y) ||
    (bf.spines || []).some((s) => s.x === x && s.y === y);

  if (occupied) {
    gameLog(gameId, 'Build failed: tile occupied.');
    return;
  }

  if (category === 'troop') {
    bf.troops = bf.troops || [];
    bf.troops.push({
      id: 't_' + Date.now() + '_' + Math.random().toString(16).slice(2, 6),
      x,
      y,
      kind: def.key,
      hp: def.baseHp,
      maxHp: def.baseHp
    });
  } else {
    bf.towers = bf.towers || [];
    bf.towers.push({
      id: 'tw_' + Date.now() + '_' + Math.random().toString(16).slice(2, 6),
      x,
      y,
      kind: def.key,
      hp: 18,
      maxHp: 18
    });
  }

  node.battlefield = bf;
  player.money = (player.money || 0) - cost;

  gameLog(gameId, `Built ${def.name} at (${x + 1},${y + 1}).`);
}

function handleUpgradeBase(gameId, game, uid) {
  const state = game.state;
  const player = game.players.get(uid);
  if (!player) return;

  const base = state.base || { level:1, upgradeCost:60, incomePerPlayer:7 };
  const cost = base.upgradeCost || 60;

  if ((player.money || 0) < cost) {
    gameLog(gameId, 'Upgrade failed: not enough credits.');
    return;
  }

  const newLevel = (base.level || 1) + 1;
  const newIncome = (base.incomePerPlayer || 7) + 3;
  const newCost = Math.round(cost * 1.7);

  player.money = (player.money || 0) - cost;

  state.base = {
    ...base,
    level: newLevel,
    incomePerPlayer: newIncome,
    upgradeCost: newCost
  };

  gameLog(gameId, '⚡ Generator upgraded. All players gain more credits per tick.');
}

function handleDropship(gameId, game, uid, action) {
  const state = game.state;
  const player = game.players.get(uid);
  if (!player) return;

  const { sourceNodeId, destNodeId } = action;
  const now = state.timeSeconds || 0;
  const last = player.lastDropshipAt || 0;

  if (now - last < 60) {
    gameLog(gameId, 'Dropship failed: still on cooldown.');
    return;
  }

  const src = state.nodesState[sourceNodeId];
  const dst = state.nodesState[destNodeId];

  if (!src || !dst) {
    gameLog(gameId, 'Dropship failed: node missing.');
    return;
  }

  if (src.owner !== 'player') {
    gameLog(gameId, 'Dropship failed: source must be player-controlled.');
    return;
  }
  if (!(dst.owner === 'player' || dst.owner === 'contested')) {
    gameLog(gameId, 'Dropship failed: destination must be player or contested.');
    return;
  }

  const srcBf = src.battlefield || { troops: [] };
  const dstBf = dst.battlefield || { troops: [] };

  const troopsToMove = srcBf.troops || [];
  if (!troopsToMove.length) {
    gameLog(gameId, 'Dropship failed: no troops in source node.');
    return;
  }

  srcBf.troops = [];
  dstBf.troops = (dstBf.troops || []).concat(troopsToMove);

  src.battlefield = srcBf;
  dst.battlefield = dstBf;
  player.lastDropshipAt = now;

  gameLog(
    gameId,
    `🚀 Dropship: moved ${troopsToMove.length} troops from ${src.row + 1}-${src.col + 1} to ${dst.row + 1}-${dst.col + 1}.`
  );
}

function handlePlayerAction(gameId, game, uid, action) {
  if (!action || !action.type) return;

  switch (action.type) {
    case 'build':
      handleBuildAction(gameId, game, uid, action);
      break;
    case 'upgradeBase':
      handleUpgradeBase(gameId, game, uid);
      break;
    case 'dropship':
      handleDropship(gameId, game, uid, action);
      break;
    default:
      console.log('Unknown action type:', action.type);
  }

  // Optionally emit updated state immediately (in addition to tick broadcasts)
  io.to(gameId).emit('state', serializeGame(game));
}

// ------------- SOCKET.IO WIRING -------------
io.on('connection', (socket) => {
  console.log('Socket connected:', socket.id);

  // Client sends: { token, gameId, name }
  socket.on('joinGame', async ({ token, gameId, name }) => {
    try {
      const decoded = await admin.auth().verifyIdToken(token);
      const uid = decoded.uid;
      socket.data.uid = uid;
      socket.data.gameId = gameId;

      // Track presence in Firestore for lobby
      const playerRef = db.doc(`grid_overlord_games/${gameId}/players/${uid}`);
      await playerRef.set(
        {
          uid,
          name,
          joinedAt: admin.firestore.FieldValue.serverTimestamp()
        },
        { merge: true }
      );

      let game = games.get(gameId);
      if (!game) {
        game = createGame(gameId);
      }

      let player = game.players.get(uid);
      if (!player) {
        player = {
          uid,
          name,
          socketId: socket.id,
          money: 30,
          incomeBonus: 0,
          lastDropshipAt: 0
        };
        game.players.set(uid, player);
      } else {
        player.name = name;
        player.socketId = socket.id;
      }

      socket.join(gameId);

      console.log(`Player ${uid} joined game ${gameId}`);

      socket.emit('joined', {
        uid,
        gameId,
        state: serializeGame(game)
      });

      socket.to(gameId).emit('playerJoined', { uid, name });
      gameLog(gameId, `${name} joined the battle.`);
    } catch (err) {
      console.error('joinGame error:', err);
      socket.emit('errorMessage', 'Auth failed: ' + err.message);
    }
  });

  socket.on('playerAction', (action) => {
    const uid = socket.data.uid;
    const gameId = socket.data.gameId;
    if (!uid || !gameId) return;

    const game = games.get(gameId);
    if (!game) return;

    handlePlayerAction(gameId, game, uid, action);
  });

  socket.on('disconnect', () => {
    const uid = socket.data.uid;
    const gameId = socket.data.gameId;
    console.log('Socket disconnected:', socket.id, uid, gameId);

    if (!uid || !gameId) return;
    const game = games.get(gameId);
    if (!game) return;

    game.players.delete(uid);
    if (game.players.size === 0) {
      console.log('No players left, destroying game', gameId);
      destroyGame(gameId);
    }
  });
});

// ------------- START SERVER -------------
const PORT = process.env.PORT || 5000;
server.listen(PORT, '0.0.0.0', () => {
  console.log('Grid Overlord Node server running on port', PORT);
});
