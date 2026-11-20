import { DIFFICULTIES, ENEMIES, MAPS, SHOTS, STARTING_WAVES, STEP_MS, TOWERS, normalizeLoadout } from './battlefield.mjs';
import { DIRECTIONS, buildEnemyField, distanceSquared, findPath, interceptPoint, movingCircleHit, terrainHit, tileAt } from './navigation.mjs';
import { isWall } from './walls.mjs';

export { DIFFICULTIES, ENEMIES, MAPS, SHOTS, STARTER_LOADOUT, STARTING_WAVES, STEP_MS, TOWERS, isCompleteLoadout, normalizeLoadout } from './battlefield.mjs';

const STEP_SECONDS = STEP_MS / 1000;
const WAVE_SIZES = Object.freeze([2, 5, 8, 12, 17, 24, 26]);
const STARTING_CAT_HEALTH = Object.freeze({ 1: 1, 5: 1.65, 10: 2.45 });
const OPENING_CHEESE = 10;
const MAP_CHEESE_REFILL_INTERVAL = 5000;
const MAP_CHEESE_REFILL_COUNT = 2;
const PREPARATION_TIME = 30_000;
const EARLY_WAVE_INTERVALS = Object.freeze([45_000, 40_000, 35_000]);
const CAT_GROWTH_INTERVAL = 60_000;
const CAT_GROWTH_FACTOR = 1.06;
const CAT_MAX_HEALTH = Number.MAX_SAFE_INTEGER;
const CAT_RECOVERY_DELAY = 14_000;
const CAT_HEAL_INTERVAL = 10_000;
const CAT_OPENING_SPEED = 1.15;
const CAT_SPEED_RAMP_TIME = 150_000;
const CAT_CHASE_RANGE = 7.1;
const CAT_CHASE_STEPS = 8;
const EMPTY_STRUCTURES = new Map();
const CAT_SKILL_UNLOCKS = Object.freeze({
  pounce: { wave: 6, time: 180_000 },
  'tail-sweep': { wave: 8, time: 240_000 },
  'cat-charge': { wave: 10, time: 300_000 },
});
const RANKED_CAT_HP = 288;
const CAT_TEAM_HP = 110;
const MAX_PROJECTILES = 150;
const MAX_ENEMIES = 75;
const MAX_ALLIES = 36;
const MAX_EVENTS = 128;
const BUILD_RANGE = 6;
const NONE = Object.freeze({ ok: true });
const MELEE_MOVES = {
  rat: 'rat-bite', runner: 'rat-bite', stink: 'rat-bite', ox: 'ram', vulture: 'talon',
  king: 'king-bash', sapper: 'rat-bite', mender: 'rat-bite', cat: 'claw',
};
const SPECIAL_MOVES = {
  pounce: { windup: 760, radius: 1.5, range: 4.8, interval: 6300, damage: 22 },
  'powder-blast': { windup: 850, radius: 1.7, range: 5.35, interval: 5150, damage: 12 },
  mend: { windup: 725, radius: 2.4, range: 3.3, interval: 4900, heal: 10 },
  'tail-sweep': { windup: 1050, radius: 2.6, range: 2.25, interval: 5450, damage: 16 },
  'cat-charge': { windup: 1050, radius: 0.95, range: 7.1, interval: 7500, damage: 12, duration: 450 },
};

const refuse = code => ({ ok: false, code });
const position = entity => ({ x: entity.x, y: entity.y });
const isAlive = entity => entity.hp > 0;

function createRandom(seed) {
  let state = 2166136261;
  for (const char of String(seed)) state = Math.imul(state ^ char.charCodeAt(0), 16777619) >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 4294967296;
  };
}

function setFacing(entity, target) {
  const dx = target.x - entity.x;
  const dy = target.y - entity.y;
  if (dx || dy) entity.facing = Math.atan2(dy, dx);
}

function moveEntity(entity, dx, dy, passable) {
  const startX = entity.x;
  const startY = entity.y;
  if (dx && passable(startX + dx, startY)) entity.x += dx;
  if (dy && passable(entity.x, startY + dy)) entity.y += dy;
  entity.vx = (entity.x - startX) / STEP_SECONDS;
  entity.vy = (entity.y - startY) / STEP_SECONDS;
  if (entity.vx || entity.vy) entity.facing = Math.atan2(entity.vy, entity.vx);
}

function approach(entity, destination, speed, passable) {
  const dx = destination.x - entity.x;
  const dy = destination.y - entity.y;
  const distance = Math.hypot(dx, dy);
  const fraction = distance ? Math.min(1, speed * STEP_SECONDS / distance) : 0;
  moveEntity(entity, dx * fraction, dy * fraction, passable);
}

function bestWithin(origin, candidates, range, score, accepts = isAlive) {
  const limit = range ** 2;
  let bestScore = Infinity;
  let best = null;
  for (const candidate of candidates) {
    const distance = distanceSquared(origin, candidate);
    if (distance > limit || !accepts(candidate)) continue;
    const value = score ? score(candidate, distance) : distance;
    if (value >= bestScore) continue;
    bestScore = value;
    best = candidate;
  }
  return best;
}

function buildPositionError(map, actor, x, y) {
  if (!Number.isInteger(x) || !Number.isInteger(y)
    || x <= 0 || x >= map.size - 1 || y <= 0 || y >= map.size - 1) return 'invalid-cell';
  if (distanceSquared(actor, { x, y }) > BUILD_RANGE ** 2) return 'out-of-reach';
  return null;
}

function structurePlacementError(map, actor, type, x, y, structures, occupants) {
  const tile = tileAt(map, x, y);
  if (tile === '#' || tile === '~') return 'blocked';
  const spot = { x, y };
  if (map.spawns.some(spawn => distanceSquared(spawn, spot) < 4)
    || map.playerSpawns.some(spawn => distanceSquared(spawn, spot) === 0)) return 'spawn-protected';
  if (structures.some(structure => structure.x === x && structure.y === y)
    || bestWithin(spot, occupants, 0.72)) return 'occupied';
  if (actor.cheese < TOWERS[type].cost) return 'not-enough-cheese';
  return null;
}

export function getPlacementError(state, playerId, type, x, y) {
  const map = MAPS[state?.mapId];
  const actor = state?.players.find(player => player.id === playerId);
  if (!map || !actor) return 'invalid-command';
  if (state.status !== 'active') return 'match-ended';
  if (!actor.hp) return 'mouse-caught';
  return buildPositionError(map, actor, x, y)
    ?? (!actor.loadout.includes(type) ? 'invalid-slot' : structurePlacementError(map, actor, type, x, y,
      state.structures, [...state.players, ...state.enemies, ...state.allies]));
}

function publicEntity(entity) {
  const {
    path, destination, inputX, inputY, previousX, previousY, aiCell, nextAttackAt,
    targetId, producerId, expiresAt, shieldUntil, attackTarget, specialTimes, spawn,
    damage, splash, bonusUntil, bornTick, laneOffsetX, laneOffsetY, charge, abilityFxAt, staggerReadyAt,
    damageScale, nextHealAt, spawnWave,
    ...publicFields
  } = entity;
  return entity.loadout ? { ...publicFields, loadout: entity.loadout.slice() } : publicFields;
}

export function createSimulation({ matchId, mapId = 'flour-mill', difficulty = 'normal', startingWave = 1, ranked = false, players, seed = matchId } = {}) {
  if (typeof matchId !== 'string' || !matchId.length) throw new TypeError('A match ID is required.');
  if (!Object.hasOwn(MAPS, mapId)) throw new RangeError('Unknown battlefield.');
  if (!Object.hasOwn(DIFFICULTIES, difficulty)) throw new RangeError('Unknown difficulty.');
  if (!STARTING_WAVES.includes(startingWave)) throw new TypeError('Choose starting wave 1, 5, or 10.');
  if (typeof ranked !== 'boolean' || (ranked && startingWave !== 1)) throw new TypeError('Ranked matches start at wave 1.');
  if (!Array.isArray(players) || players.length < 1 || players.length > 4
    || players.some(player => !player || typeof player.id !== 'string' || !player.id.length)
    || new Set(players.map(player => player.id)).size !== players.length) {
    throw new TypeError('One to four unique players are required.');
  }

  const map = MAPS[mapId];
  const rules = DIFFICULTIES[difficulty];
  if (players.some(({ id }) => /^(?:structure|enemy|ally|shot|cheese)-\d+$/.test(id))) {
    throw new TypeError('A player ID conflicts with a battlefield entity.');
  }
  const random = createRandom(seed);
  const cheeseRandom = createRandom(`${seed}:map-cheese`);
  const actors = new Map();
  const team = new Map(players.map(({ id, name, loadout }, index) => {
    const spawn = map.playerSpawns[index];
    const actor = {
      id, name: String(name || `Mouse ${index + 1}`), color: index,
      ...spawn, spawn, facing: -Math.PI / 2, vx: 0, vy: 0, inputX: 0, inputY: 0,
      maxHp: 62, hp: 62, cheese: ranked ? rules.rankedOpeningCheese : OPENING_CHEESE, loadout: normalizeLoadout(loadout),
      reviveAt: 0, lastHitAt: -1000000, path: [], destination: null,
    };
    return [id, actor];
  }));
  const structures = new Map();
  const enemies = new Map();
  const allies = new Map();
  const projectiles = new Map();
  const pickups = new Map();
  const events = [];
  const pendingSpawns = [];
  let nextEventId = 1;
  let nextEntityId = 1;
  let tick = 0;
  let time = 0;
  let status = 'active';
  let resultAt = null;
  let wave = startingWave - 1;
  let nextWaveAt = ranked ? 12_500 : PREPARATION_TIME;
  let nextMapCheeseAt = MAP_CHEESE_REFILL_INTERVAL;
  let field = null;
  const patrolRoutes = buildEnemyField(map, map.playerSpawns, EMPTY_STRUCTURES);
  let rankedMouseCells;
  let rankedMouseRoutes;

  function event(type, source, fields) {
    events.push({ id: nextEventId++, type, tick, time, x: source.x, y: source.y, ...fields });
    if (events.length > MAX_EVENTS) events.shift();
  }

  function cellAt(x, y) {
    return Math.round(y) * map.size + Math.round(x);
  }

  function passable(x, y, ownerId) {
    const tile = tileAt(map, x, y);
    if (tile === '#' || tile === '~') return false;
    const structure = structures.get(cellAt(x, y));
    return !structure || structure.ownerId === ownerId;
  }

  function setMovement(actor, action) {
    if (Number.isFinite(action.dx) && Number.isFinite(action.dy)) {
      if (Math.abs(action.dx) > 1 || Math.abs(action.dy) > 1) return refuse('invalid-movement');
      const length = Math.max(1, Math.hypot(action.dx, action.dy));
      actor.inputX = action.dx / length;
      actor.inputY = action.dy / length;
      actor.destination = null;
      actor.path = [];
      return NONE;
    }
    if (!Number.isFinite(action.x) || !Number.isFinite(action.y)) return refuse('invalid-movement');
    const destination = { x: Math.round(action.x), y: Math.round(action.y) };
    const walkable = (x, y) => passable(x, y, actor.id);
    if (!walkable(destination.x, destination.y)) return refuse('blocked');
    const route = findPath(actor, destination, map.size, walkable);
    if (!route) return refuse('unreachable');
    actor.inputX = 0;
    actor.inputY = 0;
    actor.destination = destination;
    actor.path = route.length ? route : [destination];
    return NONE;
  }

  function build(actor, action) {
    if (!Number.isInteger(action.slot) || action.slot < 0 || action.slot >= actor.loadout.length) return refuse('invalid-slot');
    const type = actor.loadout[action.slot];
    const def = TOWERS[type];
    const hasAxis = isWall(type);
    const axis = action.axis === undefined ? 'x' : action.axis;
    if (hasAxis && axis !== 'x' && axis !== 'y') return refuse('invalid-axis');
    const error = structurePlacementError(map, actor, type, action.x, action.y, [...structures.values()], actors.values());
    if (error) return refuse(error);

    const { x, y } = action;
    actor.cheese -= def.cost;
    const structure = {
      id: `structure-${nextEntityId++}`, ownerId: actor.id, type, x, y,
      hp: def.hp, maxHp: def.hp, builtAt: time, readyAt: time + 700,
      shield: 0, shieldUntil: 0, branch: null, aimX: null, aimY: null, facing: 0,
      nextAttackAt: time + 700, lastAttackAt: -1000000, lastHitAt: -1000000,
      bonusUntil: 0, disabledUntil: 0, windupUntil: 0, targetId: null,
    };
    if (hasAxis) structure.axis = axis;
    structures.set(cellAt(x, y), structure);
    actors.set(structure.id, structure);
    field = null;
    event('build', structure, { entityId: structure.id, ownerId: actor.id, kind: type, readyAt: structure.readyAt });
    return { ok: true, id: structure.id };
  }

  function upgrade(actor, action) {
    const structure = structures.get(cellAt(action.x, action.y));
    if (!structure || structure.ownerId !== actor.id) return refuse('not-your-structure');
    const choices = TOWERS[structure.type].upgrades;
    if (!choices || typeof action.branch !== 'string' || !Object.hasOwn(choices, action.branch)) return refuse('invalid-upgrade');
    if (structure.branch) return refuse('already-upgraded');
    if (structure.readyAt > time) return refuse('under-construction');
    const branch = choices[action.branch];
    if (actor.cheese < branch.cost) return refuse('not-enough-cheese');
    actor.cheese -= branch.cost;
    structure.branch = action.branch;
    event('upgrade', structure, { entityId: structure.id, ownerId: actor.id, branch: action.branch });
    return NONE;
  }

  function command(playerId, action) {
    const actor = team.get(playerId);
    if (!actor || !action || typeof action !== 'object') return refuse('invalid-command');
    if (status !== 'active') return refuse('match-ended');
    if (!actor.hp) return refuse('mouse-caught');
    if (action.type === 'move') return setMovement(actor, action);
    if (action.type !== 'build' && action.type !== 'upgrade') return refuse('invalid-command');
    const error = buildPositionError(map, actor, action.x, action.y);
    if (error) return refuse(error);
    return action.type === 'build' ? build(actor, action) : upgrade(actor, action);
  }

  function putCheese(x, y, value, lifespan = 48000) {
    if (Number.isFinite(lifespan) && pickups.size >= 90) return;
    const pickup = { id: `cheese-${nextEntityId++}`, x, y, value, expiresAt: time + lifespan };
    pickups.set(pickup.id, pickup);
  }

  function refillMapCheese() {
    if (time < nextMapCheeseAt) return;
    nextMapCheeseAt += MAP_CHEESE_REFILL_INTERVAL;
    const occupied = new Set();
    let mapCheese = 0;
    for (const pickup of pickups.values()) {
      occupied.add(cellAt(pickup.x, pickup.y));
      if (pickup.expiresAt === Infinity) mapCheese++;
    }
    const refill = Math.min(MAP_CHEESE_REFILL_COUNT, map.cheese.length - mapCheese);
    if (refill <= 0) return;
    const free = [...map.cheese, ...map.cheeseRefills].filter(spot => passable(spot.x, spot.y)
      && !occupied.has(cellAt(spot.x, spot.y)) && !bestWithin(spot, team.values(), 2));
    for (let index = 0; index < refill && free.length; index++) {
      const [spot] = free.splice(Math.floor(cheeseRandom() * free.length), 1);
      putCheese(spot.x, spot.y, 3, Infinity);
    }
  }

  function advancePlayers() {
    for (const actor of team.values()) {
      actor.previousX = actor.x;
      actor.previousY = actor.y;
      const walkable = (x, y) => passable(x, y, actor.id);
      if (!actor.hp) {
        if (actor.reviveAt <= time) {
          const safe = walkable(actor.spawn.x, actor.spawn.y) ? actor.spawn
            : map.playerSpawns.find(({ x, y }) => walkable(x, y)) ?? actor.spawn;
          Object.assign(actor, safe, { previousX: safe.x, previousY: safe.y, hp: actor.maxHp, reviveAt: 0 });
          event('revive', actor, { entityId: actor.id });
        }
        continue;
      }
      if (actor.path.length && !walkable(actor.path[0].x, actor.path[0].y)) {
        actor.path = findPath(actor, actor.destination, map.size, walkable) ?? [];
      }
      const waypoint = actor.path[0];
      if (waypoint) {
        approach(actor, waypoint, 4.1, walkable);
        if (distanceSquared(actor, waypoint) < 0.000001) actor.path.shift();
      } else {
        moveEntity(actor, actor.inputX * 4.1 * STEP_SECONDS, actor.inputY * 4.1 * STEP_SECONDS, walkable);
      }
      for (const pickup of pickups.values()) {
        if (distanceSquared(actor, pickup) > 0.64) continue;
        actor.cheese += pickup.value;
        pickups.delete(pickup.id);
        event('collect', pickup, { entityId: actor.id, pickupId: pickup.id, value: pickup.value });
      }
    }
  }

  for (const actor of team.values()) actors.set(actor.id, actor);
  for (const cheese of map.cheese) putCheese(cheese.x, cheese.y, 3, Infinity);
  let cat = ranked ? null : spawnEnemy('cat', Math.floor(random() * map.spawns.length), wave);

  function beginWave() {
    wave++;
    // A wave lasts longer than its scheduled arrivals. Drop any arrivals still queued at the live enemy cap.
    if (!ranked) pendingSpawns.length = 0;
    const schedule = (type, delay, lane) => pendingSpawns.push({ time: time + delay, type, lane, wave });
    const soloCount = WAVE_SIZES[wave - 1] ?? (WAVE_SIZES.at(-1) + (wave - WAVE_SIZES.length) * 2);
    const count = Math.min(48, soloCount + (team.size - 1) * (1 + Math.floor(wave / 2))
      + Math.floor(rules.waveBonus * (wave - 1) / 6));
    const firstLane = Math.floor(random() * map.spawns.length);
    for (let index = 0; index < count; index++) {
      const type = wave >= 3 && index % 5 === 4 ? 'runner' : 'rat';
      schedule(type, 550 * Math.floor(index / 3), (firstLane + index) % map.spawns.length);
    }
    const specials = [[2, 'stink'], [3, 'vulture'], [4, 'ox'], [5, 'king'], [6, 'stink'], [7, 'ox']];
    for (const [minimum, type] of specials) {
      if (wave < minimum) continue;
      schedule(type, minimum * 650, (minimum + firstLane) % map.spawns.length);
    }
    if (wave >= 6) {
      const groups = rules.catSweep ? 2 : 1;
      for (let group = 0; group < groups; group++) {
        const lane = (firstLane + group * 2 + 1) % map.spawns.length;
        schedule('sapper', 3550 + group * 2000, lane);
        schedule('mender', 4650 + group * 2000, lane);
      }
    }
    const finalRankedWave = ranked && wave === WAVE_SIZES.length;
    if (finalRankedWave) schedule('cat', 3400, firstLane);
    pendingSpawns.sort((a, b) => a.time - b.time);
    const interval = ranked ? (wave < 3 ? 27_000 : 30_000) : EARLY_WAVE_INTERVALS[wave - 1] ?? 30_000;
    nextWaveAt = finalRankedWave ? null : time + interval;
    for (const mouse of team.values()) mouse.cheese += Math.min(14, 2 + Math.floor(wave / 2));
    const countByEntrance = Object.fromEntries(map.spawns.map((spawn, lane) =>
      [spawn.id, pendingSpawns.filter(entry => entry.lane === lane).length],
    ));
    event('wave', map.spawns[firstLane], { wave, count: pendingSpawns.length, nextWaveAt, countByEntrance });
  }

  function spawnEnemy(type, lane, spawnWave) {
    const entrance = map.spawns[lane];
    const { x, y } = entrance;
    const def = ENEMIES[type];
    const boss = type === 'cat';
    const teamBonus = CAT_TEAM_HP * (team.size - 1);
    const catHp = ranked ? RANKED_CAT_HP + teamBonus
      : (def.hp + teamBonus) * STARTING_CAT_HEALTH[startingWave] * rules.catHpMultiplier;
    const waveHp = def.hp * (1 + (spawnWave - 1) * 0.16);
    const hp = Math.round(rules.health * (boss ? catHp : waveHp));
    const laneOffsetX = (random() - 0.5) * 0.72;
    const laneOffsetY = (random() - 0.5) * 0.72;
    const enemy = {
      id: `enemy-${nextEntityId++}`, type, x: x + laneOffsetX, y: y + laneOffsetY, laneOffsetX, laneOffsetY,
      hp, maxHp: hp, vx: 0, vy: 0, facing: 0,
      spawnedAt: time, lastAttackAt: -1000000, lastHitAt: -1000000, nextAttackAt: 0,
      windupUntil: 0, slowUntil: 0, trapUntil: 0,
      damageScale: boss ? 1 : 1 + (spawnWave - 1) * 0.10,
      spawnWave,
      specialTimes: boss ? { pounce: time - 3200 } : {},
      attackTarget: null, attackKind: null, lastAttackKind: null, aiCell: null,
    };
    enemies.set(enemy.id, enemy);
    actors.set(enemy.id, enemy);
    event('spawn', enemy, { entityId: enemy.id, kind: type, entrance: entrance.id });
    return enemy;
  }

  function spawnEnemies() {
    if (nextWaveAt !== null && time >= nextWaveAt) beginWave();
    while (pendingSpawns.length && pendingSpawns[0].time <= time && enemies.size < MAX_ENEMIES) {
      const { type, lane, wave: spawnWave } = pendingSpawns.shift();
      const enemy = spawnEnemy(type, lane, spawnWave);
      if (type === 'cat') cat = enemy;
    }
  }

  function damage(target, amount, source, kind) {
    if (!target.hp || amount <= 0) return;
    const absorption = Math.min(target.shield || 0, amount);
    if (absorption) {
      target.shield -= absorption;
      if (!target.shield) {
        target.shieldUntil = time + 2400;
        event('shield-break', target, { entityId: target.id, sourceId: source.id });
      }
    }
    target.hp = Math.max(0, target.hp - (amount - absorption));
    target.lastHitAt = time;
    event('damage', target, { entityId: target.id, sourceId: source.id, value: amount - absorption, shield: absorption, kind });
    if (target.hp > 0) return;
    event('defeat', target, { entityId: target.id, sourceId: source.id, kind: target.type || 'mouse' });
    if (team.has(target.id)) {
      Object.assign(target, {
        inputX: 0, inputY: 0, vx: 0, vy: 0, reviveAt: time + 9500, path: [], destination: null,
      });
      return;
    }
    if (enemies.delete(target.id) && ENEMIES[target.type].cheese > 0) {
      putCheese(target.x, target.y, ENEMIES[target.type].cheese);
    }
    if (structures.get(cellAt(target.x, target.y)) === target) {
      structures.delete(cellAt(target.x, target.y));
      field = null;
      event('destroy', target, { entityId: target.id, kind: target.type });
    }
    allies.delete(target.id);
    actors.delete(target.id);
  }

  function healEnemy(enemy, amount, kind, sourceId) {
    if (!enemy.hp || enemy.hp >= enemy.maxHp || (enemy.nextHealAt ?? 0) > time) return;
    const healed = Math.min(amount, enemy.maxHp - enemy.hp);
    enemy.hp += healed;
    if (enemy.type === 'cat') enemy.nextHealAt = time + CAT_HEAL_INTERVAL;
    event('heal', enemy, { kind, entityId: enemy.id, sourceId, value: healed });
  }

  function advanceCatHealth() {
    if (!cat.hp) return;
    if (time % CAT_GROWTH_INTERVAL === 0 && cat.maxHp < CAT_MAX_HEALTH) {
      const maxHp = Math.min(CAT_MAX_HEALTH, Math.ceil(cat.maxHp * CAT_GROWTH_FACTOR));
      cat.hp = Math.max(1, Math.floor(cat.hp * maxHp / cat.maxHp));
      event('cat-growth', cat, { kind: 'cat-growth', entityId: cat.id, value: maxHp - cat.maxHp, maxHp });
      cat.maxHp = maxHp;
    }
    if (time - cat.lastHitAt >= CAT_RECOVERY_DELAY) {
      healEnemy(cat, Math.min(8, Math.ceil(cat.maxHp * 0.0125)), 'cat-recover', cat.id);
    }
  }

  function staggerCat(cat, duration) {
    if (time < (cat.staggerReadyAt ?? 0)) return;
    cat.trapUntil = time + duration;
    cat.staggerReadyAt = cat.trapUntil + 1400;
  }

  const clearPath = (origin, target) =>
    terrainHit(map, origin.x, origin.y, target.x, target.y, (x, y) => !passable(x, y)) === null;
  const clearPathToStructure = (origin, target) =>
    terrainHit(map, origin.x, origin.y, target.x, target.y,
      (x, y) => (x !== target.x || y !== target.y) && !passable(x, y)) === null;
  const clearTerrain = (origin, target) => terrainHit(map, origin.x, origin.y, target.x, target.y) === null;

  function attackFields(entity, kind, targetId = entity.attackTarget) {
    return {
      entityId: entity.id, kind, targetId,
      targetX: entity.attackX, targetY: entity.attackY, radius: entity.attackRadius,
    };
  }

  function armAttack(entity, kind, target, duration, radius = 0) {
    Object.assign(entity, {
      attackKind: kind, attackTarget: target.id ?? null,
      attackX: target.x, attackY: target.y, attackRadius: radius,
      windupUntil: time + duration,
    });
    setFacing(entity, target);
    event('windup', entity, { ...attackFields(entity, kind), until: entity.windupUntil, duration });
  }

  function clearAttack(entity) {
    entity.windupUntil = 0;
    entity.attackKind = null;
    entity.attackTarget = null;
  }

  function trackAttack(entity, target) {
    entity.attackX = target.x;
    entity.attackY = target.y;
    setFacing(entity, target);
  }

  function startSpecial(enemy, kind, target) {
    const def = SPECIAL_MOVES[kind];
    if (!target || time - (enemy.specialTimes[kind] ?? -Infinity) < def.interval) return false;
    enemy.specialTimes[kind] = time;
    armAttack(enemy, kind, target, def.windup, def.radius);
    return true;
  }

  function chaseMouse(enemy) {
    let chase = null;
    let nearestDistance = CAT_CHASE_RANGE ** 2;
    for (const mouse of team.values()) {
      const distance = distanceSquared(enemy, mouse);
      if (!mouse.hp || distance > nearestDistance) continue;
      const direct = clearPath(enemy, mouse);
      const goal = { x: Math.round(mouse.x), y: Math.round(mouse.y) };
      const route = direct ? [] : findPath(enemy, goal, map.size, passable, CAT_CHASE_STEPS);
      if (route === null) continue;
      chase = { mouse, direct, waypoint: route[0] ?? mouse };
      nearestDistance = distance;
    }
    return chase;
  }

  function siegeTarget(routes, occupiedCell) {
    let cell = occupiedCell;
    while (routes[cell] >= 0 && routes[cell] !== cell) cell = routes[cell];
    return structures.get(cell);
  }

  function catSkillUnlocked(kind) {
    const unlock = CAT_SKILL_UNLOCKS[kind];
    return ranked || (wave >= unlock.wave && time >= unlock.time);
  }

  function chooseSpecial(enemy, visibleMouse, targetStructure) {
    if (time < enemy.nextAttackAt) return false;
    if (enemy.type === 'cat') {
      const charge = SPECIAL_MOVES['cat-charge'];
      const sweep = SPECIAL_MOVES['tail-sweep'];
      if (!visibleMouse) return false;
      const distance = distanceSquared(enemy, visibleMouse);
      if (rules.catCharge && catSkillUnlocked('cat-charge') && distance >= 2.45 ** 2 && distance <= charge.range ** 2
        && startSpecial(enemy, 'cat-charge', visibleMouse)) return true;
      if (rules.catSweep && catSkillUnlocked('tail-sweep')
        && distance <= sweep.range ** 2 && startSpecial(enemy, 'tail-sweep', position(enemy))) return true;
      return catSkillUnlocked('pounce') && distance <= SPECIAL_MOVES.pounce.range ** 2
        && startSpecial(enemy, 'pounce', visibleMouse);
    }
    if (enemy.type === 'sapper') {
      const def = SPECIAL_MOVES['powder-blast'];
      return targetStructure && distanceSquared(enemy, targetStructure) <= def.range ** 2
        && clearPathToStructure(enemy, targetStructure) && startSpecial(enemy, 'powder-blast', targetStructure);
    }
    if (enemy.type === 'mender') {
      const def = SPECIAL_MOVES.mend;
      const target = bestWithin(enemy, enemies.values(), def.range,
        (rat, distance) => distance - Math.min(def.heal, rat.maxHp - rat.hp) * 0.75,
        rat => rat !== enemy && (!ranked || rat.type !== 'cat') && rat.hp <= rat.maxHp - 4
          && (rat.nextHealAt ?? 0) <= time && clearTerrain(enemy, rat));
      return startSpecial(enemy, 'mend', target);
    }
    return false;
  }

  function hurtArea(enemy, center, kind, def) {
    const targets = kind === 'powder-blast' ? structures.values()
      : [...team.values(), ...structures.values(), ...allies.values()];
    for (const target of targets) {
      if (distanceSquared(center, target) > def.radius ** 2 || !clearTerrain(center, target)) continue;
      damage(target, Math.ceil(def.damage * rules.damage * enemy.damageScale), enemy, kind);
    }
  }

  function moveCharge(enemy) {
    const charge = enemy.charge;
    const def = SPECIAL_MOVES['cat-charge'];
    const fraction = Math.min(1, (time - charge.startedAt) / def.duration);
    const nextX = charge.x + (charge.endX - charge.x) * fraction;
    const nextY = charge.y + (charge.endY - charge.y) * fraction;
    const dx = nextX - enemy.x;
    const dy = nextY - enemy.y;
    const blocked = terrainHit(map, enemy.x, enemy.y, nextX, nextY, (x, y) => !passable(x, y));
    const caught = enemy.trapUntil > time;
    const travel = caught ? 0 : blocked === null ? 1 : Math.max(0, blocked - 0.025 / Math.max(Math.hypot(dx, dy), 0.025));
    for (const target of [...team.values(), ...structures.values(), ...allies.values()]) {
      if (caught || !target.hp || charge.hits.has(target.id)) continue;
      const hit = movingCircleHit(enemy.x, enemy.y, dx * travel, dy * travel, target, def.radius);
      if (hit === null) continue;
      charge.hits.add(target.id);
      const scale = structures.get(cellAt(target.x, target.y)) === target ? 1 : 0.75;
      damage(target, Math.ceil(def.damage * scale * rules.damage), enemy, 'cat-charge');
    }
    enemy.x += dx * travel;
    enemy.y += dy * travel;
    enemy.vx = dx * travel / STEP_SECONDS;
    enemy.vy = dy * travel / STEP_SECONDS;
    if (travel < 1 || fraction === 1) {
      event('charge-end', enemy, { entityId: enemy.id, kind: 'cat-charge', radius: def.radius });
      enemy.charge = null;
      enemy.chargeUntil = 0;
      enemy.aiCell = null;
    }
  }

  function resolveSpecial(enemy, kind, def) {
    const target = { x: enemy.attackX, y: enemy.attackY };
    const fields = { ...attackFields(enemy, kind), duration: def.duration || 360 };
    if (kind === 'pounce') {
      const land = clearPath(enemy, target) ? target : position(enemy);
      event('attack', enemy, { ...fields, targetX: land.x, targetY: land.y });
      Object.assign(enemy, land, { previousX: land.x, previousY: land.y, aiCell: null });
      for (const mouse of team.values()) {
        if (distanceSquared(mouse, enemy) <= def.radius ** 2 && clearPath(enemy, mouse)) {
          damage(mouse, Math.ceil(def.damage * rules.damage), enemy, kind);
        }
      }
      return;
    }
    event('attack', enemy, fields);
    if (kind === 'mend') {
      const wounded = [...enemies.values()].filter(rat => rat !== enemy && (!ranked || rat.type !== 'cat') && rat.hp < rat.maxHp
        && (rat.nextHealAt ?? 0) <= time && distanceSquared(rat, target) <= def.radius ** 2 && clearTerrain(target, rat));
      wounded.sort((a, b) => (b.maxHp - b.hp) - (a.maxHp - a.hp));
      for (const rat of wounded.slice(0, 3)) {
        const amount = rat.type === 'cat' ? Math.min(5, Math.ceil(rat.maxHp * 0.01)) : def.heal;
        healEnemy(rat, amount, kind, enemy.id);
      }
    } else if (kind === 'cat-charge') {
      enemy.charge = { ...position(enemy), endX: target.x, endY: target.y, startedAt: time, hits: new Set() };
      enemy.chargeUntil = time + def.duration;
    } else {
      hurtArea(enemy, target, kind, def);
    }
  }

  function contact(enemy) {
    const def = ENEMIES[enemy.type];
    const kind = enemy.attackKind;
    enemy.lastAttackAt = time;
    enemy.lastAttackKind = kind;
    enemy.nextAttackAt = time + def.cooldown;
    if (SPECIAL_MOVES[kind]) {
      resolveSpecial(enemy, kind, SPECIAL_MOVES[kind]);
    } else {
      const target = actors.get(enemy.attackTarget);
      const hit = Boolean(target?.hp && distanceSquared(enemy, target) <= (def.reach + 0.46) ** 2
        && (team.has(target.id) ? clearPath(enemy, target) : clearPathToStructure(enemy, target)));
      if (target) trackAttack(enemy, target);
      event('attack', enemy, { ...attackFields(enemy, kind), hit, duration: 290 });
      if (hit) {
        const supported = enemy.type !== 'king' && bestWithin(enemy, enemies.values(), 3.1, null, candidate => candidate.type === 'king');
        const breach = (enemy.type === 'cat' || enemy.type === 'ox')
          && structures.get(cellAt(target.x, target.y)) === target ? 1.45 : 1;
        const amount = Math.ceil(def.damage * rules.damage * enemy.damageScale * (supported ? 1.3 : 1) * breach);
        damage(target, amount, enemy, kind);
      }
    }
    clearAttack(enemy);
  }

  function windup(enemy, target) {
    if (time >= enemy.nextAttackAt) armAttack(enemy, MELEE_MOVES[enemy.type], target, ENEMIES[enemy.type].windup);
  }

  function catSpeed(enemy) {
    const opening = ranked ? 2.05 : CAT_OPENING_SPEED;
    const progress = 1 - Math.exp(-(time - enemy.spawnedAt) / CAT_SPEED_RAMP_TIME);
    return opening + (ENEMIES.cat.speed - opening) * progress;
  }

  function separateEnemies() {
    // Move overlapping enemies apart without blocking the lanes like a wall would.
    const crowd = [...enemies.values()];
    const nudgeX = new Float64Array(crowd.length);
    const nudgeY = new Float64Array(crowd.length);
    for (let index = 0; index < crowd.length; index++) {
      const enemy = crowd[index];
      for (let other = index + 1; other < crowd.length; other++) {
        const neighbor = crowd[other];
        const gap = ENEMIES[enemy.type].radius + ENEMIES[neighbor.type].radius + 0.08;
        const dx = enemy.x - neighbor.x;
        const dy = enemy.y - neighbor.y;
        const squared = dx * dx + dy * dy;
        if (squared >= gap * gap) continue;
        const distance = Math.sqrt(squared);
        const overlap = (gap - distance) * 0.42;
        const pushX = distance ? dx / distance * overlap : overlap;
        const pushY = distance ? dy / distance * overlap : 0;
        nudgeX[index] += pushX;
        nudgeY[index] += pushY;
        nudgeX[other] -= pushX;
        nudgeY[other] -= pushY;
      }
    }
    for (let index = 0; index < crowd.length; index++) {
      const enemy = crowd[index];
      if (enemy.charge || enemy.windupUntil || enemy.trapUntil > time) continue;
      const push = Math.hypot(nudgeX[index], nudgeY[index]);
      if (!push) continue;
      const fraction = Math.min(1, (enemy.type === 'cat' ? 0.045 : 0.105) / push);
      const facing = enemy.facing;
      moveEntity(enemy, nudgeX[index] * fraction, nudgeY[index] * fraction, passable);
      enemy.vx = (enemy.x - enemy.previousX) / STEP_SECONDS;
      enemy.vy = (enemy.y - enemy.previousY) / STEP_SECONDS;
      enemy.facing = facing;
    }
  }

  function stepEnemies() {
    // Each structure is a destination, so a route naturally ends at the first one in its way.
    const siegeRoutes = field ??= buildEnemyField(map, structures.values(), EMPTY_STRUCTURES);
    if (ranked) {
      const targets = [...team.values()].filter(isAlive).map(mouse => ({ x: Math.round(mouse.x), y: Math.round(mouse.y) }));
      const cells = targets.map(({ x, y }) => cellAt(x, y)).join(',');
      if (cells !== rankedMouseCells) {
        rankedMouseCells = cells;
        rankedMouseRoutes = buildEnemyField(map, targets, EMPTY_STRUCTURES);
      }
    }
    for (const enemy of enemies.values()) {
      const def = ENEMIES[enemy.type];
      enemy.previousX = enemy.x;
      enemy.previousY = enemy.y;
      enemy.vx = 0;
      enemy.vy = 0;
      if (enemy.charge) {
        moveCharge(enemy);
        continue;
      }
      if (enemy.trapUntil > time) {
        if (enemy.windupUntil) {
          event('interrupt', enemy, attackFields(enemy, enemy.attackKind));
          clearAttack(enemy);
        }
        continue;
      }
      if (enemy.windupUntil) {
        if (enemy.windupUntil <= time) contact(enemy);
        continue;
      }
      const occupiedCell = cellAt(enemy.x, enemy.y);
      const structureReachable = siegeRoutes[occupiedCell] >= 0;
      const chase = enemy.type === 'cat' || (ranked && !structureReachable) ? chaseMouse(enemy) : null;
      const sapperTarget = enemy.type === 'sapper' ? siegeTarget(siegeRoutes, occupiedCell) : null;
      if (chooseSpecial(enemy, chase?.direct ? chase.mouse : null, sapperTarget)) continue;

      const speed = (enemy.type === 'cat' ? catSpeed(enemy) : def.speed) * (enemy.slowUntil > time ? 0.52 : 1);
      if (chase) {
        enemy.aiCell = null;
        if (chase.direct && distanceSquared(enemy, chase.mouse) <= (def.reach + 0.19) ** 2) windup(enemy, chase.mouse);
        else approach(enemy, chase.waypoint, speed, passable);
        continue;
      }

      // Finish the current tile before changing axes to prevent corner oscillation.
      const routeCell = structureReachable ? siegeRoutes[occupiedCell] : (rankedMouseRoutes ?? patrolRoutes)[occupiedCell];
      const destinationCell = enemy.aiCell === occupiedCell ? occupiedCell : routeCell;
      if (destinationCell < 0) continue;
      const destination = {
        x: destinationCell % map.size + enemy.laneOffsetX,
        y: Math.floor(destinationCell / map.size) + enemy.laneOffsetY,
      };
      const barrier = structures.get(destinationCell);
      if (barrier?.hp && distanceSquared(enemy, barrier) <= (def.reach + 0.31) ** 2
        && clearPathToStructure(enemy, barrier)) {
        windup(enemy, barrier);
        continue;
      }
      enemy.aiCell = destinationCell;
      approach(enemy, destination, speed, passable);
      if (distanceSquared(enemy, destination) < 0.000001) enemy.aiCell = null;
    }
    separateEnemies();

    if (tick % 5 === 0) {
      for (const enemy of enemies.values()) {
        if (enemy.type !== 'stink' || enemy.trapUntil > time) continue;
        let newlyDisrupted = false;
        for (const dx of [-2, -1, 0, 1, 2]) {
          for (const dy of [-2, -1, 0, 1, 2]) {
            const structure = structures.get(cellAt(enemy.x + dx, enemy.y + dy));
            if (!structure || TOWERS[structure.type].role === 'Defense' || distanceSquared(enemy, structure) > 2.05 ** 2) continue;
            if (structure.disabledUntil <= time) {
              newlyDisrupted = true;
              event('disrupt', structure, { kind: 'stink-cloud', entityId: structure.id, sourceId: enemy.id });
            }
            structure.disabledUntil = time + 850;
          }
        }
        if (newlyDisrupted) event('activation', enemy, { entityId: enemy.id, kind: 'stink-cloud', radius: 2.05 });
      }
    }
  }

  function launch(shooter, target, { kind, damage: power, splash = 0 }, angleOffset = 0) {
    if (projectiles.size >= MAX_PROJECTILES) return;
    const def = SHOTS[kind];
    const { x: aimX, y: aimY } = interceptPoint(shooter, target, def.speed, def.lifetime);
    const angle = Math.atan2(aimY - shooter.y, aimX - shooter.x) + angleOffset;
    const cosine = Math.cos(angle);
    const sine = Math.sin(angle);
    const shot = {
      id: `shot-${nextEntityId++}`, kind, shooterId: shooter.id,
      x: shooter.x + 0.26 * cosine, y: shooter.y + 0.26 * sine,
      vx: def.speed * cosine, vy: def.speed * sine,
      expiresAt: time + def.lifetime, launchedAt: time, bornTick: tick, damage: power, splash,
    };
    projectiles.set(shot.id, shot);
    event('shot', shot, {
      kind, projectileId: shot.id, shooterId: shooter.id, targetId: target.id,
      targetX: aimX, targetY: aimY, sourceX: shooter.x, sourceY: shooter.y,
      vx: shot.vx, vy: shot.vy, lifetime: def.lifetime,
    });
  }

  function impact(shot, target, reason) {
    projectiles.delete(shot.id);
    event('impact', shot, {
      kind: shot.kind, projectileId: shot.id, shooterId: shot.shooterId,
      targetId: target?.id ?? null, radius: shot.splash, reason,
    });
    if (target) {
      const boss = ['ox', 'king', 'cat'].includes(target.type);
      if (shot.kind === 'web') {
        if (target.type === 'cat') staggerCat(target, 550);
        else target.trapUntil = Math.max(target.trapUntil, time + (boss ? 550 : 1380));
        target.slowUntil = Math.max(target.slowUntil, target.trapUntil + 1400);
      }
      damage(target, shot.damage, { id: shot.shooterId }, shot.kind);
    }
    if (shot.splash && reason !== 'expired') {
      for (const enemy of enemies.values()) {
        if (enemy === target) continue;
        const distance = Math.sqrt(distanceSquared(shot, enemy));
        if (distance > shot.splash) continue;
        damage(enemy, Math.ceil(shot.damage * (0.85 - 0.47 * distance / shot.splash)), { id: shot.shooterId }, shot.kind);
      }
    }
  }

  function stepProjectiles() {
    for (const shot of projectiles.values()) {
      if (shot.bornTick === tick) continue;
      const def = SHOTS[shot.kind];
      const travel = Math.min(STEP_MS, Math.max(0, shot.expiresAt - time + STEP_MS)) / 1000;
      const dx = shot.vx * travel;
      const dy = shot.vy * travel;
      const terrain = def.terrain ? terrainHit(map, shot.x, shot.y, shot.x + dx, shot.y + dy) : null;
      let first = terrain ?? Infinity;
      let target = null;
      for (const enemy of enemies.values()) {
        if (Math.abs(shot.x - enemy.x) > 1.8 || Math.abs(shot.y - enemy.y) > 1.8) continue;
        const hit = movingCircleHit(shot.x, shot.y, dx, dy, enemy, def.radius + ENEMIES[enemy.type].radius);
        if (hit === null || hit >= first) continue;
        first = hit;
        target = enemy;
      }
      const progress = first === Infinity ? 1 : first;
      shot.x += dx * progress;
      shot.y += dy * progress;
      if (target) impact(shot, target, 'target');
      else if (terrain !== null) impact(shot, null, 'terrain');
      else if (time >= shot.expiresAt) impact(shot, null, 'expired');
    }
  }

  function canShoot(shooter, target, kind) {
    if (kind === 'acorn') return true;
    return terrainHit(map, shooter.x, shooter.y, target.x, target.y) === null;
  }

  function chooseTarget(origin, def) {
    const score = def.kind === 'rocket' ? (candidate, distance) => distance - candidate.maxHp * 0.16 : null;
    return bestWithin(origin, enemies.values(), def.range, score,
      candidate => candidate.hp && canShoot(origin, candidate, def.kind));
  }

  function aim(shooter, target, kind) {
    const def = SHOTS[kind];
    const point = def ? interceptPoint(shooter, target, def.speed, def.lifetime) : target;
    shooter.aimX = point.x;
    shooter.aimY = point.y;
    setFacing(shooter, point);
  }

  function discharge(structure, target, def) {
    structure.lastAttackAt = time;
    structure.lastAttackKind = def.kind || structure.type;
    if (def.kind) {
      const count = def.burst || 1;
      for (let index = 0; index < count; index++) launch(structure, target, def, (index - (count - 1) / 2) * 0.055);
      return;
    }
    const kind = structure.type;
    let previous = structure;
    const targets = [target];
    if (kind === 'tesla') {
      const neighbors = [...enemies.values()].filter(enemy => enemy !== target && distanceSquared(enemy, target) <= 2.8 ** 2);
      neighbors.sort((a, b) => distanceSquared(target, a) - distanceSquared(target, b));
      targets.push(...neighbors.slice(0, 2));
    }
    for (const [index, enemy] of targets.entries()) {
      event('attack', previous, { entityId: structure.id, kind, targetId: enemy.id, targetX: enemy.x, targetY: enemy.y, duration: 300 });
      damage(enemy, Math.max(1, Math.floor(def.damage * (1 - index * 0.22))), structure, kind);
      previous = enemy;
    }
  }

  function spawnAlly(structure) {
    if (allies.size >= MAX_ALLIES || [...allies.values()].some(ally => ally.producerId === structure.id)) return false;
    for (const [dx, dy] of DIRECTIONS) {
      const x = structure.x + dx;
      const y = structure.y + dy;
      if (!passable(x, y) || bestWithin({ x, y }, actors.values(), 0.75)) continue;
      const type = structure.type;
      const hp = type === 'rabbit' ? 20 : 12;
      const ally = {
        id: `ally-${nextEntityId++}`, producerId: structure.id, type, x, y, hp, maxHp: hp,
        ownerId: structure.ownerId, vx: 0, vy: 0, facing: Math.atan2(dy, dx),
        spawnedAt: time, nextAttackAt: time, lastAttackAt: -1000000, lastHitAt: -1000000,
        slowUntil: 0, trapUntil: 0, windupUntil: 0, bonusUntil: 0,
        expiresAt: time + 35000, spawn: { x, y },
      };
      allies.set(ally.id, ally);
      actors.set(ally.id, ally);
      event('spawn', ally, { entityId: ally.id, sourceId: structure.id, kind: type });
      return true;
    }
    return false;
  }

  function nearbyStructures(origin, range, visit) {
    const reach = Math.ceil(range);
    for (let dy = -reach; dy <= reach; dy++) {
      for (let dx = -reach; dx <= reach; dx++) {
        if (dx ** 2 + dy ** 2 > range ** 2) continue;
        const x = origin.x + dx;
        const y = origin.y + dy;
        if (x < 1 || y < 1 || x >= map.size - 1 || y >= map.size - 1) continue;
        const neighbor = structures.get(cellAt(x, y));
        if (neighbor && neighbor.readyAt <= time) visit(neighbor);
      }
    }
  }

  function utility(structure, def) {
    const { type } = structure;
    let effect = false;
    if (type === 'generator') team.get(structure.ownerId).cheese++;
    if (type === 'farm') {
      const spot = DIRECTIONS.map(([dx, dy]) => ({ x: structure.x + dx, y: structure.y + dy }))
        .find(({ x, y }) => passable(x, y));
      if (!spot) return false;
      putCheese(spot.x, spot.y, 3);
    }
    if ((type === 'rabbit' || type === 'flea') && !spawnAlly(structure)) return false;
    if (type === 'repair') {
      const repair = target => {
        const hp = Math.min(target.maxHp, target.hp + 3);
        if (target.hp < hp) {
          target.hp = hp;
          effect = true;
        }
      };
      nearbyStructures(structure, def.range, repair);
    }
    if (type === 'shield') {
      nearbyStructures(structure, def.range, target => {
        if (target.shieldUntil > time) return;
        const shield = Math.min(Math.ceil(target.maxHp * 0.52), target.shield + 2);
        if (shield > target.shield) effect = true;
        target.shield = shield;
      });
    }
    if (type === 'slime') {
      for (const enemy of enemies.values()) {
        if (distanceSquared(structure, enemy) > def.range ** 2) continue;
        enemy.slowUntil = Math.max(enemy.slowUntil, time + 500);
        effect = true;
      }
    }
    if (type === 'drum') {
      nearbyStructures(structure, def.range, target => { target.bonusUntil = time + 2400; });
      for (const ally of allies.values()) {
        if (distanceSquared(structure, ally) <= def.range ** 2) ally.bonusUntil = time + 2400;
      }
    }
    if (!['slime', 'shield', 'repair'].includes(type) || (effect && time - (structure.abilityFxAt ?? -Infinity) >= 950)) {
      event('activation', structure, { entityId: structure.id, kind: type, radius: def.range });
      structure.abilityFxAt = time;
    }
    return true;
  }

  function stepStructures() {
    for (const structure of structures.values()) {
      if (structure.readyAt > time) continue;
      if (structure.disabledUntil > time) {
        structure.targetId = null;
        structure.windupUntil = 0;
        structure.attackKind = null;
        continue;
      }
      const base = TOWERS[structure.type];
      if (!base.cooldown) continue;
      if (structure.nextAttackAt > time && !structure.windupUntil) continue;
      const def = structure.branch ? { ...base, ...base.upgrades[structure.branch] } : base;
      const cooldown = def.cooldown * (structure.bonusUntil > time ? 0.82 : 1);
      if (!def.damage) {
        const activated = utility(structure, def);
        structure.nextAttackAt = time + (activated ? cooldown : 1100);
        if (activated) structure.lastAttackAt = time;
        continue;
      }
      if (structure.windupUntil) {
        const target = enemies.get(structure.targetId);
        if (target) aim(structure, target, def.kind);
        if (structure.windupUntil > time) continue;
        structure.windupUntil = 0;
        structure.targetId = null;
        structure.attackKind = null;
        if (!target || !canShoot(structure, target, def.kind)) continue;
        discharge(structure, target, def);
        structure.nextAttackAt = time + cooldown;
        continue;
      }
      const target = chooseTarget(structure, def);
      if (!target) {
        structure.nextAttackAt = time + 150;
        continue;
      }
      aim(structure, target, def.kind);
      structure.targetId = target.id;
      structure.attackKind = def.kind || structure.type;
      structure.windupUntil = time + def.windup;
      event('windup', structure, {
        entityId: structure.id, targetId: target.id, targetX: structure.aimX, targetY: structure.aimY,
        kind: structure.attackKind, until: structure.windupUntil, duration: def.windup,
      });
    }
  }

  function stepAllies() {
    for (const ally of allies.values()) {
      ally.vx = 0;
      ally.vy = 0;
      if (ally.expiresAt <= time) {
        allies.delete(ally.id);
        actors.delete(ally.id);
        event('defeat', ally, { entityId: ally.id, kind: ally.type });
        continue;
      }
      const rabbit = ally.type === 'rabbit';
      const range = rabbit ? 5 : 0.95;
      const kind = rabbit ? 'rabbit' : 'flea-bite';
      const cooldown = (rabbit ? 1100 : 725) * (ally.bonusUntil > time ? 0.82 : 1);
      if (ally.windupUntil) {
        const target = enemies.get(ally.attackTarget);
        if (target) trackAttack(ally, target);
        if (ally.windupUntil > time) continue;
        const hit = target && distanceSquared(ally, target) <= (range + 0.37) ** 2 && canShoot(ally, target, kind);
        ally.lastAttackAt = time;
        ally.lastAttackKind = kind;
        ally.nextAttackAt = time + cooldown - (rabbit ? 220 : 135);
        if (rabbit && hit) launch(ally, target, { kind, damage: 9 });
        else event('attack', ally, { ...attackFields(ally, kind), hit: Boolean(hit), duration: 230 });
        if (!rabbit && hit) {
          target.slowUntil = Math.max(target.slowUntil, time + 550);
          damage(target, 4, ally, kind);
        }
        clearAttack(ally);
        continue;
      }
      const enemy = bestWithin(ally, enemies.values(), rabbit ? 7 : 4.5, null,
        candidate => canShoot(ally, candidate, 'rabbit') && distanceSquared(candidate, ally.spawn) <= 8 ** 2);
      if (!enemy) {
        if (distanceSquared(ally, ally.spawn) > 0.6 ** 2) approach(ally, ally.spawn, 1.8, passable);
        continue;
      }
      if (distanceSquared(ally, enemy) > range ** 2) {
        approach(ally, enemy, rabbit ? 2 : 3.3, passable);
        continue;
      }
      if (time < ally.nextAttackAt) continue;
      armAttack(ally, kind, enemy, rabbit ? 220 : 135);
    }
  }

  function step() {
    if (status !== 'active') return;
    tick++;
    time = tick * STEP_MS;
    for (const [id, pickup] of pickups) if (pickup.expiresAt <= time) pickups.delete(id);
    advancePlayers();
    refillMapCheese();
    spawnEnemies();
    stepEnemies();
    stepStructures();
    stepAllies();
    stepProjectiles();
    if (!ranked) advanceCatHealth();
    const survivor = [...team.values()].find(isAlive);
    if (!survivor) status = 'lost';
    else if (cat && !cat.hp) status = 'won';
    if (status !== 'active') {
      resultAt = time;
      event('result', survivor ?? team.values().next().value, { status });
    }
  }

  function snapshot() {
    return {
      matchId, mapId, difficulty, startingWave, size: map.size, time, tick, status, resultAt, wave, nextWaveAt,
      pendingSpawns: pendingSpawns.length,
      players: [...team.values()].map(publicEntity), structures: [...structures.values()].map(publicEntity),
      enemies: [...enemies.values()].map(publicEntity), allies: [...allies.values()].map(publicEntity),
      projectiles: [...projectiles.values()].map(publicEntity), pickups: [...pickups.values()].map(publicEntity),
      objectives: [], events: events.map(entry => entry.type === 'wave'
        ? { ...entry, countByEntrance: { ...entry.countByEntrance } }
        : { ...entry }),
    };
  }

  return Object.freeze({
    command, step, snapshot,
    get status() { return status; },
    get tick() { return tick; },
    get rankedScore() {
      if (!ranked) return null;
      const unfinished = new Set([...enemies.values()].map(enemy => enemy.spawnWave));
      for (const entry of pendingSpawns) unfinished.add(entry.wave);
      return { waves: wave - unfinished.size, timeMs: resultAt ?? time };
    },
  });
}
