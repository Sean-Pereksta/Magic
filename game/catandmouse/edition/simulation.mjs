import { DIFFICULTIES, ENEMIES, MAPS, SHOTS, STEP_MS, TOWERS, normalizeLoadout } from './battlefield.mjs';
import { DIRECTIONS, buildEnemyField, distanceSquared, findPath, interceptPoint, movingCircleHit, terrainHit, tileAt } from './navigation.mjs';

export { DIFFICULTIES, ENEMIES, MAPS, SHOTS, STARTER_LOADOUT, STEP_MS, TOWERS, isCompleteLoadout, normalizeLoadout } from './battlefield.mjs';

const STEP_SECONDS = STEP_MS / 1000;
const WAVE_SIZES = Object.freeze([2, 5, 8, 12, 17, 24, 26]);
const MAX_STRUCTURES = 420;
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

function publicEntity(entity) {
  const {
    path, destination, inputX, inputY, previousX, previousY, aiCell, nextAttackAt,
    targetId, producerId, expiresAt, shieldUntil, attackTarget, specialTimes, spawn,
    damage, splash, bonusUntil, bornTick, laneOffsetX, laneOffsetY, charge, abilityFxAt,
    ...publicFields
  } = entity;
  return entity.loadout ? { ...publicFields, loadout: entity.loadout.slice() } : publicFields;
}

export function createSimulation({ matchId, mapId = 'flour-mill', difficulty = 'normal', players, seed = matchId } = {}) {
  if (typeof matchId !== 'string' || !matchId.length) throw new TypeError('A match ID is required.');
  if (!Object.hasOwn(MAPS, mapId)) throw new RangeError('Unknown battlefield.');
  if (!Object.hasOwn(DIFFICULTIES, difficulty)) throw new RangeError('Unknown difficulty.');
  if (!Array.isArray(players) || players.length < 1 || players.length > 4
    || players.some(player => !player || typeof player.id !== 'string' || !player.id.length)
    || new Set(players.map(player => player.id)).size !== players.length) {
    throw new TypeError('One to four unique players are required.');
  }

  const map = MAPS[mapId];
  const rules = DIFFICULTIES[difficulty];
  const occupiedIds = new Set(map.objectives.map(objective => objective.id));
  if (players.some(({ id }) => occupiedIds.has(id) || /^(?:structure|enemy|ally|shot|cheese)-\d+$/.test(id))) {
    throw new TypeError('A player ID conflicts with a battlefield entity.');
  }
  const random = createRandom(seed);
  const actors = new Map();
  const team = new Map(players.map(({ id, name, loadout }, index) => {
    const spawn = map.playerSpawns[index];
    const actor = {
      id, name: String(name || `Mouse ${index + 1}`), color: index,
      ...spawn, spawn, facing: -Math.PI / 2, vx: 0, vy: 0, inputX: 0, inputY: 0,
      maxHp: 62, hp: 62, cheese: rules.openingCheese, loadout: normalizeLoadout(loadout),
      reviveAt: 0, lastHitAt: -1000000, path: [], destination: null,
    };
    return [id, actor];
  }));
  const structures = new Map();
  const enemies = new Map();
  const allies = new Map();
  const projectiles = new Map();
  const pickups = new Map();
  const objectives = map.objectives.map(def => ({ ...def, hp: def.maxHp, lastHitAt: -1000000 }));
  const objectiveCells = new Map(objectives.map(objective => [objective.y * map.size + objective.x, objective]));
  const events = [];
  const pendingSpawns = [];
  let nextEventId = 1;
  let nextEntityId = 1;
  let tick = 0;
  let time = 0;
  let status = 'active';
  let resultAt = null;
  let wave = 0;
  let nextWaveAt = 12500;
  let bossDefeated = false;
  let field = null;

  function event(type, source, fields) {
    events.push({ id: nextEventId++, type, tick, time, x: source.x, y: source.y, ...fields });
    if (events.length > MAX_EVENTS) events.shift();
  }

  function cellAt(x, y) {
    return Math.round(y) * map.size + Math.round(x);
  }

  function passable(x, y) {
    const tile = tileAt(map, x, y);
    if (tile === '#' || tile === '~') return false;
    const cell = cellAt(x, y);
    return !structures.has(cell) && !objectiveCells.get(cell)?.hp;
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
    if (!passable(destination.x, destination.y)) return refuse('blocked');
    const route = findPath(actor, destination, map.size, passable);
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
    const hasAxis = type === 'wall' || type === 'barricade';
    const axis = action.axis === undefined ? 'x' : action.axis;
    if (hasAxis && axis !== 'x' && axis !== 'y') return refuse('invalid-axis');
    if (!passable(action.x, action.y) || map.spawns.some(spawn => distanceSquared(spawn, action) < 4)
      || map.playerSpawns.some(spawn => distanceSquared(spawn, action) === 0)) return refuse('blocked');
    if (structures.size >= MAX_STRUCTURES) return refuse('structure-limit');
    if (actors.size && bestWithin(action, actors.values(), 0.72)) return refuse('occupied');
    if (actor.cheese < def.cost) return refuse('not-enough-cheese');
    if ((type === 'generator' || type === 'farm') && [...structures.values()].filter(
      structure => structure.type === type && structure.ownerId === actor.id,
    ).length >= (type === 'generator' ? 6 : 4)) return refuse('economy-limit');

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
    if (!Number.isInteger(action.x) || !Number.isInteger(action.y)
      || action.x <= 0 || action.x >= map.size - 1 || action.y <= 0 || action.y >= map.size - 1) return refuse('invalid-cell');
    if (distanceSquared(actor, action) > BUILD_RANGE ** 2) return refuse('out-of-reach');
    return action.type === 'build' ? build(actor, action) : upgrade(actor, action);
  }

  function putCheese(x, y, value, lifespan = 48000) {
    if (pickups.size >= 90) return;
    const pickup = { id: `cheese-${nextEntityId++}`, x, y, value, expiresAt: time + lifespan };
    pickups.set(pickup.id, pickup);
  }

  function advancePlayers() {
    for (const actor of team.values()) {
      actor.previousX = actor.x;
      actor.previousY = actor.y;
      if (!actor.hp) {
        if (actor.reviveAt <= time) {
          const safe = passable(actor.spawn.x, actor.spawn.y) ? actor.spawn
            : map.playerSpawns.find(({ x, y }) => passable(x, y)) ?? actor.spawn;
          Object.assign(actor, safe, { previousX: safe.x, previousY: safe.y, hp: actor.maxHp, reviveAt: 0 });
          event('revive', actor, { entityId: actor.id });
        }
        continue;
      }
      if (actor.path.length && !passable(actor.path[0].x, actor.path[0].y)) {
        actor.path = findPath(actor, actor.destination, map.size, passable) ?? [];
      }
      const waypoint = actor.path[0];
      if (waypoint) {
        approach(actor, waypoint, 4.1, passable);
        if (distanceSquared(actor, waypoint) < 0.000001) actor.path.shift();
      } else {
        moveEntity(actor, actor.inputX * 4.1 * STEP_SECONDS, actor.inputY * 4.1 * STEP_SECONDS, passable);
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
  for (const objective of objectives) actors.set(objective.id, objective);
  for (const cheese of map.cheese) putCheese(cheese.x, cheese.y, 3, Infinity);

  function beginWave() {
    wave++;
    const count = Math.min(48, WAVE_SIZES[wave - 1] + (team.size - 1) * (1 + Math.floor(wave / 2))
      + Math.floor(rules.waveBonus * (wave - 1) / 6));
    const firstLane = Math.floor(random() * map.spawns.length);
    for (let index = 0; index < count; index++) {
      const type = wave >= 3 && index % 5 === 4 ? 'runner' : 'rat';
      pendingSpawns.push({ time: time + 550 * Math.floor(index / 3), type, lane: (firstLane + index) % map.spawns.length });
    }
    const specials = [[2, 'stink'], [3, 'vulture'], [4, 'ox'], [5, 'king'], [6, 'stink'], [7, 'ox']];
    for (const [minimum, type] of specials) {
      if (wave < minimum) continue;
      pendingSpawns.push({ time: time + minimum * 650, type, lane: (minimum + firstLane) % map.spawns.length });
    }
    if (wave >= 6) {
      const groups = rules.catSweep ? 2 : 1;
      for (let group = 0; group < groups; group++) {
        const lane = (firstLane + group * 2 + 1) % map.spawns.length;
        pendingSpawns.push({ time: time + 3550 + group * 2000, type: 'sapper', lane });
        pendingSpawns.push({ time: time + 4650 + group * 2000, type: 'mender', lane });
      }
    }
    if (wave === WAVE_SIZES.length) pendingSpawns.push({ time: time + 3400, type: 'cat', lane: firstLane });
    pendingSpawns.sort((a, b) => a.time - b.time);
    nextWaveAt = wave === WAVE_SIZES.length ? null : time + (wave < 3 ? 27000 : 30000);
    for (const mouse of team.values()) mouse.cheese += 2 + Math.floor(wave / 2);
    const countByEntrance = Object.fromEntries(map.spawns.map((spawn, lane) =>
      [spawn.id, pendingSpawns.filter(entry => entry.lane === lane).length],
    ));
    event('wave', map.spawns[firstLane], { wave, count: pendingSpawns.length, nextWaveAt, countByEntrance });
  }

  function spawnEnemies() {
    if (nextWaveAt !== null && time >= nextWaveAt) beginWave();
    while (pendingSpawns.length && pendingSpawns[0].time <= time && enemies.size < MAX_ENEMIES) {
      const { type, lane } = pendingSpawns.shift();
      const entrance = map.spawns[lane];
      const { x, y } = entrance;
      const def = ENEMIES[type];
      const playerBonus = type === 'cat' ? 110 * (team.size - 1) : 0;
      const hp = Math.round((def.hp + playerBonus) * rules.health);
      const laneOffsetX = (random() - 0.5) * 0.72;
      const laneOffsetY = (random() - 0.5) * 0.72;
      const enemy = {
        id: `enemy-${nextEntityId++}`, type, x: x + laneOffsetX, y: y + laneOffsetY, laneOffsetX, laneOffsetY,
        hp, maxHp: hp, vx: 0, vy: 0, facing: 0,
        spawnedAt: time, lastAttackAt: -1000000, lastHitAt: -1000000, nextAttackAt: 0,
        windupUntil: 0, slowUntil: 0, trapUntil: 0,
        specialTimes: type === 'cat' ? { pounce: time - 3200 } : {},
        attackTarget: null, attackKind: null, lastAttackKind: null, aiCell: null,
      };
      enemies.set(enemy.id, enemy);
      actors.set(enemy.id, enemy);
      event('spawn', enemy, { entityId: enemy.id, kind: type, entrance: entrance.id });
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
    event('defeat', target, { entityId: target.id, sourceId: source.id, kind: target.type || (team.has(target.id) ? 'mouse' : 'objective') });
    if (team.has(target.id)) {
      Object.assign(target, {
        inputX: 0, inputY: 0, vx: 0, vy: 0, reviveAt: time + 9500, path: [], destination: null,
      });
      return;
    }
    if (enemies.delete(target.id)) {
      putCheese(target.x, target.y, ENEMIES[target.type].cheese);
      if (target.type === 'cat') bossDefeated = true;
    }
    if (structures.get(cellAt(target.x, target.y)) === target) {
      structures.delete(cellAt(target.x, target.y));
      field = null;
      event('destroy', target, { entityId: target.id, kind: target.type });
    }
    if (objectiveCells.get(cellAt(target.x, target.y)) === target) field = null;
    allies.delete(target.id);
    actors.delete(target.id);
  }

  const clearPath = (origin, target) =>
    terrainHit(map, origin.x, origin.y, target.x, target.y, (x, y) => !passable(x, y)) === null;
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

  function nearbyDefender(enemy, range) {
    return bestWithin(enemy, team.values(), range, null, target => isAlive(target) && clearTerrain(enemy, target))
      ?? bestWithin(enemy, structures.values(), range, null, target => isAlive(target) && clearTerrain(enemy, target))
      ?? bestWithin(enemy, objectives, range, null, target => isAlive(target) && clearTerrain(enemy, target));
  }

  function chooseSpecial(enemy) {
    if (time < enemy.nextAttackAt) return false;
    if (enemy.type === 'cat') {
      const charge = SPECIAL_MOVES['cat-charge'];
      const sweep = SPECIAL_MOVES['tail-sweep'];
      if (rules.catCharge) {
        const target = nearbyDefender(enemy, charge.range);
        if (target && distanceSquared(enemy, target) >= 2.45 ** 2 && startSpecial(enemy, 'cat-charge', target)) return true;
      }
      if (rules.catSweep && nearbyDefender(enemy, sweep.range) && startSpecial(enemy, 'tail-sweep', position(enemy))) return true;
      const pounce = bestWithin(enemy, team.values(), SPECIAL_MOVES.pounce.range, null,
        target => isAlive(target) && clearPath(enemy, target));
      return startSpecial(enemy, 'pounce', pounce);
    }
    if (enemy.type === 'sapper') {
      const def = SPECIAL_MOVES['powder-blast'];
      const target = bestWithin(enemy, structures.values(), def.range,
        (structure, distance) => distance - (TOWERS[structure.type].role === 'Defense' ? 0 : 0.8),
        structure => isAlive(structure) && clearTerrain(enemy, structure));
      return startSpecial(enemy, 'powder-blast', target);
    }
    if (enemy.type === 'mender') {
      const def = SPECIAL_MOVES.mend;
      const target = bestWithin(enemy, enemies.values(), def.range,
        (rat, distance) => distance - Math.min(def.heal, rat.maxHp - rat.hp) * 0.75,
        rat => rat !== enemy && rat.type !== 'cat' && rat.hp <= rat.maxHp - 4 && clearTerrain(enemy, rat));
      return startSpecial(enemy, 'mend', target);
    }
    return false;
  }

  function hurtArea(enemy, center, kind, def) {
    const targets = [...team.values(), ...structures.values(), ...allies.values(), ...objectives];
    for (const target of targets) {
      if (distanceSquared(center, target) > def.radius ** 2 || !clearTerrain(center, target)) continue;
      const structure = structures.get(cellAt(target.x, target.y)) === target;
      const scale = kind === 'powder-blast' && !structure ? 0.65 : 1;
      damage(target, Math.ceil(def.damage * scale * rules.damage), enemy, kind);
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
    for (const target of [...team.values(), ...structures.values(), ...allies.values(), ...objectives]) {
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
        if (distanceSquared(mouse, enemy) <= def.radius ** 2) damage(mouse, Math.ceil(def.damage * rules.damage), enemy, kind);
      }
      return;
    }
    event('attack', enemy, fields);
    if (kind === 'mend') {
      const wounded = [...enemies.values()].filter(rat => rat !== enemy && rat.type !== 'cat' && rat.hp < rat.maxHp
        && distanceSquared(rat, target) <= def.radius ** 2 && clearTerrain(target, rat));
      wounded.sort((a, b) => (b.maxHp - b.hp) - (a.maxHp - a.hp));
      for (const rat of wounded.slice(0, 3)) {
        const healed = Math.min(def.heal, rat.maxHp - rat.hp);
        rat.hp += healed;
        event('heal', rat, { kind, entityId: rat.id, sourceId: enemy.id, value: healed });
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
      const hit = Boolean(target?.hp && distanceSquared(enemy, target) <= (def.reach + 0.46) ** 2);
      if (target) trackAttack(enemy, target);
      event('attack', enemy, { ...attackFields(enemy, kind), hit, duration: 290 });
      if (hit) {
        const supported = enemy.type !== 'king' && bestWithin(enemy, enemies.values(), 3.1, null, candidate => candidate.type === 'king');
        const breach = enemy.type === 'ox' && structures.get(cellAt(target.x, target.y)) === target ? 1.45 : 1;
        const amount = Math.ceil(def.damage * rules.damage * (supported ? 1.3 : 1) * breach);
        damage(target, amount, enemy, kind);
      }
    }
    clearAttack(enemy);
  }

  function windup(enemy, target) {
    if (time >= enemy.nextAttackAt) armAttack(enemy, MELEE_MOVES[enemy.type], target, ENEMIES[enemy.type].windup);
  }

  function stepEnemies() {
    const routes = field ??= buildEnemyField(map, objectives, structures);
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
      if (chooseSpecial(enemy)) continue;

      const mouseRange = enemy.type === 'vulture' ? 5.3 : enemy.type === 'cat' ? 4.6 : 2.1;
      const nearbyMouse = bestWithin(enemy, team.values(), mouseRange);
      const nearbyAlly = bestWithin(enemy, allies.values(), 1.2);
      const localTarget = nearbyMouse ?? nearbyAlly;
      const speed = def.speed * (enemy.slowUntil > time ? 0.52 : 1);
      if (localTarget && distanceSquared(enemy, localTarget) <= (def.reach + 0.19) ** 2) {
        windup(enemy, localTarget);
        continue;
      }
      if (nearbyMouse && clearPath(enemy, nearbyMouse)) {
        enemy.aiCell = null;
        approach(enemy, nearbyMouse, speed, passable);
        continue;
      }

      // Finish the current tile before changing axes to prevent corner oscillation.
      const occupiedCell = cellAt(enemy.x, enemy.y);
      const destinationCell = enemy.aiCell ?? routes[occupiedCell];
      if (destinationCell === undefined || destinationCell < 0) continue;
      const destination = {
        x: destinationCell % map.size + enemy.laneOffsetX,
        y: Math.floor(destinationCell / map.size) + enemy.laneOffsetY,
      };
      const barrier = structures.get(destinationCell) ?? objectiveCells.get(destinationCell);
      if (barrier?.hp && distanceSquared(enemy, barrier) <= (def.reach + 0.31) ** 2) {
        windup(enemy, barrier);
        continue;
      }
      enemy.aiCell = destinationCell;
      approach(enemy, destination, speed, passable);
      if (distanceSquared(enemy, destination) < 0.000001) enemy.aiCell = null;
    }

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
        target.trapUntil = Math.max(target.trapUntil, time + (boss ? 550 : 1380));
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
      for (const objective of objectives) {
        if (objective.hp && distanceSquared(structure, objective) <= def.range ** 2) repair(objective);
      }
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
    spawnEnemies();
    stepEnemies();
    stepStructures();
    stepAllies();
    stepProjectiles();
    if (bossDefeated) status = 'won';
    else if (!objectives.some(isAlive)) status = 'lost';
    if (status !== 'active') {
      resultAt = time;
      event('result', objectives[0], { status });
    }
  }

  function snapshot() {
    return {
      matchId, mapId, difficulty, size: map.size, time, tick, status, resultAt, wave, nextWaveAt,
      pendingSpawns: pendingSpawns.length,
      players: [...team.values()].map(publicEntity), structures: [...structures.values()].map(publicEntity),
      enemies: [...enemies.values()].map(publicEntity), allies: [...allies.values()].map(publicEntity),
      projectiles: [...projectiles.values()].map(publicEntity), pickups: [...pickups.values()].map(publicEntity),
      objectives: objectives.map(publicEntity), events: events.map(entry => entry.type === 'wave'
        ? { ...entry, countByEntrance: { ...entry.countByEntrance } }
        : { ...entry }),
    };
  }

  return Object.freeze({
    command, step, snapshot,
    get status() { return status; },
    get tick() { return tick; },
  });
}
