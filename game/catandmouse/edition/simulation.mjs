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
    targetId, producerId, expiresAt, shieldUntil, attackTarget, lastPounceAt, spawn,
    attackKind, damage, splash, bonusUntil, bornTick, laneOffsetX, laneOffsetY,
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
      if (!actor.hp) {
        if (actor.reviveAt <= time) {
          const safe = passable(actor.spawn.x, actor.spawn.y) ? actor.spawn
            : map.playerSpawns.find(({ x, y }) => passable(x, y)) ?? actor.spawn;
          Object.assign(actor, safe, { hp: actor.maxHp, reviveAt: 0 });
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
        windupUntil: 0, slowUntil: 0, trapUntil: 0, lastPounceAt: time - 3200,
        attackTarget: null, attackKind: null, aiCell: null,
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

  function pounce(enemy) {
    const target = bestWithin(enemy, team.values(), 4.8);
    if (!target || terrainHit(map, enemy.x, enemy.y, target.x, target.y, (x, y) => !passable(x, y)) !== null) return false;
    const landing = position(target);
    enemy.lastPounceAt = time;
    enemy.attackKind = 'pounce';
    enemy.attackTarget = landing;
    enemy.windupUntil = time + 760;
    setFacing(enemy, landing);
    event('windup', enemy, { entityId: enemy.id, targetId: target.id, targetX: landing.x, targetY: landing.y, kind: 'pounce', until: enemy.windupUntil });
    return true;
  }

  function contact(enemy) {
    const def = ENEMIES[enemy.type];
    enemy.lastAttackAt = time;
    enemy.nextAttackAt = time + def.cooldown;
    if (enemy.attackKind === 'pounce') {
      const land = enemy.attackTarget;
      const clear = terrainHit(map, enemy.x, enemy.y, land.x, land.y, (x, y) => !passable(x, y));
      if (clear === null) Object.assign(enemy, land, { previousX: land.x, previousY: land.y });
      for (const mouse of team.values()) {
        if (distanceSquared(mouse, enemy) <= 1.5 ** 2) damage(mouse, Math.ceil(22 * rules.damage), enemy, 'pounce');
      }
      event('attack', enemy, { entityId: enemy.id, kind: 'pounce', targetX: land.x, targetY: land.y });
    } else {
      const target = actors.get(enemy.attackTarget);
      if (target?.hp && distanceSquared(enemy, target) <= (def.reach + 0.46) ** 2) {
        const supported = enemy.type !== 'king' && bestWithin(enemy, enemies.values(), 3.1, null, candidate => candidate.type === 'king');
        const breach = enemy.type === 'ox' && structures.get(cellAt(target.x, target.y)) === target ? 1.45 : 1;
        const amount = Math.ceil(def.damage * rules.damage * (supported ? 1.3 : 1) * breach);
        damage(target, amount, enemy, 'claw');
        event('attack', enemy, { entityId: enemy.id, targetId: target.id, kind: 'claw', targetX: target.x, targetY: target.y });
      }
    }
    enemy.windupUntil = 0;
    enemy.attackTarget = null;
    enemy.attackKind = null;
  }

  function windup(enemy, target) {
    if (time < enemy.nextAttackAt) return;
    enemy.attackKind = 'claw';
    enemy.attackTarget = target.id;
    enemy.windupUntil = time + ENEMIES[enemy.type].windup;
    setFacing(enemy, target);
    event('windup', enemy, { entityId: enemy.id, targetId: target.id, targetX: target.x, targetY: target.y, kind: 'claw', until: enemy.windupUntil });
  }

  function stepEnemies() {
    const routes = field ??= buildEnemyField(map, objectives, structures);
    for (const enemy of enemies.values()) {
      const def = ENEMIES[enemy.type];
      enemy.previousX = enemy.x;
      enemy.previousY = enemy.y;
      enemy.vx = 0;
      enemy.vy = 0;
      if (enemy.trapUntil > time) continue;
      if (enemy.windupUntil) {
        if (enemy.windupUntil <= time) contact(enemy);
        continue;
      }
      if (enemy.type === 'cat' && time >= enemy.nextAttackAt && time - enemy.lastPounceAt >= 6300 && pounce(enemy)) continue;

      const mouseRange = enemy.type === 'vulture' ? 5.3 : enemy.type === 'cat' ? 4.6 : 2.1;
      const nearbyMouse = bestWithin(enemy, team.values(), mouseRange);
      const nearbyAlly = bestWithin(enemy, allies.values(), 1.2);
      const localTarget = nearbyMouse ?? nearbyAlly;
      const speed = def.speed * (enemy.slowUntil > time ? 0.52 : 1);
      if (localTarget && distanceSquared(enemy, localTarget) <= (def.reach + 0.19) ** 2) {
        windup(enemy, localTarget);
        continue;
      }
      if (nearbyMouse && terrainHit(map, enemy.x, enemy.y, nearbyMouse.x, nearbyMouse.y, (x, y) => !passable(x, y)) === null) {
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
        for (const dx of [-2, -1, 0, 1, 2]) {
          for (const dy of [-2, -1, 0, 1, 2]) {
            const structure = structures.get(cellAt(enemy.x + dx, enemy.y + dy));
            if (!structure || TOWERS[structure.type].role === 'Defense' || distanceSquared(enemy, structure) > 2.05 ** 2) continue;
            if (structure.disabledUntil <= time) event('disrupt', structure, { entityId: structure.id, sourceId: enemy.id });
            structure.disabledUntil = time + 850;
          }
        }
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
      targetX: aimX, targetY: aimY, vx: shot.vx, vy: shot.vy, lifetime: def.lifetime,
    });
  }

  function impact(shot, target, reason) {
    projectiles.delete(shot.id);
    event('impact', shot, {
      kind: shot.kind, projectileId: shot.id, shooterId: shot.shooterId,
      targetId: target?.id ?? null, reason,
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

  function aim(shooter, target) {
    shooter.aimX = target.x;
    shooter.aimY = target.y;
    setFacing(shooter, target);
  }

  function discharge(structure, target, def) {
    structure.lastAttackAt = time;
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
      event('attack', previous, { entityId: structure.id, kind, targetId: enemy.id, targetX: enemy.x, targetY: enemy.y });
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
    if (type === 'generator') team.get(structure.ownerId).cheese++;
    if (type === 'farm') {
      const spot = DIRECTIONS.map(([dx, dy]) => ({ x: structure.x + dx, y: structure.y + dy }))
        .find(({ x, y }) => passable(x, y));
      if (!spot) return false;
      putCheese(spot.x, spot.y, 3);
    }
    if ((type === 'rabbit' || type === 'flea') && !spawnAlly(structure)) return false;
    if (type === 'repair') {
      const repair = target => { target.hp = Math.min(target.maxHp, target.hp + 3); };
      nearbyStructures(structure, def.range, repair);
      for (const objective of objectives) {
        if (objective.hp && distanceSquared(structure, objective) <= def.range ** 2) repair(objective);
      }
    }
    if (type === 'shield') {
      nearbyStructures(structure, def.range, target => {
        if (target.shieldUntil > time) return;
        target.shield = Math.min(Math.ceil(target.maxHp * 0.52), target.shield + 2);
      });
    }
    if (type === 'slime') {
      for (const enemy of enemies.values()) {
        if (distanceSquared(structure, enemy) <= def.range ** 2) enemy.slowUntil = Math.max(enemy.slowUntil, time + 500);
      }
    }
    if (type === 'drum') {
      nearbyStructures(structure, def.range, target => { target.bonusUntil = time + 2400; });
      for (const ally of allies.values()) {
        if (distanceSquared(structure, ally) <= def.range ** 2) ally.bonusUntil = time + 2400;
      }
    }
    if (type !== 'slime' && type !== 'shield') event('activation', structure, { entityId: structure.id, kind: type });
    return true;
  }

  function stepStructures() {
    for (const structure of structures.values()) {
      if (structure.readyAt > time) continue;
      if (structure.disabledUntil > time) {
        structure.targetId = null;
        structure.windupUntil = 0;
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
        if (target) aim(structure, target);
        if (structure.windupUntil > time) continue;
        structure.windupUntil = 0;
        structure.targetId = null;
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
      aim(structure, target);
      structure.targetId = target.id;
      structure.windupUntil = time + def.windup;
      event('windup', structure, {
        entityId: structure.id, targetX: target.x, targetY: target.y, kind: def.kind || structure.type, until: structure.windupUntil,
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
      const enemy = bestWithin(ally, enemies.values(), rabbit ? 7 : 4.5, null,
        candidate => canShoot(ally, candidate, 'rabbit') && distanceSquared(candidate, ally.spawn) <= 8 ** 2);
      if (!enemy) {
        if (distanceSquared(ally, ally.spawn) > 0.6 ** 2) approach(ally, ally.spawn, 1.8, passable);
        continue;
      }
      const cooldown = (rabbit ? 1100 : 725) * (ally.bonusUntil > time ? 0.82 : 1);
      if (distanceSquared(ally, enemy) > range ** 2) {
        approach(ally, enemy, rabbit ? 2 : 3.3, passable);
        continue;
      }
      if (time < ally.nextAttackAt) continue;
      aim(ally, enemy);
      ally.lastAttackAt = time;
      ally.nextAttackAt = time + cooldown;
      if (rabbit) launch(ally, enemy, { kind: 'rabbit', damage: 9 });
      else {
        enemy.slowUntil = Math.max(enemy.slowUntil, time + 550);
        damage(enemy, 4, ally, 'claw');
        event('attack', ally, { entityId: ally.id, kind: 'claw', targetId: enemy.id, targetX: enemy.x, targetY: enemy.y });
      }
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
