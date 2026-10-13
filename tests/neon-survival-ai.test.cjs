'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const AI = require('../game/neon-arena/neon-survival-ai.js');
const actor = (kind = 'blue', x = 0, z = 0) => ({ alien: true, alienKind: kind, x, z, y: 1.86, hp: 100, maxHp: 100, r: .6, onGround: true, alienSpeed: 12, alienDamage: 20, vx: 0, vy: 0, vz: 0, fireCd: 0, color: '#ffffff' });
function setup(overrides = {}) {
  let time = 0, clear = true, revision = 0;
  const human = { entityId: 'p1', x: 16, y: 1.86, z: 0, vx: 3, vz: 0, hp: 100, maxHp: 100 }, aliens = [], warnings = [], hits = [];
  const graph = { nodes: [], links: [], burrowZones: [{ x: 16, z: 0, r: 2 }] };
  const host = { time: () => time, random: () => .2, humans: () => [human], aliens: () => aliens, structures: () => [], lineClear: () => clear,
    pointInfo: () => ({ walkable: true, height: 0, hazard: 0 }), canTraverse: () => true, topAtPoint: () => 0,
    navigation: () => graph, revision: () => revision, warn: (...args) => warnings.push(args), damage: (...args) => hits.push(args), physics: () => {}, bounds: () => 90, ...overrides };
  const controller = AI.createController(host);
  return { controller, host, human, aliens, warnings, hits, graph, tick: value => { time += value; }, visible: value => { clear = value; }, changed: () => revision++ };
}
test('endless director respects existing active cap and finite per-wave budget', () => {
  for (const wave of [1, 5, 10, 35, 50, 75, 100, 150, 200, 10000, 1000000]) {
    const plan = AI.planWave(wave, { map: 'living_hive', random: () => .45 });
    assert.equal(plan.activeCap, 25); assert.ok(plan.total <= 147); assert.equal(plan.order.length, plan.budget.total);
    assert.ok(plan.order.every(kind => AI.SPECIES[kind] ? AI.SPECIES[kind].unlock <= wave : true));
    assert.ok(plan.bosses.length <= 3); if (wave % 5 === 0) assert.ok(plan.bosses.length > 0);
  }
});
test('every new specialist is guaranteed on its introduction wave with bounded statistics', () => {
  for (const [kind, spec] of Object.entries(AI.SPECIES)) {
    assert.ok(AI.planWave(spec.unlock, { random: () => .1 }).order.includes(kind));
    assert.equal(AI.speciesSpec(kind, 1000).hp, AI.speciesSpec(kind, 100000).hp);
  }
  assert.equal(AI.balanceWave(100000), 30); assert.equal(AI.balanceWave(100000, true), 60);
});
test('difficulty and veteran waves change bounded specialty pressure and reinforcement cadence', () => {
  const seeded = () => { let seed = 42; return () => ((seed = (1664525 * seed + 1013904223) >>> 0) / 4294967296); };
  const easy = AI.planWave(100, { difficulty: 'easy', random: seeded() });
  const nightmare = AI.planWave(100, { difficulty: 'nightmare', random: seeded() });
  assert.ok(nightmare.spawnDelay < easy.spawnDelay);
  assert.notDeepEqual(easy.budget, nightmare.budget);
  assert.ok(AI.planWave(200).spawnDelay < AI.planWave(100).spawnDelay);
  assert.equal(AI.planWave(100000).spawnDelay, 2.5);
});
test('map encounter preference is authored and repetition is discouraged', () => {
  const rng = () => .5;
  const ids = new Set(['breach_ring', 'orbital_graveyard', 'living_hive', 'swarm_heart'].map(map => AI.planWave(40, { map, random: rng }).encounter));
  assert.ok(ids.size > 1);
  const first = AI.planWave(40, { random: rng });
  assert.notEqual(AI.planWave(40, { random: rng, history: [first.encounter, first.encounter] }).encounter, first.encounter);
});
test('species receive distinct squad duties in groups no larger than six', () => {
  const aliens = ['red', 'purple', 'green', 'warder', 'blue', 'climber', 'screecher'].map(actor);
  const squads = AI.assignSquad(aliens, { wave: 40 });
  assert.equal(squads.length, 2); assert.ok(squads.every(s => s.members.length <= 6));
  assert.deepEqual(aliens.slice(0, 4).map(a => a.survivalRole), ['breacher', 'suppressor', 'flanker', 'support']);
});
test('lost sight freezes a last-known position and expires instead of tracking through walls', () => {
  const f = setup(), a = actor(); f.aliens.push(a);
  let decision = f.controller.acquire(a); assert.equal(decision.target, f.human); assert.equal(decision.memory.x, 16);
  f.visible(false); f.human.x = 60; f.tick(.6); decision = f.controller.acquire(a);
  assert.equal(decision.target, null); assert.equal(decision.visible, false); assert.equal(decision.memory.x, 16);
  f.tick(10); decision = f.controller.acquire(a); assert.equal(decision.memory, null); assert.equal(decision.tactic, 'patrol');
});
test('a hidden player is never acquired without sight or a communicated squad observation', () => {
  const f = setup(), a = actor(); f.visible(false);
  const decision = f.controller.acquire(a); assert.equal(decision.target, null); assert.equal(decision.memory, null);
  f.visible(true); const observer = actor('purple', 2); AI.assignSquad([a, observer]); f.controller.acquire(observer);
  f.visible(false); const shared = f.controller.acquire(a); assert.equal(shared.memory.x, 16); assert.equal(shared.target, null);
});
test('navigation caches routes and invalidates them when structures change', () => {
  const f = setup({ canTraverse: (from, to) => Math.hypot(from.x - to.x, from.z - to.z) <= 11 });
  f.graph.nodes.push({ id: 'a', x: 0, z: 0, y: 0 }, { id: 'b', x: 10, z: 0, y: 0 }, { id: 'c', x: 20, z: 0, y: 0 });
  f.graph.links.push({ from: 'a', to: 'b', kind: 'walk' }, { from: 'b', to: 'c', kind: 'walk' });
  const a = actor(), goal = { x: 20, z: 0, y: 1.86 };
  assert.ok(f.controller.findPath(a, goal).length); assert.equal(f.controller.stats().pathSearches, 1);
  f.controller.findPath(a, goal); assert.equal(f.controller.stats().pathSearches, 1);
  f.changed(); f.controller.findPath(a, goal); assert.equal(f.controller.stats().pathSearches, 2);
  f.controller.invalidate(); assert.equal(f.controller.stats().cachedPaths, 0);
});
test('ward protection requires a live source, line of sight, range, and active ability', () => {
  const f = setup(), a = actor(), warder = actor('warder', 4); warder.wardUntil = 5; a.wardSource = warder;
  assert.equal(f.controller.damageMultiplier(a), .62);
  f.visible(false); assert.equal(f.controller.damageMultiplier(a), 1); f.visible(true);
  warder.dead = true; assert.equal(f.controller.damageMultiplier(a), 1); warder.dead = false;
  warder.empUntil = 5; assert.equal(f.controller.damageMultiplier(a), 1);
});
test('phase reposition always warns before moving and EMP cancels the commit', () => {
  const f = setup(), a = actor('lurker'); f.aliens.push(a); const decision = f.controller.acquire(a);
  f.controller.updateSpecial(a, 6, decision); assert.equal(f.warnings.length, 1); assert.equal(a.x, 0); assert.equal(a.z, 0);
  f.tick(.5); a.empUntil = 3; f.controller.updateSpecial(a, .5, decision);
  assert.equal(a.x, 0); assert.equal(a.z, 0); assert.equal(a.phaseWarning, null);
});
test('burrowing only uses authored valid locations and closes safely against new builds', () => {
  let blocked = false;
  const f = setup({ pointInfo: (x, z) => ({ walkable: !(blocked && x > 10), height: 0, hazard: 0 }) }), a = actor('stalker');
  f.aliens.push(a); const d = f.controller.acquire(a);
  f.controller.updateSpecial(a, 6, d); assert.equal(f.warnings[0][2], 'crack'); assert.equal(a.x, 0);
  f.tick(1.7); f.controller.updateSpecial(a, 1.7, d); assert.equal(a.burrowed, true);
  blocked = true; f.tick(1); f.controller.updateSpecial(a, 1, d); assert.equal(a.burrowed, false); assert.equal(a.x, 0); assert.equal(f.hits.length, 0);
});
test('artillery lobs after its warning and world geometry can stop the projectile', () => {
  const impacts = [], f = setup({ acid: zone => impacts.push(zone) }), a = actor('artillery'); f.aliens.push(a);
  const d = f.controller.acquire(a); f.controller.updateSpecial(a, 6, d);
  assert.equal(f.warnings[0][2], 'acidArc'); assert.equal(impacts.length, 0);
  f.tick(1.5); f.controller.updateSpecial(a, 1.5, d); assert.equal(impacts.length, 0);
  f.visible(false); f.tick(.4); f.controller.updateSpecial(a, .4, d); f.tick(.4); f.controller.updateSpecial(a, .4, d);
  assert.equal(impacts.length, 0);
});
test('special arrival rejects authored hazards and destructible collision volumes', () => {
  for (const obstacle of [true, false]) {
    const f = setup({ pointInfo: () => ({ walkable: true, height: 0, obstacle, destructible: obstacle, hazard: obstacle ? null : { kind: 'acid', damage: 8 } }) });
    const a = actor('stalker'), d = f.controller.acquire(a); f.controller.updateSpecial(a, 6, d);
    assert.equal(f.warnings.length, 0); assert.equal(a.x, 0);
  }
});
test('equipment holograms distract hunters while bosses and support species prioritize humans', () => {
  const decoy = { id: 'decoy', name: 'HOLOGRAM', human: true, x: 9, y: 1.86, z: 0, hp: 100, maxHp: 100 };
  const f = setup({ decoys: () => [decoy] });
  assert.equal(f.controller.acquire(actor()).target, decoy);
  const boss = actor(); boss.boss = true; boss.bossType = 'rammer';
  assert.equal(f.controller.acquire(boss).target, f.human);
  assert.equal(f.controller.acquire(actor('warder')).target, f.human);
});
test('AI consumes all eight authored layered map graphs without coordinate or height drift', () => {
  const maps = require('../game/neon-arena/neon-survival-maps.js'), base = path.resolve(__dirname, '../game/neon-arena/assets/survival');
  const read = file => JSON.parse(fs.readFileSync(file, 'utf8'));
  for (const { id } of maps.catalog()) {
    const root = path.join(base, 'tilesets', id), blueprint = read(path.join(base, 'maps', id + '.json')), tileset = read(path.join(root, 'tileset.json'));
    const definitions = {};
    for (const item of read(path.join(root, 'prefabs.json')).items) {
      const filename = path.join(root, item.file), definition = read(filename);
      definitions[item.id] = { ...definition, mesh: read(path.resolve(path.dirname(filename), definition.geometry)) };
    }
    maps.hydrateForTest(blueprint, tileset, definitions);
    const structures = [];
    maps.install({
      addPlatform: (x, z, w, d, y, h) => { const p = { type: 'platform', x: x * 1.45, z: z * 1.45, w: w * 1.45, d: d * 1.45, y, h, active: true }; structures.push(p); return p; },
      addWall: (x, z, w, d, h, color, bottom) => { const p = { type: 'wall', x: x * 1.45, z: z * 1.45, w: w * 1.45, d: d * 1.45, y: bottom + h / 2, h, active: true }; structures.push(p); return p; }
    });
    const graph = maps.navigation(), spawn = graph.spawnZones[0], a = actor('green', spawn.x, spawn.z);
    a.y = (spawn.y || 0) + 1.86;
    const f = setup({ navigation: () => graph, structures: () => structures, pointInfo: maps.pointInfo, canTraverse: maps.canTraverse, lineClear: () => false, revision: () => maps.revision });
    const target = blueprint.playerSpawns[0], goal = { x: target.x * 1.45, z: target.z * 1.45, y: (target.y || 0) + 1.86 };
    assert.ok(f.controller.findPath(a, goal).length > 0, id + ': ground route missing');
    const elevated = graph.nodes.reduce((best, node) => node.height > best.height ? node : best, graph.nodes[0]);
    if (elevated.height > 0) {
      const upper = { ...elevated, y: elevated.height + 1.86 };
      assert.ok(f.controller.findPath(a, upper).length > 0, id + ': elevated route missing');
    }
    assert.ok(graph.nodes.length >= 500); assert.ok(f.controller.stats().cachedPaths <= 160);
  }
  maps.dispose();
});
test('production alien movement remains finite and bounded during veteran-wave simulations', () => {
  const source = fs.readFileSync(path.resolve(__dirname, '../game/neon-arena/neon-arena.js'), 'utf8');
  const makeStart = source.indexOf('function makeAlien('), makeEnd = source.indexOf('function spawnAlienAtEdge(', makeStart);
  const updateStart = source.indexOf('function nearestHuman('), updateEnd = source.indexOf('function rechargeBotBuilds(', updateStart);
  assert.ok(makeStart > 0 && updateStart > 0);
  const kinds = ['blue', 'yellow', 'red', 'green', 'stripe', 'purple', 'climber', ...Object.keys(AI.SPECIES)];
  for (const wave of [50, 100, 200, 100000]) {
    let clock = 0;
    const human = { entityId: 'p1', human: true, x: 8, y: 1.86, z: 0, vx: 0, vy: 0, vz: 0, r: .5, hp: 1000000, maxHp: 1000000 }, bots = [], hazards = [];
    const context = {
      window: { NeonSurvivalAI: AI }, PLAYER_H: 1.86, ALIEN_SIZE_MULT: 1.5, ALIEN_MAX_ALIVE: 25, ARENA_SAFE: 64.3, TAU: Math.PI * 2,
      game: { mode: 'survivor', survivalMap: 'xenofall_wastes', survivorWave: wave, survivalElapsed: 0, shopOpen: false },
      walls: [], platforms: [], bots, projectiles: [], WEAPONS: { smg: { damage: 12 } },
      rand: (lo, hi) => (lo + hi) / 2, clamp: (n, lo, hi) => Math.max(lo, Math.min(hi, n)), hypot2: Math.hypot,
      survivorHumans: () => [human], survivorAliveAliens: () => bots, botSeparation: () => ({ x: 0, z: 0 }),
      topAtPoint: () => 0, blocksNear: () => [], lineClear: () => true, survivorBreachRoute: () => null, insideSurvivorRing: () => true,
      tryFire: () => {}, addMuzzleFlash: () => {}, burst: () => {}, feed: () => {}, msg: () => {}, damageStructure: () => {},
      damage: (h, amount) => { h.hp -= amount; },
      physics: (a, dt) => { a.x += a.vx * dt; a.z += a.vz * dt; a.vy -= 34 * dt; a.y += a.vy * dt; if (a.y <= 1.86) { a.y = 1.86; a.vy = 0; a.onGround = true; } }
    };
    context.survivalController = AI.createController({ time: () => clock, humans: () => [human], aliens: () => bots, structures: () => [],
      lineClear: context.lineClear, physics: context.physics, damage: context.damage, topAtPoint: context.topAtPoint,
      pointInfo: () => ({ walkable: true, height: 0, hazard: 0 }), canTraverse: () => true,
      navigation: () => ({ nodes: [], links: [], burrowZones: [{ x: 8, z: 0, r: 2 }] }),
      acid: zone => { if (hazards.length === 12) hazards.shift(); hazards.push(zone); }, warn: () => {}, burst: () => {}, random: () => .4 });
    vm.createContext(context); vm.runInContext(source.slice(makeStart, makeEnd) + '\n' + source.slice(updateStart, updateEnd), context);
    for (let i = 0; i < 25; i++) {
      const a = context.makeAlien(kinds[i % kinds.length], wave), angle = i * 2.4;
      a.x = Math.cos(angle) * 28; a.z = Math.sin(angle) * 28; a.onGround = true; bots.push(a);
    }
    AI.assignSquad(bots, { wave });
    for (let frame = 0; frame < 480; frame++) {
      const dt = 1 / 60; clock += dt; context.game.survivalElapsed = clock;
      for (const a of bots) context.updateAlien(a, dt);
      context.projectiles.length = 0; // Existing projectile lifetime update is outside this AI fixture.
      for (let i = hazards.length - 1; i >= 0; i--) if ((hazards[i].life -= dt) <= 0) hazards.splice(i, 1);
    }
    assert.equal(bots.length, 25); assert.ok(hazards.length <= 12);
    assert.ok(bots.every(a => Number.isFinite(a.x + a.y + a.z + a.vx + a.vy + a.vz)), 'wave ' + wave + ' produced an invalid actor');
    assert.ok(bots.every(a => a.maxHp < 1000), 'ordinary HP should remain shootable at wave ' + wave);
    assert.ok(context.survivalController.stats().cachedPaths <= 160);
    assert.ok(human.hp < human.maxHp, 'real production melee should reach the survivor');
  }
});
