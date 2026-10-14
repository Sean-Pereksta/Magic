'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const AI = require('../game/neon-arena/neon-tactical-ai.js');
const actor = (id, x = 0, z = 0, team = 1) => ({ entityId: id, x, z, y: 1.86, team, yaw: 0, faceYaw: 0, r: .52, hp: 100, maxHp: 100, vx: 0, vy: 0, vz: 0, onGround: true, style: 'rifler' });
function intersects(a, b, box, pad = 0) {
  if (box.active === false) return false;
  let lo = 0, hi = 1;
  for (const axis of ['x', 'z']) {
    const half = (axis === 'x' ? box.w : box.d) / 2 + pad, delta = b[axis] - a[axis];
    if (Math.abs(delta) < 1e-9) { if (a[axis] < box[axis] - half || a[axis] > box[axis] + half) return false; continue; }
    const p = (box[axis] - half - a[axis]) / delta, q = (box[axis] + half - a[axis]) / delta;
    lo = Math.max(lo, Math.min(p, q)); hi = Math.min(hi, Math.max(p, q));
    if (lo > hi) return false;
  }
  return hi >= 0 && lo <= 1;
}
function fixture(options = {}) {
  let time = 0, revision = 0, clear = true;
  const bot = actor('bot'), target = actor('p2', 0, 22, 2), actors = [bot, target], nav = options.nav || [], blocks = options.blocks || [];
  const host = { time: () => time, random: () => .5, actors: () => actors, sameTeam: (a, b) => a.team === b.team, difficulty: () => 'normal', navigation: () => nav,
    revision: () => revision, lineClear: (ax, ay, az, bx, by, bz) => clear && !blocks.some(b => intersects({ x: ax, z: az }, { x: bx, z: bz }, b)),
    pointInfo: (x, z) => ({ walkable: !blocks.some(b => b.active !== false && Math.abs(x - b.x) <= b.w / 2 + .58 && Math.abs(z - b.z) <= b.d / 2 + .58), height: 0 }),
    canTraverse: (a, b) => !blocks.some(box => intersects(a, b, box, .58)), coverBlocks: () => blocks, idealRange: () => 24, ...options.host };
  return { bot, target, actors, blocks, nav, host, controller: AI.createController(host), tick: dt => { time += dt; }, visible: value => { clear = value; }, changed: () => revision++ };
}
test('the existing five loadouts have five distinct tactical personalities', () => {
  assert.deepEqual(['rusher', 'skirmisher', 'heavy', 'marksman', 'rifler'].map(style => AI.profileName({ style })), ['aggressor', 'flanker', 'defender', 'marksman', 'balanced']);
  assert.equal(Object.keys(AI.PROFILES).length, 5);
  assert.ok(AI.PROFILES.flanker.flank > AI.PROFILES.defender.flank);
  assert.ok(AI.PROFILES.marksman.range > AI.PROFILES.aggressor.range);
});
test('bots do not acquire a hidden local second player or an opponent behind their field of view', () => {
  const f = fixture(); f.visible(false);
  const hidden = f.controller.update(f.bot, .016);
  assert.equal(hidden.enemy, null); assert.equal(hidden.memory, null); assert.equal(hidden.aim, null);
  f.visible(true); f.target.z = -22; f.tick(.2);
  assert.equal(f.controller.update(f.bot, .016).enemy, null);
  f.target.z = 22; f.tick(.2);
  assert.equal(f.controller.update(f.bot, .016).enemy, f.target, 'P2 participates in the same honest perception');
});
test('reaction delays stay finite and harder bots respond faster without immediate shots', () => {
  for (const difficulty of ['easy', 'normal', 'hard', 'nightmare']) {
    const f = fixture({ host: { difficulty: () => difficulty } });
    let d = f.controller.update(f.bot, .016); assert.equal(d.aim.fire, false);
    for (let i = 0; i < 100 && !d.aim.fire; i++) { f.tick(.01); d = f.controller.update(f.bot, .01); }
    assert.equal(d.aim.reactionReady, true);
    assert.ok(AI.DIFFICULTY[difficulty].reaction >= .14);
  }
  assert.ok(AI.DIFFICULTY.nightmare.reaction < AI.DIFFICULTY.easy.reaction);
});
test('breaking sight immediately stops shooting and freezes then expires the last observation', () => {
  const f = fixture(); f.controller.update(f.bot, .016); f.tick(.6);
  assert.equal(f.controller.update(f.bot, .016).aim.fire, true);
  f.visible(false); f.target.x = 50; f.target.z = 60;
  let d = f.controller.update(f.bot, .016);
  assert.equal(d.enemy, null); assert.equal(d.aim, null); assert.equal(d.memory.x, 0); assert.equal(d.memory.z, 22);
  f.tick(8); d = f.controller.update(f.bot, .016); assert.equal(d.memory, null);
});
test('gunfire hearing stores a noisy shot position, never a live hidden actor', () => {
  const f = fixture(); f.visible(false);
  const shot = { x: 8, y: 1.86, z: 12, team: 2, source: 'p2', range: 38 };
  f.controller.hear(shot); shot.x = 900; f.target.x = 70;
  const d = f.controller.update(f.bot, .016);
  assert.equal(d.enemy, null); assert.equal(d.aim, null); assert.equal(d.memory.source, 'hearing');
  assert.ok(Math.hypot(d.memory.x - 8, d.memory.z - 12) <= 3.51);
  assert.notEqual(d.memory.x, f.target.x);
});
test('tracking remains imperfect and movement and slides increase aim error', () => {
  const f = fixture(); const still = f.controller.update(f.bot, .016).aim.error;
  f.bot.vx = 18; f.bot.slideTimer = .4; f.tick(.2);
  const moving = f.controller.update(f.bot, .016).aim.error;
  assert.ok(still > 0); assert.ok(moving > still * 2);
});
test('automatic fire uses short bursts separated by a real pause', () => {
  const f = fixture(); f.controller.update(f.bot, .016); f.tick(.6);
  assert.equal(f.controller.update(f.bot, .016).aim.fire, true);
  for (let i = 0; i < AI.DIFFICULTY.normal.burst; i++) f.controller.recordShot(f.bot);
  assert.equal(f.controller.update(f.bot, .016).aim.fire, false);
  f.tick(.7); assert.equal(f.controller.update(f.bot, .016).aim.fire, true);
});
test('cover must block actual fire while its peek has a reachable firing angle', () => {
  const box = { x: 0, z: 5, y: 1.5, h: 3, w: 8, d: 2, structure: { active: true } };
  const f = fixture({ blocks: [box] }); f.bot.x = 7; f.bot.hp = 15; f.target.z = 18;
  const cover = f.controller.findCover(f.bot, f.target);
  assert.ok(cover); assert.ok(intersects(f.target, cover.hide, box)); assert.ok(!intersects(cover.peek, f.target, box));
  assert.ok(f.host.canTraverse(cover.hide, cover.peek));
  const d = f.controller.update(f.bot, .016); assert.equal(d.tactic, 'retreat'); assert.equal(d.aim.fire, false);
  box.structure.active = false; box.active = false;
  assert.equal(f.controller.findCover(f.bot, f.target), null, 'destroyed cover is discarded');
});
test('routes detour around physical geometry, cache searches and invalidate on geometry changes', () => {
  const box = { x: 0, z: 9, y: 1.5, h: 3, w: 8, d: 3, active: true };
  const nav = [[-8, 0], [-8, 7], [-8, 13], [-8, 20], [0, 20], [8, 0], [8, 7], [8, 13], [8, 20]].map(([x, z]) => ({ x, z, y: 1.86 }));
  const f = fixture({ blocks: [box], nav });
  const route = f.controller.findPath(f.bot, f.target); assert.ok(route.length > 1);
  let previous = f.bot; for (const point of route) { assert.ok(f.host.canTraverse(previous, point)); previous = point; }
  assert.ok(route.some(p => Math.abs(p.x) > 4.6));
  const searches = f.controller.stats().pathSearches;
  f.controller.findPath(f.bot, f.target); assert.equal(f.controller.stats().pathSearches, searches);
  f.changed(); f.controller.findPath(f.bot, f.target); assert.equal(f.controller.stats().pathSearches, searches + 1);
});
test('simultaneous expensive path requests have a bounded decision budget and resume next frame', () => {
  const nav = Array.from({ length: 16 }, (_, i) => ({ x: i * 5, y: 1.86, z: 0 }));
  const f = fixture({ nav, host: { canTraverse: (a, b) => Math.hypot(a.x - b.x, a.z - b.z) <= 6 } });
  for (let i = 2; i < 12; i++) f.controller.findPath(f.bot, nav[i]);
  assert.equal(f.controller.stats().pathSearches, 3); assert.ok(f.controller.stats().deferredSearches >= 7);
  f.tick(.05); assert.ok(f.controller.findPath(f.bot, nav[14]).length > 0);
});
test('an unreachable investigation stops instead of running into an obstacle', () => {
  const f = fixture({ host: { canTraverse: () => false } }); f.visible(false);
  f.controller.hear({ x: 0, y: 1.86, z: 12, team: 2, source: 'p2', range: 38 });
  assert.deepEqual(f.controller.update(f.bot, .016).move, { x: 0, z: 0 });
});
test('recent defeats alter cover decisions and persist across respawn without preserving targets', () => {
  const box = { x: 0, z: 5, y: 1.5, h: 3, w: 8, d: 2, structure: { active: true } }, f = fixture({ blocks: [box] });
  f.bot.x = 7; f.target.z = 18;
  const initial = f.controller.update(f.bot, .016); assert.equal(initial.tactic, 'engage');
  for (let i = 0; i < 3; i++) f.controller.recordOutcome(f.bot, f.target);
  const learned = f.controller.adaptation(f.bot);
  assert.equal(learned.defeats, 3); assert.equal(learned.dangerousLanes[0].x, 7); assert.ok(learned.risk < 1);
  f.controller.reset(f.bot); f.visible(false);
  const respawned = f.controller.update(f.bot, .016);
  assert.equal(respawned.enemy, null); assert.equal(respawned.memory, null); assert.equal(f.controller.adaptation(f.bot).defeats, 3);
  f.visible(true); f.tick(.3);
  assert.ok(['cover', 'peek'].includes(f.controller.update(f.bot, .016).tactic), 'the same visible engagement now favors real cover');
  f.controller.clear(); assert.equal(f.controller.adaptation(f.bot).defeats, 0); assert.equal(f.controller.adaptation(f.bot).risk, 1);
});
test('outcome learning is bounded, expires and changes positioning without changing aiming difficulty', () => {
  const f = fixture(), firstError = f.controller.update(f.bot, .016).aim.error;
  for (let i = 0; i < 30; i++) f.controller.recordOutcome(f.target, f.bot);
  assert.ok(f.controller.adaptation(f.bot).risk <= 1.2); assert.ok(f.controller.adaptation(f.bot).successes <= 8);
  assert.equal(f.controller.update(f.bot, .016).aim.error, firstError, 'success never improves accuracy or reaction statistics');
  for (let i = 0; i < 30; i++) f.controller.recordOutcome(f.bot, f.target);
  assert.ok(f.controller.adaptation(f.bot).risk >= .67); assert.ok(f.controller.adaptation(f.bot).dangerousLanes.length <= 4);
  f.tick(61); assert.equal(f.controller.adaptation(f.bot).risk, 1); assert.equal(f.controller.adaptation(f.bot).dangerousLanes.length, 0);
});
test('production navigation uses full standing height and real compound collision blocks', () => {
  const source = fs.readFileSync(path.resolve(__dirname, '../game/neon-arena/neon-arena.js'), 'utf8');
  const start = source.indexOf('function tacticalPointInfo('), end = source.indexOf('function humanBotController(', start);
  const ground = { x: 0, z: 0, y: -.25, h: .5, w: 30, d: 30, kind: 'platform', structure: { active: true } };
  const ceiling = { x: 0, z: 0, y: 2.4, h: .4, w: 6, d: 6, kind: 'platform', structure: { active: true } }, blocks = [ground, ceiling];
  const context = { PLAYER_H: 1.86, ARENA_SAFE: 64, game: { mode: 'dm' }, blocksNear: () => blocks, arenaRampHeight: () => NaN, actorStandingHeight: () => 2.39,
    actorOverSurface: (x, z, b, r) => Math.hypot(x - Math.max(b.x - b.w / 2, Math.min(b.x + b.w / 2, x)), z - Math.max(b.z - b.d / 2, Math.min(b.z + b.d / 2, z))) <= r };
  vm.createContext(context); vm.runInContext(source.slice(start, end), context);
  assert.equal(context.tacticalPointInfo(0, 0, actor('a')).walkable, false, 'eye height fits but the complete model does not');
  ceiling.y = 5; assert.equal(context.tacticalPointInfo(0, 0, actor('a')).walkable, true, 'a legitimate raised route preserves the ground underneath');
  blocks.push({ x: 1.5, z: 0, y: 1.5, h: 3, w: .4, d: 3, kind: 'wall', structure: { active: true } });
  assert.equal(context.tacticalCanTraverse(actor('a', 0, 0), actor('b', 3, 0), actor('a')), false);
  assert.equal(context.tacticalCanTraverse(actor('a', 0, 5), actor('b', 3, 5), actor('a')), true, 'an open gap between compound pieces remains traversable');
});

test('production bot traversal follows continuous ramp slopes and retains solid ramp sides', () => {
  const source = fs.readFileSync(path.resolve(__dirname, '../game/neon-arena/neon-arena.js'), 'utf8'), blocks = [{ x: 0, z: 5, y: -.25, h: .5, w: 30, d: 30, kind: 'platform', structure: {} }];
  for (let i = 0; i < 48; i++) { const h = (i + .5) / 16; blocks.push({ x: 0, z: (i + .5) / 4, y: h / 2, h, w: 4, d: .262, kind: 'platform', structure: { rampCollision: true } }); }
  blocks.push({ x: 0, z: 14, y: 2.85, h: .3, w: 4, d: 4, kind: 'platform', structure: {} });
  const context = { PLAYER_H: 1.86, ARENA_SAFE: 64, game: { mode: 'dm' }, blocksNear: (x, z, r) => blocks.filter(b => Math.abs(x - b.x) <= b.w / 2 + r && Math.abs(z - b.z) <= b.d / 2 + r), actorStandingHeight: () => 2.39,
    arenaRampHeight: (x, z) => Math.abs(x) <= 2 && z >= 0 && z <= 12 ? z / 4 : NaN,
    actorOverSurface: (x, z, b, r) => Math.hypot(x - Math.max(b.x - b.w / 2, Math.min(b.x + b.w / 2, x)), z - Math.max(b.z - b.d / 2, Math.min(b.z + b.d / 2, z))) <= r };
  const start = source.indexOf('function tacticalPointInfo('), end = source.indexOf('function humanBotController(', start);
  vm.createContext(context); vm.runInContext(source.slice(start, end), context);
  const from = actor('a', 0, 0), to = { ...actor('b', 0, 14), y: 4.86 };
  assert.equal(context.tacticalCanTraverse(from, to, from), true);
  assert.equal(context.tacticalCanTraverse(to, from, from), true);
  assert.equal(context.tacticalPointInfo(2.3, 8, from).walkable, false, 'a ramp remains a solid plinth from its high side');
});

test('all eight Survival arenas give ground species a physical route or a sensed destructible breach', () => {
  const maps = require('../game/neon-arena/neon-survival-maps.js'), survival = require('../game/neon-arena/neon-survival-ai.js');
  const base = path.resolve(__dirname, '../game/neon-arena/assets/survival'), read = file => JSON.parse(fs.readFileSync(file, 'utf8'));
  const source = fs.readFileSync(path.resolve(__dirname, '../game/neon-arena/neon-arena.js'), 'utf8');
  const start = source.indexOf('function tacticalPointInfo('), end = source.indexOf('function humanBotController(', start);
  for (const { id } of maps.catalog()) {
    const root = path.join(base, 'tilesets', id), blueprint = read(path.join(base, 'maps', id + '.json')), definitions = {}, blocks = [];
    for (const item of read(path.join(root, 'prefabs.json')).items) {
      const file = path.join(root, item.file), definition = read(file);
      definitions[item.id] = { ...definition, mesh: read(path.resolve(path.dirname(file), definition.geometry)) };
    }
    maps.hydrateForTest(blueprint, read(path.join(root, 'tileset.json')), definitions);
    const add = (kind, x, z, w, d, h, bottom, safety, extra) => { const piece = { ...extra, active: true, destructible: !safety, type: kind, x: x * 1.45, z: z * 1.45, w: w * 1.45, d: d * 1.45, y: kind === 'platform' ? bottom : bottom + h / 2, h }; blocks.push({ ...piece, y: bottom + h / 2, kind, structure: piece }); return piece; };
    maps.install({ addPlatform: (x, z, w, d, bottom, h, color, safety, extra) => add('platform', x, z, w, d, h, bottom, safety, extra), addWall: (x, z, w, d, h, color, bottom, safety, extra) => add('wall', x, z, w, d, h, bottom, safety, extra) });
    const context = { PLAYER_H: 1.86, ARENA_SAFE: 64.3, game: { mode: 'survivor' }, survivalMapRuntime: maps, actorStandingHeight: () => 1.86,
      blocksNear: (x, z, r) => blocks.filter(b => b.structure.active !== false && Math.abs(x - b.x) <= b.w / 2 + r && Math.abs(z - b.z) <= b.d / 2 + r),
      actorOverSurface: (x, z, b, r) => Math.hypot(x - Math.max(b.x - b.w / 2, Math.min(b.x + b.w / 2, x)), z - Math.max(b.z - b.d / 2, Math.min(b.z + b.d / 2, z))) <= r };
    vm.createContext(context); vm.runInContext(source.slice(start, end), context);
    const controller = survival.createController({ navigation: maps.navigation, revision: () => maps.revision, time: () => 0, structures: () => blocks.map(b => b.structure), humans: () => [goal],
      pointInfo: (x, z, a) => ({ ...maps.pointInfo(x, z, a), ...context.tacticalPointInfo(x, z, a) }),
      canTraverse: (from, to, a) => maps.canTraverse(from, to, a) && context.tacticalCanTraverse(from, to, a), lineClear: () => true });
    const spawn = maps.spawnPoint('ground', () => .5), a = { ...actor('alien', spawn.x, spawn.z), alien: true, alienKind: 'blue', r: .84, y: spawn.y || 1.86 };
    const target = blueprint.playerSpawns[0], goal = { entityId: 'p1', hp: 100, maxHp: 100, x: target.x * 1.45, z: target.z * 1.45, y: (target.y || 0) + 1.86 };
    const route = controller.findPath(a, goal);
    if (!route.length) {
      const decision = controller.acquire(a);
      assert.ok(decision.chew?.destructible, id + ': neither a physical route nor a sensed breach');
      assert.ok(Math.hypot(decision.chew.x - a.x, decision.chew.z - a.z) < 34, id + ': breach must be observed locally');
    }
    let previous = a;
    for (const waypoint of route) { assert.ok(context.tacticalCanTraverse(previous, waypoint, a), id + ': route crossed physical geometry'); previous = waypoint; }
  }
  maps.dispose();
});
