/* Meaningful offline regression checks. Run: node tests/engine-test.cjs */
'use strict';
const assert = require('node:assert/strict');
const { performance } = require('node:perf_hooks');
global.window = global;
require('../src/data.js');
require('../src/tactics.js');
for (const name of ['impacts', 'siege']) {
  const file = require('node:path').join(__dirname, '../src/' + name + '.js');
  if (require('node:fs').existsSync(file)) require(file);
}
require('../src/engine.js');

const results = [];
const filter = process.env.IRONFIELD_TEST_FILTER || '';
const names = process.env.IRONFIELD_TEST_NAMES ? new Set(JSON.parse(process.env.IRONFIELD_TEST_NAMES)) : null;
const clone = value => JSON.parse(JSON.stringify(value));
const army = (name, roster, doctrine = 'balanced') => ({ name, roster, doctrine });
const config = (left, right, map = 'meadow', seed = 4719) => ({
  map, seed, armies: [army('Azure', left), army('Ember', right)]
});
const regiment = (e, team, type) => e.regiments.find(r => r.team === team && (!type || r.type === type));
const place = (e, r, x, y) => e.relocateRegiments([r.id], x, y);
const living = (e, ids) => e.units.filter(u => u.alive && !u.routed && (!ids || ids.has(u.regiment)));
const averageX = units => units.reduce((sum, u) => sum + u.x, 0) / Math.max(1, units.length);
const step = (engine, seconds) => {
  for (let i = 0; i < Math.ceil(seconds * 30); i++) {
    engine.step(1 / 30);
    if (engine.phase === 'ended') break;
  }
};
const checkWorld = engine => {
  for (const u of engine.units) {
    assert(Number.isFinite(u.x) && Number.isFinite(u.y), `unit ${u.id} has invalid coordinates`);
    assert(Number.isFinite(u.hp), `unit ${u.id} has invalid HP`);
    if (u.escaped) assert(u.routed, 'Only fully routed troops may leave the battlefield');
    else {
      assert(u.x >= -1 && u.x <= engine.width + 1, `unit ${u.id} left horizontal bounds`);
      assert(u.y >= -1 && u.y <= engine.height + 1, `unit ${u.id} left vertical bounds`);
    }
    assert(GameData.TYPES[u.type], `unknown troop type ${u.type}`);
  }
  const stats = engine.getStats();
  stats.teams.forEach((s, team) => {
    assert.equal(s.initial, s.alive + s.dead + s.routed, `team ${team} conservation of soldiers`);
    for (const key of ['initial', 'alive', 'dead', 'routed', 'kills', 'morale']) {
      assert(Number.isFinite(s[key]) && s[key] >= 0, `invalid stat ${key} for team ${team}`);
    }
    if ('fallingBack' in s) assert(s.fallingBack >= 0 && s.fallingBack <= s.alive, 'Fallback troops must still be alive');
    if ('rallied' in s) assert(Number.isFinite(s.rallied) && s.rallied >= 0);
  });
  for (const r of engine.regiments) {
    assert(Number.isFinite(r.cohesion) && r.cohesion >= 0 && r.cohesion <= 100, `invalid cohesion for regiment ${r.id}`);
    assert(GameData.FORMATIONS[r.formation], `unknown formation ${r.formation}`);
    assert(['formed', 'narrowing', 'threading', 'reforming'].includes(r.maneuver), `unknown maneuver ${r.maneuver}`);
    assert(Number.isFinite(r.effectiveWidth) && r.effectiveWidth > 0);
    assert(Number.isFinite(r.effectiveDepth) && r.effectiveDepth > 0);
    assert(Number.isFinite(r.deformation) && r.deformation >= 0 && r.deformation <= 100);
    assert(Number.isFinite(r.trafficPressure) && r.trafficPressure >= 0 && r.trafficPressure <= 1);
  }
};

function test(name, run) {
  if (names && !names.has(name)) return;
  if (filter && !name.toLowerCase().includes(filter.toLowerCase())) return;
  const start = performance.now();
  try {
    const detail = run();
    const milliseconds = Math.round((performance.now() - start) * 10) / 10;
    results.push({ name, passed: true, milliseconds, ...(detail || {}) });
    console.log(`PASS ${name} (${milliseconds} ms)${detail ? ' ' + JSON.stringify(detail) : ''}`);
  } catch (error) {
    results.push({ name, passed: false, error: error.message });
    console.error(`FAIL ${name}: ${error.stack}`);
    process.exitCode = 1;
  }
}

test('Default scenario: 2,400 individuals, eighteen troop types, and independent commanders', () => {
  const e = new BattleEngine(clone(GameData.DEFAULT_CONFIG));
  assert.equal(e.phase, 'deployment');
  assert.equal(e.units.length, 2400);
  assert.equal(new Set(e.units.map(u => u.type)).size, 18);
  assert.equal(e.getStats().teams[0].initial, 1200);
  assert.equal(e.getStats().teams[1].initial, 1200);
  const commanders = e.regiments.filter(r => r.type === 'commander');
  assert.equal(commanders.length, 4);
  assert(commanders.every(r => r.initial === 1), 'Each commander should be independently selectable');
  checkWorld(e);
});

test('All authored battlefields and scenarios provide valid terrain and complete rosters', () => {
  assert.equal(GameData.TYPE_ORDER.length, 23);
  assert.equal(GameData.MAPS.length, 23);
  assert.equal(GameData.PRESETS.length, 24 + GameData.HISTORICAL_BATTLES.length);
  for (const id of ['halberdier', 'javelin', 'slinger', 'ballista']) assert(GameData.TYPES[id]);
  const allowedTerrain = new Set(['forest', 'hill', 'water', 'bridge', 'rock', 'wall', 'gate']);
  for (const map of GameData.MAPS) {
    assert(Number.isFinite(map.width) && Number.isFinite(map.height) && map.width > 0 && map.height > 0);
    for (const region of map.regions) {
      assert(allowedTerrain.has(region.type));
      assert([region.x, region.y, region.w, region.h].every(Number.isFinite));
      assert(region.x >= 0 && region.y >= 0 && region.w > 0 && region.h > 0);
      assert(region.x + region.w <= map.width && region.y + region.h <= map.height, `terrain outside ${map.id}`);
    }
    const e = new BattleEngine(config({ swordsman: 8, halberdier: 8 }, { swordsman: 8, javelin: 8 }, map.id));
    checkWorld(e); assertPassable(e);
  }
  for (const preset of GameData.PRESETS) {
    assert(GameData.MAPS.some(m => m.id === preset.map));
    for (const a of preset.armies) for (const id of GameData.TYPE_ORDER) assert(Number.isInteger(a.roster[id]) && a.roster[id] >= 0);
  }
  for (const id of ['balanced', 'epic', 'woodland', 'highland', 'basin']) {
    const p = GameData.PRESETS.find(p => p.id === id); assert(p);
    assert(p.armies.every(a => GameData.TYPE_ORDER.filter(type => !GameData.TYPES[type].equipment && !['musketeer','cannon','trebuchet'].includes(type)).every(type => a.roster[type] > 0)), `${id} omits an existing field troop class`);
    assert(p.armies.every(a => ['siegeTower', 'batteringRam'].every(type => a.roster[type] === 0)), `${id} silently added siege equipment to a legacy roster`);
  }
  assert.deepEqual(GameData.PRESETS.find(p => p.id === 'epic').armies.map(a => Object.values(a.roster).reduce((s, n) => s + n, 0)), [6000, 6000]);
});

test('New halberdiers and three distinct projectile troops inflict real combat damage', () => {
  const projectileKinds = {};
  for (const [type, expected] of [['javelin', 'javelin'], ['slinger', 'slingStone'], ['ballista', 'ballistaBolt']]) {
    const count = type === 'ballista' ? 4 : 16;
    const e = new BattleEngine(config({ [type]: count }, { guardian: 40 }));
    const launchers = e.regiments.filter(r => r.team === 0), targets = e.regiments.filter(r => r.team === 1);
    e.relocateRegiments(launchers.map(r => r.id), 2900, 2250);
    e.relocateRegiments(targets.map(r => r.id), 2900 + GameData.TYPES[type].range * .65, 2250);
    e.command(e.regiments.map(r => r.id), 'hold'); e.start();
    const kinds = new Set(); let visibleFlight = false;
    for (let tick = 0; tick < 900 && e.phase !== 'ended'; tick++) {
      e.step(1 / 30);
      for (const p of e.projectiles) if (p.team === 0) {
        kinds.add(p.kind);
        assert([p.x, p.y, p.z].every(Number.isFinite));
        if (p.z > 0 || Math.abs(p.x - p.sx) > 1) visibleFlight = true;
      }
    }
    const damage = e.units.filter(u => u.team === 1).reduce((sum, u) => sum + u.maxHp - Math.max(0, u.hp), 0);
    assert(kinds.has(expected), `${type} did not launch its distinct projectile`);
    assert(visibleFlight && damage > 0, `${type} launched no effective attack`);
    projectileKinds[type] = { kind: expected, damage: Math.round(damage) };
    checkWorld(e);
  }
  const melee = new BattleEngine(config({ halberdier: 24 }, { cavalry: 24 }));
  melee.relocateRegiments(melee.regiments.filter(r => r.team === 0).map(r => r.id), 2900, 2250);
  melee.relocateRegiments(melee.regiments.filter(r => r.team === 1).map(r => r.id), 3030, 2250);
  melee.start(); step(melee, 15);
  assert(melee.units.some(u => u.team === 1 && u.hp < u.maxHp), 'Halberdiers did not damage mounted enemies');
  assert.equal(GameData.TYPES.halberdier.ranged, false);
  assert(GameData.TYPES.halberdier.antiCavalry > 0 && GameData.TYPES.halberdier.armorPierce > 0);
  checkWorld(melee);
  return projectileKinds;
});

test('Held ranged troops fire at reachable flank enemies despite a distant assigned formation', () => {
  const e = new BattleEngine(config({ javelin: 16 }, { guardian: 1, militia: 64 }));
  const shooters = regiment(e, 0), approaching = regiment(e, 1, 'guardian'), distant = regiment(e, 1, 'militia');
  place(e, shooters, 2900, 2200); place(e, approaching, 3070, 2480); place(e, distant, 3300, 2200);
  e.command(e.regiments.map(r => r.id), 'hold'); e.start();
  assert.equal(shooters._enemyId, distant.id, 'The fixture did not create the distant strategic assignment');
  const original = shooters.unitIds.map(id => ({ id, x: e.units[id].x, y: e.units[id].y }));
  e.command([approaching.id], 'move', 3070, 2200);
  const firing = new Set(); let observedProjectile = false;
  for (let tick = 0; tick < 180; tick++) {
    e.step(1 / 30);
    for (const id of shooters.unitIds) if (e.units[id].attackFlash > .999) firing.add(id);
    if (e.projectiles.some(p => p.team === 0 && p.kind === 'javelin' && p.target === approaching.unitIds[0])) observedProjectile = true;
  }
  const damage = approaching.unitIds.reduce((sum, id) => sum + e.units[id].maxHp - e.units[id].hp, 0);
  assert(observedProjectile && firing.size >= 8 && damage > 0, 'Reachable flank enemies were ignored while the strategic target remained out of range');
  assert(distant.unitIds.every(id => e.units[id].hp === e.units[id].maxHp), 'The intended distant formation unexpectedly entered firing range');
  for (const before of original) assert(Math.hypot(e.units[before.id].x - before.x, e.units[before.id].y - before.y) < .01, 'Ranged fallback targeting moved a held shooter');
  assert.equal(shooters.order, 'hold'); checkWorld(e); assertPassable(e);
  return { shootersFiring: firing.size, reachableFlankDamage: Math.round(damage * 100) / 100, heldShootersMoved: false };
});

test('Pause, resume, and deployment orders preserve simulation state', () => {
  const e = new BattleEngine(config({ swordsman: 20 }, { swordsman: 20 }));
  const ids = e.regiments.filter(r => r.team === 0).map(r => r.id);
  e.relocateRegiments(ids, 1800, 1600);
  const before = e.units.map(u => [u.x, u.y, u.hp]);
  step(e, 1);
  assert.equal(e.time, 0);
  assert.deepEqual(e.units.map(u => [u.x, u.y, u.hp]), before);
  assert(e.command(ids, 'hold') >= 1);
  e.start();
  step(e, 2);
  assert(e.time > 0);
  e.pause();
  const paused = JSON.stringify(e.units.map(u => [u.x, u.y, u.hp]));
  const pausedTime = e.time;
  step(e, 2);
  assert.equal(e.time, pausedTime);
  assert.equal(JSON.stringify(e.units.map(u => [u.x, u.y, u.hp])), paused);
  e.resume();
  step(e, 1);
  assert(e.time > pausedTime);
  checkWorld(e);
});

test('Identical seeds and commands produce identical battle states', () => {
  const cfg = config({ swordsman: 36, archer: 16, cavalry: 8 }, { pikeman: 36, crossbow: 16, cavalry: 8 });
  const a = new BattleEngine(clone(cfg));
  const b = new BattleEngine(clone(cfg));
  for (const e of [a, b]) {
    e.relocateRegiments(e.regiments.filter(r => r.team === 0).map(r => r.id), 2750, 2200);
    e.relocateRegiments(e.regiments.filter(r => r.team === 1).map(r => r.id), 3450, 2200);
    e.start();
    step(e, 45);
  }
  assert.deepEqual(a.getStats(), b.getStats());
  assert.deepEqual(a.units.map(u => [u.x, u.y, u.hp, u.routed]), b.units.map(u => [u.x, u.y, u.hp, u.routed]));
  checkWorld(a);
});

test('Combined arms stays engaged beyond the opening clash and resolves with a valid outcome', () => {
  const e = new BattleEngine(config(
    { swordsman: 50, pikeman: 20, archer: 24, cavalry: 10, catapult: 2 },
    { militia: 70, berserker: 20, crossbow: 20, lancer: 8, elephant: 2 }
  ));
  for (let team = 0; team < 2; team++) {
    e.relocateRegiments(e.regiments.filter(r => r.team === team).map(r => r.id), team ? 3500 : 2800, 2200);
  }
  e.start();
  step(e, 20);
  assert.equal(e.phase, 'running', 'The armies disintegrated during the opening clash');
  assert(e.getStats().teams.every(t => t.alive > t.initial * .5), 'Too much of an army vanished during the opening clash');
  step(e, 880);
  const stats = e.getStats();
  assert(stats.teams.some(t => t.dead > 0), 'No casualties occurred');
  assert.equal(e.phase, 'ended', 'Battle failed to resolve within 15 simulated minutes');
  assert(e.time < 899, 'Battle only ended at the timeout instead of reaching a natural outcome');
  assert(e.time >= 45, 'Battle pace regressed to the earlier rapid collapse');
  assert([0, 1, 'draw'].includes(stats.winner), 'Missing valid battle result');
  checkWorld(e);
  return { simulatedSeconds: Math.round(e.time), winner: stats.winner, casualties: stats.teams.map(t => t.dead), routed: stats.teams.map(t => t.routed) };
});

test('Three Bridges: water blocks, bridges pass, and troops navigate a crossing', () => {
  const e = new BattleEngine(config({ cavalry: 24 }, { militia: 2 }, 'river'));
  assert.equal(e.terrainAt(3200, 1500).blocked, true);
  for (const y of [870, 2210, 3530]) assert.equal(e.terrainAt(3200, y).blocked, false);
  const left = e.regiments.filter(r => r.team === 0).map(r => r.id);
  const right = e.regiments.filter(r => r.team === 1).map(r => r.id);
  e.relocateRegiments(left, 2580, 1500);
  e.relocateRegiments(right, 5700, 4000);
  e.command(right, 'hold');
  e.start();
  e.command(left, 'move', 3800, 1500);
  for (let tick = 0; tick < 3600; tick++) {
    e.step(1 / 30);
    if (tick % 10 === 0) {
      for (const u of e.units.filter(u => u.team === 0 && u.alive && !u.routed)) {
        assert.equal(e.terrainAt(u.x, u.y).blocked, false, `unit ${u.id} entered impassable river`);
      }
    }
  }
  const riders = e.units.filter(u => u.team === 0 && u.alive && !u.routed);
  assert(riders.length > 0);
  assert(riders.filter(u => u.x > 3450).length >= riders.length * .8, 'Riders did not traverse a bridge');
  checkWorld(e);
});

test('Imported army counts and malformed values remain bounded and finite', () => {
  const oversized = Object.fromEntries(GameData.TYPE_ORDER.map(id => [id, 99999999]));
  const e = new BattleEngine(config(oversized, oversized, 'not-a-real-map', 888));
  assert(e.units.length <= GameData.MAX_UNITS);
  assert(e.getStats().teams.every(t => t.initial <= 10000));
  checkWorld(e);
  const malformed = new BattleEngine(config({ swordsman: -25, archer: 'banana', cavalry: Infinity, militia: 10 }, { pikeman: 20.7 }));
  checkWorld(malformed);
  const generals = new BattleEngine(config({ commander: 999999 }, { commander: 999999 }));
  assert(generals.units.filter(u => u.team === 0).length <= GameData.TYPES.commander.maxCount);
  assert(generals.units.filter(u => u.team === 1).length <= GameData.TYPES.commander.maxCount);
  checkWorld(generals);
});

// Continuous relative motion is checked, not only end-of-frame distances:
// a soldier must not teleport through another soldier between two snapshots.
function sweptSeparation(previousA, previousB, a, b) {
  const px = previousA.x - previousB.x, py = previousA.y - previousB.y;
  const vx = (a.x - b.x) - px, vy = (a.y - b.y) - py;
  const lengthSquared = vx * vx + vy * vy;
  const t = lengthSquared ? Math.max(0, Math.min(1, -(px * vx + py * vy) / lengthSquared)) : 0;
  return Math.hypot(px + vx * t, py + vy * t);
}

function bodySnapshot(e) { return e.units.map(u => ({ x: u.x, y: u.y })); }
function deploymentGeometry(e) {
  return e.regiments.map(r => ({ team: r.team, type: r.type,
    positions: r.unitIds.map(id => ({ x: e.units[id].x, y: e.units[id].y })) }));
}
function assertDeploymentGeometry(e, expected, message) {
  const actual = deploymentGeometry(e);
  assert.equal(actual.length, expected.length, message + ': changed group count');
  actual.forEach((group, i) => {
    assert.equal(group.team, expected[i].team); assert.equal(group.type, expected[i].type);
    assert.equal(group.positions.length, expected[i].positions.length, message + ': changed soldier count');
    group.positions.forEach((p, j) => assert(Math.hypot(p.x - expected[i].positions[j].x, p.y - expected[i].positions[j].y) < 1e-6,
      `${message}: group ${i}, soldier ${j} moved during reconstruction`));
  });
}
function minimumStaticBodyRatio(e) {
  const units = living(e); let minimum = Infinity;
  for (let i = 0; i < units.length; i++) for (let j = i + 1; j < units.length; j++) {
    const a = units[i], b = units[j];
    minimum = Math.min(minimum, Math.hypot(a.x - b.x, a.y - b.y) / (GameData.TYPES[a.type].radius + GameData.TYPES[b.type].radius));
  }
  return minimum;
}
function minimumBodyRatio(e, previous, ids) {
  const units = living(e, ids);
  let minimum = Infinity;
  for (let i = 0; i < units.length; i++) for (let j = i + 1; j < units.length; j++) {
    const a = units[i], b = units[j], pa = previous[a.id], pb = previous[b.id];
    const radius = GameData.TYPES[a.type].radius + GameData.TYPES[b.type].radius;
    // Only nearby swept boxes can intersect; this bounds test cost without
    // ignoring fast crossings between snapshots.
    if (Math.min(pa.x, a.x) - Math.max(pb.x, b.x) > radius || Math.min(pb.x, b.x) - Math.max(pa.x, a.x) > radius ||
        Math.min(pa.y, a.y) - Math.max(pb.y, b.y) > radius || Math.min(pb.y, b.y) - Math.max(pa.y, a.y) > radius) continue;
    minimum = Math.min(minimum, sweptSeparation(pa, pb, a, b) / radius);
  }
  return minimum;
}
function assertPassable(e, ids) {
  for (const u of living(e, ids)) assert.equal(e.terrainAt(u.x, u.y).blocked, false, `active unit ${u.id} entered blocked terrain at ${u.x},${u.y}`);
}
function assertBoundedMovement(e, previous, ids, dt = 1 / 30) {
  let maximum = 0;
  for (const u of living(e, ids)) {
    const distance = Math.hypot(u.x - previous[u.id].x, u.y - previous[u.id].y);
    // Allows the game's charge / formation speed modifiers and a small
    // contact tolerance, but rejects instantaneous lane-opening teleports.
    const limit = GameData.TYPES[u.type].speed * dt * 1.8 + .25;
    assert(distance <= limit, `unit ${u.id} jumped ${distance.toFixed(3)} units in one step (limit ${limit.toFixed(3)})`);
    maximum = Math.max(maximum, distance);
  }
  return maximum;
}

test('Enemy ranks physically block a move order without soldiers ghosting through', () => {
  const e = new BattleEngine(config({ swordsman: 48 }, { guardian: 80 }));
  const attackers = regiment(e, 0), defenders = regiment(e, 1);
  place(e, attackers, 2940, 2200); place(e, defenders, 3160, 2200);
  e.setFormation([defenders.id], 'shieldwall');
  e.command([defenders.id], 'hold'); e.start();
  e.command([attackers.id], 'move', 3460, 2200);
  let minimumRatio = Infinity, minimumCohesion = attackers.cohesion;
  let observedContact = false, observedMeleeStrikes = 0;
  const observedAttackers = [new Set(), new Set()];
  for (let tick = 0; tick < 600 && e.phase !== 'ended'; tick++) {
    const previous = e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp, flash: u.attackFlash }));
    e.step(1 / 30);
    for (const u of e.units) {
      if (u.attackFlash > .999 && previous[u.id].flash < .999 && u.target >= 0 &&
          e.units[u.target].hp < previous[u.target].hp - .001) {
        observedMeleeStrikes++; observedAttackers[u.team].add(u.id);
      }
    }
    assertBoundedMovement(e, previous, new Set([attackers.id, defenders.id]));
    minimumCohesion = Math.min(minimumCohesion, attackers.cohesion);
    const a = living(e, new Set([attackers.id])), b = living(e, new Set([defenders.id]));
    for (const left of a) for (const right of b) {
      const radius = GameData.TYPES[left.type].radius + GameData.TYPES[right.type].radius;
      const separation = sweptSeparation(previous[left.id], previous[right.id], left, right);
      minimumRatio = Math.min(minimumRatio, separation / radius);
      // Melee troops can engage at weapon reach without touching bodies.
      if (separation <= Math.max(GameData.TYPES[left.type].range, GameData.TYPES[right.type].range) * 1.2) observedContact = true;
    }
  }
  assert(observedContact, 'The move order never reached the held enemy front');
  assert(observedMeleeStrikes >= 40 && observedAttackers.every(ids => ids.size >= 4), 'The enemy-front test did not sustain real melee strikes and health loss on both sides');
  assert(minimumRatio >= .90, `Enemy bodies deeply overlapped: ${minimumRatio.toFixed(3)} radii`);
  const activeAttackers = living(e, new Set([attackers.id]));
  const activeDefenders = living(e, new Set([defenders.id]));
  assert(activeDefenders.length > 20, 'The blocking formation was removed before collision could be tested');
  assert(averageX(activeAttackers) < averageX(activeDefenders) + 65, 'The entire attacker formation passed through the live enemy wall');
  assert(minimumCohesion < 99, 'Contact should disrupt formation cohesion');
  checkWorld(e);
  return { observedMeleeStrikes, actualAttackersByTeam: observedAttackers.map(ids => ids.size), minimumBodySeparationRatio: Math.round(minimumRatio * 1000) / 1000, minimumCohesion: Math.round(minimumCohesion) };
});

test('Friendly crossing traffic physically passes through and reaches both destinations', () => {
  const e = new BattleEngine(config({ swordsman: 36, pikeman: 36 }, { militia: 1 }));
  const front = regiment(e, 0, 'swordsman'), other = regiment(e, 0, 'pikeman'), enemy = regiment(e, 1);
  place(e, front, 2000, 2000); place(e, other, 2400, 2000); place(e, enemy, 5800, 3800);
  e.command([enemy.id], 'hold'); e.start();
  e.command([front.id], 'move', 2800, 2000); e.command([other.id], 'move', 1600, 2000);
  let minimumRatio = Infinity, minimumCohesion = 100, near = false, arrivalTime = null;
  const initialY=e.units.map(u=>u.y);
  let maximumLateral=0;
  for (let tick = 0; tick < 3600; tick++) {
    const previous = e.units.map(u => ({ x: u.x, y: u.y }));
    e.step(1 / 30);
    assertBoundedMovement(e, previous, new Set([front.id, other.id]));
    minimumCohesion = Math.min(minimumCohesion, front.cohesion, other.cohesion);
    const a = living(e, new Set([front.id])), b = living(e, new Set([other.id]));
    for(const u of [...a,...b])maximumLateral=Math.max(maximumLateral,Math.abs(u.y-initialY[u.id]));
    for (const left of a) for (const right of b) {
      const radius = GameData.TYPES[left.type].radius + GameData.TYPES[right.type].radius;
      const separation = sweptSeparation(previous[left.id], previous[right.id], left, right);
      minimumRatio = Math.min(minimumRatio, separation / radius);
      if (separation < radius * 2) near = true;
    }
    assertPassable(e, new Set([front.id, other.id]));
    if (arrivalTime === null && a.filter(u => u.x > 2650).length >= a.length * .9 && b.filter(u => u.x < 1750).length >= b.length * .9) arrivalTime = e.time;
    if (arrivalTime !== null && e.time > arrivalTime + 25) break;
  }
  assert(near, 'The crossing formations never approached one another');
  assert(minimumRatio >= .90, `Friendly bodies deeply overlapped: ${minimumRatio.toFixed(3)} radii`);
  assert(minimumCohesion < 95, 'Traffic did not affect cohesion');
  assert(arrivalTime !== null, 'Allied formations jammed instead of reaching the opposite destinations');
  assert(maximumLateral > GameData.TYPES.swordsman.radius, 'No soldier stepped aside during the head-on crossing');
  assert(Math.min(front.cohesion, other.cohesion) > minimumCohesion + 5, 'Cohesion did not recover after the formations cleared one another');
  assert.equal(e.getStats().teams[0].dead, 0, 'Friendly traffic caused deaths');
  checkWorld(e);
  return { minimumBodySeparationRatio: Math.round(minimumRatio * 1000) / 1000, minimumCohesion: Math.round(minimumCohesion), arrivalSeconds: Math.round(arrivalTime * 10) / 10, finalCohesion: Math.round(Math.min(front.cohesion, other.cohesion)) };
});

test('Rock passage narrows the actual ranks, clears the corridor, and reforms', () => {
  const e = new BattleEngine(config({ swordsman: 160 }, { militia: 1 }, 'desert'));
  const r = regiment(e, 0), enemy = regiment(e, 1), ids = new Set([r.id]);
  place(e, r, 2700, 1375); place(e, enemy, 5900, 4000);
  e.command([enemy.id], 'hold'); e.start(); e.command([r.id], 'move', 3750, 1375);
  const originalWidth = r.width, passedGap = new Set(), maneuvers = new Set();
  let minimumWidth = r.effectiveWidth, maximumDeformation = 0, minimumCohesion = 100;
  let minimumRatio = Infinity, arrivalTime = null, maximumStep = 0;
  for (let tick = 0; tick < 4500; tick++) {
    const previous = bodySnapshot(e); e.step(1 / 30);
    maximumStep = Math.max(maximumStep, assertBoundedMovement(e, previous, ids));
    minimumRatio = Math.min(minimumRatio, minimumBodyRatio(e, previous, ids));
    assertPassable(e, ids);
    maneuvers.add(r.maneuver); minimumWidth = Math.min(minimumWidth, r.effectiveWidth);
    maximumDeformation = Math.max(maximumDeformation, r.deformation); minimumCohesion = Math.min(minimumCohesion, r.cohesion);
    const units = living(e, ids);
    for (const u of units) if (previous[u.id].x < 3200 && u.x >= 3200 && u.y > 1260 && u.y < 1490) passedGap.add(u.id);
    if (arrivalTime === null && units.filter(u => u.x > 3550).length >= units.length * .9) arrivalTime = e.time;
    if (arrivalTime !== null && e.time > arrivalTime + 35) break;
  }
  assert(arrivalTime !== null, 'The regiment failed to emerge from the rocky corridor');
  assert(passedGap.size >= 144, 'Most troops bypassed the intended narrow gap instead of threading it');
  assert(minimumRatio >= .90, `Rock threading caused body penetration: ${minimumRatio}`);
  assert(minimumWidth < originalWidth * .9, 'The formation never narrowed its physical frontage');
  assert(maximumDeformation > 8 && (maneuvers.has('narrowing') || maneuvers.has('threading')), 'No adaptive passage maneuver was observed');
  assert(r.effectiveWidth > originalWidth * .9 && r.deformation < Math.max(8, maximumDeformation * .5), 'The formation did not widen again after clearing the rocks');
  assert(r.cohesion > minimumCohesion + 5 || r.cohesion >= 90, 'The ranks did not recover cohesion on open ground');
  assert.equal(e.getStats().teams[0].dead, 0);
  checkWorld(e);
  return { crossedThroughGap: passedGap.size, arrivalSeconds: Math.round(arrivalTime * 10) / 10, minimumWidth: Math.round(minimumWidth), originalWidth, maximumDeformation: Math.round(maximumDeformation), finalDeformation: Math.round(r.deformation), maximumStepDisplacement: Math.round(maximumStep * 1000) / 1000, minimumBodySeparationRatio: Number.isFinite(minimumRatio) ? Math.round(minimumRatio * 1000) / 1000 : 'no body contact' };
});

test('Cavalry passes through opened gaps in a held allied shield wall, then both reform', () => {
  const e = new BattleEngine(config({ cavalry: 24, pikeman: 100 }, { militia: 1 }));
  const riders = regiment(e, 0, 'cavalry'), wall = regiment(e, 0, 'pikeman'), enemy = regiment(e, 1);
  place(e, riders, 1900, 2400); place(e, wall, 2300, 2400); place(e, enemy, 5900, 4000);
  e.setFormation([wall.id], 'shieldwall'); e.command([wall.id, enemy.id], 'hold');
  const ids = new Set([riders.id, wall.id]), originalWall = new Map(wall.unitIds.map(id => [id, { x: e.units[id].x, y: e.units[id].y }]));
  const centralHalfWidth = wall.width * .35, centralCrossings = new Set();
  let minimumRatio = Infinity, minimumCohesion = 100, maximumTraffic = 0, maximumYield = 0, arrivalTime = null, maximumStep = 0;
  e.start(); e.command([riders.id], 'move', 2800, 2400);
  for (let tick = 0; tick < 4500; tick++) {
    const previous = bodySnapshot(e); e.step(1 / 30);
    maximumStep = Math.max(maximumStep, assertBoundedMovement(e, previous, ids));
    minimumRatio = Math.min(minimumRatio, minimumBodyRatio(e, previous, ids)); assertPassable(e, ids);
    minimumCohesion = Math.min(minimumCohesion, riders.cohesion, wall.cohesion); maximumTraffic = Math.max(maximumTraffic, riders.trafficPressure, wall.trafficPressure);
    const units = living(e, new Set([riders.id]));
    for (const u of units) if (previous[u.id].x < 2300 && u.x >= 2300 && Math.abs(u.y - 2400) < centralHalfWidth) centralCrossings.add(u.id);
    for (const id of wall.unitIds) { const u = e.units[id], start = originalWall.get(id); maximumYield = Math.max(maximumYield, Math.hypot(u.x - start.x, u.y - start.y)); }
    if (arrivalTime === null && units.filter(u => u.x > 2650).length >= units.length * .9) arrivalTime = e.time;
    if (arrivalTime !== null && e.time > arrivalTime + 35) break;
  }
  assert(arrivalTime !== null, 'Mounted allies could not pass the stationary friendly formation');
  const ridersCleared = living(e, new Set([riders.id])).filter(u => u.x > 2650).length;
  assert.equal(ridersCleared, 24, 'Some mounted allies remained stranded after the traffic maneuver');
  assert(centralCrossings.size >= 8, 'Riders bypassed the perimeter rather than passing through opened internal gaps');
  assert(minimumRatio >= .90, `Mixed-size bodies penetrated: ${minimumRatio}`);
  assert(maximumYield > 8 && maximumTraffic > .05, 'Held troops did not visibly yield to allied traffic');
  assert(minimumCohesion < 95, 'Passing through the formation did not cause disorder');
  assert(Math.abs(wall.x - 2300) < 180 && Math.abs(wall.y - 2400) < 180, 'The held regiment abandoned its position instead of opening lanes');
  assert(Math.min(riders.cohesion, wall.cohesion) > minimumCohesion + 5, 'Ranks failed to reform after mounted traffic cleared');
  assert.equal(e.getStats().teams[0].dead, 0);
  checkWorld(e);
  return { ridersThroughCentralGaps: centralCrossings.size, ridersCleared, ridersStranded: 24 - ridersCleared, arrivalSeconds: Math.round(arrivalTime * 10) / 10, maximumYield: Math.round(maximumYield), minimumCohesion: Math.round(minimumCohesion), finalCohesion: Math.round(Math.min(riders.cohesion, wall.cohesion)), maximumStepDisplacement: Math.round(maximumStep * 1000) / 1000, minimumBodySeparationRatio: Math.round(minimumRatio * 1000) / 1000 };
});

test('Woodland slows and narrows cavalry, then allows open-ground reformation', () => {
  function traverse(y) {
    const e = new BattleEngine(config({ cavalry: 48 }, { militia: 1 }));
    const r = regiment(e, 0), enemy = regiment(e, 1), ids = new Set([r.id]);
    place(e, r, 2050, y); place(e, enemy, 5900, 4000);
    e.command([enemy.id], 'hold'); e.start(); e.command([r.id], 'move', 3450, y);
    const initialWidth = r.width;
    let arrivalTime = null, minimumWidth = r.effectiveWidth, maximumDeformation = 0, minimumCohesion = 100, forestSteps = 0, minimumRatio = Infinity;
    for (let tick = 0; tick < 4500; tick++) {
      const previous = bodySnapshot(e); e.step(1 / 30);
      assertBoundedMovement(e, previous, ids);
      minimumRatio = Math.min(minimumRatio, minimumBodyRatio(e, previous, ids)); assertPassable(e, ids);
      const units = living(e, ids);
      if (units.some(u => e.terrainAt(u.x, u.y).type === 'forest')) forestSteps++;
      minimumWidth = Math.min(minimumWidth, r.effectiveWidth); maximumDeformation = Math.max(maximumDeformation, r.deformation); minimumCohesion = Math.min(minimumCohesion, r.cohesion);
      if (arrivalTime === null && units.filter(u => u.x > 3310).length >= units.length * .9) arrivalTime = e.time;
      if (arrivalTime !== null && e.time > arrivalTime + 35) break;
    }
    assert(arrivalTime !== null, 'Cavalry failed to reach the far side of its route');
    assert(minimumRatio >= .90, `Terrain maneuver caused body penetration: ${minimumRatio}`);
    checkWorld(e);
    return { arrivalTime, initialWidth, minimumWidth, maximumDeformation, minimumCohesion, finalCohesion: r.cohesion, finalWidth: r.effectiveWidth, finalDeformation: r.deformation, forestSteps, minimumRatio };
  }
  const open = traverse(2350), woods = traverse(550);
  assert(woods.forestSteps > 150 && open.forestSteps === 0, 'The two routes did not isolate forest travel');
  assert(woods.arrivalTime > open.arrivalTime * 1.15, 'Woodland did not impose a meaningful travel delay');
  assert(woods.minimumWidth < woods.initialWidth * .9 && woods.maximumDeformation > 8, 'Cavalry never compressed its formation in the woods');
  assert(woods.finalWidth > woods.initialWidth * .9 && woods.finalDeformation < Math.max(8, woods.maximumDeformation * .5), 'Cavalry failed to restore frontage after the woods');
  assert(woods.finalCohesion > woods.minimumCohesion + 5 || woods.finalCohesion >= 90, 'Cavalry failed to recover cohesion outside the woods');
  return { openArrivalSeconds: Math.round(open.arrivalTime * 10) / 10, woodlandArrivalSeconds: Math.round(woods.arrivalTime * 10) / 10, minimumWidth: Math.round(woods.minimumWidth), originalWidth: woods.initialWidth, maximumDeformation: Math.round(woods.maximumDeformation), finalDeformation: Math.round(woods.finalDeformation) };
});

test('Shield-wall formation has a real movement tradeoff', () => {
  function distance(formation) {
    const e = new BattleEngine(config({ swordsman: 36 }, { militia: 1 }));
    const r = regiment(e, 0), enemy = regiment(e, 1);
    place(e, r, 1400, 1300); place(e, enemy, 5800, 3800);
    assert.equal(e.setFormation([r.id], formation), 1);
    e.command([enemy.id], 'hold'); const origin = averageX(living(e, new Set([r.id])));
    e.start(); e.command([r.id], 'move', 2400, 1300); step(e, 10);
    checkWorld(e); return averageX(living(e, new Set([r.id]))) - origin;
  }
  const line = distance('line'), wall = distance('shieldwall');
  assert(line > 200 && wall > 100, 'Formations did not meaningfully move');
  assert(wall < line * .92, 'Shield wall did not move more slowly than line');
  return { lineDistance: Math.round(line), shieldWallDistance: Math.round(wall) };
});

test('Loose order provides real protection during a ranged exchange', () => {
  function incomingDamage(formation) {
    const e = new BattleEngine(config({ swordsman: 80 }, { archer: 70 }));
    const targets = regiment(e, 0), archers = regiment(e, 1);
    place(e, targets, 2950, 2250); place(e, archers, 3330, 2250);
    e.setFormation([targets.id], formation);
    e.command([targets.id, archers.id], 'hold'); e.start(); step(e, 22);
    checkWorld(e);
    return e.units.filter(u => u.team === 0).reduce((sum, u) => sum + u.maxHp - Math.max(0, u.hp), 0);
  }
  const line = incomingDamage('line'), loose = incomingDamage('loose');
  assert(line > 50, 'Archers did not inflict enough damage to compare formations');
  assert(loose < line * .94, 'Loose order did not protect troops from ranged fire');
  return { lineDamage: Math.round(line), looseDamage: Math.round(loose) };
});

test('Fall back stays recoverable and a commander can rally the surviving soldiers', () => {
  const e = new BattleEngine(config({ swordsman: 48, commander: 1 }, { militia: 1 }));
  const troops = regiment(e, 0, 'swordsman'), general = regiment(e, 0, 'commander'), enemy = regiment(e, 1);
  place(e, troops, 2600, 2250); place(e, general, 2250, 2250); place(e, enemy, 5800, 3800);
  e.command([general.id, enemy.id], 'hold'); e.start();
  const before = e.getStats().teams[0].alive;
  assert.equal(e.command([troops.id], 'retreat'), 1);
  assert.equal(troops.fallingBack, true);
  assert.equal(e.getStats().teams[0].alive, before);
  assert.equal(e.getStats().teams[0].routed, 0);
  assert.equal(e.getStats().teams[0].fallingBack, 48);
  step(e, 4);
  assert.equal(troops.fallingBack, true, 'Fallback instantly recovered before a rally');
  assert(troops.supportRegimentId >= 0 || troops.regroupWith, 'Fallback did not seek friendly support');
  assert.equal(e.command([general.id], 'rally'), 1);
  step(e, 1);
  assert.equal(troops.fallingBack, false, 'Commander rally did not recover nearby fallback troops');
  assert(e.getStats().teams[0].rallied >= 48, 'Recovered troops were not reflected in rally statistics');
  assert.equal(e.getStats().teams[0].alive, before);
  assert.equal(e.getStats().teams[0].routed, 0);
  assert(general.rallyCooldown > 0, 'Rally did not start its cooldown');
  assert.equal(e.command([general.id], 'rally'), 0, 'Rally bypassed its cooldown');
  checkWorld(e);
});

test('Fallback troops reform near an allied formation without requiring permanent flight', () => {
  const e = new BattleEngine(config({ swordsman: 40, guardian: 40 }, { militia: 1 }));
  const troops = regiment(e, 0, 'swordsman'), support = regiment(e, 0, 'guardian'), enemy = regiment(e, 1);
  place(e, troops, 2600, 2250); place(e, support, 2250, 2250); place(e, enemy, 5800, 3800);
  e.command([support.id, enemy.id], 'hold'); e.start(); e.command([troops.id], 'retreat');
  step(e, 75);
  assert.equal(troops.fallingBack, false, 'Supported fallback troops never regrouped');
  assert.equal(e.getStats().teams[0].routed, 0, 'Supported troops were permanently routed');
  assert.equal(e.getStats().teams[0].alive, 80, 'Recovery lost healthy soldiers');
  assert(e.getStats().teams[0].rallied >= 40, 'Passive regroup did not count recovered troops');
  checkWorld(e);
});

// FRONTAGE_METRICS_BEGIN
function measureFrontage(Engine, options = {}) {
  const map = options.map || 'meadow', count = options.count || 640, horizon = options.seconds || 120;
  const e = new Engine({ map, seed: 4719, armies: [
    { name: 'Assault columns', doctrine: 'aggressive', roster: { swordsman: count } },
    { name: 'Held battle line', doctrine: 'defensive', roster: { swordsman: count } }
  ] });
  const left = e.regiments.filter(r => r.team === 0), right = e.regiments.filter(r => r.team === 1);
  const ys = options.ys || [1250, 1900, 2550, 3200];
  left.forEach((r, i) => e.relocateRegiments([r.id], options.parallel ? 2450 : 2750 - i * 240, options.parallel ? ys[i] : 2225));
  right.forEach((r, i) => { e.relocateRegiments([r.id], options.parallel ? 3950 : 3650, ys[i]); e.command([r.id], 'hold'); });
  const previousHp = e.units.map(u => u.hp), previousFlash = e.units.map(u => u.attackFlash), previousPos = e.units.map(u => ({ x: u.x, y: u.y }));
  const frontIds = new Set(), rearIds = new Set(), firstFrontDeath = new Map(), rearReplacements = new Set();
  for (const r of left) {
    const maximumX = Math.max(...r.unitIds.map(id => e.units[id].x));
    for (const id of r.unitIds) {
      if (e.units[id].x > maximumX - 34) frontIds.add(id);
      if (e.units[id].x < maximumX - 60) rearIds.add(id);
    }
  }
  const windows = [], allAttackers = new Set(), allDamagedRegiments = new Set(), forestVisited = new Set();
  let frontDeaths = 0, forestStrikes = 0, maximumStep = 0, invalidTerrain = 0, snapshot = null;
  const unitKeys = ['id', 'team', 'type', 'regiment', 'x', 'y', 'hp', 'maxHp', 'alive', 'routed', 'angle', 'state', 'anim', 'attackFlash', 'target', 'escaped', '_deathTime'];
  const regKeys = ['id', 'team', 'type', 'name', 'unitIds', 'initial', 'alive', 'morale', 'x', 'y', 'order', 'targetX', 'targetY', 'width', 'depth', 'facing', 'kills', 'formation', 'cohesion', 'status', 'fallingBack', 'rallyRadius', 'rallyCooldown', 'regroupWith', 'supportRegimentId', 'maneuver', 'effectiveWidth', 'effectiveDepth', 'deformation', 'trafficPressure', 'footprintFacing', 'tacticalRole', 'engagedSoldiers', 'frontX', 'frontY', 'frontageWidth'];
  const pick = (obj, keys) => Object.fromEntries(keys.filter(k => obj[k] !== undefined).map(k => [k, obj[k]]));
  e.start();
  for (let tick = 0; tick < Math.round(horizon * 30); tick++) {
    e.step(1 / 30);
    const wi = Math.floor(tick / 300);
    if (!windows[wi]) windows[wi] = { attackers: new Set(), regiments: new Map(), damaged: new Set(), hits: 0, ys: [], remaining: null };
    const w = windows[wi];
    for (const u of e.units) {
      if (u.team === 1 && u.hp < previousHp[u.id] - .001) { w.hits++; w.damaged.add(u.regiment); allDamagedRegiments.add(u.regiment); }
      if (u.team === 0 && frontIds.has(u.id) && u.hp <= 0 && previousHp[u.id] > 0) {
        frontDeaths++;
        if (!firstFrontDeath.has(u.regiment)) firstFrontDeath.set(u.regiment, e.time);
      }
    }
    for (const u of e.units) {
      if (u.team === 0 && u.attackFlash > .999 && previousFlash[u.id] < .999 && u.target >= 0) {
        const target = e.units[u.target];
        // Count genuine melee strikes, not merely attack poses or proximity.
        if (target && previousHp[target.id] > target.hp + .001) {
          w.attackers.add(u.id); allAttackers.add(u.id); w.ys.push(u.y);
          if (!w.regiments.has(u.regiment)) w.regiments.set(u.regiment, new Map());
          w.regiments.get(u.regiment).set(u.id, u.y);
          if (rearIds.has(u.id) && firstFrontDeath.has(u.regiment) && e.time > firstFrontDeath.get(u.regiment) + .5) rearReplacements.add(u.id);
          if (e.terrainAt(u.x, u.y).type === 'forest') forestStrikes++;
        }
      }
      if (u.alive && !u.routed) {
        maximumStep = Math.max(maximumStep, Math.hypot(u.x - previousPos[u.id].x, u.y - previousPos[u.id].y));
        if (tick % 15 === 0) {
          const ground = e.terrainAt(u.x, u.y);
          if (ground.blocked) invalidTerrain++;
          if (u.team === 0 && ground.type === 'forest') forestVisited.add(u.id);
        }
      }
      previousHp[u.id] = u.hp; previousFlash[u.id] = u.attackFlash; previousPos[u.id] = { x: u.x, y: u.y };
    }
    w.remaining = e.getStats().teams.map(t => t.alive);
    if (tick === 1799) snapshot = { config: e.config, map: e.map, width: e.width, height: e.height, seed: e.seed, time: e.time, phase: e.phase, units: e.units.map(u => pick(u, unitKeys)), regiments: e.regiments.map(r => pick(r, regKeys)), projectiles: e.projectiles.map(p => ({ ...p })), effects: e.effects.map(p => ({ ...p })) };
    if (e.phase === 'ended') break;
  }
  const percentile = (array, fraction) => array.length ? array.slice().sort((a, b) => a - b)[Math.floor((array.length - 1) * fraction)] : 0;
  const measured = windows.map((w, i) => ({
    start: i * 10, attackers: w.attackers.size, hits: w.hits, sectors: w.damaged.size,
    meaningfulRegiments: [...w.regiments.values()].filter(r => r.size >= 6).length,
    strikeSpan: percentile(w.ys, .9) - percentile(w.ys, .1), remaining: w.remaining,
    regimentWidths: [...w.regiments].map(([id, positions]) => ({ id, attackers: positions.size, width: percentile([...positions.values()], .9) - percentile([...positions.values()], .1) }))
  }));
  return { engine: e, snapshot, windows: measured, uniqueAttackers: allAttackers.size, damagedDefenderRegiments: allDamagedRegiments.size, rearReplacements: rearReplacements.size, frontDeaths, forestVisited: forestVisited.size, forestStrikes, invalidTerrain, maximumStep, time: e.time, phase: e.phase };
}
// FRONTAGE_METRICS_END

let frontageRun;
function getFrontageRun() { return frontageRun || (frontageRun = measureFrontage(BattleEngine)); }

test('Depth-stacked regiments form a broad active front with real hits across enemy sectors', () => {
  const m = getFrontageRun();
  const contested = m.windows.filter(w => Math.min(...w.remaining) >= 96);
  const sustained = contested.filter(w => w.sectors >= 3 && w.meaningfulRegiments >= 3 && w.attackers >= 80 && w.strikeSpan >= 1100);
  assert.equal(m.damagedDefenderRegiments, 4, 'Some side formations never faced actual attacks');
  assert(sustained.length >= 3, 'The assault did not sustain multiple separated active contact areas');
  assert(m.uniqueAttackers >= 256, `Too few soldiers ever joined melee: ${m.uniqueAttackers} of640`);
  const peak = Math.max(...contested.map(w => w.attackers));
  assert(peak >= 110, `Peak melee participation stayed too small: ${peak}`);
  const broadIndividualFronts = new Set(contested.flatMap(w => w.regimentWidths.filter(r => r.attackers >= 12 && r.width >= 90).map(r => r.id)));
  assert(broadIndividualFronts.size >= 3, 'Wide regiment banners hid narrow or idle individual contact points');
  const sustainedIndividualFronts = contested.filter(w => w.regimentWidths.filter(r => r.attackers >= 12 && r.width >= 90).length >= 3);
  assert(sustainedIndividualFronts.length >= 3, 'Individual contact fronts collapsed into narrow knots after the first engagement');
  assert.equal(m.invalidTerrain, 0);
  assert(m.maximumStep <= GameData.TYPES.swordsman.speed / 30 * 1.8 + .25, 'Front redistribution teleported soldiers');
  checkWorld(m.engine);
  return { distinctActualMeleeAttackers: m.uniqueAttackers, peakAttackersIn10Seconds: peak, damagedDefenderRegiments: m.damagedDefenderRegiments, sustainedBroadWindows: sustained.length, withinRegimentBroadFronts: broadIndividualFronts.size, sustainedWithinRegimentWindows: sustainedIndividualFronts.length };
});

test('Rear soldiers actually strike after casualties open gaps in their front ranks', () => {
  const m = getFrontageRun();
  assert(m.frontDeaths >= 10, 'The fixture never created enough frontline casualty gaps');
  assert(m.rearReplacements >= 64, 'Rear soldiers did not feed into actual melee after frontline losses');
  return { initialFrontRankDeaths: m.frontDeaths, rearSoldiersLaterDeliveringHits: m.rearReplacements };
});

test('Ridge and woodland combat routes reach several active fronts without terrain jams', () => {
  const details = {};
  for (const [map, ys] of [['highland', [900, 2160, 3390]], ['greenwood', [550, 1570, 2825]]]) {
    const m = measureFrontage(BattleEngine, { map, count: 480, parallel: true, ys, seconds: 180 });
    assert.equal(m.invalidTerrain, 0, `${map}: active troops entered blocked terrain`);
    assert.equal(m.damagedDefenderRegiments, 3, `${map}: one or more routes never reached combat`);
    const broad = m.windows.filter(w => w.sectors === 3 && w.meaningfulRegiments >= 3 && w.attackers >= 48 && w.strikeSpan >= 1000);
    assert(broad.length >= 2, `${map}: combat never spread across the three approaches`);
    assert(m.maximumStep <= GameData.TYPES.swordsman.speed / 30 * 1.8 + .25, `${map}: route traversal teleported soldiers`);
    if (map === 'greenwood') assert(m.forestVisited >= 80 && m.forestStrikes >= 20, 'Woodland troops did not navigate and actually fight in the trees');
    checkWorld(m.engine);
    details[map] = { actualMeleeAttackers: m.uniqueAttackers, damagedDefenderRegiments: m.damagedDefenderRegiments, broadCombatWindows: broad.length, forestStrikes: m.forestStrikes };
  }
  return details;
});

// This fixture deliberately sends several complete mixed formations through
// shared terrain approaches. Separate, perfectly aligned regiments at each
// passage do not expose the long traffic files reported in real battles.
function measureCongestedDesert(Engine) {
  const roster = { swordsman: 320, pikeman: 160, halberdier: 160, guardian: 160,
    berserker: 160, archer: 120, cavalry: 80, elephant: 8, commander: 2 };
  const e = new Engine({ map: 'desert', seed: 4719, armies: [
    army('Azure reinforcements', roster, 'aggressive'),
    army('Ember reinforcements', roster, 'aggressive')
  ] });
  for (let team = 0; team < 2; team++) {
    const melee = e.regiments.filter(r => r.team === team && !GameData.TYPES[r.type].ranged && !GameData.TYPES[r.type].commander);
    melee.forEach((r, i) => place(e, r, team === 0 ? 2690 - Math.floor(i / 3) * 360 : 3650 + Math.floor(i / 3) * 360, 790 + (i % 3) * 400));
    e.regiments.filter(r => r.team === team && GameData.TYPES[r.type].ranged).forEach((r, i) => place(e, r, team === 0 ? 1750 : 4600, 950 + i * 320));
    e.regiments.filter(r => r.team === team && GameData.TYPES[r.type].commander).forEach((r, i) => place(e, r, team === 0 ? 1850 : 4500, 1350 + i * 120));
  }
  const previous = e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp, flash: u.attackFlash }));
  let windowStart = e.units.map(u => ({ x: u.x, y: u.y }));
  const lastActualAttack = new Float64Array(e.units.length); lastActualAttack.fill(-1000);
  const allAttackers = new Set(), windows = [];
  let windowAttackers = new Set(), maximumStep = 0, invalidTerrain = 0;
  const percentile = (a, p) => a.length ? a.slice().sort((x, y) => x - y)[Math.floor((a.length - 1) * p)] : 0;
  // Measure a regiment's actual footprint along its principal axis. A diagonal
  // single file must not look wide just because its x and y ranges are large.
  const footprint = units => {
    const x = units.reduce((sum, u) => sum + u.x, 0) / Math.max(1, units.length);
    const y = units.reduce((sum, u) => sum + u.y, 0) / Math.max(1, units.length);
    let xx = 0, xy = 0, yy = 0;
    for (const u of units) { xx += (u.x - x) ** 2; xy += (u.x - x) * (u.y - y); yy += (u.y - y) ** 2; }
    const angle = .5 * Math.atan2(2 * xy, xx - yy), ax = Math.cos(angle), ay = Math.sin(angle);
    const along = units.map(u => (u.x - x) * ax + (u.y - y) * ay);
    const across = units.map(u => -(u.x - x) * ay + (u.y - y) * ax);
    return { length: percentile(along, .9) - percentile(along, .1), width: percentile(across, .9) - percentile(across, .1) };
  };
  e.start();
  for (let tick = 0; tick < 120 * 30 && e.phase !== 'ended'; tick++) {
    e.step(1 / 30);
    for (const u of e.units) {
      const prior = previous[u.id], t = GameData.TYPES[u.type];
      if (u.alive && !u.routed) {
        const displacement = Math.hypot(u.x - prior.x, u.y - prior.y);
        maximumStep = Math.max(maximumStep, displacement);
        assert(displacement <= t.speed / 30 * 1.8 + .25, `Congestion recovery teleported unit ${u.id}`);
        if (tick % 30 === 29 && e.terrainAt(u.x, u.y).blocked) invalidTerrain++;
      }
      if (!t.ranged && !t.commander && u.attackFlash > .999 && prior.flash < .999 && u.target >= 0) {
        const target = e.units[u.target];
        if (target && previous[target.id].hp > target.hp + .001) {
          allAttackers.add(u.id); windowAttackers.add(u.id); lastActualAttack[u.id] = e.time;
        }
      }
    }
    for (const u of e.units) Object.assign(previous[u.id], { x: u.x, y: u.y, hp: u.hp, flash: u.attackFlash });
    if ((tick + 1) % 300 === 0) {
      const eligible = e.units.filter(u => {
        const r = e.regiments[u.regiment], t = GameData.TYPES[u.type];
        return u.alive && !u.routed && !t.ranged && !t.commander && !r.fallingBack && r._rallyTime <= 0;
      });
      const immobile = eligible.filter(u => e.time - lastActualAttack[u.id] > 10 && Math.hypot(u.x - windowStart[u.id].x, u.y - windowStart[u.id].y) < 12);
      const thinIdleRegiments = [];
      for (const r of e.regiments) {
        if (r.alive < 60 || r.fallingBack || r._rallyTime > 0 || GameData.TYPES[r.type].ranged || GameData.TYPES[r.type].commander) continue;
        const units = r.unitIds.map(id => e.units[id]).filter(u => u.alive && !u.routed);
        const shape = footprint(units), recentAttackers = units.filter(u => e.time - lastActualAttack[u.id] <= 10).length;
        if (shape.length > 300 && shape.width < 40 && shape.length > shape.width * 8 && recentAttackers < 6) thinIdleRegiments.push({ id: r.id, alive: r.alive, length: shape.length, width: shape.width });
      }
      windows.push({ time: Math.round(e.time), attackers: windowAttackers.size, eligible: eligible.length,
        immobileNonattacking: immobile.length, immobileFraction: immobile.length / Math.max(1, eligible.length),
        thinIdleRegiments, remaining: e.getStats().teams.map(s => s.alive) });
      windowAttackers = new Set(); windowStart = e.units.map(u => ({ x: u.x, y: u.y }));
      checkWorld(e);
    }
  }
  return { engine: e, windows, uniqueAttackers: allAttackers.size, maximumStep, invalidTerrain };
}

test('Congested mixed desert armies open usable fronts and commit idle reinforcements', () => {
  const m = measureCongestedDesert(BattleEngine);
  const late = m.windows.filter(w => w.time >= 60 && Math.min(...w.remaining) >= 300 && w.eligible >= 800);
  assert(late.length >= 4, 'The fixture did not retain enough troops to exercise sustained congestion');
  const active = late.filter(w => w.attackers / w.eligible >= .07);
  const idleFractions = late.map(w => w.immobileFraction).sort((a, b) => a - b);
  const medianIdle = idleFractions[Math.floor(idleFractions.length / 2)];
  const thinWindows = new Map();
  for (const w of late) for (const r of w.thinIdleRegiments) thinWindows.set(r.id, (thinWindows.get(r.id) || 0) + 1);
  const persistentThinWindows = Math.max(0, ...thinWindows.values());
  assert(active.length >= 3, `Real melee remained too concentrated: only ${active.length} late windows reached 7% active participation`);
  assert(medianIdle <= .60, `Too many eligible reinforcements remained stationary without attacking: ${(medianIdle * 100).toFixed(1)}%`);
  assert(persistentThinWindows < 4, `An idle regiment remained a near-single-file tail in ${persistentThinWindows} late samples`);
  assert.equal(m.invalidTerrain, 0, 'Congestion escape crossed blocked terrain');
  checkWorld(m.engine);
  return { actualMeleeAttackers: m.uniqueAttackers, activeLateWindows: active.length,
    medianImmobileNonattackingFraction: Math.round(medianIdle * 1000) / 1000,
    persistentThinTailWindows: persistentThinWindows, final: m.windows.at(-1), maximumStep: m.maximumStep };
});

function commandPositions(e, r) {
  return r.unitIds.map(id => e.units[id]).filter(u => u.alive && !u.routed)
    .map(u => ({ id: u.id, x: u.x, y: u.y, hp: u.hp, lastAttack: u._lastAttack }));
}
function commandMotion(e, before) {
  return before.filter(p => e.units[p.id].alive && !e.units[p.id].routed).map(p => {
    const u = e.units[p.id]; return { id: p.id, dx: u.x - p.x, dy: u.y - p.y, distance: Math.hypot(u.x - p.x, u.y - p.y) };
  });
}
function checkedCommandSteps(e, ticks, ids) {
  for (let tick = 0; tick < ticks && e.phase !== 'ended'; tick++) {
    const previous = bodySnapshot(e); e.step(1 / 30);
    assertBoundedMovement(e, previous, ids); assertPassable(e, ids);
  }
}

test('A paused Move order promptly disengages troops already striking an enemy', () => {
  const e = new BattleEngine(config({ swordsman: 16 }, { guardian: 16 }));
  const troops = regiment(e, 0), enemy = regiment(e, 1), ids = new Set([troops.id]);
  place(e, troops, 2900, 2200); place(e, enemy, 3060, 2200);
  e.command([enemy.id], 'hold'); e.start(); step(e, 12);
  const before = commandPositions(e, troops), fighting = before.filter(u => u.lastAttack >= e.time - 1.5);
  assert(fighting.length >= 8, 'The fixture did not put enough selected troops into actual melee');
  e.pause(); const pausedTime = e.time;
  assert.equal(e.command([troops.id], 'move', 2200, 2200), 1);
  step(e, 1);
  assert.equal(e.time, pausedTime, 'Issuing an order while paused advanced the battle');
  assert.deepEqual(commandPositions(e, troops), before, 'A paused order physically repositioned soldiers');
  e.resume(); checkedCommandSteps(e, 15, ids);
  const immediate = commandMotion(e, fighting), responding = immediate.filter(p => p.dx < -2).length;
  assert(responding >= Math.ceil(fighting.length * .6), `Only ${responding}/${fighting.length} engaged soldiers obeyed Move within half a second`);
  checkedCommandSteps(e, 75, ids);
  const final = commandMotion(e, fighting), retreat = -final.reduce((s, p) => s + p.dx, 0) / final.length;
  assert(retreat > 45, `Engaged soldiers remained fixed on the enemy after Move: only ${retreat.toFixed(1)} units of retreat in three seconds`);
  assert.equal(troops.order, 'move'); checkWorld(e);
  return { engagedBeforeOrder: fighting.length, respondingWithinHalfSecond: responding, meanRetreatInThreeSeconds: Math.round(retreat * 10) / 10 };
});

test('Hold stops actual troops immediately during a stretched rock-passage maneuver', () => {
  const e = new BattleEngine(config({ swordsman: 160 }, { militia: 1 }, 'desert'));
  const troops = regiment(e, 0), enemy = regiment(e, 1), ids = new Set([troops.id]);
  place(e, troops, 2600, 1330); place(e, enemy, 5900, 4000);
  e.command([enemy.id], 'hold'); e.start(); e.command([troops.id], 'move', 3800, 1330);
  step(e, 14); const before = commandPositions(e, troops);
  assert(averageX(before) > 2850, 'The formation had not entered the rocky approach');
  assert.equal(e.command([troops.id], 'hold'), 1);
  checkedCommandSteps(e, 30, ids);
  const motion = commandMotion(e, before), mean = motion.reduce((s, p) => s + p.distance, 0) / motion.length;
  const maximum = Math.max(...motion.map(p => p.distance)), stopped = motion.filter(p => p.distance < 8).length;
  assert(mean < 5 && maximum < 15 && stopped >= Math.ceil(before.length * .9),
    `Hold marched toward the old banner/slots: mean ${mean.toFixed(1)}, maximum ${maximum.toFixed(1)}, stopped ${stopped}/${before.length}`);
  assert.equal(troops.order, 'hold'); checkWorld(e);
  return { soldiersHeld: stopped, total: before.length, meanMovementInOneSecond: Math.round(mean * 100) / 100, maximumMovementInOneSecond: Math.round(maximum * 100) / 100 };
});

test('Charge promptly reverses a stale terrain route and reaches the enemy behind it', () => {
  const e = new BattleEngine(config({ cavalry: 16 }, { guardian: 24 }, 'desert'));
  const troops = regiment(e, 0), enemy = regiment(e, 1), ids = new Set([troops.id]);
  place(e, troops, 2800, 1060); place(e, enemy, 2300, 1060);
  e.command([enemy.id], 'hold'); e.start(); e.command([troops.id], 'move', 3600, 1060);
  step(e, 4); const before = commandPositions(e, troops), orderedAt = e.time;
  assert(averageX(before) > 2880, 'The cavalry did not first follow its old eastward route');
  assert.equal(e.command([troops.id], 'charge'), 1);
  checkedCommandSteps(e, 15, ids);
  const immediate = commandMotion(e, before), toward = immediate.filter(p => p.dx < -5).length;
  const wrong = immediate.filter(p => p.dx > 2).length, mean = immediate.reduce((s, p) => s + p.dx, 0) / immediate.length;
  // Acceleration deliberately replaces instant mounted speed: a fresh
  // half-second run-up covers about 7 units. Direction must still reverse
  // immediately, and a full second must produce substantial real travel.
  assert(toward >= 10 && wrong <= 3 && mean < -5,
    `Charge kept the stale route: ${toward}/16 toward the enemy, ${wrong} wrong-way, mean x ${mean.toFixed(1)}`);
  checkedCommandSteps(e, 15, ids);
  const advanceAtOneSecond = -commandMotion(e, before).reduce((sum, p) => sum + p.dx, 0) / immediate.length;
  assert(advanceAtOneSecond > 25, `The redirected charge failed to accelerate: ${advanceAtOneSecond.toFixed(1)} units in one second`);
  const damageBefore = enemy.unitIds.reduce((s, id) => s + e.units[id].hp, 0);
  const attackers = new Set(); let observedHits = 0;
  for (let tick = 0; tick < 330 && e.phase !== 'ended'; tick++) {
    const previous = e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp, flash: u.attackFlash }));
    e.step(1 / 30); assertBoundedMovement(e, previous, ids); assertPassable(e, ids);
    for (const id of troops.unitIds) {
      const u = e.units[id];
      if (u.attackFlash > .999 && previous[id].flash < .999 && u.target >= 0 &&
          previous[u.target].hp > e.units[u.target].hp + .001) { attackers.add(id); observedHits++; }
    }
  }
  const damage = damageBefore - enemy.unitIds.reduce((s, id) => s + Math.max(0, e.units[id].hp), 0);
  assert(attackers.size >= 6 && observedHits >= 8 && damage > 20, 'The newly directed charge did not turn its movement into actual melee within twelve seconds');
  assert(e.time <= orderedAt + 12.1); checkWorld(e);
  return { towardWithinHalfSecond: toward, wrongWayWithinHalfSecond: wrong, meanInitialAdvance: Math.round(-mean * 10) / 10,
    advanceWithinOneSecond: Math.round(advanceAtOneSecond * 10) / 10,
    actualChargeAttackers: attackers.size, observedChargeHits: observedHits, targetDamage: Math.round(damage) };
});

test('Explicit Charge pursues the chosen enemy and obeys subsequent Move and Hold orders', () => {
  const e = new BattleEngine(config({ cavalry: 16 }, { militia: 8, guardian: 16 }));
  const troops = regiment(e, 0), nearby = regiment(e, 1, 'militia'), chosen = regiment(e, 1, 'guardian');
  const ids = new Set([troops.id]);
  place(e, troops, 2800, 2400); place(e, nearby, 2900, 2750); place(e, chosen, 3600, 2200);
  e.command(e.regiments.map(r => r.id), 'hold'); e.start();
  const origin = commandPositions(e, troops);
  assert.equal(e.command([troops.id], 'charge', undefined, undefined, { targetRegimentId: chosen.id }), 1);
  checkedCommandSteps(e, 60, ids);
  const directed = commandMotion(e, origin), dx = directed.reduce((s, p) => s + p.dx, 0) / directed.length;
  const dy = directed.reduce((s, p) => s + p.dy, 0) / directed.length;
  assert(dx > 100 && dy < -15, `Explicit Charge ignored the selected enemy: mean displacement (${dx.toFixed(1)}, ${dy.toFixed(1)})`);
  const beforeMove = commandPositions(e, troops);
  assert.equal(e.command([troops.id], 'move', 2000, 2400), 1); checkedCommandSteps(e, 30, ids);
  const moved = commandMotion(e, beforeMove), reverse = -moved.reduce((s, p) => s + p.dx, 0) / moved.length;
  // The accepted reversal immediately changes direction and builds a new
  // gallop. Check direction at the original one-second mark, and keep a
  // separate distance gate after half a second of additional acceleration.
  assert(reverse > 8 && moved.filter(p => p.dx < -3).length >= 12 && moved.filter(p => p.dx > 2).length <= 3,
    `Move did not cancel explicit Charge promptly: reverse progress ${reverse.toFixed(1)}`);
  checkedCommandSteps(e, 15, ids);
  const reverseAtOneAndHalfSeconds = -commandMotion(e, beforeMove).reduce((sum, p) => sum + p.dx, 0) / moved.length;
  assert(reverseAtOneAndHalfSeconds > 30, `The reversed Move failed to accelerate: ${reverseAtOneAndHalfSeconds.toFixed(1)} units in 1.5 seconds`);
  assert.equal(e.command([troops.id], 'charge', undefined, undefined, { targetRegimentId: chosen.id }), 1);
  checkedCommandSteps(e, 30, ids); const beforeHold = commandPositions(e, troops);
  assert.equal(e.command([troops.id], 'hold'), 1); checkedCommandSteps(e, 30, ids);
  const stopped = commandMotion(e, beforeHold), stopMean = stopped.reduce((s, p) => s + p.distance, 0) / stopped.length;
  assert(stopMean < 5 && Math.max(...stopped.map(p => p.distance)) < 15, 'Hold did not cancel explicit Charge at the actual soldier positions');
  const validOrder = { order: troops.order, x: troops.targetX, y: troops.targetY }, heldPositions = commandPositions(e, troops);
  assert.equal(e.command([troops.id], 'charge', undefined, undefined, { targetRegimentId: troops.id }), 0, 'A friendly regiment was accepted as a Charge target');
  assert.deepEqual({ order: troops.order, x: troops.targetX, y: troops.targetY }, validOrder, 'Rejected friendly targeting replaced the existing order');
  assert.deepEqual(commandPositions(e, troops), heldPositions, 'Rejected targeting changed troop positions');
  assert.equal(e.command([troops.id], 'charge', undefined, undefined, { targetRegimentId: chosen.id }), 1);
  let hits = 0; const participants = new Set();
  for (let tick = 0; tick < 14 * 30 && e.phase !== 'ended'; tick++) {
    const previous = e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp, flash: u.attackFlash }));
    e.step(1 / 30); assertBoundedMovement(e, previous, ids); assertPassable(e, ids);
    for (const id of troops.unitIds) {
      const u = e.units[id];
      if (u.attackFlash > .999 && previous[id].flash < .999 && u.target >= 0 && e.units[u.target].regiment === chosen.id &&
          previous[u.target].hp > e.units[u.target].hp + .001) { hits++; participants.add(id); }
    }
  }
  assert(hits >= 8 && participants.size >= 4, 'The selected distant enemy was approached but not actually engaged');
  checkWorld(e);
  return { chosenEnemyAdvanceInTwoSeconds: { x: Math.round(dx), y: Math.round(dy) }, moveCancellationReverseInOneSecond: Math.round(reverse),
    moveCancellationReverseInOneAndHalfSeconds: Math.round(reverseAtOneAndHalfSeconds * 10) / 10,
    meanMovementAfterHold: Math.round(stopMean * 100) / 100, chosenEnemyHitCount: hits, chosenEnemyAttackers: participants.size, friendlyTargetRejected: true };
});

test('A charge continues toward living enemies after its first target is defeated', () => {
  const e = new BattleEngine(config({ cavalry: 16 }, { militia: 1, guardian: 16 }));
  const troops = regiment(e, 0), first = regiment(e, 1, 'militia'), next = regiment(e, 1, 'guardian');
  const ids = new Set([troops.id]); place(e, troops, 2500, 2200); place(e, first, 2900, 2200); place(e, next, 3400, 2200);
  e.command([first.id, next.id], 'hold'); e.start(); e.command([troops.id], 'charge');
  let firstDefeated = null, atDefeat = null, afterThree = null, secondaryHits = 0;
  for (let tick = 0; tick < 28 * 30 && e.phase !== 'ended'; tick++) {
    const previous = e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp, flash: u.attackFlash }));
    e.step(1 / 30); assertBoundedMovement(e, previous, ids); assertPassable(e, ids);
    if (firstDefeated === null && first.alive === 0) { firstDefeated = e.time; atDefeat = commandPositions(e, troops); }
    if (firstDefeated !== null && afterThree === null && e.time >= firstDefeated + 3) afterThree = commandMotion(e, atDefeat);
    for (const id of troops.unitIds) {
      const u = e.units[id];
      if (u.attackFlash > .999 && previous[id].flash < .999 && u.target >= 0 && e.units[u.target].regiment === next.id &&
          previous[u.target].hp > e.units[u.target].hp + .001) secondaryHits++;
    }
  }
  assert(firstDefeated !== null && firstDefeated < 12, 'The first charge target was not defeated promptly');
  const onward = afterThree ? afterThree.reduce((s, p) => s + p.dx, 0) / afterThree.length : 0;
  assert(onward > 80, `Charge stalled after target defeat: only ${onward.toFixed(1)} units of onward progress in three seconds`);
  assert(secondaryHits >= 8, 'The charge never engaged its next living opponent');
  assert(!troops.fallingBack && troops._rallyTime <= 0, 'The target-rejection fixture no longer has commandable soldiers');
  assert.equal(e.command([troops.id], 'move', 2200, 2200), 1);
  const validOrder = { order: troops.order, x: troops.targetX, y: troops.targetY }, positions = commandPositions(e, troops);
  assert.equal(e.command([troops.id], 'charge', undefined, undefined, { targetRegimentId: first.id }), 0, 'A defeated regiment was accepted as a Charge target');
  assert.deepEqual({ order: troops.order, x: troops.targetX, y: troops.targetY }, validOrder, 'Rejected dead targeting replaced a valid Move order');
  assert.deepEqual(commandPositions(e, troops), positions, 'Rejected dead targeting moved the soldiers');
  checkWorld(e);
  return { firstTargetDefeatedAt: Math.round(firstDefeated * 10) / 10, onwardProgressInThreeSeconds: Math.round(onward), observedSecondaryHits: secondaryHits };
});

test('Ranged Charge closes to firing range and keeps firing instead of automatically kiting', () => {
  const e = new BattleEngine(config({ javelin: 16 }, { guardian: 24 }));
  const troops = regiment(e, 0), enemy = regiment(e, 1), ids = new Set([troops.id]);
  place(e, troops, 2500, 2200); place(e, enemy, 3250, 2200);
  e.command(e.regiments.map(r => r.id), 'hold'); e.start(); step(e, 1);
  const before = commandPositions(e, troops), shooters = new Set();
  const enemyHealth = () => enemy.unitIds.reduce((sum, id) => sum + Math.max(0, e.units[id].hp), 0);
  const nearestDistance = u => Math.min(...enemy.unitIds.map(id => e.units[id]).filter(v => v.alive && !v.routed)
    .map(v => Math.hypot(v.x - u.x, v.y - u.y)));
  const initialHealth = enemyHealth(); let minimumFiringGap = Infinity;
  assert.equal(e.command([troops.id], 'charge'), 1);
  checkedCommandSteps(e, 30, ids);
  const immediate = commandMotion(e, before), meanAdvance = immediate.reduce((sum, p) => sum + p.dx, 0) / immediate.length;
  assert(meanAdvance > 30 && immediate.filter(p => p.dx > 20).length >= 12,
    `Ranged Charge failed to approach an out-of-range enemy promptly: mean advance ${meanAdvance.toFixed(1)}`);
  for (let tick = 0; tick < 12 * 30; tick++) {
    checkedCommandSteps(e, 1, ids);
    for (const p of e.projectiles) if (p.team === 0 && p.kind === 'javelin' && e.units[p.target].regiment === enemy.id) shooters.add(p.attacker);
    for (const id of troops.unitIds) minimumFiringGap = Math.min(minimumFiringGap, nearestDistance(e.units[id]));
  }
  const rangedDamage = initialHealth - enemyHealth();
  assert(shooters.size >= 8 && rangedDamage > 20, 'Ranged Charge approached but failed to deliver real projectile damage');
  assert(minimumFiringGap > 100, `Ranged Charge unnecessarily rushed into melee: minimum distance ${minimumFiringGap.toFixed(1)}`);
  const firingX = averageX(commandPositions(e, troops));
  assert.equal(e.command([enemy.id], 'move', firingX + 80, 2200), 1);
  let enteringSkirmishRange = false;
  for (let tick = 0; tick < 12 * 30; tick++) {
    checkedCommandSteps(e, 1, ids);
    if (troops.unitIds.filter(id => nearestDistance(e.units[id]) < 125).length >= 8) { enteringSkirmishRange = true; break; }
  }
  assert(enteringSkirmishRange, 'The ordinary enemy Move never brought a majority of the shooters near skirmish distance');
  const closeBefore = commandPositions(e, troops), closeHealth = enemyHealth(), closeShooters = new Set();
  let closeCount = 0;
  for (let tick = 0; tick < 3 * 30; tick++) {
    checkedCommandSteps(e, 1, ids);
    closeCount = Math.max(closeCount, troops.unitIds.filter(id => nearestDistance(e.units[id]) < 112).length);
    for (const p of e.projectiles) if (p.team === 0 && p.kind === 'javelin' && e.units[p.target].regiment === enemy.id) closeShooters.add(p.attacker);
  }
  const closeMotion = commandMotion(e, closeBefore), retreat = -closeMotion.reduce((sum, p) => sum + p.dx, 0) / closeMotion.length;
  assert(closeCount >= 8, 'Charge shooters kept backing away before opponents could enter skirmish distance');
  assert(retreat < 10 && closeMotion.filter(p => p.dx > -12).length >= 12,
    `Ranged Charge was overridden by automatic kiting: mean retreat ${retreat.toFixed(1)}`);
  assert(closeShooters.size >= 4 && closeHealth - enemyHealth() > 5, 'Ranged Charge ceased effective fire against the approaching enemy');
  assert.equal(troops.order, 'charge'); checkWorld(e);

  const directed = new BattleEngine(config({ javelin: 16 }, { militia: 16, guardian: 24 }));
  const selected = regiment(directed, 0), distraction = regiment(directed, 1, 'militia'), chosen = regiment(directed, 1, 'guardian');
  const selectedIds = new Set([selected.id]);
  place(directed, selected, 2800, 2200); place(directed, distraction, 2800, 2390); place(directed, chosen, 3500, 2200);
  directed.command(directed.regiments.map(r => r.id), 'hold'); directed.start(); step(directed, 1);
  assert(selected.unitIds.some(id => directed.units[id]._lastAttack > 0), 'The nearby distraction never attracted a real shot');
  const selectedBefore = commandPositions(directed, selected);
  const chosenHealth = chosen.unitIds.reduce((sum, id) => sum + directed.units[id].hp, 0);
  assert.equal(directed.command([selected.id], 'charge', undefined, undefined, { targetRegimentId: chosen.id }), 1);
  checkedCommandSteps(directed, 60, selectedIds);
  const directedMotion = commandMotion(directed, selectedBefore);
  const chosenAdvance = directedMotion.reduce((sum, p) => sum + p.dx, 0) / directedMotion.length;
  assert(chosenAdvance > 60 && directedMotion.filter(p => p.dx > 30).length >= 12,
    `Nearer ranged targets stopped the explicit Charge: mean chosen-target advance ${chosenAdvance.toFixed(1)}`);
  const chosenShooters = new Set();
  for (let tick = 0; tick < 11 * 30; tick++) {
    checkedCommandSteps(directed, 1, selectedIds);
    for (const p of directed.projectiles) if (p.team === 0 && p.kind === 'javelin' && directed.units[p.target].regiment === chosen.id) chosenShooters.add(p.attacker);
  }
  const chosenDamage = chosenHealth - chosen.unitIds.reduce((sum, id) => sum + Math.max(0, directed.units[id].hp), 0);
  assert(chosenShooters.size >= 8 && chosenDamage > 20, 'Explicit ranged Charge did not turn the selected-target approach into effective fire');
  checkWorld(directed);
  return { meanAdvanceInOneSecond: Math.round(meanAdvance), projectileShooters: shooters.size,
    rangedDamage: Math.round(rangedDamage), minimumFiringGap: Math.round(minimumFiringGap),
    closeRangeShooters: closeShooters.size, meanAutomaticRetreat: Math.round(retreat * 10) / 10,
    explicitRangedAdvanceInTwoSeconds: Math.round(chosenAdvance), selectedTargetShooters: chosenShooters.size,
    selectedTargetDamage: Math.round(chosenDamage) };
});

test('Large group movement spreads destinations around the order and actually advances', () => {
  const e = new BattleEngine(config({ swordsman: 4800, archer: 1600, cavalry: 800 }, { militia: 1 }));
  const groups = e.regiments.filter(r => r.team === 0), enemy = regiment(e, 1), ids = new Set(groups.map(r => r.id));
  place(e, enemy, 5900, 4000); e.command([enemy.id], 'hold'); e.start();
  const before = new Map(groups.map(r => [r.id, commandPositions(e, r)]));
  assert.equal(groups.length, 50);
  assert.equal(e.command(groups.map(r => r.id), 'move', 4000, 2200), groups.length);
  const goals = groups.map(r => ({ id: r.id, x: r.targetX, y: r.targetY }));
  assert.equal(new Set(goals.map(p => p.x.toFixed(2) + ',' + p.y.toFixed(2))).size, groups.length, 'Group orders collapsed multiple destinations onto the same edge point');
  assert(goals.every(p => p.x > 80 && p.x < e.width - 80 && p.y > 80 && p.y < e.height - 80), 'Group destinations were clamped onto battlefield edges');
  assert(goals.every(p => Math.hypot(p.x - 4000, p.y - 2200) < 1700), 'A central order sent parts of the army far from the commanded area');
  checkedCommandSteps(e, 60, ids);
  const progress = groups.map(r => {
    const start = before.get(r.id), x = averageX(start), y = start.reduce((s, p) => s + p.y, 0) / start.length;
    const goal = goals.find(p => p.id === r.id), dx = goal.x - x, dy = goal.y - y, distance = Math.hypot(dx, dy);
    const motion = commandMotion(e, start);
    return motion.reduce((s, p) => s + (p.dx * dx + p.dy * dy) / distance, 0) / motion.length;
  });
  const advancing = progress.filter(value => value > 20).length;
  assert(advancing >= 40, `Only ${advancing}/50 formations made meaningful progress toward a large group order`);
  checkWorld(e);
  return { formations: groups.length, distinctDestinations: goals.length, advancingWithinTwoSeconds: advancing, meanAdvance: Math.round(progress.reduce((s, value) => s + value, 0) / progress.length) };
});

test('Coordinated Charge commits the natural front before opening overtaking lanes', () => {
  const outcomes = {};
  for (const mode of ['wholeArmy', 'frontOnly']) {
    const roster = { guardian: 320, pikeman: 320, swordsman: 640, cavalry: 160 };
    const cfg = config(roster, roster, 'frost', 120001); cfg.armies[1].doctrine = 'flank';
    const e = new BattleEngine(cfg), groups = e.regiments.filter(r => r.team === 0), ids = new Set(groups.map(r => r.id));
    const frontX = Math.max(...groups.map(r => r.x)), front = groups.filter(r => r.x >= frontX - 90);
    assert.equal(e.units.length, 2880); assert.equal(groups.length, 10); assert.equal(front.length, 5);
    e.start(); step(e, 1);
    const before = new Map(front.map(r => [r.id, commandPositions(e, r)]));
    const commanded = mode === 'wholeArmy' ? groups : front;
    assert.equal(e.command(commanded.map(r => r.id), 'charge'), commanded.length);
    const goals = new Map(front.map(r => {
      const target = e.getRegiment(r.commandTargetId >= 0 ? r.commandTargetId : r._enemyId);
      assert(target && target.team !== r.team && target.alive > 0, 'Charge failed to identify a living hostile formation');
      const positions = commandPositions(e, target);
      return [r.id, { x: averageX(positions), y: positions.reduce((sum, p) => sum + p.y, 0) / positions.length }];
    }));
    checkedCommandSteps(e, 30, ids);
    const progress = front.map(r => {
      const goal = goals.get(r.id), start = before.get(r.id);
      return start.reduce((sum, p) => {
        const u = e.units[p.id], dx = goal.x - p.x, dy = goal.y - p.y;
        return sum + ((u.x - p.x) * dx + (u.y - p.y) * dy) / Math.hypot(dx, dy);
      }, 0) / start.length;
    });
    const meanAdvance = progress.reduce((sum, value) => sum + value, 0) / progress.length;
    const advancing = progress.filter(value => value > 12).length;
    assert(advancing >= 4 && meanAdvance > 15,
      `${mode} Charge reformed sideways before advancing: ${advancing}/5 fronts beyond twelve units, mean ${meanAdvance.toFixed(1)} in one second`);
    checkWorld(e);
    outcomes[mode] = { formationsCommanded: commanded.length, frontFormations: front.length, frontsAdvancingInOneSecond: advancing,
      meanFrontAdvance: Math.round(meanAdvance * 10) / 10, individualFrontAdvances: progress.map(value => Math.round(value * 10) / 10) };
  }
  return outcomes;
});

test('Automatic troops strike an adjacent unassigned enemy before drifting back to their regiment target', () => {
  const e = new BattleEngine(config({ cavalry: 16 }, { guardian: 16, militia: 16 }));
  const troops = regiment(e, 0), assigned = regiment(e, 1, 'guardian'), nearby = regiment(e, 1, 'militia');
  place(e, troops, 2800, 2200); place(e, assigned, 2920, 2200); place(e, nearby, 2800, 2350);
  e.command([assigned.id, nearby.id], 'hold'); e.start();
  assert.equal(troops.order, 'auto'); assert.equal(troops._enemyId, assigned.id, 'The fixture did not assign the separate frontal enemy');
  const touching = troops.unitIds.filter(id => nearby.unitIds.some(other => {
    const u = e.units[id], v = e.units[other];
    return Math.hypot(u.x - v.x, u.y - v.y) <= GameData.TYPES[u.type].range + v._radius;
  }));
  assert(touching.length >= 1, 'No automatic soldier started within reach of the unassigned flank enemy');
  const ids = new Set([troops.id, assigned.id, nearby.id]);
  let hits = 0, damage = 0, firstHit = null, minimumRatio = Infinity;
  for (let tick = 0; tick < 60; tick++) {
    const previous = e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp, flash: u.attackFlash })); e.step(1 / 30);
    assertBoundedMovement(e, previous, ids); assertPassable(e, ids);
    minimumRatio = Math.min(minimumRatio, minimumBodyRatio(e, previous, ids));
    for (const id of touching) {
      const u = e.units[id], target = e.units[u.target];
      if (u.attackFlash > .999 && previous[id].flash < .999 && target && target.regiment === nearby.id && target.hp < previous[target.id].hp - .001) {
        hits++; damage += previous[target.id].hp - target.hp; if (firstHit === null) firstHit = e.time;
      }
    }
  }
  assert(hits >= 1 && damage > 5, 'An automatic soldier ignored a reachable unassigned enemy while following its distant formation target');
  assert(firstHit !== null && firstHit <= 1.5, 'Adjacent self-defense waited too long after the initial weapon cooldown');
  assert(minimumRatio >= .90, 'Local self-defense caused body penetration'); checkWorld(e);
  return { initiallyTouchingSoldiers: touching.length, observedLocalHits: hits, damage: Math.round(damage * 10) / 10, firstHitSeconds: Math.round(firstHit * 100) / 100 };
});

test('Active Charge, Move and point Attack keep their direction past adjacent distractions', () => {
  const outcomes = {};
  for (const order of ['charge', 'move', 'attack']) {
    const e = new BattleEngine(config({ cavalry: 16 }, { guardian: 16, militia: 16 }));
    const troops = regiment(e, 0), chosen = regiment(e, 1, 'guardian'), nearby = regiment(e, 1, 'militia');
    place(e, troops, 2800, 2200); place(e, chosen, 3600, 2200); place(e, nearby, 2800, 2350);
    e.command(e.regiments.map(r => r.id), 'hold'); e.start();
    const before = commandPositions(e, troops), touching = before.filter(p => nearby.unitIds.some(other => {
      const u = e.units[p.id], v = e.units[other];
      return Math.hypot(u.x - v.x, u.y - v.y) <= GameData.TYPES[u.type].range + v._radius;
    }));
    assert(touching.length >= 1, 'No commanded soldier started beside the distraction');
    assert.equal(e.command([troops.id], order, 3600, 2200, order === 'charge' ? { targetRegimentId: chosen.id } : undefined), 1);
    const ids = new Set([troops.id, chosen.id, nearby.id]); let minimumRatio = Infinity;
    for (let tick = 0; tick < 60; tick++) {
      const previous = bodySnapshot(e); e.step(1 / 30); assertBoundedMovement(e, previous, ids); assertPassable(e, ids);
      minimumRatio = Math.min(minimumRatio, minimumBodyRatio(e, previous, ids));
    }
    const motion = commandMotion(e, touching), meanAdvance = motion.reduce((sum, p) => sum + p.dx, 0) / motion.length;
    assert(motion.every(p => p.dx > 60) && meanAdvance > 90,
      `${order} was overridden by a side enemy: initial-contact soldiers advanced only ${meanAdvance.toFixed(1)} units in two seconds`);
    const forwardFacing = living(e, new Set([troops.id])).reduce((sum, u) => sum + Math.cos(u.angle), 0) / troops.alive;
    assert(forwardFacing > .8, `${order} turned the moving ranks away from their actual eastward route`);
    assert.equal(troops.order, order); assert(minimumRatio >= .90, 'Directional command passed through enemy bodies'); checkWorld(e);
    outcomes[order] = { initialContactSoldiers: touching.length, meanAdvanceInTwoSeconds: Math.round(meanAdvance), forwardHeadingAlignment: Math.round(forwardFacing * 100) / 100,
      minimumBodyRatio: Number.isFinite(minimumRatio) ? Math.round(minimumRatio * 1000) / 1000 : 'no body contact' };
  }
  // A point Attack may bypass a side contact, but must fight an actual blocker.
  const e = new BattleEngine(config({ swordsman: 32 }, { guardian: 32, militia: 64 }));
  const troops = regiment(e, 0), blocker = regiment(e, 1, 'guardian'), distant = regiment(e, 1, 'militia');
  place(e, troops, 2800, 2200); place(e, blocker, 2800, 2440); place(e, distant, 4500, 2200);
  e.command(e.regiments.map(r => r.id), 'hold'); e.start();
  const before = commandPositions(e, troops), initialHealth = blocker.unitIds.reduce((sum, id) => sum + e.units[id].hp, 0);
  assert.equal(e.command([troops.id], 'attack', 2800, 3400), 1);
  const ids = new Set([troops.id, blocker.id, distant.id]); let minimumRatio = Infinity;
  for (let tick = 0; tick < 240; tick++) {
    const previous = bodySnapshot(e); e.step(1 / 30); assertBoundedMovement(e, previous, ids); assertPassable(e, ids);
    minimumRatio = Math.min(minimumRatio, minimumBodyRatio(e, previous, ids));
  }
  const damage = initialHealth - blocker.unitIds.reduce((sum, id) => sum + Math.max(0, e.units[id].hp), 0);
  const advance = commandMotion(e, before).reduce((sum, p) => sum + p.dy, 0) / before.length;
  assert(damage > 20 && advance > 50, 'Point Attack failed to engage the enemy blocking its ordered route');
  assert(minimumRatio >= .90, 'Point Attack crossed the route blocker without respecting occupied bodies'); checkWorld(e);
  outcomes.blockingEnemy = { damage: Math.round(damage * 10) / 10, meanAdvanceInEightSeconds: Math.round(advance), minimumBodyRatio: Math.round(minimumRatio * 1000) / 1000 };
  return outcomes;
});

test('Manual Retreat immediately withdraws engaged troops despite repeated orders', () => {
  const e = new BattleEngine(config({ swordsman: 32, commander: 1 }, { guardian: 32 }));
  const troops = regiment(e, 0, 'swordsman'), commander = regiment(e, 0, 'commander'), enemy = regiment(e, 1);
  place(e, troops, 2900, 2200); place(e, commander, 2400, 2200); place(e, enemy, 3060, 2200);
  e.command([commander.id, enemy.id], 'hold'); e.start(); step(e, 12);
  const before = commandPositions(e, troops), engaged = before.filter(p => p.lastAttack >= e.time - 1.5);
  assert(engaged.length >= 8, 'The retreat fixture did not establish real melee');
  const initialAngles = new Map(engaged.map(p => [p.id, e.units[p.id].angle]));
  e.pause(); const pausedTime = e.time;
  assert.equal(e.command([troops.id], 'retreat'), 1); step(e, 1);
  assert.equal(e.time, pausedTime); assert.deepEqual(commandPositions(e, troops), before, 'Paused Retreat repositioned the troops');
  assert(engaged.every(p => e.units[p.id].angle === initialAngles.get(p.id)), 'Paused Retreat changed heading without a simulation step');
  e.resume();
  const ids = new Set([troops.id, commander.id, enemy.id]);
  let minimumRatio = Infinity, firstStepMotion = 0, earlyMovement = 0, oneSecondMotion = null, fourSecondRetreat = 0;
  for (let tick = 1; tick <= 300; tick++) {
    const previous = bodySnapshot(e); e.step(1 / 30); assertBoundedMovement(e, previous, ids); assertPassable(e, ids);
    minimumRatio = Math.min(minimumRatio, minimumBodyRatio(e, previous, ids));
    if (tick === 1) firstStepMotion = commandMotion(e, engaged).filter(p => p.dx < -.05).length;
    if (tick === 3) {
      earlyMovement = commandMotion(e, engaged).reduce((sum, p) => sum + p.distance, 0) / engaged.length;
    }
    if ([3, 6, 12].includes(tick)) assert.equal(e.command([troops.id], 'retreat'), 1, 'Repeated Retreat was rejected instead of retaining the active withdrawal');
    if (tick === 30) oneSecondMotion = commandMotion(e, engaged);
    if (tick === 120) {
      fourSecondRetreat = -commandMotion(e, engaged).reduce((sum, p) => sum + p.dx, 0) / engaged.length;
      assert(troops.fallingBack && troops.order === 'retreat', 'Repeated Retreat restarted combat or completed recovery prematurely');
    }
  }
  assert(firstStepMotion >= engaged.length * .5, 'Engaged troops delayed the new withdrawal');
  assert(earlyMovement > 2, `Withdrawal moved only ${earlyMovement.toFixed(2)} units in 0.1s`);
  const responding = oneSecondMotion.filter(p => p.dx < -5).length;
  assert(responding >= Math.ceil(engaged.length * .6), `Only ${responding}/${engaged.length} engaged troops withdrew in the first second`);
  const motion = commandMotion(e, engaged), retreat = -motion.reduce((sum, p) => sum + p.dx, 0) / motion.length;
  assert(fourSecondRetreat > 70, `Manual withdrawal stayed stuck in old ranks: only ${fourSecondRetreat.toFixed(1)} units of retreat in four seconds`);
  assert(retreat > 300 && retreat - fourSecondRetreat > 125,
    `Retreat stopped after its initial breakoff: ${fourSecondRetreat.toFixed(1)} units in four seconds, ${retreat.toFixed(1)} in ten seconds`);
  assert(troops.fallingBack && troops.order === 'retreat', 'The withdrawal was countermanded before reaching friendly support');
  assert(minimumRatio >= .90, 'Withdrawing soldiers passed through occupied bodies'); checkWorld(e);
  return { engagedBeforeOrder: engaged.length, respondingInOneSecond: responding, meanRetreatInFourSeconds: Math.round(fourSecondRetreat), meanRetreatInTenSeconds: Math.round(retreat),
    respondingOnFirstStep: firstStepMotion, meanMovementInFirstTenthSecond: Math.round(earlyMovement * 100) / 100,
    minimumBodyRatio: Math.round(minimumRatio * 1000) / 1000 };
});

test('Unsupported Retreat turns away from the actual enemy even after charging toward the original rear', () => {
  const e = new BattleEngine(config({ cavalry: 16 }, { guardian: 24 }));
  const troops = regiment(e, 0), enemy = regiment(e, 1), ids = new Set([troops.id, enemy.id]);
  place(e, troops, 2800, 2200); place(e, enemy, 2550, 2200); e.command([enemy.id], 'hold');
  e.start(); e.command([troops.id], 'charge'); step(e, 8);
  const before = commandPositions(e, troops), engaged = before.filter(p => p.lastAttack >= e.time - 1.5);
  assert(engaged.length >= 4, 'The reverse-facing retreat fixture did not engage its western enemy');
  assert.equal(e.command([troops.id], 'retreat'), 1); let minimumRatio = Infinity;
  for (let tick = 0; tick < 120; tick++) {
    const previous = bodySnapshot(e); e.step(1 / 30); assertBoundedMovement(e, previous, ids); assertPassable(e, ids);
    minimumRatio = Math.min(minimumRatio, minimumBodyRatio(e, previous, ids));
  }
  const motion = commandMotion(e, engaged), away = motion.reduce((sum, p) => sum + p.dx, 0) / motion.length;
  assert(away > 80 && motion.filter(p => p.dx > 40).length >= Math.ceil(engaged.length * .75),
    `Retreat followed the original team rear into the nearby enemy: mean eastward separation ${away.toFixed(1)}`);
  assert(minimumRatio >= .90, 'Unsupported withdrawal crossed occupied enemy bodies'); checkWorld(e);
  return { recentFighters: engaged.length, meanAwayProgressInFourSeconds: Math.round(away), minimumBodyRatio: Math.round(minimumRatio * 1000) / 1000 };
});

test('A new Retreat overrides commander regrouping and prevents premature passive recovery', () => {
  const e = new BattleEngine(config({ swordsman: 32, commander: 1 }, { militia: 1 }));
  const troops = regiment(e, 0, 'swordsman'), commander = regiment(e, 0, 'commander'), enemy = regiment(e, 1);
  place(e, troops, 2800, 2200); place(e, commander, 2400, 2200); place(e, enemy, 5900, 4000);
  e.command([commander.id, enemy.id], 'hold'); e.start(); e.command([troops.id], 'retreat'); step(e, 3.5);
  assert(troops.fallingBack); assert.equal(e.command([commander.id], 'rally'), 1);
  assert.equal(troops.status, 'rallying', 'The commander did not establish the public regrouping state');
  const before = commandPositions(e, troops), ids = new Set([troops.id, commander.id]);
  assert.equal(e.command([troops.id], 'retreat'), 1); checkedCommandSteps(e, 60, ids);
  const motion = commandMotion(e, before), retreat = -motion.reduce((sum, p) => sum + p.dx, 0) / motion.length;
  assert(retreat > 35 && motion.filter(p => p.dx < -20).length >= Math.ceil(before.length * .75),
    `Regrouping prevented the new withdrawal: mean retreat ${retreat.toFixed(1)} units in two seconds`);
  assert(troops.fallingBack && troops.order === 'retreat' && troops.status !== 'rallying'); checkWorld(e);

  // Create an old automatic fallback with actual combat, then issue a fresh
  // manual Retreat just before passive support recovery becomes eligible.
  const natural = new BattleEngine(config({ militia: 32, commander: 1 }, { berserker: 64 }));
  const falling = regiment(natural, 0, 'militia'), general = regiment(natural, 0, 'commander'), attackers = regiment(natural, 1);
  place(natural, falling, 2900, 2200); place(natural, general, 2400, 2200); place(natural, attackers, 3060, 2200);
  natural.command(natural.regiments.map(r => r.id), 'hold'); natural.start(); natural.command([attackers.id], 'charge');
  let fallbackStarted = null;
  for (let tick = 0; tick < 70 * 30 && natural.phase !== 'ended'; tick++) {
    natural.step(1 / 30);
    if (falling.fallingBack && fallbackStarted === null) {
      fallbackStarted = natural.time;
      const survivors = living(natural, new Set([falling.id]));
      const x = averageX(survivors), y = survivors.reduce((sum, u) => sum + u.y, 0) / survivors.length;
      natural.command([attackers.id], 'move', 4000, 2200);
      natural.command([general.id], 'move', x - 140, y + 120);
    }
    if (fallbackStarted !== null && natural.time - fallbackStarted >= 11.8) break;
  }
  assert(fallbackStarted !== null && natural.getStats().teams[0].dead > 0, 'The support fixture did not enter automatic fallback through actual combat');
  assert(falling.fallingBack && falling.morale >= 45 && falling.cohesion >= 45, 'The older fallback was not eligible for passive support recovery');
  const withdrawn = commandPositions(natural, falling), naturalIds = new Set(natural.regiments.map(r => r.id));
  assert(withdrawn.length >= 12, 'Too few survivors remained for the renewed withdrawal');
  assert.equal(natural.command([falling.id], 'retreat'), 1);
  let naturalMinimumRatio = Infinity, twoSecondRetreat = 0;
  for (let tick = 1; tick <= 120; tick++) {
    const previous = bodySnapshot(natural); natural.step(1 / 30); assertBoundedMovement(natural, previous, naturalIds); assertPassable(natural, naturalIds);
    naturalMinimumRatio = Math.min(naturalMinimumRatio, minimumBodyRatio(natural, previous, naturalIds));
    if (tick === 15) assert(falling.fallingBack && falling.order === 'retreat', 'Passive support recovery overrode a fresh manual Retreat during its initial turn');
    if (tick === 60) twoSecondRetreat = -commandMotion(natural, withdrawn).reduce((sum, p) => sum + p.dx, 0) / withdrawn.length;
  }
  const renewedRetreat = -commandMotion(natural, withdrawn).reduce((sum, p) => sum + p.dx, 0) / withdrawn.length;
  assert(twoSecondRetreat > 60 && renewedRetreat > 150, 'The renewed Retreat stopped at friendly support before completing its breakoff');
  assert(naturalMinimumRatio >= .90, 'The renewed withdrawal crossed occupied bodies'); checkWorld(natural);
  return { soldiers: before.length, meanRetreatInTwoSeconds: Math.round(retreat), recoveringOrderOverridden: true,
    naturalFallback: { combatStartedFallbackAt: Math.round(fallbackStarted * 100) / 100, survivors: withdrawn.length,
      renewedRetreatInTwoSeconds: Math.round(twoSecondRetreat), renewedRetreatInFourSeconds: Math.round(renewedRetreat), minimumBodyRatio: Math.round(naturalMinimumRatio * 1000) / 1000 } };
});

test('A flexible formation bends around an offset rock corner and reforms beyond the passage', () => {
  const e = new BattleEngine(config({ swordsman: 160 }, { militia: 1 }, 'desert'));
  const troops = regiment(e, 0), enemy = regiment(e, 1), ids = new Set([troops.id]);
  place(e, troops, 2700, 1200); place(e, enemy, 5900, 4000); e.command([enemy.id], 'hold');
  const initial = commandPositions(e, troops), originalSpan = Math.max(...initial.map(p => p.y)) - Math.min(...initial.map(p => p.y));
  e.start(); e.command([troops.id], 'move', 3800, 1375);
  const crossings = new Map(); let minimumRatio = Infinity, arrivalTime = null, minimumCohesion = 100, maximumSideDeflection = 0;
  for (let tick = 0; tick < 80 * 30; tick++) {
    const previous = bodySnapshot(e); e.step(1 / 30); assertBoundedMovement(e, previous, ids); assertPassable(e, ids);
    minimumRatio = Math.min(minimumRatio, minimumBodyRatio(e, previous, ids)); minimumCohesion = Math.min(minimumCohesion, troops.cohesion);
    for (const p of initial) {
      const u = e.units[p.id]; maximumSideDeflection = Math.max(maximumSideDeflection, Math.abs(u.y - p.y));
      if (previous[p.id].x < 3200 && u.x >= 3200 && u.y > 1260 && u.y < 1490) crossings.set(p.id, u.y);
    }
    const arrived = living(e, ids).filter(u => u.x > 3550).length;
    if (arrivalTime === null && arrived >= 144) arrivalTime = e.time;
    if (e.time >= 60 && arrivalTime === null) break;
    if (arrivalTime !== null && e.time >= arrivalTime + 20) break;
  }
  assert(arrivalTime !== null && arrivalTime <= 60, 'The formation remained pinned against the corner instead of bending into the adjacent gap');
  assert(crossings.size >= 144, 'The troops bypassed the intended rock passage');
  const crossY = [...crossings.values()].sort((a, b) => a - b), crossingSpan = crossY[Math.floor(crossY.length * .9)] - crossY[Math.floor(crossY.length * .1)];
  const final = living(e, ids), finalSpan = Math.max(...final.map(u => u.y)) - Math.min(...final.map(u => u.y));
  assert(crossingSpan > 45 && crossingSpan < originalSpan * .9, 'Passage did not show a compressed, multi-file physical approach');
  assert(maximumSideDeflection > 100 && finalSpan > originalSpan * .8, 'Ranks did not visibly bend and recover their frontage');
  assert(troops.cohesion > minimumCohesion + 5 || troops.cohesion >= 90, 'The bent formation did not recover cohesion on clear ground');
  assert(minimumRatio >= .90, 'Malleable movement caused body penetration'); assert.equal(e.getStats().teams[0].dead, 0); checkWorld(e);
  return { troopsThroughGap: crossings.size, arrivalSeconds: Math.round(arrivalTime * 10) / 10, originalPhysicalSpan: Math.round(originalSpan),
    crossingPhysicalSpan: Math.round(crossingSpan), restoredPhysicalSpan: Math.round(finalSpan), maximumSideDeflection: Math.round(maximumSideDeflection),
    minimumBodyRatio: Math.round(minimumRatio * 1000) / 1000, recoveredCohesion: Math.round(troops.cohesion) };
});

// Ranged participation is distinct from melee. A front file can keep throwing
// while hundreds of motionless support troops never reach a useful position.
// This fixture begins only through normal deployment and public Hold orders.
function measureMissileFronts(map) {
  const e = new BattleEngine(config({ javelin: 640 }, { guardian: 640 }, map));
  const attackers = e.regiments.filter(r => r.team === 0), defenders = e.regiments.filter(r => r.team === 1);
  if (map === 'desert') {
    attackers.forEach((r, i) => place(e, r, 2100 - Math.floor(i / 2) * 360, 1100 + (i % 2) * 500));
    defenders.forEach((r, i) => place(e, r, 3800 + Math.floor(i / 2) * 320, 1100 + (i % 2) * 500));
  } else {
    attackers.forEach((r, i) => place(e, r, 2600 - i * 280, 2200));
    defenders.forEach((r, i) => place(e, r, 3300, 1600 + i * 400));
  }
  e.command(defenders.map(r => r.id), 'hold');
  const rearIds = new Set(attackers.slice(2).flatMap(r => r.unitIds));
  const allShooters = new Set(), rearShooters = new Set(), regShooters = new Map(attackers.map(r => [r.id, new Set()]));
  const percentile = (a, fraction) => a.length ? a.slice().sort((x, y) => x - y)[Math.floor((a.length - 1) * fraction)] : 0;
  const previous = e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp }));
  let windowStart = e.units.map(u => ({ x: u.x, y: u.y })), windowShots = new Map(), windows = [];
  let damage = 0, maximumStep = 0, minimumRatio = Infinity;
  // A swept spatial hash gives full-radius checks without an all-pair scan.
  const checkBodies = () => {
    const buckets = new Map(), size = 48;
    for (const u of e.units) if (u.alive && !u.routed) {
      const p = previous[u.id], gx = Math.floor(p.x / size), gy = Math.floor(p.y / size);
      for (let y = gy - 1; y <= gy + 1; y++) for (let x = gx - 1; x <= gx + 1; x++) {
        const bucket = buckets.get(x + ':' + y); if (!bucket) continue;
        for (const v of bucket) {
          const q = previous[v.id], radius = GameData.TYPES[u.type].radius + GameData.TYPES[v.type].radius;
          if (Math.min(p.x, u.x) - Math.max(q.x, v.x) > radius || Math.min(q.x, v.x) - Math.max(p.x, u.x) > radius ||
              Math.min(p.y, u.y) - Math.max(q.y, v.y) > radius || Math.min(q.y, v.y) - Math.max(p.y, u.y) > radius) continue;
          minimumRatio = Math.min(minimumRatio, sweptSeparation(p, q, u, v) / radius);
        }
      }
      const key = gx + ':' + gy; if (!buckets.has(key)) buckets.set(key, []); buckets.get(key).push(u);
    }
  };
  e.start();
  for (let tick = 0; tick < 120 * 30 && e.phase === 'running'; tick++) {
    e.step(1 / 30);
    for (const p of e.projectiles) if (p.team === 0 && p.type === 'javelin' && p.age <= 1 / 30 + 1e-6) {
      const u = e.units[p.attacker];
      allShooters.add(u.id); regShooters.get(u.regiment).add(u.id);
      if (rearIds.has(u.id)) rearShooters.add(u.id);
      windowShots.set(u.id, p.sy);
    }
    for (const u of e.units) {
      const prior = previous[u.id];
      if (u.team === 1 && prior.hp > u.hp) damage += prior.hp - u.hp;
      if (u.alive && !u.routed) {
        const distance = Math.hypot(u.x - prior.x, u.y - prior.y);
        maximumStep = Math.max(maximumStep, distance);
        assert(distance <= GameData.TYPES[u.type].speed / 30 * 1.8 + .25, 'Missile frontage teleported a soldier');
        assert.equal(e.terrainAt(u.x, u.y).blocked, false, 'Missile frontage entered blocked terrain');
      }
    }
    checkBodies();
    for (const u of e.units) Object.assign(previous[u.id], { x: u.x, y: u.y, hp: u.hp });
    if ((tick + 1) % 300 === 0) {
      const eligible = e.units.filter(u => u.team === 0 && u.alive && !u.routed && !e.regiments[u.regiment].fallingBack);
      const idle = eligible.filter(u => !windowShots.has(u.id) && Math.hypot(u.x - windowStart[u.id].x, u.y - windowStart[u.id].y) < 12);
      const fronts = attackers.map(r => {
        const ys = r.unitIds.filter(id => windowShots.has(id)).map(id => windowShots.get(id));
        return { id: r.id, shooters: ys.length, firingSpan: percentile(ys, .9) - percentile(ys, .1) };
      });
      windows.push({ time: Math.round(e.time), shooters: windowShots.size, eligible: eligible.length,
        idleFraction: idle.length / Math.max(1, eligible.length), fronts, remaining: e.getStats().teams.map(s => s.alive) });
      windowShots = new Map(); windowStart = e.units.map(u => ({ x: u.x, y: u.y }));
    }
  }
  checkWorld(e);
  return { engine: e, windows, shooters: allShooters.size, rearShooters: rearShooters.size,
    regimentShooters: [...regShooters].map(([id, us]) => ({ id, shooters: us.size })), damage, maximumStep, minimumRatio };
}

test('Javelin support fans into active firing fronts instead of leaving idle rear files', () => {
  const outcomes = {};
  for (const map of ['meadow', 'desert']) {
    const m = measureMissileFronts(map);
    const late = m.windows.filter(w => w.time >= 60 && w.eligible >= 320 && w.remaining[1] >= 160);
    assert(late.length >= 3, `${map}: not enough surviving troops to test sustained missile support`);
    assert(m.shooters >= 256, `${map}: only ${m.shooters}/640 javelin soldiers ever launched a projectile`);
    assert(m.rearShooters >= 96, `${map}: only ${m.rearShooters}/320 initially rear soldiers ever fired`);
    assert(m.regimentShooters.every(r => r.shooters >= 24), `${map}: an entire javelin formation failed to contribute`);
    const active = late.filter(w => w.shooters / w.eligible >= .30);
    assert(active.length >= 3, `${map}: fewer than three late windows have30% actual shooting participation`);
    const broad = late.filter(w => w.fronts.filter(r => r.shooters >= 24 && r.firingSpan >= 70).length >= 3);
    assert(broad.length >= 3, `${map}: actual firing positions remain too narrow or concentrated in a few regiments`);
    const medianIdle = late.map(w => w.idleFraction).sort((a, b) => a - b)[Math.floor(late.length / 2)];
    assert(medianIdle <= .65, `${map}: ${(medianIdle * 100).toFixed(1)}% of available missile troops remain stationary without firing`);
    assert(m.damage > 1500, `${map}: launch animations produced too little actual target damage`);
    assert(m.minimumRatio >= .90, `${map}: firing-front movement crossed occupied bodies`);
    outcomes[map] = { distinctShooters: m.shooters, initiallyRearShooters: m.rearShooters, regimentShooters: m.regimentShooters,
      activeLateWindows: active.length, broadFiringWindows: broad.length, medianIdleFraction: Math.round(medianIdle * 1000) / 1000,
      projectileDamage: Math.round(m.damage), minimumBodyRatio: Math.round(m.minimumRatio * 1000) / 1000,
      maximumStep: m.maximumStep, finalWindow: m.windows.at(-1) };
  }
  return outcomes;
});

function measureMissileRole(type) {
  const e = new BattleEngine(config({ [type]: 48 }, { guardian: 160 }));
  const shooters = regiment(e, 0), enemy = regiment(e, 1);
  place(e, shooters, 2600, 2400); place(e, enemy, 3400, 2400);
  e.command([enemy.id], 'hold'); e.start();
  const activeCycles = new Map(), excursions = [], intervals = [], distances = [], seen = new Set();
  let previous = e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp })), damage = 0;
  for (let tick = 0; tick < 50 * 30 && e.phase === 'running'; tick++) {
    e.step(1 / 30);
    for (const u of e.units) {
      const p = previous[u.id];
      if (u.team === 1 && p.hp > u.hp) damage += p.hp - u.hp;
      if (u.alive && !u.routed) {
        assert(Math.hypot(u.x - p.x, u.y - p.y) <= GameData.TYPES[u.type].speed / 30 * 1.8 + .25, `${type}: attack cycle teleported a soldier`);
        assert.equal(e.terrainAt(u.x, u.y).blocked, false, `${type}: attack cycle entered blocked ground`);
        const cycle = activeCycles.get(u.id);
        if (cycle) cycle.excursion = Math.max(cycle.excursion, Math.hypot(u.x - cycle.x, u.y - cycle.y));
      }
    }
    for (const p of e.projectiles) if (p.team === 0 && p.age <= 1 / 30 + 1e-6 && e.time >= 25) {
      const old = activeCycles.get(p.attacker);
      if (old && e.time - old.time > .5) { excursions.push(old.excursion); intervals.push(e.time - old.time); }
      activeCycles.set(p.attacker, { x: p.sx, y: p.sy, time: e.time, excursion: 0 });
      distances.push(Math.hypot(p.tx - p.sx, p.ty - p.sy)); seen.add(p.attacker);
    }
    previous = e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp }));
  }
  const median = a => a.slice().sort((a, b) => a - b)[Math.floor(a.length / 2)] || 0;
  assert(seen.size >= 24 && excursions.length >= 32, `${type}: insufficient real firing cycles to establish its role`);
  assert(damage > 100, `${type}: firing cycles caused no meaningful target health loss`);
  checkWorld(e);
  return { shootingSoldiers: seen.size, completedCycles: excursions.length, medianExcursion: median(excursions),
    medianShotInterval: median(intervals), medianShotDistance: median(distances), damage };
}

test('Missile classes show distinct physical firing distances, reload cadence and repositioning', () => {
  const roles = Object.fromEntries(['javelin', 'archer', 'crossbow', 'slinger', 'horseArcher'].map(type => [type, measureMissileRole(type)]));
  assert(roles.javelin.medianExcursion > 6 && roles.javelin.medianExcursion <= 45,
    'Javelins did not make a bounded physical reposition between real throws');
  assert(roles.slinger.medianExcursion > 2 && roles.slinger.medianExcursion <= 30,
    'Slingers did not make their smaller physical reposition between shots');
  assert(roles.horseArcher.medianExcursion > 12 && roles.horseArcher.medianExcursion <= 85,
    'Mounted archers did not make a distinct bounded mobile firing cycle');
  assert(roles.archer.medianExcursion < 4 && roles.crossbow.medianExcursion < 4,
    'Longbows or crossbows kept wandering instead of firing and reloading from their established line');
  assert(roles.archer.medianShotDistance > roles.crossbow.medianShotDistance + 50 &&
    roles.crossbow.medianShotDistance > roles.javelin.medianShotDistance + 30,
    'The actual projectile launch distances did not distinguish bows, crossbows and close skirmishers');
  assert(roles.archer.medianShotInterval < roles.javelin.medianShotInterval &&
    roles.javelin.medianShotInterval < roles.crossbow.medianShotInterval &&
    roles.crossbow.medianShotInterval - roles.archer.medianShotInterval > .7,
    'Actual firing cadence did not preserve the slower crossbow reload and faster bow cycle');
  return Object.fromEntries(Object.entries(roles).map(([type, result]) => [type,
    Object.fromEntries(Object.entries(result).map(([key, value]) => [key, Math.round(value * 100) / 100]))]));
});

test('Infantry reinforcements pass through an active friendly firing band without crossing occupied bodies', () => {
  const e = new BattleEngine(config({ archer: 80, swordsman: 32 }, { guardian: 160 }));
  const bows = regiment(e, 0, 'archer'), reserve = regiment(e, 0, 'swordsman'), enemy = regiment(e, 1);
  place(e, bows, 2850, 2200); place(e, reserve, 2400, 2200); place(e, enemy, 3400, 2200);
  e.command([reserve.id, enemy.id], 'hold'); e.start();
  const initialShooters = new Set(); let damage = 0;
  for (let tick = 0; tick < 25 * 30; tick++) {
    const hp = enemy.unitIds.reduce((sum, id) => sum + e.units[id].hp, 0); e.step(1 / 30);
    damage += hp - enemy.unitIds.reduce((sum, id) => sum + e.units[id].hp, 0);
    for (const p of e.projectiles) if (p.team === 0 && p.age <= 1 / 30 + 1e-6) initialShooters.add(p.attacker);
  }
  assert(initialShooters.size >= 8 && damage > 100, 'The bows did not establish a real active firing band');
  const band = averageX(bows.unitIds.map(id => e.units[id]));
  const before = reserve.unitIds.map(id => ({ id, x: e.units[id].x, y: e.units[id].y }));
  assert.equal(e.command([reserve.id], 'move', 3220, 2200), 1);
  const continuedShooters = new Set(), observed = new Set([bows.id, reserve.id, enemy.id]);
  let minimumRatio = Infinity, maximumStep = 0;
  for (let tick = 0; tick < 35 * 30; tick++) {
    const previous = bodySnapshot(e); e.step(1 / 30);
    maximumStep = Math.max(maximumStep, assertBoundedMovement(e, previous, observed));
    assertPassable(e, observed);
    for (const p of e.projectiles) if (p.team === 0 && p.age <= 1 / 30 + 1e-6) continuedShooters.add(p.attacker);
    for (const id of reserve.unitIds) {
      const u = e.units[id];
      for (const other of bows.unitIds) {
        const v = e.units[other];
        if (u.alive && !u.routed && v.alive && !v.routed) minimumRatio = Math.min(minimumRatio,
          sweptSeparation(previous[id], previous[other], u, v) / (GameData.TYPES[u.type].radius + GameData.TYPES[v.type].radius));
      }
    }
  }
  const passed = before.filter(p => e.units[p.id].x > band + 30).length;
  const advance = before.reduce((sum, p) => sum + e.units[p.id].x - p.x, 0) / before.length;
  assert(passed >= 24, `Only ${passed}/32 reinforcements passed the active friendly firing band`);
  assert(advance > 650, `Reinforcements remained pinned behind friendly archers: ${advance.toFixed(1)} units of progress`);
  assert.equal(reserve.alive, 32, 'Reinforcement passage created casualties before enemy contact');
  assert(continuedShooters.size >= 8, 'Archery stopped instead of leaving usable reinforcement passages');
  assert(minimumRatio >= .90, 'Reinforcements passed through occupied archer bodies');
  checkWorld(e);
  return { activeBowsBeforeMove: initialShooters.size, activeBowsDuringPassage: continuedShooters.size,
    reinforcingSoldiersThrough: passed, meanAdvance: Math.round(advance),
    minimumBodyRatio: Math.round(minimumRatio * 1000) / 1000, maximumStep };
});

test('Custom encounters begin empty and clone bounded terrain without altering stock maps', () => {
  const stock = JSON.stringify(GameData.MAPS), empty = new BattleEngine(config({}, {}, 'custom'));
  assert.equal(empty.map.id, 'custom'); assert.equal(empty.units.length, 0); assert.equal(empty.regiments.length, 0);
  assert.equal(empty.map.regions.length, 0, 'Custom map inherited obstacles from a stock battlefield');
  assert(empty.getStats().teams.every(s => s.initial === 0 && s.alive === 0));
  const customMap = { name: 'A wooded crossing', biome: 'woodland', regions: [
    { type: 'forest', x: 1000, y: 1000, w: 500, h: 450 },
    { type: 'rock', x: 3000, y: 1800, w: 160, h: 220 },
    { type: 'hill', x: 4000, y: 1100, w: 500, h: 400 }
  ] };
  const custom = new BattleEngine({ ...config({ swordsman: 16 }, { guardian: 16 }, 'custom'), customMap });
  assert.equal(custom.map.name, 'A wooded crossing'); assert.equal(custom.map.biome, 'woodland');
  assert.equal(custom.terrainAt(1100, 1100).type, 'forest');
  assert.equal(custom.terrainAt(3050, 1900).blocked, true);
  customMap.regions[0].x = 6000;
  assert.equal(custom.terrainAt(1100, 1100).type, 'forest', 'External input mutation changed active terrain');
  assert.equal(JSON.stringify(GameData.MAPS), stock, 'Custom editing mutated the stock map catalog');
  const invalid = [{ type: 'unknown', x: 0, y: 0, w: 400, h: 400 }, { type: 'rock', x: NaN, y: 0, w: 200, h: 100 },
    ...Array.from({ length: 130 }, (_, i) => ({ type: i % 2 ? 'forest' : 'hill', x: 100 + i * 30, y: 500, w: 70, h: 70 })),
    { type: 'water', x: -1000, y: -1000, w: 1000000, h: 1000000 }];
  const bounded = new BattleEngine({ ...config({}, {}, 'custom'), customMap: { regions: invalid } });
  assert(bounded.map.regions.length <= 96, 'Custom terrain exceeded the region cap');
  for (const r of bounded.map.regions) {
    assert(['forest', 'hill', 'water', 'rock', 'bridge'].includes(r.type));
    assert([r.x, r.y, r.w, r.h].every(Number.isFinite) && r.x >= 0 && r.y >= 0 && r.w > 0 && r.h > 0);
    assert(r.x + r.w <= bounded.width && r.y + r.h <= bounded.height, 'Custom terrain escaped map bounds');
  }
  checkWorld(custom); checkWorld(bounded);
  return { blankTroops: empty.units.length, blankRegions: empty.map.regions.length, customRegions: custom.map.regions.length,
    boundedRegions: bounded.map.regions.length, stockMapsUnchanged: true };
});

test('Deployment splits preserve individual soldiers and round-trip explicit encounter formations', () => {
  const cfg = { ...config({ swordsman: 96, archer: 48, commander: 1 }, { guardian: 96 }, 'custom'),
    customMap: { name: 'Split-company encounter', biome: 'grassland', regions: [{ type: 'forest', x: 3900, y: 3000, w: 400, h: 300 }] },
    formations: [
      { team: 0, type: 'swordsman', count: 48, x: 2300, y: 2000, formation: 'loose' },
      { team: 0, type: 'swordsman', count: 48, x: 2400, y: 2400, formation: 'shieldwall' },
      { team: 0, type: 'archer', count: 48, x: 2000, y: 2200, formation: 'line' },
      { team: 0, type: 'commander', count: 1, x: 1900, y: 2600, formation: 'line' },
      { team: 1, type: 'guardian', count: 96, x: 3500, y: 2200, formation: 'line' }
    ] };
  const e = new BattleEngine(cfg), r = e.regiments.find(r => r.team === 0 && r.type === 'swordsman');
  assert.equal(e.regiments.length, 5, 'Explicit saved groups were collapsed into default army blocks');
  assert.equal(r.formation, 'loose');
  const before = e.units.map(u => ({ ref: u, id: u.id, x: u.x, y: u.y, hp: u.hp, alive: u.alive, routed: u.routed }));
  const stats = JSON.stringify(e.getStats()), originalIds = new Set(r.unitIds), oldCount = e.regiments.length;
  const added = e.splitRegiments([r.id, r.id]);
  assert.equal(added.length, 1, 'Duplicate selected IDs split the same company twice');
  assert.equal(e.regiments.length, oldCount + 1); assert.equal(e.units.length, before.length);
  const child = e.getRegiment(added[0]);
  assert.equal(child.type, r.type); assert.equal(child.team, r.team); assert.equal(child.formation, r.formation);
  assert(Math.abs(child.alive - r.alive) <= 1 && child.alive >= 2);
  const combined = r.unitIds.concat(child.unitIds);
  assert.equal(new Set(combined).size, originalIds.size); assert(combined.every(id => originalIds.has(id)));
  assert.equal(r.initial + child.initial, 48);
  for (const p of before) {
    const u = e.units[p.id]; assert.equal(u, p.ref, 'Splitting replaced an individual soldier');
    assert.deepEqual({ id: u.id, x: u.x, y: u.y, hp: u.hp, alive: u.alive, routed: u.routed },
      { id: p.id, x: p.x, y: p.y, hp: p.hp, alive: p.alive, routed: p.routed }, 'Splitting moved, healed or duplicated a soldier');
    assert(e.getRegiment(u.regiment).unitIds.includes(u.id));
  }
  assert.equal(JSON.stringify(e.getStats()), stats, 'A deployment split changed army statistics');
  assert.deepEqual(e.splitRegiments([regiment(e, 0, 'commander').id]), [], 'A one-soldier commander was split');
  const geometry = deploymentGeometry(e), layout = e.captureFormations(), reloaded = new BattleEngine({ ...cfg, formations: layout });
  assert.equal(reloaded.regiments.length, e.regiments.length); assert.equal(reloaded.units.length, e.units.length);
  assert.equal(reloaded.map.regions.length, 1);
  const summarize = formations => formations.map(p => ({ team: p.team, type: p.type, count: p.count, x: p.x, y: p.y, formation: p.formation }));
  assert.deepEqual(summarize(reloaded.captureFormations()), summarize(layout), 'Split group layout did not survive encounter reload');
  assertDeploymentGeometry(reloaded, geometry, 'Reload changed the split formation footprint');
  assert(minimumStaticBodyRatio(reloaded) >= .90, 'Reload reconstructed split groups on top of other soldiers');
  layout[0].x = 0; assert.notEqual(e.captureFormations()[0].x, 0, 'Captured layout retained mutable engine objects');
  layout[0].offsets[0].x = 9999; assert.notEqual(e.captureFormations()[0].offsets[0].x, 9999, 'Captured offsets retained mutable engine objects');
  const lineConfig = { ...config({ swordsman: 160 }, {}, 'custom'), customMap: { biome: 'grassland', regions: [] } };
  const line = new BattleEngine(lineConfig); place(line, regiment(line, 0), 2800, 2200);
  assert.equal(line.splitRegiments([0]).length, 1);
  const splitGeometry = deploymentGeometry(line), lineReload = new BattleEngine({ ...lineConfig, formations: line.captureFormations() });
  assertDeploymentGeometry(lineReload, splitGeometry, 'Reload re-expanded adjacent eighty-soldier halves');
  const splitRatio = minimumStaticBodyRatio(lineReload);
  assert(splitRatio >= .90, 'Adjacent split halves acquired overlapping bodies after reload');
  checkWorld(e); checkWorld(reloaded);
  return { initialGroups: oldCount, splitGroups: e.regiments.length, soldiersPreserved: e.units.length, restoredGroups: reloaded.regiments.length,
    exactReloadedPositions: true, adjacentHalfSoldiers: lineReload.units.length, minimumReloadBodyRatio: Math.round(splitRatio * 1000) / 1000 };
});

test('Battle splits preserve casualties, current orders and physical command response', () => {
  const e = new BattleEngine(config({ swordsman: 64, commander: 1 }, { guardian: 64 }));
  const r = regiment(e, 0, 'swordsman'), enemy = regiment(e, 1), commander = regiment(e, 0, 'commander');
  place(e, r, 2900, 2400); place(e, enemy, 3060, 2400); place(e, commander, 2400, 2800);
  e.command([enemy.id, commander.id], 'hold'); e.start();
  for (let tick = 0; tick < 60 * 30 && e.phase === 'running' && e.getStats().teams[0].dead < 2; tick++) e.step(1 / 30);
  assert(e.getStats().teams[0].dead >= 2 && r.alive >= 12, 'The split fixture did not establish real casualties and surviving troops');
  assert.equal(e.command([r.id], 'move', 2400, 2400), 1);
  e.step(1 / 30); e.pause();
  const before = e.units.map(u => ({ ref: u, id: u.id, x: u.x, y: u.y, hp: u.hp, alive: u.alive, routed: u.routed }));
  const beforeStats = JSON.stringify(e.getStats()), count = e.units.length, original = new Set(r.unitIds);
  const goal = { x: r.targetX, y: r.targetY }, added = e.splitRegiments([r.id]);
  assert.equal(added.length, 1); const child = e.getRegiment(added[0]);
  assert.equal(e.units.length, count); assert.equal(JSON.stringify(e.getStats()), beforeStats, 'Splitting reset casualties, kills, morale or army totals');
  assert.equal(r.order, 'move'); assert.equal(child.order, 'move');
  const combinedAlive = r.alive + child.alive;
  assert(Math.abs((r.targetX * r.alive + child.targetX * child.alive) / combinedAlive - goal.x) < 1e-6 &&
    Math.abs((r.targetY * r.alive + child.targetY * child.alive) / combinedAlive - goal.y) < 1e-6,
    'Splitting changed the accepted destination area instead of preserving the group layout');
  assert(Math.hypot(r.targetX - child.targetX, r.targetY - child.targetY) > 10,
    'Both new halves received one overlapping destination instead of retaining their physical spacing');
  assert.equal(r.initial + child.initial, original.size);
  const combined = r.unitIds.concat(child.unitIds); assert.equal(new Set(combined).size, original.size);
  assert(combined.every(id => original.has(id)));
  for (const p of before) {
    const u = e.units[p.id]; assert.equal(u, p.ref);
    assert.deepEqual([u.x, u.y, u.hp, u.alive, u.routed], [p.x, p.y, p.hp, p.alive, p.routed]);
  }
  const ids = new Set([r.id, child.id, enemy.id, commander.id]);
  e.resume(); let minimumRatio = Infinity;
  for (let tick = 0; tick < 60; tick++) {
    const previous = bodySnapshot(e); e.step(1 / 30); assertBoundedMovement(e, previous, ids); assertPassable(e, ids);
    minimumRatio = Math.min(minimumRatio, minimumBodyRatio(e, previous, ids));
  }
  const movement = [r, child].map(group => {
    const us = group.unitIds.map(id => e.units[id]).filter(u => u.alive && !u.routed);
    return us.reduce((sum, u) => sum + before[u.id].x - u.x, 0) / us.length;
  });
  assert(movement.every(n => n > 25), 'One split half lost the inherited command or remained stuck');
  assert(minimumRatio >= .90); checkWorld(e);
  return { casualtiesBeforeSplit: JSON.parse(beforeStats).teams[0].dead, survivingHalfSizes: [r.alive, child.alive],
    meanWithdrawalByHalf: movement.map(n => Math.round(n)), minimumBodyRatio: Math.round(minimumRatio * 1000) / 1000 };
});

require('./formation-continuity-test.cjs')(test);
require('./counter-formation-test.cjs')(test);
require('./blockage-test.cjs')(test);
require('./mobility-tactics-test.cjs')(test);
require('./impact-siege-test.cjs')(test);
require('./lancer-breakthrough-test.cjs')(test);
require('./mounted-push-test.cjs')(test);
require('./mounted-speed-test.cjs')(test);
require('./friendly-mounted-test.cjs')(test);
require('./mounted-traffic-test.cjs')(test);
require('./formation-pressure-test.cjs')(test);
require('./battle-momentum-test.cjs')(test);
require('./march-response-test.cjs')(test);
require('./castle-navigation-test.cjs')(test);
require('./club-sweep-test.cjs')(test);
require('./ordnance-test.cjs')(test);
require('./rank-volley-test.cjs')(test);
require('./historical-battles-test.cjs')(test);
require('./gunline-charge-test.cjs')(test);
require('./expanded-battlefields-test.cjs')(test);
require('./army-plans-test.cjs')(test);
require('./strategy-edge-test.cjs')(test);

test('12,000 soldiers stay stable through 10 simulated seconds', () => {
  const cfg = clone(GameData.PRESETS.find(p => p.id === 'epic'));
  const e = new BattleEngine(cfg);
  assert.equal(e.units.length, 12000);
  e.start();
  const start = performance.now();
  step(e, 10);
  const milliseconds = performance.now() - start;
  checkWorld(e);
  return { units: e.units.length, millisecondsPerStep: Math.round(milliseconds / 300 * 100) / 100 };
});

test('20,000-soldier cap remains stable while moving', () => {
  const e = new BattleEngine(config({ swordsman: 7000, archer: 2000, cavalry: 1000 }, { pikeman: 7000, archer: 2000, lancer: 1000 }));
  assert.equal(e.units.length, 20000);
  e.start();
  const start = performance.now();
  step(e, 6);
  const milliseconds = performance.now() - start;
  checkWorld(e);
  return { units: e.units.length, millisecondsPerStep: Math.round(milliseconds / 180 * 100) / 100 };
});

console.log(JSON.stringify({ passed: results.filter(r => r.passed).length, failed: results.filter(r => !r.passed).length, results }, null, 2));
