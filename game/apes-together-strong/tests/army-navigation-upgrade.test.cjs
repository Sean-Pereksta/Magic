'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { loadEngine } = require('./performance-harness.cjs');

function arena() {
  const c = loadEngine(), world = new c.ATSWorld('ARMY-ROUTES');
  world.objects.clear(); world._spatial.clear(); world._streaming = false;
  world.ensure = () => {}; world.terrain = () => ({ biome: 'forest', water: false });
  world.waterBlocked = () => false; world.waterFreeBounds = () => true;
  return { c, world, nav: new c.ATSNavigation(world) };
}
function obstacle(world, data) {
  const o = { solid: true, hp: 1000, r: 12, ...data };
  world.objects.set(o.id, o); world._indexObject(o); return o;
}

for (const count of [25, 100, 250, 500]) test(`${count} apes share long forest, boulder, settlement and prison-opening routes without teleporting`, () => {
  const { world, nav } = arena();
  // Interleaved trunks/boulders precede a 64px gate passage and buildings.
  for (let x = 40; x <= 360; x += 80) for (let y = -210; y <= 210; y += 70) {
    obstacle(world, { id: `forest-${x}-${y}`, type: y % 140 ? 'tree' : 'rock', x, y: y + (x % 160 ? 19 : -19), r: 24, moveRadius: y % 140 ? 8 : 17 });
  }
  for (const [id, x, y, w, h] of [['north-wall', 610, -182, 20, 300], ['south-wall', 610, 182, 20, 300], ['workshop', 790, -95, 110, 80], ['cellblock', 790, 95, 110, 80]]) {
    obstacle(world, { id, type: 'wall', x, y, w, h, collision: 'rect', climbable: false });
  }
  const gate = obstacle(world, { id: 'prison-gate', type: 'gate', x: 940, y: 0, w: 18, h: 800, collision: 'rect' });
  const target = { x: 1080, y: 0 }, actors = Array.from({ length: count }, (_, i) => ({ id: `ape-${i}`, x: -110 - Math.floor(i / 25) * 6, y: (i % 25 - 12) * 14, radius: i % 4 ? 10 : 14, navCohort: 'army' }));
  let crossingsBeforeOpening = 0;
  for (let tick = 0; tick < 2400; tick++) {
    if (tick === 420) { gate.hp = 0; gate.dead = true; gate.solid = false; world.navigationChanged(gate); }
    nav.beginFrame(tick / 60, { budgetMs: Infinity });
    if (tick % 20 === 0) nav.setCohortRoute('army', actors[0], target, 14, 3, 'ape');
    for (const a of actors) {
      const x = a.x, y = a.y; nav.move(a, target.x - a.x, target.y - a.y, 115, 1 / 60);
      assert.ok(Math.hypot(a.x - x, a.y - y) <= 115 / 60 + 1e-6, 'recovery never relocates a unit');
      if (tick < 420 && a.x > 940) crossingsBeforeOpening++;
      if (tick % 60 === 0) assert.equal(world.actorBlocked(a.x, a.y, a.radius, 'ape'), false);
    }
    assert.ok(nav.stats.frameExpanded <= 192); assert.ok(nav.stats.frameSearches <= 3);
    if (tick > 420 && actors.every(a => Math.hypot(a.x - target.x, a.y - target.y) < 25)) break;
  }
  assert.equal(crossingsBeforeOpening, 0, 'a closed prison gate stays solid');
  const stranded = actors.filter(a => Math.hypot(a.x - target.x, a.y - target.y) >= 25);
  assert.equal(stranded.length, 0, JSON.stringify(stranded.slice(0, 3)));
  assert.ok(nav.stats.sharedHits > count, 'the army reuses shared corridors');
  assert.ok(nav.stats.searches < count + 30, 'the army does not require a full route per unit');
});

test('follow slots contract around trunks and thin walls, then expand after destruction', () => {
  const { world, nav } = arena(), king = { x: 0, y: 0 }, a = { id: 'ape-slot', x: -100, y: 0 };
  const wall = obstacle(world, { id: 'slot-wall', x: 48, y: 0, w: 4, h: 400, collision: 'rect' });
  nav.beginFrame(0);
  const point = nav.followTarget(a, king, 110, 0);
  assert.ok(point.x < 36, 'a formation slot cannot be assigned beyond a wall');
  assert.ok(nav.clearSegment(king.x, king.y, point.x, point.y, 10, 'ape'));
  wall.dead = true; wall.solid = false; world.navigationChanged(wall); nav.beginFrame(.01);
  assert.equal(nav.followTarget(a, king, 110, 0).x, 110, 'removed geometry immediately frees the slot');
  obstacle(world, { id: 'slot-trunk', type: 'tree', x: 110, y: 0, r: 40, moveRadius: 8 }); world.navigationChanged({ x: 110, y: 0, r: 40 }); nav.beginFrame(.02);
  const trunkSlot = nav.followTarget(a, king, 110, 0); assert.equal(world.actorBlocked(trunkSlot.x, trunkSlot.y, 10, 'ape'), false);
});

test('attacker lanes use accessible building frontage and retain collision clearance', () => {
  const { world, nav } = arena(), target = obstacle(world, { id: 'depot', x: 160, y: 0, w: 100, h: 160, collision: 'rect' });
  nav.beginFrame(0);
  const points = Array.from({ length: 25 }, (_, i) => nav.approachTarget({ id: `ape-lane-${i}`, x: 0, y: (i - 12) * 2 }, target));
  assert.ok(new Set(points.map(p => `${p.x.toFixed(1)},${p.y.toFixed(1)}`)).size > 10, 'attackers spread across real frontage');
  for (const point of points) { assert.ok(point.x < target.x); assert.equal(world.actorBlocked(point.x, point.y, 10, 'ape'), false); }
});

test('dry crowd cell shortcut keeps live thin-wall intersections and the full neighbor envelope', () => {
  const { world, nav } = arena(), wall = obstacle(world, { id: 'edge-wall', x: 106, y: 0, w: 1, h: 80, collision: 'rect' });
  const a = { x: 63, y: 0 }, b = { x: 107, y: 0 }; nav.beginSeparation();
  assert.equal(nav.separationClear(a, b), false, 'a wall in the last four pixels of the neighbor envelope is included');
  wall.dead = true; assert.equal(nav.separationClear(a, b), true, 'live destruction is observed without a cached pair result');
  assert.ok(nav.stats.separationFastHits > 0);
});

test('automatic charge targeting skips town civilian homes but keeps military targets', () => {
  const c = loadEngine(), g = new c.ATSGame('CIVILIAN-TARGETS');
  g.world.objects.clear(); g.world._spatial.clear(); g.world._streaming = false;
  const a = g.makeApe(0, 0, 'charge'); a.personality = 'bold';
  obstacle(g.world, { id: 'civilian', type: 'house', x: 25, y: 0, civilian: true, combatTarget: false });
  obstacle(g.world, { id: 'military', type: 'barracks', x: 90, y: 0 });
  g.humanGrid.rebuild([]); g.vehicleGrid.rebuild([]);
  assert.equal(g.tactics.pick(a, 220).id, 'military');
});

test('human and large-radius tank approach lanes complete real melee attacks', () => {
  for (const kind of ['human', 'vehicle']) {
    const c = loadEngine(), g = new c.ATSGame('MELEE-LANES-' + kind);
    g.world.objects.clear(); g.world._spatial.clear(); g.world._streaming = false;
    g.world.terrain = () => ({ biome: 'forest', water: false }); g.world.waterBlocked = () => false;
    g.king.x = -1000;
    const a = g.makeApe(0, 0, 'charge'); a.species = 'gorilla'; g.siege.balance(a, true); a.chargeTime = 100; a._tacticThink = 1000;
    const target = kind === 'human' ? g.makeHuman(160, 0, null) : { id: 'vehicle-large-tank', vehicleClass: 'tank', kind: 'tank', r: 55, x: 160, y: 0, hp: 2000, maxHp: 2000 };
    if (kind === 'vehicle') g.vehicles.push(target);
    a._tacticTarget = { id: target.id, kind }; g.syncIndexes();
    g.apeGrid.rebuild(g.apes, g.king); g.humanGrid.rebuild(g.humans); g.vehicleGrid.rebuild(g.vehicles);
    const hp = target.hp;
    for (let tick = 0; tick < 180 && target.hp === hp; tick++) { g.time = tick / 60; g.performance.beginStep(g); g.navigation.beginFrame(g.time, { budgetMs: Infinity }); g.updateApe(a, 1 / 60); }
    assert.ok(target.hp < hp, `${kind} approach must enter the real melee threshold`);
    assert.ok(Math.hypot(a.x - target.x, a.y - target.y) <= (kind === 'human' ? 30 : 40));
  }
});
