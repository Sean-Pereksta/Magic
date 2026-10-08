'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { loadEngine } = require('./performance-harness.cjs');
const c = loadEngine();
function fixture() {
  const world = new c.ATSWorld('local-navigation');
  world.ensure = () => {}; world.terrain = () => ({ biome: 'grassland', water: false, road: false });
  world.objects.clear(); world._spatial.clear();
  const gate = { id: 'local-gate', type: 'gate', x: 56, y: 0, w: 12, h: 112, r: 6, collision: 'rect', hp: 100, solid: true, gateState: 'closed' };
  world.objects.set(gate.id, gate); world._indexObject(gate);
  const nav = new c.ATSNavigation(world); nav.beginFrame(0, { budgetMs: Infinity });
  return { world, gate, nav };
}
function solve(nav, from, to) {
  for (let n = 0; n < 1200; n++) {
    const path = nav.findPath(from, to, 10, 3, 'ape');
    if (path !== null) return path;
    nav.beginFrame(nav.time + 1 / 60, { budgetMs: Infinity });
    assert.ok(nav.stats.frameExpanded <= 192); assert.ok(nav.stats.frameSearches <= 3);
  }
  assert.fail('bounded solver did not settle');
}

test('opening one gate invalidates local paths and collision cells while retaining distant route and cache hits', () => {
  const { world, gate, nav } = fixture(), from = { x: 0, y: 0 }, to = { x: 112, y: 0 }, far = { x: 5600, y: 0 }, farTo = { x: 5712, y: 0 };
  const nearPath = solve(nav, from, to), farPath = solve(nav, far, farTo), epoch = nav.revision;
  assert.ok(nearPath.length && farPath.length);
  assert.equal(nav.walk(2, 0, 10, 'ape'), false);
  assert.equal(nav.clearSegment(0, 0, 112, 0, 10, 'ape'), false);
  assert.equal(nav.clearSegment(5600, 0, 5712, 0, 10, 'ape'), true);
  const cacheSize = nav.segmentCache.size;
  gate.solid = false; gate.gateState = 'open'; world.navigationChanged(gate);
  nav.beginFrame(nav.time + 1 / 60, { budgetMs: Infinity, maxExpanded: 0 });
  assert.equal(nav.revision, epoch, 'a local mutation does not advance the full-reset epoch');
  assert.equal(nearPath._navInvalid, true, 'actors holding a shared old path see its invalidation');
  assert.equal(farPath._navInvalid, false);
  assert.equal(nav.findPath(far, farTo, 10, 3, 'ape'), farPath, 'distant route object is reused');
  assert.equal(nav.clearSegment(5600, 0, 5712, 0, 10, 'ape'), true);
  assert.equal(nav.segmentCache.size, cacheSize, 'unchanged distant segment uses its existing cache entry');
  assert.equal(nav.walk(2, 0, 10, 'ape'), true, 'the newly opened gate immediately changes walkability');
  assert.equal(nav.clearSegment(0, 0, 112, 0, 10, 'ape'), true, 'a prior negative line cache cannot survive the opening');
  assert.ok(solve(nav, from, to).length);
});

test('a changed gate restarts overlapping pending search state but preserves a distant heap and closed nodes', () => {
  const { world, gate, nav } = fixture();
  gate.solid = false; world.navigationChanged(gate); nav.beginFrame(.01, { maxExpanded: 0 });
  nav.findPath({ x: 0, y: 0 }, { x: 400, y: 0 }, 10, 3, 'ape');
  nav.findPath({ x: 5600, y: 0 }, { x: 6400, y: 0 }, 10, 3, 'ape');
  const [near, far] = [...nav.pending.values()];
  for (const job of [near, far]) for (let n = 0; n < 200 && job.expanded < 5; n++) nav._step(job);
  assert.equal(near.phase, 'expand'); assert.equal(far.phase, 'expand');
  const heap = far.heap, nodes = far.nodes, expanded = far.expanded;
  gate.solid = true; gate.gateState = 'closed'; world.navigationChanged(gate);
  nav.beginFrame(.02, { maxExpanded: 0 });
  assert.ok(!nav.pending.has(near.key), 'closed gate invalidates previously accepted local nodes');
  assert.equal(nav.pending.get(far.key), far);
  assert.equal(far.heap, heap); assert.equal(far.nodes, nodes); assert.equal(far.expanded, expanded);
  assert.equal(nav.findPath({ x: 0, y: 0 }, { x: 400, y: 0 }, 10, 3, 'ape'), null);
  assert.notEqual(nav.pending.get(near.key), near, 'local request restarts from valid geometry');
  assert.equal(nav.clearSegment(0, 0, 112, 0, 10, 'ape'), false);
});

test('local changes retain unrelated cohorts and refresh a nearby actor direct-route decision', () => {
  const { world, gate, nav } = fixture(), near = { id: 'human-near', type: 'human', x: 0, y: 0 }, far = { id: 'ape-far', type: 'ape', x: 5600, y: 0 };
  gate.solid = false; world.navigationChanged(gate); nav.beginFrame(.01, { maxExpanded: 0 });
  const target = { x: 112, y: 0 };
  nav.steer(near, target, 10, 1 / 60); assert.equal(near._nav.direct, true);
  const farPath = solve(nav, far, { x: 5712, y: 0 });
  const cohort = { path: farPath, goal: { x: 5712, y: 0 }, revision: nav.revision, touched: nav.time, leader: far, radius: 10 };
  nav.cohorts.set('distant-colony', cohort);
  gate.solid = true; world.navigationChanged(gate); nav.beginFrame(nav.time + .001, { maxExpanded: 0 });
  assert.equal(nav.cohorts.get('distant-colony'), cohort);
  nav.steer(near, target, 10, 1 / 60);
  assert.equal(near._nav.direct, false, 'closing gate refreshes a direct segment before its regular timer expires');
});

test('unlocated legacy revisions and truncated local history safely fall back to a full reset', () => {
  const { world, nav } = fixture(), from = { x: 5600, y: 0 }, to = { x: 5712, y: 0 };
  const path = solve(nav, from, to), epoch = nav.revision;
  world.navRevision++;
  nav.beginFrame(nav.time + .01, { maxExpanded: 0 });
  assert.ok(nav.revision > epoch); assert.equal(nav.routes.size, 0); assert.equal(path._navInvalid, true);
  const revision = world.navRevision;
  for (let i = 0; i < 140; i++) world.navigationChanged({ x: i, y: 0, r: 10 });
  assert.equal(world._navigationChanges.length, 128, 'change history is bounded');
  assert.equal(world.navigationChangesSince(revision), null, 'missing old dependencies are never treated as unchanged');
  const nextEpoch = nav.revision; nav.beginFrame(nav.time + .01, { maxExpanded: 0 });
  assert.ok(nav.revision > nextEpoch);
});

test('real gate, tree, fortification and moved-building APIs publish affected geometry bounds', () => {
  const { world, gate } = fixture(), g = new c.ATSGame('local-api'); g.world = world;
  let before = world.navRevision; g.siege.openGate(gate);
  assert.ok(world.navigationChangesSince(before)?.some(change => change.minX <= gate.x && change.maxX >= gate.x));
  const tree = { id: 'tree', type: 'tree', x: 200, y: 200, r: 20, hp: 100, solid: true }; world.objects.set(tree.id, tree); world._indexObject(tree);
  before = world.navRevision; world.clearTree(tree); assert.ok(world.navigationChangesSince(before)?.length);
  before = world.navRevision; const wall = world.createFortification({ id: 'wall', x: 300, y: 300 }); world.damageFortification(wall, wall.hp); world.repairFortification(wall, 10);
  assert.equal(world.navigationChangesSince(before).length, 3);
  const settlement = { id: 's', huts: [{ id: 'hut', x: 0, y: 0, hp: 100, maxHp: 100, stage: 4 }] };
  world.syncSettlementBuildings(settlement); before = world.navRevision;
  settlement.huts[0].x = 2000; world.syncSettlementBuildings(settlement);
  const moved = world.navigationChangesSince(before);
  assert.ok(moved.some(change => change.minX < 0 && change.maxX > 2000), 'both vacated and newly occupied footprints expire');
});
