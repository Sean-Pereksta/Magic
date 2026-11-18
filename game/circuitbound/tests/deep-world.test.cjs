const test = require('node:test');
const assert = require('node:assert/strict');
const Deep = require('../deep-world.js');
const { fixture, game } = require('./harness.cjs');

test('deep terrain exposes climbable shafts, rare deposits and all weapon plans', () => {
  const { T, defs } = fixture();
  const world = new Deep.World(T, defs, 41);
  const fixtures = new Map();
  world.fixturesInto((x, y, tile, data) => { world.set(x, y, tile); fixtures.set(`${x},${y}`, data); });
  assert.deepEqual([world.width, world.height], [2048, 512]);
  assert.deepEqual([84,120,196,280,368].map(y => Deep.regionAt(y).name), [
    'Fault Galleries', 'Osmium Basin', 'Gravity Hollows', 'Null Crystal Vaults', 'Starcore Mantle'
  ]);
  const descent = world.sites.find(site => site.id === 'entrance-0');
  for (let y = descent.y; y <= 394; y++) assert.equal(world.get(descent.x, y), T.LADDER, `shaft at depth ${y}`);
  const first = world.sites.find(site => site.id === 'seam-0-0');
  for (let y = first.y - 5; y < first.y; y++) assert.equal(world.get(first.x, y), T.AIR);
  assert.equal(world.get(first.x, first.y), T.OSMIUM_ORE);
  const plans = world.sites.filter(site => site.id.startsWith('plan-'));
  assert.equal(new Set(plans.map(site => site.schematic)).size, 16);
  for (const plan of plans) {
    assert.equal(world.get(plan.x, plan.y), T.STORAGE);
    assert.equal(fixtures.get(`${plan.x},${plan.y}`).schematic, plan.schematic);
  }
});

test('sparse saves keep edits, explored cells and both survey precisions', () => {
  const { T, defs } = fixture();
  const world = new Deep.World(T, defs, 891);
  const seam = world.sites.find(site => site.kind === 'deposit');
  world.set(seam.x, seam.y, T.AIR);
  assert.ok(world.reveal(seam.x, seam.y));
  assert.ok(world.isExplored(seam.x, seam.y));
  assert.ok(!world.isExplored(2047, 511));
  assert.notEqual(world.survey(seam.x, seam.y, 5, false)[0].x, seam.x);
  assert.equal(world.survey(seam.x, seam.y, 5, true)[0].x, seam.x);
  world.markers.push({id:'my-base', x:seam.x, y:seam.y - 2, name:'First Refinery', kind:'outpost'});
  world.pinned = 'my-base';
  const encoded = JSON.parse(JSON.stringify(world.export()));
  assert.ok(Deep.validSave(encoded, defs));
  const restored = new Deep.World(T, defs, 891, encoded);
  assert.equal(restored.get(seam.x, seam.y), T.AIR);
  assert.ok(restored.isExplored(seam.x, seam.y));
  assert.deepEqual(restored.surveyMarkers, world.surveyMarkers);
  assert.deepEqual(restored.markers, world.markers);
  assert.equal(restored.pinned, 'my-base');
});

test('pre-expansion save opens on the large map with original terrain and machine buffers', () => {
  const { T } = fixture();
  const old = game();
  old.saveGame();
  const legacy = JSON.parse(old.storage.get('circuitbound-save-v1'));
  legacy.version = 5;
  legacy.world = Array.from({length:220 * 84}, (_, key) => old.get(key % 220, Math.floor(key / 220)));
  legacy.meta = legacy.meta.filter(([key]) => key % 2048 < 220 && Math.floor(key / 2048) < 84)
    .map(([key, data]) => [key % 2048 + Math.floor(key / 2048) * 220, data]);
  const stash = 120 + 78 * 220;
  legacy.world[stash] = T.STORAGE;
  legacy.world[181 + 83 * 220] = T.STONE;
  legacy.meta = legacy.meta.filter(([key]) => key !== stash);
  legacy.meta.push([stash, {rot:2, contents:{aetheriumOre:5}, outputs:{steel:3}}]);
  legacy.player = {x:175 * 24, y:80 * 24, health:84};
  legacy.inventory.cobaltOre = 17;
  const migrated = game(legacy);
  assert.equal(migrated.get(120, 78), T.STORAGE);
  const buffer = migrated.engine.data(migrated.idx(120, 78));
  assert.equal(buffer.rot, 2);
  assert.equal(buffer.contents.aetheriumOre, 5);
  assert.equal(buffer.outputs.steel, 3);
  assert.equal(migrated.get(181, 83), T.STONE);
  assert.equal(migrated.get(181, 84), T.LADDER);
  assert.equal(migrated.inventory.cobaltOre, 17);
  assert.equal(migrated.player.x, legacy.player.x);
  assert.equal(migrated.player.y, legacy.player.y);
  assert.equal(migrated.survival.regionAt(181, 82).id, 'abyss');
  assert.equal(migrated.survival.regionAt(181, 84).id, 'faultGalleries');
  migrated.saveGame();
  const persisted = JSON.parse(migrated.storage.get('circuitbound-save-v1'));
  assert.equal(persisted.version, 6);
  assert.ok(Deep.validSave(persisted.world, migrated.defs));
  assert.ok(persisted.world.edits.length < 40000);
});

test('remote production uses fewer updates and retains real inventory and output limits', () => {
  const g = game();
  const { T } = g;
  const smelter = (x) => {
    g.set(x, 63, T.SMELTER, {rot:0, contents:{crushedIron:4}});
    g.set(x, 64, T.GENERATOR, {rot:0});
    return g.engine.data(g.idx(x, 63));
  };
  const local = smelter(15), remote = smelter(800);
  for (let i = 0; i < 4; i++) g.simulationStep(.05);
  assert.ok(local.progress > 0);
  assert.equal(remote.progress, undefined);
  for (let i = 4; i < 45; i++) g.simulationStep(.05);
  assert.equal(local.outputs.ironIngot, 1);
  assert.equal(remote.outputs.ironIngot, 1);
  assert.equal(remote.contents.crushedIron, 3);
  remote.outputs.ironIngot = g.defs[T.SMELTER].outputCapacity;
  for (let i = 0; i < 45; i++) g.simulationStep(.05);
  assert.equal(remote.contents.crushedIron, 3);
  assert.equal(remote.outputs.ironIngot, g.defs[T.SMELTER].outputCapacity);
});
