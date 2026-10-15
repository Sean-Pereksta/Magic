'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { loadEngine } = require('./performance-harness.cjs');

function openGame() {
  const c = loadEngine(), g = new c.ATSGame('connected-frontier', 'survival'), w = g.world;
  w.objects.clear();w.sites.clear();w.chunks.clear();w._spatial.clear();w._regionalFacilities.clear();w.discoveries.clear();
  w.ensure = () => {};w.terrain = () => ({ biome: 'farmland', water: false, road: false });w.waterBlocked = () => false;
  g.humans = [];g.humansById.clear();g.vehicles = [];g.militaryOperations = [];g.events = [];
  g.apes = [];g.apesById.clear();g.apeGrid.rebuild([]);g.humanGrid.rebuild([]);g.vehicleGrid.rebuild([]);
  return { c, g, w };
}
function chunkAt(w, x, y) {
  const cx = Math.floor(x / 768), cy = Math.floor(y / 768), id = cx + ',' + cy;
  if (!w.chunks.has(id)) w.chunks.set(id, { id, cx, cy, x: cx * 768, y: cy * 768, objects: [], sites: [], generated: true, crossingsValidated: true });
  return w.chunks.get(id);
}
function facility(w, type, x = 0, y = 0, layout = 0) {
  const radii = { logging: 222, communications: 215, town: 310, mobileCamp: 185 }, id = type + ':' + x + ':' + y;
  w._buildSite(chunkAt(w, x, y), { id, type, x, y, layout, radius: radii[type], count: 8, guards: 5, tier: 3, name: type,
    entrance: { x, y: y + radii[type] + 26 }, approach: [{ x, y: y + radii[type] + 26 }, { x, y: y + 350 }] });
  const site = w.sites.get(id);w._indexRegionalSite(site);w.chunkRevision++;
  return site;
}

test('new facilities join the deterministic catalog while all existing military hubs remain available', () => {
  const c = loadEngine(), a = new c.ATSWorld('expanded-catalog'), b = new c.ATSWorld('expanded-catalog'), found = new Map();
  for (let cy = -20; cy <= 20; cy++) for (let cx = -20; cx <= 20; cx++) {
    const plan = a._sitePlan(cx, cy);
    if (plan && !found.has(plan.type)) { found.set(plan.type, plan);assert.equal(JSON.stringify(plan), JSON.stringify(b._sitePlan(cx, cy))); }
  }
  for (const type of ['logging', 'communications', 'town', 'mobileCamp', 'forwardBase', 'armoredDepot', 'regionalCommand', 'research', 'prison']) assert.ok(found.has(type), type);
  assert.equal(a._sitePlan(0, 0).type, 'hunter');assert.equal(a._sitePlan(0, -1).type, 'transport');
});

test('each new mirrored blueprint has a real gorilla-clear entrance lane and reachable cage approaches', () => {
  for (const type of ['logging', 'communications', 'town', 'mobileCamp']) for (const layout of [0, 1, 2]) {
    const { g, w } = openGame(), site = facility(w, type, 0, 0, layout), objects = site.objects.map(id => w.objects.get(id));
    assert.equal(g.navigation.clearSegment(0, site.radius + 26, 0, -site.radius - 28, 16), true, type + ' central lane');
    assert.equal(objects.filter(o => o.type === 'cage').reduce((n, o) => n + o.count, 0), site.count);
    for (const cage of objects.filter(o => o.type === 'cage')) {
      const y = cage.y + (type === 'communications' ? -60 : 60);
      assert.equal(g.navigation.clearSegment(0, site.radius + 26, 0, y, 16), true, type + ' center approach');
      assert.equal(g.navigation.clearSegment(0, y, cage.x, y, 16), true, type + ' cage approach');
    }
    assert.ok(site.vehicleInventory.truck > 0);assert.equal(site.vehicleInventory.tank, 0);
    if (type === 'town') assert.equal(objects.filter(o => o.type === 'house').every(o => o.civilian && o.combatTarget === false), true);
  }
});

test('regional infrastructure consequences survive unloading and save/load, with live destruction invalidation', () => {
  const { c, w } = openGame(), comms = facility(w, 'communications', 800, 0), logging = facility(w, 'logging', -800, 0);
  const base = { id: 'base', x: 0, y: 0, tier: 5, military: true, strength: 0, initialStrength: 50, objects: [],
    vehicleInventory: {}, initialInventory: {}, armorCapacity: 0, campaignReserve: { personnel: 100, vehicles: {}, armor: 0 } };
  w.sites.set(base.id, base);chunkAt(w, 0, 0).sites.push(base.id);
  assert.equal(w.regionalSupport(base).coordination, 1);assert.equal(w.regionalSupport(base).logistics, 1);
  w.objects.get(comms.objects.find(id => w.objects.get(id).type === 'radio')).dead = true;
  w.objects.get(logging.objects.find(id => w.objects.get(id).type === 'depot')).hp = 0;
  assert.equal(w.regionalSupport(base).coordination, .45);assert.equal(w.regionalSupport(base).logistics, .55);
  assert.equal(w.replenishInstallation(base, 90), true);assert.equal(base.strength, 18);assert.equal(base.nextMobilizationAt, 290);
  w.trimDistant(9000, 9000, [], 1000, 10);
  assert.equal(w.regionalSupport(base).coordination, .45);assert.equal(w.regionalSupport(base).logistics, .55);
  const restored = c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(w.serialize()))), support = restored.regionalSupport(restored.sites.get(base.id));
  assert.equal(support.coordination, .45);assert.equal(support.logistics, .55);
});

test('prisoner transports conserve captive stock and leave a persistent reachable rescue cage when intercepted', () => {
  const { c, g, w } = openGame(), source = facility(w, 'logging', 600, 0), destination = facility(w, 'town', 1450, 0);
  source.spawned = destination.spawned = true;w.humanActivity.serial = 1;g.time = 130;
  const before = source.count + destination.count;
  assert.equal(g.forces.dispatchConnectedOperation([source, destination]), true);
  const vehicle = g.vehicles.find(v => v.prisonerTransport), operation = w.humanActivity.operations[0];
  assert.ok(vehicle);assert.equal(vehicle.captiveCount + source.count + destination.count, before);
  assert.equal(source.vehicleInventory.truck, 1);assert.equal(operation.channel, 'encounter');
  g.hurt(vehicle, 10000, { x: vehicle.x, y: vehicle.y, armorPiercing: true });
  const rescue = w.sites.get('rescue:' + vehicle.id), cage = w.objects.get(rescue.objects[0]);
  assert.equal(vehicle.captiveCount, 0);assert.equal(cage.count + source.count + destination.count, before);
  assert.equal(cage.x, vehicle.x);assert.equal(cage.y, vehicle.y);
  assert.equal(g.navigation.clearSegment(cage.x, cage.y - 80, cage.x, cage.y - 40, 12), true);
  const saved = c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(w.serialize())));
  assert.equal(saved.objects.get(cage.id).count, cage.count);assert.equal(saved.humanActivity.operations[0].vehicleId, vehicle.id);
});

test('arriving transports deliver prisoners once and return their surviving escort to facility duty', () => {
  const { g, w } = openGame(), source = facility(w, 'logging', 600, 0), destination = facility(w, 'town', 1450, 0);
  source.spawned = destination.spawned = true;w.humanActivity.serial = 1;g.time = 130;
  assert.equal(g.forces.dispatchConnectedOperation([source, destination]), true);
  const operation = w.humanActivity.operations[0], vehicle = g.vehicles.find(v => v.id === operation.vehicleId), captives = vehicle.captiveCount, before = destination.count;
  vehicle.x = operation.target.x;vehicle.y = operation.target.y;g.time = 132;g.forces.updateWorldActivity();
  assert.equal(operation.status, 'completed');assert.equal(destination.count, before + captives);assert.equal(vehicle.captiveCount, 0);
  assert.equal(operation.members.every(id => !g.humansById.get(id).operationId), true);
  g.time = 134;g.forces.updateWorldActivity();assert.equal(destination.count, before + captives);
});

test('save/load rejoins encounter director records and resolves director expiry exactly once', () => {
  const { c, g, w } = openGame(), source = facility(w, 'logging', 600, 0), destination = facility(w, 'town', 1450, 0);
  source.spawned = destination.spawned = true;w.humanActivity.serial = 1;g.time = 130;
  assert.equal(g.forces.dispatchConnectedOperation([source, destination]), true);
  const loaded = c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))), saved = loaded.world.humanActivity.operations[0];
  assert.notEqual(saved, loaded.militaryOperations.find(op => op.id === saved.id), 'JSON has separate object records');
  loaded.time = saved.expiresAt + 1;loaded.tickMilitaryOperations();loaded.forces.updateWorldActivity();
  const resolved = loaded.world.humanActivity.operations[0];
  assert.equal(resolved, loaded.militaryOperations.find(op => op.id === saved.id));assert.equal(resolved.activityResolved, true);assert.equal(resolved.status, 'expired');
  assert.equal(resolved.members.every(id => !loaded.humansById.get(id).operationId), true);
  const rescue = loaded.world.sites.get('rescue:' + resolved.vehicleId);assert.equal(rescue.count, 4);
  loaded.time += 2;loaded.forces.updateWorldActivity();assert.equal(rescue.count, 4);
});

test('blocked transport losses preserve cargo through saving and retry only at the exact reachable location', () => {
  const { c, g, w } = openGame(), source = facility(w, 'logging', 600, 0), destination = facility(w, 'town', 1450, 0);
  source.spawned = destination.spawned = true;w.humanActivity.serial = 1;g.time = 130;
  assert.equal(g.forces.dispatchConnectedOperation([source, destination]), true);
  const vehicle = g.vehicles.find(v => v.prisonerTransport), wall = w._object(chunkAt(w, vehicle.x, vehicle.y),
    { id: 'new-defense', type: 'wall', x: vehicle.x, y: vehicle.y, r: 40, w: 80, h: 20, collision: 'rect', hp: 100 });
  g.hurt(vehicle, 10000, { x: vehicle.x, y: vehicle.y, armorPiercing: true });
  assert.equal(w.humanActivity.pendingRescues.length, 1);assert.equal(w.humanActivity.pendingRescues[0].captiveCount, 4);assert.equal(vehicle.captiveCount, 0);
  assert.equal(w.sites.has('rescue:' + vehicle.id), false);
  const savedWorld = c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(w.serialize())));assert.equal(savedWorld.humanActivity.pendingRescues[0].captiveCount, 4);
  wall.dead = true;wall.solid = false;wall.hp = 0;w.navigationChanged(wall);g.time += 2;g.forces.updateWorldActivity();
  const rescued = w.sites.get('rescue:' + vehicle.id);assert.equal(rescued.x, vehicle.x);assert.equal(rescued.y, vehicle.y);assert.equal(rescued.count, 4);assert.equal(w.humanActivity.pendingRescues.length, 0);
});

test('facility defenders return along real navigation and engineers repair only surviving uncontested defenses', () => {
  const { g, w } = openGame(), site = facility(w, 'communications');
  const guard = g.makeHuman(600, 0, site);guard.homeX = 0;guard.homeY = 0;guard.state = 'combat';guard.targetId = 'king';g.king.x = 900;
  const start = guard.x;assert.equal(g.forces.returnDefender(guard, .25), true);assert.ok(guard.x < start);assert.equal(guard.targetId, null);
  guard.operationId = 'independent-patrol';assert.equal(g.forces.returnDefender(guard, .25), false);
  const engineer = g.makeHuman(-60, -82, site);g.forces.assign(engineer, site, 'engineer');
  const radio = site.objects.map(id => w.objects.get(id)).find(o => o.type === 'radio');radio.hp = 100;
  assert.equal(g.forces.updateEngineer(engineer, 1), true);assert.equal(radio.hp, 112);
  radio.dead = true;engineer._repairThink = 0;g.time = 2;g.forces.updateEngineer(engineer, 1);assert.equal(radio.hp, 112);
});

test('discoveries reward exploration once and legacy saves initialize compatible activity state', () => {
  const { c, g, w } = openGame();w.discoveries.set('discovery:0,0', { id: 'discovery:0,0', type: 'lostApes', name: 'Lost Ape Refuge', x: 80, y: 0, apes: 3, known: false, claimed: false });
  g.forces.discoverNearby();assert.equal(g.apes.length, 3);assert.equal(g.stats.freed, 3);assert.equal(g.events.filter(e => e.type === 'discovery').length, 1);
  g.forces.discoverNearby();assert.equal(g.apes.length, 3);assert.equal(g.events.filter(e => e.type === 'discovery').length, 1);
  const legacy = c.ATSWorld.fromJSON({ seed: 'legacy', chunks: [], sites: [], objects: [] });
  assert.equal(legacy.humanActivity.nextAt, 120);assert.equal(legacy.discoveries.size, 0);
});
