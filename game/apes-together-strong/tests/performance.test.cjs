'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { SCENARIOS, loadEngine, runScenario } = require('./performance-harness.cjs');

function emptyGame() {
  const c = loadEngine(), g = new c.ATSGame('performance-regression');
  g.world.objects.clear(); g.world._spatial.clear(); g.world.sites.clear();
  g.world.ensure = () => {};
  g.world.stream = () => {};
  g.world._streaming = false;
  g.world.getObjects = () => [];
  g.world.getSites = () => [];
  g.world.terrain = () => ({ biome: 'forest', water: false });
  g.world.lineClear = () => true;
  g.spawnSites = () => {};
  return { c, g };
}

for (const scenario of SCENARIOS) test(`stress ${scenario.id}: ${scenario.label} retains actors and bounded work`, () => {
  const { game, report } = runScenario(loadEngine(), scenario, scenario.military ? 180 : 120);
  assert.equal(report.apes, scenario.apes, 'optimization must retain the requested population');
  assert.equal(report.humans, scenario.humans);
  assert.ok(report.maxSearches <= 3, `A* request budget exceeded: ${report.maxSearches}`);
  assert.ok(report.maxExpanded <= 192, `A* expansion budget exceeded: ${report.maxExpanded}`);
  assert.ok(report.maxQueue <= 384, `navigation queue exceeds bounded storage: ${report.maxQueue}`);
  assert.ok(report.maxThink <= 32, `AI thinking budget exceeded: ${report.maxThink}`);
  assert.ok(report.maxLos <= 96, `perception LOS budget exceeded: ${report.maxLos}`);
  assert.ok(report.maxStreamSteps <= 3, `world generation budget exceeded: ${report.maxStreamSteps}`);
  assert.ok(report.maxSeparationPairs <= scenario.apes * 8, 'each ape separates against at most eight neighbors');
  assert.ok(report.peakEffects <= 448, 'bounded visible effect overflow');
  assert.ok(report.maxThink > 0, 'AI scheduler must perform real work');
  for (const actor of [...game.apes, ...game.humans]) assert.ok(Number.isFinite(actor.x) && Number.isFinite(actor.y));
  if (scenario.combat) {
    assert.ok(report.damage > 0, 'combat must still inflict damage');
    assert.ok(report.peakBullets > 0, 'human weapons must still fire');
  }
  if (scenario.military) {
    assert.ok(report.peakCannonWarnings > 0, 'combined arms benchmark must exercise visible cannon preparation');
    assert.ok(report.peakShells > 0, 'combined arms benchmark must exercise traveling armored shells');
    assert.ok(report.peakGrenades > 0, 'combined arms benchmark must exercise infantry explosives');
    assert.equal(report.tanks, scenario.tanks);
    assert.equal(report.armored, scenario.armored);
    assert.equal(report.helis, scenario.helis);
    if (scenario.mortars) assert.ok(report.peakMortars > 0, 'extreme-war benchmarks must exercise mortar telegraphs and impacts');
  }
  if (scenario.settled) {
    assert.equal(report.followers, scenario.following);
    assert.equal(report.settled, scenario.settled);
    assert.ok(report.peakAbstractApes >= scenario.settled, 'distant residents keep their real actors at a strategic simulation rate');
  }
  if (scenario.alarm) assert.equal(game.world.sites.get('stress-garrison').alarm, true);
  if (scenario.settlement) assert.equal(game.settlements[0].population, scenario.apes);
  if (scenario.exploration) assert.ok(report.chunksAdded > 0, 'streaming must generate terrain');
  // Hardware-specific latency gates are opt-in; operation ceilings above run everywhere.
  if (process.env.ATS_MAX_TICK_MS) assert.ok(report.maxMs < Number(process.env.ATS_MAX_TICK_MS), `max tick ${report.maxMs.toFixed(1)}ms`);
});

test('distant residents keep family progress, avoid detailed AI and wake immediately near the king', () => {
  const { g } = emptyGame();
  const a = g.makeApe(5000, 0, 'young', null, true);
  a.age = 34;
  let details = 0;
  const update = g.updateApe.bind(g);
  g.updateApe = (actor, dt) => { details++; return update(actor, dt); };
  for (let i = 0; i < 120; i++) g.update(1 / 60, {});
  assert.equal(a._simTier, 2);
  assert.equal(details, 0, 'distant tier skips expensive ape AI');
  assert.equal(a.state, 'settled', 'children still mature while abstract');
  assert.ok(a.age >= 35);
  g.king.x = a.x;
  g.update(1 / 60, {});
  assert.equal(a._simTier, 0);
  assert.equal(details, 1, 'detailed behavior resumes on the first nearby tick');
});

test('a genuinely separated distant follower queues bounded recovery around a wall', () => {
  const { g } = emptyGame();
  const wall = { id: 'remote-wall', type: 'wall', x: -1970, y: 0, w: 20, h: 200, r: 10, collision: 'rect', hp: 100, solid: true, dead: false };
  g.world.objects.set(wall.id, wall); g.world._indexObject(wall);
  const a = g.makeApe(-2000, 0, 'follow');
  for (let i = 0; i < 900; i++) {
    const x = a.x, y = a.y;
    g.update(1 / 60, {});
    assert.ok(g.navigation.stats.frameExpanded <= 192);
    assert.ok(Math.hypot(a.x - x, a.y - y) <= 60.001, 'recovery remains bounded movement');
    assert.equal(g.world.blocked(a.x, a.y, 10), false, 'follower never enters the live wall');
  }
  assert.ok(a.x > -1800, 'a distant follower does not remain stranded behind the first wall');
});

test('an attacked distant settlement and nearby alarm actors remain in immediate simulation', () => {
  const { g } = emptyGame();
  const s = { id: 'remote-home', x: 5000, y: 0, radius: 120, attack: true, population: 1 };
  g.settlements.push(s);
  const a = g.makeApe(5010, 0, 'settled', s.id);
  g.syncIndexes(); g.buildRelevance();
  assert.equal(g.actorTier(a), 0, 'settlement combat overrides distance');
  s.attack = false;
  const h = g.makeHuman(5020, 0, null);
  h.state = 'alarm';
  g.buildRelevance();
  assert.equal(g.actorTier(a), 0, 'alarm relevance overrides distance');
  h.state = 'patrol';
  g.buildRelevance();
  assert.equal(g.actorTier(a), 2, 'peaceful distant state sleeps again');
});

test('a vehicle in remote active combat still fires at nearby apes', () => {
  const { g } = emptyGame();
  const a = g.makeApe(5000, 0, 'hold');
  const h = g.makeHuman(5100, 0, null);
  h.hp = h.maxHp = 1000000; h.state = 'combat'; h.targetId = a.id; h.dir = Math.PI; h.suspicion = 1;
  const v = { id: 'vehicle-remote', x: 5200, y: 0, dir: Math.PI, kind: 'jeep', state: 'combat', hp: 230, maxHp: 230, phase: 0, shootTimer: 0 };
  g.vehicles.push(v);
  g.update(1 / 60, {});
  assert.equal(v._simTier, 0, 'remote combat promotes the vehicle');
  assert.ok(g.bullets.some(b => b.owner === v.id), 'the old king-distance gate must not suppress remote firing');
});

test('abstract vehicle journeys retain chassis clearance before returning to detailed combat', () => {
  const { g } = emptyGame();
  const trunk = { id: 'remote-trunk', type: 'tree', x: 5034, y: 23, r: 24, moveRadius: 8, hp: 100, solid: true, dead: false };
  g.world.objects.set(trunk.id, trunk); g.world._indexObject(trunk);
  const v = { id: 'vehicle-journey', x: 5000, y: 0, kind: 'jeep', state: 'raid', target: { x: 5100, y: 0 }, hp: 230, phase: 0, shootTimer: 0 };
  assert.equal(g.world.blocked(v.x, v.y, 21), false);
  g.abstractActor(v, .5, 'vehicle');
  assert.equal(g.world.blocked(v.x, v.y, 21), false, 'offscreen movement cannot place a 21-radius chassis inside a trunk');
});

test('perception queued behind a huge horde still discovers nearby enemies', () => {
  const { g } = emptyGame();
  for (let i = 0; i < 200; i++) g.makeApe(-80 - i % 20 * 4, (Math.floor(i / 20) - 5) * 7, 'hold');
  const guards = [];
  for (let i = 0; i < 150; i++) {
    const h = g.makeHuman(80 + i % 15 * 2, (Math.floor(i / 15) - 5) * 6, null);
    h.dir = Math.PI; h.hasRadio = false; h.perceptionTimer = 0; h.hp = h.maxHp = 1000000;
    guards.push(h);
  }
  const perceived = new Set(), originalPerceive = g.perceive.bind(g);
  g.perceive = (h, light) => { perceived.add(h.id); return originalPerceive(h, light); };
  // Every guard faces an unobstructed, stationary horde. Preserve it to keep the load.
  for (const a of g.apes) a.hp = a.maxHp = 1000000;
  g.king.hp = g.king.maxHp = 1000000;
  for (let i = 0; i < 300; i++) g.update(1 / 60, {});
  assert.ok(guards.every(h => perceived.has(h.id)), 'no guard is permanently starved of its own perception');
  assert.ok(guards.some(h => h.suspicion > 0 || h.reported || h.state !== 'patrol'), 'humans still discover the horde');
});

test('swept projectiles hit crossed actors once and stop at live walls', () => {
  const { g } = emptyGame();
  g.king.x = 500;
  const a = g.makeApe(0, 0, 'hold');
  g.apeGrid.rebuild([g.king, a]);
  g.bullets.push({ x: -100, y: 0, px: -100, py: 0, vx: 12000, vy: 0, damage: 20, life: 1, owner: 'human-test' });
  g.updateBullets(1 / 60);
  assert.equal(a.hp, a.maxHp - 20, 'fast projectiles cannot tunnel through a target');
  assert.equal(g.bullets.length, 0, 'projectile removed after one hit');
  a.x = 80; a.hp = a.maxHp;
  const wall = { id: 'blocking-wall', type: 'wall', x: 0, y: 0, w: 12, h: 120, r: 6, collision: 'rect', hp: 100, solid: true, dead: false };
  g.world.objects.set(wall.id, wall); g.world._indexObject(wall);
  g.apeGrid.rebuild([g.king, a]);
  g.bullets.push({ x: -100, y: 0, px: -100, py: 0, vx: 12000, vy: 0, damage: 20, life: 1, owner: 'human-test' });
  g.updateBullets(1 / 60);
  assert.equal(a.hp, a.maxHp, 'live structures intercept the shot');
  assert.equal(g.bullets.length, 0);
});

test('projectile objects are reused after expiry without retaining stale weapon data', () => {
  const { g } = emptyGame();
  const h = g.makeHuman(200, 0, null);
  h.kind = 'rifle';
  g.shoot(h, g.king);
  const original = g.bullets[0];
  g.updateBullets(2);
  assert.equal(g.bullets.length, 0);
  h.kind = 'pistol';
  g.shoot(h, g.king);
  assert.equal(g.bullets[0], original);
  assert.equal(g.bullets[0].damage, 38);
  assert.ok(g.bullets[0].life > 0);
});

test('performance scaling responds to sustained load and gradually restores detail', () => {
  const c = loadEngine(), p = new c.ATSPerformance();
  for (let i = 0; i < 700; i++) p.frame(45, 25, 20, 1 / 60, true);
  assert.ok(p.qualityLevel >= 2 && p.qualityLevel <= 5);
  const degraded = p.qualityLevel;
  for (let i = 0; i < 120; i++) p.frame(8, 4, 4, 1 / 60, true);
  assert.equal(p.qualityLevel, degraded, 'one short recovery does not oscillate quality');
  for (let i = 0; i < 3000; i++) p.frame(8, 4, 4, 1 / 60, true);
  assert.equal(p.qualityLevel, 0);
  const snapshot = p.snapshot();
  assert.ok(snapshot.slowFrames >= 700);
  assert.equal(snapshot.frameMs, 8);
});

test('saved runs preserve wounds, settlement supplies and strategic alarms with fresh transient caches', () => {
  const c = loadEngine(), g = new c.ATSGame('save-performance');
  const a = g.makeApe(0, 0, 'follow');
  a.hp = 59;
  a._nav = { path: [{ x: 99999, y: 99999 }] };
  a._lodElapsed = .4; a.navCohort = 'temporary-path';
  const h = g.makeHuman(100, 100, null);
  h.hp = 21; h.state = 'search'; h.searchTime = 12;
  h._sense = { targets: [a], index: 0, time: 0 };
  for (let i = 0; i < 17; i++) g.makeApe(i, 0, 'follow');
  const originalSites = g.world.getSites;
  g.world.getSites = () => [];
  g.food = 200;
  assert.equal(g.command('settleAll'), true);
  g.world.getSites = originalSites;
  const s = g.settlements[0];
  s.food = 37; s.policy = 'fortify'; s.known = true;
  const site = g.world.sites.get('opening-hunters');
  site.alarm = true; site.alarmUntil = 120;
  g.syncIndexes();
  const saved = JSON.parse(JSON.stringify(g.serialize()));
  assert.equal(saved.apes[0]._nav, undefined);
  assert.equal(saved.apes[0]._lodElapsed, undefined);
  assert.equal(saved.apes[0].navCohort, undefined);
  assert.equal(saved.humans[0]._sense, undefined);
  const loaded = c.ATSGame.fromJSON(saved);
  assert.equal(loaded.apesById.get(a.id).hp, 59);
  assert.equal(loaded.humansById.get(h.id).hp, 21);
  assert.equal(loaded.humansById.get(h.id).state, 'search');
  assert.equal(loaded.humansById.get(h.id).searchTime, 12);
  assert.equal(loaded.settlementsById.get(s.id).food, 37);
  assert.equal(loaded.settlementsById.get(s.id).policy, 'fortify');
  assert.equal(loaded.world.sites.get(site.id).alarm, true);
  assert.equal(loaded.apesById.get(a.id)._nav, undefined);
  assert.equal(loaded.navigation.world, loaded.world);
  assert.equal(loaded.performance.qualityLevel, 0);
});
