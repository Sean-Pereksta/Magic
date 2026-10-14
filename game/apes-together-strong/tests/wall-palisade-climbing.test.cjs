'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { loadEngine } = require('./performance-harness.cjs');

function arena() {
  const c = loadEngine(), g = new c.ATSGame('WALL-PALISADE-REGRESSION'), w = g.world;
  w.objects.clear(); w._spatial.clear(); w.sites.clear();
  w.ensure = () => {}; w.stream = () => {}; w.getSites = () => []; w.trimDistant = () => {};
  w.terrain = () => ({ biome: 'farmland', road: true, water: false });
  w.vehicleTerrain = w.terrain; w.waterBlocked = () => false; w._streaming = false;
  g.spawnSites = () => {}; g.nextDirectorAt = g.nextConvoyAt = g.heliTimer = 1e9;
  g.king.x = -400; g.king.y = 0;
  return { c, g, w, nav: g.navigation };
}
function addObject(w, fields) {
  const o = { id: 'wall-regression-' + w.objects.size, x: 50, y: 0, hp: 1000, maxHp: 1000, solid: true, ...fields };
  w.objects.set(o.id, o); w._indexObject(o); w.navigationChanged(o);
  return o;
}
function palisade(w, fields = {}) {
  // These are the collision flags used by completed village perimeter projects.
  return w.createFortification({ x: 50, y: 0, w: 18, h: 1800, height: 16,
    team: 'ape', owner: 'ape', settlementId: 'home', settlementWall: true,
    ringId: 1, ringLayout: 'enclosure-v1', ...fields });
}
function wall(w, fields = {}) {
  return addObject(w, { type: 'wall', faction: 'human', collision: 'rect', w: 18, h: 1400,
    wallTier: 1, climbable: true, visualHeight: 42, ...fields });
}
function ape(g, species, x = 0, y = 0) {
  const a = g.makeApe(x, y, 'follow'); a.species = species; g.siege.balance(a, true); return a;
}
function grids(g) {
  g.syncIndexes(); g.apeGrid.rebuild(g.apes, g.king); g.humanGrid.rebuild(g.humans);
  g.vehicleGrid.rebuild(g.vehicles); g.navigation.beginFrame(g.time, { budgetMs: Infinity });
  g.performance.beginStep(g);
}
function move(g, a, target, frames = 150, observed = () => {}) {
  for (let i = 0; i < frames; i++) {
    g.time += .02; g.navigation.beginFrame(g.time, { budgetMs: Infinity });
    g.move(a, target.x - a.x, target.y - a.y, 90, .02); observed(a);
  }
}
function orderStep(g, observed = () => {}, frames = 1) {
  for (let i = 0; i < frames; i++) {
    g.time += 1 / 30; grids(g); g.siege.tick(1 / 30);
    for (const a of g.apes) { g.updateApe(a, 1 / 30); observed(a); }
  }
}

test('the player and all six species physically climb real village palisades in both directions', () => {
  for (const horizontal of [false, true]) for (const species of ['king', 'gorilla', 'chimpanzee', 'orangutan', 'gibbon', 'mandrill', 'capuchin']) {
    const { c, g, w, nav } = arena(), b = palisade(w, horizontal ? { x: 0, y: 50, w: 1800, h: 18 } : {});
    const a = species === 'king' ? g.king : ape(g, species);
    a.x = a.y = 0; const hp = a.hp;
    for (const destination of [160, 0]) {
      const phases = new Set(), target = horizontal ? { x: 0, y: destination } : { x: destination, y: 0 };
      move(g, a, target, 160, actor => { const p = c.ATSPalisadePose(actor); if (p) phases.add(p.phase); });
      assert.deepEqual([...phases], ['climb', 'crest', 'jump'], species + ' uses a finite climb');
      assert.ok(Math.hypot(a.x - target.x, a.y - target.y) < 12, species + ' reaches the opposite side');
      assert.equal(a.palisadeClimb, undefined); assert.equal(nav.blocked(a.x, a.y, species === 'king' ? 12 : 10, 'ape'), false);
    }
    assert.equal(a.hp, hp, species + ' crosses without injury'); assert.equal(b.hp, b.maxHp, 'allied wall remains intact');
  }
});

test('normal player input and autonomous followers cross settlement sections during complete game updates', () => {
  for (const species of ['king', 'gorilla', 'chimpanzee', 'orangutan', 'gibbon', 'mandrill', 'capuchin']) {
    const { g, w, nav } = arena(), b = palisade(w);
    const a = species === 'king' ? g.king : ape(g, species), hp = a.hp;
    a.x = a.y = 0;
    if (species !== 'king') { g.king.x = 180; g.king.y = 0; }
    let climbed = false;
    for (let frame = 0; frame < 180; frame++) {
      g.update(.02, species === 'king' ? { x: 1, y: 0 } : {}); climbed ||= !!a.palisadeClimb;
    }
    assert.ok(climbed, species + ' uses a physical crossing with live steering');
    assert.ok(a.x > 75, species + ' crosses to the king side');
    assert.equal(a.palisadeClimb, undefined); assert.equal(nav.blocked(a.x, a.y, species === 'king' ? 12 : 10, 'ape'), false);
    assert.equal(a.hp, hp); assert.equal(b.hp, b.maxHp);
  }
});

test('human attack orders cross allied palisades instead of attacking or rejecting the barrier', () => {
  for (const species of ['gorilla', 'chimpanzee', 'orangutan', 'gibbon', 'mandrill', 'capuchin']) {
    const { g, w } = arena(), b = palisade(w), a = ape(g, species), hp = a.hp, h = g.makeHuman(260, 0, null);
    grids(g); g.command('nearestHuman', { x: 1, y: 0 }); let climbed = false;
    for (let frame = 0; frame < 240 && h.hp === h.maxHp; frame++) orderStep(g, actor => { climbed ||= !!actor.palisadeClimb; });
    assert.ok(climbed, species + ' climbs the allied palisade while pursuing infantry');
    assert.ok(h.hp < h.maxHp, species + ' reaches its human target');
    assert.equal(a.hp, hp); assert.equal(b.hp, b.maxHp); assert.ok(a.x > 70);
  }
});

test('an obstructed landing cancels at the crest onto safe ground outside the actual palisade', () => {
  const { g, w, nav } = arena(), b = palisade(w), a = ape(g, 'gorilla', 28);
  move(g, a, { x: 160, y: 0 }, 22); assert.ok(a.palisadeClimb); assert.ok(a.x > 45, 'climber reaches the wall crest');
  addObject(w, { id: 'new-landing-hut', type: 'apeBuilding', x: 84, y: 0, w: 30, h: 150, collision: 'rect' });
  for (let frame = 0; frame < 20 && a.palisadeClimb; frame++) move(g, a, { x: 160, y: 0 }, 1);
  assert.equal(a.palisadeClimb, undefined); assert.equal(nav._palisadeClimbers.has(a), false);
  assert.equal(nav.blocked(a.x, a.y, 10, 'ape'), false, 'cancellation cannot leave the actor inside the wall');
  assert.ok(a.x < b.x - b.w / 2 - 9, 'actor returns to the unobstructed approach');
});

test('nearest-human orders climb long ordinary and fortress walls before attacking for every light species', () => {
  for (const fortress of [false, true]) for (const species of ['gibbon', 'capuchin', 'chimpanzee']) {
    const { g, w, nav } = arena(), a = ape(g, species), b = wall(w, fortress ? { wallTier: 3, climbAccess: true, walkable: true, walkHeight: 80, visualHeight: 80 } : {});
    const h = g.makeHuman(140, 0, null); grids(g); assert.ok(g.command('nearestHuman', { x: 1, y: 0 }));
    let climbed = false;
    for (let frame = 0; frame < 300 && h.hp === h.maxHp; frame++) orderStep(g, actor => { climbed ||= !!(actor.wallClimb || actor.siegeTransition); });
    assert.ok(climbed, species + ' physically climbs ' + (fortress ? 'fortress access' : 'ordinary wall'));
    assert.ok(a.x > b.x + b.w / 2 + 9, species + ' lands past the wall');
    assert.ok(h.hp < h.maxHp, species + ' reaches and attacks the ordered human');
    assert.equal(b.hp, b.maxHp, 'held human-only attack never damages the climbed wall');
    assert.equal(a.wallClimb, undefined); assert.equal(a.siegeTransition, undefined); assert.equal(nav.blocked(a.x, a.y, 10, 'ape'), false);
  }
});

test('a failed ground route and actor failure cache cannot discard a human reachable by climbing', () => {
  const { g, w, nav } = arena(), a = ape(g, 'gibbon', 25), b = wall(w), h = g.makeHuman(120, 0, null);
  grids(g); g.command('nearestHuman', { x: 1, y: 0 });
  const key = [Math.round(a.x / nav.cell), 0, Math.round(h.x / nav.cell), 0, a.radius || 10, nav.revision, 'ape'].join(':');
  nav.routes.set(key, { time: nav.time, path: [] });
  assert.equal(g.nearestTargetFor(a, true)?.id, h.id, 'validated climb overrides a failed ground-only route');
  a._nearestTarget = { id: h.id, kind: 'human' };
  a._nav = { unreachable: { x: h.x, y: h.y, until: g.time + 8, revision: nav.revision } };
  let climbed = false;
  for (let frame = 0; frame < 240 && h.hp === h.maxHp; frame++) orderStep(g, actor => { climbed ||= !!(actor.wallClimb || actor.siegeTransition); });
  assert.ok(climbed); assert.ok(h.hp < h.maxHp); assert.ok(!a._nearestRejected?.[h.id], 'reachable infantry is not rejected'); assert.equal(b.hp, b.maxHp);
});

test('human-only climbing keeps non-climbable high walls intact and never grants a heavy ape a wall climb', () => {
  for (const species of ['gibbon', 'gorilla']) {
    const { g, w } = arena(), a = ape(g, species), b = wall(w, { wallTier: 3, climbable: false }), h = g.makeHuman(140, 0, null);
    grids(g); g.command('nearestHuman', { x: 1, y: 0 }); let climbed = false;
    orderStep(g, actor => { climbed ||= !!(actor.wallClimb || actor.siegeTransition); }, 90);
    assert.equal(climbed, false); assert.equal(b.hp, b.maxHp); assert.equal(h.hp, h.maxHp);
  }
});
