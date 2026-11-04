'use strict';
// Run: node tests/graphics-test.cjs [alternate-project-root]
// Dependency-free recording-Canvas tests; this does not open a browser or
// measure rendered FPS. Actual raster poses are also inspected during release.
const assert = require('node:assert/strict');
const path = require('node:path');
const crypto = require('node:crypto');
const root = process.argv[2] || path.resolve(__dirname, '..');
global.window = global;

function recordingContext() {
  const calls = [], target = { calls, globalAlpha: 1 };
  return new Proxy(target, {
    get(obj, key) {
      if (key in obj) return obj[key];
      if (String(key).startsWith('create') && String(key).endsWith('Gradient')) return () => ({ addColorStop() {} });
      if (key === 'measureText') return value => ({ width: String(value).length * 6 });
      return (...args) => {
        for (const value of args) if (typeof value === 'number') assert(Number.isFinite(value), 'Nonfinite Canvas coordinate in ' + String(key));
        if (key === 'ellipse' || key === 'arc') assert(args[2] >= 0 && (key === 'arc' || args[3] >= 0), 'Negative Canvas radius');
        calls.push([String(key), ...args.map(v => v && typeof v === 'object' && 'tag' in v ? v.tag : v)]);
      };
    },
    set(obj, key, value) { obj[key] = value; if (key !== 'calls') calls.push(['set', key, value]); return true; }
  });
}
function canvas() { const context = recordingContext(); return { width: 1, height: 1, getContext: () => context }; }
global.document = { createElement(name) { assert.equal(name, 'canvas'); return canvas(); } };
require(path.join(root, 'src/data.js'));
require(path.join(root, 'src/graphics.js'));

function renderer(units = [], time = 20) {
  const r = Object.create(BattleRenderer.prototype);
  Object.assign(r, {
    engine: { time, units, phase: 'running', width: 6400, height: 4400, regiments: [] },
    _visibleBodies: [], _bodyBuckets: Array.from({ length: 6 }, () => []), _bodyWarmQueue: [], _bodyBuildBudget: 0, _atlas: new Map(),
    _formationEnvelopes: new Map(), pendingFacing: null, _unitAtlases: [],
    _depthCounts: new Int32Array(0), _depthKeys: new Uint32Array(0), _depthSorted: [],
    _adaptiveLow: false, _qualityChangedAt: 0
  });
  return r;
}
function soldier(type = 'archer', extra = {}) {
  return Object.assign({ id: 0, type, team: 0, x: 100, y: 100, angle: 0, alive: true, routed: false,
    state: 'idle', _lastAttack: 20, _cooldown: GameData.TYPES[type].cooldown, _terrain: {}, target: 1,
    _vx: 0, _vy: 0, anim: 0, attackFlash: .8, hp: 100, maxHp: 100, _deathTime: -1, travelDistance: 0 }, extra);
}
function fakeAtlas(type, team) {
  const sprite = tag => ({ full: { tag, width: 60, height: 80 }, mid: { tag, width: 30, height: 40 }, low: { tag, width: 8, height: 10 }, w: 30, h: 40, ox: -15, oy: -30 });
  return { frames: Array.from({ length: 80 }, (_, i) => sprite(type + ':' + team + ':pose-' + Math.floor(i / 8) + ':dir-' + i % 8)),
    corpses: Array.from({ length: 8 }, (_, i) => sprite(type + ':' + team + ':fallen-' + i)) };
}

// Inspect the actual vector geometry passed to Canvas, including transforms.
// This checks sprite/portrait clipping without a raster library or browser.
function drawingBounds(calls) {
  let matrix = [1, 0, 0, 1, 0, 0], width = 1, current = null, bounds = null;
  const stack = [], include = (box, point) => !box ? { x1: point[0], y1: point[1], x2: point[0], y2: point[1] } :
    { x1: Math.min(box.x1, point[0]), y1: Math.min(box.y1, point[1]), x2: Math.max(box.x2, point[0]), y2: Math.max(box.y2, point[1]) };
  const transform = (x, y) => [matrix[0] * x + matrix[2] * y + matrix[4], matrix[1] * x + matrix[3] * y + matrix[5]];
  const point = (x, y) => { current = include(current, transform(x, y)); };
  const paint = stroke => {
    if (!current) return;
    const pad = stroke ? width * Math.max(Math.hypot(matrix[0], matrix[1]), Math.hypot(matrix[2], matrix[3])) / 2 : 0;
    bounds = include(include(bounds, [current.x1 - pad, current.y1 - pad]), [current.x2 + pad, current.y2 + pad]);
  };
  for (const [method, ...a] of calls) {
    if (method === 'save') stack.push({ matrix: matrix.slice(), width });
    else if (method === 'restore') { const prior = stack.pop(); if (prior) { matrix = prior.matrix; width = prior.width; } }
    else if (method === 'set' && a[0] === 'lineWidth') width = a[1];
    else if (method === 'setTransform') matrix = a.slice();
    else if (method === 'translate') { matrix[4] += matrix[0] * a[0] + matrix[2] * a[1]; matrix[5] += matrix[1] * a[0] + matrix[3] * a[1]; }
    else if (method === 'scale') { matrix[0] *= a[0]; matrix[1] *= a[0]; matrix[2] *= a[1]; matrix[3] *= a[1]; }
    else if (method === 'rotate') {
      const [aa, b, cc, d] = matrix, co = Math.cos(a[0]), si = Math.sin(a[0]);
      matrix[0] = aa * co + cc * si; matrix[1] = b * co + d * si; matrix[2] = cc * co - aa * si; matrix[3] = d * co - b * si;
    } else if (method === 'beginPath') current = null;
    else if (method === 'moveTo' || method === 'lineTo') point(a[0], a[1]);
    else if (method === 'quadraticCurveTo' || method === 'bezierCurveTo') for (let i = 0; i < a.length; i += 2) point(a[i], a[i + 1]);
    else if (method === 'ellipse' || method === 'arc') {
      const rx = a[2], ry = method === 'ellipse' ? a[3] : rx, angle = method === 'ellipse' ? a[4] : 0;
      const co = Math.cos(angle), si = Math.sin(angle), center = transform(a[0], a[1]);
      const dx = Math.hypot((matrix[0] * co + matrix[2] * si) * rx, (-matrix[0] * si + matrix[2] * co) * ry);
      const dy = Math.hypot((matrix[1] * co + matrix[3] * si) * rx, (-matrix[1] * si + matrix[3] * co) * ry);
      current = include(include(current, [center[0] - dx, center[1] - dy]), [center[0] + dx, center[1] + dy]);
    } else if (method === 'fill' || method === 'stroke') paint(method === 'stroke');
  }
  return bounds;
}

function paintedPaths(calls) {
  let fill = '', stroke = '', current = []; const paths = [];
  for (const [method, ...a] of calls) {
    if (method === 'set' && a[0] === 'fillStyle') fill = a[1];
    else if (method === 'set' && a[0] === 'strokeStyle') stroke = a[1];
    else if (method === 'beginPath') current = [];
    else if (['moveTo', 'lineTo', 'ellipse', 'arc', 'quadraticCurveTo'].includes(method)) current.push([method, ...a]);
    else if (method === 'fill' || method === 'stroke') paths.push({ mode: method, color: method === 'fill' ? fill : stroke, segments: current.slice() });
  }
  return paths;
}
const results = [];
function test(name, body) {
  if (process.env.IRONFIELD_GRAPHICS_FILTER && !name.toLowerCase().includes(process.env.IRONFIELD_GRAPHICS_FILTER.toLowerCase())) return;
  try { const detail = body(); results.push({ name, passed: true, ...(detail || {}) }); console.log('PASS ' + name); }
  catch (error) { results.push({ name, passed: false, error: error.stack }); console.error('FAIL ' + name + '\n' + error.stack); }
}

test('All twenty-three troop and siege types have six distinct attack poses and finite drawing geometry', () => {
  const r = renderer(), counts = {};
  assert.equal(GameData.TYPE_ORDER.length, 23); assert(GameData.TYPE_ORDER.includes('hoplite'));
  for (const type of GameData.TYPE_ORDER) {
    const poses = [];
    for (let frame = 4; frame < 10; frame++) {
      const c = recordingContext(); r._drawSprite(c, type, 0, 0, frame);
      poses.push(crypto.createHash('sha256').update(JSON.stringify(c.calls)).digest('hex'));
    }
    assert.equal(new Set(poses).size, 6, type + ' reuses indistinguishable attack/reload choreography'); counts[type] = 6;
    for (let direction = 0; direction < 8; direction++) for (const frame of [0, 4, 5, 6, 7, 8, 9]) r._drawSprite(recordingContext(), type, 1, direction, frame);
  }
  return { troopTypes: Object.keys(counts).length, distinctPosesPerType: 6 };
});

test('Hoplites carry a large bronze round shield, crested helmet and long thrusting spear', () => {
  const r = renderer(), poses = new Map();
  for (const frame of [0, 4, 5, 6, 7, 8, 9]) { const c = recordingContext(); r._drawSprite(c, 'hoplite', 0, 0, frame); poses.set(frame, c.calls); }
  const shield = frame => paintedPaths(poses.get(frame)).find(p => p.mode === 'fill' && p.color === '#b78d49' && p.segments.some(q => q[0] === 'ellipse' && q[3] >= 9 && q[4] >= 9));
  const spear = frame => paintedPaths(poses.get(frame)).find(p => p.mode === 'stroke' && p.color === '#6c5131');
  const crest = paintedPaths(poses.get(0)).find(p => p.mode === 'fill' && p.color === '#3f739e' && p.segments.some(q => q[0] === 'lineTo' && q[2] <= -37));
  assert(crest, 'The bronze helmet lost its tall, team-colored crest');
  for (const frame of poses.keys()) {
    assert(shield(frame), 'A hoplite pose lost its round bronze shield: ' + frame);
    const shaft = spear(frame), a = shaft.segments[0], b = shaft.segments[1];
    assert(Math.abs(Math.hypot(b[1] - a[1], b[2] - a[2]) - 72) < 1e-9, 'The hoplite spear changes physical length between poses');
  }
  const readyTip = spear(5).segments[1], thrustTip = spear(6).segments[1];
  assert(thrustTip[1] - readyTip[1] > 25 && Math.abs(thrustTip[2] - spear(6).segments[0][2]) < 5, 'The spear never performs a long, horizontal thrust');
  assert(shield(7).segments[0][1] > shield(5).segments[0][1] + 3, 'Follow-through never brings the shield forward for a press');
  const left = recordingContext(); r._drawSprite(left, 'hoplite', 1, 4, 6);
  assert(left.calls.some(c => c[0] === 'scale' && c[1] === -1 && c[2] === 1), 'A left-facing hoplite thrusts in the wrong direction');
  const fallen = recordingContext(); r._drawFallen(fallen, 'hoplite', 0, 0);
  assert(paintedPaths(fallen.calls).some(p => p.mode === 'fill' && p.color === '#b78d49' && p.segments.some(q => q[0] === 'ellipse' && q[3] >= 10)), 'Fallen hoplites lost their bronze round shield');
  assert(paintedPaths(fallen.calls).some(p => p.mode === 'stroke' && p.color === '#a18b5d' && p.segments.length === 2 && Math.hypot(p.segments[1][1] - p.segments[0][1], p.segments[1][2] - p.segments[0][2]) > 70), 'Fallen hoplites lost their long spear');
  return { shieldDiameter: 20.2, distinctAttackPoses: 6, mirroredFacing: true };
});

test('Hoplite attack and shield-press animation uses simulation events and yields to movement', () => {
  const u = soldier('hoplite'), enemy = soldier('militia', { id: 1, team: 1, x: 130, _radius: 6 }), r = renderer([u, enemy]);
  const frames = [.03, .23, .43, .63].map(age => { r.engine.time = 20 + age; return r._attackFrame(u); });
  assert.deepEqual(frames, [6, 7, 8, 9]);
  r.engine.time = 25; u._lastAttack = -100; u._cooldown = 1; u._lastPush = 25;
  assert.equal(r._attackFrame(u), 7, 'A real shield press never reached its cached pose');
  r.engine.time = 25.31; assert.equal(r._attackFrame(u), 0, 'An expired push leaves a permanent shove pose');
  r.engine.time = 25.1;
  for (const state of ['turning', 'fallBack', 'move']) {
    u.state = state; u._vx = 30; assert.equal(r._attackFrame(u), 0, state + ' was overridden by a recent shove');
  }
  u.state = 'idle'; u._vx = 0; u._lastPush = 26;
  assert.equal(r._attackFrame(u), 0, 'A future push timestamp produced a pretend action');
  u._lastPush = -100; u._cooldown = .27; assert.equal(r._attackFrame(u), 4);
  u._cooldown = .1; assert.equal(r._attackFrame(u), 5);
  enemy.x = 600; assert.equal(r._attackFrame(u), 0, 'An unreachable foe produced a hoplite wind-up');
  return { postStrikeFrames: frames, pushSeconds: .30, movementPrecedence: true };
});

test('Hoplite atlas and portraits contain the full crest, shield and spear within bounded cached sources', () => {
  const r = renderer(); let rasterizations = 0;
  r._cropSprite = function(scratch, ctx, cw, anchorY, scale) {
    const lastClear = ctx.calls.findLastIndex(call => call[0] === 'clearRect');
    const b = drawingBounds(ctx.calls.slice(lastClear + 1));
    assert(b && b.x1 >= 0 && b.y1 >= 0 && b.x2 <= scratch.width && b.y2 <= scratch.height, 'Hoplite equipment exceeds its sprite source canvas');
    const x1 = Math.floor(b.x1) - 2, y1 = Math.floor(b.y1) - 2, x2 = Math.ceil(b.x2) + 2, y2 = Math.ceil(b.y2) + 2;
    rasterizations++;
    const tag = 'hoplite-cache-' + rasterizations;
    return { full: { tag }, mid: { tag }, low: { tag }, w: (x2 - x1) / scale, h: (y2 - y1) / scale, ox: x1 / scale - cw / 2, oy: y1 / scale - anchorY };
  };
  for (const team of [0, 1]) {
    const atlas = r._getAtlas('hoplite', team);
    assert.equal(atlas.frames.length, 80); assert.equal(new Set(atlas.frames).size, 36);
    for (const size of [32, 80]) {
      const target = canvas(); target.width = target.height = size;
      r.drawUnitIcon(target, 'hoplite', team);
      const draws = target.getContext().calls.filter(call => call[0] === 'drawImage');
      assert.equal(draws.length, 1, 'A hoplite portrait created an extra composite draw');
      const [, , x, y, w, h] = draws[0];
      assert(x >= 1 && y >= 1 && x + w <= size - 1 && y + h <= size - 1, 'Hoplite portrait clipped its actual cached equipment bounds');
    }
  }
  assert.equal(rasterizations, 72, 'Hoplitic living frames stopped sharing duplicate facings/gaits');
  r._bodyBuildBudget = 2;
  assert(r._corpseSprite('hoplite', 0, 0)); assert(r._corpseSprite('hoplite', 1, 0));
  assert.equal(r._bodyBuildBudget, 0); assert.equal(rasterizations, 74);
  r._corpseSprite('hoplite', 0, 3); assert.equal(rasterizations, 74, 'A hoplite corpse bypassed the two-source raster budget');
  for (const team of [0, 1]) for (let dir = 0; dir < 8; dir++) { r._bodyBuildBudget = 1; assert(r._corpseSprite('hoplite', team, dir)); }
  assert.equal(rasterizations, 88, 'The hoplite body cache grew beyond eight facings per team');
  return { livingSources: 72, maximumFallenSources: 16, portraitSizes: [32, 80], newBodySourcesPerFrame: 2 };
});

test('Attack timeline follows actual shot timestamps, with pre-shot drawing only for a reachable target', () => {
  const u = soldier(), enemy = soldier('militia', { id: 1, team: 1, x: 180, _radius: 6 }), r = renderer([u, enemy]);
  const frames = [0.03, .20, .45, .76].map(age => { r.engine.time = 20 + age; return r._attackFrame(u); });
  assert.deepEqual(frames, [6, 7, 8, 9], 'Release, follow-through, nock and ready phases did not advance in order');
  r.engine.time = 26; u._cooldown = .35; assert.equal(r._attackFrame(u), 4);
  u._cooldown = .10; assert.equal(r._attackFrame(u), 5);
  enemy.x = 6000; assert.equal(r._attackFrame(u), 0, 'An unreachable target triggered a pretend wind-up');
  enemy.x = 180; enemy.alive = false; assert.equal(r._attackFrame(u), 0, 'A defeated target triggered a pretend wind-up');
  return { postShotFrames: frames, reachableWindupFrames: [4, 5] };
});

test('Crossbows show sustained cranking and loading while javelin retrieval finishes earlier', () => {
  const crossbow = soldier('crossbow'), javelin = soldier('javelin'), r = renderer([crossbow]);
  r.engine.time = 20 + GameData.TYPES.crossbow.cooldown * .40;
  assert.equal(r._attackFrame(crossbow), 8, 'Crossbow reload omitted its winding pose');
  r.engine.time = 20 + GameData.TYPES.crossbow.cooldown * .65;
  assert.equal(r._attackFrame(crossbow), 9, 'Crossbow reload omitted its bolt-loading pose');
  r.engine.time = 21.05;
  assert.equal(r._attackFrame(javelin), 0, 'Javelin retrieval incorrectly lasts as long as a crossbow reload');
  assert(r._attackFrame(crossbow) >= 8, 'Crossbow has no extended visible reload');
  return { crossbowCooldown: GameData.TYPES.crossbow.cooldown, javelinRetrievalCompleteBy: 1.05 };
});

test('Turning, withdrawing and movement keep their animation precedence', () => {
  const u = soldier('swordsman'), r = renderer([u], 20.03);
  for (const state of ['turning', 'fallBack']) { u.state = state; assert.equal(r._attackFrame(u), 0, state + ' inherited a stale strike'); }
  u.state = 'move'; u._vx = 50; assert.equal(r._attackFrame(u), 6, 'A real incidental strike lost its brief release');
  r.engine.time = 20.5; assert.equal(r._attackFrame(u), 0, 'Reload imagery suppressed an actual marching gait');
  u.state = 'idle'; u.routed = true; r.engine.time = 20.03; assert.equal(r._attackFrame(u), 0);
  u.routed = false; u.alive = false; assert.equal(r._attackFrame(u), 0);
});

test('Fallen infantry, horses, elephants and siege engines retain distinct silhouettes', () => {
  const r = renderer(), shapes = new Set();
  for (const type of GameData.TYPE_ORDER) {
    const c = recordingContext(); r._drawFallen(c, type, 0, 0);
    shapes.add(crypto.createHash('sha256').update(JSON.stringify(c.calls)).digest('hex'));
    for (let dir = 0; dir < 8; dir++) r._drawFallen(recordingContext(), type, 1, dir);
  }
  assert.equal(shapes.size, GameData.TYPE_ORDER.length, 'Different fallen classes collapsed to the same generic marker');
  return { distinctFallenTypes: shapes.size, orientationsPerType: 8 };
});

test('Bodies fade on simulation time and disappear after twenty-two seconds', () => {
  const u = soldier('javelin', { alive: false, _deathTime: 5 }), r = renderer([u], 5);
  const alpha = age => { r.engine.time = 5 + age; return r._corpseOpacity(u); };
  assert.equal(alpha(0), .86); assert.equal(alpha(14), .86);
  assert(Math.abs(alpha(18) - .43) < 1e-9); assert(alpha(21.9) > 0);
  assert.equal(alpha(22), 0); assert.equal(alpha(80), 0);
  r.engine.time = 4; assert.equal(r._corpseOpacity(u), 0);
  r.engine.time = 8; u.routed = true; assert.equal(r._corpseOpacity(u), 0); u.routed = false;
  u.alive = true; assert.equal(r._corpseOpacity(u), 0);
  return { solidSeconds: 14, goneAtSeconds: 22 };
});

test('Corpse rendering culls the viewport, prioritizes recent falls and caps draw work', () => {
  const units = [];
  for (let i = 0; i < 3000; i++) units.push(soldier(i % 2 ? 'javelin' : 'archer', { id: i, team: i % 2, alive: false, _deathTime: i < 1500 ? 9 : 1, x: 100 + i % 30, y: 100 + i % 50 }));
  units.push(soldier('elephant', { id: 3000, alive: false, _deathTime: 9, x: 9000 }));
  units.push(soldier('catapult', { id: 3001, alive: false, _deathTime: 0, x: 100 }));
  const r = renderer(units, 10); r._getAtlas = fakeAtlas;
  const bounds = { x1: 0, x2: 300, y1: 0, y2: 300 }, c = recordingContext();
  assert.equal(r._drawBodies(c, bounds, false, 'mid'), 900);
  assert.equal(c.calls.filter(x => x[0] === 'drawImage').length, 900);
  assert(r._visibleBodies.every(u => u._deathTime === 9 && u.x <= 300), 'Old or off-screen bodies displaced recent visible casualties');
  assert(r._visibleBodies.some(u => u.team === 0) && r._visibleBodies.some(u => u.team === 1), 'Body cap discarded an entire army');
  assert.equal(r._drawBodies(recordingContext(), bounds, true, 'low'), 320);
  r.engine.time = 35; assert.equal(r._drawBodies(recordingContext(), bounds, false, 'full'), 0);
  const lazy = renderer(), built = [];
  lazy._getAtlas = function(type, team) {
    const key = type + ':' + team;
    if (!this._atlas.has(key)) this._atlas.set(key, { cw: 100, ch: 100, scale: 2, corpses: new Array(8), unique: new Map() });
    return this._atlas.get(key);
  };
  lazy._drawFallen = (ctx, type, team, dir) => built.push(type + ':' + team + ':' + dir);
  lazy._cropSprite = () => fakeAtlas('cached', 0).corpses[0];
  lazy._bodyBuildBudget = 2;
  const first = lazy._corpseSprite('javelin', 0, 0); assert(first);
  assert(lazy._corpseSprite('crossbow', 1, 0));
  assert.equal(lazy._corpseSprite('archer', 0, 0), null, 'Unbounded new class rasterization occurred in one frame');
  assert.equal(lazy._corpseSprite('javelin', 0, 3), first, 'Unbuilt facing did not reuse its own cached troop silhouette');
  assert.equal(built.length, 2);
  lazy._bodyBuildBudget = 2;
  assert(lazy._corpseSprite('archer', 0, 0), 'Deferred body could not be completed next frame');
  assert(lazy._corpseSprite('javelin', 0, 3)); assert.equal(built.length, 4);
  lazy._corpseSprite('javelin', 0, 3); assert.equal(built.length, 4, 'A cached corpse was rasterized again');
  return { fullQualityDrawCap: 900, lowQualityDrawCap: 320, eligibleBodyFixture: units.length, newBodyVariantBudgetPerRender: 2 };
});

test('Paused render frames keep attack and corpse state fixed despite wall-clock changes', () => {
  const shooter = soldier('crossbow', { _lastAttack: 9.8 }), dead = soldier('pikeman', { id: 1, x: 120, alive: false, _deathTime: 6 });
  const r = renderer([shooter, dead], 10), c = recordingContext();
  Object.assign(r, { ctx: c, _terrain: { tag: 'terrain' }, _visible: [], _lastFrame: 0, _frame: 0, fps: 60,
    dpr: 1, width: 400, height: 300, camera: { x: 100, y: 100, zoom: 1 },
    options: { quality: 'high', showBodies: true, showOrders: false, cinematic: false, showHealth: false } });
  r.engine.phase = 'paused'; r._getAtlas = fakeAtlas;
  for (const name of ['_drawWaterMotion', '_drawProjectiles', '_drawEffects', '_drawStandards']) r[name] = () => {};
  const before = JSON.stringify(r.engine);
  r.render(1000, new Set()); const first = c.calls.filter(x => x[0] === 'drawImage').map(x => x[1]); c.calls.length = 0;
  r.render(2001, new Set()); const second = c.calls.filter(x => x[0] === 'drawImage').map(x => x[1]);
  assert.deepEqual(second, first, 'Paused battle animation advanced with real time');
  assert.equal(JSON.stringify(r.engine), before, 'Rendering mutated simulation state');
  assert.equal(r._renderStats.bodies, 1); assert(first.some(x => /fallen/.test(x))); assert(first.some(x => /pose-7/.test(x)));
  return { frozenDrawDispatch: first };
});

test('Blank custom maps omit preset roads and camps while retaining authored terrain', () => {
  const r = renderer(); Object.assign(r, { _terrain: null, _terrainScale: .01,
    theme: { ground: '#456', patch: ['#456'], grass: '#789' },
    map: { id: 'custom', seed: 1, regions: [{ type: 'forest', x: 500, y: 500, w: 200, h: 200 }] } });
  const counts = { roads: 0, camps: 0, forest: 0 };
  r._drawRoads = () => counts.roads++; r._paintCamp = () => counts.camps++; r._paintForest = () => counts.forest++;
  r._buildTerrain(); assert.deepEqual(counts, { roads: 0, camps: 0, forest: 1 });
  r.map.id = 'meadow'; r._buildTerrain(); assert.deepEqual(counts, { roads: 1, camps: 2, forest: 2 });
});

test('Selected living formations show their current facing with a short orientation arrow', () => {
  const r = renderer(), c = recordingContext();
  r.engine.regiments = [
    { id: 0, team: 0, alive: 40, x: 100, y: 100, facing: Math.PI / 2, effectiveDepth: 80 },
    { id: 1, team: 1, alive: 40, x: 300, y: 100, facing: Math.PI, effectiveDepth: 80 },
    { id: 2, team: 0, alive: 0, x: 500, y: 100, facing: 0, effectiveDepth: 80 }
  ];
  const before = JSON.stringify(r.engine);
  r._drawFacingIndicators(c, new Set([0, 2]), 1);
  assert.deepEqual(c.calls.filter(call => call[0] === 'rotate').map(call => call[1]), [Math.PI / 2]);
  assert.deepEqual(c.calls.filter(call => call[0] === 'translate').map(call => call.slice(1)), [[100, 100]]);
  assert(c.calls.some(call => call[0] === 'lineTo' && call[1] === 66 && call[2] === 0), 'Facing arrow has no short forward shaft');
  assert(!c.calls.some(call => call[0] === 'ellipse' || call[0] === 'fillText'), 'An idle orientation arrow acquired a destination marker or order label');
  assert.equal(JSON.stringify(r.engine), before);
});

test('Face preview uses the pointer direction without drawing a march destination', () => {
  const r = renderer();
  r.engine.regiments = [{ id: 0, team: 0, alive: 40, x: 100, y: 100, facing: Math.PI / 2, effectiveDepth: 80 }];
  const before = JSON.stringify(r.engine), selected = new Set([0]);
  r.pendingFacing = { x: 5000, y: 100 };
  const near = recordingContext(); r._drawFacingIndicators(near, selected, 1);
  r.pendingFacing = { x: 50000, y: 100 };
  const far = recordingContext(); r._drawFacingIndicators(far, selected, 1);
  assert.deepEqual(far.calls, near.calls, 'Pointer distance changed the face marker into a route');
  assert.deepEqual(near.calls.filter(call => call[0] === 'rotate').map(call => call[1]), [Math.PI / 2, 0]);
  assert(near.calls.some(call => call[0] === 'fillText' && call[1] === 'FACE'));
  assert(near.calls.some(call => call[0] === 'arc' && call[1] === 100 && call[2] === 100), 'Turning arc is not anchored to the formation');
  assert(!near.calls.some(call => call[0] === 'ellipse'), 'Facing preview drew a destination ring');
  r.pendingFacing = null;
  const cleared = recordingContext(); r._drawFacingIndicators(cleared, selected, 1);
  assert.equal(cleared.calls.filter(call => call[0] === 'rotate').length, 1, 'Canceled preview left its target arrow behind');
  assert.equal(JSON.stringify(r.engine), before, 'A face preview mutated the simulation');
});

test('Face orders report an in-place turn distinctly from retreat and settle into one held arrow', () => {
  const r = renderer(), group = { id: 0, team: 0, type: 'swordsman', alive: 40, unitIds: [], x: 100, y: 100,
    facing: 0, commandFacing: -Math.PI / 2, order: 'face', commandStatus: 'facing', commandX: 5000, commandY: 4000,
    width: 100, depth: 80, effectiveWidth: 100, effectiveDepth: 80, formation: 'line', cohesion: 100, morale: 100, status: 'steady' };
  Object.assign(r, { options: { showLabels: true, showOrders: true, cinematic: false }, camera: { x: 100, y: 100, zoom: 1 }, width: 400, height: 300, _now: 0 });
  r.engine.phase = 'paused'; r.engine.regiments = [group];
  const selected = new Set([0]), order = recordingContext(); r._drawOrders(order, selected, 1);
  assert(!order.calls.some(call => call[0] === 'ellipse'), 'A face order used the travel destination ring');
  assert(!order.calls.some(call => ['moveTo', 'lineTo'].includes(call[0]) && call[1] === 5000 && call[2] === 4000), 'A face order used an old march destination');
  const active = recordingContext(); r._drawFacingIndicators(active, selected, 1); r._drawStandards(active, selected, 1, false);
  const activeLabels = active.calls.filter(call => call[0] === 'fillText').map(call => call[1]);
  assert(activeLabels.includes('TURNING IN PLACE')); assert(activeLabels.includes('TURN IN PLACE'));
  assert(!activeLabels.some(label => /WITHDRAW/.test(label)), 'An orientation order was labeled as retreat');
  group.facing = group.commandFacing; group.order = 'hold'; group.commandStatus = 'holding';
  const settled = recordingContext(); r._drawFacingIndicators(settled, selected, 1);
  assert.deepEqual(settled.calls.filter(call => call[0] === 'rotate').map(call => call[1]), [-Math.PI / 2]);
  assert(!settled.calls.some(call => call[0] === 'fillText'), 'Completed facing kept an active-turn label');
  group.order = 'retreat'; group.fallingBack = true; group.commandStatus = 'turning';
  const retreat = recordingContext(); r._drawStandards(retreat, selected, 1, false);
  assert(retreat.calls.some(call => call[0] === 'fillText' && call[1] === 'TURNING TO WITHDRAW'), 'Retreat lost its distinct turn feedback');
});

test('Low-zoom elephants retain genuine strike frames without increasing sprite draw work', () => {
  const elephant = soldier('elephant'), foot = soldier('swordsman', { id: 1, x: 140 }), r = renderer([elephant, foot], 20.03), c = recordingContext();
  Object.assign(r, { ctx: c, _terrain: { tag: 'terrain' }, _visible: [], _lastFrame: 0, _frame: 0, fps: 60,
    dpr: 1, width: 400, height: 300, camera: { x: 100, y: 100, zoom: .15 },
    options: { quality: 'low', showBodies: false, showOrders: false, cinematic: false, showHealth: false } });
  r.engine.phase = 'paused'; r._getAtlas = fakeAtlas;
  for (const name of ['_drawWaterMotion', '_drawProjectiles', '_drawEffects', '_drawStandards']) r[name] = () => {};
  const dispatch = time => {
    r.engine.time = time; c.calls.length = 0; r.render(1000 + time * 100, new Set());
    const sprites = c.calls.filter(call => call[0] === 'drawImage').map(call => call[1]);
    assert.equal(sprites.length, 3); assert.equal(r._renderStats.drawCalls, 2);
    assert(sprites.includes('swordsman:0:pose-0:dir-0'), 'Other troops lost the existing low-detail policy');
    return sprites.find(tag => /^elephant:/.test(tag));
  };
  assert.equal(dispatch(20.03), 'elephant:0:pose-6:dir-0');
  assert.equal(dispatch(20.20), 'elephant:0:pose-7:dir-0');
  assert.equal(dispatch(20.50), 'elephant:0:pose-0:dir-0');
  elephant._lastAttack = -1000; elephant.state = 'move'; elephant._vx = 50;
  assert.equal(dispatch(21), 'elephant:0:pose-0:dir-0', 'Marching was mistaken for a new elephant strike');
  return { overviewZoom: .15, realStrikeFrames: [6, 7], spriteDrawsPerFrame: 2 };
});

test('Hoplite strikes and successful pushes retain one cached sprite at overview zoom', () => {
  const hoplite = soldier('hoplite', { angle: Math.PI }), foot = soldier('swordsman', { id: 1, x: 140 }), r = renderer([hoplite, foot], 20.03), c = recordingContext();
  Object.assign(r, { ctx: c, _terrain: { tag: 'terrain' }, _visible: [], _lastFrame: 0, _frame: 0, fps: 60,
    dpr: 1, width: 400, height: 300, camera: { x: 100, y: 100, zoom: .15 },
    options: { quality: 'low', showBodies: false, showOrders: false, cinematic: false, showHealth: false } });
  r.engine.phase = 'paused'; r._getAtlas = fakeAtlas;
  for (const name of ['_drawWaterMotion', '_drawProjectiles', '_drawEffects', '_drawStandards']) r[name] = () => {};
  const dispatch = (time, wallTime = 1000) => {
    r.engine.time = time; c.calls.length = 0; r.render(wallTime, new Set());
    const sprites = c.calls.filter(call => call[0] === 'drawImage').map(call => call[1]);
    assert.equal(sprites.length, 3); assert.equal(r._renderStats.drawCalls, 2);
    assert(sprites.includes('swordsman:0:pose-0:dir-0'), 'Legacy low-detail dispatch changed');
    return sprites.find(tag => /^hoplite:/.test(tag));
  };
  assert.equal(dispatch(20.03), 'hoplite:0:pose-6:dir-4');
  assert.equal(dispatch(20.23), 'hoplite:0:pose-7:dir-4');
  assert.equal(dispatch(20.43), 'hoplite:0:pose-0:dir-4');
  hoplite._lastAttack = -1000; hoplite._cooldown = 1; hoplite._lastPush = 22;
  assert.equal(dispatch(22.1), 'hoplite:0:pose-7:dir-4');
  assert.equal(dispatch(22.1, 3000), 'hoplite:0:pose-7:dir-4', 'Wall-clock time advanced a paused shield press');
  assert.equal(dispatch(22.31), 'hoplite:0:pose-0:dir-4');
  hoplite.state = 'turning'; assert.equal(dispatch(22.1), 'hoplite:0:pose-0:dir-4');
  hoplite.state = 'move'; hoplite._vx = 30; assert.equal(dispatch(22.1), 'hoplite:0:pose-0:dir-4');
  return { overviewZoom: .15, retainedFrames: [6, 7], spriteDrawsPerFrame: 2, pauseStable: true };
});

test('Shield-press banners require physical push counts and preserve command status priority', () => {
  const r = renderer(), group = { id: 0, team: 0, type: 'hoplite', alive: 40, unitIds: [], x: 100, y: 100,
    facing: 0, order: 'auto', commandStatus: 'advancing', width: 100, depth: 80, effectiveWidth: 100, effectiveDepth: 80,
    formation: 'line', cohesion: 100, morale: 100, status: 'steady', pushingSoldiers: 0 };
  Object.assign(r, { options: { showLabels: true, showOrders: true, cinematic: false }, camera: { x: 100, y: 100, zoom: 1 }, width: 400, height: 300, _now: 0 });
  r.engine.regiments = [group];
  const labels = () => { const c = recordingContext(); r._drawStandards(c, new Set([0]), 1, false); return c.calls.filter(q => q[0] === 'fillText').map(q => q[1]); };
  assert(!labels().includes('SHIELD PRESS'), 'A steady hoplite formation claimed displacement before any push');
  group.pushingSoldiers = 2; assert(labels().includes('SHIELD PRESS'));
  group.commandStatus = 'facing'; assert(labels().includes('TURNING IN PLACE')); assert(!labels().includes('SHIELD PRESS'));
  group.commandStatus = 'turning'; assert(labels().includes('TURNING TO WITHDRAW')); assert(!labels().includes('SHIELD PRESS'));
  group.commandStatus = 'advancing'; group.pushingSoldiers = 0; assert(!labels().includes('SHIELD PRESS'));
});

test('Overview depth buckets draw every visible soldier once without mutating the battle', () => {
  const units = Array.from({ length: 3000 }, (_, id) => soldier(id % 2 ? 'cavalry' : 'swordsman', {
    id, team: id % 2, x: 100 + id * 73 % 6000, y: 100 + id * 337 % 4200 + (id % 3) * .2,
    state: 'idle', attackFlash: 0, _lastAttack: -100
  }));
  const r = renderer(units, 20), c = recordingContext();
  const atlases = [fakeAtlas('swordsman', 0), fakeAtlas('cavalry', 1)];
  Object.assign(r, { ctx: c, _terrain: { tag: 'terrain' }, _visible: [], _lastFrame: 0, _frame: 0, fps: 60,
    dpr: 1, width: 900, height: 700, camera: { x: 3200, y: 2200, zoom: .11 },
    options: { quality: 'low', showBodies: false, showOrders: false, cinematic: false, showHealth: false },
    _unitAtlases: units.map(u => atlases[u.team]) });
  let lookups = 0; r._getAtlas = () => { lookups++; throw new Error('An already cached living atlas was looked up again'); };
  for (const name of ['_drawWaterMotion', '_drawProjectiles', '_drawEffects', '_drawStandards', '_drawFacingIndicators']) r[name] = () => {};
  const before = JSON.stringify(r.engine);
  // The tall siege-tower silhouette extends the viewport safety margin to 180.
  // Keep the exact screen-row ordering assertion at that same bucket origin.
  const y1 = r.camera.y - r.height / r.camera.zoom / 2 - 180;
  const dispatch = () => c.calls.filter(call => call[0] === 'drawImage' && call[1] !== 'terrain');
  r.render(1, new Set());
  let calls = dispatch();
  assert.equal(calls.length, units.length);
  assert.equal(new Set(calls.map(call => call[2] + ':' + call[3])).size, units.length, 'An overview soldier was duplicated or omitted');
  const rows = calls.map(call => Math.floor((call[3] + 30 - y1) * r.camera.zoom));
  assert(rows.every((row, i) => !i || row >= rows[i - 1]), 'Overview render depth moved backwards across a screen pixel row');
  const first = calls.map(call => call.slice(1)); c.calls.length = 0;
  r.render(2, new Set());
  assert.deepEqual(dispatch().map(call => call.slice(1)), first, 'Reused depth buffers changed a stationary frame');
  r.options.quality = 'high'; c.calls.length = 0; r.render(3, new Set());
  calls = dispatch();
  assert.equal(calls.length, units.length);
  assert(calls.every((call, i) => !i || call[3] >= calls[i - 1][3]), 'High detail lost exact vertical depth sorting');
  assert.equal(lookups, 0); assert.equal(JSON.stringify(r.engine), before, 'Rendering changed soldier order or physical state');
  return { visibleSoldiers: units.length, overviewDraws: first.length, cachedAtlasLookups: lookups };
});

test('Selected automatic plans show their remaining route while manual orders suppress the old plan', () => {
  const r = renderer(), c = recordingContext();
  const group = { id: 0, alive: 40, team: 0, type: 'cavalry', unitIds: [], x: 500, y: 500, width: 140, depth: 100,
    effectiveWidth: 140, effectiveDepth: 100, facing: 0, footprintFacing: 0, order: 'auto', formation: 'line',
    morale: 100, cohesion: 100, _rallyTime: 0, commandStatus: 'automatic',
    _tactic: { transit: true, charge: false, waypointIndex: 1, phaseName: 'Passing the enemy wing',
      waypoints: [{ x: 600, y: 700 }, { x: 1300, y: 700 }, { x: 1300, y: 1000 }], goalX: 1300, goalY: 700 } };
  r.engine.regiments = [group]; const before = JSON.stringify(r.engine);
  r._drawTacticRoute(c, group, 1);
  assert.deepEqual(c.calls.filter(q => q[0] === 'lineTo').map(q => q.slice(1)), [[1300, 700], [1300, 1000]]);
  assert(c.calls.some(q => q[0] === 'fillText' && q[1] === 'Passing the enemy wing'));
  assert.equal(JSON.stringify(r.engine), before, 'Drawing the route consumed its next waypoint');
  let routeCalls = 0; r._drawTacticRoute = () => routeCalls++;
  r._drawOrders(recordingContext(), new Set([0]), 1); assert.equal(routeCalls, 1);
  group.order = 'move'; group.targetX = 100; group.targetY = 500;
  r._drawOrders(recordingContext(), new Set([0]), 1); assert.equal(routeCalls, 1, 'Manual Move displayed the obsolete outer route');
  group.order = 'auto'; group.fallingBack = true;
  r._drawOrders(recordingContext(), new Set([0]), 1); assert.equal(routeCalls, 1, 'Retreat displayed the obsolete outer route');
  return { remainingWaypoints: 2, manualOrdersHideAutomaticRoute: true };
});

test('Airborne impacts draw one lifted body above a grounded shadow and keep the same flight through death', () => {
  const u = soldier('militia', { _lastAttack: -1000, _impactStart: 20, _impactDuration: .7, _impactUntil: 20.7,
    _impactLift: 36, _impactNX: 1, _impactNY: 0, state: 'knocked' }), r = renderer([u], 20.35);
  const atlas = fakeAtlas(u.type, u.team);
  atlas.airborne = atlas.frames.slice(0, 8).map((s, i) => ({ ...s,
    full: { tag: 'airborne-' + i, width: 60, height: 80 }, mid: { tag: 'airborne-' + i, width: 30, height: 40 }, low: { tag: 'airborne-' + i, width: 8, height: 10 } }));
  r._getAtlas = () => atlas; r._unitAtlases = [atlas];
  const pose = r._impactPose(u), before = JSON.stringify(r.engine), c = recordingContext();
  assert(Math.abs(pose.lift - 36) < 1e-8 && pose.tilt > .5);
  assert(r._drawImpactUnit(c, u, atlas, 0, 'mid', 0));
  assert.deepEqual(c.calls.find(q => q[0] === 'ellipse').slice(1, 3), [u.x, u.y], 'The impact shadow rose with the body');
  assert(Math.abs(c.calls.find(q => q[0] === 'translate')[2] - (u.y - 36)) < 1e-8, 'The cached sprite did not rise above its physical position');
  assert.equal(c.calls.filter(q => q[0] === 'drawImage').length, 1);
  assert.equal(JSON.stringify(r.engine), before, 'Airborne rendering changed the physical simulation');
  const bounds = { x1: 0, x2: 300, y1: 0, y2: 300 };
  u.alive = false; u._deathTime = 20.2;
  const dead = recordingContext(); assert.equal(r._drawBodies(dead, bounds, false, 'mid'), 1);
  assert.equal(dead.calls.find(q => q[0] === 'drawImage')[1], 'airborne-0', 'A fatal impact jumped directly to a grounded corpse');
  const deadTranslation = dead.calls.find(q => q[0] === 'translate');
  assert.deepEqual(deadTranslation, c.calls.find(q => q[0] === 'translate'), 'Death restarted or interrupted the in-progress flight');
  r.engine.time = 20.69; assert(r._impactPose(u).lift > 0 && r._impactPose(u).lift < 2);
  r.engine.time = 20.71; assert.equal(r._impactPose(u), null);
  const landed = recordingContext(); assert.equal(r._drawBodies(landed, bounds, false, 'mid'), 1);
  assert.match(landed.calls.find(q => q[0] === 'drawImage')[1], /fallen/);
  assert(landed.calls.some(q => q[0] === 'translate' && q[1] === u.x && q[2] === u.y), 'A landed body did not settle at its collision-checked ground position');
  r.engine.time = 19.9; assert.equal(r._impactPose(u), null, 'A future timestamp produced a flying soldier');
  return { peakLift: 36, airborneDraws: 1, shadowRemainsGrounded: true, deathPreservesFlight: true, landingAt: 20.7 };
});

test('Airborne sprite caching shares the bounded two-source budget and fallback excludes its baked ground shadow', () => {
  const r = renderer(), atlases = new Map(); let builds = 0;
  r._cropSprite = () => { builds++; return fakeAtlas('airborne-built', 0).frames[0]; };
  r._getAtlas = (type, team) => {
    const key = type + ':' + team;
    if (!atlases.has(key)) atlases.set(key, { ...fakeAtlas(type, team), cw: 144, ch: 100, scale: 2, anchorY: 72, unique: new Map(), airborne: new Array(8) });
    return atlases.get(key);
  };
  const units = ['militia', 'archer', 'javelin'].map((type, id) => soldier(type, { id, _impactStart: 19.7, _impactDuration: .7,
    _impactLift: 24, _impactNX: 1, _impactUntil: 20.4 }));
  r._bodyBuildBudget = 2;
  for (const u of units) assert(r._drawImpactUnit(recordingContext(), u, r._getAtlas(u.type, u.team), 0, 'mid', 0));
  assert.equal(builds, 2); assert.equal(r._bodyBuildBudget, 0, 'Flying sprites bypassed the shared raster budget');
  const fallback = recordingContext(), third = units[2];
  r._drawImpactUnit(fallback, third, r._getAtlas(third.type, third.team), 0, 'mid', 0);
  const draw = fallback.calls.find(q => q[0] === 'drawImage');
  assert.equal(draw.length, 10, 'An uncached airborne source used the entire baked idle shadow');
  assert(draw[5] < 40 && draw[9] < 40, 'Fallback source still included its ground strip');
  r._bodyBuildBudget = 2;
  r._drawImpactUnit(recordingContext(), third, r._getAtlas(third.type, third.team), 0, 'mid', 0);
  assert.equal(builds, 3); assert.equal(r._bodyBuildBudget, 1);
  r._drawImpactUnit(recordingContext(), third, r._getAtlas(third.type, third.team), 0, 'mid', 0);
  assert.equal(builds, 3, 'An already cached airborne pose was rebuilt');
  return { newSourcesPerFrame: 2, completedSources: builds, fallbackCropsShadow: true };
});

test('Siege equipment and impact weapons stay inside cached sources and fit army portraits', () => {
  const r = renderer(), seen = new Map(); let rasterizations = 0;
  r._cropSprite = (scratch, ctx, cw, anchorY, scale) => {
    const lastClear = ctx.calls.findLastIndex(q => q[0] === 'clearRect'), b = drawingBounds(ctx.calls.slice(lastClear + 1));
    assert(b && b.x1 >= 0 && b.y1 >= 0 && b.x2 <= scratch.width && b.y2 <= scratch.height, 'Siege equipment clips its cached source canvas');
    rasterizations++;
    const tag = 'equipment-cache-' + rasterizations, x1 = Math.floor(b.x1) - 2, y1 = Math.floor(b.y1) - 2,
      x2 = Math.ceil(b.x2) + 2, y2 = Math.ceil(b.y2) + 2;
    return { full: { tag }, mid: { tag }, low: { tag }, w: (x2 - x1) / scale, h: (y2 - y1) / scale, ox: x1 / scale - cw / 2, oy: y1 / scale - anchorY };
  };
  for (const type of ['siegeTower', 'batteringRam','musketeer','cannon','trebuchet','berserker']) {
    const ctx = recordingContext(); r._drawSprite(ctx, type, 0, 0, 0); seen.set(type, drawingBounds(ctx.calls));
    for (const team of [0, 1]) {
      const atlas = r._getAtlas(type, team); assert.equal(atlas.frames.length, 80);
      for (const size of [32, 80]) {
        const target = canvas(); target.width = target.height = size; r.drawUnitIcon(target, type, team);
        const draws = target.getContext().calls.filter(q => q[0] === 'drawImage'); assert.equal(draws.length, 1);
        const [, , x, y, w, h] = draws[0];
        assert(x >= 1 && y >= 1 && x + w <= size - 1 && y + h <= size - 1, 'An equipment portrait clips its actual cached silhouette');
      }
      for (let dir = 0; dir < 8; dir++) { r._bodyBuildBudget = 1; assert(r._corpseSprite(type, team, dir)); }
    }
  }
  const tower = seen.get('siegeTower'), ram = seen.get('batteringRam');
  assert(tower.y2 - tower.y1 > (ram.y2 - ram.y1) * 1.8, 'The tall assault tower became another low artillery body');
  assert(ram.x2 - ram.x1 > tower.x2 - tower.x1, 'The ram lost its broad log and roof silhouette');
  const height=type=>seen.get(type).y2-seen.get(type).y1;
  assert(height('trebuchet')>1.5*height('cannon'),'The counterweight frame lost its height above the gun carriage');
  return { silhouettes: seen.size, framesPerAtlas: 80, portraitSizes: [32, 80], rasterizedSourceCount: rasterizations };
});

test('Black powder, counterweight stones and club sweeps follow their actual shot and swing events', () => {
  const r=renderer(),bounds={x1:0,y1:0,x2:900,y2:600};
  const volleys=[
    ['musketeer','musketBall','#e5c88a',1],['cannon','cannonball','#414940',12],
    ['trebuchet','boulder','#868e7f',255]
  ];
  for(const [type,kind,color,peak] of volleys){
    const gun=soldier(type), target=soldier('militia',{id:1,x:300,y:110});
    r.engine.units=[gun,target];
    const cooldown=GameData.TYPES[type].cooldown;
    r.engine.time=20+.42*cooldown; assert.equal(r._attackFrame(gun),8);
    r.engine.time=20+.64*cooldown; assert.equal(r._attackFrame(gun),9);
    r.engine.projectiles=[{type,kind,sx:100,sy:500,tx:500,ty:500,x:300,y:500,z:peak,age:.5,duration:1}];
    const shot=recordingContext();r._drawProjectiles(shot,bounds,1);
    const art=paintedPaths(shot.calls).find(p=>p.color===color);
    assert(art,'Missing distinct projectile art for '+type);
    if(type==='trebuchet')assert(art.segments.some(q=>q[0]==='moveTo'&&q[2]<250),'Boulder stayed on the ground at arc peak');
  }
  const attack={kind:'clubSweep',x:150,y:160,size:36,radius:36,halfAngle:Math.PI/3,angle:Math.PI,age:.14,life:.34};
  r.engine.effects=[attack];const trail=recordingContext();r._drawEffects(trail,bounds,1,false);
  const arc=trail.calls.find(q=>q[0]==='arc');
  assert(arc&&arc[4]>=attack.angle-attack.halfAngle-1e-9&&arc[5]<=attack.angle+attack.halfAngle+1e-9,'Club left a trail outside its damage sector');
  const signatures=new Set();
  for(const kind of ['musketMuzzle','cannonMuzzle','musketHit','cannonImpact','trebuchetImpact']){
    r.engine.effects=[{...attack,kind,age:.03}];
    const c=recordingContext();r._drawEffects(c,bounds,1,false);
    signatures.add(JSON.stringify(c.calls));
  }
  assert.equal(signatures.size,5,'Distinct discharges and impacts produced the same visual');
  return { kinds:volleys.map(row=>row[1]), distinctDischargesAndImpacts:signatures.size, confinedSweep:true };
});

test('Open castle gates expose the doorway and corner towers rise above the walls', () => {
  const r=renderer(),bounds={x1:0,y1:0,x2:800,y2:800};
  const gate={type:'gate',castleGate:true,x:300,y:100,w:80,h:180,team:0,hp:1000,maxHp:1000,passages:[]};
  r.engine.structures=[gate];
  const intact=recordingContext(); r._drawFortifications(intact,bounds,1);
  assert(intact.calls.some(q=>q[0]==='fillRect'&&q[1]===gate.x&&q[2]===gate.y-29&&q[3]===gate.w&&q[4]===gate.h));
  gate.open=true;
  const open=recordingContext(); r._drawFortifications(open,bounds,1);
  assert(!open.calls.some(q=>q[0]==='fillRect'&&q[1]===gate.x&&q[2]<gate.y&&q[3]===gate.w&&q[4]===gate.h));
  assert(open.calls.some(q=>q[0]==='fillRect'&&q[2]===gate.y&&q[4]===gate.h));
  r.engine.structures=[{...gate,type:'wall',open:false,castleTower:true,h:80}];
  const tower=recordingContext(); r._drawFortifications(tower,bounds,1);
  assert(tower.calls.some(q=>q[0]==='fillRect'&&q[1]===gate.x&&q[2]<gate.y-38&&q[3]===80&&q[4]===80));
  return { retractedGate:true,raisedTower:true };
});

test('Fortification damage, breaches and ramps alter the drawn geometry without covering the passage', () => {
  const r = renderer(), bounds = { x1: 0, x2: 1000, y1: 0, y2: 1000 };
  const wall = { id: 'wall', type: 'wall', team: 1, x: 300, y: 100, w: 80, h: 500, hp: 2200, maxHp: 2200, passages: [], breached: false };
  r.engine.structures = [wall];
  const full = recordingContext(); r._drawFortifications(full, bounds, 1);
  wall.hp = 900; const damaged = recordingContext(); r._drawFortifications(damaged, bounds, 1);
  assert.notDeepEqual(damaged.calls, full.calls, 'Damaged masonry had no visible state change');
  const ramp = { id: 'ramp-0', structureId: wall.id, towerId: 0, team: 0, x: 140, y: 270, w: 400, h: 144, axis: 'x', dir: 1, height: 38 };
  wall.passages = [{ x: 300, y: 270, w: 80, h: 144 }];
  r.engine.siege = { ramps: [ramp], heightAt(x, y) { return x >= 140 && x <= 540 && y >= 270 && y <= 414 ? Math.sin((x - 140) / 400 * Math.PI) * 38 : 0; } };
  const open = recordingContext(); r._drawFortifications(open, bounds, 1);
  let fill = ''; const masonry = [];
  for (const q of open.calls) { if (q[0] === 'set' && q[1] === 'fillStyle') fill = q[2]; if (q[0] === 'fillRect' && fill === '#a7a58d') masonry.push(q); }
  assert.equal(masonry.length, 2, 'A wall passage was not split into its two surviving masonry pieces');
  assert(masonry.every(([, x, y, w, h]) => y + 38 + h <= 270 || y + 38 >= 414), 'Intact masonry still occupies the assault corridor');
  const deck = recordingContext(); r._drawSiegeRamps(deck, bounds, 1);
  assert(deck.calls.some(q => q[0] === 'fillText' && q[1] === 'ASSAULT RAMP'));
  assert(deck.calls.filter(q => q[0] === 'fill').length >= 4, 'The ramp lacks a visible sloping deck');
  const wallSignature = JSON.stringify(full.calls); wall.type = 'gate'; wall.passages = [];
  const gate = recordingContext(); r._drawFortifications(gate, bounds, 1); assert.notEqual(JSON.stringify(gate.calls), wallSignature);
  wall.breached = true; wall.hp = 0; const breach = recordingContext(); r._drawFortifications(breach, bounds, 1);
  assert(!breach.calls.some(q => q[0] === 'strokeRect'), 'A breached gate kept an intact blocking outline');
  assert(breach.calls.filter(q => q[0] === 'fill').length >= 10, 'A breach has no visible fallen masonry');
  const offscreen = recordingContext(); r._drawFortifications(offscreen, { x1: 2000, x2: 2400, y1: 0, y2: 1000 }, 1);
  assert.equal(offscreen.calls.length, 0, 'Offscreen fortifications still incurred draw work');
  const tower = soldier('siegeTower', { _siegeStage: 'deploying', _siegeProgress: 0 }), lowering = [];
  for (const progress of [0, .5, 1]) {
    tower._siegeProgress = progress; const phase = recordingContext(); r._drawDeployingBridge(phase, tower);
    lowering.push(phase.calls.find(q => q[0] === 'lineTo').slice(1));
  }
  assert(lowering[0][0] < lowering[1][0] && lowering[1][0] < lowering[2][0], 'Deployment never extended the tower bridge');
  assert(lowering[0][1] < lowering[1][1] && lowering[1][1] < lowering[2][1], 'The assault bridge never visibly lowered toward its deck');
  tower._siegeStage = 'approach'; const traveling = recordingContext(); r._drawDeployingBridge(traveling, tower);
  assert.equal(traveling.calls.length, 0, 'A traveling tower lowered a fictitious bridge');
  const ram = soldier('batteringRam', { _lastAttack: -1000, _siegeStage: 'battering', _cooldown: .5, target: -1 });
  assert.equal(r._attackFrame(ram), 4); ram._cooldown = .1; assert.equal(r._attackFrame(ram), 5);
  ram.state = 'move'; ram._vx = 26; assert.equal(r._attackFrame(ram), 0, 'A fresh equipment move kept the old structural wind-up');
  ram.state = 'idle'; ram._vx = 0; ram._siegeStage = 'approach'; assert.equal(r._attackFrame(ram), 0);
  return { survivingWallPieces: masonry.length, rampLaneWidth: 144, visibleDamageAndRubble: true, offscreenDrawCalls: 0,
    distinctDeploymentPositions: lowering.length, ramStructuralWindupFrames: [4, 5] };
});

test('Ramp occupants and their shots use deck elevation while docked towers render once beneath them', () => {
  const tower = soldier('siegeTower', { id: 0, x: 100, y: 200, _siegeDeployed: true, _lastAttack: -1000, _cooldown: 2 }),
    foot = soldier('archer', { id: 1, x: 120, y: 200, _lastAttack: -1000, _cooldown: 2, target: -1 }),
    r = renderer([tower, foot], 20), c = recordingContext();
  Object.assign(r, { ctx: c, _terrain: { tag: 'terrain' }, _visible: [], _lastFrame: 0, _frame: 0, fps: 60,
    dpr: 1, width: 400, height: 300, camera: { x: 150, y: 200, zoom: 1 }, _siegeTowers: [tower],
    options: { quality: 'high', showBodies: true, showOrders: false, cinematic: false, showHealth: false } });
  r.engine.phase = 'paused'; r._getAtlas = fakeAtlas;
  r.engine.siege = { ramps: [{ id: 'deck', x: 50, y: 150, w: 150, h: 120, axis: 'x', team: 0, height: 38 }], heightAt: (x, y) => x >= 50 && x <= 200 && y >= 150 && y <= 270 ? 38 : 0 };
  for (const name of ['_drawWaterMotion', '_drawProjectiles', '_drawEffects', '_drawStandards']) r[name] = () => {};
  const before = JSON.stringify(r.engine); r.render(1, new Set());
  const sprites = c.calls.filter(q => q[0] === 'drawImage' && q[1] !== 'terrain');
  assert.equal(sprites.length, 2); assert.match(sprites[0][1], /^siegeTower:/); assert.match(sprites[1][1], /^archer:/);
  assert.equal(sprites[0][3], tower.y - 30, 'The docked tower itself floated on top of its ramp');
  assert.equal(sprites[1][3], foot.y - 38 - 30, 'A walking ramp occupant rendered through the deck at ground level');
  assert.equal(r._renderStats.drawCalls, 2); assert.equal(JSON.stringify(r.engine), before);
  foot.alive = false; foot._deathTime = 19; c.calls.length = 0; r.render(2, new Set());
  const corpse = c.calls.find(q => q[0] === 'drawImage' && /fallen/.test(q[1]));
  assert(corpse && corpse[3] === foot.y - 38 - 30, 'A casualty on the ramp dropped through its walking surface');
  const p = { attacker: tower.id, type: 'siegeTower', team: 0, kind: 'arrow', sx: 100, sy: 200, tx: 400, ty: 200, x: 100, y: 200, z: 0, age: 0, duration: 1 };
  r.engine.projectiles = [p]; const bounds = { x1: 0, x2: 600, y1: 0, y2: 400 };
  const shot = () => { const q = recordingContext(); BattleRenderer.prototype._drawProjectiles.call(r, q, bounds, 1); return q.calls.find(call => call[0] === 'lineTo'); };
  assert.equal(shot()[2], 84, 'A tower arrow launched from ground level instead of the upper firing platform');
  p.age = .5; p.x = 250; p.z = 20; assert.equal(shot()[2], 122, 'The shot failed to interpolate from the tower platform toward its target');
  p.type = 'archer'; p.attacker = foot.id; p.sx = 120; p.age = 0; p.x = 120; p.z = 0;
  assert.equal(shot()[2], 162, 'An archer on the ramp launched below its deck');
  return { deckElevation: 38, towerLaunchHeight: 116, spriteDraws: 2, dockedTowerDrawnOnce: true, rampCasualtyElevated: true };
});

test('Every biome has deterministic distinct landmarks and a bounded ambient set', () => {
  const signatures = new Set();
  for (const biome of ['grassland', 'woodland', 'wetland', 'desert', 'tundra', 'highland', 'ashland']) {
    const r = renderer(); r.map = { biome }; r.theme = { grass: '#9ba17a' };
    const draw = () => { const c = recordingContext(); r._paintBiomeDetails(c, GameData.RNG(99), 16000, 11000); return JSON.stringify(c.calls); };
    const first = draw(), ambient = JSON.stringify(r._ambientTerrain);
    assert.equal(draw(), first, 'Landmarks vary with wall-clock state');
    assert.equal(JSON.stringify(r._ambientTerrain), ambient);
    assert(r._ambientTerrain.length <= 72, 'Ambient terrain grows with map size without a cap');
    signatures.add(first);
  }
  assert.equal(signatures.size, 7, 'Two biomes use indistinguishable landmark artwork');
  return { distinctBiomeLandmarks: 7, maximumAmbientObjects: 72 };
});

test('Wind and water animation freeze with simulation time, cull offscreen objects and respect reduced motion', () => {
  const r = renderer(); r.options = { animations: 'full', quality: 'high' }; r.theme = { grass: '#abc' };
  r._ambientTerrain = [{ x: 80, y: 70, phase: .1, size: 1, kind: 'grassland' }];
  r.map = { regions: [{ type: 'water', x: 20, y: 20, w: 300, h: 300 }] };
  const bounds = { x1: 0, x2: 400, y1: 0, y2: 400 };
  const terrain = recordingContext(); r._drawTerrainMotion(terrain, bounds, 1);
  const before = JSON.stringify(r.engine); r.engine.time += 1;
  const later = recordingContext(); r._drawTerrainMotion(later, bounds, 1);
  assert.notEqual(JSON.stringify(later.calls), JSON.stringify(terrain.calls), 'The wind does not animate');
  const water = recordingContext(), frozen = recordingContext();
  r._drawWaterMotion(water, bounds, 5, 1); r._drawWaterMotion(frozen, bounds, 1000, 1);
  assert.deepEqual(water.calls, frozen.calls, 'Wall-clock changes animate a paused river');
  const offscreen = recordingContext(); r._drawTerrainMotion(offscreen, { x1: 1000, x2: 2000, y1: 1000, y2: 2000 }, 1);
  assert(!offscreen.calls.some(call => call[0] === 'stroke'), 'Offscreen grass still renders');
  r.options.animations = 'reduced'; const reduced = recordingContext();
  r._drawTerrainMotion(reduced, bounds, 1); r._drawWaterMotion(reduced, bounds, 10, 1);
  assert.equal(reduced.calls.length, 0);
  r.engine.time -= 1; assert.equal(JSON.stringify(r.engine), before, 'Terrain rendering changes simulation state');
  return { pausedWaterFrozen: true, viewportCulled: true, reducedMotionCalls: 0 };
});

test('All troop portraits fit their complete cached equipment silhouette', () => {
  const r = renderer(); let portraits = 0;
  r._cropSprite = function(scratch, ctx, cw, anchorY, scale) {
    const start = ctx.calls.findLastIndex(call => call[0] === 'clearRect');
    const b = drawingBounds(ctx.calls.slice(start + 1));
    return { full: { tag: 'portrait' }, w: (b.x2 - b.x1 + 4) / scale, h: (b.y2 - b.y1 + 4) / scale, ox: 0, oy: 0 };
  };
  for (const type of GameData.TYPE_ORDER) for (const size of [32, 80]) {
    const target = canvas(); target.width = target.height = size; r.drawUnitIcon(target, type, 0);
    const [, , x, y, w, h] = target.getContext().calls.find(call => call[0] === 'drawImage');
    assert(x >= 1 && y >= 1 && x + w < size && y + h < size, type + ' portrait loses part of its weapon or standard');
    portraits++;
  }
  return { completePortraits: portraits };
});

test('Broken musketeer ranks animate close combat separately from musket reloads', () => {
  const u = soldier('musketeer', { regiment: 0, _meleeCooldown: .2, _lastMusketMelee: 20 }), enemy = soldier('militia', { id: 1, x: 120 });
  const r = renderer([u, enemy]); r.options = { animations: 'full' }; r.engine.regiments = [{ volleyStatus: 'melee' }];
  const normal = recordingContext(), melee = recordingContext();
  r._drawSprite(normal, 'musketeer', 0, 0, 8); r._drawActionSprite(melee, 'musketeer', 0, 0, 'musketMelee', 4);
  assert.notDeepEqual(normal.calls, melee.calls, 'A fighting musketeer still uses a reload pose');
  r.engine.time = 20.06; assert.equal(r._attackFrame(u), 6);
  assert.deepEqual(r._motionAction(u, 6), { kind: 'musketMelee', phase: 2 });
  r.engine.regiments[0].volleyStatus = 'reforming'; r.engine.time = 20.2;
  assert.equal(r._attackFrame(u), 7, 'Losing contact recast the last melee swing as a musket shot');
  assert.deepEqual(r._motionAction(u, 7), { kind: 'musketMelee', phase: 3 });
  r.engine.regiments[0].volleyStatus = 'melee'; u._lastAttack = 20.15;
  assert.equal(r._motionAction(u, 6), null, 'A recent actual shot was recast as a musket-butt attack');
  r.engine.time = 22; enemy.x = 900; assert.equal(r._attackFrame(u), 0, 'A distant soldier performs a pretend musket-butt attack');
  u.state = 'turning'; assert.equal(r._motionAction(u, 6), null);
  return { separateCloseCombatChoreography: true, honestAttackEvents: true };
});

console.log(JSON.stringify({ passed: results.filter(x => x.passed).length, failed: results.filter(x => !x.passed).length, tests: results }, null, 2));
if (results.some(x => !x.passed)) process.exitCode = 1;
