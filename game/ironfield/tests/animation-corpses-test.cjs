'use strict';
// Dependency-free, recording-Canvas checks. No browser or rendered-FPS claims.
// Run: node tests/animation-corpses-test.cjs [alternate-project-root]
const assert = require('node:assert/strict');
const path = require('node:path');
const crypto = require('node:crypto');
const root = process.argv[2] || path.resolve(__dirname, '..');
global.window = global;
function context() {
  const state = { calls: [], globalAlpha: 1 };
  return new Proxy(state, {
    get(o, key) {
      if (key in o) return o[key];
      if (String(key).startsWith('create') && String(key).endsWith('Gradient')) return () => ({ addColorStop() {} });
      if (key === 'measureText') return text => ({ width: String(text).length * 6 });
      return (...args) => {
        for (const value of args) if (typeof value === 'number') assert(Number.isFinite(value), 'Nonfinite Canvas geometry: ' + key);
        o.calls.push([String(key), ...args.map(value => value && value.tag || value)]);
      };
    },
    set(o, key, value) { o[key] = value; if (key !== 'calls') o.calls.push(['set', key, value]); return true; }
  });
}
function canvas() { const c = context(); return { width: 1, height: 1, getContext: () => c }; }
global.document = { createElement: () => canvas() };
require(path.join(root, 'src/data.js'));
require(path.join(root, 'src/graphics.js'));
const sprite = tag => ({ full: { tag, width: 40, height: 40 }, mid: { tag, width: 20, height: 20 }, low: { tag, width: 5, height: 5 }, w: 40, h: 40, ox: -20, oy: -20 });
const atlases = new Map();
function fakeAtlas(type, team) {
  const key = type + ':' + team;
  if (!atlases.has(key)) atlases.set(key, { frames: Array.from({ length: 80 }, (_, i) => sprite(key + ':frame-' + i)), corpses: Array.from({ length: 8 }, (_, i) => sprite(key + ':fallen-' + i)), airborne: Array.from({ length: 8 }, (_, i) => sprite(key + ':airborne-' + i)) });
  return atlases.get(key);
}
function unit(id, extra = {}) {
  return Object.assign({ id, type: 'militia', team: id % 2, x: 200 + id % 20, y: 200 + Math.floor(id / 20) % 20, angle: 0, alive: false, routed: false, _deathTime: 0, _lastAttack: -1000, _impactUntil: 0, _impactDuration: 0, _impactStart: 0, _impactLift: 0, _vx: 0, _vy: 0, travelDistance: 0 }, extra);
}
function renderer(units, time = 10, mode = 'battle') {
  const r = Object.create(BattleRenderer.prototype);
  Object.assign(r, { engine: { units, time, width: 16000, height: 11000, regiments: [], effects: [], phase: 'running', deadUnitIds: units.filter(u => !u.alive && !u.routed).map(u => u.id) },
    options: { corpseMode: mode, animations: 'full', quality: 'high', showBodies: true }, camera: { x: 250, y: 250, zoom: 1 }, dpr: 1,
    _visibleBodies: [], _bodyBuckets: Array.from({ length: 6 }, () => []), _bodyBuildBudget: 2, _poseBuildBudget: 2, _bodyWarmQueue: [], _unitAtlases: [], _atlas: new Map(), _hitPoses: new Map(), _formationEnvelopes: new Map(), _corpseCache: null,
    width: 1000, height: 700, _terrainScale: .4, _terrain: null, _minimapBase: null });
  r._getAtlas = fakeAtlas;
  return r;
}
const bounds = { x1: 0, y1: 0, x2: 512, y2: 512 };
const results = [];
function test(name, run) {
  if (process.env.IRONFIELD_ANIMATION_FILTER && !name.toLowerCase().includes(process.env.IRONFIELD_ANIMATION_FILTER.toLowerCase())) return;
  try { results.push({ name, passed: true, ...run() }); console.log('PASS ' + name); }
  catch (error) { results.push({ name, passed: false, error: error.stack }); console.error('FAIL ' + name + '\n' + error.stack); }
}
function signature(calls) {
  return crypto.createHash('sha256').update(JSON.stringify(calls, (_, value) => typeof value === 'number' ? Math.round(value * 1000) / 1000 : value)).digest('hex');
}

// Inspect vector bounds after Canvas transforms, without screenshots.
function drawingBounds(calls) {
  let matrix = [1, 0, 0, 1, 0, 0], width = 1, current = null, box = null;
  const stack = [], include = (b, point) => !b ? { x1: point[0], y1: point[1], x2: point[0], y2: point[1] } :
    { x1: Math.min(b.x1, point[0]), y1: Math.min(b.y1, point[1]), x2: Math.max(b.x2, point[0]), y2: Math.max(b.y2, point[1]) };
  const transform = (x, y) => [matrix[0] * x + matrix[2] * y + matrix[4], matrix[1] * x + matrix[3] * y + matrix[5]];
  for (const [method, ...a] of calls) {
    if (method === 'save') stack.push({ matrix: matrix.slice(), width });
    else if (method === 'restore') { const old = stack.pop(); if (old) { matrix = old.matrix; width = old.width; } }
    else if (method === 'set' && a[0] === 'lineWidth') width = a[1];
    else if (method === 'setTransform') matrix = a.slice();
    else if (method === 'translate') { matrix[4] += matrix[0] * a[0] + matrix[2] * a[1]; matrix[5] += matrix[1] * a[0] + matrix[3] * a[1]; }
    else if (method === 'scale') { matrix[0] *= a[0]; matrix[1] *= a[0]; matrix[2] *= a[1]; matrix[3] *= a[1]; }
    else if (method === 'rotate') { const [aa, b, cc, d] = matrix, co = Math.cos(a[0]), si = Math.sin(a[0]); matrix[0] = aa * co + cc * si; matrix[1] = b * co + d * si; matrix[2] = cc * co - aa * si; matrix[3] = d * co - b * si; }
    else if (method === 'beginPath') current = null;
    else if (method === 'moveTo' || method === 'lineTo') current = include(current, transform(a[0], a[1]));
    else if (method === 'quadraticCurveTo' || method === 'bezierCurveTo') for (let i = 0; i < a.length; i += 2) current = include(current, transform(a[i], a[i + 1]));
    else if (method === 'ellipse' || method === 'arc') {
      const rx = a[2], ry = method === 'ellipse' ? a[3] : rx, angle = method === 'ellipse' ? a[4] : 0, co = Math.cos(angle), si = Math.sin(angle), q = transform(a[0], a[1]);
      const dx = Math.hypot((matrix[0] * co + matrix[2] * si) * rx, (-matrix[0] * si + matrix[2] * co) * ry), dy = Math.hypot((matrix[1] * co + matrix[3] * si) * rx, (-matrix[1] * si + matrix[3] * co) * ry);
      current = include(include(current, [q[0] - dx, q[1] - dy]), [q[0] + dx, q[1] + dy]);
    } else if ((method === 'fill' || method === 'stroke') && current) {
      const pad = method === 'stroke' ? width * Math.max(Math.hypot(matrix[0], matrix[1]), Math.hypot(matrix[2], matrix[3])) / 2 : 0;
      box = include(include(box, [current.x1 - pad, current.y1 - pad]), [current.x2 + pad, current.y2 + pad]);
    }
  }
  return box;
}

test('Every troop has eight distinct marching poses; mounted charges and braced steps remain distinct', () => {
  const r = renderer([]), counts = {};
  for (const type of GameData.TYPE_ORDER) {
    const hashes = new Set();
    for (let phase = 0; phase < 8; phase++) { const c = context(); r._drawActionSprite(c, type, 0, 0, 'march', phase); hashes.add(signature(c.calls)); }
    assert.equal(hashes.size, 8, type + ' has duplicate walking phases after rounding to actual drawing precision'); counts[type] = hashes.size;
  }
  for (const type of ['pikeman', 'hoplite', 'swordsman', 'guardian']) {
    const c = context(), m = context(); r._drawActionSprite(c, type, 0, 0, 'brace', 2); r._drawActionSprite(m, type, 0, 0, 'march', 2); assert.notEqual(signature(c.calls), signature(m.calls));
  }
  for (const type of ['lancer', 'cavalry']) {
    const c = context(), m = context(); r._drawActionSprite(c, type, 1, 4, 'gallop', 2); r._drawActionSprite(m, type, 1, 4, 'march', 2); assert.notEqual(signature(c.calls), signature(m.calls));
  }
  for (const type of ['militia', 'pikeman', 'hoplite', 'guardian', 'cavalry']) {
    for (const kind of ['backstep', 'sidestep']) {
      const hashes = new Set();
      for (let phase = 0; phase < 8; phase++) { const c = context(); r._drawActionSprite(c, type, 0, 0, kind, phase); hashes.add(signature(c.calls)); }
      assert.equal(hashes.size, 8, type + ' has duplicate ' + kind + ' phases');
      const c = context(), m = context(); r._drawActionSprite(c, type, 0, 0, kind, 2); r._drawActionSprite(m, type, 0, 0, 'march', 2);
      assert.notEqual(signature(c.calls), signature(m.calls), type + ' yields with an unchanged full marching stride');
    }
  }
  return { troopTypes: Object.keys(counts).length, marchingPosesEach: 8, chargingTypes: 2, movingBrace: true, pressureGaits: ['backstep', 'sidestep'] };
});

test('New action sources fit their canvases, build at most two per frame and reuse their cache', () => {
  const r = renderer([]); r._getAtlas = BattleRenderer.prototype._getAtlas;
  let sources = 0;
  r._cropSprite = (scratch, ctx, cw, anchorY, scale) => {
    const at = ctx.calls.findLastIndex(q => q[0] === 'clearRect'), b = drawingBounds(ctx.calls.slice(at + 1));
    assert(b && b.x1 >= 0 && b.y1 >= 0 && b.x2 <= scratch.width && b.y2 <= scratch.height, 'A new pose clips equipment or limbs');
    sources++; return sprite('cached-' + sources);
  };
  let checked = 0;
  for (const type of GameData.TYPE_ORDER) {
    const u = unit(0, { type }), atlas = r._getAtlas(type, 0);
    assert.equal(atlas.frames.length, 80, 'New actions grew the compulsory startup atlas');
    for (const kind of ['march', ...(GameData.TYPES[type].mounted ? ['gallop'] : []), ...(GameData.TYPES[type].ranged || GameData.TYPES[type].equipment ? [] : ['brace']), ...(GameData.TYPES[type].equipment ? [] : ['collapse', 'backstep', 'sidestep']), 'recoil', 'block']) {
      for (const dir of [0, 4, 6]) for (const phase of ['march', 'gallop', 'brace', 'backstep', 'sidestep'].includes(kind) ? [0, 1, 2, 3, 4, 5, 6, 7] : kind === 'collapse' ? [0, 1, 2] : [0, 1]) {
        r._poseBuildBudget = 1; assert(r._actionSprite(u, atlas, dir, kind, phase)); checked++;
      }
    }
  }
  const atlas = r._getAtlas('hoplite', 1), u = unit(0, { type: 'hoplite', team: 1 });
  r._poseBuildBudget = 2; const before = sources;
  assert(r._actionSprite(u, atlas, 0, 'brace', 0)); assert(r._actionSprite(u, atlas, 0, 'brace', 1));
  assert.equal(r._actionSprite(u, atlas, 0, 'brace', 2), null); assert.equal(sources - before, 2);
  assert(r._actionSprite(u, atlas, 0, 'brace', 1)); assert.equal(sources - before, 2);
  return { checkedActionFacings: checked, newSourcesPerFrame: 2, baseFramesUnchanged: 80 };
});

test('Animation selection follows movement distance, actual momentum, brace state and sampled hit events', () => {
  const u = unit(0, { alive: true, type: 'pikeman', state: 'move', regiment: 0, _vx: 35, travelDistance: 8 }), r = renderer([u]);
  r.engine.regiments = [{ order: 'auto', _steadyBraceScale: .58, _tactic: { movingBrace: true } }];
  const first = { ...r._motionAction(u, 0) }; assert.equal(first.kind, 'brace');
  r.engine.time = 900; assert.deepEqual(r._motionAction(u, 0), first, 'Paused animation advanced with time instead of covered distance');
  u.travelDistance += 3; assert.notEqual(r._motionAction(u, 0).phase, first.phase);
  assert.equal(r._motionAction(u, 6), null, 'A new gait suppressed an actual strike');
  r.engine.regiments[0].order = 'move'; assert.equal(r._motionAction(u, 0).kind, 'march');
  for (const state of ['turning', 'fallBack']) { u.state = state; assert.equal(r._motionAction(u, 0), null, 'Reaction or brace overrode a fresh turn/withdrawal'); }
  u.type = 'lancer'; u.state = 'move'; u._mountSpeed = GameData.TYPES.lancer.speed * .8; r.engine.regiments[0].order = 'charge';
  assert.equal(r._motionAction(u, 0).kind, 'gallop'); u._mountSpeed *= .4; assert.equal(r._motionAction(u, 0).kind, 'march');
  u.state = 'idle'; u._vx = 0;
  r.engine.effects = [{ kind: 'hit', unitId: 0, blocked: false, age: .05 }]; r._collectHitPoses(); assert.equal(r._motionAction(u, 0).kind, 'recoil');
  r.engine.effects[0].blocked = true; r._collectHitPoses(); assert.equal(r._motionAction(u, 0).kind, 'block');
  r.engine.effects[0].age = .3; r._collectHitPoses(); assert.equal(r._motionAction(u, 0), null);
  r.options.animations = 'reduced'; u.state = 'move'; u._vx = 35; assert.equal(r._motionAction(u, 0), null);
  return { usesActualDistance: true, momentumThreshold: .65, realHitOnly: true, explicitOrderPrecedence: true };
});

test('Pressure stepping follows actual displacement without turning soldiers or overriding attacks and fresh orders', () => {
  const heading = Math.PI / 4, nx = Math.cos(heading), ny = Math.sin(heading);
  const u = unit(0, { alive: true, state: 'idle', regiment: 0, angle: heading, travelDistance: 3,
    _pressureTick: 42, _pressureDX: -nx * .1, _pressureDY: -ny * .1 });
  const r = renderer([u]), group = { order: 'hold', _formationPressureSpeed: 3, _formationPressureUntil: 10.3, _pressureSourceId: 1 };
  r.engine._tick = 42; r.engine.regiments = [group];
  const initial = JSON.stringify(r.engine), back = { ...r._motionAction(u, 0) };
  assert.equal(back.kind, 'backstep'); assert.equal(back.phase, 2);
  assert.equal(JSON.stringify(r.engine), initial, 'Selecting pressure gait changed physical state or facing');
  r.engine.phase = 'paused'; r._now = 900;
  assert.deepEqual(r._motionAction(u, 0), back, 'Paused pressure stepping advanced with wall-clock time');
  r.engine.phase = 'running'; u.travelDistance += 1.5;
  assert.notEqual(r._motionAction(u, 0).phase, back.phase, 'A real distance increment did not advance the pressure gait');
  u._pressureDX = -ny * .1; u._pressureDY = nx * .1;
  const lateral = { ...r._motionAction(u, 0) }; assert.equal(lateral.kind, 'sidestep');
  u._pressureDX *= -1; u._pressureDY *= -1;
  assert.equal(r._motionAction(u, 0).phase, (8 - lateral.phase) % 8, 'The opposite lateral displacement retained the same stepping direction');
  const oldDraw = r._drawSprite; let dispatch;
  r._drawSprite = (...args) => { dispatch = args; };
  r._drawActionSprite(context(), u.type, u.team, 5, 'backstep', 2);
  assert.equal(dispatch[3], 5, 'A backward step flipped the accepted sprite facing');
  assert.equal(dispatch[4], 0); assert(dispatch[6] < 0, 'A backward step did not reverse its foot cycle');
  r._drawSprite = oldDraw;
  r._hitPoses.set(u.id, { kind: 'recoil', phase: 0 });
  for (const frame of [4, 5, 6, 7, 8, 9]) assert.equal(r._motionAction(u, frame), null, 'Pressure or a hit reaction suppressed an actual attack pose');
  r._hitPoses.clear();
  for (const state of ['turning', 'fallBack']) { u.state = state; assert.equal(r._motionAction(u, 0), null, 'Pressure replaced an explicit turn or withdrawal pose'); }
  u.state = 'idle';
  for (const order of ['face', 'retreat']) { group.order = order; assert.equal(r._motionAction(u, 0), null, 'Pressure survived a fresh ' + order + ' order'); }
  group.order = 'move'; u.state = 'move'; u._vx = 35;
  assert.equal(r._motionAction(u, 0).kind, 'march', 'A fresh Move kept an old pressure shuffle');
  group.order = 'hold'; u.state = 'idle'; u._vx = 0;
  u._pressureTick = 41; assert.equal(r._motionAction(u, 0), null, 'Previous-tick pressure kept a stopped soldier stepping');
  u._pressureTick = 42; u._pressureDX = u._pressureDY = 0;
  assert.equal(r._motionAction(u, 0), null, 'A requested force without body displacement animated the feet');
  u._pressureDX = -.1; group._formationPressureSpeed = 0;
  assert.equal(r._motionAction(u, 0), null, 'Cleared pressure retained a stale gait');
  group._formationPressureSpeed = 3; group._formationPressureUntil = r.engine.time;
  assert.equal(r._motionAction(u, 0), null, 'Expired pressure retained a stale gait');
  assert.equal(u.angle, heading, 'Pressure rendering changed the soldier heading');
  return { backwardAndLateralSteps: true, actualDistanceOnly: true, pausedPoseFrozen: true, staleMotionRejected: true, facingPreserved: true, attackAndCommandPrecedence: true };
});

test('Friendly mounted sidesteps show actual clearance while attacks, turns and galloping keep their priority', () => {
  const u = unit(0, { alive: true, type: 'lancer', state: 'idle', regiment: 0, angle: 0,
    _friendlyTick: 12, _friendlyDistance: 1.3, _friendlyDX: 0, _friendlyDY: 1.3, travelDistance: 3 });
  const r = renderer([u]), group = { order: 'hold' }; r.engine._tick = 12; r.engine.regiments = [group];
  const before = JSON.stringify(r.engine), first = { ...r._motionAction(u, 0) };
  assert.equal(first.kind, 'sidestep'); assert.equal(JSON.stringify(r.engine), before, 'Rendering a courtesy step changed the actual heading or momentum');
  r.engine.phase = 'paused'; r._now = 99; assert.deepEqual(r._motionAction(u, 0), first, 'Paused courtesy stepping kept advancing');
  u.travelDistance += 3; assert.notEqual(r._motionAction(u, 0).phase, first.phase);
  assert.equal(r._motionAction(u, 6), null, 'A friendly sidestep suppressed a strike pose');
  for (const order of ['face', 'retreat']) { group.order = order; assert.equal(r._motionAction(u, 0), null, 'Friendly movement overrode an explicit turn or retreat'); }
  group.order = 'hold'; u._friendlyTick = 11; assert.equal(r._motionAction(u, 0), null, 'An old contact kept the feet shuffling');
  u._friendlyTick = 12; u._friendlyDistance = 0; assert.equal(r._motionAction(u, 0), null, 'A requested gap without actual movement animated the horse');
  u._friendlyDistance = 1.3; u._friendlyDX = u._friendlyDY = 0; assert.equal(r._motionAction(u, 0), null);
  group.order = 'charge'; u.state = 'move'; u._vx = 140; u._mountSpeed = 140;
  u._friendlyDY = .03; u._friendlyDistance = .03;
  assert.equal(r._motionAction(u, 0).kind, 'gallop', 'A tiny courtesy correction replaced a real forward gallop with a shuffle');
  u._friendlyDY = 1.3; u._friendlyDistance = 1.3;
  assert.equal(r._motionAction(u, 0).kind, 'sidestep', 'A substantial real lateral clearance was invisible');
  r.options.animations = 'reduced'; assert.equal(r._motionAction(u, 0), null);
  assert.equal(u.angle, 0);
  return { actualLateralMotion: true, facingAndEnergyUnchanged: true, noStaleOrRequestedOnlyGait: true, smallCorrectionsRetainGallop: true, pausedAndCommandSafe: true };
});

test('Giving ground appears only for actual live pressure and yields to command and morale banners', () => {
  const r = renderer([]), c = context(), group = { id: 0, alive: 40, team: 0, type: 'hoplite', unitIds: [], x: 250, y: 250,
    formation: 'line', maneuver: 'formed', facing: Math.PI, morale: 100, cohesion: 100, order: 'hold', commandStatus: 'holding', status: 'steady',
    _formationPressureSpeed: 3, _formationPressureUntil: 10.3, _pressureSourceId: 1 };
  r.engine.regiments = [group]; r.options.showLabels = true; r._now = 0;
  const labels = () => { c.calls.length = 0; r._drawStandards(c, new Set([0]), 1, false); return c.calls.filter(q => q[0] === 'fillText').map(q => q[1]); };
  const initial = JSON.stringify(group);
  assert(labels().includes('GIVING GROUND')); assert.equal(JSON.stringify(group), initial, 'Drawing pressure feedback changed the regiment');
  group._formationPressureSpeed = 0; assert(!labels().includes('GIVING GROUND'), 'A fully blocked force displayed actual yielding');
  group._formationPressureSpeed = 3; group._formationPressureUntil = r.engine.time;
  assert(!labels().includes('GIVING GROUND'), 'Expired pressure retained its banner');
  group._formationPressureUntil = 10.3; group._pressureSourceId = -1;
  assert(!labels().includes('GIVING GROUND'), 'Cancelled pressure retained its banner');
  group._pressureSourceId = 1;
  for (const [patch, expected] of [
    [{ order: 'face', commandStatus: 'facing' }, 'TURNING IN PLACE'],
    [{ order: 'retreat', commandStatus: 'turning' }, 'TURNING TO WITHDRAW'],
    [{ fallingBack: true, status: 'fallingBack' }, 'FALLING BACK'],
    [{ status: 'rallying' }, 'REFORMING'], [{ status: 'shaken' }, 'SHAKEN'], [{ status: 'routing' }, 'ROUTING']
  ]) {
    Object.assign(group, { order: 'hold', commandStatus: 'holding', fallingBack: false, status: 'steady' }, patch);
    const shown = labels(); assert(shown.includes(expected), 'Pressure hid ' + expected); assert(!shown.includes('GIVING GROUND'));
  }
  Object.assign(group, { order: 'move', commandStatus: 'moving', fallingBack: false, status: 'steady' });
  assert(!labels().includes('GIVING GROUND'), 'A new Move retained the pressure banner');
  return { actualPressureRequired: true, immediateCancellation: true, commandAndMoralePrecedence: true };
});

test('Colossal overview skips extra action lookups and hit-pose work while retaining base march and strike frames', () => {
  const units = Array.from({ length: 20000 }, (_, id) => unit(id, {
    alive: true, type: id === 0 ? 'hoplite' : id % 2 ? 'cavalry' : 'militia',
    x: 100 + id * 73 % 15800, y: 100 + id * 337 % 10800,
    state: 'move', _vx: 35, travelDistance: id * 11, _lastAttack: id === 0 ? 9.97 : -1000, _cooldown: 1,
    regiment: 0, _pressureTick: 42, _pressureDX: -.1, _pressureDY: 0
  }));
  const r = renderer(units), c = context();
  r.engine._tick = 42; r.engine.regiments = [{ order: 'hold', _formationPressureSpeed: 3, _formationPressureUntil: 10.3 }];
  Object.assign(r, { ctx: c, _terrain: { tag: 'terrain' }, _visible: [], _lastFrame: 0, _frame: 0, fps: 60,
    _adaptiveLow: false, _qualityChangedAt: 0, _siegeTowers: [],
    _depthCounts: new Int32Array(0), _depthKeys: new Uint32Array(0), _depthSorted: [],
    camera: { x: 8000, y: 5500, zoom: .059 }, _unitAtlases: units.map(u => fakeAtlas(u.type, u.team)) });
  r.options.showBodies = false;
  for (const name of ['_drawWaterMotion', '_drawProjectiles', '_drawEffects', '_drawStandards', '_drawFacingIndicators']) r[name] = () => {};
  const counts = { motion: 0, action: 0, hits: 0 };
  const motion = r._motionAction, collect = r._collectHitPoses;
  r._motionAction = function (...args) { counts.motion++; return motion.apply(this, args); };
  r._actionSprite = (u, atlas, dir, kind, phase) => { counts.action++; return sprite('extra-' + kind + '-' + phase); };
  r._collectHitPoses = function () { counts.hits++; return collect.call(this); };
  r.engine.effects = [{ kind: 'hit', unitId: 1, age: .05, blocked: true }];
  const dispatch = () => {
    counts.motion = counts.action = counts.hits = 0; c.calls.length = 0;
    r.render(1 + r._frame / 60, new Set());
    return c.calls.filter(call => call[0] === 'drawImage' && call[1] !== 'terrain');
  };
  for (const quality of ['auto', 'high']) for (const dpr of [1, 2]) {
    r.options.quality = quality; r.dpr = dpr; r._hitPoses.set(17, { kind: 'recoil', phase: 0 });
    const draws = dispatch();
    assert.equal(draws.length, 20000); assert.equal(r._renderStats.drawCalls, 20000);
    assert.deepEqual(counts, { motion: 0, action: 0, hits: 0 }, 'A subpixel overview still selected expanded poses for every soldier');
    assert.equal(r._hitPoses.size, 0, 'Overview retained stale hit reactions');
    assert(draws.some(call => call[1] === 'hoplite:0:frame-48'), 'Overview lost an actual base strike');
    assert(draws.some(call => /^militia:0:frame-(8|16|24)$/.test(call[1])), 'Overview lost the existing three-frame marching gait');
  }
  r.engine.regiments = [];
  r.engine.units = units.slice(0, 16).map((u, i) => ({ ...u, x: 200 + i % 4 * 20, y: 200 + Math.floor(i / 4) * 20 }));
  r.engine.units[1].state = 'idle'; r.engine.units[1]._vx = 0;
  r.camera = { x: 250, y: 250, zoom: .24 }; r.dpr = 1; r.options.quality = 'high'; dispatch();
  assert.deepEqual(counts, { motion: 0, action: 0, hits: 0 });
  for (const [zoom, dpr] of [[.25, 1], [.125, 2]]) {
    r.camera.zoom = zoom; r.dpr = dpr; const draws = dispatch();
    assert.equal(counts.motion, 16); assert.equal(counts.hits, 1); assert(counts.action >= 14);
    assert(draws.some(call => call[1] === 'extra-block-0'), 'Visible-scale hit reactions did not resume');
  }
  r.options.animations = 'reduced'; dispatch();
  assert.deepEqual(counts, { motion: 0, action: 0, hits: 0 });
  const fallen = unit(0, { _deathTime: 9.95 }); r.options.animations = 'full';
  r.camera.zoom = .059; r.dpr = 1; counts.action = 0;
  assert(r._drawFallenUnit(c, fallen, 0, 'low')); assert.equal(counts.action, 0, 'Subpixel death animations still built expanded poses');
  r.camera.zoom = .25; assert(r._drawFallenUnit(c, fallen, 0, 'low')); assert.equal(counts.action, 1);
  return { overviewSoldiers: 20000, overviewActionLookups: 0, overviewHitCollections: 0, retainedBaseGaitAndStrikes: true, expandedPosePixelZoom: .25, retinaThresholdChecked: true };
});

test('Real engine deaths and protected hits feed the renderer without duplicate casualty records', () => {
  for (const file of ['tactics.js', 'impacts.js', 'siege.js', 'engine.js']) require(path.join(root, 'src', file));
  const e = new BattleEngine({ map: 'custom', seed: 23, customMap: { regions: [] },
    armies: [{ name: 'Assault', doctrine: 'balanced', roster: { swordsman: 2 } }, { name: 'Defense', doctrine: 'balanced', roster: { hoplite: 2 } }],
    formations: [{ team: 0, type: 'swordsman', count: 2, x: 1000, y: 2000, facing: 0 }, { team: 1, type: 'hoplite', count: 2, x: 2000, y: 2000, facing: Math.PI }] });
  const attacker = e.units.find(u => u.team === 0), target = e.units.find(u => u.team === 1);
  e.time = 5; e._tick = (7 - target.id % 7) % 7; e._hit(attacker, target, 2, false, false);
  const effect = e.effects.find(effect => effect.kind === 'hit'); assert(effect && effect.unitId === target.id); assert.equal(effect.blocked, true);
  const r = renderer(e.units, 5); r.engine = e; r._collectHitPoses(); target.state = 'idle';
  assert.equal(r._motionAction(target, 0).kind, 'block');
  e._kill(target, attacker); e._kill(target, attacker); assert.deepEqual(e.deadUnitIds, [target.id]);
  e.time = 6; const all = { x1: 0, x2: e.width, y1: 0, y2: e.height };
  assert.equal(r._drawBodies(context(), all, false, 'full'), 1); assert.equal(r._corpseStats.fullArmyScans, 0);
  return { realHitId: effect.unitId, partialGuardAnimation: true, casualtyIds: e.deadUnitIds.length, fullArmyScans: 0 };
});

test('Corpse settings preserve brief timing, extend to120 seconds or retain casualties throughout battle', () => {
  const u = unit(0), r = renderer([u], 10, 'brief');
  r.engine.time = 18; assert.equal(r._corpseOpacity(u), .43); r.engine.time = 22; assert.equal(r._corpseOpacity(u), 0);
  r.options.corpseMode = 'long'; r.engine.time = 96; assert.equal(r._corpseOpacity(u), .86);
  r.engine.time = 108; assert.equal(r._corpseOpacity(u), .43); r.engine.time = 120; assert.equal(r._corpseOpacity(u), 0);
  r.options.corpseMode = 'battle'; r.engine.time = 36000; assert.equal(r._corpseOpacity(u), .86);
  for (const patch of [{ alive: true }, { routed: true }, { _deathTime: -1 }]) { const before = { ...u }; Object.assign(u, patch); assert.equal(r._corpseOpacity(u), 0); Object.assign(u, before); }
  return { briefSeconds: 22, longSeconds: 120, retainedAtHours: 10 };
});

test('Twenty thousand simultaneous casualties fully accumulate in piles and settle to one cached draw', () => {
  const units = Array.from({ length: 20000 }, (_, id) => unit(id)), r = renderer(units), c = context();
  let frames = 0, maximumNew = 0, before = 0;
  do {
    r._bodyBuildBudget = 2; c.calls.length = 0; r._drawBodies(c, bounds, false, 'full'); frames++;
    const now = r._corpseStats.registered; maximumNew = Math.max(maximumNew, now - before); before = now;
  } while ((r._corpseStats.registered < 20000 || r._corpseStats.settled < 20000 || r._corpseCache.tiles.values().next().value.baked < 20000) && frames < 30);
  assert.equal(r._corpseStats.registered, 20000); assert.equal(r._corpseStats.settled, 20000);
  const tile = r._corpseCache.tiles.values().next().value;
  assert.equal(tile.entries.length, 20000); assert.equal(new Set(tile.entries.map(q => q.id)).size, 20000); assert.equal(tile.visibleCount, 20000);
  assert.equal(Math.max(...tile.entries.map(q => q.layer)), 8, 'Deaths never build raised overlapping layers');
  assert(maximumNew <= 1200, 'A same-frame catastrophe blocked rendering by draining an unbounded queue');
  let reads = 0;
  r.engine.units = new Proxy(units, { get(target, key) { if (/^\d+$/.test(String(key))) reads++; return target[key]; } });
  r.engine.time = 3600; c.calls.length = 0; assert.equal(r._drawBodies(c, bounds, false, 'full'), 20000);
  assert.equal(reads, 0, 'Settled persistent corpses scanned the whole army again');
  assert.equal(c.calls.filter(q => q[0] === 'drawImage').length, 1); assert.equal(r._corpseStats.drawCalls, 1); assert.equal(r._corpseStats.fullArmyScans, 0);
  return { casualties: 20000, drainFrames: frames, maximumNewPerFrame: maximumNew, steadyDrawCalls: 1, steadyUnitReads: reads, pileLayers: 9, cachePixels: r._corpseStats.canvasPixels };
});

test('Persistent corpse chunks respect viewport culling and a fixed raster budget across pan and zoom', () => {
  const units = []; for (let row = 0; row < 21; row++) for (let col = 0; col < 31; col++) units.push(unit(units.length, { x: col * 512 + 240, y: row * 512 + 240 }));
  const r = renderer(units), all = { x1: 0, y1: 0, x2: 16000, y2: 11000 }; r.camera.zoom = .05;
  r._drawBodies(context(), all, false, 'low'); assert.equal(r._corpseStats.settled, units.length);
  const overviewPixels = r._corpseStats.canvasPixels;
  assert(overviewPixels <= 8000000); assert.equal(r._corpseStats.drawnTiles, units.length);
  r.camera.zoom = 1;
  for (let col = 0; col < 31; col++) {
    const viewport = { x1: col * 512, x2: col * 512 + 510, y1: 0, y2: 510 }, c = context();
    r._drawBodies(c, viewport, false, 'full');
    assert(r._corpseStats.canvasPixels <= 8000000, 'Panning leaked old raster tiles');
    assert(r._corpseStats.drawnTiles <= 6, 'Distant bodies were drawn outside the viewport');
  }
  const stored = Array.from(r._corpseCache.tiles.values()).reduce((sum, tile) => sum + tile.entries.length, 0); assert.equal(stored, units.length, 'Raster eviction removed casualty history');
  r.camera.zoom = 1; r._drawBodies(context(), all, false, 'full'); assert(r._corpseStats.canvasPixels <= 8000000, 'A huge viewport overran the budget instead of reducing only raster detail');
  return { occupiedChunks: units.length, overviewPixels, maximumPixels: 8000000, historySurvivesEviction: true };
});

test('Mode changes restore earlier deaths and long mode eventually clears its expired cached pixels', () => {
  const units = Array.from({ length: 40 }, (_, id) => unit(id)), r = renderer(units, 40, 'brief');
  assert.equal(r._drawBodies(context(), bounds, false, 'mid'), 0);
  r.options.corpseMode = 'battle'; assert.equal(r._drawBodies(context(), bounds, false, 'mid'), 40);
  r.options.corpseMode = 'long'; assert.equal(r._drawBodies(context(), bounds, false, 'mid'), 40);
  r.engine.time = 121; assert.equal(r._drawBodies(context(), bounds, false, 'mid'), 0);
  r.options.corpseMode = 'battle'; assert.equal(r._drawBodies(context(), bounds, false, 'mid'), 40);
  r.options.corpseMode = 'brief'; assert.equal(r._drawBodies(context(), bounds, false, 'mid'), 0); assert.equal(r._corpseCache, null);
  return { earlierCasualtiesRestored: 40, timedCacheClears: true };
});

test('Airborne and moving dead bodies finish their real trajectory before entering a static pile', () => {
  const u = unit(0, { _impactStart: 1, _impactDuration: .8, _impactUntil: 2.1, _impactLift: 30, _impactNX: 1 }), r = renderer([u], 1.4);
  r._drawBodies(context(), bounds, false, 'mid'); assert.equal(r._corpseStats.settled, 0); assert.equal(r._corpseStats.pending, 1);
  u.x += 37; r.engine.time = 2.2; r._drawBodies(context(), bounds, false, 'mid'); assert.equal(r._corpseStats.settled, 0);
  r.engine.siege = { heightAt: () => 38 }; r.engine.time = 2.5; r._drawBodies(context(), bounds, false, 'mid');
  assert.equal(r._corpseStats.settled, 1); const entry = r._corpseCache.tiles.values().next().value.entries[0];
  assert.equal(entry.x, u.x); assert.equal(entry.elevation, 38); assert.equal(r._corpseStats.pending, 0);
  return { settleAfterLastMotion: true, finalPositionRetained: true, deckElevationRetained: 38 };
});

test('Huge battlefields fit in the camera while terrain and minimap use bounded reusable surfaces', () => {
  const r = renderer([]); r.map = { id: 'custom', biome: 'grassland', seed: 1, regions: [] };
  r.theme = { ground: '#456', patch: ['#456'], grass: '#789' }; r._buildTerrain();
  assert(r._terrain.width <= 3072 && r._terrain.height <= 3072); assert(r._terrain.width * r._terrain.height < 4610000);
  r.fit(); assert(r.camera.zoom < .1); assert(r.engine.width * r.camera.zoom <= r.width); assert(r.engine.height * r.camera.zoom <= r.height);
  const mini = canvas(); mini.width = 270; mini.height = 180;
  r.renderMinimap(mini, new Set()); const first = r._minimapBase.canvas;
  r.renderMinimap(mini, new Set()); assert.equal(r._minimapBase.canvas, first, 'Unchanged minimap scaled the large terrain source again');
  r.engine.terrainRevision = 1; r.renderMinimap(mini, new Set()); assert.notEqual(r._minimapBase.canvas, first, 'A siege change left stale minimap masonry');
  return { world: [16000, 11000], terrainBitmap: [r._terrain.width, r._terrain.height], terrainPixels: r._terrain.width * r._terrain.height, fittedZoom: r.camera.zoom, minimapReused: true };
});

console.log(JSON.stringify({ passed: results.filter(r => r.passed).length, failed: results.filter(r => !r.passed).length, tests: results }, null, 2));
if (results.some(r => !r.passed)) process.exitCode = 1;
