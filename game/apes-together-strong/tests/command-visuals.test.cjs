'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { loadEngine } = require('./performance-harness.cjs');
const source = name => fs.readFileSync(path.join(__dirname, '..', name + '.js'), 'utf8');
function recording() {
  const calls = [], stack = []; let alpha = 1;
  const c = { calls,
    get globalAlpha() { return alpha; }, set globalAlpha(value) { alpha = value; },
    save() { stack.push(alpha); calls.push(['save']); }, restore() { assert.ok(stack.length); alpha = stack.pop(); calls.push(['restore']); },
    drawImage(...args) { calls.push(['drawImage', ...args, alpha]); },
    beginPath() { calls.push(['beginPath']); }, closePath() { calls.push(['closePath']); },
    moveTo(...a) { calls.push(['moveTo', ...a]); }, lineTo(...a) { calls.push(['lineTo', ...a]); },
    fill() { calls.push(['fill', this.fillStyle]); }, stroke() { calls.push(['stroke', this.strokeStyle]); },
    fillRect(...a) { calls.push(['fillRect', ...a]); },
    translate(...a) { calls.push(['translate', ...a]); }, scale(...a) { calls.push(['scale', ...a]); },
    rotate(...a) { calls.push(['rotate', ...a]); }, transform(...a) { calls.push(['transform', ...a]); },
    arc() { throw Error('A command marker painted an abstract circle'); }, ellipse() { throw Error('A command marker painted an abstract ring'); }
  };
  return { c, calls, stack };
}
function harness() {
  const canvases = [], drawing = recording();
  class Renderer {
    draw(game) { this.time = game.time; this.drawEffects(this.ctx, game.effects || []); }
    drawEffects(c, effects) { this.ordinary = effects; }
    project(x, y, height = 0) { return { x: 400 + (x - y) * .8, y: 300 + (x + y) * .42 - height }; }
    visible() { return true; }
  }
  const document = { createElement(type) { assert.equal(type, 'canvas'); const d = recording(), canvas = { getContext: () => d.c, recording: d }; canvases.push(canvas); return canvas; } };
  const c = vm.createContext({ ATSRenderer: Renderer, document, console }); c.window = c;
  vm.runInContext(source('command-animation'), c); vm.runInContext(source('character-art'), c); vm.runInContext(source('command-visuals'), c);
  const r = new Renderer(); Object.assign(r, { ctx: drawing.c, time: 0, camera: { zoom: 1 }, detailLevel: 0 });
  return { ...drawing, c, r, canvases };
}
function actor(fields = {}) { return { id: 'ape', species: 'gorilla', x: 0, y: 0, hp: 100, maxHp: 100, state: 'follow', ...fields }; }

test('seven commands paint distinct reusable physical art with a stable terrain anchor', () => {
  const h = harness(), visual = h.c.ATSCommandVisuals, assets = new Set(), signatures = new Set();
  for (const kind of visual.kinds) {
    h.r.drawCommandMarker(kind, 80, 40);
    const draw = h.calls.filter(a => a[0] === 'drawImage').at(-1), canvas = draw[1]; assets.add(canvas);
    signatures.add(JSON.stringify(canvas.recording.calls));
    assert.equal(canvas.width, 96); assert.equal(canvas.height, 112);
    assert.ok(canvas.recording.calls.some(a => a[0] === 'translate' && a[1] === 48 && a[2] === 96));
    assert.equal(canvas.recording.stack.length, 0);
  }
  assert.equal(assets.size, 7); assert.equal(signatures.size, 7, 'every standard has a distinct cloth/emblem');
  assert.equal(h.stack.length, 0);
  const before = h.canvases.length; h.r.commandMarkerDraws = 0;
  for (const kind of visual.kinds) h.r.drawCommandMarker(kind, -90, 120);
  assert.equal(h.canvases.length, before, 'new positions reuse cached sprites');
});

test('only commands replace waves; combat, death, gate and distraction shockwaves retain their original renderer', () => {
  const h = harness(), combat = { type: 'wave', x: 9, y: 12, life: .8, maxLife: 1, blastKind: 'grenade' }, impact = { type: 'hit', x: 4, y: 5, life: .2 };
  const tagged = { type: 'wave', commandKind: 'hold', x: 20, y: 20, life: .8, maxLife: 1 };
  h.r.draw({ time: 2, effects: [combat, impact, tagged] });
  assert.deepEqual(Array.from(h.r.ordinary), [combat, impact]); assert.equal(h.r.commandMarkerDraws, 1);
  assert.equal(combat.commandKind, undefined); assert.equal(h.stack.length, 0);
});

test('directional activation anchors the charge standard at its destination and target orders follow the real target', () => {
  const h = harness(), positions = [], original = h.r.drawCommandMarker;
  h.r.drawCommandMarker = function (kind, x, y, options) { positions.push({ kind, x, y }); return original.call(this, kind, x, y, options); };
  const enemy = { id: 'human-1', x: 120, y: -60, hp: 100 }, game = { time: 1, humansById: new Map([[enemy.id, enemy]]), effects: [
    { type: 'charge', commandKind: 'charge', x: 10, y: 20, dir: Math.PI / 2, life: .8, maxLife: 1 },
    { type: 'command', commandKind: 'attack', x: 1, y: 1, targetId: enemy.id, targetKind: 'human', life: .9, maxLife: 1 }
  ] };
  h.r.draw(game); assert.ok(Math.abs(positions[0].x - 10) < 1e-6); assert.equal(positions[0].y, 590);
  assert.deepEqual(positions[1], { kind: 'attack', x: 120, y: -60 });
  enemy.x += 17; positions.length = 0; h.r.draw(game); assert.equal(positions[1].x, 137);
});

test('a 1,000-ape charge shares bounded markers, dust cells and caches without creating particles or changing orders', () => {
  const h = harness(), apes = Array.from({ length: 1000 }, (_, i) => actor({ id: 'ape-' + i, x: i * 3, y: i % 11 * 23, state: 'charge', moving: true, phase: 0 }));
  const effects = Array.from({ length: 100 }, (_, i) => ({ type: 'command', commandKind: 'charge', x: i * 3, y: 10, life: .9, maxLife: 1 }));
  const game = { time: 0, apes, effects }, before = JSON.stringify(game);
  h.r.drawEntries = apes.map(a => ({ type: 'ape', a })); h.r.draw(game);
  assert.ok(h.r.commandMarkerDraws <= 8); assert.ok(h.r.commandDustDraws <= 24); assert.ok(h.r.commandDustDraws > 0);
  assert.equal(JSON.stringify(game), before, 'visual feedback cannot alter movement, commands or effect lifetime');
  const created = h.canvases.length; for (let frame = 0; frame < 40; frame++) h.r.draw({ ...game, time: frame / 60 });
  assert.ok(h.canvases.length - created <= 3, 'only shared cloth frames are added'); assert.ok(h.c.ATSCommandVisuals.status().cachedSprites <= 31);
  h.r.reducedMotion = true; h.r.draw(game); assert.equal(h.r.commandDustDraws, 0, 'reduced motion keeps the standard and omits environmental motion');
  assert.equal(h.stack.length, 0);
});

test('selected troops and route confirmations use sparse ground marks and retain functional climb controls', () => {
  const h = harness(), apes = Array.from({ length: 500 }, (_, i) => actor({ id: 'a-' + i, x: i, armyOrder: { group: 'group', index: 1 } }));
  const orders = [{ id: 'old', type: 'attack', x: -200, y: 0 }, { id: 'current', type: 'hold', x: 30, y: 50 }];
  const before = JSON.stringify(apes), siege = { selected: ['gorilla'], selectedMembers: () => apes, groups: { gorilla: { id: 'group', orders } }, drafts: {}, stones: [], known: () => true };
  h.r.drawCommandSiege({ siege, king: { x: 0, y: 0 }, world: { getObjects: () => [{ type: 'gateControl', x: 1, y: 1 }, { type: 'wall', climbAccess: true, x: 2, y: 2 }] } });
  assert.equal(h.r.commandMarkerDraws, 1, 'completed route orders are not shown');
  assert.ok(h.calls.filter(a => a[0] === 'stroke').length < 65, 'selection is sampled rather than painting 500 repeated rings');
  assert.equal(JSON.stringify(apes), before); assert.equal(h.stack.length, 0);
});

test('current army orders override incidental hold state and switch defensive, travel and combat poses immediately', () => {
  const h = harness(), a = actor({ state: 'hold', armyOrder: { group: 'g', index: 0 } }), g = { time: 1, siege: { groups: { gorilla: { id: 'g', orders: [{ type: 'defend' }] } } } };
  const resolve = () => h.c.ATSCharacterArt.resolve(a, 1, { game: g });
  const before = JSON.stringify(a); assert.equal(h.c.ATSCommandAnimation.orderFor(a, g), 'defend');
  assert.equal(resolve().commandGuard, true); assert.equal(resolve().state, 'prepare');
  g.siege.groups.gorilla.orders[0].type = 'charge'; a.moving = true;
  assert.equal(resolve().state, 'sprint'); assert.equal(resolve().commandOrder, 'charge');
  g.siege.groups.gorilla.orders[0].type = 'retreat'; assert.equal(resolve().state, 'sprint'); assert.equal(resolve().commandOrder, 'retreat');
  a.animation = { kind: 'slam', start: .9, duration: .4 }; assert.equal(resolve().state, 'heavyAttack', 'real contact and recovery override travel');
  a.hitTimer = .2; assert.equal(resolve().state, 'hurt'); a.hp = 0; assert.equal(resolve().state, 'death');
  assert.equal(JSON.parse(before).armyOrder.index, a.armyOrder.index);
});

test('charge activation uses a brief authored war cry without delaying travel, and changing commands interrupts it', () => {
  const c = loadEngine(); vm.runInContext(source('command-animation'), c);
  c.ATSRenderer = class {}; vm.runInContext(source('character-art'), c);
  const g = new c.ATSGame('COMMAND-POSES'), a = g.makeApe(10, 10, 'follow'); g.syncIndexes(); g.apeGrid.rebuild([g.king, a]);
  const before = { x: a.x, y: a.y, hp: a.hp }; assert.equal(g.command('charge', { x: 1, y: 0 }), true);
  let pose = c.ATSCharacterArt.resolve(a, g.time, { game: g }); assert.equal(pose.event, 'warCry'); assert.equal(a.state, 'charge');
  assert.equal(a.target.x, g.king.x + 570); assert.deepEqual({ x: a.x, y: a.y, hp: a.hp }, before);
  assert.equal(g.effects.at(-1).commandKind, 'charge'); assert.equal(g.effects.at(-1).type, 'charge', 'legacy pooling identifiers remain compatible');
  g.commandCD = 0; assert.equal(g.command('hold'), true); pose = c.ATSCharacterArt.resolve(a, g.time, { game: g });
  assert.equal(pose.commandOrder, 'hold'); assert.equal(pose.commandCry, false); assert.equal(pose.commandGuard, true);
  a.retreatUntil = 5; a.state = 'follow'; a.recallOrder = { kind: 'recruit' }; assert.equal(c.ATSCommandAnimation.orderFor(a, g), 'follow');
  a.recallOrder.kind = 'field'; assert.equal(c.ATSCommandAnimation.orderFor(a, g), 'retreat');
  g.commandCD = 0; assert.equal(g.command('charge', { x: 0, y: 1 }), true);
  assert.equal(g.effects.at(-1).dir, Math.PI / 2, 'vertical command cues keep the exact issued heading');
});

test('mixed species issued to one target share one activation effect and keep immediate gameplay orders', () => {
  const c = loadEngine(); vm.runInContext(source('command-animation'), c); const g = new c.ATSGame('COMMAND-GROUPS');
  const first = g.makeApe(10, 10, 'follow'), second = g.makeApe(20, 20, 'follow'); first.species = 'gorilla'; second.species = 'gibbon'; g.syncIndexes();
  const order = { id: 'order-qa', type: 'attack', x: 120, y: 30, targetId: 'human-qa', targetKind: 'human' };
  assert.equal(g.siege.issue(['gorilla'], [order]), true); assert.equal(g.siege.issue(['gibbon'], [order]), true);
  assert.equal(g.effects.filter(e => e.type === 'command').length, 1);
  assert.equal(c.ATSCommandAnimation.orderFor(first, g), 'attack'); assert.equal(c.ATSCommandAnimation.orderFor(second, g), 'attack');
});

test('cached fallback hold artwork stays circle-free, and painted movement skips unused legacy gait work', () => {
  const context = vm.createContext({}); context.window = context;
  vm.runInContext(source('command-animation'), context); vm.runInContext(source('render'), context);
  const drawing = recording(), r = Object.create(context.ATSRenderer.prototype), a = actor({ state: 'hold', coatVariant: 0, age: 240, moving: false });
  Object.assign(r, { time: 1, camera: { x: 0, y: 0, zoom: 1 }, detailLevel: 0,
    apeSprites: new Map([['gorilla:0:false:6:hold', {}]]) });
  assert.equal(r.drawApeSprite(drawing.c, a), true);
  r.usesPaintedPrimate = () => true; r.drawPrimateGeometry = () => {}; r.primateTempo = () => { throw Error('painted art evaluated the unused procedural stride'); };
  r.drawApe(drawing.c, actor({ moving: true })); assert.equal(drawing.stack.length, 0);
});
