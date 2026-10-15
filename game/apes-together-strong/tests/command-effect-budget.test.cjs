'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { loadEngine } = require('./performance-harness.cjs');

test('a distant command confirmation replaces routine effects at saturation and remains protected under the hard cap', () => {
  const c = loadEngine();
  vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'command-animation.js'), 'utf8'), c);
  const g = new c.ATSGame('COMMAND-EFFECT-BUDGET');
  const a = g.makeApe(10, 10, 'follow'); a.species = 'gorilla'; g.syncIndexes();
  const target = g.makeHuman(700, 0, null), budget = 400 - g.performance.qualityLevel * 35;
  for (let i = 0; i < budget; i++) g.effect('hit', 1000 + i * 40, 1000, { life: 10 });
  assert.equal(g.effects.length, budget);
  assert.equal(g.siege.issue(['gorilla'], [{ id: 'far-attack', type: 'attack', x: 700, y: 0, targetId: target.id, targetKind: 'human' }]), true);
  const confirmation = g.effects.find(e => e.type === 'command');
  assert.ok(confirmation, 'the normal 700px target still receives its group confirmation');
  assert.equal(confirmation.targetId, target.id); assert.equal(confirmation.x, 700);
  assert.equal(g.effects.length, budget, 'the command replaces a routine impact');
  class Renderer { draw(game) { this.time = game.time; this.drawEffects(this.ctx, game.effects); } drawEffects() {} }
  c.ATSRenderer = Renderer;
  vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'command-visuals.js'), 'utf8'), c);
  const renderer = new Renderer(), locations = [];
  renderer.ctx = {}; renderer.drawCommandMarker = (kind, x, y) => { locations.push({ kind, x, y }); return true; };
  renderer.draw(g); assert.deepEqual(locations.at(-1), { kind: 'attack', x: 700, y: 0 });
  target.x = 717; renderer.draw(g);
  assert.deepEqual(locations.at(-1), { kind: 'attack', x: 717, y: 0 }, 'the distant standard follows the live enemy rather than its stale issued coordinates');
  for (let i = 0; i < budget + 100; i++) g.effect('explosion', 2000 + i * 40, 1000, { life: 10 });
  assert.ok(g.effects.includes(confirmation), 'critical combat effects cannot evict the command');
  assert.equal(g.effects.length, budget + 48, 'command priority retains the established hard effect cap');
  g.effect('hit', 900, 900, { life: 10 });
  assert.equal(g.effects.length, budget + 48); assert.ok(g.effects.includes(confirmation));
});
