'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const source = fs.readFileSync(path.resolve(__dirname, '../game/neon-arena/neon-arena.js'), 'utf8');
function harness() {
  const p1 = { entityId: 'p1', team: 1, x: 0, y: 1.86, z: 0 }, p2 = { entityId: 'p2', team: 2, x: 0, y: 1.86, z: 0 }, calls = [];
  const context = { game: { mode: 'tdm' }, player: p1, viewEnt: p1, PLAYER_H: 1.86, sameTeam: (a, b) => a.team === b.team,
    actorEyePosition: a => a, actorStandingHeight: () => 2.39, humanTargetY: (a, kind) => a.y + (kind === 'head' ? .24 : -.7),
    lineClear: () => true, project: (x, y) => ({ x, y }),
    ctx: Object.fromEntries(['save', 'restore', 'beginPath', 'closePath', 'fill', 'stroke', 'moveTo', 'lineTo'].map(name => [name, (...args) => calls.push([name, ...args])])) };
  const policy = source.slice(source.indexOf('function allyHighlightForView('), source.indexOf('function renderImportedModels('));
  const marker = source.slice(source.indexOf('function drawBotLabelIfVisible('), source.indexOf('function fpAxes('));
  vm.createContext(context); vm.runInContext(policy + '\n' + marker, context);
  return { context, p1, p2, calls };
}
test('TDM friendly highlighting is relative to each local view and never marks self, enemies or other modes', () => {
  const { context: c, p1, p2 } = harness(), blue = { team: 1 }, red = { team: 2 };
  assert.equal(c.allyHighlightForView(blue, p1), true); assert.equal(c.allyHighlightForView(red, p1), false);
  assert.equal(c.allyHighlightForView(red, p2), true); assert.equal(c.allyHighlightForView(blue, p2), false);
  assert.equal(c.allyHighlightForView(p1, p1), false); assert.equal(c.allyHighlightForView({ ...blue, dead: true }, p1), false);
  assert.equal(c.allyHighlightForView({ ...blue, alien: true }, p1), false);
  for (const mode of ['dm', 'frag', 'siege', 'survivor']) { c.game.mode = mode; assert.equal(c.allyHighlightForView(blue, p1), false); }
});
test('friendly markers require a visible body and an unoccluded marker ray, with no floating name or weapon text', () => {
  const { context: c, calls } = harness(), ally = { team: 1, x: 2, y: 1.86, z: 10 }, enemy = { ...ally, team: 2 };
  c.drawBotLabelIfVisible(enemy); assert.equal(calls.length, 0);
  c.lineClear = () => false; c.drawBotLabelIfVisible(ally); assert.equal(calls.length, 0, 'walls hide the marker');
  c.lineClear = (x, y, z, bx, by) => by < 2.4; c.drawBotLabelIfVisible(ally); assert.equal(calls.length, 0, 'a roof also hides a marker above an otherwise visible body');
  c.lineClear = () => true; c.drawBotLabelIfVisible(ally); assert.equal(calls.filter(call => call[0] === 'stroke').length, 1);
  assert.equal(c.ctx.strokeStyle, '#65f4ff'); assert.ok(!/fillText|WEAPONS|b\.name/.test(source.slice(source.indexOf('function drawBotLabelIfVisible('), source.indexOf('function fpAxes('))));
});
test('the imported model draw receives the current-view friendly policy', () => {
  const draw = source.slice(source.indexOf('function renderImportedModels('), source.indexOf('function cameraClipPoint('));
  assert.match(draw, /allyHighlight:allyHighlightForView\(item\.actor\)/);
});
