'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { SCENARIOS, loadEngine, setupScenario } = require('./performance-harness.cjs');

test('one thousand blocked followers share recovery work rather than each queueing A*', () => {
  const c = loadEngine(), world = new c.ATSWorld('thousand-shared-recoveries');
  world.ensure = () => {};
  world.terrain = () => ({ biome: 'forest', water: false, road: false });
  const wall = { id: 'blocked-route', x: 120, y: 0, w: 20, h: 500, r: 10, hp: 100, solid: true, collision: 'rect' };
  world.objects.set(wall.id, wall); world._indexObject(wall);
  const nav = new c.ATSNavigation(world), goal = { x: 350, y: 0 };
  nav.beginFrame(0);
  for (let i = 0; i < 1000; i++) nav.recoveryPath({ x: i % 20 * 3, y: Math.floor(i / 20) % 20 * 3 }, goal, 10, 3);
  assert.equal(nav.pending.size, 1, 'spatially adjacent followers request one corridor');
  assert.equal(nav.stats.requested, 1);
  let route = null;
  for (let frame = 1; frame < 600 && !route; frame++) {
    nav.beginFrame(frame / 60, { budgetMs: 100 });
    route = nav.recoveryPath({ x: 1, y: 1 }, goal);
    assert.ok(nav.stats.frameExpanded <= 192);
    assert.ok(nav.stats.frameSearches <= 3);
  }
  assert.ok(route?.length > 1, 'the shared job finds an actual route around the live wall');
  assert.equal(nav.stats.searches, 1);
  for (let i = 1; i < route.length; i++) assert.equal(nav.clearSegment(route[i - 1].x, route[i - 1].y, route[i].x, route[i].y, 10), true);
});

test('charge and recall reach nearby followers immediately with a thousand real apes', () => {
  const g = setupScenario(loadEngine(), SCENARIOS.find(s => s.id === 'I'));
  const nearby = g.apes.find(a => a.state === 'follow');
  g.king.x = nearby.x + 60; g.king.y = nearby.y;
  g.update(1 / 60, {});
  assert.equal(nearby._simTier, 0);
  assert.equal(g.command('charge', { x: 1, y: 0 }), true);
  assert.equal(nearby.state, 'charge', 'commands change behavior in their issuing tick');
  assert.equal(g.apes.filter(a => a.state === 'charge').length, 650);
  assert.equal(g.apes.filter(a => a.state === 'settled').length, 350);
  for (let frame = 0; frame < 31; frame++) g.update(1 / 60, {});
  assert.equal(g.command('recall'), true);
  assert.equal(nearby.state, 'follow');
  assert.ok(nearby.retreatUntil > g.time);
  assert.equal(g.population, 1000);
});

function audio() {
  const c = vm.createContext({ console, Math, Map, Set }); c.window = c;
  vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'audio.js'), 'utf8'), c);
  const a = new c.ATSAudio();
  assert.equal(a.ctx, null, 'construction does not autoplay');
  a.ctx = { currentTime: 10, state: 'running' };
  a.windGain = { gain: { setTargetAtTime() {} } };
  a.windFilter = { frequency: { setTargetAtTime() {} } };
  a.insectTime = a.beatTime = 100;
  return a;
}

test('mortar launch, impact and convergence cues are procedural and throttled', () => {
  const a = audio(), voices = [];
  a._tone = (...v) => voices.push(['tone', ...v]);
  a._noise = (...v) => voices.push(['noise', ...v]);
  for (const cue of ['mortar', 'mortarImpact', 'distantGun', 'converge', 'offensive', 'sirens']) {
    const start = voices.length; a.play(cue, .6, -.5);
    assert.ok(voices.length > start, cue + ' produces audible voices');
    const end = voices.length; a.play(cue);
    assert.equal(voices.length, end, cue + ' has a replay throttle');
  }
});

test('war ambience announces real nearby forces with bounded sampling', () => {
  const a = audio(), heard = [];
  a.play = (...v) => heard.push(v); a._tone = a._noise = () => {};
  let reads = 0;
  const troops = new Proxy(Array.from({ length: 360 }, (_, i) => ({ x: 900 + i, y: 0, hp: 85, state: 'combat', reported: true, hasRadio: true, siteId: 'base' })), { get(target, key) { if (/^\d+$/.test(String(key))) reads++; return target[key]; } });
  const g = { king: { x: 0, y: 0 }, humans: troops, warIntensity: 5, vehicles: [{ kind: 'tank', hp: 1100, x: 1200, y: 0 }], helis: [], world: { terrain: () => ({ biome: 'forest' }), sites: new Map([['base', { x: 1500, y: 0, alarm: true }]]) } };
  a.update(g, 1 / 60);
  assert.ok(reads <= 56, '24 danger samples plus 32 war samples bound the work');
  assert.ok(heard.some(v => v[0] === 'tank'), 'approaching armor is audible before reaching melee');
  assert.ok(heard.some(v => v[0] === 'distantGun'));
  assert.ok(heard.some(v => v[0] === 'converge'));
  assert.ok(heard.some(v => v[0] === 'sirens'));
});

test('ordinary war sounds leave eight voices available for warnings and commands', () => {
  const a = audio(), limits = [];
  a._tone = a._noise = () => limits.push(a._voiceLimit());
  a.play('gun');
  assert.ok(limits.every(limit => limit === 26));
  limits.length = 0;
  a.play('mortar'); a.play('recall');
  assert.ok(limits.length > 0 && limits.every(limit => limit === 34));
  assert.equal(a._voiceLimit(), 26, 'ambience resumes its bounded voice allowance');
});
