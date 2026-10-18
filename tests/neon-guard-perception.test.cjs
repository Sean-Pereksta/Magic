'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const AI = require('../game/neon-arena/neon-tactical-ai.js');
function fixture(angle = 0, options = {}) {
  let time = 0, clear = true;
  const bot = { entityId: 'guard', x: 0, y: 1.86, z: 0, yaw: 0, faceYaw: 0, hp: 100, maxHp: 100, team: 1, style: 'rifler' };
  const target = { entityId: 'player', x: Math.sin(angle * Math.PI / 180) * 20, y: 1.86, z: Math.cos(angle * Math.PI / 180) * 20, vx: 0, vy: 0, vz: 0, hp: 100, maxHp: 100, team: 2 };
  const detections = [], transitions = [], host = { time: () => time, random: () => .5, actors: () => [bot, target], sameTeam: (a, b) => a.team === b.team, lineClear: () => clear, canTraverse: () => true, pointInfo: () => ({ walkable: true }), navigation: () => [], onDetection: (a, e) => detections.push(e), onStateChange: (a, s) => transitions.push(s), ...options };
  const controller = AI.createController(host);
  return { bot, target, detections, transitions, host, controller, tick(dt = .13) { time += dt; return controller.update(bot, dt); }, update: () => controller.update(bot, .016), visible: value => { clear = value; } };
}

test('central vision recognizes instantly at ±35 degrees but shooting keeps human reaction time', () => {
  for (const angle of [-35, 0, 35]) {
    const f = fixture(angle), state = f.update(); assert.equal(state.enemy, f.target); assert.equal(state.guardState, 'alert'); assert.equal(state.aim.fire, false);
    assert.equal(f.detections[0].kind, 'sight'); assert.equal(f.detections[0].confirmed, true);
  }
});

test('stationary and slowly crouching opponents remain unidentified in peripheral vision', () => {
  for (const angle of [-95, -70, 35.1, 70, 95]) {
    const f = fixture(angle); f.target.vx = .4; f.target.crouching = true;
    for (let i = 0; i < 40; i++) { const state = f.tick(); assert.equal(state.enemy, null); assert.equal(state.memory, null); }
    assert.equal(f.detections.length, 0);
  }
});

test('running in the peripheral cone becomes suspicious immediately then identifies sustained motion', () => {
  const f = fixture(70); f.target.vx = 12;
  let result = f.update(); assert.equal(result.enemy, null); assert.equal(result.guardState, 'suspicious'); assert.ok(result.suspicion > 0);
  for (let i = 0; i < 10 && !result.enemy; i++) result = f.tick();
  assert.equal(result.enemy, f.target); assert.equal(result.memory.source, 'sight');
});

test('ordinary crossing motion accumulates while the far side of ±95 degrees stays invisible', () => {
  const f = fixture(-85); f.target.vz = 3;
  let seen = false; for (let i = 0; i < 20; i++) seen ||= !!f.tick().enemy;
  assert.equal(seen, true);
  for (const angle of [-95.1, 95.1, 180]) { const hidden = fixture(angle); hidden.target.vx = 20; hidden.target.vy = 8; for (let i = 0; i < 20; i++) assert.equal(hidden.tick().enemy, null); }
});

test('occluding walls block peripheral suspicion and central recognition alike', () => {
  for (const angle of [0, 65]) { const f = fixture(angle); f.target.vx = 20; f.visible(false); for (let i = 0; i < 20; i++) { const state = f.tick(); assert.equal(state.enemy, null); assert.equal(state.suspicion, 0); } }
});

test('live contact ends immediately when target leaves view and remembered position never follows', () => {
  const f = fixture(); const start = f.update(); assert.equal(start.memory.z, 20);
  f.target.x = 0; f.target.z = -30;
  const lost = f.update(); assert.equal(lost.enemy, null); assert.equal(lost.aim, null); assert.equal(lost.memory.z, 20); assert.equal(lost.guardState, 'investigate');
  f.visible(false); f.target.x = 100; f.target.z = 100;
  assert.equal(f.tick().memory.x, 0);
  for (let i = 0; i < 90; i++) f.tick();
  assert.equal(f.update().memory, null); assert.ok(f.transitions.includes('search')); assert.ok(f.transitions.includes('regroup')); assert.equal(f.update().guardState, 'patrol');
});

test('gunfire conveys approximate location with obstruction attenuation and no target reference', () => {
  const f = fixture(180); f.visible(false);
  f.controller.hear({ kind: 'gunfire', x: 10, y: 1.86, z: 10, team: 2, source: f.target });
  const result = f.update(); assert.equal(result.guardState, 'alert'); assert.equal(result.enemy, null); assert.equal(result.aim, null);
  assert.equal(result.memory.id, null); assert.equal(result.memory.uncertainty, 3.5); assert.notEqual(result.memory.x, 10);
  const beyondWall = fixture(180); beyondWall.visible(false); beyondWall.controller.hear({ kind: 'gunfire', x: 30, y: 1.86, z: 0, team: 2 }); assert.equal(beyondWall.update().memory, null);
});

test('suppression and quiet reloads have much smaller hearing distance than unsuppressed fire', () => {
  for (const kind of ['suppressed', 'reload', 'footstep']) {
    const f = fixture(180); f.controller.hear({ kind, x: 20, y: 1.86, z: 0, team: 2 }); assert.equal(f.update().memory, null, kind);
  }
  const loud = fixture(180); loud.controller.hear({ kind: 'gunfire', x: 20, y: 1.86, z: 0, team: 2 }); assert.ok(loud.update().memory);
  const quiet = fixture(180); quiet.controller.hear({ kind: 'gunfire', suppressed: true, range: 48, x: 20, y: 1.86, z: 0, team: 2 }); assert.equal(quiet.update().memory, null);
});

test('footsteps cause suspicion and fallen allies can alert their own team', () => {
  const f = fixture(180); f.controller.hear({ kind: 'footstep', x: 3, y: 1.86, z: 0, team: 2 }); assert.equal(f.update().guardState, 'suspicious');
  const ally = fixture(180); ally.controller.hear({ kind: 'fallen-ally', x: 4, y: 1.86, z: 2, team: 1 }); assert.equal(ally.update().guardState, 'alert');
});

test('patrol routes and squad objective orders drive destinations without opponent tracking', () => {
  const f = fixture(180, { patrolRoute: () => [{ x: 10, y: 1.86, z: 0 }, { x: 10, y: 1.86, z: 10 }] });
  assert.equal(f.update().goal.x, 10);
  f.bot.x = 10; f.bot.z = 0; f.tick(.5); assert.equal(f.update().goal.z, 10);
  for (const role of ['attack', 'defend', 'reinforce', 'flank', 'overwatch']) {
    const squad = fixture(180, { objective: () => ({ x: 30, y: 1.86, z: 30, role, radius: 6 }) });
    const result = squad.update(); assert.equal(result.squadRole, role); assert.equal(result.tactic, role); assert.ok(result.goal.x > 25 && result.goal.z > 25); assert.equal(result.enemy, null);
  }
});

test('perception and alert updates remain staggered, bounded and optional for legacy hosts', () => {
  const f = fixture(); f.update(); for (let i = 0; i < 100; i++) f.update(); assert.equal(f.detections.length, 1);
  for (let i = 0; i < 100; i++) f.controller.hear({ x: i, y: 1, z: 0, team: 2 });
  assert.equal(f.controller.stats().sounds, 64);
  const legacy = AI.createController({ time: () => 0 }); assert.doesNotThrow(() => legacy.update(f.bot, .016));
});
