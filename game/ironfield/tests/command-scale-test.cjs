'use strict';
// Run: node tests/command-scale-test.cjs [alternate-project-root]
// Five bounded command phases on the natural 12,000-soldier Epic deployment.
// Reports real soldier movement and synchronous order cost; not browser FPS.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
const { performance } = require('node:perf_hooks');
const root = process.argv[2] || path.resolve(__dirname, '..');
const enginePath = path.join(root, 'src/engine.js');
const sha = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const engineSHA = sha(enginePath);
global.window = global;
require(path.join(root, 'src/data.js'));
if (fs.existsSync(path.join(root, 'src/tactics.js'))) require(path.join(root, 'src/tactics.js'));
for (const name of ['impacts', 'siege']) {
  const file = path.join(root, 'src', name + '.js');
  if (require('node:fs').existsSync(file)) require(file);
}
require(enginePath);
const cfg = JSON.parse(JSON.stringify(GameData.PRESETS.find(p => p.id === 'epic')));
const e = new BattleEngine(cfg);
assert.equal(e.units.length, 12000);
const blue = e.regiments.filter(r => r.team === 0);
const ids = blue.map(r => r.id);
const frontX = Math.max(...blue.filter(r => !GameData.TYPES[r.type].ranged && !GameData.TYPES[r.type].commander).map(r => r.x));
const front = blue.filter(r => r.x >= frontX - 90 && !GameData.TYPES[r.type].ranged && !GameData.TYPES[r.type].commander);
const frontIds = front.map(r => r.id), frontSet = new Set(frontIds);
const outcomes = [];
const round = x => Math.round(x * 100) / 100;
const median = a => { const s = a.slice().sort((a, b) => a - b); return s.length ? s[Math.floor(s.length / 2)] : 0; };
const mean = a => a.reduce((a, b) => a + b, 0) / Math.max(1, a.length);
const center = regs => {
  const units = regs.flatMap(r => r.unitIds).map(id => e.units[id]).filter(u => u.alive && !u.routed);
  return { x: mean(units.map(u => u.x)), y: mean(units.map(u => u.y)) };
};
function positions() { return e.units.filter(u => u.alive && !u.routed && frontSet.has(u.regiment)).map(u => ({ id: u.id, regiment: u.regiment, x: u.x, y: u.y, angle: u.angle })); }
function world() {
  let invalid = 0, impassable = 0, outside = 0;
  for (const u of e.units) {
    if (![u.x, u.y, u.hp].every(Number.isFinite)) invalid++;
    if (!u.alive || u.routed) continue;
    if (e.terrainAt(u.x, u.y).blocked) impassable++;
    if (u.x < 0 || u.x > e.width || u.y < 0 || u.y > e.height) outside++;
  }
  const stats = e.getStats();
  for (const s of stats.teams) assert.equal(s.initial, s.alive + s.dead + s.routed);
  return { invalid, impassable, outside, alive: stats.teams.reduce((sum, s) => sum + s.alive, 0) };
}
function step(seconds) { const begin = performance.now(); for (let i = 0; i < Math.round(seconds * 30); i++) e.step(1 / 30); return performance.now() - begin; }
function exercise(name, commandIds, order, x, y, options) {
  const before = positions();
  const began = performance.now();
  const accepted = e.command(commandIds, order, x, y, options);
  const commandMilliseconds = performance.now() - began;
  assert.equal(accepted, commandIds.length, name + ' did not accept every healthy formation');
  assert(commandIds.every(id => e.getRegiment(id).order === order), name + ' lost the accepted order');
  assert(commandIds.every(id => e.getRegiment(id).commandStatus !== 'automatic'), name + ' exposed stale automatic status');
  const goals = new Map(front.map(r => [r.id, { x: r.commandX, y: r.commandY, targetId: r.commandTargetId }]));
  for (const p of before) assert(e.units[p.id].x === p.x && e.units[p.id].y === p.y, 'Command teleported an individual');
  const simulationMilliseconds = step(1);
  const samples = before.map(p => {
    const u = e.units[p.id], goal = goals.get(p.regiment), dx = goal.x - p.x, dy = goal.y - p.y;
    const distance = Math.hypot(dx, dy), mx = u.x - p.x, my = u.y - p.y;
    return { regiment: p.regiment, distance: Math.hypot(mx, my), projection: distance ? (mx * dx + my * dy) / distance : 0, attacking: u.state === 'attack' };
  });
  const perRegiment = front.map(r => ({ id: r.id, name: r.name, averageProgress: round(mean(samples.filter(p => p.regiment === r.id).map(p => p.projection))) }));
  const safety = world();
  const result = { name, order, commandedFormations: commandIds.length, accepted, commandMilliseconds: round(commandMilliseconds),
    simulatedSeconds: 1, simulationMilliseconds: round(simulationMilliseconds), observedSoldiers: samples.length,
    averageDisplacement: round(mean(samples.map(s => s.distance))), medianDisplacement: round(median(samples.map(s => s.distance))),
    averageProgress: round(mean(samples.map(s => s.projection))), medianProgress: round(median(samples.map(s => s.projection))),
    forwardSoldiers: samples.filter(s => s.projection > 1).length, attackingSoldiers: samples.filter(s => s.attacking).length,
    advancingFormations: perRegiment.filter(r => r.averageProgress > 2).length, observedFormations: front.length,
    commandStatuses: Object.fromEntries([...new Set(front.map(r => r.commandStatus))].map(status => [status, front.filter(r => r.commandStatus === status).length])),
    perRegiment, safety };
  outcomes.push(result);
  assert.equal(safety.invalid + safety.impassable + safety.outside, 0, name + ' left invalid or blocked positions');
  if (order === 'hold') assert(result.medianDisplacement < 5, 'Hold did not stop the representative front');
  else {
    assert(result.averageProgress > 2, name + ' did not make first-second progress');
    assert(result.advancingFormations >= Math.ceil(front.length / 2), name + ' left most front formations unresponsive');
  }
  if (options && Number.isInteger(options.targetRegimentId)) {
    assert(commandIds.every(id => e.getRegiment(id).commandTargetId === options.targetRegimentId), 'Explicit target commitment was lost');
    result.targetRegimentId = options.targetRegimentId;
    result.targetName = e.getRegiment(options.targetRegimentId).name;
  }
}

function retreatExercise() {
  const name = 'Army-wide Retreat withdraws immediately and keeps moving through repeated orders';
  const before = positions(), threat = center(e.regiments.filter(r => r.team === 1 && r.alive > 0));
  const began = performance.now(), accepted = e.command(ids, 'retreat');
  const commandMilliseconds = performance.now() - began;
  const goals = new Map(front.map(r => [r.id, { x: r.commandX, y: r.commandY }]));
  const initialTurn = front.map(r => r.commandTurnRemaining);
  const result = { name, order: 'retreat', commandedFormations: ids.length, accepted,
    commandMilliseconds: round(commandMilliseconds), observedSoldiers: before.length,
    observedFormations: front.length, simulatedSeconds: 0, initialMaximumTurn: round(Math.max(...initialTurn)) };
  outcomes.push(result);
  assert.equal(accepted, ids.length, 'Retreat did not accept all healthy formations');
  assert(front.every(r => r.commandStatus === 'withdrawing' && r.commandTurnRemaining === 0), 'Retreat failed to release the troops immediately');
  for (const p of before) {
    const u = e.units[p.id];
    assert(u.x === p.x && u.y === p.y && u.angle === p.angle, 'Issuing Retreat instantly moved or rotated a soldier');
  }
  const deltaAngle = (a, b) => Math.atan2(Math.sin(a - b), Math.cos(a - b));
  const spriteDirection = angle => ((Math.round(angle / (Math.PI / 4)) % 8) + 8) % 8;
  const turning = before.filter(p => {
    const goal = goals.get(p.regiment);
    return Math.abs(deltaAngle(Math.atan2(goal.y - p.y, goal.x - p.x), p.angle)) > .7;
  });
  assert(turning.length >= before.length * .5, 'The charge-to-retreat fixture no longer requires most observed troops to turn');
  const turnOrigins = new Map(turning.map(p => [p.id, p])), intermediateFacings = new Set();
  const stepChecked = ticks => {
    const start = performance.now();
    for (let tick = 0; tick < ticks; tick++) {
      const previous = positions(); e.step(1 / 30);
      for (const p of previous) {
        const u = e.units[p.id], limit = GameData.TYPES[u.type].speed / 30 * 1.8 + .25;
        assert(Math.hypot(u.x - p.x, u.y - p.y) <= limit, 'Retreat skipped its physical movement bound');
        if (u.state === 'fallBack') {
          assert(Math.abs(deltaAngle(u.angle, p.angle)) <= .85, 'Retreat snapped through most of its turn in one step');
          const origin = turnOrigins.get(u.id);
          if (origin) {
            const goal = goals.get(origin.regiment), finish = Math.atan2(goal.y - origin.y, goal.x - origin.x);
            if (spriteDirection(u.angle) !== spriteDirection(origin.angle) && spriteDirection(u.angle) !== spriteDirection(finish)) intermediateFacings.add(u.id);
          }
        }
      }
    }
    return performance.now() - start;
  };
  let simulationMilliseconds = stepChecked(3);
  const turnMovement = before.map(p => Math.hypot(e.units[p.id].x - p.x, e.units[p.id].y - p.y));
  result.maximumMovementDuringFirstTenthSecond = round(Math.max(...turnMovement));
  result.troopsNeedingTurn = turning.length;
  const firstWithdrawal = mean(before.map(p => {
    const u = e.units[p.id], r = e.regiments[p.regiment];
    return (u.x - p.x) * r._retreatNX + (u.y - p.y) * r._retreatNY;
  }));
  result.meanWithdrawalDuringFirstTenthSecond = round(firstWithdrawal);
  assert(firstWithdrawal > .1, 'Soldiers did not start withdrawing on the fresh order');
  const withdrawal = front.map(r => [r._commandEpoch, r._fallbackTime, r._fallbackCount, r._withdrawGoalX, r._withdrawGoalY]);
  const repeatStart = performance.now(), repeatedAccepted = e.command(ids, 'retreat');
  result.repeatCommandMilliseconds = round(performance.now() - repeatStart);
  result.repeatedAccepted = repeatedAccepted;
  assert.equal(repeatedAccepted, ids.length, 'Repeated Retreat was rejected');
  assert.deepEqual(front.map(r => [r._commandEpoch, r._fallbackTime, r._fallbackCount, r._withdrawGoalX, r._withdrawGoalY]), withdrawal, 'Repeated Retreat restarted withdrawal');
  simulationMilliseconds += stepChecked(27);
  result.intermediateSpriteFacings = intermediateFacings.size;
  assert(intermediateFacings.size >= turning.length * .4, 'The short turn skipped visible intermediate facings');
  const measure = () => {
    const samples = before.map(p => {
      const u = e.units[p.id], goal = goals.get(p.regiment), dx = goal.x - p.x, dy = goal.y - p.y;
      const distance = Math.hypot(dx, dy), mx = u.x - p.x, my = u.y - p.y;
      const ax = p.x - threat.x, ay = p.y - threat.y, ad = Math.hypot(ax, ay);
      return { regiment: p.regiment, distance: Math.hypot(mx, my), projection: distance ? (mx * dx + my * dy) / distance : 0,
        away: ad ? (mx * ax + my * ay) / ad : 0 };
    });
    const perRegiment = front.map(r => ({ id: r.id, name: r.name, averageProgress: round(mean(samples.filter(p => p.regiment === r.id).map(p => p.projection))) }));
    return { averageProgress: round(mean(samples.map(p => p.projection))), medianProgress: round(median(samples.map(p => p.projection))),
      averageAwayFromThreat: round(mean(samples.map(p => p.away))), advancingFormations: perRegiment.filter(r => r.averageProgress > 2).length,
      forwardSoldiers: samples.filter(p => p.projection > 1).length, perRegiment };
  };
  const first = measure(), firstSafety = world();
  result.firstSecond = first;
  assert(front.every(r => r.commandStatus === 'withdrawing' && r.commandTurnRemaining === 0), 'Turn failed to yield to actual withdrawal within one second');
  assert(first.averageProgress > 2 && first.averageAwayFromThreat > 2 && first.advancingFormations >= Math.ceil(front.length / 2), 'Retreat did not make useful first-second progress away from the enemy');
  assert.equal(firstSafety.invalid + firstSafety.impassable + firstSafety.outside, 0, 'Retreat first response left invalid positions');
  assert.equal(e.command(ids, 'retreat'), ids.length, 'Retreat was refused after the first turn finished');
  assert(front.every(r => r.commandStatus === 'withdrawing' && r.commandTurnRemaining === 0), 'Repeating Retreat during withdrawal forced another turn');
  simulationMilliseconds += stepChecked(30);
  const final = measure(), safety = world();
  Object.assign(result, final, { simulatedSeconds: 2, simulationMilliseconds: round(simulationMilliseconds), safety,
    commandStatuses: Object.fromEntries([...new Set(front.map(r => r.commandStatus))].map(status => [status, front.filter(r => r.commandStatus === status).length])) });
  assert(final.averageProgress > 10 && final.averageAwayFromThreat > 10 && final.advancingFormations >= Math.ceil(front.length / 2), 'Repeated orders prevented sustained withdrawal');
  assert.equal(safety.invalid + safety.impassable + safety.outside, 0, 'Retreat left invalid or blocked positions');
}
e.start();
step(1);
let error = null;
try {
  exercise('Army-wide automatic plan to immediate Charge', ids, 'charge');
  exercise('Army-wide Hold interrupts the charge', ids, 'hold');
  const now = center(blue);
  exercise('Army-wide Move reverses away from the enemy', ids, 'move', now.x - 500, now.y);
  const target = e.regiments.filter(r => r.team === 1 && r.alive > 0 && !GameData.TYPES[r.type].commander)
    .sort((a, b) => Math.abs(a.y - e.height / 2) + a.x * .05 - (Math.abs(b.y - e.height / 2) + b.x * .05))[0];
  exercise('Front formations reverse again toward an explicit Charge target', frontIds, 'charge', target.x, target.y, { targetRegimentId: target.id });
  retreatExercise();
  assert.equal(sha(enginePath), engineSHA, 'Engine source changed during scale exercise');
} catch (err) { error = err.stack; }
console.log(JSON.stringify({ passed: !error, engineSHA, config: cfg.id, map: cfg.map, seed: cfg.seed, units: e.units.length,
  formations: e.regiments.length, commandedArmyFormations: blue.length, observedFrontFormations: front.length,
  observedFrontSoldiers: positions().length, elapsedSimulationSeconds: round(e.time), outcomes, ...(error ? { error } : {}) }, null, 2));
if (error) process.exitCode = 1;
