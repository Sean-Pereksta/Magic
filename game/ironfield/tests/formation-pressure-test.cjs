'use strict';
// Sustained contact must yield real whole ranks without weakening collision or command gates.
// Run: node tests/formation-pressure-test.cjs [alternate-project-root]
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { performance } = require('node:perf_hooks');
const DT = 1 / 30, FRONT = 2800, CENTER = 2200, COLS = 12, ROWS = 4, PITCH = 18;
const round = n => Math.round(n * 1000) / 1000;
const mean = xs => xs.reduce((s, n) => s + n, 0) / Math.max(1, xs.length);
const snapshot = e => e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp, alive: u.alive, angle: u.angle }));

function configuration(options = {}) {
  const sourceType = options.sourceType || 'guardian', type = options.type || 'militia';
  const sourceRows = options.sourceRows || ROWS, rows = options.rows || ROWS, sourceCols = options.sourceCols || COLS;
  const sourceCount = sourceRows * sourceCols, targetCount = rows * COLS + (options.outlier ? 1 : 0);
  const sourceOffsets = Array.from({ length: sourceCount }, (_, i) => ({ x: -Math.floor(i / sourceCols) * PITCH, y: (i % sourceCols - (sourceCols - 1) / 2) * PITCH }));
  if (options.distantSupport) for (let i = 2; i < sourceOffsets.length; i++) sourceOffsets[i].x -= 700;
  const targetOffsets = Array.from({ length: rows * COLS }, (_, i) => ({ x: Math.floor(i / COLS) * PITCH, y: (i % COLS - (COLS - 1) / 2) * PITCH }));
  if (options.outlier) targetOffsets.push({ x: 280, y: 230 });
  const regions = [];
  if (options.wall || options.corridor) regions.push({ type: 'rock', x: FRONT + (rows - 1) * PITCH + 14, y: CENTER - 180, w: 80, h: 360 });
  if (options.corridor) regions.push({ type: 'rock', x: FRONT - 180, y: CENTER - 200, w: 360, h: 86 },
    { type: 'rock', x: FRONT - 180, y: CENTER + 114, w: 360, h: 86 });
  const roster0 = { [sourceType]: sourceCount, commander: 1 }, roster1 = { [type]: targetCount, commander: 1 };
  if (options.backing) roster1.guardian = (roster1.guardian || 0) + COLS;
  const formations = [
    { team: 0, type: sourceType, count: sourceCount, x: FRONT - (options.distance || 42), y: CENTER, facing: 0, formation: 'line', offsets: sourceOffsets },
    { team: 0, type: 'commander', count: 1, x: 600, y: 600, facing: 0, formation: 'line' },
    { team: 1, type, count: targetCount, x: FRONT, y: CENTER, facing: Math.PI, formation: 'line', offsets: targetOffsets },
    { team: 1, type: 'commander', count: 1, x: 5500, y: 3900, facing: Math.PI, formation: 'line' }
  ];
  if (options.backing) formations.push({ team: 1, type: 'guardian', count: COLS, x: FRONT + (rows - 1) * PITCH + 16, y: CENTER,
    facing: Math.PI, formation: 'line', offsets: Array.from({ length: COLS }, (_, i) => ({ x: 0, y: (i - (COLS - 1) / 2) * PITCH })) });
  const armies = [{ name: 'Advancing Ranks', doctrine: 'balanced', roster: roster0 }, { name: 'Receiving Ranks', doctrine: 'defensive', roster: roster1 }];
  if (options.swapTeams) { for (const formation of formations) formation.team = 1 - formation.team; armies.reverse(); }
  return { map: 'custom', seed: options.seed || 7137, customMap: { name: 'The Pressed Line', biome: 'grassland', regions }, armies, formations };
}

function audit(e, before, metrics) {
  const D = global.GameData, buckets = new Map();
  for (const u of e.units) if (u.alive && !u.routed && !u.escaped) {
    const p = before[u.id], radius = D.TYPES[u.type].radius;
    assert([u.x, u.y, u.hp, u.angle].every(Number.isFinite), 'Formation pressure produced invalid physical state');
    assert(u.x >= radius && u.y >= radius && u.x <= e.width - radius && u.y <= e.height - radius, 'A yielding body left the battlefield');
    assert.equal(e.terrainAt(u.x, u.y).blocked, false, 'Formation pressure put a body in solid terrain');
    const travel = Math.hypot(u.x - p.x, u.y - p.y);
    metrics.maximumStep = Math.max(metrics.maximumStep, travel);
    assert(travel <= D.TYPES[u.type].speed * DT * 1.8 + .25, 'Sustained pressure exceeded the unchanged per-frame movement limit');
    for (const rock of e.map.regions) if (rock.type === 'rock') for (let part = 0; part <= 4; part++) {
      const x = p.x + (u.x - p.x) * part / 4, y = p.y + (u.y - p.y) * part / 4;
      const gap = Math.hypot(Math.max(rock.x - x, 0, x - rock.x - rock.w), Math.max(rock.y - y, 0, y - rock.y - rock.h));
      assert(gap >= radius - .1, 'A yielding body swept through a rock boundary');
    }
    const gx = Math.floor(p.x / 64), gy = Math.floor(p.y / 64);
    for (let y = gy - 1; y <= gy + 1; y++) for (let x = gx - 1; x <= gx + 1; x++) for (const v of buckets.get(x + ':' + y) || []) {
      const q = before[v.id], sum = radius + D.TYPES[v.type].radius;
      if (Math.abs(p.x - q.x) > sum + 16 || Math.abs(p.y - q.y) > sum + 16) continue;
      const dx = p.x - q.x, dy = p.y - q.y, vx = u.x - p.x - v.x + q.x, vy = u.y - p.y - v.y + q.y, vv = vx * vx + vy * vy;
      const at = vv ? Math.max(0, Math.min(1, -(dx * vx + dy * vy) / vv)) : 0;
      const ratio = Math.hypot(dx + vx * at, dy + vy * at) / sum;
      metrics.minimumBodyRatio = Math.min(metrics.minimumBodyRatio, ratio);
      assert(ratio >= .90, 'Formation pressure swept soldiers through occupied bodies');
    }
    const key = gx + ':' + gy; if (!buckets.has(key)) buckets.set(key, []); buckets.get(key).push(u);
  }
  for (const team of e.getStats().teams) assert.equal(team.initial, team.alive + team.dead + team.routed, 'Pressure lost or duplicated soldiers');
}

function trial(options = {}) {
  const e = new global.BattleEngine(configuration(options));
  const source = e.regiments[0], target = e.regiments.find(r => r.team !== source.team && r.type !== 'commander');
  e.command(e.regiments.filter(r => r.id !== source.id && !(options.autoDefense && r.id === target.id)).map(r => r.id), 'hold');
  if (options.equalAuto) e.command([target.id], 'auto');
  const initial = snapshot(e), metrics = { minimumBodyRatio: Infinity, maximumStep: 0 };
  const pressureTravel = new Float64Array(e.units.length), moved = new Set();
  let firstPressure = null, peakSpeed = 0, lastPressure = 0, postCommandPressure = 0, commandAt = null, commandAccepted = null, commandPositions = null, firstCommandStep = null, firstWithdrawal = 0;
  let tacticalHoldFrames = 0, largestTacticalAnchorError = 0, largestPressureReturn = 0;
  e.start();
  for (let tick = 0; tick < (options.seconds || 15) / DT && e.phase === 'running'; tick++) {
    const before = snapshot(e);
    e.step(DT); audit(e, before, metrics);
    if (target.order === 'auto' && target._tactic && target._tactic.hold) {
      tacticalHoldFrames++;
      largestTacticalAnchorError = Math.max(largestTacticalAnchorError, mean(target.unitIds.filter(id => e.units[id].alive && !e.units[id].routed)
        .map(id => Math.hypot(e.units[id].x - e.units[id]._tacticHoldX, e.units[id].y - e.units[id]._tacticHoldY))));
    }
    for (const id of target.unitIds) {
      const u = e.units[id];
      if (u._pressureTick === e._tick) {
        const along = u._pressureDX;
        const length = Math.hypot(u._pressureDX, u._pressureDY);
        largestPressureReturn = Math.max(largestPressureReturn, -((u.x - before[id].x) * u._pressureDX + (u.y - before[id].y) * u._pressureDY) / Math.max(.001, length));
        pressureTravel[id] += Math.max(0, along); if (along > .001) moved.add(id);
        if (firstPressure === null) firstPressure = e.time;
        lastPressure = e.time;
        if (commandAt !== null) postCommandPressure += Math.hypot(u._pressureDX, u._pressureDY);
      }
    }
    peakSpeed = Math.max(peakSpeed, target._formationPressureSpeed || 0);
    if (options.command && firstPressure !== null && e.time >= firstPressure + 2 && commandAt === null) {
      const receiver = options.commandReceiver ? target : source;
      const c = e._currentCenter(receiver);
      commandPositions = snapshot(e); commandAt = e.time;
      commandAccepted = e.command([receiver.id], options.command, c.x + (options.commandReceiver ? 550 : -550), c.y + (options.command === 'move' && !options.commandReceiver ? 600 : 0));
      assert.deepEqual(snapshot(e), commandPositions, 'A fresh command moved bodies before a physical step');
    } else if (commandAt !== null && firstCommandStep === null) {
      const receiver = options.commandReceiver ? target : source;
      firstWithdrawal = mean(receiver.unitIds.filter(id => e.units[id].alive && !e.units[id].routed).map(id =>
        (e.units[id].x - commandPositions[id].x) * (options.commandReceiver ? 1 : -1)));
      firstCommandStep = Math.max(...receiver.unitIds.map(id => Math.hypot(e.units[id].x - commandPositions[id].x, e.units[id].y - commandPositions[id].y)));
    }
    if (commandAt !== null && e.time >= commandAt + 3) break;
  }
  const rows = options.rows || ROWS;
  const living = target.unitIds.filter(id => e.units[id].alive && !e.units[id].routed && id !== target.unitIds[rows * COLS]);
  // Retain the last real positions of casualties: an emptied front rank
  // still has a measured physical history, not an invented zero-distance row.
  const rowDisplacements = Array.from({ length: rows }, (_, row) => mean(target.unitIds.slice(row * COLS, (row + 1) * COLS)
    .map(id => e.units[id].x - initial[id].x)));
  const rowPressureTravel = Array.from({ length: rows }, (_, row) => mean(target.unitIds.slice(row * COLS, (row + 1) * COLS).map(id => pressureTravel[id])));
  const result = { sourceType: source.type, targetType: target.type, firstPressure: firstPressure === null ? null : round(firstPressure),
    lastPressure: round(lastPressure), meanDisplacement: round(mean(living.map(id => e.units[id].x - initial[id].x))), rowDisplacements: rowDisplacements.map(round),
    rowPressureTravel: rowPressureTravel.map(round), displacedSoldiers: moved.size, alive: living.length, peakPressureSpeed: round(peakSpeed),
    minimumBodyRatio: round(metrics.minimumBodyRatio), maximumStep: round(metrics.maximumStep), targetOrder: target.order };
  if (options.outlier) {
    const id = target.unitIds[rows * COLS]; result.outlierDisplacement = round(Math.hypot(e.units[id].x - initial[id].x, e.units[id].y - initial[id].y));
    result.outlierPressureTravel = round(pressureTravel[id]);
  }
  if (options.autoDefense) { result.tacticalHoldFrames = tacticalHoldFrames; result.largestTacticalAnchorError = round(largestTacticalAnchorError); result.largestPressureReturn = round(largestPressureReturn); }
  if (options.command) {
    const receiver = options.commandReceiver ? target : source;
    result.command = options.command; result.commandAccepted = commandAccepted; result.commandAt = round(commandAt);
    result.postCommandPressure = round(postCommandPressure); result.firstCommandStep = round(firstCommandStep); result.firstWithdrawal = round(firstWithdrawal);
    result.commandedProgress = round(mean(receiver.unitIds.filter(id => e.units[id].alive && !e.units[id].routed).map(id => options.command === 'move' && !options.commandReceiver ?
      e.units[id].y - commandPositions[id].y : (e.units[id].x - commandPositions[id].x) * (options.commandReceiver ? 1 : -1))));
    result.sourceOrder = source.order; result.receiverFallingBack = receiver.fallingBack;
  }
  return result;
}

function register(test) {
  const cache = new Map(), run = options => { const key = JSON.stringify(options || {}); if (!cache.has(key)) cache.set(key, trial(options)); return cache.get(key); };
  test('Stronger supported infantry slowly displaces every connected enemy rank through real swept movement', () => {
    const outcomes = [run({}), run({ seed: 4719 })], swapped = run({ swapTeams: true });
    for (const result of [...outcomes, swapped]) {
      assert(result.firstPressure !== null && result.firstPressure < 6, 'Close stronger ranks failed to establish sustained contact pressure');
      assert(result.meanDisplacement > 20, 'The weaker formation never gave meaningful ground as a whole');
      assert(result.rowDisplacements.every(n => n > 12), 'Pressure moved only the front and left a rear rank anchored');
      assert(result.rowPressureTravel.every(n => n > 10), 'A rear rank shifted only through unrelated normal navigation');
      assert(result.displacedSoldiers >= ROWS * COLS * .8, 'Pressure affected too few connected bodies');
      assert(result.peakPressureSpeed > 1 && result.peakPressureSpeed <= 8.001, 'Formation yielding is absent or not gradual');
    }
    assert(swapped.meanDisplacement > outcomes[0].meanDisplacement * .65 && swapped.meanDisplacement < outcomes[0].meanDisplacement * 1.35,
      'Swapping team identifiers substantially changed which physical force prevailed');
    return { outcomes, swapped };
  });
  test('Equal lines, prepared pikes and hoplites, and heavy defenders resist formation pressure', () => {
    const outcomes = [];
    for (const type of ['guardian', 'pikeman', 'hoplite']) {
      const result = run({ type }); outcomes.push(result);
      assert(result.meanDisplacement < 4, 'An equal, heavy, or braced line drifted backward under sustained pressure');
      assert(result.rowPressureTravel.every(n => n < 2), 'Prepared resistance was only cosmetic');
    }
    const equal = run({ sourceType: 'swordsman', type: 'swordsman', equalAuto: true });
    assert(equal.rowPressureTravel.every(n => n < 2), 'Equal opposing active lines developed arbitrary pressure drift');
    return { outcomes, equal };
  });
  test('Nearby supporting ranks add sustained strength and automatic defensive anchors yield with their bodies', () => {
    const depth = run({ sourceType: 'swordsman', type: 'swordsman', sourceRows: 8, rows: 2 });
    assert(depth.meanDisplacement > 20 && depth.rowPressureTravel.every(n => n > 15), 'Extra coherent supporting ranks did not change the physical strength of an otherwise equal front');
    const automatic = run({ autoDefense: true, type: 'archer', seconds: 10 }), stopped = run({ autoDefense: true, type: 'archer', command: 'hold' });
    assert(automatic.tacticalHoldFrames >= 250 && automatic.meanDisplacement > 20, 'A real automatic defensive line could not give ground');
    assert(automatic.largestTacticalAnchorError < .01, 'Automatic defensive positions did not follow actual body movement');
    assert(stopped.meanDisplacement > 4 && stopped.largestTacticalAnchorError < .01 && stopped.postCommandPressure === 0,
      'Automatic defenders sprang back to obsolete anchors after the attacking pressure ended');
    const fighting = run({ autoDefense: true, seconds: 12 });
    assert.equal(fighting.targetOrder, 'auto');
    assert(fighting.meanDisplacement > 20 && fighting.rowDisplacements.every(n => n > 12), 'Automatic combat slots reclaimed the yielded rear ranks during repeated strategy updates');
    assert(fighting.largestPressureReturn < .01, 'Normal collision correction walked a pressured body back into its attacker');
    return { depth, automatic, stopped, fighting };
  });
  test('Rear rocks, narrow corridors, and heavy allied backing limit whole-formation yielding', () => {
    const open = run({}), outcomes = [run({ wall: true }), run({ corridor: true }), run({ backing: true })];
    for (const result of outcomes) {
      assert(result.firstPressure !== null && result.displacedSoldiers >= 24, 'The obstruction control never exercised actual pressure');
      assert(result.meanDisplacement < open.meanDisplacement * .65, 'A rear obstruction did not limit real formation progress');
      assert(result.rowDisplacements[ROWS - 1] < 11, 'Pressure dragged the last rank through its solid or heavy backing');
    }
    return { open, outcomes };
  });
  test('A disconnected outlier and distant or purely ranged forces produce no ghost formation displacement', () => {
    const outlier = run({ outlier: true }), distant = run({ distance: 700, seconds: 10 }), ranged = run({ sourceType: 'archer', distance: 180, seconds: 10 });
    const isolatedAttacker = run({ sourceRows: 1, sourceCols: 1 }), distantSupport = run({ distantSupport: true, seconds: 10 });
    assert(outlier.meanDisplacement > 20, 'The connected body did not yield in the outlier control');
    assert(outlier.outlierPressureTravel === 0 && outlier.outlierDisplacement < .01, 'A disconnected soldier was dragged along with distant ranks');
    for (const result of [distant, ranged, isolatedAttacker, distantSupport]) {
      assert.equal(result.firstPressure, null, 'A distant or ranged-only attacker exerted physical line pressure');
      assert(result.meanDisplacement < .01, 'An untouched held army moved without physical contact');
    }
    return { outlier, distant, ranged, isolatedAttacker, distantSupport };
  });
  test('Fresh source orders immediately end outgoing pressure and cannot revive a stale contact cache', () => {
    const outcomes = [];
    for (const command of ['hold', 'move', 'face', 'retreat']) {
      const result = run({ command }); outcomes.push(result);
      assert.equal(result.commandAccepted, 1, 'A source command was not accepted during pressure');
      assert.equal(result.postCommandPressure, 0, 'Old source pressure continued after a fresh command');
      if (command === 'move' || command === 'retreat') assert(result.commandedProgress > 20, 'Incoming melee pressure overruled source withdrawal');
      if (command === 'retreat') assert(result.firstWithdrawal > 0, 'Pressing ranks did not move away on the first retreat step');
      if (command === 'face') assert(result.firstCommandStep < .01, 'Pressure skipped the initial visible command turn');
    }
    return { outcomes };
  });
  test('A pressured formation withdraws on the first Retreat step', () => {
    const retreat = run({ command: 'retreat', commandReceiver: true }), move = run({ command: 'move', commandReceiver: true });
    assert.equal(retreat.commandAccepted, 1); assert(retreat.firstWithdrawal > 0, 'Pressed ranks did not withdraw on the first step');
    assert.equal(retreat.postCommandPressure, 0); assert(retreat.receiverFallingBack && retreat.commandedProgress > 50, 'Incoming pressure prevented a real retreat');
    assert.equal(move.commandAccepted, 1); assert.equal(move.postCommandPressure, 0); assert(move.commandedProgress > 50, 'Pressure prevented a clear fresh Move');
    return { retreat, move };
  });
}

if (require.main === module) {
  const root = process.argv[2] || path.resolve(__dirname, '..'); global.window = global;
  for (const name of ['data', 'tactics', 'impacts', 'siege', 'engine']) { const file = path.join(root, 'src', name + '.js'); if (fs.existsSync(file)) require(file); }
  const results = [], filter = (process.env.IRONFIELD_TEST_FILTER || '').toLowerCase();
  register((name, body) => {
    if (filter && !name.toLowerCase().includes(filter)) return;
    const began = performance.now();
    try { const detail = body(); results.push({ name, passed: true, milliseconds: round(performance.now() - began), ...detail }); console.log('PASS ' + name + ' ' + JSON.stringify(detail)); }
    catch (error) { results.push({ name, passed: false, error: error.message }); console.error('FAIL ' + name + ': ' + error.stack); process.exitCode = 1; }
  });
  console.log(JSON.stringify({ passed: results.filter(r => r.passed).length, failed: results.filter(r => !r.passed).length, results }, null, 2));
}
module.exports = register;
module.exports.trial = trial;
module.exports.configuration = configuration;
