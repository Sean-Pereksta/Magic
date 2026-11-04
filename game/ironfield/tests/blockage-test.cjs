'use strict';
// Real approach/passing regressions for the shipped Broken Charge scenario.
// Run: node tests/blockage-test.cjs [alternate-project-root]
const assert = require('node:assert/strict');
const path = require('node:path');
const { performance } = require('node:perf_hooks');

function register(test) {
  const D = global.GameData, Engine = global.BattleEngine;
  const round = n => Math.round(n * 1000) / 1000;
  const mean = xs => xs.reduce((sum, n) => sum + n, 0) / Math.max(1, xs.length);
  const active = (e, r) => r.unitIds.map(id => e.units[id]).filter(u => u.alive && !u.routed);
  const center = (e, r) => { const us = active(e, r); return { x: mean(us.map(u => u.x)), y: mean(us.map(u => u.y)) }; };
  const positions = e => e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp, angle: u.angle }));
  const diameter = us => {
    let maximum = 0;
    for (let i = 0; i < us.length; i++) for (let j = i + 1; j < us.length; j++) maximum = Math.max(maximum, Math.hypot(us[i].x - us[j].x, us[i].y - us[j].y));
    return maximum;
  };
  const gate = (condition, message, evidence) => assert(condition, message + '\nPhysical evidence: ' + JSON.stringify(evidence));

  function track(e) {
    const count = e.units.length, px = new Float64Array(count), py = new Float64Array(count), sx = new Float64Array(count), sy = new Float64Array(count);
    const travel = new Float64Array(count), ever = e.regiments.map(() => new Set()), windowHits = e.regiments.map(() => new Set());
    const damage = new Float64Array(e.regiments.length), firstHit = new Float64Array(e.regiments.length); firstHit.fill(-1);
    const maximumRadius = Math.max(...D.TYPE_ORDER.map(id => D.TYPES[id].radius));
    const cell = Math.max(64, Math.ceil((maximumRadius * 2 + 16) / 16) * 16), columns = Math.ceil(e.width / cell) + 1, rows = Math.ceil(e.height / cell) + 1;
    const heads = new Int32Array(columns * rows), next = new Int32Array(count);
    let minimumBodyRatio = Infinity, maximumStep = 0, closestBodies = null;
    for (const u of e.units) { px[u.id] = sx[u.id] = u.x; py[u.id] = sy[u.id] = u.y; }
    // Observe naturally dispatched attacks at their actual release coordinates.
    // Forward once, without injecting attacks, changing state, or drawing RNG.
    const attack = e._attack;
    e._attack = function(a, b) {
      const type = D.TYPES[a.type], before = Math.max(0, b.hp), distance = Math.hypot(a.x - b.x, a.y - b.y);
      const reach = type.range * (e.terrainAt(a.x, a.y).elevation ? 1.12 : 1) + (type.ranged ? 0 : D.TYPES[b.type].radius);
      const result = attack.call(this, a, b), lost = before - Math.max(0, b.hp);
      if (!type.ranged && !type.commander && lost > 0) {
        gate(distance <= reach + .01, 'A traffic-recovery melee hit exceeded its actual release-time weapon reach',
          { time: e.time, attacker: a.id, target: b.id, types: [a.type, b.type], distance, reach,
            actualElevation: e.terrainAt(a.x, a.y).elevation, cachedElevation: a._terrain && a._terrain.elevation });
        ever[a.regiment].add(a.id); windowHits[a.regiment].add(a.id); damage[a.regiment] += lost;
        if (firstHit[a.regiment] < 0) firstHit[a.regiment] = e.time;
      }
      return result;
    };
    const step = () => {
      e.step(1 / 30); heads.fill(-1);
      for (const u of e.units) if (u.alive && !u.routed) {
        assert([u.x, u.y, u.hp, u.angle].every(Number.isFinite), 'Passing produced nonfinite physical state');
        assert(u.x >= 0 && u.x <= e.width && u.y >= 0 && u.y <= e.height, 'Passing left world bounds');
        assert.equal(e.terrainAt(u.x, u.y).blocked, false, 'Passing crossed impassable terrain');
        const moved = Math.hypot(u.x - px[u.id], u.y - py[u.id]); travel[u.id] += moved; maximumStep = Math.max(maximumStep, moved);
        assert(moved <= D.TYPES[u.type].speed / 30 * 1.8 + .25, 'Passing recovery teleported or over-accelerated a soldier');
        const gx = Math.floor(px[u.id] / cell), gy = Math.floor(py[u.id] / cell), radius = D.TYPES[u.type].radius;
        for (let y = Math.max(0, gy - 1); y <= Math.min(rows - 1, gy + 1); y++) for (let x = Math.max(0, gx - 1); x <= Math.min(columns - 1, gx + 1); x++) {
          for (let id = heads[y * columns + x]; id !== -1; id = next[id]) {
            const v = e.units[id], combined = radius + D.TYPES[v.type].radius;
            const dx = px[u.id] - px[id], dy = py[u.id] - py[id];
            if (Math.abs(dx) > combined + 16 || Math.abs(dy) > combined + 16) continue;
            const vx = u.x - px[u.id] - v.x + px[id], vy = u.y - py[u.id] - v.y + py[id], vv = vx * vx + vy * vy;
            const time = vv ? Math.max(0, Math.min(1, -(dx * vx + dy * vy) / vv)) : 0;
            const ratio = Math.hypot(dx + vx * time, dy + vy * time) / combined;
            if (ratio < minimumBodyRatio) {
              minimumBodyRatio = ratio;
              closestBodies = { time: e.time, units: [u.id, id], regiments: [u.regiment, v.regiment], types: [u.type, v.type],
                before: Math.hypot(dx, dy) / combined, swept: ratio, after: Math.hypot(u.x - v.x, u.y - v.y) / combined };
            }
          }
        }
        const key = gy * columns + gx; next[u.id] = heads[key]; heads[key] = u.id;
      }
      gate(minimumBodyRatio >= .90, 'Passing recovery swept troops through occupied bodies', closestBodies);
      for (const u of e.units) { px[u.id] = u.x; py[u.id] = u.y; }
    };
    const window = () => {
      const result = e.regiments.map(r => {
        const us = active(e, r), idleIds = us.filter(u => travel[u.id] < 12 && !windowHits[r.id].has(u.id)).map(u => u.id);
        return { id: r.id, team: r.team, type: r.type, alive: us.length, idleIds, meanPath: round(mean(us.map(u => travel[u.id]))),
          meanNet: round(mean(us.map(u => Math.hypot(u.x - sx[u.id], u.y - sy[u.id])))), actualHitters: windowHits[r.id].size,
          everHitters: ever[r.id].size, damage: round(damage[r.id]), center: center(e, r), cohortDiameter: round(diameter(us)),
          order: r.order, status: r.status, fallingBack: r.fallingBack };
      });
      for (const u of e.units) { sx[u.id] = u.x; sy[u.id] = u.y; } travel.fill(0); windowHits.forEach(s => s.clear());
      for (const s of e.getStats().teams) assert.equal(s.initial, s.alive + s.dead + s.routed);
      return result;
    };
    return { step, window, ever, firstHit, damage, metrics: () => ({ minimumBodyRatio: round(minimumBodyRatio), maximumStep: round(maximumStep) }) };
  }

  function doubleScaleApproach() {
    const source = JSON.parse(JSON.stringify(D.PRESETS.find(p => p.id === 'charge')));
    // Match the public preset scale control, including commander counts.
    for (const army of source.armies) {
      const total = Object.values(army.roster).reduce((sum, count) => sum + count, 0), scale = Math.min(2, 10000 / total);
      for (const type of D.TYPE_ORDER) army.roster[type] = Math.round((army.roster[type] || 0) * scale);
    }
    const e = new Engine(source), observer = track(e), initial = positions(e), previousIdle = new Map(), persistent = [], windows = [];
    assert.equal(e.units.length, 5068); e.start();
    for (let tick = 0; tick < 40 * 30; tick++) {
      observer.step();
      if ((tick + 1) % 300 !== 0) continue;
      const rows = observer.window().filter(row => row.team === 1 && D.TYPES[row.type].mounted && !D.TYPES[row.type].ranged && !D.TYPES[row.type].commander);
      for (const row of rows) {
        const old = previousIdle.get(row.id) || new Set(), same = row.idleIds.filter(id => old.has(id));
        if (row.alive >= 32 && same.length >= 32 && same.length > row.alive * .5) persistent.push({ time: (tick + 1) / 30, id: row.id, type: row.type, alive: row.alive, stationaryAcrossBothWindows: same.length });
        previousIdle.set(row.id, new Set(row.idleIds));
      }
      windows.push({ time: (tick + 1) / 30, cohorts: rows.map(row => ({ id: row.id, type: row.type, living: row.alive,
        meanNet: row.meanNet, meanPath: row.meanPath, idle: row.idleIds.length, actualHitters: row.actualHitters,
        westwardAdvance: round(mean(active(e, e.regiments[row.id]).map(u => initial[u.id].x - u.x))) })) });
    }
    const result = { scale: 2, units: e.units.length, simulatedSeconds: round(e.time), repeatedStationaryCohorts: persistent, windows, ...observer.metrics() };
    gate(persistent.length === 0, 'Repeated automatic yields starved the same mounted body in the public double-scale scenario', result);
    return result;
  }

  test('The natural Broken Charge advances its blocked mounted cohorts without repeated precontact freezes', () => {
    const source = JSON.parse(JSON.stringify(D.PRESETS.find(p => p.id === 'charge')));
    assert.equal(source.map, 'meadow'); assert.equal(source.seed, 64172);
    const e = new Engine(source), initial = positions(e), watched = [25, 26, 18].map(id => e.regiments[id]);
    assert.deepEqual(watched.map(r => [r.team, r.type, r.alive]), [[1, 'cavalry', 80], [1, 'cavalry', 80], [1, 'elephant', 8]], 'The public preset changed its reproduced cohorts');
    assert.equal(e.units.length, 2534);
    const observer = track(e), windows = [], earlyRows = [], previousIdle = new Map(), persistent = [];
    let advanceAtForty = [];
    e.start();
    for (let tick = 0; tick < 90 * 30; tick++) {
      observer.step();
      if ((tick + 1) % 300 === 0) {
        const seconds = (tick + 1) / 30, rows = observer.window();
        if (seconds <= 40) for (const row of rows) {
          const type = D.TYPES[row.type];
          if (row.team !== 1 || !type.mounted || type.ranged || type.commander || row.alive < 32) continue;
          const old = previousIdle.get(row.id) || new Set(), same = row.idleIds.filter(id => old.has(id));
          if (same.length >= 32 && same.length > row.alive * .5) persistent.push({ time: seconds, id: row.id, type: row.type, alive: row.alive, stationaryAcrossBothWindows: same.length });
          previousIdle.set(row.id, new Set(row.idleIds));
        }
        const selected = rows.filter(row => watched.some(r => r.id === row.id)).map(row => {
          const r = e.regiments[row.id];
          return { ...row, idle: row.idleIds.length, idleIds: undefined, assignedEnemy: r._enemyId,
            tacticStage: r._tactic && r._tactic.stage, tacticTarget: r._tactic && r._tactic.targetId };
        });
        windows.push({ time: seconds, cohorts: selected }); if (seconds <= 40) earlyRows.push({ time: seconds, cohorts: selected });
        if (seconds === 40) advanceAtForty = watched.map(r => ({ id: r.id, type: r.type,
          surviving: active(e, r).length, meanWestwardAdvance: round(mean(active(e, r).map(u => initial[u.id].x - u.x))) }));
      }
    }
    const combat = watched.map(r => {
      const unengaged = active(e, r).filter(u => !observer.ever[r.id].has(u.id));
      const enemy = e.regiments[r._enemyId];
      return { id: r.id, type: r.type, distinctHitters: observer.ever[r.id].size,
        firstActualHit: observer.firstHit[r.id] < 0 ? null : round(observer.firstHit[r.id]), actualDamage: round(observer.damage[r.id]),
        surviving: active(e, r).length, neverHitSurvivors: unengaged.length,
        neverHitStates: Object.fromEntries([...new Set(unengaged.map(u => u.state))].map(state => [state, unengaged.filter(u => u.state === state).length])),
        meanNeverHitJam: round(mean(unengaged.map(u => u._jamTime || 0))),
        enemy: enemy ? { id: enemy.id, type: enemy.type, alive: enemy.alive, fallingBack: enemy.fallingBack } : null,
        contactDistance: round(r._contactDistance), tacticStage: r._tactic && r._tactic.stage };
    });
    const evidence = { preset: source.id, map: source.map, seed: source.seed, units: e.units.length, simulatedSeconds: round(e.time),
      repeatedStationaryCohorts: persistent, advanceAtForty, combat, earlyWindows: earlyRows,
      lateWindows: windows.filter(w => w.time > 40), ...observer.metrics() };
    gate(persistent.length === 0, 'A substantial mounted cohort froze without combat for two consecutive ten-second windows', evidence);
    gate(advanceAtForty.every(row => row.surviving > 0 && row.meanWestwardAdvance >= 500), 'A mounted cohort lost its sustained physical approach to an obsolete passing maneuver', evidence);
    gate(combat.every(row => row.distinctHitters >= (row.type === 'elephant' ? 1 : 32) && row.actualDamage > 30), 'An apparently moving cohort never joined actual combat by the end of the natural observation', evidence);
    return { ...evidence, doubleScaleApproach: doubleScaleApproach() };
  });

  const crossingConfig = () => ({ map: 'custom', seed: 64172, customMap: { biome: 'grassland', regions: [] },
    armies: [{ name: 'Stationary fronts', doctrine: 'defensive', roster: { guardian: 320 } }, { name: 'Crossing wings', doctrine: 'flank', roster: { elephant: 8, cavalry: 80 } }],
    formations: [
      { team: 0, type: 'guardian', count: 160, x: 2600, y: 1600, facing: 0, formation: 'line' },
      { team: 0, type: 'guardian', count: 160, x: 2600, y: 2800, facing: 0, formation: 'line' },
      { team: 1, type: 'elephant', count: 8, x: 3968, y: 2200, facing: Math.PI, formation: 'line' },
      { team: 1, type: 'cavalry', count: 80, x: 4268, y: 2200, facing: Math.PI, formation: 'line' }
    ] });
  function crossingTrial() {
    const e = new Engine(crossingConfig()), observer = track(e);
    e.command([0, 1], 'hold'); e.start();
    return { e, observer, elephants: e.regiments[2], cavalry: e.regiments[3] };
  }

  function duringActualYield() {
    // Encircle now intentionally takes cavalry outside the elephant's direct
    // approach. Use a genuine crossing under Balanced to test interruption
    // of right-of-way itself, independently of that new army strategy. With
    // mounted acceleration the former x=3800 trailing wing arrives after the
    // first wing has already cleared; x=3750 creates a real crossing again.
    const e = new Engine({ map: 'custom', seed: 6432, customMap: { biome: 'grassland', regions: [] },
      armies: [{ name: 'Stationary fronts', doctrine: 'balanced', roster: { guardian: 160 } },
        { name: 'Crossing riders', doctrine: 'balanced', roster: { cavalry: 160 } }],
      formations: [
        { team: 0, type: 'guardian', count: 80, x: 2600, y: 1800, facing: 0 },
        { team: 0, type: 'guardian', count: 80, x: 2600, y: 2600, facing: 0 },
        { team: 1, type: 'cavalry', count: 80, x: 3600, y: 2000, facing: Math.PI },
        { team: 1, type: 'cavalry', count: 80, x: 3750, y: 1700, facing: Math.PI }
      ] });
    const trial = { e, observer: track(e) };
    e.command([0, 1], 'hold'); e.start();
    for (let tick = 0; tick < 300; tick++) {
      trial.observer.step();
      // Read the actual scheduled yield only to choose when to issue the
      // public order. Progress, shape, and collision assertions use bodies.
      const plan = trial.e._trafficPlans.find(p => p.yielding);
      if (plan) return { ...trial, plan, yielding: trial.e.regiments[plan.a], other: trial.e.regiments[plan.b] };
    }
    assert.fail('The public crossing fixture did not enter a temporary yield within ten seconds');
  }

  function interruptActualYield(order) {
    const trial = duringActualYield(), { e, observer, yielding: r, plan } = trial;
    const before = positions(e), c = center(e, r), other = center(e, trial.other), issuedAt = e.time;
    const awayX = c.x - other.x, awayY = c.y - other.y, distance = Math.hypot(awayX, awayY);
    const clearX = awayX / distance, clearY = awayY / distance;
    assert.equal(order === 'wedge' ? e.setFormation([r.id], order) : e.command([r.id], order, c.x + clearX * 400, c.y + clearY * 400), 1);
    assert.deepEqual(positions(e), before, 'Changing an active yield immediately moved or turned physical troops');
    assert(!e._trafficPlans.includes(plan), 'The accepted action retained the obsolete yielding plan');
    const direction = order === 'retreat' ? { x: r.targetX - c.x, y: r.targetY - c.y } : { x: clearX, y: clearY };
    const directionLength = Math.max(.001, Math.hypot(direction.x, direction.y));
    direction.x /= directionLength; direction.y /= directionLength;
    const duration = order === 'wedge' ? 12 : order === 'hold' ? 4 : 2;
    let initialTranslation = 0;
    for (let tick = 0; tick < duration * 30; tick++) {
      observer.step();
      if (order === 'retreat' && tick < 3) initialTranslation = Math.max(initialTranslation,
        ...active(e, r).map(u => Math.hypot(u.x - before[u.id].x, u.y - before[u.id].y)));
    }
    const us = active(e, r), now = center(e, r), progress = us.map(u => (u.x - before[u.id].x) * direction.x + (u.y - before[u.id].y) * direction.y);
    const result = { action: order, yieldingType: r.type, issuedAt: round(issuedAt), observedFor: duration, living: us.length,
      meanProgress: round(mean(progress)), progressingTen: progress.filter(n => n > 10).length,
      meanNetMovement: round(mean(us.map(u => Math.hypot(u.x - before[u.id].x, u.y - before[u.id].y)))),
      centroidMovement: round(Math.hypot(now.x - c.x, now.y - c.y)), order: r.order, ...observer.metrics() };
    if (order === 'move' || order === 'retreat') {
      gate(result.meanProgress >= 25 && result.progressingTen >= us.length * .75 && r.order === order,
        'A fresh movement order failed to take physical control during an automatic yield', result);
      if (order === 'retreat') {
        result.initialTranslation = round(initialTranslation);
        gate(initialTranslation > .01, 'Retreat waited for a stationary turn before withdrawing', result);
      }
    } else if (order === 'hold') {
      gate(r.order === 'hold' && result.centroidMovement <= 8 && result.meanNetMovement <= 12,
        'The expiration of a temporary yield overrode the player\'s physical Hold', result);
    } else {
      // A wedge has a narrow physical leading quarter and a broader rear.
      // Project observed bodies along the accepted formation heading; the
      // comparison cannot pass merely by changing r.formation or its banner.
      const nx = Math.cos(r.facing), ny = Math.sin(r.facing);
      const projected = us.map(u => ({ forward: u.x * nx + u.y * ny, across: -u.x * ny + u.y * nx })).sort((a, b) => b.forward - a.forward);
      const width = values => Math.max(...values.map(p => p.across)) - Math.min(...values.map(p => p.across));
      result.frontQuarterWidth = round(width(projected.slice(0, Math.ceil(us.length / 4))));
      result.rearHalfWidth = round(width(projected.slice(Math.floor(us.length / 2))));
      gate(us.length === 80 && result.rearHalfWidth > 100 && result.frontQuarterWidth <= result.rearHalfWidth * .70,
        'Changing formation during a yield did not produce a physical wedge after the pause expired', result);
    }
    return result;
  }

  test('Crossing mounted wings keep moving and yield to fresh commands and formation changes', () => {
    const { e, observer, elephants, cavalry } = crossingTrial(), originalDiameter = diameter(active(e, cavalry));
    const north = { x: 2600 - 3968, y: 1600 - 2200 }, length = Math.hypot(north.x, north.y); north.x /= length; north.y /= length;
    let atTen = null, motion = null;
    for (let tick = 0; tick < 60 * 30; tick++) {
      observer.step();
      if (tick === 299) atTen = positions(e);
      if (tick === 599) {
        const us = active(e, elephants), progress = us.map(u => (u.x - atTen[u.id].x) * north.x + (u.y - atTen[u.id].y) * north.y);
        motion = { livingElephants: us.length, meanProgressDuringTenToTwenty: round(mean(progress)), progressingSixty: progress.filter(n => n > 60).length,
          stationaryElephants: us.filter(u => Math.hypot(u.x - atTen[u.id].x, u.y - atTen[u.id].y) < 12).length,
          initialCavalryDiameter: round(originalDiameter), cavalryDiameterAtTwenty: round(diameter(active(e, cavalry))) };
      }
    }
    const fighting = { elephantHitters: observer.ever[elephants.id].size, cavalryHitters: observer.ever[cavalry.id].size,
      elephantDamage: round(observer.damage[elephants.id]), cavalryDamage: round(observer.damage[cavalry.id]) };
    const evidence = { motion, fighting, ...observer.metrics() };
    gate(motion.livingElephants === 8 && motion.meanProgressDuringTenToTwenty >= 150 && motion.progressingSixty >= 6 && motion.stationaryElephants <= 2,
      'The crossing plan held the elephant body still after the moving rider front passed', evidence);
    gate(motion.cavalryDiameterAtTwenty <= originalDiameter * 2, 'A temporary crossing stretched the rider formation into an excessive long column', evidence);
    gate(fighting.elephantHitters >= 4 && fighting.cavalryHitters >= 24 && fighting.elephantDamage > 60 && fighting.cavalryDamage > 60,
      'The passing wings moved but never delivered actual sustained combat', evidence);

    // Independent public run: a temporary yielding role must not survive a
    // later player order. Choose the clear side away from the other body.
    const trial = crossingTrial();
    for (let tick = 0; tick < 300; tick++) trial.observer.step();
    const c = center(trial.e, trial.elephants), other = center(trial.e, trial.cavalry), before = positions(trial.e);
    const dx = c.x - other.x, dy = c.y - other.y, d = Math.max(1, Math.hypot(dx, dy)), nx = dx / d, ny = dy / d;
    assert.equal(trial.e.command([trial.elephants.id], 'move', c.x + nx * 400, c.y + ny * 400), 1);
    assert.deepEqual(positions(trial.e), before, 'A new Move immediately changed body positions or headings');
    for (let tick = 0; tick < 60; tick++) trial.observer.step();
    const progress = active(trial.e, trial.elephants).map(u => (u.x - before[u.id].x) * nx + (u.y - before[u.id].y) * ny);
    const command = { meanProgress: round(mean(progress)), progressingTen: progress.filter(n => n > 10).length, order: trial.elephants.order, ...trial.observer.metrics() };
    gate(command.meanProgress >= 25 && command.progressingTen >= 6 && command.order === 'move', 'Automatic yielding prevented a fresh clear Move', command);
    const activeYieldActions = ['move', 'retreat', 'hold', 'wedge'].map(interruptActualYield);
    return { ...evidence, command, activeYieldActions };
  });
}

if (require.main === module) {
  const root = process.argv[2] || path.resolve(__dirname, '..'); global.window = global;
  require(path.join(root, 'src/data.js'));
  if (require('node:fs').existsSync(path.join(root, 'src/tactics.js'))) require(path.join(root, 'src/tactics.js'));
  for (const name of ['impacts', 'siege']) {
    const file = path.join(root, 'src', name + '.js');
    if (require('node:fs').existsSync(file)) require(file);
  }
  require(path.join(root, 'src/engine.js'));
  const results = [], filter = (process.env.IRONFIELD_TEST_FILTER || '').toLowerCase();
  register((name, body) => {
    if (filter && !name.toLowerCase().includes(filter)) return;
    const began = performance.now();
    try { const detail = body(); results.push({ name, passed: true, milliseconds: Math.round(performance.now() - began), ...detail }); console.log('PASS ' + name + ' ' + JSON.stringify(detail)); }
    catch (error) { results.push({ name, passed: false, error: error.message }); console.error('FAIL ' + name + ': ' + error.stack); process.exitCode = 1; }
  });
  console.log(JSON.stringify({ passed: results.filter(r => r.passed).length, failed: results.filter(r => !r.passed).length, results }, null, 2));
}
module.exports = register;
