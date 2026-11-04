'use strict';
// Focused public-command geometry/combat checks, also registered in engine-test.cjs.
// Run: node tests/formation-continuity-test.cjs [alternate-project-root]
const assert = require('node:assert/strict');
const path = require('node:path');
const { performance } = require('node:perf_hooks');

function register(test) {
  const D = global.GameData, Engine = global.BattleEngine;
  const cfg = (blue, red) => ({ map: 'custom', seed: 4719, customMap: { biome: 'grassland', regions: [] },
    armies: [{ name: 'Azure', doctrine: 'balanced', roster: blue }, { name: 'Ember', doctrine: 'balanced', roster: red }] });
  const mean = a => a.reduce((sum, n) => sum + n, 0) / Math.max(1, a.length);
  const units = (e, r) => r.unitIds.map(id => e.units[id]).filter(u => u.alive && !u.routed);
  const center = (e, r) => { const us = units(e, r); return { x: mean(us.map(u => u.x)), y: mean(us.map(u => u.y)) }; };
  const snapshot = e => e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp, angle: u.angle, target: u.target, attack: u._lastAttack }));
  const deltaAngle = (a, b) => Math.atan2(Math.sin(a - b), Math.cos(a - b));
  const rounds = n => Math.round(n * 1000) / 1000;
  const releases = new WeakMap();
  function observeReleases(e) {
    const record = { byAttacker: new Map(), checked: 0, endStepOnlyOverruns: 0, maximumEndStepOverrun: 0 }, attack = e._attack;
    releases.set(e, record);
    e._attack = function(attacker, target) {
      const before = Math.max(0, target.hp), type = D.TYPES[attacker.type];
      const distance = Math.hypot(attacker.x - target.x, attacker.y - target.y);
      const reach = type.range * (e.terrainAt(attacker.x, attacker.y).elevation ? 1.12 : 1) + D.TYPES[target.type].radius;
      // Observe the dispatched attack exactly once, without changing health,
      // targets, positions, or RNG. The defender may move later this tick.
      const result = attack.call(this, attacker, target), damage = before - Math.max(0, target.hp);
      if (damage > 0 && !type.ranged) {
        assert(distance <= reach + .01, 'A counted melee strike occurred beyond actual release-time weapon reach');
        record.checked++;
        record.byAttacker.set(attacker.id, { time: e.time, targetId: target.id, targetRegiment: target.regiment, reach, damage });
      }
      return result;
    };
    return record;
  }
  function physics(e, previous) {
    const buckets = new Map(), size = 64; let ratio = Infinity, maximumStep = 0;
    for (const u of e.units) if (u.alive && !u.routed) {
      const p = previous[u.id], radius = D.TYPES[u.type].radius;
      assert([u.x, u.y, u.hp].every(Number.isFinite));
      assert(u.x >= 0 && u.x <= e.width && u.y >= 0 && u.y <= e.height);
      assert.equal(e.terrainAt(u.x, u.y).blocked, false, 'Formation movement entered blocked terrain');
      const moved = Math.hypot(u.x - p.x, u.y - p.y); maximumStep = Math.max(maximumStep, moved);
      assert(moved <= D.TYPES[u.type].speed / 30 * 1.8 + .25, 'Formation change teleported a soldier');
      const gx = Math.floor(p.x / size), gy = Math.floor(p.y / size);
      for (let y = gy - 1; y <= gy + 1; y++) for (let x = gx - 1; x <= gx + 1; x++) {
        for (const v of buckets.get(x + ':' + y) || []) {
          const q = previous[v.id], combined = radius + D.TYPES[v.type].radius;
          if (Math.abs(p.x - q.x) > combined + 8 || Math.abs(p.y - q.y) > combined + 8) continue;
          const px = p.x - q.x, py = p.y - q.y, vx = u.x - p.x - v.x + q.x, vy = u.y - p.y - v.y + q.y;
          const vv = vx * vx + vy * vy, t = vv ? Math.max(0, Math.min(1, -(px * vx + py * vy) / vv)) : 0;
          ratio = Math.min(ratio, Math.hypot(px + vx * t, py + vy * t) / combined);
        }
      }
      const key = gx + ':' + gy; if (!buckets.has(key)) buckets.set(key, []); buckets.get(key).push(u);
    }
    assert(ratio >= .90, 'Formation movement crossed occupied bodies');
    for (const team of e.getStats().teams) assert.equal(team.initial, team.alive + team.dead + team.routed);
    return { ratio, maximumStep };
  }
  function shape(e, r, initial) {
    const us = units(e, r), c = center(e, r), ox = mean(r.unitIds.map(id => initial[id].x)), oy = mean(r.unitIds.map(id => initial[id].y));
    const dx = c.x - ox, dy = c.y - oy;
    return { advance: r.team ? -dx : dx, meanDistortion: mean(us.map(u => Math.hypot(u.x - initial[u.id].x - dx, u.y - initial[u.id].y - dy))),
      lateralDrift: Math.max(...us.map(u => Math.abs(u.y - initial[u.id].y - dy))),
      wingCrossers: us.filter(u => Math.abs(initial[u.id].y - oy) > 40 && (initial[u.id].y - oy) * (u.y - c.y) < 0).length,
      depth: Math.max(...us.map(u => u.x)) - Math.min(...us.map(u => u.x)), width: Math.max(...us.map(u => u.y)) - Math.min(...us.map(u => u.y)) };
  }
  function actualHits(e, previous, r, defender, hitters) {
    const observed = releases.get(e); assert(observed, 'Actual melee checks require a release observer');
    let strikes = 0;
    for (const id of r.unitIds) {
      const u = e.units[id], hit = observed.byAttacker.get(id);
      if (!hit || hit.time <= previous[id].attack || (defender && hit.targetRegiment !== defender.id)) continue;
      const target = e.units[hit.targetId], overrun = Math.hypot(u.x - target.x, u.y - target.y) - hit.reach;
      if (overrun > .01) { observed.endStepOnlyOverruns++; observed.maximumEndStepOverrun = Math.max(observed.maximumEndStepOverrun, overrun); }
      hitters.add(id); strikes++;
    }
    return { strikes };
  }

  test('Distant automatic marches preserve both armies original physical ranks at battle start', () => {
    const result = {};
    for (const team of [0, 1]) {
      const e = new Engine(cfg({ guardian: 160 }, { guardian: 160 })), r = e.regiments.find(r => r.team === team), other = e.regiments.find(r => r.team !== team);
      e.relocateRegiments([r.id], team ? 5000 : 1400, 2200); e.relocateRegiments([other.id], team ? 1400 : 5000, 2200); e.command([other.id], 'hold');
      const initial = snapshot(e), original = shape(e, r, initial); let maximumDrift = 0, distortion = 0, minimumRatio = Infinity, earlyProgress = 0;
      e.start();
      for (let tick = 0; tick < 300; tick++) {
        const previous = snapshot(e); e.step(1 / 30); const p = physics(e, previous), s = shape(e, r, initial);
        minimumRatio = Math.min(minimumRatio, p.ratio); maximumDrift = Math.max(maximumDrift, s.lateralDrift); distortion = Math.max(distortion, s.meanDistortion);
        assert(s.lateralDrift <= 6 && s.meanDistortion <= 4, `Team ${team} reshuffled deployed ranks during an unobstructed march`);
        assert(s.depth >= original.depth * .85 && s.depth <= original.depth * 1.15 && s.width >= original.width * .85 && s.width <= original.width * 1.15,
          `Team ${team} stretched or collapsed its formation before contact`);
        assert.equal(s.wingCrossers, 0, 'Original outer-wing soldiers exchanged sides without an order');
        if (tick === 59) { earlyProgress = s.advance; assert(earlyProgress >= 40, 'Rank reshuffling prevented the first two seconds of forward progress'); }
      }
      const final = shape(e, r, initial); assert(final.advance >= 200); assert.equal(r.alive, 160);
      result[team ? 'red' : 'blue'] = { advanceAtTwoSeconds: rounds(earlyProgress), advanceAtTenSeconds: rounds(final.advance),
        maximumLateralDrift: rounds(maximumDrift), maximumDistortion: rounds(distortion), minimumBodyRatio: rounds(minimumRatio) };
    }
    return result;
  });

  test('Co-moving automatic formations open passing lanes without reversing the leading ranks into a long tail', () => {
    const e = new Engine(cfg({ guardian: 160, cavalry: 80 }, { guardian: 160 }));
    const lead = e.regiments.find(r => r.team === 0 && r.type === 'guardian'), riders = e.regiments.find(r => r.team === 0 && r.type === 'cavalry'), enemy = e.regiments.find(r => r.team === 1);
    e.relocateRegiments([lead.id], 2400, 2200); e.relocateRegiments([riders.id], 2050, 2200); e.relocateRegiments([enemy.id], 5600, 2200); e.command([enemy.id], 'hold');
    const initial = snapshot(e), original = shape(e, lead, initial); let maximumBackward = 0, mostBackward = 0, progressAtFive = 0, minimumRatio = Infinity;
    e.start();
    for (let tick = 0; tick < 900; tick++) {
      const previous = snapshot(e); e.step(1 / 30); minimumRatio = Math.min(minimumRatio, physics(e, previous).ratio);
      if (tick < 150) {
        const backward = lead.unitIds.filter(id => e.units[id].x < initial[id].x - 12).length; mostBackward = Math.max(mostBackward, backward);
        for (const id of lead.unitIds) maximumBackward = Math.max(maximumBackward, initial[id].x - e.units[id].x);
      }
      if (tick === 149) progressAtFive = shape(e, lead, initial).advance;
    }
    const final = shape(e, lead, initial), c = center(e, lead), through = units(e, riders).filter(u => u.x > c.x + 80).length;
    assert(mostBackward <= 8 && maximumBackward <= 30, 'Automatic passing sent the front formation backward to construct a long column');
    assert(progressAtFive >= 45, 'The leading body stopped marching while distant riders opened passing lanes');
    assert(final.depth <= original.depth * 2 && final.width >= original.width * .75, 'Leading ranks failed to regain a useful compact footprint after passage');
    assert(through >= 72, 'Most riders failed to overtake the moving infantry');
    assert.equal(lead.alive + riders.alive, 240);

    // The faster body owns its temporary overtake corridor. A fresh command
    // to the slower leader must invalidate that old shared intent as well.
    // Read its identity only for lifecycle inspection; real physical motion
    // below establishes that the accepted leader command actually works.
    const leaderOrderInterruptions = [];
    for (const order of ['move']) {
      const trial = new Engine(cfg({ guardian: 160, cavalry: 80 }, { guardian: 160 }));
      const leader = trial.regiments.find(r => r.team === 0 && r.type === 'guardian'), passing = trial.regiments.find(r => r.team === 0 && r.type === 'cavalry'), foe = trial.regiments.find(r => r.team === 1);
      trial.relocateRegiments([leader.id], 2400, 2200); trial.relocateRegiments([passing.id], 2050, 2200); trial.relocateRegiments([foe.id], 5600, 2200);
      trial.command([foe.id], 'hold'); const beforePassing = snapshot(trial); trial.start();
      for (let tick = 0; tick < 90; tick++) trial.step(1 / 30);
      const oldPlan = passing._traffic && passing._traffic.plan;
      assert(oldPlan && oldPlan.parallel, 'The interruption fixture did not establish automatic overtaking');
      const lateralMovement = mean(passing.unitIds.map(id => Math.abs(trial.units[id].y - beforePassing[id].y)));
      assert(lateralMovement > 20, 'The interruption fixture has only an overtake label without physical lateral movement');
      const beforeOrder = snapshot(trial);
      const lc = center(trial, leader), pc = center(trial, passing), clearSide = pc.y >= lc.y ? -1 : 1;
      assert.equal(trial.command([leader.id], order, lc.x, lc.y + clearSide * 700), 1);
      assert.deepEqual(snapshot(trial), beforeOrder, 'The interruption command immediately moved or turned a soldier');
      for (let tick = 0; tick < 30; tick++) {
        const previous = snapshot(trial); trial.step(1 / 30); minimumRatio = Math.min(minimumRatio, physics(trial, previous).ratio);
      }
      const progress = mean(leader.unitIds.map(id => (trial.units[id].y - beforeOrder[id].y) * clearSide));
      assert(progress >= 10, 'Traffic handling prevented the leader following its clear perpendicular Move: ' + rounds(progress) + ' units in one second');
      assert.equal(leader.order, order, 'An automatic traffic maneuver replaced the leader command');
      assert(!trial._trafficPlans.includes(oldPlan) && (!passing._traffic || passing._traffic.plan !== oldPlan),
        'A stale automatic overtake corridor survived the leader command');
      leaderOrderInterruptions.push({ order, lateralOvertakingBeforeCommand: rounds(lateralMovement), leaderProgressInOneSecond: rounds(progress), oldCorridorCleared: true });
    }
    return { mostLeadingTroopsMovingBackward: mostBackward, maximumBackward: rounds(maximumBackward), leadAdvanceAtFiveSeconds: rounds(progressAtFive),
      initialDepth: rounds(original.depth), finalDepth: rounds(final.depth), finalWidth: rounds(final.width), ridersThrough: through, minimumBodyRatio: rounds(minimumRatio),
      leaderOrderInterruptions };
  });

  test('An arranged four-sided envelopment keeps its physical wings and attacks the enclosed enemy despite a distant alternative', () => {
    const points = [[2400, 2200], [3200, 1400], [4000, 2200], [3200, 3000]], facings = [0, Math.PI / 2, Math.PI, -Math.PI / 2];
    const source = cfg({ swordsman: 640 }, { guardian: 320 });
    source.formations = points.map(([x, y], i) => ({ team: 0, type: 'swordsman', count: 160, x, y, formation: 'line', facing: facings[i] })).concat([
      { team: 1, type: 'guardian', count: 160, x: 3200, y: 2200, formation: 'line', facing: Math.PI },
      { team: 1, type: 'guardian', count: 160, x: 5100, y: 2200, formation: 'line', facing: Math.PI }]);
    const e = new Engine(source), wings = e.regiments.filter(r => r.team === 0), central = e.regiments.find(r => r.team === 1), releaseAudit = observeReleases(e);
    e.command(e.regiments.filter(r => r.team === 1).map(r => r.id), 'hold');
    const hitters = wings.map(() => new Set()), strikes = wings.map(() => 0), drift = wings.map(() => 0);
    let progress = [], minimumRatio = Infinity, sectorWindows = 0, centralHealthLost = 0; e.start();
    for (let tick = 0; tick < 1050; tick++) {
      const previous = snapshot(e); e.step(1 / 30); minimumRatio = Math.min(minimumRatio, physics(e, previous).ratio);
      for (const id of central.unitIds) centralHealthLost += Math.max(0, previous[id].hp - e.units[id].hp);
      wings.forEach((r, i) => {
        const hits = actualHits(e, previous, r, central, hitters[i]); strikes[i] += hits.strikes;
        const c = center(e, r), nx = (points[i][0] - 3200) / 800, ny = (points[i][1] - 2200) / 800;
        if (tick < 300) drift[i] = Math.max(drift[i], Math.abs((c.x - 3200) * -ny + (c.y - 2200) * nx));
        if (tick === 299) progress[i] = 800 - ((c.x - 3200) * nx + (c.y - 2200) * ny);
        if ([599, 749, 899, 1049].includes(tick)) {
          const us = units(e, r), onSide = us.filter(u => (u.x - 3200) * nx + (u.y - 2200) * ny > 25).length;
          assert(us.length >= 80 && onSide / us.length >= .70, 'An enclosing wing abandoned its physical side before the central enemy was defeated');
        }
      });
      if ([599, 749, 899, 1049].includes(tick)) sectorWindows++;
    }
    assert(progress.every(n => n >= 200), 'One enclosing wing did not physically approach the central enemy within ten seconds');
    assert(drift.every(n => n <= 100), 'Automatic allocation sent a wing sideways toward the distant enemy');
    assert(hitters.every(s => s.size >= 16) && strikes.every(n => n >= 40), 'An enclosing side failed to deliver sustained real melee against the central defenders');
    return { inwardProgressAtTenSeconds: progress.map(rounds), maximumEarlyTangentialDrift: drift.map(rounds), centralEnemyHitters: hitters.map(s => s.size),
      centralEnemyStrikes: strikes, centralEnemyHealthLost: rounds(centralHealthLost), preservedSectorWindows: sectorWindows, minimumBodyRatio: rounds(minimumRatio),
      releaseReachChecks: releaseAudit.checked, endStepOnlyOverruns: releaseAudit.endStepOnlyOverruns, maximumEndStepOverrun: rounds(releaseAudit.maximumEndStepOverrun) };
  });

  test('Elephant automatic and ordered attacks produce repeated reachable strikes and real enemy health loss', () => {
    const outcomes = [];
    for (const trial of [{ count: 8, type: 'guardian', foes: 80, distance: 500, order: 'auto', minimumHitters: 6, minimumDamage: 900 },
      { count: 16, type: 'swordsman', foes: 160, distance: 700, order: 'charge', minimumHitters: 8, minimumDamage: 2000 }]) {
      const e = new Engine(cfg({ elephant: trial.count }, { [trial.type]: trial.foes })), r = e.regiments[0], enemy = e.regiments.find(r => r.team === 1);
      observeReleases(e);
      e.relocateRegiments([r.id], 2800, 2200); e.relocateRegiments([enemy.id], 2800 + trial.distance, 2200); e.command([enemy.id], 'hold');
      if (trial.order === 'charge') e.command([r.id], 'charge', undefined, undefined, { targetRegimentId: enemy.id });
      const hitters = new Set(); let strikes = 0, damage = 0, firstStrike = null, minimumRatio = Infinity; e.start();
      for (let tick = 0; tick < 1050 && e.phase === 'running'; tick++) {
        const previous = snapshot(e); e.step(1 / 30); minimumRatio = Math.min(minimumRatio, physics(e, previous).ratio);
        const hit = actualHits(e, previous, r, enemy, hitters); strikes += hit.strikes; if (hit.strikes && firstStrike === null) firstStrike = e.time;
        for (const id of enemy.unitIds) damage += Math.max(0, previous[id].hp - e.units[id].hp);
      }
      assert(hitters.size >= trial.minimumHitters && strikes >= 40, 'Elephants display motion without enough actual attacking individuals');
      assert(damage >= trial.minimumDamage && firstStrike !== null && firstStrike <= 15, 'Elephant strikes did not reach and damage the enemy in time');
      outcomes.push({ order: trial.order, distinctHitters: hitters.size, strikes, damage: rounds(damage), firstStrike: rounds(firstStrike), minimumBodyRatio: rounds(minimumRatio) });
    }
    return { outcomes };
  });

  test('Deployment Face rotates actual ranks safely and preserves facing and member geometry through save reconstruction', () => {
    const source = cfg({ swordsman: 16 }, { guardian: 16 }), e = new Engine(source), r = e.regiments[0];
    e.relocateRegiments([r.id], 2500, 2200); e.relocateRegiments([e.regiments[1].id], 4400, 2200);
    assert.equal(typeof e.faceRegiments, 'function', 'The public direction-only facing API is missing');
    const before = snapshot(e), c = center(e, r), ids = r.unitIds.slice();
    assert.equal(e.command([r.id], 'face', c.x, c.y + 1000), 1);
    assert.equal(r.order, 'auto'); const after = center(e, r); assert(Math.hypot(after.x - c.x, after.y - c.y) < 1e-6);
    const expected = ids.map(id => ({ x: c.x - (before[id].y - c.y), y: c.y + (before[id].x - c.x) }));
    ids.forEach((id, i) => {
      const u = e.units[id]; assert(Math.hypot(u.x - expected[i].x, u.y - expected[i].y) < 1e-6, 'Face regenerated or translated the actual formation');
      assert(Math.abs(deltaAngle(u.angle, Math.PI / 2)) < 1e-6, 'Deployed soldiers do not face the ordered direction'); assert.equal(u.hp, before[id].hp);
    });
    const saved = e.captureFormations(); assert(Math.abs(deltaAngle(saved[0].facing, Math.PI / 2)) < 1e-6);
    const restored = new Engine({ ...source, formations: saved });
    restored.regiments.forEach((group, i) => group.unitIds.forEach((id, j) => {
      const original = e.units[e.regiments[i].unitIds[j]], actual = restored.units[id];
      assert(Math.hypot(actual.x - original.x, actual.y - original.y) < 1e-6); assert(Math.abs(deltaAngle(actual.angle, original.angle)) < 1e-6);
    }));
    // A rock sits outside the original footprint but inside its proposed rotation.
    const q = expected.reduce((best, p) => p.x > best.x ? p : best), blockedSource = { ...source, customMap: { biome: 'grassland', regions: [{ type: 'rock', x: q.x - 8, y: q.y - 8, w: 20, h: 20 }] },
      formations: [{ team: 0, type: 'swordsman', count: 16, x: 2500, y: 2200, formation: 'line' }, { team: 1, type: 'guardian', count: 16, x: 4400, y: 2200, formation: 'line' }] };
    const blocked = new Engine(blockedSource), frozen = snapshot(blocked), bc = center(blocked, blocked.regiments[0]);
    assert.equal(blocked.faceRegiments([0], bc.x, bc.y + 1000), 0, 'Deployment Face accepted a footprint through a rock');
    assert.deepEqual(snapshot(blocked), frozen, 'Refused facing partially moved or rotated soldiers');
    assert.equal(blocked.faceRegiments([0, 1], bc.x, bc.y + 1000), 0, 'A partly blocked multi-selection Face was accepted');
    assert.deepEqual(snapshot(blocked), frozen, 'A blocked selected company allowed other selected soldiers to rotate anyway');
    return { rotatedSoldiers: ids.length, memberPositionsPreservedOnReload: restored.units.length, blockedRotationRefused: true,
      multiSelectionRefusalIsAtomic: true };
  });

  test('Paused battle Face visibly turns and reforms around its center then yields to fresh movement', () => {
    const e = new Engine(cfg({ swordsman: 32 }, { guardian: 32 })), r = e.regiments[0], enemy = e.regiments[1];
    e.relocateRegiments([r.id], 2400, 2200); e.relocateRegiments([enemy.id], 5000, 2200); e.command([enemy.id], 'hold'); e.start(); e.pause();
    const original = snapshot(e), c = center(e, r), time = e.time;
    assert.equal(e.command([r.id], 'face', c.x, c.y + 1000), 1, 'A paused facing order was not accepted');
    assert.equal(r.order, 'face'); assert.equal(r.commandStatus, 'facing');
    for (let i = 0; i < 30; i++) e.step(1 / 30);
    assert.equal(e.time, time); assert.deepEqual(snapshot(e), original, 'Paused Face advanced physical state');
    e.resume(); let minimumRatio = Infinity, maximumCenterShift = 0, intermediate = new Set(), settledAt = null;
    for (let tick = 0; tick < 450; tick++) {
      const previous = snapshot(e); e.step(1 / 30); minimumRatio = Math.min(minimumRatio, physics(e, previous).ratio);
      const now = center(e, r); maximumCenterShift = Math.max(maximumCenterShift, Math.hypot(now.x - c.x, now.y - c.y));
      for (const id of r.unitIds) {
        const u = e.units[id]; assert(Math.abs(deltaAngle(u.angle, previous[id].angle)) <= .40, 'Face snapped a soldier through its turn in one frame');
        if (Math.abs(deltaAngle(u.angle, original[id].angle)) > .25 && Math.abs(deltaAngle(u.angle, Math.PI / 2)) > .25) intermediate.add(id);
        if (tick < 3) assert(Math.hypot(u.x - original[id].x, u.y - original[id].y) < .01, 'Face translated ranks before the initial visible turn');
      }
      if (r.order === 'hold') { settledAt = e.time - time; break; }
    }
    assert(settledAt !== null && settledAt <= 15, 'A direction-only Face failed to settle into Hold');
    assert(maximumCenterShift <= 12, 'Face marched the company toward the clicked direction point');
    assert(intermediate.size >= 24, 'Face omitted a visible intermediate turn for most soldiers');
    assert(units(e, r).every(u => Math.abs(deltaAngle(u.angle, Math.PI / 2)) < .20), 'Settled soldiers face the old target instead of their ordered direction');
    const second = center(e, r); assert.equal(e.command([r.id], 'face', second.x - 1000, second.y), 1); e.step(1 / 30);
    const beforeMove = snapshot(e); assert.equal(e.command([r.id], 'move', second.x + 600, second.y), 1);
    for (let tick = 0; tick < 60; tick++) { const previous = snapshot(e); e.step(1 / 30); minimumRatio = Math.min(minimumRatio, physics(e, previous).ratio); }
    const movement = mean(r.unitIds.map(id => e.units[id].x - beforeMove[id].x)); assert(movement >= 30, 'An old facing maneuver suppressed the fresh Move');
    return { settledAt: rounds(settledAt), maximumCenterShift: rounds(maximumCenterShift), intermediateTurnSoldiers: intermediate.size,
      movementAfterCancel: rounds(movement), minimumBodyRatio: rounds(minimumRatio) };
  });

  test('Splitting marching or turning formations preserves physical continuity and the accepted direction of each half', () => {
    const marching = new Engine(cfg({ guardian: 160 }, { guardian: 160 })), lead = marching.regiments[0], enemy = marching.regiments[1];
    marching.relocateRegiments([lead.id], 1400, 2200); marching.relocateRegiments([enemy.id], 5000, 2200);
    marching.command([enemy.id], 'hold'); marching.start();
    for (let tick = 0; tick < 150; tick++) marching.step(1 / 30);
    const marchBefore = snapshot(marching), originalIds = lead.unitIds.slice(), originalUnits = marching.units.slice();
    const added = marching.splitRegiments([lead.id]); assert.equal(added.length, 1);
    const halves = [lead, marching.getRegiment(added[0])], combined = { team: 0, unitIds: originalIds };
    assert.deepEqual(snapshot(marching), marchBefore, 'Splitting an automatic march moved or turned its soldiers immediately');
    assert.deepEqual(halves.flatMap(r => r.unitIds).sort((a, b) => a - b), originalIds.slice().sort((a, b) => a - b));
    marching.units.forEach((u, i) => assert.equal(u, originalUnits[i], 'Splitting replaced existing soldier identities'));
    assert(halves.every(r => r.order === 'auto'));
    let marchDistortion = 0, minimumRatio = Infinity;
    for (let tick = 0; tick < 60; tick++) {
      const previous = snapshot(marching); marching.step(1 / 30); minimumRatio = Math.min(minimumRatio, physics(marching, previous).ratio);
      const s = shape(marching, combined, marchBefore); marchDistortion = Math.max(marchDistortion, s.meanDistortion);
      for (const id of originalIds) assert(marching.units[id].x >= marchBefore[id].x - 5, 'A split half walked backward to repair stale parent offsets');
    }
    const advances = halves.map(r => mean(r.unitIds.map(id => marching.units[id].x - marchBefore[id].x)));
    assert(advances.every(n => n >= 30), 'Splitting stopped an otherwise unobstructed automatic march');
    assert(marchDistortion <= 8, 'Split automatic ranks retained offsets relative to the old parent center');

    const turning = new Engine(cfg({ swordsman: 32 }, { guardian: 32 })), body = turning.regiments[0], held = turning.regiments[1];
    turning.relocateRegiments([body.id], 2400, 2200); turning.relocateRegiments([held.id], 5000, 2200);
    turning.command([held.id], 'hold'); turning.start(); const pivot = center(turning, body);
    assert.equal(turning.faceRegiments([body.id], pivot.x, pivot.y + 1000), 1);
    for (let tick = 0; tick < 3; tick++) turning.step(1 / 30);
    const turnBefore = snapshot(turning), splitAt = turning.time, second = turning.splitRegiments([body.id]); assert.equal(second.length, 1);
    const parts = [body, turning.getRegiment(second[0])], pivots = parts.map(r => center(turning, r));
    assert.deepEqual(snapshot(turning), turnBefore, 'Splitting a visible turn changed a soldier immediately');
    assert(parts.every(r => r.order === 'face' && Math.abs(deltaAngle(r.commandFacing, Math.PI / 2)) < 1e-6));
    const desired = parts.map((r, i) => r.unitIds.map(id => ({ id, x: pivots[i].x - (turnBefore[id].y - pivots[i].y),
      y: pivots[i].y + (turnBefore[id].x - pivots[i].x) })));
    let centerDrift = 0, settledAt = null;
    for (let tick = 0; tick < 450; tick++) {
      const previous = snapshot(turning); turning.step(1 / 30); minimumRatio = Math.min(minimumRatio, physics(turning, previous).ratio);
      parts.forEach((r, i) => { const c = center(turning, r); centerDrift = Math.max(centerDrift, Math.hypot(c.x - pivots[i].x, c.y - pivots[i].y)); });
      if (parts.every(r => r.order === 'hold')) { settledAt = turning.time - splitAt; break; }
    }
    assert(settledAt !== null && settledAt <= 15, 'A split half lost its accepted facing order or could not finish it');
    assert(centerDrift <= 12, 'Split turning ranks reused the old parent center');
    const turnErrors = desired.map(points => mean(points.map(p => Math.hypot(turning.units[p.id].x - p.x, turning.units[p.id].y - p.y))));
    assert(turnErrors.every(n => n <= 12), 'A split half reformed around the wrong pivot');
    assert(parts.every(r => units(turning, r).every(u => Math.abs(deltaAngle(u.angle, Math.PI / 2)) < .20)));
    return { marchHalfProgress: advances.map(rounds), maximumMarchDistortion: rounds(marchDistortion), maximumTurnCenterDrift: rounds(centerDrift),
      turnFootprintErrors: turnErrors.map(rounds), turnSettledAfterSplit: rounds(settledAt), minimumBodyRatio: rounds(minimumRatio) };
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
    try { const detail = body(); results.push({ name, passed: true, milliseconds: rounds(performance.now() - began), ...detail }); console.log('PASS ' + name + ' ' + JSON.stringify(detail)); }
    catch (error) { results.push({ name, passed: false, error: error.message }); console.error('FAIL ' + name + ': ' + error.stack); process.exitCode = 1; }
  });
  console.log(JSON.stringify({ passed: results.filter(r => r.passed).length, failed: results.filter(r => !r.passed).length, results }, null, 2));
}
function rounds(n) { return Math.round(n * 1000) / 1000; }
module.exports = register;
