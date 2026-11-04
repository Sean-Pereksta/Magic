'use strict';
// Public deployment and commands exercise real friendly horse contact.
// Run: node tests/friendly-mounted-test.cjs [alternate-project-root]
const assert = require('node:assert/strict');
const path = require('node:path');
const { performance } = require('node:perf_hooks');
const DT = 1 / 30, CENTER = 2200;
const round = value => Math.round(value * 1000) / 1000;
const mean = values => values.reduce((sum, value) => sum + value, 0) / Math.max(1, values.length);
const living = (e, r) => r.unitIds.map(id => e.units[id]).filter(u => u.alive && !u.routed);
const snapshot = e => e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp, angle: u.angle, power: e.impacts.power(u) }));
const span = us => Math.hypot(Math.max(...us.map(u => u.x)) - Math.min(...us.map(u => u.x)),
  Math.max(...us.map(u => u.y)) - Math.min(...us.map(u => u.y)));
const center = (e, r) => ({ x: mean(living(e, r).map(u => u.x)), y: mean(living(e, r).map(u => u.y)) });
const angleGap = (a, b) => Math.abs(Math.atan2(Math.sin(a - b), Math.cos(a - b)));
function offsets(count, columns, pitch) {
  return Array.from({ length: count }, (_, i) => ({ x: -Math.floor(i / columns) * pitch,
    y: (i % columns - (columns - 1) / 2) * pitch }));
}
function formation(count, x, y, columns, pitch) {
  return { team: 0, type: 'lancer', count, x, y, facing: 0, formation: 'line', offsets: offsets(count, columns, pitch) };
}

function configuration(options = {}) {
  const kind = options.kind || 'packed', pitch = options.pitch || 22.5;
  let groups = [formation(48, 1400, CENTER, 8, pitch)], regions = [];
  if (kind === 'head-on') {
    groups = [formation(1, 1400, CENTER, 1, 28),
      Object.assign(formation(1, 1900, CENTER, 1, 28), { facing: Math.PI })];
  } else if (kind === 'crossing-companies') {
    const count = options.count, files = Math.ceil(Math.sqrt(count));
    const north = formation(count, 2400, CENTER - 230, files, 28);
    north.facing = Math.PI / 2;
    north.offsets = north.offsets.map(p => ({ x: -p.y, y: p.x }));
    groups = [formation(count, 2170, CENTER, files, 28), north];
  } else if (kind === 'interleaved') {
    // Distinct companies occupy alternating, nonoverlapping places in the
    // same moving stream. Both receive Move immediately; neither is parked.
    groups = [formation(24, 1400, CENTER, 6, 45), formation(24, 1422.5, CENTER + 8, 6, 45)];
  } else if (kind === 'courtesy') {
    // Near contact, with a clear perpendicular exit. The small longitudinal
    // separation exercises physical local passage without a distant detour.
    groups = [formation(1, 1400, CENTER, 1, 28), formation(1, 1413, CENTER + 18, 1, 28)];
  } else if (kind === 'mixed-heading') {
    // Two accepted routes meet the same held neighbor from nearby headings.
    // A sideways chain must not push either moving rider backward on its route.
    groups = [formation(1, 1400, CENTER, 1, 28), formation(1, 1413, CENTER + 18, 1, 28),
      Object.assign(formation(1, 1413, CENTER + 40, 1, 28), { facing: -Math.PI / 6 })];
  } else if (kind === 'blocked') {
    groups = [formation(1, 1400, CENTER, 1, 28), formation(1, 1422, CENTER, 1, 28)];
    // Forty units fit one 22-unit horse, but never two abreast. Navigation
    // clearance also fits, so deployment does not repair either initial body.
    regions = [{ type: 'rock', x: 1300, y: 100, w: 1300, h: 2080 },
      { type: 'rock', x: 1300, y: 2220, w: 1300, h: 2000 }];
  }
  const enemies = [];
  if (kind === 'charge') {
    enemies.push({ team: 1, type: 'militia', count: 56, x: 3200, y: CENTER, facing: Math.PI, formation: 'line',
      offsets: Array.from({ length: 56 }, (_, i) => ({ x: Math.floor(i / 14) * 18, y: (i % 14 - 6.5) * 18 })) });
  }
  enemies.push({ team: 1, type: 'guardian', count: 1, x: 5800, y: 3900, facing: Math.PI, formation: 'line', offsets: [{ x: 0, y: 0 }] });
  return { map: 'custom', seed: options.seed || 7137, customMap: { biome: 'grassland', regions },
    armies: [{ name: 'The mounted column', doctrine: 'balanced', roster: { lancer: groups.reduce((sum, g) => sum + g.count, 0) } },
      { name: 'The far line', doctrine: 'defensive', roster: kind === 'charge' ? { militia: 56, guardian: 1 } : { guardian: 1 } }],
    formations: [...groups, ...enemies] };
}

function create(options = {}) {
  const cfg = configuration(options), e = new global.BattleEngine(cfg);
  // The test's starting crowd must be the public saved deployment itself.
  // A repaired overlap or a silently relocated wall fixture is not a repro.
  for (let i = 0; i < cfg.formations.length; i++) {
    const group = cfg.formations[i], r = e.regiments[i];
    assert.equal(r.unitIds.length, group.count, 'The public deployment changed the tested cohort size');
    for (let slot = 0; slot < r.unitIds.length; slot++) {
      const u = e.units[r.unitIds[slot]], p = group.offsets[slot];
      assert(Math.hypot(u.x - group.x - p.x, u.y - group.y - p.y) < .001,
        'The packed fixture required an invalid-deployment repair');
    }
  }
  e.command(e.regiments.map(r => r.id), 'hold'); e.start();
  return { e, groups: e.regiments.filter(r => r.team === 0), target: e.regiments.find(r => r.team === 1 && r.type === 'militia') };
}

function segmentRockGap(a, b, rock) {
  const dx = b.x - a.x, dy = b.y - a.y;
  let lo = 0, hi = 1;
  for (const [start, change, min, max] of [[a.x, dx, rock.x, rock.x + rock.w], [a.y, dy, rock.y, rock.y + rock.h]]) {
    if (Math.abs(change) < 1e-12) { if (start < min || start > max) { lo = 2; break; } }
    else { const t0 = (min - start) / change, t1 = (max - start) / change;
      lo = Math.max(lo, Math.min(t0, t1)); hi = Math.min(hi, Math.max(t0, t1)); }
  }
  if (lo <= hi) return 0;
  const pointGap = p => Math.hypot(Math.max(rock.x - p.x, 0, p.x - rock.x - rock.w),
    Math.max(rock.y - p.y, 0, p.y - rock.y - rock.h));
  let gap = Math.min(pointGap(a), pointGap(b));
  const length2 = dx * dx + dy * dy;
  for (const x of [rock.x, rock.x + rock.w]) for (const y of [rock.y, rock.y + rock.h]) {
    const t = length2 ? Math.max(0, Math.min(1, ((x - a.x) * dx + (y - a.y) * dy) / length2)) : 0;
    gap = Math.min(gap, Math.hypot(a.x + dx * t - x, a.y + dy * t - y));
  }
  return gap;
}

function observer(e, dt = DT) {
  const D = global.GameData, initial = snapshot(e), previous = snapshot(e);
  const rocks = e.map.regions.filter(r => r.type === 'rock');
  const metrics = { minimumBodyRatio: Infinity, maximumStep: 0, closeFriendlyTicks: 0 };
  function step() {
    e.step(dt);
    const active = e.units.filter(u => u.alive && !u.routed && !u.escaped);
    for (let i = 0; i < active.length; i++) {
      const u = active[i], p = previous[u.id], t = D.TYPES[u.type];
      assert([u.x, u.y, u.hp, u.angle].every(Number.isFinite), 'Friendly passage produced invalid physical state');
      assert(u.x >= t.radius && u.y >= t.radius && u.x <= e.width - t.radius && u.y <= e.height - t.radius,
        'Friendly passage moved a body outside the battlefield');
      assert.equal(e.terrainAt(u.x, u.y).blocked, false, 'A friendly sidestep entered solid terrain');
      const moved = Math.hypot(u.x - p.x, u.y - p.y);
      metrics.maximumStep = Math.max(metrics.maximumStep, moved);
      assert(moved <= t.speed * dt * 1.8 + .25, 'Friendly passage exceeded the existing physical movement budget');
      for (const rock of rocks) assert(segmentRockGap(p, u, rock) >= t.radius - .1,
        'A friendly sidestep swept a horse through a rock wall');
      for (let j = 0; j < i; j++) {
        const v = active[j], q = previous[v.id], vt = D.TYPES[v.type], sum = t.radius + vt.radius;
        const reach = sum + (t.speed + vt.speed) * dt * 1.8 + .5;
        if (Math.abs(p.x - q.x) > reach || Math.abs(p.y - q.y) > reach) continue;
        const x = p.x - q.x, y = p.y - q.y, dx = u.x - p.x - v.x + q.x, dy = u.y - p.y - v.y + q.y;
        const vv = dx * dx + dy * dy, f = vv ? Math.max(0, Math.min(1, -(x * dx + y * dy) / vv)) : 0;
        const ratio = Math.hypot(x + dx * f, y + dy * f) / sum;
        metrics.minimumBodyRatio = Math.min(metrics.minimumBodyRatio, ratio);
        assert(ratio >= .90, `Friendly horses crossed occupied bodies at ${e.time.toFixed(2)}s (${u.id}/${v.id}, ${ratio.toFixed(3)})`);
        if (u.team === v.team && ratio < 1.10) metrics.closeFriendlyTicks++;
      }
    }
    for (const team of e.getStats().teams) assert.equal(team.initial, team.alive + team.dead + team.routed,
      'Friendly passage lost or duplicated a soldier');
    for (const u of e.units) { previous[u.id].x = u.x; previous[u.id].y = u.y; }
  }
  return { initial, step,
    run(seconds, visit) { for (let tick = 0; tick < Math.round(seconds / dt); tick++) { step(); if (visit) visit(); } },
    noDamage() {
      assert(e.units.every(u => u.hp === initial[u.id].hp && u.alive && !u.routed), 'Friendly passage caused damage or a casualty');
      assert.equal(e.impacts.stats.contacts, 0, 'A courtesy sidestep was counted as a hostile impact');
      assert.equal(e.impacts.stats.damage, 0, 'A courtesy sidestep added hostile impact damage');
      assert.equal(e.impacts.stats.launched, 0, 'A friendly horse was launched by courtesy movement');
    },
    metrics() { return { minimumBodyRatio: Number.isFinite(metrics.minimumBodyRatio) ? round(metrics.minimumBodyRatio) : null,
      maximumStep: round(metrics.maximumStep), closeFriendlyTicks: metrics.closeFriendlyTicks }; } };
}

function marchTrial(options = {}) {
  const dt = options.dt || DT, { e, groups } = create(options), audit = observer(e, dt), initial = snapshot(e);
  const measured = groups.map(r => ({ initialSpan: span(living(e, r)), maximumSpan: 0, maximumLateral: 0 }));
  for (const r of groups) assert.equal(e.command([r.id], 'move', 4800, center(e, r).y), 1);
  let atFour, firstTenthSpeed = 0;
  for (let tick = 1; tick <= Math.round(12 / dt); tick++) {
    const before = snapshot(e); audit.step();
    if (tick === Math.round(.1 / dt)) firstTenthSpeed = mean(groups.flatMap(r => living(e, r)).map(u => (u.x - before[u.id].x) / dt));
    if (tick === Math.round(4 / dt)) atFour = snapshot(e);
    for (const r of groups) {
      const m = measured[r.id], us = living(e, r); m.maximumSpan = Math.max(m.maximumSpan, span(us));
      for (const u of us) m.maximumLateral = Math.max(m.maximumLateral, Math.abs(u.y - initial[u.id].y));
    }
  }
  audit.noDamage();
  const outcomes = groups.map(r => {
    const us = living(e, r), speeds = us.map(u => (u.x - atFour[u.id].x) / 8), m = measured[r.id];
    return { riders: us.length, sustainedSpeed: round(mean(speeds)), slowestSustainedSpeed: round(Math.min(...speeds)),
      fastRiders: speeds.filter(speed => speed >= global.GameData.TYPES.lancer.speed * .70).length,
      meanAdvance: round(mean(us.map(u => u.x - initial[u.id].x))), leastAdvance: round(Math.min(...us.map(u => u.x - initial[u.id].x))),
      crossed2400: us.filter(u => u.x > 2400).length, initialSpan: round(m.initialSpan), maximumSpan: round(m.maximumSpan),
      maximumLateral: round(m.maximumLateral), cohesion: round(r.cohesion) };
  });
  return { kind: options.kind || 'packed', pitch: options.pitch || 22.5, dt: round(dt), firstTenthSpeed: round(firstTenthSpeed), outcomes, ...audit.metrics() };
}

function chargeTrial() {
  const { e, groups, target } = create({ kind: 'charge' }), r = groups[0], audit = observer(e), initial = snapshot(e);
  const initialSpan = span(living(e, r)), hitters = new Set(), impactEvents = [];
  let firstHit = null, actualDamage = 0, impactDamage = 0, firstImpact = null, inImpact = false, maximumSpan = initialSpan;
  let atFour, atTen;
  const realHit = e._hit, realImpact = e.impacts.beforeMove;
  e._hit = function(attacker, defender, ...args) {
    const hp = Math.max(0, defender.hp), distance = Math.hypot(attacker.x - defender.x, attacker.y - defender.y);
    const value = realHit.call(e, attacker, defender, ...args), damage = hp - Math.max(0, defender.hp);
    assert(!(attacker.team === defender.team && damage > 0), 'Friendly clearance dealt damage to an ally');
    if (defender.regiment === target.id && damage > 0) {
      assert(distance <= global.GameData.TYPES[attacker.type].range + defender._radius + .01,
        'A counted mounted strike occurred beyond real physical reach');
      actualDamage += damage; hitters.add(attacker.id); if (firstHit === null) firstHit = e.time;
      if (inImpact) impactDamage += damage;
    }
    return value;
  };
  e.impacts.beforeMove = function(u, ...args) {
    const contacts = this.stats.contacts, speed = u._mountSpeed, damage = this.stats.damage;
    inImpact = true; const value = realImpact.call(this, u, ...args); inImpact = false;
    if (this.stats.contacts > contacts) {
      impactEvents.push({ speedBefore: speed, speedAfter: u._mountSpeed, damage: this.stats.damage - damage });
      if (firstImpact === null) firstImpact = e.time;
    }
    return value;
  };
  assert.equal(e.command([r.id], 'charge', undefined, undefined, { targetRegimentId: target.id }), 1);
  for (let tick = 1; tick <= 30 * 35; tick++) {
    audit.step();
    if (tick === 120) atFour = snapshot(e);
    if (tick === 300) atTen = snapshot(e);
    if (firstHit === null) maximumSpan = Math.max(maximumSpan, span(living(e, r)));
    if (firstHit !== null && e.time - firstHit >= 2.5) break;
  }
  assert(atFour && atTen && firstHit > 10, 'Crowded Charge did not retain its measured enemy-free run-up');
  const sustainedSpeed = mean(r.unitIds.map(id => (atTen[id].x - atFour[id].x) / 6));
  const hpLoss = target.unitIds.reduce((sum, id) => sum + initial[id].hp - Math.max(0, e.units[id].hp), 0);
  assert(Math.abs(hpLoss - actualDamage) < .001, 'The actual defender health loss differs from observed damage');
  assert.equal(e.impacts.stats.contacts, impactEvents.length, 'Impact accounting counted friendly passage or omitted a real collision');
  assert(Math.abs(e.impacts.stats.damage - impactDamage) < .001, 'Impact damage no longer matches actual struck enemy health');
  return { riders: r.initial, sustainedSpeed: round(sustainedSpeed), initialSpan: round(initialSpan), maximumApproachSpan: round(maximumSpan),
    firstHit: round(firstHit), firstImpact: firstImpact === null ? null : round(firstImpact), actualHitters: hitters.size,
    actualDamage: round(actualDamage), impactDamage: round(impactDamage), impactEvents: impactEvents.map(event => Object.fromEntries(
      Object.entries(event).map(([key, value]) => [key, round(value)]))), ...audit.metrics() };
}

function register(test) {
  const speed = global.GameData.TYPES.lancer.speed;
  const gate = (name, run) => test(name, () => {
    let details;
    try { return run(value => { details = value; return value; }); }
    catch (error) { if (details) error.details = details; throw error; }
  });
  gate('Packed lancers open real space and sustain nearly their clear-file Move pace together', record => {
    const clear = marchTrial({ pitch: 28 }), packed = marchTrial(), p = packed.outcomes[0], c = clear.outcomes[0];
    const result = record({ clear, packed, packedToClearPace: round(p.sustainedSpeed / c.sustainedSpeed) });
    assert(clear.firstTenthSpeed < speed * .10 && packed.firstTenthSpeed < speed * .10,
      'Friendly cooperation created instant forward gallop from rest');
    assert(c.sustainedSpeed >= speed * .80, 'The clear-file control no longer reaches mounted marching speed');
    assert(p.sustainedSpeed >= c.sustainedSpeed * .90,
      `Packed lancers still lose too much real pace to allies (${p.sustainedSpeed} versus ${c.sustainedSpeed})`);
    assert(p.fastRiders >= Math.ceil(p.riders * .90) && p.slowestSustainedSpeed > speed * .60,
      'A fast leading average conceals riders left behind the friendly crowd');
    assert(p.crossed2400 >= Math.ceil(p.riders * .90), 'Friendly bumps still prevent most of the packed troop completing its run-up');
    assert(p.maximumLateral > 2 && packed.closeFriendlyTicks > 100, 'The packed gate did not exercise physical friendly contact and clearance');
    assert(p.maximumSpan <= p.initialSpan * 1.65 + 20 && p.cohesion >= 85, 'Friendly clearance dissolved the packed troop into a tail');
    // A courtesy step and its recipient's ordinary move can happen in either
    // traversal order. Audit their combined frame sweep at the supported long
    // frame as well; checking each small helper segment alone is insufficient.
    const longFrame = marchTrial({ dt: .1 }); result.longFrame = longFrame;
    const long = longFrame.outcomes[0];
    assert(long.meanAdvance > speed * 4 && long.maximumSpan <= long.initialSpan * 1.65 + 20 && long.cohesion >= 85,
      'Long frames turned friendly clearance into a stationary or dispersed crowd');
    return result;
  });

  gate('Head-on friendly lancers keep forward pace as they scoot past each other', record => {
    const { e, groups } = create({ kind: 'head-on' }), audit = observer(e);
    const [east, west] = groups.map(group => e.units[group.unitIds[0]]);
    e.command([groups[0].id], 'move', 4800, CENTER);
    e.command([groups[1].id], 'move', 600, CENTER);
    let approach = 0, minimumPace = Infinity, crossingClearance = 0;
    for (let tick = 0; tick < 150; tick++) {
      const xEast = east.x, xWest = west.x, separation = xWest - xEast;
      audit.step();
      const pace = Math.min(east.x - xEast, xWest - west.x) / DT;
      if (separation > 280) approach = Math.max(approach, pace);
      else if (separation > -50) minimumPace = Math.min(minimumPace, pace);
      if (separation > 0 && east.x >= west.x) crossingClearance = Math.abs(east.y - west.y);
    }
    const result = record({ approach: round(approach), minimumPace: round(minimumPace),
      crossingClearance: round(crossingClearance), ...audit.metrics() });
    assert(approach > speed * .80, 'Both riders must accelerate before they meet');
    assert(minimumPace >= approach * .90, `A friendly approach braked a running lancer (${minimumPace} versus ${approach})`);
    assert(crossingClearance >= (east._radius + west._radius) * .90, 'The riders failed to pass with sideways clearance');
    audit.noDamage();
    return result;
  });

  for (const count of [2, 16]) gate('Two ' + count + '-rider lancer groups crossing at right angles clear the intersection at a riding pace', record => {
    const { e, groups } = create({ kind: 'crossing-companies', count }), audit = observer(e);
    e.command([groups[0].id], 'move', 3900, CENTER);
    e.command([groups[1].id], 'move', 2400, CENTER + 1500);
    const stalled = new Uint16Array(e.units.length), longest = new Uint16Array(e.units.length);
    for (let frame = 0; frame < 240; frame++) {
      const before = e.units.map(u => u.regiment === 0 ? u.x : u.y);
      audit.step();
      if (frame < 60) continue;
      for (const r of groups) for (const id of r.unitIds) {
        const u = e.units[id], progress = (r.id === 0 ? u.x : u.y) - before[id];
        stalled[id] = progress < DT ? stalled[id] + 1 : 0;
        longest[id] = Math.max(longest[id], stalled[id]);
      }
    }
    const minAdvance = groups.map(r => Math.min(...r.unitIds.map(id => r.id === 0 ?
      e.units[id].x - audit.initial[id].x : e.units[id].y - audit.initial[id].y)));
    const result = record({ minAdvance: minAdvance.map(round), longestStall: Math.max(...longest), ...audit.metrics() });
    assert(minAdvance.every(d => d > 600), 'Some riders remained at the crossing or lost most of their run');
    assert(result.longestStall < 5, 'Friendly traffic made a rider wait at an open crossing');
    audit.noDamage();
    return result;
  });

  gate('Interleaved friendly lancer companies accelerate in the same direction without sacrificing either cohort', record => {
    const result = record(marchTrial({ kind: 'interleaved' }));
    assert(result.closeFriendlyTicks > 100, 'The staggered companies never physically approached one another');
    // One company can open the passage while the other keeps its own lane.
    // Require actual sideways clearance, without forcing both to sidestep
    // after their shared route is already clear.
    assert(result.outcomes.some(r => r.maximumLateral > 2), 'Neither company physically made room for the shared passage');
    for (const r of result.outcomes) {
      assert(r.sustainedSpeed >= speed * .80, `One friendly company is still dragging behind its neighbors (${r.sustainedSpeed})`);
      assert(r.fastRiders === r.riders && r.crossed2400 === r.riders,
        'The companies traded one stalled rear cohort for a faster leading cohort');
      assert(r.meanAdvance > speed * 9, 'A staggered company failed to complete its actual run-up');
      assert(r.maximumSpan <= r.initialSpan * 1.45 + 20 && r.cohesion >= 88,
        'Same-direction courtesy movement stretched a company beyond coherent ranks');
    }
    return result;
  });

  gate('A crowded lancer Charge gains real run-up speed and preserves hostile impact and damage accounting', record => {
    const result = record(chargeTrial());
    assert(result.sustainedSpeed >= speed * .80,
      `Friendly ranks still suppress the actual Charge approach (${result.sustainedSpeed})`);
    assert(result.maximumApproachSpan <= result.initialSpan * 1.65 + 20, 'Charge clearance left an excessive pre-contact tail');
    assert(result.actualHitters >= 12 && result.actualDamage > 100 && result.impactEvents.length >= 2,
      'The cleared charge never produced substantial actual enemy contact');
    assert(result.impactEvents.every(event => event.speedBefore > event.speedAfter && event.damage > 0),
      'A hostile impact retained free forward momentum or counted damage without an actual loss of health');
    return result;
  });

  gate('A held friendly horse courteously steps aside without forward momentum, damage, or stale drift', record => {
    const { e, groups } = create({ kind: 'courtesy' }), [riders, held] = groups, audit = observer(e), initial = snapshot(e);
    const receiver = e.units[held.unitIds[0]], mover = e.units[riders.unitIds[0]];
    let maximumShift = 0, maximumForward = 0, maximumPower = 0, maximumFacingError = 0;
    const observe = () => {
      maximumShift = Math.max(maximumShift, receiver.y - initial[receiver.id].y);
      maximumForward = Math.max(maximumForward, Math.abs(receiver.x - initial[receiver.id].x));
      maximumPower = Math.max(maximumPower, e.impacts.power(receiver));
      maximumFacingError = Math.max(maximumFacingError, angleGap(receiver.angle, initial[receiver.id].angle));
    };
    e.command([riders.id], 'move', 4800, CENTER); audit.run(1, observe);
    record({ lateralClearance: round(maximumShift), forwardAdvance: round(mover.x - initial[mover.id].x), ...audit.metrics() });
    assert(maximumShift >= receiver._radius * .25, 'The actual friendly blocker never made room sideways');
    assert(mover.x - initial[mover.id].x > 35 && Math.abs(mover.y - initial[mover.id].y) < 2,
      'The incoming horse still had to abandon its lane instead of the blocker yielding');
    const paused = snapshot(e), pausedTime = e.time; e.pause(); audit.run(.3);
    assert.deepEqual(snapshot(e), paused, 'Paused horses continued a queued courtesy movement');
    assert.equal(e.time, pausedTime); e.resume(); audit.run(2, observe);
    e.command([riders.id], 'hold'); const settled = { x: receiver.x, y: receiver.y }; audit.run(2, observe);
    const staleDrift = Math.hypot(receiver.x - settled.x, receiver.y - settled.y);
    assert(staleDrift < .02 && maximumShift < receiver._radius * 2,
      'A held horse drifted indefinitely or reclaimed the cleared lane after the riders passed');
    assert(maximumForward < .05 && maximumFacingError < .001 && maximumPower === 0 && held.order === 'hold',
      'Courtesy movement changed a held horse facing, command, or forward charge momentum');
    audit.noDamage();
    return { lateralClearance: round(maximumShift), maximumForward: round(maximumForward), maximumPower,
      maximumFacingError: round(maximumFacingError), staleDrift: round(staleDrift), ...audit.metrics() };
  });

  gate('Fresh Hold, Face, Retreat, and reversed Move cancel previous friendly passage requests', record => {
    const outcomes = [], mixedHeading = {};
    record({ outcomes, mixedHeading });
    for (const order of ['hold', 'face', 'retreat', 'move']) {
      const { e, groups } = create({ kind: 'courtesy' }), [riders, held] = groups, audit = observer(e);
      const mover = e.units[riders.unitIds[0]], receiver = e.units[held.unitIds[0]];
      e.command([riders.id], 'move', 4800, CENTER); audit.run(.3);
      const before = snapshot(e), x = order === 'face' ? mover.x : 1000, y = order === 'face' ? mover.y + 500 : mover.y;
      assert.equal(e.command([riders.id], order, x, y), 1);
      // Retreat chooses its axis from the actual enemy disposition. The far
      // observer is diagonal, and a later regroup leg can turn toward allies.
      const goalDX = order === 'retreat' ? riders._withdrawGoalX - mover.x : x - mover.x;
      const goalDY = order === 'retreat' ? riders._withdrawGoalY - mover.y : y - mover.y;
      const goalLength = Math.max(1, Math.hypot(goalDX, goalDY)), nx = goalDX / goalLength, ny = goalDY / goalLength;
      audit.run(.1);
      const initialMotion = Math.hypot(mover.x - before[mover.id].x, mover.y - before[mover.id].y);
      const oldRecipientMotion = Math.hypot(receiver.x - before[receiver.id].x, receiver.y - before[receiver.id].y);
      const withdrawing = order === 'retreat' || order === 'move';
      const firstProgress = (mover.x - before[mover.id].x) * nx + (mover.y - before[mover.id].y) * ny;
      assert((withdrawing ? firstProgress > .01 : initialMotion < .01) && oldRecipientMotion < .02,
        `A fresh ${order} retained movement from the old friendly passage`);
      assert(e.impacts.power(mover) < (withdrawing ? .01 : .000001), `A fresh ${order} retained old forward gallop energy`);
      let maximumCommandProgress = 0;
      audit.run(order === 'retreat' || order === 'move' ? 3.9 : .5, () => {
        maximumCommandProgress = Math.max(maximumCommandProgress,
          (mover.x - before[mover.id].x) * nx + (mover.y - before[mover.id].y) * ny);
      });
      const result = { order, initialMotion: round(initialMotion), oldRecipientMotion: round(oldRecipientMotion),
        maximumCommandProgress: round(maximumCommandProgress), ...audit.metrics() };
      outcomes.push(result);
      if (order === 'retreat' || order === 'move') assert(maximumCommandProgress > speed * 1.8,
        `The old passage direction overruled the new ${order} direction (${maximumCommandProgress.toFixed(3)})`);
      else assert(Math.hypot(mover.x - before[mover.id].x, mover.y - before[mover.id].y) < .01,
        `The new ${order} did not retain its actual location`);
      audit.noDamage();
    }
    for (const order of ['face', 'retreat']) {
      const { e, groups } = create({ kind: 'courtesy' }), [riders, receiver] = groups, audit = observer(e);
      const u = e.units[receiver.unitIds[0]], before = snapshot(e);
      e.command([riders.id], 'move', 4800, CENTER);
      assert.equal(e.command([receiver.id], order, u.x, u.y + 500), 1); audit.run(.1);
      const mx = u.x - before[u.id].x, my = u.y - before[u.id].y;
      const along = mx * receiver._retreatNX + my * receiver._retreatNY;
      assert(order === 'face' ? Math.hypot(mx, my) < .01 : along > .01,
        `An incoming friendly request displaced a horse during its fresh ${order} turn`);
      assert(e.impacts.power(u) < .01); audit.noDamage();
    }
    {
      const { e, groups } = create({ kind: 'mixed-heading' }), audit = observer(e), initial = snapshot(e);
      const routes = [{ r: groups[0], nx: 1, ny: 0 },
        { r: groups[2], nx: Math.cos(-Math.PI / 6), ny: Math.sin(-Math.PI / 6) }];
      assert.equal(e.command([routes[0].r.id], 'move', 4800, CENTER), 1);
      const diagonal = e.units[routes[1].r.unitIds[0]];
      assert.equal(e.command([routes[1].r.id], 'move', diagonal.x + routes[1].nx * 1600,
        diagonal.y + routes[1].ny * 1600), 1);
      const previous = snapshot(e);
      let minimumCommandStep = 0;
      audit.run(3, () => {
        for (const route of routes) {
          const u = e.units[route.r.unitIds[0]], before = previous[u.id];
          minimumCommandStep = Math.min(minimumCommandStep, (u.x - before.x) * route.nx + (u.y - before.y) * route.ny);
          before.x = u.x; before.y = u.y;
        }
      });
      const commandProgress = routes.map(route => {
        const u = e.units[route.r.unitIds[0]], start = initial[u.id];
        return (u.x - start.x) * route.nx + (u.y - start.y) * route.ny;
      });
      Object.assign(mixedHeading, { minimumCommandStep: round(minimumCommandStep),
        commandProgress: commandProgress.map(round), ...audit.metrics() });
      assert(minimumCommandStep >= -.000001, 'A friendly clearance chain shunted a rider backward along its accepted Move direction');
      assert(commandProgress.every(progress => progress > speed * 1.5) && mixedHeading.closeFriendlyTicks > 0,
        'Respecting the neighboring route stopped the real close-contact passage');
      audit.noDamage();
    }
    return { outcomes, receiverTurnControls: ['face', 'retreat'], mixedHeading };
  });

  gate('A wall-bounded friendly queue cannot manufacture charge energy and accelerates only after real space opens', record => {
    const { e, groups } = create({ kind: 'blocked' }), [riders, held] = groups, audit = observer(e), initial = snapshot(e);
    const mover = e.units[riders.unitIds[0]], receiver = e.units[held.unitIds[0]];
    e.command([riders.id], 'move', 4800, CENTER);
    let maximumBlockedPower = 0, stationaryChecks = 0, last = { x: mover.x, y: mover.y };
    audit.run(4, () => {
      const distance = Math.hypot(mover.x - last.x, mover.y - last.y);
      if (e.time >= 2 && distance < .01) { stationaryChecks++; maximumBlockedPower = Math.max(maximumBlockedPower, e.impacts.power(mover)); }
      last = { x: mover.x, y: mover.y };
    });
    const blockedAdvance = mover.x - initial[mover.id].x;
    const result = record({ blockedAdvance: round(blockedAdvance), stationaryChecks, maximumBlockedPower: round(maximumBlockedPower) });
    assert(stationaryChecks >= 40 && blockedAdvance < 12 && mover.x < receiver.x,
      'The wall control did not physically stop the friendly queue');
    assert(maximumBlockedPower < .01, 'A horse accumulated charge energy while unable to move through its ally');
    const releasedX = mover.x; assert.equal(e.command([held.id], 'move', 2400, CENTER), 1);
    let atFour;
    for (let tick = 1; tick <= 180; tick++) { audit.step(); if (tick === 120) atFour = mover.x; }
    const finalSpeed = (mover.x - atFour) / 2;
    Object.assign(result, { releasedAdvance: round(mover.x - releasedX), finalForwardSpeed: round(finalSpeed), ...audit.metrics() });
    assert(mover.x - releasedX > speed * 3 && finalSpeed > speed * .60,
      'The queue remained stuck after its leading horse physically moved away');
    audit.noDamage();
    return result;
  });
}

if (require.main === module) {
  const root = process.argv[2] || path.resolve(__dirname, '..'); global.window = global;
  for (const name of ['data', 'tactics', 'impacts', 'siege', 'engine']) require(path.join(root, 'src', name + '.js'));
  const results = [], filter = (process.env.IRONFIELD_TEST_FILTER || '').toLowerCase();
  register((name, run) => {
    if (filter && !name.toLowerCase().includes(filter)) return;
    const start = performance.now();
    try { const detail = run(); results.push({ name, passed: true, milliseconds: round(performance.now() - start), ...detail });
      console.log('PASS ' + name + ' ' + JSON.stringify(detail)); }
    catch (error) { results.push({ name, passed: false, error: error.message, details: error.details });
      console.error('FAIL ' + name + ' ' + error.stack); process.exitCode = 1; }
  });
  console.log(JSON.stringify({ passed: results.filter(r => r.passed).length, failed: results.filter(r => !r.passed).length, results }, null, 2));
}
module.exports = register;
module.exports.configuration = configuration;
module.exports.marchTrial = marchTrial;
module.exports.chargeTrial = chargeTrial;
module.exports.observer = observer;
