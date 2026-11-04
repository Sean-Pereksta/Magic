'use strict';
// Physical tests for stable marching and persistent army plans.
// Run: node tests/mobility-tactics-test.cjs [alternate-project-root]
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const { performance } = require('node:perf_hooks');

function register(test) {
  const D = global.GameData, Engine = global.BattleEngine;
  const round = x => Math.round(x * 1000) / 1000;
  const mean = xs => xs.reduce((a, b) => a + b, 0) / Math.max(1, xs.length);
  const members = (e, r) => r.unitIds.map(id => e.units[id]).filter(u => u.alive && !u.routed);
  const center = (e, r) => { const us = members(e, r); return { x: mean(us.map(u => u.x)), y: mean(us.map(u => u.y)) }; };
  const positions = e => e.units.map(u => [u.x, u.y, u.angle, u.hp]);
  const body = (e, r) => {
    const us = members(e, r), xs = us.map(u => u.x).sort((a, b) => a - b), ys = us.map(u => u.y).sort((a, b) => a - b);
    return { x: mean(xs), y: mean(ys), width: ys.at(-1) - ys[0], depth: xs.at(-1) - xs[0],
      coreDepth: xs[Math.floor(xs.length * .95)] - xs[Math.floor(xs.length * .05)],
      coreWidth: ys[Math.floor(ys.length * .95)] - ys[Math.floor(ys.length * .05)] };
  };
  const span = us => {
    let maximum = 0;
    for (let i = 0; i < us.length; i++) for (let j = i + 1; j < us.length; j++) maximum = Math.max(maximum, Math.hypot(us[i].x - us[j].x, us[i].y - us[j].y));
    return maximum;
  };

  function observer(e) {
    const oldX = new Float64Array(e.units.length), oldY = new Float64Array(e.units.length);
    for (const u of e.units) { oldX[u.id] = u.x; oldY[u.id] = u.y; }
    const cell = 96, cols = Math.ceil(e.width / cell) + 1, rows = Math.ceil(e.height / cell) + 1;
    const heads = new Int32Array(cols * rows), next = new Int32Array(e.units.length);
    let minimumBodyRatio = Infinity, maximumStep = 0;
    const hits = [], raw = e._attack;
    e._attack = function(a, b) {
      const before = Math.max(0, b.hp), dx = a.x - b.x, dy = a.y - b.y, distance = Math.hypot(dx, dy);
      const t = D.TYPES[a.type], facing = e.regiments[b.regiment].facing;
      const result = raw.call(this, a, b), damage = before - Math.max(0, b.hp);
      if (damage > 0 && !t.ranged) {
        const reach = t.range * (e.terrainAt(a.x, a.y).elevation ? 1.12 : 1) + D.TYPES[b.type].radius;
        assert(distance <= reach + .01, 'A tactic landed a melee hit outside actual weapon reach');
        hits.push({ time: e.time, attacker: a.id, regiment: a.regiment, targetRegiment: b.regiment, team: a.team, type: a.type,
          x: a.x, y: a.y, distance, damage, rearProjection: dx * Math.cos(facing) + dy * Math.sin(facing) });
      }
      return result;
    };
    function step() {
      e.step(1 / 30); heads.fill(-1);
      for (const u of e.units) if (u.alive && !u.routed) {
        assert([u.x, u.y, u.hp, u.angle].every(Number.isFinite), 'Nonfinite physical state');
        assert(u.x >= 0 && u.x <= e.width && u.y >= 0 && u.y <= e.height, 'Tactic left world bounds');
        assert.equal(e.terrainAt(u.x, u.y).blocked, false, 'Tactic crossed impassable terrain');
        const moved = Math.hypot(u.x - oldX[u.id], u.y - oldY[u.id]); maximumStep = Math.max(maximumStep, moved);
        assert(moved <= D.TYPES[u.type].speed / 30 * 1.8 + .25, 'Formation change teleported a soldier');
        const gx = Math.floor(oldX[u.id] / cell), gy = Math.floor(oldY[u.id] / cell);
        for (let y = Math.max(0, gy - 1); y <= Math.min(rows - 1, gy + 1); y++) for (let x = Math.max(0, gx - 1); x <= Math.min(cols - 1, gx + 1); x++) {
          for (let id = heads[y * cols + x]; id !== -1; id = next[id]) {
            const v = e.units[id], sum = D.TYPES[u.type].radius + D.TYPES[v.type].radius;
            const dx = oldX[u.id] - oldX[id], dy = oldY[u.id] - oldY[id];
            if (Math.abs(dx) > sum + 16 || Math.abs(dy) > sum + 16) continue;
            const vx = u.x - oldX[u.id] - v.x + oldX[id], vy = u.y - oldY[u.id] - v.y + oldY[id], vv = vx * vx + vy * vy;
            const t = vv ? Math.max(0, Math.min(1, -(dx * vx + dy * vy) / vv)) : 0;
            const ratio = Math.hypot(dx + vx * t, dy + vy * t) / sum;
            minimumBodyRatio = Math.min(minimumBodyRatio, ratio);
            assert(ratio >= .90, `Formation bodies crossed at ${e.time.toFixed(2)}s: ${u.id}/${id}, separation ${ratio.toFixed(3)}`);
          }
        }
        const key = gy * cols + gx; next[u.id] = heads[key]; heads[key] = u.id;
      }
      for (const u of e.units) { oldX[u.id] = u.x; oldY[u.id] = u.y; }
    }
    return { step, hits, run(seconds) { for (let i = 0; i < seconds * 30 && e.phase === 'running'; i++) step(); },
      metrics() { for (const t of e.getStats().teams) assert.equal(t.initial, t.alive + t.dead + t.routed); return { minimumBodyRatio: round(minimumBodyRatio), maximumStep: round(maximumStep) }; } };
  }

  function flankFixture(plan = 'flank') {
    const e = new Engine({ map: 'custom', seed: 3331, customMap: { biome: 'grassland', regions: [] }, armies: [
      { name: 'Maneuver force', doctrine: plan, roster: { swordsman: 160, cavalry: 80, lancer: 80 } },
      { name: 'Held line', doctrine: 'balanced', roster: { guardian: 160, pikeman: 160 } }
    ], formations: [
      { team: 0, type: 'swordsman', count: 160, x: 2400, y: 2200, facing: 0 },
      { team: 0, type: 'cavalry', count: 80, x: 2200, y: 1800, facing: 0 },
      { team: 0, type: 'lancer', count: 80, x: 2200, y: 2600, facing: 0 },
      { team: 1, type: 'guardian', count: 160, x: 3400, y: 2000, facing: Math.PI },
      { team: 1, type: 'pikeman', count: 160, x: 3400, y: 2400, facing: Math.PI }
    ] });
    e.command(e.regiments.filter(r => r.team === 1).map(r => r.id), 'hold'); e.start();
    return e;
  }

  test('Open-ground clearance cache exactly matches solid terrain and bridge geometry', () => {
    // Intentionally independent, locked reference of the pre-cache 1.9.1
    // geometry. This guards the optimization, including the 64-unit cutoff.
    const contains = (x, y, r, padding = 0) => x >= r.x - padding && x <= r.x + r.w + padding && y >= r.y - padding && y <= r.y + r.h + padding;
    const exact = (e, x, y, radius) => {
      const blocks = e.map.regions.filter(r => r.type === 'rock' || r.type === 'water'), bridges = e.map.regions.filter(r => r.type === 'bridge');
      if (x < radius + 8 || y < radius + 8 || x > e.width - radius - 8 || y > e.height - radius - 8) return true;
      if (!radius) return !bridges.some(r => contains(x, y, r)) && blocks.some(r => contains(x, y, r));
      return blocks.some(block => contains(x, y, block, radius + 1) && (block.type === 'rock' || !bridges.some(bridge => contains(x, y, bridge, -radius))));
    };
    const radii = [0, 5.5, 12, 28, 63.999, 64, 64.001, 100];
    // Keep the locked pre-siege oracle on its original eight map types.
    // Destructible walls and dynamic ramps have separate geometry checks in
    // impact-siege-test; they did not exist in the 1.9.1 reference predicate.
    const legacyMaps = D.MAPS.filter(map => ['meadow', 'river', 'desert', 'frost', 'greenwood', 'highland', 'basin', 'custom'].includes(map.id));
    assert.equal(legacyMaps.length, 8);
    let comparisons = 0;
    for (const map of legacyMaps) {
      const config = { map: map.id, seed: 4719, armies: [{ roster: {} }, { roster: {} }] };
      if (map.id === 'custom') config.customMap = { biome: 'grassland', regions: [
        { type: 'water', x: 2700, y: 800, w: 500, h: 2800 },
        { type: 'bridge', x: 2650, y: 2100, w: 600, h: 200 },
        { type: 'rock', x: 3500, y: 1000, w: 300, h: 500 }
      ] };
      const e = new Engine(config), queries = [];
      for (let y = 0; y <= e.height; y += 389) for (let x = 0; x <= e.width; x += 397) for (const radius of radii) queries.push([x, y, radius]);
      for (const region of e.map.regions) if (['rock', 'water', 'bridge'].includes(region.type)) {
        for (const radius of radii) for (const epsilon of [-1.001, -.001, 0, .001, 1.001]) {
          for (const x of [region.x - radius + epsilon, region.x + radius + epsilon, region.x + region.w - radius + epsilon, region.x + region.w + radius + epsilon]) queries.push([x, region.y + region.h / 2, radius]);
          for (const y of [region.y - radius + epsilon, region.y + radius + epsilon, region.y + region.h - radius + epsilon, region.y + region.h + radius + epsilon]) queries.push([region.x + region.w / 2, y, radius]);
        }
      }
      for (const [x, y, radius] of queries) {
        assert.equal(e._bodyBlocked(x, y, radius), exact(e, x, y, radius), 'Clearance cache disagrees at ' + JSON.stringify({ map: map.id, x, y, radius }));
        comparisons++;
      }
    }
    return { maps: legacyMaps.length, radii, comparisons };
  });

  test('Forest marching preserves physical ranks without an accordion tail', () => {
    const e = new Engine({ map: 'custom', seed: 99872, customMap: { biome: 'grassland', regions: [{ type: 'forest', x: 1800, y: 600, w: 1800, h: 3200 }] }, armies: [
      { name: 'Marching ranks', doctrine: 'balanced', roster: { swordsman: 160 } }, { name: 'Distant line', doctrine: 'balanced', roster: { guardian: 160 } }
    ] });
    const r = e.regiments.find(r => r.team === 0), foe = e.regiments.find(r => r.team === 1);
    e.relocateRegiments([r.id], 1400, 2200); e.relocateRegiments([foe.id], 5500, 2200);
    const initial = body(e, r), audit = observer(e), originalSlots = e._assignTravelSlots;
    let slotAssignments = 0, greatestDepth = initial.depth, leastCoreWidth = initial.coreWidth, forestVisits = new Set();
    e._assignTravelSlots = function(...args) { if (args[0].id === r.id) slotAssignments++; return originalSlots.apply(this, args); };
    e.start(); e.command([r.id], 'move', 4200, 2200); e.command([foe.id], 'hold');
    for (let tick = 0; tick < 1200; tick++) {
      audit.step();
      if (tick % 15 === 0) {
        const measured = body(e, r); greatestDepth = Math.max(greatestDepth, measured.depth); leastCoreWidth = Math.min(leastCoreWidth, measured.coreWidth);
        for (const u of members(e, r)) if (e.terrainAt(u.x, u.y).type === 'forest') forestVisits.add(u.id);
      }
    }
    const final = body(e, r);
    assert.equal(forestVisits.size, 160, 'The fixture did not actually traverse the forest');
    assert(final.x - initial.x > 1000 && members(e, r).every(u => u.x > 2300), 'Avoiding rearrangement stopped the march');
    assert(greatestDepth <= initial.depth * 1.35 + 10, 'A soft forest more than doubled the ranks into a long tail');
    assert(leastCoreWidth >= initial.coreWidth * .75, 'The forest collapsed the frontage into narrow files');
    assert(r.cohesion >= 90, 'A clear forest approach needlessly destroyed formation cohesion');
    return { troops: 160, initialDepth: round(initial.depth), greatestDepth: round(greatestDepth), finalDepth: round(final.depth),
      meanAdvance: round(final.x - initial.x), initialFrontage: round(initial.coreWidth), leastFrontage: round(leastCoreWidth),
      finalCohesion: round(r.cohesion), slotAssignments, ...audit.metrics() };
  });

  test('Encircle rides outside both wings and delivers real cavalry attacks from the rear', () => {
    const e = flankFixture(), audit = observer(e), riders = e.regiments.filter(r => r.team === 0 && ['cavalry', 'lancer'].includes(r.type));
    const observations = riders.map(r => ({ id: r.id, type: r.type, initialSpan: span(members(e, r)), maximumTransitSpan: 0,
      phases: new Set(), outside: false, behind: false, firstRearHit: null, rearAttackers: new Set() }));
    for (let tick = 0; tick < 60 * 30 && e.phase === 'running'; tick++) {
      audit.step();
      for (const record of observations) {
        const r = e.regiments[record.id], p = r._tactic, c = center(e, r);
        if (p) record.phases.add(p.stage);
        if (c.x > 3200 && (c.y < 1750 || c.y > 2650)) record.outside = true;
        if (c.x > 3520) record.behind = true;
        if (p && p.transit && tick % 30 === 0) record.maximumTransitSpan = Math.max(record.maximumTransitSpan, span(members(e, r)));
      }
    }
    for (const hit of audit.hits) {
      const record = observations.find(r => r.id === hit.regiment);
      if (record && hit.rearProjection < -8) {
        record.rearAttackers.add(hit.attacker);
        if (record.firstRearHit === null) record.firstRearHit = hit.time;
      }
    }
    for (const r of observations) {
      assert(r.outside && r.behind, `${r.type} did not physically travel outside a wing and behind the enemy`);
      assert(['outflank', 'round-wing', 'rear-approach', 'rear-charge'].every(stage => r.phases.has(stage)), 'Encircle skipped a maneuver phase');
      assert(r.maximumTransitSpan <= r.initialSpan * 2.1, 'A cavalry wing stretched into a long single-file tail before combat');
      assert(r.rearAttackers.size >= 12, `${r.type} displayed a rear-charge label without sustained actual rear attacks`);
    }
    return { units: e.units.length, cavalry: observations.map(r => ({ ...r, phases: [...r.phases], initialSpan: round(r.initialSpan),
      maximumTransitSpan: round(r.maximumTransitSpan), firstRearHit: round(r.firstRearHit), rearAttackers: r.rearAttackers.size })), ...audit.metrics() };
  });

  test('Army plans produce different physical advances and reserves instead of only different labels', () => {
    const outcomes = {};
    const legacyPlans=['balanced', 'aggressive', 'defensive', 'flank', 'hammer', 'refuseLeft', 'refuseRight', 'skirmish'];
    for(const id of legacyPlans)assert(D.DOCTRINES[id], 'An original army plan disappeared: '+id);
    // Preserve every original physical-outcome assertion. Six new plans have
    // their own independent behavior and command-priority fixtures.
    for (const plan of legacyPlans) {
      const e = new Engine({ map: 'custom', seed: 4482, customMap: { biome: 'grassland', regions: [] }, armies: [
        { name: 'Two wings', doctrine: plan, roster: { swordsman: 128, archer: 32 } },
        { name: 'Waiting line', doctrine: 'balanced', roster: { guardian: 128 } }
      ], formations: [
        { team: 0, type: 'swordsman', count: 64, x: 1800, y: 1600, facing: 0 },
        { team: 0, type: 'swordsman', count: 64, x: 1800, y: 2800, facing: 0 },
        { team: 0, type: 'archer', count: 32, x: 1600, y: 2200, facing: 0 },
        { team: 1, type: 'guardian', count: 64, x: 4200, y: 1600, facing: Math.PI },
        { team: 1, type: 'guardian', count: 64, x: 4200, y: 2800, facing: Math.PI }
      ] });
      assert.equal(e.config.armies[0].doctrine, plan);
      const wings = e.regiments.filter(r => r.team === 0 && r.type === 'swordsman');
      const initial = wings.map(r => center(e, r));
      e.command(e.regiments.filter(r => r.team === 1).map(r => r.id), 'hold'); e.start();
      for (let tick = 0; tick < 180; tick++) e.step(1 / 30);
      outcomes[plan] = wings.map((r, i) => ({ wing: i ? 'right' : 'left', advance: round(center(e, r).x - initial[i].x),
        stage: r._tactic.stage, role: r._tactic.role, goal: [round(r._goalX), round(r._goalY)] }));
      assert(wings.every(r => r.order === 'auto'), 'Automatic strategy was exposed as a permanent manual order');
    }
    for (const id of ['balanced', 'aggressive', 'flank']) assert(outcomes[id].every(r => r.advance > 100), id + ' did not advance both infantry wings');
    for (const id of ['defensive', 'hammer']) assert(outcomes[id].every(r => Math.abs(r.advance) < 2), id + ' failed to physically hold its anvil');
    assert(outcomes.refuseLeft[0].advance > 100 && Math.abs(outcomes.refuseLeft[1].advance) < 2, 'Attack left failed to preserve the opposite wing');
    assert(outcomes.refuseRight[1].advance > 100 && Math.abs(outcomes.refuseRight[0].advance) < 2, 'Attack right failed to preserve the opposite wing');
    assert(outcomes.skirmish.every(r => r.advance < -25), 'The infantry screen did not physically cover its missile troops');
    const verticalMarches = [];
    for (const direction of [-1, 1]) {
      // Forty soldiers have complete8×5ranks and therefore exactly equal X
      // centers. A nearly vertical test would miss an erroneous `nx || 1`.
      const ownY = direction > 0 ? 1100 : 3300, enemyY = direction > 0 ? 3300 : 1100;
      const e = new Engine({ map: 'custom', seed: 8891, customMap: { biome: 'grassland', regions: [] }, armies: [
        { doctrine: 'balanced', roster: { swordsman: 40 } }, { doctrine: 'balanced', roster: { guardian: 40 } }
      ], formations: [
        { team: 0, type: 'swordsman', count: 40, x: 3200, y: ownY, facing: direction * Math.PI / 2 },
        { team: 1, type: 'guardian', count: 40, x: 3200, y: enemyY, facing: -direction * Math.PI / 2 }
      ] });
      const r = e.regiments[0], before = center(e, r); e.command([1], 'hold'); e.start();
      const p = e._tactics.plans[0];
      assert(Math.abs(p.nx) < 1e-8 && Math.abs(p.ny - direction) < 1e-8, 'An exactly vertical plan acquired a diagonal axis');
      assert(Math.abs(Math.hypot(p.nx, p.ny) - 1) < 1e-8 && Math.abs(p.nx * p.tx + p.ny * p.ty) < 1e-8, 'Strategy axes are not orthonormal');
      assert(Math.abs(e._battleAxis.x) < 1e-8 && Math.abs(e._battleAxis.y - direction) < 1e-8, 'Engagement planning changed the exact vertical axis');
      for (let tick = 0; tick < 90; tick++) e.step(1 / 30);
      const after = center(e, r), progress = (after.y - before.y) * direction, lateral = Math.abs(after.x - before.x);
      assert(progress > 80 && lateral < 3, 'A north/south formation veered sideways instead of marching toward its front');
      verticalMarches.push({ direction, progress: round(progress), lateralDrift: round(lateral) });
    }
    return { sampledSeconds: 6, plans: outcomes, verticalMarches };
  });

  test('New manual orders interrupt an outer cavalry route and remain in charge after a live plan change', () => {
    const outcomes = [];
    for (const order of ['move', 'hold', 'retreat']) {
      const e = flankFixture(), r = e.regiments.find(r => r.team === 0 && r.type === 'cavalry'), audit = observer(e);
      audit.run(4);
      assert(r._tactic && r._tactic.transit, 'The order fixture was not following an actual outer route');
      const before = positions(e), start = center(e, r);
      assert.equal(e.command([r.id], order, start.x - 500, start.y), 1);
      assert.deepEqual(positions(e), before, 'Accepting an order instantly moved or turned cavalry');
      const epoch = r._commandEpoch, time = e.time;
      e.pause(); assert.equal(e.setDoctrine(0, 'hammer'), true);
      assert.equal(e.time, time); assert.deepEqual(positions(e), before, 'A paused plan change moved soldiers');
      assert.equal(r.order, order); assert.equal(r._commandEpoch, epoch);
      const dx = order === 'retreat' ? r.targetX - start.x : -1, dy = order === 'retreat' ? r.targetY - start.y : 0, length = Math.max(.001, Math.hypot(dx, dy));
      const nx = dx / length, ny = dy / length;
      let turnMotion = 0; e.resume();
      for (let tick = 0; tick < 60; tick++) {
        audit.step();
        if (tick < 3) turnMotion = Math.max(turnMotion, ...members(e, r).map(u => Math.hypot(u.x - before[u.id][0], u.y - before[u.id][1])));
      }
      const progress = members(e, r).map(u => (u.x - before[u.id][0]) * nx + (u.y - before[u.id][1]) * ny);
      assert.equal(r.order, order);
      if (order === 'hold') assert(members(e, r).every(u => Math.hypot(u.x - before[u.id][0], u.y - before[u.id][1]) < .01), 'Hold resumed an obsolete flank route');
      else {
        assert(mean(progress) >= 35 && progress.filter(x => x > 15).length >= 60, 'Manual movement did not take physical control of the outer wing');
        if (order === 'retreat') assert(turnMotion > .01, 'Retreat stalled during its first three steps');
      }
      outcomes.push({ order, meanDirectedProgress: round(mean(progress)), movingAtLeastFifteen: progress.filter(x => x > 15).length,
        firstTenthMotion: round(turnMotion), ...audit.metrics() });
    }
    const split = flankFixture(), group = split.regiments.find(r => r.team === 0 && r.type === 'cavalry');
    for (let tick = 0; tick < 120; tick++) split.step(1 / 30);
    const beforeSplit = positions(split), phase = group._tactic.stage, added = split.splitRegiments([group.id]);
    assert.equal(added.length, 1); const child = split.getRegiment(added[0]);
    assert.deepEqual(positions(split), beforeSplit, 'Splitting an active maneuver moved the bodies');
    assert.equal(group.alive, 40); assert.equal(child.alive, 40);
    assert.equal(group._tactic.stage, phase); assert.equal(child._tactic.stage, phase);
    assert.notEqual(group._tactic, child._tactic); assert.notEqual(group._tactic.waypoints, child._tactic.waypoints);
    assert(group._tactic.waypoints.every((point, i) => point !== child._tactic.waypoints[i]), 'Split halves share mutable route points');
    const parentRoute = JSON.stringify(group._tactic.waypoints);
    split.command([child.id], 'hold');
    assert.equal(group.order, 'auto'); assert.equal(child.order, 'hold');
    assert.equal(JSON.stringify(group._tactic.waypoints), parentRoute, 'Commanding one half altered the other half route');
    return { outcomes, splitIndependentRoutes: true, splitHalfSizes: [group.alive, child.alive] };
  });

  test('Hammer and anvil holds the foot line and visibly waits before releasing a rear charge', () => {
    const e = flankFixture('hammer'), audit = observer(e), foot = e.regiments.find(r => r.team === 0 && r.type === 'swordsman');
    const cavalry = e.regiments.find(r => r.team === 0 && r.type === 'cavalry'), initialFoot = positions(e);
    let footMotionAtTwenty = Infinity, waitingStart = null, waitingTicks = 0, waitingMotion = 0, releaseAt = null;
    for (let tick = 0; tick < 65 * 30 && e.phase === 'running'; tick++) {
      audit.step();
      if (tick === 599) footMotionAtTwenty = Math.max(...members(e, foot).map(u => Math.hypot(u.x - initialFoot[u.id][0], u.y - initialFoot[u.id][1])));
      if (cavalry._tactic && cavalry._tactic.stage === 'wait-contact') {
        if (!waitingStart) waitingStart = positions(e);
        waitingTicks++;
        waitingMotion = Math.max(waitingMotion, ...members(e, cavalry).map(u => Math.hypot(u.x - waitingStart[u.id][0], u.y - waitingStart[u.id][1])));
      } else if (waitingStart && releaseAt === null && cavalry._tactic && cavalry._tactic.stage === 'rear-charge') releaseAt = e.time;
    }
    const rear = new Set(audit.hits.filter(hit => hit.regiment === cavalry.id && hit.rearProjection < -8).map(hit => hit.attacker));
    assert(footMotionAtTwenty < .01, 'The anvil marched forward before receiving the attack');
    assert(waitingTicks >= 60 && waitingMotion < 8, 'The cavalry did not physically wait at its rear rendezvous');
    assert(releaseAt !== null && rear.size >= 12, 'The waiting cavalry never delivered its actual rear attack');
    return { heldFootSoldiers: foot.alive, footMotionAtTwenty: round(footMotionAtTwenty), cavalryWaitSeconds: round(waitingTicks / 30),
      maximumWaitingMotion: round(waitingMotion), rearChargeReleasedAt: round(releaseAt), rearAttackers: rear.size, ...audit.metrics() };
  });

  test('Screen and skirmish shoots then moves the missile body away while infantry intercepts', () => {
    const e = new Engine({ map: 'custom', seed: 90071, customMap: { biome: 'grassland', regions: [] }, armies: [
      { name: 'Screen', doctrine: 'skirmish', roster: { guardian: 64, javelin: 64 } },
      { name: 'Pursuers', doctrine: 'aggressive', roster: { swordsman: 96 } }
    ], formations: [
      { team: 0, type: 'guardian', count: 64, x: 2250, y: 2200, facing: 0 },
      { team: 0, type: 'javelin', count: 64, x: 2600, y: 2200, facing: 0 },
      { team: 1, type: 'swordsman', count: 96, x: 2910, y: 2200, facing: Math.PI }
    ] });
    const screen = e.regiments.find(r => r.team === 0 && r.type === 'guardian'), missiles = e.regiments.find(r => r.type === 'javelin');
    const enemy = e.regiments.find(r => r.team === 1), initialScreen = center(e, screen), phases = new Set(), shooters = new Set();
    const audit = observer(e); let displaceStart = null, maximumWithdrawal = 0;
    e.start(); e.command([enemy.id], 'charge', missiles.x, missiles.y, { targetRegimentId: missiles.id });
    for (let tick = 0; tick < 30 * 30 && e.phase === 'running'; tick++) {
      audit.step();
      const s = missiles._tactic;
      if (s) {
        phases.add(s.stage);
        if (s.stage === 'displace' && !displaceStart) displaceStart = center(e, missiles);
      }
      if (displaceStart) maximumWithdrawal = Math.max(maximumWithdrawal, displaceStart.x - center(e, missiles).x);
      for (const u of members(e, missiles)) if (u._lastAttack >= 0) shooters.add(u.id);
    }
    const screenHits = new Set(audit.hits.filter(hit => hit.regiment === screen.id).map(hit => hit.attacker));
    assert(phases.has('fire') && phases.has('displace'), 'The missile formation did not actually alternate firing and relocation');
    assert(shooters.size >= 32 && maximumWithdrawal > 45, 'Skirmish labels did not produce a physical shooting withdrawal');
    assert(center(e, screen).x - initialScreen.x > 80 && screenHits.size >= 8, 'The covering infantry did not physically intercept the pursuit');
    return { phases: [...phases], distinctShooters: shooters.size, maximumWithdrawal: round(maximumWithdrawal),
      screenAdvance: round(center(e, screen).x - initialScreen.x), screenMeleeAttackers: screenHits.size, ...audit.metrics() };
  });
}

if (require.main === module) {
  const root = process.argv[2] || path.resolve(__dirname, '..'); global.window = global;
  require(path.join(root, 'src/data.js'));
  if (fs.existsSync(path.join(root, 'src/tactics.js'))) require(path.join(root, 'src/tactics.js'));
  for (const name of ['impacts', 'siege']) {
    const file = path.join(root, 'src', name + '.js');
    if (require('node:fs').existsSync(file)) require(file);
  }
  require(path.join(root, 'src/engine.js'));
  const results = [], filter = (process.env.IRONFIELD_TEST_FILTER || '').toLowerCase();
  register((name, body) => {
    if (filter && !name.toLowerCase().includes(filter)) return;
    const start = performance.now();
    try { const detail = body(); results.push({ name, passed: true, milliseconds: roundMS(performance.now() - start), ...detail }); console.log('PASS ' + name + ' ' + JSON.stringify(detail)); }
    catch (error) { results.push({ name, passed: false, error: error.message }); console.error('FAIL ' + name + ': ' + error.stack); process.exitCode = 1; }
  });
  console.log(JSON.stringify({ passed: results.filter(r => r.passed).length, failed: results.filter(r => !r.passed).length, results }, null, 2));
}
function roundMS(n) { return Math.round(n * 10) / 10; }
module.exports = register;
