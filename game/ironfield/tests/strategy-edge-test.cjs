'use strict';
// Independent 2.2 strategy edge cases. These inspect soldiers' real travel
// and damage, including the unchanged physical gates used by the plan suite.
const assert = require('node:assert/strict');
const path = require('node:path');
const { performance } = require('node:perf_hooks');

function register(test) {
  const D = global.GameData, Engine = global.BattleEngine, DT = 1 / 30;
  const round = n => Math.round(n * 1000) / 1000;
  const troops = (e, r) => r.unitIds.map(id => e.units[id]).filter(u => u.alive && !u.routed);
  const center = (e, r) => {
    const us = troops(e, r);
    return { x: us.reduce((sum, u) => sum + u.x, 0) / Math.max(1, us.length),
      y: us.reduce((sum, u) => sum + u.y, 0) / Math.max(1, us.length) };
  };
  const bounds = (e, r, nx = 0, ny = 1, bodies = false) => {
    let lo = Infinity, hi = -Infinity;
    for (const u of troops(e, r)) {
      const position = u.x * nx + u.y * ny, radius = bodies ? u._radius : 0;
      lo = Math.min(lo, position - radius); hi = Math.max(hi, position + radius);
    }
    return { lo, hi };
  };
  const span = (e, r) => {
    const x = bounds(e, r, 1, 0), y = bounds(e, r, 0, 1);
    return Math.hypot(x.hi - x.lo, y.hi - y.lo);
  };
  const formation = (team, type, count, x, y) => ({ team, type, count, x, y, facing: team ? Math.PI : 0 });
  const build = (doctrine, formations, regions = []) => {
    const armies = [{ doctrine, roster: {} }, { doctrine: 'balanced', roster: {} }];
    for (const f of formations) armies[f.team].roster[f.type] = (armies[f.team].roster[f.type] || 0) + f.count;
    const e = new Engine({ map: 'custom', seed: 77291, customMap: { regions }, armies, formations });
    e.command(e.regiments.filter(r => r.team === 1).map(r => r.id), 'hold');
    e.start();
    return e;
  };
  const attackers = (hits, regiment, targets, since = -1, rearOnly = false) => new Set(hits
    .filter(h => h.regiment === regiment && targets.includes(h.target) && h.time > since && (!rearOnly || h.rear < -5))
    .map(h => h.attacker));

  function observe(e) {
    const oldX = new Float64Array(e.units.length), oldY = new Float64Array(e.units.length);
    for (const u of e.units) { oldX[u.id] = u.x; oldY[u.id] = u.y; }
    const size = 96, cols = Math.ceil(e.width / size) + 1, rows = Math.ceil(e.height / size) + 1;
    const heads = new Int32Array(cols * rows), next = new Int32Array(e.units.length), hits = [];
    let minimumSeparation = Infinity, maximumStep = 0;
    const attack = e._attack, hit = e._hit;
    e._attack = function (a, b) {
      const hp = b.hp, distance = Math.hypot(a.x - b.x, a.y - b.y);
      const result = attack.call(this, a, b);
      if (!D.TYPES[a.type].ranged && b.hp < hp) {
        const reach = D.TYPES[a.type].range * (this.terrainAt(a.x, a.y).elevation ? 1.12 : 1) + b._radius;
        assert(distance <= reach + .01, 'Strategy damage exceeded real melee reach');
      }
      return result;
    };
    e._hit = function (a, b, ...args) {
      const hp = b.hp, facing = this.regiments[b.regiment].facing;
      const rear = (a.x - b.x) * Math.cos(facing) + (a.y - b.y) * Math.sin(facing);
      const result = hit.call(this, a, b, ...args);
      if (b.hp < hp) hits.push({ attacker: a.id, regiment: a.regiment, target: b.regiment, time: this.time, rear });
      return result;
    };
    return {
      hits,
      step() {
        e.step(DT); heads.fill(-1);
        for (const u of e.units) if (u.alive && !u.routed) {
          assert([u.x, u.y, u.hp, u.angle].every(Number.isFinite), 'Strategy produced nonfinite physical state');
          assert(u.x >= 0 && u.x <= e.width && u.y >= 0 && u.y <= e.height, 'Strategy left the battlefield');
          assert(!e._bodyBlocked(u.x, u.y, u._radius), 'Strategy entered impassable ground with its body');
          const moved = Math.hypot(u.x - oldX[u.id], u.y - oldY[u.id]);
          maximumStep = Math.max(maximumStep, moved);
          assert(moved <= D.TYPES[u.type].speed * DT * 1.8 + .25, 'Strategy teleported a soldier');
          const gx = Math.floor(oldX[u.id] / size), gy = Math.floor(oldY[u.id] / size);
          for (let y = Math.max(0, gy - 1); y <= Math.min(rows - 1, gy + 1); y++) {
            for (let x = Math.max(0, gx - 1); x <= Math.min(cols - 1, gx + 1); x++) {
              for (let id = heads[y * cols + x]; id !== -1; id = next[id]) {
                const v = e.units[id], radius = u._radius + v._radius;
                const dx = oldX[u.id] - oldX[id], dy = oldY[u.id] - oldY[id];
                if (Math.abs(dx) > radius + 16 || Math.abs(dy) > radius + 16) continue;
                const vx = u.x - oldX[u.id] - v.x + oldX[id], vy = u.y - oldY[u.id] - v.y + oldY[id];
                const vv = vx * vx + vy * vy, at = vv ? Math.max(0, Math.min(1, -(dx * vx + dy * vy) / vv)) : 0;
                const separation = Math.hypot(dx + vx * at, dy + vy * at) / radius;
                minimumSeparation = Math.min(minimumSeparation, separation);
                assert(separation >= .90, `Strategy bodies crossed: ${u.id}/${id} at ${e.time.toFixed(2)} (${separation.toFixed(3)})`);
              }
            }
          }
          const key = gy * cols + gx; next[u.id] = heads[key]; heads[key] = u.id;
        }
        for (const u of e.units) { oldX[u.id] = u.x; oldY[u.id] = u.y; }
      },
      run(seconds, visit) {
        for (let i = 0; i < Math.ceil(seconds / DT) && e.phase === 'running'; i++) {
          this.step(); if (visit) visit(e);
        }
      },
      metrics() { return { minimumSeparation: round(minimumSeparation), maximumStep: round(maximumStep) }; }
    };
  }

  test('Steady outer files close onto a narrow enemy and actually fight instead of advancing forever', () => {
    const e = build('steady', [
      formation(0, 'swordsman', 32, 1600, 1800),
      formation(0, 'swordsman', 32, 1600, 2600),
      formation(1, 'guardian', 64, 2900, 2200)
    ]);
    const own = e.regiments.slice(0, 2), enemy = e.regiments[2], watch = observe(e);
    const starts = own.map(r => center(e, r)), initialSpans = own.map(r => span(e, r));
    const greatestTransitSpan = [0, 0], contacted = [false, false];
    watch.run(60, () => {
      for (let i = 0; i < own.length; i++) {
        const r = own[i], s = r._tactic;
        if (s && s.transit) greatestTransitSpan[i] = Math.max(greatestTransitSpan[i], span(e, r));
        if (s && s.stage === 'steady-contact') contacted[i] = true;
        if (!contacted[i]) assert(center(e, r).x < enemy.x + enemy.depth, 'An outer file marched beyond the enemy while still searching for its original lane');
      }
    });
    const outcomes = own.map((r, i) => {
      const final = center(e, r), count = attackers(watch.hits, r.id, [enemy.id]).size;
      const inward = (i === 0 ? 1 : -1) * (final.y - starts[i].y);
      assert(count >= 8, 'An outer Steady lane never joined real combat');
      assert(contacted[i] && inward > 200 && final.x - starts[i].x > 900, 'Steady did not physically correct its missed frontage');
      assert(greatestTransitSpan[i] <= initialSpans[i] * 2.1, 'Steady lateral correction strung out the marching ranks');
      return { regiment: r.id, actualAttackers: count, inwardTravel: round(inward), advance: round(final.x - starts[i].x), maximumTransitRatio: round(greatestTransitSpan[i] / initialSpans[i]) };
    });
    return { outerFiles: outcomes, ...watch.metrics() };
  });

  test('Rapid mixed-width wings keep separate physical lanes and coherent ranks through the rear approach', () => {
    const e = build('rapid', [
      formation(0, 'swordsman', 32, 2100, 2200),
      formation(0, 'cavalry', 8, 1900, 1200),
      formation(0, 'cavalry', 64, 2100, 1800),
      formation(0, 'cavalry', 64, 2100, 2600),
      formation(0, 'cavalry', 8, 1900, 3200),
      formation(1, 'guardian', 32, 3400, 2050),
      formation(1, 'guardian', 32, 3400, 2350)
    ]);
    const watch = observe(e), wings = e.regiments.slice(1, 5), pairs = [[2, 1, -1], [3, 4, 1]];
    const initial = new Map(wings.map(r => [r.id, span(e, r)])), greatestTransitSpan = new Map(wings.map(r => [r.id, 0]));
    const phases = new Map(wings.map(r => [r.id, new Set()])), rearPosition = new Map(wings.map(r => [r.id, -Infinity]));
    const minimumLaneGap = [Infinity, Infinity], roundSamples = [0, 0];
    watch.run(45, () => {
      const p = e._tactics.plans[0];
      for (const r of wings) {
        const s = r._tactic; if (!s) continue;
        phases.get(r.id).add(s.stage);
        if (s.transit) greatestTransitSpan.set(r.id, Math.max(greatestTransitSpan.get(r.id), span(e, r)));
        if (s.stage === 'rapid-rear' || s.charge) {
          const c = center(e, r); rearPosition.set(r.id, Math.max(rearPosition.get(r.id), c.x * p.nx + c.y * p.ny));
        }
      }
      for (let i = 0; i < pairs.length; i++) {
        const [innerId, outerId, side] = pairs[i], inner = e.regiments[innerId], outer = e.regiments[outerId];
        const a = inner._tactic, b = outer._tactic;
        if (!a || !b || a.stage !== 'rapid-round' || b.stage !== 'rapid-round') continue;
        const innerBody = bounds(e, inner, p.tx, p.ty, true), outerBody = bounds(e, outer, p.tx, p.ty, true);
        const clearance = side < 0 ? innerBody.lo - outerBody.hi : outerBody.lo - innerBody.hi;
        minimumLaneGap[i] = Math.min(minimumLaneGap[i], clearance); roundSamples[i]++;
        assert(clearance > 20, 'A narrow outer wing merged into, or swapped through, its broad inner wing');
      }
    });
    for (let i = 0; i < pairs.length; i++) assert(roundSamples[i] >= 60, 'The wing pair never physically marched in its separate outside lanes');
    const p = e._tactics.plans[0], enemyRear = Math.max(...e.regiments.slice(5).map(r => bounds(e, r, p.nx, p.ny).hi));
    const outcomes = wings.map(r => {
      const seen = phases.get(r.id), count = attackers(watch.hits, r.id, [5, 6], -1, true).size;
      assert(seen.has('rapid-round') && seen.has('rapid-rear'), 'A wing skipped the physical route around the enemy');
      assert(rearPosition.get(r.id) > enemyRear + 10, 'A wing called itself a rear attack without reaching behind the enemy');
      assert(count >= (r.initial <= 8 ? 2 : 6), 'A separated wing did not deliver actual rear attacks');
      assert(greatestTransitSpan.get(r.id) <= initial.get(r.id) * 2.1, `Rapid wing ${r.id} stretched to ${(greatestTransitSpan.get(r.id) / initial.get(r.id)).toFixed(3)} times its initial footprint while still in transit`);
      return { regiment: r.id, strength: r.initial, rearAttackers: count, maximumTransitRatio: round(greatestTransitSpan.get(r.id) / initial.get(r.id)) };
    });
    return { wings: outcomes, minimumPhysicalLaneGaps: minimumLaneGap.map(round), ...watch.metrics() };
  });

  test('Rapid pin and missile support fight their assigned position then physically retarget its replacement', () => {
    const e = build('rapid', [
      formation(0, 'cavalry', 64, 2300, 1700),
      formation(0, 'swordsman', 32, 2100, 2050),
      formation(0, 'archer', 24, 1900, 2050),
      formation(1, 'guardian', 16, 2800, 1700),
      formation(1, 'guardian', 32, 3000, 2600),
      formation(1, 'guardian', 32, 3000, 2880),
      formation(1, 'guardian', 32, 4300, 2400),
      formation(1, 'guardian', 32, 4300, 2700)
    ]);
    const watch = observe(e), assigned = e.regiments.slice(1, 3);
    watch.run(1);
    assert.equal(assigned[0]._tactic.role, 'pin'); assert.equal(assigned[1]._tactic.role, 'support');
    for (const r of assigned) {
      assert.deepEqual([...r._tactic.targetIds].sort((a, b) => a - b), [4, 5], 'The pin/support fixture did not receive its separate connected objective');
      assert([4, 5].includes(r._enemyId), 'Engine commitment ignored the pin/support objective');
      const c = center(e, r), near = center(e, e.regiments[3]), selected = center(e, e.regiments[4]);
      assert(Math.hypot(c.x - near.x, c.y - near.y) < Math.hypot(c.x - selected.x, c.y - selected.y), 'The fixture no longer distinguishes assigned scope from the nearest enemy');
    }
    watch.run(29);
    const firstAttackers = assigned.map(r => attackers(watch.hits, r.id, [4, 5]).size);
    assert(firstAttackers.every(count => count >= 8), 'Pin/support labels did not result in real damage to their assigned position');
    // The ordinary casualty path exhausts the two nearby objectives. Both
    // roles must now recognize the remaining connected pair and actually go.
    const retargetAt = e.time, previous = assigned.map(r => center(e, r));
    for (const r of e.regiments.slice(3, 6)) for (const id of r.unitIds) {
      if (e.units[id].alive) e._kill(e.units[id], e.units[assigned[0].unitIds[0]]);
    }
    e._strategyTimer = 0; watch.run(1);
    for (const r of assigned) {
      assert.deepEqual([...r._tactic.targetIds].sort((a, b) => a - b), [6, 7], 'Pin/support retained stale scope after its position fell');
      assert.equal(r._tactic.targetCount, 2); assert.equal(r._tactic.scopeLabel, 'Enemy position');
      assert([6, 7].includes(r._tactic.targetId) && [6, 7].includes(r._enemyId), 'Pin/support did not commit the engine to its replacement objective');
    }
    watch.run(39);
    const outcomes = assigned.map((r, i) => {
      const final = center(e, r), count = attackers(watch.hits, r.id, [6, 7], retargetAt).size;
      assert(final.x - previous[i].x > 800, 'Retargeting changed scope without physically advancing the support body');
      assert(count >= 4, 'Retargeted pin/support never dealt real damage to the replacement position');
      return { role: r._tactic.role, initialAttackers: firstAttackers[i], replacementAttackers: count, retargetAdvance: round(final.x - previous[i].x) };
    });
    return { assignments: outcomes, ...watch.metrics() };
  });

  test('Rapid abandons a blocked outside approach within a bounded time and physically completes the alternate wing', () => {
    // A long ledge separates the initial left-wing waypoint from its riders.
    // The reachable detour initially leads away from that waypoint. Its
    // progress timeout must choose the open right side and finish the attack.
    const e = build('rapid', [
      formation(0, 'swordsman', 16, 1800, 2500),
      formation(0, 'cavalry', 8, 1700, 2450),
      formation(1, 'guardian', 64, 3300, 2500)
    ], [{ type: 'rock', x: 1000, y: 2300, w: 2800, h: 100 }]);
    const r = e.regiments[1], watch = observe(e), start = center(e, r), initialSpan = span(e, r);
    let firstSide = null, replanTime = null, replanCenter = null, maximumRetries = 0, greatestTransitSpan = 0;
    let openWingReached = false, greatestRearX = -Infinity;
    watch.run(55, () => {
      const s = r._tactic; if (!s) return;
      const c = center(e, r);
      if (firstSide === null) {
        firstSide = s.side;
        assert.equal(firstSide, -1, 'Blocked-route fixture did not start on the ledge side');
        assert(s.goalY < 2300 && c.y > 2400, 'The first physical route no longer crosses the ledge');
      }
      maximumRetries = Math.max(maximumRetries, s.routeRetries || 0);
      if (replanTime === null && s.routeRetries > 0) { replanTime = e.time; replanCenter = c; }
      if (replanTime !== null && s.side === 1 && c.y > 2600) openWingReached = true;
      if (s.stage === 'rapid-rear' || s.charge) greatestRearX = Math.max(greatestRearX, c.x);
      if (s.transit) greatestTransitSpan = Math.max(greatestTransitSpan, span(e, r));
    });
    assert(replanTime !== null && replanTime >= 4 && replanTime <= 12, 'Blocked Rapid route waited indefinitely instead of replanning on measured progress');
    assert(maximumRetries >= 1 && maximumRetries <= 2, 'Blocked route oscillated through unbounded replanning');
    assert(replanCenter.x < start.x - 300, 'The fixture never physically attempted the obstructed route');
    assert(openWingReached && greatestRearX > 3380, 'Replanning changed a label without travelling around the open flank');
    const count = attackers(watch.hits, r.id, [2]).size;
    assert(count >= 4, 'The alternate route never produced a real attack');
    assert(greatestTransitSpan <= initialSpan * 2.1, 'The alternate route left a long marching tail');
    return { firstReplanSeconds: round(replanTime), retries: maximumRetries, openWingReached, actualAttackers: count,
      maximumTransitRatio: round(greatestTransitSpan / initialSpan), ...watch.metrics() };
  });
}

if (require.main === module) {
  const root = process.argv[2] || path.resolve(__dirname, '..'); global.window = global;
  for (const name of ['data', 'tactics', 'impacts', 'siege', 'engine']) require(path.join(root, 'src', name + '.js'));
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
