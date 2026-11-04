'use strict';
// New 2.2 plans are checked through actual positions, damage and command
// response. Run alone, or register with the main engine suite.
const assert = require('node:assert/strict');
const path = require('node:path');
const { performance } = require('node:perf_hooks');

function register(test) {
  const D = global.GameData, Engine = global.BattleEngine;
  const ids = ['steady', 'rapid', 'refused', 'breakthrough', 'elastic', 'echelon'];
  const round = n => Math.round(n * 1000) / 1000;
  const mean = values => values.reduce((a, b) => a + b, 0) / Math.max(1, values.length);
  const members = (e, r) => r.unitIds.map(id => e.units[id]).filter(u => u.alive && !u.routed);
  const center = (e, r) => { const us = members(e, r); return { x: mean(us.map(u => u.x)), y: mean(us.map(u => u.y)) }; };
  const extent = (e, r) => {
    const us = members(e, r), xs = us.map(u => u.x), ys = us.map(u => u.y);
    return { depth: Math.max(...xs) - Math.min(...xs), width: Math.max(...ys) - Math.min(...ys) };
  };
  const run = (e, seconds, visit) => { for (let i = 0; i < seconds * 30 && e.phase === 'running'; i++) { e.step(1 / 30); if (visit) visit(e); } };
  const build = (doctrine, formations, customMap = { regions: [] }) => {
    const armies = [{ doctrine, roster: {} }, { doctrine: 'balanced', roster: {} }];
    for (const f of formations) armies[f.team].roster[f.type] = (armies[f.team].roster[f.type] || 0) + f.count;
    return new Engine({ map: 'custom', seed: 77291, customMap, armies, formations });
  };
  const holdEnemies = e => e.command(e.regiments.filter(r => r.team === 1).map(r => r.id), 'hold');
  const mixed = plan => build(plan, [
    { team: 0, type: 'swordsman', count: 64, x: 2300, y: 1800, facing: 0 },
    { team: 0, type: 'hoplite', count: 64, x: 2300, y: 2200, facing: 0 },
    { team: 0, type: 'pikeman', count: 64, x: 2300, y: 2600, facing: 0 },
    { team: 0, type: 'cavalry', count: 40, x: 2100, y: 1400, facing: 0 },
    { team: 0, type: 'lancer', count: 40, x: 2100, y: 3000, facing: 0 },
    { team: 1, type: 'guardian', count: 64, x: 3400, y: 1800, facing: Math.PI },
    { team: 1, type: 'swordsman', count: 64, x: 3400, y: 2200, facing: Math.PI },
    { team: 1, type: 'militia', count: 64, x: 3400, y: 2600, facing: Math.PI }
  ]);

  function audit(e) {
    const oldX = new Float64Array(e.units.length), oldY = new Float64Array(e.units.length);
    for (const u of e.units) { oldX[u.id] = u.x; oldY[u.id] = u.y; }
    const size = 96, cols = Math.ceil(e.width / size) + 1, rows = Math.ceil(e.height / size) + 1;
    const heads = new Int32Array(cols * rows), next = new Int32Array(e.units.length);
    const hits = [], attack = e._attack;
    e._attack = function (a, b) {
      const before = b.hp, dx = a.x - b.x, dy = a.y - b.y;
      const rear = dx * Math.cos(this.regiments[b.regiment].facing) + dy * Math.sin(this.regiments[b.regiment].facing);
      const result = attack.call(this, a, b);
      if (!D.TYPES[a.type].ranged && before - b.hp > 0) {
        const reach = D.TYPES[a.type].range * (this.terrainAt(a.x, a.y).elevation ? 1.12 : 1) + b._radius;
        assert(Math.hypot(dx, dy) <= reach + .01, 'A plan dealt damage beyond physical weapon reach');
        hits.push({ attacker: a.id, regiment: a.regiment, target: b.regiment, rear, time: e.time });
      }
      return result;
    };
    let minimumSeparation = Infinity, maximumStep = 0;
    return {
      hits,
      step() {
        e.step(1 / 30); heads.fill(-1);
        for (const u of e.units) if (u.alive && !u.routed) {
          assert([u.x, u.y, u.hp, u.angle].every(Number.isFinite), 'Plan produced nonfinite state');
          assert(u.x >= 0 && u.x <= e.width && u.y >= 0 && u.y <= e.height, 'Plan crossed a world boundary');
          assert(!e._bodyBlocked(u.x, u.y, u._radius), 'Plan entered terrain with its physical body');
          const moved = Math.hypot(u.x - oldX[u.id], u.y - oldY[u.id]);
          maximumStep = Math.max(maximumStep, moved);
          assert(moved <= D.TYPES[u.type].speed / 30 * 1.8 + .25, 'Plan teleported a soldier');
          const gx = Math.floor(oldX[u.id] / size), gy = Math.floor(oldY[u.id] / size);
          for (let y = Math.max(0, gy - 1); y <= Math.min(rows - 1, gy + 1); y++) for (let x = Math.max(0, gx - 1); x <= Math.min(cols - 1, gx + 1); x++) {
            for (let id = heads[y * cols + x]; id !== -1; id = next[id]) {
              const v = e.units[id], radius = u._radius + v._radius;
              const dx = oldX[u.id] - oldX[id], dy = oldY[u.id] - oldY[id];
              if (Math.abs(dx) > radius + 16 || Math.abs(dy) > radius + 16) continue;
              const vx = u.x - oldX[u.id] - v.x + oldX[id], vy = u.y - oldY[u.id] - v.y + oldY[id], vv = vx * vx + vy * vy;
              const at = vv ? Math.max(0, Math.min(1, -(dx * vx + dy * vy) / vv)) : 0;
              const separation = Math.hypot(dx + vx * at, dy + vy * at) / radius;
              minimumSeparation = Math.min(minimumSeparation, separation);
              assert(separation >= .90, `Plan bodies crossed: ${u.id}/${id} at ${e.time.toFixed(2)} (${separation.toFixed(3)})`);
            }
          }
          const key = gy * cols + gx; next[u.id] = heads[key]; heads[key] = u.id;
        }
        for (const u of e.units) { oldX[u.id] = u.x; oldY[u.id] = u.y; }
      },
      run(seconds, visit) { for (let i = 0; i < seconds * 30 && e.phase === 'running'; i++) { this.step(); if (visit) visit(e); } },
      metrics() { return { minimumSeparation: round(minimumSeparation), maximumStep: round(maximumStep) }; }
    };
  }

  test('Steady advance walks intact ranks under a partial frontal brace and yields to fresh orders', () => {
    const formations = [
      { team: 0, type: 'pikeman', count: 48, x: 2000, y: 1800, facing: 0 },
      { team: 0, type: 'hoplite', count: 48, x: 2000, y: 2300, facing: 0 },
      { team: 0, type: 'militia', count: 48, x: 2000, y: 2800, facing: 0 },
      { team: 1, type: 'guardian', count: 96, x: 4800, y: 2300, facing: Math.PI }
    ];
    const e = build('steady', formations), ordinary = build('balanced', formations);
    holdEnemies(e); holdEnemies(ordinary); e.start(); ordinary.start();
    const starts = e.regiments.slice(0, 3).map(r => ({ ...center(e, r), ...extent(e, r) }));
    const observer = audit(e); observer.run(12); run(ordinary, 12);
    const results = [];
    for (let i = 0; i < 3; i++) {
      const r = e.regiments[i], us = members(e, r), position = center(e, r), body = extent(e, r);
      const advance = position.x - starts[i].x;
      assert(advance > 200, 'Steady advance held its troops still');
      assert(advance < center(ordinary, ordinary.regiments[i]).x - starts[i].x - 40, 'Steady advance did not have a deliberate pace');
      assert(body.depth < starts[i].depth * 1.35 + 12 && body.width >= starts[i].width * .85, 'Walking brace stretched ranks into a tail');
      assert(r.cohesion > 90, 'Steady ranks lost their cohesion on open ground');
      const frontal = mean(us.map(u => D.TYPES[u.type].brace ? e._braceStrength(u, { x: u.x + 100, y: u.y }) : e._steadyGuardStrength(u, { x: u.x + 100, y: u.y })));
      const flank = mean(us.map(u => D.TYPES[u.type].brace ? e._braceStrength(u, { x: u.x, y: u.y + 100 }) : e._steadyGuardStrength(u, { x: u.x, y: u.y + 100 })));
      assert(frontal > (i === 2 ? .15 : .35) && frontal < .7, 'Moving brace was absent or as strong as stationary pikes');
      assert(flank < .01, 'Walking brace protected the flank');
      results.push({ type: r.type, advance: round(advance), walkingBrace: round(frontal), cohesion: round(r.cohesion) });
    }
    const pikes = e.regiments[0], u = e.units[pikes.unitIds[0]], oldReadiness = e._braceReadiness(u);
    e.command([pikes.id], 'hold'); observer.run(1.5);
    assert(e._braceReadiness(u) > oldReadiness + .3 && e._braceReadiness(u) > .9, 'Stopping did not strengthen the planted brace');
    e.command([pikes.id], 'move', u.x, u.y + 450); observer.run(1);
    assert.equal(e._braceReadiness(u), 0, 'A fresh move retained the automatic walking brace');
    return { formations: results, stationaryBrace: 1, ...observer.metrics() };
  });

  test('Rapid encirclement chooses bounded enemy positions and reassesses scope after an objective falls', () => {
    const e = build('rapid', [
      { team: 0, type: 'swordsman', count: 64, x: 3300, y: 2200, facing: 0 },
      { team: 0, type: 'cavalry', count: 40, x: 3100, y: 1700, facing: 0 },
      { team: 0, type: 'lancer', count: 40, x: 3100, y: 2700, facing: 0 },
      { team: 0, type: 'swordsman', count: 64, x: 8000, y: 7000, facing: 0 },
      { team: 0, type: 'cavalry', count: 24, x: 7800, y: 7400, facing: 0 },
      { team: 1, type: 'guardian', count: 64, x: 4300, y: 2000, facing: Math.PI },
      { team: 1, type: 'pikeman', count: 64, x: 4300, y: 2400, facing: Math.PI },
      { team: 1, type: 'militia', count: 32, x: 9000, y: 7000, facing: Math.PI },
      { team: 1, type: 'swordsman', count: 64, x: 6200, y: 2000, facing: Math.PI },
      { team: 1, type: 'hoplite', count: 64, x: 6200, y: 2400, facing: Math.PI }
    ], { width: 16000, height: 11000, regions: [] });
    holdEnemies(e); e.start(); run(e, 1);
    for (const r of e.regiments.slice(0, 3)) {
      assert.equal(r._tactic.targetCount, 2, 'Local wings ignored the neighboring enemy squad');
      assert.equal(r._tactic.scopeLabel, 'Enemy position');
      assert.deepEqual([...r._tactic.targetIds].sort((a, b) => a - b), [5, 6]);
    }
    for (const r of e.regiments.slice(3, 5)) {
      assert.equal(r._tactic.targetCount, 1, 'An isolated local opponent became an all-map objective');
      assert.equal(r._tactic.targetIds[0], 7);
    }
    // The ordinary kill path marks the first objective exhausted. The next
    // strategy tick must group the next position, not select one of its files.
    for (const r of [e.regiments[5], e.regiments[6]]) for (const id of r.unitIds) e._kill(e.units[id], e.units[0]);
    e._strategyTimer = 0; run(e, .7);
    const wing = e.regiments[1];
    assert.equal(wing._tactic.targetCount, 2, 'Retargeting forgot multi-formation scope');
    assert.deepEqual([...wing._tactic.targetIds].sort((a, b) => a - b), [8, 9]);
    return { dimensions: [e.width, e.height], initialScope: [2, 2, 2, 1, 1], retargetedIds: wing._tactic.targetIds };
  });

  test('Rapid encirclement takes separate physical wings around a connected line and lands rear attacks', () => {
    const e = mixed('rapid'); holdEnemies(e); e.start();
    const observer = audit(e), wings = e.regiments.filter(r => r.team === 0 && ['cavalry', 'lancer'].includes(r.type));
    const stages = new Map(wings.map(r => [r.id, new Set()])), greatestSpan = new Map(wings.map(r => [r.id, 0]));
    const transitSpan = new Map(wings.map(r => [r.id, 0]));
    const initial = new Map(wings.map(r => [r.id, extent(e, r)]));
    observer.run(80, () => {
      for (const r of wings) {
        if (r._tactic) stages.get(r.id).add(r._tactic.stage);
        if (e._tick % 15 === 0) {
          const b = extent(e, r), span = Math.hypot(b.width, b.depth);
          greatestSpan.set(r.id, Math.max(greatestSpan.get(r.id), span));
          if (r._tactic && r._tactic.transit) transitSpan.set(r.id, Math.max(transitSpan.get(r.id), span));
        }
      }
    });
    const results = [];
    for (const r of wings) {
      const phases = stages.get(r.id), rearAttackers = new Set(observer.hits.filter(h => h.regiment === r.id && h.rear < -5).map(h => h.attacker));
      assert(phases.has('rapid-round') && phases.has('rapid-rear') && phases.has('rapid-charge'), 'A wing skipped the actual outside/rear route');
      assert(rearAttackers.size >= 6, 'The rear charge was only a status label');
      const footprint = initial.get(r.id);
      // Once combat begins, one wing may fight along the whole three-squad
      // rear. The marching-tail gate therefore uses every transit phase,
      // with the same tight 2.1× limit as the retained Encircle regression.
      assert(transitSpan.get(r.id) <= Math.hypot(footprint.width, footprint.depth) * 2.1, 'Rapid wings stretched into a long tail before combat');
      results.push({ type: r.type, phases: [...phases], rearAttackers: rearAttackers.size, maximumTransitSpan: round(transitSpan.get(r.id)), maximumCombatSpan: round(greatestSpan.get(r.id)) });
    }
    return { wings: results, ...observer.metrics() };
  });

  test('Rapid encirclement attacks an accessible flank when the world boundary closes the rear', () => {
    const e = build('rapid', [
      { team: 0, type: 'swordsman', count: 32, x: 5000, y: 2200, facing: 0 },
      { team: 0, type: 'cavalry', count: 16, x: 4800, y: 1800, facing: 0 },
      { team: 1, type: 'guardian', count: 64, x: 6260, y: 2200, facing: Math.PI }
    ]);
    holdEnemies(e); e.start();
    const wing = e.regiments[1], point = { x: wing._tactic.goalX, y: wing._tactic.goalY };
    assert(wing._tactic.routeConstrained, 'A defender against the world edge received a nonexistent rear route');
    assert(point.y > 1600 && point.y < 2700, 'An infeasible diagonal rear plane sent the wing toward a remote map corner');
    const observer = audit(e); observer.run(45);
    const attackers = new Set(observer.hits.filter(h => h.regiment === wing.id).map(h => h.attacker));
    assert(attackers.size >= 4, 'The bounded flank approach never reached a real fight');
    return { guardedFlankDestination: { x: round(point.x), y: round(point.y) }, distinctAttackers: attackers.size, ...observer.metrics() };
  });

  test('Refused flank, breakthrough and echelon issue distinct persistent movement jobs', () => {
    const outcomes = {};
    for (const plan of ['refused', 'breakthrough', 'echelon']) {
      const e = mixed(plan); holdEnemies(e); e.start();
      const own = e.regiments.filter(r => r.team === 0), starts = new Map(own.map(r => [r.id, center(e, r)]));
      const observer = audit(e), early = [];
      observer.run(2, () => {});
      for (const r of own) early.push({ id: r.id, dx: center(e, r).x - starts.get(r.id).x, wave: r._tactic.wave || 0 });
      observer.run(10);
      const movements = own.map(r => ({ id: r.id, role: r._tactic.role, stage: r._tactic.stage, dx: center(e, r).x - starts.get(r.id).x,
        dy: center(e, r).y - starts.get(r.id).y, wave: r._tactic.wave || 0 }));
      if (plan === 'refused') {
        assert(movements.filter(m => m.role === 'refused-wing').every(m => m.dx < -160 && Math.abs(m.dy) > 65), 'Refused wing did not physically fold back and inward');
        assert(movements.some(m => m.role === 'attack-wing' && m.dx > 250), 'The attack wing waited with the refused wing');
        assert(movements.some(m => m.stage === 'refuse-guard'), 'The folded wing never held the shortened line');
      } else if (plan === 'breakthrough') {
        assert(movements.filter(m => m.role === 'spearhead').every(m => m.dx > 250), 'Breakthrough did not move its first wave');
        assert(movements.some(m => m.role === 'spearhead' && Math.abs(m.dy) > 100), 'Breakthrough did not concentrate toward a sector');
        assert(movements.filter(m => m.role === 'reserve').every(m => Math.abs(m.dx) < 10), 'The supporting wave piled into the initial concentration');
        observer.run(25);
        assert(own.some(r => r._tactic && r._tactic.stage === 'breakthrough-follow' && center(e, r).x - starts.get(r.id).x > 250), 'The supporting wave never physically followed the assault');
        assert(observer.hits.some(h => e.regiments[h.regiment].team === 0), 'Breakthrough never joined actual combat');
      } else {
        assert(early.some(m => m.wave === 0 && m.dx > 40), 'First echelon failed to lead');
        assert(early.filter(m => m.wave > 0).every(m => Math.abs(m.dx) < 1), 'Later echelons did not wait');
        assert(movements.filter(m => m.wave > 0).some(m => m.dx > 90), 'Later echelons never joined the attack');
        const fronts = movements.filter(m => m.role === 'echelon').sort((a, b) => a.wave - b.wave);
        assert(fronts[0].dx > fronts.at(-1).dx + 70, 'Successive waves collapsed into simultaneous movement');
      }
      outcomes[plan] = { movements: movements.map(m => ({ ...m, dx: round(m.dx), dy: round(m.dy) })), ...observer.metrics() };
    }
    return outcomes;
  });

  test('Elastic defense visibly gives ground, reforms and counterattacks the pursuit', () => {
    const e = build('elastic', [
      { team: 0, type: 'swordsman', count: 48, x: 2500, y: 2200, facing: 0 },
      { team: 1, type: 'militia', count: 64, x: 2900, y: 2200, facing: Math.PI }
    ]);
    e.start();
    const r = e.regiments[0], start = center(e, r), observer = audit(e), phases = new Set();
    let farthestBack = 0, counterStart = null;
    observer.run(55, () => {
      if (r._tactic) {
        phases.add(r._tactic.stage);
        if (r._tactic.stage === 'elastic-counter' && counterStart === null) counterStart = center(e, r).x;
      }
      farthestBack = Math.max(farthestBack, start.x - center(e, r).x);
    });
    assert(phases.has('elastic-fall') && phases.has('elastic-second') && phases.has('elastic-counter'), 'Elastic defense did not complete its real phase sequence');
    assert(farthestBack > 240, 'Elastic defense changed its label without yielding ground');
    assert(counterStart !== null && center(e, r).x > counterStart + 35, 'Reformed soldiers never advanced back into the pursuit');
    assert(new Set(observer.hits.filter(h => h.regiment === r.id).map(h => h.attacker)).size >= 8, 'The counterattack never hit pursuers');
    return { phases: [...phases], maximumWithdrawal: round(farthestBack), counterAdvance: round(center(e, r).x - counterStart), ...observer.metrics() };
  });

  test('Every new army plan immediately yields to move, hold, facing, charge and retreat orders', () => {
    const results = [];
    for (const plan of ids) {
      assert(D.DOCTRINES[plan], 'Missing registered doctrine ' + plan);
      const e = build(plan, [
        { team: 0, type: 'pikeman', count: 24, x: 2200, y: 2200, facing: 0 },
        { team: 0, type: 'cavalry', count: 16, x: 2000, y: 1700, facing: 0 },
        { team: 1, type: 'guardian', count: 64, x: 4300, y: 2200, facing: Math.PI }
      ]);
      holdEnemies(e); e.start(); run(e, 2);
      const r = e.regiments[0], before = center(e, r);
      const startingY = members(e, r).map(u => u.y);
      assert(r._tactic, plan + ' was not active before the override');
      e.command([r.id], 'move', before.x, before.y + 450); e.step(1 / 30);
      const marchStep = Math.min(...members(e, r).map((u, i) => u.y - startingY[i]));
      assert(marchStep > .5, plan + ' held a free file on the first Move step');
      run(e, 59 / 30);
      assert(center(e, r).y > before.y + 25, plan + ' refused the fresh move');
      assert.equal(r._tactic, null, plan + ' kept an automatic route after Move');
      e.command([r.id], 'hold'); const held = members(e, r).map(u => [u.x, u.y]);
      e.setDoctrine(0, ids[(ids.indexOf(plan) + 1) % ids.length]); run(e, 1);
      assert.deepEqual(members(e, r).map(u => [u.x, u.y]), held, plan + ' moved a held formation after a doctrine change');
      e.command([r.id], 'face', r.x - 500, r.y); run(e, 1);
      assert.equal(r._tactic, null, plan + ' overrode Face');
      assert(r.order === 'face' || r.order === 'hold', plan + ' cancelled the explicit facing command');
      e.command([r.id], 'charge'); run(e, 1);
      assert.equal(r.order, 'charge', plan + ' cancelled Charge');
      assert.equal(r._tactic, null, plan + ' kept a competing army plan during Charge');
      const retreat = center(e, r), positions = members(e, r).map(u => [u.x, u.y]);
      const threat = center(e, e.regiments.find(group => group.team === 1));
      const dx = retreat.x - threat.x, dy = retreat.y - threat.y, distance = Math.hypot(dx, dy);
      e.command([r.id], 'retreat'); e.step(1 / 30);
      const retreatStep = Math.min(...members(e, r).map((u, i) =>
        ((u.x - positions[i][0]) * dx + (u.y - positions[i][1]) * dy) / distance));
      assert(retreatStep > .5, plan + ' held a free file on the first withdrawal step');
      run(e, 2);
      assert(r.fallingBack && center(e, r).x < retreat.x - 20, plan + ' prevented physical withdrawal');
      assert.equal(r._tactic, null, plan + ' competed with Retreat');
      results.push({ plan, firstMarchStep: round(marchStep), firstRetreatStep: round(retreatStep), held: true, faced: true, charged: true });
    }
    return { orders: results };
  });

  test('Splitting every new plan preserves physical halves and independent persistent jobs and orders', () => {
    const results = [];
    for (const plan of ids) {
      const e = mixed(plan); holdEnemies(e); e.start(); run(e, 2);
      const original = e.regiments[plan === 'rapid' ? 3 : plan === 'steady' ? 2 : 0];
      const positions = e.units.map(u => [u.x, u.y]);
      const phase = original._tactic.stage, childId = e.splitRegiments([original.id])[0], child = e.regiments[childId];
      assert(child, 'The plan could not create a detachment');
      assert.deepEqual(e.units.map(u => [u.x, u.y]), positions, 'Splitting teleported a physical half');
      assert.equal(original._tactic.stage, phase); assert.equal(child._tactic.stage, phase);
      assert.notEqual(original._tactic, child._tactic, 'The two halves shared a mutable phase');
      assert.notEqual(original._tactic.waypoints, child._tactic.waypoints, 'The two halves shared a mutable route');
      if (original._tactic.targetIds) assert.notEqual(original._tactic.targetIds, child._tactic.targetIds, 'The two halves shared their mutable objective list');
      const p = e._tactics.plans[0];
      assert(p.jobs.has(original.id) && p.jobs.has(child.id), 'The new half had no persistent strategy job');
      assert.notEqual(p.jobs.get(original.id), p.jobs.get(child.id), 'The two halves shared their job');
      const before = center(e, child);
      run(e, 2); // Exercise the inherited phase before issuing a new order.
      if (plan === 'rapid') {
        assert.notEqual(p.jobs.get(original.id).lane, p.jobs.get(child.id).lane, 'Future replans put both detached wings on one lane');
        assert(e._tactics._rapidRoute(child, p, e._tactics._context(0)), 'The detached Rapid wing could not replan its route');
        assert(Math.hypot(original._tactic.goalX - child._tactic.goalX, original._tactic.goalY - child._tactic.goalY) > child.width * .5, 'A detached replan reused the parent transit center');
      }
      const ordered = center(e, child), parentJob = original._tactic;
      const moving = members(e, child), vx = mean(moving.map(u => u._vx)), vy = mean(moving.map(u => u._vy)), speed = Math.hypot(vx, vy);
      const nx = speed > 3 ? vx / speed : Math.cos(child.facing), ny = speed > 3 ? vy / speed : Math.sin(child.facing);
      // Extend the already clear physical travel bearing. Full redirections
      // are covered by the separate six-plan command gate; here a crossing
      // through another wing would conflate traffic with ownership of a half.
      e.command([child.id], 'move', ordered.x + nx * 400, ordered.y + ny * 400); run(e, 2);
      assert.equal(child.order, 'move', 'The inherited plan cancelled the detachment order');
      assert.equal(child._tactic, null, 'A stale strategy competed with the detachment order');
      const final = center(e, child), advance = (final.x - ordered.x) * nx + (final.y - ordered.y) * ny;
      assert(advance > 15, plan + ' detached soldiers did not physically obey Move: ' + advance);
      assert.equal(original._tactic, parentJob, 'A detachment order reset its parent strategy');
      results.push({ plan, sizes: [original.alive, child.alive], inheritedPhase: phase,
        independentJob: true, orderedAdvance: round(advance), firstCenter: { x: round(before.x), y: round(before.y) } });
    }
    return { detachments: results };
  });

  test('Colossal custom terrain and deployments retain far-side coordinates and path safely through a corridor', () => {
    const e = build('steady', [
      { team: 0, type: 'swordsman', count: 64, x: 11200, y: 8000, facing: 0 },
      { team: 1, type: 'guardian', count: 64, x: 14500, y: 8000, facing: Math.PI }
    ], { width: 16000, height: 11000, regions: [
      { type: 'rock', x: 12000, y: 7000, w: 250, h: 880 },
      { type: 'rock', x: 12000, y: 8120, w: 250, h: 880 },
      { type: 'forest', x: 11900, y: 7880, w: 500, h: 240 }
    ] });
    assert.deepEqual([e.width, e.height], [16000, 11000]);
    assert.equal(e.config.formations[0].x, 11200); assert.equal(e.config.formations[0].y, 8000);
    assert.equal(e.map.regions[0].x, 12000); assert.equal(e.map.regions[0].h, 880);
    const r = e.regiments[0]; holdEnemies(e); e.start(); e.command([r.id], 'move', 13000, 8000);
    const observer = audit(e); observer.run(55);
    assert(members(e, r).every(u => u.x > 12280), 'Far-side navigation failed to cross the actual corridor');
    assert(center(e, r).x > 12750, 'Large-map march failed to reach its destination area');
    return { world: [e.width, e.height], finalX: round(center(e, r).x), navCells: e._navBlocked.length, ...observer.metrics() };
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
