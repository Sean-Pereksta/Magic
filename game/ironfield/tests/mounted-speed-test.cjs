'use strict';
// Measure sustained travel of actual bodies, including acceleration, turns,
// woodland, a solid corridor, and fresh commands. No synthetic velocities.
// Run: node tests/mounted-speed-test.cjs [alternate-project-root]
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { performance } = require('node:perf_hooks');
const DT = 1 / 30, HORSES = ['cavalry', 'lancer', 'horseArcher'];
const round = value => Math.round(value * 1000) / 1000;
const mean = values => values.reduce((sum, value) => sum + value, 0) / Math.max(1, values.length);
const members = (e, r) => r.unitIds.map(id => e.units[id]).filter(u => u.alive && !u.routed);
function body(e, r) {
  const us = members(e, r), xs = us.map(u => u.x), ys = us.map(u => u.y);
  return { x: mean(xs), y: mean(ys), minX: Math.min(...xs), maxX: Math.max(...xs),
    span: Math.hypot(Math.max(...xs) - Math.min(...xs), Math.max(...ys) - Math.min(...ys)),
    speed: mean(us.map(u => Math.hypot(u._vx, u._vy))),
    momentum: mean(us.map(u => e.impacts.power(u))) };
}
function fixture(type, regions = [], count = 48) {
  return new global.BattleEngine({ map: 'custom', seed: 7137, customMap: { biome: 'grassland', regions },
    armies: [{ name: 'Mounted movement', doctrine: 'balanced', roster: { [type]: count } },
      { name: 'Distant observer', doctrine: 'defensive', roster: { guardian: 1 } }],
    formations: [{ team: 0, type, count, x: 1300, y: 2200, facing: 0, formation: 'line' },
      { team: 1, type: 'guardian', count: 1, x: 5800, y: 3900, facing: Math.PI, formation: 'line' }] });
}
function observer(e) {
  const D = global.GameData, previous = e.units.map(u => ({ x: u.x, y: u.y }));
  const solids = e.map.regions.filter(r => r.type === 'rock');
  const metrics = { minimumBodyRatio: Infinity, maximumStep: 0 };
  const rectGap = (x, y, r) => Math.hypot(Math.max(r.x - x, 0, x - r.x - r.w), Math.max(r.y - y, 0, y - r.y - r.h));
  function step() {
    e.step(DT);
    const active = e.units.filter(u => u.alive && !u.routed);
    for (let i = 0; i < active.length; i++) {
      const u = active[i], p = previous[u.id], t = D.TYPES[u.type];
      assert([u.x, u.y, u.hp, u.angle].every(Number.isFinite), 'Faster horses produced invalid physical state');
      assert(u.x >= t.radius && u.y >= t.radius && u.x <= e.width - t.radius && u.y <= e.height - t.radius, 'A faster horse left the world');
      assert.equal(e.terrainAt(u.x, u.y).blocked, false, 'A faster horse entered blocked terrain');
      const moved = Math.hypot(u.x - p.x, u.y - p.y);
      metrics.maximumStep = Math.max(metrics.maximumStep, moved);
      assert(moved <= t.speed * DT * 1.8 + .25, 'A faster horse teleported during a command or formation change');
      for (const rock of solids) for (let part = 0; part <= 4; part++) {
        assert(rectGap(p.x + (u.x - p.x) * part / 4, p.y + (u.y - p.y) * part / 4, rock) >= t.radius - .1,
          'A faster horse swept its body through a corridor wall');
      }
      for (let j = 0; j < i; j++) {
        const v = active[j], q = previous[v.id], vt = D.TYPES[v.type], sum = t.radius + vt.radius;
        const possibleTravel = (t.speed + vt.speed) * DT * 1.8 + .5;
        if (Math.abs(p.x - q.x) > sum + possibleTravel || Math.abs(p.y - q.y) > sum + possibleTravel) continue;
        const dx = p.x - q.x, dy = p.y - q.y;
        const vx = u.x - p.x - v.x + q.x, vy = u.y - p.y - v.y + q.y, vv = vx * vx + vy * vy;
        const along = vv ? Math.max(0, Math.min(1, -(dx * vx + dy * vy) / vv)) : 0;
        const ratio = Math.hypot(dx + vx * along, dy + vy * along) / sum;
        metrics.minimumBodyRatio = Math.min(metrics.minimumBodyRatio, ratio);
        assert(ratio >= .90, `Faster bodies crossed at ${e.time.toFixed(2)}s (${u.id}/${v.id}, separation ${ratio.toFixed(3)})`);
      }
    }
    for (const u of e.units) { previous[u.id].x = u.x; previous[u.id].y = u.y; }
    for (const team of e.getStats().teams) {
      assert.equal(team.initial, team.alive + team.dead + team.routed, 'Mounted movement lost or duplicated a soldier');
      assert.equal(team.dead + team.routed, 0, 'An uncontested movement fixture lost soldiers');
    }
  }
  return { step, run(seconds) { for (let tick = 0; tick < Math.round(seconds / DT); tick++) step(); },
    metrics() { return { minimumBodyRatio: round(metrics.minimumBodyRatio), maximumStep: round(metrics.maximumStep) }; } };
}

function register(test) {
  const D = global.GameData;
  test('All horse formations sustain a much faster physical Move pace than the fastest infantry', () => {
    const types = ['swordsman', 'berserker', ...HORSES, 'commander', 'elephant'];
    const count = type => type === 'commander' ? 1 : type === 'elephant' ? 8 : 48;
    const e = new global.BattleEngine({ map: 'custom', seed: 7137,
      customMap: { biome: 'grassland', width: 9600, height: 6600, regions: [] },
      armies: [{ name: 'Pace comparison', doctrine: 'balanced', roster: Object.fromEntries(types.map(type => [type, count(type)])) },
        { name: 'Distant observer', doctrine: 'defensive', roster: { guardian: 1 } }],
      formations: [...types.map((type, index) => ({ team: 0, type, count: count(type), x: 1200, y: 700 + index * 700, facing: 0, formation: 'line' })),
        { team: 1, type: 'guardian', count: 1, x: 8500, y: 6000, facing: Math.PI, formation: 'line' }] });
    const groups = e.regiments.filter(r => r.team === 0), audit = observer(e), initial = new Map(groups.map(r => [r.id, body(e, r)]));
    const maximumSpan = new Map(groups.map(r => [r.id, initial.get(r.id).span]));
    e.command(e.regiments.filter(r => r.team === 1).map(r => r.id), 'hold'); e.start();
    for (const r of groups) assert.equal(e.command([r.id], 'move', 7000, initial.get(r.id).y), 1);
    let settled;
    for (let tick = 1; tick <= 12 / DT; tick++) {
      audit.step();
      for (const r of groups) maximumSpan.set(r.id, Math.max(maximumSpan.get(r.id), body(e, r).span));
      if (tick === 4 / DT) settled = new Map(groups.map(r => [r.id, body(e, r)]));
    }
    const speeds = Object.fromEntries(groups.map(r => [r.type, (body(e, r).x - settled.get(r.id).x) / 8]));
    for (const type of HORSES) {
      assert(speeds[type] > speeds.berserker * 2.05, `${type} failed to travel at more than twice the fastest foot pace`);
      assert(speeds[type] > speeds.swordsman * 2.6, `${type} is still close to ordinary infantry pace`);
      assert(speeds[type] >= D.TYPES[type].speed * .80, `${type}'s declared speed is hidden behind a physical movement cap`);
    }
    assert(speeds.cavalry > speeds.lancer * 1.10, 'Heavy lancers are not physically slower than Royal Cavalry');
    assert(speeds.horseArcher > speeds.cavalry * 1.05, 'Light mounted archers lost their faster pace');
    assert(speeds.commander >= speeds.cavalry * .95, 'The mounted commander cannot keep pace with its cavalry');
    assert(speeds.elephant < speeds.berserker && D.TYPES.elephant.speed === 48, 'The horse speed change also accelerated elephants');
    for (const r of groups) {
      assert(maximumSpan.get(r.id) <= initial.get(r.id).span * 1.2 + 12, 'Straight galloping tore a formation into a tail');
      assert(r.cohesion >= 90, 'A faster clear-ground march destroyed formation cohesion');
    }
    return { troops: e.units.length, sustainedSpeeds: Object.fromEntries(Object.entries(speeds).map(([type, speed]) => [type, round(speed)])),
      lancerToFastestFoot: round(speeds.lancer / speeds.berserker), ...audit.metrics() };
  });

  test('Faster horses accelerate from rest and Hold and reversed Move reset their real momentum', () => {
    const outcomes = [];
    for (const type of HORSES) {
      const e = fixture(type), r = e.regiments[0], audit = observer(e), initial = body(e, r), samples = {};
      e.command([1], 'hold'); e.start(); e.command([r.id], 'move', 5000, initial.y);
      for (let tick = 1; tick <= 6 / DT; tick++) {
        audit.step();
        if ([3, 15, 30, 180].includes(tick)) samples[tick] = body(e, r);
      }
      assert(samples[3].speed < D.TYPES[type].speed * .09 && samples[3].x - initial.x < 1, 'A horse instantly acquired full gallop');
      assert(samples[15].speed > samples[3].speed * 3 && samples[30].speed > samples[15].speed * 1.7, 'A horse failed to accelerate through actual travel');
      assert(samples[180].speed > D.TYPES[type].speed * .80, 'A horse never reached its faster gallop');
      const haltedAt = body(e, r);
      assert.equal(e.command([r.id], 'hold'), 1); audit.run(.5);
      const held = body(e, r);
      assert(Math.hypot(held.x - haltedAt.x, held.y - haltedAt.y) < .01 && held.speed === 0 && held.momentum === 0,
        'Hold allowed a previous gallop or charge energy to continue');
      assert.equal(e.command([r.id], 'move', 5000, held.y), 1); audit.run(.1);
      const restarted = body(e, r);
      assert(restarted.speed < D.TYPES[type].speed * .09 && restarted.momentum < .01, 'A fresh Move reused stopped gallop momentum');
      audit.run(3.9); const reversedAt = body(e, r);
      assert.equal(e.command([r.id], 'move', 1000, reversedAt.y), 1); audit.run(.1);
      const turning = body(e, r);
      assert(reversedAt.x - turning.x > .01 && turning.momentum < .01,
        'A reversed Move failed to depart promptly or reused the forward gallop energy');
      audit.run(3.9); const withdrawn = body(e, r);
      assert(reversedAt.x - withdrawn.x > D.TYPES[type].speed * 2.2, 'A reversed Move remained trapped behind stale forward motion');
      assert(withdrawn.span < initial.span * 1.35 + 15, 'A reversed fast march swapped ranks through one another or left a long tail');
      outcomes.push({ type, firstTenthSpeed: round(samples[3].speed), halfSecondSpeed: round(samples[15].speed),
        oneSecondSpeed: round(samples[30].speed), fullGallop: round(samples[180].speed), restartedSpeed: round(restarted.speed),
        reverseTravel: round(reversedAt.x - withdrawn.x), ...audit.metrics() });
    }
    return { outcomes };
  });

  test('Fast mounted wheels shed gallop momentum, keep real ranks together, and accept Retreat', () => {
    const outcomes = [];
    for (const type of HORSES) {
      const e = fixture(type), r = e.regiments[0], audit = observer(e), initial = body(e, r);
      e.command([1], 'hold'); e.start(); e.command([r.id], 'move', 5000, initial.y); audit.run(6);
      const corner = body(e, r); e.command([r.id], 'move', corner.x, 4000); audit.run(.1);
      const turning = body(e, r); let maximumSpan = turning.span;
      assert(turning.speed < corner.speed * .65 && turning.momentum < corner.momentum * .5, 'A sharp wheel retained straight-line gallop momentum');
      for (let tick = 3; tick < 10 / DT; tick++) { audit.step(); maximumSpan = Math.max(maximumSpan, body(e, r).span); }
      const wheeled = body(e, r);
      assert(wheeled.y - corner.y > D.TYPES[type].speed * 5, 'Faster horses stalled instead of completing their commanded wheel');
      assert(Math.abs(wheeled.x - corner.x) < 40, 'A fast wheel ignored the new destination direction');
      assert(maximumSpan < initial.span * 1.65 + 15 && r.cohesion >= 85, 'A fast wheel stretched the formation beyond bounded ranks');
      const positions = r.unitIds.map(id => ({ id, x: e.units[id].x, y: e.units[id].y }));
      assert.equal(e.command([r.id], 'retreat'), 1); audit.run(.1);
      assert(mean(positions.map(p => p.x - e.units[p.id].x)) > .01, 'Retreat failed to move the wheeling formation away immediately');
      assert(body(e, r).momentum < .01, 'Retreat retained the old charge momentum');
      const readiness = () => r.unitIds.map(id => {
        const u = e.units[id];
        return [u._mountSpeed, u._mountDriveSpeed, u._charge, u._cooldown, u.target];
      });
      const beforeRepeat = readiness();
      assert.equal(e.command([r.id], 'retreat'), 1);
      assert.deepEqual(readiness(), beforeRepeat, 'Repeating Retreat changed speed, charge power, or attack readiness');
      audit.run(3.9); const retreated = body(e, r);
      assert(r.fallingBack && r.order === 'retreat' && wheeled.x - retreated.x > D.TYPES[type].speed * 1.5,
        'The previous wheel overruled Retreat or slowed it to an infantry crawl');
      outcomes.push({ type, turnSpeed: round(turning.speed), newDirectionTravel: round(wheeled.y - corner.y),
        maximumSpan: round(maximumSpan), retreatTravel: round(wheeled.x - retreated.x), ...audit.metrics() });
    }
    return { outcomes };
  });

  test('Faster horses really traverse woodland while terrain penalties and compact ranks remain', () => {
    const outcomes = [];
    for (const type of HORSES) {
      const e = fixture(type, [{ type: 'forest', x: 1800, y: 400, w: 1200, h: 3600 }]);
      const r = e.regiments[0], audit = observer(e), initial = body(e, r), visitors = new Set();
      let maximumSpan = initial.span, forestStart, forestEnd;
      e.command([1], 'hold'); e.start(); e.command([r.id], 'move', 4800, initial.y);
      for (let tick = 1; tick <= 35 / DT; tick++) {
        audit.step(); const measured = body(e, r); maximumSpan = Math.max(maximumSpan, measured.span);
        for (const u of members(e, r)) if (e.terrainAt(u.x, u.y).type === 'forest') visitors.add(u.id);
        if (tick === 10 / DT) forestStart = measured;
        if (tick === 20 / DT) forestEnd = measured;
      }
      const final = body(e, r), forestSpeed = (forestEnd.x - forestStart.x) / 10;
      assert.equal(visitors.size, r.initial, 'A faster formation avoided the tested woodland');
      assert(final.minX > 3100, 'Rear horses never completed the woodland crossing');
      assert(forestSpeed > D.TYPES[type].speed * .30 && forestSpeed < D.TYPES[type].speed * .50,
        'Woodland either stopped the faster horses or lost its real terrain penalty');
      assert(maximumSpan < initial.span * 1.4 + 15 && r.cohesion >= 90, 'Faster woodland travel left an excessive tail or broken ranks');
      outcomes.push({ type, woodlandVisitors: visitors.size, woodlandSpeed: round(forestSpeed),
        totalAdvance: round(final.x - initial.x), initialSpan: round(initial.span), maximumSpan: round(maximumSpan),
        cohesion: round(r.cohesion), ...audit.metrics() });
    }
    return { outcomes };
  });

  test('Fast mounted formations narrow through a solid corridor and reform without body crossing', () => {
    const outcomes = [];
    for (const type of HORSES) {
      const e = fixture(type, [{ type: 'rock', x: 1900, y: 0, w: 1000, h: 2090 },
        { type: 'rock', x: 1900, y: 2310, w: 1000, h: 2090 }]);
      const r = e.regiments[0], audit = observer(e), initial = body(e, r), crossers = new Set();
      let maximumSpan = initial.span, narrowed = false;
      e.command([1], 'hold'); e.start(); e.command([r.id], 'move', 4800, 2200);
      for (let tick = 0; tick < 36 / DT; tick++) {
        audit.step(); maximumSpan = Math.max(maximumSpan, body(e, r).span);
        if (r._terrainColumns < r._shapeColumns) narrowed = true;
        for (const u of members(e, r)) if (u.x > 2900 + D.TYPES[type].radius) crossers.add(u.id);
      }
      const final = body(e, r);
      assert(narrowed && crossers.size === r.initial, 'The solid corridor did not exercise narrowing and complete passage');
      assert(final.minX > 3900, 'Rear horses stalled after the narrow passage');
      assert(maximumSpan < initial.span * 2.3 + 20, 'The narrow passage stretched faster horses into an unbounded queue');
      assert(final.span < initial.span * 1.35 + 15 && r.cohesion >= 88, 'The mounted formation failed to reform after clearing the corridor');
      outcomes.push({ type, corridorCrossers: crossers.size, initialSpan: round(initial.span), maximumSpan: round(maximumSpan),
        finalSpan: round(final.span), advance: round(final.x - initial.x), cohesion: round(r.cohesion), ...audit.metrics() });
    }
    return { outcomes };
  });
}

if (require.main === module) {
  const root = process.argv[2] || path.resolve(__dirname, '..'); global.window = global;
  for (const name of ['data', 'tactics', 'impacts', 'siege', 'engine']) {
    const file = path.join(root, 'src', name + '.js'); if (fs.existsSync(file)) require(file);
  }
  const results = [], filter = (process.env.IRONFIELD_TEST_FILTER || '').toLowerCase();
  register((name, run) => {
    if (filter && !name.toLowerCase().includes(filter)) return;
    const start = performance.now();
    try {
      const detail = run(); results.push({ name, passed: true, milliseconds: round(performance.now() - start), ...detail });
      console.log('PASS ' + name + ' ' + JSON.stringify(detail));
    } catch (error) {
      results.push({ name, passed: false, error: error.message }); console.error('FAIL ' + name + ' ' + error.stack); process.exitCode = 1;
    }
  });
  console.log(JSON.stringify({ passed: results.filter(r => r.passed).length, failed: results.filter(r => !r.passed).length, results }, null, 2));
}
module.exports = register;
