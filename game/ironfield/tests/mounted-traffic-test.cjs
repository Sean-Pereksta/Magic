'use strict';
// Public saved layouts and ordinary player orders. Measure forward progress
// from body coordinates; a rising momentum indicator cannot satisfy a gate.
const assert = require('node:assert/strict');
const path = require('node:path');
const { observer } = require('./friendly-mounted-test.cjs');
const dt = 1 / 30, mean = a => a.reduce((s, x) => s + x, 0) / a.length;
const round = n => Math.round(n * 1000) / 1000;
function company(x, y, offsets, team = 0) {
  return { team, type: team ? 'guardian' : 'lancer', formation: 'line', count: offsets.length,
    facing: team ? Math.PI : 0, x, y, offsets };
}
function deploy(groups, doctrine) {
  const e = new global.BattleEngine({ seed: 7137, map: 'custom',
    customMap: { biome: 'grassland', regions: [] }, formations: groups,
    armies: [{ name: 'The lancers', doctrine, roster: { lancer: groups.filter(g => g.team === 0).reduce((n, g) => n + g.count, 0) } },
      { name: 'Far guard', doctrine: 'defensive', roster: { guardian: groups.filter(g => g.team === 1).reduce((n, g) => n + g.count, 0) } }] });
  assert.equal(e.regiments.length, groups.length);
  for (const [index, g] of groups.entries()) {
    const r = e.regiments[index]; assert.equal(r.unitIds.length, g.count);
    if (!g.offsets) continue; // Use the game's default company layout.
    for (const [slot, id] of r.unitIds.entries()) {
      const u = e.units[id], offset = g.offsets[slot];
      assert(Math.hypot(u.x - g.x - offset.x, u.y - g.y - offset.y) < .001,
        'A starting body required relocation; this is not the public encounter under test');
    }
  }
  e.command(e.regiments.map(r => r.id), 'hold'); e.start();
  return e;
}
function convoy(order, pitch) {
  const n = 48, files = 8;
  // Distinct companies occupy alternating rows in a shared lane. Their rear
  // riders initially face the *other* company's riders, not their own front.
  const groups = [0, 1].map(k => company(2150, 2000, Array.from({length: n}, (_, i) =>
    ({x: -(Math.floor(i / files) * 2 + k) * pitch, y: (i % files - (files - 1) / 2) * pitch}))));
  groups.push(company(5500, 2000, [{x: 0, y: 0}], 1));
  const e = deploy(groups, 'aggressive'), audit = observer(e), riders = e.units.filter(u => u.team === 0);
  for (const r of e.regiments.slice(0, 2)) {
    assert.equal(e.command([r.id], order, order === 'move' ? 5050 : undefined, order === 'move' ? 2000 : undefined,
      order === 'charge' ? {targetRegimentId: 2} : {}), 1);
  }
  let warm, stopped = 0, samples = 0, initialSpeed = 0, samplePaces = [];
  const last = riders.map(u => u.x);
  for (let frame = 1; frame <= 300; frame++) {
    audit.step();
    if (frame === 90) warm = riders.map(u => u.x);
    for (const [i, u] of riders.entries()) {
      const v = (u.x - last[i]) / dt; last[i] = u.x;
      if (frame === 3) initialSpeed = Math.max(initialSpeed, v);
      if (frame > 90) { stopped += Number(v < 1); samples++; samplePaces.push(v); }
    }
  }
  audit.noDamage();
  const paces = riders.map((u, i) => (u.x - warm[i]) / 7);
  samplePaces.sort((a, b) => a - b);
  return { companyPaces: [0, 1].map(k => round(mean(paces.slice(k * n, (k + 1) * n)))),
    meanPace: round(mean(paces)), slowestRider: round(Math.min(...paces)), firstPercentile: round(samplePaces[Math.floor(samples * .01)]),
    stoppedRiderFrames: stopped, riderFrames: samples, firstTenthPeak: round(initialSpeed), ...audit.metrics() };
}
function passHeld(order, clear, count) {
  const single = [{x: 0, y: 0}], movingFile = Array.from({length: count}, (_, i) => ({x: -i * 24, y: 0}));
  const e = deploy([company(1400, 2200, movingFile),
    company(1880, clear ? 2600 : 2200, single), company(5200, 2200, single, 1)], 'balanced');
  assert.equal(e.command([0], order, order === 'charge' ? 5200 : 4500, 2200), 1);
  const audit = observer(e), horses = e.regiments[0].unitIds.map(id => e.units[id]), held = e.units[e.regiments[1].unitIds[0]];
  const originX = held.x, originY = held.y, originFacing = held.angle, speeds = horses.map(() => []);
  let slowest = Infinity, side = 0, fore = 0, power = 0, facingError = 0;
  for (let frame = 0; frame < 300 && horses.some(u => u.x < originX + 75); frame++) {
    const before = horses.map(u => u.x); audit.step();
    for (const [i, u] of horses.entries()) {
      const x = before[i], v = (u.x - x) / dt;
      if (x >= originX - 100 && x <= originX + 28) { speeds[i].push(v); slowest = Math.min(slowest, v); }
    }
    side = Math.max(side, Math.abs(held.y - originY)); fore = Math.max(fore, Math.abs(held.x - originX));
    power = Math.max(power, e.impacts.power(held));
    facingError = Math.max(facingError, Math.abs(Math.atan2(Math.sin(held.angle - originFacing), Math.cos(held.angle - originFacing))));
  }
  audit.noDamage();
  assert(horses.every((u, i) => u.x >= originX + 75 && speeds[i].length >= 12), 'A rider never completed its actual pass');
  return { meanPace: round(mean(speeds.map(mean))), slowestPass: round(Math.min(...speeds.map(mean))),
    minimumPace: round(slowest), side: round(side), heldForward: round(fore),
    heldPower: round(power), facingError: round(facingError), ...audit.metrics() };
}
function register(test) {
  test('Default lancer wings charging one target keep their approach files and both attack', () => {
    const wings = [2070, 2330].map(y => ({ team: 0, type: 'lancer', formation: 'line', count: 48,
      x: 1400, y, facing: Math.atan2(2200 - y, 2200) }));
    const target = { team: 1, type: 'guardian', formation: 'line', count: 48, x: 3600, y: 2200, facing: Math.PI };
    const e = deploy([...wings, target], 'balanced'), audit = observer(e);
    const riders = e.units.filter(u => u.team === 0), last = riders.map(u => u.x);
    const hitters = [new Set(), new Set()], realHit = e._hit;
    e._hit = function(attacker, defender, ...args) {
      const hp = defender.hp, out = realHit.call(this, attacker, defender, ...args);
      if (defender.regiment === 2 && defender.hp < hp) hitters[attacker.regiment].add(attacker.id);
      return out;
    };
    assert.equal(e.command([0, 1], 'charge', undefined, undefined, {targetRegimentId: 2}), 2);
    let lowestDecile = Infinity;
    for (let frame = 1; frame <= 10 / dt; frame++) {
      audit.step();
      if (frame % 15) continue;
      for (const wing of e.regiments.slice(0, 2)) {
        const paces = wing.unitIds.map(id => (e.units[id].x - last[id]) / (15 * dt)).sort((a, b) => a - b);
        if (frame >= 4 / dt) lowestDecile = Math.min(lowestDecile, paces[Math.floor(paces.length * .1)]);
      }
      for (const rider of riders) last[rider.id] = rider.x;
    }
    audit.noDamage();
    assert(lowestDecile >= GameData.TYPES.lancer.speed * 1.05,
      'Allied charge sectors braked the trailing files during the approach: ' + round(lowestDecile));
    for (let frame = 0; frame < 25 / dt && e.phase !== 'ended' && hitters.some(h => h.size < 3); frame++) audit.step();
    assert(hitters.every(h => h.size >= 3), 'Both wings must finish their charge against the selected enemy');
    return { lowestDecile: round(lowestDecile), attackingRiders: hitters.map(h => h.size), ...audit.metrics() };
  });
  for (const order of ['move', 'auto', 'charge']) test('Alternating allied lancer ranks maintain actual ' + order + ' pace across company boundaries', () => {
    const clear = convoy(order, 36), packed = convoy(order, 22.5), result = { order, clear, packed };
    try {
      assert(clear.meanPace >= GameData.TYPES.lancer.speed * .75, 'The clear-ground control did not reach a normal riding pace');
      assert(clear.firstTenthPeak < GameData.TYPES.lancer.speed * .16 && packed.firstTenthPeak < GameData.TYPES.lancer.speed * .16,
        'Ordering the moving files created an instant gallop from rest');
      assert(packed.closeFriendlyTicks > 1000, 'The packed formations did not exercise real friendly proximity');
      assert(packed.companyPaces.every(v => v >= clear.meanPace * .95), 'A packed company lost substantial forward pace to its ally');
      assert(packed.slowestRider >= clear.meanPace * .88, 'The average concealed a slow or abandoned rear rider');
      assert(packed.firstPercentile >= clear.meanPace * .70 && packed.stoppedRiderFrames <= packed.riderFrames * .001,
        'Moving files repeatedly braked despite open ground ahead of the convoy');
    } catch (error) { error.details = result; throw error; }
    return result;
  });
  test('A lancer at riding speed passes a single held ally on open ground without a marching slowdown', () => {
    // Include the tail of a formed charge. A skirting leader on its own is
    // not proof that the rest of a file gets past the same parked horse.
    const results = [['move', 1], ['charge', 1], ['charge', 8]].map(([order, count]) =>
      ({ order, count, clear: passHeld(order, true, count), crowded: passHeld(order, false, count) }));
    for (const {clear, crowded} of results) {
      try {
        assert(clear.meanPace > GameData.TYPES.lancer.speed * .8);
        assert(crowded.meanPace >= clear.meanPace * .95 && crowded.slowestPass >= clear.meanPace * .90 &&
          crowded.minimumPace >= clear.meanPace * .85,
          'Meeting one held ally took too much real speed out of the passing rider');
        assert(crowded.side > GameData.TYPES.lancer.radius * .60 && crowded.heldForward < .1 &&
          crowded.heldPower === 0 && crowded.facingError < .001,
          'The held ally must actually yield sideways while keeping its order and zero charge power');
      } catch (error) { error.details = results; throw error; }
    }
    return { results };
  });
  test('One group Charge carries both Muster Field companies to the wide enemy front at riding speed', () => {
    // Exercise precisely the example a player can import and play. Ordering
    // regiments separately against a single distant target misses premature
    // steering toward opposing frontage assignments in a group Charge.
    const scene = JSON.parse(require('node:fs').readFileSync(
      path.join(__dirname, '..', 'examples', 'lancer-convoy.ironfield.json'), 'utf8'));
    const e = new global.BattleEngine(scene.config), allies = e.regiments.filter(r => r.team === 0);
    assert.deepEqual(allies.map(r => [r.type, r.initial]), [['lancer', 48], ['lancer', 48]]);
    assert(e.regiments.some(r => r.team === 1 && r.initial >= 100), 'The published drill lost its broad battle line');
    for (const [i, r] of e.regiments.entries()) {
      const g = scene.config.formations[i];
      for (const [j, id] of r.unitIds.entries()) {
        const u = e.units[id], p = g.offsets[j];
        assert(Math.hypot(u.x - g.x - p.x, u.y - g.y - p.y) < .001, 'The published drill required deployment repair');
      }
    }
    e.start();
    assert.equal(e.command(allies.map(r => r.id), 'charge'), 2);
    const audit = observer(e), horses = allies.flatMap(r => r.unitIds.map(id => e.units[id]));
    const hitters = allies.map(() => new Set()), damage = allies.map(() => 0), realHit = e._hit;
    e._hit = function(attacker, target, ...args) {
      const hp = Math.max(0, target.hp), out = realHit.call(this, attacker, target, ...args);
      const loss = hp - Math.max(0, target.hp);
      if (attacker.team === 0 && target.team === 1 && loss > 0) {
        hitters[attacker.regiment].add(attacker.id); damage[attacker.regiment] += loss;
      }
      return out;
    };
    let three, stopped = 0, frames = 0;
    for (let tick = 0; tick < 300; tick++) {
      const before = horses.map(u => u.x);
      if (tick === 90) three = before;
      audit.step();
      if (tick >= 90) for (const [i, u] of horses.entries()) { stopped += Number((u.x - before[i]) / dt < 1); frames++; }
    }
    audit.noDamage();
    const paces = horses.map((u, i) => (u.x - three[i]) / 7);
    for (let tick = 300; tick < 35 * 30 && e.phase !== 'ended' && hitters.some(s => s.size < 6); tick++) audit.step();
    const result = { scenario: scene.name, meanPace: round(mean(paces)), slowestRider: round(Math.min(...paces)),
      stoppedRiderFrames: stopped, riderFrames: frames, attackingRiders: hitters.map(s => s.size),
      actualDamage: damage.map(round), observedSeconds: round(e.time), ...audit.metrics() };
    try {
      assert(result.meanPace > GameData.TYPES.lancer.speed * 1.10 && result.slowestRider > GameData.TYPES.lancer.speed * 1.07,
        'Early frontline assignments slowed the charging companies on open ground');
      assert(stopped <= frames * .001, 'Members of this group Charge repeatedly stopped during the long run-up');
      assert(hitters.every(s => s.size >= 6) && damage.every(n => n > 0),
        'The convoy remained on the march without both companies physically attacking the enemy frontage');
    } catch (error) { error.details = result; throw error; }
    return result;
  });
  test('Two riders on converging orders retain gallop through real open-ground passing gaps', () => {
    const trials = [[180, 0, dt, 'move'], [180, 0, dt, 'charge'], [180, 0, .1, 'charge'],
      [90, 0, dt, 'charge'], [90, 150, dt, 'charge']];
    const outcomes = [];
    for (const [degrees, delay, frameTime, order] of trials) {
      const a = degrees * Math.PI / 180, axes = [{x: 1,y: 0}, {x: Math.cos(a), y: Math.sin(a)}];
      const make = (i, team) => {
        const z = axes[i], distance = team ? 1600 : -(550 + (i ? delay : 0));
        return {...company(3000 + z.x * distance, 2100 + z.y * distance, [{x: 0, y: 0}], team),
          facing: Math.atan2(z.y,z.x) + (team ? Math.PI : 0)};
      };
      const groups = [make(0,0),make(1,0),make(0,1),make(1,1)], e = deploy(groups, 'balanced'), audit = observer(e, frameTime);
      for (let i = 0; i < 2; i++) assert.equal(e.command([i], order, groups[i+2].x, groups[i+2].y,
        order === 'charge' ? {targetRegimentId: i+2} : {}), 1);
      const riders = e.regiments.slice(0,2).map(r => e.units[r.unitIds[0]]);
      const metrics = riders.map(() => ({ approachPaces: [], minimumAtCrossing: Infinity, frames: 0, side: 0, passedBy: 0 }));
      for (let frame = 0; frame < Math.round(8/frameTime); frame++) {
        const before = riders.map(u => ({x:u.x, y:u.y})); audit.step();
        for (const [i, u] of riders.entries()) {
          const z = axes[i], m = metrics[i], along = (u.x - 3000) * z.x + (u.y - 2100) * z.y;
          const pace = ((u.x - before[i].x) * z.x + (u.y - before[i].y) * z.y) / frameTime;
          if (along >= -300 && along <= -160) m.approachPaces.push(pace);
          if (along >= -110 && along <= 110) { m.frames++; m.minimumAtCrossing = Math.min(m.minimumAtCrossing, pace); }
          m.side = Math.max(m.side, Math.abs((u.y - groups[i].y) * z.x - (u.x - groups[i].x) * z.y));
          m.passedBy = along;
        }
      }
      audit.noDamage();
      outcomes.push({ degrees, offsetInArrivalDistance: delay, dt: round(frameTime), order,
        // Use the actual typical run-up rather than a transient one-step
        // formation catch-up burst as the reference for sustained riding.
        riders: metrics.map(({approachPaces, ...m}) => ({cruise: round(approachPaces.sort((a,b) => a-b)[Math.floor(approachPaces.length/2)]),
          ...Object.fromEntries(Object.entries(m).map(([k,v]) => [k,round(v)]))})), ...audit.metrics() });
    }
    try {
      for (const result of outcomes) {
        for (const r of result.riders) {
          assert(r.cruise >= GameData.TYPES.lancer.speed * (result.order === 'charge' ? 1.07 : .82),
            'Two individual riders were incorrectly slowed as a marching column before the crossing');
          assert(r.frames >= 10 && r.passedBy > 180 && r.minimumAtCrossing >= r.cruise * .93,
            'A rider stopped or lost substantial actual gallop while meeting its moving ally');
        }
        const side = result.riders.reduce((sum, r) => sum + r.side, 0);
        if (result.offsetInArrivalDistance) assert(side < 2, 'A crossing at different times should not force either horse out of its lane');
        else assert(side > GameData.TYPES.lancer.radius, 'The genuinely conflicting trajectories did not open a real sideways gap');
      }
    } catch (error) { error.details = outcomes; throw error; }
    return {outcomes};
  });
}
module.exports = register;
if (require.main === module) {
  const root = process.argv[2] || path.resolve(__dirname, '..'); global.window = global;
  for (const name of ['data', 'tactics', 'impacts', 'siege', 'engine']) require(path.join(root, 'src', name + '.js'));
  const filter = (process.env.IRONFIELD_TEST_FILTER || '').toLowerCase();
  register((name, run) => {
    if (filter && !name.toLowerCase().includes(filter)) return;
    try { console.log('PASS ' + name, JSON.stringify(run())); }
    catch (error) { console.error('FAIL ' + name, error.message, JSON.stringify(error.details)); process.exitCode = 1; }
  });
}
