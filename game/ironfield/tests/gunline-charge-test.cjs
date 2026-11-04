'use strict';
const assert = require('node:assert/strict');
const path = require('node:path');

function chargeTrial(seed) {
  const formations = [];
  const add = (team, type, count, x, y) => formations.push({
    team, type, count, x, y, facing: team ? Math.PI : 0, formation: 'line'
  });
  add(0, 'lancer', 64, 2200, 2100);
  add(0, 'lancer', 64, 2200, 2300);
  add(1, 'musketeer', 64, 3600, 2128);
  add(1, 'musketeer', 64, 3600, 2272);
  add(1, 'cannon', 8, 3730, 2200);
  const e = new global.BattleEngine({
    map: 'custom', seed, customMap: { biome: 'grassland', regions: [] }, formations,
    armies: [{ name: 'Riders', doctrine: 'aggressive', roster: { lancer: 128 } },
      { name: 'Gun line', doctrine: 'defensive', roster: { musketeer: 128, cannon: 8 } }]
  });
  const riders = e.units.filter(u => !u.team);
  const lineX = Math.min(...e.units.filter(u => u.type === 'musketeer').map(u => u.x));
  const dt = 1 / 30;
  e.command(e.regiments.map(r => r.id), 'hold');
  e.start();
  // Hold outside cannon range until both armies are ready. This isolates
  // repeat fire from the randomly staggered first shot of a fresh battle.
  for (let i = 0; i < 32 * 30; i++) e.step(dt);
  assert(e.units.filter(u => u.team).every(u => u._cooldown <= 0));
  const start = e.time;
  const shots = { musketBall: 0, cannonball: 0 }, crossed = new Set();
  const wings = new Set(), batteryHitters = new Set();
  let opening = null;
  const launch = e._launchProjectile, hit = e._hit;
  e._launchProjectile = function(p) {
    if (p.team === 1 && Object.hasOwn(shots, p.kind)) shots[p.kind]++;
    return launch.call(this, p);
  };
  e._hit = function(attacker, target, ...args) {
    const before = target.hp, result = hit.call(this, attacker, target, ...args);
    if (attacker.team === 0 && target.team === 1 && target.hp < before) {
      wings.add(attacker.regiment);
      if (!opening) opening = { seconds: +(e.time - start).toFixed(2), ...shots, riders: e.getStats().teams[0].alive };
      if (target.type === 'cannon') batteryHitters.add(attacker.id);
    }
    return result;
  };
  for (let wing = 0; wing < 2; wing++)
    assert.equal(e.command([wing], 'charge', undefined, undefined, { targetRegimentId: 2 + wing }), 1);
  for (let i = 0; i < 25 * 30 && e.phase === 'running'; i++) {
    e.step(dt);
    for (const u of riders) if (u.alive && !u.routed && u.x >= lineX) crossed.add(u.id);
  }
  return { seed, opening, shots, wings: wings.size, enteredLine: crossed.size,
    hitBattery: batteryHitters.size, survivingRiders: e.getStats().teams[0].alive };
}

function register(test) {
  test('A loaded mixed gun line fires its opening volley and leaves a window for a massed cavalry charge', () => {
    const outcomes = [47319, 94].map(chargeTrial);
    for (const r of outcomes) {
      assert(r.opening && r.opening.cannonball === 8 && r.opening.musketBall >= 120,
        'The charging lancers must face the fully loaded opening volleys');
      assert(r.opening.riders < 128 && r.wings === 2, 'Riders must take casualties and hit the enemy from both wings');
      assert(r.shots.cannonball <= 12 && r.shots.musketBall <= 256,
        'Sustained gunfire gave the battery too many volleys in 25 seconds');
      assert(r.survivingRiders >= 90 && r.enteredLine >= 50 && r.hitBattery >= 6,
        'Too few riders survived to enter the musket line and strike the guns: ' + JSON.stringify(r));
    }
    return { outcomes };
  });
}
if (require.main === module) {
  const root = path.resolve(process.argv[2] || path.join(__dirname, '..'));
  for (const name of ['data', 'tactics', 'impacts', 'siege', 'engine']) require(path.join(root, 'src', name + '.js'));
  register((name, run) => console.log(name, JSON.stringify(run())));
}
module.exports = register;
