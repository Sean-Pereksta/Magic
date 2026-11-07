'use strict';
const assert = require('node:assert/strict');
const path = require('node:path');

function register(test) {
  const DT = 1 / 30;
  const make = (formation = 'volley', enemyX = 2110) => {
    const e = new global.BattleEngine({ map: 'custom', seed: 32114,
      customMap: { biome: 'grassland', regions: [] },
      armies: [{ roster: { musketeer: 36 } }, { roster: { guardian: 180 } }],
      formations: [{ team: 0, type: 'musketeer', count: 36, x: 1800, y: 2200, formation, facing: 0 },
        { team: 1, type: 'guardian', count: 180, x: enemyX, y: 2200, facing: Math.PI }] });
    e.command(e.regiments.map(r => r.id), 'hold');
    const shots = [], launch = e._launchProjectile;
    e._launchProjectile = function (p) {
      if (p.type === 'musketeer') shots.push({ time: this.time, shooter: p.attacker, rank: this.units[p.attacker]._volleyRank });
      return launch.call(this, p);
    };
    e.start(); return { e, shots, r: e.regiments[0] };
  };
  const advance = (e, seconds) => { for (let i = 0; i < Math.ceil(seconds / DT); i++) e.step(DT); };
  for (const formation of ['volley', 'column']) test('All ranks in a ' + formation + ' fire together and keep the full musket reload', () => {
    const { e, shots, r } = make(formation);
    advance(e, 52);
    const batches = [...Map.groupBy(shots, s => s.time)];
    assert.equal(batches.length, 3);
    for (const [i, [time, batch]] of batches.entries()) {
      assert.equal(new Set(batch.map(s => s.shooter)).size, 36);
      assert.equal(new Set(batch.map(s => s.rank)).size, formation === 'volley' ? 3 : 12);
      if (i) assert(time - batches[i - 1][0] >= 24 - 1e-7);
    }
    return { volleys: batches.length, ranks: r.volleyRankCount, shots: shots.length };
  });
  test('Eight of 32 formed musketeers release a company volley but seven cannot, even with detached scouts in range', () => {
    const { e, r, shots } = make('volley', 3500);
    const enemy = e.units.find(u => u.team === 1);
    enemy.x = 2225; enemy.y = 2200;
    const troops = r.unitIds.map(id => e.units[id]);
    const place = (u, inside) => {
      const dy = u.y - enemy.y;
      u.x = enemy.x - Math.sqrt(GameData.TYPES.musketeer.range ** 2 - dy * dy) + (inside ? 4 : -4);
    };
    for (let i = 0; i < 32; i++) {
      const u = troops[i];
      u.y = 2200 + (i - 15.5) * 17;
      place(u, i < 7);
      const aim = Math.atan2(enemy.y - u.y, enemy.x - u.x);
      u.angle = i < 17 ? aim + 70 * Math.PI / 180 : Math.PI;
      if (i % 4 === 0) u._vy = 8; // Some files are still dressing their ranks.
    }
    for (const [i,u] of troops.slice(32).entries()) { u.x = 2225 + i * 17; u.y = 2490; }
    e._rebuildHash();
    e._stepMusketVolleys(DT);
    assert.equal(shots.length, 0);
    place(troops[7], true); e._rebuildHash();
    r._frontTurning = true; // A slight wheel must not veto a majority already facing the enemy.
    e.time += .25; e._stepMusketVolleys(DT);
    assert.deepEqual(shots.map(s => s.shooter).sort((a,b) => a-b), r.unitIds.slice(0,32));
    assert.equal(new Set(shots.map(s => s.time)).size, 1);
  });
  test('A musket company facing away turns to aim before firing', () => {
    const { e, shots, r } = make();
    for (const id of r.unitIds) e.units[id].angle = Math.PI;
    e._stepMusketVolleys(DT); assert.equal(shots.length, 0);
    for (let tick = 0; tick < 60 && !shots.length; tick++) { e.time += DT; e._stepMusketVolleys(DT); }
    assert.equal(shots.length, 36);
  });
  test('Musketeers fire while the last files are still arriving or wheeling', () => {
    for (const order of ['move', 'face']) {
      const { e, r, shots } = make();
      // The turn is long enough that the outer files will still be moving
      // when the soldiers finish turning their heads toward the enemy.
      e.command([r.id], order, r.x + 130, r.y + (order === 'face' ? 100 : 0));
      for (let tick = 0; tick < 300 && !shots.length; tick++) e.step(DT);
      assert.equal(shots.length, 36, order + ' failed to release the company volley');
      assert.equal(r.order, order, order + ' waited for the formation to finish');
      if (order === 'face') assert(r.commandTurnRemaining > 0, 'The wheel had already finished');
      else assert(Math.hypot(r.x - r.targetX, r.y - r.targetY) <= 28 + r._shapeSpacing);
    }
  });
  test('Casualties close musket rank numbers without creating phantom fourth ranks', () => {
    const { e, r } = make();
    for (const id of r.unitIds.slice(0, 9)) e._kill(e.units[id], e.units[36]);
    e.step(DT);
    const ranks = r.unitIds.map(id => e.units[id]).filter(u => u.alive).map(u => u._volleyRank);
    assert.equal(r.volleyRankCount, 3);
    assert.deepEqual([...new Set(ranks)], [0, 1, 2]);
    assert.equal(ranks.filter(rank => rank === 2).length, 9);
  });
  test('Automatic musket companies stay within ninety men while authored battalions retain their count', () => {
    const auto = new global.BattleEngine({ armies: [{ roster: { musketeer: 720 } }, { roster: { militia: 1 } }] });
    const companies = auto.regiments.filter(r => r.type === 'musketeer');
    assert.equal(companies.length, 8); assert(companies.every(r => r.initial === 90));
    const explicit = new global.BattleEngine({ map: 'custom', customMap: { regions: [] },
      armies: [{ roster: { musketeer: 144 } }, { roster: { militia: 1 } }],
      formations: [{ team: 0, type: 'musketeer', count: 144, x: 1800, y: 2200, name: 'Authored battalion' }] });
    assert.equal(explicit.regiments[0].initial, 144); assert.equal(explicit.regiments[0].name, 'Authored battalion');
    assert.equal(explicit.captureFormations()[0].name, 'Authored battalion');
  });
  test('Close combat interrupts the whole company’s volleys and discipline returns after contact clears', () => {
    const { e, shots, r } = make('volley', 2400);
    const enemy = e.units.find(u => u.team === 1);
    // The independently chosen contact fixture is clear of friendly bodies.
    enemy.x = 1844; enemy.y = 2200; enemy.hp = 10000; enemy._cooldown = 999; e._rebuildHash();
    const hp = enemy.hp;
    advance(e, 2);
    assert.equal(r.volleyStatus, 'melee'); assert.equal(shots.length, 0);
    assert(enemy.hp < hp, 'Musketeers must defend themselves with melee attacks');
    for (const u of e.units.filter(u => u.team === 1)) u.x += 250;
    e._rebuildHash();
    advance(e, 2);
    assert.equal(shots.length, 0, 'The company must reform before resuming fire');
    advance(e, 3);
    assert(shots.length > 0, 'The company must recover volley fire after leaving melee');
    assert.equal(new Set(shots.map(s => s.time)).size, 1, 'Recovery must resume with a synchronized rank');
  });
  test('Moving musketeers retain their reload and wait to plant before firing', () => {
    const { e, shots, r } = make();
    e.command([r.id], 'move', 1550, 2200);
    advance(e, 4);
    assert.equal(shots.length, 0); assert.equal(r.volleyStatus, 'marching');
    e.command([r.id], 'hold');
    advance(e, 1);
    // Move brought the line out of range; restore a reachable target while held.
    for (const u of e.units.filter(u => u.team === 1)) u.x -= 130;
    e._rebuildHash(); advance(e, 1);
    assert(shots.length > 0);
  });
  test('Automatic musketeers fire company volleys against a held enemy', () => {
    const { e, shots, r } = make();
    e.command([r.id], 'auto');
    advance(e, 56);
    const batches = [...Map.groupBy(shots, s => s.time).values()];
    assert(batches.length >= 2, 'Automatic orders must establish and keep a working firing line');
    assert.equal(r.volleyRankCount, 3);
    for (const batch of batches) { assert.equal(batch.length, 36); assert.equal(new Set(batch.map(s => s.rank)).size, 3); }
    return { volleys: batches.length, firstVolley: +shots[0].time.toFixed(2) };
  });
  test('Artillery throws victims farther, with cannons and trebuchets exceeding catapults', () => {
    const results = {};
    for (const type of ['catapult', 'cannon', 'trebuchet']) {
      const e = new global.BattleEngine({ map: 'custom', seed: 892,
        customMap: { biome: 'grassland', regions: [] },
        armies: [{ roster: { [type]: 1 } }, { roster: { militia: 1, guardian: 1 } }],
        formations: [{ team: 0, type, count: 1, x: 1800, y: 2200 },
          { team: 1, type: 'militia', count: 1, x: 2150, y: 2200 },
          { team: 1, type: 'guardian', count: 1, x: 4500, y: 3300 }] });
      e.command(e.regiments.map(r => r.id), 'hold'); e.start();
      const target = e.units[1], x = target.x; target.hp = 1;
      e._attack(e.units[0], target);
      let lift = 0;
      for (let i = 0; i < 150; i++) { e.step(DT); lift = Math.max(lift, target._impactLift); }
      results[type] = { distance: Math.hypot(target.x - x, target.y - 2200), lift };
      assert(lift > 40 && results[type].distance > 20, type + ' must visibly launch and displace a casualty');
      assert.equal(e.terrainAt(target.x, target.y).blocked, false);
    }
    for (const type of ['cannon', 'trebuchet']) {
      assert(results[type].distance > results.catapult.distance * 1.25, type + ' must throw farther than a catapult');
      assert(results[type].lift > results.catapult.lift * 1.4);
    }
    return results;
  });
}
if (require.main === module) {
  const root = path.resolve(process.argv[2] || path.join(__dirname, '..'));
  for (const file of ['data', 'tactics', 'impacts', 'siege', 'engine']) require(path.join(root, 'src', file + '.js'));
  register((name, run) => console.log('PASS ' + name, JSON.stringify(run() || {})));
}
module.exports = register;
