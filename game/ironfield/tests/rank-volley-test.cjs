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
  test('Musketeer ranks fire synchronized rotating volleys with a full individual reload', () => {
    const { e, shots, r } = make();
    assert.equal(r.formation, 'volley');
    assert.equal(new Set(r.unitIds.map(id => e.units[id].x)).size, 3, 'Deployment must have exactly three physical ranks');
    advance(e, 56);
    const volleys = Map.groupBy(shots, s => s.time);
    assert(volleys.size >= 6, 'The gun line must continue rotating through the ranks');
    const batches = [...volleys.entries()];
    for (let i = 0; i < batches.length; i++) {
      const [time, batch] = batches[i];
      assert.equal(batch.length, 12, 'Every man in a complete rank must fire in the same simulation step');
      assert.equal(new Set(batch.map(s => s.rank)).size, 1, 'Two ranks may never fire in the same step');
      assert.equal(batch[0].rank, i % 3, 'Fire must pass to the next physical rank');
      if (i) assert(time - batches[i - 1][0] >= 8 - 1e-7, 'Ranks must leave the specified interval between volleys');
    }
    for (const history of Map.groupBy(shots, s => s.shooter).values()) for (let i = 1; i < history.length; i++)
      assert(history[i].time - history[i - 1].time >= 24 - 1e-7, 'A soldier fired before finishing the long reload');
    assert.equal(r.volleyRankCount, 3);
    return { volleys: batches.length, shots: shots.length, firstRankTimes: batches.slice(0, 4).map(x => +x[0].toFixed(2)) };
  });
  test('A three-file musket column has a distinct footprint and fires one physical row at a time', () => {
    const { e, shots, r } = make('column', 2070);
    assert.equal(new Set(r.unitIds.map(id => e.units[id].y)).size, 3, 'Column must be three soldiers wide');
    assert.equal(new Set(r.unitIds.map(id => e.units[id].x)).size, 12, 'Column must have twelve physical rows');
    advance(e, 20);
    const batches = [...Map.groupBy(shots, s => s.time).values()];
    assert(batches.length >= 6);
    for (const batch of batches) { assert.equal(batch.length, 3); assert.equal(new Set(batch.map(s => s.rank)).size, 1); }
    assert.equal(r.volleyRankCount, 12);
    return { rows: r.volleyRankCount, volleys: batches.length };
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
  test('Automatic musketeers settle into three ranks and fire whole rows against a held enemy', () => {
    const { e, shots, r } = make();
    e.command([r.id], 'auto');
    advance(e, 24);
    const batches = [...Map.groupBy(shots, s => s.time).values()];
    assert(batches.length >= 2, 'Automatic orders must establish and keep a working firing line');
    assert.equal(r.volleyRankCount, 3);
    for (const batch of batches) { assert.equal(batch.length, 12); assert.equal(new Set(batch.map(s => s.rank)).size, 1); }
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
