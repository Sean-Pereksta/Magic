'use strict';
const assert = require('node:assert/strict');
const path = require('node:path');

function register(test) {
  test('A Berserker club sweeps the forward arc once, shoves enemies and respects cover', () => {
    const e = new global.BattleEngine({ map: 'custom', seed: 362, customMap: { biome: 'grassland', regions: [] },
      armies: [{ name: 'Azure', roster: { berserker: 2 } }, { name: 'Ember', roster: { swordsman: 6 } }] });
    const [attacker, ally] = e.units.filter(u => !u.team);
    const foe = e.units.filter(u => u.team);
    const place = (u, x, y) => { u.x = 900 + x; u.y = 900 + y; };
    place(attacker, 0, 0); place(ally, 23, 3);
    [[24, 0], [11, 30], [-15, 0], [4, 30], [50, 1], [20, -25]].forEach(([x, y], i) => place(foe[i], x, y));
    e._rebuildHash();
    e._blocks.push({ type: 'wall', x: 904, y: 884, w: 13, h: 6 });
    const before = foe.map(u => u.hp), allyHP = ally.hp;
    e._attack(attacker, foe[0]);
    const loss = foe.map((u, i) => before[i] - u.hp);
    assert(loss[0] > loss[1] && loss[1] > 0);
    assert.deepEqual(loss.slice(2), [0, 0, 0, 0]); assert.equal(ally.hp, allyHP);
    assert.deepEqual(foe.map(u => u._impactCount), [1, 1, 0, 0, 0, 0]);
    assert(foe[0]._impactNX > .99 && foe[1]._impactNY > .8);
    const effects = e.effects.filter(effect => effect.kind === 'clubSweep');
    assert.equal(effects.length, 1); assert.equal(effects[0].angle, 0);
    return { forwardHits: 2, halfAngleDegrees: Math.round(effects[0].halfAngle * 180 / Math.PI) };
  });

  test('A charging Berserker scatters a three-file rank and rock arrests the shove', () => {
    const samples = [];
    for (const rock of [false, true]) {
      const regions = rock ? [{ type: 'rock', x: 1236, y: 1180, w: 80, h: 80 }] : [];
      const e = new global.BattleEngine({ map: 'custom', seed: 362, customMap: { biome: 'grassland', regions },
        armies: [{ name: 'Azure', roster: { berserker: 1 } }, { name: 'Ember', roster: { militia: 3 } }],
        formations: [
          { team: 0, type: 'berserker', count: 1, x: 1100, y: 1200, facing: 0, formation: 'line', offsets: [{ x: 0, y: 0 }] },
          { team: 1, type: 'militia', count: 3, x: 1226, y: 1200, facing: Math.PI, formation: 'line',
            offsets: [0, 14, -14].map(y => ({ x: 0, y })) }
        ] });
      e.command([1], 'hold'); e.command([0], 'charge', undefined, undefined, { targetRegimentId: 1 }); e.start();
      const [attacker, ...line] = e.units;
      const startX = attacker.x, before = line.map(u => ({ x: u.x, y: u.y, hp: u.hp }));
      for (let i = 0; i < 180 && !line.some(u => u._impactCount); i++) e.step(1 / 30);
      assert(attacker.x - startX > 50, 'The club struck before the charge closed');
      assert(line.every(u => u._impactCount === 1));
      const losses = line.map((u, i) => before[i].hp - u.hp);
      assert(losses.every(d => d > 10) && losses[0] > losses[1]);
      for (let i = 0; i < 9; i++) e.step(1 / 30);
      const travel = line.map((u, i) => Math.hypot(u.x - before[i].x, u.y - before[i].y));
      if (rock) assert(line.every(u => u.x + u._radius <= regions[0].x) && travel.every(d => d < 4));
      else assert(travel.every(d => d > 10));
      samples.push({ rock, damage: losses.map(d => +d.toFixed(2)), displacement: travel.map(d => +d.toFixed(2)) });
    }
    return { samples };
  });
}
if (require.main === module) {
  global.window = global;
  for (const name of ['data', 'tactics', 'impacts', 'siege', 'engine']) require(path.resolve(process.argv[2] || path.resolve(__dirname, '..'), 'src', name + '.js'));
  register((name, run) => console.log(name, run()));
}
module.exports = register;
