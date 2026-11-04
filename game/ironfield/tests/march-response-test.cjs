'use strict';
const assert = require('node:assert/strict');
const path = require('node:path');

function register(test) {
  function battle() {
    const engine = new global.BattleEngine({ map: 'custom', seed: 4719, customMap: { biome: 'grassland', regions: [] },
      armies: [{ name: 'Azure', doctrine: 'balanced', roster: { swordsman: 24 } },
        { name: 'Ember', doctrine: 'balanced', roster: { guardian: 24 } }] });
    const [marchers, defenders] = engine.regiments;
    engine.relocateRegiments([marchers.id], 2400, 2200);
    engine.relocateRegiments([defenders.id], 2540, 2200);
    engine.command([defenders.id], 'hold'); engine.start();
    for (let tick = 0; tick < 600 && marchers.engagedSoldiers < 4; tick++) engine.step(1 / 30);
    assert(marchers.engagedSoldiers >= 4);
    return { engine, marchers, soldiers: marchers.unitIds.map(id => engine.units[id]).filter(u => u.alive) };
  }
  test('Muster infantry continue their ordered withdrawal past a crossing allied wing', () => {
    const outcomes = [];
    for (const order of ['move', 'retreat']) {
      const engine = new global.BattleEngine();
      const regiment = engine.regiments.find(r => r.team === 0 && r.type === 'swordsman');
      const soldiers = regiment.unitIds.map(id => engine.units[id]);
      const start = soldiers.map(u => u.x);
      engine.start(); engine.command([regiment.id], order, regiment.x - 500, regiment.y);
      for (let tick = 0; tick < 150; tick++) engine.step(1 / 30);
      const progress = soldiers.map((u, i) => start[i] - u.x).sort((a, b) => a - b);
      const median = progress[Math.floor(progress.length / 2)];
      assert(progress[0] > 20 && median > 115, `${order} sent ranks back to form files mid-march`);
      outcomes.push({ order, slowest: progress[0], median });
    }
    return { outcomes };
  });
  test('Scattered troops march across open ground before closing ranks at the destination', () => {
    const { engine, marchers, soldiers } = battle();
    // An uneven surviving wing in open space makes en-route regrouping measurable.
    for (let i = 0; i < soldiers.length; i++) {
      soldiers[i].x = 1800 + (i % 4) * 25;
      soldiers[i].y = 2100 + Math.floor(i / 4) * 52;
    }
    engine._rebuildHash();
    const positions = soldiers.map(u => ({ x: u.x, y: u.y }));
    engine.command([marchers.id], 'move', 1500, 2230);
    for (let tick = 0; tick < 30; tick++) engine.step(1 / 30);
    const crossDrift = Math.max(...soldiers.map((u, i) => Math.abs(u.y - positions[i].y)));
    assert(crossDrift < 4 && soldiers.every((u, i) => positions[i].x - u.x > 25));
    for (let tick = 0; tick < 800; tick++) engine.step(1 / 30);
    assert.equal(marchers.order, 'hold');
    const finalWidth = Math.max(...soldiers.map(u => u.y)) - Math.min(...soldiers.map(u => u.y));
    assert(finalWidth < 150, `Scattered wing remained ${finalWidth} units wide after arrival`);
    return { crossDrift, finalWidth };
  });
  for (const order of ['move', 'retreat']) test(`Engaged infantry obey ${order} on the next simulation step`, () => {
    const { engine, marchers, soldiers } = battle();
    const start = soldiers.map(u => u.x), center = start.reduce((a, b) => a + b, 0) / start.length;
    const destination = center - 320;
    assert.equal(engine.command([marchers.id], order, destination, 2200), 1);
    engine.step(1 / 30);
    const moved = soldiers.filter((u, i) => start[i] - u.x > .1).length;
    assert(moved >= soldiers.length * .8, `${moved}/${soldiers.length} moved away on the first step`);
    if (order === 'retreat') assert.equal(engine.command([marchers.id], 'move', destination, 2200), 1);
    for (let tick = 0; tick < 600 && marchers.order !== 'hold'; tick++) engine.step(1 / 30);
    assert.equal(marchers.order, 'hold');
    return { actedOnFirstStep: moved, soldiers: soldiers.length };
  });
}
if (require.main === module) {
  global.window = global;
  for (const name of ['data', 'tactics', 'impacts', 'siege', 'engine']) require(path.resolve(process.argv[2] || path.resolve(__dirname, '..'), 'src', name + '.js'));
  register((name, run) => console.log(name, run()));
}
module.exports = register;
