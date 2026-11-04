'use strict';
const assert = require('node:assert/strict');
const path = require('node:path');

function register(test) {
  const dt = 1 / 30;
  function battle(type, targets, regions = [], reserve = true) {
    const roster = reserve ? { guardian: 1 } : {};
    const formations = [{ team: 0, type, count: 1, x: 1800, y: 2200, facing: 0 }];
    for (const [unit, x, y] of targets) {
      roster[unit] = (roster[unit] || 0) + 1;
      formations.push({ team: 1, type: unit, count: 1, x, y, facing: Math.PI });
    }
    if (reserve) formations.push({ team: 1, type: 'guardian', count: 1, x: 4500, y: 3300, facing: Math.PI });
    const e = new global.BattleEngine({ map: 'custom', seed: 360, customMap: { biome: 'grassland', regions },
      armies: [{ roster: { [type]: 1 } }, { roster }], formations });
    e.start();
    e.command(e.regiments.map(r => r.id), 'hold');
    return e;
  }
  function fire(e, victim) {
    const [gun] = e.units;
    e._attack(gun, victim);
    assert.equal(e.projectiles.length, 1);
    assert(gun._cooldown > 0);
    return e.projectiles[0];
  }
  function land(e, p) {
    let frames = 0;
    while (e.projectiles.includes(p) && frames++ < 180) e.step(dt);
    assert(frames > 1 && frames < 180, 'Shell must travel before striking');
  }
  test('Muskets and cannons take a full reload between successive shots', () => {
    for (const [type, reload, bombard] of [['musketeer', 12, false], ['cannon', 22, false], ['cannon', 22, true]]) {
      const wall = { type: 'wall', x: 2200, y: 2100, w: 30, h: 200, hp: 5000, team: 1 };
      const e = bombard ? battle(type, [], [wall]) : battle(type, [['elephant', 2050, 2200]]);
      const [gun, enemy] = e.units;
      if (bombard) e.siege.command([gun.regiment], e.structures[0].id);
      const launches = [];
      let lastShot;
      while (e.time < 3 * reload + 2 && e.phase === 'running' && launches.length < 3) {
        e.step(dt);
        const shot = e.projectiles.find(p => p.attacker === gun.id);
        if (!shot || shot === lastShot) continue;
        lastShot = shot;
        assert.equal(bombard ? shot.structureTarget : shot.target, bombard ? e.structures[0].id : enemy.id);
        launches.push(e.time);
        if (bombard && launches.length === 1) {
          const before = gun._cooldown;
          e.impacts.weaponImpulse(enemy, gun, -1, 0, 50, .4, 0);
          e.step(dt);
          assert.equal(gun.state, 'knocked');
          assert(Math.abs(before - gun._cooldown - dt) < 1e-8);
        }
      }
      assert.equal(launches.length, 3, `${type} fired at ${launches}`);
      for (let i = 1; i < launches.length; i++) {
        const interval = launches[i] - launches[i - 1];
        assert(interval >= reload - 1e-8 && interval <= reload + dt + 1e-8, `${type} reload took ${interval}s`);
      }
    }
  });
  test('Musket fire strikes the first enemy on its flight path and pushes that soldier back', () => {
    const e = battle('musketeer', [['guardian', 2010, 2200], ['militia', 2150, 2200]]);
    const [, front, rear] = e.units, startX = front.x, hp = front.hp, rearHP = rear.hp;
    const p = fire(e, rear);
    assert.equal(front.hp, hp);
    land(e, p);
    assert(front.hp < hp); assert.equal(rear.hp, rearHP);
    for (let i = 0; i < 6; i++) e.step(dt);
    assert(front.x > startX + 2); assert.equal(front._impactLift, 0);
  });
  test('Cannon fire launches survivors and casualties, then lands the fallen body', () => {
    const e = battle('cannon', [['militia', 2190, 2200], ['guardian', 2210, 2222], ['guardian', 2320, 2200]]);
    const [, center, flank, far] = e.units;
    center.hp = 5;
    const x = center.x, hp = flank.hp, outsideHP = far.hp, p = fire(e, center);
    land(e, p);
    assert(!center.alive && e.deadUnitIds.includes(center.id));
    assert(center._impactLift > 25 && flank._impactLift > 10 && flank.hp < hp);
    assert.equal(far.hp, outsideHP);
    for (let i = 0; i < 30; i++) e.step(dt);
    assert(center.x > x + 4 && e.time > center._impactStart + center._impactDuration);
    assert(!e.impacts.active.has(center.id));
  });
  test('A lethal final cannon shot finishes its flight before victory freezes the field', () => {
    const e = battle('cannon', [['militia', 2150, 2200]], [], false);
    const victim = e.units[1]; victim.hp = 5;
    land(e, fire(e, victim));
    const x = victim.x; assert(!victim.alive && victim._impactLift > 0);
    for (let i = 0; i < 90 && e.phase === 'running'; i++) e.step(dt);
    assert.equal(e.winner, 0); assert(victim.x > x && e.time >= victim._impactStart + victim._impactDuration);
  });
  test('Intact walls block low shots and their blast, but trebuchets arc over them', () => {
    const wall = { type: 'wall', x: 2050, y: 2100, w: 20, h: 200, team: 1, hp: 5000 };
    for (const type of ['musketeer', 'cannon', 'trebuchet']) {
      const e = battle(type, [['guardian', 2100, 2200]], [wall]);
      const victim = e.units[1], hp = victim.hp, structure = e.structures[0], initial = structure.hp;
      const p = fire(e, victim); land(e, p);
      if (type === 'trebuchet') {
        assert(victim.hp < hp && victim._impactLift > 20);
        assert.equal(structure.hp, initial);
      } else {
        assert.equal(victim.hp, hp);
        assert.equal(structure.hp < initial, type === 'cannon');
      }
    }
  });
  test('Batteries accept explicit bombardment, expend a reload on release and damage masonry on arrival', () => {
    const outcomes = [];
    for (const type of ['cannon', 'trebuchet']) {
      const range = global.GameData.TYPES[type].range, wallX = 1800 + range * .76;
      const e = battle(type, [], [{ type: 'wall', x: wallX, y: 2000, w: 36, h: 400, team: 1, hp: 5000 }]);
      const gun = e.units[0], wall = e.structures[0], hp = wall.hp;
      assert.equal(e.siege.command([gun.regiment], wall.id), 1);
      const reload = global.GameData.TYPES[type].cooldown;
      for (let i = 0; i < (reload + 2) / dt && !e.projectiles.length; i++) e.step(dt);
      const p = e.projectiles[0]; assert(p && p.structureTarget === wall.id);
      assert.equal(wall.hp, hp); assert(Math.abs(gun._cooldown - reload) < 1e-8);
      land(e, p); assert(wall.hp < hp); outcomes.push(hp - wall.hp);
      const after = wall.hp; for (let i = 0; i < 90; i++) e.step(dt);
      assert.equal(wall.hp, after);
    }
    assert(outcomes[1] > outcomes[0] * 2);
  });
}
if (require.main === module) {
  const root = process.argv[2] || path.resolve(__dirname, '..');
  for (const file of ['data', 'tactics', 'impacts', 'siege', 'engine']) require(path.join(root, 'src', file + '.js'));
  register((name, run) => { run(); console.log('PASS ' + name); });
}
module.exports = register;
