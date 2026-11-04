'use strict';
const assert = require('node:assert/strict');
const { configuration } = require('./formation-pressure-test.cjs');

function fight(rearGap, reserveTeam = 1, withdraw = 'source', rearCount = 1) {
  const sourceRows = reserveTeam ? 3 : 1;
  const config = configuration({ rows: reserveTeam ? 1 : 3, sourceRows, distance: 23 });
  const front = config.formations.find(f => f.team === reserveTeam && f.type !== 'commander');
  const sign = reserveTeam ? 1 : -1;
  const backRank = reserveTeam ? 0 : (sourceRows - 1) * 18;
  for (let rank = 1; rank <= rearCount; rank++) config.formations.push({ ...front, x: front.x + sign * (rank * rearGap + backRank), count: 12, offsets: front.offsets.slice(0, 12) });
  config.armies[reserveTeam].roster[front.type] += 12 * rearCount;
  const e = new global.BattleEngine(config), source = e.regiments[0];
  const target = e.regiments.find(r => r.team === 1 && r.type === 'militia');
  const reserve = e.regiments.at(-1);
  e.command(e.regiments.filter(r => r !== source).map(r => r.id), 'hold');
  const pile = [target, ...e.regiments.slice(-rearCount)], starts = e.units.map(u => u.x);
  let companyTravel = [];
  let pressure = 0, peak = 0, stopped = 0, earlySpeed = 0, lateSpeed = 0;
  e.start();
  for (let i = 0; i < 150; i++) {
    if (i === 120) {
      companyTravel = pile.map(r => {
        const living = r.unitIds.filter(id => e.units[id].alive && !e.units[id].routed);
        return living.reduce((sum, id) => sum + e.units[id].x - starts[id], 0) / living.length;
      });
      const leave = withdraw === 'middle' ? pile[1] : target;
      if (withdraw === 'source') e.command([source.id], 'hold');
      else e.command([leave.id], 'move', leave.x + 400, leave.y);
      assert.equal(reserve._formationPressureSpeed, 0, 'A fresh front order left the rear under stale pressure');
    }
    e.step(1 / 30);
    const pair = [...e.impacts._pressurePairs.values()].find(p => p.target === target);
    if (i < 15) earlySpeed = Math.max(earlySpeed, target._formationPressureSpeed);
    if (i > 30 && i < 120) lateSpeed = Math.max(lateSpeed, target._formationPressureSpeed);
    if (i < 120 && pair) peak = Math.max(peak, pair.speed);
    if (i < 120) pressure += reserve._formationPressureSpeed / 30;
    else stopped += reserve._formationPressureSpeed / 30;
  }
  return { pressure, peak, earlySpeed, lateSpeed, stopped, displacement: companyTravel.at(-1), companyTravel };
}

function morale(survivors, withdrawing) {
  const e = new global.BattleEngine(configuration()), r = e.regiments.find(r => r.type === 'militia');
  const attacker = e.units[e.regiments[0].unitIds[0]];
  for (const id of r.unitIds.slice(survivors)) e._kill(e.units[id], attacker);
  r.fallingBack = withdrawing; r._fallbackCount = 4; r._unsupportedTime = 60; r._recentHits = 20; r.morale = 3;
  e._moraleUpdate();
  return !!r._routed;
}

function register(test) {
  test('A pressed front carries a physically connected allied reserve back; cutting contact ends the shove', () => {
    const piled = fight(18), separated = fight(200);
    assert(piled.pressure > 1 && piled.displacement > 1, JSON.stringify(piled));
    assert(piled.lateSpeed > piled.earlySpeed + 1, 'A new contact must build up to a sustained push');
    assert.equal(separated.pressure, 0); assert.equal(piled.stopped, 0); assert.equal(fight(18, 1, 'front').stopped, 0);
    assert(piled.peak <= 8 && piled.peak < separated.peak, 'Attached defenders must also add resistance');
    const thirdLine = fight(18, 1, 'source', 2);
    assert.equal(thirdLine.stopped, 0);
    assert(thirdLine.companyTravel.every(distance => distance > 1), 'Every company in the pile must yield real ground');
    fight(18, 1, 'middle', 2);
    assert(thirdLine.pressure > 1 && thirdLine.displacement > 1 && thirdLine.peak < piled.peak, 'Pressure did not propagate through two connected reserves: ' + JSON.stringify(thirdLine));
    const supported = fight(18, 0), unsupported = fight(200, 0);
    assert(supported.peak > unsupported.peak + .5, JSON.stringify({ supported, unsupported }));
    return { piled, thirdLine, separated, supported, unsupported };
  });
  test('Only devastated regiments rout under combat or repeated unsupported withdrawal', () => {
    for (const retreat of [true, false]) { assert.equal(morale(12, retreat), false); assert.equal(morale(9, retreat), true); }
    return { startingStrength: 48, heldTogether: 12, broke: 9 };
  });
}
if (require.main === module) {
  global.window = global;
  for (const name of ['data', 'tactics', 'impacts', 'siege', 'engine']) require('../src/' + name + '.js');
  register((name, body) => console.log('PASS', name, body()));
}
module.exports = register;
