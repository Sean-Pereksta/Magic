'use strict';
const assert = require('node:assert/strict');
const path = require('node:path');

function register(test) {
  const D = global.GameData, Engine = global.BattleEngine;
  test('Authored castles enclose their garrisons on four sides and every gate connects to the courtyard', () => {
    const outcomes = [];
    for (const id of ['stonekeep', 'floodgate', 'highcitadel']) {
      const e = new Engine(D.PRESETS.find(p => p.map === id));
      const towers = e.structures.filter(s => s.castleTower);
      assert.equal(towers.length, 4, id + ' requires four corner towers');
      const left = Math.min(...towers.map(s => s.x)), top = Math.min(...towers.map(s => s.y));
      const right = Math.max(...towers.map(s => s.x + s.w)), bottom = Math.max(...towers.map(s => s.y + s.h));
      const cx = (right + left) / 2, cy = (bottom + top) / 2, margin = towers[0].w;
      for (let i = 0; i <= 100; i++) {
        const x = left + 2 + (right - left - 4) * i / 100, y = top + 2 + (bottom - top - 4) * i / 100;
        for (const [px, py] of [[x, top + 2], [x, bottom - 2], [left + 2, y], [right - 2, y]]) {
          assert(e._bodyBlocked(px, py, 1), id + ' has a hole in the closed curtain');
        }
      }
      const interior = u => u.x > left + margin && u.x < right - margin && u.y > top + margin && u.y < bottom - margin;
      const garrison = e.units.filter(u => u.team === 1 && interior(u)).length;
      assert(garrison > 150 && !e.units.some(u => u.team === 0 && interior(u)), id + ' has a misplaced assault or garrison');
      assert(e.units.every(u => !e._bodyBlocked(u.x, u.y, u._radius)), id + ' troops deployed in masonry');
      const gates = e.structures.filter(s => s.castleGate);
      assert(gates.length >= 2);
      for (const g of gates) {
        const x = g.x + g.w / 2, y = g.y + g.h / 2;
        const from = g.h > g.w ? [x + Math.sign(x - cx) * 180, y] : [x, y + Math.sign(y - cy) * 180];
        assert.equal(e._findPath(...from, cx, cy, 14).length, 0, 'A closed castle had a path through its walls');
        assert(e.siege.setGateOpen(g.id, true, 1));
        const route = e._findPath(...from, cx, cy, 14);
        assert(route.length > 0, 'An opened entrance failed to connect to its courtyard');
        let prev = from;
        for (const p of route) {
          assert(e._lineClear(...prev, p.x, p.y, 14), 'A castle waypoint cut through stone');
          prev = [p.x, p.y];
        }
        assert(e.siege.setGateOpen(g.id, false, 1));
      }
      if (id === 'highcitadel') {
        const outer = e.structures.find(s => s.type === 'gate' && !s.castleGate && s.y === 5200);
        const inner = gates.find(s => s.h > s.w);
        const routeToKeep = () => e._findPath(8900, 5500, cx, cy, 18);
        e.siege.damage(outer, outer.hp);
        assert.equal(routeToKeep().length, 0, 'Opening the outer rampart bypassed the inner castle');
        assert(e.siege.setGateOpen(inner.id, true, 1));
        const route = routeToKeep();
        assert(route.length > 0);
        let prev = [8900, 5500];
        for (const p of route) {
          assert(e._lineClear(...prev, p.x, p.y, 18), 'The two-gate route clipped a curtain');
          prev = [p.x, p.y];
        }
      }
      outcomes.push({ map: id, towers: towers.length, gates: gates.length, garrison });
    }
    return { outcomes };
  });

  test('A wide company rounds a castle corner and enters a ram breach through the north gate', () => {
    const e = new Engine({
      map: 'stonekeep', seed: 867,
      armies: [{ roster: { swordsman: 128, batteringRam: 1 } }, { roster: { guardian: 1 } }],
      formations: [
        { team: 0, type: 'swordsman', count: 128, x: 3190, y: 1220, facing: 0 },
        { team: 0, type: 'batteringRam', count: 1, x: 4520, y: 620, facing: 0 },
        { team: 1, type: 'guardian', count: 1, x: 5780, y: 3120, facing: Math.PI }
      ]
    });
    const gate = e.structures.find(s => s.castleGate && s.w > s.h);
    assert(gate, 'This castle requires a usable flank gate');
    const [foot, machine] = e.regiments, troops = foot.unitIds.map(id => e.units[id]);
    const ram = e.units[machine.unitIds[0]], entered = new Set();
    const mid = gate.y + gate.h / 2, initialHp = gate.hp;
    e.command(e.regiments.map(r => r.id), 'hold'); e.start();
    assert.equal(e.command([foot.id], 'move', 4780, 1460), 1);
    assert.equal(e.siege.command([machine.id], gate.id), 1);
    let firstHit = null, breachedAt = null, arrived = 0;
    for (let frame = 0; frame < 155 * 30; frame++) {
      const before = troops.map(u => ({ x: u.x, y: u.y }));
      e.step(1/30);
      if (firstHit === null && gate.hp < initialHp) {
        firstHit = e.time;
        assert(Math.abs(ram.y - gate.y) <= ram._radius + D.TYPES.batteringRam.range, 'Ram inflicted damage from afar');
      }
      if (breachedAt === null && gate.breached) breachedAt = e.time;
      arrived = 0;
      for (const [i, u] of troops.entries()) {
        const p = before[i];
        // The route planner pads square corners. The actual moving body is
        // circular, so measure radius clearance when a step cuts that padding.
        if (!e._lineClear(p.x, p.y, u.x, u.y, u._radius)) {
          for (const s of e.structures) if (!s.breached) for (const f of [0, .25, .5, .75, 1]) {
            const x = p.x + (u.x - p.x) * f, y = p.y + (u.y - p.y) * f;
            const gap = Math.hypot(Math.max(0, s.x-x, x-s.x-s.w), Math.max(0, s.y-y, y-s.y-s.h));
            assert(gap >= u._radius, 'Troop swept through a surviving wall');
          }
        }
        if (p.y <= mid && u.y > mid && u.x > gate.x && u.x < gate.x + gate.w) {
          assert(gate.breached); entered.add(u.id);
        }
        if (u.x > 3640 && u.x < 5960 && u.y > 1040 && u.y < 3360) arrived++;
      }
      if (arrived === troops.length && entered.size === troops.length) break;
    }
    assert(firstHit > 0 && breachedAt > firstHit);
    assert.equal(entered.size, 128); assert.equal(arrived, 128);
    assert.equal(e.siege.setGateOpen(gate.id, false, 1), false, 'A destroyed door closed across the breach');
    return { troopsEntered: arrived, firstHit: +firstHit.toFixed(2), breach: +breachedAt.toFixed(2), enteredAt: +e.time.toFixed(2) };
  });

  test('Custom castle gates obey their owner, stay open around occupants and close after the tail passes', () => {
    // The minimum castle is placed off the 80-unit navigation lattice.
    const regions = D.castleWalls(1939, 1383, 320, 320, { team: 0 });
    const e = new Engine({ map: 'custom', seed: 725,
      customMap: { regions }, armies: [{ roster: { swordsman: 8 } }, { roster: { guardian: 1 } }],
      formations: [{ team: 0, type: 'swordsman', count: 8, x: 2099, y: 1543, facing: Math.PI },
        { team: 1, type: 'guardian', count: 1, x: 4910, y: 3850, facing: 0 }] });
    const [r] = e.regiments, g = e.structures.find(s => s.castleGate), troops = r.unitIds.map(id => e.units[id]);
    assert(g && e.structures.filter(s => s.castleTower).length === 4);
    const gx = g.x + g.w / 2, gy = g.y + g.h / 2;
    assert.equal(e.siege.setGateOpen(g.id, true, 1), false);
    e.command(e.regiments.map(r => r.id), 'hold'); e.start();
    assert.equal(e.command([r.id], 'move', 1590, gy), 1);
    const revision = e.terrainRevision;
    assert(e.siege.setGateOpen(g.id, true, 0));
    assert(e.terrainRevision > revision && !e._bodyBlocked(gx, gy, 26));
    assert.equal(e.siege.structureAt(gx, gy), g);
    assert.equal(e.siege.command([1], g.id), 0);
    assert.equal(e.siege.damage(g, 95, e.units.at(-1)), 0);
    let occupied = false;
    for (let i = 0; i < 35 * 30; i++) {
      e.step(1/30);
      if (troops.some(u => u.x >= g.x && u.x <= g.x + g.w && u.y >= g.y && u.y <= g.y + g.h)) {
        occupied = true;
        assert.equal(e.siege.setGateOpen(g.id, false, 0), false, 'A gate closed on a passing soldier');
      }
      if (troops.every(u => u.x < g.x - 45)) break;
    }
    assert(occupied && troops.every(u => u.x < g.x - 45));
    assert(e.siege.setGateOpen(g.id, false, 0));
    assert(e._bodyBlocked(gx, gy, 26));
    assert.equal(e._findPath(1850, gy, 2099, 1543, 16).length, 0);
    return { soldiersThrough: troops.length, revisions: e.terrainRevision };
  });

  test('Move orders from either side of an intact castle gate wait for an opening and resume without reissuing', () => {
    const outcomes = [];
    // The same gate serves an inside garrison sortie and an outside assault.
    for (const sortie of [false, true]) {
      const type = sortie ? 'cavalry' : 'swordsman', owner = sortie ? 0 : 1;
      const regions = D.castleWalls(2000, 1600, 800, 800, { team: owner });
      const x = sortie ? 2400 : 1760, gx = sortie ? 1720 : 2400, y = 2000;
      const e = new Engine({ map: 'custom', seed: 729, customMap: { regions },
        armies: [{ roster: { [type]: 24 } }, { roster: { guardian: 1 } }],
        formations: [{ team: 0, type, count: 24, x, y, facing: sortie ? Math.PI : 0 },
          { team: 1, type: 'guardian', count: 1, x: 4700, y: 3700, facing: Math.PI }] });
      const r = e.regiments[0], us = r.unitIds.map(id => e.units[id]), g = e.structures.find(s => s.castleGate);
      e.command(e.regiments.map(reg => reg.id), 'hold'); e.start(); e.command([r.id], 'move', gx, y);
      for (let i = 0; i < 15 * 30; i++) {
        e.step(1 / 30);
        assert(us.every(u => sortie ? u.x > g.x + g.w + u._radius : u.x < g.x - u._radius),
          'A marching formation crossed still-closed masonry');
      }
      assert(g.hp === g.maxHp && !g.breached && !g.open);
      assert(e.siege.setGateOpen(g.id, true, owner));
      const through = u => sortie ? u.x < g.x - u._radius : u.x > g.x + g.w + u._radius;
      for (let i = 0; i < 45 * 30 && !us.every(through); i++) e.step(1 / 30);
      assert(us.every(through), 'Opening the gate did not restart the pending move');
      outcomes.push({ type, sortie, waiting: us.length, crossed: us.filter(through).length });
    }
    return { outcomes };
  });
}
module.exports = register;
if (require.main === module) {
  global.window = global;
  const root = process.argv[2] || path.join(__dirname, '..');
  for (const n of ['data','tactics','impacts','siege','engine']) require(path.resolve(root, 'src', n + '.js'));
  register((name, run) => { console.log('PASS', name, JSON.stringify(run())); });
}
