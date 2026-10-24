import test from 'node:test';
import assert from 'node:assert/strict';
import { UNITS } from '../data.mjs';
import { createBattle, beginBattle, stepBattle, issueOrder, battleResult, finishAutomatically, validateBattleCheckpoint, nearestBattlePosition, requestAlliedOrder } from '../battle-sim.mjs';

function encounter(a = { menAtArms: 5 }, d = { levy: 5 }, extras = {}) {
  return { id: 'encounter-1', seed: 42, playerOwner: 'ashen', playerSide: 0, tile: { terrain: 'plains' }, participants: [
    { armyId: 'attackers', owner: 'ashen', side: 0, units: a, morale: .85, formation: 'balanced', general: { name: 'Captain Mira' } },
    { armyId: 'defenders', owner: 'vesper', side: 1, units: d, morale: .85, formation: 'balanced' }
  ], ...extras };
}
function run(b, seconds, input = {}) { for (let n = 0; n < Math.round(seconds * 20); n++) stepBattle(b, .05, input); return b; }
function duel(type = 'menAtArms', enemy = 'menAtArms', enemyFacing = Math.PI) {
  const b = createBattle(encounter({ [type]: 1 }, { [enemy]: 1 })); beginBattle(b);
  const a = b.units[0], d = b.units[1];
  Object.assign(a, { x: 0, z: 0, facing: 0, explicitOrder: true });
  Object.assign(d, { x: 0, z: 1.9, facing: enemyFacing, explicitOrder: true, order: { kind: 'hold', x: 0, z: 1.9, facing: enemyFacing } });
  return { b, a, d };
}

test('every campaign troop is represented exactly once, including mounted and siege units', () => {
  const roster = Object.fromEntries(Object.keys(UNITS).map(type => [type, 3]));
  const e = encounter(roster), b = createBattle(e);
  assert.equal(b.units.filter(u => u.armyId === 'attackers').length, 45);
  assert.equal(new Set(b.units.map(u => u.id)).size, b.units.length);
  assert.equal(b.commanderId, b.units.find(u => u.armyId === 'attackers' && u.family !== 'siege').id);
  assert.equal(b.commanderName, 'Captain Mira');
  assert.equal(b.units[0].morale, 85);
  assert.equal(validateBattleCheckpoint(b, e), true);
});

test('proportional active caps preserve numerical advantage and all reserves', () => {
  const b = createBattle(encounter({ levy: 500, cavalry: 50, archer: 50 }, { levy: 60 }), { maxActive: 110 });
  assert.deepEqual(b.activeCaps, [100, 10]);
  assert.equal(b.units.filter(u => u.status === 'active').length, 110);
  assert.equal(b.units.length, 660);
  assert.ok(b.units.some(u => u.type === 'cavalry' && u.status === 'active'));
  assert.ok(b.units.some(u => u.type === 'archer' && u.status === 'active'));
  const a = battleResult(b).armies[0];
  assert.equal(Object.values(a.units).reduce((s, n) => s + n), 600);
  assert.equal(Object.values(a.undeployed).reduce((s, n) => s + n), 500);
});

test('rare recruited siege roles deploy without creating replacement troops', () => {
  const b = createBattle(encounter({ levy: 1000, ram: 2, trebuchet: 1 }, { levy: 100 }), { maxActive: 110 });
  assert.ok(b.units.some(u => u.type === 'ram' && u.status === 'active'));
  assert.ok(b.units.some(u => u.type === 'trebuchet' && u.status === 'active'));
  assert.equal(b.units.filter(u => u.type === 'ram').length, 2);
  assert.equal(b.units.filter(u => u.type === 'trebuchet').length, 1);
});

test('seed and fixed timesteps make replay independent of render frequency', () => {
  const e = encounter(), a = createBattle(e), b = createBattle(e);
  beginBattle(a); beginBattle(b);
  for (let n = 0; n < 20; n++) stepBattle(a, .1, { commanderAI: true });
  for (let n = 0; n < 40; n++) stepBattle(b, .05, { commanderAI: true });
  assert.deepEqual(a.units, b.units);
  assert.deepEqual(a.projectiles, b.projectiles);
  assert.equal(a.rng, b.rng);
});

test('reinforcements arrive from their own edge and prevent premature victory', () => {
  const e = encounter({ levy: 8 }, { levy: 1 });
  e.participants.push({ armyId: 'relief', owner: 'vesper', side: 1, units: { knight: 2 }, morale: .8, arrivalSeconds: 25, entryDirection: 'east', distance: 1 });
  const b = createBattle(e); beginBattle(b);
  const d = b.units.find(u => u.armyId === 'defenders'); d.status = 'dead'; d.hp = 0;
  run(b, 24);
  assert.equal(b.phase, 'combat');
  assert.ok(b.units.filter(u => u.armyId === 'relief').every(u => u.status === 'reserve'));
  run(b, 2);
  assert.equal(b.armies.find(a => a.armyId === 'relief').arrived, true);
  assert.ok(b.units.filter(u => u.armyId === 'relief').every(u => u.status === 'active' && u.x > 65));
});

test('a blocked entry keeps arriving troops in reserve instead of spawning inside opponents', () => {
  const e = encounter({ levy: 1 }, { levy: 32 });
  e.participants.push({ armyId: 'relief', owner: 'ashen', side: 0, units: { knight: 2 }, morale: .8, arrivalSeconds: 1, entryDirection: 'east' });
  const b = createBattle(e); beginBattle(b);
  const enemy = b.units.filter(u => u.side === 1);
  enemy.forEach((u, n) => { u.x = 75; u.z = -75 + n * 5; u.explicitOrder = true; u.order = { kind: 'hold', x: u.x, z: u.z }; });
  run(b, 1.1);
  assert.ok(b.units.filter(u => u.armyId === 'relief').every(u => u.status === 'reserve'));
  enemy.forEach((u, n) => { u.x = n - 15; u.z = 35; u.order = { kind: 'hold', x: u.x, z: u.z }; });
  run(b, 1.1);
  assert.ok(b.units.filter(u => u.armyId === 'relief').every(u => u.status === 'active'));
});

test('withdrawal preserves unarrived and undeployed troops without overlapping result categories', () => {
  const e = encounter({ levy: 300 }, { levy: 15 });
  e.participants.push({ armyId: 'late-friends', owner: 'ashen', side: 0, units: { archer: 9 }, morale: .8, arrivalSeconds: 50 });
  const b = createBattle(e, { maxActive: 30 }); beginBattle(b);
  issueOrder(b, 'all', 'retreat'); run(b, .1);
  const r = battleResult(b);
  assert.equal(b.phase, 'complete'); assert.equal(r.winner, 1);
  const late = r.armies.find(a => a.armyId === 'late-friends');
  assert.equal(late.units.archer, 9); assert.equal(late.undeployed.archer, 9); assert.equal(late.losses.archer, 0);
  for (const a of r.armies) for (const [type, n] of Object.entries(a.starting)) assert.equal(a.units[type] + a.losses[type], n);
});

test('commander knockout leaves the army fighting and does not create another commander', () => {
  const b = createBattle(encounter()); beginBattle(b);
  const commander = b.units.find(u => u.id === b.commanderId); commander.status = 'dead'; commander.hp = 0;
  run(b, .2);
  assert.equal(b.phase, 'combat'); assert.equal(b.commanderId, commander.id);
  assert.equal(b.units.filter(u => u.commander).length, 1);
  assert.equal(battleResult(b).armies[0].commanderKnockedOut, true);
});

test('weapon attacks have windup and recovery and consume stamina', () => {
  const { b, a, d } = duel(); d.shield = false;
  stepBattle(b, .05, { attack: true, facing: 0 });
  assert.equal(d.hp, d.maxHp); assert.equal(a.action.kind, 'attack'); assert.ok(a.stamina < 100);
  run(b, .2, { attack: true, facing: 0 }); assert.equal(d.hp, d.maxHp);
  run(b, .15, { attack: true, facing: 0 }); assert.ok(d.hp < d.maxHp);
  const damage = d.hp; run(b, .2, { attack: true, facing: 0 }); assert.equal(d.hp, damage);
});

test('shields block their facing arc, and heavy blows pressure stamina', () => {
  function strike(front, heavy) {
    const { b, a, d } = duel();
    d.facing = front ? Math.PI : 0; d.blocking = true; d.blockStarted = -1;
    a.action = { kind: 'attack', elapsed: .29, windup: .3, recovery: .45, duration: .75, targetId: d.id, facing: 0, heavy, impacted: false };
    stepBattle(b, .05, { facing: 0 });
    return { damage: d.maxHp - d.hp, stamina: d.stamina };
  }
  assert.ok(strike(true, false).damage < strike(false, false).damage / 2);
  assert.ok(strike(true, true).stamina < strike(true, false).stamina);
});

test('front-facing braced spears physically stop a fast mounted charge', () => {
  function charge(front) {
    const { b, a, d } = duel('cavalry', 'spearman'); d.z = 3;
    d.braced = true; d.facing = front ? Math.PI : 0;
    a.speed = 6; a.chargeDistance = 15;
    a.action = { kind: 'attack', elapsed: .39, windup: .4, recovery: .9, duration: 1.3, targetId: d.id, facing: 0, impacted: false };
    stepBattle(b, .05, { moveZ: 1, facing: 0 });
    return { hp: a.hp, speed: a.speed, events: b.events };
  }
  const front = charge(true), rear = charge(false);
  assert.ok(front.hp < rear.hp); assert.equal(front.speed, 0);
  assert.ok(front.events.some(e => e.type === 'brace'));
});

test('ranged attacks launch traveling projectiles and consume finite ammunition', () => {
  const { b, a, d } = duel('archer', 'levy'); d.z = 25; d.order.z = 25;
  run(b, .9, { attack: true, facing: 0, aimX: 0, aimZ: 25 });
  assert.equal(a.ammo, 23); assert.ok(b.projectiles.length > 0); assert.equal(d.hp, d.maxHp);
  run(b, .7, { facing: 0 }); assert.ok(d.hp < d.maxHp);
  const ammo = a.ammo;
  run(b, 1, { weapon: 'sword' }); run(b, .1, { weapon: 'crossbow' });
  assert.equal(a.ammo, ammo); assert.equal(a.weapon, 'crossbow');
});

test('loadout switching changes actual reach; lance and bows require appropriate roster', () => {
  const { b, a } = duel('levy');
  stepBattle(b, .05, { weapon: 'spear', block: true }); assert.equal(a.weapon, 'spear'); assert.equal(a.braced, true);
  stepBattle(b, .05, { weapon: 'lance' }); assert.equal(a.weapon, 'spear');
  stepBattle(b, .05, { weapon: 'bow' }); assert.equal(a.weapon, 'spear'); assert.equal(a.ammo, 0);
});

test('mounting and dismounting reuse the counted rider and require a fresh interact press', () => {
  const { b, a } = duel('knight');
  const n = b.units.length;
  stepBattle(b, .05, { interact: true }); assert.equal(a.mounted, false);
  stepBattle(b, .05, { interact: true }); assert.equal(a.mounted, false);
  stepBattle(b, .05, { interact: false }); stepBattle(b, .05, { interact: true });
  assert.equal(a.mounted, true); assert.equal(b.units.length, n);
});

test('brief input survives render frames that precede the next physics step', () => {
  const { b, a } = duel('knight');
  stepBattle(b, .016, { interact: true }); stepBattle(b, .016, {}); stepBattle(b, .018, {});
  assert.equal(a.mounted, false);
  stepBattle(b, .016, { attack: true }); stepBattle(b, .016, {}); stepBattle(b, .018, {});
  assert.equal(a.action?.kind, 'attack');
});

test('aiming beyond bow range still hits bodies in the actual firing lane', () => {
  const { b, a, d } = duel('archer', 'levy'); d.z = 18; d.order.z = 18;
  run(b, 1.5, { attack: true, facing: 0, aimX: 0, aimZ: 70 });
  assert.ok(d.hp < d.maxHp);
  assert.ok(a.ammo < 24);
});

test('an empty commander quiver uses a close sidearm instead of an invisible long range hit', () => {
  const { b, a, d } = duel('archer', 'levy'); a.ammo = 0; d.z = 18; d.order.z = 18;
  run(b, 3, { attack: true, facing: 0, aimX: 0, aimZ: 70 });
  assert.equal(d.hp, d.maxHp); assert.equal(b.projectiles.length, 0); assert.equal(a.sidearm, true);
});

test('hold fire conserves arrows and hold position does not pursue distant targets', () => {
  const b = createBattle(encounter({ archer: 3 }, { levy: 2 }));
  issueOrder(b, 'ranged', 'holdFire'); issueOrder(b, 'all', 'hold'); beginBattle(b);
  const original = b.units.filter(u => u.owner === 'ashen').map(u => ({ id: u.id, x: u.x, z: u.z, ammo: u.ammo }));
  run(b, 4);
  for (const p of original) { const u = b.units.find(u => u.id === p.id); assert.equal(u.ammo, p.ammo); assert.ok(Math.hypot(u.x - p.x, u.z - p.z) < 2); }
});

test('deployment stays in the starting area and reports obstructed positions', () => {
  const e = encounter(); e.tile = { terrain: 'plains', building: 'fort', walls: 60, fortIntegrity: 45 };
  const b = createBattle(e), u = b.units[0], before = { x: u.x, z: u.z };
  const blocked = nearestBattlePosition(b, { x: -19, z: 12 }, u);
  assert.equal(blocked.reachable, false); assert.match(blocked.reason, /obstructed/);
  issueOrder(b, 'all', 'move', { x: 75, z: 75, facing: 1 });
  assert.ok(Math.hypot(u.x - before.x, u.z - before.z) <= 27);
});

test('sieges retain exact breached gates and wall sections between encounters', () => {
  const e = encounter(); e.tile = { terrain: 'plains', building: 'fort', walls: 27, fortIntegrity: 20, wallBreaches: ['gate', 'wall-east-front'], wallSections: [{ id: 'wall-west', hp: 37, maxHp: 220 }] };
  const b = createBattle(e);
  assert.equal(b.layout.gate.hp, 0); assert.equal(b.layout.walls.find(w => w.id === 'wall-east-front').hp, 0);
  assert.equal(b.layout.walls.find(w => w.id === 'wall-west').hp, 37);
  assert.equal(b.fortifications.walls, 27); assert.equal(b.fortifications.fortIntegrity, 20);
  const r = battleResult(b); assert.ok(r.wallBreaches.includes('gate')); assert.ok(r.wallBreaches.includes('wall-east-front'));
  const next = createBattle({ ...e, id: 'next', tile: { ...e.tile, walls: r.walls, fortIntegrity: r.fortIntegrity, wallSections: r.wallSections, wallBreaches: r.wallBreaches } });
  assert.deepEqual(next.layout.walls, b.layout.walls);
});

function defenseBattle() {
  const e = encounter({ levy: 3 }, { archer: 2, menAtArms: 1, cavalry: 1 }, { playerOwner: 'vesper', playerSide: 1, tile: { terrain: 'plains', building: 'fort', walls: 60, fortIntegrity: 45 } });
  const b = createBattle(e);
  for (const u of b.units.filter(u => u.side === 0)) { u.explicitOrder = true; u.order = { kind: 'hold', x: u.x, z: u.z, facing: 0 }; }
  return { b, e };
}

test('defenders deploy infantry and archers on intact parapets, with explicit stairs', () => {
  const { b, e } = defenseBattle();
  const result = issueOrder(b, 'all', 'walls');
  assert.equal(result.accepted, true); assert.equal(result.count, 3);
  const defenders = b.units.filter(u => u.side === 1);
  assert.ok(defenders.filter(u => u.family !== 'mounted').every(u => u.wallId && u.y === 6));
  assert.equal(defenders.find(u => u.family === 'mounted').wallId, undefined);
  assert.equal(b.layout.stairs.length, 5);
  assert.equal(new Set(defenders.filter(u => u.wallPosition).map(u => u.wallPosition.slot)).size, 3);
  assert.equal(validateBattleCheckpoint(JSON.parse(JSON.stringify(b)), e), true);
});

test('combat wall orders walk and climb the interior stair instead of teleporting', () => {
  const { b, e } = defenseBattle(); beginBattle(b);
  const archer = b.units.find(u => u.side === 1 && u.type === 'archer');
  const stair = b.layout.stairs.find(s => s.wallId === 'wall-west-front');
  archer.x = stair.x; archer.z = stair.z;
  issueOrder(b, [archer.id], 'walls', { wallId: stair.wallId });
  stepBattle(b, .05, { commanderAI: true });
  assert.equal(archer.action?.kind, 'stairs'); assert.equal(archer.y, 0);
  run(b, 2, { commanderAI: true });
  assert.ok(archer.y > 2 && archer.y < 4); assert.equal(archer.wallId, undefined);
  const saved = JSON.parse(JSON.stringify(b)); assert.equal(validateBattleCheckpoint(saved, e), true);
  run(b, 3, { commanderAI: true }); run(saved, 3, { commanderAI: true });
  assert.equal(archer.wallId, stair.wallId); assert.equal(archer.y, 6); assert.deepEqual(saved, b);
});

test('move orders descend a real stair before leaving the defended wall', () => {
  const { b } = defenseBattle(); issueOrder(b, 'ranged', 'walls'); beginBattle(b);
  const archers = b.units.filter(u => u.side === 1 && u.family === 'ranged');
  issueOrder(b, 'ranged', 'move', { x: 0, z: 36, facing: Math.PI });
  stepBattle(b, .05, { commanderAI: true });
  assert.ok(archers.every(u => u.y === 6));
  run(b, 25, { commanderAI: true });
  assert.ok(archers.every(u => !u.wallId && u.y === 0 && u.z > 19));
});

test('wall archers shoot downhill and cannot be struck by a ground level melee swing', () => {
  const { b } = defenseBattle(); issueOrder(b, 'ranged', 'walls'); beginBattle(b);
  const archer = b.units.find(u => u.id === b.commanderId), attacker = b.units.find(u => u.side === 0);
  attacker.x = archer.x; attacker.z = -8; attacker.order = { kind: 'hold', x: attacker.x, z: attacker.z };
  run(b, 1.5, { attack: true, facing: Math.PI, aimX: attacker.x, aimZ: attacker.z });
  assert.ok(attacker.hp < attacker.maxHp); assert.ok(archer.ammo < 24);
  attacker.x = archer.x; attacker.z = archer.z - 1.5; attacker.facing = 0;
  attacker.action = { kind: 'attack', elapsed: .44, windup: .45, recovery: .7, duration: 1.15, targetId: archer.id, facing: 0, impacted: false };
  const hp = archer.hp; stepBattle(b, .05, {});
  assert.equal(archer.hp, hp);
});

test('destroyed parapets drop occupants and cannot receive new wall defense orders', () => {
  const { b } = defenseBattle(); issueOrder(b, 'ranged', 'walls', { wallId: 'wall-west-front' }); beginBattle(b);
  const archer = b.units.find(u => u.id === b.commanderId), before = archer.hp;
  b.layout.walls.find(w => w.id === archer.wallId).hp = 0;
  stepBattle(b, .05, {});
  assert.equal(archer.wallId, null); assert.equal(archer.y, 0); assert.ok(archer.hp < before);
  assert.equal(issueOrder(b, 'ranged', 'walls', { wallId: 'wall-west-front' }).accepted, false);
});

test('assault infantry can climb to occupied parapets and reach wall defenders', () => {
  const e = encounter({ menAtArms: 2 }, { archer: 1 }, { playerOwner: 'vesper', playerSide: 1, tile: { building: 'fort', walls: 60, fortIntegrity: 45 } });
  const b = createBattle(e); issueOrder(b, 'all', 'walls', { wallId: 'wall-west-front' }); issueOrder(b, 'all', 'holdFire'); beginBattle(b);
  const attackers = b.units.filter(u => u.side === 0);
  attackers.forEach((u, index) => { u.x = -20 - index * 2; u.z = 7; });
  run(b, 15, { commanderAI: true });
  assert.ok(attackers.some(u => u.wallId === 'wall-west-front' && u.y === 6));
  assert.ok(b.events.some(event => event.type === 'ladder'));
});

test('parapet cover reduces incoming arrows without making the defender invulnerable', () => {
  function shot(onWall) {
    const e = encounter({ archer: 1 }, { menAtArms: 1 }, { playerOwner: 'vesper', playerSide: 1, tile: { building: 'fort', walls: 60, fortIntegrity: 45 } });
    const b = createBattle(e); if (onWall) issueOrder(b, 'all', 'walls', { wallId: 'wall-west-front' }); beginBattle(b);
    const a = b.units.find(u => u.side === 0), d = b.units.find(u => u.side === 1);
    if (!onWall) { d.x = -29.8; d.z = -8; }
    a.x = d.x; a.z = d.z - 15; a.explicitOrder = true; a.order = { kind: 'hold', x: a.x, z: a.z };
    a.action = { kind: 'shoot', elapsed: .74, windup: .75, recovery: 1.4, duration: 2.15, targetId: d.id, targetX: d.x, targetZ: d.z, targetY: d.y, facing: 0, impacted: false, ranged: true };
    run(b, .5, {});
    return d.maxHp - d.hp;
  }
  const covered = shot(true), open = shot(false);
  assert.ok(covered > 0); assert.ok(covered < open);
});

test('ram attacks damage the real gate and update navigation on a breach', () => {
  const e = encounter({ levy: 1, ram: 1 }, { levy: 2 }); e.tile = { building: 'fort', walls: 60, fortIntegrity: 45 };
  const b = createBattle(e); beginBattle(b);
  const ram = b.units.find(u => u.type === 'ram'); ram.x = 0; ram.z = 8.7;
  for (let i = 0; i < 4; i++) {
    ram.action = { kind: 'siege', elapsed: 1.69, windup: 1.7, recovery: 2.3, duration: 4, targetId: 'gate', targetX: 0, targetZ: 12, facing: 0, impacted: false, ranged: false };
    stepBattle(b, .05);
  }
  assert.equal(b.layout.gate.hp, 0); assert.equal(b.layout.gate.state, 'breached'); assert.ok(b.layout.navRevision > 0);
  assert.ok(b.fortifications.walls < 60); assert.ok(b.fortifications.fortIntegrity < 45);
});

test('rivers have actual impassable water and passable crossing lanes', () => {
  const e = encounter(); e.tile.terrain = 'river'; const b = createBattle(e);
  assert.equal(nearestBattlePosition(b, { x: 0, z: 0 }, b.units[0]).reachable, false);
  assert.equal(nearestBattlePosition(b, { x: -20, z: 0 }, b.units[0]).reachable, true);
});

test('subsequent strategic damage scales wall sections and repairs reopen the layout budget', () => {
  const e = encounter();
  e.tile = { building: 'fort', walls: 20, fortIntegrity: 15, battleDamageIntegrity: { walls: 40, fortIntegrity: 30 }, wallBreaches: ['gate'], wallSections: [{ id: 'wall-west', hp: 120, maxHp: 220 }] };
  const damaged = createBattle(e);
  assert.ok(Math.abs(damaged.layout.walls.find(w => w.id === 'wall-west').hp - 60) < 1e-8);
  assert.equal(damaged.layout.gate.hp, 0);
  const repaired = createBattle({ ...e, tile: { ...e.tile, walls: 60, fortIntegrity: 45 } });
  assert.equal(repaired.layout.walls.find(w => w.id === 'wall-west').hp, 220);
  assert.ok(repaired.layout.gate.hp > 0);
});

test('empty siege ammunition never becomes a long range melee attack', () => {
  const e = encounter({ levy: 1, catapult: 1 }, { levy: 1 }); const b = createBattle(e); beginBattle(b);
  const engine = b.units.find(u => u.type === 'catapult'), enemy = b.units.find(u => u.side === 1);
  engine.x = 0; engine.z = 0; engine.ammo = 0; engine.explicitOrder = true; engine.order = { kind: 'hold', x: 0, z: 0 };
  enemy.x = 0; enemy.z = 30; enemy.explicitOrder = true; enemy.order = { kind: 'hold', x: 0, z: 30 };
  run(b, 8);
  assert.equal(enemy.hp, enemy.maxHp); assert.equal(engine.sidearm, true);
});

test('allied orders remain with their own commander and return acceptance', () => {
  const e = encounter(); e.participants.push({ armyId: 'ally', owner: 'wintermere', side: 0, units: { archer: 2 }, morale: .8 });
  const b = createBattle(e);
  assert.equal(issueOrder(b, 'ally', 'charge').accepted, false);
  assert.equal(requestAlliedOrder(b, 'ally', 'cover').accepted, true);
  assert.ok(b.units.filter(u => u.armyId === 'ally').every(u => u.order.kind === 'hold'));
});

test('JSON checkpoints resume attack phases, RNG, projectiles and rosters exactly', () => {
  const e = encounter({ archer: 3 }, { levy: 5 }), b = createBattle(e); beginBattle(b);
  run(b, 16, { commanderAI: true });
  const copy = JSON.parse(JSON.stringify(b)); assert.equal(validateBattleCheckpoint(copy, e), true);
  run(b, 8, { commanderAI: true }); run(copy, 8, { commanderAI: true });
  assert.deepEqual(copy, b);
});

test('checkpoint validation rejects duplicate units, altered army membership and restored wall budgets', () => {
  const e = encounter(); e.tile.walls = 60; const base = createBattle(e);
  for (const mutate of [b => b.units.push({ ...b.units[0] }), b => { b.units[0].owner = 'vesper'; }, b => { b.units[0].id = 'forged'; }, b => { b.fortifications.initialWalls = 90; b.fortifications.walls = 90; }, b => { b.units[0].hp = NaN; }, b => { b.fortifications.walls = NaN; }]) {
    const b = JSON.parse(JSON.stringify(base)); mutate(b); assert.equal(validateBattleCheckpoint(b, e), false);
  }
});

test('automatic completion continues recorded losses, ammo, morale and delayed identities', () => {
  const e = encounter({ menAtArms: 5, archer: 3 }, { levy: 7 });
  e.participants.push({ armyId: 'relief', owner: 'vesper', side: 1, units: { levy: 3 }, morale: .7, arrivalSeconds: 45, entryDirection: 'east' });
  const b = createBattle(e); beginBattle(b);
  const dead = b.units[1]; dead.status = 'dead'; dead.hp = 0;
  const archer = b.units.find(u => u.type === 'archer'); archer.ammo = 0;
  const r = finishAutomatically(b);
  assert.equal(r.complete, true); assert.equal(dead.status, 'dead'); assert.equal(archer.ammo, 0);
  assert.ok(r.armies[0].losses.menAtArms >= 1); assert.ok(r.armies[0].morale >= .1 && r.armies[0].morale <= 1);
  for (const a of r.armies) for (const [type, n] of Object.entries(a.starting)) assert.equal(a.units[type] + a.losses[type], n);
  assert.deepEqual(finishAutomatically(b), r);
  assert.equal(validateBattleCheckpoint(b, e), true);
});

test('no defenders are manufactured for an empty settlement', () => {
  const b = createBattle(encounter({ levy: 3 }, {}, { tile: { terrain: 'plains', building: 'town' } }));
  beginBattle(b);
  assert.equal(b.units.length, 3); assert.equal(b.phase, 'complete'); assert.equal(b.winner, 0);
});
