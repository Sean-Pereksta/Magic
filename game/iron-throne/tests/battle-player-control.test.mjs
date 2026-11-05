import test from 'node:test';
import assert from 'node:assert/strict';
import { createBattle, beginBattle, issueOrder, stepBattle, normalizeBattleCheckpoint, playerStairOption, terrainHeight } from '../battle-sim.mjs';

function arena(fortified = false) {
  const b = createBattle({ id: 'personal-control', seed: 711, playerOwner: 'red', playerSide: 0,
    tile: fortified ? { terrain: 'plains', building: 'fort', walls: 60, fortIntegrity: 45 } : { terrain: 'plains' },
    participants: [
      { armyId: 'home', owner: 'red', side: 0, units: { menAtArms: 3, archer: 2, ram: 1 } },
      { armyId: 'away', owner: 'white', side: 1, units: { cavalry: 1 } }
    ] });
  beginBattle(b);
  const player = b.units.find(u => u.id === b.commanderId);
  for (const u of b.units) {
    u.explicitOrder = true;
    u.order = { kind: 'hold', x: u.x, z: u.z };
  }
  return { b, player };
}
function run(b, seconds, input = {}) {
  for (let i = 0; i < seconds * 20; i++) stepBattle(b, .05, input);
}

test('every troop order excludes the player through all, army, type and saved-group selections', () => {
  for (const selection of ['all', 'home', 'infantry', ['home:menAtArms:0'], { ids: ['home:menAtArms:0', 'home:menAtArms:1'] }]) {
    for (const order of ['hold', 'follow', 'charge', 'advance', 'move', 'face', 'formation', 'fire', 'holdFire', 'retreat', 'withdraw', 'walls', 'ladder', 'crew', 'breach', 'attack', 'nearest']) {
      const { b, player } = arena(true);
      const before = structuredClone(player);
      const ram = b.units.find(u => u.type === 'ram');
      issueOrder(b, selection, order, { x: 0, z: -35, facing: 1, formation: 'line', atCurrent: order === 'hold', wallId: 'wall-west-front', ladderId: 'ladder-0', ramId: ram.id });
      assert.deepEqual(player, before, `${order} must leave the player untouched for ${JSON.stringify(selection)}`);
    }
  }
});

test('crowds and a legacy wall assignment cannot pull the player away from their input direction', () => {
  const { b, player } = arena();
  Object.assign(player, { x: 0, z: -25, speed: 0, order: { kind: 'walls' }, wallPosition: { wallId: 'stale', x: 20, z: 20 } });
  const peers = b.units.filter(u => u.side === 0 && u.id !== player.id && u.family === 'infantry');
  for (const u of peers) Object.assign(u, { x: player.x, z: player.z, order: { kind: 'hold', x: player.x, z: player.z } });
  run(b, 1);
  assert.deepEqual([player.x, player.z], [0, -25]);
  assert.ok(peers.every(u => Math.hypot(u.x - player.x, u.z - player.z) > .8));
  run(b, .5, { moveX: 1, facing: 0 });
  assert.ok(player.x > .5);
  assert.equal(player.z, -25);
});

test('old command-driven retreats are cleared once while preserving a saved stair traversal and wounds', () => {
  const { b, player } = arena(true);
  b.battleRulesVersion = 4;
  player.routing = true;
  player.order = { kind: 'retreat', orderedRetreat: true };
  player.hp = 42;
  player.action = { kind: 'stairs', elapsed: 2 };
  const position = [player.x, player.y, player.z];
  normalizeBattleCheckpoint(b);
  assert.equal(player.routing, false);
  assert.equal(player.order.kind, 'hold');
  assert.equal(player.hp, 42);
  assert.deepEqual(player.action, { kind: 'stairs', elapsed: 2 });
  assert.deepEqual([player.x, player.y, player.z], position);
  const once = structuredClone(b);
  normalizeBattleCheckpoint(b);
  assert.deepEqual(b, once);
});

test('stairs require a fresh E press in each direction and wall movement never steers toward the stairs', () => {
  const { b, player } = arena(true);
  const stair = b.layout.stairs[0];
  Object.assign(player, { x: stair.x, z: stair.z, y: terrainHeight(b, stair.x, stair.z) });
  run(b, .2, { moveX: 1 });
  assert.equal(player.action, null);
  assert.equal(player.wallId, undefined);
  assert.equal(playerStairOption(b, player).stair.id, stair.id);
  stepBattle(b, .05, { interact: true });
  assert.equal(player.action.kind, 'stairs');
  run(b, 4.5, { interact: true });
  assert.equal(player.action, null);
  assert.equal(player.wallId, stair.wallId);
  const top = [player.x, player.z];
  run(b, .2, { moveZ: -1 });
  assert.equal(player.wallId, stair.wallId);
  assert.equal(player.action, null);
  assert.equal(player.x, top[0], 'walking toward a parapet edge must not redirect sideways');
  Object.assign(player, { x: stair.topX, z: stair.topZ });
  assert.equal(playerStairOption(b, player).ascending, false);
  stepBattle(b, .05, { interact: true });
  assert.equal(player.action.kind, 'stairs');
  assert.equal(player.action.ascending, false);
  run(b, 4.5, { interact: true });
  assert.equal(player.wallId, null);
  assert.equal(player.action, null);
});

test('the stair button buffers a short tap and rejects stale or destroyed destinations', () => {
  const { b, player } = arena(true), stair = b.layout.stairs[0];
  Object.assign(player, { x: stair.x, z: stair.z, y: terrainHeight(b, stair.x, stair.z) });
  const byKey = structuredClone(b), byButton = structuredClone(b);
  stepBattle(byKey, .05, { interact: true });
  stepBattle(byButton, .02, { stairId: stair.id });
  stepBattle(byButton, .03);
  assert.deepEqual(byButton.units.find(u => u.commander).action, byKey.units.find(u => u.commander).action);
  stepBattle(b, .05, { stairId: b.layout.stairs[1].id });
  assert.equal(player.action, null);
  b.layout.walls.find(w => w.id === stair.wallId).hp = 0;
  assert.equal(playerStairOption(b, player), null);
  stepBattle(b, .05, { stairId: stair.id });
  assert.equal(player.action, null);
});

test('walking into a ladder does not begin an unrequested climb', () => {
  const { b, player } = arena(true), ladder = b.layout.ladders[0];
  Object.assign(player, ladder.foot);
  run(b, 2, { moveZ: 1 });
  assert.equal(player.action, null);
  assert.equal(player.wallId, undefined);
});
