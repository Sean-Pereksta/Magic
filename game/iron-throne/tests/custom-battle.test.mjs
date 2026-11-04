import test from 'node:test';
import assert from 'node:assert/strict';
import { customDeployment, balancedArmy, westWallAssault } from '../custom-battle.mjs';
import { validateBattleCheckpoint } from '../battle-sim.mjs';

test('West Wall Assault starts with both ladders, 72 soldiers, a ram and 48 defenders including 16 archers',()=>{
  const settings=westWallAssault();
  const {battle,encounter}=customDeployment(settings);
  assert.ok(validateBattleCheckpoint(battle,encounter));
  assert.deepEqual(battle.activeCaps,[73,48]);
  assert.equal(battle.units.filter(u=>!u.side&&u.family!=='siege').length,72);
  assert.equal(battle.units.filter(u=>u.type==='ram').length,1);
  assert.equal(battle.units.filter(u=>u.side===1&&u.type==='archer').length,16);
  assert.equal(battle.layout.ladders.length,2);
  assert.deepEqual(battle,customDeployment(settings).battle);
});

test('300 attackers and 200 defenders keep their exact roster at every combined active limit',()=>{
  const settings=westWallAssault();
  settings.armies=[balancedArmy(300,0,0),balancedArmy(200,1,0)];settings.mode='field';settings.playerSide=1;
  for(const limit of [100,150,200]){
    settings.limit=limit;
    const {battle,encounter}=customDeployment(settings);
    assert.equal(battle.playerSide,1);assert.ok(validateBattleCheckpoint(battle,encounter));
    for(const side of [0,1]){
      const units=battle.units.filter(u=>u.side===side),active=side?limit*.4:limit*.6;
      assert.equal(units.filter(u=>u.status==='active').length,active);
      assert.equal(units.filter(u=>u.status==='reserve').length,(side?200:300)-active);
    }
  }
});

test('mixed siege engines remain distinct from soldiers and a human defender starts on the parapet',()=>{
  const settings=westWallAssault();
  settings.mode='defend';settings.playerSide=1;
  Object.assign(settings.armies[0],{catapult:2,trebuchet:1});
  const {battle}=customDeployment(settings);
  assert.deepEqual(battle.units.filter(u=>u.family==='siege').map(u=>u.type),['ram','catapult','catapult','trebuchet']);
  assert.ok(battle.units.filter(u=>u.side===1&&u.type==='archer').every(u=>u.wallId&&u.y===6));
});
