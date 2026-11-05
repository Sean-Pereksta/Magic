import test from 'node:test';
import assert from 'node:assert/strict';
import {createBattle,beginBattle,stepBattle,issueOrder,validateBattleCheckpoint,terrainHeight} from '../battle-sim.mjs';

function arena(commandTroop = false){
  const encounter={id:'horse-movement',seed:23,playerOwner:'ashen',playerSide:0,tile:{terrain:'plains'},
    participants:[{armyId:'a',side:0,owner:'ashen',units:{cavalry:commandTroop?2:1}},{armyId:'d',side:1,owner:'vesper',units:{levy:1}}]};
  const b=createBattle(encounter);beginBattle(b);
  const horse=b.units.find(u=>u.side===0&&(!commandTroop||!u.commander)),enemy=b.units.find(u=>u.side===1);
  Object.assign(horse,{x:0,z:-30,facing:0});
  Object.assign(enemy,{x:64,z:66,explicitOrder:true,order:{kind:'hold',x:64,z:66}});
  return {b,horse,enemy,encounter};
}
function ride(b,steps,input={}){for(let i=0;i<steps;i++)stepBattle(b,.05,input);}

test('sprinting, lateral steering and releasing the reins trace a forward arc and a stopping distance',()=>{
  const {b,horse}=arena();
  ride(b,72,{moveZ:1,sprint:true,facing:0});assert.ok(horse.speed>8.8);
  const {x,z,speed}=horse;
  ride(b,1,{moveX:1,sprint:true,facing:Math.PI});
  assert.ok(horse.z-z>speed*.046&&horse.x-x<.02,'the horse advances instead of strafing sideways');
  assert.ok(horse.facing>0&&horse.facing<.04);assert.ok(horse.horseLean<0);
  ride(b,24,{moveX:1,sprint:true,facing:Math.PI});
  assert.ok(horse.x>x+1&&horse.z>z+3&&horse.facing>.6);
  const stopSpeed=horse.speed,start=horse.rideDistance,heading=horse.facing;
  ride(b,60,{facing:heading+Math.PI});
  assert.equal(horse.speed,0);assert.ok(Math.abs(horse.facing-heading)<1e-8,'camera look does not turn the horse');
  assert.ok(Math.abs((horse.rideDistance-start)-stopSpeed**2/(2*5))<.02);
  assert.equal(horse.y,terrainHeight(b,horse.x,horse.z));
});

test('a full reversal sheds speed before turning back, including through a saved mid-turn',()=>{
  const {b,horse,encounter}=arena();Object.assign(horse,{speed:8,chargeDistance:12});
  ride(b,1,{moveZ:-1,facing:Math.PI});
  assert.ok(horse.z>-30&&horse.speed<8&&horse.facing<.04);
  ride(b,18,{moveZ:-1,sprint:true,facing:Math.PI});
  assert.ok(validateBattleCheckpoint(b,encounter));
  const resumed=JSON.parse(JSON.stringify(b)),apex=horse.z;
  ride(b,100,{moveZ:-1,sprint:true,facing:Math.PI});ride(resumed,100,{moveZ:-1,sprint:true,facing:Math.PI});
  assert.deepEqual(b,resumed);assert.ok(horse.z<apex-4&&Math.cos(horse.facing)<-.99);
  const legacy=structuredClone(b);for(const u of legacy.units){delete u.horseStepAt;delete u.rideDistance;delete u.horseLean;}
  assert.ok(validateBattleCheckpoint(legacy,encounter));ride(legacy,1,{moveZ:-1});assert.ok(legacy.units[0].rideDistance>0);
  legacy.units[0].speed=Infinity;assert.equal(validateBattleCheckpoint(legacy,encounter),false);
});

test('the horse stops at solid cover and the map edge, while dismounting restores foot controls',()=>{
  const {b,horse}=arena();Object.assign(horse,{z:0,speed:9,chargeDistance:16});
  b.layout.obstacles.push({id:'test-wall',kind:'rock',x:0,z:3,width:5,depth:1,height:4});
  ride(b,24,{moveZ:1,sprint:true});assert.ok(horse.z<=2.15&&horse.z>=1.8);
  assert.equal(horse.chargeDistance,0);
  Object.assign(horse,{x:20,z:77,speed:9,facing:0});
  ride(b,20,{moveZ:1,sprint:true});assert.ok(horse.z<=78.5&&horse.z>78);
  Object.assign(horse,{x:0,z:0,speed:3});
  ride(b,1,{interact:true});assert.equal(horse.mounted,false);assert.equal(horse.speed,0);
  ride(b,1,{moveX:1,facing:0});assert.ok(horse.x>0);assert.equal(horse.facing,0);
  ride(b,1,{interact:true});assert.equal(horse.mounted,true);assert.equal(horse.speed,0);
});

test('AI cavalry uses the same steering on a move order and cannot lance an enemy behind it instantly',()=>{
  const {b,horse,enemy}=arena(true);Object.assign(horse,{speed:8,z:0});
  issueOrder(b,[horse.id],'move',{x:17,z:0});
  ride(b,1,{commanderAI:true});assert.ok(horse.z>.35&&horse.x<.025&&horse.facing<.04);
  ride(b,105,{commanderAI:true});assert.ok(horse.x>13&&Math.abs(horse.z)<4);
  Object.assign(horse,{x:0,z:0,speed:0,facing:0,order:{kind:'charge'},explicitOrder:true,action:null});
  Object.assign(enemy,{x:0,z:-2.7,speed:0,order:{kind:'hold',x:0,z:-2.7}});
  ride(b,1,{commanderAI:true});assert.ok(Math.abs(horse.facing)<.10);assert.equal(horse.action,null);
  ride(b,50,{commanderAI:true});
  assert.ok(enemy.hp<enemy.maxHp,'the mounted rider eventually turns and can connect');
});

test('400 active troops can maneuver with momentum in the same field simulation',()=>{
  const roster={cavalry:65,lightCavalry:35,spearman:50,menAtArms:50};
  const e={id:'mounted-field',seed:26,playerOwner:'ashen',playerSide:0,tile:{terrain:'plains'},
    participants:[{armyId:'a',side:0,owner:'ashen',units:roster},{armyId:'d',side:1,owner:'vesper',units:roster}]};
  const b=createBattle(e,{maxActive:400});beginBattle(b);
  for(const u of b.units){u.order={kind:'charge'};u.explicitOrder=true;}
  ride(b,40,{commanderAI:true});
  assert.equal(b.units.filter(u=>u.status==='active').length,400);
  const horses=b.units.filter(u=>u.mounted);assert.equal(horses.length,200);
  assert.ok(horses.every(u=>Number.isFinite(u.rideDistance)&&Number.isFinite(u.facing)&&u.speed<=12));
  assert.ok(horses.some(u=>u.rideDistance>1));assert.ok(validateBattleCheckpoint(b,e));
});
