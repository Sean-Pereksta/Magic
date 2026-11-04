import test from 'node:test';
import assert from 'node:assert/strict';
import { createBattle,beginBattle,stepBattle } from '../battle-sim.mjs';

function duel(count=1){
  const b=createBattle({id:'guard',playerOwner:'blue',tile:{terrain:'plains'},participants:[
    {armyId:'red',owner:'red',side:0,units:{menAtArms:count}},
    {armyId:'blue',owner:'blue',side:1,units:{menAtArms:1}}]});
  beginBattle(b);delete b.layout.climate;
  const guard=b.units.find(u=>u.commander),enemies=b.units.filter(u=>u.side===0);
  for(const [i,u] of enemies.entries())Object.assign(u,{x:i*.5,z:-1.8,y:0,facing:0,explicitOrder:true,order:{kind:'hold',x:i*.5,z:-1.8}});
  Object.assign(guard,{x:0,z:0,y:0,facing:Math.PI,explicitOrder:true,order:{kind:'hold',x:0,z:0}});
  return {b,guard,enemies};
}
function strike(u,target) {
  u.action={kind:'attack',elapsed:.29,windup:.3,recovery:.45,duration:.75,targetId:target.id,targetX:target.x,targetZ:target.z,
    targetY:target.y,ranged:false,heavy:false,facing:u.facing,impacted:false};
}
test('a guard raised on the impact step interrupts one frontal swing even if attackers tick first',()=>{
  const {b,guard,enemies}=duel(2);enemies.forEach(u=>strike(u,guard));
  stepBattle(b,.05,{block:true,attack:true,facing:Math.PI});
  assert.equal(guard.blocking,true);assert.equal(guard.action,null,'guard takes priority over simultaneous attack');
  assert.equal(b.events.filter(e=>e.type==='block').length,2);
  assert.equal(b.events.filter(e=>e.type==='parry').length,1);
  const stunned=enemies[0];assert.equal(stunned.action,null);assert.equal(stunned.stunnedUntil,b.time+.7);
  assert.equal(enemies[1].stunnedUntil,undefined);
  for(let i=0;i<12;i++){stepBattle(b,.05,{block:true,facing:Math.PI});assert.equal(stunned.action,null);assert.equal(stunned.blocking,false);}
  for(let i=0;i<30;i++)stepBattle(b,.05,{block:true,facing:Math.PI});
  assert.ok(stunned.stunnedUntil<b.time);assert.equal(b.events.filter(e=>e.type==='parry').length,1,'holding the button never rearms the stun');
  stepBattle(b,.05,{block:false,facing:Math.PI});strike(stunned,guard);
  stepBattle(b,.05,{block:true,facing:Math.PI});assert.equal(b.events.filter(e=>e.type==='parry').length,2);
});
test('a long held guard still blocks, a rear strike bypasses it, and deflecting a projectile cannot stun its shooter',()=>{
  const {b,guard,enemies:[enemy]}=duel();enemy.attackCooldown=9;
  for(let i=0;i<9;i++)stepBattle(b,.05,{block:true,facing:Math.PI});
  strike(enemy,guard);stepBattle(b,.05,{block:true,facing:Math.PI});
  assert.ok(b.events.some(e=>e.type==='block'));assert.equal(guard.lastParryAt,undefined);
  stepBattle(b,.05,{block:false});strike(enemy,guard);
  stepBattle(b,.05,{block:true,facing:0});assert.equal(b.events.filter(e=>e.type==='block').length,1);
  stepBattle(b,.05,{block:false,facing:Math.PI});
  b.projectiles.push({id:'test-arrow',kind:'arrow',sourceId:enemy.id,startX:0,startY:1.2,startZ:-1,x:0,y:1.2,z:-1,
    targetX:0,targetY:1.2,targetZ:0,elapsed:0,duration:.05,damage:10});
  stepBattle(b,.05,{block:true,facing:Math.PI});
  assert.equal(b.events.filter(e=>e.type==='block').length,2);assert.equal(enemy.stunnedUntil,undefined);
});
test('a short touch press between simulation steps protects exactly one step',()=>{
  const {b,guard,enemies:[enemy]}=duel();strike(enemy,guard);
  stepBattle(b,.02,{blockPressed:true});stepBattle(b,.03,{block:false,facing:Math.PI});
  assert.ok(guard.parrySpent);assert.equal(enemy.action,null);
  stepBattle(b,.05,{block:false});assert.equal(guard.blocking,false);
});
