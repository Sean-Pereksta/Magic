import test from 'node:test';
import assert from 'node:assert/strict';
import { beginBattle, createBattle, issueOrder, stepBattle } from '../battle-sim.mjs';

function engaged(type = 'menAtArms') {
  const battle = createBattle({id:'recall', playerOwner:'north', seed:19, tile:{terrain:'plains'}, participants:[
    {armyId:'blue', owner:'north', side:0, units:{spearman:1,[type]:3}},
    {armyId:'red', owner:'south', side:1, units:{menAtArms:1}}
  ]});
  beginBattle(battle);
  const commander = battle.units.find(u => u.commander);
  const soldiers = battle.units.filter(u => u.owner === 'north' && !u.commander);
  const enemy = battle.units.find(u => u.owner === 'south');
  Object.assign(commander, {x:0,z:-24,facing:0});
  for (const [i,u] of soldiers.entries()) Object.assign(u, {x:i*5,z:1.8,facing:Math.PI});
  Object.assign(enemy,{x:0,z:0, explicitOrder:true, order:{kind:'hold',x:0,z:0}});
  return {battle,commander,soldiers,enemy};
}
function windup(u,enemy) {
  u.targetId = enemy.id;
  u.action = {kind:u.family==='ranged'?'shoot':'attack',targetId:enemy.id,elapsed:.29,windup:.3,duration:.9,recovery:.6,impacted:false,facing:Math.PI,targetX:enemy.x,targetY:enemy.y,targetZ:enemy.z,ranged:u.family==='ranged'};
}
for (const type of ['menAtArms','archer']) test(`${type} disengages on the first 50 ms step without completing a strike or loose shot`, () => {
  const {battle,soldiers,enemy} = engaged(type), u = soldiers[0], startZ = u.z, ammo = u.ammo, enemyHp = enemy.hp;
  windup(u,enemy);
  issueOrder(battle, soldiers.map(v=>v.id), 'follow');
  stepBattle(battle,.05);
  assert.ok(u.z < startZ);
  assert.equal(u.action,null);assert.equal(u.targetId,null);
  assert.ok(Math.cos(u.facing)<0);
  for(let i=0;i<30;i++)stepBattle(battle,.05);
  assert.equal(enemy.hp,enemyHp);assert.equal(u.ammo,ammo);assert.equal(battle.projectiles.length,0);
  assert.equal(new Set(soldiers.map(v=>v.order.followSlot)).size,3);
});
test('changing direction recalls a reformed follower before its next impact; separated companies keep exclusive slots', () => {
  const {battle,commander,soldiers,enemy} = engaged(), [a,b] = soldiers;
  issueOrder(battle,[a.id],'follow');issueOrder(battle,[b.id],'follow');
  assert.notEqual(a.order.followSlot,b.order.followSlot);
  commander.x=0;commander.z=0;a.x=0;a.z=-2.8;enemy.x=0;enemy.z=-4.7;
  windup(a,enemy);
  commander.facing=Math.PI/2;
  const hp=enemy.hp;stepBattle(battle,.05,{facing:Math.PI/2});
  assert.equal(enemy.hp,hp);assert.equal(a.action,null);assert.ok(a.x<0);
});
test('a fired arrow continues on recall and an ordered move ignores enemies until arrival', () => {
  const {battle,soldiers,enemy}=engaged('archer'),u=soldiers[0];
  enemy.x=12;enemy.z=u.z;enemy.order={kind:'hold',x:12,z:u.z};
  windup(u,enemy);u.facing=Math.PI/2;stepBattle(battle,.05);
  const arrow=battle.projectiles.find(p=>p.ownerId===u.id||p.attackerId===u.id||p.sourceId===u.id)||battle.projectiles[0];
  assert.ok(arrow,'the shot has been released');const t=arrow.elapsed;
  issueOrder(battle,[u.id],'follow');stepBattle(battle,.05);
  assert.ok(arrow.elapsed>t);
  issueOrder(battle,[u.id],'move',{x:-14,z:-15});stepBattle(battle,.05);
  assert.equal(u.order.kind,'move');assert.equal(u.action,null);assert.equal(u.targetId,null);
});
