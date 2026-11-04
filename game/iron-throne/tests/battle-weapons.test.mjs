import test from 'node:test';
import assert from 'node:assert/strict';
import {createBattle,beginBattle,stepBattle,issueOrder} from '../battle-sim.mjs';

function arena(redType='menAtArms',blueType='levy',blueCount=1,playerOwner='blue') {
  const b=createBattle({id:'weapons',seed:31,playerOwner,tile:{terrain:'plains'},participants:[
    {armyId:'red',owner:'red',side:0,units:{[redType]:1}},
    {armyId:'blue',owner:'blue',side:1,units:{[blueType]:blueCount}}
  ]});
  beginBattle(b); delete b.layout.climate;
  const red=b.units[0], targets=b.units.filter(u=>u.side===1);
  for(const u of b.units) Object.assign(u,{x:0,z:u.side?2:0,y:0,facing:u.side?Math.PI:0,
    explicitOrder:true,order:{kind:'hold',x:0,z:u.side?2:0,lockFacing:true,facing:u.side?Math.PI:0},attackCooldown:20});
  return {b,red,targets,blue:targets[0]};
}
function release(attacker,target,heavy=false) {
  attacker.action={kind:'attack',elapsed:.49,windup:.5,recovery:.75,duration:1.25,
    facing:attacker.facing,targetId:target.id,ranged:false,impacted:false,heavy};
}
function damageFor(redType,blueType,weapon,distance,options={}) {
  const {b,red,blue}=arena(redType,blueType);
  red.weapon=weapon;
  Object.assign(blue,{z:distance,...options.target});
  Object.assign(red,options.attacker || {});
  const before=blue.hp;
  release(red,blue); stepBattle(b,.05,options.input || {});
  return {damage:before-blue.hp,b,red,blue};
}

test('spearpoint checks an approaching foot soldier, loses leverage at close range, and doubles against a presented horse',()=>{
  const thrust=damageFor('spearman','levy','spear',3.5);
  assert.ok(thrust.damage>0 && thrust.damage<18); assert.ok(thrust.blue.z>3.9);
  assert.equal(damageFor('menAtArms','levy','sword',3.5).damage,0);
  assert.ok(damageFor('spearman','levy','spear',.9).damage<thrust.damage*.55);
  const foot=damageFor('spearman','cavalry','spear',3.5,{target:{mounted:false}});
  const horse=damageFor('spearman','cavalry','spear',3.5,{target:{speed:7,chargeDistance:12}});
  assert.ok(horse.damage>foot.damage*2);
  assert.equal(horse.blue.chargeDistance,0); assert.equal(horse.blue.speed,0);
  assert.ok(horse.b.events.some(e=>e.type==='brace'));
});
test('a formed spearman times a real thrust at a running infantryman before he reaches sword distance',()=>{
  const {b,red,blue}=arena('spearman','levy',1,'red');
  Object.assign(red,{attackCooldown:0,formation:'spearWall',order:{kind:'hold',x:0,z:0}});
  Object.assign(blue,{x:0,z:9,speed:3.5,facing:Math.PI,order:{kind:'charge'},explicitOrder:true});
  let windupDistance=0;
  for(let i=0;i<60 && !blue.spearRecoilUntil;i++) {
    stepBattle(b,.05,{commanderAI:true});
    if (!windupDistance && red.action?.kind==='attack') windupDistance=blue.z-red.z;
  }
  assert.ok(windupDistance>4.3,`began winding up at ${windupDistance}m`);
  assert.ok(blue.spearRecoilUntil>0);
  const first=b.events.find(e=>e.type==='hit');
  assert.equal(first.sourceId,red.id); assert.ok(first.z>1.85);
});

test('thrusts only reach a narrow lane; heavy cuts, axes, and maces have different contact patterns',()=>{
  for(const [weapon,wanted] of [['sword',3],['axe',2],['mace',1],['spear',1]]) {
    const {b,red,targets}=arena('menAtArms','levy',3);
    red.weapon=weapon;
    targets.forEach((u,i)=>Object.assign(u,{x:[0,-.8,.85][i],z:1.75}));
    release(red,targets[0],true); stepBattle(b,.05);
    const connected=b.events.filter(e=>e.type==='hit' && e.sourceId===red.id);
    assert.equal(connected.length,wanted,weapon);
  }
  assert.equal(damageFor('spearman','levy','spear',3,{target:{x:1}}).damage,0);
  assert.ok(damageFor('menAtArms','heavyInfantry','mace',1.8).damage>damageFor('menAtArms','heavyInfantry','sword',1.8).damage*1.7);
  const axe=damageFor('menAtArms','menAtArms','axe',1.8,{input:{block:true}});
  const sword=damageFor('menAtArms','menAtArms','sword',1.8,{input:{block:true}});
  assert.ok(axe.blue.stamina<sword.blue.stamina-10);
});

test('spear wielders cannot instantly reverse body, guard or committed thrust toward a flanker',()=>{
  for(const playerOwner of ['red','blue']) {
    const {b,red,blue}=arena('spearman','menAtArms',1,playerOwner);
    Object.assign(red,{attackCooldown:0,order:{kind:'hold',x:0,z:0}});
    Object.assign(blue,{x:0,z:-3.1,order:{kind:'hold',x:0,z:-3.1}});
    for(let i=0;i<5;i++) stepBattle(b,.05,playerOwner==='red'?{facing:Math.PI,attack:true}:{});
    assert.ok(Math.abs(red.facing)<.55);
    if(playerOwner==='red'){
      assert.ok(red.action.windup>.7 && red.action.recovery>=1);
      assert.ok(red.action.facing<.55);
      for(let i=0;i<14;i++) stepBattle(b,.05,{facing:Math.PI});
      assert.equal(blue.hp,blue.maxHp);
    }else assert.equal(red.action,null);
  }
  const front=damageFor('menAtArms','spearman','sword',1.8).damage;
  for(const facing of [Math.PI/2,0]) {
    assert.ok(damageFor('menAtArms','spearman','sword',1.8,{target:{facing}}).damage>front*1.15);
  }
});

test('lance impact rewards forward momentum; dismounting and a drawn quiver sidearm use sword handling',()=>{
  const idle=damageFor('cavalry','levy','lance',3.8);
  const charge=damageFor('cavalry','levy','lance',3.8,{attacker:{speed:7,chargeDistance:12}});
  assert.ok(idle.damage>0 && charge.damage>idle.damage*2);
  assert.ok(charge.b.events.some(e=>e.type==='charge'));
  const {b,red,blue}=arena('knight','levy',1,'red');
  red.attackCooldown=0; blue.z=3.7; blue.order.z=3.7;
  stepBattle(b,.05,{interact:true}); assert.equal(red.mounted,false); assert.equal(red.sidearm,true);
  stepBattle(b,.05,{weapon:'lance',attack:true});
  assert.equal(red.action.windup,.3); assert.equal(red.action.recovery,.45);
  for(let i=0;i<18;i++) stepBattle(b,.05);
  assert.equal(blue.hp,blue.maxHp);
  stepBattle(b,.05,{interact:true}); assert.equal(red.mounted,true); assert.equal(red.sidearm,false);
  const archer=arena('archer','levy',1,'red'); archer.red.attackCooldown=0;
  stepBattle(archer.b,.05,{sidearm:true,attack:true,facing:0});
  assert.equal(archer.red.action.windup,.3); assert.equal(archer.red.action.recovery,.45);
  for(let i=0;i<7;i++) stepBattle(archer.b,.05);
  assert.ok(archer.blue.hp<archer.blue.maxHp); assert.equal(archer.red.ammo,24);
});

function company(tile={terrain:'plains'}) {
  const b=createBattle({id:'spear-company',seed:37,playerOwner:'red',tile,participants:[
    {armyId:'red',owner:'red',side:0,units:{archer:1,spearman:1,menAtArms:1}},
    {armyId:'blue',owner:'blue',side:1,units:{levy:1}}
  ]});
  const [captain,spear,sword,enemy]=b.units;
  return {b,captain,spear,sword,enemy};
}
test('move, reform and recall start immediately while foot spears turn within one step of their rate',()=>{
  for (const order of ['move','formation','follow']) {
    const {b,captain,spear,sword}=company(); beginBattle(b);
    Object.assign(captain,{x:0,z:-17,facing:0});
    Object.assign(spear,{x:-2,z:0,facing:0}); Object.assign(sword,{x:2,z:0,facing:0});
    release(spear,b.units[3]); release(sword,b.units[3]);
    const result=issueOrder(b,[spear.id,sword.id],order,{x:0,z:-12,facing:Math.PI,formation:'line'});
    assert.ok(result.accepted);
    assert.equal(spear.action,null); assert.equal(spear.facing,0);
    stepBattle(b,.05);
    assert.ok(spear.z<0 && sword.z<0,order);
    assert.ok(Math.abs(spear.facing)>0 && Math.abs(spear.facing)<=.105001,order);
    assert.ok(Math.cos(sword.facing)<-.9,order);
  }
});
test('face and locked hold rotate combat spears over time, leaving deliberate deployment facing immediate',()=>{
  const {b,spear,sword}=company();
  assert.ok(issueOrder(b,[spear.id],'face',{facing:Math.PI}).accepted);
  assert.equal(spear.facing,Math.PI);
  beginBattle(b);
  for (const [order,params] of [['face',{}],['hold',{atCurrent:true}]]) {
    spear.facing=0; sword.facing=0;
    issueOrder(b,[spear.id,sword.id],order,{...params,facing:Math.PI});
    assert.equal(spear.facing,0);
    stepBattle(b,.05);
    assert.ok(spear.facing>0 && spear.facing<=.105001);
    assert.equal(sword.facing,Math.PI);
    for(let i=0;i<36;i++) stepBattle(b,.05);
    assert.ok(Math.abs(Math.PI-spear.facing)<.001);
  }
});
test('parapet walking uses the spear turn limit, including steps with more than one facing request',()=>{
  const {b,spear}=company({terrain:'plains',building:'fort',walls:60,fortIntegrity:45});
  beginBattle(b);
  const wall=b.layout.walls[0];
  Object.assign(spear,{x:wall.x+6,z:wall.z,y:wall.height,wallId:wall.id,facing:Math.PI/2,
    wallPosition:{x:wall.x-6,z:wall.z,wallId:wall.id,facing:Math.PI},order:{kind:'walls'},explicitOrder:true});
  stepBattle(b,.05);
  assert.ok(spear.x<wall.x+6); assert.ok(Math.abs(spear.facing-Math.PI/2)<=.105001);
  // Reaching a nearby move target and applying the formation's final facing
  // can request a second turn before these same 50ms have elapsed.
  spear.wallId=null; Object.assign(spear,{x:0,z:0,y:0,facing:0,speed:0});
  issueOrder(b,[spear.id],'move',{x:0,z:-.45,facing:Math.PI});
  stepBattle(b,.05);
  assert.equal(spear.order.kind,'hold');
  assert.ok(spear.facing>0 && spear.facing<=.105001);
});
