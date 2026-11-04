import test from 'node:test';
import assert from 'node:assert/strict';
import { createBattle, beginBattle, issueOrder, stepBattle, battleResult, validateBattleCheckpoint, nearestBattlePosition, playerLadderOption } from '../battle-sim.mjs';
import { ramApproach, ramHandle } from '../battle-siege.mjs';

function fort(attack = { menAtArms:12 }, defend = { cavalry:1 }, playerSide = 0) {
  const e = { id:'west-assault', seed:711, playerOwner:playerSide?'white':'red', playerSide,
    tile:{terrain:'plains',building:'fort',walls:60,fortIntegrity:45},
    participants:[{armyId:'attack',owner:'red',side:0,units:attack}, {armyId:'defend',owner:'white',side:1,units:defend}] };
  const b=createBattle(e);
  return { e,b,attackers:b.units.filter(u => u.side===0), defenders:b.units.filter(u => u.side===1) };
}
function run(b,seconds,input={commanderAI:true}) {
  for(let i=0;i<Math.ceil(seconds*20);i++) stepBattle(b,.05,input);
}
test('nearest available infantry crew a player ram automatically and replace losses without overriding recalls',()=>{
  const {b,attackers}=fort({menAtArms:12,ram:1});
  const ram=attackers.find(u=>u.type==='ram'), troops=attackers.filter(u=>u.type==='menAtArms'&&!u.commander);
  beginBattle(b); Object.assign(ram,{x:0,z:-30});
  troops.forEach((u,i)=>Object.assign(u,{x:i+3,z:-30}));
  issueOrder(b,[troops[0].id],'hold',{atCurrent:true});
  stepBattle(b,.05);
  const assigned=()=>troops.filter(u=>u.status==='active'&&u.order.ramId===ram.id);
  assert.deepEqual(assigned().map(u=>u.id),troops.slice(1,7).map(u=>u.id));
  assert.equal(ram.ramTargetId,'gate');assert.equal(ram.crewCount,0);
  assert.equal(new Set(assigned().map(u=>u.order.handle)).size,6);
  const casualty=troops[1], recalled=troops[2];
  casualty.hp=0;casualty.status='dead';
  issueOrder(b,[recalled.id],'follow');
  Object.assign(troops[7],{x:ram.x+3,z:ram.z});Object.assign(troops[8],{x:ram.x+5,z:ram.z});
  run(b,1.1,{});
  assert.equal(recalled.order.kind,'follow');
  assert.equal(troops[0].order.kind,'hold');
  assert.deepEqual(assigned().map(u=>u.id),troops.slice(3,9).map(u=>u.id));
  issueOrder(b,'all','follow');run(b,2,{});
  assert.equal(assigned().length,0);assert.equal(ram.crewCount,0);
});
test('foot troops behind a sealed gate cannot take a handle from a reachable soldier outside',()=>{
  const {b,attackers}=fort({menAtArms:8,ram:1});
  const ram=attackers.find(u=>u.type==='ram'), troops=attackers.filter(u=>u.type==='menAtArms'&&!u.commander);
  beginBattle(b);Object.assign(ram,{x:0,z:4});Object.assign(troops[0],{x:0,z:16});
  for(const [i,u] of troops.slice(1).entries())Object.assign(u,{x:i*2,z:-18});
  stepBattle(b,.05);
  assert.notEqual(troops[0].order.kind,'crew');
  assert.ok(troops.slice(1).every(u=>u.order.ramId===ram.id));
});
test('two rams share infantry by proximity and B can redirect an automatically crewed engine',()=>{
  const {b,attackers}=fort({menAtArms:13,ram:2});
  const rams=attackers.filter(u=>u.type==='ram'), troops=attackers.filter(u=>u.family==='infantry'&&!u.commander);
  beginBattle(b);
  rams.forEach((ram,i)=>Object.assign(ram,{x:i?30:-30,z:-35}));
  troops.forEach((u,i)=>Object.assign(u,{x:rams[i%2].x+(i%3),z:-40-i/3}));
  stepBattle(b,.05);
  assert.ok(troops.every((u,i)=>u.order.ramId===rams[i%2].id));
  for(const ram of rams)assert.equal(new Set(troops.filter(u=>u.order.ramId===ram.id).map(u=>u.order.handle)).size,6);
  const east=rams[1];
  assert.ok(issueOrder(b,[east.id],'breach',{ramId:east.id,wallId:'wall-east-front'}).accepted);
  run(b,1.1,{});
  assert.equal(east.ramTargetId,'wall-east-front');assert.equal(rams[0].ramTargetId,'gate');
});
function unopposed() {
  // The player retains direct control of the archer captain throughout.
  const f=fort({archer:1,menAtArms:12}), {b,defenders}=f;
  f.attackers=f.attackers.filter(u=>u.family==='infantry');
  for (const v of defenders) {v.explicitOrder=true; v.order={kind:'hold',x:0,z:51}; v.x=0; v.z=51;}
  beginBattle(b);
  assert.equal(issueOrder(b,'infantry','ladder',{ladderId:'ladder-0'}).count,12);
  return f;
}
test('twelve infantry clear a single real landing inside sixty seconds and release the ladder order',()=>{
  const {b,attackers}=unopposed();
  let multiple=false, simultaneous=0;
  for(let i=0;i<1200;i++){
    stepBattle(b,.05,{});
    const lane=attackers.filter(u=>u.action?.kind==='climb').map(u=>u.action.progress).sort((a,c)=>a-c);
    simultaneous=Math.max(simultaneous,lane.length);
    if(lane.length>1) multiple=true;
    for(let n=1;n<lane.length;n++) assert.ok(lane[n]-lane[n-1]>=.20);
    if(attackers.every(u=>u.wallId)) break;
  }
  assert.ok(multiple,`peak number of climbers: ${simultaneous}`);
  assert.ok(attackers.every(u=>u.wallId==='wall-west-front' && u.y===6 && u.order.kind!=='ladder'),JSON.stringify(attackers.map(u=>[u.id,u.x,u.z,u.action?.progress,u.order.kind])));
  assert.equal(b.events.filter(e=>e.type==='landed').length,12);
});
test('a player interacting at the foot uses the rungs, mantle and side step onto the real parapet',()=>{
  const {b,attackers}=fort({menAtArms:1});
  beginBattle(b);
  const commander=attackers[0], ladder=b.layout.ladders[0];
  Object.assign(commander,ladder.foot);
  stepBattle(b,.05,{interact:true});
  assert.equal(commander.action?.kind,'climb');
  run(b,2,{});
  assert.ok(commander.y>1 && commander.y<5 && commander.z<11);
  run(b,4,{});
  assert.equal(commander.wallId,ladder.wallId); assert.equal(commander.y,6);
  assert.ok(Math.abs(commander.x-ladder.landing.x)>1);
});
test('E and the contextual button choose the same nearest ladder; a stale tap cannot climb another one',()=>{
  const {b}=fort({menAtArms:2});beginBattle(b);
  const player=b.units.find(u=>u.commander),[west,east]=b.layout.ladders;
  Object.assign(east.foot,{x:west.foot.x+1.7,z:west.foot.z});Object.assign(player,east.foot);
  assert.equal(playerLadderOption(b,player).ladder.id,east.id,'closest reachable foot wins even when listed second');
  const key=structuredClone(b),button=structuredClone(b);
  stepBattle(key,.05,{interact:true});
  stepBattle(button,.02,{climbLadderId:east.id});stepBattle(button,.03,{});
  const a=key.units.find(u=>u.commander).action,c=button.units.find(u=>u.commander).action;
  assert.equal(a.ladderId,east.id);assert.deepEqual(c,a);
  b.layout.walls.find(w=>w.id===east.wallId).hp=0;
  assert.equal(playerLadderOption(b,player).ladder.id,west.id);
  stepBattle(b,.05,{climbLadderId:east.id});assert.equal(player.action,null);
  b.layout.walls.find(w=>w.id===west.wallId).hp=0;
  assert.equal(playerLadderOption(b,player),null);stepBattle(b,.05,{interact:true});assert.equal(player.action,null);
});
test('a player at an occupied foot can board once the leading soldier clears its rungs',()=>{
  const {b}=fort({menAtArms:2});beginBattle(b);
  const player=b.units.find(u=>u.commander),other=b.units.find(u=>u.side===player.side&&u!==player),ladder=b.layout.ladders[0];
  Object.assign(player,ladder.foot);Object.assign(other,ladder.foot);issueOrder(b,[other.id],'ladder',{ladderId:ladder.id});
  stepBattle(b,.05);assert.equal(other.action?.kind,'climb');
  assert.equal(playerLadderOption(b,player).blocked,'Foot occupied');
  stepBattle(b,.05,{climbLadderId:ladder.id});assert.equal(player.action,null);
  run(b,1.6,{});assert.equal(playerLadderOption(b,player).blocked,null);
  stepBattle(b,.05,{climbLadderId:ladder.id});assert.equal(player.action?.ladderId,ladder.id);
});
test('a company larger than the wall walk keeps landing as the forward ranks take the stairs',()=>{
  const {b,attackers,defenders}=fort({archer:1,menAtArms:28},{cavalry:1});
  const reserve=defenders[0];
  Object.assign(reserve,{x:0,z:53,explicitOrder:true,order:{kind:'hold',x:0,z:53}});
  beginBattle(b);
  const soldiers=attackers.filter(u=>u.family==='infantry');
  issueOrder(b,'infantry','ladder',{ladderId:'ladder-0'});
  const landed=new Set();
  for(let i=0;i<2400 && landed.size<soldiers.length;i++){
    stepBattle(b,.05,{});
    for(const u of soldiers) if(u.wallId) landed.add(u.id);
    // Keep a legal defender in the keep so taking the field does not end the exercise early.
    reserve.hp=reserve.maxHp;
  }
  assert.equal(landed.size,soldiers.length);
});
test('a living defender holds the crest, and his death immediately opens it to the queue',()=>{
  const {b,attackers,defenders}=fort({menAtArms:4},{spearman:1,cavalry:1});
  const l=b.layout.ladders[0], guard=defenders.find(u=>u.type==='spearman');
  beginBattle(b);
  Object.assign(guard,{wallId:l.wallId,facing:Math.PI,x:l.x,z:l.z,y:6,
    order:{kind:'hold',wallId:l.wallId,x:l.x,z:l.z,facing:Math.PI},explicitOrder:true});
  issueOrder(b,'infantry','ladder',{ladderId:l.id});
  for(let i=0;i<900 && !l.held;i++) stepBattle(b,.05,{commanderAI:true});
  assert.equal(l.held,true);
  assert.ok(b.events.some(e=>e.type==='ladderHeld' && e.message==='West ladder held by defenders'));
  assert.equal(attackers.some(u=>u.wallId),false);
  guard.status='dead'; guard.hp=0; guard.action=null;
  run(b,5);
  assert.equal(l.held,false); assert.ok(attackers.some(u=>u.wallId===l.wallId));
});
test('death, recall, reload and a destroyed support release the lane from live unit state',()=>{
  const {b,e,attackers}=unopposed();
  let lane;
  for(let i=0;i<700;i++) {
    stepBattle(b,.05,{commanderAI:true});
    lane=attackers.filter(u=>u.action?.kind==='climb');
    if(lane.length>=3) break;
  }
  assert.ok(lane.length>=3);
  const casualty=lane.at(-1); casualty.hp=0; casualty.status='dead'; casualty.action=null;
  const saved=JSON.parse(JSON.stringify(b));
  assert.ok(validateBattleCheckpoint(saved,e));
  for(const sim of [b,saved]){
    issueOrder(sim,'infantry','follow'); run(sim,9);
    assert.equal(sim.units.filter(u=>u.status==='active' && (u.action?.kind==='climb' || u.order.kind==='ladder')).length,0);
  }
  assert.deepEqual(b,saved);
  const second=unopposed();
  for(let i=0;i<700 && !second.attackers.some(u=>u.action?.kind==='climb');i++) stepBattle(second.b,.05,{commanderAI:true});
  second.b.layout.walls[0].hp=0;
  stepBattle(second.b,.05,{commanderAI:true});
  assert.equal(second.attackers.filter(u=>u.action?.ladderId==='ladder-0' || u.order.ladderId==='ladder-0').length,0);
});
function ramFixture(count=5,targetId='wall-east-front') {
  const f=fort({menAtArms:count,ram:1});
  const {b,attackers}=f, ram=attackers.find(u=>u.type==='ram'), crew=attackers.filter(u=>u.family==='infantry' && !u.commander);
  const wall=[...b.layout.walls,b.layout.gate].find(w=>w.id===targetId);
  beginBattle(b);
  Object.assign(ram,ramApproach(wall));
  assert.equal(issueOrder(b,crew.map(u=>u.id),'crew',{ramId:ram.id,wallId:wall.id}).count,crew.length);
  for(const u of crew) Object.assign(u,ramHandle(ram,u.order.handle));
  return {...f,ram,crew,wall};
}
test('the fourth carrier falling before contact cancels the blow and drops the timber in place',()=>{
  const {b,ram,crew,wall}=ramFixture();
  run(b,.1);
  assert.equal(ram.crewCount,4); assert.equal(ram.action.kind,'ramStrike');
  ram.action.elapsed=ram.action.windup-.01;
  crew[0].hp=0; crew[0].status='dead';
  const before={x:ram.x,z:ram.z,hp:wall.hp};
  run(b,3);
  assert.equal(ram.crewCount,3); assert.equal(ram.action,null);
  assert.deepEqual({x:ram.x,z:ram.z,hp:wall.hp},before);
});
test('a full crew breaches the chosen surface and its opening survives the campaign round trip',()=>{
  const {b,e,ram,crew,wall}=ramFixture(7);
  run(b,30);
  assert.equal(wall.hp,0);
  assert.ok(b.events.some(e=>e.type==='ramImpact' && e.wallId===wall.id));
  assert.equal(b.layout.gate.hp,b.layout.gate.maxHp);
  assert.equal(nearestBattlePosition(b,wall).reachable,true);
  assert.equal(nearestBattlePosition(b,{x:wall.x+7,z:wall.z}).reachable,false);
  const result=battleResult(b), next=createBattle({...e,tile:{...e.tile,walls:result.walls,fortIntegrity:result.fortIntegrity,wallSections:result.wallSections,wallBreaches:result.wallBreaches}});
  assert.deepEqual(next.layout.walls.find(w=>w.id===wall.id).breach,wall.breach);
  assert.equal(ram.crewCount,0); assert.ok(crew.every(u=>u.order.kind!=='crew'));
});
test('a crew must reach the beam at its deployment point before hauling it to front, flank or gate',()=>{
  for(const targetId of ['wall-east-front','wall-east','gate']) {
    const {b,attackers}=fort({menAtArms:7,ram:1});
    const ram=attackers.find(u=>u.type==='ram'), start={x:ram.x,z:ram.z};
    const target=[...b.layout.walls,b.layout.gate].find(w=>w.id===targetId);
    beginBattle(b);
    issueOrder(b,'infantry','crew',{ramId:ram.id,wallId:targetId});
    run(b,.05);
    assert.equal(ram.crewCount,0); assert.deepEqual({x:ram.x,z:ram.z},start);
    for(let i=0;i<3600 && target.hp>0;i++) stepBattle(b,.05,{commanderAI:true});
    assert.equal(target.hp,0,`${targetId}: ram at ${ram.x},${ram.z} with ${ram.crewCount} crew`);
    assert.ok(b.events.some(e=>e.type==='breach' && e.wallId===targetId));
  }
});
test('AI garrisons real firing positions and archers cannot shoot through a closed wall',()=>{
  const {b,attackers,defenders}=fort({menAtArms:3},{archer:4,crossbow:2,menAtArms:3,spearman:2});
  beginBattle(b);
  const bows=defenders.filter(u=>u.family==='ranged');
  assert.ok(bows.every(u=>u.wallId && u.y===6));
  const victim=attackers[0], archer=bows[0];
  Object.assign(victim,{x:archer.x,z:-9,order:{kind:'hold',x:archer.x,z:-9},explicitOrder:true});
  run(b,1.6);
  assert.ok(victim.hp<victim.maxHp);
  const hidden=attackers[1]; Object.assign(hidden,{x:archer.x,z:10.5,y:0,order:{kind:'hold',x:archer.x,z:10.5},explicitOrder:true});
  run(b,.05); assert.equal(archer.sidearm,false);
  b.projectiles.push({id:'cover-check',sourceId:archer.id,side:1,kind:'arrow',x:archer.x,z:14,y:1,startX:archer.x,startZ:14,startY:1,targetX:hidden.x,targetZ:10.5,targetY:1,elapsed:0,duration:.05,damage:100});
  const hp=hidden.hp; stepBattle(b,.05);
  assert.equal(hidden.hp,hp); assert.ok(b.events.some(e=>e.type==='missileBlocked'));
});
test('enemy assault commits physical carriers to the east wall and separate infantry to the west ladder',()=>{
  const {b,attackers}=fort({menAtArms:18,ram:1},{archer:4,menAtArms:3},1);
  beginBattle(b); run(b,.1);
  const crew=attackers.filter(u=>u.order.kind==='crew');
  assert.equal(crew.length,6);
  assert.equal(new Set(crew.map(u=>u.order.handle)).size,6);
  assert.equal(attackers.find(u=>u.type==='ram').ramTargetId,'wall-east-front');
  assert.ok(attackers.some(u=>u.order.ladderId==='ladder-0'));
});
test('an intact ladder gives surviving infantry a legal occupation route without commander input',()=>{
  const {b,attackers,defenders}=unopposed();
  for(const v of defenders) {v.hp=0; v.status='dead';}
  run(b,.1,{});
  assert.equal(b.phase,'complete'); assert.equal(battleResult(b).occupationSide,0);
  assert.ok(b.units.some(u=>u.id===b.securedBy && u.side===0));
  const lone=fort({ram:1},{}); beginBattle(lone.b);
  assert.equal(battleResult(lone.b).occupationSide,null);
});
test('foot reserves can secure the keep when the active cap is full of cavalry',()=>{
  const {b,attackers,defenders}=fort({cavalry:4,menAtArms:1},{cavalry:1});
  const reserve=attackers.find(u=>u.family==='infantry');
  reserve.status='reserve'; reserve.wasDeployed=false;
  const army=b.armies.find(a=>a.side===0);
  b.maxActive=5; b.activeCaps=[4,1]; b.commanderId=attackers[0].id;
  for(const u of attackers) u.commander=u.id===b.commanderId;
  for(const u of defenders){u.status='dead';u.hp=0;}
  beginBattle(b); run(b,.05,{});
  assert.equal(army.arrived,true); assert.equal(b.phase,'complete');
  assert.equal(b.securedBy,reserve.id); assert.equal(battleResult(b).occupationSide,0);
  assert.equal(b.units.filter(u=>u.status==='active').length,4);
});
