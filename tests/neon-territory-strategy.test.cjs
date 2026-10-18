'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const C=require('../game/neon-arena/neon-conquest.js'),AI=require('../game/neon-arena/neon-tactical-ai.js');
const map=()=>({sectors:[{id:'A',x:-20,z:0,links:['cat_hq','B','C']},{id:'B',x:-10,z:-10,links:['A','D']},{id:'C',x:0,z:10,links:['A','D']},{id:'D',x:10,z:0,links:['B','C','E']},{id:'E',x:20,z:0,links:['D','mouse_hq']}],hq:{1:{x:-30,y:1.86,z:0},2:{x:30,y:1.86,z:0}}});
const actor=(id,team=1,x=-30,z=0)=>({entityId:id,team,x,z,y:1.86,yaw:0,faceYaw:0,hp:100,maxHp:100,r:.5,style:'rifler',vx:0,vz:0,onGround:true});
const troops=(team=1,n=8)=>Array.from({length:n},(_,i)=>actor('soldier-'+team+'-'+i,team,team===1?-30:30,i*.5));
const assignments=(s,list)=>list.map((a,i)=>C.assignment(s,a,i));
const attackers=orders=>orders.filter(o=>['capture','counterattack'].includes(o.intent));

test('both factions concentrate a majority on one reachable frontier and retain a small defensive group',()=>{
  for(const team of [1,2]){
    const s=C.create(map()),list=troops(team);s.sectors[team===1?0:4].owner=team;C.step(s,.1,list);
    const orders=assignments(s,list),attack=attackers(orders);
    assert.equal(attack.length,6);assert.equal(new Set(attack.map(o=>o.sector)).size,1);
    assert.ok(attack.every(o=>C.canCapture(s,s.sectors.find(f=>f.id===o.sector),team)));
    assert.equal(orders.filter(o=>o.role==='defend').length,1);assert.equal(orders.filter(o=>o.role==='overwatch').length,1);
    assert.ok(attack.some(o=>o.role==='flank'));assert.ok(orders.every(o=>o.strategic));
  }
});

test('a committed assault survives roster order changes and short-lived competing frontier incentives',()=>{
  const s=C.create(map()),list=troops();s.sectors[0].owner=1;C.step(s,.1,list);
  const before=new Map(list.map((a,i)=>[a.entityId,C.assignment(s,a,i)])),primary=attackers([...before.values()])[0].sector;
  const other=s.sectors.find(f=>f.owner!==1&&f.id!==primary&&C.canCapture(s,f,1));other.owner=2;
  for(let i=0;i<8;i++)C.step(s,1,[...list].reverse());
  for(const [index,a] of [...list].reverse().entries()){
    const order=C.assignment(s,a,index),old=before.get(a.entityId);
    assert.equal(order.sector,old.sector);assert.equal(order.role,old.role);assert.equal(order.slot,old.slot);
  }
  assert.equal(s.strategy.teams[1].primary,primary);
});

test('an endangered supply flag draws reserves and a lost flag triggers a coordinated replacement counterattack',()=>{
  const s=C.create(map()),list=troops();s.sectors[0].owner=s.sectors[1].owner=1;C.step(s,.1,list);
  s.sectors[0].capturing=2;s.sectors[0].progress=.6;C.step(s,.1,list);
  const response=assignments(s,list).filter(o=>o.intent==='reinforce');
  assert.ok(response.length>=4);assert.ok(response.every(o=>o.sector==='A'));
  assert.ok(attackers(assignments(s,list)).length>=1,'keep some pressure on the forward objective');
  s.sectors[0].owner=0;s.sectors[0].capturing=0;s.sectors[0].progress=0;C.step(s,.1,list);
  const counter=assignments(s,list);assert.ok(counter.every(o=>o.sector==='A'&&o.intent==='counterattack'));
  const replacement=C.assignment(s,actor('replacement'),8);
  assert.equal(replacement.sector,'A');assert.equal(replacement.intent,'counterattack');assert.equal(replacement.role,'reinforce');
  s.sectors[0].owner=1;C.step(s,.1,list);
  assert.ok(attackers(assignments(s,list)).every(o=>o.sector!=='A'),'retaking the flag releases the assault to advance again');
});

test('winning the assigned flag advances the assault and cut-off routes are abandoned immediately',()=>{
  const s=C.create(map()),list=troops();s.sectors[0].owner=1;C.step(s,.1,list);
  const first=attackers(assignments(s,list))[0].sector;s.sectors.find(f=>f.id===first).owner=1;C.step(s,.1,list);
  assert.ok(attackers(assignments(s,list)).every(o=>o.sector!==first));
  s.sectors[0].owner=2;C.step(s,.1,list);
  assert.ok(attackers(assignments(s,list)).every(o=>o.sector==='A'),'reconnect the HQ instead of ordering an invalid deep capture');
});

test('a lone remaining soldier protects an endangered supply flag before seeking another one',()=>{
  const s=C.create(map()),list=troops(1,1);s.sectors[0].owner=1;C.step(s,.1,list);
  s.sectors[0].capturing=2;s.sectors[0].progress=.6;C.step(s,.1,list);
  const order=C.assignment(s,list[0]);assert.equal(order.sector,'A');assert.equal(order.intent,'reinforce');
});

test('complete territorial control distributes garrisons across captured ground',()=>{
  const s=C.create(map()),list=troops(1,6);for(const sector of s.sectors)sector.owner=1;C.step(s,.1,list);
  const orders=assignments(s,list);assert.equal(new Set(orders.map(o=>o.sector)).size,5);assert.ok(orders.every(o=>o.intent==='hold'));
});

test('hidden opponents moving outside flag areas cannot influence friendly strategic orders',()=>{
  const compare=(x,z)=>{const s=C.create(map()),list=troops();s.sectors[0].owner=1;C.step(s,.1,[...list,actor('hidden',2,x,z)]);return assignments(s,list);};
  assert.deepEqual(compare(100,100),compare(-100,-100));
});

function tactical(order,options={}){
  let time=0;const bot=actor('leader',1,0,0),enemy=actor('enemy',2,0,35),actors=[bot];
  if(options.enemy)actors.push(enemy);
  const host={time:()=>time,random:()=>.5,actors:()=>actors,sameTeam:(a,b)=>a.team===b.team,lineClear:()=>true,canTraverse:()=>true,pointInfo:()=>({walkable:true}),navigation:()=>[],objective:()=>order,...options.host};
  const controller=AI.createController(host);
  return {bot,enemy,controller,tick(dt=.35){time+=dt;return controller.update(bot,dt);}};
}

test('a healthy squad advances its objective while firing at a distant visible distraction',()=>{
  const f=tactical({x:30,y:1.86,z:0,role:'attack',radius:5,strategic:true,squad:'one',slot:0},{enemy:true});
  const first=f.tick();assert.equal(first.enemy,f.enemy);assert.ok(first.goal.x>25);assert.ok(Math.abs(first.goal.z)<5);
  const next=f.tick(.5);assert.equal(next.enemy,f.enemy);assert.ok(next.aim);assert.ok(next.move.x>.9);assert.equal(next.tactic,'push');
});

test('soldiers inside a clear capture circle hold position through long engagements instead of circling away',()=>{
  const f=tactical({x:0,y:1.86,z:0,role:'attack',radius:5,strategic:true,squad:'one',slot:0},{enemy:true});
  for(let i=0;i<40;i++){const result=f.tick();assert.equal(result.enemy,f.enemy);assert.equal(result.move.x,0);assert.equal(result.move.z,0);assert.equal(result.goal.x,0);assert.equal(result.goal.z,0);}
});

test('far-off sound memories do not pull a strategic squad away or reveal a moving hidden enemy',()=>{
  const f=tactical({x:30,y:1.86,z:0,role:'attack',radius:5,strategic:true,squad:'one',slot:0},{enemy:true,host:{lineClear:()=>false}});
  f.controller.hear({x:0,y:1.86,z:20,kind:'gunfire',team:2});
  let result=f.tick(.1);assert.ok(result.memory);const remembered={x:result.memory.x,z:result.memory.z};
  f.enemy.x=-200;f.enemy.z=-100;
  for(let i=0;i<20;i++){result=f.tick(.1);assert.equal(result.enemy,null);assert.equal(result.aim,null);assert.ok(result.goal.x>25);assert.ok(Math.abs(result.goal.z)<5);if(result.memory)assert.deepEqual({x:result.memory.x,z:result.memory.z},remembered);}
});

test('flankers use a stable separate approach before entering the same capture area',()=>{
  const order={x:0,y:1.86,z:35,role:'flank',radius:5,strategic:true,squad:'one',slot:2,approachYaw:0},f=tactical(order);
  const approach=f.tick().goal;assert.ok(Math.abs(approach.x)>=9);assert.ok(approach.z<order.z-5);
  for(let i=0;i<12;i++)assert.deepEqual(f.tick().goal,approach,'reconsidering tactics does not swap the approach lane');
  Object.assign(f.bot,approach);const inside=f.tick().goal;assert.ok(Math.hypot(inside.x-order.x,inside.z-order.z)<order.radius);
});

test('unopposed squads actually take successive connected ground without a human shepherding them',()=>{
  for(const team of [1,2]){
    const s=C.create(map(),{captureSeconds:3}),list=troops(team,6),nav=s.sectors.map(f=>({...f,y:1.86}));
    const controller=AI.createController({time:()=>s.elapsed,random:()=>.5,actors:()=>list,sameTeam:(a,b)=>a.team===b.team,lineClear:()=>true,canTraverse:()=>true,pointInfo:()=>({walkable:true}),navigation:()=>nav,objective:a=>C.assignment(s,a,list.indexOf(a))});
    const captured=[];
    for(let i=0;i<700&&C.controlled(s,team)<5;i++){
      for(const event of C.step(s,.1,list))if(event.type==='captured'&&event.team===team)captured.push(event.sector);
      for(const a of list){const d=controller.update(a,.1);a.x+=d.move.x*d.speedMultiplier*.8;a.z+=d.move.z*d.speedMultiplier*.8;a.yaw=a.faceYaw=d.yaw;}
    }
    assert.equal(C.controlled(s,team),5,'all flags captured within seventy simulated seconds for team '+team);
    assert.equal(captured[0],team===1?'A':'E');assert.equal(new Set(captured).size,5);
  }
});
