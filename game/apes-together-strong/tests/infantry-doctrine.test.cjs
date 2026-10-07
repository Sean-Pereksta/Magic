'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function arena(){
 const g=new (loadEngine().ATSGame)('INFANTRY-FIRE','survival');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];g.world.getObjects=()=>[];
 g.world.terrain=()=>({biome:'farmland',road:true,water:false});g.world.lineClear=()=>true;g.world.blocked=()=>false;g.spawnSites=()=>{};
 g.tier=5;g.king.x=250;g.king.y=0;g.exposure=1;return g;
}
function refresh(g){g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);g.performance.beginStep();g.visibilityCache.clear()}
function soldier(g,role='rifleman',x=0,y=0){const h=g.makeHuman(x,y,null);g.forces.assign(h,null,role);Object.assign(h,{state:'combat',targetId:'king',lastSeenAt:g.time,perceptionTimer:0,shootTimer:0,specialAt:10000,dir:0,reported:true,suspicion:1});return h}
function updateHuman(g,h,dt){if(!g.forces.update(h,dt))g.updateHuman(h,dt)}

test('a patrol shadowing a visible army holds still most of the time and sustains rifle fire',()=>{
 const g=arena(),members=Array.from({length:4},(_,i)=>soldier(g,i===0?'leader':'rifleman',0,i*8)),s=g.forces.createSquad(members,g.king);s.nextReport=10000;
 for(let i=0;i<330;i++)g.makeApe(250+i%10,Math.floor(i/10)%10,'hold');
 const h=members[3],shoot=g.shoot.bind(g);let shots=0,moving=0,combat=0;
 g.shoot=(actor,target)=>{if(actor===h)shots++;shoot(actor,target)};
 for(let frame=0;frame<480;frame++){g.time+=1/60;refresh(g);updateHuman(g,h,1/60);moving+=h.moving?1:0;combat+=h.state==='combat'?1:0;assert.ok(Math.cos(h.dir-Math.atan2(g.king.y-h.y,g.king.x-h.x))>.99)}
 assert.equal(s.order,'Shadow');assert.ok(shots>=7,`expected sustained rifle fire, got ${shots} shots`);
 assert.ok(moving<=480*.18,`infantry moved on ${moving}/480 combat frames`);assert.equal(combat,480);
 assert.ok(Math.hypot(h.x,h.y)<90,'the patrol does not flee to a stand-off beyond rifle range');
});

test('close backsteps are capped in distance and time even when an ape follows every step',()=>{
 const g=arena(),h=soldier(g,'ranger');h.shootTimer=1;let travelled=0,moving=0;
 for(let frame=0;frame<480;frame++){
  g.time+=1/60;g.king.x=h.x+20;g.king.y=h.y;const x=h.x,y=h.y;
  g.forces.combatMovement(h,g.king,1/60);travelled+=Math.hypot(h.x-x,h.y-y);moving+=h.moving?1:0;
  assert.ok(h._combatStep.remaining>=0);assert.ok(Math.cos(h.dir)>.99,'backstepping keeps the weapon facing the ape');
  if(frame===59)assert.ok(travelled<=16.001,'the first close-range response cannot exceed 16px');
 }
 assert.ok(travelled<=64.001,`four short steps moved ${travelled}px`);assert.ok(travelled>=40,'close range still permits a useful short escape');
 assert.ok(moving<=480*.15,`backsteps consumed ${moving}/480 frames`);
 g.time+=3;g.king.x=h.x+20;const x=h.x;g.forces.combatMovement(h,g.king,1);assert.ok(Math.abs(h.x-x)<=16.001,'a coarse update cannot bypass the step cap');
});

test('backpedaling preserves contact through the next real perception tick',()=>{
 const g=arena(),h=soldier(g,'rifleman');h.shootTimer=.1;g.king.x=35;g.time=.1;refresh(g);
 g.forces.combatMovement(h,g.king,.1);assert.ok(h.x<0);assert.ok(Math.cos(h.dir)>.99);
 h.shootTimer=0;g.updateHuman(h,1/60);assert.equal(h.state,'combat');assert.equal(h.targetId,'king');assert.ok(g.bullets.length>0);
});

test('a damaged squad can fire during a longer supported fallback, then ends in a stationary line',()=>{
 const g=arena(),members=Array.from({length:4},(_,i)=>soldier(g,i===0?'leader':'rifleman',0,i*8));
 members.forEach(h=>h.hp=h.maxHp*.4);const s=g.forces.createSquad(members,g.king);s.nextReport=10000;
 const ally=Array.from({length:4},(_,i)=>soldier(g,'rifleman',-340,i*5));g.forces.createSquad(ally,{x:250,y:0},{order:'Hold'});
 g.king.x=70;refresh(g);g.forces.thinkSquad(s);assert.equal(s.order,'Fallback');assert.ok(s.fallbackPoint.x<-300);
 const destination={...s.fallbackPoint},h=members[0],shoot=g.shoot.bind(g);let shots=0,moving=0,finalMoving=0;
 g.shoot=(actor,target)=>{if(actor===h)shots++;shoot(actor,target)};
 for(let frame=0;frame<600;frame++){
  g.time+=1/60;g.king.x=h.x+70;g.king.y=h.y;h.lastSeenAt=g.time;refresh(g);updateHuman(g,h,1/60);
  moving+=h.moving?1:0;if(frame>=540)finalMoving+=h.moving?1:0;
  assert.deepEqual({...s.fallbackPoint},destination,'the destination stays fixed as contact follows');
 }
 assert.ok(moving>150,'strategic fallback can travel for longer than an individual backstep');assert.ok(shots>=9,'withdrawing infantry keeps firing');
 assert.equal(s.order,'Hold & Suppress');assert.equal(s.fallbackComplete,true);assert.ok(h.x<-270);assert.equal(finalMoving,0);
});

test('strategic fallback reaches fixed support after a stationary enemy falls out of sight',()=>{
 const g=arena(),members=Array.from({length:4},(_,i)=>soldier(g,i===0?'leader':'rifleman',0,i*8));members.forEach(h=>h.hp=h.maxHp*.4);
 const s=g.forces.createSquad(members,g.king);s.nextReport=10000;
 const allies=Array.from({length:4},(_,i)=>soldier(g,'rifleman',-420,i*5));g.forces.createSquad(allies,g.king,{order:'Hold'});g.king.x=70;
 refresh(g);g.forces.thinkSquad(s);const point={...s.fallbackPoint},h=members[0];let lostContact=false;
 for(let frame=0;frame<600;frame++){
  g.time+=1/60;refresh(g);updateHuman(g,h,1/60);
  if(h.state==='search'){lostContact=true;if(!s.fallbackComplete)assert.equal(s.order,'Fallback')}
  assert.deepEqual({...s.fallbackPoint},point);
 }
 assert.equal(lostContact,true,'the fixed target really leaves the withdrawing soldier\'s visible range');
 assert.equal(s.fallbackComplete,true);assert.equal(s.order,'Hold & Suppress');assert.ok(Math.hypot(h.x-point.x,h.y-point.y)<60);
});

test('leader loss regroups at a fixed nearby rally point without dragging the survivor backward',()=>{
 const g=arena(),members=Array.from({length:4},(_,i)=>soldier(g,i===0?'leader':'rifleman',0,i*8)),s=g.forces.createSquad(members,g.king);s.nextReport=10000;
 g.hurt(members[0],1000,g.king);const h=members[1];refresh(g);g.forces.thinkSquad(s);assert.equal(s.order,'Regroup');const rally={...s.regroupPoint},formation={...g.forces.formationPoint(h,s)};
 for(let frame=0;frame<390;frame++){g.time+=1/60;refresh(g);updateHuman(g,h,1/60);assert.deepEqual({...s.regroupPoint},rally);assert.deepEqual({...g.forces.formationPoint(h,s)},formation)}
 assert.ok(Math.hypot(h.x-rally.x,h.y-rally.y)<100);assert.ok(Math.hypot(h.x-formation.x,h.y-formation.y)<30);assert.equal(h.moving,false);
 g.time=8;refresh(g);g.forces.thinkSquad(s);assert.notEqual(s.order,'Regroup');
});

test('a marksman joins a committed fallback while retaining its warned aimed shot',()=>{
 const g=arena(),members=[soldier(g,'leader'),soldier(g,'sniper',0,15)];members.forEach(h=>h.hp=h.maxHp*.4);
 const s=g.forces.createSquad(members,g.king);s.nextReport=10000;
 const allies=Array.from({length:4},(_,i)=>soldier(g,'rifleman',-340,i*5));g.forces.createSquad(allies,g.king,{order:'Hold'});g.king.x=100;
 const h=members[1];let warning=false,shot=false;
 for(let frame=0;frame<120;frame++){g.time+=1/60;h.lastSeenAt=g.time;refresh(g);updateHuman(g,h,1/60);warning||=!!h.aiming;shot||=g.bullets.some(b=>b.owner===h.id)}
 assert.equal(s.order,'Fallback');assert.ok(h.x<-70,'the marksman moves toward support during its aiming cycle');assert.equal(warning,true);assert.equal(shot,true);
});

test('supporting armor reverses to one fixed infantry position and holds when the swarm follows',()=>{
 const g=arena(),h=soldier(g,'rifleman',-190,0),v={id:'vehicle-'+g.nextId++,kind:'tank',x:0,y:0,dir:0,turretDir:0,state:'combat'};
 g.forces.initVehicle(v,'tank',0);g.vehicles.push(v);v.approachCount=12;g.king.x=100;refresh(g);
 g.forces.positionTank(v,g.king,.05);const point={...v.reverseSupport};let reversing=0;
 for(let frame=0;frame<200;frame++){g.time+=.05;g.king.x=v.x+100;refresh(g);g.forces.positionTank(v,g.king,.05);reversing+=v.reversing?1:0;assert.deepEqual({...v.reverseSupport},point)}
 assert.ok(reversing>0);assert.equal(v.reverseComplete,true);assert.equal(v.moving,false);assert.ok(v.x>-220,'the rear destination cannot recede with the tank');
 assert.ok(Math.hypot(v.x-point.x,v.y-point.y)<=28);assert.equal(v.hp,1100);
 const loaded=loadEngine().ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),saved=loaded.vehicles[0];
 assert.deepEqual({...saved.reverseSupport},point);assert.equal(saved.reverseUntil,v.reverseUntil);assert.equal(saved.reverseComplete,true);
 loaded.king.x=saved.x+100;saved.approachCount=12;loaded.forces.positionTank(saved,loaded.king,.05);assert.equal(saved.moving,false,'loading does not restart a completed withdrawal');
});
