const test=require('node:test'),assert=require('node:assert/strict');
const T=require('./tactics.js');
const actor=(x=0,z=0)=>({mesh:{position:{x,z}},velocity:{x:0,z:0},heading:{x:0,z:-1}});
test('formation calls carry the full set, depths, routes and screen role',()=>{
  const p=T.formation({routes:['Go','Bubble','Lead','Lead'],xs:[-19,5,9,13],depths:[0,3,0,1],screen:1},40);
  assert.deepEqual(p[1],{x:5,z:42.1,route:'Bubble',screenTarget:true});assert.equal(p[2].screenTarget,false);
});
test('cross-field shifts move continuously, settle and preserve actor identity',()=>{
  const a=actor(-18,40),target={x:14,z:42};let done=false;
  for(let i=0;i<1200&&!done;i++){const prior={...a.mesh.position};done=T.advance(a,target,1/120);assert.ok(Math.hypot(a.mesh.position.x-prior.x,a.mesh.position.z-prior.z)<.1);}
  assert.ok(done);assert.deepEqual(a.mesh.position,target);
});
test('mid-shift target changes do not reset or teleport the player',()=>{
  const a=actor(-18,40);for(let i=0;i<60;i++)T.advance(a,{x:18,z:40},1/60);
  const before={...a.mesh.position};T.advance(a,{x:-10,z:42},1/60);assert.ok(Math.hypot(a.mesh.position.x-before.x,a.mesh.position.z-before.z)<.2);
});
test('defenders have staggered read delays and repeated audibles cannot extend a pending read',()=>{
  const fast=T.reaction({now:0,random:0,discipline:.9,reaction:.1}),slow=T.reaction({now:0,random:1,discipline:.2,reaction:.3});
  assert.ok(fast.readyAt>0&&slow.readyAt>fast.readyAt+200);
  assert.equal(T.reaction({now:100,random:1,previous:slow}),slow);
});
test('defensive reads use observed alignment and preserve deep shell depth',()=>{
  const o={kind:'safety',mode:'zone',zoneX:12,homeZ:-28,observed:[{x:6,z:0},{x:12,z:0},{x:17,z:0},{x:22,z:0}]};
  const d=T.defensiveLandmark(o);assert.equal(d.z,-28);assert.ok(Math.abs(d.x-12)<2);
  const man=T.defensiveLandmark({...o,kind:'corner',mode:'press',target:3});assert.equal(man.x,22);assert.equal(man.z,-1.6);
});
test('strength edge controls block duration, drive direction, shedding and constrained reach',()=>{
  const win=T.blockContest({strength:95,defenseStrength:45}),even=T.blockContest({strength:70,defenseStrength:70}),lose=T.blockContest({strength:45,defenseStrength:95});
  assert.ok(win.duration>even.duration&&even.duration>lose.duration);
  assert.ok(win.shedRate<even.shedRate&&even.shedRate<lose.shedRate);
  assert.ok(win.driveSpeed>0&&lose.driveSpeed<0);assert.equal(even.driveSpeed,0);
  assert.ok(win.reach<lose.reach&&lose.reach<1);
});
test('neither equal nor weaker defenders receive a bull-rush win from random shove rolls',()=>{
  for(let defenseStrength=20;defenseStrength<=80;defenseStrength+=5)assert.ok(T.blockContest({strength:80,defenseStrength}).driveSpeed>=0);
});
test('shed thresholds are deterministic and time-scaled, not a per-frame success lottery',()=>{
  const c=T.blockContest({strength:55,defenseStrength:90});
  function time(hz){const e=T.engagement(c,.35);while(!T.advanceEngagement(e,1/hz)){}return e.elapsed;}
  assert.ok(Math.abs(time(30)-time(120))<1/30+.001);
  const hold=T.engagement(T.blockContest({strength:100,defenseStrength:30}),.9999);
  for(let i=0;i<600;i++)T.advanceEngagement(hold,1/120);
  assert.ok(hold.elapsed>hold.duration&&T.advanceEngagement(hold,0));
});
test('a blocker protects only the shoulder it physically seals',()=>{
  const d={x:0,z:-1.2},b={x:0,z:0};
  assert.equal(T.shielded(d,{x:0,z:1.2},b),true);
  assert.equal(T.shielded(d,{x:3,z:0},b),false);
  assert.equal(T.shielded(d,{x:0,z:-3},b),false);
  assert.equal(T.shielded(d,{x:0,z:5},{x:0,z:3}),false);
});
test('blocked tackle reach is reduced but real body contact remains dangerous',()=>{
  const normal=T.tackleRadius({}),held=T.tackleRadius({blockReach:.12}),dive=T.tackleRadius({diving:true});
  assert.ok(held<normal&&held>=.8&&dive>normal);assert.equal(T.tackleRadius({fooled:true}),.8);
});
test('runner chooses separation from an exposed tackle envelope',()=>{
  const base={x:0,z:-1,limit:3.2};
  const lane=T.safeLane({x:0,z:0,defenders:[{id:'a',x:.9,z:-2,vx:0,vz:0,tackleRadius:1.5}],speed:8,vz:-8,markerZ:-30},base);
  assert.ok(lane.x<-.2);assert.ok(lane.z<=0);assert.equal(lane.limit,3.2);
});
test('runner accounts for a crossing defender rather than only current positions',()=>{
  const base={x:0,z:-1,limit:1.6},o={x:0,z:0,speed:8,vz:-8,markerZ:-30};
  const lane=T.safeLane({...o,defenders:[{x:3,z:-3,vx:-7,vz:0,tackleRadius:1.4}]},base);
  assert.ok(Math.abs(lane.x)>.2&&lane.z<=0);
});
test('held blocks improve protected lanes but do not hide an unblocked support tackler',()=>{
  const base={x:0,z:-1,limit:1.6},o={x:0,z:2,speed:7,vz:-7,markerZ:-30,screen:true,defenders:[{id:'d',x:0,z:-1.4,tackleRadius:1.1,blocked:true}],blockers:[{x:0,z:-.2,target:'d',engaged:true}]};
  const protectedLane=T.safeLane(o,base),support=T.safeLane({...o,defenders:[...o.defenders,{id:'s',x:-1.8,z:.5,tackleRadius:1.3}]},base);
  assert.ok(protectedLane.score>support.score);assert.ok(support.x>0);
});
test('sidelines and forward-progress limits stay authoritative',()=>{
  const lane=T.safeLane({x:23.3,z:0,bestZ:0,markerZ:-30,speed:9,vz:-9,defenders:[{x:23,z:-3,tackleRadius:1.4}]},{x:0,z:-1,limit:1.2});
  assert.ok(lane.x<=0&&lane.z<=0);assert.equal(lane.limit,1.2);
});
