'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const F=require('./football-simulation.js');
const actor=(overrides={})=>({position:{x:0,y:0,z:-12},velocity:{x:0,y:0,z:0},heading:{x:0,y:0,z:-1},maxSpeed:0,acceleration:0,reach:{horizontal:.75,standingHeight:2.2,jumpHeight:0,minHeight:.08},...overrides});
const origin={x:0,y:1.7,z:0};
const close=(a,b,epsilon=1e-7)=>assert.ok(Math.abs(a-b)<epsilon,`${a} differs from ${b}`);
test('away placement improves the observed passing window without adding catch range',()=>{
  const receiver=actor(),defender=actor({position:{x:-.7,y:0,z:-12},observed:true});
  const args={origin,receiver,defenders:[defender],flightTime:.45,amount:.55};
  const outside=F.placementTarget({...args,mode:'outside_shoulder'}),inside=F.placementTarget({...args,mode:'inside_shoulder'});
  assert.ok(outside.x>0&&inside.x<0);
  const a=F.passingWindow({...args,target:outside}),b=F.passingWindow({...args,target:inside});
  assert.equal(a.receiverReachable,true);assert.equal(b.receiverReachable,true);
  assert.ok(a.clearance>b.clearance);assert.equal(a.label,'clean');assert.equal(b.label,'danger');
  const beyond=F.placementTarget({...args,mode:'outside_shoulder',freeOffset:{x:4,y:0,z:0}});
  const miss=F.passingWindow({...args,target:beyond});assert.equal(miss.receiverReachable,false);assert.equal(miss.label,'danger');
});
test('window assistance and placement have no information about unobserved defenders',()=>{
  const receiver=actor(),base={origin,receiver,target:{x:0,y:1.65,z:-12},flightTime:.45};
  const hidden=actor({position:{x:0,y:0,z:-12},visible:false,id:'hidden'});
  assert.deepEqual(F.passingWindow({...base,defenders:[hidden]}),F.passingWindow({...base,defenders:[]}));
  assert.deepEqual(F.placementTarget({...base,mode:'outside',defenders:[hidden]}),F.placementTarget({...base,mode:'outside',defenders:[]}));
});
test('measured height and hand reach bound planning instead of an invisible rating radius',()=>{
  const point={x:0,y:2.2,z:-12};
  assert.equal(F.reachAt(actor({reach:{horizontal:.4,standingHeight:1.8,jumpHeight:0}}),point,.3).reachable,false);
  assert.equal(F.reachAt(actor({reach:{horizontal:.4,standingHeight:2.3,jumpHeight:0}}),point,.3).reachable,true);
  assert.equal(F.reachAt(actor({reach:{horizontal:.4,standingHeight:2.3,jumpHeight:0}}),{...point,x:.41},0).reachable,false);
});
test('prediction is bounded current-velocity anticipation and ignores route/path data',()=>{
  const receiver=actor({velocity:{x:4,y:0,z:-6},maxSpeed:10});
  assert.deepEqual(F.predictPosition(receiver,99),{x:10,y:0,z:-27});
  const a=F.placementTarget({receiver,flightTime:.5,mode:'in_stride'});
  const b=F.placementTarget({receiver:{...receiver,path:[{x:999,y:999,z:999}],route:'secret-cut'},flightTime:.5,mode:'in_stride'});
  assert.deepEqual(a,b);
});
test('all nine placement choices preserve manual offsets and finite world coordinates',()=>{
  for(const mode of F.placementModes){
    const base={receiver:actor(),origin,flightTime:.4,mode,field:{minX:-25,maxX:25}};
    const a=F.placementTarget(base),b=F.placementTarget({...base,freeOffset:{x:3,y:.4,z:-2}});
    close(b.x-a.x,3);close(b.y-a.y,.4);close(b.z-a.z,-2);
    assert.ok(Object.values(a).every(Number.isFinite));
  }
  assert.throws(()=>F.placementTarget({mode:'hidden_route_lock'}),RangeError);
});
test('accuracy, set feet and release timing narrow a bounded launch error',()=>{
  const args={origin,target:{x:4,y:1.7,z:-24},random:()=>1,power:.8};
  const good=F.throwSolution({...args,qb:{accuracy:95,strength:80,footwork:95,releaseTiming:.98}});
  const poor=F.throwSolution({...args,qb:{accuracy:35,strength:80,footwork:30,releaseTiming:.3}});
  assert.ok(good.spread<poor.spread);assert.ok(Math.hypot(good.error.x,good.error.y,good.error.z)<Math.hypot(poor.error.x,poor.error.y,poor.error.z));
  const strong=F.throwSolution({...args,qb:{strength:95}}),weak=F.throwSolution({...args,qb:{strength:30}});
  assert.ok(strong.maxSpeed>weak.maxSpeed);
});
test('a reachable throw uses the same gravity and wind as its exact flight endpoint',()=>{
  const solution=F.throwSolution({origin,target:{x:2,y:1.8,z:-25},type:'touch',power:.9,random:()=>.5,weather:{wind:{x:.7,y:0,z:.1},wetness:.3}});
  assert.equal(solution.reachable,true);
  const p=F.ballPoint({position:origin,velocity:solution.velocity,gravity:solution.gravity,wind:solution.wind},solution.flightTime);
  close(p.x,solution.target.x);close(p.y,solution.target.y);close(p.z,solution.target.z);
  const impossible=F.throwSolution({origin,target:{x:0,y:2,z:-1000},random:()=>.5});
  assert.equal(impossible.reachable,false);assert.ok(Math.hypot(...Object.values(impossible.velocity))<=impossible.maxSpeed+1e-8);
});
test('a defender must see the ball and spend actual reaction time before choosing a technique',()=>{
  const defender=actor({position:{x:0,y:0,z:0},reaction:.2});
  const rear={position:{x:0,y:1.7,z:2},velocity:{x:0,y:0,z:0}};
  const unseen=F.observeBall({defender,ball:rear,dt:.25,previous:{observedFor:4}});
  assert.equal(unseen.visible,false);assert.equal(unseen.canReact,false);
  assert.equal(F.defensiveTechnique({defender,ball:rear,observation:unseen}).technique,'cover');
  const front={position:{x:0,y:1.7,z:-.3},velocity:{x:0,y:0,z:0}};
  const one=F.observeBall({defender,ball:front,dt:.05}),two=F.observeBall({defender,ball:front,dt:.16,previous:one});
  assert.equal(one.canReact,false);assert.equal(two.canReact,true);
  assert.equal(F.defensiveTechnique({defender,ball:front,observation:one}).technique,'cover');
  assert.notEqual(F.defensiveTechnique({defender,ball:front,observation:two}).technique,'cover');
  const blocked=F.observeBall({defender,ball:front,dt:.25,previous:two,occluded:true});assert.equal(blocked.canReact,false);
});
test('fourth-down discipline favors visible swats while a ball hawk can attempt an interception',()=>{
  const defender=actor({position:{x:0,y:0,z:0},ballSkills:95,discipline:90,personality:'ballhawk'}),ball={position:{x:0,y:1.5,z:-.3},velocity:{x:0,y:0,z:0}},observation={visible:true,observedFor:.4};
  const aggressive=F.defensiveTechnique({defender,ball,observation,game:{down:2}}),fourth=F.defensiveTechnique({defender,ball,observation,game:{down:4}});
  assert.equal(aggressive.canIntercept,true);assert.equal(aggressive.forceSwat,false);
  assert.equal(fourth.canIntercept,false);assert.equal(fourth.forceSwat,true);
  assert.equal(fourth.contactRequired,true);assert.equal('possession' in fourth,false);
  assert.equal(F.defensiveTechnique({defender,ball:{...ball,position:{x:30,y:1.5,z:-30}},observation}).technique,'cover');
});
test('late disruption and tip drills describe techniques without awarding an invisible touch',()=>{
  const defender=actor({position:{x:0,y:0,z:0},ballSkills:20}),ball={position:{x:0,y:1.5,z:-.3},velocity:{x:0,y:0,z:0}},observation={visible:true,observedFor:.4};
  const late=F.defensiveTechnique({defender,ball,observation,receiver:{...actor(),securing:true}});
  assert.equal(late.technique,'late_disruption');assert.equal(late.forceSwat,true);assert.equal(late.contactRequired,true);
  assert.equal(F.defensiveTechnique({defender,ball:{...ball,tipCount:1},observation}).technique,'tip_drill');
});
function dive(){return F.beginTechnique({defender:actor({position:{x:0,y:0,z:0},velocity:{x:0,y:0,z:-6},maxSpeed:8,acceleration:20}),decision:{technique:'dive_swat',target:{x:.3,y:.9,z:-4},dive:true,forceSwat:true,commitment:.78,recovery:.72}});}
test('a committed dive cannot steer or instantly recover, and stepping does not mutate inputs',()=>{
  const state=dive(),copy=JSON.parse(JSON.stringify(state)),next=F.advanceTechnique(state,.2,{target:{x:100,y:9,z:100}});
  assert.deepEqual(state,copy);assert.deepEqual(next.lockedDirection,state.lockedDirection);assert.equal(next.phase,'committed');assert.ok(next.position.y>0);
  const a=F.advanceTechnique(state,.2),b=F.advanceTechnique(state,.2,{direction:{x:1,y:0,z:0}});assert.deepEqual(a,b);
  let s=state;for(let i=0;i<4;i++)s=F.advanceTechnique(s,.2);assert.equal(s.phase,'recovery');assert.ok(s.remaining>.6);
  for(let i=0;i<4;i++)s=F.advanceTechnique(s,.2);assert.equal(s.phase,'complete');close(s.position.y,0);
});
test('bounded elapsed time and exact ballistic/slide integration agree at 30 and 120 Hz',()=>{
  const state=dive();assert.deepEqual(F.advanceTechnique(state,0).position,state.position);
  assert.equal(F.advanceTechnique(state,-1).elapsed,0);assert.equal(F.advanceTechnique(state,999).elapsed,.25);
  assert.throws(()=>F.advanceTechnique(state,Infinity),TypeError);
  const run=dt=>{let s=dive();for(let i=0;i<Math.round(1.5/dt);i++)s=F.advanceTechnique(s,dt);return s;};
  const a=run(1/30),b=run(1/120);close(a.position.x,b.position.x);close(a.position.y,b.position.y);close(a.position.z,b.position.z);close(a.elapsed,b.elapsed);
});
test('finite validation and the browser global share the CommonJS API',()=>{
  assert.throws(()=>F.reachAt(actor(),{x:NaN,y:1,z:0},.2),TypeError);
  const sandbox={};vm.runInNewContext(fs.readFileSync(require.resolve('./football-simulation.js'),'utf8'),sandbox);
  assert.equal(typeof sandbox.QBFootball.defensiveTechnique,'function');assert.deepEqual(Object.keys(sandbox.QBFootball),Object.keys(F));
});
