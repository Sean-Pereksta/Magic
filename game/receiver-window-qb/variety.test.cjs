const test=require('node:test'),assert=require('node:assert/strict');
const V=require('./variety.js'),P=require('./progression.js');
const player=(overrides={})=>P.migratePlayer({name:'Test',speed:70,cutting:70,turning:70,evasion:70,catching:70,strength:70,...overrides});
test('equal overall specialists retain different movement and earned identities',()=>{
  const fast=player({speed:95,cutting:55,turning:60}),route=player({speed:55,cutting:80,turning:75});
  assert.ok(V.movement(fast).speed>V.movement(route).speed+.9);
  assert.ok(V.movement(route).turn>V.movement(fast).turn);
  assert.ok(V.movement(route).cutLoss<V.movement(fast).cutLoss);
  assert.equal(V.signature(fast),null);fast.trainingByStat.speed=5;assert.equal(V.signature(fast),'Deep Threat');
  assert.ok(V.movement(player({speed:100000})).speed<10);
});
test('defensive tendencies wait for evidence, cap, decay, and sanitize saved history',()=>{
  const pass={depth:30,target:0,route:'Go'};
  assert.equal(V.tendencies([pass,pass]).depth,0);
  let history=[];for(let i=0;i<50;i++)history=V.remember(history,pass);
  assert.equal(history.length,12);assert.equal(V.tendencies(history).depth,3);
  assert.equal(V.tendencies(history).help,0);assert.equal(V.tendencies(history).attention,1);
  for(let i=0;i<12;i++)history=V.remember(history,{depth:5,target:i%4,route:'Drag'});
  assert.equal(V.tendencies(history).depth,-2);assert.equal(V.tendencies(history).attention,0);
  assert.deepEqual(V.cleanHistory([null,{depth:NaN,target:0},{depth:1,target:9},pass]),[pass]);
});
test('opponent identities permit varied schemes rather than a guaranteed counter',()=>{
  for(let round=1;round<25;round++){
    const identity=V.identity(round),seen=new Set();
    for(let i=0;i<100;i++)seen.add(V.chooseCoverage(identity,{depth:3},identity.schemes[0],()=>i/100));
    assert.deepEqual([...seen].sort(),[...identity.schemes].sort());
  }
});
test('actual placement rewards stride and protection but charges difficult catch momentum',()=>{
  const base={dx:0,dz:-.5,height:1.4,headingX:0,headingZ:-1,speed:8};
  const stride=V.placement(base),behind=V.placement({...base,dz:.6}),low=V.placement({...base,height:.5});
  assert.equal(stride.feedback,'IN STRIDE');assert.equal(behind.kind,'BACK SHOULDER');
  assert.ok(stride.retention>behind.retention+.3);assert.ok(low.retention<stride.retention);
  const protectedBall=V.placement({...base,dx:-.5,contest:2,defenderX:1});
  assert.ok(protectedBall.away);assert.ok(protectedBall.hands>behind.hands);
  assert.equal(V.placement({...base,sideline:true}).kind,'TOE TAP');
  assert.equal(V.placement({...base,jump:.6,contest:1}).feedback,'JUMP BALL');
  assert.equal(V.placement({...base,bobbled:true}).feedback,'BOBBLE RECOVERY');
});
test('pursuit predicts a bounded intercept without changing defender speed',()=>{
  assert.equal(V.pursuitTime(0,5,0,-5,5),.5);
  assert.equal(V.pursuitTime(0,50,0,-8,9),1.25);
  for(const speed of [0,5,10])assert.ok(Number.isFinite(V.pursuitTime(0,5,0,8,speed)));
});
