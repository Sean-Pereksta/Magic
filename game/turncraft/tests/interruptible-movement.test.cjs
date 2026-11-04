const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

test('every moving troop advances one legal grid step and reverses at its next short deadline',()=>{
  const run=simulation(),result=run.plain(`Object.keys(UNITS).filter(type=>UNITS[type].move>0).map(type=>{
    entities=[];const e=addEntity('unit',type,0,15,15),start=testClock,range=UNITS[type].move,cd=effectiveMoveCd(e)*1000;
    issueMoveOrder(e,35,15);const first={x:e.x,y:e.y,ready:e.moveReadyAt};testClock=start+1;issueMoveOrder(e,5,15);const ready=e.moveReadyAt;
    testClock=ready-.01;updatePersistentOrders();const waiting=e.x===first.x&&e.y===first.y;
    testClock=ready+.001;updatePersistentOrders();return {type,range,cd,start,first,ready,waiting,reversed:e.x<first.x&&e.y===first.y};
  })`);
  assert.ok(result.length>=35);
  for(const r of result){assert.deepEqual({x:r.first.x,y:r.first.y},{x:16,y:15},r.type);assert.ok(Math.abs(r.first.ready-r.ready)<.001,r.type);assert.ok(Math.abs(r.first.ready-r.start-r.cd/r.range)<.001,r.type);assert.equal(r.waiting,true,r.type);assert.equal(r.reversed,true,r.type)}
});

test('continuous travel preserves the original tiles per cooldown for ground, diagonal air, slows, and charge',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    setBoardDimensions('rift-dominion');terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
    return ['rifleman','medic','hellion','dragoon','phoenix','zergling','ultralisk'].map(type=>{
      entities=[];const e=addEntity('unit',type,0,5,5),start=testClock,cd=effectiveMoveCd(e)*1000,range=UNITS[type].move,air=isFlying(e);issueMoveOrder(e,105,air?105:5);let steps=1;
      while(e.moveDestination&&steps<150){testClock=e.moveReadyAt+.0001;updatePersistentOrders();steps++}
      const elapsed=e.moveReadyAt-start;return {type,steps,elapsed,expected:100*cd/range,x:e.x,y:e.y,air};
    })
  })()`);
  for(const r of result){assert.equal(r.steps,100,r.type);assert.ok(Math.abs(r.elapsed-r.expected)<.02,JSON.stringify(r));assert.equal(r.x,105);assert.equal(r.y,r.air?105:5)}
});

test('short-step deadlines carry ordinary frame overrun without increasing long-distance speed',()=>{
  const run=simulation(),r=run.plain(`(()=>{setBoardDimensions('rift-dominion');terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));const e=addEntity('unit','phoenix',0,5,5),start=testClock,expected=100*effectiveMoveCd(e)*1000/UNITS[e.type].move;issueMoveOrder(e,105,5);while(e.moveDestination){testClock+=1000/60;updatePersistentOrders()}return {actual:e.moveReadyAt-start,expected}})()`);
  assert.ok(Math.abs(r.actual-r.expected)<.001,JSON.stringify(r));
});

test('a fresh order after an idle stop cannot spend overdue movement time as a catch-up burst',()=>{
  const run=simulation(),r=run.plain(`(()=>{const e=addEntity('unit','rifleman',0,10,10);issueMoveOrder(e,30,10);const duration=e.moveStepDuration;applyCommand({type:'stop',slot:0,entityId:e.id});testClock=e.moveReadyAt+duration*.5;const issued=testClock;issueMoveOrder(e,30,10);return {remaining:e.moveReadyAt-issued,duration}})()`);
  assert.ok(Math.abs(r.remaining-r.duration)<.001);
});

test('stop and hold keep the current short step clock but clear future travel; attack replaces movement intent',()=>{
  for(const type of ['stop','hold','attack']){
    const run=simulation(),r=run.plain(`(()=>{const e=addEntity('unit','rifleman',0,15,15),enemy=addEntity('unit','zealot',1,4,15);issueMoveOrder(e,35,15);const ready=e.moveReadyAt,x=e.x;testClock++;applyCommand({type:${JSON.stringify(type)},slot:0,entityId:e.id,targetId:enemy.id});const immediate={x:e.x,ready:e.moveReadyAt,moving:!!e.moveDestination,order:e.order,target:e.targetId};testClock=ready+1;updatePersistentOrders();updateCombat(.01);return {immediate,x:e.x,ready,xBefore:x,enemy:enemy.id}})()`);
    assert.equal(r.immediate.x,r.xBefore);assert.equal(r.immediate.ready,r.ready);assert.equal(r.immediate.moving,false);
    if(type==='attack'){assert.equal(r.immediate.target,r.enemy);assert.ok(r.x<r.xBefore)}else{assert.equal(r.immediate.order,type);assert.equal(r.x,r.xBefore)}
  }
});

test('charge lasts one original boosted distance budget, and fungal movement retains its one-tile cadence',()=>{
  const run=simulation('ascendant'),r=run.plain(`(()=>{const e=addEntity('unit','sentinel',0,10,10),base=UNITS[e.type].move,cd=effectiveMoveCd(e)*1000;e.chargeUntil=testClock+7000;const durations=[];for(let i=0;i<base+3;i++){if(i)testClock=e.moveReadyAt+.001;moveEntity(e,e.x+1,e.y);durations.push(e.moveStepDuration)}e.fungalSlowedUntil=testClock+10000;testClock=e.moveReadyAt+.001;moveEntity(e,e.x+1,e.y);return {base,cd,durations,fungal:e.moveStepDuration,budget:e.chargeStepsRemaining}})()`);
  for(const duration of r.durations.slice(0,r.base+2))assert.ok(Math.abs(duration-r.cd/(r.base+2))<.001);
  assert.ok(Math.abs(r.durations.at(-1)-r.cd/r.base)<.001);assert.equal(r.fungal,r.cd);assert.equal(r.budget,0);
});

test('a short step cannot cross a wall, corner, held troop, or bypass a deployed-unit restriction',()=>{
  const run=simulation(),r=run.plain(`(()=>{const e=addEntity('unit','rifleman',0,10,10),held=addEntity('unit','rifleman',0,11,10),tank=addEntity('unit','siegeTank',0,15,15);held.order='hold';tileAt(10,9).blocked=true;tank.siegeMode=true;return {long:moveEntity(e,12,10),wall:moveEntity(e,10,9),corner:moveEntity(e,11,11),occupied:moveEntity(e,11,10),sieged:issueMoveOrder(tank,25,15),position:{x:e.x,y:e.y}}})()`);
  assert.deepEqual(r,{long:false,wall:false,corner:false,occupied:false,sieged:false,position:{x:10,y:10}});
});

test('Emergency Stabilization keeps its mobility support proportional to a short movement step',()=>{
  const run=simulation(),r=run.plain(`(()=>{const e=addEntity('unit','rifleman',0,10,10);e.hp-=20;issueMoveOrder(e,35,10);const duration=e.moveStepDuration;testClock+=100;const now=testClock;addAreaEffect({id:'test-stabilize',kind:'stabilization',targetId:e.id,startAt:now-100,endAt:now+3000,nextTickAt:now,lastTickAt:now-100});updateAreaEffects();return {remaining:e.moveReadyAt-now,expected:duration*.65,hp:e.hp}})()`);
  assert.ok(Math.abs(r.remaining-r.expected)<.001);assert.ok(r.hp>0);
});

test('a controller snapshot preserves the active step duration and remaining charge distance',()=>{
  const run=simulation('ascendant'),r=run.plain(`(()=>{const e=addEntity('unit','sentinel',0,10,10);e.chargeUntil=testClock+7000;issueMoveOrder(e,35,10);const before={ready:e.moveReadyAt,duration:e.moveStepDuration,charge:e.chargeStepsRemaining};applyRemoteState(JSON.parse(JSON.stringify(serializeState())));const restored=entityById(e.id);return {before,after:{ready:restored.moveReadyAt,duration:restored.moveStepDuration,charge:restored.chargeStepsRemaining}}})()`);
  assert.deepEqual(r.after,r.before);
});

test('a crowded mixed group reverses inside a narrow passage and can subsequently finish a new crossing',()=>{
  const run=simulation(),r=run.plain(`(()=>{setBoardDimensions('rift-dominion');terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));for(let y=0;y<BOARD_H;y++)if(y<60||y>62)tileAt(64,y).blocked=true;const units=Array.from({length:24},(_,i)=>addEntity('unit',['rifleman','medic','siegeTank','goliath'][i%4],0,50+i%6,58+Math.floor(i/6))),ids=units.map(u=>u.id);let invalid=false;const tick=n=>{for(let i=0;i<n;i++){testClock+=250;updatePersistentOrders();invalid||=units.some(u=>groundBlocked(u.x,u.y,u))||new Set(units.map(u=>u.x+','+u.y)).size!==units.length}};orderFormation(ids,105,61,0);tick(48);orderFormation(ids,30,61,0);tick(36);orderFormation(ids,105,61,0);tick(1100);return {invalid,arrived:units.filter(u=>!u.moveDestination&&u.x>100).length}})()`);
  assert.equal(r.invalid,false);assert.equal(r.arrived,24);
});
