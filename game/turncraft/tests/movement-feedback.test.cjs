const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

test('a troop selected at its moving body displays a step X without changing its action',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    cam.x=0;cam.y=0;cam.zoom=1;W=1600;H=1200;
    const troop=addEntity('unit','rifleman',0,3,4);
    issueMoveOrder(troop,10,4);
    const snapshot=JSON.stringify(troop),body=entityScreenAnchor(troop);
    handlePolishTap(body.x,body.y);
    renderSelection();const feedback=renderMovementActionFeedback();
    return {selected:selectedId,id:troop.id,moving:troop.px!==troop.x,unchanged:snapshot===JSON.stringify(troop),remaining:feedback.remaining,count:feedback.count,description:feedback.description,badge:selectionName.dataset.stepLocked,mapX:drawMovementActionLock(troop,body.x,body.y,TILE),title:selectionName.title};
  })()`);
  assert.equal(result.selected,result.id);
  assert.equal(result.moving,true);
  assert.equal(result.unchanged,true,'selection and feedback must never alter the order, position, cooldown or current step');
  assert.equal(result.count,1);assert.ok(result.remaining>0);
  assert.equal(result.badge,'true');assert.equal(result.mapX,true);
  assert.match(result.description,/Next movement order takes effect after the current step/);
  assert.equal(result.title,result.description);
});

test('queued course changes preserve the current step and timer, and X clears exactly at the ready boundary',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const troop=addEntity('unit','rifleman',0,3,4);issueMoveOrder(troop,10,4);selectEntity(troop);
    const ready=troop.moveReadyAt,current={x:troop.x,y:troop.y,px:troop.px,py:troop.py};
    sendSelectionMovement({x:3,y:10});
    const after={x:troop.x,y:troop.y,px:troop.px,py:troop.py},queued=troop.moveDestination;
    testClock=ready-1;const before=drawMovementActionLock(troop,0,0,TILE);
    testClock=ready;renderSelection();renderMovementActionFeedback();const at=drawMovementActionLock(troop,0,0,TILE),badge=selectionName.dataset.stepLocked;
    const currentReady=troop.moveReadyAt;stepTowardDestination(troop);
    return {ready,currentReady,current,after,queued,before,at,badge,next:{x:troop.x,y:troop.y},nextReady:troop.moveReadyAt};
  })()`);
  assert.deepEqual(result.after,result.current,'the already committed movement cannot be redirected before the step completes');
  assert.deepEqual(result.queued,{x:3,y:10});assert.equal(result.currentReady,result.ready);
  assert.equal(result.before,true);assert.equal(result.at,false);assert.equal(result.badge,'false');
  assert.notDeepEqual(result.next,{x:result.current.x,y:result.current.y});assert.ok(result.nextReady>result.ready);
});

test('mixed groups show a separate X on each busy owned troop and keep ready troops available',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const busy=addEntity('unit','rifleman',0,3,4),ready=addEntity('unit','engineer',0,4,5),foreign=addEntity('unit','zealot',1,5,5),unselected=addEntity('unit','rifleman',0,6,6);
    busy.moveReadyAt=foreign.moveReadyAt=unselected.moveReadyAt=testClock+1200;
    selectEntities([busy,ready,foreign]);const feedback=movementSelectionFeedback();
    return {count:feedback.count,total:feedback.total,remaining:feedback.remaining,ids:feedback.locked.map(e=>e.id),badges:[busy,ready,foreign,unselected].map(e=>drawMovementActionLock(e,0,0,TILE)),description:feedback.description};
  })()`);
  assert.equal(result.count,1);assert.equal(result.total,2);assert.equal(result.remaining,1.2);
  assert.deepEqual(result.ids,[1]);assert.deepEqual(result.badges,[true,false,false,false]);
  assert.match(result.description,/1\/2 troops completing their current step/);
});

test('weapon cooldowns and shot animations do not claim a command lock that the simulation does not have',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const troop=addEntity('unit','rifleman',0,3,4);selectEntity(troop);
    troop.attackReadyAt=testClock+5000;troop.projectileAnim={startAt:testClock,endAt:testClock+580};troop.attackAnim={startAt:testClock,endAt:testClock+520};
    return {remaining:movementStepRemaining(troop),mapX:drawMovementActionLock(troop,0,0,TILE),count:movementSelectionFeedback().count};
  })()`);
  assert.deepEqual(result,{remaining:0,mapX:false,count:0});
});

test('movement button feedback counts down and restores the existing command without disabling it',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const troop=addEntity('unit','rifleman',0,3,4);troop.moveReadyAt=testClock+1200;selectEntity(troop);
    const small={textContent:'1.2s · queues',dataset:{}},button={dataset:{},disabled:false,title:'Any tile',textContent:'Move',classList:{toggle(){}},querySelector(selector){return selector==='span'?{textContent:'Move'}:small}};
    commandButtons.querySelectorAll=()=>[button];renderSelection();renderMovementActionFeedback();const first=small.textContent,title=button.title;
    testClock+=700;renderSelection();renderMovementActionFeedback();const later=small.textContent;
    testClock+=500;renderSelection();renderMovementActionFeedback();return {first,later,title,restored:small.textContent,restoredTitle:button.title,disabled:button.disabled};
  })()`);
  assert.match(result.first,/× Step ends 1\.2s/);assert.match(result.later,/× Step ends 0\.5s/);
  assert.match(result.title,/Next movement order/);assert.equal(result.disabled,false);
  assert.equal(result.restored,'1.2s · queues');assert.equal(result.restoredTitle,'Any tile');
});
