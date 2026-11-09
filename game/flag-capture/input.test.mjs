import test from 'node:test';
import assert from 'node:assert/strict';
import {createGame,applyAction,legalActions} from './engine.mjs';
import {createInputController,previewActions} from './input.mjs';

function battle(role='scout'){
  const state=createGame({playerFaction:'bloom',aiFaction:'chaos',opponentMode:'local',seed:52});
  state.tiles.forEach(tile=>tile.terrain='grass');
  const unit=state.units.find(u=>u.side==='player'&&u.role===role),enemy=state.units.find(u=>u.side==='ai'&&u.role==='brute');
  state.units=[unit,enemy];Object.assign(unit,{x:1,y:4});Object.assign(enemy,{x:2,y:4});
  return {state,unit,enemy};
}
const move = unit => ({type:'move',unitId:unit.id,x:1,y:3});
const attack = (unit,enemy) => ({type:'attack',unitId:unit.id,targetId:enemy.id,x:enemy.x,y:enemy.y});

test('recharging previews retain real geometry without changing any real deadline or status',()=>{
  const {state,unit}=battle();unit.readyAt=.4;unit.abilityReadyAt=7;unit.abilityReadyAts=[7];
  const before=structuredClone(state),previews=previewActions(state,unit.id);
  assert.ok(previews.some(action=>action.type==='move'));
  assert.ok(previews.some(action=>action.type==='attack'));
  assert.ok(previews.some(action=>action.type==='ability'));
  assert.deepEqual(state,before);
  unit.rootedUntil=1;
  assert.equal(previewActions(state,unit.id).some(action=>action.type==='move'),false);
});

test('near-ready movement executes once on recharge without clicking a second time',()=>{
  const {state,unit}=battle(),input=createInputController();unit.readyAt=.4;
  assert.equal(input.offer(state,move(unit)).status,'queued');
  state.time=.39;assert.deepEqual(input.drain(state),[]);
  state.time=.4;const ready=input.drain(state);assert.equal(ready.length,1);
  assert.equal(applyAction(state,ready[0]).ok,true);assert.equal(unit.y,3);
  assert.deepEqual(input.drain(state),[]);assert.equal(input.pending().length,0);
});

test('ready orders pass through immediately; long recovery and illegal destinations never queue',()=>{
  const {state,unit}=battle(),input=createInputController();
  assert.equal(input.offer(state,move(unit)).status,'ready');
  unit.readyAt=.56;
  assert.equal(input.offer(state,move(unit)).reason,'recovering');
  assert.equal(input.offer(state,{...move(unit),x:8,y:0}).reason,'target');
  assert.equal(input.pending().length,0);
});

test('secondary specials respect troop recovery, their own timer and the army refill',()=>{
  const {state,unit}=battle('leader'),input=createInputController();
  unit.readyAt=5;unit.abilityReadyAt=10;unit.abilityReadyAts=[10,.3,8];
  const action={type:'ability',skillIndex:1,unitId:unit.id,x:1,y:3};
  assert.equal(input.offer(state,action).reason,'recovering');
  unit.readyAt=.2;state.orders.player=0;state.nextOrderAt=.4;
  assert.equal(input.offer(state,action).status,'queued');state.time=.3;
  assert.deepEqual(input.drain(state),[]);state.time=.4;state.orders.player=1;
  const [ready]=input.drain(state);assert.equal(ready.skillIndex,1);assert.equal(applyAction(state,ready).ok,true);
  assert.equal(unit.abilityReadyAt,10);assert.equal(unit.abilityReadyAts[1],8.4);
});

test('an empty army can inspect targets, and one refill releases only one buffered order',()=>{
  const {state,unit}=battle(),input=createInputController();
  const second={...unit,id:'other',x:3,y:3};state.units.push(second);
  state.orders.player=0;state.nextOrderAt=2.4;
  assert.ok(previewActions(state,unit.id).some(a=>a.type==='move'));
  assert.equal(input.offer(state,move(unit)).reason,'orders');
  state.time=2;
  assert.equal(input.offer(state,move(unit)).status,'queued');
  assert.equal(input.offer(state,previewActions(state,second.id).find(a=>a.type==='move')).status,'queued');
  state.time=2.4;state.orders.player=1;
  const ready=input.drain(state);assert.equal(ready.length,1);
  assert.equal(applyAction(state,ready[0]).ok,true);assert.equal(state.orders.player,0);
  assert.deepEqual(input.drain(state),[]);
});

test('a troop has one intent and the latest valid command replaces an earlier one',()=>{
  const {state,unit,enemy}=battle(),input=createInputController();unit.readyAt=.2;
  input.offer(state,move(unit));input.offer(state,attack(unit,enemy));
  assert.equal(input.pending().length,1);assert.equal(input.pending()[0].action.type,'attack');
  state.time=.2;const [ready]=input.drain(state);assert.equal(ready.type,'attack');assert.equal(applyAction(state,ready).ok,true);
});

test('a rejected long-recharge special cannot silently erase an accepted short movement intent',()=>{
  const {state,unit}=battle(),input=createInputController();unit.readyAt=.2;unit.abilityReadyAt=8;unit.abilityReadyAts=[8];
  input.offer(state,move(unit));
  const special=previewActions(state,unit.id).find(action=>action.type==='ability');
  assert.equal(input.offer(state,special).reason,'recovering');
  assert.equal(input.pending()[0].action.type,'move');
});

test('moving, dying, or changing ownership of either combat participant cancels a queued strike',()=>{
  for(const mutation of [
    ({unit})=>unit.x++,({unit})=>unit.hp=0,({unit})=>unit.side='ai',({unit})=>unit.faction='chaos',
    ({enemy})=>enemy.x++,({enemy})=>enemy.hp=0,({enemy})=>enemy.side='player'
  ]){
    const fixture=battle(),{state,unit,enemy}=fixture,input=createInputController();unit.readyAt=.2;
    assert.equal(input.offer(state,attack(unit,enemy)).status,'queued');mutation(fixture);state.time=.2;
    assert.deepEqual(input.drain(state),[]);assert.equal(input.pending().length,0);
  }
});

test('new obstacles, roots, expired orders, pauses, countdowns, and victory cannot fire stale commands',()=>{
  for(const mutation of [
    ({state})=>state.tiles[3*state.width+1].terrain='rock',({unit})=>unit.rootedUntil=3,
    ({state})=>state.time=.6,({state})=>state.winner='player'
  ]){
    const fixture=battle(),{state,unit}=fixture,input=createInputController();unit.readyAt=.2;input.offer(state,move(unit));state.time=.2;mutation(fixture);
    assert.deepEqual(input.drain(state),[]);assert.equal(input.pending().length,0);
  }
  for(const condition of [{paused:true},{countdown:true}]){
    const {state,unit}=battle(),input=createInputController();unit.readyAt=.2;input.offer(state,move(unit));state.time=.2;
    assert.deepEqual(input.drain(state,condition),[]);assert.equal(input.pending().length,0);
    assert.equal(input.offer(state,move(unit),condition).status,'rejected');
  }
});

test('local players retain independent intents and cannot issue orders for the other army',()=>{
  const {state,unit,enemy}=battle(),input=createInputController();unit.readyAt=.2;enemy.readyAt=.3;
  assert.equal(input.offer(state,move(unit),{side:'ai'}).reason,'ownership');
  input.offer(state,move(unit),{side:'player'});
  const enemyMove=previewActions(state,enemy.id).find(action=>action.type==='move');
  assert.equal(input.offer(state,enemyMove,{side:'ai'}).status,'queued');
  state.time=.3;assert.equal(input.drain(state).length,2);
  state.opponentMode='ai';assert.equal(input.offer(state,enemyMove,{side:'ai'}).reason,'ownership');
});

test('pending descriptions are copied, and clearing one troop does not discard another player order',()=>{
  const {state,unit,enemy}=battle(),input=createInputController();unit.readyAt=.2;enemy.readyAt=.2;
  input.offer(state,move(unit));input.offer(state,previewActions(state,enemy.id).find(a=>a.type==='move'),{side:'ai'});
  input.pending()[0].action.x=99;assert.notEqual(input.pending()[0].action.x,99);
  input.clear(unit.id);assert.equal(input.pending().length,1);input.clear();assert.equal(input.pending().length,0);
});

test('simultaneous pointer gestures keep the original troop, owner, command, and special slot',()=>{
  const input=createInputController();
  const first=input.beginPointer(11,{unitId:'u1',side:'player',mode:'ability',skillIndex:2,x:20,y:100});
  input.beginPointer(12,{unitId:'u8',side:'ai',mode:'move',skillIndex:0,x:200,y:20});
  first.unitId='tampered';
  assert.equal(input.movePointer(11,{x:24,y:103}).dragging,false);
  assert.equal(input.movePointer(12,{x:200,y:40}).dragging,true);
  assert.equal(input.movePointer(11,{x:50,y:100}).dragging,true);
  const p2=input.endPointer(12),p1=input.endPointer(11);
  assert.deepEqual([p1.unitId,p1.side,p1.mode,p1.skillIndex],['u1','player','ability',2]);
  assert.deepEqual([p2.unitId,p2.side,p2.mode,p2.skillIndex],['u8','ai','move',0]);
  assert.equal(input.endPointer(11),null);
});

test('cancelling a pointer releases only that gesture and invalid pointer data is ignored',()=>{
  const input=createInputController();
  input.beginPointer(1,{unitId:'u1',side:'player',x:0,y:0});input.beginPointer(2,{unitId:'u2',side:'ai',x:10,y:10});
  input.cancelPointer(1);assert.equal(input.movePointer(1,{x:20,y:20}),null);assert.ok(input.endPointer(2));
  assert.equal(input.beginPointer(3,{unitId:'u1',side:'unknown',x:0,y:0}),null);
  input.beginPointer(4,{unitId:'u1',side:'player',x:0,y:0});input.clearPointers();assert.equal(input.endPointer(4),null);
});

test('a phase reset discards both a held gesture and any short buffered order',()=>{
  const {state,unit}=battle(),input=createInputController();unit.readyAt=.3;
  input.offer(state,move(unit));input.beginPointer(8,{unitId:unit.id,side:'player',x:0,y:0});
  input.reset();assert.equal(input.pending().length,0);assert.equal(input.endPointer(8),null);
  state.time=.3;assert.deepEqual(input.drain(state),[]);
});
