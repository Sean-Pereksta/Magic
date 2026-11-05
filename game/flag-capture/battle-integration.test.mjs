import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {createGame,unitDef,at,advance,applyAction} from './engine.mjs';
import {createInputController,previewActions} from './input.mjs';
import {displayTargets} from './targeting.mjs';

// Run the real app's input handlers and frame scheduler without loading a browser.
// DOM drawing is observed at its boundary; combat, geometry and buffering are real.
const source=readFileSync(new URL('./app.mjs',import.meta.url),'utf8').replace(/\r\n/g,'\n');
function functionSource(name){
  const start=source.indexOf(`function ${name}(`);
  assert.ok(start>=0,`Missing app function ${name}`);
  let end=source.indexOf('\nfunction ',start+1);
  if(name==='frame')end=source.lastIndexOf('\nrender();');
  assert.ok(end>start,`Missing boundary for app function ${name}`);
  return source.slice(start,end);
}
function region(start,end){return source.slice(source.indexOf(start),source.indexOf(end,source.indexOf(start)+start.length));}
function fixture(){
  const game=createGame({playerFaction:'bloom',aiFaction:'chaos',opponentMode:'local',seed:52});
  game.tiles.forEach(tile=>tile.terrain='grass');game.nextPulseAt=99;
  const player=game.units.find(u=>u.side==='player'&&u.role==='scout'),other=game.units.find(u=>u.side==='ai'&&u.role==='scout');
  game.units=[player,other];Object.assign(player,{x:1,y:4});Object.assign(other,{x:6,y:1});
  const handlers=new Map(),calls={render:0,orders:[],events:[],announcements:[],captures:[],pending:[]};
  const classes=()=>({add(){},remove(){}});
  const cell=(x,y)=>({dataset:{cell:`${x},${y}`},classList:classes(),closest(selector){return selector.split(',').some(value=>value==='[data-cell]')?this:null;}});
  const unitTile=u=>({dataset:{cell:`${u.x},${u.y}`,unit:u.id},closest(selector){return selector.split(',').some(value=>value==='.tile[data-unit]'||value==='[data-cell]')?this:null;}});
  const countdown={textContent:'3',parentElement:{classList:classes(),remove(){}}};
  const context={game,input:createInputController(),previewActions,unitDef,at,advance,displayTargets,
    scene:'battle',paused:false,countdownRemainingMs:0,countdownNumber:0,activeSeat:'player',selectedId:player.id,mode:'auto',
    seatSelections:{player:player.id,ai:other.id},seatModes:{player:'auto',ai:'auto'},seatSkills:{player:0,ai:0},selectedSkillIndex:0,
    pointers:new Map(),consumedPointers:new Map(),heldPointers:new Set(),lastFrame:1000,accumulated:0,deferredEvents:[],lastReadySignature:'',
    hit:null,performance:{now:()=>1000},setTimeout:()=>1,requestAnimationFrame(){},
    document:{hidden:false,activeElement:null,querySelector:()=>null,querySelectorAll:()=>[],
      addEventListener(type,callback){handlers.set(type,callback);},elementFromPoint(){return context.hit;},
      createElement(){return {style:{},classList:classes(),remove(){},innerHTML:''};},body:{append(){}}},
    $:selector=>selector==='#app'?{setPointerCapture:id=>calls.captures.push(id)}:selector==='.battle-countdown span'?countdown:{getBoundingClientRect:()=>({x:0,y:0,width:60,height:60})},
    sprite:()=>'<span></span>',previewGroundAt(){},clearGroundPreview(){},announce:message=>calls.announcements.push(message),toast:message=>calls.announcements.push(message),persist(){},updateTimers(){},
    showEvents:events=>calls.events.push(...events),
    applyAction(state,action){calls.orders.push({...action});return applyAction(state,action);},
    render(){calls.render++;calls.pending.push(context.input.pending());context.lastReadySignature=context.readySignature();}
  };
  vm.createContext(context);
  for(const name of ['localGame','current','commandMode','abilities','skillIndex','setMode','selectUnit','remaining','cooldownLabel','targetActions','readySignature','executeOrder','resetInput','perform','frame'])vm.runInContext(functionSource(name),context);
  vm.runInContext(region("document.addEventListener('pointerdown',event=>{\n  const tile=","document.addEventListener('visibilitychange'"),context);
  vm.runInContext(region("document.addEventListener('click'","document.addEventListener('change'"),context);
  context.lastReadySignature=context.readySignature();
  const event=(id,target,x=0,y=0)=>({pointerId:id,target,clientX:x,clientY:y,button:0,preventDefault(){this.prevented=true;}});
  return {context,game,player,other,calls,handlers,cell,unitTile,event};
}

test('a stalled frame drains near-ready commands inside simulation steps before expiry',()=>{
  const {context,game,player,calls}=fixture();player.readyAt=.5;
  const action={type:'move',unitId:player.id,x:1,y:3};
  assert.equal(context.input.offer(game,action).status,'queued');context.lastReadySignature=context.readySignature();
  context.frame(2000);
  assert.equal(player.y,3);assert.equal(calls.orders.length,1);assert.equal(context.input.pending().length,0);
  assert.ok(game.time>.9);assert.equal(calls.render,1);
});

test('a silently invalidated buffered destination immediately redraws its pending marker',()=>{
  const {context,game,player,calls}=fixture();player.readyAt=.4;
  assert.equal(context.input.offer(game,{type:'move',unitId:player.id,x:1,y:3}).status,'queued');
  game.tiles.find(tile=>tile.x===1&&tile.y===3).terrain='rock';context.lastReadySignature=context.readySignature();
  context.frame(1050);
  assert.equal(calls.orders.length,0);assert.equal(context.input.pending().length,0);
  assert.equal(calls.render,1);assert.equal(calls.pending.at(-1).length,0);
});

test('pause and pre-battle countdown cannot advance combat or execute queued commands',()=>{
  const {context,game,player,calls}=fixture();player.readyAt=.2;
  context.input.offer(game,{type:'move',unitId:player.id,x:1,y:3});context.paused=true;
  context.frame(2000);assert.equal(game.time,0);assert.equal(calls.orders.length,0);
  context.resetInput();context.paused=false;context.countdownRemainingMs=3000;context.countdownNumber=3;
  context.frame(3000);assert.equal(game.time,0);assert.equal(context.countdownRemainingMs,2000);assert.equal(calls.orders.length,0);
});

test('simultaneous drags retain each troop and command despite changed active selection',()=>{
  const {context,game,player,other,calls,handlers,cell,unitTile,event}=fixture();
  handlers.get('pointerdown')(event(21,unitTile(player),10,200));
  handlers.get('pointerdown')(event(22,unitTile(other),300,10));
  context.mode='ability';context.seatModes.player='ability';context.seatModes.ai='attack';context.activeSeat='ai';
  context.hit=cell(1,3);handlers.get('pointermove')(event(21,unitTile(player),30,160));
  context.hit=cell(6,2);handlers.get('pointermove')(event(22,unitTile(other),300,60));
  context.hit=cell(1,3);handlers.get('pointerup')(event(21,unitTile(player),30,160));
  context.hit=cell(6,2);handlers.get('pointerup')(event(22,unitTile(other),300,60));
  assert.deepEqual(calls.orders.map(order=>[order.unitId,order.type,order.x,order.y]),[[player.id,'move',1,3],[other.id,'move',6,2]]);
  assert.equal(at(game,1,3).id,player.id);assert.equal(at(game,6,2).id,other.id);
  assert.equal(context.pointers.size,0);assert.deepEqual(calls.captures,[21,22]);
});

test('a compatibility click after dragging cannot overwrite or duplicate its accepted order',()=>{
  const {context,game,player,calls,handlers,cell,unitTile,event}=fixture();player.readyAt=.3;
  handlers.get('pointerdown')(event(31,unitTile(player),20,200));context.hit=cell(1,3);
  handlers.get('pointermove')(event(31,unitTile(player),30,150));handlers.get('pointerup')(event(31,unitTile(player),30,150));
  const before=context.input.pending();assert.equal(before.length,1);
  handlers.get('click')(event(31,cell(2,4),50,200));
  assert.deepEqual(context.input.pending(),before);assert.equal(calls.orders.length,0);
  context.frame(1350);assert.equal(calls.orders.length,1);assert.equal(player.y,3);
});
