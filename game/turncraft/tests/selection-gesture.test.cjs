const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

// Exercise the real battlefield handlers with deterministic pointer events and timers.
const html=fs.readFileSync(path.resolve(__dirname,'../../turncraft.html'),'utf8');
const input=html.slice(html.indexOf('function visibleOnScreen('),html.indexOf("window.addEventListener('keyup'"));
function setup(){
  const listeners=new Map(),windowListeners=new Map(),timers=new Map(),bodyClasses=new Set(),canvasClasses=new Set(),taps=[],selections=[];
  let clock=0,nextTimer=0,sSelected=[];
  const classes=set=>({add:name=>set.add(name),remove:name=>set.delete(name)});
  const troops=[
    {id:1,kind:'unit',type:'engineer',faction:0,x:2,y:3},
    {id:2,kind:'unit',type:'marine',faction:0,x:5,y:6},
    {id:3,kind:'unit',type:'marine',faction:1,x:4,y:4},
    {id:4,kind:'structure',type:'barracks',faction:0,x:3,y:3},
    {id:5,kind:'unit',type:'marine',faction:0,x:4,y:3,alive:false},
    {id:6,kind:'unit',type:'marine',faction:0,x:3,y:4,garrisonedIn:4}
  ];
  const context={
    canvas:{addEventListener:(name,fn)=>listeners.set(name,fn),setPointerCapture:()=>{},classList:classes(canvasClasses)},
    window:{addEventListener:(name,fn)=>windowListeners.set(name,fn)},document:{body:{classList:classes(bodyClasses)}},
    gameStarted:true,paused:false,gameOver:false,PLAYER:0,W:500,H:500,MIN_ZOOM:.25,MAX_ZOOM:2.4,
    TILE:20,lastStackTap:{key:'',idx:0,at:0},isFlying:()=>false,spriteEntityVisible:e=>e?.alive!==false&&!e?.garrisonedIn,
    cam:{x:0,y:0,zoom:1},entities:troops,UNITS:{marine:{name:'Marine'},engineer:{name:'Engineer'}},
    dragging:false,dragStart:null,pointerMoved:false,activePointers:new Map(),pinch:null,selectionBox:null,selectionHoldTimer:null,
    lastCanvasTap:{at:0,entityId:null},interactionMode:null,interactionPayload:null,legalTiles:[],hoverTile:null,
    setTimeout:(fn,ms)=>{const id=nextTimer++;timers.set(id,{fn,at:clock+ms});return id},clearTimeout:id=>timers.delete(id),
    nowMs:()=>clock,clamp:(n,min,max)=>Math.min(max,Math.max(min,n)),clampCam:()=>{},toast:()=>{},
    worldToScreen:(x,y)=>({x:x*20*context.cam.zoom-context.cam.x,y:y*20*context.cam.zoom-context.cam.y}),
    screenToTile:(x,y)=>({x:Math.floor((x+context.cam.x)/(20*context.cam.zoom)),y:Math.floor((y+context.cam.y)/(20*context.cam.zoom))}),
    entityVisibleToPlayer:e=>e?.alive!==false&&!e?.garrisonedIn,
    inBounds:(x,y)=>x>=0&&y>=0&&x<30&&y<30,isMultiSelectable:e=>e?.faction===0&&e.kind==='unit',
    entityById:id=>troops.find(e=>e.id===id&&e.alive!==false)||null,
    selectableEntityAt:(x,y)=>troops.find(e=>e.x===x&&e.y===y&&e.alive!==false&&!e.garrisonedIn)||null,
    selectedTroops:()=>troops.filter(e=>sSelected.includes(e.id)&&e.kind==='unit'&&e.faction===0),
    selectEntities:list=>{sSelected=list.map(e=>e.id);selections.push(sSelected);context.interactionMode=null},
    selectEntity:unit=>selections.push(unit?[unit.id]:[]),
    handlePolishTap:(x,y)=>taps.push({x,y,mode:context.interactionMode}),requestZoom:()=>{},refreshUI:()=>{}
  };
  vm.runInNewContext(input,context,{filename:'turncraft-input.js'});
  return {
    context,taps,selections,timers,bodyClasses,canvasClasses,
    dispatch(type,x=10,y=10,options={}){const event={pointerId:1,pointerType:'mouse',button:0,clientX:x,clientY:y,shiftKey:false,...options};listeners.get(type)(event)},
    advance(ms){clock+=ms;for(const [id,timer] of [...timers])if(timer.at<=clock){timers.delete(id);timer.fn()}},
    blur(){windowListeners.get('blur')()},
    selectedIds(){return Array.from(selections.at(-1)||[])}
  };
}

test('stationary mouse hold boxes every friendly troop in the area without panning or issuing orders',()=>{
  const s=setup();
  s.dispatch('pointerdown');s.dispatch('pointermove',13,13);s.advance(379);
  assert.equal(s.context.selectionBox,null);
  s.advance(1);assert.equal(s.bodyClasses.has('selection-active'),true);
  s.dispatch('pointermove',145,155);s.dispatch('pointerup',145,155);
  assert.deepEqual(s.selectedIds(),[1,2]);assert.deepEqual(s.context.cam,{x:0,y:0,zoom:1});
  assert.deepEqual(s.taps,[]);assert.equal(s.context.activePointers.size,0);
  assert.equal(s.bodyClasses.size,0);assert.equal(s.canvasClasses.size,0);assert.equal(s.timers.size,0);
});

test('touch hold supports reverse-direction box selection and consumes a held target tap',()=>{
  const s=setup(),touch={pointerType:'touch'};
  s.dispatch('pointerdown',145,155,touch);s.advance(380);s.dispatch('pointermove',10,10,touch);s.dispatch('pointerup',10,10,touch);
  assert.deepEqual(s.selectedIds(),[1,2]);assert.deepEqual(s.taps,[]);assert.equal(s.context.interactionMode,null);
  s.context.interactionMode='build';s.dispatch('pointerdown',150,160,touch);s.advance(380);s.dispatch('pointermove',10,10,touch);s.dispatch('pointerup',10,10,touch);
  assert.deepEqual(s.taps,[]);assert.equal(s.context.interactionMode,'build','an armed build consumes a held drag without selecting or placing');
});

test('immediate drag pans, Shift-drag selects immediately, and short taps retain command behavior',()=>{
  const pan=setup();pan.dispatch('pointerdown',100,100);pan.dispatch('pointermove',120,125);pan.advance(500);pan.dispatch('pointerup',120,125);
  assert.deepEqual(pan.context.cam,{x:-20,y:-25,zoom:1});assert.deepEqual(pan.selections,[]);assert.deepEqual(pan.taps,[]);
  const shift=setup();shift.dispatch('pointerdown',10,10,{shiftKey:true});shift.dispatch('pointermove',145,155);shift.dispatch('pointerup',145,155);
  assert.deepEqual(shift.selectedIds(),[1,2]);assert.equal(shift.timers.size,0);
  const tap=setup();tap.dispatch('pointerdown',50,70);tap.advance(100);tap.dispatch('pointerup',50,70);tap.advance(500);
  assert.deepEqual(tap.taps,[{x:50,y:70,mode:null}]);assert.equal(tap.context.selectionBox,null);
  tap.dispatch('pointerdown',50,70,{pointerType:'touch'});tap.dispatch('pointerup',50,70,{pointerType:'touch'});
  assert.equal(tap.taps.length,2,'unarmed touch follows contextual selection/order routing');
  tap.context.interactionMode='move';tap.dispatch('pointerdown',160,180,{pointerType:'touch'});tap.dispatch('pointerup',160,180,{pointerType:'touch'});
  assert.equal(tap.taps.at(-1).mode,'move');
  tap.dispatch('pointerdown',160,180,{shiftKey:true});tap.dispatch('pointerup',160,180,{shiftKey:true});
  assert.equal(tap.taps.length,4,'Shift-click without dragging still follows the normal tap path');
  const released=setup();released.dispatch('pointerdown',10,10);released.dispatch('pointerup',145,155);
  assert.deepEqual(released.taps,[],'a displaced release is a drag even when no move event arrived');
});

test('pinch cancels pending and active boxes and never turns its final finger into a tap',()=>{
  for(const held of [false,true]){
    const s=setup(),touch={pointerType:'touch'};
    s.dispatch('pointerdown',100,100,touch);if(held)s.advance(380);
    s.dispatch('pointerdown',200,100,{...touch,pointerId:2});s.advance(500);
    assert.equal(s.context.selectionBox,null);assert.equal(s.bodyClasses.size,0);assert.equal(s.timers.size,0);
    s.dispatch('pointermove',250,100,{...touch,pointerId:2});assert.equal(s.context.cam.zoom,1.5);
    s.dispatch('pointerup',250,100,{...touch,pointerId:2});s.dispatch('pointermove',120,120,touch);s.dispatch('pointerup',120,120,touch);
    assert.deepEqual(s.taps,[]);assert.deepEqual(s.selections,[]);assert.equal(s.context.activePointers.size,0);
  }
});

test('cancellation, lost capture and blur clear every hold timer and box',()=>{
  for(const cancel of ['pointercancel','lostpointercapture','blur'])for(const held of [false,true]){
    const s=setup();s.dispatch('pointerdown');if(held)s.advance(380);
    if(cancel==='blur')s.blur();else s.dispatch(cancel);
    s.advance(500);s.dispatch('pointerup',145,155);
    assert.equal(s.context.selectionBox,null);assert.equal(s.context.activePointers.size,0);assert.equal(s.timers.size,0);
    assert.equal(s.bodyClasses.size,0);assert.deepEqual(s.taps,[]);assert.deepEqual(s.selections,[]);
  }
});


test('stationary touch hold toggles an owned troop while hold-drag boxes and enemy holds do not issue orders',()=>{
 const s=setup(),touch={pointerType:'touch'};
 const hold=(x,y)=>{s.dispatch('pointerdown',x,y,touch);s.advance(380);s.dispatch('pointerup',x,y,touch)};
 hold(50,70);assert.deepEqual(s.selectedIds(),[1]);
 hold(110,130);assert.deepEqual(s.selectedIds(),[1,2]);
 hold(50,70);assert.deepEqual(s.selectedIds(),[2]);
 hold(90,90);assert.deepEqual(s.selectedIds(),[2]);assert.deepEqual(s.taps,[]);
 s.context.interactionMode='ability';hold(50,70);assert.deepEqual(s.selectedIds(),[2]);assert.equal(s.context.interactionMode,'ability');
 s.context.interactionMode=null;s.dispatch('pointerdown',10,10,touch);s.advance(380);s.dispatch('pointermove',145,155,touch);s.dispatch('pointerup',145,155,touch);assert.deepEqual(s.selectedIds(),[1,2]);assert.deepEqual(s.taps,[]);
});


test('every armed mode suppresses contextual holds, held boxes and pinches, including modes armed during a hold',()=>{
 for(const mode of ['ability','build','mine','rally','attack','attackMove','assistBuild']){
  const s=setup(),touch={pointerType:'touch'};s.context.interactionMode=mode;
  s.dispatch('pointerdown',10,10,touch);s.advance(380);s.dispatch('pointermove',145,155,touch);s.dispatch('pointerup',145,155,touch);
  assert.equal(s.context.interactionMode,mode);assert.deepEqual(s.selections,[]);assert.deepEqual(s.taps,[]);
  s.dispatch('pointerdown',10,10,touch);s.dispatch('pointerdown',110,10,{...touch,pointerId:2});s.dispatch('pointerup',110,10,{...touch,pointerId:2});s.dispatch('pointerup',10,10,touch);
  assert.equal(s.context.interactionMode,mode);assert.deepEqual(s.taps,[]);
 }
 const later=setup(),touch={pointerType:'touch'};later.dispatch('pointerdown',10,10,touch);later.advance(380);later.context.interactionMode='ability';later.dispatch('pointermove',145,155,touch);later.dispatch('pointerup',145,155,touch);
 assert.equal(later.context.interactionMode,'ability');assert.deepEqual(later.selections,[]);assert.deepEqual(later.taps,[]);
});


test('stationary hold follows the initially pressed moving troop rather than a different body that moves under the finger',()=>{
 const s=setup(),touch={pointerType:'touch'};s.dispatch('pointerdown',50,70,touch);
 s.context.entities[0].px=8;s.context.entities[0].py=3;s.context.entities[1].px=2;s.context.entities[1].py=3;
 s.advance(380);s.dispatch('pointerup',50,70,touch);assert.deepEqual(s.selectedIds(),[1]);assert.deepEqual(s.taps,[]);
});
