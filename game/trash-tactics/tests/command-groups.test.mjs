import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';

// Exercise the actual game input handlers without needing a browser or graphics assets.
class Element {
  constructor(id='') {
    this.id=id; this.listeners={}; this.dataset={}; this.style={}; this.hidden=false;
    this.classList={toggle(){},add(){},remove(){},contains(){return false;}};
    this.disabled=false;
  }
  addEventListener(type,fn){(this.listeners[type]??=[]).push(fn);}
  dispatch(type,props={}){
    const event={type,target:this,currentTarget:this,preventDefault(){},stopPropagation(){},
      key:'',pointerId:1,pointerType:'touch',button:0,clientX:0,clientY:0,...props};
    for(const fn of this.listeners[type]??[])fn(event);
  }
  setAttribute(){} focus(){} setPointerCapture(){} releasePointerCapture(){}
  hasPointerCapture(){return true;}
  querySelectorAll(){return [];} querySelector(){return null;} closest(){return null;}
  getBoundingClientRect(){return {left:0,top:0,width:800,height:600};}
  getContext(){return {};}
  click(){this.dispatch('click');}
}

async function harness(){
  const elements=new Map(), element=id=>{if(!elements.has(id))elements.set(id,new Element(id));return elements.get(id);};
  element('guide').hidden=true;
  const document={getElementById:element,querySelector:()=>element('sidebar'),querySelectorAll:()=>[],activeElement:null};
  const window=new Element('window');window.innerWidth=1000;window.innerHeight=700;
  let now=10000,nextTimer=1; const timers=new Map();
  function advance(ms){
    const end=now+ms;
    while(true){let first=null;for(const [id,t] of timers)if(t.at<=end&&(!first||t.at<first[1].at))first=[id,t];
      if(!first)break;now=first[1].at;timers.delete(first[0]);first[1].fn();}
    now=end;
  }
  Object.assign(globalThis,{document,window,localStorage:{getItem:()=>null,setItem(){}},
    performance:{now:()=>now},requestAnimationFrame:()=>0,
    setTimeout:(fn,ms)=>{const id=nextTimer++;timers.set(id,{at:now+ms,fn});return id;},
    clearTimeout:id=>timers.delete(id)});
  const fakeRender=`export class Renderer { constructor(canvas,game){this.canvas=canvas;this.game=game;this.camera={zoom:1};this.centers=[];} resize(){} centerOn(x,y){this.centers.push({x,y});} screenToWorld(x,y){return {x,y};} worldToScreen(x,y){return {x,y};} pan(){} zoomAt(){} draw(){} drawMinimap(){} } export function drawIcon(){}`;
  const renderUrl='data:text/javascript;base64,'+Buffer.from(fakeRender).toString('base64');
  const source=(await readFile(new URL('../game.js',import.meta.url),'utf8'))
    .replace("'./engine.js'",JSON.stringify(pathToFileURL(new URL('../engine.js',import.meta.url).pathname).href))
    .replace("'./render.js'",JSON.stringify(renderUrl));
  await import('data:text/javascript;base64,'+Buffer.from(source+'\n//# sourceURL=trash-tactics-game-under-test.js').toString('base64'));
  element('startButton').click();
  const state=window.trashTactics;
  const key=(type,key,more={})=>window.dispatch(type,{key,...more});
  const pointer=(slot,type,more={})=>element('group-'+slot).dispatch(type,more);
  const select=unit=>{const canvas=element('world');const event={pointerType:'mouse',clientX:unit.x,clientY:unit.y};canvas.dispatch('pointerdown',event);canvas.dispatch('pointerup',event);};
  return {state,element,key,pointer,select,advance};
}

test('command groups save, recall, center, prune, reset, and cancel input',async()=>{
  const h=await harness();
  const unit=h.state.game.entities.find(e=>e.team===0&&e.type==='rock');
  const other=h.state.game.entities.find(e=>e.team===0&&e.type==='paper');
  h.select(unit);
  assert.deepEqual(h.state.selected,[unit.id]);
  h.pointer(1,'pointerdown');h.advance(1000);h.pointer(1,'pointerup');
  h.select(other);
  h.pointer(1,'pointerdown');h.pointer(1,'pointerup');
  assert.deepEqual(h.state.selected,[unit.id],'tap recalls a held group');
  const centers=h.state.renderer.centers.length;
  h.advance(100);h.pointer(1,'pointerdown');h.pointer(1,'pointerup');
  assert.deepEqual(h.state.renderer.centers.at(-1),{x:unit.x,y:unit.y},'double tap centers on the group');
  assert.equal(h.state.renderer.centers.length,centers+1);

  h.select(other);h.key('keydown','2');h.advance(1000);h.key('keyup','2');
  h.select(unit);h.key('keydown','2');h.key('keyup','2');
  assert.deepEqual(h.state.selected,[other.id],'keyboard hold saves and key tap recalls');
  h.advance(100);h.key('keydown','2');h.key('keyup','2');
  assert.deepEqual(h.state.renderer.centers.at(-1),{x:other.x,y:other.y},'double key centers on the group');

  // Auto-repeat while holding is ignored and must not trigger recall or camera movement.
  h.select(unit);const beforeRepeat=h.state.renderer.centers.length;
  h.key('keydown','3');h.key('keydown','3',{repeat:true});h.advance(1000);h.key('keyup','3');
  assert.equal(h.state.renderer.centers.length,beforeRepeat);
  h.select(other);h.key('keydown','3');h.key('keyup','3');
  assert.deepEqual(h.state.selected,[unit.id]);
  // An empty/building selection must not erase an assigned group.
  const desk=h.state.game.entities.find(e=>e.team===0&&e.type==='desk');
  h.select(desk);h.pointer(2,'pointerdown');h.advance(1000);h.pointer(2,'pointerup');
  h.key('keydown','2');h.key('keyup','2');
  assert.deepEqual(h.state.selected,[other.id]);
  // Pointer cancellation and movement should never replace the group.
  h.select(unit);h.pointer(2,'pointerdown');h.pointer(2,'pointercancel');h.advance(1000);
  h.key('keydown','2');h.key('keyup','2');
  assert.deepEqual(h.state.selected,[other.id],'cancelled pointer hold does not overwrite group');
  h.pointer(2,'pointerdown');h.pointer(2,'pointermove',{clientX:30});h.advance(1000);h.pointer(2,'pointerup');
  h.key('keydown','2');h.key('keyup','2');assert.deepEqual(h.state.selected,[other.id]);
  // Browser-synthesized pointer clicks do not activate a second time after release.
  const beforeClick=h.state.renderer.centers.length;
  h.element('group-2').dispatch('click',{detail:1});
  assert.equal(h.state.renderer.centers.length,beforeClick);
  // Ctrl/Meta assignment remains available, and a blur cancels a pending hold.
  h.select(unit);h.key('keydown','4',{ctrlKey:true});h.key('keyup','4');
  h.select(other);h.key('keydown','4');h.key('keyup','4');assert.deepEqual(h.state.selected,[unit.id]);
  h.select(other);h.key('keydown','4');h.state.renderer.centers.length=0;
  globalThis.window.dispatch('blur');h.advance(1000);h.key('keyup','4');
  h.key('keydown','4');h.key('keyup','4');assert.deepEqual(h.state.selected,[unit.id]);
  unit.hp=0;h.select(other);h.pointer(1,'pointerdown');h.pointer(1,'pointerup');
  assert(!h.state.selected.includes(unit.id),'dead troops are not recalled');
  h.element('startButton').click();h.select(h.state.game.entities.find(e=>e.team===0&&e.type==='paper'));
  const selected=h.state.selected.slice();h.pointer(2,'pointerdown');h.pointer(2,'pointerup');
  assert.deepEqual(h.state.selected,selected,'starting another match clears old groups');
});
