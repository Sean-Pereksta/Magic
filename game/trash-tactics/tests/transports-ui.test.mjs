import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';


// Run the real input and sidebar code; graphics and browser timing are the only stubs.
class Element {
  constructor(id='') {
    this.id=id;this.listeners={};this.dataset={};this.style={};this.hidden=false;this.disabled=false;
    this.classList={toggle(){},add(){},remove(){},contains(){return false;}};
  }
  addEventListener(type,fn){(this.listeners[type]??=[]).push(fn);}
  dispatch(type,props={}) {
    const event={type,target:this,currentTarget:this,preventDefault(){},stopPropagation(){},key:'',
      pointerId:1,pointerType:'mouse',button:0,clientX:0,clientY:0,...props};
    for(const fn of this.listeners[type]??[])fn(event);
  }
  setAttribute(){}focus(){}setPointerCapture(){}releasePointerCapture(){}
  hasPointerCapture(){return true;}
  querySelectorAll(){return [];}querySelector(){return null;}closest(){return null;}
  getBoundingClientRect(){return {left:0,top:0,width:800,height:600};}
  getContext(){return {};}
  click(){this.dispatch('click');}
}

let harnessId=0;
async function harness() {
  const elements=new Map(),element=id=>{if(!elements.has(id))elements.set(id,new Element(id));return elements.get(id);};
  element('guide').hidden=true;
  const document={getElementById:element,querySelector:()=>element('sidebar'),querySelectorAll:()=>[],activeElement:null};
  const window=new Element('window');window.innerWidth=1000;window.innerHeight=700;
  let now=10000,nextTimer=1,frame;const timers=new Map();
  const advance=ms=>{now+=ms;for(const [id,timer] of [...timers])if(timer.at<=now){timers.delete(id);timer.fn();}};
  Object.assign(globalThis,{document,window,localStorage:{getItem:()=>null,setItem(){}},performance:{now:()=>now},
    requestAnimationFrame:fn=>{frame=fn;return 1;},
    setTimeout:(fn,ms)=>{const id=nextTimer++;timers.set(id,{at:now+ms,fn});return id;},clearTimeout:id=>timers.delete(id)});
  const fakeRender=`export class Renderer {constructor(canvas,game){this.canvas=canvas;this.game=game;this.camera={zoom:1};this.centers=[];}resize(){}centerOn(x,y){this.centers.push({x,y});}screenToWorld(x,y){return {x,y};}worldToScreen(x,y){return {x,y};}pan(){}zoomAt(){}draw(){}drawMinimap(){}}export function drawIcon(){}`;
  const source=(await readFile(new URL('../game.js',import.meta.url),'utf8'))
    .replace("'./engine.js'",JSON.stringify(new URL('../engine.js',import.meta.url).href))
    .replace("'./ability-icons.js'",JSON.stringify(new URL('../ability-icons.js',import.meta.url).href))
    .replace("'./skirmish-menu.js'",JSON.stringify(new URL('../skirmish-menu.js',import.meta.url).href))
    .replace("'./minimap-alerts.js'",JSON.stringify(new URL('../minimap-alerts.js',import.meta.url).href))
    .replace("'./match-report.js'",JSON.stringify(new URL('../match-report.js',import.meta.url).href))
    .replace("'./render.js'",JSON.stringify('data:text/javascript;base64,'+Buffer.from(fakeRender).toString('base64')));
  await import('data:text/javascript;base64,'+Buffer.from(source+`\n//# sourceURL=trash-tactics-transports-under-test-${++harnessId}.js`).toString('base64'));
  element('startButton').click();
  const state=window.trashTactics,game=state.game;
  game._think=Infinity;game.entities=game.entities.filter(e=>e.type==='desk');game._collisionBuildings=null;game.map.obstacles=[];
  const point=(x,y,more={})=>{const event={clientX:x,clientY:y,...more};element('world').dispatch('pointerdown',event);element('world').dispatch('pointerup',event);};
  const select=(unit,more={})=>point(unit.x,unit.y,more);
  const key=(key,more={})=>{window.dispatch('keydown',{key,...more});window.dispatch('keyup',{key,...more});};
  const refresh=()=>{for(let i=0;i<6;i++){advance(50);frame(now);}};
  const tick=seconds=>{for(let remaining=seconds;remaining>0;remaining-=.05)game.update(Math.min(.05,remaining));refresh();};
  const action=(action,type)=>{const button={dataset:{action,type},disabled:false,closest(){return this;}};element('actionContent').dispatch('click',{target:button});};
  const spawn=(type,x,y,team=0)=>game._spawn(type,team,x,y);
  return {state,game,element,select,point,key,tick,refresh,action,spawn};
}

test('a worker builds a CD reader and its production controls summon a flying CD',async()=>{
  const h=await harness(),worker=h.spawn('twig',700,800);
  h.select(worker);
  assert.match(h.element('actionContent').innerHTML,/data-type="cdreader"/,'reader is available in the build menu');
  h.key('v');h.point(900,800);
  const reader=h.game.entities.find(e=>e.type==='cdreader');
  assert(reader,'V and a ground click place the reader blueprint');
  assert(reader.constructing>0);
  h.tick(15);
  assert.equal(reader.constructing,0);
  h.select(reader);
  assert.match(h.element('actionContent').innerHTML,/data-type="cd"/,'reader exposes the CD summon button and icon');
  h.key('q');
  assert.equal(reader.queue[0]?.type,'cd');
  h.tick(9);
  const cd=h.game.entities.find(e=>e.type==='cd');
  assert(cd,'the queued summon produces a CD through normal simulation');
  h.select(cd);
  assert.match(h.element('actionContent').innerHTML,/data-action="load"/);
  assert.match(h.element('actionContent').innerHTML,/data-action="drop"/);
});

test('mouse and touch CD taps board troops, exclude twigs, and keep groups usable in flight',async()=>{
  const h=await harness(),cd=h.spawn('cd',850,800),rock=h.spawn('rock',790,800),paper=h.spawn('paper',850,860),twig=h.spawn('twig',910,800);
  h.select(rock);h.key('1',{ctrlKey:true});
  h.select(cd);
  assert(rock.order.type==='board'||rock.transportedBy===cd.id,'left clicking the CD with a troop selected orders boarding');
  h.tick(1);
  assert.equal(rock.transportedBy,cd.id);
  h.key('x');h.select(paper,{pointerType:'touch'});h.select(cd,{pointerType:'touch'});h.tick(1);
  assert.equal(paper.transportedBy,cd.id,'touch uses the same boarding command');
  h.key('x');h.select(twig);h.select(cd);h.tick(.3);
  assert(!twig.transportedBy,'workers cannot become cargo');
  h.key('f2');
  assert(!h.state.selected.includes(rock.id)&&!h.state.selected.includes(paper.id),'army selection excludes embarked units');
  h.key('1');
  assert.deepEqual(h.state.selected,[cd.id],'recalling an embarked troop selects its visible transport');
  assert.match(h.element('selectionInfo').innerHTML+h.element('actionContent').innerHTML,/2\s*\/\s*6/,'sidebar displays the occupied cargo slots');
  // A passenger shares the CD position, but clicking there must select the CD.
  h.key('x');h.select(cd);
  assert.deepEqual(h.state.selected,[cd.id]);
  h.key('u');h.point(1100,800);h.tick(3);h.key('1');
  assert.deepEqual(h.state.selected,[rock.id],'the same group returns to its original troops after landing');
});

test('Load fills selected CDs from nearest troops and one Drop action unloads every selected CD',async()=>{
  const h=await harness(),a=h.spawn('cd',800,800),b=h.spawn('cd',1000,800);
  const troops=[];
  for(let i=0;i<6;i++){troops.push(h.spawn('rock',750+(i%2)*35,700+Math.floor(i/2)*45));troops.push(h.spawn('paper',1020+(i%2)*35,700+Math.floor(i/2)*45));}
  const twig=h.spawn('twig',800,820);
  h.select(a);h.select(b,{shiftKey:true});
  assert.deepEqual(new Set(h.state.selected),new Set([a.id,b.id]));
  h.action('load');h.tick(4);
  assert.equal(a.cargo.length,6);assert.equal(b.cargo.length,6);
  assert(!twig.transportedBy);
  assert(troops.every(e=>e.transportedBy===a.id||e.transportedBy===b.id));
  h.action('drop');h.point(1300,800,{pointerType:'touch'});
  assert.equal(a.order.type,'drop');assert.equal(b.order.type,'drop');
  h.tick(5);
  assert.equal(a.cargo.length,0);assert.equal(b.cargo.length,0);
  assert(troops.every(e=>!e.transportedBy));
  assert(troops.every(e=>Math.hypot(e.x-1300,e.y-800)<220),'all passengers disembark at the commanded area');
});

test('L loads nearest troops, U targets the minimap, and an empty Load reports why',async()=>{
  const h=await harness(),cd=h.spawn('cd',800,800),rock=h.spawn('rock',780,750);
  h.select(cd);h.key('l');h.tick(1);
  assert.equal(rock.transportedBy,cd.id);
  h.key('u');
  // Map a nearby drop zone through the minimap's current world dimensions.
  h.element('minimap').dispatch('pointerdown',{clientX:1200/h.game.width*800,clientY:800/h.game.height*600,pointerType:'touch'});
  assert.equal(cd.order.type,'drop');
  h.tick(4);
  assert.equal(cd.cargo.length,0);assert(!rock.transportedBy);
  // Remove the only troop and verify an unavailable Load gets actionable feedback.
  rock.hp=0;h.select(cd);h.key('l');
  assert.match(h.element('toast').textContent,/troop|load|near|board/i);
  assert.equal(cd.cargo.length,0);
});

test('the defensive mouse is available through worker controls and shows its purpose after construction',async()=>{
  const h=await harness(),worker=h.spawn('twig',700,800);
  h.select(worker);
  assert.match(h.element('actionContent').innerHTML,/data-type="mouse"/);
  h.key('m');h.point(900,800);h.tick(12);
  const mouse=h.game.entities.find(e=>e.type==='mouse');
  assert(mouse);assert.equal(mouse.constructing,0);
  h.select(mouse);
  assert.match(h.element('selectionInfo').innerHTML,/data-icon="mouse"/);
  assert.match(h.element('actionContent').innerHTML,/Pointer defense/);
  const enemy=h.spawn('cd',1150,800,1);h.tick(1);
  assert(enemy.hp<enemy.maxHp,'the mouse placed through the real UI can defend against CDs');
});
