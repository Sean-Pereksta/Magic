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
  const fakeRender=`export class Renderer {constructor(canvas,game){this.canvas=canvas;this.game=game;this.camera={zoom:1};this.centers=[];}resize(){}centerOn(x,y){this.centers.push({x,y});}screenToWorld(x,y){return {x,y};}worldToScreen(x,y){return {x,y};}pan(dx,dy){(this.pans??=[]).push({dx,dy});}zoomAt(){}draw(state){this.lastDraw=state;}drawMinimap(){}}export function drawIcon(){}`;
  const source=(await readFile(new URL('../game.js',import.meta.url),'utf8'))
    .replace("'./engine.js'",JSON.stringify(new URL('../engine.js',import.meta.url).href))
    .replace("'./render.js'",JSON.stringify('data:text/javascript;base64,'+Buffer.from(fakeRender).toString('base64')));
  await import('data:text/javascript;base64,'+Buffer.from(source+`\n//# sourceURL=trash-tactics-abilities-under-test-${++harnessId}.js`).toString('base64'));
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

test('Q, W, E activate only their troop ability in a mixed squad and expose their exact keys',async()=>{
 const h=await harness(),rock=h.spawn('rock',600,800),paper=h.spawn('paper',600,900),scissors=h.spawn('scissors',600,1000);
 Object.assign(h.game.teams[0].upgrades,{rock:true,paper:true,scissors:true});
 h.select(rock);h.select(paper,{shiftKey:true});h.select(scissors,{shiftKey:true});
 const html=h.element('actionContent').innerHTML;
 for(const [type,key] of [['rock','Q'],['paper','W'],['scissors','E']]){
  assert.match(html,new RegExp(`data-type="${type}"[^>]*aria-keyshortcuts="${key}"`));
 }
 h.key('q');h.refresh();
 assert.equal(h.state.renderer.lastDraw.abilityType,'rock','preview receives the selected ability type');
 h.point(720,800);
 assert(rock.leap,'Q and ground click launch the rock');
 assert.equal(paper.paperInvulnerable,0,'Launch does not also crumple the paper');
 assert.equal(scissors.abilityCooldown,0,'Launch does not also sweep with scissors');
 assert.match(h.element('actionContent').innerHTML,/Recharging/,'cast updates cooldown feedback immediately');
 h.key('w');
 assert.equal(paper.paperInvulnerable,3,'W crumples paper immediately without a target click');
 assert.equal(scissors.abilityCooldown,0);
 h.key('e');h.refresh();assert.equal(h.state.renderer.lastDraw.abilityType,'scissors');
 h.point(750,1000);assert.equal(scissors.abilityCooldown,10);
});

test('locked and cooling abilities cannot enter targeting mode, and key repeat or modifiers never cast',async()=>{
 const h=await harness(),paper=h.spawn('paper',650,800),rock=h.spawn('rock',650,900);
 h.select(paper);h.key('w');assert.equal(paper.paperInvulnerable,0);
 assert.match(h.element('toast').textContent,/Research Crumple/);
 h.game.teams[0].upgrades.paper=true;h.select(paper);
 for(const more of [{ctrlKey:true},{metaKey:true},{altKey:true},{repeat:true},{isComposing:true}])h.key('w',more);
 assert.equal(paper.paperInvulnerable,0,'browser shortcuts, composition and held keys are ignored');
 globalThis.document.activeElement={isContentEditable:true};h.key('w');
 assert.equal(paper.paperInvulnerable,0,'editing text never issues commands');
 globalThis.document.activeElement=null;h.key('w');assert.equal(paper.paperInvulnerable,3);
 paper.paperInvulnerable=1;h.key('w');assert.equal(paper.paperInvulnerable,1,'cooldown prevents repeated cast');
 assert.match(h.element('toast').textContent,/recharging/);
 h.select(rock);h.game.teams[0].upgrades.rock=true;rock.abilityCooldown=4;h.key('q');h.refresh();
 assert.equal(h.state.renderer.lastDraw.mode,null,'cooling targeted ability never arms the cursor');
});

test('production keys remain contextual and the previous scissors training key still works',async()=>{
 const h=await harness(),printer=h.spawn('printer',700,800);
 h.game.teams[0].trash=1000;h.select(printer);
 for(const key of ['q','w','e','r'])h.key(key);
 assert.deepEqual(printer.queue.map(q=>q.type),['rock','paper','scissors','scissors']);
 const desk=h.game.entities.find(e=>e.type==='desk'&&e.team===0);h.select(desk);h.key('q');
 assert.equal(desk.queue[0].type,'twig');
});

test('pointer, nearest-attack, and construction controls never reveal hidden enemies or ground',async()=>{
 const h=await harness(),rock=h.spawn('rock',700,800),enemy=h.spawn('scissors',900,800,1),worker=h.spawn('twig',700,1000);
 h.game.isVisible=e=>e.team===0;h.game.canSee=()=>false;
 h.select(rock);h.point(enemy.x,enemy.y,{button:2});
 assert.equal(rock.order.type,'move','right clicking an unseen enemy is a ground move');
 assert(!rock.order.targetId,'hidden enemy ID never enters the order');
 h.action('attack');
 assert.match(h.element('toast').textContent,/No visible enemy/);
 h.select(worker);h.key('b');h.point(900,1000);
 assert.equal(h.game.entities.filter(e=>e.type==='printer').length,0);
 assert.match(h.element('toast').textContent,/Scout this ground/);
 h.key('a');h.point(1200,1000,{button:2});
 assert.equal(worker.order.type,'attackMove','players can scout unknown ground with attack move');
});

test('desktop edge scrolling pauses over HUD, touch, menus, and drags',async()=>{
 const h=await harness(),canvas=h.element('world');
 canvas.dispatch('pointermove',{clientX:795,clientY:300});h.refresh();
 assert(h.state.renderer.pans.some(p=>p.dx<0),'hovering the right battlefield edge scrolls right');
 const count=()=>h.state.renderer.pans.length;
 canvas.dispatch('pointerleave');let before=count();h.refresh();assert.equal(count(),before,'moving onto HUD stops edge scroll');
 canvas.dispatch('pointermove',{clientX:5,clientY:300,pointerType:'touch'});h.refresh();assert.equal(count(),before,'touch does not edge scroll');
 canvas.dispatch('pointermove',{clientX:5,clientY:300});
 canvas.dispatch('pointerdown',{clientX:5,clientY:300});h.refresh();assert.equal(count(),before,'a held pointer cannot accidentally edge scroll');
 canvas.dispatch('pointercancel');canvas.dispatch('pointermove',{clientX:5,clientY:300});h.key('p');
 before=count();h.refresh();assert.equal(count(),before,'paused menus stop camera motion');
 h.key('p');h.refresh();assert.equal(count(),before,'resuming requires a fresh battlefield hover');
});

test('minimap click and drag use current map dimensions and clamp outside captured moves',async()=>{
 const h=await harness(),minimap=h.element('minimap');
 h.game.width=4800;h.game.height=3200;
 minimap.dispatch('pointerdown',{clientX:400,clientY:300});
 assert.deepEqual(h.state.renderer.centers.at(-1),{x:2400,y:1600});
 minimap.dispatch('pointermove',{clientX:600,clientY:450});
 assert.deepEqual(h.state.renderer.centers.at(-1),{x:3600,y:2400});
 minimap.dispatch('pointermove',{clientX:1000,clientY:-50});
 assert.deepEqual(h.state.renderer.centers.at(-1),{x:4800,y:0});
 minimap.dispatch('pointerup');const count=h.state.renderer.centers.length;
 minimap.dispatch('pointermove',{clientX:0,clientY:0});
 assert.equal(h.state.renderer.centers.length,count,'hovering minimap after release never moves the camera');
});

