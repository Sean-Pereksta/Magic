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
    .replace("'./ability-icons.js'",JSON.stringify(new URL('../ability-icons.js',import.meta.url).href))
    .replace("'./render.js'",JSON.stringify('data:text/javascript;base64,'+Buffer.from(fakeRender).toString('base64')));
  await import('data:text/javascript;base64,'+Buffer.from(source+`\n//# sourceURL=trash-tactics-abilities-under-test-${++harnessId}.js`).toString('base64'));
  element('startButton').click();
  const state=window.trashTactics,game=state.game;
  game._think=Infinity;game.entities=game.entities.filter(e=>e.type==='desk');game._collisionBuildings=null;game.map.obstacles=[];game.map.plateaus=[];game.map.ramps=[];game.canSee=()=>true;game.isVisible=()=>true;
  const point=(x,y,more={})=>{const event={clientX:x,clientY:y,...more};element('world').dispatch('pointerdown',event);element('world').dispatch('pointerup',event);};
  const select=(unit,more={})=>point(unit.x,unit.y,more);
  const key=(key,more={})=>{window.dispatch('keydown',{key,...more});window.dispatch('keyup',{key,...more});};
  const refresh=()=>{for(let i=0;i<6;i++){advance(50);frame(now);}};
  const tick=seconds=>{for(let remaining=seconds;remaining>0;remaining-=.05)game.update(Math.min(.05,remaining));refresh();};
  const action=(action,type)=>{const button={dataset:{action,type},disabled:false,closest(){return this;}};element('actionContent').dispatch('click',{target:button});};
  const spawn=(type,x,y,team=0)=>game._spawn(type,team,x,y);
  return {state,game,element,select,point,key,tick,refresh,action,spawn};
}


test('specialist production menus show real costs, supply and contextual training shortcuts',async()=>{
 const h=await harness();h.game.teams[0].trash=5000;
 const workshop=h.spawn('workshop',600,800),lab=h.spawn('laboratory',1000,800);
 h.select(workshop);
 const html=h.element('actionContent').innerHTML;
 assert.match(html,/Scissors Sniper/);assert.match(html,/11s · 2 supply/);
 for(const key of ['q','w','e'])h.key(key);
 assert.deepEqual(workshop.queue.map(q=>q.type),['sniper','airplane','mage']);
 h.spawn('cabinet',1400,800);h.spawn('cabinet',1500,800);h.select(lab);
 for(const key of ['q','w','e'])h.key(key);
 assert.deepEqual(lab.queue.map(q=>q.type),['divider','rollingrock','shears']);
 assert.match(h.element('actionContent').innerHTML,/25s · 6 supply/);
});
test('a mage group keeps targeting armed and casts exactly one dome on each click',async()=>{
 const h=await harness(),a=h.spawn('mage',600,800),b=h.spawn('mage',650,900);
 h.select(a);h.select(b,{shiftKey:true});h.key('g');h.point(850,800);h.refresh();
 assert.equal(h.game.forceFields.length,1);assert.equal(h.state.renderer.lastDraw.mode,'specialist');
 h.point(900,850);assert.equal(h.game.forceFields.length,2);
 h.key('escape');h.refresh();assert.equal(h.state.renderer.lastDraw.mode,null);
 assert.equal(h.element('pauseOverlay').hidden,true,'Escape cancels a targeting mode before opening the menu');
});

test('queued specialist casts explain their approach and cancel cleanly without disabling ready group members',async()=>{
 const h=await harness(),mage=h.spawn('mage',600,800),other=h.spawn('mage',650,900);
 h.select(mage);h.select(other,{shiftKey:true});h.key('g');
 assert.match(h.element('modeBanner').textContent,/80 radius · pushes ground troops out/);
 assert.match(h.element('modeBanner').textContent,/moves into range when needed/);
 h.point(1500,800);h.refresh();
 assert.equal(h.game.forceFields.length,0,'the click queues movement before a distant cast');
 assert.equal([mage,other].filter(e=>e.pendingAbility?.name==='forcefield').length,1);
 assert.match(h.element('toast').textContent,/Moving into range to cast Force Field Dome\. Stop or a new order cancels\./);
 const html=h.element('actionContent').innerHTML;
 assert.match(html,/<span class="ability-status">Moving<\/span>/);
 assert.doesNotMatch(html,/<button[^>]*data-type="forcefield"[^>]*disabled/,'another ready caster keeps the card available');
 assert.equal(h.state.renderer.marker.type,'move','approach orders do not show a cast impact marker');
 h.key('s');h.refresh();
 assert(!mage.pendingAbility&&!other.pendingAbility);
 assert.doesNotMatch(h.element('actionContent').innerHTML,/<span class="ability-status">Moving<\/span>/,'cancellation refreshes the card status');
});
test('sniper targeting uses one caster and retains the selected enemy id',async()=>{
 const h=await harness(),a=h.spawn('sniper',600,800),b=h.spawn('sniper',650,900),enemy=h.spawn('paper',950,800,1);
 h.select(a);h.select(b,{shiftKey:true});h.key('f');h.point(enemy.x,enemy.y);
 const aiming=[a,b].filter(e=>e.aim);assert.equal(aiming.length,1);assert.equal(aiming[0].aim.targetId,enemy.id);
});
test('waypoint patrol and drag Shadow Path issue actual shaped routes on mouse and touch',async()=>{
 const h=await harness(),plane=h.spawn('airplane',600,800),shears=h.spawn('shears',600,1000);
 h.point(plane.x,plane.y-27);h.key('j');h.point(800,800);h.point(1000,900);h.action('finishRoute');
 assert.equal(plane.order.type,'patrol');assert.equal(plane.order.points.length,2);
 h.select(shears);h.key('h');const c=h.element('world');
 c.dispatch('pointerdown',{clientX:650,clientY:1000,pointerType:'touch'});
 c.dispatch('pointermove',{clientX:740,clientY:1040,pointerType:'touch'});
 c.dispatch('pointermove',{clientX:850,clientY:1000,pointerType:'touch'});
 c.dispatch('pointerup',{clientX:900,clientY:950,pointerType:'touch'});
 assert(shears.shadow);assert(shears.shadow.points.length>=3,'drawn route bends through sampled waypoints');
});
test('Army selection includes all new combat families and workers expose both new structures',async()=>{
 const h=await harness();const types=['sniper','airplane','mage','divider','rollingrock','shears'];
 const units=types.map((type,i)=>h.spawn(type,600+i*80,800));h.key('f2');
 assert.deepEqual(new Set(h.state.selected),new Set(units.map(e=>e.id)));
 const worker=h.spawn('twig',600,1100);h.select(worker);
 assert.match(h.element('actionContent').innerHTML,/Prototype Workshop/);
 assert.match(h.element('actionContent').innerHTML,/Shredder Laboratory/);
});

test('a mixed army presents all eleven illustrated abilities in one keyboard-labelled grid',async()=>{
 const h=await harness();Object.assign(h.game.teams[0].upgrades,{rock:true,paper:true,scissors:true});
 for(const [i,type] of ['rock','paper','scissors','sniper','airplane','mage','divider','rollingrock','shears'].entries())h.spawn(type,600+i*80,800);
 h.key('f2');const html=h.element('actionContent').innerHTML;
 assert.equal((html.match(/class="ability-grid"/g)||[]).length,1,'all ability families share the same grid');
 assert.equal((html.match(/class="ability-art"/g)||[]).length,11,'every ability gets custom art');
 assert.equal((html.match(/class="ability-icon"/g)||[]).length,11,'art is real SVG, not fallback glyphs');
 for(const key of ['Q','W','E','F','I','J','K','G','T','Y','H'])assert.match(html,new RegExp(`<kbd>${key}</kbd>`));
});

test('wall placement stays armed, snaps connected blocks and keeps every paid construction order',async()=>{
 const h=await harness(),worker=h.spawn('twig',1008,900);h.game.teams[0].trash=200;
 h.select(worker);assert.match(h.element('actionContent').innerHTML,/data-type="wall"/);
 h.key('z');h.point(1002,1001);h.point(1054,1003);h.refresh();
 const walls=h.game.entities.filter(e=>e.type==='wall');assert.equal(walls.length,2);
 assert.deepEqual(walls.map(e=>[e.x,e.y]),[[1008,1008],[1056,1008]]);
 assert.equal(h.game.teams[0].trash,160);assert.equal(h.state.renderer.lastDraw.mode,'wall');
 assert.match(h.element('actionContent').innerHTML,/Done placing/);
 assert(worker.buildQueue.length>0,'second wall waits behind the active construction');
 h.action('cancelTarget');h.tick(9);
 assert(walls.every(e=>e.constructing===0&&e.hp===1200),'leaving placement mode does not cancel the work');
 assert.deepEqual(h.state.selected,[worker.id]);
});

test('touch wall placement and the Done button preserve selection, and Z toggles targeting',async()=>{
 const h=await harness(),worker=h.spawn('twig',1008,900);h.game.teams[0].trash=200;h.select(worker);
 h.action('build','wall');h.point(1002,1002,{pointerType:'touch'});h.refresh();
 assert.equal(h.game.entities.filter(e=>e.type==='wall').length,1);
 assert.equal(h.state.renderer.lastDraw.mode,'wall');h.action('cancelTarget');h.refresh();
 assert.equal(h.state.renderer.lastDraw.mode,null);assert.deepEqual(h.state.selected,[worker.id]);
 h.key('z');h.refresh();assert.equal(h.state.renderer.lastDraw.mode,'wall');
 h.key('z');h.refresh();assert.equal(h.state.renderer.lastDraw.mode,null);
});

test('mouse selection reports the buffed stats and airplane description reports its scouting radius',async()=>{
 const h=await harness(),mouse=h.spawn('mouse',1000,800),plane=h.spawn('airplane',1300,800);
 h.select(mouse);const html=h.element('actionContent').innerHTML;
 assert.match(html,/850 HP · 360 range/);assert.match(html,/28 impact every 1.25s/);assert.match(html,/6 damage\/s for 3s/);
 h.point(plane.x,plane.y-27);assert.match(h.element('selectionInfo').innerHTML,/900 pixels/);
});

test('selected owned wall exposes deliberate dismantling and opens its space without a refund',async()=>{
 const h=await harness(),wall=h.spawn('wall',1008,1008);h.game.teams[0].trash=123;
 h.select(wall);assert.match(h.element('actionContent').innerHTML,/data-action="dismantleWall"/);
 h.action('dismantleWall');assert(!h.game.getEntity(wall.id));assert.equal(h.game.teams[0].trash,123);
 assert(!h.game._blocked(1008,1008,17));assert.equal(h.state.selected.length,0);
});
