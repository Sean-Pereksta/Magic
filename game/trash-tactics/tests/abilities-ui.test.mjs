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
  const difficulties=['easy','normal','hard','impossible'].map(type=>{const e=element('difficulty-'+type);e.dataset.difficulty=type;return e;});
  const players=[2,3,4].map(count=>{const e=element('players-'+count);e.dataset.players=String(count);return e;});
  const document={getElementById:element,querySelector:()=>element('sidebar'),querySelectorAll:selector=>selector==='[data-difficulty]'?difficulties:selector==='[data-players]'?players:[],activeElement:null};
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
    .replace("'./skirmish-menu.js'",JSON.stringify(new URL('../skirmish-menu.js',import.meta.url).href))
    .replace("'./minimap-alerts.js'",JSON.stringify(new URL('../minimap-alerts.js',import.meta.url).href))
    .replace("'./match-report.js'",JSON.stringify(new URL('../match-report.js',import.meta.url).href))
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

test('native map dropdown updates the real preview and starts the chosen map, player count and Impossible economy',async()=>{
 const h=await harness(),select=h.element('mapSelect');
 select.value='serverstack';select.dispatch('change');h.element('players-4').click();h.element('difficulty-impossible').click();
 assert.match(h.element('mapBriefTitle').textContent,/corridors/);
 assert.match(h.element('mapBriefStats').textContent,/4× area/);
 assert.match(h.element('difficultyHint').textContent,/Impossible cheats.*2×/);
 h.element('startButton').click();const game=h.state.game;
 assert.equal(game.map.id,'serverstack');assert.equal(game.players,4);assert.equal(game.difficulty,'impossible');
 assert.equal(game.teams[0].trash,300,'player resources do not receive Impossible bonuses');assert.equal(game.teams[1].trash,600);
 select.value='not-a-map';select.dispatch('change');h.element('startButton').click();assert.equal(h.state.game.map.id,'serverstack','an invalid select value cannot alter the next match');
 const html=await readFile(new URL('../index.html',import.meta.url),'utf8');
 assert.match(html,/<label[^>]+for="mapSelect"/);assert.match(html,/<select id="mapSelect"/);
 assert.equal((html.match(/<option value=/g)||[]).length,7);assert(!html.includes('class="map-options"'),'the old card grid is replaced');
});

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

test('recalled printer groups distribute one training order per press to the shortest ready queue',async()=>{
 const h=await harness(),a=h.spawn('printer',700,800),b=h.spawn('printer',900,800);
 h.game.teams[0].trash=5000;
 h.select(b);h.select(a,{shiftKey:true});h.key('2',{ctrlKey:true});
 h.select(h.game.entities.find(e=>e.type==='desk'&&e.team===0));h.key('2');
 assert.deepEqual(new Set(h.state.selected),new Set([a.id,b.id]));
 assert.match(h.element('actionContent').innerHTML,/GROUP QUEUES/);
 assert.match(h.element('actionContent').innerHTML,/class="production production-group"/,'group queue summary has its visible HUD style hook');
 assert.match(h.element('actionContent').innerHTML,/data-type="rock"/);
 h.key('q');assert.deepEqual(a.queue.map(q=>q.type),['rock']);assert.equal(b.queue.length,0,'stable id tie-break chooses one building');
 h.key('q');assert.deepEqual(b.queue.map(q=>q.type),['rock'],'the next press goes to the shorter queue');
 h.key('w');h.action('train','scissors');
 assert.deepEqual(a.queue.map(q=>q.type),['rock','paper']);assert.deepEqual(b.queue.map(q=>q.type),['rock','scissors']);
 assert.equal(h.game.teams[0].trash,4800,'each key or click pays for exactly one troop');
 h.action('research','rock');
 assert.equal(a.queue.filter(q=>q.kind==='research').length,1);assert.equal(b.queue.filter(q=>q.kind==='research').length,0);
 assert.equal(h.game.teams[0].trash,4650,'group research is queued and paid once');
 a.constructing=5;h.key('e');assert.equal(a.queue.length,3);assert.equal(b.queue.length,3,'construction is excluded from group production');
 while(b.queue.length<6)h.game.train(b.id,'rock');
 const trash=h.game.teams[0].trash;h.key('q');
 assert.equal(b.queue.length,6);assert.equal(h.game.teams[0].trash,trash,'full and unfinished buildings cannot consume resources');
 assert.match(h.element('toast').textContent,/queue is full/);
});

test('every homogeneous production-building group keeps contextual training controls',async()=>{
 const h=await harness();h.game.teams[0].trash=20000;
 for(let i=0;i<10;i++)h.spawn('cabinet',1600+i*90,1600);
 const production={desk:'twig',printer:'rock',cdreader:'cd',workshop:'sniper',laboratory:'divider'};
 for(const [i,[type,unit]] of Object.entries(production).entries()){
  const a=h.spawn(type,600,800+i*190),b=h.spawn(type,900,800+i*190);
  h.select(a);h.select(b,{shiftKey:true});
  assert.match(h.element('actionContent').innerHTML,new RegExp(`data-action="train" data-type="${unit}"`));
  h.key('q');assert.equal(a.queue.length+b.queue.length,1);assert.equal(a.queue[0].type,unit);
 }
});

test('pointer, attack targeting, and construction controls never reveal hidden enemies or ground',async()=>{
 const h=await harness(),rock=h.spawn('rock',700,800),enemy=h.spawn('scissors',900,800,1),worker=h.spawn('twig',700,1000);
 h.game.isVisible=e=>e.team===0;h.game.canSee=()=>false;
 h.select(rock);h.point(enemy.x,enemy.y,{button:2});
 assert.equal(rock.order.type,'move','right clicking an unseen enemy is a ground move');
 assert(!rock.order.targetId,'hidden enemy ID never enters the order');
 h.action('attack');
 h.refresh();assert.equal(h.state.renderer.lastDraw.mode,'attack','Attack button arms the same targeting mode as A');
 h.point(enemy.x,enemy.y);
 assert.equal(rock.order.type,'attackMove');assert(!rock.order.targetId,'attack targeting never reveals a hidden enemy');
 h.select(worker);h.key('b');h.point(900,1000);
 assert.equal(h.game.entities.filter(e=>e.type==='printer').length,0);
 assert.match(h.element('toast').textContent,/Scout this ground/);
 h.key('a');h.point(1200,1000,{button:2});
 assert.equal(worker.order.type,'attackMove','players can scout unknown ground with attack move');
});

test('A and the Attack button target enemies or attack-move to ground, and F2 selects only the ready army',async()=>{
 const h=await harness(),rock=h.spawn('rock',700,800),paper=h.spawn('paper',700,900),worker=h.spawn('twig',700,1100),enemy=h.spawn('scissors',900,800,1),cd=h.spawn('cd',1000,1000);
 h.game.isVisible=()=>true;
 h.select(rock);h.key('a');h.point(enemy.x,enemy.y);
 assert.equal(rock.order.type,'attack');assert.equal(rock.order.targetId,enemy.id,'A-click on visible enemy targets that enemy');
 h.action('attack');h.point(1100,800,{button:2});
 assert.equal(rock.order.type,'attackMove');assert(!rock.order.targetId);
 paper.transportedBy=cd.id;h.select(worker);h.key('f2');
 assert.deepEqual(h.state.selected,[rock.id],'army hotkey excludes workers, buildings, transports and embarked troops');
 h.key('f2',{ctrlKey:true});assert.deepEqual(h.state.selected,[rock.id],'modified shortcuts do not change selection');
 h.refresh();assert.match(h.element('actionContent').innerHTML,/data-action="attack" aria-keyshortcuts="A"/);
});

test('workers have padded click and box-selection bounds without larger collision footprints',async()=>{
 const h=await harness(),worker=h.spawn('twig',700,1000),r=worker.r;
 h.point(worker.x+29,worker.y-29);
 assert.deepEqual(h.state.selected,[worker.id],'clicking near the worker silhouette selects its padded box');
 h.key('x');const canvas=h.element('world');
 canvas.dispatch('pointerdown',{clientX:worker.x+25,clientY:worker.y-30});
 canvas.dispatch('pointermove',{clientX:worker.x+45,clientY:worker.y-15});
 canvas.dispatch('pointerup',{clientX:worker.x+45,clientY:worker.y-15});
 assert.deepEqual(h.state.selected,[worker.id],'a drag intersecting the worker box catches the worker');
 assert.equal(worker.r,r,'selection padding leaves navigation and collision geometry unchanged');
});

test('A-click respects each family counter and leaves buildings and ineligible troops alone',async()=>{
 const h=await harness(),rock=h.spawn('rock',700,800),scissors=h.spawn('scissors',700,900),printer=h.spawn('printer',500,1000),enemy=h.spawn('paper',1000,800,1);
 h.game.isVisible=()=>true;
 h.game.command([rock.id],{type:'move',x:1200,y:1000});const rockOrder={...rock.order};
 printer.rally={x:1500,y:1000};const rally={...printer.rally};
 h.select(rock);h.select(scissors,{shiftKey:true});h.select(printer,{shiftKey:true});
 h.key('a');h.point(enemy.x,enemy.y);
 assert.equal(scissors.order.type,'attack');assert.equal(scissors.order.targetId,enemy.id);
 assert.deepEqual(rock.order,rockOrder,'rock cannot attack paper and retains its previous order');
 assert.deepEqual(printer.rally,rally,'mixed selected buildings never receive troop orders');
 h.select(rock);h.key('a');h.point(enemy.x,enemy.y);h.refresh();
 assert.deepEqual(rock.order.type,'move');assert.equal(h.state.renderer.lastDraw.mode,'attack','an invalid target keeps targeting armed');
 assert.match(h.element('toast').textContent,/cannot attack that target/);
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


test('workers queue mixed paid buildings through Shift and the touch toggle without abandoning active work',async()=>{
 const h=await harness(),w=h.spawn('twig',700,1000);h.game.teams[0].trash=2000;h.game.canSee=()=>true;h.game.map.plateaus=[];h.game.map.ramps=[];
 h.select(w);h.key('b');h.point(900,1000,{shiftKey:true});h.refresh();
 const first=h.game.getEntity(w.order.targetId);assert.equal(first.type,'printer');assert.equal(h.state.renderer.lastDraw.mode,'printer','Shift keeps placement active');
 h.key('c');h.point(1150,1000);h.key('v');h.point(1400,1000);h.refresh();
 assert.deepEqual(h.game.getBuildQueue(w.id).map(j=>[j.type,j.status]),[['printer','active'],['cabinet','queued'],['cdreader','queued']]);
 assert.equal(w.order.targetId,first.id);assert.equal(h.game.teams[0].trash,1600,'each paid plan is charged exactly once');
 assert.match(h.element('actionContent').innerHTML,/1 active · 2 queued/);assert.match(h.element('actionContent').innerHTML,/BUILD PLAN/);
 h.action('toggleBuildQueue');h.key('c');h.point(1650,1000,{pointerType:'touch'});h.refresh();
 assert.equal(h.state.renderer.lastDraw.mode,'cabinet','touch toggle keeps placement active');assert.equal(h.game.getBuildQueue(w.id).length,4);
 h.action('cancelTarget');h.refresh();assert.equal(h.state.renderer.lastDraw.mode,null);assert.equal(h.game.getBuildQueue(w.id).length,4,'Done placing preserves paid plans');
 h.tick(65);assert(h.game.entities.filter(e=>['printer','cabinet','cdreader'].includes(e.type)).every(e=>!e.constructing),'one worker finishes every planned structure');assert.equal(h.game.getBuildQueue(w.id).length,0);
});

test('Stop clears displayed build assignments and another worker can resume paid construction',async()=>{
 const h=await harness(),a=h.spawn('twig',700,1000),b=h.spawn('twig',600,1150);h.game.teams[0].trash=2000;h.game.canSee=()=>true;h.game.map.plateaus=[];h.game.map.ramps=[];
 h.select(a);h.key('b');h.point(900,1000);h.key('c');h.point(1150,1000);const plans=h.game.getBuildQueue(a.id),paid=h.game.teams[0].trash;
 h.key('s');assert.equal(h.game.getBuildQueue(a.id).length,0);assert(plans.every(j=>!h.game.getEntity(j.id).builderId));
 assert(!h.element('actionContent').innerHTML.includes('BUILD PLAN'));h.select(b);const target=h.game.getEntity(plans[0].id);h.point(target.x,target.y,{button:2});
 assert.equal(b.order.type,'build');assert.equal(b.order.targetId,target.id);assert.equal(target.builderId,b.id);assert.equal(h.game.teams[0].trash,paid,'resuming never pays twice');
 h.select(a);h.point(target.x,target.y,{button:2});assert.equal(a.order.targetId,target.id);assert.equal(b.order.type,'idle','reassignment releases the previous builder');
});

test('finished match opens the frozen charts, can reopen from setup, and clears when a new match starts',async()=>{
 const h=await harness();h.element('endOverlay').hidden=true;
 assert.equal(h.game.getMatchReport(),null,'live matches expose no report');
 for(const e of h.game.entities)if(e.team===1)e.hp=0;h.refresh();
 assert.equal(h.game.winner,0);assert.equal(h.element('endOverlay').hidden,false);assert.match(h.element('matchReport').innerHTML,/THE AFTER-SCRAP/);assert.match(h.element('matchReport').innerHTML,/data-report-tab="economy"/);assert.match(h.element('matchReport').innerHTML,/<svg/);
 assert.equal(h.element('lastReportButton').hidden,false);const snapshot=h.game.getMatchReport();assert(Object.isFrozen(snapshot));
 h.element('reportLobbyButton').click();assert.equal(h.element('endOverlay').hidden,true);h.element('lastReportButton').click();assert.equal(h.element('endOverlay').hidden,false);
 h.element('playAgain').click();h.element('startButton').click();assert.equal(h.element('lastReportButton').hidden,true);assert.equal(h.element('matchReport').innerHTML,'');assert.equal(h.state.game.getMatchReport(),null);
});

test('an eliminated player cannot resume the ended three-team match behind the final report',async()=>{
 const h=await harness();h.element('players-3').click();h.element('startButton').click();const game=h.state.game;game._think=Infinity;
 for(const e of game.entities)if(e.team===0&&e.type==='desk')e.hp=0;h.refresh();
 assert.equal(game.teams[0].eliminated,true);assert.equal(game.winner,null);assert.equal(h.element('endOverlay').hidden,false);
 const report=game.getMatchReport(),time=game.time;h.element('reportLobbyButton').click();assert.equal(h.element('resumeFromLobby').hidden,true);
 h.element('resumeFromLobby').click();h.refresh();assert.equal(game.time,time,'completed simulation remains frozen');assert.equal(game.getMatchReport(),report);
});
