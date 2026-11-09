import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,TYPES,MAPS} from '../engine.js';
import {BOT_CONFIGS,updateBotStrategy,chooseBotTroop,updateBotResources} from '../bot-strategies.js';
import {specialistBot} from '../warfare.js';

function arena(difficulty='hard',map='desktop'){
 const g=new Game({difficulty,map});g._think=Infinity;g.entities=g.entities.filter(e=>e.type==='desk');
 g.map.obstacles=[];g.map.plateaus=[];g.map.ramps=[];g.map.cliffs=[];g._collisionBuildings=null;
 g.canSee=()=>true;g.isVisible=(e,team)=>e.team===team;return g;
}
function advance(g,seconds,inspect=()=>{}){for(let t=0;t<seconds-1e-9;){const dt=Math.min(.08,seconds-t);g.update(dt);inspect(g);t+=dt;}}
function army(g,count=30){const home=g.starts[1];return Array.from({length:count},(_,i)=>g._spawn(['rock','paper','scissors'][i%3],1,home.x-350+(i%6)*43,home.y-120+Math.floor(i/6)*43));}

test('worker durability leaves two normal melee hits survivable while maintaining cost and economy',()=>{
 const g=arena(),worker=g._spawn('twig',0,3000,3000),attacker=g._spawn('rock',1,3040,3000);
 assert.equal(TYPES.twig.hp,75);assert.equal(TYPES.twig.cost,25);
 g._damage(worker,30,attacker);g._damage(worker,30,attacker);assert.equal(worker.hp,15);
 g._damage(worker,30,attacker);assert(worker.hp<=0);
});

test('difficulty escalates real scouting, planning cadence, economy targets and tactical micro',()=>{
 const names=['easy','normal','hard','impossible'];
 for(let i=1;i<names.length;i++){
  const lower=BOT_CONFIGS[names[i-1]],higher=BOT_CONFIGS[names[i]];
  assert(higher.think<lower.think);assert(higher.attack<lower.attack);assert(higher.scoutAt<lower.scoutAt);assert(higher.workers>lower.workers);assert(higher.cap>lower.cap);assert(higher.micro>lower.micro);
 }
 for(const name of names.slice(0,3)){assert.equal(BOT_CONFIGS[name].resourceMultiplier,1);assert.equal(BOT_CONFIGS[name].initialBonus,0);assert.equal(BOT_CONFIGS[name].stipend,0);}
});

test('map and team seeds rotate personalities and issue distinct executable main, flank, feint and raid routes',()=>{
 const personalities=new Set(),kinds=new Set(),routes=new Set();
 for(const map of Object.keys(MAPS))for(let cursor=0;cursor<5;cursor++){
  const g=arena('hard',map),troops=army(g),state=g._bots[1];personalities.add(state.personality);g.time=200;state.nextAttack=0;state.nextScout=Infinity;state.planCursor=cursor;
  updateBotStrategy(g,1);for(const h of state.history){kinds.add(h.kind);if(h.route)routes.add(JSON.stringify(h.route));}
  assert(troops.some(e=>e.aiSquad&&['move','attackMove'].includes(e.order.type)),`${map}/${cursor}: a plan creates actual troop orders`);
  assert(state.squads.every(s=>s.expires>g.time+60),'travel budgets include long battlefield routes');
 }
 assert(personalities.size>=4);for(const kind of ['mainPush','flank','feint','raid'])assert(kinds.has(kind),kind);assert(routes.size>=10);
});

test('hidden enemy positions cannot influence targeting, memory or production counters at any difficulty',()=>{
 for(const difficulty of Object.keys(BOT_CONFIGS)){
  const g=arena(difficulty),troops=army(g),hidden=g._spawn('paper',0,100,100),state=g._bots[1];
  g.time=200;state.nextAttack=0;state.nextScout=Infinity;g.canSee=()=>false;state.planCursor=2;
  updateBotStrategy(g,1);const first=state.history.at(-1)?.target;assert.equal(state.enemyMemory.size,0);
  hidden.x=8000;hidden.y=6000;state.squads=[];for(const e of troops)delete e.aiSquad;state.planCursor=2;state.nextAttack=0;
  updateBotStrategy(g,1);assert.deepEqual(state.history.at(-1)?.target,first);assert.equal(state.observedFamilies.paper,0);
  const baseline=state.productionCursor;const a=chooseBotTroop(g,1,troops,[]);state.productionCursor=baseline;const b=chooseBotTroop(g,1,troops,[]);assert.equal(a,b);
  g.isVisible=()=>true;updateBotStrategy(g,1);assert.equal(state.observedFamilies.paper,1);
  assert.equal(chooseBotTroop(g,1,troops,[hidden]),'scissors','visible Paper causes counter production');
 }
});

test('hard bots evacuate workers, call local defenders and retreat damaged troops instead of abandoning the economy',()=>{
 const g=arena(),home=g.starts[1],workers=[g._spawn('twig',1,home.x-180,home.y),g._spawn('twig',1,home.x-200,home.y+40)];
 const defenders=Array.from({length:6},(_,i)=>g._spawn('scissors',1,home.x-250-i*40,home.y+100));
 const damaged=g._spawn('rock',1,home.x-300,home.y-100);damaged.hp=30;
 const intruder=g._spawn('paper',0,home.x-260,home.y-60);g.isVisible=()=>true;g.time=20;g._bots[1].nextScout=Infinity;
 updateBotStrategy(g,1);const kinds=new Set(g._bots[1].history.map(h=>h.kind));
 assert(kinds.has('defendEconomy'));assert(kinds.has('evacuateWorkers'));assert(kinds.has('retreatRegroup'));
 assert(workers.some(e=>e.aiTask==='evacuate'&&e.order.type==='move'));assert(defenders.some(e=>e.aiTask==='defend'));assert.equal(damaged.aiTask,'retreat');assert.equal(damaged.order.type,'move');
});

test('CD plans load actual passengers, fly, unload and begin a ground raid with an aircraft escort',()=>{
 const g=arena('hard'),home=g.starts[1],troops=army(g,18),cd=g._spawn('cd',1,home.x-250,home.y+40),escort=g._spawn('airplane',1,home.x-200,home.y+80),state=g._bots[1];
 const victim=g._spawn('twig',0,home.x-1100,home.y+100);g.isVisible=()=>true;g.time=200;state.personality='airmarshal';state.nextAttack=0;state.nextScout=Infinity;
 updateBotStrategy(g,1);assert.equal(state.squads[0].kind,'airDrop');assert.equal(state.squads[0].carrierId,cd.id);assert(state.squads[0].escortIds.includes(escort.id));
 let launched=false,landed=false;
 for(let t=0;t<22;t+=.08){g.update(.08);updateBotStrategy(g,1);launched||=state.history.some(h=>h.kind==='airDropLaunch');landed||=state.history.some(h=>h.kind==='dropRaid');}
 assert(launched);assert(landed);assert(!(cd.cargo||[]).length);assert(troops.some(e=>!e.transportedBy&&distance(e,victim)<500));
});
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);

test('specialists coordinate distinct sniper targets and keep friendly formations clear of their own domes',()=>{
 const g=arena('hard'),snipers=[g._spawn('sniper',1,3000,3000),g._spawn('sniper',1,3040,3040)];
 const papers=[g._spawn('paper',0,3400,3000),g._spawn('paper',0,3400,3040)];g.isVisible=()=>true;
 specialistBot(g,1);assert(snipers.every(e=>e.aim));assert.equal(new Set(snipers.map(e=>e.aim.targetId)).size,2);
 const h=arena('hard'),mage=h._spawn('mage',1,3000,3000),friend=h._spawn('rock',1,3300,3000);h.isVisible=()=>true;
 for(let i=0;i<4;i++)h._spawn('paper',0,3190+i*8,2970+i*20);
 specialistBot(h,1);assert.equal(h.forceFields.length,0,'the planned dome would block a friendly troop');
 friend.hp=0;specialistBot(h,1);assert.equal(h.forceFields.length,1);assert(h.forceFields[0].x>3200,'the dome cuts reinforcement lanes behind the hostile front');
});

test('Impossible bonuses are explicit, bot-only, paid into the ledger and capped for passive stipends',()=>{
 const g=new Game({difficulty:'impossible',players:3});g._think=Infinity;
 assert.equal(g.teams[0].trash,300);assert.equal(g.teams[0].resourceMultiplier,1);assert.equal(g.stats.botBonus[0],0);
 for(const team of [1,2]){assert.equal(g.teams[team].trash,600);assert.equal(g.teams[team].resourceMultiplier,2);assert.equal(g.stats.botBonus[team],300);}
 advance(g,25);for(const team of [0,1,2])assert(Math.abs(g.teams[team].trash-(300+g.stats.collected[team]+g.stats.botBonus[team]))<1e-6);
 assert(g.stats.botBonus[1]>350,'gathered income is doubled in addition to the stipend');assert.equal(g.stats.botBonus[0],0);
 updateBotResources(g,10000);assert.equal(g.stats.botStipend[1],1200);const before=g.teams[1].trash;updateBotResources(g,10000);assert.equal(g.teams[1].trash,before);
});

test('counter micro preserves an exposed troop on Hard while Easy leaves it to its counter',t=>{
 const results={};
 for(const difficulty of ['easy','normal','hard','impossible']){
  const g=arena(difficulty),paper=g._spawn('paper',1,3000,3000),rock=g._spawn('rock',1,2760,3000),enemy=g._spawn('scissors',0,3090,3000);g.isVisible=()=>true;g._bots[1].nextScout=Infinity;g._bots[1].nextAttack=Infinity;
  let next=0;for(let elapsed=0;elapsed<8;elapsed+=.08){if(g.time>=next){updateBotStrategy(g,1);next=g.time+g.botConfig.think;}g.update(.08);}
  results[difficulty]={paperHp:Math.max(0,paper.hp),enemyHp:Math.max(0,enemy.hp)};
 }
 assert(results.hard.paperHp>results.easy.paperHp);assert(results.impossible.paperHp>=results.hard.paperHp);t.diagnostic(JSON.stringify(results));
});

test('all four difficulties pay for production and show escalating pressure with explicit Impossible income',t=>{
 const results={};
 for(const difficulty of ['easy','normal','hard','impossible']){
  const g=new Game({difficulty});for(const desk of g.entities.filter(e=>e.type==='desk'))desk.hp=1e8;
  let spent=0;for(const method of ['train','research','build']){
   const original=g[method].bind(g);g[method]=(...args)=>{const actor=g.getEntity(method==='build'?args[0][0]:args[0]),team=actor?.team,before=g.teams[1].trash,result=original(...args);if(team===1&&result.ok){const price=method==='research'?150:TYPES[args[1]].cost;assert(Math.abs(before-g.teams[1].trash-price)<1e-6);spent+=price;}return result;};
  }
  advance(g,150,()=>{assert(Math.abs(g.teams[1].trash-(300+g.stats.collected[1]+g.stats.botBonus[1]-spent))<1e-6);if(difficulty!=='impossible')assert.equal(g.stats.botBonus[1],0);assert.equal(g.stats.botBonus[0],0);});
  const state=g._bots[1];assert(g.stats.collected[1]>500);assert(state.history.some(h=>h.kind==='scout'));assert(state.history.some(h=>['mainPush','flank','raid','feint'].includes(h.kind)));
  results[difficulty]={collected:g.stats.collected[1],bonus:Math.round(g.stats.botBonus[1]),spent,trained:g.stats.trained[1],workers:g.entities.filter(e=>e.team===1&&e.type==='twig').length,orders:state.ordersIssued,plans:[...new Set(state.history.map(h=>h.kind))]};
 }
 assert(results.hard.orders>results.easy.orders,'faster planning and layered missions create more tactical activity');assert(results.hard.workers>results.easy.workers,'Hard invests in a larger paid economy');
 assert(results.impossible.spent>results.hard.spent,'Impossible bonuses translate into a larger paid war effort');assert(results.impossible.orders>results.hard.orders,'Impossible maintains more battlefield pressure');
 t.diagnostic(JSON.stringify(results));
});
