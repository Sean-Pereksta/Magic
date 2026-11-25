import { HEROES, DIFFICULTIES, WORLD, RULES } from './data.mjs';
import { ITEM_BY_ID } from './items.mjs';
import { makeMetrics } from './strategy.mjs';

const VERSION=1,MAX_BYTES=3_000_000;
const TRANSIENT=new Set(['player','grid','navCache','progressionDepth','_castSource']);
const COLLECTION_LIMITS={units:2500,projectiles:3000,zones:1000,effects:500,texts:150,events:100,scheduledImpacts:200,progressionQueue:200};
const fail=message=>{throw new Error('Invalid saved match: '+message);};
const number=(value,min,max)=>typeof value==='number'&&Number.isFinite(value)&&value>=min&&value<=max;
const record=value=>!!value&&typeof value==='object'&&!Array.isArray(value);

function checkTree(value,depth=0,budget={count:0}){
  if(depth>28||++budget.count>240000)fail('state is too large');
  if(value===null||typeof value==='boolean')return;
  if(typeof value==='number'){if(!Number.isFinite(value)||Math.abs(value)>1e12)fail('invalid number');return;}
  if(typeof value==='string'){if(value.length>20000)fail('text is too long');return;}
  if(typeof value!=='object')fail('unsupported value');
  if(Array.isArray(value)){if(value.length>5000)fail('collection is too large');for(const v of value)checkTree(v,depth+1,budget);return;}
  if(Object.getPrototypeOf(value)!==Object.prototype&&Object.getPrototypeOf(value)!==null)fail('unsupported object');
  for(const [key,v] of Object.entries(value)){
    if(['__proto__','prototype','constructor'].includes(key))fail('unsafe key');
    checkTree(v,depth+1,budget);
  }
}
function encode(value){
  if(value instanceof Set)return {__mobaSet:[...value].map(encode)};
  if(Array.isArray(value))return value.map(encode);
  if(value&&typeof value==='object'){const out={};for(const [key,v] of Object.entries(value))if(v!==undefined)out[key]=encode(v);return out;}
  return value;
}
function decode(value){
  if(Array.isArray(value))return value.map(decode);
  if(value&&typeof value==='object'){
    if(Object.keys(value).length===1&&Array.isArray(value.__mobaSet))return new Set(value.__mobaSet.map(decode));
    const out={};for(const [key,v] of Object.entries(value))out[key]=decode(v);return out;
  }
  return value;
}
export function serializeGame(game){
  const state={};for(const [key,value] of Object.entries(game))if(!TRANSIENT.has(key)&&value!==undefined)state[key]=encode(value);
  const snapshot={version:VERSION,state};
  checkTree(snapshot);
  if(JSON.stringify(snapshot).length>MAX_BYTES)fail('match exceeds the save limit');
  return snapshot;
}

export function restoreGame(Game,snapshot){
  if(typeof snapshot==='string'){
    if(snapshot.length>MAX_BYTES)fail('file is too large');
    try{snapshot=JSON.parse(snapshot);}catch{fail('cannot read JSON');}
  }
  checkTree(snapshot);
  if(JSON.stringify(snapshot).length>MAX_BYTES)fail('file is too large');
  const state=snapshot?.state;
  if(snapshot?.version!==VERSION||!state||typeof state!=='object')fail('unsupported version');
  for(const name of Object.keys(state))if(TRANSIENT.has(name)||name in Game.prototype)fail('unexpected field');
  if(!DIFFICULTIES[state.difficulty]||!number(state.time,0,172800)||!number(state.seed,1,4294967295)||!number(state.randomState,0,4294967295))fail('missing match settings');
  if(![null,0,1].includes(state.winner)||typeof state.paused!=='boolean')fail('missing match state');
  for(const key of ['units','projectiles','zones','effects','texts','events','campStates','teamPlans'])if(!Array.isArray(state[key]))fail('missing '+key);
  for(const [key,limit] of Object.entries(COLLECTION_LIMITS))if(state[key]&&(!Array.isArray(state[key])||state[key].length>limit))fail('too many '+key);
  const ids=new Set(),kinds=new Set(['hero','tower','core','camp','beast','footman','archer','summon']);
  for(const unit of state.units){
    if(!unit||!number(unit.id,1,1e9)||!Number.isInteger(unit.id)||ids.has(unit.id)||!kinds.has(unit.kind)||![-1,0,1].includes(unit.team))fail('invalid unit identity');
    ids.add(unit.id);
    if(!number(unit.x,-200,WORLD.width+200)||!number(unit.y,-200,WORLD.height+200)||!number(unit.hp,0,1e8)||!number(unit.maxHp,1,1e8)||unit.hp>unit.maxHp+1||typeof unit.alive!=='boolean'||!record(unit.status))fail('invalid unit state');
    for(const key of ['speed','range','damage','r','attackRate'])if(!number(unit[key],0,1e6))fail('invalid combat statistic');
    for(const key of ['attackAnim','attackTimer','splashIn','deathTime','shield','shieldTime'])if(!number(unit[key],-10,1e8))fail('missing unit timer');
    if(unit.kind==='hero'){
      if(!HEROES[unit.heroId]||!number(unit.level,1,RULES.maxLevel)||!Number.isInteger(unit.level)||!number(unit.gold,0,1e9)||!Array.isArray(unit.items)||new Set(unit.items).size!==unit.items.length||unit.items.some(id=>!ITEM_BY_ID[id]))fail('invalid hero build');
      if(!Array.isArray(unit.cooldowns)||unit.cooldowns.length!==4||unit.cooldowns.some(c=>!number(c,0,1e5)))fail('invalid cooldowns');
      if(!number(unit.mana,0,1e8)||!number(unit.maxMana,0,1e8)||!number(unit.xp,0,1e9)||!number(unit.xpNext,0,1e9)||!record(unit.metrics)||!record(unit.brain)||!number(unit.respawnIn,0,1e5)||!record(unit.talents))fail('missing hero state');
      if(Object.keys(makeMetrics()).some(key=>!number(unit.metrics[key],0,1e10)))fail('invalid hero metrics');
      if(unit.evolution&&(!record(unit.evolution)||['readyAt','counts','marks'].some(key=>unit.evolution[key]&&!record(unit.evolution[key]))||(unit.evolution.castHistory&&!Array.isArray(unit.evolution.castHistory))))fail('invalid evolution state');
      for(const [level,id] of Object.entries(unit.talents||{}))if(!HEROES[unit.heroId].talents.some(t=>t.level===Number(level)&&unit.level>=t.level&&t.choices.some(c=>c.id===id)))fail('invalid talent');
    }
    for(const status of Object.values(unit.status))if(!status||!number(status.remaining,-1,1e6)||(status.layers&&(!Array.isArray(status.layers)||status.layers.some(layer=>!number(layer.remaining,-1,1e6)))))fail('invalid status');
  }
  const player=state.units.find(u=>u.id===state.playerId&&u.kind==='hero'&&u.isPlayer&&u.team===0);
  if(!player||state.units.filter(u=>u.isPlayer).length!==1)fail('missing player');
  if(!number(state.nextId,1,1e9)||state.nextId<=Math.max(...ids))fail('invalid entity sequence');
  if(!number(state.eventId,1,1e9)||!number(state.waveNumber,0,1e8)||!number(state.beastIndex,0,1e8))fail('invalid event sequence');
  for(const timer of ['waveIn','beastIn','planIn'])if(!number(state[timer],-10,1e9))fail('invalid timer');
  if(!state.stats||['kills','towers','beasts','damage'].some(key=>!Array.isArray(state.stats[key])||state.stats[key].length!==2||state.stats[key].some(n=>!number(n,0,1e10))))fail('invalid scoreboard');
  for(const projectile of state.projectiles)if(!number(projectile.x,-1000,WORLD.width+1000)||!number(projectile.y,-1000,WORLD.height+1000)||!number(projectile.life,0,10000)||!Array.isArray(projectile.hit?.__mobaSet)||!number(projectile.damage,0,1e8)||!number(projectile.speed,0,1e6)||!number(projectile.pierce,-1,1000))fail('invalid projectile');
  for(const zone of state.zones)if(!number(zone.x,-1000,WORLD.width+1000)||!number(zone.y,-1000,WORLD.height+1000)||!number(zone.r,0,10000)||!number(zone.life,-1,10000)||!number(zone.tickIn,-10,10000)||(zone.type==='field'&&!Array.isArray(zone.applied?.__mobaSet)))fail('invalid zone');
  if(state.teamPlans.length!==2||state.campStates.length>WORLD.camps.length)fail('invalid map state');
  for(const plan of state.teamPlans)if(!plan||!['lanes','push','defend','beast'].includes(plan.type)||![0,1,2].includes(plan.lane))fail('invalid team plan');
  for(const camp of state.campStates)if(!WORLD.camps.some(c=>c.id===camp.id)||typeof camp.spawned!=='boolean'||!number(camp.respawnIn,-10,1e6))fail('invalid camp');
  for(const impact of state.scheduledImpacts||[])if(!number(impact.executeAt,0,1e9)||!number(impact.x,-1000,WORLD.width+1000)||!number(impact.y,-1000,WORLD.height+1000)||!number(impact.r,0,10000)||!ids.has(impact.ownerId))fail('invalid scheduled impact');
  if(!Array.isArray(state.lastSeen)||state.lastSeen.length!==2||!Array.isArray(state.assignments)||state.assignments.length!==2)fail('missing team information');
  for(const seen of state.lastSeen){
    if(!record(seen))fail('invalid remembered vision');
    for(const marker of Object.values(seen))if(!record(marker)||!number(marker.x,0,WORLD.width)||!number(marker.y,0,WORLD.height)||!number(marker.time,0,state.time)||!HEROES[marker.heroId])fail('invalid remembered marker');
  }
  if(state.assignments.some(jobs=>!record(jobs)))fail('invalid team jobs');
  const game=new Game({hero:player.heroId,difficulty:state.difficulty,seed:state.seed});
  Object.assign(game,decode(state));
  game.player=game.units.find(u=>u.id===game.playerId);game.grid=new Map();game.navCache=new Map();game.progressionDepth=0;
  game.rebuildGrid();
  return game;
}
