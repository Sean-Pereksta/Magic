import { HEROES, DIFFICULTIES, WORLD, RULES } from './data.mjs';
import { ITEM_BY_ID } from './items.mjs';
import { makeMetrics } from './strategy.mjs';

const VERSION=1,MAX_BYTES=3_000_000;
const TRANSIENT=new Set(['player','grid','navCache','progressionDepth','_castSource']);
const COLLECTION_LIMITS={units:2500,projectiles:3000,zones:1000,effects:600,texts:150,events:100,scheduledImpacts:200,progressionQueue:200};
const fail=message=>{throw new Error('Invalid saved match: '+message);};
const number=(value,min,max)=>typeof value==='number'&&Number.isFinite(value)&&value>=min&&value<=max;
const record=value=>!!value&&typeof value==='object'&&!Array.isArray(value);
const entityId=value=>number(value,1,1e9)&&Number.isInteger(value);
const optionalNumber=(value,min,max)=>value===undefined||number(value,min,max);
const encodedIds=value=>record(value)&&Object.keys(value).length===1&&Array.isArray(value.__mobaSet)&&value.__mobaSet.length<=2500&&value.__mobaSet.every(entityId);

function checkEvolution(evolution,time){
  if(evolution===undefined)return;
  if(!record(evolution))fail('invalid evolution state');
  for(const key of ['readyAt','counts','marks'])if(evolution[key]!==undefined){
    const values=evolution[key];
    if(!record(values)||Object.keys(values).length>128)fail('invalid evolution state');
    if(key!=='marks'&&Object.values(values).some(value=>!number(value,0,1e9)||(key==='counts'&&!Number.isInteger(value))))fail('invalid evolution counter');
  }
  if(evolution.castHistory!==undefined&&(!Array.isArray(evolution.castHistory)||evolution.castHistory.length>16||evolution.castHistory.some(c=>!record(c)||!Number.isInteger(c.slot)||!number(c.slot,0,3)||!number(c.time,0,time))))fail('invalid cast history');
  for(const key of ['mobilityReadyUntil','tempestReadyUntil','dragonMeteorUntil','unbrokenUntil','retaliateUntil','judgementUntil'])if(!optionalNumber(evolution[key],0,1e9))fail('invalid evolution timer');
  if(evolution.lastMarkedId!==undefined&&!entityId(evolution.lastMarkedId))fail('invalid marked target');
}

function checkExpansionUnit(unit,units){
  if(!optionalNumber(unit.displacementRecoveryUntil,0,1e9)||!optionalNumber(unit.overclockUntil,0,1e9))fail('invalid expansion timer');
  if(unit.magazine!==undefined){
    const m=unit.magazine;
    if(unit.heroId!=='vela'||!record(m)||!Number.isInteger(m.rounds)||!number(m.rounds,0,6)||!number(m.reloadRemaining,0,2)||!number(m.reloadDuration,.1,2)||!Number.isInteger(m.openingShots)||!number(m.openingShots,0,2))fail('invalid magazine');
  }
  if(unit.channel!==undefined){
    const c=unit.channel;
    if(unit.heroId!=='vela'||!record(c)||c.kind!=='silver-volley'||c.shotCount!==6||!Number.isInteger(c.nextShot)||!number(c.nextShot,0,6)||!number(c.startedAt,0,1e9)||!number(c.fireAt,c.startedAt,1e9)||!number(c.endsAt,c.fireAt,1e9)||!number(c.x,0,WORLD.width)||!number(c.y,0,WORLD.height)||!number(c.angle,-1e6,1e6)||!number(c.range,1,10000)||!number(c.width,1,1000)||!number(c.damage,0,1e8))fail('invalid volley channel');
  }
  if(unit.timeWard!==undefined){
    const w=unit.timeWard;
    if(unit.kind!=='hero'||!record(w)||!units.some(u=>u.id===w.ownerId&&u.heroId==='solstice')||typeof w.shieldId!=='string'||w.shieldId.length>120||!number(w.startedAt,0,1e9)||!number(w.expiresAt,w.startedAt,1e9)||!number(w.absorbed,0,1e10)||!number(w.healthLost,0,1e10)||typeof w.triggered!=='boolean'||!number(w.shieldPower,0,1000)||!number(w.healPower,0,1000)||!number(w.healCap,0,1e8)||!number(w.guardFactor,0,1))fail('invalid time ward');
  }
  if(unit.expansion!==undefined){
    const s=unit.expansion;
    if(!record(s)||!record(s.readyAt)||Object.keys(s.readyAt).length>32||Object.values(s.readyAt).some(t=>!number(t,0,1e9)))fail('invalid expansion state');
    for(const key of ['heavyLandingUntil','heavyLandingDamage'])if(!optionalNumber(s[key],0,1e9))fail('invalid landing state');
    if(s.heavyLandingCast!==undefined&&!entityId(s.heavyLandingCast))fail('invalid landing identity');
    if(s.splitPairs!==undefined&&(!Array.isArray(s.splitPairs)||s.splitPairs.length>4||s.splitPairs.some(p=>!record(p)||!entityId(p.id)||!number(p.expiresAt,0,1e9)||!Array.isArray(p.hits)||p.hits.length>2||p.hits.some(id=>!entityId(id))||typeof p.firstHit!=='boolean')))fail('invalid split shot state');
  }
  if(unit.stationary){
    if(unit.kind!=='summon'||!['sentry','mine'].includes(unit.subtype)||!units.some(u=>u.id===unit.ownerId&&u.heroId==='rivet')||unit.noCollision!==true||(unit.subtype==='mine'&&unit.noVision!==true)||!number(unit.createdAt,0,1e9)||!number(unit.assembleUntil,0,1e9)||!number(unit.armAt,0,1e9)||!number(unit.lifetime,-1,1000)||!number(unit.maxLifetime,0,1000))fail('invalid device');
    for(const key of ['crossfireReadyAt','overclockDamage','emergencyShield','crossfireDamage','triggerRadius','blastRadius'])if(!optionalNumber(unit[key],0,1e8))fail('invalid device statistic');
  }
}

function validGeometry(effect){
  const shape=effect.shape===undefined?(effect.arc===undefined?'circle':'cone'):effect.shape;
  if(!['circle','cone','line'].includes(shape)||!optionalNumber(effect.angle,-1e6,1e6)||!optionalNumber(effect.arc,0,Math.PI*2)||!optionalNumber(effect.range,0,10000)||!optionalNumber(effect.width,0,10000))return false;
  if(shape==='cone'&&(!number(effect.angle,-1e6,1e6)||!number(effect.arc,Number.EPSILON,Math.PI*2)))return false;
  if(shape==='line'&&(!number(effect.angle,-1e6,1e6)||!number(effect.range,Number.EPSILON,10000)||!number(effect.width,Number.EPSILON,10000)))return false;
  return true;
}

function checkProjectile(projectile){
  if(!record(projectile)||!entityId(projectile.id)||!entityId(projectile.ownerId)||!entityId(projectile.sourceId)||![-1,0,1].includes(projectile.team)||typeof projectile.type!=='string'||!projectile.type.length)fail('invalid projectile identity');
  if(!number(projectile.x,-1000,WORLD.width+1000)||!number(projectile.y,-1000,WORLD.height+1000)||!number(projectile.prevX,-1000,WORLD.width+1000)||!number(projectile.prevY,-1000,WORLD.height+1000)||!number(projectile.angle,-1e6,1e6)||!number(projectile.r,0,10000)||!number(projectile.life,0,10000)||!encodedIds(projectile.hit)||!number(projectile.damage,0,1e8)||!number(projectile.speed,0,1e6)||!number(projectile.pierce,-1,1000))fail('invalid projectile');
  // Homing basic shots acquire velocity on their first tick; skill shots must already have it.
  if(projectile.targetId!==undefined&&!entityId(projectile.targetId))fail('invalid projectile target');
  if(projectile.targetId===undefined?(!number(projectile.vx,-1e6,1e6)||!number(projectile.vy,-1e6,1e6)):(!optionalNumber(projectile.vx,-1e6,1e6)||!optionalNumber(projectile.vy,-1e6,1e6)))fail('invalid projectile velocity');
  for(const key of ['healing','burn','poison'])if(!optionalNumber(projectile[key],0,1e8))fail('invalid projectile effect');
  for(const key of ['burnDuration','poisonDuration'])if(!optionalNumber(projectile[key],0,10000))fail('invalid projectile duration');
  if((projectile.burn&&!number(projectile.burnDuration,0,10000))||(projectile.poison&&!number(projectile.poisonDuration,0,10000)))fail('missing projectile duration');
  if(projectile.bounceDamages!==undefined&&(!Array.isArray(projectile.bounceDamages)||projectile.bounceDamages.length>2||projectile.bounceDamages.some(d=>!number(d,0,1e8))))fail('invalid ricochet damage');
  if(projectile.bouncedIds!==undefined&&(!Array.isArray(projectile.bouncedIds)||projectile.bouncedIds.length>3||projectile.bouncedIds.some(id=>!entityId(id))))fail('invalid ricochet history');
  if(projectile.pairId!==undefined&&!entityId(projectile.pairId))fail('invalid split shot identity');
  for(const key of ['secondDamage','echoDamage','slowDuration','fromX','fromY'])if(!optionalNumber(projectile[key],0,1e8))fail('invalid expansion projectile');
}

function checkImpact(impact,units){
  if(!record(impact)||!entityId(impact.id)||typeof impact.kind!=='string'||!impact.kind.length||typeof impact.interruptible!=='boolean'||(impact.proc!==undefined&&typeof impact.proc!=='boolean'))fail('invalid scheduled impact');
  const owner=units.find(unit=>unit.id===impact.ownerId&&unit.kind==='hero');
  if(!owner||impact.team!==owner.team||!number(impact.executeAt,0,1e9)||!number(impact.delay,Number.EPSILON,10000)||!number(impact.x,-1000,WORLD.width+1000)||!number(impact.y,-1000,WORLD.height+1000)||!number(impact.r,0,10000)||typeof impact.shape!=='string'||!validGeometry(impact))fail('invalid impact geometry');
  for(const key of ['damage','healing','shield','burn','poison'])if(!optionalNumber(impact[key],0,1e8))fail('invalid impact effect');
  for(const key of ['stun','slowDuration','burnDuration','poisonDuration','duration','push'])if(!optionalNumber(impact[key],0,10000))fail('invalid impact duration');
  for(const key of ['slow','guard'])if(!optionalNumber(impact[key],0,1))fail('invalid impact factor');
}

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
  for(const [key,limit] of Object.entries(COLLECTION_LIMITS))if(state[key]!==undefined&&(!Array.isArray(state[key])||state[key].length>limit))fail('too many '+key);
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
      checkEvolution(unit.evolution,state.time);
      for(const [level,id] of Object.entries(unit.talents||{}))if(!HEROES[unit.heroId].talents.some(t=>t.level===Number(level)&&unit.level>=t.level&&t.choices.some(c=>c.id===id)))fail('invalid talent');
    }
    for(const status of Object.values(unit.status))if(!status||!number(status.remaining,-1,1e6)||(status.layers&&(!Array.isArray(status.layers)||status.layers.some(layer=>!number(layer.remaining,-1,1e6)))))fail('invalid status');
    checkExpansionUnit(unit,state.units);
  }
  const player=state.units.find(u=>u.id===state.playerId&&u.kind==='hero'&&u.isPlayer&&u.team===0);
  if(!player||state.units.filter(u=>u.isPlayer).length!==1)fail('missing player');
  if(!number(state.nextId,1,1e9)||state.nextId<=Math.max(...ids))fail('invalid entity sequence');
  if(!number(state.eventId,1,1e9)||!number(state.waveNumber,0,1e8)||!number(state.beastIndex,0,1e8))fail('invalid event sequence');
  for(const timer of ['waveIn','beastIn','planIn'])if(!number(state[timer],-10,1e9))fail('invalid timer');
  if(!state.stats||['kills','towers','beasts','damage'].some(key=>!Array.isArray(state.stats[key])||state.stats[key].length!==2||state.stats[key].some(n=>!number(n,0,1e10))))fail('invalid scoreboard');
  for(const projectile of state.projectiles)checkProjectile(projectile);
  for(const zone of state.zones)if(!record(zone)||!number(zone.x,-1000,WORLD.width+1000)||!number(zone.y,-1000,WORLD.height+1000)||!number(zone.r,0,10000)||!number(zone.life,-1,10000)||!number(zone.tickIn,-10,10000)||!validGeometry(zone)||(zone.type==='field'&&!encodedIds(zone.applied)))fail('invalid zone');
  if(state.teamPlans.length!==2||state.campStates.length>WORLD.camps.length)fail('invalid map state');
  for(const plan of state.teamPlans)if(!plan||!['lanes','push','defend','beast'].includes(plan.type)||![0,1,2].includes(plan.lane))fail('invalid team plan');
  for(const camp of state.campStates)if(!WORLD.camps.some(c=>c.id===camp.id)||typeof camp.spawned!=='boolean'||!number(camp.respawnIn,-10,1e6))fail('invalid camp');
  for(const camp of state.campStates)if(camp.scoutedEmptyUntil!==undefined&&(!Array.isArray(camp.scoutedEmptyUntil)||camp.scoutedEmptyUntil.length!==2||camp.scoutedEmptyUntil.some(t=>!number(t,0,state.time+10))))fail('invalid camp observation');
  for(const impact of state.scheduledImpacts||[])checkImpact(impact,state.units);
  if(!Array.isArray(state.lastSeen)||state.lastSeen.length!==2||!Array.isArray(state.assignments)||state.assignments.length!==2)fail('missing team information');
  for(const seen of state.lastSeen){
    if(!record(seen))fail('invalid remembered vision');
    for(const marker of Object.values(seen))if(!record(marker)||!number(marker.x,0,WORLD.width)||!number(marker.y,0,WORLD.height)||!number(marker.time,0,state.time)||!HEROES[marker.heroId])fail('invalid remembered marker');
  }
  if(state.assignments.some(jobs=>!record(jobs)))fail('invalid team jobs');
  const game=new Game({hero:player.heroId,difficulty:state.difficulty,seed:state.seed});
  Object.assign(game,decode(state));
  game.player=game.units.find(u=>u.id===game.playerId);game.grid=new Map();game.navCache=new Map();game.progressionDepth=0;
  // Older saves predate magazines; missing optional expansion state gets a safe default.
  for(const hero of game.units)if(hero.heroId==='vela')hero.magazine??={rounds:6,reloadRemaining:0,reloadDuration:1.4,openingShots:0};
  game.rebuildGrid();
  return game;
}
