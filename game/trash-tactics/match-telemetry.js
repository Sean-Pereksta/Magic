// Match accounting is independent of rendering and uses successful purchases,
// real damage and lifecycle events. Sampling allocates only once per 5 seconds.
export const TELEMETRY_INTERVAL=5;
export const TELEMETRY_MAX_SAMPLES=721;
export const TELEMETRY_MAX_TEAM_EVENTS=256;
export const TELEMETRY_MAX_EVENTS=512;
const finite=n=>Number.isFinite(n)?n:0;
const stamp=g=>Math.round(finite(g.time)*1000)/1000;
const boundedPush=(list,item,limit)=>{list.push(item);if(list.length>limit)list.shift();};
const totals=()=>({physicalCollected:0,bonus:0,spent:0,damageDealt:0,damageTaken:0,kills:0,losses:0,workerLosses:0,trained:0,buildingsCompleted:0,expansions:0,lossValue:0,killValue:0,armyValue:0,firstExpansionAt:null,peakArmyCount:0,peakArmyValue:0,peakArmyAt:0,peakArmyCountAt:0,peakArmyValueAt:0});
const typeCount=(table,type,value=0)=>{const entry=table[type]??={count:0,value:0};entry.count++;entry.value+=value;};

export function initializeTelemetry(g,definitions){
 g.telemetry={schemaVersion:1,sampleInterval:TELEMETRY_INTERVAL,nextSampleAt:TELEMETRY_INTERVAL,definitions,events:[],report:null,finalized:false,
  teams:g.teams.map((_,team)=>({team,totals:totals(),spentByCategory:{train:0,research:0,build:0},lossesByType:{},killsByType:{},trainedByType:{},samples:[],sampleCompactions:0,events:[]}))};
 captureTelemetry(g);
}
function addEvent(g,team,type,extra={}){
 const telemetry=g.telemetry;if(!telemetry||telemetry.finalized)return;
 const event={time:stamp(g),team,type,...extra};
 boundedPush(telemetry.events,event,TELEMETRY_MAX_EVENTS);
 if(telemetry.teams[team])boundedPush(telemetry.teams[team].events,{...event},TELEMETRY_MAX_TEAM_EVENTS);
}
function armyPeak(g,team){
 const entry=g.telemetry.teams[team],definitions=g.telemetry.definitions;let count=0,value=0;
 for(const e of g.entities)if(e.hp>0&&e.team===team&&(definitions[e.type]?.family||e.type==='cd')){count++;value+=finite(definitions[e.type]?.cost);}
 if(count>entry.totals.peakArmyCount){entry.totals.peakArmyCount=count;entry.totals.peakArmyAt=stamp(g);entry.totals.peakArmyCountAt=stamp(g);}
 if(value>entry.totals.peakArmyValue){entry.totals.peakArmyValue=value;entry.totals.peakArmyValueAt=stamp(g);}
}
function pushSample(entry,sample){
 const samples=entry.samples,previous=samples.at(-1);if(previous?.time===sample.time){samples[samples.length-1]=sample;return;}
 if(samples.length>=TELEMETRY_MAX_SAMPLES){
  // Progressively thin history rather than dropping the beginning of a long
  // match. Every retained point remains an actual observation at its real time.
  const reduced=samples.filter((_,i)=>i===0||i%2===0||i===samples.length-1);samples.length=0;samples.push(...reduced);entry.sampleCompactions++;
 }
 samples.push(sample);
}
export function recordTelemetrySpend(g,team,category,type,amount){
 const entry=g.telemetry?.teams[team];if(!entry||g.telemetry.finalized)return;
 const paid=Math.max(0,finite(amount));entry.totals.spent+=paid;entry.spentByCategory[category]=(entry.spentByCategory[category]||0)+paid;
 addEvent(g,team,'purchase',{category,entityType:type,value:paid});
}
export function recordTelemetryHit(g,source,target,amount){
 const telemetry=g.telemetry;if(!telemetry||telemetry.finalized)return;
 // The engine has already subtracted the hit. Cap overkill at the health that
 // actually existed without changing the original hit event/stat semantics.
 const damage=Math.max(0,Math.min(finite(amount),Math.max(0,finite(target.hp)+finite(amount))));
 if(telemetry.teams[source.team])telemetry.teams[source.team].totals.damageDealt+=damage;
 if(telemetry.teams[target.team])telemetry.teams[target.team].totals.damageTaken+=damage;
 if(target.hp<=0&&source.team>=0&&source.team!==target.team)target._telemetryKillerTeam=source.team;
}
export function recordTelemetryDeath(g,entity,reason='destroyed'){
 const telemetry=g.telemetry;if(!telemetry||telemetry.finalized||entity._telemetryDeathRecorded)return;
 entity._telemetryDeathRecorded=true;
 const type=entity.type,definition=telemetry.definitions[type]||{},value=finite(definition.cost),team=entity.team,entry=telemetry.teams[team];
 const killer=Number.isInteger(entity._telemetryKillerTeam)?entity._telemetryKillerTeam:null;
 if(entry){
  entry.totals.losses++;entry.totals.lossValue+=value;if(type==='twig')entry.totals.workerLosses++;
  typeCount(entry.lossesByType,type,value);
  const eventType=reason==='eliminated'?'death':type==='twig'?'workerDeath':definition.building?'buildingDestroyed':'death';
  if(reason!=='eliminated')addEvent(g,team,eventType,{id:entity.id,entityType:type,x:entity.x,y:entity.y,value,killerTeam:killer,reason});
 }
 if(entry&&killer!==null&&killer!==team&&telemetry.teams[killer]){
  const winner=telemetry.teams[killer];winner.totals.kills++;winner.totals.killValue+=value;typeCount(winner.killsByType,type,value);
  addEvent(g,killer,'kill',{targetTeam:team,id:entity.id,entityType:type,x:entity.x,y:entity.y,value});
 }
}
export function recordTelemetryEvent(g,type,details){
 const telemetry=g.telemetry;if(!telemetry||telemetry.finalized)return;
 if(type==='hit'){recordTelemetryHit(g,details.source,details.target,details.amount);return;}
 const entity=details.entity,team=entity?.team??details.team,entry=telemetry.teams[team];
 if(type==='death'){recordTelemetryDeath(g,entity,entity._telemetryRemovalReason||'destroyed');return;}
 if(type==='trained'&&entry){
  entry.totals.trained++;entry.trainedByType[entity.type]=(entry.trainedByType[entity.type]||0)+1;
  armyPeak(g,team);
  addEvent(g,team,'trained',{id:entity.id,entityType:entity.type,x:entity.x,y:entity.y});
 }else if(type==='buildingCompleted'&&entry){
  entry.totals.buildingsCompleted++;addEvent(g,team,'buildingCompleted',{id:entity.id,entityType:entity.type,x:entity.x,y:entity.y});
  const home=g.starts[team];if(entity.type==='desk'&&home&&Math.hypot(entity.x-home.x,entity.y-home.y)>250){entry.totals.expansions++;entry.totals.firstExpansionAt??=stamp(g);addEvent(g,team,'expansion',{id:entity.id,entityType:'desk',x:entity.x,y:entity.y});}
 }else if(type==='researchCompleted'&&entry)addEvent(g,team,'researchCompleted',{entityType:details.researchType});
 else if(type==='eliminated'&&entry)addEvent(g,team,'teamEliminated');
}
export function captureTelemetry(g){
 const telemetry=g.telemetry;if(!telemetry||telemetry.finalized)return;
 const definitions=telemetry.definitions,counters=g.teams.map(()=>({armyCount:0,armyValue:0,workers:0,buildings:0,supplyUnits:0,supplyReserved:0,supplyCap:0}));
 for(const e of g.entities){
  if(e.hp<=0||!counters[e.team])continue;const c=counters[e.team],type=definitions[e.type]||{};
  if(type.family||e.type==='cd'){c.armyCount++;c.armyValue+=finite(type.cost);}
  if(e.type==='twig')c.workers++;
  if(type.building){c.buildings++;if(!e.constructing)c.supplyCap+=finite(type.supplyCap);for(const order of e.queue||[])if(order.kind==='train')c.supplyReserved+=finite(definitions[order.type]?.supply);}
  else c.supplyUnits+=finite(type.supply);
 }
 for(const entry of telemetry.teams){
  const team=entry.team,c=counters[team],t=entry.totals;
  t.physicalCollected=finite(g.stats.collected[team]);t.bonus=finite(g.stats.botBonus?.[team]);t.armyValue=c.armyValue;
  if(c.armyCount>t.peakArmyCount){t.peakArmyCount=c.armyCount;t.peakArmyAt=stamp(g);t.peakArmyCountAt=stamp(g);}if(c.armyValue>t.peakArmyValue){t.peakArmyValue=c.armyValue;t.peakArmyValueAt=stamp(g);}
  const sample={time:stamp(g),trash:finite(g.teams[team].trash),collected:t.physicalCollected,bonus:t.bonus,spent:t.spent,armyCount:c.armyCount,armyValue:c.armyValue,workers:c.workers,losses:t.losses,kills:t.kills,buildings:c.buildings,supplyUsed:c.supplyUnits+c.supplyReserved,supplyCap:Math.min(100,c.supplyCap)};
  pushSample(entry,sample);
 }
}
export function updateTelemetry(g){
 const telemetry=g.telemetry;if(!telemetry||telemetry.finalized||g.time+1e-8<telemetry.nextSampleAt)return;
 captureTelemetry(g);telemetry.nextSampleAt=(Math.floor((g.time+1e-8)/TELEMETRY_INTERVAL)+1)*TELEMETRY_INTERVAL;
}
function freezeDeep(value){if(value&&typeof value==='object'&&!Object.isFrozen(value)){for(const child of Object.values(value))freezeDeep(child);Object.freeze(value);}return value;}
export function getMatchReport(g){
 if(g.winner===null&&!g.teams[0]?.eliminated)return null;
 const telemetry=g.telemetry;if(!telemetry)return null;if(telemetry.report)return telemetry.report;
 captureTelemetry(g);
 const finalCounts=g.teams.map(()=>({}));for(const e of g.entities)if(e.hp>0&&finalCounts[e.team])finalCounts[e.team][e.type]=(finalCounts[e.team][e.type]||0)+1;
 const report={schemaVersion:1,mapId:g.map.id,mapName:g.map.name||g.map.id,difficulty:g.difficulty,players:g.players,duration:stamp(g),winner:g.winner,playerEliminated:!!g.teams[0]?.eliminated,
  sampleInterval:TELEMETRY_INTERVAL,sampleLimit:TELEMETRY_MAX_SAMPLES,teams:telemetry.teams.map(entry=>({...entry,finalCountByType:finalCounts[entry.team]})),events:telemetry.events};
 // The report must never share mutable accounting arrays with the simulation.
 telemetry.report=freezeDeep(JSON.parse(JSON.stringify(report)));telemetry.finalized=true;return telemetry.report;
}
