import { isAiHouse, court } from './house-control.mjs';
import { BUILDINGS, HOUSES, RESOURCES, UNITS } from './data.mjs';
import { buildingLevel, productionPlan } from './economy.mjs';
import { PLAYER, alive, armiesOf, atWar, distance, kingdom, log, pay, random, relation, settlements, sizeOf, treaty } from './core.mjs';
import { changeRelation, recordPoliticalMemory, stationedAmbassador } from './living.mjs';
import { activePlan, audit, militaryPlan, operationForPlan, PLAN_STATUSES } from './plans.mjs';
import { politicalAttitude } from './politics.mjs';

export const MISSIONS = {
  court:{name:'Observe Court',network:0,risk:.018,cost:0},
  economy:{name:'Economic Intelligence',network:20,risk:.025,cost:0},
  diplomacy:{name:'Diplomatic Intelligence',network:40,risk:.035,cost:1},
  military:{name:'Military Intelligence',network:60,risk:.045,cost:1},
  plans:{name:'Acquire Strategic Plans',network:40,risk:.08,cost:3},
  counter:{name:'Counterintelligence',network:0,risk:0,cost:0}
};
export const SPY_STATUSES=['Available','Traveling','Embedded','Compromised','Captured','Imprisoned','Dead'];
const NAMES=['Mara','Oren','Selene','Corvin','Lysa','Torren','Neris','Alden','Vela','Rook','Iris','Darin'];
const operational=a=>['Available','Traveling','Embedded','Compromised'].includes(a.status);
export const officeLevel=(s,owner)=>Math.max(0,...Object.values(s.tiles).filter(t=>t.owner===owner).map(t=>buildingLevel(t,'intelligenceOffice')));
export const spyCapacity=(s,owner)=>[0,1,3,5][officeLevel(s,owner)];
export function initializeEspionage(s){s.intelligence??={agents:[],reports:[],incidents:[],lastTurn:0};}
export const spyUpkeep=(s,owner)=>(s.intelligence?.agents||[]).filter(a=>a.owner===owner&&a.status!=='Dead').length*2;
export function recruitSpy(s,owner=PLAYER) {
  initializeEspionage(s);
  const k=kingdom(s,owner),agents=s.intelligence.agents.filter(a=>a.owner===owner&&a.status!=='Dead');
  if(s.outcome||!k||!alive(s,owner))return {ok:false,error:'This House cannot recruit agents.'};
  if(agents.length>=spyCapacity(s,owner))return {ok:false,error:'Build or upgrade an Intelligence Office for more agents (1 / 3 / 5). Captive agents retain their slot.'};
  if(k.resources.gold<60||k.commands<1)return {ok:false,error:'A spy costs 60 gold and one order, plus 2 gold upkeep each turn.'};
  pay(k,{gold:60});k.commands--;
  const id=s.nextId++,a={id:`spy-${id}`,name:`${NAMES[id%NAMES.length]} ${kingdom(s,owner).name.replace(/^House /,'')}`,owner,skill:35+Math.floor(random(s)*16),experience:0,assignedHouse:null,network:0,mission:'court',risk:0,status:'Available',arrivalTurn:null,captor:null,capturedTurn:null,lastReportTurn:0};
  s.intelligence.agents=s.intelligence.agents.filter(a=>a.status!=='Dead'||s.turn-(a.capturedTurn||0)<20);
  while(s.intelligence.agents.length>=90){const dead=s.intelligence.agents.findIndex(a=>a.status==='Dead');if(dead<0)break;s.intelligence.agents.splice(dead,1);}
  s.intelligence.agents.push(a);return {ok:true,spyId:a.id};
}
export function assignSpy(s,owner,id,host,mission='court') {
  const a=s.intelligence?.agents.find(a=>a.id===id&&a.owner===owner);
  if(s.outcome||!a||!operational(a)||!Object.hasOwn(MISSIONS,mission))return {ok:false,error:'Select an available agent and a valid mission.'};
  if(!host){Object.assign(a,{assignedHouse:null,network:0,mission:'court',status:'Available',arrivalTurn:null,risk:0});return {ok:true};}
  if(!alive(s,host)||host===owner&&mission!=='counter'||host!==owner&&mission==='counter')return {ok:false,error:'Choose a living foreign House, or your own House for counterintelligence.'};
  if(mission==='counter'&&officeLevel(s,owner)<2)return {ok:false,error:'Counterintelligence requires a Level II Intelligence Bureau.'};
  if(!spyCapacity(s,owner))return {ok:false,error:'An active Intelligence Office is required.'};
  if(a.assignedHouse===host&&a.status==='Embedded'){a.mission=mission;a.risk=detectionRisk(s,a);return {ok:true};}
  const home=settlements(s,owner)[0],target=settlements(s,host)[0];
  Object.assign(a,{assignedHouse:host,mission,network:0,status:host===owner?'Embedded':'Traveling',arrivalTurn:host===owner?s.turn:s.turn+Math.max(1,Math.ceil(distance(home,target)/5)),risk:0});
  return {ok:true};
}
export function counterStrength(s,owner) {
  if(officeLevel(s,owner)<2)return 0;
  return Math.min(.28,.025*officeLevel(s,owner)+(s.intelligence?.agents||[]).filter(a=>a.owner===owner&&a.assignedHouse===owner&&a.status==='Embedded'&&a.mission==='counter').reduce((n,a)=>n+.05+a.skill/1000+a.network/2000,0));
}
export function detectionRisk(s,a) {
  if(a.mission==='counter'||!a.assignedHouse)return 0;
  return Math.max(.005,Math.min(.55,MISSIONS[a.mission].risk+counterStrength(s,a.assignedHouse)+kingdom(s,a.assignedHouse).paranoia*.015+(atWar(s,a.owner,a.assignedHouse)?.02:0)-a.skill*.0003-a.network*.00015));
}
function incident(s,a,action,actor) {
  const item={turn:s.turn,spyId:a.id,owner:a.owner,actor,action};
  s.intelligence.incidents.push(item);s.intelligence.incidents=s.intelligence.incidents.slice(-60);
  if(!s.controllers && [a.owner,actor].includes(PLAYER))log(s,`${a.name}: ${action} by ${kingdom(s,actor).name}.`, 'intelligence');
}
export function captureSpy(s,a) {
  if(!a||!['Embedded','Traveling','Compromised'].includes(a.status)||a.mission==='counter'||!a.assignedHouse||a.assignedHouse===a.owner)return false;
  Object.assign(a,{status:'Captured',captor:a.assignedHouse,capturedTurn:s.turn,network:0,risk:0});
  changeRelation(s,a.captor,a.owner,{trust:-12,grievance:14,wariness:8},'A foreign spy was caught in our court.');
  recordPoliticalMemory(s,a.captor,a.owner,'espionage',`${kingdom(s,a.owner).name}'s agent ${a.name} was captured.`,8);
  incident(s,a,'Captured',a.captor);return true;
}
export function resolveCaptive(s,captor,id,action) {
  const a=s.intelligence?.agents.find(a=>a.id===id&&a.captor===captor&&['Captured','Imprisoned'].includes(a.status));
  if(s.outcome||!a||!['expel','imprison','ransom','exchange','execute'].includes(action))return {ok:false,error:'Select a prisoner held by your House.'};
  const formerCaptor=a.captor,owner=kingdom(s,a.owner);
  if(action==='imprison') {if(a.status==='Imprisoned')return {ok:false,error:'This spy is already imprisoned.'};a.status='Imprisoned';}
  else if(action==='execute') {
    const harm=atWar(s,captor,a.owner)?8:treaty(s,captor,a.owner,'alliance')?40:22;
    a.status='Dead';changeRelation(s,a.owner,captor,{trust:-harm,opinion:-harm,grievance:harm},'Our captured spy was executed.');
  } else {
    if(action==='ransom') {
      if(!isAiHouse(s,a.owner))return {ok:false,error:'The player must choose to pay this ransom from their captive agent card.'};
      if(owner.resources.gold<40)return {ok:false,error:'The owning House cannot pay the 40 gold ransom.'};
      pay(owner,{gold:40});pay(kingdom(s,captor),{gold:40},1);
    }
    if(action==='exchange') {
      const other=s.intelligence.agents.find(b=>b.owner===captor&&b.captor===a.owner&&['Captured','Imprisoned'].includes(b.status));
      if(!other)return {ok:false,error:'An exchange needs one of your spies held by this House.'};
      release(other);incident(s,other,'Exchanged',a.owner);
    }
    release(a);
    changeRelation(s,a.owner,captor,{opinion:action==='expel'?3:1},'Our spy was released from custody.');
  }
  incident(s,a,action,formerCaptor);return {ok:true};
}
function release(a){Object.assign(a,{status:'Available',assignedHouse:null,mission:'court',network:0,captor:null,capturedTurn:null,arrivalTurn:null,risk:0});}
export function paySpyRansom(s,owner,id) {
  const a=s.intelligence?.agents.find(a=>a.id===id&&a.owner===owner&&['Captured','Imprisoned'].includes(a.status));
  if(s.outcome||!a||kingdom(s,owner).resources.gold<40)return {ok:false,error:'A captive agent and 40 gold are required.'};
  // Ruthless wartime rulers refuse; no hidden transfer occurs without acceptance.
  if(atWar(s,owner,a.captor)&&kingdom(s,a.captor).honor<.4)return {ok:false,error:'The captor refuses ransom during this war.'};
  const captor=a.captor;pay(kingdom(s,owner),{gold:40});pay(kingdom(s,captor),{gold:40},1);release(a);incident(s,a,'Ransomed',captor);return {ok:true};
}
function addReport(s,a,data) {
  const r={id:`report-${s.nextId++}`,owner:a.owner,spyId:a.id,house:a.assignedHouse,turn:s.turn,mission:a.mission,planId:null,...data};
  s.intelligence.reports.push(r);
  // A bounded per-House history prevents AI reports from crowding out the player.
  s.intelligence.reports=s.intelligence.reports.filter(x=>s.intelligence.reports.filter(y=>y.owner===x.owner).slice(-24).includes(x));
  while(JSON.stringify(s.intelligence.reports).length>240000)s.intelligence.reports.shift();
  a.lastReportTurn=s.turn;return r;
}
export function discoverPlan(s,a,p) {
  if(!p||!s.intrigue?.plans.includes(p)||!s.intelligence?.agents.includes(a)||p.actor!==a.assignedHouse||a.status!=='Embedded'||a.network<MISSIONS.plans.network)return null;
  const op=operationForPlan(s,p),effective=a.network+Math.round((op?.exposure||0)*.35)-Math.round(counterStrength(s,p.actor)*100);
  const detail=effective>=80?3:effective>=58?2:1;
  p.discoveredBy ||= [];
  if(detail>=2&&activePlan(p)&&militaryPlan(p)&&p.target===a.owner&&!p.discoveredBy.includes(a.owner)){
    p.discoveredBy.push(a.owner);
    changeRelation(s,a.owner,p.actor,{trust:-6,grievance:8},'Verified intelligence revealed hostile military preparations.');
    recordPoliticalMemory(s,a.owner,p.actor,'hostile-plan',`Intelligence identified hostile preparations by ${kingdom(s,p.actor).name}.`,8);
  }
  const snapshot={actor:p.actor,status:p.status,createdTurn:p.createdTurn,observedTurn:s.turn};
  if(detail>=2)Object.assign(snapshot,{target:p.target,type:p.type});
  if(op&&detail>=2)snapshot.operation={id:op.id,participants:[...op.participants],exposure:op.exposure};
  if(detail>=3){
    Object.assign(snapshot,{objective:p.objective,targetTile:p.targetTile,structure:p.structure,assignedArmies:[...p.assignedArmies],desiredExecutionTurn:p.desiredExecutionTurn,requiredSiege:p.requiredSiege,cancellationReason:p.cancellationReason});
    if(op)Object.assign(snapshot.operation,{name:op.name,targetTile:op.targetTile,roles:{...op.roles},rallyPoints:{...op.rallyPoints},requiredForces:{...op.requiredForces},requiredSiege:op.requiredSiege,
      attackWindow:[...op.attackWindow],supply:structuredClone(op.supply),status:op.status});
  }
  const name=kingdom(s,p.actor).name,target=p.target?kingdom(s,p.target).name:'its own realm';
  let description;
  if(op)description=detail===1?`${name} appears to be preparing a military operation.`:
    detail===2?`${op.participants.map(id=>kingdom(s,id).name).join(' and ')} appear to be coordinating against ${target}.`:
      `${op.name}: ${op.objective} Participants: ${op.participants.map(id=>kingdom(s,id).name).join(', ')}. Target: ${s.tiles[op.targetTile].name||op.targetTile}. Planned attack window: turns ${op.attackWindow[0]}–${op.attackWindow[1]}. Status: ${op.status}; observed exposure ${op.exposure}%.`;
  else description=detail===1?`${name} has ${militaryPlan(p)?'military':'political or economic'} preparations.`:
    detail===2?`${name} has a ${p.type==='infrastructure'?'resource infrastructure attack':p.type} plan concerning ${target}.`:
      `${name}: ${p.objective} Status: ${p.status}. ${p.targetTile?`Target: ${s.tiles[p.targetTile].name||p.targetTile}. `:''}${p.assignedArmies.length?`Forces: ${p.assignedArmies.join(', ')}. `:''}Desired execution: turn ${p.desiredExecutionTurn}; conditions may change.${p.cancellationReason?` Abandoned: ${p.cancellationReason}`:''}`;
  const report=addReport(s,a,{planId:p.id,detail,snapshot,text:description});
  audit(s,p,`${a.name} discovered detail ${detail} intelligence.`);return report;
}
export function gatherIntelligence(s,a) {
  if(a.status!=='Embedded'||a.mission==='counter'||a.network<MISSIONS[a.mission].network)return null;
  const host=a.assignedHouse;
  if(a.mission==='plans') {
    const plans=(s.intrigue?.plans||[]).filter(p=>p.actor===host&&(activePlan(p)||s.intelligence.reports.some(r=>r.owner===a.owner&&r.planId===p.id&&r.snapshot.status!==p.status))); 
    const p=plans.sort((p,q)=>{
      const last=id=>Math.max(0,...s.intelligence.reports.filter(r=>r.owner===a.owner&&r.planId===id).map(r=>r.turn));
      return Number(activePlan(p))-Number(activePlan(q))||last(p.id)-last(q.id)||Number(q.target===a.owner)-Number(p.target===a.owner)||p.id.localeCompare(q.id);
    })[0];
    return p?discoverPlan(s,a,p):addReport(s,a,{text:'No active strategic plan was found in this court.',snapshot:{activePlans:0}});
  }
  if(a.mission==='court'||a.mission==='diplomacy') {
    const relations=HOUSES.filter(h=>h.id!==host).map(h=>({house:h.id,...politicalAttitude(s,host,h.id)}));
    const agreements=s.treaties.filter(t=>t.parties.includes(host)&&t.expires>s.turn).map(t=>({type:t.type,parties:[...t.parties],expires:t.expires,targetId:t.targetId||null}));
    const negotiations=(s.intrigue?.plans||[]).filter(p=>p.actor===host&&activePlan(p)&&['seekAlliance','secureTrade','embargo','jointWar'].includes(p.type));
    // Negotiation claims also retain a direct plan reference via a separate report.
    if(a.mission==='diplomacy'&&negotiations.length)return discoverPlan(s,a,negotiations[0]);
    return addReport(s,a,{text:`Court attitudes and ${agreements.length} active agreements observed.`,snapshot:{relations:a.mission==='court'?relations.slice(0,2):relations,agreements}});
  }
  if(a.mission==='economy') {
    const k=kingdom(s,host),production=productionPlan(s,host).gross;
    const shortages=RESOURCES.filter(r=>k.resources[r]<40),surpluses=RESOURCES.filter(r=>k.resources[r]>180);
    const construction=Object.values(s.tiles).filter(t=>t.owner===host&&t.project).slice(0,8).map(t=>({tile:t.id,type:t.project.type,remaining:t.project.remaining}));
    return addReport(s,a,{text:`Shortages: ${shortages.join(', ')||'none'}. Surpluses: ${surpluses.join(', ')||'none'}. ${construction.length} construction projects observed.`,snapshot:{shortages,surpluses,production,construction}});
  }
  const forces=armiesOf(s,host),total=forces.reduce((n,a)=>n+sizeOf(a),0);
  const armies=forces.map(a=>({id:a.id,tile:a.tile,minimum:Math.floor(sizeOf(a)/10)*10,maximum:Math.floor(sizeOf(a)/10)*10+9,mobilizing:!!a.path.length||a.order==='bombard',composition:Object.keys(a.units).filter(u=>a.units[u]>0)}));
  const recruitment=(s.strategy?.history||[]).filter(r=>r.turn>=s.turn-2).flatMap(r=>r.houses.filter(h=>h.owner===host).flatMap(h=>h.actions.filter(a=>a.kind==='recruit')));
  return addReport(s,a,{text:`Approximately ${Math.floor(total/10)*10}–${Math.floor(total/10)*10+9} troops; ${armies.length} armies; ${armies.filter(a=>a.mobilizing).length} mobilizing. ${recruitment.length} recent musters observed.`,snapshot:{armies,recruitment}});
}
export function resolveEspionage(s,{roll=()=>random(s)}={}) {
  initializeEspionage(s);if(s.intelligence.lastTurn>=s.turn||s.outcome)return;
  for(const a of s.intelligence.agents) {
    if(a.status==='Dead')continue;
    if(!alive(s,a.owner)){a.status='Dead';continue;}
    if(['Captured','Imprisoned'].includes(a.status)) {
      if(!alive(s,a.captor)){release(a);continue;}
      if(isAiHouse(s,a.captor)&&a.capturedTurn<s.turn&&a.status==='Captured') {
        const captor=kingdom(s,a.captor);
        const action=atWar(s,a.owner,a.captor)&&captor.honor<.4?'execute':s.intelligence.agents.some(b=>b.owner===a.captor&&b.captor===a.owner&&['Captured','Imprisoned'].includes(b.status))?'exchange':captor.honor>=.8?'expel':isAiHouse(s,a.owner)&&kingdom(s,a.owner).resources.gold>=40?'ransom':'imprison';
        resolveCaptive(s,a.captor,a.id,action);
      }
      continue;
    }
    if(!a.assignedHouse)continue;
    if(!alive(s,a.assignedHouse)||!officeLevel(s,a.owner)||a.mission==='counter'&&officeLevel(s,a.owner)<2){release(a);continue;}
    if(a.status==='Compromised'){release(a);continue;}
    if(a.status==='Traveling') {if(s.turn<a.arrivalTurn)continue;a.status='Embedded';}
    const k=kingdom(s,a.owner),cost=MISSIONS[a.mission].cost;
    if(k.resources.gold<spyUpkeep(s,a.owner)+cost){a.network=Math.max(0,a.network-5);continue;}
    pay(k,{gold:cost});
    a.network=Math.min(100,a.network+5+Math.floor(a.skill/20)+officeLevel(s,a.owner)*2);
    a.experience++;a.skill=Math.min(100,a.skill+(a.experience%5===0?1:0));a.risk=detectionRisk(s,a);
    const counter=a.mission!=='counter'?counterStrength(s,a.assignedHouse):0;
    if(counter>=.08&&roll()<Math.min(.3,counter*.8)&&!s.intelligence.incidents.some(i=>i.turn===s.turn&&i.spyId===a.id&&i.action.startsWith('Counterintelligence detected')))
      incident(s,a,`Counterintelligence detected ${kingdom(s,a.owner).name} investigating ${MISSIONS[a.mission].name.toLowerCase()}`,a.assignedHouse);
    if(a.mission!=='counter'&&roll()<a.risk) {
      if(roll()<.8)captureSpy(s,a);else {a.status='Compromised';incident(s,a,'Compromised; returning home',a.assignedHouse);}
      continue;
    }
    if(a.mission!=='counter'&&s.turn-a.lastReportTurn>=3&&roll()<.4+a.network/200+a.skill/300)gatherIntelligence(s,a);
  }
  s.intelligence.lastTurn=s.turn;
}
export function runAISpies(s,k) {
  initializeEspionage(s);if(!officeLevel(s,k.id))return false;
  const own=s.intelligence.agents.filter(a=>a.owner===k.id&&a.status!=='Dead');
  let recruited=false;
  if(own.length<Math.min(2,spyCapacity(s,k.id))&&k.resources.gold>100&&k.commands>1){const r=recruitSpy(s,k.id);recruited=r.ok;if(r.ok)own.push(s.intelligence.agents.find(a=>a.id===r.spyId));}
  const plans=(s.intrigue?.plans||[]).filter(p=>p.actor===k.id&&activePlan(p));
  const target=plans.find(militaryPlan)?.target||Object.entries(k.relations).filter(([id])=>alive(s,id)).sort((a,b)=>(b[1].fear+b[1].wariness+b[1].dependency)-(a[1].fear+a[1].wariness+a[1].dependency))[0]?.[0];
  for(const [i,a] of own.entries())if(operational(a)) {
    if(i===1&&officeLevel(s,k.id)>=2&&a.mission!=='counter')assignSpy(s,k.id,a.id,k.id,'counter');
    else if(target&&a.status==='Available')assignSpy(s,k.id,a.id,target,relation(s,k.id,target).dependency>=20?'diplomacy':'plans');
  }
  return recruited;
}
export function knownRelationships(s,viewer,host) {
  const reports=(s.intelligence?.reports||[]).filter(r=>r.owner===viewer&&r.house===host&&r.snapshot.relations).slice().reverse();
  return HOUSES.filter(h=>h.id!==host).map(h=>{
    if(h.id===viewer||host===viewer||stationedAmbassador(s,viewer,host))return {house:h.id,...politicalAttitude(s,host,h.id),turn:s.turn};
    const report=reports.find(r=>r.snapshot.relations.some(x=>x.house===h.id)),found=report?.snapshot.relations.find(x=>x.house===h.id);
    if(found)return {...found,turn:report.turn};
    return {house:h.id,label:atWar(s,host,h.id)?'At war':treaty(s,host,h.id,'alliance')?'Public alliance':'Unknown',tone:'',reasons:[]};
  });
}
export function visiblePlans(s,viewer,host=null) {
  const seen=new Set();
  return (s.intelligence?.reports||[]).filter(r=>r.owner===viewer&&r.planId&&(!host||r.house===host)).slice().reverse().filter(r=>!seen.has(r.planId)&&seen.add(r.planId));
}
export function validateEspionage(s) {
  initializeEspionage(s);const d=s.intelligence,fail=()=>{throw new Error('Damaged intelligence data.');};
  const integer=(n,min=0,max=10000000)=>Number.isInteger(n)&&n>=min&&n<=max;
  if(!Array.isArray(d.agents)||d.agents.length>90||new Set(d.agents.map(a=>a?.id)).size!==d.agents.length||!Array.isArray(d.reports)||d.reports.length>144||!Array.isArray(d.incidents)||d.incidents.length>60||!integer(d.lastTurn,0,s.turn))fail();
  for(const a of d.agents) {
    if(!a||!/^spy-\d+$/.test(a.id)||typeof a.name!=='string'||a.name.length>80||!kingdom(s,a.owner)||a.assignedHouse&&!kingdom(s,a.assignedHouse)||!integer(a.skill,0,100)||!integer(a.network,0,100)||!integer(a.experience)||!Object.hasOwn(MISSIONS,a.mission)||!SPY_STATUSES.includes(a.status)||!Number.isFinite(a.risk)||a.risk<0||a.risk>1||!integer(a.lastReportTurn,0,s.turn)||a.arrivalTurn!==null&&!integer(a.arrivalTurn,1,s.turn+20)||a.captor&&!kingdom(s,a.captor)||a.capturedTurn!==null&&!integer(a.capturedTurn,1,s.turn))fail();
    if(['Captured','Imprisoned'].includes(a.status)&&(!a.captor||a.captor===a.owner)||['Embedded','Traveling','Compromised'].includes(a.status)&&!a.assignedHouse)fail();
  }
  if(new Set(d.reports.map(r=>r?.id)).size!==d.reports.length)fail();
  for(const r of d.reports) {
    if(!r||typeof r.id!=='string'||r.id.length>80||!kingdom(s,r.owner)||!kingdom(s,r.house)||!integer(r.turn,1,s.turn)||!Object.hasOwn(MISSIONS,r.mission)||typeof r.spyId!=='string'||typeof r.text!=='string'||r.text.length>1200||!r.snapshot||typeof r.snapshot!=='object'||Array.isArray(r.snapshot)||JSON.stringify(r.snapshot).length>14000)fail();
    if(r.planId) {
      const p=s.intrigue.plans.find(p=>p.id===r.planId),x=r.snapshot;
      if(!p||p.actor!==r.house||x.actor!==p.actor||x.target!==undefined&&x.target!==p.target||x.createdTurn!==p.createdTurn||x.observedTurn!==r.turn||r.turn<p.createdTurn||!PLAN_STATUSES.includes(x.status)||!integer(r.detail,1,3)||r.detail>=2&&(x.target!==p.target||x.type!==p.type)||r.detail>=3&&(x.targetTile!==p.targetTile||x.structure!==p.structure||x.objective!==p.objective||x.desiredExecutionTurn!==p.desiredExecutionTurn||x.requiredSiege!==p.requiredSiege||x.cancellationReason!==p.cancellationReason&&x.status==='Abandoned'||!Array.isArray(x.assignedArmies)||x.assignedArmies.length>500||x.assignedArmies.some(id=>typeof id!=='string'||id.length>80)))fail();
      if(r.detail<3&&['targetTile','structure','assignedArmies','objective','desiredExecutionTurn'].some(key=>Object.hasOwn(x,key))||r.detail<2&&Object.hasOwn(x,'type'))fail();
      if(x.operation!==undefined){
        const op=operationForPlan(s,p),o=x.operation;
        if(!op||r.detail<2||!o||o.id!==op.id||!Array.isArray(o.participants)||o.participants.some((id,i)=>id!==op.participants[i])||!integer(o.exposure,0,100))fail();
        if(r.detail>=3&&(!(typeof o.name==='string'&&o.name.length<=100)||o.targetTile!==op.targetTile||!o.roles||!o.rallyPoints||!o.requiredForces||!integer(o.requiredSiege,0,100000)||!Array.isArray(o.attackWindow)||o.attackWindow.length!==2||o.attackWindow.some(v=>!integer(v,1,100020))||!o.supply||!PLAN_STATUSES.includes(o.status)))fail();
        if(r.detail<3&&['name','targetTile','roles','rallyPoints','requiredForces','requiredSiege','attackWindow','supply','status'].some(key=>Object.hasOwn(o,key)))fail();
      }
    } else {
      const x=r.snapshot, text=(v,max=240)=>typeof v==='string'&&v.length<=max, list=(v,max)=>Array.isArray(v)&&v.length<=max;
      if(r.mission==='plans'){if(x.activePlans!==0)fail();}
      else if(['court','diplomacy'].includes(r.mission)) {
        if(!list(x.relations,5)||x.relations.some(row=>!row||!kingdom(s,row.house)||!text(row.label,60)||!text(row.tone,100)||!list(row.reasons,4)||row.reasons.some(v=>!text(v)))||!list(x.agreements,1000)||x.agreements.some(t=>!t||!text(t.type,40)||!list(t.parties,2)||t.parties.some(id=>!kingdom(s,id))||!integer(t.expires)))fail();
      } else if(r.mission==='economy') {
        if(!list(x.shortages,RESOURCES.length)||!list(x.surpluses,RESOURCES.length)||[...x.shortages,...x.surpluses].some(id=>!RESOURCES.includes(id))||!x.production||!RESOURCES.every(id=>integer(x.production[id],0,1000000))||!list(x.construction,8)||x.construction.some(p=>!p||!s.tiles[p.tile]||!Object.hasOwn(BUILDINGS,p.type)||!integer(p.remaining,1,8)))fail();
      } else if(r.mission==='military') {
        if(!list(x.armies,500)||x.armies.some(a=>!a||!text(a.id,80)||!s.tiles[a.tile]||!integer(a.minimum)||!integer(a.maximum)||a.maximum<a.minimum||!list(a.composition,Object.keys(UNITS).length)||a.composition.some(id=>!Object.hasOwn(UNITS,id)))||!list(x.recruitment,96)||x.recruitment.some(a=>!a||a.kind!=='recruit'||!Object.hasOwn(UNITS,a.unit)||!s.tiles[a.tile]||!integer(a.count,1,12)))fail();
      } else fail();
    }
  }
  for(const i of d.incidents)if(!i||!integer(i.turn,1,s.turn)||!kingdom(s,i.owner)||!kingdom(s,i.actor)||typeof i.spyId!=='string'||typeof i.action!=='string'||i.action.length>100)fail();
}
