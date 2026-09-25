import { isAiHouse, court } from './house-control.mjs';
import { BUILDINGS, RESOURCES } from './data.mjs';
import { buildingLevel, fortMaximum, tileProduction } from './economy.mjs';
import { familyCount, fortificationDefense, resolveFieldBattle, troopTotal } from './warfare.mjs';
import { bombardRange, structureAttackCheck } from './structures.mjs';
import { PLAYER, alive, armiesOf, atWar, canAfford, declareWar, distance, findPath, kingdom, log, neighbors, orderArmy, orderStructureAttack, passable, pay, relation, settlements, sizeOf, strength, treaty } from './core.mjs';
import { changeRelation, tradeBlocked } from './living.mjs';

export const PLAN_TYPES = ['invasion','jointWar','infrastructure','defendFrontier','seekAlliance','secureTrade','acquireResource','embargo','buildDefenses','recruitMilitary','expandTerritory'];
export const PLAN_STATUSES = ['Considering','Preparing','Committed','Executing','Completed','Abandoned'];
export const activePlan = p => !['Completed','Abandoned'].includes(p.status);
export const militaryPlan = p => ['invasion','jointWar','infrastructure'].includes(p.type);
export function initializePlans(s) {
  s.intrigue ??= {plans:[],audit:[],operations:[]};
  s.intrigue.plans ??= []; s.intrigue.audit ??= []; s.intrigue.operations ??= [];
}
export function audit(s,p,message) {
  initializePlans(s);
  s.intrigue.audit.push({turn:s.turn,planId:p?.id||null,message:String(message).slice(0,240)});
  s.intrigue.audit=s.intrigue.audit.slice(-240);
}
export function transitionPlan(s,p,status,reason='') {
  if(p.status===status)return;
  p.status=status;p.updatedTurn=s.turn;
  if(status==='Abandoned')p.cancellationReason=reason || 'The strategic situation changed.';
  audit(s,p,`${status}${reason?`: ${reason}`:''}`);
}
export function createPlan(s,actor,type,{target=null,targetTile=null,structure=null,resource=null,objective='',allies=[],requiredForces=20,requiredSiege=0,requiredResources={food:24,gold:18},delay=2,building=null,operationId=null}={}) {
  initializePlans(s);
  if(!PLAN_TYPES.includes(type)||!kingdom(s,actor)||target&&(!kingdom(s,target)||target===actor)||targetTile&&!s.tiles[targetTile])return null;
  const existing=s.intrigue.plans.find(p=>p.actor===actor&&p.type===type&&p.target===target&&p.targetTile===targetTile&&activePlan(p));
  if(existing){if(operationId&&!existing.operationId)existing.operationId=operationId;return existing;}
  if(s.intrigue.plans.filter(p=>p.actor===actor&&activePlan(p)).length>=4)return null;
  const p={id:`PLAN-${s.nextId++}`,actor,target,type,objective:objective.slice(0,200),targetTile,structure,resource,building,
    createdTurn:s.turn,desiredExecutionTurn:s.turn+delay,updatedTurn:s.turn,status:'Considering',requiredForces,requiredSiege,
    requiredResources,assignedArmies:[],allies:[...allies],discoveredBy:[],conditions:['Honor active treaties','Defend the capital first'],cancellationReason:null,wasAtWar:target?atWar(s,actor,target):false,operationId};
  s.intrigue.plans.push(p);audit(s,p,`Created: ${p.objective || type}`);prunePlans(s);return p;
}
export function prunePlans(s) {
  const live=s.intrigue.plans.filter(activePlan),old=s.intrigue.plans.filter(p=>!activePlan(p)).slice(-(120-live.length));
  s.intrigue.plans=s.intrigue.plans.filter(p=>live.includes(p)||old.includes(p));
  const ids=new Set(s.intrigue.plans.map(p=>p.id));
  // Reports are snapshots; retain their authoritative source for their whole lifetime.
  if(s.intelligence)s.intelligence.reports=s.intelligence.reports.filter(r=>!r.planId||ids.has(r.planId));
  s.intrigue.audit=s.intrigue.audit.filter(a=>!a.planId||ids.has(a.planId));
  const liveOperations=s.intrigue.operations.filter(o=>!['Completed','Abandoned'].includes(o.status));
  const oldOperations=s.intrigue.operations.filter(o=>['Completed','Abandoned'].includes(o.status)).slice(-(60-liveOperations.length));
  s.intrigue.operations=s.intrigue.operations.filter(o=>liveOperations.includes(o)||oldOperations.includes(o));
}
const protectedPeace=(s,a,b)=>['peace','non-aggression','alliance','vassalage'].some(type=>treaty(s,a,b,type));
const totalPower=(s,id)=>armiesOf(s,id).reduce((n,a)=>n+strength(a),0);
const qualityValue={poor:0,normal:1,rich:2,exceptional:3};
export function strategicValue(s,owner,tile,origin=null) {
  if(!tile||!passable(tile))return -Infinity;
  const exits=neighbors(s,tile).filter(passable).length,production=Object.values(tileProduction(tile,owner)).reduce((n,v)=>n+v,0);
  let score=production*1.5+(qualityValue[tile.quality]||0)*4+(tile.resource?6:0)+(tile.road?8:0)+(tile.river?9:0);
  if(tile.building==='fort')score+=28;
  if(tile.building==='town')score+=22;
  if(tile.building==='city')score+=35;
  if(tile.capital)score+=22;
  if(exits<=2)score+=22; else if(exits===3)score+=10;
  if(neighbors(s,tile).filter(n=>n.road).length>=3)score+=10;
  if(origin)score-=distance(origin,tile)*1.35;
  return score;
}
const operationName=(s,tile)=>`Operation ${String(tile.name||tile.id).replace(/[^A-Za-z0-9 -]/g,'').slice(0,48)||'Iron Gate'}`;
const nearestOwned=(s,owner,target)=>settlements(s,owner).slice().sort((a,b)=>distance(a,target)-distance(b,target)||a.id.localeCompare(b.id))[0];
export function operationForPlan(s,p) {return p?.operationId?(s.intrigue?.operations||[]).find(o=>o.id===p.operationId)||null:null;}
export function createJointOperation(s,lead,partner,target,{duration=8,targetTile=null}={}) {
  initializePlans(s);
  if(!kingdom(s,lead)||!kingdom(s,partner)||!kingdom(s,target)||new Set([lead,partner,target]).size!==3||![lead,partner,target].every(id=>alive(s,id)))return null;
  const proposed=[lead,partner],active=s.intrigue.operations.filter(o=>o.target===target&&!['Completed','Abandoned'].includes(o.status));
  const exact=active.find(o=>proposed.every(id=>o.participants.includes(id)));
  if(exact)return exact;
  // A second bilateral agreement may join an existing coalition before its attack
  // window opens. This keeps one authoritative operation instead of parallel wars.
  const expandable=active.find(o=>o.status==='Preparing'&&s.turn<o.attackWindow[0]&&o.participants.length<5&&
    proposed.some(id=>o.participants.includes(id))&&proposed.some(id=>!o.participants.includes(id)));
  if(expandable){
    const newcomers=proposed.filter(id=>!expandable.participants.includes(id));
    for(const id of newcomers){
      if(s.intrigue.plans.filter(p=>p.actor===id&&activePlan(p)).length>=4)return null;
      const rally=nearestOwned(s,id,s.tiles[expandable.targetTile]);if(!rally)return null;
      expandable.participants.push(id);expandable.roles[id]=expandable.requiredSiege?'Siege support':'Supporting flank';
      expandable.rallyPoints[id]=rally.id;expandable.requiredForces[id]=18;expandable.supply[id]={food:20,gold:14};
      const p=createPlan(s,id,'jointWar',{target,targetTile:expandable.targetTile,allies:expandable.participants.filter(x=>x!==id).slice(0,4),objective:expandable.objective,
        requiredForces:expandable.requiredForces[id],requiredSiege:expandable.roles[id]==='Siege support'?expandable.requiredSiege:0,requiredResources:{...expandable.supply[id]},
        delay:Math.max(0,expandable.attackWindow[0]-s.turn),operationId:expandable.id});
      if(!p){expandable.participants=expandable.participants.filter(x=>x!==id);delete expandable.roles[id];delete expandable.rallyPoints[id];delete expandable.requiredForces[id];delete expandable.supply[id];return null;}
      p.role=expandable.roles[id];p.rallyPoint=rally.id;transitionPlan(s,p,'Preparing','Joined the coalition; forces are gathering for the shared objective.');expandable.planIds.push(p.id);
    }
    for(const id of expandable.participants){
      const p=expandable.planIds.map(pid=>s.intrigue.plans.find(x=>x.id===pid)).find(p=>p?.actor===id);
      if(p)p.allies=expandable.participants.filter(x=>x!==id).slice(0,4);
    }
    expandable.updatedTurn=s.turn;expandable.exposure=Math.min(100,expandable.exposure+newcomers.length*5);
    audit(s,null,`${newcomers.map(id=>kingdom(s,id).name).join(' + ')} joined ${expandable.name} against ${kingdom(s,target).name}.`);
    prunePlans(s);return expandable;
  }
  const participants=[lead,partner];
  const objectiveTile=targetTile&&s.tiles[targetTile]?.owner===target?s.tiles[targetTile]:settlements(s,target).slice().sort((a,b)=>strategicValue(s,lead,b,nearestOwned(s,lead,b))-strategicValue(s,lead,a,nearestOwned(s,lead,a))||a.id.localeCompare(b.id))[0];
  if(!objectiveTile||participants.some(id=>s.intrigue.plans.filter(p=>p.actor===id&&activePlan(p)).length>=4))return null;
  const start=s.turn+3,end=Math.min(s.turn+Math.max(4,duration),start+2),siege=fortificationDefense(objectiveTile).bonus>0?2:0;
  const rallyPoints=Object.fromEntries(participants.map(id=>[id,nearestOwned(s,id,objectiveTile)?.id||null]));
  if(Object.values(rallyPoints).some(id=>!id))return null;
  const op={id:`OP-${s.nextId++}`,name:operationName(s,objectiveTile),target,targetTile:objectiveTile.id,objective:`Capture ${objectiveTile.name||objectiveTile.id}.`,
    participants,roles:{[lead]:'Main assault',[partner]:siege?'Siege support':'Supporting flank'},rallyPoints,requiredForces:{[lead]:28,[partner]:18},requiredSiege:siege,
    supply:{[lead]:{food:28,gold:20},[partner]:{food:20,gold:14}},attackWindow:[start,end],commitments:[],planIds:[],createdTurn:s.turn,updatedTurn:s.turn,status:'Preparing',exposure:8,cancellationReason:null};
  s.intrigue.operations.push(op);
  for(const id of participants){
    const p=createPlan(s,id,'jointWar',{target,targetTile:objectiveTile.id,allies:participants.filter(x=>x!==id),objective:op.objective,
      requiredForces:op.requiredForces[id],requiredSiege:op.roles[id]==='Siege support'?siege:0,requiredResources:{...op.supply[id]},delay:start-s.turn,operationId:op.id});
    if(!p){op.status='Abandoned';op.cancellationReason='A participant could not reserve a strategic plan slot.';break;}
    p.role=op.roles[id];p.rallyPoint=rallyPoints[id];transitionPlan(s,p,'Preparing','Joint operation ratified; forces are gathering for the shared objective.');op.planIds.push(p.id);
  }
  audit(s,null,`${op.name} created for ${participants.map(id=>kingdom(s,id).name).join(' + ')} against ${kingdom(s,target).name}.`);
  prunePlans(s);return op;
}
export function linkOperationCommitment(s,operationId,pledgeId) {
  const op=(s.intrigue?.operations||[]).find(o=>o.id===operationId);
  if(op&&pledgeId&&!op.commitments.includes(pledgeId)){op.commitments.push(pledgeId);op.updatedTurn=s.turn;}
  return op;
}
export function refreshOperations(s) {
  initializePlans(s);
  for(const op of s.intrigue.operations){
    if(['Completed','Abandoned'].includes(op.status))continue;
    const tile=s.tiles[op.targetTile],plans=op.planIds.map(id=>s.intrigue.plans.find(p=>p.id===id)).filter(Boolean);
    for(const p of plans.filter(p=>!isAiHouse(s,p.actor)&&activePlan(p))){
      const forces=armiesOf(s,p.actor),rally=s.tiles[op.rallyPoints[p.actor]],role=op.roles[p.actor];
      const forceReady=forces.some(a=>sizeOf(a)>=p.requiredForces&&rally&&distance(s.tiles[a.tile],rally)<=1&&(role!=='Siege support'||familyCount(a,'siege')>=Math.max(1,p.requiredSiege)));
      const ready=forceReady&&(p.suppliesCommitted||canAfford(kingdom(s,p.actor),p.requiredResources));
      if(p.status==='Considering')transitionPlan(s,p,'Preparing','Joint operation ratified; forces are gathering for the shared objective.');
      if(p.status==='Preparing'&&ready&&s.turn>=p.desiredExecutionTurn){
        if(!p.suppliesCommitted){pay(kingdom(s,p.actor),p.requiredResources);p.suppliesCommitted=true;audit(s,p,'Committed operation supplies: '+Object.entries(p.requiredResources).map(([r,n])=>`${n} ${r}`).join(', ')+'.');}
        transitionPlan(s,p,'Committed',role==='Siege support'?'Siege engines, escort troops and supplies are assembled at the rally point.':'The pledged force and supplies are assembled at the rally point.');
      }
    }
    if(!alive(s,op.target)||op.participants.some(id=>!alive(s,id))){op.status='Abandoned';op.cancellationReason='A participating House or target realm no longer survives.';op.updatedTurn=s.turn;continue;}
    if(tile&&op.participants.includes(tile.owner)){op.status='Completed';op.updatedTurn=s.turn;continue;}
    if(tile&&tile.owner!==op.target){op.status='Abandoned';op.cancellationReason='The objective changed hands outside the coalition.';op.updatedTurn=s.turn;continue;}
    if(plans.length&&plans.every(p=>p.status==='Abandoned')){op.status='Abandoned';op.cancellationReason=plans.map(p=>p.cancellationReason).filter(Boolean).at(-1)||'The coalition abandoned the operation.';op.updatedTurn=s.turn;continue;}
    const coordinatedReady=plans.length===op.participants.length&&plans.every(p=>['Committed','Executing','Completed'].includes(p.status));
    if(!coordinatedReady&&s.turn>op.attackWindow[1]){
      op.status='Abandoned';op.cancellationReason='The coalition failed to assemble its pledged forces, siege support and supplies inside the agreed attack window.';op.updatedTurn=s.turn;
      for(const p of plans.filter(activePlan))transitionPlan(s,p,'Abandoned',op.cancellationReason);
      for(const pledge of (s.pledges||[]).filter(p=>p.operationId===op.id&&p.status==='pending'))pledge.breached=true;
      continue;
    }
    if(coordinatedReady&&s.turn>=op.attackWindow[0]&&s.turn<=op.attackWindow[1])for(const p of plans.filter(p=>['Committed','Executing'].includes(p.status))){
      if(!atWar(s,p.actor,p.target)&&declareWar(s,p.actor,p.target)){p.wasAtWar=true;audit(s,p,`Entered the war as ${op.roles[p.actor]} when ${op.name}'s attack window opened.`);}
    }
    op.status=plans.some(p=>p.status==='Executing')?'Executing':coordinatedReady?'Committed':'Preparing';
    let obvious=8+op.participants.length*5+op.commitments.length*3;
    for(const id of op.participants){
      const rally=s.tiles[op.rallyPoints[id]],forces=armiesOf(s,id);
      const nearTarget=forces.filter(a=>distance(s.tiles[a.tile],tile)<=6),nearRally=rally?forces.filter(a=>distance(s.tiles[a.tile],rally)<=1):[];
      obvious+=Math.min(22,nearTarget.length*7)+Math.min(10,nearRally.length*4);
      if(forces.some(a=>a.path?.length||a.target===op.targetTile))obvious+=7;
      if(forces.some(a=>Object.entries(a.units||{}).some(([unit,n])=>n>0&&/ram|trebuchet|catapult|siege/i.test(unit))))obvious+=8;
    }
    if(plans.some(p=>['Committed','Executing'].includes(p.status)))obvious+=12;
    const counter=op.participants.reduce((n,id)=>n+buildingLevel(Object.values(s.tiles).find(t=>t.owner===id&&buildingLevel(t,'intelligenceOffice'))||{},'intelligenceOffice')*3+
      (s.intelligence?.agents||[]).filter(a=>a.owner===id&&a.assignedHouse===id&&a.status==='Embedded'&&a.mission==='counter').length*7,0);
    op.exposure=Math.max(0,Math.min(100,Math.round(obvious-counter)));op.updatedTurn=s.turn;
  }
}
export function operationPledgeComplete(s,pledge) {
  const op=(s.intrigue?.operations||[]).find(o=>o.id===pledge?.operationId);
  if(!op||!op.participants.includes(pledge.debtor))return null;
  const role=op.roles[pledge.debtor],plan=op.planIds.map(id=>s.intrigue.plans.find(p=>p.id===id)).find(p=>p?.actor===pledge.debtor),target=s.tiles[op.targetTile];
  const events=s.militaryEvents.filter(e=>e.turn>=pledge.created&&(pledge.eventAfter===undefined||(e.id||0)>pledge.eventAfter)&&e.attacker===pledge.debtor&&e.defender===op.target);
  const attacked=events.some(e=>['battle','capture','siege','structure'].includes(e.action));
  if(role==='Main assault')return attacked&&!!plan?.suppliesCommitted;
  const near=armiesOf(s,pledge.debtor).filter(a=>target&&distance(s.tiles[a.tile],target)<=2&&sizeOf(a)>=Math.max(1,Math.ceil((op.requiredForces[pledge.debtor]||1)*.7)));
  const rolePresent=atWar(s,pledge.debtor,op.target)&&(role==='Siege support'?near.some(a=>familyCount(a,'siege')>=Math.max(1,op.requiredSiege)):near.length>0);
  if(pledge.lastVerified!==s.turn){pledge.held=rolePresent?Math.min(2,(pledge.held||0)+1):0;pledge.lastVerified=s.turn;}
  const siegeAction=role==='Siege support'&&events.some(e=>e.action==='siege'||e.action==='structure');
  return !!plan?.suppliesCommitted&&(attacked||siegeAction||pledge.held>=2);
}
export function assaultAssessment(s,k,a,t) {
  const defenders=s.armies.filter(e=>e.tile===t.id&&e.owner!==a.owner&&sizeOf(e)>0&&(e.owner===t.owner||atWar(s,a.owner,e.owner)));
  if(!defenders.length)return {assault:true,bombard:false,defense:0,lossFraction:0,empty:true};
  const defense=defenders.reduce((n,e)=>n+strength(e,true,t),0),attack=strength(a,false,t);
  // Aggregate stacked garrisons conservatively and sample the actual phases.
  // This never consumes campaign RNG and avoids running 32 UI forecasts per AI tile.
  const garrison={...defenders[0],units:{},morale:Math.max(...defenders.map(e=>e.morale)),formation:defenders.some(e=>e.formation==='defensive')?'defensive':defenders[0].formation};
  for(const e of defenders)for(const [id,n] of Object.entries(e.units))garrison.units[id]=(garrison.units[id]||0)+n;
  let losses=0,wins=0;
  for(const roll of [.15,.5,.85]){
    const army=structuredClone(a),enemy=structuredClone(garrison);
    const result=resolveFieldBattle(army,enemy,t,{roll:()=>roll});
    losses=Math.max(losses,sizeOf(a)-troopTotal(army));
    if(result.winner===0)wins++;
  }
  const importance=t.capital?.04:['city','fort'].includes(t.building)?.02:0;
  const lossFraction=losses/Math.max(1,sizeOf(a));
  const assault=attack>=defense*(1.35-k.aggression*.3)&&wins>=2&&lossFraction<=.32+k.aggression*.12+importance;
  const breached={...t,walls:0,fortIntegrity:0};
  const bombard=!assault&&fortificationDefense(t).bonus>0&&bombardRange(a)>0&&assaultAssessment(s,k,a,breached).assault;
  return {assault,bombard,defense,lossFraction,empty:false};
}
export function dangerousTiles(s,k,a) {
  const tiles=new Set(s.armies.filter(e=>e.owner!==a.owner&&sizeOf(e)>0&&atWar(s,a.owner,e.owner)).map(e=>e.tile));
  return new Set([...tiles].filter(id=>!assaultAssessment(s,k,a,s.tiles[id]).assault));
}
function orderBombardment(s,k,a,t,avoid) {
  const type=t.walls>0?'wall':fortMaximum(t)&&(t.fortIntegrity??fortMaximum(t))>0?'fort':null;
  if(!type)return {ok:false};
  if(!structureAttackCheck(s,a,t,type,'bombard'))return orderStructureAttack(s,k.id,a.id,t.id,type,'bombard');
  // March to a legal firing position without marching through the garrison.
  const blocked=new Set([...avoid,t.id]);
  const positions=Object.values(s.tiles).filter(tile=>tile.id!==t.id&&!blocked.has(tile.id)&&distance(tile,t)<=bombardRange(a)&&!structureAttackCheck(s,{...a,tile:tile.id},t,type,'bombard'))
    .sort((x,y)=>distance(s.tiles[a.tile],x)-distance(s.tiles[a.tile],y)||distance(y,t)-distance(x,t)||x.id.localeCompare(y.id));
  for(const tile of positions){
    if(!findPath(s,a.tile,tile.id,k.id,false,blocked).length)continue;
    const result=orderArmy(s,k.id,a.id,tile.id,'move',blocked);if(result.ok)return result;
  }
  return {ok:false};
}
export function proposeInvasion(s,k,target,tile) {
  const allies=s.kingdoms.filter(o=>o.id!==k.id&&treaty(s,k.id,o.id,'alliance')&&atWar(s,o.id,target)).map(o=>o.id);
  if(allies.length){const op=createJointOperation(s,k.id,allies[0],target,{targetTile:tile.id});return op&&s.intrigue.plans.find(p=>p.operationId===op.id&&p.actor===k.id)||null;}
  return createPlan(s,k.id,'invasion',{target,targetTile:tile.id,objective:`Capture ${tile.name||tile.id}.`,requiredForces:30,delay:3});
}
export function infrastructureTarget(s,actor,target,origin) {
  const mounted=armiesOf(s,target).some(a=>familyCount(a,'mounted')>sizeOf(a)*.3);
  return Object.values(s.tiles).filter(t=>t.owner===target&&t.building&&!['city','town'].includes(t.building)).map(t=>{
    const production=tileProduction(t,target),value=Object.values(production).reduce((n,v)=>n+v,0);
    return {t,score:value+(t.building==='fort'?25:0)+(mounted&&t.building==='ranch'?25:0)-distance(origin,t)*2};
  }).sort((a,b)=>b.score-a.score||a.t.id.localeCompare(b.t.id))[0]?.t;
}
export function preparePlans(s,k,c) {
  initializePlans(s);refreshOperations(s);
  const plans=()=>s.intrigue.plans.filter(p=>p.actor===k.id&&activePlan(p));
  if(c.war&&!plans().some(militaryPlan)&&!c.threats.length) {
    const ranked=c.enemyTowns.map(t=>({t,ready:c.forces.some(a=>assaultAssessment(s,k,a,t).assault)}));
    const target=ranked.sort((a,b)=>Number(b.ready)-Number(a.ready)||distance(c.home,a.t)-distance(c.home,b.t))[0]?.t;
    if(target)createPlan(s,k.id,'invasion',{target:target.owner,targetTile:target.id,objective:`Capture ${target.name||target.id}.`,requiredForces:20,delay:0});
  }
  if(c.war&&!c.threats.length&&!plans().some(p=>p.type==='infrastructure')&&plans().length<3) {
    const target=c.enemyTowns[0]?.owner, tile=target&&infrastructureTarget(s,k.id,target,c.home);
    if(tile)createPlan(s,k.id,'infrastructure',{target,targetTile:tile.id,structure:tile.building,objective:`Disrupt ${kingdom(s,target).name}'s ${BUILDINGS[tile.building].name} production or defenses.`,requiredForces:16,requiredSiege:tile.building==='fort'?2:0,delay:1});
  }
  if(!plans().some(p=>!militaryPlan(p))) {
    const ally=s.kingdoms.find(o=>isAiHouse(s,o.id)&&o.id!==k.id&&alive(s,o.id)&&!atWar(s,k.id,o.id)&&relation(s,k.id,o.id).trust>=35&&relation(s,o.id,k.id).trust>=25&&!treaty(s,k.id,o.id,'alliance'));
    const partner=s.kingdoms.find(o=>isAiHouse(s,o.id)&&o.id!==k.id&&alive(s,o.id)&&!atWar(s,k.id,o.id)&&relation(s,k.id,o.id).opinion>=0&&!tradeBlocked(s,k.id,o.id)&&!treaty(s,k.id,o.id,'trade'));
    const rival=s.kingdoms.find(o=>o.id!==k.id&&alive(s,o.id)&&relation(s,k.id,o.id).grievance>=50&&relation(s,k.id,o.id).dependency<20&&!protectedPeace(s,k.id,o.id)&&!tradeBlocked(s,k.id,o.id));
    if(c.threats.length)createPlan(s,k.id,'defendFrontier',{targetTile:c.threats[0].tile.id,objective:`Defend ${c.threats[0].tile.name||c.threats[0].tile.id}.`,delay:0});
    else if(c.crisis){const resource=k.resources.food<40?'food':'gold';createPlan(s,k.id,'acquireResource',{resource,objective:`Rebuild ${resource} reserves.`,requiredResources:{[resource]:70},building:resource==='food'?'farm':'market'});}
    else if(ally)createPlan(s,k.id,'seekAlliance',{target:ally.id,objective:`Seek an alliance with ${ally.name}.`});
    else if(rival&&partner)createPlan(s,k.id,'embargo',{target:rival.id,allies:[partner.id],objective:`Restrict trade with ${rival.name}.`});
    else if(partner)createPlan(s,k.id,'secureTrade',{target:partner.id,objective:`Secure commerce with ${partner.name}.`});
    else createPlan(s,k.id,c.wary?'buildDefenses':c.forces.reduce((n,a)=>n+sizeOf(a),0)<30?'recruitMilitary':'expandTerritory',{
      targetTile:c.home.id,building:c.wary?'wall':'town',objective:c.wary?'Strengthen frontier defenses.':'Expand a sustainable realm.',requiredForces:36});
  }
  for(const p of plans()) {
    if(!alive(s,k.id)||p.target&&!alive(s,p.target)){transitionPlan(s,p,'Abandoned','A participating House lost its final settlement.');continue;}
    if(militaryPlan(p)) {
      if(protectedPeace(s,k.id,p.target)||p.wasAtWar&&!atWar(s,k.id,p.target)){transitionPlan(s,p,'Abandoned','A treaty or peace agreement prevents the attack.');continue;}
      if(c.threats.some(t=>t.tile.id===c.home.id)){transitionPlan(s,p,'Abandoned','Enemy armies threaten the capital; forces recalled to defend it.');continue;}
      if(s.tiles[p.targetTile]?.owner!==p.target){transitionPlan(s,p,s.tiles[p.targetTile]?.owner===k.id?'Completed':'Abandoned','The target changed ownership.');continue;}
      if(p.structure&&!buildingLevel(s.tiles[p.targetTile],p.structure)){transitionPlan(s,p,'Completed','The target structure no longer stands.');continue;}
      if(p.assignedArmies.length&&!p.assignedArmies.some(id=>s.armies.some(a=>a.id===id&&a.owner===k.id))){if(p.type==='jointWar')p.assignedArmies=[];else{transitionPlan(s,p,'Abandoned','The assigned army was destroyed.');continue;}}
      if((c.crisis||totalPower(s,p.target)>Math.max(1,totalPower(s,k.id))*3)&&s.turn>p.createdTurn+2){transitionPlan(s,p,'Abandoned',c.crisis?'Resources ran out; the realm must recover.':'Enemy military strength became overwhelming.');continue;}
      if(p.status==='Considering')transitionPlan(s,p,'Preparing','Gathering supplies and troops.');
      const forces=armiesOf(s,k.id),target=s.tiles[p.targetTile];
      const assessments=forces.map(a=>({a,assessment:assaultAssessment(s,k,a,target)}));
      // Keep the saved field as an equipment preference for recruitment, never
      // as permission to attack. Re-evaluate old plans against the real garrison.
      const op=operationForPlan(s,p),role=op?.roles?.[k.id];
      p.requiredSiege=op?(role==='Siege support'?op.requiredSiege:0):fortificationDefense(target).bonus>0&&!assessments.some(x=>x.assessment.assault)?2:0;
      const rally=op&&s.tiles[op.rallyPoints[k.id]],operationForceReady=forces.some(a=>sizeOf(a)>=p.requiredForces&&(!rally||distance(s.tiles[a.tile],rally)<=1)&&(role!=='Siege support'||familyCount(a,'siege')>=Math.max(1,p.requiredSiege)));
      const ready=(p.type==='jointWar'?operationForceReady:assessments.some(({a,assessment})=>(assessment.empty||sizeOf(a)>=p.requiredForces)&&(assessment.assault||assessment.bombard)))&&(p.suppliesCommitted||canAfford(k,p.requiredResources));
      if(ready&&s.turn>=p.desiredExecutionTurn&&p.status==='Preparing'){
        if(op&&!p.suppliesCommitted){pay(k,p.requiredResources);p.suppliesCommitted=true;audit(s,p,'Committed operation supplies: '+Object.entries(p.requiredResources).map(([r,n])=>`${n} ${r}`).join(', ')+'.');}
        transitionPlan(s,p,'Committed',role==='Siege support'?'Siege engines and operation supplies are ready.':'A viable force and operation supplies are ready.');
      }
      if(p.status==='Committed'&&!op&&!atWar(s,k.id,p.target)) {
        const probe={...s,wars:[...s.wars,[k.id,p.target].sort().join(':')]};
        if(!findPath(probe,c.home.id,p.targetTile,k.id).length){transitionPlan(s,p,'Abandoned','No legal route to the objective.');continue;}
        if(declareWar(s,k.id,p.target)){p.wasAtWar=true;audit(s,p,'Declared war to execute the campaign.');}
      }
    } else {
      if(p.status==='Considering')transitionPlan(s,p,'Preparing','Council preparing this objective.');
      // Political plans are intentions only. diplomacy.mjs negotiates them through the same acceptance/counteroffer rules used by players.
      if(p.type==='acquireResource'&&canAfford(k,p.requiredResources))transitionPlan(s,p,'Completed','Required reserves secured.');
      if(p.type==='defendFrontier'&&!c.threats.length)transitionPlan(s,p,'Completed','No enemy force threatens the frontier.');
      if(p.type==='recruitMilitary'&&c.forces.reduce((n,a)=>n+sizeOf(a),0)>=p.requiredForces)transitionPlan(s,p,'Completed','Required military strength mustered.');
    }
    if(activePlan(p)&&s.turn-p.createdTurn>24)transitionPlan(s,p,'Abandoned','Preparation stalled for 24 turns.');
  }
}
function executePoliticalPlan(s,p) {
  const k=kingdom(s,p.actor),r=relation(s,p.actor,p.target);
  if(!isAiHouse(s,p.target)){transitionPlan(s,p,'Abandoned','Player agreements require ratification in council.');return;}
  if(atWar(s,p.actor,p.target)&&p.type!=='embargo'){transitionPlan(s,p,'Abandoned','War prevents peaceful negotiation.');return;}
  const type={seekAlliance:'alliance',secureTrade:'trade',embargo:'embargo'}[p.type];
  const partner=p.type==='embargo'?p.allies[0]:p.target;
  if(p.type==='seekAlliance'&&(r.trust<35||relation(s,partner,p.actor).trust<25)||p.type==='secureTrade'&&tradeBlocked(s,p.actor,p.target)||p.type==='embargo'&&(!partner||atWar(s,p.actor,partner)||protectedPeace(s,p.actor,p.target)||relation(s,partner,p.target).opinion>0)) {
    transitionPlan(s,p,'Abandoned','The other court or an existing agreement no longer supports these terms.');return;
  }
  transitionPlan(s,p,'Committed','Both councils support compatible terms.');
  if(!treaty(s,p.actor,partner,type))s.treaties.push({id:`treaty-${s.nextId++}`,type,parties:[p.actor,partner],expires:s.turn+12,...(type==='embargo'?{targetId:p.target}:{})});
  if(type==='alliance'&&!treaty(s,p.actor,partner,'access'))s.treaties.push({id:`treaty-${s.nextId++}`,type:'access',parties:[p.actor,partner],expires:s.turn+12});
  for(const [a,b] of [[p.actor,partner],[partner,p.actor]])changeRelation(s,a,b,{trust:4,respect:3},`Courts concluded a ${type} agreement.`);
  log(s,`${k.name} and ${kingdom(s,partner).name} sign a ${type} accord.`, 'diplomacy');
  transitionPlan(s,p,'Completed','The agreement is now binding.');
}
export function plannedArmyOrder(s,k,a,c) {
  // Keep the field army at its muster point while the primary nearby siege
  // objective is preparing; do not send it away on a secondary economic raid.
  const pending=s.intrigue.plans.find(p=>p.actor===k.id&&['invasion','jointWar'].includes(p.type)&&p.status==='Preparing'&&(!p.operationId||s.turn<p.desiredExecutionTurn||!assaultAssessment(s,k,a,s.tiles[p.targetTile]).assault));
  if(pending&&sizeOf(a)>=pending.requiredForces) {
    const op=operationForPlan(s,pending),muster=op?.rallyPoints?.[k.id]&&s.tiles[op.rallyPoints[k.id]]||c.home;
    if(orderArmy(s,k.id,a.id,muster.id,'move').ok) {
      if(!pending.assignedArmies.includes(a.id)){pending.assignedArmies.push(a.id);audit(s,pending,`${a.id} mustering at ${muster.id} for reinforcements or siege support.`);}
      return pending;
    }
  }
  const plans=s.intrigue.plans.filter(p=>p.actor===k.id&&militaryPlan(p)&&['Committed','Executing'].includes(p.status));
  plans.sort((p,q)=>distance(s.tiles[a.tile],s.tiles[p.targetTile])-distance(s.tiles[a.tile],s.tiles[q.targetTile])||(p.type==='infrastructure'?1:-1));
  for(const p of plans) {
    const t=s.tiles[p.targetTile];
    if(!atWar(s,k.id,p.target)||t.owner!==p.target)continue;
    const assessment=assaultAssessment(s,k,a,t),op=operationForPlan(s,p);
    const support=op?op.participants.filter(id=>id!==k.id).flatMap(id=>armiesOf(s,id)).filter(x=>distance(s.tiles[x.tile],t)<=3).reduce((n,x)=>n+strength(x),0):0;
    const coalitionAssault=p.type==='jointWar'&&sizeOf(a)>=p.requiredForces&&strength(a)+support>=assessment.defense*1.05;
    if(!assessment.empty&&sizeOf(a)<p.requiredForces||!assessment.assault&&!assessment.bombard&&!coalitionAssault)continue;
    const avoid=dangerousTiles(s,k,a);if(assessment.assault||coalitionAssault)avoid.delete(t.id);
    const result=assessment.bombard&&!coalitionAssault?orderBombardment(s,k,a,t,avoid):p.type==='infrastructure'?orderStructureAttack(s,k.id,a.id,t.id,p.structure,'attack',avoid):orderArmy(s,k.id,a.id,t.id,'attack',avoid);
    if(!result.ok)continue;
    if(!p.assignedArmies.includes(a.id)){p.assignedArmies.push(a.id);audit(s,p,`${a.id} assigned to ${t.id}.`);}
    transitionPlan(s,p,'Executing',assessment.bombard?'Bombardment approach or firing orders issued to reduce assault losses.':'Real assault orders issued.');return p;
  }
  return null;
}
export function recordPlanAction(s,owner,a) {
  for(const p of s.intrigue?.plans||[])if(p.actor===owner&&activePlan(p)) {
    if(['build','complete','recruit'].includes(a.kind))audit(s,p,`${a.kind}: ${a.building||a.unit} at ${a.tile}.`);
    if(a.kind==='complete'&&(p.type==='expandTerritory'&&a.building==='town'||p.type==='buildDefenses'&&['wall','fort','watchtower'].includes(a.building)))transitionPlan(s,p,'Completed','The planned construction is complete.');
  }
}
export function finishPlans(s) {
  for(const p of s.intrigue?.plans||[])if(activePlan(p)&&militaryPlan(p)) {
    const op=operationForPlan(s,p),event=s.militaryEvents.find(e=>e.turn>=p.createdTurn&&e.attacker===p.actor&&e.tile===p.targetTile&&
      (p.type==='infrastructure'?e.action==='structure'&&e.destroyed&&e.structure===p.structure:e.action==='capture'));
    if(event||op&&op.participants.includes(s.tiles[p.targetTile]?.owner))transitionPlan(s,p,'Completed','The assigned military objective was achieved.');
    else if(p.wasAtWar&&!atWar(s,p.actor,p.target))transitionPlan(s,p,'Abandoned','Peace ended the campaign.');
  }
  refreshOperations(s);
}
// Human attack orders are real intentions too; AI spies may discover them.
export function recordPlayerPlans(s, actorHouseId = PLAYER) {
  for(const a of armiesOf(s,actorHouseId))if(['attack','bombard'].includes(a.order)&&s.tiles[a.target]?.owner&&atWar(s,actorHouseId,s.tiles[a.target].owner)) {
    const p=createPlan(s,actorHouseId,a.structureTarget?'infrastructure':'invasion',{target:s.tiles[a.target].owner,targetTile:a.target,structure:a.structureTarget||null,objective:`Attack ${s.tiles[a.target].name||a.target}.`,delay:0,requiredForces:1});
    if(p){p.assignedArmies=[a.id];p.playerOrderTracked=true;transitionPlan(s,p,'Executing','Player attack orders recorded.');}
  }
  for(const p of s.intrigue.plans.filter(p=>p.actor===actorHouseId&&activePlan(p)&&p.playerOrderTracked))if(!p.assignedArmies.some(id=>s.armies.some(a=>a.id===id&&a.target===p.targetTile&&['attack','bombard'].includes(a.order))))transitionPlan(s,p,'Abandoned','Player changed the assigned orders.');
}
export function validatePlans(s) {
  initializePlans(s);const fail=()=>{throw new Error('Damaged strategic plans.');};
  const integer=(n,min=0,max=100020)=>Number.isInteger(n)&&n>=min&&n<=max;
  const ps=s.intrigue.plans,ops=s.intrigue.operations;
  if(!Array.isArray(ps)||ps.length>120||new Set(ps.map(p=>p?.id)).size!==ps.length||!Array.isArray(ops)||ops.length>60||new Set(ops.map(o=>o?.id)).size!==ops.length||!Array.isArray(s.intrigue.audit)||s.intrigue.audit.length>240)fail();
  for(const p of ps) {
    if(!p||typeof p.id!=='string'||!/^PLAN-\d+$/.test(p.id)||!kingdom(s,p.actor)||p.target&&(!kingdom(s,p.target)||p.target===p.actor)||!PLAN_TYPES.includes(p.type)||!PLAN_STATUSES.includes(p.status)||typeof p.objective!=='string'||p.objective.length>200||p.targetTile&&!s.tiles[p.targetTile]||p.structure&&!Object.hasOwn(BUILDINGS,p.structure)||p.building&&!Object.hasOwn(BUILDINGS,p.building)||p.resource&&!RESOURCES.includes(p.resource))fail();
    if(!integer(p.createdTurn,1,s.turn)||!integer(p.updatedTurn,p.createdTurn,s.turn)||!integer(p.desiredExecutionTurn,p.createdTurn)||!integer(p.requiredForces,1,100000)||!integer(p.requiredSiege,0,100000)||!p.requiredResources||typeof p.requiredResources!=='object'||Array.isArray(p.requiredResources)||Object.entries(p.requiredResources).some(([r,n])=>!RESOURCES.includes(r)||!integer(n)))fail();
    if(!Array.isArray(p.assignedArmies)||p.assignedArmies.length>500||p.assignedArmies.some(id=>typeof id!=='string'||id.length>80)||!Array.isArray(p.allies)||p.allies.length>5||p.allies.some(id=>!kingdom(s,id)||id===p.actor)||!Array.isArray(p.conditions)||p.conditions.length>4||p.conditions.some(x=>typeof x!=='string'||x.length>160)||p.cancellationReason!==null&&(typeof p.cancellationReason!=='string'||p.cancellationReason.length>240)||p.status==='Abandoned'&&!p.cancellationReason||typeof p.wasAtWar!=='boolean')fail();
    if(p.discoveredBy!==undefined&&(!Array.isArray(p.discoveredBy)||p.discoveredBy.length>6||p.discoveredBy.some(id=>!kingdom(s,id))))fail();
    if(p.operationId!==undefined&&p.operationId!==null&&!ops.some(o=>o.id===p.operationId)||p.role!==undefined&&(typeof p.role!=='string'||p.role.length>80)||p.rallyPoint!==undefined&&p.rallyPoint!==null&&!s.tiles[p.rallyPoint]||p.playerOrderTracked!==undefined&&typeof p.playerOrderTracked!=='boolean'||p.suppliesCommitted!==undefined&&typeof p.suppliesCommitted!=='boolean')fail();
    if(militaryPlan(p)&&(!p.target||!p.targetTile)||p.type==='infrastructure'&&!p.structure)fail();
  }
  for(const o of ops){
    if(!o||typeof o.id!=='string'||!/^OP-\d+$/.test(o.id)||typeof o.name!=='string'||o.name.length>100||!kingdom(s,o.target)||!s.tiles[o.targetTile]||typeof o.objective!=='string'||o.objective.length>200||!PLAN_STATUSES.includes(o.status)||!integer(o.createdTurn,1,s.turn)||!integer(o.updatedTurn,o.createdTurn,s.turn)||!Number.isInteger(o.exposure)||o.exposure<0||o.exposure>100||!integer(o.requiredSiege,0,100000))fail();
    if(!Array.isArray(o.participants)||o.participants.length<2||o.participants.length>5||new Set(o.participants).size!==o.participants.length||o.participants.some(id=>!kingdom(s,id)||id===o.target))fail();
    if(!o.roles||!o.rallyPoints||!o.requiredForces||!o.supply||o.participants.some(id=>typeof o.roles[id]!=='string'||o.roles[id].length>80||!s.tiles[o.rallyPoints[id]]||!integer(o.requiredForces[id],1,100000)||!o.supply[id]||Object.entries(o.supply[id]).some(([r,n])=>!RESOURCES.includes(r)||!integer(n))))fail();
    if(!Array.isArray(o.attackWindow)||o.attackWindow.length!==2||!integer(o.attackWindow[0],o.createdTurn)||!integer(o.attackWindow[1],o.attackWindow[0])||!Array.isArray(o.commitments)||o.commitments.length>12||o.commitments.some(id=>typeof id!=='string'||id.length>80)||!Array.isArray(o.planIds)||o.planIds.length>5||o.planIds.some(id=>!ps.some(p=>p.id===id&&p.operationId===o.id))||o.cancellationReason!==null&&(typeof o.cancellationReason!=='string'||o.cancellationReason.length>240))fail();
  }
  for(const a of s.intrigue.audit)if(!a||!integer(a.turn,1,s.turn)||a.planId&&!ps.some(p=>p.id===a.planId)||typeof a.message!=='string'||a.message.length>240)fail();
}
