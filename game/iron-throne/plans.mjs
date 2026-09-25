import { planningView } from './ai-knowledge.mjs';
import { memberOperation } from './cooperation-state.mjs';
import { isAiHouse, court } from './house-control.mjs';
import { BUILDINGS, RESOURCES } from './data.mjs';
import { buildingLevel, fortMaximum, tileProduction } from './economy.mjs';
import { familyCount, fortificationDefense, resolveFieldBattle, troopTotal } from './warfare.mjs';
import { bombardRange, structureAttackCheck } from './structures.mjs';
import { PLAYER, alive, armiesOf, atWar, canAfford, declareWar, distance, findPath, kingdom, log, orderArmy, orderStructureAttack, relation, settlements, sizeOf, strength, treaty } from './core.mjs';
import { changeRelation, tradeBlocked } from './living.mjs';
import { negotiatePoliticalPlan, cooperationInterest } from './strategic-diplomacy.mjs';
import { proposeJointOperation } from './operations.mjs';
import { rankObjectives, strategicLocation } from './strategic-geography.mjs';

export const PLAN_TYPES = ['invasion','jointWar','infrastructure','defendFrontier','seekAlliance','secureTrade','acquireResource','embargo','buildDefenses','recruitMilitary','expandTerritory'];
export const PLAN_STATUSES = ['Considering','Preparing','Committed','Executing','Completed','Abandoned'];
export const activePlan = p => !['Completed','Abandoned'].includes(p.status);
export const militaryPlan = p => ['invasion','jointWar','infrastructure'].includes(p.type);
export function initializePlans(s) { s.intrigue ??= {plans:[],audit:[]}; }
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
  const existing=s.intrigue.plans.find(p=>p.actor===actor&&p.type===type&&p.target===target&&p.targetTile===targetTile&&(p.operationId||null)===operationId&&activePlan(p));
  if(existing)return existing;
  if(s.intrigue.plans.filter(p=>p.actor===actor&&activePlan(p)).length>=4)return null;
  const p={id:`PLAN-${s.nextId++}`,actor,target,type,objective:objective.slice(0,200),targetTile,structure,resource,building,
    createdTurn:s.turn,desiredExecutionTurn:s.turn+delay,updatedTurn:s.turn,status:'Considering',requiredForces,requiredSiege,
    requiredResources,assignedArmies:[],allies:[...allies],discoveredBy:[],conditions:['Honor active treaties','Defend the capital first'],cancellationReason:null,wasAtWar:target?atWar(s,actor,target):false};
  if(operationId)p.operationId=operationId;
  s.intrigue.plans.push(p);audit(s,p,`Created: ${p.objective || type}`);prunePlans(s);return p;
}
export function prunePlans(s) {
  const live=s.intrigue.plans.filter(activePlan),old=s.intrigue.plans.filter(p=>!activePlan(p)).slice(-(120-live.length));
  s.intrigue.plans=s.intrigue.plans.filter(p=>live.includes(p)||old.includes(p));
  const ids=new Set(s.intrigue.plans.map(p=>p.id));
  // Reports are snapshots; retain their authoritative source for their whole lifetime.
  if(s.intelligence)s.intelligence.reports=s.intelligence.reports.filter(r=>!r.planId||ids.has(r.planId));
  s.intrigue.audit=s.intrigue.audit.filter(a=>!a.planId||ids.has(a.planId));
}
const protectedPeace=(s,a,b)=>['peace','non-aggression','alliance','vassalage'].some(type=>treaty(s,a,b,type));
const totalPower=(s,id)=>armiesOf(s,id).reduce((n,a)=>n+strength(a),0);
export function assaultAssessment(s,k,a,t) {
  if(!s.knowledgeView) { s=planningView(s,k.id); t=s.tiles[t.id]; }
  const defenders=s.armies.filter(e=>e.tile===t.id&&e.owner!==a.owner&&sizeOf(e)>0&&(e.owner===t.owner||atWar(s,a.owner,e.owner)));
  if(!defenders.length) {
    if(t.fog && t.fog!=='visible' && t.owner!==k.id) return {assault:true,bombard:false,defense:0,lossFraction:.15,empty:false,uncertain:true};
    return {assault:true,bombard:false,defense:0,lossFraction:0,empty:true};
  }
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
  s=planningView(s,k.id);
  const tiles=new Set(s.armies.filter(e=>e.owner!==a.owner&&sizeOf(e)>0&&atWar(s,a.owner,e.owner)).map(e=>e.tile));
  return new Set([...tiles].filter(id=>!assaultAssessment(s,k,a,s.tiles[id]).assault));
}
function orderBombardment(s,k,a,t,avoid) {
  const world=s; s=planningView(world,k.id);
  const type=t.walls>0?'wall':fortMaximum(t)&&(t.fortIntegrity??fortMaximum(t))>0?'fort':null;
  if(!type)return {ok:false};
  if(!structureAttackCheck(s,a,t,type,'bombard'))return orderStructureAttack(world,k.id,a.id,t.id,type,'bombard');
  // March to a legal firing position without marching through the garrison.
  const blocked=new Set([...avoid,t.id]);
  const positions=Object.values(s.tiles).filter(tile=>tile.id!==t.id&&!blocked.has(tile.id)&&distance(tile,t)<=bombardRange(a)&&!structureAttackCheck(s,{...a,tile:tile.id},t,type,'bombard'))
    .sort((x,y)=>distance(s.tiles[a.tile],x)-distance(s.tiles[a.tile],y)||distance(y,t)-distance(x,t)||x.id.localeCompare(y.id));
  for(const tile of positions){
    if(!findPath(s,a.tile,tile.id,k.id,false,blocked).length)continue;
    const result=orderArmy(world,k.id,a.id,tile.id,'move',blocked);if(result.ok)return result;
  }
  return {ok:false};
}
export function proposeInvasion(s,k,target,tile) {
  const operation=proposeJointOperation(s,k,target,tile);
  if(operation)return s.intrigue.plans.find(p=>p.operationId===operation.id&&p.actor===k.id);
  return createPlan(s,k.id,'invasion',{target,targetTile:tile.id,objective:`Capture ${tile.name||tile.id}.`,requiredForces:30,
    delay:3});
}
export function infrastructureTarget(s,actor,target,origin) {
  s=planningView(s,actor);
  const mounted=armiesOf(s,target).some(a=>familyCount(a,'mounted')>sizeOf(a)*.3);
  return Object.values(s.tiles).filter(t=>t.owner===target&&t.building&&!['city','town'].includes(t.building)).map(t=>{
    const production=tileProduction(t,target),value=Object.values(production).reduce((n,v)=>n+v,0);
    return {t,score:strategicLocation(s,t,actor).value+value+(t.building==='fort'?25:0)+(mounted&&t.building==='ranch'?25:0)-distance(origin,t)*2};
  }).sort((a,b)=>b.score-a.score||a.t.id.localeCompare(b.t.id))[0]?.t;
}
export function preparePlans(s,k,c) {
  const world=s; s=planningView(world,k.id);
  initializePlans(world);
  const plans=()=>world.intrigue.plans.filter(p=>p.actor===k.id&&activePlan(p));
  if(c.war&&!plans().some(militaryPlan)&&!c.threats.length) {
    const ranked=rankObjectives(s,k.id,c.enemyTowns,c.home).map(x=>({...x,ready:c.forces.some(a=>assaultAssessment(s,k,a,x.tile).assault)}));
    const target=ranked.sort((a,b)=>Number(b.ready)-Number(a.ready)||b.score-a.score)[0]?.tile;
    if(target&&!proposeJointOperation(world,k,target.owner,target))createPlan(world,k.id,'invasion',{target:target.owner,targetTile:target.id,objective:`Capture ${target.name||target.id}.`,requiredForces:20,delay:0});
  }
  if(c.war&&!c.threats.length&&!plans().some(p=>p.type==='infrastructure')&&plans().length<3) {
    const target=c.enemyTowns[0]?.owner, tile=target&&infrastructureTarget(s,k.id,target,c.home);
    if(tile)createPlan(world,k.id,'infrastructure',{target,targetTile:tile.id,structure:tile.building,objective:`Disrupt ${kingdom(s,target).name}'s ${BUILDINGS[tile.building].name} production or defenses.`,requiredForces:16,requiredSiege:tile.building==='fort'?2:0,delay:1});
  }
  if(!plans().some(p=>!militaryPlan(p))) {
    const ally=s.kingdoms.filter(o=>o.id!==k.id&&alive(s,o.id)&&!atWar(s,k.id,o.id)&&relation(s,k.id,o.id).trust>=35&&!treaty(s,k.id,o.id,'alliance')).sort((a,b)=>cooperationInterest(s,k.id,b.id,'alliance').score-cooperationInterest(s,k.id,a.id,'alliance').score)[0];
    const partner=s.kingdoms.filter(o=>o.id!==k.id&&alive(s,o.id)&&!atWar(s,k.id,o.id)&&relation(s,k.id,o.id).opinion>=0&&!tradeBlocked(s,k.id,o.id)&&!treaty(s,k.id,o.id,'trade')).sort((a,b)=>cooperationInterest(s,k.id,b.id,'trade').score-cooperationInterest(s,k.id,a.id,'trade').score)[0];
    const rival=s.kingdoms.find(o=>o.id!==k.id&&alive(s,o.id)&&relation(s,k.id,o.id).grievance>=50&&relation(s,k.id,o.id).dependency<20&&!protectedPeace(s,k.id,o.id)&&!tradeBlocked(s,k.id,o.id));
    if(c.threats.length)createPlan(world,k.id,'defendFrontier',{targetTile:c.threats[0].tile.id,objective:`Defend ${c.threats[0].tile.name||c.threats[0].tile.id}.`,delay:0});
    else if(c.crisis){const resource=k.resources.food<40?'food':'gold';createPlan(world,k.id,'acquireResource',{resource,objective:`Rebuild ${resource} reserves.`,requiredResources:{[resource]:70},building:resource==='food'?'farm':'market'});}
    else if(ally)createPlan(world,k.id,'seekAlliance',{target:ally.id,objective:`Seek an alliance with ${ally.name}.`});
    else if(rival&&partner)createPlan(world,k.id,'embargo',{target:rival.id,allies:[partner.id],objective:`Restrict trade with ${rival.name}.`});
    else if(partner)createPlan(world,k.id,'secureTrade',{target:partner.id,objective:`Secure commerce with ${partner.name}.`});
    else createPlan(world,k.id,c.wary?'buildDefenses':c.forces.reduce((n,a)=>n+sizeOf(a),0)<30?'recruitMilitary':'expandTerritory',{
      targetTile:c.home.id,building:c.wary?'wall':'town',objective:c.wary?'Strengthen frontier defenses.':'Expand a sustainable realm.',requiredForces:36});
  }
  for(const p of plans()) {
    if(p.operationId)continue; // Shared readiness, consent and deadlines govern these plans.
    if(!alive(s,k.id)||p.target&&!alive(s,p.target)){transitionPlan(world,p,'Abandoned','A participating House lost its final settlement.');continue;}
    if(militaryPlan(p)) {
      if(protectedPeace(s,k.id,p.target)||p.wasAtWar&&!atWar(s,k.id,p.target)){transitionPlan(world,p,'Abandoned','A treaty or peace agreement prevents the attack.');continue;}
      if(c.threats.some(t=>t.tile.id===c.home.id)){transitionPlan(world,p,'Abandoned','Enemy armies threaten the capital; forces recalled to defend it.');continue;}
      if(s.tiles[p.targetTile]?.owner!==p.target && s.tiles[p.targetTile]?.fog==='visible'){transitionPlan(world,p,s.tiles[p.targetTile]?.owner===k.id?'Completed':'Abandoned','The target changed ownership.');continue;}
      if(p.structure&&s.tiles[p.targetTile]?.fog==='visible'&&!buildingLevel(s.tiles[p.targetTile],p.structure)){transitionPlan(world,p,'Completed','The target structure no longer stands.');continue;}
      if(p.assignedArmies.length&&!p.assignedArmies.some(id=>s.armies.some(a=>a.id===id&&a.owner===k.id))){transitionPlan(world,p,'Abandoned','The assigned army was destroyed.');continue;}
      if((c.crisis||totalPower(s,p.target)>Math.max(1,totalPower(s,k.id))*3)&&s.turn>p.createdTurn+2){transitionPlan(world,p,'Abandoned',c.crisis?'Resources ran out; the realm must recover.':'Enemy military strength became overwhelming.');continue;}
      if(p.status==='Considering')transitionPlan(world,p,'Preparing','Gathering supplies and troops.');
      const forces=armiesOf(s,k.id),target=s.tiles[p.targetTile];
      const assessments=forces.map(a=>({a,assessment:assaultAssessment(s,k,a,target)}));
      // Keep the saved field as an equipment preference for recruitment, never
      // as permission to attack. Re-evaluate old plans against the observed or estimated garrison.
      p.requiredSiege=fortificationDefense(target).bonus>0&&!assessments.some(x=>x.assessment.assault)?2:0;
      const ready=assessments.some(({a,assessment})=>(assessment.empty||sizeOf(a)>=p.requiredForces)&&(assessment.assault||assessment.bombard))&&canAfford(k,p.requiredResources);
      if(ready&&s.turn>=p.desiredExecutionTurn&&p.status==='Preparing')transitionPlan(world,p,'Committed','A viable assault or bombardment and supplies are ready.');
      if(p.status==='Committed'&&!atWar(s,k.id,p.target)) {
        const probe={...s,wars:[...s.wars,[k.id,p.target].sort().join(':')]};
        if(!findPath(probe,c.home.id,p.targetTile,k.id).length){transitionPlan(world,p,'Abandoned','No legal route to the objective.');continue;}
        if(declareWar(world,k.id,p.target)){p.wasAtWar=true;audit(world,p,'Declared war to execute the campaign.');}
      }
    } else {
      if(p.status==='Considering')transitionPlan(world,p,'Preparing','Council preparing this objective.');
      if(['seekAlliance','secureTrade','embargo'].includes(p.type)&&s.turn>=p.desiredExecutionTurn)negotiatePoliticalPlan(world,p);
      if(p.type==='acquireResource'&&canAfford(k,p.requiredResources))transitionPlan(world,p,'Completed','Required reserves secured.');
      if(p.type==='defendFrontier'&&!c.threats.length)transitionPlan(world,p,'Completed','No enemy force threatens the frontier.');
      if(p.type==='recruitMilitary'&&c.forces.reduce((n,a)=>n+sizeOf(a),0)>=p.requiredForces)transitionPlan(world,p,'Completed','Required military strength mustered.');
    }
    if(activePlan(p)&&s.turn-p.createdTurn>24)transitionPlan(world,p,'Abandoned','Preparation stalled for 24 turns.');
  }
}
export function plannedArmyOrder(s,k,a,c) {
  const world=s; s=planningView(world,k.id);
  // Keep the field army at its muster point while the primary nearby siege
  // objective is preparing; do not send it away on a secondary economic raid.
  const pending=world.intrigue.plans.find(p=>!p.operationId&&p.actor===k.id&&['invasion','jointWar'].includes(p.type)&&p.status==='Preparing'&&distance(c.home,s.tiles[p.targetTile])<=6&&!assaultAssessment(s,k,a,s.tiles[p.targetTile]).assault);
  if(pending&&sizeOf(a)>=pending.requiredForces) {
    if(orderArmy(world,k.id,a.id,c.home.id,'move').ok) {
      if(!pending.assignedArmies.includes(a.id)){pending.assignedArmies.push(a.id);audit(world,pending,`${a.id} mustering at ${c.home.id} for reinforcements or siege support.`);}
      return pending;
    }
  }
  const shared=memberOperation(s,k.id);
  const plans=world.intrigue.plans.filter(p=>p.actor===k.id&&militaryPlan(p)&&(!shared||p.operationId===shared.id)&&['Committed','Executing'].includes(p.status));
  plans.sort((p,q)=>distance(s.tiles[a.tile],s.tiles[p.targetTile])-distance(s.tiles[a.tile],s.tiles[q.targetTile])||(p.type==='infrastructure'?1:-1));
  for(const p of plans) {
    const t=s.tiles[p.targetTile];
    if(!atWar(s,k.id,p.target)||t.owner!==p.target)continue;
    const assessment=assaultAssessment(s,k,a,t);
    if(!assessment.empty&&sizeOf(a)<p.requiredForces||!assessment.assault&&!assessment.bombard)continue;
    const avoid=dangerousTiles(s,k,a);if(assessment.assault)avoid.delete(t.id);
    const result=assessment.bombard?orderBombardment(world,k,a,t,avoid):p.type==='infrastructure'?(t.fog==='visible'?orderStructureAttack(world,k.id,a.id,t.id,p.structure,'attack',avoid):orderArmy(world,k.id,a.id,t.id,'move',avoid)):orderArmy(world,k.id,a.id,t.id,'attack',avoid);
    if(!result.ok)continue;
    if(!p.assignedArmies.includes(a.id)){p.assignedArmies.push(a.id);audit(world,p,`${a.id} assigned to ${t.id}.`);}
    transitionPlan(world,p,'Executing',assessment.bombard?'Bombardment approach or firing orders issued to reduce assault losses.':'Real assault orders issued.');return p;
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
  for(const p of s.intrigue?.plans||[])if(!p.operationId&&activePlan(p)&&militaryPlan(p)) {
    const event=s.militaryEvents.find(e=>e.turn>=p.createdTurn&&e.attacker===p.actor&&e.tile===p.targetTile&&
      (p.type==='infrastructure'?e.action==='structure'&&e.destroyed&&e.structure===p.structure:e.action==='capture'));
    if(event)transitionPlan(s,p,'Completed','The assigned military objective was achieved.');
    else if(p.wasAtWar&&!atWar(s,p.actor,p.target))transitionPlan(s,p,'Abandoned','Peace ended the campaign.');
  }
}
// Human attack orders are real intentions too; AI spies may discover them.
export function recordPlayerPlans(s, actorHouseId = PLAYER) {
  for(const a of armiesOf(s,actorHouseId))if(['attack','bombard'].includes(a.order)&&s.tiles[a.target]?.owner&&atWar(s,actorHouseId,s.tiles[a.target].owner)&&!s.intrigue.plans.some(p=>p.operationId&&p.actor===actorHouseId&&activePlan(p)&&p.targetTile===a.target)) {
    const p=createPlan(s,actorHouseId,a.structureTarget?'infrastructure':'invasion',{target:s.tiles[a.target].owner,targetTile:a.target,structure:a.structureTarget||null,objective:`Attack ${s.tiles[a.target].name||a.target}.`,delay:0,requiredForces:1});
    if(p){p.assignedArmies=[a.id];transitionPlan(s,p,'Executing','Player attack orders recorded.');}
  }
  for(const p of s.intrigue.plans.filter(p=>!p.operationId&&p.actor===actorHouseId&&activePlan(p)))if(!p.assignedArmies.some(id=>s.armies.some(a=>a.id===id&&a.target===p.targetTile&&['attack','bombard'].includes(a.order))))transitionPlan(s,p,'Abandoned','Player changed the assigned orders.');
}
export function validatePlans(s) {
  initializePlans(s);const fail=()=>{throw new Error('Damaged strategic plans.');};
  const integer=(n,min=0,max=100020)=>Number.isInteger(n)&&n>=min&&n<=max;
  const ps=s.intrigue.plans;
  if(!Array.isArray(ps)||ps.length>120||new Set(ps.map(p=>p?.id)).size!==ps.length||!Array.isArray(s.intrigue.audit)||s.intrigue.audit.length>240)fail();
  for(const p of ps) {
    if(!p||typeof p.id!=='string'||!/^PLAN-\d+$/.test(p.id)||!kingdom(s,p.actor)||p.target&&(!kingdom(s,p.target)||p.target===p.actor)||!PLAN_TYPES.includes(p.type)||!PLAN_STATUSES.includes(p.status)||typeof p.objective!=='string'||p.objective.length>200||p.targetTile&&!s.tiles[p.targetTile]||p.structure&&!Object.hasOwn(BUILDINGS,p.structure)||p.building&&!Object.hasOwn(BUILDINGS,p.building)||p.resource&&!RESOURCES.includes(p.resource))fail();
    if(!integer(p.createdTurn,1,s.turn)||!integer(p.updatedTurn,p.createdTurn,s.turn)||!integer(p.desiredExecutionTurn,p.createdTurn)||!integer(p.requiredForces,1,100000)||!integer(p.requiredSiege,0,100000)||!p.requiredResources||typeof p.requiredResources!=='object'||Array.isArray(p.requiredResources)||Object.entries(p.requiredResources).some(([r,n])=>!RESOURCES.includes(r)||!integer(n)))fail();
    if(!Array.isArray(p.assignedArmies)||p.assignedArmies.length>500||p.assignedArmies.some(id=>typeof id!=='string'||id.length>80)||!Array.isArray(p.allies)||p.allies.length>s.kingdoms.length-1||p.allies.some(id=>!kingdom(s,id)||id===p.actor)||!Array.isArray(p.conditions)||p.conditions.length>4||p.conditions.some(x=>typeof x!=='string'||x.length>160)||p.cancellationReason!==null&&(typeof p.cancellationReason!=='string'||p.cancellationReason.length>240)||p.status==='Abandoned'&&!p.cancellationReason||typeof p.wasAtWar!=='boolean')fail();
    if(p.discoveredBy!==undefined&&(!Array.isArray(p.discoveredBy)||p.discoveredBy.length>s.kingdoms.length||p.discoveredBy.some(id=>!kingdom(s,id))))fail();
    if(militaryPlan(p)&&(!p.target||!p.targetTile)||p.type==='infrastructure'&&!p.structure)fail();
  }
  for(const a of s.intrigue.audit)if(!a||!integer(a.turn,1,s.turn)||a.planId&&!ps.some(p=>p.id===a.planId)||typeof a.message!=='string'||a.message.length>240)fail();
}
