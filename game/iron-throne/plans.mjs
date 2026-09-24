import { BUILDINGS, RESOURCES } from './data.mjs';
import { buildingLevel, fortMaximum, tileProduction } from './economy.mjs';
import { familyCount } from './warfare.mjs';
import { PLAYER, alive, armiesOf, atWar, canAfford, declareWar, distance, findPath, kingdom, log, orderArmy, orderStructureAttack, relation, settlements, sizeOf, strength, treaty } from './core.mjs';
import { changeRelation, tradeBlocked } from './living.mjs';

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
export function createPlan(s,actor,type,{target=null,targetTile=null,structure=null,resource=null,objective='',allies=[],requiredForces=20,requiredSiege=0,requiredResources={food:24,gold:18},delay=2,building=null}={}) {
  initializePlans(s);
  if(!PLAN_TYPES.includes(type)||!kingdom(s,actor)||target&&(!kingdom(s,target)||target===actor)||targetTile&&!s.tiles[targetTile])return null;
  const existing=s.intrigue.plans.find(p=>p.actor===actor&&p.type===type&&p.target===target&&p.targetTile===targetTile&&activePlan(p));
  if(existing)return existing;
  if(s.intrigue.plans.filter(p=>p.actor===actor&&activePlan(p)).length>=4)return null;
  const p={id:`PLAN-${s.nextId++}`,actor,target,type,objective:objective.slice(0,200),targetTile,structure,resource,building,
    createdTurn:s.turn,desiredExecutionTurn:s.turn+delay,updatedTurn:s.turn,status:'Considering',requiredForces,requiredSiege,
    requiredResources,assignedArmies:[],allies:[...allies],discoveredBy:[],conditions:['Honor active treaties','Defend the capital first'],cancellationReason:null,wasAtWar:target?atWar(s,actor,target):false};
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
export function proposeInvasion(s,k,target,tile) {
  const allies=s.kingdoms.filter(o=>o.id!==k.id&&treaty(s,k.id,o.id,'alliance')&&atWar(s,o.id,target)).map(o=>o.id);
  return createPlan(s,k.id,allies.length?'jointWar':'invasion',{target,targetTile:tile.id,allies,objective:`Capture ${tile.name||tile.id}.`,requiredForces:30,
    requiredSiege:tile.walls>0||fortMaximum(tile)?2:0,delay:3});
}
export function infrastructureTarget(s,actor,target,origin) {
  const mounted=armiesOf(s,target).some(a=>familyCount(a,'mounted')>sizeOf(a)*.3);
  return Object.values(s.tiles).filter(t=>t.owner===target&&t.building&&!['city','town'].includes(t.building)).map(t=>{
    const production=tileProduction(t,target),value=Object.values(production).reduce((n,v)=>n+v,0);
    return {t,score:value+(t.building==='fort'?25:0)+(mounted&&t.building==='ranch'?25:0)-distance(origin,t)*2};
  }).sort((a,b)=>b.score-a.score||a.t.id.localeCompare(b.t.id))[0]?.t;
}
export function preparePlans(s,k,c) {
  initializePlans(s);
  const plans=()=>s.intrigue.plans.filter(p=>p.actor===k.id&&activePlan(p));
  if(c.war&&!plans().some(militaryPlan)&&!c.threats.length) {
    const engines=c.forces.reduce((n,a)=>n+familyCount(a,'siege'),0);
    const target=[...c.enemyTowns].sort((a,b)=>Number((a.walls>0||fortMaximum(a)>0)&&engines<2)-Number((b.walls>0||fortMaximum(b)>0)&&engines<2)||distance(c.home,a)-distance(c.home,b))[0];
    if(target)createPlan(s,k.id,'invasion',{target:target.owner,targetTile:target.id,objective:`Capture ${target.name||target.id}.`,requiredForces:20,requiredSiege:target.walls>0||fortMaximum(target)?2:0,delay:0});
  }
  if(c.war&&!c.threats.length&&!plans().some(p=>p.type==='infrastructure')&&plans().length<3) {
    const target=c.enemyTowns[0]?.owner, tile=target&&infrastructureTarget(s,k.id,target,c.home);
    if(tile)createPlan(s,k.id,'infrastructure',{target,targetTile:tile.id,structure:tile.building,objective:`Disrupt ${kingdom(s,target).name}'s ${BUILDINGS[tile.building].name} production or defenses.`,requiredForces:16,requiredSiege:tile.building==='fort'?2:0,delay:1});
  }
  if(!plans().some(p=>!militaryPlan(p))) {
    const ally=s.kingdoms.find(o=>![PLAYER,k.id].includes(o.id)&&alive(s,o.id)&&!atWar(s,k.id,o.id)&&relation(s,k.id,o.id).trust>=35&&relation(s,o.id,k.id).trust>=25&&!treaty(s,k.id,o.id,'alliance'));
    const partner=s.kingdoms.find(o=>![PLAYER,k.id].includes(o.id)&&alive(s,o.id)&&!atWar(s,k.id,o.id)&&relation(s,k.id,o.id).opinion>=0&&!tradeBlocked(s,k.id,o.id)&&!treaty(s,k.id,o.id,'trade'));
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
      if(p.assignedArmies.length&&!p.assignedArmies.some(id=>s.armies.some(a=>a.id===id&&a.owner===k.id))){transitionPlan(s,p,'Abandoned','The assigned army was destroyed.');continue;}
      if((c.crisis||totalPower(s,p.target)>Math.max(1,totalPower(s,k.id))*3)&&s.turn>p.createdTurn+2){transitionPlan(s,p,'Abandoned',c.crisis?'Resources ran out; the realm must recover.':'Enemy military strength became overwhelming.');continue;}
      if(p.status==='Considering')transitionPlan(s,p,'Preparing','Gathering supplies and troops.');
      const forces=armiesOf(s,k.id),ready=forces.some(a=>sizeOf(a)>=p.requiredForces&&familyCount(a,'siege')>=p.requiredSiege)&&canAfford(k,p.requiredResources);
      if(ready&&s.turn>=p.desiredExecutionTurn&&p.status==='Preparing')transitionPlan(s,p,'Committed','Required troops, siege support and supplies are ready.');
      if(p.status==='Committed'&&!atWar(s,k.id,p.target)) {
        const probe={...s,wars:[...s.wars,[k.id,p.target].sort().join(':')]};
        if(!findPath(probe,c.home.id,p.targetTile,k.id).length){transitionPlan(s,p,'Abandoned','No legal route to the objective.');continue;}
        if(declareWar(s,k.id,p.target)){p.wasAtWar=true;audit(s,p,'Declared war to execute the campaign.');}
      }
    } else {
      if(p.status==='Considering')transitionPlan(s,p,'Preparing','Council preparing this objective.');
      if(['seekAlliance','secureTrade','embargo'].includes(p.type)&&s.turn>=p.desiredExecutionTurn)executePoliticalPlan(s,p);
      if(p.type==='acquireResource'&&canAfford(k,p.requiredResources))transitionPlan(s,p,'Completed','Required reserves secured.');
      if(p.type==='defendFrontier'&&!c.threats.length)transitionPlan(s,p,'Completed','No enemy force threatens the frontier.');
      if(p.type==='recruitMilitary'&&c.forces.reduce((n,a)=>n+sizeOf(a),0)>=p.requiredForces)transitionPlan(s,p,'Completed','Required military strength mustered.');
    }
    if(activePlan(p)&&s.turn-p.createdTurn>24)transitionPlan(s,p,'Abandoned','Preparation stalled for 24 turns.');
  }
}
function executePoliticalPlan(s,p) {
  const k=kingdom(s,p.actor),r=relation(s,p.actor,p.target);
  if(p.target===PLAYER){transitionPlan(s,p,'Abandoned','Player agreements require ratification in council.');return;}
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
  const pending=s.intrigue.plans.find(p=>p.actor===k.id&&['invasion','jointWar'].includes(p.type)&&p.status==='Preparing'&&p.requiredSiege>familyCount(a,'siege')&&distance(c.home,s.tiles[p.targetTile])<=6);
  if(pending&&sizeOf(a)>=pending.requiredForces) {
    if(orderArmy(s,k.id,a.id,c.home.id,'move').ok) {
      if(!pending.assignedArmies.includes(a.id)){pending.assignedArmies.push(a.id);audit(s,pending,`${a.id} mustering at ${c.home.id} for siege equipment.`);}
      return pending;
    }
  }
  const plans=s.intrigue.plans.filter(p=>p.actor===k.id&&militaryPlan(p)&&['Committed','Executing'].includes(p.status));
  plans.sort((p,q)=>distance(s.tiles[a.tile],s.tiles[p.targetTile])-distance(s.tiles[a.tile],s.tiles[q.targetTile])||(p.type==='infrastructure'?1:-1));
  for(const p of plans) {
    const t=s.tiles[p.targetTile];
    if(!atWar(s,k.id,p.target)||t.owner!==p.target||sizeOf(a)<p.requiredForces||familyCount(a,'siege')<p.requiredSiege)continue;
    const defense=s.armies.filter(e=>e.tile===t.id&&atWar(s,k.id,e.owner)).reduce((n,e)=>n+strength(e,true,t),0)+(t.building==='city'?14:8);
    if(strength(a)<defense*(1.35-k.aggression*.3))continue;
    const avoid=new Set(Object.values(s.tiles).filter(tile=>tile.id!==t.id&&atWar(s,k.id,tile.owner)&&(tile.walls>0||(tile.fortIntegrity??fortMaximum(tile))>0)&&familyCount(a,'siege')<2).map(t=>t.id));
    for(const e of c.enemies)if(e.tile!==t.id&&strength(e,true,s.tiles[e.tile])>strength(a))avoid.add(e.tile);
    const result=p.type==='infrastructure'?orderStructureAttack(s,k.id,a.id,t.id,p.structure,'attack',avoid):orderArmy(s,k.id,a.id,t.id,'attack',avoid);
    if(!result.ok)continue;
    if(!p.assignedArmies.includes(a.id)){p.assignedArmies.push(a.id);audit(s,p,`${a.id} assigned to ${t.id}.`);}
    transitionPlan(s,p,'Executing','Real attack orders issued.');return p;
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
    const event=s.militaryEvents.find(e=>e.turn>=p.createdTurn&&e.attacker===p.actor&&e.tile===p.targetTile&&
      (p.type==='infrastructure'?e.action==='structure'&&e.destroyed&&e.structure===p.structure:e.action==='capture'));
    if(event)transitionPlan(s,p,'Completed','The assigned military objective was achieved.');
    else if(p.wasAtWar&&!atWar(s,p.actor,p.target))transitionPlan(s,p,'Abandoned','Peace ended the campaign.');
  }
}
// Human attack orders are real intentions too; AI spies may discover them.
export function recordPlayerPlans(s) {
  for(const a of armiesOf(s,PLAYER))if(['attack','bombard'].includes(a.order)&&s.tiles[a.target]?.owner&&atWar(s,PLAYER,s.tiles[a.target].owner)) {
    const p=createPlan(s,PLAYER,a.structureTarget?'infrastructure':'invasion',{target:s.tiles[a.target].owner,targetTile:a.target,structure:a.structureTarget||null,objective:`Attack ${s.tiles[a.target].name||a.target}.`,delay:0,requiredForces:1});
    if(p){p.assignedArmies=[a.id];transitionPlan(s,p,'Executing','Player attack orders recorded.');}
  }
  for(const p of s.intrigue.plans.filter(p=>p.actor===PLAYER&&activePlan(p)))if(!p.assignedArmies.some(id=>s.armies.some(a=>a.id===id&&a.target===p.targetTile&&['attack','bombard'].includes(a.order))))transitionPlan(s,p,'Abandoned','Player changed the assigned orders.');
}
export function validatePlans(s) {
  initializePlans(s);const fail=()=>{throw new Error('Damaged strategic plans.');};
  const integer=(n,min=0,max=100020)=>Number.isInteger(n)&&n>=min&&n<=max;
  const ps=s.intrigue.plans;
  if(!Array.isArray(ps)||ps.length>120||new Set(ps.map(p=>p?.id)).size!==ps.length||!Array.isArray(s.intrigue.audit)||s.intrigue.audit.length>240)fail();
  for(const p of ps) {
    if(!p||typeof p.id!=='string'||!/^PLAN-\d+$/.test(p.id)||!kingdom(s,p.actor)||p.target&&(!kingdom(s,p.target)||p.target===p.actor)||!PLAN_TYPES.includes(p.type)||!PLAN_STATUSES.includes(p.status)||typeof p.objective!=='string'||p.objective.length>200||p.targetTile&&!s.tiles[p.targetTile]||p.structure&&!Object.hasOwn(BUILDINGS,p.structure)||p.building&&!Object.hasOwn(BUILDINGS,p.building)||p.resource&&!RESOURCES.includes(p.resource))fail();
    if(!integer(p.createdTurn,1,s.turn)||!integer(p.updatedTurn,p.createdTurn,s.turn)||!integer(p.desiredExecutionTurn,p.createdTurn)||!integer(p.requiredForces,1,100000)||!integer(p.requiredSiege,0,100000)||!p.requiredResources||typeof p.requiredResources!=='object'||Array.isArray(p.requiredResources)||Object.entries(p.requiredResources).some(([r,n])=>!RESOURCES.includes(r)||!integer(n)))fail();
    if(!Array.isArray(p.assignedArmies)||p.assignedArmies.length>500||p.assignedArmies.some(id=>typeof id!=='string'||id.length>80)||!Array.isArray(p.allies)||p.allies.length>5||p.allies.some(id=>!kingdom(s,id)||id===p.actor)||!Array.isArray(p.conditions)||p.conditions.length>4||p.conditions.some(x=>typeof x!=='string'||x.length>160)||p.cancellationReason!==null&&(typeof p.cancellationReason!=='string'||p.cancellationReason.length>240)||p.status==='Abandoned'&&!p.cancellationReason||typeof p.wasAtWar!=='boolean')fail();
    if(p.discoveredBy!==undefined&&(!Array.isArray(p.discoveredBy)||p.discoveredBy.length>6||p.discoveredBy.some(id=>!kingdom(s,id))))fail();
    if(militaryPlan(p)&&(!p.target||!p.targetTile)||p.type==='infrastructure'&&!p.structure)fail();
  }
  for(const a of s.intrigue.audit)if(!a||!integer(a.turn,1,s.turn)||a.planId&&!ps.some(p=>p.id===a.planId)||typeof a.message!=='string'||a.message.length>240)fail();
}
