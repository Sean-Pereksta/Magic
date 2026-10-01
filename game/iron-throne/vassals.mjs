import { COMMAND_KINDS, COMMAND_STATUSES } from './command-state.mjs';
import { initializeCooperation } from './cooperation-state.mjs';
import { createPlan, activePlan, assaultAssessment, dangerousTiles, transitionPlan, orderBombardment } from './plans.mjs';
import { planningView } from './ai-knowledge.mjs';
import { isAiHouse } from './house-control.mjs';
import { appendConversation } from './living.mjs';
import { armiesOf, atWar, declareWar, distance, kingdom, orderArmy, relation, settlements, sizeOf, strength, treaty } from './core.mjs';

const fail=error=>({ok:false,error});
const standingCommand=o=>['defend','hold','frontier'].includes(o.kind)||o.kind==='reinforce'&&!!o.army;
import { vassalBond } from './vassal-role.mjs';
export { vassalBond } from './vassal-role.mjs';
export const vassalOrder=(s,vassal)=>s.cooperation?.vassalOrders?.find(o=>o.vassal===vassal&&o.status!=='Completed');
export function initializeVassals(s){initializeCooperation(s);s.cooperation.vassalOrders??=[];s.fealty??={};}
function notice(s,o,status,reason){
  if(o.status===status&&o.reason===reason)return;
  o.status=status;o.reason=reason;o.updated=s.turn;
  if(status==='Completed'){
    const plan=s.intrigue.plans.find(p=>p.id===o.planId);
    if(plan&&activePlan(plan))transitionPlan(s,plan,'Completed',reason);
  }
  appendConversation(s,o.vassal,'ruler',`${status}: ${reason}`,{actorHouseId:o.liege,unread:true,kind:'vassal-command'});
}
function reachedObjective(s,o){
  notice(s,o,standingCommand(o)?'Holding':'Completed',standingCommand(o)?'Forces hold the designated position. This standing command remains in effect until replaced or released.':'Forces reached and hold the designated position.');
}
export function issueVassalCommand(s,liege,vassal,raw){
  initializeVassals(s);
  if(s.outcome||!vassalBond(s,liege,vassal))return fail('Only your current vassal can receive this command.');
  if(!raw||!COMMAND_KINDS.includes(raw.kind)||typeof raw.target!=='string')return fail('Choose a supported objective and known location.');
  const view=planningView(s,liege),support=raw.army&&view.armies.find(a=>a.id===raw.army&&!a.remembered),tile=view.tiles[support?.tile||raw.target];
  if(raw.army&&(raw.kind!=='reinforce'||!support||support.owner!==liege&&!['alliance','vassalage'].some(type=>treaty(s,liege,support.owner,type))))return fail('Choose an observed friendly army to reinforce.');
  if(!tile||tile.fog==='unknown')return fail('Observe the objective before issuing a command.');
  if(['attack','siege'].includes(raw.kind)&&(!tile.owner||!atWar(s,liege,tile.owner)||!atWar(s,vassal,tile.owner)))return fail('Both Houses must already be at war with this target. Arrange war separately.');
  if(raw.kind==='siege'&&!['city','town','fort'].includes(tile.building))return fail('A siege requires an observed city, town or fort.');
  const old=vassalOrder(s,vassal),oldPlan=old&&s.intrigue.plans.find(p=>p.id===old.planId);
  // Replace the old objective only after the new plan is legal and has capacity.
  if(s.intrigue.plans.filter(p=>p.actor===vassal&&activePlan(p)&&p!==oldPlan).length>=4)return fail('The vassal must finish an existing strategic commitment first.');
  if(old){notice(s,old,'Completed','Replaced by a new command from the liege.');if(oldPlan)transitionPlan(s,oldPlan,'Abandoned','The liege replaced this objective.');}
  const attack=['attack','siege'].includes(raw.kind);
  const p=createPlan(s,vassal,attack?'jointWar':'defendFrontier',{target:attack?tile.owner:null,targetTile:tile.id,objective:`Liege command: ${raw.kind} ${tile.name||tile.id}`,requiredForces:24,requiredSiege:raw.kind==='siege'?2:0,delay:0});
  if(!p)return fail('No strategic plan slot is available.');
  p.vassalCommand=true;transitionPlan(s,p,'Preparing','A persistent command from the liege takes priority.');
  const o={id:`VASSAL-${s.nextId++}`,liege,vassal,kind:raw.kind,target:tile.id,planId:p.id,status:'Preparing',reason:'',created:s.turn,updated:s.turn,accepted:isAiHouse(s,vassal)};
  if(raw.army)o.army=raw.army;
  s.cooperation.vassalOrders.push(o);s.cooperation.vassalOrders=s.cooperation.vassalOrders.slice(-36);
  notice(s,o,'Preparing',o.accepted?'I acknowledge the command. I will prepare real orders during my activation.':'A human ruler controls this House. Awaiting their explicit acceptance; their armies remain under their control.');
  return {ok:true,id:o.id};
}
export function acceptVassalRequest(s,actor,id){
  const o=s.cooperation?.vassalOrders?.find(o=>o.id===id&&o.vassal===actor&&o.status!=='Completed');
  if(!o||!vassalBond(s,o.liege,actor))return fail('This request is no longer active.');
  o.accepted=true;notice(s,o,'Preparing','The human vassal accepted this obligation and will issue their own army orders.');return {ok:true};
}
export function vassalArmyOrder(s,k,a,c){
  if(s.sequential&&s.sequential.order[s.sequential.index]!==k.id)return false;
  const o=vassalOrder(s,k.id);if(!o?.accepted||!vassalBond(s,o.liege,k.id)||!isAiHouse(s,k.id))return false;
  const view=planningView(s,k.id),plan=s.intrigue.plans.find(p=>p.id===o.planId);
  if(o.army){
    const support=view.armies.find(a=>a.id===o.army&&!a.remembered);
    if(!support||support.owner!==k.id&&support.owner!==o.liege&&!['alliance','vassalage'].some(type=>treaty(s,k.id,support.owner,type))){notice(s,o,'Blocked','The designated friendly army is no longer observed. Share a new rally location or restore contact.');orderArmy(s,k.id,a.id,a.tile,'hold');return true;}
    o.target=support.tile;if(plan)plan.targetTile=o.target;
  }
  const t=view.tiles[o.target];
  const homeThreat=c.threats.find(x=>x.tile.id===c.home.id);
  const recovering=a.regroupTarget||a.morale<.55||c.desperation?.score>=55;
  if(homeThreat||recovering){
    const refuge=recovering?c.towns.filter(t=>!view.armies.some(e=>atWar(view,k.id,e.owner)&&distance(t,view.tiles[e.tile])<=1)).sort((x,y)=>distance(view.tiles[a.tile],x)-distance(view.tiles[a.tile],y))[0]:c.home;
    const result=orderArmy(s,k.id,a.id,refuge?.id||a.tile,recovering?'retreat':'move');
    if(!result.ok)orderArmy(s,k.id,a.id,a.tile,'hold');
    notice(s,o,'Blocked',!result.ok?`Emergency regrouping is blocked: ${result.error}`:recovering?'Battle losses require recovery and reinforcements. Our retreat orders retain the original objective.':'Enemy forces threaten our capital. Real defensive orders take priority until the danger passes.');return true;
  }
  if(!plan){orderArmy(s,k.id,a.id,a.tile,'hold');notice(s,o,'Blocked','The linked campaign plan is unavailable. Please renew the command.');return true;}
  if(!plan.assignedArmies.includes(a.id))plan.assignedArmies.push(a.id);
  const attack=['attack','siege'].includes(o.kind);
  if(attack&&t.owner===k.id){notice(s,o,'Completed','The designated position is captured. Holding the position.');transitionPlan(s,plan,'Completed','The command was fulfilled by occupation.');orderArmy(s,k.id,a.id,t.id);return true;}
  if(attack&&(!t.owner||!atWar(s,k.id,t.owner))){notice(s,o,'Blocked','A treaty or changed ownership prevents this attack. Please designate another objective.');orderArmy(s,k.id,a.id,a.tile,'hold');return true;}
  if(attack){
    const assessment=assaultAssessment(view,k,a,t);
    if(assessment.bombard&&orderBombardment(s,k,a,t,dangerousTiles(view,k,a)).ok){notice(s,o,'Engaged','Siege orders issued against the known fortifications.');return true;}
    if(!assessment.assault){
      plan.requiredForces=Math.min(300,Math.max(plan.requiredForces,Math.ceil(sizeOf(a)*1.35)));plan.requiredSiege=assessment.bombard?2:plan.requiredSiege;
      const rally=c.towns.slice().sort((x,y)=>distance(x,t)-distance(y,t))[0];
      const result=orderArmy(s,k.id,a.id,rally.id,'move');
      if(!result.ok){orderArmy(s,k.id,a.id,a.tile,'hold');notice(s,o,'Blocked',`Reinforcements are needed, but the rally route is blocked: ${result.error}`);}
      else notice(s,o,'Preparing',`Known defenses at ${t.name||t.id} require reinforcements${plan.requiredSiege?' and siege support':''}; gathering at ${rally.name||rally.id}.`);
      return true;
    }
  }
  const result=orderArmy(s,k.id,a.id,t.id,attack?'attack':o.kind==='withdraw'?'retreat':'move',dangerousTiles(view,k,a));
  if(!result.ok){orderArmy(s,k.id,a.id,a.tile,'hold');notice(s,o,'Blocked',`${result.error} Offer access or choose a reachable rally point.`);}
  else if(a.tile===t.id)reachedObjective(s,o);
  else notice(s,o,'Marching',`Real army orders issued toward ${t.name||t.id}; movement awaits our turn resolution.`);
  return true;
}
export function updateVassalProgress(s){
  initializeVassals(s);
  for(const o of s.cooperation.vassalOrders.filter(o=>o.status!=='Completed')){
    if(!vassalBond(s,o.liege,o.vassal)){notice(s,o,'Completed','The vassalage ended; this command was released.');continue;}
    const plan=s.intrigue.plans.find(p=>p.id===o.planId),armies=armiesOf(s,o.vassal).filter(a=>plan?.assignedArmies.includes(a.id)||!isAiHouse(s,o.vassal)&&o.accepted&&a.tile===o.target);
    if(armies.some(a=>a.tile===o.target)&&(!['attack','siege'].includes(o.kind)||s.tiles[o.target].owner===o.vassal))reachedObjective(s,o);
    else if(o.status==='Holding')notice(s,o,'Blocked','Forces no longer hold the designated position. The standing objective remains in effect.');
    else if(s.militaryEvents.some(e=>e.turn===s.turn&&e.attacker===o.vassal&&e.tile===o.target))notice(s,o,'Engaged','Our army fought at the objective. The position remains contested.');
  }
}
export function updateFealty(s,{allowRebellion=true}={}){
  initializeVassals(s);
  for(const bond of s.treaties.filter(t=>t.type==='vassalage'&&t.expires>s.turn&&t.liege&&t.vassal)){
    const {liege,vassal}=bond,k=kingdom(s,vassal),r=relation(s,vassal,liege);
    const f=s.fealty[vassal]??={liege,vassal,status:'Loyal',warningTurn:null,reason:'The sworn relationship is being honored.'};
    if(f.liege!==liege)Object.assign(f,{liege,status:'Loyal',warningTurn:null});
    const serious=(r.history||[]).filter(e=>s.turn-e.turn<=16&&(e.changes?.trust<=-12||e.changes?.grievance>=15)&&/execut|broken|broke|abandon|failed|breach|attack|betray/i.test(e.reason));
    const sustained=new Set(serious.map(e=>e.turn)).size>=2;
    const protection=s.pledges.some(p=>p.debtor===liege&&p.creditor===vassal&&p.status==='broken'&&/DEFEND|GUARANTEE/.test(p.intent.type));
    const view=planningView(s,vassal),ours=armiesOf(view,vassal).reduce((n,a)=>n+strength(a),0),theirs=armiesOf(view,liege).reduce((n,a)=>n+strength(a)*(a.confidence??1),0);
    const informed=armiesOf(view,liege).length>0,opportunity=informed&&ours>=40&&ours>theirs*1.25;
    const qualifies=(sustained||protection&&serious.length>0)&&r.grievance>=65&&r.trust<=-20&&(k.ambition>=.65||k.honor<.55)&&opportunity;
    if(qualifies){
      if(f.warningTurn===null){f.warningTurn=s.turn;f.status='Rebellion Warning';f.reason=`Unresolved grave mistreatment: ${serious.at(-1)?.reason||'a broken protection obligation'}. Repair trust and grievances within three rounds.`;appendConversation(s,vassal,'ruler',f.reason,{actorHouseId:liege,unread:true,kind:'fealty'});}
      else if(allowRebellion&&s.turn>=f.warningTurn+3&&isAiHouse(s,vassal)&&(!s.sequential||s.sequential.order[s.sequential.index]===vassal)){f.status='Rebel';f.reason='The warned grievances remained unresolved and a credible escape became possible.';declareWar(s,vassal,liege);}
    }else{
      f.status=serious.length&&r.grievance>=35?'Strained':'Loyal';f.warningTurn=null;f.reason=f.status==='Loyal'?'The sworn relationship is being honored.':`A recorded grievance remains: ${serious.at(-1).reason}`;
    }
  }
  for(const f of Object.values(s.fealty))if(f.status!=='Rebel'&&!vassalBond(s,f.liege,f.vassal)){f.status='Released';f.warningTurn=null;f.reason='Vassalage expired or ended peacefully; this is not a rebellion.';}
}
export function validateVassals(s){
  initializeVassals(s);const fail=()=>{throw new Error('Damaged vassal command data.');};
  if(s.cooperation.vassalOrders.length>36||Object.keys(s.fealty).length>s.kingdoms.length)fail();
  for(const o of s.cooperation.vassalOrders)if(!o||!/^VASSAL-\d+$/.test(o.id)||!kingdom(s,o.liege)||!kingdom(s,o.vassal)||o.liege===o.vassal||!COMMAND_KINDS.includes(o.kind)||!COMMAND_STATUSES.includes(o.status)||!s.tiles[o.target]||!/^PLAN-\d+$/.test(o.planId)||o.status!=='Completed'&&!s.intrigue.plans.some(p=>p.id===o.planId&&p.actor===o.vassal)||typeof o.accepted!=='boolean'||typeof o.reason!=='string'||o.reason.length>500||!Number.isInteger(o.created)||o.created<1||o.updated<o.created||o.updated>s.turn)fail();
  for(const o of s.cooperation.vassalOrders)if(o.army!==undefined&&(o.kind!=='reinforce'||typeof o.army!=='string'||!/^army-\d+$/.test(o.army)))fail();
  for(const p of s.intrigue.plans)if(p.vassalCommand&&activePlan(p)&&!s.cooperation.vassalOrders.some(o=>o.planId===p.id&&o.vassal===p.actor&&o.status!=='Completed'))fail();
  for(const [id,f] of Object.entries(s.fealty))if(!f||f.vassal!==id||!kingdom(s,id)||!kingdom(s,f.liege)||!['Loyal','Strained','Rebellion Warning','Rebel','Released'].includes(f.status)||f.warningTurn!==null&&(!Number.isInteger(f.warningTurn)||f.warningTurn<1||f.warningTurn>s.turn)||typeof f.reason!=='string'||f.reason.length>500)fail();
}
