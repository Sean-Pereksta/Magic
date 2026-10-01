import { OBJECTIVE_TYPES, offensiveObjective } from './strategic-locations.mjs';
import { emotionalEvent } from './emotions.mjs';
import { planningView } from './ai-knowledge.mjs';
import { alive, armiesOf, atWar, canAfford, canEnter, declareWar, distance, findPath, kingdom, orderArmy, pay, relation, settlements, sizeOf, treaty } from './core.mjs';
import { isAiHouse } from './house-control.mjs';
import { familyCount } from './warfare.mjs';
import { UNITS } from './data.mjs';
import { recordTrade, tradeBlocked } from './living.mjs';
import { activePlan, assaultAssessment, createPlan, dangerousTiles, transitionPlan } from './plans.mjs';
import { counterStrength } from './espionage.mjs';
import { OPERATION_ROLES, acceptedMembers, initializeCooperation, memberOperation, ongoingOperation, operationFor, operationMember, pruneCooperation } from './cooperation-state.mjs';

const fail = error => ({ok:false,error});
const integer = (x, lo, hi) => Number.isInteger(x) && x >= lo && x <= hi;
const attackRole = p => ['assault','flank','siege'].includes(p.role);
const protectedPeace = (s, a, b) => !!b && ['peace','non-aggression','alliance','vassalage'].some(type => treaty(s,a,b,type));
const pendingPledges = (s,o,house) => s.pledges.filter(p => p.operationId === o.id && (!house || p.debtor === house) && p.status === 'pending');
export const operationPledges = (s,o) => s.pledges.filter(p => p.operationId === o.id);
const currentPlan = (s,p) => s.intrigue.plans.find(x => x.id === p.planId);

export function defaultRally(s, house, targetTile, role='assault') {
  s=planningView(s,house);
  const target=s.tiles[targetTile];
  return settlements(s,house).sort((a,b) => role==='flank' ? b.r-a.r || distance(a,target)-distance(b,target) : distance(a,target)-distance(b,target))[0]?.id;
}
function memberError(s, o, p) {
  if (!p || !alive(s,p.house) || p.house===o.target || !Object.hasOwn(OPERATION_ROLES,p.role)) return 'Choose a living participant and a role.';
  if (p.house!==o.owner && (!treaty(s,o.owner,p.house,'alliance')&&!treaty(s,o.owner,p.house,'vassalage') || atWar(s,o.owner,p.house))) return 'Participating Houses must be allied to the operation leader.';
  if (offensiveObjective(o.objectiveType) && protectedPeace(s,p.house,o.target)) return 'A participant has a treaty protecting the target.';
  const rally=typeof p.rally==='string'&&Object.hasOwn(s.tiles,p.rally)?s.tiles[p.rally]:null;
  if (!rally || !canEnter(s,p.house,rally) ) return 'Choose a rally point with legal military access.';
  if (p.role==='defend' && rally.owner!==o.owner) return 'Defenders must rally in the leader’s territory.';
  if (!integer(p.requiredTroops,p.role==='supply'?0:1,500) || !integer(p.requiredSiege,0,20) || !integer(p.food,0,250) || p.role==='supply' && p.food<10) return 'Use 1–500 troops, 0–20 siege engines, and 10–250 food for a supplier.';
  if (p.house===o.owner && p.food) return 'Assign supply deliveries to a partner House.';
  const existing=memberOperation(s,p.house);
  if (existing && existing.id!==o.id) return 'This House is already committed to another operation.';
  if (s.intrigue.plans.filter(x=>x.actor===p.house&&activePlan(x)&&x.operationId!==o.id).length>=4) return 'This House must finish an existing plan first.';
  if (p.role!=='supply' && !armiesOf(s,p.house).some(a=>a.tile===p.rally||findPath(s,a.tile,p.rally,p.house).length)) return 'No army has a legal route to this rally point.';
  return null;
}
function pledge(s,o,p,task,type,targetId,amount=0) {
  // Existing ledger and consequence path; operationTask specifies observable proof.
  const creditor=p.house===o.owner?o.participants.find(x=>x.house!==o.owner&&x.status==='accepted')?.house:o.owner;
  const row={id:`pledge-${s.nextId++}`,debtor:p.house,creditor,intent:{type,targetId,giveAmount:amount,giveResource:'food',receiveAmount:0,receiveResource:'gold',duration:o.attackEnd-s.turn},operationId:o.id,operationTask:task,created:s.turn,deadline:['attack','position','defend','hold'].includes(task)?o.attackEnd+8:o.attackEnd,held:0,status:'pending',delivered:false,breached:false,eventAfter:s.nextId-1,produced:0};
  s.pledges.push(row);
}
function acceptMember(s,o,p) {
  const error=memberError(s,o,p);if(error)return fail(error);
  const plan=createPlan(s,p.house,offensiveObjective(o.objectiveType)?'jointWar':'defendFrontier',{target:o.target,targetTile:o.targetTile,objective:`${OBJECTIVE_TYPES[o.objectiveType||'attack']} Hex ${o.targetTile}; ${OPERATION_ROLES[p.role]} in ${o.name}.`,allies:o.participants.filter(x=>x.house!==p.house).map(x=>x.house),requiredForces:Math.max(1,p.requiredTroops),requiredSiege:p.requiredSiege,requiredResources:{food:24,gold:18},delay:Math.max(0,o.attackStart-s.turn),operationId:o.id});
  if(!plan)return fail('No strategic plan slot is available.');
  p.status='accepted';p.planId=plan.id;p.acceptedTurn=s.turn;
  transitionPlan(s,plan,'Preparing','A shared operation was accepted.');
  for(const other of acceptedMembers(o))if(other.house!==p.house){
    emotionalEvent(s,other.house,p.house,'private',{key:`private:${o.id}`,text:`Entrusted us with preparations for ${o.name}.`});
    emotionalEvent(s,p.house,other.house,'private',{key:`private:${o.id}`,text:`Entrusted us with preparations for ${o.name}.`});
  }
  bindCommitments(s,o,p);
  if(p.house!==o.owner)bindCommitments(s,o,operationMember(o,o.owner));
  return {ok:true};
}
function bindCommitments(s,o,p) {
  if(!acceptedMembers(o).some(x=>x.house!==p.house)||operationPledges(s,o).some(x=>x.debtor===p.house))return;
  if(p.food)pledge(s,o,p,'supply','PROMISE',o.owner,p.food);
  if(p.role!=='supply') {
    pledge(s,o,p,'rally','POSITION',p.rally);
    if(offensiveObjective(o.objectiveType)&&attackRole(p)&&o.target)pledge(s,o,p,'attack','JOINT_WAR',o.target);
    else {const duty=offensiveObjective(o.objectiveType)?(['defend','hold'].includes(p.role)?p.role:'position'):(['defend','hold'].includes(o.objectiveType)?o.objectiveType:'position');pledge(s,o,p,duty,'POSITION',!offensiveObjective(o.objectiveType)||attackRole(p)?o.targetTile:p.rally);}
  }
  if(p.requiredSiege)pledge(s,o,p,'siege','BUILD_DEFENSES',p.rally);
}
export function createOperation(s,owner,terms={}) {
  initializeCooperation(s);
  if(!terms||typeof terms!=='object')return fail('Invalid operation terms.');
  if(s.outcome || s.phase==='founding' || !alive(s,owner))return fail('Operations require an active kingdom.');
  if(typeof terms.name!=='string'||!terms.name.trim()||terms.name.length>60)return fail('Name the operation (up to 60 characters).');
  const objectiveType=terms.objectiveType??'attack';
  if(!Object.hasOwn(OBJECTIVE_TYPES,objectiveType)||typeof terms.targetTile!=='string'||!Object.hasOwn(s.tiles,terms.targetTile))return fail('Choose a valid action and map location.');
  const targetTile=planningView(s,owner).tiles[terms.targetTile],offensive=offensiveObjective(objectiveType);
  // Never infer the enemy House from the hidden authoritative tile.
  const knownOwner=targetTile.owner||targetTile.knownCapital||null;
  const targetHouse=offensive?(terms.targetHouse??knownOwner):null;
  if(terms.targetHouse!=null&&(!offensive||typeof terms.targetHouse!=='string'||!kingdom(s,terms.targetHouse)))return fail('Choose a valid target House for an offensive objective.');
  if(targetHouse&&(!alive(s,targetHouse)||targetHouse===owner||knownOwner&&knownOwner!==targetHouse))return fail('Choose a valid foreign objective.');
  if(objectiveType==='siege'&&(targetTile.fog!=='visible'||!['city','town','fort'].includes(targetTile.building)||!targetHouse))return fail('Sieges require an observed enemy city, town or fort.');
  if(!offensive&&targetTile.fog==='visible'&&knownOwner&&knownOwner!==owner&&!['alliance','access','vassalage'].some(type=>treaty(s,owner,knownOwner,type)))return fail('This position requires military access or an offensive objective.');
  if(!integer(terms.attackStart,s.turn+1,s.turn+20)||!integer(terms.attackEnd,terms.attackStart,Math.min(s.turn+24,terms.attackStart+6)))return fail('Plan an attack 1–20 turns ahead with a window of up to 6 turns.');
  if(!Array.isArray(terms.participants)||terms.participants.length<2||terms.participants.length>5||new Set(terms.participants.map(p=>p?.house)).size!==terms.participants.length||terms.participants.some(p=>!p||typeof p!=='object'))return fail('Choose two to five different participating Houses.');
  const participants=terms.participants.map(p=>({house:p?.house,role:p.role,rally:p.rally,requiredTroops:p.requiredTroops,requiredSiege:p.requiredSiege,food:p.food,status:'invited',planId:null,acceptedTurn:null}));
  if(participants.find(p=>p.house===owner)?.role!=='assault')return fail('The leader must commit to the main assault.');
  if(s.cooperation.operations.some(o=>o.owner===owner&&s.turn-o.createdTurn<6))return fail('Allow six turns between new operations.');
  const o={id:`OP-${s.nextId}`,owner,name:terms.name.trim(),target:targetHouse,targetHouse,objectiveType,targetTile:targetTile.id,createdTurn:s.turn,updatedTurn:s.turn,attackStart:terms.attackStart,attackEnd:terms.attackEnd,status:'Preparing',reason:'Gathering consent and preparations.',participants,exposure:0,exposureReasons:[],launchedTurn:null};
  for(const p of participants){const error=memberError(s,o,p);if(error)return fail(error);}
  s.nextId++;s.cooperation.operations.push(o);
  const result=acceptMember(s,o,operationMember(o,owner));
  if(!result.ok){s.cooperation.operations.pop();return result;}
  pruneCooperation(s);return {ok:true,operationId:o.id};
}
export function respondOperation(s,actor,id,decision,memberHouse=actor) {
  const o=operationFor(s,id),p=operationMember(o,memberHouse);
  if(s.outcome||!o||!ongoingOperation(o)||o.status!=='Preparing'||!p||s.turn>o.attackEnd)return fail('This invitation is no longer available.');
  if(p.status==='counter') {
    if(actor!==o.owner||!['accept','decline'].includes(decision))return fail('Only the leader can answer these counterterms.');
    if(decision==='decline'){p.status='declined';return {ok:true};}
    const adjusted={...p,requiredTroops:p.counterTroops};
    const error=memberError(s,o,adjusted);if(error)return fail(error);
    p.requiredTroops=p.counterTroops;delete p.counterTroops;
    return acceptMember(s,o,p);
  }
  if(actor!==p.house||p.status!=='invited'||!['accept','decline','counter'].includes(decision))return fail('Only the invited House can answer.');
  if(decision==='decline'){p.status='declined';return {ok:true};}
  if(decision==='counter') {
    if(p.requiredTroops<2||p.role==='supply')return fail('This role cannot offer fewer troops.');
    p.counterTroops=Math.max(1,Math.floor(p.requiredTroops/2));p.status='counter';return {ok:true};
  }
  return acceptMember(s,o,p);
}
export function leaveOperation(s,actor,id) {
  const o=operationFor(s,id),p=operationMember(o,actor);
  if(s.outcome||!o||!ongoingOperation(o)||p?.status!=='accepted')return fail('You have no active commitment here.');
  for(const pledge of pendingPledges(s,o,actor))pledge.breached=true;
  if(actor===o.owner)closeOperation(s,o,'Abandoned','The leader withdrew from the operation.');
  else {p.status='withdrawn';const plan=currentPlan(s,p);if(plan)transitionPlan(s,plan,'Abandoned','Withdrew from a shared operation.');stopOrders(s,p);}
  return {ok:true};
}
function stopOrders(s,p) {
  const plan=currentPlan(s,p);
  if(!isAiHouse(s,p.house))return;
  for(const a of armiesOf(s,p.house).filter(a=>plan?.assignedArmies.includes(a.id)))orderArmy(s,p.house,a.id,a.tile,'hold');
}
function closeOperation(s,o,status,reason) {
  if(status==='Completed'&&o.launchedTurn!==null)for(const a of acceptedMembers(o))for(const b of acceptedMembers(o))if(a.house!==b.house&&operationPledges(s,o).some(p=>p.debtor===b.house&&(p.delivered||p.status==='fulfilled')))
    emotionalEvent(s,a.house,b.house,'campaign',{key:`campaign:${o.id}`,text:`Completed the shared campaign ${o.name}.`});
  o.status=status;o.reason=reason;o.updatedTurn=s.turn;
  for(const p of o.participants) {
    const plan=currentPlan(s,p);
    if(plan&&activePlan(plan))transitionPlan(s,plan,status,reason);
    stopOrders(s,p);
  }
  // A changed objective releases unbroken tasks; abandonment never erases a
  // recorded breach. Deadlines are settled by the shared pledge verifier.
  for(const p of pendingPledges(s,o))if(!p.breached && !p.delivered && s.turn<=p.deadline)p.status='released';
}
export function operationProgress(s,o,p) {
  if(s.knowledgeView&&p.reportedProgress)return p.reportedProgress;
  const forces=armiesOf(s,p.house).filter(a=>distance(s.tiles[a.tile],s.tiles[p.rally])<=1);
  const troops=forces.reduce((n,a)=>n+sizeOf(a),0),siege=forces.reduce((n,a)=>n+familyCount(a,'siege'),0);
  const pledges=operationPledges(s,o).filter(x=>x.debtor===p.house);
  const supplyReady=!p.food||pledges.some(x=>x.operationTask==='supply'&&(x.delivered||x.status==='fulfilled'));
  const built=!p.requiredSiege||pledges.some(x=>x.operationTask==='siege'&&x.produced>=p.requiredSiege);
  return {troops,siege,supplyReady,built,ready:p.status==='accepted'&&troops>=p.requiredTroops&&siege>=p.requiredSiege&&supplyReady&&built};
}
export function operationPledgeProgress(s,p) {
  const o=operationFor(s,p.operationId),member=operationMember(o,p.debtor);
  if(!o||!member)return 'released';
  if(p.breached||!alive(s,p.debtor))return 'broken';
  const forces=armiesOf(s,p.debtor),stationed=forces.filter(a=>a.tile===member.rally).reduce((n,a)=>n+sizeOf(a),0)>=member.requiredTroops;
  let done=p.delivered;
  if(p.operationTask==='rally')done=stationed;
  if(p.operationTask==='siege')done=p.produced>=member.requiredSiege;
  if(p.operationTask==='attack')done=p.delivered||o.launchedTurn!==null&&(s.militaryEvents.some(e=>e.id>p.eventAfter&&e.turn>=o.launchedTurn&&e.attacker===p.debtor&&e.defender===o.target&&e.tile===o.targetTile)||s.tiles[o.targetTile].owner===p.debtor&&forces.some(a=>a.tile===o.targetTile));
  if(p.operationTask==='position')done=o.launchedTurn!==null&&forces.filter(a=>a.tile===p.intent.targetId).reduce((n,a)=>n+sizeOf(a),0)>=member.requiredTroops;
  if(['hold','defend'].includes(p.operationTask)) {
    if(p.lastVerified!==s.turn){p.held=o.launchedTurn!==null&&forces.filter(a=>a.tile===p.intent.targetId).reduce((n,a)=>n+sizeOf(a),0)>=member.requiredTroops&&canEnter(s,p.debtor,s.tiles[p.intent.targetId])?Math.min(2,p.held+1):0;p.lastVerified=s.turn;}
    done=p.held>=2;
  }
  return done?'fulfilled':s.turn>=p.deadline?'broken':ongoingOperation(o)?'pending':'released';
}
export function recordOperationAction(s,owner,action) {
  if(action.kind!=='recruit'||UNITS[action.unit]?.family!=='siege')return;
  for(const p of s.pledges.filter(p=>p.debtor===owner&&p.operationTask==='siege'&&p.status==='pending'))p.produced+=action.count||0;
}
export function supplyOperation(s,owner,id) {
  const o=operationFor(s,id),p=pendingPledges(s,o||{id:''},owner).find(p=>p.operationTask==='supply');
  if(s.outcome||!o||!ongoingOperation(o)||!p||p.delivered||p.breached||s.turn>p.deadline||tradeBlocked(s,owner,p.creditor)||atWar(s,owner,p.creditor))return fail('No deliverable supply commitment.');
  const cost={food:p.intent.giveAmount};if(!canAfford(kingdom(s,owner),cost))return fail('Insufficient food for this delivery.');
  pay(kingdom(s,owner),cost);pay(kingdom(s,p.creditor),cost,1);p.delivered=true;
  recordTrade(s,owner,p.creditor,'food',cost.food,'operation');return {ok:true};
}
function refreshExposure(s,o) {
  const members=acceptedMembers(o),moving=members.flatMap(p=>armiesOf(s,p.house).filter(a=>currentPlan(s,p)?.assignedArmies.includes(a.id)||a.target===p.rally||a.target===o.targetTile||distance(s.tiles[a.tile],s.tiles[p.rally])<=1)),reasons=[];
  let exposure=members.length>1?8*(members.length-1):0;
  if(exposure)reasons.push('Multiple courts coordinating');
  const marches=moving.filter(a=>a.path.length).reduce((n,a)=>n+sizeOf(a),0);
  if(marches){exposure+=Math.min(28,Math.ceil(marches/4));reasons.push('Large troop movements');}
  const siege=moving.reduce((n,a)=>n+familyCount(a,'siege'),0);
  if(siege){exposure+=Math.min(20,siege*3);reasons.push('Siege equipment mobilized');}
  const border=Object.values(s.tiles).filter(t=>t.owner===o.target);
  if(moving.some(a=>border.some(t=>distance(s.tiles[a.tile],t)<=3))){exposure+=20;reasons.push('Muster near foreign borders');}
  if(operationPledges(s,o).some(p=>p.operationTask==='siege'&&p.produced)){exposure+=10;reasons.push('Siege recruitment');}
  if(o.participants.some(p=>p.status==='invited'||p.status==='counter')){exposure+=8;reasons.push('Diplomatic couriers');}
  const protection=members.length?Math.min(...members.map(p=>counterStrength(s,p.house))):0;
  if(protection<.1){exposure+=10;reasons.push('Weak counterintelligence');}else {exposure-=Math.round(protection*75);reasons.push('Counterintelligence conceals preparations');}
  o.exposure=Math.max(0,Math.min(100,Math.round(exposure)));o.exposureReasons=reasons;
}
export function updateOperations(s,{afterMovement=false}={}) {
  initializeCooperation(s);
  for(const o of s.cooperation.operations.filter(ongoingOperation)) {
    for(const p of acceptedMembers(o))if(!alive(s,p.house)||p.house!==o.owner&&!treaty(s,o.owner,p.house,'alliance')&&!treaty(s,o.owner,p.house,'vassalage')) {
      for(const row of pendingPledges(s,o,p.house))row.breached=true;
      if(p.house===o.owner){closeOperation(s,o,'Abandoned','The leading House was defeated.');break;}
      p.status='withdrawn';const plan=currentPlan(s,p);if(plan)transitionPlan(s,plan,'Abandoned','The alliance ended or the House was defeated.');stopOrders(s,p);
    }
    if(!ongoingOperation(o))continue;
    const members=acceptedMembers(o),tile=s.tiles[o.targetTile];
    const observed=members.some(p=>planningView(s,p.house).tiles[o.targetTile]?.fog==='visible');
    if(offensiveObjective(o.objectiveType) && o.target && observed && tile.owner!==o.target) {
      // Verify the last combat before closing and releasing redundant obligations.
      for(const row of pendingPledges(s,o))if(operationPledgeProgress(s,row)==='fulfilled')row.delivered=true;
      closeOperation(s,o,members.some(p=>p.house===tile.owner)?'Completed':'Abandoned','The objective changed hands.');continue;
    }
    if(offensiveObjective(o.objectiveType)&&o.target&&members.some(p=>protectedPeace(s,p.house,o.target)||o.launchedTurn!==null&&attackRole(p)&&currentPlan(s,p)?.wasAtWar&&!atWar(s,p.house,o.target))) {closeOperation(s,o,'Abandoned','A peace agreement prevents the shared campaign.');continue;}
    if(s.turn>o.attackEnd+(o.launchedTurn===null?0:8)) {
      for(const row of pendingPledges(s,o))if(s.turn>=row.deadline&&operationPledgeProgress(s,row)!=='fulfilled')row.breached=true;
      closeOperation(s,o,'Abandoned',o.launchedTurn===null?'The attack window passed without preparations.':'The campaign stalled after the attack window.');continue;
    }
    if(o.launchedTurn!==null&&(!offensiveObjective(o.objectiveType)||!o.target)) {
      const duties=pendingPledges(s,o).filter(p=>['position','hold','defend'].includes(p.operationTask));
      for(const row of duties)if(operationPledgeProgress(s,row)==='fulfilled')row.delivered=true;
      const tasks=operationPledges(s,o).filter(p=>['position','hold','defend'].includes(p.operationTask));
      if(tasks.length&&tasks.every(p=>p.delivered||p.status==='fulfilled')){closeOperation(s,o,'Completed','Participating forces reached and secured the designated position.');continue;}
    }
    refreshExposure(s,o);
    if(afterMovement||o.status!=='Preparing')continue;
    if(s.turn>=o.attackStart&&members.length>=2&&members.every(p=>operationProgress(s,o,p).ready)) {
      const fighters=members.filter(p=>p.role!=='supply'&&(!offensiveObjective(o.objectiveType)||attackRole(p)));
      const legal=fighters.every(p=>{const view=planningView(s,p.house),probe={...view,wars:offensiveObjective(o.objectiveType)&&o.target?[...s.wars,[p.house,o.target].sort().join(':')]:s.wars};return armiesOf(s,p.house).some(a=>findPath(probe,a.tile,o.targetTile,p.house).length||a.tile===o.targetTile);});
      if(!legal){o.reason='Waiting for legal routes to the objective.';continue;}
      for(const p of fighters){if(offensiveObjective(o.objectiveType)&&o.target&&(!s.sequential||s.sequential.order[s.sequential.index]===p.house)&&!atWar(s,p.house,o.target))declareWar(s,p.house,o.target);const plan=currentPlan(s,p);plan.wasAtWar=atWar(s,p.house,o.target);transitionPlan(s,plan,'Committed','Shared action window opened; the operation is ready.');}
      for(const p of members.filter(p=>!attackRole(p)))transitionPlan(s,currentPlan(s,p),'Executing','Shared support commitments are on station.');
      o.status='Executing';o.launchedTurn=s.turn;o.updatedTurn=s.turn;o.reason='Coordinated orders released to participating armies.';
    }
  }
  pruneCooperation(s);
}
export function operationArmyOrder(s,k,a,c) {
  const o=memberOperation(s,k.id),p=operationMember(o,k.id);
  if(!o)return null;
  if(p.role==='supply'){orderArmy(s,k.id,a.id,a.tile,'hold');return currentPlan(s,p);}
  const plan=currentPlan(s,p);
  if(!plan.assignedArmies.includes(a.id))plan.assignedArmies.push(a.id);
  if(o.status==='Preparing'||offensiveObjective(o.objectiveType)&&!attackRole(p)) {
    orderArmy(s,k.id,a.id,p.rally,a.tile===p.rally?'hold':'move');return plan;
  }
  if(offensiveObjective(o.objectiveType)&&o.target&&s.sequential&&s.sequential.order[s.sequential.index]===k.id&&!atWar(s,k.id,o.target)&&!protectedPeace(s,k.id,o.target)){declareWar(s,k.id,o.target);plan.wasAtWar=true;}
  const view=planningView(s,k.id),t=view.tiles[o.targetTile];
  if(!offensiveObjective(o.objectiveType)||!o.target){
    // The coordinate is an instruction, never a grant of reconnaissance.
    const enemy=view.armies.some(e=>e.tile===t.id&&atWar(s,k.id,e.owner))||t.owner&&atWar(s,k.id,t.owner);
    const mode=a.tile===t.id?'hold':offensiveObjective(o.objectiveType)&&enemy?'attack':'move';
    const result=orderArmy(s,k.id,a.id,t.id,mode,dangerousTiles(view,k,a));
    if(!result.ok)orderArmy(s,k.id,a.id,a.tile,'hold');
    else transitionPlan(s,plan,'Executing','Orders issued to the exact strategic location.');
    return plan;
  }
  const assessment=assaultAssessment(view,k,a,t);
  if(assessment.assault&&atWar(s,k.id,o.target)) {
    const avoid=dangerousTiles(view,k,a);avoid.delete(t.id);
    if(orderArmy(s,k.id,a.id,t.id,'attack',avoid).ok){transitionPlan(s,plan,'Executing','Shared assault orders issued.');return plan;}
  }
  // The ordinary plan executor handles safe bombardment with real equipment.
  if(assessment.bombard)return null;
  orderArmy(s,k.id,a.id,p.rally,a.tile===p.rally?'hold':'move');return plan;
}
export function prepareOperationAI(s,k) {
  initializeCooperation(s);
  for(const o of s.cooperation.operations.filter(o=>o.status==='Preparing'&&o.createdTurn<s.turn)) {
    for(const p of o.participants) {
      if(p.status==='counter'&&o.owner===k.id)respondOperation(s,k.id,o.id,'accept',p.house);
      if(p.house!==k.id||p.status!=='invited')continue;
      const r=relation(s,k.id,o.owner),enemy=o.target?relation(s,k.id,o.target):{grievance:0,fear:0,dependency:0};
      const benefit=(atWar(s,k.id,o.target)?25:0)+enemy.grievance*.35+enemy.fear*.2+k.aggression*12;
      const troops=armiesOf(s,k.id).reduce((n,a)=>n+sizeOf(a),0);
      const score=r.trust*.5+r.reliability*.15-r.grievance*.5+benefit-enemy.dependency*.5;
      const uncertain=planningView(s,k.id).tiles[o.targetTile]?.fog!=='visible';
      const decision=score-(uncertain?12:0)<20||memberError(planningView(s,k.id),o,p)?'decline':p.requiredTroops>troops&&p.requiredTroops>1?'counter':'accept';
      respondOperation(s,k.id,o.id,decision);
    }
  }
  const own=memberOperation(s,k.id);
  if(own&&pendingPledges(s,own,k.id).some(p=>p.operationTask==='supply'&&!p.delivered)&&kingdom(s,k.id).resources.food>=operationMember(own,k.id).food+30)supplyOperation(s,k.id,own.id);
}
export function proposeJointOperation(s,k,target,tile) {
  const world=s; s=planningView(world,k.id);
  const partners=s.kingdoms.filter(h=>h.id!==k.id&&h.id!==target&&alive(s,h.id)&&treaty(s,k.id,h.id,'alliance')&&!protectedPeace(s,h.id,target)&&relation(s,k.id,h.id).trust>=20)
    .sort((a,b)=>Number(atWar(s,b.id,target))-Number(atWar(s,a.id,target))||a.id.localeCompare(b.id)).slice(0,2);
  if(!partners.length)return null;
  const participants=[{house:k.id,role:'assault',requiredTroops:30,requiredSiege:0,food:0},...partners.map((h,i)=>({house:h.id,role:i===1?'supply':'flank',requiredTroops:i===1?0:20,requiredSiege:0,food:i===1?30:0}))];
  for(const p of participants)p.rally=defaultRally(s,p.house,tile.id,p.role);
  if(participants.some(p=>!s.tiles[p.rally]))return null;
  const travel=Math.max(...participants.map(p=>distance(s.tiles[p.rally],tile)));
  const attackStart=s.turn+Math.min(12,Math.max(3,Math.ceil(travel/3)));
  const result=createOperation(world,k.id,{name:`Operation ${tile.name||'Iron Gate'}`.slice(0,60),targetTile:tile.id,attackStart,attackEnd:attackStart+3,participants});
  return result.ok?operationFor(world,result.operationId):null;
}
export function publicMobilizations(s,viewer) {
  s=planningView(s,viewer);
  const border=Object.values(s.tiles).filter(t=>t.owner===viewer);
  return s.kingdoms.filter(k=>k.id!==viewer).flatMap(k=>{
    const forces=armiesOf(s,k.id).filter(a=>!a.remembered&&(s.knowledgeView||a.path.length)&&border.some(t=>distance(s.tiles[a.tile],t)<=4));
    const count=forces.reduce((n,a)=>n+sizeOf(a),0),siege=forces.reduce((n,a)=>n+familyCount(a,'siege'),0);
    return count>=40||siege>=2?[{house:k.id,text:`${k.name}: ${count} troops${siege?` including ${siege} siege engines`:''} observed near your frontier. Their objective is unknown.`}]:[];
  });
}
