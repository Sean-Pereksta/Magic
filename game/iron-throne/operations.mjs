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
const protectedPeace = (s, a, b) => ['peace','non-aggression','alliance','vassalage'].some(type => treaty(s,a,b,type));
const pendingPledges = (s,o,house) => s.pledges.filter(p => p.operationId === o.id && (!house || p.debtor === house) && p.status === 'pending');
export const operationPledges = (s,o) => s.pledges.filter(p => p.operationId === o.id);
const currentPlan = (s,p) => s.intrigue.plans.find(x => x.id === p.planId);

export function defaultRally(s, house, targetTile, role='assault') {
  const target=s.tiles[targetTile];
  return settlements(s,house).sort((a,b) => role==='flank' ? b.r-a.r || distance(a,target)-distance(b,target) : distance(a,target)-distance(b,target))[0]?.id;
}
function memberError(s, o, p) {
  if (!p || !alive(s,p.house) || p.house===o.target || !Object.hasOwn(OPERATION_ROLES,p.role)) return 'Choose a living participant and a role.';
  if (p.house!==o.owner && (!treaty(s,o.owner,p.house,'alliance') || atWar(s,o.owner,p.house))) return 'Participating Houses must be allied to the operation leader.';
  if (protectedPeace(s,p.house,o.target)) return 'A participant has a treaty protecting the target.';
  const rally=s.tiles[p.rally];
  if (!rally || !canEnter(s,p.house,rally) || ![p.house,o.owner].includes(rally.owner)) return 'Choose a reachable rally point in your or the leader’s territory.';
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
  const row={id:`pledge-${s.nextId++}`,debtor:p.house,creditor,intent:{type,targetId,giveAmount:amount,giveResource:'food',receiveAmount:0,receiveResource:'gold',duration:o.attackEnd-s.turn},operationId:o.id,operationTask:task,created:s.turn,deadline:task==='attack'?o.attackEnd+8:o.attackEnd,held:0,status:'pending',delivered:false,breached:false,eventAfter:s.nextId-1,produced:0};
  s.pledges.push(row);
}
function acceptMember(s,o,p) {
  const error=memberError(s,o,p);if(error)return fail(error);
  const plan=createPlan(s,p.house,'jointWar',{target:o.target,targetTile:o.targetTile,objective:`${OPERATION_ROLES[p.role]} in ${o.name}.`,allies:o.participants.filter(x=>x.house!==p.house).map(x=>x.house),requiredForces:Math.max(1,p.requiredTroops),requiredSiege:p.requiredSiege,requiredResources:{food:24,gold:18},delay:Math.max(0,o.attackStart-s.turn),operationId:o.id});
  if(!plan)return fail('No strategic plan slot is available.');
  p.status='accepted';p.planId=plan.id;p.acceptedTurn=s.turn;
  transitionPlan(s,plan,'Preparing','A shared operation was accepted.');
  bindCommitments(s,o,p);
  if(p.house!==o.owner)bindCommitments(s,o,operationMember(o,o.owner));
  return {ok:true};
}
function bindCommitments(s,o,p) {
  if(!acceptedMembers(o).some(x=>x.house!==p.house)||operationPledges(s,o).some(x=>x.debtor===p.house))return;
  if(p.food)pledge(s,o,p,'supply','PROMISE',o.owner,p.food);
  if(p.role!=='supply') {
    pledge(s,o,p,'rally','POSITION',p.rally);
    pledge(s,o,p,attackRole(p)?'attack':p.role,attackRole(p)?'JOINT_WAR':'DEFEND',attackRole(p)?o.target:p.rally);
  }
  if(p.requiredSiege)pledge(s,o,p,'siege','BUILD_DEFENSES',p.rally);
}
export function createOperation(s,owner,terms={}) {
  initializeCooperation(s);
  if(!terms||typeof terms!=='object')return fail('Invalid operation terms.');
  if(s.outcome || s.phase==='founding' || !alive(s,owner))return fail('Operations require an active kingdom.');
  if(typeof terms.name!=='string'||!terms.name.trim()||terms.name.length>60)return fail('Name the operation (up to 60 characters).');
  const targetTile=s.tiles[terms.targetTile];
  if(!targetTile || !targetTile.owner || targetTile.owner===owner || !alive(s,targetTile.owner))return fail('Choose a foreign objective.');
  if(!integer(terms.attackStart,s.turn+1,s.turn+20)||!integer(terms.attackEnd,terms.attackStart,Math.min(s.turn+24,terms.attackStart+6)))return fail('Plan an attack 1–20 turns ahead with a window of up to 6 turns.');
  if(!Array.isArray(terms.participants)||terms.participants.length<2||terms.participants.length>5||new Set(terms.participants.map(p=>p?.house)).size!==terms.participants.length||terms.participants.some(p=>!p||typeof p!=='object'))return fail('Choose two to five different participating Houses.');
  const participants=terms.participants.map(p=>({house:p?.house,role:p.role,rally:p.rally,requiredTroops:p.requiredTroops,requiredSiege:p.requiredSiege,food:p.food,status:'invited',planId:null,acceptedTurn:null}));
  if(participants.find(p=>p.house===owner)?.role!=='assault')return fail('The leader must commit to the main assault.');
  if(s.cooperation.operations.some(o=>o.owner===owner&&s.turn-o.createdTurn<6))return fail('Allow six turns between new operations.');
  const o={id:`OP-${s.nextId}`,owner,name:terms.name.trim(),target:targetTile.owner,targetTile:targetTile.id,createdTurn:s.turn,updatedTurn:s.turn,attackStart:terms.attackStart,attackEnd:terms.attackEnd,status:'Preparing',reason:'Gathering consent and preparations.',participants,exposure:0,exposureReasons:[],launchedTurn:null};
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
  if(p.operationTask==='attack')done=p.delivered||o.launchedTurn!==null&&s.militaryEvents.some(e=>e.id>p.eventAfter&&e.turn>=o.launchedTurn&&e.attacker===p.debtor&&e.defender===o.target&&e.tile===o.targetTile);
  if(['hold','defend'].includes(p.operationTask)) {
    if(p.lastVerified!==s.turn){p.held=stationed&&[p.debtor,o.owner].includes(s.tiles[member.rally].owner)?Math.min(2,p.held+1):0;p.lastVerified=s.turn;}
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
    for(const p of acceptedMembers(o))if(!alive(s,p.house)||p.house!==o.owner&&!treaty(s,o.owner,p.house,'alliance')) {
      for(const row of pendingPledges(s,o,p.house))row.breached=true;
      if(p.house===o.owner){closeOperation(s,o,'Abandoned','The leading House was defeated.');break;}
      p.status='withdrawn';const plan=currentPlan(s,p);if(plan)transitionPlan(s,plan,'Abandoned','The alliance ended or the House was defeated.');stopOrders(s,p);
    }
    if(!ongoingOperation(o))continue;
    const tile=s.tiles[o.targetTile],members=acceptedMembers(o);
    if(tile.owner!==o.target) {
      // Verify the last combat before closing and releasing redundant obligations.
      for(const row of pendingPledges(s,o))if(operationPledgeProgress(s,row)==='fulfilled')row.delivered=true;
      closeOperation(s,o,members.some(p=>p.house===tile.owner)?'Completed':'Abandoned','The objective changed hands.');continue;
    }
    if(members.some(p=>protectedPeace(s,p.house,o.target)||o.launchedTurn!==null&&attackRole(p)&&!atWar(s,p.house,o.target))) {closeOperation(s,o,'Abandoned','A peace agreement prevents the shared campaign.');continue;}
    if(s.turn>o.attackEnd+(o.launchedTurn===null?0:8)) {
      for(const row of pendingPledges(s,o))if(s.turn>=row.deadline&&operationPledgeProgress(s,row)!=='fulfilled')row.breached=true;
      closeOperation(s,o,'Abandoned',o.launchedTurn===null?'The attack window passed without preparations.':'The campaign stalled after the attack window.');continue;
    }
    refreshExposure(s,o);
    if(afterMovement||o.status!=='Preparing')continue;
    if(s.turn>=o.attackStart&&members.length>=2&&members.every(p=>operationProgress(s,o,p).ready)) {
      const fighters=members.filter(attackRole);
      const legal=fighters.every(p=>{const probe={...s,wars:[...s.wars,[p.house,o.target].sort().join(':')]};return armiesOf(s,p.house).some(a=>findPath(probe,a.tile,o.targetTile,p.house).length||a.tile===o.targetTile);});
      if(!legal){o.reason='Waiting for legal routes to the objective.';continue;}
      for(const p of fighters){if(!atWar(s,p.house,o.target))declareWar(s,p.house,o.target);const plan=currentPlan(s,p);plan.wasAtWar=true;transitionPlan(s,plan,'Committed','Shared attack window opened; the operation is ready.');}
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
  if(o.status==='Preparing'||!attackRole(p)) {
    orderArmy(s,k.id,a.id,p.rally,a.tile===p.rally?'hold':'move');return plan;
  }
  const t=s.tiles[o.targetTile],assessment=assaultAssessment(s,k,a,t);
  if(assessment.assault&&atWar(s,k.id,o.target)) {
    const avoid=dangerousTiles(s,k,a);avoid.delete(t.id);
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
      const r=relation(s,k.id,o.owner),enemy=relation(s,k.id,o.target);
      const benefit=(atWar(s,k.id,o.target)?25:0)+enemy.grievance*.35+enemy.fear*.2+k.aggression*12;
      const troops=armiesOf(s,k.id).reduce((n,a)=>n+sizeOf(a),0);
      const score=r.trust*.5+r.reliability*.15-r.grievance*.5+benefit-enemy.dependency*.5;
      const decision=score<20||memberError(s,o,p)?'decline':p.requiredTroops>troops&&p.requiredTroops>1?'counter':'accept';
      respondOperation(s,k.id,o.id,decision);
    }
  }
  const own=memberOperation(s,k.id);
  if(own&&pendingPledges(s,own,k.id).some(p=>p.operationTask==='supply'&&!p.delivered)&&kingdom(s,k.id).resources.food>=operationMember(own,k.id).food+30)supplyOperation(s,k.id,own.id);
}
export function proposeJointOperation(s,k,target,tile) {
  const partners=s.kingdoms.filter(h=>h.id!==k.id&&h.id!==target&&alive(s,h.id)&&treaty(s,k.id,h.id,'alliance')&&!protectedPeace(s,h.id,target)&&!memberOperation(s,h.id)&&relation(s,h.id,k.id).trust>=20)
    .sort((a,b)=>Number(atWar(s,b.id,target))-Number(atWar(s,a.id,target))||a.id.localeCompare(b.id)).slice(0,2);
  if(!partners.length)return null;
  const participants=[{house:k.id,role:'assault',requiredTroops:30,requiredSiege:0,food:0},...partners.map((h,i)=>({house:h.id,role:i===1?'supply':'flank',requiredTroops:i===1?0:20,requiredSiege:0,food:i===1?30:0}))];
  for(const p of participants)p.rally=defaultRally(s,p.house,tile.id,p.role);
  const travel=Math.max(...participants.map(p=>distance(s.tiles[p.rally],tile)));
  const attackStart=s.turn+Math.min(12,Math.max(3,Math.ceil(travel/3)));
  const result=createOperation(s,k.id,{name:`Operation ${tile.name||'Iron Gate'}`.slice(0,60),targetTile:tile.id,attackStart,attackEnd:attackStart+3,participants});
  return result.ok?operationFor(s,result.operationId):null;
}
export function publicMobilizations(s,viewer) {
  const border=Object.values(s.tiles).filter(t=>t.owner===viewer);
  return s.kingdoms.filter(k=>k.id!==viewer).flatMap(k=>{
    const forces=armiesOf(s,k.id).filter(a=>(s.knowledgeView||a.path.length)&&border.some(t=>distance(s.tiles[a.tile],t)<=4));
    const count=forces.reduce((n,a)=>n+sizeOf(a),0),siege=forces.reduce((n,a)=>n+familyCount(a,'siege'),0);
    return count>=40||siege>=2?[{house:k.id,text:`${k.name}: ${count} troops${siege?` including ${siege} siege engines`:''} observed near your frontier. Their objective is unknown.`}]:[];
  });
}
