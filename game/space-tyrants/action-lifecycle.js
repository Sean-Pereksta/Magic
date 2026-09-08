/* Shared action receipts live on saved simulation objects. A completed receipt
   is immutable; a failed attempt restores object identities and committed goods. */
function stxAction(q,kind="action"){
  return q.stxAction||(q.stxAction={id:q.stxProjectId||q.id||`${kind}-${Math.floor(random()*1e12)}`,kind,state:"QUEUED",createdAt:state.simTime,updatedAt:state.simTime,attempts:0});
}
function stxActionSet(q,status,blocker=""){
  const a=stxAction(q);if(a.state==="COMPLETED")return a;
  if(a.state!==status||a.blocker!==blocker)a.updatedAt=state.simTime;
  a.state=status;a.blocker=blocker;return a;
}
function stxActionComplete(q,result){const a=stxActionSet(q,"COMPLETED");a.completedAt=state.simTime;a.resultId=result?.id||a.resultId;return result||true}
function stxActionCheckpoint(roots){
  const snapshots=new Map();
  function visit(o){if(!o||typeof o!=="object"||snapshots.has(o))return;
    if(o instanceof Set){snapshots.set(o,{set:[...o]});for(const v of o)visit(v);return}
    snapshots.set(o,{values:{...o},length:Array.isArray(o)?o.length:null});for(const v of Object.values(o))visit(v);
  }
  roots.forEach(visit);
  return()=>{for(const [o,s] of snapshots){if(s.set){o.clear();s.set.forEach(v=>o.add(v));continue}for(const k of Object.keys(o))if(!(k in s.values))delete o[k];Object.assign(o,s.values);if(s.length!==null)o.length=s.length}};
}
function stxActionEconomyRoots(){return [state.ships,state.fleets,...state.planets,...state.empires,state.contracts,state.militaryRequests,state.proposals,state.effects,state.events,state.news,state.rivalDiplomacy,state.resourceTradeEconomy,state.deepSpaceBases,state.deepSpaceBattles,state.deepSpaceOperations,state.battles].filter(Boolean)}
function stxActionAtomic(work,roots=stxActionEconomyRoots()){
  const ships=state.ships,fleets=state.fleets,undo=stxActionCheckpoint(roots);
  try{const result=work();if(result!==false&&result!=null)return result}catch(error){console.warn("[Space Tyrants] action rolled back",error)}
  undo();state.ships=ships;state.fleets=fleets;return false;
}
function stxActionFinish(q,kind,work,verify,roots){
  const a=stxAction(q,kind);if(a.state==="COMPLETED")return true;if(a.state==="EXECUTING")return false;
  stxActionSet(q,"READY");a.attempts++;stxActionSet(q,"EXECUTING");
  const result=stxActionAtomic(()=>{const result=work(a);return result&&verify(result)?result:false},roots);
  if(!result){stxActionSet(q,"RETRYING","Launch Ready — Retrying: transport creation unavailable");q.status=q.stxAction.blocker;return false}
  stxActionComplete(q,result);return true;
}
function stxActionFinalizeShip(p,q){
  const ok=stxActionFinish(q,"ship",a=>{const existing=[...state.ships,...state.fleets].find(s=>s.stxConstructionActionId===a.id);if(existing)return existing;const s=launchBuiltShip(p,q.type);if(s)s.stxConstructionActionId=a.id;return s},s=>state.ships.includes(s)||state.fleets.includes(s),[p,state.ships,state.fleets,empire(p.owner),state.effects,state.events,state.news]);
  if(ok){const i=p.buildQueue.indexOf(q);if(i>=0)p.buildQueue.splice(i,1)}return ok;
}
function stxActionFinalizeColony(p,q,d,target){
  if(stxAction(q,"colony").state==="COMPLETED")return true;
  const settlers=Math.min(q.volunteers,Math.max(0,p.pop-.004));if(settlers<=0){stxActionSet(q,"BLOCKED","Insufficient population to embark");return false}
  const ok=stxActionFinish(q,"colony",a=>{const existing=state.ships.find(s=>s.stxConstructionActionId===a.id);if(existing)return existing;
    const s=createShip("colony",p,target,p.owner,{cargo:{population:settlers,trained:Number(d.need.trained)||0},strength:4,missionTitle:`Settlement of ${target.name}`,volunteers:settlers,stxIFColonyMission:true,stxConstructionActionId:a.id});if(s)p.pop-=settlers;return s;
  },s=>state.ships.includes(s),[p,state.ships,state.fleets,empire(p.owner),state.effects,state.events,state.news]);
  if(ok&&p.expansionProject===q){p.expansionProject=null;if(p.owner===0)logEvent(`${fmtNum(settlers)} settlers departed for ${target.name}.`,"good")}return ok;
}
function stxActionArrive(s,apply){
  const a=stxAction(s,"arrival");if(a.state==="COMPLETED")return true;
  if(s.stxArrivalAttemptAt===state.simTime)return false;s.stxArrivalAttemptAt=state.simTime;a.attempts++;stxActionSet(s,"EXECUTING");
  let blocker="";const ok=stxActionAtomic(()=>{delete s.stxArrivalBlocked;apply();blocker=s.stxArrivalBlocked||"";return !blocker});
  if(!ok){stxActionSet(s,"RETRYING",blocker||"Arrival blocked — retrying");return false}stxActionComplete(s);return true;
}
function stxActionProgress(d){
  const q=d.q,a=stxAction(q,d.kind);if(a.state==="COMPLETED")return 100;
  const progress=stxSDProgress(d),missing=Object.entries(d.need||{}).filter(([r,n])=>n>0&&stxSDRemaining(d,r)>(typeof stxIFEpsilon==="function"?stxIFEpsilon(r):.000001));
  if(a.lastProgress!==progress){a.lastProgress=progress;a.lastProgressAt=state.simTime}
  if(d.kind==="expansion"&&!state.planets.some(p=>p.id===q.targetId&&p.owner===null))stxActionSet(q,"BLOCKED","Destination is no longer unclaimed");
  else if(a.state!=="RETRYING"){
    if(d.p.underAttack)stxActionSet(q,"BLOCKED","Construction paused during battle");
    else if(missing.length)stxActionSet(q,"BLOCKED",missing.map(([r,n])=>`${stxSDResourceLabel(r)}: ${stxSDFmt(r,stxSDDelivered(d,r))} / ${stxSDFmt(r,n)}`).join(" · "));
    else stxActionSet(q,progress>=1?"READY":progress>0?"ACTIVE":"QUEUED");
  }
  return Math.min(99,Math.floor(progress*100));
}
globalThis.SpaceTyrantsActions={ensure:stxAction,set:stxActionSet,atomic:stxActionAtomic,arrive:stxActionArrive,finishShip:stxActionFinalizeShip,finishColony:stxActionFinalizeColony};
