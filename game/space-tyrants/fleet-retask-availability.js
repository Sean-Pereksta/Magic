/* Space Tyrants — combat-only player fleet lock.
   Explicit Invade / Concentrate / Defend orders may retask any surviving Mandate
   fleet, including fleets that are defending, concentrated, refitting, patrolling,
   retreating, staged, docked, or already in transit. A fleet is unavailable only
   while its persistent fleet record is participating in a live battle. */

function stxFARActiveCombatFleetIds(){
  const ids=new Set();
  state.battles.forEach(b=>{
    (b.attackerFleetIds||[]).forEach(id=>ids.add(id));
    (b.defenderFleetIds||[]).forEach(id=>ids.add(id));
  });
  return ids;
}
function stxFARTransit(f){return f&&state.ships.find(s=>s.fleetId===f.id)||null}
function stxFARPosition(f){
  const s=stxFARTransit(f);if(s&&Number.isFinite(s.x)&&Number.isFinite(s.y))return{x:s.x,y:s.y};
  const p=f&&state.planets.find(x=>x.id===f.location);return p?{x:p.x,y:p.y}:null;
}
function stxFARDistance(f,target){const p=stxFARPosition(f);return p&&target?Math.hypot(p.x-target.x,p.y-target.y):Infinity}
function stxFARAvailable(f){
  if(!f||f.owner!==0||f.destroyed)return false;
  if(typeof stxFCACommitted!=="undefined"&&stxFCACommitted.has(f.id))return false;
  return !stxFARActiveCombatFleetIds().has(f.id);
}
function stxFARTravelLabel(f){
  const s=stxFARTransit(f);if(!s)return state.planets.find(p=>p.id===f.location)?.name||"Fleet position unknown";
  const to=state.planets.find(p=>p.id===s.to);return `In transit${to?` → ${to.name}`:""}`;
}

/* The player-facing allocation pool is deliberately much broader than legacy
   automatic deployment selectors. Current-batch commitments remain excluded so
   one fleet cannot satisfy two simultaneous mandates, but status/assignment,
   refit, docking and movement no longer make a fleet disappear from the next
   explicit player order. */
if(typeof stxFCAFreeFleet==="function"){
  stxFCAFreeFleet=function(f){return stxFARAvailable(f)};
}
if(typeof stxFCAFree==="function"){
  stxFCAFree=function(target=null){
    return state.fleets.filter(stxFARAvailable).sort((a,b)=>{
      if(target){const ad=stxFARDistance(a,target),bd=stxFARDistance(b,target);if(ad!==bd)return ad-bd}
      return (Number(b.strength)||0)-(Number(a.strength)||0);
    });
  };
}

function stxFARCancelRefit(f){
  const q=f?.stxRefit;if(!q)return false;
  const p=state.planets.find(x=>x.id===q.planetId);
  if(p){
    Object.entries(q.delivered||{}).forEach(([r,a])=>{if((Number(a)||0)>0)p.stock[r]=(p.stock[r]||0)+(Number(a)||0)});
    p.orders=(p.orders||[]).filter(o=>!(o.type==="fleetRefit"&&String(o.label||"").includes(f.name)));
  }
  f.stxRefit=null;
  if(f.stxAssignment?.kind==="upgrade")f.stxAssignment=null;
  if(f.readiness==="refitting")f.readiness="ready";
  logEvent(`FLEET REFIT CANCELLED: ${f.name} was returned to operational control for a new fleet order. Delivered refit materials were returned to local stock.`,"warning");
  return true;
}
function stxFARDetachOldInvasion(s,f,newPlanId=null){
  const oldId=s?.invasionPlanId;if(!oldId||oldId===newPlanId)return;
  const plans=empire(0)?.invasionPlans||[],old=plans.find(p=>p.id===oldId);if(!old)return;
  old.stxAssignedFleetIds=(old.stxAssignedFleetIds||[]).filter(id=>id!==f.id);
  const otherInbound=state.ships.some(x=>x!==s&&x.owner===0&&x.invasionPlanId===oldId);
  if(old.stxManualAllocation&&!otherInbound)old.status="awaiting player reinforcement";
}
function stxFARPrepareTransit(s,f,target,kind,plan=null,war=null){
  if(!s||!f||!target)return false;
  stxFARCancelRefit(f);if(typeof stxFCAClear==="function")stxFCAClear(f);
  stxFARDetachOldInvasion(s,f,plan?.id||null);
  const x=Number.isFinite(s.x)?s.x:(stxFARPosition(f)?.x??target.x),y=Number.isFinite(s.y)?s.y:(stxFARPosition(f)?.y??target.y);
  s.type="fleet";s.startX=x;s.startY=y;s.x=x;s.y=y;s.progress=0;s.distance=Math.max(1,Math.hypot(target.x-x,target.y-y));s.to=target.id;
  delete s.battleId;delete s.retreat;delete s.reinforcement;delete s.stxFleetOrderKind;delete s.servicePurpose;delete s.stxManualFleetOrder;delete s.invasionPlanId;delete s.warId;
  s.strength=Math.max(1,Number(s.strength)||Number(f.strength)||1);s.vesselName=f.name;
  if(kind==="invade"){
    s.warId=war?.id;s.invasionPlanId=plan?.id;s.stxManualFleetOrder=true;
    f.status=`Invading ${target.name}`;
  }else{
    s.reinforcement=kind==="defend";s.stxFleetOrderKind=kind;s.servicePurpose=kind;
    f.status=kind==="defend"?`Redeploying to defend ${target.name}`:`Concentrating at ${target.name}`;
  }
  f.location=null;f.readiness="inbound";f.strength=s.strength;
  return true;
}

/* Re-task a moving fleet from its actual map position instead of teleporting it
   back to an origin planet. The same ship record changes destination and begins
   a new route from its current coordinates. */
if(typeof stxFCAMove==="function"){
  stxFCAMove=function(f,target,kind){
    if(!f||!target||!stxFARAvailable(f))return false;
    const moving=stxFARTransit(f);if(moving)return stxFARPrepareTransit(moving,f,target,kind);
    stxFARCancelRefit(f);if(typeof stxFCAClear==="function")stxFCAClear(f);
    const source=state.planets.find(p=>p.id===f.location);if(!source)return false;
    if(source.id===target.id){stxFCAAssign(f,target,kind);return true}
    const old={location:f.location,status:f.status,readiness:f.readiness};f.location=null;f.readiness="inbound";f.status=kind==="defend"?`Redeploying to defend ${target.name}`:`Concentrating at ${target.name}`;
    const ship=createShip("fleet",source,target,0,{strength:Math.max(1,Number(f.strength)||1),fleetId:f.id,vesselName:f.name,reinforcement:kind==="defend",stxFleetOrderKind:kind,servicePurpose:kind,speedBoost:1.08+(source.orbitals?.base||0)*.22});
    if(!ship){Object.assign(f,old);return false}return true;
  };
}

/* Replace the final invasion executor (including the warfare-balance manual
   selector wrapper) so recommended and manual allocation obey the same rule. */
if(typeof stxFCAExecuteInvade==="function"){
  stxFCAExecuteInvade=function(target,count){
    if(!target||target.owner===null||target.owner===0||typeof stxQueueInvasion!=="function")return 0;
    const selected=typeof stxWBSelectedInvasionFleets==="function"?stxWBSelectedInvasionFleets(target,count):stxFCAFree(target).slice(0,Math.max(1,count||1));
    if(!selected.length)return 0;
    if(!stxQueueInvasion(target,`Manual invasion of ${target.name}`,false))return 0;
    const plan=(empire(0).invasionPlans||[]).find(p=>p.targetId===target.id),war=getWar(0,target.owner);let launched=0,ids=[];
    for(const f of selected){
      if(!stxFARAvailable(f))continue;
      let ok=false;const moving=stxFARTransit(f);
      if(moving)ok=stxFARPrepareTransit(moving,f,target,"invade",plan,war);
      else{
        stxFARCancelRefit(f);if(typeof stxFCAClear==="function")stxFCAClear(f);
        const source=state.planets.find(p=>p.id===f.location);if(!source)continue;
        const old={location:f.location,status:f.status,readiness:f.readiness};f.location=null;f.readiness="inbound";f.status=`Invading ${target.name}`;
        const ship=createShip("fleet",source,target,0,{strength:Math.max(1,Number(f.strength)||1),fleetId:f.id,vesselName:f.name,warId:war?.id,invasionPlanId:plan?.id,stxManualFleetOrder:true,speedBoost:1.08+(source.orbitals?.base||0)*.38});
        if(!ship){Object.assign(f,old);continue}ok=true;
      }
      if(ok){stxFCACommitted.add(f.id);launched++;ids.push(f.id)}
    }
    if(plan&&launched){plan.stxManualAllocation=true;plan.stxRequestedFleetCount=launched;plan.stxAssignedFleetIds=ids;plan.stxManualLaunchedAt=state.simTime;plan.lastLaunchAt=state.simTime;plan.status="fleet inbound"}
    if(launched){stxFCAUsedTargets.add(target.id);logEvent(`INVASION LAUNCHED: ${launched} fleet${launched===1?"":"s"} retasked to ${target.name}. Only fleets in active combat were excluded.`,"warning");if(typeof stxActivity==="function")stxActivity(`${launched} fleet${launched===1?"":"s"} committed to ${target.name}; in-transit forces were redirected from their current positions.`,target.id,null,"warning");if(typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator()}
    return launched;
  };
}

/* Fleet upgrades are intentionally still stationary work. Broadening the player
   Invade/Concentrate pool must not make a moving or currently-refitting fleet a
   candidate for a new refit mandate. */
if(typeof stxFCAUpgradeCandidates==="function"){
  stxFCAUpgradeCandidates=function(){
    const engaged=stxFARActiveCombatFleetIds();
    return state.fleets.filter(f=>{
      if(!f||f.owner!==0||f.destroyed||engaged.has(f.id)||f.stxRefit||stxFARTransit(f)||!f.location)return false;
      const p=state.planets.find(x=>x.id===f.location);if(!p||p.owner!==0||state.simTime-(f.lastUpgradeAt??-999)<=70)return false;
      return typeof stxWBPlayerRefitTarget!=="function"||stxWBPlayerRefitTarget(f,p)>(Number(f.strength)||0)+.5;
    }).sort((a,b)=>(Number(a.strength)||0)-(Number(b.strength)||0));
  };
}

/* Manual allocation now reports where a moving fleet actually is and uses its
   live position for distance ordering/preview instead of showing Unknown / ?. */
if(typeof stxWBRenderManualFleets==="function"){
  stxWBRenderManualFleets=function(){
    const list=$("stxWBManualList"),order=stxFCAQueue?.[0],target=state.planets.find(p=>p.id===$("stxFCATarget")?.value);if(!list)return;
    if(!stxWBManualMode||order?.kind!=="invade"||!target){list.innerHTML="";return}
    const fleets=stxFCAFree(target);list.innerHTML=fleets.map(f=>{const checked=stxWBManualFleetIds.has(f.id)?"checked":"",d=stxFARDistance(f,target);return`<label class="stx-wb-fleet"><input type="checkbox" data-wb-fleet="${f.id}" ${checked}><span><b>${f.name}</b><small>${stxFARTravelLabel(f)} · strength ${Math.round(f.strength||0)}</small></span><span>${Number.isFinite(d)?Math.round(d):"?"}u</span></label>`}).join("");
    list.querySelectorAll("[data-wb-fleet]").forEach(i=>i.onchange=()=>{i.checked?stxWBManualFleetIds.add(i.dataset.wbFleet):stxWBManualFleetIds.delete(i.dataset.wbFleet);stxFCASyncUi()});
  };
}
