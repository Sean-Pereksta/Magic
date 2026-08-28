/* Space Tyrants — defensive war posture, clearer escalation, and faster camera zoom.
   This final runtime layer keeps AI empires from stripping their worlds for
   offensives, makes active invasions trigger defense-first behavior, adds a
   visible frustration window before AI declarations, and accelerates map zoom. */

const STX_DEC_FRUSTRATION_NOTICE_DELAY=10;
const STX_DEC_MIN_WAR_DELAY=30;
const STX_DEC_WARNING_TO_WAR_DELAY=14;
const STX_DEC_EXTRA_ZOOM_RATE=.0022;

function stxDECDefenseFloor(p){
  if(!p)return 9;
  return p.home?14:Math.max(9,7+(p.infra?.defense||0)*2+(p.orbitals?.base||0)*2);
}

/* Keep the newer enemy-war-pressure reserve logic, but make its floor a hard
   wartime minimum instead of merely a preference when building reserve fleets. */
if(typeof stxEWPReserveFloor==="function"){
  const STX_DEC_EWPReserveFloor=stxEWPReserveFloor;
  stxEWPReserveFloor=function(p){return Math.max(STX_DEC_EWPReserveFloor(p),stxDECDefenseFloor(p))};
}
if(typeof stxEWPIdleWarFleets==="function"){
  const STX_DEC_EWPIdleWarFleets=stxEWPIdleWarFleets;
  stxEWPIdleWarFleets=function(e,target){
    return STX_DEC_EWPIdleWarFleets(e,target).filter(f=>{
      const source=state.planets.find(p=>p.id===f.location);
      return !!source&&(Number(source.garrison)||0)>=stxDECDefenseFloor(source);
    });
  };
}

function stxDECIncomingThreatStrength(owner,worldId){
  return state.ships.filter(s=>s.owner===owner&&(s.type==="fleet"||s.type==="patrol")&&s.to===worldId).reduce((n,s)=>n+(Number(s.strength)||0),0);
}
function stxDECThreatenedWorlds(e,foeId){
  return owned(e.id).map(p=>{
    const incoming=stxDECIncomingThreatStrength(foeId,p.id),battle=activeBattleAt(p),underAttack=!!p.underAttack||!!battle;
    const value=(p.home?60:0)+(p.infra?.shipyard||0)*12+(p.infra?.factory||0)*7+(p.orbitals?.base||0)*18+(p.orbitals?.station||0)*8;
    return{p,incoming,underAttack,score:(underAttack?1000:0)+incoming*18+value};
  }).filter(x=>x.underAttack||x.incoming>0).sort((a,b)=>b.score-a.score);
}
function stxDECIdleDefender(e,target){
  const engaged=typeof stxEWPActiveBattleFleetIds==="function"?stxEWPActiveBattleFleetIds():new Set();
  return state.fleets.filter(f=>{
    if(f.owner!==e.id||f.destroyed||!f.location||engaged.has(f.id)||state.ships.some(s=>s.fleetId===f.id))return false;
    const source=state.planets.find(p=>p.id===f.location);
    if(!source||source.owner!==e.id||source.id===target.id||source.underAttack)return false;
    return (Number(source.garrison)||0)>=stxDECDefenseFloor(source)&&(Number(f.strength)||0)>4;
  }).sort((a,b)=>{
    const ap=state.planets.find(p=>p.id===a.location),bp=state.planets.find(p=>p.id===b.location);
    return dist(ap,target)-dist(bp,target)||(Number(b.strength)||0)-(Number(a.strength)||0);
  })[0]||null;
}
function stxDECLaunchDefender(e,target,f){
  if(!f||!target)return false;
  const source=state.planets.find(p=>p.id===f.location);if(!source)return false;
  if((Number(source.garrison)||0)<stxDECDefenseFloor(source))return false;
  const alreadyInbound=state.ships.filter(s=>s.owner===e.id&&(s.type==="fleet"||s.type==="patrol")&&s.to===target.id).length;
  if(alreadyInbound>=2)return false;
  const oldLocation=f.location,oldStatus=f.status,strength=Math.max(5,Number(f.strength)||5);
  f.location=null;f.status=`Defending ${target.name}`;
  const ship=createShip("fleet",source,target,e.id,{strength,fleetId:f.id,vesselName:f.name,reinforcement:true,status:`Defending ${target.name}`,speedBoost:1+(source.orbitals?.base||0)*.35});
  if(!ship){f.location=oldLocation;f.status=oldStatus;return false}
  return true;
}
function stxDECReinforceThreat(e,target){
  let f=stxDECIdleDefender(e,target);
  if(!f&&typeof stxEWPAssembleReserveFleet==="function")f=stxEWPAssembleReserveFleet(e,target);
  return stxDECLaunchDefender(e,target,f);
}

/* Defense comes first. An AI empire facing an actual incoming invasion pauses
   new offensive launches, reinforces the threatened world, and keeps that hold
   briefly after the immediate contact clears. */
const STX_DEC_launchWarCampaign=launchWarCampaign;
launchWarCampaign=function(e,foeId){
  if(!e||e.id===0)return STX_DEC_launchWarCampaign(e,foeId);
  const war=getWar(e.id,foeId);if(!war)return false;
  e.enemyWarPressure=e.enemyWarPressure||{};
  const threats=stxDECThreatenedWorlds(e,foeId);
  if(threats.length){
    const threat=threats[0];
    stxDECReinforceThreat(e,threat.p);
    e.enemyWarPressure.defenseHoldUntil=Math.max(Number(e.enemyWarPressure.defenseHoldUntil)||0,state.simTime+(threat.underAttack?22:14));
    e.enemyWarPressure.defenseTargetId=threat.p.id;
    return false;
  }
  if(state.simTime<(Number(e.enemyWarPressure.defenseHoldUntil)||0))return false;
  const hasSafeLaunchWorld=owned(e.id).some(p=>!p.underAttack&&(Number(p.garrison)||0)>=stxDECDefenseFloor(p));
  if(!hasSafeLaunchWorld){if(typeof stxEWPMobilizeEmpire==="function")stxEWPMobilizeEmpire(e);return false}
  return STX_DEC_launchWarCampaign(e,foeId);
};

function stxDECLatestGrievanceHolder(a,b){
  const r=stxRDPair(a,b),recent=typeof stxRDRecent==="function"?stxRDRecent(r?.grievances||[],10):(r?.grievances||[]);
  return recent[0]?.holder??(empire(a).foreignPolicy?.aggression>empire(b).foreignPolicy?.aggression?a:b);
}
function stxDECSignalFrustration(a,b){
  const r=stxRDPair(a,b);if(!r||!Number.isFinite(r.ultimatumRejectedAt)||r.ultimatumRejectedAt<0)return false;
  const age=state.simTime-r.ultimatumRejectedAt;if(age<STX_DEC_FRUSTRATION_NOTICE_DELAY)return false;
  if((Number(r.frustrationWarnedAt)||-999)>=r.ultimatumRejectedAt)return true;
  const aggressor=stxDECLatestGrievanceHolder(a,b),defender=aggressor===a?b:a;
  if(aggressor===0)return false;
  r.frustrationWarnedAt=state.simTime;r.frustrationSince=state.simTime;r.borderPosture="Frustrated — reinforcing";
  if(typeof stxRDMobilize==="function")stxRDMobilize(aggressor,defender,false);
  if(defender===0){
    logEvent(`${empire(aggressor).name} signals mounting frustration. Fleets are reinforcing the frontier, but war has not been declared.`,"warning");
    galacticNews(`${empire(aggressor).name.toUpperCase()} WARNS PATIENCE IS RUNNING OUT`,`${empire(aggressor).name} has publicly hardened its position after the failed ultimatum. Border forces are reinforcing while diplomats leave a final window for de-escalation.`,"warning");
  }
  return true;
}
function stxDECWarGate(a,b){
  const r=stxRDPair(a,b);if(!r||r.escalationStage<5||!Number.isFinite(r.ultimatumRejectedAt)||r.ultimatumRejectedAt<0)return false;
  const age=state.simTime-r.ultimatumRejectedAt,warningAge=state.simTime-(Number(r.frustrationWarnedAt)||state.simTime);
  if(age<STX_DEC_MIN_WAR_DELAY||warningAge<STX_DEC_WARNING_TO_WAR_DELAY)return false;
  const aggressor=stxDECLatestGrievanceHolder(a,b),defender=aggressor===a?b:a;if(aggressor===0)return false;
  const support=typeof stxRDThreatSupport==="function"?stxRDThreatSupport(aggressor,defender):.5;
  const recent=typeof stxRDRecent==="function"?stxRDRecent(r.grievances||[],12):(r.grievances||[]);
  const serious=recent.filter(g=>(g.severity||0)>=2||(g.weight||0)>=8).length;
  const repeatedPressure=(r.refusedRequests||0)+(r.ignoredRequests||0)>=2||serious>=2||r.tension>=88;
  return r.tension>=76&&support>=.58&&repeatedPressure;
}

/* Preserve the richer diplomacy simulation, but require a visible frustration
   phase and stronger readiness before its existing probabilistic war roll. */
if(typeof stxRDMaybeDeclareWar==="function"){
  const STX_DEC_RDMaybeDeclareWar=stxRDMaybeDeclareWar;
  stxRDMaybeDeclareWar=function(a,b){
    const r=stxRDPair(a,b);if(!r||getWar(a,b))return false;
    if(r.escalationStage>=5&&r.ultimatumRejectedAt>=0)stxDECSignalFrustration(a,b);
    if(!stxDECWarGate(a,b))return false;
    return STX_DEC_RDMaybeDeclareWar(a,b);
  };
}
if(typeof stxRDRecalculatePair==="function"){
  const STX_DEC_RDRecalculatePair=stxRDRecalculatePair;
  stxRDRecalculatePair=function(a,b){
    const r=STX_DEC_RDRecalculatePair(a,b);if(!r||getWar(a,b))return r;
    if(r.escalationStage>=5&&r.ultimatumRejectedAt>=0){
      const age=state.simTime-r.ultimatumRejectedAt;
      if(age>=STX_DEC_FRUSTRATION_NOTICE_DELAY)r.borderPosture=stxDECWarGate(a,b)?"Final warning — mobilized":"Frustrated — reinforcing";
    }
    return r;
  };
}

/* The core wheel multiplier is intentionally conservative. Apply an additional
   cursor-anchored zoom step so normal mouse wheels and trackpads traverse the
   galaxy several times faster without changing min/max zoom bounds. */
canvas.addEventListener("wheel",e=>{
  const before=screenToWorld(e.clientX,e.clientY),delta=clamp(e.deltaY,-180,180),factor=Math.exp(-delta*STX_DEC_EXTRA_ZOOM_RATE);
  state.camera.zoom=clamp(state.camera.zoom*factor,.22,2.15);
  const after=screenToWorld(e.clientX,e.clientY);
  state.camera.x=clamp(state.camera.x+before.x-after.x,0,WORLD.w);
  state.camera.y=clamp(state.camera.y+before.y-after.y,0,WORLD.h);
},{passive:false});
