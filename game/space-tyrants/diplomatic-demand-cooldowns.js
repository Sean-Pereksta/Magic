/* Space Tyrants — pacing for major rival concession demands.
   Reparations and territorial demands should feel consequential, not like inbox spam.
   Ordinary diplomatic traffic may continue while major demands cool down. */

const STX_DDC_PAIR_COOLDOWN=180;
const STX_DDC_GLOBAL_PLAYER_COOLDOWN=60;

function stxDDCIsMajorDemand(q){
  if(!q)return false;
  if(q.kind==="territorial"||q.kind==="tribute")return true;
  return q.kind==="ultimatum"&&(q.actionKind==="tribute"||q.actionKind==="nonColonization");
}

function stxDDCRequestHistory(){
  return state.rivalDiplomacy?.requests||[];
}

function stxDDCLatestMajorDemand(from,to){
  const r=typeof stxRDPair==="function"?stxRDPair(from,to):null;
  const cached=Number.isFinite(r?.lastMajorConcessionDemandAt)?r.lastMajorConcessionDemandAt:-999;
  const historical=stxDDCRequestHistory()
    .filter(q=>q.from===from&&q.to===to&&stxDDCIsMajorDemand(q))
    .reduce((latest,q)=>Math.max(latest,Number(q.createdAt)||-999),-999);
  return Math.max(cached,historical);
}

function stxDDCLatestPlayerMajorDemand(){
  return stxDDCRequestHistory()
    .filter(q=>q.to===0&&q.from!==0&&stxDDCIsMajorDemand(q))
    .reduce((latest,q)=>Math.max(latest,Number(q.createdAt)||-999),-999);
}

function stxDDCMajorDemandCooldown(from,to){
  if(to!==0||from===0)return 0;
  const pairAge=state.simTime-stxDDCLatestMajorDemand(from,to);
  const globalAge=state.simTime-stxDDCLatestPlayerMajorDemand();
  return Math.max(0,STX_DDC_PAIR_COOLDOWN-pairAge,STX_DDC_GLOBAL_PLAYER_COOLDOWN-globalAge);
}

function stxDDCRecordMajorDemand(q){
  if(!q||q.to!==0||q.from===0||!stxDDCIsMajorDemand(q))return;
  const r=typeof stxRDPair==="function"?stxRDPair(q.from,q.to):null;
  if(r)r.lastMajorConcessionDemandAt=state.simTime;

  /* Major demands get one clear decision window. The standard halfway reminder
     is useful for routine diplomacy but makes reparations/territorial demands
     feel like the rival is repeating itself. */
  q.reminderAt=Infinity;
}

function stxDDCSoftRequestKind(from,to){
  const e=empire(from),p=e?.foreignPolicy||{},r=typeof stxRDPair==="function"?stxRDPair(from,to):null;
  const borderFleets=typeof stxRDBorderFleets==="function"?stxRDBorderFleets(to,from).length:0;
  if(borderFleets>0&&(r?.tension||0)>28&&random()<.42)return"military";
  if((p.commercialism||0)>.62&&random()<.48)return"trade";
  return"resource";
}

/* Normal request generation can keep diplomacy alive during the cooldown, but
   it substitutes a softer request rather than immediately asking for money or
   territory again. Explicit major-demand attempts simply wait. */
if(typeof stxRDCreateRequest==="function"){
  const STX_DDC_CreateRequest=stxRDCreateRequest;
  stxRDCreateRequest=function(from,to,preferred=null,forcedTarget=null){
    const remaining=stxDDCMajorDemandCooldown(from,to);
    const explicitlyMajor=preferred==="territorial"||preferred==="tribute";

    if(remaining>0&&to===0&&from!==0){
      if(explicitlyMajor||forcedTarget)return null;
      if(preferred===null)preferred=stxDDCSoftRequestKind(from,to);
    }

    const q=STX_DDC_CreateRequest(from,to,preferred,forcedTarget);
    stxDDCRecordMajorDemand(q);
    return q;
  };
}

/* Final ultimatums used to become eligible only 18 simulation units after the
   previous demand. Keep withdrawal ultimatums available, but pace reparations
   and territorial-abandonment ultimatums through the same major-demand timer. */
if(typeof stxRDCreateUltimatum==="function"){
  const STX_DDC_CreateUltimatum=stxRDCreateUltimatum;
  stxRDCreateUltimatum=function(from,to){
    if(to===0&&from!==0){
      const contest=typeof stxRDContestFor==="function"?stxRDContestFor(from,to):null;
      const target=contest&&state.planets.find(p=>p.id===contest.planetId);
      const borderFleets=typeof stxRDBorderFleets==="function"?stxRDBorderFleets(to,from).length:0;
      const wouldBeMajor=!!target||borderFleets===0;
      if(wouldBeMajor&&stxDDCMajorDemandCooldown(from,to)>0)return null;
    }

    const q=STX_DDC_CreateUltimatum(from,to);
    stxDDCRecordMajorDemand(q);
    return q;
  };
}

/* Existing saves may already contain a pending major demand created before this
   layer loaded. Remove its reminder immediately so the fix applies mid-game. */
if(typeof stxRDProcessRequests==="function"){
  const STX_DDC_ProcessRequests=stxRDProcessRequests;
  stxRDProcessRequests=function(){
    stxDDCRequestHistory().forEach(q=>{
      if(q.status==="pending"&&q.to===0&&stxDDCIsMajorDemand(q))q.reminderAt=Infinity;
    });
    return STX_DDC_ProcessRequests();
  };
}
