/* Space Tyrants — enemy wartime pressure.
   Active AI wars must produce real expeditionary fleets. Distance controls
   travel time instead of blocking attacks, while wartime mobilization keeps
   garrisons and trained crews from deadlocking after a single offensive. */

function stxEWPEnsureState(){
  (state.empires||[]).forEach(e=>{
    const s=e.enemyWarPressure||(e.enemyWarPressure={});
    if(!Number.isFinite(s.lastReserveAt))s.lastReserveAt=-999;
    if(!Number.isFinite(s.lastCommissionAt))s.lastCommissionAt=-999;
  });
}

const STX_EWP_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_EWP_generateGalaxy();stxEWPEnsureState()};
const STX_EWP_loadGame=loadGame;
loadGame=function(){const ok=STX_EWP_loadGame();if(ok)stxEWPEnsureState();return ok};
stxEWPEnsureState();

function stxEWPActiveBattleFleetIds(){
  const ids=new Set();
  state.battles.forEach(b=>{
    (b.attackerFleetIds||[]).forEach(id=>ids.add(id));
    (b.defenderFleetIds||[]).forEach(id=>ids.add(id));
  });
  return ids;
}
function stxEWPFleetBusy(f,engaged=stxEWPActiveBattleFleetIds()){
  return !f||f.destroyed||engaged.has(f.id)||state.ships.some(s=>s.fleetId===f.id);
}
function stxEWPIdleWarFleets(e,target){
  const engaged=stxEWPActiveBattleFleetIds();
  return state.fleets.filter(f=>{
    if(f.owner!==e.id||stxEWPFleetBusy(f,engaged)||!f.location||(Number(f.strength)||0)<=4)return false;
    const p=state.planets.find(q=>q.id===f.location);
    return !!p&&p.owner===e.id&&!p.underAttack;
  }).sort((a,b)=>{
    const ap=state.planets.find(p=>p.id===a.location),bp=state.planets.find(p=>p.id===b.location);
    const ad=ap?dist(ap,target):Infinity,bd=bp?dist(bp,target):Infinity;
    return ad-bd||(Number(b.strength)||0)-(Number(a.strength)||0)||(b.veterans||0)-(a.veterans||0);
  });
}
function stxEWPTargetDefense(p){
  return (Number(p.garrison)||0)+(p.infra?.defense||0)*8+(p.orbitals?.station||0)*9+(p.orbitals?.base||0)*22;
}
function stxEWPTargetValue(p){
  return (p.home?32:0)+(p.infra?.factory||0)*12+(p.infra?.shipyard||0)*18+(p.infra?.mine||0)*5+(p.orbitals?.station||0)*15+(p.orbitals?.base||0)*28+(p.pop||0)*22;
}
function stxEWPChooseTarget(e,foeId){
  const sources=owned(e.id).filter(p=>!p.underAttack);
  if(!sources.length)return null;
  return owned(foeId).filter(p=>!p.underAttack).map(p=>{
    const d=Math.min(...sources.map(s=>dist(s,p)));
    const score=d/95+stxEWPTargetDefense(p)*.72-stxEWPTargetValue(p)*.3;
    return{p,score,d};
  }).sort((a,b)=>a.score-b.score)[0]||null;
}
function stxEWPReserveFloor(p){
  return p.home?12:Math.max(8,6+(p.infra?.defense||0)*2);
}
function stxEWPReserveSource(e,target){
  return owned(e.id).filter(p=>{
    if(p.underAttack)return false;
    const floor=stxEWPReserveFloor(p);
    return (Number(p.garrison)||0)-floor>=6;
  }).sort((a,b)=>{
    const ad=dist(a,target),bd=dist(b,target);
    const av=(Number(a.garrison)||0)+(a.infra?.shipyard||0)*4+(a.infra?.training||0)*3;
    const bv=(Number(b.garrison)||0)+(b.infra?.shipyard||0)*4+(b.infra?.training||0)*3;
    return ad-bd-(av-bv)*8;
  })[0]||null;
}
function stxEWPAssembleReserveFleet(e,target){
  stxEWPEnsureState();
  const s=e.enemyWarPressure;
  if(state.simTime-s.lastReserveAt<18)return null;
  const source=stxEWPReserveSource(e,target);if(!source)return null;
  const floor=stxEWPReserveFloor(source),available=Math.max(0,(Number(source.garrison)||0)-floor);
  const desired=Math.max(7,(Number(source.garrison)||0)*(.27+(e.aggression||0)*.08));
  const strength=Math.min(16,available,desired);
  if(strength<6)return null;
  source.garrison=Math.max(floor,source.garrison-strength);
  const f=registerFleet(e.id,source,strength,"fleet");
  if(!f){source.garrison+=strength;return null}
  f.role="fleet";f.location=source.id;f.status=`Wartime expeditionary reserve at ${source.name}`;
  s.lastReserveAt=state.simTime;
  return f;
}
function stxEWPQueueEmergencyFleet(e,target){
  stxEWPEnsureState();
  const s=e.enemyWarPressure;
  if(state.simTime-s.lastCommissionAt<30)return false;
  const yard=owned(e.id).filter(p=>p.infra?.shipyard>0&&!p.underAttack).sort((a,b)=>dist(a,target)-dist(b,target)||(a.buildQueue?.length||0)-(b.buildQueue?.length||0))[0];
  if(!yard)return false;
  const queued=(yard.buildQueue||[]).some(q=>q.type==="fleet"||q.type==="patrol");
  if(queued){s.lastCommissionAt=state.simTime;return true}
  let ok=false;
  if(typeof stxQueueFleetCommission==="function")ok=!!stxQueueFleetCommission(yard,"fleet","Wartime emergency fleet commission");
  else{
    yard.buildQueue=yard.buildQueue||[];
    yard.buildQueue.unshift({type:"fleet",progress:0,startedAt:state.simTime,need:{components:38,helium:18,titanium:15,trained:.009},commissioned:true,commissionSource:"Wartime emergency fleet commission"});
    ok=true;
  }
  if(ok)s.lastCommissionAt=state.simTime;
  return ok;
}
function stxEWPPrepareFleet(e,target){
  const idle=stxEWPIdleWarFleets(e,target)[0];
  if(idle){idle.role="fleet";idle.status=`Prepared for offensive against ${target.name}`;return idle}
  const reserve=stxEWPAssembleReserveFleet(e,target);if(reserve)return reserve;
  stxEWPQueueEmergencyFleet(e,target);
  return null;
}
function stxEWPLaunchPreparedFleet(e,foeId,target,f){
  const source=state.planets.find(p=>p.id===f.location);if(!source||source.owner!==e.id)return false;
  const oldLocation=f.location,oldStatus=f.status,strength=Math.max(5,Number(f.strength)||5);
  f.location=null;f.role="fleet";f.status=`Invading ${target.name}`;
  const ship=createShip("fleet",source,target,e.id,{strength,fleetId:f.id,vesselName:f.name,warId:getWar(e.id,foeId)?.id,status:`Invading ${target.name}`,speedBoost:1+(source.orbitals?.base||0)*.22});
  if(!ship){f.location=oldLocation;f.status=oldStatus;return false}
  e.lastAttack=state.simTime;
  return true;
}
function stxEWPIncomingCount(e,foeId){
  const foeWorlds=new Set(owned(foeId).map(p=>p.id));
  return state.ships.filter(s=>s.owner===e.id&&(s.type==="fleet"||s.type==="patrol")&&foeWorlds.has(s.to)).length;
}
function stxEWPMobilizeEmpire(e){
  const wars=state.wars.filter(w=>w.active&&(w.a===e.id||w.b===e.id));if(!wars.length)return;
  const doctrine=e.doctrine==="military"?1.14:1;
  owned(e.id).forEach(p=>{
    if(p.underAttack)return;
    const militaryWorld=p.home||p.infra?.training>0||p.infra?.shipyard>0||p.infra?.defense>1;
    if(!militaryWorld)return;
    const cap=(p.home?34:16)+(p.infra?.defense||0)*2+(p.orbitals?.base||0)*6;
    if((Number(p.garrison)||0)<cap){
      const gain=(.28+(p.infra?.training||0)*.18+(p.infra?.shipyard||0)*.14+(p.infra?.defense||0)*.07)*doctrine;
      p.garrison=Math.min(cap,(Number(p.garrison)||0)+gain);
    }
    const planned=.00018+(p.infra?.training||0)*.00016+(p.infra?.shipyard||0)*.00007;
    const gearNeed=planned*1.2,gear=Math.min(Number(p.stock?.equipment)||0,gearNeed);
    const crew=planned*Math.max(.35,gear/Math.max(.000001,gearNeed))*doctrine;
    if(crew>0){
      p.stock.trained=(Number(p.stock.trained)||0)+crew;
      p.stock.equipment=Math.max(0,(Number(p.stock.equipment)||0)-gear);
    }
  });
}

/* Preserve the player's newer invasion-plan logic. Replace only AI campaigns:
   enemy fleets may cross the whole galaxy, with distance expressed as actual
   flight time rather than an arbitrary 1,750-unit eligibility wall. */
const STX_EWP_launchWarCampaign=launchWarCampaign;
launchWarCampaign=function(e,foeId){
  if(!e||e.id===0)return STX_EWP_launchWarCampaign(e,foeId);
  const war=getWar(e.id,foeId);if(!war)return false;
  const cooldown=clamp(30-(e.aggression||0)*10,21,29);
  if(state.simTime-(Number(e.lastAttack)||0)<cooldown)return false;
  const targetInfo=stxEWPChooseTarget(e,foeId);if(!targetInfo)return false;
  const maxInbound=(e.aggression||0)>.68?2:1;
  if(stxEWPIncomingCount(e,foeId)>=maxInbound)return false;
  const f=stxEWPPrepareFleet(e,targetInfo.p);if(!f)return false;
  return stxEWPLaunchPreparedFleet(e,foeId,targetInfo.p,f);
};

/* AI ticks already ask every empire in every active war to launch a campaign.
   Add measured wartime replenishment after that attempt so failed/returning
   offensives recover instead of leaving an empire permanently below threshold. */
const STX_EWP_aiTick=aiTick;
aiTick=function(){
  STX_EWP_aiTick();
  state.empires.slice(1).forEach(stxEWPMobilizeEmpire);
};
