/* Space Tyrants — guaranteed directive execution.
   Loaded after directive-impact.js so the final command hand only contains
   executable orders and named orbital fleets are the forces used by invasions. */

function stxDGEnsureState(){
  const e=empire(0);
  if(e&&!Array.isArray(e.guaranteedOrders))e.guaranteedOrders=[];
  return e;
}
const STX_DG_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_DG_generateGalaxy();stxDGEnsureState()};
const STX_DG_loadGame=loadGame;
loadGame=function(){const ok=STX_DG_loadGame();if(ok)stxDGEnsureState();return ok};
stxDGEnsureState();

function stxDGActiveBattleFleetIds(){
  const ids=new Set();
  state.battles.forEach(b=>{
    (b.attackerFleetIds||[]).forEach(id=>ids.add(id));
    (b.defenderFleetIds||[]).forEach(id=>ids.add(id));
  });
  return ids;
}
function stxDGDeployableFleets(target=null){
  const engaged=stxDGActiveBattleFleetIds();
  return state.fleets.filter(f=>{
    if(f.owner!==0||f.destroyed||engaged.has(f.id)||(f.role&&f.role!=="fleet"))return false;
    if(!f.location||state.ships.some(s=>s.fleetId===f.id))return false;
    const p=state.planets.find(q=>q.id===f.location);
    return !!p&&p.owner===0&&!p.underAttack&&(Number(f.strength)||0)>4;
  }).sort((a,b)=>{
    if(target){
      const ap=state.planets.find(p=>p.id===a.location),bp=state.planets.find(p=>p.id===b.location);
      const ad=ap?dist(ap,target):Infinity,bd=bp?dist(bp,target):Infinity;
      if(ad!==bd)return ad-bd;
    }
    return (b.veterans||0)-(a.veterans||0)||(Number(b.strength)||0)-(Number(a.strength)||0);
  });
}
function stxDGHasDeployableFleet(){return stxDGDeployableFleets().length>0}
function stxDGIsInvasionChoice(c){
  const key=`${c?.id||""} ${c?.title||""}`.toLowerCase();
  return /(^|[-\s])invasion|\binvade\b/.test(key);
}
function stxDGValidEnemyTarget(p){return !!p&&p.owner!==null&&p.owner!==0&&!p.underAttack}

function stxDGOrbitalTarget(type,preferred=null){
  const e=empire(0);if(!e||!orbitalUnlocked(e,type))return null;
  const valid=p=>!!p&&p.owner===0&&!p.underAttack&&!p.orbitalProject&&(p.orbitals?.[type]||0)<2&&p.infra.shipyard>0&&p.infra.factory>0;
  if(valid(preferred))return preferred;
  const best=bestOrbitalWorld(e,type,null);if(valid(best))return best;
  return playerWorlds().filter(valid).sort((a,b)=>type==="base"?(borderThreat(b)-borderThreat(a)):(b.infra.shipyard+b.infra.factory)-(a.infra.shipyard+a.infra.factory))[0]||null;
}
function stxDGPrimeOrbitalOrders(p){
  const q=p?.orbitalProject;if(!q)return;
  Object.entries(q.need||{}).forEach(([r,a])=>{
    if((p.stock[r]||0)<a)addOrder(p,r==="trained"?"crew":"construction",r,a,6,`${q.type==="base"?"Military base":"Space station"} mandate supply`);
  });
}
function stxDGStartOrbitalOrder(e,type,preferred=null,label="Imperial construction order"){
  const target=stxDGOrbitalTarget(type,preferred);
  if(target&&queueOrbitalProject(target,type,true)){
    addDirective(e,type==="base"?"militaryBase":"orbitalStation",target.id,type==="base"?180:160);
    stxDGPrimeOrbitalOrders(target);target.mandateGlow=1;
    logEvent(`ORDER EXECUTING: ${target.name} began the mandated ${type==="base"?"sector military base":"orbital space station"}. The project is visible in its planet ledger.`,"good");
    if(typeof stxActivity==="function")stxActivity(`${target.name} began mandated ${type==="base"?"military-base":"space-station"} construction.`,target.id,null,"good");
    return true;
  }
  e.guaranteedOrders=e.guaranteedOrders||[];
  if(!e.guaranteedOrders.some(o=>o.kind==="orbital"&&o.type===type)){
    e.guaranteedOrders.push({id:`go${Math.floor(random()*1e9)}`,kind:"orbital",type,preferredId:preferred?.id||null,createdAt:state.simTime,label});
  }
  logEvent(`ORDER QUEUED: the ${type==="base"?"military-base":"space-station"} mandate is retained until an eligible Mandate world can start it.`,"warning");
  return false;
}
function stxDGProcessGuaranteedOrders(){
  const e=stxDGEnsureState();if(!e?.guaranteedOrders?.length)return;
  e.guaranteedOrders=e.guaranteedOrders.filter(o=>{
    if(o.kind!=="orbital")return true;
    const preferred=state.planets.find(p=>p.id===o.preferredId);
    const target=stxDGOrbitalTarget(o.type,preferred);
    if(!target||!queueOrbitalProject(target,o.type,true))return true;
    addDirective(e,o.type==="base"?"militaryBase":"orbitalStation",target.id,o.type==="base"?180:160);
    stxDGPrimeOrbitalOrders(target);target.mandateGlow=1;
    logEvent(`QUEUED ORDER ACTIVE: ${target.name} has started the delayed ${o.type==="base"?"sector military base":"orbital space station"}.`,"good");
    if(typeof stxActivity==="function")stxActivity(`Delayed ${o.type==="base"?"military-base":"space-station"} construction is now active at ${target.name}.`,target.id,null,"good");
    return false;
  });
}
const STX_DG_logisticsTick=logisticsTick;
logisticsTick=function(){STX_DG_logisticsTick();stxDGProcessGuaranteedOrders()};

const STX_DG_tickOrbitalProject=tickOrbitalProject;
tickOrbitalProject=function(p,dt){
  const before=p.orbitalProject,type=before?.type,count=type?(p.orbitals?.[type]||0):0;
  STX_DG_tickOrbitalProject(p,dt);
  if(p.owner===0&&before&&!p.orbitalProject&&type&&(p.orbitals?.[type]||0)>count){
    const label=type==="base"?"Sector military base":"Orbital space station";
    if(typeof stxActivity==="function")stxActivity(`${label} completed at ${p.name} and is now visibly orbiting the planet.`,p.id,null,"good");
    showToast(`${p.name} · ${label} operational`);
    stxDGProcessGuaranteedOrders();
  }
};

/* Invasions now consume the persistent named fleets the player can actually
   see in orbit. They no longer ignore those fleets and silently inspect only
   the hidden planetary garrison number. */
const STX_DG_priorMobilizeInvasionPlan=stxMobilizeInvasionPlan;
stxMobilizeInvasionPlan=function(plan,force=false){
  const target=state.planets.find(p=>p.id===plan?.targetId);
  if(!plan||!target||target.owner===0||target.owner===null){if(plan)plan.status="completed";return false}
  if(!empiresAtWar(0,target.owner)){plan.status="awaiting war";return false}
  const inbound=state.ships.filter(s=>s.owner===0&&STX_MILITARY_TYPES.has(s.type)&&s.to===target.id&&s.invasionPlanId===plan.id);
  if(target.underAttack||inbound.length){plan.status=target.underAttack?"engaged":"fleet inbound";return true}
  if(!force&&state.simTime-(plan.lastLaunchAt||-999)<18)return false;
  const fleets=stxDGDeployableFleets(target);
  if(!fleets.length){
    plan.status="awaiting commissioned fleet";
    if(state.simTime-(plan.lastFleetCommissionAt||-999)>24&&typeof stxDIQueueFleetProgram==="function"){
      plan.lastFleetCommissionAt=state.simTime;
      stxDIQueueFleetProgram(empire(0),1,`Invasion force for ${target.name}`,false);
      logEvent(`INVASION PREPARATION: ${target.name} remains the objective while the Admiralty assembles a deployable battle fleet.`,"warning");
    }
    return false;
  }
  const count=Math.min(force?2:1,fleets.length);let launched=0;const origins=[];
  for(const f of fleets.slice(0,count)){
    const source=state.planets.find(p=>p.id===f.location);if(!source)continue;
    const oldStatus=f.status,oldLocation=f.location,strength=Math.max(5,Number(f.strength)||5);
    f.location=null;f.status=`Invading ${target.name}`;
    const ship=createShip("fleet",source,target,0,{strength,fleetId:f.id,vesselName:f.name,warId:getWar(0,target.owner)?.id,invasionPlanId:plan.id,speedBoost:1.08+(source.orbitals?.base||0)*.38});
    if(!ship){f.location=oldLocation;f.status=oldStatus;continue}
    launched++;origins.push(source.name);
  }
  if(!launched){plan.status="launch blocked";return false}
  plan.lastLaunchAt=state.simTime;plan.status="fleet inbound";
  logEvent(`INVASION LAUNCHED: ${launched} named fleet${launched===1?"":"s"} broke orbit${origins.length?` from ${[...new Set(origins)].join(", ")}`:""} and are en route to ${target.name}.`,"warning");
  if(typeof stxActivity==="function")stxActivity(`${launched} named fleet${launched===1?"":"s"} launched for the invasion of ${target.name}.`,target.id,null,"warning");
  if(typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator();
  return true;
};

const STX_DG_launchWarCampaign=launchWarCampaign;
launchWarCampaign=function(e,foeId){
  if(e?.id!==0)return STX_DG_launchWarCampaign(e,foeId);
  const plan=(e.invasionPlans||[]).find(pl=>!["completed","cancelled"].includes(pl.status)&&state.planets.find(p=>p.id===pl.targetId)?.owner===foeId);
  if(plan)return stxMobilizeInvasionPlan(plan,false);
  return STX_DG_launchWarCampaign(e,foeId);
};

function stxDGCanExecuteChoice(c){
  if(!c)return false;
  if(typeof c.target==="function"&&!c.targetObj)return false;
  if(stxDGIsInvasionChoice(c))return stxDGHasDeployableFleet()&&stxDGValidEnemyTarget(c.targetObj||nearestEnemy());
  switch(c.id){
    case "spaceStation":return !!stxDGOrbitalTarget("station",c.targetObj);
    case "sectorBase":return !!stxDGOrbitalTarget("base",c.targetObj);
    case "fleetCommission":return !!(c.targetObj||stxBestFleetYard?.(0));
    case "reserveActivation":return !!c.targetObj;
    case "frontierShipyard":return !!c.targetObj;
    case "mineWorks":return typeof stxMineTargets==="function"&&stxMineTargets().length>0;
    case "factoryWave":return typeof stxFactoryTargets==="function"&&stxFactoryTargets().length>0;
    case "sensorArray":return !!c.targetObj;
    case "patrolNetwork":return playerWorlds().some(p=>p.infra.shipyard>0||(p.orbitals?.station||0)>0||(p.orbitals?.base||0)>0);
    default:return true;
  }
}
function stxDGChoiceFromCommand(c){
  if(!c)return null;const targetObj=typeof c.target==="function"?(c.target()||null):(c.targetObj||null);
  return {...c,targetObj};
}
function stxDGAssembleFleetChoice(){
  const base=COMMANDS.find(c=>c.id==="fleet")||COMMANDS.find(c=>c.id==="shipbuilding");if(!base)return null;
  return {...base,id:"assembleFleet",cat:"Military",title:"Assemble Battle Fleets",desc:"No deployable battle fleet is available. Commission a permanent named formation before attempting an invasion.",effects:["A real battle-fleet commission starts","If no shipyard is ready, shipyard construction is queued","The completed fleet will appear in orbit and the Fleet Locator"],targetObj:null};
}
function stxDGRebuildCommandHand(){
  let choices=state.commandChoices.filter(stxDGCanExecuteChoice);
  const noFleet=!stxDGHasDeployableFleet();
  if(noFleet){
    choices=choices.filter(c=>!stxDGIsInvasionChoice(c));
    const hasAssembly=choices.some(c=>["assembleFleet","fleet","shipbuilding","fleetCommission"].includes(c.id));
    if(!hasAssembly){
      const assemble=stxDGAssembleFleetChoice(),military=choices.findIndex(c=>c.cat==="Military");
      if(assemble){if(military>=0)choices[military]=assemble;else choices.push(assemble)}
    }
  }
  const ids=new Set(choices.map(c=>c.id)),cats=new Set(choices.map(c=>c.cat));
  const pool=COMMANDS.map(c=>{
    const choice=stxDGChoiceFromCommand(c);let score=0;
    try{score=Number(c.score?.()||0)+rand(-8,8)}catch(_){score=0}
    return{choice,score};
  }).filter(x=>x.choice&&!ids.has(x.choice.id)&&x.score>5&&stxDGCanExecuteChoice(x.choice)).sort((a,b)=>b.score-a.score);
  for(const entry of pool){
    if(choices.length>=4)break;
    if(cats.has(entry.choice.cat))continue;
    choices.push(entry.choice);ids.add(entry.choice.id);cats.add(entry.choice.cat);
  }
  if(noFleet&&!choices.some(c=>["assembleFleet","fleet","shipbuilding","fleetCommission"].includes(c.id))){
    const assemble=stxDGAssembleFleetChoice();if(assemble){const idx=choices.findIndex(c=>c.cat==="Military");if(idx>=0)choices[idx]=assemble;else if(choices.length<4)choices.push(assemble)}
  }
  state.commandChoices=choices.slice(0,4);state.commandSelected.clear();renderCommands();
}

const STX_DG_spaceStation=COMMANDS.find(c=>c.id==="spaceStation");
if(STX_DG_spaceStation){
  STX_DG_spaceStation.desc="Start a real, visible orbital-space-station construction project on an eligible developed Mandate world.";
  STX_DG_spaceStation.effects=["Construction project starts immediately","Supply orders are priority 6","Completed station visibly orbits the planet"];
  STX_DG_spaceStation.target=()=>stxDGOrbitalTarget("station");
  STX_DG_spaceStation.score=()=>stxDGOrbitalTarget("station")?86:0;
  STX_DG_spaceStation.apply=(e,t)=>stxDGStartOrbitalOrder(e,"station",t,"Orbital space station mandate");
}
const STX_DG_sectorBase=COMMANDS.find(c=>c.id==="sectorBase");
if(STX_DG_sectorBase){
  STX_DG_sectorBase.target=()=>stxDGOrbitalTarget("base");
  STX_DG_sectorBase.score=()=>stxDGOrbitalTarget("base")?88:0;
  STX_DG_sectorBase.apply=(e,t)=>stxDGStartOrbitalOrder(e,"base",t,"Sector military base mandate");
}

const STX_DG_openCommandPhase=openCommandPhase;
openCommandPhase=function(){
  STX_DG_openCommandPhase();if($("commandModal").hidden)return;
  stxDGRebuildCommandHand();
};
