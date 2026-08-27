/* Space Tyrants — directive action + visible impact pass.
   Loaded last so it can make mandate cards produce concrete simulation work,
   keep fleet power honest, diversify command hands, and restore rare but real
   peacetime invasion opportunities without returning to invasion spam. */

function stxDIEnsureState(){
  const s=state.directiveImpact||(state.directiveImpact={});
  if(!Number.isFinite(s.phase))s.phase=0;
  if(!Number.isFinite(s.lastPeaceInvasionOffer))s.lastPeaceInvasionOffer=0;
  if(!Array.isArray(s.recent))s.recent=[];
  if(!Array.isArray(s.recentOffers))s.recentOffers=[];
  if(!Array.isArray(s.outcomes))s.outcomes=[];
  state.empires.forEach(e=>{
    if(!Array.isArray(e.pendingDirectiveClaims))e.pendingDirectiveClaims=[];
    if(!Number.isFinite(e.pendingDirectiveFleetBuilds))e.pendingDirectiveFleetBuilds=0;
  });
  return s;
}

const STX_DI_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_DI_generateGalaxy();stxDIEnsureState()};
const STX_DI_loadGame=loadGame;
loadGame=function(){const ok=STX_DI_loadGame();if(ok)stxDIEnsureState();return ok};
stxDIEnsureState();

function stxDIStationedFleetStrength(owner){
  const moving=new Set(state.ships.filter(s=>s.owner===owner&&s.fleetId).map(s=>s.fleetId));
  const engaged=new Set();
  state.battles.forEach(b=>{
    if(b.attacker===owner)(b.attackerFleetIds||[]).forEach(id=>engaged.add(id));
    if(b.defender===owner)(b.defenderFleetIds||[]).forEach(id=>engaged.add(id));
  });
  return state.fleets.filter(f=>f.owner===owner&&!f.destroyed&&!moving.has(f.id)&&!engaged.has(f.id)).reduce((n,f)=>n+Math.max(0,Number(f.strength)||0),0);
}

/* Core fleet power counts fleets in transit and battle, but not persistent
   commissioned fleets that are sitting in orbit. Count those formations too,
   so a completed fleet visibly raises the empire's military strength. */
const STX_DI_empireFleet=empireFleet;
empireFleet=function(id){return STX_DI_empireFleet(id)+stxDIStationedFleetStrength(id)};

function stxDIExpansionSource(target){
  return playerWorlds().filter(p=>!p.underAttack&&!p.expansionProject&&p.pop>.025)
    .sort((a,b)=>dist(a,target)-dist(b,target)||b.pop-a.pop)[0]||null;
}
function stxDIStartClaim(e,target,sourceLabel="Imperial strategic-world directive"){
  if(!target||target.owner!==null)return false;
  addDirective(e,"claim",target.id,135);
  setModifier(e,"migration",1.55,105);
  const source=stxDIExpansionSource(target);
  if(source&&startExpansionProject(e,source,target,true)){
    const q=source.expansionProject;
    if(q)q.volunteers=Math.min(q.goal,Math.max(q.volunteers,q.goal*.2));
    logEvent(`ACTION STARTED: ${source.name} is recruiting and fitting a colony expedition for ${target.name}.`,"good");
    galacticNews(`COLONY EXPEDITION AUTHORIZED FOR ${target.name.toUpperCase()}`,`${source.name} has opened priority volunteer rolls and construction orders. The mission remains visible until settlers launch.`,"good",source.id);
    return true;
  }
  if(!e.pendingDirectiveClaims.includes(target.id))e.pendingDirectiveClaims.push(target.id);
  logEvent(`ACTION QUEUED: ${target.name} is reserved for settlement as soon as a Mandate world can launch the expedition.`,"warning");
  return false;
}

function stxDIFleetYards(){
  return playerWorlds().filter(p=>p.infra.shipyard>0&&!p.underAttack).sort((a,b)=>(a.buildQueue?.length||0)-(b.buildQueue?.length||0)||(b.infra.shipyard+b.infra.factory*.6)-(a.infra.shipyard+a.infra.factory*.6));
}
function stxDIShipyardCandidate(){
  return playerWorlds().filter(p=>!p.underAttack&&!p.localProject&&p.infra.shipyard<1)
    .sort((a,b)=>(b.infra.factory+b.pop*7+b.stock.components/60)-(a.infra.factory+a.pop*7+a.stock.components/60))[0]||null;
}
function stxDIQueueFleetProgram(e,count=1,source="Imperial shipbuilding directive",includePatrol=false){
  let queued=0;
  for(let i=0;i<count;i++){
    const yards=stxDIFleetYards();
    const yard=yards[i%Math.max(1,yards.length)];
    if(!yard)break;
    const type=includePatrol&&i===count-1&&count>1?"patrol":"fleet";
    if(typeof stxQueueFleetCommission==="function"&&stxQueueFleetCommission(yard,type,source))queued++;
  }
  if(queued<count){
    e.pendingDirectiveFleetBuilds=Math.min(5,(e.pendingDirectiveFleetBuilds||0)+(count-queued));
    const candidate=stxDIShipyardCandidate();
    if(candidate&&startLocalProject(candidate,"shipyard","Imperial shipbuilding directive")){
      addOrder(candidate,"shipyard","components",62,6,"Priority shipyard expansion");
      logEvent(`ACTION STARTED: ${candidate.name} began a priority shipyard project so the remaining fleet commissions can be built.`,"good");
    }
  }
  if(queued)logEvent(`ACTION STARTED: ${queued} priority ${queued===1?"military formation is":"military formations are"} now in visible shipyard queues.`,"good");
  return queued;
}

function stxDIProcessPendingActions(){
  const e=empire(0);if(!e)return;stxDIEnsureState();
  e.pendingDirectiveClaims=e.pendingDirectiveClaims.filter(id=>{
    const target=state.planets.find(p=>p.id===id);
    if(!target||target.owner!==null)return false;
    const source=stxDIExpansionSource(target);if(!source)return true;
    if(startExpansionProject(e,source,target,true)){
      source.expansionProject.volunteers=Math.min(source.expansionProject.goal,source.expansionProject.goal*.2);
      logEvent(`QUEUED ORDER ACTIVE: ${source.name} has begun the delayed settlement mission to ${target.name}.`,"good");
      return false;
    }
    return true;
  });
  if((e.pendingDirectiveFleetBuilds||0)>0){
    const yard=stxDIFleetYards()[0];
    if(yard&&typeof stxQueueFleetCommission==="function"&&stxQueueFleetCommission(yard,"fleet","Deferred imperial fleet commission")){
      e.pendingDirectiveFleetBuilds--;
      logEvent(`QUEUED ORDER ACTIVE: ${yard.name} accepted a deferred battle-fleet commission.`,"good");
    }
  }
}
const STX_DI_logisticsTick=logisticsTick;
logisticsTick=function(){STX_DI_logisticsTick();stxDIProcessPendingActions()};

function stxDIProjectSnapshot(){
  const worlds=playerWorlds(),fleets=state.fleets.filter(f=>f.owner===0&&!f.destroyed);
  return{
    worlds:worlds.length,
    fleetCount:fleets.length,
    fleetStrength:fleets.reduce((n,f)=>n+(Number(f.strength)||0),0),
    power:empireFleet(0),
    militaryQueued:worlds.reduce((n,p)=>n+(p.buildQueue||[]).filter(q=>q.type==="fleet"||q.type==="patrol").length,0),
    expansion:worlds.filter(p=>p.expansionProject).length,
    localProjects:worlds.filter(p=>p.localProject).length,
    orbitals:worlds.filter(p=>p.orbitalProject).length,
    scans:worlds.filter(p=>p.scanProject).length
  };
}
function stxDIImpactText(before,after,commands){
  const bits=[];
  const dq=after.militaryQueued-before.militaryQueued,dx=after.expansion-before.expansion,dl=after.localProjects-before.localProjects,doo=after.orbitals-before.orbitals,ds=after.scans-before.scans;
  if(dq>0)bits.push(`+${dq} military build${dq===1?"":"s"} queued`);
  if(dx>0)bits.push(`+${dx} colony mission${dx===1?"":"s"} active`);
  if(dl>0)bits.push(`+${dl} local construction project${dl===1?"":"s"}`);
  if(doo>0)bits.push(`+${doo} orbital project${doo===1?"":"s"}`);
  if(ds>0)bits.push(`+${ds} sensor project${ds===1?"":"s"}`);
  const df=after.fleetCount-before.fleetCount,dp=after.power-before.power;
  if(df>0)bits.push(`+${df} active fleet${df===1?"":"s"}`);
  if(dp>=1)bits.push(`fleet power +${Math.round(dp)}`);
  if(!bits.length){
    const concrete=commands.flatMap(c=>c.effects||[]).filter(Boolean).slice(0,3);
    if(concrete.length)bits.push(...concrete);
  }
  return bits.join(" · ")||"orders entered the implementation queue";
}

const STX_DI_issueCommands=issueCommands;
issueCommands=function(){
  const selected=[...state.commandSelected].map(i=>state.commandChoices[i]).filter(Boolean),before=stxDIProjectSnapshot();
  STX_DI_issueCommands();
  const after=stxDIProjectSnapshot(),impact=stxDIImpactText(before,after,selected),s=stxDIEnsureState();
  s.recent.push(...selected.map(c=>c.id));s.recent=s.recent.slice(-10);
  s.outcomes.unshift({time:state.simTime,titles:selected.map(c=>c.title),impact});s.outcomes=s.outcomes.slice(0,12);
  logEvent(`MANDATE IMPACT: ${impact}.`,"good");
  showToast(`Mandate impact · ${impact}`);
};

function stxDICommand(id){return COMMANDS.find(c=>c.id===id)}
const STX_DI_claim=stxDICommand("claim");
if(STX_DI_claim){
  STX_DI_claim.desc="Select a real neutral world and immediately open a colony expedition instead of only setting an expansion preference.";
  STX_DI_claim.effects=["Colony mission starts immediately","Volunteer + hull progress visible on source world","If every source is busy, the mission stays queued"];
  STX_DI_claim.apply=(e,t)=>{if(t)stxDIStartClaim(e,t,"Claim strategic world")};
}
const STX_DI_homesteads=stxDICommand("homesteads");
if(STX_DI_homesteads){
  STX_DI_homesteads.effects=["Priority colony expedition begins","Migration +110%","Volunteer recruitment accelerated"];
  STX_DI_homesteads.apply=(e,t)=>{setModifier(e,"migration",2.1,95);setModifier(e,"growth",.88,70);if(t)stxDIStartClaim(e,t,"Frontier homestead expedition")};
}
const STX_DI_shipbuilding=stxDICommand("shipbuilding");
if(STX_DI_shipbuilding){
  STX_DI_shipbuilding.desc="Place actual combat formations into shipyard queues and keep production accelerated until they launch.";
  STX_DI_shipbuilding.effects=["2 real military formations queued","Shipbuilding +85%","If no shipyard exists, a priority shipyard project begins"];
  STX_DI_shipbuilding.score=()=>stxDIFleetYards().length?78:70;
  STX_DI_shipbuilding.apply=e=>{setModifier(e,"shipbuilding",1.85,105);setModifier(e,"industry",1.2,105);addDirective(e,"fleet",null,105);stxDIQueueFleetProgram(e,2,"Galactic shipbuilding program",true)};
}
const STX_DI_fleet=stxDICommand("fleet");
if(STX_DI_fleet){
  STX_DI_fleet.desc="Mobilize the navy with an actual priority fleet commission, border reinforcement, and faster follow-on construction.";
  STX_DI_fleet.effects=["1 battle fleet queued now","Shipbuilding +55%","Threatened garrisons reinforce"];
  STX_DI_fleet.apply=e=>{setModifier(e,"shipbuilding",1.55,90);addDirective(e,"fleet",null,90);stxDIQueueFleetProgram(e,1,"Fleet mobilization",false);playerWorlds().sort((a,b)=>borderThreat(b)-borderThreat(a)).slice(0,2).forEach(p=>p.garrison+=4)};
}
const STX_DI_fleetCommission=stxDICommand("fleetCommission");
if(STX_DI_fleetCommission){
  STX_DI_fleetCommission.effects=["Named battle fleet queued immediately","Permanent admiral + fleet record","Completed fleet raises displayed fleet power"];
}

function stxDIShipyardTarget(){return stxDIShipyardCandidate()}
function stxDIRichFrontier(){return state.planets.filter(p=>p.owner===null).map(p=>({p,v:Math.max(...Object.values(p.quality))*18+Object.values(p.quality).reduce((a,b)=>a+b,0)*3-stxNearestPlayerDistance(p)/160})).sort((a,b)=>b.v-a.v)[0]?.p||bestNeutral()}

if(!stxDICommand("frontierShipyard"))COMMANDS.push({
  id:"frontierShipyard",cat:"Industry",title:"Open a Frontier Shipyard",desc:"Build permanent naval capacity on a productive Mandate world instead of relying on a temporary output bonus.",
  effects:["Real shipyard project begins","Military queues unlock on completion","Component orders receive priority"],score:()=>stxDIShipyardTarget()?76:10,target:()=>stxDIShipyardTarget(),
  apply:(e,t)=>{if(t){startLocalProject(t,"shipyard","Imperial shipyard charter");addOrder(t,"shipyard","components",62,6,"Frontier shipyard charter");e.pendingDirectiveFleetBuilds=Math.min(5,(e.pendingDirectiveFleetBuilds||0)+1);setModifier(e,"industry",1.18,90);logEvent(`ACTION STARTED: ${t.name} is constructing a new shipyard with a fleet commission waiting behind it.`,"good")}}
});
if(!stxDICommand("reserveActivation"))COMMANDS.push({
  id:"reserveActivation",cat:"Military",title:"Activate the Naval Reserve",desc:"Convert trained planetary reserves into a named orbital fleet that can be located and deployed immediately.",
  effects:["Named reserve fleet appears now","Planetary garrison supplies the crews","Fleet becomes selectable in Fleet Locator"],score:()=>playerWorlds().some(p=>p.garrison>26)?66:18,target:()=>playerWorlds().filter(p=>p.garrison>26&&!p.underAttack).sort((a,b)=>b.garrison-a.garrison)[0]||null,
  apply:(e,t)=>{if(!t)return;const strength=Math.min(18,Math.max(10,t.garrison*.28));t.garrison=Math.max(12,t.garrison-strength);const f=registerFleet(0,t,strength,"fleet");f.status=`Reserve activated at ${t.name}`;f.location=t.id;setModifier(e,"militaryMorale",1.08,70);logEvent(`ACTION COMPLETE: ${f.name} activated above ${t.name} at ${Math.round(strength)} strength.`,"good");stxRefreshFleetLocator?.()}
});
if(!stxDICommand("frontierGuard"))COMMANDS.push({
  id:"frontierGuard",cat:"Infrastructure",title:"Frontier Guard Deployment",desc:"Reinforce the most exposed Mandate worlds now while permanent defenses catch up.",
  effects:["3 threatened worlds gain garrison strength","Defense projects favored","Border readiness rises immediately"],score:()=>Math.max(0,...playerWorlds().map(borderThreat))>.18?72:38,
  apply:e=>{addDirective(e,"fortify",null,85);setModifier(e,"defense",1.22,85);playerWorlds().sort((a,b)=>borderThreat(b)-borderThreat(a)).slice(0,3).forEach((p,i)=>{p.garrison+=8-i*2;p.mandateGlow=1});logEvent("ACTION COMPLETE: Frontier garrisons received immediate reserve detachments.","good")}
});
if(!stxDICommand("strategicSurvey"))COMMANDS.push({
  id:"strategicSurvey",cat:"Research",title:"Strategic Frontier Survey",desc:"Survey a promising neutral world and prepare it for the next expansion push without committing to war.",
  effects:["Target intelligence jumps","Propulsion + survey research","Claim opportunity becomes easier to act on"],score:()=>stxDIRichFrontier()?64:0,target:()=>stxDIRichFrontier(),
  apply:(e,t)=>{if(!t)return;e.intel.add(t.id);t.intel=Math.min(1,(t.intel||0)+.7);e.tech.propulsion+=.08;setModifier(e,"migration",1.25,75);state.effects.push({type:"shock",x:t.x,y:t.y,life:1.4,maxLife:1.4,size:42,color:e.color});logEvent(`ACTION COMPLETE: ${t.name} has been fully surveyed for future settlement.`,"good")}
});
if(!stxDICommand("industrialConvoy"))COMMANDS.push({
  id:"industrialConvoy",cat:"Resources",title:"Emergency Industrial Convoy",desc:"Move construction inputs toward the Mandate world with the largest active project backlog.",
  effects:["Construction orders jump to priority 6","Logistics launches immediately","Factory/shipyard projects receive materials first"],score:()=>playerWorlds().some(p=>(p.orders?.length||0)>1||p.localProject||p.buildQueue?.length)?69:28,
  target:()=>playerWorlds().sort((a,b)=>((b.orders?.length||0)+(b.localProject?3:0)+(b.buildQueue?.length||0)*2)-((a.orders?.length||0)+(a.localProject?3:0)+(a.buildQueue?.length||0)*2))[0]||null,
  apply:(e,t)=>{if(!t)return;t.orders.forEach(o=>o.priority=Math.max(6,o.priority));setModifier(e,"tradeTraffic",1.28,65);logisticsTick();logisticsTick();logEvent(`ACTION STARTED: emergency freighters are feeding ${t.name}'s active projects.`,"good")}
});

function stxDIPeaceInvasionChoice(){
  const ranked=typeof stxInvasionCandidates==="function"?stxInvasionCandidates("balanced"):[];
  const r=ranked[0],target=r?.p||nearestEnemy();if(!target||target.owner===0||target.owner===null)return null;
  const opening=r?.opening??clamp(.45-borderThreat(playerWorlds()[0]||target)*.1,.18,.72),def=r?.def??Math.round(target.garrison+target.infra.defense*8),d=r?.d??stxNearestPlayerDistance(target);
  return{id:`peace-invasion-${target.id}-${Math.floor(state.simTime)}`,cat:"Military",title:`Authorize Invasion of ${target.name}`,
    desc:`Open a deliberate war against ${empire(target.owner).name} and make ${target.name} the first persistent invasion objective.`,
    effects:[`Starts a war by player choice`,`Estimated opening ${Math.round(opening*100)}%`,`${Math.round(def)} defense · ${Math.round(d)}u away`],targetObj:target,
    apply:(e,t)=>stxQueueInvasion(t||target,`Authorized invasion of ${(t||target).name}`,true)};
}
function stxDIChoiceBaseId(c){return String(c?.id||"").replace(/-p\d+-\d+$/,'').replace(/^peace-invasion-.+$/,'peace-invasion')}
function stxDIVaryPeaceChoices(){
  const s=stxDIEnsureState(),recent=new Set([...s.recent.slice(-7),...s.recentOffers.slice(-12)]);
  if(stxPlayerAtWar())return;
  const eligible=COMMANDS.filter(c=>!stxWarOnlyDirective(c)&&!state.commandChoices.some(x=>x.id===c.id)).map(c=>({c,score:Number(c.score?.()||0)+rand(-10,10)-(recent.has(c.id)?38:0)})).filter(x=>x.score>8).sort((a,b)=>b.score-a.score);
  for(let i=0;i<state.commandChoices.length;i++){
    const current=state.commandChoices[i];if(!recent.has(current.id))continue;
    const replacement=eligible.find(x=>x.c.cat===current.cat)||eligible[0];
    if(replacement){state.commandChoices[i]={...replacement.c,targetObj:replacement.c.target?.()||null};eligible.splice(eligible.indexOf(replacement),1)}
  }
}

/* fleet-clarity intentionally removed all peacetime invasion cards. Keep peace
   hands peaceful most of the time, but make war authorization a rare rising
   opportunity and force one before a long command drought can happen. */
const STX_DI_openCommandPhase=openCommandPhase;
openCommandPhase=function(){
  STX_DI_openCommandPhase();if($("commandModal").hidden)return;
  const s=stxDIEnsureState();s.phase++;stxDIVaryPeaceChoices();
  if(!stxPlayerAtWar()){
    const drought=s.phase-s.lastPeaceInvasionOffer,hasRival=!!nearestEnemy();
    const chance=drought<3?0:drought>=7?1:Math.min(.58,.1+(drought-3)*.12);
    if(hasRival&&random()<chance){
      const invasion=stxDIPeaceInvasionChoice();
      if(invasion){
        const replaceIndex=state.commandChoices.findIndex(c=>c.cat==="Military");
        if(replaceIndex>=0)state.commandChoices[replaceIndex]=invasion;else state.commandChoices[state.commandChoices.length-1]=invasion;
        s.lastPeaceInvasionOffer=s.phase;
      }
    }
  }
  s.recentOffers.push(...state.commandChoices.map(stxDIChoiceBaseId));s.recentOffers=s.recentOffers.slice(-16);
  state.commandSelected.clear();renderCommands();
};
