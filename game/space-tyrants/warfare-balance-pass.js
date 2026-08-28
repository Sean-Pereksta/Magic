/* Space Tyrants — invasion reliability + strategic military balance pass.
   Loaded after fleet-command-allocation.js so unresolved fleet orders are valid
   mandate outcomes, invasion targeting is explicit, and rival navies scale with
   real empire development instead of only emergency wartime reserves. */

const STX_WB_VERSION=1;
const STX_WB_AI_REARM_MIN=28;
const STX_WB_AI_REARM_MAX=58;
const STX_WB_AI_REFIT_COOLDOWN=105;
const STX_WB_AUDIT_INTERVAL=8;
let stxWBManualMode=false;
let stxWBManualFleetIds=new Set();

function stxWBEnsureState(){
  const s=state.stxWarfareBalance||(state.stxWarfareBalance={version:STX_WB_VERSION,audit:[],lastAuditAt:-999,lastBreakdowns:{}});
  s.version=STX_WB_VERSION;
  s.audit=Array.isArray(s.audit)?s.audit:[];
  s.lastBreakdowns=s.lastBreakdowns||{};
  state.empires.forEach(e=>{
    const n=e.enemyStrategicNavy||(e.enemyStrategicNavy={});
    if(!Number.isFinite(n.nextReviewAt))n.nextReviewAt=state.simTime+rand(18,36);
    if(!Number.isFinite(n.nextCommissionAt))n.nextCommissionAt=state.simTime+rand(18,42);
    if(!Number.isFinite(n.nextShipyardAt))n.nextShipyardAt=state.simTime+rand(90,150);
    if(!Number.isFinite(n.nextRefitAt))n.nextRefitAt=state.simTime+rand(65,115);
    if(!Number.isFinite(n.lastPower))n.lastPower=0;
    n.trend=n.trend||"Stable";
  });
  return s;
}

function stxWBActiveFleetRecords(owner){return state.fleets.filter(f=>f.owner===owner&&!f.destroyed)}
function stxWBMovingFleetStrength(owner){return state.ships.filter(s=>s.owner===owner&&(s.type==="fleet"||s.type==="patrol")).reduce((n,s)=>n+(Number(s.strength)||0),0)}
function stxWBStationedFleetStrength(owner){
  return stxWBActiveFleetRecords(owner).filter(f=>f.location&&!state.ships.some(s=>s.fleetId===f.id)).reduce((n,f)=>n+(Number(f.strength)||0),0);
}
function stxWBBattleFleetStrength(owner){
  return state.battles.reduce((n,b)=>n+(b.attacker===owner?(Number(b.attackerStrength)||0):0)+(b.defender===owner?(Number(b.defenderStrength)||0):0),0);
}
function stxWBGarrisonPower(owner){return owned(owner).reduce((n,p)=>n+(Number(p.garrison)||0)*.18,0)}
function stxWBOrbitalPower(owner){return owned(owner).reduce((n,p)=>n+(p.orbitals?.station||0)*3+(p.orbitals?.base||0)*8,0)}
function stxWBPowerBreakdown(owner){
  const moving=stxWBMovingFleetStrength(owner),stationed=stxWBStationedFleetStrength(owner),battle=stxWBBattleFleetStrength(owner),garrison=stxWBGarrisonPower(owner),orbital=stxWBOrbitalPower(owner);
  const deployable=moving+stationed+battle,total=deployable+garrison+orbital;
  const fleets=stxWBActiveFleetRecords(owner),strongest=Math.max(0,...fleets.map(f=>Number(f.strength)||0),...state.ships.filter(s=>s.owner===owner&&(s.type==="fleet"||s.type==="patrol")).map(s=>Number(s.strength)||0));
  return{moving,stationed,battle,garrison,orbital,deployable,total,fleetCount:fleets.length,strongest,shipyards:owned(owner).filter(p=>(p.infra?.shipyard||0)>0).length};
}
function stxWBStrongestRivalPower(owner=0){return Math.max(0,...state.empires.filter(e=>e.id!==owner&&owned(e.id).length).map(e=>stxWBPowerBreakdown(e.id).total))}
function stxWBDominance(owner=0){const own=stxWBPowerBreakdown(owner).total,rival=stxWBStrongestRivalPower(owner);return rival>1?own/rival:1}
function stxWBAtWar(owner){return state.wars.some(w=>w.active&&(w.a===owner||w.b===owner))}
function stxWBQueuedMilitary(owner){return owned(owner).reduce((n,p)=>n+(p.buildQueue||[]).filter(q=>q.type==="fleet"||q.type==="patrol").length,0)}
function stxWBPolicy(e){return e.foreignPolicy||{aggression:Number(e.aggression)||.45,expansionism:.45,caution:.5}}
function stxWBDesiredFleetCount(e){
  const worlds=owned(e.id).length;if(!worlds)return 0;const p=stxWBPolicy(e),wars=state.wars.filter(w=>w.active&&(w.a===e.id||w.b===e.id)).length;
  const doctrine=e.doctrine==="military"?1:e.doctrine==="fortress"?0:-0;
  const personality=(p.aggression>=.66?1:0)+(p.expansionism>=.7?1:0)-(p.caution>=.78?1:0);
  let target=Math.ceil(worlds/2.5)+wars+doctrine+personality;
  const playerPower=stxWBPowerBreakdown(0).total,own=stxWBPowerBreakdown(e.id).total;
  if(e.id!==0&&playerPower>Math.max(28,own*1.45))target++;
  if(e.id!==0&&playerPower>Math.max(52,own*1.9)&&(p.aggression>.48||p.expansionism>.58))target++;
  return clamp(target,1,Math.max(2,Math.ceil(worlds*.72)));
}
function stxWBStrategicPowerFloor(e){
  const worlds=owned(e.id).length,p=stxWBPolicy(e),yards=owned(e.id).filter(x=>(x.infra?.shipyard||0)>0).length;
  return worlds*(2.8+(e.doctrine==="military"?.85:0))+yards*5+(p.aggression||.45)*10+(p.expansionism||.45)*6;
}

/* A pending target/fleet allocation is concrete work. The older mandate
   guarantee must never translate it into an accidental fleet commission. */
if(typeof stxMESnapshot==="function"){
  const STX_WB_stxMESnapshot=stxMESnapshot;
  stxMESnapshot=function(){const snap=STX_WB_stxMESnapshot();snap.fleetAllocationPending=Array.isArray(stxFCAQueue)?stxFCAQueue.length:0;return snap};
}
if(typeof stxMEFallbackFor==="function"){
  const STX_WB_stxMEFallbackFor=stxMEFallbackFor;
  stxMEFallbackFor=function(c){
    if(c?.stxFleetOrderKind&&Array.isArray(stxFCAQueue)&&stxFCAQueue.some(o=>o.kind===c.stxFleetOrderKind))return false;
    return STX_WB_stxMEFallbackFor(c);
  };
}
const STX_WB_issueCommands=issueCommands;
issueCommands=function(){
  STX_WB_issueCommands();
  if(Array.isArray(stxFCAQueue)&&stxFCAQueue.length&&!stxFCAActive)stxFCAStart();
};

/* Never offer a wartime invasion card unless there is both a real target and a
   fleet that can be allocated. Preserve peaceful military choices otherwise. */
if(typeof stxFCAPostHand==="function"){
  const STX_WB_stxFCAPostHand=stxFCAPostHand;
  stxFCAPostHand=function(){
    STX_WB_stxFCAPostHand();
    const wars=stxFCAWars(),targets=stxFCATargets("invade"),free=stxFCAFree().length;
    let changed=false;
    state.commandChoices=state.commandChoices.map(c=>{
      if(c?.stxFleetOrderKind!=="invade")return c;
      if(wars.length&&targets.length&&free)return c;
      changed=true;
      if(wars.length&&!free){
        return{id:`wb-invasion-readiness-${Math.floor(state.simTime)}`,cat:"Military",title:"Restore Invasion Readiness",desc:"No battle fleet is currently free to attack. Reorganize existing forces and reserves before another invasion order is offered.",effects:["Regroup idle formations first","Planetary reserves only if necessary","No phantom invasion order"],apply:e=>{if(typeof stxIFAAssembleActiveFleets==="function"&&stxIFAAssembleActiveFleets("Restore invasion readiness")>0)return true;addDirective(e,"fleet",null,48);setModifier(e,"militaryMorale",1.05,42);return true}};
      }
      return stxFCAChoice("concentrate");
    });
    if(changed){state.commandSelected.clear();renderCommands()}
  };
}

function stxWBTargetDefense(p){
  if(typeof stxEWPTargetDefense==="function")return stxEWPTargetDefense(p);
  return (Number(p?.garrison)||0)+(p?.infra?.defense||0)*8+(p?.orbitals?.station||0)*9+(p?.orbitals?.base||0)*22;
}
function stxWBSelectedInvasionFleets(target,count){
  const free=target?stxFCAFree(target):[];
  if(stxWBManualMode&&stxWBManualFleetIds.size)return free.filter(f=>stxWBManualFleetIds.has(f.id));
  return free.slice(0,Math.max(1,count||1));
}
function stxWBAssessment(attack,defense){const ratio=attack/Math.max(1,defense);return ratio>=1.55?"Favorable":ratio>=1.05?"Contested":"Risky"}

function stxWBInstallAllocationUi(){
  const box=$("stxFCAOrderModal")?.querySelector(".stx-fca-box");if(!box||$("stxWBAllocation"))return;
  const block=document.createElement("div");block.id="stxWBAllocation";block.hidden=true;block.innerHTML=`<div class="stx-wb-mode-row"><button type="button" class="choice-btn stx-wb-mode active" data-wb-mode="recommended">Recommended Force</button><button type="button" class="choice-btn stx-wb-mode" data-wb-mode="manual">Manual Allocation</button></div><div id="stxWBIntel" class="stx-wb-intel"></div><div id="stxWBManualList" class="stx-wb-fleet-list" hidden></div>`;
  const footer=box.querySelector(".command-footer");box.insertBefore(block,footer);
  block.querySelectorAll("[data-wb-mode]").forEach(b=>b.onclick=()=>{
    stxWBManualMode=b.dataset.wbMode==="manual";stxWBManualFleetIds.clear();
    block.querySelectorAll("[data-wb-mode]").forEach(x=>x.classList.toggle("active",(x.dataset.wbMode==="manual")===stxWBManualMode));
    $("stxWBManualList").hidden=!stxWBManualMode;stxWBRenderManualFleets();stxFCASyncUi();
  });
  const style=document.createElement("style");style.id="stxWBStyles";style.textContent=`
  #stxWBAllocation{margin-top:12px;padding-top:11px;border-top:1px solid rgba(120,153,224,.14)}.stx-wb-mode-row{display:flex;gap:7px;flex-wrap:wrap;margin-bottom:9px}.stx-wb-mode{padding:7px 10px;font-size:.68rem}.stx-wb-mode.active{border-color:rgba(78,231,255,.55);background:rgba(78,231,255,.12)}.stx-wb-intel{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin:8px 0}.stx-wb-intel span,.stx-wb-fleet{padding:8px;border-radius:9px;background:rgba(80,108,169,.09);border:1px solid rgba(114,150,225,.1);font-size:.65rem}.stx-wb-intel label{display:block;color:#8298bd;font-size:.54rem;text-transform:uppercase;letter-spacing:.08em}.stx-wb-intel b{display:block;margin-top:2px}.stx-wb-fleet-list{display:grid;gap:5px;max-height:180px;overflow:auto;margin-top:8px}.stx-wb-fleet{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:8px;cursor:pointer}.stx-wb-fleet small{display:block;color:#8398b8}.stx-wb-fleet input{width:auto!important;margin:0}.stx-wb-military-breakdown{display:grid;grid-template-columns:repeat(3,1fr);gap:5px;margin:7px 0;padding:7px;border-radius:8px;background:rgba(76,126,190,.055);border:1px solid rgba(111,156,255,.1)}.stx-wb-military-breakdown span{font:700 8px/1.25 system-ui;color:#9eb1c7}.stx-wb-military-breakdown b{display:block;color:#edf6ff;font-size:9px}.stx-wb-own-power{margin-top:9px;padding:9px;border-radius:9px;background:rgba(78,231,255,.055);border:1px solid rgba(78,231,255,.14);font:700 9px system-ui;color:#9fb3cb}.stx-wb-own-power b{color:#eef8ff}@media(max-width:650px){.stx-wb-intel,.stx-wb-military-breakdown{grid-template-columns:1fr 1fr}}
  `;document.head.appendChild(style);
}
function stxWBRenderManualFleets(){
  const list=$("stxWBManualList"),order=stxFCAQueue?.[0],target=state.planets.find(p=>p.id===$("stxFCATarget")?.value);if(!list)return;
  if(!stxWBManualMode||order?.kind!=="invade"||!target){list.innerHTML="";return}
  const fleets=stxFCAFree(target);list.innerHTML=fleets.map(f=>{const p=state.planets.find(x=>x.id===f.location),checked=stxWBManualFleetIds.has(f.id)?"checked":"";return`<label class="stx-wb-fleet"><input type="checkbox" data-wb-fleet="${f.id}" ${checked}><span><b>${f.name}</b><small>${p?.name||"Unknown"} · strength ${Math.round(f.strength||0)}</small></span><span>${p?Math.round(dist(p,target)):"?"}u</span></label>`}).join("");
  list.querySelectorAll("[data-wb-fleet]").forEach(i=>i.onchange=()=>{i.checked?stxWBManualFleetIds.add(i.dataset.wbFleet):stxWBManualFleetIds.delete(i.dataset.wbFleet);stxFCASyncUi()});
}
stxWBInstallAllocationUi();

if(typeof stxFCAShow==="function"){
  const STX_WB_stxFCAShow=stxFCAShow;
  stxFCAShow=function(){
    stxWBManualMode=false;stxWBManualFleetIds.clear();STX_WB_stxFCAShow();stxWBInstallAllocationUi();
    const o=stxFCAQueue?.[0],block=$("stxWBAllocation");if(!block)return;block.hidden=o?.kind!=="invade";
    block.querySelectorAll("[data-wb-mode]").forEach(x=>x.classList.toggle("active",x.dataset.wbMode==="recommended"));
    if(o?.kind==="invade"){
      const targets=stxFCATargets("invade"),sel=$("stxFCATarget");
      sel.innerHTML=`<option value="">Choose enemy planet...</option>`+targets.map(x=>`<option value="${x.id}">${x.name} · ${empire(x.owner)?.name||"Enemy"} · defense ${Math.round(stxWBTargetDefense(x))} · distance ${Math.round(typeof stxNearestPlayerDistance==="function"?stxNearestPlayerDistance(x):0)}</option>`).join("");
      $("stxFCATitle").textContent="Choose the Planet to Invade";$("stxFCADesc").textContent="This mandate is not complete until you select an enemy world, allocate real fleets, review the force comparison, and confirm launch.";
      stxFCASyncUi();
    }
  };
}
if(typeof stxFCASyncUi==="function"){
  const STX_WB_stxFCASyncUi=stxFCASyncUi;
  stxFCASyncUi=function(){
    STX_WB_stxFCASyncUi();
    const o=stxFCAQueue?.[0],intel=$("stxWBIntel");if(o?.kind!=="invade"||!intel)return;
    const target=state.planets.find(p=>p.id===$("stxFCATarget")?.value),count=Math.max(1,Number($("stxFCACount")?.value)||1);stxWBRenderManualFleets();
    if(!target){intel.innerHTML=`<span><label>Target</label><b>Choose a planet</b></span><span><label>Fleet allocation</label><b>${stxFCAFree().length} available</b></span><span><label>Status</label><b>Awaiting order</b></span>`;$("stxFCAConfirm").disabled=true;return}
    const selected=stxWBSelectedInvasionFleets(target,count),attack=selected.reduce((n,f)=>n+(Number(f.strength)||0),0),defense=stxWBTargetDefense(target),assessment=stxWBAssessment(attack,defense);
    intel.innerHTML=`<span><label>Attack strength</label><b>${Math.round(attack)} · ${selected.length} fleet${selected.length===1?"":"s"}</b></span><span><label>Known defense</label><b>${Math.round(defense)}</b></span><span><label>Assessment</label><b>${assessment}</b></span>`;
    $("stxFCAPreview").textContent=selected.length?`${selected.map(f=>`${f.name} (${Math.round(f.strength||0)})`).join(" · ")} · ${assessment} against ${target.name}.`:"Select at least one fleet to continue.";
    $("stxFCAConfirm").disabled=!selected.length;
  };
}

if(typeof stxFCAExecuteInvade==="function"){
  const STX_WB_stxFCAExecuteInvade=stxFCAExecuteInvade;
  stxFCAExecuteInvade=function(target,count){
    if(!stxWBManualMode)return STX_WB_stxFCAExecuteInvade(target,count);
    const selected=stxWBSelectedInvasionFleets(target,count);if(!target||!selected.length||typeof stxQueueInvasion!=="function")return 0;
    if(!stxQueueInvasion(target,`Manual invasion of ${target.name}`,false))return 0;
    const plan=(empire(0).invasionPlans||[]).find(p=>p.targetId===target.id),war=getWar(0,target.owner);let launched=0,ids=[];
    for(const f of selected){
      if(!stxFCAFreeFleet(f))continue;const source=state.planets.find(p=>p.id===f.location);if(!source)continue;stxFCAClear(f);const old={location:f.location,status:f.status,readiness:f.readiness};f.location=null;f.readiness="inbound";f.status=`Invading ${target.name}`;
      const ship=createShip("fleet",source,target,0,{strength:Math.max(5,f.strength||5),fleetId:f.id,vesselName:f.name,warId:war?.id,invasionPlanId:plan?.id,stxManualFleetOrder:true,speedBoost:1.08+(source.orbitals?.base||0)*.38});
      if(!ship){Object.assign(f,old);continue}stxFCACommitted.add(f.id);launched++;ids.push(f.id);
    }
    if(plan&&launched){plan.stxManualAllocation=true;plan.stxRequestedFleetCount=launched;plan.stxAssignedFleetIds=ids;plan.stxManualLaunchedAt=state.simTime;plan.lastLaunchAt=state.simTime;plan.status="fleet inbound"}
    if(launched){stxFCAUsedTargets.add(target.id);logEvent(`INVASION LAUNCHED: ${launched} manually selected fleet${launched===1?"":"s"} committed to ${target.name}.`,"warning");if(typeof stxActivity==="function")stxActivity(`${launched} manually selected fleet${launched===1?"":"s"} launched for ${target.name}.`,target.id,null,"warning");if(typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator()}
    return launched;
  };
}

/* Player modernization now has diminishing returns and a tech/yard ceiling.
   Veteran fleets remain meaningful without compounding 24% forever. */
function stxWBPlayerRefitTarget(f,p){
  const s=Math.max(5,Number(f.strength)||5),weapons=Number(empire(0).tech?.weapons)||0,yard=p?.infra?.shipyard||0,base=p?.orbitals?.base||0;
  const cap=26+weapons*10+yard*3+base*4;
  const gain=Math.max(2,Math.min(6,Math.round(5.5+weapons*.7+yard*.35-s*.065)));
  return Math.max(s+2,Math.min(cap,s+gain));
}
if(typeof stxFCAStartRefit==="function"){
  const STX_WB_stxFCAStartRefit=stxFCAStartRefit;
  stxFCAStartRefit=function(f){
    const p=f&&state.planets.find(x=>x.id===f.location),ok=STX_WB_stxFCAStartRefit(f);if(!ok||!f?.stxRefit)return ok;
    const target=Math.ceil(stxWBPlayerRefitTarget(f,p)),scale=1+Math.max(0,(Number(f.strength)||0)-18)*.035;
    f.stxRefit.targetStrength=target;Object.keys(f.stxRefit.need).forEach(r=>f.stxRefit.need[r]=Math.ceil(f.stxRefit.need[r]*scale));
    return ok;
  };
}
if(typeof stxFCAUpgradeCandidates==="function"){
  const STX_WB_stxFCAUpgradeCandidates=stxFCAUpgradeCandidates;
  stxFCAUpgradeCandidates=function(){return STX_WB_stxFCAUpgradeCandidates().filter(f=>{const p=state.planets.find(x=>x.id===f.location);return stxWBPlayerRefitTarget(f,p)>(Number(f.strength)||0)+.5})};
}

/* When the Mandate is already far ahead, stop stacking additional fleet-build
   prompts. Existing forces, readiness and logistics remain available. */
if(typeof stxFleetNeed==="function"){
  const STX_WB_stxFleetNeed=stxFleetNeed;
  stxFleetNeed=function(owner){
    const base=STX_WB_stxFleetNeed(owner);if(owner!==0)return base;
    const dominance=stxWBDominance(0),wars=stxWBAtWar(0);if(!wars&&dominance>=1.55)return Math.min(0,base);if(dominance>=2.05)return 0;return base;
  };
}
function stxWBThrottlePlayerMilitaryCards(){
  const ids=new Set(["shipbuilding","fleet","fleetCommission","reserveActivation"]);
  COMMANDS.filter(c=>ids.has(c.id)&&!c.stxWBThrottleWrapped).forEach(c=>{const oldScore=c.score;c.score=function(){const raw=Number(oldScore?.call(this))||0,dom=stxWBDominance(0),war=stxWBAtWar(0);if(!war&&dom>=1.65)return Math.min(raw,4);if(dom>=2.15)return Math.min(raw,3);if(dom>=1.45)return raw*(war?.55:.32);return raw};c.stxWBThrottleWrapped=true});
}
stxWBThrottlePlayerMilitaryCards();
const STX_WB_openCommandPhase=openCommandPhase;
openCommandPhase=function(){stxWBThrottlePlayerMilitaryCards();return STX_WB_openCommandPhase()};

/* Rival peacetime naval planning uses real shipyard queues and real local
   resources. Falling behind accelerates investment but never spawns free power. */
function stxWBBestAIYard(e){return owned(e.id).filter(p=>(p.infra?.shipyard||0)>0&&!p.underAttack).sort((a,b)=>(a.buildQueue?.length||0)-(b.buildQueue?.length||0)||(b.infra.shipyard+b.infra.factory*.6)-(a.infra.shipyard+a.infra.factory*.6))[0]||null}
function stxWBMaybeBuildAIShipyard(e){
  const n=e.enemyStrategicNavy;if(state.simTime<n.nextShipyardAt||stxWBBestAIYard(e))return false;
  const worlds=owned(e.id);if(worlds.length<3)return false;const p=worlds.filter(x=>!x.localProject&&!x.underAttack).sort((a,b)=>(b.infra?.factory||0)+(b.pop||0)*6-((a.infra?.factory||0)+(a.pop||0)*6))[0];if(!p)return false;
  const ok=startLocalProject(p,"shipyard","Strategic naval development");if(ok){addOrder(p,"shipyard","components",62,5,"Strategic naval development");n.nextShipyardAt=state.simTime+rand(125,190)}return ok;
}
function stxWBMaybeCommissionAI(e){
  const n=e.enemyStrategicNavy;if(state.simTime<n.nextCommissionAt)return false;
  const desired=stxWBDesiredFleetCount(e),active=stxWBActiveFleetRecords(e.id).length,queued=stxWBQueuedMilitary(e.id),power=stxWBPowerBreakdown(e.id).total,floor=stxWBStrategicPowerFloor(e);
  if(active+queued>=desired&&power>=floor)return false;
  const yard=stxWBBestAIYard(e);if(!yard){stxWBMaybeBuildAIShipyard(e);n.nextCommissionAt=state.simTime+32;return false}
  if((yard.buildQueue||[]).filter(q=>q.type==="fleet"||q.type==="patrol").length>=1){n.nextCommissionAt=state.simTime+18;return false}
  const player=stxWBPowerBreakdown(0).total,ratio=player/Math.max(8,power),p=stxWBPolicy(e),urgent=ratio>=1.55||stxWBAtWar(e.id),type=!urgent&&p.aggression<.48&&random()<.32?"patrol":"fleet";
  const ok=typeof stxQueueFleetCommission==="function"&&stxQueueFleetCommission(yard,type,urgent?"Strategic naval catch-up":"Peacetime naval program");
  const pressure=ratio>=1.9?-.28:ratio>=1.45?-.14:0,base=clamp(rand(STX_WB_AI_REARM_MIN,STX_WB_AI_REARM_MAX)+(1-(p.aggression||.45))*16,22,72);n.nextCommissionAt=state.simTime+base*(1+pressure);return !!ok;
}
function stxWBAIRefitCandidate(e){
  return stxWBActiveFleetRecords(e.id).filter(f=>f.location&&!state.ships.some(s=>s.fleetId===f.id)).map(f=>({f,p:state.planets.find(x=>x.id===f.location)})).filter(x=>x.p?.owner===e.id&&(x.p.infra?.shipyard||0)>0&&!x.p.underAttack).sort((a,b)=>(a.f.strength||0)-(b.f.strength||0))[0]||null;
}
function stxWBMaybeRefitAI(e){
  const n=e.enemyStrategicNavy;if(state.simTime<n.nextRefitAt)return false;const c=stxWBAIRefitCandidate(e);if(!c){n.nextRefitAt=state.simTime+45;return false}
  const {f,p}=c,s=Math.max(5,Number(f.strength)||5),weapons=Number(e.tech?.weapons)||0,cap=25+weapons*10+(p.infra?.shipyard||0)*3+(p.orbitals?.base||0)*4;if(s>=cap-.5){n.nextRefitAt=state.simTime+STX_WB_AI_REFIT_COOLDOWN;return false}
  const need={components:Math.ceil(12+s*.65),equipment:Math.ceil(8+s*.42),titanium:Math.ceil(7+s*.34)};
  const ready=Object.entries(need).every(([r,a])=>(Number(p.stock?.[r])||0)>=a);if(!ready){Object.entries(need).forEach(([r,a])=>{if((p.stock?.[r]||0)<a)addOrder(p,"aiFleetRefit",r,a,4,`${f.name} modernization`)});n.nextRefitAt=state.simTime+36;return false}
  Object.entries(need).forEach(([r,a])=>consume(p,r,a));const gain=Math.max(2,Math.min(6,Math.round(4.5+weapons*.8+(p.infra?.shipyard||0)*.35-s*.055))),old=Math.round(s);f.strength=Math.min(cap,s+gain);f.maxServiceStrength=Math.max(f.maxServiceStrength||0,f.strength);f.status=`Modernized at ${p.name}`;f.lastUpgradeAt=state.simTime;n.nextRefitAt=state.simTime+STX_WB_AI_REFIT_COOLDOWN+rand(-18,28);if(e.id===0||state.empires[0]?.intel?.has(p.id))logEvent(`${e.name} modernized ${f.name}: ${old} → ${Math.round(f.strength)} strength.`,"warning");return true;
}
function stxWBReviewAINavy(e){
  if(!e||e.id===0||!owned(e.id).length)return;const n=e.enemyStrategicNavy;if(state.simTime<n.nextReviewAt)return;n.nextReviewAt=state.simTime+rand(15,24);const before=n.lastPower||0,current=stxWBPowerBreakdown(e.id).total;n.trend=current>before+2?"Rising":current<before-2?"Falling":"Stable";n.lastPower=current;stxWBMaybeCommissionAI(e);stxWBMaybeRefitAI(e);
}
const STX_WB_aiTick=aiTick;
aiTick=function(){STX_WB_aiTick();stxWBEnsureState();state.empires.slice(1).forEach(stxWBReviewAINavy);stxWBAuditMilitary()};

/* Aggressive rivals defend immediate threats, then resume counteroffensives
   sooner instead of repeatedly refreshing a long passive hold. */
const STX_WB_launchWarCampaign=launchWarCampaign;
launchWarCampaign=function(e,foeId){
  if(e&&e.id!==0&&e.enemyWarPressure&&typeof stxDECThreatenedWorlds==="function"){
    const threats=stxDECThreatenedWorlds(e,foeId),p=stxWBPolicy(e);if(!threats.length&&(p.aggression>=.62||e.doctrine==="military")&&state.simTime<(Number(e.enemyWarPressure.defenseHoldUntil)||0)-7)e.enemyWarPressure.defenseHoldUntil=state.simTime+7;
  }
  return STX_WB_launchWarCampaign(e,foeId);
};

function stxWBAuditMilitary(){
  const s=stxWBEnsureState();if(state.simTime-s.lastAuditAt<STX_WB_AUDIT_INTERVAL)return;s.lastAuditAt=state.simTime;
  state.empires.forEach(e=>{if(!owned(e.id).length)return;const b=stxWBPowerBreakdown(e.id),old=s.lastBreakdowns[e.id];if(old){const parts=[['Fleet strength',b.deployable-old.deployable],['Garrisons',b.garrison-old.garrison],['Orbital defenses',b.orbital-old.orbital]].filter(x=>Math.abs(x[1])>=1);if(parts.length){s.audit.unshift({time:state.simTime,empireId:e.id,total:b.total,changes:parts.map(([label,d])=>`${label} ${d>=0?'+':''}${d.toFixed(1)}`)});s.audit=s.audit.slice(0,60)}}s.lastBreakdowns[e.id]={deployable:b.deployable,garrison:b.garrison,orbital:b.orbital,total:b.total}});
}
window.SpaceTyrantsMilitaryAudit=()=>state.stxWarfareBalance?.audit?.slice()||[];

/* Rival Powers now exposes the same numerical military model for every empire. */
if(typeof stxRDRivalCard==="function"){
  const STX_WB_stxRDRivalCard=stxRDRivalCard;
  stxRDRivalCard=function(e){const html=STX_WB_stxRDRivalCard(e),b=stxWBPowerBreakdown(e.id),trend=e.enemyStrategicNavy?.trend||"Stable",tech=Number(e.tech?.weapons)||0;return html+`<div class="stx-wb-military-breakdown"><span>Total Military<b>${Math.round(b.total)}</b></span><span>Deployable<b>${Math.round(b.deployable)}</b></span><span>Active Fleets<b>${b.fleetCount}</b></span><span>Strongest Fleet<b>${Math.round(b.strongest)}</b></span><span>Shipyards<b>${b.shipyards}</b></span><span>Weapons / Trend<b>${tech.toFixed(1)} · ${trend}</b></span></div>`};
}
if(typeof stxRDSelectedActions==="function"){
  const STX_WB_stxRDSelectedActions=stxRDSelectedActions;
  stxRDSelectedActions=function(enemyId){const html=STX_WB_stxRDSelectedActions(enemyId),b=stxWBPowerBreakdown(0),r=stxWBPowerBreakdown(enemyId);return html+`<div class="stx-wb-own-power">Mandate military: <b>${Math.round(b.total)}</b> total · <b>${Math.round(b.deployable)}</b> deployable · ${b.fleetCount} fleets &nbsp; | &nbsp; Selected rival: <b>${Math.round(r.total)}</b> total · <b>${Math.round(r.deployable)}</b> deployable.</div>`};
}

const STX_WB_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_WB_generateGalaxy();state.stxWarfareBalance=null;stxWBEnsureState()};
const STX_WB_loadGame=loadGame;
loadGame=function(){const ok=STX_WB_loadGame();if(ok)stxWBEnsureState();return ok};
stxWBEnsureState();
