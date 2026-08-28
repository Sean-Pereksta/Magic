/* Space Tyrants — mandate certainty + credit economy pass.
   Every mandate shown to the player must either start a concrete action now or
   start the prerequisite project that unlocks the requested special action.
   Expensive civic/Admiralty requests make credits strategically meaningful. */

const STX_ME_REQUEST_COSTS={
  patrol:Math.round(rand(55,85)),
  fleet:Math.round(rand(135,210)),
  emergencyFleet:Math.round(rand(220,320)),
  shipyard:Math.round(rand(95,145)),
  station:Math.round(rand(120,180)),
  base:Math.round(rand(180,260))
};

function stxMEEnsureState(){
  state.mandatePaths=Array.isArray(state.mandatePaths)?state.mandatePaths:[];
  const e=empire(0);
  if(e){
    e.guaranteedOrders=Array.isArray(e.guaranteedOrders)?e.guaranteedOrders:[];
    if(!Number.isFinite(e.nextCivicCapitalRequestAt))e.nextCivicCapitalRequestAt=state.simTime+rand(55,100);
  }
  return e;
}
const STX_ME_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_ME_generateGalaxy();stxMEEnsureState()};
const STX_ME_loadGame=loadGame;
loadGame=function(){const ok=STX_ME_loadGame();if(ok)stxMEEnsureState();return ok};
stxMEEnsureState();

function stxMEPathExists(kind,targetId=null){return state.mandatePaths.some(x=>x.status!=="completed"&&x.kind===kind&&(targetId==null||x.targetId===targetId))}
function stxMERecordPath(kind,label,targetId=null,next="Implementation"){
  if(stxMEPathExists(kind,targetId))return;
  state.mandatePaths.unshift({id:`mp${Math.floor(random()*1e9)}`,kind,label,targetId,next,status:"active",createdAt:state.simTime});
  state.mandatePaths=state.mandatePaths.slice(0,30);
}
function stxMEWorldForProject(type){
  const worlds=playerWorlds().filter(p=>!p.underAttack&&!p.localProject);
  if(type==="shipyard")return worlds.filter(p=>p.infra.shipyard<1).sort((a,b)=>(b.infra.factory+b.pop*8)-(a.infra.factory+a.pop*8))[0]||null;
  if(type==="factory")return worlds.filter(p=>p.infra.factory<6).sort((a,b)=>(b.infra.mine+b.pop*7)-(a.infra.mine+a.pop*7))[0]||null;
  if(type==="research")return worlds.filter(p=>p.infra.research<6).sort((a,b)=>(b.infra.factory+b.pop*6)-(a.infra.factory+a.pop*6))[0]||null;
  if(type==="defense")return worlds.sort((a,b)=>borderThreat(b)-borderThreat(a))[0]||null;
  return worlds[0]||null;
}
function stxMEStartPrerequisite(kind,label){
  const e=empire(0);if(!e)return false;
  if(kind==="fleet"){
    if(typeof stxDIQueueFleetProgram==="function"){
      const before=playerWorlds().reduce((n,p)=>n+(p.buildQueue||[]).length,0);
      stxDIQueueFleetProgram(e,1,label,false);
      const after=playerWorlds().reduce((n,p)=>n+(p.buildQueue||[]).length,0);
      stxMERecordPath("fleet",label,null,"Battle fleet commission");
      return after>before||e.pendingDirectiveFleetBuilds>0;
    }
  }
  if(kind==="shipyard"){
    const p=stxMEWorldForProject("shipyard");if(p&&startLocalProject(p,"shipyard",label)){addOrder(p,"shipyard","components",64,6,label);stxMERecordPath("shipyard",label,p.id,"Shipyard completion");return true}
  }
  if(kind==="research"){
    const p=stxMEWorldForProject("research");if(p&&startLocalProject(p,"research",label)){stxMERecordPath("research",label,p.id,"Research complex completion");return true}
  }
  if(kind==="factory"){
    const p=stxMEWorldForProject("factory");if(p&&startLocalProject(p,"factory",label)){stxMERecordPath("factory",label,p.id,"Factory completion");return true}
  }
  if(kind==="defense"){
    const p=stxMEWorldForProject("defense");if(p&&startLocalProject(p,"defense",label)){stxMERecordPath("defense",label,p.id,"Defense network completion");return true}
  }
  return false;
}

/* Special mandate prerequisites are surfaced as actual path-building cards.
   The player never receives a dead special option: missing fleet -> build fleet,
   missing shipyard -> build shipyard, missing research -> build research, etc. */
function stxMEPrerequisiteChoice(c){
  if(!c)return null;
  const id=c.id||"",title=c.title||"Special Mandate";
  if(stxDGIsInvasionChoice?.(c)&&!stxDGHasDeployableFleet?.())return {id:`path-fleet-${id}`,cat:"Military",title:"Prepare an Invasion Fleet",desc:`${title} requires a deployable named battle fleet. Start that prerequisite now.`,effects:["Battle-fleet commission starts now","If needed, shipyard construction starts first","Invasion mandate becomes eligible once the fleet is ready"],apply:()=>stxMEStartPrerequisite("fleet",`Prerequisite for ${title}`)};
  if(["fleetCommission","assembleFleet"].includes(id)&&!stxBestFleetYard?.(0))return {id:`path-yard-${id}`,cat:"Industry",title:"Build the Required Shipyard",desc:`${title} needs a working shipyard. Begin one now instead of offering a mandate that cannot execute.`,effects:["Shipyard construction starts now","Fleet commission is retained behind it","Special military mandate unlocks on completion"],apply:()=>{const e=empire(0);if(e)e.pendingDirectiveFleetBuilds=Math.max(1,e.pendingDirectiveFleetBuilds||0);return stxMEStartPrerequisite("shipyard",`Prerequisite for ${title}`)}};
  if(["spaceStation","sectorBase"].includes(id)&&!c.targetObj){
    const candidate=stxMEWorldForProject("shipyard");
    if(candidate&&candidate.infra.shipyard<1)return {id:`path-orbital-yard-${id}`,cat:"Industry",title:"Develop an Orbital Construction Yard",desc:`${title} needs a developed shipyard world. Start the enabling yard now.`,effects:["Shipyard project starts now","Orbital construction remains the strategic goal","Station/base mandate becomes eligible afterward"],apply:()=>stxMEStartPrerequisite("shipyard",`Prerequisite for ${title}`)};
    return {id:`path-orbital-industry-${id}`,cat:"Industry",title:"Develop an Orbital Construction World",desc:`${title} needs a developed industrial world. Start factory expansion now.`,effects:["Factory project starts now","Creates an orbital construction candidate","Special orbital mandate becomes eligible afterward"],apply:()=>stxMEStartPrerequisite("factory",`Prerequisite for ${title}`)};
  }
  if(id==="sensorArray"&&!c.targetObj)return {id:`path-sensor-${id}`,cat:"Research",title:"Develop Frontier Research Capacity",desc:"A frontier sensor mandate needs an eligible research world. Build that capacity now.",effects:["Research complex starts now","Creates a sensor-array candidate","Sensor mandate becomes eligible afterward"],apply:()=>stxMEStartPrerequisite("research","Prerequisite for frontier sensor array")};
  return null;
}

const STX_ME_rebuild=stxDGRebuildCommandHand;
stxDGRebuildCommandHand=function(){
  STX_ME_rebuild();
  const current=[...state.commandChoices];
  const pool=COMMANDS.map(c=>stxDGChoiceFromCommand(c)).filter(Boolean);
  const blocked=pool.filter(c=>!stxDGCanExecuteChoice(c)).map(stxMEPrerequisiteChoice).filter(Boolean);
  const hasPath=current.some(c=>String(c.id).startsWith("path-"));
  if(!hasPath&&blocked.length){
    const replacement=blocked[0];
    const weak=current.findIndex(c=>!stxDGCanExecuteChoice(c));
    if(weak>=0)current[weak]=replacement;
    else if(current.length<4)current.push(replacement);
    else{
      const idx=current.findIndex(c=>c.cat===replacement.cat);
      current[idx>=0?idx:current.length-1]=replacement;
    }
    state.commandChoices=current.slice(0,4);state.commandSelected.clear();renderCommands();
  }
};

/* Last-line mandate execution contract. A selected command must leave behind a
   project, movement, trade/research directive, invasion plan, fleet commission,
   or a persisted prerequisite path. If not, automatically start a concrete
   fallback project in the same strategic family. */
function stxMESnapshot(){
  const e=empire(0);
  return {
    local:playerWorlds().filter(p=>p.localProject).length,
    orbital:playerWorlds().filter(p=>p.orbitalProject).length,
    scans:playerWorlds().filter(p=>p.scanProject).length,
    trade:playerWorlds().filter(p=>p.tradeStationProject).length,
    queues:playerWorlds().reduce((n,p)=>n+(p.buildQueue||[]).length,0),
    ships:state.ships.filter(s=>s.owner===0).length,
    fleets:state.fleets.filter(f=>f.owner===0&&!f.destroyed).length,
    invasions:(e?.invasionPlans||[]).filter(x=>!["completed","cancelled"].includes(x.status)).length,
    orders:(e?.guaranteedOrders||[]).length,
    paths:state.mandatePaths.filter(x=>x.status!=="completed").length,
    directives:(e?.directives||[]).length
  };
}
function stxMEChanged(a,b){return Object.keys(a).some(k=>b[k]>a[k])}
function stxMEFallbackFor(c){
  const cat=(c?.cat||"").toLowerCase(),title=c?.title||"Mandate";
  if(/military|war|campaign/.test(cat)||stxDGIsInvasionChoice?.(c))return stxMEStartPrerequisite("fleet",`Guaranteed action fallback for ${title}`);
  if(/research/.test(cat))return stxMEStartPrerequisite("research",`Guaranteed action fallback for ${title}`);
  if(/infrastructure|industry/.test(cat))return stxMEStartPrerequisite("factory",`Guaranteed action fallback for ${title}`);
  if(/resource/.test(cat)){
    const p=typeof stxMineTargets==="function"?stxMineTargets()[0]:null;
    if(p&&startLocalProject(p,"mine",`Guaranteed action fallback for ${title}`)){stxMERecordPath("mine",title,p.id,"Mine completion");return true}
  }
  const p=stxMEWorldForProject("defense");if(p&&startLocalProject(p,"defense",`Guaranteed action fallback for ${title}`)){stxMERecordPath("defense",title,p.id,"Defense completion");return true}
  return false;
}
const STX_ME_issueCommands=issueCommands;
issueCommands=function(){
  const selected=[...state.commandSelected].map(i=>state.commandChoices[i]).filter(Boolean),before=stxMESnapshot();
  STX_ME_issueCommands();
  let cursor=before;
  selected.forEach(c=>{
    const after=stxMESnapshot();
    if(!stxMEChanged(cursor,after)){
      const started=stxMEFallbackFor(c),final=stxMESnapshot();
      if(started){logEvent(`MANDATE GUARANTEE: ${c.title} could not resolve through its normal handler, so a concrete prerequisite project was started automatically.`,"good");showToast(`${c.title} · prerequisite action started`)}
      cursor=final;
    }else cursor=after;
  });
};

/* Credits now matter for major requests. Fleet requests can be deliberately
   expensive because approving them buys an immediate, concrete commission. */
function stxMEFleetRequestCost(type,war=false){
  if(type==="patrol")return Math.round(rand(55,90));
  return Math.round(war?rand(210,320):rand(135,225));
}
const STX_ME_createMilitaryRequest=stxCreateMilitaryRequest;
stxCreateMilitaryRequest=function(){
  const made=STX_ME_createMilitaryRequest();if(!made)return false;
  const r=[...state.militaryRequests].reverse().find(x=>x.status==="pending");
  if(r){r.creditCost=stxMEFleetRequestCost(r.type,!!stxPlayerAtWar());r.message+=` Funding this request requires ${r.creditCost} credits from the imperial treasury.`}
  return true;
};
const STX_ME_respondMilitaryRequest=stxRespondMilitaryRequest;
stxRespondMilitaryRequest=function(id,action){
  const r=state.militaryRequests.find(x=>x.id===id&&x.status==="pending"),e=empire(0);
  if(action==="approve"&&r){
    const cost=Math.max(0,Math.round(r.creditCost||0));
    if(cost&&e.credits<cost){showToast(`Treasury shortfall · need ${cost} credits`);logEvent(`REQUEST HELD: ${r.title} requires ${cost} credits; the treasury currently has ${Math.floor(e.credits)}.`,"warning");return}
    if(cost)e.credits-=cost;
  }
  STX_ME_respondMilitaryRequest(id,action);
};
const STX_ME_renderTransmissions=renderTransmissions;
renderTransmissions=function(){
  STX_ME_renderTransmissions();
  const e=empire(0),box=$("transmissionList");if(!box)return;
  box.querySelectorAll(".stx-military-request").forEach(card=>{
    const btn=card.querySelector('[data-military-response="approve"]'),id=btn?.dataset.militaryId;
    const r=id&&state.militaryRequests.find(x=>x.id===id);if(!btn||!r)return;
    const cost=Math.max(0,Math.round(r.creditCost||0));
    if(cost){btn.textContent=`Approve · ${cost} credits`;btn.disabled=e.credits<cost;btn.title=e.credits<cost?`Need ${cost-Math.floor(e.credits)} more credits`:"Fund and begin this project immediately"}
  });
};

/* Additional high-value local requests make the treasury compete with other
   strategic priorities instead of passively accumulating. */
function stxMECreateCapitalRequest(){
  const e=empire(0);if(!e||state.militaryRequests.some(r=>r.status==="pending"&&r.expiresAt>state.simTime))return false;
  const options=[];
  const yard=stxMEWorldForProject("shipyard");if(yard)options.push({kind:"shipyard",planet:yard,cost:Math.round(rand(95,155)),title:`${yard.name} Requests a Naval Shipyard Grant`});
  const defense=stxMEWorldForProject("defense");if(defense)options.push({kind:"defense",planet:defense,cost:Math.round(rand(70,125)),title:`${defense.name} Requests a Frontier Defense Grant`});
  if(!options.length)return false;
  const o=pick(options),r={id:`mr${Math.floor(random()*1e9)}`,status:"pending",createdAt:state.simTime,expiresAt:state.simTime+48,planetId:o.planet.id,type:"capital",capitalKind:o.kind,creditCost:o.cost,title:o.title,message:`Local leaders are asking the Mandate to fund a permanent ${o.kind==="shipyard"?"shipyard":"defense network"}. Approval immediately starts construction and costs ${o.cost} credits.`};
  state.militaryRequests.push(r);e.nextCivicCapitalRequestAt=state.simTime+rand(95,160);renderTransmissions();updateBadges?.();return true;
}
const STX_ME_militaryTick=stxMilitaryRequestTick;
stxMilitaryRequestTick=function(){
  STX_ME_militaryTick();const e=empire(0);if(e&&state.simTime>=(e.nextCivicCapitalRequestAt||Infinity))stxMECreateCapitalRequest();
};
const STX_ME_response2=stxRespondMilitaryRequest;
stxRespondMilitaryRequest=function(id,action){
  const r=state.militaryRequests.find(x=>x.id===id&&x.status==="pending");
  if(r?.type!=="capital")return STX_ME_response2(id,action);
  const e=empire(0),p=state.planets.find(x=>x.id===r.planetId),cost=Math.round(r.creditCost||0);
  if(action!=="approve"){r.status="declined";showToast("Capital request declined");renderTransmissions();return}
  if(!p||e.credits<cost){showToast(`Treasury shortfall · need ${cost} credits`);return}
  e.credits-=cost;r.status="accepted";
  const ok=r.capitalKind==="shipyard"?startLocalProject(p,"shipyard","Approved local capital request"):startLocalProject(p,"defense","Approved local capital request");
  if(ok){p.mandateGlow=1;logEvent(`TREASURY FUNDED: ${p.name} received ${cost} credits and immediately began ${r.capitalKind==="shipyard"?"shipyard":"defense"} construction.`,"good");showToast(`${p.name} project funded · ${cost} credits`)}
  renderTransmissions();updateBadges?.();
};
const STX_ME_render2=renderTransmissions;
renderTransmissions=function(){
  STX_ME_render2();const box=$("transmissionList"),e=empire(0);if(!box)return;
  const capital=state.militaryRequests.filter(r=>r.type==="capital"&&r.status==="pending"&&r.expiresAt>state.simTime);
  if(!capital.length)return;
  if(box.querySelector(".empty-hub"))box.innerHTML="";
  box.insertAdjacentHTML("beforeend",capital.map(r=>{const p=state.planets.find(x=>x.id===r.planetId),short=e.credits<r.creditCost;return `<article class="transmission-card urgent stx-capital-request"><div class="card-kicker">PLANETARY CAPITAL REQUEST // ${p?.name||"MANDATE WORLD"}</div><div class="card-title"><strong>${r.title}</strong><span class="news-time">${fmtEta(r.expiresAt-state.simTime)}</span></div><p class="card-copy">${r.message}</p><div class="choice-row"><button class="choice-btn primary-choice" data-military-response="approve" data-military-id="${r.id}" ${short?"disabled":""}>Fund · ${r.creditCost} credits</button><button class="choice-btn danger-choice" data-military-response="decline" data-military-id="${r.id}">Decline</button></div></article>`}).join(""));
  box.querySelectorAll('.stx-capital-request [data-military-id]').forEach(b=>b.onclick=()=>stxRespondMilitaryRequest(b.dataset.militaryId,b.dataset.militaryResponse));
};
