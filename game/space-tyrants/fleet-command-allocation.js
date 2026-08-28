/* Space Tyrants — explicit fleet allocation orders.
   Player invasion/defense/concentration mandates choose targets and fleet counts
   after issuance. Fleet refits lock one named fleet in orbit until supplied and complete. */

let stxFCAQueue=[],stxFCAActive=false,stxFCAResume=1,stxFCAUsedTargets=new Set(),stxFCACommitted=new Set();

function stxFCAState(){
  const s=state.stxFleetAllocation||(state.stxFleetAllocation={phase:0,lastUpgradeOffer:-4});
  if(!Number.isFinite(s.phase))s.phase=0;
  if(!Number.isFinite(s.lastUpgradeOffer))s.lastUpgradeOffer=-4;
  return s;
}
function stxFCABattleIds(){
  if(typeof stxDGActiveBattleFleetIds==="function")return stxDGActiveBattleFleetIds();
  const out=new Set();state.battles.forEach(b=>{(b.attackerFleetIds||[]).forEach(x=>out.add(x));(b.defenderFleetIds||[]).forEach(x=>out.add(x))});return out;
}
function stxFCAFreeFleet(f,allowReserved=true){
  if(!f||f.owner!==0||f.destroyed||!f.location||f.stxRefit||stxFCACommitted.has(f.id))return false;
  if(state.ships.some(s=>s.fleetId===f.id)||stxFCABattleIds().has(f.id))return false;
  const p=state.planets.find(x=>x.id===f.location);
  return !!p&&p.owner===0&&(Number(f.strength)||0)>4&&(allowReserved||!f.stxAssignment?.locked);
}
function stxFCAFree(target=null,allowReserved=true){
  return state.fleets.filter(f=>stxFCAFreeFleet(f,allowReserved)).sort((a,b)=>{
    if(target){
      const ap=state.planets.find(p=>p.id===a.location),bp=state.planets.find(p=>p.id===b.location);
      const ad=ap?dist(ap,target):Infinity,bd=bp?dist(bp,target):Infinity;if(ad!==bd)return ad-bd;
    }
    return (b.strength||0)-(a.strength||0);
  });
}
function stxFCAWars(){return state.wars.filter(w=>w.active&&(w.a===0||w.b===0))}
function stxFCATargets(kind){
  if(kind==="invade"){
    const foes=new Set(stxFCAWars().map(w=>w.a===0?w.b:w.a));
    let worlds=state.planets.filter(p=>p.owner!==null&&p.owner!==0&&!p.underAttack&&!stxFCAUsedTargets.has(p.id));
    if(foes.size)worlds=worlds.filter(p=>foes.has(p.owner));
    return worlds.sort((a,b)=>{
      const ad=typeof stxNearestPlayerDistance==="function"?stxNearestPlayerDistance(a):0,bd=typeof stxNearestPlayerDistance==="function"?stxNearestPlayerDistance(b):0;
      return ad-bd;
    });
  }
  return playerWorlds().slice().sort((a,b)=>kind==="defend"?borderThreat(b)-borderThreat(a):((b.orbitals?.base||0)+(b.infra?.shipyard||0))-((a.orbitals?.base||0)+(a.infra?.shipyard||0)));
}
function stxFCAUpgradeCandidates(){
  return stxFCAFree().filter(f=>state.simTime-(f.lastUpgradeAt??-999)>70).sort((a,b)=>(a.strength||0)-(b.strength||0));
}
function stxFCAClear(f){
  if(!f)return;f.stxAssignment=null;
  if(typeof stxOLRemoveDocking==="function")stxOLRemoveDocking(f);else f.dockedAt=null;
}

function stxFCAIsInvade(c){
  if(c?.stxFleetOrderKind==="invade")return true;
  if(typeof stxWDBIsInvasion==="function")return stxWDBIsInvasion(c);
  if(typeof stxDGIsInvasionChoice==="function")return stxDGIsInvasionChoice(c);
  return /invasion|\binvade\b/i.test(`${c?.id||""} ${c?.title||""}`);
}
function stxFCAChoice(kind,n=0){
  const free=stxFCAFree().length,upgrade=stxFCAUpgradeCandidates().length;
  const base={stxFleetOrderKind:kind,targetObj:null,apply:()=>{stxFCAQueue.push({kind});return true}};
  if(kind==="invade")return{...base,id:`fca-invade-${n}-${Math.floor(state.simTime)}`,cat:n?"War":"Military",title:"Invade",desc:"Choose the enemy planet after issuing the mandate, then choose how many of the closest available fleets will attack.",effects:[`${free} free fleets now`,"Choose target after issuance","Committed fleets disappear from later allocation"]};
  if(kind==="defend")return{...base,id:`fca-defend-${Math.floor(state.simTime)}`,cat:"Military",title:"Defend",desc:"Choose one of your planets and assign a specific number of the closest fleets to defend it.",effects:[`${free} free fleets now`,"Choose friendly target","Assigned fleets stay reserved until retasked"]};
  if(kind==="concentrate")return{...base,id:`fca-concentrate-${Math.floor(state.simTime)}`,cat:"Military",title:"Concentrate Fleets",desc:"Mass a selected number of fleets at one friendly system for a future campaign or rapid response.",effects:[`${free} free fleets now`,"Closest fleets redeploy","Concentrated fleets stay reserved"]};
  return{...base,id:`fca-upgrade-${Math.floor(state.simTime)}`,cat:"Military",title:"Upgrade a Fleet",desc:"Choose one named fleet to refit. It must remain at its current friendly planet until the work completes.",effects:[`${upgrade} eligible fleets`,"Uses Components, Equipment, Titanium","Fleet is unavailable during refit"]};
}
function stxFCAReplace(choices,choice,protect=[]){
  if(choices.length<4)return choices.push(choice);
  let i=choices.findIndex(c=>!c.stxFleetOrderKind&&!stxFCAIsInvade(c));
  if(i<0)i=choices.findIndex(c=>!protect.includes(c.stxFleetOrderKind));
  choices[i<0?choices.length-1:i]=choice;
}
function stxFCAPostHand(){
  if($("commandModal").hidden)return;
  const s=stxFCAState();s.phase++;
  const free=stxFCAFree().length,wars=stxFCAWars(),targets=stxFCATargets("invade");let choices=state.commandChoices.slice(),n=0;
  choices=choices.map(c=>stxFCAIsInvade(c)?stxFCAChoice("invade",n++):c);
  if(wars.length&&free&&targets.length){
    const desired=Math.min(2,free,targets.length);
    while(choices.filter(c=>c.stxFleetOrderKind==="invade").length<desired)stxFCAReplace(choices,stxFCAChoice("invade",n++),["invade"]);
    if(!choices.some(c=>c.stxFleetOrderKind==="defend"))stxFCAReplace(choices,stxFCAChoice("defend"),["invade"]);
  }else if(!wars.length&&free>=2&&!choices.some(c=>c.stxFleetOrderKind)){
    const i=choices.findIndex(c=>/military/i.test(c.cat||""));if(i>=0)choices[i]=stxFCAChoice("concentrate");else stxFCAReplace(choices,stxFCAChoice("concentrate"));
  }
  const drought=s.phase-s.lastUpgradeOffer;
  if(stxFCAUpgradeCandidates().length&&(drought>=5||(drought>=3&&random()<.42))&&!choices.some(c=>c.stxFleetOrderKind==="upgrade")){
    const i=choices.findIndex(c=>!c.stxFleetOrderKind&&!stxFCAIsInvade(c));if(i>=0)choices[i]=stxFCAChoice("upgrade");else if(!wars.length)stxFCAReplace(choices,stxFCAChoice("upgrade"),["invade","defend"]);
    if(choices.some(c=>c.stxFleetOrderKind==="upgrade"))s.lastUpgradeOffer=s.phase;
  }
  state.commandChoices=choices.slice(0,4);state.commandSelected.clear();renderCommands();
}
const STX_FCA_openCommandPhase=openCommandPhase;
openCommandPhase=function(){STX_FCA_openCommandPhase();stxFCAPostHand()};

const STX_FCA_renderCommands=renderCommands;
renderCommands=function(){
  STX_FCA_renderCommands();const free=stxFCAFree().length,grid=$("commandGrid");if(!grid)return;
  [...grid.children].forEach((b,i)=>{const c=state.commandChoices[i];if(!c?.stxFleetOrderKind)return;const old=b.onclick;b.onclick=()=>{
    if(!state.commandSelected.has(i)){
      const selected=[...state.commandSelected].filter(x=>state.commandChoices[x]?.stxFleetOrderKind).length;
      if(selected>=free)return showToast(`Only ${free} free fleet${free===1?" is":"s are"} available for fleet orders`);
    }
    old?.();
  }});
};

if(typeof stxDGDeployableFleets==="function"){const old=stxDGDeployableFleets;stxDGDeployableFleets=function(t=null){return old(t).filter(f=>!f.stxRefit&&!f.stxAssignment?.locked)}}
if(typeof stxIFAStationedMilitaryFleets==="function"){const old=stxIFAStationedMilitaryFleets;stxIFAStationedMilitaryFleets=function(){return old().filter(f=>!f.stxRefit&&!f.stxAssignment?.locked)}}
if(typeof stxOLFleetCandidates==="function"){const old=stxOLFleetCandidates;stxOLFleetCandidates=function(owner=0){return old(owner).filter(f=>!f.stxRefit)}}

function stxFCAInstallUi(){
  if($("stxFCAOrderModal"))return;
  const modal=document.createElement("div");modal.id="stxFCAOrderModal";modal.className="modal-wrap";modal.hidden=true;
  modal.innerHTML=`<section class="glass stx-fca-box"><div class="kicker" id="stxFCAKicker">FLEET ORDER</div><h2 id="stxFCATitle"></h2><p class="subtle" id="stxFCADesc"></p><label class="section-label" for="stxFCATarget">Target / Fleet</label><select id="stxFCATarget"></select><div id="stxFCACountWrap"><label class="section-label" for="stxFCACount">Fleet Count</label><input id="stxFCACount" type="number" min="1" value="1"><div class="subtle" id="stxFCAPreview"></div></div><div class="command-footer"><span class="command-note" id="stxFCAAvailable"></span><button class="issue-btn" id="stxFCAConfirm">Confirm Order</button></div></section>`;
  document.body.appendChild(modal);
  const style=document.createElement("style");style.textContent=`.stx-fca-box{width:min(620px,94vw);border-radius:20px;padding:22px;background:linear-gradient(155deg,rgba(13,22,48,.99),rgba(5,8,20,.99));border:1px solid rgba(111,156,255,.28)}.stx-fca-box h2{margin:6px 0}.stx-fca-box select,.stx-fca-box input{width:100%;border:1px solid rgba(120,153,224,.25);background:#091326;color:#eef5ff;border-radius:10px;padding:10px;font:inherit}.stx-fca-box input{width:110px}.stx-fca-box .command-footer{margin-top:18px}`;
  document.head.appendChild(style);
  $("stxFCATarget").addEventListener("change",stxFCASyncUi);$("stxFCACount").addEventListener("input",stxFCASyncUi);$("stxFCAConfirm").onclick=stxFCAConfirm;
}
stxFCAInstallUi();

function stxFCARemaining(){return Math.max(0,stxFCAQueue.length-1)}
function stxFCASyncUi(){
  const order=stxFCAQueue[0];if(!order)return;
  const select=$("stxFCATarget"),count=$("stxFCACount"),id=select.value;
  if(order.kind==="upgrade"){
    const f=fleetRecord(id);$("stxFCAPreview").textContent=f?`Upgrade ${Math.round(f.strength)} → ${Math.ceil((f.strength||5)*1.24+3)} strength at ${state.planets.find(p=>p.id===f.location)?.name||"current planet"}.`:"";
    $("stxFCAConfirm").disabled=!f;return;
  }
  const target=state.planets.find(p=>p.id===id),free=target?stxFCAFree(target):[];
  const max=Math.max(0,free.length-stxFCARemaining());count.max=Math.max(1,max);count.value=String(clamp(Number(count.value)||1,1,Math.max(1,max)));
  const n=Number(count.value)||1,$p=$("stxFCAPreview");$p.textContent=target?`Closest ${n}: ${free.slice(0,n).map(f=>`${f.name} (${Math.round(f.strength)})`).join(" · ")}${stxFCARemaining()?` · ${stxFCARemaining()} fleet${stxFCARemaining()===1?"":"s"} reserved for remaining selected mandate${stxFCARemaining()===1?"":"s"}.`:""}`:"";
  $("stxFCAConfirm").disabled=!target||max<1;
}
function stxFCAShow(){
  if(!stxFCAQueue.length)return stxFCAFinish();
  const o=stxFCAQueue[0],upgrade=o.kind==="upgrade",targets=upgrade?stxFCAUpgradeCandidates():stxFCATargets(o.kind),sel=$("stxFCATarget");
  $("stxFCAOrderModal").hidden=false;$("stxFCAKicker").textContent=`${o.kind.toUpperCase()} ORDER · ${stxFCAQueue.length} MANDATE${stxFCAQueue.length===1?"":"S"} TO RESOLVE`;
  $("stxFCATitle").textContent=upgrade?"Select a Fleet to Upgrade":o.kind==="invade"?"Select Enemy Planet":o.kind==="defend"?"Select Planet to Defend":"Select Fleet Concentration Point";
  $("stxFCADesc").textContent=upgrade?"The fleet stays locked at its current planet until all refit materials arrive and work reaches 100%.":"Choose the target and fleet count. The closest free fleets are selected automatically.";
  sel.innerHTML=`<option value="">Choose...</option>`+targets.map(x=>{
    if(upgrade){const p=state.planets.find(q=>q.id===x.location);return`<option value="${x.id}">${x.name} · ${Math.round(x.strength)} strength · ${p?.name||"Unknown"}</option>`}
    return`<option value="${x.id}">${x.name}${o.kind==="invade"?` · ${empire(x.owner)?.name||"Enemy"}`:""}</option>`;
  }).join("");
  $("stxFCACountWrap").hidden=upgrade;$("stxFCAAvailable").textContent=`${stxFCAFree().length} free fleets now`;
  $("stxFCACount").value="1";stxFCASyncUi();
}
function stxFCAStart(){
  if(stxFCAActive||!stxFCAQueue.length)return;stxFCAActive=true;stxFCAUsedTargets=new Set();stxFCACommitted=new Set();stxFCAResume=state.speed||state.preCommandSpeed||1;state.speed=0;updateSpeedButtons();stxFCAShow();
}
function stxFCAFinish(){
  $("stxFCAOrderModal").hidden=true;stxFCAActive=false;stxFCAQueue=[];stxFCAUsedTargets.clear();stxFCACommitted.clear();state.speed=stxFCAResume||1;updateSpeedButtons();updateHud(true);saveGame(false);
}

function stxFCAAssign(f,p,kind){
  stxFCAClear(f);f.location=p.id;f.readiness=kind==="defend"?"rapid response ready":"ready";f.status=kind==="defend"?`Defending ${p.name}`:`Concentrated at ${p.name}`;f.stxAssignment={kind,targetId:p.id,locked:true,assignedAt:state.simTime};p.mandateGlow=1;
}
function stxFCAMove(f,target,kind){
  const source=state.planets.find(p=>p.id===f.location);if(!source)return false;stxFCAClear(f);
  if(source.id===target.id){stxFCAAssign(f,target,kind);return true}
  const old={location:f.location,status:f.status,readiness:f.readiness};f.location=null;f.readiness="inbound";f.status=kind==="defend"?`Redeploying to defend ${target.name}`:`Concentrating at ${target.name}`;
  const ship=createShip("fleet",source,target,0,{strength:f.strength,fleetId:f.id,vesselName:f.name,reinforcement:kind==="defend",stxFleetOrderKind:kind,servicePurpose:kind,speedBoost:1.08+(source.orbitals?.base||0)*.22});
  if(!ship){Object.assign(f,old);return false}return true;
}
function stxFCAExecuteMove(kind,target,count){
  let moved=0;stxFCAFree(target).slice(0,count).forEach(f=>{if(stxFCAMove(f,target,kind)){stxFCACommitted.add(f.id);moved++}});
  if(moved){logEvent(`${kind==="defend"?"DEFENSE ORDER":"FLEET CONCENTRATION"}: ${moved} closest fleet${moved===1?"":"s"} committed to ${target.name}.`,"good");if(typeof stxActivity==="function")stxActivity(`${moved} fleet${moved===1?"":"s"} assigned to ${target.name}.`,target.id,null,"good");if(typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator()}return moved;
}
function stxFCAExecuteInvade(target,count){
  if(!target||target.owner===null||target.owner===0||typeof stxQueueInvasion!=="function")return 0;
  if(!stxQueueInvasion(target,`Manual invasion of ${target.name}`,false))return 0;
  const plan=(empire(0).invasionPlans||[]).find(p=>p.targetId===target.id),war=getWar(0,target.owner);let launched=0,ids=[];
  for(const f of stxFCAFree(target).slice(0,count)){
    const source=state.planets.find(p=>p.id===f.location);if(!source)continue;stxFCAClear(f);
    const old={location:f.location,status:f.status,readiness:f.readiness};f.location=null;f.readiness="inbound";f.status=`Invading ${target.name}`;
    const ship=createShip("fleet",source,target,0,{strength:Math.max(5,f.strength||5),fleetId:f.id,vesselName:f.name,warId:war?.id,invasionPlanId:plan?.id,stxManualFleetOrder:true,speedBoost:1.08+(source.orbitals?.base||0)*.38});
    if(!ship){Object.assign(f,old);continue}stxFCACommitted.add(f.id);launched++;ids.push(f.id);
  }
  if(plan&&launched){plan.stxManualAllocation=true;plan.stxRequestedFleetCount=launched;plan.stxAssignedFleetIds=ids;plan.stxManualLaunchedAt=state.simTime;plan.lastLaunchAt=state.simTime;plan.status="fleet inbound"}
  if(launched){stxFCAUsedTargets.add(target.id);logEvent(`INVASION LAUNCHED: ${launched} closest fleet${launched===1?"":"s"} committed to ${target.name}.`,"warning");if(typeof stxActivity==="function")stxActivity(`${launched} manually allocated fleet${launched===1?"":"s"} launched for ${target.name}.`,target.id,null,"warning");if(typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator()}return launched;
}

function stxFCARefitNeed(f){const s=Math.max(5,f.strength||5);return{components:Math.ceil(18+s*1.25),equipment:Math.ceil(12+s*.82),titanium:Math.ceil(10+s*.68)}}
function stxFCAPrimeRefit(f,p){
  const q=f.stxRefit;if(!q||state.simTime<(q.nextSupplyAt||-999))return;q.nextSupplyAt=state.simTime+6;
  Object.entries(q.need).forEach(([r,a])=>{const left=Math.max(0,a-(q.delivered?.[r]||0));if(left>.01)addOrder(p,"fleetRefit",r,left,7,`${f.name} fleet upgrade`)});
}
function stxFCAStartRefit(f){
  if(!stxFCAFreeFleet(f))return false;stxFCACommitted.add(f.id);const p=state.planets.find(x=>x.id===f.location);if(!p)return false;stxFCAClear(f);
  const need=stxFCARefitNeed(f);f.stxRefit={planetId:p.id,need,delivered:Object.fromEntries(Object.keys(need).map(r=>[r,0])),progress:0,targetStrength:Math.ceil((f.strength||5)*1.24+3),startedAt:state.simTime,nextSupplyAt:-999};
  f.stxAssignment={kind:"upgrade",targetId:p.id,locked:true};f.readiness="refitting";f.status=`Upgrading at ${p.name} · awaiting materials`;p.mandateGlow=1;stxFCAPrimeRefit(f,p);
  logEvent(`FLEET UPGRADE: ${f.name} entered refit at ${p.name} and cannot deploy until complete.`,"good");if(typeof stxActivity==="function")stxActivity(`${f.name} began a supply-backed refit at ${p.name}.`,p.id,f.id,"good");return true;
}
function stxFCATickRefits(p,dt){
  state.fleets.filter(f=>f.owner===0&&!f.destroyed&&f.stxRefit?.planetId===p.id&&f.location===p.id).forEach(f=>{
    const q=f.stxRefit;Object.entries(q.need).forEach(([r,a])=>{const have=q.delivered[r]||0,left=Math.max(0,a-have),floor=["components","equipment"].includes(r)?4:8,take=Math.min(left,Math.max(0,(p.stock?.[r]||0)-floor));if(take>.01)q.delivered[r]=have+consume(p,r,take)});
    const ready=Object.entries(q.need).every(([r,a])=>(q.delivered[r]||0)>=a-.01);
    if(!ready){stxFCAPrimeRefit(f,p);const missing=Object.entries(q.need).filter(([r,a])=>(q.delivered[r]||0)<a-.01).map(([r])=>RESOURCE_LABEL[r]||r).join(", ");f.status=`Upgrading at ${p.name} · waiting for ${missing}`;return}
    q.progress=clamp(q.progress+dt*.012*(1+(p.infra?.shipyard||0)*.22+(p.orbitals?.base||0)*.18+(p.infra?.factory||0)*.08),0,1);f.status=`Upgrading at ${p.name} · ${Math.floor(q.progress*100)}%`;
    if(q.progress<1)return;const old=Math.round(f.strength||0);f.strength=Math.max(f.strength||0,q.targetStrength);f.maxServiceStrength=Math.max(f.maxServiceStrength||0,f.strength);f.lastUpgradeAt=state.simTime;f.stxRefit=null;f.stxAssignment=null;f.readiness="ready";f.supplyReadiness=1;f.status=`Upgraded and ready at ${p.name}`;
    logEvent(`FLEET UPGRADE COMPLETE: ${f.name} increased from ${old} to ${Math.round(f.strength)} strength.`,"good");if(typeof stxActivity==="function")stxActivity(`${f.name} completed its refit at ${p.name}.`,p.id,f.id,"good");showToast(`${f.name} upgrade complete`);if(typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator();
  });
}
const STX_FCA_tickPlanet=tickPlanet;
tickPlanet=function(p,dt){STX_FCA_tickPlanet(p,dt);stxFCATickRefits(p,dt)};

const STX_FCA_arriveShip=arriveShip;
arriveShip=function(s,p){const kind=s?.stxFleetOrderKind,id=s?.fleetId;STX_FCA_arriveShip(s,p);if((kind==="defend"||kind==="concentrate")&&p?.owner===0&&id){const f=fleetRecord(id);if(f&&!f.destroyed&&!state.ships.some(x=>x.fleetId===id))stxFCAAssign(f,p,kind)}};

if(typeof stxMobilizeInvasionPlan==="function"){
  const old=stxMobilizeInvasionPlan;stxMobilizeInvasionPlan=function(plan,force=false){
    if(plan?.stxManualAllocation){
      const t=state.planets.find(p=>p.id===plan.targetId);if(!t||t.owner===0||t.owner===null){plan.status="completed";return false}
      if(!empiresAtWar(0,t.owner)){plan.status="awaiting war";return false}
      if(t.underAttack||state.ships.some(s=>s.owner===0&&s.invasionPlanId===plan.id)){plan.status=t.underAttack?"engaged":"fleet inbound";return true}
      if(plan.stxManualLaunchedAt!=null){plan.status="awaiting player reinforcement";return false}
    }
    return old(plan,force);
  };
}

function stxFCAConfirm(){
  const o=stxFCAQueue[0],id=$("stxFCATarget").value;if(!o)return;let ok=false;
  if(o.kind==="upgrade")ok=stxFCAStartRefit(fleetRecord(id));
  else{const t=state.planets.find(p=>p.id===id),n=Math.max(1,Number($("stxFCACount").value)||1);ok=o.kind==="invade"?stxFCAExecuteInvade(t,n)>0:stxFCAExecuteMove(o.kind,t,n)>0}
  if(!ok)return showToast("That fleet order cannot execute now");
  stxFCAQueue.shift();saveGame(false);stxFCAQueue.length?stxFCAShow():stxFCAFinish();
}

const STX_FCA_issueCommands=issueCommands;
issueCommands=function(){const before=stxFCAQueue.length;STX_FCA_issueCommands();if(stxFCAQueue.length>before)stxFCAStart()};

const STX_FCA_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_FCA_generateGalaxy();stxFCAState();stxFCAQueue=[];stxFCACommitted.clear();stxFCAActive=false};
const STX_FCA_loadGame=loadGame;
loadGame=function(){const ok=STX_FCA_loadGame();if(ok){stxFCAState();stxFCAQueue=[];stxFCACommitted.clear();stxFCAActive=false}return ok};

if(typeof renderShipLedger==="function"){
  const old=renderShipLedger;renderShipLedger=function(p){
    const base=old(p),rows=state.fleets.filter(f=>f.stxRefit?.planetId===p.id).map(f=>{const q=f.stxRefit,m=Object.entries(q.need).map(([r,a])=>`${RESOURCE_LABEL[r]||r} ${Math.floor(q.delivered[r]||0)}/${a}`).join(" · ");return`<div class="project-row mandate"><div class="project-head"><strong>Fleet Refit · ${f.name}</strong><b>${Math.floor(q.progress*100)}%</b></div><div class="project-desc">Locked at ${p.name} · target strength ${q.targetStrength}</div><div class="project-track"><i style="width:${Math.floor(q.progress*100)}%"></i></div><div class="project-meta"><span>${m}</span><span>Cannot deploy</span></div></div>`}).join("");
    return rows+base;
  };
}
