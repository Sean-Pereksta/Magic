/* Space Tyrants — integrated economic logistics + deep-space fortresses.
   This layer intentionally works through existing stockpiles, production,
   project orders and physical ships. Priority changes allocation; it never
   grants free materials or instant completion. */

const STX_IF_VERSION=1;
const STX_IF_RESOURCES=["iron","silicates","titanium","helium","rare","components","equipment"];
const STX_IF_RAW=new Set(["iron","silicates","titanium","helium","rare"]);
const STX_IF_MODES=["balanced","project","export"];
let stxIFLastTick=-999,stxIFLastRouting=-999;

function stxIFN(v,d=0){const n=Number(v);return Number.isFinite(n)?n:d}
function stxIFLabel(r){return typeof stxSDResourceLabel==="function"?stxSDResourceLabel(r):(RESOURCE_LABEL?.[r]||r)}
function stxIFEnsurePlanet(p){if(p)p.stxResourceRouting=p.stxResourceRouting||{};return p}
function stxIFRoute(p,r){stxIFEnsurePlanet(p);return STX_IF_MODES.includes(p?.stxResourceRouting?.[r])?p.stxResourceRouting[r]:"balanced"}
function stxIFPriorityProject(){return typeof stxPSPriorityProject==="function"?stxPSPriorityProject(0):null}
function stxIFNeed(d,r){
  if(!d)return 0;const remaining=typeof stxSDRemaining==="function"?stxSDRemaining(d,r):Math.max(0,stxIFN(d.need?.[r])-stxIFN(d.q?.stxSupply?.delivered?.[r]));
  const incoming=typeof stxSDIncomingAmount==="function"?stxSDIncomingAmount(d,r):0;return Math.max(0,remaining-incoming);
}
function stxIFLocalNeed(p,r){return typeof stxSDDescriptors==="function"?stxSDDescriptors(p).reduce((n,d)=>n+Math.max(0,stxIFN(d.need?.[r])-stxIFN(d.q?.stxSupply?.delivered?.[r])),0):0}
function stxIFExpansionDescriptor(p){return typeof stxSDDescriptors==="function"?stxSDDescriptors(p).find(d=>d.kind==="expansion")||null:null}
function stxIFExpansionLoad(p){
  const d=stxIFExpansionDescriptor(p);if(!d)return 0;const supplied=typeof stxSDCeiling==="function"?stxSDCeiling(d):clamp(stxIFN(d.q?.progress),0,1);
  return clamp(.35+(1-supplied)*.65,.35,1);
}

function stxIFSetRoute(planetId,r,mode){
  const p=state.planets.find(x=>x.id===planetId);if(!p||p.owner!==0||!STX_IF_RESOURCES.includes(r)||!STX_IF_MODES.includes(mode))return false;
  stxIFEnsurePlanet(p);p.stxResourceRouting[r]=mode;p.mandateGlow=1;showToast(`${p.name} · ${stxIFLabel(r)}: ${mode}`);renderPlanet();return true;
}
function stxIFCycleRoute(planetId,r){const p=state.planets.find(x=>x.id===planetId);if(!p)return false;const i=STX_IF_MODES.indexOf(stxIFRoute(p,r));return stxIFSetRoute(planetId,r,STX_IF_MODES[(i+1)%STX_IF_MODES.length])}

/* PROJECT worlds protect more discretionary stock from commerce. EXPORT worlds
   release more surplus, but their active local-project commitments stay safe. */
if(typeof stxRTPlanetReserve==="function"){
  const STX_IF_rtReserve=stxRTPlanetReserve;
  stxRTPlanetReserve=function(p,r){
    const base=STX_IF_rtReserve(p,r);if(!p||p.owner!==0)return base;const local=stxIFLocalNeed(p,r),mode=stxIFRoute(p,r),normal=typeof stxRTReserve==="function"?stxRTReserve(r):18;
    if(mode==="project")return Math.max(base,local+normal*.65);
    if(mode==="export")return Math.max(local,base*.55);
    return base;
  };
}

/* A player-prioritized project gets first claim on deeper reserves. Travel and
   production time remain real, so this is a logistics guarantee rather than a
   hidden completion cheat. */
if(typeof stxSDReserveFor==="function"){
  const STX_IF_sdReserve=stxSDReserveFor;
  stxSDReserveFor=function(r,amount,priority=1){
    if(priority>=100){
      if(r==="trained")return Math.min(.00003,Math.max(.000005,stxIFN(amount)*.04));
      if(r==="components"||r==="equipment")return Math.max(1.5,stxIFN(amount)*.045);
      return Math.max(3,stxIFN(amount)*.055);
    }
    return STX_IF_sdReserve(r,amount,priority);
  };
}

function stxIFMobilization(p){
  const m={mine:1,factory:1,training:1,research:1,label:""},expansion=stxIFExpansionLoad(p),priority=stxIFPriorityProject();
  if(expansion){m.mine*=1-.18*expansion;m.factory*=1-.30*expansion;m.training*=1-.22*expansion;m.research*=1-.12*expansion;m.label="Colony mobilization"}
  if(!priority||p.owner!==0||p===priority.p)return m;
  const shortages=Object.fromEntries(Object.keys(priority.need||{}).map(r=>[r,stxIFNeed(priority,r)]));
  const raw=Object.entries(shortages).filter(([r,a])=>STX_IF_RAW.has(r)&&a>.2).sort((a,b)=>b[1]-a[1])[0];
  if(raw&&stxIFN(p.infra?.mine)>0){const r=raw[0],good=p.stxResourcePrimary===r||p.stxResourceSecondary===r||stxIFN(p.quality?.[r])>=4;if(good){m.mine*=1.28;m.factory*=.90;m.label=`Priority ${stxIFLabel(r)} extraction`}}
  if(stxIFN(shortages.components)+stxIFN(shortages.equipment)>.3&&stxIFN(p.infra?.factory)>0){m.factory*=1.22;m.mine*=.94;m.label="Priority industrial production"}
  if(stxIFN(shortages.trained)>.00002&&(stxIFN(p.infra?.training)>0||stxIFN(p.infra?.shipyard)>0)){m.training*=1.45;m.factory*=.92;m.label="Priority personnel mobilization"}
  return m;
}

/* Expansion therefore has an explicit opportunity cost: the sponsor remains
   productive, but up to 30% of factory capacity plus smaller mining/training/
   research shares are tied up until the colony mission finishes. */
if(typeof tickPlanet==="function"){
  const STX_IF_tickPlanet=tickPlanet;
  tickPlanet=function(p,dt){
    if(!p||p.owner===null)return STX_IF_tickPlanet(p,dt);stxIFEnsurePlanet(p);const m=stxIFMobilization(p),infra=p.infra||{},old={};
    for(const [key,factor] of [["mine",m.mine],["factory",m.factory],["training",m.training],["research",m.research]])if(Number.isFinite(infra[key])){old[key]=infra[key];infra[key]*=factor}
    p.stxIFEconomicMobilization={...m,updatedAt:state.simTime};
    try{return STX_IF_tickPlanet(p,dt)}finally{for(const [key,value] of Object.entries(old))infra[key]=value}
  };
}

/* If a priority is short trained crew, training worlds and naval worlds start
   a gradual recruitment drive. Crew comes out of civilian population and
   consumes Equipment, then the existing freight system ships it normally. */
function stxIFCrewMobilization(dt){
  const d=stxIFPriorityProject();if(!d)return;const need=stxIFNeed(d,"trained");if(need<=.00002)return;
  const worlds=owned(0).filter(p=>!p.underAttack),available=worlds.reduce((n,p)=>n+Math.max(0,stxIFN(p.stock?.trained)-.00005),0);let gap=Math.max(0,need-available);if(gap<=.00002)return;
  const donors=worlds.filter(p=>(stxIFN(p.infra?.training)>0||stxIFN(p.infra?.shipyard)>0)&&stxIFN(p.pop)>.02&&stxIFN(p.stock?.equipment)>1).sort((a,b)=>(stxIFN(b.infra?.training)*2+stxIFN(b.infra?.shipyard))-(stxIFN(a.infra?.training)*2+stxIFN(a.infra?.shipyard)));
  for(const p of donors.slice(0,4)){
    if(gap<=.00001)break;const capacity=stxIFN(p.infra?.training)+stxIFN(p.infra?.shipyard)*.28,potential=Math.min(gap,capacity*.000055*dt,Math.max(0,p.pop-.018)*.004),gearLimit=stxIFN(p.stock?.equipment)/700,crew=Math.min(potential,gearLimit);if(crew<=.000001)continue;
    p.pop-=crew;p.stock.trained=stxIFN(p.stock?.trained)+crew;p.stock.equipment=Math.max(0,stxIFN(p.stock?.equipment)-crew*700);gap-=crew;p.stxIFMobilizedUntil=state.simTime+10;
    if(!p.stxIFCrewMobilizedAt||state.simTime-p.stxIFCrewMobilizedAt>45){p.stxIFCrewMobilizedAt=state.simTime;logEvent(`${p.name} is recruiting and training personnel for prioritized ${d.title}. Civilian population and Equipment are being committed.`,"warning")}
  }
}

/* EXPORT is also a real domestic routing instruction. If another player world
   has an uncovered local project shortage, surplus leaves on an actual cargo
   ship instead of teleporting between stockpiles. */
function stxIFRoutingTick(){
  if(state.simTime-stxIFLastRouting<3)return;stxIFLastRouting=state.simTime;
  for(const dest of owned(0).filter(p=>!p.underAttack))for(const r of STX_IF_RESOURCES){
    const short=Math.max(0,stxIFLocalNeed(dest,r)-stxIFN(dest.stock?.[r]));if(short<1)continue;
    if(state.ships.some(s=>s.stxIFRedistribution&&s.to===dest.id&&stxIFN(s.cargo?.[r])>0))continue;
    const source=owned(0).filter(p=>p!==dest&&!p.underAttack&&stxIFRoute(p,r)==="export").map(p=>({p,surplus:Math.max(0,stxIFN(p.stock?.[r])-(typeof stxRTPlanetReserve==="function"?stxRTPlanetReserve(p,r):12))})).filter(x=>x.surplus>2).sort((a,b)=>b.surplus-a.surplus)[0];
    if(!source)continue;const amount=Math.min(short,source.surplus,36);source.p.stock[r]-=amount;const type=r==="helium"?"tanker":r==="equipment"?"supply":"freighter",ship=createShip(type,source.p,dest,0,{cargo:{[r]:amount},vesselName:vesselName(type)});
    if(!ship){source.p.stock[r]+=amount;continue}ship.stxIFRedistribution=true;ship.commercial=false;ship.stxIFResource=r;break;
  }
}

/* -------------------------- Fortress upgrades -------------------------- */
function stxIFTierName(base,tier=base?.tier||1){const names={military:["Frontier Bastion","Sector Fortress","System Citadel"],trade:["Trade Anchorage","Orbital Exchange","Grand Commerce Ring"],logistics:["Logistics Relay","Fleet Waystation","Strategic Logistics Nexus"],sensor:["Listening Outpost","Deepwatch Array","Far-Reach Observatory"]};return(names[base?.type]||names.logistics)[clamp(tier,1,3)-1]}
function stxIFUpgradeCost(base,target=(base?.tier||1)+1){
  if(!base||target<2||target>3)return null;const original=stxDSBaseSpec(base).cost||{},mult=target===2?.78:1.28,out={};for(const [r,a] of Object.entries(original))out[r]=Math.max(3,Math.ceil(a*mult));out.components=Math.ceil(stxIFN(out.components)+target*8);out.equipment=Math.ceil(stxIFN(out.equipment)+target*7);out.titanium=Math.ceil(stxIFN(out.titanium)+target*10);if(target===3)out.rare=Math.ceil(stxIFN(out.rare)+18);return out;
}
function stxIFUpgradeRatio(q){if(!q)return 0;const total=Object.values(q.need||{}).reduce((n,v)=>n+stxIFN(v),0)||1,got=Object.entries(q.need||{}).reduce((n,[r,a])=>n+Math.min(stxIFN(a),stxIFN(q.delivered?.[r])),0);return clamp(got/total,0,1)}
function stxIFUpgradeReady(q){return!!q&&Object.entries(q.need||{}).every(([r,a])=>stxIFN(q.delivered?.[r])>=stxIFN(a)-.02)}
function stxIFStartUpgrade(baseId){
  const base=stxDSBase(baseId);if(!base||base.owner!==0||base.status!=="operational"||base.tier>=3||base.upgradeProject)return false;const sponsor=state.planets.find(p=>p.id===base.sponsorPlanetId),target=base.tier+1,need=stxIFUpgradeCost(base,target),e=empire(0);if(!sponsor||!need)return false;
  const credits=Math.ceil(Object.values(need).reduce((n,v)=>n+v,0)*.09);if(stxIFN(e.credits)<credits){showToast(`Need ${credits} credits to authorize the upgrade`);return false}e.credits-=credits;base.upgradeProject={id:`ifup${Math.floor(random()*1e9)}`,targetTier:target,need,delivered:Object.fromEntries(Object.keys(need).map(r=>[r,0])),progress:0,nextDispatchAt:state.simTime,shipmentIds:[],startedAt:state.simTime,credits};
  stxDSAddActivity(base,`${stxIFTierName(base,target)} upgrade authorized. The station stays operational while modules arrive.`,"good");logEvent(`${base.name} began its Tier ${target} upgrade. Materials must physically reach the station.`,"good");showToast(`${base.name} · Tier ${target} upgrade authorized`);stxDSPersist();renderPlanet();return true;
}
function stxIFUpgradeIncoming(base,r){return state.ships.filter(s=>s.stxDeepTransit&&s.deepBaseId===base.id&&s.stxDeepMission==="fortress-upgrade").reduce((n,s)=>n+stxIFN(s.cargo?.[r]),0)}
function stxIFSourceUpgrade(base){
  const q=base.upgradeProject,p=state.planets.find(x=>x.id===base.sponsorPlanetId);if(!q||!p||state.simTime<(q.nextDispatchAt||0))return;let inbound=state.ships.filter(s=>s.stxDeepTransit&&s.deepBaseId===base.id&&s.stxDeepMission==="fortress-upgrade").length;if(inbound>=3)return;
  const priority=stxIFPriorityProject();for(const [r,a] of Object.entries(q.need)){
    if(inbound>=3)break;const left=Math.max(0,stxIFN(a)-stxIFN(q.delivered[r])-stxIFUpgradeIncoming(base,r));if(left<.1)continue;if(priority&&stxIFNeed(priority,r)>.2)continue;
    const floor=Math.max(["components","equipment"].includes(r)?3:7,stxIFLocalNeed(p,r)),available=Math.max(0,stxIFN(p.stock?.[r])-floor),amount=Math.min(left,available,34);
    if(amount<=.1){let order=p.orders?.find(o=>o.stxFortressUpgradeId===base.id&&o.resource===r&&o.status!=="filled");if(!order){order=addOrder(p,"fortress-upgrade",r,left,5,`${base.name} Tier ${q.targetTier} upgrade`);if(order)order.stxFortressUpgradeId=base.id}continue}
    p.stock[r]-=amount;const ship=stxDSCreateTransit({type:q.shipmentIds.length?"freighter":"construction",owner:0,fromPoint:p,targetPoint:base,fromPlanetId:p.id,baseId:base.id,mission:"fortress-upgrade",cargo:{[r]:amount},vesselName:q.shipmentIds.length?vesselName("freighter"):`${p.name} Fortress Engineering Convoy`,speedBoost:.94});if(!ship){p.stock[r]+=amount;break}q.shipmentIds.push(ship.id);inbound++;stxDSAddActivity(base,`${ship.vesselName} departed with ${Math.round(amount)} ${stxIFLabel(r)}.`,"good");
  }
  q.nextDispatchAt=state.simTime+3.4;
}
function stxIFArriveUpgrade(ship,base){const q=base?.upgradeProject;if(!q)return;for(const [r,v] of Object.entries(ship.cargo||{}))q.delivered[r]=Math.min(stxIFN(q.need[r],Infinity),stxIFN(q.delivered[r])+stxIFN(v));stxDSAddActivity(base,`${ship.vesselName} delivered fortress modules.`,"good")}
if(typeof stxDSHandleTransitArrival==="function"){const STX_IF_arrival=stxDSHandleTransitArrival;stxDSHandleTransitArrival=function(s){if(s?.stxDeepMission==="fortress-upgrade")return stxIFArriveUpgrade(s,stxDSBase(s.deepBaseId));return STX_IF_arrival(s)}}
function stxIFCompleteUpgrade(base){const q=base.upgradeProject;if(!q)return;const oldMax=base.maxHp;base.tier=q.targetTier;base.maxHp=Math.max(oldMax+48+(base.tier-2)*22,stxDSBaseSpec(base).maxHp+(base.tier-1)*58);base.hp=Math.min(base.maxHp,base.hp+(base.maxHp-oldMax)*.75);base.supplyReadiness=clamp(base.supplyReadiness+.14,0,1);delete base.upgradeProject;stxDSAddActivity(base,`${stxIFTierName(base)} commissioned.`,"good");logEvent(`${base.name} is now a Tier ${base.tier} ${stxIFTierName(base)}.`,"good");galacticNews(`${base.name.toUpperCase()} EXPANDS INTO A ${stxIFTierName(base).toUpperCase()}`,`The Tier ${base.tier} facility completed physical module installation and commissioning.`,"good",base.sponsorPlanetId);showToast(`${base.name} · Tier ${base.tier} operational`);stxDSPersist()}
function stxIFUpgradeTick(dt){for(const base of stxDSBases().filter(b=>b.status==="operational"&&b.upgradeProject)){const q=base.upgradeProject;stxIFSourceUpgrade(base);const ratio=stxIFUpgradeRatio(q);q.progress=Math.max(q.progress,ratio*.55);if(ratio>.18)q.progress=Math.min(stxIFUpgradeReady(q)?.97:.91,q.progress+dt*.008*(1+base.tier*.16)*Math.max(.35,ratio));base.supplyReadiness=clamp(base.supplyReadiness-dt*.00035,0,1);if(stxIFUpgradeReady(q)&&q.progress>=.95)stxIFCompleteUpgrade(base)}}

/* Upgrade freight yields to an active player priority instead of stealing the
   same resource out from under it. */
if(typeof fillOrder==="function"){const STX_IF_fillOrder=fillOrder;fillOrder=function(dest,o){if(o?.stxFortressUpgradeId){const p=stxIFPriorityProject();if(p&&stxIFNeed(p,o.resource)>.2)return}return STX_IF_fillOrder(dest,o)}}

/* Keep the existing station glyph as a central core, then add large modular
   rings, docking arms, cargo pods, dishes and fortress batteries around it. */
if(typeof stxDSDrawBase==="function"){
  const STX_IF_drawBase=stxDSDrawBase;
  stxDSDrawBase=function(base){STX_IF_drawBase(base);if(!base||base.status!=="operational"||state.camera.zoom<.34||!visible(base.x,base.y,180))return;const spec=stxDSBaseSpec(base),s=worldToScreen(base.x,base.y),z=state.camera.zoom,t=performance.now()/1000,tier=base.tier||1,size=clamp((10+tier*4)*Math.max(.7,z),8,34),spin=t*.018+(stxDSHash(base.id)%80)*.01;ctx.save();ctx.translate(s.x,s.y);ctx.rotate(spin);ctx.strokeStyle=spec.color;ctx.fillStyle="rgba(5,11,22,.92)";ctx.lineWidth=1.25;ctx.shadowColor=spec.color;ctx.shadowBlur=9;
    ctx.beginPath();ctx.ellipse(0,0,size*(1.05+tier*.14),size*(.42+tier*.04),0,0,6.283);ctx.stroke();if(tier>=2){ctx.globalAlpha=.68;ctx.beginPath();ctx.ellipse(0,0,size*(1.5+tier*.08),size*(.61+tier*.03),0,0,6.283);ctx.stroke();ctx.globalAlpha=1}
    const arms=base.type==="military"?4+tier*2:base.type==="trade"?5+tier:base.type==="logistics"?4+tier:3+tier;for(let i=0;i<arms;i++){const a=i/arms*6.283,r=size*(tier>=2?1.42:1.16);ctx.save();ctx.rotate(a);ctx.beginPath();ctx.moveTo(size*.4,0);ctx.lineTo(r,0);ctx.stroke();if(base.type==="military"){ctx.fillRect(r-size*.22,-size*.17,size*.44,size*.34);ctx.strokeRect(r-size*.22,-size*.17,size*.44,size*.34);ctx.fillStyle="#ffd0a0";ctx.fillRect(r+size*.12,-1,size*(.24+tier*.05),2)}else if(base.type==="trade"){ctx.fillRect(r-size*.24,-size*.15,size*.48,size*.3);ctx.strokeRect(r-size*.24,-size*.15,size*.48,size*.3);ctx.fillStyle=i%2?"#ffd56f":"#75eaff";ctx.fillRect(r-1,-1,3,2)}else if(base.type==="logistics"){ctx.fillRect(r-size*.27,-size*.18,size*.54,size*.36);ctx.strokeRect(r-size*.27,-size*.18,size*.54,size*.36)}else{ctx.beginPath();ctx.arc(r,0,size*.22,-1.2,1.2);ctx.stroke();ctx.beginPath();ctx.moveTo(r-size*.2,0);ctx.lineTo(r+size*.35,0);ctx.stroke()}ctx.restore()}
    if(base.type==="military"){for(let i=0;i<tier;i++){ctx.globalAlpha=.45;ctx.beginPath();ctx.arc(0,0,size*(1.75+i*.2),0,6.283);ctx.stroke()}ctx.globalAlpha=1}else if(base.type==="sensor"){ctx.rotate(-spin*1.7);ctx.beginPath();ctx.arc(0,-size*.1,size*(1.55+tier*.14),-2.7,-.45);ctx.stroke();ctx.beginPath();ctx.moveTo(0,0);ctx.lineTo(size*1.55,-size*.78);ctx.stroke()}
    if(base.upgradeProject){ctx.strokeStyle="#fff";ctx.setLineDash([3,4]);ctx.globalAlpha=.55;ctx.beginPath();ctx.arc(0,0,size*2.05,-1.57,-1.57+6.283*stxIFUpgradeRatio(base.upgradeProject));ctx.stroke();ctx.setLineDash([])}ctx.restore();
  };
}

function stxIFUpgradePanel(base){
  if(!base||base.owner!==0||base.status!=="operational")return"";if(base.tier>=3&&!base.upgradeProject)return`<div class="stx-if-upgrade"><b>TIER 3 · ${stxIFTierName(base)}</b><small>Maximum fortress tier.</small></div>`;const q=base.upgradeProject,target=q?.targetTier||base.tier+1,cost=q?.need||stxIFUpgradeCost(base,target),ratio=q?stxIFUpgradeRatio(q):0;
  return`<div class="stx-if-upgrade"><b>${q?`UPGRADING TO TIER ${target}`:`TIER ${target} · ${stxIFTierName(base,target)}`}</b><small>${q?`${Math.round(ratio*100)}% of modules physically delivered; station remains operational.`:"Adds integrity, service capacity, regional support and a larger station/fortress silhouette."}</small>${q?`<div class="stx-if-progress"><i style="width:${Math.round(ratio*100)}%"></i></div>`:`<button class="choice-btn primary-choice" data-stx-if-upgrade="${base.id}">Authorize Upgrade</button>`}<div class="stx-if-cost">${Object.entries(cost||{}).map(([r,a])=>`<span>${stxIFLabel(r)} ${Math.ceil(a)}</span>`).join("")}</div></div>`;
}
if(typeof stxDSBasePanel==="function"){const STX_IF_basePanel=stxDSBasePanel;stxDSBasePanel=function(base){const html=STX_IF_basePanel(base);return html&&base?.status==="operational"?html.replace(/<\/section>\s*$/,`${stxIFUpgradePanel(base)}</section>`):html}}

function stxIFPlanetCard(p){
  if(!p||p.owner!==0)return"";const priority=stxIFPriorityProject(),expansion=stxIFExpansionLoad(p),m=p.stxIFEconomicMobilization||{},ships=state.ships||[],inbound=ships.filter(s=>s.to===p.id).reduce((n,s)=>n+Object.values(s.cargo||{}).reduce((a,v)=>a+stxIFN(v),0),0),outbound=ships.filter(s=>s.from===p.id).reduce((n,s)=>n+Object.values(s.cargo||{}).reduce((a,v)=>a+stxIFN(v),0),0),commit=STX_IF_RESOURCES.reduce((n,r)=>n+stxIFLocalNeed(p,r),0),priorityText=priority?(priority.p===p?`PRIORITY DESTINATION · ${priority.title}`:(m.label||"Contributing to Imperial priority")):"No Imperial priority active";
  return`<section class="stx-if-econ"><div class="section-label">Integrated Economic Flow</div><div class="stx-if-metrics"><span>Committed <b>${Math.round(commit)}</b></span><span>Inbound <b>${Math.round(inbound)}</b></span><span>Outbound <b>${Math.round(outbound)}</b></span><span>Expansion <b>${expansion?`${Math.round(expansion*100)}% load`:"none"}</b></span></div><div class="stx-if-priority"><b>${priorityText}</b><small>${priority?"Priority pulls real stock, production and personnel from the empire; delivery still takes time.":"Prioritize an active project to give it guaranteed first claim on domestic supply."}</small></div><div class="stx-if-routes">${STX_IF_RESOURCES.map(r=>{const mode=stxIFRoute(p,r);return`<button data-stx-if-route="${r}" class="${mode}"><b>${stxIFLabel(r)}</b><span>${Math.round(stxIFN(p.stock?.[r]))}</span><small>${mode.toUpperCase()}</small></button>`}).join("")}</div><p class="subtle">Click a resource to cycle BALANCED → PROJECT → EXPORT. PROJECT holds a larger commercial reserve; EXPORT actively sends surplus toward other worlds with project shortages.</p></section>`;
}
if(typeof renderPlanet==="function"){const STX_IF_renderPlanet=renderPlanet;renderPlanet=function(){STX_IF_renderPlanet();const p=state.selected,body=$("planetBody");if(!p||!body)return;if(p.owner===0)body.insertAdjacentHTML("beforeend",stxIFPlanetCard(p));body.querySelectorAll?.("[data-stx-if-route]").forEach(b=>b.onclick=e=>{e.preventDefault();e.stopPropagation();stxIFCycleRoute(p.id,b.dataset.stxIfRoute)});body.querySelectorAll?.("[data-stx-if-upgrade]").forEach(b=>b.onclick=e=>{e.preventDefault();e.stopPropagation();stxIFStartUpgrade(b.dataset.stxIfUpgrade)})}}

function stxIFTick(){const now=state.simTime,dt=Math.max(0,Math.min(1.2,now-stxIFLastTick));stxIFLastTick=now;if(dt<=0)return;state.planets.forEach(stxIFEnsurePlanet);stxIFCrewMobilization(dt);stxIFRoutingTick();stxIFUpgradeTick(dt)}
if(typeof simulate==="function"){const STX_IF_simulate=simulate;simulate=function(dt){const result=STX_IF_simulate(dt);if(state.simTime-stxIFLastTick>=.45)stxIFTick();return result}}

function stxIFStyles(){if(document.getElementById?.("stxIFStyles"))return;const s=document.createElement("style");s.id="stxIFStyles";s.textContent=`.stx-if-econ{margin-top:12px;padding:10px;border:1px solid rgba(112,233,255,.2);border-radius:9px;background:rgba(5,13,24,.86)}.stx-if-metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:5px;margin:6px 0}.stx-if-metrics span{padding:6px;background:rgba(255,255,255,.035);border-radius:6px;font-size:7px;color:#7890a5}.stx-if-metrics b{display:block;color:#e9f8ff;font-size:11px}.stx-if-priority{padding:7px;border-left:2px solid #ffd56f;background:rgba(255,213,111,.045);margin:6px 0}.stx-if-priority b,.stx-if-priority small{display:block}.stx-if-priority small{color:#8da3b7;margin-top:2px}.stx-if-routes{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:4px}.stx-if-routes button{padding:6px 5px;text-align:left;background:#091321;border:1px solid rgba(255,255,255,.09);border-radius:6px;color:#bed1df;cursor:pointer}.stx-if-routes button b,.stx-if-routes button span,.stx-if-routes button small{display:block;overflow:hidden;text-overflow:ellipsis}.stx-if-routes button span{color:#fff;font-size:11px}.stx-if-routes button small{font-size:7px;color:#728a9f}.stx-if-routes button.project{border-color:rgba(255,213,111,.42)}.stx-if-routes button.export{border-color:rgba(112,233,255,.45)}.stx-if-upgrade{margin-top:10px;padding:9px;border:1px solid rgba(140,205,235,.2);border-radius:8px;background:rgba(5,12,24,.76)}.stx-if-upgrade>b,.stx-if-upgrade>small{display:block}.stx-if-upgrade>small{color:#8ca3b7;margin:3px 0 7px}.stx-if-cost{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}.stx-if-cost span{font-size:7px;padding:3px 5px;border-radius:9px;background:rgba(255,255,255,.05);color:#a9bfd0}.stx-if-progress{height:5px;background:#111d2a;border-radius:5px;overflow:hidden}.stx-if-progress i{display:block;height:100%;background:#75eaff;box-shadow:0 0 8px rgba(117,234,255,.5)}@media(max-width:760px){.stx-if-metrics,.stx-if-routes{grid-template-columns:repeat(2,minmax(0,1fr))}}`;document.head.appendChild(s)}
stxIFStyles();

globalThis.SpaceTyrantsIntegratedEconomy={version:STX_IF_VERSION,route:stxIFRoute,setRoute:stxIFSetRoute,expansionLoad:stxIFExpansionLoad,mobilization:stxIFMobilization,tierName:stxIFTierName,upgradeCost:stxIFUpgradeCost,startUpgrade:stxIFStartUpgrade,upgradeRatio:stxIFUpgradeRatio,tick:stxIFTick};
