/* Space Tyrants — integrated economic logistics, guaranteed Imperial priorities,
   expansion opportunity costs, per-world resource routing, and fortress-grade
   deep-space station upgrades/visuals.

   Design rules:
   - No project receives free materials. Priority reallocates real production,
     stockpiles, labor and freight capacity.
   - Expansion is strategically valuable but temporarily consumes sponsor-world
     industrial capacity until the colony mission is completed.
   - Player resource routing is local and legible: keep for projects, balance,
     or export surplus. Existing physical freight remains the delivery system.
   - Deep-space station upgrades are physical projects whose cargo must reach
     the station before the new tier becomes operational. */

const STX_IF_VERSION=1;
const STX_IF_ROUTE_RESOURCES=["iron","silicates","titanium","helium","rare","components","equipment"];
const STX_IF_ROUTE_MODES=["balanced","project","export"];
const STX_IF_ROUTE_LABEL={balanced:"BAL",project:"PROJECT",export:"EXPORT"};
const STX_IF_RAW=new Set(["iron","silicates","titanium","helium","rare"]);
let stxIFLastTick=-999;

function stxIFNum(n,d=0){return Number.isFinite(Number(n))?Number(n):d}
function stxIFPct(n){return `${Math.round(clamp(stxIFNum(n),0,1)*100)}%`}
function stxIFResourceLabel(r){return typeof stxSDResourceLabel==="function"?stxSDResourceLabel(r):(RESOURCE_LABEL?.[r]||r)}
function stxIFProjectPriority(){return typeof stxPSPriorityProject==="function"?stxPSPriorityProject(0):null}
function stxIFEnsurePlanet(p){if(!p)return p;p.stxResourceRouting=p.stxResourceRouting||{};return p}
function stxIFRoute(p,r){stxIFEnsurePlanet(p);return STX_IF_ROUTE_MODES.includes(p.stxResourceRouting[r])?p.stxResourceRouting[r]:"balanced"}
function stxIFSetRoute(planetId,r,mode){
  const p=state.planets.find(x=>x.id===planetId);if(!p||p.owner!==0||!STX_IF_ROUTE_RESOURCES.includes(r)||!STX_IF_ROUTE_MODES.includes(mode))return false;
  stxIFEnsurePlanet(p);p.stxResourceRouting[r]=mode;p.mandateGlow=1;
  showToast(`${p.name} · ${stxIFResourceLabel(r)} routing: ${mode}`);renderPlanet();return true;
}
function stxIFCycleRoute(planetId,r){const p=state.planets.find(x=>x.id===planetId);if(!p)return false;const current=stxIFRoute(p,r),i=STX_IF_ROUTE_MODES.indexOf(current);return stxIFSetRoute(planetId,r,STX_IF_ROUTE_MODES[(i+1)%STX_IF_ROUTE_MODES.length])}

function stxIFDescriptorNeeds(d){
  const result={};if(!d)return result;
  for(const [r,a] of Object.entries(d.need||{})){
    const remaining=typeof stxSDRemaining==="function"?stxSDRemaining(d,r):Math.max(0,stxIFNum(a)-stxIFNum(d.q?.stxSupply?.delivered?.[r]));
    const incoming=typeof stxSDIncomingAmount==="function"?stxSDIncomingAmount(d,r):0;
    result[r]=Math.max(0,remaining-incoming);
  }
  return result;
}
function stxIFLocalNeed(p,r){
  if(typeof stxSDDescriptors!=="function")return 0;
  return stxSDDescriptors(p).reduce((n,d)=>n+Math.max(0,stxIFNum(d.need?.[r])-stxIFNum(d.q?.stxSupply?.delivered?.[r])),0);
}
function stxIFExpansionDescriptor(p){return typeof stxSDDescriptors==="function"?stxSDDescriptors(p).find(d=>d.kind==="expansion")||null:null}
function stxIFExpansionBurden(p){
  const d=stxIFExpansionDescriptor(p);if(!d)return 0;
  const ceiling=typeof stxSDCeiling==="function"?stxSDCeiling(d):clamp(stxIFNum(d.q?.progress),0,1);
  // Mobilization is strongest during launch preparation and eases as the
  // colony project approaches completion.
  return clamp(.35+(1-ceiling)*.65,.35,1);
}

/* Per-world routing modifies only discretionary reserves. Open local project
   commitments remain protected even when the world is marked EXPORT. */
if(typeof stxRTPlanetReserve==="function"){
  const STX_IF_stxRTPlanetReserve=stxRTPlanetReserve;
  stxRTPlanetReserve=function(p,r){
    const base=STX_IF_stxRTPlanetReserve(p,r);if(!p||p.owner!==0)return base;
    const local=stxIFLocalNeed(p,r),mode=stxIFRoute(p,r);
    if(mode==="project")return Math.max(base,local+(typeof stxRTReserve==="function"?stxRTReserve(r):18)*.65);
    if(mode==="export")return Math.max(local,base*.55);
    return base;
  };
}

/* Player priority is a guarantee of routing, not instant completion. Donor
   worlds will release much deeper reserves, but the destination still waits
   for a real ship to carry the material. */
if(typeof stxSDReserveFor==="function"){
  const STX_IF_stxSDReserveFor=stxSDReserveFor;
  stxSDReserveFor=function(resource,amount,priority=1){
    if(priority>=100){
      if(resource==="trained")return Math.min(.00003,Math.max(.000005,stxIFNum(amount)*.04));
      if(resource==="components"||resource==="equipment")return Math.max(1.5,stxIFNum(amount)*.045);
      return Math.max(3,stxIFNum(amount)*.055);
    }
    return STX_IF_stxSDReserveFor(resource,amount,priority);
  };
}

function stxIFMobilizationProfile(p){
  const result={mine:1,factory:1,training:1,research:1,label:""},priority=stxIFProjectPriority();
  const expansion=stxIFExpansionBurden(p);
  if(expansion>0){
    result.mine*=1-.18*expansion;result.factory*=1-.30*expansion;result.training*=1-.22*expansion;result.research*=1-.12*expansion;
    result.label="Colony mobilization";
  }
  if(!priority||p.owner!==0||p===priority.p)return result;
  const need=stxIFDescriptorNeeds(priority),raw=Object.entries(need).filter(([r,a])=>STX_IF_RAW.has(r)&&a>.2).sort((a,b)=>b[1]-a[1])[0];
  const manufactured=(need.components||0)+(need.equipment||0),crew=need.trained||0;
  if(raw){
    const [r]=raw,good=p.stxResourcePrimary===r||p.stxResourceSecondary===r||stxIFNum(p.quality?.[r])>=4;
    if(good&&stxIFNum(p.infra?.mine)>0){result.mine*=1.28;result.factory*=.9;result.label=`Priority ${stxIFResourceLabel(r)} extraction`}
  }
  if(manufactured>.3&&stxIFNum(p.infra?.factory)>0){result.factory*=1.22;result.mine*=.94;result.label="Priority industrial production"}
  if(crew>.00002&&(stxIFNum(p.infra?.training)>0||stxIFNum(p.infra?.shipyard)>0)){result.training*=1.45;result.factory*=.92;result.label="Priority personnel mobilization"}
  return result;
}

/* Apply opportunity costs through existing production functions by temporarily
   changing effective infrastructure capacity, then restoring the real tier. */
if(typeof tickPlanet==="function"){
  const STX_IF_tickPlanet=tickPlanet;
  tickPlanet=function(p,dt){
    if(!p||p.owner===null)return STX_IF_tickPlanet(p,dt);stxIFEnsurePlanet(p);
    const m=stxIFMobilizationProfile(p),infra=p.infra||{},original={};
    for(const [key,factor] of [["mine",m.mine],["factory",m.factory],["training",m.training],["research",m.research]])if(Number.isFinite(infra[key])){original[key]=infra[key];infra[key]=infra[key]*factor}
    p.stxIFEconomicMobilization={...m,updatedAt:state.simTime};
    try{return STX_IF_tickPlanet(p,dt)}finally{for(const [key,value] of Object.entries(original))infra[key]=value}
  };
}

function stxIFPriorityCrewTick(dt){
  const d=stxIFProjectPriority();if(!d||d.p.owner!==0)return;
  const need=stxIFDescriptorNeeds(d),crewGap=need.trained||0;if(crewGap<=.00002)return;
  const worlds=owned(0).filter(p=>p!==d.p&&!p.underAttack),ready=worlds.reduce((n,p)=>n+Math.max(0,stxIFNum(p.stock?.trained)-.00005),0);
  let gap=Math.max(0,crewGap-ready);if(gap<=.00002)return;
  const donors=worlds.filter(p=>(stxIFNum(p.infra?.training)>0||stxIFNum(p.infra?.shipyard)>0)&&stxIFNum(p.pop)>.02&&stxIFNum(p.stock?.equipment)>1).sort((a,b)=>(stxIFNum(b.infra.training)*2+stxIFNum(b.infra.shipyard))-(stxIFNum(a.infra.training)*2+stxIFNum(a.infra.shipyard)));
  for(const p of donors.slice(0,4)){
    if(gap<=.00001)break;
    const capacity=stxIFNum(p.infra.training)+stxIFNum(p.infra.shipyard)*.28;
    const potential=Math.min(gap,capacity*.000055*dt,Math.max(0,p.pop-.018)*.004);
    const gearPerCrew=700,gearLimit=stxIFNum(p.stock.equipment)/gearPerCrew,crew=Math.min(potential,gearLimit);
    if(crew<=.000001)continue;
    p.pop=Math.max(.001,p.pop-crew);p.stock.trained=stxIFNum(p.stock.trained)+crew;p.stock.equipment=Math.max(0,p.stock.equipment-crew*gearPerCrew);gap-=crew;
    if(!p.stxIFCrewMobilizedAt||state.simTime-p.stxIFCrewMobilizedAt>45){
      p.stxIFCrewMobilizedAt=state.simTime;logEvent(`${p.name} is mobilizing personnel for the prioritized ${d.title}. Training capacity, civilian population, and Equipment are being committed.`,"warning");
    }
    p.stxIFMobilizedUntil=state.simTime+10;
  }
}

/* ------------------------ Deep-space fortress tiers ----------------------- */
function stxIFTierLabel(base,tier=base?.tier||1){
  const names={military:["Frontier Bastion","Sector Fortress","System Citadel"],trade:["Trade Anchorage","Orbital Exchange","Grand Commerce Ring"],logistics:["Logistics Relay","Fleet Waystation","Strategic Logistics Nexus"],sensor:["Listening Outpost","Deepwatch Array","Far-Reach Observatory"]};
  return(names[base?.type]||names.logistics)[clamp(tier,1,3)-1];
}
function stxIFUpgradeCost(base,targetTier=(base?.tier||1)+1){
  if(!base||targetTier<2||targetTier>3)return null;
  const seed={...(stxDSBaseSpec(base).cost||{})},mult=targetTier===2?.78:1.28,cost={};
  for(const [r,a] of Object.entries(seed))cost[r]=Math.max(3,Math.ceil(a*mult));
  cost.components=Math.ceil(stxIFNum(cost.components)+targetTier*8);
  cost.equipment=Math.ceil(stxIFNum(cost.equipment)+targetTier*7);
  cost.titanium=Math.ceil(stxIFNum(cost.titanium)+targetTier*10);
  if(targetTier===3)cost.rare=Math.ceil(stxIFNum(cost.rare)+18);
  return cost;
}
function stxIFUpgradeRatio(q){if(!q)return 0;const total=Object.values(q.need||{}).reduce((n,v)=>n+stxIFNum(v),0)||1,got=Object.entries(q.need||{}).reduce((n,[r,v])=>n+Math.min(stxIFNum(v),stxIFNum(q.delivered?.[r])),0);return clamp(got/total,0,1)}
function stxIFUpgradeReady(q){return!!q&&Object.entries(q.need||{}).every(([r,a])=>stxIFNum(q.delivered?.[r])>=stxIFNum(a)-.02)}
function stxIFStartUpgrade(baseId){
  const base=stxDSBase(baseId);if(!base||base.owner!==0||base.status!=="operational"||base.upgradeProject||base.tier>=3)return false;
  const targetTier=base.tier+1,need=stxIFUpgradeCost(base,targetTier),sponsor=state.planets.find(p=>p.id===base.sponsorPlanetId),e=empire(base.owner);if(!need||!sponsor)return false;
  const credits=Math.ceil(Object.values(need).reduce((n,v)=>n+v,0)*.09);if(stxIFNum(e.credits)<credits){showToast(`Need ${credits} credits to authorize the upgrade`);return false}
  e.credits-=credits;base.upgradeProject={id:`ifup${Math.floor(random()*1e9)}`,targetTier,need,delivered:Object.fromEntries(Object.keys(need).map(r=>[r,0])),progress:0,startedAt:state.simTime,nextDispatchAt:state.simTime,shipmentIds:[],credits};
  for(const [r,a] of Object.entries(need))if(stxIFNum(sponsor.stock?.[r])<a*.6)addOrder(sponsor,`fortress-upgrade-${base.id}`,r,a,8,`${base.name} Tier ${targetTier} upgrade`);
  stxDSAddActivity(base,`${stxIFTierLabel(base,targetTier)} upgrade authorized. The station remains operational while modules are shipped and installed.`,"good");
  logEvent(`${base.name} began its Tier ${targetTier} upgrade. Real cargo must reach the station before commissioning.`,"good");showToast(`${base.name} · Tier ${targetTier} upgrade authorized`);stxDSPersist();renderPlanet();return true;
}
function stxIFUpgradeIncoming(base,r=null){return state.ships.filter(s=>s.stxDeepTransit&&s.deepBaseId===base.id&&s.stxDeepMission==="fortress-upgrade").reduce((n,s)=>n+(r?stxIFNum(s.cargo?.[r]):Object.values(s.cargo||{}).reduce((a,v)=>a+stxIFNum(v),0)),0)}
function stxIFSourceUpgrade(base){
  const q=base.upgradeProject,p=state.planets.find(x=>x.id===base.sponsorPlanetId);if(!q||!p||base.status!=="operational"||state.simTime<(q.nextDispatchAt||0))return;
  let inbound=state.ships.filter(s=>s.stxDeepTransit&&s.deepBaseId===base.id&&s.stxDeepMission==="fortress-upgrade").length;if(inbound>=3)return;
  const items=Object.entries(q.need).map(([r,a])=>({r,left:Math.max(0,stxIFNum(a)-stxIFNum(q.delivered[r])-stxIFUpgradeIncoming(base,r))})).filter(x=>x.left>.1).sort((a,b)=>b.left-a.left);
  for(const item of items){
    if(inbound>=3)break;const localNeed=stxIFLocalNeed(p,item.r),floor=Math.max(["components","equipment"].includes(item.r)?3:7,localNeed),available=Math.max(0,stxIFNum(p.stock?.[item.r])-floor),amount=Math.min(item.left,available,34);
    if(amount<=.1){const open=p.orders?.some(o=>o.status!=="filled"&&o.type===`fortress-upgrade-${base.id}`&&o.resource===item.r);if(!open)addOrder(p,`fortress-upgrade-${base.id}`,item.r,item.left,8,`${base.name} Tier ${q.targetTier} upgrade`);continue}
    p.stock[item.r]-=amount;const ship=stxDSCreateTransit({type:q.shipmentIds.length?"freighter":"construction",owner:base.owner,fromPoint:p,targetPoint:base,fromPlanetId:p.id,baseId:base.id,mission:"fortress-upgrade",cargo:{[item.r]:amount},vesselName:q.shipmentIds.length?vesselName("freighter"):`${p.name} Fortress Engineering Convoy`,speedBoost:.94});
    if(!ship){p.stock[item.r]+=amount;break}q.shipmentIds.push(ship.id);inbound++;stxDSAddActivity(base,`${ship.vesselName} departed ${p.name} with ${Math.round(amount)} ${stxIFResourceLabel(item.r)} for the Tier ${q.targetTier} upgrade.`,"good");
  }
  q.nextDispatchAt=state.simTime+3.4;
}
function stxIFArriveUpgrade(ship,base){
  const q=base?.upgradeProject;if(!q)return;
  for(const [r,v] of Object.entries(ship.cargo||{}))q.delivered[r]=Math.min(stxIFNum(q.need[r],Infinity),stxIFNum(q.delivered[r])+stxIFNum(v));
  stxDSAddActivity(base,`${ship.vesselName} delivered upgrade modules: ${Object.entries(ship.cargo||{}).map(([r,v])=>`${Math.round(v)} ${stxIFResourceLabel(r)}`).join(", ")}.`,"good");
}
if(typeof stxDSHandleTransitArrival==="function"){
  const STX_IF_stxDSHandleTransitArrival=stxDSHandleTransitArrival;
  stxDSHandleTransitArrival=function(s){if(s?.stxDeepMission==="fortress-upgrade")return stxIFArriveUpgrade(s,stxDSBase(s.deepBaseId));return STX_IF_stxDSHandleTransitArrival(s)};
}
function stxIFCompleteUpgrade(base){
  const q=base.upgradeProject;if(!q)return;base.tier=q.targetTier;const oldMax=base.maxHp;base.maxHp=Math.max(oldMax+48+(base.tier-2)*22,stxDSBaseSpec(base).maxHp+(base.tier-1)*58);base.hp=Math.min(base.maxHp,base.hp+(base.maxHp-oldMax)*.75);base.supplyReadiness=clamp(base.supplyReadiness+.14,0,1);delete base.upgradeProject;
  stxDSAddActivity(base,`${stxIFTierLabel(base)} commissioned at Tier ${base.tier}. Docking, defense and regional support capacity increased.`,"good");
  logEvent(`${base.name} is now a Tier ${base.tier} ${stxIFTierLabel(base)}.`,"good");galacticNews(`${base.name.toUpperCase()} EXPANDS INTO A ${stxIFTierLabel(base).toUpperCase()}`,`The Tier ${base.tier} facility in the ${base.region} completed physical module installation and commissioning.`,"good",base.sponsorPlanetId);showToast(`${base.name} · Tier ${base.tier} operational`);stxDSPersist();
}
function stxIFTickUpgrades(dt){
  for(const base of stxDSBases().filter(b=>b.status==="operational"&&b.upgradeProject)){
    const q=base.upgradeProject;stxIFSourceUpgrade(base);const ratio=stxIFUpgradeRatio(q);q.progress=Math.max(q.progress,ratio*.48);
    if(ratio>.18)q.progress=Math.min(stxIFUpgradeReady(q)?.97:.91,q.progress+dt*.0032*(1+base.tier*.2)*Math.max(.35,ratio));
    // Keeping a station online during construction has a modest supply burden.
    base.supplyReadiness=clamp(base.supplyReadiness-dt*.00035,0,1);
    if(stxIFUpgradeReady(q)&&q.progress>=.95)stxIFCompleteUpgrade(base);
  }
}

/* Visual pass: turn tiny glyphs into recognizable modular stations. Wreck and
   initial construction visuals stay with the proven original renderer. */
if(typeof stxDSDrawBase==="function"){
  const STX_IF_stxDSDrawBase=stxDSDrawBase;
  stxDSDrawBase=function(base){
    if(base?.status!=="operational")return STX_IF_stxDSDrawBase(base);if(!visible(base.x,base.y,180))return;
    const spec=stxDSBaseSpec(base),s=worldToScreen(base.x,base.y),z=state.camera.zoom,t=performance.now()/1000,tier=base.tier||1,selected=state.stxSelectedDeepBaseId===base.id,size=clamp((9+tier*3.8)*Math.max(.7,z),7,34),spin=t*.025+(stxDSHash(base.id)%100)*.01;
    ctx.save();
    if(selected){ctx.strokeStyle=spec.color;ctx.globalAlpha=.38;ctx.setLineDash([6,8]);ctx.beginPath();ctx.arc(s.x,s.y,Math.max(34,250*z),0,6.283);ctx.stroke();ctx.setLineDash([]);ctx.globalAlpha=1}
    ctx.translate(s.x,s.y);ctx.rotate(spin);ctx.lineWidth=clamp(1.1*z,1,2.2);ctx.strokeStyle=spec.color;ctx.fillStyle="rgba(5,11,22,.96)";ctx.shadowColor=spec.color;ctx.shadowBlur=selected?22:12;
    // armored central pressure hull
    ctx.beginPath();ctx.arc(0,0,size*.48,0,6.283);ctx.fill();ctx.stroke();
    ctx.fillStyle="rgba(175,220,255,.28)";for(let i=0;i<6+tier*2;i++){const a=i/(6+tier*2)*6.283;ctx.fillRect(Math.cos(a)*size*.42-1,Math.sin(a)*size*.42-1,2,2)}
    // primary rotating habitat/defense ring
    ctx.strokeStyle=spec.color;ctx.lineWidth=1.5;ctx.beginPath();ctx.ellipse(0,0,size*(1.05+tier*.12),size*(.42+tier*.035),0,0,6.283);ctx.stroke();
    if(tier>=2){ctx.globalAlpha=.68;ctx.beginPath();ctx.ellipse(0,0,size*(1.42+tier*.1),size*(.58+tier*.035),0,0,6.283);ctx.stroke();ctx.globalAlpha=1}
    const arms=base.type==="military"?4+tier*2:base.type==="trade"?5+tier:base.type==="logistics"?4+tier:3+tier;
    for(let i=0;i<arms;i++){
      const a=i/arms*6.283,r=size*(tier>=2?1.35:1.12),x=Math.cos(a)*r,y=Math.sin(a)*r*.62;ctx.save();ctx.rotate(a);ctx.strokeStyle=spec.color;ctx.beginPath();ctx.moveTo(size*.38,0);ctx.lineTo(r*.88,0);ctx.stroke();
      if(base.type==="military"){ctx.fillStyle="rgba(12,20,34,.98)";ctx.strokeRect(r*.82-size*.22,-size*.18,size*.44,size*.36);ctx.fillRect(r*.82-size*.22,-size*.18,size*.44,size*.36);ctx.fillStyle="#ffcf9a";ctx.fillRect(r*.98,-1,size*(.25+tier*.05),2)}
      else if(base.type==="trade"){ctx.fillStyle="rgba(14,24,38,.98)";ctx.fillRect(r*.82-size*.24,-size*.16,size*.48,size*.32);ctx.strokeRect(r*.82-size*.24,-size*.16,size*.48,size*.32);ctx.fillStyle=i%2?"#ffd56f":"#75eaff";ctx.fillRect(r*.94,-1,3,2)}
      else if(base.type==="logistics"){ctx.fillStyle=i%2?"rgba(75,145,170,.75)":"rgba(20,30,46,.98)";ctx.fillRect(r*.78-size*.25,-size*.19,size*.5,size*.38);ctx.strokeRect(r*.78-size*.25,-size*.19,size*.5,size*.38)}
      else{ctx.strokeStyle="#d5c9ff";ctx.beginPath();ctx.arc(r*.86,0,size*.2,-1.15,1.15);ctx.stroke();ctx.beginPath();ctx.moveTo(r*.74,0);ctx.lineTo(r*1.12,0);ctx.stroke()}
      ctx.restore();
    }
    if(base.type==="military"){
      ctx.strokeStyle="rgba(255,120,111,.58)";for(let i=0;i<tier;i++){ctx.beginPath();ctx.arc(0,0,size*(1.62+i*.2),0,6.283);ctx.stroke()}
      if(tier===3){ctx.fillStyle="#ff8f83";for(let i=0;i<6;i++){const a=i/6*6.283;ctx.beginPath();ctx.arc(Math.cos(a)*size*1.8,Math.sin(a)*size*1.05,2.1,0,6.283);ctx.fill()}}
    }else if(base.type==="sensor"){
      ctx.rotate(-spin*1.8);ctx.strokeStyle="#c9b8ff";ctx.beginPath();ctx.arc(0,-size*.15,size*(1.55+tier*.16),-2.7,-.45);ctx.stroke();ctx.beginPath();ctx.moveTo(0,0);ctx.lineTo(size*1.55,-size*.8);ctx.stroke();
    }else if(base.type==="logistics"&&tier>=2){ctx.strokeStyle="rgba(117,234,255,.55)";ctx.beginPath();ctx.moveTo(-size*1.75,size*.72);ctx.lineTo(size*1.75,size*.72);ctx.stroke()}
    if(base.upgradeProject){ctx.strokeStyle="#ffffff";ctx.globalAlpha=.45+.35*Math.sin(t*4);ctx.setLineDash([3,4]);ctx.beginPath();ctx.arc(0,0,size*2.05,0,6.283*stxIFUpgradeRatio(base.upgradeProject));ctx.stroke();ctx.setLineDash([]);ctx.globalAlpha=1}
    ctx.restore();
    if(z>.48){ctx.save();ctx.textAlign="left";ctx.fillStyle=spec.color;ctx.font=`900 ${Math.max(8,9*z)}px system-ui`;ctx.fillText(base.name.toUpperCase(),s.x+size*1.8,s.y-size*.9);ctx.fillStyle="#9bb0c4";ctx.font=`700 ${Math.max(7,7.5*z)}px system-ui`;ctx.fillText(`TIER ${tier} · ${stxIFTierLabel(base).toUpperCase()} · ${Math.round(base.supplyReadiness*100)}% SUPPLY`,s.x+size*1.8,s.y+3);if(base.upgradeProject)ctx.fillText(`UPGRADE ${stxIFPct(stxIFUpgradeRatio(base.upgradeProject))} DELIVERED`,s.x+size*1.8,s.y+14);ctx.restore()}
    if(base.dockedFleetIds?.length&&z>.68){base.dockedFleetIds.map(fleetRecord).filter(Boolean).slice(0,5).forEach((f,i)=>{const a=t*(i%2?-.12:.1)+i*1.6,r=size*(2.15+i*.16),p={x:s.x+Math.cos(a)*r,y:s.y+Math.sin(a)*r*.58};drawCombatCraft(p,{x:p.x-Math.sin(a)*10,y:p.y+Math.cos(a)*10},empire(f.owner)?.color||"#fff",clamp(2.1*z,1.8,4.8),i===0&&f.strength>35)})}
    state.stats.visible++;
  };
}

function stxIFUpgradePanel(base){
  if(!base||base.owner!==0||base.status!=="operational")return"";
  if(base.tier>=3&&!base.upgradeProject)return`<div class="stx-if-upgrade max"><b>TIER 3 · ${stxIFTierLabel(base)}</b><small>Maximum fortress tier. This installation has its full docking, defensive and regional support footprint.</small></div>`;
  const q=base.upgradeProject,target=q?.targetTier||base.tier+1,cost=q?.need||stxIFUpgradeCost(base,target),ratio=q?stxIFUpgradeRatio(q):0;
  return`<div class="stx-if-upgrade"><div><b>${q?`UPGRADING TO TIER ${target}`:`TIER ${target} UPGRADE · ${stxIFTierLabel(base,target)}`}</b><small>${q?`${stxIFPct(ratio)} of modules physically delivered · station remains operational`:`Expands the station silhouette, integrity, docking/service capacity and type-specific regional effect.`}</small></div>${q?`<div class="stx-if-progress"><i style="width:${Math.round(ratio*100)}%"></i></div>`:`<button class="choice-btn primary-choice" data-stx-if-upgrade="${base.id}">Authorize Upgrade</button>`}<div class="stx-if-cost">${Object.entries(cost||{}).map(([r,a])=>`<span>${stxIFResourceLabel(r)} ${Math.ceil(a)}</span>`).join("")}</div></div>`;
}
if(typeof stxDSBasePanel==="function"){
  const STX_IF_stxDSBasePanel=stxDSBasePanel;
  stxDSBasePanel=function(base){const html=STX_IF_stxDSBasePanel(base);if(!html||base?.status!=="operational")return html;return html.replace(/<\/section>\s*$/,`${stxIFUpgradePanel(base)}</section>`)};
}

function stxIFPlanetFlowCard(p){
  if(!p||p.owner!==0)return"";stxIFEnsurePlanet(p);const priority=stxIFProjectPriority(),expansion=stxIFExpansionBurden(p),m=p.stxIFEconomicMobilization||{},ships=state.ships||[];
  const inbound=ships.filter(s=>s.to===p.id).reduce((n,s)=>n+Object.values(s.cargo||{}).reduce((a,v)=>a+stxIFNum(v),0),0),outbound=ships.filter(s=>s.from===p.id).reduce((n,s)=>n+Object.values(s.cargo||{}).reduce((a,v)=>a+stxIFNum(v),0),0),commit=STX_IF_ROUTE_RESOURCES.reduce((n,r)=>n+stxIFLocalNeed(p,r),0);
  const priorityText=priority?(priority.p===p?`PRIORITY DESTINATION · ${priority.title}`:(m.label||"Contributing to Imperial priority")):"No Imperial priority active";
  return`<section class="stx-if-economy"><div class="section-label">Integrated Economic Flow</div><div class="stx-if-metrics"><span><label>Project commitments</label><b>${Math.round(commit)}</b></span><span><label>Inbound cargo</label><b>${Math.round(inbound)}</b></span><span><label>Outbound cargo</label><b>${Math.round(outbound)}</b></span><span><label>Expansion load</label><b>${expansion?`${Math.round(expansion*100)}% mobilized`:"None"}</b></span></div><div class="stx-if-priority ${priority?"active":""}"><b>${priorityText}</b><small>${priority?"Priority may pull real stock, production and trained personnel from other worlds; freight still takes travel time.":"Choose PRIORITIZE on an active project to give it first claim on empire supply."}</small></div><div class="stx-if-route-grid">${STX_IF_ROUTE_RESOURCES.map(r=>{const mode=stxIFRoute(p,r);return`<button data-stx-if-route="${r}" class="${mode}"><b>${stxIFResourceLabel(r)}</b><span>${Math.round(stxIFNum(p.stock?.[r]))}</span><small>${STX_IF_ROUTE_LABEL[mode]}</small></button>`}).join("")}</div><p class="subtle">Click a resource to cycle BAL → PROJECT → EXPORT. PROJECT keeps a larger local reserve; EXPORT releases discretionary surplus to domestic/foreign logistics, but active local projects remain protected.</p></section>`;
}
if(typeof renderPlanet==="function"){
  const STX_IF_renderPlanet=renderPlanet;
  renderPlanet=function(){
    STX_IF_renderPlanet();const p=state.selected,body=$("planetBody");if(!p||!body)return;
    if(p.owner===0)body.insertAdjacentHTML("beforeend",stxIFPlanetFlowCard(p));
    body.querySelectorAll?.("[data-stx-if-route]").forEach(btn=>btn.onclick=e=>{e.preventDefault();e.stopPropagation();stxIFCycleRoute(p.id,btn.dataset.stxIfRoute)});
    body.querySelectorAll?.("[data-stx-if-upgrade]").forEach(btn=>btn.onclick=e=>{e.preventDefault();e.stopPropagation();stxIFStartUpgrade(btn.dataset.stxIfUpgrade)});
  };
}

function stxIFTick(){
  const now=state.simTime,dt=Math.max(0,Math.min(1.2,now-stxIFLastTick));stxIFLastTick=now;if(dt<=0)return;
  state.planets.forEach(stxIFEnsurePlanet);stxIFPriorityCrewTick(dt);stxIFTickUpgrades(dt);
}
if(typeof simulate==="function"){
  const STX_IF_simulate=simulate;
  simulate=function(dt){const result=STX_IF_simulate(dt);if(state.simTime-stxIFLastTick>=.45)stxIFTick();return result};
}

function stxIFInstallStyles(){
  if(document.getElementById?.("stxIntegratedEconomyStyles"))return;const style=document.createElement("style");style.id="stxIntegratedEconomyStyles";style.textContent=`
  .stx-if-economy{margin-top:12px;padding:11px;border:1px solid rgba(114,225,255,.2);border-radius:10px;background:linear-gradient(180deg,rgba(11,23,39,.82),rgba(5,12,22,.88))}
  .stx-if-metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:6px;margin:7px 0}.stx-if-metrics span{padding:7px;background:rgba(255,255,255,.035);border:1px solid rgba(255,255,255,.07);border-radius:7px}.stx-if-metrics label{display:block;font-size:7px;letter-spacing:.08em;color:#7790a8;text-transform:uppercase}.stx-if-metrics b{font-size:11px;color:#e7f8ff}
  .stx-if-priority{padding:8px;border-left:2px solid #52677d;background:rgba(255,255,255,.025);margin:7px 0}.stx-if-priority.active{border-left-color:#ffd56f;background:rgba(255,213,111,.055)}.stx-if-priority b,.stx-if-priority small{display:block}.stx-if-priority small{color:#8fa5b9;margin-top:2px}
  .stx-if-route-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:5px}.stx-if-route-grid button{min-width:0;padding:7px 5px;border:1px solid rgba(255,255,255,.09);border-radius:7px;background:#09121f;color:#bcd0df;text-align:left;cursor:pointer}.stx-if-route-grid button b,.stx-if-route-grid button span,.stx-if-route-grid button small{display:block;overflow:hidden;text-overflow:ellipsis}.stx-if-route-grid button b{font-size:8px}.stx-if-route-grid button span{font-size:11px;color:#fff}.stx-if-route-grid button small{font-size:7px;color:#738ca2}.stx-if-route-grid button.project{border-color:rgba(255,213,111,.38);box-shadow:inset 0 0 0 1px rgba(255,213,111,.08)}.stx-if-route-grid button.export{border-color:rgba(112,233,255,.4);box-shadow:inset 0 0 0 1px rgba(112,233,255,.08)}
  .stx-if-upgrade{margin-top:10px;padding:10px;border:1px solid color-mix(in srgb,var(--ds-color) 34%,transparent);border-radius:9px;background:rgba(5,12,24,.76)}.stx-if-upgrade>div:first-child b,.stx-if-upgrade>div:first-child small{display:block}.stx-if-upgrade>div:first-child small{color:#8ca3b7;margin:3px 0 7px}.stx-if-cost{display:flex;flex-wrap:wrap;gap:4px;margin-top:7px}.stx-if-cost span{font-size:7px;padding:3px 5px;border-radius:10px;background:rgba(255,255,255,.05);color:#a9bfd0}.stx-if-progress{height:5px;background:#111d2a;border-radius:5px;overflow:hidden}.stx-if-progress i{display:block;height:100%;background:var(--ds-color);box-shadow:0 0 8px var(--ds-color)}.stx-if-upgrade.max{opacity:.85}
  @media(max-width:760px){.stx-if-metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.stx-if-route-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
  `;document.head.appendChild(style);
}
stxIFInstallStyles();

const SpaceTyrantsIntegratedEconomy={
  version:STX_IF_VERSION,route:stxIFRoute,setRoute:stxIFSetRoute,cycleRoute:stxIFCycleRoute,
  expansionBurden:stxIFExpansionBurden,mobilizationProfile:stxIFMobilizationProfile,descriptorNeeds:stxIFDescriptorNeeds,
  tierLabel:stxIFTierLabel,upgradeCost:stxIFUpgradeCost,startUpgrade:stxIFStartUpgrade,upgradeRatio:stxIFUpgradeRatio,tick:stxIFTick
};
globalThis.SpaceTyrantsIntegratedEconomy=SpaceTyrantsIntegratedEconomy;
