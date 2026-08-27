/* Space Tyrants — military commissioning + strategic trade network.
   Loaded after fleet-clarity.js. Adds persistent fleet commissioning, Admiralty
   requests, rare upgradeable trade stations, favorable exchange terms, and
   station raids without turning every system into an orbital parking lot. */

DIRECTIVE_LABEL.fleetCommission="Fleet Commissioning Program";
DIRECTIVE_LABEL.tradeStation="Interstellar Trade Station";

const STX_TRADE_STATION_LEVELS=[
  {name:"Commercial Anchorage",need:{components:72,titanium:28,rare:16,helium:18},hp:72,bonus:.08},
  {name:"Interstellar Exchange",need:{components:105,titanium:42,rare:27,helium:28},hp:108,bonus:.15},
  {name:"Grand Trade Citadel",need:{components:148,titanium:58,rare:42,helium:38},hp:154,bonus:.24}
];

function stxEnsureMilitaryTradeState(){
  state.militaryRequests=Array.isArray(state.militaryRequests)?state.militaryRequests:[];
  state.planets.forEach(p=>{
    p.tradeStation=p.tradeStation||null;
    p.tradeStationProject=p.tradeStationProject||null;
    p.lastTradeStationRaid=Number.isFinite(p.lastTradeStationRaid)?p.lastTradeStationRaid:-999;
  });
  state.empires.forEach(e=>{
    if(!Number.isFinite(e.nextTradeStationAt))e.nextTradeStationAt=state.simTime+rand(80,180);
    if(!Number.isFinite(e.nextMilitaryRequestAt))e.nextMilitaryRequestAt=state.simTime+rand(25,60);
  });
}

const STX_mtGenerateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_mtGenerateGalaxy();stxEnsureMilitaryTradeState()};
const STX_mtLoadGame=loadGame;
loadGame=function(){const ok=STX_mtLoadGame();if(ok)stxEnsureMilitaryTradeState();return ok};
stxEnsureMilitaryTradeState();

function stxActiveFleets(owner){return state.fleets.filter(f=>f.owner===owner&&!f.destroyed)}
function stxStationedFleetCount(owner){return stxActiveFleets(owner).filter(f=>f.location&& !state.ships.some(s=>s.fleetId===f.id)).length}
function stxFleetNeed(owner){
  const worlds=owned(owner),wars=state.wars.filter(w=>w.active&&(w.a===owner||w.b===owner)).length;
  return Math.max(1,Math.ceil(worlds.length/2)+wars)-stxActiveFleets(owner).length;
}
function stxBestFleetYard(owner=0){
  return owned(owner).filter(p=>p.infra.shipyard>0&&!p.underAttack).sort((a,b)=>{
    const aq=(a.buildQueue||[]).length,bq=(b.buildQueue||[]).length;
    return aq-bq||(b.infra.shipyard+b.infra.factory*.7+b.stock.components/80)-(a.infra.shipyard+a.infra.factory*.7+a.stock.components/80);
  })[0]||null;
}
function stxQueueFleetCommission(p,type="fleet",source="Admiralty commission"){
  if(!p||p.owner===null||p.infra.shipyard<=0)return false;
  p.buildQueue=p.buildQueue||[];
  const patrol=type==="patrol",need={components:patrol?26:38,helium:patrol?12:18,titanium:patrol?9:15,trained:patrol?.005:.009};
  const q={type:patrol?"patrol":"fleet",progress:0,startedAt:state.simTime,need,commissioned:true,commissionSource:source};
  p.buildQueue.unshift(q);
  Object.entries(need).forEach(([r,a])=>{if((p.stock[r]||0)<a*.8)addOrder(p,r==="trained"?"crew":"ship",r,a,5,source)});
  p.mandateGlow=1;
  if(p.owner===0){logEvent(`${p.name} accepted a ${patrol?"patrol flotilla":"fleet"} commission from the Admiralty.`,"good");stxActivity(`${p.name}'s shipyard began a priority ${patrol?"patrol flotilla":"fleet"} commission.`,p.id,null,"good")}
  return true;
}

/* Combat construction should always produce a persistent fleet. Previously a
   completed fleet with no valid destination could disappear into garrison. */
const STX_mtLaunchBuiltShip=launchBuiltShip;
launchBuiltShip=function(p,type){
  if(type!=="fleet"&&type!=="patrol")return STX_mtLaunchBuiltShip(p,type);
  const e=empire(p.owner),target=chooseShipTarget(p,type),patrol=type==="patrol";
  const boost=1+(p.orbitals?.station||0)*.08+(p.orbitals?.base||0)*.2;
  const strength=(patrol?10:18)*(1+e.tech.weapons*.15)*boost;
  if(!target||target===p){
    const f=registerFleet(p.owner,p,strength,patrol?"patrol":"fleet");
    f.status=`Stationed at ${p.name}`;f.location=p.id;
    if(p.owner===0){logEvent(`${f.name} was commissioned at ${p.name} and entered permanent orbital service.`,"good");stxActivity(`${f.name} commissioned at ${p.name}; it will remain in orbit until assigned.`,p.id,f.id,"good");stxRefreshFleetLocator()}
    return f;
  }
  const ship=deployFleet(p,target,p.owner,strength,{home:p.id,speedBoost:1+(p.orbitals?.station||0)*.12+(p.orbitals?.base||0)*.38,role:patrol?"patrol":"fleet"});
  if(p.owner===0)logEvent(`${p.name} launched a new ${type}.`,"good");
  return ship;
};

function stxPendingMilitaryRequest(){return state.militaryRequests.find(r=>r.status==="pending"&&r.expiresAt>state.simTime)}
function stxCreateMilitaryRequest(){
  const e=empire(0),yard=stxBestFleetYard(0);if(!e||!yard||stxPendingMilitaryRequest()||stxFleetNeed(0)<=0)return false;
  const war=stxPlayerAtWar(),threat=borderThreat(yard),patrol=!war&&threat<.48&&random()<.42;
  const request={id:`mr${Math.floor(random()*1e9)}`,status:"pending",createdAt:state.simTime,expiresAt:state.simTime+42,planetId:yard.id,type:patrol?"patrol":"fleet",title:patrol?"Commission a Frontier Patrol":"Commission a New Battle Fleet",message:war?`The Admiralty reports that active war commitments exceed available fleet formations. ${yard.name} can begin a priority battle-fleet commission immediately.`:`Naval planners believe the Mandate has too few visible fleet formations for its ${playerWorlds().length} worlds. ${yard.name} can begin a priority ${patrol?"patrol flotilla":"fleet"} commission.`};
  state.militaryRequests.push(request);e.nextMilitaryRequestAt=state.simTime+rand(70,115);updateBadges?.();renderTransmissions();return true;
}
function stxRespondMilitaryRequest(id,action){
  const r=state.militaryRequests.find(x=>x.id===id&&x.status==="pending");if(!r)return;
  const p=state.planets.find(x=>x.id===r.planetId);
  if(action==="approve"&&p){r.status="accepted";stxQueueFleetCommission(p,r.type,"Approved Admiralty request");setModifier(empire(0),"shipbuilding",1.22,70);showToast(`${r.type==="patrol"?"Patrol":"Fleet"} commission approved at ${p.name}`)}
  else{r.status="declined";showToast("Admiralty request declined")}
  renderTransmissions();updateBadges?.();
}
function stxMilitaryRequestTick(){
  const e=empire(0);if(!e)return;
  state.militaryRequests.forEach(r=>{if(r.status==="pending"&&r.expiresAt<=state.simTime)r.status="expired"});
  if(state.simTime>=(e.nextMilitaryRequestAt||0)&&stxFleetNeed(0)>0)stxCreateMilitaryRequest();
}

const STX_mtGovernorTick=governorTick;
governorTick=function(){STX_mtGovernorTick();stxEnsureMilitaryTradeState();stxMilitaryRequestTick();stxTradeStationAITick()};

/* Add Admiralty requests to the existing transmission hub without replacing
   diplomacy/governor cards. */
const STX_mtRenderTransmissions=renderTransmissions;
renderTransmissions=function(){
  stxEnsureMilitaryTradeState();stxApplyTradeStationTerms();STX_mtRenderTransmissions();
  const box=$("transmissionList");if(!box)return;
  const pending=state.militaryRequests.filter(r=>r.status==="pending"&&r.expiresAt>state.simTime);
  if(!pending.length)return;
  if(box.querySelector(".empty-hub"))box.innerHTML="";
  box.insertAdjacentHTML("beforeend",pending.map(r=>{const p=state.planets.find(x=>x.id===r.planetId);return `<article class="transmission-card urgent stx-military-request"><div class="card-kicker">ADMIRALTY REQUEST // FLEET COMMAND</div><div class="card-title"><strong>${r.title}</strong><span class="news-time">${fmtEta(r.expiresAt-state.simTime)}</span></div><p class="card-copy">${r.message}</p><div class="choice-row"><button class="choice-btn primary-choice" data-military-response="approve" data-military-id="${r.id}">Approve · ${p?.name||"Shipyard"}</button><button class="choice-btn danger-choice" data-military-response="decline" data-military-id="${r.id}">Decline</button></div></article>`}).join(""));
  box.querySelectorAll("[data-military-id]").forEach(b=>b.onclick=()=>stxRespondMilitaryRequest(b.dataset.militaryId,b.dataset.militaryResponse));
};

COMMANDS.push({id:"fleetCommission",cat:"Military",title:"Commission New Fleet",desc:"Order an actual persistent fleet formation instead of waiting for shipyards to assign one automatically.",effects:["Priority fleet construction","Fleet remains stationed if idle","Named admiral and permanent battle record"],score:()=>stxBestFleetYard(0)?(stxFleetNeed(0)>0?88:48):0,target:()=>stxBestFleetYard(0),apply:(e,t)=>{if(t){stxQueueFleetCommission(t,"fleet","Imperial fleet commission");addDirective(e,"fleetCommission",t.id,90);setModifier(e,"shipbuilding",1.3,90)}}});

/* -------------------------- TRADE STATIONS -------------------------- */
function stxTradeStationCap(owner){return clamp(Math.floor((owned(owner).length+2)/5),1,3)}
function stxTradeStations(owner){return owned(owner).filter(p=>p.tradeStation&&p.tradeStation.hp>0)}
function stxTradeStationLevelSum(owner){return stxTradeStations(owner).reduce((n,p)=>n+(p.tradeStation.level||1),0)}
function stxTradeStationTermsBonus(owner=0){return Math.min(.36,stxTradeStationLevelSum(owner)*.075)}
function stxBestTradeStationLevel(owner=0){return Math.max(0,...stxTradeStations(owner).map(p=>p.tradeStation.level||1))}
function stxCanHostTradeStation(p){return !!p&&p.owner!==null&&p.infra.shipyard>0&&p.infra.factory>0&&!p.underAttack}
function stxTradeStationCandidates(owner=0){
  return owned(owner).filter(p=>stxCanHostTradeStation(p)&&!p.tradeStation&&!p.tradeStationProject).sort((a,b)=>(b.tradeVolume||0)-(a.tradeVolume||0)||(b.infra.factory+b.infra.shipyard+b.pop*4)-(a.infra.factory+a.infra.shipyard+a.pop*4));
}
function stxTradeStationUpgradeCandidates(owner=0){return owned(owner).filter(p=>p.tradeStation&&p.tradeStation.level<3&&!p.tradeStationProject&&!p.underAttack).sort((a,b)=>b.tradeStation.level-a.tradeStation.level||(b.tradeVolume||0)-(a.tradeVolume||0))}
function stxQueueTradeStationProject(p,mandated=false){
  if(!stxCanHostTradeStation(p)||p.tradeStationProject)return false;
  const current=p.tradeStation?.level||0;
  if(current===0&&stxTradeStations(p.owner).length>=stxTradeStationCap(p.owner))return false;
  if(current>=3)return false;
  const level=current+1,spec=STX_TRADE_STATION_LEVELS[level-1];
  p.tradeStationProject={level,progress:0,need:{...spec.need},startedAt:state.simTime,mandated};p.mandateGlow=1;
  Object.entries(spec.need).forEach(([r,a])=>{if((p.stock[r]||0)<a)addOrder(p,"trade-station",r,a,mandated?6:3,`${spec.name} construction`)});
  if(p.owner===0)logEvent(`${p.name} began ${current?"expanding":"constructing"} its trade station toward ${spec.name}.`,"good");
  return true;
}
function stxTickTradeStationProject(p,dt){
  const q=p.tradeStationProject;if(!q||p.owner===null)return;
  const spec=STX_TRADE_STATION_LEVELS[q.level-1];
  Object.entries(q.need).forEach(([r,a])=>{if((p.stock[r]||0)<a)addOrder(p,"trade-station",r,a,q.mandated?6:3,`${spec.name} construction`)});
  if(!Object.entries(q.need).every(([r,a])=>(p.stock[r]||0)>=a))return;
  q.progress+=dt*.0036*(1+p.infra.factory*.24+p.infra.shipyard*.28)*(q.mandated?1.3:1);
  if(q.progress<1)return;
  Object.entries(q.need).forEach(([r,a])=>consume(p,r,a));
  p.tradeStation={level:q.level,name:spec.name,hp:spec.hp,maxHp:spec.hp,createdAt:p.tradeStation?.createdAt||state.simTime,lastIncomeAt:state.simTime};p.tradeStationProject=null;p.mandateGlow=1;
  const e=empire(p.owner);setModifier(e,"foreignTrade",1+spec.bonus,90);
  if(p.owner===0){logEvent(`${spec.name} opened above ${p.name}. Foreign merchants are offering better exchange terms.`,"good");galacticNews(`${p.name.toUpperCase()} OPENS ${spec.name.toUpperCase()}`,`The new trade station is drawing commercial traffic and improving negotiated exchange terms. It is valuable infrastructure—and a valid wartime fleet target.`,"trade",p.id)}
}
function stxTradeStationEconomyTick(p,dt){
  const s=p.tradeStation;if(!s||s.hp<=0||p.owner===null)return;
  const health=clamp(s.hp/Math.max(1,s.maxHp),.25,1),level=s.level||1,e=empire(p.owner);
  const revenue=dt*.0018*level*health*(1+(p.tradeVolume||0)*.018);
  e.credits+=revenue;p.tradeVolume=(p.tradeVolume||0)+dt*.0025*level*health;
  if(s.hp<s.maxHp&&!p.underAttack)s.hp=Math.min(s.maxHp,s.hp+dt*.004*(1+p.infra.factory*.15));
}
const STX_mtTickPlanet=tickPlanet;
tickPlanet=function(p,dt){STX_mtTickPlanet(p,dt);stxTickTradeStationProject(p,dt);stxTradeStationEconomyTick(p,dt)};

function stxTradeStationAITick(){
  state.empires.forEach(e=>{
    if(state.simTime<(e.nextTradeStationAt||0))return;
    e.nextTradeStationAt=state.simTime+rand(150,250);
    const existing=stxTradeStations(e.id),cap=stxTradeStationCap(e.id);
    if(existing.length<cap&&empireDevelopment(e)>3.3&&random()<.42){const p=stxTradeStationCandidates(e.id)[0];if(p)stxQueueTradeStationProject(p,false);return}
    if(existing.length&&random()<.24){const p=stxTradeStationUpgradeCandidates(e.id)[0];if(p)stxQueueTradeStationProject(p,false)}
  });
}
function stxBestTradeStationTarget(owner=0){
  const wars=state.wars.filter(w=>w.active&&(w.a===owner||w.b===owner)),foes=new Set(wars.map(w=>w.a===owner?w.b:w.a));
  return state.planets.filter(p=>p.owner!==null&&p.owner!==owner&&foes.has(p.owner)&&p.tradeStation).sort((a,b)=>((b.tradeStation.level||1)*45+(b.tradeVolume||0)*2)-(a.tradeStation.level||1)*45-(a.tradeVolume||0)*2||stxNearestPlayerDistance(a)-stxNearestPlayerDistance(b))[0]||null;
}
function stxRaidTradeStation(target,owner=0){
  if(!target?.tradeStation||target.owner===owner||!empiresAtWar(owner,target.owner))return false;
  const sources=owned(owner).filter(p=>!p.underAttack&&dist(p,target)<3800).sort((a,b)=>{
    const af=stxActiveFleets(owner).filter(f=>f.location===a.id).reduce((n,f)=>n+f.strength,0),bf=stxActiveFleets(owner).filter(f=>f.location===b.id).reduce((n,f)=>n+f.strength,0);return bf-af||dist(a,target)-dist(b,target);
  });
  const source=sources[0];if(!source)return false;
  let f=stxActiveFleets(owner).filter(x=>x.location===source.id&&!state.ships.some(s=>s.fleetId===x.id)).sort((a,b)=>b.strength-a.strength)[0],ship;
  if(f){f.location=null;f.status=`Raiding ${target.tradeStation.name} at ${target.name}`;ship=createShip("fleet",source,target,owner,{strength:f.strength,fleetId:f.id,vesselName:f.name,speedBoost:1.18})}
  else if(source.garrison>13){const strength=Math.max(8,source.garrison*.28);source.garrison-=strength;ship=deployFleet(source,target,owner,strength,{status:`Raiding trade station at ${target.name}`})}
  if(!ship)return false;ship.stationTargetId=target.id;ship.stationRaid=true;
  if(owner===0){logEvent(`Trade-station raid ordered against ${target.tradeStation.name} at ${target.name}.`,"warning");stxActivity(`A fleet departed to raid ${target.tradeStation.name} at ${target.name}.`,target.id,ship.fleetId,"warning")}
  return true;
}

/* AI campaigns sometimes choose a valuable station instead of the planet.
   This is intentionally uncommon so stations remain strategic targets rather
   than making every war a sequence of orbital raids. */
const STX_mtDeployFleet=deployFleet;
deployFleet=function(from,to,owner,strength,extra={}){
  const ship=STX_mtDeployFleet(from,to,owner,strength,extra);
  if(ship&&owner!==0&&to?.tradeStation&&to.owner!==owner&&empiresAtWar(owner,to.owner)&&!extra.reinforcement&&random()<.22){ship.stationTargetId=to.id;ship.stationRaid=true;const f=fleetRecord(ship.fleetId);if(f)f.status=`Raiding ${to.tradeStation.name} at ${to.name}`}
  return ship;
};

function stxResolveTradeStationRaid(s,p){
  if(!s?.stationRaid||!p?.tradeStation||p.owner===s.owner||!empiresAtWar(s.owner,p.owner))return false;
  const station=p.tradeStation,level=station.level||1,stationed=stxActiveFleets(p.owner).filter(f=>f.location===p.id&&!state.ships.some(x=>x.fleetId===f.id)).reduce((n,f)=>n+f.strength,0);
  const defense=10+level*11+(p.infra.defense||0)*3+stationed*.22,attack=Math.max(6,s.strength||12),damage=Math.max(7,attack*rand(.58,.92)-defense*.18);
  station.hp=Math.max(0,station.hp-damage);p.lastTradeStationRaid=state.simTime;
  const f=fleetRecord(s.fleetId),loss=clamp(defense/(attack+defense)*.28,.05,.32);if(f)f.strength=Math.max(1,f.strength*(1-loss));
  if(station.hp<=0){const name=station.name;p.tradeStation=null;p.tradeStationProject=null;galacticNews(`${p.name.toUpperCase()} TRADE STATION DESTROYED`,`${empire(s.owner).name} fleet elements destroyed ${name}. Regional exchange traffic is already rerouting.`,"danger",p.id);if(p.owner===0)logEvent(`${name} at ${p.name} was destroyed in a fleet raid.`,"danger")}
  else{galacticNews(`${p.name.toUpperCase()} TRADE STATION RAIDED`,`${empire(s.owner).name} struck ${station.name}, leaving it at ${Math.round(station.hp/station.maxHp*100)}% structural integrity.`,"warning",p.id);if(p.owner===0)logEvent(`${station.name} at ${p.name} was hit by an enemy fleet raid.`,"danger")}
  const retreat=owned(s.owner).sort((a,b)=>dist(a,p)-dist(b,p))[0];
  if(f&&retreat&&f.strength>2){f.location=null;f.status=`Returning from raid at ${p.name}`;createShip("fleet",p,retreat,s.owner,{strength:f.strength,fleetId:f.id,vesselName:f.name,retreat:true,speedBoost:1.2})}
  else if(f){f.location=p.id;f.status=`Raid completed at ${p.name}`}
  renderPlanet();return true;
}
const STX_mtArriveShip=arriveShip;
arriveShip=function(s,p){if(stxResolveTradeStationRaid(s,p))return;return STX_mtArriveShip(s,p)};

function stxApplyTradeStationTerms(){
  const bonus=stxTradeStationTermsBonus(0),best=stxBestTradeStationLevel(0);if(bonus<=0)return;
  state.proposals.filter(p=>p.kind==="trade"&&p.status==="pending"&&(p.to===0||p.empireId===0)&&!p.stxTradeStationAdjusted).forEach(p=>{
    if(p.offer?.amount)p.offer.amount=Math.ceil(p.offer.amount*(1+bonus));
    if(p.request?.amount)p.request.amount=Math.max(1,Math.ceil(p.request.amount*(1-bonus*.48)));
    if(p.request?.credits)p.request.credits=Math.max(1,Math.ceil(p.request.credits*(1-bonus*.42)));
    if(best>=3&&Number.isFinite(p.duration))p.duration+=1;
    p.stxTradeStationAdjusted=true;p.stxTradeBonus=bonus;
  });
}

function stxBestTradeStationBuildTarget(){
  const existing=stxTradeStations(0);
  if(existing.length<stxTradeStationCap(0))return stxTradeStationCandidates(0)[0]||null;
  return stxTradeStationUpgradeCandidates(0)[0]||null;
}
COMMANDS.push(
  {id:"tradeStation",cat:"Trade",title:"Establish a Trade Station",desc:"Build a rare commercial orbital that improves exchange terms and produces brokerage revenue—but becomes a valuable wartime target.",effects:["Better foreign trade offers","Commercial revenue","Upgradeable to 3 tiers","Can be raided by fleets"],score:()=>stxBestTradeStationBuildTarget()?76:0,target:()=>stxBestTradeStationBuildTarget(),apply:(e,t)=>{if(t){stxQueueTradeStationProject(t,true);addDirective(e,"tradeStation",t.id,125);setModifier(e,"foreignTrade",1.2,125)}}},
  {id:"raidTradeStation",cat:"War",title:"Raid Enemy Trade Station",desc:"Strike a rival's commercial orbital without committing to planetary conquest.",effects:["Damages/destroys trade infrastructure","Fleet returns after raid","Reduces enemy trade leverage"],score:()=>stxPlayerAtWar()&&stxBestTradeStationTarget(0)?70:0,target:()=>stxBestTradeStationTarget(0),apply:(e,t)=>{if(t)stxRaidTradeStation(t,0)}}
);

function stxDrawTradeStation(p,s,r){
  const station=p.tradeStation,project=p.tradeStationProject;if(!station&&!project)return;
  const level=station?.level||project?.level||1,owner=p.owner!==null?empire(p.owner):null,now=performance.now()/1000,rr=r*(3.35+level*.22),a=now*.19+(+String(p.id).replace(/\D/g,"")||1)*.77;
  const x=s.x+Math.cos(a)*rr,y=s.y+Math.sin(a)*rr*.44,color=owner?.color||"#ffd979";ctx.save();ctx.strokeStyle="rgba(255,217,121,.38)";ctx.lineWidth=1;ctx.setLineDash([2,6]);ctx.beginPath();ctx.ellipse(s.x,s.y,rr,rr*.44,0,0,6.283);ctx.stroke();ctx.setLineDash([]);ctx.translate(x,y);ctx.rotate(a*.35);ctx.fillStyle="rgba(10,18,30,.95)";ctx.strokeStyle="#ffd979";ctx.shadowColor="#ffd979";ctx.shadowBlur=10;
  ctx.beginPath();ctx.moveTo(-8,0);ctx.lineTo(-4,-5);ctx.lineTo(5,-5);ctx.lineTo(9,0);ctx.lineTo(5,5);ctx.lineTo(-4,5);ctx.closePath();ctx.fill();ctx.stroke();ctx.beginPath();ctx.moveTo(-11,0);ctx.lineTo(12,0);ctx.moveTo(0,-9);ctx.lineTo(0,9);ctx.stroke();
  if(level>=2){ctx.beginPath();ctx.arc(0,0,7,0,6.283);ctx.stroke()}if(level>=3){ctx.fillStyle="#ffd979";ctx.fillRect(-2,-2,4,4)}ctx.restore();
  if(state.camera.zoom>.62){ctx.save();ctx.fillStyle="#ffd979";ctx.font="700 8px system-ui";ctx.textAlign="left";ctx.fillText(project?`TRADE STATION · ${Math.round((project.progress||0)*100)}%`:`${station.name.toUpperCase()} · L${level}`,x+11,y-6);ctx.restore()}
}
const STX_mtDrawConstructionState=drawConstructionState;
drawConstructionState=function(p,s,r){STX_mtDrawConstructionState(p,s,r);stxDrawTradeStation(p,s,r)};

const STX_mtRenderShipLedger=renderShipLedger;
renderShipLedger=function(p){
  let html=STX_mtRenderShipLedger(p),extra="";
  if(p.tradeStation){const s=p.tradeStation,integrity=Math.round(s.hp/s.maxHp*100),bonus=Math.round(STX_TRADE_STATION_LEVELS[s.level-1].bonus*100);extra+=`<div class="project-row active stx-trade-station"><div class="project-head"><strong>◇ ${s.name}</strong><b>Level ${s.level}</b></div><div class="project-desc">Commercial orbital · ${integrity}% integrity · local trade leverage +${bonus}%. Empire-wide exchange offers improve while the station survives.</div><div class="project-meta"><span>Fleet target in wartime</span><span>${s.level<3&&p.owner===0?`<button class="choice-btn" data-stx-upgrade-trade="${p.id}">Upgrade</button>`:""}</span></div></div>`}
  if(p.tradeStationProject){const q=p.tradeStationProject,spec=STX_TRADE_STATION_LEVELS[q.level-1];extra+=`<div class="project-row mandate"><div class="project-head"><strong>Trade Station Construction</strong><b>${Math.round((q.progress||0)*100)}%</b></div><div class="project-desc">Building ${spec.name}. Construction waits automatically for missing strategic materials.</div><div class="project-track"><i style="width:${Math.round((q.progress||0)*100)}%"></i></div></div>`}
  if(!p.tradeStation&&!p.tradeStationProject&&p.owner===0&&stxCanHostTradeStation(p)&&stxTradeStations(0).length<stxTradeStationCap(0))extra+=`<div class="project-row"><div class="project-head"><strong>Trade Station Site</strong><b>Available</b></div><div class="project-desc">This developed shipyard can host one of the Mandate's limited commercial orbitals.</div><div class="project-meta"><span>Cap ${stxTradeStations(0).length}/${stxTradeStationCap(0)}</span><span><button class="choice-btn" data-stx-build-trade="${p.id}">Establish</button></span></div></div>`;
  if(p.tradeStation&&p.owner!==0&&empiresAtWar(0,p.owner))extra+=`<div class="project-row war"><div class="project-head"><strong>Enemy Trade Station</strong><b>Valid target</b></div><div class="project-desc">A fleet can raid this station without beginning planetary conquest.</div><div class="project-meta"><span>${Math.round(p.tradeStation.hp/p.tradeStation.maxHp*100)}% integrity</span><span><button class="choice-btn danger-choice" data-stx-raid-trade="${p.id}">Raid Station</button></span></div></div>`;
  return html+extra;
};
const STX_mtRenderPlanet=renderPlanet;
renderPlanet=function(){STX_mtRenderPlanet();const body=$("planetBody");if(!body)return;body.querySelectorAll("[data-stx-build-trade]").forEach(b=>b.onclick=()=>{const p=state.planets.find(x=>x.id===b.dataset.stxBuildTrade);if(p&&stxQueueTradeStationProject(p,true)){renderPlanet();showToast(`Trade station construction started at ${p.name}`)}});body.querySelectorAll("[data-stx-upgrade-trade]").forEach(b=>b.onclick=()=>{const p=state.planets.find(x=>x.id===b.dataset.stxUpgradeTrade);if(p&&stxQueueTradeStationProject(p,true)){renderPlanet();showToast(`Trade station expansion started at ${p.name}`)}});body.querySelectorAll("[data-stx-raid-trade]").forEach(b=>b.onclick=()=>{const p=state.planets.find(x=>x.id===b.dataset.stxRaidTrade);if(p){stxRaidTradeStation(p,0);renderPlanet()}})};
