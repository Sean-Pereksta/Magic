/* Space Tyrants — physical orbital logistics, facility progression, fleet staging,
   direct war controls, and strategic network overlays.

   This layer is intentionally additive. Existing orbitals, trade stations, fleets,
   project priority, wars, saves, and freight remain authoritative compatibility
   summaries while richer facility/project records provide the interactive layer. */

const STX_OL_VERSION = 1;
const STX_OL_PROJECT_LIMIT = 3;
const STX_OL_VISIBLE_DOCK_CRAFT = 4;
let stxOLLastSimulationTick = -999;
const STX_OL_PHASES = {
  authorized:"AUTHORIZED", sourcing:"SOURCING", transit:"IN TRANSIT",
  assembly:"ASSEMBLY / SITE WORK", commissioning:"COMMISSIONING", operations:"OPERATIONAL"
};
const STX_OL_FACILITY_NAMES = {
  station:["Orbital Hub","Orbital Complex","System Citadel"],
  military:["Naval Base","Sector Base","Fleet Command Citadel"],
  trade:["Commercial Anchorage","Interstellar Exchange","Grand Trade Citadel"],
  shipyard:["Orbital Shipyard","Gantry Complex","Imperial Megayard"]
};
const STX_OL_MODULES = {
  logistics:{name:"Logistics Depot",desc:"Adds a berth and accelerates construction cargo transfer.",cost:{components:48,titanium:18,equipment:14}},
  fuel:{name:"Fuel / Helium Depot",desc:"Improves prepared first-leg and relay launch speed.",cost:{components:34,helium:32,titanium:14}},
  repair:{name:"Repair Dock",desc:"Restores damaged fleets while they are docked.",cost:{components:52,titanium:26,equipment:24}},
  command:{name:"Fleet Command Center",desc:"Shortens invasion staging and strengthens response readiness.",cost:{components:62,equipment:30,rare:18}},
  gantry:{name:"Orbital Shipyard Gantry",desc:"Improves shipbuilding and visibly commissions ships here.",cost:{components:68,titanium:34,equipment:22}},
  exchange:{name:"Trade Exchange",desc:"Raises commercial throughput and attracts civilian traffic.",cost:{components:46,rare:24,silicates:25}},
  sensors:{name:"Sensor / Communications Array",desc:"Extends detection and fleet-contact tracking.",cost:{components:42,rare:26,equipment:16}},
  defense:{name:"Defense Ring",desc:"Adds orbital battle support and makes the facility harder to raid.",cost:{components:55,titanium:38,equipment:24}}
};
const STX_OL_BUILD_LABELS = {
  mining:"Expand Primary Resource Extraction", factory:"Expand Industrial District",
  shipyard:"Expand Orbital Shipyard", orbital:"Upgrade Orbital Station",
  military:"Expand Military Base", module:"Install Station Module", repair:"Repair Orbital Facility"
};

function stxOLEscape(value){return String(value??"").replace(/[&<>\"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"})[c])}
function stxOLId(prefix){return `${prefix}${Math.floor(random()*1e9)}`}
function stxOLFacilitySlots(f){return Math.max(1,(f.tier||1)+(f.kind==="station"?1:0))}
function stxOLFacilitySpec(kind,tier){const names=STX_OL_FACILITY_NAMES[kind]||STX_OL_FACILITY_NAMES.station;return{name:names[clamp((tier||1)-1,0,2)],hp:72+(tier||1)*42,docks:kind==="military"?1+(tier||1)*2:1+(tier||1)}}
function stxOLFacilityAt(id){for(const p of state.planets){const f=(p.orbitalFacilities||[]).find(x=>x.id===id);if(f)return{p,f}}return null}
function stxOLFacilities(owner=null){return state.planets.flatMap(p=>(p.orbitalFacilities||[]).filter(f=>owner===null||p.owner===owner).map(f=>({p,f})))}
function stxOLFacilityForPlanet(p,kind=null){return(p?.orbitalFacilities||[]).find(f=>(!kind||f.kind===kind)&&f.hp>0)||null}
function stxOLAddFacilityActivity(f,text,tone=""){
  f.activity=Array.isArray(f.activity)?f.activity:[];
  f.activity.unshift({text,time:state.simTime,tone});f.activity=f.activity.slice(0,12);
}
function stxOLCreateFacility(p,kind,tier=1,source="physical"){
  const spec=stxOLFacilitySpec(kind,tier),f={id:`olf-${p.id}-${kind}`,kind,tier,name:`${p.name} ${spec.name}`,hp:spec.hp,maxHp:spec.hp,modules:[],baseDockSlots:spec.docks,dockSlots:spec.docks,dockedShipIds:[],serviceQueue:[],cargoThroughput:0,createdAt:state.simTime,source,priority:"balanced",rally:false,activity:[]};
  p.orbitalFacilities.push(f);stxOLAddFacilityActivity(f,`${spec.name} entered local service.`,"good");return f;
}
function stxOLEnsurePlanet(p){
  if(!p)return;
  p.orbitalFacilities=Array.isArray(p.orbitalFacilities)?p.orbitalFacilities:[];
  p.physicalProjects=Array.isArray(p.physicalProjects)?p.physicalProjects:[];
  const ensure=(kind,tier,source)=>{
    if(!tier)return null;
    let f=p.orbitalFacilities.find(x=>x.kind===kind);
    if(!f)f=stxOLCreateFacility(p,kind,tier,source);
    const spec=stxOLFacilitySpec(kind,tier);f.tier=clamp(Math.max(f.tier||1,tier),1,3);f.name=f.name||`${p.name} ${spec.name}`;f.maxHp=Math.max(f.maxHp||0,spec.hp);f.hp=Number.isFinite(f.hp)?clamp(f.hp,0,f.maxHp):f.maxHp;f.modules=Array.isArray(f.modules)?f.modules:[];f.dockedShipIds=Array.isArray(f.dockedShipIds)?f.dockedShipIds:[];f.serviceQueue=Array.isArray(f.serviceQueue)?f.serviceQueue:[];f.activity=Array.isArray(f.activity)?f.activity:[];f.baseDockSlots=Math.max(f.baseDockSlots||0,spec.docks);return f;
  };
  ensure("station",p.orbitals?.station||0,"legacy-station");
  ensure("military",p.orbitals?.base||0,"legacy-base");
  if(p.tradeStation){const f=ensure("trade",p.tradeStation.level||1,"trade-station");f.hp=p.tradeStation.hp;f.maxHp=p.tradeStation.maxHp;f.name=`${p.name} ${p.tradeStation.name}`;if(!f.modules.includes("exchange"))f.modules.push("exchange")}
  else{const old=p.orbitalFacilities.find(f=>f.source==="trade-station");if(old&&old.hp>0)old.hp=0}
  if((p.infra?.shipyard||0)>=2)ensure("shipyard",Math.min(3,Math.max(1,p.infra.shipyard-1)),"legacy-shipyard");
  p.orbitalFacilities.forEach(f=>{const spec=stxOLFacilitySpec(f.kind,f.tier);f.maxHp=Math.max(1,f.maxHp||spec.hp);f.hp=clamp(Number.isFinite(f.hp)?f.hp:f.maxHp,0,f.maxHp);f.modules=[...new Set(f.modules||[])].filter(m=>STX_OL_MODULES[m]);f.baseDockSlots=Math.max(spec.docks,f.baseDockSlots||0);f.dockSlots=f.baseDockSlots+(f.modules.includes("logistics")?1:0)});
}
function stxOLEnsureState(){
  state.planets.forEach(stxOLEnsurePlanet);
  state.fleets.forEach(f=>{f.readiness=f.readiness||"patrol";f.supplyReadiness=clamp(Number(f.supplyReadiness)||0,0,1);f.dockedAt=f.dockedAt||null});
  state.stxNetworkOverlay=state.stxNetworkOverlay||"none";
  state.stxSelectedRivalId=Number.isFinite(state.stxSelectedRivalId)?state.stxSelectedRivalId:1;
}

const STX_OL_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_OL_generateGalaxy();stxOLEnsureState();stxOLLastSimulationTick=state.simTime};
const STX_OL_loadGame=loadGame;
loadGame=function(){const ok=STX_OL_loadGame();if(ok){stxOLEnsureState();stxOLLastSimulationTick=state.simTime}return ok};

function stxOLScaleNeed(need,mult){return Object.fromEntries(Object.entries(need).map(([r,v])=>[r,Math.ceil(v*mult)]))}
function stxOLBuildCost(kind,p,option=null){
  const level=kind==="mining"?(p.infra.mine||0)+1:kind==="factory"?(p.infra.factory||0)+1:kind==="shipyard"?(p.infra.shipyard||0)+1:kind==="orbital"?((stxOLFacilityForPlanet(p,"station")?.tier||0)+1):kind==="military"?((stxOLFacilityForPlanet(p,"military")?.tier||0)+1):1;
  const mult=1+Math.max(0,level-1)*.42;
  if(kind==="mining")return stxOLScaleNeed({components:10,equipment:20,iron:44,titanium:26},mult);
  if(kind==="factory")return stxOLScaleNeed({components:22,equipment:10,iron:44,silicates:30,titanium:16,rare:6},mult);
  if(kind==="shipyard")return stxOLScaleNeed({components:24,equipment:14,titanium:46,iron:44,helium:26},mult);
  if(kind==="orbital")return stxOLScaleNeed({components:20,equipment:12,titanium:28,iron:28,silicates:24,helium:14},mult);
  if(kind==="military")return stxOLScaleNeed({components:26,equipment:26,titanium:48,iron:44,helium:28},mult);
  if(kind==="module")return{...(STX_OL_MODULES[option?.moduleId]?.cost||{})};
  if(kind==="repair"){const f=stxOLFacilityAt(option?.facilityId)?.f,damage=f?1-f.hp/Math.max(1,f.maxHp):.25;return stxOLScaleNeed({components:8,titanium:12,equipment:8,iron:16,silicates:12},Math.max(.4,damage*1.8))}
  return{components:35,iron:24};
}
function stxOLProjectName(kind,p,option){
  if(kind==="module")return `${STX_OL_MODULES[option.moduleId]?.name||"Station Module"} Installation`;
  if(kind==="repair")return `${stxOLFacilityAt(option.facilityId)?.f?.name||"Orbital Facility"} Repairs`;
  if(kind==="mining"){const next=(p.infra.mine||0)+1;return["Extraction Site","Mining District","Deep Extraction Complex","Deep Extraction Complex Expansion"][Math.min(3,next-1)]}
  if(kind==="factory"){const next=(p.infra.factory||0)+1;return["Local Workshops","Industrial District","Fabrication Complex","Megaforge Expansion"][Math.min(3,next-1)]}
  if(kind==="shipyard")return `Shipyard Tier ${(p.infra.shipyard||0)+1}`;
  if(kind==="orbital")return stxOLFacilitySpec("station",(stxOLFacilityForPlanet(p,"station")?.tier||0)+1).name;
  if(kind==="military")return stxOLFacilitySpec("military",(stxOLFacilityForPlanet(p,"military")?.tier||0)+1).name;
  return STX_OL_BUILD_LABELS[kind]||"Infrastructure Project";
}
function stxOLCanQueue(p,kind,option={}){
  if(!p||p.owner!==0||p.underAttack)return false;
  if((p.physicalProjects||[]).filter(q=>q.phase!=="operations").length>=STX_OL_PROJECT_LIMIT)return false;
  if(kind==="mining"&&((p.infra.mine||0)>=7||p.localProject?.type==="mine"))return false;
  if(kind==="factory"&&((p.infra.factory||0)>=7||p.localProject?.type==="factory"))return false;
  if(kind==="shipyard"&&((p.infra.shipyard||0)>=5||p.localProject?.type==="shipyard"))return false;
  if(kind==="orbital"&&(stxOLFacilityForPlanet(p,"station")?.tier||0)>=3)return false;
  if(kind==="military"&&(stxOLFacilityForPlanet(p,"military")?.tier||0)>=3)return false;
  if(kind==="module"){
    const hit=stxOLFacilityAt(option.facilityId),f=hit?.f;if(!f||f.hp<=0||f.modules.includes(option.moduleId)||f.modules.length>=stxOLFacilitySlots(f))return false;
  }
  if(kind==="repair"){const f=stxOLFacilityAt(option.facilityId)?.f;if(!f||f.hp>=f.maxHp*.995)return false}
  return !(p.physicalProjects||[]).some(q=>q.phase!=="operations"&&q.kind===kind&&(kind!=="module"||q.option?.facilityId===option.facilityId));
}
function stxOLQueueProject(p,kind,option={}){
  stxOLEnsurePlanet(p);if(!stxOLCanQueue(p,kind,option)){showToast("That infrastructure project cannot start here right now");return false}
  const need=stxOLBuildCost(kind,p,option),name=stxOLProjectName(kind,p,option),creditCost=Math.ceil(Object.values(need).reduce((n,v)=>n+v,0)*.11);
  if((empire(0)?.credits||0)<creditCost){showToast(`Need ${creditCost} credits to authorize ${name}`);return false}
  empire(0).credits-=creditCost;
  const q={id:stxOLId("olp"),kind,name,option:{...option},phase:"authorized",progress:0,need,delivered:Object.fromEntries(Object.keys(need).map(r=>[r,0])),startedAt:state.simTime,authorizedAt:state.simTime,nextDispatchAt:state.simTime,creditCost,shipmentIds:[],activity:[]};
  p.physicalProjects.push(q);p.mandateGlow=1;
  logEvent(`${p.name} authorized ${name}. Materials must now arrive through real freight movement.`,"good");
  if(typeof stxActivity==="function")stxActivity(`${name} authorized at ${p.name}; sourcing offices are assigning supplier worlds.`,p.id,null,"good");
  stxOLSourceProject(p,q,true);renderPlanet();updateHud(true);return true;
}
function stxOLProjectIncoming(q,r=null){return state.ships.filter(s=>s.projectId===q.id&&s.projectDelivery).reduce((n,s)=>n+(r?Number(s.cargo?.[r]||0):Object.values(s.cargo||{}).reduce((a,v)=>a+Number(v||0),0)),0)}
function stxOLProjectRatio(q){const total=Object.values(q.need||{}).reduce((n,v)=>n+v,0)||1,got=Object.keys(q.need||{}).reduce((n,r)=>n+Math.min(q.need[r],q.delivered?.[r]||0),0);return clamp(got/total,0,1)}
function stxOLProjectReady(q){return Object.keys(q.need||{}).every(r=>(q.delivered?.[r]||0)>=q.need[r]-.01)}
function stxOLProjectPriority(q){return empire(0)?.stxOLPriorityProjectId===q.id}
function stxOLReserveLocal(p,q){
  Object.entries(q.need).forEach(([r,required])=>{
    const remaining=Math.max(0,required-(q.delivered[r]||0)-stxOLProjectIncoming(q,r));if(remaining<=.01)return;
    const floor=["components","equipment"].includes(r)?5:12,available=Math.max(0,(p.stock[r]||0)-floor),take=Math.min(remaining,available);
    if(take>.01){const used=consume(p,r,take);q.delivered[r]=(q.delivered[r]||0)+used;if(used>0)q.activity.unshift({time:state.simTime,text:`${Math.round(used)} ${RESOURCE_LABEL[r]||r} reserved locally.`})}
  });
  q.activity=q.activity.slice(0,12);
}
function stxOLSupplierScore(p,r,dest){
  const focus=p.stxResourcePrimary===r?85:p.stxResourceSecondary===r?36:0,manufactured=r==="components"||r==="equipment"?(p.infra.factory||0)*22:0;
  return (p.stock[r]||0)+focus+manufactured-dist(p,dest)*.008-(p.underAttack?1000:0);
}
function stxOLSourceProject(p,q,immediate=false){
  if(!q||q.phase==="operations"||q.phase==="commissioning")return;
  stxOLReserveLocal(p,q);
  const priority=stxOLProjectPriority(q),logistics=stxOLFacilityForPlanet(p,"station")?.modules?.includes("logistics"),inbound=state.ships.filter(s=>s.projectId===q.id).length,maxInbound=Math.min(4,(priority?4:2)+(logistics?1:0));
  if(inbound>=maxInbound||(!immediate&&state.simTime<(q.nextDispatchAt||0)))return;
  const needs=Object.keys(q.need).map(r=>({r,remaining:Math.max(0,q.need[r]-(q.delivered[r]||0)-stxOLProjectIncoming(q,r))})).filter(x=>x.remaining>.1).sort((a,b)=>b.remaining-a.remaining);
  for(const item of needs){
    if(state.ships.filter(s=>s.projectId===q.id).length>=maxInbound)break;
    const floor=["components","equipment"].includes(item.r)?7:18,supplier=owned(p.owner).filter(x=>x!==p&&!x.underAttack&&(x.stock[item.r]||0)>floor+1).sort((a,b)=>stxOLSupplierScore(b,item.r,p)-stxOLSupplierScore(a,item.r,p))[0];
    if(!supplier){
      const open=p.orders?.some(o=>o.status==="waiting"&&o.type===`ol-${q.id}`&&o.resource===item.r);
      if(!open)addOrder(p,`ol-${q.id}`,item.r,item.remaining,priority?100:7,`${q.name} physical supply`);
      continue;
    }
    const amount=Math.min(item.remaining,Math.max(0,(supplier.stock[item.r]||0)-floor),priority?46:32);if(amount<=.1)continue;
    supplier.stock[item.r]-=amount;
    const ship=createShip("construction",supplier,p,p.owner,{cargo:{[item.r]:amount},projectId:q.id,projectDelivery:true,servicePurpose:"construction supply",dockingState:"inbound",vesselName:vesselName("construction")});
    if(!ship){supplier.stock[item.r]+=amount;break}
    q.shipmentIds.push(ship.id);q.phase="transit";q.activity.unshift({time:state.simTime,text:`${ship.vesselName} departed ${supplier.name} with ${Math.round(amount)} ${RESOURCE_LABEL[item.r]||item.r}.`});
    if(p.owner===0&&typeof stxActivity==="function")stxActivity(`${ship.vesselName} departed ${supplier.name} with ${Math.round(amount)} ${RESOURCE_LABEL[item.r]||item.r} for ${p.name}.`,p.id,null,"good");
  }
  q.nextDispatchAt=state.simTime+(priority?2.4:5.5)*(logistics ? .72 : 1);if(q.phase==="authorized")q.phase="sourcing";q.activity=q.activity.slice(0,12);
}
function stxOLCompleteProject(p,q){
  if(q.kind==="mining")p.infra.mine=(p.infra.mine||0)+1;
  else if(q.kind==="factory")p.infra.factory=(p.infra.factory||0)+1;
  else if(q.kind==="shipyard")p.infra.shipyard=(p.infra.shipyard||0)+1;
  else if(q.kind==="orbital"){
    let f=p.orbitalFacilities.find(x=>x.kind==="station"),tier=Math.min(3,(f?.tier||0)+1);if(!f)f=stxOLCreateFacility(p,"station",tier);const spec=stxOLFacilitySpec("station",tier);f.tier=tier;f.name=`${p.name} ${spec.name}`;f.maxHp=spec.hp;f.hp=spec.hp;f.baseDockSlots=spec.docks;f.dockSlots=spec.docks+(f.modules.includes("logistics")?1:0);p.orbitals.station=tier;stxOLAddFacilityActivity(f,`${spec.name} commissioned after final systems checks.`,"good")
  }else if(q.kind==="military"){
    let f=p.orbitalFacilities.find(x=>x.kind==="military"),tier=Math.min(3,(f?.tier||0)+1);if(!f)f=stxOLCreateFacility(p,"military",tier);const spec=stxOLFacilitySpec("military",tier);f.tier=tier;f.name=`${p.name} ${spec.name}`;f.maxHp=spec.hp;f.hp=spec.hp;f.baseDockSlots=spec.docks;f.dockSlots=spec.docks+(f.modules.includes("logistics")?1:0);p.orbitals.base=tier;stxOLAddFacilityActivity(f,`${spec.name} accepted its first operational watch.`,"good")
  }else if(q.kind==="module"){
    const f=stxOLFacilityAt(q.option.facilityId)?.f;if(f&&!f.modules.includes(q.option.moduleId)){f.modules.push(q.option.moduleId);stxOLAddFacilityActivity(f,`${STX_OL_MODULES[q.option.moduleId].name} commissioned.`,"good")}
  }else if(q.kind==="repair"){
    const f=stxOLFacilityAt(q.option.facilityId)?.f;if(f){f.hp=f.maxHp;if(f.kind==="trade"&&!p.tradeStation){const spec=stxOLFacilitySpec("trade",f.tier);p.tradeStation={level:f.tier,name:spec.name,hp:f.maxHp,maxHp:f.maxHp,createdAt:state.simTime,lastIncomeAt:state.simTime}}stxOLAddFacilityActivity(f,"Structural repairs and pressure tests completed.","good")}
  }
  q.phase="operations";q.progress=1;q.completedAt=state.simTime;p.mandateGlow=1;if(empire(p.owner)?.stxOLPriorityProjectId===q.id){empire(p.owner).stxOLPriorityProjectId=null;empire(p.owner).stxOLPriorityPlanetId=null}
  state.effects.push({type:"shock",x:p.x,y:p.y,life:1.6,maxLife:1.6,size:54,color:empire(p.owner)?.color||"#6feeff"});
  logEvent(`${q.name} at ${p.name} is commissioned and operational.`,"good");
  if(typeof stxActivity==="function")stxActivity(`${q.name} completed commissioning at ${p.name}.`,p.id,null,"good");
}
function stxOLTickProject(p,q,dt){
  if(q.phase==="operations")return;
  stxOLSourceProject(p,q,false);const supplied=stxOLProjectRatio(q),incoming=stxOLProjectIncoming(q);
  if(supplied>=.28&&!["commissioning"].includes(q.phase))q.phase="assembly";
  else if(incoming>0&&q.phase!=="assembly")q.phase="transit";
  else if(q.phase==="authorized")q.phase="sourcing";
  if(q.phase==="assembly"){
    const capacity=1+(p.infra.factory||0)*.22+(p.infra.shipyard||0)*.25+(stxOLFacilityForPlanet(p,"station")?.modules?.includes("logistics") ? .2 : 0);
    q.progress=Math.max(q.progress||0,supplied*.34);q.progress=Math.min(stxOLProjectReady(q) ? .94 : .86,q.progress+dt*.0042*capacity*Math.max(.35,supplied));
    if(stxOLProjectReady(q)&&q.progress>=.92){q.phase="commissioning";q.progress=.96;q.commissioningEnds=state.simTime+Math.max(5,9-capacity);if(typeof stxActivity==="function")stxActivity(`${q.name} entered commissioning at ${p.name}.`,p.id,null,"good")}
  }else q.progress=Math.max(q.progress||0,supplied*.32);
  if(q.phase==="commissioning"&&state.simTime>=(q.commissioningEnds||0))stxOLCompleteProject(p,q);
}
function stxOLTickProjects(dt){
  state.planets.forEach(p=>{stxOLEnsurePlanet(p);p.physicalProjects.forEach(q=>stxOLTickProject(p,q,dt));p.physicalProjects=p.physicalProjects.filter(q=>q.phase!=="operations"||state.simTime-(q.completedAt||0)<45)});
}
function stxOLArriveProjectDelivery(s,p){
  const q=(p.physicalProjects||[]).find(x=>x.id===s.projectId);if(!q)return STX_OL_arriveShip(s,p);
  Object.entries(s.cargo||{}).forEach(([r,v])=>q.delivered[r]=Math.min(q.need[r]||Infinity,(q.delivered[r]||0)+Number(v||0)));
  const order=s.orderId&&p.orders?.find(o=>o.id===s.orderId);if(order){order.filled=Math.min(order.amount,(order.filled||0)+Object.values(s.cargo||{}).reduce((n,v)=>n+Number(v||0),0));order.status=order.filled>=order.amount*.98?"filled":"waiting"}
  q.activity.unshift({time:state.simTime,text:`${s.vesselName} delivered ${Object.entries(s.cargo||{}).map(([r,v])=>`${Math.round(v)} ${RESOURCE_LABEL[r]||r}`).join(", ")} from ${state.planets.find(x=>x.id===s.from)?.name||"a supplier"}.`});q.activity=q.activity.slice(0,12);
  const f=stxOLFacilityForPlanet(p,"station")||stxOLFacilityForPlanet(p,"military")||stxOLFacilityForPlanet(p,"trade");if(f){f.cargoThroughput=(f.cargoThroughput||0)+Object.values(s.cargo||{}).reduce((n,v)=>n+Number(v||0),0);f.serviceQueue.push({id:s.id,name:s.vesselName,purpose:"construction delivery",until:state.simTime+5});stxOLAddFacilityActivity(f,`${s.vesselName} discharged project cargo.`,"good")}
  if(typeof stxActivity==="function")stxActivity(`${s.vesselName} reached ${p.name} and delivered cargo for ${q.name}.`,p.id,null,"good");
}

function stxOLFacilityPosition(p,f,index=0,now=performance.now()/1000){
  const base={station:.35,military:2.25,trade:4.15,shipyard:5.35}[f.kind]??index*1.4,rr=p.r*(3.15+index*.38+(f.tier||1)*.16),a=base+now*(f.kind==="military"?-.055:.045)+(+String(p.id).replace(/\D/g,"")||1)*.23;
  return{x:p.x+Math.cos(a)*rr,y:p.y+Math.sin(a)*rr*.46,a,rr};
}
function stxOLFacilityColor(f){return f.hp<=0?"#ff526f":f.kind==="military"?"#ff7b69":f.kind==="trade"?"#ffd979":f.kind==="shipyard"?"#a98bff":"#69efff"}
function stxOLDrawFacilityShape(p,f,index){
  const pos=stxOLFacilityPosition(p,f,index),s=worldToScreen(pos.x,pos.y),z=state.camera.zoom,color=stxOLFacilityColor(f);if(!visible(pos.x,pos.y,100))return;
  const selected=state.stxSelectedFacilityId===f.id,health=clamp(f.hp/Math.max(1,f.maxHp),0,1),size=Math.max(3.2,(5+(f.tier||1)*1.5)*Math.max(.62,z));ctx.save();
  if(selected){ctx.strokeStyle=color;ctx.globalAlpha=.26;ctx.lineWidth=1.5;ctx.beginPath();ctx.arc(s.x,s.y,Math.max(28,(260+(f.tier||1)*95)*z),0,6.283);ctx.stroke();ctx.globalAlpha=1;ctx.shadowColor=color;ctx.shadowBlur=18}
  ctx.translate(s.x,s.y);ctx.rotate(pos.a*.3);ctx.fillStyle="rgba(7,13,27,.96)";ctx.strokeStyle=color;ctx.lineWidth=selected?2:1.2;ctx.globalAlpha=f.hp<=0 ? .48 : 1;
  if(f.kind==="military"){ctx.beginPath();ctx.moveTo(0,-size);ctx.lineTo(size*.82,-size*.2);ctx.lineTo(size*.62,size);ctx.lineTo(-size*.62,size);ctx.lineTo(-size*.82,-size*.2);ctx.closePath();ctx.fill();ctx.stroke()}
  else if(f.kind==="trade"){ctx.beginPath();ctx.moveTo(-size,0);ctx.lineTo(-size*.45,-size*.62);ctx.lineTo(size*.5,-size*.62);ctx.lineTo(size,0);ctx.lineTo(size*.5,size*.62);ctx.lineTo(-size*.45,size*.62);ctx.closePath();ctx.fill();ctx.stroke()}
  else{ctx.fillRect(-size*.75,-size*.38,size*1.5,size*.76);ctx.strokeRect(-size*.75,-size*.38,size*1.5,size*.76);ctx.beginPath();ctx.arc(0,0,size*.72,0,6.283);ctx.stroke()}
  ctx.beginPath();ctx.moveTo(-size*1.35,0);ctx.lineTo(size*1.35,0);ctx.moveTo(0,-size*1.15);ctx.lineTo(0,size*1.15);ctx.stroke();if((f.tier||1)>=2){ctx.beginPath();ctx.ellipse(0,0,size*1.2,size*.38,0,0,6.283);ctx.stroke()}if((f.tier||1)>=3){ctx.fillStyle=color;ctx.fillRect(-2,-2,4,4)}
  if(health<.7){ctx.strokeStyle="#ff526f";ctx.globalAlpha=1-health;ctx.beginPath();ctx.moveTo(-size*.6,-size*.8);ctx.lineTo(size*.3,size*.8);ctx.stroke()}
  const docked=(f.dockedShipIds||[]).map(id=>fleetRecord(id)).filter(Boolean).slice(0,STX_OL_VISIBLE_DOCK_CRAFT);if(z>.62)docked.forEach((fleet,i)=>{const a=i*1.7+.4,dx=Math.cos(a)*size*1.7,dy=Math.sin(a)*size*.85;ctx.fillStyle=empire(fleet.owner)?.color||"#fff";ctx.beginPath();ctx.moveTo(dx+3,dy);ctx.lineTo(dx-2,dy-2);ctx.lineTo(dx-2,dy+2);ctx.closePath();ctx.fill()});ctx.restore();
  if(z>.6){ctx.save();ctx.fillStyle=color;ctx.font=`700 ${Math.max(7,8*z)}px system-ui`;ctx.textAlign="left";ctx.fillText(`${f.name.toUpperCase()} · ${Math.round(health*100)}%`,s.x+size+7,s.y-size-3);ctx.restore()}
}
function stxOLDrawProject(p,q,index){
  if(q.phase==="operations"||!visible(p.x,p.y,120))return;const base=worldToScreen(p.x,p.y),z=state.camera.zoom,a=1.2+index*.85,rr=p.r*(2.3+index*.32),x=base.x+Math.cos(a)*rr*z,y=base.y+Math.sin(a)*rr*.5*z,pr=clamp(q.progress||0,0,1);ctx.save();ctx.translate(x,y);ctx.strokeStyle="#79edff";ctx.fillStyle="rgba(9,21,38,.82)";ctx.globalAlpha=.35+pr*.65;ctx.setLineDash([3,3]);ctx.strokeRect(-11,-7,22,14);ctx.setLineDash([]);ctx.beginPath();ctx.arc(0,0,7,-1.57,-1.57+6.283*pr);ctx.stroke();if(q.phase==="assembly"||q.phase==="commissioning"){const t=performance.now()/500;for(let i=0;i<2;i++){ctx.fillStyle=i?"#ffd979":"#69efff";ctx.fillRect(Math.cos(t+i*3.1)*15-1,Math.sin(t+i*3.1)*7-1,3,2)}}ctx.restore()
}
function stxOLDrawFacilities(){state.planets.forEach(p=>{if(!visible(p.x,p.y,180))return;(p.orbitalFacilities||[]).forEach((f,i)=>stxOLDrawFacilityShape(p,f,i));(p.physicalProjects||[]).forEach((q,i)=>stxOLDrawProject(p,q,i))})}
function stxOLDrawOverlays(){
  const mode=state.stxNetworkOverlay;if(!mode||mode==="none")return;ctx.save();ctx.lineWidth=1.2;ctx.setLineDash([5,7]);
  if(mode==="logistics")state.ships.filter(s=>s.projectDelivery).forEach(s=>{const a=state.planets.find(p=>p.id===s.from),b=state.planets.find(p=>p.id===s.to);if(!a||!b)return;const sa=worldToScreen(a.x,a.y),sb=worldToScreen(b.x,b.y);ctx.strokeStyle="rgba(105,239,255,.55)";ctx.beginPath();ctx.moveTo(sa.x,sa.y);ctx.lineTo(sb.x,sb.y);ctx.stroke()});
  if(mode==="military")stxOLFacilities(0).filter(x=>x.f.kind==="military"||x.f.modules.includes("command")).forEach(({p,f})=>{const s=worldToScreen(p.x,p.y),radius=(470+(f.tier||1)*310)*state.camera.zoom;ctx.strokeStyle="rgba(255,101,112,.28)";ctx.beginPath();ctx.arc(s.x,s.y,radius,0,6.283);ctx.stroke()});
  if(mode==="trade"){const hubs=stxOLFacilities().filter(x=>x.f.kind==="trade"&&x.f.hp>0);hubs.forEach((a,i)=>{const b=hubs.slice(i+1).sort((x,y)=>dist(a.p,x.p)-dist(a.p,y.p))[0];if(!b||dist(a.p,b.p)>2600)return;const sa=worldToScreen(a.p.x,a.p.y),sb=worldToScreen(b.p.x,b.p.y);ctx.strokeStyle="rgba(255,217,121,.36)";ctx.beginPath();ctx.moveTo(sa.x,sa.y);ctx.lineTo(sb.x,sb.y);ctx.stroke()})}
  ctx.setLineDash([]);ctx.restore();
}
const STX_OL_draw=draw;
draw=function(){STX_OL_draw();stxOLDrawOverlays();stxOLDrawFacilities()};

function stxOLFleetCandidates(owner=0){return state.fleets.filter(f=>{const p=f.location&&state.planets.find(x=>x.id===f.location);return f.owner===owner&&!f.destroyed&&!!p&&!state.ships.some(s=>s.fleetId===f.id)&&!activeBattleAt(p)}).sort((a,b)=>(b.strength||0)-(a.strength||0))}
function stxOLRemoveDocking(f){if(!f?.dockedAt)return;const hit=stxOLFacilityAt(f.dockedAt);if(hit)hit.f.dockedShipIds=hit.f.dockedShipIds.filter(id=>id!==f.id);f.dockedAt=null}
function stxOLBeginService(f,facility,p){
  stxOLRemoveDocking(f);f.location=p.id;f.dockedAt=facility.id;f.readiness="staging";f.supplyReadiness=Math.max(.25,f.supplyReadiness||0);f.serviceUntil=state.simTime+Math.max(5,12-(facility.tier||1)*1.6-(facility.modules.includes("command")?2.2:0));f.status=`Staging at ${facility.name}`;f.maxServiceStrength=Math.max(f.maxServiceStrength||0,f.strength||0);facility.dockedShipIds=[...new Set([...(facility.dockedShipIds||[]),f.id])];facility.serviceQueue.push({id:f.id,name:f.name,purpose:"invasion staging",until:f.serviceUntil});stxOLAddFacilityActivity(facility,`${f.name} docked for invasion staging.`,"good");if(typeof stxActivity==="function")stxActivity(`${f.name} docked at ${facility.name} for repair, fuel, and invasion staging.`,p.id,f.id,"good");
}
function stxOLStageFleet(facilityId,fleetId){
  const hit=stxOLFacilityAt(facilityId),f=fleetRecord(fleetId);if(!hit||!f||hit.p.owner!==f.owner||f.destroyed)return false;
  const from=state.planets.find(p=>p.id===f.location);if(!from||state.ships.some(s=>s.fleetId===f.id))return showToast("That fleet is already committed or in transit");
  if(from.id===hit.p.id){stxOLBeginService(f,hit.f,hit.p);renderPlanet();return true}
  f.location=null;f.readiness="inbound";f.status=`Inbound to ${hit.f.name}`;
  const ship=createShip("fleet",from,hit.p,f.owner,{strength:f.strength,fleetId:f.id,vesselName:f.name,stationTargetId:hit.f.id,dockingState:"inbound",servicePurpose:"invasion staging",speedBoost:1});
  if(!ship){f.location=from.id;f.readiness="patrol";return false}
  stxOLAddFacilityActivity(hit.f,`${f.name} inbound from ${from.name}.`);if(typeof stxActivity==="function")stxActivity(`${f.name} departed ${from.name} for ${hit.f.name}.`,hit.p.id,f.id,"good");renderPlanet();return true;
}
function stxOLDockFleet(s,p){const hit=stxOLFacilityAt(s.stationTargetId),f=fleetRecord(s.fleetId);if(!hit||hit.p.id!==p.id||!f)return STX_OL_arriveShip(s,p);stxOLBeginService(f,hit.f,p)}
function stxOLLaunchBonus(f){if(!f||f.hp<=0)return 0;const base=f.kind==="military"?[.30,.35,.40][(f.tier||1)-1]:[.12,.20,.25][(f.tier||1)-1],health=clamp(f.hp/Math.max(1,f.maxHp),0,1);return Math.min(.45,(base+(f.modules.includes("fuel") ? .05 : 0)+(f.modules.includes("command") ? .04 : 0))*health)}
function stxOLRelayPlan(from,to,owner){
  const direct=dist(from,to);if(direct<3000)return[];let last=from;
  return stxOLFacilities(owner).filter(({p,f})=>p!==from&&p!==to&&f.hp>f.maxHp*.35&&(f.kind==="station"||f.kind==="military"||f.modules.includes("fuel"))).filter(({p})=>dist(from,p)>650&&dist(p,to)<direct-500&&dist(from,p)+dist(p,to)<direct*1.28).sort((a,b)=>dist(from,a.p)-dist(from,b.p)).filter(({p})=>{if(dist(last,p)<700)return false;last=p;return true}).slice(0,2);
}
const STX_OL_createShip=createShip;
createShip=function(type,from,to,owner,extra={}){
  let actualTo=to,opts={...extra};const f=fleetRecord(opts.fleetId),dock=f?.dockedAt&&stxOLFacilityAt(f.dockedAt);
  if(!opts.stationTargetId&&to&&(["freighter","tanker","construction","miningBarge","supply","liner","luxury","courier","research"].includes(type)||opts.commercial)){
    const arrival=stxOLFacilityForPlanet(to,"trade")||stxOLFacilityForPlanet(to,"station")||stxOLFacilityForPlanet(to,"shipyard");if(arrival)opts.stationTargetId=arrival.id;
  }
  if((type==="fleet"||type==="patrol")&&dock&&dock.p.id===from?.id&&f.readiness==="ready"){
    const bonus=stxOLLaunchBonus(dock.f);opts.speedBoost=(opts.speedBoost||1)*(1+bonus);opts.launchBonus=bonus;opts.launchedFromFacilityId=dock.f.id;opts.dockingState="departing";opts.servicePurpose="invasion launch";
    const relays=stxOLRelayPlan(from,to,owner);if(relays.length){actualTo=relays[0].p;opts.olRelayQueue=relays.slice(1).map(x=>x.p.id);opts.olRelayFinalId=to.id;opts.olRelayNames=relays.map(x=>x.f.name)}
    stxOLAddFacilityActivity(dock.f,`${f.name} launched${bonus?` with a ${Math.round(bonus*100)}% prepared-departure advantage`:""}.`,"good");dock.f.dockedShipIds=dock.f.dockedShipIds.filter(id=>id!==f.id);f.dockedAt=null;f.readiness="launched";f.supplyReadiness=Math.max(0,(f.supplyReadiness||1)-.35);
    if(typeof stxActivity==="function")stxActivity(`${f.name} launched from ${dock.f.name}; prepared transit improved by ${Math.round(bonus*100)}%.`,from.id,f.id,"good");
  }
  return STX_OL_createShip(type,from,actualTo,owner,opts);
};
function stxOLContinueRelay(s,p){
  if(!s.olRelayFinalId)return false;const nextId=(s.olRelayQueue||[]).shift()||s.olRelayFinalId,next=state.planets.find(x=>x.id===nextId);if(!next)return false;
  const facility=stxOLFacilityForPlanet(p,"military")||stxOLFacilityForPlanet(p,"station");if(facility){facility.serviceQueue.push({id:s.id,name:s.vesselName,purpose:"relay refuel",until:state.simTime+3});stxOLAddFacilityActivity(facility,`${s.vesselName} completed a rapid relay service stop.`,"good")}
  const leg=STX_OL_createShip(s.type,p,next,s.owner,{strength:s.strength,fleetId:s.fleetId,vesselName:s.vesselName,warId:s.warId,invasionPlanId:s.invasionPlanId,status:s.status,launchBonus:s.launchBonus,olRelayQueue:s.olRelayQueue,olRelayFinalId:nextId===s.olRelayFinalId?null:s.olRelayFinalId,olRelayNames:s.olRelayNames,speedBoost:1+(facility?.modules.includes("fuel") ? .12 : .06),dockingState:"departing",servicePurpose:"station relay"});
  if(leg&&typeof stxActivity==="function")stxActivity(`${s.vesselName} relayed through ${facility?.name||p.name} and departed for ${next.name}.`,p.id,s.fleetId,"good");return true;
}
function stxOLRecordCivilianDock(s,p){
  const f=s.stationTargetId?stxOLFacilityAt(s.stationTargetId)?.f:(stxOLFacilityForPlanet(p,"trade")||stxOLFacilityForPlanet(p,"station"));if(!f||f.hp<=0)return;
  const cargo=Object.values(s.cargo||{}).reduce((n,v)=>n+Number(v||0),0);f.cargoThroughput=(f.cargoThroughput||0)+Math.max(1,cargo);f.serviceQueue.push({id:s.id,name:s.vesselName,purpose:s.commercial?"commercial exchange":"cargo transfer",until:state.simTime+rand(3,7)});stxOLAddFacilityActivity(f,`${s.vesselName} docked for ${s.commercial?"commercial exchange":"cargo transfer"}.`);
}
function stxOLResolveFacilityRaid(s,p){
  const hit=stxOLFacilityAt(s.orbitalRaidFacilityId);if(!hit||hit.p.id!==p.id||hit.f.hp<=0)return false;const f=hit.f,attacker=fleetRecord(s.fleetId),defense=(f.tier||1)*13+(f.modules.includes("defense")?30:0)+(p.infra.defense||0)*4,damage=Math.max(8,(s.strength||10)*rand(.55,.92)-defense*.14);f.hp=Math.max(0,f.hp-damage);if(f.kind==="trade"&&p.tradeStation){p.tradeStation.hp=f.hp;if(f.hp<=0)p.tradeStation=null}stxOLAddFacilityActivity(f,`${s.vesselName} raided the facility for ${Math.round(damage)} structural damage.`,"danger");state.effects.push({type:"shock",x:p.x,y:p.y,life:1.3,maxLife:1.3,size:42,color:"#ff526f"});
  galacticNews(`${f.name.toUpperCase()} ${f.hp<=0?"DISABLED":"RAIDED"}`,`${empire(s.owner).name} struck the orbital facility, leaving it at ${Math.round(f.hp/f.maxHp*100)}% integrity. Its service benefits scale with surviving systems.`,"danger",p.id);
  const retreat=owned(s.owner).sort((a,b)=>dist(a,p)-dist(b,p))[0];if(attacker&&retreat){attacker.strength=Math.max(1,attacker.strength*(1-clamp(defense/(defense+(s.strength||1))*.24,.04,.28)));attacker.location=null;attacker.status=`Returning from orbital raid at ${p.name}`;STX_OL_createShip("fleet",p,retreat,s.owner,{strength:attacker.strength,fleetId:attacker.id,vesselName:attacker.name,retreat:true,speedBoost:1.15})}return true;
}
const STX_OL_arriveShip=arriveShip;
arriveShip=function(s,p){
  if(s?.projectDelivery)return stxOLArriveProjectDelivery(s,p);
  if(s?.orbitalRaidFacilityId&&stxOLResolveFacilityRaid(s,p))return;
  if(s?.olRelayFinalId&&stxOLContinueRelay(s,p))return;
  if(s?.stationTargetId&&s?.fleetId)return stxOLDockFleet(s,p);
  if(s?.commercial||["freighter","tanker","construction","miningBarge","supply"].includes(s?.type))stxOLRecordCivilianDock(s,p);
  return STX_OL_arriveShip(s,p);
};
if(typeof fillOrder==="function"){
  const STX_OL_fillOrder=fillOrder;
  fillOrder=function(dest,order){
    const before=new Set(state.ships.map(s=>s.id)),result=STX_OL_fillOrder(dest,order);if(String(order?.type||"").startsWith("ol-")){const projectId=String(order.type).slice(3),ship=state.ships.find(s=>!before.has(s.id)&&s.orderId===order.id);if(ship){ship.projectId=projectId;ship.projectDelivery=true;ship.servicePurpose="construction supply";ship.dockingState="inbound";const facility=stxOLFacilityForPlanet(dest,"station")||stxOLFacilityForPlanet(dest,"shipyard");if(facility)ship.stationTargetId=facility.id}}return result;
  };
}
const STX_OL_tickShips=tickShips;
tickShips=function(dt){
  STX_OL_tickShips(dt);
  state.ships.forEach(s=>{if((s.progress||0)<.72)return;const hit=s.stationTargetId&&stxOLFacilityAt(s.stationTargetId);if(!hit)return;const pos=stxOLFacilityPosition(hit.p,hit.f,(hit.p.orbitalFacilities||[]).indexOf(hit.f)),blend=clamp(((s.progress||0)-.72)/.28,0,1);s.x+=(pos.x-s.x)*blend*.72;s.y+=(pos.y-s.y)*blend*.72;s.dockingState=blend>.72?"docking":"inbound"});
};
if(typeof stxScanRange==="function"){
  const STX_OL_scanRange=stxScanRange;
  stxScanRange=function(p){const base=STX_OL_scanRange(p),bonus=(p.orbitalFacilities||[]).filter(f=>f.hp>0&&f.modules.includes("sensors")).reduce((n,f)=>n+420*(f.tier||1)*clamp(f.hp/f.maxHp,0,1),0);return base+bonus};
}
if(typeof buildQueueRate==="function"){
  const STX_OL_buildQueueRate=buildQueueRate;
  buildQueueRate=function(p){const base=STX_OL_buildQueueRate(p),gantry=(p.orbitalFacilities||[]).find(f=>f.hp>0&&(f.kind==="shipyard"||f.modules.includes("gantry")));return base*(gantry?1+.11*(gantry.tier||1)*clamp(gantry.hp/gantry.maxHp,0,1):1)};
}
const STX_OL_launchBuiltShip=launchBuiltShip;
launchBuiltShip=function(p,type){
  const result=STX_OL_launchBuiltShip(p,type),facility=stxOLFacilityForPlanet(p,"shipyard")||(p.orbitalFacilities||[]).find(f=>f.hp>0&&f.modules.includes("gantry"));if(!result||!facility)return result;const pos=stxOLFacilityPosition(p,facility,(p.orbitalFacilities||[]).indexOf(facility));
  if(result.id&&state.ships.includes(result)){result.x=pos.x;result.y=pos.y;result.startX=pos.x;result.startY=pos.y;result.launchedFromFacilityId=facility.id;result.dockingState="departing";stxOLAddFacilityActivity(facility,`${result.vesselName} cleared the construction gantry.`,"good")}
  else if(result.id&&state.fleets.includes(result)){result.dockedAt=facility.id;result.readiness="patrol";facility.dockedShipIds=[...new Set([...(facility.dockedShipIds||[]),result.id])];stxOLAddFacilityActivity(facility,`${result.name} commissioned into orbital service.`,"good")}
  return result;
};
function stxOLTickFacilities(dt){
  stxOLFacilities().forEach(({p,f})=>{
    f.cargoThroughput=Math.max(0,(f.cargoThroughput||0)-dt*.08);f.serviceQueue=(f.serviceQueue||[]).filter(x=>(x.until||0)>state.simTime);const health=clamp(f.hp/Math.max(1,f.maxHp),0,1),priorityMult=f.priority==="trade"?1.25:1;if(f.modules.includes("exchange")&&health>0){empire(p.owner).credits+=dt*.0007*(f.tier||1)*health*priorityMult;p.tradeVolume=(p.tradeVolume||0)+dt*.0008*(f.tier||1)*health*priorityMult}
    f.dockedShipIds=(f.dockedShipIds||[]).filter(id=>{const fleet=fleetRecord(id);return fleet&&!fleet.destroyed&&fleet.dockedAt===f.id});
    f.dockedShipIds.forEach(id=>{const fleet=fleetRecord(id);if(!fleet)return;if(f.modules.includes("repair")&&fleet.strength<(fleet.maxServiceStrength||fleet.strength))fleet.strength=Math.min(fleet.maxServiceStrength,fleet.strength+dt*.05*(f.tier||1)*health*(f.priority==="military"?1.3:1));if(fleet.readiness==="staging"){fleet.supplyReadiness=clamp((fleet.supplyReadiness||0)+dt*.075*(1+(f.tier||1)*.18)*Math.max(.25,health)*(f.priority==="military"?1.3:1),0,1);if(state.simTime>=(fleet.serviceUntil||Infinity)){fleet.readiness="ready";fleet.supplyReadiness=1;fleet.status=`INVASION READY at ${f.name}`;stxOLAddFacilityActivity(f,`${fleet.name} is INVASION READY.`,"good");if(p.owner===0&&typeof stxActivity==="function")stxActivity(`${fleet.name} is INVASION READY at ${f.name}.`,p.id,fleet.id,"good")}}});
  });
}
const STX_OL_simulate=simulate;
simulate=function(dt){STX_OL_simulate(dt);if(state.simTime-stxOLLastSimulationTick>=.48){const step=Math.min(1.2,state.simTime-stxOLLastSimulationTick);stxOLLastSimulationTick=state.simTime;stxOLTickProjects(step);stxOLTickFacilities(step)}};

/* Prepared persistent fleets are used before abstract planetary garrison groups. */
if(typeof stxMobilizeInvasionPlan==="function"){
  const STX_OL_mobilizeInvasionPlan=stxMobilizeInvasionPlan;
  stxMobilizeInvasionPlan=function(plan,force=false){
    const target=state.planets.find(p=>p.id===plan?.targetId);if(!target||!empiresAtWar(0,target.owner))return STX_OL_mobilizeInvasionPlan(plan,force);
    if(target.underAttack||state.ships.some(s=>s.owner===0&&s.to===target.id&&STX_MILITARY_TYPES.has(s.type)))return STX_OL_mobilizeInvasionPlan(plan,force);
    const ready=state.fleets.filter(f=>f.owner===0&&!f.destroyed&&f.readiness==="ready"&&f.dockedAt&&f.location&&!state.ships.some(s=>s.fleetId===f.id)).sort((a,b)=>(b.strength||0)-(a.strength||0));
    if(!ready.length)return STX_OL_mobilizeInvasionPlan(plan,force);
    ready.slice(0,force?2:1).forEach(f=>{const from=state.planets.find(p=>p.id===f.location);if(!from)return;f.location=null;createShip("fleet",from,target,0,{strength:f.strength,fleetId:f.id,vesselName:f.name,warId:getWar(0,target.owner)?.id,status:`Invading ${target.name}`,invasionPlanId:plan.id,speedBoost:1})});plan.lastLaunchAt=state.simTime;plan.status="staged fleet inbound";return true;
  };
}

function stxOLSetPriority(projectId){
  const e=empire(0),found=playerWorlds().flatMap(p=>(p.physicalProjects||[]).map(q=>({p,q}))).find(x=>x.q.id===projectId&&x.q.phase!=="operations");if(!e||!found)return;
  if(e.stxOLPriorityProjectId===projectId){e.stxOLPriorityProjectId=null;showToast("Infrastructure priority cleared")}
  else{e.stxPriorityProjectId=null;e.stxPriorityPlanetId=null;e.stxOLPriorityProjectId=projectId;e.stxOLPriorityPlanetId=found.p.id;found.q.nextDispatchAt=state.simTime;stxOLSourceProject(found.p,found.q,true);showToast(`${found.q.name} now has first claim on project freight`)}renderPlanet();
}
if(typeof stxPSSetPriority==="function"){
  const STX_OL_setStandardPriority=stxPSSetPriority;
  stxPSSetPriority=function(...args){const e=empire(0);if(e){e.stxOLPriorityProjectId=null;e.stxOLPriorityPlanetId=null}return STX_OL_setStandardPriority(...args)};
}
function stxOLQueueFacilityRaid(facilityId){
  const hit=stxOLFacilityAt(facilityId);if(!hit||hit.p.owner===0||!empiresAtWar(0,hit.p.owner)||hit.f.hp<=0)return false;const fleet=stxOLFleetCandidates(0)[0],source=fleet&&state.planets.find(p=>p.id===fleet.location);if(!fleet||!source)return showToast("No idle fleet is available for this orbital raid");fleet.location=null;fleet.status=`Raiding ${hit.f.name}`;const ship=createShip("fleet",source,hit.p,0,{strength:fleet.strength,fleetId:fleet.id,vesselName:fleet.name,orbitalRaidFacilityId:facilityId,servicePurpose:"orbital raid",speedBoost:1.08});if(!ship){fleet.location=source.id;return false}showToast(`Raid ordered against ${hit.f.name}`);return true;
}

function stxOLMaterialRows(q){return Object.keys(q.need).map(r=>{const got=q.delivered?.[r]||0,incoming=stxOLProjectIncoming(q,r);return `<span class="stx-ol-material ${got>=q.need[r]-.01?"ready":""}">${stxOLEscape(RESOURCE_LABEL[r]||r)} ${Math.floor(got)}/${Math.ceil(q.need[r])}${incoming?` · ${Math.ceil(incoming)} inbound`:""}</span>`}).join("")}
function stxOLShipmentRows(q){
  const ships=state.ships.filter(s=>s.projectId===q.id).slice(0,4);if(!ships.length)return `<div class="subtle">${q.phase==="sourcing"?"Supplier offices are searching available stockpiles.":"No project freight currently in transit."}</div>`;
  return ships.map(s=>{const from=state.planets.find(p=>p.id===s.from),cargo=Object.entries(s.cargo||{}).map(([r,v])=>`${Math.round(v)} ${RESOURCE_LABEL[r]||r}`).join(", ");return `<button class="stx-ol-shipment" data-ol-focus-ship="${s.id}"><b>${stxOLEscape(s.vesselName)}</b><span>${stxOLEscape(from?.name||"Supplier")} → ${stxOLEscape(state.planets.find(p=>p.id===s.to)?.name||"Project")} · ${stxOLEscape(cargo)} · ${fmtEta(shipEta(s))}</span></button>`}).join("");
}
function stxOLProjectCards(p){
  const active=(p.physicalProjects||[]).filter(q=>q.phase!=="operations");if(!active.length)return"";
  return `<section class="stx-ol-block"><div class="section-label">Physical Construction Logistics</div>${active.map(q=>{const prioritized=stxOLProjectPriority(q);return `<article class="project-row active stx-ol-project"><div class="stx-ol-phase">${STX_OL_PHASES[q.phase]||q.phase}</div><div class="project-head"><strong>${stxOLEscape(q.name)}</strong><b>${Math.round((q.progress||0)*100)}%</b></div><div class="project-desc">Remote materials count only after the named freight vessel reaches ${stxOLEscape(p.name)}.</div><div class="stx-ol-materials">${stxOLMaterialRows(q)}</div><div class="project-track"><i style="width:${Math.round((q.progress||0)*100)}%"></i></div><button class="stx-priority-btn ${prioritized?"active":""}" data-ol-priority="${q.id}">${prioritized?"★ PRIORITIZED":"☆ PRIORITIZE"}</button><div class="stx-ol-shipments">${stxOLShipmentRows(q)}</div></article>`}).join("")}</section>`;
}
function stxOLInfrastructureBlock(p){
  if(p.owner!==0)return"";const primary=p.stxResourcePrimary||RESOURCES.slice().sort((a,b)=>(p.quality[b]||0)-(p.quality[a]||0))[0],station=stxOLFacilityForPlanet(p,"station"),base=stxOLFacilityForPlanet(p,"military"),focus=p.stxManufacturingFocus||"components";
  return `<section class="stx-ol-block"><div class="section-label">Development & Infrastructure</div><div class="stx-ol-infra-grid">
    <article class="stx-ol-infra"><b>MINING · TIER ${p.infra.mine||0}</b><span>${stxOLEscape(RESOURCE_LABEL[primary]||primary)} primary · ${(p.stxResourceYield?.[primary]||1).toFixed(1)}× yield</span><button class="choice-btn" data-ol-build="mining" ${stxOLCanQueue(p,"mining")?"":"disabled"}>Expand Extraction</button></article>
    <article class="stx-ol-infra"><b>INDUSTRY · TIER ${p.infra.factory||0}</b><span>${stxOLEscape(focus)} focus · ${Math.round((p.factoryEfficiency||0)*100)}% flow</span><div><button class="choice-btn" data-ol-build="factory" ${stxOLCanQueue(p,"factory")?"":"disabled"}>Expand Factory</button><button class="choice-btn" data-ol-focus>${focus==="components"?"Switch to Equipment":"Switch to Components"}</button></div></article>
    <article class="stx-ol-infra"><b>ORBITAL · ${station?`TIER ${station.tier}`:"NO HUB"}</b><span>${station?`${station.dockedShipIds.length}/${station.dockSlots} berths · ${station.modules.length}/${stxOLFacilitySlots(station)} modules`:"Authorize a physical logistics hub"}</span><button class="choice-btn" data-ol-build="orbital" ${stxOLCanQueue(p,"orbital")?"":"disabled"}>${station?"Upgrade Station":"Build Orbital Hub"}</button></article>
    <article class="stx-ol-infra"><b>MILITARY · ${base?`TIER ${base.tier}`:"PLANETARY"}</b><span>${base?`${base.dockedShipIds.length} fleets staged · ${Math.round(stxOLLaunchBonus(base)*100)}% prepared launch`:"Build visible naval staging infrastructure"}</span><button class="choice-btn" data-ol-build="military" ${stxOLCanQueue(p,"military")?"":"disabled"}>${base?"Expand Base":"Build Naval Base"}</button></article>
  </div></section>`;
}
function stxOLFacilityPanel(p,f){
  const inbound=state.ships.filter(s=>s.stationTargetId===f.id||(s.to===p.id&&(s.commercial||s.projectDelivery))).slice(0,6),docked=(f.dockedShipIds||[]).map(id=>fleetRecord(id)).filter(Boolean),candidates=stxOLFleetCandidates(p.owner).filter(x=>x.dockedAt!==f.id).slice(0,5),health=Math.round(f.hp/f.maxHp*100),slots=stxOLFacilitySlots(f),available=Object.keys(STX_OL_MODULES).filter(id=>!f.modules.includes(id)&&f.modules.length<slots).slice(0,5);
  return `<section class="stx-ol-facility-panel" style="--facility:${stxOLFacilityColor(f)}"><div class="stx-ol-facility-head"><div><div class="stx-ol-phase">ORBITAL FACILITY · ${f.kind.toUpperCase()}</div><h3>${stxOLEscape(f.name)}</h3></div><b>${health}% INTEGRITY</b></div><div class="stx-ol-stat-grid"><span><label>Tier</label><b>${f.tier}</b></span><span><label>Berths</label><b>${docked.length}/${f.dockSlots}</b></span><span><label>Inbound</label><b>${inbound.length}</b></span><span><label>Throughput</label><b>${Math.round(f.cargoThroughput||0)}</b></span><span><label>Launch</label><b>+${Math.round(stxOLLaunchBonus(f)*100)}%</b></span><span><label>Modules</label><b>${f.modules.length}/${slots}</b></span></div><div class="choice-row"><button class="choice-btn" data-ol-rally="${f.id}">${empire(p.owner)?.rallyFacilityId===f.id?"✓ Fleet Rally Point":"Set as Rally Point"}</button><button class="choice-btn" data-ol-facility-priority="military" data-ol-facility="${f.id}">Prioritize Military Supply</button><button class="choice-btn" data-ol-facility-priority="trade" data-ol-facility="${f.id}">Prioritize Trade Throughput</button>${f.hp<f.maxHp?`<button class="choice-btn" data-ol-repair="${f.id}">Start Supply-Backed Repairs</button>`:""}${p.owner!==0&&empiresAtWar(0,p.owner)?`<button class="choice-btn danger-choice" data-ol-raid="${f.id}">Raid Facility</button>`:""}</div>
  <div class="stx-ol-columns"><div><div class="section-label">Docked / Servicing</div>${docked.length?docked.map(x=>`<div class="stx-ol-list"><b>${stxOLEscape(x.name)}</b><span>${stxOLEscape(x.status)} · ${Math.round((x.supplyReadiness||0)*100)}% supply</span></div>`).join(""):'<div class="subtle">No fleet is docked.</div>'}</div><div><div class="section-label">Inbound Traffic</div>${inbound.length?inbound.map(s=>`<div class="stx-ol-list"><b>${stxOLEscape(s.vesselName)}</b><span>${stxOLEscape(s.servicePurpose||s.type)} · ${fmtEta(shipEta(s))}</span></div>`).join(""):'<div class="subtle">No tracked arrivals.</div>'}</div></div>
  ${p.owner===0&&candidates.length?`<div class="section-label">Stage a Fleet Here</div><div class="choice-row">${candidates.map(x=>`<button class="choice-btn" data-ol-stage-fleet="${x.id}" data-ol-stage-at="${f.id}">${stxOLEscape(x.name)} · ${Math.round(x.strength)}</button>`).join("")}</div>`:""}
  ${p.owner===0&&available.length?`<div class="section-label">Available Module Projects</div><div class="choice-row">${available.map(id=>`<button class="choice-btn" title="${stxOLEscape(STX_OL_MODULES[id].desc)}" data-ol-module="${id}" data-ol-module-at="${f.id}">${stxOLEscape(STX_OL_MODULES[id].name)}</button>`).join("")}</div>`:""}
  <div class="section-label">Recent Station Activity</div>${(f.activity||[]).slice(0,4).map(a=>`<div class="stx-ol-list"><b>${stxOLEscape(a.text)}</b><span>${fmtEta(Math.max(0,state.simTime-a.time))} ago</span></div>`).join("")||'<div class="subtle">No recent station operations.</div>'}</section>`;
}
function stxOLFocusShip(id){const s=state.ships.find(x=>x.id===id);if(!s)return;stxFocusPoint(s.x,s.y,1.35);showToast(`${s.vesselName} · ${fmtEta(shipEta(s))} to destination`)}
function stxOLBindPlanetActions(p,body){
  body.querySelectorAll("[data-ol-build]").forEach(b=>b.onclick=()=>stxOLQueueProject(p,b.dataset.olBuild));
  body.querySelectorAll("[data-ol-priority]").forEach(b=>b.onclick=()=>stxOLSetPriority(b.dataset.olPriority));
  body.querySelectorAll("[data-ol-focus-ship]").forEach(b=>b.onclick=()=>stxOLFocusShip(b.dataset.olFocusShip));
  body.querySelectorAll("[data-ol-stage-fleet]").forEach(b=>b.onclick=()=>stxOLStageFleet(b.dataset.olStageAt,b.dataset.olStageFleet));
  body.querySelectorAll("[data-ol-module]").forEach(b=>b.onclick=()=>stxOLQueueProject(p,"module",{facilityId:b.dataset.olModuleAt,moduleId:b.dataset.olModule}));
  body.querySelectorAll("[data-ol-repair]").forEach(b=>b.onclick=()=>stxOLQueueProject(p,"repair",{facilityId:b.dataset.olRepair}));
  body.querySelectorAll("[data-ol-raid]").forEach(b=>b.onclick=()=>stxOLQueueFacilityRaid(b.dataset.olRaid));
  body.querySelectorAll("[data-ol-rally]").forEach(b=>b.onclick=()=>{const e=empire(p.owner);if(e){e.rallyFacilityId=b.dataset.olRally;stxOLFacilities(p.owner).forEach(x=>x.f.rally=x.f.id===e.rallyFacilityId);showToast("Fleet rally point updated");renderPlanet()}});
  body.querySelectorAll("[data-ol-facility-priority]").forEach(b=>b.onclick=()=>{const hit=stxOLFacilityAt(b.dataset.olFacility);if(hit){hit.f.priority=b.dataset.olFacilityPriority;stxOLAddFacilityActivity(hit.f,`${b.dataset.olFacilityPriority==="military"?"Military supply":"Trade throughput"} priority established.`,"good");renderPlanet()}});
  body.querySelectorAll("[data-ol-focus]").forEach(b=>b.onclick=()=>{p.stxManufacturingFocus=p.stxManufacturingFocus==="components"?"equipment":"components";showToast(`${p.name} now emphasizes ${p.stxManufacturingFocus}`);renderPlanet()});
}
const STX_OL_renderPlanet=renderPlanet;
renderPlanet=function(){
  STX_OL_renderPlanet();const p=state.selected,body=$("planetBody");if(!p||!body)return;stxOLEnsurePlanet(p);
  const selected=(p.orbitalFacilities||[]).find(f=>f.id===state.stxSelectedFacilityId);if(!selected&&state.stxSelectedFacilityId)state.stxSelectedFacilityId=null;if(selected)body.insertAdjacentHTML("afterbegin",stxOLFacilityPanel(p,selected));
  body.insertAdjacentHTML("beforeend",stxOLProjectCards(p)+stxOLInfrastructureBlock(p));stxOLBindPlanetActions(p,body);
};

function stxOLSelectFacilityAtEvent(e){
  if(drag?.moved)return;const w=screenToWorld(e.clientX,e.clientY),hit=stxOLFacilities().map(({p,f},index)=>({p,f,index,pos:stxOLFacilityPosition(p,f,(p.orbitalFacilities||[]).indexOf(f))})).filter(x=>visible(x.pos.x,x.pos.y,80)&&Math.hypot(x.pos.x-w.x,x.pos.y-w.y)<Math.max(15,24/state.camera.zoom)).sort((a,b)=>Math.hypot(a.pos.x-w.x,a.pos.y-w.y)-Math.hypot(b.pos.x-w.x,b.pos.y-w.y))[0];if(!hit)return;
  e.preventDefault();e.stopImmediatePropagation();state.stxSelectedFacilityId=hit.f.id;state.selected=hit.p;hit.p.intel=Math.min(1,hit.p.intel+.1);stxFocusPoint(hit.pos.x,hit.pos.y,1.18);renderPlanet();$("leftPanel").classList.add("open");showToast(`${hit.f.name} · ${Math.round(hit.f.hp/hit.f.maxHp*100)}% integrity`);
}
canvas.addEventListener("pointerup",stxOLSelectFacilityAtEvent,true);

function stxOLWarTargetCards(enemyId){return owned(enemyId).filter(p=>!p.underAttack).sort((a,b)=>stxNearestPlayerDistance(a)-stxNearestPlayerDistance(b)).slice(0,8).map(p=>`<button class="stx-ol-war-target" data-ol-war-target="${p.id}"><b>${stxOLEscape(p.name)}</b><span>${stxOLEscape(p.specialization)} · strength ${Math.round(stxTargetStrength(p))} · nearest ${Math.round(stxNearestPlayerDistance(p))}u</span></button>`).join("")||'<div class="subtle">No valid invasion target is currently visible.</div>'}
function stxOLRivalActions(enemyId){
  const e=empire(enemyId),war=getWar(0,enemyId);if(!e)return"";const treaty=(state.simTime-(e.lastTreaty??-999))<70,deployable=stxOLFleetCandidates(0).length,relationText=relationLabel(relation(0,enemyId));
  if(!war)return `<section class="stx-ol-rival-actions"><div class="stx-ol-phase">DIRECT DIPLOMATIC CONTROL · ${stxOLEscape(e.name)}</div><h3>${treaty?"Break Treaty & Declare War":"Declare War"}</h3><div class="stx-ol-stat-grid"><span><label>Relation</label><b>${relationText}</b></span><span><label>Our fleet</label><b>${Math.round(empireFleet(0))}</b></span><span><label>Estimated rival</label><b>${Math.round(empireFleet(enemyId))}</b></span><span><label>Deployable fleets</label><b>${deployable}</b></span></div><p class="subtle">Declaration creates a persistent war only. It will not automatically order an invasion.${deployable?"":" Warning: no idle named fleet is currently deployable."}${treaty?" Breaking the active peace guarantee will inflict an additional diplomatic penalty.":""}</p><button class="choice-btn danger-choice stx-ol-declare" data-ol-declare="${enemyId}">${treaty?"BREAK TREATY & DECLARE WAR":"DECLARE WAR"}</button></section>`;
  const duration=Math.max(0,state.simTime-war.startedAt);return `<section class="stx-ol-rival-actions"><div class="stx-ol-phase">ACTIVE WAR FRONT · ${stxOLEscape(e.name)}</div><h3>${fmtEta(duration)} at war · conquests ${(war.conquests[0]||0)}–${(war.conquests[enemyId]||0)}</h3><div class="choice-row"><button class="choice-btn danger-choice" data-ol-target-mode="${enemyId}">Select Invasion Target</button><button class="choice-btn" data-ol-open-front="${enemyId}">Open War Front</button><button class="choice-btn" data-ol-war-stage="${enemyId}">Stage Fleets</button><button class="choice-btn" data-ol-peace="${enemyId}" ${empire(0).credits<tributeDemand(enemyId)?"disabled":""}>Offer ${tributeDemand(enemyId)} cr for Peace</button></div><div id="stxOlWarChoices"></div></section>`;
}
function stxOLDecorateRivals(){
  const grid=$("rivalGrid");if(!grid)return;const rivals=stxVisibleRivals(),cards=[...grid.querySelectorAll(".rival-card")];cards.forEach((card,i)=>{const e=rivals[i];if(!e)return;card.dataset.olRival=e.id;card.classList.toggle("stx-ol-selected-rival",e.id===state.stxSelectedRivalId);card.onclick=ev=>{if(ev.target.closest("button"))return;state.stxSelectedRivalId=e.id;stxOLDecorateRivals()}});
  let panel=$("stxOlRivalActions");if(!panel){panel=document.createElement("div");panel.id="stxOlRivalActions";grid.after(panel)}panel.innerHTML=stxOLRivalActions(state.stxSelectedRivalId);
  panel.querySelectorAll("[data-ol-declare]").forEach(b=>b.onclick=()=>{const id=Number(b.dataset.olDeclare),foe=empire(id),treaty=(state.simTime-(foe.lastTreaty??-999))<70,msg=`DECLARE WAR ON ${foe.name}?\n\nRelation: ${relationLabel(relation(0,id))}\nOur visible fleet: ${Math.round(empireFleet(0))}\nTheir estimated fleet: ${Math.round(empireFleet(id))}\nDeployable named fleets: ${stxOLFleetCandidates(0).length}\n${treaty?"ACTIVE PEACE GUARANTEE WILL BE BROKEN.\n":""}\nThis creates war state only; no invasion will launch.`;if(!confirm(msg))return;if(treaty){adjustRelation(0,id,-.18);foe.lastTreatyBrokenAt=state.simTime}const war=declareWar(0,id,treaty?"a broken peace guarantee":"a direct imperial declaration");if(war){showToast(`War declared on ${foe.name} · no invasion ordered`);renderRivals();updateHud(true)}});
  panel.querySelectorAll("[data-ol-target-mode]").forEach(b=>b.onclick=()=>{const box=$("stxOlWarChoices");box.innerHTML=`<div class="section-label">Choose an objective — selection does not launch until the war plan mobilizes</div>${stxOLWarTargetCards(Number(b.dataset.olTargetMode))}`;box.querySelectorAll("[data-ol-war-target]").forEach(x=>x.onclick=()=>{const p=state.planets.find(q=>q.id===x.dataset.olWarTarget);if(p&&stxQueueInvasion(p,"Direct Rival Powers war-front objective",false)){state.selected=p;state.stxSelectedFacilityId=null;stxFocusPoint(p.x,p.y,1.2);$("rivalsModal").hidden=true;renderPlanet();showToast(`${p.name} selected as invasion objective · fleets not yet launched`)}})});
  panel.querySelectorAll("[data-ol-open-front]").forEach(b=>b.onclick=()=>{const id=Number(b.dataset.olOpenFront),battle=state.battles.find(x=>{const p=state.planets.find(q=>q.id===x.planetId);return p&&(p.owner===id||x.attacker===id||x.defender===id)}),plan=(empire(0).invasionPlans||[]).find(x=>state.planets.find(p=>p.id===x.targetId)?.owner===id),p=battle?state.planets.find(x=>x.id===battle.planetId):state.planets.find(x=>x.id===plan?.targetId)||owned(id).sort((a,b)=>stxNearestPlayerDistance(a)-stxNearestPlayerDistance(b))[0];if(p){state.selected=p;stxFocusPoint(p.x,p.y,1.3);$("rivalsModal").hidden=true;renderPlanet()}});
  panel.querySelectorAll("[data-ol-war-stage]").forEach(b=>b.onclick=()=>{const box=$("stxOlWarChoices"),facilities=stxOLFacilities(0).filter(x=>x.f.hp>0&&(x.f.kind==="military"||x.f.kind==="station")),fleets=stxOLFleetCandidates(0);box.innerHTML=`<div class="section-label">Stage strongest available fleet</div>${facilities.slice(0,8).map(x=>`<button class="stx-ol-war-target" data-ol-quick-stage="${x.f.id}"><b>${stxOLEscape(x.f.name)}</b><span>${x.f.dockedShipIds.length}/${x.f.dockSlots} berths · +${Math.round(stxOLLaunchBonus(x.f)*100)}% prepared launch</span></button>`).join("")||'<div class="subtle">Build an Orbital Hub or Naval Base to stage fleets.</div>'}`;box.querySelectorAll("[data-ol-quick-stage]").forEach(x=>x.onclick=()=>{const fleet=fleets.find(f=>!f.dockedAt);if(!fleet)return showToast("No idle fleet is available to stage");stxOLStageFleet(x.dataset.olQuickStage,fleet.id);renderRivals()})});
  panel.querySelectorAll("[data-ol-peace]").forEach(b=>b.onclick=()=>offerTribute(Number(b.dataset.olPeace)));
}
const STX_OL_renderRivals=renderRivals;
renderRivals=function(){STX_OL_renderRivals();stxOLDecorateRivals()};

function stxOLInstallControls(){
  if($("stxNetworkControls"))return;const box=document.createElement("div");box.id="stxNetworkControls";box.className="stx-ol-network-controls";box.innerHTML=`<span>NETWORK</span><button data-ol-overlay="none" class="active">OFF</button><button data-ol-overlay="logistics">LOGISTICS</button><button data-ol-overlay="military">MILITARY</button><button data-ol-overlay="trade">TRADE</button>`;document.body.appendChild(box);box.querySelectorAll("[data-ol-overlay]").forEach(b=>b.onclick=()=>{state.stxNetworkOverlay=b.dataset.olOverlay;box.querySelectorAll("button").forEach(x=>x.classList.toggle("active",x===b));showToast(`${b.dataset.olOverlay==="none"?"Strategic overlays hidden":`${b.dataset.olOverlay} network overlay active`}`)})
}
function stxOLInstallStyles(){
  if($("stxOlStyles"))return;const style=document.createElement("style");style.id="stxOlStyles";style.textContent=`
  .stx-ol-block{margin-top:16px}.stx-ol-project{border-color:rgba(105,239,255,.24)!important}.stx-ol-phase{font:800 9px/1.2 system-ui;letter-spacing:.14em;color:#69efff;text-transform:uppercase}.stx-ol-materials{display:flex;gap:5px;flex-wrap:wrap;margin:8px 0}.stx-ol-material{padding:4px 6px;border:1px solid rgba(255,255,255,.09);border-radius:6px;font:700 9px system-ui;color:#ffcf69}.stx-ol-material.ready{color:#6ef2a2;border-color:rgba(110,242,162,.24)}.stx-ol-shipments{display:grid;gap:5px;margin-top:8px}.stx-ol-shipment,.stx-ol-war-target{display:flex;width:100%;flex-direction:column;align-items:flex-start;gap:2px;padding:8px;border:1px solid rgba(105,239,255,.15);border-radius:8px;background:rgba(4,12,25,.64);color:#ecf9ff;text-align:left;cursor:pointer}.stx-ol-shipment span,.stx-ol-war-target span{font:600 9px system-ui;color:#94a8bd}.stx-ol-infra-grid{display:grid;grid-template-columns:1fr 1fr;gap:7px}.stx-ol-infra{display:flex;flex-direction:column;gap:6px;padding:9px;border:1px solid rgba(255,255,255,.09);border-radius:9px;background:rgba(7,14,28,.62)}.stx-ol-infra>b{font:800 10px system-ui;color:#eefaff}.stx-ol-infra>span{font:600 9px/1.35 system-ui;color:#9db1c6}.stx-ol-facility-panel{border:1px solid color-mix(in srgb,var(--facility),transparent 68%);border-radius:12px;background:linear-gradient(145deg,color-mix(in srgb,var(--facility),transparent 92%),rgba(5,10,24,.92));padding:12px;margin-bottom:14px;box-shadow:0 0 22px color-mix(in srgb,var(--facility),transparent 89%)}.stx-ol-facility-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}.stx-ol-facility-head h3{margin:4px 0;color:var(--facility)}.stx-ol-facility-head>b{font:800 10px system-ui;color:var(--facility)}.stx-ol-stat-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin:9px 0}.stx-ol-stat-grid>span{display:flex;flex-direction:column;padding:6px;border-radius:7px;background:rgba(255,255,255,.035)}.stx-ol-stat-grid label{font:700 8px system-ui;color:#7f93aa;text-transform:uppercase}.stx-ol-stat-grid b{font:800 11px system-ui;color:#edf8ff}.stx-ol-columns{display:grid;grid-template-columns:1fr 1fr;gap:10px}.stx-ol-list{display:flex;justify-content:space-between;gap:8px;padding:5px 0;border-bottom:1px solid rgba(255,255,255,.055);font:700 9px system-ui}.stx-ol-list span{color:#91a6bb;text-align:right}.stx-ol-network-controls{position:fixed;left:12px;bottom:12px;z-index:24;display:flex;align-items:center;gap:4px;padding:6px;border:1px solid rgba(105,239,255,.16);border-radius:9px;background:rgba(3,8,19,.82);backdrop-filter:blur(10px)}.stx-ol-network-controls span{font:800 8px system-ui;color:#6e8298;letter-spacing:.12em}.stx-ol-network-controls button{border:0;border-radius:5px;padding:5px 7px;background:transparent;color:#7890a6;font:800 8px system-ui;cursor:pointer}.stx-ol-network-controls button.active{background:rgba(105,239,255,.14);color:#7ef2ff}.stx-ol-selected-rival{border-color:rgba(105,239,255,.48)!important;box-shadow:0 0 18px rgba(105,239,255,.09)}.stx-ol-rival-actions{margin-top:12px;padding:14px;border:1px solid rgba(255,91,111,.22);border-radius:12px;background:linear-gradient(145deg,rgba(255,91,111,.06),rgba(5,10,22,.76))}.stx-ol-rival-actions h3{margin:5px 0 8px}.stx-ol-declare{font-weight:900!important;letter-spacing:.07em}@media(max-width:720px){.stx-ol-infra-grid,.stx-ol-columns{grid-template-columns:1fr}.stx-ol-network-controls{left:7px;bottom:7px;max-width:calc(100vw - 14px);overflow:auto}.stx-ol-network-controls span{display:none}.stx-ol-stat-grid{grid-template-columns:repeat(2,1fr)}}`;
  document.head.appendChild(style);
}

stxOLEnsureState();stxOLInstallStyles();stxOLInstallControls();
