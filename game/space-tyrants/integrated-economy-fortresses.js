/* Space Tyrants — integrated economy and physical fortress construction.
   All quantities use the existing stock units (population/crew: millions).
   Project ledgers reserve real goods. Remote projects separately track goods
   staged at the sponsor, in transit, and delivered to the construction site. */
const STX_IF_VERSION=2;
const STX_IF_RESOURCES=["iron","silicates","titanium","helium","rare","components","equipment","trained"];
const STX_IF_RAW=new Set(RESOURCES);
const STX_IF_MODES=["balanced","project","export"];
const STX_IF_STAGES=["Normal sourcing","Strategic sourcing","Production mobilization","Personnel mobilization","Emergency freight"];
let stxIFLastTick=-999,stxIFLastRouting=-999,stxIFLastPlan=-999;
let stxIFPlans=new Map();
function stxIFN(v,d=0){const n=Number(v);return Number.isFinite(n)?n:d}
function stxIFLabel(r){return stxSDResourceLabel(r)}
function stxIFEpsilon(r){return r==="trained"?1e-9:1e-6}
function stxIFEnsurePlanet(p){
  if(!p)return p;
  p.stxResourceRouting=p.stxResourceRouting||{};
  p.stxFreightCapacity=Math.max(0,stxIFN(p.stxFreightCapacity));
  p.stxEconomicTotals=p.stxEconomicTotals||{};
  for(const kind of ["production","consumption","imports","exports"])p.stxEconomicTotals[kind]=p.stxEconomicTotals[kind]||{};
  return p;
}
function stxIFRoute(p,r){stxIFEnsurePlanet(p);return STX_IF_MODES.includes(p?.stxResourceRouting?.[r])?p.stxResourceRouting[r]:"balanced"}
function stxIFPriorityProject(owner=0){return stxPSPriorityProject(owner)}
function stxIFNeed(d,r){return d?Math.max(0,stxSDRemaining(d,r)-stxSDIncomingAmount(d,r)):0}
function stxIFLocalNeed(p,r){return stxSDDescriptors(p).reduce((n,d)=>n+stxSDRemaining(d,r),0)}
function stxIFExpansionDescriptor(p){return stxSDDescriptors(p).find(d=>d.kind==="expansion")||null}
function stxIFExpansionLoad(p){
  const d=stxIFExpansionDescriptor(p);if(!d)return 0;
  return clamp(.35+(1-stxSDCeiling(d))*.65,.35,1);
}
function stxIFSetRoute(planetId,r,mode){
  const p=state.planets.find(x=>x.id===planetId);
  if(!p||p.owner!==0||!STX_IF_RESOURCES.includes(r)||!STX_IF_MODES.includes(mode))return false;
  stxIFEnsurePlanet(p);p.stxResourceRouting[r]=mode;p.mandateGlow=1;
  showToast(`${p.name} · ${stxIFLabel(r)}: ${mode==="project"?"local priority":mode}`);renderPlanet();return true;
}
function stxIFCycleRoute(planetId,r){const p=state.planets.find(x=>x.id===planetId);if(!p)return false;return stxIFSetRoute(planetId,r,STX_IF_MODES[(STX_IF_MODES.indexOf(stxIFRoute(p,r))+1)%STX_IF_MODES.length])}
function stxIFRecord(p,kind,r,amount){
  if(!p||amount<=0)return;stxIFEnsurePlanet(p);const bucket=p.stxEconomicTotals[kind];bucket[r]=stxIFN(bucket[r])+amount;
}
function stxIFRemote(d){return d?.kind==="deep-base"||d?.kind==="deep-upgrade"}
function stxIFRemoteIncoming(base,r,mission){return state.ships.filter(s=>!s.stxCancelled&&!s.stxIntercepted&&s.stxDeepTransit&&s.deepBaseId===base.id&&s.stxDeepMission===mission).reduce((n,s)=>n+stxIFN(s.cargo?.[r]),0)}
function stxIFRemoteDescriptor(base,p){
  const q=base.status==="construction"?base.project:base.status==="operational"?base.upgradeProject:null;if(!q)return null;
  const kind=base.status==="construction"?"deep-base":"deep-upgrade",mission=kind==="deep-base"?"construction":"fortress-upgrade";
  q.staged=q.staged||{};q.delivered=q.delivered||{};q.shipmentIds=Array.isArray(q.shipmentIds)?q.shipmentIds:[];
  // An old station's progress does not imply extra paid material at its sponsor.
  q.stxSupply=q.stxSupply||{delivered:{},orderIds:{},createdAt:state.simTime,lastProgressAt:state.simTime,lastSupplyAt:state.simTime};
  for(const r of Object.keys(q.need||{}))q.stxSupply.delivered[r]=Math.min(q.need[r],stxIFN(q.staged[r])+stxIFN(q.delivered[r])+stxIFRemoteIncoming(base,r,mission));
  const d={kind,q,p,base,mission,title:`${base.name} · ${kind==="deep-base"?"Construction":stxIFTierName(base,q.targetTier)}`,need:q.need,orderType:kind,label:`${base.name} modules`};
  d.id=stxSDProjectId(q,kind);d.priority=stxSDPriority(kind,q,p);stxSDEnsureSupply(d);return d;
}
const STX_IF_descriptors=stxSDDescriptors;
stxSDDescriptors=function(p){
  const descs=STX_IF_descriptors(p);if(!p||p.owner===null)return descs;
  for(const d of descs)if(d.kind==="expansion"&&d.q.stxColonyRecipe){d.need=d.q.stxColonyRecipe;stxSDEnsureSupply(d)}
  for(const q of p.physicalProjects||[])if(q.phase!=="operations"){
    q.stxSupply=q.stxSupply||{delivered:q.delivered||{},orderIds:{},createdAt:q.startedAt,lastProgress:q.progress||0,lastProgressAt:state.simTime,lastSupplyAt:state.simTime};
    q.delivered=q.delivered||{};q.stxSupply.delivered=q.delivered;
    const d={kind:"physical",q,p,title:q.name,need:q.need,orderType:"physical",label:q.name};d.id=stxSDProjectId(q,"physical");d.priority=stxSDPriority(d.kind,q,p);stxSDEnsureSupply(d);descs.push(d);
  }
  for(const base of (state.deepSpaceBases||[]))if(base.sponsorPlanetId===p.id&&base.owner===p.owner){const d=stxIFRemoteDescriptor(base,p);if(d)descs.push(d)}
  return descs;
};
const STX_IF_priority=stxSDPriority;
stxSDPriority=function(kind,q,p){
  if(p&&empire(p.owner)?.stxPriorityProjectId===stxSDProjectId(q,kind))return 100;
  return STX_IF_priority(kind,q,p);
};
// The same allocator reserves materials for ships, colonies, infrastructure,
// and station staging. Delivered materials never remain exportable stock.
stxSDAllocatePlanet=function(p){
  if(!p||p.owner===null)return;
  const descs=stxSDDescriptors(p).sort((a,b)=>b.priority-a.priority||(a.q.startedAt||0)-(b.q.startedAt||0));
  for(const d of descs){
    const priority=stxIFPlans.get(p.owner)?.priority;
    for(const r of Object.keys(d.need||{})){
      if(priority&&priority.id!==d.id&&stxIFNeed(priority,r)>stxIFEpsilon(r)&&!stxIFProductionPrerequisite(d))continue;
      const take=Math.min(stxSDRemaining(d,r),Math.max(0,stxIFN(p.stock[r])));if(take<=0)continue;
      p.stock[r]-=take;d.q.stxSupply.delivered[r]=stxSDDelivered(d,r)+take;
      if(stxIFRemote(d))d.q.staged[r]=stxIFN(d.q.staged[r])+take;
      d.q.stxSupply.lastSupplyAt=state.simTime;
    }
    stxSDEnsureOrders(d);
  }
};
function stxIFProductionPrerequisite(d){
  const type=d.kind==="local"?d.q.type:d.kind==="physical"?(d.q.kind==="mining"?"mine":d.q.kind):null;
  return ["mine","factory","training"].includes(type)&&stxIFN(d.p.infra[type])<1;
}
function stxIFReserve(p,r,priority=0,stage=0){
  const normal=r==="trained"?.00012:stxRTReserve(r),local=stxIFLocalNeed(p,r);
  if(priority>=100&&stage>=1)return normal*(stage>=4?0:stage>=2?.025:.12);
  const factor=stxIFRoute(p,r)==="project"?1.65:stxIFRoute(p,r)==="export"?.25:1;
  return local+normal*factor;
}
stxRTPlanetReserve=function(p,r){return stxIFReserve(p,r)};
function stxIFCapacityShare(d){
  const projects=stxSDDescriptors(d.p),weight=x=>x.priority>=100?3:stxIFProductionPrerequisite(x)?1.1:1;
  const total=projects.reduce((n,x)=>n+weight(x),0),capacity=1+Math.max(0,stxIFN(d.p.infra.factory)-1)*.22+stxIFN(d.p.infra.shipyard)*.12;
  return Math.min(1,capacity*weight(d)/Math.max(1,total));
}
const STX_IF_advance=stxSDAdvance;
stxSDAdvance=function(d,dt,rate){return STX_IF_advance(d,dt,rate*stxIFCapacityShare(d))};
function stxIFMobilization(p){
  const m={mine:1,factory:1,training:1,research:1,label:""},expansion=stxIFExpansionLoad(p),plan=stxIFPlans.get(p.owner),priority=plan?.priority;
  if(expansion){m.mine*=1-.18*expansion;m.factory*=1-.30*expansion;m.training*=1-.22*expansion;m.research*=1-.12*expansion;m.label="Colony mobilization"}
  const projects=stxSDDescriptors(p),load=Math.min(.32,Math.max(0,projects.length-1)*.08);
  m.factory*=1-load;m.research*=1-load*.5;
  if(priority&&plan.stage>=2){
    const short=plan.shortages,raw=Object.entries(short).filter(([r,a])=>STX_IF_RAW.has(r)&&a>.2).sort((a,b)=>b[1]-a[1])[0];
    if(raw&&(p.stxResourcePrimary===raw[0]||p.stxResourceSecondary===raw[0])){m.mine*=1.28;m.factory*=.90;m.label=`Priority ${stxIFLabel(raw[0])} extraction`}
    if(stxIFN(short.components)+stxIFN(short.equipment)>.3&&p.infra.factory>0){m.factory*=1.22;m.mine*=.94;m.research*=.9;m.label="Priority industrial production"}
    if(short.trained>1e-9&&plan.stage>=3){m.training*=1.35;m.factory*=.92;m.label="Priority personnel mobilization"}
  }
  const colony=p.stxColony;
  if(colony&&colony.stage!=="developed"){const readiness=stxIFN(colony.readiness,1);m.factory*=.2+.8*readiness;m.mine*=.65+.35*readiness;m.research*=.4+.6*readiness}
  return m;
}
const STX_IF_tickPlanet=tickPlanet;
tickPlanet=function(p,dt){
  if(!p||p.owner===null||dt<=0)return STX_IF_tickPlanet(p,dt);
  stxIFEnsurePlanet(p);const m=stxIFMobilization(p),infra=p.infra,original={},before={...p.stock};
  for(const [key,factor] of [["mine",m.mine],["factory",m.factory],["training",m.training],["research",m.research]]){original[key]={value:infra[key],scaled:infra[key]*factor};infra[key]*=factor}
  p.stxIFEconomicMobilization={...m,updatedAt:state.simTime};
  try{STX_IF_tickPlanet(p,dt)}finally{
    // A project can finish during this tick. Preserve its infrastructure gain.
    for(const [key,{value,scaled}] of Object.entries(original))infra[key]=value+(infra[key]-scaled);
  }
  for(const r of STX_IF_RESOURCES){
    const made=STX_IF_RAW.has(r)?stxIFN(p.stxRawProduction?.[r])*dt:r==="components"?stxIFN(p.stxAllocationOutput?.componentRate)*dt:r==="equipment"?stxIFN(p.stxAllocationOutput?.equipmentRate)*dt:stxIFN(p.stxCrewRate)*dt;
    stxIFRecord(p,"production",r,made);stxIFRecord(p,"consumption",r,Math.max(0,stxIFN(before[r])+made-stxIFN(p.stock[r])));
  }
};
function stxIFCrewMobilization(dt){
  for(const e of state.empires){
    const plan=stxIFPlans.get(e.id),d=plan?.priority;if(!d||plan.stage<3)continue;
    const worlds=owned(e.id).filter(p=>!p.underAttack),available=worlds.reduce((n,p)=>n+Math.max(0,stxIFN(p.stock.trained)-.00001),0);
    let gap=Math.max(0,stxIFNeed(d,"trained")-available);if(gap<=1e-9)continue;
    for(const p of worlds.sort((a,b)=>b.infra.training-a.infra.training)){
      if(gap<=1e-9)break;
      const training=stxIFN(p.infra.training)+stxIFN(p.infra.shipyard)*.28;
      if(!training||p.pop<=.018)continue;
      const crew=Math.min(gap,training*.000055*dt,Math.max(0,p.pop-.018)*.004,Math.max(0,stxIFN(p.stock.equipment))/700);
      if(crew<=1e-9){addOrder(p,"priority-training","equipment",8,101,"Priority training requires Equipment");continue}
      p.pop-=crew;p.stock.trained=stxIFN(p.stock.trained)+crew;p.stock.equipment-=crew*700;gap-=crew;
      p.stxIFMobilizedUntil=state.simTime+10;stxIFRecord(p,"production","trained",crew);stxIFRecord(p,"consumption","equipment",crew*700);
      if(state.simTime-stxIFN(p.stxIFCrewMobilizedAt,-999)>45){p.stxIFCrewMobilizedAt=state.simTime;if(e.id===0)logEvent(`${p.name} is training civilian recruits for ${d.title}; Equipment and population are being committed.`,"warning")}
    }
  }
}
function stxIFFreightLimit(owner){return 3+owned(owner).reduce((n,p)=>n+Math.floor(stxIFN(p.infra.shipyard)*.7)+Math.floor(stxIFN(p.stxFreightCapacity)),0)+(state.deepSpaceBases||[]).filter(b=>b.owner===owner&&b.status==="operational"&&["trade","logistics"].includes(b.type)).reduce((n,b)=>n+Math.floor(b.tier*2*b.supplyReadiness),0)}
function stxIFFreightUsed(owner){return state.ships.filter(s=>s.owner===owner&&!s.stxCancelled&&!s.stxIntercepted&&Object.values(s.cargo||{}).some(v=>v>0)&&s.type!=="colony"&&s.type!=="migrant"&&s.type!=="refugee").length}
function stxIFRouteBonus(from,to,owner){
  const length=dist(from,to);let bonus=0;
  for(const b of (state.deepSpaceBases||[]))if(b.owner===owner&&b.status==="operational"&&["logistics","trade"].includes(b.type)&&dist(from,b)+dist(b,to)<length*1.25+120)bonus=Math.max(bonus,b.tier*(b.type==="logistics"?.12:.07)*b.supplyReadiness);
  return bonus;
}
function stxIFCanFreight(owner){return stxIFFreightUsed(owner)<stxIFFreightLimit(owner)&&stxIFN(empire(owner)?.credits)>=.04}
function stxIFChargeFreight(ship,from,to){
  const e=empire(ship.owner);e.credits=Math.max(0,e.credits-.04);
  const fuelNeed=.05+Math.min(1.2,dist(from,to)/5000),fuel=Math.min(fuelNeed,Math.max(0,stxIFN(from.stock?.helium)));
  if(from.stock){from.stock.helium-=fuel;stxIFRecord(from,"consumption","helium",fuel)}
  ship.speed*= (.55+.45*fuel/fuelNeed)*(1+stxIFRouteBonus(from,to,ship.owner));
  ship.stxIFPhysicalCargo=true;ship.stxIFIntendedOwner=ship.owner;
  for(const [r,a] of Object.entries(ship.cargo||{}))stxIFRecord(from,"exports",r,a);
}
const STX_IF_createShip=createShip;
createShip=function(type,from,to,owner,extra={}){
  const freight=STX_DS_FREIGHT_TYPES.has(type)&&Object.values(extra.cargo||{}).some(v=>v>0);
  if(freight&&!stxIFCanFreight(owner))return null;
  const ship=STX_IF_createShip(type,from,to,owner,extra);
  if(ship&&freight)stxIFChargeFreight(ship,from,to);
  return ship;
};
const STX_IF_createTransit=stxDSCreateTransit;
stxDSCreateTransit=function(args){
  const freight=["construction","fortress-upgrade","supply"].includes(args.mission)&&Object.values(args.cargo||{}).some(v=>v>0);
  if(freight&&!stxIFCanFreight(args.owner))return null;
  const ship=STX_IF_createTransit(args);if(ship&&freight)stxIFChargeFreight(ship,args.fromPoint,args.targetPoint);return ship;
};
const STX_IF_launchBuiltShip=launchBuiltShip;
launchBuiltShip=function(p,type){const result=STX_IF_launchBuiltShip(p,type);if(["freighter","tanker","construction"].includes(type)){stxIFEnsurePlanet(p);p.stxFreightCapacity+=1}return result};
// All domestic orders use one source-selection policy. Nearby available cargo
// wins; lower-priority projects yield while factories/training keep their inputs.
fillOrder=function(dest,o){
  if(!dest||!o||dest.owner===null)return;
  const carrier=state.ships.some(s=>s.orderId===o.id&&!s.stxIntercepted&&!s.stxCancelled);
  if(o.status==="in transit"&&!carrier)o.status="waiting";
  if(o.status!=="waiting"||carrier||!stxIFCanFreight(dest.owner))return;
  const plan=stxIFPlans.get(dest.owner),priority=plan?.priority,project=stxSDDescriptors(dest).find(d=>Object.values(d.q.stxSupply?.orderIds||{}).includes(o.id));
  if(priority&&project&&priority.id!==project.id&&stxIFNeed(priority,o.resource)>stxIFEpsilon(o.resource)&&!stxIFProductionPrerequisite(project))return;
  const amountNeeded=project?stxIFNeed(project,o.resource):Math.max(0,stxIFN(o.amount)-stxIFN(o.filled));
  if(amountNeeded<=stxIFEpsilon(o.resource)){o.status="filled";return}
  const inputPriority=priority&&plan.stage>=2&&["material","factory","equipment","priority-training"].includes(o.type)&&["components","equipment","trained"].some(r=>stxIFNeed(priority,r)>stxIFEpsilon(r));
  const level=project?.id===priority?.id||inputPriority?100:stxIFN(o.priority),stage=plan?.stage||0;
  const sources=owned(dest.owner).filter(p=>p!==dest&&!p.underAttack).map(p=>({p,surplus:Math.max(0,stxIFN(p.stock[o.resource])-stxIFReserve(p,o.resource,level,stage))})).filter(x=>x.surplus>stxIFEpsilon(o.resource)).sort((a,b)=>dist(a.p,dest)-dist(b.p,dest)||b.surplus-a.surplus);
  const source=sources[0];if(!source){o.stxFailedAttempts=stxIFN(o.stxFailedAttempts)+1;return}
  const amount=Math.min(amountNeeded,source.surplus,o.resource==="trained"?.014:70),type=o.resource==="helium"?"tanker":["trained","equipment"].includes(o.resource)?"supply":"freighter";
  source.p.stock[o.resource]-=amount;
  const ship=createShip(type,source.p,dest,dest.owner,{cargo:{[o.resource]:amount},orderId:o.id,stxProjectId:project?.id,speedBoost:level>=100&&stage>=4?1.15:1,vesselName:o.resource==="trained"?`${source.p.name} Personnel Transport`:vesselName(type)});
  if(!ship){source.p.stock[o.resource]+=amount;return}
  o.status="in transit";o.stxLastAttempt=state.simTime;ship.stxIFRedistribution=stxIFRoute(source.p,o.resource)==="export";
};
function stxIFRoutingTick(){
  if(state.simTime-stxIFLastRouting<3)return;stxIFLastRouting=state.simTime;
  for(const e of state.empires){
    const orders=owned(e.id).flatMap(p=>{stxSDDescriptors(p).forEach(stxSDEnsureOrders);return p.orders.map(o=>({p,o}))});
    orders.sort((a,b)=>b.o.priority-a.o.priority||b.o.age-a.o.age);
    for(const {p,o} of orders)if(o.status==="waiting")fillOrder(p,o);
  }
}

/* Expansion is authorized with capital, supplied with a diversified recipe,
   and only deducts settlers when a real colony ship can leave. */
const STX_IF_startExpansion=startExpansionProject;
startExpansionProject=function(e,p,target,directed=false){
  if(!e||!p)return false;
  const credits=8+Math.min(.025,p.pop*.035)*200;
  if(e.credits<credits)return false;
  if(e.id!==0&&!directed&&stxSDDescriptors(p).length>=(e.doctrine==="expansion"?4:3))return false;
  const ok=STX_IF_startExpansion(e,p,target,directed);if(!ok)return ok;
  e.credits-=credits;const q=p.expansionProject;q.stxColonyCredits=credits;q.goal=Math.max(.004,q.goal);
  q.stxColonyRecipe={iron:24,silicates:22,titanium:8,helium:16,components:14,equipment:9,trained:Math.max(.00015,q.goal*.035)};
  q.stxRecipeNeed=q.stxColonyRecipe;return ok;
};
tickExpansionProject=function(p,dt){
  const q=p.expansionProject;if(!q)return;q.goal=Math.max(.004,q.goal);const target=state.planets.find(x=>x.id===q.targetId);
  if(!target||target.owner!==null){
    // Work is cancelled locally; unlaunched committed materials remain here.
    for(const [r,a] of Object.entries(q.stxSupply?.delivered||{}))p.stock[r]=stxIFN(p.stock[r])+a;
    p.expansionProject=null;return;
  }
  if(p.underAttack)return;
  const d=stxIFExpansionDescriptor(p);q.volunteers=Math.min(q.goal,q.volunteers+expansionRecruitRate(p,q)*dt*stxIFCapacityShare(d));stxSDEnsureOrders(d);
  q.status=q.volunteers<q.goal?"recruiting":stxSDAllDelivered(d)?"launch ready":"awaiting colony supplies";
  if(q.volunteers<q.goal||!stxSDAllDelivered(d))return;
  const settlers=Math.min(q.volunteers,Math.max(0,p.pop-.004));if(settlers<=0)return;
  const cargo={population:settlers,trained:stxIFN(d.need.trained)},ship=createShip("colony",p,target,p.owner,{cargo,strength:4,missionTitle:`Settlement of ${target.name}`,volunteers:settlers,stxIFColonyMission:true});
  if(!ship)return;
  p.pop-=settlers;p.expansionProject=null;
  if(p.owner===0)logEvent(`${fmtNum(settlers)} settlers and ${fmtNum(cargo.trained)} technical crew departed for ${target.name}.`,"good");
};
const STX_IF_arriveShip=arriveShip;
arriveShip=function(s,p){
  if(s?.stxIntercepted||s?.stxCancelled)return;
  if(s.stxIFReturnCargo){
    if(p.owner===s.owner){for(const [r,a] of Object.entries(s.cargo||{})){if(r==="population")p.pop+=a;else p.stock[r]=stxIFN(p.stock[r])+a;stxIFRecord(p,"imports",r,a)}return}
  }
  if((s.stxIFPhysicalCargo&&p.owner!==s.owner&&!s.crossBorder&&!s.commercial)||(s.type==="colony"&&p.owner!==null)){
    if(s.type==="colony"&&p.owner===s.owner){p.pop+=stxIFN(s.cargo.population);p.stock.trained=stxIFN(p.stock.trained)+stxIFN(s.cargo.trained);return}
    const home=owned(s.owner).filter(x=>!x.underAttack).sort((a,b)=>dist(p,a)-dist(p,b))[0];
    if(home){const returned=STX_IF_createShip("supply",p,home,s.owner,{cargo:{...s.cargo},stxIFReturnCargo:true,vesselName:`Returning ${s.vesselName}`,speedBoost:.75});if(returned)return}
    // No controlled landing site/capacity: the cargo is lost; its demand reopens.
    return;
  }
  const newColony=s.type==="colony"&&p.owner===null;
  const result=STX_IF_arriveShip(s,p);
  if(newColony&&p.owner===s.owner){
    p.pop=Math.max(.000001,stxIFN(s.cargo.population,.004));
    // Neutral map stockpiles were never mined. A new outpost gets only cargo.
    for(const r of STX_IF_RESOURCES)p.stock[r]=r==="trained"?stxIFN(s.cargo.trained):0;
    p.stxColony={stage:"landing",age:0,development:0,readiness:1,isolatedFor:0,sponsorPlanetId:s.from,arrivedAt:state.simTime};
  }
  if(s.stxIFPhysicalCargo)for(const [r,a] of Object.entries(s.cargo||{}))stxIFRecord(p,"imports",r,a);
  return result;
};
function stxIFColonyTick(p,dt){
  const c=p.stxColony;if(!c||c.stage==="developed"||p.owner===null)return;
  const demand={equipment:.012,components:.016,helium:.01,silicates:.018},entries=Object.entries(demand),paid=entries.reduce((n,[r,a])=>Math.min(n,Math.max(0,stxIFN(p.stock[r]))/(a*dt||1)),1);
  for(const [r,a] of entries){const amount=a*dt*paid;p.stock[r]-=amount;stxIFRecord(p,"consumption",r,amount);if(p.stock[r]<a*40)addOrder(p,"colony-support",r,a*150,9,`${p.name} colony life support and construction`)}
  c.age+=dt;c.readiness=clamp(stxIFN(c.readiness,1)+(paid-stxIFN(c.readiness,1))*Math.min(1,dt*.07),0,1);
  c.development+=dt*paid;c.isolatedFor=paid<.2?c.isolatedFor+dt:Math.max(0,c.isolatedFor-dt*2);
  c.stage=c.age<8?"landing":c.development<60?"outpost":c.development<180?"young colony":c.development<360?"established":"developed";
  if(c.isolatedFor>160){p.unrest=clamp(p.unrest+dt*.0001,0,1);p.pop=Math.max(.004,p.pop-p.pop*dt*.000012)}
  if(c.isolatedFor>50&&state.simTime-stxIFN(c.lastWarningAt,-999)>90){c.lastWarningAt=state.simTime;if(p.owner===0)logEvent(`${p.name}'s governor requests Equipment and construction shipments. Colony development is paused until supply improves.`,"warning")}
}
const STX_IF_populationEfficiency=populationEfficiency;
populationEfficiency=function(p){return STX_IF_populationEfficiency(p)*(p?.stxColony&&p.stxColony.stage!=="developed"?.4+.6*stxIFN(p.stxColony.readiness,1):1)};

/* -------------------------- Fortress upgrades -------------------------- */
function stxIFTierName(base,tier=base?.tier||1){const names={military:["Frontier Bastion","Sector Fortress","System Citadel"],trade:["Trade Anchorage","Orbital Exchange","Grand Commerce Ring"],logistics:["Logistics Relay","Fleet Waystation","Strategic Logistics Nexus"],sensor:["Listening Outpost","Deepwatch Array","Far-Reach Observatory"]};return(names[base?.type]||names.logistics)[clamp(tier,1,3)-1]}
function stxIFUpgradeCost(base,target=(base?.tier||1)+1){
  if(!base||target<2||target>3)return null;const mult=target===2?.78:1.28,out={};
  for(const [r,a] of Object.entries(stxDSBaseSpec(base).cost))out[r]=Math.max(3,Math.ceil(a*mult));
  out.components+=target*8;out.equipment+=target*7;out.titanium+=target*10;if(target===3)out.rare=stxIFN(out.rare)+18;return out;
}
function stxIFUpgradeRatio(q){const total=Object.values(q?.need||{}).reduce((n,v)=>n+v,0)||1;return q?clamp(Object.entries(q.need).reduce((n,[r,a])=>n+Math.min(a,stxIFN(q.delivered?.[r])),0)/total,0,1):0}
function stxIFUpgradeReady(q){return!!q&&Object.entries(q.need||{}).every(([r,a])=>stxIFN(q.delivered?.[r])>=a-stxIFEpsilon(r))}
function stxIFStartUpgrade(baseId,automatic=false){
  const base=stxDSBase(baseId);if(!base||(!automatic&&base.owner!==0)||base.status!=="operational"||base.tier>=3||base.upgradeProject)return false;
  const p=state.planets.find(x=>x.id===base.sponsorPlanetId),need=stxIFUpgradeCost(base),e=empire(base.owner);
  if(!p||p.owner!==base.owner||p.underAttack||!need)return false;
  const credits=Math.ceil(Object.values(need).reduce((n,v)=>n+v,0)*.09);if(e.credits<credits){if(!automatic)showToast(`Need ${credits} credits to authorize the upgrade`);return false}
  e.credits-=credits;base.upgradeProject={id:stxDSId("ifup"),targetTier:base.tier+1,need,staged:{},delivered:{},progress:0,nextDispatchAt:state.simTime,shipmentIds:[],startedAt:state.simTime,credits};
  stxDSAddActivity(base,`${stxIFTierName(base,base.tier+1)} authorized; awaiting physical modules.`,"good");
  stxSDDescriptors(p).forEach(stxSDEnsureOrders);stxDSPersist();
  if(!automatic){showToast(`${base.name} · upgrade authorized`);renderPlanet()}return true;
}
function stxIFUpgradeIncoming(base,r){return stxIFRemoteIncoming(base,r,"fortress-upgrade")}
function stxIFSourceRemote(base,mission){
  const p=state.planets.find(x=>x.id===base.sponsorPlanetId),q=mission==="construction"?base.project:base.upgradeProject;
  if(!p||p.owner!==base.owner||p.underAttack||!q||state.simTime<stxIFN(q.nextDispatchAt))return;
  const d=stxIFRemoteDescriptor(base,p);if(!d)return;const before={...p.stock};stxSDAllocatePlanet(p);for(const r of STX_IF_RESOURCES)stxIFRecord(p,"consumption",r,Math.max(0,stxIFN(before[r])-stxIFN(p.stock[r])));
  let inbound=state.ships.filter(s=>!s.stxCancelled&&!s.stxIntercepted&&s.deepBaseId===base.id&&s.stxDeepMission===mission).length;
  for(const r of Object.keys(q.need)){
    if(inbound>=3)break;
    const amount=Math.min(stxIFN(q.staged[r]),q.need[r]-stxIFN(q.delivered[r])-stxIFRemoteIncoming(base,r,mission),40);if(amount<=stxIFEpsilon(r))continue;
    const s=stxDSCreateTransit({type:q.shipmentIds.length?"freighter":"construction",owner:base.owner,fromPoint:p,targetPoint:base,fromPlanetId:p.id,baseId:base.id,mission,cargo:{[r]:amount},vesselName:`${p.name} Engineering Convoy`,extra:{stxIFRemoteProjectId:q.id}});
    if(!s)break;q.staged[r]-=amount;q.shipmentIds.push(s.id);q.shipmentIds=q.shipmentIds.slice(-30);inbound++;
    q.phase="freight en route";stxDSAddActivity(base,`${s.vesselName} departed with ${Math.round(amount)} ${stxIFLabel(r)}.`,"good");
  }
  q.nextDispatchAt=state.simTime+3;stxIFRemoteDescriptor(base,p);
}
function stxIFSourceUpgrade(base){return stxIFSourceRemote(base,"fortress-upgrade")}
stxDSSourceProject=function(base){return stxIFSourceRemote(base,"construction")};
function stxIFArriveUpgrade(ship,base){
  const q=base?.upgradeProject;if(!q||base.owner!==ship.owner||base.status!=="operational"||ship.stxCancelled||ship.stxIntercepted||(ship.stxIFRemoteProjectId&&ship.stxIFRemoteProjectId!==q.id))return;
  for(const [r,a] of Object.entries(ship.cargo||{}))if(q.need[r])q.delivered[r]=Math.min(q.need[r],stxIFN(q.delivered[r])+a);
  stxDSAddActivity(base,`${ship.vesselName} delivered modules to the installation.`,"good");
}
const STX_IF_arrival=stxDSHandleTransitArrival;
stxDSHandleTransitArrival=function(s){if(s?.stxDeepMission==="fortress-upgrade")return stxIFArriveUpgrade(s,stxDSBase(s.deepBaseId));return STX_IF_arrival(s)};
function stxIFCompleteUpgrade(base){
  const q=base?.upgradeProject;if(!q||!stxIFUpgradeReady(q)||q.progress<.999||base.status!=="operational")return false;
  const old=base.maxHp;base.tier=q.targetTier;base.maxHp=stxDSBaseSpec(base).maxHp+(base.tier-1)*68;base.hp=Math.min(base.maxHp,base.hp+Math.max(0,base.maxHp-old));
  delete base.upgradeProject;stxDSAddActivity(base,`${stxIFTierName(base)} commissioned.`,"good");
  if(base.owner===0){logEvent(`${base.name} is now a Tier ${base.tier} ${stxIFTierName(base)}.`,"good");showToast(`${base.name} · Tier ${base.tier} operational`)}stxDSPersist();return true;
}
function stxIFUpgradeTick(dt){
  for(const base of stxDSBases().filter(b=>b.status==="operational"&&b.upgradeProject)){
    const p=state.planets.find(x=>x.id===base.sponsorPlanetId);if(!p||p.owner!==base.owner)continue;
    const q=base.upgradeProject;stxIFSourceUpgrade(base);const ratio=stxIFUpgradeRatio(q),d=stxIFRemoteDescriptor(base,p);
    q.progress=Math.min(stxIFUpgradeReady(q)?1:ratio*.96,stxIFN(q.progress)+dt*.008*(1+p.infra.factory*.12)*stxIFCapacityShare(d));
    if(stxIFUpgradeReady(q)&&q.progress>=.999)stxIFCompleteUpgrade(base);
  }
}
stxDSTickProjects=function(dt){
  for(const base of stxDSBases().filter(b=>b.status==="construction")){
    const p=state.planets.find(x=>x.id===base.sponsorPlanetId);if(!p||p.owner!==base.owner)continue;
    const q=base.project;if(!q)continue;stxDSSourceProject(base);const d=stxIFRemoteDescriptor(base,p),ratio=stxIFUpgradeRatio(q),ready=stxIFUpgradeReady(q);
    q.progress=Math.min(ready?1:ratio*.96,stxIFN(q.progress)+dt*.005*(1+p.infra.factory*.18+p.infra.shipyard*.22)*stxIFCapacityShare(d));
    q.phase=ratio===0?"awaiting deliveries":ready?"final assembly":"site assembly";
    if(ready&&q.progress>=.999)stxDSCommission(base);
  }
};
function stxIFUpkeep(base){
  const scale=Math.pow(1.7,(base.tier||1)-1)*(1+Math.max(0,stxDSBases(base.owner,true).length-3)*.04);
  return{helium:.025*scale,equipment:.012*scale,components:.008*scale,credits:.004*scale};
}
function stxIFServiceCapacity(base){return Math.max(1,Math.floor((2+base.tier*2)*(base.type==="logistics"?1.4:1)*Math.max(.2,base.supplyReadiness)))}
stxDSSendSupply=function(base){
  if(base.status!=="operational"||state.ships.some(s=>!s.stxIntercepted&&!s.stxCancelled&&s.deepBaseId===base.id&&s.stxDeepMission==="supply"))return false;
  base.stxSupplyStock=base.stxSupplyStock||{};const upkeep=stxIFUpkeep(base),p=stxDSSupplySource(base);if(!p)return false;
  const cargo={};for(const [r,a] of Object.entries(upkeep))if(r!=="credits"){
    const desired=Math.max(0,a*(base.type==="logistics"?180:120)-stxIFN(base.stxSupplyStock[r]));
    const take=Math.min(desired,Math.max(0,stxIFN(p.stock[r])-stxIFLocalNeed(p,r)-stxRTReserve(r)*.08));
    if(take>1e-6)cargo[r]=take;
    if(take<desired*.5)addOrder(p,`station-upkeep-${base.id}`,r,desired,6,`${base.name} maintenance supply`);
  }
  if(!Object.keys(cargo).length){base.nextSupplyAt=state.simTime+6;return false}
  for(const [r,a] of Object.entries(cargo))p.stock[r]-=a;
  const ship=stxDSCreateTransit({type:"supply",owner:base.owner,fromPoint:p,targetPoint:base,fromPlanetId:p.id,baseId:base.id,mission:"supply",cargo,vesselName:`${p.name} Base Tender`});
  if(!ship){for(const [r,a] of Object.entries(cargo))p.stock[r]+=a;return false}base.nextSupplyAt=state.simTime+18;return true;
};
stxDSArriveSupply=function(s,base){
  if(!base||base.status!=="operational"||base.owner!==s.owner||s.stxCancelled||s.stxIntercepted)return;
  base.stxSupplyStock=base.stxSupplyStock||{};
  for(const [r,a] of Object.entries(s.cargo||{}))if(STX_IF_RESOURCES.includes(r))base.stxSupplyStock[r]=stxIFN(base.stxSupplyStock[r])+a;
  base.lastSupplyAt=state.simTime;stxDSAddActivity(base,`${s.vesselName} unloaded maintenance reserves.`,"good");
};
stxDSTickBases=function(dt){
  for(const base of stxDSBases().filter(b=>b.status==="operational")){
    const e=empire(base.owner);if(!e)continue;base.stxSupplyStock=base.stxSupplyStock||{};
    const upkeep=stxIFUpkeep(base),paid=Object.entries(upkeep).reduce((n,[r,a])=>Math.min(n,Math.max(0,stxIFN(r==="credits"?e.credits:base.stxSupplyStock[r]))/(a*dt||1)),1);
    for(const [r,a] of Object.entries(upkeep)){if(r==="credits")e.credits=Math.max(0,e.credits-a*dt*paid);else base.stxSupplyStock[r]=Math.max(0,stxIFN(base.stxSupplyStock[r])-a*dt*paid)}
    const blockaded=state.deepSpaceOperations.some(o=>o.active!==false&&o.targetType==="base"&&o.targetId===base.id&&o.owner!==base.owner&&stxDSOwnerAtWar(o.owner,base.owner));
    base.supplyReadiness=clamp(base.supplyReadiness+(paid*(blockaded?.6:1)-base.supplyReadiness)*Math.min(1,dt*.035),0,1);
    base.tradeThroughput=Math.max(0,stxIFN(base.tradeThroughput)-dt*.04);
    if(state.simTime>=stxIFN(base.nextSupplyAt))stxDSSendSupply(base);
    base.dockedFleetIds=base.dockedFleetIds.filter(id=>{const f=fleetRecord(id);return f&&!f.destroyed&&f.deepSpaceBaseId===base.id});
    const served=new Set(base.dockedFleetIds.slice(0,stxIFServiceCapacity(base)));
    for(const id of base.dockedFleetIds){const f=fleetRecord(id);if(!served.has(id)||base.supplyReadiness<.2||paid===0)continue;
      if(f.strength<f.maxServiceStrength)f.strength=Math.min(f.maxServiceStrength,f.strength+dt*.045*base.tier*base.supplyReadiness);
      if(f.readiness==="servicing"){
        f.stxServiceWork=stxIFN(f.stxServiceWork)+dt*base.supplyReadiness;
        if(f.stxServiceWork>=Math.max(4,stxDSBaseSpec(base).service-base.tier*.7)){
          const bonus=stxDSLaunchBonus(base);f.readiness="base prepared";f.supplyReadiness=base.supplyReadiness;f.preparedUntil=state.simTime+(state.commandCycle||45)*2;f.preparedSpeed=bonus.speed;f.preparedCoordination=bonus.coordination;f.status=`BASE PREPARED at ${base.name}`;
        }
      }
    }
    base.serviceQueue=base.serviceQueue.filter(x=>fleetRecord(x.fleetId)?.readiness==="servicing");
    if(base.type==="trade"){e.credits+=dt*.013*base.tier*base.supplyReadiness;base.tradeThroughput+=dt*.008*base.tier*base.supplyReadiness}
  }
  for(const f of state.fleets)if(f.preparedUntil&&state.simTime>=f.preparedUntil){f.preparedUntil=0;f.preparedSpeed=0;f.preparedCoordination=0;if(f.readiness==="base prepared")f.readiness="patrol"}
};
const STX_IF_dock=stxDSDockFleet;
stxDSDockFleet=function(f,b){const ok=STX_IF_dock(f,b);if(ok)f.stxServiceWork=0;return ok};

/* Planner cadence is simulation time, never rendering time. Cached shortage
   summaries keep the production path independent of empire-wide routing. */
STX_RT_FOCUSES.mobilization={label:"Heavy Mobilization",description:"Commit civilian labor to heavy industry; more raw inputs, slower growth and lower tax income.",mining:.85,components:1.35,equipment:1.4,summary:"+35% Components / +40% Equipment · −15% mining, growth and local tax income"};
function stxIFPlanOwner(e){
  const worlds=owned(e.id);if(!worlds.length){stxIFPlans.delete(e.id);return}
  const descriptors=worlds.flatMap(p=>stxSDDescriptors(p)),activeIds=new Set(descriptors.map(d=>d.id));
  for(const p of worlds)for(const o of p.orders)if(String(o.type).startsWith("stx-")&&!activeIds.has(o.type.slice(4))){o.status="filled";o.filled=o.amount}
  let priority=descriptors.find(d=>d.id===e.stxPriorityProjectId);
  if(!priority){e.stxPriorityProjectId=null;e.stxPriorityPlanetId=null}
  if(e.id!==0&&state.simTime>=stxIFN(e.stxNextEconomyPlanAt)){
    e.stxNextEconomyPlanAt=state.simTime+20;
    if(!priority){
      const score=d=>(e.doctrine==="expansion"&&d.kind==="expansion"?45:e.doctrine==="industry"&&stxIFProductionPrerequisite(d)?42:e.doctrine==="commerce"&&stxIFRemote(d)?38:d.kind==="ship"?32:20)+Math.min(25,(state.simTime-stxIFN(d.q.startedAt))/20);
      priority=descriptors.sort((a,b)=>score(b)-score(a))[0];
      if(priority){e.stxPriorityProjectId=priority.id;e.stxPriorityPlanetId=priority.p.id;e.stxPrioritySetAt=state.simTime}
    }
    let shortage=priority?Object.keys(priority.need).sort((a,b)=>stxIFNeed(priority,b)/Math.max(stxIFEpsilon(b),priority.need[b])-stxIFNeed(priority,a)/Math.max(stxIFEpsilon(a),priority.need[a]))[0]:null;
    if(shortage==="trained"&&worlds.reduce((n,p)=>n+p.stock.equipment,0)<worlds.length*6)shortage="equipment";
    worlds.forEach((p,i)=>{
      p.stxEconomicFocus=shortage==="equipment"?"equipment":shortage==="components"?"components":STX_IF_RAW.has(shortage)?"mining":e.doctrine==="industry"?(i%2?"equipment":"components"):"balanced";
      if(worlds.length>=3&&i%3===2)p.stxEconomicFocus="balanced";
      for(const r of STX_IF_RESOURCES)p.stxResourceRouting[r]=stxIFLocalNeed(p,r)>0?"project":r===p.stxResourcePrimary||r===p.stxResourceSecondary?"export":"balanced";
    });
    const upgrade=stxDSBases(e.id,true).filter(b=>!b.upgradeProject&&b.tier<3&&b.supplyReadiness>.65).sort((a,b)=>b.tradeThroughput-a.tradeThroughput)[0];
    if(upgrade&&e.credits>70&&!stxDSBases(e.id).some(b=>b.upgradeProject)&&worlds.length>=3){
      const need=stxIFUpgradeCost(upgrade),affordable=Object.entries(need).every(([r,a])=>worlds.reduce((n,p)=>n+stxIFN(p.stock[r]),0)>=a*.7);
      if(affordable&&stxIFStartUpgrade(upgrade.id,true)&&!priority){
        const p=state.planets.find(p=>p.id===upgrade.sponsorPlanetId);priority=stxIFRemoteDescriptor(upgrade,p);e.stxPriorityProjectId=priority.id;e.stxPriorityPlanetId=p.id;e.stxPrioritySetAt=state.simTime;
      }
    }
  }
  if(priority){priority.priority=100;const age=Math.max(0,state.simTime-stxIFN(e.stxPrioritySetAt,priority.q.startedAt)),stage=age>=80?4:age>=50?3:age>=30?2:age>=12?1:0;
    stxIFPlans.set(e.id,{priority,stage,shortages:Object.fromEntries(Object.keys(priority.need).map(r=>[r,stxIFNeed(priority,r)]))});
    priority.q.stxPriorityStage=stage;
  }else stxIFPlans.set(e.id,{priority:null,stage:0,shortages:{}});
  for(const d of descriptors){d.priority=stxSDPriority(d.kind,d.q,d.p);stxSDEnsureOrders(d);for(const id of Object.values(d.q.stxSupply.orderIds)){const o=d.p.orders.find(x=>x.id===id);if(o)o.priority=d.priority}}
}
function stxIFSnapshot(e){
  const worlds=owned(e.id),previous=e.stxEconomicOverview,span=Math.max(.001,state.simTime-stxIFN(previous?.at,state.simTime)),resources={};
  for(const r of STX_IF_RESOURCES){
    const sum=kind=>worlds.reduce((n,p)=>n+stxIFN(p.stxEconomicTotals?.[kind]?.[r]),0),production=sum("production"),consumption=sum("consumption"),best=kind=>worlds.slice().sort((a,b)=>stxIFN(b.stxEconomicTotals?.[kind]?.[r])-stxIFN(a.stxEconomicTotals?.[kind]?.[r]))[0];
    resources[r]={stock:worlds.reduce((n,p)=>n+stxIFN(p.stock[r]),0),production,consumption,productionRate:previous?Math.max(0,production-stxIFN(previous.resources?.[r]?.production))/span:0,consumptionRate:previous?Math.max(0,consumption-stxIFN(previous.resources?.[r]?.consumption))/span:0,exporter:best("exports")?.name||"—",importer:best("imports")?.name||"—"};
  }
  e.stxEconomicOverview={at:state.simTime,resources,freightUsed:stxIFFreightUsed(e.id),freightCapacity:stxIFFreightLimit(e.id)};
}
function stxIFPlan(){
  if(state.simTime-stxIFLastPlan<3)return;stxIFLastPlan=state.simTime;
  state.planets.forEach(stxIFEnsurePlanet);
  for(const b of stxDSBases()){
    const p=state.planets.find(x=>x.id===b.sponsorPlanetId);if(p?.owner===b.owner)continue;
    const next=stxDSSupplySource(b);if(!next)continue;
    for(const q of [b.project,b.upgradeProject].filter(Boolean)){
      // Staged goods at a conquered sponsor remain there, never teleport.
      if(p)for(const [r,a] of Object.entries(q.staged||{}))p.stock[r]=stxIFN(p.stock[r])+a;
      q.staged={};
    }
    b.sponsorPlanetId=next.id;
  }
  for(const e of state.empires){stxIFPlanOwner(e);stxIFSnapshot(e)}
}
function stxIFTick(){
  const now=state.simTime,dt=Math.max(0,Math.min(1.2,now-stxIFLastTick));stxIFLastTick=now;if(dt<=0)return;
  stxIFPlan();stxIFCrewMobilization(dt);
  for(const p of state.planets)if(p.owner!==null)stxIFColonyTick(p,dt);
  stxIFRoutingTick();stxIFUpgradeTick(dt);
}
const STX_IF_simulate=simulate;
simulate=function(dt){stxIFPlan();const result=STX_IF_simulate(dt);if(state.simTime-stxIFLastTick>=.45)stxIFTick();return result};
function stxIFReset(){stxIFLastTick=state.simTime;stxIFLastRouting=state.simTime-3;stxIFLastPlan=state.simTime-3;stxIFPlans=new Map();state.planets.forEach(stxIFEnsurePlanet)}
const STX_IF_generate=generateGalaxy;
generateGalaxy=function(){STX_IF_generate();stxIFReset()};
const STX_IF_load=loadGame;
loadGame=function(){const ok=STX_IF_load();if(ok)stxIFReset();return ok};
const STX_IF_setPriority=stxPSSetPriority;
stxPSSetPriority=function(...args){const result=STX_IF_setPriority(...args);stxIFLastPlan=state.simTime-3;stxIFPlan();return result};
const STX_IF_startLocal=startLocalProject;
startLocalProject=function(p,type,source){if(!p||!empire(p.owner)||empire(p.owner).credits<3)return false;const ok=STX_IF_startLocal(p,type,source);if(ok){empire(p.owner).credits-=3;p.localProject.stxCredits=3}return ok};
const STX_IF_economyTick=economyTick;
economyTick=function(dt){
  const result=STX_IF_economyTick(dt);
  for(const e of state.empires){const mobilized=owned(e.id).filter(p=>p.stxEconomicFocus==="mobilization");e.credits=Math.max(0,e.credits-mobilized.reduce((n,p)=>n+(.018+p.pop*.018)*.15*dt,0))}
  return result;
};
const STX_IF_growth=populationEfficiency;
populationEfficiency=function(p){return STX_IF_growth(p)*(p?.stxEconomicFocus==="mobilization"?.85:1)};

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


const STX_IF_stationDetail=stxDSDrawBase;
stxDSDrawBase=function(base){
  STX_IF_stationDetail(base);if(!base||state.camera.zoom<.34||!visible(base.x,base.y,180))return;
  const q=base.upgradeProject||base.project,battle=(state.deepSpaceBattles||[]).find(b=>b.baseId===base.id),s=worldToScreen(base.x,base.y),z=state.camera.zoom,t=performance.now()/1000;
  if(q){
    const size=clamp((18+(q.targetTier||base.tier)*6)*z,15,65);ctx.save();ctx.translate(s.x,s.y);ctx.strokeStyle="#779ba8";ctx.lineWidth=1;ctx.setLineDash([4,5]);
    for(let i=0;i<4;i++){ctx.beginPath();ctx.arc(0,0,size,-1.5+i*1.57,-.5+i*1.57);ctx.stroke()}
    ctx.setLineDash([]);ctx.strokeStyle="#75eaff";ctx.beginPath();ctx.arc(0,0,size+5,-1.57,-1.57+6.283*stxIFUpgradeRatio(q));ctx.stroke();
    const angle=t*.4;ctx.fillStyle="#e5ffff";ctx.fillRect(Math.cos(angle)*size-2,Math.sin(angle)*size-1,5,3);
    if(Math.sin(t*12)>.5){ctx.strokeStyle="#fff1bc";for(let i=0;i<3;i++){ctx.beginPath();ctx.moveTo(Math.cos(angle)*size,Math.sin(angle)*size);ctx.lineTo(Math.cos(angle)*size+Math.sin(i*2+t)*7,Math.sin(angle)*size+Math.cos(i*2+t)*7);ctx.stroke()}}
    ctx.restore();
  }
  if(battle&&base.type==="military"&&base.supplyReadiness>.1){
    ctx.save();ctx.translate(s.x,s.y);ctx.strokeStyle="#ffb978";ctx.lineWidth=1.6;ctx.globalAlpha=.35+.4*Math.abs(Math.sin(t*7));
    for(let i=0;i<base.tier+1;i++){const a=t*.17+i*1.7;ctx.beginPath();ctx.moveTo(Math.cos(a)*10*z,Math.sin(a)*10*z);ctx.lineTo(Math.cos(a)*(40+base.tier*12)*z,Math.sin(a)*(40+base.tier*12)*z);ctx.stroke()}
    ctx.strokeStyle="#80e9ff";ctx.beginPath();ctx.arc(0,0,(27+base.tier*7)*z,t*.5,t*.5+1.3);ctx.stroke();ctx.restore();
  }
};
function stxIFBottleneck(d){
  const b=stxSDBottleneck(d);if(!b||b.remaining<=stxIFEpsilon(b.resource))return"Materials allocated; construction capacity is active.";
  const label=stxIFLabel(b.resource),incoming=stxSDIncoming(d,b.resource).length;
  if(incoming)return`${label}: ${incoming} inbound transport${incoming===1?"":"s"}.`;
  if(stxIFFreightUsed(d.p.owner)>=stxIFFreightLimit(d.p.owner))return`${label}: freight capacity is occupied. Build freighters or support a logistics station.`;
  const sources=owned(d.p.owner).filter(p=>p!==d.p&&p.stock[b.resource]>stxIFReserve(p,b.resource,d.priority,stxIFPlans.get(d.p.owner)?.stage||0));
  if(!sources.length)return`${label}: no exporting surplus. ${b.resource==="trained"?"Training needs civilian population and Equipment.":"Increase production, release reserves, or request trade in Transmissions."}`;
  return`${label}: sourcing from ${sources.length} eligible world${sources.length===1?"":"s"}.`;
}
function stxIFUpgradePanel(base){
  if(!base||base.owner!==0||base.status!=="operational")return"";
  const q=base.upgradeProject,target=q?.targetTier||base.tier+1,cost=q?.need||(base.tier<3?stxIFUpgradeCost(base,target):{}),p=state.planets.find(x=>x.id===base.sponsorPlanetId),d=q&&p?stxIFRemoteDescriptor(base,p):null,priority=d&&empire(0).stxPriorityProjectId===d.id;
  return `<div class="stx-if-upgrade"><b>TIER ${base.tier} · ${stxIFTierName(base)}</b><small>Readiness ${Math.round(base.supplyReadiness*100)}% · service capacity ${stxIFServiceCapacity(base)} fleets<br>Upkeep / cycle: ${Object.entries(stxIFUpkeep(base)).map(([r,a])=>`${(a*(state.commandCycle||45)).toFixed(1)} ${r==="credits"?"Credits":stxIFLabel(r)}`).join(" · ")}</small>${q?`<b>UPGRADING TO ${stxIFTierName(base,target)}</b><small>${Math.round(stxIFUpgradeRatio(q)*100)}% delivered to station · ${Math.round(q.progress*100)}% assembled. Station remains operational.</small><div class="stx-if-progress"><i style="width:${Math.round(q.progress*100)}%"></i></div><button class="choice-btn" data-stx-if-priority="${d?.id||""}" data-stx-if-planet="${p?.id||""}">${priority?"CLEAR IMPERIAL PRIORITY":"PRIORITIZE UPGRADE"}</button>`:base.tier<3?`<button class="choice-btn primary-choice" data-stx-if-upgrade="${base.id}">Authorize Tier ${target} · ${Math.ceil(Object.values(cost).reduce((n,v)=>n+v,0)*.09)} Credits</button>`:"<small>Maximum station tier.</small>"}<div class="stx-if-cost">${Object.entries(cost).map(([r,a])=>`<span>${stxIFLabel(r)} ${q?`${stxIFN(q.delivered[r]).toFixed(1)} / `:""}${Math.ceil(a)}${q?` · staged ${stxIFN(q.staged?.[r]).toFixed(1)} · inbound ${stxIFUpgradeIncoming(base,r).toFixed(1)}`:""}</span>`).join("")}</div>${d?`<small>${stxRTEscape(stxIFBottleneck(d))}</small>`:""}</div>`;
}
const STX_IF_basePanel=stxDSBasePanel;
stxDSBasePanel=function(base){const html=STX_IF_basePanel(base);return html&&base?.status==="operational"?html.replace(/<\/section>\s*$/,`${stxIFUpgradePanel(base)}</section>`):html};
function stxIFPlanetCard(p){
  if(!p||p.owner!==0)return"";const plan=stxIFPlans.get(0),priority=plan?.priority,projects=stxSDDescriptors(p),expansion=stxIFExpansionLoad(p),ships=state.ships.filter(s=>!s.stxIntercepted&&!s.stxCancelled),cargo=s=>Object.entries(s.cargo||{}).reduce((n,[r,a])=>n+(r==="trained"||r==="population"?0:a),0),incoming=ships.filter(s=>s.to===p.id&&!s.stxDeepTransit).reduce((n,s)=>n+cargo(s),0),outgoing=ships.filter(s=>s.from===p.id).reduce((n,s)=>n+cargo(s),0),commit=projects.reduce((n,d)=>n+Object.entries(d.need).reduce((a,[r])=>a+(r==="trained"?0:stxSDRemaining(d,r)),0),0);
  const load=projects.length?Math.min(100,projects.length/(1+Math.max(0,p.infra.factory-1)*.22+p.infra.shipyard*.12)*100):0;
  return `<section class="stx-if-econ"><div class="section-label">Integrated Economic Flow</div><div class="stx-if-metrics"><span>Commitments <b>${Math.round(commit)}</b></span><span>Inbound / outbound <b>${Math.round(incoming)} / ${Math.round(outgoing)}</b></span><span>Industrial load <b>${Math.round(load)}%</b></span><span>Expansion <b>${Math.round(expansion*30)}% factory capacity</b></span></div>${p.stxColony?`<p class="subtle">${stxRTEscape(p.stxColony.stage.toUpperCase())} · supply ${Math.round(p.stxColony.readiness*100)}% · development ${Math.floor(p.stxColony.development)} / 360. Colony growth and construction depend on imports.</p>`:""}<div class="stx-if-priority"><b>${priority?stxRTEscape(priority.title):"No Imperial priority active"}</b><small>${priority?`${STX_IF_STAGES[plan.stage]} · ${stxRTEscape(stxIFBottleneck(priority))}`:"Prioritize one project to redirect actual production, reserves and freight."}</small></div><div class="stx-if-routes">${STX_IF_RESOURCES.map(r=>`<button data-stx-if-route="${r}" class="${stxIFRoute(p,r)}"><b>${stxIFLabel(r)}</b><span>${r==="trained"?fmtNum(p.stock[r]):Math.round(stxIFN(p.stock[r]))}</span><small>${stxIFRoute(p,r)==="project"?"LOCAL":stxIFRoute(p,r).toUpperCase()}</small></button>`).join("")}</div><p class="subtle">Click to cycle BALANCED → LOCAL → EXPORT. Committed material remains protected. Priority may requisition uncommitted reserves.</p>${projects.length?`<details><summary>Project supply and capacity</summary>${projects.map(d=>`<p class="subtle"><b>${stxRTEscape(d.title)}</b> · ${Math.round(stxIFCapacityShare(d)*100)}% construction share<br>${stxRTEscape(stxIFBottleneck(d))}</p>`).join("")}</details>`:""}</section>`;
}
const STX_IF_renderPlanet=renderPlanet;
renderPlanet=function(){
  STX_IF_renderPlanet();const p=state.selected,body=$("planetBody");if(!p||!body)return;if(p.owner===0&&!body.querySelector?.(".stx-if-econ"))body.insertAdjacentHTML("beforeend",stxIFPlanetCard(p));
  body.querySelectorAll?.("[data-stx-if-route]").forEach(b=>b.onclick=e=>{e.preventDefault();e.stopPropagation();stxIFCycleRoute(p.id,b.dataset.stxIfRoute)});
  body.querySelectorAll?.("[data-stx-if-upgrade]").forEach(b=>b.onclick=e=>{e.preventDefault();e.stopPropagation();stxIFStartUpgrade(b.dataset.stxIfUpgrade)});
  body.querySelectorAll?.("[data-stx-if-priority]").forEach(b=>b.onclick=e=>{e.preventDefault();e.stopPropagation();stxPSSetPriority(b.dataset.stxIfPriority,b.dataset.stxIfPlanet)});
};
function stxIFOverview(){
  const s=empire(0)?.stxEconomicOverview;if(!s)return"";const cycle=state.commandCycle||45;
  return `<details class="stx-if-econ"><summary>EMPIRE ECONOMY · Freight ${s.freightUsed} / ${s.freightCapacity}</summary><p class="subtle">Recent measured production / consumption per cycle. Consumption includes project commitments. Leading exporters/importers use cumulative physical cargo.</p><div style="overflow-x:auto"><table><thead><tr><th>Resource</th><th>Stock</th><th>Produced</th><th>Consumed</th><th>Net</th><th>Exporter → importer</th></tr></thead><tbody>${Object.entries(s.resources).map(([r,a])=>{const f=v=>r==="trained"?fmtNum(v):v.toFixed(1);return`<tr><td>${stxIFLabel(r)}</td><td>${f(a.stock)}</td><td>+${f(a.productionRate*cycle)}</td><td>−${f(a.consumptionRate*cycle)}</td><td>${f((a.productionRate-a.consumptionRate)*cycle)}</td><td>${stxRTEscape(a.exporter)} → ${stxRTEscape(a.importer)}</td></tr>`}).join("")}</tbody></table></div></details>`;
}
const STX_IF_transmissions=renderTransmissions;
renderTransmissions=function(){STX_IF_transmissions();$("transmissionList")?.insertAdjacentHTML("beforeend",stxIFOverview())};

function stxIFStyles(){if(document.getElementById?.("stxIFStyles"))return;const s=document.createElement("style");s.id="stxIFStyles";s.textContent=`.stx-if-econ{margin-top:12px;padding:10px;border:1px solid rgba(112,233,255,.2);border-radius:9px;background:rgba(5,13,24,.86)}.stx-if-metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:5px;margin:6px 0}.stx-if-metrics span{padding:6px;background:rgba(255,255,255,.035);border-radius:6px;font-size:7px;color:#7890a5}.stx-if-metrics b{display:block;color:#e9f8ff;font-size:11px}.stx-if-priority{padding:7px;border-left:2px solid #ffd56f;background:rgba(255,213,111,.045);margin:6px 0}.stx-if-priority b,.stx-if-priority small{display:block}.stx-if-priority small{color:#8da3b7;margin-top:2px}.stx-if-routes{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:4px}.stx-if-routes button{padding:6px 5px;text-align:left;background:#091321;border:1px solid rgba(255,255,255,.09);border-radius:6px;color:#bed1df;cursor:pointer}.stx-if-routes button b,.stx-if-routes button span,.stx-if-routes button small{display:block;overflow:hidden;text-overflow:ellipsis}.stx-if-routes button span{color:#fff;font-size:11px}.stx-if-routes button small{font-size:7px;color:#728a9f}.stx-if-routes button.project{border-color:rgba(255,213,111,.42)}.stx-if-routes button.export{border-color:rgba(112,233,255,.45)}.stx-if-upgrade{margin-top:10px;padding:9px;border:1px solid rgba(140,205,235,.2);border-radius:8px;background:rgba(5,12,24,.76)}.stx-if-upgrade>b,.stx-if-upgrade>small{display:block}.stx-if-upgrade>small{color:#8ca3b7;margin:3px 0 7px}.stx-if-cost{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}.stx-if-cost span{font-size:7px;padding:3px 5px;border-radius:9px;background:rgba(255,255,255,.05);color:#a9bfd0}.stx-if-progress{height:5px;background:#111d2a;border-radius:5px;overflow:hidden}.stx-if-progress i{display:block;height:100%;background:#75eaff;box-shadow:0 0 8px rgba(117,234,255,.5)}@media(max-width:760px){.stx-if-metrics,.stx-if-routes{grid-template-columns:repeat(2,minmax(0,1fr))}}`;document.head.appendChild(s)}
stxIFStyles();


globalThis.SpaceTyrantsIntegratedEconomy={version:STX_IF_VERSION,route:stxIFRoute,setRoute:stxIFSetRoute,expansionLoad:stxIFExpansionLoad,mobilization:stxIFMobilization,tierName:stxIFTierName,upgradeCost:stxIFUpgradeCost,startUpgrade:stxIFStartUpgrade,upgradeRatio:stxIFUpgradeRatio,capacityShare:stxIFCapacityShare,upkeep:stxIFUpkeep,freightLimit:stxIFFreightLimit,plan:stxIFPlan,tick:stxIFTick};

// Completion and reserve-release actions never round tiny deficits into free goods.
stxSDAllDelivered=function(d){return Object.entries(d.need||{}).every(([r,a])=>stxSDDelivered(d,r)>=a-stxIFEpsilon(r))};
stxSDReserveRelease=function(resource,crisis){
  const d=stxSDBestProjectForCrisis(crisis);if(!d)return false;
  const e=empire(0);e.stxPriorityProjectId=d.id;e.stxPriorityPlanetId=d.p.id;e.stxPrioritySetAt=state.simTime-80;
  stxIFLastPlan=state.simTime-3;stxIFPlan();stxSDAllocatePlanet(d.p);stxSDEnsureOrders(d);
  const order=d.p.orders.find(o=>o.id===d.q.stxSupply.orderIds[resource]);if(order)fillOrder(d.p,order);
  if(stxIFNeed(d,resource)>stxIFEpsilon(resource))stxSDApplyProduction(resource);
  showToast("Existing reserves requisitioned; missing supply must be produced and shipped.");return true;
};
stxSDDispatchDomestic=function(resource,crisis){
  let sent=false;for(const {desc:d} of crisis?.projects||[]){stxSDEnsureOrders(d);const o=d.p.orders.find(o=>o.id===d.q.stxSupply.orderIds[resource]);if(o){fillOrder(d.p,o);sent=sent||o.status==="in transit"}}return sent;
};
// Legacy emergency Equipment production had a 35% output floor without inputs.
const STX_IF_emergencyProduction=stxSDProductionTick;
stxSDProductionTick=function(p,dt){
  const programs=empire(p.owner)?.stxSupplyPrograms,program=programs?.equipment;
  if(!program||program.until<=state.simTime)return STX_IF_emergencyProduction(p,dt);
  delete programs.equipment;
  try{STX_IF_emergencyProduction(p,dt)}finally{programs.equipment=program}
  const wanted=p.infra.factory*.018*Math.max(0,p.factoryEfficiency)*dt*program.boost;
  const ratio=Math.min(1,(p.stock.iron||0)/Math.max(1e-9,wanted*.8),(p.stock.titanium||0)/Math.max(1e-9,wanted*.35),(p.stock.rare||0)/Math.max(1e-9,wanted*.22));
  for(const [r,a] of Object.entries({iron:.8,titanium:.35,rare:.22}))consume(p,r,wanted*a*ratio);
  p.stock.equipment+=wanted*ratio;
};
const STX_IF_acceptTrade=acceptTradeProposal;
acceptTradeProposal=function(proposal,counter=false){
  if(!proposal?.stxProcurement)return STX_IF_acceptTrade(proposal,counter);
  const target=state.planets.find(p=>p.id===proposal.projectPlanetId&&p.owner===0)||owned(0)[0],r=proposal.offer.resource,amount=proposal.offer.amount*(counter?.85:1);
  if(!Number.isFinite(amount)||amount<=0||!STX_IF_RESOURCES.includes(r))return false;
  const source=owned(proposal.from).find(p=>p.stock[r]>=amount+stxIFReserve(p,r));
  const payment=proposal.request||{},cost=stxIFN(payment.credits)*(counter?.85:1),payAmount=stxIFN(payment.amount)*(counter?.85:1),payer=payment.resource&&owned(0).find(p=>p.stock[payment.resource]>=payAmount+stxIFReserve(p,payment.resource));
  if(!target||!source||!stxIFCanFreight(source.owner)||empire(0).credits<cost+(payment.resource?.04:0)||state.ships.length+(payment.resource?2:1)>280||(payment.resource&&(!payer||!stxIFCanFreight(0))))return false;
  source.stock[r]-=amount;
  const ship=createShip(r==="helium"?"tanker":"supply",source,target,source.owner,{cargo:{[r]:amount},commercial:true,crossBorder:true,tradeKind:"emergency-procurement",tradePartner:0,vesselName:vesselName("supply")});
  if(!ship){source.stock[r]+=amount;return false}
  if(payment.resource){
    payer.stock[payment.resource]-=payAmount;
    const paid=createShip("freighter",payer,source,0,{cargo:{[payment.resource]:payAmount},commercial:true,crossBorder:true,tradeKind:"emergency-payment",tradePartner:source.owner});
    if(!paid){payer.stock[payment.resource]+=payAmount;source.stock[r]+=amount;state.ships=state.ships.filter(s=>s!==ship);return false}
  }else{empire(0).credits-=cost;empire(source.owner).credits+=cost}
  adjustRelation(0,source.owner,.08);return true;
};

// Existing orbital/industrial construction uses the same demand and capacity
// budget. Its old in-flight cargo and delivered ledger remain compatible.
const STX_IF_incoming=stxSDIncoming;
stxSDIncoming=function(d,r=null){
  const ordinary=STX_IF_incoming(d,r).filter(s=>!s.stxCancelled&&!s.stxIntercepted);
  if(d.kind!=="physical")return ordinary;
  return [...new Set([...ordinary,...state.ships.filter(s=>s.projectId===d.q.id&&s.projectDelivery&&!s.stxCancelled&&!s.stxIntercepted&&(!r||s.cargo?.[r]>0))])];
};
stxOLSourceProject=function(p,q){
  if(q.phase==="operations")return;
  const before={...p.stock};stxSDAllocatePlanet(p);for(const r of STX_IF_RESOURCES)stxIFRecord(p,"consumption",r,Math.max(0,stxIFN(before[r])-stxIFN(p.stock[r])));
  const d=stxSDDescriptors(p).find(d=>d.q===q);if(!d)return;stxSDEnsureOrders(d);
  for(const id of Object.values(q.stxSupply.orderIds)){const order=p.orders.find(o=>o.id===id);if(order)fillOrder(p,order)}
  if(q.phase==="authorized")q.phase="sourcing";
};
stxOLProjectPriority=function(q){return empire(0)?.stxPriorityProjectId===q.stxProjectId};
stxOLProjectReady=function(q){return Object.entries(q.need).every(([r,a])=>stxIFN(q.delivered[r])>=a-stxIFEpsilon(r))};
const STX_IF_olTick=stxOLTickProject;
stxOLTickProject=function(p,q,dt){const d=stxSDDescriptors(p).find(d=>d.q===q);return STX_IF_olTick(p,q,dt*(d?stxIFCapacityShare(d):1))};
// Roll back charter fees and fuel as well as goods if a multi-leg exchange
// cannot dispatch every leg. Successful exchanges still pay real freight cost.
const STX_IF_exchange=stxRTExchange;
stxRTExchange=function(...args){
  const credits=state.empires.map(e=>e.credits),before=state.planets.map(p=>({p,helium:p.stock.helium,totals:JSON.stringify(p.stxEconomicTotals||null)}));
  const ok=STX_IF_exchange(...args);if(!ok){state.empires.forEach((e,i)=>e.credits=credits[i]);for(const x of before){x.p.stock.helium=x.helium;x.p.stxEconomicTotals=JSON.parse(x.totals)||undefined}}return ok;
};
function stxIFEmergencyReroute(d){
  for(const r of Object.keys(d.need)){
    if(stxIFNeed(d,r)<=stxIFEpsilon(r))continue;
    const s=state.ships.find(s=>s.owner===d.p.owner&&s.commercial&&!s.crossBorder&&!s.stxDeepTransit&&!s.projectId&&!s.stxProjectId&&s.to!==d.p.id&&s.cargo?.[r]>0);
    if(!s)continue;
    stxSDEnsureOrders(d);s.startX=s.x;s.startY=s.y;s.distance=Math.max(1,dist(s,d.p));s.progress=0;s.to=d.p.id;s.commercial=false;s.stxProjectId=d.id;s.orderId=d.q.stxSupply.orderIds[r];s.stxIFRequisitioned=true;
    const o=d.p.orders.find(o=>o.id===s.orderId);if(o)o.status="in transit";
  }
}
const STX_IF_planOwner=stxIFPlanOwner;
stxIFPlanOwner=function(e){STX_IF_planOwner(e);const plan=stxIFPlans.get(e.id);if(plan?.priority&&plan.stage>=4)stxIFEmergencyReroute(plan.priority)};
stxOLSetPriority=function(projectId){
  const d=stxPSAllProjectDescriptors(0).find(d=>d.kind==="physical"&&d.q.id===projectId);if(d)stxPSSetPriority(d.id,d.p.id);
};
const STX_IF_projectIncoming=stxOLProjectIncoming;
stxOLProjectIncoming=function(q,r=null){
  const ids=new Set(Object.values(q.stxSupply?.orderIds||{}));
  return state.ships.filter(s=>!s.stxCancelled&&!s.stxIntercepted&&(s.projectId===q.id||s.stxProjectId===q.stxProjectId||ids.has(s.orderId))).reduce((n,s)=>n+(r?stxIFN(s.cargo?.[r]):Object.values(s.cargo||{}).reduce((a,v)=>a+v,0)),0);
};

// The legacy AI trade planner must not overwrite the selected production plan.
stxRTAIChooseFocus=function(e){if(state.simTime>=stxIFN(e.stxNextEconomyPlanAt))stxIFPlanOwner(e)};
// A crew priority can commission an actual training camp when the empire has
// no training or naval infrastructure. It consumes materials and build time,
// and uses the existing physical-project ledger instead of spawning crew.
const STX_IF_crewTick=stxIFCrewMobilization;
stxIFCrewMobilization=function(dt){
  for(const e of state.empires){
    const plan=stxIFPlans.get(e.id),worlds=owned(e.id).filter(p=>!p.underAttack);
    if(!plan?.priority||plan.stage<3||stxIFNeed(plan.priority,"trained")<=1e-9||worlds.some(p=>p.infra.training>0||p.infra.shipyard>0))continue;
    if(worlds.some(p=>(p.physicalProjects||[]).some(q=>q.kind==="training"&&q.phase!=="operations")))continue;
    const p=worlds.filter(p=>p.pop>.025).sort((a,b)=>b.infra.factory-a.infra.factory)[0];if(!p||e.credits<3)continue;
    e.credits-=3;p.physicalProjects=p.physicalProjects||[];
    p.physicalProjects.push({id:stxOLId("training"),kind:"training",name:"Emergency Training Camp",option:{},phase:"authorized",progress:0,need:{iron:8,components:3,equipment:5},delivered:{},startedAt:state.simTime,authorizedAt:state.simTime,creditCost:3,shipmentIds:[],activity:[],stxEmergencyTraining:true});
    if(e.id===0)logEvent(`${p.name} authorized a training camp to resolve the personnel bottleneck. Equipment, construction materials and build capacity are required.`,"warning");
  }
  STX_IF_crewTick(dt);
};
const STX_IF_completePhysical=stxOLCompleteProject;
stxOLCompleteProject=function(p,q){if(q.kind==="training"&&q.phase!=="operations")p.infra.training+=1;return STX_IF_completePhysical(p,q)};
const STX_IF_trainingProjectTick=stxOLTickProject;
stxOLTickProject=function(p,q,dt){return STX_IF_trainingProjectTick(p,q,dt*(q.stxEmergencyTraining?3:1))};
