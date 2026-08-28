/* Space Tyrants — incremental construction, deadlock detection + emergency supply mandates.
   Loaded last. Keeps shortages strategically meaningful while guaranteeing that prolonged
   project stalls become visible economic problems with concrete domestic/trade solutions. */

const STX_SD_STAGE={NORMAL:0,DELAYED:1,CRITICAL:2,SEVERE:3,EMERGENCY:4};
const STX_SD_STAGE_LABEL=["Normal supply","Delayed","Critical delay","Severe deadlock","Strategic emergency"];
const STX_SD_RAW=new Set(["iron","silicates","rare","titanium","helium"]);
const STX_SD_FACTORY=new Set(["components","equipment"]);
let stxSDLastAudit=-999;
let stxSDCrisisCache=[];

function stxSDResourceLabel(r){return RESOURCE_LABEL?.[r]||({trained:"Trained Crew",components:"Components",equipment:"Equipment",rare:"Rare Earth",silicates:"Silicates",titanium:"Titanium",helium:"Helium",iron:"Iron"}[r])||r}
function stxSDFmt(r,v){if(r==="trained")return fmtNum(v);return Number(v||0)<10?Number(v||0).toFixed(1):Math.round(v||0).toString()}
function stxSDProjectId(q,prefix){if(!q.stxProjectId)q.stxProjectId=`${prefix}-${Math.floor(random()*1e9)}`;return q.stxProjectId}
function stxSDSourceIsImperial(q){return !!(q?.mandated||q?.directed||q?.commissioned||/Imperial|Admiralty|Mandate|war/i.test(q?.source||q?.commissionSource||""))}
function stxSDAtWar(owner){return state.wars.some(w=>w.active&&(w.a===owner||w.b===owner))}
function stxSDPriority(kind,q,p){
  if(kind==="reconstruction"||((kind==="ship"||kind==="orbital")&&stxSDAtWar(p.owner)))return 7;
  if(stxSDSourceIsImperial(q))return 6;
  if(kind==="scan"||kind==="orbital")return 4;
  if(kind==="ship")return 3;
  if(kind==="local")return /Governor/i.test(q?.source||"")?2:4;
  if(kind==="expansion")return q?.directed?6:2;
  if(kind==="trade")return q?.mandated?5:1;
  return 2;
}
function stxSDPriorityLabel(n){return n>=7?"War-critical emergency":n>=6?"Explicit Imperial Mandate":n>=5?"Strategic priority":n>=4?"Governor strategic project":n>=3?"Military construction":n>=2?"Routine governor project":"Commercial expansion"}
function stxSDNeedMap(desc){return desc.need||{}}
function stxSDProgress(desc){
  if(desc.kind==="expansion"){
    const s=desc.q.stxSupply?.delivered||{},need=desc.need.components||1;
    return Math.min(desc.q.volunteers/Math.max(.000001,desc.q.goal),s.components/Math.max(.000001,need));
  }
  return clamp(Number(desc.q.progress||0),0,1);
}
function stxSDEnsureSupply(desc){
  const q=desc.q,need=stxSDNeedMap(desc),id=stxSDProjectId(q,desc.kind);
  if(!q.stxSupply){
    const delivered={};
    const prior=desc.kind==="expansion"?0:clamp(Number(q.progress||0),0,1);
    Object.entries(need).forEach(([r,a])=>delivered[r]=Math.max(0,Number(a||0)*prior));
    q.stxSupply={delivered,orderIds:{},createdAt:state.simTime,lastProgress:prior,lastProgressAt:state.simTime,waitingSince:null,lastSupplyAt:state.simTime,notifiedStage:0,resumeNotified:false};
  }
  q.stxSupply.delivered=q.stxSupply.delivered||{};q.stxSupply.orderIds=q.stxSupply.orderIds||{};
  Object.entries(need).forEach(([r,a])=>q.stxSupply.delivered[r]=clamp(Number(q.stxSupply.delivered[r]||0),0,Number(a||0)));
  q.stxSupply.projectId=id;
  return q.stxSupply;
}
function stxSDDescriptors(p){
  if(!p||p.owner===null)return[];
  const out=[];
  if(p.expansionProject)out.push({kind:"expansion",q:p.expansionProject,p,title:`Colony mission → ${state.planets.find(x=>x.id===p.expansionProject.targetId)?.name||"Frontier"}`,need:{components:p.expansionProject.componentCost||24},orderType:"colony",label:"Colony hull materials"});
  if(p.orbitalProject)out.push({kind:"orbital",q:p.orbitalProject,p,title:p.orbitalProject.type==="base"?"Sector Military Base":"Orbital Space Station",need:p.orbitalProject.need||{},orderType:"orbital",label:p.orbitalProject.type==="base"?"Military base construction":"Space station construction"});
  if(p.localProject)out.push({kind:"local",q:p.localProject,p,title:`${p.localProject.type?.[0]?.toUpperCase()||""}${p.localProject.type?.slice(1)||"Local"} Expansion`,need:{components:p.localProject.cost||40},orderType:"local",label:`${p.localProject.type||"Local"} project materials`});
  if(p.reconstruction)out.push({kind:"reconstruction",q:p.reconstruction,p,title:"Post-War Reconstruction",need:{components:p.reconstruction.need||30},orderType:"reconstruction",label:"War reconstruction materials"});
  const bq=p.buildQueue?.[0];if(bq)out.push({kind:"ship",q:bq,p,title:`${TYPE_LABEL?.[bq.type]||bq.type||"Ship"} under construction`,need:bq.need||{},orderType:"ship",label:`${bq.type||"ship"} construction`});
  if(p.tradeStationProject){const q=p.tradeStationProject,spec=typeof STX_TRADE_STATION_LEVELS!=="undefined"?STX_TRADE_STATION_LEVELS[q.level-1]:null;out.push({kind:"trade",q,p,title:spec?.name||"Trade Station",need:q.need||spec?.need||{},orderType:"trade-station",label:`${spec?.name||"Trade station"} construction`})}
  if(p.scanProject)out.push({kind:"scan",q:p.scanProject,p,title:"Deep-Space Sensor Array",need:p.scanProject.need||{},orderType:"sensor",label:"Deep-space sensor array"});
  out.forEach(d=>{d.priority=stxSDPriority(d.kind,d.q,p);d.id=stxSDProjectId(d.q,d.kind);stxSDEnsureSupply(d)});
  return out;
}
function stxSDDelivered(desc,r){return Number(desc.q.stxSupply?.delivered?.[r]||0)}
function stxSDRemaining(desc,r){return Math.max(0,Number(desc.need?.[r]||0)-stxSDDelivered(desc,r))}
function stxSDIncoming(desc,r=null){
  const orderIds=new Set(Object.values(desc.q.stxSupply?.orderIds||{}).filter(Boolean));
  return state.ships.filter(s=>s.to===desc.p.id&&(s.stxProjectId===desc.id||orderIds.has(s.orderId))&&(!r||Number(s.cargo?.[r]||0)>0));
}
function stxSDIncomingAmount(desc,r){return stxSDIncoming(desc,r).reduce((n,s)=>n+Number(s.cargo?.[r]||0),0)}
function stxSDCeiling(desc){
  const entries=Object.entries(desc.need||{}).filter(([,a])=>Number(a)>0);if(!entries.length)return 1;
  return clamp(entries.reduce((n,[r,a])=>n+clamp(stxSDDelivered(desc,r)/a,0,1),0)/entries.length,0,1);
}
function stxSDBottleneck(desc){
  const entries=Object.entries(desc.need||{}).filter(([,a])=>Number(a)>0);if(!entries.length)return null;
  return entries.map(([r,a])=>({resource:r,ratio:clamp(stxSDDelivered(desc,r)/a,0,1),remaining:stxSDRemaining(desc,r),incoming:stxSDIncomingAmount(desc,r)})).sort((a,b)=>a.ratio-b.ratio||b.remaining-a.remaining)[0]||null;
}
function stxSDReserveFor(resource,amount,priority=1){
  if(resource==="trained")return priority>=5?Math.max(.001,Math.min(.0025,amount*.22)):Math.max(.002,Math.min(.004,amount*.45));
  if(resource==="components"||resource==="equipment")return priority>=5?Math.max(4,amount*.12):Math.max(8,amount*.28);
  return priority>=5?Math.max(7,amount*.14):Math.max(12,amount*.3);
}
function stxSDAllocatePlanet(p){
  const descs=stxSDDescriptors(p).sort((a,b)=>b.priority-a.priority||(a.q.startedAt||0)-(b.q.startedAt||0));
  const resources=new Set(descs.flatMap(d=>Object.keys(d.need||{})));
  resources.forEach(r=>{
    let available=Math.max(0,Number(p.stock[r]||0));if(available<=0)return;
    for(const d of descs){
      const rem=stxSDRemaining(d,r);if(rem<=1e-9)continue;
      const take=Math.min(rem,available);if(take<=0)break;
      p.stock[r]=Math.max(0,Number(p.stock[r]||0)-take);available-=take;
      d.q.stxSupply.delivered[r]=stxSDDelivered(d,r)+take;d.q.stxSupply.lastSupplyAt=state.simTime;
      if(available<=1e-9)break;
    }
  });
  descs.forEach(stxSDEnsureOrders);
}
function stxSDEnsureOrders(desc){
  const sup=stxSDEnsureSupply(desc);
  Object.entries(desc.need||{}).forEach(([r,a])=>{
    const rem=stxSDRemaining(desc,r),incoming=stxSDIncomingAmount(desc,r),desired=Math.max(0,rem-incoming),existingId=sup.orderIds[r],existing=existingId&&desc.p.orders.find(o=>o.id===existingId);
    if(desired<=Math.max(r==="trained"?.0002:.35,Number(a)*.003)){
      if(existing&&!state.ships.some(s=>s.orderId===existing.id)){existing.filled=existing.amount;existing.status="filled"}
      return;
    }
    if(existing){existing.amount=Math.max(existing.filled,existing.filled+desired);existing.priority=Math.max(existing.priority,desc.priority);if(existing.status==="filled")existing.status="waiting";return}
    const o=addOrder(desc.p,`stx-${desc.id}`,r,desired,desc.priority,`${desc.label} · ${stxSDResourceLabel(r)}`);if(o)sup.orderIds[r]=o.id;
  });
}
function stxSDAdvance(desc,dt,rate){
  const q=desc.q,ceiling=stxSDCeiling(desc),before=Number(q.progress||0);
  if(ceiling>before+1e-7)q.progress=Math.min(ceiling,before+Math.max(0,rate)*dt);
  return Math.max(0,Number(q.progress||0)-before);
}
function stxSDAllDelivered(desc){return Object.entries(desc.need||{}).every(([r,a])=>stxSDDelivered(desc,r)>=Number(a)-Math.max(r==="trained"?.00005:.02,Number(a)*.001))}

/* Resource-aware freight. Crew is measured in population units (.005 = ~5K),
   so trained-personnel orders use milliscale reserves rather than the generic 10-unit floor. */
fillOrder=function(dest,o){
  if(!dest||!o||o.status!=="waiting"||state.ships.some(s=>s.orderId===o.id))return;
  if(!String(o.type||"").startsWith("stx-")&&dest.orders.some(x=>String(x.type||"").startsWith("stx-")&&x.resource===o.resource&&x.status!=="filled")&&["construction","ship","crew","sensor","trade-station","reconstruction"].includes(o.type))return;
  const remaining=Math.max(0,Number(o.amount||0)-Number(o.filled||0));if(remaining<=0){o.status="filled";return}
  const reserve=stxSDReserveFor(o.resource,remaining,o.priority||1),sources=owned(dest.owner).filter(p=>p!==dest&&!p.underAttack&&(p.stock[o.resource]||0)>reserve+Math.max(o.resource==="trained"?.0002:.4,remaining*.04));
  sources.sort((a,b)=>{
    if(o.resource==="trained")return (b.infra.training||0)-(a.infra.training||0)||dist(a,dest)-dist(b,dest);
    return ((b.stock[o.resource]||0)-reserve)-((a.stock[o.resource]||0)-reserve)||dist(a,dest)-dist(b,dest);
  });
  const source=sources[0];if(!source){o.stxFailedAttempts=Math.max(o.stxFailedAttempts||0,Math.floor((o.age||0)/15));return}
  const cap=o.resource==="trained"?.014:70,amount=Math.min(remaining,Math.max(0,(source.stock[o.resource]||0)-reserve),cap);if(amount<=Math.max(o.resource==="trained"?.0001:.25,remaining*.01))return;
  source.stock[o.resource]-=amount;const mobilized=(empire(dest.owner)?.stxEmergencyFreightUntil||0)>state.simTime,type=o.resource==="helium"?"tanker":o.resource==="trained"||o.resource==="equipment"?"supply":"freighter",projectId=String(o.type||"").startsWith("stx-")?String(o.type).slice(4):null;
  const ship=createShip(type,source,dest,dest.owner,{cargo:{[o.resource]:amount},orderId:o.id,stxProjectId:projectId,speedBoost:mobilized?1.55:1,vesselName:vesselName(type)});
  if(!ship){source.stock[o.resource]+=amount;return}o.status="in transit";o.stxLastAttempt=state.simTime;
};

/* Incremental project progression. Materials are reserved/consumed when allocated,
   not in one all-or-nothing charge at completion. */
tickOrbitalProject=function(p,dt){
  const q=p.orbitalProject;if(!q)return;const d=stxSDDescriptors(p).find(x=>x.q===q);if(!d)return;
  p.stock.components+=p.infra.factory*.08*(.65+p.factoryEfficiency*.35)*dt;
  stxSDEnsureOrders(d);stxSDAdvance(d,dt,orbitalProjectRate(p,q));
  if(q.progress<.999||!stxSDAllDelivered(d))return;
  q.progress=1;p.orbitals[q.type]=(p.orbitals[q.type]||0)+1;p.garrison+=q.type==="base"?22:7;p.orbitalProject=null;p.mandateGlow=1;state.effects.push({type:"launch",x:p.x,y:p.y,life:2,maxLife:2,size:34,color:empire(p.owner).color});
  if(p.owner===0)logEvent(`${p.name}'s ${q.type==="base"?"sector military base":"orbital space station"} is operational.`,"good");
  galacticNews(`${p.name.toUpperCase()} ORBITAL INSTALLATION OPERATIONAL`,`${q.type==="base"?"A sector military base":"An orbital space station"} has opened after progressive material deliveries and visible construction.`,"good",p.id);
};
tickLocalProject=function(p,dt){
  const q=p.localProject;if(!q)return;const d=stxSDDescriptors(p).find(x=>x.q===q);if(!d)return;const rate=.0065*(1+p.infra.factory*.22)*(stxSDSourceIsImperial(q)?1.25:1);
  stxSDEnsureOrders(d);stxSDAdvance(d,dt,rate);if(q.progress<.999||!stxSDAllDelivered(d))return;
  q.progress=1;p.infra[q.type]++;if(q.type==="city")p.capacity+=.12+Math.sqrt(p.pop)*.05;if(q.type==="defense")p.garrison+=7;p.localProject=null;p.mandateGlow=1;
  if(p.owner===0)logEvent(`${p.name} completed its ${q.type} project.`,"good");if(["shipyard","research","factory"].includes(q.type))galacticNews(`${p.name.toUpperCase()} COMPLETES ${q.type.toUpperCase()} EXPANSION`,`${p.governor?.name||"The local government"} opened the completed facilities after progressive construction deliveries.`,"good",p.id);
};
tickReconstruction=function(p,dt){
  const q=p.reconstruction;if(!q)return;const d=stxSDDescriptors(p).find(x=>x.q===q);if(!d)return;stxSDEnsureOrders(d);const moved=stxSDAdvance(d,dt,.0045*(1+p.infra.factory*.18));
  if(moved>0){p.warDamage=Math.max(0,p.warDamage-moved*.34);p.cityLights=clamp(1-p.warDamage*.78,.18,1)}
  if(q.progress<.999||!stxSDAllDelivered(d))return;q.progress=1;p.warDamage=Math.max(0,p.warDamage-.48);p.reconstruction=null;p.unrest=Math.max(0,p.unrest-.2);galacticNews(`${p.name.toUpperCase()} REOPENS AFTER WAR`,"Reconstruction ships restored power, cleared orbital wreckage, and reopened civilian routes.","good",p.id);
};
tickBuildQueue=function(p,dt){
  if(!p.buildQueue.length&&p.infra.shipyard>0&&p.owner!==null){
    const e=empire(p.owner),military=hasDirective(e,"fleet")||hasDirective(e,"patrolNetwork")||state.wars.some(w=>w.active&&(w.a===e.id||w.b===e.id))||borderThreat(p)>.45,type=military?(random()<.62?"patrol":"fleet"):(random()<.42?"freighter":"scout"),combat=type==="fleet"||type==="patrol";
    p.buildQueue.push({type,progress:0,startedAt:state.simTime,need:{components:combat?(type==="patrol"?26:38):18,helium:combat?(type==="patrol"?12:18):9,titanium:combat?(type==="patrol"?9:15):5,trained:combat?(type==="patrol"?.005:.009):0}});
  }
  const q=p.buildQueue[0];if(!q)return;const d=stxSDDescriptors(p).find(x=>x.q===q);if(!d)return;stxSDEnsureOrders(d);
  const orbitalLoad=p.orbitalProject?(p.orbitalProject.mandated?.48:.38):0,tradeLoad=p.tradeStationProject?.18:0,scanLoad=p.scanProject?.12:0,tech=(empire(p.owner).tech.automation||0),parallelBonus=Math.min(.34,Math.max(0,p.infra.shipyard-1)*.09+tech*.035),capacityShare=clamp(1-orbitalLoad-tradeLoad-scanLoad+parallelBonus,.28,1);
  stxSDAdvance(d,dt,buildQueueRate(p)*capacityShare);if(q.progress<.999||!stxSDAllDelivered(d))return;q.progress=1;launchBuiltShip(p,q.type);p.buildQueue.shift();
};
const STX_SD_tickExpansionProject=tickExpansionProject;
tickExpansionProject=function(p,dt){
  const q=p.expansionProject;if(!q)return;const target=state.planets.find(x=>x.id===q.targetId);if(!target||target.owner!==null||p.underAttack){if(target?.owner!==null)p.expansionProject=null;return}
  q.volunteers=Math.min(q.goal,q.volunteers+expansionRecruitRate(p,q)*dt);const d=stxSDDescriptors(p).find(x=>x.q===q);if(!d)return;stxSDEnsureOrders(d);const hull=stxSDCeiling(d);q.status=q.volunteers<q.goal?"recruiting":hull>=.999?"launch ready":`hull ${Math.floor(hull*100)}% supplied`;
  if(q.volunteers>=q.goal&&stxSDAllDelivered(d)){const settlers=Math.min(q.volunteers,p.pop*.07);p.pop=Math.max(.004,p.pop-settlers);const ship=createShip("colony",p,target,p.owner,{cargo:{population:settlers},strength:4,missionTitle:`Settlement of ${target.name}`,volunteers:settlers});p.expansionProject=null;if(ship&&p.owner===0)logEvent(`${fmtNum(settlers)} volunteers departed ${p.name} for ${target.name}.`,"good")}
};
if(typeof stxTickScanProject==="function")stxTickScanProject=function(p,dt){
  const q=p.scanProject;if(!q||p.owner===null)return;const d=stxSDDescriptors(p).find(x=>x.q===q);if(!d)return;stxSDEnsureOrders(d);stxSDAdvance(d,dt,.0052*(1+p.infra.factory*.28+p.infra.research*.52)*(q.mandated?1.35:1));if(q.progress<.999||!stxSDAllDelivered(d))return;
  q.progress=1;p.scanArray=(p.scanArray||0)+1;p.scanProject=null;p.mandateGlow=1;const e=empire(p.owner);e.tech.sensors=(e.tech.sensors||0)+.14;e.tech.orbital=(e.tech.orbital||0)+.04;state.effects.push({type:"shock",x:p.x,y:p.y,life:1.6,maxLife:1.6,size:48,color:e.color});if(p.owner===0){logEvent(`${p.name}'s sensor array is online. Enemy fleets can now be tracked much farther from the frontier.`,"good");galacticNews(`${p.name.toUpperCase()} SENSOR ARRAY ONLINE`,`Deep-space tracking coverage has expanded around ${p.name}.`,"good",p.id)}
};
if(typeof stxTickTradeStationProject==="function")stxTickTradeStationProject=function(p,dt){
  const q=p.tradeStationProject;if(!q||p.owner===null)return;const spec=STX_TRADE_STATION_LEVELS[q.level-1],d=stxSDDescriptors(p).find(x=>x.q===q);if(!d)return;stxSDEnsureOrders(d);stxSDAdvance(d,dt,.0036*(1+p.infra.factory*.24+p.infra.shipyard*.28)*(q.mandated?1.3:1));if(q.progress<.999||!stxSDAllDelivered(d))return;
  q.progress=1;p.tradeStation={level:q.level,name:spec.name,hp:spec.hp,maxHp:spec.hp,createdAt:p.tradeStation?.createdAt||state.simTime,lastIncomeAt:state.simTime};p.tradeStationProject=null;p.mandateGlow=1;const e=empire(p.owner);setModifier(e,"foreignTrade",1+spec.bonus,90);if(p.owner===0){logEvent(`${spec.name} opened above ${p.name}. Foreign merchants are offering better exchange terms.`,"good");galacticNews(`${p.name.toUpperCase()} OPENS ${spec.name.toUpperCase()}`,`The new trade station is drawing commercial traffic and improving negotiated exchange terms.`,"trade",p.id)}
};

/* Emergency production is a real conversion program: target output rises while
   other output/prosperity pays the opportunity cost. */
function stxSDProgramFor(owner,r){const e=empire(owner);return e?.stxSupplyPrograms?.[r]&&e.stxSupplyPrograms[r].until>state.simTime?e.stxSupplyPrograms[r]:null}
function stxSDProductionTick(p,dt){
  if(p.owner===null)return;const e=empire(p.owner);if(!e?.stxSupplyPrograms)return;
  Object.entries(e.stxSupplyPrograms).forEach(([r,program])=>{
    if(!program||program.until<=state.simTime)return;
    if(r==="components"&&p.infra.factory>0){const base=p.infra.factory*.055*Math.max(.12,p.factoryEfficiency)*dt,ironNeed=base*.64,silNeed=base*.45,rareNeed=base*.12,ratio=Math.min(1,(p.stock.iron||0)/Math.max(.001,ironNeed),(p.stock.silicates||0)/Math.max(.001,silNeed),(p.stock.rare||0)/Math.max(.001,rareNeed));if(ratio>0){consume(p,"iron",ironNeed*ratio);consume(p,"silicates",silNeed*ratio);consume(p,"rare",rareNeed*ratio);p.stock.components+=base*program.boost*ratio;p.stock.equipment=Math.max(0,(p.stock.equipment||0)-p.infra.factory*.0018*dt)}}
    else if(r==="equipment"&&p.infra.factory>0){const base=p.infra.factory*.018*Math.max(.12,p.factoryEfficiency)*dt,components=Math.min(p.stock.components||0,base*.5);p.stock.components-=components;p.stock.equipment+=base*program.boost*Math.max(.35,components/Math.max(.001,base*.5));}
    else if(r==="trained"&&p.infra.training>0){const extra=Math.min(p.pop*.000002*dt,p.infra.training*.000013*Math.max(.15,p.trainingEfficiency)*dt)*program.boost,gear=Math.min(p.stock.equipment||0,extra*1.4);const made=extra*Math.min(1,gear/Math.max(.000001,extra*1.4));p.stock.equipment-=gear;p.pop=Math.max(.004,p.pop-made);p.stock.trained=(p.stock.trained||0)+made;p.prosperity=Math.max(0,(p.prosperity||0)-dt*.00012)}
    else if(STX_SD_RAW.has(r)&&p.infra.mine>0){const q=p.quality?.[r]||1,base=q*.13*Math.min(p.infra.mine,sustainableMines(p))*Math.max(.1,p.mineEfficiency)*dt,extra=Math.min(p.reserve[r]||0,base*program.boost);p.reserve[r]=Math.max(0,(p.reserve[r]||0)-extra);p.stock[r]=(p.stock[r]||0)+extra;for(const other of STX_SD_RAW){if(other!==r)p.stock[other]=Math.max(0,(p.stock[other]||0)-(p.quality?.[other]||1)*.13*Math.min(p.infra.mine,sustainableMines(p))*Math.max(.1,p.mineEfficiency)*dt*.1)}}
    p.prosperity=Math.max(0,(p.prosperity||0)-dt*.00008);
  });
}
const STX_SD_tickPlanet=tickPlanet;
tickPlanet=function(p,dt){if(p?.owner!==null)stxSDAllocatePlanet(p);STX_SD_tickPlanet(p,dt);stxSDProductionTick(p,dt)};
const STX_SD_logisticsTick=logisticsTick;
logisticsTick=function(){STX_SD_logisticsTick();state.empires.forEach(e=>{if((e.stxEmergencyFreightUntil||0)<=state.simTime)return;owned(e.id).forEach(p=>p.orders.filter(o=>o.status==="waiting").sort((a,b)=>b.priority-a.priority||b.age-a.age).slice(0,2).forEach(o=>fillOrder(p,o)))})};
