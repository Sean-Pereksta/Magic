/* Living planets. Population and personnel use the existing millions unit.
   Vessel reserves are whole hulls plus fractional work. Stored resources are
   committed only to orders; routine industry, training and services are free. */
const STX_LP_CREW=.0002; // 200 personnel per vessel; same pool as legacy trained crew.
let stxLPSimulating=false,stxLPTicking=false;
const stxLPNumber=(v,fallback=0)=>Number.isFinite(Number(v))?Number(v):fallback;
function stxLPEnsure(p){
  if(!p||p.owner==null)return p;
  if(p.stxLivingVersion!==1){
    p.infra.city=Math.max(1,Math.floor(p.infra.city||1));
    p.capacity=Math.max(p.capacity||0,p.pop||0,.05);
    p.stxCityCapacity=Math.max(.06,p.capacity/p.infra.city);
    // Preserve mature worlds, existing population, crew and every active fleet.
    p.reserveVessels=Math.max(0,stxLPNumber(p.reserveVessels,Math.floor((p.infra.shipyard||0)*6+(p.infra.factory||0)*2)));
    p.factoryModes=Array.isArray(p.factoryModes)?p.factoryModes:[];
    p.stxLocalProjects=Array.isArray(p.stxLocalProjects)?p.stxLocalProjects:[];
    p.stxOrbitalProjects=Array.isArray(p.stxOrbitalProjects)?p.stxOrbitalProjects:[];
    p.stxLivingVersion=1;
  }
  for(const [field,list] of [['localProject','stxLocalProjects'],['orbitalProject','stxOrbitalProjects']]){
    // JSON duplicates the legacy pointer. Reunite by the persistent project ID.
    const q=p[field];if(q){const id=stxSDProjectId(q,field);const existing=p[list].find(x=>stxSDProjectId(x,field)===id);if(existing)p[field]=existing;else p[list].unshift(q)}
  }
  const count=stxLPTicking?p.factoryModes.length:Math.max(0,Math.floor((p.infra.factory||0)+1e-6));
  while(p.factoryModes.length<count)p.factoryModes.push(p.owner!==0&&p.factoryModes.length===0?'war':p.stxManufacturingFocus==='equipment'?'equipment':'components');
  if(p.factoryModes.length>count)p.factoryModes.length=count;
  p.factoryModes=p.factoryModes.map(m=>['components','equipment','war'].includes(m)?m:'components');
  p.stock.trained=Math.max(0,stxLPNumber(p.stock.trained));
  p.reserveVessels=Math.max(0,stxLPNumber(p.reserveVessels));
  return p;
}
function stxLPWorkforce(p){
  const labor=Math.max(0,p.pop)*.56,capacity=.025*(p.infra.mine||0)+.045*(p.infra.factory||0)+.03*(p.infra.training||0)+.035*(p.infra.shipyard||0);
  return clamp(labor/Math.max(.02,capacity),.05,1);
}
function stxLPGrowPopulation(p,dt){
  const e=empire(p.owner),cap=Math.max(.004,p.capacity),room=Math.max(0,cap-p.pop);
  const rate=(.000025*p.pop+.000014)*(p.popMomentum??1)*modifier(e,'growth')*modifier(e,'birthrate')*stxPolicyGrowth(p);
  // Exact approach prevents overshoot at high game speeds. Over-cap survivors
  // from battles/migration are preserved; natural growth waits for housing.
  return p.pop+room*(1-Math.exp(-Math.max(0,rate)*Math.max(0,dt)/cap));
}
function stxLPRates(p){
  const e=empire(p.owner),work=stxLPWorkforce(p),eff=work*populationEfficiency(p)*modifier(e,'industry')*modifier(e,'civilEfficiency')*(1+e.tech.automation*.1)*supplyReadiness(p);
  const counts={components:0,equipment:0,war:0};for(const mode of p.factoryModes||[])counts[mode]++;
  const focus=STX_RT_FOCUSES[p.stxEconomicFocus]||STX_RT_FOCUSES.balanced;
  const allocation=Math.min(1,(p.infra.factory||0)/Math.max(1,p.factoryModes.length));
  return {work,eff,counts,components:counts.components*.24*allocation*eff*focus.components*stxPolicyValue(p.owner,'components',p),equipment:counts.equipment*.19*allocation*eff*focus.equipment*stxPolicyValue(p.owner,'equipment',p),vessels:counts.war*.018*allocation*eff*modifier(e,'shipbuilding'),personnel:Math.max(0,p.pop*.000008)*(1+(p.infra.training||0)*.6+(p.infra.defense||0)*.12)*modifier(e,'training'),personnelCap:p.pop*.08};
}
stxRTProduceFactory=function(p,dt){
  if(!stxLPTicking)stxLPEnsure(p);const r=stxLPRates(p);p.factoryEfficiency=r.eff;
  p.stock.components+=r.components*dt;p.stock.equipment+=r.equipment*dt;p.reserveVessels+=r.vessels*dt;
  p.stxAllocationOutput={focus:p.stxEconomicFocus,componentRate:r.components,equipmentRate:r.equipment,updatedAt:state.simTime};
  p.stxVesselRate=r.vessels;return r;
};
function stxLPPassiveIndustry(p,dt){
  const r=stxRTProduceFactory(p,dt);
  const crew=Math.min(Math.max(0,r.personnelCap-p.stock.trained),r.personnel*dt);
  p.stock.trained+=crew;p.trainingEfficiency=r.work;p.stxCrewRate=dt?crew/dt:0;
  p.stxPopulationGrowth=stxLPGrowPopulation(p,1)-p.pop;
}
const STX_LP_tickPlanet=tickPlanet;
tickPlanet=function(p,dt){stxLPEnsure(p);stxLPTicking=true;try{return STX_LP_tickPlanet(p,dt)}finally{stxLPTicking=false}};
const STX_LP_generate=generateGalaxy;
generateGalaxy=function(){STX_LP_generate();state.planets.forEach(stxLPEnsure)};
const STX_LP_load=loadGame;
loadGame=function(){const ok=STX_LP_load();if(ok){state.planets.forEach(stxLPEnsure);stxLPPruneDomesticQueue()}return ok};
const STX_LP_simulate=simulate;
simulate=function(dt){stxLPSimulating=true;try{return STX_LP_simulate(dt)}finally{stxLPSimulating=false}};
function stxLPCanOrder(p){return !!p&&p.owner!=null&&!(p.owner===0&&stxLPSimulating)}

// Keep legacy project pointers for callers while each project has its own ledger.
const STX_LP_startLocal=startLocalProject;
startLocalProject=function(p,type,source='Player commission'){
  if(!stxLPCanOrder(p)||p.underAttack||!['city','mine','factory','training','defense','shipyard','research'].includes(type))return false;
  stxLPEnsure(p);const current=p.localProject;p.localProject=null;
  try{const ok=STX_LP_startLocal(p,type,source);if(ok){p.localProject.stxPlayerOrdered=p.owner===0;stxSDProjectId(p.localProject,'local');p.stxLocalProjects.push(p.localProject)}return ok}
  finally{p.localProject=current||p.stxLocalProjects[0]||null}
};
const STX_LP_orbital=queueOrbitalProject;
queueOrbitalProject=function(p,type,mandated=false){
  if(!stxLPCanOrder(p))return false;stxLPEnsure(p);
  if((p.orbitals?.[type]||0)+p.stxOrbitalProjects.filter(q=>q.type===type).length>=2)return false;
  const current=p.orbitalProject;p.orbitalProject=null;
  try{const ok=STX_LP_orbital(p,type,mandated);if(ok){stxSDProjectId(p.orbitalProject,'orbital');p.stxOrbitalProjects.push(p.orbitalProject)}return ok}
  finally{p.orbitalProject=current||p.stxOrbitalProjects[0]||null}
};
const STX_LP_descriptors=stxSDDescriptors;
stxSDDescriptors=function(p){
  const result=STX_LP_descriptors(p);if(!p||p.owner==null)return result;
  for(const [field,list] of [['localProject','stxLocalProjects'],['orbitalProject','stxOrbitalProjects']]){
    const current=p[field];try{for(const q of p[list]||[]){if(q===current)continue;p[field]=q;const d=STX_LP_descriptors(p).find(d=>d.q===q);if(d)result.push(d)}}finally{p[field]=current}
  }
  return [...new Map(result.map(d=>[d.id,d])).values()];
};
function stxLPParallelTick(p,dt,field,list,ticker){
  if(!p||p.owner==null)return;if(!p.stxLivingVersion)stxLPEnsure(p);
  try{for(const q of [...p[list]]){p[field]=q;if(p.underAttack){stxActionSet(q,'BLOCKED','Construction paused during battle');continue}
    const cap=p.capacity,cities=p.infra.city;ticker(p,dt);
    if(!p[field]){p[list]=p[list].filter(x=>x!==q);if(p.infra.city>cities)p.capacity=cap+p.stxCityCapacity*(p.infra.city-cities)}
  }}finally{p[field]=p[list][0]||null}
}
const STX_LP_localTick=tickLocalProject,STX_LP_orbitalTick=tickOrbitalProject;
tickLocalProject=function(p,dt){return stxLPParallelTick(p,dt,'localProject','stxLocalProjects',STX_LP_localTick)};
tickOrbitalProject=function(p,dt){return stxLPParallelTick(p,dt,'orbitalProject','stxOrbitalProjects',STX_LP_orbitalTick)};
// Physical facility upgrades keep their real tier prerequisites; independent
// ground facilities have no shared construction-slot limit.
const STX_LP_canPhysical=stxOLCanQueue;
stxOLCanQueue=function(p,kind,option={}){
  if(!stxLPCanOrder(p)||p.owner!==0||p.underAttack)return false;
  if(['mining','factory','shipyard'].includes(kind))return true;
  // Only the same tier/module must wait for its predecessor.
  const projects=p.physicalProjects;p.physicalProjects=(projects||[]).filter(q=>q.kind===kind);
  try{return STX_LP_canPhysical(p,kind,option)}finally{p.physicalProjects=projects}
};

const STX_LP_expansion=startExpansionProject;
startExpansionProject=function(e,p,...args){return stxLPCanOrder(p)?STX_LP_expansion(e,p,...args):false};
const STX_LP_reconstruct=beginReconstruction;
beginReconstruction=function(p,...args){return stxLPCanOrder(p)?STX_LP_reconstruct(p,...args):false};
const STX_LP_tradeStation=stxQueueTradeStationProject;
stxQueueTradeStationProject=function(p,...args){return stxLPCanOrder(p)?STX_LP_tradeStation(p,...args):false};
// Routine services are funded by their installed infrastructure, with no
// hidden freight fees, station upkeep or priority-training equipment drain.
const STX_LP_upkeep=stxIFUpkeep;
stxIFUpkeep=function(base){return base.owner===0?{}:STX_LP_upkeep(base)};
const STX_LP_freight=stxIFChargeFreight;
stxIFChargeFreight=function(ship,from,to){
  if(ship.owner!==0)return STX_LP_freight(ship,from,to);
  ship.speed*=1+stxIFRouteBonus(from,to,0);ship.stxIFPhysicalCargo=true;ship.stxIFIntendedOwner=0;
  for(const [r,a] of Object.entries(ship.cargo||{}))stxIFRecord(from,'exports',r,a);
};
const STX_LP_canFreight=stxIFCanFreight;
stxIFCanFreight=function(owner){return owner===0?stxIFFreightUsed(owner)<stxIFFreightLimit(owner):STX_LP_canFreight(owner)};
stxIFCrewMobilization=function(){}; // Recruitment is owned by passive industry.
const STX_LP_tradeResource=tradeResource;
tradeResource=function(from,to){return from?.owner===0?null:STX_LP_tradeResource(from,to)};
// No automatic shipyard jobs: all old, already committed jobs still finish.
const STX_LP_build=tickBuildQueue;
tickBuildQueue=function(p,dt){if(!p?.buildQueue?.length)return;return STX_LP_build(p,dt)};

function stxLPFleetPlan(p,count){
  if(!p||p.owner==null||p.underAttack)return {ok:false,reason:'Choose a safe owned staging world'};
  stxLPEnsure(p);const vessels=Number(count),crew=vessels*STX_LP_CREW;
  if(!Number.isSafeInteger(vessels)||vessels<1||vessels>500)return {ok:false,reason:'Choose 1–500 vessels'};
  if(p.infra.shipyard<=0)return {ok:false,reason:'An operational shipyard is required'};
  if(p.reserveVessels+1e-9<vessels||p.stock.trained+1e-12<crew)return {ok:false,reason:`Need ${vessels} reserve vessels and ${Math.round(crew*1e6).toLocaleString()} personnel at ${p.name}`};
  return {ok:true,vessels,crew,source:p.id,reason:`${vessels} vessels and ${Math.round(crew*1e6).toLocaleString()} personnel from ${p.name}`};
}
function stxLPMobilize(p,count=15,role='fleet',name=''){
  const plan=stxLPFleetPlan(p,count);if(!plan.ok)return false;
  const f=registerFleet(p.owner,p,plan.vessels*1.2*(1+empire(p.owner).tech.weapons*.15),role);
  if(!f)return false;p.reserveVessels-=plan.vessels;p.stock.trained-=plan.crew;
  f.vesselCount=plan.vessels;f.personnel=plan.crew;f.mobilizationSources=[{planetId:p.id,vessels:plan.vessels,personnel:plan.crew}];
  if(String(name).trim())f.name=String(name).trim().replace(/[<>&"']/g,'').slice(0,64);
  f.location=p.id;f.status=`Stationed at ${p.name}`;p.mandateGlow=1;return f;
}
stxQueueFleetCommission=function(p,type='fleet'){
  if(!stxLPCanOrder(p))return false;return !!stxLPMobilize(p,type==='patrol'?6:15,type==='patrol'?'patrol':'fleet');
};
// Old paid shipbuilding jobs now deliver hull reserves, never unrequested fleets.
const STX_LP_launch=launchBuiltShip;
launchBuiltShip=function(p,type){if(!['fleet','patrol'].includes(type))return STX_LP_launch(p,type);stxLPEnsure(p);p.reserveVessels+=type==='patrol'?6:15;return {reserveDelivery:true}};
stxIFAReserveWorlds=function(){return playerWorlds().filter(p=>stxLPFleetPlan(p,6).ok)};
const STX_LP_assemble=stxIFAAssembleActiveFleets;
stxIFAAssembleActiveFleets=function(source){
  if(stxDGDeployableFleets().length||stxIFAStationedMilitaryFleets().length)return STX_LP_assemble(source);
  let count=0;for(const p of stxIFAReserveWorlds().slice(0,2))if(stxLPMobilize(p,6))count++;return count;
};
stxEWPAssembleReserveFleet=function(e,target){const p=owned(e?.id).find(p=>stxLPFleetPlan(p,6).ok);return p&&e.id!==0?stxLPMobilize(p,6):null};
// Patrolling reuses the existing fleet movement layer, never garrison conversion.
patrolTick=function(){};
function stxLPRush(p,count=10){
  if(!p||p.owner!==0||p.underAttack||!Number.isSafeInteger(count)||count<1)return false;
  stxLPEnsure(p);const need={components:count*3,equipment:count*2},cost=count*5;
  if(empire(0).credits<cost||Object.entries(need).some(([r,a])=>p.stock[r]<a))return false;
  empire(0).credits-=cost;for(const [r,a] of Object.entries(need))p.stock[r]-=a;p.reserveVessels+=count;return true;
}
function stxLPMode(p,index,mode){
  if(!p||p.owner!==0)return false;stxLPEnsure(p);
  if(!Number.isInteger(index)||index<0||index>=p.factoryModes.length||!['components','equipment','war'].includes(mode))return false;
  p.factoryModes[index]=mode;return true;
}
for(const c of COMMANDS){
  if(c.id==='reserveActivation'){c.desc='Mobilize six existing reserve vessels and 1,200 personnel at a shipyard.';c.apply=(e,t)=>stxLPMobilize(t,6);c.target=()=>playerWorlds().find(p=>stxLPFleetPlan(p,6).ok);c.score=()=>c.target()?65:0}
  if(c.id==='fleetCommission'){c.desc='Mobilize 15 reserve vessels and 3,000 personnel at one shipyard.';c.effects=['Uses existing vessels and personnel','Remains at the named staging world'];c.target=()=>playerWorlds().find(p=>stxLPFleetPlan(p,15).ok);c.score=()=>c.target()?80:0;c.apply=(e,t)=>!!stxLPMobilize(t,15)}
}
COMMANDS.push({id:'stxEmergencyVessels',cat:'Military',title:'Emergency Vessel Construction',desc:'Spend 50 credits, 30 Components and 20 Equipment at a world to add 10 reserve vessels immediately.',effects:['10 reserve vessels','Personnel still required for mobilization'],target:()=>playerWorlds().find(p=>!p.underAttack&&p.stock.components>=30&&p.stock.equipment>=20),score:()=>empire(0).credits>=50?60:0,apply:(e,p)=>stxLPRush(p)});

function stxLPDomestic(type,q){return type==='military'||type==='proposal'&&q?.kind==='governor'}
function stxLPPruneDomesticQueue(){if(empire(0)?.stxTransmissionQueue)empire(0).stxTransmissionQueue=empire(0).stxTransmissionQueue.filter(x=>!stxLPDomestic(x.type,stxTXFind(x.type,x.id)))}
const STX_LP_enqueue=stxTXEnqueue;
stxTXEnqueue=function(type,id,...args){if(stxLPDomestic(type,stxTXFind(type,id)))return false;return STX_LP_enqueue(type,id,...args)};
const STX_LP_processQueue=stxTXProcessQueue;
stxTXProcessQueue=function(){stxLPPruneDomesticQueue();return STX_LP_processQueue()};
const STX_LP_eligibility=stxTXEligibility;
stxTXEligibility=function(type,q,action){const result=STX_LP_eligibility(type,q,action);if(result.ok&&type==='military'&&q.type!=='capital')return stxLPFleetPlan(state.planets.find(p=>p.id===q.planetId),q.type==='patrol'?6:15);return result};
const STX_LP_actionMarkup=stxTXActionMarkup;
stxTXActionMarkup=function(type,q){let html=STX_LP_actionMarkup(type,q);if(stxLPDomestic(type,q))html=html.replace(/<button[^>]*data-tx-action="(?:queue|unqueue)"[^>]*>.*?<\/button>/g,'');
  const pay=type==='trade'?q.payment:q.request;
  if(pay){const rows=[];if(pay.credits)rows.push(`Credits: ${Math.floor(empire(0).credits)} / ${pay.credits}`);if(pay.resource)rows.push(`${stxRTLabel(pay.resource)}: ${stxRTFmt(empireResource(0,pay.resource))} / ${stxRTFmt(pay.amount)}`);html+=`<div class="subtle">Available / Required · ${stxRTEscape(rows.join(' · '))}</div>`}return html;
};
const STX_LP_transmissions=renderTransmissions;
renderTransmissions=function(){STX_LP_transmissions();const box=$('transmissionList');if(!box||stxTXPointerActive)return;
  let header=$('stxLivingResources');if(!header){header=document.createElement('div');header.id='stxLivingResources';header.className='stx-lp-resources';box.before(header)}
  if(!empire(0))return;header.innerHTML=`<b>Credits ${fmtStock(empire(0).credits)}</b>`+[...new Set([...STX_RT_RESOURCES,...RESOURCES])].map(r=>`<span>${r==='helium'?'Helium / Fuel':stxRTLabel(r)} <b>${fmtStock(empireResource(0,r))}</b></span>`).join('')+`<span>Freight capacity <b>${stxIFFreightUsed(0)} / ${stxIFFreightLimit(0)}</b></span>`;
};
const STX_LP_finalizeShip=stxActionFinalizeShip;
stxActionFinalizeShip=function(p,q){
  if(!['fleet','patrol'].includes(q.type))return STX_LP_finalizeShip(p,q);
  if(stxAction(q,'ship').state!=='COMPLETED'){
    stxLPEnsure(p);p.reserveVessels+=q.type==='patrol'?6:15;
    // Crew already reserved in the legacy job returns to the available pool.
    p.stock.trained+=stxSDDelivered({q},'trained');stxActionComplete(q);
  }
  p.buildQueue=p.buildQueue.filter(x=>x!==q);return true;
};

const STX_LP_colony=stxIFColonyTick;
stxIFColonyTick=function(p,dt){
  if(p.owner!==0)return STX_LP_colony(p,dt);const c=p.stxColony;if(!c||c.stage==='developed')return;
  c.age+=dt;c.development+=dt;c.readiness=1;c.isolatedFor=0;
  c.stage=c.age<8?'landing':c.development<60?'outpost':c.development<180?'young colony':c.development<360?'established':'developed';
};
// Remove obsolete passive input requests; project ledgers and approved policies
// retain their physical freight. No resources are refunded or fabricated.
const STX_LP_addOrder=addOrder;
addOrder=function(p,type,...args){
  if(p?.owner===0&&['material','factory','equipment','research','priority-training','colony-support'].includes(type))return null;
  return STX_LP_addOrder(p,type,...args);
};
const STX_LP_fillOrder=fillOrder;
fillOrder=function(p,o){if(p?.owner===0&&['material','factory','equipment','research','priority-training','colony-support'].includes(o?.type))return;return STX_LP_fillOrder(p,o)};

// Regional staging uses visible transports. Mobilization only draws from the
// staging world's arrived reserves, never from an empire-wide inventory.
function stxLPNearby(p){return owned(p.owner).filter(x=>x!==p&&!x.underAttack&&dist(x,p)<=900).sort((a,b)=>dist(a,p)-dist(b,p))}
function stxLPTransfer(from,to,count){
  if(!from||!to||from.owner!==0||to.owner!==0||from===to||from.underAttack||to.underAttack||dist(from,to)>900)return false;
  stxLPEnsure(from);stxLPEnsure(to);const crew=count*STX_LP_CREW;
  if(!Number.isSafeInteger(count)||count<1||count>500||from.reserveVessels<count||from.stock.trained<crew)return false;
  const ship=createShip('supply',from,to,0,{cargo:{militaryVessels:count,trained:crew},stxLPMuster:true,vesselName:`${from.name} reserve transfer`,strength:0});
  if(!ship)return false;from.reserveVessels-=count;from.stock.trained-=crew;return true;
}
const STX_LP_arrive=arriveShip;
arriveShip=function(s,p){
  if(!s.stxLPMuster)return STX_LP_arrive(s,p);
  if(s.stxCancelled||s.stxIntercepted)return;
  if(p.owner!==s.owner){
    const home=owned(s.owner).filter(x=>!x.underAttack).sort((a,b)=>dist(p,a)-dist(p,b))[0];
    const back=home&&createShip('supply',p,home,s.owner,{cargo:{...s.cargo},stxLPMuster:true,vesselName:`Returning ${s.vesselName}`});
    if(!back)s.stxArrivalBlocked='No safe port or transport capacity for reserve return';return;
  }
  stxLPEnsure(p);p.reserveVessels+=s.cargo.militaryVessels||0;p.stock.trained+=s.cargo.trained||0;
};
const stxLPDrafts=new Map();
function stxLPDraft(p){if(!stxLPDrafts.has(p.id))stxLPDrafts.set(p.id,{count:6,name:'',mobilizeOpen:false,buildOpen:false});return stxLPDrafts.get(p.id)}
function stxLPProjectMarkup(p){
  const descs=stxSDDescriptors(p);
  return descs.length?descs.map(d=>{
    const missing=Object.entries(d.need||{}).filter(([r])=>stxSDRemaining(d,r)>stxIFEpsilon(r));
    const reason=p.underAttack?'Paused: battle in progress':missing.length?`Awaiting ${missing.map(([r])=>`${stxSDResourceLabel(r)} ${stxSDFmt(r,stxSDRemaining(d,r))}`).join(', ')}`:'Supplied · building';
    const rate=d.kind==='local'?.0065*(1+p.infra.factory*.22)*(stxSDSourceIsImperial(d.q)?1.25:1)*stxIFCapacityShare(d):null;
    const eta=rate&&!missing.length&&!p.underAttack?` · ~${fmtEta((1-stxSDProgress(d))/rate)}`:'';
    return `<div class="project-row"><div class="project-head"><b>${stxRTEscape(d.title)}</b><span>${Math.floor(stxSDProgress(d)*100)}%</span></div><progress max="1" value="${stxSDProgress(d)}"></progress><small>${stxRTEscape(reason+eta)}</small></div>`;
  }).join(''):'<p class="subtle">No active projects. Orders below run in parallel.</p>';
}
function stxLPPanel(p){
  stxLPEnsure(p);const r=stxLPRates(p),esc=stxRTEscape,local=state.fleets.filter(f=>f.owner===p.owner&&!f.destroyed&&f.location===p.id),cities=p.stxLocalProjects.filter(q=>q.type==='city').length;
  const stat=(label,value)=>`<span><label>${label}</label><b>${value}</b></span>`;
  const draft=stxLPDraft(p),plan=stxLPFleetPlan(p,Number(draft.count));
  const facilities=(p.orbitalFacilities||[]).filter(f=>f.hp>0).map(f=>f.name).join(', ');
  return `<section class="stx-lp-panel"><div class="section-label">POPULATION</div><div class="stx-ol-stat-grid">${stat('Population / capacity',`${fmtNum(p.pop)} / ${fmtNum(p.capacity)}`)}${stat('Cities',`${p.infra.city} (+${cities} constructing)`)}${stat('Growth / minute',`+${Math.round((stxLPGrowPopulation(p,60)-p.pop)*1e6).toLocaleString()}`)}</div><small>Each new city adds ${fmtNum(p.stxCityCapacity)} capacity. People grow into the new space.</small>
  <div class="section-label">ECONOMY & INDUSTRY</div><div class="stx-ol-stat-grid">${stat('Base tax / minute',((.018+p.pop*.018)*60).toFixed(2)+' cr')}${stat('Components / minute',(r.components*60).toFixed(2))}${stat('Equipment / minute',(r.equipment*60).toFixed(2))}${stat('Workforce utilization',Math.round(r.work*100)+'%')}${stat('Component factories',r.counts.components)}${stat('Equipment factories',r.counts.equipment)}${stat('War factories',r.counts.war)}${stat('Reserve vessels / minute','+'+(r.vessels*60).toFixed(2))}</div>
  <p class="subtle">Factory count sets capacity; population fills jobs. Normal production and recruitment have no recurring resource cost.</p>
  ${p.owner===0?p.factoryModes.map((m,i)=>`<label class="stx-lp-mode">Factory ${i+1}<select data-lp-mode="${i}">${[['components','Component Production'],['equipment','Equipment Production'],['war','War Production']].map(([v,t])=>`<option value="${v}" ${v===m?'selected':''}>${t}</option>`).join('')}</select></label>`).join(''):''}
  <div class="section-label">MILITARY</div><div class="stx-ol-stat-grid">${stat('Available personnel',Math.floor(p.stock.trained*1e6).toLocaleString())}${stat('Reserve vessels',Math.floor(p.reserveVessels))}${stat('Active local fleets',local.length)}${stat('Recruitment / minute',Math.round(r.personnel*60*1e6).toLocaleString())}${stat('Mobilization capacity',Math.min(Math.floor(p.reserveVessels),Math.floor(p.stock.trained/STX_LP_CREW))+' vessels')}</div>
  ${p.owner===0?`<details class="stx-lp-mobilize" ${draft.mobilizeOpen?'open':''}><summary>Commission Fleet · ${esc(p.name)}</summary><p class="subtle">All vessels and 200 personnel per vessel come from ${esc(p.name)}. Nearby reserves must arrive by transport first.</p><label>Fleet size<select id="stxLPSize">${[6,15,30].map(n=>`<option value="${n}" ${Number(draft.count)===n?'selected':''}>${n} vessels</option>`).join('')}<option value="custom" ${![6,15,30].includes(Number(draft.count))?'selected':''}>Custom</option></select></label><label>Vessels<input id="stxLPCount" type="number" min="1" max="500" value="${esc(draft.count)}"></label><label>Fleet name<input id="stxLPName" maxlength="64" value="${esc(draft.name)}" placeholder="Automatic fleet name"></label><p id="stxLPPlan" class="subtle">${esc(plan.reason)}</p><button class="choice-btn" data-lp-action="mobilize" ${plan.ok?'':'disabled'}>Commission fleet</button><button class="choice-btn" data-lp-action="rush">Rush 10 vessels · 50 cr + 30 Components + 20 Equipment</button>
  <div class="section-label">NEARBY STAGING SOURCES · 900u RANGE</div>${stxLPNearby(p).map(q=>{stxLPEnsure(q);return `<div class="project-row"><b>${esc(q.name)} · ${Math.round(dist(q,p))}u</b><small>${Math.floor(q.reserveVessels)} vessels · ${Math.floor(q.stock.trained*1e6).toLocaleString()} personnel · transit ~${fmtEta(dist(q,p)/96)}</small><button class="choice-btn" data-lp-source="${q.id}">Transfer selected vessel count here</button></div>`}).join('')||'<p class="subtle">No safe nearby worlds.</p>'}${state.ships.filter(s=>s.stxLPMuster&&s.to===p.id).map(s=>`<p class="subtle">${esc(s.vesselName)} · ${s.cargo.militaryVessels} vessels · ${fmtEta(shipEta(s))}</p>`).join('')}</details>`:''}
  <div class="section-label">CONSTRUCTION · ALL ACTIVE PROJECTS</div>${stxLPProjectMarkup(p)}
  <p class="subtle">Materials are committed as delivered. Concurrent projects share industrial labor (${(stxIFCapacityShare({p,priority:4})*100).toFixed(0)}% standard share). No shared construction slot.</p>
  ${p.owner===0?`<details class="stx-lp-build" ${draft.buildOpen?'open':''}><summary>Order development · 3 cr authorization + listed materials</summary>${['city','factory','training','defense','shipyard','mine','research'].map(type=>{const cost={city:36,factory:46,training:40,defense:34,shipyard:62,mine:30,research:55}[type];return `<div class="project-row"><button class="choice-btn" data-lp-build="${type}">Build ${type}</button><small>${Object.entries(STX_PS_RECIPES[type]).map(([r,share])=>`${stxSDResourceLabel(r)} ${(share*cost).toFixed(1)}`).join(' · ')}</small></div>`}).join('')}</details>`:''}
  <div class="section-label">ORBIT</div><p class="subtle">${esc(facilities||'No modular facilities')} · ${p.orbitals?.station||0} stations · ${p.orbitals?.base||0} bases · ${p.infra.shipyard||0} shipyards · ${local.length} stationed fleets</p></section>`;
}
const STX_LP_renderPlanet=renderPlanet;
renderPlanet=function(){const current=$('planetBody'),active=document.activeElement;if(current?.contains(active)&&['INPUT','SELECT'].includes(active?.tagName))return;STX_LP_renderPlanet();const p=state.selected,body=$('planetBody');if(!p||p.owner==null||!body)return;
  body.insertAdjacentHTML('afterbegin',stxLPPanel(p));const draft=stxLPDraft(p);
  for(const [selector,key] of [['.stx-lp-mobilize','mobilizeOpen'],['.stx-lp-build','buildOpen']]){const node=body.querySelector(selector);if(node)node.ontoggle=()=>{if(node.isConnected)draft[key]=node.open}}
  body.querySelectorAll('[data-ol-focus]').forEach(b=>{const next=p.stxManufacturingFocus==='components'?'equipment':'components';b.textContent=`Civil factories → ${next}`;b.onclick=()=>{p.stxManufacturingFocus=next;p.factoryModes=p.factoryModes.map(m=>m==='war'?m:next);saveGame(false);renderPlanet()}});
  body.querySelectorAll('[data-lp-mode]').forEach(b=>b.onchange=()=>{stxLPMode(p,Number(b.dataset.lpMode),b.value);saveGame(false);renderPlanet()});
  body.querySelectorAll('[data-lp-build]').forEach(b=>b.onclick=()=>{const ok=startLocalProject(p,b.dataset.lpBuild,'Player commission');showToast(ok?'Project authorized; materials reserved as delivered':'Cannot authorize: check credits and battle status');saveGame(false);renderPlanet()});
  const refreshPlan=()=>{const plan=stxLPFleetPlan(p,Number(draft.count));if($('stxLPPlan'))$('stxLPPlan').textContent=plan.reason;const button=body.querySelector('[data-lp-action="mobilize"]');if(button)button.disabled=!plan.ok};
  if($('stxLPCount'))$('stxLPCount').oninput=e=>{draft.count=e.target.value;refreshPlan()};
  if($('stxLPName'))$('stxLPName').oninput=e=>draft.name=e.target.value;
  if($('stxLPSize'))$('stxLPSize').onchange=e=>{if(e.target.value!=='custom'){draft.count=Number(e.target.value);$('stxLPCount').value=draft.count;refreshPlan()}else $('stxLPCount').focus()};
  body.querySelectorAll('[data-lp-action]').forEach(b=>b.onclick=()=>{const ok=b.dataset.lpAction==='rush'?stxLPRush(p):stxLPMobilize(p,Number(draft.count),'fleet',draft.name);showToast(ok?'Military order completed':'Insufficient local reserves, personnel or resources');saveGame(false);renderPlanet();updateHud(true)});
  body.querySelectorAll('[data-lp-source]').forEach(b=>b.onclick=()=>{const source=state.planets.find(q=>q.id===b.dataset.lpSource);const ok=stxLPTransfer(source,p,Number(draft.count));showToast(ok?'Reserve transport dispatched':'Transfer unavailable: check reserves, distance and freight capacity');saveGame(false);renderPlanet()});
};
const stxLPStyles=document.createElement('style');stxLPStyles.textContent=`.stx-lp-panel{padding:12px;margin-bottom:14px;border:1px solid #356079;border-radius:12px;background:#0b1727}.stx-lp-panel small{display:block;line-height:1.5;color:#a5b9ce}.stx-lp-panel progress{width:100%;height:7px;accent-color:#67e4ce}.stx-lp-panel label{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:8px 0;color:#b8cadd;font-size:11px}.stx-lp-panel select,.stx-lp-panel input{min-width:0;max-width:100%;flex:1;padding:7px;border:1px solid #35556f;border-radius:6px;background:#101f32;color:#e5f4ff}.stx-lp-panel summary{cursor:pointer;color:#7ee9d3;padding:8px 0;font-weight:700}.stx-lp-panel .choice-btn{white-space:normal}.stx-lp-resources{position:sticky;top:0;z-index:4;display:flex;flex-wrap:wrap;gap:8px;padding:12px;background:#0b1829;border:1px solid #37586e;border-radius:10px;margin-bottom:12px;font-size:12px}.stx-lp-resources span{color:#b8cadd}.stx-lp-resources b{color:#fff}`;document.head.appendChild(stxLPStyles);

STX_POLICIES['crew-training'].effects=['+60% personnel recruitment','20 credits on activation; −10% growth; no normal training upkeep'];
STX_POLICIES['naval-recruitment'].effects=['+90% personnel recruitment for 50 seconds','35 credits on activation; −20% growth'];
STX_POLICIES['component-drive'].effects=['+70% Components; −35% Equipment','Paid activation; no recurring factory input cost'];
STX_POLICIES['equipment-drive'].effects=['+70% Equipment; −35% Components','Paid activation; no recurring factory input cost'];
STX_POLICIES['heavy-industry'].effects=['+30% Components and Equipment','Paid activation; −10% growth and tax income'];
STX_RT_FOCUSES.mobilization.description='Commit civilian labor to heavy industry; slower growth and lower tax income.';
