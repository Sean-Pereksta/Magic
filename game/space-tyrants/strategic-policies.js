/* Immediate government policies. State is stored on each empire so the core
   save format preserves durations, cooldowns, AI decisions and recent choices.
   Construction still uses the existing physical delivery/priority pipeline. */

const STX_POLICIES={
  "encourage-settlement":{title:"Encourage Settlement",group:"population",cat:"Population",duration:85,cost:22,mods:{migration:1.4},upkeep:{equipment:.002},effects:["+45% growth on uncrowded worlds; +65% on small colonies","More migration; consumes civilian Equipment"]},
  "frontier-resettlement":{title:"Frontier Resettlement Program",group:"population",cat:"Population",duration:80,cost:25,mods:{migration:2,growth:1.12},upkeep:{helium:.003},effects:["Civilian ships move people from crowded cores to colonies","+12% growth; transport consumes Helium"]},
  "population-expansion":{title:"Population Expansion Initiative",group:"population",cat:"Population",duration:90,cost:42,mods:{growth:1.35},upkeep:{equipment:.006,silicates:.01},effects:["+35% population growth; housing pressure rises","Consumes Equipment and Silicates"]},
  "crew-training":{title:"Accelerated Crew Training",group:"personnel",cat:"Military",duration:75,cost:20,mods:{training:1.6,growth:.9,trainingGear:1.2},effects:["+60% trained crew from existing training facilities","+20% training Equipment use; −10% growth"]},
  "naval-recruitment":{title:"Emergency Naval Recruitment",group:"personnel",cat:"Military",duration:50,cost:35,mods:{training:1.9,growth:.8,trainingGear:1.5},effects:["+90% crew training for 50 seconds","+50% training Equipment use; −20% growth"]},
  "veteran-recall":{title:"Recall Naval Reserves",group:"personnel",cat:"Military",duration:50,cost:28,cooldown:140,mods:{industry:.92,growth:.92},effects:["Immediate crew transfer from civilian population at naval worlds","Costs Equipment; −8% industry and growth; 140s cooldown"]},
  "mining-surge":{title:"Strategic Mining Surge",group:"mining",cat:"Resources",duration:75,cost:18,mods:{mining:1.45,growth:.94},upkeep:{equipment:.006},effects:["+45% extraction of raw resources","Extra Equipment use; −6% population growth"]},
  "rich-deposits":{title:"Exploit Rich Deposits",group:"mining",cat:"Resources",duration:80,cost:15,upkeep:{equipment:.003},effects:["+60% primary-resource extraction","−20% extraction of other deposits"]},
  "component-drive":{title:"Component Production Drive",group:"manufacturing",cat:"Industry",duration:80,cost:18,mods:{components:1.7,equipment:.65},effects:["+70% Components; −35% Equipment","Additional Iron and Silicates consumed"]},
  "equipment-drive":{title:"Equipment Production Drive",group:"manufacturing",cat:"Industry",duration:80,cost:18,mods:{equipment:1.7,components:.65},effects:["+70% Equipment; −35% Components","Additional Iron, Titanium and Rare Earth consumed"]},
  "heavy-industry":{title:"Heavy Industry Mobilization",group:"manufacturing",cat:"Industry",duration:75,cost:30,mods:{components:1.3,equipment:1.3,growth:.9,commerce:.9},effects:["+30% Components and Equipment; additional raw-material demand","−10% population growth and tax income"]},
  "naval-construction":{title:"Naval Construction Priority",group:"shipbuilding",cat:"Military",duration:85,cost:25,mods:{navalBuild:1.5,civilBuild:.8},effects:["Existing military ship construction +50%; military freight priority","Civilian construction −20%; no new shipyard ordered"]},
  "merchant-marine":{title:"Merchant Marine Expansion",group:"shipbuilding",cat:"Infrastructure",duration:85,cost:22,mods:{merchantBuild:1.55,navalBuild:.85,tradeTraffic:1.3,freight:1.12},effects:["Civilian hull construction +55%; freight +12%; more commercial traffic","Military ship construction −15%"]},
  "emergency-freight":{title:"Emergency Freight Mobilization",group:"logistics",cat:"Infrastructure",duration:65,cost:24,mods:{freight:1.5,commerce:.9},upkeep:{helium:.007},effects:["Freight speed +50%; shortage shipments gain priority","Idle domestic couriers can haul supplies; Helium use; −10% income"]},
  "strategic-supply":{title:"Strategic Supply Priority",group:"logistics",cat:"Infrastructure",duration:75,cost:16,mods:{freight:1.2,tradeTraffic:.8},effects:["Near-complete and stalled projects gain freight priority; +20% freight speed","−20% commercial traffic; player priority always stays first"]},
  "frontier-supply":{title:"Frontier Supply Initiative",group:"logistics",cat:"Infrastructure",duration:85,cost:20,mods:{frontierGrowth:1.25,coreBuild:.9},upkeep:{helium:.004},effects:["Frontier freight priority; +25% frontier growth","Core construction −10%; consumes Helium"]},
  "scientific-mobilization":{title:"Scientific Mobilization",group:"research",cat:"Research",duration:85,cost:25,mods:{research:1.5,components:.9,equipment:.9},effects:["Research output +50% at existing research facilities","More Rare Earth and Equipment use; −10% manufacturing"]},
  "military-research":{title:"Military Research Priority",group:"research",cat:"Research",duration:80,cost:28,mods:{research:1.3},upkeep:{credits:.015},effects:["80% of research goes to weapons, carriers and propulsion","Civilian research slows; Rare Earth, Equipment and Credits consumed"]},
  "industrial-research":{title:"Industrial Research Priority",group:"research",cat:"Research",duration:85,cost:24,mods:{research:1.3},effects:["80% of research goes to extraction, automation and commerce","Military research slows; Rare Earth and Equipment consumed"]},
  "fleet-readiness":{title:"Raise Fleet Readiness",group:"readiness",cat:"Military",duration:70,cost:25,mods:{fleetSpeed:1.25,repair:1.5},upkeep:{equipment:.005,helium:.006},effects:["Fleet travel +25%; faster repair and service readiness","Military freight priority; consumes Equipment and Helium"]},
  "fortify-frontier":{title:"Fortify the Frontier",group:"readiness",cat:"Military",duration:80,cost:22,mods:{defense:1.2,responseRange:1.25},upkeep:{equipment:.004},effects:["Defensive readiness +20%; response range +25%","Existing frontier defense builds and military freight get priority; Equipment use"]},
  "war-economy":{title:"War Economy",group:"mobilization",cat:"Military",duration:80,cost:40,mods:{mining:1.2,equipment:1.25,training:1.3,navalBuild:1.3,growth:.75,commerce:.7,trainingGear:1.2},upkeep:{equipment:.004},effects:["+20% mining, +25% Equipment, +30% training and naval construction","−25% growth; −30% tax income; more Equipment and raw-material consumption"]},
  "trade-stimulus":{title:"Stimulate Interplanetary Trade",group:"commerce",cat:"Infrastructure",duration:85,cost:18,mods:{tradeTraffic:1.5,commerce:1.25},upkeep:{helium:.003},effects:["+50% commercial traffic; +25% tax income","More visible shipping exposure; consumes Helium"]},
  "export-surplus":{title:"Export Surplus Resources",group:"commerce",cat:"Resources",duration:85,cost:12,mods:{tradeTraffic:1.2},effects:["Trade ministry seeks paying foreign buyers for real surpluses","Export reserves −25%; goods leave on physical ships"]},
  "strategic-reserves":{title:"Build Strategic Reserves",group:"commerce",cat:"Resources",duration:90,cost:15,mods:{civilBuild:.9},effects:["Export reserves +60%; surplus cargo moves toward safer worlds","Civilian construction −10%; fewer exports"]},
  "commercial-outreach":{title:"Commercial Outreach",group:"diplomacy",cat:"Diplomacy",duration:85,cost:25,effects:["Better foreign request access; credit offer prices −10%","Improves commercial relations without overriding war or embargoes"]},
  "diplomatic-reassurance":{title:"Diplomatic Reassurance",group:"diplomacy",cat:"Diplomacy",duration:80,cost:20,effects:["Reduces recorded aggression and reassures neutral/friendly powers","No new declarations of war while active"]},
  "economic-pressure":{title:"Economic Pressure",group:"diplomacy",cat:"Diplomacy",duration:80,cost:30,mods:{commerce:.9},effects:["Embargo a hostile rival through existing diplomacy","−10% tax income; worsens relations with the target"]}
};
for(const resource of STX_RT_RAW)STX_POLICIES[`mining-${resource}`]={title:`Prioritize ${stxRTLabel(resource)} Extraction`,group:"mining",cat:"Resources",duration:80,cost:16,resource,upkeep:{equipment:.003},effects:[`+70% ${stxRTLabel(resource)} extraction; +90% on primary/secondary worlds`,"Other raw-resource extraction −15%; consumes Equipment"]};

function stxPolicyState(e){
  return e.stxPolicies||(e.stxPolicies={active:[],recent:[],cooldowns:{},nextAIAt:state.simTime+rand(30,55),nextActionAt:state.simTime});
}
function stxPolicyActive(owner){const e=empire(owner);return e?(stxPolicyState(e).active||[]).filter(p=>p.until>state.simTime&&STX_POLICIES[p.id]):[]}
function stxPolicyHas(owner,id){return stxPolicyActive(owner).some(p=>p.id===id)}
function stxPolicyValue(owner,key,p=null){
  let value=1;
  for(const active of stxPolicyActive(owner)){
    const n=STX_POLICIES[active.id].mods?.[key]??1,funded=p?.stxPolicyFunding?.[active.id]??1;
    value*=n>1?1+(n-1)*funded:n;
  }
  return clamp(value,.3,2.5);
}
function stxPolicyReserve(owner){return stxPolicyHas(owner,"strategic-reserves")?1.6:stxPolicyHas(owner,"export-surplus")?.75:1}
function stxPolicyFrontier(p){return p.pop/Math.max(.02,p.capacity)<.45||borderThreat(p)>.45}
function stxPolicyGrowth(p){
  let m=stxPolicyValue(p.owner,"frontierGrowth",p);
  if(!stxPolicyFrontier(p))m=1;
  if(stxPolicyHas(p.owner,"encourage-settlement")&&p.pop/p.capacity<.86)m*=1+(p.pop/p.capacity<.4?.65:.45)*(p.stxPolicyFunding?.["encourage-settlement"]??1);
  return m;
}
function stxPolicyPay(p,costs,dt){
  const e=empire(p.owner),entries=Object.entries(costs).filter(([,a])=>a>0),factor=entries.reduce((n,[r,a])=>Math.min(n,(r==="credits"?e.credits:p.stock[r]||0)/Math.max(1e-9,a*dt)),1);
  const paid=clamp(factor,0,1);
  for(const [r,a] of entries){if(r==="credits")e.credits=Math.max(0,e.credits-a*dt*paid);else p.stock[r]=Math.max(0,(p.stock[r]||0)-a*dt*paid)}
  return paid;
}
function stxPolicyContext(owner){
  const worlds=owned(owner),needs=Object.fromEntries(STX_RT_RESOURCES.map(r=>[r,0]));
  let crew=0,ships=0,stalled=0;
  for(const p of worlds)for(const d of stxSDDescriptors(p)){
    if(d.kind==="ship")ships++;
    for(const [r,a] of Object.entries(d.need)){
      const missing=Math.max(0,a-(d.q.stxSupply?.delivered?.[r]||0)-(p.stock[r]||0));
      if(r==="trained")crew+=missing;else if(r in needs)needs[r]+=missing;
    }
    if((d.q.stxSupply?.waitingSince??state.simTime)<state.simTime-12)stalled++;
  }
  for(const r of STX_RT_RESOURCES)needs[r]+=Math.max(0,worlds.length*stxRTReserve(r)-empireResource(owner,r))*.35;
  const raw=STX_RT_RAW.slice().sort((a,b)=>needs[b]-needs[a])[0];
  return {worlds,needs,raw,crew,ships,stalled,war:state.wars.some(w=>w.active&&(w.a===owner||w.b===owner)),spare:worlds.some(p=>p.pop/p.capacity<.65),migration:worlds.some(p=>p.pop/p.capacity>.8)&&worlds.some(p=>p.pop/p.capacity<.5)};
}
function stxPolicyPressureTarget(owner){return state.empires.filter(e=>e.id!==owner&&stxRTActiveEmpire(e.id)&&!empiresAtWar(owner,e.id)&&relation(owner,e.id)<.1&&!stxRTEmbargoed(owner,e.id)).sort((a,b)=>relation(owner,a.id)-relation(owner,b.id))[0]}
function stxPolicyEligible(owner,id,c=stxPolicyContext(owner)){
  const e=empire(owner),d=STX_POLICIES[id];if(!e||!d||!c.worlds.length||e.credits<d.cost||(stxPolicyState(e).cooldowns[id]||0)>state.simTime)return false;
  if((id==="war-economy"||id==="naval-recruitment")&&!c.war&&c.ships<=1)return false;
  if(d.resource&&!c.worlds.some(p=>p.infra.mine>0))return false;
  if(d.group==="mining")return c.worlds.some(p=>p.infra.mine>0);
  if(d.group==="manufacturing")return c.worlds.some(p=>p.infra.factory>0);
  if(d.group==="personnel")return c.worlds.some(p=>(id==="veteran-recall"?(p.infra.shipyard>0||p.infra.training>0)&&p.pop>.02&&(p.stock.equipment||0)>=2:p.infra.training>0));
  if(d.group==="research")return c.worlds.some(p=>p.infra.research>0);
  if(d.group==="shipbuilding")return c.worlds.some(p=>p.infra.shipyard>0);
  if(id==="war-economy"||id==="naval-recruitment")return c.war||c.ships>1;
  if(id==="frontier-resettlement")return c.migration;
  if(id==="encourage-settlement"||id==="population-expansion")return c.spare;
  if(id==="economic-pressure")return !!stxPolicyPressureTarget(owner);
  if(id==="fleet-readiness")return state.fleets.some(f=>f.owner===owner&&!f.destroyed);
  return true;
}
function stxPolicyScore(owner,id,c){
  const d=STX_POLICIES[id];let score=35;
  if(d.resource)score+=Math.min(95,c.needs[d.resource]*2);
  if(id==="mining-surge")score+=Math.min(65,STX_RT_RAW.reduce((n,r)=>n+c.needs[r],0)*.35);
  if(id==="component-drive")score+=Math.min(90,c.needs.components*2);
  if(id==="equipment-drive")score+=Math.min(90,c.needs.equipment*2);
  if(d.group==="personnel")score+=Math.min(95,c.crew*15000)+(c.war?20:0);
  if(id==="frontier-resettlement"&&c.migration)score+=50;
  if(id==="encourage-settlement"&&c.spare)score+=22;
  if(id==="naval-construction")score+=Math.min(70,c.ships*22);
  if(d.group==="logistics")score+=Math.min(65,c.stalled*25);
  if(id==="war-economy"&&c.war)score+=75;
  const recent=stxPolicyState(empire(owner)).recent.slice(-5);if(recent.includes(id))score-=35;
  if(stxPolicyHas(owner,id))score-=20;
  return score;
}
function stxPolicyActivate(owner,id){
  const e=empire(owner),d=STX_POLICIES[id];if(!stxPolicyEligible(owner,id)){if(owner===0)showToast("Policy unavailable: check treasury, facilities, or cooldown");return false}
  const s=stxPolicyState(e),refresh=stxPolicyHas(owner,id),target=id==="economic-pressure"?stxPolicyPressureTarget(owner):null;
  e.credits-=d.cost;s.active=s.active.filter(p=>STX_POLICIES[p.id]?.group!==d.group);
  s.active.push({id,startedAt:state.simTime,until:state.simTime+d.duration,targetId:target?.id});s.recent.push(id);s.recent=s.recent.slice(-12);
  if(d.cooldown)s.cooldowns[id]=state.simTime+d.cooldown;
  if(id==="veteran-recall")for(const p of owned(owner).filter(p=>(p.infra.shipyard||p.infra.training)&&p.pop>.02&&(p.stock.equipment||0)>=2).slice(0,3)){
    const crew=Math.min(.003,p.pop*.008,(p.stock.equipment||0)/800);p.pop-=crew;p.stock.trained=(p.stock.trained||0)+crew;p.stock.equipment-=crew*800;
  }
  if(!refresh&&id==="diplomatic-reassurance"){
    (e.stxAggressionHistory||[]).forEach(x=>x.weight*=.85);
    state.empires.filter(x=>x.id!==owner&&stxRTActiveEmpire(x.id)&&!empiresAtWar(owner,x.id)&&relation(owner,x.id)>-.15).forEach(x=>adjustRelation(owner,x.id,.06));
  }
  if(!refresh&&id==="commercial-outreach")state.empires.filter(x=>x.id!==owner&&stxRTActiveEmpire(x.id)&&!empiresAtWar(owner,x.id)&&!stxRTEmbargoed(owner,x.id)).forEach(x=>adjustRelation(owner,x.id,.035));
  if(target){stxGBImposeEmbargo(owner,target.id,"Economic pressure mandate");adjustRelation(owner,target.id,-.08)}
  for(const p of owned(owner))p.mandateGlow=1;
  stxPolicyRefreshFreight();stxPolicyRefreshOrders(owner);s.nextActionAt=state.simTime;
  if(id==="frontier-resettlement")stxPolicyMigrate(owner);
  if(id==="emergency-freight")stxPolicyReassignCouriers(owner);
  if(owner===0){galacticNews("IMMEDIATE MANDATE ACTIVE",`${d.title}: ${d.effects.join(". ")}. ${d.duration} simulation seconds; ${d.cost} Credits.`,"good");stxPolicyRenderActive()}
  return true;
}

let stxPolicyPlanet=null;
const STX_POLICY_modifier=modifier;
modifier=function(e,key){return STX_POLICY_modifier(e,key)*stxPolicyValue(e.id,key,stxPolicyPlanet?.owner===e.id?stxPolicyPlanet:null)};
const STX_POLICY_tickPlanet=tickPlanet;
tickPlanet=function(p,dt){
  if(!p||p.owner===null||dt<=0)return STX_POLICY_tickPlanet(p,dt);
  const active=stxPolicyActive(p.owner);p.stxPolicyFunding={};
  for(const a of active)p.stxPolicyFunding[a.id]=stxPolicyPay(p,STX_POLICIES[a.id].upkeep||{},dt);
  const yields=p.stxResourceYield,original=yields?{...yields}:null;
  if(yields)for(const r of STX_RT_RAW){
    let factor=1;
    for(const a of active){const d=STX_POLICIES[a.id];if(d.resource)factor*=r===d.resource?1+((p.stxResourcePrimary===r||p.stxResourceSecondary===r)?.9:.7)*p.stxPolicyFunding[a.id]:.85;
      if(a.id==="rich-deposits")factor*=r===p.stxResourcePrimary?1+.6*p.stxPolicyFunding[a.id]:.8;
    }
    yields[r]*=factor;
  }
  const previous=stxPolicyPlanet;stxPolicyPlanet=p;
  try{STX_POLICY_tickPlanet(p,dt)}finally{stxPolicyPlanet=previous;if(original)Object.assign(yields,original)}
  stxPolicyResearch(p,dt);
};

// Research facilities now advance technology steadily, with physical local
// Rare Earth and Equipment inputs. Policies change its rate and allocation.
function stxPolicyResearch(p,dt){
  if(!p.infra.research)return;
  const e=empire(p.owner),rate=stxPolicyValue(p.owner,"research",p),work=p.infra.research*Math.max(.1,p.efficiency||1)*rate;
  const paid=stxPolicyPay(p,{rare:work*.012,equipment:work*.007},dt);
  if(p.stock.rare<3)addOrder(p,"research","rare",14,2,"Research Rare Earth supply");
  if(p.stock.equipment<2)addOrder(p,"research","equipment",10,2,"Research Equipment supply");
  const military=["weapons","carriers","propulsion"],civil=["extraction","automation","commerce","medicine","orbital"];
  const share=stxPolicyHas(p.owner,"military-research")?.8:stxPolicyHas(p.owner,"industrial-research")?.2:.4;
  const output=work*.0003*dt*paid;p.stxResearchRate=dt?output/dt:0;
  military.forEach(k=>e.tech[k]=(e.tech[k]||0)+output*share/military.length);
  civil.forEach(k=>e.tech[k]=(e.tech[k]||0)+output*(1-share)/civil.length);
}
const STX_POLICY_advance=stxSDAdvance;
stxSDAdvance=function(d,dt,rate){
  const p=d.p,owner=p.owner,military=d.kind==="ship"&&["fleet","patrol"].includes(d.q.type);
  if(d.kind==="ship")rate*=stxPolicyValue(owner,military?"navalBuild":"merchantBuild",p);
  if(!military)rate*=stxPolicyValue(owner,"civilBuild",p);
  if(!stxPolicyFrontier(p))rate*=stxPolicyValue(owner,"coreBuild",p);
  if(d.kind==="local"&&d.q.type==="defense"&&stxPolicyFrontier(p)&&stxPolicyHas(owner,"fortify-frontier"))rate*=1.35;
  return STX_POLICY_advance(d,dt,rate);
};
const STX_POLICY_priority=stxSDPriority;
stxSDPriority=function(kind,q,p){
  let priority=STX_POLICY_priority(kind,q,p);if(!p||priority>=STX_PS_PRIORITY)return priority;
  const owner=p.owner,military=kind==="ship"&&["fleet","patrol"].includes(q.type)||kind==="local"&&q.type==="defense";
  if(military&&(stxPolicyHas(owner,"naval-construction")||stxPolicyHas(owner,"fleet-readiness")||stxPolicyHas(owner,"fortify-frontier")))priority+=18;
  if(stxPolicyHas(owner,"strategic-supply"))priority+=Math.min(16,(q.progress||0)*12+((q.stxSupply?.waitingSince??state.simTime)<state.simTime-12?8:0));
  if(stxPolicyHas(owner,"frontier-supply")&&stxPolicyFrontier(p))priority+=15;
  if(stxPolicyHas(owner,"emergency-freight")&&(q.stxSupply?.waitingSince!=null||p.shortages.size))priority+=12;
  return Math.min(80,priority);
};
function stxPolicyRefreshOrders(owner){
  for(const p of owned(owner))for(const d of stxSDDescriptors(p))for(const id of Object.values(d.q.stxSupply?.orderIds||{})){
    const order=p.orders.find(o=>o.id===id);if(order)order.priority=d.priority;
  }
}
function stxPolicyShipSpeed(s){
  const from=state.planets.find(p=>p.id===s.from),p=from?.owner===s.owner?from:null,military=["fleet","patrol"].includes(s.type);
  const freight=Object.keys(s.cargo||{}).some(r=>r!=="population");
  let factor=military?stxPolicyValue(s.owner,"fleetSpeed",p):freight?stxPolicyValue(s.owner,"freight",p):1;
  if(freight&&s.stxProjectId&&s.stxProjectId===empire(s.owner)?.stxPriorityProjectId&&stxPolicyHas(s.owner,"emergency-freight"))factor*=1.1;
  s.speed=s.speed/(s.stxPolicySpeedFactor||1)*factor;s.stxPolicySpeedFactor=factor;
}
function stxPolicyRefreshFreight(){state.ships.forEach(stxPolicyShipSpeed)}
const STX_POLICY_createShip=createShip;
createShip=function(...args){const s=STX_POLICY_createShip(...args);if(s)stxPolicyShipSpeed(s);return s};
const STX_POLICY_tickShips=tickShips;
tickShips=function(dt){stxPolicyRefreshFreight();return STX_POLICY_tickShips(dt)};
function stxPolicyMigrate(owner){
  if(state.ships.length>=260||state.ships.filter(s=>s.owner===owner&&s.stxPolicyMigration).length>=5)return false;
  const worlds=owned(owner).filter(p=>!p.underAttack),from=worlds.filter(p=>p.pop/p.capacity>.8).sort((a,b)=>b.pop/b.capacity-a.pop/a.capacity)[0];
  const to=worlds.filter(p=>p!==from&&p.pop/p.capacity<.55).sort((a,b)=>a.pop/a.capacity-b.pop/b.capacity)[0];
  if(!from||!to||(from.stock.helium||0)<1)return false;
  const incoming=state.ships.filter(s=>s.to===to.id).reduce((n,s)=>n+(s.cargo?.population||0),0),amount=Math.min(.012,from.pop*.025,Math.max(0,to.capacity*.75-to.pop-incoming));
  if(amount<=.0001)return false;
  const ship=createShip("migrant",from,to,owner,{cargo:{population:amount},stxPolicyMigration:true});
  if(!ship)return false;from.pop-=amount;from.stock.helium-=1;return true;
}
function stxPolicyReassignCouriers(owner){
  const ships=state.ships.filter(s=>s.owner===owner&&s.commercial&&!s.crossBorder&&!s.stxDeepMission&&!s.orderId&&["courier","liner"].includes(s.type)&&!Object.values(s.cargo||{}).some(n=>n>0)&&s.progress<.15);
  const orders=owned(owner).flatMap(p=>p.orders.filter(o=>o.status==="waiting").map(o=>({p,o}))).sort((a,b)=>b.o.priority-a.o.priority);
  for(const ship of ships.slice(0,2)){
    const from=state.planets.find(p=>p.id===ship.from&&p.owner===owner),entry=from&&orders.find(({p,o})=>p!==from&&o.resource!=="trained"&&(from.stock[o.resource]||0)>stxRTPlanetReserve(from,o.resource)+3);
    if(!entry)continue;const {p,o}=entry,amount=Math.min(30,o.amount-o.filled,(from.stock[o.resource]||0)-stxRTPlanetReserve(from,o.resource));if(amount<=0)continue;
    from.stock[o.resource]-=amount;ship.cargo={[o.resource]:amount};ship.to=p.id;ship.startX=ship.x;ship.startY=ship.y;ship.distance=dist(ship,p);ship.progress=0;ship.orderId=o.id;ship.stxProjectId=String(o.type).startsWith("stx-")?o.type.slice(4):null;ship.stxReassignedFreight=true;ship.commercial=false;o.status="in transit";
  }
}
function stxPolicyCommerce(owner){
  if(stxPolicyHas(owner,"export-surplus")){
    const resources=STX_RT_RESOURCES.slice().sort((a,b)=>stxRTEmpireAvailable(owner,b)-stxRTEmpireAvailable(owner,a));
    for(const resource of resources){
      if(stxRTEmpireAvailable(owner,resource)<30)continue;
      const buyer=state.empires.filter(e=>e.id!==owner&&stxRTActiveEmpire(e.id)&&!empiresAtWar(owner,e.id)&&!stxRTEmbargoed(owner,e.id)&&relation(owner,e.id)>-.3).find(e=>empireNeed(e.id).slice(0,2).some(n=>n.resource===resource));
      if(buyer&&stxRTExchange(owner,buyer.id,resource,18,{credits:Math.ceil(18*STX_RT_VALUES[resource])}))break;
    }
  }
  if(stxPolicyHas(owner,"strategic-reserves")){
    const worlds=owned(owner).filter(p=>!p.underAttack),to=worlds.slice().sort((a,b)=>borderThreat(a)-borderThreat(b))[0];
    if(!to)return;
    for(const r of STX_RT_RAW){const from=worlds.find(p=>p!==to&&borderThreat(p)>borderThreat(to)+.1&&(p.stock[r]||0)>stxRTPlanetReserve(p,r)+30);if(!from)continue;
      const ship=createShip(r==="helium"?"tanker":"freighter",from,to,owner,{cargo:{[r]:20},stxReserveTransfer:true});if(ship)from.stock[r]-=20;break;
    }
  }
}
function stxPolicyTick(dt){
  for(const e of state.empires){
    const s=stxPolicyState(e),expired=s.active.filter(p=>p.until<=state.simTime);s.active=s.active.filter(p=>p.until>state.simTime);
    if(expired.length){stxPolicyRefreshOrders(e.id);if(e.id===0)expired.forEach(p=>galacticNews("MANDATE EXPIRED",`${STX_POLICIES[p.id]?.title||"Policy"} ended; normal output and routing restored.`,"trade"))}
    if(!owned(e.id).length)continue;
    if(e.id!==0&&state.simTime>=s.nextAIAt){s.nextAIAt=state.simTime+rand(50,85);const c=stxPolicyContext(e.id),ranked=Object.keys(STX_POLICIES).filter(id=>stxPolicyEligible(e.id,id,c)).map(id=>({id,score:stxPolicyScore(e.id,id,c)+rand(-12,12)})).sort((a,b)=>b.score-a.score);if(ranked[0])stxPolicyActivate(e.id,ranked[0].id)}
    if(state.simTime>=s.nextActionAt){s.nextActionAt=state.simTime+12;stxPolicyRefreshOrders(e.id);if(stxPolicyHas(e.id,"frontier-resettlement"))stxPolicyMigrate(e.id);if(stxPolicyHas(e.id,"emergency-freight"))stxPolicyReassignCouriers(e.id);stxPolicyCommerce(e.id)}
    // Match the existing treasury tax baseline; commerce policies change its
    // real rate instead of granting a one-time bundle of free credits.
    e.credits=Math.max(0,e.credits+(owned(e.id).length*.018+totalPop(e.id)*.018)*dt*(stxPolicyValue(e.id,"commerce")-1));
    if(stxPolicyHas(e.id,"fleet-readiness"))for(const f of state.fleets.filter(f=>f.owner===e.id&&!f.destroyed&&f.location)){
      const p=state.planets.find(p=>p.id===f.location&&p.owner===e.id);if(!p)continue;
      const paid=stxPolicyPay(p,{equipment:.01,helium:.01},dt),missing=Math.max(0,(f.maxServiceStrength||f.strength)-f.strength);
      f.strength+=Math.min(missing,dt*.025*paid);if(f.serviceUntil>state.simTime)f.serviceUntil=Math.max(state.simTime,f.serviceUntil-dt*.5*paid);
    }
  }
}
const STX_POLICY_simulate=simulate;
simulate=function(dt){const result=STX_POLICY_simulate(dt);stxPolicyTick(dt);stxRTBroadcastTick();return result};
const STX_POLICY_declareWar=declareWar;
declareWar=function(a,b,...rest){if(stxPolicyHas(a,"diplomatic-reassurance")&&!empiresAtWar(a,b)){if(a===0)showToast("Diplomatic reassurance prevents new war declarations until it expires");return null}return STX_POLICY_declareWar(a,b,...rest)};
const STX_POLICY_tradeResource=tradeResource;
tradeResource=function(from,to){const deal=STX_POLICY_tradeResource(from,to);if(!deal||from.owner===to.owner)return deal;deal.amount=Math.min(deal.amount,Math.max(0,(from.stock[deal.resource]||0)-stxRTPlanetReserve(from,deal.resource)));return deal.amount>=1?deal:null};

function stxPolicyChoice(id){const d=STX_POLICIES[id];return{id:`policy-${id}`,policyId:id,cat:d.cat,title:d.title,desc:`Immediate policy · ${d.duration} simulation seconds · ${d.cost} Credits. Replaces the active ${d.group} policy.`,effects:d.effects,apply:()=>stxPolicyActivate(0,id)}}
function stxPolicyHand(){
  const count=playerWorlds().length>=8?5:4,c=stxPolicyContext(0),original=state.commandChoices||[],special=original.filter(x=>x.stxFleetOrderKind==="breakEmbargo"),forces=original.filter(x=>["invade","concentrate"].includes(x.stxFleetOrderKind));
  const ranked=Object.keys(STX_POLICIES).filter(id=>stxPolicyEligible(0,id,c)&&(!STX_POLICIES[id].resource||STX_POLICIES[id].resource===c.raw)).map(id=>({id,score:stxPolicyScore(0,id,c)+rand(-15,15)})).sort((a,b)=>b.score-a.score);
  const choices=forces.slice(0,2),groups=new Set();
  const policySlots=Math.min(count-choices.length,forces.length?count-forces.length:count-1);
  for(const {id} of ranked){const group=STX_POLICIES[id].group;if(groups.has(group))continue;choices.push(stxPolicyChoice(id));groups.add(group);if(choices.length>=forces.length+policySlots)break}
  const legacy=original.filter(x=>!x.policyId&&!x.stxFleetOrderKind&&!x.stxSpecialFifth);
  for(const x of legacy){if(choices.length>=count)break;if(!choices.some(c=>c.id===x.id))choices.push(x)}
  for(const {id} of ranked){if(choices.length>=count)break;if(!choices.some(c=>c.id===`policy-${id}`))choices.push(stxPolicyChoice(id))}
  // If the treasury cannot fund policies, keep normal executable construction
  // choices rather than padding with cosmetic or unaffordable cards.
  if(choices.length<count)for(const cmd of COMMANDS){if(choices.length>=count)break;const choice=stxDGChoiceFromCommand(cmd);if(choice&&!choices.some(c=>c.id===choice.id)&&!stxDGIsInvasionChoice(choice)&&stxDGCanExecuteChoice(choice))choices.push(choice)}
  const selected=new Set([...state.commandSelected].map(i=>original[i]?.id));
  state.commandChoices=[...choices.slice(0,count),...special];state.commandSelected=new Set(state.commandChoices.map((x,i)=>selected.has(x.id)?i:-1).filter(i=>i>=0));
}
const STX_POLICY_openCommandPhase=openCommandPhase;
openCommandPhase=function(){const result=STX_POLICY_openCommandPhase();if(!$("commandModal").hidden){stxPolicyHand();renderCommands()}return result};
const STX_POLICY_issueCommands=issueCommands;
issueCommands=function(){
  const selected=[...state.commandSelected],policies=selected.filter(i=>state.commandChoices[i]?.policyId);
  if(!policies.length)return STX_POLICY_issueCommands();
  if(policies.some(i=>!stxPolicyEligible(0,state.commandChoices[i].policyId))||policies.reduce((n,i)=>n+STX_POLICIES[state.commandChoices[i].policyId].cost,0)>empire(0).credits){showToast("Treasury or facilities cannot support the selected policies");return}
  for(const i of policies)stxPolicyActivate(0,state.commandChoices[i].policyId);
  // Legacy guarantee handlers must see only project commands: a policy is a
  // completed immediate action, never a reason to enqueue a fallback building.
  state.commandSelected=new Set(selected.filter(i=>!policies.includes(i)));
  STX_POLICY_issueCommands();state.commandSelected.clear();saveGame(false);stxPolicyRenderActive();
};
function stxPolicyActiveHTML(owner=0){
  const active=stxPolicyActive(owner);if(!active.length)return '<p class="card-copy">No temporary policies active.</p>';
  return active.map(a=>{const d=STX_POLICIES[a.id],funding=owned(owner).map(p=>p.stxPolicyFunding?.[a.id]??1),limited=funding.some(n=>n<.99);return `<div class="stx-policy-row"><strong>${stxRTEscape(d.title)}</strong> <span>${Math.ceil(a.until-state.simTime)}s remaining</span><small>${stxRTEscape(d.effects.join(" · "))}${limited?" · SUPPLY LIMITED: positive bonuses scale with available inputs":""}</small></div>`}).join("");
}
function stxPolicyRenderActive(){
  const grid=$("commandGrid");if(!grid)return;
  if(!$("stxActivePolicies"))grid.insertAdjacentHTML("beforebegin",'<section id="stxActivePolicies" aria-live="polite"><div class="card-kicker">ACTIVE GOVERNMENT POLICIES</div><div id="stxActivePolicyRows"></div></section>');
  const rows=$("stxActivePolicyRows");if(rows){const html=stxPolicyActiveHTML();if(rows.innerHTML!==html)rows.innerHTML=html}
}
const STX_POLICY_renderCommands=renderCommands;
renderCommands=function(){STX_POLICY_renderCommands();stxPolicyRenderActive()};
const STX_POLICY_renderPlanet=renderPlanet;
renderPlanet=function(){STX_POLICY_renderPlanet();const p=state.selected,body=$("planetBody");if(!p||p.owner!==0||!body)return;body.querySelector?.(".stx-planet-policies")?.remove();body.insertAdjacentHTML("beforeend",`<section class="stx-planet-policies"><div class="card-kicker">ACTIVE GOVERNMENT POLICIES</div>${stxPolicyActiveHTML()}<p class="card-copy">Components: ${((p.stxAllocationOutput?.componentRate||0)*60).toFixed(1)}/min · Equipment: ${((p.stxAllocationOutput?.equipmentRate||0)*60).toFixed(1)}/min<br>Crew training: ${fmtNum((p.stxCrewRate||0)*60)}/min · Research: ${((p.stxResearchRate||0)*60).toFixed(3)}/min<br>${STX_RT_RAW.map(r=>`${stxRTLabel(r)}: ${((p.stxRawProduction?.[r]||0)*60).toFixed(1)}/min`).join(" · ")}</p></section>`)};
const STX_POLICY_updateHud=updateHud;
updateHud=function(...args){const result=STX_POLICY_updateHud(...args);if(!$("commandModal")?.hidden)stxPolicyRenderActive();return result};
const stxPolicyStyle=document.createElement("style");stxPolicyStyle.textContent='#stxActivePolicies,.stx-planet-policies{margin:10px 0;padding:10px;border:1px solid #365575;border-radius:10px;background:#0a172a}.stx-policy-row{padding:7px 0;font-size:.68rem;border-bottom:1px solid #20364e}.stx-policy-row span{color:#ffce69}.stx-policy-row small{display:block;color:#9fb2d1;margin-top:4px;line-height:1.5}';document.head.appendChild(stxPolicyStyle);
globalThis.SpaceTyrantsPolicies={catalog:STX_POLICIES,activate:stxPolicyActivate,active:stxPolicyActive,context:stxPolicyContext,score:stxPolicyScore,value:stxPolicyValue,hand:stxPolicyHand,tick:stxPolicyTick};
