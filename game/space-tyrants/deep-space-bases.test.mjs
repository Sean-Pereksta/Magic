import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const moduleSource=readFileSync(new URL("./deep-space-bases.js",import.meta.url),"utf8");
const loaderSource=readFileSync(new URL("../space-tyrants.html",import.meta.url),"utf8");

function createElementRegistry(){
  const elements=new Map();
  const make=id=>{
    const node={
      id,hidden:false,children:[],dataset:{},style:{},className:"",innerHTML:"",textContent:"",value:"",checked:false,
      classList:{add(){},remove(){},toggle(){},contains(){return false}},
      appendChild(child){this.children.push(child);if(child.id)elements.set(child.id,child);return child},
      insertAdjacentHTML(){},addEventListener(){},removeEventListener(){},dispatchEvent(){return true},
      querySelector(){return null},querySelectorAll(){return[]},before(){},after(){},focus(){},
      setAttribute(){},getAttribute(){return null},getBoundingClientRect(){return{x:0,y:0,width:1200,height:800}}
    };
    return node;
  };
  const get=id=>{if(!elements.has(id))elements.set(id,make(id));return elements.get(id)};
  const body=make("body"),head=make("head");
  return{get,document:{body,head,createElement:tag=>make(tag),open(){},write(){},close(){}}};
}

function makePlanet(id,name,owner,x,y){
  return{
    id,name,owner,x,y,r:22,pop:6,underAttack:false,tradeVolume:18,orders:[],
    infra:{factory:3,shipyard:3},stock:{components:600,titanium:600,equipment:600,helium:600,rare:600},
    specialization:owner===0?"Industrial Center":"Trade Hub"
  };
}

function createHarness(){
  const {get,document}=createElementRegistry();
  const state={
    simTime:0,commandCycle:40,
    empires:[
      {id:0,name:"Solar Mandate",color:"#65d9ff",credits:5000,tech:{propulsion:2,weapons:2},foreignPolicy:{aggression:.5,caution:.5,commercialism:.5},relations:{1:0}},
      {id:1,name:"Orion Compact",color:"#ff6d85",credits:5000,tech:{propulsion:1,weapons:1},foreignPolicy:{aggression:.65,caution:.45,commercialism:.6},relations:{0:0},stxNextDeepBaseAt:Infinity}
    ],
    planets:[
      makePlanet("p0","Sol",0,900,900),
      makePlanet("p1","Vega",0,2500,1050),
      makePlanet("p2","Orion",1,3850,2100),
      makePlanet("p3","Drift",null,2350,2850)
    ],
    fleets:[{id:"f0",name:"First Fleet",owner:0,location:"p0",homePort:"p0",strength:42,readiness:"patrol",status:"Ready"}],
    ships:[],wars:[],battles:[],effects:[],camera:{x:2000,y:1600,zoom:1},stats:{visible:0},rivalDiplomacy:{agreements:[],pairs:{}}
  };
  let seed=123456;
  const random=()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296};
  const noop=()=>{};
  const drawContext=new Proxy({save:noop,restore:noop,beginPath:noop,closePath:noop,moveTo:noop,lineTo:noop,arc:noop,ellipse:noop,stroke:noop,fill:noop,fillRect:noop,strokeRect:noop,translate:noop,rotate:noop,setLineDash:noop,fillText:noop,setTransform:noop},{get(target,key){return key in target?target[key]:0},set(target,key,value){target[key]=value;return true}});
  const empire=id=>state.empires.find(e=>e.id===id);
  const owned=id=>state.planets.filter(p=>p.owner===id);
  const getWar=(a,b)=>state.wars.find(w=>w.active&&((w.a===a&&w.b===b)||(w.a===b&&w.b===a)))||null;
  const context={
    console,Math,Set,Map,Date,JSON,Object,Array,Number,String,Boolean,RegExp,
    state,WORLD:{w:5000,h:4000},RESOURCE_LABEL:{components:"components",titanium:"titanium",equipment:"equipment",helium:"helium",rare:"rare"},
    COMMANDS:[],STX_MILITARY_TYPES:new Set(["fleet","patrol"]),stxFCACommitted:new Set(),
    document,canvas:get("canvas"),ctx:drawContext,performance:{now:()=>state.simTime*1000},
    innerWidth:1200,innerHeight:800,devicePixelRatio:1,Event:class{constructor(type,options){this.type=type;this.options=options}},
    $:get,random,rand:(a,b)=>a+(b-a)*random(),clamp:(n,a,b)=>Math.max(a,Math.min(b,n)),
    roman:n=>["","I","II","III","IV"][n]||String(n),dist:(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),
    empire,owned,fleetRecord:id=>state.fleets.find(f=>f.id===id)||null,
    empireIndustry:id=>state.industryOverride?.[id]??owned(id).reduce((sum,p)=>sum+(p.infra.factory||0)+(p.infra.shipyard||0)*.8,0),
    empiresAtWar:(a,b)=>!!getWar(a,b),getWar,relation:(a,b)=>empire(a)?.relations?.[b]||0,
    adjustRelation:(a,b,delta)=>{empire(a).relations[b]=(empire(a).relations[b]||0)+delta},
    declareWar:(a,b,reason="test war")=>{const existing=getWar(a,b);if(existing)return existing;const war={id:`w${state.wars.length+1}`,a,b,aggressor:a,active:true,startedAt:state.simTime,reason};state.wars.push(war);return war},
    endWar:(war,reason)=>{war.active=false;war.endedReason=reason},
    canTrade:()=>false,
    addOrder:(planet,type,resource,amount,priority,label)=>{const order={id:`o${planet.orders.length+1}`,type,resource,amount,priority,label,status:"waiting"};planet.orders.push(order);return order},
    vesselName:type=>`${type} vessel`,arriveShip:(ship,planet)=>{ship.to=planet.id},fmtEta:n=>`${Math.max(0,Math.round(n))}s`,
    logEvent:noop,galacticNews:noop,showToast:noop,stxActivity:noop,updateHud:noop,renderPlanet:noop,renderTransmissions:noop,renderRivals:noop,updateBadges:noop,
    saveGame:()=>true,loadGame:()=>true,generateGalaxy:noop,simulate:dt=>{state.simTime+=dt},tickShips:noop,
    draw:noop,drawShips:noop,drawBattles:noop,drawCombatCraft:noop,battlePoint:(center,r,i,count,direction,time)=>{const angle=i/count*Math.PI*2+time*.2*direction;return{x:center.x+Math.cos(angle)*r,y:center.y+Math.sin(angle)*r}},
    visible:()=>true,worldToScreen:(x,y)=>({x,y}),screenToWorld:(x,y)=>({x,y}),stxFocusPoint:noop,
    stxFCAConcentrationTargets:()=>[],stxFCAPosition:f=>state.planets.find(p=>p.id===f.location)||null,stxFCADispatchFleet:()=>false
  };
  context.globalThis=context;
  vm.createContext(context);
  vm.runInContext(moduleSource,context,{filename:"deep-space-bases.js"});
  return{context,state,api:context.SpaceTyrantsDeepSpace};
}

function advanceConstruction(harness,base){
  const {context,state,api}=harness;
  for(let i=0;i<12&&!Object.keys(base.project.need).every(r=>(base.project.delivered[r]||0)>=base.project.need[r]);i++){
    state.simTime+=5;
    api.tick();
    context.tickShips(1e6);
  }
}

test("loader installs deep-space bases as the final runtime layer",()=>{
  const deep=loaderSource.indexOf("'./space-tyrants/deep-space-bases.js'");
  const mobile=loaderSource.indexOf("'./space-tyrants/mobile-layout.js'");
  assert.ok(deep>mobile);
  assert.equal(loaderSource.match(/deep-space-bases\.js/g)?.length,1);
});

test("site suggestions are automatic, named, bounded, and independent of planets",()=>{
  const {state,api}=createHarness();
  const sites=api.generateSites(state.planets[0],"military");
  assert.equal(sites.length,3);
  for(const site of sites){
    assert.match(site.name,/Base|Bastion|Citadel|Hold|Station/);
    assert.ok(site.region.length>3);
    assert.ok(site.x>=180&&site.x<=4820&&site.y>=180&&site.y<=3820);
    assert.ok(Math.hypot(site.x-state.planets[0].x,site.y-state.planets[0].y)>220);
  }
});

test("military bases unlock from the one-decimal Industry value shown in the HUD",()=>{
  const {state,api}=createHarness(),home=state.planets[0];
  state.industryOverride={0:4.94};
  assert.equal(api.displayedIndustry(0),4.9);
  assert.equal(api.canSponsor(home,"military"),false);
  assert.equal(api.canSponsor(home,"trade"),true);

  state.industryOverride[0]=4.96;
  assert.equal(api.displayedIndustry(0),5);
  assert.equal(api.canSponsor(home,"military"),true);
});

test("construction requires physical cargo arrivals before commissioning",()=>{
  const harness=createHarness();
  const {state,api}=harness,home=state.planets[0],site=api.generateSites(home,"logistics")[0];
  const base=api.queueBase(home,"logistics",site,true);
  assert.equal(base.status,"construction");
  assert.equal(base.sponsorPlanetId,home.id);
  assert.notEqual(base.x,home.x);
  state.simTime=.5;
  api.tick();
  assert.ok(state.ships.some(ship=>ship.stxDeepMission==="construction"&&ship.deepBaseId===base.id));
  assert.equal(Object.values(base.project.delivered).reduce((sum,value)=>sum+value,0),0);

  advanceConstruction(harness,base);
  assert.ok(Object.keys(base.project.need).every(resource=>base.project.delivered[resource]>=base.project.need[resource]));
  base.project.progress=.92;
  state.simTime+=1;
  api.tick();
  assert.equal(base.status,"construction");
  state.simTime+=12;
  api.tick();
  assert.equal(base.status,"operational");
  assert.ok(base.supplyReadiness>.7&&base.supplyReadiness<=.72);
});

test("fleet concentration docks at map coordinates and grants exactly two prepared cycles",()=>{
  const harness=createHarness();
  const {context,state,api}=harness,home=state.planets[0],fleet=state.fleets[0],site=api.generateSites(home,"military")[0];
  const base=api.queueBase(home,"military",site,true);
  base.status="operational";
  delete base.project;
  base.hp=base.maxHp;
  base.supplyReadiness=1;

  const targets=context.stxFCAConcentrationTargets();
  assert.ok(targets.some(target=>target.base?.id===base.id));
  assert.equal(api.dispatchFleet(fleet,{id:base.id,type:"deepBase",name:base.name,base,planet:home},"concentrate"),true);
  const transit=state.ships.find(ship=>ship.fleetId===fleet.id);
  assert.deepEqual([transit.targetX,transit.targetY],[base.x,base.y]);
  context.tickShips(1e6);
  assert.equal(fleet.deepSpaceBaseId,base.id);
  assert.ok(base.dockedFleetIds.includes(fleet.id));

  fleet.strength=20;
  state.simTime=fleet.serviceUntil+.5;
  api.tick();
  assert.equal(fleet.readiness,"base prepared");
  assert.ok(fleet.strength>20);
  assert.equal(fleet.maxServiceStrength,42);
  assert.equal(fleet.preparedUntil,state.simTime+state.commandCycle*2);
  assert.ok(fleet.preparedSpeed>0&&fleet.preparedCoordination>0);
});

test("base assaults leave wrecks and never transfer ownership",()=>{
  const {state,api}=createHarness();
  const enemy=state.planets[2],base=api.queueBase(enemy,"trade",api.generateSites(enemy,"trade")[0],true);
  base.status="operational";
  delete base.project;
  const owner=base.owner;
  api.destroyBase(base,0,"destroyed by test assault");
  assert.equal(base.status,"wreck");
  assert.equal(base.owner,owner);
  assert.equal(base.destroyedBy,0);
  assert.ok(base.wreckUntil>state.simTime);
});

test("commerce pressure can satisfy a limited objective and trigger peace",()=>{
  const harness=createHarness();
  const {context,state,api}=harness,enemy=state.planets[2],fleet=state.fleets[0];
  const base=api.queueBase(enemy,"trade",api.generateSites(enemy,"trade")[0],true);
  base.status="operational";
  delete base.project;
  base.supplyReadiness=.25;
  const war=context.declareWar(0,1,"break the trade network");
  war.stxLimitedWar=true;
  war.warGoal={type:"break-trade-network",targetId:base.id,label:`Break ${base.name} trade network`,limited:true,status:"active"};
  context.stxDSStartOperation(fleet,{id:base.id,type:"deepBase",name:base.name,base,planet:enemy},"raid");
  state.simTime+=1;
  api.tick();
  assert.equal(war.warGoal.status,"achieved");
  assert.ok(state.deepSpacePeaceOffers.some(offer=>offer.warId===war.id&&offer.status==="pending"));
});

test("intercepted project freight reopens its order and records the exact shortage",()=>{
  const harness=createHarness();
  const {context,state,api}=harness,enemy=state.planets[2],fleet=state.fleets[0];
  const base=api.queueBase(enemy,"trade",api.generateSites(enemy,"trade")[0],true);
  base.status="operational";
  delete base.project;
  const order={id:"critical-titanium",type:"construction",resource:"titanium",amount:30,filled:0,status:"in transit"};
  enemy.orders.push(order);
  const ship={id:"cargo-1",type:"freighter",owner:1,to:enemy.id,orderId:order.id,vesselName:"Orion Titanium Convoy",cargo:{titanium:30}};
  context.declareWar(0,1,"commerce war");
  const operation=context.stxDSStartOperation(fleet,{id:base.id,type:"deepBase",name:base.name,base,planet:enemy},"raid")&&state.deepSpaceOperations.at(-1);
  context.stxDSInterceptShip(ship,operation);
  assert.equal(order.status,"waiting");
  assert.equal(operation.lostCargo.titanium,30);
  assert.equal(base.raidLossLedger[0].cargo.titanium,30);
});

test("defending fleets break a blockade when they restore local control",()=>{
  const harness=createHarness();
  const {context,state,api}=harness,enemy=state.planets[2],fleet=state.fleets[0];
  state.fleets.push({id:"f1",name:"Orion Home Fleet",owner:1,location:enemy.id,homePort:enemy.id,strength:55,readiness:"patrol",status:"Ready"});
  api.ensureState(false);
  context.declareWar(0,1,"blockade war");
  context.stxDSStartOperation(fleet,{id:enemy.id,type:"planet",name:enemy.name,planet:enemy},"blockade");
  const operation=state.deepSpaceOperations.at(-1);
  state.simTime+=1;
  api.tick();
  assert.equal(operation.active,false);
  assert.equal(fleet.deepSpaceOperationId,null);
  assert.match(fleet.status,/Driven off|Returning/);
});

test("the existing fleet allocator sends planet blockades through deep-space transit",()=>{
  const {context,state}=createHarness();
  const fleet=state.fleets[0],enemy=state.planets[2],destination={key:`planet:${enemy.id}`,type:"planet",id:enemy.id,name:enemy.name,planet:enemy};
  assert.equal(context.stxFCADispatchFleet(fleet,destination,"blockade",{stxDeepMission:"blockade"}),true);
  const ship=state.ships.find(item=>item.fleetId===fleet.id);
  assert.equal(ship.stxDeepTransit,true);
  assert.equal(ship.stxDeepMission,"blockade");
  assert.equal(ship.stxFleetOrderKind,"blockade");
  assert.deepEqual([ship.targetX,ship.targetY],[enemy.x,enemy.y]);
  assert.ok(context.stxFCACommitted.has(fleet.id));
});

test("economic operations stand down when their war ends",()=>{
  const {context,state,api}=createHarness();
  const fleet=state.fleets[0],enemy=state.planets[2],war=context.declareWar(0,1,"temporary war");
  context.stxDSStartOperation(fleet,{id:enemy.id,type:"planet",name:enemy.name,planet:enemy},"blockade");
  const operation=state.deepSpaceOperations.at(-1);
  context.endWar(war,"test ceasefire");
  state.simTime+=1;
  api.tick();
  assert.equal(operation.active,false);
  assert.equal(fleet.deepSpaceOperationId,null);
  assert.match(fleet.status,/ceasefire|Returning/);
});

test("close-zoom fleet proxies honor the global visual budget",()=>{
  const {context,state,api}=createHarness();
  for(let i=1;i<35;i++)state.fleets.push({id:`bulk-${i}`,name:`Fleet ${i}`,owner:0,location:"p0",homePort:"p0",strength:70,readiness:"patrol",status:"Ready"});
  api.ensureState(false);
  let craft=0;
  context.drawCombatCraft=()=>{craft++};
  state.camera.zoom=1;
  context.draw();
  assert.ok(craft>0);
  assert.ok(craft<=200);
});
