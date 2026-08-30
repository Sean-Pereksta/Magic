import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source=readFileSync(new URL("./resource-trade-economy.js",import.meta.url),"utf8");

function domHarness(){
  const elements=new Map();
  const make=id=>({id,hidden:false,value:"components",innerHTML:"",textContent:"",children:[],dataset:{},classList:{add(){}},style:{setProperty(){}},appendChild(node){this.children.push(node);if(node.id)elements.set(node.id,node)},insertAdjacentHTML(_where,html){this.innerHTML+=html},querySelector(){return null},querySelectorAll(){return[]}});
  ["planetBody","transmissionList","messageCount"].forEach(id=>elements.set(id,make(id)));
  return{$:id=>elements.get(id)||null,document:{head:{appendChild(node){if(node.id)elements.set(node.id,node)}},createElement:tag=>make(tag)}};
}

function planet(id,owner,stock={}){return{id,name:`World ${id}`,owner,x:owner*900+Number(id.slice(1))*40,y:owner*300,underAttack:false,orders:[],infra:{factory:2,shipyard:1},stock:{components:80,equipment:60,iron:100,titanium:100,helium:100,rare:100,silicates:100,...stock},stxResourceYield:{iron:1,titanium:1,helium:1,rare:1,silicates:1},factoryEfficiency:1,buildQueue:[],mandateGlow:0}}

function createHarness(){
  const dom=domHarness(),state={simTime:20,running:true,empires:[],planets:[],ships:[],proposals:[],contracts:[],rivalDiplomacy:{agreements:[]}};
  state.empires=[0,1,2,3].map(id=>({id,name:["Aurelian Mandate","Aurelian League","Orion Compact","Khepri Directorate"][id],doctrine:id===1?"industry":id===2?"commerce":"expansion",credits:1200,relations:{},commercialAccess:{}}));
  state.planets=[planet("p0",0,{components:15,equipment:180,titanium:180,rare:180}),planet("p1",1,{components:230,equipment:18,titanium:15}),planet("p2",2,{components:210,equipment:20,rare:15}),planet("p3",3,{components:190,equipment:18,silicates:12})];
  let seed=91;const random=()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296},clamp=(n,a,b)=>Math.max(a,Math.min(b,n)),empire=id=>state.empires.find(e=>e.id===id),owned=id=>state.planets.filter(p=>p.owner===id),empireResource=(id,r)=>owned(id).reduce((n,p)=>n+(p.stock[r]||0),0),relation=(a,b)=>empire(a)?.relations?.[b]||0;
  const context={console,Math,Set,Map,Date,JSON,Object,Array,Number,String,Boolean,RegExp,state,document:dom.document,$:dom.$,RESOURCE_LABEL:{},RESOURCES:["iron","titanium","rare","silicates","helium"],random,rand:(a,b)=>a+(b-a)*random(),clamp,empire,owned,empireResource,relation,adjustRelation:(a,b,v)=>{empire(a).relations[b]=(empire(a).relations[b]||0)+v;empire(b).relations[a]=empire(a).relations[b]},getWar:()=>null,empiresAtWar:()=>false,empireNeed:id=>["components","equipment","iron","titanium","helium","rare","silicates"].map(resource=>({resource,amount:empireResource(id,resource),orders:owned(id).reduce((n,p)=>n+p.orders.filter(o=>o.resource===resource).length,0)})).sort((a,b)=>(b.orders*45-b.amount)-(a.orders*45-a.amount)),generateGalaxy(){},loadGame:()=>true,saveGame:()=>true,addOrder(){},logEvent(){},showToast(){},galacticNews(){},renderPlanet(){},renderTransmissions(){},updateBadges(){},updateHud(){},respondProposal(id,action){const p=state.proposals.find(x=>x.id===id&&x.status==="pending");if(!p)return;if(action==="decline"){p.status="declined";context.adjustRelation(0,p.from,-.035)}},fmtEta:n=>`${Math.round(n)}s`,vesselName:type=>`${type} vessel`,stxRDAddCooperation(){},tickBuildQueue(){},commerceTick(){},tickPlanet(p,dt){for(const r of ["iron","titanium","rare","silicates","helium"])p.stock[r]+=10*(p.stxResourceYield[r]||1)*dt;context.stxRTProduceFactory(p,dt)},createShip(type,from,to,owner,extra={}){const ship={id:`s${state.ships.length}`,type,from:from.id,to:to.id,owner,...extra};state.ships.push(ship);return ship}};
  context.globalThis=context;vm.createContext(context);vm.runInContext(source,context,{filename:"resource-trade-economy.js"});return{state,api:context.SpaceTyrantsResourceTrade,context};
}

test("economic allocation creates real mining and manufacturing tradeoffs",()=>{
  const {state,api,context}=createHarness(),p=state.planets[0];
  const run=focus=>{p.stock={components:20,equipment:20,iron:20,titanium:20,helium:20,rare:20,silicates:20};api.setFocus(p,focus);context.tickPlanet(p,1);return{iron:p.stock.iron-20,components:p.stock.components-20,equipment:p.stock.equipment-20}};
  const balanced=run("balanced"),mining=run("mining"),components=run("components"),equipment=run("equipment");
  assert.ok(mining.iron>balanced.iron);
  assert.ok(components.components>balanced.components);
  assert.ok(components.equipment<balanced.equipment);
  assert.ok(equipment.equipment>balanced.equipment);
  assert.ok(equipment.components<balanced.components);
});

test("requested trade creates several stockpile-backed offers that remain independently acceptable",()=>{
  const {state,api}=createHarness(),request=api.requestTrade("components",50);
  assert.equal(request.offerIds.length,0);
  state.simTime+=31;api.broadcastTick();
  assert.equal(request.offerIds.length,3);
  const offers=state.resourceTradeEconomy.offers.filter(o=>o.requestId===request.id);
  assert.ok(offers.every(o=>o.amount<=api.available(o.from,"components")));

  assert.equal(api.acceptOffer(offers[0].id),true);
  assert.equal(offers[0].status,"accepted");
  assert.ok(offers.slice(1).some(o=>o.status==="pending"));
  assert.ok(state.ships.some(s=>s.tradeLeg==="delivery"&&s.cargo.components>0));

  for(const offer of offers.slice(1))if(offer.status==="pending")api.acceptOffer(offer.id);
  assert.equal(request.status,"fulfilled");
  assert.ok(request.accepted>=request.desired);
  assert.ok(offers.every(o=>o.status!=="pending"));
});

test("declining a routine requested-trade offer has no relationship penalty",()=>{
  const {state,api}=createHarness(),request=api.requestTrade("components",50);state.simTime+=31;api.broadcastTick();const offer=state.resourceTradeEconomy.offers.find(o=>o.requestId===request.id),before=state.empires[0].relations[offer.from]||0;
  assert.equal(api.declineOffer(offer.id),true);
  assert.equal(offer.status,"declined");
  assert.equal(state.empires[0].relations[offer.from]||0,before);
});

test("declining a normal incoming resource proposal is also penalty-free",()=>{
  const {state,context}=createHarness();state.proposals.push({id:"legacy-trade",kind:"trade",from:1,to:0,status:"pending"});const before=state.empires[0].relations[1]||0;
  context.respondProposal("legacy-trade","decline");
  assert.equal(state.proposals[0].status,"declined");
  assert.equal(state.empires[0].relations[1]||0,before);
  assert.equal(state.proposals[0].rdMemoryRecorded,true);
});

test("all ship classes require the four strategic shipbuilding inputs",()=>{
  const {api}=createHarness(),queue={type:"fleet",need:{components:38,trained:.009}};
  api.diversifyShipNeed(queue);
  for(const resource of ["components","equipment","titanium","helium"])assert.ok(queue.need[resource]>0);
});
