import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source=readFileSync(new URL("./galactic-balance.js",import.meta.url),"utf8");

function createDom(){
  const elements=new Map();
  const make=id=>({id,hidden:id==="commandModal",innerHTML:"",textContent:"",children:[],dataset:{},classList:{add(){}},style:{setProperty(){}},appendChild(node){this.children.push(node);if(node.id)elements.set(node.id,node)},insertAdjacentHTML(_where,html){this.innerHTML+=html},querySelector(){return null},querySelectorAll(){return[]}});
  for(const id of ["commandModal","commandGrid","transmissionList","fleetGrid","rivalGrid","warSummary","messageCount","rivalsModal"])elements.set(id,make(id));
  return{$:id=>elements.get(id)||null,document:{head:{appendChild(node){if(node.id)elements.set(node.id,node)}},createElement:tag=>make(tag)}};
}
function makePlanet(id,owner,x,industry=2){return{id,name:`World ${id}`,owner,x,y:owner*240,pop:.5,underAttack:false,orders:[],infra:{factory:industry,shipyard:1,defense:1},stock:{components:180,equipment:120,iron:160,titanium:160,helium:160,rare:160,silicates:160},quality:{iron:3,titanium:3,helium:3,rare:3,silicates:3},ownershipHistory:[{owner,time:0}],localProject:null}}

function createHarness(){
  const dom=createDom(),state={simTime:200,running:true,commandChoices:[],commandSelected:new Set(),empires:[],planets:[],fleets:[],ships:[],battles:[],wars:[],contracts:[],proposals:[],rivalDiplomacy:{agreements:[],requests:[]},deepSpaceBases:[],deepSpaceOperations:[],camera:{zoom:1},galacticBalance:null};
  state.empires=[0,1,2,3,4].map(id=>({id,name:["Aurelian Mandate","Aurelian League","Orion Compact","Khepri Directorate","Sable Dominion"][id],doctrine:id===4?"military":"balanced",credits:500,relations:{},commercialAccess:{},tech:{weapons:1,propulsion:1},foreignPolicy:{aggression:id===4?.8:.45,caution:.5,commercialism:.5},directives:[],initiativeProjects:[]}));
  state.planets=[makePlanet("p0",0,0,3),makePlanet("p1",1,700,3),makePlanet("p2",2,1400,2),makePlanet("p3",3,2100,2),makePlanet("p4",4,2800,3),makePlanet("n1",null,3500,0),makePlanet("n2",null,3900,0),makePlanet("n3",null,4300,0),makePlanet("n4",null,4700,0),makePlanet("n5",null,5100,0),makePlanet("n6",null,5500,0),makePlanet("n7",null,5900,0),makePlanet("n8",null,6300,0)];
  state.fleets=[{id:"f0",owner:0,name:"First Mandate Fleet",admiral:{name:"Leona Arden",trait:"Tactician"},location:"p0",homePort:"p0",strength:110,veterans:.2,battles:0,victories:0,destroyed:false},{id:"f1",owner:1,name:"League Fleet",admiral:{name:"Ari Vale",trait:"Logistician"},location:"p1",homePort:"p1",strength:70,veterans:.1,battles:0,victories:0,destroyed:false},{id:"f2",owner:2,name:"Compact Fleet",admiral:{name:"Ren Sol",trait:"Veteran"},location:"p2",homePort:"p2",strength:45,veterans:.3,battles:0,victories:0,destroyed:false}];
  let seed=44;const random=()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296},clamp=(n,a,b)=>Math.max(a,Math.min(b,n)),empire=id=>state.empires.find(e=>e.id===id),owned=id=>state.planets.filter(p=>p.owner===id),fleetRecord=id=>state.fleets.find(f=>f.id===id),relation=(a,b)=>empire(a)?.relations?.[b]||0,getWar=(a,b)=>state.wars.find(w=>w.active&&((w.a===a&&w.b===b)||(w.a===b&&w.b===a)))||null,frontierDistance=(a,b)=>{if(!owned(a).length||!owned(b).length)return Infinity;return Math.min(...owned(a).flatMap(x=>owned(b).map(y=>Math.hypot(x.x-y.x,x.y-y.y))))},empireFleet=id=>state.fleets.filter(f=>f.owner===id&&!f.destroyed).reduce((n,f)=>n+f.strength,0),empireIndustry=id=>owned(id).reduce((n,p)=>n+p.infra.factory+p.infra.shipyard*.8,0),totalPop=id=>owned(id).reduce((n,p)=>n+p.pop,0);
  const pairs={};const stxRDPair=(a,b)=>pairs[a<b?`${a}:${b}`:`${b}:${a}`]||(pairs[a<b?`${a}:${b}`:`${b}:${a}`]={a:Math.min(a,b),b:Math.max(a,b),tension:20,goodwill:10,grievances:[],cooperation:[]});
  const context={console,Math,Set,Map,Date,JSON,Object,Array,Number,String,Boolean,RegExp,state,document:dom.document,$:dom.$,random,rand:(a,b)=>a+(b-a)*random(),clamp,empire,owned,fleetRecord,relation,adjustRelation:(a,b,v)=>{empire(a).relations[b]=clamp(relation(a,b)+v,-1,1);empire(b).relations[a]=empire(a).relations[b]},getWar,frontierDistance,empireFleet,empireIndustry,totalPop,dist:(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),visible:()=>true,worldToScreen:(x,y)=>({x,y}),performance:{now:()=>state.simTime*1000},ctx:new Proxy({save(){},restore(){},beginPath(){},closePath(){},moveTo(){},lineTo(){},arc(){},stroke(){},fill(){},translate(){},rotate(){},setLineDash(){},fillText(){}},{get:(t,k)=>k in t?t[k]:0,set:(t,k,v)=>(t[k]=v,true)}),drawCombatCraft(){},RESOURCE_LABEL:{},playerWorlds:()=>owned(0),empireNeed:id=>["components","equipment","iron","titanium","helium","rare","silicates"].map(resource=>({resource,amount:owned(id).reduce((n,p)=>n+(p.stock[resource]||0),0),orders:0})),stxRDPair,stxRDAddAgreement(kind,by,withId,duration,extra={}){const a={id:`a${state.rivalDiplomacy.agreements.length}`,kind,by,with:withId,active:true,createdAt:state.simTime,expiresAt:state.simTime+duration,...extra};state.rivalDiplomacy.agreements.push(a);return a},stxRDAddGrievance(){},stxRTLabel:r=>r,stxRTEmpireAvailable:(id,r)=>owned(id).reduce((n,p)=>n+Math.max(0,(p.stock[r]||0)-20),0),stxRTSource:(id,r)=>owned(id).find(p=>(p.stock[r]||0)>20),stxRTDestination:(id)=>owned(id)[0],stxRTDispatchCargo(){return{}},generateGalaxy(){},loadGame:()=>true,saveGame:()=>true,startLocalProject:()=>false,addDirective(){},setModifier(){},stxBestFleetYard:id=>owned(id).find(p=>p.infra.shipyard),stxQueueFleetCommission:()=>true,declareWar(a,b,reason){if(getWar(a,b))return null;const w={id:`w${state.wars.length}`,a,b,active:true,reason,conquests:{[a]:0,[b]:0}};state.wars.push(w);return w},endWar(w){w.active=false},renderTransmissions(){},renderRivals(){},updateBadges(){},updateHud(){},renderFleetRegistry(){},switchHubTab(){},logEvent(){},showToast(){},galacticNews(){},fmtEta:n=>`${Math.round(n)}s`,resolveBattle(){},retireFleet(f){f.destroyed=true},deployFleet(){return{}},simulate(){},drawTradeCorridors(){},draw(){},openCommandPhase(){state.commandChoices=[0,1,2,3].map(i=>({id:`c${i}`,cat:"Industry",title:`Command ${i}`,desc:"",effects:[]}));state.commandSelected.clear();dom.$("commandModal").hidden=false},renderCommands(){dom.$("commandGrid").children=state.commandChoices.map(()=>dom.document.createElement("button"))}};
  context.globalThis=context;vm.createContext(context);vm.runInContext(source,context,{filename:"galactic-balance.js"});return{state,api:context.SpaceTyrantsGalacticBalance,context,dom};
}

test("power plus aggression is much more threatening than peaceful strength alone",()=>{
  const {api}=createHarness(),peaceful=api.threat(2,0);
  api.recordAggression(0,"war-declaration","Declared war on a weaker state",4,3);
  api.recordAggression(0,"conquest","Conquered a frontier world",2.2,3);
  api.recordAggression(0,"war-declaration","Declared another rapid war",4,4);
  const aggressive=api.threat(2,0);
  assert.ok(aggressive.score>peaceful.score+25);
  assert.ok(["Strategic Threat","Existential Threat"].includes(aggressive.level));
});

test("rival military targets scale from visible worlds, industry, population, and threats",()=>{
  const {state,api}=createHarness(),before=api.militaryTarget(1);
  state.planets.push(makePlanet("p5",1,760,4),makePlanet("p6",1,820,4),makePlanet("p7",1,880,4));
  const after=api.militaryTarget(1);
  assert.ok(after>before);
  assert.ok(after>state.fleets.find(f=>f.owner===1).strength);
});

test("embargoes use enforcing fleet strength and add the special fifth command",()=>{
  const {state,api,context,dom}=createHarness(),embargo=api.imposeEmbargo(1,0,"test pressure");
  assert.ok(embargo);
  assert.ok(embargo.strength>0);
  assert.deepEqual(embargo.fleetIds,["f1"]);
  assert.equal(api.embargoesAgainst(0).length,1);
  context.openCommandPhase();
  assert.equal(state.commandChoices.length,5);
  assert.equal(state.commandChoices[4].title,"BREAK EMBARGO");
  assert.equal(dom.$("commandModal").hidden,false);
});

test("AI empires reroute, rearm, mobilize, and can fight to break an embargo",()=>{
  const {state,api}=createHarness();state.fleets.find(f=>f.owner===1).strength=150;state.empires[1].foreignPolicy.aggression=.9;const embargo=api.imposeEmbargo(0,1,"coercive test embargo");
  api.respondToEmbargo(true);
  assert.ok(state.empires[1].stxEmergencyRearmUntil>state.simTime);
  assert.ok(state.rivalDiplomacy.agreements.some(a=>a.kind==="trade-agreement"&&a.by===1&&a.embargoDiversion===embargo.id));
  assert.ok(state.wars.some(w=>w.active&&w.a===1&&w.b===0&&w.warGoal?.type==="break-embargo"));
});

test("each empire can have only one visible, named flagship",()=>{
  const {state,api}=createHarness(),fleet=state.fleets[0],before=fleet.strength;
  assert.equal(api.designateFlagship(0,fleet.id),true);
  assert.ok(fleet.flagshipName);
  assert.ok(fleet.strength>before);
  assert.equal(api.flagship(0).id,fleet.id);
  state.fleets.push({...fleet,id:"f9",name:"Second Mandate Fleet",isFlagship:false,flagshipName:null});
  assert.equal(api.designateFlagship(0,"f9"),false);
});

test("anti-hegemony coalitions create real mutual-defense ties",()=>{
  const {state,api}=createHarness(),coalition=api.formCoalition(0,[1,2,3],0);
  assert.equal(coalition.status,"active");
  assert.deepEqual(Array.from(coalition.members),[1,2,3]);
  const pacts=state.rivalDiplomacy.agreements.filter(a=>a.kind==="mutual-defense"&&a.coalitionId===coalition.id);
  assert.equal(pacts.length,3);
});

test("defeated factions lose every ghost-state system and are removed from active play",()=>{
  const {state,api}=createHarness();
  state.ships.push({id:"enemy-ship",owner:1});state.contracts.push({id:"contract",a:1,b:2,active:true});state.proposals.push({id:"proposal",from:1,to:0,status:"pending"});state.rivalDiplomacy.requests.push({id:"request",from:1,to:0,status:"pending"});const embargo=api.imposeEmbargo(1,0,"last pressure");state.planets.find(p=>p.owner===1).owner=0;
  assert.equal(api.eliminateEmpire(1,0),true);
  assert.equal(state.empires[1].stxEliminated,true);
  assert.equal(state.ships.some(s=>s.owner===1),false);
  assert.equal(state.fleets.find(f=>f.owner===1).destroyed,true);
  assert.equal(state.contracts[0].active,false);
  assert.equal(state.proposals[0].status,"cancelled");
  assert.equal(state.rivalDiplomacy.requests[0].status,"cancelled");
  assert.equal(embargo.active,false);
});

test("stalled smaller rivals receive expansion catch-up without free planets",()=>{
  const {state,api}=createHarness(),rival=state.empires[2];rival.lastExpansion=0;
  state.planets.push(
    makePlanet("p8",1,740,2),makePlanet("p9",1,780,2),makePlanet("p10",1,820,2),
    makePlanet("p11",3,2140,2),makePlanet("p12",3,2180,2),makePlanet("p13",3,2220,2),
    makePlanet("p14",4,2840,2),makePlanet("p15",4,2880,2),makePlanet("p16",4,2920,2)
  );
  const base={maxWorlds:4,cooldown:120,intent:.25,reach:2000},result=api.expansionCatchup(rival,base,1);
  assert.ok(result.maxWorlds>base.maxWorlds);
  assert.ok(result.cooldown<base.cooldown);
  assert.ok(result.intent>base.intent);
  assert.equal(state.planets.filter(p=>p.owner===2).length,1);
});
