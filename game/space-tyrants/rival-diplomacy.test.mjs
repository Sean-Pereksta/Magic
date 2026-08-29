import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const moduleSource=readFileSync(new URL("./rival-diplomacy.js",import.meta.url),"utf8");

function createDom(){
  const elements=new Map(),inserted=[];
  const make=()=>({id:"",className:"",innerHTML:"",textContent:"",listeners:{},querySelector(){return null},querySelectorAll(){return[]},insertAdjacentHTML(){},before(){},after(){},addEventListener(type,handler){this.listeners[type]=handler}});
  const actions={before(node){inserted.push(node);if(node.id)elements.set(node.id,node)}};
  elements.set("newGameBtn",{id:"newGameBtn",parentElement:actions});
  const head={appendChild(node){if(node.id)elements.set(node.id,node)}};
  return{inserted,$:id=>elements.get(id)||null,document:{head,createElement:make}};
}

function createHarness(){
  const dom=createDom(),state={running:false,simTime:0,enemyExpansionDifficulty:"standard",empires:[],planets:[],ships:[],fleets:[],wars:[],proposals:[],militaryRequests:[]};
  const empireData=[
    ["Aurelian Mandate","industry",.44],
    ["Karthos Directorate","expansion",.58],
    ["Veyran Compact","commerce",.42],
    ["Nyxian Synod","fortress",.36],
    ["Orion Freeholds","industry",.5],
    ["Sable Dominion","military",.72],
    ["Helion Ascendancy","expansion",.6]
  ];
  state.empires=empireData.map(([name,doctrine,aggression],id)=>({id,name,doctrine,aggression,relations:{},credits:200,tech:{},commercialAccess:{},directives:[],initiativeProjects:[]}));
  state.planets=state.empires.map((e,id)=>({id:`p${id}`,name:`World ${id}`,owner:id,x:id*300,y:id*170,pop:.2,capacity:.4,garrison:20,underAttack:false,infra:{factory:1,shipyard:1,defense:1},orbitals:{base:0},quality:{iron:3,titanium:3},orders:[]}));
  let seed=12345;const random=()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296},rand=(a,b)=>a+(b-a)*random(),clamp=(value,min,max)=>Math.max(min,Math.min(max,value));
  const empire=id=>state.empires.find(e=>e.id===id),owned=id=>state.planets.filter(p=>p.owner===id),relation=(a,b)=>empire(a)?.relations?.[b]??0,noop=()=>{};
  const context={
    console,Math,Set,Map,Date,JSON,Object,Array,Number,String,Boolean,RegExp,state,document:dom.document,$:dom.$,random,rand,clamp,empire,owned,relation,
    dist:(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),empireFleet:()=>20,empireIndustry:()=>3,frontierDistance:()=>600,getWar:()=>null,adjustRelation:noop,
    generateGalaxy:noop,loadGame:()=>true,saveGame:()=>true,startExpansionProject:()=>true,attemptExpansion:noop,declareWar:()=>null,endWar:noop,respondProposal:noop,diplomacyTick:noop,arriveShip:noop,
    renderTransmissions:noop,updateBadges:noop,renderRivals:noop,renderEvents:noop,logEvent:noop,galacticNews:noop,showToast:noop,updateHud:noop,fmtEta:n=>`${n}s`,fleetRecord:()=>null,
    deployFleet:noop,createShip:()=>null,borderThreat:()=>0,startLocalProject:()=>false,queueOrbitalProject:()=>false,addDirective:noop,hasDirective:()=>null,empiresAtWar:()=>false
  };
  context.globalThis=context;vm.createContext(context);vm.runInContext(moduleSource,context,{filename:"rival-diplomacy.js"});return{api:context.SpaceTyrantsRivalDiplomacy,state,dom};
}

test("new-game difficulty selector offers four rival-expansion settings",()=>{
  const {dom}=createHarness(),selector=dom.inserted.find(node=>node.id==="stxRDDifficulty");
  assert.ok(selector);
  for(const label of ["Relaxed","Standard","Hard","Relentless"])assert.match(selector.innerHTML,new RegExp(`>${label}<`));
  assert.match(selector.innerHTML,/enemy colonization pace, reach, and territorial ceiling/i);
});

test("higher difficulties monotonically increase rival expansion pressure",()=>{
  const {api,state}=createHarness(),rival=state.empires[1],results=[];
  for(const id of ["relaxed","standard","hard","relentless"]){
    api.setDifficulty(id);rival.foreignPolicy=api.buildPolicy(rival,false);results.push({policy:rival.foreignPolicy.expansionism,...api.expansionRules(rival,3)});
  }
  for(let i=1;i<results.length;i++){
    assert.ok(results[i].policy>results[i-1].policy);
    assert.ok(results[i].maxWorlds>results[i-1].maxWorlds);
    assert.ok(results[i].cooldown<results[i-1].cooldown);
    assert.ok(results[i].reach>results[i-1].reach);
  }
});

test("campaign difficulty persists with rival diplomacy state",()=>{
  const {api,state}=createHarness();
  api.setDifficulty("hard");api.ensureState(true);
  assert.equal(state.enemyExpansionDifficulty,"hard");
  assert.equal(state.rivalDiplomacy.difficulty,"hard");
  assert.equal(state.empires[0].rivalDiplomacy.difficulty,"hard");

  state.enemyExpansionDifficulty="relaxed";
  api.ensureState(false);
  assert.equal(state.enemyExpansionDifficulty,"hard");
});
