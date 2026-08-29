import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const moduleSource=readFileSync(new URL("./war-campaign-continuity.js",import.meta.url),"utf8");
const loaderSource=readFileSync(new URL("../space-tyrants.html",import.meta.url),"utf8");

function createHarness(){
  const state={
    simTime:0,ships:[],fleets:[],battles:[],wars:[],commandChoices:[],commandSelected:new Set(),
    planets:[
      {id:"p0",name:"Aurelian Prime",owner:0,x:0,y:0,underAttack:false},
      {id:"p1",name:"Rival Home",owner:1,x:100,y:0,underAttack:false}
    ],
    empires:[
      {id:0,name:"Aurelian Mandate",invasionPlans:[]},
      {id:1,name:"Rival Dominion",invasionPlans:[]}
    ]
  };
  let coreResolveCalls=0,coreArrivalCalls=0,renderCalls=0;
  const empire=id=>state.empires.find(e=>e.id===id);
  const owned=id=>state.planets.filter(p=>p.owner===id);
  const getWar=(a,b)=>state.wars.find(w=>w.active&&((w.a===a&&w.b===b)||(w.a===b&&w.b===a)))||null;
  const COMMANDS=[
    {id:"industry",cat:"Industry",title:"Expand Factories",desc:"Invest in industrial capacity",score:()=>40},
    {id:"research",cat:"Research",title:"Fund Research",desc:"Advance science",score:()=>36},
    {id:"trade",cat:"Trade",title:"Open Trade Routes",desc:"Build commerce",score:()=>34},
    {id:"fortify",cat:"Military",title:"Fortify Border",desc:"Improve defenses",score:()=>30}
  ];
  const context={
    console,Math,Set,Map,Number,String,Boolean,Object,Array,RegExp,state,COMMANDS,
    empire,owned,getWar,dist:(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),fleetRecord:id=>state.fleets.find(f=>f.id===id)||null,
    empiresAtWar:(a,b)=>!!getWar(a,b),rand:(a,b)=>(a+b)/2,
    createShip:(type,from,to,owner,opts={})=>{const ship={id:`s${state.ships.length+1}`,type,from:from.id,to:to.id,owner,...opts};state.ships.push(ship);return ship},
    endWar:(war,reason)=>{war.active=false;war.endedReason=reason;return war},
    declareWar:(a,b,reason)=>{const current=getWar(a,b);if(current)return current;const war={id:`w${state.wars.length+1}`,a,b,aggressor:a,active:true,startedAt:state.simTime,reason};state.wars.push(war);return war},
    resolveBattle:(battle,planet,index)=>{coreResolveCalls++;const win=battle.attackerStrength>battle.defenderStrength*1.04;if(win)planet.owner=battle.attacker;planet.underAttack=false;state.battles.splice(index,1)},
    resolveFleetArrival:()=>{coreArrivalCalls++},
    stxFCAPostHand:()=>{state.commandChoices=[{id:"old-invade",stxFleetOrderKind:"invade"},{id:"old-concentrate",stxFleetOrderKind:"concentrate"},COMMANDS[0],COMMANDS[1]]},
    stxFCAWars:()=>state.wars.filter(w=>w.active&&(w.a===0||w.b===0)),
    stxFCAChoice:kind=>({id:`new-${kind}`,title:kind==="invade"?"Invade":"Concentrate Forces",stxFleetOrderKind:kind}),
    stxFCAChoiceCandidate:c=>!c||c.stxFleetOrderKind?null:c,
    $:()=>({hidden:false}),renderCommands:()=>{renderCalls++},showToast:()=>{},stxActivity:()=>{}
  };
  context.globalThis=context;
  vm.createContext(context);
  vm.runInContext(moduleSource,context,{filename:"war-campaign-continuity.js"});
  return{context,state,COMMANDS,metrics:{get coreResolveCalls(){return coreResolveCalls},get coreArrivalCalls(){return coreArrivalCalls},get renderCalls(){return renderCalls}}};
}

test("loader installs war campaign continuity after every other tactical layer",()=>{
  const continuity=loaderSource.indexOf("'./space-tyrants/war-campaign-continuity.js'");
  const deep=loaderSource.indexOf("'./space-tyrants/deep-space-bases.js'");
  assert.ok(continuity>deep);
  assert.equal(loaderSource.match(/war-campaign-continuity\.js/g)?.length,1);
});

test("negotiated peace blocks an immediate redeclaration for the same pair",()=>{
  const {context,state}=createHarness();
  const war=context.declareWar(0,1,"test war");
  context.endWar(war,"accepted peace");
  assert.equal(context.SpaceTyrantsWarContinuity.truceUntil(0,1),60);
  state.simTime=1;
  assert.equal(context.declareWar(1,0,"instant revenge"),null);
  assert.equal(state.wars.filter(w=>w.active).length,0);
  state.simTime=61;
  assert.ok(context.declareWar(1,0,"later dispute"));
});

test("fleets already traveling when peace lands turn back instead of restarting combat",()=>{
  const {context,state,metrics}=createHarness();
  state.fleets.push({id:"enemy-fleet",name:"Enemy Fleet",owner:1,location:null,strength:18,status:"Inbound"});
  const war=context.declareWar(0,1,"test war");
  context.endWar(war,"accepted peace");
  const arriving={id:"arriving",type:"fleet",owner:1,fleetId:"enemy-fleet",strength:18,to:"p0",vesselName:"Enemy Fleet"};
  context.resolveFleetArrival(arriving,state.planets[0]);
  assert.equal(metrics.coreArrivalCalls,0);
  assert.ok(state.ships.some(s=>s.owner===1&&s.to==="p1"&&s.retreat));
});

test("manual invasion battle stays open until all assigned invasion ships arrive",()=>{
  const {context,state,metrics}=createHarness();
  const plan={id:"ip1",targetId:"p1",stxManualAllocation:true,stxAssignedFleetIds:["early","late"]};
  state.empires[0].invasionPlans.push(plan);
  state.fleets.push({id:"early",owner:0,strength:2},{id:"late",owner:0,strength:15});
  const battle={id:"b1",planetId:"p1",attacker:0,defender:1,attackerStrength:.2,defenderStrength:12,attackerFleetIds:["early"],defenderFleetIds:[],elapsed:9,maxDuration:20};
  state.battles.push(battle);state.planets[1].underAttack=true;
  state.ships.push({id:"late-ship",type:"fleet",owner:0,fleetId:"late",to:"p1",invasionPlanId:"ip1",strength:15});
  context.resolveBattle(battle,state.planets[1],0);
  assert.equal(metrics.coreResolveCalls,0);
  assert.ok(battle.attackerStrength>=.65);
  assert.equal(state.battles.length,1);
  state.ships.length=0;
  context.resolveBattle(battle,state.planets[1],0);
  assert.equal(metrics.coreResolveCalls,1);
});

test("a displayed defensive victory waits for already committed enemy reinforcements",()=>{
  const {context,state,metrics}=createHarness();
  const battle={id:"b2",planetId:"p0",attacker:1,defender:0,attackerStrength:.3,defenderStrength:14,attackerFleetIds:["raider"],defenderFleetIds:[],elapsed:10,maxDuration:20};
  state.battles.push(battle);state.planets[0].underAttack=true;
  state.ships.push({id:"reinforcement",type:"fleet",owner:1,fleetId:"enemy-reinforcement",to:"p0",battleId:"b2",strength:20});
  context.resolveBattle(battle,state.planets[0],0);
  assert.equal(metrics.coreResolveCalls,0);
  assert.equal(state.battles.length,1);
  state.ships.length=0;
  context.resolveBattle(battle,state.planets[0],0);
  assert.equal(metrics.coreResolveCalls,1);
  assert.equal(state.planets[0].stxSecuredOwner,0);
  assert.equal(state.planets[0].stxSecuredFrom,1);
});

test("recent battle winner gets a short consolidation guard against instant straggler recapture",()=>{
  const {context,state,metrics}=createHarness();
  state.fleets.push({id:"straggler",name:"Straggler Fleet",owner:1,location:null,strength:10,status:"Inbound"});
  const battle={id:"b3",planetId:"p0",attacker:1,defender:0,attackerStrength:.1,defenderStrength:15,attackerFleetIds:[],defenderFleetIds:[],elapsed:20,maxDuration:20};
  state.battles.push(battle);state.planets[0].underAttack=true;
  context.resolveBattle(battle,state.planets[0],0);
  assert.equal(state.planets[0].stxSecuredOwner,0);
  context.resolveFleetArrival({id:"straggler-ship",type:"fleet",owner:1,fleetId:"straggler",strength:10,to:"p0"},state.planets[0]);
  assert.equal(metrics.coreArrivalCalls,0);
  assert.ok(state.ships.some(s=>s.owner===1&&s.to==="p1"&&s.retreat));
});

test("wartime command hand is five cards with three non-force choices",()=>{
  const {context,state}=createHarness();
  state.wars.push({id:"war",a:0,b:1,active:true});
  context.stxFCAPostHand();
  assert.equal(state.commandChoices.length,5);
  assert.equal(state.commandChoices[0].stxFleetOrderKind,"invade");
  assert.equal(state.commandChoices[1].stxFleetOrderKind,"concentrate");
  assert.equal(state.commandChoices.slice(2).filter(c=>!c.stxFleetOrderKind).length,3);
  assert.ok(state.commandChoices.slice(2).some(c=>/Industry|Research|Trade/.test(c.cat)));
});
