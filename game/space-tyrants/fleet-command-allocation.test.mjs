import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";
import {fileURLToPath} from "node:url";

const patchPath=path.join(path.dirname(fileURLToPath(import.meta.url)),"fleet-command-allocation.js");
const patchSource=fs.readFileSync(patchPath,"utf8");

function element(id=""){
  return{
    id,hidden:false,value:"",disabled:false,checked:false,children:[],dataset:{},style:{setProperty(){}},classList:{add(){},toggle(){}},
    addEventListener(){},appendChild(child){this.children.push(child)},querySelector(){return null},querySelectorAll(){return[]},closest(){return null},matches(){return false},
    set innerHTML(value){this._innerHTML=value;for(const match of value.matchAll(/\bid="([^"]+)"/g))if(!this._registry[match[1]])this._registry[match[1]]=makeElement(match[1],this._registry)},
    get innerHTML(){return this._innerHTML||""},textContent:"",_registry:null
  };
}
function makeElement(id,registry){const node=element(id);node._registry=registry;return node}
function planet(id,name,owner,x,y){return{id,name,owner,x,y,r:20,home:false,underAttack:false,mandateGlow:0,garrison:12,infra:{defense:1,shipyard:1,factory:1,mine:1},orbitals:{station:0,base:0},orbitalFacilities:[],stock:{components:100,equipment:100,titanium:100}}}
function fleet(id,status,location="p0",strength=10){return{id,owner:0,name:id.toUpperCase(),status,location,homePort:"p0",strength,readiness:"ready",role:"fleet",destroyed:false,veterans:.2,battles:0,shipsLost:0}}

function harness(){
  const registry={};registry.commandModal=makeElement("commandModal",registry);registry.commandModal.hidden=false;registry.commandGrid=makeElement("commandGrid",registry);
  const body=makeElement("body",registry),head=makeElement("head",registry);body.appendChild=child=>{body.children.push(child);if(child.id)registry[child.id]=child};head.appendChild=child=>{head.children.push(child);if(child.id)registry[child.id]=child};
  const planets=[planet("p0","Earth",0,0,0),planet("p1","Mars",0,300,0),planet("p2","Noctis Prime",1,900,0),planet("p3","Noctis III",1,1100,120),planet("p4","Neutral",null,700,500),planet("p5","Other Rival",2,1300,400)];
  const empires=[{id:0,name:"Mandate",tech:{propulsion:0,weapons:0},invasionPlans:[]},{id:1,name:"Noctis",tech:{propulsion:0,weapons:0},invasionPlans:[]},{id:2,name:"Other",tech:{propulsion:0,weapons:0},invasionPlans:[]}];
  const state={fleets:[],ships:[],battles:[],wars:[{id:"w1",a:0,b:1,active:true}],planets,empires,simTime:100,commandChoices:[],commandSelected:new Set(),speed:0,preCommandSpeed:1,commandCycle:45,nextCommand:120,stxFleetAllocation:null};
  let serial=0;
  const context={
    console,document:{body,head,createElement:()=>makeElement("",registry)},window:{},state,COMMANDS:[
      {id:"invasion",cat:"Military",title:"Old Invasion",effects:[],score:()=>100,apply(){}},
      {id:"orbitalDefense",cat:"War",title:"Reinforce Orbital Defenses",effects:[],score:()=>80,apply(){}},
      {id:"enemyLogistics",cat:"War",title:"Disrupt Enemy Logistics",effects:[],score:()=>75,apply(){}}
    ],
    $:id=>registry[id]||(registry[id]=makeElement(id,registry)),
    openCommandPhase(){},renderCommands(){},issueCommands(){},tickPlanet(){},arriveShip(){},generateGalaxy(){},loadGame(){return true},renderShipLedger(){return""},
    clamp:(v,min,max)=>Math.max(min,Math.min(max,v)),random:()=>.25,rand:(min,max)=>(min+max)/2,
    dist:(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),borderThreat:()=>0,playerWorlds:()=>state.planets.filter(p=>p.owner===0),fleetRecord:id=>state.fleets.find(f=>f.id===id),
    empire:id=>state.empires.find(e=>e.id===id),empiresAtWar:(a,b)=>state.wars.some(w=>w.active&&((w.a===a&&w.b===b)||(w.a===b&&w.b===a))),getWar:(a,b)=>state.wars.find(w=>w.active&&((w.a===a&&w.b===b)||(w.a===b&&w.b===a))),
    stxNearestPlayerDistance:p=>Math.min(...state.planets.filter(x=>x.owner===0).map(x=>Math.hypot(x.x-p.x,x.y-p.y))),
    stxQueueInvasion(target,label){if(!context.empiresAtWar(0,target.owner))return false;let plan=empires[0].invasionPlans.find(p=>p.targetId===target.id);if(!plan){plan={id:`plan-${target.id}`,targetId:target.id,status:"assembling",label};empires[0].invasionPlans.push(plan)}return true},
    createShip(type,from,to,owner,extra={}){const ship={id:`s${++serial}`,type,from:from.id,to:to.id,x:from.x,y:from.y,startX:from.x,startY:from.y,progress:0,distance:context.dist(from,to),speed:86,owner,cargo:{},...extra};state.ships.push(ship);const f=context.fleetRecord(extra.fleetId);if(f){f.location=null;f.strength=ship.strength;f.status=`En route to ${to.name}`}return ship},
    vesselName:()=>"Fleet",showToast(){},logEvent(){},stxActivity(){},stxRefreshFleetLocator(){},updateSpeedButtons(){},updateHud(){},saveGame(){},addOrder(){},consume(){return 0},RESOURCE_LABEL:{},
    stxOLFacilityAt:id=>{for(const p of state.planets){const f=p.orbitalFacilities.find(x=>x.id===id);if(f)return{p,f}}return null},
    stxOLRemoveDocking(f){if(!f.dockedAt)return;const hit=context.stxOLFacilityAt(f.dockedAt);if(hit)hit.f.dockedShipIds=hit.f.dockedShipIds.filter(id=>id!==f.id);f.dockedAt=null},
    stxOLAddFacilityActivity(){},setModifier(){},performance:{now:()=>0}
  };
  vm.createContext(context);vm.runInContext(patchSource,context,{filename:patchPath});
  return{context,state,planets,registry};
}

function mixedFleetState(state){
  state.fleets=[fleet("i1","Idle"),fleet("i2","Commissioned"),fleet("i3","Stationed at Earth"),fleet("p1","Patrolling Earth"),fleet("p2","Orbit patrol"),fleet("t1","En route",null),fleet("t2","En route",null),Object.assign(fleet("o1","Defending Mars","p1"),{stxAssignment:{kind:"defend",locked:true}}),fleet("c1","Engaged at Noctis",null),fleet("c2","Engaged at Noctis",null),fleet("c3","Engaged at Noctis",null),fleet("c4","Engaged at Noctis",null)];
  state.ships.push({id:"move1",type:"fleet",fleetId:"t1",owner:0,from:"p0",to:"p1",x:120,y:0,startX:0,startY:0,progress:.4,distance:300,speed:86,strength:10},{id:"move2",type:"patrol",fleetId:"t2",owner:0,from:"p1",to:"p0",x:220,y:0,startX:300,startY:0,progress:.3,distance:300,speed:112,strength:10});
  state.battles=[{id:"battle",planetId:"p2",attacker:0,defender:1,attackerStrength:20,attackerInitial:40,defenderStrength:25,defenderInitial:30,attackerFleetIds:["c1","c2","c3","c4"],defenderFleetIds:[]}];
}

test("status changes selection priority, never eligibility",()=>{
  const{context,state}=harness();mixedFleetState(state);
  assert.deepEqual(context.stxFCAFree().map(f=>f.id),["i1","i2","i3","p1","p2","t1","t2","o1","c1","c2","c3","c4"]);
});

test("an exact ten-ship order uses combat ships only after all lower priorities",()=>{
  const{context,state,planets}=harness();mixedFleetState(state);
  assert.equal(context.stxFCAExecuteMove("concentrate",{key:"planet:p1",type:"planet",id:"p1",name:"Mars",planet:planets[1]},10),10);
  const commanded=state.fleets.filter(f=>f.stxAssignment?.kind==="concentrate"||state.ships.some(s=>s.fleetId===f.id&&s.stxFleetOrderKind==="concentrate"));
  assert.equal(commanded.length,10);
  assert.deepEqual(state.battles[0].attackerFleetIds,["c3","c4"]);
  assert.equal(state.fleets.find(f=>f.id==="c1").strength,5);
  assert.equal(state.fleets.find(f=>f.id==="c2").strength,5);
  assert.match(state.fleets.find(f=>f.id==="c1").status,/Disengaging from Battle of Noctis Prime/);
});

test("concentration destinations include stations and arrivals remain attached to the station",()=>{
  const{context,state,planets}=harness();state.fleets=[fleet("a1","Idle"),fleet("a2","Patrolling Earth","p1")];
  const station={id:"station-1",kind:"military",name:"Sol Orbital Command",hp:100,maxHp:100,dockedShipIds:[]};planets[0].orbitalFacilities=[station];
  const targets=context.stxFCAConcentrationTargets();assert.ok(targets.some(t=>t.type==="planet"&&t.id==="p0"));const destination=targets.find(t=>t.id==="station-1");assert.ok(destination);
  assert.equal(context.stxFCAExecuteMove("concentrate",destination,2),2);
  for(const ship of [...state.ships]){context.arriveShip(ship,planets[0]);state.ships=state.ships.filter(s=>s!==ship)}
  assert.deepEqual(new Set(station.dockedShipIds),new Set(["a1","a2"]));
  assert.equal(state.fleets.find(f=>f.id==="a2").dockedAt,"station-1");
  assert.match(state.fleets.find(f=>f.id==="a2").status,/Concentrated at Sol Orbital Command/);
});

test("two-target invasions split all commanded ships and preserve damaged strength",()=>{
  const{context,state,planets}=harness();state.fleets=Array.from({length:7},(_,i)=>fleet(`f${i+1}`,i<3?"Idle":"Patrolling Earth","p0",i===0?2:10));
  const destinations=[{key:"planet:p2",type:"planet",id:"p2",name:"Noctis Prime",planet:planets[2]},{key:"planet:p3",type:"planet",id:"p3",name:"Noctis III",planet:planets[3]}];
  const split=context.stxFCAEvenAllocation(destinations,7);assert.equal(split["planet:p2"],4);assert.equal(split["planet:p3"],3);
  assert.equal(context.stxFCAExecuteInvasionOrder(destinations,{"planet:p2":4,"planet:p3":3}),7);
  assert.equal(state.ships.filter(s=>s.to==="p2").length,4);assert.equal(state.ships.filter(s=>s.to==="p3").length,3);
  assert.equal(state.ships.find(s=>s.fleetId==="f1").strength,2);
});

test("wartime commands contain one red Invade and one Concentrate Forces card",()=>{
  const{context,state,registry}=harness();state.commandChoices=[{id:"old-a",cat:"Military",title:"Invade Noctis",effects:[],apply(){}},{id:"old-b",cat:"Campaign",title:"Invade Mars",effects:[],apply(){}},context.COMMANDS[1],context.COMMANDS[2]];registry.commandModal.hidden=false;
  context.stxFCAPostHand();
  assert.equal(state.commandChoices.filter(c=>c.stxFleetOrderKind==="invade").length,1);assert.equal(state.commandChoices.filter(c=>c.stxFleetOrderKind==="concentrate").length,1);
  assert.equal(state.commandChoices[0].title,"Invade");assert.equal(state.commandChoices[0].cat,"War");assert.equal(state.commandChoices[1].title,"Concentrate Forces");assert.equal(state.commandChoices[1].cat,"War");assert.equal(state.commandChoices.length,4);
});

test("selecting Invade opens its panel immediately without consuming the command",()=>{
  const{context,state,registry}=harness();state.fleets=[fleet("f1","Idle")];
  context.stxFCAOpenDirect("invade");
  assert.equal(registry.commandModal.hidden,true);assert.equal(registry.stxFCAOrderModal.hidden,false);assert.equal(registry.stxFCATitle.textContent,"Invasion Command");assert.equal(state.nextCommand,120);
});

test("invasion targets are only planets owned by active-war enemies",()=>{
  const{context}=harness();assert.deepEqual(context.stxFCATargets("invade").map(p=>p.id),["p2","p3"]);
});
