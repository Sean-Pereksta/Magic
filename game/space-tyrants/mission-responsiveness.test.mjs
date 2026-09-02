import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source=readFileSync(new URL("./mission-responsiveness.js",import.meta.url),"utf8");
const loader=readFileSync(new URL("../space-tyrants.html",import.meta.url),"utf8");

function harness(progress=.996){
  const sourcePlanet={id:"p0",name:"Origin",owner:0,x:0,y:0,pop:.1,capacity:.2,efficiency:1};
  const targetPlanet={id:"p1",name:"Destination",owner:0,x:100,y:50,pop:.19,capacity:.2,efficiency:.9};
  const ship={id:"s1",from:"p0",to:"p1",owner:0,progress,x:99,y:49};
  const state={ships:[ship],planets:[sourcePlanet,targetPlanet],selected:targetPlanet,commandChoices:[],commandSelected:new Set()};
  const elements=new Map([["commandModal",{hidden:true}],["commandGrid",{querySelectorAll:()=>[]}],["planetBody",{querySelectorAll:()=>[]}] ]);
  const styleIds=new Set();let arrivals=0,now=0;const hudCalls=[];
  const document={
    getElementById:id=>styleIds.has(id)?{id}:null,
    createElement:tag=>({tag,id:"",className:"",innerHTML:"",textContent:"",appendChild(){}}),
    head:{appendChild(node){if(node.id)styleIds.add(node.id)}}
  };
  const populationEfficiency=p=>{const r=p.pop/Math.max(.02,p.capacity);return r<=.86?1:Math.max(.38,Math.min(1,1-(r-.86)*.62))};
  const modifiers={industry:1,shipbuilding:1,mining:1,training:1};
  const context={
    console,Math,Array,Object,Number,String,Set,Map,RegExp,globalThis:null,state,document,
    performance:{now:()=>now},
    $:id=>elements.get(id)||null,
    playerWorlds:()=>state.planets.filter(p=>p.owner===0),
    populationEfficiency,
    empire:()=>({modifiers:{}}),
    modifier:(_e,key)=>modifiers[key]??1,
    arriveShip(){arrivals++},
    tickShips(){},renderCommands(){},renderPlanet(){},
    updateHud(force=false){hudCalls.push(force)}
  };
  context.globalThis=context;vm.createContext(context);vm.runInContext(source,context,{filename:"mission-responsiveness.js"});
  return{context,state,ship,targetPlanet,hudCalls,setNow:v=>{now=v},arrivals:()=>arrivals};
}

test("missions at the visually complete threshold finish immediately",()=>{
  const h=harness(.996);
  const count=h.context.SpaceTyrantsMissionResponsiveness.finishReadyShips();
  assert.equal(count,1);
  assert.equal(h.arrivals(),1);
  assert.equal(h.state.ships.length,0);
  assert.equal(h.ship.progress,1);
  assert.equal(h.ship.x,h.targetPlanet.x);
  assert.equal(h.ship.y,h.targetPlanet.y);
});

test("missions below the completion threshold continue normally",()=>{
  const h=harness(.994);
  assert.equal(h.context.SpaceTyrantsMissionResponsiveness.finishReadyShips(),0);
  assert.equal(h.arrivals(),0);
  assert.equal(h.state.ships.length,1);
});

test("productive mandates expose concrete output previews",()=>{
  const h=harness(.2),api=h.context.SpaceTyrantsMissionResponsiveness;
  assert.deepEqual([...api.productivityLines({id:"industry",effects:[]})],["Factory production multiplier → ×1.45 for 70 cycles"]);
  const urban=[...api.productivityLines({id:"urbanRenewal",targetObj:h.targetPlanet,effects:["Productivity rises"]})];
  assert.match(urban[0],/^Target productivity \d+% → \d+%$/);
  assert.match(urban[1],/×1\.16/);
});

test("selected-world HUD can refresh at responsive cadence",()=>{
  const h=harness(.2);
  h.setNow(130);h.context.updateHud(false);
  assert.deepEqual(h.hudCalls,[true]);
  h.setNow(180);h.context.updateHud(false);
  assert.deepEqual(h.hudCalls,[true,false]);
  h.setNow(260);h.context.updateHud(false);
  assert.deepEqual(h.hudCalls,[true,false,true]);
});

test("loader installs responsiveness after command scaling and before cloud save",()=>{
  const scaling=loader.indexOf("./space-tyrants/command-option-scaling.js");
  const responsive=loader.indexOf("./space-tyrants/mission-responsiveness.js");
  const cloud=loader.indexOf("./space-tyrants/cloud-save.js");
  assert.ok(scaling>=0&&responsive>scaling&&cloud>responsive);
});
