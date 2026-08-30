import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source=readFileSync(new URL("./command-option-scaling.js",import.meta.url),"utf8");

function createHarness(worldCount=1){
  const elements=new Map();
  const panel={classList:{values:new Set(),add(v){this.values.add(v)}}};
  const make=id=>({id,hidden:id==="commandModal",textContent:"",innerHTML:"",classList:{add(){}},closest:()=>panel});
  for(const id of ["commandModal","commandGrid","commandSlots"])elements.set(id,make(id));
  const document={
    head:{appendChild(node){if(node.id)elements.set(node.id,node)}},
    getElementById:id=>elements.get(id)||null,
    createElement:tag=>({tag,id:"",textContent:"",classList:{add(){}}})
  };
  const state={
    planets:[],wars:[],commandChoices:[],commandSelected:new Set()
  };
  for(let i=0;i<worldCount;i++)state.planets.push({id:`p${i}`,name:`Mandate ${i}`,owner:0,x:i*100,y:0,underAttack:false,quality:{iron:2,titanium:2,helium:2,rare:2,silicates:2}});
  for(let i=0;i<12;i++)state.planets.push({id:`n${i}`,name:`Neutral ${i}`,owner:null,x:1000+i*120,y:100,underAttack:false,quality:{iron:2+i%3,titanium:2,helium:3,rare:2,silicates:2}});
  const expansionApply=()=>true;
  const COMMANDS=[
    {id:"claim",cat:"Resources",title:"Claim a Strategic World",desc:"Open a colony expedition to a neutral world.",effects:["Colony mission"],score:()=>70,target:()=>state.planets.find(p=>p.owner===null),apply:expansionApply},
    {id:"homesteads",cat:"Population",title:"Frontier Homestead Act",desc:"Settle a neutral world with a frontier expedition.",effects:["Settlement mission"],score:()=>65,target:()=>state.planets.find(p=>p.owner===null),apply:expansionApply}
  ];
  const cats=["Military","Industry","Resources","Population","Research","Infrastructure"];
  for(let i=0;i<24;i++)COMMANDS.push({id:`c${i}`,cat:cats[i%cats.length],title:`Command ${i}`,desc:"General strategic directive.",effects:["Effect"],score:()=>80-i,apply:()=>true});
  let seed=9;const random=()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296};
  const context={
    console,Math,Set,Map,Object,Array,Number,String,Boolean,RegExp,globalThis:null,state,COMMANDS,document,
    $:id=>elements.get(id)||null,
    playerWorlds:()=>state.planets.filter(p=>p.owner===0),
    rand:(a,b)=>a+(b-a)*random(),
    dist:(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),
    stxPlayerAtWar:()=>false,
    stxWarOnlyDirective:()=>false,
    stxDGIsInvasionChoice:c=>c.id==="invasion",
    stxDGCanExecuteChoice:c=>!(typeof c.target==="function"&&!c.targetObj),
    renderCommands(){},
    openCommandPhase(){
      state.commandChoices=COMMANDS.slice(2,6).map(c=>({...c,targetObj:null}));
      state.commandSelected.clear();
      elements.get("commandModal").hidden=false;
      elements.get("commandSlots").textContent="Sector Mandate · choose up to 2 directives";
    }
  };
  context.globalThis=context;vm.createContext(context);vm.runInContext(source,context,{filename:"command-option-scaling.js"});
  return{context,state,elements,panel};
}

test("option count scales at the requested planet thresholds",()=>{
  const {context}=createHarness();const api=context.SpaceTyrantsCommandOptions;
  assert.equal(api.optionCount(1),8);
  assert.equal(api.optionCount(2),8);
  assert.equal(api.optionCount(3),10);
  assert.equal(api.optionCount(7),10);
  assert.equal(api.optionCount(8),12);
  assert.equal(api.optionCount(19),12);
  assert.equal(api.optionCount(20),16);
  assert.equal(api.optionCount(40),16);
});

test("large empires receive sixteen actionable options with expansion choices kept common",()=>{
  const {context,state,elements,panel}=createHarness(20);
  context.openCommandPhase();
  assert.equal(state.commandChoices.length,16);
  const expansion=state.commandChoices.filter(context.SpaceTyrantsCommandOptions.isExpansionChoice);
  assert.ok(expansion.length>=4);
  assert.ok(expansion.every(c=>c.targetObj?.owner===null));
  assert.match(elements.get("commandSlots").textContent,/16 options$/);
  assert.ok(panel.classList.values.has("stx-scaled-command-panel"));
});

test("three-world empires receive ten options and keep at least a quarter expansion-oriented",()=>{
  const {context,state}=createHarness(3);
  context.openCommandPhase();
  assert.equal(state.commandChoices.length,10);
  const expansion=state.commandChoices.filter(context.SpaceTyrantsCommandOptions.isExpansionChoice);
  assert.ok(expansion.length>=3);
});
