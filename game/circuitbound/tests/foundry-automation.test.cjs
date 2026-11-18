const test = require('node:test');
const assert = require('node:assert/strict');
const Engineering = require('../engineering.js');
const Automation = require('../foundry-automation.js');

function createWorld() {
  const width=48,height=30,T={AIR:0,STONE:1,DIRT:2,GRASS:3,OBSIDIAN:4,STORAGE:5,PLATFORM:6,
    LADDER:7,WIRE:8,TEST_RIG:9,TEST_RARE:10};
  const defs={
    0:{solid:false},1:{solid:true,drop:'stone'},2:{solid:true,drop:'dirt'},3:{solid:true},4:{solid:true},
    5:{solid:true,transport:true,ports:[],inventoryCapacity:64},
    6:{solid:true,platform:true},7:{solid:false,ladder:true},8:{solid:false},
    9:{solid:true,foundryRig:{},ports:[],transport:true,accepts:['coolantCanister']},
    10:{solid:true,rareOre:true}
  };
  const items=['copper','stone','dirt','ammo','repairKit','coolantCanister','emptyCoolantCanister']
    .map(key=>({key,name:key,tile:-2}));
  items.push({key:'platform',name:'Platform',tile:T.PLATFORM}, {key:'wire',name:'Wire',tile:T.WIRE},
    {key:'ladder',name:'Ladder',tile:T.LADDER});
  const recipes=[],meta=new Map(),cells=new Uint8Array(width*height),inventory={},notices=[],scans=[];
  const survival={projectiles:[],enemies:[],hit(){}};
  Automation.install(T,defs,items,recipes);
  const key=(x,y)=>x+y*width, inside=(x,y)=>x>=0&&y>=0&&x<width&&y<height;
  const get=(x,y)=>inside(x,y)?cells[key(x,y)]:T.STONE;
  let engine=null;
  function set(x,y,id,data) {
    cells[key(x,y)]=id;
    if (data) meta.set(key(x,y),data);
    else meta.delete(key(x,y));
    engine?.changed(x,y);
  }
  engine=new Engineering.Simulation({T,defs,width,height,get,set,meta:()=>meta});
  const home={x:5,y:10}, active={powered:true};
  for(let x=0;x<width;x++) set(x,11,T.STONE);
  set(home.x,home.y,T.FOUNDRY_FLEET_DOCK,{...active,nodeId:1,contents:{}});
  const getMeta=(x,y)=>meta.get(key(x,y));
  const fleet=Automation.create({T,defs,inventory,engine,get,set,getMeta,inside,items,
    player:{x:5*24,y:9*24,w:16,h:33},getSurvival:()=>survival,notify:msg=>notices.push(msg),
    survey:(x,y,radius)=>{scans.push({x,y,radius});return [{}];}});
  const deploy=(role,extra={})=>fleet.restore({units:[{id:1,key:role,home:{...home,node:1},x:6,y:10,charge:100,...extra}]});
  const tick=(seconds)=>{for(let i=0;i<Math.round(seconds/.05);i++)fleet.tick(.05);};
  return {fleet,T,get,set,getMeta,active,deploy,tick,scans,survival,engine};
}

test('freight aircraft holds real cargo at a full depot and continues after saving',()=>{
  const {fleet,T,set,getMeta,deploy,tick}=createWorld();
  const source={x:12,y:10}, destination={x:25,y:10};
  set(source.x,source.y,T.STORAGE,{contents:{copper:32}});
  set(destination.x,destination.y,T.STORAGE,{contents:{stone:64}});
  deploy('freightDrone',{pickup:source,target:destination});
  tick(12);
  const saved=fleet.export();
  assert.deepEqual(saved.units[0].cargo,{copper:24});
  assert.deepEqual(getMeta(source.x,source.y).contents,{copper:8});
  assert.match(fleet.describe(5,10).deployedRows[0].reason,/full/);
  fleet.restore(saved);
  getMeta(destination.x,destination.y).contents={};
  tick(12);
  assert.deepEqual(getMeta(destination.x,destination.y).contents,{copper:32});
  assert.deepEqual(getMeta(source.x,source.y).contents,{});
});

test('rail transfer follows rails and an interruption stops service',()=>{
  const {fleet,T,set,getMeta,active,deploy,tick}=createWorld();
  const source={x:12,y:9}, target={x:28,y:9};
  set(source.x,source.y,T.FREIGHT_STOP,{...active,freightName:'Mine',contents:{copper:72}});
  set(target.x,target.y,T.FREIGHT_STOP,{...active,freightName:'Mill',contents:{}});
  for(let x=6;x<=28;x++) set(x,10,T.FREIGHT_RAIL);
  assert.deepEqual(fleet.stations().map(stop=>stop.name),['Mine','Mill']);
  assert.equal(fleet.routeBetween(source,target).length,17);
  set(20,10,T.AIR);
  assert.equal(fleet.routeBetween(source,target),null);
  deploy('railHauler',{pickup:source,target});
  tick(8);
  assert.deepEqual(getMeta(target.x,target.y).contents,{});
  set(20,10,T.FREIGHT_RAIL);
  tick(14);
  assert.deepEqual(getMeta(target.x,target.y).contents,{copper:72});
});

test('rig tender delivers coolant, repairs and returns used cans as inventory',()=>{
  const {fleet,T,set,getMeta,deploy,tick}=createWorld();
  const target={x:15,y:10};
  set(target.x,target.y,T.TEST_RIG,{hp:30,maxHp:90,contents:{},outputs:{emptyCoolantCanister:2}});
  getMeta(5,10).contents={coolantCanister:3,repairKit:1};
  deploy('rigTender',{target});
  tick(14);
  const rig=getMeta(target.x,target.y);
  assert.equal(rig.hp,90);
  assert.deepEqual(rig.contents,{coolantCanister:2});
  assert.deepEqual(rig.outputs,{});
  assert.deepEqual(getMeta(5,10).contents,{emptyCoolantCanister:2});
  assert.deepEqual(fleet.export().units[0].cargo,{coolantCanister:1});
});

test('survey runs once per assignment and preserves marked target',()=>{
  const {fleet,deploy,tick,scans}=createWorld();
  deploy('surveyDrone',{target:{x:15,y:6}});
  tick(12);
  assert.deepEqual(scans,[{x:15,y:6,radius:14}]);
  assert.equal(fleet.export().units[0].done,true);
  const saved=fleet.export();
  fleet.restore(saved);
  tick(8);
  assert.equal(scans.length,1);
});

test('construction consumes stocked placements and leaves a blocked blueprint intact',()=>{
  const {T,set,get,getMeta,deploy,tick}=createWorld();
  const target={x:11,y:9};
  getMeta(5,10).contents={platform:2};
  const blueprint={name:'Walkway',placements:[{x:0,y:0,key:'platform'},{x:1,y:0,key:'platform'}]};
  set(12,9,T.TEST_RARE);
  deploy('constructionDrone',{target,blueprint});
  tick(7);
  assert.equal(get(11,9),T.AIR);
  assert.deepEqual(getMeta(5,10).contents,{platform:2});
  set(12,9,T.AIR);
  tick(10);
  assert.equal(get(11,9),T.PLATFORM);
  assert.equal(get(12,9),T.PLATFORM);
  assert.deepEqual(getMeta(5,10).contents,{});
});

test('interceptor expends a round per intercepted shot within its zone',()=>{
  const {fleet,survival,deploy,tick}=createWorld();
  deploy('interceptorDrone',{target:{x:8,y:9},cargo:{ammo:2}});
  survival.projectiles=[{owner:'enemy',x:9*24,y:9*24,life:5}];
  tick(4);
  assert.equal(survival.projectiles[0].life,0);
  assert.deepEqual(fleet.export().units[0].cargo,{ammo:1});
});

test('a tunnel crawler clears ordinary terrain without removing rare seams or installed parts',()=>{
  const {fleet,T,set,get,deploy,tick,engine}=createWorld();
  for(let x=8;x<=14;x++) set(x,10,T.STONE);
  set(10,10,T.TEST_RARE);
  set(11,10,T.FREIGHT_RAIL);
  for(let x=8;x<=14;x++) set(x,9,T.DIRT);
  deploy('tunnelCrawler',{target:{x:14,y:10}});
  tick(25);
  assert.equal(fleet.export().units[0].done,true);
  assert.equal(get(10,10),T.TEST_RARE);
  assert.equal(get(11,10),T.FREIGHT_RAIL);
  assert.equal(get(14,10),T.AIR);
  assert.ok(engine.drops.length>0);
});

test('a stabilizer holds its anchor while active and releases it when recalled',()=>{
  const {fleet,T,set,getMeta,deploy,tick}=createWorld();
  const target={x:15,y:10};
  set(target.x,target.y,T.TEST_RIG,{});
  deploy('stabilizerWalker',{target});
  tick(8);
  assert.equal(getMeta(target.x,target.y).foundryStabilized,true);
  assert.match(fleet.describe(5,10).deployedRows[0].phase,/Anchored/);
  const state=fleet.export();
  state.units[0].recalled=true;
  fleet.restore(state);
  tick(8);
  assert.equal(getMeta(target.x,target.y).foundryStabilized,false);
});

test('a repair drone takes real kits and stays idle after completing repairs',()=>{
  const {fleet,T,set,getMeta,deploy,tick}=createWorld();
  const target={x:12,y:10};
  set(target.x,target.y,T.STORAGE,{hp:25,maxHp:100});
  getMeta(5,10).contents={repairKit:3};
  deploy('repairDrone',{target});
  tick(9);
  assert.equal(getMeta(target.x,target.y).hp,100);
  assert.deepEqual(getMeta(5,10).contents,{});
  assert.deepEqual(fleet.export().units[0].cargo,{repairKit:1});
});

test('a directional shield blocks an incoming shot only on its configured side',()=>{
  const {T,set,survival,deploy,tick}=createWorld();
  const target={x:10,y:10};
  set(target.x,target.y,T.STORAGE,{});
  deploy('shieldDrone',{target,shieldDir:1});
  survival.projectiles=[
    {owner:'enemy',x:12*24,y:10*24,life:5,vx:-100},
    {owner:'enemy',x:9*24,y:10*24,life:5,vx:100}
  ];
  tick(5);
  assert.equal(survival.projectiles[0].life,0);
  assert.equal(survival.projectiles[1].life,5);
});

