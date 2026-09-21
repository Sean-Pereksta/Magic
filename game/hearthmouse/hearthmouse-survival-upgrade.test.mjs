import test from "node:test";
import assert from "node:assert/strict";
import { foodLoad, FOOD_LOADS, HabitMemory, SightBudget, canReuseActorPath, rememberActorPath, darknessVisibility, sweptPawHit } from "./hearthmouse-survival-core.mjs";
import { bundledRuntime } from "./hearthmouse-runtime-fixture.mjs";
import { installSurvivalUpgrade, dropPlayerFood, updatePhysicalPounce } from "./hearthmouse-survival-upgrade.mjs";
import { buildTraversal, moveTraversingPlayer, startContextClimb, advanceTraversal } from "./hearthmouse-survival-traversal.mjs";
import { canCloseEventDoor, triggerHouseholdEvent, updateHouseholdEvents, resetHouseholdEvents } from "./hearthmouse-survival-events.mjs";
import { batchStaticScenery, staticBatchEligible } from "./hearthmouse-static-batching.mjs";
import { carveMouseOpening } from "./hearthmouse-habitat.mjs";
import { HearthmousePerformanceManager } from "./hearthmouse-performance-manager.mjs";
const R = bundledRuntime(), I = R.I, v = (x=0,z=0,y=0.025) => new I.Vector3(x,y,z);

test("routes are retained until meaningful target, door, room, strategy or blockage changes", () => {
  const a = { rig: { root: { position:v() } }, path:[v(2)], pathIndex:0 }, target=v(2);
  const c = { time:1, revision:1, intent:"chase", playerRoom:"living", targetRoom:"living", tier:"full", chasing:true };
  rememberActorPath(a,target,c);
  for(let i=0;i<30;i++) assert.equal(canReuseActorPath(a,v(2.15),{...c,time:1+i/60},()=>true),true);
  assert.equal(canReuseActorPath(a,v(3),c,()=>true),false);
  for(const change of [{revision:2},{intent:"ambush"},{playerRoom:"kitchen"},{targetRoom:"kitchen"},{tier:"adjacent"}]) assert.equal(canReuseActorPath(a,target,{...c,...change},()=>true),false);
  assert.equal(canReuseActorPath(a,target,{...c,time:2},()=>false),false);
  a.path=[]; assert.equal(canReuseActorPath(a,target,c,()=>true),false);
});

test("sight work never exceeds its global ray cap; urgent reservations and deferred peers survive", () => {
  const budget=new SightBudget(), chase={rank:0}, far={rank:3}, peers=Array.from({length:5},()=>({rank:2}));
  const served=new Set();
  for(let frame=1;frame<12;frame++) {
    budget.begin([far,...peers,chase],frame,12,c=>c.rank,()=>5);
    assert.equal(budget.take(far,5),false);
    assert.equal(budget.take(chase,5),true);
    for(const cat of peers)if(budget.take(cat,5))served.add(cat);
    assert.ok(budget.used<=12);
  }
  assert.equal(served.size,peers.length);
});

test("cat intelligence instantly promotes near the player and keeps distant cats off the ray budget", () => {
  const root=new I.Group(), cat={id:"far",state:"relaxed",pouncePhase:"none",rig:{root:new I.Group()}};root.add(cat.rig.root);cat.rig.root.position.set(22,0,4);
  const e={playerPosition:v(-5,2),snapshot:{night:12},cats:[cat],mice:[],world:{root}};
  const m=new HearthmousePerformanceManager(e);m.beginFrame(1/60);
  assert.equal(m.catIntelligenceTier(cat),"distant");assert.equal(m.shouldScanCatVision(cat),false);
  e.playerPosition.set(22,0,3);m.beginFrame(1/60);
  assert.equal(m.catIntelligenceTier(cat),"full");assert.equal(m.takeSightRays(cat,5),true);
});

test("habit learning needs repeated witnessed encounters and decays without sharing cat knowledge", () => {
  const a=new HabitMemory(),b=new HabitMemory();
  assert.equal(a.observe("hole",v(3),0,false),0);
  a.observe("hole",v(3),1,true);for(let i=2;i<8;i++)a.observe("hole",v(3),i,true);
  assert.equal(a.best(8),null);
  a.observe("hole",v(3),11,true);a.observe("hole",v(3),21,true);
  assert.equal(a.best(21).key,"hole");assert.equal(b.best(21),null);assert.equal(a.best(500),null);
});

test("heavy food affects every movement factor; darkness still reveals nearby mice", () => {
  const small=foodLoad({value:1}),heavy=foodLoad({value:5});
  for(const key of ["speed","acceleration","turn","jump"])assert.ok(heavy[key]<small[key]);
  assert.ok(heavy.noise>small.noise);assert.ok(heavy.bulk>0.17);
  assert.equal(darknessVisibility(0.08,0.6),1);assert.equal(darknessVisibility(0.08,6),0);assert.ok(darknessVisibility(1,6)>0.8);
});

function barePlayer() {
  class Engine extends I.Engine {}
  Engine.prototype.__livingPredatorsInstalled=true; installSurvivalUpgrade({...I,Engine});
  const e=Object.create(Engine.prototype);
  Object.assign(e,{time:10,snapshot:{phase:"foraging",timeRemaining:240},world:{root:new I.Group(),colliders:[],nestCenter:v(-8,4),surfaceAt:()=>"carpet"},cats:[],mice:[],foods:[],
    playerPosition:v(),playerVelocity:v(0,0,0),playerEyeY:0.066,verticalVelocity:0,onGround:true,yaw:0,pitch:0,keys:new Set(),touch:{moveX:0,moveY:0,lookX:0,lookY:0},
    tempA:v(),tempB:v(),tempC:v(),audio:new Proxy({}, {get:()=>()=>{}}),lastPlayerNoise:0,lastPlayerMoving:0,
    updateCamera(){},showMessage(){},publish(){},insideNest:()=>false});
  return e;
}

test("actual player movement slows under load and dropping restores speed without losing the meal", () => {
  const run=food=>{const e=barePlayer();e.carriedFood=food;e.keys.add("KeyW");e.keys.add("ShiftLeft");for(let i=0;i<60;i++){e.time+=1/60;e.updatePlayer(1/60);}return Math.abs(e.playerPosition.z);};
  assert.ok(run({value:5})<run(null)*0.72);
  const e=barePlayer(), food={value:5,kind:"cheese",mesh:I.makeFood("cheese"),carriedBy:"player"};e.carriedFood=food;e.world.root.add(food.mesh);e.foods.push(food);
  assert.equal(dropPlayerFood(e,I),true);assert.equal(e.carriedFood,null);assert.equal(e.survivalMovement,FOOD_LOADS.empty);
  assert.equal(food.deposited,false);assert.equal(food.carriedBy,null);assert.equal(food.mesh.parent,e.world.root);assert.ok(food.__pickupBlockedUntil>e.time);
  assert.equal(dropPlayerFood(e,I),false);
});

test("committed pounce can be dodged, hits crossed paws, and recovers after furniture collision", () => {
  function encounter(block=false,dodge=false) {
    const e=barePlayer(),cat={id:"mabel",personality:"hunter",state:"chase",targetId:"player",rig:R.cat(),pouncePhase:"windup",pounceTimer:0.1,pounceVisual:0,lastSeen:v(0,-0.8),path:[]};
    cat.rig.root.position.set(0,0,0);e.playerPosition.set(0,0.025,-0.8);e.targetPosition=()=>e.playerPosition;e.targetVisibility=()=>1;e.yawToward=()=>0;e.playerCaught=()=>e.caught=true;
    if(block)e.world.colliders=[{minX:-1,maxX:1,minZ:-0.48,maxZ:-0.4,minY:0,maxY:1}];
    updatePhysicalPounce(e,cat,0.02,I,()=>{});
    // Init goal while windup is still tracking, then move after the lock.
    if(dodge)e.playerPosition.x=0.7;
    for(let i=0;i<32;i++){e.time+=0.02;updatePhysicalPounce(e,cat,0.02,I,()=>({speed:0,pounce:0}));}
    return {e,cat};
  }
  assert.ok(encounter(false,false).e.caught);
  const missed=encounter(false,true);assert.equal(missed.e.caught,undefined);assert.equal(missed.cat.pouncePhase,"recover");
  const blocked=encounter(true,false);assert.equal(blocked.e.caught,undefined);assert.ok(blocked.cat.rig.root.position.z>-0.4);
  assert.equal(sweptPawHit(v(-1,0,0.09),v(1,0,0.09),v(0,0,0.07)),true);
  assert.equal(sweptPawHit(v(-1,0,0.09),v(1,0,0.09),v(0,0,0.7)),false);
});

test("static scenery batches preserve source LOS and cut walls; interactive props remain separate", () => {
  const root=new I.Group(),mat=new I.MeshStandardMaterial();
  const walls=[0,3].map(x=>{const m=new I.Mesh(new I.BoxGeometry(.2,3,4),mat);m.name='north-wall';m.position.set(x,1.5,0);root.add(m);return m;});
  const door=new I.Mesh(new I.BoxGeometry(.2,3,1),mat);door.name="locked-door";root.add(door);
  const e={world:{root,colliders:[]}};
  assert.equal(staticBatchEligible(door,root),false);assert.equal(batchStaticScenery(e,I),1);assert.equal(door.visible,true);
  const ray=new R.Raycaster(v(-1,0,.09),v(1,0,0));root.updateWorldMatrix(true,true);assert.ok(ray.intersectObject(walls[0]).length);
  assert.equal(carveMouseOpening(e,I,{id:"hole",x:-.11,z:0,axis:"x",style:"gnawed-wall"}),1);
  batchStaticScenery(e,I);root.updateWorldMatrix(true,true);ray.far=2;
  const batch=root.getObjectByName("static-furniture-visual-batch");assert.equal(ray.intersectObject(batch).length,0);
  ray.set(v(-1,0,.6),v(1,0,0));assert.ok(ray.intersectObject(batch).length);
});

test("doors require an alternate route and respect other closed doors", () => {
  const door={id:"ab",roomId:"b",mesh:new I.Group(),collider:{minX:0,maxX:.2,minZ:0,maxZ:1,active:false}}, other={id:"cb",collider:{active:false}};
  const e=barePlayer();e.snapshot.night=5;e.playerPosition=v(-4,2);e.world.occluders=[];e.world.shelterPoints=[];
  e.__expansion={roomDoors:[door,other],navEdges:[{a:"a",b:"b",doorId:"ab"},{a:"a",b:"c"},{a:"c",b:"b",doorId:"cb"}],dynamicProps:new Map(),performanceManager:{roomUnlocked:()=>true},routeRevision:1,routeCache:new Map(),roomRouteCache:new Map()};
  assert.equal(canCloseEventDoor(e,door),true);other.collider.active=true;assert.equal(canCloseEventDoor(e,door),false);other.collider.active=false;
  e.__expansion.navEdges.pop();assert.equal(canCloseEventDoor(e,door),false);
});

test("furniture traversal is spatially anchored, load gated, and supports landings", () => {
  const e=barePlayer();e.world=R.world(new I.Group());const t=buildTraversal(e,I);assert.ok(t.routes.length>=2);
  const state={traversal:t};const route=t.routes.find(r=>r.kind==="cord");assert.ok(route);
  e.playerPosition.copy(route.start);e.playerVelocity.set(route.direction,0,0);e.carriedFood={value:5};startContextClimb(e,state);assert.equal(t.climb,null);
  e.carriedFood=null;startContextClimb(e,state);assert.ok(t.climb);e.time+=3;advanceTraversal(e,state,.016);assert.equal(t.climb,null);assert.ok(e.playerElevation>0.15);
  e.playerVelocity.set(0,0,0);advanceTraversal(e,state,.016);assert.ok(e.playerElevation>0.15);
});

test("real indexed raycasts obey the frame budget and invalidate sight when a door closes", async () => {
  const { installEnginePatches, rebuildStaticSpatialIndexes } = await import("./hearthmouse-expansion-core.mjs");
  const { installExpansionSightGuard } = await import("./hearthmouse-expansion-startup-guard.mjs");
  const r=bundledRuntime(), oldWindow=globalThis.window, J=r.I;
  globalThis.window={HearthmouseInternals:J,matchMedia:()=>({matches:false})};
  try {
    const e=Object.create(J.Engine.prototype);
    Object.assign(e,{time:1,scene:new J.Group(),snapshot:{night:1,population:4},cats:[],mice:[],foods:[],claimedPrizeIds:new Set(),startOfNightPopulation:4,isTouchOnlyDevice:()=>false});
    e.world=r.world(e.scene);e.playerPosition=new J.Vector3(-4,.025,2);e.playerView=new J.Group();
    installExpansionSightGuard(e,J);installEnginePatches(J);e.chooseNightEvent(1);
    e.world.occluders=[];e.world.colliders=[];rebuildStaticSpatialIndexes(e,e.__expansion);
    for(let i=0;i<6;i++) {const c={id:`cat-${i}`,state:"chase",targetId:"player",pouncePhase:"none",rig:r.cat(),yaw:0,lookYaw:0};c.rig.root.position.set(-4,0,3);e.world.root.add(c.rig.root);e.cats.push(c);}
    e.raycaster=new r.Raycaster();let casts=0;const cast=e.raycaster.intersectObjects.bind(e.raycaster);e.raycaster.intersectObjects=(...args)=>{casts++;return cast(...args);};
    const m=e.__expansion.performanceManager;m.beginFrame(1/60);
    for(const c of e.cats)e.targetVisibility(c,"player",e.playerPosition);
    assert.equal(casts,20);assert.equal(m.sightBudget.used,20);
    const wall=new J.Mesh(new J.BoxGeometry(2,2,.15),new J.MeshStandardMaterial());wall.position.set(-4,1,2.5);e.world.root.add(wall);e.world.occluders.push(wall);
    e.__expansion.routeRevision++;rebuildStaticSpatialIndexes(e,e.__expansion);e.time+=.1;m.beginFrame(.1);
    assert.equal(e.targetVisibility(e.cats[0],"player",e.playerPosition),0);
  } finally {if(oldWindow===undefined)delete globalThis.window;else globalThis.window=oldWindow;}
});
