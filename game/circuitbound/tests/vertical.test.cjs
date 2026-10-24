const test=require('node:test');
const assert=require('node:assert/strict');
const {fixture}=require('./harness.cjs');
const V=require('../vertical.js');
function setup(){
  const f=fixture(60,50);f.T.COUNTERWEIGHT=f.T.COUNTERWEIGHT||99;
  f.defs[f.T.COUNTERWEIGHT]={name:'Counterweight',solid:true,weight:8,strength:20,counterweight:true,drop:'counterweight'};
  f.player={x:45*24,y:10*24,w:15,h:34,health:100};f.m={running:true};
  f.options=(group,counterGroup=[],extra={})=>({group,counterGroup,x:5,y:3,m:f.m,dy:-1,torque:5,engine:f.sim,get:f.get,set:f.put,meta:()=>f.meta,defs:f.defs,T:f.T,player:f.player,tile:24,width:60,height:50,...extra});
  f.load=()=>{f.put(5,10,f.T.IRON_BLOCK);f.put(6,10,f.T.IRON_BLOCK);const group=[[5,10],[6,10]];f.sim.link(group.map(p=>f.key(...p)));return group;};
  f.counter=()=>{f.put(10,10,f.T.COUNTERWEIGHT);f.put(11,10,f.T.COUNTERWEIGHT);const group=[[10,10],[11,10]];f.sim.link(group.map(p=>f.key(...p)));return group;};
  return f;
}
test('a heavy unbalanced elevator stalls while a real separate counterweight assists the same drive',()=>{
  const f=setup(),load=f.load();assert.equal(V.moveLift(f.options(load)),false);assert.match(f.m.fault,/STALLED/);assert.ok(f.m.requiredTorque>15);
  const counter=f.counter();assert.equal(V.moveLift(f.options(load,counter)),true);
  assert.equal(f.get(5,9),f.T.IRON_BLOCK);assert.equal(f.get(10,11),f.T.COUNTERWEIGHT);assert.equal(f.get(5,10),f.T.AIR);
  assert.equal(f.m.loadMass,14);assert.equal(f.m.counterweightMass,16);assert.equal(f.m.requiredTorque,3);assert.equal(f.m.counterbalanced,true);
});
test('counterbalance has real directional work: raising the heavier counterweight needs additional torque',()=>{
  const f=setup(),load=f.load(),counter=f.counter();assert.equal(V.moveLift(f.options(load,counter,{torque:4})),true);
  assert.equal(V.moveLift(f.options([[5,9],[6,9]],[[10,11],[11,11]],{dy:1,torque:4})),false);
  assert.equal(f.m.requiredTorque,5);assert.equal(f.get(5,9),f.T.IRON_BLOCK);assert.equal(f.get(10,11),f.T.COUNTERWEIGHT);
});
test('an obstruction under the counterweight atomically prevents movement of both assemblies and all cargo',()=>{
  const f=setup(),load=f.load(),counter=f.counter();f.sim.data(f.key(5,10)).contents={iron:7};f.put(10,11,f.T.STONE);
  const before=Array.from(f.world);assert.equal(V.moveLift(f.options(load,counter)),false);assert.match(f.m.fault,/COUNTERWEIGHT PATH BLOCKED/);
  assert.deepEqual(Array.from(f.world),before);assert.equal(f.sim.data(f.key(5,10)).contents.iron,7);
});
test('a rising elevator checks the riders whole body path and never pushes a rider into a ceiling',()=>{
  const f=setup(),load=f.load(),counter=f.counter();f.player.x=5*24+3;f.player.y=10*24-f.player.h;f.put(5,7,f.T.STONE);
  const before=f.player.y;assert.equal(V.moveLift(f.options(load,counter,{torque:12})),false);assert.match(f.m.fault,/RIDER PATH BLOCKED/);
  assert.equal(f.player.y,before);assert.equal(f.player.health,100);assert.equal(f.get(5,10),f.T.IRON_BLOCK);assert.equal(f.get(10,10),f.T.COUNTERWEIGHT);
});
test('opposing assemblies cannot exchange occupied cells by passing through each other',()=>{
  const f=setup();f.put(5,10,f.T.IRON_BLOCK);f.put(5,9,f.T.COUNTERWEIGHT);
  f.sim.link([f.key(5,10)]);f.sim.link([f.key(5,9)]);
  assert.equal(V.moveLift(f.options([[5,10]],[[5,9]],{torque:12})),false);assert.match(f.m.fault,/PATHS INTERSECT/);
  assert.equal(f.get(5,10),f.T.IRON_BLOCK);assert.equal(f.get(5,9),f.T.COUNTERWEIGHT);
});
test('guide rails constrain a lift to a continued track and stop it at either track end',()=>{
  const f=setup();f.put(5,10,f.T.PLATFORM);f.sim.link([f.key(5,10)]);f.put(4,10,f.T.RAIL);
  assert.equal(V.hasGuide([[5,10]],f.get,f.T),true);assert.equal(V.moveLift(f.options([[5,10]])),false);assert.match(f.m.fault,/RAIL ENDS/);
  f.put(4,9,f.T.RAIL);assert.equal(V.moveLift(f.options([[5,10]])),true);assert.equal(f.m.guided,true);
  assert.equal(V.moveLift(f.options([[5,9]])),false);assert.equal(f.get(5,9),f.T.PLATFORM);
});
test('counterweight discovery requires a separate linked real weight and cannot detach anchored joint loads',()=>{
  const f=setup(),load=f.load();f.put(7,8,f.T.COUNTERWEIGHT);assert.deepEqual(V.findCounterweight(f.sim,load,5,3),[]);
  const counter=f.counter();assert.deepEqual(V.findCounterweight(f.sim,load,5,3),counter);
  const counterId=f.meta.get(f.key(...counter[0])).assembly;f.put(9,10,f.T.BEARING,{jointAssembly:counterId});
  assert.equal(V.moveLift(f.options(load,counter)),false);assert.match(f.m.fault,/ANCHORED JOINT/);
});
test('a real counterweight cannot be borrowed by another winch that already claims its physical assembly',()=>{
  const f=setup(),load=f.load(),counter=f.counter(),id=f.meta.get(f.key(...counter[0])).assembly;
  f.put(5,3,f.T.WINCH,{counterweightAssembly:id});assert.deepEqual(V.findCounterweight(f.sim,load,5,3),counter,'the owning winch keeps its own weight');
  f.put(8,3,f.T.WINCH,{counterweightAssembly:id});assert.deepEqual(V.findCounterweight(f.sim,load,5,3),[]);
  assert.equal(V.moveLift(f.options(load,counter)),false);assert.match(f.m.fault,/ANOTHER WINCH/);assert.equal(f.get(5,10),f.T.IRON_BLOCK);
});
test('a winch keeps its attached weight instead of exchanging it for a newly closer unclaimed weight',()=>{
  const f=setup(),load=f.load(),counter=f.counter(),id=f.meta.get(f.key(...counter[0])).assembly;
  f.put(5,3,f.T.WINCH,{counterweightAssembly:id});f.put(7,7,f.T.COUNTERWEIGHT);f.sim.link([f.key(7,7)]);
  assert.deepEqual(V.findCounterweight(f.sim,load,5,3),counter);
});
test('the counterweight cannot cross above its anchor and disappear from the attached cable range',()=>{
  const f=setup(),load=f.load();f.put(10,4,f.T.COUNTERWEIGHT);f.sim.link([f.key(10,4)]);
  assert.equal(V.moveLift(f.options(load,[[10,4]],{dy:1,torque:12})),false);assert.match(f.m.fault,/CABLE LIMIT/);
  assert.equal(f.get(5,10),f.T.IRON_BLOCK);assert.equal(f.get(10,4),f.T.COUNTERWEIGHT);
});
test('lift movement and save/restore preserve distinct assembly IDs and both input and output cargo',()=>{
  const f=setup(),load=f.load(),counter=f.counter(),a=f.meta.get(f.key(...load[0])),b=f.meta.get(f.key(...counter[0]));
  a.contents={iron:3};a.outputs={steel:2};b.contents={stone:4};const mainId=a.assembly,counterId=b.assembly;
  f.player.x=5*24+3;f.player.y=10*24-f.player.h;const originalY=f.player.y;
  assert.equal(V.moveLift(f.options(load,counter,{torque:12})),true);assert.equal(f.player.y,originalY-24);
  const saved=JSON.parse(JSON.stringify({world:Array.from(f.world),meta:Array.from(f.meta),m:f.m,player:f.player})),restored=setup();
  restored.world.set(saved.world);restored.meta.clear();for(const [k,v] of saved.meta)restored.meta.set(k,v);restored.sim.reset();restored.m=saved.m;restored.player=saved.player;
  assert.equal(V.moveLift(restored.options([[5,9],[6,9]],[[10,11],[11,11]],{dy:1,torque:12})),true);
  assert.equal(restored.player.y,originalY);assert.equal(restored.meta.get(restored.key(5,10)).assembly,mainId);assert.equal(restored.meta.get(restored.key(10,10)).assembly,counterId);
  assert.deepEqual(restored.meta.get(restored.key(5,10)).contents,{iron:3});assert.deepEqual(restored.meta.get(restored.key(5,10)).outputs,{steel:2});assert.deepEqual(restored.meta.get(restored.key(10,10)).contents,{stone:4});
});
