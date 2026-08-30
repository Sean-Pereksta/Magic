const test=require('node:test');
const assert=require('node:assert/strict');
const {fixture,E}=require('./harness.cjs');
function step(sim,n=1){for(let i=0;i<n;i++)sim.step(.05)}
function gate(symbol,a,b,rot=0){const f=fixture(),{T,sim,put}=f,k=sim.key(12,12);put(12,12,T[symbol],{rot});
  for(const [side,on] of [[2,a],[3,b]]){const s=(side+rot)%4,[dx,dy]=E.DIRS[s];put(12+dx,12+dy,T.SWITCH,{rot:(s+2)%4,on});}
  step(sim);return {f,m:sim.data(k)};
}
test('all added components have distinct IDs, recipes, names and rotated typed ports',()=>{
  const {T,defs,items,recipes}=fixture();assert.equal(new Set(Object.values(T)).size,Object.keys(T).length);
  for(const [symbol,key] of E.CATALOG){assert.ok(items.some(i=>i.key===key));assert.ok(recipes.some(i=>i.key===key));assert.ok(defs[T[symbol]].name)}
  for(let rot=0;rot<4;rot++){const p=E.ports(defs[T.MOTOR],{rot});assert.equal(p.find(p=>p.type==='power').side,(2+rot)%4);assert.equal(p.find(p=>p.type==='mechanical').side,rot)}
});
for(const symbol of ['AND','OR','NOT','XOR','NAND','NOR'])test(`${symbol} truth table at all four orientations`,()=>{
  for(let rot=0;rot<4;rot++)for(const a of [false,true])for(const b of [false,true]){
    const wanted=symbol==='AND'?a&&b:symbol==='OR'?a||b:symbol==='NOT'?!a:symbol==='XOR'?a!==b:symbol==='NAND'?!(a&&b):!(a||b);
    assert.equal(gate(symbol,a,b,rot).m.output,wanted,`${symbol} ${a},${b}, rot ${rot}`);
  }
});
test('gate output cannot backfeed into its independent input wire bus',()=>{
  const {T,sim,put,key}=fixture();put(5,8,T.SIGNAL);put(6,8,T.NOT);put(7,8,T.SIGNAL);step(sim);
  assert.equal(sim.data(key(5,8)).active,false);assert.equal(sim.data(key(7,8)).active,true);
});
test('long combinational chain settles within one logic step',()=>{
  const {T,sim,put,key}=fixture();put(1,8,T.SWITCH,{on:true});for(let x=2;x<=21;x++)put(x,8,T.NOT);step(sim);
  assert.equal(sim.data(key(21,8)).output,true);assert.equal(sim.data(key(21,8)).logicUnstable,false);
});
test('timer requires sustained input and resets immediately when low',()=>{
  const {T,sim,put,key}=fixture();const lever=put(2,3,T.SWITCH,{on:true});put(3,3,T.TIMER,{delay:.25});
  step(sim,4);assert.equal(sim.data(key(3,3)).output,false);step(sim);assert.equal(sim.data(key(3,3)).output,true);
  lever.on=false;step(sim);assert.equal(sim.data(key(3,3)).output,false);lever.on=true;step(sim,4);assert.equal(sim.data(key(3,3)).output,false);
});
test('pulse generator timing feeds a rising-edge counter without level double counting',()=>{
  const {T,sim,put,key}=fixture();put(2,3,T.PULSE,{delay:.25});put(3,3,T.COUNTER,{target:3});
  step(sim,16);assert.equal(sim.data(key(3,3)).count,3);assert.equal(sim.data(key(3,3)).output,true);
});
test('toggle flips only on rising edges, not every tick held high',()=>{
  const {T,sim,put,key}=fixture(),lever=put(2,3,T.SWITCH,{on:true});put(3,3,T.TOGGLE);step(sim,8);assert.equal(sim.data(key(3,3)).output,true);
  lever.on=false;step(sim);lever.on=true;step(sim);assert.equal(sim.data(key(3,3)).output,false);
});
test('latch holds state with reset priority',()=>{
  const {T,sim,put,key}=fixture(),set=put(2,3,T.SWITCH,{on:true}),reset=put(3,2,T.SWITCH,{rot:1});put(3,3,T.LATCH);
  step(sim);set.on=false;step(sim);assert.equal(sim.data(key(3,3)).output,true);
  set.on=reset.on=true;step(sim);assert.equal(sim.data(key(3,3)).output,false);
});
test('counter reset and disabled clock produce no phantom pulses',()=>{
  const {T,sim,put,key}=fixture(),en=put(1,3,T.SWITCH,{on:false});put(2,3,T.PULSE,{delay:.25});put(3,3,T.COUNTER,{target:2});const reset=put(3,2,T.SWITCH,{rot:1});
  step(sim,20);assert.equal(sim.data(key(3,3)).count,0);en.on=true;step(sim,11);assert.equal(sim.data(key(3,3)).count,2);
  reset.on=true;step(sim);assert.equal(sim.data(key(3,3)).count,0);assert.equal(sim.data(key(3,3)).output,false);
});
function drive(){const f=fixture();f.put(1,8,f.T.GENERATOR);f.put(2,8,f.T.MOTOR);return f}
test('motor accepts rear power only; control wire on wrong side does not enable it',()=>{
  const {T,sim,put,key}=fixture();put(2,7,T.GENERATOR);put(2,8,T.MOTOR);step(sim);assert.equal(sim.data(key(2,8)).running,false);
  put(1,8,T.GENERATOR);step(sim);assert.equal(sim.data(key(2,8)).running,true);
  const lever=put(2,7,T.SWITCH,{rot:1,on:false});step(sim);assert.equal(sim.data(key(2,8)).running,false);lever.on=true;step(sim);assert.equal(sim.data(key(2,8)).running,true);
});
test('gear ratios conserve speed × torque downstream and do not alter upstream values',()=>{
  const {T,sim,put,key}=drive();put(3,8,T.GEAR,{mode:0});put(4,8,T.SHAFT);step(sim);
  assert.equal(sim.data(key(2,8)).torque,12);assert.equal(sim.data(key(4,8)).torque,24);assert.equal(sim.data(key(4,8)).speed,.5);
  put(5,8,T.SMALL_GEAR);put(6,8,T.SHAFT);step(sim);assert.equal(sim.data(key(6,8)).torque,12);assert.equal(sim.data(key(6,8)).speed,-1);
});
test('clutch completely disconnects downstream rotation',()=>{
  const {T,sim,put,key}=drive();put(3,8,T.CLUTCH);put(4,8,T.PISTON);const en=put(3,7,T.SWITCH,{rot:1,on:false});step(sim);assert.equal(sim.data(key(4,8)).torque,0);
  en.on=true;step(sim);assert.equal(sim.data(key(4,8)).torque,12);en.on=false;step(sim);assert.equal(sim.data(key(4,8)).running,false);
});
test('bevel drives only its 90-degree output',()=>{
  const {T,sim,put,key}=drive();put(3,8,T.BEVEL);put(3,9,T.SHAFT,{rot:1});put(4,8,T.SHAFT);step(sim);
  assert.equal(sim.data(key(3,9)).running,true);assert.equal(sim.data(key(4,8)).running,false);
});
test('belt reaches a facing pulley but stops at an obstruction or the distance limit',()=>{
  const {T,sim,put,key}=drive();put(3,8,T.BELT);put(9,8,T.PULLEY);put(10,8,T.SHAFT);step(sim);assert.equal(sim.data(key(10,8)).torque,12);
  put(5,8,T.STONE);step(sim);assert.equal(sim.data(key(10,8)).running,false);put(5,8,T.AIR);put(9,8,T.AIR);put(10,8,T.AIR);put(11,8,T.PULLEY);step(sim);assert.equal(sim.data(key(11,8)).running,false);
});
test('power overload exposes exact capacity and stops machinery',()=>{
  const {T,sim,put,key}=fixture();put(1,8,T.GENERATOR);put(2,8,T.WIRE);put(3,8,T.CORE);put(2,9,T.WIRE);put(3,9,T.MOTOR);step(sim);
  assert.equal(sim.data(key(3,9)).load,11);assert.equal(sim.data(key(3,9)).capacity,10);assert.equal(sim.data(key(3,9)).overload,true);assert.equal(sim.data(key(3,9)).running,false);
});
test('unchanged topology never rescans world or rebuilds graphs on simulation ticks',()=>{
  const {T,sim,put}=drive();put(3,8,T.PISTON);step(sim);const count=sim.rebuilds;
  step(sim,100);assert.equal(sim.rebuilds,count);put(4,8,T.SHAFT);step(sim);assert.equal(sim.rebuilds,count+1);
});
test('touching selections retain independent assembly IDs and exact masses',()=>{
  const {T,sim,put,key}=fixture();put(2,4,T.WOOD);put(3,4,T.IRON_BLOCK);const a=sim.link([key(2,4)]),b=sim.link([key(3,4)]);
  assert.notEqual(a,b);assert.equal(sim.group(2,4).length,1);assert.equal(sim.structureInfo(a).mass,1);assert.equal(sim.structureInfo(b).mass,7);
  sim.link([key(3,4)],'add',a);assert.equal(sim.group(2,4).length,2);assert.equal(sim.structureInfo(a).mass,8);
});
test('removing a connection splits the remaining structure without merging adjacent assemblies',()=>{
  const {T,sim,put,key}=fixture();for(let x=2;x<5;x++)put(x,4,T.WOOD);sim.link([2,3,4].map(x=>key(x,4)));sim.link([key(3,4)],'remove');
  assert.notEqual(sim.data(key(2,4)).assembly,sim.data(key(4,4)).assembly);assert.equal(sim.data(key(3,4)).linked,false);
});
test('legacy linked saves migrate and next IDs never collide after restoring',()=>{
  const {T,sim,put,key}=fixture();put(2,4,T.WOOD,{linked:true});put(3,4,T.WOOD,{linked:true});put(6,4,T.WOOD,{linked:true});sim.reset();sim.rebuild();
  assert.equal(sim.data(key(2,4)).assembly,sim.data(key(3,4)).assembly);assert.notEqual(sim.data(key(2,4)).assembly,sim.data(key(6,4)).assembly);
  sim.restore({nextAssembly:99});assert.equal(sim.nextAssembly,99);
});
test('extreme repeated stress detaches without deleting materials',()=>{
  const {T,sim,put,key,world}=fixture();put(2,4,T.WOOD);sim.link([key(2,4)]);sim.applyStress([[2,4]],30);sim.applyStress([[2,4]],30);
  assert.equal(sim.applyStress([[2,4]],30),key(2,4));assert.equal(world[key(2,4)],T.WOOD);assert.equal(sim.data(key(2,4)).linked,false);
});
test('dropped items enter hopper and preserve exact counts through storage',()=>{
  const {T,sim,put,key}=fixture();put(2,4,T.HOPPER);put(3,4,T.STORAGE);sim.spawn('iron',5,2.5,3.5);
  for(let i=0;i<50;i++)sim.logistics(.05);assert.equal(sim.stored(key(3,4)),5);assert.equal(sim.drops.length,0);
});
test('splitter alternates distinct physical items to facing outputs',()=>{
  const {T,sim,put,key}=fixture();put(2,4,T.SPLITTER,{contents:{iron:4}});put(3,4,T.STORAGE);put(2,5,T.STORAGE,{rot:1});
  for(let i=0;i<25;i++)sim.logistics(.05);assert.equal(sim.stored(key(3,4)),2);assert.equal(sim.stored(key(2,5)),2);
});
test('a full selected splitter output blocks without rerouting or losing resources',()=>{
  const {T,sim,put,key}=fixture();put(2,4,T.SPLITTER,{contents:{iron:3}});put(3,4,T.STORAGE,{contents:{stone:64}});put(2,5,T.STORAGE,{rot:1});
  for(let i=0;i<25;i++)sim.logistics(.05);assert.equal(sim.stored(key(2,4)),3);assert.equal(sim.stored(key(2,5)),0);assert.match(sim.data(key(2,4)).logisticsFault,/BLOCKED/);
});
test('filter refuses incompatible material and does not lose queued goods after reconfiguration',()=>{
  const {T,sim,put,key}=fixture();const f=put(2,4,T.FILTER,{filter:'iron'});put(3,4,T.STORAGE);assert.equal(sim.insert(key(2,4),'copper',3),0);assert.equal(sim.insert(key(2,4),'iron',3),3);
  f.filter='copper';sim.logistics(.25);assert.equal(sim.stored(key(2,4)),3);assert.equal(sim.stored(key(3,4)),0);
});
test('unpowered conveyor buffers actual items and resumes after shaft power arrives',()=>{
  const {T,sim,put,key}=fixture();put(4,4,T.CONVEYOR);put(5,4,T.STORAGE);sim.insert(key(4,4),'copper',3);step(sim);sim.logistics(.25);assert.equal(sim.stored(key(4,4)),3);
  put(2,4,T.GENERATOR);put(3,4,T.MOTOR);step(sim);sim.logistics(.25);assert.equal(sim.stored(key(5,4)),1);assert.equal(sim.stored(key(4,4)),2);
});
test('an item can move at most one component per tick, including same-key stacks',()=>{
  const {T,sim,put,key}=fixture();put(2,4,T.HOPPER,{contents:{iron:2}});put(3,4,T.HOPPER);put(4,4,T.STORAGE);sim.logistics(.25);
  assert.equal(sim.stored(key(3,4)),1);assert.equal(sim.stored(key(4,4)),0);sim.logistics(.25);assert.equal(sim.stored(key(4,4)),1);
});
test('storage extraction needs a facing hopper and cannot silently leak goods',()=>{
  const {T,sim,put,key}=fixture();put(2,4,T.STORAGE,{contents:{iron:2}});put(3,4,T.FILTER);sim.logistics(.25);assert.equal(sim.stored(key(2,4)),2);assert.equal(sim.stored(key(3,4)),0);
  put(3,4,T.HOPPER);sim.logistics(.25);assert.equal(sim.stored(key(2,4)),1);assert.equal(sim.stored(key(3,4)),1);
});
test('entity cap refuses new loose items but still merges nearby identical stacks',()=>{
  const {sim}=fixture();for(let i=0;i<512;i++)assert.equal(sim.spawn('iron',1,i,1),true);
  assert.equal(sim.spawn('copper',1,1,2),false);assert.equal(sim.spawn('iron',1,0,1),true);assert.equal(sim.drops[0].count,2);
});
test('items save and restore with their position/count and ID allocation',()=>{
  const a=fixture(),b=fixture();a.sim.spawn('copper',7,3.5,2.5);const saved=JSON.parse(JSON.stringify(a.sim.export()));b.sim.restore(saved);
  assert.deepEqual(b.sim.drops,a.sim.drops);b.sim.spawn('iron',1,8,8);assert.notEqual(b.sim.drops[0].id,b.sim.drops[1].id);
});
