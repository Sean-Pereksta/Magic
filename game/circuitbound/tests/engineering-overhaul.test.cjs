const test=require('node:test');
const assert=require('node:assert/strict');
const {fixture,E}=require('./harness.cjs');

function tick(f,n=1,dt=.05){for(let i=0;i<n;i++){f.sim.step(dt);f.sim.production(dt,{set:f.put});f.sim.logistics(dt);}}
function miner(){const f=fixture(),{put,T}=f;
  put(8,2,T.GENERATOR);put(8,3,T.MOTOR,{rot:1});put(8,4,T.DRILL,{rot:1});put(9,4,T.GENERATOR);
  put(7,4,T.HOPPER,{rot:2});put(6,4,T.STORAGE,{rot:2});put(8,5,T.IRON);return f;
}
function production(symbol='SMELTER',m={}){const f=fixture(),{put,T,sim}=f;
  put(8,4,T[symbol],m);put(8,5,T.GENERATOR);put(9,4,T.STORAGE);sim.step();return f;
}

test('new tile IDs reserve original door, ladder and ledge save IDs',()=>{
  const {T}=fixture();assert.equal(T.DRILL,55);assert.equal(T.RECOVERY_STATION,72);
  assert.equal(T.DOOR,52);assert.equal(T.LADDER,53);assert.equal(T.LEDGE,54);
});
test('drill removes a real world ore tile and transfers one conserved resource into storage',()=>{
  const f=miner();tick(f,80);assert.equal(f.get(8,5),f.T.AIR);assert.equal(f.sim.data(f.key(6,4)).contents.iron,1);
  assert.equal(f.sim.metrics.mined,1);assert.equal(f.sim.drops.length,0);
});
test('drill requires both real power and a correctly oriented mechanical drive',()=>{
  const f=miner();f.put(9,4,f.T.AIR);tick(f,80);assert.equal(f.get(8,5),f.T.IRON);
  f.put(9,4,f.T.GENERATOR);f.put(8,3,f.T.MOTOR,{rot:0});tick(f,80);
  assert.equal(f.get(8,5),f.T.IRON);assert.match(f.sim.data(f.key(8,4)).machineFault,/DRIVE/);
});
test('drill output backpressure retains ore and resumes after space becomes available',()=>{
  const f=miner();f.put(7,4,f.T.HOPPER,{rot:2,contents:{iron:8}});f.put(6,4,f.T.STORAGE,{rot:2,contents:{iron:64}});
  tick(f,80);assert.equal(f.get(8,5),f.T.IRON);assert.equal(f.sim.metrics.mined,0);
  f.sim.data(f.key(6,4)).contents={};tick(f,80);assert.equal(f.get(8,5),f.T.AIR);
  assert.equal(f.sim.data(f.key(6,4)).contents.iron,9);
});
test('drill cannot create a resource when world mutation is refused',()=>{
  const f=miner();for(let i=0;i<80;i++){f.sim.step(.05);f.sim.production(.05,{set(){}});f.sim.logistics(.05);}
  assert.equal(f.get(8,5),f.T.IRON);assert.equal(f.sim.metrics.mined,0);assert.equal(f.sim.stored(f.key(6,4)),0);
});
test('mining does not destroy player construction or protected core tiles',()=>{
  const f=miner();f.put(8,5,f.T.CORE);f.put(8,6,f.T.IRON);tick(f,80);
  assert.equal(f.get(8,5),f.T.CORE);assert.equal(f.get(8,6),f.T.IRON);assert.equal(f.sim.metrics.mined,0);
});
test('excavator never skips protected obstructions in its left and right mining rays',()=>{
  const f=fixture(),{T,put,sim,key}=f;put(8,4,T.EXCAVATOR,{rot:1});
  put(7,5,T.BARRIER);put(7,6,T.IRON);put(8,5,T.CORE);put(9,5,T.CORE);put(9,6,T.COPPER);
  assert.equal(sim.miningTarget(key(8,4)),null);
  put(9,5,T.AIR);assert.deepEqual(sim.miningTarget(key(8,4)),{x:9,y:6,id:T.COPPER,key:'copper'});
  put(9,6,T.AIR);put(8,6,T.IRON);assert.equal(sim.miningTarget(key(8,4)),null);
});
test('replacing a mining target resets work instead of transferring cheap dirt progress to obsidian',()=>{
  const f=miner();f.put(8,5,f.T.DIRT);tick(f,8);const m=f.sim.data(f.key(8,4));assert.ok(m.progress>.7);
  f.put(8,5,f.T.OBSIDIAN);tick(f,1);assert.ok(m.progress<.03);assert.equal(f.get(8,5),f.T.OBSIDIAN);
});
test('mining refuses raw terrain that belongs to a linked player structure',()=>{
  const f=miner();f.sim.link([f.key(8,5)]);tick(f,80);assert.equal(f.get(8,5),f.T.IRON);assert.equal(f.sim.metrics.mined,0);
});
test('smelting consumes real ingredients and sends only products into logistics',()=>{
  const f=production('SMELTER',{contents:{crushedIron:2,copper:3}});tick(f,100);
  assert.equal(f.sim.data(f.key(9,4)).contents.ironIngot,2);
  assert.equal(f.sim.data(f.key(8,4)).contents.copper,3);assert.equal(f.sim.data(f.key(8,4)).contents.crushedIron,undefined);
  assert.equal(f.sim.metrics.produced,2);
});
test('a connected motor, crusher and smelter line conserves ore through both inventories',()=>{
  const f=fixture(),{put,T,sim,key}=f;
  put(8,2,T.GENERATOR);put(8,3,T.MOTOR,{rot:1});put(8,4,T.CRUSHER,{recipe:'crushedIron'});
  put(8,5,T.GENERATOR);put(7,4,T.HOPPER,{contents:{iron:5}});put(9,4,T.SMELTER,{recipe:'ironIngot'});
  put(9,5,T.GENERATOR);put(10,4,T.STORAGE);tick(f,300);
  assert.equal(sim.data(key(10,4)).contents.ironIngot,5);assert.equal(sim.metrics.produced,10);
  assert.equal(sim.stored(key(7,4))+sim.stored(key(8,4))+sim.stored(key(9,4)),0);assert.equal(sim.drops.length,0);
});
test('manufacturing waits for space without consuming inputs or accumulating production',()=>{
  const f=production('AMMO_FABRICATOR',{contents:{ironIngot:2},outputs:{ammo:16}});
  f.put(9,4,f.T.STORAGE,{contents:{stone:64}});tick(f,100);
  const m=f.sim.data(f.key(8,4));assert.equal(m.contents.ironIngot,2);assert.equal(m.outputs.ammo,16);assert.equal(m.progress||0,0);
  f.sim.data(f.key(9,4)).contents={};tick(f,90);assert.equal(m.contents.ironIngot,undefined);
  assert.equal((m.outputs.ammo||0)+(f.sim.data(f.key(9,4)).contents.ammo||0),24);
});
test('a disconnected producer retains products and eventually stops without spilling output into air',()=>{
  const f=production('AMMO_FABRICATOR',{contents:{ironIngot:8}});f.put(9,4,f.T.AIR);tick(f,300);
  const m=f.sim.data(f.key(8,4));assert.equal(m.outputs.ammo,16);assert.equal(m.contents.ironIngot,4);assert.equal(f.sim.drops.length,0);
  assert.match(m.machineFault,/FULL/);
});
test('production pauses under a real low enable signal and resumes when high',()=>{
  const f=production('SMELTER',{contents:{crushedIron:1}}),lever=f.put(8,3,f.T.SWITCH,{rot:1,on:false});
  tick(f,70);assert.equal(f.sim.data(f.key(8,4)).contents.crushedIron,1);
  lever.on=true;tick(f,70);assert.equal(f.sim.data(f.key(9,4)).contents.ironIngot,1);
});
test('recipe changes reset progress and cannot substitute cheap in-progress inputs',()=>{
  const f=production('SMELTER',{contents:{crushedIron:1,crushedCopper:1}});tick(f,25);
  const m=f.sim.data(f.key(8,4));assert.ok(m.progress>.5);m.recipe='copperIngot';tick(f,10);
  assert.equal(m.contents.crushedIron,1);assert.equal(m.contents.crushedCopper,1);assert.ok(m.progress<.3);
});
test('battery stores surplus and spends only stored energy after generators disconnect',()=>{
  const f=fixture(),{put,T,sim,key}=f;put(3,4,T.GENERATOR);put(4,4,T.BATTERY);put(5,4,T.LAMP);
  for(let i=0;i<20;i++)sim.step(.05);const energy=sim.data(key(4,4)).energy;assert.ok(energy>3.9);
  put(3,4,T.AIR);sim.step(.05);assert.equal(sim.data(key(5,4)).powered,true);assert.ok(sim.data(key(4,4)).energy<energy);
  for(let i=0;i<120;i++)sim.step(.05);assert.equal(sim.data(key(5,4)).powered,false);assert.ok(sim.data(key(4,4)).energy<1e-6);
});
test('battery charging respects cable capacity and reports charging as actual grid load',()=>{
  const f=fixture(),{T,put,sim,key}=f;put(2,4,T.GENERATOR);put(3,4,T.WIRE);put(4,4,T.BATTERY);put(5,4,T.CORE);
  put(4,3,T.LAMP);put(4,5,T.LAMP,{rot:2});
  for(let i=0;i<20;i++)sim.step(.05);const m=sim.data(key(4,4));assert.equal(m.capacity,10);assert.equal(m.load,10);
  assert.equal(m.batteryFlow,2);assert.ok(Math.abs(m.energy-2)<1e-7);assert.equal(m.overload,false);
});
test('zero-time network inspection neither removes power from a charged battery nor changes stored energy',()=>{
  const f=fixture(),{T,put,sim,key}=f;put(4,4,T.BATTERY,{energy:20});put(5,4,T.LAMP);sim.step(0);
  assert.equal(sim.data(key(5,4)).powered,true);assert.equal(sim.data(key(4,4)).energy,20);
});
test('disabled batteries do not charge, discharge, or supply machinery',()=>{
  const f=fixture(),{T,put,sim,key}=f;put(4,4,T.BATTERY,{energy:20,disabled:true});put(5,4,T.LAMP);sim.step(.05);
  assert.equal(sim.data(key(5,4)).powered,false);assert.equal(sim.data(key(4,4)).energy,20);
});
test('battery brownouts preserve stored energy when a whole simulation interval cannot be powered',()=>{
  const f=fixture(),{T,put,sim,key}=f;put(4,4,T.BATTERY,{energy:.01});put(5,4,T.LAMP);sim.step(.05);
  assert.equal(sim.data(key(5,4)).powered,false);assert.equal(sim.data(key(4,4)).energy,.01);
});
test('disabled motors and disabled transmission components cannot power a drill',()=>{
  const f=miner();f.sim.data(f.key(8,3)).disabled=true;tick(f,80);assert.equal(f.get(8,5),f.T.IRON);
  f.sim.data(f.key(8,3)).disabled=false;f.put(8,4,f.T.SHAFT,{rot:1,disabled:true});f.put(8,5,f.T.DRILL,{rot:1});f.put(9,5,f.T.GENERATOR);
  f.put(7,5,f.T.HOPPER,{rot:2});f.put(8,6,f.T.IRON);tick(f,80);assert.equal(f.get(8,6),f.T.IRON);
});
test('turret consumes one ammunition round per real cooldown-limited hit and stops without power',()=>{
  const f=fixture(),{T,sim,put,key}=f;put(8,4,T.TURRET,{contents:{ammo:2}});put(8,5,T.GENERATOR);sim.step();
  const target={x:11,y:4.5,health:100},damage=(t,n)=>{t.health-=n};
  sim.defenses(.05,{targets:[target],damage});assert.equal(target.health,86);assert.equal(sim.data(key(8,4)).contents.ammo,1);
  sim.defenses(.05,{targets:[target],damage});assert.equal(target.health,86);
  sim.defenses(1,{targets:[target],damage});assert.equal(target.health,72);assert.equal(sim.data(key(8,4)).contents.ammo,undefined);
  assert.equal(sim.insert(key(8,4),'iron',1),0);sim.insert(key(8,4),'ammo',1);put(8,5,T.AIR);sim.step();sim.defenses(1,{targets:[target],damage});assert.equal(target.health,72);
});
test('enemy sensor drives signal wire and powers an automated door through typed ports',()=>{
  const f=fixture(),{T,sim,put,key}=f;put(8,2,T.ENEMY_SENSOR,{rot:1});put(8,3,T.SIGNAL);put(8,4,T.AUTO_DOOR);put(8,5,T.GENERATOR);
  sim.defenses(.05,{targets:[{x:10,y:4,health:100}]});sim.step();sim.defenses(.05);assert.equal(sim.data(key(8,4)).open,true);
  sim.step();sim.defenses(.05);assert.equal(sim.data(key(8,4)).open,false);
});
test('an attached low control wire prevents turret ammunition consumption until detection goes high',()=>{
  const f=fixture(),{T,sim,put,key}=f;put(8,2,T.ENEMY_SENSOR,{rot:1});put(8,3,T.SIGNAL);
  put(8,4,T.TURRET,{contents:{ammo:3}});put(8,5,T.GENERATOR);const target={x:10,y:4.5,health:100};
  const damage=(t,n)=>t.health-=n;sim.step();sim.defenses(.05,{targets:[target],damage});assert.equal(target.health,100);
  assert.equal(sim.data(key(8,4)).contents.ammo,3);sim.step();sim.defenses(.05,{targets:[target],damage});
  assert.equal(target.health,86);assert.equal(sim.data(key(8,4)).contents.ammo,2);
});
test('line of sight blocks turret shots and preserves ammunition',()=>{
  const f=fixture(),{T,sim,put,key}=f;put(8,4,T.TURRET,{contents:{ammo:2}});put(8,5,T.GENERATOR);sim.step();
  let hits=0;sim.defenses(.05,{targets:[{x:10,y:4,health:100}],damage(){hits++},lineOfSight(){return false}});
  assert.equal(hits,0);assert.equal(sim.data(key(8,4)).contents.ammo,2);
});
test('powered tesla area bursts hit nearby enemies and exclude distant targets',()=>{
  const f=fixture(),{T,sim,put}=f;put(8,4,T.TESLA_COIL);put(8,5,T.GENERATOR);sim.step();
  const near={x:10,y:4.5,health:100},alsoNear={x:8,y:7,health:100},far={x:20,y:4.5,health:100};
  sim.defenses(.05,{targets:[near,alsoNear,far],damage(t,n){t.health-=n;}});
  assert.equal(near.health,82);assert.equal(alsoNear.health,82);assert.equal(far.health,100);
});
test('recovery station requires power and enforces its healing cooldown',()=>{
  const f=fixture(),{T,sim,put}=f;put(8,4,T.RECOVERY_STATION);let health=50;
  const heal=(x,y,n)=>{assert.equal(x,8.5);health+=n;return n};sim.step();sim.defenses(.05,{heal});assert.equal(health,50);
  put(8,5,T.GENERATOR);sim.step();sim.defenses(.05,{heal});sim.defenses(.05,{heal});assert.equal(health,54);
  sim.defenses(1,{heal});assert.equal(health,58);assert.equal(sim.metrics.healed,8);
});
test('disabled recovery stations and doors remain inactive even when the net has power',()=>{
  const f=fixture(),{T,sim,put,key}=f;put(8,4,T.RECOVERY_STATION,{disabled:true});put(8,5,T.GENERATOR);let healed=0;
  sim.step();sim.defenses(.05,{heal(){healed++;return 4}});assert.equal(healed,0);
  put(8,4,T.AUTO_DOOR,{disabled:true});put(8,3,T.SWITCH,{rot:1,on:true});sim.step();sim.defenses(.05);
  assert.equal(sim.data(key(8,4)).open,false);
});
test('automated doors wait until an occupied doorway clears before closing under a low signal',()=>{
  const f=fixture(),{T,sim,put,key}=f;put(8,4,T.AUTO_DOOR,{open:true});put(8,5,T.GENERATOR);sim.step();
  sim.defenses(.05,{canClose(x,y){assert.equal(x,8);assert.equal(y,4);return false;}});
  assert.equal(sim.data(key(8,4)).open,true);assert.match(sim.data(key(8,4)).defenseFault,/DOORWAY/);
  sim.defenses(.05,{canClose(){return true;}});assert.equal(sim.data(key(8,4)).open,false);assert.equal(sim.data(key(8,4)).defenseFault,'');
});
test('smart placement orients one unique compatible output while preserving manual rotation for ties',()=>{
  const f=fixture(),{T,sim,put}=f;put(7,4,T.MOTOR);assert.equal(sim.smartRotation(8,4,T.SHAFT,2),0);
  assert.equal(sim.smartRotation(15,15,T.SHAFT,2),2);
});
test('diagnostics expose actual ingredients, finished goods, power, progress and actionable faults',()=>{
  const f=production('SMELTER',{contents:{crushedIron:1},outputs:{ironIngot:2}});f.sim.production(.05);const d=f.sim.diagnose(f.key(8,4));
  assert.equal(d.powered,true);assert.equal(d.load,4);assert.equal(d.recipe.key,'ironIngot');assert.equal(d.contents.crushedIron,1);assert.equal(d.outputs.ironIngot,2);
  assert.equal(d.connections.find(c=>c.type==='item').count,1);assert.equal(typeof E.MACHINES,'object');
});
