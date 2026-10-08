'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),{loadEngine}=require('./performance-harness.cjs');
function arena(){const c=loadEngine();if(!c.ATSGame.prototype.nearestTargetFor)vm.runInContext(fs.readFileSync(path.join(__dirname,'../nearest-orders.js'),'utf8'),c);const g=new c.ATSGame('NEAREST-ORDERS');g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;g.king.x=-400;const a=g.makeApe(0,0,'follow');a.species='gorilla';g.siege.balance(a,true);return{c,g,a}}
function object(g,type,x,extra={}){const o={id:'object-'+g.nextId++,type,x,y:0,r:10,hp:200,maxHp:200,solid:true,...extra};g.world.objects.set(o.id,o);g.world._indexObject(o);return o}
function vehicle(g,kind,x,y=0){const v=g.forces.initVehicle({id:'vehicle-'+g.nextId++,x,y,dir:0,state:'idle'},kind,0);g.vehicles.push(v);return v}
function grids(g){g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);g.vehicleGrid.rebuild(g.vehicles);g.navigation.beginFrame(g.time);g.performance.beginStep(g)}
function step(g,frames=1){for(let i=0;i<frames;i++){g.time+=1/30;grids(g);g.siege.tick(1/30);for(const a of g.apes)if(a.hp>0)g.updateApe(a,1/30)}}
test('tap and hold share the full charge scan radius in every direction',()=>{
 const {g,a}=arena(),h=g.makeHuman(0,0,null);
 for(const angle of [0,Math.PI/4,Math.PI/2,Math.PI,Math.PI*1.5])for(const radius of [321,500,569.99,570.01]){
  h.x=Math.cos(angle)*radius;h.y=Math.sin(angle)*radius;grids(g);
  for(const humansOnly of [false,true]){g.command(humansOnly?'nearestHuman':'nearestTarget',{x:Math.cos(angle),y:Math.sin(angle)});assert.equal(g.nearestTargetFor(a,humansOnly)?.id??null,radius<=570?h.id:null,`radius ${radius}, angle ${angle}, humansOnly ${humansOnly}`);}
 }
 h.x=570;h.y=0;g.command('nearestHuman',{x:1,y:0});grids(g);
 assert.equal(g.nearestTargetFor(a,false).id,h.id);assert.equal(g.nearestTargetFor(a,true).id,h.id);
});
test('both E orders pursue distant humans and drop targets beyond the shared limit',()=>{
 for(const command of ['nearestTarget','nearestHuman']){
  const {g,a}=arena(),h=g.makeHuman(570,0,null);grids(g);g.command(command);step(g);
  assert.equal(a._nearestTarget?.id,h.id);assert.ok(a.x>0,`${command} must approach a human beyond the old scan range`);
  h.x=a.x+571;h.y=a.y;grids(g);const x=a.x;g.updateApe(a,1/30);
  assert.equal(a._nearestTarget,null);assert.ok(a.x>x,'resume directional charge when the target leaves range');
 }
});
test('tap selects actual nearest hostile target across humans, vehicles, structures and occupied cages',()=>{const {g,a}=arena(),human=g.makeHuman(95,0,null),cage=object(g,'cage',55,{count:4}),vehicle={id:'vehicle-test',x:75,y:0,hp:200,maxHp:200,kind:'jeep'};g.vehicles.push(vehicle);object(g,'tree',12);object(g,'apeBuilding',17);object(g,'cage',20,{count:0});object(g,'gate',23,{solid:false});grids(g);assert.equal(g.nearestTargetFor(a).id,cage.id);cage.dead=true;assert.equal(g.nearestTargetFor(a).id,vehicle.id);vehicle.hp=0;grids(g);assert.equal(g.nearestTargetFor(a).id,human.id);const gate=object(g,'gate',35);assert.equal(g.nearestTargetFor(a).id,gate.id)});
test('held command attacks nearby infantry ahead of a farther vehicle and leaves structures alone',()=>{const {g,a}=arena(),cage=object(g,'cage',0,{y:45,count:2}),v=vehicle(g,'jeep',85);const far=g.makeHuman(150,0,null),near=g.makeHuman(28,0,null);grids(g);assert.ok(g.command('nearestHuman'));step(g,6);assert.equal(a._nearestTarget.id,near.id);assert.ok(near.hp<near.maxHp);assert.equal(far.hp,far.maxHp);assert.equal(cage.hp,cage.maxHp);assert.equal(v.hp,v.maxHp)});
test('held combat-target pathfinding never breaches a blocking wall or fortification',()=>{const {g,a}=arena(),wall=object(g,'wall',48,{w:20,h:140,collision:'rect',faction:'human',wallTier:3,climbable:false,fortification:true,team:'human'}),h=g.makeHuman(110,0,null);grids(g);g.command('nearestHuman');step(g,330);assert.equal(wall.hp,wall.maxHp);assert.ok(h.hp<h.maxHp,'apes route around the wall to the human');assert.equal(a.wallClimb,undefined);assert.equal(a.siegeTransition,undefined)});
test('held command charges when no humans or vehicles remain and never redirects to structures',()=>{const {g,a}=arena(),cage=object(g,'cage',25,{count:2});grids(g);g.command('nearestHuman');step(g,90);assert.equal(cage.hp,200);assert.ok(a.x>25);assert.equal(a._nearestTarget,null)});
test('both gestures ignore closer targets behind or outside the charge direction',()=>{
 for(const command of ['nearestTarget','nearestHuman']){
  const {g,a}=arena();g.makeHuman(-10,0,null);g.makeHuman(5,45,null);const h=g.makeHuman(450,0,null);
  g.vehicles.push({id:'vehicle-behind',x:-15,y:0,hp:200,maxHp:200,kind:'jeep'});object(g,'tower',-20);
  grids(g);g.command(command,{x:7,y:0});step(g);assert.equal(a._nearestTarget?.id,h.id);assert.ok(a.x>0);
 }
});
test('direction is fixed at issue time and both modes resume the charge after a kill',()=>{
 for(const command of ['nearestTarget','nearestHuman']){
  const {g,a}=arena(),h=g.makeHuman(0,-100,null);grids(g);g.command(command,{x:0,y:-9});g.aim={x:1,y:0};step(g);
  assert.equal(a._nearestTarget?.id,h.id);assert.ok(a.y<0);assert.equal(a.nearestOrder.goal.x,0);assert.equal(a.nearestOrder.goal.y,-570);
  h.hp=0;const y=a.y;step(g);assert.ok(a.y<y);assert.equal(a._nearestTarget,null);
  step(g,390);assert.equal(a.state,'hold');assert.equal(a.nearestOrder,undefined);
 }
});
test('saved directional charges retain their heading and destination after loading',()=>{
 const {c,g,a}=arena();g.command('nearestHuman',{x:0,y:1});const saved=JSON.parse(JSON.stringify(g.serialize()));
 const loaded=c.ATSGame.fromJSON(saved),copy=loaded.apes.find(x=>x.id===a.id);loaded.aim={x:-1,y:0};
 assert.equal(copy.nearestOrder.dx,0);assert.equal(copy.nearestOrder.dy,1);assert.equal(copy.nearestOrder.goal.y,570);
 loaded.world.waterBlocked=()=>false;loaded.world.terrain=()=>({biome:'forest',water:false});loaded.world.objects.clear();loaded.world._spatial.clear();
 grids(loaded);loaded.updateApe(copy,1/30);assert.ok(copy.y>0);
});
test('commands affect selected travelers only and leave villagers and unselected divisions alone',()=>{const {g,a}=arena(),b=g.makeApe(0,50,'follow'),resident=g.makeApe(0,70,'settled','home');b.species='gibbon';g.siege.balance(b,true);g.champions.veteran(b);g.champions.assign('climbers',b.id,[b.id]);g.siege.selectUnits([a.id]);grids(g);g.command('nearestTarget');assert.equal(a.nearestOrder.kind,'all');assert.equal(b.nearestOrder,undefined);assert.equal(resident.nearestOrder,undefined);assert.equal(g.champions.divisions.climbers.suspended,false);assert.ok(g.champions.members.has(b.id))});
test('nearest orders retarget dead humans, expire after thirteen seconds, and yield to recall',()=>{const {g,a}=arena(),first=g.makeHuman(25,0,null),next=g.makeHuman(55,0,null);grids(g);g.command('nearestHuman');step(g);first.hp=0;step(g,10);assert.equal(a._nearestTarget.id,next.id);g.time=14;step(g);assert.equal(a.nearestOrder,undefined);assert.equal(a.state,'hold');g.commandCD=0;g.command('nearestTarget');assert.ok(a.nearestOrder);g.commandCD=0;g.siege.selected=[];g.command('recallField');assert.equal(a.nearestOrder,undefined);assert.equal(a.state,'follow')});
test('active orders survive save/load with original expiry and capuchins preserve exact target choice',()=>{const {c,g,a}=arena();a.species='capuchin';g.siege.balance(a,true);const cage=object(g,'cage',70,{count:2}),h=g.makeHuman(150,0,null);grids(g);g.command('nearestTarget');step(g);assert.equal(a._nearestTarget.id,cage.id);assert.equal(a.throwWindup.targetId,cage.id);const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),copy=loaded.apes.find(x=>x.id===a.id);assert.equal(copy.nearestOrder.until,13);assert.equal(copy.nearestOrder.kind,'all');assert.equal(h.hp,h.maxHp)});
test('round-robin planning reaches every ape in a thousand-ape order within bounded budgets',()=>{const {g}=arena();for(let i=1;i<1000;i++)g.makeApe(i%10,Math.floor(i/10),'follow');const h=g.makeHuman(150,50,null);h.hp=h.maxHp=1e8;grids(g);g.command('nearestHuman');let highest=0;for(let frame=0;frame<100;frame++){g.time+=1/30;grids(g);g.planNearestOrders();highest=Math.max(highest,g.performance.counters.aiThinks)}assert.ok(highest<=12);assert.equal(g.apes.filter(a=>a._nearestTarget?.id===h.id).length,1000)});
test('a hold replaces a recent tap immediately even during feedback cooldown',()=>{const {g,a}=arena(),tower=object(g,'tower',26),h=g.makeHuman(70,0,null);grids(g);assert.equal(g.nearestTargetFor(a).id,tower.id);assert.equal(g.command('nearestTarget'),true);assert.ok(g.commandCD>0);assert.equal(g.command('nearestHuman'),true);assert.equal(a.nearestOrder.kind,'human');step(g,30);assert.equal(a._nearestTarget.id,h.id);assert.equal(tower.hp,200)});
test('nearest target scoring prefers an accessible bank over a shorter straight line across a river',()=>{
 const {g,a}=arena();delete g.world.terrain;delete g.world.waterBlocked;
 const river=g.world._riverInfo(944,1320);a.x=944;a.y=river.centerY-120;
 const across=g.makeHuman(944,river.centerY+120,null),same=g.makeHuman(1204,river.centerY-80,null);
 assert.equal(g.world.landRegionAt(a.x,a.y),g.world.landRegionAt(same.x,same.y));assert.notEqual(g.world.landRegionAt(a.x,a.y),g.world.landRegionAt(across.x,across.y));
 assert.ok(Math.hypot(across.x-a.x,across.y-a.y)<Math.hypot(same.x-a.x,same.y-a.y));
 grids(g);g.command('nearestHuman',{x:1,y:1});
 const stats=JSON.stringify(g.navigation.stats),pending=g.navigation.pending.size,actor=JSON.stringify(a);
 g.navigation.findPath=()=>{throw Error('target scoring must not request A*')};g.navigation.clearSegment=()=>{throw Error('target scoring must not add collision scans')};
 for(let i=0;i<30;i++)assert.equal(g.nearestTargetFor(a,true).id,same.id);
 assert.equal(g.navigation.pending.size,pending);assert.equal(JSON.stringify(g.navigation.stats),stats);assert.equal(JSON.stringify(a),actor);
 same.hp=0;grids(g);assert.equal(g.nearestTargetFor(a,true).id,across.id,'a detour does not make the remaining reachable enemy ineligible');
});
test('nearest selection reads valid cached detours and failures without adding path requests',()=>{
 const {g,a}=arena(),near=g.makeHuman(80,0,null),far=g.makeHuman(150,0,null);grids(g);g.command('nearestHuman',{x:1,y:0});
 const n=g.navigation,key=[0,0,Math.round(near.x/n.cell),0,a.radius||10,n.revision,'ape'].join(':'),route={time:n.time,path:[{x:0,y:0},{x:0,y:200},{x:80,y:200},{x:80,y:0}]};n.routes.set(key,route);
 const requested=n.stats.requested;n.findPath=()=>{throw Error('cached scoring must remain read-only')};
 assert.equal(g.nearestTargetFor(a,true).id,far.id,'known wall detour is more expensive');
 route.path._navInvalid=true;assert.equal(g.nearestTargetFor(a,true).id,near.id,'a gate change invalidates the old cost');
 route.path=[];assert.equal(g.nearestTargetFor(a,true).id,far.id,'recent proven failure selects an alternative');
 route.time=n.time-2;assert.equal(g.nearestTargetFor(a,true).id,near.id,'expired failure is eligible again');
 assert.equal(n.stats.requested,requested);assert.equal(n.pending.size,0);
});
test('target scoring keeps its six-candidate cap and preserves explicit combat targets',()=>{
 const {g,a}=arena();for(let i=0;i<40;i++)g.makeHuman(60+i,0,null);for(let i=0;i<40;i++)object(g,'tower',110+i);grids(g);g.command('nearestTarget',{x:1,y:0});
 let calls=0;g.world.crossingFor=()=>{calls++;return null};g.navigation.findPath=()=>{throw Error('unexpected path request')};
 const explicit={kind:'human',id:g.humans.at(-1).id};a._nearestTarget=explicit;g.nearestTargetFor(a);assert.ok(calls<=6);assert.equal(a._nearestTarget,explicit,'scoring does not mutate active/explicit orders');
});
test('held E ranks infantry and armor by the same distance and excludes closer structures',()=>{
 const {g,a}=arena(),h=g.makeHuman(90,0,null),tank=vehicle(g,'tank',60);
 const structures=['cage','gate','tower','barracks','depot'].map((kind,i)=>object(g,kind,15+i*5,{count:3}));grids(g);g.command('nearestHuman',{x:1,y:0});
 assert.equal(g.nearestTargetFor(a,true).id,tank.id);
 h.x=45;grids(g);assert.equal(g.nearestTargetFor(a,true).id,h.id,'nearer infantry wins over armor');
 tank.x=35;grids(g);assert.equal(g.nearestTargetFor(a,true).id,tank.id,'nearer armor wins over infantry');
 h.hp=tank.hp=0;grids(g);assert.equal(g.nearestTargetFor(a,true),null);for(const o of structures)assert.equal(o.hp,o.maxHp);
});
test('held E actually damages both patrol vehicles and military tanks without harming structures',()=>{
 for(const kind of ['jeep','tank']){
  const {g,a}=arena(),v=vehicle(g,kind,36),h=g.makeHuman(160,0,null),cage=object(g,'cage',0,{y:60,count:3});grids(g);g.command('nearestHuman',{x:1,y:0});step(g,8);
  assert.equal(a._nearestTarget?.id,v.id);assert.equal(a._nearestTarget?.kind,'vehicle');assert.ok(v.hp<v.maxHp,kind+' must take actual combat damage');assert.equal(h.hp,h.maxHp);assert.equal(cage.hp,cage.maxHp);
 }
});
test('held E retargets from infantry to armor and back as targets die',()=>{
 const {g,a}=arena(),first=g.makeHuman(26,0,null),tank=vehicle(g,'tank',55),last=g.makeHuman(95,0,null);object(g,'tower',0,{y:65});grids(g);g.command('nearestHuman',{x:1,y:0});step(g,2);assert.equal(a._nearestTarget?.id,first.id);
 first.hp=0;for(let frame=0;frame<90&&tank.hp===tank.maxHp;frame++)step(g);assert.equal(a._nearestTarget?.id,tank.id);assert.ok(tank.hp<tank.maxHp);
 tank.hp=0;step(g,30);assert.equal(a._nearestTarget?.id,last.id);assert.ok(last.hp<last.maxHp);assert.equal(a.nearestOrder.kind,'human','legacy saved discriminator remains compatible');
});
test('held E uses the same 570-unit range and directional cone for vehicles and tanks',()=>{
 const {g,a}=arena(),v=vehicle(g,'tank',0);
 for(const angle of [0,Math.PI/2,Math.PI,Math.PI*1.5])for(const radius of [569.99,570,570.01]){
  v.x=Math.cos(angle)*radius;v.y=Math.sin(angle)*radius;grids(g);g.command('nearestHuman',{x:Math.cos(angle),y:Math.sin(angle)});assert.equal(g.nearestTargetFor(a,true)?.id??null,radius<=570?v.id:null);
 }
 g.command('nearestHuman',{x:1,y:0});for(const [x,y]of [[-15,0],[5,60]]){v.x=x;v.y=y;grids(g);assert.equal(g.nearestTargetFor(a,true),null)}
 v.x=80;v.y=0;grids(g);assert.equal(g.nearestTargetFor(a,true).id,v.id);
});
test('legacy saved held orders retain mixed infantry and vehicle targeting after loading',()=>{
 const {c,g,a}=arena(),tank=vehicle(g,'tank',40);g.makeHuman(120,0,null);grids(g);g.command('nearestHuman',{x:1,y:0});
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),copy=loaded.apes.find(x=>x.id===a.id);loaded.world.terrain=()=>({biome:'forest',water:false});loaded.world.waterBlocked=()=>false;loaded.world.objects.clear();loaded.world._spatial.clear();grids(loaded);
 assert.equal(copy.nearestOrder.kind,'human');loaded.planNearestOrders();assert.equal(copy._nearestTarget?.id,tank.id);assert.equal(copy._nearestTarget?.kind,'vehicle');
});

test('held E attacks parked tank objects while excluding closer structures and friendly vehicles',()=>{
 const {g,a}=arena(),tank=object(g,'vehicle',45,{vehicleType:'tank',parked:true,r:20,w:40,h:30,collision:'rect'}),cage=object(g,'cage',12,{count:2,solid:false}),friendly=object(g,'vehicle',8,{team:'ape',solid:false});g.makeHuman(140,0,null);grids(g);g.command('nearestHuman',{x:1,y:0});
 assert.equal(g.nearestTargetFor(a,true).id,tank.id);step(g,6);assert.equal(a._nearestTarget?.kind,'object');assert.equal(a._nearestTarget?.id,tank.id);assert.ok(tank.hp<tank.maxHp);assert.equal(cage.hp,cage.maxHp);assert.equal(friendly.hp,friendly.maxHp);
 g.damageObject(cage,100,a);assert.equal(cage.hp,cage.maxHp,'structure damage remains blocked throughout held orders');
});
