'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function arena(){
 const c=loadEngine(),g=new c.ATSGame('ELITE-COMBAT','survival');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];g.world.getObjects=()=>[];
 g.world.terrain=()=>({biome:'farmland',road:true,water:false});g.world.lineClear=()=>true;g.world.blocked=()=>false;g.world.vehicleBlocked=()=>false;
 g.spawnSites=()=>{};g.responseDirector=()=>{};g.heliTimer=100000;g.king.x=1000;g.king.y=1000;g.tier=5;g.exposure=1;return {c,g};
}
function population(g,n){g.apes=Array.from({length:n},()=>({hp:120}))}
function refresh(g){g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);g.performance.beginStep();g.visibilityCache.clear()}
function vehicle(g,variant,kind='tank'){const v={id:'vehicle-'+g.nextId++,x:0,y:0,dir:0,turretDir:0,kind,variant,state:'combat',shootTimer:999,cannonTimer:0};g.forces.initVehicle(v,kind,0);g.vehicles.push(v);return v}

test('elite unlocks rotate stronger tank types while preserving the ordinary 300-ape army',()=>{
 const {c,g}=arena();population(g,300);
 assert.equal(g.forces.variantFor('tank'),null);assert.equal(g.forces.variantFor('ifv'),null);
 assert.equal(c.ATSVehicleSpecs.tank.hp,1100);assert.equal(c.ATSVehicleSpecs.tank.front,.2);assert.equal(c.ATSVehicleSpecs.tank.cannonDamage,135);
 assert.ok(Array.from({length:16},(_,i)=>g.forces.militaryRole(i)).every(role=>!['assault','commando','juggernaut'].includes(role)));
 for(const [n,variant]of [[449,null],[450,'veteran'],[649,'veteran'],[650,'siege'],[849,'siege'],[850,'ironclad']]){population(g,n);assert.equal(g.forces.variantFor('tank'),variant)}
 assert.deepEqual(Array.from({length:4},(_,i)=>g.forces.variantFor('tank',i)),['ironclad','siege','veteran',null]);
 assert.equal(g.forces.variantFor('ifv'),'sentinel');assert.equal(g.forces.variantFor('apc'),null);
 const roster=Array.from({length:16},(_,i)=>g.forces.militaryRole(i));for(const role of ['assault','commando','juggernaut','leader','heavy','engineer','medic','mortar','sniper','grenadier'])assert.ok(roster.includes(role));
 assert.equal(g.forces.vehicleSpec({kind:'apc',variant:'ironclad'}),c.ATSVehicleSpecs.apc,'a variant cannot turn a carrier into a tank');
});

test('late tanks have real extra cost and protection but retain rear vulnerability and component damage',()=>{
 const {c,g}=arena();for(const id of ['veteran','siege','ironclad']){
  const v=vehicle(g,id),spec=g.forces.vehicleSpec(v);assert.equal(v.vehicleClass,'tank');assert.equal(v.navClass,'tank');assert.equal(v.hp,spec.hp);assert.ok(v.hp>1100);assert.ok(v.weight>12);assert.equal(spec.budget,v.weight);
  assert.ok(spec.range>570&&spec.cannonDamage>135&&spec.cannonRadius>108);assert.equal(v.radius,c.ATSVehicleSpecs.tank.radius);
  assert.equal(g.forces.vehicleDamage(v,100,{x:100,y:0}),100*spec.front);assert.equal(g.forces.vehicleDamage(v,100,{x:0,y:100}),100*spec.side);
  assert.equal(g.forces.vehicleDamage(v,100,{x:-100,y:0}),100);assert.equal(g.forces.vehicleDamage(v,100,{x:100,y:0,type:'shell'}),100);
  assert.ok(v.engineDamage>v.weaponDamage,'rear attacks damage the exposed engine more effectively');
 }
 const ifv=vehicle(g,'sentinel','ifv');assert.equal(ifv.hp,1050);assert.equal(ifv.weight,13);assert.equal(ifv.vehicleClass,'ifv');assert.ok(g.forces.vehicleSpec(ifv).cannonDamage>48);
});

test('a siege tank retains its full cannon warning and fires the stronger committed shell',()=>{
 const {g}=arena(),v=vehicle(g,'siege');g.king.x=350;g.king.y=0;refresh(g);
 g.time=.01;g.forces.updateVehicle(v,.01);assert.ok(v.cannonTarget);assert.equal(v.cannonTarget.duration,1.8);const point={x:v.cannonTarget.x,y:v.cannonTarget.y};
 g.king.y=200;g.time=1.7;refresh(g);g.forces.updateVehicle(v,1.69);assert.equal(g.forces.hazards.length,0);
 g.time=1.82;refresh(g);g.forces.updateVehicle(v,.12);const shell=g.forces.hazards.find(h=>h.type==='shell');assert.ok(shell);assert.equal(shell.damage,195);assert.equal(shell.radius,126);assert.equal(shell.targetX,point.x);assert.equal(shell.targetY,point.y);
});

test('sixteen nearby apes still overrun ironclad armor and disable its cannon',()=>{
 const {g}=arena(),v=vehicle(g,'ironclad');v.cannonTarget={x:300,y:0,start:0,until:2,duration:1.8};
 for(let i=0;i<16;i++)g.makeApe(5+i%4,5+Math.floor(i/4),'hold');refresh(g);g.forces.updateSwarm(v,.5);
 assert.equal(v.swarmCount,16);assert.equal(v.overrun,true);assert.equal(v.cannonTarget,null);assert.ok(v.hp<2400);assert.ok(v.mobilityDamage>0&&v.weaponDamage>0&&v.engineDamage>0);assert.ok(v.rotationMultiplier<1&&v.turretMultiplier<.2);
});

test('elite infantry gains distinct weapons and accurate fire while juggernaut armor breaks under a swarm',()=>{
 const {c,g}=arena(),troops={};for(const role of ['assault','commando','juggernaut']){const h=g.makeHuman(0,0,null);g.forces.assign(h,null,role);troops[role]=h;assert.ok(c.ATSHumanRoles[role].weight>2);assert.equal(h.role,role)}
 assert.equal(troops.assault.hp,150);assert.equal(troops.assault.kind,'assault');assert.equal(troops.commando.hp,110);assert.equal(troops.commando.kind,'rifle');assert.ok(troops.commando.accuracyMultiplier<c.ATSHumanRoles.rifleman.accuracy);
 const h=troops.juggernaut;assert.equal(h.hp,180);assert.equal(h.kind,'machine');assert.ok(c.ATSHumanRoles.juggernaut.speed<c.ATSHumanRoles.heavy.speed);
 for(let i=0;i<5;i++)g.makeApe(5+i,0,'hold');refresh(g);assert.equal(g.forces.armor(h,100,{x:10,y:0}),80);assert.equal(g.forces.armor(h,100,{x:10,y:0,armorPiercing:true}),100);
 g.makeApe(5,5,'hold');refresh(g);assert.equal(g.forces.armor(h,100,{x:10,y:0}),100);
 h.state='combat';h.targetId='king';h.lastSeenAt=0;h.perceptionTimer=10;h.shootTimer=0;g.king.x=180;g.king.y=0;g.forces.createSquad([troops.assault,h],g.king);refresh(g);g.forces.updateDoctrine(h,.05);assert.ok(g.bullets.some(b=>b.owner===h.id));assert.ok(h.burstUntil>g.time);
});

test('transport unloads its prepaid elite roster once and legacy cargo gains no free population upgrades',()=>{
 const {g}=arena(),v=vehicle(g,null,'apc');population(g,850);v.troopRoles=Array.from({length:8},(_,i)=>g.forces.militaryRole(i));v.troops=8;const planned=[...v.troopRoles];g.apes=[];
 for(let i=0;i<20;i++)g.forces.dismount(v,.36);assert.equal(g.humans.length,8);assert.equal(v.troops,0);assert.deepEqual(Array.from(g.humans,h=>h.role),planned);assert.equal(g.humans[2].maxHp,150);assert.equal(g.humans[3].maxHp,180);assert.equal(g.humans[4].maxHp,110);
 const legacy=vehicle(g,null,'truck');legacy.troops=4;population(g,850);for(let i=0;i<10;i++)g.forces.dismount(legacy,.36);assert.deepEqual(Array.from(g.humans.slice(8),h=>h.role),['leader','rifleman','rifleman','heavy']);
});

test('saves preserve variant costs, wounded elite HP, damaged components and the remaining troop roster',()=>{
 const {c,g}=arena(),v=vehicle(g,'ironclad');v.hp=817;v.engineDamage=47;v.mobilityDamage=19;v.weaponDamage=33;v.reverseSupport={x:-170,y:20};v.reverseUntil=8;v.reverseComplete=true;
 const carrier=vehicle(g,'sentinel','ifv');carrier.troopRoles=['leader','assault','juggernaut','commando','medic'];carrier.deployedTroops=2;carrier.troops=3;
 const h=g.makeHuman(10,20,null);g.forces.assign(h,null,'commando');h.hp=43;h.state='combat';h.targetId='king';
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),saved=loaded.vehicles.find(a=>a.id===v.id),lh=loaded.humansById.get(h.id);
 assert.equal(saved.variant,'ironclad');assert.equal(saved.vehicleClass,'tank');assert.equal(saved.hp,817);assert.equal(saved.maxHp,2400);assert.equal(saved.engineDamage,47);assert.equal(saved.mobilityDamage,19);assert.equal(saved.weaponDamage,33);assert.equal(saved.weight,25);assert.equal(saved.reverseComplete,true);
 assert.equal(loaded.forces.vehicleSpec(saved).cannonDamage,225);assert.equal(lh.role,'commando');assert.equal(lh.hp,43);assert.equal(lh.maxHp,110);
 const cargo=loaded.vehicles.find(a=>a.id===carrier.id);assert.deepEqual(Array.from(cargo.troopRoles),carrier.troopRoles);assert.equal(cargo.deployedTroops,2);assert.equal(cargo.troops,3);
});

test('cached elite specs and dense elite combat retain the existing frame work ceilings',()=>{
 const {g}=arena();g.king.hp=g.king.maxHp=1000000;g.king.x=-300;g.king.y=0;
 for(let i=0;i<1000;i++){const a=g.makeApe(-100+i%25*8,(Math.floor(i/25)-20)*8,'hold');a.hp=a.maxHp=1000000}
 for(let i=0;i<64;i++){const h=g.makeHuman(230,i*3-95,null);g.forces.assign(h,null,g.forces.militaryRole(i));h.hp=h.maxHp=1000000;h.state='combat';h.reported=true;h.dir=Math.PI;h.targetId=g.apes[500+i%25].id;h.lastSeenAt=0;h.perceptionTimer=.3;h.shootTimer=0}
 for(let i=0;i<12;i++){const kind=i%3?'tank':'ifv',v=vehicle(g,g.forces.variantFor(kind,i),kind);v.x=500;v.y=i*25-130;v.shootTimer=0}
 const v=g.vehicles[1],spec=g.forces.vehicleSpec(v);for(let i=0;i<10000;i++)assert.equal(g.forces.vehicleSpec(v),spec);
 let weapons=0;for(let frame=0;frame<20;frame++){g.update(1/60,{});weapons=Math.max(weapons,g.bullets.length+g.forces.hazards.length);assert.ok(g.performance.counters.aiThinks<=32);assert.ok(g.performance.counters.losTests<=96);assert.ok(g.navigation.stats.frameExpanded<=192);assert.ok(g.navigation.stats.frameSearches<=3);assert.ok(g.effects.length<=448)}
 assert.equal(g.population,1000);assert.equal(g.humans.length,64);assert.equal(g.vehicles.length,12);assert.ok(weapons>0,'the bounded benchmark performs real combat');
});
