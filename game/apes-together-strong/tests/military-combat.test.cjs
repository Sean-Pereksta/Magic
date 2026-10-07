'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function arena(){
 const c=loadEngine(),g=new c.ATSGame('MILITARY-TEST','survival');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.getSites=()=>[];g.world.getObjects=()=>[];g.world.terrain=()=>({biome:'farmland',road:true,water:false});g.world.lineClear=()=>true;g.world.blocked=()=>false;g.world.vehicleBlocked=()=>false;
 g.king.x=1000;g.king.y=1000;g.lastContact=0;g.apeGrid.rebuild([g.king]);g.humanGrid.rebuild([]);g.spawnSites=()=>{};
 return {c,g};
}
function vehicle(g,kind,x=0,y=0,troops=0){const v={id:'vehicle-'+g.nextId++,kind,x,y,dir:0,state:'raid',shootTimer:999};g.forces.initVehicle(v,kind,troops);g.vehicles.push(v);return v}
function refresh(g){g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);g.performance.beginStep()}
function stepVehicle(g,v,dt){g.time+=dt;g.performance.beginStep();g.forces.updateVehicle(v,dt)}

test('tank armor rewards rear and side attacks and damages the component on that facing',()=>{
 const {g}=arena(),v=vehicle(g,'tank');
 assert.equal(g.forces.vehicleDamage(v,100,{x:50,y:0}),20);
 assert.equal(g.forces.vehicleDamage(v,100,{x:0,y:50}),50);
 assert.equal(g.forces.vehicleDamage(v,100,{x:-50,y:0}),100);
 assert.ok(v.weaponDamage>0&&v.mobilityDamage>0&&v.engineDamage>v.mobilityDamage);
 assert.equal(g.forces.vehicleDamage(v,100,{x:50,y:0,type:'shell'}),100);
 assert.equal(v.maxHp,1100);
});
test('5, 8, 12 and 16 physically nearby apes progressively disable tank handling and overrun the cannon',()=>{
 const {g}=arena(),v=vehicle(g,'tank');
 for(const count of [5,8,12,16]){
  while(g.apes.length<count)g.makeApe(5+g.apes.length,5,'hold');
  refresh(g);g.time+=.3;g.forces.updateSwarm(v,.3);
  assert.equal(v.swarmCount,count);assert.ok(v.rotationMultiplier<1);
  if(count>=8)assert.ok(v.accuracyMultiplier>1);
  if(count>=12)assert.ok(v.turretMultiplier<.2);
 }
 assert.equal(v.overrun,true);assert.equal(v.cannonTarget,null);assert.ok(v.hp<1100);assert.ok(v.weaponDamage>0);
 assert.ok(g.apes.every(a=>a.climbingVehicleId===v.id));
 g.apes.forEach(a=>{a.x=500;a.y=500});refresh(g);g.time+=.3;g.forces.updateSwarm(v,.1);
 assert.equal(v.overrun,false);assert.equal(v.rotationMultiplier,1);
});
test('damaged mobility immobilizes a tank and damaged weapon disables both guns',()=>{
 const {g}=arena(),v=vehicle(g,'tank');g.king.x=250;g.king.y=0;refresh(g);
 v.target={x:600,y:0};v.mobilityDamage=100;v.weaponDamage=100;v.shootTimer=0;v.cannonTimer=0;
 stepVehicle(g,v,.2);assert.equal(v.x,0);assert.equal(v.y,0);assert.equal(g.bullets.length,0);assert.equal(g.forces.hazards.length,0);
});
test('an abstract vehicle releases an expired physical swarm while preserving component damage',()=>{
 const {g}=arena(),v=vehicle(g,'tank');v._simTier=2;v._swarmThink=0;v.overrun=true;v.swarmCount=20;v.rotationMultiplier=.5;v.weaponDamage=30;v.mobilityDamage=15;g.time=2;
 g.navigation.steer=()=>({x:300,y:0});g.forces.vehicleMove(v,{x:300,y:0},.5);
 assert.equal(v.overrun,false);assert.equal(v.swarmCount,0);assert.equal(v.rotationMultiplier,1);assert.equal(v.weaponDamage,30);assert.equal(v.mobilityDamage,15);assert.ok(v.x>10);
});
test('tank cannon warns for 1.8 seconds then sends a traveling shell to the committed location',()=>{
 const {g}=arena(),v=vehicle(g,'tank');g.king.x=300;g.king.y=0;v.cannonTimer=0;refresh(g);
 stepVehicle(g,v,.01);assert.ok(v.cannonTarget);assert.equal(v.cannonTarget.duration,1.8);const committed={x:v.cannonTarget.x,y:v.cannonTarget.y};
 g.king.y=220;refresh(g);stepVehicle(g,v,1.7);assert.equal(g.forces.hazards.length,0);assert.equal(g.king.hp,160);
 stepVehicle(g,v,.11);const shell=g.forces.hazards[0];assert.ok(shell&&shell.type==='shell');assert.equal(shell.targetX,committed.x);assert.equal(shell.targetY,committed.y);assert.ok(shell.x<shell.targetX);
 g.forces.tick(shell.duration/2);assert.equal(g.king.hp,160);assert.equal(g.forces.hazards.length,1);
 g.forces.tick(shell.duration);assert.equal(g.king.hp,160);assert.equal(g.forces.hazards.length,0);
});
test('shell impact deals splash damage and brief knockback while a blocked impact leaves cover useful',()=>{
 const {g}=arena();g.king.x=40;g.king.y=0;refresh(g);
 g.forces.hazards.push({id:'shell-test',type:'shell',x:-100,y:0,fromX:-100,fromY:0,targetX:0,targetY:0,start:0,duration:1,life:1,radius:108,damage:100});
 g.forces.tick(.5);assert.equal(g.king.hp,160);assert.equal(g.forces.hazards[0].x,-50);
 g.forces.tick(.5);assert.ok(g.king.hp<90);assert.equal(g.king.blastReaction.stage,'flight');g.updateBlastReaction(g.king,.1);assert.ok(g.king.x>40);assert.ok(g.king.knockbackUntil>g.time);
 const before=g.king.hp;g.world.lineClear=()=>false;g.forces.blast({x:g.king.x,y:g.king.y,type:'shell',radius:108,damage:135});assert.equal(g.king.hp,before);
});
test('APC troops are prepaid, unload once into a shared squad, and die with an undeployed transport',()=>{
 const {g}=arena(),site={id:'base',x:0,y:0,tier:4,strength:20,objects:[]};g.world.sites.set(site.id,site);
 const v=vehicle(g,'apc',0,0,8);v.siteId=site.id;v.operationId='field-pursuit';v.target={x:400,y:0};
 for(let i=0;i<20;i++)g.forces.dismount(v,.36);
 assert.equal(g.humans.length,8);assert.equal(v.troops,0);assert.equal(site.strength,20);assert.equal(g.forces.squads.size,1);
 assert.ok(g.humans.every(h=>h.responseAllocated&&h.squadId===v.dismountSquadId&&h.operationId==='field-pursuit'));assert.equal(g.humans[0].role,'leader');
 g.hurt(g.humans[1],1000,{x:0,y:0});assert.equal(site.strength,20);
 const doomed=vehicle(g,'truck',100,0,12);g.hurt(doomed,1000,{x:200,y:0});assert.equal(doomed.troops,0);assert.equal(doomed.troopsLost,12);
 const count=g.humans.length;g.forces.dismount(doomed,1);assert.equal(g.humans.length,count);
});
test('military squads hold against 40 visible apes, withdraw against 80, and never pursue hidden concentrations',()=>{
 const {g}=arena();g.tier=4;g.king.x=250;g.king.y=0;
 const humans=[];for(let i=0;i<4;i++){const h=g.makeHuman(0,i*10,null);g.forces.assign(h,null,i===0?'leader':'rifleman');h.state='combat';h.targetId='king';h.lastSeenAt=0;humans.push(h)}
 const s=g.forces.createSquad(humans,{x:250,y:0});s.nextReport=1000;
 for(let i=0;i<39;i++)g.makeApe(240+i%10,i%6,'hold');refresh(g);g.forces.thinkSquad(s);assert.equal(s.order,'Suppress');
 for(let i=0;i<40;i++)g.makeApe(240+i%10,i%6,'hold');refresh(g);g.time=.6;g.forces.thinkSquad(s);assert.equal(s.order,'Fallback');
 const reported={...s.objective},fallback={...s.fallbackPoint};g.king.x=900;g.world.lineClear=()=>false;g.visibilityCache.clear();g.time=7;refresh(g);g.forces.thinkSquad(s);assert.equal(s.order,'Fallback','loss of contact does not cancel travel to confirmed support');assert.equal(s.density,0);assert.deepEqual({...s.objective},reported);assert.deepEqual({...s.fallbackPoint},fallback);g.time=9;g.performance.beginStep();g.forces.thinkSquad(s);assert.equal(s.order,'Hold & Suppress');assert.equal(s.fallbackComplete,true);
});
test('an exhausted LOS budget retains the last confirmed density and withdrawal order',()=>{
 const {g}=arena();g.king.x=250;g.king.y=0;const h=g.makeHuman(0,0,null);g.forces.assign(h,null,'rifleman');h.state='combat';h.targetId='king';h.lastSeenAt=0;
 const s=g.forces.createSquad([h],{x:250,y:0});s.density=100;s.order='Fallback';s.nextReport=1000;
 for(let i=0;i<80;i++)g.makeApe(250+i%8,i%9,'hold');refresh(g);g.performance.losRemaining=0;g.lineVisible=(observer,target)=>target.id==='king'?true:null;
 g.forces.thinkSquad(s);assert.equal(s.density,100);assert.equal(s.order,'Fallback');
});
test('leader loss briefly breaks coordination and military wounds, transport cargo, and squad reports survive a legacy save',()=>{
 const {c,g}=arena(),leader=g.makeHuman(0,0,null),soldier=g.makeHuman(25,0,null);g.forces.assign(leader,null,'leader');g.forces.assign(soldier,null,'ranger');
 const s=g.forces.createSquad([leader,soldier],{x:550,y:200},{order:'Attack Settlement'});g.hurt(leader,1000,{x:0,y:0});assert.equal(s.leaderId,null);assert.equal(soldier.squadOrder,'Regroup');assert.ok(soldier.cohesionLossUntil>g.time);
 soldier.hp=41;const v=vehicle(g,'apc',100,0,6);v.engineDamage=63;v.mobilityDamage=22;v.hp=390;
 const saved=JSON.parse(JSON.stringify(g.serialize())),loaded=c.ATSGame.fromJSON(saved),lv=loaded.vehicles.find(a=>a.id===v.id),lh=loaded.humans.find(h=>h.id===soldier.id);
 assert.equal(lv.troops,6);assert.equal(lv.hp,390);assert.equal(lv.engineDamage,63);assert.equal(lv.mobilityDamage,22);assert.equal(lh.hp,41);assert.equal(loaded.forces.squads.get(s.id).objective.x,550);assert.ok(loaded.forces.squads.get(s.id).cohesionLossUntil>loaded.time);
 const legacy=JSON.parse(JSON.stringify(saved));delete legacy.squads;delete legacy.responseStage;delete legacy.vehicles[0].vehicleClass;delete legacy.vehicles[0].mobilityDamage;delete legacy.vehicles[0].engineDamage;delete legacy.vehicles[0].components;
 const old=c.ATSGame.fromJSON(legacy);assert.ok(Number.isFinite(old.vehicles[0].mobilityDamage));assert.equal(old.vehicles[0].hp,390);
});
