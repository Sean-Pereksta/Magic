'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function arena(){
 const c=loadEngine(),g=new c.ATSGame('RELENTLESS-COMBAT','survival');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.getSites=()=>[];g.world.getObjects=()=>[];g.world.terrain=()=>({biome:'farmland',road:true,water:false});g.world.lineClear=()=>true;g.world.blocked=()=>false;g.world.vehicleBlocked=()=>false;g.spawnSites=()=>{};
 g.tier=5;g.king.x=250;g.king.y=0;g.lastContact=0;refresh(g);return g;
}
function refresh(g){g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);g.performance.beginStep();g.visibilityCache.clear()}
function soldier(g,role='rifleman',x=0,y=0,site=null){const h=g.makeHuman(x,y,site);g.forces.assign(h,site,role);h.state='combat';h.targetId='king';h.lastSeenAt=g.time;h.perceptionTimer=10;h.shootTimer=0;h.specialAt=0;h.dir=0;h.reported=true;return h}
function squad(g,count=6){const members=Array.from({length:count},(_,i)=>soldier(g,i===0?'leader':'rifleman',0,i*8));const s=g.forces.createSquad(members,g.king);s.nextReport=999;return s}
function horde(g,count,x=250,y=0){for(let i=0;i<count;i++)g.makeApe(x+i%10,y+Math.floor(i/10)%10,'hold');refresh(g)}
function tank(g,x=0,y=70){const v={id:'vehicle-'+g.nextId++,x,y,dir:0,turretDir:0,state:'raid',target:{x:500,y:0},shootTimer:0,cannonTimer:0};g.forces.initVehicle(v,'tank',0);g.vehicles.push(v);return v}

test('armor-supported squads hold against 80–299 visible apes and record contact only through functioning radios',()=>{
 const g=arena(),s=squad(g);tank(g);horde(g,180);const calls=[];g.recordHordeContact=(...args)=>calls.push(args);
 g.forces.thinkSquad(s);assert.equal(s.order,'Hold & Suppress');assert.equal(s.supported,true);assert.equal(calls.length,1);assert.ok(calls[0][2]>=150);
 const leader=g.humansById.get(s.leaderId);leader.siteId='radio-base';g.world.sites.set('radio-base',{id:'radio-base',radioDown:true});g.time=.6;g.humans.forEach(h=>h.lastSeenAt=g.time);refresh(g);g.forces.thinkSquad(s);assert.equal(calls.length,1);
});
test('a small patrol shadows an army until combined forces arrive, then deliberately counterattacks',()=>{
 const g=arena(),s=squad(g,4);horde(g,330);g.forces.thinkSquad(s);assert.equal(s.order,'Shadow');
 tank(g,0,90);tank(g,-80,-80);g.time=.6;g.humans.forEach(h=>h.lastSeenAt=g.time);refresh(g);g.forces.thinkSquad(s);assert.equal(s.order,'Counterattack');assert.ok(s.supportStrength>=36);
});
test('holding a supported firing line does not quietly retreat as the ape contact advances',()=>{
 const g=arena(),s=squad(g);tank(g);horde(g,140);g.forces.thinkSquad(s);assert.equal(s.order,'Hold & Suppress');const h=g.humans[1],line={...s.firingLine},point={...g.forces.formationPoint(h,s)};
 g.king.x-=110;g.apes.forEach(a=>a.x-=110);g.time=.6;g.humans.forEach(h=>h.lastSeenAt=g.time);refresh(g);g.forces.thinkSquad(s);
 assert.equal(s.order,'Hold & Suppress');assert.deepEqual({...s.firingLine},line);assert.deepEqual({...g.forces.formationPoint(h,s)},point);
 const saved=JSON.parse(JSON.stringify(g.serialize())),loaded=loadEngine().ATSGame.fromJSON(saved),restored=loaded.forces.squads.get(s.id);assert.deepEqual({...restored.firingLine},line);assert.equal(restored.firingFacing,s.firingFacing);
});
test('fallback is bounded to 200 units and ends at a firing line rather than an endless retreat',()=>{
 const g=arena(),s=squad(g,4);horde(g,100);g.forces.thinkSquad(s);assert.equal(s.order,'Fallback');assert.ok(Math.hypot(s.fallbackPoint.x,s.fallbackPoint.y)<=200.01);
 const point={...s.fallbackPoint};g.time=9;g.humans.forEach(h=>h.lastSeenAt=g.time);refresh(g);g.forces.thinkSquad(s);assert.equal(s.order,'Hold & Suppress');assert.deepEqual({...s.fallbackPoint},point);
 g.time=10;g.humans.forEach(h=>h.lastSeenAt=g.time);refresh(g);g.forces.thinkSquad(s);assert.equal(s.order,'Hold & Suppress');
});
test('falling visible density starts a finite counterattack, while hidden apes never refresh it',()=>{
 const g=arena(),s=squad(g);tank(g);horde(g,140);g.forces.thinkSquad(s);
 g.apes.slice(35).forEach(a=>{a.x=1000;a.y=1000});g.time=.6;g.humans.forEach(h=>h.lastSeenAt=g.time);refresh(g);g.forces.thinkSquad(s);assert.equal(s.order,'Counterattack');const until=s.counterattackUntil;
 g.time=1.2;g.humans.forEach(h=>h.lastSeenAt=g.time);refresh(g);g.forces.thinkSquad(s);assert.equal(s.counterattackUntil,until);
 g.time=10;g.humans.forEach(h=>h.lastSeenAt=g.time);refresh(g);g.forces.thinkSquad(s);assert.notEqual(s.order,'Counterattack');
});
test('heavy weapons select a visible cluster over the nearest isolated ape and decline hidden centers',()=>{
 const g=arena(),h=soldier(g,'grenadier');g.king.x=130;horde(g,35,290,70);
 const cluster=g.forces.selectCluster(h,g.king,{radius:67,range:330,minRange:105});assert.ok(cluster.count>=30);assert.ok(cluster.x>260);
 g.world.lineClear=(ax,ay,bx)=>bx<200;g.time=1;refresh(g);const visible=g.forces.selectCluster(h,g.king,{radius:67,range:330,minRange:105});assert.equal(visible.id,'king');
 g.performance.thinksRemaining=0;g.performance.think=()=>false;g.time=2;assert.equal(g.forces.selectCluster(h,g.king,{radius:67,range:330,minRange:105}),null);
});
test('grenadiers spread area attacks instead of duplicating a warned impact',()=>{
 const g=arena(),a=soldier(g,'grenadier'),b=soldier(g,'grenadier',5,5);horde(g,45,250,0);g.time=.1;g.forces.update(a,.01);g.forces.update(b,.01);
 const grenades=g.forces.hazards.filter(h=>h.type==='grenade');assert.equal(grenades.length,1);assert.equal(grenades[0].fuse,1.8);
});
test('mortars require fresh contact, communications and range, warn for two seconds and stagger crews',()=>{
 const g=arena(),site={id:'comms',x:0,y:0,tier:5,objects:[],radioDown:false};g.world.sites.set(site.id,site);
 const leader=soldier(g,'leader',0,0,site),a=soldier(g,'mortar',-100,0,site),b=soldier(g,'mortar',-120,20,site);const s=g.forces.createSquad([leader,a,b],g.king);s.nextReport=999;tank(g);horde(g,60);
 g.forces.thinkSquad(s);g.forces.updateMortar(a);const hazard=g.forces.hazards.find(h=>h.type==='mortar');assert.ok(hazard);assert.equal(hazard.life,2.1);assert.ok(a.specialAt>=10&&a.specialAt<=14);g.forces.updateMortar(b);assert.equal(g.forces.hazards.length,1);
 const victim=g.apesById.get(hazard.id==='king'?'king':g.apes[0].id),before=victim.hp;g.forces.tick(2);assert.equal(victim.hp,before);g.forces.tick(.11);assert.ok(victim.hp<before);
 g.time=15;g.humans.forEach(h=>h.lastSeenAt=g.time);a.specialAt=0;site.radioDown=true;refresh(g);g.forces.updateMortar(a);assert.equal(g.forces.hazards.length,0);
 site.radioDown=false;s.reportAt=g.time-3;g.humans.forEach(h=>h.lastSeenAt=g.time-3);s.nextThink=g.time+5;g.forces.updateMortar(a);assert.equal(g.forces.hazards.length,0);
 s.reportAt=g.time;a.x=150;g.forces.updateMortar(a);assert.equal(g.forces.hazards.length,0);
});
test('mortars and gunship impacts damage nearby huts only after their warning expires',()=>{
 const g=arena(),hits=[];g.colonies.damageNearby=(...args)=>hits.push(args);g.forces.hazards.push({type:'airstrike',x:0,y:0,start:0,life:2,fuse:2,radius:80,damage:85});
 g.forces.tick(1.9);assert.equal(hits.length,0);g.forces.tick(.2);assert.equal(hits.length,1);assert.equal(hits[0][3],85);
});
test('IFV follow-up shells keep the original warned point rather than retargeting with a short tell',()=>{
 const g=arena(),v=tank(g,0,0);g.forces.initVehicle(v,'ifv',0);g.king.x=300;v.cannonTimer=0;refresh(g);g.forces.updateVehicle(v,.01);assert.equal(v.cannonTarget.duration,1.1);const point={x:v.cannonTarget.x,y:v.cannonTarget.y};
 g.time=1.2;refresh(g);g.forces.updateVehicle(v,.01);assert.equal(v.cannonBurst,1);g.king.x=330;g.king.y=50;g.time=1.7;refresh(g);g.forces.updateVehicle(v,.5);assert.equal(v.cannonTarget.duration,.25);assert.deepEqual({x:v.cannonTarget.x,y:v.cannonTarget.y},point);
});
test('a tank reverses away from an approaching swarm while preserving its front armor facing',()=>{
 const g=arena(),v=tank(g,0,0);g.king.x=900;horde(g,14,120,-5);soldier(g,'rifleman',-110,0);refresh(g);g.forces.updateVehicle(v,.2);
 assert.ok(v.approachCount>=12);assert.ok(v.x<0);assert.ok(Math.cos(v.dir)>.98);assert.equal(v.reversing,true);assert.ok(g.bullets.length>0);
});
test('riflemen clear an overrun friendly tank before aiming at the distant horde',()=>{
 const g=arena(),v=tank(g,0,0),h=soldier(g,'rifleman',-110,0);g.king.x=300;horde(g,10,5,0);v.swarmCount=10;v.overrun=true;g.forces.tacticalTarget(h);
 assert.equal(h.clearingVehicleId,v.id);assert.notEqual(h.targetId,'king');assert.ok(g.apesById.get(h.targetId).x<20);
});
test('rangers choose isolated horde edges and heavy gunners move slowly during bursts',()=>{
 const g=arena(),r=soldier(g,'ranger');horde(g,45,240,0);const lone=g.makeApe(170,-90,'hold');refresh(g);g.forces.tacticalTarget(r);assert.equal(r.targetId,lone.id);
 const heavy=soldier(g,'heavy',-110,0),leader=soldier(g,'leader',-90,30);const s=g.forces.createSquad([leader,heavy],g.king);s.nextReport=999;heavy.burstUntil=4;const speeds=[];g.move=(h,x,y,speed)=>speeds.push(speed);refresh(g);g.forces.updateDoctrine(heavy,.05);assert.ok(speeds.includes(14));assert.ok(heavy.accuracyMultiplier<.85);
});
test('marksmen identify an exposed King over time and retain a dodgeable warning that cover cancels',()=>{
 const g=arena(),h=soldier(g,'sniper'),ape=g.makeApe(180,40,'hold');h.targetId=ape.id;g.exposure=.6;refresh(g);g.forces.tacticalTarget(h);assert.equal(h.targetId,ape.id);
 g.time=1.4;refresh(g);g.forces.tacticalTarget(h);assert.equal(h.targetId,'king');g.forces.update(h,.01);assert.ok(h.aiming);assert.equal(h.aiming.duration,1.35);assert.equal(g.bullets.length,0);
 g.world.lineClear=()=>false;g.time=2;refresh(g);g.forces.update(h,.01);assert.equal(h.aiming,null);assert.equal(g.bullets.length,0);
});
test('saved fallback positions, casualty thresholds and the current mortar volley remain bounded after loading',()=>{
 const g=arena(),s=squad(g,4);horde(g,100);g.forces.thinkSquad(s);s.initialSize=10;g.forces.hazards.push({id:'saved-mortar',type:'mortar',x:250,y:0,start:0,life:2.1,fuse:2.1,radius:100,damage:90});
 const saved=JSON.parse(JSON.stringify(g.serialize())),c=loadEngine(),loaded=c.ATSGame.fromJSON(saved),restored=loaded.forces.squads.get(s.id);
 assert.equal(restored.order,'Fallback');assert.equal(restored.initialSize,10);assert.equal(restored.density,101);assert.deepEqual({...restored.fallbackPoint},{...s.fallbackPoint});assert.equal(restored.fallbackUntil,s.fallbackUntil);assert.equal(loaded.forces.nextMortarAt,2.4);
});
