'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function arena(){
 const c=loadEngine(),g=new c.ATSGame('FORTIFICATION-TEST','survival');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.getSites=()=>[];g.world.terrain=()=>({biome:'farmland',road:true,water:false});g.world.vehicleTerrain=g.world.terrain;g.world.lineClear=()=>true;g.king.x=g.king.y=1000;g.apeGrid.rebuild([g.king]);g.humanGrid.rebuild([]);g.spawnSites=()=>{};
 return {c,g,w:g.world,nav:g.navigation};
}
function soldier(g,role='rifleman',x=0,y=0){const h=g.makeHuman(x,y,null);g.forces.assign(h,null,role);return h}
function refresh(g){g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);g.performance.beginStep()}
function move(g,actor,target,seconds,speed=90){for(let i=0;i<Math.ceil(seconds*20);i++){g.time+=.05;g.navigation.beginFrame(g.time);g.navigation.move(actor,target.x-actor.x,target.y-actor.y,speed,.05,true)}}
function barrier(w,team,extra={}){return w.createFortification({x:50,y:0,w:18,h:240,team,kind:'basic',...extra})}

test('engineers take four to seven seconds, build 300/600 HP cover, and damage interrupts a job',()=>{
 const {g,w}=arena(),h=soldier(g,'engineer');const id=g.forces.startEngineerJob(h,{x:32,y:0,kind:'basic'});assert.ok(id);const duration=h.engineerJob.duration;assert.ok(duration>=4&&duration<=7);
 g.forces.updateEngineer(h,duration-.1);assert.equal(w.objects.size,0);g.time+=duration;g.forces.updateEngineer(h,.11);assert.equal(h.lastEngineerJob.status,'completed');assert.equal(w.objects.get(h.lastEngineerJob.objectId).hp,300);
 const other=soldier(g,'engineer',200,0);g.forces.startEngineerJob(other,{x:230,y:0,kind:'heavy'});g.forces.updateEngineer(other,6.9);assert.ok(other.engineerJob);g.time+=7;g.forces.updateEngineer(other,.11);assert.equal(w.objects.get(other.lastEngineerJob.objectId).maxHp,600);
 const stopped=soldier(g,'engineer',400,0);g.forces.startEngineerJob(stopped,{x:430,y:0});stopped.hitTimer=.2;g.forces.updateEngineer(stopped,1);assert.equal(stopped.engineerJob,null);assert.equal(stopped.lastEngineerJob.status,'interrupted');assert.equal(w.objects.size,2);
});

test('enemy presence at the construction footprint and a fallback order cancel construction',()=>{
 const {g,w}=arena(),h=soldier(g,'engineer',0,0);g.forces.startEngineerJob(h,{x:44,y:0,w:82,h:18});g.makeApe(109,0,'hold');refresh(g);g.forces.updateEngineer(h,10);assert.equal(h.lastEngineerJob.status,'interrupted');assert.equal(w.objects.size,0);
 g.apes[0].x=1000;refresh(g);g.forces.startEngineerJob(h,{x:44,y:0});h.squadOrder='Fallback';g.forces.updateEngineer(h,10);assert.equal(h.lastEngineerJob.reason,'withdrawal');assert.equal(w.objects.size,0);
});

test('a defensive line leaves a central passage and squads hold riflemen ahead of guns and mortars',()=>{
 const {g,w}=arena(),members=['leader','rifleman','heavy','mortar','engineer','engineer'].map((r,i)=>soldier(g,r,0,i*6)),s=g.forces.createSquad(members,{x:400,y:0},{order:'Hold Line',operationId:'defense'});refresh(g);
 const placements=g.forces.prepareLine(s,{x:0,y:0,angle:0,kind:'heavy'});assert.equal(placements.length,4);assert.equal(members.filter(h=>h.engineerJob).length,2);
 for(const p of placements)w.createFortification(p);assert.equal(w.actorBlocked(36,0,10,'ape'),false,'line retains its central opening');assert.equal(w.actorBlocked(36,88,10,'ape'),true);
 g.forces.thinkSquad(s);assert.equal(s.order,'Hold Line');const rifle=g.forces.formationPoint(members[1],s),gun=g.forces.formationPoint(members[2],s),mortar=g.forces.formationPoint(members[3],s);assert.ok(rifle.x>gun.x&&gun.x>mortar.x);assert.ok(rifle.x<36);
});

test('platoons share confirmed contacts across two to four squads without replacing phased assault staging',()=>{
 const {g}=arena(),squads=[];g.king.x=250;g.king.y=0;
 for(let i=0;i<4;i++){const h=soldier(g,'leader',0,i*15);const s=g.forces.createSquad([h],{x:450,y:0},{operationId:'joint-operation'});squads.push(s)}refresh(g);assert.equal(g.forces.platoons.size,1);const p=[...g.forces.platoons.values()][0];assert.equal(p.squads.length,4);
 const first=g.humans[0];first.state='combat';first.targetId='king';first.lastSeenAt=0;g.forces.thinkSquad(squads[0]);g.forces.thinkSquad(squads[1]);assert.equal(squads[1].objective.x,250);
 squads[2].assaultPhase='engineers';squads[2].objective={x:420,y:90};const h=g.humans[2];h.state='combat';h.targetId='king';h.lastSeenAt=0;g.forces.thinkSquad(squads[2]);assert.equal(squads[2].order,'Hold Line');assert.equal(squads[2].objective.x,420);
 const saved=JSON.parse(JSON.stringify(Array.from(g.forces.platoons.values())));g.forces.restoreSquads(JSON.parse(JSON.stringify(Array.from(g.forces.squads.values()))));g.forces.restorePlatoons(saved);assert.equal(g.forces.platoons.get(p.id).squads.length,4);
});

test('humans pass their own barricades while apes must physically destroy them before crossing',()=>{
 const {g,w}=arena(),b=barrier(w,'human'),human=soldier(g),ape=g.makeApe(0,0,'charge');refresh(g);move(g,human,{x:170,y:0},2);assert.ok(human.x>150);assert.equal(b.hp,300);
 move(g,ape,{x:170,y:0},2);assert.ok(ape.x<30);assert.ok(b.hp>0&&b.hp<300);move(g,ape,{x:170,y:0},9);assert.equal(b.dead,true);assert.ok(ape.x>150);assert.ok(w.objects.has(b.id),'destruction retains debris');
});

test('apes vault ape barriers quickly, infantry climbing is slow and interruptible, and engineers breach faster',()=>{
 const {g,w}=arena(),b=barrier(w,'ape'),ape=g.makeApe(0,0,'follow');refresh(g);move(g,ape,{x:170,y:0},.2);assert.ok(ape.x<20);move(g,ape,{x:170,y:0},1.7);assert.ok(ape.x>140);assert.equal(b.hp,b.maxHp);
 const h=soldier(g);move(g,h,{x:170,y:0},1);assert.ok(h.x<20);h.hitTimer=.2;move(g,h,{x:170,y:0},.1);h.hitTimer=0;move(g,h,{x:170,y:0},1.8);assert.ok(h.x<20);move(g,h,{x:170,y:0},2);assert.ok(h.x>85);assert.equal(b.hp,b.maxHp);
 const engineer=soldier(g,'engineer');move(g,engineer,{x:170,y:0},2);assert.equal(b.dead,true);move(g,engineer,{x:170,y:0},.5);assert.ok(engineer.x>20);
});

test('only tanks crush weak ape sections and strong barriers plus wetland remain impassable',()=>{
 const {g,w}=arena(),b=barrier(w,'ape',{weak:true,h:2000}),tank={id:'vehicle-tank',x:0,y:0,dir:0,vehicleClass:'tank',radius:31};move(g,tank,{x:170,y:0},4,50);assert.equal(b.dead,true);assert.ok(tank.x>65);
 const strong=barrier(w,'ape',{x:280,h:2000,weak:false}),apc={id:'vehicle-apc',x:230,y:0,dir:0,vehicleClass:'apc',radius:25};move(g,apc,{x:430,y:0},2,70);assert.ok(apc.x<246);assert.equal(strong.hp,strong.maxHp);tank.x=230;tank.y=0;move(g,tank,{x:430,y:0},2,50);assert.ok(tank.x<=240);assert.equal(strong.hp,strong.maxHp);
 w.vehicleTerrain=()=>({biome:'wetland',water:false,road:false});assert.equal(g.navigation.clearSegment(0,300,200,300,31,'tank'),false);assert.equal(g.navigation.blocked(0,300,31,'tank'),true);
});

test('APC and IFV support nearby infantry from standoff range and reverse behind the line under pressure',()=>{
 const {g}=arena();const h=soldier(g,'rifleman',30,0);h.operationId='support';refresh(g);for(const kind of ['apc','ifv']){const v={id:'vehicle-'+kind,x:0,y:0,dir:0,vehicleClass:kind,operationId:'support',swarmCount:0};g.forces.initVehicle(v);const moves=[];g.forces.vehicleMove=(actor,point,dt,reverse)=>moves.push({point,reverse});g.forces.positionCarrier(v,{x:400,y:0},.1);assert.ok(moves[0].point.x<=175);v.swarmCount=10;g.forces.positionCarrier(v,{x:400,y:0},.1);assert.equal(moves[1].reverse,true);assert.ok(moves[1].point.x<h.x)}
});



test('regional armor keeps its independent staging and support objective when the crown is seen elsewhere',()=>{
 const {g}=arena(),v={id:'vehicle-independent',x:0,y:0,dir:0,vehicleClass:'tank',settlementTarget:'village',assaultPhase:'engineers',target:{x:150,y:60}};g.forces.initVehicle(v);const moves=[];g.forces.vehicleMove=(actor,point)=>moves.push(point);
 assert.equal(g.forces.positionOperationArmor(v,{x:500,y:-250},.1),true);assert.deepEqual(moves[0],v.target);v.x=150;v.y=60;v.assaultPhase='support';moves.length=0;g.forces.positionOperationArmor(v,{x:500,y:-250},.1);assert.equal(moves.length,0);assert.equal(v.moving,false);
});

test('real tank steering breaches an encountered weak section instead of waiting forever for a blocked route',()=>{
 const {g,w}=arena(),b=barrier(w,'ape',{weak:true,h:2000}),v={id:'vehicle-real-breach',x:0,y:0,dir:0};g.forces.initVehicle(v,'tank');g.vehicles.push(v);
 for(let i=0;i<80;i++){g.time+=.05;g.navigation.beginFrame(g.time);g.forces.vehicleMove(v,{x:170,y:0},.05)}assert.equal(b.dead,true);assert.ok(v.x>65);
});

test('assault infantry leaves its built staging barricade and moves through the settlement entrance',()=>{
 const {g,w}=arena(),members=['leader','rifleman','engineer','heavy'].map((role,i)=>soldier(g,role,0,i*5)),s=g.forces.createSquad(members,{x:200,y:0},{order:'Hold Line',operationId:'entrance-assault'});s.assaultPhase='engineers';w.createFortification({x:40,y:0,w:18,h:140,team:'human'});refresh(g);g.forces.thinkSquad(s);assert.equal(s.order,'Hold Line');assert.ok(s.firingLine);
 w.createFortification({x:200,y:-68,w:18,h:54,team:'ape'});w.createFortification({x:200,y:68,w:18,h:54,team:'ape'});s.assaultPhase='assault';s.objective={x:200,y:0};g.time=.6;g.performance.beginStep();g.forces.thinkSquad(s);assert.equal(s.order,'Attack Settlement');assert.equal(s.firingLine,null);assert.ok(g.forces.formationPoint(members[1],s).x>200);
 for(let i=0;i<100;i++){g.time+=.05;g.navigation.beginFrame(g.time);g.performance.beginStep();for(const h of members)g.forces.updateDoctrine(h,.05,true)}assert.ok(members[1].x>220,JSON.stringify({x:members[1].x,y:members[1].y,order:s.order}));assert.ok(members[2].x>220);
});

test('platoons preserve distinct prepared defense frontage and share only actual observed contacts',()=>{
 const {g}=arena(),h1=soldier(g,'leader',0,-95),h2=soldier(g,'leader',0,95),s1=g.forces.createSquad([h1],{x:420,y:-95},{order:'Hold',siteId:'same-base'}),s2=g.forces.createSquad([h2],{x:420,y:95},{order:'Hold',siteId:'same-base'});refresh(g);assert.notEqual(s1.platoonId,s2.platoonId,'distinct defense positions do not inherit a shared operational frontage');g.forces.createPlatoon([s1,s2],s1.objective);g.forces.thinkSquad(s2);assert.equal(s2.objective.y,95,'unconfirmed staging positions are not contact reports');const p=g.forces.formationPoint(h2,s2);assert.equal(Math.round(p.y),37,'own prepared line does not gain a second platoon lateral offset');
});

test('each explosive kind launches every affected survivor, then blasts new and existing corpses',()=>{
 for(const type of ['shell','mortar','airstrike','grenade']){const {g}=arena(),survivor=g.makeApe(25,0,'hold'),killed=g.makeApe(-25,0,'hold'),h=soldier(g,'rifleman',0,30);killed.hp=5;refresh(g);const launches=[];let corpseCalls=0;g.launchBlastReaction=(a,source,options)=>launches.push({id:a.id,source,options});g.blastBodies=(source,radius,options)=>{corpseCalls++;assert.ok(g.corpses.some(c=>c.id===killed.id));assert.equal(radius,100);assert.equal(options.power,type==='grenade'?.7:1)};g.forces.blast({type,x:0,y:0,radius:100,damage:30});assert.ok(launches.some(l=>l.id===survivor.id));assert.ok(launches.some(l=>l.id===h.id));assert.equal(corpseCalls,1);assert.ok(g.effects.some(e=>e.type==='explosion'&&e.blastKind===type));}
 const {g}=arena();for(let i=0;i<140;i++){const a=g.makeApe(i%10,i%7,'hold');a.hp=a.maxHp=1000}refresh(g);let reactions=0;g.launchBlastReaction=()=>{reactions++};g.blastBodies=()=>{};g.forces.blast({type:'airstrike',x:0,y:0,radius:100,damage:20});assert.equal(reactions,140,'no affected survivor is silently omitted in a dense blast');
});

test('a dense 1000-ape blast reacts on every survivor and all 320 bodies with linear collision work',t=>{
 const {g,w}=arena();g.world.stream=()=>{};g.world.trimDistant=()=>{};const actors=[];
 for(let i=0;i<1000;i++){const a=g.makeApe(i%20-10,Math.floor(i/20)%20-10,'hold');a.hp=a.maxHp=1000;actors.push(a)}
 for(let i=0;i<320;i++)g.corpses.push({id:'dense-body-'+i,type:'ape',x:i%20-10,y:Math.floor(i/20)-8,hp:0,life:20,age:2,dir:0});
 const wall={id:'dense-flight-cover',type:'wall',x:65,y:0,w:18,h:500,r:10,collision:'rect',solid:true,hp:500};w.objects.set(wall.id,wall);w._indexObject(wall);refresh(g);
 const {performance}=require('node:perf_hooks'),start=performance.now();g.forces.blast({type:'airstrike',x:0,y:0,radius:100,damage:20});const impactMs=performance.now()-start;
 assert.equal(actors.filter(a=>a.blastReaction?.stage==='flight').length,1000);assert.equal(g.corpses.filter(a=>a.blastReaction?.stage==='flight').length,320);assert.equal(g.population,1000);assert.ok(g.performance.counters.blastIntegrations<=13,'only a small kickoff batch does immediate collision physics');assert.ok(g.performance.counters.blastCollisionSweeps<=39);
 const all=actors.concat(g.corpses);let maxMs=0,maxSweeps=0,maxIntegrations=0;
 for(let frame=0;frame<65;frame++){g.time+=.05;g.navigation.beginFrame(g.time);g.performance.beginStep(g);const frameStart=performance.now();for(const a of all)g.updateBlastReaction(a,.05);maxMs=Math.max(maxMs,performance.now()-frameStart);const counters=g.performance.counters;maxSweeps=Math.max(maxSweeps,counters.blastCollisionSweeps);maxIntegrations=Math.max(maxIntegrations,counters.blastIntegrations);assert.ok(counters.blastIntegrations<=all.length);assert.ok(counters.blastCollisionSweeps<=all.length*3);assert.equal(counters.aiThinks,0);assert.equal(counters.losTests,0);if(frame===0)assert.ok(all.every(a=>a.blastZ>0),'all affected actors visibly rise by the next physics frame')}
 assert.ok(actors.every(a=>a.blastReaction===undefined&&a.blastZ===0),'every survivor finishes its finite get-up animation');assert.ok(g.corpses.every(a=>a.blastReaction.stage==='landed'));assert.equal(g.corpses.length,320);assert.equal(g.navigation.stats.searches,0,'blast flight never queues individual pathfinding');for(const a of all)assert.equal(g.navigation.blocked(a.x,a.y,10,a.type==='human'?'human':'ape'),false);
 if(process.env.ATS_MAX_TICK_MS)assert.ok(maxMs<Number(process.env.ATS_MAX_TICK_MS),`dense blast flight tick ${maxMs.toFixed(1)}ms`);t.diagnostic(`dense blast impact ${impactMs.toFixed(1)}ms; peak flight ${maxMs.toFixed(1)}ms, ${maxIntegrations} integrations/${maxSweeps} collision sweeps`);
});
