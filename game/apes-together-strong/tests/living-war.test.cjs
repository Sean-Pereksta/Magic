'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');

function setup(population=0){
 const c=loadEngine(),g=new c.ATSGame('LIVING-WAR');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();
 g.world.getSites=()=>[];g.world.ensure=()=>{};g.world.stream=()=>{};g.world.boundsReady=()=>true;
 g.world.blocked=()=>false;g.world.lineClear=()=>true;g.world.terrain=()=>({biome:'farmland',road:true,water:false});
 g.world.vehicleBlocked=()=>false;g.world.vehicleStaging=(site,kind,index)=>({x:site.x+index*65,y:site.y});
 for(let i=0;i<population;i++)g.makeApe((i%30)*12,Math.floor(i/30)*12,'hold');
 g.time=200;g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.updateResponseStage();return{c,g};
}
function base(g,x=1500,y=0){
 const site={id:'base-'+x+','+y,name:'Iron Road',x,y,tier:5,military:true,guards:0,strength:260,initialStrength:260,spawned:true,objects:[],lastRaid:-100,
 vehicleInventory:{jeep:4,command:3,truck:8,apc:8,ifv:4,tank:8,heli:6},armorCapacity:220,
 staging:[{x,y}],approach:[{x,y},{x:x*.65,y:y*.65}],roadblocks:[{x:x*.65,y:y*.65}]};
 g.world.sites.set(site.id,site);return site;
}
function town(g,population=250){
 const s={id:'moonroot',name:'Moonroot',x:400,y:400,known:true,population,level:7,radius:300,food:10000,lastRaid:-100,scouts:3,children:0};
 g.settlements.push(s);for(const a of g.apes.slice(-population)){a.state='settled';a.settlementId=s.id;a.x=s.x+(a.x%100)-50;a.y=s.y+(a.y%100)-50}
 g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);return s;
}
function regionalWar(){
 const {c,g}=setup(900),s=town(g);for(const[x,y]of [[1600,0],[-1600,0],[0,1600],[0,-1600],[1500,1500],[-1500,-1500],[2400,1500],[-2400,1500]])base(g,x,y);
 g.addIntel(0,0,30);g.responseDirector();return{c,g,s};
}

test('civilian growth sustains escalating military pressure and grows inside a war band',()=>{
 const {g}=setup(500);for(const a of g.apes.slice(20))a.state='settled';g.updateResponseStage();
 assert.equal(g.followers.length,20);assert.equal(g.warIntensity,3);assert.ok(g.tier<5,'visible horde minimum remains separate');
 const townBudget=g.responseBudget;for(const a of g.apes)a.state='hold';assert.equal(g.responseBudget,townBudget,'recruiting residents does not reset regional commitment');
 const stages=[];for(const n of [300,400,450,600,650,800,850,1000]){const{g:war}=setup(n);stages.push({n,budget:war.responseBudget,people:war.activeHumanCapacity(),interval:war.directorInterval,slots:war.operationCapacity})}
 for(let i=1;i<stages.length;i++){assert.ok(stages[i].budget>stages[i-1].budget);assert.ok(stages[i].people>stages[i-1].people);assert.ok(stages[i].interval<stages[i-1].interval);assert.ok(stages[i].slots>=stages[i-1].slots)}
});

test('total war commits concurrent independent objectives without exceeding real resources',()=>{
 const {g,s}=regionalWar(),channels=new Set(g.activeOperations().map(op=>op.channel));
 for(const channel of ['field','regional','roadblock','recon'])assert.ok(channels.has(channel),channel+' is active');
 assert.ok(g.activeOperations().length<=g.operationCapacity);assert.ok(g.forceWeight()<=g.responseBudget);
 assert.ok(g.humans.length+g.vehicles.reduce((n,v)=>n+(v.troops||0),0)<=g.activeHumanCapacity());
 const allocation=g.events.filter(e=>e.type==='raid').reduce((n,e)=>n+e.people,0)+g.roadblockOperations.reduce((n,op)=>n+op.members.length,0);
 const spent=[...g.world.sites.values()].reduce((n,b)=>n+260-b.strength,0);assert.equal(spent,allocation,'every army soldier comes from an installation');
 const assault=g.activeOperations('regional')[0],vehicle=g.vehicles.find(v=>v.operationId===assault.id),old={...vehicle.target};
 g.king.x=9000;g.redirectFieldForces({x:3000,y:-3000});assert.equal(vehicle.target.x,old.x);assert.equal(vehicle.target.y,old.y);assert.equal(assault.target.id,s.id);
 assert.ok([...g.forces.platoons.values()].some(p=>p.squads.length>=2),'multiple squads share command');
});

test('settlement assault stages outside the village, deploys, prepares then advances with outer armor',()=>{
 const {g,s}=regionalWar(),op=g.activeOperations('regional')[0];assert.equal(op.phase,'recon');assert.ok(Math.hypot(op.staging.x-s.x,op.staging.y-s.y)>s.radius);
 g.time=op.phaseAt+8;g.tickMilitaryOperations();assert.equal(op.phase,'approach');
 for(const h of g.humans.filter(h=>h.operationId===op.id)){h.x=op.staging.x;h.y=op.staging.y}
 for(const v of g.vehicles.filter(v=>v.operationId===op.id)){v.x=op.staging.x;v.y=op.staging.y;v.troops=0}
 g.syncIndexes();g.time++;g.tickMilitaryOperations();assert.equal(op.phase,'deployment');
 g.time=op.phaseAt+8;g.tickMilitaryOperations();assert.equal(op.phase,'engineers');assert.ok(op.engineerJobs.length>0);
 assert.ok(g.humans.filter(h=>h.operationId===op.id&&h.role==='engineer').some(h=>h.engineerJob));
 assert.ok([...g.forces.squads.values()].filter(q=>q.operationId===op.id).every(q=>q.order==='Hold Line'));
 g.time=op.phaseAt+12;g.tickMilitaryOperations();assert.equal(op.phase,'assault');assert.equal(s.attack,true);
 for(const v of g.vehicles.filter(v=>v.operationId===op.id&&['tank','apc','ifv'].includes(v.kind)))assert.ok(Math.hypot(v.target.x-s.x,v.target.y-s.y)>s.radius,'armor supports outside the dense village');
 assert.deepEqual(Array.from(g.events.filter(e=>e.type==='assault-phase'&&e.operationId===op.id),e=>e.phase),['approach','deployment','engineers','assault']);
});

test('roadblocks require protected engineers and retain destroyed sections as battlefield history',()=>{
 const {g}=setup(900),source=base(g);assert.equal(g.deployRoadblock(source,{x:0,y:0}),true);const op=g.roadblockOperations[0];
 for(const id of op.engineers){const h=g.humansById.get(id);h.x=op.point.x;h.y=op.point.y}
 g.tickRoadblocks();assert.equal(op.phase,'building');assert.equal(op.built,false);assert.equal(g.world.objects.size,0,'arrival alone creates no barricades');
 const engineer=g.humansById.get(op.engineers[0]),job=engineer.engineerJob;assert.ok(job.remaining>=4&&job.remaining<=7);engineer.x=job.x;engineer.y=job.y;
 g.forces.updateEngineer(engineer,3);assert.equal(g.world.objects.size,0);g.forces.updateEngineer(engineer,5);g.tickRoadblocks();assert.equal(op.built,true);
 const barrier=g.world.objects.get(engineer.lastEngineerJob.objectId);assert.equal(barrier.type,'humanBarricade');assert.equal(barrier.maxHp,600);
 g.damageObject(barrier,10000,g.king);assert.equal(barrier.dead,true);assert.equal(barrier.solid,false);assert.equal(barrier.damageStage,'destroyed');assert.equal(g.world.objects.has(barrier.id),true);
 const exposed=g.humansById.get(op.engineers[1]);exposed.hitTimer=.3;g.forces.updateEngineer(exposed,8);assert.equal(exposed.lastEngineerJob.status,'interrupted');assert.equal(g.world.objects.size,1,'attacked builders create no completed cover');
});

test('save/load preserves active assault phases, platoons, engineer progress and independent roadblocks',()=>{
 const {c,g}=regionalWar(),op=g.activeOperations('regional')[0];op.phase='engineers';op.phaseAt=g.time;
 const engineer=g.humans.find(h=>h.operationId===op.id&&h.role==='engineer');const jobId=g.forces.startEngineerJob(engineer,{x:engineer.x+50,y:engineer.y,kind:'heavy',operationId:op.id});engineer.engineerJob.remaining=3.25;op.engineerJobs.push(jobId);
 const snapshot=JSON.parse(JSON.stringify(g.serialize())),savedBudget=g.responseBudget,loaded=c.ATSGame.fromJSON(snapshot),restored=loaded.activeOperations('regional').find(x=>x.id===op.id);
 assert.equal(restored.phase,'engineers');assert.equal(restored.phaseAt,op.phaseAt);assert.equal(loaded.responseBudget,savedBudget);
 const builder=loaded.humansById.get(engineer.id);assert.equal(builder.engineerJob.remaining,3.25);assert.equal(loaded.forces.engineerJobs.get(jobId),builder.engineerJob);
 assert.equal(loaded.forces.platoons.size,g.forces.platoons.size);for(const p of loaded.forces.platoons.values())for(const id of p.squads)assert.ok(loaded.forces.squads.has(id));
 for(const roadblock of loaded.roadblockOperations)assert.equal(loaded.militaryOperations.find(x=>x.id===roadblock.id),roadblock,'one operation record owns the live construction state');
 assert.equal(loaded.nextReconAt,g.nextReconAt);assert.equal(loaded.nextInterceptAt,g.nextInterceptAt);
});

test('infantry rounds clear waist-high firing cover while movement still collides',()=>{
 const {g}=setup(),barrier=g.world.createFortification({x:60,y:0,team:'human',kind:'basic',w:20,h:90});g.king.x=-1000;
 const ape=g.makeApe(120,0,'hold');g.apeGrid.rebuild([g.king,...g.apes]);
 assert.equal(g.navigation.clearSegment(0,0,120,0,2,'ape'),false);assert.equal(g.projectileSegmentClear(0,0,120,0),true);
 g.bullets.push(g.bulletPool.take({x:0,y:0,px:0,py:0,vx:550,vy:0,damage:12,life:1,owner:'human-rifle'}));g.updateBullets(.25);
 assert.equal(ape.hp,108);assert.equal(barrier.hp,300);assert.equal(g.bullets.length,0);
});

test('near and distant residents use their colony activity and friendly barrier traversal profile',()=>{
 const {g}=setup(),s={id:'home',x:4000,y:0,name:'Home',population:1,attack:false,radius:100};g.settlements.push(s);
 const ape=g.makeApe(4000,0,'settled',s.id);g.syncIndexes();let calls=0,profile=null;
 g.colonies.activityTarget=(a,colony)=>{calls++;a.activity='hauling timber';return{x:colony.x+100,y:colony.y,speed:40}};
 g.move=(a,dx,dy,speed,dt)=>{a.x+=Math.sign(dx)*speed*dt;a.moving=true};g.updateApe(ape,.5);assert.equal(ape.x,4020);assert.equal(ape.activity,'hauling timber');
 g.navigation.clearSegment=(ax,ay,bx,by,r,p)=>{profile=p;return true};g.abstractActor(ape,.5,'ape');assert.equal(ape.x,4040);assert.equal(profile,'ape');assert.equal(calls,2);
});

test('source reconstruction consumes a finite reserve and destroyed infrastructure cancels remaining supply',()=>{
 const {g}=setup(),s=base(g);s.strength=0;s.vehicleInventory.tank=0;s.initialInventory={tank:2};s.campaignReserve={personnel:40,vehicles:{tank:2},armor:24};s.nextMobilizationAt=300;
 assert.equal(g.world.replenishInstallation(s,299),false);assert.equal(s.strength,0);g.world.replenishInstallation(s,300);assert.equal(s.strength,33);assert.equal(s.campaignReserve.personnel,7);assert.equal(s.vehicleInventory.tank,1);
 s.barracksDown=s.depotDown=true;g.world.replenishInstallation(s,390);assert.equal(s.strength,33);assert.equal(s.campaignReserve.personnel,0);assert.equal(Object.keys(s.campaignReserve.vehicles).length,0);
 g.world.replenishInstallation(s,480);assert.equal(s.strength,33);assert.equal(s.vehicleInventory.tank,1,'no infinite replacement army');
});
