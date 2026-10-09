'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function home(population=40){
 const c=loadEngine(),g=new c.ATSGame('RENEWABLE-VILLAGE');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.terrain=()=>({biome:'rocky',water:false});g.world.getSites=()=>[];g.world.settlementPlot=()=>({valid:true,trees:[],rocks:[],blocked:[]});
 const s={id:'renewable',name:'Stonegrove',x:0,y:0,population,radius:100,level:1,food:100,wood:0,livingFounding:true,birthTimer:-1e9};
 g.settlements.push(s);for(let i=0;i<population;i++)g.makeApe(0,0,'settled',s.id);g.syncIndexes();g.refreshSettlements();g.colonies.init(s);s.suitability={fertility:0,water:false};g.king.x=g.king.y=0;return{c,g,s};
}
function building(s,kind,i=0){const b={id:kind+'-'+i,kind,x:140+i*150,y:150,hp:100,maxHp:100,stage:4};s.structures.push(b);return b}
function infrastructure(g,s,gardens=4,workshops=1){for(let i=0;i<gardens;i++)building(s,'garden',i);for(let i=0;i<workshops;i++)building(s,'workShelter',i);g.colonies.assignJobs(s,g.colonies.members(s));for(const a of g.apes){const p=g.colonies.activityTarget(a,s);a.x=p.x;a.y=p.y}}
function economy(g,s,seconds){g.colonies.plan=()=>{};g.colonies.work=()=>{};for(let i=0;i<seconds;i++){g.time++;g.colonies.tick(s)}}

test('poor-soil gardens sustain 40 and 100 residents without berries or hidden free income',()=>{
 for(const [population,count]of [[40,4],[100,9]]){const {g,s}=home(population);infrastructure(g,s,count,0);const before=s.food;economy(g,s,30);assert.ok(s.gardenFoodRate>=count*.7);assert.ok(s.food>before,`${population} residents recover reserves`);assert.equal(s.wood,0)}
 const {g,s}=home();s.food=100;economy(g,s,10);assert.equal(s.gardenFoodRate,0);assert.ok(s.food<100);assert.equal(s.wood,0);
});

test('gardens keep a .7 floor without staff; actual gardening and fertile water improve it',()=>{
 const {g,s}=home();const garden=building(s,'garden');g.colonies.assignJobs(s,g.colonies.members(s));const worker=g.apes.find(a=>a.job==='gardener');worker.x=9000;assert.equal(g.colonies.production(s).food,.7);worker.x=garden.x;worker.y=garden.y;assert.ok(Math.abs(g.colonies.production(s).food-.9)<1e-9);s.suitability={fertility:1,water:true};assert.ok(g.colonies.production(s).food>1.2);garden.hp=0;assert.equal(g.colonies.production(s).food,0);
});

test('workshops produce .2 timber per second only with a living present adult',()=>{
 const {g,s}=home();infrastructure(g,s,4);const worker=g.apes.find(a=>a.job==='workshop');assert.ok(worker);assert.equal(g.colonies.production(s).wood,.2);economy(g,s,30);assert.ok(Math.abs(s.wood-6)<1e-8);worker.x=9000;assert.equal(g.colonies.production(s).wood,0);worker.x=185;worker.y=164;worker.hp=0;assert.equal(g.colonies.production(s).wood,0);
});

test('additional workshops have modest diminishing returns, specialization and a storage cap',()=>{
 const {g,s}=home();infrastructure(g,s,4,3);const ordinary=g.colonies.production(s).wood;assert.ok(ordinary>.5&&ordinary<.6);s.specialization='workshop';assert.ok(Math.abs(g.colonies.production(s).wood-ordinary*1.2)<1e-8);s.wood=g.colonies.woodCapacity(s)-.01;economy(g,s,3);assert.equal(s.wood,g.colonies.woodCapacity(s));const workshop=s.structures.find(o=>o.kind==='workShelter');workshop.hp=0;assert.ok(g.colonies.production(s).wood<ordinary);
});

test('equal elapsed on-screen and offscreen economy produces equal food and timber',()=>{
 const visible=home(),remote=home();for(const {g,s}of [visible,remote])infrastructure(g,s,4,2);remote.g.king.x=6000;economy(visible.g,visible.s,60);economy(remote.g,remote.s,60);assert.equal(remote.s.simLOD,2);assert.ok(Math.abs(visible.s.food-remote.s.food)<1e-8);assert.ok(Math.abs(visible.s.wood-remote.s.wood)<1e-8);
});

test('job assignments follow real opportunities and remain stable between unchanged needs',()=>{
 const {g,s}=home();g.colonies.assignJobs(s,g.colonies.members(s));assert.equal(g.apes.some(a=>['gardener','workshop','forager','lumber','hauler'].includes(a.job)),false);infrastructure(g,s,2,2);assert.equal(s.gardeners,2);assert.equal(s.workshopWorkers,2);assert.ok(s.guards>=2);const roles=g.apes.map(a=>[a.id,a.job,a.gardenId,a.workshopId]);for(let i=0;i<10;i++)g.colonies.assignJobs(s,g.colonies.members(s));assert.deepEqual(g.apes.map(a=>[a.id,a.job,a.gardenId,a.workshopId]),roles);const child=g.makeApe(0,0,'young',s.id,true);g.refreshSettlements();g.colonies.assignJobs(s,g.colonies.members(s));assert.equal(child.job,'young');
});

test('expedition members cannot be reassigned, staffed or counted as local producers',()=>{
 const {g,s}=home();infrastructure(g,s);const worker=g.apes.find(a=>a.job==='workshop');worker.kingdomMission={id:'trip'};worker.job='expedition';g.colonies.assignJobs(s,g.apes);g.colonies.staffFacilities(s);assert.equal(worker.job,'expedition');assert.equal(worker.towerId,undefined);assert.equal(worker.trainingFacilityId,undefined);assert.ok(!s._adults.includes(worker));
});

test('food emergencies prioritize a garden over expanding already full homes',()=>{
 const {g,s}=home(40);s.food=1;s.wood=6;g.colonies.assignJobs(s,g.colonies.members(s));g.colonies.plan(s);assert.equal(s.projects[0].kind,'garden');assert.equal(s.wood,6,'planning reserves but does not charge an unbuilt automatic project');
});

test('raid-damaged productive infrastructure is repaired without duplicate buildings',()=>{
 const {g,s}=home();const workshop=building(s,'workShelter');workshop.hp=0;s.wood=20;s.food=300;g.colonies.assignJobs(s,g.colonies.members(s));g.colonies.plan(s);const p=s.projects.find(p=>p.kind==='repairFacility');assert.equal(p.structureId,workshop.id);g.colonies.complete(s,p);assert.equal(workshop.hp,100);assert.equal(s.structures.filter(o=>o.kind==='workShelter').length,1);
});

test('250 commissions share a cached task board and bounded incremental plot and work budgets',()=>{
 const {g,s}=home(250);s.wood=s.food=100000;g.world.settlementPlot=()=>({valid:false,pending:true,trees:[],blocked:[]});for(let i=0;i<250;i++)assert.equal(g.colonies.commission(s.id,'hut').ok,true);assert.equal(s.projects.length,250);g.colonies.assignJobs(s,g.colonies.members(s));g.colonies.board(s);const builds=g.colonies.counters.boardBuilds;for(let pass=0;pass<10;pass++)for(const a of g.apes)g.colonies.activityTarget(a,s);assert.equal(g.colonies.counters.boardBuilds,builds,'workers reuse the shared queue');const checks=g.colonies.counters.plotChecks;g.colonies.work(s);assert.ok(g.colonies.counters.plotChecks-checks<=96);assert.ok(g.colonies.counters.projectsWorked<=8);assert.equal(s.wood,98000);
});

test('a delayed paid commission restores its cursor, charges once and completes once',()=>{
 const {c,g,s}=home(20);s.wood=50;s.food=100;g.world.settlementPlot=()=>({valid:false,pending:true,trees:[],blocked:[]});const result=g.colonies.commission(s.id,'garden'),wood=s.wood;assert.equal(result.ok,true);const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),restored=loaded.settlement(s.id);loaded.world.settlementPlot=()=>({valid:true,trees:[],blocked:[]});loaded.colonies.locateCommissions(restored);const p=restored.projects[0];loaded.colonies.complete(restored,p);loaded.colonies.complete(restored,p);assert.equal(restored.wood,wood);assert.equal(restored.structures.filter(o=>o.kind==='garden').length,1);assert.equal(restored.commissions[0].status,'complete');
});

test('legacy production counts migrate once to real gardens without reviving modern ruins',()=>{
 const {g}=home(),old={id:'legacy',name:'Old orchard',x:2500,y:0,population:10,economyVersion:2,housing:16,gardens:3,cooking:1,stores:1,food:48,wood:19,suitability:{fertility:0,water:false}};g.settlements.push(old);g.colonies.init(old);assert.equal(old.structures.filter(o=>o.kind==='garden').length,3);assert.equal(old.structures.filter(o=>o.kind==='storage').length,1);assert.equal(old.food,48);assert.equal(old.wood,19);const garden=old.structures.find(o=>o.kind==='garden');garden.hp=0;for(let i=0;i<5;i++)g.colonies.init(old);assert.equal(old.structures.filter(o=>o.kind==='garden').length,3);assert.equal(g.colonies.production(old).food,1.4);assert.equal(garden.hp,0);
});

test('full storage keeps returned cargo at home until it can be deposited exactly once',()=>{
 const {g,s}=home();infrastructure(g,s);const worker=g.apes.find(a=>a.job==='workshop');worker.expeditionCargo={sourceId:s.id,resource:'wood',amount:5};worker.x=worker.y=0;s.wood=g.colonies.woodCapacity(s);assert.equal(g.kingdom.depositExpeditionCargo(worker),true);assert.equal(worker.expeditionCargo.amount,5);const target=g.colonies.activityTarget(worker,s);assert.equal(target.x,s.x);assert.equal(target.y,s.y);assert.equal(g.colonies.production(s).wood,0);s.wood-=5;g.kingdom.depositExpeditionCargo(worker);assert.equal(worker.expeditionCargo,undefined);assert.equal(s.wood,g.colonies.woodCapacity(s));assert.equal(g.kingdom.depositExpeditionCargo(worker),false);
});

test('local foragers cannot consume berries reserved by a traveling expedition',()=>{
 const {g,s}=home(),berry={id:'reserved-bush',type:'berry',x:150,y:0,food:50,count:50};s._berries=[berry];s._resourcesAt=Infinity;s.kingdomMissions.push({id:'reservation',kind:'expedition',resource:'food',status:'traveling',target:{resourceId:berry.id,x:berry.x,y:berry.y},members:[]});g.kingdom.tick=()=>{};economy(g,s,1);assert.equal(berry.food,50);assert.ok(s.foragers>0);assert.ok(Math.abs(s.food-(100-40*.06))<1e-8);
});

test('depleted villages keep essential staff but retain workers for distant expeditions',()=>{
 const {g,s}=home();infrastructure(g,s);g.colonies.plan(s);g.colonies.assignJobs(s,g.colonies.members(s));const candidates=g.kingdom.expeditionWorkers(s);assert.ok(candidates.length>=3);assert.ok(candidates.every(a=>!['gardener','workshop'].includes(a.job)));assert.ok(g.apes.filter(a=>a.job==='guardian'&&!candidates.includes(a)).length>=8);
});

test('new commissions do not interrupt a builder already carrying an automatic project delivery',()=>{
 const {g,s}=home();s.wood=100;const p=g.colonies.queue(s,'hut');g.colonies.assignJobs(s,g.colonies.members(s));const worker=g.apes.find(a=>a.job==='builder'),depot=g.colonies.zone(s,'wood');worker.x=depot.x;worker.y=depot.y;g.colonies.activityTarget(worker,s);assert.equal(worker.materialProjectId,p.id);g.colonies.commission(s.id,'workshop');g.colonies.assignJobs(s,g.colonies.members(s));g.colonies.activityTarget(worker,s);assert.equal(worker.materialProjectId,p.id);assert.equal(worker.workTargetId,p.id);
});

test('offscreen builders must physically travel, clear their plot and deliver before construction',()=>{
 const {g,s}=home(6);s.wood=50;g.king.x=6000;s.simLOD=2;const p=g.colonies.queue(s,'hut'),tree={id:'remote-trunk',type:'tree',x:p.x,y:p.y,r:16,moveRadius:16,size:1,hp:80,maxHp:80,solid:true};g.world.objects.set(tree.id,tree);g.world._indexObject(tree);p.treeIds=[tree.id];g.colonies.assignJobs(s,g.colonies.members(s));
 for(let i=0;i<10;i++){g.time++;g.colonies.work(s)}assert.equal(tree.dead,undefined);assert.equal(p.work,0);const before=g.apes.map(a=>({x:a.x,y:a.y}));
 for(let second=0;second<100&&!p.done;second++){for(let step=0;step<4;step++){g.time+=.25;g.navigation.beginFrame(g.time);for(const a of g.apes)g.abstractActor(a,.25,'ape')}g.colonies.work(s)}
 assert.ok(g.apes.some((a,i)=>Math.hypot(a.x-before[i].x,a.y-before[i].y)>60));assert.equal(tree.dead,true);assert.equal(p.done,true);assert.ok(p.deliveries>0);assert.equal(s.huts[0].stage,4);
});

test('population refresh cannot count guardians, mission workers or undeposited carriers as foragers',()=>{
 const {g,s}=home(),berry={id:'local-bush',type:'berry',x:150,y:0,food:50,count:50};s._berries=[berry];s._resourcesAt=Infinity;for(const a of g.apes)a.job='guardian';const carrier=g.apes[0];carrier.job='forager';carrier.expeditionCargo={sourceId:s.id,resource:'wood',amount:5};s._jobsAt=Infinity;s._jobsAttack=false;s._memberCount=g.apes.length;s.wood=g.colonies.woodCapacity(s);g.kingdom.tick=()=>{};
 g.refreshSettlements();economy(g,s,1);assert.equal(berry.food,50,'a full-storage carrier cannot also gather local food');delete carrier.expeditionCargo;g.refreshSettlements();economy(g,s,1);assert.ok(Math.abs(berry.food-49.68)<1e-9,'only the one real forager harvests after a population refresh');
});
