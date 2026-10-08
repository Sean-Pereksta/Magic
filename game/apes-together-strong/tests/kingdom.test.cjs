'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {loadEngine}=require('./performance-harness.cjs');
function game(){const c=loadEngine();for(const [name,key] of [['champions','ATSChampions'],['kingdom','ATSKingdom']])if(!c[key])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),c);const g=new c.ATSGame('kingdom-regression');g.world.getSites=()=>[];g.world.getObjects=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.blocked=()=>false;g.world.lineClear=()=>true;g.world.settlementPlot=()=>({valid:true,trees:[]});return{g,c}}
function home(g,id,x=0,n=24){const s={id,name:id,x,y:0,population:n,housing:n+20,food:500,wood:150,level:2,safety:1,birthTimer:-1000,known:false,age:0};g.settlements.push(s);for(let i=0;i<n;i++)g.makeApe(x+i%6*7,Math.floor(i/6)*7,'settled',id);g.refreshSettlements();g.colonies.init(s);s.suitability={fertility:1,wood:12,capacity:1000,water:false};s.gardens=5;g.colonies.assignJobs(s,g.colonies.members(s));return s}
function step(g,s,n=1){for(let i=0;i<n;i++){g.time++;g.refreshSettlements();g.colonies.tick(s)}}

test('old saves retain resources, wounds and general economy; strategy settings persist',()=>{
 const{g,c}=game(),s=home(g,'Legacy'),h=s.huts[0];h.hp=37;s.food=45;s.wood=19;delete s.kingdomVersion;delete s.specialization;
 g.colonies.init(s);assert.equal(s.specialization,'balanced');assert.equal(s.food,45);assert.equal(s.wood,19);assert.equal(h.hp,37);
 g.kingdom.specialize(s.id,'workshop');g.kingdom.posture(s.id,'fortified');g.kingdom.rally(s.id);s.gearKits=3;s.craftProgress=27;
 const restored=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),saved=restored.settlements[0];assert.equal(saved.specialization,'workshop');assert.equal(saved.defensePosture,'fortified');assert.equal(saved.rallyPoint,true);assert.equal(saved.gearKits,3);assert.equal(saved.craftProgress,27);assert.equal(saved.huts[0].hp,37);assert.equal(saved.food,45);assert.equal(saved.wood,19);
});

test('sanctuary beds, recovery and family growth are actual economy effects',()=>{
 const a=game().g,b=game().g,normal=home(a,'Home'),sanctuary=home(b,'Home');normal.birthTimer=sanctuary.birthTimer=0;const health=a.apes[0].hp=a.apes[0].maxHp-10;b.apes[0].hp=b.apes[0].maxHp-10;
 b.kingdom.specialize(sanctuary.id,'sanctuary');assert.equal(sanctuary.housing,normal.housing+sanctuary.huts.length*2);step(a,normal);step(b,sanctuary);assert.ok(sanctuary.birthTimer>normal.birthTimer);assert.ok(b.apes[0].hp>a.apes[0].hp);assert.ok(a.apes[0].hp>health);
 const capacity=sanctuary.housing;sanctuary.huts[0].hp=0;b.colonies.updateHousing(sanctuary);assert.equal(sanctuary.housing,capacity-sanctuary.huts[0].capacity-2);b.kingdom.specialize(sanctuary.id,'balanced');assert.equal(sanctuary.housing,6+sanctuary.huts.filter(h=>h.hp>0).reduce((n,h)=>n+h.capacity,0));
});

test('supply villages produce more garden food and workshops finish real construction faster',()=>{
 const a=game().g,b=game().g,normal=home(a,'Home'),supply=home(b,'Home');normal.food=supply.food=100;b.kingdom.specialize(supply.id,'supply');step(a,normal);step(b,supply);assert.ok(supply.food>normal.food);
 const makeProject=(g,s)=>{s.projects=[];s.builders=2;s.simLOD=2;g.time=10;const p={id:'test-project',kind:'garden',x:s.x+150,y:0,treeIds:[],createdAt:0,work:0,totalWork:100,timber:0};s.projects.push(p);return p};
 b.kingdom.specialize(supply.id,'workshop');const p=makeProject(a,normal),q=makeProject(b,supply);a.colonies.work(normal);b.colonies.work(supply);assert.ok(q.work>p.work);assert.equal(q.work,p.work*1.3);
});

test('war camps train residents and fortified posture reduces local incoming damage',()=>{
 const{g}=game(),s=home(g,'War camp',0,18);g.kingdom.specialize(s.id,'warcamp');g.colonies.assignJobs(s,g.colonies.members(s));const guards=s.guards;assert.ok(guards>=4);for(const a of g.apes)a.trainingProgress=45;
 g.kingdom.tick(s);assert.ok(g.apes.some(a=>a.trainingLevel===1));const trained=g.apes.find(a=>a.trainingLevel===1);assert.equal(trained.maxHp,trained.hp+12);
 s.defense=0;const normal=g.colonies.absorb(g.apes[0],100);g.kingdom.posture(s.id,'fortified');assert.ok(g.colonies.absorb(g.apes[0],100)<normal);g.kingdom.posture(s.id,'mobile');const guardian=g.apes.find(a=>a.job==='guardian');assert.ok(guardian);const mobile=g.colonies.activityTarget(guardian,s).speed;g.kingdom.posture(s.id,'balanced');assert.ok(mobile>g.colonies.activityTarget(guardian,s).speed);assert.ok(g.kingdom.counters.trainingCandidates<=12);
});

test('physical supply carriers cannot deliver remotely; deaths reduce cargo and saves cannot duplicate it',()=>{
 const{g,c}=game(),s=home(g,'Supply',0,18),target=home(g,'Target',900,12);const original=target.food,result=g.kingdom.sendSupplies(s.id,target.id);assert.equal(result.ok,true);const mission=result.mission,paid=s.food;
 g.kingdom.tick(s);assert.equal(target.food,original);const carriers=mission.members.map(id=>g.apesById.get(id));assert.ok(carriers.every(a=>a.x<100));assert.ok(carriers.every(a=>g.colonies.activityTarget(a,s).x>600&&g.colonies.activityTarget(a,s).x<=target.x));
 carriers[0].hp=0;for(const a of carriers.slice(1)){a.x=target.x;a.y=target.y}g.kingdom.tick(s);assert.equal(target.food,original+Math.floor(mission.members.length>0?40*2/3:0));assert.equal(s.food,paid);assert.equal(mission.returning,true);
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),from=loaded.settlement(s.id),to=loaded.settlement(target.id),delivered=to.food;loaded.kingdom.tick(from);assert.equal(to.food,delivered);for(const id of mission.members){const a=loaded.apesById.get(id);if(a?.hp>0){a.x=from.x;a.y=from.y}}loaded.kingdom.tick(from);assert.equal(from.kingdomMissions[0].status,'complete');assert.ok(loaded.apes.filter(a=>a.hp>0).every(a=>!a.kingdomMission));assert.equal(to.food,delivered);
});

test('reinforcements and evacuations move existing apes and change homes only on arrival',()=>{
 const{g}=game(),source=home(g,'Reserve',0,30),target=home(g,'Frontier',1000,12),before=g.apes.length,result=g.kingdom.requestReinforcements(target.id);assert.equal(result.ok,true);assert.equal(result.count,8);assert.equal(g.apes.length,before);
 const actors=result.mission.members.map(id=>g.apesById.get(id));g.kingdom.tick(source);assert.ok(actors.every(a=>a.settlementId===source.id));for(const a of actors){a.x=target.x;a.y=target.y}g.kingdom.tick(source);assert.ok(actors.every(a=>a.settlementId===target.id&&!a.kingdomMission));
 const child=g.makeApe(target.x,0,'young',target.id,true);g.refreshSettlements();g.colonies.assignJobs(target,g.colonies.members(target));const evacuation=g.kingdom.evacuate(target.id);assert.equal(evacuation.ok,true);assert.ok(evacuation.mission.members.includes(child.id));assert.equal(child.settlementId,target.id);assert.equal(child.state,'young');assert.equal(g.colonies.activityTarget(child,target).x,source.x);
 for(const id of evacuation.mission.members){const a=g.apesById.get(id);a.x=source.x;a.y=0}g.kingdom.tick(target);assert.equal(child.settlementId,source.id);assert.equal(child.state,'young');
});

test('prison survivors physically reach the rally village before its celebration begins',()=>{
 const{g}=game(),s=home(g,'Sanctuary',900,18);g.kingdom.specialize(s.id,'sanctuary');g.kingdom.rally(s.id);const survivor=g.makeApe(0,0,'follow');g.syncIndexes();const response=g.kingdom.welcomeSurvivors([survivor],{facilityName:'Blacksite K-12'});assert.equal(response.ok,true);assert.equal(s.moraleUntil,0);assert.equal(survivor.x,0);g.kingdom.tick(s);assert.equal(s.moraleUntil,0);survivor.x=s.x;g.kingdom.tick(s);assert.equal(s.moraleUntil,g.time+90);assert.equal(survivor.kingdomMission,undefined);assert.ok(s.kingdomEvents.some(e=>e.kind==='survivors'&&e.text.includes('K-12')));for(let i=0;i<20;i++)g.kingdom.event(s,'test-'+i,'event','green',0);assert.equal(s.kingdomEvents.length,8);
});

test('workshop materials consume resources and produce usable shields and capped champion gear',()=>{
 const{g}=game(),s=home(g,'Forge',0,18);g.kingdom.specialize(s.id,'workshop');const food=s.food,wood=s.wood;
 for(let i=0;i<45;i++){g.time++;g.kingdom.tick(s)}assert.equal(s.gearKits,1);assert.equal(s.siegeMaterials,1);assert.equal(s.food,food-6);assert.equal(s.wood,wood-12);
 const ape=g.apes[0];ape.species='gorilla';g.apeGrid.rebuild([g.king,...g.apes]);assert.equal(g.kingdom.issueShields(s.id).ok,true);assert.equal(ape.shield.maxHp,225);assert.equal(ape.shield.reinforced,true);assert.equal(s.siegeMaterials,0);g.champions.promote(ape,{archetype:'bulwark'});assert.equal(g.kingdom.refit(s.id).ok,true);assert.equal(s.gearKits,0);assert.equal(ape.champion.gearLevel,1);assert.equal(g.kingdom.refit(s.id).ok,false);
});

test('scout outposts reveal more terrain and issue bounded advance warnings without a local attack',()=>{
 const{g}=game(),s=home(g,'Watch',0,18);g.kingdom.specialize(s.id,'scout');let revealed=0;g.world.reveal=(x,y,r)=>{revealed=r};const threat=g.makeHuman(900,0);threat.state='search';g.humanGrid.rebuild(g.humans);g.kingdom.tick(s);assert.ok(revealed>700);assert.equal(s.warning.direction,'east');assert.equal(s.attack,undefined);assert.ok(g.kingdom.counters.warningCandidates<=48);const count=s.kingdomEvents.filter(e=>e.kind==='raid-warning').length;g.kingdom.tick(s);assert.equal(s.kingdomEvents.filter(e=>e.kind==='raid-warning').length,count);
});

test('travel cohorts are bounded and failed commands do not spend stores or move children',()=>{
 const{g}=game(),s=home(g,'Origin',0,80);g.king.x=1000;for(let i=0;i<4;i++)assert.equal(g.kingdom.sendSupplies(s.id).ok,true);const before=s.food,wood=s.wood;assert.equal(g.kingdom.sendSupplies(s.id).ok,false);assert.equal(s.food,before);assert.equal(s.wood,wood);g.kingdom.tick(s);assert.equal(s.kingdomMissions.length,4);assert.ok(g.kingdom.counters.missionMembers<=48);assert.equal(g.kingdom.evacuate(s.id).ok,false);assert.equal(g.kingdom.specialize(s.id,'bogus').ok,false);
});

test('Q and absolute recall mobilize nearby travelers and cancel stale caravan reservations',()=>{
 for(const cmd of ['call','recallAll']){const{g}=game(),s=home(g,'Home',0,18),result=g.kingdom.sendSupplies(s.id),carrier=g.apesById.get(result.mission.members[0]);assert.ok(carrier.kingdomMission);g.commandCD=0;g.command(cmd);assert.equal(carrier.state,'follow');assert.equal(carrier.settlementId,null);g.kingdom.tick(s);assert.equal(carrier.kingdomMission,undefined);assert.equal(result.mission.status,'cancelled')}
});

test('actual distant caravan navigation detours around a solid tree, delivers and returns home',()=>{
 const{g,c}=game(),s=home(g,'Origin',0,18),w=new c.ATSWorld('caravan-route');w.ensure=()=>{};w.terrain=()=>({biome:'forest',water:false,road:false});g.world=w;g.navigation=new c.ATSNavigation(w);g.king.x=480;g.king.y=0;
 const tree={id:'journey-trunk',type:'tree',x:190,y:0,r:50,moveRadius:50,hp:100,solid:true,dead:false};w.objects.set(tree.id,tree);w._indexObject(tree);
 const before=g.food,result=g.kingdom.sendSupplies(s.id),actors=result.mission.members.map(id=>g.apesById.get(id));let detoured=false;
 for(let i=0;i<100&&result.mission.status==='traveling';i++){g.time+=.5;g.navigation.beginFrame(g.time,{budgetMs:Infinity});if(i%2===0)g.kingdom.tick(s);for(const a of actors)if(a.kingdomMission){g.abstractActor(a,.5,'ape');assert.equal(w.actorBlocked(a.x,a.y,10,'ape'),false);if(Math.abs(a.y)>55)detoured=true}}
 assert.equal(result.mission.status,'complete',JSON.stringify({mission:result.mission,actors:actors.map(a=>({x:a.x,y:a.y,nav:a._nav,activity:a._activityTarget})),stats:g.navigation.stats}));assert.equal(g.food,before+40);assert.ok(detoured,'route bends around the trunk rather than passing through it');assert.ok(actors.every(a=>Math.hypot(a.x-s.x,a.y-s.y)<75));assert.ok(g.navigation.stats.frameExpanded<=192);
});
