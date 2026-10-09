'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function fixture(n=12){
 const c=loadEngine(),g=new c.ATSGame('VILLAGE-STRATEGY'),w=g.world;
 w.objects.clear();w._spatial.clear();w.sites.clear();w._streaming=false;w.ensure=()=>{};w.stream=()=>{};w.getSites=()=>[];w.terrain=()=>({biome:'forest',water:false});w.waterBlocked=()=>false;
 const s={id:'village',name:'Willow',x:0,y:0,population:n,food:2000,wood:500,livingFounding:true};g.settlements.push(s);
 for(let i=0;i<n;i++)g.makeApe(-50,i*3,'settled',s.id);g.refreshSettlements();g.colonies.init(s);
 s.huts.push({id:'home',kind:'hut',x:-140,y:140,hp:100,maxHp:100,stage:4,capacity:1000});g.colonies.updateHousing(s);w.syncSettlementBuildings(s,g);
 g.king.x=g.king.y=0;g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;g.performance.beginStep(g);
 return{c,g,w,s};
}
const nursery=(s,i)=>s.facilities.push({id:'nursery-'+i,kind:'nursery',hp:180,stage:4});
function grids(g){g.syncIndexes();g.apeGrid.rebuild(g.apes,g.king);g.humanGrid.rebuild(g.humans);g.performance.beginStep(g);g.navigation.beginFrame(g.time)}
function base(g,x=1200){const b={id:'base',name:'Human fort',x,y:0,tier:2,strength:100,objects:[],guards:10,spawned:true,lastRaid:-100};g.world.sites.set(b.id,b);return b}
function survey(g,s){assert.ok(g.colonies.completePerimeter(s).ok);for(let i=0;i<100&&s.perimeter.stage==='surveying';i++)g.colonies.advancePerimeter(s);assert.equal(s.perimeter.stage,'building')}
function build(g,s){
 s.builders=12;s.cohorts=[];s._members=g.apes;s.essentialWoodReserve=0;
 for(let turn=0;turn<1500&&s.perimeter.stage!=='complete';turn++){
  g.time++;g.colonies.advancePerimeter(s);const active=g.colonies.board(s).active;
  g.apes.forEach((a,i)=>{const p=active[i%Math.max(1,active.length)];a.job='builder';if(p){a.x=p.x;a.y=p.y;a.workTargetId=p.id}});g.colonies.work(s);
 }
 assert.equal(s.perimeter.stage,'complete');
}
test('small villages reproduce at about half the old rate and nursery returns diminish',()=>{
 const {g,s}=fixture(8),baseline=g.colonies.familyGrowth(s).rate,old=1.06;assert.ok(baseline/old>=.45&&baseline/old<=.6);
 for(const [i,expected]of[[0,.35],[1,.55],[2,.67],[3,.69]]){nursery(s,i);assert.ok(Math.abs(g.colonies.growthFacilities(s).nurseryBonus-expected)<1e-10)}
 for(let i=4;i<100;i++)nursery(s,i);assert.equal(g.colonies.growthFacilities(s).nurseryBonus,.75);assert.ok(g.colonies.familyGrowth(s).rate<baseline*1.8);
});
test('food and actual housing stop births without deleting residents or recruiting restrictions',()=>{
 const {g,s}=fixture(8);s.food=0;s.birthTimer=500;g.colonies.plan=()=>{};g.colonies.work=()=>{};g.colonies.resources=()=>{};g.colonies.tick(s);assert.equal(g.stats.born,0);assert.match(s.growthStatus,/food/);assert.equal(s.population,8);
 s.food=500;s.housing=s.population;assert.equal(g.colonies.familyGrowth(s).rate,0);s.housing++;assert.ok(g.colonies.familyGrowth(s).rate>0);
 const a=g.makeApe(0,0,'follow');assert.ok(a);assert.equal(g.population,9);
});
test('large populations have bounded growth, ruined and unfinished nurseries confer nothing',()=>{
 const {g,s}=fixture(40),small=g.colonies.familyGrowth(s).rate;for(let i=40;i<400;i++)g.makeApe(0,0,'settled',s.id);g.refreshSettlements();s.food=10000;const large=g.colonies.familyGrowth(s).rate;assert.ok(large<small*2);
 s.facilities.push({kind:'nursery',hp:0,stage:4},{kind:'nursery',hp:100,stage:2});assert.equal(g.colonies.familyGrowth(s).rate,large);
});
test('five population tiers use delayed downgrade hysteresis and buildings add attention',()=>{
 const {g,s}=fixture();for(const [population,name]of[[1,'Outpost'],[16,'Hamlet'],[40,'Village'],[80,'Township'],[150,'Stronghold']]){s.population=population;assert.equal(g.colonies.development(s).name,name)}
 s.population=149;g.time+=40;assert.equal(g.colonies.development(s).tier,5);s.population=110;g.colonies.development(s);g.time+=31;assert.equal(g.colonies.development(s).tier,4);
 const before=g.colonies.development(s).attention;nursery(s,0);assert.ok(g.colonies.development(s).attention>before);
});
test('scouts need facing and unobstructed physical sight before a report exists',()=>{
 const {g,w,s}=fixture(),h=g.makeHuman(-110,0,null);h.dir=Math.PI;grids(g);assert.equal(g.colonies.observeVillage(s,h),false);h.dir=0;
 const visible=g.lineVisible;g.lineVisible=()=>false;assert.equal(g.colonies.observeVillage(s,h),false);g.lineVisible=visible;
 assert.equal(g.colonies.observeVillage(s,h),true);assert.equal(s.known,undefined);assert.equal(g.colonies.intel(s).state,'Suspected');assert.equal(g.colonies.canRaid(s),false);
});
test('a surviving radio scout takes four uninterrupted seconds to report; dead scouts reveal nothing',()=>{
 const {g,s}=fixture(),h=g.makeHuman(-110,0,null);h.dir=0;h.hasRadio=true;grids(g);assert.ok(g.colonies.observeVillage(s,h));
 g.colonies.scoutHuman(h,2);assert.ok(!s.known);h.hitTimer=.2;g.colonies.scoutHuman(h,2);h.hitTimer=0;g.colonies.scoutHuman(h,3);assert.ok(!s.known);g.colonies.scoutHuman(h,1);assert.equal(s.known,true);assert.equal(g.colonies.intel(s).state,'Discovered');assert.equal(g.colonies.canRaid(s),false,'preparation grace');
 g.time+=46;assert.equal(g.colonies.canRaid(s),true);
 const other=fixture(),dead=other.g.makeHuman(-110,0,null);dead.dir=0;grids(other.g);other.g.colonies.observeVillage(other.s,dead);dead.hp=0;assert.equal(other.g.colonies.communicateVillage(dead,'radio'),false);assert.ok(!other.s.known);
});
test('couriers must physically return or reach a radio patrol, and a local shout is not discovery',()=>{
 const {g,s}=fixture(),b=base(g),h=g.makeHuman(-110,0,b);h.dir=0;h.hasRadio=false;grids(g);g.colonies.observeVillage(s,h);g.alert(h,'shout');assert.ok(!s.known);
 g.colonies.scoutHuman(h,.1);assert.ok(!s.known);assert.ok(h.x>-110,'messenger starts a physical return');h.x=b.x;h.y=b.y;g.colonies.scoutHuman(h,.1);assert.equal(s.known,true);
});
test('radio sabotage prevents quick reporting and old intelligence expires',()=>{
 const {g,s}=fixture(),b=base(g);b.radioDown=true;const h=g.makeHuman(-110,0,b);h.hasRadio=true;h.dir=0;grids(g);g.colonies.observeVillage(s,h);g.colonies.scoutHuman(h,5);assert.ok(!s.known);
 g.colonies.reportVillage(s,{x:12,y:34});g.time=901;g.colonies.threatTick(s);assert.equal(s.known,false);assert.equal(s.humanIntel.position,null);assert.equal(g.colonies.canRaid(s),false);
});
test('search parties receive coarse regions, stay active offscreen, and respect global caps',()=>{
 const {g,s}=fixture(80);base(g);g.colonies.intel(s).nextScoutAt=0;assert.equal(g.colonies.dispatchScouts(s),true);assert.equal(g.humans.length,4);
 for(const h of g.humans){assert.equal(h.scoutMission.settlementId,undefined);assert.notEqual(h.lastX,s.x);assert.equal(h.raidTarget,undefined);g.king.x=10000;assert.equal(g.actorTier(h),1)}
 for(let i=0;i<3;i++){s.humanIntel.nextScoutAt=0;g.colonies.dispatchScouts(s)}assert.ok(g.humans.length<=12);
});

test('scouting never borrows nonexistent garrison personnel or exceeds twelve active scouts',()=>{
 const {g,s}=fixture(80),b=base(g);b.strength=3;s.humanIntel={state:'Undetected',nextScoutAt:0};assert.ok(g.colonies.dispatchScouts(s));assert.equal(g.humans.length,3);assert.equal(b.strength,0);
 for(let i=3;i<11;i++){const h=g.makeHuman(1200,0,b);h.scoutMission={}}b.strength=50;s.humanIntel.nextScoutAt=0;assert.equal(g.colonies.dispatchScouts(s),false);assert.equal(g.humans.length,11);assert.equal(b.strength,50);
});
test('targeted raids require intelligence, scale to the village and never spawn inside a perimeter',()=>{
 const {g,s}=fixture(16),b=base(g);g.time=200;g.prepareMilitarySource=()=>true;g.responsePackage=()=>({name:'Patrol',stage:1,people:30,vehicles:[],capacity:{}});
 assert.equal(g.spawnRaid(b,s,true),false);g.colonies.reportVillage(s,s);g.time+=46;assert.equal(g.spawnRaid(b,s,true),true);assert.ok(g.humans.length<=7);assert.ok(g.humans.every(h=>Math.abs(h.x-s.x)>g.colonies.defenseExtent(s)));assert.equal(s.humanIntel.state,'Targeted');assert.equal(g.spawnRaid(b,s,true),false);
 for(const h of g.humans)h.hp=0;grids(g);g.colonies.threatTick(s);assert.equal(s.humanIntel.state,'Recovering');assert.equal(g.colonies.canRaid(s),false);assert.ok(s.humanIntel.recoveryUntil>=g.time+150);
});
test('a blueprint costs no upfront timber, requires real worker arrival, and never creates free walls',()=>{
 const {g,s}=fixture();s.wood=0;survey(g,s);assert.equal(s.barriers.length,0);s.builders=12;g.time++;g.colonies.work(s);assert.equal(s.barriers.length,0);assert.ok(s.projects.length<=6);
 const wood=s.perimeter.totalWood;s.wood=wood;build(g,s);assert.equal(s.wood,0);assert.equal(s.barriers.length,s.perimeter.total);assert.equal(s.barriers.filter(b=>b.gate).length,4);
});
test('full enclosures block every human edge, admit apes at gates, and reopen only after a breach',()=>{
 const {g,w,s}=fixture();survey(g,s);s.wood=s.perimeter.totalWood;build(g,s);const p=s.perimeter;
 for(let side=0;side<4;side++)for(let along=-p.radius;along<=p.radius;along+=5){const x=s.x+(side===0?p.radius:side===2?-p.radius:along),y=s.y+(side===1?p.radius:side===3?-p.radius:along);assert.equal(w.actorBlocked(x,y,8,'human'),true,'complete physical perimeter')}
 const gate=s.barriers.find(b=>b.gate),wall=s.barriers.find(b=>!b.gate);assert.equal(w.actorBlocked(gate.x,gate.y,9,'ape'),false);assert.equal(w.actorBlocked(wall.x,wall.y,9,'ape'),true);assert.equal(w.actorBlocked(gate.x,gate.y,9,'human'),true);
 w.damageFortification(gate,9999,g.time);assert.equal(w.actorBlocked(gate.x,gate.y,9,'human'),false);w.repairFortification(gate,90,g.time);assert.equal(w.actorBlocked(gate.x,gate.y,9,'human'),true);
});
test('ordinary infantry damages a settlement wall instead of climbing through it',()=>{
 const {g,w}=fixture(),wall=w.addFortification({id:'wall',team:'ape',settlementId:'village',settlementWall:true,x:100,y:0,w:16,h:300,hp:320,maxHp:320}),h=g.makeHuman(70,0,null);
 for(let i=0;i<80;i++){g.time+=.05;g.navigation.beginFrame(g.time);g.navigation.move(h,100,0,70,.05,true)}
 assert.ok(h.x<92);assert.ok(wall.hp>0&&wall.hp<320);assert.equal(h._barrierCrossing,undefined);
});
test('perimeter surveys preserve buildings, bound work, and save unfinished material projects',()=>{
 const {c,g,w,s}=fixture(),positions=Array.from(s.huts,h=>[h.id,h.x,h.y]);let checks=0;w.settlementPlot=()=>{checks++;return{valid:true,trees:[],blocked:[]}};g.colonies.completePerimeter(s);assert.ok(checks<=96);
 while(s.perimeter.stage==='surveying')g.colonies.advancePerimeter(s);s.wood=0;g.colonies.work(s);const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),home=loaded.settlement(s.id);
 assert.equal(home.perimeter.total,s.perimeter.total);assert.equal(home.wood,0);assert.deepEqual(Array.from(home.huts,h=>[h.id,h.x,h.y]),positions);assert.equal(home.population,s.population);
});
test('invalid terrain never receives overlapping free walls and the survey reports its limit',()=>{
 const {g,w,s}=fixture();w.terrain=()=>({biome:'water',water:true});const wood=s.wood;g.colonies.completePerimeter(s);for(let i=0;i<20;i++)g.colonies.advancePerimeter(s);assert.equal(s.perimeter.stage,'blocked');assert.equal(s.barriers.length,0);assert.equal(s.wood,wood);
});
test('large fort conquest awards meaningful food and training once, including after save/load',()=>{
 const {c,g,s}=fixture(),b=base(g,200);b.tier=5;b.strength=0;const a=g.apes[0];a.x=180;a.y=0;const before=g.food;g.checkSite(b);assert.equal(g.food-before,600);assert.equal(a.trainingLevel,1);g.checkSite(b);assert.equal(g.food-before,600);
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),saved=loaded.world.sites.get(b.id),food=loaded.food;loaded.checkSite(saved);assert.equal(loaded.food,food);assert.equal(saved.conquestReward.food,600);
});

test('residents physically enter a completed enclosure through its gate without climbing walls',()=>{
 const {g,s}=fixture();survey(g,s);s.wood=s.perimeter.totalWood;build(g,s);const gate=s.barriers.find(b=>b.gate&&b.x>s.x),a=g.apes[0],destination={x:gate.x-110,y:gate.y};a.x=gate.x+100;a.y=gate.y;a._nav=null;
 for(let i=0;i<180;i++){g.time+=.05;g.navigation.beginFrame(g.time);g.move(a,destination.x-a.x,destination.y-a.y,90,.05);assert.equal(a.palisadeClimb,undefined)}assert.ok(a.x<gate.x-90);
});

test('finishing or repairing a wall releases overlapping workers on the same side',()=>{
 const {g,w,s}=fixture(),a=g.apes[0];a.x=99;a.y=0;grids(g);
 g.colonies.complete(s,{id:'new-wall',kind:'barrier',x:100,y:0,barrierDir:Math.PI/2,barrierWidth:70});
 const wall=s.barriers.at(-1);assert.ok(a.x<90);assert.equal(w.actorBlocked(a.x,a.y,10,'ape'),false);
 w.damageFortification(wall,9999,g.time);a.x=101;a.y=0;grids(g);
 g.colonies.complete(s,{id:'repair-wall',kind:'repairBarrier',structureId:wall.id,x:100,y:0});
 assert.ok(a.x>110);assert.equal(w.actorBlocked(a.x,a.y,10,'ape'),false);
});
