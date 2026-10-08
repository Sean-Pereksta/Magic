'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function home(n=1){
 const c=loadEngine(),g=new c.ATSGame('OPEN-BUILDING');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};
 g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;g.world.getSites=()=>[];
 const s={id:'home',name:'Home',x:0,y:0,population:n,radius:100,level:1,food:100000,wood:100000,livingFounding:true,birthTimer:-100000};
 g.settlements.push(s);for(let i=0;i<n;i++)g.makeApe(0,i*10,'settled',s.id);g.refreshSettlements();g.colonies.init(s);g.king.x=g.king.y=0;g.colonies.plan=()=>{};
 return{c,g,s};
}
function build(g,s,steps=120){
 g.colonies.assignJobs(s,g.colonies.members(s));s.simLOD=0;
 for(let i=0;i<steps&&s.projects.length;i++){
  g.time++;g.colonies.work(s);
  for(const a of g.colonies.members(s)){const p=g.colonies.activityTarget(a,s);Object.assign(a,{x:p.x,y:p.y})}
 }
}
test('one-resident settlements can commission all affordable buildings without rank or queue gates',()=>{
 const {g,s}=home();g.progression.tier=0;
 for(const kind of ['spearTower','training','garden','store','workshop','longhouse','canopyHut','nursery','rallyGrove','orchard','spearBattery','spearBallista'])assert.equal(g.colonies.commission(s.id,kind).ok,true,kind);
 for(let i=0;i<30;i++)assert.equal(g.colonies.commission(s.id,'spearTower').ok,true);
 assert.equal(s.commissions.length,42);assert.equal(g.colonies.catalog(s).find(o=>o.kind==='spearTower').available,true);
 assert.equal(g.colonies.commission(s.id,'royalExpansion').ok,true);const wood=s.wood;assert.equal(g.colonies.commission(s.id,'royalExpansion').ok,false);assert.equal(s.wood,wood,'unique lodge upgrade cannot be charged twice');
});
test('paid plots fill concentric rings and keep expanding beyond old settlement and housing limits',()=>{
 const {g,s}=home(),wood=s.wood;
 for(let i=0;i<150;i++)assert.equal(g.colonies.commission(s.id,'hut').ok,true);
 assert.equal(s.wood,wood-150*8);assert.equal(s.projects.length,150);
 for(let i=0;i<600&&s.projects.some(p=>p.awaitingPlot);i++)g.colonies.locateCommissions(s);
 assert.equal(s.projects.filter(p=>p.awaitingPlot).length,0);assert.equal(s.huts.length,150);
 assert.equal(new Set(s.huts.map(h=>h.x+','+h.y)).size,150);
 assert.ok(s.developedRadius>650,'construction is not capped by population or expansion tier');
 for(const p of s.projects)assert.ok(Math.abs(Math.hypot(p.x-s.x,p.y-s.y)-(120+p.layoutRing*150))<.001);
 for(let i=0;i<s.huts.length;i++)for(let j=0;j<i;j++)assert.ok(Math.hypot(s.huts[i].x-s.huts[j].x,s.huts[i].y-s.huts[j].y)>=104);
 assert.ok(s.projects.filter(p=>p.layoutRing===0).length>=3,'first homes occupy multiple directions around the main hut');
 assert.equal(s.wood,wood-150*8,'finding land never charges an accepted order again');
});
test('trees and rocks become real clearing work, then a lone builder completes the paid structure',()=>{
 const {g,s}=home(),plot=g.colonies.surveyPlot(s,'hut');
 const tree={id:'plot-tree',type:'tree',x:plot.x-8,y:plot.y,r:14,moveRadius:14,hp:80,maxHp:80,solid:true,size:1},rock={id:'plot-rock',type:'rock',x:plot.x+15,y:plot.y,r:18,hp:100,maxHp:100,solid:true};
 for(const o of [tree,rock]){g.world.objects.set(o.id,o);g.world._indexObject(o)}
 const wood=s.wood,result=g.colonies.commission(s.id,'hut'),p=s.projects.find(p=>p.id===result.projectId);assert.equal(result.ok,true);assert.equal(p.treeIds.length,2);assert.equal(s.wood,wood-8);
 build(g,s);assert.equal(s.projects.length,0);assert.equal(s.huts[0].stage,4);assert.equal(g.apes[0].job,'builder');
 for(const o of [tree,rock]){assert.equal(o.dead,true);assert.equal(o.solid,false);assert.equal(o.clearedBy,s.id)}
 const earned=s.wood;assert.equal(g.world.clearSettlementObstacle(tree,{settlementId:s.id}),0);assert.equal(g.world.clearSettlementObstacle(rock,{settlementId:s.id}),0);assert.equal(s.wood,earned);
 assert.equal(g.colonies.catalog(s).find(o=>o.kind==='hut').available,true);
});
test('unloaded plots are accepted once, survive reload and resume without phantom huts or duplicate payment',()=>{
 const {c,g,s}=home(),wood=s.wood;g.world.settlementPlot=()=>({valid:false,pending:true,trees:[],blocked:[]});
 const result=g.colonies.commission(s.id,'hut');assert.equal(result.ok,true);assert.equal(s.huts.length,0);assert.equal(s.projects[0].awaitingPlot,true);assert.equal(s.wood,wood-8);
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),saved=loaded.settlement(s.id);loaded.world.settlementPlot=()=>({valid:true,trees:[],rocks:[],blocked:[]});
 loaded.colonies.locateCommissions(saved);assert.equal(saved.projects[0].awaitingPlot,undefined);assert.equal(saved.huts.length,1);assert.equal(saved.wood,wood-8);loaded.colonies.locateCommissions(saved);assert.equal(saved.huts.length,1);assert.equal(saved.commissions.length,1);
});
test('water and occupied terrain send the survey farther outward instead of refusing an affordable order',()=>{
 const {g,s}=home();let checks=0;g.world.settlementPlot=(x,y)=>{checks++;return{valid:Math.hypot(x,y)>2000,water:Math.hypot(x,y)<=2000,trees:[],blocked:[]}};
 const wood=s.wood,result=g.colonies.commission(s.id,'training');assert.equal(result.ok,true);assert.ok(checks<=96);assert.equal(s.projects[0].awaitingPlot,true);
 for(let i=0;i<30&&s.projects[0].awaitingPlot;i++){checks=0;g.colonies.locateCommissions(s);assert.ok(checks<=96)}
 assert.equal(s.projects[0].awaitingPlot,undefined);assert.ok(Math.hypot(s.projects[0].x,s.projects[0].y)>2000);assert.equal(s.wood,wood-24);
});
test('built homes and human structures are preserved while the next free ring position is selected',()=>{
 const {g,s}=home(),first=g.colonies.surveyPlot(s,'store'),wall={id:'existing-wall',type:'wall',x:first.x,y:first.y,w:80,h:80,r:40,hp:200,solid:true,collision:'rect'};
 g.world.objects.set(wall.id,wall);g.world._indexObject(wall);const result=g.colonies.commission(s.id,'store'),p=s.projects.find(p=>p.id===result.projectId);assert.equal(result.ok,true);assert.ok(Math.hypot(p.x-wall.x,p.y-wall.y)>60);assert.equal(wall.hp,200);assert.equal(wall.solid,true);
});
test('more defense rings can be commissioned without moving an existing palisade',()=>{
 const {g,s}=home(),first=g.colonies.commission(s.id,'defense'),p=s.projects.find(p=>p.id===first.projectId);g.colonies.complete(s,p);s.projects=[];const position=[s.barriers[0].x,s.barriers[0].y];
 for(let i=0;i<50;i++)assert.equal(g.colonies.commission(s.id,'defense').ok,true);
 for(let i=0;i<100&&s.projects.some(p=>p.awaitingPlot);i++)g.colonies.locateCommissions(s);
 assert.ok(new Set(s.projects.map(p=>p.ringId)).size>1);assert.deepEqual([s.barriers[0].x,s.barriers[0].y],position);assert.equal(s.projects.filter(p=>p.awaitingPlot).length,0);
});
test('scarce resources reject without a charge, but an attack cannot prevent queueing',()=>{
 const {g,s}=home();s.wood=0;const food=s.food;assert.equal(g.colonies.commission(s.id,'hut').ok,false);assert.equal(s.food,food);assert.equal(s.projects.length,0);
 s.wood=26;s.food=11;assert.equal(g.colonies.commission(s.id,'spearTower').ok,false);assert.equal(s.wood,26);s.food=12;s.attack=true;assert.equal(g.colonies.commission(s.id,'spearTower').ok,true);assert.equal(s.wood,0);assert.equal(s.food,0);
});

test('a real walking builder clears a large rock and completes an outer-ring home without turning back',()=>{
 const {g,s}=home();g.world.settlementPlot=(x,y)=>({valid:Math.hypot(x,y)>1200,water:Math.hypot(x,y)<=1200,trees:[],rocks:[],blocked:[]});
 const result=g.colonies.commission(s.id,'hut');for(let i=0;i<30&&s.projects[0].awaitingPlot;i++)g.colonies.locateCommissions(s);
 const p=s.projects.find(p=>p.id===result.projectId),rock={id:'large-plot-rock',type:'rock',x:p.x,y:p.y,r:70,hp:100,maxHp:100,solid:true};
 g.world.objects.set(rock.id,rock);g.world._indexObject(rock);p.treeIds=[rock.id];g.colonies.assignJobs(s,g.colonies.members(s));s.simLOD=0;
 const a=g.apes[0];let farthest=0;
 for(let i=0;i<2400&&s.projects.length;i++){
  g.time+=.1;g.navigation.beginFrame(g.time);g.updateApe(a,.1);farthest=Math.max(farthest,Math.hypot(a.x,a.y));
  if(i%10===0)g.colonies.work(s);
 }
 assert.ok(farthest>1100,'builder physically reaches the distant work site');assert.equal(rock.dead,true);assert.equal(s.huts[0].stage,4);assert.equal(s.projects.length,0);
});

test('lodge upgrades completed out of order never reduce lodge strength',()=>{
 const {g,s}=home();const royal=g.colonies.commission(s.id,'royalExpansion'),war=g.colonies.commission(s.id,'warlordExpansion');
 g.colonies.complete(s,s.projects.find(p=>p.id===war.projectId));g.colonies.complete(s,s.projects.find(p=>p.id===royal.projectId));
 assert.equal(s.expansionLevel,2);assert.equal(s.lodge.maxHp,1000);assert.equal(g.colonies.catalog(s).find(o=>o.kind==='royalExpansion').available,false);
});
