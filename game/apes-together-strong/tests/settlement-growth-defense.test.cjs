'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');

function colony(n=4){
 const c=loadEngine(),g=new c.ATSGame('SETTLEMENT-GROWTH-DEFENSE');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();
 g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];
 g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;
 const s={id:'home',name:'Home',x:0,y:0,population:n,level:1,food:2000,wood:200,livingFounding:true};
 g.settlements.push(s);for(let i=0;i<n;i++)g.makeApe(-50,i*12,'settled',s.id);
 g.refreshSettlements();g.colonies.init(s);s.suitability={fertility:1,capacity:1,water:false};
 g.colonies.plan=()=>{};g.colonies.work=()=>{};
 return {c,g,s};
}
function step(g,s,n=1){for(let i=0;i<n;i++){g.time++;g.refreshSettlements();g.colonies.tick(s)}}
function hut(g,s,capacity=10){const h={id:'hut-'+s.huts.length,kind:'hut',x:-200-s.huts.length*100,y:200,stage:4,hp:100,maxHp:100,capacity};s.huts.push(h);g.colonies.updateHousing(s);return h}
function tower(g,s,kind='spearTower'){
 const t={id:'tower',kind,x:100,y:0,stage:4,hp:520,maxHp:520,station:{x:58,y:24},shotAt:0};
 s.facilities.push(t);g.colonies.assignJobs(s,g.colonies.members(s));g.colonies.staffFacilities(s);
 Object.assign(g.apesById.get(t.staffedBy),t.station);g.world.syncSettlementBuildings(s,g);
 return t;
}
function fire(g,s,t,target){
 g.humanGrid.rebuild(g.humans);g.vehicleGrid.rebuild(g.vehicles);t.shotAt=t.acquireAt=0;
 g.colonies.updateDefenses(1/60);assert.ok(s.spears.length,'staffed tower launches a visible shaft');
 const spear=s.spears[0],hp=target.hp;assert.equal(spear.elapsed,0);assert.equal(target.hp,hp);
 t.shotAt=1e9;return {spear,hp};
}
function fly(g,seconds){for(let i=0;i<Math.ceil(seconds*60);i++){g.time+=1/60;g.colonies.updateDefenses(1/60)}}

test('all spear defenses hit at their full range beyond infantry and tank guns',()=>{
 for(const [kind,range] of [['spearTower',900],['spearBattery',1000],['spearBallista',1150]]){
  const {g,s}=colony(),t=tower(g,s,kind),h=g.makeHuman(t.x+range-5,0,null);
  g.forces.assign(h,null,'sniper');h.role='sniper';assert.ok(t.range>g.forces.range(h));
  const {spear,hp}=fire(g,s,t,h);assert.ok(spear.duration>1.4);assert.ok(spear.life>spear.duration);
  fly(g,1.45);assert.equal(h.hp,hp,'long shot is still traveling after the former lifetime cap');
  fly(g,spear.duration);assert.ok(h.hp<hp,'physical collision reaches the far target');
  h.hp=0;const v={id:'armor',kind:'tank',x:t.x+range-10,y:0,state:'raid',dir:0};
  g.forces.initVehicle(v,'tank');g.vehicles.push(v);const armor=fire(g,s,t,v);fly(g,armor.spear.duration+.3);assert.ok(v.hp<armor.hp);
 }
});
test('a tower fires above its own palisade while a tall obstacle still blocks it',()=>{
 const {g,s}=colony(),t=tower(g,s),wall=g.world.addFortification({id:'palisade',type:'apeBarricade',team:'ape',owner:'ape',x:200,y:0,width:120,height:16,dir:Math.PI/2,hp:320,maxHp:320});
 const h=g.makeHuman(980,0,null);const shot=fire(g,s,t,h);fly(g,shot.spear.duration+.2);assert.ok(h.hp<shot.hp);
 h.hp=h.maxHp;wall.visualHeight=220;t.shotAt=t.acquireAt=0;g.colonies.updateDefenses(1/60);assert.equal(s.spears.length,0);
});
test('cover raised after launch intercepts a traveling spear and friendly armor is ignored',()=>{
 const {g,s}=colony(),t=tower(g,s),h=g.makeHuman(800,0,null),friendly={id:'friendly',kind:'tank',vehicleClass:'tank',team:'ape',hp:500,maxHp:500,x:300,y:0};g.vehicles.push(friendly);
 const shot=fire(g,s,t,h);assert.equal(shot.spear.targetId,h.id);
 g.world.addFortification({id:'new-wall',type:'wall',team:'human',x:500,y:0,width:120,height:180,dir:Math.PI/2,hp:320,maxHp:320});
 fly(g,shot.spear.duration+.3);assert.equal(h.hp,shot.hp);assert.equal(friendly.hp,500);assert.equal(s.spears.length,0);
});
test('settlements fill actual housing beyond low terrain capacity and resume when a home is added',()=>{
 const {g,s}=colony(4);hut(g,s);s.structures.push({id:'garden1',kind:'garden',hp:100,stage:4},{id:'garden2',kind:'garden',hp:100,stage:4});step(g,s,1200);assert.equal(s.population,16);assert.equal(g.stats.born,12);
 const count=g.population;step(g,s,60);assert.equal(g.population,count);assert.equal(s.growthStatus,'Homes full');
 hut(g,s);step(g,s,80);assert.ok(s.population>16);assert.ok(s.population<=s.housing);
});
test('food shortages pause births while recovering safety alone does not',()=>{
 const {g,s}=colony(1);s.food=0;s.safety=0;s.birthTimer=30;
 const h=g.makeHuman(90,0,null);h.state='combat';g.humanGrid.rebuild(g.humans);
 step(g,s);assert.equal(s.attack,true);assert.equal(g.stats.born,0);assert.equal(s.population,1);assert.equal(s.food,0);
 h.hp=0;g.humanGrid.rebuild([]);const before=s.birthTimer;step(g,s);assert.equal(s.birthTimer,before);s.food=100;step(g,s);assert.equal(g.stats.born,1);
});
test('completed structures and larger adult communities accelerate births; ruins and frames do not',()=>{
 const {g,s}=colony(4);hut(g,s);const base=g.colonies.familyGrowth(s).rate;
 s.structures.push({id:'store',kind:'storage',hp:100,stage:4});const developed=g.colonies.familyGrowth(s).rate;assert.ok(developed>base);
 s.structures.push({id:'frame',kind:'cooking',hp:100,stage:2},{id:'ruin',kind:'workShelter',hp:0,stage:4});assert.equal(g.colonies.familyGrowth(s).rate,developed);
 hut(g,s);for(let i=0;i<8;i++)g.makeApe(0,80+i*12,'settled',s.id);g.refreshSettlements();assert.ok(g.colonies.familyGrowth(s).rate>developed);
});
test('newborns count immediately, returning travelers retain beds, and destroyed huts remove capacity',()=>{
 const {g,s}=colony(4),h=hut(g,s);g.apes[0].kingdomMission={id:'delivery',kind:'supply'};s.birthTimer=10000;step(g,s,12);
 assert.equal(s.population,16);assert.equal(g.population,16);assert.equal(g.settlementMembers.get(s.id).length,16);
 g.colonies.damageHut(s,h,100);step(g,s,5);assert.equal(s.housing,6);assert.equal(g.population,16);
 g.colonies.complete(s,{kind:'rebuildHut',structureId:h.id});g.apes.at(-1).state='follow';g.apes.at(-1).settlementId=null;g.refreshSettlements();s.birthTimer=30;step(g,s);assert.equal(s.population,16);assert.equal(g.population,17);
});
test('empty villages stay empty and several settlements cannot overfill the kingdom cap',()=>{
 const {c,g,s}=colony(0);hut(g,s);s.birthTimer=30;step(g,s);assert.equal(g.population,0);
 g.makeApe(0,0,'settled',s.id);for(let i=1;i<c.MAX_APE_POPULATION-1;i++)g.makeApe(2000+i,0,'follow');g.refreshSettlements();s.birthTimer=1000;step(g,s);assert.equal(g.population,c.MAX_APE_POPULATION);
 const before=g.stats.born;step(g,s,5);assert.equal(g.stats.born,before);assert.ok(s.birthTimer<=30);
});
test('building plots provide irregular, deterministic walking gaps without moving saved structures',()=>{
 const {c,g,s}=colony(100);s.developedRadius=s.radius=650;g.colonies.layout(s);g.world.settlementPlot=()=>({valid:true,trees:[],blocked:[]});
 const first=g.colonies.plot(s,'hut',0);assert.deepEqual(g.colonies.plot(s,'hut',0),first);
 for(let i=0;i<24;i++){const p=g.colonies.queue(s,'hut');assert.ok(p,'room for spread-out homes');g.colonies.complete(s,p);s.projects=s.projects.filter(q=>q!==p)}
 for(let i=0;i<s.huts.length;i++)for(let j=0;j<i;j++)assert.ok(Math.hypot(s.huts[i].x-s.huts[j].x,s.huts[i].y-s.huts[j].y)>=104);
 const angles=new Set(s.huts.map(h=>Math.round((Math.atan2(h.y,h.x)+Math.PI)*180/Math.PI)%30));assert.ok(angles.size>8,'buildings do not repeat twelve rigid spokes');
 const positions=Array.from(s.huts,h=>[h.id,h.x,h.y]),loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize())));assert.deepEqual(Array.from(loaded.settlement(s.id).huts,h=>[h.id,h.x,h.y]),positions);
});
