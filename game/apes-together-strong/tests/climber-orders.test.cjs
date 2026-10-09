'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function setup(){const c=loadEngine(),g=new c.ATSGame('CLIMB-ORDERS'),w=g.world;w.objects.clear();w._spatial.clear();w.sites.clear();w.ensure=()=>{};w.stream=()=>{};w.terrain=()=>({biome:'forest',water:false});w.waterBlocked=()=>false;w._streaming=false;g.king.x=-400;g.spawnSites=()=>{};
 const site={id:'fort',x:200,y:0,extentX:200,extentY:200,radius:350,military:true,fortressVersion:1,objects:[],guards:0,strength:0,stairs:[],walkways:[],known:true};w.sites.set(site.id,site);
 const wall={id:'wall',type:'wall',siteId:site.id,x:0,y:0,w:20,h:350,r:12,hp:500,maxHp:500,solid:true,collision:'rect',visualHeight:80,walkHeight:80,walkable:true,climbAccess:true};w.objects.set(wall.id,wall);w._indexObject(wall);site.climbRoutes=[[wall.id]];site.objects=[wall.id];
 return{c,g,w,wall};}
function ape(g,species,state='follow'){const a=g.makeApe(-90,0,state);a.species=species;g.siege.balance(a,true);return a}
function step(g){g.time+=1/30;g.syncIndexes();g.apeGrid.rebuild(g.apes,g.king);g.humanGrid.rebuild(g.humans);g.navigation.beginFrame(g.time);g.performance.beginStep(g);g.siege.tick(1/30);for(const a of g.apes)g.updateApe(a,1/30)}
test('climber selection immediately snapshots only eligible field climbers, regardless of the prior species',()=>{
 const {g}=setup();for(const s of ['gorilla','orangutan','chimpanzee','gibbon','capuchin','mandrill'])ape(g,s);ape(g,'gibbon','settled');const dead=ape(g,'capuchin');dead.hp=0;g.syncIndexes();g.siege.select('gorilla');assert.equal(g.siege.selectClimbers(),3);assert.deepEqual(Array.from(g.siege.selected).sort(),['capuchin','chimpanzee','gibbon']);assert.equal(g.siege.unitIds.length,3);
});
test('ordinary move orders inside a fort cross a real climbable wall for all three light species',()=>{
 for(const species of ['gibbon','capuchin','chimpanzee']){const {g}=setup(),a=ape(g,species);g.syncIndexes();g.siege.selectClimbers();assert.ok(g.siege.issue([species],[{id:'inside',type:'move',x:200,y:0}]));let climbed=false;
  for(let i=0;i<600;i++){step(g);climbed||=!!a.siegeTransition}assert.ok(climbed,species+' uses a physical climb');assert.ok(a.x>140,species+' reaches inside');assert.ok(!a.siegeTransition);
 }
});
test('an attack order at an interior objective automatically crosses the wall before fighting',()=>{
 const {g,w}=setup(),a=ape(g,'gibbon'),target={id:'target',type:'radio',siteId:'fort',x:160,y:0,r:15,hp:50,maxHp:50,solid:true};w.objects.set(target.id,target);w._indexObject(target);g.syncIndexes();g.siege.selectClimbers();g.siege.issue(['gibbon'],[{id:'attack',type:'attack',targetId:target.id,targetKind:'object',x:160,y:0}]);let climbed=false;
 for(let i=0;i<700&&target.hp>0;i++){step(g);climbed||=!!a.siegeTransition}assert.ok(climbed);assert.equal(target.hp,0);assert.ok(a.x>0);
});
test('empty ground cannot create a malformed climb, and a stale saved wall step cannot crash',()=>{
 const {g,wall}=setup(),a=ape(g,'gibbon');g.syncIndexes();g.siege.selectClimbers();assert.equal(g.siege.draft({x:-500,y:-500},'climb'),false);assert.match(g.siege.status,/visible wall/);
 assert.ok(g.siege.issue(['gibbon'],[{id:'old',type:'climb',targetId:wall.id,targetKind:'object',x:0,y:0}]));g.world.objects.delete(wall.id);assert.doesNotThrow(()=>{for(let i=0;i<10;i++)step(g)});assert.ok(Number.isFinite(a.x));
});

test('clicking a climb access selects all field climbers and never drafts a heavy ape climb',()=>{
 const {g}=setup();for(const species of ['gorilla','chimpanzee','gibbon','capuchin'])ape(g,species);g.syncIndexes();g.siege.select('gorilla');
 assert.ok(g.siege.draft({x:0,y:0},'climb'));assert.equal(g.siege.selectedMembers().length,3);assert.equal(g.siege.drafts.gorilla?.length||0,0);
 for(const species of ['chimpanzee','gibbon','capuchin'])assert.equal(g.siege.drafts[species][0].type,'climb');assert.ok(g.siege.dispatch());
});

test('large formations keep their final movement destination inside the fort they entered',()=>{
 const {g}=setup();for(let i=0;i<100;i++)ape(g,'gibbon');g.syncIndexes();const order={id:'inside',type:'move',x:200,y:0};g.siege.issue(['gibbon'],[order]);const a=g.apes.at(-1);a.x=80;a.y=0;
 g.siege.followRoute(a,order,.1);assert.ok(a.target.x>=28&&a.target.x<=372);assert.ok(Math.abs(a.target.y)<=172);
});
