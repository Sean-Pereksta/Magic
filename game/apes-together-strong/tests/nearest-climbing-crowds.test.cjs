'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),{loadEngine}=require('./performance-harness.cjs');

function arena(){
 const c=loadEngine(),g=new c.ATSGame('NEAREST-CLIMBING-CROWDS'),w=g.world;
 w.objects.clear();w._spatial.clear();w.sites.clear();w.ensure=()=>{};w.stream=()=>{};
 w.terrain=()=>({biome:'farmland',road:true,water:false});w.waterBlocked=()=>false;w._streaming=false;
 g.king.x=-400;return {g,w};
}
function wall(w,x,extra={}){
 const b={id:'climbing-wall-'+x,type:'wall',x,y:0,w:18,h:1400,collision:'rect',faction:'human',wallTier:1,climbable:true,visualHeight:42,hp:1000,maxHp:1000,solid:true,...extra};
 w.objects.set(b.id,b);w._indexObject(b);w.navigationChanged(b);return b;
}
function ape(g,species,y=0){const a=g.makeApe(0,y,'follow');a.species=species;g.siege.balance(a,true);return a}
function step(g,observe=()=>{}){
 g.time+=1/30;g.syncIndexes();g.apeGrid.rebuild(g.apes,g.king);g.humanGrid.rebuild(g.humans);g.vehicleGrid.rebuild(g.vehicles);
 g.navigation.beginFrame(g.time,{budgetMs:Infinity});g.performance.beginStep(g);g.siege.tick(1/30);
 for(const a of g.apes){g.updateApe(a,1/30);observe(a)}
}

test('nearest-human climbers cross two and three consecutive walls without reusing the previous crossing',()=>{
 for(const count of [2,3])for(const species of ['gibbon','chimpanzee','capuchin']){
  const {g,w}=arena(),a=ape(g,species),walls=Array.from({length:count},(_,i)=>wall(w,50+i*60)),human=g.makeHuman(260,0,null);
  human.hp=human.maxHp=100000;step(g);g.command('nearestHuman',{x:1,y:0});const crossed=new Set();
  for(let frame=0;frame<380&&human.hp===human.maxHp;frame++)step(g,p=>{const id=p.wallClimb?.wallId||p.siegeTransition?.wallId;if(id)crossed.add(id);if(!id)assert.equal(g.navigation.blocked(p.x,p.y,10,'ape'),false)});
  assert.equal(crossed.size,count,species+' physically crosses every wall');assert.ok(human.hp<human.maxHp,species+' reaches infantry');
  assert.ok(a.x>walls.at(-1).x);assert.equal(a.wallClimb,undefined);assert.equal(a.siegeTransition,undefined);
  for(const b of walls)assert.equal(b.hp,b.maxHp,'the held human order leaves every wall intact');
 }
});

test('twelve selected climbers safely queue at the three-ape fortress climb limit',()=>{
 const {g,w}=arena(),b=wall(w,50,{wallTier:3,climbAccess:true,walkable:true,walkHeight:80,visualHeight:80}),human=g.makeHuman(120,0,null);
 human.hp=human.maxHp=100000;const apes=Array.from({length:12},(_,i)=>ape(g,'gibbon',(i-5.5)*6));
 step(g);g.siege.selectUnits(apes.map(a=>a.id));g.command('nearestHuman',{x:1,y:0});const climbers=new Set(),landed=new Set();let highest=0;
 for(let frame=0;frame<385;frame++){
  step(g,a=>{if(a.siegeTransition)climbers.add(a.id);else{assert.equal(g.navigation.blocked(a.x,a.y,10,'ape'),false,'a waiting ape stays outside the wall');if(a.x>b.x+20)landed.add(a.id)}});
  highest=Math.max(highest,apes.filter(a=>a.siegeTransition).length);assert.ok(highest<=3,'wall occupancy stays bounded');
 }
 assert.equal(highest,3);assert.equal(climbers.size,12);assert.equal(landed.size,12,'every selected climber lands before its order expires');
 assert.ok(human.hp<human.maxHp);assert.equal(b.hp,b.maxHp);assert.ok(apes.every(a=>!a.siegeTransition&&!a.wallClimb));
});
