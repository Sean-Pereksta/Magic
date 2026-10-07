'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function arena(){
 const c=loadEngine(),g=new c.ATSGame('HUMAN-FIRE-PERCEPTION','survival');g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.getSites=()=>[];g.world.getObjects=()=>[];g.world.terrain=()=>({biome:'farmland',road:true,water:false});g.world.lineClear=()=>true;g.spawnSites=()=>{};
 g.tier=1;g.king.x=24;g.king.y=0;g.king.hp=g.king.maxHp=10000;
 const h=g.makeHuman(0,0,null);g.forces.assign(h,null,'guard');Object.assign(h,{state:'combat',targetId:'king',dir:0,reported:true,suspicion:1,shootTimer:0,perceptionTimer:.05,lastSeenAt:0});
 return {g,h};
}
test('an early guard retains real visual contact while taking only brief close-range backsteps',()=>{
 const {g,h}=arena(),start={x:h.x,y:h.y};let moving=0,shots=0,stationaryShots=0;
 for(let i=0;i<120;i++){
  g.time+=.05;g.king.x=h.x+24;g.king.y=h.y;g.performance.beginStep();g.visibilityCache.clear();g.navigation.beginFrame(g.time);g.apeGrid.rebuild([g.king]);g.humanGrid.rebuild([h]);g.bullets=[];
  g.updateHuman(h,.05);if(h.moving)moving++;if(g.bullets.length){shots++;if(!h.moving)stationaryShots++;}
  assert.equal(h.state,'combat','movement must not turn the next perception cone away from its live target');assert.equal(h.targetId,'king');assert.ok(Math.cos(h.dir)>.99,'the guard keeps facing the close ape');
 }
 assert.ok(shots>=4,`${shots} firing ticks in six seconds`);assert.ok(stationaryShots>=shots*.65,'most shots come from planted feet');assert.ok(moving<=30,`${moving}/120 moving ticks`);assert.ok(Math.hypot(h.x-start.x,h.y-start.y)<=50,'pressure cannot restart an unbounded retreat every frame');assert.equal(h.squadId,undefined,'the unsquadded early-game path is exercised');
});
test('an unsquadded guard fires at an in-range target instead of chasing an arbitrary preferred distance',()=>{
 const {g,h}=arena();g.king.x=220;const start={x:h.x,y:h.y};let shots=0;
 for(let i=0;i<80;i++){g.time+=.05;g.performance.beginStep();g.visibilityCache.clear();g.navigation.beginFrame(g.time);g.apeGrid.rebuild([g.king]);g.humanGrid.rebuild([h]);g.bullets=[];g.updateHuman(h,.05);if(g.bullets.length)shots++;}
 assert.ok(shots>=3);assert.equal(h.x,start.x);assert.equal(h.y,start.y);assert.equal(h.state,'combat');
});
