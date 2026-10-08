'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');

test('a continued 300-ape frontal charge can defeat prepared combined arms with serious ordinary-HP losses',()=>{
 const g=new (loadEngine().ATSGame)('FRONTAL-300-A','survival');
 // An open, isolated battlefield keeps procedural cover and reinforcements out
 // of this engagement. Damage, accuracy, reloads, targeting and movement are real.
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();
 g.world.ensure=()=>{};g.world.stream=()=>{};g.world.trimDistant=()=>{};
 g.world.getSites=()=>[];g.world.getObjects=()=>[];
 g.world.terrain=()=>({biome:'farmland',road:true,water:false});
 g.world.lineClear=()=>true;g.world.blocked=()=>false;g.world.vehicleBlocked=()=>false;
 g.spawnSites=()=>{};g.responseDirector=()=>{};g.launchHelicopter=()=>false;g.heliTimer=100000;
 g.king.x=-150;g.king.y=0;g.food=1000;
 for(let i=0;i<300;i++)g.makeApe(-350+i%20*26,(Math.floor(i/20)-7)*26,'follow');
 assert.ok(g.apes.every(a=>a.hp===a.maxHp&&a.maxHp>=90&&a.maxHp<=260));g.updateResponseStage();
 const site={id:'prepared-defense',tier:5,x:650,y:0,objects:[],strength:0,nextOperation:100000,spawned:true,radioDown:false};
 g.world.sites.set(site.id,site);const soldiers=[],vehicles=[];
 for(let i=0;i<28;i++){
  const role=i===0||i===14?'leader':i===6||i===20?'heavy':i===9||i===19?'grenadier':i===12||i===26?'medic':'rifleman';
  const rear=role==='heavy'||role==='grenadier'?60:role==='medic'?110:0;
  const h=g.makeHuman(420+rear,(i<14?-95:95)+(i%14-6.5)*19,site);
  g.forces.assign(h,site,role);Object.assign(h,{dir:Math.PI,state:'search',reported:true,suspicion:1,lastSeenAt:-100,shootTimer:0,specialAt:0,searchTime:10000});soldiers.push(h);
 }
 for(let i=0;i<3;i++){
  const v={id:'vehicle-'+g.nextId++,x:i<2?680:590,y:i===0?-105:i===1?105:0,dir:Math.PI,turretDir:Math.PI,state:'combat',shootTimer:0,cannonTimer:0,siteId:site.id,operationId:'defense',platoonId:'defense'};
  g.forces.initVehicle(v,i<2?'tank':'apc',0);g.vehicles.push(v);vehicles.push(v);
 }
 assert.equal(vehicles[0].maxHp,1100);assert.equal(vehicles[2].maxHp,550);
 g.forces.createSquad(soldiers.slice(0,14),{x:420,y:-95},{order:'Hold',vehicleId:vehicles[0].id,siteId:site.id});
 g.forces.createSquad(soldiers.slice(14),{x:420,y:95},{order:'Hold',vehicleId:vehicles[1].id,siteId:site.id});
 g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);g.vehicleGrid.rebuild(g.vehicles);
 const stillFighting=()=>soldiers.some(h=>h.hp>0)||vehicles.some(v=>v.hp>0);
 let nextCharge=7,lastChargeAt=0,maxWarnings=0,maxThinks=0,maxLos=0,reverseFrames=0;
 g.command('charge',{x:1,y:0});
 for(let frame=0;frame<1200&&!g.ended&&g.population>0&&stillFighting();frame++){
  // A player follows safely behind the frontal army and renews the ordinary
  // charge when its fixed endpoint leaves a substantial group out of contact.
  const stranded=g.apes.filter(a=>a.hp>0&&a.state==='hold'&&!g.humanGrid.near(a.x,a.y,90).length&&!g.vehicleGrid.near(a.x,a.y,80).length).length;
  if(g.time>=nextCharge||g.time-lastChargeAt>=1&&stranded>Math.max(20,g.population*.3)){
   const target=[...soldiers,...vehicles].filter(h=>h.hp>0).sort((a,b)=>Math.hypot(a.x-g.king.x,a.y-g.king.y)-Math.hypot(b.x-g.king.x,b.y-g.king.y))[0];
   g.command('charge',{x:target.x-g.king.x,y:target.y-g.king.y});lastChargeAt=g.time;nextCharge=g.time+7;
  }
  const xs=g.apes.filter(a=>a.hp>0).map(a=>a.x).sort((a,b)=>a-b),safeX=xs[Math.floor(xs.length*.3)]-250;
  const input={x:g.king.x<safeX-25?1:g.king.x>safeX+80?-1:0};
  const warning=g.vehicles.find(v=>v.cannonTarget&&Math.hypot(v.cannonTarget.x-g.king.x,v.cannonTarget.y-g.king.y)<125);
  if(warning)input.y=g.king.y>=warning.cannonTarget.y?1:-1;
  g.update(1/60,input);
  maxWarnings=Math.max(maxWarnings,g.vehicles.filter(v=>v.cannonTarget).length);
  reverseFrames+=g.vehicles.some(v=>v.reversing)?1:0;
  maxThinks=Math.max(maxThinks,g.performance.counters.aiThinks);maxLos=Math.max(maxLos,g.performance.counters.losTests);
 }
 assert.equal(stillFighting(),false,'the ordinary-HP army must finish the engagement');
 assert.ok(g.king.hp>0);assert.ok(g.stats.lost>=40&&g.stats.lost<=150,`expected costly victory, got ${g.stats.lost} casualties`);
 assert.ok(g.population>=150);assert.ok(maxWarnings>=1,'tank shells must retain their warnings');
 assert.ok(reverseFrames>0,'the tanks must attempt to reverse away from approaching apes');
 assert.ok(maxThinks<=32&&maxLos<=96,'combat must respect shared simulation budgets');
});
