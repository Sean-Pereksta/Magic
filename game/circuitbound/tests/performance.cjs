/* Optional deterministic load benchmark; run from the repository root with:
   node game/circuitbound/tests/performance.cjs
   Times describe this host, not a browser frame-rate guarantee. */
'use strict';
const assert=require('node:assert/strict');
const {performance}=require('node:perf_hooks');
const {fixture}=require('./harness.cjs');
const S=require('../survival.js');
const f=fixture(84,54),{T,sim,put,key}=f,tile=24,ticks=1200,dt=.05;
for(let y=50;y<f.sim.height;y++)for(let x=0;x<f.sim.width;x++)put(x,y,T.STONE);
for(let row=0;row<10;row++)for(let column=0;column<10;column++){
  const x=2+column*8,y=2+row*4;
  put(x,y,T.GENERATOR);put(x+1,y,T.MOTOR);put(x+2,y,T.SHAFT);
  put(x+3,y,T.CONVEYOR,{contents:{iron:4}});put(x+4,y,T.STORAGE);
}
sim.step(0);assert.equal(sim.active.length,500);
const initialRebuilds=sim.rebuilds,started=performance.now();
for(let i=0;i<ticks;i++){sim.step(dt);sim.production(dt);sim.logistics(dt);}
const engineeringMs=performance.now()-started;
assert.equal(sim.rebuilds,initialRebuilds,'unchanged topology must never rebuild');
assert.equal(sim.drops.length,0);assert.equal([...sim.transport].reduce((n,k)=>n+sim.stored(k),0),400,'transport must conserve the fixture cargo');
for(let row=0;row<10;row++)for(let column=0;column<10;column++)assert.equal(sim.data(key(3+column*8,2+row*4)).running,true);

let seed=42;const random=()=>((seed=(seed*1664525+1013904223)>>>0)/4294967296);
const player={x:42*tile,y:50*tile-34,w:15,h:34,health:100,vx:0,vy:0},inventory={},stats={};
const survival=new S.Survival({width:sim.width,height:sim.height,tile,T,defs:f.defs,get:f.get,set:put,meta:()=>f.meta,engine:sim,player,inventory,stats,random});
survival.setDifficulty('hardcore');survival.invulnerable=1e9;survival.spawnClock=1e9;survival.nextRaid=1e9;
for(let i=0;i<46;i++)assert.ok(survival.spawn('wisp',(3+(i%20)*4)*tile,(44+Math.floor(i/20)*2)*tile,{ignoreDistance:true}));
const aiStarted=performance.now();for(let i=0;i<ticks;i++)survival.tick(dt);const enemyAiMs=performance.now()-aiStarted;

// The production spatial index, stressed above the game's capped 22/34/46 populations.
const enemies=Array.from({length:512},(_,i)=>({id:i,x:random()*220*tile,y:random()*84*tile,w:20,h:18,hp:30}));
const grid=new S.SpatialGrid(tile*8);grid.rebuild(enemies);
const queries=Array.from({length:1000},()=>({x:random()*220*tile,y:random()*84*tile,r:tile*9}));
for(const q of queries.slice(0,100)){
  const expected=enemies.filter(e=>Math.hypot(e.x-q.x,e.y-q.y)<=q.r+Math.max(e.w,e.h)/2).map(e=>e.id).sort((a,b)=>a-b);
  assert.deepEqual(grid.query(q.x,q.y,q.r).map(e=>e.id).sort((a,b)=>a-b),expected);
}
const queryStarted=performance.now();let hits=0;
for(let i=0;i<50000;i++){const q=queries[i%queries.length];hits+=grid.query(q.x,q.y,q.r).length;}
const spatialMs=performance.now()-queryStarted;
console.log(JSON.stringify({
  host:process.platform,node:process.version,engineering:{nodes:500,ticks,simulatedSeconds:ticks*dt,totalMs:+engineeringMs.toFixed(2),msPerTick:+(engineeringMs/ticks).toFixed(3),topologyRebuildsDuringRun:sim.rebuilds-initialRebuilds,conservedCargo:400},
  enemies:{initial:46,remaining:survival.enemies.length,ticks,totalMs:+enemyAiMs.toFixed(2),msPerTick:+(enemyAiMs/ticks).toFixed(3)},
  spatialIndex:{enemies:512,queries:50000,totalMs:+spatialMs.toFixed(2),microsecondsPerQuery:+(spatialMs/50).toFixed(3),verifiedAgainstBruteForce:100,hits}
},null,2));
