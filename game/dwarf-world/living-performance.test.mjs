import test from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
const {FixedClock,SpatialHash,NavigationQueue,interpolatePosition}=require('./living-performance.js');

test('fixed simulation advances equally at 30, 60 and 144 render frames per second',()=>{
  for(const fps of [30,60,144]){
    const clock=new FixedClock(); let elapsed=0;
    for(let i=0;i<fps*10;i++) clock.advance(1/fps,dt=>elapsed+=dt);
    assert.ok(Math.abs(elapsed-10)<1e-8);
  }
});
test('long stalls and paused time never cause an unbounded catch-up',()=>{
  const clock=new FixedClock(); let calls=0;
  assert.equal(clock.advance(60,()=>calls++),3);
  assert.ok(clock.dropped>59);
  clock.advance(0.02,()=>calls++);
  clock.advance(40,()=>calls++,false);
  assert.equal(clock.accumulator,0);
  assert.equal(clock.advance(1/60,()=>calls++),0);
  assert.equal(calls,3);
});
test('navigation is incremental, bounded and produces connected walkable paths',()=>{
  const blocked=new Set(['3,0','3,1','3,2','3,3','3,4']);
  const nav=new NavigationQueue(9,9,(x,y)=>!blocked.has(`${x},${y}`));
  const goal=(x,y)=>x===7&&y===1;
  assert.equal(nav.request(1,1,'destination',goal),null);
  for(let i=0;i<100 && nav.stats.completed===0;i++) assert.ok(nav.tick(3)<=3);
  const path=nav.request(1,1,'destination',goal); assert.ok(path);
  let previous={x:1,y:1};
  for(const point of path){
    assert.equal(Math.abs(point.x-previous.x)+Math.abs(point.y-previous.y),1);
    assert.equal(blocked.has(`${point.x},${point.y}`),false); previous=point;
  }
  assert.deepEqual(previous,{x:7,y:1});
  path.shift();
  assert.equal(nav.request(1,1,'destination',goal).length,path.length+1,'cached routes must not be mutated by entity movement');
});
test('unreachable goals back off and opening terrain allows immediate retry',()=>{
  let open=false;
  const nav=new NavigationQueue(5,3,(x)=>x!==2||open);
  const goal=(x,y)=>x===4&&y===1;
  nav.request(0,1,'destination',goal); nav.tick(100);
  const expanded=nav.stats.expanded;
  for(let i=0;i<20;i++){ nav.request(0,1,'destination',goal); nav.tick(100,0.01); }
  assert.equal(nav.stats.expanded,expanded);
  open=true; nav.invalidate(false);
  nav.request(0,1,'destination',goal); nav.tick(100);
  assert.deepEqual(nav.request(0,1,'destination',goal).at(-1),{x:4,y:1});
});
test('blocking infrastructure cancels cached routes, including actor-specific gates',()=>{
  let closed=false;
  const nav=new NavigationQueue(7,1,(x,y,enemy)=>x!==3||(!enemy&&!closed));
  const goal=(x)=>x===6;
  nav.request(0,0,'gate',goal); nav.request(0,0,'gate',goal,{enemy:true}); nav.tick(100);
  assert.equal(nav.request(0,0,'gate',goal).length,6);
  assert.equal(nav.request(0,0,'gate',goal,{enemy:true}),null);
  closed=true; nav.invalidate(); nav.request(0,0,'gate',goal); nav.tick(100);
  assert.equal(nav.request(0,0,'gate',goal),null);
});
test('closest approach is produced within the same bounded search',()=>{
  const nav=new NavigationQueue(8,1,x=>x<4);
  nav.request(0,0,'approach',x=>x===7,{closest:{x:7,y:0}}); nav.tick(100);
  assert.deepEqual(nav.request(0,0,'approach',x=>x===7).at(-1),{x:3,y:0});
});
test('closest-approach fallback is never reused as a completed point route',()=>{
  const nav=new NavigationQueue(8,2,x=>x<4);
  const goal=(x,y)=>x===7&&y===0;
  nav.request(0,0,'point:7,0:0',goal,{closest:{x:7,y:0}});nav.tick(100);
  assert.deepEqual(nav.request(0,0,'point:7,0:0',goal).at(-1),{x:3,y:0});
  assert.equal(nav.regionalRoutes.size,0);
  nav.request(0,1,'point:7,0:0',goal);nav.tick(100);
  assert.equal(nav.request(0,1,'point:7,0:0',goal),null);
  assert.equal(nav.stats.regionalHits,0);
});
test('queue and cache limits bound retained work',()=>{
  const nav=new NavigationQueue(12,12,()=>true,{maxQueued:4,maxCache:5});
  for(let i=0;i<30;i++) nav.request(0,0,`goal${i}`,(x,y)=>x===11&&y===11);
  assert.equal(nav.pending.length,4); assert.equal(nav.stats.deferred,26);
  nav.tick(2000);
  for(let i=0;i<20;i++){ nav.request(0,0,`second${i}`,(x,y)=>x===1&&y===1); nav.tick(100); }
  assert.ok(nav.entries.size<=5);
});
test('spatial query agrees with brute force after movement and negative coordinates',()=>{
  const spatial=new SpatialHash(8);
  const items=Array.from({length:300},(_,i)=>({id:i,x:(i*17)%137-50,y:(i*13)%89-40}));
  spatial.rebuild(items);
  for(let i=0;i<50;i++){items[i].x+=70; spatial.update(items[i]);}
  for(const [x,y,r] of [[0,0,12],[60,20,7],[-35,-12,20]]){
    const exact=list=>list.filter(p=>(p.x-x)**2+(p.y-y)**2<=r*r).map(p=>p.id).sort((a,b)=>a-b);
    assert.deepEqual(exact(spatial.query(x,y,r)),exact(items));
  }
});
test('abandoned targets expire but actively retried requests remain queued',()=>{
  const nav=new NavigationQueue(100,100,()=>true);
  const goal=(x,y)=>x===99&&y===99;
  nav.request(0,0,'abandoned',goal); nav.request(1,1,'alive',goal);
  nav.tick(1,2);
  nav.request(1,1,'alive',goal);
  nav.tick(1,2);
  assert.equal(nav.stats.canceled,1);
  assert.equal(nav.active.key,'1,1:alive');
});
test('render interpolation smooths fixed steps without mutating position and snaps teleports',()=>{
  const current={x:8,y:5},previous={x:7,y:5};
  assert.deepEqual(interpolatePosition(current,previous,.25),{x:7.25,y:5});
  assert.deepEqual(current,{x:8,y:5});assert.deepEqual(previous,{x:7,y:5});
  assert.deepEqual(interpolatePosition(current,previous,2),current);
  assert.deepEqual(interpolatePosition({x:200,y:5},previous,.1),{x:200,y:5});
  assert.deepEqual(interpolatePosition(current,null,.1),current);
});
test('different starts join a shared point route across region boundaries',()=>{
  const nav=new NavigationQueue(80,3,()=>true),goal=(x,y)=>x===79&&y===1;
  nav.request(0,1,'point:79,1:0',goal);nav.tick(1000);
  const previousExpanded=nav.stats.expanded;
  nav.request(10,0,'point:79,1:0',goal);nav.tick(1000);
  const route=nav.request(10,0,'point:79,1:0',goal);
  assert.ok(nav.stats.regionalHits>0);
  assert.ok(nav.stats.expanded-previousExpanded<10,'joining the known region route avoids a repeated full search');
  let previous={x:10,y:0};const seen=new Set(['10,0']);
  for(const p of route){assert.equal(Math.abs(p.x-previous.x)+Math.abs(p.y-previous.y),1);assert.ok(!seen.has(`${p.x},${p.y}`));seen.add(`${p.x},${p.y}`);previous=p;}
  assert.deepEqual(previous,{x:79,y:1});
  nav.invalidate();assert.equal(nav.regionalCells,0);assert.equal(nav.regionalRoutes.size,0);
});
test('regional routes respect global memory bounds and actor-specific gates',()=>{
  const nav=new NavigationQueue(100,2,(x,y,enemy)=>x!==40||!enemy);
  for(let i=50;i<90;i++){
    nav.request(0,0,`point:${i},0:0`,(x,y)=>x===i&&y===0);nav.tick(1000,0);
  }
  assert.ok(nav.regionalCells<=8192);assert.ok(nav.regionalRoutes.size<=32);
  nav.request(0,0,'point:89,0:0',(x,y)=>x===89&&y===0,{enemy:true});nav.tick(1000);
  assert.equal(nav.request(0,0,'point:89,0:0',(x,y)=>x===89&&y===0,{enemy:true}),null);
});
