'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');

test('pooled crowd queries retain the exact nearest eligible neighbors and clear stale buckets',()=>{
 const c=loadEngine(),g=new c.ATSGame('crowd-buffer'),grid=g.apeGrid,actors=[];
 for(let i=0;i<80;i++)actors.push({id:'ape-'+i,x:(i%9)*8,y:Math.floor(i/9)*7,hp:100,_separationStamp:i%3?12:11});
 grid.rebuild(actors);const out=[],distances=[];
 for(const a of actors){
  const expected=grid.nearest(a.x,a.y,48,8,p=>p!==a&&p.hp>0&&(p.id>a.id||p._separationStamp!==12));
  assert.deepEqual(Array.from(grid.separationNeighbors(a,12,out,distances),p=>p.id),Array.from(expected,p=>p.id));
 }
 const oldBuckets=Array.from(grid.cells.values());grid.rebuild([{id:'new',x:900,y:900,hp:1}]);
 assert.equal(grid.near(0,0,100).length,0,'removed actors do not remain indexed');
 assert.ok(oldBuckets.includes(Array.from(grid.cells.values())[0]),'bucket arrays are reused');
 assert.ok(grid.free.length<=4096);
});

test('crowd collision candidates are shared while wall, water and changed geometry remain exact',()=>{
 const c=loadEngine(),g=new c.ATSGame('crowd-candidates'),w=g.world,n=g.navigation;
 w.objects.clear();w._spatial.clear();w._streaming=false;w.terrain=(x,y)=>({biome:'forest',water:y>15});
 const wall={id:'thin-wall',x:16,y:0,w:2,h:100,r:1,collision:'rect',solid:true,hp:100};w.objects.set(wall.id,wall);w._indexObject(wall);
 let queries=0;const query=w._queryCollision;w._queryCollision=function(...args){queries++;return query.apply(this,args)};
 const a={x:5,y:0},b={x:30,y:0};n.beginSeparation();
 for(let i=0;i<50;i++)assert.equal(n.separationClear(a,b),false);
 assert.equal(queries,1,'one broad-phase query serves fifty overlapping crowd pairs');
 wall.dead=true;assert.equal(n.separationClear(a,b),true,'live destruction is checked for every pair');
 assert.equal(n.separationClear(a,{x:5,y:25}),false,'water tests remain live');
 wall.dead=false;n.beginSeparation();assert.equal(n.separationClear(a,b),false);assert.equal(queries,2,'each separation step refreshes live candidates');
});

test('native dry-ground bounds never skip rivers or custom terrain',()=>{
 const c=loadEngine(),w=new c.ATSWorld('water-envelope');
 assert.equal(w.waterFreeBounds(-200,0,200,200),true,'ordinary forest avoids repeated river sampling');
 for(let row=-3;row<=3;row++)for(let x=-3000;x<=3000;x+=79){
  const river=w._riverInfo(x,row*2304+1320);
  assert.equal(w.waterFreeBounds(x-10,river.centerY-river.width-10,x+10,river.centerY+river.width+10),false,'all natural river banks retain exact checks');
 }
 w.terrain=()=>({water:true,biome:'wetland'});assert.equal(w.waterFreeBounds(-20,0,20,20),false,'custom water geometry disables the shortcut');
 const n=new c.ATSNavigation(w);w._streaming=false;assert.equal(n.clearSegment(0,0,20,0,10),false);
});

test('offscreen local workers reach their physical work station beyond the coarse arrival radius',()=>{
 const c=loadEngine(),g=new c.ATSGame('worker-arrival');g.world.objects.clear();g.world._spatial.clear();g.world._streaming=false;g.world.terrain=()=>({biome:'forest',water:false});
 const s={id:'distant-work',x:5000,y:0,population:1};g.settlements.push(s);g.syncIndexes();
 const a=g.makeApe(5000,0,'settled',s.id);a._activityTarget={x:5025,y:0,speed:30};a._activityAt=100;
 g.abstractActor(a,.5,'ape');assert.ok(a.x>5000,'workers do not stop 25 units before their assigned point');
 g.abstractActor(a,.5,'ape');assert.ok(Math.hypot(a.x-5025,a.y)<=8,'offscreen movement reaches actual delivery distance');
});
