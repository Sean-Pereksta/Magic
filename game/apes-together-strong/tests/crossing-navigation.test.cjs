'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const {loadEngine}=require('./performance-harness.cjs');
function engine(){const c=vm.createContext({console,Math,Map,Set});c.window=c;for(const f of ['world','navigation'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',f+'.js'),'utf8'),c);return c}
function terrainWorld(c,seed='crossing-regression'){const w=new c.ATSWorld(seed);w.ensure=()=>{};return w}
function add(w,o){o={hp:500,maxHp:500,solid:true,dead:false,r:10,...o};w.objects.set(o.id,o);w._indexObject(o);return o}

test('all four procedural crossing categories expose at least two clear nav cells and matching collision',()=>{
 const c=engine(),types=new Set();
 for(const seed of ['BRIDGE-A','BRIDGE-B','BRIDGE-C']){
  const w=terrainWorld(c,seed);
  for(let row=-1;row<=1;row++)for(let col=-3;col<=3;col++){
   const b=w.crossing(row,col);types.add(b.type);assert.ok(b.width>=96);
   for(const offset of [-28,0,28])for(let y=b.approaches[0].y;y<=b.approaches[1].y;y+=5)assert.equal(w.waterBlocked(b.x+offset,y,10),false,`${b.id} lane ${offset}`);
   assert.equal(w.terrain(b.x,b.y).crossing.id,b.id);
   assert.equal(w.terrain(b.x,b.y).crossingType,b.type);
   assert.equal(w.waterBlocked(b.maxX+30,w._riverInfo(b.maxX+30,b.y).centerY,10),true,'deep water beside deck stays impassable');
   assert.equal(w.vehicleBlocked(b.x,b.y,31,'tank'),!b.vehicleCompatible);
  }
 }
 assert.deepEqual([...types].sort(),['ford','military','stone','wood']);
});

test('generated scenery leaves both banks and every crossing lane connected',()=>{
 const c=engine();
 for(const seed of ['BANK-A','BANK-B','BANK-C']){
  const w=new c.ATSWorld(seed),b=w.crossing(0,1);w.ensure(b.x,b.y,430);
  for(const offset of [-28,0,28])for(let y=b.approaches[0].y;y<=b.approaches[1].y;y+=8)assert.equal(w.actorBlocked(b.x+offset,y,10,'ape'),false,`${seed} ${offset}, ${y}`);
  const regenerated=c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(w.serialize())));
  assert.deepEqual(JSON.parse(JSON.stringify(regenerated.crossing(0,1))),JSON.parse(JSON.stringify(b)),'save seed preserves crossing shape');
 }
});

test('followers select an offset crossing before reaching water and preserve it while their king moves',()=>{
 const c=engine(),w=terrainWorld(c),nav=new c.ATSNavigation(w),b=w.crossing(0,0),a={id:'ape-moving-king',x:b.x-240,y:b.minY-130};let selected=null,entered=false,exited=false;
 for(let frame=0;frame<1400;frame++){
  nav.beginFrame(frame/60,{budgetMs:Infinity});
  const target={x:b.x+(frame<90?-180:570),y:b.maxY+180};
  nav.move(a,target.x-a.x,target.y-a.y,110,1/60);
  if(frame===0){selected=a._navCrossing?.id;assert.equal(selected,b.id);assert.equal(a._navCrossing.phase,0)}
  if(a._navCrossing&&!exited){assert.equal(a._navCrossing.id,selected);if(a._navCrossing.phase===1)entered=true}
  if(entered&&!a._navCrossing)exited=true;
  assert.equal(w.waterBlocked(a.x,a.y,10),false);
 }
 assert.ok(entered&&exited);assert.ok(Math.hypot(a.x-(b.x+570),a.y-(b.maxY+180))<25,JSON.stringify(a));
});

test('240 apes use three physical lanes across stepping stones with bounded shared work',()=>{
 const c=engine(),w=terrainWorld(c),nav=new c.ATSNavigation(w),b=w.crossingsNear(1000,1300,8000).find(b=>b.type==='ford');assert.ok(b);
 const actors=Array.from({length:240},(_,i)=>({id:'ape-crossing-'+i,x:b.x-150+i%8*3,y:b.minY-120-Math.floor(i/8)*2})),target={x:b.x+150,y:b.maxY+130},lanes=new Set();
 for(let frame=0;frame<900;frame++){
  nav.beginFrame(frame/60,{budgetMs:Infinity});assert.ok(nav.stats.frameExpanded<=192);assert.ok(nav.stats.frameSearches<=3);
  for(const a of actors){nav.move(a,target.x-a.x,target.y-a.y,110,1/60);if(a._navCrossing?.phase===1)lanes.add(Math.round(a._navCrossing.far.x-b.x));assert.equal(w.waterBlocked(a.x,a.y,10),false)}
 }
 assert.equal(actors.filter(a=>Math.hypot(a.x-target.x,a.y-target.y)<25).length,240);
 assert.equal(lanes.size,3);assert.ok(nav.stats.searches<20);assert.ok(nav.pending.size<20);
});

test('distant abstract recalled followers seek the crossing with collision-safe coarse movement',()=>{
 const c=loadEngine(),g=new c.ATSGame('DISTANT-CROSSING'),w=g.world;
 w.objects.clear();w._spatial.clear();w.sites.clear();w.ensure=()=>{};w._streaming=false;
 const b=w.crossing(0,0),a=g.makeApe(b.x-210,b.minY-400,'follow');a.recallOrder={kind:'field'};a._simTier=2;
 g.king.x=b.x+220;g.king.y=b.maxY+1500;
 for(let frame=0;frame<1800;frame++){
  g.time=frame/60;g.navigation.beginFrame(g.time,{budgetMs:Infinity});
  if(frame%30===0){g.abstractActor(a,.5,'ape');assert.equal(w.waterBlocked(a.x,a.y,10),false)}
 }
 assert.ok(Math.hypot(a.x-g.king.x,a.y-g.king.y)<80,JSON.stringify(a));
 assert.ok(w._corridorRequests.size>0);assert.ok(w._corridorRequests.size<=24);
});

test('a loaded follower on the bridge completes its crossing before pursuing a sideways target',()=>{
 const c=engine(),w=terrainWorld(c),nav=new c.ATSNavigation(w),b=w.crossing(0,0),a={id:'ape-loaded',x:b.x,y:b.y},target={x:b.x+500,y:b.maxY+150};
 nav.beginFrame(0);nav.move(a,target.x-a.x,target.y-a.y,110,1/60);assert.equal(a._navCrossing.id,b.id);assert.equal(a._navCrossing.phase,1);
 for(let frame=1;frame<600;frame++){nav.beginFrame(frame/60,{budgetMs:Infinity});nav.move(a,target.x-a.x,target.y-a.y,110,1/60);assert.equal(w.waterBlocked(a.x,a.y,10),false)}
 assert.ok(Math.hypot(a.x-target.x,a.y-target.y)<25);
});

test('a thousand recalled apes coalesce streaming requests by region instead of rebuilding them per actor',()=>{
 const c=engine(),w=terrainWorld(c),nav=new c.ATSNavigation(w),actors=Array.from({length:1000},(_,i)=>({id:'ape-recall-'+i,x:200+i%20*20,y:-100+Math.floor(i/20)*5,recallOrder:{kind:'field'}})),target={x:2200,y:120};let requests=0;
 const request=w.requestCorridor.bind(w);w.requestCorridor=(...args)=>{requests++;return request(...args)};
 for(const time of [0,.1,.4]){nav.beginFrame(time,{budgetMs:Infinity});for(const a of actors)nav.steer(a,target,10,1/60);assert.ok(nav.stats.frameSearches<=3);assert.ok(nav.stats.frameExpanded<=192)}
 assert.ok(requests<=4,`only two regions should refresh twice, got ${requests}`);assert.ok(nav.pending.size<20);assert.ok(w._corridorRequests.size<=2);
});

test('unloaded approach chunks retain crossing commitment without futile searches',()=>{
 const c=engine(),w=terrainWorld(c),nav=new c.ATSNavigation(w),b=w.crossing(0,0),a={id:'ape-loading',x:b.x-180,y:b.minY-140},target={x:b.x+50,y:b.maxY+200};
 w.boundsReady=()=>false;
 for(let frame=0;frame<300;frame++){nav.beginFrame(frame/60,{budgetMs:Infinity});nav.steer(a,target,10,1/60);assert.equal(a._navCrossing.id,b.id)}
 assert.equal(nav.stats.searches,0);assert.equal(nav.stats.failures,0);assert.equal(nav.pending.size,0);assert.equal(a._navAvoidCrossings,undefined);
});

test('legacy save validation clears conflicting scenery but preserves player and fortress collision',()=>{
 const c=engine(),w=terrainWorld(c),b=w.crossing(0,1),rock=add(w,{id:'obj:legacy:0',type:'rock',x:b.x,y:b.approaches[0].y}),wall=add(w,{id:'player-wall',type:'wall',x:b.x+10,y:b.approaches[0].y});
 w.chunks.set('legacy',{id:'legacy',objects:[rock.id,wall.id],sites:[],generated:true});
 const loaded=c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(w.serialize())));
 assert.equal(loaded.objects.get(rock.id).solid,false);assert.equal(loaded.objects.get(wall.id).solid,true);assert.equal(loaded.objects.get(wall.id).hp,500);assert.equal(loaded.stats.crossingRepairs,1);
});

test('an obstructed bank approach falls back to a different legitimate crossing',()=>{
 const c=engine(),w=terrainWorld(c),nav=new c.ATSNavigation(w),b=w.crossing(0,0),cy=b.approaches[0].y;
 for(const o of [{id:'left',x:b.x-100,y:cy,w:18,h:100},{id:'right',x:b.x+100,y:cy,w:18,h:100},{id:'top',x:b.x,y:cy-50,w:200,h:18},{id:'bottom',x:b.x,y:cy+50,w:200,h:18}])add(w,{...o,collision:'rect'});
 const a={id:'ape-alternate-crossing',x:b.x-150,y:cy-140},target={x:b.x-100,y:b.maxY+180};let alternate=false;
 for(let frame=0;frame<2700;frame++){nav.beginFrame(frame/60,{budgetMs:Infinity});nav.move(a,target.x-a.x,target.y-a.y,110,1/60);alternate||=!!a._navCrossing&&a._navCrossing.id!==b.id;assert.equal(w.waterBlocked(a.x,a.y,10),false)}
 assert.ok(alternate);assert.ok(Math.hypot(a.x-target.x,a.y-target.y)<25,JSON.stringify(a));
});

test('unreachable fortress targets are rejected and a reachable human is selected without damaging walls',()=>{
 const c=loadEngine(),g=new c.ATSGame('SEALED-FORTRESS'),w=g.world;
 w.objects.clear();w._spatial.clear();w.sites.clear();w.ensure=()=>{};w.terrain=()=>({biome:'forest',water:false});w._streaming=false;g.king.x=-500;
 const a=g.makeApe(0,0,'follow');a.species='gorilla';g.siege.balance(a,true);
 const walls=[add(w,{id:'left',x:140,y:0,w:18,h:140,collision:'rect'}),add(w,{id:'right',x:240,y:0,w:18,h:140,collision:'rect'}),add(w,{id:'top',x:190,y:-65,w:100,h:18,collision:'rect'}),add(w,{id:'bottom',x:190,y:65,w:100,h:18,collision:'rect'})];
 const sealed=g.makeHuman(190,0,null),reachable=g.makeHuman(360,-130,null);sealed.hp=sealed.maxHp=1e6;reachable.hp=reachable.maxHp=1e6;
 g.syncIndexes();g.humanGrid.rebuild(g.humans);g.vehicleGrid.rebuild([]);g.command('nearestHuman',{x:1,y:0});let rejected=false,retargeted=false;
 for(let frame=0;frame<720;frame++){
  g.time=frame/60;g.performance.beginStep(g);g.navigation.beginFrame(g.time,{budgetMs:Infinity});g.planNearestOrders();g.updateApe(a,1/60);
  rejected||=!!a._nearestRejected?.[sealed.id];retargeted||=a._nearestTarget?.id===reachable.id;
 }
 assert.ok(rejected,'failed shared search marks a temporarily unreachable target');assert.ok(retargeted,'bounded target scan chooses an available alternative');assert.equal(sealed.hp,sealed.maxHp);assert.ok(reachable.hp<reachable.maxHp);
 for(const wall of walls)assert.equal(wall.hp,wall.maxHp);
});

test('opening a gate invalidates a negative route and makes the legal entrance usable',()=>{
 const c=engine(),w=terrainWorld(c),nav=new c.ATSNavigation(w);w.terrain=()=>({biome:'forest',water:false});
 for(const o of [{id:'left',x:100,y:0,w:20,h:200},{id:'right',x:300,y:0,w:20,h:200},{id:'top',x:200,y:-100,w:220,h:20},{id:'bottomLeft',x:125,y:100,w:70,h:20},{id:'bottomRight',x:275,y:100,w:70,h:20}])add(w,{...o,collision:'rect'});
 const gate=add(w,{id:'gate',type:'gate',x:200,y:100,w:80,h:20,collision:'rect'}),a={id:'ape-gate',x:200,y:200},target={x:200,y:0};
 for(let frame=0;frame<120;frame++){nav.beginFrame(frame/60,{budgetMs:Infinity});nav.move(a,target.x-a.x,target.y-a.y,100,1/60);assert.ok(a.y>119)}
 gate.dead=true;gate.solid=false;gate.hp=0;w.navRevision++;
 for(let frame=120;frame<420;frame++){nav.beginFrame(frame/60,{budgetMs:Infinity});nav.move(a,target.x-a.x,target.y-a.y,100,1/60)}
 assert.ok(Math.hypot(a.x-target.x,a.y-target.y)<25);assert.equal(w.actorBlocked(a.x,a.y,10,'ape'),false);
});
