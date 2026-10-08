'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function empty(g){g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world._streaming=false;g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e6;}
function object(g,o){o={r:10,solid:true,hp:1000,maxHp:1000,...o};g.world.objects.set(o.id,o);g.world._indexObject(o);return o}

test('a real 120-ape recall crosses a ford under crowd separation without leaving actors at the bank',()=>{
 const c=loadEngine(),g=new c.ATSGame('CROWD-FORD');empty(g);
 const w=g.world,b=w.crossingsNear(1000,1300,8000).find(p=>p.type==='ford');assert.ok(b);
 g.king.x=b.x;g.king.y=b.maxY+360;
 for(let i=0;i<120;i++){const a=g.makeApe(b.x+(i%12-5.5)*18,b.minY-110-Math.floor(i/12)*22,'hold');a.offsetX=(i%12-5.5)*12;a.offsetY=(Math.floor(i/12)-4.5)*12;}
 g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.command('recallField');
 const begin=g.navigation.beginFrame.bind(g.navigation);g.navigation.beginFrame=(time,options={})=>begin(time,{...options,budgetMs:Infinity});
 let peakCrossing=0;
 for(let frame=0;frame<1800;frame++){
  g.update(1/60,{});peakCrossing=Math.max(peakCrossing,g.apes.filter(a=>a._navCrossing?.phase===1).length);
  if(frame%30===0)for(const a of g.apes)assert.equal(w.waterBlocked(a.x,a.y,10),false);
  assert.ok(g.navigation.stats.frameExpanded<=192);assert.ok(g.navigation.stats.frameSearches<=3);
 }
 assert.ok(peakCrossing>10,'exercise actual simultaneous bridge congestion');
 const stranded=g.apes.filter(a=>a.y<b.maxY+20);assert.equal(stranded.length,0,JSON.stringify(stranded.map(a=>({id:a.id,x:a.x,y:a.y,crossing:a._navCrossing,nav:a._nav}))));
 assert.equal(g.apes.filter(a=>Math.hypot(a.x-g.king.x,a.y-g.king.y)<250).length,120);
});

test('human-only nearest attack cannot damage a human through a live thin fortress wall',()=>{
 const c=loadEngine(),g=new c.ATSGame('WALL-MELEE');empty(g);g.world.terrain=()=>({biome:'forest',water:false});g.king.x=-500;
 const a=g.makeApe(0,0,'follow');a.species='gorilla';g.siege.balance(a,true);a.attackCD=0;
 const h=g.makeHuman(28,0,null),wall=object(g,{id:'wall-melee',type:'wall',faction:'human',x:14,y:0,w:2,h:220,collision:'rect',height:95,visualHeight:95,climbable:false});
 g.syncIndexes();g.humanGrid.rebuild(g.humans);g.apeGrid.rebuild([g.king,a]);g.navigation.beginFrame(0);g.command('nearestHuman',{x:1,y:0});
 for(let frame=0;frame<30;frame++){g.time=frame/60;g.performance.beginStep(g);g.navigation.beginFrame(g.time,{budgetMs:Infinity});g.planNearestOrders();g.updateApe(a,1/60);}
 assert.equal(h.hp,h.maxHp);assert.equal(wall.hp,wall.maxHp);assert.ok(a.x<3);
 wall.dead=true;wall.solid=false;wall.hp=0;g.world.navRevision++;
 for(let frame=30;frame<120;frame++){g.time=frame/60;g.performance.beginStep(g);g.navigation.beginFrame(g.time,{budgetMs:Infinity});g.planNearestOrders();g.updateApe(a,1/60);}
 assert.ok(h.hp<h.maxHp,'the same target becomes attackable through a real opening');
});

test('a subsequent direct order cancels a recalled bridge commitment and survives saving',()=>{
 const c=loadEngine(),g=new c.ATSGame('RECALL-OVERRIDE');empty(g);const b=g.world.crossing(0,0),a=g.makeApe(b.x-150,b.minY-120,'follow');g.king.x=b.x;g.king.y=b.maxY+250;
 g.syncIndexes();g.command('recallField');g.navigation.beginFrame(0);g.navigation.steer(a,g.king,10,.02);assert.ok(a._navCrossing);
 g.command('nearestHuman',{x:-1,y:0});assert.equal(a.recallOrder,undefined);assert.equal(a._navCrossing,undefined);assert.equal(a.hordeOwner,'king');
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),copy=loaded.apesById.get(a.id);
 assert.equal(copy.hordeOwner,'king');assert.equal(copy.recallOrder,undefined);assert.equal(copy.nearestOrder.kind,'human');assert.equal(copy.nearestOrder.dx,-1);
});

test('a legal sub-cell fortress opening earns one bounded refined corridor without ignoring walls',()=>{
 const c=loadEngine(),g=new c.ATSGame('NARROW-ENTRANCE');empty(g);g.world.terrain=()=>({biome:'forest',water:false});
 for(const o of [{id:'left',x:-70,y:0,w:20,h:220},{id:'right',x:310,y:0,w:20,h:220},{id:'top',x:120,y:-100,w:400,h:20},{id:'bottom',x:120,y:100,w:400,h:20},{id:'upper-divider',x:150,y:-49,w:20,h:82},{id:'lower-divider',x:150,y:56,w:20,h:68}])object(g,{...o,collision:'rect'});
 const a=g.makeApe(0,0,'follow'),nav=g.navigation,target={x:240,y:0};
 for(let frame=0;frame<720;frame++){g.time=frame/60;nav.beginFrame(g.time,{budgetMs:Infinity});nav.move(a,target.x-a.x,target.y-a.y,90,1/60);assert.equal(g.world.actorBlocked(a.x,a.y,10,'ape'),false);assert.ok(nav.stats.frameExpanded<=192);assert.ok(nav.stats.frameSearches<=3)}
 assert.ok(Math.hypot(a.x-target.x,a.y-target.y)<25);assert.ok(nav.stats.searches>=2,'coarse and refined route both execute');assert.ok(nav.stats.searches<6,'the refined corridor is reused');
});

test('formation slots across a river fall back to the king bank and committed movement ignores arrival radius',()=>{
 const c=loadEngine(),g=new c.ATSGame('SHORE-FORMATION');empty(g);const w=g.world,b=w.crossing(0,0),a=g.makeApe(b.x-160,b.minY-120,'follow');
 g.king.x=b.x-160;g.king.y=w._riverInfo(g.king.x,b.y).centerY+90;
 const slot=g.navigation.followTarget(a,g.king,g.king.x,g.king.y-240);assert.equal(slot.x,g.king.x);assert.equal(slot.y,g.king.y);
 a._navCrossing={id:'in-progress'};a.x=g.king.x;a.y=g.king.y;a.offsetX=0;a.offsetY=0;
 const before=a.y;let steered=false;const steer=g.navigation.steer;g.navigation.steer=()=>{steered=true;return {x:a.x,y:a.y+40}};
 g.abstractActor(a,.2,'ape');assert.ok(steered);assert.ok(a.y>before,'coarse follow movement advances an existing crossing');g.navigation.steer=steer;
 g.navigation.resetActor(a);a.x=b.x;a.y=b.y;g.navigation.beginFrame(1);g.navigation.steer(a,{x:b.x,y:b.maxY+150},10,.02);assert.ok(a._navCrossing);const deckY=a.y;
 g.navigation.move(a,0,0,90,.02);assert.ok(a.y>deckY,'zero final-target delta cannot stop a committed deck crossing');
});
