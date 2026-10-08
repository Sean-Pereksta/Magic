'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {loadEngine}=require('./performance-harness.cjs');
function arena(){
 const c=loadEngine();if(!c.ATSPalisadePose)vm.runInContext(fs.readFileSync(path.join(__dirname,'..','palisade-traversal.js'),'utf8'),c);
 const g=new c.ATSGame('PALISADE-CROSSING'),w=g.world;w.objects.clear();w._spatial.clear();w.sites.clear();w.ensure=()=>{};w.getSites=()=>[];w.terrain=()=>({biome:'farmland',road:true,water:false});w.vehicleTerrain=w.terrain;g.spawnSites=()=>{};g.king.x=-600;g.king.y=0;
 return{c,g,w,nav:g.navigation};
}
function wall(w,x=50,extra={}){return w.createFortification({x,y:0,w:18,h:300,team:'ape',height:34,...extra})}
function tick(g,a,target,dt=.02){g.time+=dt;g.navigation.beginFrame(g.time,{budgetMs:Infinity});g.navigation.move(a,target.x-a.x,target.y-a.y,90,dt,true)}
function cross(g,a,target,seconds=3){for(let i=0;i<seconds/.02;i++)tick(g,a,target)}
function object(w,o){w.objects.set(o.id,o);w._indexObject(o);w.navRevision++;return o}

test('the king and every primate species climb, crest and land across a friendly palisade',()=>{
 const {g,w,c}=arena(),b=wall(w);
 for(const [i,species]of ['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin'].entries()){
  const a=i===0?g.king:g.makeApe(0,0,'follow');a.x=0;a.y=0;a.species=species;const phases=new Set();let high=0;
  for(let n=0;n<140;n++){tick(g,a,{x:160,y:0});const p=c.ATSPalisadePose(a);if(p){phases.add(p.phase);high=Math.max(high,p.height)}}
  assert.ok(a.x>145,species+' reaches other side');assert.deepEqual([...phases],['climb','crest','jump']);assert.ok(high>=30);assert.equal(a.palisadeClimb,undefined);assert.equal(b.hp,b.maxHp);
 }
});
test('three concentric late-game defense sections each get a separate safe crossing',()=>{
 const {g,w}=arena();for(const x of[50,155,260])wall(w,x);const a=g.makeApe(0,0,'young'),seen=new Set();
 for(let n=0;n<450;n++){tick(g,a,{x:360,y:0});if(a.palisadeClimb)seen.add(a.palisadeClimb.wallId)}
 assert.equal(seen.size,3);assert.ok(a.x>345);assert.equal(a.palisadeClimb,undefined);
});
test('the landing finishes after movement input is released and a second move cannot double its speed',()=>{
 const {g,w,nav}=arena();wall(w);const a=g.king;a.x=25;a.y=0;tick(g,a,{x:160,y:0});assert.ok(a.palisadeClimb);
 const initial=a.palisadeClimb.elapsed;nav.move(a,200,0,90,.02,true);assert.equal(a.palisadeClimb.elapsed,initial);
 // Real updates, with movement and settlement work inactive, continue only the active set.
 g.apes=[];g.humans=[];g.updateKing=()=>{};for(let i=0;i<55;i++)g.update(.02,{});
 assert.equal(a.palisadeClimb,undefined);assert.ok(a.x>60);assert.equal(nav._palisadeClimbers.size,0);
});
test('walls, huts and water still stop traversal; hostile infantry receives no friendly pass',()=>{
 for(const kind of['hut','water']){const {g,w,nav}=arena();wall(w);const a=g.makeApe(0,0,'follow');
  if(kind==='hut')object(w,{id:'landing-hut',type:'apeBuilding',x:83,y:0,w:30,h:500,r:20,collision:'rect',solid:true,hp:100});
  else w.terrain=(x)=>({water:x>69,road:false,biome:'forest'});
  cross(g,a,{x:170,y:0},2);assert.ok(a.x<70,kind);assert.equal(nav.blocked(a.x,a.y,10,'ape'),false);
 }
 const {g,w}=arena();wall(w,50,{h:3000});const h=g.makeHuman(0,0,null);cross(g,h,{x:150,y:0},1);assert.ok(h.x<35);assert.equal(h.palisadeClimb,undefined);assert.equal(w.actorBlocked(50,0,10,'human'),true);
});
test('save records restore mid-climb without using fortress-climbing state, while malformed records are discarded',()=>{
 const {g,w,nav,c}=arena();wall(w);const a=g.makeApe(25,0,'follow');for(let n=0;n<8;n++)tick(g,a,{x:140,y:0});
 const saved=JSON.parse(JSON.stringify(g.savedActor(a)));assert.ok(saved.palisadeClimb);assert.equal(saved.wallClimb,undefined);assert.equal(saved.climbingWallId,undefined);
 nav.restorePalisadeClimb(saved);assert.ok(nav._palisadeClimbers.has(saved));cross(g,saved,{x:140,y:0});assert.ok(saved.x>125);assert.equal(saved.palisadeClimb,undefined);
 const bad={...saved,palisadeClimb:{wallId:'fake',fromX:1,fromY:1,exitX:100000,exitY:0}};nav.restorePalisadeClimb(bad);assert.equal(bad.palisadeClimb,undefined);assert.equal(c.ATSPalisadePose(bad),null);
});
test('a full campaign save restores the active crossing and can finish after reload',()=>{
 const {g,w,c}=arena();wall(w);const a=g.makeApe(28,0,'follow');for(let i=0;i<9;i++)tick(g,a,{x:160,y:0});const elapsed=a.palisadeClimb.elapsed;
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),restored=loaded.apes.find(p=>p.id===a.id);assert.equal(restored.palisadeClimb.elapsed,elapsed);assert.ok(loaded.navigation._palisadeClimbers.has(restored));
 loaded.world.ensure=()=>{};loaded.world.terrain=()=>({biome:'farmland',road:true,water:false});for(let i=0;i<40;i++){loaded.time+=.02;loaded.navigation.stepPalisadeClimb(restored,.02)}assert.equal(restored.palisadeClimb,undefined);assert.ok(restored.x>60);
});
test('death, a destroyed barrier and a new landing obstruction safely cancel an in-progress climb',()=>{
 for(const scenario of['death','destroy','obstruction']){const {g,w,nav}=arena(),b=wall(w),a=g.makeApe(28,0,'follow');tick(g,a,{x:150,y:0});assert.ok(a.palisadeClimb);
  if(scenario==='death')g.hurt(a,100000,{type:'bullet',x:0,y:0});
  else if(scenario==='destroy')w.damageFortification(b,10000,g.time);
  else object(w,{id:'new-hut',x:66,y:0,w:30,h:100,r:15,collision:'rect',solid:true,hp:100});
  for(let i=0;i<50&&a.palisadeClimb;i++){g.time+=.02;nav.stepPalisadeClimb(a,.02)}
  assert.equal(a.palisadeClimb,undefined,scenario);assert.equal(nav._palisadeClimbers.has(a),false);assert.ok(Number.isFinite(a.x)&&Number.isFinite(a.y));
 }
});
test('1000 apes near distant fortifications use local index queries instead of scanning all structures',()=>{
 const {g,w,nav}=arena();for(let i=0;i<400;i++)wall(w,2000+i*90,{y:2000,h:80});wall(w);let queries=0,candidates=0;
 const query=w._queryCollision.bind(w);w._queryCollision=(x,y,u,v,fn)=>{queries++;return query(x,y,u,v,o=>{candidates++;return fn(o)})};
 const actors=Array.from({length:1000},(_,i)=>({id:'ape-load-'+i,x:28,y:i%50-25,hp:100}));g.time=1;nav.beginFrame(1);
 for(const a of actors)nav.move(a,200,0,90,.02,true);
 assert.equal(actors.filter(a=>a.palisadeClimb).length,1000);assert.ok(queries<=2100);assert.ok(candidates<=2100,'distant objects never enter the crossing query');assert.equal(nav.stats.searches,0);
});
test('new defensive and growth structures get destructible collision proxies',()=>{
 const {g,w}=arena();const kinds=['spearTower','spearBattery','spearBallista','nursery','rallyGrove','orchard'],s={id:'buildings',x:0,y:0,huts:[],structures:[],facilities:kinds.map((kind,i)=>({id:'building-'+i,kind,x:i*120,y:250,hp:100,maxHp:100,stage:4}))};
 w.syncSettlementBuildings(s);for(const f of s.facilities){const o=w.objects.get(f.id+':collision');assert.ok(o);assert.equal(o.kind,f.kind);assert.equal(o.solid,true);f.hp=0;w.syncSettlementBuildings(s);assert.equal(o.solid,false)}
});
test('climbing bypasses the crowd atlas and lifts species artwork with raised climbing arms',()=>{
 const {g,w,c}=arena();wall(w);const a=g.makeApe(28,0,'follow');for(let i=0;i<8;i++)tick(g,a,{x:150,y:0});
 const calls=[];function Renderer(){}Renderer.prototype.drawPrimateGeometry=function(ctx,actor){calls.push({geometry:true,actor})};Renderer.prototype.drawApe=function(ctx,actor){this.drawPrimateGeometry(ctx,actor)};Renderer.prototype.drawApeSprite=function(){calls.push({atlas:true})};c.ATSRenderer=Renderer;
 vm.runInContext(fs.readFileSync(path.join(__dirname,'..','palisade-render.js'),'utf8'),c);const r=new Renderer();r.time=g.time;r.reducedMotion=false;const ctx=new Proxy({translate:(x,y)=>calls.push({translate:[x,y]})},{get:(o,k)=>o[k]||(()=>{}),set:(o,k,v)=>(o[k]=v,true)});
 assert.equal(r.drawApeSprite(ctx,a),false);assert.ok(calls.some(v=>v.translate?.[1]<-10));assert.ok(calls.some(v=>v.geometry&&v.actor.climbingVehicleId==='friendly-palisade'));assert.equal(calls.some(v=>v.atlas),false);assert.equal(a.climbingVehicleId,undefined,'temporary pose does not leak into simulation');
});
