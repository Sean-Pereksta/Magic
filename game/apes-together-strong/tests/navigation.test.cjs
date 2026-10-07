const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
function engine(){const ctx=vm.createContext({console,Math,Map,Set});ctx.window=ctx;for(const f of ['world','navigation','settlements','forces','sim'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',f+'.js'),'utf8'),ctx);return ctx}
function moveTo(nav,actors,target,seconds=25){for(let i=0;i<seconds*60;i++){nav.beginFrame(i/60);for(const a of actors)nav.move(a,target.x-a.x,target.y-a.y,100,1/60)}return actors}
function empty(ctx){const w=new ctx.ATSWorld('navigation');w.ensure=()=>{};w.terrain=()=>({biome:'forest',water:false,road:false});w.objects.clear();w._spatial.clear();return w}
function add(w,o){o={hp:100,solid:true,dead:false,...o};w.objects.set(o.id,o);w._indexObject(o)}
test('routes around a U-shaped enclosure instead of pressing against its wall',()=>{const c=engine(),w=empty(c);for(const o of [{id:'back',x:170,y:0,w:20,h:320},{id:'top',x:60,y:-160,w:240,h:20},{id:'bottom',x:60,y:160,w:240,h:20}])add(w,{...o,r:10,collision:'rect'});const nav=new c.ATSNavigation(w),a={id:'ape-1',x:100,y:0};moveTo(nav,[a],{x:300,y:0});assert.ok(Math.hypot(a.x-300,a.y)<24,JSON.stringify(a));});
test('100 apes and humans cross a dense deterministic forest without remaining at trunks',()=>{const c=engine(),w=empty(c);const random=c.ATSUtil.rng('forest-test');for(let x=60;x<580;x+=64)for(let y=-320;y<=320;y+=64)add(w,{id:'tree'+x+','+y,x:x+(random()-.5)*25,y:y+(random()-.5)*25,r:23,moveRadius:8});const nav=new c.ATSNavigation(w),actors=Array.from({length:100},(_,i)=>({id:(i%2?'human-':'ape-')+i,x:-60-(i%10)*3,y:(i-50)*4}));moveTo(nav,actors,{x:700,y:0},30);const reached=actors.filter(a=>Math.hypot(a.x-700,a.y)<30);if(reached.length<100)console.log(JSON.stringify(actors.find(a=>!reached.includes(a))),nav.stats);assert.equal(reached.length,100);for(const a of actors)assert.equal(w.blocked(a.x,a.y,10),false);});
test('destroying an obstacle invalidates cached routes and actors never cross a live wall',()=>{const c=engine(),w=empty(c),wall={id:'wall',x:150,y:0,w:16,h:600,r:8,collision:'rect',hp:100,solid:true};add(w,wall);const nav=new c.ATSNavigation(w),a={id:'human-2',x:50,y:0};moveTo(nav,[a],{x:260,y:0},.6);assert.ok(a.x<132);w.objects.get('wall').dead=true;w.navRevision++;moveTo(nav,[a],{x:260,y:0},5);assert.ok(a.x>240);});
test('river crossing is routed through a bridge',()=>{const c=engine(),w=empty(c);w.terrain=(x,y)=>({biome:'forest',water:x>80&&x<150&&Math.abs(y-230)>36,road:Math.abs(y-230)<36});const nav=new c.ATSNavigation(w),a={id:'ape-3',x:0,y:0};moveTo(nav,[a],{x:260,y:0},15);assert.ok(Math.hypot(a.x-260,a.y)<25,JSON.stringify(a));});
test('tree canopy remains large while trunk collision is smaller and still solid',()=>{const c=engine(),w=empty(c);add(w,{id:'tree',x:0,y:0,r:24,moveRadius:8});assert.equal(w.blocked(0,0,10),true);assert.equal(w.blocked(20,0,10),false);assert.equal(w.lineClear(-60,18,60,18),false);});
test('the real follower AI brings 100 apes through a generated forest without returning to stale trail points',()=>{
  const c=engine(),g=new c.ATSGame('FOREST-A'),random=c.ATSUtil.rng('follower-regression');
  g.spawnSites=()=>{};
  for(let i=0;i<100;i++){
    const a=g.makeApe(-30-i%10*4,(i-50)*3,'follow');
    a.speed=84+random()*18;a.offsetX=(random()-.5)*120;a.offsetY=(random()-.5)*120;a.phase=random()*Math.PI*2;
  }
  Object.assign(g.king,g.findOpen(720,0,12));
  for(let i=0;i<25*60;i++)g.update(1/60,{});
  // Larger hordes now occupy more ground; every follower must still reach the king's clearing.
  assert.equal(g.apes.filter(a=>Math.hypot(a.x-g.king.x,a.y-g.king.y)<210).length,100);
  for(const a of g.apes)assert.equal(g.world.blocked(a.x,a.y,10),false);
});
