const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
function engine(){const c=vm.createContext({console,Math,Map,Set});c.window=c;for(const name of ['world','navigation'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),c);return c}
function empty(c){const w=new c.ATSWorld('queue-test');w.ensure=()=>{};w.terrain=()=>({biome:'forest',water:false,road:false});return w}
function add(w,o){o={hp:100,solid:true,dead:false,...o};w.objects.set(o.id,o);w._indexObject(o)}

test('long routes are queued, prioritize combat and obey every tick expansion budget',()=>{
  const c=engine(),nav=new c.ATSNavigation(empty(c));nav.beginFrame(0);
  const distant={x:0,y:84},combat={x:0,y:0},goal={x:1400,y:0};
  assert.equal(nav.findPath(distant,goal,10,5),null);
  assert.equal(nav.findPath(combat,goal,10,1),null);
  assert.equal(nav.stats.searches,0);
  const low=[...nav.pending.values()][0];let route=null;
  for(let tick=1;tick<300&&!route;tick++){
    nav.beginFrame(tick/60,{maxExpanded:4,maxRequests:1,budgetMs:100});
    assert.ok(nav.stats.frameExpanded<=4);
    assert.equal(low.phase,'start','lower priority work should wait for the combat route');
    route=nav.findPath(combat,goal,10,1);
  }
  assert.ok(route&&route.length>1);assert.equal(nav.stats.searches,1);
  const searches=nav.stats.searches;assert.equal(nav.findPath(combat,goal),route);assert.equal(nav.stats.searches,searches);
});

test('a shared corridor brings a horde around enclosure walls with exact local clearance',()=>{
  const c=engine(),w=empty(c);
  for(const o of [{id:'back',x:170,y:0,w:20,h:320},{id:'top',x:60,y:-160,w:240,h:20},{id:'bottom',x:60,y:160,w:240,h:20}])add(w,{...o,r:10,collision:'rect'});
  const nav=new c.ATSNavigation(w),actors=Array.from({length:60},(_,i)=>({id:'ape-'+i,x:90+(i%6)*2,y:(i-30)*2,navCohort:'rescue'})),goal={x:300,y:0};
  for(let tick=0;tick<30*60;tick++){
    nav.beginFrame(tick/60);
    if(tick%20===0)nav.setCohortRoute('rescue',actors[0],goal);
    for(const a of actors){nav.move(a,goal.x-a.x,goal.y-a.y,100,1/60);assert.equal(w.blocked(a.x,a.y,10),false)}
  }
  assert.equal(actors.filter(a=>Math.hypot(a.x-goal.x,a.y-goal.y)<25).length,60);
  assert.ok(nav.stats.sharedHits>100);assert.ok(nav.stats.searches<20,'nearby followers should share the broad route');
});

test('streaming preserves dense deterministic chunks and never exposes unfinished collisions',()=>{
  const c=engine(),reference=new c.ATSWorld('staged-world'),streamed=new c.ATSWorld('staged-world');
  reference.ensure(0,0,1400);
  streamed.stream(0,0,1400,{budgetMs:100,maxSteps:1});
  assert.equal(streamed.stats.streamSteps,1);
  assert.equal(streamed.chunks.size,0);assert.equal(streamed.objects.size,0);assert.equal(streamed.sites.size,0);
  assert.equal(streamed.blocked(0,0,12),true);
  assert.equal(streamed.chunks.size,0,'collision must not drain the generation queue');
  for(let tick=0;tick<200&&streamed.stats.pendingChunks;tick++){
    streamed.stream(0,0,1400,{budgetMs:100,maxSteps:3});assert.ok(streamed.stats.streamSteps<=3);
  }
  const normalized=w=>JSON.stringify([...w.objects.entries()].sort((a,b)=>a[0].localeCompare(b[0])));
  assert.equal(normalized(streamed),normalized(reference));
  assert.equal(streamed.objects.size,reference.objects.size);assert.ok(streamed.objects.size>600);
  assert.equal(streamed.blocked(0,0,12),false);
  assert.equal(streamed.blocked(8000,8000,12),true);
});

test('save and reentry preserve wounds, destroyed structures and depleted food during streaming',()=>{
  const c=engine(),w=new c.ATSWorld('stream-save');w.ensure(0,0,1400);
  const cage=w.objects.get('opening-rescue:cage:0'),berry=w.objects.get('opening-berries');
  cage.hp=0;cage.dead=true;cage.solid=false;berry.food=7;w.sites.get('opening-hunters').strength=3;w.navRevision++;
  w.stream(4500,0,1200,{budgetMs:100,maxSteps:1});
  const loaded=c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(w.serialize())));
  loaded.ensure(4500,0,1200);loaded.ensure(0,0,1400);
  assert.equal(loaded.objects.get(cage.id).dead,true);assert.equal(loaded.objects.get(cage.id).hp,0);
  assert.equal(loaded.objects.get(berry.id).food,7);assert.equal(loaded.sites.get('opening-hunters').strength,3);
  assert.equal(loaded.blocked(cage.x,cage.y,10),false);
  assert.equal(new Set([...loaded.objects.keys()]).size,loaded.objects.size);
  assert.ok(!JSON.stringify(loaded.serialize()).includes('_atsQueryStamp'));
});

test('spatial object queries retain non-solid resources and return overlapping structures once',()=>{
  const c=engine(),w=empty(c);add(w,{id:'food',x:120,y:0,r:15,solid:false,type:'berry'});add(w,{id:'wall',x:230,y:0,r:12,w:300,h:20,collision:'rect'});
  const found=w.getObjects(0,0,120);assert.equal(found.filter(o=>o.id==='food').length,1);assert.equal(found.filter(o=>o.id==='wall').length,1);
  assert.equal(w.blocked(120,0,10),true);w.objects.get('wall').dead=true;w.objects.get('wall').solid=false;w.navRevision++;
  assert.equal(w.blocked(120,0,10),false);
});

test('cold chunks discard reproducible geometry and preserve every changed scenery and site state',()=>{
  const c=engine(),w=new c.ATSWorld('cold-forest');w.ensure(0,0,1400);
  const count=w.objects.size,tree=[...w.objects.values()].find(o=>o.type==='tree'),berry=w.objects.get('opening-berries'),cage=w.objects.get('opening-rescue:cage:0');
  tree.hp=27;tree.angle=.123;tree.x+=1;berry.food=3;cage.dead=true;cage.solid=false;cage.hp=0;
  const site=w.sites.get('opening-hunters');site.strength=2;site.sleepingHumans=[{id:'guard-saved',x:560,y:120,hp:19}];
  const expected=JSON.stringify({tree,berry,cage,site});w.reveal(0,0,300);const discovery=w.discovered.size;
  while(w.trimDistant(10000,0,[],6500,6));
  assert.equal(w.chunks.size,0);assert.ok(w.objects.size<count/3);assert.equal(w.objects.get(cage.id),cage);
  assert.ok(w._coldChunks.size>0);assert.equal(w._spatial.size,0);
  const loaded=c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(w.serialize())));
  assert.equal(loaded._spatial.size,0);assert.equal(loaded.objects.has(tree.id),false);
  for(let tick=0;tick<200;tick++){loaded.stream(0,0,1400,{budgetMs:100,maxSteps:3});if(!loaded.stats.pendingChunks)break}
  const actual=JSON.stringify({tree:loaded.objects.get(tree.id),berry:loaded.objects.get(berry.id),cage:loaded.objects.get(cage.id),site:loaded.sites.get(site.id)});
  assert.equal(actual,expected);assert.equal(loaded.objects.size,count);assert.equal(loaded.discovered.size,discovery);
  assert.equal(loaded._coldChunks.size,0);
});

test('pinned settlements and distant detailed actors keep collision-ready chunks',()=>{
  const c=engine(),w=new c.ATSWorld('cold-pins');w.ensure(0,0,1400);const count=w.chunks.size;
  assert.equal(w.trimDistant(10000,0,[{x:0,y:0}],6500,100),0);assert.equal(w.chunks.size,count);
});
