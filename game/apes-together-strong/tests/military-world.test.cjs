const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
function engine(){const c=vm.createContext({console,Math,Map,Set});c.window=c;for(const name of ['world','navigation'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),c);return c}
function empty(c){const w=new c.ATSWorld('military-navigation');w.ensure=()=>{};w.terrain=()=>({biome:'farmland',water:false,road:false});return w}
function add(w,o){o={hp:100,solid:true,dead:false,...o};w.objects.set(o.id,o);w._indexObject(o);return o}
function route(nav,from,to,r,profile){let result=null;nav.beginFrame(0);for(let tick=1;tick<1800&&result===null;tick++){result=nav.findPath(from,to,r,1,profile);nav.beginFrame(tick/60,{maxExpanded:12,budgetMs:100});assert.ok(nav.stats.frameExpanded<=12)}return result}

test('armor clearance differs from infantry trunks and never sweeps through major obstacles',()=>{
  const c=engine(),w=empty(c),nav=new c.ATSNavigation(w);
  add(w,{id:'north-tree',type:'tree',x:0,y:35,r:24,moveRadius:8,size:1.4,height:105});
  add(w,{id:'south-tree',type:'tree',x:0,y:-35,r:24,moveRadius:8,size:1.4,height:105});
  assert.equal(nav.clearSegment(-80,0,80,0,10),true);
  for(const kind of ['truck','apc','ifv','tank'])assert.equal(nav.clearSegment(-80,0,80,0,w.vehicleRadius(kind),kind),false);
  assert.equal(w.vehicleBlocked(0,0,31,'tank'),true);
  const tank={id:'vehicle-tank',x:-80,y:0,vehicleClass:'tank'};
  for(let i=0;i<180;i++)nav.move(tank,150,0,50,1/60,true);
  assert.ok(tank.x<0,'controlled hull movement must retain the same armor clearance');
  assert.equal(w.vehicleBlocked(tank.x,tank.y,31,'tank'),false);
  add(w,{id:'building',type:'house',x:140,y:0,w:70,h:90,r:35,collision:'rect'});
  assert.equal(nav.clearSegment(80,0,200,0,31,'tank'),false);
  add(w,{id:'boulder',type:'rock',x:140,y:170,r:32});
  assert.equal(nav.clearSegment(80,170,200,170,31,'tank'),false);
});

test('only tanks may crush very small vegetation; water and wetland remain restrictions',()=>{
  const c=engine(),w=empty(c),nav=new c.ATSNavigation(w);
  add(w,{id:'sapling',type:'tree',x:0,y:0,r:11,moveRadius:5,size:.75,height:70});
  assert.equal(nav.clearSegment(-80,0,80,0,31,'tank'),true);
  assert.equal(nav.clearSegment(-80,0,80,0,25,'apc'),false);
  w.terrain=(x,y)=>({biome:'wetland',water:x>100&&x<200,road:false});
  assert.equal(w.vehicleBlocked(90,200,31,'tank'),true);
  assert.equal(w.vehicleBlocked(280,200,24,'truck'),true);
  assert.equal(nav.clearSegment(60,200,250,200,31,'tank'),false);
  w.terrain=(x,y)=>({biome:'wetland',water:x>100&&x<200&&Math.abs(y-230)>40,road:Math.abs(y-230)<40});
  assert.equal(nav.clearSegment(60,230,250,230,31,'tank'),true);
});

test('queued armor paths prefer a road detour and smoothing preserves that preference',()=>{
  const c=engine(),w=empty(c),nav=new c.ATSNavigation(w);
  w.terrain=(x,y)=>({biome:'forest',water:false,road:Math.abs(x)<20||Math.abs(x-280)<20||Math.abs(y-112)<20});
  const from={x:0,y:0},to={x:280,y:0},path=route(nav,from,to,31,'tank');
  assert.ok(path?.length>2);
  assert.ok(path.some(p=>p.y>=84),'road preference should survive path smoothing');
  assert.equal(nav.preferredSegment(0,0,280,0,31,'tank'),false);
  const infantry=route(nav,from,to,31,'');
  assert.ok(infantry.every(p=>Math.abs(p.y)<56),'vehicle and infantry route caches must be separate');
  const truck={id:'vehicle-truck',x:0,y:0,kind:'truck'};
  let usedRoad=false;
  for(let tick=0;tick<1100;tick++){nav.beginFrame(tick/60,{budgetMs:100});nav.move(truck,to.x-truck.x,to.y-truck.y,60,1/60);usedRoad ||= truck.y>75}
  assert.ok(usedRoad);assert.ok(Math.hypot(truck.x-to.x,truck.y-to.y)<25);
});

test('deterministic regional military bases contain finite supplies, captivity and road staging',()=>{
  const c=engine(),w=new c.ATSWorld('military-bases'),types=new Map();
  for(let cy=-24;cy<=24;cy++)for(let cx=-24;cx<=24;cx++){
    const plan=w._sitePlan(cx,cy);if(plan?.military&&!types.has(plan.type))types.set(plan.type,{plan,cx,cy});
  }
  assert.equal(types.size,3);
  for(const [type,{plan,cx,cy}] of types){
    w._generateChunk(cx,cy);const site=w.sites.get(plan.id),capacity=w.militaryCapacity(site);
    assert.ok(site.count>=30);assert.ok(site.guards>=(type==='regionalCommand'?82:type==='armoredDepot'?44:30));
    if(type==='forwardBase')assert.ok(site.guards<=50);
    assert.ok(site.staging.length);assert.ok(site.roadblocks.length);
    for(const p of site.roadblocks)assert.equal(w.terrain(p.x,p.y).road,true);
    assert.ok(capacity.inventory.apc>0);assert.ok(capacity.inventory.truck>0);assert.ok(capacity.armorCapacity>0);
    const objects=site.objects.map(id=>w.objects.get(id));
    for(const infrastructure of ['barracks','radio','depot','fuel','tower','cage'])assert.ok(objects.some(o=>o.type===infrastructure));
    assert.equal(objects.filter(o=>o.type==='cage').reduce((sum,o)=>sum+o.prisoners,0),site.count);
    assert.ok(objects.some(o=>o.type==='vehicle'&&o.vehicleType==='apc'));
    if(type!=='forwardBase')assert.ok(objects.some(o=>o.type==='vehicle'&&o.vehicleType==='tank'));
    if(type==='regionalCommand')assert.ok(objects.some(o=>o.commandCenter));
    assert.ok(site.radius>=(type==='regionalCommand'?690:type==='armoredDepot'?540:420));
    assert.ok(objects.some(o=>o.barricade&&o.defenseRing===2));
    assert.ok(objects.filter(o=>o.type==='tower').length>=6);
    assert.ok(objects.some(o=>o.type==='gate'&&o.w>=140));
    const road=w._roadInfo(site.x,site.y);
    assert.ok(road.dx>site.footprintX+65&&road.dy>site.footprintY+65,'large perimeters must leave main roads open');
    for(const kind of ['truck','apc','tank'])for(let index=0;index<3;index++){
      const p=w.vehicleStaging(site,kind,index);assert.ok(p,'military installations must have hull-clear launch positions');
      assert.equal(w.terrain(p.x,p.y).road,true);assert.equal(w.vehicleBlocked(p.x,p.y,w.vehicleRadius(kind),kind),false);
    }
  }
  const reference=new c.ATSWorld('military-bases');
  for(const {plan,cx,cy} of types.values())assert.equal(JSON.stringify(reference._sitePlan(cx,cy)),JSON.stringify(plan));
});

test('infrastructure destruction reduces supplies and spent inventory survives legacy-compatible loading',()=>{
  const c=engine(),w=empty(c),site={id:'legacy-depot',x:0,y:0,tier:5,type:'experimental',objects:[],strength:50};
  w.sites.set(site.id,site);
  for(const type of ['radio','depot','fuel','barracks']){const o=add(w,{id:type,type,x:0,y:0,r:12});site.objects.push(o.id)}
  const first=w.militaryCapacity(site);
  assert.equal(first.inventory.tank,1);assert.equal(first.inventory.armored,2);assert.equal(first.coordination,1);assert.equal(first.fuelFactor,1);
  site.radioDown=true;assert.equal(w.militaryCapacity(site).radio,false);site.radioDown=false;
  site.vehicleInventory.tank--;site.armorCapacity-=12;
  for(const type of ['radio','depot','fuel','barracks']){w.objects.get(type).dead=true;w.objects.get(type).hp=0}
  const disabled=w.militaryCapacity(site);
  assert.ok(disabled.coordination<1);assert.ok(disabled.armorFactor<1);assert.ok(disabled.fuelFactor<1);assert.ok(disabled.infantryFactor<1);
  assert.equal(disabled.inventory.tank,0);
  const loaded=c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(w.serialize()))),saved=loaded.militaryCapacity(loaded.sites.get(site.id));
  assert.equal(saved.inventory.tank,0);assert.equal(saved.armorCapacity,site.armorCapacity);assert.equal(saved.depot,false);assert.equal(saved.radio,false);
});

test('distant operations request a bounded stream corridor without draining generation synchronously',()=>{
  const c=engine(),w=new c.ATSWorld('remote-column');w.ensure(0,0,800);w.stream(0,0,800,{maxSteps:3,budgetMs:100});
  const from={x:5000,y:120},to={x:560,y:120},before=w.chunks.size;
  w.requestCorridor(from,to,{id:'column-one',profile:'tank'});
  assert.equal(w.chunks.size,before);assert.equal(w.boundsReady(from.x-31,from.y-31,from.x+31,from.y+31),false);
  assert.ok(w._corridorRequests.get('column-one').chunks.size<=96);
  let ready=false;
  for(let tick=0;tick<420;tick++){
    if(tick%30===0)w.requestCorridor(from,to,{id:'column-one',profile:'tank'});
    w.stream(0,0,800,{maxSteps:3,budgetMs:100});assert.ok(w.stats.streamSteps<=3);
    ready=w.boundsReady(from.x-31,from.y-31,from.x+31,from.y+31)&&w.boundsReady(to.x-31,to.y-31,to.x+31,to.y+31);
    if(ready&&!w.stats.pendingChunks)break;
  }
  assert.equal(ready,true,'source installation geometry must load even while the king stays far away');
  assert.ok(w.chunks.size>before);assert.ok(w.chunks.has('6,0'));
  const nav=new c.ATSNavigation(w),vehicle={id:'vehicle-remote',x:from.x,y:from.y,vehicleClass:'tank',operationId:'column-two'};
  nav.beginFrame(0);nav.steer(vehicle,to,31,.5);
  assert.ok(w._corridorRequests.has('column-two:tank'),'vehicle routes must keep their travel corridor alive');
});

test('a real tank travels from an unloaded distant road source using bounded strategic route segments',()=>{
  const c=engine(),w=new c.ATSWorld('remote-journey');w.ensure(0,0,1200);w.stream(0,0,1200,{maxSteps:3,budgetMs:100});
  const nav=new c.ATSNavigation(w),target={x:560,y:120},tank={id:'vehicle-source',vehicleClass:'tank',operationId:'remote',x:w._roadInfo(560,-2600).x,y:-2600};
  const initial=Math.hypot(tank.x-target.x,tank.y-target.y);
  for(let tick=0;tick<4200;tick++){
    nav.beginFrame(tick/60,{maxExpanded:64,budgetMs:100});assert.ok(nav.stats.frameExpanded<=64);
    w.stream(0,0,1200,{maxSteps:3,budgetMs:100});assert.ok(w.stats.streamSteps<=3);
    if(tick%30===0){const point=nav.steer(tank,target,31,.5);nav.move(tank,point.x-tank.x,point.y-tank.y,50,.5,true);if(tank.moving)assert.equal(w.vehicleBlocked(tank.x,tank.y,31,'tank'),false)}
  }
  assert.ok(initial>2500);assert.ok(Math.hypot(tank.x-target.x,tank.y-target.y)<80);
  assert.ok(nav.stats.searches<=8);assert.equal(nav.stats.failures,0);
});
