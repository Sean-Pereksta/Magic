'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function setup(n=40){
 const c=loadEngine(),g=new c.ATSGame('expedition-regression'),w=new c.ATSWorld('expedition-empty');
 w.ensure=()=>{};w.terrain=()=>({biome:'forest',water:false,road:false});w.getSites=()=>[];w.settlementPlot=()=>({valid:true,trees:[]});g.world=w;g.navigation=new c.ATSNavigation(w);
 const s={id:'expedition-home',name:'Home',x:0,y:0,population:n,food:0,wood:0,housing:n+20,level:2,birthTimer:-1000};g.settlements.push(s);
 for(let i=0;i<n;i++){const a=g.makeApe(-30-i%4*7,(Math.floor(i/4)-n/8)*9,'settled',s.id);a.job=i<8?'guardian':'forager';a.speed=95}
 g.refreshSettlements();g.colonies.init(s);w.objects.clear();w._spatial.clear();g.king.x=5000;g.king.y=5000;
 for(let i=0;i<8;i++)g.apes[i].job='guardian';s.nextKingdomScoutAt=1e8;return{g,c,s,w};
}
function add(w,id,type,x,y=0,quantity=80){const o={id,type,x,y,hp:type==='berry'?1:155,r:14,moveRadius:8,size:1,solid:type==='tree',food:quantity,count:quantity,dead:false};w.objects.set(id,o);w._indexObject(o);return o}
function configure(g,s,settings={foodRange:'extended'}){assert.equal(g.kingdom.configureExpeditions(s.id,settings).ok,true);g.kingdom.tick(s);return s.kingdomMissions.find(m=>m.kind==='expedition'&&m.status==='traveling')}
function actors(g,m){return m.members.map(id=>g.apesById.get(id))}
function advance(g,s,seconds,inspect=()=>{}){for(let i=0;i<Math.ceil(seconds/.25);i++){g.time+=.25;g.navigation.beginFrame(g.time,{budgetMs:Infinity});if(i%4===0)g.kingdom.tick(s);for(const a of g.apes)if(a.kingdomMission)g.abstractActor(a,.25,'ape');if(inspect()===false)break;}}
function arrive(g,s,m){for(const a of actors(g,m)){a.x=m.target.x;a.y=m.target.y}g.time++;g.navigation.beginFrame(g.time,{budgetMs:Infinity});g.kingdom.tick(s)}

test('independent range settings migrate, reject invalid choices and survive saves',()=>{
 const{g,c,s}=setup();assert.deepEqual(JSON.parse(JSON.stringify(s.expeditionSettings)),{foodRange:'local',woodRange:'local',priority:'balanced'});
 assert.equal(g.kingdom.configureExpeditions(s.id,{foodRange:'extended',woodRange:'frontier',priority:'wood'}).ok,true);
 assert.equal(g.kingdom.configureExpeditions(s.id,{foodRange:'world'}).ok,false);assert.equal(s.expeditionSettings.foodRange,'extended');
 const restored=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize())));assert.deepEqual(JSON.parse(JSON.stringify(restored.settlements[0].expeditionSettings)),{foodRange:'extended',woodRange:'frontier',priority:'wood'});
});

test('frontier discovers genuine distant resources; parties preserve defenders, gardeners and workshops',()=>{
 const{g,s,w}=setup();add(w,'distant-tree','tree',2300);for(const a of g.apes.slice(8,12))a.job='gardener';for(const a of g.apes.slice(12,15))a.job='workshop';
 configure(g,s,{woodRange:'extended'});assert.equal(s.kingdomMissions.length,0);
 const m=configure(g,s,{woodRange:'frontier'});assert.ok(m);assert.equal(m.target.resourceId,'distant-tree');assert.ok(m.members.length>=3&&m.members.length<=8);assert.equal(g.apes.length,40);
 assert.ok(g.apes.slice(0,15).every(a=>!a.kingdomMission));assert.equal(g.kingdom.expeditionStatus(s).parties,1);assert.equal(s.food,0);assert.equal(s.wood,0);
});

test('food priority selects food while shortages in the other resource still get later parties',()=>{
 const{g,s,w}=setup(100);add(w,'food','berry',1000);add(w,'wood','tree',-1000);const m=configure(g,s,{foodRange:'extended',woodRange:'extended',priority:'food'});assert.equal(m.resource,'food');g.time=9;g.kingdom.tick(s);assert.ok(s.kingdomMissions.some(m=>m.resource==='wood'));
});

test('expeditions reuse one cohort, navigate a river bridge, gather and physically return to deposit',()=>{
 const{g,s,w}=setup(24);w.terrain=(x,y)=>({biome:'forest',water:y>130&&y<200&&Math.abs(x-230)>42,road:Math.abs(x-230)<42});
 const berry=add(w,'across-river','berry',0,1000,80),m=configure(g,s);assert.ok(m);let crossed=false,carried=false,routeHits=0,lastFood=0;
 advance(g,s,110,()=>{for(const a of actors(g,m)){assert.equal(w.actorBlocked(a.x,a.y,10,'ape'),false);if(a.y>130&&a.y<200){assert.ok(Math.abs(a.x-230)<=42);crossed=true}if(a.expeditionCargo)carried=true}if(s.food>lastFood)assert.ok(actors(g,m).some(a=>Math.hypot(a.x-s.x,a.y-s.y)<=80));lastFood=s.food;routeHits=Math.max(routeHits,g.navigation.stats.sharedHits);if(m.status==='complete')return false;});
 assert.equal(m.status,'complete',JSON.stringify({m,positions:actors(g,m).map(a=>[a.x,a.y]),navigation:g.navigation.stats}));assert.ok(crossed);assert.ok(carried);assert.ok(routeHits>0);assert.ok(s.food>0);assert.ok(Math.abs(s.food-(80-berry.food))<1e-8);assert.ok(actors(g,m).every(a=>!a.kingdomMission&&!a.expeditionCargo));
});

test('gathered cargo persists across a return-trip save and credits each carrier once',()=>{
 const{g,c,s,w}=setup(24),berry=add(w,'saved-berries','berry',1100,0,36),m=configure(g,s);arrive(g,s,m);for(let i=0;i<12&&!m.returning;i++){g.time++;g.kingdom.tick(s)}assert.ok(m.returning);
 const harvested=36-berry.food;assert.ok(harvested>0);assert.equal(s.food,0);
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),home=loaded.settlement(s.id),saved=home.kingdomMissions.find(x=>x.id===m.id);home.nextExpeditionAt=1e8;
 loaded.kingdom.tick(home);assert.equal(home.food,0);for(const a of actors(loaded,saved)){a.x=home.x;a.y=home.y}loaded.time++;loaded.kingdom.tick(home);assert.equal(home.food,harvested);loaded.kingdom.tick(home);assert.equal(home.food,harvested);assert.equal(saved.status,'complete');
});

test('recall and recruitment preserve cargo but release mission reservations and assignments',()=>{
 for(const command of ['call','recallAll']){const{g,s,w}=setup(24);add(w,'recall-berries','berry',1100);const m=configure(g,s);arrive(g,s,m);const group=actors(g,m),carried=group.reduce((sum,a)=>sum+(a.expeditionCargo?.amount||0),0);assert.ok(carried>0);g.king.x=m.target.x;g.king.y=m.target.y;g.apeGrid.rebuild([g.king,...g.apes]);g.commandCD=0;assert.equal(g.command(command),true);g.kingdom.tick(s);
  assert.ok(group.every(a=>!a.kingdomMission));assert.equal(m.status,'cancelled');assert.equal(group.reduce((sum,a)=>sum+(a.expeditionCargo?.amount||0),0),carried);assert.equal(s.food,0);
  for(const a of group){a.x=s.x;a.y=s.y;g.kingdom.depositExpeditionCargo(a);g.kingdom.depositExpeditionCargo(a)}assert.equal(s.food,carried);
 }
});

test('resource reservations prevent competing villages from selecting the same site',()=>{
 const{g,s,w}=setup(40);add(w,'shared-berries','berry',1000);const m=configure(g,s);assert.ok(m);const other={...s,id:'other-home',x:100,kingdomMissions:[],projects:[],expeditionSettings:{foodRange:'extended',woodRange:'local',priority:'balanced'}};g.settlements.push(other);
 assert.equal(g.kingdom.discoverResource(other,'food'),null);add(w,'alternative-berries','berry',-900);assert.equal(g.kingdom.discoverResource(other,'food').resourceId,'alternative-berries');
});

test('exhausted destinations are replaced and known patrols are avoided',()=>{
 const{g,s,w}=setup();const first=add(w,'emptying','berry',900),safe=add(w,'safe','berry',1300),risky=add(w,'risky','berry',800,300);const human=g.makeHuman(risky.x,risky.y);g.humanGrid.rebuild(g.humans);const m=configure(g,s);assert.equal(m.target.resourceId,first.id);first.food=0;first.dead=true;g.time++;g.kingdom.tick(s);assert.equal(m.target.resourceId,safe.id);
 human.x=m.target.x;human.y=m.target.y;g.humanGrid.rebuild(g.humans);for(const a of actors(g,m)){a.x=m.target.x;a.y=m.target.y}g.time++;g.kingdom.tick(s);assert.equal(m.returning,true);assert.equal(m.phase,'threatened');assert.equal(s.food,0);
});

test('unreachable targets are abandoned after bounded progress retries and not immediately chosen again',()=>{
 const{g,s,w}=setup();add(w,'unreachable','berry',1200);const m=configure(g,s);g.time=25;g.kingdom.tick(s);assert.equal(m.returning,true);assert.ok(s.expeditionRejected.unreachable>g.time);assert.equal(g.kingdom.discoverResource(s,'food'),null);assert.equal(s.food,0);
});

test('timber comes from an actual tree once; carrier death never reallocates lost cargo',()=>{
 const{g,s,w}=setup(24),tree=add(w,'timber','tree',1100),m=configure(g,s,{woodRange:'extended'});arrive(g,s,m);g.time+=3;g.kingdom.tick(s);assert.equal(tree.dead,true);assert.equal(tree.woodClaimed,true);const group=actors(g,m),lost=group.find(a=>a.expeditionCargo);assert.ok(lost);const original=group.reduce((n,a)=>n+(a.expeditionCargo?.amount||0),0);lost.hp=0;g.time++;g.kingdom.tick(s);
 for(const a of group){a.x=s.x;a.y=s.y}g.time++;g.kingdom.tick(s);assert.equal(s.wood,original-lost.expeditionCargo.amount);assert.equal(w.clearTree(tree),0);
});

test('full storage retains real surplus on carriers instead of losing or duplicating it',()=>{
 const{g,s}=setup(),a=g.apes[12],cap=Math.max(s.housing,s.population)*10+80+(s.stores||0)*120+g.colonies.expansion(s)*800;s.food=cap-2;a.x=s.x;a.y=s.y;a.expeditionCargo={sourceId:s.id,resource:'food',amount:12};g.kingdom.depositExpeditionCargo(a);assert.equal(s.food,cap);assert.equal(a.expeditionCargo.amount,10);g.kingdom.depositExpeditionCargo(a);assert.equal(a.expeditionCargo.amount,10);s.food-=10;g.kingdom.depositExpeditionCargo(a);assert.equal(s.food,cap);assert.equal(a.expeditionCargo,undefined);
});

test('resource scoring honors known route length instead of straight-line proximity',()=>{
 const{g,s,w}=setup(),near=add(w,'near-long-detour','berry',900),far=add(w,'far-short-route','berry',1200);g.kingdom.configureExpeditions(s.id,{foodRange:'extended'});
 const nav=g.navigation,key=[Math.round(s.x/nav.cell),Math.round(s.y/nav.cell),Math.round(near.x/nav.cell),Math.round(near.y/nav.cell),10,nav.revision,'ape'].join(':');nav.routes.set(key,{time:g.time,path:[{x:0,y:800},{x:900,y:800},{x:900,y:0}]});
 assert.equal(g.kingdom.discoverResource(s,'food').resourceId,far.id);
});

test('the slowest offscreen cadence preserves elapsed travel time',()=>{
 const first=setup(24),second=setup(24);for(const {g,s,w}of [first,second]){add(w,'elapsed-trip','berry',1100);configure(g,s);g.navigation.beginFrame(1,{budgetMs:Infinity});g.time=1;g.kingdom.tick(s)}
 const a=actors(first.g,first.s.kingdomMissions[0])[0],b=actors(second.g,second.s.kingdomMissions[0])[0];first.g.abstractActor(a,.8,'ape');for(let i=0;i<4;i++)second.g.abstractActor(b,.2,'ape');assert.ok(Math.hypot(a.x-b.x,a.y-b.y)<1e-6);assert.ok(a.x>20);
});

test('all-idle depleted villages can release excess guards without losing their garrison',()=>{
 const{g,s,w}=setup();for(const a of g.apes)a.job='guardian';add(w,'recovery-wood','tree',2100);const m=configure(g,s,{woodRange:'frontier'});assert.ok(m);assert.ok(g.apes.filter(a=>a.job==='guardian'&&!a.kingdomMission).length>=8);
});

test('eight guarded candidates cannot permanently conceal a farther safe resource',()=>{
 const{g,s,w}=setup();for(let i=0;i<8;i++)add(w,'guarded-'+i,'berry',650+i*8);add(w,'safe-beyond-shortlist','berry',1400);g.makeHuman(700,0);g.humanGrid.rebuild(g.humans);g.kingdom.configureExpeditions(s.id,{foodRange:'extended'});
 assert.equal(g.kingdom.discoverResource(s,'food'),null);assert.equal(g.kingdom.discoverResource(s,'food').resourceId,'safe-beyond-shortlist');
 // Known fortified territory is filtered before the bounded patrol shortlist.
 delete s.expeditionSearchAfter;w.getSites=()=>[{id:'known-camp',x:700,y:0,radius:100,guards:20,cleared:false}];assert.equal(g.kingdom.discoverResource(s,'food').resourceId,'safe-beyond-shortlist');
});
