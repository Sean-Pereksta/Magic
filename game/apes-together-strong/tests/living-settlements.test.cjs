'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');

function colony(n=30){
 const c=loadEngine(),g=new c.ATSGame('living-kingdom');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};
 g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.blocked=()=>false;g.world.lineClear=()=>true;
 g.world.getObjects=(x,y,r)=>[...g.world.objects.values()].filter(o=>Math.hypot(o.x-x,o.y-y)<=r);
 g.world.settlementPlot=(x,y,r)=>({valid:true,trees:g.world.getObjects(x,y,r).filter(o=>o.type==='tree'&&!o.dead),blocked:[]});
 g.world.syncSettlementBuildings=()=>{};
 g.world.clearTree=(tree)=>{if(tree.dead)return 0;tree.dead=true;tree.hp=0;tree.solid=false;return 9};
 g.world.addFortification=o=>{const b={...o,id:o.id,team:'ape',fortification:true,solid:true,dead:false};g.world.objects.set(b.id,b);return b};
 const s={id:'home',name:'Moonroot',x:0,y:0,population:n,level:1,radius:100,food:n*10,age:0,livingFounding:true,birthTimer:-100000};
 g.settlements.push(s);for(let i=0;i<n;i++)g.makeApe(0,0,'settled',s.id);g.syncIndexes();g.colonies.init(s);
 Object.assign(s,{food:n*10,wood:180,suitability:{fertility:1,wood:24,capacity:1000,water:true}});
 return {c,g,s};
}
function tick(g,s,n=1,workers=true){for(let i=0;i<n;i++){g.time++;g.refreshSettlements();g.colonies.tick(s);if(workers)for(const a of g.settlementMembers.get(s.id)||[]){const p=g.colonies.activityTarget(a,s);a.x=p.x;a.y=p.y}}}

test('fresh camps expand gradually while legacy footprints and housing survive migration',()=>{
 const {g,s}=colony(500);assert.equal(s.housing,6);assert.equal(s.huts.length,0);const before=s.radius,target=g.colonies.targetFootprint(s);assert.ok(target>=500&&target<=650);
 tick(g,s);assert.ok(s.radius>before&&s.radius<before+4);assert.ok(s.radius<target);const radius=s.radius;g.refreshSettlements();assert.equal(s.radius,radius,'population refresh cannot claim the whole region');
 for(const [pop,expected]of [[10,100],[50,180],[100,250],[200,340],[400,450],[600,550],[1000,650]]){s.population=pop;s.level=1;s.structures=[];assert.equal(g.colonies.targetFootprint(s),expected)}
 const legacy={id:'old',name:'Old home',x:1200,y:0,population:24,level:3,economyVersion:2,housing:54,wood:19,food:37,suitability:s.suitability};
 g.colonies.init(legacy);assert.equal(legacy.housing,54);assert.equal(legacy.huts.length,4);assert.equal(legacy.food,37);assert.equal(legacy.wood,19);g.colonies.damageHut(legacy,legacy.huts[0],100);g.colonies.init(legacy);assert.equal(legacy.huts[0].hp,0);assert.equal(legacy.housing,42);
});

test('individual homes keep persistent construction stages and only completed homes provide housing',()=>{
 const {g,s}=colony(6);tick(g,s);assert.equal(s.huts.length,1);const hut=s.huts[0];assert.equal(hut.capacity,10);assert.equal(hut.stage,0);assert.equal(hut.hp,0);assert.equal(s.housing,6);
 const stages=new Set([hut.stage]);for(let i=0;i<36;i++){tick(g,s);stages.add(hut.stage)}assert.ok(stages.has(1)&&stages.has(2)&&stages.has(3)&&stages.has(4));assert.equal(hut.hp,100);assert.equal(s.housing,16);assert.ok(s.paths.some(p=>p.points.at(-1).x===hut.x&&p.points.at(-1).y===hut.y));
});

test('visible construction waits for a builder and records delivered material trips',()=>{
 const {g,s}=colony(6);tick(g,s,12,false);const p=s.projects.find(p=>p.kind==='hut'),hut=s.huts.find(h=>h.id===p.structureId);assert.equal(p.work,0);assert.equal(hut.stage,0,'timber alone cannot animate an unattended work site');
 const builder=g.apes.find(a=>a.job==='builder');builder.x=p.x+20;builder.y=p.y;builder.workTargetId=p.id;builder.carrying='wood';tick(g,s,1,false);assert.ok(p.work>0);assert.equal(p.deliveries,1);assert.equal(builder.activity,'building');assert.equal(builder.workAnimationAt,g.time);assert.equal(builder.carrying,false);
 const work=p.work;builder.blastReaction={stage:'gettingUp',recoverAt:g.time+10};tick(g,s,1,false);assert.equal(p.work,work,'a knocked-down builder cannot work during recovery');delete builder.blastReaction;
});

test('construction must clear actual timber with nearby workers and cannot grant wood twice',()=>{
 const {g,s}=colony(18);s.wood=0;const plot=g.colonies.plot(s,'hut',1),tree={id:'claimed-trunk',type:'tree',x:plot.x,y:plot.y,hp:80,solid:true,dead:false},shade={id:'shade-tree',type:'tree',x:plot.x+65,y:plot.y+35,hp:80,solid:true,dead:false};
 g.world.objects.set(tree.id,tree);g.world.objects.set(shade.id,shade);const p=g.colonies.queue(s,'hut');p.treeIds=[tree.id];const hut=s.huts.find(h=>h.id===p.structureId);tick(g,s,12,false);assert.equal(tree.dead,false,'economy cannot silently clear a visible plot without a worker');assert.equal(hut.stage,0);assert.equal(s.wood,0);
 const worker=g.apes.find(a=>a.job==='builder');worker.x=tree.x+35;worker.y=tree.y;tick(g,s,8);assert.equal(tree.dead,true);assert.ok(s.clearedTrees>=1);assert.ok(s.wood>0,'actual clearing supplies building timber');const after=s.wood;assert.equal(g.world.clearTree(tree),0);assert.equal(s.wood,after);assert.equal(shade.dead,false,'unclaimed shade is preserved');tick(g,s,20,true);assert.equal(hut.stage,4);assert.equal(hut.hp,100);
});

test('trees and unfinished homes keep their work, resources and occupied capacity through a save',()=>{
 const {c,g,s}=colony(18);tick(g,s,3);const p=s.projects.find(p=>p.kind==='hut');assert.ok(p);const saved=JSON.parse(JSON.stringify(g.serialize())),loaded=c.ATSGame.fromJSON(saved),home=loaded.settlements[0];
 assert.equal(home.housing,s.housing);assert.equal(home.huts[0].stage,s.huts[0].stage);assert.equal(home.projects[0].work,p.work);assert.equal(home.wood,s.wood);assert.equal(home.developedRadius,s.developedRadius);
 loaded.colonies.init(home);assert.equal(home.projects.filter(p=>p.kind==='hut').length,s.projects.filter(p=>p.kind==='hut').length,'loading does not duplicate building jobs');
});

test('a developed 500-resident village builds dozens of distinct ten-ape huts',()=>{
 const {g,s}=colony(500);s.developedRadius=s.radius=550;s.level=10;s.gardens=50;s.food=5000;g.colonies.layout(s);
 for(let i=0;i<260;i++){s.wood=400;tick(g,s)}const completed=s.huts.filter(h=>h.stage===4&&h.hp>0);assert.ok(completed.length>=50,`${completed.length} occupied huts`);assert.ok(completed.length<=60);assert.equal(new Set(completed.map(h=>h.id)).size,completed.length);assert.ok(completed.every(h=>h.capacity===10));assert.ok(completed.some(h=>Math.hypot(h.x,h.y)>300));assert.ok(s.paths.length>=completed.length);
});

test('work cohorts share projects and jobs choose appropriate resource and social zones',()=>{
 const {g,s}=colony(300);tick(g,s,2);assert.ok(s.cohorts.length<30);assert.ok(s.cohorts.every(c=>c.count<=20));for(const job of ['forager','builder','lumber','gardener','cook','hauler','caretaker','guardian'])assert.ok(g.apes.some(a=>a.job===job),job);
 const cook=g.apes.find(a=>a.job==='cook'),gardener=g.apes.find(a=>a.job==='gardener'),forager=g.apes.find(a=>a.job==='forager');assert.equal(g.colonies.activityTarget(cook,s).activity,'cooking');assert.equal(g.colonies.activityTarget(gardener,s).activity,'gardening');let carried=false;for(let time=0;time<20;time++){g.time=time;carried=carried||g.colonies.activityTarget(forager,s).carrying==='food'}assert.ok(carried);
 g.time=2;const child=g.makeApe(s.radius,0,'young',s.id,true);const target=g.colonies.activityTarget(child,s);assert.ok(Math.hypot(target.x-s.x,target.y-s.y)<s.radius*.5);assert.equal(target.activity,'playing');
});

test('mature settlements prepare irregular ape barriers, lookouts and communal facilities',()=>{
 const {g,s}=colony(180);s.developedRadius=s.radius=400;s.level=10;s.gardens=18;s.housing=206;delete s.structuresVersion;g.colonies.init(s);g.colonies.layout(s);
 for(let i=0;i<160;i++){s.wood=400;tick(g,s)}assert.ok(s.cooking>0);assert.ok(s.stores>0);assert.ok(s.lookouts.length>0);assert.ok(s.barriers.length>=8);assert.ok(s.barriers.every(b=>b.team==='ape'&&b.settlementId===s.id));assert.ok(s.barriers.some(b=>b.weak));assert.equal(s.entrances.length,3);
 for(const b of s.barriers){const angle=Math.atan2(b.y-s.y,b.x-s.x);assert.ok(s.entrances.every(e=>Math.abs(Math.atan2(Math.sin(angle-e.angle),Math.cos(angle-e.angle)))>.18),'barriers leave deliberate entrances')}
});

test('an invasion recalls food workers, sends young to shelter and moves guards to entrances',()=>{
 const {g,s}=colony(180);tick(g,s);s.attack=true;const forager=g.apes.find(a=>a.job==='forager'),guard=g.apes.find(a=>a.job==='guardian'),builder=g.apes.find(a=>a.job==='builder'),child=g.makeApe(s.radius,0,'young',s.id,true);
 assert.equal(g.colonies.activityTarget(forager,s).activity,'returning home');assert.equal(g.colonies.activityTarget(guard,s).activity,'defending entrance');assert.equal(g.colonies.activityTarget(builder,s).activity,'repairing defenses');const p=g.colonies.activityTarget(child,s);assert.equal(p.activity,'sheltering');assert.ok(Math.hypot(p.x-s.x,p.y-s.y)<s.radius*.4);
});

test('distant villages keep aggregate economy without per-resident planning or frequent world scans',()=>{
 const {g,s}=colony(500);g.king.x=5000;s.level=10;s.gardens=50;s.developedRadius=s.radius=550;let scans=0;const query=g.world.getObjects;g.world.getObjects=(...args)=>{if(args[2]>100)scans++;return query(...args)};tick(g,s);const first=scans,cohortCount=s.cohorts.length;scans=0;tick(g,s,19);assert.equal(s.simLOD,2);assert.equal(first,1);assert.equal(scans,0,'far resource scans run every thirty seconds');assert.equal(s.cohorts.length,cohortCount);assert.ok(s.food>0);assert.ok(s.huts.some(h=>h.stage===4),'aggregate construction continues out of view');
});

test('real actor navigation brings a wood crew to a trunk before occupied construction',()=>{
 const c=loadEngine(),g=new c.ATSGame('wood-trip');g.spawnSites=()=>{};g.world.getSites=()=>[];g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;
 for(let i=0;i<30;i++){const p=g.findOpen(-25+i%6*10,-15+Math.floor(i/6)*10,10);g.makeApe(p.x,p.y,'follow')}g.food=1000;assert.equal(g.command('settleAll'),true);const s=g.settlements[0];s.wood=0;s.food=1000;s.birthTimer=-100000;
 const plot=g.colonies.plot(s,'hut',0),tree={id:'work-trunk',type:'tree',x:plot.x,y:plot.y,r:10,moveRadius:10,hp:80,maxHp:80,solid:true,size:1,height:90};g.world.objects.set(tree.id,tree);g.world._indexObject(tree);
 const p=g.colonies.queue(s,'hut');assert.ok(p.treeIds.includes(tree.id));const hut=s.huts.find(h=>h.id===p.structureId);assert.equal(hut.hp,0);assert.equal(tree.dead,undefined);
 for(let i=0;i<400;i++)g.update(.2,{});assert.equal(tree.dead,true);assert.equal(tree.woodClaimed,true);assert.equal(hut.stage,4);assert.equal(hut.hp,100);assert.ok(s.clearedTrees>0);assert.ok(g.apes.some(a=>Math.hypot(a.x,a.y)>35),'workers physically leave the central shelter');
 const collision=g.world.objects.get(hut.id+':collision');assert.ok(collision?.solid,'the finished hut occupies world space');let firing;
 for(let i=0;i<32&&!firing;i++){const angle=i*Math.PI/16,x=hut.x+Math.cos(angle)*55,y=hut.y+Math.sin(angle)*55;if(g.world.lineClear(x,y,hut.x+Math.cos(angle)*28,hut.y+Math.sin(angle)*28)&&s.huts.every(h=>h===hut||h.hp<=0||Math.hypot(h.x-x,h.y-y)>55))firing={x,y}}
 assert.ok(firing,'there is an exposed near face');const raider=g.makeHuman(firing.x,firing.y,null);raider.state='search';raider.raidTarget=s.id;g.colonies.siege(s,[raider]);assert.ok(hut.hp<100,'a hut does not block a shot at its own near face');
});

test('completed homes free their occupants before activating collision and restored homes remain safe',()=>{
 const c=loadEngine(),g=new c.ATSGame('wood-trip');g.spawnSites=()=>{};g.world.getSites=()=>[];g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;
 for(let i=0;i<30;i++){const p=g.findOpen(-25+i%6*10,-15+Math.floor(i/6)*10,10);g.makeApe(p.x,p.y,'follow')}g.food=1000;assert.equal(g.command('settleAll'),true);const s=g.settlements[0];s.food=1000;s.wood=200;s.birthTimer=-100000;
 const sync=g.world.syncSettlementBuildings.bind(g.world),evacuated=[];let home;
 g.world.syncSettlementBuildings=(settlement,game)=>{
  if(evacuated.length)return sync(settlement,game);
  const hut=settlement.huts.find(h=>h.hp>0&&h.stage===4&&!g.world.objects.has(h.id+':collision'));
  const inside=hut?g.apes.filter(a=>g.world._touches({x:hut.x,y:hut.y,collision:'rect',w:40,h:34},a.x,a.y,10)):[];
  const before=inside.map(a=>({a,x:a.x,y:a.y}));sync(settlement,game);
  if(!inside.length)return;home=hut;
  for(const p of before){assert.ok(Math.hypot(p.a.x-p.x,p.a.y-p.y)>0,'a resident inside the new footprint is moved to its edge');assert.equal(g.world.actorBlocked(p.a.x,p.a.y,10,'ape'),false,'the new position clears buildings, terrain and trees');evacuated.push({a:p.a,x:p.a.x,y:p.a.y})}
 };
 for(let i=0;i<150;i++)g.update(.2,{});
 assert.ok(evacuated.some(p=>p.a.job==='builder'),'the real construction crew occupies its unfinished work site');
 assert.ok(evacuated.some(p=>Math.hypot(p.a.x-p.x,p.a.y-p.y)>10),'evacuated residents resume ordinary movement');
 const saved=JSON.parse(JSON.stringify(g.serialize())),savedResident=saved.apes.find(a=>a.id===evacuated[0].a.id);savedResident.x=home.x;savedResident.y=home.y;
 const loaded=c.ATSGame.fromJSON(saved),resident=loaded.apes.find(a=>a.id===savedResident.id);assert.equal(loaded.world.actorBlocked(resident.x,resident.y,10,'ape'),false,'loading an older overlap also puts the resident outside the occupied home');
});

test('blast damage reaches preserved homes beyond the new development limit',()=>{
 const {g,s}=colony(18);s.huts=[{id:'legacy-outer-home',slot:0,x:800,y:0,hp:100,maxHp:100,capacity:12,stage:4,progress:1}];g.colonies.init(s);s.developedRadius=s.radius=650;
 assert.equal(g.colonies.builtExtent(s),838);assert.equal(g.colonies.builtExtent(s,Infinity),838);const housing=s.housing;
 g.colonies.damageNearby(800,0,30,100,{type:'shell'});assert.equal(s.huts[0].hp,0);assert.equal(s.housing,housing-12);assert.equal(s.developedRadius,650);assert.equal(g.colonies.targetFootprint(s)<=650,true);
});
