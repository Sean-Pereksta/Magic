'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');

function openWorld(g){
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.blocked=()=>false;g.world.lineClear=()=>true;g.siege.clearRay=()=>g.world.lineClear();
 g.world.getObjects=(x,y,r)=>[...g.world.objects.values()].filter(o=>Math.hypot(o.x-x,o.y-y)<=r);g.world.settlementPlot=()=>({valid:true,trees:[],blocked:[]});g.world.syncSettlementBuildings=()=>{};
 g.world.addFortification=o=>{const b={...o,team:'ape',fortification:true,solid:true,dead:false};g.world.objects.set(b.id,b);return b};
}
function colony(n=60){const c=loadEngine(),g=new c.ATSGame('COMMISSION-'+n);openWorld(g);const s={id:'village',name:'Willow Crown',x:0,y:0,population:n,level:6,radius:300,food:2000,wood:300,livingFounding:true,birthTimer:-100000};g.settlements.push(s);for(let i=0;i<n;i++)g.makeApe(0,0,'settled',s.id);g.syncIndexes();g.refreshSettlements();g.colonies.init(s);s.developedRadius=s.radius=300;s.suitability={fertility:1,capacity:1000,water:true};g.colonies.layout(s);g.colonies.assignJobs(s,g.apes);g.colonies.plan=()=>{};return{c,g,s}}
function advance(g,s,n=1,arrive=true){for(let i=0;i<n;i++){g.time++;g.colonies.tick(s);if(arrive)for(const a of g.apes){const target=g.colonies.activityTarget(a,s);a.x=target.x;a.y=target.y}}}
function finished(g,s,kind){const result=g.colonies.commission(s.id,kind);assert.equal(result.ok,true,result.reason);for(let i=0;i<120&&s.projects.some(p=>p.id===result.projectId);i++)advance(g,s);assert.ok(!s.projects.some(p=>p.id===result.projectId),'commission completes through worker progress');return s.facilities.find(f=>f.id===result.projectId+'-built')||s.structures.find(f=>f.id===result.projectId+'-built')||s.barriers.find(f=>f.id===result.projectId+'-barrier')}
function crew(g,s,tower){s._staffAt=0;g.colonies.staffFacilities(s);const a=g.apes.find(a=>a.id===tower.staffedBy);assert.ok(a&&a.state!=='young');Object.assign(a,tower.station);return a}
function shoot(g,s,tower,target){crew(g,s,tower);tower.shotAt=0;tower.acquireAt=0;g.humanGrid.rebuild(g.humans);g.vehicleGrid.rebuild(g.vehicles);g.colonies.updateDefenses(1/60);return s.spears[0]}
function fly(g,s,seconds){for(let i=0;i<Math.ceil(seconds*60);i++){g.time+=1/60;g.colonies.updateDefenses(1/60)}}

test('catalog, local lodge access and scarce supplies reject orders without mutations',()=>{
 const {g,s}=colony(18),catalog=g.colonies.catalog(s);assert.equal(catalog.length,16);assert.ok(catalog.every(c=>c.description&&c.cost&&c.totalWork>0&&c.limit>0));assert.equal(catalog.find(c=>c.kind==='spearTower').unlocked,false);assert.equal(g.colonies.commission(s.id,'spearTower').ok,false);assert.equal(s.projects.length,0);
 g.king.x=91;assert.equal(g.colonies.lodgeAt(g.king),null);assert.match(g.colonies.commission(s.id,'hut').reason,/Visit/);g.king.x=90;assert.equal(g.colonies.lodgeAt(g.king),s);g.king.x=0;s.wood=0;s.food=0;const before=JSON.stringify(s.commissions);assert.match(g.colonies.commission(s.id,'hut').reason,/timber/);assert.equal(JSON.stringify(s.commissions),before);assert.equal(g.colonies.commission(s.id,'unknown').ok,false);
 s.wood=100;g.world.terrain=()=>({water:true});const wood=s.wood;assert.equal(g.colonies.commission(s.id,'hut').ok,false);assert.equal(s.wood,wood);assert.equal(s.projects.length,0,'invalid terrain spends no supplies and queues no phantom job');
});

test('commissions deduct once, require an arriving crew and create staged real facilities',()=>{
 const {g,s}=colony(),cost=g.colonies.catalog(s).find(c=>c.kind==='spearTower').cost,wood=s.wood,food=s.food,result=g.colonies.commission(s.id,'spearTower'),p=s.projects.find(p=>p.id===result.projectId);assert.equal(result.ok,true);assert.equal(s.wood,wood-cost.wood);assert.equal(s.food,food-cost.food);assert.equal(p.timber,0);assert.equal(s.facilities.length,0);
 advance(g,s,8,false);assert.equal(p.work,0,'an unattended visible tower plot cannot progress');const stages=new Set([p.stage]);for(let i=0;i<100&&s.projects.includes(p);i++){advance(g,s);stages.add(p.stage)}assert.ok([1,2,3,4].every(stage=>stages.has(stage)));assert.equal(s.facilities[0].kind,'spearTower');assert.equal(s.facilities[0].hp,220);assert.equal(s.wood,wood-cost.wood,'completion cannot charge prepaid timber again');assert.equal(s.commissions[0].status,'complete');assert.equal(s.commissions[0].structureId,s.facilities[0].id);
});

test('attacks pause unfinished commissions and work resumes after the attack',()=>{
 const {g,s}=colony(),result=g.colonies.commission(s.id,'training'),p=s.projects.find(p=>p.id===result.projectId);advance(g,s,5);const before=p.work,wood=s.wood,raider=g.makeHuman(0,0,null);raider.state='combat';g.humanGrid.rebuild(g.humans);advance(g,s,15);assert.equal(s.attack,true);assert.equal(p.work,before);assert.equal(s.wood,wood);assert.equal(s.facilities.length,0);assert.equal(g.colonies.commission(s.id,'hut').ok,false);
 raider.hp=0;g.humanGrid.rebuild(g.humans);advance(g,s,100);assert.ok(s.facilities.some(f=>f.kind==='training'));assert.equal(s.commissions[0].status,'complete');
});

test('staffed towers launch visible traveling shafts before human or vehicle damage',()=>{
 const {g,s}=colony(),tower=finished(g,s,'spearTower'),human=g.makeHuman(tower.x+230,tower.y,null);human.state='combat';const hp=human.hp,spear=shoot(g,s,tower,human);assert.ok(spear&&spear.towerId===tower.id);assert.equal(spear.elapsed,0);assert.equal(human.hp,hp,'launching a spear does not hit remotely');assert.ok(spear.fromZ>0&&spear.duration>.4);fly(g,s,.1);assert.equal(human.hp,hp);assert.ok(s.spears[0].x>spear.fromX+32,'shaft advances physically');fly(g,s,.6);assert.ok(human.hp<hp);assert.ok(g.effects.some(e=>e.type==='spearImpact'));
 human.hp=0;const vehicle={id:'vehicle-'+g.nextId++,kind:'tank',x:tower.x+200,y:tower.y,dir:0,state:'raid'};g.forces.initVehicle(vehicle,'tank');g.vehicles.push(vehicle);g.vehicleGrid.rebuild(g.vehicles);const armorHp=vehicle.hp;assert.ok(shoot(g,s,tower,vehicle));assert.equal(vehicle.hp,armorHp);fly(g,s,.7);assert.ok(vehicle.hp<armorHp,'traveling spear collision also reaches armor');
});

test('walls, evasion, absent staff, children and knocked-down staff cannot cause remote hits',()=>{
 const {g,s}=colony(),tower=finished(g,s,'spearTower'),human=g.makeHuman(tower.x+220,tower.y,null),hp=human.hp,a=crew(g,s,tower);g.humanGrid.rebuild(g.humans);g.world.lineClear=()=>false;tower.shotAt=0;g.colonies.updateDefenses(1/60);assert.equal(s.spears.length,0);assert.equal(human.hp,hp);
 g.world.lineClear=()=>true;tower.acquireAt=0;Object.assign(a,{x:0,y:0});g.colonies.updateDefenses(1/60);assert.equal(s.spears.length,0);Object.assign(a,tower.station);a.state='young';g.colonies.updateDefenses(1/60);assert.equal(s.spears.length,0);a.state='settled';a.blastReaction={stage:'gettingUp'};g.colonies.updateDefenses(1/60);assert.equal(s.spears.length,0);delete a.blastReaction;
 assert.ok(shoot(g,s,tower,human));human.y+=100;g.humanGrid.rebuild(g.humans);fly(g,s,1.3);assert.equal(human.hp,hp,'moving away from the fixed aim point evades the shaft');assert.equal(s.spears.length,0);
 human.y=tower.y;assert.ok(shoot(g,s,tower,human));g.world.lineClear=()=>false;fly(g,s,.6);assert.equal(human.hp,hp,'cover blocks a spear already in flight');
});

test('training requires completed facilities, physical adult practice and finite food',()=>{
 const {g,s}=colony(),facility=finished(g,s,'training');s._staffAt=0;g.colonies.staffFacilities(s);const a=g.apes.find(a=>a.id===facility.trainingResidents[0]);assert.ok(a);a.hp=40;a.x=9000;a.y=0;a.trainingProgress=0;const wounded=a.hp;
 for(let i=0;i<100;i++)g.colonies.train(s);assert.equal(a.trainingLevel||0,0,'remote idle villagers are not trained');a.x=facility.x+38;a.y=facility.y+18;s.food=0;for(let i=0;i<100;i++)g.colonies.train(s);assert.equal(a.trainingLevel||0,0);assert.equal(a.trainingProgress,45,'scarcity preserves bounded practice progress');
 s.food=100;g.colonies.train(s);assert.equal(a.trainingLevel,1);assert.equal(a.hp,wounded,'a strength upgrade cannot heal accumulated wounds');const after=s.food;g.colonies.train(s);assert.equal(s.food,after,'the bonus is not repeatedly charged');for(let i=0;i<400;i++)g.colonies.train(s);assert.equal(a.trainingLevel,3);const capFood=s.food;for(let i=0;i<200;i++)g.colonies.train(s);assert.equal(a.trainingLevel,3);assert.equal(s.food,capFood);assert.equal(g.colonies.trainingBonus(a).maxHp,36);assert.equal(g.colonies.trainingBonus(a).damageMultiplier,1.24);
});

test('population scales available facilities and palisade length without instant land claims',()=>{
 const {g,s}=colony(40),small=g.colonies.catalog(s);s.population=1000;const large=g.colonies.catalog(s),before=s.developedRadius;assert.ok(large.find(c=>c.kind==='spearTower').limit>small.find(c=>c.kind==='spearTower').limit);assert.equal(large.find(c=>c.kind==='spearTower').limit,14);assert.equal(large.find(c=>c.kind==='training').limit,6);assert.equal(g.colonies.commissioningSlots(s).limit,12);g.colonies.expand(s);assert.ok(s.developedRadius>before&&s.developedRadius<before+4);assert.ok(s.targetRadius<=650);
 const result=g.colonies.commission(s.id,'defense'),p=s.projects.find(p=>p.id===result.projectId);assert.equal(result.ok,true);p.work=p.totalWork;g.colonies.complete(s,p);const b=s.barriers[0];assert.ok(b.width>=120);const angle=Math.atan2(b.y-s.y,b.x-s.x);assert.ok(s.entrances.every(e=>Math.abs(Math.atan2(Math.sin(angle-e.angle),Math.cos(angle-e.angle)))>=.19));
});

test('unfinished paid work, wounded facilities, practice and flying spears persist idempotently',()=>{
 const {c,g,s}=colony(),tower=finished(g,s,'spearTower');tower.hp=117;const human=g.makeHuman(tower.x+240,tower.y,null);shoot(g,s,tower,human);fly(g,s,.1);const result=g.colonies.commission(s.id,'training');advance(g,s,4);const p=s.projects.find(p=>p.id===result.projectId),a=g.apes[0];a.trainingLevel=1;a.trainingProgress=17;a.hp=43;s.lodge.hp=211;const saved=JSON.parse(JSON.stringify(g.serialize())),loaded=c.ATSGame.fromJSON(saved),home=loaded.settlements[0];openWorld(loaded);loaded.colonies.plan=()=>{};
 assert.equal(home.wood,s.wood);assert.equal(home.facilities[0].hp,117);assert.equal(home.lodge.hp,211);assert.equal(home.projects.find(p=>p.id===result.projectId).work,p.work);assert.equal(home.spears[0].elapsed,s.spears[0].elapsed);assert.equal(loaded.apes[0].trainingProgress,17);assert.equal(loaded.apes[0].hp,43);const recordCount=home.commissions.length;loaded.colonies.init(home);loaded.colonies.init(home);assert.equal(home.commissions.length,recordCount);assert.equal(home.facilities.length,1);assert.equal(home.lodge.hp,211);advance(loaded,home,100);assert.equal(home.commissions.find(c=>c.projectId===result.projectId).status,'complete');
});

test('commission queues, dense training and defense physics have explicit shared work limits',()=>{
 const {g,s}=colony(1000);s.wood=5000;s.food=5000;s.developedRadius=s.radius=650;g.colonies.layout(s);for(let i=0;i<12;i++){const kinds=['spearTower','training','defense','hut','garden','store','workshop'],kind=kinds[i%kinds.length],result=g.colonies.commission(s.id,kind);assert.equal(result.ok,true,result.reason)}assert.equal(g.colonies.commissioningSlots(s).active,12);assert.match(g.colonies.commission(s.id,'garden').reason,/queue is full/);advance(g,s,2);assert.ok(s.activeConstruction<=6);assert.ok(s.projects.filter(p=>!p.waiting&&p.kind!=='lumber').length<=6);
 s.facilities=[];for(let i=0;i<14;i++){const a=g.apes[i];Object.assign(a,{x:i*8,y:0});s.facilities.push({id:'tower-'+i,kind:'spearTower',x:i*8,y:0,hp:220,stage:4,station:{x:i*8,y:0},staffedBy:a.id,shotAt:0})}for(let i=0;i<100;i++)g.makeHuman(180+i%10,Math.floor(i/10),null);g.syncIndexes();g.humanGrid.rebuild(g.humans);g.colonies._defenseRosterAt=0;g.colonies.updateDefenses(1/60);assert.ok(s.spears.length<=32);for(let i=0;i<100;i++){g.time+=1/60;g.colonies.updateDefenses(1/60);const c=g.colonies.defenseCounters;assert.ok(c.towerChecks<=16&&c.projectiles<=32&&c.losTests<=32&&c.collisionChecks<=32*24)}
});

test('a full shared spear workload advances every saved projectile without dropping the tail',()=>{
 const {g,s}=colony();s.facilities=[];for(let village=0;village<4;village++){const home=village===0?s:{id:'remote-'+village,x:village*1000,y:0,population:1,facilities:[],spears:[]};if(village)g.settlements.push(home);home.spears=Array.from({length:32},(_,i)=>({id:home.id+'-shaft-'+i,x:home.x,y:i*3,fromX:home.x,fromY:i*3,vx:360,vy:0,life:1.3,damage:18,elapsed:0}));}
 for(let frame=0;frame<4;frame++){g.time+=1/60;g.colonies.updateDefenses(1/60);assert.equal(g.settlements.reduce((n,s)=>n+s.spears.length,0),128);assert.ok(g.colonies.defenseCounters.projectiles<=32&&g.colonies.defenseCounters.losTests<=32)}assert.ok(g.settlements.every(s=>s.spears.every(p=>p.elapsed>0)),'round robin services all four villages within four frames');
 for(let frame=0;frame<110;frame++){g.time+=1/60;g.colonies.updateDefenses(1/60)}assert.equal(g.settlements.reduce((n,s)=>n+s.spears.length,0),0,'finite projectile lifetime drains the workload');
});

test('older paid work reconstructs missing commission metadata without charging again',()=>{
 const {g,s}=colony(),result=g.colonies.commission(s.id,'training'),wood=s.wood,food=s.food;s.commissions=[];const old={id:'old-tower',kind:'spearTower',x:220,y:0,stage:4,hp:97,maxHp:220};s.structures.push(old);g.colonies.init(s);g.colonies.init(s);assert.equal(s.commissions.filter(c=>c.projectId===result.projectId).length,1);assert.equal(s.commissions[0].status,'queued');assert.equal(s.facilities.filter(f=>f.id==='old-tower').length,1);assert.equal(s.facilities[0].hp,97);assert.equal(s.wood,wood);assert.equal(s.food,food);assert.ok(!s.structures.includes(old));
});
