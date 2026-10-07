'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');

function arena(population=1000){
 const c=loadEngine(),g=new c.ATSGame('LATE-ARMY-ESCALATION','survival');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.intel.clear();
 g.world.ensure=()=>{};g.world.stream=()=>{};g.world.trimDistant=()=>{};g.world.getSites=()=>[];g.world.getObjects=()=>[];
 g.world.boundsReady=()=>true;g.world.blocked=()=>false;g.world.vehicleBlocked=()=>false;g.world.lineClear=()=>true;
 g.world.terrain=()=>({biome:'farmland',road:true,water:false});g.world.vehicleStaging=(s,k,i)=>({x:s.x+i*65,y:s.y});
 g.spawnSites=()=>{};g.time=200;g.food=100000;
 for(let i=0;i<population;i++)g.makeApe(i%30*12,Math.floor(i/30)*12,'hold');
 g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.updateResponseStage();return{c,g};
}
function source(g,x,y=0,strength=1200){
 const s={id:'source-'+x+','+y,name:'Regional installation',x,y,tier:5,military:true,guards:0,strength,initialStrength:strength,spawned:true,objects:[],lastRaid:-100,
  vehicleInventory:{jeep:16,command:16,truck:40,apc:40,ifv:24,tank:24,heli:12},armorCapacity:1000,
  staging:[{x,y}],approach:[{x,y},{x:x*.65,y:y*.65}],roadblocks:[{x:x*.65,y:y*.65}]};
 g.world.sites.set(s.id,s);return s;
}
function campaign(population){
 const {c,g}=arena(population);
 for(let i=0;i<8;i++){const a=i*Math.PI/4;source(g,Math.round(Math.cos(a)*1800),Math.round(Math.sin(a)*1800))}
 g.addIntel(0,0,30);g.responseDirector();return{c,g};
}
function people(g){return g.humans.filter(h=>h.hp>0).length+g.vehicles.filter(v=>v.hp>0).reduce((n,v)=>n+(v.troops||0),0)}
function roleWeight(c,role){return c.ATSHumanRoles[role]?.weight||1}
function measuredWeight(c,g){
 return g.humans.filter(h=>h.hp>0).reduce((n,h)=>n+roleWeight(c,h.role),0)+g.vehicles.filter(v=>v.hp>0).reduce((n,v)=>{
  const cargo=v.troopRoles?Array.from(v.troopRoles).slice(v.deployedTroops||0,(v.deployedTroops||0)+(v.troops||0)).reduce((sum,role)=>sum+roleWeight(c,role),0):(v.troops||0)*2;
  return n+g.forces.vehicleSpec(v).budget+cargo;
 },0)+g.helis.filter(h=>h.hp>0).length*10;
}
function bounded(c,g){
 assert.equal(g.forceWeight(),measuredWeight(c,g),'elite soldiers and armored variants consume their actual weighted cost');
 assert.ok(g.forceWeight()<=g.responseBudget);assert.ok(people(g)<=g.activeHumanCapacity());
 const tanks=g.vehicles.filter(v=>v.hp>0&&v.vehicleClass==='tank').length,armor=g.vehicles.filter(v=>v.hp>0&&['apc','ifv','armored'].includes(v.vehicleClass)).length;
 assert.ok(tanks<=g.vehicleLimits.tank);assert.ok(armor<=g.vehicleLimits.armored);assert.ok(g.vehicles.filter(v=>v.hp>0).length<=g.vehicleLimits.total);
}

test('later war bands deploy larger real multi-base armies while retaining the 300-ape scale',()=>{
 const results=[];
 for(const n of [300,450,650,850,1000]){
  const {c,g}=campaign(n),operation=g.activeOperations('field')[0];assert.ok(operation,'a reported army receives a field response');
  const committed=g.humans.filter(h=>h.operationId===operation.id).length+g.vehicles.filter(v=>v.operationId===operation.id).reduce((sum,v)=>sum+(v.troops||0),0);
  assert.equal(committed,operation.people,'operation strength is backed by actual soldiers and transport passengers');
  assert.ok(operation.origins.length>=2,'separate installations contribute actual columns');bounded(c,g);
  results.push({n,committed,people:people(g),vehicles:g.vehicles.length});
 }
 assert.ok(results[0].committed<=60,'the existing early war package stays bounded');
 for(let i=1;i<results.length;i++)assert.ok(results[i].committed>results[i-1].committed,JSON.stringify(results));
 assert.ok(results[1].committed>=100);assert.ok(results[2].committed>=180);assert.ok(results[3].committed>=260);assert.ok(results[4].committed>=330);
 assert.ok(results[4].people>=results[0].people*3,'total-war escalation produces a substantially larger army, beyond raising caps');
});

test('population growth inside each late war band increases dispatched soldiers',()=>{
 for(const [lower,upper]of [[450,600],[650,800],[850,1000]]){
  const low=campaign(lower).g,high=campaign(upper).g;
  assert.ok(high.activeOperations('field')[0].people>low.activeOperations('field')[0].people,`${lower} to ${upper} changes real deployed strength`);
  assert.ok(high.responseBudget>low.responseBudget);assert.ok(high.activeHumanCapacity()>low.activeHumanCapacity());
 }
});

test('large formations bring unlocked specialists and distinct stronger armored variants',()=>{
 const snapshots=new Map();
 for(const n of [300,450,650,850,1000]){
  const {c,g}=campaign(n),roles=new Set(g.humans.map(h=>h.role));for(const v of g.vehicles)for(const role of v.troopRoles||[])roles.add(role);
  const variants=new Set(g.vehicles.map(v=>v.variant).filter(Boolean));snapshots.set(n,{roles,variants});
  for(const v of g.vehicles.filter(v=>v.variant)){const spec=g.forces.vehicleSpec(v),base=c.ATSVehicleSpecs[v.vehicleClass];assert.ok(spec.hp>base.hp);assert.ok(spec.budget>base.budget);assert.equal(v.maxHp,spec.hp);assert.equal(v.kind,v.vehicleClass)}
  bounded(c,g);
 }
 for(const role of ['assault','commando','juggernaut'])assert.equal(snapshots.get(300).roles.has(role),false);
 assert.equal(snapshots.get(300).variants.size,0);
 assert.ok(snapshots.get(450).roles.has('assault'));assert.ok(snapshots.get(450).variants.has('veteran'));
 assert.ok(snapshots.get(650).roles.has('commando'));assert.ok(snapshots.get(650).variants.has('siege'));assert.ok(snapshots.get(650).variants.has('sentinel'));
 assert.ok(snapshots.get(850).roles.has('juggernaut'));assert.ok(snapshots.get(850).variants.has('ironclad'));
 assert.ok(snapshots.get(1000).variants.size>=4,'a late army retains several recognizable tank and support models');
});

test('every deployed soldier and vehicle debits finite installation resources',()=>{
 const {c,g}=arena(1000),sites=[];
 for(let i=0;i<8;i++){const a=i*Math.PI/4;sites.push(source(g,Math.round(Math.cos(a)*1800),Math.round(Math.sin(a)*1800),300))}
 const inventory=sites.map(s=>({...s.vehicleInventory}));let deployments=0;
 for(let wave=0;wave<16;wave++){
  const before=people(g);g.time+=35;g.addIntel(0,0,30);g.nextDirectorAt=0;g.performance.beginStep(g);g.responseDirector();deployments+=people(g)-before;bounded(c,g);
  for(const s of sites){assert.ok(s.strength>=0);assert.ok(s.armorCapacity>=0);for(const amount of Object.values(s.vehicleInventory))assert.ok(amount>=0)}
 }
 assert.ok(deployments>300,'the budget permits a real expanded war');
 assert.equal(sites.reduce((sum,s)=>sum+300-s.strength,0),people(g),'embarked soldiers spend source personnel immediately');
 for(let i=0;i<sites.length;i++)for(const kind of Object.keys(inventory[i])){
  const deployed=kind==='heli'?g.helis.filter(h=>h.siteId===sites[i].id).length:g.vehicles.filter(v=>v.siteId===sites[i].id&&v.vehicleClass===kind).length;
  assert.equal(inventory[i][kind]-sites[i].vehicleInventory[kind],deployed,'a variant consumes one chassis from its base inventory');
 }
 const {g:depleted}=arena(1000),small=source(depleted,1600,0,21);small.vehicleInventory={tank:1};small.armorCapacity=25;
 assert.equal(depleted.spawnRaid(small,{x:0,y:0},false,{major:true}),true);assert.equal(people(depleted),21);assert.equal(small.strength,0);assert.equal(small.vehicleInventory.tank,0);
 depleted.time+=90;assert.equal(depleted.world.replenishInstallation(small,depleted.time),false);assert.equal(depleted.spawnRaid(small,{x:0,y:0}),false);assert.equal(people(depleted),21,'mock installations receive no free reserve');
});

test('passenger roles and armored variants survive growth, deployment and saving without a budget jump',()=>{
 const {c,g}=campaign(650),carrier=g.vehicles.find(v=>v.troops>0&&v.troopRoles?.length);assert.ok(carrier);
 const roster=Array.from(carrier.troopRoles),initial=carrier.troops;g.forces.dismount(carrier,1);assert.equal(carrier.troops,initial-1);
 for(let i=g.population;i<1000;i++)g.makeApe(i%30*12,Math.floor(i/30)*12,'hold');g.updateResponseStage();
 const snapshot=JSON.parse(JSON.stringify(g.serialize())),loaded=c.ATSGame.fromJSON(snapshot),saved=loaded.vehicles.find(v=>v.id===carrier.id);
 assert.deepEqual(Array.from(saved.troopRoles),roster);assert.equal(saved.deployedTroops,carrier.deployedTroops);assert.equal(saved.troops,carrier.troops);
 for(const v of g.vehicles){const restored=loaded.vehicles.find(a=>a.id===v.id);assert.equal(restored.variant,v.variant);assert.equal(restored.maxHp,v.maxHp);assert.equal(loaded.forces.vehicleSpec(restored).budget,g.forces.vehicleSpec(v).budget)}
 for(const site of g.world.sites.values()){const restored=loaded.world.sites.get(site.id);assert.equal(restored.strength,site.strength);assert.deepEqual({...restored.vehicleInventory},{...site.vehicleInventory})}
 while(saved.troops){const before=loaded.forceWeight(),next=saved.troopRoles[saved.deployedTroops||0];loaded.time+=.4;loaded.forces.dismount(saved,.4);assert.equal(loaded.humans.at(-1).role,next);assert.equal(loaded.forceWeight(),before,'a frozen passenger roster cannot become a free stronger soldier after growth')}
 bounded(c,loaded);assert.equal(saved.deployedTroops,roster.length);
});

test('an army at the expanded human capacity retains distant actors and bounded combat work',()=>{
 const {c,g}=arena(1000),capacity=g.activeHumanCapacity(),near=80;
 g.responseDirector=()=>{};g.heliTimer=100000;g.king.hp=g.king.maxHp=1000000;for(const a of g.apes)a.hp=a.maxHp=1000000;
 for(let i=0;i<capacity;i++){
  const close=i<near,h=g.makeHuman(close?480+i%10*8:5000+i%40*12,close?(Math.floor(i/10)-4)*12:Math.floor(i/40)*12,null);
  g.forces.assign(h,null,g.forces.militaryRole(i));h.hp=h.maxHp=1000000;h.specialAt=100000;
  if(close)Object.assign(h,{state:'combat',targetId:g.apes[29].id,reported:true,suspicion:1,dir:Math.PI,lastSeenAt:g.time,shootTimer:0});
 }
 let maxThink=0,maxLos=0,maxExpanded=0,maxSearches=0,abstractWork=0,bullets=0;
 for(let frame=0;frame<60;frame++){
  g.update(1/60,{});maxThink=Math.max(maxThink,g.performance.counters.aiThinks);maxLos=Math.max(maxLos,g.performance.counters.losTests);
  maxExpanded=Math.max(maxExpanded,g.navigation.stats.frameExpanded);maxSearches=Math.max(maxSearches,g.navigation.stats.frameSearches||0);
  abstractWork+=g.performance.counters.abstractHumans;bullets=Math.max(bullets,g.bullets.length);
 }
 assert.equal(g.humans.length,capacity);assert.equal(g.population,1000);assert.ok(capacity>=1000);
 assert.equal(g.humans.filter(h=>h._simTier===2).length,capacity-near,'offscreen reinforcement actors remain present at their strategic cadence');
 assert.ok(abstractWork>0);assert.ok(bullets>0,'the nearby part of the army still fights');
 assert.ok(maxThink<=32);assert.ok(maxLos<=96);assert.ok(maxSearches<=3);assert.ok(maxExpanded<=192);
 for(const h of g.humans)assert.ok(Number.isFinite(h.x)&&Number.isFinite(h.y));
});
