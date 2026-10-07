'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function setup(n=0){
 const c=loadEngine(),g=new c.ATSGame('MILITARY');g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.getObjects=()=>[];g.world.getSites=()=>[];g.world.ensure=()=>{};g.world.stream=()=>{};g.world.boundsReady=()=>true;g.world.blocked=()=>false;g.world.lineClear=()=>true;g.world.terrain=()=>({biome:'farmland',road:true,water:false});
 for(let i=0;i<n;i++)g.makeApe(i%20*15,Math.floor(i/20)*15,'hold');g.time=60;g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.updateResponseStage();return{c,g};
}
function base(g,x=1400,y=0){const s={id:'base-'+x+','+y,name:'Iron Road',x,y,tier:5,guards:0,strength:160,spawned:true,objects:[],lastRaid:0,vehicleInventory:{jeep:2,command:1,truck:4,apc:4,ifv:2,tank:3,heli:3},armorCapacity:90,staging:[{x,y}],approach:[{x,y},{x:x-300,y}],roadblocks:[{x:x-300,y}]};g.world.sites.set(s.id,s);return s}
test('active followers guarantee all five stage boundaries while settled apes do not',()=>{
 for(const [n,stage]of [[24,1],[25,2],[59,2],[60,3],[119,3],[120,4],[219,4],[220,5],[350,5]]){const {g}=setup(n);assert.equal(g.responseStage,stage,`${n} followers`)}
 const{g}=setup(120);for(const a of g.apes)a.state='settled';g.updateResponseStage();assert.equal(g.tier,1);
});
test('mobilization happens once at 200 and survives save load',()=>{
 const{c,g}=setup(200);assert.equal(g.mobilized,true);const count=()=>g.messages.filter(m=>m.text.startsWith('MILITARY MOBILIZATION')).length;assert.equal(count(),1);g.updateResponseStage();assert.equal(count(),1);
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize())));loaded.updateResponseStage();assert.equal(loaded.mobilized,true);assert.equal(loaded.messages.filter(m=>m.text.startsWith('MILITARY MOBILIZATION')).length,1);
});
test('armored response spends finite personnel and inventory at the source with frozen intelligence',()=>{
 const{g}=setup(230),s=base(g),target={x:0,y:0};assert.equal(g.spawnRaid(s,target),true);
 const event=g.events.at(-1);assert.ok(event.vehicles.includes('tank'));assert.ok(event.vehicles.includes('apc'));assert.equal(s.strength,160-event.people);assert.equal(s.vehicleInventory.tank,2);assert.equal(s.vehicleInventory.apc,3);assert.ok(g.forceWeight()<=g.responseBudget);
 assert.ok(g.humans.every(h=>h.x>=s.x-10));assert.ok(g.vehicles.every(v=>v.x>=s.x-10));target.x=800;assert.equal(g.vehicles[0].target.x,0);
 const h=g.humans[0],reserve=s.strength;g.hurt(h,10000,g.king);assert.equal(s.strength,reserve,'allocated troop death cannot debit reserve twice');
});
test('destroying infrastructure prevents armor, radio convergence and air launches',()=>{
 const{g}=setup(350),s=base(g);s.depotDown=true;assert.equal(g.responsePackage(s,{x:0,y:0}).vehicles.length,0);s.depotDown=false;s.fuelDown=true;assert.equal(g.responsePackage(s,{x:0,y:0}).vehicles.length,0);assert.equal(g.launchHelicopter(),false);
 s.fuelDown=false;s.radioDown=true;const other=base(g,-1400,0);other.radioDown=true;g.addIntel(0,0,20);g.responseDirector();assert.equal(g.events.filter(e=>e.type==='raid').length,1,'isolated sites cannot coordinate encirclement');
});
test('a full response budget and depleted base cannot generate reinforcements',()=>{
 const{g}=setup(230),s=base(g);g.forceWeight=()=>g.responseBudget;assert.equal(g.spawnRaid(s,{x:0,y:0}),false);assert.equal(s.strength,160);g.forceWeight=()=>0;s.strength=0;assert.equal(g.spawnRaid(s,{x:0,y:0}),false);
});
test('director requires fresh reports or known settlement; three origins leave an escape bearing',()=>{
 const{g}=setup(350);for(const[x,y]of [[1400,0],[-1400,0],[0,1400],[0,-1400]])base(g,x,y);
 g.responseDirector();assert.equal(g.events.length,0);g.nextDirectorAt=0;g.addIntel(0,0,20);g.responseDirector();assert.ok(g.events.filter(e=>e.type==='raid').length>=2);assert.ok(g.events.filter(e=>e.type==='raid').length<=3);
 const{g:stale}=setup(230);base(stale);stale.world.intel.set('0,0',{x:0,y:0,heat:90,lastSeen:-200});stale.responseDirector();assert.equal(stale.events.length,0);
});
test('known settlements receive a named army warning from a real installation',()=>{
 const{g}=setup(230);base(g);g.time=200;g.settlements.push({id:'moonroot',name:'Moonroot',x:0,y:0,known:true,population:20,lastRaid:0});g.responseDirector();assert.ok(g.events.some(e=>e.settlementId==='moonroot'));assert.ok(g.messages.some(m=>m.text.startsWith('ARMY APPROACHING MOONROOT')));
});
test('legacy version one saves derive military fields without invalidating campaigns',()=>{
 const{c,g}=setup(230);const saved=JSON.parse(JSON.stringify(g.serialize()));for(const key of ['militaryVersion','responseStage','responsePeak','mobilized','warPhase','squads'])delete saved[key];saved.stats.largestHorde=230;saved.vehicles=[{id:'vehicle-old',x:100,y:0,kind:'armored',hp:170,maxHp:400,dir:0,state:'idle',phase:0,shootTimer:2}];
 const loaded=c.ATSGame.fromJSON(saved);assert.equal(loaded.vehicles[0].hp,170);assert.equal(loaded.mobilized,true);assert.equal(loaded.responseStage,5);assert.equal(loaded.navigation.world,loaded.world);
});
test('cold military sources queue terrain without spending or placing response actors',()=>{
 const{g}=setup(230),s=base(g);base(g,2600,0);g.time=300;g.world.boundsReady=()=>false;let requests=0;g.world.requestCorridor=()=>requests++;
 assert.equal(g.spawnRaid(s,{x:0,y:0}),false);assert.equal(g.dispatchConvoy([...g.world.sites.values()]),false);assert.equal(g.deployRoadblock(s,{x:0,y:0}),false);assert.ok(requests>=2);assert.equal(g.humans.length,0);assert.equal(g.vehicles.length,0);assert.equal(s.strength,160);assert.equal(s.vehicleInventory.tank,3);
});
test('base convoys carry finite troops and respect both vehicle and passenger ceilings',()=>{
 const{g}=setup(230),s=base(g);base(g,2600,0);g.time=300;for(let i=0;i<22;i++)g.vehicles.push({id:'vehicle-existing-'+i,kind:'jeep',vehicleClass:'jeep',hp:230});
 assert.equal(g.dispatchConvoy([...g.world.sites.values()]),true);assert.equal(g.vehicles.length,24);assert.equal(s.strength,148);assert.equal(g.vehicles.at(-1).troops,12);assert.ok(g.forceWeight()<=g.responseBudget);
 const{g:full}=setup(230);base(full);base(full,2600,0);full.time=300;for(let i=0;i<215;i++)full.makeHuman(0,0,null);full.forceWeight=()=>0;assert.equal(full.dispatchConvoy([...full.world.sites.values()]),false);
});
