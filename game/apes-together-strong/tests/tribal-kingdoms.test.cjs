'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function arena(id='legacy'){
 const c=loadEngine(),g=new c.ATSGame('TRIBAL-TEST');if(id!=='legacy')assert.ok(g.chooseTribe(id));
 const w=g.world;w.objects.clear();w._spatial.clear();w.sites.clear();w.discovered.clear();w.ensure=()=>{};w.stream=()=>{};w.terrain=()=>({biome:'forest',water:false});w.waterBlocked=()=>false;w.concealment=()=>0;g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;return {c,g};
}
function ape(g,species='chimpanzee',x=0,y=0){const a=g.makeApe(x,y,'follow');a.species=species;g.siege.balance(a,true);return a}
function grids(g){g.syncIndexes();g.apeGrid.rebuild(g.apes,g.king);g.humanGrid.rebuild(g.humans);g.vehicleGrid.rebuild(g.vehicles);g.navigation.beginFrame(g.time);g.performance.beginStep(g)}
function object(g,data){const o={id:'object-'+g.nextId++,type:'wall',x:0,y:0,w:24,h:24,r:12,hp:1000,maxHp:1000,solid:true,...data};g.world.objects.set(o.id,o);g.world._indexObject(o);return o}
test('all three tribes keep comparable openings, exclusive unlocks and persistent identity; legacy loads unchanged',()=>{
 for(const id of ['ironback','shadowhand','emberwood']){const {c,g}=arena(id);const list=[];for(let i=0;i<6;i++)list.push(g.makeApe(i*35,0));assert.deepEqual(list.map(a=>a.species),Array.from(c.ATSTribes[id].opening));assert.equal(g.food,0);assert.equal(g.chooseTribe('shadowhand'),false);const data=JSON.parse(JSON.stringify(g.serialize())),copy=c.ATSGame.fromJSON(data);assert.equal(copy.tribe.id,id);assert.equal(copy.tribe.opening,6);assert.equal(copy.apes.length,6);assert.deepEqual(copy.apes.map(a=>a.hp),list.map(a=>a.hp));}
 const {c,g}=arena(),a=ape(g,'gorilla');a.hp=31;const data=JSON.parse(JSON.stringify(g.serialize()));delete data.tribe;const loaded=c.ATSGame.fromJSON(data);assert.equal(loaded.tribe.id,'legacy');assert.equal(loaded.apes[0].hp,31);assert.equal(loaded.chooseTribe('ironback'),false);
});
test('Ironback shields gain durability once and real ordinary-bullet resistance is exclusive',()=>{
 const {g}=arena('ironback'),a=ape(g,'gorilla');a.dir=0;a.shield={hp:100,maxHp:100};assert.equal(g.siege.absorb(a,0,{x:20,y:0,type:'bullet'}),0);assert.equal(a.shield.maxHp,120);g.siege.absorb(a,0,{x:20,y:0,type:'bullet'});assert.equal(a.shield.maxHp,120);
 delete a.shield;const hp=a.hp;g.hurt(a,100,{x:-30,y:0,type:'bullet'});assert.equal(hp-a.hp,90);assert.equal(g.batteringAssault(),false);
 const b=ape(g,'gorilla',30);grids(g);g.siege.select('gorilla');assert.ok(g.batteringAssault());assert.equal(a.batteringUntil,8);assert.equal(b.batteringUntil,8);assert.equal(g.batteringAssault(),false);
 const o=object(g,{type:'gate',x:50,y:0});g.damageObject(o,100,a);assert.ok(Math.abs((1000-o.hp)-155.25)<.001);
});
test('tribal construction and production bonuses have finite multipliers',()=>{
 for(const [id,expected]of [['ironback',.9],['shadowhand',1],['emberwood',1.2]]){const {g}=arena(id);assert.equal(g.tribalConstruction({_adults:[]}),expected)}
 const {g}=arena('emberwood'),worker=ape(g,'orangutan');worker.job='workshop';worker.workshopId='shop';const s={x:0,y:0,radius:100,_adults:[worker],suitability:{fertility:1},population:1,specialization:'balanced'};g.kingdom.modifiers=()=>({food:1});const income=g.colonies.production(s,{gardens:[],orchards:[],cooking:[],workshops:[{id:'shop',x:0,y:0}]});assert.ok(Math.abs(income.wood-.23)<1e-8);
});
test('Shadowhand construction costs reserve finite resources exactly once and only reinforce at a premium',()=>{
 const {g}=arena('shadowhand'),s={id:'home',name:'Home',x:0,y:0,population:12,level:2,radius:150,food:1000,wood:1000,livingFounding:true};g.settlements.push(s);
 for(let i=0;i<12;i++){const a=ape(g);a.state='settled';a.settlementId=s.id}grids(g);g.refreshSettlements();g.colonies.init(s);
 for(const kind of ['hut','spearTower','spearBattery','royalExpansion']){const quote=g.colonies.catalog(s).find(r=>r.kind===kind),wood=s.wood,food=s.food;assert.ok(Number.isFinite(quote.cost.wood));const result=g.colonies.commission(s.id,kind);assert.ok(result.ok,result.reason);assert.equal(s.wood,wood-quote.cost.wood);assert.equal(s.food,food-quote.cost.food);assert.equal(s.projects.find(p=>p.id===result.projectId).paidCost.wood,quote.cost.wood)}
 assert.equal(g.colonies.catalog(s).find(r=>r.kind==='spearBattery').cost.wood,77);
});
test('species, equipment, attack and quiet movement change real visibility without invisibility',()=>{
 const {g}=arena('shadowhand'),a=ape(g,'gibbon',180),h=g.makeHuman(0,0,null);h.dir=0;h.role='rifleman';g.king.x=-1000;grids(g);
 const quiet=g.stealthFactor(a);a.species='gorilla';assert.ok(g.stealthFactor(a)>quiet);a.species='gibbon';a.infiltration={response:'continue',safe:{x:180,y:0}};const still=g.stealthFactor(a);a.moving=true;assert.ok(g.stealthFactor(a)>0);a.equipment={torch:true};assert.ok(g.stealthFactor(a)>still*2);a.equipment={};a.attackTimer=.5;assert.ok(g.stealthFactor(a)>1);a.x=20;assert.equal(g.lineVisible(h,a),true);
});
test('distractions send only a proportionate guard response to the noise and preserve hidden army positions',()=>{
 const {g}=arena('shadowhand'),a=ape(g,'capuchin');g.king.x=-900;for(let i=0;i<12;i++){const h=g.makeHuman(100+i*4,40,null);h.state='patrol';h.hasRadio=true}grids(g);const before=g.lastContact;g.emitDistraction(a,{x:130,y:0});const investigating=g.humans.filter(h=>h.decoyUntil>g.time);assert.equal(investigating.length,2);assert.ok(investigating.every(h=>h.lastX===130&&h.lastY===0&&!h.targetId));assert.equal(g.siege.reports.length,0);assert.equal(g.lastContact,before);assert.equal(a.distractionReady,14);assert.ok(g.humans.some(h=>h.state==='patrol'));
});
test('silent relay cut interacts physically and expires without a destructive alarm',()=>{
 const {g}=arena('shadowhand'),a=ape(g,'chimpanzee',0),radio=object(g,{type:'radio',x:25,siteId:'base'}),site={id:'base',x:0,y:0,objects:[radio.id],radioDown:false};g.world.sites.set(site.id,site);grids(g);g.siege.select('chimpanzee');g.setInfiltration(true);g.silentRelay();g.siege.issue(['chimpanzee'],[{id:'cut',type:'sabotage',targetId:radio.id,targetKind:'object',x:25,y:0}]);
 for(let i=0;i<100&&!radio.disabledUntil;i++){g.time+=.05;g.updateApe(a,.05)}assert.ok(site.radioDown);assert.ok(radio.hp>0);assert.equal(g.siege.reports.length,0);assert.ok(radio.disabledUntil>g.time);g.time=radio.disabledUntil+.1;g.siege.tick(.1);assert.equal(site.radioDown,false);
});
test('ordinary gate-control orders open the matching gate and invalidate navigation',()=>{
 const {g}=arena('shadowhand'),a=ape(g,'chimpanzee'),gate=object(g,{id:'gate',type:'gate',x:150,gateState:'closed'}),control=object(g,{type:'gateControl',x:25,gateId:gate.id,discovered:true});grids(g);g.siege.select('chimpanzee');g.setInfiltration(true);const before=g.world.navRevision;assert.ok(g.siege.draft({x:control.x,y:control.y}));assert.ok(g.siege.dispatch());for(let i=0;i<100&&gate.solid;i++){g.time+=.05;g.updateApe(a,.05)}assert.equal(gate.solid,false);assert.ok(g.world.navRevision>before);assert.ok(control.hp>0);assert.equal(control.controlDisabled,true);
});
test('blocked climb points choose another access and interrupted climbs recover safely',()=>{
 const {g}=arena(),a=ape(g,'gibbon',-100),left=object(g,{id:'left',x:0,w:20,h:80,climbAccess:true,walkable:true}),right=object(g,{id:'right',x:400,w:20,h:80,climbAccess:true,walkable:true});
 const site={id:'fort',x:200,y:0,extentX:200,extentY:200,radius:350,fortressVersion:1,climbRoutes:[['left'],['right']],gates:[],innerAccess:[]};g.world.sites.set(site.id,site);left.siteId=right.siteId=site.id;object(g,{type:'rock',x:-35,y:0,w:22,h:30});const steps=g.siege.routeFor(a,{id:'move',type:'move',x:200,y:0});assert.equal(steps[1].wallId,'right');
 a.x=435;assert.ok(g.siege.startTransition(a,right,{x:365,y:0}));right.dead=true;g.siege.transition(a,.1);assert.equal(a.siegeTransition,undefined);assert.ok(Number.isFinite(a.x));assert.ok(!g.navigation.blocked(a.x,a.y,9,'ape'));
});
test('movement-facing uses committed translation and stays stable at rest and under small reversals',()=>{
 const {g}=arena(),a=ape(g);g.navigation.move=(a,dx,dy)=>{a.x-=3;a.dir=0};g.move(a,100,0,90,.03);assert.ok(Math.abs(a.visualDir-Math.PI)<.001);assert.equal(a.dir,0,'visual facing does not change shield simulation');const stable=a.visualDir;g.updateVisualFacing(a,a.x,a.y,.1);assert.equal(a.visualDir,stable);g.time=2;g.updateVisualFacing(a,a.x-.4,a.y,.03);assert.equal(a.visualDir,stable);
});
test('all elite identities survive the champion cap and repeated save loads without stacking health',()=>{
 const {c,g}=arena();for(let i=0;i<24;i++){const a=ape(g,'gorilla');g.champions.promote(a)}
 const elite=ape(g,'gibbon');assert.equal(g.champions.promote(elite),null);assert.ok(g.grantElite(elite,'gibbon','cell-1'));const hp=elite.maxHp;assert.equal(g.grantElite(elite,'gibbon','cell-1'),false);assert.equal(elite.maxHp,hp);assert.equal(elite.elite.name,'Ghost Gibbon');assert.ok(elite.veteran);
 let loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize())));loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(loaded.serialize())));const copy=loaded.apes.find(a=>a.id===elite.id);assert.equal(copy.elite.identity,elite.elite.identity);assert.equal(copy.maxHp,hp);
});
test('garrison holds posts, prioritizes tower crews and respects manual recall',()=>{
 const {g}=arena('emberwood'),s={id:'home',x:0,y:0,radius:200,entrances:[{x:0,y:180}],barriers:[],facilities:[],_adults:[]};g.settlements.push(s);for(let i=0;i<20;i++){const a=ape(g,'gorilla',i*5);a.state='settled';a.settlementId=s.id;a.job='guardian';s._adults.push(a)}grids(g);g.colonies.planGarrison(s);assert.ok(s.garrison.gate>0&&s.garrison.reserve>0&&s.garrison.emergency>0);const a=s._adults[0];a.state='follow';a.settlementId=null;g.time=3;g.colonies.planGarrison(s);const prior={x:a.x,y:a.y};g.tactics.combat(a,.1,null);assert.deepEqual({x:a.x,y:a.y},prior);
});
test('completed movement preserves infiltration and distant armored raids activate village defense',()=>{
 const {g}=arena('shadowhand'),a=ape(g,'gibbon');grids(g);g.siege.select('gibbon');g.setInfiltration(true,'retreat');g.siege.issue(['gibbon'],[{id:'finish',type:'move',x:0,y:0}]);g.siege.advance(a);g.siege.tick(.1);assert.equal(a.armyOrder,undefined);assert.equal(a.infiltration.response,'retreat');
 const s={id:'remote',name:'Remote',x:4000,y:0,radius:200,_adults:[a],entrances:[{x:4000,y:180}],barriers:[]};g.settlements.push(s);a.state='settled';a.settlementId=s.id;a.job='guardian';a.x=4000;delete a.infiltration;
 g.vehicles.push({id:'vehicle-test',vehicleClass:'tank',x:4100,y:0,hp:1000});grids(g);g.colonies.planGarrison(s);const activity=g.colonies.activityTarget(a,s);assert.equal(activity.x,a.garrisonPost.x);assert.equal(activity.y,a.garrisonPost.y);g.buildRelevance();assert.equal(s.attack,true);assert.equal(g.actorTier(a),0);
});
test('selected tribal upgrades charge once, persist and reject another tribe',()=>{
 for(const [id,upgrade]of [['ironback','gatehouse'],['emberwood','masterwork'],['shadowhand','decoys']]){
  const {c,g}=arena(id),target={id:'defense',kind:'spearTower',gate:true,hp:100,maxHp:100,stage:4},s={id:'village',x:0,y:0,wood:500,food:500,barriers:[target],facilities:[target]};g.settlements.push(s);g.colonies.lodgeAt=()=>s;g.world.syncSettlementBuildings=()=>{};
  const spec=c.ATSTribalUpgrades[upgrade];assert.ok(g.tribalUpgrade(upgrade,s.id,target.id));assert.equal(s.wood,500-spec.wood);assert.equal(s.food,500-spec.food);assert.equal(g.tribalUpgrade(upgrade,s.id,target.id),false);assert.equal(g.tribalUpgrade(id==='emberwood'?'gatehouse':'masterwork',s.id,target.id),false);assert.ok(g.serialize().tribe.unlocks.includes(upgrade));
 }
});
