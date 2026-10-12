'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function arena(kind='blacksite',count=44,tier=4,seed='PRISON-OPERATIONS'){
 const c=loadEngine(),g=new c.ATSGame(seed);
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;
 const chunk=g.world.chunks.get('0,0');chunk.objects=[];chunk.sites=[];
 g.world._buildSite(chunk,{id:'operation',x:0,y:0,name:'Operation test',type:kind==='transfer'?'regionalCommand':'armoredDepot',prisonKind:kind,military:true,tier,guards:12,count,radius:540,extentX:466,extentY:300,layout:0});
 const s=g.world.sites.get('operation');s.known=true;const objects=s.objects.map(id=>g.world.objects.get(id));return{c,g,s,objects,cells:objects.filter(o=>o.type==='cage')};
}
function grids(g){g.syncIndexes();g.apeGrid.rebuild(g.apes,g.king);g.humanGrid.rebuild(g.humans);g.vehicleGrid.rebuild(g.vehicles);g.navigation.beginFrame(g.time);g.performance.beginStep(g)}
function open(g,s,cell){if(cell.prisonLock==='dual')g.damageObject(g.world.objects.get(s.prison.powerId),10000,g.king);g.damageObject(g.world.objects.get(s.prison.controlId),10000,g.king);g.damageObject(cell,10000,g.king);return s.prison.cohorts.find(c=>c.cageId===cell.id)}
function members(g,c){return c.memberIds.map(id=>g.apesById.get(id))}
function frame(g,dt=.1){g.time+=dt;grids(g);g.siege.tick(dt);for(const a of g.apes)if(a.hp>0)g.updateApe(a,dt);g.prisons.tick(dt)}

test('high-level cages contain 10-20 normal monkeys and a seeded chance of 0-2 champions, retaining security layouts',()=>{
 const rolls=new Set();
 for(const kind of ['regional','blacksite','transfer','labor']){const {g,s,objects,cells}=arena(kind);assert.equal(s.count,cells.reduce((n,c)=>n+c.count,0));assert.equal(s.prison.initialCaptives,s.count);assert.ok(s.gates.length>=3);assert.equal(s.climbRoutes.length,2);assert.ok(objects.some(o=>o.type==='generator'));assert.ok(objects.some(o=>o.type==='prisonControl'));assert.ok(cells.every(o=>o.normalCount>=10&&o.normalCount<=20&&o.championCount>=0&&o.championCount<=2&&o.count===o.normalCount+o.championCount&&o.prisonReward==='reinforcements'&&!o.eliteForm));assert.ok(g.prisons.recon(s));}
 for(let i=0;i<18;i++){const {cells}=arena('blacksite',44,4,'PRISON-ROLL-'+i);for(const o of cells)rolls.add(o.championCount)}
 assert.deepEqual([...rolls].sort(),[0,1,2]);
 const a=arena(),b=arena();assert.equal(JSON.stringify(a.cells.map(o=>[o.normalCount,o.championCount])),JSON.stringify(b.cells.map(o=>[o.normalCount,o.championCount])));
});

test('lower-tier labor and regional cages preserve their finite stocks and ordinary support rewards',()=>{
 for(const kind of ['labor','regional']){const {cells,s}=arena(kind,8,3);assert.equal(cells.reduce((n,o)=>n+o.count,0),8);assert.equal(s.count,8);assert.ok(cells.some(o=>o.prisonReward==='family'));assert.ok(cells.every(o=>!o.eliteForm&&!o.championCount));}
});

test('steel cells reject raw damage and dual locks require both power and controls',()=>{
 const {g,s,cells}=arena(),dual=cells[0],power=cells.find(o=>o.prisonLock==='power'),steel=cells.find(o=>o.prisonLock==='control'),hp=power.hp;
 g.damageObject(power,1e9,g.king);assert.equal(power.hp,hp);assert.equal(s.prison.cohorts.length,0);
 g.damageObject(g.world.objects.get(s.prison.powerId),1e5,g.king);assert.ok(g.prisons.unlocked(power));assert.equal(g.prisons.unlocked(steel),false);assert.equal(g.prisons.unlocked(dual),false);assert.equal(g.getLights().filter(l=>l.kind==='tower').length,0);
 g.damageObject(steel,1e9,g.king);assert.equal(steel.dead,false);g.damageObject(g.world.objects.get(s.prison.controlId),1e5,g.king);assert.ok(g.prisons.unlocked(dual));g.damageObject(steel,1e9,g.king);assert.equal(steel.dead,true);assert.ok(s.prison.cohorts.length===1);
});

test('breaking a high-level cage creates distinct usable monkeys and champions immediately',()=>{
 const {g,s,cells}=arena(),cell=cells.find(o=>o.championCount),expected=cell.count,normal=cell.normalCount,champions=cell.championCount,c=open(g,s,cell),apes=members(g,c);
 assert.equal(g.apes.length,expected);assert.equal(c.count,0);assert.equal(g.prisons.reserved,0);assert.equal(apes.filter(a=>!a.champion).length,normal);assert.equal(apes.filter(a=>a.champion?.prisonChampion).length,champions);assert.ok(apes.every(a=>a.state==='follow'&&!a.elite&&!a.recoveryRemaining));
 assert.equal(new Set(apes.map(a=>a.id)).size,expected);for(let i=0;i<apes.length;i++)for(let j=i+1;j<apes.length;j++)assert.ok(Math.hypot(apes[i].x-apes[j].x,apes[i].y-apes[j].y)>=20,'released units start at separate walkable positions');
 assert.ok(apes.every(a=>!g.world.blocked(a.x,a.y,9)));assert.equal(g.stats.freed,0);assert.equal(g.prisons.release(cell),0);assert.equal(g.apes.length,expected);
});

test('released survivors damage a nearby guard and follow the king before receiving safe-rescue credit',()=>{
 const {g,s,cells}=arena(),c=open(g,s,cells[0]),a=members(g,c)[0];a.x=0;a.y=100;a.attackCD=0;g.king.x=0;g.king.y=300;
 const h=g.makeHuman(20,100,null),hp=h.hp;for(let i=0;i<10&&h.hp===hp;i++)frame(g,.1);assert.ok(h.hp<hp,'a rescued monkey immediately fights a real human');assert.equal(g.stats.freed,0);
 h.hp=0;a.x=0;a.y=100;const start=Math.hypot(a.x-g.king.x,a.y-g.king.y);g.commandCD=0;g.command('follow');for(let i=0;i<30;i++)frame(g,.1);assert.ok(Math.hypot(a.x-g.king.x,a.y-g.king.y)<start-40,'ordinary follower AI carries out the follow command');assert.equal(g.stats.freed,0);
});

test('population cap releases real individuals incrementally and cannot duplicate champions across partial releases',()=>{
 const {c,g,s,cells}=arena(),cell=cells.find(o=>o.championCount),total=cell.count;
 for(let i=0;i<997;i++)g.makeApe(700+i%10,700,'follow');const cohort=open(g,s,cell);assert.equal(cohort.memberIds.length,3);assert.equal(g.apes.length,1000);assert.equal(g.population,1000);assert.equal(g.makeApe(0,0),null);assert.equal(g.prisons.reserved,0);assert.equal(cell.count,total-3);
 g.prisons.reward(s,cohort);assert.equal(g.stats.freed,3);g.prisons.reward(s,cohort);assert.equal(g.stats.freed,3);
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),site=loaded.world.sites.get(s.id),copy=loaded.world.objects.get(cell.id);for(const a of loaded.apes.slice(0,30))a.hp=0;assert.equal(loaded.prisons.release(copy),total-3);assert.equal(copy.count,0);assert.equal(loaded.apes.filter(a=>a.rescuedFrom===s.id&&a.champion?.prisonChampion).length,cell.championCount);assert.equal(loaded.prisons.release(copy),0);assert.equal(site.count,s.count-(total-3));
});

test('recapture preserves individual IDs and champion identity across reload and rerelease',()=>{
 const {c,g,s,cells}=arena(),cell=cells.find(o=>o.championCount),cohort=open(g,s,cell),apes=members(g,cohort),ids=apes.map(a=>a.id).sort(),champion=apes.find(a=>a.champion),hp=champion.maxHp;
 g.prisons.recapture(s,cohort);assert.equal(g.population,0);assert.equal(cell.count,ids.length);assert.equal(cell.recapturedApes.length,ids.length);assert.equal(g.stats.freed,0);assert.equal(g.prisons.release(cell),0);assert.equal(g.prisons.captiveChampions(cell),cell.championCount);
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),copy=loaded.world.objects.get(cell.id);loaded.time=13;assert.equal(loaded.prisons.release(copy),ids.length);assert.equal(JSON.stringify(loaded.apes.map(a=>a.id).sort()),JSON.stringify(ids));assert.equal(loaded.apesById.get(champion.id).maxHp,hp);assert.equal(loaded.apes.filter(a=>a.champion).length,cell.championCount);assert.equal(copy.recapturedApes.length,0);assert.equal(loaded.prisons.release(copy),0);
});

test('safe rescue credit, casualties and cage completion are based on individual survivors exactly once',()=>{
 const {g,s,cells}=arena('regional',8,3),cell=cells[0],c=open(g,s,cell),apes=members(g,c);g.king.x=0;g.king.y=550;apes[0].x=0;apes[0].y=550;apes[1].hp=0;
 for(let i=0;i<18;i++){g.time+=.25;grids(g);g.prisons.tick(.25)}assert.equal(g.stats.freed,1);assert.equal(s.prison.casualties,1);assert.equal(s.prison.escorted,1);assert.equal(c.memberIds.length,0);assert.equal(cell.rescueCredited,true);assert.equal(g.stats.prisons,1);g.prisons.reward(s,c);assert.equal(g.stats.freed,1);assert.equal(g.stats.prisons,1);
});

test('saving between an individual death and the next prison update accounts for the casualty once',()=>{
 const {c,g,s,cells}=arena(),cohort=open(g,s,cells[0]),a=members(g,cohort)[0],n=cohort.memberIds.length;a.hp=0;g.apes=g.apes.filter(p=>p.hp>0);g.syncIndexes();
 let loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),site=loaded.world.sites.get(s.id);assert.equal(site.prison.casualties,1);assert.equal(site.prison.cohorts[0].memberIds.length,n-1);assert.equal(loaded.stats.freed,0);loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(loaded.serialize())));assert.equal(loaded.world.sites.get(s.id).prison.casualties,1);
});

test('save/load preserves live escaping individuals, their orders and issued champion grants',()=>{
 const {c,g,s,cells}=arena(),cell=cells.find(o=>o.championCount),cohort=open(g,s,cell),a=members(g,cohort)[0];a.x=91;a.prisonSafety=1.5;a.state='hold';s.prison.reserve=7;s.prison.rewardPaid=true;const ids=cohort.memberIds.slice(),hp=g.apes.map(a=>a.maxHp);
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),p=loaded.world.sites.get(s.id).prison;assert.deepEqual(p.cohorts[0].memberIds,ids);assert.equal(loaded.apesById.get(a.id).x,91);assert.equal(loaded.apesById.get(a.id).state,'hold');assert.equal(loaded.apesById.get(a.id).prisonSafety,1.5);assert.equal(p.reserve,7);assert.equal(p.rewardPaid,true);assert.equal(loaded.population,ids.length);assert.equal(JSON.stringify(loaded.apes.map(a=>a.maxHp)),JSON.stringify(hp));loaded.prisons.restore();assert.equal(loaded.population,ids.length);assert.equal(loaded.world.objects.get(p.controlId).dead,true);
});

test('legacy stacked crowds become usable survivors at their saved position once without rerolling finite stock',()=>{
 const {c,g,s,cells}=arena(),cell=cells[0],n=6,stock=s.count;cell.count-=n;cell.prisoners=cell.count;s.count-=n;s.prison.cohorts=[{id:'old-stack',cageId:cell.id,siteId:s.id,x:40,y:90,count:n,species:'gibbon',reward:'champion',eliteForm:'gibbon',rewardClaimed:false,_nav:{transient:true}}];delete g._prisons;
 let loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),site=loaded.world.sites.get(s.id),cohort=site.prison.cohorts[0];assert.equal(loaded.population,n);assert.equal(cohort.count,0);assert.equal(cohort.memberIds.length,n);assert.equal(loaded.apes.filter(a=>a.champion?.prisonChampion).length,1);assert.ok(loaded.apes.every(a=>!a.elite&&Math.hypot(a.x-40,a.y-90)<160));assert.equal(site.count,stock-n);const ids=cohort.memberIds.slice();
 loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(loaded.serialize())));loaded.prisons.restore();assert.equal(loaded.population,n);assert.deepEqual(loaded.world.sites.get(s.id).prison.cohorts[0].memberIds,ids);assert.equal(loaded.apes.filter(a=>a.champion).length,1);
});

test('unopened legacy high-level cages upgrade once while opened and escaping captive stocks remain finite',()=>{
 const {c,g,s,cells}=arena();for(const cell of cells){for(const key of ['prisonReleaseVersion','normalCount','normalRemaining','championCount','championRemaining'])delete cell[key];cell.count=cell.prisoners=6;cell.eliteForm='gibbon';cell.prisonReward='champion'}s.count=s.prison.initialCaptives=24;
 const opened=cells[0];opened.rescueOpened=true;opened.count=opened.prisoners=2;s.count-=4;s.prison.cohorts=[{id:'legacy-partial',siteId:s.id,cageId:opened.id,x:0,y:90,count:4,species:'gibbon',reward:'champion',rewardClaimed:false}];
 let loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),site=loaded.world.sites.get(s.id),liveCells=site.objects.map(id=>loaded.world.objects.get(id)).filter(o=>o?.type==='cage');assert.equal(liveCells[0].count,2);assert.equal(loaded.population,4);for(const cell of liveCells.slice(1)){assert.ok(cell.normalCount>=10&&cell.normalCount<=20);assert.ok(cell.championCount<=2);assert.equal(cell.count,cell.normalCount+cell.championCount)}assert.equal(site.count,liveCells.reduce((n,o)=>n+o.count,0));const stock=site.count;
 loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(loaded.serialize())));assert.equal(loaded.world.sites.get(s.id).count,stock);assert.equal(loaded.population,4);
});

test('ordinary survivors navigate an opened exit and earn credit only beyond the real perimeter',()=>{
 const {g,s,cells}=arena('regional',8,3),c=open(g,s,cells[0]);s.radioDown=true;for(const id of s.gates)g.siege.openGate(g.world.objects.get(id));members(g,c).forEach((a,i)=>{a.x=i*24;a.y=170});g.king.x=0;g.king.y=550;
 for(let i=0;i<250&&c.memberIds.length;i++)frame(g,.1);assert.equal(c.memberIds.length,0,'real individuals pass the gate and reach safety');assert.equal(g.stats.freed,2);assert.equal(g.prisons.reserved,0);
});

test('reinforcements spend saved reserves and cease after relay sabotage',()=>{
 const {g,s}=arena();s.alarm=true;s.prison.powerDown=false;grids(g);g.prisons.response(s);const spent=s.prison.reserveCommitted;assert.ok(spent>0);assert.equal(s.prison.reserve,20-spent);g.time=30;s.radioDown=true;g.prisons.response(s);assert.equal(s.prison.reserveCommitted,spent);s.radioDown=false;for(let i=0;i<10;i++){g.time+=30;g.prisons.response(s)}assert.ok(s.prison.waves<=3);assert.ok(s.prison.reserveCommitted<=20);
});

test('lockdown seals the inner gate without rebuilding a destroyed gate',()=>{
 const {g,s}=arena(),gate=g.world.objects.get(s.prison.riotId);s.alarm=true;s.prison.alarmLevel=3;g.time=1;grids(g);g.prisons.tick(.25);assert.equal(gate.solid,true);assert.equal(gate.gateState,'closed');g.damageObject(gate,1e9,g.king);g.time=2;grids(g);g.prisons.tick(.25);assert.equal(gate.solid,false);assert.equal(gate.dead,true);
});

test('procedural prison variants use existing military parcels and preserve opening rescues',()=>{
 const {c}=arena(),w=new c.ATSWorld('KINGDOM-PRISONS'),kinds=new Set();assert.equal(w._sitePlan(0,-1).prisonKind,undefined);assert.equal(w._sitePlan(0,0).count,6);for(let y=-24;y<=24;y+=3)for(let x=-23;x<=25;x+=3){const p=w._sitePlan(x,y);if(!p?.prisonKind)continue;kinds.add(p.prisonKind);assert.equal(p.military,true);assert.ok(p.extentX>=300);assert.ok(p.approach.length>=2)}assert.equal(kinds.size,4);
});

test('a climbing raider reaches the internal release controls in each real prison blueprint',()=>{
 for(const kind of ['regional','blacksite','transfer','labor']){const {g,s}=arena(kind);s.radioDown=true;const target=g.world.objects.get(s.prison.controlId),a=g.makeApe(-s.extentX-100,0,'follow');a.species='gibbon';g.siege.balance(a,true);g.siege.issue(['gibbon'],[{id:'release-controls',type:'sabotage',targetId:target.id,targetKind:'object',x:target.x,y:target.y}]);for(let frame=0;frame<5000&&!target.dead;frame++){g.time+=1/30;grids(g);g.siege.tick(1/30);g.updateApe(a,1/30)}assert.ok(target.dead,kind+' release controls reachable');assert.equal(s.prison.controlsDown,true);}
});

test('lower-tier family survivors become saved journeys and celebrate only after reaching home',()=>{
 const {c,g,s,cells}=arena('labor',8,3),home={id:'sanctuary-home',name:'Sanctuary',x:1300,y:800,population:1,food:500,wood:100,housing:20,level:1,age:0,birthTimer:0,known:true};g.settlements.push(home);g.makeApe(home.x,home.y,'settled',home.id);g.refreshSettlements();g.colonies.init(home);g.kingdom.specialize(home.id,'sanctuary');const crowd=open(g,s,cells.find(c=>c.prisonReward==='family'));g.prisons.reward(s,crowd);const survivors=g.apes.filter(a=>a.rescuedFrom===s.id);assert.equal(survivors.length,2);assert.ok(survivors.every(a=>a.kingdomMission?.kind==='survivors'));assert.equal(home.moraleUntil,0);
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),destination=loaded.settlement(home.id);for(const a of loaded.apes.filter(a=>a.rescuedFrom===s.id)){a.x=destination.x;a.y=destination.y}loaded.kingdom.tick(destination);assert.ok(destination.moraleUntil>loaded.time);assert.ok(destination.kingdomEvents.some(e=>e.kind==='survivors'));assert.equal(loaded.population,3);
});

test('high-security rare armor and materials reward an existing safe survivor once per saved facility',()=>{
 const {c,g,s,cells}=arena(),crowd=open(g,s,cells[0]),population=g.population;assert.equal(g.equipment.rareMaterials||0,0);g.prisons.reward(s,crowd);assert.equal(g.population,population);assert.equal(g.equipment.rareMaterials,4);assert.ok(g.apes.some(a=>a.outfit?.body==='heavyArmor'));g.prisons.reward(s,crowd);assert.equal(g.equipment.rareMaterials,4);
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),site=loaded.world.sites.get(s.id);assert.equal(site.prison.rareRewardClaimed,true);assert.equal(loaded.equipment.rareMaterials,4);loaded.prisons.reward(site,site.prison.cohorts[0]);assert.equal(loaded.equipment.rareMaterials,4);
});
