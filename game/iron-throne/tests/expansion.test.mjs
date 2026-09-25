import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, access } from 'node:fs/promises';
import { BUILDINGS, FORMATIONS, RESOURCES, SAVE_VERSION, UNITS } from '../data.mjs';
import { PLAYER, build, buildCheck, buildHighway, declareWar, economyProjection, kingdom, mergeArmies, parseSave, recruit, resolveEconomy, resolveMovement, settlements, splitArmy, strategyTurn } from '../core.mjs';
import { createGame } from './fixtures/legacy-game.mjs';
import { buildingLevel, emptyUnits, productionPlan, tileProduction } from '../economy.mjs';
import { commitDeal, endTurn, evaluateDeal, resolveRecurringTrade, makeContext } from '../diplomacy.mjs';
import { economicRelationship, recordTrade } from '../living.mjs';
import { aiResourceTrade, contractCheck, economicNeeds, scheduleTrade, tradeInfrastructure, tradeRoute } from '../trade.mjs';
import { armySpeed, resolveFieldBattle, setFormation, siegePower } from '../warfare.mjs';
import { ART, fallbackArtURL } from '../asset-manifest.mjs';
const stocked=s=>{for(const k of s.kingdoms){for(const r of RESOURCES)k.resources[r]=400;k.commands=8;k.population=140;}};
const army=(units,formation='balanced',owner=PLAYER)=>({units:{...emptyUnits(),...units},formation,morale:1,retreats:0,owner});
const terrain=(name='plains',extra={})=>({terrain:name,owner:'wintermere',walls:0,building:null,levels:{},...extra});
function routes(s){for(const [id,owner] of [['6,6',PLAYER],['18,4','wintermere']])Object.assign(s.tiles[id],{owner,building:'tradeOutpost',levels:{tradeOutpost:2,road:1},road:true});for(const id of tradeRoute(s,PLAYER,'wintermere').path){s.tiles[id].road=true;s.tiles[id].levels.road=1;}}
const supply={type:'RECURRING',giveResource:'gold',giveAmount:12,receiveResource:'food',receiveAmount:12,duration:6};

test('regional generation is seeded, unequal, and not six balanced resource rings',()=>{
 const s=createGame(911),other=createGame(912);assert.deepEqual(s,createGame(911));assert.notDeepEqual(s.tiles,other.tiles);
 const opening=id=>Object.values(s.tiles).filter(t=>t.owner===id&&t.building).map(t=>t.building).sort();
 assert.notDeepEqual(opening('thornwall'),opening('wintermere'));
 const north=Object.values(s.tiles).filter(t=>t.region==='wintermere'),iron=Object.values(s.tiles).filter(t=>t.region==='thornwall');
 assert.ok(north.filter(t=>t.resource==='wood').length>north.filter(t=>t.resource==='iron').length*2);
 assert.ok(iron.filter(t=>t.resource==='iron').length>iron.filter(t=>t.resource==='food').length*2);
 assert.ok(Object.values(s.tiles).some(t=>t.quality==='exceptional'));
});
test('deposit quality and building tiers improve actual output',()=>{
 const t={building:'mine',resource:'iron',quality:'poor',levels:{mine:1}};
 const poor=tileProduction(t,PLAYER).iron;t.quality='exceptional';const exceptional=tileProduction(t,PLAYER).iron;t.levels.mine=3;
 assert.ok(exceptional>poor*2);assert.ok(tileProduction(t,PLAYER).iron>exceptional*2);
});
test('manufacturing consumes shared inputs once, stalls without them, and never invents iron',()=>{
 const s=createGame(),k=kingdom(s,PLAYER);for(const t of Object.values(s.tiles))if(t.owner===PLAYER)t.owner=null;
 const a=s.tiles['5,6'],b=s.tiles['6,6'];for(const t of [a,b])Object.assign(t,{owner:PLAYER,building:null,workshop:true,levels:{workshop:1}});
 for(const r of RESOURCES)k.resources[r]=0;k.resources.iron=2;k.resources.wood=3;
 const plan=productionPlan(s,PLAYER);assert.equal(plan.income.tools,5);assert.equal(plan.income.iron,-2);assert.equal(plan.income.wood,-3);assert.equal(plan.stalls.length,1);
 k.resources.iron=0;assert.equal(productionPlan(s,PLAYER).income.tools,0);
});
test('upgrades pay once, preserve completed production, then activate on the final turn',()=>{
 const s=createGame();stocked(s);const t=s.tiles['6,6'],k=kingdom(s,PLAYER),before=tileProduction(t,PLAYER).food;
 assert.equal(build(s,PLAYER,t.id,'farm').ok,true);assert.equal(t.project.level,2);assert.equal(buildingLevel(t,'farm'),1);assert.equal(tileProduction(t,PLAYER).food,before);
 const gold=k.resources.gold;assert.equal(build(s,PLAYER,t.id,'farm').ok,false);assert.equal(k.resources.gold,gold);
 const remaining=t.project.remaining;for(let n=0;n<remaining-1;n++)resolveEconomy(s);assert.equal(buildingLevel(t,'farm'),1);resolveEconomy(s);assert.equal(buildingLevel(t,'farm'),2);assert.ok(tileProduction(t,PLAYER).food>before);
});
test('construction rejects missing tools, wrong terrain, and unmet local project prerequisites',()=>{
 const s=createGame();stocked(s);const home=s.tiles['5,6'];assert.match(buildCheck(s,PLAYER,home.id,'royalArsenal'),/Grand Arsenal/);
 const mine=Object.values(s.tiles).find(t=>t.owner===PLAYER&&t.building==='mine');kingdom(s,PLAYER).resources.tools=0;assert.match(buildCheck(s,PLAYER,mine.id,'mine'),/tools/);
 assert.match(buildCheck(s,PLAYER,home.id,'harbor'),/coastal/);assert.match(buildCheck(s,PLAYER,'17,4','market'),/borders/);
});
test('harbors and diplomatic offices require towns or cities while forts retain military construction',()=>{
 for(const type of ['harbor','envoyOffice','chancery'])for(const level of [0,1,2]){
  const s=createGame();stocked(s);const t=s.tiles['6,6'];
  Object.assign(t,{building:'fort',terrain:'coast',levels:{fort:1,envoyOffice:1},envoyOffice:true});
  t[type]=level>0;t.levels[type]=level;
  const before=structuredClone(s);
  assert.deepEqual(build(s,PLAYER,t.id,type),{ok:false,error:'Requires a town or city.'});
  assert.deepEqual(s,before,'rejected construction must not spend resources or orders');
  for(const building of ['town','city']){
   t.building=building;assert.equal(buildCheck(s,PLAYER,t.id,type),null,`${type} level ${level+1} in a ${building}`);
  }
 }
 const s=createGame();stocked(s);const t=s.tiles['6,6'];Object.assign(t,{building:'fort',levels:{fort:1}});
 assert.equal(build(s,PLAYER,t.id,'barracks').ok,true);
});
test('new military units require their local tier and real horses and arms',()=>{
 const s=createGame();stocked(s);const home=s.tiles['5,6'];assert.match(recruit(s,PLAYER,home.id,'knight').error,/Knightly Hall/);
 home.stable=true;home.levels.stable=3;kingdom(s,PLAYER).resources.horses=0;assert.match(recruit(s,PLAYER,home.id,'knight').error,/horses/);
 kingdom(s,PLAYER).resources.horses=9;const arms=kingdom(s,PLAYER).resources.arms;assert.equal(recruit(s,PLAYER,home.id,'knight').ok,true);assert.equal(kingdom(s,PLAYER).resources.horses,6);assert.equal(kingdom(s,PLAYER).resources.arms,arms-9);
});
test('splitting and merging preserve all expanded troop classes',()=>{
 const s=createGame(),a=s.armies[0];a.units.knight=7;a.units.crossbow=9;const original={...a.units};assert.equal(splitArmy(s,PLAYER,a.id).ok,true);assert.equal(mergeArmies(s,PLAYER,a.tile).ok,true);assert.deepEqual(a.units,original);
});
test('formations enforce composition and siege slows stacks',()=>{
 const s=createGame(),a=s.armies[0];a.units.cavalry=0;assert.equal(setFormation(s,PLAYER,a.id,'flanking').ok,false);a.units.knight=2;assert.equal(setFormation(s,PLAYER,a.id,'flanking').ok,true);assert.equal(setFormation(s,PLAYER,a.id,'bogus').ok,false);
 const quick=army({knight:8});assert.equal(armySpeed(quick),5);quick.units.trebuchet=1;assert.equal(armySpeed(quick),1.5);
});
test('spearmen and spear wall reduce cavalry charge losses',()=>{
 const attack=()=>army({knight:30},'charge'),defend=()=>army({spearman:40},'balanced','wintermere');
 const normal=resolveFieldBattle(attack(),defend(),terrain()),spear=defend();spear.formation='spearWall';const prepared=resolveFieldBattle(attack(),spear,terrain());
 assert.ok(prepared.phases[2].loss[1]<normal.phases[2].loss[1]);
 const levy=resolveFieldBattle(attack(),army({levy:40},'balanced','wintermere'),terrain());assert.ok(levy.phases[2].loss[1]>normal.phases[2].loss[1]);
});
test('archers resolve before melee, hill defense matters, and forest limits charge and volleys',()=>{
 const a=()=>army({archer:40,levy:20}),d=()=>army({levy:60},'balanced','wintermere');
 const plain=resolveFieldBattle(a(),d(),terrain()),forest=resolveFieldBattle(a(),d(),terrain('forest')),hill=resolveFieldBattle(a(),d(),terrain('hills'));
 assert.deepEqual(plain.phases.map(p=>p.name),['Positioning','Missile Fire','Charge / Engagement','Main Melee','Flanking','Morale','Pursuit']);
 assert.ok(plain.phases[1].loss[1]>forest.phases[1].loss[1]);assert.ok(plain.phases[1].loss[1]>hill.phases[1].loss[1]);
});
test('each watchtower tier reduces actual battle casualties',()=>{
 const reports=[0,1,2,3].map(level=>resolveFieldBattle(army({archer:40,levy:40}),army({levy:80},'balanced','wintermere'),terrain('plains',{building:level?'watchtower':null,levels:{watchtower:level}})));
 for(let level=1;level<=3;level++){
  assert.ok(reports[level].phases[1].loss[1]<reports[level-1].phases[1].loss[1],`tier ${level} protects against volleys`);
  assert.ok(reports[level].casualties[1].levy<reports[level-1].casualties[1].levy,`tier ${level} reduces total defender losses`);
 }
});
test('watchtower protection contributes to the final battle outcome',()=>{
 const fight=building=>{
  const defender=army({levy:1},'balanced','wintermere');defender.morale=.75;
  return resolveFieldBattle(army({levy:1}),defender,terrain('plains',{owner:null,building,levels:{watchtower:3}}));
 };
 const open=fight(null),tower=fight('watchtower');
 assert.ok([...open.phases,...tower.phases].every(p=>p.loss.every(n=>n===0)),'isolate combat power from casualties');
 assert.equal(open.winner,0);assert.equal(tower.winner,1);
});
test('cavalry flanks and pursues routed armies; army losses and morale are recorded by class',()=>{
 // This force leaves opponents for the later phases; the old overwhelming
 // fixture now destroys every defender before flanking without casualty caps.
 const a=army({knight:10,lightCavalry:10},'flanking'),d=army({levy:60},'balanced','wintermere');d.morale=.4;
 const report=resolveFieldBattle(a,d,terrain());assert.ok(report.phases[4].loss[1]>0);assert.equal(report.routed,true);assert.ok(report.phases[6].loss[1]>0);assert.equal(report.casualties[1].levy,60-d.units.levy);assert.ok(d.morale<.4);assert.equal(d.retreats,1);
});
test('siege equipment differs against high walls and an empty fort can be occupied',()=>{
 const t=terrain('plains',{building:'fort',levels:{fort:3,wall:3},walls:180});assert.ok(siegePower(army({trebuchet:2}),t)>siegePower(army({catapult:2}),t));assert.ok(siegePower(army({catapult:2}),t)>siegePower(army({ram:2}),t));
 const s=createGame(),a=s.armies[0],target=s.tiles['6,6'];Object.assign(target,{owner:'wintermere',building:'fort',levels:{fort:3},walls:0,fortIntegrity:180});declareWar(s,PLAYER,'wintermere');a.path=[target.id];a.units.siege=0;resolveMovement(s);assert.equal(a.tile,'6,6');assert.equal(target.owner,PLAYER);assert.equal(target.fortIntegrity,180);
});
test('wall upgrades add persistent strength and can be repaired through construction',()=>{
 const s=createGame();stocked(s);const t=s.tiles['5,6'];t.walls=30;t.levels.wall=3;assert.equal(build(s,PLAYER,t.id,'wall').ok,true);assert.equal(t.project.repair,true);resolveEconomy(s);assert.equal(t.walls,30);resolveEconomy(s);assert.equal(t.walls,180);
});
test('need scheduler is bounded, respects per-House cooldown and never spends player resources',()=>{
 const s=createGame();stocked(s);kingdom(s,'wintermere').resources.iron=0;kingdom(s,'wintermere').resources.wood=550;kingdom(s,PLAYER).resources.iron=450;
 const initial=structuredClone(kingdom(s,PLAYER).resources);s.turn=2;scheduleTrade(s);assert.equal(s.commerce.offers.length,1);assert.equal(s.commerce.offers[0].from,'wintermere');assert.match(s.commerce.offers[0].reason,/iron/);assert.deepEqual(kingdom(s,PLAYER).resources,initial);
 scheduleTrade(s);assert.equal(s.commerce.offers.length,1);s.commerce.offers[0].status='declined';s.turn=4;scheduleTrade(s);assert.equal(s.commerce.offers.length,1);s.turn=6;scheduleTrade(s);assert.equal(s.commerce.offers.at(-1).created,6);
});
test('outposts expand capacity; multiple contracts remain atomic and expire on lost infrastructure',()=>{
 const s=createGame();stocked(s);const capacity=tradeInfrastructure(s,PLAYER).capacity;routes(s);assert.ok(tradeInfrastructure(s,PLAYER).capacity>capacity);
 assert.equal(commitDeal(s,'wintermere',supply).ok,true);assert.equal(commitDeal(s,'wintermere',{...supply,giveResource:'wood',giveAmount:15,receiveResource:'iron',receiveAmount:5}).ok,true);assert.equal(s.treaties.length,2);
 const a=kingdom(s,PLAYER),b=kingdom(s,'wintermere');const total=r=>a.resources[r]+b.resources[r];const sums=[total('gold'),total('food')];s.turn++;resolveRecurringTrade(s);assert.deepEqual([total('gold'),total('food')],sums);const stores=structuredClone(a.resources);resolveRecurringTrade(s);assert.deepEqual(a.resources,stores);
 s.tiles['6,6'].owner='thornwall';s.turn++;const prior=structuredClone(a.resources);resolveRecurringTrade(s);assert.deepEqual(a.resources,prior);assert.match(s.treaties[0].routeStatus,/captured/);assert.equal(s.treaties[0].disrupted,1);
 for(let i=0;i<2;i++){s.turn++;resolveRecurringTrade(s);}assert.equal(s.treaties[0].expires,s.turn);
});
test('an embargo blocks economic proposals and recurring deliveries',()=>{
 const s=createGame();routes(s);assert.equal(commitDeal(s,'wintermere',supply).ok,true);s.treaties.push({id:'ban',type:'embargo',parties:[PLAYER,'vesper'],targetId:'wintermere',expires:20});assert.equal(tradeRoute(s,PLAYER,'wintermere').safe,false);assert.equal(evaluateDeal(s,'wintermere',{...supply,type:'EXCHANGE'}).status,'reject');s.turn++;resolveRecurringTrade(s);assert.equal(s.treaties[0].expires,s.turn);
});
test('dependent imports use all suppliers in the denominator, enabling diversification',()=>{
 const s=createGame();for(const t of Object.values(s.tiles))if(t.owner==='thornwall'&&t.resource==='food')t.building=null;
 recordTrade(s,PLAYER,'thornwall','food',600);const one=economicRelationship(s,'thornwall',PLAYER).dependency;recordTrade(s,'redharbor','thornwall','food',600);const two=economicRelationship(s,'thornwall',PLAYER).dependency;assert.ok(two<one);assert.ok(two<=50);
});
test('AI plans and recruiting spend resources, manufactured resource goals enter needs, and rival trade conserves stocks',()=>{
 const s=createGame();stocked(s);s.turn=6;kingdom(s,'wintermere').resources.iron=0;kingdom(s,'wintermere').resources.wood=550;kingdom(s,'thornwall').resources.wood=0;
 const totals=()=>RESOURCES.map(r=>s.kingdoms.reduce((n,k)=>n+k.resources[r],0));const before=totals();aiResourceTrade(s);assert.deepEqual(totals(),before);assert.ok(s.diplomacy.tradeHistory.some(t=>t.kind==='ai-trade'));
 kingdom(s,'redharbor').resources.horses=0;assert.equal(economicNeeds(s,'redharbor').find(n=>n.resource==='horses').need,0,'healthy ranch production avoids unnecessary imports');for(const t of Object.values(s.tiles))if(t.owner==='redharbor'&&t.building==='ranch')t.building=null;assert.ok(economicNeeds(s,'redharbor').find(n=>n.resource==='horses').need>0);strategyTurn(s);for(const k of s.kingdoms)assert.ok(Object.values(k.resources).every(n=>n>=0&&Number.isFinite(n)));
});
test('version 2 saves retain old units, buildings, chats and treaties without receiving free materials',()=>{
 const s=createGame();s.version=2;s.conversations.wintermere=[{role:'player',text:'Our original bargain'}];
 delete s.commerce;for(const k of s.kingdoms)for(const r of ['horses','tools','arms'])delete k.resources[r];for(const t of Object.values(s.tiles)){delete t.levels;delete t.quality;delete t.region;}
 for(const a of s.armies){for(const u of Object.keys(a.units))if(!['levy','archer','cavalry','siege'].includes(u))delete a.units[u];delete a.formation;delete a.retreats;}
 const resumed=parseSave(JSON.stringify(s));assert.equal(resumed.version,SAVE_VERSION);assert.equal(resumed.armies[0].units.cavalry,2);assert.equal(resumed.armies[0].formation,'balanced');assert.equal(buildingLevel(resumed.tiles['6,6'],'farm'),1);assert.equal(resumed.kingdoms[0].resources.tools,0);assert.deepEqual(resumed.conversations,s.conversations);
});
test('new save fields reject invalid levels, formations, construction timers and unsafe offer content',()=>{
 for(const mutate of [s=>s.tiles['6,6'].levels.farm=9,s=>s.armies[0].formation='infinite',s=>s.tiles['6,6'].project={type:'farm',owner:PLAYER,level:2,total:99,remaining:2},s=>s.commerce.offers.push({id:1,from:'wintermere',created:1,expires:4,status:'pending',reason:'x',intent:{type:'WAR',giveResource:'gold',receiveResource:'food',giveAmount:1,receiveAmount:1,duration:2}})]){const s=createGame();mutate(s);assert.throws(()=>parseSave(JSON.stringify(s)));}
});
test('expanded simulation resumes deterministically and dialogue hides foreign treasuries',()=>{
 const s=createGame(78);for(let i=0;i<6;i++)endTurn(s);const r=parseSave(JSON.stringify(s));for(let i=0;i<5;i++){endTurn(s);endTurn(r);}assert.deepEqual(s,r);const context=makeContext(s,'wintermere','What are you building?');assert.equal(context.world.self.economicNeeds,undefined);assert.match(context.world.knowledge,/Fog of war/);assert.ok(context.world.houses.every(h=>!h.resources));
});
test('every remote catalog asset retains repository fallback artwork',async()=>{
 const paths=[...Object.values(ART.structures).flatMap(Object.values),...Object.values(ART.units),...Object.values(ART.resources),...Object.values(ART.titles),...ART.construction];
 for(const path of paths){const fallback=fallbackArtURL(path)||path;await access(new URL(fallback));const svg=await readFile(new URL(fallback),'utf8');assert.match(svg,/<svg/);assert.doesNotMatch(svg,/https?:\/\/(?!www.w3.org)/);}
 assert.ok(paths.length>100);
});

test('Royal Highway upgrades a complete owned corridor atomically and activates after construction',()=>{
 const s=createGame();stocked(s);const k=kingdom(s,PLAYER),end=s.tiles['7,6'];Object.assign(end,{owner:PLAYER,building:'town',terrain:'plains',road:true,levels:{town:1,road:1}});
 k.resources.tools=0;const before=JSON.stringify(s);assert.equal(buildHighway(s,PLAYER,'5,6',end.id).ok,false);assert.equal(JSON.stringify(s),before);
 k.resources.tools=400;const commands=k.commands,result=buildHighway(s,PLAYER,'5,6',end.id);assert.equal(result.ok,true);assert.equal(k.commands,commands-1);assert.equal(buildingLevel(s.tiles['6,6'],'road'),1);
 for(let i=0;i<5;i++)resolveEconomy(s);assert.equal(buildingLevel(s.tiles['5,6'],'road'),3);assert.equal(buildingLevel(end,'road'),3);
});
test('blocked routes pause both transfers and retain each side’s resources',()=>{
 const s=createGame();routes(s);assert.equal(commitDeal(s,'wintermere',supply).ok,true);
 for(const id of ['6,6','6,5','5,5','4,6','4,7','5,7'])s.tiles[id].terrain='water';
 assert.equal(tradeRoute(s,PLAYER,'wintermere').safe,false);const before=s.kingdoms.map(k=>({...k.resources}));s.turn++;resolveRecurringTrade(s);assert.deepEqual(s.kingdoms.map(k=>k.resources),before);assert.equal(s.treaties[0].disrupted,1);
});
test('gold purchases and trust-based supply variants obey authoritative validation',()=>{
 const s=createGame();routes(s);
 assert.equal(evaluateDeal(s,'wintermere',{...supply,type:'EXCHANGE',tradeKind:'purchase',giveResource:'wood'}).status,'reject');
 assert.match(evaluateDeal(s,'wintermere',{...supply,tradeKind:'strategic'}).reason,/30 trust/);
 kingdom(s,'wintermere').relations[PLAYER].trust=40;assert.equal(evaluateDeal(s,'wintermere',{...supply,tradeKind:'strategic'}).status,'accept');
 assert.equal(evaluateDeal(s,'wintermere',{...supply,tradeKind:'preferential'}).status,'reject');
});
