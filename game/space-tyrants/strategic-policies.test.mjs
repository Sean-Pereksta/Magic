import assert from 'node:assert/strict';
import test from 'node:test';
import {createSimulation} from './simulation-harness.mjs';

function peaceful(){const h=createSimulation();h.run('state.empires.forEach(e=>{e.credits=5000;e.relations={};});');return h}
function lab(){const h=peaceful();h.run(`state.testPlanet=playerWorlds()[0];Object.assign(state.testPlanet,{pop:1,capacity:3,unrest:0,localProject:null,orbitalProject:null,expansionProject:null,reconstruction:null,buildQueue:[],orders:[],infra:{mine:2,factory:2,training:2,shipyard:0,research:2,city:1,defense:1},stxEconomicFocus:'balanced'});for(const r of [...RESOURCES,'components','equipment'])state.testPlanet.stock[r]=1000;state.testPlanet.stock.trained=0;for(const r of RESOURCES){state.testPlanet.quality[r]=5;state.testPlanet.stxResourceYield[r]=1;state.testPlanet.reserve[r]=100000};state.testPlanet.stxResourcePrimary="iron";state.testPlanet.stxResourceSecondary="helium";`);return h}

test('broadcast contacts all living powers and staggers replies in simulation time',()=>{
  const h=peaceful(),r=h.run('stxRTRequestTrade("iron",80)');
  assert.equal(r.contacts.length,6);assert.equal(r.offerIds.length,0);
  assert.ok(r.contacts.every(c=>c.nextCheckAt>=5&&c.nextCheckAt<=30));
  assert.ok(new Set(r.contacts.map(c=>c.nextCheckAt)).size>1);
  h.run('renderTransmissions();renderTransmissions()');assert.equal(r.offerIds.length,0);
  h.run('state.simTime=31;stxRTBroadcastTick()');assert.ok(r.contacts.every(c=>c.responded||c.status==='ignored'));
});
test('an initially unable supplier reconsiders after gaining real production',()=>{
  const h=peaceful();h.run('owned(1).forEach(p=>p.stock.iron=0);state.testRequest=stxRTRequestTrade("iron",80);state.simTime=31;stxRTBroadcastTick()');
  assert.equal(h.run('state.testRequest.contacts.find(c=>c.from===1).status'),'unable');
  h.run('owned(1).forEach(p=>p.stock.iron=300);state.simTime=46;stxRTBroadcastTick()');
  assert.equal(h.run('state.testRequest.contacts.find(c=>c.from===1).status'),'offered');
  assert.equal(h.run('state.resourceTradeEconomy.offers.filter(o=>o.from===1).length'),1);
  h.run('stxRTBroadcastTick()');assert.equal(h.run('state.resourceTradeEconomy.offers.filter(o=>o.from===1).length'),1);
});
test('withdrawn surplus produces an explicit response and can be offered again later',()=>{
  const h=peaceful();offered(h);h.run('owned(1).forEach(p=>p.stock.iron=0);state.simTime=45;stxRTBroadcastTick()');
  assert.equal(h.run('state.testOffer.status'),'withdrawn');assert.equal(h.run('state.testRequest.contacts.find(c=>c.from===1).status'),'unable');
  h.run('owned(1).forEach(p=>p.stock.iron=1000);state.simTime=60;stxRTBroadcastTick()');
  assert.equal(h.run('state.testRequest.contacts.find(c=>c.from===1).status'),'offered');
  assert.equal(h.run('state.resourceTradeEconomy.offers.filter(o=>o.from===1&&o.status==="pending").length'),1);
});
test('small requests can be fulfilled and invalid amounts cannot create broadcasts',()=>{
  const h=peaceful();assert.equal(h.run('stxRTRequestTrade("iron",NaN)'),null);assert.equal(h.run('stxRTRequestTrade("iron",-5)'),null);
  h.run('owned(1).forEach(p=>p.stock.iron=1000);stxRTRequestTrade("iron",1);state.simTime=31;stxRTBroadcastTick();');
  assert.equal(h.run('state.resourceTradeEconomy.offers.find(o=>o.from===1).amount'),1);
});
test('poor relations decline, severed relations ignore, and inability has no penalty',()=>{
  const h=peaceful();h.run('empire(0).relations[1]=-.6;empire(0).relations[2]=-.95;owned(3).forEach(p=>p.stock.iron=0);state.testRequest=stxRTRequestTrade("iron",70);state.simTime=31;stxRTBroadcastTick()');
  assert.equal(h.run('state.testRequest.contacts.find(c=>c.from===1).status'),'declined');
  assert.equal(h.run('state.testRequest.contacts.find(c=>c.from===2).status'),'ignored');
  assert.equal(h.run('state.testRequest.contacts.find(c=>c.from===3).status'),'unable');
  assert.equal(h.run('relation(0,3)'),0);
});
function offered(h){h.run('owned(1).forEach(p=>p.stock.iron=1000);state.testRequest=stxRTRequestTrade("iron",80);state.simTime=31;stxRTBroadcastTick();state.testOffer=state.resourceTradeEconomy.offers.find(o=>o.from===1);state.testOffer.payment={credits:25};');}
test('accepted cargo deducts stock and credits but reaches the player only on arrival',()=>{
  const h=peaceful();offered(h);const before=h.run('({stock:empireResource(0,"iron"),seller:empireResource(1,"iron"),credits:empire(0).credits,amount:state.testOffer.amount})');
  assert.equal(h.run('stxRTAcceptOffer(state.testOffer.id)'),true);
  assert.equal(h.run('empireResource(0,"iron")'),before.stock);
  assert.equal(h.run('empireResource(1,"iron")'),before.seller-before.amount);
  assert.equal(h.run('empire(0).credits'),before.credits-25);
  h.run('state.ships.filter(s=>s.tradeOfferId===state.testOffer.id&&s.tradeLeg==="delivery").forEach(s=>arriveShip(s,state.planets.find(p=>p.id===s.to)))');
  assert.equal(h.run('empireResource(0,"iron")'),before.stock+before.amount);
});
test('barter dispatches both legs and rolls back a failed multi-vessel transaction',()=>{
  const h=peaceful();offered(h);h.run('playerWorlds().forEach(p=>p.stock.equipment=1000);state.testOffer.payment={resource:"equipment",amount:22};');
  const before=h.run('JSON.stringify({ships:state.ships,iron:empireResource(1,"iron"),equipment:empireResource(0,"equipment")})');
  h.run('state.originalCreateShip=createShip;state.createCalls=0;createShip=function(...args){return ++state.createCalls===2?null:state.originalCreateShip(...args)}');
  assert.equal(h.run('stxRTAcceptOffer(state.testOffer.id)'),false);
  assert.equal(h.run('JSON.stringify({ships:state.ships,iron:empireResource(1,"iron"),equipment:empireResource(0,"equipment")})'),before);
  h.run('createShip=state.originalCreateShip');assert.equal(h.run('stxRTAcceptOffer(state.testOffer.id)'),true);
  assert.ok(h.run('state.ships.some(s=>s.tradeLeg==="payment"&&s.cargo.equipment===22)'));
  assert.ok(h.run('state.ships.some(s=>s.tradeLeg==="delivery"&&s.cargo.iron>0)'));
});
test('expiry closes the conversation once and blocks stale or embargoed acceptance',()=>{
  const h=peaceful();offered(h);h.run('stxGBImposeEmbargo(1,0,"test")');assert.equal(h.run('stxRTAcceptOffer(state.testOffer.id)'),false);
  h.run('state.simTime=96;stxRTExpire()');assert.equal(h.run('state.testRequest.status'),'expired');
  const n=h.run('state.news.filter(n=>n.title==="RESOURCE REQUEST CLOSED").length');h.run('stxRTExpire()');assert.equal(h.run('state.news.filter(n=>n.title==="RESOURCE REQUEST CLOSED").length'),n);
  assert.equal(h.run('stxRTAcceptOffer(state.testOffer.id)'),false);
});
test('save and load preserve pending response times and active policy duration',()=>{
  const h=lab();h.run('stxPolicyActivate(0,"mining-iron");stxRTRequestTrade("iron",75);state.simTime=8;saveGame(false)');
  const before=h.run('JSON.stringify({request:state.resourceTradeEconomy.requests[0],active:stxPolicyActive(0)})');
  assert.equal(h.run('loadGame()'),true);
  assert.equal(h.run('JSON.stringify({request:state.resourceTradeEconomy.requests[0],active:stxPolicyActive(0)})'),before);
  h.run('state.simTime=31;stxRTBroadcastTick()');assert.ok(h.run('state.resourceTradeEconomy.requests[0].contacts.some(c=>c.responded)'));
});
test('all normal recipes are 65–80% raw materials and preserve distinct identities',()=>{
  const h=lab(),recipes=h.run('STX_PS_RECIPES');
  for(const [kind,recipe] of Object.entries(recipes)){const raw=Object.entries(recipe).filter(([r])=>r!=='components'&&r!=='equipment').reduce((n,[,v])=>n+v,0);assert.ok(raw>=.65-1e-9&&raw<=.8+1e-9,`${kind}: ${raw}`);assert.ok(recipe.components>0&&recipe.equipment>0)}
  assert.ok(recipes.city.silicates>recipes.city.components);assert.ok(recipes.research.rare>recipes.research.iron||!recipes.research.iron);
  const ships=h.run('STX_RT_SHIP_RECIPES');assert.ok(ships.fleet.iron+ships.fleet.helium+ships.fleet.titanium>ships.fleet.components+ships.fleet.equipment);assert.ok(ships.tanker.helium>ships.freighter.helium);assert.ok(ships.research.rare>ships.fleet.rare);
});
test('paid goods survive legacy recipe migration and new projects consume real Iron',()=>{
  const h=lab();h.run('state.testPlanet.localProject={type:"city",cost:100,progress:0,startedAt:0};state.testPlanet.stock.iron=20;state.testPlanet.stock.components=500;stxSDAllocatePlanet(state.testPlanet)');
  assert.equal(h.run('state.testPlanet.stock.iron'),0);
  assert.ok(h.run('stxSDRemaining(stxSDDescriptors(state.testPlanet)[0],"iron")>10'));
  h.run('state.testPlanet.localProject={type:"city",cost:100,progress:.3,stxSupply:{delivered:{components:38},orderIds:{}}};stxSDDescriptors(state.testPlanet)');
  assert.equal(h.run('state.testPlanet.localProject.stxSupply.delivered.components'),38);
});
test('actual command hand has four choices at seven worlds and five at eight',()=>{
  const h=lab();h.run('state.planets.filter(p=>p.owner===null).slice(0,7-playerWorlds().length).forEach(p=>p.owner=0);openCommandPhase()');
  assert.equal(h.run('playerWorlds().length'),7);assert.equal(h.run('state.commandChoices.length'),4);
  h.run('$("commandModal").hidden=true;state.planets.find(p=>p.owner===null).owner=0;openCommandPhase()');
  assert.equal(h.run('state.commandChoices.length'),5);assert.ok(h.run('state.commandChoices.every(c=>typeof c.apply==="function")'));
});
test('war controls and embargo action coexist with the ordinary mandate quota',()=>{
  const h=lab();h.run('state.planets.filter(p=>p.owner===null).slice(0,7).forEach(p=>p.owner=0);declareWar(0,1,"test");stxGBImposeEmbargo(2,0,"test");openCommandPhase()');
  assert.equal(h.run('state.commandChoices.filter(c=>c.stxFleetOrderKind!=="breakEmbargo").length'),5);
  assert.equal(h.run('state.commandChoices.filter(c=>c.stxFleetOrderKind==="invade").length'),1);
  assert.equal(h.run('state.commandChoices.filter(c=>c.stxFleetOrderKind==="concentrate").length'),1);
  assert.equal(h.run('state.commandChoices.filter(c=>c.stxFleetOrderKind==="breakEmbargo").length'),1);
});
test('issuing an immediate mandate never creates fallback construction',()=>{
  const h=lab();h.run('state.commandChoices=[stxPolicyChoice("crew-training")];state.commandSelected=new Set([0]);');
  const before=h.run('playerWorlds().filter(p=>p.localProject||p.orbitalProject||p.buildQueue.length).length');
  h.run('issueCommands()');assert.ok(h.run('stxPolicyHas(0,"crew-training")'));
  assert.equal(h.run('playerWorlds().filter(p=>p.localProject||p.orbitalProject||p.buildQueue.length).length'),before);
});
test('crew mandate increases real recruitment by 60% and consumes more Equipment',()=>{
  const a=lab(),b=lab();b.run('stxPolicyActivate(0,"crew-training")');
  for(const h of [a,b])h.run('state.testPlanet.infra.factory=0;state.testPlanet.infra.research=0;tickPlanet(state.testPlanet,1)');
  const base=a.run('state.testPlanet.stxCrewRate'),fast=b.run('state.testPlanet.stxCrewRate');
  assert.ok(fast/base>1.59&&fast/base<1.61);assert.ok(b.run('state.testPlanet.stock.equipment')<a.run('state.testPlanet.stock.equipment'));
  assert.ok(b.run('modifier(empire(0),"growth")')<1);
});
test('growth, mining specialization and Equipment-limited surge affect actual production',()=>{
  const base=lab(),growth=lab(),mining=lab(),empty=lab();
  growth.run('stxPolicyActivate(0,"population-expansion")');
  mining.run('stxPolicyActivate(0,"mining-iron")');empty.run('stxPolicyActivate(0,"mining-surge");state.testPlanet.stock.equipment=0');
  for(const h of [base,growth,mining,empty])h.run('state.testPlanet.infra.factory=0;state.testPlanet.infra.training=0;state.testPlanet.infra.research=0;tickPlanet(state.testPlanet,1)');
  assert.ok(growth.run('state.testPlanet.pop')>base.run('state.testPlanet.pop'));
  assert.ok(mining.run('state.testPlanet.stock.iron')>base.run('state.testPlanet.stock.iron'));
  assert.ok(mining.run('state.testPlanet.stock.silicates')<base.run('state.testPlanet.stock.silicates'));
  assert.equal(empty.run('state.testPlanet.stock.iron'),base.run('state.testPlanet.stock.iron'));
});
test('manufacturing policies alter both outputs and need extra inputs',()=>{
  const a=lab(),b=lab();b.run('stxPolicyActivate(0,"component-drive")');
  for(const h of [a,b])h.run('state.testPlanet.factoryEfficiency=1;stxRTProduceFactory(state.testPlanet,1)');
  assert.ok(b.run('state.testPlanet.stock.components')>a.run('state.testPlanet.stock.components'));
  assert.ok(b.run('state.testPlanet.stock.equipment')<a.run('state.testPlanet.stock.equipment'));
  assert.ok(b.run('state.testPlanet.stock.iron')<a.run('state.testPlanet.stock.iron'));
  const c=lab();c.run('state.testPlanet.factoryEfficiency=1;state.testPlanet.stock.titanium=0;stxRTProduceFactory(state.testPlanet,1)');
  assert.equal(c.run('state.testPlanet.stock.equipment'),1000);assert.ok(c.run('state.testPlanet.stock.components')>1000);
});
test('policies replace their category, refresh without stacking, and expire',()=>{
  const h=lab();h.run('stxPolicyActivate(0,"component-drive");stxPolicyActivate(0,"component-drive")');assert.equal(h.run('stxPolicyValue(0,"components")'),1.7);
  h.run('stxPolicyActivate(0,"equipment-drive");stxPolicyActivate(0,"mining-surge")');assert.equal(h.run('stxPolicyActive(0).length'),2);assert.equal(h.run('stxPolicyValue(0,"components")'),.65);
  h.run('state.simTime=200;stxPolicyTick(0)');assert.equal(h.run('stxPolicyActive(0).length'),0);assert.equal(h.run('stxPolicyValue(0,"components")'),1);
});
test('frontier resettlement conserves people and creates visible migrant cargo',()=>{
  const h=lab();h.run('state.testPlanet.pop=2.7;state.testTarget=state.planets.find(p=>p.owner===null);Object.assign(state.testTarget,{owner:0,pop:.02,capacity:1});state.testPop=totalPop(0);stxPolicyActivate(0,"frontier-resettlement")');
  assert.ok(h.run('state.ships.some(s=>s.stxPolicyMigration&&s.cargo.population>0)'));
  assert.ok(Math.abs(h.run('totalPop(0)+state.ships.filter(s=>s.stxPolicyMigration).reduce((n,s)=>n+s.cargo.population,0)-state.testPop'))<1e-10);
});
test('veteran recall transfers civilians into crew and cannot be spammed',()=>{
  const h=lab(),before=h.run('state.testPlanet.pop+state.testPlanet.stock.trained');assert.equal(h.run('stxPolicyActivate(0,"veteran-recall")'),true);
  assert.equal(h.run('state.testPlanet.pop+state.testPlanet.stock.trained'),before);assert.ok(h.run('state.testPlanet.stock.trained')>0);assert.equal(h.run('stxPolicyActivate(0,"veteran-recall")'),false);
});
test('freight speed changes immediately and returns to baseline after expiry',()=>{
  const h=lab();h.run('state.testShip=createShip("freighter",state.testPlanet,owned(1)[0],0,{cargo:{iron:10}});state.testSpeed=state.testShip.speed;stxPolicyActivate(0,"emergency-freight")');
  assert.ok(Math.abs(h.run('state.testShip.speed/state.testSpeed')-1.5)<1e-10);
  h.run('state.simTime=80;tickShips(.01)');assert.ok(Math.abs(h.run('state.testShip.speed/state.testSpeed')-1)<1e-10);
});
test('naval policy accelerates an existing supplied queue without ordering a yard',()=>{
  const a=lab(),b=lab();for(const h of [a,b])h.run('state.testPlanet.infra.shipyard=1;state.testPlanet.factoryEfficiency=1;state.testPlanet.buildQueue=[{type:"fleet",need:{trained:.009},progress:0}];stxSDDescriptors(state.testPlanet);state.testPlanet.stock.trained=1;stxSDAllocatePlanet(state.testPlanet)');
  b.run('stxPolicyActivate(0,"naval-construction")');for(const h of [a,b])h.run('tickBuildQueue(state.testPlanet,1)');
  assert.ok(b.run('state.testPlanet.buildQueue[0].progress')>a.run('state.testPlanet.buildQueue[0].progress')*1.49);assert.equal(b.run('state.testPlanet.localProject'),null);
});
test('player project priority outranks policies and receives foreign shipments',()=>{
  const h=lab();h.run('state.testPlanet.localProject={type:"shipyard",cost:100,progress:0};state.testDesc=stxSDDescriptors(state.testPlanet)[0];empire(0).stxPriorityProjectId=state.testDesc.id;empire(0).stxPriorityPlanetId=state.testPlanet.id;stxPolicyActivate(0,"strategic-supply")');
  assert.equal(h.run('stxSDDescriptors(state.testPlanet)[0].priority'),100);assert.equal(h.run('stxRTDestination(0,"iron").id'),h.run('state.testPlanet.id'));
});
test('research policies advance technology using real local Rare Earth and Equipment',()=>{
  const a=lab(),b=lab();b.run('stxPolicyActivate(0,"scientific-mobilization")');for(const h of [a,b])h.run('stxPolicyResearch(state.testPlanet,10)');
  assert.ok(b.run('state.testPlanet.stxResearchRate')>a.run('state.testPlanet.stxResearchRate')*1.49);assert.ok(b.run('state.testPlanet.stock.rare')<a.run('state.testPlanet.stock.rare'));
});
test('shortage context raises the appropriate policy score and AI uses the same paid policies',()=>{
  const h=lab();h.run('state.testPlanet.stock.iron=0;state.testPlanet.localProject={type:"factory",cost:180,progress:0};');
  assert.ok(h.run('stxPolicyScore(0,"mining-iron",stxPolicyContext(0))>stxPolicyScore(0,"mining-helium",stxPolicyContext(0))'));
  h.run('stxPolicyState(empire(1)).nextAIAt=0;state.testCredits=empire(1).credits;stxPolicyTick(0)');assert.ok(h.run('stxPolicyActive(1).length')>0);assert.ok(h.run('empire(1).credits<state.testCredits'));
});
test('conquest hides defeated rival cards without removing fleet owner identities',()=>{
  const h=peaceful();h.run('state.testFleet=registerFleet(1,owned(1)[0],30,"fleet");state.testColor=empire(1).color;owned(1).forEach(p=>p.owner=0);stxGBEliminateEmpire(1,0);renderRivals();renderFleetRegistry()');
  assert.equal(h.run('state.empires.length'),7);assert.equal(h.run('empire(1).color'),h.run('state.testColor'));
  assert.equal(h.run('stxVisibleRivals().some(e=>e.id===1)'),false);assert.ok(h.elements.get('fleetGrid').innerHTML.includes(h.run('state.testFleet.name')));
  assert.ok(h.elements.get('fleetGrid').innerHTML.includes(h.run('state.testColor')));
  h.run('saveGame(false);loadGame();renderRivals()');assert.equal(h.run('empire(6).id'),6);
});
test('orphaned historical fleet data cannot crash the registry',()=>{
  const h=peaceful();h.run('state.fleets.push({id:"old",name:"Historical fleet",owner:99,destroyed:true,admiral:null});renderFleetRegistry()');assert.ok(h.elements.get('fleetGrid').innerHTML.includes('Historical fleet'));
});
test('integrated simulation stays finite through policy expiry, AI decisions and requests',()=>{
  const h=lab();h.run('stxPolicyActivate(0,"mining-surge");stxRTRequestTrade("iron",100);for(let i=0;i<240;i++)simulate(.5)');
  assert.ok(h.run('state.planets.every(p=>Number.isFinite(p.pop)&&Object.values(p.stock).every(n=>Number.isFinite(n)&&n>=-1e-8))'));
  assert.ok(h.run('state.ships.length<=280'));assert.ok(h.run('state.resourceTradeEconomy.requests[0].status!=="open"'));
});
