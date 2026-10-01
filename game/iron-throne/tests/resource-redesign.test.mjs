import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame as legacyGame } from './fixtures/legacy-game.mjs';
import { economyProjection, resolveEconomy, kingdom, parseSave, recruit, build, createGame } from '../core.mjs';
import { RESOURCES, UNITS, TERRAINS, BUILDINGS } from '../data.mjs';
import { economicNeeds, packageValue, contractCheck } from '../trade-economy.mjs';
import { validateIntent, describeIntent, commitDeal, evaluateDeal, resolveRecurringTrade } from '../diplomacy.mjs';
import { parseBarter } from '../trade-negotiation.mjs';
import { normalizeItems, tradeItems, itemCost, withItems } from '../trade-package.mjs';
import { foodSecurity } from '../food-security.mjs';
import { calculatePopulationChange } from '../population.mjs';
import { tileProduction } from '../economy.mjs';
import { foundAIKingdoms } from '../founding.mjs';
import { tradeCandidate, scheduleTrade, aiResourceTrade } from '../trade.mjs';
const wealth=s=>RESOURCES.map(r=>s.kingdoms.reduce((n,k)=>n+k.resources[r],0));
const offer=(giveItems,receiveItems,extra={})=>validateIntent({type:'EXCHANGE',giveItems,receiveItems,...extra});
const stock=s=>{for(const k of s.kingdoms)for(const r of RESOURCES)k.resources[r]=500;};

test('food pressure includes civilians, mounted troops, settlements, ships and cargo without army splitting discounts',()=>{
 const s=legacyGame(),before=economyProjection(s,'ashen').food,k=kingdom(s,'ashen');
 assert.equal(before.consumption,before.population+before.armies+before.settlements+before.crews);
 k.population+=16;assert.equal(economyProjection(s,'ashen').food.population,before.population+2);
 const a=s.armies.find(a=>a.owner==='ashen');a.units.cavalry+=4;assert.equal(economyProjection(s,'ashen').food.armies,before.armies+2);
 const x=Object.values(s.tiles).find(t=>!t.building&&t.owner==='ashen');Object.assign(x,{building:'town',levels:{town:1}});
 assert.equal(economyProjection(s,'ashen').food.settlements,before.settlements+5);
 s.fleets.push({id:'test-fleet',owner:'ashen',tile:a.tile,cargo:[{...a,id:'cargo',units:{levy:8}}],ships:[{crew:4,type:'transport'}]});
 assert.equal(economyProjection(s,'ashen').food.crews,1);assert.equal(economyProjection(s,'ashen').food.armies,before.armies+4);
 const snapshot=economyProjection(s,'ashen').food.armies;a.units.levy-=2;s.armies.push({...a,id:'split',units:{levy:2}});assert.equal(economyProjection(s,'ashen').food.armies,snapshot);
});
test('food status distinguishes reserves, declining flow and exhaustion; all troop types and town costs use food',()=>{
 assert.deepEqual([foodSecurity(200,10,20),foodSecurity(100,2,20),foodSecurity(100,-5,20),foodSecurity(20,-10,20),foodSecurity(2,-10,20)],['Abundant','Stable','Strained','Shortage','Famine']);
 assert.ok(Object.values(UNITS).every(u=>u.cost.food>0));assert.ok(BUILDINGS.town.cost.food>0);
 const s=legacyGame(),k=kingdom(s,'ashen'),before=k.resources.food;assert.ok(recruit(s,'ashen','5,6','archer').ok);assert.equal(k.resources.food,before-UNITS.archer.cost.food);
 k.population=240;k.resources.food=100;const projection=economyProjection(s,k.id);assert.equal(calculatePopulationChange(s,k.id,projection.income).foodStatus,projection.food.status);
});
test('duplicate package rows merge, malformed and over-limit amounts reject, and unequal sides describe every item',()=>{
 assert.deepEqual(normalizeItems([{resource:'iron',amount:15},{resource:'iron',amount:5}]),[{resource:'iron',amount:20}]);
 for(const bad of [[],[{resource:'iron',amount:-1}],[{resource:'iron',amount:1.5}],[{resource:'unknown',amount:1}],[{resource:'iron',amount:900},{resource:'iron',amount:200}]])assert.equal(normalizeItems(bad),null);
 const i=offer([{resource:'iron',amount:15},{resource:'wood',amount:30}],[{resource:'food',amount:100}]);assert.ok(i);assert.match(describeIntent(i),/30 wood \+ 15 iron/);assert.match(describeIntent(i),/100 food/);
 assert.equal(validateIntent({type:'AID',giveItems:i.giveItems}),null);
 assert.deepEqual(parseBarter('I give 15 iron and 30 wood for 100 food').giveItems,i.giveItems);
 assert.equal(parseBarter('I might give 15 iron and 30 wood for 100 food'),null);
});
test('whole package evaluation can accept combined resources where one item is insufficient',()=>{
 const s=legacyGame();stock(s);const k=kingdom(s,'wintermere');k.resources.food=500;k.resources.wood=5;k.resources.iron=5;
 const receive=[{resource:'food',amount:100}],one=offer([{resource:'iron',amount:1}],receive),bundle=offer([{resource:'iron',amount:15},{resource:'wood',amount:30}],receive);
 assert.notEqual(evaluateDeal(s,k.id,one).status,'accept');assert.equal(evaluateDeal(s,k.id,bundle).status,'accept');
 const before=wealth(s);assert.ok(commitDeal(s,k.id,bundle).ok);assert.deepEqual(wealth(s),before);
 const snapshot=JSON.stringify(s),poor=offer([{resource:'gold',amount:10},{resource:'arms',amount:1000}],receive);assert.equal(commitDeal(s,k.id,poor).ok,false);assert.equal(JSON.stringify(s),snapshot);
});
test('planned costs and obligations protect real surplus and divergent geography gives kingdom-specific values',()=>{
 const s=legacyGame(),k=kingdom(s,'thornwall');k.resources.iron=100;k.economicPlan={type:'workshop',tile:'31,5'};
 s.treaties.push({id:'committed',type:'recurring',parties:[k.id,'ashen'],payer:k.id,expires:s.turn+3,intent:{type:'RECURRING',giveResource:'iron',giveAmount:10,receiveResource:'food',receiveAmount:4}});
 const n=economicNeeds(s,k.id).find(n=>n.resource==='iron');assert.equal(n.planned,25);assert.equal(n.obligations,30);assert.equal(n.surplus,25);
 k.resources.food=30;for(const t of Object.values(s.tiles))if(t.owner===k.id&&t.building==='farm')t.building=null;
 const theirs=economicNeeds(s,k.id),other=economicNeeds(s,'wintermere');assert.ok(theirs.find(n=>n.resource==='food').value>other.find(n=>n.resource==='food').value);
 kingdom(s,'ashen').resources.gold=500;const v=evaluateDeal(s,k.id,offer([{resource:'gold',amount:400}],[{resource:'food',amount:20}]));assert.notEqual(v.status,'accept');assert.match(v.reason,/reserves|projected|supplies/);
});
test('recurring multi-resource shipments are atomic, capacity counts the full package and repeat resolution cannot pay twice',()=>{
 const s=legacyGame();stock(s);const i=offer([{resource:'iron',amount:2},{resource:'wood',amount:3}],[{resource:'food',amount:5}],{type:'RECURRING',duration:3});
 s.treaties.push({id:'old-contract',type:'recurring',parties:['ashen','wintermere'],payer:'ashen',expires:10,lastPaid:1,legacyRoute:true,intent:i});s.turn=2;
 const before=wealth(s),iron=kingdom(s,'ashen').resources.iron;resolveRecurringTrade(s);assert.equal(kingdom(s,'ashen').resources.iron,iron-2);assert.deepEqual(wealth(s),before);
 const once=JSON.stringify(s);resolveRecurringTrade(s);assert.equal(JSON.stringify(s),once);
 s.turn++;kingdom(s,'ashen').resources.wood=0;const resources=s.kingdoms.map(k=>({...k.resources}));resolveRecurringTrade(s);assert.deepEqual(s.kingdoms.map(k=>k.resources),resources);
 const large=offer([{resource:'wood',amount:8},{resource:'iron',amount:8}],[{resource:'food',amount:4}],{type:'RECURRING'});assert.match(contractCheck(legacyGame(),'ashen','wintermere',large),/capacity/);
});
test('legacy single-item contracts migrate to arrays and canonical packages resume without changes',()=>{
 const s=legacyGame();s.treaties.push({id:'legacy',type:'recurring',parties:['ashen','wintermere'],payer:'ashen',expires:10,lastPaid:1,intent:{type:'RECURRING',giveResource:'iron',giveAmount:2,receiveResource:'food',receiveAmount:5,duration:3}});
 const loaded=parseSave(JSON.stringify(s));assert.deepEqual(loaded.treaties[0].intent.giveItems,[{resource:'iron',amount:2}]);assert.deepEqual(parseSave(JSON.stringify(loaded)),loaded);
 loaded.treaties[0].intent.giveItems[0].amount=-1;assert.throws(()=>parseSave(JSON.stringify(loaded)),/trade/);
});
test('AI proposes real needs with multiple items, preserves both kingdoms reserves and throttles declined offers',()=>{
 const s=legacyGame();stock(s);s.turn=6;const k=kingdom(s,'wintermere');k.resources.iron=0;k.resources.stone=0;
 for(const t of Object.values(s.tiles))if(t.owner===k.id&&['mine','quarry'].includes(t.building))t.building=null;
 const candidate=tradeCandidate(s,k.id,'ashen');assert.ok(candidate);assert.equal(candidate.intent.giveItems.length,2);assert.match(candidate.reason,/iron|stone/);
 const before=wealth(s),player={...kingdom(s,'ashen').resources};scheduleTrade(s);assert.deepEqual(kingdom(s,'ashen').resources,player);const count=s.commerce.offers.length;scheduleTrade(s);assert.equal(s.commerce.offers.length,count);
 s.commerce.offers[0].status='declined';s.turn++;scheduleTrade(s);assert.equal(s.commerce.offers.length,count);
 s.turn=9;aiResourceTrade(s);assert.deepEqual(wealth(s),before);
});
test('desert regions are contiguous, retain playable movement and starting industries follow geography with positive food',()=>{
 const s=createGame(42,'heartlands'),deserts=Object.values(s.tiles).filter(t=>t.terrain==='desert');assert.ok(deserts.length>20);assert.equal(TERRAINS.desert.cost,1);
 const dry={terrain:'desert',resource:null,quality:'normal',building:'farm',levels:{farm:1}},oasis={...dry,river:true,resource:'food',quality:'rich'};assert.ok(tileProduction(oasis,'ashen').food>tileProduction(dry,'ashen').food);
 s.controllers=Object.fromEntries(s.kingdoms.map(k=>[k.id,{kind:'ai'}]));assert.ok(foundAIKingdoms(s).ok);
 const sets=s.kingdoms.map(k=>Object.values(s.tiles).filter(t=>t.owner===k.id&&['lumber','quarry','mine','ranch'].includes(t.building)).map(t=>t.building).sort().join(','));assert.ok(new Set(sets).size>1);
 for(const k of s.kingdoms)assert.ok(economyProjection(s,k.id).food.net>=5);
 assert.deepEqual(parseSave(JSON.stringify(s)),s);
});
