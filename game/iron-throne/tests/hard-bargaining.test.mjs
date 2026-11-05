import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { kingdom, relation } from '../core.mjs';
import { RESOURCES } from '../data.mjs';
import { planningView } from '../ai-knowledge.mjs';
import { refreshKnowledge } from '../fog.mjs';
import { economicNeeds, buyerTradeTerms, referencePackageValue, availableTradeAmount } from '../trade-economy.mjs';
import { tradeCandidate, aiResourceTrade } from '../trade.mjs';
import { tradeItems, withItems } from '../trade-package.mjs';
import { evaluateDeal, commitDeal } from '../diplomacy.mjs';
import { createPlan } from '../plans.mjs';

function market(){
  const s=createGame();s.difficulty='hard';
  for(const k of s.kingdoms)for(const r of RESOURCES)k.resources[r]=500;
  const buyer=kingdom(s,'wintermere');buyer.resources.iron=10;buyer.resources.wood=600;
  for(const t of Object.values(s.tiles))if(t.owner==='wintermere'&&t.building==='mine'){t.building=null;t.levels.mine=0;}
  refreshKnowledge(s);
  return s;
}
const terms=(s,items=[{resource:'iron',amount:8}])=>buyerTradeTerms(s,'wintermere','ashen',items,economicNeeds(s,'wintermere'),planningView(s,'wintermere'));
const exchange=(iron,wood)=>withItems({type:'EXCHANGE',duration:2,tradeKind:'immediate'},[{resource:'iron',amount:iron}],[{resource:'wood',amount:wood}]);

test('ordinary Hard first bids ask for one 8–12 unit shipment near 75–85% reference value',()=>{
  const s=market(),offer=tradeCandidate(s,'wintermere','ashen');
  assert.ok(offer);
  const requested=tradeItems(offer.intent,'give'),payment=tradeItems(offer.intent,'receive');
  assert.equal(requested.length,1);assert.equal(requested[0].resource,'iron');
  assert.ok(requested[0].amount>=8&&requested[0].amount<=12);
  const ratio=referencePackageValue(payment)/referencePackageValue(requested);
  assert.ok(ratio>=.74&&ratio<=.86,`reference price ${ratio}`);
  assert.equal(evaluateDeal(s,'wintermere',offer.intent).status,'accept');
  assert.doesNotMatch(JSON.stringify(offer),/maximumValue|openingRate/);
});

test('whole tools, arms and horses use the nearest valid small shipment without overbidding',()=>{
  for(const [resource,expected] of [['horses',[8,12]],['tools',[9,9]],['arms',[10,10]]])for(const [index,paranoia] of [1,0].entries()){
    const s=market(),buyer=kingdom(s,'wintermere');buyer.paranoia=paranoia;buyer.greed=.5;
    for(const t of Object.values(s.tiles))if(t.owner===buyer.id&&t.building==='farm'){t.building=null;t.levels.farm=0;}
    for(const n of economicNeeds(s,buyer.id,{tradeReserves:true}))buyer.resources[n.resource]=Math.ceil(n.goal+Math.max(0,-n.production*3));
    const food=economicNeeds(s,buyer.id,{tradeReserves:true}).find(n=>n.resource==='food');
    buyer.resources.food=food.goal-food.production*3-8;buyer.resources[resource]=500;
    refreshKnowledge(s);
    const offer=tradeCandidate(s,buyer.id,'ashen');assert.ok(offer);
    const requested=tradeItems(offer.intent,'give'),payment=tradeItems(offer.intent,'receive');
    assert.deepEqual(requested,[{resource:'food',amount:expected[index]}]);
    assert.equal(payment.length,1);assert.equal(payment[0].resource,resource);
    const value=referencePackageValue(payment),ratio=value/referencePackageValue(requested);
    assert.ok(ratio>=.75&&ratio<=.85,`${resource} opening ratio ${ratio}`);
    const needs=economicNeeds(s,buyer.id,{tradeReserves:true});
    assert.ok(value<=buyerTradeTerms(s,buyer.id,'ashen',requested,needs,planningView(s,buyer.id)).maximumValue);
    assert.ok(payment[0].amount<=needs.find(n=>n.resource===resource).surplus);
    assert.equal(evaluateDeal(s,buyer.id,offer.intent).status,'accept');
  }
});

test('a Hard buyer skips an opening when its protected surplus cannot fund a fair small shipment',()=>{
  const s=market(),buyer=kingdom(s,'wintermere');
  for(const n of economicNeeds(s,buyer.id,{tradeReserves:true}))buyer.resources[n.resource]=Math.ceil(n.goal+Math.max(0,-n.production*3));
  buyer.resources.iron=0;buyer.resources.food+=6;
  assert.equal(economicNeeds(s,buyer.id,{tradeReserves:true}).find(n=>n.resource==='food').surplus,6);
  assert.equal(tradeCandidate(s,buyer.id,'ashen'),null);
});

test('a Hard buyer can combine limited exports into a valid opening without spending its reserves',()=>{
  const s=market(),buyer=kingdom(s,'wintermere');buyer.paranoia=1;
  for(const t of Object.values(s.tiles))if(t.owner===buyer.id&&t.building==='farm'){t.building=null;t.levels.farm=0;}
  for(const n of economicNeeds(s,buyer.id,{tradeReserves:true}))buyer.resources[n.resource]=Math.ceil(n.goal+Math.max(0,-n.production*3));
  buyer.resources.food-=8;buyer.resources.stone+=6;buyer.resources.gold+=6;refreshKnowledge(s);
  const offer=tradeCandidate(s,buyer.id,'ashen');assert.ok(offer);
  assert.deepEqual(tradeItems(offer.intent,'give'),[{resource:'food',amount:8}]);
  const payment=tradeItems(offer.intent,'receive');assert.equal(payment.length,2);
  assert.ok(payment.every(item=>['stone','gold'].includes(item.resource)&&item.amount<=3));
  assert.equal(referencePackageValue(payment),6);
  assert.equal(evaluateDeal(s,buyer.id,offer.intent).status,'accept');
});

test('a credible counter earns a bounded concession and repeated declines do not increase it',()=>{
  const s=market(),opening=tradeCandidate(s,'wintermere','ashen'),wanted=tradeItems(opening.intent,'give')[0].amount;
  const price=tradeItems(opening.intent,'receive').find(x=>x.resource==='wood').amount;
  const counter=evaluateDeal(s,'wintermere',exchange(wanted,price+8));
  assert.equal(counter.status,'counter');
  assert.ok(counter.counter.receiveAmount>price);
  assert.ok(counter.counter.receiveAmount<price+8);
  assert.equal(evaluateDeal(s,'wintermere',counter.counter).status,'accept');
  for(let attempt=0;attempt<8;attempt++){
    s.commerce.offers.push({from:'wintermere',to:'ashen',status:'declined',intent:opening.intent,created:s.turn,expires:s.turn+3});
    assert.deepEqual(tradeCandidate(s,'wintermere','ashen'),opening);
    assert.deepEqual(evaluateDeal(s,'wintermere',exchange(wanted,price+8)),counter);
  }
});

test('urgent shortage at war raises the private ceiling, known alternative supply lowers it',()=>{
  const s=market(),ordinary=terms(s).maximumValue;
  kingdom(s,'wintermere').resources.iron=0;s.wars.push('thornwall:wintermere');
  assert.ok(terms(s).maximumValue>ordinary);
  const known=planningView(s,'wintermere'),needs=economicNeeds(s,'wintermere'),isolated=structuredClone(known);
  for(const t of Object.values(isolated.tiles))if(t.owner&&!['wintermere','ashen'].includes(t.owner))t.fog='unknown';
  const a=buyerTradeTerms(s,'wintermere','ashen',[{resource:'iron',amount:8}],needs,isolated);
  const tile=Object.values(isolated.tiles).find(t=>t.owner==='sunspire');
  Object.assign(tile,{fog:'visible',inferredOwner:false,building:'mine',levels:{mine:1},resource:'iron'});
  const b=buyerTradeTerms(s,'wintermere','ashen',[{resource:'iron',amount:8}],needs,isolated);
  assert.ok(b.maximumValue<a.maximumValue);
});

test('successful deliveries unlock larger shipments, while a frontier project protects its materials',()=>{
  const s=market();kingdom(s,'wintermere').resources.iron=0;
  const first=tradeCandidate(s,'wintermere','ashen');
  s.turn=4;
  s.diplomacy.tradeHistory.push(...[1,2].map(turn=>({turn,from:'ashen',to:'wintermere',resource:'iron',amount:10,kind:'exchange'})));
  assert.ok(tradeCandidate(s,'wintermere','ashen').intent.giveAmount>first.intent.giveAmount);
  const site=Object.values(s.tiles).find(t=>t.owner==='wintermere'&&!t.building);
  const plan=createPlan(s,'wintermere','buildDefenses',{targetTile:site.id,building:'fort'});plan.status='Committed';
  const buyer=kingdom(s,'wintermere');buyer.resources.wood=65;
  assert.equal(economicNeeds(s,'wintermere').find(n=>n.resource==='wood').planned,0,'trade reservation does not change strategic planning needs');
  const need=economicNeeds(s,'wintermere',{tradeReserves:true}).find(n=>n.resource==='wood');
  assert.equal(need.planned,35);assert.equal(need.surplus,0);
  const result=evaluateDeal(s,'wintermere',exchange(8,12),undefined,{allowCounter:false});
  assert.equal(result.reasonCode,'protected_reserve');
});

test('every resource in a package is protected and commitments reduce recurring capacity',()=>{
  const s=market(),buyer=kingdom(s,'wintermere');buyer.resources.tools=8;
  const packageOffer=withItems(exchange(8,12),[{resource:'iron',amount:8}],[{resource:'wood',amount:12},{resource:'tools',amount:1}]);
  assert.equal(evaluateDeal(s,'wintermere',packageOffer,undefined,{allowCounter:false}).reasonCode,'protected_reserve');
  const need={resource:'wood',surplus:100,production:10,reserve:35,planned:0};buyer.resources.wood=135;
  const capacity=availableTradeAmount(s,'wintermere',need,8);
  s.treaties.push({type:'recurring',parties:['wintermere','vesper'],payer:'wintermere',expires:s.turn+10,lastPaid:s.turn,intent:withItems({type:'RECURRING',duration:10},[{resource:'wood',amount:12}],[{resource:'food',amount:10}])});
  assert.ok(availableTradeAmount(s,'wintermere',need,8)<capacity);
});

test('AI-to-AI negotiation preserves materials reserved for projects and conserves resources',()=>{
  const s=market();s.turn=6;
  const all=()=>Object.fromEntries(RESOURCES.map(r=>[r,s.kingdoms.reduce((n,k)=>n+k.resources[r],0)]));
  const before=all();aiResourceTrade(s,'wintermere');assert.deepEqual(all(),before);
  for(const k of s.kingdoms)assert.ok(Object.values(k.resources).every(n=>n>=0));
  const buyer=kingdom(s,'wintermere'),beforeResources={...buyer.resources};
  const tooMuch=exchange(8,100);
  assert.equal(commitDeal(s,'wintermere',tooMuch).ok,false);
  assert.deepEqual(buyer.resources,beforeResources);
});
