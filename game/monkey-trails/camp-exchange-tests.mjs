import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {exchangeOffers,buyEssence} from './camp-exchange.mjs';

const save=()=>C.newSave('Camp exchange','capuchin','practice',23456);
const helpers=s=>{s.run.fruit=1000;for(const id of Object.keys(C.ITEMS))s.run.items[id]=100;s.run.items.charmMaterial=100;return s;};
function reserve(s,id,hp=0){const p=structuredClone(s.run.monkey);p.id=id;p.hp=hp;p.energy=2;s.collection[id]=p;return p;}

test('earned camp arrival fully heals every collected monkey once without changing energy or invitation',()=>{
  const s=save(),p=s.run.monkey,fallen=reserve(s,'fallen'),hurt=reserve(s,'hurt',8);p.hp=5;p.energy=1;
  s.meta.gear.heart=2;C.equip(s,'heart',hurt.id);s.run.injuries={fallen:{hp:0,energy:2},hurt:{hp:8,energy:2}};
  s.run.distance=5999*C.FEET;s.run.nextEncounter=6000*C.FEET;s.run.nextCamp=6000*C.FEET;s.run.ordinaryWins=2;
  C.advance(s,10*C.FEET);assert.equal(s.run.phase,'camp');assert.ok(s.run.pendingEncounter);assert.equal(s.run.camp.fullRecovery,true);assert.equal(s.run.camp.recoveredMonkeys,3);
  for(const monkey of Object.values(s.collection))assert.equal(monkey.hp,C.stats(monkey,C.equipped(s,monkey)).hp);
  assert.equal(p.energy,1);assert.equal(fallen.energy,2);assert.equal(hurt.energy,2);assert.deepEqual(s.run.injuries,{});
  const campId=s.run.camp.id,invitation=structuredClone(s.run.pendingEncounter);p.hp=7;
  const loaded=C.validate(structuredClone(s));assert.equal(loaded.run.monkey.hp,7);assert.equal(loaded.run.camp.id,campId);assert.deepEqual(loaded.run.pendingEncounter,invitation);
  C.advance(loaded,1000);assert.equal(loaded.run.monkey.hp,7);C.setParty(loaded,[fallen.id,hurt.id]);C.setParty(loaded,[p.id]);assert.equal(loaded.run.monkey.hp,7);
  C.leaveCamp(loaded);assert.equal(loaded.run.phase,'trail');assert.equal(loaded.run.monkey.hp,7);assert.deepEqual(loaded.run.pendingEncounter,invitation);
});

test('lineup changes at any trail distance preserve injuries and permit replacing fallen teammates',()=>{
  const s=save(),p=s.run.monkey,fallen=reserve(s,'fallen'),incoming=reserve(s,'incoming',9);s.run.party.push(fallen.id);s.run.distance=50000;s.run.encounters=12;
  const before=structuredClone(s.collection);C.setParty(s,[p.id,incoming.id]);assert.deepEqual(s.collection,before);assert.equal(fallen.hp,0);assert.equal(incoming.hp,9);
  // A different slot can change while a fallen current teammate is retained.
  s.run.party.push(fallen.id);C.setParty(s,[incoming.id,fallen.id]);assert.equal(fallen.hp,0);
  C.setParty(s,[incoming.id]);assert.throws(()=>C.setParty(s,[fallen.id]),/conscious incoming/);
});

test('utility crafting and four-for-one essence exchanges remain available on the trail',()=>{
  const s=helpers(save());s.run.distance=10000;s.run.encounters=5;s.meta.fragments.fang=5;
  C.craft(s,'bondCharm');assert.equal(s.run.fruit,998);assert.equal(s.run.items.charmMaterial,99);assert.equal(s.run.items.bondCharm,101);
  C.craft(s,'fang');assert.equal(s.meta.gear.fang,1);assert.equal(s.meta.fragments.fang,0);
  s.run.essence.leaf=8;C.exchangeEssence(s,'leaf','ice');assert.equal(s.run.essence.leaf,4);assert.equal(s.run.essence.ice,1);
  for(const phase of ['battle','replacement','befriend','dead']){s.run.phase=phase;const before=structuredClone(s);assert.throws(()=>C.craft(s,'berry'),/between encounters/);assert.throws(()=>C.exchangeEssence(s,'leaf','ice'),/between encounters/);assert.deepEqual(s,before);}
});

test('exchange previews are pure, transparent and give a twenty-percent fruit discount at camp',()=>{
  const s=helpers(save()),before=structuredClone(s),trail=exchangeOffers(s);assert.deepEqual(s,before);
  assert.equal(trail.find(row=>row.id==='fruit').cost.fruit,10);assert.equal(trail.find(row=>row.id==='fruit').available,25);
  assert.ok(trail.every(row=>row.canBuy&&row.amount>0&&row.costSummary.every(cost=>cost.count>0&&cost.owned>=cost.count)));
  s.run.phase='camp';const camp=exchangeOffers(s);assert.equal(camp.find(row=>row.id==='fruit').cost.fruit,8);assert.equal(camp.find(row=>row.id==='fruit').discounted,true);
  for(const row of camp.filter(row=>row.id!=='fruit'))assert.deepEqual(row.cost,trail.find(other=>other.id===row.id).cost);
});

test('all eight essences can be purchased for exact resource costs and batch outputs',()=>{
  for(const element of Object.keys(C.ELEMENTS))for(const recipe of exchangeOffers(helpers(save()))){
    const s=helpers(save()),before=structuredClone(s),receipt=buyEssence(s,{offerId:recipe.id,element,quantity:3});
    assert.equal(receipt.amount,recipe.amount*3);assert.equal(s.run.essence[element],before.run.essence[element]+receipt.amount);
    assert.equal(s.run.fruit,before.run.fruit-recipe.cost.fruit*3);
    for(const [id,n]of Object.entries(recipe.cost.items))assert.equal(s.run.items[id],before.run.items[id]-n*3);
    assert.equal(s.rng,before.rng);assert.deepEqual(s.collection,before.collection);assert.equal(s.run.distance,before.run.distance);
  }
});

test('invalid, unaffordable and blocked exchanges are atomic',()=>{
  const s=helpers(save());
  for(const options of [{offerId:'missing',element:'ice'},{offerId:'fruit',element:'missing'},{offerId:'fruit',element:'ice',quantity:0},{offerId:'fruit',element:'ice',quantity:1.5},{offerId:'fruit',element:'ice',quantity:26}]){const before=structuredClone(s);assert.throws(()=>buyEssence(s,options));assert.deepEqual(s,before);}
  s.run.items.berry=2;let before=structuredClone(s);assert.throws(()=>buyEssence(s,{offerId:'berry',element:'shadow'}),/Gather/);assert.deepEqual(s,before);
  for(const phase of ['battle','replacement','befriend','dead']){s.run.phase=phase;before=structuredClone(s);assert.equal(exchangeOffers(s).every(row=>!row.canBuy),true);assert.throws(()=>buyEssence(s,{offerId:'fruit',element:'earth'}),/between encounters/);assert.deepEqual(s,before);}
});

test('resource exchanges cannot produce fruit, supplies or a free reverse cycle',()=>{
  const s=helpers(save());s.run.phase='camp';const before=structuredClone(s);buyEssence(s,{offerId:'fruit',element:'leaf',quantity:4});C.exchangeEssence(s,'leaf','shadow');
  assert.equal(s.run.fruit,before.run.fruit-32);assert.equal(s.run.essence.shadow,1);assert.equal(s.run.essence.leaf,0);assert.deepEqual(s.run.items,before.run.items);
});
