import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {explorationEncounterWeight,trailResourceWeights,rollTrailResource,foragePickup} from './exploration.mjs';
import {HABITATS,habitatAtPosition} from './habitats.mjs';

const start=()=>C.newSave('Walking companions','capuchin','practice',12345);
function add(s,species='magnetuft',id=`fixture-${Object.keys(s.collection).length}`){
  const p=structuredClone(s.run.monkey);p.id=id;p.species=species;p.types=[...C.SPECIES[species].types];p.level=1;p.stage=0;p.learned=[];p.loadout=['strike','brace','breathe'];C.syncProgression(p);p.hp=C.stats(p).hp;p.energy=C.stats(p).maxEnergy;s.collection[id]=p;return p;
}
function walk(s,meters,east=0,north=0,callback){s.run.nextEncounter=s.run.nextCamp=1e9;return C.advance(s,meters,east,north,callback);}
function win(s,ultra=false){
  C.advance(s,s.run.nextEncounter-s.run.distance);C.beginPendingEncounter(s);C.readyBattle(s);
  const b=s.run.battle;b.ultra=ultra;if(ultra)s.run.ultra={id:b.id,species:b.enemy.species,challengeRemaining:0,spawnDistance:0,status:'battle'};
  s.run.monkey.level=60;s.run.monkey.hp=C.stats(s.run.monkey).hp;b.enemies.forEach(p=>p.hp=1);b.intent='breathe';b.enemyAction={type:'move',id:'breathe'};C.turn(s,'strike');assert.equal(s.run.phase,'befriend');return b;
}

test('walking choice migrates older checkpoints and persists independently of the battle lead',()=>{
  const s=start(),first=s.run.monkey,other=add(s);C.setParty(s,[first.id,other.id]);C.setActiveMonkey(s,other.id);
  assert.equal(C.activeExplorer(s),other);assert.equal(s.run.activeId,first.id);
  C.selectBattleLead(s,other.id);C.selectBattleLead(s,first.id);assert.equal(s.run.explorerId,other.id);
  let reopened=C.validate(JSON.parse(JSON.stringify(s)));assert.equal(reopened.run.explorerId,other.id);assert.equal(reopened.run.activeId,first.id);
  delete reopened.run.explorerId;delete reopened.run.items.ingot;delete reopened.run.items.starOre;delete reopened.run.items.crownCore;
  reopened=C.validate(reopened);assert.equal(reopened.run.explorerId,first.id);assert.equal(reopened.run.items.ingot,0);assert.equal(reopened.run.items.starOre,0);assert.equal(reopened.run.items.crownCore,0);
});

test('invalid, removed and knocked-out explorers fall back to a conscious roster member',()=>{
  const s=start(),first=s.run.monkey,other=add(s),reserve=add(s,'eclipsemask');C.setParty(s,[first.id,other.id]);
  for(const invalid of ['missing',reserve.id,null,{},false]){s.run.explorerId=invalid;assert.equal(C.activeExplorer(s),first);}
  C.setActiveMonkey(s,other.id);other.hp=0;assert.equal(C.activeExplorer(s),first);
  assert.throws(()=>C.setActiveMonkey(s,other.id),/conscious/);assert.throws(()=>C.setActiveMonkey(s,reserve.id),/active roster/);
  first.hp=0;assert.equal(C.activeExplorer(s),null);assert.equal(explorationEncounterWeight(s,'rootstone'),1);
  assert.equal(foragePickup(s,()=>{throw Error('A fallen team must not roll bonuses.');},HABITATS.cave),null);
});

test('party changes and sales repair walking selection without selecting a sanctuary monkey',()=>{
  const s=start(),first=s.run.monkey,other=add(s),reserve=add(s,'mossmop');C.setParty(s,[first.id,other.id]);C.setActiveMonkey(s,other.id);
  C.setParty(s,[first.id]);assert.equal(s.run.explorerId,first.id);C.setParty(s,[first.id,other.id]);C.setActiveMonkey(s,other.id);
  C.sellMonkey(s,other.id);assert.equal(C.activeExplorer(s),first);assert.ok(s.collection[reserve.id]);assert.equal(s.collection[other.id],undefined);
});

test('combat switching keeps the walking choice and changing it during combat is rejected',()=>{
  const s=start(),first=s.run.monkey,other=add(s);C.setParty(s,[first.id,other.id]);C.setActiveMonkey(s,other.id);
  C.advance(s,C.ENCOUNTER_STEP);C.beginPendingEncounter(s);C.readyBattle(s);s.run.battle.enemyAction={type:'move',id:'breathe'};s.run.battle.intent='breathe';
  C.switchMonkey(s,other.id);assert.equal(C.activeExplorer(s),other);s.run.battle.enemyAction={type:'move',id:'breathe'};C.switchMonkey(s,first.id);
  assert.equal(s.run.activeId,first.id);assert.equal(s.run.explorerId,other.id);assert.throws(()=>C.setActiveMonkey(s,first.id),/between encounters/);
});

test('rare mineral foragers and scouts have distinct specialties that only the chosen monkey supplies',()=>{
  const s=start(),first=s.run.monkey,miner=add(s),scout=add(s,'eclipsemask');C.setParty(s,[first.id,miner.id,scout.id]);
  const plain=trailResourceWeights(s,HABITATS.cave),common=C.explorationProfile(first),mining=C.explorationProfile(miner),scouting=C.explorationProfile(scout);
  assert.ok(mining.forageChance>common.forageChance);assert.ok(mining.materialWeights.ingot>mining.materialWeights.fruit||!mining.materialWeights.fruit);assert.ok(mining.specialties.some(text=>/Ingots/.test(text)));
  C.setActiveMonkey(s,miner.id);const minerals=trailResourceWeights(s,HABITATS.cave);assert.ok(minerals['item:ingot']>plain['item:ingot']);assert.ok(minerals['item:starOre']>plain['item:starOre']);assert.ok(minerals['item:ingot']<=.15);assert.ok(minerals['item:starOre']<=.02);
  assert.ok(scouting.rareEncounterBonus>mining.rareEncounterBonus);C.setActiveMonkey(s,scout.id);assert.equal(explorationEncounterWeight(s,'rootstone'),1+scouting.rareEncounterBonus);assert.equal(explorationEncounterWeight(s,'mossmop'),1);assert.equal(explorationEncounterWeight(s,'verdara'),1);
  C.setActiveMonkey(s,first.id);assert.deepEqual(trailResourceWeights(s,HABITATS.cave),plain);assert.equal(explorationEncounterWeight(s,'rootstone'),1);
  assert.ok(C.encounterPool(s).every(id=>C.SPECIES[id].rarity!=='rare'),'A scout never bypasses the encounter depth gate.');
});

test('habitat resources favor groves and caves and every drop table is a complete probability distribution',()=>{
  const s=start();const grove=trailResourceWeights(s,HABITATS.grove),cave=trailResourceWeights(s,HABITATS.cave);
  assert.ok(grove.fruit>cave.fruit);assert.ok(grove['essence:leaf']>cave['essence:leaf']);assert.ok(cave['item:ingot']>grove['item:ingot']);assert.ok(cave['essence:earth']>grove['essence:earth']);assert.ok(trailResourceWeights(s,HABITATS.riverbank)['essence:water']>grove['essence:water'],'Wetland Water essence never requires a westward route.');assert.ok(trailResourceWeights(s,HABITATS.landmark)['essence:storm']>grove['essence:storm'],'Ridge Storm essence never requires an eastward route.');
  const water=add(s,'cascadeHowler');C.setParty(s,[s.run.activeId,water.id]);C.setActiveMonkey(s,water.id);assert.ok(trailResourceWeights(s,HABITATS.grove)['essence:water']>grove['essence:water'],'A Water specialist can forage Water essence outside a wetland while walking north.');
  for(const habitat of Object.values(HABITATS))for(const forage of[false,true]){const table=trailResourceWeights(s,habitat,{forage});assert.ok(Math.abs(Object.values(table).reduce((a,b)=>a+b,0)-1)<1e-10);assert.ok(Object.values(table).every(n=>n>=0));assert.equal('item:crownCore'in table,false);}
  assert.equal(rollTrailResource(s,.999999,HABITATS.grove).id,'starOre');
});

test('forage rolls use saved RNG once per discovery and bonus pickups never create discovery progress',()=>{
  const s=start(),miner=add(s);C.setParty(s,[s.run.activeId,miner.id]);C.setActiveMonkey(s,miner.id);s.rng=1;const beforeRng=s.rng;
  C.explorationProfile(miner);C.activeExplorer(s);trailResourceWeights(s,HABITATS.cave);assert.equal(s.rng,beforeRng,'Reading UI stats must never consume RNG.');
  const control=structuredClone(s),events=[];walk(s,C.RESOURCE_STEP,0,0,event=>events.push(event));walk(control,C.RESOURCE_STEP);
  assert.deepEqual(s,control);assert.equal(s.run.discoveries,1);assert.equal(events.length,1);assert.equal(events[0].foragingBonuses.length,1);assert.equal(events[0].foragingBonuses[0].monkeyId,miner.id);
  const frozen=JSON.stringify(s);assert.equal(C.advance(s,0),0);assert.equal(JSON.stringify(s),frozen);
  const rolls=[0,.999999];let called=0;const extra=foragePickup(s,()=>rolls[called++],HABITATS.cave);assert.equal(called,2);assert.equal(extra.id,'starOre');
  s.run.phase='battle';assert.equal(foragePickup(s,()=>{throw Error('No battle foraging.');},HABITATS.cave),null);
});

test('each discovery uses its position along a long accepted sample instead of the final habitat',()=>{
  const s=start(),events=[],distance=C.RESOURCE_STEP*12;walk(s,distance,distance,0,event=>events.push(event));
  assert.equal(events.length,12);
  for(let i=0;i<events.length;i++)assert.equal(events[i].habitat,habitatAtPosition(s.world.activeLand,C.RESOURCE_STEP*(i+1),0).id);
  assert.ok(new Set(events.map(row=>row.habitat)).size>1);
});

test('late rare and Ultra training requires the displayed materials and spends them atomically',()=>{
  const s=start(),rare=add(s),ultra=add(s,'obsidrax');C.setParty(s,[s.run.activeId,rare.id,ultra.id]);
  assert.deepEqual(C.trainingCost(rare).materials,{});rare.level=20;rare.training=9999;s.run.fruit=9999;
  assert.deepEqual(C.trainingCost(rare).materials,{starOre:1});const before=JSON.stringify(s);assert.throws(()=>C.train(s,rare.id),/Star Ore/);assert.equal(JSON.stringify(s),before);
  s.run.items.starOre=3;C.train(s,rare.id);assert.equal(rare.level,21);assert.equal(s.run.items.starOre,2);
  ultra.level=40;ultra.training=9999;assert.deepEqual(C.trainingCost(ultra).materials,{starOre:2,crownCore:2});s.run.items.crownCore=1;
  const blocked=JSON.stringify(s);assert.throws(()=>C.train(s,ultra.id),/Crown Core/);assert.equal(JSON.stringify(s),blocked);
  s.run.items.crownCore=2;C.train(s,ultra.id);assert.equal(ultra.level,41);assert.equal(s.run.items.starOre,0);assert.equal(s.run.items.crownCore,0);
});

test('only Ultra victories award a Crown Core and a completed victory cannot award it twice',()=>{
  const ordinary=start();win(ordinary);assert.equal(ordinary.run.items.crownCore,0);
  const s=start();win(s,true);assert.equal(s.run.items.crownCore,1);const restored=C.validate(JSON.parse(JSON.stringify(s)));assert.equal(restored.run.items.crownCore,1);
  assert.throws(()=>C.turn(restored,'strike'));assert.equal(restored.run.items.crownCore,1);C.skipBefriend(restored);assert.equal(restored.run.items.crownCore,1);
});

test('new crafting and training materials reject corrupted checkpoint counts',()=>{
  for(const id of['ingot','starOre','crownCore'])for(const invalid of[-1,.5,NaN,Infinity,'3',1e9+1]){const s=start();s.run.items[id]=invalid;assert.throws(()=>C.validate(s),/inventory/);}
});

test('hard-earned Star Ore and Crown Cores survive defeat, save reload and expedition restart',()=>{
  const s=start();C.advance(s,C.ENCOUNTER_STEP);C.beginPendingEncounter(s);C.readyBattle(s);s.run.items.starOre=7;s.run.items.crownCore=3;s.run.items.ingot=4;
  s.run.monkey.hp=1;s.run.monkey.loadout=['strike','brace','breathe'];s.run.battle.enemyAction={type:'move',id:'strike'};s.run.battle.intent='strike';C.turn(s,'breathe');assert.equal(s.run.phase,'dead');
  assert.equal(s.run.items.starOre,7);assert.equal(s.run.items.crownCore,3);assert.equal(s.run.items.ingot,0);
  const restored=C.validate(JSON.parse(JSON.stringify(s)));C.restart(restored);assert.equal(restored.run.items.starOre,7);assert.equal(restored.run.items.crownCore,3);
  const rare=add(restored);rare.level=20;rare.training=9999;restored.run.fruit=9999;C.setParty(restored,[restored.run.activeId,rare.id]);C.train(restored,rare.id);assert.equal(restored.run.items.starOre,6);
});
