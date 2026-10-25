import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as T from './equipment-trail.mjs';
import * as F from './forge.mjs';
import {awardGearCopy,ownedEquipmentCopies,setEquipmentCopyFlag} from './equipment-inventory.mjs';
import {regionChallenges,recordRegionEvent,claimChallenge} from './challenges.mjs';
import {REGION_ADVENTURES,adventureSteps,claimAdventure} from './regional-adventures.mjs';

const save=()=>C.newSave('Equipment Trail','capuchin','gps',12345);
function equip(s,id,slot='charm',p=s.run.monkey){const award=awardGearCopy(s,id);C.equip(s,award.copyId,p.id,slot);return award.copyId;}
function freeWalk(s,meters){s.run.nextEncounter=s.run.nextCamp=1e8;return C.advance(s,meters,0,meters);}
function discovered(s,category='fruit',id){s.run.discoveries++;const event={category,id,amount:category==='fruit'?2:1};if(category==='fruit')s.run.fruit+=2;else s.run.essence[id]++;return T.recordEquipmentDiscovery(s,event);}

test('forager progress follows real discovery serials, survives reload and never counts bonus resources',()=>{
 let s=save();equip(s,'foragersPouch');const before=s.run.fruit;
 for(let n=0;n<11;n++)assert.deepEqual(discovered(s).bonuses,[]);
 s=C.validate(structuredClone(s));const result=discovered(s);
 assert.deepEqual(result.bonuses,[{kind:'foraged',id:'fruit',count:1,quality:'common'}]);assert.equal(s.run.fruit,before+25);assert.equal(s.run.discoveries,12);
 const frozen=JSON.stringify(s);assert.deepEqual(T.recordEquipmentDiscovery(s,{category:'fruit',amount:2}).bonuses,[]);assert.equal(JSON.stringify(s),frozen);
 C.unequip(s,'charm');discovered(s);assert.equal(s.run.equipmentTrail.foragerProgress,0);
});

test('ordinary accepted walking invokes equipment discovery hooks but distance challenges retain real meters',()=>{
 const s=save();equip(s,'foragersPouch');equip(s,'trailblazerMedal','weapon');const distance=C.RESOURCE_STEP*.95*12;
 const events=[];s.run.nextEncounter=s.run.nextCamp=1e8;C.advance(s,distance,0,distance,event=>events.push(event));
 assert.equal(events.length,12);assert.equal(events.flatMap(event=>event.equipmentBonuses).filter(b=>b.kind==='foraged').length,1);
 assert.equal(s.world.totalDistance,distance);assert.equal(s.run.discoveries,12);assert.equal(s.run.equipmentTrail.foragerProgress,0);
 assert.equal(C.discoveryInterval(s),C.RESOURCE_STEP*.95);
});

test('resource lens promotes exactly one awarded common ingredient to usable uncommon forging material',()=>{
 const s=save();equip(s,'resourceLens');s.rng=1;const before=s.run.fruit,result=discovered(s);
 assert.equal(result.bonuses[0].kind,'upgraded');assert.equal(s.run.fruit,before+1);assert.equal(s.run.forgeMaterials.fruit,1);
 const row=F.forgeInventory(s).find(row=>row.id==='quality:fruit');assert.equal(row.owned,1);assert.equal(row.rarity,'uncommon');
 s.run.fruit=20;const preview=F.previewForge(s,{'quality:fruit':1,fruit:8});assert.ok(preview.canCraft);assert.ok(preview.quality>F.previewForge(s,{fruit:10}).quality);
 F.craftForge(s,{'quality:fruit':1,fruit:8},'forge-1');assert.equal(s.run.forgeMaterials.fruit,0);assert.equal(s.run.discoveries,1);
});

test('prism compass converts one eligible essence and cannot introduce an element outside its pool',()=>{
 const s=save();equip(s,'prismCompass');s.meta.equipmentChoices={prismCompass:'earth'};s.rng=1;
 const result=discovered(s,'essence','leaf');assert.equal(s.run.essence.leaf,0);assert.equal(s.run.essence.earth,1);assert.equal(result.bonuses[0].quality,'common');
 assert.deepEqual(T.eligibleCompassElements(s),['leaf','earth','ice','light','shadow']);
 s.meta.equipmentChoices.prismCompass='fire';s.rng=1;discovered(s,'essence','leaf');assert.equal(s.run.essence.leaf,1);assert.equal(s.run.essence.fire,0);
});

test('supply satchel camp essence is region appropriate, uncommon and claim-once across reload',()=>{
 let s=save();equip(s,'supplySatchel');s.run.nextEncounter=1e8;C.advance(s,C.CONFIG.campFeet*C.FEET,0,C.CONFIG.campFeet*C.FEET);
 assert.equal(s.run.phase,'camp');assert.equal(s.run.forgeMaterials['essence:leaf'],1);assert.equal(s.run.camp.fullRecovery,true);
 s=C.validate(structuredClone(s));const frozen=JSON.stringify(s);assert.equal(T.awardEquipmentCamp(s),null);assert.equal(JSON.stringify(s),frozen);
 const preview=F.previewForge(s,{'quality:essence:leaf':1,fruit:8});assert.ok(preview.canCraft);assert.equal(preview.costInputs['quality:essence:leaf'],1);
});

test('capture, walking recovery, training fruit and encounter weight use their precise relative bonuses',()=>{
 const s=save();equip(s,'trackersKnot');assert.equal(T.equipmentCaptureChance(s,.50),.525);assert.equal(T.equipmentCaptureChance(s,.98),.99);assert.equal(T.equipmentCaptureChance(s,1),1);
 C.unequip(s,'charm');equip(s,'campwalkerBrooch');const p=s.run.monkey,max=C.playerStats(s).hp;p.hp=1;freeWalk(s,100*C.FEET);assert.ok(Math.abs(p.hp-(1+max*.022))<1e-9);
 C.unequip(s,'charm');equip(s,'fruitkeeperClip');const cost=C.trainingCost(p);p.training=cost.xp;s.run.fruit=cost.fruit;C.train(s,p.id);assert.ok(Math.abs(p.training+p.xpRemainder-cost.xp*.05)<1e-9);
 C.unequip(s,'charm');equip(s,'trailfinderCompass');s.meta.equipmentChoices={trailfinderCompass:'leaf'};
 const leafSpecies=Object.keys(C.SPECIES).find(id=>C.SPECIES[id].types.includes('leaf'));
 assert.equal(T.equipmentEncounterWeight(s,leafSpecies),1.1);assert.equal(T.equipmentEncounterWeight(s,'macaque'),1);assert.ok(C.encounterPool(s).every(id=>C.SPECIES[id].rarity!=='ultra'));
});

test('trailforger returns only actually spent common ingredients after discount, once per operation',()=>{
 let s=save();equip(s,'trailforgersPrism');s.run.phase='camp';s.run.fruit=100;s.run.essence.leaf=100;
 const inputs={fruit:10,'essence:leaf':10},preview=F.previewForge(s,inputs);assert.deepEqual(preview.costInputs,{'essence:leaf':8,fruit:8});assert.deepEqual(preview.refunds,{fruit:1});
 const receipt=F.craftForge(s,inputs,'forge-1');assert.equal(s.run.fruit,93);assert.equal(s.run.essence.leaf,92);assert.deepEqual(receipt.refunds,{fruit:1});
 s=C.validate(structuredClone(s));const frozen=JSON.stringify(s);assert.deepEqual(F.craftForge(s,inputs,'forge-1'),receipt);assert.equal(JSON.stringify(s),frozen);
 assert.deepEqual(T.equipmentCraftRefund(s,{'essence:leaf':50,'quality:fruit':50,'fragment:worldroot':50}),{});
 assert.deepEqual(T.equipmentCraftRefund(s,{fruit:3,'item:berry':2}),{fruit:1});
});

test('equipped and protected duplicates survive forge upgrades and maximum ranks retain a new copy',()=>{
 const s=save();s.run.fruit=100;const first=F.craftForge(s,{fruit:8},'forge-1');C.equip(s,first.copyId,s.run.activeId);
 s.rng=12345;const second=F.craftForge(s,{fruit:8},'forge-2');assert.equal(second.itemId,first.itemId);assert.equal(second.kind,'discovered');assert.notEqual(second.copyId,first.copyId);assert.equal(s.meta.equipmentCopies[first.copyId].tier,1);
 setEquipmentCopyFlag(s,second.copyId,'protected',true);s.rng=12345;const third=F.craftForge(s,{fruit:8},'forge-3');assert.equal(third.kind,'discovered');assert.equal(ownedEquipmentCopies(s).filter(c=>c.itemId===first.itemId).length,3);
 s.rng=12345;const fourth=F.craftForge(s,{fruit:8},'forge-4');assert.equal(fourth.kind,'upgraded');assert.equal(fourth.copyId,third.copyId);assert.equal(s.meta.equipmentCopies[second.copyId].tier,1);
 s.meta.equipmentCopies[third.copyId].tier=12;s.meta.gear[first.itemId]=12;s.rng=12345;const duplicate=F.craftForge(s,{fruit:8},'forge-5');assert.equal(duplicate.kind,'discovered');assert.equal(duplicate.tier,1);assert.notEqual(duplicate.copyId,third.copyId);assert.equal(s.meta.equipmentCopies[third.copyId].tier,12);assert.equal(s.meta.equipmentCopies[first.copyId].tier,1);assert.equal(ownedEquipmentCopies(s).filter(c=>c.itemId===first.itemId).length,4);
});

test('regional victory challenges award themed Rare equipment exactly once',()=>{
 let s=save();const goal=regionChallenges(s).find(row=>row.kind==='victories');assert.deepEqual(goal.reward.gear,['briarloopBand']);recordRegionEvent(s,'victories',goal.goal);
 const reward=claimChallenge(s,goal.id);assert.equal(reward.gear[0],'briarloopBand');assert.equal(s.meta.gear.briarloopBand,1);s=C.validate(structuredClone(s));assert.throws(()=>claimChallenge(s,goal.id),/already claimed/);assert.equal(ownedEquipmentCopies(s).filter(c=>c.itemId==='briarloopBand').length,1);
});

test('regional keepsake milestones include reachable Rare equipment in their saved claim pool',()=>{
 for(const adventure of REGION_ADVENTURES){const reward=adventure.steps.at(-1).reward;assert.equal(reward.gear.length,1);assert.equal(C.GEAR[reward.gear[0]].rarity,'rare');}
 let s=save();const adventure=REGION_ADVENTURES[0],last=adventure.steps.at(-1);adventureSteps(s);
 const local=s.meta.adventures.regions[adventure.id];local.claimed=adventure.steps.slice(0,-1).map(step=>step.id);local.stepId=last.id;local.current=last.goal;
 assert.equal(claimAdventure(s,last.id).gear[0],'trailfinderCompass');assert.equal(s.meta.gear.trailfinderCompass,1);s=C.validate(structuredClone(s));assert.throws(()=>claimAdventure(s,last.id),/already claimed/);
});

test('malformed material/event imports are rejected before any reward or spend',()=>{
 const s=save();for(const materials of [{fruit:-1},{'essence:bogus':1},{fruit:Infinity}]){s.run.forgeMaterials=materials;assert.throws(()=>T.normalizeEquipmentTrail(s),/Invalid/);}
 s.run.forgeMaterials={};s.run.equipmentTrail={version:1,lastDiscovery:3,foragerProgress:0,camps:[]};assert.throws(()=>T.normalizeEquipmentTrail(s),/Invalid/);
});
