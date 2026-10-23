import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as F from './companions.mjs';
import {renderFriendship,renderMemories,renderSanctuary,renderLineups,renderEvolutionGoal} from './companions-ui.mjs';
const save=()=>C.newSave('Friends','capuchin','gps',12345);
const cloneCompanion=(s,id='second',species='capuchin')=>{const p=structuredClone(s.run.monkey);p.id=id;p.species=species;delete p.memories;s.collection[id]=p;return p;};

test('normalization preserves old bonds, materials, collections and clamps compact memory fields',()=>{
  const s=save(),p=s.run.monkey;p.bond=47;s.meta.bondMaterials.capuchin=17;
  p.memories={distance:345.5,firstRegion:'emerald-jungle',firstGuardian:'frostpeak-mountains',latitude:51,trail:[{north:2}],victories:8,expeditions:3};
  F.normalizeCompanions(s);F.normalizeCompanions(s);
  assert.equal(p.bond,47);assert.equal(s.meta.bondMaterials.capuchin,17);assert.equal(p.memories.distance,345.5);assert.equal(p.memories.victories,8);
  assert.equal(F.friendshipInfo(p).tier.name,'Trusted Companion');assert.equal('latitude' in p.memories,false);assert.equal('trail' in p.memories,false);
  assert.deepEqual(s.meta.companions.lineups,[]);
});
test('friendship uses only conscious active companions and accepted travel remainder',()=>{
  const s=save(),p=s.run.monkey,other=cloneCompanion(s);C.setParty(s,[p.id,other.id]);other.hp=0;
  F.recordCompanionTravel(s,499,'emerald-jungle');F.recordCompanionTravel(s,501,'emerald-jungle');
  assert.equal(p.bond,1);assert.equal(p.memories.distance,1000);assert.equal(p.memories.firstRegion,'emerald-jungle');assert.equal(other.bond,0);assert.equal(other.memories.distance,0);
  s.run.phase='battle';F.recordCompanionTravel(s,10000);s.run.phase='trail';F.recordCompanionTravel(s,-2);F.recordCompanionTravel(s,NaN);
  assert.equal(p.bond,1);assert.equal(p.memories.distance,1000);
});
test('friendship travel is segmentation invariant and survives expedition restart',()=>{
  const a=save(),b=save(),p=a.run.monkey;F.recordCompanionTravel(a,4321);
  for(let i=0;i<4321;i++)F.recordCompanionTravel(b,1);
  assert.equal(a.run.monkey.bond,b.run.monkey.bond);assert.equal(p.memories.distance,4321);assert.equal(p.memories.travelRemainder,321);
  F.recordCompanionVictory(a,{guardian:true,regionId:'emerald-jungle'});F.recordCompanionExpedition(a);F.recordCompanionExpedition(a);
  assert.equal(p.memories.expeditions,1);assert.equal(p.memories.firstGuardian,'emerald-jungle');assert.equal(p.memories.victories,1);
  a.run.phase='dead';C.restart(a);assert.equal(p.bond,4);assert.equal(p.memories.distance,4321);assert.equal(p.memories.expeditions,1);
});
test('material preview and spending agree exactly and never remove or heal a companion',()=>{
  const s=save(),p=s.run.monkey;s.meta.bondMaterials.capuchin=3;p.bond=10;p.hp=10;
  const preview=F.bondMaterialPreview(s,p.id);assert.equal(preview.gain,4);assert.deepEqual(preview.unlocks,['partner']);
  F.spendBondMaterial(s,p.id);assert.equal(p.bond,14);assert.equal(p.hp,10);assert.equal(s.meta.bondMaterials.capuchin,2);assert.equal(Object.keys(s.collection).length,1);
  F.spendBondMaterial(s,p.id,2);assert.equal(p.bond,22);assert.equal(s.meta.bondMaterials.capuchin,0);assert.throws(()=>F.spendBondMaterial(s,p.id),/Gather/);
  s.meta.bondMaterials.capuchin=5;p.hp=0;assert.throws(()=>F.spendBondMaterial(s,p.id),/Revive/);p.hp=10;s.run.phase='battle';assert.throws(()=>F.spendBondMaterial(s,p.id),/between/);
});
test('material spending rejects invalid counts and already unlocked cosmetics without charging',()=>{
  const s=save(),p=s.run.monkey;s.meta.bondMaterials.capuchin=99;
  for(const amount of [0,-1,1.5,NaN,26])assert.throws(()=>F.spendBondMaterial(s,p.id,amount));
  p.bond=100;assert.throws(()=>F.spendBondMaterial(s,p.id),/already unlocked/);assert.equal(s.meta.bondMaterials.capuchin,99);
});
test('saved lineups reference individuals and use the existing party setter without healing',()=>{
  const s=save(),a=s.run.monkey,b=cloneCompanion(s,'b'),c=cloneCompanion(s,'c');a.hp=7;b.hp=11;c.hp=5;b.energy=2;
  const row=F.saveLineup(s,'Boss Team',[a.id,b.id,c.id]);F.applyLineup(s,row.id,C.setParty);
  assert.deepEqual(s.run.party,[a.id,b.id,c.id]);assert.equal(a.hp,7);assert.equal(b.hp,11);assert.equal(c.hp,5);assert.equal(b.energy,2);
  const same=F.saveLineup(s,'boss team',[b.id]);assert.equal(same.id,row.id);assert.equal(s.meta.companions.lineups.length,1);
  const restored=JSON.parse(JSON.stringify(s));F.normalizeCompanions(restored);assert.deepEqual(restored.meta.companions.lineups[0].members,[b.id]);
  assert.equal(F.removeLineup(s,row.id),true);assert.equal(F.removeLineup(s,row.id),false);
});
test('lineup application obeys camp, party, injury and duplicate equipment restrictions',()=>{
  const s=save(),a=s.run.monkey,b=cloneCompanion(s,'b'),row=F.saveLineup(s,'Second',[b.id]);
  s.run.distance=1;assert.throws(()=>F.applyLineup(s,row.id,C.setParty),/camp/);s.run.phase='camp';s.run.camp={heals:1};
  b.hp=0;assert.throws(()=>F.applyLineup(s,row.id,C.setParty),/Revive/);b.hp=8;a.hp=0;assert.throws(()=>F.applyLineup(s,row.id,C.setParty),/knocked-out/);a.hp=8;
  assert.throws(()=>F.saveLineup(s,'Bad',[b.id,b.id]),/different/);
  const gear=Object.keys(C.GEAR)[0];s.meta.gear[gear]=1;const slot=C.GEAR[gear].slot;a.equipment[slot]=gear;b.equipment[slot]=gear;const both=F.saveLineup(s,'Both',[a.id,b.id]);
  assert.throws(()=>F.applyLineup(s,both.id,C.setParty),/equipment/);assert.deepEqual(s.run.party,[a.id]);
});
test('favorites and saved lineups protect duplicate conversion and show exact result',()=>{
  const s=save(),p=cloneCompanion(s);s.run.lastCapture={id:p.id,duplicate:true,converted:false};
  const preview=F.duplicateConversionPreview(s,p.id);assert.equal(preview.allowed,true);assert.equal(preview.materials,3);assert.equal(preview.friendship,12);
  F.setFavorite(s,p.id);assert.equal(F.duplicateConversionPreview(s,p.id).allowed,false);assert.throws(()=>F.assertDuplicateConvertible(s,p.id),/favorites/);
  F.setFavorite(s,p.id,false);const row=F.saveLineup(s,'Keep',[p.id]);assert.throws(()=>F.assertDuplicateConvertible(s,p.id),/lineups/);F.removeLineup(s,row.id);
  const gear=Object.keys(C.GEAR)[0];p.equipment[C.GEAR[gear].slot]=gear;assert.throws(()=>F.assertDuplicateConvertible(s,p.id),/equipment/);
});
test('favorites have a small maximum and never create a party member',()=>{
  const s=save(),ids=[s.run.monkey.id,cloneCompanion(s,'a').id,cloneCompanion(s,'b').id,cloneCompanion(s,'c').id];
  for(const id of ids.slice(0,3))F.setFavorite(s,id);assert.throws(()=>F.setFavorite(s,ids[3]),/three/);assert.equal(F.favoriteCompanions(s).length,3);assert.equal(s.run.party.length,1);
});
test('evolution goals show real requirements and share one player-controlled pin',()=>{
  const s=save(),p=s.run.monkey;s.meta.regionChallenges={tracked:'old'};
  const goal=F.trackEvolution(s,p.id,'leaf',C.evolutionCost);assert.equal(goal.requiredLevel,3);assert.equal(goal.remaining[0].needed,6);
  assert.equal(s.meta.regionChallenges.tracked,null);assert.equal(s.meta.pinnedGoal.kind,'evolution');assert.equal(s.meta.pinnedGoal.id,p.id);assert.equal(goal.remaining[0].habitats,'Groves');assert.equal(goal.remaining[0].compass,'N');assert.match(renderEvolutionGoal(goal),/adventure-open/);assert.match(renderEvolutionGoal(goal),/4|four spare essence/);
  p.level=3;s.run.essence.leaf=6;assert.equal(F.evolutionGoal(s,C.evolutionCost).ready,true);C.evolve(s,'leaf',p.id);assert.equal(F.evolutionGoal(s,C.evolutionCost).complete,true);
});
test('rendered friendship, scene, lineups and goal controls escape user labels',()=>{
  const s=save(),p=s.run.monkey;p.nickname='<img onerror="x">';F.saveLineup(s,'<script>',[p.id]);F.normalizeCompanions(s);
  const html=renderFriendship(s,p)+renderMemories(p)+renderSanctuary(s)+renderLineups(s)+renderEvolutionGoal(F.trackEvolution(s,p.id,'leaf',C.evolutionCost));
  assert.equal(html.includes('<img onerror='),false);assert.equal(html.includes('<script>'),false);assert.match(html,/friendship-spend/);assert.match(html,/lineup-apply/);assert.match(html,/data-companion-scene/);assert.match(html,/evolution-goal-open/);
});

test('tracking one elemental evolution does not complete on a different form',()=>{
  const s=save(),p=s.run.monkey;F.trackEvolution(s,p.id,'leaf',C.evolutionCost);p.stage=1;p.types=['fire'];
  assert.equal(F.evolutionGoal(s,C.evolutionCost).complete,false);
});
