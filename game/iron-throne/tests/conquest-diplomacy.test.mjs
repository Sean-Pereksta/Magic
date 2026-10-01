import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { declareWar, parseSave, rebuildTerritory, relation, resolveMovement } from '../core.mjs';
import { borderThreat, updatePoliticalState } from '../living.mjs';
import { makeContext, scriptedReply } from '../diplomacy.mjs';
import { knowledgeView, refreshKnowledge } from '../fog.mjs';
import { planningView } from '../ai-knowledge.mjs';
import { dispatchTitle } from '../conversation-context.mjs';

function campaign() {
  const s=createGame();s.turn=36;
  // Sunspire retains a distant settlement after losing its capital. Redharbor
  // holds a fort close enough to observe the shared campaign at Solstice.
  Object.assign(s.tiles['32,12'],{owner:'sunspire',building:'town',terrain:'plains',name:'Sunspire Refuge'});
  Object.assign(s.tiles['29,23'],{owner:'redharbor',building:'fort',terrain:'plains',name:'Redharbor Frontier'});
  Object.assign(s.tiles['31,21'],{terrain:'plains',road:true});
  rebuildTerritory(s);
  const army=s.armies.find(a=>a.owner==='ashen');army.tile='31,21';army.units.levy=1000;
  s.armies.find(a=>a.owner==='sunspire').tile='32,12';
  s.armies.find(a=>a.owner==='redharbor').tile='29,23';
  s.treaties.push({type:'alliance',parties:['ashen','redharbor'],expires:60});
  declareWar(s,'ashen','sunspire');declareWar(s,'redharbor','sunspire');
  refreshKnowledge(s);updatePoliticalState(s,{sendDispatches:false});
  s.conversations={};
  return {s,army};
}
function conquer(s,army) {
  army.order='attack';army.target='32,21';army.path=['32,21'];
  resolveMovement(s,'ashen');
  assert.equal(s.tiles['32,21'].owner,'ashen');assert.equal(army.target,null);
  assert.ok(s.militaryEvents.some(e=>e.action==='capture'&&e.tile==='32,21'&&e.defender==='sunspire'));
  updatePoliticalState(s);
}
test('capturing Solstice supports Redharbor instead of triggering an allied border complaint',()=>{
  const {s,army}=campaign();conquer(s,army);
  const threat=borderThreat(s,'redharbor','ashen');
  assert.ok(threat.nearby.some(a=>a.id===army.id&&a.cooperating));assert.equal(threat.score,0);
  assert.ok(threat.sharedVictories.some(e=>e.tile==='32,21'&&e.defender==='sunspire'));
  const messages=s.conversations.redharbor||[];
  assert.ok(messages.some(m=>m.kind==='shared-victory'&&/Solstice/.test(m.text)));
  assert.ok(!messages.some(m=>['border','withdrawal'].includes(m.kind)));
  assert.doesNotMatch(scriptedReply(s,'redharbor','Solstice is ours. I will capture the rest of them.').reply,/Explain their purpose|soldiers stand close/);
  assert.ok(makeContext(s,'redharbor','Solstice is ours.').world.militaryRelationship.sharedVictories.some(e=>e.tile==='32,21'));
  assert.equal(dispatchTitle('shared-victory'),'Shared War Victory');
});
test('the defeated ruler acknowledges a capital loss without fabricating an enemy withdrawal',()=>{
  const {s,army}=campaign();const before=relation(s,'sunspire','ashen').wariness;assert.ok(before>=20);
  conquer(s,army);assert.ok(relation(s,'sunspire','ashen').wariness<before-12);
  const messages=s.conversations.sunspire||[];
  assert.ok(messages.some(m=>m.kind==='territory-loss'&&/Solstice, our capital/.test(m.text)));
  assert.ok(!messages.some(m=>['withdrawal','border-report'].includes(m.kind)));
  assert.ok(!relation(s,'sunspire','ashen').history.some(h=>h.turn===36&&/withdrew/.test(h.reason)));
  const context=makeContext(s,'sunspire','I have captured your capital.');
  assert.ok(context.world.militaryRelationship.losses.some(e=>e.tile==='32,21'&&e.capital));
  assert.equal(dispatchTitle('territory-loss'),'Military Defeat');
  const count=messages.filter(m=>m.kind==='territory-loss').length;updatePoliticalState(s);
  assert.equal(messages.filter(m=>m.kind==='territory-loss').length,count,'rechecking does not send the loss twice');
});
test('cooperation does not hide allied armies threatening the ally or an unrelated conquest',()=>{
  const {s,army}=campaign();conquer(s,army);
  army.tile='29,23';refreshKnowledge(s);updatePoliticalState(s);
  assert.ok(borderThreat(s,'redharbor','ashen').score>=20);
  assert.ok((s.conversations.redharbor||[]).some(m=>m.kind==='border'));
  const other=createGame(),a=other.armies[0];a.units.levy=1000;a.tile='17,4';
  other.treaties.push({type:'alliance',parties:['ashen','wintermere'],expires:60});refreshKnowledge(other);
  assert.ok(borderThreat(other,'wintermere','ashen').score>=20,'an alliance alone is not immunity from border concern');
});
test('border ownership changes cannot turn a stationary observed army into a retreat',()=>{
  const {s,army}=campaign();conquer(s,army);
  const r=relation(s,'redharbor','ashen');assert.equal(r.observedArmyTiles[army.id],army.tile);
  Object.assign(s.tiles['33,21'],{owner:'redharbor',building:'fort',terrain:'plains'});rebuildTerritory(s);refreshKnowledge(s);
  const observed=borderThreat(s,'redharbor','ashen').nearby.find(a=>a.id===army.id);
  assert.ok(observed);assert.equal(observed.movement,'holding');
});
test('a genuine observed retreat still produces a withdrawal dispatch',()=>{
  const s=createGame(),a=s.armies[0];a.units.levy=200;a.tile='17,4';
  refreshKnowledge(s);updatePoliticalState(s);s.conversations.wintermere=[];
  // Both positions remain visible to Wintermere, while distance to its border grows.
  a.tile='13,4';s.turn++;refreshKnowledge(s);updatePoliticalState(s);
  assert.ok((s.conversations.wintermere||[]).some(m=>m.kind==='withdrawal'));
  assert.ok(relation(s,'wintermere','ashen').history.some(h=>/withdrew/.test(h.reason)));
});
test('army position observations survive saves, accept old saves, and stay private to each court',()=>{
  const {s,army}=campaign();conquer(s,army);
  const loaded=parseSave(JSON.stringify(s));
  assert.equal(relation(loaded,'redharbor','ashen').observedArmyTiles[army.id],army.tile);
  const legacy=structuredClone(s);for(const k of legacy.kingdoms)for(const r of Object.values(k.relations))delete r.observedArmyTiles;
  assert.doesNotThrow(()=>parseSave(JSON.stringify(legacy)));
  assert.deepEqual(relation(knowledgeView(s,'ashen'),'redharbor','ashen').observedArmyTiles,{});
  assert.deepEqual(relation(planningView(s,'wintermere'),'redharbor','ashen').observedArmyTiles,{});
  const damaged=structuredClone(s);relation(damaged,'redharbor','ashen').observedArmyTiles[army.id]='not-a-tile';
  assert.throws(()=>parseSave(JSON.stringify(damaged)),/living diplomacy/);
});
