import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { relation, parseSave } from '../core.mjs';
import { scriptedReply, makeContext, relationshipResponse } from '../diplomacy.mjs';
import { borderThreat, updatePoliticalState, diplomaticPriorities } from '../living.mjs';
import { politicalAttitude } from '../politics.mjs';
import { knowledgeView, refreshKnowledge } from '../fog.mjs';
import { splitCampaign, playerView } from '../multiplayer-state.mjs';
import { vassalMuster } from '../vassal-role.mjs';
import { systemPrompt } from '../worker/worker.mjs';

function sworn() {
  const s=createGame();s.turn=38;
  s.treaties.push({type:'vassalage',parties:['ashen','sunspire'],liege:'ashen',vassal:'sunspire',expires:58});
  Object.assign(relation(s,'sunspire','ashen'),{opinion:-48,trust:-30,grievance:50,wariness:100});
  s.armies[0].tile='32,21';s.armies[0].units.levy=1000;refreshKnowledge(s);
  return s;
}
test('a resentful vassal respects permitted liege troops and acknowledges its duty immediately',()=>{
  const s=sworn();assert.equal(politicalAttitude(s,'sunspire','ashen').label,'Sworn Vassal');
  assert.equal(borderThreat(s,'sunspire','ashen').score,0);updatePoliticalState(s);
  assert.equal(relation(s,'sunspire','ashen').wariness,0);assert.equal(relation(s,'sunspire','ashen').trust,-30);
  assert.equal(politicalAttitude(s,'sunspire','ashen').tone,'Respectful and obedient');
  assert.ok(!(s.conversations.sunspire||[]).some(m=>['border','withdrawal','border-report','alliance'].includes(m.kind)));
  const reply=scriptedReply(s,'sunspire','What should I do next?');assert.match(reply.reply,/My liege/);
  assert.doesNotMatch(reply.reply,/Explain their purpose|word you failed|what you seek in return/);
  assert.match(diplomaticPriorities(s,'sunspire')[0],/sworn vassalage/);
  const context=makeContext(s,'sunspire','What should I do next?');assert.equal(context.world.vassalRelationship.liege,'ashen');
  assert.match(systemPrompt('sunspire'),/sworn vassal|lawful military commands/);
});
test('the liege receives a full accurate muster including unseen vassal armies without fabricated movement',()=>{
  const s=sworn(),a=s.armies.find(a=>a.owner==='sunspire');
  a.units.levy=43;a.units.archer=7;a.units.cavalry=2;a.units.siege=3;
  const distant=structuredClone(a);distant.id=`army-${s.nextId++}`;distant.tile='35,12';distant.units.levy=100;s.armies.push(distant);
  refreshKnowledge(s);const before=JSON.stringify(s),report=vassalMuster(s,'ashen','sunspire');
  assert.equal(report.armies,2);assert.equal(report.units.levy,143);assert.equal(report.units.siege,6);assert.equal(report.troops,161);
  const local=scriptedReply(s,'sunspire','What are your full forces numbers right now?');assert.match(local.reply,/161 troops across 2 armies/);
  const corrected=relationshipResponse(s,'sunspire','What are your full forces numbers right now?',{reply:'Your troops worry me.',tone:'cold',intents:[]});
  assert.equal(corrected.reply,local.reply);assert.equal(JSON.stringify(s),before);
  const context=makeContext(s,'sunspire','Full forces numbers?');assert.deepEqual(context.world.vassalMuster,report);
  const view=knowledgeView(s,'ashen');assert.deepEqual(vassalMuster(view,'ashen','sunspire'),report);
  assert.match(scriptedReply(view,'sunspire','What are your full forces numbers?').reply,/161 troops/);
});
test('Gemini cannot reinstate a generic border accusation against the current liege',()=>{
  const s=sworn(),raw={reply:'Your soldiers stand close to our frontier. Explain their purpose before you speak of friendship.',tone:'hostile',intents:[]};
  const corrected=relationshipResponse(s,'sunspire','I will position troops here.',raw);
  assert.match(corrected.reply,/My liege/);assert.doesNotMatch(corrected.reply,/Explain their purpose/);
  const friendly={reply:'My liege, our captains await your chosen objective.',tone:'neutral',intents:[]};
  assert.equal(relationshipResponse(s,'sunspire','I will position troops here.',friendly).reply,friendly.reply);
});
test('vassal reports stay private to the liege and never expose human vassal forces',()=>{
  const s=sworn();const split=splitCampaign(s);
  assert.ok(split.privateByHouse.ashen.view.vassalReports.sunspire);
  assert.deepEqual(split.world.vassalReports,{});assert.deepEqual(split.privateByHouse.wintermere.view.vassalReports,{});
  const view=playerView(split.world,split.privateByHouse.ashen,'ashen');assert.ok(view.vassalReports.sunspire);
  s.controllers={sunspire:{kind:'human',uid:'human-vassal'}};
  assert.equal(vassalMuster(s,'ashen','sunspire'),null);assert.deepEqual(knowledgeView(s,'ashen').vassalReports,{});
  assert.equal(makeContext(s,'sunspire','Your full forces?').world.vassalMuster,null);
});
test('the sworn role survives saves and ends with the directed treaty',()=>{
  const s=sworn();updatePoliticalState(s);const loaded=parseSave(JSON.stringify(s));
  assert.equal(politicalAttitude(loaded,'sunspire','ashen').label,'Sworn Vassal');
  loaded.turn=58;assert.notEqual(politicalAttitude(loaded,'sunspire','ashen').label,'Sworn Vassal');
  assert.equal(vassalMuster(loaded,'ashen','sunspire'),null);assert.equal(makeContext(loaded,'sunspire','Greetings').world.vassalRelationship,null);
  const reverse=sworn();reverse.treaties[0].liege='sunspire';reverse.treaties[0].vassal='ashen';
  assert.ok(borderThreat(reverse,'sunspire','ashen').score>20,'vassalage permissions are directional');
});

test('a same-turn submission releases sovereign war demands instead of sending a fresh defeat dispatch',()=>{
 const s=sworn();Object.assign(s.tiles['32,12'],{owner:'sunspire',building:'town',terrain:'plains'});
 s.tiles['32,21'].owner='ashen';s.militaryEvents.push({id:s.nextId++,turn:s.turn,action:'capture',attacker:'ashen',defender:'sunspire',tile:'32,21'});
 refreshKnowledge(s);updatePoliticalState(s);
 assert.ok(!(s.conversations.sunspire||[]).some(m=>['territory-loss','border','withdrawal'].includes(m.kind)));
 assert.equal(politicalAttitude(s,'sunspire','ashen').label,'Sworn Vassal');
});
