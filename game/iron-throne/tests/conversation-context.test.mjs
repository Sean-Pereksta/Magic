import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { appendConversation, contact, changeRelation, updatePoliticalState } from '../living.mjs';
import { makeContext, relationshipResponse } from '../diplomacy.mjs';
import { conversationWindow } from '../conversation-context.mjs';
import { sanitizeContext, systemPrompt } from '../worker/worker.mjs';
test('active dialogue has turns; decades-old speech is historical memory and notices are not dialogue',()=>{
  const s=createGame();appendConversation(s,'wintermere','player','I once offered an alliance.');
  s.turn=45;appendConversation(s,'wintermere','player','Our current subject is trade.');
  s.turn=47;appendConversation(s,'wintermere','ruler','We can discuss trade.');
  changeRelation(s,'wintermere','ashen',{wariness:9,fear:21},'Armies approached.');
  const ctx=makeContext(s,'wintermere','What are your terms?');
  assert.equal(ctx.history.length,2);assert.ok(ctx.history.every(m=>m.turn>=45));
  assert.doesNotMatch(JSON.stringify(ctx.history),/once offered|WARINESS|FEAR/);
  assert.match(ctx.memories.join(' '),/Historical dialogue.*turn 1.*offered an alliance/);
  assert.equal(ctx.world.conversationMode,'player-message');assert.ok(sanitizeContext(ctx));
});
test('new dispatches identify their reason, turn and correct army/territory owners',()=>{
  const s=createGame();s.turn=47;contact(s,'wintermere','border',"House Ashen's armies approach House Wintermere's land.",4,'ashen');
  const event=s.conversations.wintermere.at(-1).dispatch;
  assert.equal(event.mode,'ai-initiated');assert.equal(event.turn,47);assert.equal(event.armyOwner,'ashen');assert.equal(event.territoryOwner,'wintermere');
  const ctx=makeContext(s,'wintermere','Deliver the supplied dispatch.',{event});
  assert.equal(ctx.world.conversationMode,'ai-initiated-dispatch');assert.equal(ctx.world.dispatch.reason,'border');
  assert.equal(ctx.world.militaryRelationship.armyOwner,'ashen');assert.equal(ctx.world.militaryRelationship.territoryOwner,'wintermere');
});
test('unsolicited model replies cannot invent a fresh player request from old speech',()=>{
  const s=createGame();appendConversation(s,'wintermere','player','Let us be friends.');s.turn=47;
  const event={kind:'border',text:"House Ashen's armies are near House Wintermere's frontier."};
  const out=relationshipResponse(s,'wintermere','Deliver dispatch.',{reply:'You ask for friendship while your armies gather.',intents:[],tone:'guarded'},{event});
  assert.equal(out.reply,event.text);assert.match(systemPrompt('wintermere'),/Older dialogue.*historical/);
  appendConversation(s,'wintermere','player','Let us be friends.');
  const current=relationshipResponse(s,'wintermere','Deliver dispatch.',{reply:'You ask for friendship while your armies gather.',intents:[],tone:'guarded'},{event});
  assert.match(current.reply,/You ask/);
});
test('undated legacy messages are historical and cannot be relabeled as the current conversation',()=>{
  const w=conversationWindow([{role:'player',text:'Old undated offer.'}],47);assert.deepEqual(w.history,[]);assert.match(w.historical[0],/undated/);
  const s=createGame(),ctx=makeContext(s,'wintermere','Hello.');
  assert.equal(sanitizeContext({...ctx,history:[{role:'player',text:'Old offer.'}]}),null);
});
