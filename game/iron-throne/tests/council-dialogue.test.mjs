import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame, kingdom, relation } from '../core.mjs';
import { foundCity, foundAIKingdoms, planFoundings } from '../founding.mjs';
import { refreshKnowledge, knowledgeView } from '../fog.mjs';
import { ownCouncil, appendCouncil } from '../council-state.mjs';
import { councilFacts, scriptedCouncil, sendCouncilMessage, beginCouncilMessage, makeCouncilContext, validateCouncilResponse } from '../alliance-council.mjs';
import { councilDiscussion } from '../council-dialogue.mjs';
import { DiplomacyClient } from '../chat.mjs';
import { followupCredit } from '../proposal-followup.mjs';
import { councilSystemPrompt, sanitizeCouncilContext } from '../worker/worker.mjs';

const offer = 'I can provide assistance King Oren, let us produce an alliance that aids one another and thus conquer the north!';
const warning = 'My frontier is threatened. I cannot strip its defenses for another offensive.';
let initial;
function setup() {
  if (initial) { const s=structuredClone(initial); return {s,c:s.allianceCouncils[0]}; }
  const s=createGame(8147,'random',8);
  foundCity(s,'ashen',planFoundings(s)[0].capital);foundAIKingdoms(s);
  for(const id of ['goldmere','redharbor'])s.treaties.push({id:`alliance-${s.nextId++}`,type:'alliance',parties:['ashen',id],expires:50});
  refreshKnowledge(s);
  const c=ownCouncil(s,'ashen',true);
  appendCouncil(s,c,'ashen','Let us produce an alliance to conquer the north.');
  appendCouncil(s,c,'goldmere','There may be advantage for my House in this. Send me your proposal; the burden and reward must be clear.');
  appendCouncil(s,c,'redharbor',warning);
  initial=structuredClone(s);
  return {s,c};
}
function threatenedView(s,c) {
  const view=knowledgeView(s,'ashen'), facts=councilFacts(s,c);
  facts.participants.find(p=>p.id==='redharbor').concern='frontier threatened';
  view.councilFacts={[c.id]:facts};
  return view;
}
const answer = (result,id='redharbor') => result.responses.find(r=>r.speakerHouseId===id)?.message;

test('the screenshot offer addresses Oren first and distinguishes receiving aid from sending an offensive',()=>{
  const {s,c}=setup(), view=threatenedView(s,c), before=JSON.stringify(view);
  const response=scriptedCouncil(view,c,'ashen',offer);
  assert.equal(response.responses[0].speakerHouseId,'redharbor');
  assert.match(answer(response),/offer of aid.*help defending my frontier/i);
  assert.doesNotMatch(answer(response),/cannot strip|another offensive/);
  assert.match(answer(response,'goldmere'),/King Oren.*offer of aid.*addressed to you/i);
  assert.equal(response.responses.some(r=>r.requestedIntent),false);
  assert.equal(JSON.stringify(view),before,'discussion does not mutate resources, trust, troops or agreements');
});

test('an unavailable Gemini uses the same contextual reply and spends no retry request',async()=>{
  const {s,c}=setup(),view=threatenedView(s,c);let calls=0;
  const client=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',fetcher:async()=>{calls++;return new Response('',{status:503});}});
  client.session={token:'test-session',expires:Date.now()+3600000};
  const result=await client.send(view,'goldmere',offer,'',true,{actorHouseId:'ashen',councilId:c.id});
  assert.equal(result.source,'scripted');assert.equal(calls,1);
  assert.match(answer(result),/offer of aid/i);
  const before=structuredClone({armies:s.armies,treaties:s.treaties,pledges:s.pledges,resources:kingdom(s,'ashen').resources,relationship:relation(s,'redharbor','ashen')});
  assert.equal(sendCouncilMessage(s,'ashen',c.id,offer,result).ok,true);
  assert.equal(s.diplomacy.messages.regular,1);
  assert.deepEqual({armies:s.armies,treaties:s.treaties,pledges:s.pledges,resources:kingdom(s,'ashen').resources,relationship:relation(s,'redharbor','ashen')},before);
  assert.equal(followupCredit(s,'ashen','redharbor',c.id),null,'a vague aid offer does not open an unrelated alliance proposal');
});

test('rulers advance repeated discussion, acknowledge corrections and retain concrete offer details',()=>{
  const {s,c}=setup();
  sendCouncilMessage(s,'ashen',c.id,offer);
  const first=c.messages.findLast(m=>m.speakerHouseId==='redharbor').message;
  const repeated=scriptedCouncil(s,c,'ashen',offer);
  assert.notEqual(answer(repeated),first);
  const clarification=scriptedCouncil(s,c,'ashen','I meant I would help you, King Oren.');
  assert.match(answer(clarification),/offering me help; I understand/i);
  const details=scriptedCouncil(s,c,'ashen','25 troops next turn, King Oren.');
  assert.match(answer(details),/25 troops next turn/);
  assert.match(answer(details),/where.*send/i);
  assert.doesNotMatch(answer(details),/how many|when could|have arrived|already sent/i);
});

test('requests, negation, third-party speech and stale or unrelated topics are not offers from the player',()=>{
  const {s,c}=setup(),facts=councilFacts(s,c);
  for(const message of ['Can you help me, King Oren?', 'I cannot provide aid, King Oren.', "I won't help you, King Oren.", 'Goldmere can send troops to King Oren.', 'I would like you to send troops to me.']) {
    assert.equal(councilDiscussion(facts,c,'ashen',message,s.turn).offeringAid,false,message);
  }
  sendCouncilMessage(s,'ashen',c.id,offer);
  assert.equal(councilDiscussion(facts,c,'ashen','When should we attack Vesper instead?',s.turn).offeringAid,false);
  assert.equal(councilDiscussion(facts,c,'ashen','I withdraw my offer.',s.turn).offeringAid,false);
  assert.doesNotMatch(scriptedCouncil(s,c,'ashen','I will not attack Vesper.').responses.map(r=>r.message).join(' '),/put the terms|send me your proposal|can your house provide/i);
  s.turn+=3;
  assert.equal(councilDiscussion(facts,c,'ashen','25 troops next turn.',s.turn).offeringAid,false);
});

test('conditional offers, existing alliances and sovereign constraints keep their meaning',()=>{
  const {s,c}=setup();
  assert.match(answer(scriptedCouncil(s,c,'ashen','King Oren, I will send 25 troops if we agree on a defensive plan.')),/conditions.*agreement/i);
  kingdom(s,'redharbor').resources.food=0;
  assert.match(answer(scriptedCouncil(s,c,'ashen','King Oren, strengthen our alliance.')),/already allied/i);
  assert.match(answer(scriptedCouncil(s,c,'ashen','King Oren, attack Vesper.')),/food/i);
  Object.assign(relation(s,'redharbor','ashen'),{trust:-40,grievance:60});
  assert.match(answer(scriptedCouncil(s,c,'ashen',offer)),/offer of help.*grievances/i);
});

test('repeat model replies are repaired per speaker at the authoritative append boundary',()=>{
  const {s,c}=setup();
  const repeated={responses:['goldmere','redharbor'].map(id=>({speakerHouseId:id,message:c.messages.findLast(m=>m.speakerHouseId===id).message}))};
  assert.equal(sendCouncilMessage(s,'ashen',c.id,offer,repeated).ok,true);
  for(const r of repeated.responses)assert.notEqual(c.messages.findLast(m=>m.speakerHouseId===r.speakerHouseId).message,r.message);
  assert.match(c.messages.findLast(m=>m.speakerHouseId==='redharbor').message,/offer of aid/i);
  const invalid={responses:[{speakerHouseId:'redharbor',message:'One reply.'},{speakerHouseId:'redharbor',message:'Another reply.'}]};
  assert.equal(validateCouncilResponse(invalid,c.participants,['redharbor','goldmere']),null);
});

test('single-player and multiplayer submit the same recent context without duplicating the current message',()=>{
  const {s,c}=setup(),before=makeCouncilContext(s,c,'ashen',offer);
  beginCouncilMessage(s,'ashen',c.id,offer);
  assert.deepEqual(makeCouncilContext(s,c,'ashen',offer),before);
  assert.ok(sanitizeCouncilContext(before));
  assert.match(councilSystemPrompt(),/offer to help a ruler from a request/);
});
