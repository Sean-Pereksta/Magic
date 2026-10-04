import test from 'node:test';
import assert from 'node:assert/strict';
import {createGame} from './fixtures/legacy-game.mjs';
import {onlineGame,activateForTest} from './fixtures/online-game.mjs';
import {atWar,declareWar,makePeace,relation,parseSave,treaty,kingdom} from '../core.mjs';
import {refreshKnowledge} from '../fog.mjs';
import {ownCouncil} from '../council-state.mjs';
import {makeCouncilContext} from '../alliance-council.mjs';
import {evaluateDeal,commitDeal,makeContext,validateIntent} from '../diplomacy.mjs';
import {submitFormalProposal,resolveFormalResponse,recordFormalVoice,inferFormalProposal,stageConversationProposal,ratifyFormalProposal,answerFormalProposal} from '../formal-proposals.mjs';
import {applyCommand} from '../multiplayer-commands.mjs';
import {sanitizeContext} from '../worker/worker.mjs';
const proposal=(s,id)=>s.cooperation.formalProposals.find(p=>p.id===id);
function friendly(s,a,b){for(const [x,y] of [[a,b],[b,a]])Object.assign(relation(s,x,y),{trust:95,opinion:95,reliability:95,grievance:0,aggression:0,wariness:0});}
function setup(){
 const s=createGame(311);for(const h of ['wintermere','redharbor','thornwall'])s.treaties.push({id:`ally-${h}`,type:'alliance',parties:['ashen',h],expires:100});
 for(const h of ['wintermere','redharbor']){declareWar(s,h,'sunspire');friendly(s,h,'sunspire');}
 for(const h of ['wintermere','redharbor','thornwall'])friendly(s,h,'ashen');
 declareWar(s,'ashen','sunspire');for(const h of ['wintermere','redharbor'])friendly(s,h,'sunspire');
 refreshKnowledge(s);return {s,c:ownCouncil(s,'ashen',true)};
}
const request=(c,houses=['wintermere'])=>({councilId:c?.id||null,requestedHouses:houses,direction:'request',intent:{type:'PEACE',targetId:'sunspire',duration:8}});
for(const mode of ['private','council'])test(`${mode}: mediated peace ends only each accepted recipient's war, persists and uses Gemini-only voice`,()=>{
 const {s,c}=setup(),before=s.kingdoms.map(k=>({...k.resources})),houses=mode==='council'?['wintermere','redharbor']:['wintermere'];
 const submitted=submitFormalProposal(s,'ashen',request(mode==='council'?c:null,houses));assert.equal(submitted.ok,true,submitted.error);
 for(const house of houses){
  const r=resolveFormalResponse(s,'ashen',submitted.proposalId,house),p=proposal(s,submitted.proposalId);assert.equal(r.status,'accepted',p.responses[house].message);
  assert.equal(atWar(s,house,'sunspire'),false);assert.equal(treaty(s,house,'sunspire','peace').expires,s.turn+8);
  const ctx=mode==='council'?makeCouncilContext(s,c,'ashen','Voice the recorded decision.',null,{proposalId:p.id,house}):makeContext(s,house,'Voice the recorded decision.',{actorHouseId:'ashen',formalDecision:{proposalId:p.id,house}});
  assert.ok(sanitizeContext(ctx));assert.equal(ctx.world.formalDecision.intent.targetId,'sunspire');assert.match(ctx.world.formalDecision.reason,/end their war/);
  recordFormalVoice(s,'ashen',p.id,house,'A local scripted peace reply','scripted');assert.equal(p.responses[house].spoken,false);assert.equal(p.responses[house].voice,undefined);
  assert.equal(resolveFormalResponse(s,'ashen',p.id,house).ok,false);
 }
 assert.equal(atWar(s,'ashen','sunspire'),true);if(mode==='private')assert.equal(atWar(s,'redharbor','sunspire'),true);
 assert.deepEqual(s.kingdoms.map(k=>k.resources),before);assert.equal(s.pledges.length,0);
 const restored=parseSave(JSON.stringify(s));assert.deepEqual(restored.wars,s.wars);assert.deepEqual(restored.cooperation.formalProposals,s.cooperation.formalProposals);
});
for(const party of ['wintermere','sunspire'])test(`${party} can decline without changing wars, resources or treaties`,()=>{
 const {s,c}=setup(),other=party==='wintermere'?'sunspire':'wintermere';Object.assign(relation(s,party,other),{trust:-100,opinion:-100,reliability:0,grievance:100});
 const before=JSON.stringify([s.wars,s.treaties,s.kingdoms.map(k=>k.resources)]),sent=submitFormalProposal(s,'ashen',request(c));
 const result=resolveFormalResponse(s,'ashen',sent.proposalId,'wintermere');assert.equal(result.status,'declined');assert.equal(proposal(s,sent.proposalId).responses.wintermere.reasonCodes[0],party==='wintermere'?'peace_terms_unacceptable':'target_declined_peace');
 assert.equal(JSON.stringify([s.wars,s.treaties,s.kingdoms.map(k=>k.resources)]),before);
});
for(const mode of ['private','council'])test(`${mode}: typed peace request is a draft until ratified`,()=>{
 const {s,c}=setup(),opts=mode==='council'?{councilId:c.id}:{ruler:'wintermere'};
 const staged=stageConversationProposal(s,'ashen','Wintermere, please make peace with House Sunspire for eight turns.',opts);assert.equal(staged.ok,true,staged.error);
 const p=proposal(s,staged.proposalId);assert.equal(p.approved,false);assert.deepEqual(p.requestedHouses,['wintermere']);assert.equal(p.intent.duration,8);assert.equal(atWar(s,'wintermere','sunspire'),true);
 assert.equal(resolveFormalResponse(s,'ashen',p.id,'wintermere').ok,false);assert.equal(ratifyFormalProposal(s,'ashen',p.id).ok,true);
 assert.equal(resolveFormalResponse(s,'ashen',p.id,'wintermere').status,'accepted');assert.equal(atWar(s,'wintermere','sunspire'),false);
});
test('peace phrasing preserves no-attack promises and ignores conditional, negated or player commitments',()=>{
 const {s,c}=setup();for(const text of ['Can you negotiate a peace treaty with Sunspire?','Please end your war against Sunspire.','Seek a truce with Sunspire.'])assert.equal(inferFormalProposal(s,'ashen',text,{ruler:'wintermere'}).intent.type,'PEACE');
 for(const text of ['Do not make peace with Sunspire.','Don’t make peace with Sunspire.','Make peace with Sunspire if they give us land.','I will end our war with Sunspire.','I’ll make peace with Sunspire.','Make peace with the faction.','Make peace with Ashen.','Make peace with Wintermere.'])assert.equal(inferFormalProposal(s,'ashen',text,{ruler:'wintermere'}),null,text);
 assert.equal(inferFormalProposal(s,'ashen','Promise not to attack Sunspire.',{ruler:'wintermere'}).intent.type,'PLEDGE_PEACE');
 const raw=inferFormalProposal(s,'ashen','Everyone, make peace with Redharbor.',{councilId:c.id});assert.deepEqual([...raw.requestedHouses].sort(),['thornwall','wintermere']);
});
test('invalid targets, self-peace, payments and reversed commitments are rejected before spending dispatches',()=>{
 const {s,c}=setup(),before=JSON.stringify(s);
 for(const targetId of ['ashen','wintermere','bad-id','5,6'])assert.equal(submitFormalProposal(s,'ashen',{...request(c),intent:{type:'PEACE',targetId}}).ok,false);
 for(const edit of [{giveAmount:10},{receiveAmount:10}])assert.equal(submitFormalProposal(s,'ashen',{...request(c),intent:{...request(c).intent,...edit}}).ok,false);
 assert.equal(submitFormalProposal(s,'ashen',{...request(c),direction:'offer'}).ok,false);assert.equal(JSON.stringify(s),before);
});
test('already peaceful, fallen or newly peaceful targets cannot create duplicate treaties',()=>{
 for(const change of ['peace','fallen']){
  const {s,c}=setup(),sent=submitFormalProposal(s,'ashen',request(c));if(change==='peace')makePeace(s,'wintermere','sunspire');else for(const tile of Object.values(s.tiles))if(tile.owner==='sunspire')tile.owner='ashen';
  const before=JSON.stringify([s.wars,s.treaties]);assert.equal(resolveFormalResponse(s,'ashen',sent.proposalId,'wintermere').status,'invalid');assert.equal(JSON.stringify([s.wars,s.treaties]),before);
 }
});
test('an offensive pledge blocks mediation rather than silently breaking a commitment',()=>{
 const {s,c}=setup();s.pledges.push({debtor:'wintermere',creditor:'ashen',status:'pending',intent:validateIntent({type:'JOINT_WAR',targetId:'sunspire'})});
 const sent=submitFormalProposal(s,'ashen',request(c));assert.equal(resolveFormalResponse(s,'ashen',sent.proposalId,'wintermere').status,'declined');assert.equal(atWar(s,'wintermere','sunspire'),true);assert.equal(proposal(s,sent.proposalId).responses.wintermere.reasonCodes[0],'peace_commitment_conflict');
});
test('ordinary bilateral peace remains intact and targeted intents cannot accidentally end the player war',()=>{
 const {s}=setup();friendly(s,'ashen','sunspire');
 assert.equal(evaluateDeal(s,'sunspire',{type:'PEACE'},'ashen').status,'accept');assert.equal(commitDeal(s,'sunspire',{type:'PEACE',targetId:'wintermere'},'ashen',{consentingHuman:true}).ok,false);
 assert.equal(atWar(s,'ashen','sunspire'),true);assert.equal(commitDeal(s,'sunspire',{type:'PEACE'},'ashen',{consentingHuman:true}).ok,true);assert.equal(atWar(s,'wintermere','sunspire'),true);
});
test('human target consent cannot be forged by a mediator',()=>{
 const {state:s,meta}=onlineGame(2),[actor,target]=Object.keys(meta.seats).filter(h=>meta.seats[h].kind==='human'),house=s.kingdoms.find(k=>s.controllers[k.id]?.kind==='ai').id;
 activateForTest(s,meta,actor);declareWar(s,house,target);friendly(s,house,target);
 const sent=submitFormalProposal(s,actor,{requestedHouses:[house],direction:'request',intent:{type:'PEACE',targetId:target}});assert.equal(sent.ok,true,sent.error);
 assert.equal(resolveFormalResponse(s,actor,sent.proposalId,house).status,'declined');assert.equal(proposal(s,sent.proposalId).responses[house].reasonCodes[0],'target_consent_required');assert.equal(atWar(s,house,target),true);
});
test('requested human ruler explicitly accepts through the existing authenticated multiplayer command',()=>{
 const {state:s,meta}=onlineGame(2),[actor,house]=Object.keys(meta.seats).filter(h=>meta.seats[h].kind==='human'),target=s.kingdoms.find(k=>s.controllers[k.id]?.kind==='ai').id;
 activateForTest(s,meta,actor);declareWar(s,house,target);friendly(s,house,target);
 const envelope={id:'peace-submit',clientId:'peace-test',sequence:1,uid:meta.seats[actor].uid,actorHouseId:actor,turn:s.turn,stateVersion:meta.stateVersion,epoch:meta.epoch,activationId:meta.activationId};
 const sent=applyCommand(s,meta,{...envelope,type:'formalSubmit',args:{requestedHouses:[house],direction:'request',intent:{type:'PEACE',targetId:target,duration:6}}});assert.equal(sent.ok,true,sent.error);
 assert.equal(proposal(s,sent.proposalId).responses[house].status,'awaiting-human');assert.equal(atWar(s,house,target),true);
 assert.equal(answerFormalProposal(s,actor,sent.proposalId,house,'accept').ok,false);
 activateForTest(s,meta,house);const accepted=applyCommand(s,meta,{...envelope,id:'peace-accept',actorHouseId:house,uid:meta.seats[house].uid,activationId:meta.activationId,type:'formalAnswer',args:{id:sent.proposalId,house,decision:'accept'}});
 assert.equal(accepted.ok,true,accepted.error);assert.equal(atWar(s,house,target),false);assert.equal(treaty(s,house,target,'peace').expires,s.turn+6);assert.ok(parseSave(JSON.stringify(s)));
});
test('Gemini cannot voice peace acceptance over a refusal or refusal over an active treaty',()=>{
 for(const accepted of [true,false]){
  const {s,c}=setup();if(!accepted)Object.assign(relation(s,'wintermere','sunspire'),{trust:-100,opinion:-100,grievance:100});
  const sent=submitFormalProposal(s,'ashen',request(c));assert.equal(resolveFormalResponse(s,'ashen',sent.proposalId,'wintermere').status,accepted?'accepted':'declined');
  const before=JSON.stringify([s.wars,s.treaties]);recordFormalVoice(s,'ashen',sent.proposalId,'wintermere',accepted?'I will not make peace with Sunspire.':'We will make peace with Sunspire.','gemini');
  const row=proposal(s,sent.proposalId).responses.wintermere;assert.equal(row.source,'failed');assert.equal(row.spoken,false);assert.equal(row.voice,undefined);assert.equal(JSON.stringify([s.wars,s.treaties]),before);
 }
});
