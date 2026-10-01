import test from 'node:test';
import {readFile} from 'node:fs/promises';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { onlineGame, activateForTest } from './fixtures/online-game.mjs';
import { atWar, kingdom, relation, parseSave, settlements } from '../core.mjs';
import { refreshKnowledge, knowledgeView } from '../fog.mjs';
import { ownCouncil } from '../council-state.mjs';
import { submitFormalProposal, ratifyFormalProposal, dismissFormalProposal, resolveFormalResponse, evaluateCouncilProposal, answerFormalProposal, inferFormalProposal, stageConversationProposal, runFormalResponseQueue, recordFormalVoice } from '../formal-proposals.mjs';
import { pruneCooperation } from '../cooperation-state.mjs';
import { grantFollowup } from '../proposal-followup.mjs';
import { makeCouncilContext } from '../alliance-council.mjs';
import { sanitizeContext } from '../worker/worker.mjs';
import { splitCampaign } from '../multiplayer-state.mjs';
import { applyCommand, COMMAND_TYPES } from '../multiplayer-commands.mjs';
import { commitDeal, validateIntent, makeContext } from '../diplomacy.mjs';
function setup(){const s=createGame(311);for(const h of ['wintermere','redharbor','thornwall']){s.treaties.push({id:`ally-${h}`,type:'alliance',parties:['ashen',h],expires:100});Object.assign(relation(s,h,'ashen'),{trust:95,opinion:95,reliability:95,grievance:0});Object.assign(relation(s,'ashen',h),{trust:95,opinion:95,reliability:95,grievance:0});}for(const k of s.kingdoms){k.resources.food=k.resources.gold=k.resources.iron=500;k.commands=0;}refreshKnowledge(s);return {s,c:ownCouncil(s,'ashen',true)};}
const proposal=(s,id)=>s.cooperation.formalProposals.find(p=>p.id===id);
const request=(c,intent,houses=['wintermere','redharbor','thornwall'])=>({councilId:c.id,requestedHouses:houses,direction:'request',intent});

test('explicit proposals resolve independently without a second player ratification',()=>{
 const {s,c}=setup();s.treaties.push({id:'protection',type:'non-aggression',parties:['redharbor','sunspire'],expires:100});
 s.pledges.push({id:'existing',debtor:'thornwall',creditor:'ashen',intent:validateIntent({type:'POSITION',targetId:'5,6'}),status:'pending',deadline:10,created:1,held:0});
 const r=submitFormalProposal(s,'ashen',request(c,{type:'JOINT_WAR',targetId:'sunspire',duration:10}));assert.equal(r.ok,true,r.error);const p=proposal(s,r.proposalId);assert.equal(p.approved,true);
 assert.equal(resolveFormalResponse(s,'ashen',p.id,'thornwall').ok,false,'cannot jump the queue');
 for(const h of p.requestedHouses)assert.equal(resolveFormalResponse(s,'ashen',p.id,h).ok,true);
 assert.equal(p.responses.wintermere.status,'accepted');assert.ok(atWar(s,'wintermere','sunspire'));assert.equal(s.pledges.filter(x=>x.formalProposalId===p.id).length,1);
 assert.ok(['declined','alternative'].includes(p.responses.redharbor.status));assert.ok(p.responses.redharbor.reasonCodes.includes('treaty_conflict'));
 assert.ok(p.responses.thornwall.reasonCodes.includes('military_committed'));assert.equal(atWar(s,'redharbor','sunspire'),false);assert.equal(p.status,'resolved');
 assert.equal(resolveFormalResponse(s,'ashen',p.id,'wintermere').ok,false);assert.equal(ratifyFormalProposal(s,'ashen',p.id).ok,false);
});
test('an AI support offer transfers its exact resource package immediately and once',()=>{
 const {s,c}=setup();s.treaties.push({id:'protected',type:'alliance',parties:['wintermere','sunspire'],expires:100});
 const r=submitFormalProposal(s,'ashen',request(c,{type:'JOINT_WAR',targetId:'sunspire'},['wintermere']));resolveFormalResponse(s,'ashen',r.proposalId,'wintermere');const p=proposal(s,r.proposalId),row=p.responses.wintermere;
 assert.equal(row.status,'alternative');assert.equal(row.alternativeIntents[0].type,'AID');assert.ok(row.alternativeIntents[0].giveItems.length>1);
 const before={...kingdom(s,'ashen').resources},offered=row.alternativeIntents[0].giveItems;
 assert.equal(answerFormalProposal(s,'ashen',p.id,'wintermere','accept').ok,true);
 for(const item of offered)assert.equal(kingdom(s,'ashen').resources[item.resource],before[item.resource]+item.amount);
 assert.equal(answerFormalProposal(s,'ashen',p.id,'wintermere','accept').ok,false);assert.equal(atWar(s,'wintermere','sunspire'),false);
});
test('alternative offers revalidate resources atomically and outsiders cannot accept',()=>{
 const {s,c}=setup();s.treaties.push({id:'protected',type:'alliance',parties:['wintermere','sunspire'],expires:100});
 const r=submitFormalProposal(s,'ashen',request(c,{type:'JOINT_WAR',targetId:'sunspire'},['wintermere']));resolveFormalResponse(s,'ashen',r.proposalId,'wintermere');
 assert.equal(answerFormalProposal(s,'thornwall',r.proposalId,'wintermere','accept').ok,false);
 kingdom(s,'wintermere').resources.iron=0;const before=JSON.stringify(s);assert.equal(answerFormalProposal(s,'ashen',r.proposalId,'wintermere','accept').ok,false);assert.equal(JSON.stringify(s),before);
});
test('several independent allies can supply one ruler in the same turn',()=>{
 const {s,c}=setup();for(const h of ['wintermere','redharbor'])s.treaties.push({id:`protected-${h}`,type:'alliance',parties:[h,'sunspire'],expires:100});
 const result=submitFormalProposal(s,'ashen',request(c,{type:'JOINT_WAR',targetId:'sunspire'},['wintermere','redharbor']));
 for(const h of ['wintermere','redharbor'])resolveFormalResponse(s,'ashen',result.proposalId,h);
 for(const h of ['wintermere','redharbor'])assert.equal(answerFormalProposal(s,'ashen',result.proposalId,h,'accept').ok,true);
 assert.equal(proposal(s,result.proposalId).responses.redharbor.offerAnswered,'accepted');
});
test('an offered defense binds the player, without treating it as an AI army request',()=>{
 const {s,c}=setup();kingdom(s,'wintermere').resources.food=0;
 const r=submitFormalProposal(s,'ashen',{...request(c,{type:'DEFEND',targetId:'17,4'},['wintermere']),direction:'offer'});
 resolveFormalResponse(s,'ashen',r.proposalId,'wintermere');assert.equal(proposal(s,r.proposalId).responses.wintermere.status,'accepted');
 assert.equal(s.pledges.at(-1).debtor,'ashen');assert.equal(s.pledges.at(-1).intent.targetId,'17,4');
});
test('private attack requests can name a public capital, and contradictory model voices cannot change the result',()=>{
 const {s}=setup();const r=stageConversationProposal(s,'ashen','I want you to attack Solstice within ten turns.',{ruler:'wintermere'});
 assert.equal(ratifyFormalProposal(s,'ashen',r.proposalId).ok,true);resolveFormalResponse(s,'ashen',r.proposalId,'wintermere');
 const p=proposal(s,r.proposalId);assert.equal(p.responses.wintermere.status,'accepted');assert.equal(s.pledges.at(-1).debtor,'wintermere');
 recordFormalVoice(s,'ashen',p.id,'wintermere','I refuse to commit to this campaign.');assert.equal(p.responses.wintermere.voice,p.responses.wintermere.message);
 assert.doesNotMatch(p.responses.wintermere.message,/ratif|must accept/i);
});
test('expired proposals release pending slots, and bounded Council reactions do not create obligations',()=>{
 const {s,c}=setup();s.treaties.push({id:'protected',type:'alliance',parties:['redharbor','sunspire'],expires:100});
 const r=submitFormalProposal(s,'ashen',request(c,{type:'JOINT_WAR',targetId:'sunspire'},['wintermere','redharbor']));
 for(const h of ['wintermere','redharbor'])resolveFormalResponse(s,'ashen',r.proposalId,h);
 const p=proposal(s,r.proposalId);assert.equal(p.reactions,1);assert.equal(c.messages.filter(m=>m.formalProposalId===p.id&&m.speakerHouseId!=='ashen').length,1);
 assert.equal(s.pledges.filter(x=>x.formalProposalId===p.id).length,1);
 const draft=submitFormalProposal(s,'ashen',request(c,{type:'ACCESS'},['thornwall']),{inferred:true});s.turn+=4;pruneCooperation(s);
 assert.equal(proposal(s,draft.proposalId).status,'dismissed');assert.ok(parseSave(JSON.stringify(s)));
});
test('chat-inferred requests execute nothing until ratified; dismissal and modifications stay explicit',()=>{
 const {s,c}=setup();const raw=inferFormalProposal(s,'ashen','Let us declare war against Sunspire within ten turns.',{councilId:c.id});assert.equal(raw.intent.type,'JOINT_WAR');assert.equal(raw.intent.duration,10);
 const r=submitFormalProposal(s,'ashen',raw,{inferred:true}),p=proposal(s,r.proposalId);assert.equal(p.source,'conversation_inferred');assert.equal(p.approved,false);assert.deepEqual(s.wars,[]);assert.equal(resolveFormalResponse(s,'ashen',p.id,'wintermere').ok,false);
 assert.equal(dismissFormalProposal(s,'ashen',p.id).ok,true);assert.deepEqual(s.wars,[]);
 const altered=submitFormalProposal(s,'ashen',{...raw,intent:{type:'POSITION',targetId:'5,6',duration:5},requestedHouses:['wintermere']});assert.equal(altered.ok,true);resolveFormalResponse(s,'ashen',altered.proposalId,'wintermere');assert.deepEqual(s.wars,[]);assert.equal(s.pledges.at(-1).intent.targetId,'5,6');assert.equal(s.pledges.at(-1).deadline,6);
});
test('private conversation parser preserves exact known location and requires ratification',()=>{
 const {s}=setup(),r=stageConversationProposal(s,'ashen','I want you to attack Solstice within ten turns.',{ruler:'wintermere'}),p=proposal(s,r.proposalId);
 assert.equal(p.intent.type,'PLEDGE_ATTACK');assert.equal(p.targetTile,'32,21');assert.equal(p.direction,'request');assert.equal(p.status,'draft');assert.equal(s.pledges.length,0);
 const supply=inferFormalProposal(s,'ashen','Can you provide fifty food for the invasion?',{ruler:'wintermere'});assert.equal(supply.intent.type,'AID');assert.equal(supply.intent.giveAmount,50);
 const peace=inferFormalProposal(s,'ashen',"Promise me you won't attack Wintermere for the next twelve turns.",{ruler:'redharbor'});assert.equal(peace.intent.type,'PLEDGE_PEACE');assert.equal(peace.intent.duration,12);
});
test('a Council map proposal retains its tile without granting any observations',()=>{
 const {s,c}=setup(),tile=Object.values(knowledgeView(s,'ashen').tiles).find(t=>t.fog==='unknown'&&!t.knownCapital).id,old=JSON.stringify(s.fog);
 const r=submitFormalProposal(s,'ashen',request(c,{type:'POSITION',targetId:tile,duration:10},['wintermere']));assert.equal(r.ok,true);assert.equal(proposal(s,r.proposalId).targetTile,tile);assert.equal(JSON.stringify(s.fog),old);
 const split=splitCampaign(s);assert.equal(split.world.cooperation.formalProposals.length,0);assert.equal(split.privateByHouse.sunspire.view.cooperation.formalProposals.length,0);assert.equal(split.privateByHouse.wintermere.view.cooperation.formalProposals[0].targetTile,tile);
});
test('formal proposal state and independent statuses survive save/load',()=>{
 const {s,c}=setup(),r=submitFormalProposal(s,'ashen',request(c,{type:'POSITION',targetId:'5,6'},['wintermere']));resolveFormalResponse(s,'ashen',r.proposalId,'wintermere');
 assert.deepEqual(parseSave(JSON.stringify(s)).cooperation.formalProposals,s.cooperation.formalProposals);
 const bad=structuredClone(s);bad.cooperation.formalProposals[0].requestedHouses.push('sunspire');assert.throws(()=>parseSave(JSON.stringify(bad)),/formal/);
});
test('response queue stays sequential and continues after voice failure',async()=>{
 const work=['wintermere','redharbor','thornwall'],events=[];let i=0;
 await runFormalResponseQueue({next:()=>i<work.length?{id:'test',house:work[i]}:null,resolve:async item=>{events.push(`resolve:${item.house}`);i++;return {ok:true};},voice:async item=>{events.push(`voice:${item.house}`);if(item.house==='wintermere')throw Error('timeout');return 'Recorded';},record:async item=>events.push(`record:${item.house}`)});
 assert.deepEqual(events,['resolve:wintermere','voice:wintermere','resolve:redharbor','voice:redharbor','record:redharbor','resolve:thornwall','voice:thornwall','record:thornwall']);
});
test('formal Council followups honor a ruler’s one-use invitation after dispatches run out',()=>{
 const {s,c}=setup();s.diplomacy.messages.regular=3;
 assert.equal(grantFollowup(s,'ashen','wintermere',c.id,'Attack Sunspire.',{reply:'Put the terms before me.'},{paid:true,requestedIntent:validateIntent({type:'JOINT_WAR',targetId:'sunspire'})}),true);
 const r=submitFormalProposal(s,'ashen',request(c,{type:'JOINT_WAR',targetId:'sunspire'},['wintermere']));assert.equal(r.ok,true,r.error);assert.equal(s.diplomacy.messages.regular,3);
 assert.equal(s.proposalFollowups['ashen:wintermere'],undefined);
});
test('model contexts receive only the current ruler’s authoritative formal decision',()=>{
 const {s,c}=setup(),r=submitFormalProposal(s,'ashen',request(c,{type:'POSITION',targetId:'5,6'},['wintermere','redharbor']));resolveFormalResponse(s,'ashen',r.proposalId,'wintermere');
 const ctx=makeCouncilContext(s,c,'ashen','Voice the recorded decision.',null,{proposalId:r.proposalId,house:'wintermere'});
 assert.ok(sanitizeContext(ctx));assert.deepEqual(ctx.world.participants.filter(x=>x.ai).map(x=>x.id),['wintermere']);assert.equal(ctx.world.formalDecision.status,'accepted');assert.equal(ctx.world.formalDecision.targetTile,'5,6');
 const direct=submitFormalProposal(s,'ashen',{requestedHouses:['thornwall'],direction:'offer',intent:{type:'AID',giveResource:'food',giveAmount:20}});resolveFormalResponse(s,'ashen',direct.proposalId,'thornwall');
 const privateCtx=makeContext(s,'thornwall','Voice the recorded decision.',{actorHouseId:'ashen',formalDecision:{proposalId:direct.proposalId,house:'thornwall'}});
 assert.ok(sanitizeContext(privateCtx));assert.equal(privateCtx.world.formalDecision.status,'accepted');assert.equal(privateCtx.world.formalDecision.intent.giveAmount,20);
});
test('multiplayer formal submissions and acceptance use authenticated House ownership',()=>{
 const {state:s,meta}=onlineGame(2),[actor,recipient]=Object.keys(meta.seats).filter(h=>meta.seats[h].kind==='human');activateForTest(s,meta,actor);
 const envelope={id:'formal-submit',clientId:'formal-proposal-test',sequence:1,uid:meta.seats[actor].uid,actorHouseId:actor,turn:s.turn,stateVersion:meta.stateVersion,epoch:meta.epoch,activationId:meta.activationId};
 const r=applyCommand(s,meta,{...envelope,type:'formalSubmit',args:{requestedHouses:[recipient],intent:{type:'AID',giveAmount:20,giveResource:'food'},direction:'offer'}});assert.equal(r.ok,true,r.error);assert.equal(proposal(s,r.proposalId).responses[recipient].status,'awaiting-human');
 assert.equal(applyCommand(s,meta,{...envelope,id:'forged',sequence:2,type:'formalAnswer',args:{id:r.proposalId,house:recipient,decision:'accept'}}).ok,false);
 activateForTest(s,meta,recipient);const accept={...envelope,id:'formal-accept',uid:meta.seats[recipient].uid,actorHouseId:recipient,activationId:meta.activationId,type:'formalAnswer',args:{id:r.proposalId,house:recipient,decision:'accept'}};assert.equal(applyCommand(s,meta,accept).ok,true);assert.equal(proposal(s,r.proposalId).responses[recipient].status,'accepted');
});

test('Firestore accepts the same authenticated command vocabulary as the controller',async()=>{
 const rules=await readFile(new URL('../../../firestore.rules',import.meta.url),'utf8');
 const allowlist=[...rules.matchAll(/request\.resource\.data\.type in \[([^\]]+)\]/g)].find(m=>m[1].includes("'generalHire'"))[1];
 assert.deepEqual([...allowlist.matchAll(/'([^']+)'/g)].map(m=>m[1]).sort(),[...COMMAND_TYPES].sort());
});
