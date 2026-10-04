import test from 'node:test';
import {readFile} from 'node:fs/promises';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { onlineGame, activateForTest } from './fixtures/online-game.mjs';
import { atWar, kingdom, relation, parseSave, settlements } from '../core.mjs';
import { refreshKnowledge, knowledgeView } from '../fog.mjs';
import { ownCouncil } from '../council-state.mjs';
import { submitFormalProposal, ratifyFormalProposal, dismissFormalProposal, resolveFormalResponse, evaluateCouncilProposal, answerFormalProposal, inferFormalProposal, stageConversationProposal, runFormalResponseQueue, recordFormalVoice, formalDescription, markFormalConsidering, retryFormalVoice, formalConversationCurrent, isFormalCouncilBusy, cancelFormalConversation } from '../formal-proposals.mjs';
import { pruneCooperation } from '../cooperation-state.mjs';
import { grantFollowup } from '../proposal-followup.mjs';
import { makeCouncilContext, beginCouncilMessage, finalizeCouncilMessage } from '../alliance-council.mjs';
import { sanitizeContext } from '../worker/worker.mjs';
import { splitCampaign } from '../multiplayer-state.mjs';
import { applyCommand, COMMAND_TYPES } from '../multiplayer-commands.mjs';
import { commitDeal, validateIntent, makeContext, resolveRecurringTrade, verifyPledges } from '../diplomacy.mjs';
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
 recordFormalVoice(s,'ashen',p.id,'wintermere','I refuse to commit to this campaign.','gemini');assert.equal(p.responses.wintermere.voice,undefined);assert.equal(p.responses.wintermere.voiceComplete,true);assert.equal(p.responses.wintermere.source,'failed');
 assert.doesNotMatch(p.responses.wintermere.message,/ratif|must accept/i);
});
test('expired proposals release pending slots without adding scripted Council reactions',()=>{
 const {s,c}=setup();s.treaties.push({id:'protected',type:'alliance',parties:['redharbor','sunspire'],expires:100});
 const r=submitFormalProposal(s,'ashen',request(c,{type:'JOINT_WAR',targetId:'sunspire'},['wintermere','redharbor']));
 for(const h of ['wintermere','redharbor'])resolveFormalResponse(s,'ashen',r.proposalId,h);
 const p=proposal(s,r.proposalId);assert.equal(p.reactions,0);
 for(const h of p.requestedHouses)recordFormalVoice(s,'ashen',p.id,h,'The recorded commitment reflects my position.','gemini');
 assert.equal(p.reactions,0);assert.equal(c.messages.filter(m=>m.formalProposalId===p.id&&m.speakerHouseId!=='ashen').length,2);
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
 await runFormalResponseQueue({gapMs:0,next:()=>i<work.length?{id:'test',house:work[i]}:null,resolve:async item=>{events.push(`resolve:${item.house}`);i++;return {ok:true};},voice:async item=>{events.push(`voice:${item.house}`);if(item.house==='wintermere')throw Error('timeout');return 'Recorded';},record:async item=>events.push(`record:${item.house}`)});
 assert.deepEqual(events,['resolve:wintermere','voice:wintermere','record:wintermere','resolve:redharbor','voice:redharbor','record:redharbor','resolve:thornwall','voice:thornwall','record:thornwall']);
});
test('formal voices commit progressively after authoritative accepted, declined and alternative decisions',async()=>{
 const {s,c}=setup(),houses=['wintermere','redharbor','thornwall'];
 for(const h of houses.slice(1))s.treaties.push({id:`protected-${h}`,type:'non-aggression',parties:[h,'sunspire'],expires:100});
 Object.assign(relation(s,'redharbor','ashen'),{trust:0});
 const submitted=submitFormalProposal(s,'ashen',request(c,{type:'JOINT_WAR',targetId:'sunspire'},houses)),p=proposal(s,submitted.proposalId),events=[],contexts=[],pauses=[],snapshots=[];
 assert.equal(isFormalCouncilBusy(s,c.id),true);
 const result=await runFormalResponseQueue({
  next:()=>{const house=houses.find(h=>!p.responses[h].spoken&&!p.responses[h].voiceComplete);return house?{id:p.id,house}:null;},
  isCurrent:()=>formalConversationCurrent(s,p),
  onStatus:(item,status)=>{if(status==='considering'){assert.equal(markFormalConsidering(s,'ashen',item.id,item.house).ok,true);assert.equal(p.responses[item.house].status,'considering');}if(status==='resolved')snapshots.push(houses.map(h=>p.responses[h].status));},
  resolve:item=>{events.push(`resolve:${item.house}`);return resolveFormalResponse(s,'ashen',item.id,item.house);},
  voice:async item=>{
   events.push(`voice:${item.house}`);assert.ok(['accepted','declined','alternative'].includes(p.responses[item.house].status));
   const ctx=makeCouncilContext(s,c,'ashen','Voice the recorded decision.',null,{proposalId:p.id,house:item.house});contexts.push(ctx);
   assert.deepEqual(ctx.world.participants.filter(x=>x.ai).map(x=>x.id),[item.house]);
   if(item.house==='redharbor')throw Object.assign(Error('Gemini timed out'),{code:'GEMINI_TIMEOUT'});
   return {message:item.house==='wintermere'?'You have my word. Wintermere will join the campaign.':'My House can send the offered supplies instead.',source:'gemini'};
  },
  fallback:(item,error)=>{assert.equal(error.code,'GEMINI_TIMEOUT');return {message:'',source:'failed',diagnostic:{version:1,code:error.code,httpStatus:503,path:'/diplomacy'}};},
  record:(item,reply)=>{events.push(`record:${item.house}`);return recordFormalVoice(s,'ashen',item.id,item.house,reply.message,reply.source,reply.diagnostic);},
  pause:async ms=>{pauses.push(ms);assert.equal(houses.filter(h=>p.responses[h].voiceComplete).length,pauses.length);}
 });
 assert.equal(result.ok,true);assert.deepEqual(pauses,[750,750]);
 assert.deepEqual(snapshots,[['accepted','waiting','waiting'],['accepted','declined','waiting'],['accepted','declined','alternative']]);
 assert.deepEqual(events,houses.flatMap(h=>[`resolve:${h}`,`voice:${h}`,`record:${h}`]));
 const spoken=c.messages.filter(m=>m.formalResponse);assert.deepEqual(spoken.map(m=>m.speakerHouseId),['wintermere','thornwall']);assert.deepEqual(spoken.map(m=>m.source),['gemini','gemini']);
 assert.equal(p.responses.redharbor.diagnostic.code,'GEMINI_TIMEOUT');assert.equal(p.responses.redharbor.voiceComplete,true);assert.equal(p.responses.redharbor.spoken,false);assert.equal(p.responses.redharbor.diagnostic.httpStatus,503);
 assert.ok(contexts[1].history.some(m=>m.message===spoken[0].message));
 assert.ok(contexts[2].history.some(m=>m.message===spoken[0].message));assert.equal(contexts[2].history.some(m=>m.speakerHouseId==='redharbor'),false);
 assert.equal(isFormalCouncilBusy(s,c.id),false);assert.deepEqual(parseSave(JSON.stringify(s)).cooperation.formalProposals,s.cooperation.formalProposals);
 const count=c.messages.length;assert.equal(recordFormalVoice(s,'ashen',p.id,'wintermere','Duplicate response','gemini').duplicate,true);assert.equal(c.messages.length,count);
});
test('formal cancellation keeps committed replies and decisions but prevents stale voices or later rulers',async()=>{
 const {s,c}=setup(),houses=['wintermere','redharbor','thornwall'];
 const submitted=submitFormalProposal(s,'ashen',request(c,{type:'POSITION',targetId:'5,6'},houses)),p=proposal(s,submitted.proposalId),voiced=[];
 const result=await runFormalResponseQueue({gapMs:0,isCurrent:()=>formalConversationCurrent(s,p),next:()=>{const house=houses.find(h=>!p.responses[h].spoken&&!p.responses[h].voiceComplete);return house?{id:p.id,house}:null;},resolve:item=>resolveFormalResponse(s,'ashen',item.id,item.house),voice:async item=>{voiced.push(item.house);if(item.house==='redharbor')s.turn++;return 'My commitment stands.';},record:(item,message)=>recordFormalVoice(s,'ashen',item.id,item.house,message,'gemini')});
 assert.equal(result.cancelled,true);assert.deepEqual(voiced,['wintermere','redharbor']);
 assert.deepEqual(c.messages.filter(m=>m.formalResponse).map(m=>m.speakerHouseId),['wintermere']);
 assert.equal(p.responses.redharbor.status,'accepted');assert.equal(p.responses.redharbor.spoken,false);assert.equal(p.responses.thornwall.status,'waiting');
 assert.equal(resolveFormalResponse(s,'ashen',p.id,'thornwall').ok,false);assert.equal(recordFormalVoice(s,'ashen',p.id,'redharbor','Late voice').ok,false);assert.equal(isFormalCouncilBusy(s,c.id),false);
});
test('formal considering survives save/load and conflicting submissions remain locked through the last voice',()=>{
 const {s,c}=setup(),raw=request(c,{type:'POSITION',targetId:'5,6'},['wintermere']),r=submitFormalProposal(s,'ashen',raw),p=proposal(s,r.proposalId);
 assert.equal(markFormalConsidering(s,'ashen',p.id,'wintermere').ok,true);assert.equal(parseSave(JSON.stringify(s)).cooperation.formalProposals[0].responses.wintermere.status,'considering');
 assert.equal(submitFormalProposal(s,'ashen',raw).ok,false);assert.equal(resolveFormalResponse(s,'ashen',p.id,'wintermere').ok,true);assert.equal(p.status,'resolved');
 assert.equal(submitFormalProposal(s,'ashen',raw).ok,false,'recorded decision still awaits its voice');
 assert.equal(beginCouncilMessage(s,'ashen',c.id,'Another message').ok,false,'ordinary council chat waits for the final formal voice');
 assert.equal(recordFormalVoice(s,'ashen',p.id,'wintermere','I refuse to join.','gemini').ok,true);assert.equal(p.responses.wintermere.source,'failed');assert.equal(p.responses.wintermere.voiceComplete,true);assert.equal(c.messages.filter(m=>m.formalResponse).length,0);
 assert.equal(submitFormalProposal(s,'ashen',raw).ok,true);
});
test('ordinary active council sequences prevent overlapping formal submissions',()=>{
 const {s,c}=setup(),start=beginCouncilMessage(s,'ashen',c.id,'What should our council discuss?');assert.equal(start.ok,true);
 const raw=request(c,{type:'POSITION',targetId:'5,6'},['wintermere']);assert.equal(submitFormalProposal(s,'ashen',raw).ok,false);
 assert.equal(finalizeCouncilMessage(s,'ashen',start,{cancelled:true}).ok,true);assert.equal(submitFormalProposal(s,'ashen',raw).ok,true);
});
test('ending an activation releases formal conversation locks without undoing completed commitments',()=>{
 const {s,c}=setup(),submitted=submitFormalProposal(s,'ashen',request(c,{type:'POSITION',targetId:'5,6'})),p=proposal(s,submitted.proposalId);
 for(const house of ['wintermere','redharbor'])assert.equal(resolveFormalResponse(s,'ashen',p.id,house).ok,true);
 assert.equal(recordFormalVoice(s,'ashen',p.id,'wintermere','My commitment stands.','gemini').ok,true);
 const count=c.messages.length,pledges=s.pledges.length;
 assert.equal(cancelFormalConversation(s,'ashen'),1);assert.equal(p.conversationCancelled,true);assert.equal(p.status,'resolved');
 assert.equal(p.responses.wintermere.spoken,true);assert.equal(p.responses.redharbor.status,'accepted');assert.equal(p.responses.redharbor.spoken,false);assert.equal(p.responses.thornwall.status,'invalid');
 assert.equal(s.pledges.length,pledges);assert.equal(c.messages.length,count);assert.equal(formalConversationCurrent(s,p),false);assert.equal(isFormalCouncilBusy(s,c.id),false);
 assert.equal(recordFormalVoice(s,'ashen',p.id,'redharbor','Late reply').ok,false);assert.equal(cancelFormalConversation(s,'ashen'),0);assert.equal(parseSave(JSON.stringify(s)).cooperation.formalProposals[0].conversationCancelled,true);
 assert.equal(beginCouncilMessage(s,'ashen',c.id,'The previous conversation has concluded.').ok,true);
});
test('resuming a resolved unspoken formal decision marks its reply unavailable without repeating its paid request',async()=>{
 const {s,c}=setup(),houses=['wintermere','redharbor'],submitted=submitFormalProposal(s,'ashen',request(c,{type:'POSITION',targetId:'5,6'},houses)),p=proposal(s,submitted.proposalId),paid=[];
 assert.equal(resolveFormalResponse(s,'ashen',p.id,'wintermere').ok,true);
 const result=await runFormalResponseQueue({gapMs:0,isCurrent:()=>formalConversationCurrent(s,p),
  next:()=>{const house=houses.find(h=>!p.responses[h].spoken&&!p.responses[h].voiceComplete);return house?{id:p.id,house,resumeVoice:p.responses[house].status!=='waiting'}:null;},
  resolve:item=>item.resumeVoice?{ok:true}:resolveFormalResponse(s,'ashen',item.id,item.house),
  voice:async item=>{paid.push(item.house);return {message:'My commitment stands.',source:'gemini'};},
  fallback:item=>({message:'',source:'failed'}),
  record:(item,reply)=>recordFormalVoice(s,'ashen',item.id,item.house,reply.message,reply.source)
 });
 assert.equal(result.ok,true);assert.deepEqual(paid,['redharbor']);assert.equal(p.responses.wintermere.status,'accepted');
 assert.deepEqual(c.messages.filter(m=>m.formalResponse).map(m=>m.source),['gemini']);assert.equal(p.responses.wintermere.source,'failed');assert.equal(p.responses.wintermere.voiceComplete,true);
});
test('an explicit retry permits one Gemini voice without repeating the formal decision or obligations',async()=>{
 const {s,c}=setup(),submitted=submitFormalProposal(s,'ashen',request(c,{type:'POSITION',targetId:'5,6'},['wintermere'])),p=proposal(s,submitted.proposalId);
 assert.equal(resolveFormalResponse(s,'ashen',p.id,'wintermere').ok,true);const pledged=s.pledges.length;
 assert.equal(recordFormalVoice(s,'ashen',p.id,'wintermere','','failed',{version:1,code:'GEMINI_TIMEOUT'}).voiceUnavailable,true);
 assert.equal(c.messages.filter(m=>m.formalResponse).length,0);assert.equal(isFormalCouncilBusy(s,c.id),false);
 assert.equal(retryFormalVoice(s,'ashen',p.id,'wintermere').ok,true);assert.equal(isFormalCouncilBusy(s,c.id),true);assert.equal(retryFormalVoice(s,'ashen',p.id,'wintermere').ok,false);
 assert.equal(parseSave(JSON.stringify(s)).cooperation.formalProposals[0].responses.wintermere.retryRequested,true);
 let calls=0;
 await runFormalResponseQueue({gapMs:0,next:()=>p.responses.wintermere.voiceComplete?null:{id:p.id,house:'wintermere',manualRetry:true,resumeVoice:false},
  onStatus:(item,status)=>status==='considering'?markFormalConsidering(s,'ashen',item.id,item.house):undefined,
  resolve:()=>({ok:true}),voice:async()=>{calls++;return {message:'You have my word. My commitment stands.',source:'gemini'};},
  record:(item,reply)=>recordFormalVoice(s,'ashen',item.id,item.house,reply.message,reply.source)});
 assert.equal(calls,1);assert.equal(s.pledges.length,pledged);assert.equal(p.responses.wintermere.status,'accepted');assert.equal(p.responses.wintermere.retryRequested,false);assert.equal(p.responses.wintermere.source,'gemini');
 assert.equal(c.messages.filter(m=>m.formalResponse).length,1);assert.equal(retryFormalVoice(s,'ashen',p.id,'wintermere').ok,false);
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

for(const direction of ['request','offer'])for(const type of ['EXCHANGE','RECURRING','LOAN'])test(`${direction} ${type} transfers and obligations follow the selected sender exactly once`,()=>{
 const {s,c}=setup(),sender=direction==='request'?'wintermere':'ashen',receiver=direction==='request'?'ashen':'wintermere',price=direction==='request'?10:1;
 const intent=type==='LOAN'?{type,giveResource:'gold',giveAmount:20,receiveResource:'gold',receiveAmount:22,duration:2}:{type,giveItems:[{resource:'food',amount:2},{resource:'iron',amount:1}],receiveItems:[{resource:'gold',amount:price}],duration:2};
 const r=submitFormalProposal(s,'ashen',{...request(c,intent,['wintermere']),direction});assert.equal(r.ok,true,r.error);
 const p=proposal(s,r.proposalId),before=Object.fromEntries([sender,receiver].map(h=>[h,{...kingdom(s,h).resources}]));
 resolveFormalResponse(s,'ashen',p.id,'wintermere');assert.equal(p.responses.wintermere.status,'accepted',p.responses.wintermere.message);
 if(type==='LOAN'){
  assert.equal(kingdom(s,sender).resources.gold,before[sender].gold-20);assert.equal(kingdom(s,receiver).resources.gold,before[receiver].gold+20);
  const loan=s.pledges.find(x=>x.formalProposalId===p.id);assert.equal(loan.debtor,receiver);assert.equal(loan.creditor,sender);
  s.turn+=2;verifyPledges(s);assert.equal(loan.status,'fulfilled');assert.equal(kingdom(s,sender).resources.gold,before[sender].gold+2);assert.equal(kingdom(s,receiver).resources.gold,before[receiver].gold-2);
 }else{
  assert.equal(kingdom(s,sender).resources.food,before[sender].food-2);assert.equal(kingdom(s,receiver).resources.food,before[receiver].food+2);
  assert.equal(kingdom(s,sender).resources.iron,before[sender].iron-1);assert.equal(kingdom(s,receiver).resources.iron,before[receiver].iron+1);
  if(type==='EXCHANGE'){assert.equal(kingdom(s,sender).resources.gold,before[sender].gold+price);assert.equal(kingdom(s,receiver).resources.gold,before[receiver].gold-price);}
  else {assert.equal(s.treaties.find(t=>t.type==='recurring').payer,sender);s.turn++;resolveRecurringTrade(s);assert.equal(kingdom(s,sender).resources.food,before[sender].food-4);assert.equal(kingdom(s,receiver).resources.food,before[receiver].food+4);}
 }
 const once=JSON.stringify(s);resolveRecurringTrade(s);verifyPledges(s);assert.equal(resolveFormalResponse(s,'ashen',p.id,'wintermere').ok,false);assert.equal(JSON.stringify(s),once);
 assert.match(formalDescription(p),direction==='request'?/Requested House gives/:/Proposer gives/);
});
test('requested trades retain AI economic judgment and counteroffers retain their transfer direction',()=>{
 const {s,c}=setup(),r=submitFormalProposal(s,'ashen',request(c,{type:'EXCHANGE',giveResource:'food',giveAmount:100,receiveResource:'gold',receiveAmount:1},['wintermere']));
 const before=JSON.stringify(s.kingdoms.map(k=>k.resources));resolveFormalResponse(s,'ashen',r.proposalId,'wintermere');const row=proposal(s,r.proposalId).responses.wintermere;
 assert.equal(row.status,'counter');assert.equal(JSON.stringify(s.kingdoms.map(k=>k.resources)),before);
 const counter=row.counterIntent,resources={...kingdom(s,'wintermere').resources};assert.equal(counter.giveItems[0].resource,'food');
 assert.equal(answerFormalProposal(s,'ashen',r.proposalId,'wintermere','accept').ok,true);
 for(const item of counter.giveItems)assert.equal(kingdom(s,'wintermere').resources[item.resource],resources[item.resource]-item.amount);
 for(const item of counter.receiveItems)assert.equal(kingdom(s,'wintermere').resources[item.resource],resources[item.resource]+item.amount);
});
test('requested loans do not lend domestic reserves or bypass a ruler’s distrust',()=>{
 for(const scarce of [false,true]){const {s,c}=setup();if(scarce)kingdom(s,'wintermere').resources.gold=20;else Object.assign(relation(s,'wintermere','ashen'),{trust:-80,reliability:0});
  const r=submitFormalProposal(s,'ashen',request(c,{type:'LOAN',giveResource:'gold',giveAmount:20,receiveResource:'gold',receiveAmount:22},['wintermere'])),before=kingdom(s,'wintermere').resources.gold;
  resolveFormalResponse(s,'ashen',r.proposalId,'wintermere');assert.notEqual(proposal(s,r.proposalId).responses.wintermere.status,'accepted');assert.equal(kingdom(s,'wintermere').resources.gold,before);assert.equal(s.pledges.length,0);
 }
});

test('a next-turn payment promise preserves its debtor and deadline without becoming immediate aid',()=>{
 const {s}=setup(),before=kingdom(s,'ashen').resources.food;
 const result=stageConversationProposal(s,'ashen',"I'll send you 20 food next turn.",{ruler:'wintermere'}),p=proposal(s,result.proposalId);
 assert.equal(p.intent.type,'PROMISE');assert.equal(p.intent.duration,1);assert.equal(p.direction,'offer');assert.equal(p.approved,false);
 assert.equal(ratifyFormalProposal(s,'ashen',p.id).ok,true);resolveFormalResponse(s,'ashen',p.id,'wintermere');
 assert.equal(p.responses.wintermere.status,'accepted');assert.equal(kingdom(s,'ashen').resources.food,before);
 const oath=s.pledges.at(-1);assert.equal(oath.debtor,'ashen');assert.equal(oath.creditor,'wintermere');assert.equal(oath.deadline,s.turn+1);
});
