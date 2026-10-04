import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { onlineGame, activateForTest } from './fixtures/online-game.mjs';
import { parseSave, kingdom } from '../core.mjs';
import { refreshKnowledge } from '../fog.mjs';
import { ownCouncil, councilUnread } from '../council-state.mjs';
import { beginCouncilMessage, considerCouncilSpeaker, commitCouncilSpeaker, finalizeCouncilMessage, retryCouncilSpeakers, councilSequenceValid, makeCouncilContext, scriptedCouncil } from '../alliance-council.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { splitCampaign, joinCampaign, playerView } from '../multiplayer-state.mjs';
import { makeDiagnostic } from '../diagnostics.mjs';
import { resolutionDue, resolveRound } from '../multiplayer-rounds.mjs';
import { submitFormalProposal, markFormalConsidering, resolveFormalResponse, recordFormalVoice, isFormalCouncilBusy, formalConversationCurrent } from '../formal-proposals.mjs';

const houses=['wintermere','redharbor','thornwall'];
function council(s=createGame()) {
  for(const id of houses)s.treaties.push({id:`alliance-${s.nextId++}`,type:'alliance',parties:['ashen',id],expires:s.turn+10});
  // The queue honors campaign roster order rather than hardcoding House names.
  const order=['ashen',...houses,'sunspire','vesper'];s.kingdoms.sort((a,b)=>order.indexOf(a.id)-order.indexOf(b.id));
  refreshKnowledge(s);return {s,c:ownCouncil(s,'ashen',true)};
}
const reply=(house,message=`${house} has heard the council.`,extra={})=>({responses:[{speakerHouseId:house,message}],source:'gemini',...extra});
function commit(s,start,house,raw=reply(house)) {
  assert.equal(considerCouncilSpeaker(s,'ashen',start,house).ok,true);
  const result=commitCouncilSpeaker(s,'ashen',start,house,raw);assert.equal(result.ok,true,result.error);return result;
}

test('a stable anchor commits one ruler at a time into authoritative shared history',()=>{
  const {s,c}=council(),start=beginCouncilMessage(s,'ashen',c.id,'Let us coordinate our defense.');
  assert.deepEqual(start.speakers,houses);assert.equal(c.messages.length,1);
  assert.equal(considerCouncilSpeaker(s,'ashen',start,'redharbor').ok,false);
  assert.equal(beginCouncilMessage(s,'ashen',c.id,'A conflicting message.').ok,false);
  assert.equal(s.diplomacy.messages.regular,1);
  assert.equal(considerCouncilSpeaker(s,'ashen',start,'wintermere').ok,true);
  assert.equal(c.activeSequence.currentSpeaker,'wintermere');
  const first=commitCouncilSpeaker(s,'ashen',start,'wintermere',reply('wintermere','Wintermere will watch the north.'));
  assert.equal(first.ok,true);assert.equal(c.messages.length,2);assert.equal(c.activeSequence.currentSpeaker,null);
  assert.equal(c.messages[1].anchorMessageId,start.entryId);assert.equal(c.messages[1].source,'gemini');
  assert.equal(councilSequenceValid(s,'ashen',start),true,'increasing the message sequence does not invalidate the anchor');
  assert.equal(c.read.ashen,1);assert.equal(c.read.redharbor||0,0);
  assert.match(JSON.stringify(makeCouncilContext(s,c,'ashen',c.messages[0].message).history),/Wintermere will watch the north/);
  assert.equal(finalizeCouncilMessage(s,'ashen',start).ok,false,'cannot finish before the remaining rulers');
  const again=commitCouncilSpeaker(s,'ashen',start,'wintermere',reply('wintermere','DUPLICATE'));
  assert.equal(again.duplicate,true);assert.equal(c.messages.length,2);
  commit(s,start,'redharbor',reply('redharbor','I heard Wintermere and will guard the coast.'));
  assert.match(JSON.stringify(makeCouncilContext(s,c,'ashen',c.messages[0].message).history),/Wintermere will watch the north.*I heard Wintermere/);
  commit(s,start,'thornwall');assert.equal(finalizeCouncilMessage(s,'ashen',start).ok,true);
  assert.equal(c.activeSequence.status,'complete');assert.equal(finalizeCouncilMessage(s,'ashen',start).duplicate,true);
  assert.equal(commitCouncilSpeaker(s,'ashen',start,'thornwall',reply('thornwall')).duplicate,true);
  assert.deepEqual(c.messages.slice(1).map(m=>m.speakerHouseId),houses);
});

test('responses arriving with the panel closed stay unread until the conversation is opened',()=>{
  const {s,c}=council(),start=beginCouncilMessage(s,'ashen',c.id,'I will review your answers shortly.');
  assert.equal(councilUnread(c,'ashen'),0);
  commit(s,start,'wintermere');commit(s,start,'redharbor');
  assert.equal(councilUnread(c,'ashen'),2,'committing replies does not pretend the player saw them');
  c.read.ashen=c.sequence; // The open Council UI issues this explicit read action.
  assert.equal(councilUnread(c,'ashen'),0);
  commit(s,start,'thornwall');finalizeCouncilMessage(s,'ashen',start);
  assert.equal(councilUnread(c,'ashen'),1,'finalization does not mark a later unseen response read');
});

test('a failed Gemini speaker preserves its diagnostic without inventing dialogue and later Gemini replies still arrive',()=>{
  const {s,c}=council(),start=beginCouncilMessage(s,'ashen',c.id,'What is our situation?');
  commit(s,start,'wintermere');
  const diagnostic={...makeDiagnostic('GEMINI_TIMEOUT'),httpStatus:503,path:'/diplomacy',at:1234,secret:'NEVER_PERSIST_THIS'};
  commit(s,start,'redharbor',{responses:[],source:'failed',diagnostic});
  assert.equal(c.messages.length,2);assert.equal(c.activeSequence.failed.redharbor.diagnostic.code,'GEMINI_TIMEOUT');
  assert.equal(c.activeSequence.failed.redharbor.diagnostic.httpStatus,503);assert.doesNotMatch(JSON.stringify(c),/NEVER_PERSIST_THIS/);
  assert.equal(c.activeSequence.completed.redharbor,undefined);assert.equal(commitCouncilSpeaker(s,'ashen',start,'redharbor',reply('redharbor')).duplicate,true);
  commit(s,start,'thornwall');assert.equal(finalizeCouncilMessage(s,'ashen',start).ok,true);
  assert.deepEqual(c.messages.slice(1).map(m=>m.source),['gemini','gemini']);
  assert.deepEqual(c.messages.slice(1).map(m=>m.speakerHouseId),['wintermere','thornwall']);
  assert.equal(parseSave(JSON.stringify(s)).allianceCouncils[0].activeSequence.failed.redharbor.diagnostic.code,'GEMINI_TIMEOUT');
});

test('only an explicit current-sequence retry requests failed rulers again and preserves successful replies',()=>{
  const {s,c}=council(),start=beginCouncilMessage(s,'ashen',c.id,'How should we prepare?');
  commit(s,start,'wintermere');commit(s,start,'redharbor',{source:'failed',responses:[],diagnostic:makeDiagnostic('GEMINI_TIMEOUT')});commit(s,start,'thornwall');
  assert.equal(retryCouncilSpeakers(s,'ashen',start).ok,false,'a still-running sequence cannot be retried');
  finalizeCouncilMessage(s,'ashen',start);const history=structuredClone(c.messages),dispatches=s.diplomacy.messages.regular;
  const retry=retryCouncilSpeakers(s,'ashen',start);assert.equal(retry.ok,true);assert.deepEqual(retry.speakers,['redharbor']);
  assert.equal(c.activeSequence.status,'pending');assert.deepEqual(c.activeSequence.failed,{});assert.equal(c.activeSequence.retryCount,1);
  assert.equal(parseSave(JSON.stringify(s)).allianceCouncils[0].activeSequence.status,'pending','successful later speakers do not invalidate a retry save');
  assert.equal(commitCouncilSpeaker(s,'ashen',start,'wintermere',reply('wintermere')).duplicate,true);
  commit(s,start,'redharbor',reply('redharbor','Gemini can now hear the completed council history.'));
  assert.equal(finalizeCouncilMessage(s,'ashen',start).ok,true);assert.deepEqual(c.messages.slice(0,history.length),history);
  assert.equal(c.messages.length,history.length+1);assert.equal(s.diplomacy.messages.regular,dispatches);
  assert.equal(c.activeSequence.failureHistory[0].diagnostic.code,'GEMINI_TIMEOUT');assert.equal(parseSave(JSON.stringify(s)).allianceCouncils[0].activeSequence.completed.redharbor,c.sequence);
  assert.equal(retryCouncilSpeakers(s,'ashen',start).ok,false,'a fully delivered conversation offers no retry');
});

test('wrong-speaker, invalid and local payloads cannot become council dialogue',()=>{
  for(const payload of [reply('ashen','FORGED'),{responses:[{speakerHouseId:'wintermere',message:'LOCAL'}],source:'scripted'},{responses:[{speakerHouseId:'wintermere',message:'NO_SOURCE'}]},{responses:[],source:'gemini'}]){
    const {s,c}=council(),start=beginCouncilMessage(s,'ashen',c.id,'A valid player message.');
    assert.equal(commit(s,start,'wintermere',payload).failed,true);assert.equal(c.messages.length,1);assert.equal(c.activeSequence.completed.wintermere,undefined);
    assert.equal(c.activeSequence.failed.wintermere.diagnostic.code,'GEMINI_RESPONSE_INVALID');
    const broken=structuredClone(s);broken.allianceCouncils[0].activeSequence.failed.wintermere.diagnostic.code='UNTRUSTED';assert.throws(()=>parseSave(JSON.stringify(broken)),/council/i);
  }
});

for(const change of ['turn','seed','coalition','anchor','actor'])test(`a changed ${change} invalidates remaining speakers without erasing committed replies`,()=>{
  const {s,c}=council(),start=beginCouncilMessage(s,'ashen',c.id,'Coordinate the council.');commit(s,start,'wintermere');
  const completed=structuredClone(c.messages[1]);
  if(change==='turn')s.turn++;
  if(change==='seed')s.seed++;
  if(change==='coalition')s.treaties=s.treaties.filter(t=>!t.parties.includes('thornwall'));
  if(change==='anchor')c.messages=c.messages.filter(m=>m.id!==start.entryId);
  if(change==='actor')c.participants=c.participants.filter(id=>id!=='ashen');
  assert.equal(councilSequenceValid(s,'ashen',start),false);
  assert.equal(considerCouncilSpeaker(s,'ashen',start,'redharbor').ok,false);
  assert.equal(commitCouncilSpeaker(s,'ashen',start,'redharbor',reply('redharbor')).ok,false);
  assert.equal(finalizeCouncilMessage(s,'ashen',start,{cancelled:true}).ok,true);
  assert.deepEqual(c.messages.find(m=>m.id===completed.id),completed);assert.equal(c.activeSequence.status,'cancelled');
});

test('saved partial sequences preserve their anchor, completed messages and current ruler',()=>{
  const {s,c}=council(),start=beginCouncilMessage(s,'ashen',c.id,'Coordinate the council.');commit(s,start,'wintermere');
  considerCouncilSpeaker(s,'ashen',start,'redharbor');
  const restored=parseSave(JSON.stringify(s)),record=restored.allianceCouncils[0];
  assert.equal(record.activeSequence.currentSpeaker,'redharbor');
  assert.equal(commitCouncilSpeaker(restored,'ashen',start,'wintermere',reply('wintermere')).duplicate,true);
  assert.equal(commitCouncilSpeaker(restored,'ashen',start,'redharbor',reply('redharbor')).ok,true);
  assert.equal(record.messages.length,3);
  for(const mutate of [
    x=>x.activeSequence.currentSpeaker='thornwall',
    x=>x.activeSequence.completed.redharbor=2,
    x=>x.activeSequence.anchorId=2,
    x=>x.activeSequence.speakers.push('ashen'),
    x=>x.messages[1].anchorMessageId=999,
    x=>x.messages[1].diagnostic={version:1,code:'UNTRUSTED_PROVIDER_TEXT'}
  ]){
    const damaged=structuredClone(s);mutate(damaged.allianceCouncils[0]);assert.throws(()=>parseSave(JSON.stringify(damaged)),/council/i);
  }
});

test('selected local speakers retain real personality and concerns even when the group chooser omits them',()=>{
  const {s,c}=council();
  for(const id of houses)kingdom(s,id).resources.food=5;
  const group=scriptedCouncil(s,c,'ashen','Attack Vesper.');
  const omitted=houses.find(id=>!group.responses.some(r=>r.speakerHouseId===id));assert.ok(omitted);
  const response=scriptedCouncil(s,c,'ashen','Attack Vesper.',null,omitted);
  assert.equal(response.responses.length,1);assert.equal(response.responses[0].speakerHouseId,omitted);assert.match(response.responses[0].message,/food/);
});

test('multiplayer progressively applies authenticated begin, consider, commit and finalize commands',()=>{
  const {state:s,meta}=onlineGame(1),{c}=council(s);activateForTest(s,meta,'ashen');let serial=1;
  const command=(type,args)=>({id:`council-sequence-${serial}`,clientId:'council-sequence',sequence:serial++,uid:meta.seats.ashen.uid,actorHouseId:'ashen',turn:s.turn,stateVersion:meta.stateVersion,epoch:meta.epoch,activationId:meta.activationId,type,args});
  const start=applyCommand(s,meta,command('councilBegin',{councilId:c.id,message:'Coordinate our defense.'}));
  assert.equal(start.ok,true);assert.equal(c.messages.length,1);
  for(const [index,house] of houses.entries()){
    const considered=command('councilConsider',{start,house});assert.equal(applyCommand(s,meta,considered).ok,true);
    const forged=command('councilCommit',{start,house,response:reply(house)});forged.uid='stranger';assert.equal(applyCommand(s,meta,forged).ok,false);
    const response=command('councilCommit',{start,house,response:reply(house)});assert.equal(applyCommand(s,meta,response).ok,true);
    assert.equal(applyCommand(s,meta,response).ok,false,'authenticated receipt replay cannot apply twice');
    assert.equal(applyCommand(s,meta,command('councilCommit',{start,house,response:reply(house,'DUPLICATE')})).duplicate,true);
    assert.equal(c.messages.length,index+2);
    const split=splitCampaign(s),joined=joinCampaign(split.canonical,split.privateByHouse);
    assert.equal(joined.allianceCouncils[0].messages.length,index+2);
    assert.equal(playerView(split.world,split.privateByHouse.ashen,'ashen').allianceCouncils[0].activeSequence.completed[house],index+2);
    assert.doesNotMatch(JSON.stringify(split.world),/Coordinate our defense/);
  }
  assert.equal(applyCommand(s,meta,command('councilFinalize',{start})).ok,true);
  assert.equal(c.activeSequence.status,'complete');assert.equal(s.courts.ashen.messages.regular,1);
});

test('multiplayer failed delivery and manual retry retain a single player message and successful speakers',()=>{
  const {state:s,meta}=onlineGame(1),{c}=council(s);activateForTest(s,meta,'ashen');let serial=1;
  const action=(type,args)=>applyCommand(s,meta,{id:`council-retry-${serial}`,clientId:'council-retry',sequence:serial++,uid:meta.seats.ashen.uid,actorHouseId:'ashen',turn:s.turn,stateVersion:meta.stateVersion,epoch:meta.epoch,activationId:meta.activationId,type,args});
  const start=action('councilBegin',{councilId:c.id,message:'Can you hear the council?'});
  for(const house of houses){
    assert.equal(action('councilConsider',{start,house}).ok,true);
    assert.equal(action('councilCommit',{start,house,response:house==='wintermere'?{source:'failed',responses:[],diagnostic:makeDiagnostic('GEMINI_TIMEOUT')}:reply(house)}).ok,true);
  }
  assert.equal(action('councilFinalize',{start}).ok,true);assert.equal(c.messages.length,3);
  const split=splitCampaign(s);assert.equal(joinCampaign(split.canonical,split.privateByHouse).allianceCouncils[0].activeSequence.failed.wintermere.diagnostic.code,'GEMINI_TIMEOUT');
  assert.equal(action('councilRetry',{start}).ok,true);assert.equal(action('councilConsider',{start,house:'wintermere'}).ok,true);
  assert.equal(action('councilCommit',{start,house:'wintermere',response:reply('wintermere')}).ok,true);
  assert.equal(action('councilFinalize',{start}).ok,true);assert.equal(c.messages.length,4);
  assert.equal(c.messages.filter(m=>m.speakerHouseId==='ashen').length,1);assert.equal(s.courts.ashen.messages.regular,1);
});

test('multiplayer private AI conversations require Gemini speech and failed delivery has no state effects',()=>{
  const {state:s,meta}=onlineGame(1);activateForTest(s,meta,'ashen');let serial=1;
  const action=response=>applyCommand(s,meta,{id:`private-gemini-${serial}`,clientId:'private-gemini',sequence:serial++,uid:meta.seats.ashen.uid,actorHouseId:'ashen',turn:s.turn,stateVersion:meta.stateVersion,epoch:meta.epoch,activationId:meta.activationId,type:'chat',args:{targetHouseId:'wintermere',message:'How does your realm fare?',response}});
  for(const response of [undefined,{reply:'Local invented reply.',intents:[],source:'scripted'},{reply:'Unattributed reply.',intents:[]},{source:'failed',reply:'',intents:[]}]){
    const before=structuredClone(s);assert.equal(action(response).ok,false);assert.deepEqual(s,before);
  }
  const result=action({reply:'Our banners watch the northern hills.',intents:[],tone:'neutral',source:'gemini'});
  assert.equal(result.ok,true,result.error);assert.equal(s.courts.ashen.conversations.wintermere.at(-1).source,'gemini');
  assert.equal(s.courts.ashen.conversations.wintermere.at(-1).text,'Our banners watch the northern hills.');
  assert.equal(parseSave(JSON.stringify(s)).courts.ashen.conversations.wintermere.at(-1).source,'gemini');
  const split=splitCampaign(s);assert.equal(playerView(split.world,split.privateByHouse.ashen,'ashen').courts.ashen.conversations.wintermere.at(-1).source,'gemini');
});

for(const expiry of ['explicit end','timer'])test(`multiplayer ${expiry} cancels outgoing council work before another human activation in the same round`,()=>{
  const {state:s,meta}=onlineGame(2),{c}=council(s);
  s.sequential.order=['ashen','wintermere','thornwall','sunspire','vesper','redharbor'];activateForTest(s,meta,'ashen');
  const start=beginCouncilMessage(s,'ashen',c.id,'Coordinate our defense.');commit(s,start,'redharbor');
  considerCouncilSpeaker(s,'ashen',start,'thornwall');
  const first=structuredClone(c.messages[1]),turn=s.turn;
  if(expiry==='explicit end')meta.ready.ashen=true;else meta.deadline=1100;
  const presence={u0:{at:1100},u1:{at:1100}};assert.equal(resolutionDue(meta,presence,1100,s),true);
  meta.phase='resolving';resolveRound(s,meta,presence,1100);
  assert.equal(s.turn,turn);assert.equal(meta.activeHouse,'wintermere');
  assert.equal(c.activeSequence.status,'cancelled');assert.equal(c.activeSequence.currentSpeaker,null);
  assert.deepEqual(c.messages.find(m=>m.id===first.id),first);
  assert.equal(commitCouncilSpeaker(s,'ashen',start,'thornwall',reply('thornwall')).ok,false);
  assert.equal(beginCouncilMessage(s,'wintermere',c.id,'I will take up our discussion.').ok,true);
});

test('activation advance closes formal sequencing without reversing accepted decisions or delivered speech',()=>{
  const {state:s,meta}=onlineGame(2),{c}=council(s);
  s.sequential.order=['ashen','wintermere','thornwall','sunspire','vesper','redharbor'];activateForTest(s,meta,'ashen');
  const result=submitFormalProposal(s,'ashen',{councilId:c.id,requestedHouses:['redharbor','thornwall'],direction:'offer',intent:{type:'AID',giveResource:'gold',giveAmount:10}});
  assert.equal(result.ok,true,result.error);
  const p=s.cooperation.formalProposals.find(p=>p.id===result.proposalId);
  assert.equal(resolveFormalResponse(s,'ashen',p.id,'redharbor').status,'accepted');
  assert.equal(recordFormalVoice(s,'ashen',p.id,'redharbor','I accept the offered support.','gemini').ok,true);
  const accepted=structuredClone(p.responses.redharbor),spoken=structuredClone(c.messages.find(m=>m.formalResponse));
  assert.equal(markFormalConsidering(s,'ashen',p.id,'thornwall').ok,true);assert.equal(isFormalCouncilBusy(s,c.id),true);
  meta.phase='resolving';resolveRound(s,meta,{u0:{at:1100},u1:{at:1100}},1100);
  assert.equal(s.turn,1);assert.equal(meta.activeHouse,'wintermere');
  assert.equal(formalConversationCurrent(s,p),false);assert.equal(isFormalCouncilBusy(s,c.id),false);
  assert.deepEqual(p.responses.redharbor,accepted);assert.deepEqual(c.messages.find(m=>m.formalResponse),spoken);
  assert.equal(resolveFormalResponse(s,'ashen',p.id,'thornwall').ok,false,'old activation cannot restart the formal sequence');
  assert.equal(beginCouncilMessage(s,'wintermere',c.id,'Let us continue with our next objective.').ok,true);
  assert.equal(parseSave(JSON.stringify(s)).cooperation.formalProposals.find(x=>x.id===p.id).conversationCancelled,true);
});
