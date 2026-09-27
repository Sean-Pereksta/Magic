import test from 'node:test';
import assert from 'node:assert/strict';
import { onlineGame } from './fixtures/online-game.mjs';
import { createGame } from './fixtures/legacy-game.mjs';
import { kingdom, relation, parseSave, atWar, declareWar, treaty } from '../core.mjs';
import { appendConversation, applySpeech } from '../living.mjs';
import { court } from '../house-control.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { splitCampaign, joinCampaign, playerView } from '../multiplayer-state.mjs';
import { knowledgeView } from '../fog.mjs';
import { DiplomacyClient } from '../chat.mjs';
import { validateIntent, validateResponse, describeIntent, makeContext, scriptedReply, relationshipResponse, evaluateDeal, commitDeal, disclosedDeal } from '../diplomacy.mjs';
import { recordRulerSpeech } from '../ruler-knowledge.mjs';
import { sanitizeContext } from '../worker/worker.mjs';

let serial=0;
const command=(s,m,actor,type,args)=>({id:`knowledge-${++serial}`,clientId:'knowledge-tests',sequence:serial,uid:m.seats[actor].uid,actorHouseId:actor,turn:s.turn,stateVersion:m.stateVersion,epoch:m.epoch,type,args});
const send=(s,m,actor,target,message,response)=>applyCommand(s,m,command(s,m,actor,'chat',{targetHouseId:target,message,...(response?{response}:{})}));
const last=(s,actor,ruler)=>court(s,actor).conversations[ruler].filter(m=>m.role==='ruler').at(-1).text;
function onlineCase(){
 const {state:s,meta:m}=onlineGame();
 assert.equal(send(s,m,'ashen','sunspire','I do not like Wintermere. What do you want to do about them?').ok,true);
 assert.equal(send(s,m,'wintermere','sunspire','Has anyone spoken to you about overthrowing my House?').ok,true);
 return {s,m};
}
function quote(s,m){
 assert.equal(send(s,m,'wintermere','sunspire','Could I pay you 25 gold to know who?').ok,true);
 const i=court(s,'wintermere').offers.sunspire.find(i=>i.type==='INTELLIGENCE');assert.ok(i);return i;
}

test('real online controller knows the NPC conversation but refuses a forged model denial',()=>{
 const {s,m}=onlineCase();
 assert.match(last(s,'wintermere','sunspire'),/private correspondence/);
 const invented={reply:'No one has spoken about overthrowing you. Pay me a gift instead.',tone:'warm',intents:[validateIntent({type:'AID',giveAmount:25})],memoryCandidates:['Nobody has discussed hostility against Wintermere.'],relationshipSummary:'There are no plots.'};
 assert.equal(send(s,m,'wintermere','sunspire','Is Ashen plotting against me?',invented).ok,true);
 assert.match(last(s,'wintermere','sunspire'),/private correspondence/);
 assert.deepEqual(court(s,'wintermere').offers.sunspire,[]);
 assert.doesNotMatch(JSON.stringify(kingdom(s,'sunspire').memories),/Nobody has discussed/);
 assert.doesNotMatch(kingdom(s,'sunspire').conversationSummaries?.wintermere||'',/no plots/);
 assert.equal(s.rulerKnowledge.facts[0].kind,'hostility');
});

test('online quote, recipient snapshot, exact ratification, receipt, duplicate rejection and reload',()=>{
 const {s,m}=onlineCase(),i=quote(s,m),buyer=kingdom(s,'wintermere'),seller=kingdom(s,'sunspire');
 const before=buyer.resources.gold,received=seller.resources.gold,split=splitCampaign(s);
 assert.ok(split.canonical.rulerKnowledge.facts.length);
 assert.equal(split.world.rulerKnowledge,undefined);
 for(const p of Object.values(split.privateByHouse))assert.equal(p.view.rulerKnowledge,undefined);
 const v=playerView(split.world,split.privateByHouse.wintermere,'wintermere');
 assert.equal(v.rulerDisclosures.offers.length,1);assert.equal(v.rulerDisclosures.receipts.length,0);
 assert.doesNotMatch(JSON.stringify(v.rulerDisclosures),/House Ashen|evidence|factIds|report|motive/);
 assert.equal(disclosedDeal(v,'sunspire',i,'wintermere').status,'accept');
 assert.equal(validateResponse({reply:'Review the quotation.',tone:'guarded',intents:[i]}).intents[0].type,'INTELLIGENCE');
 const request=command(s,m,'wintermere','ratify',{targetHouseId:'sunspire',intent:i});
 assert.equal(applyCommand(s,m,request).ok,true);
 assert.equal(buyer.resources.gold,before-i.giveAmount);assert.equal(seller.resources.gold,received+i.giveAmount);
 assert.match(court(s,'wintermere').conversations.sunspire.find(x=>x.kind==='intelligence-purchase').text,/House Ashen.*voiced hostility/);
 assert.equal(applyCommand(s,m,request).ok,false);
 const saved=JSON.stringify(s);
 assert.equal(commitDeal(s,'sunspire',i,'wintermere').ok,false);assert.equal(JSON.stringify(s),saved);
 const restored=parseSave(saved),parts=splitCampaign(restored),joined=joinCampaign(parts.canonical,parts.privateByHouse);
 assert.equal(joined.rulerKnowledge.receipts[0].price,i.giveAmount);
 assert.match(parts.privateByHouse.wintermere.view.rulerDisclosures.receipts[0].report,/House Ashen/);
 assert.deepEqual(parts.privateByHouse.ashen.view.rulerDisclosures.receipts,[]);
 assert.deepEqual(parts.world.rulerDisclosures,{offers:[],receipts:[]});
 const context=makeContext(playerView(parts.world,parts.privateByHouse.wintermere,'wintermere'),'sunspire','Thank you.',{actorHouseId:'wintermere'});
 assert.match(context.world.disclosedCorrespondence[0].report,/House Ashen/);assert.ok(sanitizeContext(context));
 assert.equal(makeContext(playerView(parts.world,parts.privateByHouse.ashen,'ashen'),'sunspire','Hello',{actorHouseId:'ashen'}).world.disclosedCorrespondence.length,0);
});

test('forged, unquoted, wrong-owner and expired information intents cannot debit a treasury',()=>{
 const {s,m}=onlineCase(),i=quote(s,m),before=JSON.stringify(s.kingdoms.map(k=>k.resources));
 for(const [actor,terms] of [['ashen',i],['wintermere',{...i,giveAmount:1}],['wintermere',{...i,targetId:'intel-999999'}]])assert.equal(applyCommand(s,m,command(s,m,actor,'ratify',{targetHouseId:'sunspire',intent:terms})).ok,false);
 assert.equal(JSON.stringify(s.kingdoms.map(k=>k.resources)),before);
 s.turn+=4;assert.equal(commitDeal(s,'sunspire',i,'wintermere').ok,false);assert.equal(JSON.stringify(s.kingdoms.map(k=>k.resources)),before);
});

test('client private enquiries use rules without sending hidden memories to Gemini',async()=>{
 const {s,m}=onlineCase();quote(s,m);
 const parts=splitCampaign(s),v=playerView(parts.world,parts.privateByHouse.wintermere,'wintermere');
 const original=JSON.stringify(v);let networkCalls=0;
 const client=new DiplomacyClient({endpoint:'https://example.test/diplomacy',fetcher:()=>{networkCalls++;throw new Error('Should not request an AI voice for a private disclosure decision.');}});
 const response=await client.send(v,'sunspire','Who is plotting against me?','',true,{actorHouseId:'wintermere'});
 assert.equal(response.source,'rules');assert.equal(networkCalls,0);assert.equal(JSON.stringify(v),original);
 assert.match(response.reply,/court must review/);assert.doesNotMatch(response.reply,/House Ashen/);
});

test('single-player accepted message path remembers, quotes and delivers without Gemini',async()=>{
 const s=createGame();
 const text='We should overthrow Redharbor.';
 applySpeech(s,'sunspire',text);recordRulerSpeech(s,'sunspire',text);appendConversation(s,'sunspire','player',text);
 // The ruler's own memories are authoritative, but an unrelated human subject
 // still gets only the approved disclosure when tested through the engine.
 const query='Has Ashen approached you about overthrowing Redharbor?';
 recordRulerSpeech(s,'sunspire',query,'wintermere');
 const answer=relationshipResponse(s,'sunspire',query,{reply:'Nobody has.',tone:'neutral',intents:[]},{actorHouseId:'wintermere'});
 assert.match(answer.reply,/private correspondence/);
 const q=recordRulerSpeech(s,'sunspire','May I pay for that information?','wintermere');
 const client=new DiplomacyClient();
 const response=await client.send(s,'sunspire','May I pay for that information?','',false,{actorHouseId:'wintermere'});
 assert.equal(response.source,'rules');assert.deepEqual(response.intents,q.intents);
 assert.equal(commitDeal(s,'sunspire',q.intents[0],'wintermere').ok,true);
 assert.ok(parseSave(JSON.stringify(s)).rulerKnowledge.receipts.length);
});

test('an accepted joint-war treaty is recorded separately from the initial approach',()=>{
 const s=createGame();kingdom(s,'ashen').resources.gold=2000;
 const i=validateIntent({type:'JOINT_WAR',targetId:'redharbor',giveAmount:500,duration:10});
 assert.equal(s.rulerKnowledge,undefined);
 evaluateDeal(s,'sunspire',i);assert.equal(s.rulerKnowledge,undefined);
 assert.equal(commitDeal(s,'sunspire',i).ok,true);
 assert.equal(s.rulerKnowledge.facts.at(-1).kind,'joint-war');assert.equal(atWar(s,'sunspire','redharbor'),true);
 assert.ok(parseSave(JSON.stringify(s)).rulerKnowledge.facts.length);
});

test('unknown and expired intelligence are not converted to paid gifts or commercial terms',()=>{
 const {state:s,meta:m}=onlineGame();
 send(s,m,'wintermere','sunspire','Can I pay you for information about plots against me?');
 assert.deepEqual(court(s,'wintermere').offers.sunspire,[]);assert.match(last(s,'wintermere','sunspire'),/not assurance/);
 const response=relationshipResponse(s,'sunspire','Good morning.',{reply:'Hello.',tone:'warm',intents:[validateIntent({type:'INTELLIGENCE',targetId:'intel-44',giveAmount:40,duration:3})]},{actorHouseId:'wintermere'});
 assert.deepEqual(response.intents,[]);
});

test('private ledger corruption fails save validation and does not become public knowledge',()=>{
 const {s,m}=onlineCase();quote(s,m);
 const damaged=structuredClone(s);damaged.rulerKnowledge.facts[0].subject='invented';
 assert.throws(()=>parseSave(JSON.stringify(damaged)),/Damaged private correspondence/);
 const unchanged=JSON.stringify(s);makeContext(s,'sunspire','Hello',{actorHouseId:'wintermere'});assert.equal(JSON.stringify(s),unchanged);
 const raw=JSON.stringify(knowledgeView(s,'public'));assert.doesNotMatch(raw,/rulerKnowledge|conceal-correspondence|I do not like Wintermere/);
});
