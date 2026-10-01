import { activateForTest } from './fixtures/online-game.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { onlineGame } from './fixtures/online-game.mjs';
import { CAMPAIGN_HOUSES, RESOURCES } from '../data.mjs';
import { kingdom, relation, parseSave } from '../core.mjs';
import { refreshKnowledge, knowledgeView } from '../fog.mjs';
import { applySpeech, appendConversation } from '../living.mjs';
import { evaluateDeal, commitDeal, validateIntent, validateResponse, makeContext, scriptedReply, relationshipResponse, describeIntent, deliverPledge } from '../diplomacy.mjs';
import { tradeBriefing, parseCommercialOffer, validateNegotiation } from '../trade-negotiation.mjs';
import { tradeRoute } from '../trade.mjs';
import { splitCampaign, playerView } from '../multiplayer-state.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { grantFollowup, consumeDiplomaticMessage, privateConversation } from '../proposal-followup.mjs';
import { DiplomacyClient } from '../chat.mjs';
import { systemPrompt, sanitizeContext } from '../worker/worker.mjs';

const opener = 'as neighbors I would love to establish a good relationship, I have much food, what do you need?';
const terms = (type, extra = {}) => validateIntent({ type, duration: 10, ...extra });
function alia() {
  const s = createGame(), k = kingdom(s, 'sunspire'), h = CAMPAIGN_HOUSES.find(h => h.id === 'dawnreach');
  for (const key of ['honor','greed','paranoia','ambition','aggression']) k[key] = h[key];
  return s;
}
function connected(s, host = 'sunspire') {
  for (const [id,owner,building] of [['10,10','ashen','town'],['11,10','ashen',null],['12,10',host,'town']])
    Object.assign(s.tiles[id],{terrain:'plains',owner,building,road:true,levels:{road:1,town:1},capital:null,project:null});
  refreshKnowledge(s);
}
const wealth = s => Object.fromEntries(RESOURCES.map(id => [id,s.kingdoms.reduce((n,k)=>n+k.resources[id],0)]));

test('the exact neighborly opener creates goodwill, not a mandatory payment or surprise treaty', () => {
  const s = alia(), before = wealth(s), r = relation(s,'sunspire','ashen'), opinion = r.opinion;
  applySpeech(s,'sunspire',opener);
  const reply = scriptedReply(s,'sunspire',opener);
  assert.ok(r.opinion > opinion); assert.equal(r.trust,15);
  assert.equal(reply.intents.length,0); assert.equal(s.treaties.length,0); assert.deepEqual(wealth(s),before);
  assert.doesNotMatch(reply.reply,/\b(?:pay|fee|upfront)\b/);
  assert.ok(s.conversations.sunspire.some(m=>m.kind==='negotiation'));
});

test('relevant cooperative dialogue changes Alia-style willingness to accept the same no-fee charter', () => {
  const s = alia(), offer = terms('TRADE');
  assert.notEqual(evaluateDeal(s,'sunspire',offer).status,'accept');
  applySpeech(s,'sunspire',opener);
  const v=evaluateDeal(s,'sunspire',offer);
  assert.equal(v.status,'accept',v.reason); assert.equal(v.intent.giveAmount,0);
  assert.match(v.reason,/interests of both|No upfront/);
  const gold=kingdom(s,'ashen').resources.gold;
  assert.equal(commitDeal(s,'sunspire',offer).ok,true);assert.equal(kingdom(s,'ashen').resources.gold,gold);
});

test('connected merchant benefit justifies a no-fee charter even without a compliment', () => {
  const s = createGame(); connected(s);
  const v=evaluateDeal(s,'sunspire',terms('TRADE'));
  assert.equal(v.status,'accept',v.reason); assert.match(v.reason,/connected road/);
});

test('fair one-time barter needs no charter, preserves resource totals and transfers no gold', () => {
  const s=createGame(), totals=wealth(s), gold=kingdom(s,'ashen').resources.gold;
  const i=terms('EXCHANGE',{giveResource:'food',giveAmount:12,receiveResource:'wood',receiveAmount:10});
  assert.equal(evaluateDeal(s,'wintermere',i).status,'accept');
  assert.equal(commitDeal(s,'wintermere',i).ok,true);
  assert.equal(kingdom(s,'ashen').resources.gold,gold);assert.deepEqual(wealth(s),totals);assert.equal(s.treaties.length,0);
});

test('paraphrased substantive arguments do not refresh their timestamps or farm relationship gains', () => {
  const s=alia();applySpeech(s,'sunspire',opener);applySpeech(s,'sunspire','Let us begin trade with a small trial exchange.');
  const before=structuredClone(relation(s,'sunspire','ashen').negotiation), trust=relation(s,'sunspire','ashen').trust;
  for(let j=0;j<12;j++){s.turn++;applySpeech(s,'sunspire','Both our peoples could prosper together through trade.');applySpeech(s,'sunspire','A modest first trade would limit our risk.');}
  assert.deepEqual(relation(s,'sunspire','ashen').negotiation,before);
  assert.equal(relation(s,'sunspire','ashen').trust,trust);
  assert.doesNotMatch(evaluateDeal(s,'sunspire',terms('TRADE')).factors.join(' '),/Discussed the interests/);
});

test('empty flattery cannot earn the new persuasion credit or any trust', () => {
  const s=createGame(), r=relation(s,'wintermere','ashen');
  for(let j=0;j<20;j++)applySpeech(s,'wintermere','You are an amazing friend and a wonderful ruler.');
  assert.equal(r.negotiation,undefined);assert.equal(r.trust,15);assert.ok(r.opinion<=22);
});

test('offers and invented claims of fulfilled promises do not earn deed-based trust', () => {
  const s=createGame(), r=relation(s,'wintermere','ashen');
  applySpeech(s,'wintermere','For trade, remember that I kept my promise and delivered your shipment.');
  assert.equal(r.negotiation,undefined);assert.equal(r.trust,15);assert.equal(s.pledges.length,0);
  const promise=scriptedReply(s,'wintermere',"I'll send you 100 food next turn.");
  assert.equal(promise.promiseDetected.type,'PROMISE');assert.equal(s.pledges.length,0);
});

test('a verified kept promise supports negotiation once, without crediting a second delivery', () => {
  const s=createGame();assert.equal(commitDeal(s,'wintermere',terms('PROMISE',{giveAmount:10,duration:2})).ok,true);
  assert.equal(deliverPledge(s,s.pledges[0].id).ok,true);
  const r=relation(s,'wintermere','ashen'),trust=r.trust,stock=wealth(s);
  applySpeech(s,'wintermere','In this trade, remember I kept my promise and delivered the shipment.');
  assert.equal(r.trust,trust+1);assert.ok(r.negotiation.arguments.kept_word);
  applySpeech(s,'wintermere','I fulfilled my promise; consider our trade terms.');
  assert.equal(r.trust,trust+1);assert.deepEqual(wealth(s),stock);
});

test('large gold offers and friendly words cannot buy away a serious breach or frontier threat', () => {
  for(const obstacle of ['betrayal','army']){
    const s=alia(),r=relation(s,'sunspire','ashen');kingdom(s,'ashen').resources.gold=2000;
    if(obstacle==='betrayal'){r.trust=-65;r.grievance=75;}else{s.armies[0].tile='32,21';s.armies[0].units.levy=200;refreshKnowledge(s);}
    applySpeech(s,'sunspire',opener);
    const v=evaluateDeal(s,'sunspire',terms('TRADE',{giveAmount:500}));
    assert.notEqual(v.status,'accept');assert.notEqual(v.counter?.type,'TRADE');
    assert.match(v.reason,obstacle==='betrayal'?/breach|confidence/:/frontier|threat/);
  }
});

test('apologies acknowledge actual grievances but do not erase them', () => {
  const s=alia(),r=relation(s,'sunspire','ashen');r.trust=-70;r.grievance=80;
  applySpeech(s,'sunspire','Before we trade, I apologize for the broken agreement and understand your grievance.');
  assert.equal(r.grievance,80);assert.equal(r.trust,-70);assert.ok(r.negotiation.arguments.acknowledge_grievance);
  assert.notEqual(evaluateDeal(s,'sunspire',terms('TRADE')).status,'accept');
});

test('threats revoke active arguments without reopening the same words for farming', () => {
  const s=alia();applySpeech(s,'sunspire',opener);
  applySpeech(s,'sunspire','Trade with me or I will destroy you.');
  const entry=structuredClone(relation(s,'sunspire','ashen').negotiation.arguments.mutual_interest);
  assert.equal(entry.revoked,true);applySpeech(s,'sunspire',opener);
  assert.deepEqual(relation(s,'sunspire','ashen').negotiation.arguments.mutual_interest,entry);
});

test('real food shortages are disclosed without inventing a cash shortage or revealing stores', () => {
  const s=alia(),k=kingdom(s,'sunspire');k.resources.food=0;k.resources.gold=999;k.economicPlan={type:'farm',tile:'33,21'};
  const c=makeContext(s,'sunspire',opener),text=JSON.stringify(c.world.tradeDiscussion);
  assert.ok(c.world.self.economicNeeds.imports.includes('food'));assert.ok(!c.world.self.economicNeeds.imports.includes('gold'));
  assert.doesNotMatch(text,/999|33,21|resources|economicPlan/);
  assert.equal(sanitizeContext(c).world.tradeDiscussion.phase,'exploration');
});

test('a genuine treasury need can justify gold terms; healthy treasuries do not invent an entry fee', () => {
  const s=createGame(),k=kingdom(s,'sunspire'),r=relation(s,'sunspire','ashen');
  for(const id of RESOURCES)k.resources[id]=500;k.resources.gold=0;k.population=0;k.tax='low';r.opinion=-15;r.trust=10;
  const v=evaluateDeal(s,'sunspire',terms('TRADE'));
  assert.equal(v.status,'counter',v.reason);assert.equal(v.counter.type,'TRADE');assert.equal(v.counter.giveResource,'gold');assert.ok(v.counter.giveAmount>0);
  assert.equal(evaluateDeal(s,'sunspire',v.counter).status,'accept');
});

test('a limited trial can soften a greedy bargaining margin but never fair package value', () => {
  const s=createGame(),offer=terms('EXCHANGE',{giveResource:'food',giveAmount:11,receiveResource:'wood',receiveAmount:10}),r=relation(s,'sunspire','ashen');
  assert.notEqual(evaluateDeal(s,'sunspire',offer).status,'accept');
  applySpeech(s,'sunspire','Let us start trade with a small trial exchange.');
  // The ruler can still bargain: a second distinct substantive point closes the small premium.
  applySpeech(s,'sunspire',opener);
  assert.equal(evaluateDeal(s,'sunspire',offer).status,'accept');
  r.trust=100;r.opinion=100;
  assert.notEqual(evaluateDeal(s,'sunspire',{...offer,giveItems:[{resource:'food',amount:1}],giveAmount:1}).status,'accept');
});

test('every generated counter is legal, affordable, and immediately re-evaluates to acceptance', () => {
  const s=createGame();for(const k of s.kingdoms)for(const id of RESOURCES)k.resources[id]=120;
  for(const host of ['wintermere','sunspire','vesper'])for(const offer of [terms('TRADE'),terms('EXCHANGE',{giveAmount:1,receiveAmount:60}),terms('RECURRING',{giveAmount:8,receiveAmount:12,duration:12})]){
    const v=evaluateDeal(s,host,offer);
    if(v.counter){assert.equal(evaluateDeal(s,host,v.counter).status,'accept',v.reason);assert.ok(v.counter.giveAmount<=kingdom(s,'ashen').resources[v.counter.giveResource]);}
  }
  // An otherwise useful offer that becomes unaffordable cannot silently execute.
  const v=evaluateDeal(s,'sunspire',terms('EXCHANGE',{giveAmount:1,receiveAmount:60}));assert.ok(v.counter);
  kingdom(s,'ashen').resources[v.counter.giveResource]=0;const before=wealth(s);
  assert.equal(commitDeal(s,'sunspire',v.counter).ok,false);assert.deepEqual(wealth(s),before);
});

test('over-capacity recurring terms may counter with a bounded, legal immediate trial', () => {
  const s=createGame(),v=evaluateDeal(s,'wintermere',terms('RECURRING',{giveAmount:90,receiveAmount:90,duration:12}));
  assert.equal(v.status,'counter');assert.equal(v.counter.type,'EXCHANGE');assert.ok(Math.max(v.counter.giveAmount,v.counter.receiveAmount)<=12);
  assert.equal(evaluateDeal(s,'wintermere',v.counter).status,'accept');assert.equal(s.treaties.length,0);
});

test('commercial permission, quantity balance and embargo restrictions survive persuasion', () => {
  const s=createGame();applySpeech(s,'wintermere',opener);applySpeech(s,'wintermere','Let us start with a small trade.');
  s.treaties.push({id:'ban',type:'embargo',parties:['ashen','vesper'],targetId:'wintermere',expires:20});
  for(const type of ['TRADE','EXCHANGE','RECURRING'])assert.equal(evaluateDeal(s,'wintermere',terms(type,{giveAmount:20,receiveAmount:type==='TRADE'?0:10})).status,'reject');
});

test('barter/charter parsing distinguishes holdings, conditions and later promises from offers', () => {
  assert.equal(parseCommercialOffer('I offer 12 grain for 10 timber.').type,'EXCHANGE');
  assert.equal(parseCommercialOffer('I offer 12 food for 10 wood each turn for 5 turns.').type,'RECURRING');
  assert.equal(parseCommercialOffer('Let us establish a trade agreement.').giveAmount,0);
  for(const message of ['I have 100 food.','I will not trade 12 food for 10 wood.','If you help, I offer 12 food for 10 wood.','I offer 12 food for 10 wood each turn.',"I'll send you 100 food next turn."])
    assert.equal(parseCommercialOffer(message),null,message);
});

test('new wording and fallback preserve the same authoritative counter, not a model-invented fee', () => {
  const s=alia(),bad=validateResponse({reply:'Pay 999 gold before we open trade.',tone:'guarded',intents:[terms('TRADE',{giveAmount:999})],trust:100});
  assert.equal(bad.trust,undefined);
  const exploratory=relationshipResponse(s,'sunspire',opener,bad);
  assert.equal(exploratory.intents.length,0);assert.doesNotMatch(exploratory.reply,/999|Pay/);
  const proposal=terms('TRADE');
  assert.deepEqual(relationshipResponse(s,'sunspire',describeIntent(proposal),bad,{proposal}).counterProposal,scriptedReply(s,'sunspire',describeIntent(proposal),{proposal}).counterProposal);
});

test('recent arguments re-evaluate the original active offer; ancient offers are not revived', () => {
  const s=alia(),offer=terms('TRADE');s.diplomacy.offers.sunspire=[offer];appendConversation(s,'sunspire','player',describeIntent(offer));
  applySpeech(s,'sunspire','Both our people could prosper through trade.');
  const reply=scriptedReply(s,'sunspire','Both our people could prosper through trade.');
  assert.equal(reply.speechAct,'accept');assert.equal(reply.intents[0].giveAmount,0);
  s.turn+=40;const later=scriptedReply(s,'sunspire','Let us discuss trade again.');assert.equal(later.intents.length,0);
});

test('all read-only context, reply, and decision paths leave the simulation unchanged', () => {
  const s=alia(),before=JSON.stringify(s);
  makeContext(s,'sunspire',opener);scriptedReply(s,'sunspire',opener);evaluateDeal(s,'sunspire',terms('TRADE'));tradeBriefing(s,'sunspire');
  assert.equal(JSON.stringify(s),before);
});

test('multiplayer publishes only dated coarse AI economic briefings in private views', () => {
  const {state:s}=onlineGame(1);kingdom(s,'thornwall').resources.food=0;kingdom(s,'thornwall').resources.gold=98765;
  const {world,privateByHouse}=splitCampaign(s),view=playerView(world,privateByHouse.ashen,'ashen');
  assert.equal(world.tradeBriefings,undefined);assert.equal(view.tradeBriefings.ashen,undefined);
  assert.deepEqual(tradeBriefing(view,'thornwall'),tradeBriefing(s,'thornwall'));
  assert.doesNotMatch(JSON.stringify(view.tradeBriefings),/98765|economicPlan|armies|path/);
  view.turn++;assert.equal(tradeBriefing(view,'thornwall'),null);
  assert.equal(tradeBriefing(knowledgeView(s),'thornwall'),null);
});

test('multiplayer applies the same bounded speech once and overrides a forged fee without transferring anything', () => {
  const {state:s,meta}=onlineGame(1), expected=structuredClone(s),ruler='thornwall',before=wealth(s);
  applySpeech(expected,ruler,opener);
  activateForTest(s,meta,'ashen');
  const result=applyCommand(s,meta,{id:'trade-chat',clientId:'trade-test',sequence:1,uid:'u0',actorHouseId:'ashen',turn:s.turn,stateVersion:meta.stateVersion,epoch:meta.epoch,activationId:meta.activationId,type:'chat',args:{targetHouseId:ruler,message:opener,response:{reply:'Pay 999 gold before we open trade.',tone:'cold',intents:[terms('TRADE',{giveAmount:999})],trust:100}}});
  assert.equal(result.ok,true,result.error);
  assert.deepEqual(relation(s,ruler,'ashen').negotiation,relation(expected,ruler,'ashen').negotiation);
  assert.equal(relation(s,ruler,'ashen').trust,relation(expected,ruler,'ashen').trust);assert.deepEqual(wealth(s),before);
  assert.doesNotMatch(s.courts.ashen.conversations[ruler].at(-1).text,/999/);assert.equal(s.courts.ashen.offers[ruler].length,0);
});

test('human rulers retain their own consent and are not automatically persuaded by the engine', () => {
  const {state:s}=onlineGame(2),before=structuredClone(relation(s,'wintermere','ashen'));
  applySpeech(s,'wintermere',opener);
  assert.deepEqual(relation(s,'wintermere','ashen'),before);assert.equal(tradeBriefing(s,'wintermere'),null);
  assert.equal(commitDeal(s,'wintermere',terms('TRADE')).ok,false);
});

test('an invited trade proposal uses one scoped follow-up credit for barter, never for unrelated actions', () => {
  const s=alia(),response=scriptedReply(s,'sunspire',opener),conversation=privateConversation('sunspire');
  assert.equal(grantFollowup(s,'ashen','sunspire',conversation,opener,response,{paid:true}),true);
  const offer=terms('EXCHANGE',{giveResource:'food',giveAmount:12,receiveResource:'wood',receiveAmount:10});
  assert.equal(consumeDiplomaticMessage(s,'sunspire','ashen',offer,conversation).free,true);
  assert.notEqual(consumeDiplomaticMessage(s,'sunspire','ashen',offer,conversation).free,true);
});

test('old saves remain valid and malformed/future/unbounded persuasion records are rejected', () => {
  const s=alia();assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));applySpeech(s,'sunspire',opener);
  assert.deepEqual(parseSave(JSON.stringify(s)).kingdoms.find(k=>k.id==='sunspire').relations.ashen.negotiation,relation(s,'sunspire','ashen').negotiation);
  for(const mutate of [n=>n.trustGain=100,n=>n.arguments.mutual_interest.turn=999,n=>n.arguments.forced={key:'yes',turn:1},n=>n.arguments.mutual_interest.key='forged']){
    const n=structuredClone(relation(s,'sunspire','ashen').negotiation);mutate(n);assert.equal(validateNegotiation(n,s.turn),false);
  }
});

test('Gemini receives grounded needs in one request; fallback and provider use the same trade guard', async () => {
  const s=alia();let calls=0,payload;
  const client=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',fetcher:async(url,opts)=>{calls++;payload=JSON.parse(opts.body);return Response.json({reply:'Pay 999 gold before we open trade.',tone:'cold',intents:[terms('TRADE',{giveAmount:999})]});}});
  client.session={token:'test',expires:Date.now()+1800000};
  const reply=await client.send(s,'sunspire',opener);
  assert.equal(calls,1);assert.equal(reply.source,'gemini');assert.equal(reply.intents.length,0);assert.doesNotMatch(reply.reply,/999/);
  assert.ok(payload.world.self.economicNeeds);assert.equal(payload.world.tradeDiscussion.phase,'exploration');
  assert.match(systemPrompt('sunspire'),/never invent a gold initiation fee/);
  const local=await client.send(s,'sunspire',opener,'',false);assert.deepEqual(local.intents,reply.intents);
});


test('exploration cannot be repackaged as an unsolicited gold gift, loan or payment oath', () => {
  const s=alia();
  const malicious=validateResponse({reply:'Our permission requires payment.',tone:'cold',intents:[terms('AID',{giveAmount:40}),terms('LOAN',{giveAmount:40,receiveResource:'gold',receiveAmount:50})],promiseDetected:terms('PROMISE',{giveAmount:40})});
  const out=relationshipResponse(s,'sunspire',opener,malicious);
  assert.deepEqual(out.intents,[]);assert.equal(out.promiseDetected,undefined);assert.equal(out.proposal,undefined);
});

test('persuasion evidence from a different private relationship never enters another viewer or the public snapshot', () => {
  const s=createGame();applySpeech(s,'sunspire',opener,'wintermere');
  assert.ok(relation(s,'sunspire','wintermere').negotiation);
  assert.equal(knowledgeView(s,'ashen').kingdoms.find(k=>k.id==='sunspire').relations.wintermere.negotiation,undefined);
  assert.equal(knowledgeView(s,'public').kingdoms.find(k=>k.id==='sunspire').relations.wintermere.negotiation,undefined);
  assert.ok(knowledgeView(s,'wintermere').kingdoms.find(k=>k.id==='sunspire').relations.wintermere.negotiation);
});


test('cycling previously discussed shortage resources cannot refresh persuasion', () => {
  const s=alia(),k=kingdom(s,'sunspire');k.resources.food=0;k.resources.tools=0;
  applySpeech(s,'sunspire','For trade, I offer food supplies.');
  applySpeech(s,'sunspire','For trade, I offer tools.');
  const n=structuredClone(relation(s,'sunspire','ashen').negotiation);
  s.turn+=4;
  applySpeech(s,'sunspire','For trade, I offer food supplies.');
  applySpeech(s,'sunspire','For trade, I offer tools.');
  assert.deepEqual(relation(s,'sunspire','ashen').negotiation,n);
  assert.equal(validateNegotiation(n,s.turn),true);
  assert.equal(validateNegotiation({...n,seenSupplies:['food','food']},s.turn),false);
});

test('consistent Gemini character voice survives while exact authoritative terms remain visible', () => {
  const s=alia();applySpeech(s,'sunspire',opener);
  const offer=terms('TRADE');
  const response={reply:'Let our merchants look beyond these horizons together, Regent.',intents:[offer],tone:'warm',speechAct:'accept'};
  const out=relationshipResponse(s,'sunspire','Let us establish a trade agreement.',response,{proposal:offer});
  assert.match(out.reply,/beyond these horizons/);assert.match(out.reply,/No upfront payment/);assert.deepEqual(out.intents,[offer]);
  const invented=relationshipResponse(s,'sunspire','Let us establish a trade agreement.',{...response,reply:'Send 999 gold and our merchants can begin.'},{proposal:offer});
  assert.doesNotMatch(invented.reply,/999/);
});

test('marriage conversations mentioning trade retain family-system precedence', () => {
  const s=alia(),message='Would you consider joining our families in marriage and trade?';
  const local=scriptedReply(s,'sunspire',message);
  const out=relationshipResponse(s,'sunspire',message,{reply:local.reply,intents:local.intents,tone:local.tone});
  assert.equal(out.reply,local.reply);
  assert.equal(makeContext(s,'sunspire',message).world.tradeDiscussion,undefined);
});
