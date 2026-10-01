import { activateForTest } from './fixtures/online-game.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { onlineGame } from './fixtures/online-game.mjs';
import { kingdom, relation, parseSave, treaty } from '../core.mjs';
import { refreshKnowledge, knowledgeView } from '../fog.mjs';
import { court } from '../house-control.mjs';
import { ownCouncil, councilActive, councilUnread } from '../council-state.mjs';
import { councilFacts, councilMood, makeCouncilContext, scriptedCouncil, sendCouncilMessage, beginCouncilMessage, finishCouncilMessage, validateCouncilResponse, initiateCouncilDiscussions, allianceRenewalOutreach } from '../alliance-council.mjs';
import { consumeDiplomaticMessage, grantFollowup, followupCredit, expireFollowups, privateConversation } from '../proposal-followup.mjs';
import { DiplomacyClient } from '../chat.mjs';
import { sanitizeContext, callGemini, councilResponseSchema } from '../worker/worker.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { splitCampaign, joinCampaign, playerView } from '../multiplayer-state.mjs';
const allies=(s,ids=['wintermere','thornwall'])=>{
  for(const id of ids)s.treaties.push({id:`alliance-${s.nextId++}`,type:'alliance',parties:['ashen',id],expires:s.turn+10});
  refreshKnowledge(s);return ownCouncil(s,'ashen',true);
};
const setup=()=>{const s=createGame(),c=allies(s);return {s,c};};
const reply={responses:[{speakerHouseId:'wintermere',message:'I would consider it. Put the terms before me.',requestedIntent:{type:'JOINT_WAR',targetId:'vesper'}}]};
let sequence=0;
const command=(s,m,actor,type,args)=>{activateForTest(s,m,actor);return ({id:`council-test-${++sequence}`,clientId:'council-test',sequence,uid:m.seats[actor].uid,actorHouseId:actor,turn:s.turn,stateVersion:m.stateVersion,epoch:m.epoch,activationId:m.activationId,type,args});};

test('a shared council charges once, speaks only as independent AI rulers and never executes terms',()=>{
  const {s,c}=setup();
  assert.equal(sendCouncilMessage(s,'ashen',c.id,'Attack Vesper.',reply).ok,true);
  assert.equal(s.diplomacy.messages.regular,1);assert.equal(c.messages.length,2);
  assert.equal(s.wars.length,0);assert.equal(s.pledges.length,0);
  assert.ok(followupCredit(s,'ashen','wintermere',c.id,{type:'JOINT_WAR',targetId:'vesper'}));
  assert.ok(councilUnread(c,'thornwall')>0);
});
test('pairwise distrust makes allies disagree even when both trust the player',()=>{
  const {s,c}=setup();
  for(const id of c.participants)for(const other of c.participants.filter(x=>x!==id))Object.assign(relation(s,id,other),{trust:60,opinion:60,grievance:0});
  assert.equal(councilMood(s,c.participants),'Cooperative');
  Object.assign(relation(s,'thornwall','wintermere'),{grievance:60,trust:-40});
  const response=scriptedCouncil(s,c,'ashen','Attack Vesper.');
  assert.equal(councilMood(s,c.participants),'Heated');assert.ok(response.responses.length>=2);
  assert.match(response.responses.find(r=>r.speakerHouseId==='thornwall').message,/Wintermere|assurance/i);
  Object.assign(relation(s,'thornwall','wintermere'),{grievance:85,trust:-60});
  assert.equal(councilMood(s,c.participants),'Uncontrolled');assert.ok(treaty(s,'ashen','thornwall','alliance'));
});
test('mood is somber after known defeats and deterministic otherwise',()=>{
  const {s,c}=setup();assert.equal(councilMood(s,c.participants),'Cordial');
  assert.equal(councilMood(s,c.participants,[{turn:s.turn,attacker:'vesper',defender:'wintermere',winner:'vesper'}]),'Somber');
});
test('a constrained ruler keeps sovereign interests and refuses to betray another ally',()=>{
  const {s,c}=setup();kingdom(s,'wintermere').resources.food=5;
  assert.match(scriptedCouncil(s,c,'ashen','Attack Vesper.').responses.find(r=>r.speakerHouseId==='wintermere').message,/food/);
  kingdom(s,'wintermere').resources.food=140;
  s.treaties.push({id:'other-alliance',type:'alliance',parties:['wintermere','vesper'],expires:10});
  assert.match(scriptedCouncil(s,c,'ashen','Attack Vesper.').responses.find(r=>r.speakerHouseId==='wintermere').message,/betray|sworn/);
});
test('council credits are scoped, one-use, expire on a new topic and do not refill from free proposals',()=>{
  const {s,c}=setup();sendCouncilMessage(s,'ashen',c.id,'Attack Vesper.',reply);
  s.diplomacy.messages.regular=3;
  assert.equal(consumeDiplomaticMessage(s,'wintermere','ashen',{type:'TRADE'},c.id).ok,false);
  assert.equal(consumeDiplomaticMessage(s,'thornwall','ashen',{type:'JOINT_WAR',targetId:'vesper'},c.id).ok,false);
  assert.equal(consumeDiplomaticMessage(s,'wintermere','ashen',{type:'JOINT_WAR',targetId:'redharbor'},c.id).ok,false);
  const i={type:'JOINT_WAR',targetId:'vesper'};
  assert.equal(consumeDiplomaticMessage(s,'wintermere','ashen',i,privateConversation('wintermere')).ok,false);
  assert.equal(consumeDiplomaticMessage(s,'wintermere','ashen',i,c.id).free,true);
  assert.equal(consumeDiplomaticMessage(s,'wintermere','ashen',i,c.id).ok,false);
  assert.equal(grantFollowup(s,'ashen','wintermere',c.id,'Attack Vesper.',{reply:'Send me your proposal.'},{paid:false}),false);
  s.diplomacy.messages.regular=0;sendCouncilMessage(s,'ashen',c.id,'Attack Vesper.',reply);
  sendCouncilMessage(s,'ashen',c.id,'Let us talk about peace instead.');
  assert.equal(followupCredit(s,'ashen','wintermere',c.id),null);
});
test('private dialogue grants the same one-use continuation and expires at the turn boundary',()=>{
  const {s}=setup(),id=privateConversation('wintermere');
  consumeDiplomaticMessage(s,'wintermere','ashen');
  assert.equal(grantFollowup(s,'ashen','wintermere',id,'Would you consider an alliance?',{reply:'What terms are you offering?'},{paid:true}),true);
  assert.equal(consumeDiplomaticMessage(s,'wintermere','ashen',{type:'ALLIANCE'},id).free,true);
  assert.equal(s.diplomacy.messages.regular,1);
  grantFollowup(s,'ashen','wintermere',id,'An alliance?',{reply:'Send me your proposal.'},{paid:true});
  s.turn++;assert.equal(followupCredit(s,'ashen','wintermere',id),null);
  expireFollowups(s,'ashen');assert.deepEqual(s.proposalFollowups,{});
});
test('changing coalition prevents stale replies, stale credits and access by the new member to old messages',()=>{
  const {s,c}=setup();const start=beginCouncilMessage(s,'ashen',c.id,'Attack Vesper.');
  s.treaties=s.treaties.filter(t=>!t.parties.includes('thornwall'));allies(s,['sunspire']);
  assert.equal(councilActive(s,c),false);assert.equal(finishCouncilMessage(s,'ashen',start,'Attack Vesper.',reply).ok,false);
  assert.equal(knowledgeView(s,'sunspire').allianceCouncils.some(x=>x.id===c.id),false);
  assert.equal(sendCouncilMessage(s,'thornwall',c.id,'I should not still be here.').ok,false);
});
test('a newer council exchange prevents an older async reply from spending or issuing credits',()=>{
  const {s,c}=setup();const start=beginCouncilMessage(s,'ashen',c.id,'Attack Vesper.');
  beginCouncilMessage(s,'ashen',c.id,'Consider peace instead.');
  assert.equal(finishCouncilMessage(s,'ashen',start,'Attack Vesper.',reply).ok,false);
  assert.equal(followupCredit(s,'ashen','wintermere',c.id),null);
});
test('model and worker validation reject nonparticipants, human impersonation and too many speakers',()=>{
  const {s,c}=setup(),ctx=makeCouncilContext(s,c,'ashen','Attack Vesper.');
  assert.ok(sanitizeContext(ctx));
  assert.equal(validateCouncilResponse({responses:[{speakerHouseId:'ashen',message:'I accept.'}]},c.participants,['wintermere','thornwall']),null);
  assert.equal(validateCouncilResponse({responses:[{speakerHouseId:'vesper',message:'Secret.'}]},c.participants),null);
  assert.equal(validateCouncilResponse({responses:Array(4).fill(reply.responses[0])},c.participants),null);
  assert.equal(sanitizeContext({...ctx,participants:['ashen','ashen']}),null);
});
test('a council requests each leader separately using the existing session, schema and fallback',async()=>{
  const {s,c}=setup();let calls=0;
  const client=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',fetcher:async(url,options)=>{calls++;const context=JSON.parse(options.body);assert.equal(context.mode,'allianceCouncil');const speaker=context.world.participants.find(p=>p.ai).id;return Response.json({responses:[{speakerHouseId:speaker,message:'Our frontier scouts stand ready.'}]});}});
  client.session={token:'test-session',expires:Date.now()+3600000};
  const result=await client.send(s,'wintermere','Attack Vesper.','',true,{actorHouseId:'ashen',councilId:c.id});
  assert.equal(calls,2);assert.equal(result.source,'gemini');assert.deepEqual(result.responses.map(r=>r.speakerHouseId).sort(),['thornwall','wintermere']);
  const fallback=await client.send(s,'wintermere','Attack Vesper.','',false,{actorHouseId:'ashen',councilId:c.id});
  assert.equal(calls,2);assert.equal(fallback.source,'scripted');assert.ok(fallback.responses.length);
  await callGemini(makeCouncilContext(s,c,'ashen','Attack Vesper.'),{GEMINI_API_KEY:'test'},async(url,options)=>{
    const body=JSON.parse(options.body);assert.ok(body.generationConfig.responseSchema.properties.responses);
    return Response.json({candidates:[{finishReason:'STOP',content:{parts:[{text:JSON.stringify(reply)}]}}]});
  });
});
test('fog-safe council context excludes private chat, spy reports, resources and observations not common to all',()=>{
  const {s,c}=setup();s.conversations.wintermere=[{role:'player',text:'PRIVATE_CANARY',turn:1}];
  s.pledges.push({debtor:'wintermere',creditor:'ashen',status:'pending',intent:{type:'JOINT_WAR'},deadline:9});
  kingdom(s,'vesper').resources.gold=987654;kingdom(s,'wintermere').memories.push({text:'SPY_CANARY',turn:1,importance:10});
  const secretArmy=s.armies.find(a=>a.owner==='vesper');secretArmy.id='HIDDEN_ARMY_CANARY';
  const ctx=makeCouncilContext(s,c,'ashen','What is our situation?'),text=JSON.stringify(ctx);
  assert.doesNotMatch(text,/PRIVATE_CANARY|SPY_CANARY|987654|HIDDEN_ARMY_CANARY/);
  assert.equal(ctx.world.observedForces.some(a=>a.tile===secretArmy.tile),false);
  assert.equal(Object.hasOwn(ctx.world,'resources'),false);
  assert.deepEqual(ctx.world.commitments,[],'a private two-House pledge is not shared with the third ally');
});
test('AI initiates only meaningful discussions with a per-turn ceiling and topic cooldown',()=>{
  const {s,c}=setup();initiateCouncilDiscussions(s);assert.equal(c.messages.length,0);
  kingdom(s,'wintermere').resources.food=0;initiateCouncilDiscussions(s);
  assert.ok(c.messages.length>=1);assert.match(c.messages[0].message,/food/);
  const n=c.messages.length;initiateCouncilDiscussions(s);assert.equal(c.messages.length,n);
  s.turn++;initiateCouncilDiscussions(s);assert.equal(c.messages.length,n);
});
test('particularly successful expired allies reach out once without auto-renewing',()=>{
  const {s}=setup(),expired=s.treaties[0];s.turn=expired.expires;s.treaties=s.treaties.filter(t=>t!==expired);
  Object.assign(relation(s,'wintermere','ashen'),{trust:75,opinion:70,reliability:80,grievance:0});
  allianceRenewalOutreach(s,[expired]);const n=s.conversations.wintermere.length;
  assert.equal(s.conversations.wintermere.at(-1).kind,'alliance-renewal');assert.equal(treaty(s,'ashen','wintermere','alliance'),undefined);
  assert.equal(s.diplomacy.offers.wintermere[0].type,'ALLIANCE');
  allianceRenewalOutreach(s,[expired]);assert.equal(s.conversations.wintermere.length,n);
  Object.assign(relation(s,'thornwall','ashen'),{trust:-25,grievance:60});
  allianceRenewalOutreach(s,[{parties:['ashen','thornwall'],expires:s.turn}]);assert.equal(s.conversations.thornwall?.some(m=>m.kind==='alliance-renewal')||false,false);
});
test('multiplayer uses authenticated commands, private projections and controller reassembly',()=>{
  const {state:s,meta:m}=onlineGame(3),c=allies(s);
  const sent=command(s,m,'ashen','councilChat',{councilId:c.id,message:'COUNCIL_PRIVATE_CANARY'});
  assert.equal(applyCommand(s,m,sent).ok,true);assert.equal(applyCommand(s,m,sent).ok,false);
  const stranger=command(s,m,'wintermere','councilChat',{councilId:c.id,message:'Hello',response:{responses:[{speakerHouseId:'ashen',message:'FORGED_HUMAN_CANARY'}]}});
  assert.equal(applyCommand(s,m,stranger).ok,true);assert.doesNotMatch(JSON.stringify(c),/FORGED_HUMAN_CANARY/);
  const split=splitCampaign(s);
  assert.doesNotMatch(JSON.stringify(split.world),/COUNCIL_PRIVATE_CANARY/);
  assert.doesNotMatch(JSON.stringify(split.privateByHouse.vesper),/COUNCIL_PRIVATE_CANARY/);
  assert.match(JSON.stringify(split.privateByHouse.thornwall),/COUNCIL_PRIVATE_CANARY/);
  const restored=joinCampaign(split.canonical,split.privateByHouse);assert.equal(restored.allianceCouncils[0].messages[0].message,'COUNCIL_PRIVATE_CANARY');
  assert.equal(playerView(split.world,split.privateByHouse.wintermere,'wintermere').allianceCouncils[0].id,c.id);
});
test('all-human councils never synthesize human replies or spend model quota',async()=>{
  const {state:s}=onlineGame(3),c=allies(s);let calls=0;
  const client=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',fetcher:async()=>{calls++;return Response.json(reply);}});
  const result=await client.send(s,'wintermere','Let us coordinate.','',true,{actorHouseId:'ashen',councilId:c.id});
  assert.equal(calls,0);assert.deepEqual(result.responses,[]);
  assert.equal(sendCouncilMessage(s,'ashen',c.id,'Let us coordinate.',result).ok,true);assert.equal(c.messages.length,1);
});
test('saves retain bounded council history and only unused same-turn continuation credits',()=>{
  const {s,c}=setup();sendCouncilMessage(s,'ashen',c.id,'Attack Vesper.',reply);
  const restored=parseSave(JSON.stringify(s));assert.equal(restored.allianceCouncils[0].messages.length,c.messages.length);
  assert.ok(followupCredit(restored,'ashen','wintermere',c.id));
  consumeDiplomaticMessage(restored,'wintermere','ashen',{type:'JOINT_WAR',targetId:'vesper'},c.id);
  assert.deepEqual(parseSave(JSON.stringify(restored)).proposalFollowups,{});
  const old=createGame();assert.equal(parseSave(JSON.stringify(old)).allianceCouncils,undefined);
  c.messages[0].speakerHouseId='vesper';assert.throws(()=>parseSave(JSON.stringify(s)),/council/);
});

test('automatic council voices retain their event, audience, speakers and simulation state',async()=>{
  const {nextCouncilDispatch,makeCouncilDispatchContext,applyCouncilDispatch}=await import('../council-dispatch.mjs');
  const {s,c}=setup();for(const t of s.treaties)t.expires=s.turn+1;
  initiateCouncilDiscussions(s);const dispatch=nextCouncilDispatch(s,'ashen');assert.ok(dispatch);
  const context=makeCouncilDispatchContext(s,c,'ashen',dispatch);assert.ok(sanitizeContext(context));
  const schema=councilResponseSchema(context);assert.equal(schema.type,'OBJECT');
  assert.equal(schema.properties.responses.items.properties.requestedIntent,undefined);
  assert.equal(context.world.conversationMode,'ai-initiated-council');assert.equal(context.world.dispatch.reason,'expiry');
  const wars=JSON.stringify(s.wars),treaties=JSON.stringify(s.treaties),sequence=c.sequence,allowance=s.diplomacy.messages.regular;
  const response={responses:dispatch.entries.map(e=>({speakerHouseId:e.speakerHouseId,message:`${e.speakerHouseId} considers renewing our northern alliance.`}))};
  const forged=structuredClone(response);forged.responses[0].speakerHouseId='ashen';assert.equal(applyCouncilDispatch(s,'ashen',dispatch,forged),false);
  const intent=structuredClone(response);intent.responses[0].requestedIntent={type:'ALLIANCE',duration:10};assert.equal(applyCouncilDispatch(s,'ashen',dispatch,intent),false);
  assert.equal(applyCouncilDispatch(s,'ashen',dispatch,response),true);
  assert.equal(c.sequence,sequence);assert.equal(s.diplomacy.messages.regular,allowance);assert.equal(JSON.stringify(s.wars),wars);assert.equal(JSON.stringify(s.treaties),treaties);
  assert.equal(c.messages.find(m=>m.initiated).source,'gemini');assert.equal(nextCouncilDispatch(s,'ashen'),null);
});

test('online Council voicing is authenticated, only replaces matching AI dispatches, and replays cannot overwrite them',async()=>{
  const {nextCouncilDispatch}=await import('../council-dispatch.mjs');
  const {state:s,meta:m}=onlineGame(1),c=allies(s);
  for(const t of s.treaties)t.expires=s.turn+1;initiateCouncilDiscussions(s);
  const dispatch=nextCouncilDispatch(s,'ashen');assert.ok(dispatch);
  const response={responses:dispatch.entries.map(e=>({speakerHouseId:e.speakerHouseId,message:'Our alliance stands watch at the frontier.'}))};
  const args={dispatch,response};
  const forged=command(s,m,'ashen','councilVoice',args);forged.uid='stranger';assert.equal(applyCommand(s,m,forged).ok,false);
  const bad=structuredClone(args);bad.response.responses[0].speakerHouseId='ashen';assert.equal(applyCommand(s,m,command(s,m,'ashen','councilVoice',bad)).ok,false);
  const before={wars:JSON.stringify(s.wars),treaties:JSON.stringify(s.treaties),sequence:c.sequence};
  assert.equal(applyCommand(s,m,command(s,m,'ashen','councilVoice',args)).ok,true);
  assert.equal(applyCommand(s,m,command(s,m,'ashen','councilVoice',args)).ok,false);
  assert.equal(JSON.stringify(s.wars),before.wars);assert.equal(JSON.stringify(s.treaties),before.treaties);assert.equal(c.sequence,before.sequence);
  assert.ok(c.messages.every(message=>message.source==='gemini'));
});

test('queued automatic voices survive later dialogue without overwriting it, but reject changed coalitions',async()=>{
  const {nextCouncilDispatch,applyCouncilDispatch,makeCouncilDispatchContext}=await import('../council-dispatch.mjs');
  const {s,c}=setup();for(const t of s.treaties)t.expires=s.turn+1;
  initiateCouncilDiscussions(s);const dispatch=nextCouncilDispatch(s,'ashen');assert.ok(dispatch);
  const response={responses:dispatch.entries.map(e=>({speakerHouseId:e.speakerHouseId,message:'An old background reply.'}))};
  beginCouncilMessage(s,'ashen',c.id,'I will help defend the north.');
  const player=structuredClone(c.messages.at(-1));
  assert.ok(makeCouncilDispatchContext(s,c,'ashen',dispatch));
  assert.equal(applyCouncilDispatch(s,'ashen',dispatch,response),true);assert.deepEqual(c.messages.at(-1),player);
  assert.equal(applyCouncilDispatch(s,'ashen',dispatch,response),false,'cannot voice twice');
  const changed=setup();initiateCouncilDiscussions(changed.s);const old=nextCouncilDispatch(changed.s,'ashen');
  changed.s.treaties=[];assert.equal(makeCouncilDispatchContext(changed.s,changed.c,'ashen',old),null);
});

test('council Gemini replies with absent optional terms survive the Worker, client and conversation log',async()=>{
 const {s,c}=setup();let calls=0;
 const client=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',fetcher:async(url,options)=>{
  const context=sanitizeContext(JSON.parse(options.body));assert.ok(context);
  const response=await callGemini(context,{},async(url,options)=>{
   calls++;const schema=JSON.parse(options.body).generationConfig.responseSchema;
   const speakers=context.world.participants.filter(p=>p.ai).map(p=>p.id);assert.equal(speakers.length,1);assert.deepEqual(schema.properties.responses.items.properties.speakerHouseId.enum,speakers);
   return Response.json({candidates:[{finishReason:'STOP',content:{parts:[{text:JSON.stringify({responses:[{speakerHouseId:speakers[0],message:'Our frontier scouts will support the shared campaign.',requestedIntent:{type:'ALLIANCE',giveItems:[],actorMember:null}}]})}]}}]});
  });
  return Response.json(response);
 }});
 client.session={token:'test',expires:Date.now()+1800000};
 const start=beginCouncilMessage(s,'ashen',c.id,'Let us coordinate.');assert.equal(start.ok,true);
 const response=await client.send(s,'wintermere','Let us coordinate.','',true,{actorHouseId:'ashen',councilId:c.id});
 assert.equal(calls,2);assert.equal(response.source,'gemini');assert.equal(client.lastDiagnostic,null);
 assert.equal(finishCouncilMessage(s,'ashen',start,'Let us coordinate.',response).ok,true);
 assert.equal(c.messages.at(-1).source,'gemini');assert.match(c.messages.at(-1).message,/frontier scouts/);
 assert.equal(c.messages.at(-1).requestedIntent.giveItems,undefined);
});
test('a vassal in council acknowledges lawful service even with negative feelings',()=>{
 const s=createGame(),c=allies(s,['wintermere']);s.treaties.push({type:'vassalage',parties:['ashen','wintermere'],liege:'ashen',vassal:'wintermere',expires:20});
 Object.assign(relation(s,'wintermere','ashen'),{grievance:75,trust:-60});
 const response=scriptedCouncil(s,c,'ashen','Attack Vesper.');const vassal=response.responses.find(r=>r.speakerHouseId==='wintermere');
 assert.ok(vassal);assert.match(vassal.message,/My liege/);assert.doesNotMatch(vassal.message,/grievances remain unanswered/);
 const bond=makeCouncilContext(s,c,'ashen','Attack Vesper.').world.treaties.find(t=>t.type==='vassalage');assert.equal(bond.liege,'ashen');
});
