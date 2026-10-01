import test from 'node:test';
import assert from 'node:assert/strict';
import { callGemini, DiplomacyBudget, reserveBudget } from '../worker/worker.mjs';
import { DiplomacyClient } from '../chat.mjs';
import { validateResponse } from '../diplomacy.mjs';
import { RESPONSE_SCHEMA } from '../worker/reply-contract.mjs';
import { createGame } from './fixtures/legacy-game.mjs';
import { diagnosticReport, makeDiagnostic } from '../diagnostics.mjs';
const context={rulerId:'wintermere',turn:1,message:'Greetings',world:{},history:[],summary:'',memories:[]};
const reply={reply:'Your envoy is welcome.',tone:'warm',intents:[]};
const output=(finishReason,text)=>Response.json({candidates:[{finishReason,content:{parts:[{text}]}}]});
class Storage {data=new Map();async get(k){return structuredClone(this.data.get(k));}async put(k,v){this.data.set(k,structuredClone(v));}async transaction(fn){return fn(this);}}
test('Gemini 3.5 uses chat-speed thinking and room for a complete structured response',async()=>{
 await callGemini(context,{GEMINI_MODEL:'gemini-3.5-flash'},async(url,opts)=>{
  const config=JSON.parse(opts.body).generationConfig;
  assert.equal(config.maxOutputTokens,2048);assert.equal(config.thinkingConfig.thinkingLevel,'MINIMAL');assert.equal(config.temperature,undefined);
  return output('STOP',JSON.stringify(reply));
 });
});
test('truncated, empty, malformed and schema-invalid output remain rejected with safe distinct diagnostics',async()=>{
 for(const [finish,text,issue] of [['MAX_TOKENS','private-partial-text','output_limit'],['STOP','','empty_reply'],['STOP','private-not-json','invalid_json'],['STOP',JSON.stringify({...reply,intents:[{type:'UNSAFE'}]}),'invalid_intent']]){
  await assert.rejects(callGemini(context,{},async()=>output(finish,text)),error=>{
   assert.equal(error.replyIssue,issue);const report=diagnosticReport(makeDiagnostic(error.diagnosticCode,{replyIssue:error.replyIssue,providerStatus:200}));assert.doesNotMatch(report,/private-|UNSAFE/);assert.match(report,/Reply validation:/);return true;
  });
 }
 assert.equal(makeDiagnostic('GEMINI_RESPONSE_INVALID',{replyIssue:'private-error'}).replyIssue,undefined);
});
test('invalid reply waits five seconds end to end, without automatic model retries',async()=>{
 let now=Date.now(),calls=0;const original=globalThis.fetch;
 const storage=new Storage(),budget=new DiplomacyBudget({storage},{GEMINI_MODEL:'gemini-3.5-flash',GEMINI_API_KEY:'private-key'});
 const client=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',now:()=>now,fetcher:(url,opts)=>budget.fetch(new Request(url,{method:'POST',body:JSON.stringify({context:JSON.parse(opts.body),clientId:'test',origin:'https://catnmice.com'})}))});
 client.session={token:'test',expires:now+1800000};
 try{
  globalThis.fetch=async()=>{calls++;return calls===1?output('MAX_TOKENS','{'):output('STOP',JSON.stringify(reply));};
  const s=createGame();await client.send(s,'wintermere','First');assert.equal(client.cooldownUntil,now+5000);assert.equal(client.lastDiagnostic.code,'GEMINI_RESPONSE_TRUNCATED');
  now+=4000;const wait=await client.send(s,'wintermere','Second');assert.equal(calls,1);assert.match(wait.notice,/1 seconds/);
  now+=1000;assert.equal((await client.send(s,'wintermere','Third')).source,'gemini');assert.equal(calls,2);assert.equal((await storage.get('budget')).used,2);
 }finally{globalThis.fetch=original;}
});
test('minute limits wait only until their actual reset, without raising allowances',async()=>{
 const storage=new Storage(),now=Date.UTC(2026,8,24,1,2,59);
 const env={CLIENT_PER_MINUTE:1};assert.equal((await reserveBudget(storage,env,'client',now)).ok,true);
 const denied=await reserveBudget(storage,env,'client',now);assert.equal(denied.code,'CLIENT_RATE_LIMIT');assert.equal(denied.retryAfter,1);
 assert.equal((await reserveBudget(storage,env,'client',now+1000)).ok,true);
});

test('model-only null placeholders and empty irrelevant item lists become a canonical browser reply',async()=>{
 const raw={...reply,intents:[{type:'ALLIANCE',giveItems:[],receiveItems:null,actorMember:null,conditionHouseId:null}],
  proposal:null,counterProposal:null,promiseDetected:null,speechAct:null,relationshipSummary:null,relationshipSignals:null,memoryCandidates:null};
 assert.equal(validateResponse(raw),null,'the player-facing contract remains strict');
 const result=await callGemini(context,{},async()=>output('STOP',JSON.stringify(raw)));
 assert.ok(validateResponse(result));assert.equal(result.intents[0].type,'ALLIANCE');
 for(const key of ['giveItems','receiveItems','actorMember','conditionHouseId'])assert.equal(result.intents[0][key],undefined);
 for(const key of ['speechAct','relationshipSummary','relationshipSignals','memoryCandidates'])assert.equal(result[key],undefined);
});
test('Gemini proposals preserve complete multi-resource packages and reject meaningful invalid terms',async()=>{
 const exchange={type:'EXCHANGE',giveItems:[{resource:'food',amount:20},{resource:'iron',amount:10}],receiveItems:[{resource:'gold',amount:40}]};
 const result=await callGemini(context,{},async()=>output('STOP',JSON.stringify({...reply,intents:[exchange]})));
 assert.deepEqual(result.intents[0].giveItems,exchange.giveItems);assert.deepEqual(result.intents[0].receiveItems,exchange.receiveItems);
 for(const terms of [{...exchange,giveItems:[]},{type:'ALLIANCE',giveItems:[{resource:'gold',amount:10}]},{type:'ALLIANCE',actorMember:'son'},{type:'ALLIANCE',duration:1},{type:'EXCHANGE',giveAmount:'20'}])
  await assert.rejects(callGemini(context,{},async()=>output('STOP',JSON.stringify({...reply,intents:[terms]}))),e=>e.replyIssue==='invalid_intent');
 await assert.rejects(callGemini(context,{},async()=>output('STOP',JSON.stringify({...reply,promiseDetected:{type:'ALLIANCE'}}))),e=>e.replyIssue==='invalid_intent');
 for(const [field,value,issue] of [['reply','x'.repeat(1601),'invalid_reply'],['relationshipSummary','x'.repeat(361),'invalid_metadata'],['memoryCandidates',['x'.repeat(181)],'invalid_metadata']]){
  await assert.rejects(callGemini(context,{},async()=>output('STOP',JSON.stringify({...reply,[field]:value}))),e=>e.replyIssue===issue);
 }
});
test('flat provider schema retains bounded terms, promise types and interpretation',()=>{
 const terms=RESPONSE_SCHEMA.properties.intents.items.properties;
 assert.equal(terms.giveItems.minItems,1);assert.equal(terms.giveItems.maxItems,8);
 assert.equal(terms.duration.minimum,1);assert.equal(terms.duration.maximum,20);
 assert.ok(RESPONSE_SCHEMA.properties.promiseDetected.properties.type.enum.every(t=>t==='PROMISE'||t==='GUARANTEE'||t.startsWith('PLEDGE_')));
 assert.equal(RESPONSE_SCHEMA.properties.reply.maxLength,1600);assert.equal(RESPONSE_SCHEMA.properties.memoryCandidates.items.maxLength,180);
});

test('all conversation modes serialize one typed schema format and make exactly one provider request',async()=>{
 const council={...context,mode:'allianceCouncil',actorHouseId:'ashen',participants:['ashen','wintermere'],world:{participants:[{id:'ashen',ai:false},{id:'wintermere',ai:true}]}};
 const councilReply={responses:[{speakerHouseId:'wintermere',message:'Our scouts stand ready.'}]};
 const cases=[
  [context,reply],
  [council,{responses:[{...councilReply.responses[0],requestedIntent:{type:'JOINT_WAR',targetId:'vesper'}}]}],
  [{...council,world:{...council.world,conversationMode:'ai-initiated-council'}},councilReply],
  [{...council,world:{...council.world,formalDecision:{status:'accepted'}}},councilReply],
  [{...context,mode:'general'},{reply:'We await your orders.'}]
 ];
 // Check the actual wire body, not only exported schema objects. This is a
 // regression guard for the selected request subset, not a live Google test.
 function typedSchema(schema){
  assert.ok(['OBJECT','ARRAY','STRING','INTEGER','NUMBER','BOOLEAN'].includes(schema.type));
  assert.equal(schema.anyOf,undefined);
  if(schema.nullable!==undefined)assert.equal(typeof schema.nullable,'boolean');
  if(schema.properties)for(const child of Object.values(schema.properties))typedSchema(child);
  if(schema.items)typedSchema(schema.items);
 }
 for(const [input,response] of cases){
  let calls=0;
  const result=await callGemini(input,{GEMINI_MODEL:'gemini-3.5-flash'},async(url,options)=>{
   calls++;assert.match(url,/\/models\/gemini-3\.5-flash:generateContent$/);
   const body=JSON.parse(options.body),config=body.generationConfig;
   assert.equal(config.responseMimeType,'application/json');assert.equal(config.responseJsonSchema,undefined);
   typedSchema(config.responseSchema);assert.deepEqual(JSON.parse(body.contents[0].parts[0].text),input);
   if(input.mode==='allianceCouncil'){
    const speakers=config.responseSchema.properties.responses;
    assert.deepEqual(speakers.items.properties.speakerHouseId.enum,['wintermere']);
    if(input.world.conversationMode||input.world.formalDecision)assert.equal(speakers.items.properties.requestedIntent,undefined);
    else assert.equal(speakers.items.properties.requestedIntent.type,'OBJECT');
    if(input.world.formalDecision)assert.equal(speakers.maxItems,1);
   }
   return output('STOP',JSON.stringify(response));
  });
  assert.equal(calls,1);assert.ok(result);
 }
});
