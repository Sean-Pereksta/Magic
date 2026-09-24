import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { normalizeGeminiModel,geminiModelSetting,DEFAULT_GEMINI_MODEL } from '../gemini-model.mjs';
import { callGemini, DiplomacyBudget } from '../worker/worker.mjs';
import { listTextModels } from '../worker/check-models.mjs';
import { makeDiagnostic,readDiagnostic,diagnosticReport } from '../diagnostics.mjs';
const context={rulerId:'wintermere',message:'Greetings',turn:1,history:[],memories:[],summary:'',world:{}};
const success=()=>Response.json({candidates:[{finishReason:'STOP',content:{parts:[{text:JSON.stringify({reply:'We hear you.',tone:'neutral',intents:[]})}]}}]});
test('runtime model choice survives deployment configuration and normalizes Google names',async()=>{
 const config=await readFile(new URL('../worker/wrangler.toml',import.meta.url),'utf8');
 assert.match(config,/^keep_vars\s*=\s*true/m);assert.doesNotMatch(config,/^\s*GEMINI_MODEL\s*=/m);
 assert.deepEqual(geminiModelSetting({}),{model:DEFAULT_GEMINI_MODEL,modelSource:'default'});
 const env={GEMINI_MODEL:'  models/gemini-working-model  ',GEMINI_API_KEY:'private-key'};let calls=0;
 await callGemini(context,env,async(url,options)=>{calls++;assert.equal(url,'https://generativelanguage.googleapis.com/v1beta/models/gemini-working-model:generateContent');assert.equal(options.headers['x-goog-api-key'],'private-key');return success();});assert.equal(calls,1);
});
test('invalid model names never become URLs or diagnostics',async()=>{
 for(const value of ['https://bad.example/key','AIza-private-key','gemini-model?key=private','gemini-test\nprivate','models/models/gemini-x',42]){
  assert.equal(normalizeGeminiModel(value),null);
  await assert.rejects(callGemini(context,{GEMINI_MODEL:value},()=>{assert.fail('no provider request');}),{diagnosticCode:'GEMINI_MODEL_CONFIG'});
  assert.equal(readDiagnostic(makeDiagnostic('GEMINI_MODEL',{model:value})).model,undefined);
 }
});
test('Google 404 reports attempted model without retrying or disclosing provider body',async()=>{
 let calls=0;const storage={data:new Map(),async get(k){return this.data.get(k);},async put(k,v){this.data.set(k,v);},async transaction(fn){return fn(this);}};
 const env={GEMINI_MODEL:'models/gemini-working-model',GEMINI_API_KEY:'private-api-key',TURNSTILE_SECRET:'private-turnstile',BUDGET:{}};
 const original=globalThis.fetch;
 try{
  globalThis.fetch=async()=>{calls++;return Response.json({error:{message:'private-provider-body'}},{status:404});};
  const budget=new DiplomacyBudget({storage},env);
  const result=await budget.fetch(new Request('https://internal/diplomacy',{method:'POST',body:JSON.stringify({context,clientId:'private-client',origin:'https://catnmice.com'})}));
  const body=await result.json();assert.equal(result.status,503);assert.equal(body.diagnostics.model,'gemini-working-model');assert.equal(body.diagnostics.modelSource,'configured');
  const report=diagnosticReport({...body.diagnostics,path:'/diplomacy',httpStatus:503},'https://worker.example','https://catnmice.com');
  assert.match(report,/Gemini model: gemini-working-model/);assert.match(report,/Google HTTP status: 404/);assert.doesNotMatch(report,/private-/);assert.equal(calls,1);
 }finally{globalThis.fetch=original;}
});
test('model listing filters generation support, follows pagination and sends key only in header',async()=>{
 let calls=0;
 const models=await listTextModels('private-key',async(url,options)=>{
  assert.ok(!url.includes('private-key'));assert.equal(options.headers['x-goog-api-key'],'private-key');calls++;
  return Response.json(calls===1?{models:[{name:'models/gemini-a',supportedGenerationMethods:['generateContent']},{name:'models/gemini-b',supportedGenerationMethods:['embedContent']}],nextPageToken:'page&2'}:{models:[{name:'models/gemini-c',supportedGenerationMethods:['generateContent']},{name:'https://bad.example',supportedGenerationMethods:['generateContent']}]});
 });assert.deepEqual(models,['gemini-a','gemini-c']);assert.equal(calls,2);
 await assert.rejects(listTextModels('private-key',async()=>Response.json({error:'private-key'},{status:403})),error=>error.message.includes('403')&&!error.message.includes('private-key'));
});
