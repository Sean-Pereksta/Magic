import test from 'node:test';import assert from 'node:assert/strict';
import {createProgressionService} from '../progression.js';
const auth={currentUser:{uid:'owner',getIdToken:async()=>'verified-token'}};
const response=(body,status=200)=>({ok:status===200,status,json:async()=>body});
const memoryStorage=()=>{const values=new Map();return {getItem:k=>values.get(k)||null,setItem:(k,v)=>values.set(k,v),removeItem:k=>values.delete(k)};};

test('no endpoint fails closed and does not fabricate local cards, rewards, or campaign completion',async()=>{
  const service=createProgressionService({auth,fetchImpl:()=>{throw new Error('Must not be called');}});assert.equal(service.configured,false);
  await assert.rejects(service.loadProfile(),{code:'SERVICE_NOT_CONFIGURED'});assert.equal(service.profile,null);
});
test('transport sends auth token, legal choices only, publishes canonical profile, and retains request IDs after uncertain failures including refresh',async()=>{
  const storage=memoryStorage(),sent=[];let shouldFail=true;
  const fetchImpl=async(url,options)=>{sent.push({url,...options,body:JSON.parse(options.body)});if(shouldFail){shouldFail=false;throw new Error('Lost server response');}return response({profile:{forgeDust:900},transaction:{toTier:2}});};
  const one=createProgressionService({auth,apiBaseUrl:'https://example.invalid/api/faction-forge/',fetchImpl,requestStorage:storage});
  await assert.rejects(one.upgradeTroop('rifleman'),{code:'CONNECTION_FAILED'});
  const otherDeviceReload=createProgressionService({auth,apiBaseUrl:'https://example.invalid/api/faction-forge',fetchImpl,requestStorage:storage});
  const result=await otherDeviceReload.upgradeTroop('rifleman');assert.equal(result.transaction.toTier,2);assert.equal(sent[0].body.requestId,sent[1].body.requestId);assert.equal(sent[1].headers.Authorization,'Bearer verified-token');assert.deepEqual(Object.keys(sent[1].body).sort(),['requestId','troopKey']);assert.equal(otherDeviceReload.profile.forgeDust,900);
  await otherDeviceReload.upgradeTroop('rifleman');assert.notEqual(sent[2].body.requestId,sent[1].body.requestId);
});
test('pick mutations carry sequence/index rather than outcome; structured permission failures stay explicit',async()=>{
  let received;
  const service=createProgressionService({auth,apiBaseUrl:'https://example.invalid',requestStorage:memoryStorage(),fetchImpl:async(_url,options)=>{received=JSON.parse(options.body);return response({error:{code:'ACCOUNT_NOT_LINKED',message:'Ownership needs verification.'}},403);}});
  await assert.rejects(service.pick('session',1,{sequence:7}),{code:'ACCOUNT_NOT_LINKED',status:403});assert.equal(received.index,1);assert.equal(received.sequence,7);assert.equal(received.winner,undefined);assert.equal(received.forgeDust,undefined);
});

test('profile subscriptions follow a newly signed-in UID and dispose the old account listener',()=>{
  let authCallback;let stopped=0;const paths=[],callbacks=[];
  const identity={currentUser:{uid:'anonymous'},onAuthStateChanged(fn){authCallback=fn;return ()=>stopped++;}};
  const firestoreApi={doc:(_db,collection,uid)=>({collection,uid}),onSnapshot:(ref,fn)=>{paths.push(ref.uid);callbacks.push(fn);return ()=>stopped++;}};
  const service=createProgressionService({auth:identity,db:{},apiBaseUrl:'https://example.invalid',firestoreApi,requestStorage:memoryStorage()});
  const profiles=[];service.subscribeProfile(p=>profiles.push(p));assert.deepEqual(paths,['anonymous']);
  identity.currentUser={uid:'durable-account'};authCallback(identity.currentUser);assert.deepEqual(paths,['anonymous','durable-account']);assert.equal(stopped,1);
  callbacks[0]({exists:()=>true,data:()=>({uid:'anonymous',forgeDust:9000})});assert.equal(service.profile,null);
  callbacks[1]({exists:()=>true,data:()=>({uid:'durable-account',forgeDust:20})});assert.equal(service.profile.uid,'durable-account');service.dispose();assert.equal(stopped,3);
});

test('unchanged battle polling reuses its validated replay rather than download the whole frame set repeatedly',async()=>{
  const urls=[],frames=[{at:0,units:[{id:'unit'}]}];let calls=0;
  const service=createProgressionService({auth,apiBaseUrl:'https://example.invalid',requestStorage:memoryStorage(),fetchImpl:async url=>{urls.push(url);calls++;return response({session:{id:'match',phase:'battle',battle:{replayId:'1',durationMs:100,...(calls===1?{frames}:{})}}});}});
  const initial=await service.getSession('match'),next=await service.getSession('match');assert.equal(initial.session.battle.frames.length,1);assert.equal(next.session.battle.frames[0].units[0].id,'unit');assert.match(urls[1],/knownReplayId=1$/);
});
