import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createHash,randomBytes} from 'node:crypto';
import {createDesktopAuthorization,createDesktopTokenIssuer} from './desktop-authorization.mjs';
import {createHubService} from './server.mjs';
import {createLedger} from './ledger.mjs';

const verifier=()=>randomBytes(32).toString('base64url');
const challenge=v=>createHash('sha256').update(v,'ascii').digest('base64url');
const owner={uid:'already-verified-firebase-uid',username:'Main account'};
const website='https://www.catnmice.com';
test('approval checks the independent code and verifier, then consumes the request before concurrent mint attempts',async()=>{
  let clock=Date.now(),calls=0,finish;
  const held=new Promise(resolve=>{finish=resolve;});
  const service=createDesktopAuthorization({now:()=>clock,mint:async player=>{assert.equal(player.uid,owner.uid);calls++;await held;return 'test-token';}});
  const proof=verifier(),r=service.start(challenge(proof),'one');
  assert.deepEqual(Object.keys(r).sort(),['code','expiresAt','id','pollIntervalMs','verificationUrl'].sort());
  assert.ok(!r.verificationUrl.includes(r.code)&&!r.verificationUrl.includes(proof));
  assert.equal((await service.redeem(r.id,proof)).state,'pending');
  await assert.rejects(service.redeem(r.id,proof),{status:429});
  assert.throws(()=>service.approve(r.id,'WRONG',owner),{status:403});
  service.approve(r.id,r.code.toLowerCase(),owner);
  await assert.rejects(service.redeem(r.id,verifier()),{status:403});
  assert.throws(()=>service.approve(r.id,r.code,{uid:'second',username:'Other'}),{status:410});
  const redemption=service.redeem(r.id,proof);
  await assert.rejects(service.redeem(r.id,proof),{status:410});finish();
  assert.deepEqual(await redemption,{state:'approved',customToken:'test-token',username:owner.username});
  assert.equal(calls,1);
});
test('expired requests, denied requests and five incorrect codes cannot be redeemed',async()=>{
  let clock=Date.now();const s=createDesktopAuthorization({now:()=>clock,mint:()=>assert.fail('unexpected mint')});
  for(const kind of ['expired','denied','cancelled','attempts']){
    const v=verifier(),r=s.start(challenge(v),'one');
    if(kind==='expired'){s.approve(r.id,r.code,owner);clock+=5*60000;}
    if(kind==='denied')s.deny(r.id,r.code);
    if(kind==='cancelled'){assert.throws(()=>s.cancel(r.id,verifier()),{status:403});s.cancel(r.id,v);}
    if(kind==='attempts')for(let i=0;i<5;i++)assert.throws(()=>s.approve(r.id,'0000-0000',owner),{status:403});
    await assert.rejects(s.redeem(r.id,v),{status:410});
  }
  assert.throws(()=>s.start('plain-verifier','x'),{status:400});
  for(let i=0;i<8;i++)s.start(challenge(verifier()),'burst');
  assert.throws(()=>s.start(challenge(verifier()),'burst'),{status:429});
});
test('Firebase issuer rechecks the still-bound UID, profile and revocation before using the privileged signer',async()=>{
  const docs=new Map([['factionForgeAccounts/'+owner.uid,{username:owner.username,verified:true}],['factionForgeNames/'+owner.username,{uid:owner.uid,verified:true}],['users/'+owner.username,{}]]);
  let enabled=true,validAfter=0;const signed=[];
  const auth={getUser:async()=>({uid:owner.uid,disabled:!enabled,tokensValidAfterTime:new Date(validAfter).toISOString()}),createCustomToken:async uid=>{signed.push(uid);return 'custom';}};
  const db={collection:collection=>({doc:id=>({get:async()=>({data:()=>docs.get(collection+'/'+id),exists:docs.has(collection+'/'+id)})})})};
  const mint=createDesktopTokenIssuer({auth,db}),approvedAt=Date.now();
  assert.equal(await mint({...owner,approvedAt}),'custom');
  enabled=false;await assert.rejects(mint({...owner,approvedAt}),{status:403});enabled=true;
  validAfter=approvedAt+1000;await assert.rejects(mint({...owner,approvedAt}),{status:403});validAfter=0;
  docs.get('factionForgeNames/'+owner.username).uid='different';
  await assert.rejects(mint({...owner,approvedAt}),{status:403});
  assert.deepEqual(signed,[owner.uid]);
});
test('only explicit verified browser approval can grant the requesting desktop an identity',async t=>{
  const ledger=createLedger(':memory:',['cat','bannerlands']);
  const signed=[];
  const gateway=createHubService({ledger,identity:async token=>{if(token!=='verified-session')throw Object.assign(Error('Account is unlinked.'),{status:403});return owner;},
    engineFactory:()=>assert.fail('No game starts during desktop sign-in.'),mintDesktopToken:async p=>{signed.push(p.uid);return 'fixture-custom-token';}});
  await new Promise(done=>gateway.server.listen(0,'127.0.0.1',done));const base='http://127.0.0.1:'+gateway.server.address().port+'/v1/desktop/';
  t.after(async()=>{await gateway.close();ledger.close();});
  const post=(p,data,headers={})=>fetch(base+p,{method:'POST',headers:{'Content-Type':'application/json',...headers},body:JSON.stringify(data)});
  const secret=verifier(),start=await post('start',{challenge:challenge(secret)});
  assert.equal(start.status,201);const {id,code}=await start.json();
  const valid={id,code,username:owner.username,consent:true},headers={Origin:website,Authorization:'Bearer verified-session'};
  assert.equal((await post('approve',valid,{...headers,Origin:'https://unrelated.invalid'})).status,403);
  assert.equal((await post('approve',valid,{...headers,Authorization:'Bearer unverified'})).status,403);
  assert.equal((await post('approve',{...valid,username:'Other account'},headers)).status,403);
  assert.equal((await post('approve',{...valid,consent:false},headers)).status,400);
  assert.equal((await post('approve',valid,headers)).status,200);
  assert.equal((await post('redeem',{id,verifier:secret},headers)).status,403);
  assert.equal((await post('redeem',{id,verifier:verifier()})).status,403);
  const response=await post('redeem',{id,verifier:secret,uid:'untrusted-claim'});
  assert.equal(response.headers.get('cache-control'),'no-store');
  assert.deepEqual(await response.json(),{state:'approved',username:owner.username,customToken:'fixture-custom-token'});
  assert.equal((await post('redeem',{id,verifier:secret})).status,410);assert.deepEqual(signed,[owner.uid]);
  assert.deepEqual(await(await fetch(base+'profile',{headers})).json(),{username:owner.username});
});
