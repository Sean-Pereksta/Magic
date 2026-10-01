import test from 'node:test';
import assert from 'node:assert/strict';
import { DiplomacyQueue } from '../diplomacy-queue.mjs';
import { DiplomacyClient } from '../chat.mjs';
import { createGame } from './fixtures/legacy-game.mjs';
import { makeDiagnostic } from '../diagnostics.mjs';
import { ownCouncil } from '../council-state.mjs';
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
const reply={reply:'Your help on the northern frontier is welcome.',tone:'warm',intents:[]};
function councilFixture() {
  const state=createGame();state.treaties.push({id:'queue-alliance',type:'alliance',parties:['ashen','wintermere'],expires:11});
  return {state,options:{actorHouseId:'ashen',councilId:ownCouncil(state,'ashen',true).id}};
}
const councilReply={responses:[{speakerHouseId:'wintermere',message:'My scouts welcome your help.'}]};
function client(fetcher){const c=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',fetcher});c.session={token:'test-session',expires:Date.now()+1800000};return c;}

test('player jobs pass waiting background jobs, and only one request runs at once',async()=>{
  const gate=deferred(),entered=deferred(),seen=[];let active=0,peak=0;
  const c=client(async(_url,options)=>{
    const message=JSON.parse(options.body).message;seen.push(message);peak=Math.max(peak,++active);
    if(message==='first'){entered.resolve();await gate.promise;}active--;return Response.json(reply);
  });
  const state=createGame();
  const first=c.send(state,'wintermere','first');await entered.promise;
  const background=c.send(state,'wintermere','background','',true,{background:true});
  const player=c.send(state,'wintermere','player');gate.resolve();
  const results=await Promise.all([first,background,player]);
  assert.deepEqual(seen,['first','player','background']);assert.equal(peak,1);
  assert.ok(results.every(r=>r.source==='gemini'),'busy clients queue instead of generating fallback dialogue');
});

test('background quota failure is shared with the waiting player and preserves the exact error',async()=>{
  let calls=0;const entered=deferred(),gate=deferred();
  const c=client(async()=>{calls++;entered.resolve();await gate.promise;return Response.json({diagnostics:makeDiagnostic('GEMINI_QUOTA',{providerStatus:429})},{status:503,headers:{'Retry-After':'300'}});});
  const state=createGame(),background=c.send(state,'wintermere','background','',true,{background:true});
  await entered.promise;const player=c.send(state,'wintermere','player');gate.resolve();
  const [a,b]=await Promise.all([background,player]);
  assert.equal(calls,1);assert.equal(b.diagnostic.code,'GEMINI_QUOTA');assert.match(b.notice,/GEMINI_QUOTA/);assert.match(b.notice,/seconds/);assert.equal(a.diagnostic,b.diagnostic);
});

test('cancel and stale guards discard queued work without network requests',async()=>{
  const q=new DiplomacyQueue();let calls=0;
  const old=q.enqueue(()=>calls++);q.cancel();assert.equal((await old).source,'cancelled');
  assert.equal((await q.enqueue(()=>calls++,{isCurrent:()=>false})).source,'cancelled');assert.equal(calls,0);
  assert.equal(await q.enqueue(()=> 'new campaign'),'new campaign');
});

test('cancelling an active request does not impose a provider cooldown on the next campaign',async()=>{
  const entered=deferred();let calls=0;
  const c=client(async(_url,{signal})=>{calls++;if(calls>1)return Response.json(reply);entered.resolve();return new Promise((_,reject)=>signal.addEventListener('abort',()=>reject(new Error('aborted'))));});
  const pending=c.send(createGame(),'wintermere','first');await entered.promise;c.cancel();
  assert.equal((await pending).source,'cancelled');assert.equal(c.cooldownUntil,0);
  assert.equal((await c.send(createGame(),'wintermere','second')).source,'gemini');
});

test('Council messages wait for verification and all drain in order after a Worker allowance cooldown',async()=>{
  let now=Date.now(),calls=0;const seen=[],blocked=deferred();
  const c=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',now:()=>now,fetcher:async(url,init)=>{
    if(url.endsWith('/session'))return Response.json({token:'verified',expires:now+1800000});
    calls++;const body=JSON.parse(init.body);seen.push(body.message);
    if(calls===1)return Response.json({diagnostics:makeDiagnostic('CLIENT_RATE_LIMIT')},{status:429,headers:{'Retry-After':'30'}});
    return Response.json(councilReply);
  }});
  const {state,options}=councilFixture(),statuses=[];
  const first=c.send(state,'wintermere','first','',true,{...options,onStatus:s=>{statuses.push(s);if(s==='cooldown')blocked.resolve();}});
  const second=c.send(state,'wintermere','second','',true,options);
  await c.queue.wake();assert.equal(calls,0);assert.ok(statuses.includes('verification'));
  await c.openSession('challenge');await blocked.promise;
  assert.equal(calls,1);assert.equal(c.queue.pending.length,2);
  now+=30000;await c.queue.wake();
  assert.deepEqual((await Promise.all([first,second])).map(r=>r.source),['gemini','gemini']);
  assert.deepEqual(seen,['first','first','second']);
});

test('a queued Council can be cancelled during verification without sending or hanging',async()=>{
  let calls=0;const c=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',fetcher:async()=>{calls++;}});
  const {state,options}=councilFixture();const pending=c.send(state,'wintermere','waiting','',true,options);
  await c.queue.wake();c.cancel();assert.equal((await pending).source,'cancelled');assert.equal(calls,0);assert.equal(c.queue.timer,null);
});

test('a Google format rejection is reported once; waiting Council jobs still wait for cooldown',async()=>{
  let now=Date.now(),calls=0;const c=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',now:()=>now,fetcher:async()=>{
    calls++;return calls===1?Response.json({diagnostics:makeDiagnostic('GEMINI_SCHEMA',{providerStatus:400})},{status:503,headers:{'Retry-After':'60'}}):Response.json(councilReply);
  }});c.session={token:'test',expires:now+1800000};
  const {state,options}=councilFixture(),first=c.send(state,'wintermere','first','',true,options),second=c.send(state,'wintermere','second','',true,options);
  assert.equal((await first).diagnostic.code,'GEMINI_SCHEMA');assert.equal(calls,1);assert.equal(c.queue.pending.length,1);
  now+=60000;await c.queue.wake();assert.equal((await second).source,'gemini');assert.equal(calls,2);
});
