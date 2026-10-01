import test from 'node:test';
import assert from 'node:assert/strict';
import { DiplomacyQueue } from '../diplomacy-queue.mjs';
import { DiplomacyClient } from '../chat.mjs';
import { createGame } from './fixtures/legacy-game.mjs';
import { makeDiagnostic } from '../diagnostics.mjs';
import { beginCouncilMessage, finishCouncilMessage } from '../alliance-council.mjs';
import { ownCouncil } from '../council-state.mjs';
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
const reply={reply:'Your help on the northern frontier is welcome.',tone:'warm',intents:[]};
function councilFixture() {
  const state=createGame();state.treaties.push({id:'queue-alliance',type:'alliance',parties:['ashen','wintermere'],expires:11});
  return {state,options:{actorHouseId:'ashen',councilId:ownCouncil(state,'ashen',true).id}};
}
const councilReply={responses:[{speakerHouseId:'wintermere',message:'My scouts welcome your help.'}]};
function client(fetcher){const c=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',fetcher});c.session={token:'test-session',expires:Date.now()+1800000};return c;}

test('private conversations bypass a pending Council request',async()=>{
  const gate=deferred(),entered=deferred();let councilCalls=0,privateCalls=0;
  const c=client(async(_url,options)=>{
    const body=JSON.parse(options.body);
    if(body.mode==='allianceCouncil'){councilCalls++;entered.resolve();await gate.promise;return Response.json(councilReply);}
    privateCalls++;return Response.json(reply);
  });
  const {state,options}=councilFixture();
  const council=c.send(state,'wintermere','Council request','',true,options);await entered.promise;
  assert.equal((await c.send(state,'thornwall','Private request')).source,'gemini');
  assert.equal(privateCalls,1);assert.equal(councilCalls,1);assert.equal(c.busy,true,'Council still in flight');
  gate.resolve();assert.equal((await council).source,'gemini');assert.equal(c.busy,false);
});

test('private requests are independent of one another and share verification only',async()=>{
  const gate=deferred(),entered=deferred();const seen=[];
  const c=client(async(_url,options)=>{
    const {message}=JSON.parse(options.body);seen.push(message);
    if(message==='background'){entered.resolve();await gate.promise;}
    return Response.json(reply);
  });
  const state=createGame(),background=c.send(state,'wintermere','background','',true,{background:true});
  await entered.promise;assert.equal((await c.send(state,'thornwall','player')).source,'gemini');
  assert.deepEqual(seen,['background','player']);gate.resolve();assert.equal((await background).source,'gemini');
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

test('a Google format rejection does not delay the next Council or private message',async()=>{
  let calls=0;const c=client(async(_url,options)=>{
    calls++;if(calls===1)return Response.json({diagnostics:makeDiagnostic('GEMINI_REQUEST',{providerStatus:400})},{status:503,headers:{'Retry-After':'60'}});
    return Response.json(JSON.parse(options.body).mode==='allianceCouncil'?councilReply:reply);
  });
  const {state,options}=councilFixture();
  assert.equal((await c.send(state,'wintermere','first','',true,options)).diagnostic.code,'GEMINI_REQUEST');
  assert.equal(c.cooldownUntil,0);
  assert.equal((await c.send(state,'wintermere','second','',true,options)).source,'gemini');
  assert.equal((await c.send(state,'thornwall','private')).source,'gemini');assert.equal(calls,3);
});

test('each Council leader waits for the previous reply and receives it as history',async()=>{
  const gate=deferred(),entered=deferred();let active=0,peak=0;const seen=[];
  const c=client(async(_url,options)=>{
    const body=JSON.parse(options.body),speakers=body.world.participants.filter(p=>p.ai);
    assert.equal(speakers.length,1);seen.push(body);peak=Math.max(peak,++active);
    if(seen.length===1){entered.resolve();await gate.promise;}active--;
    return Response.json({responses:[{speakerHouseId:speakers[0].id,message:`${speakers[0].id} proposes a northern rally.`}]});
  });
  const {state}=councilFixture();state.treaties.push({id:'second-alliance',type:'alliance',parties:['ashen','thornwall'],expires:11});
  const council=ownCouncil(state,'ashen',true),before=JSON.stringify(state);
  const result=c.send(state,'wintermere','Our next move?','',true,{actorHouseId:'ashen',councilId:council.id});
  await entered.promise;assert.equal(seen.length,1);gate.resolve();
  const response=await result;assert.equal(response.source,'gemini');assert.equal(seen.length,2);assert.equal(peak,1);
  assert.equal(seen[1].history.at(-1).message,`${seen[0].world.participants.find(p=>p.ai).id} proposes a northern rally.`);
  assert.deepEqual(response.responses.map(r=>r.speakerHouseId).sort(),['thornwall','wintermere']);assert.equal(JSON.stringify(state),before);
});

test('one malformed leader reply does not suppress the remaining leaders or mislabel local text',async()=>{
  const {state}=councilFixture();state.treaties.push({id:'second-alliance',type:'alliance',parties:['ashen','thornwall'],expires:11});
  const council=ownCouncil(state,'ashen',true);let calls=0;
  const c=client(async(_url,options)=>{
    const speaker=JSON.parse(options.body).world.participants.find(p=>p.ai).id;calls++;
    return calls===1?Response.json({responses:[]}):Response.json({responses:[{speakerHouseId:speaker,message:'I will discuss the next objective.'}]});
  });
  const start=beginCouncilMessage(state,'ashen',council.id,'Our next move?');
  const response=await c.send(state,'wintermere','Our next move?','',true,{actorHouseId:'ashen',councilId:council.id});
  assert.equal(calls,2);assert.equal(c.cooldownUntil,0);assert.equal(response.source,'mixed');assert.equal(response.diagnostic.code,'GEMINI_RESPONSE_INVALID');
  assert.equal(finishCouncilMessage(state,'ashen',start,'Our next move?',response).ok,true);
  assert.deepEqual(council.messages.slice(-2).map(m=>m.source),['scripted','gemini']);
});

for(const failure of ['malformed','network'])test(`${failure} failures allow an immediate explicit retry`,async()=>{
  let calls=0;const c=client(async()=>{if(++calls===1){if(failure==='network')throw Error('offline');return new Response('invalid JSON');}return Response.json(reply);});
  const state=createGame();assert.equal((await c.send(state,'wintermere','first')).source,'scripted');
  assert.equal(c.cooldownUntil,0);assert.equal(calls,1);assert.equal((await c.send(state,'wintermere','second')).source,'gemini');assert.equal(calls,2);
});

test('an actual Gemini rate limit retains its retry time for every chat',async()=>{
  let now=Date.now(),calls=0;const c=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',now:()=>now,fetcher:async()=>{
    calls++;return calls===1?Response.json({diagnostics:makeDiagnostic('GEMINI_QUOTA',{providerStatus:429})},{status:503,headers:{'Retry-After':'30'}}):Response.json(reply);
  }});c.session={token:'test',expires:now+1800000};const state=createGame();
  const limited=await c.send(state,'wintermere','first');assert.equal(limited.diagnostic.retryAt,now+30000);
  assert.equal((await c.send(state,'thornwall','next')).diagnostic.code,'GEMINI_QUOTA');assert.equal(calls,1);
  now+=30000;assert.equal((await c.send(state,'thornwall','again')).source,'gemini');assert.equal(calls,2);
});

test('generals can answer while a Council leader is still generating',async()=>{
  const {refreshGeneralCandidates,hireGeneral}=await import('../generals.mjs');
  const {kingdom}=await import('../core.mjs');
  const {state,options}=councilFixture();state.turn=8;state.commanders.nextOffer.ashen=8;refreshGeneralCandidates(state);
  const general=state.commanders.candidates.find(g=>g.owner==='ashen');kingdom(state,'ashen').resources.gold=1000;
  assert.equal(hireGeneral(state,'ashen',general.id).ok,true);
  const entered=deferred(),gate=deferred();const c=client(async(_url,init)=>{
    const body=JSON.parse(init.body);
    if(body.mode==='allianceCouncil'){entered.resolve();await gate.promise;return Response.json(councilReply);}
    assert.equal(body.mode,'general');return Response.json({reply:'Our forces await your orders.'});
  });
  const pending=c.send(state,'wintermere','Council','',true,options);await entered.promise;
  const response=await c.send(state,'ashen','General report','',true,{actorHouseId:'ashen',generalId:general.id});
  assert.equal(response.source,'gemini');gate.resolve();assert.equal((await pending).source,'gemini');
});

test('campaign cancellation aborts all concurrently active private requests',async()=>{
  const entered=deferred();let calls=0;
  const c=client(async(_url,{signal})=>{if(++calls===2)entered.resolve();return new Promise((_,reject)=>signal.addEventListener('abort',()=>reject(Error('cancelled'))));});
  const state=createGame(),a=c.send(state,'wintermere','first'),b=c.send(state,'thornwall','second');
  await entered.promise;c.cancel();assert.deepEqual((await Promise.all([a,b])).map(r=>r.source),['cancelled','cancelled']);
  assert.equal(c.cooldownUntil,0);assert.equal(c.busy,false);assert.equal(c.controllers.size,0);
});

test('partial automatic Council voices retain successful Gemini text and can retry the failed reaction',async()=>{
  const {nextCouncilDispatch,applyCouncilDispatch}=await import('../council-dispatch.mjs');
  const {appendCouncil}=await import('../council-state.mjs');
  const {state}=councilFixture();state.treaties.push({id:'second-alliance',type:'alliance',parties:['ashen','thornwall'],expires:11});
  const council=ownCouncil(state,'ashen',true);
  appendCouncil(state,council,'wintermere','Food is needed.',{initiated:true,source:'scripted',reason:'food'});
  appendCouncil(state,council,'thornwall','We can discuss supplies.',{source:'scripted'});
  const dispatch=nextCouncilDispatch(state,'ashen');let calls=0;
  const c=client(async(_url,options)=>{
    const body=JSON.parse(options.body),speaker=body.world.participants.find(p=>p.ai).id;
    assert.equal(body.world.dispatch.entries.length,1);calls++;
    return calls===2?Response.json({responses:[]}):Response.json({responses:[{speakerHouseId:speaker,message:'Let us arrange food for the frontier.'}]});
  });
  const options={actorHouseId:'ashen',councilId:council.id,councilDispatch:dispatch};
  const response=await c.send(state,'wintermere','Dispatch','',true,options);
  assert.equal(response.source,'mixed');assert.equal(applyCouncilDispatch(state,'ashen',dispatch,response),true);
  assert.deepEqual(council.messages.map(m=>m.source),['gemini','scripted']);
  const retry=nextCouncilDispatch(state,'ashen');assert.deepEqual(retry.entries.map(e=>e.speakerHouseId),['thornwall']);
  const second=await c.send(state,'thornwall','Dispatch','',true,{...options,councilDispatch:retry});
  assert.equal(applyCouncilDispatch(state,'ashen',retry,second),true);assert.equal(calls,3);
  assert.deepEqual(council.messages.map(m=>m.source),['gemini','gemini']);assert.equal(nextCouncilDispatch(state,'ashen'),null);
});
