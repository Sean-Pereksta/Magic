import test from 'node:test';
import assert from 'node:assert/strict';
import { DiplomacyQueue } from '../diplomacy-queue.mjs';
import { DiplomacyClient } from '../chat.mjs';
import { createGame } from './fixtures/legacy-game.mjs';
import { makeDiagnostic } from '../diagnostics.mjs';
import { beginCouncilMessage } from '../alliance-council.mjs';
import { appendCouncil, ownCouncil } from '../council-state.mjs';
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

test('failed verification completes queued rulers without speech or retrying its token and new verification recovers',async t=>{
  const {state}=councilFixture();state.treaties.push({id:'second-alliance',type:'alliance',parties:['ashen','thornwall'],expires:11});
  const council=ownCouncil(state,'ashen',true),options={actorHouseId:'ashen',councilId:council.id};
  const recovery=deferred(),entered=deferred();let sessions=0,diplomacy=0;
  const c=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',fetcher:async(url,init)=>{
    if(url.endsWith('/session')){
      sessions++;
      if(sessions===1)return Response.json({diagnostics:makeDiagnostic('TURNSTILE_REJECTED')},{status:403});
      entered.resolve();await recovery.promise;
      return Response.json({token:'recovered',expires:Date.now()+1800000});
    }
    diplomacy++;const speaker=JSON.parse(init.body).world.participants.find(p=>p.ai).id;
    return Response.json({responses:[{speakerHouseId:speaker,message:'Our verified scouts stand ready.'}]});
  }});
  t.after(()=>c.cancel());
  const waiting=c.sendCouncilSpeaker(state,'wintermere','first','',true,options);
  await c.queue.wake();assert.equal(sessions,0);assert.equal(c.queue.pending.length,1,'first verification can still wait');
  assert.equal(await c.openSession('failed-token'),false);
  const first=await waiting;
  assert.equal(first.source,'failed');assert.equal(first.diagnostic.path,'/session');
  const second=await c.sendCouncilSpeaker(state,'thornwall','second','failed-token',true,options);
  assert.equal(second.source,'failed');assert.equal(second.diagnostic.code,first.diagnostic.code);
  assert.equal(sessions,1,'another ruler must not resubmit the already rejected token');assert.equal(diplomacy,0);
  const restored=c.sendCouncilSpeaker(state,'wintermere','third','fresh-token',true,options);
  await entered.promise;assert.equal(c.verificationFailure,null);assert.equal(diplomacy,0,'fresh verification must finish first');
  recovery.resolve();assert.equal((await restored).source,'gemini');assert.equal(sessions,2);assert.equal(diplomacy,1);
});

for(const code of ['TURNSTILE_LOAD_FAILED','TURNSTILE_WIDGET_FAILED'])test(`${code} releases waiting Council speakers without synthetic speech`,async t=>{
  const {state,options}=councilFixture();let calls=0;
  const c=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',fetcher:async()=>calls++});t.after(()=>c.cancel());
  const waiting=c.sendCouncilSpeaker(state,'wintermere','first','',true,options);
  await c.queue.wake();c.recordFailure(code,{},'/verification');
  const result=await waiting;assert.equal(result.source,'failed');assert.equal(result.diagnostic.code,code);
  assert.equal((await c.sendCouncilSpeaker(state,'wintermere','second','',true,options)).source,'failed');assert.equal(calls,0);
  assert.equal(c.queue.pending.length,0);
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

test('one Council call returns one ruler and subsequent calls read committed public history',async()=>{
  const seen=[];
  const c=client(async(_url,options)=>{
    const body=JSON.parse(options.body),speakers=body.world.participants.filter(p=>p.ai);
    assert.equal(speakers.length,1);seen.push(body);
    return Response.json({responses:[{speakerHouseId:speakers[0].id,message:`${speakers[0].id} proposes a northern rally.`}]});
  });
  const {state}=councilFixture();state.treaties.push({id:'second-alliance',type:'alliance',parties:['ashen','thornwall'],expires:11});
  const council=ownCouncil(state,'ashen',true);
  appendCouncil(state,council,'ashen','Our next move?');
  const options={actorHouseId:'ashen',councilId:council.id};
  const before=JSON.stringify(state),first=await c.sendCouncilSpeaker(state,'wintermere','Our next move?','',true,{...options,speakerHouseId:'wintermere'});
  assert.equal(seen.length,1,'client never starts the next ruler by itself');
  assert.equal(first.responses.length,1);assert.equal(first.responses[0].speakerHouseId,'wintermere');assert.equal(JSON.stringify(state),before);
  appendCouncil(state,council,'wintermere',first.responses[0].message,{source:first.source});
  const second=await c.sendCouncilSpeaker(state,'thornwall','Our next move?','',true,{...options,speakerHouseId:'thornwall'});
  assert.equal(second.source,'gemini');assert.equal(seen.length,2);
  assert.deepEqual(seen[1].history.map(m=>m.speakerHouseId),['ashen','wintermere']);
  assert.equal(seen[1].history.at(-1).message,first.responses[0].message);
});

test('queued Council speakers never overlap and build context when they actually start',async()=>{
  const gate=deferred(),entered=deferred();let active=0,peak=0;const seen=[];
  const c=client(async(_url,options)=>{
    const body=JSON.parse(options.body),speaker=body.world.participants.find(p=>p.ai).id;
    seen.push(body);peak=Math.max(peak,++active);
    if(seen.length===1){entered.resolve();await gate.promise;}
    active--;return Response.json({responses:[{speakerHouseId:speaker,message:'The shared frontier is safe.'}]});
  });
  const {state}=councilFixture();state.treaties.push({id:'second-alliance',type:'alliance',parties:['ashen','thornwall'],expires:11});
  const council=ownCouncil(state,'ashen',true),options={actorHouseId:'ashen',councilId:council.id};
  const first=c.sendCouncilSpeaker(state,'wintermere','Our next move?','',true,options);await entered.promise;
  let authoritative=state;
  const second=c.sendCouncilSpeaker(state,'thornwall','Our next move?','',true,{...options,getState:()=>authoritative});
  authoritative=structuredClone(state);
  appendCouncil(authoritative,authoritative.allianceCouncils.find(c=>c.id===council.id),'ashen','A newly committed public update.');
  assert.equal(seen.length,1);gate.resolve();await first;await second;
  assert.equal(peak,1);assert.equal(seen[1].history.at(-1).message,'A newly committed public update.');
});

test('a Gemini timeout affects only its speaker and is never automatically retried',async()=>{
  const {state}=councilFixture();
  for(const id of ['redharbor','thornwall'])state.treaties.push({id:`alliance-${id}`,type:'alliance',parties:['ashen',id],expires:11});
  const council=ownCouncil(state,'ashen',true),seen=[],diagnostics=[];
  const c=client(async(_url,options)=>{
    const body=JSON.parse(options.body),speaker=body.world.participants.find(p=>p.ai).id;seen.push(body);
    return speaker==='redharbor'?Response.json({diagnostics:makeDiagnostic('GEMINI_TIMEOUT')},{status:503}):Response.json({responses:[{speakerHouseId:speaker,message:`${speaker} is ready to discuss the frontier.`}]});
  });
  beginCouncilMessage(state,'ashen',council.id,'Our next move?');
  for(const speakerHouseId of ['wintermere','redharbor','thornwall']){
    const response=await c.sendCouncilSpeaker(state,speakerHouseId,'Our next move?','',true,{actorHouseId:'ashen',councilId:council.id,speakerHouseId,onDiagnostic:d=>diagnostics.push(d)});
    if(response.source==='failed'){assert.equal(response.responses.length,0);assert.equal(response.reply,'');}
    else{assert.equal(response.responses.length,1);appendCouncil(state,council,speakerHouseId,response.responses[0].message,{source:response.source});}
  }
  assert.deepEqual(seen.map(b=>b.world.participants.find(p=>p.ai).id),['wintermere','redharbor','thornwall']);
  assert.deepEqual(council.messages.slice(-2).map(m=>m.source),['gemini','gemini']);
  assert.deepEqual(seen[2].history.map(m=>m.speakerHouseId),['ashen','wintermere']);
  assert.equal(diagnostics.length,1);assert.equal(diagnostics[0].code,'GEMINI_TIMEOUT');assert.equal(c.cooldownUntil,0);
  assert.equal(council.messages.some(m=>m.speakerHouseId==='redharbor'),false,'failed ruler has no synthetic response');
});

test('one malformed leader reply fails without generating substitute speech',async()=>{
  const {state}=councilFixture();state.treaties.push({id:'second-alliance',type:'alliance',parties:['ashen','thornwall'],expires:11});
  const council=ownCouncil(state,'ashen',true);let calls=0;
  const c=client(async()=>{calls++;return Response.json({responses:[{speakerHouseId:'thornwall',message:'I impersonate the current speaker.'}]});});
  const response=await c.sendCouncilSpeaker(state,'wintermere','Our next move?','',true,{actorHouseId:'ashen',councilId:council.id});
  assert.equal(calls,1);assert.equal(response.source,'failed');assert.equal(response.diagnostic.code,'GEMINI_RESPONSE_INVALID');
  assert.deepEqual(response.responses,[]);assert.equal(response.reply,'');
});

test('invalid or human Council speakers cancel without waiting for verification',async()=>{
  const {state,options}=councilFixture();let calls=0;
  const c=new DiplomacyClient({endpoint:'https://worker.example/diplomacy',fetcher:async()=>calls++});
  for(const speakerHouseId of ['ashen','vesper'])assert.equal((await c.sendCouncilSpeaker(state,speakerHouseId,'hello','',true,options)).source,'cancelled');
  assert.equal(calls,0);assert.equal(c.queue.pending.length,0);
});

for(const failure of ['malformed','network'])test(`${failure} failures allow an immediate explicit retry`,async()=>{
  let calls=0;const c=client(async()=>{if(++calls===1){if(failure==='network')throw Error('offline');return new Response('invalid JSON');}return Response.json(reply);});
  const state=createGame();assert.equal((await c.send(state,'wintermere','first')).source,'failed');
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

test('automatic Council voices commit independently and a failed voice creates no new speech',async()=>{
  const {nextCouncilDispatch,applyCouncilDispatch,councilDispatchCurrent}=await import('../council-dispatch.mjs');
  const {state}=councilFixture();state.treaties.push({id:'second-alliance',type:'alliance',parties:['ashen','thornwall'],expires:11});
  const council=ownCouncil(state,'ashen',true);
  appendCouncil(state,council,'wintermere','Food is needed.',{initiated:true,source:'scripted',reason:'food'});
  appendCouncil(state,council,'thornwall','We can discuss supplies.',{source:'scripted'});
  const dispatch=nextCouncilDispatch(state,'ashen');let calls=0;
  const c=client(async(_url,options)=>{
    const body=JSON.parse(options.body),speaker=body.world.participants.find(p=>p.ai).id;
    assert.equal(body.world.dispatch.entries.length,1);calls++;
    if(calls===2){assert.equal(body.history.at(-1).message,'Let us arrange food for the frontier.');return Response.json({responses:[]});}
    return Response.json({responses:[{speakerHouseId:speaker,message:'Let us arrange food for the frontier.'}]});
  });
  for(const entry of dispatch.entries){
    const currentDispatch={...dispatch,entries:[entry]};
    assert.equal(councilDispatchCurrent(state,'ashen',currentDispatch),true);
    const response=await c.sendCouncilSpeaker(state,entry.speakerHouseId,'Dispatch','',true,{actorHouseId:'ashen',councilId:council.id,councilDispatch:currentDispatch});
    assert.equal(applyCouncilDispatch(state,'ashen',currentDispatch,response),true);
    assert.equal(applyCouncilDispatch(state,'ashen',currentDispatch,response),false,'same entry cannot be committed twice');
  }
  assert.equal(calls,2);assert.deepEqual(council.messages.map(m=>m.source),['gemini','scripted']);
  assert.equal(council.messages[1].message,'We can discuss supplies.','failed voice leaves underlying event facts unchanged');
  assert.equal(council.messages[1].voiceFailed,true);assert.equal(council.messages[1].voiced,undefined);
  assert.equal(council.messages[1].diagnostic.code,'GEMINI_RESPONSE_INVALID');
  assert.equal(nextCouncilDispatch(state,'ashen'),null);
});

test('Gemini keeps its own valid vassal voice while concrete contradictions fail silently',async()=>{
  const state=createGame();state.treaties.push({type:'vassalage',parties:['ashen','wintermere'],liege:'ashen',vassal:'wintermere',expires:20});
  const voices=['My liege, our forces await your orders.','My liege, I command 999999 troops.','Your soldiers stand close to our frontier. Explain their purpose.'];
  let calls=0;const c=client(async()=>Response.json({reply:voices[calls++],tone:'neutral',intents:[]}));
  const valid=await c.send(state,'wintermere','How many troops are in your full army?');
  assert.equal(valid.source,'gemini');assert.equal(valid.reply,voices[0],'do not replace a legitimate Gemini response with scripted muster prose');
  const count=await c.send(state,'wintermere','How many troops are in your full force?');
  assert.equal(count.source,'failed');assert.equal(count.reply,'');assert.equal(count.diagnostic.code,'GEMINI_RESPONSE_INVALID');
  const bark=await c.send(state,'wintermere','Greetings.');
  assert.equal(bark.source,'failed');assert.equal(bark.reply,'');assert.equal(calls,3);
});

test('disabled Gemini and missing endpoint never generate private, general or council dialogue',async()=>{
  const {state,options}=councilFixture();
  for(const c of [new DiplomacyClient(),client(async()=>{throw Error('must not fetch');})]){
    for(const request of [()=>c.send(state,'wintermere','hello','',false),()=>c.send(state,'ashen','report','',false,{actorHouseId:'ashen',generalId:'general-1'}),()=>c.sendCouncilSpeaker(state,'wintermere','hello','',false,options)]){
      const result=await request();assert.equal(result.source,'failed');assert.equal(result.reply,'');assert.deepEqual(result.responses,[]);assert.deepEqual(result.intents,[]);
    }
  }
});

test('a rejected general claim is not cached and an explicit retry can receive Gemini speech',async()=>{
  const {refreshGeneralCandidates,hireGeneral}=await import('../generals.mjs');const {kingdom}=await import('../core.mjs');
  const state=createGame();state.turn=8;state.commanders.nextOffer.ashen=8;refreshGeneralCandidates(state);
  const general=state.commanders.candidates.find(g=>g.owner==='ashen');kingdom(state,'ashen').resources.gold=1000;assert.equal(hireGeneral(state,'ashen',general.id).ok,true);
  let calls=0;const c=client(async()=>Response.json({reply:++calls===1?'We have captured the city.':'Our army awaits the next order.'}));
  const options={actorHouseId:'ashen',generalId:general.id};
  const invalid=await c.send(state,'ashen','Report your progress.','',true,options);
  assert.equal(invalid.source,'failed');assert.equal(invalid.reply,'');assert.equal(c.cache.size,0);
  const retry=await c.send(state,'ashen','Report your progress.','',true,options);
  assert.equal(retry.source,'gemini');assert.equal(retry.reply,'Our army awaits the next order.');assert.equal(calls,2);
});
