import {test} from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {guestKey,accountKey,createLedger,survivalWinner} from './ledger.mjs';
import {createHubService} from './server.mjs';
import {createActivityClient} from './web/activity.mjs';

const catalog=['bannerlands','cat','trashtactics'];
const players=[{uid:'uid1',username:'Anna'},{uid:'uid2',username:'Eli'}];
const draft={id:'verified-match',game:'bannerlands',season:'2026-Q4',players,winner:0};
test('a retry or guest migration cannot add a second lifetime player',()=>{
  const ledger=createLedger(':memory:',catalog);
  const guest=guestKey(randomBytes(32).toString('base64url')),player=accountKey(players[0]);
  ledger.activity(guest,['cat','bannerlands']);ledger.activity(guest,['cat']);
  ledger.activity(player,['cat']);assert.equal(ledger.counts().hub.total,2);
  ledger.linkGuest(guest,player);ledger.linkGuest(guest,player);ledger.activity(guest,['trashtactics']);
  assert.equal(ledger.counts().hub.total,1);assert.equal(ledger.counts().games.cat.accounts,1);
  assert.equal(ledger.counts().games.trashtactics.guests,0);
  assert.throws(()=>ledger.linkGuest(guest,accountKey(players[1])),/different account/);
  assert.throws(()=>ledger.activity(player,['cat','unpublished']),/Unrecognized/);ledger.close();
});
test('ratings are seasonal, symmetric, and recorded once; placement completes after five games',()=>{
  const ledger=createLedger(':memory:',catalog);
  const first=ledger.record(draft);assert.equal(first.players[0].before,1000);assert.equal(first.players[0].rating,1016);assert.equal(first.players[1].change,-16);
  assert.deepEqual(ledger.record(draft),first);assert.throws(()=>ledger.record({...draft,winner:1}),/Conflicting/);
  for(let i=0;i<4;i++)ledger.record({...draft,id:'match-'+i,winner:null});
  const board=ledger.leaderboard('bannerlands','2026-Q4');assert.equal(board.players[0].placementsRemaining,0);assert.notEqual(board.players[0].tier,'Placement');
  assert.equal(ledger.history('Anna').length,5);assert.deepEqual(ledger.leaderboard('bannerlands','2027-Q1').players,[]);ledger.close();
});
test('ranked survival compares cleared waves, then simulated survival time',()=>{
  const score=(waves,timeMs)=>({waves,timeMs});
  assert.equal(survivalWinner([score(4,3000),score(3,4000)]),0);
  assert.equal(survivalWinner([score(4,3000),score(4,4000)]),1);
  assert.equal(survivalWinner([score(4,3000),score(4,3000)]),null);
});
test('offline activity waits for its original account and survives retry',async()=>{
  const data=new Map(),storage={getItem:k=>data.get(k),setItem:(k,v)=>data.set(k,v),removeItem:k=>data.delete(k)};
  let current=players[1],connected=false;const seen=[];
  const api=createActivityClient({storage,withLock:f=>f(),getAccount:async()=>({...current,token:()=>current.uid}),fetchImpl:async(url,req)=>{if(!connected)throw Error('offline');seen.push([JSON.parse(req.body),req.headers.Authorization]);return Response.json({accepted:true});}});
  await api.gameplayStarted('cat','Anna');await api.gameplayStarted('cat','Anna');await api.gameplayStarted('bannerlands',null);
  await assert.rejects(api.sync(),/offline/);connected=true;await api.sync();assert.equal(seen.length,1);assert.match(seen[0][1],/^Guest /);
  current=players[0];await api.sync();await api.sync();assert.equal(seen.length,2);assert.deepEqual(seen[1][0],{games:['cat'],username:'Anna'});
});
test('two verified players receive separate seats; clients cannot submit a result',async t=>{
  const ledger=createLedger(':memory:',catalog);let outcome=null;
  const gateway=createHubService({ledger,identity:async token=>{const who=players.find(p=>p.uid===token);if(!who)throw Object.assign(Error('Unverified'),{status:403});return who;},
    engineFactory:async()=>({seats:players.map((p,i)=>({url:'http://127.0.0.1:1',session:{token:'seat-'+i,team:i}})),started:()=>true,outcome:()=>outcome,close:async()=>{}})});
  await new Promise(done=>gateway.server.listen(0,'127.0.0.1',done));const base='http://127.0.0.1:'+gateway.server.address().port;
  t.after(async()=>{await gateway.close();ledger.close();});
  const post=(path,data,token,origin)=>fetch(base+path,{method:'POST',headers:{'Content-Type':'application/json',...(token?{Authorization:'Bearer '+token}:{}),...(origin?{Origin:origin}:{})},body:JSON.stringify(data)});
  assert.equal((await post('/v1/queue',{game:'bannerlands'},'fake')).status,403);
  assert.equal((await post('/v1/queue',{game:'bannerlands',username:'Eli'},'uid1')).status,403);
  assert.equal((await (await post('/v1/queue',{game:'bannerlands'},'uid1')).json()).state,'waiting');
  const b=await(await post('/v1/queue',{game:'bannerlands'},'uid2')).json(),a=await(await post('/v1/queue',{game:'bannerlands'},'uid1')).json();
  assert.notEqual(a.handoff,b.handoff);assert.equal(b.state,'ready');
  const claimed=await post('/v1/claim',{key:a.handoff},null,'https://hub.catnmice.com');
  assert.match(claimed.headers.get('set-cookie'),/HttpOnly; Secure/);assert.equal((await claimed.json()).session.team,0);
  assert.equal((await post('/v1/claim',{key:a.handoff},null,'https://hub.catnmice.com')).status,410);
  assert.equal((await post('/v1/results',{winner:0},'uid1')).status,404);
  assert.equal(ledger.history('Anna').length,0);outcome={winner:1};
  await new Promise(done=>setTimeout(done,1100));assert.equal(ledger.history('Anna').length,1);assert.equal(ledger.history('Anna')[0].players[1].rating,1016);
});
