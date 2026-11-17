import test from 'node:test';
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
import {join} from 'node:path';
import {createEngineFactory} from './engines.mjs';
import {createHubService} from './server.mjs';
import {createLedger} from './ledger.mjs';

const bannerlands=process.env.BANNERLANDS_ROOT,catEdition=process.env.CAT_EDITION_ROOT;
const players=[{username:'TestA',uid:'test-a'},{username:'TestB',uid:'test-b'}];
const submit=async(url,path,data,auth,headers={})=>fetch(url+path,{method:'POST',headers:{'Content-Type':'application/json',...(auth?{Authorization:'Bearer '+auth}:{}),...headers},body:JSON.stringify(data)});
test('real Bannerlands clients reserve, ready, synchronize and see the same official battle', {skip:!bannerlands},async t=>{
  const match=await createEngineFactory({bannerlands})({id:'local-proof',game:'bannerlands',players});t.after(()=>match.close());
  const [a,b]=match.seats;assert.equal(a.url,b.url);assert.equal(a.session.team,0);assert.equal(b.session.team,1);
  const lobby=(await(await fetch(a.url+'/api/lobby')).json()).lobby;
  assert.equal(lobby.aiHouses,0);assert.equal(lobby.maxHumans,2);
  for(const seat of [a,b])assert.equal((await submit(seat.url,'/api/ready',{assetsReady:true,ready:true,revision:lobby.revision},seat.session.token)).status,200);
  const started=await(await submit(a.url,'/api/start',{revision:lobby.revision},a.session.token)).json();
  for(const seat of [a,b]){
    const state=await(await fetch(seat.url+'/api/state?initial=1',{headers:{Authorization:'Bearer '+seat.session.token}})).json();
    assert.equal(state.lobby.matchId,started.lobby.matchId);
    assert.equal((await submit(seat.url,'/api/synchronized',{sequence:state.snapshot.sequence,matchId:started.lobby.matchId},seat.session.token)).status,200);
  }
  assert.equal(match.started(),true);assert.equal(match.outcome(),null);
});
test('ranked survival uses matching seeded simulations and derives terminal waves and time', {skip:!catEdition},async()=>{
  const {createSimulation}=await import(pathToFileURL(join(catEdition,'simulation-ranked.mjs')));
  const sims=players.map((p,i)=>createSimulation({matchId:'match-'+i,seed:'identical-challenge',mapId:'flour-mill',difficulty:'hard',players:[{id:p.uid,name:p.username}]}));
  for(const sim of sims){for(let i=0;i<30000&&sim.status==='active';i++)sim.step();assert.notEqual(sim.status,'active');}
  assert.deepEqual(sims[0].rankedScore,sims[1].rankedScore);assert.ok(sims[0].rankedScore.timeMs>0);
});
test('ranked Cat & Mouse players recover independent seats through the official HTTP and WebSocket gateway', {skip:!catEdition},async t=>{
  const ledger=createLedger(':memory:',['cat','bannerlands']);
  const siteOrigin='https://www.catnmice.com',serviceOrigin='https://hub.catnmice.com';
  const service=createHubService({ledger,identity:async token=>players.find(p=>p.uid===token),engineFactory:createEngineFactory({catEdition}),siteOrigin,serviceOrigin});
  await new Promise(done=>service.server.listen(0,'127.0.0.1',done));const base='http://127.0.0.1:'+service.server.address().port;
  t.after(async()=>{await service.close();ledger.close();});
  const enqueue=async p=>(await submit(base,'/v1/queue',{username:p.username,game:'catandmouse'},p.uid)).json();
  assert.equal((await enqueue(players[0])).state,'waiting');
  const keys=[null,(await enqueue(players[1])).handoff];keys[0]=(await enqueue(players[0])).handoff;
  const rooms=[];
  for(const key of keys){
    const claim=await submit(base,'/v1/claim',{key},null,{origin:serviceOrigin});
    const seat=await claim.json(),cookie=claim.headers.get('set-cookie').split(';')[0];
    assert.ok(seat.token);rooms.push(seat.room);
    const page=await fetch(base+'/',{headers:{cookie}});assert.match(await page.text(),/hub-status\.mjs/);
    const socket=new WebSocket(base.replace('http:','ws:')+'/socket',{headers:{cookie,origin:serviceOrigin}});
    try{await new Promise((done,reject)=>{
      const timer=setTimeout(()=>reject(Error('Join timed out.')),5000);
      socket.onopen=()=>socket.send(JSON.stringify({type:'join',version:'2',roomId:seat.room,token:seat.token}));
      socket.onmessage=e=>{const result=JSON.parse(e.data);if(result.type==='lobby'){clearTimeout(timer);assert.equal(result.lobby.players.length,1);assert.equal(result.lobby.players[0].isHost,true);assert.equal(result.lobby.difficulty,'hard');done();}};
      socket.onerror=()=>{clearTimeout(timer);reject(Error('WebSocket admission failed.'));};
    });}finally{socket.close();}
  }
  assert.notEqual(rooms[0],rooms[1]);
});
