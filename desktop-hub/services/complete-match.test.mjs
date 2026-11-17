import test from 'node:test';
import assert from 'node:assert/strict';
import {createLedger} from './ledger.mjs';
import {createHubService} from './server.mjs';
import {createEngineFactory} from './engines.mjs';
import {pathToFileURL} from 'node:url';
import {join} from 'node:path';

// This exercises the real-time simulator at its production clock rate. Both
// protocol clients here run on one computer using test-only verified identities.
test('complete a ranked Bannerlands match and read the same Elo on both clients',{
  skip:process.env.HUB_LONG_MATCH!=='1'||!process.env.BANNERLANDS_ROOT,timeout:420000
},async t=>{
  const ledger=createLedger(':memory:',['bannerlands']);
  const game='bannerlands',players=[{uid:'test-1',username:'Castle A'},{uid:'test-2',username:'Castle B'}];
  const service=createHubService({ledger,identity:async token=>players.find(p=>p.uid===token),engineFactory:createEngineFactory({bannerlands:process.env.BANNERLANDS_ROOT})});
  await new Promise(done=>service.server.listen(0,'127.0.0.1',done));
  t.after(async()=>{await service.close();ledger.close();});
  const base='http://127.0.0.1:'+service.server.address().port;
  async function request(path,{data,token,cookie,origin}={}){
    const headers={...(token?{Authorization:'Bearer '+token}:{}),...(cookie?{cookie}:{}),...(origin?{origin}:{}),...(data?{'Content-Type':'application/json'}:{})};
    const res=await fetch(base+path,{method:data?'POST':'GET',headers,body:data?JSON.stringify(data):undefined});
    const body=await res.json();
    if(!res.ok&&path!='/api/command')throw new Error(path+' '+res.status+' '+JSON.stringify(body).slice(0,200));
    return {body,cookie:res.headers.get('set-cookie')?.split(';')[0]};
  }
  const queue=player=>request('/v1/queue',{data:{game,username:player.username},token:player.uid});
  assert.equal((await queue(players[0])).body.state,'waiting');
  const right=(await queue(players[1])).body,left=(await queue(players[0])).body;
  const seats=await Promise.all([left,right].map(async r=>{
    const claim=await request('/v1/claim',{data:{key:r.handoff},origin:'https://hub.catnmice.com'});
    return {token:claim.body.session.token,cookie:claim.cookie};
  }));
  const call=async(team,path,data)=> (await request(path,{...seats[team],data})).body;
  const revision=(await call(0,'/api/state?initial=1')).lobby.revision;
  for(const i of [0,1])await call(i,'/api/ready',{assetsReady:true,ready:true,revision});
  const matchId=(await call(0,'/api/start',{revision})).lobby.matchId;
  for(const i of [0,1]){
    const start=await call(i,'/api/state?initial=1');
    await call(i,'/api/synchronized',{matchId,sequence:start.snapshot.sequence});
  }
  const seq=[0,0];let elapsed=0,receipt;
  const order=(i,type,args)=>call(i,'/api/command',{v:1,seq:++seq[i],matchId,type,args});
  const expires=Date.now()+390000;
  while(Date.now()<expires){
    const outcome=(await request('/v1/result',{cookie:seats[0].cookie})).body;
    if(outcome.receipt){receipt=outcome.receipt;break;}
    const views=await Promise.all([0,1].map(i=>call(i,'/api/state?initial=1')));
    elapsed=views[0].snapshot.time;
    const keep=views[0].snapshot.buildings.find(b=>b.type==='keep'&&b.team===1&&b.hp>0);
    const units=i=>views[i].snapshot.units.filter(u=>u.team===i&&u.hp>0).map(u=>u.id);
    if(keep&&units(0).length)await order(0,'attackTarget',[units(0),keep.id]);
    if(units(1).length)await order(1,'command',[units(1),views[1].snapshot.W/2,views[1].snapshot.H/2,'move']);
    for(let n=0;n<2;n++)await order(0,'recruit',['infantry']);
    await new Promise(done=>setTimeout(done,5000));
  }
  assert.ok(receipt,'No natural victory after '+elapsed+' seconds.');
  const rival=(await request('/v1/result',{cookie:seats[1].cookie})).body.receipt;
  assert.deepEqual(receipt,rival);const board=(await request('/v1/leaderboard?game=bannerlands')).body;
  assert.deepEqual(board.players.map(p=>p.rating).sort(),[1016,984].sort());assert.equal(ledger.counts().hub.accounts,2);
  t.diagnostic('Authoritative winner '+players[receipt.winner].username+' after '+Math.round(elapsed)+' simulated seconds. Both clients received the same receipt.');
});

test('complete both server-controlled survival challenges and record one shared receipt',{
  skip:process.env.HUB_LONG_MATCH!=='1'||!process.env.CAT_EDITION_ROOT,timeout:240000
},async t=>{
  const catEdition=process.env.CAT_EDITION_ROOT;
  const {STARTER_LOADOUT}=await import(pathToFileURL(join(catEdition,'battlefield.mjs')));
  const ledger=createLedger(':memory:',['cat']);
  const players=[{uid:'a',username:'Mouse A'},{uid:'b',username:'Mouse B'}];
  const serviceOrigin='https://hub.catnmice.com';
  const gateway=createHubService({ledger,identity:async token=>players.find(p=>p.uid===token),engineFactory:createEngineFactory({catEdition})});
  await new Promise(done=>gateway.server.listen(0,'127.0.0.1',done));
  const base='http://127.0.0.1:'+gateway.server.address().port;
  const sockets=[];t.after(async()=>{for(const s of sockets)s.close();await gateway.close();ledger.close();});
  const post=(path,data,headers={})=>fetch(base+path,{method:'POST',headers:{'Content-Type':'application/json',...headers},body:JSON.stringify(data)});
  const queue=async p=>(await post('/v1/queue',{game:'catandmouse',username:p.username},{Authorization:'Bearer '+p.uid})).json();
  await queue(players[0]);const right=await queue(players[1]),left=await queue(players[0]);
  const cookies=[];
  await Promise.all([left,right].map(async r=>{
    const res=await post('/v1/claim',{key:r.handoff},{origin:serviceOrigin}),seat=await res.json();
    const cookie=res.headers.get('set-cookie').split(';')[0];cookies.push(cookie);
    const socket=new WebSocket(base.replace('http:','ws:')+'/socket',{headers:{cookie,origin:serviceOrigin}});
    sockets.push(socket);const send=(type,data={})=>socket.send(JSON.stringify({type,...data}));
    await new Promise((done,reject)=>{
      const timeout=setTimeout(()=>reject(Error('Survival run did not finish.')),215000);let ready=false,started=false;
      socket.onopen=()=>send('join',{token:seat.token,version:'2',roomId:seat.room});
      socket.onerror=()=>{clearTimeout(timeout);reject(Error('Survival WebSocket failed.'));};
      socket.onmessage=e=>{
        const m=JSON.parse(e.data);
        if(m.type==='error'){clearTimeout(timeout);reject(Error(m.message));return;}
        if(m.type==='lobby'&&m.lobby.phase==='waiting'){
          if(!ready){ready=true;send('ready',{ready:true,loadout:STARTER_LOADOUT});}
          else if(m.lobby.canStart&&!started){started=true;send('start');}
        }
        if(m.type==='prepare')send('loaded',{matchId:m.matchId});
        if(m.type==='lobby'&&m.lobby.phase==='finished'){clearTimeout(timeout);done();}
      };
    });
  }));
  await new Promise(done=>setTimeout(done,800));
  const receipts=await Promise.all(cookies.map(async cookie=>(await(await fetch(base+'/v1/result',{headers:{cookie}})).json()).receipt));
  assert.ok(receipts[0]);assert.deepEqual(receipts[0],receipts[1]);assert.equal(receipts[0].winner,null);
  assert.ok(receipts[0].scores[0].timeMs>0);assert.deepEqual(receipts[0].scores[0],receipts[0].scores[1]);
  assert.equal(ledger.history('Mouse A').length,1);assert.equal(ledger.counts().games.cat.accounts,2);
  t.diagnostic('Survival tie at '+receipts[0].scores[0].timeMs+' simulated milliseconds. Both clients received the same authoritative result.');
});
