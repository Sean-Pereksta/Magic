import test from 'node:test';
import assert from 'node:assert/strict';
import {WebSocket} from 'ws';
import {createTrashTacticsServer} from '../server.mjs';
import {Game} from '../engine.js';
import {executeOrder} from '../online-rules.mjs';
import {snapshotFor} from '../online-snapshot.mjs';
import {quickTunnelOrigin} from '../lobby-directory.mjs';

function peer(origin, request) {
  const socket=new WebSocket(origin.replace('http:','ws:')+'/api/play');
  const messages=[];
  socket.on('message',payload=>{
    const message=JSON.parse(payload);
    messages.push(message);
    if (message.type==='hello') send(request);
  });
  const send=message=>socket.send(JSON.stringify(message));
  async function wait(type,accept=()=>true) {
    const timeout=Date.now()+5000;
    while (Date.now()<timeout) {
      const index=messages.findIndex(item=>item.type===type&&accept(item));
      if (index!==-1) return messages.splice(index,1)[0];
      await new Promise(resolve=>setTimeout(resolve,10));
    }
    throw new Error('Timed out waiting for '+type);
  }
  return {socket,send,wait};
}

test('both seats load, issue server-validated orders, resume their seat and finish by forfeit',async()=>{
  const app=createTrashTacticsServer();
  await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));
  const origin='http://127.0.0.1:'+app.server.address().port;
  try {
    const owner=peer(origin,{type:'create',name:'Owner',map:'rainyard'});
    const host=await owner.wait('seat');
    assert.equal(host.team,0);
    const entries=await (await fetch(origin+'/api/lobbies')).json();
    assert.equal(entries.rooms[0].id,host.room);
    const challenger=peer(origin,{type:'join',room:host.room,name:'Guest'});
    const guest=await challenger.wait('seat');
    assert.equal(guest.team,1);
    owner.send({type:'start'});
    await owner.wait('load');await challenger.wait('load');
    owner.send({type:'ready'});
    await owner.wait('room',room=>room.seats[0]?.ready && !room.seats[1]?.ready);
    challenger.send({type:'ready'});
    await owner.wait('start');await challenger.wait('start');
    const view=await owner.wait('state');
    const other=await challenger.wait('state');
    const ownDesk=view.entities.find(e=>e.type==='desk' && e.team===0);
    const guestDesk=other.entities.find(e=>e.type==='desk' && e.team===1);
    assert.equal(view.entities.some(e=>e.team===1),false);
    assert.equal(other.entities.some(e=>e.team===0),false);
    assert.equal(view.teams[1].trash,undefined);
    owner.send({type:'order',seq:1,method:'train',args:[guestDesk.id,'twig']});
    assert.equal((await owner.wait('ack',m=>m.seq===1)).ok,false);
    owner.send({type:'order',seq:2,method:'train',args:[ownDesk.id,'twig']});
    assert.equal((await owner.wait('ack',m=>m.seq===2)).ok,true);
    await owner.wait('state',s=>s.entities.find(e=>e.id===ownDesk.id)?.queue[0]?.type==='twig');
    challenger.socket.close();
    await owner.wait('room',r=>r.seats[1]?.connected===false);
    const resumed=peer(origin,{type:'resume',room:guest.room,session:guest.session});
    assert.equal((await resumed.wait('seat')).team,1);
    await resumed.wait('start');
    resumed.send({type:'leave'});
    const result=await owner.wait('result');
    assert.equal(result.winner,0);
    assert.equal(result.reason,'forfeit');
    assert.equal(result.ratingStatus,'casual');
  } finally {await app.close();}
});

test('orders cannot target unseen entities and visible enemy production stays private',()=>{
  const game=new Game({online:true});
  const rock=game.entities.find(e=>e.type==='rock' && e.team===0);
  const desk=game.entities.find(e=>e.type==='desk' && e.team===1);
  assert.equal(executeOrder(game,0,{method:'command',args:[[rock.id],{type:'attack',targetId:desk.id}]}).ok,false);
  assert.equal(game.train(desk.id,'twig').ok,true);
  game.isVisible=()=>true;
  const enemy=snapshotFor(game,{team:0}).entities.find(e=>e.id===desk.id);
  assert.equal(enemy.queue.length,1);
  assert.equal(enemy.queue[0].type,undefined);
  assert.equal(enemy.queue[0].remaining,4);
});

test('managed host seats exactly two guests, rejects manual creation and reopens after a match',async()=>{
  const app=createTrashTacticsServer({community:true,managed:true});
  await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));
  const origin='http://127.0.0.1:'+app.server.address().port;
  try {
    const status=()=>app.getStatus();
    assert.deepEqual(status(),{phase:'waiting',players:0,capacity:2,queued:0});
    const bypass=peer(origin,{type:'create',map:'rainyard'});
    assert.match((await bypass.wait('error')).message,/managed server/);
    const host=peer(origin,{type:'managedJoin',name:'Birch'});
    const first=await host.wait('seat');
    assert.equal(first.team,0);
    const other=peer(origin,{type:'managedJoin',name:'Pine'});
    const second=await other.wait('seat');
    assert.equal(second.room,first.room);
    assert.equal(second.team,1);
    assert.equal(status().players,2);
    const full=peer(origin,{type:'managedJoin'});
    assert.match((await full.wait('error')).message,/open seat/);
    host.send({type:'start'});
    await host.wait('load'); await other.wait('load');
    host.send({type:'ready'}); other.send({type:'ready'});
    await host.wait('start');
    assert.equal(status().phase,'playing');
    other.send({type:'leave'});
    assert.equal((await host.wait('result')).reason,'forfeit');
    assert.equal(status().players,0);
    const next=peer(origin,{type:'managedJoin',name:'Maple'});
    const nextSeat=await next.wait('seat');
    assert.equal(nextSeat.team,0);
    assert.notEqual(nextSeat.room,first.room);
    next.send({type:'leave'});
    await next.wait('closed');
    assert.equal(status().players,0);
  } finally {await app.close();}
});

test('community listings accept exact HTTPS Quick Tunnel origins',()=>{
  assert.equal(quickTunnelOrigin('https://rust-paper-bridge.trycloudflare.com/'),'https://rust-paper-bridge.trycloudflare.com');
  for (const url of [
    'https://trycloudflare.com','https://host.trycloudflare.com/path','http://host.trycloudflare.com',
    'https://host.trycloudflare.com:8080','https://host.trycloudflare.com.evil.example',
    'https://name:secret@host.trycloudflare.com','https://host.trycloudflare.com#fragment'
  ]) assert.equal(quickTunnelOrigin(url),null);
});
