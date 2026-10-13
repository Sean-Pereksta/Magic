import test from 'node:test';
import assert from 'node:assert/strict';
import { publicForgeLobby,createMatchmaking,estimatedOpponent } from '../matchmaking.js';

test('only actual open public Faction Forge seats appear in search',()=>{
  const good={gameType:'crowncouncil',status:'waiting',visibility:'public',host:'Commander',players:['Commander']};
  assert.equal(publicForgeLobby(good,'Visitor'),true);
  for(const patch of [{visibility:'private'},{inviteOnly:true},{password:'secret'},{status:'started'},{gameType:'turncraft'},{players:['Commander','Other']},{players:[]},{host:'Imposter'}])assert.equal(publicForgeLobby({...good,...patch},'Visitor'),false);
  assert.equal(publicForgeLobby(good,'Commander'),false);
});

test('a racing second join cannot overfill a two-player lobby',async()=>{
  let state={gameType:'crowncouncil',status:'waiting',visibility:'public',host:'Host',hostUid:'Host-uid',members:{'Host-uid':'Host'},players:['Host']};
  let queue=Promise.resolve(),joined=[];
  const api={doc:(_,c,id)=>({c,id}),runTransaction:(_,fn)=>{
    const task=queue.then(()=>fn({get:async()=>({exists:()=>true,data:()=>structuredClone(state)}),update:(_,patch)=>{state={...state,...patch}}}));
    queue=task.catch(()=>{});return task;
  }};
  const make=username=>createMatchmaking({db:{},auth:{currentUser:{uid:username+'-uid'}},api,username,onJoined:id=>joined.push(id)});
  const results=await Promise.allSettled([make('One').join('real-lobby'),make('Two').join('real-lobby')]);
  assert.equal(results.filter(r=>r.status==='fulfilled').length,1);
  assert.equal(state.players.length,2);assert.equal(joined.length,1);
  assert.equal(Object.keys(state.members).length,2);
});

test('rapid duplicate joins issue only one transaction',async()=>{
  let calls=0,release;const gate=new Promise(r=>release=r);
  const service=createMatchmaking({db:{},username:'One',auth:{currentUser:{uid:'uid'}},api:{doc:()=>({}),runTransaction:async()=>{calls++;await gate;}}});
  const first=service.join('x');assert.equal(await service.join('x'),false);release();assert.equal(await first,true);assert.equal(calls,1);
});

test('AI fallback is explicitly estimated and never creates a player document',()=>{
  const ai=estimatedOpponent(1200,.5,()=>.2);assert.equal(ai.ai,true);assert.equal(ai.name,'AshenWolf');assert.ok(ai.estimatedMmr>=1100&&ai.estimatedMmr<=1300);assert.equal(ai.uid,undefined);
});
