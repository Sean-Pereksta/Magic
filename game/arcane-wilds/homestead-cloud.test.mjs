import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';
import {webcrypto} from 'node:crypto';
const SERVER=1800000000000;
function fixture(){
 const h=runtime();Object.defineProperty(h.w.crypto,'subtle',{value:webcrypto.subtle});h.run(`startNewGame();AWCampaign.enter(AWHome.HOME);game.homestead.deed=true;game.homestead.tier=1;game.gold=10000;game.materials.timber=100;game.materials.fiber=100;AWHome.rebuild();`);
 const records=new Map(),fake={time:SERVER,writes:0,reads:0,race:false,failAfterCommit:false},stamp=()=>({toMillis:()=>fake.time});
 const snapshot=key=>({exists:()=>records.has(key),data:()=>records.get(key)});
 const resolve=record=>Object.fromEntries(Object.entries(record).map(([k,v])=>[k,v?.serverStamp?stamp():v]));
 const api={db:{},auth:{currentUser:{uid:'test-device'}},doc:(_db,...path)=>path.join('/'),Bytes:{fromUint8Array:v=>v},serverTimestamp:()=>({serverStamp:true}),
  async setDoc(key,value){fake.writes++;records.set(key,resolve(value));},
  async getDocFromServer(key){fake.reads++;return snapshot(key);},async getDoc(key){return snapshot(key);},
  async runTransaction(_db,fn){let write;const tx={get:async key=>{if(fake.race){fake.race=false;records.set(key,{...records.get(key),iv:[250]});}return snapshot(key);},set:(key,value)=>{write=[key,resolve(value)];}};await fn(tx);if(write){records.set(...write);fake.writes++;}if(fake.failAfterCommit){fake.failAfterCommit=false;throw new Error('Lost acknowledgement');}}
 };
 h.w.fakeFirebase=api;h.w.fixtureEncoder=new TextEncoder();h.w.fixtureDecoder=new TextDecoder();
 h.run(`awCloudFirebase=async()=>window.fakeFirebase;var encryptionSerial=0;
   awCloudEncrypt=async raw=>({salt:new Uint8Array([1]),iv:new Uint8Array([++encryptionSerial]),payload:window.fixtureEncoder.encode(raw),compression:'none',iterations:1});
   awCloudDecrypt=async record=>window.fixtureDecoder.decode(record.payload);`);
 fake.h=h;fake.records=records;fake.save=()=>h.run(`awCloudSaveNamed('Hearth test','password')`);
 fake.record=()=>[...records].find(([key])=>!key.includes('/homeClocks/'))?.[1];
 return fake;
}
test('official Firebase bundle retains house, planting/watering clocks and one shared wallet',async()=>{
 const f=fixture(),h=f.h;try{
  await f.save();assert.equal(h.run('AWHome.state().mode'),'cloud');
  assert.equal(await h.run(`AWHome.action('bed',{x:11,y:2})`),true);h.run(`AWHome.state().seeds.lanternberry=1`);
  assert.equal(await h.run(`AWHome.action('plant',{id:AWHome.state().plots[0].id,crop:'lanternberry'})`),true);
  assert.equal(await h.run(`AWHome.action('water',{id:AWHome.state().plots[0].id})`),true);
  const raw=JSON.parse(new TextDecoder().decode(f.record().payload)),base=JSON.parse(raw.base),expansion=JSON.parse(raw.expansion),p=base.homestead.plots[0].plant;
  assert.equal(p.plantedAt,SERVER);assert.equal(p.lastWateredAt,SERVER);assert.equal(p.wateredUntil,SERVER+8*3600000);assert.equal(base.gold,h.run('game.gold'));assert.equal(expansion.materials.timber,h.run('game.materials.timber'));
  const writes=f.writes;h.step(120);assert.equal(f.writes,writes,'rendering never writes crop timer ticks');
 }finally{h.close();}
});
test('cloud actions fail closed offline, without consuming seeds, resources or water',async()=>{
 const f=fixture(),h=f.h;try{await f.save();Object.defineProperty(h.w.navigator,'onLine',{value:false,configurable:true});const before=h.run('JSON.stringify([AWHome.state(),game.gold,game.materials])');
  assert.equal(await h.run(`AWHome.action('bed',{x:11,y:2})`),false);assert.equal(h.run('JSON.stringify([AWHome.state(),game.gold,game.materials])'),before);
 }finally{h.close();}
});
test('two-device revision race rejects the losing construction without local deductions',async()=>{
 const f=fixture(),h=f.h;try{await f.save();const before=h.run('JSON.stringify([AWHome.state(),game.gold,game.materials])');f.race=true;
  assert.equal(await h.run(`AWHome.action('bed',{x:11,y:2})`),false);assert.equal(h.run('JSON.stringify([AWHome.state(),game.gold,game.materials])'),before);
  assert.match(h.run('AWHomeCloud.status()'),/reconcile/);
 }finally{h.close();}
});
test('lost acknowledgement blocks retry until cloud reopen reconciles exactly one result',async()=>{
 const f=fixture(),h=f.h;try{await f.save();const timber=h.run('game.materials.timber');f.failAfterCommit=true;
  assert.equal(await h.run(`AWHome.action('bed',{x:11,y:2})`),false);assert.equal(h.run('AWHome.state().plots.length'),0);
  assert.equal(await h.run(`AWHome.action('bed',{x:11,y:2})`),false);
  await h.run(`awCloudLoadNamed('Hearth test','password')`);assert.equal(h.run('AWHome.state().plots.length'),1);assert.equal(h.run('game.materials.timber'),timber-3);
  assert.equal(await h.run(`AWHome.action('bed',{x:11,y:2})`),false);assert.equal(h.run('game.materials.timber'),timber-3);
 }finally{h.close();}
});
test('a later device clock change cannot accelerate confirmed cloud crop time',async()=>{
 const f=fixture(),h=f.h;try{await f.save();await h.run(`AWHome.action('bed',{x:11,y:2})`);h.run(`AWHome.state().seeds.stormgrape=1;game.campaign.unlocked.push('meridian')`);await h.run(`AWHome.action('plant',{id:AWHome.state().plots[0].id,crop:'stormgrape'})`);await h.run(`AWHome.action('water',{id:AWHome.state().plots[0].id})`);
  h.run(`Date.now=()=>9999999999999`);f.time+=10*3600000;await h.run(`AWHome.action('water',{id:AWHome.state().plots[0].id})`);
  assert.equal(h.run('AWHome.state().plots[0].plant.growthMs'),8*3600000);assert.equal(h.run('AWHome.state().plots[0].plant.lastWateredAt'),SERVER+10*3600000);
 }finally{h.close();}
});
test('a stale official save is rejected even when the older client changed only the ciphertext IV',async()=>{
 const f=fixture(),h=f.h;try{await f.save();const [key,record]=[...f.records].find(([key])=>!key.includes('/homeClocks/'));f.records.set(key,{...record,iv:[222]});
  await assert.rejects(f.save(),/changed|reopened/);assert.equal(f.records.get(key).iv[0],222);
 }finally{h.close();}
});
