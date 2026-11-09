const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {webcrypto}=require('node:crypto');
const {gzipSync}=require('node:zlib');
const Cloud=require('../cloud-save.js');
const secret='a private journey password';

function game(){
  const context=vm.createContext({console});context.window=context;
  for(const name of ['catalog','legacy','progression','living-combat','synergies','provisions','engine'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),context);
  return context.GameEngine;
}
const E=game();
function bundle(){
  const profile=E.freshProfile(),camp=E.freshProfile();E.start(camp);
  E.act(camp,'equip',camp.run.pack.find(it=>it.id==='dewglass_vial').uid);
  E.act(camp,'combine',camp.run.active.find(it=>it.id==='stick').uid,camp.run.pack.find(it=>it.id==='stick').uid);
  return JSON.stringify({game:'bramblebound',format:1,profile,camp,settings:{audio:true,motion:false}});
}
function validate(raw){
  const data=JSON.parse(raw);assert.equal(data.game,'bramblebound');assert.equal(data.format,1);
  E.validate(data.profile);
  if(data.camp){const camp=E.validate(data.camp);assert.equal(camp.run.phase,'walk');assert.equal(camp.run.round,1);assert.equal(camp.run.time,0);}
  return data;
}
class Bytes{
  constructor(bytes){this.bytes=new Uint8Array(bytes);}
  toUint8Array(){return new Uint8Array(this.bytes);}
  static fromUint8Array(bytes){return new Bytes(bytes);}
}
function clone(value){
  if(value instanceof Bytes)return new Bytes(value.toUint8Array());
  if(value instanceof Uint8Array)return new Uint8Array(value);
  if(Array.isArray(value))return value.map(clone);
  if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,v])=>[key,clone(v)]));
  return value;
}
function fakeFirebase(){
  const docs=new Map(),state={writes:0,reads:0,transactions:0,cached:false,readError:null,writeError:null,writeDelayMs:0,beforeTransaction:null};
  const snap=ref=>({metadata:{fromCache:state.cached},exists:()=>docs.has(ref.path),data:()=>clone(docs.get(ref.path))});
  const firebase={db:{},auth:{currentUser:{uid:'anonymous-test-device'}},Bytes,
    doc:(_db,...parts)=>({path:parts.join('/'),id:parts.at(-1)}),
    serverTimestamp:()=>({serverTimestamp:true}),
    async getDocFromServer(ref){state.reads++;if(state.readError)throw state.readError;return snap(ref);},
    async runTransaction(_db,action){
      state.transactions++;
      if(state.beforeTransaction){const callback=state.beforeTransaction;state.beforeTransaction=null;await callback();}
      const pending=[];
      const result=await action({get:async ref=>snap(ref),set:(ref,data)=>pending.push([ref.path,clone(data)])});
      if(state.writeError)throw state.writeError;
      if(state.writeDelayMs)await new Promise(resolve=>setTimeout(resolve,state.writeDelayMs));
      for(const [id,data]of pending){docs.set(id,data);state.writes++;}
      return result;
    }
  };
  return {firebase,docs,state,record:()=>clone([...docs.values()][0]),key:()=>[...docs.keys()][0]};
}
const client=(backend,extra={})=>Cloud.create({validate,firebase:backend.firebase,online:()=>true,...extra});

test('named Firebase slots have a stable Bramblebound namespace and normalize names',async()=>{
  assert.equal(Cloud.normalizeName('  Forest   Pilgrim  '),'forest pilgrim');
  const first=await Cloud.documentId('Forest Pilgrim'),second=await Cloud.documentId(' forest  PILGRIM ');
  assert.equal(first,second);assert.match(first,/^bramblebound-save--[a-f0-9]{64}$/);assert.notEqual(first,await Cloud.documentId('Another journey'));
});

test('real encryption transfers complete validated profiles, prepared kits and settings',async()=>{
  const backend=fakeFirebase(),a=client(backend),raw=bundle();
  const saved=await a.save('Forest Pilgrim',secret,raw),record=backend.record();
  assert.equal(saved.revision,1);assert.equal(record.gameType,'bramblebound-cloud-save');assert.equal(record.kind,'brambleboundCloudSave');
  assert.equal(backend.key(),'lobbies/'+saved.docId);assert.equal(record.cipher,'AES-GCM-256');assert.equal(record.kdf,'PBKDF2-SHA256');assert.equal(record.iterations,210000);
  assert.equal(record.players.length,0);assert.equal(record.host,'bramblebound');
  assert.doesNotMatch(JSON.stringify(record),/private journey password|"password"|dewglass_vial|bramblebound\.profile/);
  assert.doesNotMatch(JSON.stringify(a.status()),/private journey password|"password"/);
  const reopened=await client(backend).load(' forest pilgrim ',secret);assert.equal(reopened.raw,raw);assert.deepEqual(JSON.parse(reopened.raw),JSON.parse(raw));assert.equal(reopened.revision,1);
  validate(reopened.raw);assert.equal(JSON.parse(reopened.raw).camp.run.active.length,3);
  const savedAgain=await a.save('Forest Pilgrim',secret,raw);assert.equal(savedAgain.revision,2);assert.equal(backend.state.writes,2);
  assert.notDeepEqual(backend.record().iv,record.iv,'each revision uses a fresh encryption nonce');
});

test('expanded Legacy, two Oaths, elemental ownership and live companion timers survive encrypted transfer',async()=>{
  const data=JSON.parse(bundle()),p=E.freshProfile();p.points=10000;p.best=64;p.deaths=4;
  for(const id of ['oath_worldroot','oath_phoenix','convergence','fate_rerolls','banishing_ink','expanded_destiny'])assert.equal(E.act(p,'legacyBuy',id).ok,true,id);
  for(const el of ['leaf','fire'])for(let i=0;i<3;i++)assert.equal(E.act(p,'legacyBuy','mastery_'+el).ok,true);
  assert.equal(E.act(p,'legacyBuy','ashroot').ok,true);
  p.legacy.activeOaths=['worldroot','phoenix'];E.start(p);E.tick(p,3.3);
  assert.ok(p.run.living.companions.length);data.profile=E.validate(p);
  const raw=JSON.stringify(data),backend=fakeFirebase();
  await client(backend).save('Living Legacy',secret,raw);
  const loaded=await client(backend).load('Living Legacy',secret),restored=E.validate(JSON.parse(loaded.raw).profile);
  assert.equal(JSON.stringify(restored),JSON.stringify(data.profile));
  assert.deepEqual(Array.from(restored.run.legacy.oaths),['worldroot','phoenix']);
  assert.equal(restored.run.draftRerolls,1);assert.equal(restored.run.banishes,1);
  assert.equal(restored.run.living.companions[0].timer,p.run.living.companions[0].timer);
});

test('encrypted transfer keeps spent supplies, upgraded potency and active set cooldowns without refilling',async()=>{
  const data=JSON.parse(bundle()),p=E.freshProfile();p.points=10000;p.best=64;p.deaths=2;
  for(const id of ['provision_pockets','provision_pockets','field_alchemy'])assert.equal(E.act(p,'legacyBuy',id).ok,true,id);
  E.start(p);const r=p.run;
  r.active=['torch','flake'].map((id,slot)=>({uid:r.nextUid++,id,slot,level:1,cd:1}));
  for(const it of r.active)if(!p.seenItems.includes(it.id))p.seenItems.push(it.id);
  r.walkLeft=0;E.tick(p,.001);r.enemies[0].chill=2;r.active[0].cd=0;r.active[1].cd=1;r.hp=r.maxHp*.5;
  assert.equal(E.act(p,'useConsumable','moss_tonic').ok,true);const events=E.tick(p,.01);
  assert.ok(events.some(v=>v.kind==='synergy'&&v.pattern==='emberglass'));
  assert.ok(r.synergies.timers.emberglass>0);assert.ok(r.provisions.cooldown>0);assert.equal(r.provisions.used,1);assert.equal(r.provisions.counts.moss_tonic,0);
  const original=E.validate(p);data.profile=original;const backend=fakeFirebase(),raw=JSON.stringify(data);
  await client(backend).save('Sets and supplies',secret,raw);
  const loaded=await client(backend).load('Sets and supplies',secret),restored=E.validate(JSON.parse(loaded.raw).profile);
  assert.equal(JSON.stringify(restored.run.provisions),JSON.stringify(original.run.provisions));
  assert.equal(JSON.stringify(restored.run.synergies),JSON.stringify(original.run.synergies));
  assert.equal(restored.run.legacy.ranks.field_alchemy,1);assert.equal(restored.run.legacy.ranks.provision_pockets,2);
  const expected=E.tick(original,.3),actual=E.tick(restored,.3);
  assert.equal(JSON.stringify(actual),JSON.stringify(expected));assert.equal(JSON.stringify(restored),JSON.stringify(original));
});

test('wrong passwords and corrupted ciphertext never write or return a partial profile',async()=>{
  const backend=fakeFirebase(),a=client(backend);await a.save('Locked journey',secret,bundle());const before=backend.record(),writes=backend.state.writes;
  await assert.rejects(()=>client(backend).load('Locked journey','a different password'),/password|decrypt|damaged|incorrect/i);
  await assert.rejects(()=>client(backend).save('Locked journey','a different password',bundle()),/password|decrypt|damaged|incorrect/i);
  assert.equal(backend.state.writes,writes);assert.deepEqual(backend.record(),before);
  const bytes=before.payload.toUint8Array();bytes[0]^=0xff;before.payload=Bytes.fromUint8Array(bytes);backend.docs.set(backend.key(),before);
  await assert.rejects(()=>client(backend).load('Locked journey',secret),/password|decrypt|damaged|incorrect/i);assert.equal(backend.state.writes,writes);
});

test('reopening is required before an existing slot can be overwritten, including after forgetting',async()=>{
  const backend=fakeFirebase(),a=client(backend),raw=bundle();await a.save('Existing slot',secret,raw);const b=client(backend);
  await assert.rejects(()=>b.save('Existing slot',secret,raw),/reopen|open|load|session/i);assert.equal(backend.state.writes,1);
  await b.load('Existing slot',secret);await b.save('Existing slot',secret,raw);assert.equal(backend.state.writes,2);
  b.forget();await assert.rejects(()=>b.save('Existing slot',secret,raw),/reopen|open|load|session/i);assert.equal(backend.state.writes,2);
});

test('stale devices and a transaction race cannot replace a newer cloud revision',async()=>{
  const backend=fakeFirebase(),a=client(backend),b=client(backend),raw=bundle();await a.save('Shared road',secret,raw);await b.load('Shared road',secret);
  await a.save('Shared road',secret,raw);const current=backend.record();
  await assert.rejects(()=>b.save('Shared road',secret,raw),/changed|newer|another|reopen|conflict/i);assert.equal(backend.state.writes,2);assert.deepEqual(backend.record(),current);
  await b.load('Shared road',secret);
  backend.state.beforeTransaction=()=>{const newer=backend.record();newer.revision++;const iv=newer.iv.toUint8Array();iv[0]^=1;newer.iv=Bytes.fromUint8Array(iv);backend.docs.set(backend.key(),newer);};
  await assert.rejects(()=>b.save('Shared road',secret,raw),/changed|newer|another|reopen|conflict/i);assert.equal(backend.state.writes,2);
});

test('concurrent creation preserves the slot created by the other device',async()=>{
  const backend=fakeFirebase(),a=client(backend),raw=bundle(),otherBackend=fakeFirebase();
  await client(otherBackend).save('Creation race',secret,raw);const winner=otherBackend.record(),key=otherBackend.key();
  backend.state.beforeTransaction=()=>backend.docs.set(key,clone(winner));
  await assert.rejects(()=>a.save('Creation race',secret,raw),/changed|newer|another|reopen|conflict|exist/i);assert.equal(backend.state.writes,0);assert.deepEqual(backend.record(),winner);
});

test('offline and unavailable-server failures never masquerade as a successful cloud save',async()=>{
  const backend=fakeFirebase(),raw=bundle();
  await assert.rejects(()=>client(backend,{online:()=>false}).save('Offline slot',secret,raw),/offline|connection|network/i);assert.equal(backend.state.reads,0);assert.equal(backend.state.writes,0);
  backend.state.readError=new Error('Server unavailable');await assert.rejects(()=>client(backend).load('Offline slot',secret),/unavailable/i);assert.equal(backend.state.writes,0);
  backend.state.readError=null;backend.state.writeError=new Error('Write acknowledgement unavailable');const a=client(backend);
  await assert.rejects(()=>a.save('Uncertain slot',secret,raw),/unavailable/i);assert.equal(backend.state.writes,0);
  backend.state.writeError=null;await assert.rejects(()=>a.save('Uncertain slot',secret,raw),/reopen|uncertain|confirm|reconcile/i);assert.equal(backend.state.writes,0);
});

test('server-confirmed absence reconciles an unconfirmed new slot so it can be retried',async()=>{
  const backend=fakeFirebase(),a=client(backend),raw=bundle();backend.state.writeError=new Error('Acknowledgement lost');
  await assert.rejects(()=>a.save('Retry new slot',secret,raw),/lost/i);assert.equal(a.status().uncertain,true);
  backend.state.writeError=null;await assert.rejects(()=>a.load('Retry new slot',secret),{code:'not-found'});
  assert.equal(a.status().uncertain,false);await a.save('Retry new slot',secret,raw);assert.equal(backend.state.writes,1);
});

test('cached snapshots cannot claim a successful server restore or create',async()=>{
  const backend=fakeFirebase(),a=client(backend);backend.state.cached=true;
  await assert.rejects(()=>a.save('Cache is not cloud',secret,bundle()),/server|offline|connect/i);assert.equal(backend.state.writes,0);
  backend.state.cached=false;await a.save('Cache is not cloud',secret,bundle());backend.state.cached=true;
  await assert.rejects(()=>client(backend).load('Cache is not cloud',secret),/server|offline|connect/i);assert.equal(backend.state.writes,1);
});

test('untrusted slot-name metadata cannot redirect the restored cloud binding',async()=>{
  const backend=fakeFirebase(),a=client(backend);await a.save('Original slot',secret,bundle());
  const record=backend.record();record.saveName='Different slot';backend.docs.set(backend.key(),record);
  await assert.rejects(()=>client(backend).load('Original slot',secret),/name|match|invalid|malformed|incompatible/i);assert.equal(backend.state.writes,1);
});

test('invalid profile uploads, malformed metadata and another game never mutate the cloud',async()=>{
  const backend=fakeFirebase(),a=client(backend),invalid=JSON.parse(bundle());invalid.profile.points=-1;
  await assert.rejects(()=>a.save('Bad profile',secret,JSON.stringify(invalid)));assert.equal(backend.state.writes,0);
  await a.save('Valid profile',secret,bundle());const key=backend.key(),record=backend.record(),writes=backend.state.writes;
  for(const change of [{gameType:'arcane-wilds-cloud-save'},{format:999},{cipher:'none'},{kdf:'none'},{iterations:2000000000},{iv:new Bytes([1,2])},{salt:new Bytes([1,2])}]){
    backend.docs.set(key,{...clone(record),...change});await assert.rejects(()=>client(backend).load('Valid profile',secret));assert.equal(backend.state.writes,writes);
  }
  backend.docs.set(key,{...clone(record),gameType:'arcane-wilds-cloud-save'});await assert.rejects(()=>client(backend).save('Valid profile',secret,bundle()),/reserved|another|belong|type|game|supported/i);assert.equal(backend.state.writes,writes);
});

test('a late write acknowledgement requires server reconciliation before another save',async()=>{
  const backend=fakeFirebase(),a=client(backend,{timeoutMs:10}),raw=bundle();backend.state.writeDelayMs=60;
  await assert.rejects(()=>a.save('Late acknowledgement',secret,raw),/timeout|timed out|confirm|reopen/i);
  assert.equal(a.status().uncertain,true);
  await assert.rejects(()=>a.save('Late acknowledgement',secret,raw),/reopen|uncertain|confirm|reconcile/i);
  await new Promise(resolve=>setTimeout(resolve,80));assert.equal(backend.state.writes,1);
  backend.state.writeDelayMs=0;const restored=await a.load('Late acknowledgement',secret);assert.equal(restored.raw,raw);assert.equal(a.status().uncertain,false);
  await a.save('Late acknowledgement',secret,raw);assert.equal(backend.state.writes,2);
});

test('overlapping actions cannot save and restore concurrently',async()=>{
  const backend=fakeFirebase(),a=client(backend),pending=a.save('Busy journey',secret,bundle());
  await assert.rejects(()=>a.load('Busy journey',secret),/busy|pending|progress|finish/i);
  await pending;assert.equal(backend.state.writes,1);
});

test('decrypted but invalid journeys are rejected before any local restoration can happen',async()=>{
  const backend=fakeFirebase(),a=client(backend);await a.save('Damaged journey',secret,bundle());
  const invalid=JSON.parse(bundle());invalid.profile.points=-5;
  const envelope=await Cloud.encrypt(JSON.stringify(invalid),secret),record=backend.record();backend.docs.set(backend.key(),{...record,...envelope});
  await assert.rejects(()=>client(backend).load('Damaged journey',secret));assert.equal(backend.state.writes,1);
});

test('payload, decompression and KDF bounds reject oversized or hostile cloud envelopes',{timeout:5000},async()=>{
  const limits=Cloud.constants;
  await assert.rejects(()=>Cloud.encrypt('x'.repeat(limits.MAX_PLAIN_BYTES+1),secret),/large|limit|size/i);
  const envelope=await Cloud.encrypt(bundle(),secret);
  await assert.rejects(()=>Cloud.decrypt({...envelope,iterations:limits.MAX_ITERATIONS+1},secret),/iterations|format|invalid|malformed|unsupported|limit/i);
  await assert.rejects(()=>Cloud.decrypt({...envelope,payload:new Uint8Array(limits.MAX_ENCRYPTED_BYTES+1)},secret),/large|limit|size|malformed|invalid/i);
  const salt=envelope.salt,iv=envelope.iv;
  const material=await webcrypto.subtle.importKey('raw',new TextEncoder().encode(secret),'PBKDF2',false,['deriveKey']);
  const key=await webcrypto.subtle.deriveKey({name:'PBKDF2',salt,iterations:envelope.iterations,hash:'SHA-256'},material,{name:'AES-GCM',length:256},false,['encrypt']);
  const bomb=gzipSync(Buffer.alloc(limits.MAX_PLAIN_BYTES+1,120));
  const payload=new Uint8Array(await webcrypto.subtle.encrypt({name:'AES-GCM',iv},key,bomb));
  await assert.rejects(()=>Cloud.decrypt({...envelope,compression:'gzip',payload},secret),/large|limit|size/i);
});

test('crypto works without optional compression support and malformed credentials never touch Firebase',async()=>{
  const backend=fakeFirebase(),a=client(backend);
  for(const [name,password]of [['a',secret],['valid','abc'],['valid','x'.repeat(1025)]])await assert.rejects(()=>a.save(name,password,bundle()));
  assert.equal(backend.state.reads,0);assert.equal(backend.state.writes,0);
  const compression=globalThis.CompressionStream;
  try{
    globalThis.CompressionStream=undefined;
    const raw=bundle(),envelope=await Cloud.encrypt(raw,secret);assert.equal(envelope.compression,'none');assert.equal(await Cloud.decrypt(envelope,secret),raw);
  }finally{globalThis.CompressionStream=compression;}
});
