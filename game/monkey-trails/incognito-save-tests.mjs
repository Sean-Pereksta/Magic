import test from 'node:test';
import assert from 'node:assert/strict';
import {Vault,unseal} from './storage.mjs';

const name='Shared trail',password='a-unique-shared-password';
class DeviceStorage {
  rows=new Map();
  getItem(key){return this.rows.get(key)||null;}
  setItem(key,value){this.rows.set(key,value);}
}
function sharedCloud(){
  const docs=new Map();let online=true,reads=0,commits=0;
  return {docs,get reads(){return reads;},get commits(){return commits;},set online(value){online=value;},
    async read(id){reads++;if(!online)throw Error('offline');return structuredClone(docs.get(id)||null);},
    async commit(id,base,envelope){
      if(!online)throw Error('offline');
      const previous=docs.get(id);
      if((previous?.revision||0)!==base||previous&&previous.salt!==envelope.salt){const error=Error('Another session changed the cloud checkpoint.');error.code='save-conflict';throw error;}
      commits++;docs.set(id,{...structuredClone(envelope),revision:base+1});return base+1;
    }
  };
}
async function synced(vault){
  for(let attempt=0;attempt<6;attempt++){
    if(vault.localTask)await vault.localTask;if(vault.inflight)await vault.inflight;
    await vault.sync();await Promise.resolve();
    if(!vault.dirty)return;
  }
  assert.fail('The test checkpoint did not receive cloud acknowledgement.');
}
const stock=state=>{Object.keys(state.run.essence).forEach((type,index)=>{state.run.essence[type]=19+index*11;});return structuredClone(state.run.essence);};
async function fixture(){
  const cloud=sharedCloud(),normalStorage=new DeviceStorage(),normal=new Vault(()=>{},normalStorage,async()=>cloud);
  const state=await normal.open(name,password,{create:true});await synced(normal);
  return {cloud,normalStorage,normal,state};
}

test('a failed latest encryption never uploads the previous empty ciphertext as current progress',async()=>{
  const f=await fixture(),expected=stock(f.state),key=f.normal.key,commits=f.cloud.commits;
  f.normal.key=null;
  await f.normal.checkpoint(f.state,true);await f.normal.sync();
  assert.equal(await f.normal.flushCloud({timeoutMs:30}),false);
  assert.equal(f.cloud.commits,commits,'failed encryption cannot publish or acknowledge an older payload');
  assert.equal(f.normal.dirty,true);assert.notEqual(f.normal.encryptedSeq,f.normal.seq);
  f.normal.key=key;await f.normal.checkpoint(f.state,true);
  assert.equal(await f.normal.flushCloud(),true);
  const incognito=new Vault(()=>{},new DeviceStorage(),async()=>f.cloud);
  assert.deepEqual((await incognito.open(name,password)).run.essence,expected);
});

test('a fresh private context reopens every acknowledged essence balance from the encrypted cloud checkpoint',async()=>{
  const f=await fixture(),expected=stock(f.state);await f.normal.checkpoint(f.state,true);await synced(f.normal);
  const normalRows=[...f.normalStorage.rows],privateStorage=new DeviceStorage(),privateVault=new Vault(()=>{},privateStorage,async()=>f.cloud),commits=f.cloud.commits;
  assert.equal(privateStorage.rows.size,0,'private context has no normal-browser device data');
  const restored=await privateVault.open(name,password);
  assert.deepEqual(restored.run.essence,expected);assert.equal(privateVault.dirty,false);assert.equal(privateVault.conflict,false);
  assert.equal(f.cloud.commits,commits,'opening the cloud copy does not write a replacement');
  assert.deepEqual([...f.normalStorage.rows],normalRows,'private reopening leaves normal-browser checkpoints intact');
  assert.deepEqual((await unseal(f.cloud.docs.get(f.normal.id),f.normal.key,f.normal.id)).run.essence,expected);
});

test('device-only progress is absent in a fresh private context until the normal browser syncs it',async()=>{
  const f=await fixture(),acknowledged=structuredClone(f.state.run.essence);f.cloud.online=false;
  const expected=stock(f.state);await f.normal.checkpoint(f.state,true);await f.normal.sync();assert.equal(f.normal.dirty,true);
  const deviceRows=[...f.normalStorage.rows];f.cloud.online=true;
  const privateStorage=new DeviceStorage(),privateVault=new Vault(()=>{},privateStorage,async()=>f.cloud),commits=f.cloud.commits;
  const cloudOnly=await privateVault.open(name,password);
  assert.deepEqual(cloudOnly.run.essence,acknowledged,'a new context can only know the last acknowledged cloud copy');
  assert.deepEqual(await privateVault.recoveryCopies(),[],'normal-browser backups are not shared into a private context');
  assert.equal(f.cloud.commits,commits);assert.deepEqual([...f.normalStorage.rows],deviceRows);
  await synced(f.normal);
  const afterSync=await new Vault(()=>{},new DeviceStorage(),async()=>f.cloud).open(name,password);
  assert.deepEqual(afterSync.run.essence,expected,'explicit sync publishes the exact previously device-only inventory');
});

test('a stale private session cannot replace newer cloud essence with its older zero balances',async()=>{
  const f=await fixture(),privateStorage=new DeviceStorage(),privateVault=new Vault(()=>{},privateStorage,async()=>f.cloud);
  const stale=await privateVault.open(name,password);assert.ok(Object.values(stale.run.essence).every(amount=>amount===0));
  const expected=stock(f.state);await f.normal.checkpoint(f.state,true);await synced(f.normal);
  const cloudBefore=structuredClone(f.cloud.docs.get(f.normal.id));
  stale.run.fruit++;await privateVault.checkpoint(stale,true);await privateVault.sync();
  assert.equal(privateVault.conflict,true);assert.equal(privateVault.dirty,true);
  assert.deepEqual(f.cloud.docs.get(f.normal.id),cloudBefore,'revision conflict refuses the stale replacement atomically');
  assert.deepEqual((await new Vault(()=>{},new DeviceStorage(),async()=>f.cloud).open(name,password)).run.essence,expected);
});

test('private practice storage is separate from the named GPS cloud account',async()=>{
  const f=await fixture(),expected=stock(f.state);await f.normal.checkpoint(f.state,true);await synced(f.normal);
  const storage=new DeviceStorage();let connections=0;
  const practice=new Vault(()=>{},storage,async()=>{connections++;throw Error('Practice must not connect.');});
  await assert.rejects(practice.open(name,password,{mode:'practice'}),/No save found/);
  assert.equal(storage.rows.size,0);
  const separate=await practice.open(name,password,{create:true,mode:'practice'});
  assert.notEqual(practice.id,f.normal.id);assert.ok(Object.values(separate.run.essence).every(amount=>amount===0));
  assert.equal(connections,0);
  assert.deepEqual((await unseal(f.cloud.docs.get(f.normal.id),f.normal.key,f.normal.id)).run.essence,expected);
});

test('an empty private context cannot silently start a zero save when the cloud copy is unavailable',async()=>{
  const f=await fixture();f.cloud.online=false;const storage=new DeviceStorage(),vault=new Vault(()=>{},storage,async()=>f.cloud),commits=f.cloud.commits;
  await assert.rejects(vault.open(name,password),/Cloud unavailable and no device copy found/);
  assert.equal(storage.rows.size,0);assert.equal(f.cloud.commits,commits);assert.equal(vault.snapshot,undefined);
});

function gatedCloud(cloud){
  let begin,release,active=0,maximum=0,calls=0;
  const started=new Promise(resolve=>{begin=resolve;}),gate=new Promise(resolve=>{release=resolve;});
  return {started,release:()=>release(),get calls(){return calls;},get maximum(){return maximum;},
    read:id=>cloud.read(id),async commit(...args){
      calls++;active++;maximum=Math.max(maximum,active);
      try{if(calls===1){begin();await gate;}return await cloud.commit(...args);}finally{active--;}
    }
  };
}

test('a deliberate cloud flush drains an older zero write before acknowledging the newest inventory for a private context',async()=>{
  const f=await fixture(),gate=gatedCloud(f.cloud);f.normal.adapter=gate;
  await f.normal.checkpoint(f.state,true);await gate.started;
  const expected=stock(f.state);await f.normal.checkpoint(f.state,true);
  let finished=false;const flushing=f.normal.flushCloud().then(result=>{finished=true;return result;});
  await Promise.resolve();assert.equal(finished,false,'flush cannot acknowledge only the older write');
  gate.release();assert.equal(await flushing,true);assert.equal(gate.calls,2);assert.equal(gate.maximum,1);
  assert.equal(f.normal.dirty,false);f.normal.retire();
  const privateVault=new Vault(()=>{},new DeviceStorage(),async()=>f.cloud),restored=await privateVault.open(name,password);
  assert.deepEqual(restored.run.essence,expected);assert.equal(privateVault.conflict,false);
});

test('a cloud flush timeout leaves the transaction singular and the newer exact device inventory available',async()=>{
  const f=await fixture(),gate=gatedCloud(f.cloud);f.normal.adapter=gate;
  await f.normal.checkpoint(f.state,true);await gate.started;
  const expected=stock(f.state);await f.normal.checkpoint(f.state,true);
  const started=Date.now();assert.equal(await f.normal.flushCloud({timeoutMs:20}),false);assert.ok(Date.now()-started<1000);
  assert.equal(gate.calls,1);assert.equal(gate.maximum,1);assert.equal(f.normal.dirty,true);
  const row=JSON.parse(f.normalStorage.getItem('monkey-trails.v1.'+f.normal.id));
  assert.deepEqual((await unseal(row,f.normal.key,f.normal.id)).run.essence,expected);
  f.normal.retire();gate.release();if(f.normal.inflight)await f.normal.inflight;
  assert.equal(gate.calls,1,'timeout and retirement cannot spawn a second transaction');
  assert.deepEqual((await unseal(JSON.parse(f.normalStorage.getItem('monkey-trails.v1.'+f.normal.id)),f.normal.key,f.normal.id)).run.essence,expected);
});

test('offline, conflicted and practice flushes return without retry loops or changing device balances',async()=>{
  const f=await fixture();f.cloud.online=false;const expected=stock(f.state);
  await f.normal.checkpoint(f.state,true);if(f.normal.inflight)await f.normal.inflight;
  let attempts=0;f.normal.adapter={read:id=>f.cloud.read(id),async commit(){attempts++;throw Error('offline');}};
  const started=Date.now();assert.equal(await f.normal.flushCloud(),false);assert.ok(Date.now()-started<1000);assert.equal(attempts,1);assert.equal(f.normal.dirty,true);
  assert.deepEqual(f.normal.snapshot.run.essence,expected);
  f.normal.conflict=true;assert.equal(await f.normal.flushCloud(),false);assert.equal(attempts,1);
  const practice=new Vault(()=>{},new DeviceStorage(),async()=>{throw Error('must not connect');});
  await practice.open(name,password,{create:true,mode:'practice'});assert.equal(await practice.flushCloud(),false);
});
