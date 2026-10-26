import test from 'node:test';
import assert from 'node:assert/strict';
import {Vault,identity,seal,derive} from './storage.mjs';
import {RememberedAccount,IndexedDBAccountStore,validRememberedAccount} from './remembered-account.mjs';

class DeviceStorage {
  constructor(){this.rows=new Map();}
  getItem(key){return this.rows.get(key)||null;}
  setItem(key,value){this.rows.set(key,value);}
  removeItem(key){this.rows.delete(key);}
}
class AccountStorage {
  async read(){return this.row?structuredClone(this.row):undefined;}
  async write(row){this.row=structuredClone(row);}
  async clear(){this.row=undefined;}
}
const offline=async()=>{throw Error('offline');};
async function fixture(name='Mossfoot'){
  const storage=new DeviceStorage(),accounts=new AccountStorage(),remembered=new RememberedAccount(accounts),vault=new Vault(()=>{},storage,offline);
  const state=await vault.open(name,'a-unique-test-password',{create:true});await vault.sync();
  const account=await remembered.remember(vault);
  return {storage,accounts,remembered,vault,state,account};
}

test('remember only persists the named account and a non-exportable key, never a password',async()=>{
  const f=await fixture();
  assert.deepEqual(Object.keys(f.accounts.row).sort(),['format','id','key','mode','name','salt']);
  assert.equal(f.account.name,'Mossfoot');assert.equal(f.account.key.extractable,false);assert.ok(validRememberedAccount(f.account));
  await assert.rejects(crypto.subtle.exportKey('raw',f.account.key));
  assert.ok(!JSON.stringify([...f.storage.rows]).includes('a-unique-test-password'));
  assert.ok(!JSON.stringify([...f.storage.rows]).includes('Mossfoot'));
  const reloaded=new RememberedAccount(f.accounts);assert.equal((await reloaded.load()).id,f.vault.id);
});

test('explicit reopen decrypts the offline device checkpoint and keeps later progress',async()=>{
  const f=await fixture();f.state.run.fruit+=31;await f.vault.checkpoint(f.state);await f.vault.sync();
  const v=new Vault(()=>{},f.storage,offline),restored=await v.reopen(await f.remembered.load());
  assert.equal(restored.run.fruit,f.state.run.fruit);assert.equal(restored.name,'Mossfoot');assert.equal(v.dirty,true);assert.equal(v.conflict,false);
});

test('remembered reopen keeps dirty device progress and blocks a conflicting cloud write',async()=>{
  const f=await fixture(),cloudState=structuredClone(f.state);f.state.run.fruit+=50;await f.vault.checkpoint(f.state);await f.vault.sync();
  const remote={...await seal(cloudState,f.vault.key,f.vault.salt,f.vault.id),revision:7};let commits=0;
  const v=new Vault(()=>{},f.storage,async()=>({read:async()=>remote,commit:async()=>{commits++;}}));
  const restored=await v.reopen(f.account);assert.equal(restored.run.fruit,f.state.run.fruit);assert.equal(v.conflict,true);
  await v.sync();assert.equal(commits,0);assert.equal(v.base,0);
});

test('remembered reopen chooses a newer cloud checkpoint when the local checkpoint is clean',async()=>{
  const f=await fixture(),deviceKey='monkey-trails.v1.'+f.vault.id,local=JSON.parse(f.storage.getItem(deviceKey));
  f.storage.setItem(deviceKey,JSON.stringify({...local,dirty:false,baseRevision:1}));
  const newer=structuredClone(f.state);newer.run.fruit+=70;
  const remote={...await seal(newer,f.vault.key,f.vault.salt,f.vault.id),revision:2};
  const v=new Vault(()=>{},f.storage,async()=>({read:async()=>remote})),restored=await v.reopen(f.account);
  assert.equal(restored.run.fruit,newer.run.fruit);assert.equal(v.base,2);assert.equal(v.dirty,false);assert.equal(v.conflict,false);
});

test('a changed cloud salt preserves dirty local progress and requires conflict recovery',async()=>{
  const f=await fixture(),salt=btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(16)))),key=await derive('different-password',salt);
  const remote={...await seal(f.state,key,salt,f.vault.id),revision:1};
  const v=new Vault(()=>{},f.storage,async()=>({read:async()=>remote}));
  assert.equal((await v.reopen(f.account)).name,'Mossfoot');assert.equal(v.conflict,true);
});

test('stale or mismatched remembered credentials do not overwrite a checkpoint',async()=>{
  const f=await fixture(),before=[...f.storage.rows];
  await assert.rejects(new Vault(()=>{},f.storage,offline).reopen({...f.account,salt:'wrong-salt'}),/save name and password/);
  await assert.rejects(new Vault(()=>{},f.storage,offline).reopen({...f.account,name:'Different name'}),/save name and password/);
  assert.deepEqual([...f.storage.rows],before);
});

test('forget removes quick access while password reopen and the encrypted checkpoint remain',async()=>{
  const f=await fixture(),before=[...f.storage.rows];await f.remembered.forget();
  assert.equal(await f.remembered.load(),null);assert.deepEqual([...f.storage.rows],before);
  assert.equal((await new Vault(()=>{},f.storage,offline).open('Mossfoot','a-unique-test-password')).name,'Mossfoot');
});

test('a second login replaces the remembered account without modifying the first save',async()=>{
  const f=await fixture(),first=await f.remembered.load(),second=new Vault(()=>{},f.storage,offline);
  await second.open('Leafwalker','another-unique-password',{create:true});await second.sync();await f.remembered.remember(second);
  assert.equal((await f.remembered.load()).name,'Leafwalker');assert.notEqual((await f.remembered.load()).id,first.id);
  assert.ok(f.storage.getItem('monkey-trails.v1.'+await identity('Mossfoot')));
});

test('invalid, practice, extractable, or unavailable key storage never grants quick access',async()=>{
  const f=await fixture();
  for(const row of [{...f.account,mode:'practice'},{...f.account,key:{...f.account.key,extractable:true}},{...f.account,key:null}]){
    f.accounts.row=row;assert.equal(await f.remembered.load(),null);
  }
  await assert.rejects(new RememberedAccount(new IndexedDBAccountStore(null)).load(),/cannot remember/);
  f.vault.localOK=false;await assert.rejects(f.remembered.remember(f.vault),/device checkpoint/);
  f.vault.localOK=true;const denied=new RememberedAccount({async write(){throw Error('quota');}});
  await assert.rejects(denied.remember(f.vault),/quota/);
  assert.equal((await new Vault(()=>{},f.storage,offline).open('Mossfoot','a-unique-test-password')).name,'Mossfoot');
});

function delayedCloud(){
  let release,started,remote=null,calls=0;
  const gate=new Promise(resolve=>{release=resolve;}),pending=new Promise(resolve=>{started=resolve;});
  const adapter={read:async()=>remote,async commit(_id,base,envelope){calls++;started();await gate;remote={...envelope,revision:base+1};return remote.revision;}};
  return {adapter,release:()=>release(),started:pending,get calls(){return calls;},get remote(){return remote;}};
}

test('a retired delayed sync cannot replace newer reopened progress, status, or schedule retries',async()=>{
  const f=await fixture(),cloud=delayedCloud(),oldStatuses=[];f.vault.notify=value=>oldStatuses.push(value);f.vault.adapter=cloud.adapter;
  f.state.run.fruit=23;await f.vault.checkpoint(f.state);const pending=f.vault.sync();await cloud.started;
  // The menu keeps a final local checkpoint without waiting for cloud writes.
  await f.vault.checkpoint(f.state,true);f.vault.retire();const statusesAtMenu=oldStatuses.length;
  const next=new Vault(()=>{},f.storage,offline),newState=await next.reopen(f.account);assert.equal(newState.run.fruit,23);
  newState.run.fruit=99;await next.checkpoint(newState,true);await next.sync();
  const savedBeforeOldCommit=[...f.storage.rows];cloud.release();await pending;await Promise.resolve();
  assert.deepEqual([...f.storage.rows],savedBeforeOldCommit);assert.equal(oldStatuses.length,statusesAtMenu);assert.equal(cloud.calls,1);
  await f.vault.checkpoint(f.state,true);await f.vault.sync();assert.deepEqual([...f.storage.rows],savedBeforeOldCommit);assert.equal(cloud.calls,1);
  const recovery=new Vault(()=>{},f.storage,async()=>cloud.adapter),recovered=await recovery.reopen(f.account);
  assert.equal(recovered.run.fruit,99);assert.equal(recovery.conflict,true);assert.equal(cloud.remote.revision,1);
});

test('a retired commit can acknowledge its own device row before another session claims it',async()=>{
  const f=await fixture(),cloud=delayedCloud();f.vault.adapter=cloud.adapter;const pending=f.vault.sync();await cloud.started;
  f.vault.retire();cloud.release();await pending;
  const row=JSON.parse(f.storage.getItem('monkey-trails.v1.'+f.vault.id));assert.equal(row.baseRevision,1);assert.equal(row.dirty,false);
  const next=new Vault(()=>{},f.storage,async()=>cloud.adapter);await next.reopen(f.account);assert.equal(next.conflict,false);assert.equal(next.base,1);
});

test('an identical authenticated cloud checkpoint avoids a false conflict after an old session settles',async()=>{
  const f=await fixture(),cloud=delayedCloud();f.vault.adapter=cloud.adapter;const pending=f.vault.sync();await cloud.started;
  await f.vault.checkpoint(f.state,true);f.vault.retire();
  const next=new Vault(()=>{},f.storage,offline);await next.reopen(f.account);
  const before=[...f.storage.rows];cloud.release();await pending;assert.deepEqual([...f.storage.rows],before);
  const reopened=new Vault(()=>{},f.storage,async()=>cloud.adapter);await reopened.reopen(f.account);
  assert.equal(reopened.conflict,false);assert.equal(reopened.dirty,false);assert.equal(reopened.base,1);
});

test('retiring during a delayed connection prevents an unsent cloud transaction',async()=>{
  const f=await fixture();let release;const connecting=new Promise(resolve=>{release=resolve;});let commits=0;
  f.vault.adapter=null;f.vault.connect=()=>connecting;const pending=f.vault.sync();await Promise.resolve();
  const before=[...f.storage.rows];f.vault.retire();release({async commit(){commits++;return 1;}});await pending;
  assert.equal(commits,0);assert.deepEqual([...f.storage.rows],before);
});
