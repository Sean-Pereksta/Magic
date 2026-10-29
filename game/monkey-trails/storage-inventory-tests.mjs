import test from 'node:test';
import assert from 'node:assert/strict';
import {Vault,seal,unseal,derive,importBackup} from './storage.mjs';

const name='Essence Keeper',password='a-unique-essence-password';
const offline=async()=>{throw Error('offline');};
class DeviceStorage {
  rows=new Map();
  getItem(key){return this.rows.get(key)||null;}
  setItem(key,value){this.rows.set(key,value);}
}
async function fixture(){
  const storage=new DeviceStorage(),vault=new Vault(()=>{},storage,offline);
  const state=await vault.open(name,password,{create:true});await vault.sync();
  const empty=structuredClone(state);
  Object.keys(state.run.essence).forEach((id,index)=>{state.run.essence[id]=17+index*13;});
  state.run.fruit=137;state.run.items.charmMaterial=29;
  await vault.checkpoint(state);await vault.sync();
  const deviceKey='monkey-trails.v1.'+vault.id;
  const account={format:1,name,id:vault.id,mode:'gps',salt:vault.salt,key:vault.key};
  const envelope=s=>seal(s,vault.key,vault.salt,vault.id);
  const row=(baseRevision,dirty=false)=>{
    const saved=JSON.parse(storage.getItem(deviceKey));
    storage.setItem(deviceKey,JSON.stringify({...saved,baseRevision,dirty}));
  };
  return {storage,vault,state,empty,deviceKey,account,envelope,row};
}
const inventory=s=>({essence:s.run.essence,fruit:s.run.fruit,items:s.run.items});

test('password and remembered reload preserve every distinct elemental balance exactly',async()=>{
  const f=await fixture(),expected=structuredClone(inventory(f.state));
  for(const remembered of [false,true]){
    const vault=new Vault(()=>{},f.storage,offline);
    const restored=remembered?await vault.reopen(f.account):await vault.open(name,password);
    assert.deepEqual(inventory(restored),expected);
    await vault.checkpoint(restored);await vault.sync();
  }
});

for(const cloudRevision of [4,5])test(`clean device inventory cannot roll back to differing cloud revision ${cloudRevision} at local revision 5`,async()=>{
  const f=await fixture();f.row(5);const remote={...await f.envelope(f.empty),revision:cloudRevision};let commits=0;
  const vault=new Vault(()=>{},f.storage,async()=>({read:async()=>remote,commit:async()=>{commits++;}}));
  const restored=await vault.open(name,password);
  assert.deepEqual(inventory(restored),inventory(f.state));
  assert.equal(vault.base,5);assert.equal(vault.dirty,true);assert.equal(vault.conflict,true);
  await vault.sync();assert.equal(commits,0,'divergent checkpoints require explicit recovery');
});

test('a truly newer cloud checkpoint may contain legitimate essence spending without duplication',async()=>{
  const f=await fixture();f.row(5);const spent=structuredClone(f.state);spent.run.essence.water-=3;spent.run.essence.leaf-=2;
  const remote={...await f.envelope(spent),revision:6};
  const vault=new Vault(()=>{},f.storage,async()=>({read:async()=>remote})),restored=await vault.open(name,password);
  assert.deepEqual(inventory(restored),inventory(spent));assert.equal(vault.base,6);assert.equal(vault.dirty,false);assert.equal(vault.conflict,false);
});

test('a checkpoint finishing while reload waits for cloud cannot be overwritten by the opening snapshot',async()=>{
  const f=await fixture(),remote={...await f.envelope(f.empty),revision:5};
  f.storage.setItem(f.deviceKey,JSON.stringify({...remote,baseRevision:5,dirty:false,seq:1}));
  let begin,release;const started=new Promise(resolve=>{begin=resolve;}),waiting=new Promise(resolve=>{release=resolve;});
  const vault=new Vault(()=>{},f.storage,async()=>({read:async()=>{begin();return waiting;}}));
  const opening=vault.open(name,password);await started;
  f.storage.setItem(f.deviceKey,JSON.stringify({...await f.envelope(f.state),baseRevision:5,dirty:true,seq:2}));
  release(remote);const restored=await opening;
  assert.deepEqual(inventory(restored),inventory(f.state));assert.equal(vault.dirty,true);assert.equal(vault.conflict,false);
});

test('a checkpoint finishing during password derivation cannot be overwritten by reload',async()=>{
  const f=await fixture(),empty={...await f.envelope(f.empty),baseRevision:5,dirty:false};
  f.storage.setItem(f.deviceKey,JSON.stringify(empty));
  let begin,release,calls=0;const started=new Promise(resolve=>{begin=resolve;}),waiting=new Promise(resolve=>{release=resolve;});
  const vault=new Vault(()=>{},f.storage,offline);
  const opening=vault.unlock(name,{mode:'gps'},async()=>{if(++calls===1){begin();await waiting;}return f.vault.key;});
  await started;f.storage.setItem(f.deviceKey,JSON.stringify({...await f.envelope(f.state),baseRevision:5,dirty:true}));
  release();const restored=await opening;
  assert.deepEqual(inventory(restored),inventory(f.state));assert.equal(vault.dirty,true);
  assert.deepEqual(inventory(await unseal(JSON.parse(f.storage.getItem(f.deviceKey)),f.vault.key,f.vault.id)),inventory(f.state));
});

test('an invalid encrypted primary falls back to the verified backup and keeps that backup intact',async()=>{
  const f=await fixture();f.row(5);const good=f.storage.getItem(f.deviceKey),corrupt=JSON.parse(good);
  corrupt.cipher=(corrupt.cipher[0]==='A'?'B':'A')+corrupt.cipher.slice(1);
  f.storage.setItem(f.deviceKey+'.backup',good);f.storage.setItem(f.deviceKey,JSON.stringify(corrupt));
  const remote={...await f.envelope(f.empty),revision:4};
  const vault=new Vault(()=>{},f.storage,async()=>({read:async()=>remote})),restored=await vault.open(name,password);
  assert.deepEqual(inventory(restored),inventory(f.state));assert.equal(vault.conflict,true);
  assert.equal(f.storage.getItem(f.deviceKey+'.backup'),good);
  assert.deepEqual(inventory(await unseal(JSON.parse(good),f.vault.key,f.vault.id)),inventory(f.state));
  const reloaded=await new Vault(()=>{},f.storage,offline).reopen(f.account);
  assert.deepEqual(inventory(reloaded),inventory(f.state));
});

test('an unauthenticated cloud checkpoint cannot replace a clean valid device inventory',async()=>{
  const f=await fixture();f.row(5);const remote={...await f.envelope(f.empty),revision:6};
  remote.cipher=(remote.cipher[0]==='A'?'B':'A')+remote.cipher.slice(1);
  const vault=new Vault(()=>{},f.storage,async()=>({read:async()=>remote})),restored=await vault.open(name,password);
  assert.deepEqual(inventory(restored),inventory(f.state));assert.equal(vault.conflict,true);assert.equal(vault.dirty,true);
});

test('a different decryptable cloud key cannot replace a clean valid device inventory',async()=>{
  const f=await fixture();f.row(5);
  const salt=btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(16)))),key=await derive(password,salt),remote={...await seal(f.empty,key,salt,f.vault.id),revision:6};
  const vault=new Vault(()=>{},f.storage,async()=>({read:async()=>remote})),restored=await vault.open(name,password);
  assert.deepEqual(inventory(restored),inventory(f.state));assert.equal(vault.conflict,true);assert.equal(vault.dirty,true);
});

test('identical cloud content at an older revision cannot lower the acknowledged device revision',async()=>{
  const f=await fixture();f.row(5);const remote={...await f.envelope(f.state),revision:4};
  const vault=new Vault(()=>{},f.storage,async()=>({read:async()=>remote})),restored=await vault.open(name,password);
  assert.deepEqual(inventory(restored),inventory(f.state));assert.equal(vault.base,5);assert.equal(vault.conflict,true);
});

test('review and portable export expose an intact rich backup without changing the current zero inventory or either copy',async()=>{
  const f=await fixture();f.state.run.forgeMaterials={'essence:water':7,'essence:leaf':4};
  const rich={...await f.envelope(f.state),baseRevision:5,dirty:false};
  await f.vault.checkpoint(f.empty);await f.vault.sync();
  f.storage.setItem(f.deviceKey+'.backup',JSON.stringify(rich));f.storage.setItem(f.deviceKey+'.recovery',JSON.stringify({...rich,baseRevision:4}));
  const before=[...f.storage.rows],snapshot=structuredClone(f.vault.snapshot),rows=await f.vault.recoveryCopies();
  assert.deepEqual(rows.map(row=>row.source),['backup','recovery']);
  for(const row of rows){assert.deepEqual(row.essence,f.state.run.essence);assert.equal(row.forgeEssence.water,7);assert.equal(row.forgeEssence.leaf,4);assert.equal(row.runId,f.state.run.id);assert.equal(row.expedition,f.state.meta.runs);assert.equal(row.fruit,137);}
  const text=await f.vault.exportRecovery('backup',rows[0].checkpointId),exported=JSON.parse(text);
  assert.equal(exported.cipher,rich.cipher);assert.equal(exported.iv,rich.iv);assert.equal(exported.salt,rich.salt);assert.equal(exported.baseRevision,5);assert.equal(exported.dirty,true);assert.equal('key' in exported,false);
  assert.deepEqual([...f.storage.rows],before);assert.deepEqual(f.vault.snapshot,snapshot);
  const imported=new DeviceStorage();await importBackup(name,password,text,imported);
  const restored=await new Vault(()=>{},imported,offline).open(name,password);
  assert.deepEqual(inventory(restored),inventory(f.state));assert.equal(restored.run.forgeMaterials['essence:water'],7);
});

test('backup review does not automatically undo legitimate spending and omits invalid or mismatched copies',async()=>{
  const f=await fixture(),rich={...await f.envelope(f.state),baseRevision:5,dirty:false};
  f.state.run.essence.water-=3;await f.vault.checkpoint(f.state);await f.vault.sync();
  f.storage.setItem(f.deviceKey+'.backup',JSON.stringify(rich));
  const other=structuredClone(f.state);other.name='Another Save';
  f.storage.setItem(f.deviceKey+'.recovery',JSON.stringify({...await f.envelope(other),baseRevision:5}));
  const before=[...f.storage.rows],rows=await f.vault.recoveryCopies();
  assert.equal(rows.length,1);assert.equal(rows[0].essence.water,f.state.run.essence.water+3);
  await f.vault.exportRecovery('backup',rows[0].checkpointId);assert.deepEqual(f.vault.snapshot.run.essence,f.state.run.essence);assert.deepEqual([...f.storage.rows],before);
  await assert.rejects(f.vault.exportRecovery('recovery'),/different save/);await assert.rejects(f.vault.exportRecovery('unknown'),/Choose a saved/);
  const corrupt={...rich,cipher:(rich.cipher[0]==='A'?'B':'A')+rich.cipher.slice(1)};
  f.storage.setItem(f.deviceKey+'.backup',JSON.stringify(corrupt));assert.deepEqual(await f.vault.recoveryCopies(),[]);
  await assert.rejects(f.vault.exportRecovery('backup'));
});

test('a rotating backup must be reviewed again before exporting a different encrypted checkpoint',async()=>{
  const f=await fixture(),rich={...await f.envelope(f.state),baseRevision:5,dirty:false};
  f.storage.setItem(f.deviceKey+'.backup',JSON.stringify(rich));
  const [reviewed]=await f.vault.recoveryCopies();assert.match(reviewed.checkpointId,/^[a-f0-9]{64}$/);
  f.storage.setItem(f.deviceKey+'.backup',JSON.stringify({...await f.envelope(f.empty),baseRevision:6,dirty:false}));
  const before=[...f.storage.rows],snapshot=structuredClone(f.vault.snapshot);
  await assert.rejects(f.vault.exportRecovery('backup',reviewed.checkpointId),/backup changed.*Review saved backups again/);
  assert.deepEqual([...f.storage.rows],before);assert.deepEqual(f.vault.snapshot,snapshot);
  const [current]=await f.vault.recoveryCopies();assert.notEqual(current.checkpointId,reviewed.checkpointId);
  const exported=JSON.parse(await f.vault.exportRecovery('backup',current.checkpointId));
  assert.deepEqual(inventory(await unseal(exported,f.vault.key,f.vault.id)),inventory(f.empty));
});

test('an already zeroed save retains the preceding rich backup through opening, automatic checkpoints and another reload',async()=>{
  const f=await fixture(),rich={...await f.envelope(f.state),baseRevision:5,dirty:false};
  const richRaw=JSON.stringify(rich),recoveryRaw=JSON.stringify({...rich,baseRevision:4});
  f.storage.setItem(f.deviceKey,JSON.stringify({...await f.envelope(f.empty),baseRevision:6,dirty:false}));
  f.storage.setItem(f.deviceKey+'.backup',richRaw);f.storage.setItem(f.deviceKey+'.recovery',recoveryRaw);
  const vault=new Vault(()=>{},f.storage,offline),current=await vault.open(name,password);
  assert.deepEqual(inventory(current),inventory(f.empty),'a richer backup is never silently applied');
  assert.equal(f.storage.getItem(f.deviceKey+'.backup'),richRaw,'opening does not rotate the last valid backup');
  assert.equal(f.storage.getItem(f.deviceKey+'.reopen-backup'),richRaw,'the verified prior backup has a stable review copy');
  await vault.checkpoint(current);await vault.sync();
  assert.equal(f.storage.getItem(f.deviceKey+'.recovery'),recoveryRaw,'explicit recovery copy stays intact');
  const reloaded=new Vault(()=>{},f.storage,offline),again=await reloaded.open(name,password);
  assert.deepEqual(inventory(again),inventory(f.empty));
  const retained=(await reloaded.recoveryCopies()).find(row=>row.source==='reopen-backup');
  assert.ok(retained);assert.deepEqual(retained.essence,f.state.run.essence);
  const exported=JSON.parse(await reloaded.exportRecovery('reopen-backup',retained.checkpointId));
  assert.equal(exported.cipher,rich.cipher);assert.deepEqual(inventory(await unseal(exported,f.vault.key,f.vault.id)),inventory(f.state));
  await reloaded.checkpoint(again);await reloaded.sync();
  assert.equal(f.storage.getItem(f.deviceKey+'.reopen-backup'),richRaw,'later checkpoints cannot rotate the stable retained copy');
});

test('a review-only retained copy is never automatically applied when all opening copies are invalid',async()=>{
  const f=await fixture(),good=f.storage.getItem(f.deviceKey),corrupt=JSON.parse(good);
  corrupt.cipher=(corrupt.cipher[0]==='A'?'B':'A')+corrupt.cipher.slice(1);
  f.storage.setItem(f.deviceKey,JSON.stringify(corrupt));f.storage.setItem(f.deviceKey+'.backup',JSON.stringify(corrupt));
  f.storage.setItem(f.deviceKey+'.reopen-backup',good);const before=[...f.storage.rows];
  await assert.rejects(new Vault(()=>{},f.storage,offline).open(name,password),/password did not unlock a valid save/);
  assert.deepEqual([...f.storage.rows],before);
});
