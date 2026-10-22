import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {Vault} from './storage.mjs';

const memory=()=>{const map=new Map();return {getItem:key=>map.get(key)||null,setItem:(key,value)=>map.set(key,value)};};
const flush=async vault=>{if(vault.localTask)await vault.localTask;if(vault.inflight)await vault.inflight;await vault.sync();};

test('new species loadouts at every evolution stage survive encrypted reopening on another device',async()=>{
  const docs=new Map(),cloud={async read(id){return structuredClone(docs.get(id)||null);},async commit(id,base,envelope){const old=docs.get(id);if((old?.revision||0)!==base){const error=Error('Conflict');error.code='save-conflict';throw error;}docs.set(id,{...structuredClone(envelope),revision:base+1});return base+1;}};
  const device=memory(),vault=new Vault(()=>{},device,async()=>cloud);
  const save=await vault.open('Expansion Loadouts','expansion-save-password',{create:true});
  await flush(vault);
  const additions=Object.values(C.SPECIES).filter(species=>species.number>60);
  assert.equal(additions.length,30);
  for(const species of additions)for(let stage=0;stage<=3;stage++){
    const companion=structuredClone(save.run.monkey);companion.id=`${species.id}-${stage}`;companion.species=species.id;companion.types=[...species.types];companion.stage=stage;companion.level=10;companion.learned=[];companion.loadout=['strike','brace','breathe'];
    if(stage===3)companion.mastery=companion.types[0];
    companion.hp=C.stats(companion).hp;companion.energy=C.stats(companion).maxEnergy;save.collection[companion.id]=companion;
    const option=C.unlocked(companion).find(id=>!companion.loadout.includes(id));assert.ok(option,`${companion.id} has a legal replacement`);
    C.setMove(save,3,option,companion.id);assert.throws(()=>C.setMove(save,2,option,companion.id),/already equipped/);
  }
  await vault.checkpoint(save,true);await flush(vault);
  const remoteVault=new Vault(()=>{},memory(),async()=>cloud),remote=await remoteVault.open('Expansion Loadouts','expansion-save-password');
  assert.equal(Object.keys(remote.collection).length,121);
  for(const [id,companion]of Object.entries(save.collection))assert.deepEqual(remote.collection[id].loadout,companion.loadout,id);
  const localVault=new Vault(()=>{},device,async()=>{throw Error('Offline');}),local=await localVault.open('Expansion Loadouts','expansion-save-password');
  assert.deepEqual(Object.values(local.collection).map(p=>p.loadout),Object.values(remote.collection).map(p=>p.loadout));
  assert.ok(!JSON.stringify([...docs.values()]).includes(additions[0].id),'cloud payload remains encrypted');
});
