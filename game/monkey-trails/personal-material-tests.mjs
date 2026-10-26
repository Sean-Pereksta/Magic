import test from 'node:test';
import assert from 'node:assert/strict';
import {newSave} from './core.mjs';
import {PERSONAL_MATERIAL_IDS} from './exploration.mjs';
import {availableInPack,depositFromPack,retryPackDeposits,applySocialReceipt} from './social-local.mjs';
import {SocialStore,createMemorySocialAdapter} from './social-store.mjs';
import {renderSocialView} from './social-ui.mjs';

test('personal ore and victory materials cannot be deposited, retried or credited by a social receipt',async()=>{
  for(const asset of PERSONAL_MATERIAL_IDS){
    const s=newSave('Personal materials','capuchin','practice',123);s.run.items[asset]=5;const before=structuredClone(s);let calls=0;
    const transfer={state:s,store:{deposit:async()=>{calls++;}},uid:'a',saveId:'saved',asset,amount:1,persist:async()=>{calls++;}};
    assert.throws(()=>availableInPack(s,asset),/Personal crafting/);await assert.rejects(depositFromPack(transfer),/Personal crafting/);assert.deepEqual(s,before);assert.equal(calls,0);
    for(const walletCredited of[false,true]){assert.throws(()=>applySocialReceipt(s,{id:'bad',uid:'a',walletCredited,payload:{fruit:5,items:{[asset]:1}}}),/Invalid social reward/);assert.deepEqual(s,before);}
    s.social={applied:{},deposits:{pending:{asset,amount:1,uid:'a',saveId:'saved',runId:s.run.id}}};const pending=structuredClone(s);
    await assert.rejects(retryPackDeposits(transfer),/Personal crafting/);assert.equal(calls,0);assert.deepEqual(s,pending);
  }
});

test('social store rejects personal material deposits, withdrawals, gifts and either side of a trade before writes',async()=>{
  const adapter=createMemorySocialAdapter(),a=new SocialStore(adapter,{uid:'a'});
  await a.register({saveId:'saved',name:'Social materials',team:[newSave('Crew','capuchin','practice',123).run.monkey]});
  const before=await a.getPlayer();
  for(const asset of PERSONAL_MATERIAL_IDS){
    for(const operation of[()=>a.deposit(asset,1,'bad'),()=>a.withdraw(asset,1,'bad'),()=>a.gift('b',asset,1),()=>a.offerTrade('b',{asset,amount:1},{asset:'fruit',amount:1}),()=>a.offerTrade('b',{asset:'fruit',amount:1},{asset,amount:1})])assert.throws(operation,/Personal crafting/);
  }
  assert.deepEqual(await a.getPlayer(),before);assert.deepEqual(await adapter.list('operations'),[]);
});

test('multiplayer transfer and trade selectors expose usable supplies and omit personal crafting materials',()=>{
  const me={uid:'a',name:'Canopy',team:[newSave('Crew','capuchin','practice',123).run.monkey],wallet:{fruit:2,items:{berry:1,ingot:1,starOre:1,crownCore:1}}};
  const html=renderSocialView({status:'ready',now:1800000000000,me,members:[me],nearby:[],challenges:[],offers:[],tribe:{id:'CANOPY',name:'Canopy',ownerId:'a',members:['a'],boss:{level:1,hp:1500,maxHp:1500}}});
  assert.match(html,/<option value="berry"/);
  for(const id of PERSONAL_MATERIAL_IDS)assert.doesNotMatch(html,new RegExp(`<option value="${id}"`));
});
