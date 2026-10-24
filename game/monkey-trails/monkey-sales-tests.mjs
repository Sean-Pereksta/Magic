import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as S from './monkey-sales.mjs';
import {saveLineup,setFavorite} from './companions.mjs';
import {awardGearCopy,assignEquipment,ownedEquipmentCopies} from './equipment-inventory.mjs';
import {seal,unseal,derive} from './storage.mjs';
const setup=()=>{const s=C.newSave('Sale tests','capuchin','practice',100);for(const id of ['second','third']){const p=structuredClone(s.run.monkey);p.id=id;p.equipment={weapon:null,armor:null,charm:null};p.equipmentCopies={};s.collection[id]=p;}C.setParty(s,Object.keys(s.collection));return s;};
test('sale from exactly three keeps a playable team, returns exact gear copies and repairs saved lineups',()=>{
 const s=setup(),p=s.run.monkey,id=p.id,gear=awardGearCopy(s,'bambooKnuckles');assignEquipment(s,gear.copyId,id,'weapon');saveLineup(s,'Original',[id,'second']);
 const beforeFruit=s.run.fruit,preview=S.salePreview(s,id),receipt=S.sellMonkey(s,id);
 assert.deepEqual(receipt.payout,preview.payout);assert.equal(s.run.fruit,beforeFruit);assert.equal(Object.keys(s.collection).length,2);assert.equal(s.run.monkey.id,'second');assert.deepEqual(s.run.party,['second','third']);
 assert.deepEqual(s.meta.companions.lineups[0].members,['second']);assert.equal(ownedEquipmentCopies(s).find(row=>row.id===gear.copyId).ownerId,null);C.validate(s);
 assert.throws(()=>S.sellMonkey(s,'second'),/three/);assert.ok(C.encounterPool(s).length>0);
});
test('sale receipt replays after JSON reload without any additional resources or deletion',()=>{
 const s=setup(),id=s.run.monkey.id,first=S.sellMonkey(s,id),restored=C.validate(JSON.parse(JSON.stringify(s))),before=JSON.stringify(restored);
 assert.deepEqual(S.sellMonkey(restored,id),first);assert.equal(JSON.stringify(restored),before);
 const stale=structuredClone(s);stale.collection[id]=structuredClone(stale.run.monkey);stale.collection[id].id=id;assert.throws(()=>S.normalizeSales(stale),/receipt/);
});
test('neutral starters retain the advertised essence payout and evolved starters match their active type',()=>{
 const s=setup(),id=s.run.monkey.id;s.run.direction='E';assert.equal(S.salePreview(s,id).payout['essence:storm'],5);s.run.monkey.types=['fire'];assert.equal(S.salePreview(s,id).payout['essence:fire'],5);
});
test('favorites, protection, combat and unresolved transitions block sales atomically',()=>{
 for(const block of ['favorite','protected','battle','replacement','pending','befriend']){const s=setup(),id=s.run.monkey.id;
  if(block==='favorite')setFavorite(s,id,true);else if(block==='protected')S.setMonkeyProtection(s,id,true);else if(block==='pending')s.run.pendingEncounter={id:'pending'};else s.run.phase=block;
  S.salePreview(s,id);const before=JSON.stringify(s);assert.throws(()=>S.sellMonkey(s,id));assert.equal(JSON.stringify(s),before,block);
 }
 const s=setup(),id=s.run.monkey.id;s.collection.second.hp=0;s.collection.third.hp=0;assert.throws(()=>S.sellMonkey(s,id),/conscious/);
});
test('Ultra requires additional confirmation, rarity payouts scale and investment bonus is capped',()=>{
 const s=setup(),id=s.run.monkey.id,p=s.collection[id];p.species='ashwing';p.level=60;p.stage=3;p.types=C.SPECIES.ashwing.types;
 const offer=S.salePreview(s,id);assert.equal(offer.rarity,'ultra');assert.ok(offer.bonus<=.2);assert.ok(offer.payout.fruit>=60&&offer.payout.fruit<=72);assert.equal(Object.keys(offer.payout).filter(id=>id.startsWith('fragment:')).length,2);
 assert.throws(()=>S.sellMonkey(s,id),/Ultra/);assert.deepEqual(S.sellMonkey(s,id,{confirmUltra:true}).payout,offer.payout);
});
test('reserve survives a new expedition and withdrawals persist once, with partial transfers and bounds',()=>{
 const s=setup(),id=s.run.monkey.id;S.sellMonkey(s,id);const reserve=structuredClone(S.normalizeSales(s).reserve);s.run.phase='dead';C.restart(s);assert.deepEqual(S.normalizeSales(s).reserve,reserve);
 const fruit=s.run.fruit,op=S.nextReserveOperation(s),receipt=S.transferReserve(s,{fruit:5},op);assert.equal(s.run.fruit,fruit+5);assert.equal(s.meta.monkeySales.reserve.fruit,reserve.fruit-5);
 assert.deepEqual(S.transferReserve(s,{fruit:5},op),receipt);assert.equal(s.run.fruit,fruit+5);assert.throws(()=>S.transferReserve(s,{fruit:6},op),/different/);
 const before=JSON.stringify(s);assert.throws(()=>S.transferReserve(s,{fruit:1e9},S.nextReserveOperation(s)));assert.equal(JSON.stringify(s),before);
});
test('sale and reserve receipts survive real encrypted save/reopen',async()=>{
 const s=setup(),id=s.run.monkey.id;S.sellMonkey(s,id);const salt=btoa(String.fromCharCode(...new Uint8Array(16).fill(8))),key=await derive('sales-password-long',salt),e=await seal(s,key,salt,'sale-identity'),restored=await unseal(e,key,'sale-identity');
 assert.deepEqual(restored.meta.monkeySales,s.meta.monkeySales);assert.equal(restored.collection[id],undefined);assert.deepEqual(S.sellMonkey(restored,id),s.meta.monkeySales.receipts[id]);
});
