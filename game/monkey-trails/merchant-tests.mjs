import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as M from './merchant.mjs';
import {renderMerchant} from './merchant-ui.mjs';
import {normalizeLivingWorld,SPECIAL_LOCATIONS} from './living-world.mjs';
function fixture(){
 const s=C.newSave('Merchant tests','capuchin','practice',23),w=normalizeLivingWorld(s),land=s.world.activeLand,id=`location:${land}:1`,def=SPECIAL_LOCATIONS.find(row=>row.landId===land&&row.icon==='merchant');w.regions[land].spawnSerial=1;w.locations[id]={id,definitionId:def.id,landId:land,serial:1,status:'visited',position:{east:0,north:0},stage:0};
 s.run.fruit=100;s.run.items.berry=10;s.run.items.revive=0;s.run.items.ingot=3;s.run.items.starOre=1;return{s,id};
}
test('merchant previews use exact forge material values without changing inventory or RNG',()=>{
 const {s,id}=fixture(),before=JSON.stringify(s),v=M.merchantView(s,id,{fruit:2,'item:ingot':2});assert.equal(v.paymentValue,12);assert.equal(v.offers.find(x=>x.id==='berries').price,8);assert.equal(v.offers.find(x=>x.id==='revival').price,13);assert.equal(JSON.stringify(s),before);assert.match(renderMerchant(s,id,{fruit:2,'item:ingot':2}),/Offered pack value: <strong>12/);
});
test('barter consumes only selected pack materials, awards stock and keeps excess value as saved credit',()=>{
 const {s,id}=fixture(),rng=s.rng,first=M.purchaseMerchant(s,id,'berries',{'item:ingot':2},M.nextMerchantOperationId(s));assert.equal(s.run.items.ingot,1);assert.equal(s.run.items.berry,12);assert.equal(s.run.fruit,100);assert.equal(first.creditAfter,2);assert.equal(s.rng,rng);
 const before=JSON.stringify(s);assert.deepEqual(M.purchaseMerchant(s,id,'berries',{'item:ingot':2},first.operationId),first);assert.equal(JSON.stringify(s),before);
 const loaded=C.validate(JSON.parse(before));assert.equal(M.merchantView(loaded,id).credit,2);M.purchaseMerchant(loaded,id,'berries',{fruit:6},M.nextMerchantOperationId(loaded));assert.equal(loaded.meta.merchants.credit,0);assert.equal(loaded.run.fruit,94);
});
test('stock limits and changed operation inputs cannot duplicate purchases',()=>{
 const {s,id}=fixture(),receipt=M.purchaseMerchant(s,id,'revival',{fruit:13},'merchant-1'),before=JSON.stringify(s);assert.throws(()=>M.purchaseMerchant(s,id,'revival',{fruit:14},'merchant-1'),/different/);assert.throws(()=>M.purchaseMerchant(s,id,'revival',{fruit:13},'merchant-2'),/sold out/);assert.equal(JSON.stringify(s),before);assert.equal(receipt.reward.quantity,1);
});
test('invalid payments, wrong regions, battles and unvisited merchants fail without mutations',()=>{
 const {s,id}=fixture();for(const payment of [{fruit:-1},{fruit:.5},{fruit:101},{'equipment:fang':1},{'item:not-real':1},{fruit:1}]){const before=JSON.stringify(s);assert.throws(()=>M.purchaseMerchant(s,id,'revival',payment,'merchant-1'));assert.equal(JSON.stringify(s),before);}
 for(const phase of ['battle','replacement','dead']){s.run.phase=phase;const before=JSON.stringify(s);assert.throws(()=>M.purchaseMerchant(s,id,'berries',{fruit:8},'merchant-1'),/between encounters/);assert.equal(JSON.stringify(s),before);}s.run.phase='trail';s.meta.livingWorld.locations[id].status='discovered';const before=JSON.stringify(s);assert.throws(()=>M.purchaseMerchant(s,id,'berries',{fruit:8},'merchant-1'),/Visit/);assert.equal(JSON.stringify(s),before);
});
test('materials bought back from merchants never create value through resale',()=>{
 const {s,id}=fixture();const v=M.merchantView(s,id);for(const offer of v.offers)assert.ok(offer.price>offer.craftValue);
});

test('merchant ledgers prune inaccessible stock and reject a full active ledger atomically',()=>{
 const {s,id}=fixture(),land=s.world.activeLand,def=s.meta.livingWorld.locations[id].definitionId;
 s.meta.merchants={version:1,credit:0,serial:0,last:null,shops:{}};
 for(let i=2;i<=1001;i++)s.meta.merchants.shops[`location:${land}:${i}`]={purchased:{}};
 M.purchaseMerchant(s,id,'berries',{fruit:8},'merchant-1');assert.equal(Object.keys(s.meta.merchants.shops).length,1);
 const {s:full,id:target}=fixture();full.meta.livingWorld.regions[land].spawnSerial=1001;full.meta.merchants={version:1,credit:0,serial:0,last:null,shops:{}};
 for(let i=2;i<=1001;i++){const key=`location:${land}:${i}`;full.meta.merchants.shops[key]={purchased:{}};full.meta.livingWorld.locations[key]={id:key,definitionId:def,landId:land,serial:i,status:'visited',position:{east:0,north:0}};}
 const before=JSON.stringify(full);assert.throws(()=>M.purchaseMerchant(full,target,'berries',{fruit:8},'merchant-1'),/ledger is full/);assert.equal(JSON.stringify(full),before);
});

test('near-limit valid saves reject a purchase without spending payment, stock or credit',()=>{
 const {s,id}=fixture();C.validate(s);s.meta.merchantTestPadding='';s.meta.merchantTestPadding='x'.repeat(649900-JSON.stringify(s).length);
 assert.equal(JSON.stringify(s).length,649900);assert.doesNotThrow(()=>C.validate(JSON.parse(JSON.stringify(s))));
 const before=JSON.stringify(s);assert.throws(()=>M.purchaseMerchant(s,id,'berries',{fruit:8},'merchant-1'),/save is too large/);assert.equal(JSON.stringify(s),before);
});
