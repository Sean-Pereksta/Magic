import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {identity,derive,seal,unseal} from './storage.mjs';

const amounts=()=>Object.fromEntries(Object.keys(C.ELEMENTS).map((type,index)=>[type,31+index*17]));
const stocked=()=>{const s=C.newSave('Essence keeper','capuchin','practice',12345);s.run.essence=amounts();return s;};
function defeat(s){
  s.run.nextEncounter=s.run.distance;C.advance(s,1e-7);C.beginPendingEncounter(s);C.readyBattle(s);
  for(const p of C.team(s))p.hp=0;
  s.run.monkey.hp=1;s.run.monkey.status.poison=2;s.run.monkey.loadout=['strike','brace','breathe'];
  for(const enemy of s.run.battle.enemies){enemy.hp=999;enemy.loadout=['breathe'];}
  C.turn(s,'breathe');assert.equal(s.run.phase,'dead');
}

test('all eight essence balances survive real defeat, encrypted reload, and expedition restart',async()=>{
  let s=stocked();const expected=structuredClone(s.run.essence);
  defeat(s);assert.deepEqual(s.run.essence,expected);
  const id=await identity(s.name,s.mode),salt=btoa('0123456789abcdef'),key=await derive('preserve-my-essences',salt);
  s=await unseal(await seal(s,key,salt,id),key,id);assert.deepEqual(s.run.essence,expected);
  const previous=s.run.essence;C.restart(s);assert.deepEqual(s.run.essence,expected);assert.notEqual(s.run.essence,previous);
  assert.deepEqual(previous,expected,'the previous expedition inventory is not mutated by restart');
  s=C.validate(structuredClone(s));assert.deepEqual(s.run.essence,expected);
  defeat(s);C.restart(s);assert.deepEqual(s.run.essence,expected,'repeated expeditions never reset or duplicate essence');
});

test('legacy starting essence adds its earned bonus to carried balances once per expedition',()=>{
  const s=stocked();s.meta.trailWisdom.earned=1000;
  for(let rank=0;rank<4;rank++)C.purchaseLegacyUpgrade(s,'prepared');
  const expected=structuredClone(s.run.essence);defeat(s);C.restart(s);
  const types=Object.keys(C.ELEMENTS);for(let i=0;i<3;i++)expected[types[(s.meta.runs+i)%types.length]]++;
  assert.deepEqual(s.run.essence,expected);
  assert.deepEqual(C.validate(structuredClone(s)).run.essence,expected,'loading does not award another starting bonus');
});

test('uncommon essence also survives defeat and restart while other temporary forge materials reset',()=>{
  const s=stocked(),expected=Object.fromEntries(Object.entries(amounts()).map(([type,amount])=>['essence:'+type,amount]));
  s.run.forgeMaterials={...expected,fruit:5,'item:berry':3};
  defeat(s);assert.deepEqual(s.run.forgeMaterials,expected);
  C.restart(s);assert.deepEqual(s.run.forgeMaterials,expected);
  assert.deepEqual(C.validate(structuredClone(s)).run.forgeMaterials,expected);
});

test('existing and original-version save validation retain every owned essence amount',()=>{
  const current=stocked(),expected=structuredClone(current.run.essence);
  for(let load=0;load<4;load++)assert.deepEqual(C.validate(structuredClone(current)).run.essence,expected);
  const original=structuredClone(current);original.version=1;delete original.collection;
  assert.deepEqual(C.validate(original).run.essence,expected);
});

test('fresh saves initialize empty essence without affecting an existing save',()=>{
  const existing=stocked(),before=structuredClone(existing.run.essence),fresh=C.newSave('New keeper','macaque','practice',44);
  assert.ok(Object.values(fresh.run.essence).every(amount=>amount===0));
  assert.deepEqual(existing.run.essence,before);
});
