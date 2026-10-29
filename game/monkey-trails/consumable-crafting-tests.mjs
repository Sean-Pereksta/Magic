import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {awardGearCopy} from './equipment-inventory.mjs';

function stocked(){
  const s=C.newSave('Consumable crafting','capuchin','practice',13);
  s.run.fruit=50;s.run.items.charmMaterial=10;s.run.essence.fire=10;s.run.essence.water=10;
  return s;
}

test('consumables spend the displayed recipe and only the selected essence',()=>{
  const s=stocked();
  const initial={...s.run.items};
  C.craft(s,'charm');assert.equal(s.run.items.bondCharm,initial.bondCharm+1);assert.equal(s.run.fruit,48);assert.equal(s.run.items.charmMaterial,9);
  C.craft(s,'berry','water');assert.equal(s.run.items.berry,initial.berry+1);assert.equal(s.run.fruit,44);assert.equal(s.run.essence.water,8);assert.equal(s.run.essence.fire,10);
  C.craft(s,'revive','fire');assert.equal(s.run.items.revive,initial.revive+1);assert.equal(s.run.fruit,24);assert.equal(s.run.items.charmMaterial,1);assert.equal(s.run.essence.fire,5);assert.equal(s.run.essence.water,8);
});

test('missing ingredients or an invalid essence never spend resources',()=>{
  for(const [id,element,alter] of [
    ['berry',undefined,()=>{}],['berry','unknown',()=>{}],['berry','__proto__',()=>{}],
    ['berry','fire',s=>s.run.essence.fire=1],['berry','fire',s=>s.run.fruit=3],
    ['revive','fire',s=>s.run.items.charmMaterial=7],['revive','fire',s=>s.run.fruit=19],
    ['revive','fire',s=>s.run.essence.fire=4],['bogus','fire',()=>{}]
  ]){
    const s=stocked();alter(s);const before=structuredClone(s);
    assert.equal(C.consumableCraftAvailability(s,id,element).ok,false);
    assert.throws(()=>C.craft(s,id,element));assert.deepEqual(s,before);
  }
});

test('a charm can be crafted for the current recruitment opportunity',()=>{
  const s=stocked(),candidate=structuredClone(s.run.monkey);candidate.id='candidate';
  s.run.phase='befriend';s.run.items.bondCharm=0;
  s.run.capture={id:'test-capture',candidates:[candidate],attempts:0,maxAttempts:3,guaranteed:true,ultra:false,claimed:false};
  const capture=structuredClone(s.run.capture);
  C.craft(s,'bondCharm');assert.deepEqual(s.run.capture,capture);assert.equal(s.run.phase,'befriend');
  assert.equal(C.befriend(s,'candidate').success,true);assert.equal(s.run.items.bondCharm,0);
});

test('crafting is blocked during combat and after an expedition ends',()=>{
  for(const phase of ['battle','replacement','dead']){
    const s=stocked();s.run.phase=phase;const before=structuredClone(s);
    assert.equal(C.consumableCraftAvailability(s,'revive','fire').ok,false);
    assert.throws(()=>C.craft(s,'revive','fire'));assert.deepEqual(s,before);
  }
});

test('Trailforger refunds only eligible common ingredients, never essence or charm material',()=>{
  const s=stocked(),copy=awardGearCopy(s,'trailforgersPrism');C.equip(s,copy.copyId,s.run.activeId,'charm');
  C.craft(s,'revive','fire');
  assert.equal(s.run.fruit,34); // 20 spent, 4 fruit returned by the equipped prism.
  assert.equal(s.run.items.charmMaterial,2);assert.equal(s.run.essence.fire,5);
});
