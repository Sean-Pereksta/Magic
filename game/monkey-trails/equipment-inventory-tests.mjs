import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as I from './equipment-inventory.mjs';
import {monkeyPower} from './power.mjs';
const save=()=>C.newSave('Equipment copies','capuchin','practice',345);
const other=(s,id='second',species='capuchin')=>{const p=structuredClone(s.run.monkey);p.id=id;p.species=species;p.equipment={weapon:null,armor:null,charm:null};delete p.equipmentCopies;s.collection[id]=p;return p;};

test('migration retains original ranks, injuries and equipment assignments',()=>{
  const s=save(),p=s.run.monkey;s.meta.gear={fang:4,heart:2};p.equipment.weapon='fang';p.hp=7;
  I.normalizeEquipmentInventory(s);assert.equal(p.hp,7);assert.deepEqual(C.equipped(s,p),{fang:4});
  const before=JSON.stringify(s);I.normalizeEquipmentInventory(s);assert.equal(JSON.stringify(s),before);C.validate(JSON.parse(before));
});
test('standard and Ultra equipment capacity is independent of item rarity',()=>{
  const s=save(),p=s.run.monkey,u=other(s,'ultra',Object.keys(C.SPECIES).find(k=>C.SPECIES[k].rarity==='ultra'));
  assert.equal(I.equipmentSlots(p).length,3);assert.equal(I.equipmentSlots(u).length,4);
  for(const id of ['fang','shell','heart','wind']){const c=I.awardGearCopy(s,id);C.equip(s,c.copyId,u.id,I.equipmentSlots(u)[Object.keys(C.equipped(s,u)).length]);}
  assert.equal(Object.keys(C.equipped(s,u)).length,4);assert.throws(()=>C.equip(s,I.awardGearCopy(s,'satchel').copyId,p.id,'ultra'),/slot/);C.validate(s);
});
test('separate copies can belong to separate monkeys and transfer exactly once',()=>{
  const s=save(),a=s.run.monkey,b=other(s),one=I.awardGearCopy(s,'fang');C.equip(s,one.copyId,a.id,'weapon');
  const two=I.awardGearCopy(s,'fang');C.equip(s,two.copyId,b.id,'charm');assert.equal(C.equipped(s,a).fang,1);assert.equal(C.equipped(s,b).fang,1);
  assert.throws(()=>C.equip(s,two.copyId,a.id,'armor'),/named item/);C.unequip(s,'weapon',a.id);C.equip(s,two.copyId,a.id,'armor');assert.deepEqual(C.equipped(s,b),{});assert.equal(C.equipped(s,a).fang,1);C.validate(s);
});
test('automatic duplicate upgrades exclude equipped, favorite and protected copies',()=>{
  const s=save(),p=s.run.monkey,a=I.awardGearCopy(s,'fang');C.equip(s,a.copyId,p.id);
  const b=I.awardGearCopy(s,'fang');assert.equal(b.kind,'discovered');I.setEquipmentCopyFlag(s,b.copyId,'favorite',true);
  const c=I.awardGearCopy(s,'fang');I.setEquipmentCopyFlag(s,c.copyId,'protected',true);
  const d=I.awardGearCopy(s,'fang');assert.equal(d.kind,'discovered');assert.equal(I.awardGearCopy(s,'fang').kind,'upgraded');
  for(const id of [a.copyId,b.copyId,c.copyId])assert.equal(I.equipmentCopy(s,id).tier,1);assert.equal(I.equipmentCopy(s,d.copyId).tier,2);
});
test('copy award previews never mutate checkpoints or RNG',()=>{
  const s=save();s.meta.gear.fang=3;const before=JSON.stringify(s);assert.equal(I.previewGearAward(s,'fang').tier,4);assert.equal(JSON.stringify(s),before);
});
test('explicit upgrades consume only unassigned, unprotected same-rank duplicates',()=>{
  const s=save(),p=s.run.monkey,a=I.awardGearCopy(s,'fang');C.equip(s,a.copyId,p.id);const b=I.awardGearCopy(s,'fang');I.setEquipmentCopyFlag(s,b.copyId,'protected',true);
  assert.throws(()=>I.upgradeEquipmentCopy(s,a.copyId),/duplicate/);I.setEquipmentCopyFlag(s,b.copyId,'protected',false);I.upgradeEquipmentCopy(s,a.copyId);assert.equal(C.equipped(s,p).fang,2);assert.equal(I.equipmentCopy(s,b.copyId),null);C.validate(s);
});
test('equipping health gear never heals and removal clamps only when needed',()=>{
  const s=save(),p=s.run.monkey,c=I.awardGearCopy(s,'heart');p.hp=5;C.equip(s,c.copyId,p.id);assert.equal(p.hp,5);
  p.hp=C.stats(p,C.equipped(s,p)).hp;C.unequip(s,'charm',p.id);assert.equal(p.hp,C.stats(p).hp);
});
test('battle locks block equipping, upgrades and element changes',()=>{
  const s=save(),c=I.awardGearCopy(s,'fang');s.run.phase='battle';assert.throws(()=>C.equip(s,c.copyId,s.run.activeId),/between/);assert.throws(()=>I.upgradeEquipmentCopy(s,c.copyId),/between/);assert.throws(()=>I.setEquipmentChoice(s,'prismCompass','fire'),/between/);
});
test('full resulting loadouts feed power rather than item subtraction',()=>{
  const s=save(),p=s.run.monkey,c=I.awardGearCopy(s,'heart');const view=I.previewEquipmentLoadout(s,c.copyId,p.id,'weapon');assert.ok(monkeyPower(p,view.after)>monkeyPower(p,view.before));C.equip(s,c.copyId,p.id,'weapon');assert.deepEqual(C.equipped(s,p),view.after);
});
