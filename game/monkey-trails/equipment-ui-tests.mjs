import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as I from './equipment-inventory.mjs';
import * as UI from './equipment-ui.mjs';
import * as F from './forge.mjs';

const save=()=>C.newSave('Equipment UI','capuchin','practice',1435);
function extra(s,id='reserve'){const p=structuredClone(s.run.monkey);p.id=id;p.nickname='Sanctuary friend';p.equipment={weapon:null,armor:null,charm:null};p.equipmentCopies={};s.collection[id]=p;return p;}

test('direct slots and team overview illustrate every slot and open the specific monkey',()=>{
  const s=save(),p=s.run.monkey,copy=I.awardGearCopy(s,'fang');C.equip(s,copy.copyId,p.id,'weapon');
  const html=UI.renderEquipmentSlots(s,p),compact=UI.renderCompactSlots(s,p);
  for(const label of ['Weapon','Armor','Charm']){assert.ok(html.includes(label));assert.ok(compact.includes(label));}
  assert.equal((compact.match(/data-action="gear-picker"/g)||[]).length,3);assert.match(html,/rarity-common/);assert.match(html,/Brawler|Fang|fang/i);assert.ok(html.includes(`data-monkey="${p.id}"`));assert.ok((html.match(/<svg/g)||[]).length>=3);
});

test('copy picker preserves rank, owner and protections and has one clear Equip action',()=>{
  const s=save(),p=s.run.monkey,reserve=extra(s),one=I.awardGearCopy(s,'heart');C.equip(s,one.copyId,reserve.id,'charm');
  const two=I.awardGearCopy(s,'heart');I.setEquipmentCopyFlag(s,two.copyId,'favorite',true);
  const html=UI.renderGearPicker(s,{monkeyId:p.id,slot:'charm',copyId:one.copyId,element:'all',rarity:'all'});
  assert.equal((html.match(/data-action="gear-select"/g)||[]).length,2);assert.ok(html.includes(one.copyId));assert.ok(html.includes(two.copyId));assert.match(html,/Sanctuary friend/);assert.match(html,/Moves this copy/);assert.equal((html.match(/data-action="gear-equip"/g)||[]).length,1);assert.doesNotMatch(html,/<select[^>]*data-equipment-monkey/);
});

test('comparison uses whole actual loadouts and reports positive and negative changes',()=>{
  const s=save(),p=s.run.monkey,hp=I.awardGearCopy(s,'heart'),atk=I.awardGearCopy(s,'fang');C.equip(s,hp.copyId,p.id,'weapon');
  const view=UI.compareEquipment(s,atk.copyId,p.id,'weapon');assert.deepEqual(view.beforeStats,C.stats(p,{heart:1}));assert.deepEqual(view.afterStats,C.stats(p,{fang:1}));
  const html=UI.renderEquipmentComparison(view);assert.match(html,/stat-gain/);assert.match(html,/stat-loss/);assert.match(html,/\+2/);assert.match(html,/-9/);
  p.hp=3;C.equip(s,atk.copyId,p.id,'weapon');assert.equal(p.hp,3);assert.equal(I.equipmentCopy(s,hp.copyId).ownerId,null);
});

test('moving a selected copy among sanctuary monkeys preserves the replaced copy',()=>{
  const s=save(),p=s.run.monkey,reserve=extra(s),fang=I.awardGearCopy(s,'fang'),heart=I.awardGearCopy(s,'heart');
  C.equip(s,fang.copyId,p.id,'weapon');C.equip(s,heart.copyId,reserve.id,'weapon');
  const html=UI.renderEquipRoster(s,{copyId:fang.copyId,monkeyId:reserve.id,slot:'weapon'},{portrait:monkey=>`<svg aria-label="${monkey.nickname||'Team monkey'}"></svg>`});
  assert.match(html,/Sanctuary/);assert.match(html,/data-action="gear-roster-monkey"/);assert.match(html,/data-action="gear-roster-slot"/);assert.match(html,/returns to your pack/);assert.equal((html.match(/data-action="gear-equip"/g)||[]).length,1);
  C.equip(s,fang.copyId,reserve.id,'weapon');assert.equal(p.equipment.weapon,null);assert.equal(I.equipmentCopy(s,heart.copyId).ownerId,null);assert.equal(I.equipmentCopy(s,fang.copyId).ownerId,reserve.id);C.validate(s);
});

test('craft reveal uses real item art and offers roster and keep with save lock',()=>{
  const s=save();s.run.fruit=20;F.craftForge(s,{fruit:8},'forge-1');const html=UI.renderForgeResult(s,s.run.monkey),pending=UI.renderForgeResult(s,s.run.monkey,{unsaved:true});
  assert.match(html,/<svg/);assert.match(html,/SMELT COMPLETE/);for(const action of ['forge-equip-roster','forge-keep','forge-skip-reveal'])assert.ok(html.includes(`data-action="${action}"`));
  assert.match(pending,/SAVE PENDING/);assert.match(pending,/data-action="forge-equip-roster"[^>]*disabled/);
});

test('rarity and element filters act on individual copies without changing inventory',()=>{
  const s=save(),p=s.run.monkey;I.awardGearCopy(s,'embergrip');I.awardGearCopy(s,'mossVest');const before=JSON.stringify(s),html=UI.renderGearPicker(s,{monkeyId:p.id,slot:'armor',element:'fire',rarity:'common'});
  assert.match(html,/Embergrip Wraps/);assert.doesNotMatch(html,/Mosstitch Vest/);assert.equal(JSON.stringify(s),before);
});
