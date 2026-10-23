import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as I from './equipment-inventory.mjs';
import * as UI from './inventory-ui.mjs';
import {renderEquipmentSlots,renderForgeOdds} from './forge-ui.mjs';
import {previewForge} from './forge.mjs';
import {monkeyPower} from './power.mjs';

function fixture(){
  const s=C.newSave('Inventory checks','capuchin','practice',12345);s.meta.gear={heart:1,bambooKnuckles:1,glacierAegis:1};I.normalizeEquipmentInventory(s);
  const add=(itemId,tier=1,flags={})=>{const id=`gear-copy-${++s.meta.equipmentSerial}`;s.meta.equipmentCopies[id]={id,itemId,tier,favorite:false,protected:false,isNew:true,acquiredAt:s.meta.equipmentSerial,...flags};s.meta.gear[itemId]=Math.max(s.meta.gear[itemId]||0,tier);return id;};
  return {s,add};
}
test('inventory groups copies only at matching item and rank, preserving assignments and flags',()=>{
  const {s,add}=fixture(),p=s.run.monkey,first=I.inspectGearCopies(s,'bambooKnuckles')[0];C.equip(s,first.id,p.id,'weapon');add('bambooKnuckles',1,{protected:true});add('bambooKnuckles',2,{favorite:true});
  const groups=UI.inventoryGroups(s).filter(g=>g.itemId==='bambooKnuckles');assert.equal(groups.length,2);assert.equal(groups.find(g=>g.tier===1).copies.length,2);assert.equal(groups.find(g=>g.tier===1).copies.filter(c=>c.ownerId).length,1);assert.equal(groups.find(g=>g.tier===2).copies[0].favorite,true);
});
test('effect search and combined filters keep owned default and can expose undiscovered collection',()=>{
  const {s}=fixture();assert.deepEqual(UI.inventoryGroups(s,{search:'shield',rarities:['epic'],affinities:['ice']}).map(g=>g.itemId),['glacierAegis']);assert.equal(UI.inventoryGroups(s,{search:'shield',affinities:['fire']}).length,0);assert.ok(UI.inventoryGroups(s,{collection:'all'}).some(g=>g.tier===0));assert.ok(UI.inventoryGroups(s).every(g=>g.copies.length));
  const p=s.run.monkey,copy=I.inspectGearCopies(s,'heart')[0];C.equip(s,copy.id,p.id,'armor');assert.deepEqual(UI.inventoryGroups(s,{status:'equipped'}).map(g=>g.itemId),['heart']);
});
test('comparison computes complete resulting stats and power and distinguishes passive gain',()=>{
  const {s}=fixture(),p=s.run.monkey;C.equip(s,I.inspectGearCopies(s,'heart')[0].id,p.id,'armor');const candidate=I.inspectGearCopies(s,'glacierAegis')[0],preview=UI.equipmentComparison(s,candidate.id,p.id,'weapon');
  assert.deepEqual(preview.afterStats,C.stats(p,{heart:1,glacierAegis:1},{team:C.team(s),includeConditional:true}));assert.equal(preview.afterPower,monkeyPower(p,{heart:1,glacierAegis:1}));assert.ok(preview.afterPower>preview.beforePower);assert.ok(preview.gained.some(text=>/shield/i.test(text)));assert.equal(preview.lost.length,0);
  const beforeHp=p.hp;UI.renderItemDetails(s,{itemId:candidate.itemId,copyId:candidate.id,monkeyId:p.id,slot:'weapon'});assert.equal(p.hp,beforeHp);
});
test('details expose copy safety, written rarity, full comparison, and all Ultra equipment slots',()=>{
  const {s,add}=fixture(),p=s.run.monkey;add('glacierAegis',1,{protected:true});const copy=I.inspectGearCopies(s,'glacierAegis')[0];let html=UI.renderItemDetails(s,{itemId:copy.itemId,copyId:copy.id,monkeyId:p.id,slot:'armor'});assert.match(html,/Epic · Ice · Rank 1/);assert.match(html,/protected or favorited/);assert.match(html,/Complete loadout/);assert.match(html,/data-action="inventory-upgrade"[^>]*disabled/);
  const ultra={...p,species:Object.keys(C.SPECIES).find(id=>C.SPECIES[id].rarity==='ultra')};html=renderEquipmentSlots(s,ultra);assert.equal((html.match(/class="equipment-slot /g)||[]).length,4);assert.match(html,/Ultra Slot/);assert.equal((renderEquipmentSlots(s,p).match(/class="equipment-slot /g)||[]).length,3);
});
test('possible items uses overall lottery probabilities and every outcome opens full details',()=>{
  const {s}=fixture();s.run.fruit=20;const mix={fruit:8},preview=previewForge(s,mix),html=renderForgeOdds(s,mix);assert.equal(preview.rarityOdds.length,6);assert.match(html,/actual overall chance/);assert.match(html,/data-from-odds="true"/);assert.match(html,/compact-outcome-grid/);
});
test('cap examples use actual battle results and inspection leaves saved monkeys untouched',()=>{
  const {s}=fixture(),p=s.run.monkey,before=structuredClone(p),damage=UI.equipmentCapExamples(p,{cinderOpener:12,nightfangTooth:12,infernoSignet:12}),reduction=UI.equipmentCapExamples(p,{duskBand:1,heartOfTheFirstMountain:1}),shield=UI.equipmentCapExamples(p,{glacierAegis:12,sunweaverSeal:12,rimeScarf:12});
  assert.ok(damage.some(row=>row.kind==='damage'&&row.raw>.4&&row.effective===.4));assert.ok(reduction.some(row=>row.kind==='reduction'&&Math.abs(row.raw-.35)<1e-10&&row.effective===.3));assert.ok(shield.some(row=>row.kind==='shield'&&row.raw>row.effective));assert.deepEqual(p,before);
});
test('upgraded descriptions show exact numeric effects while preserving thresholds, limits and counts',()=>{
  assert.match(UI.rankedEquipmentDescription('heartbloomLocket',12),/below 35% health, heal 18.8% maximum health, once per battle/);
  assert.match(UI.rankedEquipmentDescription('oceanmemoryOrb',12),/94% of its health damage/);assert.match(UI.rankedEquipmentDescription('oceanmemoryOrb',12),/capped at 15%/);
  assert.match(UI.rankedEquipmentDescription('campwalkerBrooch',12),/2.376%/);assert.match(UI.rankedEquipmentDescription('foragersPouch',12),/12 ordinary resource discoveries/);assert.match(UI.rankedEquipmentDescription('trailforgersPrism',12),/every five common ingredients/);
});
test('compass selectors show only a real eligible saved choice and explain inactive Prism selections',()=>{
  const {s,add}=fixture();add('prismCompass');add('trailfinderCompass');
  const render=itemId=>UI.renderItemDetails(s,{itemId},{eligibleElements:['leaf','shadow']}),select=(html,id)=>html.match(new RegExp(`<select data-equipment-choice="${id}"[^>]*>([\\s\\S]*?)</select>`))[1];
  for(const id of ['prismCompass','trailfinderCompass'])assert.match(select(render(id),id),/<option value="" disabled selected>Choose an element/);
  s.meta.equipmentChoices={prismCompass:'shadow'};assert.match(select(render('prismCompass'),'prismCompass'),/<option value="shadow" selected>Shadow/);assert.doesNotMatch(select(render('prismCompass'),'prismCompass'),/disabled selected/);
  s.meta.equipmentChoices.prismCompass='fire';const html=render('prismCompass');assert.match(select(html,'prismCompass'),/<option value="" disabled selected>Choose an element/);assert.match(html,/saved Fire choice is unavailable in the current resource pool/);assert.equal(s.meta.equipmentChoices.prismCompass,'fire');
});
