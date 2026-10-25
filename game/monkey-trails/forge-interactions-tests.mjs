import test from 'node:test';
import assert from 'node:assert/strict';
import {newSave} from './core.mjs';
import {forgeQuantityBounds,chooseForgeQuantity,renderForgeQuantityPicker,attachForgeInteractions} from './forge-interactions.mjs';
import {cachedForgePreview,forgeGridRows,renderForge,renderForgeInventory,renderForgePreview} from './forge-ui.mjs';
import {normalizeEquipmentInventory,awardGearCopy,assignEquipment,setEquipmentCopyFlag} from './equipment-inventory.mjs';

function fixture(){const s=newSave('Furnace','capuchin','practice',12345);s.run.fruit=100;s.run.essence.fire=8;s.run.phase='trail';normalizeEquipmentInventory(s);return s;}

test('a stack of eight offers every quantity one through eight without spending inventory',()=>{
  const s=fixture(),before=JSON.stringify(s),mix={};assert.equal(forgeQuantityBounds(s,mix,'essence:fire').max,8);
  for(let amount=1;amount<=8;amount++){const next=chooseForgeQuantity(s,mix,'essence:fire',amount);assert.equal(next['essence:fire'],amount);assert.deepEqual(mix,{});}
  assert.equal(JSON.stringify(s),before);assert.throws(()=>chooseForgeQuantity(s,mix,'essence:fire',9),/Only 8/);
  const html=renderForgeQuantityPicker(s,mix,'essence:fire');assert.match(html,/min="1" max="8"/);assert.match(html,/Add to Furnace/);assert.match(html,/Cancel/);assert.equal(JSON.stringify(s),before);
});

test('adding and editing a selected stack respects unreserved inventory and furnace capacity',()=>{
  const s=fixture(),initial={'essence:fire':3,fruit:54};assert.equal(forgeQuantityBounds(s,initial,'essence:fire').max,3);
  const next=chooseForgeQuantity(s,initial,'essence:fire',2);assert.equal(next['essence:fire'],5);assert.equal(s.run.essence.fire,8);
  assert.throws(()=>chooseForgeQuantity(s,next,'essence:fire',2),/60 ingredients/);
  assert.equal(forgeQuantityBounds(s,next,'essence:fire',{edit:true}).max,6);
  const edited=chooseForgeQuantity(s,next,'essence:fire',1,{edit:true});assert.equal(edited['essence:fire'],1);assert.equal(forgeQuantityBounds(s,edited,'essence:fire').max,5);
  assert.deepEqual(initial,{'essence:fire':3,fruit:54});assert.equal(s.run.fruit,100);
  for(const value of [0,-1,1.5,NaN,Infinity])assert.throws(()=>chooseForgeQuantity(s,{},'essence:fire',value),/whole quantity/);
  assert.throws(()=>chooseForgeQuantity(s,{},'equipment:fang',1),/no longer/);
});

test('preview cache follows actual mix, inventory and camp terms while unrelated movement leaves odds cached',()=>{
  const s=fixture(),mix={fruit:8,'essence:fire':2},first=cachedForgePreview(s,mix);
  s.run.distance=(s.run.distance||0)+25;assert.equal(cachedForgePreview(s,mix),first);
  const essenceOnly={'essence:fire':4},essencePreview=cachedForgePreview(s,essenceOnly);s.run.fruit++;assert.equal(cachedForgePreview(s,essenceOnly),essencePreview,'unselected resource pickups update the grid without rerunning the odds');
  const changed=cachedForgePreview(s,{...mix,'essence:fire':3});assert.notEqual(changed,first);
  s.run.phase='camp';const camp=cachedForgePreview(s,mix);assert.equal(camp.campBonus,true);assert.notDeepEqual(camp.rarityOdds,first.rarityOdds);
  assert.equal(camp.rarityOdds.reduce((sum,row)=>sum+row.tickets,0),1000000);
  s.run.fruit=0;assert.equal(cachedForgePreview(s,mix).canCraft,false);
});

test('forge filters expose artwork, selected remainder and protected equipment without making it an ingredient',()=>{
  const s=fixture(),copy=awardGearCopy(s,'fang');assignEquipment(s,copy.copyId,s.run.activeId,'weapon');setEquipmentCopyFlag(s,copy.copyId,'favorite',true);
  s.run.forgeMaterials={'essence:fire':2};const mix={'essence:fire':3};
  const essence=forgeGridRows(s,mix,{filter:'essence'});assert.ok(essence.some(row=>row.id==='quality:essence:fire'));assert.equal(essence.find(row=>row.id==='essence:fire').available,5);
  assert.ok(forgeGridRows(s,mix,{filter:'materials'}).every(row=>row.kind!=='equipment'&&!row.id.includes('essence:')));
  assert.ok(forgeGridRows(s,mix,{filter:'favorites',favorites:['essence:fire']}).some(row=>row.id==='equipment:fang'));
  const gear=renderForgeInventory(s,mix,{filter:'equipment'});assert.match(gear,/<svg/);assert.match(gear,/Equipped/);assert.doesNotMatch(gear,/data-forge-drag|draggable="true"/);
  const html=renderForge(s,mix);assert.match(html,/data-forge-drop/);assert.match(html,/data-forge-search/);assert.match(html,/SMELT/);assert.doesNotMatch(html,/id="forge-resource"/);
});

test('element influence uses the actual weighted offering and describes hybrid mixes without promising item bonuses',()=>{
  const s=fixture();s.run.essence.water=8;
  assert.match(renderForgePreview(s,{fruit:8}),/Balanced influence/);
  const hybrid=renderForgePreview(s,{'essence:fire':5,'essence:water':3});assert.match(hybrid,/Fire influence/);assert.match(hybrid,/Mix favors Fire \+ Water/);assert.match(hybrid,/Potential:/);assert.match(hybrid,/Final bonuses depend on the item rolled/);
});

// A small event surface keeps the cancellation test deterministic without a
// browser timer or cloud connection. Browser-forge separately checks real DnD.
function eventSurface(){
  const handlers=new Map(),ghosts=[],classes=()=>({add(){},remove(){},toggle(){}}),host={open:false,querySelector:()=>section};
  const number={value:'3'},range={value:'3',max:'8'},section={dataset:{id:'essence:fire'},closest:selector=>selector==='dialog'?host:null,querySelector:selector=>selector==='[data-forge-picker-number]'?number:selector==='[data-forge-picker-range]'?range:{disabled:false,textContent:''}};
  const root={querySelector:selector=>selector==='[data-forge-quantity-dialog]'?section:null,querySelectorAll:()=>[],addEventListener:(name,handler)=>handlers.set(name,handler),removeEventListener:name=>handlers.delete(name),body:{append:ghost=>ghosts.push(ghost)},elementFromPoint:()=>({closest:()=>({})})};
  const source={isConnected:true,dataset:{forgeDrag:'essence:fire',forgeOrigin:'inventory'},classList:classes(),closest:()=>source,cloneNode:()=>({classList:classes(),style:{},removeAttribute(){},setAttribute(){},remove(){this.removed=true;}})};
  const target=attribute=>({closest(){return this;},hasAttribute:key=>key===attribute,dataset:{}});
  const fire=(name,event={})=>handlers.get(name)?.({preventDefault(){},stopPropagation(){},...event});
  return {root,host,source,ghosts,fire,target,modal(){host.open=true;},closeModal(){host.open=false;fire('close',{target:host});}};
}

test('scroll intent and interrupted touch holds never reserve ingredients; a completed drop accepts immediate confirmation',t=>{
  t.mock.timers.enable({apis:['setTimeout']});const s=fixture(),surface=eventSurface();let mix={},opened=0;
  const controller=attachForgeInteractions({getState:()=>s,getMix:()=>mix,setMix:next=>{mix=next;},modal(){opened++;surface.modal();},closeModal:surface.closeModal,root:surface.root});
  const point=(x=20,y=50)=>({identifier:1,clientX:x,clientY:y}),start=()=>surface.fire('touchstart',{target:surface.source,touches:[point()]});
  start();surface.fire('touchmove',{touches:[point(20,75)]});t.mock.timers.tick(500);surface.fire('touchend',{changedTouches:[point(20,75)]});assert.equal(opened,0);assert.deepEqual(mix,{});assert.equal(surface.ghosts.length,0);
  start();t.mock.timers.tick(340);assert.equal(surface.ghosts.length,1);surface.fire('touchcancel');surface.fire('touchend',{changedTouches:[point()]});assert.equal(surface.ghosts[0].removed,true);assert.equal(opened,0);assert.deepEqual(mix,{});
  start();t.mock.timers.tick(340);surface.fire('touchend',{changedTouches:[point()]});assert.equal(opened,1);
  surface.fire('click',{target:surface.target('data-forge-picker-confirm')});assert.deepEqual(mix,{'essence:fire':3});assert.equal(s.run.essence.fire,8);assert.equal(surface.host.open,false);controller.destroy();
});

test('dismissed quantity popups cannot apply a stale selection after close or Escape',()=>{
  const s=fixture(),surface=eventSurface();let mix={};const controller=attachForgeInteractions({getState:()=>s,getMix:()=>mix,setMix:next=>{mix=next;},modal:surface.modal,closeModal:surface.closeModal,root:surface.root});
  controller.openQuantity('essence:fire');surface.closeModal();surface.fire('click',{target:surface.target('data-forge-picker-confirm')});assert.deepEqual(mix,{});
  controller.openQuantity('essence:fire');surface.fire('cancel',{target:surface.host});surface.fire('click',{target:surface.target('data-forge-picker-confirm')});assert.deepEqual(mix,{});assert.equal(s.run.essence.fire,8);controller.destroy();
});
