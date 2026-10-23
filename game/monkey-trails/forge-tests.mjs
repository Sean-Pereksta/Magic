import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as F from './forge.mjs';
import {FORGE_GEAR,EXPANSION_GEAR,LEGACY_GEAR_AFFINITIES} from './equipment-data.mjs';
import {normalizeEquipmentInventory} from './equipment-inventory.mjs';

const save=()=>{const s=C.newSave('Forge','capuchin','gps',12345);s.run.phase='camp';return s;};
const snapshot=s=>JSON.stringify(s);
const odds=(view,rarity)=>view.rarityOdds.find(row=>row.id===rarity).chance;
function stock(s){
  s.run.fruit=1000;for(const id of Object.keys(C.ELEMENTS))s.run.essence[id]=100;
  for(const id of Object.keys(C.ITEMS))s.run.items[id]=100;s.run.items.charmMaterial=100;
  for(const id of Object.keys(C.GEAR))s.meta.fragments[id]=100;
  for(const id of Object.keys(C.SPECIES))s.meta.bondMaterials[id]=100;
  return s;
}

test('catalogue keeps forty original items and adds sixty items across six rarities and neutral affinity',()=>{
  assert.equal(Object.keys(FORGE_GEAR).length,100);assert.equal(Object.keys(EXPANSION_GEAR).length,60);
  for(const rarity of Object.keys(F.GEAR_RARITIES))assert.equal(Object.values(EXPANSION_GEAR).filter(g=>g.rarity===rarity).length,10);
  assert.equal(Object.values(EXPANSION_GEAR).filter(g=>g.element==='neutral').length,12);
  for(const element of Object.keys(C.ELEMENTS))for(const rarity of Object.keys(F.GEAR_RARITIES))assert.ok(Object.values(FORGE_GEAR).some(g=>g.element===element&&g.rarity===rarity),`${element} ${rarity}`);
  for(const g of Object.values(FORGE_GEAR)){
    assert.ok(['weapon','armor','charm'].includes(g.slot));assert.match(g.desc,/\d|one|first|five/);
    assert.ok(Object.keys(g.bonuses).length+Object.keys(g.effects).length+Object.keys(g.statsPct||{}).length>0);
    assert.ok(Object.values(g.bonuses).every(n=>Number.isFinite(n)&&n>0&&n<=12));
  }
  for(const id of Object.keys(LEGACY_GEAR_AFFINITIES))assert.ok(C.GEAR[id]);
  assert.deepEqual(['fang','shell','heart','wind','satchel'].map(id=>[C.GEAR[id].stat,C.GEAR[id].amount]),[['atk',2],['def',2],['hp',9],['speed',2],['hp',3]]);
});

test('inventory exposes existing resources once and never creates a new balance',()=>{
  const s=stock(save()),before=snapshot(s),rows=F.forgeInventory(s);
  assert.equal(new Set(rows.map(row=>row.id)).size,rows.length);assert.equal(snapshot(s),before);
  for(const kind of ['fruit','essence','item','fragment','bond'])assert.ok(rows.some(row=>row.kind===kind));
  assert.equal(rows.find(row=>row.id==='item:charmMaterial').owned,100);
  const ultra=Object.values(C.SPECIES).find(p=>p.rarity==='ultra');
  assert.equal(rows.find(row=>row.id===`bond:${ultra.id}`).rarity,'legendary');
});

test('empty and underfunded offerings cannot create a free roll',()=>{
  const s=save(),before=snapshot(s);
  for(const inputs of [{},{fruit:0},{fruit:7}]){
    assert.equal(F.previewForge(s,inputs).canCraft,false);assert.throws(()=>F.craftForge(s,inputs,'forge-1'),/at least/);
    assert.equal(snapshot(s),before);
  }
});

test('previews expose the complete exact lottery without touching RNG or save state',()=>{
  const s=stock(save()),before=snapshot(s),p=F.previewForge(s,{fruit:3,'essence:fire':7,'fragment:worldroot':2,'bond:capuchin':1});
  assert.equal(snapshot(s),before);assert.equal(p.canCraft,true);assert.equal(p.outcomes.length,105);
  for(const rows of [p.outcomes,p.rarityOdds,p.elementOdds]){
    assert.equal(rows.reduce((n,row)=>n+row.tickets,0),F.FORGE_LIMITS.tickets);
    assert.ok(Math.abs(rows.reduce((n,row)=>n+row.chance,0)-1)<1e-12);
    assert.ok(rows.every(row=>row.chance>0&&row.chance===row.tickets/F.FORGE_LIMITS.tickets));
  }
  for(const row of p.rarityOdds)assert.equal(row.tickets,p.outcomes.filter(item=>item.rarity===row.id).reduce((n,item)=>n+item.tickets,0));
  for(const row of p.elementOdds)assert.equal(row.tickets,p.outcomes.filter(item=>item.element===row.id).reduce((n,item)=>n+item.tickets,0));
  assert.deepEqual(F.previewForge(s,{'bond:capuchin':1,'fragment:worldroot':2,'essence:fire':7,fruit:3}),p);
});

test('rarer resources monotonically improve rare and legendary odds',()=>{
  const s=stock(save()),ultra=Object.values(C.SPECIES).find(p=>p.rarity==='ultra').id;
  s.run.phase='trail';
  const mixes=[{fruit:8},{'essence:leaf':8},{'bond:capuchin':8},{'fragment:fang':8},{'fragment:sapwell':8},{'fragment:worldroot':8},{[`bond:${ultra}`]:8}];
  const previews=mixes.map(m=>F.previewForge(s,m));
  for(let i=1;i<previews.length;i++)for(const rarity of ['rare','legendary'])assert.ok(odds(previews[i],rarity)>=odds(previews[i-1],rarity));
  assert.ok(odds(previews.at(-1),'legendary')>odds(previews[0],'legendary')*50);
  assert.ok(previews.at(-1).quality<=1);
});

test('more essence of an element monotonically focuses actual item odds',()=>{
  const s=stock(save());let prior=0;
  for(let count=0;count<=52;count++){
    const p=F.previewForge(s,{fruit:8,'essence:water':count}),focus=p.elementOdds.find(row=>row.id==='water').chance;
    assert.ok(focus>=prior);prior=focus;
    assert.equal(p.outcomes.filter(row=>row.element==='water').reduce((n,row)=>n+row.tickets,0),p.elementOdds.find(row=>row.id==='water').tickets);
  }
  assert.ok(prior>.9);
});

test('crafting spends each chosen inventory balance once and leaves other resources alone',()=>{
  const s=stock(save()),before=structuredClone(s),inputs={fruit:2,'essence:fire':2,'item:berry':1,'item:charmMaterial':1,'fragment:fang':1,'bond:capuchin':1};
  const p=F.previewForge(s,inputs),receipt=F.craftForge(s,inputs,F.nextForgeOperationId(s));
  assert.equal(s.run.fruit,before.run.fruit-2);assert.equal(s.run.essence.fire,before.run.essence.fire-2);assert.equal(s.run.essence.water,before.run.essence.water);
  assert.equal(s.run.items.berry,before.run.items.berry-1);assert.equal(s.run.items.charmMaterial,before.run.items.charmMaterial-1);assert.equal(s.run.items.tonic,before.run.items.tonic);
  assert.equal(s.meta.fragments.fang,before.meta.fragments.fang-1);assert.equal(s.meta.bondMaterials.capuchin,before.meta.bondMaterials.capuchin-1);
  assert.equal(receipt.chance,p.outcomes.find(row=>row.id===receipt.itemId).chance);assert.equal(receipt.kind,'discovered');assert.equal(s.meta.gear[receipt.itemId],1);
});

test('operation receipts survive reload, resolve the same roll and never charge twice',()=>{
  const s=stock(save()),inputs={fruit:8},receipt=F.craftForge(s,inputs,F.nextForgeOperationId(s));
  const restored=C.validate(JSON.parse(JSON.stringify(s)));restored.run.phase='trail';const after=snapshot(restored);
  assert.deepEqual(F.craftForge(restored,inputs,receipt.operationId),receipt);assert.equal(snapshot(restored),after);
  assert.throws(()=>F.craftForge(restored,{fruit:9},receipt.operationId),/different mix/);assert.equal(snapshot(restored),after);
  restored.run.phase='camp';F.craftForge(restored,inputs,F.nextForgeOperationId(restored));const second=snapshot(restored);
  assert.throws(()=>F.craftForge(restored,inputs,receipt.operationId),/resolved/);assert.equal(snapshot(restored),second);
});

test('duplicates really upgrade and tier twelve gives explicit recyclable salvage',()=>{
  const s=stock(save()),inputs={fruit:8},first=F.craftForge(s,inputs,'forge-1');
  s.rng=12345;const second=F.craftForge(s,inputs,'forge-2');
  assert.equal(second.itemId,first.itemId);assert.equal(second.kind,'upgraded');assert.equal(second.tier,2);
  for(const id of Object.keys(C.GEAR))s.meta.gear[id]=12;
  normalizeEquipmentInventory(s);for(const row of Object.values(s.meta.equipmentCopies))row.tier=12;
  const before=structuredClone(s),result=F.craftForge(s,{fruit:60},'forge-3');
  assert.equal(result.kind,'salvaged');assert.equal(result.tier,12);assert.equal(s.meta.gear[result.itemId],12);
  assert.ok(result.salvage.count>0);assert.equal(s.meta.fragments[result.itemId],before.meta.fragments[result.itemId]+result.salvage.count);
  assert.ok(result.salvage.value<=result.value*.5);assert.ok(result.salvage.value>=result.value*.3);
  assert.deepEqual(F.validateForgeCheckpoint(JSON.parse(JSON.stringify(s))).meta.forge.last,result);
});

test('invalid amounts, unknown IDs, sequence IDs and overlarge mixes are atomic',()=>{
  const s=stock(save()),before=snapshot(s);
  const bad=[{fruit:-1},{fruit:8.5},{fruit:Infinity},{fruit:NaN},{fruit:61},{'essence:imaginary':8},{'item:bogus':8},{'fragment:bogus':8},{'bond:bogus':8},{fruit:1001},[],null];
  for(const inputs of bad){assert.throws(()=>F.craftForge(s,inputs,'forge-1'));assert.equal(snapshot(s),before);}
  for(const operationId of [undefined,'','forge-0','forge-2','forge-1x',{},1]){assert.throws(()=>F.craftForge(s,{fruit:8},operationId));assert.equal(snapshot(s),before);}
  s.meta.gear.fang=13;const invalid=snapshot(s);assert.throws(()=>F.craftForge(s,{fruit:8},'forge-1'),/invalid tier/);assert.equal(snapshot(s),invalid);
});

test('full copy collections and malformed individual ownership reject before spending or changing RNG',()=>{
  const full=stock(save());normalizeEquipmentInventory(full);
  for(let n=1;n<=4000;n++){const id=`gear-copy-${n}`;full.meta.equipmentCopies[id]={id,itemId:'fang',tier:1,favorite:false,protected:true,isNew:false,acquiredAt:n,source:'test'};}
  full.meta.equipmentSerial=4000;full.meta.gear.fang=1;const before=snapshot(full);
  assert.throws(()=>F.craftForge(full,{fruit:8},'forge-1'),/full/);assert.equal(snapshot(full),before);
  const broken=stock(save());normalizeEquipmentInventory(broken);broken.meta.equipmentCopies.bad={id:'bad',itemId:'fang',tier:99};const badBefore=snapshot(broken);
  assert.throws(()=>F.craftForge(broken,{fruit:8},'forge-1'),/Invalid equipment copy/);assert.equal(snapshot(broken),badBefore);
});

test('trail and camp forging work while active battles and replacements remain locked',()=>{
  const s=stock(save());
  s.run.phase='trail';assert.equal(F.previewForge(s,{fruit:8}).canCraft,true);
  const receipt=F.craftForge(s,{fruit:8},'forge-1');assert.equal(receipt.campBonus,false);assert.equal(receipt.chargedValue,8);
  for(const phase of ['battle','replacement']){
    s.run.phase=phase;const before=snapshot(s),view=F.previewForge(s,{fruit:8});
    assert.equal(view.outcomes.length,105);assert.equal(view.canCraft,false);assert.match(view.reason,/battle/);
    assert.throws(()=>F.craftForge(s,{fruit:8},'forge-2'),/battle/);assert.equal(snapshot(s),before);
  }
});

test('crafting remains available during post-battle recruitment without changing its saved invitation',()=>{
  const s=stock(save());s.run.phase='befriend';s.run.capture={id:'saved-recruitment',candidates:[],attempts:0,claimed:false};const invitation=structuredClone(s.run.capture);
  assert.equal(F.previewForge(s,{fruit:8}).canCraft,true);const receipt=F.craftForge(s,{fruit:8},'forge-1');
  assert.equal(receipt.campBonus,false);assert.equal(receipt.chargedValue,8);assert.deepEqual(s.run.capture,invitation);assert.equal(s.run.phase,'befriend');
  s.run.phase='dead';s.run.fruit=0;assert.equal(F.previewForge(s,{fruit:8}).canCraft,false);const before=snapshot(s);assert.throws(()=>F.craftForge(s,{fruit:8},'forge-2'),/Gather/);assert.equal(snapshot(s),before);
});

test('camps charge fewer ingredients and improve the same offering without changing elemental focus',()=>{
  const trail=stock(save()),camp=stock(save()),inputs={fruit:8,'essence:fire':5,'item:berry':1};trail.run.phase='trail';
  const a=F.previewForge(trail,inputs),b=F.previewForge(camp,inputs);
  assert.deepEqual(a.inputs,b.inputs);assert.equal(a.fingerprint,b.fingerprint);assert.equal(a.value,21);assert.equal(b.value,21);
  assert.deepEqual(a.costInputs,{'essence:fire':5,fruit:8,'item:berry':1});assert.deepEqual(b.costInputs,{'essence:fire':4,fruit:7,'item:berry':1});
  assert.equal(a.chargedValue,21);assert.equal(b.chargedValue,18);assert.equal(b.chargedUnits,12);assert.equal(b.offeredUnits,14);
  assert.equal(a.discount,0);assert.equal(b.discount,.2);assert.equal(a.qualityBoost,0);assert.equal(b.qualityBoost,.1);
  assert.equal(b.baseQuality,a.baseQuality);assert.ok(Math.abs(b.quality-a.quality-.1)<1e-12);
  for(const rarity of ['rare','legendary'])assert.ok(odds(b,rarity)>odds(a,rarity));
  for(const row of a.elementOdds)assert.ok(Math.abs(row.chance-b.elementOdds.find(v=>v.id===row.id).chance)<.00001);
  const before=structuredClone(camp),receipt=F.craftForge(camp,inputs,'forge-1');
  assert.equal(camp.run.fruit,before.run.fruit-7);assert.equal(camp.run.essence.fire,before.run.essence.fire-4);assert.equal(camp.run.items.berry,before.run.items.berry-1);
  assert.equal(receipt.discount,.2);assert.deepEqual(receipt.costSummary,b.costSummary);assert.deepEqual(receipt.costInputs,b.costInputs);
  const restored=C.validate(JSON.parse(JSON.stringify(camp)));restored.run.phase='trail';const after=snapshot(restored);
  assert.deepEqual(F.craftForge(restored,inputs,'forge-1'),receipt);assert.equal(snapshot(restored),after);
});

test('camp rounding never charges zero and the bonus stays bounded at full quality',()=>{
  const s=stock(save()),ultra=Object.values(C.SPECIES).find(p=>p.rarity==='ultra').id;
  const one=F.previewForge(s,{[`bond:${ultra}`]:1});assert.equal(one.canCraft,true);assert.equal(one.chargedUnits,1);assert.equal(one.costInputs[`bond:${ultra}`],1);
  const max=F.previewForge(s,{[`bond:${ultra}`]:60});assert.equal(max.quality,1);assert.equal(max.chargedUnits,48);assert.equal(max.units,60);
  const before=snapshot(s);assert.throws(()=>F.craftForge(s,{fruit:7},'forge-1'),/at least/);assert.equal(snapshot(s),before);
  s.run.fruit=7;const scarce=snapshot(s);assert.equal(F.previewForge(s,{fruit:8}).canCraft,false);
  assert.throws(()=>F.craftForge(s,{fruit:8},'forge-1'),/Gather/);assert.equal(snapshot(s),scarce);
});

test('receipt validation accepts old saves and rejects malformed persisted records',()=>{
  const old=save();assert.equal(F.validateForgeCheckpoint(old),old);assert.equal(F.nextForgeOperationId(old),'forge-1');
  const s=stock(save());F.craftForge(s,{fruit:8},'forge-1');
  for(const change of [r=>r.serial=-1,r=>r.last.itemId='missing',r=>r.last.inputs.fruit=-1,r=>r.last.fingerprint='{}',r=>r.last.chance=NaN,r=>r.last.tier=13,r=>r.last.salvage={count:999}]){
    const bad=structuredClone(s);change(bad.meta.forge);assert.throws(()=>F.validateForgeCheckpoint(bad));
  }
  const valid=C.validate(JSON.parse(JSON.stringify(s)));assert.equal(valid.meta.forge.last.operationId,'forge-1');
  const legacy=structuredClone(s),r=legacy.meta.forge.last;
  for(const key of ['costInputs','chargedUnits','chargedValue','campBonus','discount','qualityBoost','baseQuality','offeredUnits'])delete r[key];
  r.costSummary=r.costSummary.map(row=>({id:row.id,name:row.name,count:row.offeredCount,value:row.offeredValue}));
  assert.equal(F.validateForgeCheckpoint(legacy),legacy);assert.deepEqual(F.craftForge(legacy,{fruit:8},'forge-1'),r);
  for(const change of [r=>r.costInputs.fruit=8,r=>r.chargedValue=8,r=>r.campBonus=false,r=>r.discount=0,r=>r.qualityBoost=0,r=>r.costSummary[0].count=8]){
    const bad=structuredClone(s);change(bad.meta.forge.last);assert.throws(()=>F.validateForgeCheckpoint(bad));
  }
});

test('two identically seeded saves produce the same item, cost, RNG and receipt',()=>{
  const a=stock(save()),b=stock(save()),inputs={fruit:4,'essence:light':4,'fragment:lifegiver':2};
  b.run.startedAt=a.run.startedAt; // Wall-clock creation time is not combat RNG.
  assert.deepEqual(F.craftForge(a,inputs,'forge-1'),F.craftForge(b,{'fragment:lifegiver':2,'essence:light':4,fruit:4},'forge-1'));
  assert.equal(snapshot(a),snapshot(b));
});
