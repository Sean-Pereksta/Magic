'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function arena(){const c=loadEngine(),g=new c.ATSGame('OUTFITTING');g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;g.world.lineClear=()=>true;g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;for(let i=0;i<8;i++)g.makeApe(i*3,20,'follow');g.command('settle');const s=g.settlements[0];s.wood=s.food=10000;s.attack=false;const w={id:s.id+'-shop',kind:'workShelter',x:50,y:0,stage:4,hp:250,maxHp:250};s.structures.push(w);g.syncIndexes();return {c,g,s,w,e:g.equipment}}
function ape(g,species='gorilla'){const a=g.makeApe(35,25,'follow');a.species=species;g.siege.balance(a,true);g.syncIndexes();return a}
function work(g,n){for(let i=0;i<n;i++){g.time++;g.equipment.tick(1)}}
function batch(o,item,ids,n='all',shop=false){return o.e.orderBatch(o.s.id,shop?o.w.id:o.s.id+':camp',item,ids,n)}

for(const royal of ['wovenRoyal','timberRoyal'])test(royal+' reserves the King against legacy armor purchases until cancelled or fitted',()=>{
 const o=arena(),{g,e,s,w,c}=o;while(g.population<25)ape(g);Object.defineProperty(g,'progression',{value:{tier:2}});
 assert.equal(batch(o,royal,['king'],1).ok,true);const wood=s.wood,food=s.food;
 const entry=e.catalog(s.id,w.id).find(it=>it.kind==='armor');assert.equal(entry.available,false);assert.match(entry.reason,/already.*armor.*queue/);
 assert.equal(e.purchase(s.id,w.id,'armor').ok,false);assert.equal(s.wood,wood);assert.equal(s.food,food);assert.equal(e.queue.length,0);
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize())));Object.defineProperty(loaded,'progression',{value:{tier:2}});
 assert.equal(loaded.equipment.purchase(s.id,w.id,'armor').ok,false);assert.equal(loaded.settlement(s.id).wood,wood);assert.equal(loaded.settlement(s.id).food,food);
 e.cancelBatch(e.batches[0].id);assert.equal(e.catalog(s.id,w.id).find(it=>it.kind==='armor').available,true);
 assert.equal(batch(o,royal,['king'],1).ok,true);work(g,c.ATSOutfitItems[royal].seconds);assert.equal(g.king.outfit.royal,royal);
 assert.equal(e.purchase(s.id,w.id,'armor').ok,true);work(g,25);assert.equal(e.armorLevel,1);assert.equal(g.king.outfit.royal,undefined);
});

test('legacy armor reserves the King against camp fittings without blocking follower batches',()=>{
 const o=arena();Object.defineProperty(o.g,'progression',{value:{tier:2}});assert.equal(o.e.purchase(o.s.id,o.w.id,'armor').ok,true);
 const wood=o.s.wood,food=o.s.food;assert.equal(batch(o,'wovenRoyal',['king'],1).ok,false);assert.equal(o.s.wood,wood);assert.equal(o.s.food,food);
 const a=ape(o.g);assert.equal(batch(o,'bark',[a.id],1).ok,true);o.e.cancel(o.e.queue[0].id);
 assert.equal(o.e.catalog(o.s.id,o.w.id).find(it=>it.kind==='armor').available,true);
});

test('first camp fits early royal armor without healing wounds; legacy upgrades remain exact',()=>{const o=arena(),{g,e,c}=o;g.king.hp=120;assert.equal(batch(o,'wovenRoyal',['king'],1).ok,true);work(g,14);assert.equal(g.king.maxHp,185);assert.equal(g.king.hp,145);assert.equal(e.armor.label,'Woven Royal Guard');assert.equal(batch(o,'wovenRoyal',['king'],1).ok,false);let restored=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize())));assert.equal(restored.king.maxHp,185);assert.equal(restored.king.hp,145);Object.defineProperty(g,'progression',{value:{tier:2}});assert.equal(e.purchase(o.s.id,o.w.id,'armor').ok,true);work(g,25);assert.equal(g.king.maxHp,240);assert.equal(g.king.hp,200);assert.equal(g.king.outfit.royal,undefined);work(g,1);restored=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize())));assert.equal(restored.equipment.armorLevel,1);assert.equal(restored.king.maxHp,240)});

test('batch reserves supplies once, fits physical arrivals and cancels unused materials',()=>{const o=arena(),a=ape(o.g),b=ape(o.g);b.x=500;const wood=o.s.wood;assert.equal(batch(o,'bark',[a.id,b.id,b.id],10).assigned,2);assert.equal(o.e.batches.length,1);assert.equal(o.s.wood,wood-24);assert.equal(batch(o,'leaf',[a.id],1).ok,false);work(o.g,6);assert.equal(a.outfit.body,'bark');assert.equal(b.outfit,undefined);work(o.g,3);assert.equal(o.e.batches[0].progress,0);const x=b.x;o.g.navigation.beginFrame(o.g.time);o.e.updateApe(b,.1);assert.ok(b.x<x);o.e.cancelBatch(o.e.batches[0].id);assert.equal(o.s.wood,wood-12);assert.equal(o.e.batches.length,0)});

test('partial affordability and species restrictions prevent free or impossible gear',()=>{const o=arena(),a=ape(o.g,'gorilla'),b=ape(o.g,'capuchin'),free=ape(o.g);free.state='free';o.s.wood=13;o.s.food=100;const result=batch(o,'club',[a.id,b.id,free.id],10);assert.equal(result.assigned,2);assert.equal(o.s.wood,3);assert.equal(batch(o,'sling',[a.id]).ok,false);assert.equal(batch(o,'elite',[a.id],1,true).ok,false);assert.equal(batch(o,'ironwood',[a.id],1).ok,false)});

test('fitting pauses for raids, new orders, recall, hits and lost stations; ended game stays frozen',()=>{const o=arena(),a=ape(o.g);a.armyOrder={group:'first'};batch(o,'bark',[a.id]);work(o.g,2);const b=o.e.batches[0];assert.equal(b.progress,2);o.s.attack=true;work(o.g,2);assert.equal(b.progress,2);o.s.attack=false;delete a.armyOrder;work(o.g,2);assert.match(o.e.batchState(b),/army orders/);assert.equal(b.progress,2);a.armyOrder={group:'first'};o.g.ended=true;work(o.g,2);assert.equal(b.progress,2)});

test('body armor and utility protection cap, heavy movement and actual stealth are distinct',()=>{const o=arena(),a=ape(o.g,'gorilla'),light=ape(o.g,'gibbon');const damage=20,hp=a.hp;const base=o.g.stealthFactor(a);o.e.fitItem(a,'royalHeavy');o.e.fitItem(a,'royalMantle');const m=o.g.outfitModifiers(a);assert.ok(m.reduction<=.33);assert.ok(m.speed<.9);assert.ok(o.g.stealthFactor(a)>base);o.g.hurt(a,damage,{x:-200,y:0,type:'melee'});assert.ok(a.hp>hp-damage);o.e.fitItem(light,'leaf');o.e.fitItem(light,'camo');assert.ok(o.g.outfitModifiers(light).stealth<.85);const old=o.g.outfitModifiers(a);o.e.fitItem(a,'bark');assert.notEqual(o.g.outfitModifiers(a),old);assert.equal(a.outfit.body,'bark');assert.equal(o.g.outfitModifiers(a).reduction,1-(1-.09)*(1-.06))});

test('weapons, shields, carry rigs and builder gear use real bounded mechanics',()=>{const o=arena(),a=ape(o.g),c=ape(o.g,'capuchin'),base=o.g.apeDamage(a,20),ranged=o.g.apeDamage(c,6);o.e.fitItem(a,'gauntlets');assert.ok(o.g.apeDamage(a,20)>base*1.4);o.e.fitItem(c,'sling');assert.equal(o.g.apeDamage(c,6),ranged*1.25);o.e.fitItem(a,'ironShield');a.dir=0;assert.equal(o.g.siege.absorb(a,50,{type:'bullet',x:a.x+100,y:a.y}),0);assert.equal(a.shield.hp,280);o.e.fitItem(a,'lumber');assert.equal(o.g.outfitCarry(a),24);o.e.fitItem(a,'builder');assert.equal(o.g.outfitModifiers(a).build,1.2);assert.equal(o.g.outfitCarry(a),16);assert.equal(a.outfit.utility,'builder')});

test('batches, loadouts, gear and durability survive repeated restores without refunds or healing',()=>{const o=arena(),a=ape(o.g),b=ape(o.g);o.e.fitItem(a,'bark');o.e.fitItem(a,'buckler');a.shield.hp=33;o.e.saveLoadout('My guards',['leaf','bark','club','scout']);assert.equal(o.e.loadouts['My guards'].items.length,3);batch(o,'club',[b.id]);work(o.g,2);let g=o.g;for(let i=0;i<3;i++)g=o.c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize())));assert.equal(g.apesById.get(a.id).outfit.body,'bark');assert.equal(g.apesById.get(a.id).shield.hp,33);assert.equal(g.equipment.batches[0].progress,2);assert.equal(g.settlement(o.s.id).wood,o.s.wood);assert.equal(g.equipment.loadouts['My guards'].items[0],'leaf');work(g,2);assert.equal(g.apesById.get(b.id).outfit.weapon,'club')});

test('one thousand recipients produce one bounded batch, not one thousand jobs',()=>{const o=arena();while(o.g.apes.length<1000)ape(o.g);const ids=o.g.apes.map(a=>a.id);o.s.wood=o.s.food=100000;const result=batch(o,'vine',ids,'all');assert.equal(result.assigned,1000);assert.equal(o.e.batches.length,1);assert.equal(o.e.queue.length,0);o.e.tick(1);assert.ok(o.e.fitting.size<=4)});

test('medical packs consume one real food and cannot heal under nearby fire',()=>{const o=arena(),a=ape(o.g),b=ape(o.g);o.e.fitItem(a,'medical');b.hp-=10;const hp=b.hp,food=o.s.food;a.settlementId=o.s.id;o.g.apeGrid.rebuild(o.g.apes);o.g.humanGrid.rebuild([]);o.g.vehicleGrid.rebuild([]);o.g.time=20;o.e.tick(1);assert.equal(b.hp,hp+4);assert.equal(o.s.food,food-1);const h=o.g.makeHuman(a.x+20,a.y,null);o.g.humanGrid.rebuild([h]);o.g.time=25;o.e.tick(1);assert.equal(b.hp,hp+4);assert.equal(o.s.food,food-1)});

test('recall issued before a batch begins wins over pending fitting assignments',()=>{const o=arena(),a=ape(o.g);batch(o,'bark',[a.id]);o.g.receiveHordeRecall(a,'field',30);work(o.g,2);assert.equal(o.e.batches[0].progress,0);assert.match(o.e.batchState(o.e.batches[0]),/orders/);assert.equal(o.e.updateApe(a,.1),false)});

test('legacy torches and deterministic royal appearance survive migration',()=>{const o=arena();for(let i=0;i<30;i++)ape(o.g,'gibbon').equipment={torch:true};const g=o.c.ATSGame.fromJSON(JSON.parse(JSON.stringify(o.g.serialize())));assert.equal(g.apes.filter(a=>a.equipment?.torch).length,30);assert.ok(g.equipment.lights().length<=24);assert.equal(g.king.tribeId,'ironback');assert.equal(g.king.appearanceVersion,2);assert.equal(g.tribe.id,'legacy')});

test('broken shields stop adding weight and may be replaced through paid fitting',()=>{const o=arena(),a=ape(o.g);o.e.fitItem(a,'log');assert.ok(o.g.outfitModifiers(a).speed<1);a.shield.hp=0;assert.equal(o.g.outfitModifiers(a).speed,1);assert.equal(o.e.outfitEligible(a,o.c.ATSOutfitItems.log),true);o.e.fitItem(a,'log');assert.equal(a.shield.hp,240);assert.equal(o.e.outfitEligible(a,o.c.ATSOutfitItems.log),false)});
