import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as E from './equipment-battle.mjs';
import * as F from './forge.mjs';
import {LIVING_GEAR,GEAR_RARITIES} from './equipment-data.mjs';
import {equipmentIconSvg} from './equipment-icons.mjs';
import {awardGearCopy,assignEquipment} from './equipment-inventory.mjs';
import {equipmentSpecialDiscovery,equipmentLocationBonusChance} from './equipment-trail.mjs';
const battle=(gear={})=>{const a={id:'a',hp:100,maxHp:100,energy:2,shield:0,types:['storm'],status:{},cooldowns:{},gear,combatStats:{hp:100,maxEnergy:10}},e={id:'e',hp:100,maxHp:100,energy:10,shield:0,types:['fire'],status:{},cooldowns:{},gear:{},combatStats:{hp:100,maxEnergy:10}},ally={...structuredClone(a),id:'ally',gear:{}},b={round:1};E.lockEquipmentBattle(b,[[a,ally],[e]],p=>p.gear,p=>p.combatStats);return{a,e,ally,b};};
test('thirty equipment additions have six balanced rarities and unique illustrations in the actual forge pool',()=>{
 assert.equal(Object.keys(LIVING_GEAR).length,30);assert.equal(Object.keys(C.GEAR).length,135);for(const rarity of Object.keys(GEAR_RARITIES))assert.equal(Object.values(LIVING_GEAR).filter(g=>g.rarity===rarity).length,5);
 assert.equal(new Set(Object.keys(LIVING_GEAR).map(id=>equipmentIconSvg(id))).size,30);
 const s=C.newSave('Gear','capuchin','practice',44);s.run.fruit=60;const view=F.previewForge(s,{fruit:8});assert.equal(view.outcomes.length,135);assert.equal(view.outcomes.reduce((n,row)=>n+row.tickets,0),F.FORGE_LIMITS.tickets);for(const id of Object.keys(LIVING_GEAR))assert.ok(view.outcomes.find(row=>row.id===id)?.tickets>0,id);
});
test('first Shadow discount previews without consuming and persists after a battle snapshot reload',()=>{
 const{b,a}=battle({moonthreadSash:1}),move={type:'shadow',cost:5};assert.equal(E.equipmentMoveCost(b,a,move),4);assert.equal(E.equipmentMoveCost(b,a,move),4);assert.equal(E.equipmentMoveCost(b,a,move,{consume:true}),4);assert.equal(E.equipmentMoveCost(JSON.parse(JSON.stringify(b)),a,move),5);assert.equal(E.equipmentMoveCost(b,a,{cost:2,type:'fire'}),2);
});
test('counter equipment activates once, energy caps and cooldowns never go below ready',()=>{
 const{a,b}=battle({echoheartMedallion:1,celestialHourglass:1});a.energy=9;a.cooldowns={one:4,two:1};const first=E.equipmentTacticalEvent(b,a,'counter',{maxEnergy:10});assert.equal(first.energy,1);assert.equal(a.energy,10);assert.deepEqual(a.cooldowns,{one:3,two:1});E.equipmentTacticalEvent(b,a,'counter',{maxEnergy:10});assert.deepEqual(a.cooldowns,{one:3,two:1});
});
test('first shields, team-healing shields, weaken extension and cooldown reduction obey limits',()=>{
 const{a,e,ally,b}=battle({icefernBuckler:1,bloomguardCloak:1,marshcallTotem:1,frostclockRing:1});assert.equal(E.equipmentShieldMultiplier(b,a,{shield:.2},{consume:false}),1.15);assert.equal(E.equipmentShieldMultiplier(b,a,{shield:.2}),1.15);assert.equal(E.equipmentShieldMultiplier(b,a,{shield:.2}),1);
 e.status.weaken=2;a.cooldowns.heal=5;E.equipmentAfterAbility(b,a,{teamHeal:.2,weaken:.2,cooldown:4},{team:[a,ally],target:e,moveId:'heal'});assert.equal(e.status.weaken,3);assert.equal(a.cooldowns.heal,4);assert.equal(a.shield,5);assert.equal(ally.shield,5);
 E.equipmentAfterAbility(b,a,{teamHeal:.2,weaken:.2,cooldown:4},{team:[a,ally],target:e,moveId:'heal'});assert.equal(e.status.weaken,3);assert.equal(a.cooldowns.heal,4);assert.equal(ally.shield,5);
});
test('precision, status combos and interruption share bounded damage bonuses',()=>{
 const{a,e,b}=battle({voltglassTalon:1,eclipseDrum:1,thunderScepter:1});const move={type:'storm',cost:5,power:50,perfectTiming:true,comboActive:true,interrupted:true};assert.ok(Math.abs(E.equipmentAttackMultiplier(b,a,e,move)-1.31)<1e-12);
 const high=battle({voltglassTalon:12,eclipseDrum:12,thunderScepter:12});assert.equal(E.equipmentAttackMultiplier(high.b,high.a,high.e,move),1.4);
});
test('block reflection cannot recurse and stops at the per-turn cap',()=>{
 const{a,e,b}=battle({abyssalMirror:1});a.shield=50;const hit=E.equipmentApplyHit(b,e,a,{type:'fire',power:30},30);assert.equal(hit.damage,0);assert.equal(hit.reflected,3);assert.equal(e.hp,97);a.shield=50;E.equipmentApplyHit(b,e,a,{type:'fire',power:30},30);assert.equal(e.hp,97);
});
test('forge quality gear appears in exact preview and receipt and remains bounded',()=>{
 const s=C.newSave('Forge quality','capuchin','practice',52);s.run.fruit=60;const before=F.previewForge(s,{fruit:8});const copy=awardGearCopy(s,'kingdomForgeheart');assignEquipment(s,copy.copyId,s.run.activeId,'charm');const preview=F.previewForge(s,{fruit:8});assert.equal(preview.gearQualityBoost,.03);assert.ok(preview.rarityOdds.find(row=>row.id==='mythic').chance>before.rarityOdds.find(row=>row.id==='mythic').chance);
 const receipt=F.craftForge(s,{fruit:8},F.nextForgeOperationId(s));assert.equal(receipt.gearQualityBoost,.03);C.validate(JSON.parse(JSON.stringify(s)));
 const c=awardGearCopy(s,'regentsCompass');assignEquipment(s,c.copyId,s.run.activeId,'weapon');assert.equal(equipmentSpecialDiscovery(s),.08);const idol=awardGearCopy(s,'omnifruitIdol');assignEquipment(s,idol.copyId,s.run.activeId,'armor');assert.equal(equipmentLocationBonusChance(s,{offering:true}),.1);
});
