import test from 'node:test';
import assert from 'node:assert/strict';
import {GEAR} from './data.mjs';
import {EXPANSION_GEAR,ORIGINAL_FORGE_GEAR,GEAR_RARITIES} from './equipment-data.mjs';
import {equipmentIconSvg,EXPANSION_ICON_IDS} from './equipment-icons.mjs';
import {equipmentPercentBonuses,applyEquipmentStatPercentages} from './equipment-effects.mjs';
import * as E from './equipment-battle.mjs';

const attack={power:20,type:'neutral'},fire={power:20,type:'fire'},ice={power:20,type:'ice'},support={energy:2},healing={heal:.2};
function setup(gear={},enemyGear={},otherGear={}){
 const make=(id,equipment)=>({id,hp:1000,maxHp:1000,shield:0,energy:5,types:['leaf'],gear:equipment,combatStats:{...applyEquipmentStatPercentages({hp:1000,atk:100,def:100,speed:100,maxEnergy:10},equipment),hp:1000}});
 const a=make('a',gear),c=make('c',otherGear),d=make('d',{}),e=make('e',enemyGear),team=[a,c,d],b={};d.types=['fire'];c.types=['water'];
 E.lockEquipmentBattle(b,[team,[e]],p=>p.gear,p=>p.combatStats);return{b,a,c,d,e,team};
}
const state=(b,p)=>b.equipmentBattle.units[p.equipmentBattleKey];
const hit=(b,a,e,damage=100,move=attack,effectiveness=1)=>E.equipmentApplyHit(b,a,e,move,damage,{effectiveness});

test('sixty genuinely additional items have six balanced rarities, affinity and individual vector icons',()=>{
 assert.equal(Object.keys(ORIGINAL_FORGE_GEAR).length,40);assert.equal(Object.keys(GEAR).length,105);assert.equal(Object.keys(EXPANSION_GEAR).length,60);
 assert.equal(Object.values(EXPANSION_GEAR).filter(g=>g.affinity===null).length,12);
 assert.equal(new Set(Object.values(GEAR).map(g=>g.name)).size,105);
 assert.deepEqual(Object.keys(GEAR_RARITIES),['common','uncommon','rare','epic','legendary','mythic']);
 for(const rarity of Object.keys(GEAR_RARITIES))assert.equal(Object.values(EXPANSION_GEAR).filter(g=>g.rarity===rarity).length,10);
 assert.equal(EXPANSION_ICON_IDS.length,60);assert.equal(new Set(EXPANSION_ICON_IDS.map(id=>equipmentIconSvg(id))).size,60);
 for(const[id,item]of Object.entries(EXPANSION_GEAR)){assert.ok(item.desc&&item.summary&&item.acquisition&&item.purpose.length,id);assert.match(equipmentIconSvg(id),/^<svg/);assert.equal(!!item.affinity,item.element!=='neutral');}
});

test('stat percentages add within category, signed speed cost applies, and team requirements are explicit',()=>{
 assert.equal(equipmentPercentBonuses({bambooKnuckles:1,infernoSignet:1,sunforgeCrown:1}).atk,.22);
 assert.deepEqual(applyEquipmentStatPercentages({hp:100,atk:100,def:100,speed:100,maxEnergy:10},{quakeboundBracers:1,riverglassShell:1}),{hp:105,atk:112.00000000000001,def:100,speed:96,maxEnergy:10});
 const{a,team}=setup();assert.equal(equipmentPercentBonuses({wanderkingCrown:1},{p:a,team,includeConditional:true}).atk,.1);
 team[1].types=['leaf'];assert.equal(equipmentPercentBonuses({wanderkingCrown:1},{p:a,team,includeConditional:true}).atk,0);
 a.hp=400;assert.equal(equipmentPercentBonuses({charcoalHeart:1,rootstoneCuffs:1},{p:a,includeConditional:true}).atk,.1);
 assert.equal(equipmentPercentBonuses({rootstoneCuffs:1},{p:a,includeConditional:true}).def,0);
});

test('first attack and target conditions add, then the equipment attack cap applies',()=>{
 const{b,a,e}=setup({cinderOpener:1,infernoSignet:1,nightfangTooth:1,emberFang:1});a.hp=500;
 const before=JSON.stringify(b),preview=E.equipmentAttackDetails(b,a,e,fire);assert.equal(JSON.stringify(b),before);assert.equal(preview.bonus,.4);assert.equal(preview.capped,true);
 assert.equal(E.equipmentAttackMultiplier(b,a,e,fire),1.4);assert.equal(E.equipmentAttackMultiplier(b,a,e,fire),1.15);
 e.hp=300;assert.equal(E.equipmentAttackMultiplier(b,a,e,fire),1.05);
 const moon=setup({moonfangTalon:1});moon.e.hp=349;assert.equal(E.equipmentAttackMultiplier(moon.b,moon.a,moon.e,attack),1.15);
});

test('first hit, healthy mountain and conditional protection share a 30 percent cap and require damage',()=>{
 const{b,a,e}=setup({duskBand:1,heartOfTheFirstMountain:1,frostButton:1});
 hit(b,e,a,0,ice);assert.equal(state(b,a).used.firstHitReduction,undefined);
 const first=hit(b,e,a,100,ice);assert.equal(first.reduction,.3);assert.equal(first.damage,70);
 assert.equal(hit(b,e,a,100,ice).damage,92);
 const wounded=setup({quakehideBelt:1});wounded.a.hp=399;assert.equal(hit(wounded.b,wounded.e,wounded.a).damage,90);
});

test('first deployment, voluntary entry and knockout replacement are distinct and limits survive reload',()=>{
 const{b,a,c,e,team}=setup({tempestSpurs:1,glacierAegis:1,tidecallShell:1,eyeOfTheTempest:1});a.types=['ice'];a.hp=500;
 E.equipmentEnter(b,a,team);assert.equal(a.shield,160);assert.equal(a.hp,500);assert.equal(E.equipmentBattleStats(b,a,a.combatStats,team).speed,122);
 E.equipmentEnter(b,a,team,{reason:'voluntary'});assert.equal(a.hp,550);
 E.equipmentEnter(b,a,team,{reason:'knockout'});assert.equal(E.equipmentBattleStats(b,a,a.combatStats,team).speed,137);
 const reload=JSON.parse(JSON.stringify({b,a,c,e,team}));E.lockEquipmentBattle(reload.b,[reload.team,[reload.e]],()=>({}));
 E.equipmentEnter(reload.b,reload.a,reload.team,{reason:'voluntary'});assert.equal(reload.a.hp,550);assert.equal(reload.a.shield,160);
 assert.equal(E.equipmentAttackMultiplier(reload.b,reload.a,reload.e,attack),1.2);
 assert.equal(E.equipmentAttackMultiplier(reload.b,reload.a,reload.e,attack),1);
});

test('relay bonuses occur once on a voluntary switch and worldroot is once per team',()=>{
 const{b,a,c,d,team}=setup({staticRelayCharm:1,dawnRelayBell:1,canopyHeart:1,thunderheadCrest:1,worldrootHeart:1},{},{worldrootHeart:1});
 a.hp=c.hp=d.hp=500;E.equipmentSwitch(b,a,c,team,{voluntary:false});assert.equal(c.hp,500);assert.equal(c.shield,0);
 E.equipmentSwitch(b,a,c,team);assert.equal(c.hp,640);assert.equal(a.hp,580);assert.equal(c.shield,120);assert.equal(E.equipmentBattleStats(b,c,c.combatStats,team).speed,112);
 assert.equal(E.equipmentAttackMultiplier(b,c,d,attack),1.18);E.equipmentSwitch(b,c,a,team);assert.equal(a.hp,580,'second worldroot bearer cannot repeat team heal');
 E.equipmentSwitch(b,a,c,team);assert.equal(c.hp,640);assert.equal(c.shield,120);
});

test('equipment shields combine to a 30 percent capacity while ability shields are retained',()=>{
 const{b,a,e,team}=setup({glacierAegis:12,frostguardBrooch:12,permafrostMantle:12});a.hp=350;a.shield=100;
 E.equipmentEnter(b,a,team);hit(b,e,a,100,fire,2);
 assert.ok(state(b,a).equipmentShield<=300);assert.ok(a.shield>0);
 const capped=setup({glacierAegis:12,rimeScarf:12});E.equipmentEnter(capped.b,capped.a,capped.team);hit(capped.b,capped.a,capped.e,10,ice);assert.equal(capped.a.shield,300);
});

test('Duskglass increases only shield damage and never increases health spill',()=>{
 const{b,a,e}=setup({duskglassLens:1});e.shield=56;const result=hit(b,a,e,100);assert.equal(result.absorbed,56);assert.equal(result.damage,50);assert.equal(e.hp,950);
 const plain=setup({duskglassLens:1});assert.equal(hit(plain.b,plain.a,plain.e,100).damage,100);
});

test('legacy round shields and new shields share capacity while shield attack bonuses share the attack cap',()=>{
 const{b,a,e,team}=setup({atlasShell:12,glacierAegis:12,rimeScarf:12});
 E.equipmentApplyRecovery(b,a,a.combatStats);E.equipmentEnter(b,a,team);assert.equal(a.shield,300);assert.equal(state(b,a).equipmentShield,300);
 const mixed=setup({atlasShell:12,glacierAegis:12,rimeScarf:12});mixed.a.shield=100;E.equipmentApplyRecovery(mixed.b,mixed.a,mixed.a.combatStats);E.equipmentEnter(mixed.b,mixed.a,mixed.team);hit(mixed.b,mixed.a,mixed.e,10,ice);assert.equal(mixed.a.shield,400);assert.equal(state(mixed.b,mixed.a).equipmentShield,300);
 const capped=setup({duskglassLens:1,cinderOpener:1,moonfangTalon:1,emberFang:12});capped.e.hp=300;capped.e.shield=1000;
 assert.ok(Math.abs(E.equipmentAttackMultiplier(capped.b,capped.a,capped.e,fire)-1.364)<1e-10);assert.equal(hit(capped.b,capped.a,capped.e,136,fire).absorbed,140,'combined shield-only bonus stays within +40 percent equipment cap');
});

test('Furnace Beads require consecutive completed damaging turns and reset on switching',()=>{
 const{b,a,c,e,team}=setup({furnaceBeads:1});
 for(let i=0;i<2;i++){assert.equal(E.equipmentAttackMultiplier(b,a,e,fire),1);hit(b,a,e,10,fire);E.equipmentTurnEnd(b,a,team);}
 assert.equal(E.equipmentAttackMultiplier(b,a,e,fire),1.15);hit(b,a,e,10,fire);E.equipmentTurnEnd(b,a,team);
 assert.equal(E.equipmentAttackMultiplier(b,a,e,fire),1);E.equipmentTurnEnd(b,a,team);
 hit(b,a,e,10,fire);E.equipmentTurnEnd(b,a,team);E.equipmentSwitch(b,a,c,team);assert.equal(state(b,a).furnaceStreak,0);
});

test('healing and support abilities apply their distinct limits without recursive triggers',()=>{
 const{b,a,c,d,team}=setup({dewdropChalice:1,sunweaverSeal:1,dawnbringerDiadem:1,dawnthreadBracelet:1});a.hp=500;c.hp=300;d.hp=100;
 assert.equal(E.equipmentHealingMultiplier(b,a,healing),1.2);assert.equal(E.equipmentHealingMultiplier(b,a,healing),1);
 E.equipmentAfterAbility(b,a,healing,{team});assert.equal(a.hp,540);assert.equal(d.hp,180);assert.equal(c.hp,300);assert.equal(a.shield,50);
 E.equipmentAfterAbility(b,a,healing,{team});E.equipmentAfterAbility(b,a,healing,{team});assert.equal(a.shield,100);assert.equal(d.hp,180);assert.equal(a.hp,540);
 assert.equal(E.equipmentConsumableHealing({sunseedPendant:1}),1.08);
});

test('Stormstep refreshes through the next turn and temporary buffs expire on wearer turns only',()=>{
 const{b,a,c,team}=setup({stormstepAnklet:1});const storm={power:10,type:'storm'};
 E.equipmentAfterAbility(b,a,storm,{team});E.equipmentTurnEnd(b,a,team);assert.equal(E.equipmentBattleStats(b,a,a.combatStats,team).speed,108);
 E.equipmentTurnEnd(b,c,team);assert.equal(E.equipmentBattleStats(b,a,a.combatStats,team).speed,108);
 E.equipmentAfterAbility(b,a,storm,{team});E.equipmentTurnEnd(b,a,team);assert.equal(E.equipmentBattleStats(b,a,a.combatStats,team).speed,108);
 E.equipmentTurnEnd(b,a,team);assert.equal(E.equipmentBattleStats(b,a,a.combatStats,team).speed,100);
});

test('third-turn, repeated third-turn and delayed ocean recovery do not revive or exceed their limits',()=>{
 const{b,a,e,team}=setup({springbudLocket:1,tidekeeperPearls:1,oceanmemoryOrb:1});a.hp=300;
 hit(b,e,a,100);assert.equal(a.hp,200);E.equipmentTurnEnd(b,a,team);assert.equal(a.hp,250);
 for(let i=1;i<9;i++)E.equipmentTurnEnd(b,a,team);assert.equal(a.hp,410);
 hit(b,e,a,100);E.equipmentTurnEnd(b,a,team);assert.equal(a.hp,310,'ocean recovery cannot refresh');
 a.hp=0;for(let i=0;i<9;i++)E.equipmentTurnEnd(b,a,team);assert.equal(a.hp,0);
});

test('critical-health effects require surviving actual enemy damage; fatal prevention excludes indirect damage',()=>{
 const{b,a,e}=setup({heartbloomLocket:1,tideglassPendant:1,permafrostMantle:1});a.hp=400;
 hit(b,e,a,100);assert.equal(a.hp,400);assert.equal(a.shield,180);
 hit(b,e,a,10000);assert.equal(a.hp,1);hit(b,e,a,100);assert.equal(a.hp,0);
 const indirect=setup({tideglassPendant:1});indirect.a.hp=10;E.equipmentApplyHit(indirect.b,indirect.e,indirect.a,fire,20,{direct:false});assert.equal(indirect.a.hp,0);
});

test('super-effective hit shields and arms the next reduction and next Fire attack exactly once',()=>{
 const{b,a,e}=setup({sunforgeCrown:1},{mountainheartTotem:1,frostguardBrooch:1});
 hit(b,a,e,10,fire,2);assert.equal(e.shield,80);assert.equal(E.equipmentAttackMultiplier(b,a,e,fire),1.25);assert.equal(E.equipmentAttackMultiplier(b,a,e,fire),1);
 assert.equal(hit(b,a,e,100,fire).reduction,.2);assert.equal(hit(b,a,e,100,fire).reduction,0);
 hit(b,a,e,10,fire,2);assert.equal(E.equipmentAttackMultiplier(b,a,e,fire),1);
});

test('Eclipse Talon arms only once on voluntary entry and vulnerability ends at its damaging attack',()=>{
 const{b,a,e,team}=setup({eclipseTalon:1});E.equipmentEnter(b,a,team);assert.equal(hit(b,e,a).damage,100);
 E.equipmentEnter(b,a,team,{reason:'voluntary'});assert.equal(hit(b,e,a).damage,110);assert.equal(E.equipmentAttackMultiplier(b,a,e,support),1);assert.equal(hit(b,e,a).damage,110);
 assert.equal(E.equipmentAttackMultiplier(b,a,e,attack),1.25);assert.equal(hit(b,e,a).damage,100);E.equipmentEnter(b,a,team,{reason:'voluntary'});assert.equal(E.equipmentAttackMultiplier(b,a,e,attack),1);
});

test('Wintercrown requires an enemy shield break, heals once, and adds a two-turn Attack buff',()=>{
 const{b,a,e,team}=setup({wintercrownRelic:1});a.hp=500;a.shield=50;
 hit(b,e,a,60);assert.equal(a.hp,570);assert.equal(E.equipmentBattleStats(b,a,a.combatStats,team).atk,110);
 E.equipmentTurnEnd(b,a,team);assert.equal(E.equipmentBattleStats(b,a,a.combatStats,team).atk,110);E.equipmentTurnEnd(b,a,team);assert.equal(E.equipmentBattleStats(b,a,a.combatStats,team).atk,100);
 a.shield=10;hit(b,e,a,20);assert.equal(a.hp,560);
});

test('Briarloop reflection and Nightsovereign lifesteal use health damage, per-turn caps and no chains',()=>{
 const{b,a,e,team}=setup({briarloopBand:1},{nightsovereignFangs:1,briarloopBand:1});a.hp=1000;e.hp=500;
 assert.equal(hit(b,e,a,500).reflected,30);assert.equal(e.hp,500,'30 reflected then 30 lifesteal; reflected damage cannot reflect itself');
 assert.equal(hit(b,e,a,100).reflected,0);assert.equal(e.hp,500,'same-turn lifesteal cap');
 E.equipmentTurnEnd(b,e,[e]);hit(b,e,a,50);assert.equal(e.hp,499,'5 reflection and 4 healing on next turn');
 const fangs=setup({nightsovereignFangs:1});fangs.a.hp=100;fangs.e.hp=100000;
 for(let i=0;i<7;i++){hit(fangs.b,fangs.a,fangs.e,100);E.equipmentTurnEnd(fangs.b,fangs.a,fangs.team);}assert.equal(fangs.a.hp,140);
});

test('Trailward replacement and Beacon revival work once, with reserve-only revival and no passive chain',()=>{
 const{b,a,c,d,team}=setup({trailwardStandard:1},{},{beaconOfTheFirstDawn:1});a.hp=0;
 assert.deepEqual(E.equipmentKnockout(b,a,team),{revived:true,reserveOnly:true});assert.equal(a.hp,150);assert.equal(E.equipmentRevivedToReserve(b,a),true);
 E.equipmentSwitch(b,a,c,team,{voluntary:false});assert.equal(c.shield,80);assert.equal(E.equipmentAttackMultiplier(b,c,d,attack),1.1);
 d.hp=0;assert.equal(E.equipmentKnockout(b,d,team).revived,false);
 const noChain=setup({beaconOfTheFirstDawn:1});noChain.c.hp=0;state(noChain.b,noChain.c).passiveKnockout=true;assert.equal(E.equipmentKnockout(noChain.b,noChain.c,noChain.team).revived,false);
});

test('equipment snapshots cannot be altered by re-equipping or relocking after JSON resume',()=>{
 const{b,a,e,team}=setup({cinderOpener:1});assert.equal(E.equipmentAttackMultiplier(b,a,e,attack),1.12);
 a.gear={cinderOpener:12,moonfangTalon:12};E.lockEquipmentBattle(b,[team,[e]],p=>p.gear,p=>p.combatStats);assert.equal(E.equipmentAttackMultiplier(b,a,e,attack),1);
 const resume=JSON.parse(JSON.stringify({b,a,e,team}));assert.equal(E.equipmentAttackMultiplier(resume.b,resume.a,resume.e,attack),1);assert.deepEqual(E.equipmentBattleGear(resume.b,resume.a),{cinderOpener:1});
});

test('a real revival permits a later eligible knockout without refreshing spent equipment effects',()=>{
 const{b,a,c,team}=setup({trailwardStandard:1},{},{beaconOfTheFirstDawn:1});a.hp=c.hp=0;assert.equal(E.equipmentKnockout(b,a,team).revived,false);
 a.hp=100;c.hp=100;E.equipmentKnockout(b,a,team);a.hp=0;assert.equal(E.equipmentKnockout(b,a,team).revived,true);assert.equal(state(b,a).used.knockoutShield,true);
});

test('a reserve knockout cannot transfer Trailward to an unrelated future replacement',()=>{
 const{b,a,c,d,team}=setup({trailwardStandard:1});a.hp=0;E.equipmentKnockout(b,a,team,{active:false});E.equipmentEnter(b,c,team,{reason:'knockout'});assert.equal(c.shield,0);assert.equal(E.equipmentAttackMultiplier(b,c,d,attack),1);
});
