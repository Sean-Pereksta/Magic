import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import * as C from './core.mjs';
import {FORGE_GEAR} from './equipment-data.mjs';
import {equipmentBonuses,equipmentEffects,equipmentDamageMultiplier,equipmentSummary,applyEquipmentRecovery,applyEquipmentLifesteal} from './equipment-effects.mjs';
import {damagePreview} from './battle-guide.mjs';
import {newDuel,duelAction,newRaidAttempt,raidAction,snapshotTeam,SOCIAL_COMBAT_VERSION} from './social-combat.mjs';
import {monkeyPower} from './power.mjs';

function save(seed=765){return C.newSave('Equipment test','capuchin','gps',seed);}
function addReserve(s,id='reserve'){
  const p=structuredClone(save(11).run.monkey);p.id=id;s.collection[id]=p;s.run.party.push(id);return p;
}
function battle(s,enemyMove='breathe'){
  teach(s.run.monkey);
  const e=structuredClone(s.run.monkey);e.id='equipment-enemy';e.species='cinderpaw';e.types=['fire'];e.gear={};e.equipment={weapon:null,armor:null,charm:null};e.hp=10000;e.energy=100;e.loadout=['strike','cinder','breathe'];
  s.run.phase='battle';s.run.battle={id:'equipment-battle',enemies:[e],enemy:e,enemyActive:0,round:1,turn:1,ready:true,boss:false,ultra:false,intent:enemyMove,enemyAction:{type:'move',id:enemyMove},rewarded:false,bondBonuses:{},pendingRally:null,enemyRally:null};return s;
}
function equip(s,id,tier=1,p=s.run.monkey){s.meta.gear[id]=tier;C.equip(s,id,p.id);}
function teach(p,id='cinder'){p.learned.push(id);p.loadout=['strike',id,'brace','breathe'];}
function playerDamage(s){return s.run.battle.hitFeedback.find(hit=>hit.side==='player').damage;}
function profile(id,team){return{id,name:id,team};}

test('expanded equipment has supported effects, bounded tiers and usable descriptions',()=>{
  assert.equal(Object.keys(FORGE_GEAR).length,40);
  const supported=new Set(['roundHeal','energyDamage','physicalDamage','elementDamage','elementResist','lifesteal','roundEnergy','roundShield']);
  for(const[id,item]of Object.entries(FORGE_GEAR)){
    assert.ok(['weapon','armor','charm'].includes(item.slot));assert.ok(C.ELEMENTS[item.element]);
    assert.ok(equipmentSummary({[id]:1}).length,id);
    for(const key of Object.keys(item.effects))assert.ok(supported.has(key),`${id}: ${key}`);
    assert.deepEqual(equipmentEffects({[id]:999}),equipmentEffects({[id]:12}));
    for(const value of Object.values(equipmentBonuses({[id]:12})))assert.ok(Number.isFinite(value)&&value>=0&&value<=24);
  }
  const effects=equipmentEffects(Object.fromEntries(Object.keys(FORGE_GEAR).map(id=>[id,12])));
  assert.ok(effects.roundHeal<=.08&&effects.lifesteal<=.25&&effects.roundShield<=.18&&effects.energyDamage<=.60&&effects.roundEnergy<=3);
  assert.ok(Object.values(effects.elementResist).every(value=>value<=.5));
});

test('original stat gear keeps released tier values while new flat bonuses grow gently',()=>{
  const p=save().run.monkey,base=C.stats(p);
  for(const id of ['fang','shell','heart','wind','satchel'])for(const tier of [1,2,12]){
    const item=C.GEAR[id],actual=C.stats(p,{[id]:tier});assert.equal(actual[item.stat],base[item.stat]+item.amount*tier);
  }
  assert.ok(Math.abs(equipmentBonuses({worldroot:12}).hp-18.8)<1e-10);assert.equal(C.stats(p,{worldroot:12}).hp,base.hp+19);
  assert.deepEqual(C.statsForCombatVersion(p,{worldroot:12},2),base);assert.ok(monkeyPower(p,{worldroot:1})>monkeyPower(p));
});

test('equipping and removing items preserve injuries, unique ownership and maximum bounds',()=>{
  const s=save(),p=s.run.monkey,reserve=addReserve(s);p.hp=10;p.energy=3;reserve.hp=12;reserve.energy=2;
  equip(s,'glacierLocket',12);assert.equal(p.hp,10);assert.equal(p.energy,3);
  p.hp=C.stats(p,C.equipped(s,p)).hp;p.energy=C.stats(p,C.equipped(s,p)).maxEnergy;
  C.equip(s,'glacierLocket',reserve.id);assert.equal(p.equipment.charm,null);assert.equal(s.meta.equipped.charm,null);assert.equal(reserve.hp,12);assert.equal(reserve.energy,2);
  assert.equal(p.hp,C.stats(p).hp);assert.equal(p.energy,C.stats(p).maxEnergy);
  reserve.hp=C.stats(reserve,C.equipped(s,reserve)).hp;C.unequip(s,'charm',reserve.id);assert.equal(reserve.hp,C.stats(reserve).hp);assert.equal(reserve.equipment.charm,null);
  battle(s);const before=JSON.stringify(s);assert.throws(()=>C.unequip(s,'charm'),/between encounters/);assert.equal(JSON.stringify(s),before);
});

test('saved gear cannot use the wrong slot or have two monkey owners',()=>{
  const s=save(),reserve=addReserve(s);equip(s,'sapwell');const wrong=structuredClone(s);wrong.run.monkey.equipment.weapon='sapwell';assert.throws(()=>C.validate(wrong),/equipment ownership/);
  reserve.equipment.charm='sapwell';assert.throws(()=>C.validate(s),/equipment ownership/);
});

test('maximum tier fragment upgrades reject before spending any resource',()=>{
  const s=save();s.run.phase='camp';s.meta.gear.fang=12;s.meta.fragments.fang=20;const before=JSON.stringify(s);assert.throws(()=>C.craft(s,'fang'),/maximum tier/);assert.equal(JSON.stringify(s),before);
});

test('damage effects distinguish physical, paid abilities, split elements and resistance',()=>{
  assert.equal(equipmentDamageMultiplier({furnaceCore:1},{},C.MOVES.strike),1);
  assert.equal(equipmentDamageMultiplier({furnaceCore:1},{},C.MOVES.cinder),1.1);
  assert.equal(equipmentDamageMultiplier({faultHammer:1},{},C.MOVES.strike),1.1);
  assert.equal(equipmentDamageMultiplier({faultHammer:1},{},C.MOVES.cinder),1);
  assert.equal(equipmentDamageMultiplier({embergrip:1},{},{power:10,cost:0,types:['fire','water']}),1.02);
  assert.equal(equipmentDamageMultiplier({},{kilnVest:1},C.MOVES.cinder),.92);
  assert.equal(equipmentDamageMultiplier({furnaceCore:1},{},C.MOVES.breathe),1);
});

test('actual expedition hits include energy effects and pure previews bound every result',()=>{
  for(let seed=1;seed<=30;seed++){
    const s=save(seed);teach(s.run.monkey);equip(s,'furnaceCore');battle(s);s.run.battle.enemy.gear={kilnVest:1};
    const control=structuredClone(s);control.meta.gear.furnaceCore=undefined;control.run.monkey.equipment.charm=null;
    const before=JSON.stringify(s),preview=damagePreview(s,'cinder');assert.equal(JSON.stringify(s),before);
    C.turn(s,'cinder');C.turn(control,'cinder');const hit=s.run.battle.hitFeedback.find(row=>row.side==='player');
    assert.ok(hit.damage>=preview.min&&hit.damage<=(hit.critical?preview.criticalMax:preview.max));assert.ok(hit.damage>=playerDamage(control));
  }
  const s=save();equip(s,'furnaceCore');battle(s);const control=structuredClone(s);control.run.monkey.equipment.charm=null;C.turn(s,'strike');C.turn(control,'strike');assert.equal(playerDamage(s),playerDamage(control));
});

test('elemental resistance reduces real incoming expedition damage and appears in preview',()=>{
  const s=save(777);equip(s,'kilnVest');battle(s,'cinder');const control=structuredClone(s);control.run.monkey.equipment.armor=null;
  const preview=damagePreview(s,'cinder',{actor:s.run.battle.enemy,target:s.run.monkey});assert.ok(preview.notes.some(note=>/resistance/.test(note)));
  C.turn(s,'breathe');C.turn(control,'breathe');const hit=s.run.battle.hitFeedback.find(row=>row.side==='enemy'),plain=control.run.battle.hitFeedback.find(row=>row.side==='enemy');
  assert.ok(hit.damage<plain.damage);assert.ok(hit.damage>=preview.min&&hit.damage<=(hit.critical?preview.criticalMax:preview.max));
});

test('between-round equipment recovery benefits only the active conscious monkey',()=>{
  const s=save(),p=s.run.monkey,reserve=addReserve(s),fallen=addReserve(s,'fallen');equip(s,'sapwell',1,p);equip(s,'snowmend',1,reserve);equip(s,'lifegiver',1,fallen);
  p.hp=10;reserve.hp=10;fallen.hp=0;battle(s);const expected=Math.round(C.stats(p,C.equipped(s,p)).hp*.015);C.turn(s,'breathe');
  assert.equal(p.hp,10+expected);assert.equal(reserve.hp,10);assert.equal(fallen.hp,0);assert.ok(s.log.some(line=>/from equipment/.test(line)));
});

test('recovery never revives or overheals and refreshed shields never stack',()=>{
  const p={hp:99,energy:9,shield:0},st={hp:100,maxEnergy:10},gear={oceanheart:12,lifegiver:12};
  applyEquipmentRecovery(p,st,gear);assert.equal(p.hp,100);assert.equal(p.energy,10);const shield=p.shield;
  for(let i=0;i<20;i++)applyEquipmentRecovery(p,st,gear);assert.equal(p.shield,shield);assert.equal(p.hp,100);
  p.shield=50;applyEquipmentRecovery(p,st,gear);assert.equal(p.shield,50,'Do not erase a stronger ability shield');
  p.hp=0;p.energy=0;p.shield=0;assert.deepEqual(applyEquipmentRecovery(p,st,gear),{healing:0,energy:0,shield:0});assert.equal(applyEquipmentLifesteal(p,st,{nightdrink:12},100),0);assert.equal(p.hp,0);
});

test('lifesteal restores only actual HP damage and active energy and shield effects execute',()=>{
  const s=save(),p=s.run.monkey;equip(s,'nightdrink');p.hp=10;battle(s);C.turn(s,'strike');assert.equal(p.hp,10+Math.round(playerDamage(s)*.075));
  const blocked=save();equip(blocked,'nightdrink');blocked.run.monkey.hp=10;battle(blocked);blocked.run.battle.enemy.shield=10000;C.turn(blocked,'strike');assert.equal(blocked.run.monkey.hp,10);
  const recovered=save();equip(recovered,'tidesong');equip(recovered,'atlasShell');recovered.run.monkey.energy=0;battle(recovered);C.turn(recovered,'strike');
  assert.equal(recovered.run.monkey.energy,2);assert.equal(recovered.run.monkey.shield,Math.round(C.playerStats(recovered).hp*.06));
});

test('new social copies apply equipment stats and effects without modifying expedition injuries',()=>{
  const s=save();teach(s.run.monkey);equip(s,'furnaceCore');equip(s,'snowmend');s.run.monkey.hp=10;s.run.monkey.energy=1;const source=JSON.stringify(s),members=snapshotTeam(s),r=newRaidAttempt(members,3,123);
  assert.equal(r.version,SOCIAL_COMBAT_VERSION);assert.equal(r.version,3);r.team[0].hp=10;
  const preview=damagePreview(null,'cinder',{actor:r.team[0],target:r.boss,socialVersion:r.version});raidAction(r,{type:'move',id:'cinder'});
  assert.ok(r.lastHit.damage>=preview.min&&r.lastHit.damage<=(r.lastHit.critical?preview.criticalMax:preview.max));assert.ok(r.team[0].hp>10);assert.equal(JSON.stringify(s),source);
  const old=newRaidAttempt(members,3,123,2);old.team[0].hp=10;raidAction(old,{type:'move',id:'cinder'});assert.equal(old.team[0].hp,10);assert.ok(old.totalDamage<r.totalDamage);
});

test('version-three social gear has one item per slot and one owner per gear ID',()=>{
  const s=save(),reserve=addReserve(s);equip(s,'sapwell');const members=snapshotTeam(s);members[0].gear.furnaceCore=1;
  const r=newRaidAttempt(members);assert.equal(Object.keys(r.team[0].gear).length,1);
  members[1].gear.sapwell=1;assert.throws(()=>newRaidAttempt(members),/unique companion owner/);
  assert.doesNotThrow(()=>newRaidAttempt(members,1,5,2));
});

test('version-three duels and raids replay identical equipment effects after JSON reload',()=>{
  const a=save(),b=save(22);teach(a.run.monkey);teach(b.run.monkey);equip(a,'sapwell');equip(a,'nightdrink');equip(b,'kilnVest');
  const first=newDuel(profile('a',snapshotTeam(a)),profile('b',snapshotTeam(b)),555),second=JSON.parse(JSON.stringify(first));
  first.teams[0][0].hp=second.teams[0][0].hp=20;
  for(let i=0;i<6&&first.status==='active';i++){const action={type:'move',id:i%2?'breathe':'strike'};duelAction(first,first.players[first.turn],action);duelAction(second,second.players[second.turn],action);assert.deepEqual(first,second);}
  const r=newRaidAttempt(snapshotTeam(a),4,88),copy=JSON.parse(JSON.stringify(r));for(let i=0;i<10;i++){raidAction(r,{type:'move',id:'strike'});raidAction(copy,{type:'move',id:'strike'});}assert.deepEqual(r,copy);
});

test('released version-two duel and raid snapshots retain exact pre-equipment transcripts',()=>{
  const a={id:'a',name:'A',team:[{id:'ember',species:'cinderpaw',nickname:'Ember',level:7,stage:2,types:['fire'],learned:['cinder'],loadout:['strike','cinder','brace','breathe'],gear:{fang:2}}]},b={id:'b',name:'B',team:[{id:'fern',species:'mossmop',nickname:'Fern',level:7,stage:2,types:['leaf'],learned:['vine'],loadout:['strike','vine','brace','breathe'],gear:{heart:2}}]};
  const hash=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex'),d=newDuel(a,b,34567,2);
  for(const[uid,id]of [['a','strike'],['b','brace'],['a','strike'],['b','breathe'],['a','cinder']])duelAction(d,uid,{type:'move',id});
  assert.equal(hash(d),'f4b1d8d3b60cebf23bae7da9a84798cf1a83eafe1b2c7354d60ade61e61ad9ef');
  const r=newRaidAttempt(a.team,9,4343,2,1);for(let i=0;i<10;i++)raidAction(r,{type:'move',id:i%2?'strike':'cinder'});
  assert.equal(hash(r),'88d891b318c4765889895b0aa1d25b9ccf127a7262da7d1028146dafc0deea9a');
});
