import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {monkeyPower} from './power.mjs';
import {normalizeEquipmentInventory} from './equipment-inventory.mjs';

const fixture=()=>C.newSave('Balance trail','capuchin','practice',192837);
const close=(actual,expected)=>assert.ok(Math.abs(actual-expected)<1e-8,`${actual} != ${expected}`);
function companion(species,level=1,stage=0){return{species,level,stage,types:[...C.SPECIES[species].types]};}
function battle(targetTypes=[],actorTypes=['fire'],enemyMove='breathe'){
 const s=fixture(),p=s.run.monkey;p.level=10;p.types=[...actorTypes];p.learned.push('cinder');p.loadout=['strike','cinder','brace','breathe'];p.hp=C.stats(p).hp;
 C.advance(s,s.run.nextEncounter);if(s.run.pendingEncounter)C.beginPendingEncounter(s);C.readyBattle(s);const b=s.run.battle;b.enemy.types=[...targetTypes];b.enemy.hp=10000;b.intent=enemyMove;b.enemyAction={type:'move',id:enemyMove};s.rng=192837;
 return s;
}

test('accepted walking restores exactly 2% equipped max HP per 100 feet without chunk or healer bonuses',()=>{
 const a=fixture(),p=a.run.monkey;p.hp=10;a.meta.gear.heart=2;C.equip(a,'heart');const before=p.hp,max=C.playerStats(a).hp,b=structuredClone(a);
 C.advance(a,100*C.FEET);for(let i=0;i<10;i++)C.advance(b,10*C.FEET);
 close(p.hp,before+max*.02);close(p.hp,b.run.monkey.hp);assert.equal(p.training,b.run.monkey.training);
 const healer=C.newSave('Healing trail','langur','practice',3);healer.run.monkey.hp=10;const healerMax=C.playerStats(healer).hp;C.advance(healer,100*C.FEET);close(healer.run.monkey.hp,10+healerMax*.02);
 p.hp=0;C.advance(a,100*C.FEET);assert.equal(p.hp,0,'Walking cannot revive a fallen companion.');
 p.hp=max;C.advance(a,100*C.FEET);assert.equal(p.hp,max);p.hp=10;C.advance(a,50*C.FEET);close(p.hp,10+max*.01);
});

test('all catalogue rarity tiers have distinct permanent power bands at equal level and evolution',()=>{
 const rarities=['common','uncommon','rare','ultra'];assert.equal(Object.keys(C.SPECIES).length,110);
 for(const level of [1,10,60])for(const stage of [0,1,2,3]){
  let previousMax=-1;
  for(const rarity of rarities){
   const powers=Object.values(C.SPECIES).filter(s=>s.rarity===rarity).map(s=>monkeyPower(companion(s.id,level,stage)));
   assert.ok(Math.min(...powers)>previousMax,`Rarity tiers overlap at Lv ${level}, stage ${stage}, ${rarity}.`);previousMax=Math.max(...powers);
  }
 }
});

test('rarity improves growth while role identities, equipment values and mastery remain meaningful',()=>{
 const rare=Object.values(C.SPECIES).find(s=>s.rarity==='rare'),p=companion(rare.id,10,2),base=C.stats(p),geared=C.stats(p,{heart:2,shell:3});
 assert.equal(geared.hp-base.hp,18);assert.equal(geared.def-base.def,6);
 for(const key of ['hp','atk','def'])assert.ok(C.stats(p)[key]>C.statsLegacy(p)[key]);
 const common=companion('capuchin',10,0);assert.deepEqual(C.stats(common),C.statsLegacy(common));
 const defender=C.stats(companion('mandrill')),runner=C.stats(companion('spider'));assert.ok(defender.def>runner.def);assert.ok(runner.speed>defender.speed);
 const mastered={...p,stage:3,types:['fire'],mastery:'fire'},plain={...mastered,mastery:undefined};assert.ok(C.stats(mastered).atk>C.stats(plain).atk);
});

test('every evolution raises all maximum stats and first evolution costs six of each required essence',()=>{
 for(const species of Object.values(C.SPECIES)){
  const s=fixture(),p=s.run.monkey;p.species=species.id;p.types=[...species.types];p.level=10;p.learned=C.unlocked(p);p.loadout=['strike','brace','breathe'];p.hp=C.stats(p).hp;
  for(const element of Object.keys(C.ELEMENTS))s.run.essence[element]=1000;
  for(let stage=0;stage<3;stage++){
   const type=p.types[0]||'leaf',offer=C.evolutionPreview(s,type);assert.ok(offer,species.id);
   if(!stage)for(const amount of Object.values(offer.cost))assert.equal(amount,6);
   for(const key of ['hp','atk','def','speed','maxEnergy'])assert.ok(offer.statGains[key]>0,`${species.id} stage ${stage+1} did not improve ${key}.`);
   const expected=offer.stats;C.evolve(s,type);assert.deepEqual(C.stats(p),expected);
  }
 }
 // Adding a second element must still improve defense after losing pure harmony.
 const s=fixture(),p=s.run.monkey;p.level=10;for(const key of ['leaf','fire'])s.run.essence[key]=100;C.evolve(s,'leaf');const preview=C.evolutionPreview(s,'fire');for(const value of Object.values(preview.statGains))assert.ok(value>0);
});

test('stronger elemental strengths, resistances and bounded mixed defenses drive actual damage',()=>{
 close(C.effectiveness('fire',['leaf']),2.25);close(C.effectiveness('fire',['water']),.4);
 assert.equal(C.effectiveness('fire',['leaf','ice']),4);assert.equal(C.effectiveness('fire',['water','earth']),.25);
 close(C.moveEffect(C.MOVES.wildfire,['water']),(.4+2.25)/2);assert.equal(C.moveEffect(C.MOVES.strike,['leaf','ice']),1);
 const damage=types=>{const s=battle(types);C.turn(s,'cinder');return s.run.battle.hitFeedback.find(h=>h.side==='player').damage;};
 const neutral=damage([]),strong=damage(['leaf']),resisted=damage(['water']);assert.ok(strong>=neutral*2);assert.ok(resisted<neutral*.5);assert.ok(resisted>0);
 const matching=battle([],['fire']),unmatched=battle([],['water']);C.turn(matching,'cinder');C.turn(unmatched,'cinder');assert.ok(matching.run.battle.lastHit.damage>unmatched.run.battle.lastHit.damage);
});

test('effectiveness feedback retains both resolved hits, ignores free items and survives the winning turn',()=>{
 const s=battle(['leaf'],['fire'],'strike'),b=s.run.battle;C.turn(s,'cinder');assert.equal(b.hitFeedback.length,2);assert.deepEqual(b.lastHits,b.hitFeedback);
 const hit=b.hitFeedback.find(h=>h.side==='player');assert.equal(C.effectInfo(hit.multiplier).label,'Very effective!');assert.match(s.log.join('\n'),/Very effective!/);assert.equal(hit.moveId,'cinder');assert.equal(hit.actorName,'Capuchin');
 const before=structuredClone(b.hitFeedback);s.run.items.berry=1;s.run.monkey.hp=10;C.useItem(s,'berry');assert.deepEqual(b.hitFeedback,before);
 assert.throws(()=>C.turn(s,'missing-ability'));assert.deepEqual(b.hitFeedback,before);
 b.enemy.hp=1;b.enemyAction={type:'move',id:'breathe'};b.intent='breathe';C.turn(s,'cinder');assert.equal(s.run.phase,'befriend');assert.equal(s.run.lastBattleFeedback[0].damage,1);assert.deepEqual(s.run.lastBattleHits,s.run.lastBattleFeedback);
 const resisted=battle(['water']);C.turn(resisted,'cinder');assert.equal(C.effectInfo(resisted.run.battle.lastHit.multiplier).label,'Not effective');
 const support=battle();C.turn(support,'breathe');assert.deepEqual(support.run.battle.hitFeedback,[]);
});

test('existing checkpoints preserve injuries, balances, companions and exact legacy online balance',()=>{
 const s=fixture(),p=s.run.monkey;p.hp=17;p.energy=3;p.training=100;p.bond=4;s.run.fruit=21;const original=structuredClone(s),restored=C.validate(structuredClone(s));normalizeEquipmentInventory(original);assert.equal(restored.run.monkey.hp,17);assert.equal(restored.run.monkey.energy,3);assert.equal(restored.run.fruit,21);assert.deepEqual(restored.collection,original.collection);
 const old=C.statsLegacy({species:'capuchin',level:4,stage:2,types:['leaf','fire']},{heart:1});
 assert.deepEqual(old,{hp:C.SPECIES.capuchin.hp+24+28+9,atk:Math.round(C.SPECIES.capuchin.atk+6+4.8),def:Math.round(C.SPECIES.capuchin.def+4.2+3.4),speed:Math.round(C.SPECIES.capuchin.speed+1.95+2),maxEnergy:16});
 assert.equal(C.moveEffectLegacy({type:'fire'},['leaf','water']),1);assert.equal(C.moveEffectLegacy({type:'fire'},['leaf','ice']),4);assert.equal(C.moveEffectLegacy({type:'fire'},['water','earth']),.25);
});
