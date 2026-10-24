import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {ELEMENTAL_TECHNIQUES,SPECIES_SIGNATURES,LIVING_SPECIES_ROWS} from './living-combat-data.mjs';
import {speciesHomes,trackHunt,huntView} from './regional-adventures.mjs';
import {portrait,ANIMATION_STATES} from './art.mjs';

function fixture(move='intercept',species='chronomane'){
 const s=C.newSave('Living combat','capuchin','practice',76192),p=s.run.monkey;
 Object.assign(p,{species,types:[...C.SPECIES[species].types],level:60,stage:3,mastery:C.SPECIES[species].types[0],loadout:['strike',move,'brace','breathe'],learned:[]});C.syncProgression(p);p.hp=C.stats(p).hp;p.energy=C.stats(p).maxEnergy;
 const reserve=structuredClone(p);reserve.id='living-reserve';reserve.loadout=['strike','brace','breathe'];reserve.hp=Math.round(C.stats(reserve).hp*.3);reserve.energy=1;s.collection[reserve.id]=reserve;s.run.party.push(reserve.id);
 C.advance(s,C.ENCOUNTER_STEP,0,0);C.beginPendingEncounter(s);C.readyBattle(s);
 const b=s.run.battle,e=b.enemy;e.hp=10000;e.types=[];e.energy=20;e.loadout=['strike','breathe'];e.status={};e.cooldowns={};e.uses={};b.enemyAction={type:'move',id:'breathe'};b.intent='breathe';
 return{s,p,reserve,b,e};
}
function quiet(b){b.enemyAction={type:'move',id:'breathe'};b.intent='breathe';}

test('twenty appended identities have exclusive signatures, regional capture routes and six articulated animations',()=>{
 assert.equal(Object.keys(C.SPECIES).length,110);assert.equal(Object.keys(ELEMENTAL_TECHNIQUES).length,24);assert.equal(Object.keys(SPECIES_SIGNATURES).length,20);assert.equal(LIVING_SPECIES_ROWS.length,20);
 const signatures=new Set();
 for(const [index,row]of LIVING_SPECIES_ROWS.entries()){
  const p=C.SPECIES[row[0]];assert.equal(p.number,index+91);assert.equal(p.signature,row[12]);assert.ok(p.learnset.includes(p.signature));signatures.add(p.signature);assert.ok(speciesHomes(p.id).includes(p.region));assert.ok(p.minFeet>0);assert.ok(p.hp&&p.atk&&p.def&&p.speed);assert.equal(p.evolutionAppearances.length,3);
  for(const state of ANIMATION_STATES){const svg=portrait({species:p.id,types:p.types,stage:0},{state});assert.match(svg,new RegExp(`data-state="${state}"`));assert.doesNotMatch(svg,/undefined|NaN/);}
  const s=C.newSave('Capture route','capuchin','practice',index+9);s.world.totalDistance=160000;s.world.unlocked=C.LANDS.map(l=>l.id);C.selectLand(s,p.region);trackHunt(s,p.id);const hunt=huntView(s,p.id);assert.ok(hunt&&hunt.homes.some(h=>h.id===p.region));
  if(p.rarity!=='ultra'){s.run.distance=20000*C.FEET;assert.ok(C.encounterPool(s).includes(p.id));}else assert.equal(p.encounterWeight,0);
 }
 assert.equal(signatures.size,20);assert.equal(LIVING_SPECIES_ROWS.filter(row=>row[9]==='ultra').length,4);
});

test('every species gets distinct active requirements at every milestone through level sixty',()=>{
 for(const species of Object.values(C.SPECIES)){
  const p={species:species.id,types:species.types.length?[...species.types]:['leaf'],stage:0,level:1,learned:[],loadout:['strike','brace','breathe']};
  for(const level of[3,6,10,20,30,40,50,60]){
   p.level=level;p.stage=level===3?1:level===6?2:3;const prior=C.unlocked({...p,level:level-1}),current=C.unlocked(p),newMoves=current.filter(id=>!prior.includes(id));
   assert.ok(newMoves.length,`${species.id} has no level ${level} technique`);C.syncProgression(p);
   for(const id of newMoves){const info=C.abilityInfo(p,id);assert.equal(info.requiredLevel,level);assert.equal(info.eligible,true);assert.equal(info.discovered,true);assert.match(info.requirement,new RegExp(`Level ${level}$`));}
  }
  const upgrade=C.MOVES[species.progressionUpgrade];assert.equal(upgrade.owner,species.id);assert.equal(upgrade.minLevel,30);assert.ok(C.unlocked(p).includes('finalMastery'));
 }
});

test('level sixty discoveries and evolution branches survive reset but cannot bypass the active expedition requirement',()=>{
 const {s,p}=fixture('finalMastery','fernflick');C.syncProgression(p);p.discovered=['3:leaf:mastery=leaf'];p.highestForm={stage:3,types:['leaf'],mastery:'leaf'};
 s.run.phase='dead';s.run.battle=null;C.restart(s);assert.equal(p.level,1);assert.equal(p.stage,0);assert.equal(p.highestLevel,60);assert.ok(p.learned.includes('finalMastery'));assert.deepEqual(p.discovered,['3:leaf:mastery=leaf']);
 const info=C.abilityInfo(p,'finalMastery');assert.equal(info.discovered,true);assert.equal(info.eligible,false);assert.equal(info.requirement,'Requires Ascendant + Level 60');assert.ok(!p.loadout.includes('finalMastery'));assert.throws(()=>C.setMove(s,1,'finalMastery',p.id));
 const restored=C.validate(JSON.parse(JSON.stringify(s)));assert.equal(C.abilityInfo(restored.collection[p.id],'finalMastery').discovered,true);
});

test('copied signature history never grants another species exclusive signature or upgrade',()=>{
 const p={species:'fernflick',types:['leaf'],stage:3,level:60,learned:['timeCrown','chronomaneMasterSignature'],loadout:['strike']};
 for(const id of p.learned){assert.equal(C.abilityInfo(p,id).eligible,false);assert.match(C.abilityInfo(p,id).lockedReason,/Exclusive/);}assert.ok(C.unlocked(p).includes('quickleafSlip'));
});

test('late expedition Ultras stay within the capturable level sixty save limit',()=>{
 const s=C.newSave('Apex expedition','capuchin','practice',941);s.run.distance=1e6*C.FEET;assert.equal(C.encounterPlan(s,true).level,60);assert.equal(C.encounterPlan(s).level,60);
});

test('charged attacks pay once, telegraph, survive reload and release despite energy, use and cooldown locks',()=>{
 let {s,p,b,e}=fixture('crownfireComet','sunscar');const energy=p.energy;C.turn(s,'crownfireComet');assert.equal(e.hp,10000);assert.equal(p.charging.resolveRound,2);assert.equal(p.energy,energy-C.MOVES.crownfireComet.cost+1);assert.equal(p.uses.crownfireComet,1);const due=p.cooldowns.crownfireComet;
 s=C.validate(JSON.parse(JSON.stringify(s)));p=s.run.monkey;b=s.run.battle;e=b.enemy;p.energy=0;quiet(b);C.turn(s,'crownfireComet');assert.ok(e.hp<10000);assert.equal(p.charging,undefined);assert.equal(p.uses.crownfireComet,1);assert.equal(p.cooldowns.crownfireComet,due);assert.equal(p.energy,1);assert.ok(b.lastHits.some(h=>h.charged));
});

test('intercept cancels a paid enemy charge before it resolves and records a tactical victory metric',()=>{
 const {s,p,b,e}=fixture('intercept');e.charging={id:'crownfireComet',resolveRound:1,paid:10};b.enemyAction={type:'move',id:'crownfireComet'};b.intent='crownfireComet';const hp=p.hp;
 assert.equal(C.battleIntent(s).charging,true);assert.ok(C.battleOpportunities(s).some(o=>o.kind==='interrupt'));C.turn(s,'intercept');assert.equal(p.hp,hp);assert.equal(e.charging,undefined);assert.equal(b.metrics.interrupts,1);assert.ok(b.lastHits.some(h=>h.interrupted));
});

test('frost timing delays a charge only once and a shielded block still delivers its counter',()=>{
 const {s,p,b,e}=fixture('frostTiming','rimepounce');e.charging={id:'crownfireComet',resolveRound:2,paid:10};b.enemyAction={type:'move',id:'brace'};b.intent='brace';C.turn(s,'frostTiming');assert.equal(e.charging.resolveRound,3);assert.equal(e.charging.delayed,true);
 const t=fixture('stoneCounter','stormforge');t.b.enemyAction={type:'move',id:'strike'};t.b.intent='strike';const eh=t.e.hp;C.turn(t.s,'stoneCounter');assert.ok(t.e.hp<eh);assert.equal(t.b.metrics.counters,1);assert.equal(t.b.metrics.blocks,1);assert.ok(t.p.hp>0);
});

test('optional precision is bounded, misses retain the normal attack, assist is deterministic and online ignores it',()=>{
 const play=timing=>{const {s,b}=fixture('emberJuke','sunscar');C.turn(s,'emberJuke',false,{timing});return b.lastHits.find(h=>h.side==='player');};
 const ordinary=play(null),miss=play({position:.2}),perfect=play({position:.5}),assisted=play({assisted:true});assert.equal(miss.damage,ordinary.damage);assert.ok(Math.abs(perfect.damage-ordinary.damage*1.1)<=1);assert.equal(perfect.perfectTiming,true);assert.equal(assisted.damage,perfect.damage);assert.equal(C.timingResult(.5,{online:true}).bonus,0);
 const {s,b}=fixture('emberJuke','sunscar');b.online=true;C.turn(s,'emberJuke',false,{timing:{assisted:true}});assert.equal(b.lastHits.find(h=>h.side==='player').perfectTiming,false);
});

test('element combos consume setup, feints punish defenses and shields protect every conscious ally',()=>{
 const a=fixture('verdantVerdict','fernflick'),b=fixture('verdantVerdict','fernflick');b.e.status.poison=3;C.turn(a.s,'verdantVerdict');C.turn(b.s,'verdantVerdict');assert.ok(b.b.lastHits[0].damage>a.b.lastHits[0].damage);assert.equal(b.e.status.poison,undefined);assert.equal(b.b.metrics.combos,1);
 const c=fixture('tacticalFeint'),d=fixture('tacticalFeint');d.e.guard=.45;C.turn(c.s,'tacticalFeint');C.turn(d.s,'tacticalFeint');assert.ok(d.b.lastHits[0].damage>c.b.lastHits[0].damage);assert.ok(d.b.lastHits[0].feint);
 const h=fixture('pollenWard','fernflick');h.p.status.burn=2;h.reserve.status.poison=2;C.turn(h.s,'pollenWard');assert.ok(h.p.shield>0);assert.ok(h.reserve.shield>0);assert.equal(h.p.status.burn,undefined);assert.equal(h.reserve.status.poison,undefined);
});

test('telegraphed defensive setups create real feint and guard-break opportunities',()=>{
 const a=fixture('tacticalFeint'),b=fixture('tacticalFeint');b.b.enemyAction={type:'move',id:'ironGuard'};b.b.intent='ironGuard';assert.match(C.battleIntent(b.s).label,/Guard Break/);assert.ok(C.battleOpportunities(b.s).some(o=>o.kind==='guardBreak'));C.turn(a.s,'tacticalFeint');C.turn(b.s,'tacticalFeint');assert.ok(b.b.lastHits[0].damage>a.b.lastHits[0].damage);
 const c=fixture('guardBreak');c.b.enemyAction={type:'move',id:'ironGuard'};c.b.intent='ironGuard';C.turn(c.s,'guardBreak');assert.equal(c.e.shield||0,0);assert.equal(c.b.metrics.guardBreaks,1);
});

test('four Ultra signatures offer different bounded tactics: revive, team recovery, oath counter, tempo disruption',()=>{
 const a=fixture('phoenixCircuit','ashwing');a.reserve.hp=0;C.turn(a.s,'phoenixCircuit');assert.equal(a.p.uses.phoenixCircuit,1);assert.ok(a.reserve.hp>0);assert.ok(a.reserve.hp<=C.stats(a.reserve).hp*.21);assert.throws(()=>C.turn(a.s,'phoenixCircuit'));
 const b=fixture('midnightBloom','nightbloom');b.p.hp=20;b.reserve.hp=20;b.reserve.status.burn=3;C.turn(b.s,'midnightBloom');assert.ok(b.p.hp>20);assert.ok(b.reserve.hp>20);assert.equal(b.reserve.status.burn,undefined);assert.ok(b.p.shield>0);
 const c=fixture('auroraOath','lumenkeeper');c.b.enemyAction={type:'move',id:'strike'};C.turn(c.s,'auroraOath');assert.equal(c.b.metrics.counters,1);assert.ok(c.reserve.shield>0);
 const d=fixture('timeCrown','chronomane');d.e.charging={id:'heavyPalm',resolveRound:2,paid:4};d.p.cooldowns.intercept=5;const enemyEnergy=d.e.energy;C.turn(d.s,'timeCrown');assert.equal(d.e.charging,undefined);assert.equal(d.p.cooldowns.intercept,4);assert.ok(d.e.energy<=enemyEnergy-4);assert.equal(d.b.metrics.interrupts,1);
});

test('healing momentum, reserve splash and mastery finisher reward setup without instant wins',()=>{
 const a=fixture('healingGleam','starweaver');a.p.loadout=['strike','healingGleam','solarJudgment','brace'];a.reserve.hp=1;C.turn(a.s,'healingGleam',false,{targetId:a.reserve.id});assert.ok(a.reserve.hp>1);assert.ok(a.p.healingMomentum>0);a.p.energy=20;quiet(a.b);C.turn(a.s,'solarJudgment');assert.equal(a.p.healingMomentum,0);
 const b=fixture('leviathanPulse','vinewhisk'),enemyReserve=structuredClone(b.e);enemyReserve.id='enemy-reserve';b.b.enemies.push(enemyReserve);C.turn(b.s,'leviathanPulse');assert.ok(enemyReserve.hp<10000);assert.ok(b.e.hp>0);
 const c=fixture('finalMastery');C.turn(c.s,'finalMastery');quiet(c.b);C.turn(c.s,'finalMastery');assert.ok(c.e.hp>0);assert.equal(c.p.uses.finalMastery,1);
});
