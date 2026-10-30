import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as L from './legacy.mjs';
import * as W from './living-world.mjs';
import {regionChallenges,claimChallenge} from './challenges.mjs';
import {adventureSteps,claimAdventure} from './regional-adventures.mjs';
import {awardGearCopy} from './equipment-inventory.mjs';
import {craftForge} from './forge.mjs';
import {snapshotTeam} from './social-combat.mjs';
import {Vault} from './storage.mjs';
import {renderLegacy} from './legacy-ui.mjs';

const start=()=>C.newSave('Legacy walker','capuchin','practice',123456);
const walk=(s,feet)=>{s.run.nextEncounter=1e8;s.run.nextCamp=1e8;return C.advance(s,feet*C.FEET,0,feet*C.FEET);};
function buy(s,id,ranks=1){s.meta.trailWisdom.earned=1000;for(let n=0;n<ranks;n++)C.purchaseLegacyUpgrade(s,id);}
function restart(s){s.run.phase='dead';C.restart(s);return s;}
function battle(s){s.run.nextEncounter=s.run.distance;C.advance(s,1e-7);C.beginPendingEncounter(s);C.readyBattle(s);return s.run.battle;}
function die(s){
 if(s.run.phase==='befriend')C.skipBefriend(s);if(s.run.phase==='camp')C.leaveCamp(s);
 if(s.run.phase==='trail')battle(s);
 for(const p of C.team(s))p.hp=0;s.run.monkey.hp=1;s.run.monkey.status.poison=2;s.run.monkey.loadout=['strike','brace','breathe'];s.run.battle.enemies.forEach(p=>{p.hp=999;p.loadout=['breathe'];});
 C.turn(s,'breathe');assert.equal(s.run.phase,'dead');return C.expeditionRecap(s);
}

test('Trail Wisdom counts only actual accepted walking, survives reload, and continues beyond the old cap',()=>{
 let s=start();walk(s,1999);assert.equal(C.legacyView(s).wisdom,0);
 for(const value of [0,-1,NaN,Infinity])assert.equal(C.advance(s,value),0);
 walk(s,1);assert.equal(C.legacyView(s).wisdom,1);
 s=C.validate(structuredClone(s));walk(s,28000);assert.equal(C.legacyView(s).wisdom,15);
 assert.equal(s.run.legacy.walkingWisdom,15);assert.equal(C.legacyView(s).milestone.capped,false);
 const before=C.legacyView(s).wisdom;s.run.phase='camp';assert.equal(C.advance(s,5000),0);assert.equal(C.legacyView(s).wisdom,before);
 restart(s);assert.equal(C.legacyView(s).wisdom,before);assert.equal(s.run.legacy.earned,0);walk(s,2000);assert.equal(C.legacyView(s).wisdom,before+1);
});

test('five permanent upgrades have increasing costs, rank caps, durable balances and future-only effects',()=>{
 let s=start();assert.equal(C.LEGACY_UPGRADES.length,5);
 for(const row of C.LEGACY_UPGRADES){assert.ok(row.costs.length>=100);assert.ok(row.costs.every((cost,i)=>cost>0&&(!i||cost>=row.costs[i-1])));}
 const before={fruit:s.run.fruit,hp:s.run.monkey.hp,distance:s.run.distance};buy(s,'prepared',4);buy(s,'crew',4);
 assert.deepEqual({fruit:s.run.fruit,hp:s.run.monkey.hp,distance:s.run.distance},before);
 const spent=s.meta.trailWisdom.spent;assert.ok(spent>0);assert.equal(C.legacyView(s).upgrades.find(row=>row.id==='prepared').maxRank,100);
 s=C.validate(structuredClone(s));assert.equal(s.meta.trailWisdom.spent,spent);assert.equal(C.legacyView(s).wisdom,s.meta.trailWisdom.earned-spent);
 restart(s);assert.equal(s.run.fruit,16);assert.equal(Object.values(s.run.essence).reduce((a,b)=>a+b,0),3);
 assert.equal(['trailRibbon','treasureScent','scoutFeather'].reduce((n,id)=>n+s.run.items[id],0),1);
 assert.deepEqual(s.run.monkey.trailCrew,{hp:8,maxEnergy:2});assert.equal(s.run.monkey.hp,C.stats(s.run.monkey).hp);
 const balance=C.legacyView(s).wisdom;s.meta.trailWisdom.earned=s.meta.trailWisdom.spent;assert.throws(()=>C.purchaseLegacyUpgrade(s,'instinct'),/more Trail Wisdom/);assert.equal(s.meta.trailWisdom.spent,spent);assert.ok(balance>0);
});

test('virtual starting depth earns no walking, XP, discoveries, locations or Wisdom and opening enemies stay fair',()=>{
 const s=start();buy(s,'trailblazer',4);const wisdom=C.legacyView(s).wisdom,world=s.world.totalDistance;restart(s);
 assert.equal(s.run.distance,0);assert.equal(C.expeditionDepth(s),1000*C.FEET);assert.equal(s.world.totalDistance,world);
 assert.equal(s.run.monkey.training,0);assert.equal(s.run.discoveries,0);assert.equal(s.run.legacy.locations.length,0);assert.equal(C.legacyView(s).wisdom,wisdom);assert.equal(W.specialLocations(s).length,0);
 const accepted=C.advance(s,1000*C.FEET);assert.equal(accepted,1000*C.FEET);assert.equal(s.run.monkey.training,20);assert.equal(s.run.legacy.earned,0);assert.equal(s.run.pendingEncounter.plan.size,1);assert.equal(s.run.pendingEncounter.plan.level,1);
 C.beginPendingEncounter(s);C.readyBattle(s);const recap=die(s);assert.equal(recap.feet,1000);assert.equal(recap.depthFeet,2000);assert.equal(recap.wisdom,0);
});

test('Scout protection is snapshotted, limited to the opening ordinary battles and absent in social combat',()=>{
 const s=start();buy(s,'protection',4);assert.equal(s.run.legacy.ranks.protection,0);restart(s);
 let b=battle(s);assert.equal(b.legacyProtectionPercent,10);assert.equal(L.legacyDamage(s,100),90);
 const restored=C.validate(structuredClone(s));assert.equal(L.legacyDamage(restored,100),90);L.beginLegacyBattle(restored,restored.run.battle);assert.equal(restored.run.legacy.ordinaryBattles,1);
 for(const expected of [10,0]){s.run.phase='trail';s.run.battle=null;b=battle(s);assert.equal(b.legacyProtectionPercent,expected);}
 b.online=true;assert.equal(L.legacyDamage(s,100),100);b.online=false;b.social=true;assert.equal(L.legacyDamage(s,100),100);
 const before=s.run.legacy.ordinaryBattles;for(const kind of ['guardian','ultra','hunt'])L.beginLegacyBattle(s,{[kind]:true});assert.equal(s.run.legacy.ordinaryBattles,before);
});

test('Scout protection reduces actual reflected and status damage without changing enemy health or RNG',()=>{
 for(const source of ['counter','poison']){
  const s=start();buy(s,'protection',4);restart(s);const b=battle(s),p=s.run.monkey;p.level=40;p.hp=C.stats(p).hp;p.loadout=['strike','brace','breathe'];b.enemies.forEach(e=>e.hp=999);b.enemyAction={type:'move',id:'breathe'};b.intent='breathe';
  if(source==='counter'){b.enemy.counter=1;b.enemy.status.counter=1;}else p.status.poison=1;
  const control=structuredClone(s);control.run.monkey=control.collection[control.run.activeId];control.run.battle.enemy=control.run.battle.enemies[0];control.run.battle.legacyProtectionPercent=0;
  C.turn(s,source==='counter'?'strike':'breathe');C.turn(control,source==='counter'?'strike':'breathe');
  assert.ok(s.run.monkey.hp>control.run.monkey.hp,source);assert.equal(s.rng,control.rng);assert.equal(s.run.battle.enemy.hp,control.run.battle.enemy.hp);
 }
});

test('Experienced Crew covers sanctuary choices and new recruits without entering competitive team copies',()=>{
 const s=start(),other=structuredClone(C.newSave('Second crew','langur','practice',4).run.monkey);other.id='reserve';s.collection[other.id]=other;
 buy(s,'crew',4);restart(s);const base=C.stats({...other,trailCrew:undefined});C.setParty(s,[other.id,s.run.activeId]);C.selectBattleLead(s,other.id);
 assert.equal(other.hp,base.hp+8);assert.equal(other.energy,base.maxEnergy+2);
 const social=snapshotTeam(s);assert.equal(social[0].maxHp,base.hp);assert.equal(social[0].maxEnergy,base.maxEnergy);assert.equal(social[0].trailCrew,undefined);
 const b=battle(s);b.enemies.forEach(p=>p.hp=1);C.turn(s,'strike');const result=C.befriend(s,s.run.capture.candidates[0].id);
 assert.equal(result.success,true);assert.deepEqual(result.monkey.trailCrew,{hp:8,maxEnergy:2});assert.equal(s.run.legacy.captured,1);
});

test('first regional challenges and adventure landmarks grant once and remain claimed across restarts',()=>{
 let s=start();walk(s,2500);const goal=regionChallenges(s).find(row=>row.kind==='distance'),before=C.legacyView(s).wisdom;claimChallenge(s,goal.id);assert.equal(C.legacyView(s).wisdom,before+2);
 assert.throws(()=>claimChallenge(s,goal.id),/already claimed/);
 const first=adventureSteps(s)[0],preAdventure=C.legacyView(s).wisdom;claimAdventure(s,first.id);assert.equal(C.legacyView(s).wisdom,preAdventure+4);assert.equal(s.run.legacy.locations.length,1);
 const earned=C.legacyView(s).wisdom;s=C.validate(structuredClone(s));restart(s);assert.throws(()=>claimChallenge(s,goal.id),/already claimed/);assert.throws(()=>claimAdventure(s,first.id),/already claimed/);assert.equal(C.legacyView(s).wisdom,earned);
});

test('a major destination rewards its definition once despite later spawned copies',()=>{
 const s=start();walk(s,3500);let row=W.specialLocations(s)[0];assert.ok(row.main);s.world.regions[s.world.activeLand].position={...row.position};const before=C.legacyView(s).wisdom;
 W.visitLocation(s,row.id);assert.equal(C.legacyView(s).wisdom,before+2);assert.equal(s.run.legacy.locations.length,1);
 W.dismissLocation(s,row.id);walk(s,3500);row=W.specialLocations(s).at(-1);s.meta.livingWorld.locations[row.id].definitionId='emerald-jungle:canopy-temple';row=W.specialLocation(s,row.id);s.world.regions[s.world.activeLand].position={...row.position};W.visitLocation(s,row.id);
 assert.equal(s.run.legacy.locations.length,1);assert.equal(C.legacyView(s).wisdom,before+2+2,'Only the additional 3500 accepted feet earn more Wisdom');
});

test('new guardian and Ultra victories reward first achievements once while recap counts actual wins',()=>{
 const s=start(),guardian={id:'g-one',rewarded:true,guardian:true,originLandId:'emerald-jungle',enemies:[]};
 assert.equal(L.recordLegacyBattle(s,guardian),3);assert.equal(L.recordLegacyBattle(s,guardian),0);
 assert.equal(L.recordLegacyBattle(s,{...guardian,id:'g-two',legacyRecorded:false}),0);
 const species=Object.keys(C.SPECIES).find(id=>C.SPECIES[id].rarity==='ultra'),ultra={id:'u-one',rewarded:true,ultra:true,enemies:[{species}]};
 assert.equal(L.recordLegacyBattle(s,ultra),6);assert.equal(L.recordLegacyBattle(s,ultra),0);assert.equal(s.run.legacy.ultras,1);
 assert.equal(L.recordLegacyBattle(s,{...ultra,id:'u-two',legacyRecorded:false}),0);assert.equal(s.run.legacy.ultras,2);
 const before=C.legacyView(s).wisdom;restart(s);L.recordLegacyBattle(s,{...ultra,id:'u-three',legacyRecorded:false});assert.equal(C.legacyView(s).wisdom,before);
});

test('recap equipment counts both crafted and discovered equipment and forge receipt retries stay exact',()=>{
 const s=start();awardGearCopy(s,'fang');s.meta.fragments.shell=5;C.craft(s,'shell');s.run.fruit=100;
 const receipt=craftForge(s,{fruit:8},'forge-1'),after=structuredClone(s);assert.equal(s.run.legacy.equipmentDiscovered,1);assert.equal(s.run.legacy.equipmentCrafted,2);
 assert.deepEqual(craftForge(s,{fruit:8},'forge-1'),receipt);assert.deepEqual(s,after);
 const recap=die(s);assert.equal(recap.equipment,3);assert.equal(recap.equipmentCrafted,2);assert.equal(recap.equipmentDiscovered,1);
});

test('distance records and complete expedition receipts cannot pay twice through death, reload or restart',()=>{
 let s=start();walk(s,4000);const recap=die(s),earned=C.legacyView(s).wisdom;
 assert.equal(recap.feet,4000);assert.equal(recap.wisdom,4);assert.equal(recap.newRecord,true);assert.equal(recap.records.length,1);assert.equal(s.meta.history.length,1);
 const history=structuredClone(s.meta.history);L.completeLegacyExpedition(s);assert.equal(C.legacyView(s).wisdom,earned);assert.deepEqual(s.meta.history,history);
 s=C.validate(structuredClone(s));assert.deepEqual(C.expeditionRecap(s),recap);restart(s);assert.equal(C.legacyView(s).wisdom,earned);
 walk(s,4000);const repeated=die(s);assert.equal(repeated.wisdom,2);assert.equal(repeated.newRecord,false);assert.equal(s.meta.history.length,2);
 restart(s);walk(s,6000);const longer=die(s);assert.equal(longer.wisdom,5);assert.equal(longer.previousBestFeet,4000);assert.equal(longer.improvementFeet,2000);
});

test('legacy save migration retains supplies and old achievements without manufacturing rewards',()=>{
 const s=start();walk(s,4000);const goal=regionChallenges(s).find(row=>row.kind==='distance');claimChallenge(s,goal.id);delete s.meta.trailWisdom;delete s.run.legacy;
 const before={fruit:s.run.fruit,distance:s.run.distance,hp:s.run.monkey.hp,gear:{...s.meta.gear}},loaded=C.validate(structuredClone(s));
 assert.deepEqual({fruit:loaded.run.fruit,distance:loaded.run.distance,hp:loaded.run.monkey.hp,gear:loaded.meta.gear},before);assert.equal(C.legacyView(loaded).wisdom,0);assert.equal(loaded.meta.trailWisdom.claims[`challenge:${goal.id}`],true);
 walk(loaded,1);assert.equal(C.legacyView(loaded).wisdom,0);walk(loaded,1999);assert.equal(C.legacyView(loaded).wisdom,1);
});

test('old collected Ultras migrate as claimed without hiding first victories earned after the feature launches',()=>{
 const s=start(),species=Object.keys(C.SPECIES).find(id=>C.SPECIES[id].rarity==='ultra'),p={...structuredClone(s.run.monkey),id:'veteran-ultra',species,types:[...C.SPECIES[species].types],loadout:['strike','brace','breathe']};
 s.collection[p.id]=p;delete s.meta.trailWisdom;delete s.run.legacy;const old=C.validate(structuredClone(s));
 assert.equal(old.meta.trailWisdom.claims[`ultra:${species}`],true);assert.equal(C.legacyView(old).wisdom,0);
 const victory={id:'old-ultra-victory',rewarded:true,ultra:true,enemies:[{species}]};assert.equal(L.recordLegacyBattle(old,victory),0);assert.equal(C.legacyView(old).wisdom,0);
 const current=start();current.collection[p.id]=structuredClone(p);C.validate(current);assert.equal(current.meta.trailWisdom.claims[`ultra:${species}`],undefined);
 assert.equal(L.recordLegacyBattle(current,{...victory,id:'new-ultra-victory',legacyRecorded:false}),5);assert.equal(C.legacyView(current).wisdom,5);
});

test('an interrupted encrypted cloud checkpoint preserves the completed recap and Wisdom exactly once',async()=>{
 const local=()=>{const map=new Map();return{getItem:key=>map.get(key)||null,setItem:(key,value)=>map.set(key,value)};},documents=new Map();let online=true;
 const adapter={async read(id){if(!online)throw Error('offline');return structuredClone(documents.get(id)||null);},async commit(id,base,envelope){if(!online)throw Error('offline');const old=documents.get(id);if((old?.revision||0)!==base)throw Error('conflict');documents.set(id,{...structuredClone(envelope),revision:base+1});return base+1;}};
 const vault=new Vault(()=>{},local(),async()=>adapter),s=await vault.open('Legacy cloud','legacy-cloud-password',{create:true});if(vault.inflight)await vault.inflight;
 walk(s,4000);const recap=die(s),wisdom=C.legacyView(s).wisdom;online=false;await vault.checkpoint(s,true);await vault.sync();assert.equal(vault.dirty,true);
 online=true;await vault.sync();assert.equal(vault.dirty,false);
 const reopened=await new Vault(()=>{},local(),async()=>adapter).open('Legacy cloud','legacy-cloud-password');
 assert.equal(C.legacyView(reopened).wisdom,wisdom);assert.deepEqual(C.expeditionRecap(reopened),recap);assert.equal(reopened.meta.history.length,1);
 L.completeLegacyExpedition(reopened);assert.equal(C.legacyView(reopened).wisdom,wisdom);C.restart(reopened);assert.equal(C.legacyView(reopened).wisdom,wisdom);assert.equal(reopened.meta.history.length,1);
});

test('all tracks grow beyond rank four and Trailblazer reaches exactly 50,000 feet',()=>{
 let s=start();s.meta.trailWisdom.earned=1000000;
 for(const row of C.LEGACY_UPGRADES){
  const costs=row.costs;assert.ok(costs.every((cost,i)=>!i||cost>=costs[i-1]));
  for(let i=0;i<costs.length;i++)C.purchaseLegacyUpgrade(s,row.id);
  assert.throws(()=>C.purchaseLegacyUpgrade(s,row.id),/maximum rank/);
 }
 const v=C.legacyView(s);assert.equal(v.bonuses.startingFeet,50000);assert.equal(v.bonuses.fruit,200);assert.equal(v.bonuses.essence,99);assert.equal(v.bonuses.utilityItem,true);
 assert.equal(v.bonuses.crewHp,200);assert.equal(v.bonuses.crewEnergy,50);assert.ok(v.bonuses.protectionPercent>10);assert.ok(v.bonuses.discoveryBonus>.03);
 assert.ok(C.LEGACY_UPGRADES.find(x=>x.id==='trailblazer').costs.reduce((a,b)=>a+b,0)>15000);
 const balance=v.wisdom;restart(s);assert.equal(s.run.startingDepth,50000*C.FEET);assert.equal(s.run.distance,0);assert.equal(C.legacyView(s).wisdom,balance);
 assert.equal(s.run.monkey.trailCrew.hp,200);const base=C.stats({...s.run.monkey,trailCrew:undefined});assert.equal(C.stats(s.run.monkey).hp,base.hp+200);
 s=C.validate(structuredClone(s));assert.equal(s.run.startingDepth,50000*C.FEET);assert.equal(C.legacyView(s).bonuses.startingFeet,50000);
 assert.equal(s.run.legacy.ranks.trailblazer,200);assert.equal(s.run.discoveries,0);assert.equal(s.run.legacy.walkingWisdom,0);
 const html=renderLegacy(s);assert.match(html,/50,000 virtual feet/);assert.match(html,/Rank 200 \/ 200/);assert.match(html,/Earn more Trail Wisdom/);assert.match(html,/per 3 expedition victories/);
});

test('repeatable victory, capture and crafting Wisdom is exact across reloads and restarts',()=>{
 let s=start();const victory=n=>({id:'ordinary-'+n,rewarded:true,enemies:[]});
 L.recordLegacyBattle(s,victory(1));L.recordLegacyBattle(s,victory(2));L.recordLegacyCapture(s);L.recordLegacyEquipment(s,{crafted:true});L.recordLegacyEquipment(s,{crafted:true});
 assert.deepEqual(C.legacyView(s).repeatable,{victories:2,captures:1,crafted:2});s=C.validate(structuredClone(s));const b=victory(3);
 assert.equal(L.recordLegacyBattle(s,b),1);assert.equal(L.recordLegacyBattle(s,b),0);L.recordLegacyCapture(s);L.recordLegacyEquipment(s,{crafted:true});assert.equal(C.legacyView(s).wisdom,3);
 assert.deepEqual(C.legacyView(s).repeatable,{victories:0,captures:0,crafted:0});const before=C.legacyView(s).wisdom;restart(s);assert.equal(C.legacyView(s).wisdom,before);assert.deepEqual(C.legacyView(s).repeatable,{victories:0,captures:0,crafted:0});
});

test('old capped walking and activity counters migrate without retroactive payouts',()=>{
 let s=start();walk(s,30000);const l=s.run.legacy;l.walkingWisdom=12;l.captured=1;l.equipmentCrafted=2;s.run.wins=2;delete l.victories;delete l.expandedWisdom;
 const before=C.legacyView(s).wisdom;s=C.validate(structuredClone(s));walk(s,1999);assert.equal(C.legacyView(s).wisdom,before);walk(s,1);assert.equal(C.legacyView(s).wisdom,before+1);
 L.recordLegacyBattle(s,{id:'after-migration',rewarded:true,enemies:[]});L.recordLegacyCapture(s);L.recordLegacyEquipment(s,{crafted:true});assert.equal(C.legacyView(s).wisdom,before+4);
});

test('deep head starts preserve ordinary encounter scaling without overwhelming a fresh team',()=>{
 const s=start();s.meta.trailWisdom.earned=1000000;for(let i=0;i<200;i++)C.purchaseLegacyUpgrade(s,'trailblazer');restart(s);s.run.ordinaryWins=2;
 const b=battle(s);assert.equal(b.plan.size,1);assert.ok(b.plan.level<=s.run.monkey.level+3);assert.equal(b.plan.stage,0);assert.equal(s.run.distance>0,true);
});
