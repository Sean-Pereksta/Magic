import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as Life from './adventure-life.mjs';
import * as Team from './team-techniques.mjs';
import {WORLD_EVENT_EPOCH,WORLD_EVENT_INTERVAL,WORLD_DAY,currentWorldEvent} from './world-events.mjs';

const fresh=()=>C.newSave('World stories','capuchin','practice',76192);
const stats=(p,s)=>C.stats(p,C.equipped(s,p));
function addFriend(s,id,species='macaque'){
 const p=structuredClone(s.run.monkey);p.id=id;p.species=species;p.types=[...C.SPECIES[species].types];p.loadout=['strike','brace','breathe'];p.learned=[];p.memories={};C.syncProgression(p);s.collection[id]=p;s.run.party.push(id);return p;
}
function campSave(){const s=fresh();s.run.phase='camp';s.run.camp={id:'camp:first',heals:2};return s;}
function battleSave(){
 const s=fresh();addFriend(s,'second');s.run.monkey.loadout=['strike','brace','breathe'];
 C.advance(s,C.ENCOUNTER_STEP);C.beginPendingEncounter(s);C.readyBattle(s);
 const e=s.run.battle.enemy;e.hp=10000;e.energy=20;e.types=[];e.loadout=['strike','breathe'];e.status={};e.cooldowns={};e.uses={};quiet(s);return s;
}
function quiet(s){s.run.battle.enemyAction={type:'move',id:'breathe'};s.run.battle.intent='breathe';}
const perform=(s,id)=>Team.useTeamTechnique(s,id,{turn:C.turn,stats});

test('old saves retain inventory and companions while journal adopts existing achievements',()=>{
 const s=fresh();addFriend(s,'second');addFriend(s,'third','langur');s.world.totalDistance=4321;s.world.regions['emerald-jungle'].distance=4321;s.run.monkey.memories.guardianVictories=2;s.run.monkey.memories.firstGuardian='emerald-jungle';
 const before=structuredClone({collection:s.collection,items:s.run.items,fruit:s.run.fruit,metaGear:s.meta.gear,runId:s.run.id});
 const view=Life.journalView(s);assert.ok(view.goals.every(row=>row.complete));assert.equal(view.companions.owned,3);assert.equal(view.guardians,2);
 for(const goal of view.goals){Life.trackJournalGoal(s,goal.id);assert.equal(Life.journalView(s).trackedGoal.id,goal.id);Life.claimJournalGoal(s,goal.id);assert.throws(()=>Life.claimJournalGoal(s,goal.id),/already/);}
 assert.deepEqual({collection:s.collection,items:s.run.items,fruit:s.run.fruit,metaGear:s.meta.gear,runId:s.run.id},before);
 const loaded=C.validate(JSON.parse(JSON.stringify(s)));assert.equal(Life.journalView(loaded).goals.filter(row=>row.claimed).length,3);assert.equal(Life.journalView(loaded).trackedGoal,null);
});

test('journal keeps encounter identities after battle and adopts old guardian adventure claims',()=>{
 const s=fresh();Life.recordJournalEncounter(s,{enemies:[{species:'macaque'},{species:'macaque'},{species:'bogus'}]});
 assert.equal(Life.journalView(s).companions.encountered,2);
 s.meta.adventures.regions['emerald-jungle'].claimed.push('adventure:emerald-jungle:5');
 assert.equal(Life.journalView(s).goals.find(row=>row.id==='guardian-story').complete,true);
 assert.equal(Life.journalView(s).regions.find(row=>row.id==='emerald-jungle').guardianDefeated,true);
 const b=battleSave();assert.ok(Life.journalView(b).companions.encountered>=2);
});

test('unlocking a land does not mark it explored before the player visits',()=>{
 const s=fresh();s.world.totalDistance=2000;s.world.unlocked.push('sunstone-savannah');
 assert.equal(Life.journalView(s).regions.find(row=>row.id==='sunstone-savannah').discovered,false);
 C.selectLand(s,'sunstone-savannah');assert.equal(Life.journalView(s).regions.find(row=>row.id==='sunstone-savannah').discovered,true);
 const loaded=C.validate(JSON.parse(JSON.stringify(s)));assert.equal(Life.journalView(loaded).regions.filter(row=>row.discovered).length,2);
});

test('sanctuary places existing tribal and regional rewards without changing ownership',()=>{
 const s=fresh();s.meta.cosmetics={'moonwell-sanctuary':true,'canopy-banner':true};s.meta.adventures.cosmetics=['emerald-jungle:keepsake'];const old=structuredClone({tribe:s.meta.cosmetics,regional:s.meta.adventures.cosmetics});
 assert.throws(()=>Life.setSanctuaryArea(s,'river'),/1,000/);assert.throws(()=>Life.placeDecoration(s,'trail-lantern','center'),/owned/);
 Life.placeDecoration(s,'moonwell-sanctuary','left');Life.placeDecoration(s,'emerald-jungle:keepsake','right');Life.placeDecoration(s,'moonwell-sanctuary','center');
 assert.equal(Life.sanctuaryView(s).slots.find(row=>row.id==='left').decoration,null);assert.equal(Life.sanctuaryView(s).slots.find(row=>row.id==='center').decoration.id,'moonwell-sanctuary');
 assert.deepEqual({tribe:s.meta.cosmetics,regional:s.meta.adventures.cosmetics},old);
 s.world.totalDistance=1000;Life.setSanctuaryArea(s,'river');Life.claimJournalGoal(s,'first-paths');Life.placeDecoration(s,'trail-lantern','left');
 let loaded=C.validate(JSON.parse(JSON.stringify(s)));assert.equal(Life.sanctuaryView(loaded).activeArea.id,'river');assert.equal(Life.sanctuaryView(loaded).slots.filter(row=>row.decoration).length,3);
 loaded.run.phase='dead';C.restart(loaded);assert.equal(Life.sanctuaryView(loaded).slots.filter(row=>row.decoration).length,3);Life.clearDecoration(loaded,'right');assert.ok(Life.sanctuaryView(loaded).decorations.find(row=>row.id==='emerald-jungle:keepsake').owned);
});

test('meal and study are mutually exclusive once per camp; Pack remains available',()=>{
 const s=campSave(),fruit=s.run.fruit;Life.chooseCampMoment(s,'meal');assert.equal(s.run.fruit,fruit-2);assert.equal(s.run.adventureLife.mealRounds,3);
 assert.throws(()=>Life.chooseCampMoment(s,'study'),/already/);assert.equal(Life.chooseCampMoment(s,'pack').action,'pack');
 const loaded=C.validate(JSON.parse(JSON.stringify(s)));assert.throws(()=>Life.chooseCampMoment(loaded,'meal'),/already/);loaded.run.camp={id:'camp:next'};assert.throws(()=>Life.chooseCampMoment(loaded,'meal'),/previous meal/);
 const studied=Life.chooseCampMoment(loaded,'study');assert.match(studied.message,/250 meters|Waiting encounter/);assert.equal(loaded.run.adventureLife.studyHint,studied.message);
 const pending=campSave();pending.run.pendingEncounter={enemies:[{species:'macaque',level:3}]};assert.match(Life.chooseCampMoment(pending,'study').message,/Macaque.*level 3/);
});

test('meal energy is capped, excludes fallen monkeys and lasts exactly three rounds',()=>{
 const s=campSave(),other=addFriend(s,'second');Life.chooseCampMoment(s,'meal');s.run.monkey.energy=0;other.hp=0;other.energy=0;
 assert.equal(Life.applyCampMealRound(s,p=>stats(p,s)),1);assert.equal(other.energy,0);assert.equal(s.run.adventureLife.mealRounds,2);
 for(let n=0;n<5;n++)Life.applyCampMealRound(s,p=>stats(p,s));assert.equal(s.run.monkey.energy,3);assert.equal(s.run.adventureLife.mealRounds,0);
 s.run.adventureLife.mealRounds=1;s.run.monkey.energy=stats(s.run.monkey,s).maxEnergy;assert.equal(Life.applyCampMealRound(s,p=>stats(p,s)),0);
});

test('the ordinary battle engine consumes meal warmth once per resolved round',()=>{
 const s=battleSave();Life.normalizeAdventureLife(s);s.run.adventureLife.mealRounds=3;s.run.monkey.energy=0;
 C.turn(s,'strike');assert.equal(s.run.monkey.energy,2);assert.equal(s.run.adventureLife.mealRounds,2);
 const loaded=C.validate(JSON.parse(JSON.stringify(s)));quiet(loaded);C.turn(loaded,'strike');assert.equal(loaded.run.monkey.energy,4);assert.equal(loaded.run.adventureLife.mealRounds,1);
});

test('merchant uses exact event boundaries, persists receipts and does not replace camp actions',()=>{
 const s=campSave();s.run.fruit=50;const start=WORLD_EVENT_EPOCH+4*WORLD_EVENT_INTERVAL,end=start+WORLD_DAY,event=currentWorldEvent(start);
 assert.equal(event.type,'traveling-merchant');assert.equal(Life.merchantOffers(s,{now:start-1})[0].available,false);
 const offered=Life.merchantOffers(s,{now:start})[0];const berries=s.run.items.berry;Life.merchantTrade(s,offered.id,{now:start,activeEvent:event});assert.equal(s.run.items.berry,berries+2);assert.equal(s.run.fruit,46);
 const loaded=C.validate(JSON.parse(JSON.stringify(s)));loaded.run.camp={id:'another-camp'};assert.throws(()=>Life.merchantTrade(loaded,offered.id,{now:start}),/Already/);
 assert.equal(Life.merchantOffers(loaded,{now:end-1})[1].available,true);assert.throws(()=>Life.merchantTrade(loaded,'crafting-bundle',{now:end}),/departed/);
 assert.ok(Life.campMoments(loaded,{now:start}).find(row=>row.id==='meal').available);assert.equal(Life.chooseCampMoment(loaded,'merchant',{now:start}).action,'merchant');
 const next=start+5*WORLD_EVENT_INTERVAL;assert.equal(Life.merchantOffers(loaded,{now:next})[0].available,true);
});

test('Canopy Relay requires another guarding teammate and spends a normal combat round',()=>{
 let s=battleSave();assert.equal(Team.teamTechniques(s)[0].ready,false);const guard=s.run.monkey.id;
 C.turn(s,'brace');quiet(s);assert.equal(Team.teamTechniques(s)[0].ready,false);C.switchMonkey(s,'second');quiet(s);
 s=C.validate(JSON.parse(JSON.stringify(s)));const ready=Team.teamTechniques(s)[0];assert.equal(ready.ready,true);assert.equal(ready.helperId,guard);
 const round=s.run.battle.round,energy=s.run.monkey.energy,helperEnergy=s.collection[guard].energy;perform(s,'canopy-relay');
 assert.equal(s.run.battle.round,round+1);assert.equal(s.run.monkey.energy,energy-3+1);assert.equal(s.collection[guard].energy,helperEnergy-2+1);assert.ok(s.run.battle.lastHits.some(hit=>hit.teamTechnique==='canopy-relay'));
 assert.equal(s.run.lastTeamTechnique.id,'canopy-relay');assert.equal(Team.teamTechniques(s)[0].used,true);const after=JSON.stringify(s);assert.throws(()=>perform(s,'canopy-relay'),/Already/);assert.equal(JSON.stringify(s),after);
});

test('team setups expire and unconscious or unfunded helpers cannot participate',()=>{
 const s=battleSave(),guard=s.run.monkey.id;C.turn(s,'brace');quiet(s);C.switchMonkey(s,'second');quiet(s);
 s.collection[guard].energy=1;assert.equal(Team.teamTechniques(s)[0].ready,false);s.collection[guard].energy=10;s.collection[guard].hp=0;assert.equal(Team.teamTechniques(s)[0].ready,false);s.collection[guard].hp=10;
 s.run.battle.round=5;assert.equal(Team.teamTechniques(s)[0].ready,false);s.run.battle.round=3;s.run.monkey.charging={id:'strike',resolveRound:4};assert.equal(Team.teamTechniques(s)[0].ready,false);
});

test('Dewfall Chorus needs complementary elements, heals without revival and uses Brace',()=>{
 const s=battleSave(),leaf=s.run.monkey;leaf.types=['leaf'];leaf.level=3;leaf.stage=1;leaf.loadout=['strike','leafCleanse','brace','breathe'];C.syncProgression(leaf);leaf.energy=20;
 C.turn(s,'leafCleanse');quiet(s);const water=s.collection.second;water.types=['water'];C.switchMonkey(s,water.id);quiet(s);
 leaf.hp=10;water.hp=10;const before=water.energy,round=s.run.battle.round;assert.equal(Team.teamTechniques(s)[1].ready,true);perform(s,'dewfall-chorus');
 assert.ok(s.collection[leaf.id].hp>10);assert.ok(s.collection.second.hp>10);assert.equal(s.run.monkey.energy,before-5+2+1);assert.equal(s.run.battle.round,round+1);assert.equal(s.run.lastTeamTechnique.artKey,'technique-dewfall-chorus');
});

test('failed normal turns do not spend team energy, rewards, or one-time use',()=>{
 const s=battleSave();C.turn(s,'brace');quiet(s);C.switchMonkey(s,'second');quiet(s);const before=structuredClone(s);
 assert.throws(()=>Team.useTeamTechnique(s,'canopy-relay',{turn:()=>{throw Error('Simulated rejected action');},stats}),/rejected/);
 assert.equal(s.run.monkey.energy,before.run.monkey.energy);assert.equal(s.run.battle.round,before.run.battle.round);assert.deepEqual(s.run.battle.teamwork.used,[]);
});

test('team feedback survives victory and reload while used techniques remain spent',()=>{
 const s=battleSave();C.turn(s,'brace');quiet(s);C.switchMonkey(s,'second');quiet(s);s.run.battle.enemy.hp=1;
 const battleId=s.run.battle.id;perform(s,'canopy-relay');assert.equal(s.run.lastTeamTechnique.battleId,battleId);assert.ok(['befriend','trail'].includes(s.run.phase));
 const loaded=C.validate(JSON.parse(JSON.stringify(s)));assert.equal(loaded.run.lastTeamTechnique.name,'Canopy Relay');assert.equal(loaded.run.battle?.teamwork?.used?.includes('canopy-relay')??true,true);
});
