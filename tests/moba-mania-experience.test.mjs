import test from 'node:test';
import assert from 'node:assert/strict';
import { Game } from '../game/moba-mania/engine.mjs';
import { ITEMS, ITEM_BY_ID } from '../game/moba-mania/items.mjs';
import { PROFILE_KEY, SAVE_KEY, COSMETICS, normalizeProfile, readProfile, writeProfile, heroCosmetics, equipCosmetic, recordMatch, readSavedMatch, saveMatch, clearSavedMatch, itemPlan, recommendedItems, previewItem, normalizeControls, matchBadges } from '../game/moba-mania/profile.mjs';
const memory=()=>{const data=new Map();return {getItem:key=>data.get(key)??null,setItem:(key,value)=>data.set(key,String(value)),removeItem:key=>data.delete(key)};};
const create=hero=>{const game=new Game({hero:hero||'arc',seed:14});game.matchId='match-a';return game;};

test('pinned build lists missing ingredients in order, counting a prerequisite once',()=>{
  const game=create(),hero=game.player;
  const plan=itemPlan(hero,'starfire-scepter');
  assert.equal(plan.next.id,'spark-crystal');
  assert.deepEqual(plan.remaining.map(item=>item.id),['spark-crystal','storm-tome','sand-clock','starfire-scepter']);
  assert.equal(plan.cost,plan.remaining.reduce((sum,item)=>sum+item.cost,0));
  assert.equal(plan.affordable,false);
  hero.gold=100;
  assert.equal(itemPlan(hero,'starfire-scepter').affordable,true);
  game.buyItem(hero,'spark-crystal');
  const after=itemPlan(hero,'starfire-scepter');assert.equal(after.next.id,'storm-tome');assert.equal(after.cost,plan.cost-100);
  assert.deepEqual(hero.items,['spark-crystal'],'planning does not purchase or consume anything');
});

test('every advanced build plan becomes purchasable without duplicate or missing dependencies',()=>{
  for(const item of ITEMS.filter(i=>i.tier===3)){
    const game=create(),hero=game.player;hero.gold=10000;
    const start=itemPlan(hero,item.id);assert.equal(new Set(start.remaining.map(i=>i.id)).size,start.remaining.length);
    for(const component of start.remaining)assert(game.buyItem(hero,component.id),`${item.id}: ${component.id}`);
    assert.equal(itemPlan(hero,item.id).cost,0);assert.equal(itemPlan(hero,item.id).next,null);
    assert.equal(hero.gold,10000-start.cost);
  }
});

test('item preview matches actual health, damage and cooldowns without changing the match',()=>{
  const game=create(),hero=game.player;hero.gold=3000;game.gainXP(hero,1000);
  const before=JSON.stringify(hero),preview=previewItem(game,hero,'ruby-heart');
  assert.equal(JSON.stringify(hero),before,'read-only comparison');
  game.buyItem(hero,'ruby-heart');assert.equal(preview.after.hp,hero.maxHp);assert.equal(preview.after.damage,hero.damage);
  for(const [i,ability] of preview.after.abilities.entries())assert.equal(ability.cooldown,game.getAbility(hero,i).cooldown);
  assert(preview.after.hp>preview.before.hp);
});

test('cooldown preview respects the combat cap and flags excess reduction',()=>{
  const game=create(),hero=game.player;
  hero.items=ITEMS.filter(item=>item.bonuses.cooldown).map(i=>i.id);
  for(const tier of (heroTiers(hero))) {const choice=tier.choices.find(c=>c.bonuses?.cooldown);if(choice)hero.talents[tier.level]=choice.id;}
  // A copied hero with every existing cooldown source exercises the shared cap, not an invented formula.
  game.refreshStats(hero);const preview=previewItem(game,hero,'eternity-hourglass');
  assert(preview.after.cooldownCut<=.5);
  assert.equal(preview.capped,game.bonus(hero,'cooldown')>.5);
  preview.after.abilities.forEach((ability,i)=>assert.equal(ability.cooldown,game.getAbility(hero,i).cooldown));
});
import { HEROES } from '../game/moba-mania/data.mjs';
function heroTiers(hero){return HEROES[hero.heroId].talents;}

test('recommendations fit hero roles and target only advanced items',()=>{
  const support=recommendedItems({heroId:'angel'}),marksman=recommendedItems({heroId:'ranger'}),summoner=recommendedItems({heroId:'druid'});
  assert.equal(support[0].id,'phoenix-crown');assert.equal(summoner[0].id,'elderwood-staff');
  assert(marksman.some(i=>i.id==='worldsplitter'||i.id==='thunderbolt-crossbow'));
  assert(!recommendedItems({heroId:'seraph'}).some(i=>i.id==='phoenix-crown'),'Blue Angel builds do not recommend healer equipment');
  assert(support.every(i=>i.tier===3));assert.notDeepEqual(support.map(i=>i.id),marksman.map(i=>i.id));
});

test('mastery rewards finished matches once and cosmetics never alter combat stats',()=>{
  const profile=normalizeProfile(),game=create('ranger'),before=JSON.stringify(game.player);
  assert.equal(recordMatch(profile,game).earned,0);game.winner=game.player.team;game.time=1200;
  assert.equal(recordMatch(profile,game).earned,100);assert.equal(recordMatch(profile,game).earned,0);
  assert.equal(profile.heroes.ranger.matches,1);assert.equal(profile.heroes.ranger.wins,1);
  assert.equal(equipCosmetic(profile,'ranger','astral'),false);
  game.matchId='match-b';recordMatch(profile,game);assert.equal(equipCosmetic(profile,'ranger','verdant'),true);
  assert.equal(heroCosmetics(profile,'ranger').armorTint,'#86c99c');assert.equal(heroCosmetics(profile,'arc').armorTint,null);
  assert.equal(JSON.stringify(game.player),before,'recording and equipping cosmetics does not edit simulation stats');
});

test('damaged profiles recover safely and locked equipped variants are removed',()=>{
  const storage=memory();storage.setItem(PROFILE_KEY,'{bad');
  const profile=readProfile(storage);assert.equal(profile.heroes.arc.xp,0);
  const normalized=normalizeProfile({heroes:{arc:{xp:-10,equipped:'sovereign'},ranger:{xp:Infinity,matches:-1}},completed:['same','same',42]});
  assert.equal(normalized.heroes.arc.equipped,'classic');assert.equal(normalized.heroes.ranger.xp,0);assert.deepEqual(normalized.completed,['same']);
  assert(writeProfile(storage,profile));assert.equal(readProfile(storage).heroes.arc.xp,0);
});

test('save and resume retain real battle state, build target and identity',()=>{
  const storage=memory(),game=create();game.player.gold=2000;game.buyItem(game.player,'ruby-heart');game.player.hp-=91;game.time=137;game.paused=false;
  const before=JSON.stringify(game.serialize());
  assert(saveMatch(storage,game,{pinnedGoal:'sovereign-aegis'}).ok);
  const saved=readSavedMatch(storage,snapshot=>Game.restore(snapshot));
  assert.equal(saved.status,'ready');assert.equal(saved.game.matchId,game.matchId);assert.equal(saved.record.ui.pinnedGoal,'sovereign-aegis');
  assert.equal(saved.game.player.gold,game.player.gold);assert.equal(saved.game.player.hp,game.player.hp);assert.deepEqual(saved.game.player.items,['ruby-heart']);
  assert.equal(saved.game.time,137);assert.equal(saved.game.paused,true,'restore always pauses before player input');
  assert.equal(JSON.stringify(game.serialize()),before,'saving does not change the running simulation');
});

test('save slot requires confirmation before replacing another match or corrupt save',()=>{
  const storage=memory(),first=create(),next=create();next.matchId='match-b';
  assert(saveMatch(storage,first).ok);const original=storage.getItem(SAVE_KEY);
  assert.equal(saveMatch(storage,next).ok,false);assert.equal(storage.getItem(SAVE_KEY),original);
  assert(saveMatch(storage,next,{},true).ok);
  assert.equal(clearSavedMatch(storage,'match-a'),false);assert(storage.getItem(SAVE_KEY));
  storage.setItem(SAVE_KEY,'not JSON');assert.equal(readSavedMatch(storage,s=>Game.restore(s)).status,'invalid');
  assert.equal(saveMatch(storage,next).ok,false);assert.equal(storage.getItem(SAVE_KEY),'not JSON');
  assert(saveMatch(storage,next,{},true).ok);assert(clearSavedMatch(storage,'match-b'));assert.equal(storage.getItem(SAVE_KEY),null);
});

test('storage failure leaves the game playable and reports a failed save',()=>{
  const denied={getItem(){throw Error('Denied')},setItem(){throw Error('Quota')},removeItem(){throw Error('Denied')}};
  const game=create();assert.equal(saveMatch(denied,game).ok,false);assert.equal(writeProfile(denied,normalizeProfile()),false);
  assert.equal(readSavedMatch(null,s=>Game.restore(s)).status,'unavailable');assert.equal(game.winner,null);
});

test('control preferences clamp unsafe values and badges use match contributions',()=>{
  assert.deepEqual(normalizeControls({scale:9,height:-2,inset:Infinity,handed:'broken'}),{scale:1.25,height:0,inset:0,handed:'right'});
  assert.deepEqual(normalizeControls({scale:.9,height:40,inset:8,handed:'left'}),{scale:.9,height:40,inset:8,handed:'left'});
  assert(matchBadges({deaths:2,metrics:{beastsCaptured:1,healingDone:1000,bestStreak:4,shutdowns:1}}).includes('Team guardian'));
  assert(COSMETICS.every(c=>!('damage' in c)&&!('hp' in c)));
});
