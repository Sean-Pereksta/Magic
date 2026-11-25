import test from 'node:test';
import assert from 'node:assert/strict';
import {Game} from '../game/moba-mania/engine.mjs';
import {HEROES,RULES,xpToNextLevel} from '../game/moba-mania/data.mjs';
import {ITEMS,ITEM_BY_ID} from '../game/moba-mania/items.mjs';

function arena(hero='arc'){
  const game=new Game({hero,seed:7341}),player=game.player;
  game.units=[player];game.campStates=[];game.waveIn=game.beastIn=game.planIn=1e8;
  Object.assign(player,{x:1500,y:1100,attackTimer:0});game.rebuildGrid();
  return {game,player};
}
function hero(game,team=1,slot=0){
  const unit=game.makeHero('dragon',team,slot);
  Object.assign(unit,{x:1600,y:1100,bot:false,hp:2000,maxHp:2000,speed:0,attackTimer:100});
  return unit;
}
function totalXP(unit){
  let total=unit.xp;for(let level=1;level<unit.level;level++)total+=xpToNextLevel(level);return total;
}
function minion(game,team=1,x=1600,kind='footman'){
  return game.addUnit(kind,team,x,1100,{hp:300,maxHp:300,speed:0,damage:0,attackTimer:100});
}
function buyRequirements(game,unit,item){
  for(const id of item.requires)if(!unit.items.includes(id)){
    buyRequirements(game,unit,ITEM_BY_ID[id]);assert(game.buyItem(unit,id));
  }
}

test('level costs are slower at every level, cap at18, and stop accumulating XP',()=>{
  const {game,player}=arena();assert.equal(RULES.maxLevel,18);
  for(let level=1;level<18;level++){
    assert.equal(player.level,level);assert.equal(player.xpNext,xpToNextLevel(level));
    assert(player.xpNext>90+(level-1)*42,'each threshold exceeds the previous progression curve');
    game.gainXP(player,player.xpNext-1);assert.equal(player.level,level);
    game.gainXP(player,1);assert.equal(player.level,level+1);
  }
  assert.equal(player.xp,0);assert.equal(player.xpNext,0);
  game.gainXP(player,1e7);assert.equal(player.level,18);assert.equal(player.xp,0);
  assert.equal(player.maxHp,HEROES.arc.hp+17*62,'levels beyond12 keep increasing combat stats');
});

test('a hero kill pays full personal XP and gold; only recent contributors receive assist XP',()=>{
  const {game,player}=arena(),helper=hero(game,0,1),bystander=hero(game,0,2),victim=hero(game);
  helper.x=100;bystander.x=victim.x+5;
  game.damage(victim,50,helper);game.damage(victim,1e6,player);
  assert.equal(totalXP(player),162);assert.equal(player.gold,190);assert.equal(player.goldEarned,190);
  assert.equal(totalXP(helper),81);assert.equal(helper.gold,0);assert.equal(helper.assists,1);
  assert.equal(totalXP(bystander),0);assert.equal(bystander.gold,0);assert.equal(bystander.assists,0);
  assert.equal(player.kills,1);assert.equal(player.assists,0);
  assert.equal(player.lastGoldGain.amount,190);assert(game.texts.some(t=>t.text==='+190 GOLD'));
  game.damage(victim,1e6,player);game.kill(victim,player);
  assert.equal(player.gold,190);assert.equal(totalXP(player),162);assert.equal(helper.assists,1,'a defeated unit pays once');
});

for(const kind of ['footman','archer','camp','tower','beast'])test(`${kind}: rewards are personal even on shared objectives`,()=>{
  const {game,player}=arena(),helper=hero(game,0,1),bystander=hero(game,0,2);
  const target=game.addUnit(kind,kind==='camp'||kind==='beast'?-1:1,1600,1100,{hp:300,lane:1,tier:0});
  game.damage(target,10,helper);game.damage(target,1e6,player);
  assert(totalXP(player)>0);assert.equal(totalXP(helper),totalXP(player)/2);
  assert(player.gold>0);assert.equal(helper.gold,0);assert.equal(bystander.gold,0);assert.equal(totalXP(bystander),0);
  assert.equal(helper.assists,0,'the hero K/D/A counter does not count minion or objective assists');
});

test('assists expire after10 seconds; shield damage counts but a zero-damage hit does not',()=>{
  for(const [elapsed,expected] of [[10,81],[10.01,0]]){
    const {game,player}=arena(),helper=hero(game,0,1),victim=hero(game);
    victim.shield=100;game.damage(victim,20,helper);game.time=elapsed;
    game.damage(victim,1e6,player);assert.equal(totalXP(helper),expected);assert.equal(helper.gold,0);
  }
  const {game,player}=arena(),helper=hero(game,0,1),victim=hero(game);
  victim.status.guard={factor:0,remaining:5};game.damage(victim,20,helper);delete victim.status.guard;
  game.damage(victim,1e6,player);assert.equal(totalXP(helper),0);assert.equal(helper.assists,0);
});

test('allied minion and tower finishers give contributors assist XP, never personal gold or kill credit',()=>{
  for(const kind of ['footman','archer','tower']){
    const {game,player}=arena(),victim=hero(game),ally=minion(game,0,1560,kind);
    game.damage(victim,30,player);game.damage(victim,1e6,ally);
    assert.equal(totalXP(player),81);assert.equal(player.gold,0);assert.equal(player.kills,0);assert.equal(player.assists,1);
    assert.equal(game.stats.kills[0],1);assert.equal(player.lastGoldGain,null);
  }
});

test('captured siege beasts belong to the team and do not farm personal income for their capturer',()=>{
  const {game,player}=arena(),beast=game.spawnBeast(0);
  game.damage(beast,1e6,player);
  const siege=game.units.find(u=>u.alive&&u.kind==='beast'&&u.team===0);
  assert(siege);assert.equal(siege.ownerId,undefined);
  const before={gold:player.gold,xp:totalXP(player)},victim=minion(game);
  game.damage(victim,1e6,siege);assert.equal(player.gold,before.gold);assert.equal(totalXP(player),before.xp);
});

test('personal spell summons, projectiles, and damage over time retain their owner reward credit',()=>{
  for(const type of ['summon','projectile','expired-summon-projectile','poison']){
    const {game,player}=arena('ranger'),victim=minion(game,1,1545);
    victim.hp=1;
    if(type==='summon'){
      const summon=game.addUnit('summon',0,1530,1100,{ownerId:player.id,damage:20,range:40,attackTimer:0});
      assert(game.basicAttack(summon,victim));
    }else if(type==='expired-summon-projectile'){
      const summon=game.addUnit('summon',0,1500,1100,{ownerId:player.id,damage:20,range:150,attackTimer:0});
      game.rebuildGrid();assert(game.basicAttack(summon,victim));
      game.units=game.units.filter(unit=>unit!==summon);game.rebuildGrid();game.tickProjectiles(.1);
    }else if(type==='projectile'){
      game.rebuildGrid();assert(game.basicAttack(player,victim));game.tickProjectiles(.1);
    }else{
      game.applyStatus(victim,'poison',4,{dps:20,sourceId:player.id});game.tickStatuses(victim,.1);
    }
    assert.equal(victim.alive,false,type+' delivers the final hit');assert.equal(player.gold,18);assert.equal(totalXP(player),22);
  }
});

test('posthumous damage and assists still earn personal rewards without reviving the hero',()=>{
  const {game,player}=arena(),helper=hero(game,0,1),victim=hero(game);
  game.damage(victim,10,helper);game.kill(helper,victim);
  game.kill(player,victim);assert.equal(player.hp,0);
  game.applyStatus(victim,'burn',2,{dps:1e6,sourceId:player.id});game.tickStatuses(victim,.1);
  assert.equal(player.alive,false);assert.equal(player.hp,0);assert.equal(player.level,2);assert.equal(player.gold,200,'the victim reached level2 before its delayed defeat');
  assert.equal(helper.alive,false);assert.equal(helper.hp,0);assert.equal(helper.assists,1);assert.equal(helper.gold,0);
});

test('respawns and neutral leash resets discard old contribution records',()=>{
  const {game,player}=arena(),helper=hero(game,0,1),victim=hero(game);
  game.damage(victim,10,helper);game.damage(victim,1e6,player);game.respawn(victim);
  const oldXP=totalXP(helper);game.damage(victim,1e6,player);assert.equal(totalXP(helper),oldXP);
  const camp=game.addUnit('camp',-1,1600,1100,{hp:200,homeX:1600,homeY:1100});
  game.damage(camp,10,helper);helper.x=100;game.tickNeutral(camp,.1);
  game.damage(camp,1e6,player);assert.equal(totalXP(helper),oldXP,'leashed neutral no longer owes an old assist');
});

test('friendly kills, idle time, and another lane fight cannot create personal gold or XP',()=>{
  const {game,player}=arena(),ally=hero(game,0,1),enemy=hero(game);
  assert.equal(game.damage(ally,1e6,player),0);game.kill(ally,player);assert(ally.alive);
  game.damage(enemy,1e6,ally);assert.equal(totalXP(player),0);assert.equal(player.gold,0);
  game.units=[player];for(let i=0;i<60;i++)game.update(.05);
  assert.equal(totalXP(player),0);assert.equal(player.gold,0);
});

test('automatic attacks keep the nearest valid target while explicit enemy focus stays locked',()=>{
  for(const heroId of ['arc','dragon']){
    const {game,player}=arena(heroId),first=minion(game,1,1600),second=minion(game,1,1700);
    game.attackNearest();assert.equal(player.targetId,first.id);
    second.x=player.x+30;game.rebuildGrid();game.update(.05);assert.equal(player.targetId,second.id,'closer arriving enemy replaces an automatic target');
    game.attackTarget(first.id);game.update(.05);assert.equal(player.targetId,first.id,'explicit focus overrides proximity');
    game.damage(first,1e6,player);game.update(.05);assert.equal(player.targetId,second.id);
    game.damage(second,1e6,player);game.update(.05);assert.equal(player.targetId,null);
    const arriving=minion(game,1,1550);game.update(.05);assert.equal(player.targetId,arriving.id,'automatic order waits for the next valid enemy');
  }
});

test('purchases are atomic, activate stats immediately, and retain prerequisites and damage taken',()=>{
  const {game,player}=arena(),healthItem=ITEMS.find(i=>i.tier===1&&i.bonuses.hp),upgrade=ITEMS.find(i=>i.tier===3);
  const before={hp:player.maxHp,damage:player.damage,gold:player.gold};
  assert.equal(game.buyItem(player,healthItem.id),false);assert.equal(game.buyItem(player,'unknown'),false);
  assert.deepEqual({hp:player.maxHp,damage:player.damage,gold:player.gold},before);
  player.gold=100000;player.hp-=75;game.paused=true;
  assert.equal(game.buyItem(player,upgrade.id),false,'gold cannot bypass prerequisites');
  assert(game.buyItem(player,healthItem.id));assert(player.maxHp>before.hp);assert.equal(player.maxHp-player.hp,75);
  const paid=player.gold,hp=player.hp;assert.equal(game.buyItem(player,healthItem.id),false);assert.equal(player.gold,paid);assert.equal(player.hp,hp);
  buyRequirements(game,player,upgrade);assert(game.buyItem(player,upgrade.id));
  assert(upgrade.requires.every(id=>player.items.includes(id)),'the two lower-layer items stay active');
  assert.equal(new Set(player.items).size,player.items.length);assert(player.items.includes(upgrade.id));
  assert.equal(game.buyItem({...player},ITEMS.find(i=>!player.items.includes(i.id)).id),false,'foreign hero objects cannot spend match currency');
});

test('item stats affect real basic damage, spell damage, and ability cooldowns',()=>{
  const {game,player}=arena('dragon'),victim=minion(game,1,1540);
  const damageItem=ITEMS.find(i=>i.tier===1&&i.bonuses.damage),spellItem=ITEMS.find(i=>i.tier===1&&i.bonuses.spellDamage),cooldownItem=ITEMS.find(i=>i.tier===1&&i.bonuses.cooldown);
  player.gold=10000;game.rebuildGrid();const baseline=player.damage,baseCooldown=game.getAbility(player,0).cooldown;
  assert(game.buyItem(player,damageItem.id));player.attackTimer=0;const hp=victim.hp;assert(game.basicAttack(player,victim));assert(hp-victim.hp>baseline);
  const measure=()=>{
    victim.hp=victim.maxHp=10000;victim.alive=true;victim.status={};player.cooldowns[0]=0;player.mana=player.maxMana;game.projectiles=[];
    game.cast(player,0,{x:victim.x,y:victim.y});game.rebuildGrid();for(let i=0;i<5;i++)game.tickProjectiles(.05);
    return 10000-victim.hp;
  };
  const beforeSpell=measure();assert(beforeSpell>0);assert(game.buyItem(player,spellItem.id));assert(measure()>beforeSpell);
  assert(game.buyItem(player,cooldownItem.id));assert(game.getAbility(player,0).cooldown<baseCooldown);
  player.cooldowns[0]=0;player.mana=player.maxMana;game.cast(player,0,{x:victim.x,y:victim.y});assert.equal(player.cooldowns[0],game.getAbility(player,0).cooldown);
});

test('all purchases survive death, keep cooldown reductions capped, and reset only in a new match',()=>{
  const {game,player}=arena();player.gold=100000;
  for(const item of ITEMS){buyRequirements(game,player,item);if(!player.items.includes(item.id))assert(game.buyItem(player,item.id));}
  assert.equal(player.items.length,45);const owned=[...player.items],gold=player.gold,maxHp=player.maxHp;
  assert(game.getAbility(player,0).cooldown>=HEROES.arc.abilities[0].cooldown*.5);
  const foe=hero(game);game.kill(player,foe);game.respawn(player);
  assert.deepEqual(player.items,owned);assert.equal(player.gold,gold);assert.equal(player.maxHp,maxHp);assert.equal(player.hp,maxHp);
  const next=new Game({seed:3});assert.deepEqual(next.player.items,[]);assert.equal(next.player.gold,0);assert.equal(next.player.level,1);
});

test('bots spend only earned gold and obey the same purchase requirements',()=>{
  const {game,player}=arena(),bot=hero(game,1,1);bot.bot=true;
  assert.equal(game.botShop(bot),false);assert.equal(bot.gold,0);assert.deepEqual(bot.items,[]);
  game.damage(player,1e6,bot);const earned=bot.gold;assert(earned>0);assert(game.botShop(bot));
  assert.equal(bot.items.length,1);assert.equal(ITEM_BY_ID[bot.items[0]].tier,1);
  assert.equal(bot.gold,earned-ITEM_BY_ID[bot.items[0]].cost);assert.equal(bot.goldEarned,earned);
});
