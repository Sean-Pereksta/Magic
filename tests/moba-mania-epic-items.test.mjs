import test from 'node:test';
import assert from 'node:assert/strict';
import {Game} from '../game/moba-mania/engine.mjs';
import {xpToNextLevel} from '../game/moba-mania/data.mjs';
import {ITEM_BY_ID,ITEM_BONUS_CAPS,itemBonus,killRewardBonus,modifyItemStatus} from '../game/moba-mania/items.mjs';
import {scheduleImpact,tickProgression,onCast,onSummon} from '../game/moba-mania/progression.mjs';
import {tickExpansionDevice,tickExpansion} from '../game/moba-mania/hero-expansion.mjs';
import {tickSolstice} from '../game/moba-mania/solstice-runtime.mjs';

function arena(id='dragon'){
  const game=new Game({hero:id,seed:921147}),hero=game.player;
  game.units=[hero];game.campStates=[];game.waveIn=game.beastIn=game.planIn=1e8;
  Object.assign(hero,{x:1450,y:1100,attackTimer:0,bot:false,angle:0});
  const add=(kind='footman',team=1,x=1500,y=1100)=>{
    const unit=kind==='hero'?game.makeHero('dragon',team,team?0:1):game.addUnit(kind,team,x,y,{hp:10000});
    Object.assign(unit,{x,y,hp:10000,maxHp:10000,damage:0,speed:0,bot:false,attackTimer:1e8,range:0,status:{}});
    game.rebuildGrid();return unit;
  };
  return {game,hero,add};
}
function equip(game,hero,id){
  hero.gold=100000;
  for(const prerequisite of ITEM_BY_ID[id].requires)if(!hero.items.includes(prerequisite))equip(game,hero,prerequisite);
  if(!hero.items.includes(id))assert(game.buyItem(hero,id));
}
function economy(game,hero){equip(game,hero,'ascendant-codex');equip(game,hero,'kings-ransom');hero.gold=hero.goldEarned=0;}
function totalXP(hero){let xp=hero.xp;for(let level=1;level<hero.level;level++)xp+=xpToNextLevel(level);return xp;}
function close(actual,expected){assert(Math.abs(actual-expected)<1e-8,`${actual} ≈ ${expected}`);}
function resolve(game,seconds=.6){game.time+=seconds;tickProgression(game,seconds);}

test('economy paths add retained components once and expose explicit reward/status caps',()=>{
  const {game,hero}=arena();economy(game,hero);
  close(killRewardBonus(hero,'xp'),.60);close(killRewardBonus(hero,'gold'),.45);
  assert.equal(killRewardBonus(hero,'mana'),0);assert.equal(killRewardBonus(null,'xp'),0);
  hero.items.push('ascendant-codex','kings-ransom','missing');
  close(killRewardBonus(hero,'xp'),.60);close(killRewardBonus(hero,'gold'),.45);
  assert.deepEqual(ITEM_BONUS_CAPS,{killXp:.60,killGold:.45,burnPower:.75,burnDurationPct:.35,poisonPower:.75,poisonDurationPct:.35,slowPower:.35,slowDurationPct:.35});
});

test('kill bonuses affect the finisher only; an equipped assistant still receives base assist XP and zero gold',()=>{
  const {game,hero,add}=arena(),helper=add('hero',0,1400),bystander=add('hero',0,1400,1150),victim=add('hero');
  for(const owner of [hero,helper,bystander])economy(game,owner);
  game.damage(victim,10,helper);game.damage(victim,1e6,hero);
  close(totalXP(hero),162*1.6);assert.equal(hero.gold,275);assert.equal(hero.lastGoldGain.bonus,85);
  close(totalXP(helper),81);assert.equal(helper.gold,0);assert.equal(helper.assists,1);
  assert.equal(totalXP(bystander),0);assert.equal(bystander.gold,0);
  game.kill(victim,hero);assert.equal(hero.gold,275,'a defeated unit cannot pay twice');
});

test('bonus gold rounds once per real final hit, including shutdown bounty, without compounding inventory bonuses',()=>{
  const {game,hero,add}=arena();economy(game,hero);
  const minion=add();game.damage(minion,1e6,hero);assert.equal(hero.gold,26);assert.equal(hero.lastGoldGain.bonus,8);
  const victim=add('hero');victim.streak=4;game.damage(victim,1e6,hero);
  assert.equal(hero.lastGoldGain.amount,449);assert.equal(hero.lastGoldGain.bonus,139);
  assert.equal(hero.gold,475);assert.equal(hero.goldEarned,475);
});

test('owned summons, expired summon projectiles and posthumous DOT keep boosted final-hit credit',()=>{
  for(const kind of ['summon','expired-projectile','posthumous-burn']){
    const {game,hero,add}=arena(),target=add();economy(game,hero);target.hp=1;
    if(kind==='posthumous-burn'){
      game.kill(hero,target);assert.equal(hero.hp,0);
      game.applyStatus(target,'burn',2,{dps:100,sourceId:hero.id});game.tickStatuses(target,.05);
      assert.equal(hero.alive,false);assert.equal(hero.hp,0);assert.equal(hero.shield,0);
    }else{
      const summon=game.addUnit('summon',0,hero.x,hero.y,{ownerId:hero.id,damage:20,range:kind==='summon'?70:180,attackTimer:0});
      assert(game.basicAttack(summon,target));
      if(kind==='expired-projectile'){game.units=game.units.filter(u=>u!==summon);game.rebuildGrid();game.tickProjectiles(.2);}
    }
    assert.equal(target.alive,false,kind);close(totalXP(hero),22*1.6);assert.equal(hero.gold,26,kind);
  }
});

test('equipped helpers gain no bonus from allied minion final hits or idle time',()=>{
  const {game,hero,add}=arena();economy(game,hero);
  const victim=add(),ally=add('footman',0,1480);game.damage(victim,10,hero);game.damage(victim,1e6,ally);
  close(totalXP(hero),11);assert.equal(hero.gold,0);assert.equal(hero.lastGoldGain,null);
  game.units=[hero];for(let n=0;n<20;n++)game.update(.05);
  close(totalXP(hero),11);assert.equal(hero.gold,0);
});

test('economy epics grant finite hero-kill rewards even when the finishing blow is an item proc',()=>{
  const {game,hero,add}=arena();economy(game,hero);hero.cooldowns=[7,5,1,20];
  const victim=add('hero');victim.hp=1;
  scheduleImpact(game,hero,{kind:'starfire',x:victim.x,y:victim.y,r:40,damage:20,proc:true,interruptible:false});
  resolve(game);assert.equal(victim.alive,false);assert(hero.shield>=100);assert(hero.status.haste);
  assert.deepEqual(hero.cooldowns,[5,3,.5,20]);assert.equal(game.progressionDepth,0);
  hero.shield=0;hero.shieldLayers=[];hero.status={};hero.cooldowns=[7,5,1,20];
  game.damage(add('hero'),1e6,hero);
  assert.equal(hero.shield,0);assert.equal(hero.status.haste,undefined);assert.deepEqual(hero.cooldowns,[7,5,1,20]);
});

test('burn and poison equipment amplify actual outgoing DOT once, preserving talent and item snapshots',()=>{
  for(const [id,type] of [['inferno-heart','burn'],['pandemic-engine','poison']]){
    const {game,hero,add}=arena(),target=add();equip(game,hero,id);
    close(itemBonus(hero,type==='burn'?'burnPower':'poisonPower'),.75);
    game.applyStatus(target,type,4,{dps:20,sourceId:hero.id});
    close(target.status[type].dps,35);close(target.status[type].remaining,5.4);assert.equal(target.status[type].itemScaled,true);
    const before=target.hp;game.tickStatuses(target,.1);close(before-target.hp,3.5);
    const fixed=modifyItemStatus(game,target,type,5.4,{dps:35,sourceId:hero.id,itemScaled:true});close(fixed.extra.dps,35);close(fixed.duration,5.4);
  }
});

test('a smaller same-owner spread cannot overwrite or extend a stronger native DOT',()=>{
  const {game,hero,add}=arena(),target=add();equip(game,hero,'inferno-heart');
  game.applyStatus(target,'burn',2,{dps:40,sourceId:hero.id});
  const saved={...target.status.burn};game.applyStatus(target,'burn',8,{dps:10,sourceId:hero.id,proc:true,itemScaled:true});
  assert.deepEqual(target.status.burn,saved);
  game.applyStatus(target,'burn',1,{dps:100,sourceId:hero.id,itemScaled:true});
  close(target.status.burn.dps,100);close(target.status.burn.remaining,1);
});

test('status equipment purchased after launch cannot alter a projectile already in flight',()=>{
  const {game,hero,add}=arena(),target=add('footman',1,1600);
  const projectile=game.fireSkill(hero,0,{type:'dragon',r:12,range:400,speed:500,damage:5,pierce:0,burn:10,burnDuration:4});
  assert.equal(projectile.itemScaled,true);equip(game,hero,'inferno-heart');
  for(let i=0;i<5;i++)game.tickProjectiles(.1);
  close(target.status.burn.dps,10);close(target.status.burn.remaining,4);
  target.status={};const next=game.fireSkill(hero,0,{type:'dragon',r:12,range:400,speed:500,damage:5,pierce:0,burn:10,burnDuration:4});
  close(next.burn,17.5);close(next.burnDuration,5.4);
});

test('warned impacts also preserve status snapshots through later purchases',()=>{
  const {game,hero,add}=arena(),target=add();
  const impact=scheduleImpact(game,hero,{kind:'fixture',x:target.x,y:target.y,r:60,damage:10,burn:10,burnDuration:4,slow:.2,slowDuration:2,proc:true});
  equip(game,hero,'inferno-heart');equip(game,hero,'winters-dominion');resolve(game);
  assert.equal(impact.itemScaled,true);close(target.status.burn.dps,10);close(target.status.burn.remaining,4);
  close(target.status.slow.factor,.2);close(target.status.slow.remaining,2);
});

test('slow equipment increases only outgoing slows, caps added strength and never modifies stuns',()=>{
  const {game,hero,add}=arena(),target=add('hero');equip(game,hero,'winters-dominion');
  game.applyStatus(target,'slow',2,{factor:.5,sourceId:hero.id});close(target.status.slow.factor,.65);close(target.status.slow.remaining,2.7);
  target.status={};game.applyStatus(target,'slow',1,{factor:.8,sourceId:hero.id});close(target.status.slow.factor,.8,'native stronger slow is preserved');
  game.applyStatus(target,'stun',1,{sourceId:hero.id});close(target.status.stun.remaining,1);
  game.applyStatus(hero,'slow',2,{factor:.3,sourceId:target.id});close(hero.status.slow.factor,.3);close(hero.status.slow.remaining,2);
  const ally=add('hero',0);game.applyStatus(ally,'slow',2,{factor:.3,sourceId:hero.id});close(ally.status.slow.factor,.3);
});

for(const [id,type,kind,ratio] of [['inferno-heart','burn','inferno-chain',.5],['pandemic-engine','poison','pandemic-burst',.6]]){
  test(`${id}: warned spread infects at most three other foes and cannot recursively trigger itself`,()=>{
    const {game,hero,add}=arena(),target=add();equip(game,hero,id);
    const others=[30,55,80,110].map(offset=>add('footman',1,target.x,target.y+offset));
    game.applyStatus(target,type,4,{dps:20,sourceId:hero.id});game.tickStatuses(target,.05);
    const impact=game.scheduledImpacts.find(i=>i.kind===kind);assert(impact);assert.equal(impact.maxSpread,3);assert.equal(impact.itemScaled,true);
    assert(others.every(u=>!u.status[type]),'warning precedes actual spread');
    resolve(game);assert.equal(others.filter(u=>u.status[type]).length,3);
    for(const infected of others.filter(u=>u.status[type])){
      close(infected.status[type].dps,35*ratio);assert.equal(infected.status[type].proc,true);close(infected.status[type].remaining,3);
      game.tickStatuses(infected,.1);
    }
    assert.equal(game.scheduledImpacts.length,0);assert.equal(game.progressionDepth,0);
    close(target.status[type].dps,35,'primary keeps its stronger burn');
    game.tickStatuses(target,.1);assert.equal(game.scheduledImpacts.length,0,'source cannot bypass its recharge');
  });
}

test('Winter Dominion is a dodgeable cooldown-limited slow burst and proc damage never reschedules it',()=>{
  const {game,hero,add}=arena(),target=add();equip(game,hero,'winters-dominion');
  game.damage(target,10,hero,'magic');assert.equal(game.scheduledImpacts.filter(i=>i.kind==='winter-field').length,1);
  const impact=game.scheduledImpacts[0];close(impact.slow,.405);close(impact.slowDuration,3.375);
  const dodger=add('footman',1,1510,1110);dodger.x=1900;resolve(game);
  close(target.status.slow.factor,.405);assert.equal(dodger.status.slow,undefined);assert.equal(game.scheduledImpacts.length,0);
  game.damage(target,10,hero,'magic');assert.equal(game.scheduledImpacts.length,0);assert.equal(game.progressionDepth,0);
});

test('upgraded protection and sustain epics deliver their larger effects without shield recursion',()=>{
  const {game,hero,add}=arena(),one=add('hero',0,1480),two=add('hero',0,1480,1130),three=add('hero',0,1480,1160);
  equip(game,hero,'sovereign-aegis');two.status.guard={factor:.5,remaining:5};
  game.shield(hero,100,3,hero);assert.equal(one.shield,90);assert.equal(two.shield,90);assert.equal(three.shield,0);
  close(one.status.guard.factor,.85);close(two.status.guard.factor,.5);assert.equal(game.progressionDepth,0);
  const pulse=arena('arc');equip(pulse.game,pulse.hero,'celestial-font');pulse.hero.mana=0;pulse.hero.hp-=200;
  for(let i=0;i<3;i++)onCast(pulse.game,pulse.hero,{id:'fixture'},{slot:0});
  close(pulse.hero.mana,pulse.hero.maxMana*.1+14);assert(pulse.hero.shield>=45);assert(pulse.hero.hp>pulse.hero.maxHp-200);
});

test('upgraded weapon epics deal a heavier cleave, larger star impact and once-per-mobility bonus',()=>{
  const {game,hero,add}=arena(),target=add(),side=add('footman',1,1550,1130);equip(game,hero,'worldsplitter');
  const damage=hero.damage;for(let i=0;i<3;i++)game.damage(target,damage,hero,'physical',false,{basic:true});
  close(side.maxHp-side.hp,damage*.75);close(target.maxHp-target.hp,damage*3.35);
  equip(game,hero,'starfire-scepter');game.damage(target,1,hero,'magic');const blast=game.scheduledImpacts.find(i=>i.kind==='starfire');
  assert.equal(blast.damage,80);assert.equal(blast.r,135);assert.equal(blast.delay,.45);
  const other=arena();equip(other.game,other.hero,'tempest-wings');const foe=other.add();
  onCast(other.game,other.hero,{id:'fixture',mobility:true},{slot:0});close(other.hero.status.haste.factor,.25);close(other.hero.status.haste.attack,.15);
  other.game.damage(foe,10,other.hero,'physical',false,{basic:true});close(foe.maxHp-foe.hp,70);assert(foe.status.slow);
  const before=foe.hp;other.game.damage(foe,10,other.hero,'physical',false,{basic:true});close(before-foe.hp,10);
});

test('Eternity refunds 2.5 seconds without resetting ready cooldowns, and Phoenix heals another injured ally',()=>{
  const {game,hero,add}=arena();equip(game,hero,'eternity-hourglass');hero.cooldowns=[8,.3,6,20];
  for(const slot of [0,1,2])onCast(game,hero,{id:'fixture'},{slot});
  assert.deepEqual(hero.cooldowns,[5.5,.3,6,17.5]);
  equip(game,hero,'phoenix-crown');const one=add('hero',0,1480),two=add('hero',0,1480,1150);one.hp-=500;two.hp-=500;
  game.heal(one,50,hero);assert.equal(one.shield,90);close(two.maxHp-two.hp,500-40*(1+game.bonus(hero,'healing')));
});

test('Elder Guardian adds larger health and splash, and Crimson works at 55 percent health with a stronger ward',()=>{
  const {game,hero,add}=arena('druid');equip(game,hero,'elderwood-staff');
  const summon=game.addUnit('summon',0,hero.x,hero.y,{ownerId:hero.id,hp:100,damage:40});onSummon(game,hero,summon,0);
  close(summon.maxHp,140);close(summon.damage,50);assert.equal(summon.shield,100);
  const target=add(),side=add('footman',1,1540,1130);game.damage(target,10,summon,'physical',false,{basic:true});close(side.maxHp-side.hp,25);
  const before=side.hp;game.damage(target,10,summon,'physical',false,{basic:true});close(side.hp,before);
  const duel=arena();equip(duel.game,duel.hero,'crimson-relic');duel.hero.hp=duel.hero.maxHp*.55;
  const enemy=duel.add('hero'),old=duel.hero.hp;duel.game.damage(enemy,10,duel.hero,'physical',false,{basic:true});
  close(duel.hero.hp-old,duel.hero.maxHp*.10);assert.equal(duel.hero.shield,85);
});

test('new items, active epic recharges and a pending spread resume deterministically',()=>{
  const {game,hero,add}=arena();equip(game,hero,'inferno-heart');economy(game,hero);
  const target=add(),side=add('footman',1,1530,1120);game.applyStatus(target,'burn',4,{dps:20,sourceId:hero.id});game.tickStatuses(target,.05);
  assert(game.scheduledImpacts.some(i=>i.kind==='inferno-chain'));
  const restored=Game.restore(JSON.parse(JSON.stringify(game.serialize())));
  for(const match of [game,restored]){resolve(match);for(let i=0;i<8;i++)match.update(.05);}
  assert.deepEqual(restored.serialize(),game.serialize());assert(side.status.burn);
});

test('Solstice projectile and delayed ultimate apply the slow path once, with cast-time values',()=>{
  for(const equippedBefore of [false,true])for(const slot of [0,3]){
    const {game,hero,add}=arena('solstice'),target=add();hero.level=18;game.refreshStats(hero);hero.mana=hero.maxMana;
    if(equippedBefore)equip(game,hero,'winters-dominion');
    assert(game.cast(hero,slot,target));
    if(!equippedBefore)equip(game,hero,'winters-dominion');
    if(slot===0){for(let n=0;n<5;n++)game.tickProjectiles(.05);}else resolve(game,.75);
    close(target.status.slow.factor,(slot===0?.25:.35)*(equippedBefore?1.35:1));
    close(target.status.slow.remaining,(slot===0?2:3)*(equippedBefore?1.35:1));
  }
});

test('cast-time slows survive later purchases on Brine Undertow, Rivet mines and Vela Covering Shot',()=>{
  for(const id of ['brinehook','rivet','vela']){
    const {game,hero,add}=arena(id),target=add();hero.level=18;game.refreshStats(hero);hero.mana=hero.maxMana;
    if(id==='brinehook')assert(game.chooseTalent(hero,15,'brinehook-rough-current'));
    if(id==='vela')assert(game.chooseTalent(hero,15,'vela-covering-shot'));
    assert(game.cast(hero,id==='vela'?0:2,target));equip(game,hero,'winters-dominion');
    if(id==='brinehook'){
      resolve(game,.65);close(target.status.slow.factor,.3);close(target.status.slow.remaining,2.5);
      target.status={};tickExpansion(game,.05);close(target.status.slow.factor,.2);close(target.status.slow.remaining,.1);
    }else if(id==='rivet'){
      game.time=1;tickExpansionDevice(game,game.units.find(u=>u.subtype==='mine'),.05);
      close(target.status.slow.factor,.35);close(target.status.slow.remaining,2);
    }else{
      for(let n=0;n<5;n++)game.tickProjectiles(.05);
      close(target.status.slow.factor,.2);close(target.status.slow.remaining,1.5);
    }
  }
});

test('Solstice lingering sand keeps the projectile snapshot while the field continues ticking',()=>{
  const {game,hero,add}=arena('solstice'),target=add();hero.level=18;game.refreshStats(hero);hero.mana=hero.maxMana;
  assert(game.chooseTalent(hero,12,'solstice-lingering-sand'));assert(game.cast(hero,0,target));
  equip(game,hero,'winters-dominion');for(let n=0;n<5;n++)game.tickProjectiles(.05);
  assert(game.zones.some(z=>z.type==='time-sand'));target.status={};tickSolstice(game,.05);
  close(target.status.slow.factor,.2);close(target.status.slow.remaining,.15);
});

test('existing forcefields, healing trees and summoned bears keep their slow values through purchases and saves; new casts use the improved path',()=>{
  for(const [id,slot,kind,factor,duration] of [['arc',1,'field',.25,.6],['druid',0,'tree',.3,1.1],['druid',2,'bear',.3,1.2]]){
    const {game,hero,add}=arena(id),target=add();hero.level=18;game.refreshStats(hero);hero.mana=hero.maxMana;
    if(kind==='tree')assert(game.chooseTalent(hero,3,'druid-roots'));
    assert(game.cast(hero,slot,target));equip(game,hero,'winters-dominion');
    const restored=Game.restore(JSON.parse(JSON.stringify(game.serialize())));
    for(const match of [game,restored]){
      const victim=match.units.find(u=>u.id===target.id);
      if(kind==='bear'){const bear=match.units.find(u=>u.alive&&u.subtype==='bear');bear.attackTimer=0;assert(match.basicAttack(bear,victim));}
      else match.tickZones(.05);
      close(victim.status.slow.factor,factor);close(victim.status.slow.remaining,duration);
    }
    game.zones=[];target.status={};hero.cooldowns[slot]=0;hero.mana=hero.maxMana;assert(game.cast(hero,slot,target));
    if(kind==='bear'){const bear=game.units.find(u=>u.alive&&u.subtype==='bear');bear.attackTimer=0;assert(game.basicAttack(bear,target));}
    else game.tickZones(.05);
    close(target.status.slow.factor,factor*1.35);close(target.status.slow.remaining,duration*1.35);
  }
});
