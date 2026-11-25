import test from 'node:test';
import assert from 'node:assert/strict';
import {Game} from '../game/moba-mania/engine.mjs';
import {HEROES} from '../game/moba-mania/data.mjs';
import {ITEM_BY_ID} from '../game/moba-mania/items.mjs';
import {tickProgression} from '../game/moba-mania/progression.mjs';

function arena(id='arc',level=18){
  const game=new Game({hero:id,seed:836}),hero=game.player;
  game.units=[hero];game.campStates=[];game.waveIn=game.beastIn=game.planIn=1e8;
  while(hero.level<level)game.gainXP(hero,hero.xpNext);
  Object.assign(hero,{x:1450,y:1100,attackTimer:0,bot:false,angle:0});
  const add=(x=1500,y=1100,team=1,kind='footman')=>{
    const unit=kind==='hero'?game.makeHero('dragon',team,team?0:1):game.addUnit(kind,team,x,y,{hp:10000});
    Object.assign(unit,{x,y,hp:10000,maxHp:10000,damage:0,speed:0,bot:false,attackTimer:1e8,range:0,status:{}});
    game.rebuildGrid();return unit;
  };
  const target=add();return {game,hero,target,add};
}
function advance(game,seconds){for(let t=0;t<seconds-1e-8;t+=.05)game.update(Math.min(.05,seconds-t));}
function cast(game,hero,slot,at=hero){hero.cooldowns[slot]=0;hero.mana=hero.maxMana;assert(game.cast(hero,slot,{x:at.x,y:at.y}));}
function equip(game,hero,id){
  hero.gold=1e6;const item=ITEM_BY_ID[id];
  for(const prerequisite of item.requires)if(!hero.items.includes(prerequisite))equip(game,hero,prerequisite);
  if(!hero.items.includes(id))assert(game.buyItem(hero,id));
}
function basic(game,hero,target){
  hero.attackTimer=0;game.rebuildGrid();assert(game.basicAttack(hero,target));
  if(hero.range>100){for(let i=0;i<8&&game.projectiles.some(p=>p.basic);i++)game.tickProjectiles(.05);}
}
function choose(game,hero,level,id){assert(game.chooseTalent(hero,level,id));}

test('every hero has meaningful late choice milestones with enforced locks and exclusive branches',()=>{
  for(const id of Object.keys(HEROES)){
    const {game,hero}=arena(id,10);
    assert.deepEqual(HEROES[id].talents.map(t=>t.level),[3,5,8,10,12,15,18]);
    for(const level of [12,15,18]){
      const tier=HEROES[id].talents.find(t=>t.level===level);
      assert.equal(tier.choices.length,3);
      assert(!game.chooseTalent(hero,level,tier.choices[0].id));
      while(hero.level<level)game.gainXP(hero,hero.xpNext);
      assert(game.chooseTalent(hero,level,tier.choices[0].id));
      assert(!game.chooseTalent(hero,level,tier.choices[1].id));
      assert(Object.values(tier.choices[0].bonuses).some(n=>n>0));
    }
  }
});

test('Azure Conduit triggers on a third successful cast and failed casts never charge it',()=>{
  const {game,hero,target}=arena('arc');
  cast(game,hero,1,target);cast(game,hero,1,target);
  const before=target.hp;
  assert(!game.cast(hero,1,target));assert.equal(target.hp,before);
  cast(game,hero,1,target);assert(target.hp<before);assert(hero.mana>hero.maxMana-game.getAbility(hero,1).mana);
});
test('Kindled Claws bursts only after the third basic hit on burning prey',()=>{
  const {game,hero,target,add}=arena('dragon'),near=add(target.x+30,target.y+50);
  game.applyStatus(target,'burn',5,{dps:1,sourceId:hero.id});
  for(let i=0;i<2;i++)basic(game,hero,target);assert.equal(near.hp,near.maxHp);
  basic(game,hero,target);assert(near.hp<near.maxHp);
});
test('Wild Bond wards each cast leader and living summons can heal a wounded allied hero',()=>{
  const {game,hero,add}=arena('druid'),ally=add(hero.x+40,hero.y+50,0,'hero');ally.hp-=500;
  cast(game,hero,1,hero);
  const summons=game.units.filter(u=>u.kind==='summon'),first=summons[0],second=summons[1];
  assert(first.shield>0);assert.equal(second.shield,0);
  const target=add(first.x+25,first.y),before=ally.hp;
  first.attackTimer=0;assert(game.basicAttack(first,target));assert(ally.hp>before);
});
test('Devotion rewards meaningful support and never grows from healing full health',()=>{
  const {game,hero,add}=arena('angel'),ally=add(hero.x+40,hero.y,0,'hero');
  for(let i=0;i<5;i++)game.heal(ally,40,hero);assert(!ally.status.haste);
  ally.hp-=400;hero.mana-=50;
  for(let i=0;i<3;i++)game.heal(ally,30,hero);
  assert(ally.status.haste);assert(hero.mana>hero.maxMana-50);
});
test('Marked Quarry requires three hits on the same target and reveals poisoned prey',()=>{
  const {game,hero,target,add}=arena('ranger'),second=add(target.x+15,target.y+40);
  basic(game,hero,target);basic(game,hero,target);basic(game,hero,second);assert(!target.status.poison);assert(!second.status.poison);
  basic(game,hero,second);basic(game,hero,second);assert(second.status.poison);assert(second.revealedUntil>game.time);
});
test('Resolute Counter grants a ward after damage then a basic counterattack, with a recharge',()=>{
  const {game,hero,target}=arena('bulwark');
  basic(game,hero,target);assert.equal(hero.shield,0);
  game.damage(hero,20,target);basic(game,hero,target);assert(hero.shield>0);
  hero.shield=0;game.damage(hero,20,target);basic(game,hero,target);assert.equal(hero.shield,0);
});
test('Cinder Opening empowers exactly one basic hit following a movement skill',()=>{
  const {game,hero,target}=arena('ember');
  target.x=hero.x+275;cast(game,hero,1,{x:hero.x+220,y:hero.y});
  const before=target.hp;basic(game,hero,target);const empowered=before-target.hp;
  const second=target.hp;basic(game,hero,target);assert(empowered>second-target.hp);
});
test('Winter Rhythm gives its third hit extra frost damage and useful allied haste',()=>{
  const {game,hero,target,add}=arena('seraph'),ally=add(hero.x+30,hero.y+40,0,'hero');
  basic(game,hero,target);basic(game,hero,target);assert(!target.status.slow);
  const before=target.hp;basic(game,hero,target);assert(before-target.hp>hero.damage);assert(target.status.slow?.frost);assert(ally.status.haste);
});

test('Sovereign Aegis shares a new shield once and cannot recursively bounce between owners',()=>{
  const {game,hero,add}=arena('bulwark'),ally=add(hero.x+35,hero.y,0,'hero');
  equip(game,hero,'sovereign-aegis');equip(game,ally,'sovereign-aegis');
  game.shield(hero,100,3,hero);assert(ally.shield>0);assert.equal(hero.shield,100);assert.equal(game.progressionDepth,0);
  ally.shield=0;game.shield(hero,200,3,hero);assert.equal(ally.shield,0,'shared ward is on cooldown');
});
test('Worldsplitter cleaves on every third attack without replacing target focus',()=>{
  const {game,hero,target,add}=arena('dragon'),side=add(hero.x+100,hero.y+45);
  equip(game,hero,'worldsplitter');game.attackTarget(target.id);
  basic(game,hero,target);basic(game,hero,target);assert.equal(side.hp,side.maxHp);
  basic(game,hero,target);assert(side.hp<side.maxHp);assert.equal(game.attackOrder.targetId,target.id);
});
test('Starfire Scepter leaves a dodgeable delayed blast and its own blast cannot chain',()=>{
  const {game,hero,target,add}=arena('ember'),side=add(target.x,target.y+45);
  equip(game,hero,'starfire-scepter');cast(game,hero,0,target);
  assert.equal(side.hp,side.maxHp);assert(game.scheduledImpacts.some(i=>i.kind==='starfire'));
  advance(game,.5);assert(side.hp<side.maxHp);assert(!game.scheduledImpacts.some(i=>i.kind==='starfire'));assert.equal(game.progressionDepth,0);
});
test('Eternity Hourglass rewards three different abilities and respects its proc cooldown',()=>{
  const {game,hero}=arena('arc');equip(game,hero,'eternity-hourglass');
  cast(game,hero,0,{x:1800,y:1100});const first=hero.cooldowns[0];
  cast(game,hero,1,hero);cast(game,hero,2,hero);assert(hero.cooldowns[0]<=first-1.24);
  const before=hero.cooldowns[0];cast(game,hero,1,hero);cast(game,hero,2,hero);cast(game,hero,3,hero);assert.equal(hero.cooldowns[0],before);
});
test('Celestial Font returns mana and heals nearby allies on every third cast',()=>{
  const {game,hero,add}=arena('dragon'),ally=add(hero.x+40,hero.y+30,0,'hero');equip(game,hero,'celestial-font');ally.hp-=400;
  cast(game,hero,2);cast(game,hero,2);const before=ally.hp;
  cast(game,hero,2);assert(ally.hp>before);assert(hero.mana>hero.maxMana-game.getAbility(hero,2).mana);
});
test('Tempest Wings empower one basic hit after mobility and retain recipe ingredients',()=>{
  const plain=arena('seraph'),geared=arena('seraph');equip(geared.game,geared.hero,'tempest-wings');
  for(const a of [plain,geared]){a.target.x=a.hero.x+270;cast(a.game,a.hero,1,{x:a.hero.x+220,y:a.hero.y});}
  const before=geared.target.hp;basic(geared.game,geared.hero,geared.target);const first=before-geared.target.hp;
  const next=geared.target.hp;basic(geared.game,geared.hero,geared.target);assert(first>next-geared.target.hp);
  assert(ITEM_BY_ID['tempest-wings'].requires.every(id=>geared.hero.items.includes(id)));
});
test('Thunderbolt Crossbow chains to at most three additional nearby foes every third hit',()=>{
  const {game,hero,target,add}=arena('dragon'),one=add(target.x+45,target.y),two=add(target.x+80,target.y+35),third=add(target.x+120,target.y+40);
  equip(game,hero,'thunderbolt-crossbow');for(let i=0;i<2;i++)basic(game,hero,target);assert.equal(one.hp,one.maxHp);
  basic(game,hero,target);assert(one.hp<one.maxHp);assert(two.hp<two.maxHp);assert(third.hp<third.maxHp);
});
test('Phoenix Crown wards actual healing and never activates on an empty heal',()=>{
  const {game,hero,add}=arena('angel'),ally=add(hero.x+30,hero.y,0,'hero');equip(game,hero,'phoenix-crown');
  game.heal(ally,50,hero);assert.equal(ally.shield,0);
  ally.hp-=400;game.heal(ally,50,hero);assert(ally.shield>0);
  ally.shield=0;game.heal(ally,50,hero);assert.equal(ally.shield,0,'healing ward has a cooldown');
});
test('Elderwood Staff empowers only the first new summon and recasting keeps army size bounded',()=>{
  const {game,hero}=arena('druid');equip(game,hero,'elderwood-staff');cast(game,hero,1,hero);
  let summons=game.units.filter(u=>u.kind==='summon'&&u.alive);assert.equal(summons.length,2);
  assert(summons[0].elderGuardian);assert(!summons[1].elderGuardian);assert(summons[0].maxHp>summons[1].maxHp);assert(summons[0].damage>summons[1].damage);
  cast(game,hero,1,hero);summons=game.units.filter(u=>u.kind==='summon'&&u.alive);assert.equal(summons.length,2);assert.equal(summons.filter(u=>u.elderGuardian).length,1);
});
test('Crimson Relic requires a hero hit below 60% health and does not retrigger during cooldown',()=>{
  const {game,hero,add}=arena('dragon'),enemy=add(hero.x+50,hero.y,1,'hero');equip(game,hero,'crimson-relic');hero.hp=hero.maxHp*.3;
  const before=hero.hp;basic(game,hero,enemy);assert(hero.hp>before);assert(hero.shield>0);
  hero.shield=0;basic(game,hero,enemy);assert.equal(hero.shield,0);
});

test('Arc Rift capstone makes a real ranged Spirit Captain',()=>{
  const {game,hero}=arena('arc');choose(game,hero,18,'arc-captain');cast(game,hero,2,hero);
  const summons=game.units.filter(u=>u.kind==='summon');assert.equal(summons.filter(u=>u.captain).length,1);assert(summons[0].range>100);assert(summons[0].maxHp>summons[1].maxHp);
});
test('Dragon Phoenix Scales rescue surviving low-health damage without reviving a lethal hit',()=>{
  const {game,hero,target}=arena('dragon');choose(game,hero,18,'dragon-phoenix');hero.hp=hero.maxHp*.31;
  game.damage(hero,hero.maxHp*.04,target);assert(hero.shield>0);assert(hero.alive);
  hero.shield=0;game.damage(hero,1e8,target);assert(!hero.alive);assert.equal(hero.hp,0);
});
test('Druid Ancient Grove creates a delayed wider healing and protection bloom',()=>{
  const {game,hero,add}=arena('druid'),ally=add(hero.x+135,hero.y,0,'hero');choose(game,hero,18,'druid-ancient-grove');ally.hp-=500;
  cast(game,hero,0,hero);assert.equal(ally.shield,0);advance(game,1.01);assert(ally.shield>0);assert(ally.hp>9500);
});
test('Pink Angel Rose Sanctum restores real health when Sanctuary is cast',()=>{
  const {game,hero,add}=arena('angel'),ally=add(hero.x+40,hero.y,0,'hero');choose(game,hero,18,'angel-sanctum');ally.hp-=500;
  cast(game,hero,2,ally);assert(ally.hp>9500);assert(ally.shield>0);
});
test('Dark Ranger Night Hunter adds damage and self-healing to the third mark hit',()=>{
  const {game,hero,target}=arena('ranger');choose(game,hero,18,'ranger-night-hunter');hero.hp-=250;
  basic(game,hero,target);basic(game,hero,target);const hp=hero.hp,before=target.hp;
  basic(game,hero,target);assert(hero.hp>hp);assert(before-target.hp>hero.damage);
});
test('Purple Knight Walking Fortress protects allies only when its warned quake lands',()=>{
  const {game,hero,target,add}=arena('bulwark'),ally=add(hero.x+40,hero.y+30,0,'hero');choose(game,hero,18,'bulwark-fortress');
  cast(game,hero,3,hero);assert.equal(ally.shield,0);assert(!target.status.stun);advance(game,.46);
  assert(ally.shield>0);assert(ally.status.guard);assert(target.status.stun);
});
test('Orange Assassin Fresh Opening resets skills on a hero final hit only',()=>{
  const {game,hero,target,add}=arena('ember');choose(game,hero,18,'ember-reset');hero.cooldowns[1]=8;hero.cooldowns[2]=8;
  game.damage(target,1e8,hero);assert.equal(hero.cooldowns[1],8);
  const enemy=add(hero.x+40,hero.y,1,'hero');game.damage(enemy,1e8,hero);assert.equal(hero.cooldowns[1],0);assert.equal(hero.cooldowns[2],4);
});
test('Blue Angel Glacial Crown turns the third basic hit into team protection',()=>{
  const {game,hero,target,add}=arena('seraph'),ally=add(hero.x+30,hero.y+20,0,'hero');choose(game,hero,18,'seraph-glacial-crown');
  basic(game,hero,target);basic(game,hero,target);assert.equal(ally.shield,0);basic(game,hero,target);assert(ally.shield>0);
});

test('late intermediate evolutions change cast behavior before level18',()=>{
  const {game,hero,target}=arena('arc',12);choose(game,hero,12,'arc-echo');cast(game,hero,0,target);
  assert(game.scheduledImpacts.some(i=>i.kind==='echo'));
  const {game:druidGame,hero:druid}=arena('druid',15);choose(druidGame,druid,15,'druid-cycle-grove');druid.cooldowns[0]=8;cast(druidGame,druid,1,druid);assert.equal(druid.cooldowns[0],6.5);
});
test('big crowd-control warnings allow dodging and are cancelled by a stun or caster death',()=>{
  for(const cause of ['dodge','stun','death']){
    const {game,hero,target}=arena('seraph');cast(game,hero,3,target);const before=target.hp;
    assert(game.scheduledImpacts.length>0);assert(!target.status.stun);
    if(cause==='dodge')target.y+=300;
    if(cause==='stun')game.applyStatus(hero,'stun',1,{sourceId:target.id});
    if(cause==='death')game.kill(hero,target);
    advance(game,.55);assert.equal(target.hp,before);assert(!target.status.stun);assert.equal(game.scheduledImpacts.length,0);
  }
});
test('warned impact state survives JSON serialization and damages each victim exactly once',()=>{
  const {game,hero,target}=arena('bulwark');cast(game,hero,3,hero);
  game.scheduledImpacts=JSON.parse(JSON.stringify(game.scheduledImpacts));
  game.time=.46;const before=target.hp;tickProgression(game,.46);assert(target.hp<before);
  const after=target.hp;tickProgression(game,.1);assert.equal(target.hp,after);assert.equal(game.scheduledImpacts.length,0);
});
test('an already released item blast can finish a target after its owner dies without reviving them',()=>{
  const {game,hero,target,add}=arena('ember'),secondary=add(target.x+20,target.y+25);
  equip(game,hero,'starfire-scepter');hero.gold=0;hero.goldEarned=0;
  cast(game,hero,0,target);secondary.hp=1;game.kill(hero,target);
  advance(game,.5);assert(!secondary.alive);assert.equal(hero.gold,18);assert(!hero.alive);assert.equal(hero.hp,0);
});
test('equipment proc final hits retain personal gold and never generate chain income or assist gold',()=>{
  const {game,hero,target,add}=arena('dragon'),secondary=add(hero.x+95,hero.y+35),helper=add(hero.x-10,hero.y,0,'hero');
  equip(game,hero,'worldsplitter');hero.gold=0;hero.goldEarned=0;secondary.hp=1;
  game.damage(secondary,.1,helper);basic(game,hero,target);basic(game,hero,target);basic(game,hero,target);
  assert(!secondary.alive);assert.equal(hero.gold,18);assert.equal(helper.gold,0);
  assert.equal(game.progressionDepth,0);assert.equal(game.scheduledImpacts.length,0);
});
