import test from 'node:test';
import assert from 'node:assert/strict';
import {Game} from '../game/moba-mania/engine.mjs';
import {HEROES,WORLD} from '../game/moba-mania/data.mjs';
import {botSkillAim} from '../game/moba-mania/hero-skills.mjs';

const aim=u=>({x:u.x,y:u.y});
function arena(id){
  const game=new Game({hero:id,difficulty:'hard',seed:73}),hero=game.player;
  game.units=[hero];game.waveIn=game.beastIn=game.planIn=1e6;
  Object.assign(hero,{x:1400,y:1100,level:12,mana:1000,maxMana:1000,angle:0});
  const add=(dx,dy=0,team=1,kind='footman')=>game.addUnit(kind,team,hero.x+dx,hero.y+dy,{r:17,hp:5000,speed:0,damage:0,range:30,attackTimer:1000,name:'Practice target'});
  const enemy=add(90);game.rebuildGrid();
  return {game,hero,enemy,add};
}
function advance(game,seconds){for(let t=0;t<seconds;t+=.05)game.update(Math.min(.05,seconds-t));}
function resetCast(hero,slot){hero.cooldowns[slot]=0;hero.mana=1000;}

test('eight selectable heroes keep six combatants and include all three new identities',()=>{
  assert.equal(Object.keys(HEROES).length,8);
  for(const id of ['bulwark','ember','seraph']){
    const game=new Game({hero:id,seed:1}),heroes=game.units.filter(u=>u.kind==='hero');
    assert.equal(game.player.heroId,id);assert.equal(heroes.length,6);
    assert.equal(heroes.filter(u=>u.bot).length,5);
    assert.equal(heroes.filter(u=>u.team===0).length,3);
    assert.equal(heroes.filter(u=>u.team===1).length,3);
    assert(HEROES[id].range<100,'new kits use their melee weapons');
  }
  assert(HEROES.bulwark.hp>HEROES.dragon.hp);
  assert(HEROES.ember.speed>HEROES.dragon.speed);
});

test('all 168 talents have three exclusive choices per tier with small icon metadata',()=>{
  const ids=new Set();let total=0;
  for(const hero of Object.values(HEROES))for(const tier of hero.talents){
    assert.equal(tier.choices.length,3);
    for(const choice of tier.choices){
      assert(!ids.has(choice.id));ids.add(choice.id);total++;
      assert(choice.icon&&choice.color&&choice.description);
    }
  }
  assert.equal(total,168);
  const {game,hero}=arena('seraph');
  assert(game.chooseTalent(hero,5,'seraph-duelist'));
  assert(!game.chooseTalent(hero,5,'seraph-tailwind'));
  assert.equal(hero.talents[5],'seraph-duelist');
  assert(hero.damage>HEROES.seraph.damage);
});

test('Purple Knight shield bash hits a forward arc, pushes and stuns without moving structures',()=>{
  const {game,hero,enemy,add}=arena('bulwark');
  const behind=add(-75),ally=add(65,0,0),tower=add(95,20,1,'tower');tower.tier=0;
  const oldX=enemy.x,oldHp=enemy.hp,towerX=tower.x,behindHP=behind.hp,allyHP=ally.hp;
  assert(game.cast(hero,0,{x:hero.x+200,y:hero.y}));
  assert(enemy.x>oldX+90);assert(enemy.hp<oldHp);assert(enemy.status.stun.remaining>=.8);
  assert.equal(behind.hp,behindHP);assert.equal(ally.hp,allyHP);assert.equal(tower.x,towerX);
  assert(!tower.status.stun);assert(tower.hp<tower.maxHp);
});

test('Purple Knight guard shields the frontline and reduces damage after its shield breaks',()=>{
  const {game,hero,enemy,add}=arena('bulwark'),ally=add(70,30,0),farAlly=add(500,0,0);
  assert(game.cast(hero,1,aim(hero)));
  assert(hero.shield>200);assert.equal(ally.shield,hero.shield*.5);assert.equal(farAlly.shield,0);assert.equal(enemy.shield,0);
  hero.shield=0;const before=hero.hp;game.damage(hero,100,enemy);
  assert(Math.abs(before-hero.hp-60)<1e-8);
  game.tickStatuses(hero,5.01);assert(!hero.status.guard);
});

test('Purple Knight rush hits its actual route and quake adds lasting slow after stun',()=>{
  const {game,hero,enemy}=arena('bulwark');enemy.x=hero.x+150;
  const x=hero.x,hp=enemy.hp;
  assert(game.cast(hero,2,{x:x+230,y:hero.y}));
  assert(hero.x>x+200);assert(enemy.hp<hp);assert(enemy.status.stun);
  enemy.x=hero.x+65;enemy.y=hero.y;enemy.status={};game.rebuildGrid();
  assert(game.cast(hero,3,aim(hero)));assert(!enemy.status.stun,'quake has a visible wind-up');advance(game,.46);assert(enemy.status.stun);assert(enemy.status.slow);
  game.tickStatuses(enemy,1.3);assert(!enemy.status.stun);assert(enemy.status.slow);
});

test('Orange Assassin precise strikes hit one target, follow aim and can miss',()=>{
  const {game,hero,enemy,add}=arena('ember'),second=add(140),offAxis=add(100,90);
  const initial=enemy.hp;
  assert(game.cast(hero,0,{x:hero.x+200,y:hero.y}));
  assert(enemy.hp<initial);assert.equal(second.hp,second.maxHp);assert.equal(offAxis.hp,offAxis.maxHp);
  const hp=enemy.hp;resetCast(hero,0);
  assert(game.cast(hero,0,{x:hero.x,y:hero.y-200}));assert.equal(enemy.hp,hp,'aimed thrust does not seek an enemy outside its path');
});

test('Cinder Dash moves through openings, damages only the first enemy and grants haste',()=>{
  const {game,hero,enemy,add}=arena('ember'),second=add(180),x=hero.x;
  assert(game.cast(hero,1,{x:x+250,y:hero.y}));
  assert(hero.x>x+240);assert(enemy.hp<enemy.maxHp);assert.equal(second.hp,second.maxHp);
  assert.equal(hero.status.haste.factor,.4);assert(game.speed(hero)>HEROES.ember.speed);
});

test('Execution Cut deals substantially more damage to a wounded single target',()=>{
  const damageAt=ratio=>{
    const {game,hero,enemy,add}=arena('ember'),second=add(140);
    enemy.hp=enemy.maxHp*ratio;const before=enemy.hp;
    assert(game.cast(hero,2,{x:hero.x+200,y:hero.y}));
    assert.equal(second.hp,second.maxHp);return before-enemy.hp;
  };
  assert(damageAt(.3)>damageAt(1)*1.5);
});

test('Flicker Strike closes distance and refreshes escape dash only on a kill',()=>{
  for(const kill of [false,true]){
    const {game,hero,enemy}=arena('ember');enemy.x=hero.x+270;if(kill)enemy.hp=100;
    assert(game.chooseTalent(hero,10,'ember-vanish'));hero.cooldowns[1]=8;
    const x=hero.x;assert(game.cast(hero,3,aim(enemy)));
    assert(hero.x>x+190);assert(enemy.hp<enemy.maxHp);
    assert.equal(hero.cooldowns[1],kill?0:8);
    assert.equal(enemy.alive,!kill);assert.equal(hero.shield>0,kill);
  }
});

test('Blue Angel sweeps a wide forward arc while leaving enemies behind untouched',()=>{
  const {game,hero,enemy,add}=arena('seraph'),side=add(70,95),behind=add(-85),ally=add(70,20,0);
  assert(game.cast(hero,0,{x:hero.x+200,y:hero.y}));
  assert(enemy.hp<enemy.maxHp);assert(side.hp<side.maxHp);
  assert.equal(behind.hp,behind.maxHp);assert.equal(ally.hp,ally.maxHp);
  assert.equal(enemy.status.slow.factor,.35);assert(enemy.status.slow.frost);
  assert.equal(game.effects.find(e=>e.type==='sweep').arc,HEROES.seraph.abilities[0].arc);
});

test('Blue Angel Wingburst gives movement, protection and speed',()=>{
  const {game,hero,enemy}=arena('seraph'),x=hero.x;
  assert(game.cast(hero,1,{x:x+220,y:hero.y}));
  assert(hero.x>x+210);assert(hero.shield>0);assert.equal(hero.status.haste.factor,.45);
  assert.equal(enemy.hp,enemy.maxHp,'Wingburst is movement and defense');
});

test('Northern Inspire buffs nearby allies, including minions, with attack speed and shields',()=>{
  const {game,hero,enemy,add}=arena('seraph'),ally=add(80,45,0),farAlly=add(600,0,0),tower=add(50,20,0,'tower');tower.tier=0;
  assert(game.cast(hero,2,aim(hero)));
  for(const target of [hero,ally]){assert.equal(target.status.haste.attack,.3);assert.equal(target.status.haste.factor,.25);assert(target.shield>0);}
  assert(!farAlly.status.haste);assert(!enemy.status.haste);assert(!tower.status.haste);assert.equal(tower.shield,0);
});

test('Winterfall briefly freezes enemies then leaves an icy slow without disabling structures',()=>{
  const {game,hero,enemy,add}=arena('seraph'),tower=add(100,20,1,'tower');tower.tier=0;
  assert(game.cast(hero,3,aim(enemy)));
  assert(!enemy.status.stun,'winterfall can be avoided during its warning');advance(game,.51);
  assert(enemy.status.stun);assert.equal(enemy.status.slow.factor,.5);assert(enemy.status.slow.frost);
  assert(!tower.status.stun);assert(!tower.status.slow);
  game.tickStatuses(enemy,.7);assert(!enemy.status.stun);assert(enemy.status.slow.remaining>3);
  game.tickStatuses(enemy,3.4);assert(!enemy.status.slow);
});

test('movement skills stop before solid jungle obstacles and map edges',()=>{
  for(const [id,slot] of [['bulwark',2],['ember',1],['ember',3],['seraph',1]]){
    const {game,hero}=arena(id);game.units=[hero];
    const obstacle=WORLD.obstacles.find(o=>!WORLD.obstacles.some(other=>other!==o&&Math.hypot(other.x-(o.x-o.r-hero.r-25),other.y-o.y)<other.r+hero.r+4));
    assert(obstacle);
    hero.x=obstacle.x-obstacle.r-hero.r-25;hero.y=obstacle.y;
    const before=hero.x;assert(game.cast(hero,slot,{x:obstacle.x+150,y:hero.y}));
    assert(hero.x>=before);assert(hero.x<obstacle.x-obstacle.r-hero.r,'dash must not emerge beyond the obstacle');
    resetCast(hero,slot);hero.x=hero.r+20;hero.y=100;
    assert(game.cast(hero,slot,{x:-1000,y:100}));assert(hero.x>=hero.r+15);
  }
});

test('Purple Knight knockbacks cannot push targets through terrain or out of the map',()=>{
  const {game,hero,enemy}=arena('bulwark');game.units=[hero,enemy];
  const obstacle=WORLD.obstacles.find(o=>!WORLD.obstacles.some(other=>other!==o&&Math.hypot(other.x-(o.x-o.r-enemy.r-20),other.y-o.y)<other.r+enemy.r+4));
  enemy.x=obstacle.x-obstacle.r-enemy.r-20;enemy.y=obstacle.y;hero.x=enemy.x-60;hero.y=enemy.y;
  assert(game.cast(hero,0,{x:enemy.x+100,y:enemy.y}));
  assert(enemy.x<obstacle.x-obstacle.r-enemy.r);
  resetCast(hero,0);hero.x=90;hero.y=100;enemy.x=45;enemy.y=100;
  assert(game.cast(hero,0,{x:-200,y:100}));assert(enemy.x>=enemy.r+15);
});

test('new talents alter real defensive, displacement, execution and frost behavior',()=>{
  {
    const {game,hero,enemy}=arena('bulwark');assert(game.chooseTalent(hero,3,'bulwark-impact'));
    const x=enemy.x;game.cast(hero,0,aim(enemy));assert(enemy.x>x+150);assert(enemy.status.stun.remaining>1);
  }
  {
    const {game,hero,enemy}=arena('seraph');assert(game.chooseTalent(hero,8,'seraph-deepfreeze'));
    game.cast(hero,3,aim(enemy));advance(game,.5);assert.equal(enemy.status.slow.factor,.65);assert(enemy.status.stun.remaining>=.99);
  }
  {
    const base=arena('ember'),boost=arena('ember');assert(boost.game.chooseTalent(boost.hero,8,'ember-finish'));
    base.game.cast(base.hero,2,aim(base.enemy));boost.game.cast(boost.hero,2,aim(boost.enemy));
    assert(boost.enemy.maxHp-boost.enemy.hp>(base.enemy.maxHp-base.enemy.hp)*1.25);
  }
});

test('new bots use defensive support and escapes intentionally',()=>{
  const cfg={lead:1};
  {
    const {game,hero,enemy,add}=arena('bulwark');hero.hp=hero.maxHp*.6;add(50,40,0);game.rebuildGrid();
    assert(botSkillAim(game,hero,game.getAbility(hero,1),enemy,false,cfg));
    assert.equal(botSkillAim(game,hero,game.getAbility(hero,2),enemy,true,cfg),null,'retreating tanks do not charge an enemy');
  }
  for(const id of ['ember','seraph']){
    const {game,hero,enemy}=arena(id);
    const escape=botSkillAim(game,hero,game.getAbility(hero,1),enemy,true,cfg);
    assert.deepEqual(escape,aim(WORLD.bases[0].well));
  }
  {
    const {game,hero,enemy,add}=arena('seraph');
    assert.equal(botSkillAim(game,hero,game.getAbility(hero,2),enemy,false,cfg),null,'no team buff is wasted without another ally');
    add(55,35,0);assert(botSkillAim(game,hero,game.getAbility(hero,2),enemy,false,cfg));
  }
});

test('Blue Angel can combine inspiration, flight and frost without weakening active effects',()=>{
  const {game,hero,enemy}=arena('seraph');
  game.cast(hero,1,{x:hero.x+40,y:hero.y});
  game.cast(hero,2,aim(hero));
  assert.equal(hero.status.haste.factor,.45,'inspiration preserves stronger flight speed');
  assert.equal(hero.status.haste.attack,.3,'inspiration adds its attack speed');
  game.cast(hero,3,aim(enemy));
  game.cast(hero,0,aim(enemy));
  advance(game,.5);
  assert.equal(enemy.status.slow.factor,.5,'the sword sweep preserves stronger winter freeze');
  assert(enemy.status.slow.frost);
  game.tickStatuses(hero,3.6);
  assert.equal(hero.status.haste.factor,.25,'flight speed ends on time and leaves inspiration active');
  assert.equal(hero.status.haste.attack,.3);
  game.tickStatuses(hero,1.5);assert(!hero.status.haste,'inspiration expires after its own five seconds');
});


test('precise thrusts and sword or shield sweeps do not hit through solid jungle terrain',()=>{
  for(const [id,slot] of [['ember',0],['ember',2],['bulwark',0],['seraph',0]]){
    const {game,hero,enemy}=arena(id),obstacle=WORLD.obstacles[0];
    hero.x=obstacle.x-obstacle.r-hero.r-5;hero.y=obstacle.y;
    enemy.x=obstacle.x+obstacle.r+enemy.r+5;enemy.y=obstacle.y;
    const before=enemy.hp;assert(game.cast(hero,slot,aim(enemy)));
    assert.equal(enemy.hp,before,'solid cover blocks a melee hit');
    assert(!enemy.status.stun);assert(!enemy.status.slow);
  }
});

