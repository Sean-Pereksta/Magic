import test from 'node:test';
import assert from 'node:assert/strict';
import {Game} from '../game/moba-mania/engine.mjs';
import {WORLD} from '../game/moba-mania/data.mjs';

function arena(hero='arc',difficulty='hard'){
  const game=new Game({hero,difficulty,seed:918}),player=game.player;
  game.units=[player];game.campStates=[];game.waveIn=game.beastIn=game.planIn=1e8;
  Object.assign(player,{x:1450,y:1100,attackTimer:0});game.cancelPlayerOrder();game.rebuildGrid();
  return {game,player};
}
function enemy(game,x,y=1100,hp=3000){
  const unit=game.makeHero('dragon',1,0);Object.assign(unit,{x,y,bot:false,speed:0,hp,maxHp:hp});game.rebuildGrid();return unit;
}
function advance(game,seconds,input={}){for(let t=0;t<seconds-1e-8;t+=.05)game.update(Math.min(.05,seconds-t),input);}
function tower(game,team=1,x=1800,y=1100){
  const unit=game.addUnit('tower',team,x,y,{r:28,hp:1950,range:255,damage:55,attackRate:1.05,lane:1,tier:0,attackTimer:0});game.rebuildGrid();return unit;
}

for(const hero of ['dragon','arc'])test(`${hero}: attack selects nearest, closes to its own range, and keeps fighting after release`,()=>{
  const {game,player}=arena(hero),near=enemy(game,player.x+player.range+130),far=enemy(game,near.x+150);
  game.update(.05,{attack:true,aim:{x:far.x,y:far.y}});
  assert.equal(player.targetId,near.id,'aim must not bias nearest enemy selection');
  assert(player.x>1450,'out-of-range attack order starts an approach');
  advance(game,2.5);
  assert(near.hp<near.maxHp,'melee and ranged attacks both land');assert.equal(far.hp,far.maxHp);
  assert.equal(player.targetId,near.id,'button release retains focus');
  assert(Math.hypot(player.x-near.x,player.y-near.y)<=player.range+near.r+player.r*.35+1);
});

test('tapped target remains focused over closer units, then acquires the next enemy after death',()=>{
  const {game,player}=arena('dragon'),near=enemy(game,1510),chosen=enemy(game,1700);
  assert(game.attackTarget(chosen.id));advance(game,.4);assert.equal(player.targetId,chosen.id);
  near.x=player.x+10;advance(game,.2);assert.equal(player.targetId,chosen.id,'a closer enemy cannot steal focus');
  game.kill(chosen,player);game.update(.05);
  assert.equal(player.targetId,near.id);assert.equal(game.attackOrder.explicit,false);
  assert.equal(game.attackOrder.type,'attack');
});

test('a fresh attack press replaces a tap focus with the nearest enemy',()=>{
  const {game,player}=arena(),near=enemy(game,1500),far=enemy(game,1700);
  game.attackTarget(far.id);assert.equal(player.targetId,far.id);
  game.attackNearest();assert.equal(player.targetId,near.id);assert.equal(game.attackOrder.explicit,false);
});

test('Attack waits locally instead of walking toward distant structures or shared team vision',()=>{
  const {game,player}=arena(),distantTower=tower(game,1,2400);
  const ally=game.makeHero('angel',0,1);Object.assign(ally,{x:2300,y:1100,bot:false,speed:0});
  const distantEnemy=enemy(game,2250);
  assert(game.canSee(player.team,distantTower),'known structures are visible across the map');
  assert(game.canSee(player.team,distantEnemy),'the teammate reveals a distant enemy');
  const start={x:player.x,y:player.y};
  assert(game.attackNearest());advance(game,1);
  assert(game.attackOrder,'Attack remains armed for a nearby enemy');
  assert.equal(player.targetId,null,'shared visibility alone must not start a chase');
  assert.equal(game.moveTarget,null);
  assert.deepEqual({x:player.x,y:player.y},start,'pressing Attack without a local enemy stays put');
});

test('an idle Attack order acquires and damages an enemy that enters nearby',()=>{
  const {game,player}=arena(),foe=enemy(game,2400);
  game.attackNearest();advance(game,.2);assert.equal(player.targetId,null);
  foe.x=player.x+120;game.rebuildGrid();advance(game,.7);
  assert.equal(player.targetId,foe.id);assert(foe.hp<foe.maxHp);
});

test('a defeated local target cannot chain an automatic chase across the map',()=>{
  const {game,player}=arena(),chosen=enemy(game,1550);tower(game,1,2400);
  game.attackTarget(chosen.id);game.kill(chosen,player);
  const start={x:player.x,y:player.y};advance(game,.5);
  assert.equal(player.targetId,null);assert.equal(game.attackOrder.explicit,false);
  assert.deepEqual({x:player.x,y:player.y},start);
});

test('an automatically selected enemy escaping the local area ends its chase',()=>{
  const {game,player}=arena(),foe=enemy(game,1600);
  game.attackNearest();advance(game,.1);assert.equal(player.targetId,foe.id);
  foe.x=2400;game.rebuildGrid();const start={x:player.x,y:player.y};advance(game,.5);
  assert.equal(player.targetId,null);assert(game.attackOrder);
  assert.deepEqual({x:player.x,y:player.y},start);
});

test('explicitly selecting a distant visible target still authorizes an approach',()=>{
  const {game,player}=arena(),chosen=tower(game,1,2400);
  assert(game.attackTarget(chosen.id));advance(game,.3);
  assert.equal(player.targetId,chosen.id);assert.equal(game.attackOrder.explicit,true);
  assert(player.x>1450,'explicit target focus can pursue beyond automatic acquisition range');
});

test('movement and new orders cancel pursuit; a held attack cannot restart it',()=>{
  const {game,player}=arena(),foe=enemy(game,1700);
  game.update(.05,{attack:true});assert.equal(player.targetId,foe.id);
  game.update(.05,{attack:true,move:{x:-1,y:0}});assert.equal(game.attackOrder,null);
  advance(game,.2,{attack:true});assert.equal(game.attackOrder,null);
  game.update(.05);game.update(.05,{attack:true});assert(game.attackOrder,'a new press is a new attack order');
  assert(game.movePlayerTo({x:1300,y:1100}));assert.equal(game.attackOrder,null);assert.equal(player.targetId,null);
  const start=player.x;advance(game,.2);assert(player.x<start);
  game.attackTarget(foe.id);assert.equal(game.moveTarget,null);
  game.update(.05,{moveTo:{x:1350,y:1100}});assert.equal(game.attackOrder,null);
});

test('casts, recall, team commands, death, and movement during stun clear old attack orders',()=>{
  const {game,player}=arena(),foe=enemy(game,1650);
  game.attackTarget(foe.id);assert.equal(game.cast(player,3,{x:1700,y:1100}),false);
  assert(game.attackOrder,'an unavailable ability does not cancel focus');
  assert(game.cast(player,0,{x:1650,y:1100}));assert.equal(game.attackOrder,null);
  game.attackTarget(foe.id);assert(game.recall(player));assert.equal(game.attackOrder,null);
  game.attackTarget(foe.id);assert.equal(player.recallIn,0);assert(game.command('push',1));assert.equal(game.attackOrder,null);
  game.attackTarget(foe.id);game.applyStatus(player,'stun',1);game.update(.05,{move:{x:1,y:0}});
  assert.equal(game.attackOrder,null,'move cancels even before a stun wears off');
  player.status={};game.attackTarget(foe.id);game.kill(player,foe);assert.equal(game.attackOrder,null);
  game.respawn(player);assert.equal(game.attackOrder,null);assert.equal(player.targetId,null);
});

test('nearest targeting includes neutral creatures but excludes friends, dead foes, and protected structures',()=>{
  const {game,player}=arena();
  const outer=tower(game,1,2100),inner=game.addUnit('tower',1,1470,1100,{tier:1,lane:1});
  const friendly=game.addUnit('footman',0,1460,1100);
  const dead=enemy(game,1465);dead.alive=false;
  const neutral=game.addUnit('camp',-1,1540,1100,{hp:200,homeX:1540,homeY:1100});
  game.rebuildGrid();game.attackNearest();assert.equal(player.targetId,neutral.id);
  for(const invalid of [friendly,dead,inner])assert.equal(game.attackTarget(invalid.id),false);
  assert.equal(player.targetId,neutral.id,'invalid commands preserve the current valid target');
  game.kill(outer,player);assert(game.attackTarget(inner.id),'newly exposed structures can be selected');
});

test('towers fire damaging projectiles, retain a target, and ramp consecutive hero shots',()=>{
  const {game,player}=arena();player.x=1600;player.hp=player.maxHp=10000;
  const defense=tower(game),shots=[];
  for(let i=0;i<4;i++){
    defense.attackTimer=0;game.tickStructure(defense);const projectile=game.projectiles.at(-1);
    shots.push(projectile.damage);assert.equal(projectile.targetId,player.id);assert.equal(defense.targetId,player.id);
    const hp=player.hp;advance(game,.5);assert(player.hp<hp,'the visible shot must actually damage its target');
  }
  assert(shots[1]>shots[0]&&shots[2]>shots[1]&&shots[3]>shots[2]);
  player.x=1300;game.rebuildGrid();game.tickStructure(defense);assert.equal(defense.targetId,null);
  assert.equal(defense.focusShots,0);
});

test('tower screens minions, retaliates against hero aggression, and returns to a wave after the hero leaves',()=>{
  const {game,player}=arena();player.x=1600;
  const defense=tower(game),guard=enemy(game,1760);
  const minion=game.addUnit('footman',0,1650,1100,{hp:600,maxHp:600,speed:0});game.rebuildGrid();
  game.tickStructure(defense);assert.equal(defense.targetId,minion.id);
  game.damage(guard,10,player);assert.equal(defense.aggroId,player.id);assert.equal(defense.targetId,player.id);
  defense.attackTimer=0;game.tickStructure(defense);assert.equal(game.projectiles.at(-1).targetId,player.id);
  player.x=1200;game.rebuildGrid();defense.attackTimer=0;game.tickStructure(defense);
  assert.equal(defense.targetId,minion.id,'tower must not chase the escaped hero');
});

for(const difficulty of ['easy','normal','hard','ultimate'])test(`${difficulty} bots refuse an unrewarding chase through a live tower`,()=>{
  const {game,player}=arena('dragon',difficulty);player.x=1850;player.bot=false;
  const bot=game.makeHero('ember',1,0);Object.assign(bot,{x:2200,y:1100,level:12,mana:1000,maxMana:1000,thinkIn:99,brain:{action:'fight',targetId:player.id}});
  const defense=tower(game,0,1850);game.rebuildGrid();
  const before=bot.x;game.tickBot(bot,.1);
  assert.equal(bot.aiState,'waiting for wave');assert(bot.x>=before-1,'bot waits outside instead of charging in');
  assert(bot.cooldowns.every(c=>c===0),'a mobility spell cannot bypass the tower decision');
  assert.equal(game.canDiveTower(bot,defense,player),false);
});

test('tower decisions allow a short valuable finish, reject stronger defense, and allow screened structure pushes',()=>{
  const {game,player}=arena('dragon','ultimate');player.x=1820;player.hp=30;player.maxHp=1000;
  const bot=game.makeHero('bulwark',1,0);Object.assign(bot,{x:2100,y:1100,hp:1600,maxHp:1600,damage:100,speed:190});
  const defense=tower(game,0,1850);game.rebuildGrid();
  // Bring the low-health target close enough to finish and escape, rather than chase past the tower.
  player.x=1970;
  assert.equal(game.canDiveTower(bot,defense,player),true,'a healthy coordinated finisher can take a clear reward');
  const extra=game.makeHero('ranger',0,2);Object.assign(extra,{x:1800,y:1120,bot:false});game.rebuildGrid();
  assert.equal(game.canDiveTower(bot,defense,player),false,'an additional healthy defender makes that dive unsafe');
  game.units=game.units.filter(u=>u!==extra);player.x=1400;
  const wave=game.addUnit('footman',1,1950,1100,{hp:450,maxHp:450,speed:0});game.rebuildGrid();
  defense.targetId=wave.id;assert.equal(game.canDiveTower(bot,defense,defense),true,'actual wave pressure permits a structure push');
  wave.alive=false;assert.equal(game.canDiveTower(bot,defense,defense),false,'a dead frontline must immediately remove permission');
});

test('bot travel toward a goal beyond a tower also respects the tower perimeter',()=>{
  const {game,player}=arena('arc');player.x=1300;
  const bot=game.makeHero('dragon',1,0);Object.assign(bot,{x:2200,y:1100,thinkIn:99,brain:{action:'push',goal:{x:1400,y:1100}}});
  const defense=tower(game,0,1850);game.rebuildGrid();game.tickBot(bot,.05);
  assert.equal(bot.aiState,'waiting for wave');
  bot.x=1950;assert.equal(game.towerThreatOnPath(bot,WORLD.bases[1].well),null,'retreat out of tower range remains possible');
  assert(defense.alive);
});

test('switching targets invalidates the old obstacle route even when goals are close together',()=>{
  const {game,player}=arena('dragon');Object.assign(player,{x:421,y:886});
  const first=enemy(game,621,854),second=enemy(game,620.267,927.899);
  game.attackTarget(first.id);game.moveToward(player,first,.05);assert(player.navGoal);
  game.attackTarget(second.id);assert.equal(player.navPath,null);assert.equal(player.navGoal,null);assert.equal(player.navTime,0);
  game.moveToward(player,second,.05);assert.equal(player.navGoal.x,second.x);assert.equal(player.navGoal.y,second.y);
});

test('obstacle detours and local steering cannot route a bot through tower fire',()=>{
  const {game,player}=arena();player.x=100;
  const bot=game.makeHero('dragon',1,0);
  Object.assign(bot,{x:2260.021606,y:743.670268,thinkIn:99,brain:{action:'push',goal:{x:1872.129988,y:465.698167}}});
  const defense=tower(game,0,2260.021606,371.670268);game.rebuildGrid();
  assert.equal(game.towerThreatOnPath(bot,bot.brain.goal),null,'the straight request narrowly misses the tower');
  assert(game.findPath(bot,bot.brain.goal).length>0,'terrain requires a real navigation path');
  const before={x:bot.x,y:bot.y};game.moveToward(bot,bot.brain.goal,.05);
  assert.equal(bot.aiState,'waiting for wave');assert.equal(bot.x,before.x);assert.equal(bot.y,before.y);
  bot.x=defense.x;bot.y=defense.y+defense.range+bot.r+31;
  const y=bot.y;game.moveDirect(bot,0,-1,.1);assert.equal(bot.y,y,'last-step steering cannot enter the danger perimeter');
  defense.x=1600;defense.y=1100;bot.x=1600;bot.y=1280;
  game.moveDirect(bot,0,1,.01);assert(bot.y>1280,'even short outward escape steps remain allowed');
});
