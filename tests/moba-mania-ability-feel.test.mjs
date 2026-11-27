import test from 'node:test';
import assert from 'node:assert/strict';
import { Game } from '../game/moba-mania/engine.mjs';
import { OnlineMatch } from '../game/moba-mania/online-match.mjs';
import { prismCrossBeams, prismPylons } from '../game/moba-mania/prism.mjs';

function arena(id){
  const game=new Game({hero:id,seed:214}),hero=game.player;
  game.units=[hero];
  game.campStates=[];
  game.waveIn=game.beastIn=game.planIn=1e8;
  Object.assign(hero,{x:1450,y:1100,level:6,mana:1000,maxMana:1000,attackTimer:0});
  return {game,hero};
}
function addHero(game,id,team,x,y=1100){
  const hero=game.makeHero(id,team,1);
  return Object.assign(hero,{x,y,level:6,bot:false,attackTimer:60,hp:3000,maxHp:3000});
}
function addMinion(game,x,y=1100){
  return game.addUnit('footman',1,x,y,{hp:500,speed:0,damage:0,attackTimer:60});
}

test('Silver Volley holds position on existing input and a deliberate new move refunds an unfired volley',()=>{
  const {game,hero}=arena('vela'),move={move:{x:1,y:0}};
  game.update(.05,move);
  const start=hero.x;
  assert(game.cast(hero,3,{x:hero.x+200,y:hero.y}));
  game.update(.2,move);
  assert.equal(hero.x,start);
  assert.equal(hero.channel.nextShot,0);
  game.update(.025,{move:{x:0,y:0}});
  game.update(.025,move);
  assert(!hero.channel);
  assert(hero.x>start);
  assert.equal(hero.cooldowns[3],17.75);
});

test('online movement refreshes preserve the volley and a new movement press cancels it',()=>{
  const match=new OnlineMatch({seed:4,roster:[
    [{hero:'vela',playerId:'vela'},{hero:'prism'},{hero:'solstice'}],
    [{hero:'arc'},{hero:'dragon'},{hero:'druid'}]
  ]});
  const hero=match.players.get('vela');
  Object.assign(hero,{level:6,x:1450,y:1100});
  const move=x=>match.command('vela',{type:'move',x,y:0});
  move(1);match.advance(.1);
  assert(match.command('vela',{type:'cast',slot:3,x:1700,y:1100}));
  const start=hero.x;
  move(1);match.advance(.2);
  assert.equal(match.snapshot('vela').units.find(unit=>unit.isPlayer).channel.nextShot,0);
  assert.equal(hero.x,start);
  move(0);move(-1);match.advance(.1);
  assert(!hero.channel);
  assert(hero.x<start);
});

test('Execution Cut and Flicker Strike reach heroes through a wave while Heartseeker hits the first body',()=>{
  const {game,hero}=arena('ember'),enemy=addHero(game,'arc',1,1575),minion=addMinion(game,1510);
  enemy.hp=170;
  assert(game.cast(hero,0,enemy));
  assert(minion.hp<500);
  assert.equal(enemy.hp,170);
  const minionHP=minion.hp;
  assert(game.cast(hero,2,enemy));
  assert(!enemy.alive);
  assert.equal(minion.hp,minionHP);
  const next=addHero(game,'dragon',1,1675);
  next.hp=200;hero.cooldowns[1]=8;
  assert(game.cast(hero,3,next));
  assert(!next.alive);
  assert.equal(hero.cooldowns[1],0);
});

test('Rampart Rush stops in melee reach while Shield Bash keeps a long peel',()=>{
  const {game,hero}=arena('bulwark'),enemy=addHero(game,'arc',1,1585);
  assert(game.cast(hero,2,{x:hero.x+235,y:hero.y}));
  assert(enemy.hp<enemy.maxHp);
  assert(game.basicAttack(hero,enemy));
  const before=enemy.x;
  assert(game.cast(hero,0,enemy));
  assert(enemy.x>=before+100);
});

test('Great Silence refunds once on stun and a missed windup pays the full cost',()=>{
  const {game,hero}=arena('knell'),enemy=addHero(game,'arc',1,1540),mana=hero.mana;
  assert(game.cast(hero,3,hero));
  game.applyStatus(hero,'stun',.03,{sourceId:enemy.id});
  assert.equal(hero.mana,mana-47.5);
  assert.equal(hero.cooldowns[3],20);
  game.applyStatus(hero,'stun',.03,{sourceId:enemy.id});
  assert.equal(hero.cooldowns[3],20);
  assert(game.effects.some(effect=>effect.type==='bell-crack'));
  assert(!game.scheduledImpacts.some(impact=>impact.kind==='great-silence'));
  game.update(.8);
  assert.equal(enemy.hp,enemy.maxHp);
  enemy.x+=600;hero.cooldowns[3]=0;hero.mana=mana;
  assert(game.cast(hero,3,hero));
  game.update(.8);
  assert(hero.cooldowns[3]>39);
  assert(hero.mana<mana-90);
});

test('overlapping guard keeps its strongest reduction and resumes the surviving weaker layer',()=>{
  const {game,hero}=arena('dragon'),ally=addHero(game,'knell',0,1490);
  game.applyStatus(hero,'guard',1.4,{factor:.85,sourceId:ally.id});
  assert(game.cast(hero,2,hero));
  assert.equal(hero.status.guard.factor,.5);
  game.tickStatuses(hero,3.9);
  assert.equal(hero.status.guard.factor,.5);
  game.applyStatus(hero,'guard',1.4,{factor:.85,sourceId:ally.id});
  const restored=Game.restore(game.serialize());
  restored.tickStatuses(restored.player,1.2);
  assert.equal(restored.player.status.guard.factor,.85);
  restored.tickStatuses(restored.player,.3);
  assert(!restored.player.status.guard);
});

test('Moonlit Pack hunts the marked hero even when its owner is attacking a minion',()=>{
  const {game,hero}=arena('druid'),enemy=addHero(game,'arc',1,1610),minion=addMinion(game,1500);
  hero.targetId=minion.id;
  assert(game.cast(hero,3,enemy));
  const wolves=game.units.filter(unit=>unit.subtype==='wolf');
  assert.equal(wolves.length,3);
  game.update(.05);
  assert(wolves.every(wolf=>wolf.targetId===enemy.id));
  assert(game.effects.some(effect=>effect.type==='wolf-hunt'&&effect.followId===enemy.id));
  game.time=3.01;
  game.tickSummon(wolves[0],.05);
  assert.equal(wolves[0].targetId,minion.id);
});

test('Borrowed Seconds refunds active slots and shields a ready ally',()=>{
  const {game,hero}=arena('solstice'),spender=addHero(game,'dragon',0,1470),ready=addHero(game,'arc',0,1530);
  spender.cooldowns=[4,5,0,12];
  assert(game.cast(hero,3,{x:1510,y:1100}));
  game.update(.75);
  assert.deepEqual(spender.cooldownRefunds.amounts,[1.875,2,0]);
  assert(spender.cooldowns[3]>11);
  assert.equal(spender.shield,0);
  assert(ready.shield>0);
  assert.doesNotThrow(()=>Game.restore(game.serialize()));
});

test('Mirror Pylons reject blocked placements, replace the oldest and make Facet Bolt turn',()=>{
  const {game,hero}=arena('prism'),offAngle=addHero(game,'arc',1,1650,1195);
  const relay={x:1590,y:1100};
  assert.equal(game.cast(hero,1,hero),false);
  assert.equal(hero.cooldowns[1],0);
  assert(game.cast(hero,1,relay));
  const oldest=prismPylons(game,hero)[0];
  assert.equal(oldest.noVision,true);
  assert(game.cast(hero,0,relay));
  game.update(.5);
  assert(offAngle.hp<offAngle.maxHp);
  assert(hero.metrics.damageToHeroes>0);
  assert(game.effects.some(effect=>effect.type==='prism-flare'&&effect.followId===oldest.id));
  hero.cooldowns[1]=0;assert(game.cast(hero,1,{x:1500,y:1170}));
  hero.cooldowns[1]=0;assert(game.cast(hero,1,{x:1650,y:1160}));
  assert(!oldest.alive);
  assert.equal(prismPylons(game,hero).length,2);
});

test('Prism echoes and reflected rays match the layout and break with their pylon',()=>{
  const {game,hero}=arena('prism');
  assert(game.cast(hero,1,{x:1520,y:1170}));
  const [pylon]=prismPylons(game,hero);
  const echoVictim=addHero(game,'arc',1,1520,1190),center={x:1700,y:1100};
  assert(game.cast(hero,2,center));
  game.update(.75);
  assert(echoVictim.hp<echoVictim.maxHp);
  assert(echoVictim.status.slow.factor>0);
  const target=addHero(game,'dragon',1,center.x,center.y),ability=game.getAbility(hero,3);
  assert.equal(prismCrossBeams(game,hero,center,0,ability.radius).length,3);
  assert(game.cast(hero,3,center));
  const restored=Game.restore(game.serialize());
  game.damage(pylon,pylon.hp+1,target);
  game.update(.85);restored.update(.85);
  const full=restored.units.find(unit=>unit.id===target.id);
  assert(full.hp<target.hp);
  assert(Math.abs(target.maxHp-target.hp-184*1.375)<1e-6);
  assert(game.effects.some(effect=>effect.type==='prism-beam'));
});
