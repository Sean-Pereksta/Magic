import test from 'node:test';
import assert from 'node:assert/strict';
import { Game } from '../game/moba-mania/engine.mjs';
import { ITEMS } from '../game/moba-mania/items.mjs';
import { tickProgression } from '../game/moba-mania/progression.mjs';

const clone=value=>JSON.parse(JSON.stringify(value));
const close=(actual,expected)=>assert(Math.abs(actual-expected)<1e-7,`${actual} should equal ${expected}`);
function arena(hero='dragon'){
  const game=new Game({hero,seed:83}),player=game.player;
  game.units=[player];game.effects=[];game.campStates=[];game.waveIn=game.beastIn=game.planIn=1e8;
  Object.assign(player,{x:1450,y:1100,level:8,attackTimer:0});game.refreshStats(player);player.hp=player.maxHp;player.mana=player.maxMana;
  const enemy=game.addUnit('footman',1,1490,1100,{hp:10000,damage:0,speed:0,attackTimer:1e8});
  game.rebuildGrid();return {game,player,enemy};
}
const events=(game,type)=>game.effects.filter(e=>e.type===type);

test('both river monsters retain the first contest and grow much more strongly at later time milestones',()=>{
  const milestones=[{time:55,tier:1,hp:1350,damage:50},{time:100,tier:2,hp:1944,damage:65},{time:300,tier:4,hp:3132,damage:95},{time:600,tier:7,hp:4914,damage:140}];
  for(const index of [0,1])for(const expected of milestones){
    const {game}=arena();game.time=expected.time;
    const beast=game.spawnBeast(index);assert(beast);
    assert.equal(beast.tier,expected.tier);close(beast.maxHp,expected.hp);close(beast.damage,expected.damage);
    assert.equal(game.spawnBeast(index),null,'an occupied pit never receives a duplicate monster');
  }
});

test('captured monsters retain the defeated tier and stronger species-specific lane statistics for either team',()=>{
  for(const team of [0,1])for(const index of [0,1]){
    const {game,player}=arena();const killer=team?game.makeHero('dragon',1,0):player;
    game.time=300;const beast=game.spawnBeast(index);game.time=900;
    game.damage(beast,1e7,killer,'physical');
    const captured=game.units.find(u=>u.alive&&u.kind==='beast'&&u.team===team);
    assert(captured);assert.equal(captured.tier,4,'capture does not acquire tiers while waiting in its pit');
    close(captured.maxHp,(index?1600:1250)*2.38);close(captured.damage,(index?105:85)*1.96);
    assert.equal(captured.visualSpawn.kind,'capture');
    const arrival=events(game,'beast-capture').at(-1);
    assert.equal(arrival.team,team);assert.equal(arrival.subtype,captured.subtype);assert.equal(arrival.tier,4);
    assert.equal(arrival.createdAt,game.time);
  }
});

test('late monster warnings carry real attack times and their special damage grows with their tier',()=>{
  for(const index of [0,1]){
    const {game,player}=arena();game.time=300;const beast=game.spawnBeast(index);
    Object.assign(player,{x:beast.x+60,y:beast.y});Object.assign(beast,{aggroId:player.id,aggroUntil:1000,slamIn:0,attackTimer:99});
    game.rebuildGrid();const health=player.hp;game.tickNeutral(beast,.05);
    assert.equal(player.hp,health,'a warning is still harmless until its real impact');
    const zones=game.zones.filter(z=>z.ownerId===beast.id);assert.equal(zones.length,index?3:1);
    for(const zone of zones)close(zone.damage,(index?75:95)*1.9);
    close(beast.visualAction.impactAt,game.time+(index?.8:.75));
    assert.equal(beast.visualAction.kind,index?'colossus-rock':'maw-bite');
    assert.equal(events(game,'ground-mark').length,0,'windups do not paint impact scars');
    game.tickZones(.5);assert.equal(player.hp,health);
    game.tickZones(.31);assert(events(game,'ground-mark').some(e=>e.mark==='slam'));
  }
});

test('a captured Colossus scales the actual shield it grants to its wave',()=>{
  const {game,player}=arena();game.time=300;const neutral=game.spawnBeast(1);game.damage(neutral,1e7,player);
  const beast=game.units.find(u=>u.alive&&u.kind==='beast'&&u.team===0);
  const minion=game.addUnit('footman',0,beast.x+30,beast.y,{hp:500,speed:0,damage:0});
  beast.dispatchIn=0;beast.siegeIn=0;game.rebuildGrid();game.tickSiege(beast,.05);
  close(minion.shield,78.75);
});

test('attack and cast poses reflect successful actions without delaying damage or replaying failed casts',()=>{
  const {game,player,enemy}=arena();const health=enemy.hp;
  assert(game.basicAttack(player,enemy));assert(enemy.hp<health,'melee damage still lands immediately');
  assert.equal(player.visualAction.kind,'attack');assert.equal(player.visualAction.impactAt,game.time);
  assert.equal(enemy.visualHit.at,game.time);
  const action=clone(player.visualAction);assert.equal(game.basicAttack(player,enemy),false);assert.deepEqual(player.visualAction,action);
  assert(game.cast(player,3,{x:player.x+80,y:player.y}));
  assert.equal(player.visualAction.abilityId,'sky-cleave');close(player.visualAction.impactAt,game.time+.35);
  assert.equal(player.visualAction.toX,player.x);assert.equal(player.visualAction.toY,player.y);
  const cast=clone(player.visualAction);assert.equal(game.cast(player,3,{x:1600,y:1100}),false);assert.deepEqual(player.visualAction,cast);
});

test('summon appearances and departures preserve real replacement, expiry and combat ownership',()=>{
  for(const subtype of ['treant','wolf','bear','spirit']){
    const {game,player}=arena(subtype==='spirit'?'arc':'druid');
    game.summon(player,subtype,1,{x:player.x+30,y:player.y},10);
    const first=game.units.find(u=>u.kind==='summon'&&u.alive);
    assert.equal(first.visualSpawn.kind,'summon');assert.equal(first.visualSpawn.subtype,subtype);
    assert.equal(events(game,'summon-arrive').at(-1).subtype,subtype);
    assert.equal(first.ownerId,player.id);
    game.summon(player,subtype,1,{x:player.x+30,y:player.y},10);
    assert.equal(first.alive,false);assert(events(game,'summon-depart').some(e=>e.visibilityId===first.id));
    const second=game.units.find(u=>u.kind==='summon'&&u.alive);assert(second);
    game.tickSummon(second,11);assert.equal(second.alive,false);
    assert(events(game,'summon-depart').some(e=>e.visibilityId===second.id));
  }
});

test('heavy spell aftermath starts at impact, including delayed frost, quake and poison',()=>{
  for(const [hero,slot,mark] of [['dragon',1,'scorch'],['seraph',3,'frost'],['bulwark',3,'quake'],['ranger',2,'poison']]){
    const {game,player}=arena(hero);assert(game.cast(player,slot,{x:player.x+70,y:player.y}));
    if(hero==='dragon')assert(events(game,'ground-mark').some(e=>e.mark===mark));
    else{
      assert.equal(events(game,'ground-mark').length,0);
      game.time+=.6;tickProgression(game,.6);game.tickZones(.6);
      assert(events(game,'ground-mark').some(e=>e.mark===mark&&e.createdAt===game.time));
    }
  }
});

test('collapse and reward events describe real defeats, personal gold and advanced purchases',()=>{
  const {game,player,enemy}=arena();
  const helper=game.addUnit('footman',0,player.x+10,player.y,{hp:1000});
  game.damage(enemy,10,player);game.damage(enemy,1e7,helper);assert.equal(events(game,'reward-gold').length,0,'assisting earns no coin flight');
  const target=game.addUnit('footman',1,player.x+40,player.y,{hp:20});game.damage(target,100,player);
  const gold=events(game,'reward-gold').at(-1);assert(gold);assert.equal(gold.amount,18);assert.equal(gold.ownerId,player.id);assert.equal(gold.x,target.x);
  const tower=game.addUnit('tower',1,player.x+100,player.y,{hp:30,tier:0,lane:1,r:28});game.damage(tower,1000,player);
  const collapse=events(game,'structure-collapse').at(-1);assert.equal(collapse.kind,'tower');assert.equal(collapse.createdAt,game.time);assert(collapse.visibleTo.includes(0));
  assert.equal(tower.visualDeath.at,game.time);
  const item=ITEMS.find(i=>i.tier===3);player.items=[...item.requires];player.gold=10000;
  assert(game.buyItem(player,item.id));assert.equal(events(game,'reward-item').at(-1).itemId,item.id);
  const core=game.addUnit('core',1,player.x+100,player.y,{hp:20,r:48});game.damage(core,1e6,player);
  assert.equal(game.winner,0);assert.equal(events(game,'structure-collapse').at(-1).kind,'core');
  const time=game.time;game.update(1);assert.equal(game.time,time,'the finale uses presentation time instead of resuming combat');
});

test('presentation history has a bounded independent pool and uses no combat randomness or IDs',()=>{
  const {game,player}=arena();game.effects=[];
  const rng=game.randomState,nextId=game.nextId,eventId=game.eventId;
  for(let i=0;i<400;i++)game.effect('field',1300+i,1100,20,'#67cfff',5);
  for(let i=0;i<120;i++)game.combatBurst(player,player,'cast','fire');
  for(let i=0;i<100;i++)game.presentation('ground-mark',{x:1200+i*2,y:1100},6,{mark:'scorch',theme:'fire'});
  assert.equal(events(game,'field').length,400);assert.equal(events(game,'combat-burst').length,96);assert.equal(events(game,'ground-mark').length,48);
  assert.equal(game.randomState,rng);assert.equal(game.nextId,nextId);assert.equal(game.eventId,eventId);
  const restored=Game.restore(clone(game.serialize()));assert.equal(restored.effects.length,544);
  assert.deepEqual(restored.serialize(),game.serialize());
});

test('new animation state resumes deterministically while older saves remain valid',()=>{
  const {game,player,enemy}=arena('seraph');
  game.cast(player,3,{x:enemy.x,y:enemy.y});game.damage(player,10,enemy);game.gainXP(player,player.xpNext);
  const restored=Game.restore(clone(game.serialize()));
  for(let i=0;i<18;i++){game.update(.05);restored.update(.05);}
  assert.deepEqual(restored.serialize(),game.serialize());
  const legacy=clone(game.serialize());
  for(const unit of legacy.state.units)for(const field of ['visualAction','visualHit','visualSpawn','visualDeath'])delete unit[field];
  legacy.state.effects=legacy.state.effects.filter(e=>!['ground-mark','structure-collapse','summon-arrive','summon-depart','beast-capture','reward-gold','reward-item','reward-level'].includes(e.type));
  assert.doesNotThrow(()=>Game.restore(legacy).update(.05));
});
