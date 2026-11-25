import test from 'node:test';
import assert from 'node:assert/strict';
import { Game } from '../game/moba-mania/engine.mjs';
import { WORLD } from '../game/moba-mania/data.mjs';

const clone=value=>JSON.parse(JSON.stringify(value));
const bursts=(game,fx)=>game.effects.filter(e=>e.type==='combat-burst'&&(!fx||e.fx===fx));
function arena(heroId='arc'){
  const game=new Game({hero:heroId,seed:918}),hero=game.player;
  game.units=[hero];game.effects=[];game.campStates=[];game.waveIn=game.beastIn=game.planIn=1e8;
  Object.assign(hero,{x:1450,y:1100,level:12,attackTimer:0,bot:false,angle:0});
  game.refreshStats(hero);hero.hp=hero.maxHp;hero.mana=hero.maxMana;
  const target=game.addUnit('footman',1,1540,1100,{r:17,hp:10000,damage:0,speed:0,attackTimer:1e8});
  game.rebuildGrid();return {game,hero,target};
}
const advance=(game,seconds)=>{for(let t=0;t<seconds-1e-8;t+=.05)game.update(Math.min(.05,seconds-t));};

test('landed damage adds one directional burst without inventing hits on rejected damage',()=>{
  const {game,hero,target}=arena('dragon'),before=target.hp;
  assert.equal(game.damage(target,40,hero,'fire'),40);
  assert.equal(target.hp,before-40);
  assert.equal(bursts(game,'hit').length,1);
  const hit=bursts(game,'hit')[0];
  assert.equal(hit.theme,'fire');assert.equal(hit.visibilityId,target.id);
  assert.equal(hit.x,target.x);assert.equal(hit.y,target.y);
  assert(Number.isFinite(hit.angle));assert(hit.visibleTo.includes(hero.team));

  game.effects=[];
  const ally=game.addUnit('footman',0,hero.x+20,hero.y,{hp:100});
  for(const amount of [0,-20,NaN,Infinity])assert.equal(game.damage(target,amount,hero),0);
  assert.equal(game.damage(ally,20,hero),0);
  target.alive=false;assert.equal(game.damage(target,20,hero),0);
  assert.equal(bursts(game).length,0,'invalid, friendly and dead targets never produce an impact');

  const protectedGame=new Game({hero:'arc',seed:918});protectedGame.effects=[];
  const inner=protectedGame.units.find(u=>u.kind==='tower'&&u.team===1&&u.tier===1);
  assert.equal(protectedGame.damage(inner,30,protectedGame.player),0);
  assert.equal(bursts(protectedGame).length,0,'a protected structure did not receive a landed hit');
});

test('a non-piercing projectile draws its hit before the projectile is removed; a miss does not',()=>{
  const {game,hero,target}=arena('ranger'),before=target.hp;
  assert(game.basicAttack(hero,target));assert.equal(game.projectiles.length,1);
  for(let i=0;i<20&&game.projectiles.length;i++)game.tickProjectiles(.05);
  assert.equal(game.projectiles.length,0);assert(target.hp<before);
  assert.equal(bursts(game,'hit').filter(e=>e.visibilityId===target.id).length,1);
  assert.equal(bursts(game,'hit')[0].theme,'steel','ordinary arrow hits stay visually distinct from venom');

  game.effects=[];const hp=target.hp;
  game.fireSkill(hero,-Math.PI/2,{type:'arrow',color:'#fff1cf',r:4,range:150,speed:600,damage:30,pierce:0});
  for(let i=0;i<8;i++)game.tickProjectiles(.05);
  assert.equal(target.hp,hp);assert.equal(bursts(game,'hit').length,0);
});

test('emitted visibility preserves hidden brush targets and captures visible deaths before removal',()=>{
  const {game,hero,target}=arena(),brush=WORLD.brush[0];
  Object.assign(target,{x:brush.x,y:brush.y});Object.assign(hero,{x:brush.x-brush.r-130,y:brush.y});
  assert(game.pointVisible(hero.team,target.x,target.y));assert.equal(game.canSee(hero.team,target),false);
  const unseen=game.combatBurst(target,hero,'hit','arcane');
  assert(!unseen.visibleTo.includes(hero.team),'blind area damage cannot reveal a unit hidden in brush');
  game.effects=[];hero.x=brush.x-90;assert(game.canSee(hero.team,target));
  game.damage(target,target.hp*2,hero);
  assert.equal(target.alive,false);
  const death=bursts(game,'death').find(effect=>effect.visibilityId===target.id);
  assert(death?.visibleTo.includes(hero.team),'visibility is captured before the defeated unit stops granting sight');
});

test('failed casts stay quiet while successful abilities add their own casting color',()=>{
  const {game,hero,target}=arena('seraph');
  const mana=hero.mana;hero.mana=0;
  assert.equal(game.cast(hero,0,target),false);assert.equal(bursts(game,'cast').length,0);
  hero.mana=mana;assert(game.cast(hero,0,target));
  assert.equal(bursts(game,'cast').length,1);assert.equal(bursts(game,'cast')[0].theme,'frost');
  game.effects=[];assert.equal(game.cast(hero,0,target),false);
  assert.equal(bursts(game,'cast').length,0,'cooldown denial does not flash a spell');
});

test('shield identity follows its granting ability and survives a weaker refresh',()=>{
  for(const [heroId,slot,theme] of [['dragon',2,'fire'],['bulwark',1,'violet'],['angel',2,'holy'],['seraph',2,'frost'],['arc',1,'arcane']]){
    const {game,hero}=arena(heroId);
    assert(game.cast(hero,slot,hero));
    if(heroId==='arc')game.tickZones(.01);
    assert(hero.shield>0,heroId+' actually granted a shield');
    assert.equal(hero.shieldVisual.theme,theme);
    assert.equal(hero.shieldVisual.amount,hero.shield);
  }

  const {game,hero,target}=arena('dragon');
  game.shield(target,120,3,hero);const original=clone(target.shieldVisual);
  const frost=game.makeHero('seraph',1,1);game.time=.5;
  game.shield(target,60,5,frost);
  assert.equal(target.shield,120);assert.deepEqual(target.shieldVisual,original,'a weaker refresh keeps the active stronger shield appearance');
  game.shield(target,180,5,frost);
  assert.equal(target.shieldVisual.theme,'frost');assert.equal(target.shieldVisual.amount,180);
  game.tickStatuses(target,5.1);
  assert.equal(target.shield,0);assert(!target.shieldVisual,'expired shields cannot leave a stale shell');
});

test('shield absorption, shield break and health damage produce distinct feedback',()=>{
  const {game,hero,target}=arena('dragon');
  game.shield(target,100,4,hero);game.effects=[];const hp=target.hp;
  assert.equal(game.damage(target,35,hero,'fire'),0);
  assert.equal(target.hp,hp);assert.equal(target.shield,65);
  assert.equal(bursts(game,'hit').length,0,'no health hit flashes through an intact shield');
  assert.equal(bursts(game,'shield-hit').length,1);
  assert.equal(bursts(game,'shield-hit')[0].theme,'fire');

  game.effects=[];assert.equal(game.damage(target,80,hero,'physical'),15);
  assert.equal(target.shield,0);assert.equal(target.hp,hp-15);
  assert.equal(bursts(game,'shield-break').length,1);assert.equal(bursts(game,'hit').length,1);
  const frost=game.makeHero('seraph',1,1);game.shield(target,50,2,frost);
  assert.equal(target.shieldVisual.theme,'frost','a fresh shield after a break takes the new granting source identity');
});

test('quiet damage over time and regeneration do not flood the battle with particles',()=>{
  const {game,hero,target}=arena('dragon');
  let emitted=0;const effect=game.effect;
  game.effect=function(...args){if(args[0]==='combat-burst')emitted++;return effect.apply(this,args);};
  game.applyStatus(target,'burn',3,{dps:20,sourceId:hero.id});const hp=target.hp;
  advance(game,1.2);
  assert(target.hp<hp);assert(emitted<8,'continuous damage should not emit on every simulation step');

  game.effects=[];hero.hp-=100;emitted=0;
  for(let i=0;i<100;i++)game.heal(hero,.2,null,true);
  assert.equal(emitted,0,'quiet recovery stays quiet');
  game.heal(hero,30,hero);assert.equal(bursts(game,'heal').length,1);
  game.effects=[];hero.hp=hero.maxHp;game.heal(hero,100,hero);
  assert.equal(bursts(game,'heal').length,0,'overhealing should not imply a successful heal');
});

test('cosmetic bursts have a separate bounded pool and never evict lasting ability fields',()=>{
  const {game,hero,target}=arena();
  const persistent=game.effect('field',hero.x,hero.y,130,'#86d8ff',5);
  for(let i=1;i<400;i++)game.effect('ring',hero.x,hero.y,20,'#fff1cf',2);
  for(let i=0;i<180;i++)game.combatBurst({...target,id:1000+i},hero,'hit','arcane',1);
  assert(bursts(game).length>0&&bursts(game).length<=96);
  assert(game.effects.includes(persistent),'a particle-heavy clash must retain the shield-field visual');
  assert.equal(game.effects.filter(e=>e.type!=='combat-burst').length,400);
  assert(game.effects.length<=496,'the combined pool fits the saved-match limit');
  game.effect('ring',hero.x,hero.y,20,'#fff1cf',2);
  assert(game.effects.length<=496,'adding legacy effects is bounded too');
});

test('cosmetic emission is deterministic and does not consume combat randomness or IDs',()=>{
  const first=arena('dragon'),second=arena('dragon');
  const before=first.game.serialize();
  const emit=({game,hero,target})=>{
    for(const [fx,theme] of [['hit','fire'],['shield-break','frost'],['heal','nature']])game.combatBurst(target,hero,fx,theme,1.4);
  };
  emit(first);emit(second);
  assert.deepEqual(bursts(first.game),bursts(second.game));
  const after=first.game.serialize();after.state.effects=before.state.effects;
  assert.deepEqual(after,before,'drawing feedback cannot alter gameplay state, RNG or identifiers');
  assert.equal(first.game.random(),second.game.random(),'the next simulation random value is unchanged');
});

test('colored shields and mid-impact bursts resume deterministically, including legacy shields',()=>{
  const {game,hero,target}=arena('dragon');
  game.shield(hero,140,3,hero);game.damage(target,40,hero,'fire');
  const restored=Game.restore(clone(game.serialize()));
  assert.deepEqual(restored.player.shieldVisual,hero.shieldVisual);
  assert.deepEqual(bursts(restored),bursts(game));
  for(const match of [game,restored])advance(match,.4);
  assert.deepEqual(restored.serialize(),game.serialize(),'saving during the burst preserves replay timing and gameplay');

  const legacy=clone(game.serialize());delete legacy.state.units.find(u=>u.id===hero.id).shieldVisual;
  const older=Game.restore(legacy);assert.equal(older.player.shield,hero.shield);
  older.tickStatuses(older.player,4);assert.equal(older.player.shield,0);
});
