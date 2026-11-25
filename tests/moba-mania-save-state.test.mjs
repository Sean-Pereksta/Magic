import test from 'node:test';
import assert from 'node:assert/strict';
import { Game } from '../game/moba-mania/engine.mjs';

function arena(id='ranger'){
  const game=new Game({hero:id,seed:918}),hero=game.player;
  game.units=[hero];game.campStates=[];game.waveIn=game.beastIn=game.planIn=1e8;
  while(hero.level<18)game.gainXP(hero,hero.xpNext);
  Object.assign(hero,{x:1450,y:1100,attackTimer:0,bot:false,angle:0});
  const target=game.addUnit('footman',1,1620,1100,{hp:10000,damage:0,speed:0,attackTimer:1e8});
  game.rebuildGrid();return {game,hero,target};
}
const clone=value=>JSON.parse(JSON.stringify(value));
const advance=(game,seconds)=>{for(let t=0;t<seconds-1e-8;t+=.05)game.update(.05);};

test('restore rejects malformed cast history before a later cast can dereference it',()=>{
  const {game,hero}=arena('arc');assert(game.cast(hero,1,hero));
  const snapshot=game.serialize();
  for(const history of [null,[null],[{}],[{slot:4,time:0}],[{slot:1,time:1}],Array(17).fill({slot:1,time:0})]){
    const saved=clone(snapshot);saved.state.units.find(u=>u.id===hero.id).evolution.castHistory=history;
    assert.throws(()=>Game.restore(saved),/Invalid saved match: invalid cast history/);
  }
  const restored=Game.restore(clone(snapshot));
  for(const match of [game,restored])assert(match.cast(match.player,2,match.player));
  assert.deepEqual(restored.serialize(),game.serialize(),'valid cast counters and subsequent effects resume unchanged');
});

test('restore rejects missing skill-shot velocity and invalid projectile geometry or hit sets',()=>{
  const {game,hero,target}=arena();assert(game.cast(hero,0,target));const snapshot=game.serialize();
  for(const corrupt of [p=>delete p.vx,p=>delete p.vy,p=>p.vx=null,p=>delete p.angle,p=>p.r='wide',p=>p.hit.extra=true,p=>p.hit.__mobaSet=[null]]){
    const saved=clone(snapshot);corrupt(saved.state.projectiles[0]);
    assert.throws(()=>Game.restore(saved),/Invalid saved match: invalid projectile/);
  }
  const restored=Game.restore(clone(snapshot));
  for(const match of [game,restored])advance(match,.5);
  assert(restored.units.find(u=>u.id===target.id).hp<target.maxHp,'restored skill shot still lands');
  assert.deepEqual(restored.serialize(),game.serialize());
});

test('fresh homing shots restore without initial velocity and hit the same target',()=>{
  const {game,hero,target}=arena();assert(game.basicAttack(hero,target));
  assert.equal(game.projectiles[0].vx,undefined);assert.equal(game.projectiles[0].vy,undefined);
  const restored=Game.restore(clone(game.serialize()));
  for(const match of [game,restored])advance(match,.5);
  assert(restored.units.find(u=>u.id===target.id).hp<target.maxHp);
  assert.deepEqual(restored.serialize(),game.serialize(),'homing acquires finite velocity after resuming');
});

function warnedCast(shape){
  const id=shape==='cone'?'bulwark':shape==='line'?'ranger':'seraph';
  const context=arena(id),{game,hero,target}=context;
  if(shape==='cone')assert(game.chooseTalent(hero,18,'bulwark-fissure'));
  if(shape==='line')assert(game.chooseTalent(hero,18,'ranger-deadfall'));
  assert(game.cast(hero,3,target));
  assert.equal(game.scheduledImpacts[0].shape,shape);return context;
}

test('restore rejects incomplete cone and line warning geometry and malformed impact payloads',()=>{
  for(const shape of ['circle','cone','line']){
    const {game}=warnedCast(shape),snapshot=game.serialize();
    const corruptions=[i=>i.shape='triangle',i=>i.delay=null,i=>i.damage='strong',i=>i.stun={},i=>i.slow=2,i=>i.interruptible=1];
    if(shape!=='circle')corruptions.push(i=>delete i.angle);
    if(shape==='cone')corruptions.push(i=>delete i.arc,i=>i.arc=-1);
    if(shape==='line')corruptions.push(i=>delete i.range,i=>delete i.width,i=>i.width=-1);
    for(const corrupt of corruptions){
      const saved=clone(snapshot);corrupt(saved.state.scheduledImpacts[0]);
      assert.throws(()=>Game.restore(saved),/Invalid saved match: invalid (scheduled impact|impact)/);
    }
  }
});

test('valid circle, cone and line warnings resume with identical impacts and no duplicate hits',()=>{
  for(const shape of ['circle','cone','line']){
    const {game,target}=warnedCast(shape),before=target.hp,restored=Game.restore(clone(game.serialize()));
    for(const match of [game,restored])advance(match,.8);
    assert(target.hp<before,shape+' lands');assert.equal(restored.scheduledImpacts.length,0);
    assert.deepEqual(restored.serialize(),game.serialize(),shape+' resumes deterministically');
    const hp=restored.units.find(u=>u.id===target.id).hp;
    advance(restored,.1);assert.equal(restored.units.find(u=>u.id===target.id).hp,hp,shape+' does not resolve twice');
  }
});

test('restoring a malformed zone shape or non-array impact queue fails before ticking',()=>{
  const {game,hero,target}=arena('arc');assert(game.cast(hero,0,target));const snapshot=game.serialize();
  for(const corrupt of [s=>s.zones[0].shape='triangle',s=>s.zones[0].shape='cone',s=>s.zones[0].range='far',s=>s.scheduledImpacts=0,s=>s.scheduledImpacts=null]){
    const saved=clone(snapshot);corrupt(saved.state);
    assert.throws(()=>Game.restore(saved),/Invalid saved match/);
  }
});
