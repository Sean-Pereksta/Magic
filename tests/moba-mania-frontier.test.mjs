import test from 'node:test';
import assert from 'node:assert/strict';
import {Game} from '../game/moba-mania/engine.mjs';
import {HEROES} from '../game/moba-mania/data.mjs';
import {FRONTIER_HEROES} from '../game/moba-mania/hero-frontier-data.mjs';
import {captureMotion, renderMotion} from '../game/moba-mania/render-motion.mjs';

function cast(game,slot,aim){
  game.player.mana=game.player.maxMana;
  game.player.cooldowns[slot]=0;
  assert(game.cast(game.player,slot,aim));
}

function arena(id){
  const game=new Game({hero:id,seed:551});
  game.player.level=18;
  game.player.x=1450;game.player.y=1100;
  game.refreshStats(game.player);
  const enemy=game.units.find(u=>u.kind==='hero'&&u.team!==game.player.team);
  Object.assign(enemy,{x:1520,y:1100,bot:false,attackTimer:100});
  const ally=game.units.find(u=>u.kind==='hero'&&u.team===game.player.team&&u!==game.player);
  Object.assign(ally,{x:1460,y:1100,bot:false,hp:200,attackTimer:100});
  game.rebuildGrid();
  return {game,player:game.player,ally,enemy};
}

test('each frontier hero has four castable live abilities and can resume the resulting match',()=>{
  for(const id of Object.keys(FRONTIER_HEROES)){
    const {game,player,enemy,ally}=arena(id);
    const mana=player.maxMana;
    for(let slot=0;slot<4;slot++){
      cast(game,slot,id==='mirelle'&&slot===2?ally:enemy);
      assert(player.mana<mana);
      assert(player.cooldowns[slot]>0);
    }
    assert.equal(Game.restore(game.serialize()).player.heroId,id);
    if(id==='mirelle'){assert(ally.hp>200);assert(game.zones.some(zone=>zone.slow>.25));}
    if(id==='knell'){assert(ally.shield>0);assert(game.scheduledImpacts.some(impact=>impact.kind==='great-silence'));}
    if(id==='zephyr'){assert(player.status.haste?.attack>0);assert(game.scheduledImpacts.some(impact=>impact.kind==='returning-quill'));}
    if(id==='prism'){assert.equal(game.scheduledImpacts.filter(impact=>impact.kind==='spectrum-cross').length,2);assert.equal(game.projectiles.length,3);}
  }
});

test('all 54 late evolutions add a live follow-up and survive serialization',()=>{
  for(const definition of Object.values(HEROES)){
    for(const level of [12,15,18]){
      const {game,player,ally}=arena(definition.id);
      const talent=definition.talents.find(t=>t.level===level).choices.find(c=>c.evolution);
      assert(game.chooseTalent(player,level,talent.id),talent.id);
      const previous=new Set(game.effects);
      cast(game,talent.evolution.slot,ally);
      if(['echo','snare','burn','venom','stun'].includes(talent.evolution.effect)){
        const followup=game.scheduledImpacts.find(impact=>impact.kind==='talent-followup');
        assert(followup,talent.id);
        assert(Game.restore(game.serialize()).scheduledImpacts.some(impact=>impact.id===followup.id),talent.id);
      }else{
        assert(game.effects.some(effect=>!previous.has(effect)&&effect.r===120),talent.id);
      }
      assert.equal(Game.restore(game.serialize()).player.talents[level],talent.id);
    }
  }
});

test('motion interpolation preserves simulation state and snaps discontinuities',()=>{
  const {game,player}=arena('zephyr');
  const shot=game.fireSkill(player,0,{speed:600,range:320,r:6});
  const before=captureMotion(game);
  player.x+=10;shot.x+=20;
  const view=renderMotion(game,before,.5);
  assert.equal(view.player.x,player.x-5);
  assert.equal(view.projectiles[0].x,shot.x-10);
  assert.equal(game.player.x,1460);
  assert.equal(view.pointVisible(0,player.x,player.y),game.pointVisible(0,player.x,player.y));
  player.x+=200;
  assert.equal(renderMotion(game,before,.5).player.x,player.x);
  game.paused=true;
  assert.equal(renderMotion(game,before,.5),game);
});
