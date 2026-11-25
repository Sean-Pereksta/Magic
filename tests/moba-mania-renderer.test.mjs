import test from 'node:test';
import assert from 'node:assert/strict';
import { Game } from '../game/moba-mania/engine.mjs';
import { Renderer } from '../game/moba-mania/renderer.mjs';

// Observe the warning countdown without requiring a browser or a native canvas.
function countdowns(game) {
  const arcs=[];
  const context={save(){},restore(){},setLineDash(){},beginPath(){},moveTo(){},lineTo(){},closePath(){},fill(){},stroke(){},arc(x,y,r,start,end){if(r===13)arcs.push((end-start)/(Math.PI*2));}};
  Renderer.prototype.drawTelegraphs.call({visible:()=>true,label(){}},context,game);
  return arcs;
}

test('delayed zone warnings fill with the real countdown and stop after their first impact',()=>{
  for(const [hero,slot] of [['ranger',2],['arc',0]]) {
    const game=new Game({hero,seed:73}),player=game.player;
    game.units=[player];game.campStates=[];game.waveIn=game.beastIn=game.planIn=1e8;
    player.level=8;game.refreshStats(player);player.mana=player.maxMana;game.rebuildGrid();
    assert(game.cast(player,slot,{x:player.x,y:player.y}));
    assert.deepEqual(countdowns(game),[0]);
    game.tickZones(.275);
    assert(Math.abs(countdowns(game)[0]-.5)<1e-9,'the warning reaches half-full halfway to the blast');
    game.tickZones(.3);
    assert.deepEqual(countdowns(game),[],'the initial warning disappears after detonation');
    if(hero==='arc') {
      assert.equal(game.zones.length,1,'storm continues dealing damage after its warning ends');
      game.tickZones(.1);
      assert.deepEqual(countdowns(game),[],'later storm pulses do not restart the initial warning');
    }
  }
});

test('beast shrine labels do not disclose an objective outside allied vision',()=>{
  const game=new Game({hero:'arc',seed:73});
  game.units=[game.player];game.rebuildGrid();
  const beast=game.spawnBeast(0),labels=[];
  const context={save(){},restore(){},setLineDash(){},beginPath(){},moveTo(){},lineTo(){},stroke(){}};
  const draw=()=>{
    labels.length=0;
    Renderer.prototype.drawObjectiveGround.call({visible:()=>true,label(c,text){labels.push(text);}},context,game);
    return labels.includes('CLAIM THE BEAST');
  };
  assert.equal(game.canSee(0,beast),false);
  assert.equal(draw(),false);
  game.player.x=beast.x-100;game.player.y=beast.y;game.rebuildGrid();
  assert.equal(game.canSee(0,beast),true);
  assert.equal(draw(),true);
});
