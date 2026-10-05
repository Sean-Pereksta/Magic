import test from 'node:test';
import assert from 'node:assert/strict';
import { createBattleRuntime as runtime } from './runtime-fixture.mjs';

test('whole optimized engine executes a mixed battlefield without transport writes or stale rat double steps',async()=>{
  const h=await runtime();
  assert.ok(h.document.getElementById('unitLayer').children.length>=10);
  const tick=await h.fixture.tick();assert.ok(tick.maxRatStep<=1);assert.equal(tick.invalidSpawn,false);assert.ok(tick.rabbits<=12);
  assert.ok(tick.rabbits>1);assert.ok(tick.fleas>0);
  const before=h.fixture.authority();h.frame(80);h.frame(80);
  assert.equal(h.fixture.authority(),before);assert.deepEqual(h.warnings,[]);
  const stats=h.fixture.stats();assert.ok(stats.ai.peakDecisions<=5);assert.ok(stats.ai.peakPaths<=10);
});
test('real unit factory preserves remote wrappers and interpolates their received movement',async()=>{
  const h=await runtime(),layer=h.document.getElementById('unitLayer');
  const remote=layer.children.find(n=>n.dataset.unitKey==='mouse:remote');
  h.fixture.moveRemote();h.frame(55);const position=h.fixture.rendered('mouse:remote');
  assert.ok(position.x>10 && position.x<11);assert.ok(layer.children.includes(remote));
  h.frame(200);assert.equal(h.fixture.rendered('mouse:remote').x,11);
});
test('local input starts visual movement before the next coalesced board redraw',async()=>{
  const h=await runtime();h.fixture.moveLocal();h.frame(30);
  const position=h.fixture.rendered('mouse:test-player');
  assert.ok(position.x>9 && position.x<10);h.frame(100);assert.equal(h.fixture.rendered('mouse:test-player').x,10);
});
test('actual tower/structure wiring retains reduced-motion markers and bounded effect cleanup',async()=>{
  const h=await runtime(true);h.fixture.warn();h.fixture.fx();h.fixture.hitStructure();h.frame(40);
  const effects=h.document.getElementById('combatEffectLayer');
  assert.ok(effects.children.some(n=>n.classList.contains('cm-marker')));assert.ok(effects.children.some(n=>n.classList.contains('cm-projectile')));
  assert.ok(h.document.getElementById('structureLayer').children.some(n=>n.classList.contains('cm-damaged')));
  h.fixture.overload();h.frame(30);assert.ok(h.fixture.stats().presentation.effects<=128);assert.equal(h.fixture.stats().presentation.reducedMotion,true);
  h.fixture.finish();h.frame(7000);h.frame(1000);assert.equal(h.fixture.stats().presentation.effects,0);assert.deepEqual(h.warnings,[]);
});
test('mixed waves repeatedly run within caps, consume friendly spawns and keep metadata local',async()=>{
  const h=await runtime();
  for(let i=0;i<6;i++){
    h.advance(900);const tick=await h.fixture.tick();h.frame(200);
    assert.ok(tick.maxRatStep<=1);assert.equal(tick.invalidSpawn,false);assert.ok(tick.rabbits<=12);
  }
  assert.ok(h.fixture.stats().ai.peakDecisions<=5);assert.ok(h.fixture.stats().ai.peakPaths<=10);assert.deepEqual(h.warnings,[]);
});

test('real structure damage and remote snapshots both produce the emphasized contact cue',async()=>{
  const host=await runtime(),damage=await host.fixture.enemyImpact();assert.equal(damage.before-damage.after,1);host.frame(30);
  assert.ok(host.document.getElementById('combatEffectLayer').children.some(n=>n.classList.contains('cm-breach')));
  assert.ok(host.document.getElementById('structureLayer').children.some(n=>n.classList.contains('cm-structure-hit')));
  const remote=await runtime();remote.fixture.remoteImpact();remote.frame(30);
  assert.ok(remote.document.getElementById('combatEffectLayer').children.some(n=>n.classList.contains('cm-flare')));
  const count=remote.fixture.stats().presentation.effects;remote.fixture.finish();assert.equal(remote.fixture.stats().presentation.effects,count);
  assert.deepEqual(host.warnings,[]);assert.deepEqual(remote.warnings,[]);
});

test('real rat and cat captures get one fade/caption and animation never changes the resulting authority',async()=>{
  for(const reduced of [false,true])for(const kind of ['rat','cat']){
    const h=await runtime(reduced),capture=await h.fixture.captureMouse(kind);assert.equal(capture.alive,false);assert.equal(capture.killedBy,kind);h.frame(30);
    assert.ok(h.document.getElementById('unitLayer').children.some(n=>n.classList.contains('cm-captured') && n.classList.contains('cm-dying')));
    assert.equal(h.document.getElementById('combatEffectLayer').children.filter(n=>n.textContent==='CAUGHT').length,1);
    const authority=h.fixture.authority();h.frame(1200);assert.equal(h.fixture.authority(),authority);
    assert.equal(h.fixture.stats().presentation.captures,0);assert.equal(h.fixture.stats().presentation.effects,0);
    assert.deepEqual(h.warnings,[]);
  }
});
