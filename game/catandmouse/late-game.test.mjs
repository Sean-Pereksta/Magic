import test from 'node:test';
import assert from 'node:assert/strict';
import { createBattleRuntime } from './runtime-fixture.mjs';
import { lateGameFixture } from './late-game-fixture.mjs';

const late=()=>createBattleRuntime(false,{scenarioExtra:lateGameFixture});
test('420-building redraws avoid quadratic tower scans and preserve every unchanged structure',async()=>{
  const h=await late();assert.equal(h.fixture.lateGame(),420);
  const layer=h.document.getElementById('structureLayer'),nodes=[...layer.children],before=h.fixture.reads();
  for(let i=0;i<40;i++)h.fixture.draw();
  assert.ok(h.fixture.reads()-before<=80);assert.deepEqual(layer.children,nodes);assert.equal(layer.children.length,420);
  assert.ok(h.fixture.stats().structureQueries.stinkCached<=16);assert.deepEqual(h.warnings,[]);
});
test('health-only updates reuse path topology and shield coverage; destruction invalidates both',async()=>{
  const h=await late();h.fixture.lateGame();await h.fixture.shields();
  const revision=h.fixture.structureRevision(),stats=h.fixture.stats().structureQueries,st=h.fixture.firstStructure();
  h.fixture.hpUpdate(st.x,st.y);assert.equal(h.fixture.structureRevision(),revision);
  await h.fixture.shields();assert.equal(h.fixture.stats().structureQueries.shieldBuilds,stats.shieldBuilds);
  h.fixture.removeStructure(st.x,st.y);assert.ok(h.fixture.structureRevision()>revision);
  await h.fixture.shields();assert.ok(h.fixture.stats().structureQueries.shieldBuilds>stats.shieldBuilds);
});
test('unchanged terrain performs no texture lookups and blood updates only its dirty cell',async()=>{
  const h=await late();h.fixture.lateGame();const reads=h.fixture.terrainReads();
  for(let i=0;i<10;i++)h.fixture.draw();assert.equal(h.fixture.terrainReads(),reads);
  h.fixture.terrainEffect();assert.equal(h.fixture.terrainReads(),reads+1);
  assert.match(h.document.getElementById('cell-1-28').dataset.texture,/grass blood/);
});
test('pickup and trap nodes survive unchanged redraws instead of recreating their sprites',async()=>{
  const h=await late();h.fixture.pickups();const layer=h.document.getElementById('entityLayer'),nodes=[...layer.children];
  assert.ok(nodes.some(n=>n.classList.contains('iso-mousetrap')));
  for(let i=0;i<10;i++)h.fixture.draw();assert.deepEqual(layer.children,nodes);
});
test('late combat prunes retired slime history while keeping AI and presentation budgets bounded',async()=>{
  const h=await late();h.fixture.lateGame();h.fixture.addSlimeHistory();assert.equal(h.fixture.slimeSize(),5000);
  await h.fixture.tick();assert.ok(h.fixture.slimeSize()<100);
  for(let i=0;i<12;i++){h.advance(700);await h.fixture.tick();h.frame(80);}
  const stats=h.fixture.stats();assert.ok(stats.ai.peakDecisions<=5);assert.ok(stats.ai.peakPaths<=10);
  assert.ok(stats.presentation.effects<=128);assert.ok(stats.structureQueries.stinkCached<=16);assert.deepEqual(h.warnings,[]);
});

test('a level-60 four-player battle at the shared population cap retains bounded effects and AI over repeated ticks',async()=>{
  const h=await late();h.fixture.lateGame();const initial=h.fixture.lateActors();assert.equal(initial.living,initial.cap);
  for(let i=0;i<20;i++){
    h.advance(700);const result=await h.fixture.tick();h.frame(80);
    assert.ok(result.rabbits<=12);assert.ok(h.fixture.unitCounts().fleas<=8);assert.ok(result.maxRatStep<=1);assert.equal(result.invalidSpawn,false);
  }
  const stats=h.fixture.stats();assert.ok(stats.ai.peakDecisions<=5);assert.ok(stats.ai.peakPaths<=10);assert.ok(stats.presentation.effects<=128);
  h.frame(5000);h.frame(1000);assert.equal(h.fixture.stats().presentation.effects,0);assert.deepEqual(h.warnings,[]);
});
