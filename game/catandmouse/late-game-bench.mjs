import { performance } from 'node:perf_hooks';
import { createBattleRuntime } from './runtime-fixture.mjs';
import { lateGameFixture } from './late-game-fixture.mjs';

const h=await createBattleRuntime(false,{scenarioExtra:lateGameFixture});
const structures=h.fixture.lateGame(420);
const samples=[],before=h.fixture.reads();
for(let i=0;i<40;i++){
  const start=performance.now();h.fixture.draw();samples.push(performance.now()-start);
}
const renderStructureListReads=h.fixture.reads()-before;
samples.sort((a,b)=>a-b);
const shieldReadsBefore=h.fixture.reads(),start=performance.now();await h.fixture.shields();
console.log(JSON.stringify({structures,renders:samples.length,renderStructureListReads,shieldStructureListReads:h.fixture.reads()-shieldReadsBefore,
  renderMedianMs:+samples[20].toFixed(2),renderP95Ms:+samples[38].toFixed(2),shieldTickMs:+(performance.now()-start).toFixed(2)},null,2));
console.log('CPU/operation benchmark in a deterministic DOM fixture; these numbers are not browser FPS.');
