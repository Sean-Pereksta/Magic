// Compare identical saved campaigns against two source snapshots. To isolate
// performance, both snapshots must have the same economy and AI tuning.
// node game/iron-throne/tests/turn-performance-benchmark.mjs BEFORE_DIR AFTER_DIR SAVE...
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const [beforeDirectory,afterDirectory,...saves]=process.argv.slice(2);
if(!beforeDirectory||!afterDirectory||!saves.length)throw new Error('Provide two Iron Thrones source directories and one or more campaign saves.');
const before=await import(pathToFileURL(resolve(beforeDirectory,'diplomacy.mjs')));
const after=await import(pathToFileURL(resolve(afterDirectory,'diplomacy.mjs')));

for(const save of saves){
  const initial=JSON.parse(await readFile(save,'utf8'));
  const results=[];
  for(const [name,engine] of [['before',before],['after',after]]){
    const state=structuredClone(initial),start=performance.now();
    let aiMilliseconds=0;
    engine.endTurn(state,progress=>{
      if(progress.phase==='resolving')aiMilliseconds=performance.now()-start;
    });
    results.push({name,state,totalMilliseconds:performance.now()-start,aiMilliseconds});
  }
  const [oldResult,newResult]=results;
  assert.deepEqual(newResult.state,oldResult.state,`Turn ${initial.turn} changed orders, discoveries, battles, treasuries or other campaign state.`);
  console.log(JSON.stringify({save,turn:initial.turn,houses:initial.kingdoms.length,difficulty:initial.difficulty,
    beforeMilliseconds:Math.round(oldResult.totalMilliseconds),afterMilliseconds:Math.round(newResult.totalMilliseconds),
    reductionPercent:Math.round((1-newResult.totalMilliseconds/oldResult.totalMilliseconds)*100),
    beforeAiMilliseconds:Math.round(oldResult.aiMilliseconds),afterAiMilliseconds:Math.round(newResult.aiMilliseconds),
    identicalCampaignState:true}));
}
