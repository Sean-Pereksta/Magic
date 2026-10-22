const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

function match(faction,difficulty='easy'){
  const run=simulation(faction,{factions:['frontier',faction]});
  run(`Math.random=()=>.314159;chosenMapId='classic';chosenDifficulty='${difficulty}';createGame([{name:'Human',factionId:'frontier',ai:false},{name:'AI',factionId:'${faction}',ai:true,aiDifficulty:'${difficulty}',aiStyle:'balanced'}]);performance.now=()=>testClock;mapSeed=123456;tileVisibleToFaction=(f,x,y)=>!!visionCache.get(f)?.has(x+','+y)`);
  return run;
}

function develop(faction,difficulty){
  const run=match(faction,difficulty);
  const report=run.plain(`(()=>{const start=testClock;let firstArmy=null,firstProducer=null,firstScout=null,firstWave=null,peakArmy=0,maxWave=0;const initial={minerals:players[1].minerals,gas:players[1].gas,workers:workerCommitment(1)};
    for(let tick=0;tick<1440;tick++){testClock+=250;updatePersistentOrders();updateCombat(.25);resolveProjectileImpacts();updateProduction(.25);updateRegeneration(.25);updateAreaEffects();updateMining();updateLastSeenIntel();aiThinkAll();
      const p=players[1],seconds=(testClock-start)/1000,army=playerUnits(1).filter(u=>!isWorker(u));peakArmy=Math.max(peakArmy,army.length);maxWave=Math.max(maxWave,p.strategy.wave?.ids.length||0);
      if(army.length&&firstArmy===null)firstArmy=seconds;if(playerStructures(1).some(s=>(STRUCTURES[s.type].produces||[]).some(t=>!UNITS[t].worker)||s.type==='spawningPool')&&firstProducer===null)firstProducer=seconds;
      if(p.strategy.scoutId&&firstScout===null)firstScout=seconds;if(p.strategy.wave&&firstWave===null)firstWave=seconds;
    }return {initial,firstArmy,firstProducer,firstScout,firstWave,peakArmy,maxWave,workers:workerCommitment(1),mineralsMined:ensureMatchStats(players[1]).minerals,gasMined:ensureMatchStats(players[1]).gas,structures:playerStructures(1).map(s=>s.type)};})()`);
  return report;
}

for(const faction of ['frontier','ascendant','brood'])test(`${faction}: Easy develops a real economy, scouts and attacks while remaining smaller than Medium`,()=>{
  const easy=develop(faction,'easy'),medium=develop(faction,'medium');
  assert.deepEqual(easy.initial,{minerals:360,gas:0,workers:4});
  assert.ok(easy.firstProducer!==null&&easy.firstProducer<=45,JSON.stringify(easy));
  assert.ok(easy.firstArmy!==null&&easy.firstArmy<=75,JSON.stringify(easy));
  assert.ok(easy.firstScout!==null&&easy.firstScout<=90,JSON.stringify(easy));
  assert.ok(easy.firstWave!==null&&easy.firstWave<=240,JSON.stringify(easy));
  assert.ok(easy.mineralsMined>=700&&easy.gasMined>0,JSON.stringify(easy));
  assert.ok(easy.workers>=4&&easy.workers<=5,JSON.stringify(easy));
  assert.ok(easy.peakArmy<medium.peakArmy,JSON.stringify({easy,medium}));
  assert.ok(easy.maxWave<=medium.maxWave,JSON.stringify({easy,medium}));
});

test('Easy assigns idle starting workers before its first slow macro decision',()=>{
  const run=match('frontier');
  const result=run.plain(`(()=>{let decisions=0;aiThink=()=>decisions++;for(let tick=0;tick<6;tick++){testClock+=250;updatePersistentOrders();updateMining();aiThinkAll()}return {decisions,working:playerUnits(1).filter(w=>w.miningNodeId||w.economyNodeId).length}})()`);
  assert.equal(result.decisions,0);assert.equal(result.working,4);
});

test('Ascendant opening power and basic production work even when every optional macro roll declines',()=>{
  const run=match('ascendant');
  const result=run.plain(`(()=>{rand=()=>.999;for(let tick=0;tick<320;tick++){testClock+=250;updatePersistentOrders();updateProduction(.25);updateMining();updateLastSeenIntel();aiThinkAll()}return {power:countType(1,'pylon'),producer:countType(1,'gateway'),army:playerUnits(1).filter(u=>!isWorker(u)).length}})()`);
  assert.ok(result.power>=1);assert.equal(result.producer,1);assert.ok(result.army>=1);
});

test('a completed gas structure gains one worker and low-workforce economies return to minerals',()=>{
  const run=simulation();
  const result=run.plain(`(()=>{players[0].ai=true;players[0].aiDifficulty='easy';players[0].minerals=360;players[0].gas=0;addEntity('structure','commandHub',0,10,10);addResourceCluster({x:10,y:10});const minerals=resourceNodes.filter(n=>n.type==='mineral');for(const n of minerals.slice(0,5))addEntity('unit','engineer',0,n.x,n.y);updateMining();const vent=resourceNodes.find(n=>n.type==='gas');addEntity('structure','refinery',0,vent.x,vent.y);for(let i=0;i<40;i++){testClock+=250;aiAssignWorkers(0,DIFFICULTY.easy);updatePersistentOrders();updateMining()}const count=()=>{const workers=playerUnits(0).filter(isWorker);return {gas:workers.filter(w=>resourceNodes.find(n=>n.id===w.miningNodeId)?.type==='gas').length,mineral:workers.filter(w=>resourceNodes.find(n=>n.id===w.miningNodeId)?.type==='mineral').length}};const saturated=count();playerUnits(0).filter(isWorker).slice(0,2).forEach(w=>{releaseMining(w);w.alive=false});for(let i=0;i<40;i++){testClock+=250;aiAssignWorkers(0,DIFFICULTY.easy);updatePersistentOrders();updateMining()}return {saturated,recovering:count()}})()`);
  assert.deepEqual(result.saturated,{gas:1,mineral:4});assert.deepEqual(result.recovering,{gas:0,mineral:3});
});

test('lost Ascendant power is rebuilt near the unpowered producer instead of at a distant existing Pylon',()=>{
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{players[0].ai=true;players[0].aiDifficulty='easy';addEntity('structure','nexus',0,10,10);addEntity('structure','pylon',0,10,11);const gateway=addEntity('structure','gateway',0,18,10);addEntity('unit','probe',0,17,11);const before=structurePowered(gateway);const planned=aiRestorePower(0);for(let i=0;i<80;i++){testClock+=250;aiManageBuilders(0);updatePersistentOrders();updateProduction(.25)}return {before,planned,after:structurePowered(gateway),job:players[0].strategy.buildJob}})()`);
  assert.deepEqual(result,{before:false,planned:true,after:true,job:null});
});
