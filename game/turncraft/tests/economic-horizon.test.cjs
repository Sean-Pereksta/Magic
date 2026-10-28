const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

for(const difficulty of ['easy','medium','hard','pro'])for(const style of ['cautious','balanced','aggressive'])test(`${difficulty}/${style}: economic targets continue beyond the opening plan`,()=>{
  const run=simulation(),result=run.plain(`(()=>{chosenMapId='fourfold-divide';setBoardDimensions();players[0].aiDifficulty=${JSON.stringify(difficulty)};players[0].aiStyle=${JSON.stringify(style)};matchStartedAt=testClock;const start=aiEconomicTarget(0,DIFFICULTY[${JSON.stringify(difficulty)}]);testClock+=25*60000;const late=aiEconomicTarget(0,DIFFICULTY[${JSON.stringify(difficulty)}]);return {start,late}})()`);
  assert.ok(result.late>=3,JSON.stringify(result)); assert.ok(result.late>result.start,JSON.stringify(result));
});

test('six depleted bases do not prevent a real paid replacement expansion',()=>{
  const run=simulation(),result=run.plain(`(()=>{chosenMapId='classic';addEntity('structure','commandHub',0,6,15);for(let i=0;i<5;i++)addEntity('structure','commandHub',0,2+i,2);addEntity('unit','engineer',0,7,15);const site=expansionPositions()[0];addExpansionCluster(site);const before=players[0].minerals,planned=aiExpand(0,DIFFICULTY.easy);for(let i=0;i<200;i++){testClock+=250;aiManageBuilders(0);updatePersistentOrders();updateProduction(.25)}return {planned,bases:playerStructures(0).filter(isBase).length,spent:before-players[0].minerals,job:players[0].strategy.buildJob}})()`);
  assert.deepEqual(result,{planned:true,bases:7,spent:300,job:null});
});

for(const reason of ['worker died','blocked stand','enemy arrived','money spent'])test(`failed expansion recovers after ${reason}`,()=>{
  const run=simulation(),result=run.plain(`(()=>{chosenMapId='classic';addEntity('structure','commandHub',0,6,15);const worker=addEntity('unit','engineer',0,7,15),site=expansionPositions()[0];addExpansionCluster(site);const planned=aiExpand(0,DIFFICULTY.medium),job=players[0].strategy.buildJob;const reason=${JSON.stringify(reason)};if(reason==='worker died')worker.alive=false;if(reason==='enemy arrived')addEntity('unit','zealot',1,job.x+1,job.y);if(reason==='money spent')players[0].minerals=0;if(reason==='blocked stand'){clearMoveDestination(worker);job.lastProgressAt=testClock-23000;job.lastX=worker.x;job.lastY=worker.y}else testClock+=13000;aiManageBuilders(0);const cancelled=!players[0].strategy.buildJob,working=worker.miningStopped===false;for(const e of entities)if(e.faction===1)e.alive=false;players[0].minerals=1000;if(reason==='worker died')addEntity('unit','engineer',0,7,15);testClock+=26000;const retry=aiExpand(0,DIFFICULTY.medium);return {planned,cancelled,working:reason==='worker died'||working,retry}})()`);
  assert.deepEqual(result,{planned:true,cancelled:true,working:true,retry:true});
});

test('a long stalemate changes reconnaissance and releases obsolete attack plans',()=>{
  const run=simulation(),result=run.plain(`(()=>{addEntity('structure','commandHub',0,6,15);addEntity('unit','engineer',0,7,15);const s=players[0].strategy;s.lastScoutAt=testClock;s.scoutIndex=0;s.wave={ids:[],startedAt:testClock};aiMaintainEconomy(0,DIFFICULTY.medium);testClock+=190000;aiMaintainEconomy(0,DIFFICULTY.medium);return {count:s.stalemateReassessments,scout:s.lastScoutAt,index:s.scoutIndex,wave:s.wave}})()`);
  assert.deepEqual(result,{count:1,scout:0,index:1,wave:null});
});

test('failed bounded builder searches rotate through all eligible workers',()=>{
  const run=simulation(),result=run.plain(`(()=>{addEntity('structure','commandHub',0,6,15);const trapped=[];for(let i=0;i<5;i++){const w=addEntity('unit','engineer',0,12+i*3,7);trapped.push(w);for(const [dx,dy]of DIR8)tileAt(w.x+dx,w.y+dy).blocked=true}const free=addEntity('unit','engineer',0,3,25),site={x:25,y:12};let accepted=false;for(let i=0;i<3&&!accepted;i++)accepted=aiPlanBuild(0,'commandHub',site,true);return {accepted,worker:players[0].strategy.buildJob?.workerId,free:free.id}})()`);
  assert.equal(result.accepted,true);assert.equal(result.worker,result.free);
});

test('a mature four-worker economy reserves an expansion before full worker saturation',()=>{
  const run=simulation(),result=run.plain(`(()=>{chosenMapId='classic';matchStartedAt=testClock-600000;players[0].ai=true;players[0].aiDifficulty='medium';players[0].minerals=320;addEntity('structure','commandHub',0,6,15);addResourceCluster({x:6,y:15});addExpansionCluster(expansionPositions()[0]);for(let i=0;i<4;i++)addEntity('unit','engineer',0,7+i,15);for(let i=0;i<3;i++)addEntity('unit','rifleman',0,7+i,17);const before=aiWorkerGoal(0,DIFFICULTY.medium);aiMaintainEconomy(0,DIFFICULTY.medium);return {before,planned:players[0].strategy.buildJob?.expansion||playerStructures(0).filter(isBase).length>1,reserve:players[0].strategy.expansionReserve}})()`);
  assert.ok(result.before>4);assert.equal(result.planned,true);
});

test('optional structures cannot spend reserved expansion income',()=>{
  const run=simulation(),result=run.plain(`(()=>{players[0].ai=true;players[0].minerals=320;players[0].strategy.expansionReserve=300;addEntity('structure','commandHub',0,6,15);addEntity('unit','engineer',0,7,15);return {accepted:aiBuildNear(0,'supplyDepot'),minerals:players[0].minerals,job:players[0].strategy.buildJob||null}})()`);
  assert.deepEqual(result,{accepted:false,minerals:320,job:null});
});

test('Brood construction preserves its final worker until a replacement is committed',()=>{
  const run=simulation('brood'),result=run.plain(`(()=>{players[0].ai=true;addEntity('structure','hatchery',0,6,15);addEntity('unit','drone',0,7,15);const before=aiBuildNear(0,'spawningPool');const larva=addEntity('larva','larva',0,7,17);larva.kind='cocoon';larva.morphType='drone';larva.morphEnd=testClock+5000;return {before,after:aiBuildNear(0,'spawningPool')}})()`);
  assert.deepEqual(result,{before:false,after:true});
});

for(const [index,difficulty]of ['easy','medium','hard','pro'].entries())for(const [styleIndex,style]of ['cautious','balanced','aggressive'].entries())test(`${difficulty}/${style}: a 30-minute real economy keeps building and replacing bases`,async(t)=>{
  const faction=['frontier','ascendant','brood'][(index+styleIndex)%3],run=simulation(faction,{factions:['frontier',faction]});
  run(`Math.random=()=>.314159;chosenMapId='classic';chosenDifficulty=${JSON.stringify(difficulty)};createGame([{name:'Human',factionId:'frontier',ai:false},{name:'AI',factionId:${JSON.stringify(faction)},ai:true,aiDifficulty:${JSON.stringify(difficulty)},aiStyle:${JSON.stringify(style)}}]);performance.now=()=>testClock;tileVisibleToFaction=(f,x,y)=>!!visionCache.get(f)?.has(x+','+y);playerStructures(0).find(isBase).hp=1000000;playerStructures(0).find(isBase).maxHp=1000000;window.horizon={expansions:0,firstExpansion:null,maxBases:1,start:testClock};window.horizonBuild=issueBuild;issueBuild=function(w,type,x,y){const accepted=horizonBuild(w,type,x,y);if(accepted&&w.faction===1&&STRUCTURES[type]?.base){horizon.expansions++;horizon.firstExpansion??=(testClock-horizon.start)/1000}return accepted};`);
  // Advance in five-minute batches so a large Pro army cannot hit the harness's per-evaluation watchdog.
  for(let batch=0;batch<6;batch++){run(`for(let tick=0;tick<600;tick++){testClock+=500;updatePersistentOrders();updateCombat(.5);resolveProjectileImpacts();updateProduction(.5);updateRegeneration(.5);updateAreaEffects();updateMining();updateLastSeenIntel();aiThinkAll();horizon.maxBases=Math.max(horizon.maxBases,playerStructures(1).filter(isBase).length)}`);await new Promise(resolve=>setImmediate(resolve));}
  const result=run.plain(`(()=>{const p=players[1],connected=connectedResources(1).filter(n=>n.type==='mineral'),unconnected=resourceNodes.filter(n=>n.type==='mineral'&&n.amount>0&&!resourceConnected(1,n));return {faction:${JSON.stringify(faction)},...horizon,workers:workerCommitment(1),mined:ensureMatchStats(p).minerals,target:p.strategy.economicTarget,activeBases:playerStructures(1).filter(isBase).filter(b=>connected.some(n=>dist(n,b)<=BASE_COLLECTION_RADIUS)).length,connectedMinerals:connected.reduce((sum,n)=>sum+n.amount,0),unconnectedMinerals:unconnected.reduce((sum,n)=>sum+n.amount,0),bank:p.minerals,reserve:p.strategy.expansionReserve,remaining:resourceNodes.filter(n=>n.type==='mineral'&&n.amount>0).length,nextEconomyIn:p.strategy.nextEconomyAt-testClock,job:p.strategy.buildJob&&{type:p.strategy.buildJob.type,expansion:p.strategy.buildJob.expansion,age:testClock-p.strategy.buildJob.startedAt}}})()`);
  assert.ok(result.expansions>=1,JSON.stringify(result)); assert.ok(result.maxBases>=2,JSON.stringify(result));assert.ok(result.mined>2000,JSON.stringify(result));assert.ok(result.workers>=2,JSON.stringify(result));assert.ok(!result.job||result.job.age<150000,JSON.stringify(result));
  assert.ok(result.activeBases>0||result.unconnectedMinerals===0||result.job?.expansion,JSON.stringify(result));
  t.diagnostic(`${faction}: ${result.expansions} paid expansions; first at ${result.firstExpansion}s; ${result.maxBases} peak bases; ${result.activeBases} active bases / ${result.connectedMinerals} connected minerals at30min; ${result.mined} minerals gathered.`);
});
