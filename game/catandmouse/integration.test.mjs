import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import vm from 'node:vm';
import { eligibleHostCandidate, stateFieldPatch } from './sync.mjs';

const core=readFileSync(new URL('../catandmouse-core.html',import.meta.url),'utf8');
const loader=readFileSync(new URL('../catandmouse.html',import.meta.url),'utf8');
const section=(start,end)=>core.slice(core.indexOf(start),core.indexOf(end,core.indexOf(start)));
function compileModule(html) {
  const dir=mkdtempSync(join(tmpdir(),'catmouse-syntax-'));
  try {
    const script=html.match(/<script type="module">([\s\S]*?)<\/script>/)[1];
    const file=join(dir,'core.mjs');writeFileSync(file,script);
    const result=spawnSync(process.execPath,['--check',file],{encoding:'utf8'});
    assert.equal(result.status,0,result.stderr);
  } finally { rmSync(dir,{recursive:true,force:true}); }
}

async function optimizedCore(){
  let output='', errors=[];
  const script=loader.match(/<script>([\s\S]*)<\/script>/)[1];
  await vm.runInNewContext(script,{
    document:{getElementById:()=>({}),open(){},write:html=>{output=html;},close(){}},
    fetch:async()=>({ok:true,text:async()=>core}),
    console:{error:(...e)=>errors.push(e.map(String).join(" ")),warn:(...e)=>errors.push(e.map(String).join(" "))}
  });
  assert.deepEqual(errors,[]);
  return output;
}

test('public loader applies every existing patch to the updated core; both modules parse',async()=>{
  const output=await optimizedCore();
  assert.match(output,/__CATMOUSE_PERF_PATCH_VERSION/);
  assert.match(output,/rat:\[0,7,8,9,10\]\[n\]\+ratBonus/);
  assert.match(output,/MAX_RATS = 13/);
  assert.match(output,/MAX_TOTAL_ENEMIES = 25/);
  assert.match(output,/reserveEnemySpawnSlots\("rat", getRatWaveBudget\(\)\)/);
  assert.match(output,/MAX_FRIENDLY_RABBITS = 12/);
  assert.match(output,/from "\.\/catandmouse\/sync.mjs"/);
  assert.match(output,/from "\.\/catandmouse\/tactics.mjs"/);
  assert.match(output,/from "\.\/catandmouse\/combat-presentation.mjs"/);
  assert.match(output,/battlePresentation\.syncEntities/);
  assert.match(output,/tacticalAI\.plan/);
  assert.match(output,/combat-presentation\.css/);
  assert.equal((output.match(/const MAX_FRIENDLY_RABBITS =/g) || []).length,1);
  compileModule(core);compileModule(output);
});

async function ratSpawnHarness(source){
  source ??= await optimizedCore();
  const ctx=vm.createContext({currentDifficulty:'medium',activePlayers:1,catPower:34,ratPower:1,counts:{},pendingEnemySpawnReservations:new Map(),
    getActivePlayerCount:()=>ctx.activePlayers,normalizeEnemyKind:kind=>kind,
    getLivingEnemyCount:kind=>ctx.counts[kind] || 0,
    getTotalLivingEnemyCount:()=>Object.values(ctx.counts).reduce((sum,count)=>sum+count,0),
    getPendingEnemyCount:kind=>ctx.pendingEnemySpawnReservations.get(kind) || 0});
  const code=(start,end)=>source.slice(source.indexOf(start),source.indexOf(end,source.indexOf(start)));
  vm.runInContext(code('  const DIFFICULTY_SETTINGS = {','  let canMove = true;'),ctx);
  vm.runInContext(code('  function getRatSpawnBonus(){','  function getPendingEnemyCount('),ctx);
  vm.runInContext(code('  function canSpawnEnemy(','  function getEnemyScaling('),ctx);
  return {ctx,source,code};
}

test('rat difficulty caps increase by two or three for every player count; other caps stay fixed',async()=>{
  const {ctx}=await ratSpawnHarness();
  const expected={easy:[9,10,11,12],medium:[9,10,11,12],hard:[10,11,12,13],insane:[10,11,12,13]};
  for(const [difficulty,rats] of Object.entries(expected))for(let players=1;players<=4;players++){
    ctx.currentDifficulty=difficulty;ctx.activePlayers=players;
    const caps=ctx.getEnemyCaps();assert.equal(caps.rat,rats[players-1]);
    assert.equal(caps.stinkrat,[1,2,2,2][players-1]);assert.equal(caps.ox,[2,2,3,4][players-1]);
    assert.equal(caps.ratking,1);assert.equal(caps.vulture,[2,2,3,4][players-1]);
    assert.equal(caps.termite,[3,3,4,5][players-1]);assert.equal(caps.flea,[5,6,7,8][players-1]);
  }
  assert.equal(ctx.getEnemyCaps(0).rat,10);assert.equal(ctx.getEnemyCaps(10).rat,13);
  ctx.currentDifficulty='unknown';assert.equal(ctx.getEnemyCaps(1).rat,9);
});

test('rat wave allowance preserves opening waves and increases only the existing late tiers',async()=>{
  const {ctx}=await ratSpawnHarness();
  const expected={easy:[6,7,8,9],medium:[6,7,8,9],hard:[7,8,9,10],insane:[7,8,9,10]};
  for(const [difficulty,late] of Object.entries(expected))for(let players=1;players<=4;players++){
    ctx.currentDifficulty=difficulty;
    for(const level of [1,10,21])assert.equal(ctx.getRatWaveBudget(players,level),players+1);
    assert.equal(ctx.getRatWaveBudget(players,22),late[players-1]-1);
    for(const level of [34,1000])assert.equal(ctx.getRatWaveBudget(players,level),late[players-1]);
  }
});

test('a full battlefield admits the extra regular rats but denies other kinds the new headroom',async()=>{
  const {ctx}=await ratSpawnHarness();
  for(const [difficulty,bonus] of [['easy',2],['medium',2],['hard',3],['insane',3]]){
    ctx.currentDifficulty=difficulty;ctx.counts={rat:7,ox:2,stinkrat:1,ratking:1,vulture:1,termite:3};
    assert.equal(ctx.getTotalLivingEnemyCount(),15);assert.equal(ctx.canSpawnEnemy('flea'),false);
    const reservation=ctx.reserveEnemySpawnSlots('rat',20);assert.equal(reservation.count,bonus);
    assert.equal(ctx.canSpawnEnemy('rat'),false);assert.equal(ctx.canSpawnEnemy('vulture'),false);
    ctx.releaseEnemySpawnReservations(reservation);assert.equal(ctx.getPendingEnemyCount('rat'),0);
    ctx.counts.rat+=bonus;assert.equal(ctx.canSpawnEnemy('flea'),false);
    ctx.counts.rat=7;assert.equal(ctx.canSpawnEnemy('flea'),false);
  }
});

test('rat reservations preserve ordinary free slots without lending their bonus to other spawners',async()=>{
  const {ctx}=await ratSpawnHarness();ctx.counts={rat:7,ox:2,stinkrat:1,ratking:1,vulture:2,termite:1};
  const rats=ctx.reserveEnemySpawnSlots('rat',10);assert.equal(rats.count,2);
  const fleas=ctx.reserveEnemySpawnSlots('flea',5);assert.equal(fleas.count,1);
  assert.equal(ctx.canSpawnEnemy('flea'),false);ctx.releaseEnemySpawnReservations(rats);
  assert.equal(ctx.canSpawnEnemy('flea'),false);ctx.releaseEnemySpawnReservations(fleas);
  assert.equal(ctx.canSpawnEnemy('flea'),true);
  ctx.counts={rat:0,ox:2};assert.equal(ctx.canSpawnEnemy('ox'),false);
});

test('fallback core uses the same rat-only difficulty bonus and bounded wave progression',async()=>{
  const {ctx}=await ratSpawnHarness(core);ctx.currentDifficulty='easy';
  assert.equal(ctx.getEnemyCaps(1).rat,12);assert.equal(ctx.getEnemyCaps(1).total,26);
  assert.equal(ctx.getRatWaveBudget(1,1),2);assert.equal(ctx.getRatWaveBudget(1,34),6);
  ctx.currentDifficulty='insane';assert.equal(ctx.getEnemyCaps(4).rat,19);
  assert.equal(ctx.getEnemyCaps(4).total,39);assert.equal(ctx.getRatWaveBudget(4,34),10);
});

test('real wave spawner consumes the difficulty budget and releases all spawn reservations',async()=>{
  const {ctx,code}=await ratSpawnHarness();const batches=[];let id=0;
  Object.assign(ctx,{isHost:true,catPos:{x:10,y:10},gridSize:21,grid:Array.from({length:21},()=>Array(21).fill('')),
    rats:[],oxen:[],ratKings:[],isLivingRat:()=>true,isLivingEnemy:()=>true,crypto:{randomUUID:()=>String(++id)},
    Math:Object.assign(Object.create(Math),{random:()=>1}),ccRef:id=>({id}),ID:{rat:id=>`rat_${id}`,ox:id=>`ox_${id}`},
    getRatHealthForDifficulty:()=>4,getOxHealthForDifficulty:()=>18,scaleEnemyStats:health=>({health}),
    commitEnemyBatch:async actions=>batches.push(actions),requestRender(){}});
  vm.runInContext(code('  async function spawnRatWave(){','  // ============================================================\n  // Host enemies movement'),ctx);
  for(const [difficulty,late] of [['medium',[6,7,8,9]],['insane',[7,8,9,10]]])for(let players=1;players<=4;players++){
    ctx.currentDifficulty=difficulty;ctx.activePlayers=players;await ctx.spawnRatWave();
    assert.equal(batches.at(-1).filter(action=>action.data.kind==='rat').length,late[players-1]);
    assert.ok([...ctx.pendingEnemySpawnReservations.values()].every(count=>count===0));
  }
  ctx.activePlayers=1;ctx.counts={rat:7,ox:2,stinkrat:1,ratking:1,vulture:1,termite:3};
  await ctx.spawnRatWave();assert.equal(batches.at(-1).filter(action=>action.data.kind==='rat').length,3);
});

test('real savePosition does not overwrite death, cheese or identity, and skips idle input',async()=>{
  const writes=[];
  const ctx=vm.createContext({playerPos:{x:3,y:4},playerFacing:'east',lastRequestedPosition:null,
    localPlayerSequence:0,soloMode:false,isAlive:true,cheeseCount:12,
    playerWriter:{enqueue:async p=>writes.push(p)}});
  vm.runInContext(section('  function savePosition(){','  async function upsertMyPlayerDoc'),ctx);
  await ctx.savePosition();await ctx.savePosition();
  assert.equal(writes.length,1);
  const remote={alive:false,cheese:22,uid:'me',displayName:'mouse',...writes[0]};
  assert.equal(remote.alive,false);assert.equal(remote.cheese,22);assert.equal(remote.uid,'me');
  assert.equal(ctx.localPlayerSequence,1);
  ctx.playerPos.x=5;await ctx.savePosition();assert.equal(writes.length,2);
});

function listenerHarness() {
  const elements=new Map();let next, failure, renders=0, cancelled=0, menus=0;
  const ctx=vm.createContext({
    liveSnapshotReady:false,listenerFailed:false,knownRealtimeDocs:new Map(),isHost:false,uid:'me',
    gameStarted:false,hostInitialized:false,catMoveInterval:null,
    players:{me:{uid:'me',x:5,y:5,alive:true,cheese:7}},playerPos:{x:5,y:5},playerFacing:'east',
    localPlayerSequence:4,lastRequestedPosition:'pending',isAlive:true,cheeseCount:7,
    ID:{STATE:'state'},ccCol:{},catPos:{x:29,y:29},catFacingDir:'se',catHealth:100,catPower:1,ratPower:1,
    objectiveState:{},currentDifficulty:'medium',catWeakenedUntil:0,nextRatWaveAt:0,soloMode:false,matchId:null,
    observedHostUid:'host',observedHostHeartbeatAt:0,selectionDeadlineAt:null,selectionAssetsReadyAt:null,
    cleanupFirebaseListeners(){},addFirebaseListener(){},
    onSnapshot:(_col,options,callback,error)=>{assert.equal(options.includeMetadataChanges,true);next=callback;failure=error;return()=>{};},
    document:{getElementById:id=>{if(!elements.has(id))elements.set(id,{});return elements.get(id);}},
    setConnectionStatus:label=>{ctx.connection=label;},
    getLobbyParticipants:()=>Object.entries(ctx.players),
    requestRender:()=>renders++,setTileEffect(){},startCatBehaviorLoop(){},
    stateWriter:{cancel:()=>cancelled++,observe(){},resume(){}},
    tacticalAI:{reset(){}},delayedEnemySteps:new Map(),
    battlePresentation:{moveEntity(){}},enemyMovementCadence:()=>400,
    playerWriter:{cancel:()=>cancelled++,resume(){}},
    stateFieldPatch,applyDifficulty(){},updateHostDisplay(){},updateCatHealthBar(){},updateDifficultyUI(){},
    renderSelectionOverlay:()=>menus++,ensureLocalBuildControls(){},playGameMusic(){},startHostLoops(){},
    clearInterval(){},console:{error(){}},setTimeout(){},activateSoloRuntime(){}
  });
  vm.runInContext(section('  function listenToCrownCouncil(){','  // ============================================================\n  // Ensure lobby'),ctx);
  const doc=(id,data)=>({id,data:()=>data,metadata:{hasPendingWrites:false}});
  const deliver=(items,{cached=false,docs=items.map(i=>doc(i.id,i.data))}={})=>next({
    metadata:{fromCache:cached,hasPendingWrites:false},docs,
    docChanges:()=>items.map(i=>({type:i.type||'modified',doc:doc(i.id,i.data)}))
  });
  ctx.listenToCrownCouncil();
  return {ctx,deliver,fail:()=>failure(new Error('closed')),get renders(){return renders;},get cancelled(){return cancelled;},get menus(){return menus;}};
}

test('listener applies death during pending movement without snapping live movement backwards',()=>{
  const h=listenerHarness();
  h.deliver([{id:'player_me',data:{uid:'me',x:2,y:2,alive:false,cheese:11,clientSeq:1,killedBy:'cat',slowedUntil:10000,slowMult:0.5}}]);
  assert.equal(h.ctx.isAlive,false);assert.equal(h.ctx.cheeseCount,11);assert.equal(h.cancelled,1);
  assert.equal(h.ctx.playerPos.x,5);assert.equal(h.ctx.players.me.slowMult,0.5);
  assert.equal(h.ctx.lastRequestedPosition,null);
});

test('listener accepts authoritative revive position despite older movement sequence',()=>{
  const h=listenerHarness();h.ctx.players.me.alive=false;h.ctx.isAlive=false;
  h.deliver([{id:'player_me',data:{uid:'me',x:9,y:10,alive:true,clientSeq:1}}]);
  assert.equal(h.ctx.isAlive,true);assert.equal(h.ctx.playerPos.x,9);assert.equal(h.cancelled,1);
});

test('cached and metadata-only snapshots report connection truthfully without redrawing',()=>{
  const h=listenerHarness();h.deliver([],{cached:true});
  assert.equal(h.ctx.liveSnapshotReady,false);assert.equal(h.ctx.connection,'Reconnecting');
  h.deliver([]);assert.equal(h.ctx.connection,'Connected');assert.equal(h.renders,0);
  h.fail();assert.equal(h.ctx.liveSnapshotReady,false);assert.equal(h.ctx.listenerFailed,true);
});

test('restarted listeners remove entities absent from the first server snapshot',()=>{
  const h=listenerHarness();
  h.deliver([{id:'player_other',data:{uid:'other',x:3,y:3,alive:true}}]);
  assert.ok(h.ctx.players.other);
  h.ctx.listenToCrownCouncil();
  h.deliver([],{cached:true,docs:[]});assert.ok(h.ctx.players.other);
  h.deliver([],{docs:[]});assert.equal(h.ctx.players.other,undefined);
});

test('playing transition hides selection once and host loss cancels pending state writes',()=>{
  const h=listenerHarness();h.ctx.isHost=true;
  h.deliver([{id:'state',data:{hostUid:'other',hostHeartbeatAt:100,phase:'playing',cat:{x:29,y:29,health:100}}}]);
  assert.equal(h.ctx.isHost,false);assert.equal(h.cancelled,1);assert.equal(h.ctx.gameStarted,true);assert.equal(h.menus,1);
  h.deliver([{id:'state',data:{hostUid:'other',hostHeartbeatAt:200,phase:'playing',cat:{x:29,y:29,health:100}}}]);
  assert.equal(h.menus,1);
});

test('healthy host causes zero polling transactions; only elected replacement attempts takeover',async()=>{
  let transactions=0,writes=[];
  const ctx=vm.createContext({soloMode:false,syncEnabled:()=>true,hostTransferPending:false,isHost:false,
    lastHostCheckAt:0,observedHostHeartbeatAt:100000,HOST_STALE_MS:20000,observedHostUid:'host',
    uid:'me',username:'Mouse',players:{host:{joinedAt:1,online:true},me:{joinedAt:2,online:true},later:{joinedAt:3,online:true}},
    isPlayerActive:p=>p.online,eligibleHostCandidate,ccRef:()=>({}),ID:{STATE:'state'},db:{},Date:{now:()=>100000},
    runTransaction:async(_db,fn)=>{transactions++;await fn({get:async()=>({exists:()=>true,data:()=>({hostUid:'host',hostHeartbeatAt:1})}),update:(_ref,patch)=>writes.push(patch)});}});
  vm.runInContext(section('  async function checkHostTransfer(){','  async function hostSyncAssetLoadingBarrier'),ctx);
  for(let i=0;i<100;i++)await ctx.checkHostTransfer();assert.equal(transactions,0);
  ctx.observedHostHeartbeatAt=1;ctx.uid='later';await ctx.checkHostTransfer();assert.equal(transactions,0);
  ctx.uid='me';await ctx.checkHostTransfer();assert.equal(transactions,1);assert.equal(writes[0].hostUid,'me');
});

test('authoritative state patches go through the queue and solo remains local',async()=>{
  const queued=[], local=[];
  const ctx=vm.createContext({soloMode:false,gameStarted:true,isHost:true,ID:{STATE:'state'},stateFieldPatch,
    stateWriter:{enqueue:async patch=>queued.push(patch)},applyLocalRuntimeMutation:(...args)=>local.push(args),updateDoc:()=>{throw Error('unexpected direct state write');}});
  vm.runInContext(section('  async function runtimeUpdateDoc(ref, data){','  async function runtimeDeleteDoc'),ctx);
  await ctx.runtimeUpdateDoc({id:'state'},{cat:{x:4},updatedAt:1});assert.equal(queued[0]['cat.x'],4);
  ctx.isHost=false;await ctx.runtimeUpdateDoc({id:'state'},{cat:{x:5}});assert.equal(queued.length,1);
  ctx.soloMode=true;await ctx.runtimeUpdateDoc({id:'state'},{cat:{x:6}});assert.equal(local.length,1);
});

test('combat batches merge per-unit updates, let death win, and preserve coalesced dotted state writes',async()=>{
  const batches=[],queued=[];
  const ctx=vm.createContext({soloMode:false,isHost:true,stateFieldPatch,ID:{STATE:'state'},db:{},
    stateWriter:{enqueue:async patch=>queued.push(patch)},writeBatch:()=>({
      update:(ref,data)=>batches.push({type:'update',ref,data}),set:(ref,data)=>batches.push({type:'set',ref,data}),delete:ref=>batches.push({type:'delete',ref}),commit:async()=>{}
    })});
  vm.runInContext(section('  async function commitEnemyBatch(actions)', '  const ID = {'),ctx);
  await ctx.commitEnemyBatch([
    {type:'update',ref:{id:'rabbit_a'},data:{x:2,y:3}},
    {type:'update',ref:{id:'rabbit_a'},data:{health:4}},
    {type:'update',ref:{id:'rat_dead'},data:{x:4}},
    {type:'delete',ref:{id:'rat_dead'}},
    {type:'update',ref:{id:'rat_dead'},data:{health:0}},
    {type:'update',ref:{id:'state'},data:{cat:{health:42},updatedAt:100}}
  ]);
  assert.equal(batches.length,2);assert.equal(batches.find(b=>b.ref.id==='rat_dead').type,'delete');
  assert.equal(batches.find(b=>b.ref.id==='rabbit_a').data.health,4);
  assert.equal(batches.find(b=>b.ref.id==='rabbit_a').data.x,2);
  assert.equal(queued.length,1);assert.equal(queued[0]['cat.health'],42);assert.equal(queued[0].cat,undefined);
});

function denHarness({blocked=false,count=11,canRun=true,fail=false}={}) {
  let ack;const batches=[];
  const ctx=vm.createContext({window:{},rabbits:Array.from({length:count},(_,i)=>({id:`r${i}`,x:20,y:i,health:5})),
    canRunHostSimulation:()=>canRun,MAX_FRIENDLY_RABBITS:12,Date:{now:()=>100000},crypto:{randomUUID:()=>String(batches.length)},
    isLivingEnemy:r=>r.health>0,getStructuresByType:()=>[{x:1,y:1},{x:4,y:4}],stableHash:()=>0,
    openFriendlySpawn:den=>blocked ? null : {x:den.x+1,y:den.y},directionFromDelta:()=> 'east',
    ccRef:id=>({id}),ID:{rabbit:id=>`rabbit_${id}`},battlePresentation:{pulseNode(){}},structureNodes:new Map(),
    isNearRallyDrum:()=>false,tacticalPlan:(_kind,rb)=>({target:{x:0,y:0,kind:'rat',enemy:{health:50}},next:{x:rb.x-1,y:rb.y}}),
    moveAlongPlan:(_kind,rb,plan,writes)=>{rb.x=plan.next.x;writes.push({type:'update',ref:{id:`rabbit_${rb.id}`},data:{x:rb.x}});},
    requestRender(){},commitEnemyBatch:actions=>{batches.push(actions);return fail ? Promise.reject(Error('offline')) : new Promise(resolve=>ack=resolve);}
  });
  vm.runInContext(section('  async function rabbitDenTick(){','  async function fleaNestAttackTick(){'),ctx);
  return {ctx,batches,ack:()=>ack?.()};
}
test('rabbit den obeys the global cap and prepares every movement before a network acknowledgement',async()=>{
  const h=denHarness(),promise=h.ctx.rabbitDenTick();await Promise.resolve();
  assert.equal(h.ctx.rabbits.length,12);assert.equal(h.batches.length,1);
  assert.ok(h.ctx.rabbits.slice(0,11).every(r=>r.x===19));
  assert.equal(h.batches[0].filter(a=>a.type==='set').length,1);
  const spawn=h.batches[0].find(a=>a.type==='set').data;assert.equal(spawn.x,2);assert.equal(spawn.spawnX,1);
  h.ack();await promise;
});
test('blocked dens do not spawn, and non-hosts cannot spawn or move allies',async()=>{
  const blocked=denHarness({blocked:true,count:0}),promise=blocked.ctx.rabbitDenTick();blocked.ack();await promise;
  assert.equal(blocked.ctx.rabbits.length,0);assert.equal(blocked.batches[0].length,0);
  const spectator=denHarness({canRun:false});await spectator.ctx.rabbitDenTick();assert.equal(spectator.batches.length,0);
});
test('failed den batches release optimistic spawn slots for a later retry',async()=>{
  const h=denHarness({fail:true});await assert.rejects(h.ctx.rabbitDenTick(),/offline/);
  assert.equal(h.ctx.rabbits.length,11);assert.equal(h.ctx.window.__lastRabbitSpawnAt['1,1'],0);
});
test('real enemy group takes one step, stores no AI cache in Firebase, and respects host authority',async()=>{
  const writes=[],rat={id:'one',kind:'rat',x:1,y:1,health:20,updatedAt:100000};
  const ctx=vm.createContext({canRunHostSimulation:()=>true,Date:{now:()=>100000},combatUnitKey:(kind,e)=>`${kind}:${e.id}`,
    enemyDocRef:(_kind,id)=>({id:`rat_${id}`}),ccRef:id=>({id}),isLivingEnemy:e=>e.health>0,delayedEnemySteps:new Map(),traps:[],ratKings:[],ratPower:1,
    tacticalPlan:()=>({next:{x:2,y:1},target:{type:'mouse',key:'m',x:8,y:1,uid:'other'}}),
    moveAlongPlan:(_kind,e,plan,out)=>{Object.assign(e,plan.next);out.push({type:'update',ref:{id:e.id},data:{x:e.x,y:e.y}});},
    requestRender(){},commitEnemyBatch:async out=>writes.push(...out)});
  vm.runInContext(section('  async function hostMoveTacticalGroup(', '  async function hostMoveFleas(){'),ctx);
  await ctx.hostMoveTacticalGroup([rat],'rat');assert.equal(rat.x,2);
  assert.ok(writes.every(a=>!a.data?.ai));
  ctx.canRunHostSimulation=()=>false;await ctx.hostMoveTacticalGroup([rat],'rat');assert.equal(rat.x,2);
});
test('ox damage waits for its authoritative windup, then retains the original attack damage',async()=>{
  let now=100000,damage=0;const ox={id:'o',x:2,y:1,health:30,updatedAt:now};
  const ctx=vm.createContext({canRunHostSimulation:()=>true,Date:{now:()=>now},combatUnitKey:()=> 'ox:o',enemyDocRef:()=>({id:'ox_o'}),
    isLivingEnemy:e=>e.health>0,delayedEnemySteps:new Map(),traps:[],ratKings:[],ratPower:2,
    tacticalPlan:()=>({next:null,target:{type:'structure',key:'wall',x:3,y:1}}),moveAlongPlan(){},
    damageStructure:async(_x,_y,amount)=>{damage+=amount;},requestRender(){},commitEnemyBatch:async()=>{}});
  vm.runInContext(section('  async function hostMoveTacticalGroup(', '  async function hostMoveFleas(){'),ctx);
  await ctx.hostMoveTacticalGroup([ox],'ox');assert.equal(damage,0);assert.equal(ox.attackIntent.executeAt,100180);
  now+=500;await ctx.hostMoveTacticalGroup([ox],'ox');assert.equal(damage,20);assert.equal(ox.health,29);
});
