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

test('public loader applies every existing patch to the updated core; both modules parse',async()=>{
  let output='', errors=[];
  const script=loader.match(/<script>([\s\S]*)<\/script>/)[1];
  await vm.runInNewContext(script,{
    document:{getElementById:()=>({}),open(){},write:html=>{output=html;},close(){}},
    fetch:async()=>({ok:true,text:async()=>core}),
    console:{error:(...e)=>errors.push(e.map(String).join(" ")),warn:(...e)=>errors.push(e.map(String).join(" "))}
  });
  assert.deepEqual(errors,[]);
  assert.match(output,/__CATMOUSE_PERF_PATCH_VERSION/);
  assert.match(output,/rat:\[0,7,8,9,10\]\[n\]/);
  assert.match(output,/MAX_FRIENDLY_RABBITS = 12/);
  assert.match(output,/from "\.\/catandmouse\/sync.mjs"/);
  compileModule(core);compileModule(output);
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
