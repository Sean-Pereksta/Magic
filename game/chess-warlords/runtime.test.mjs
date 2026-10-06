import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
import {snapshotDelta,placementRating,applyWarResult,rankProgress} from './core.mjs';
import {capitalProduction} from './objectives.mjs';
const html=fs.readFileSync(new URL('../chess_warlord.html',import.meta.url),'utf8');
function fn(name){const match=html.match(new RegExp(`^(?:async )?function ${name}\\(`,'m'));assert.ok(match);const start=match.index,end=html.indexOf('\n}',start)+2;return html.slice(start,end);}
function writeHarness({failLease=false}={}){
  let release;const blocked=new Promise(r=>release=r),writes=[];
  const env={console,Map,Set,JSON,Date,Number,Math,Array,Promise,
    stateRef:'state',fullStateRef:'full',lobbyRef:'lobby',db:{},gameStarted:true,writingState:false,isController:true,
    stateMutation:1,landRevision:1,localStateVersion:1,lastStateWrite:0,lastFullSnapshotWrite:0,
    recoverySnapshot:null,matchRecord:{id:'match',participants:[],placements:{}},
    pendingCommandReceipts:new Map([['cmd',{id:'cmd',ok:true,reason:'move_applied'}]]),
    commandReceipts:[{id:'cmd',ok:true,reason:'move_applied'}],
    username:'Host',auth:{currentUser:{uid:'host'}},FULL_SNAPSHOT_SECONDS:24,
    syncDiagnostics:{writes:0,bytes:0,fullBytes:0},liveX:2,stateDirty:true,landDirty:true,
    canAuthoritativelyWriteState:()=>true,currentControllerEpoch:()=>1,now:()=>100,
    controllerLeaseForLobby:d=>d,leaseIsFresh:()=>true,commandDocRef:id=>id,
    snapshotDelta,setNetwork:()=>{},settleMatchResults:()=>{},setTimeout:()=>{},
    pieceSyncSignature:p=>JSON.stringify(p),lastAuthoritativeData:null,lastAuthoritativeOwners:'',lastAuthoritativeKinds:'',lastAuthoritativeWalls:'',lastAuthoritativePieces:new Map(),lastAuthoritativePieceSignatures:new Map(),lastLandSync:0,localWritePendingUntil:0,
    runTransaction:async(db,callback)=>{const staged=[];const result=await callback({get:async()=>({data:()=>({ownerUid:failLease?'other':'host',epoch:1})}),set:(ref,data)=>staged.push([ref,structuredClone(data)])});await blocked;writes.push(...staged);return result;}
  };
  env.buildSelfContainedState=()=>({version:1,mapSeed:1,controllerEpoch:1,matchRecord:env.matchRecord,pieces:[{i:1,x:env.liveX}],factions:[],boardOwners:'-',boardKinds:'.',boardWalls:'-'});
  vm.createContext(env);vm.runInContext(fn('writeStateNow'),env);
  return {env,release,writes};
}
test('writes commit exactly the sent snapshot and leave in-flight mutations dirty',async()=>{
  const h=writeHarness(),promise=h.env.writeStateNow('test');
  await new Promise(r=>setImmediate(r));
  h.env.liveX=9;h.env.stateMutation++;h.env.landRevision++;h.env.matchRecord.placements[0]=2;
  h.release();await promise;
  assert.equal(h.writes.find(([ref])=>ref==='state')[1].pieces[0].x,2);
  assert.equal(h.env.lastAuthoritativePieces.get(1).x,2);
  assert.equal(h.env.lastAuthoritativeData.matchRecord.placements[0],undefined);
  assert.equal(h.env.stateDirty,true);assert.equal(h.env.landDirty,true);
  assert.equal(h.writes.find(([ref])=>ref==='cmd')[1].hostStateVersion,2,'ack committed with the state');
});
test('expired controller cannot publish state or acknowledge commands',async()=>{
  const h=writeHarness({failLease:true}),promise=h.env.writeStateNow();h.release();await promise;
  assert.equal(h.writes.length,0);assert.equal(h.env.pendingCommandReceipts.size,1);assert.equal(h.env.stateDirty,true);
});

test('duplicate result settlement increments every player once, including eliminated players',async()=>{
  const record={id:'m1',season:'2026-Q3',participants:[{slot:0,name:'Winner',elo:100,human:true,faction:'iron'},{slot:1,name:'RunnerUp',elo:100,human:true,faction:'shadow'},{slot:2,name:'Third',elo:100,human:true,faction:'ember'}],placements:{0:1,1:2,2:3},kings:{0:2},territory:{0:50}};
  const docs=new Map([['state',{matchRecord:record}]]);
  const env={console,JSON,Date,Number,Math,Map,Set,Promise,lobbyId:'lobby',stateRef:'state',db:{},resultSaving:false,settledResults:new Set(),username:'RunnerUp',matchRecord:record,
    canAuthoritativelyWriteState:()=>true,placementRating,applyWarResult,
    resultDocFor:(r,slot)=>'result/'+r.id+'/'+slot,chessEloProfileDocRef:name=>'users/'+name,
    getChessEloProfile:name=>({username:name,elo:100}),chessEloProfileFromUserDoc:(name,data)=>data.chessWarlord,
    normalizeChessEloProfile:(name,p)=>({...p,username:name}),serverTimestamp:()=>0,
    rememberChessEloProfile:()=>{},writeLocalChessEloProfile:()=>{},showRankResult:()=>{},renderEloProfileLine:()=>{},
    runTransaction:async(db,callback)=>{const staged=[];const value=await callback({get:async ref=>({exists:()=>docs.has(ref),data:()=>structuredClone(docs.get(ref))}),set:(ref,value)=>staged.push([ref,structuredClone(value)])});for(const [ref,value] of staged)docs.set(ref,value);return value;}
  };
  vm.createContext(env);vm.runInContext(fn('settleMatchResults'),env);
  await env.settleMatchResults();env.settledResults.clear();await env.settleMatchResults();
  for(const name of ['Winner','RunnerUp','Third'])assert.equal(docs.get('users/'+name).chessWarlord.games,1);
  assert.equal(docs.get('users/Winner').chessWarlord.wins,1);
  assert.equal(docs.get('users/Third').chessWarlord.losses,1);
  assert.ok(docs.get('users/RunnerUp').chessWarlord.elo>docs.get('users/Third').chessWarlord.elo);
});

test('unchanged remote walls retain terrain caches and changed walls invalidate only nearby chunks',()=>{
  let actorDirty=0,terrainDirty=0;const touched=[];
  const env={board:[{x:0,y:0,wallOwner:null},{x:1,y:0,wallOwner:1}],markTileChunkDirty:(x,y)=>touched.push([x,y]),markWallIndexDirty:()=>actorDirty++,invalidateSimulation:()=>{},invalidateSupplyDistanceCache:()=>{},wallPatchApplyCount:0,markLandDirty:()=>terrainDirty++};
  vm.createContext(env);vm.runInContext(fn('applyWalls'),env);
  env.applyWalls('-1');assert.equal(actorDirty,0);assert.equal(touched.length,0);
  env.applyWalls('01');assert.equal(actorDirty,1);assert.deepEqual(touched,[[0,0]]);assert.equal(terrainDirty,0);
});

test('production snapshots freeze objective state before simulation can mutate an in-flight checkpoint',()=>{
  const war={mode:'grand',scores:{0:12},objectives:[{id:'banner-0',owner:0,progress:.5}],victory:null};
  const env={JSON,Date,Math,recalcOwnerCounts:()=>{},GAME_KEY:'cw',localStateVersion:1,serverTimestamp:()=>0,username:'Host',
    matchRecord:null,commandReceipts:[],mapSeed:1,BOARD_W:1,BOARD_H:1,selectedMultiplayerAiCount:0,slotHumans:['Host'],slotNames:['Host'],slotFactionIds:['crownward'],
    controllerName:'Host',currentControllerEpoch:()=>1,gameStats:{},now:()=>10,factions:[],warState:war,winnerName:()=>'',
    compressOwners:()=>'-',compressCaptureKinds:()=>'.',compressWalls:()=>'-',buildFullPieces:()=>[]};
  vm.createContext(env);vm.runInContext(fn('buildSelfContainedState'),env);
  const snapshot=env.buildSelfContainedState();war.scores[0]=119;war.objectives[0].owner=1;war.victory={slot:1,reason:'dominion'};
  assert.equal(snapshot.war.scores[0],12);assert.equal(snapshot.war.objectives[0].owner,0);assert.equal(snapshot.war.victory,null);
});

test('timed troop production runs at half speed during capital occupation and resumes normally after recapture',()=>{
  let time=0,spawns=0;const b={type:'barrackskeep',faction:0,nextBuildingTick:55,objectiveProductionAt:0};
  const capital={kind:'capital',home:0,owner:1};
  const env={Math,lobbyId:null,now:()=>time,canAuthoritativelyWriteState:()=>true,warState:{objectives:[capital]},capitalProduction,
    updateCrownwardGeneralBanners:()=>{},liveBuildings:()=>[b],spawnBuildingUnit:()=>{spawns++;return {};}};
  vm.createContext(env);vm.runInContext(fn('updateBuildingRules'),env);
  for(time=1;time<110;time++)env.updateBuildingRules({idx:0},1);
  assert.equal(spawns,0);env.updateBuildingRules({idx:0},1);assert.equal(spawns,1);
  capital.owner=0;
  for(time=111;time<=165;time++)env.updateBuildingRules({idx:0},1);
  assert.equal(spawns,2,'recaptured capital restores the 55-second interval');
});

test('normal and Sunspire token generation both apply exactly the reversible capital penalty',()=>{
  const capital={kind:'capital',home:0,owner:0};
  const env={Math,SPAWN_SLOWEST_SECONDS:105,SPAWN_FASTEST_SECONDS:8.8,SUNSPIRE_MAX_SOLAR_BEACONS:10,SUNSPIRE_MIN_SPAWN_SECONDS:45,SUNSPIRE_BASE_SPAWN_SECONDS:91.5,
    SUNSPIRE_SOLAR_BEACON_SECONDS:4.5,SPAWN_TOKEN_RATE_MULT:2,GLOBAL_TOKEN_INTERVAL_MULT:.67,
    warState:{objectives:[capital]},capitalProduction,activeSolarBeacons:()=>3,isHumanSlot:()=>true,shrineSpawnMultiplier:()=>1,
    spawnTierInfoForFaction:()=>({seconds:50}),specialSpawnTilesForFaction:()=>0};
  vm.createContext(env);vm.runInContext(fn('spawnRateForFaction'),env);
  for(const id of ['crownward','sunspire']){
    const f={idx:0,id,spawnRate:1};capital.owner=0;const baseline=env.spawnRateForFaction(f,100,12);
    capital.owner=1;assert.equal(env.spawnRateForFaction(f,100,12),baseline*.5);
    capital.owner=0;assert.equal(env.spawnRateForFaction(f,100,12),baseline);
  }
});
