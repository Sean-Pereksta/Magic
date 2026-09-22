import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
import {snapshotDelta,placementRating,applyWarResult,rankProgress} from './core.mjs';
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
