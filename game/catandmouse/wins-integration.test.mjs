import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import * as wins from './wins.mjs';
import { firestoreFixture, memoryStorage } from './wins-fixture.mjs';

const core=readFileSync(new URL('../catandmouse-core.html',import.meta.url),'utf8');
const section=core.slice(core.indexOf('  async function fetchPlayerWins(){'),core.indexOf('  function defeatAllEnemies(){'));
function harness(h=firestoreFixture(),storage=memoryStorage()){
  const elements=new Map(),timers=[];
  const ctx=vm.createContext({...h.sdk,...wins,username:'Sean',usernameNorm:'sean',savedAccountUsername:'Sean',gameId:'lobby',matchId:'match',uid:'anonymous-uid',
    playerWins:0,playerWinsKnown:false,playerUserDocId:null,winsFetchPromise:null,winsUnsubscribe:null,winsStatus:'loading',winsError:'',
    pendingWinFlush:null,winRetryTimer:null,winAwardPromise:null,winAwardMatch:null,winSavedMatch:null,pageSuspended:false,catHealth:0,
    navigator:{onLine:true},localStorage:storage,ensureLocalBuildControls(){},handleCatDefeated(){},
    setTimeout:fn=>{timers.push(fn);return timers.length;},console:{warn(){},error(){}},
    document:{getElementById:id=>{if(!elements.has(id))elements.set(id,{});return elements.get(id);}}});
  vm.runInContext(section,ctx);return {h,ctx,elements,timers,storage};
}
test('real win loading and live snapshots use users/{account}, retain known wins on error and ignore stale cached totals',async()=>{
  const a=harness();await a.ctx.fetchPlayerWins();assert.equal(a.ctx.playerWins,17);
  assert.equal(a.ctx.playerUserDocId,'Sean');assert.equal(a.h.listeners.size,1);
  const listener=[...a.h.listeners][0];assert.equal(listener.ref.path,'users/Sean');
  listener.next({exists:()=>true,data:()=>({wins:2}),metadata:{fromCache:true,hasPendingWrites:false}});assert.equal(a.ctx.playerWins,17);
  listener.next({exists:()=>true,data:()=>({wins:22}),metadata:{fromCache:false,hasPendingWrites:false}});assert.equal(a.ctx.playerWins,22);
  a.h.offline(true);assert.equal(await a.ctx.fetchPlayerWins(),false);assert.equal(a.ctx.playerWins,22);
  assert.equal(a.elements.get('hudWins').textContent,'🏆 22');assert.equal(a.ctx.winsStatus,'unavailable');
});
test('real award failures retain a pending victory and reload recovery saves it once without phantom wins',async()=>{
  const a=harness();a.h.offline(true);
  assert.equal(await a.ctx.awardWinOnce(),false);assert.equal(a.ctx.playerWinsKnown,false);
  assert.equal(wins.readPendingWins(a.storage,'Sean').length,1);assert.equal(a.ctx.winSavedMatch,null);
  a.h.offline(false);const reload=harness(a.h,a.storage);reload.ctx.catHealth=1000;reload.ctx.matchId='next-match';
  await reload.ctx.fetchPlayerWins();await reload.ctx.retryPendingWins();
  assert.equal(a.h.records.get('users/Sean').wins,18);assert.equal(wins.readPendingWins(a.storage,'Sean').length,0);
  assert.equal(reload.ctx.winSavedMatch,null);assert.equal(a.h.commits.length,1);
});
test('real award coalesces repeated victory events and pending retries remain idempotent after an acknowledgement loss',async()=>{
  const a=harness();await a.ctx.fetchPlayerWins();await a.ctx.retryPendingWins();a.h.loseNextAcknowledgement();
  assert.equal(await a.ctx.awardWinOnce(),false);assert.equal(a.ctx.playerWins,17);
  await a.ctx.retryPendingWins();assert.equal(a.ctx.playerWins,18);assert.equal(a.ctx.winSavedMatch,'match');
  await Promise.all([a.ctx.awardWinOnce(),a.ctx.awardWinOnce()]);assert.equal(a.h.records.get('users/Sean').wins,18);
  assert.equal(a.h.commits.length,1);
});
test('failed loading shows an unavailable count and cannot replace a saved account total with zero',async()=>{
  const a=harness();a.h.offline(true);await a.ctx.fetchPlayerWins();
  assert.equal(a.elements.get('hudWins').textContent,'🏆 —');assert.equal(a.h.records.get('users/Sean').wins,17);
  assert.equal(a.h.commits.length,0);
});
test('win receipts use the existing authenticated gameStats document rules',()=>{
  const rules=readFileSync(new URL('../../firestore.rules',import.meta.url),'utf8');
  assert.match(rules,/match \/gameStats\/\{gameId\}\s*\{\s*allow read, write: if isAuthed\(\);/);
});
