import test from 'node:test';
import assert from 'node:assert/strict';
import { resolveWinAccount, readWinCount, recordWinTransaction, winReceiptId, readPendingWins, savePendingWins } from './wins.mjs';
import { firestoreFixture, memoryStorage } from './wins-fixture.mjs';

const resolveWith=h=>({readExact:async name=>{const snap=await h.sdk.getDoc(h.sdk.doc(h.sdk.db,'users',name));return snap.exists()?{id:snap.id,data:snap.data()}:null;},
  findByUsername:async name=>{const snap=await h.sdk.getDocs(h.sdk.query(h.sdk.collection(h.sdk.db,'users'),h.sdk.where('username','==',name),h.sdk.limit(2)));return snap.docs.map(s=>({id:s.id,data:s.data()}));}});
const award=(h,matchId='match-one',extras={})=>recordWinTransaction({...h.sdk,accountId:'Sean',matchId,gameId:'lobby-one',uid:'anonymous-session',now:()=>123,...extras});

test('account wins prefer the exact lobby account ID even when username is absent or duplicated',async()=>{
  const h=firestoreFixture({'users/Sean':{wins:17},'users/duplicate':{username:'Sean',wins:0}});
  assert.deepEqual(await resolveWinAccount({username:'Sean',...resolveWith(h)}),{id:'Sean',wins:17});
  assert.deepEqual(h.reads,['users/Sean']);
});
test('legacy lookup is bounded and refuses ambiguous accounts or missing records',async()=>{
  const h=firestoreFixture({'users/legacy-id':{username:'Sean',wins:'12'}});
  assert.deepEqual(await resolveWinAccount({username:'Sean',...resolveWith(h)}),{id:'legacy-id',wins:12});
  assert.ok(h.reads.filter(r=>typeof r==='object').every(q=>q.constraints.some(c=>c.count===2)));
  h.records.set('users/another-id',{username:'Sean',wins:0});
  await assert.rejects(resolveWinAccount({username:'Sean',...resolveWith(h)}),/Several accounts/);
  await assert.rejects(resolveWinAccount({username:'Missing',...resolveWith(h)}),/No saved account/);
});
test('read failures and malformed counts remain errors instead of invented zero counts',async()=>{
  const h=firestoreFixture();h.offline(true);
  await assert.rejects(resolveWinAccount({username:'Sean',...resolveWith(h)}),/unavailable/);
  assert.equal(readWinCount(undefined),0);assert.equal(readWinCount('30'),30);
  for(const value of [-1,NaN,1.5,'bad'])assert.throws(()=>readWinCount(value),/invalid/);
});
test('simultaneous devices and reloads award a match once and preserve unrelated account fields',async()=>{
  const h=firestoreFixture({'users/Sean':{wins:17,displayName:'Sean',favoriteGames:['catmouse']}});
  await Promise.all([award(h),award(h)]);await award(h);
  assert.equal(h.records.get('users/Sean').wins,18);assert.equal(h.commits.length,1);
  assert.equal(h.records.get('users/Sean').displayName,'Sean');assert.deepEqual(h.records.get('users/Sean').favoriteGames,['catmouse']);
  assert.equal(h.records.get('gameStats/'+winReceiptId('Sean','match-one')).accountId,'Sean');
});
test('distinct matches racing on the same account each award one win',async()=>{
  const h=firestoreFixture();await Promise.all([award(h,'one'),award(h,'two')]);
  assert.equal(h.records.get('users/Sean').wins,19);assert.equal(h.commits.length,2);
});
test('a lost acknowledgement can be retried without a second increment',async()=>{
  const h=firestoreFixture();h.loseNextAcknowledgement();
  await assert.rejects(award(h),/acknowledgement lost/);
  assert.deepEqual(await award(h),{wins:18,awarded:false});assert.equal(h.commits.length,1);
});
test('legacy confirmed markers migrate to durable receipts without recounting an old win',async()=>{
  const h=firestoreFixture();assert.deepEqual(await award(h,'old',{legacyAlreadyAwarded:true}),{wins:17,awarded:false});
  await award(h,'old');assert.equal(h.records.get('users/Sean').wins,17);
  await award(h,'new');assert.equal(h.records.get('users/Sean').wins,18);
});
test('deleted accounts are not recreated and receipt names cannot collide or escape their document',async()=>{
  const h=firestoreFixture({});await assert.rejects(award(h),/no longer exists/);assert.equal(h.commits.length,0);
  assert.notEqual(winReceiptId('a_b','c'),winReceiptId('a','b_c'));assert.ok(!winReceiptId('a/b','c/d').includes('/'));
});
test('pending victories survive a reload before account lookup succeeds and stay scoped to the account name',()=>{
  const storage=memoryStorage(),job={accountName:'Sean',accountId:null,matchId:'one',gameId:'lobby',uid:'anonymous'};
  assert.equal(savePendingWins(storage,'Sean',[job,job]),true);assert.deepEqual(readPendingWins(storage,'Sean'),[job]);
  assert.deepEqual(readPendingWins(storage,'SomeoneElse'),[]);savePendingWins(storage,'Sean',[]);assert.deepEqual(readPendingWins(storage,'Sean'),[]);
});
