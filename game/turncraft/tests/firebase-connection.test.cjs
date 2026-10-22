const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {pathToFileURL} = require('node:url');
const {simulation} = require('./helpers/expansion-harness.cjs');
const connection = () => import(pathToFileURL(path.resolve(__dirname, '../shattered-front/firebase-connection.mjs')).href);

test('persisted credentials settle before a guest sign-in is considered', async () => {
  const {restoreFirebaseUser} = await connection();
  let release, signIns = 0;
  const auth = {currentUser: null, authStateReady: () => new Promise(resolve => {release = resolve;})};
  const ready = restoreFirebaseUser(auth, {signInAnonymously: async () => {signIns++;}});
  await Promise.resolve();assert.equal(signIns, 0);
  auth.currentUser = {uid: 'durable-player'};release();
  assert.equal((await ready).uid, 'durable-player');assert.equal(signIns, 0);
});

test('game and profile startup share one anonymous sign-in and the same resulting UID', async () => {
  const {restoreFirebaseUser} = await connection();
  let release, signIns = 0;
  const auth = {currentUser: null, authStateReady: async () => {}};
  const api = {signInAnonymously: async () => {signIns++;await new Promise(resolve => {release = resolve;});auth.currentUser = {uid: 'one-guest'};}};
  const game = restoreFirebaseUser(auth, api), profile = restoreFirebaseUser(auth, api);
  assert.equal(game, profile);
  await new Promise(resolve => setImmediate(resolve));assert.equal(signIns, 1);release();
  assert.equal((await game).uid, 'one-guest');assert.equal((await profile).uid, 'one-guest');
});

test('a rejected sign-in clears the shared request so retry can recover', async () => {
  const {restoreFirebaseUser} = await connection();
  let signIns = 0;
  const auth = {currentUser: null, authStateReady: async () => {}};
  const api = {signInAnonymously: async () => {if (++signIns === 1) throw Object.assign(new Error('Disabled'), {code: 'auth/operation-not-allowed'});auth.currentUser = {uid: 'recovered'};}};
  await assert.rejects(restoreFirebaseUser(auth, api), /Enable it in Firebase Authentication/);
  assert.equal((await restoreFirebaseUser(auth, api)).uid, 'recovered');assert.equal(signIns, 2);
});

test('a stalled initial Auth callback times out and unsubscribes', async () => {
  const {restoreFirebaseUser} = await connection();
  let stopped = 0;
  await assert.rejects(restoreFirebaseUser({currentUser: null}, {onAuthStateChanged: () => () => {stopped++;}, signInAnonymously: async () => {}}, 10), {code: 'deadline-exceeded'});
  assert.equal(stopped, 1);
});

test('the main game reuses a profile-first initialized app with the real Firebase SDK', async () => {
  const sdk = require('firebase/app');
  const {firebaseConfig} = await import(pathToFileURL(path.resolve(__dirname, '../../../lobby/firebase-config.mjs')).href);
  const profileApp = sdk.initializeApp(firebaseConfig);
  try {
    const html = fs.readFileSync(path.resolve(__dirname, '../../turncraft.html'), 'utf8');
    const start = html.indexOf('const firebaseConfig='), end = html.indexOf('const params=', start);
    const context = {initializeApp: sdk.initializeApp, getApps: sdk.getApps, getApp: sdk.getApp, getAuth: () => ({}), getFirestore: () => ({})};
    vm.runInNewContext(html.slice(start, end) + ';globalThis.loadedApp=app;', context);
    assert.equal(context.loadedApp, profileApp);assert.equal(sdk.getApps().length, 1);
  } finally {await sdk.deleteApp(profileApp);}
});

test('an empty presence snapshot cannot replace another player’s live controller lease', async () => {
  const run = simulation();
  run(`lobbyId='shared';lobbyRef='room';presenceRef='presence';auth.currentUser={uid:'peer'};lobbyData={host:'Host'};onlinePresence=[];globalThis.leaseWrites=[];
    globalThis.currentLease={ownerUid:'host',ownerName:'Host',expiresAtMs:testClock+60000,epoch:4};
    runTransaction=async(db,operation)=>operation({get:async()=>({exists:()=>true,data:()=>({chessStarfrontController:currentLease})}),set:(...args)=>leaseWrites.push(args)});`);
  await run('refreshPresenceAndLease()');
  assert.deepEqual(run.plain('leaseWrites'), []);
  assert.equal(run('controllerLease.ownerUid'), 'host');assert.equal(run('isController'), false);
});

test('a controller reconnect reinstalls its command listener even when it remains the controller', async () => {
  const run = simulation();
  run.context.restoreFirebaseUser = async auth => auth.currentUser;
  run.context.onAuthStateChanged = () => () => {};
  run.context.signInAnonymously = async () => {};
  run(`lobbyId='shared';username='Host';auth.currentUser={uid:'host'};isController=true;gameStarted=true;
    controllerLease={ownerUid:'host',ownerName:'Host',expiresAtMs:testClock+60000,epoch:3};
    globalThis.room={players:['Host','Peer'],host:'Host',chessStarfrontController:controllerLease};
    globalThis.oldListenerStopped=0;commandUnsub=()=>oldListenerStopped++;
    globalThis.commandSubscriptions=0;globalThis.receivedCommands=[];
    getDoc=async ref=>({exists:()=>ref==='lobbies/shared',data:()=>room});
    onSnapshot=(ref,next)=>{if(ref.endsWith('_commands')){commandSubscriptions++;globalThis.receiveRemote=next;}return()=>{}};
    runTransaction=async(db,operation)=>operation({get:async()=>({exists:()=>true,data:()=>room}),set:()=>{}});
    renderStartMenu=()=>{};renderStartStatus=()=>{};applyCommand=command=>receivedCommands.push(command);`);
  await run('reconnectNetwork()');
  assert.equal(run('isController'), true);assert.equal(run('oldListenerStopped'), 1);assert.equal(run('commandSubscriptions'), 1, run('statusText.textContent'));
  run(`receiveRemote({docChanges:()=>[{type:'added',doc:{id:'remote-one',data:()=>({id:'remote-one',createdAtMs:testClock,slot:1,type:'groupStop',entityIds:[]})}}]});`);
  assert.equal(run('receivedCommands.length'), 1);assert.equal(run('receivedCommands[0].id'), 'remote-one');
});
