import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {Maze,LatestWriter,chooseSpawn,RULES} from '../game/stoney-relic/core.mjs';

// A deterministic transaction double: commits are atomic, concurrent callbacks serialize,
// all reads must precede writes, and injected commit failures leave every document unchanged.
// This is not the Firebase emulator or a live-project integration test.
const directory=await mkdtemp(join(tmpdir(),'stoney-network-'));
const core=await readFile(new URL('../game/stoney-relic/core.mjs',import.meta.url),'utf8');
let network=await readFile(new URL('../game/stoney-relic/network.mjs',import.meta.url),'utf8');
network=network.replace(/from 'https:\/\/www\.gstatic\.com\/firebasejs\/[^']+'/g,"from './sdk.mjs'");
await writeFile(join(directory,'core.mjs'),core);await writeFile(join(directory,'network.mjs'),network);
await writeFile(join(directory,'sdk.mjs'),`
export const store=new Map();export let rejectPrefix='';export function reject(path){rejectPrefix=path;}
let id=0,chain=Promise.resolve();export const db={};
export const doc=(...parts)=>({path:parts.map(p=>p?.path|| (typeof p==='string'?p:'')).filter(Boolean).join('/')||''+(++id)});
export function collection(...parts){return doc(...parts);}
const originalDoc=doc;
export const serverTimestamp=()=>10000;
export const getApps=()=>[];export const initializeApp=()=>({});export const getFirestore=()=>db;export const getAuth=()=>({currentUser:{uid:'p'}});
export const signInAnonymously=async()=>({user:{uid:'p'}});export const onAuthStateChanged=(auth,fn)=>{queueMicrotask(()=>fn(auth.currentUser));return()=>{};};
function snapshot(ref,data=store.get(ref.path)){return {exists:()=>data!==undefined,data:()=>structuredClone(data),id:ref.path.split('/').at(-1)};}
function patch(target,values){for(const [key,value] of Object.entries(values)){const names=key.split('.');let node=target;for(const part of names.slice(0,-1))node=node[part]??={};node[names.at(-1)]=structuredClone(value);}return target;}
export async function runTransaction(db,fn){const next=chain.then(async()=>{let writing=false;const changes=[];const result=await fn({get:async ref=>{if(writing)throw Error('Read after write');return snapshot(ref);},set:(ref,data,options)=>{writing=true;changes.push({ref,data,merge:options?.merge});},update:(ref,data)=>{writing=true;changes.push({ref,data,merge:true});}});if(rejectPrefix&&changes.some(c=>c.ref.path.startsWith(rejectPrefix)))throw Object.assign(Error('Injected write failure'),{code:'unavailable'});const staged=new Map(store);for(const c of changes)staged.set(c.ref.path,patch(c.merge?structuredClone(staged.get(c.ref.path)||{}):{},c.data));store.clear();for(const [k,v] of staged)store.set(k,v);return result;});chain=next.catch(()=>{});return next;}
export const setDoc=(ref,data,options)=>runTransaction(db,tx=>tx.set(ref,data,options));export const getDocFromServer=async ref=>snapshot(ref);
export const onSnapshot=()=>()=>{};
export const writeBatch=()=>{const paths=[];return {delete:r=>paths.push(r.path),commit:async()=>paths.forEach(p=>store.delete(p))};};
`);
// doc(collection) needs a stable auto-ID for idempotent placement retries.
let sdk=await readFile(join(directory,'sdk.mjs'),'utf8');sdk=sdk.replace("parts.map(p=>p?.path||", "(parts.length===1&&parts[0]?.path?[parts[0],String(++id)]:parts).map(p=>p?.path||");await writeFile(join(directory,'sdk.mjs'),sdk);
const {RelicSession}=await import(pathToFileURL(join(directory,'network.mjs')));
const {store,db,reject}=await import(pathToFileURL(join(directory,'sdk.mjs')));
Object.defineProperty(globalThis,'navigator',{value:{onLine:true},configurable:true});
globalThis.document={hidden:false,addEventListener(){},removeEventListener(){}};
globalThis.window={addEventListener(){},removeEventListener(){}};
let now=10000;
const ref=path=>({path}),lobby='lobbies/test',base=()=>({seed:42,w:21,h:21,cellSize:4,runId:'r',dmUid:'dm',phase:'play',startAt:1000,endAt:301000,crown:null,carrierId:null,dmEnergyBase:100,dmEnergyStamp:10000,spawnRevision:0});
function setup(s=base()){
 store.clear();reject('');now=10000;store.set(lobby,{dm:'DM',stoney:s});return s;
}
function session(id='p',s=store.get(lobby).stoney,point=new Maze(s).center(100)){
 const client=new RelicSession({gameId:'test',name:id,dm:id==='dm'});Object.assign(client,{uid:id,sessionId:`session-${id}`,db,lobbyRef:ref(lobby),myRef:ref(`${lobby}/players/${id}`),playersRef:ref(`${lobby}/players`),trapsRef:ref(`${lobby}/traps`),runtimeRef:ref(`${lobby}/stoneyRuntime/state`),s:structuredClone(s),maze:new Maze(s),ready:new Set(['lobby','players','traps','runtime'])});client.now=()=>now;
 const p={...point,uid:id,name:id,sessionId:client.sessionId,role:id==='dm'?'dm':'player',runId:'r',ready:true,online:true,hidden:false,alive:true,deadUntil:0,protectedUntil:0,spawnSeq:1,yaw:0,seq:1,serverAt:now,updatedAt:now};store.set(client.myRef.path,p);client.pose={...p};client.players.set(id,{...p});client.writer=new LatestWriter(v=>client.writePosition(v),{now:()=>now});return client;
}
function authority(client){const r={runId:'r',hostId:client.uid,hostSession:client.sessionId,hostEpoch:1,hostUntil:now+6500,seq:1,frameAt:now,enemies:{}};store.set(client.runtimeRef.path,r);client.runtime=structuredClone(r);return r;}
function playerDoc(client){return store.get(client.myRef.path);}
test.after(async()=>{await rm(directory,{recursive:true,force:true});});
test('simultaneous pickups result in exactly one carrier',async()=>{
 const s=setup(),m=new Maze(s);s.crown=m.center(100);const a=session('a',s),b=session('b',s);await Promise.all([a.playerAction('pickup'),b.playerAction('pickup')]);assert.ok(['a','b'].includes(store.get(lobby).stoney.carrierId));assert.equal(store.get(lobby).stoney.carrierName,store.get(lobby).stoney.carrierId);
});
test('a dead player cannot claim the crown',async()=>{
 const s=setup(),p=session('p',s);s.crown={x:p.pose.x,z:p.pose.z};playerDoc(p).alive=false;await p.playerAction('pickup');assert.equal(store.get(lobby).stoney.carrierId,null);
});
test('death and crown drop commit atomically; delayed movement cannot revive or teleport',async()=>{
 const s=setup(),p=session('p',s);s.carrierId='p';s.carrierName='p';const old={...p.pose};store.set(`${lobby}/traps/fire`,{type:'fire',x:p.pose.x,z:p.pose.z,runId:'r',placedAt:1000,ttlMs:100000});
 await p.playerAction('caught',{id:'fire'});assert.equal(playerDoc(p).alive,false);assert.equal(playerDoc(p).deadUntil,now+15000);assert.equal(store.get(lobby).stoney.carrierId,null);
 await p.writePosition({...old,x:old.x+5});assert.equal(playerDoc(p).alive,false);assert.equal(playerDoc(p).x,old.x);
});
test('early respawn is rejected; successful respawn grants protection',async()=>{
 const s=setup(),p=session('p',s);playerDoc(p).alive=false;playerDoc(p).deadUntil=now+15000;await p.playerAction('respawn');assert.equal(playerDoc(p).alive,false);now+=15001;await p.playerAction('respawn');assert.equal(playerDoc(p).alive,true);assert.equal(playerDoc(p).protectedUntil,now+3000);assert.equal(playerDoc(p).spawnSeq,2);
});
test('escape revalidates crown ownership and deadline',async()=>{
 const s=setup(),p=session('p',s,new Maze(s).spawn('p'));s.carrierId='other';await p.playerAction('escape');assert.equal(store.get(lobby).stoney.phase,'play');now+=1000;s.carrierId='p';s.endAt=now-1;await p.playerAction('escape');assert.equal(store.get(lobby).stoney.phase,'play');
});
test('spawn failure rolls back its energy charge',async()=>{
 const s=setup(),dm=session('dm',s);const point=chooseSpawn('demon',s,dm.maze,dm.players,dm.traps,now);reject(`${lobby}/traps`);await dm.place('demon',point);assert.equal(store.get(lobby).stoney.dmEnergyBase,100);assert.equal([...store.keys()].filter(k=>k.startsWith(`${lobby}/traps`)).length,0);reject('');
});
test('one spawn creates one trap and deducts energy in the same commit',async()=>{
 const s=setup(),dm=session('dm',s),point=chooseSpawn('ghost',s,dm.maze,dm.players,dm.traps,now);await dm.place('ghost',point);assert.equal(store.get(lobby).stoney.dmEnergyBase,50);const traps=[...store].filter(([k])=>k.startsWith(`${lobby}/traps`));assert.equal(traps.length,1);assert.equal(traps[0][1].armedAt,now+1200);
});
test('new host cannot steal an active lease; expired lease resumes snapshots with a new epoch',async()=>{
 setup();const a=session('a'),b=session('b'),r=authority(a);r.enemies={d:{x:1,z:1}};await b.claimHost();assert.equal(store.get(a.runtimeRef.path).hostId,'a');now+=7000;await b.claimHost();const next=store.get(a.runtimeRef.path);assert.equal(next.hostId,'b');assert.equal(next.hostEpoch,2);assert.deepEqual(next.enemies,r.enemies);
});
test('old host publication is fenced after a handoff',async()=>{
 setup();const a=session('a'),b=session('b');authority(a);now+=7000;await b.claimHost();a.enemies={evil:{x:99,z:99}};await a.publish();assert.equal(store.get(a.runtimeRef.path).hostId,'b');assert.deepEqual(store.get(a.runtimeRef.path).enemies,{});
});
test('authority finalizes the timer without needing another lobby snapshot',async()=>{
 const s=setup();s.endAt=9999;const dm=session('dm',s);authority(dm);await dm.maintain();assert.equal(store.get(lobby).stoney.phase,'ended');assert.equal(store.get(lobby).stoney.winner,'dm');
});
test('a stale carrier drops the crown at their last safe position',async()=>{
 const s=setup(),p=session('p',s),dm=session('dm',s);s.carrierId='p';playerDoc(p).serverAt=-10000;playerDoc(p).updatedAt=-10000;authority(dm);await dm.maintain();assert.equal(store.get(lobby).stoney.carrierId,null);assert.deepEqual(store.get(lobby).stoney.crown,{x:p.pose.x,z:p.pose.z});
});
test('per-action single-flight guard prevents frame-rate transaction storms',async()=>{
 setup();const p=session();let resolve,count=0;const work=()=>{count++;return new Promise(r=>resolve=r);};const first=p.action('test',work);for(let i=0;i<100;i++)await p.action('test',work);assert.equal(count,1);resolve();await first;
});
test('a superseded browser session cannot overwrite the player',async()=>{
 setup();const p=session();playerDoc(p).sessionId='new-tab';await assert.rejects(p.writePosition({...p.pose,x:999}),/session changed/);assert.notEqual(playerDoc(p).x,999);
});
