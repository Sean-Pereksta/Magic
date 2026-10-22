import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {MotionFilter,MAX_MPS,delta,TrailGPS} from './gps.mjs';
import {Vault,identity,derive,seal,unseal,importBackup} from './storage.mjs';
const start=(species='capuchin')=>C.newSave('Mossfoot',species,'gps',12345);
const fight=s=>{C.advance(s,C.ENCOUNTER_STEP,0,C.ENCOUNTER_STEP);C.readyBattle(s);};
const flush=async v=>{if(v.localTask)await v.localTask;if(v.inflight)await v.inflight;await v.sync();};
const memory=()=>{const map=new Map();return {getItem:k=>map.get(k)||null,setItem:(k,v)=>map.set(k,v),map};};
function cloud(){const docs=new Map(),scores=new Map();let available=true;return {docs,scores,set online(v){available=v;},async read(id){if(!available)throw Error('offline');return structuredClone(docs.get(id)||null);},async commit(id,base,e,history){if(!available)throw Error('offline');const old=docs.get(id);if((old?.revision||0)!==base||old&&old.salt!==e.salt){const err=Error('Cloud changed');err.code='save-conflict';throw err;}for(const h of history)if(h.mode==='gps')scores.set(`${id}_${h.id}`,structuredClone(h));docs.set(id,{...structuredClone(e),revision:base+1});return base+1;},async leaderboard(){return [...scores.values()].sort((a,b)=>b.score-a.score);}};}

// Core rules are covered by expansion-tests.mjs; tracker and vault regressions follow.
const METERS_PER_DEGREE=6371008.8*Math.PI/180,epoch=1800000000000;
const point=(t,north=0,east=0,speed=null,accuracy=3)=>({timestamp:epoch+t*1000,coords:{latitude:north/METERS_PER_DEGREE,longitude:east/METERS_PER_DEGREE,accuracy,speed}});
const fix=(f,t,n=0,e=0,speed=null,acc=3)=>f.accept(point(t,n,e,speed,acc),epoch+t*1000,true);
test('GPS accepts steady walking and computes cardinal displacement',()=>{const f=new MotionFilter();let distance=0;for(let t=0;t<=60;t++)distance+=fix(f,t,t*1.4,0,1.4).meters;assert.ok(distance>70&&distance<=84);});
test('GPS cap accepts 10 mph and rejects above it without banking the gap',()=>{const f=new MotionFilter();let good=0;for(let t=0;t<10;t+=2)good+=fix(f,t,t*MAX_MPS,0,MAX_MPS).meters;assert.ok(good>0);const fast=fix(f,10,100,0,6);assert.equal(fast.meters,0);assert.match(fast.reason,/10 mph/);assert.equal(fix(f,11,101,0,1).meters,0);assert.equal(fix(f,12,102,0,1).meters,0);});
test('stationary jitter and low device speed do not grant distance',()=>{const f=new MotionFilter();let moved=0;for(let t=0;t<500;t++)moved+=fix(f,t,Math.sin(t)*1.5,Math.cos(t)*1.5).meters;assert.equal(moved,0);assert.equal(fix(f,501,2,2,0).meters,0);});
test('teleports, weak fixes, stale points, duplicate times, and long gaps are rejected',()=>{const f=new MotionFilter();fix(f,0);fix(f,1,1);assert.equal(fix(f,2,1000).meters,0);assert.equal(fix(f,3,1001,0,1,40).meters,0);assert.equal(f.accept(point(0),epoch+20000).meters,0);fix(f,4,1002);assert.equal(fix(f,4,1003).meters,0);assert.equal(fix(f,25,1030).meters,0);});
test('hiding or resetting loses the baseline, not a jump on return',()=>{const f=new MotionFilter();fix(f,0);fix(f,1,1);assert.equal(f.accept(point(2,100),epoch+2000,false).meters,0);assert.equal(fix(f,3,200).meters,0);assert.equal(fix(f,4,201).meters,0);f.reset();assert.equal(fix(f,5,202).meters,0);});
test('haversine handles the antimeridian',()=>{assert.ok(delta({lat:0,lon:179.9999},{lat:0,lon:-179.9999}).meters<30);});

test('fresh stationary positions can confirm a hunt without earning walking progress',()=>{
  const f=new MotionFilter();fix(f,0,0,0,0);fix(f,1,0,0,0);const stationary=fix(f,2,0,0,0);
  assert.equal(stationary.meters,0);assert.equal(stationary.positionValid,true);assert.equal(stationary.timestamp,epoch+2000);
  assert.equal(fix(f,3,0,0,0,40).positionValid,undefined);assert.equal(fix(f,4,0,0,7).positionValid,undefined);
  fix(f,5,0,0,0);fix(f,6,0,0,0);assert.equal(fix(f,40,0,0,0).positionValid,undefined);
});

test('tracking requests permission only on start and discards callbacks after stop',async()=>{
  const saved={navigator:Object.getOwnPropertyDescriptor(globalThis,'navigator'),document:Object.getOwnPropertyDescriptor(globalThis,'document'),secure:Object.getOwnPropertyDescriptor(globalThis,'isSecureContext')};
  let requests=0,cleared=0,callback,fixes=0;
  try{
    Object.defineProperty(globalThis,'navigator',{configurable:true,value:{geolocation:{watchPosition(fn){requests++;callback=fn;return 7;},clearWatch(id){assert.equal(id,7);cleared++;}}}});
    Object.defineProperty(globalThis,'document',{configurable:true,value:{hidden:false}});Object.defineProperty(globalThis,'isSecureContext',{configurable:true,value:true});
    const gps=new TrailGPS(()=>fixes++,()=>{});assert.equal(requests,0);await gps.start({wakeLock:false});assert.equal(requests,1);await gps.start();assert.equal(requests,1);
    gps.stop();assert.equal(cleared,1);callback(point(1));assert.equal(fixes,0);assert.equal(gps.active,false);
    globalThis.document.hidden=true;await assert.rejects(gps.start(),/foreground/);assert.equal(requests,1);
  }finally{for(const [key,prop]of [['navigator',saved.navigator],['document',saved.document],['isSecureContext',saved.secure]]){if(prop)Object.defineProperty(globalThis,key,prop);else delete globalThis[key];}}
});

test('encryption round trip, random IVs, wrong-password and tamper rejection',async()=>{const s=start(),salt=btoa('0123456789abcdef'),id=await identity('Mossfoot'),key=await derive('a-unique-password',salt);const a=await seal(s,key,salt,id),b=await seal(s,key,salt,id);assert.notEqual(a.iv,b.iv);assert.deepEqual(await unseal(a,key,id),s);assert.equal(JSON.stringify(a).includes('Mossfoot'),false);await assert.rejects(unseal(a,await derive('incorrect',salt),id));await assert.rejects(unseal({...a,cipher:a.cipher.slice(0,15)+'AAAA'+a.cipher.slice(19)},key,id));assert.equal(await identity(' MOSSFOOT '),id);assert.notEqual(await identity('Mossfoot','practice'),id);});
test('cloud save and password reopen use encrypted records and no coordinates',async()=>{const c=cloud(),m=memory(),v=new Vault(()=>{},m,async()=>c);let s=await v.open('Mossfoot','a-unique-password',{create:true});C.advance(s,100,0,100);await v.checkpoint(s,true);await flush(v);const w=new Vault(()=>{},memory(),async()=>c),reopened=await w.open('MOSSFOOT','a-unique-password');assert.equal(reopened.run.distance,100);await assert.rejects(new Vault(()=>{},memory(),async()=>c).open('Mossfoot','wrong'));assert.ok(!JSON.stringify([...c.docs.values()]).includes('latitude'));assert.ok(!JSON.stringify([...m.map.values()]).includes('a-unique-password'));});
test('separate devices detect conflicts and do not silently overwrite',async()=>{const c=cloud(),a=new Vault(()=>{},memory(),async()=>c);const first=await a.open('Mossfoot','a-unique-password',{create:true});await flush(a);const b=new Vault(()=>{},memory(),async()=>c),second=await b.open('Mossfoot','a-unique-password');first.run.fruit=91;await a.checkpoint(first,true);await flush(a);second.run.fruit=42;await b.checkpoint(second,true);await flush(b);assert.equal(b.conflict,true);const restored=await b.loadCloud();assert.equal(restored.run.fruit,91);assert.equal(b.conflict,false);});
test('offline queue retries and completed run scores are idempotent',async()=>{const c=cloud(),v=new Vault(()=>{},memory(),async()=>c);const s=await v.open('Mossfoot','a-unique-password',{create:true});await flush(v);c.online=false;s.run.fruit=99;s.meta.history=[{id:'run-one',name:s.name,mode:'gps',score:2000,feet:1000,wins:2,level:2,form:'Leaf Capuchin',endedAt:1}];await v.checkpoint(s,true);await flush(v);assert.equal(v.dirty,true);c.online=true;await flush(v);assert.equal(v.dirty,false);assert.equal(c.scores.size,1);await v.checkpoint(s,true);await flush(v);assert.equal(c.scores.size,1);});
test('practice never connects or posts scores',async()=>{let calls=0;const v=new Vault(()=>{},memory(),async()=>{calls++;throw Error('should not connect');});const s=await v.open('Practice Monkey','practice-password',{create:true,mode:'practice'});C.advance(s,C.ENCOUNTER_STEP,0,C.ENCOUNTER_STEP);await v.checkpoint(s,true);await flush(v);assert.equal(calls,0);});
test('latest checkpoint wins local coalescing',async()=>{const c=cloud(),v=new Vault(()=>{},memory(),async()=>c);const s=await v.open('Mossfoot','a-unique-password',{create:true});await flush(v);for(let i=0;i<30;i++){s.run.fruit=i;void v.checkpoint(s);}await flush(v);const w=new Vault(()=>{},memory(),async()=>c),r=await w.open('Mossfoot','a-unique-password');assert.equal(r.run.fruit,29);});
test('storage failure is reported while cloud saving can still succeed',async()=>{const c=cloud(),messages=[],v=new Vault(m=>messages.push(m),{getItem:()=>null,setItem:()=>{throw Error('quota');}},async()=>c);await v.open('Mossfoot','a-unique-password',{create:true});await flush(v);assert.equal(v.localOK,false);assert.ok(messages.some(m=>m.kind==='error'));assert.equal(v.dirty,false);assert.ok(messages.at(-1).text.includes('Cloud saved'));});

test('portable encrypted backups restore after password verification',async()=>{const m=memory(),v=new Vault(()=>{},m,async()=>{throw Error('offline');});const s=await v.open('Backup Monkey','backup-password',{create:true,mode:'practice'});s.run.fruit=64;await v.checkpoint(s);const text=await v.export(),other=memory();await assert.rejects(importBackup('Backup Monkey','wrong',text,other));assert.equal(other.map.size,0);assert.equal(await importBackup('Backup Monkey','backup-password',text,other),'practice');const w=new Vault(()=>{},other);const r=await w.open('Backup Monkey','backup-password',{mode:'practice'});assert.equal(r.run.fruit,64);});

test('accepted walking reports each exact discovery once without changing rewards',()=>{
  const categories=new Set();
  for(let seed=1;seed<=100;seed++){
    const s=C.newSave('Finds','capuchin','gps',seed),control=structuredClone(s),found=[];
    const used=C.advance(s,C.RESOURCE_STEP*3,0,C.RESOURCE_STEP*3,d=>found.push(d));
    assert.equal(used,C.advance(control,C.RESOURCE_STEP*3,0,C.RESOURCE_STEP*3));
    assert.deepEqual(s,control,'Notifications must not alter rolls, rewards or saved progress');
    assert.equal(found.length,3);assert.deepEqual(found.at(-1),s.run.lastDiscovery);
    for(const d of found){categories.add(d.category);assert.ok(d.amount>0);}
    C.advance(s,0,0,0,d=>found.push(d));assert.equal(found.length,3);
  }
  assert.deepEqual([...categories].sort(),['essence','fragment','fruit','item']);
});
