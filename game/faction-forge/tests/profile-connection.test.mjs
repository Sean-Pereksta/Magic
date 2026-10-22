import test from 'node:test';
import assert from 'node:assert/strict';
import {createProfileConnectionWaiter,profileConnectionError} from '../profile-connection.js';

test('concurrent saved actions share a delayed connection and run only once it is ready',async()=>{
  let release,calls=0;
  const connection=new Promise(resolve=>{release=resolve;});
  const ready=createProfileConnectionWaiter({connect:async()=>{calls++;return connection;},timeoutMs:500});
  const first=ready(),second=ready();
  assert.equal(first,second);
  await Promise.resolve();assert.equal(calls,1);
  release({username:'Sean',forgeDust:87});
  assert.deepEqual(await first,{username:'Sean',forgeDust:87});
  assert.deepEqual(await second,{username:'Sean',forgeDust:87});
});

test('a failed restore can retry the saved profile instead of retaining a rejected wait',async()=>{
  let calls=0;
  const saved={username:'Sean',forgeDust:87};
  const ready=createProfileConnectionWaiter({connect:async()=>{
    if(++calls===1)throw Object.assign(new Error('Denied'),{code:'permission-denied'});
    return saved;
  }});
  await assert.rejects(ready(),{code:'permission-denied'});
  assert.equal(await ready(),saved);assert.equal(calls,2);
});

test('a stalled restore exposes an actionable timeout and allows a fresh retry',async()=>{
  let calls=0,timedOut;
  const ready=createProfileConnectionWaiter({connect:()=>++calls===1?new Promise(()=>{}):'saved profile',timeoutMs:10,onTimeout:error=>{timedOut=error;}});
  await assert.rejects(ready(),error=>error.code==='deadline-exceeded'&&/Retry profile connection/.test(error.message));
  assert.equal(timedOut.code,'deadline-exceeded');assert.equal(await ready(),'saved profile');
});

test('connection failures distinguish rules and disabled anonymous Auth from network failures',()=>{
  assert.match(profileConnectionError({code:'permission-denied'}).message,/updated Firestore rules/);
  assert.match(profileConnectionError({code:'auth/operation-not-allowed'}).message,/Enable Anonymous sign-in/);
  assert.match(profileConnectionError({code:'unavailable'}).message,/saved data has not been replaced/);
  assert.equal(profileConnectionError({code:'MISSION_LOCKED',message:'Complete the previous mission first.'}).message,'Complete the previous mission first.');
});
