import test from 'node:test';
import assert from 'node:assert/strict';
import { createPatchWriter, stateFieldPatch, eligibleHostCandidate, reconcileKeyedNodes } from './sync.mjs';

const deferred = () => { let resolve, reject; const promise = new Promise((a,b) => {resolve=a;reject=b;}); return {promise,resolve,reject}; };
const microtasks = async () => { for (let i=0;i<8;i++) await Promise.resolve(); };
function clock() {
  let time=0, id=0;
  const timers=new Map();
  return {
    now:()=>time,
    schedule:(fn,delay)=>{timers.set(++id,{fn,at:time+delay});return id;},
    unschedule:key=>timers.delete(key),
    async advance(ms=0) {
      const end=time+ms;
      await microtasks();
      for (;;) {
        const next=[...timers].filter(([,t])=>t.at<=end).sort((a,b)=>a[1].at-b[1].at)[0];
        if (!next) break;
        time=next[1].at;timers.delete(next[0]);next[1].fn();await microtasks();
      }
      time=end;await microtasks();
    }
  };
}
function harness(options={}) {
  const c=clock(), calls=[];
  const writer=createPatchWriter({...c,write:async patch=>{calls.push({patch,at:c.now()});},...options});
  return {c,calls,writer};
}

test('coalesces a burst into the newest position and a single completion', async()=>{
  const {writer,c,calls}=harness();
  const first=writer.enqueue({x:1,y:1,clientSeq:1});
  for(let x=2;x<=100;x++) assert.equal(writer.enqueue({x,clientSeq:x}),first);
  assert.equal(writer.stats.pendingFields,3);
  await c.advance();await first;
  assert.deepEqual(calls,[{at:0,patch:{x:100,y:1,clientSeq:100}}]);
});

test('slow acknowledgements do not add another rate-limit interval', async()=>{
  const first=deferred(), calls=[], c=clock();
  const writer=createPatchWriter({...c,write:patch=>{calls.push({patch,at:c.now()});return calls.length===1?first.promise:Promise.resolve();}});
  const done=writer.enqueue({x:1});await c.advance();
  writer.enqueue({x:2});await c.advance(500);
  assert.equal(calls.length,1);
  first.resolve();await c.advance();await done;
  assert.deepEqual(calls.map(c=>c.at),[0,500]);
  assert.equal(calls[1].patch.x,2);
});

test('fast acknowledgements still honor the write budget', async()=>{
  const {writer,c,calls}=harness();writer.enqueue({x:1});await c.advance();
  writer.enqueue({x:2});await c.advance(119);assert.equal(calls.length,1);
  await c.advance(1);assert.equal(calls.length,2);
});

test('identical gameplay payloads skip timestamp-only writes', async()=>{
  const {writer,c,calls}=harness();writer.enqueue({x:1,updatedAt:0,clientSeq:1});await c.advance();
  const done=writer.enqueue({x:1,updatedAt:500,clientSeq:2});await c.advance(500);await done;
  assert.equal(calls.length,1);assert.equal(writer.stats.skipped,1);
});

test('sends field deltas without resending stable fields', async()=>{
  const {writer,c,calls}=harness();writer.enqueue({x:1,y:2,facing:'east'});await c.advance();
  writer.enqueue({x:2,y:2,facing:'east',updatedAt:120});await c.advance(120);
  assert.deepEqual(calls[1].patch,{x:2,updatedAt:120});
});

test('retries transient failure without losing unrelated fields or newer data', async()=>{
  const first=deferred(), calls=[], c=clock();
  const writer=createPatchWriter({...c,write:patch=>{calls.push(patch);return calls.length===1?first.promise:Promise.resolve();}});
  const done=writer.enqueue({'cat.x':1,'cat.health':90});await c.advance();
  writer.enqueue({'cat.x':2,'cat.facing':'east'});
  first.reject({code:'firestore/unavailable'});await c.advance(499);assert.equal(calls.length,1);
  await c.advance(1);await done;
  assert.deepEqual(calls[1],{'cat.x':2,'cat.health':90,'cat.facing':'east'});
});

test('failed values are not recorded as successfully deduplicated', async()=>{
  const calls=[], c=clock();let fail=true;
  const writer=createPatchWriter({...c,write:async patch=>{calls.push(patch);if(fail){fail=false;throw {code:'unavailable'};}}});
  const done=writer.enqueue({health:7});await c.advance(500);await done;
  assert.equal(calls.length,2);assert.equal(writer.stats.skipped,0);
});

test('permanent failures reject and a later explicit update can retry', async()=>{
  const calls=[], errors=[], c=clock();let fail=true;
  const writer=createPatchWriter({...c,onError:e=>errors.push(e),write:async patch=>{calls.push(patch);if(fail)throw {code:'permission-denied'};}});
  const done=writer.enqueue({health:7});await c.advance();await assert.rejects(done,e=>e.code==='permission-denied');
  await c.advance(10000);assert.equal(calls.length,1);assert.equal(errors.length,1);
  fail=false;const retried=writer.enqueue({health:7});await c.advance();await retried;assert.equal(calls.length,2);
});

test('transient retries are bounded', async()=>{
  const {writer,c,calls}=harness({maxRetries:2,write:async()=>{throw {code:'unavailable'};}});
  const done=writer.enqueue({x:4});await c.advance(5000);await assert.rejects(done);
  assert.equal(writer.stats.writes,3);assert.equal(writer.stats.retries,2);
});

test('offline clients keep only latest pending state and resume explicitly', async()=>{
  let enabled=false;const {writer,c,calls}=harness({enabled:()=>enabled});
  const done=writer.enqueue({x:1});for(let x=2;x<=1000;x++)writer.enqueue({x});
  await c.advance(60000);assert.equal(calls.length,0);assert.equal(writer.stats.pendingFields,1);
  enabled=true;writer.resume();await c.advance();await done;
  assert.deepEqual(calls[0].patch,{x:1000});
});

test('losing ownership cancels unsent work, including retry timers', async()=>{
  const {writer,c,calls}=harness();const done=writer.enqueue({hostHeartbeatAt:1});
  writer.cancel();await c.advance(10000);assert.equal(await done,false);assert.equal(calls.length,0);
});

test('old in-flight completion cannot restore cancelled work or overlap a new write', async()=>{
  const first=deferred(), calls=[], c=clock();
  const writer=createPatchWriter({...c,write:patch=>{calls.push(patch);return calls.length===1?first.promise:Promise.resolve();}});
  const old=writer.enqueue({x:1});await c.advance();writer.enqueue({x:2});writer.cancel();assert.equal(await old,false);
  const fresh=writer.enqueue({x:3});await c.advance(500);assert.equal(calls.length,1);
  first.reject({code:'unavailable'});await c.advance();await fresh;
  assert.deepEqual(calls,[{x:1},{x:3}]);
});

test('snapshots invalidate confirmed values after another writer changes them', async()=>{
  const {writer,c,calls}=harness();writer.enqueue({x:1});await c.advance();
  writer.observe({x:2});writer.enqueue({x:1});await c.advance(120);assert.equal(calls.length,2);
});

test('nested state patches preserve sibling maps and capture enqueue-time values',()=>{
  const data={cat:{x:3,health:10},objectives:{supplyDrop:{active:false}},updatedAt:1};
  const patch=stateFieldPatch(data);data.objectives.supplyDrop.active=true;
  assert.deepEqual(patch,{'cat.x':3,'cat.health':10,'objectives.supplyDrop':{active:false},updatedAt:1});
});

test('host election excludes stale owner, ignores offline players, and breaks join-time ties',()=>{
  const players={host:{joinedAt:0,online:true},z:{joinedAt:1,online:true},a:{joinedAt:1,online:true},b:{joinedAt:0,online:false}};
  assert.equal(eligibleHostCandidate(players,p=>p.online,'host'),'a');
  assert.equal(eligibleHostCandidate({},()=>true),null);
});

test('unchanged structures reuse nodes; damaged and deleted structures update independently',()=>{
  const cache=new Map(), layer={children:[],appendChild(node){this.children.push(node);node.parentNode=this;}};let created=0;
  const create=()=>{created++;return {remove(){const l=this.parentNode;l.children.splice(l.children.indexOf(this),1);this.parentNode=null;}};};
  const render=entries=>reconcileKeyedNodes(cache,layer,entries,e=>e.id,e=>JSON.stringify(e),create);
  render([{id:1,hp:10},{id:2,hp:20}]);const first=layer.children[0];
  for(let i=0;i<100;i++)render([{id:1,hp:10},{id:2,hp:20}]);
  assert.equal(created,2);assert.equal(layer.children[0],first);
  render([{id:1,hp:10},{id:2,hp:19}]);assert.equal(created,3);assert.equal(layer.children[0],first);
  render([{id:2,hp:19}]);assert.equal(cache.size,1);assert.equal(layer.children.length,1);
});
