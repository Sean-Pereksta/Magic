import test from 'node:test';
import assert from 'node:assert/strict';
import {createCoopStore,GAME_TYPE} from './sync.mjs';
import {rules,command} from './test-fixtures.mjs';

function database() {
  const rows=new Map(),listeners=new Map();let moment=100000,attempts=0,offline=false;
  const pack=v=>JSON.parse(JSON.stringify(v));
  const hydrate=v=>v && typeof v==='object' ? v.__ms!=null ? {toMillis:()=>v.__ms} : Array.isArray(v) ? v.map(hydrate) : Object.fromEntries(Object.entries(v).map(([k,x])=>[k,hydrate(x)])) : v;
  const stamp=v=>v&&typeof v==='object'?v.__timestamp?{__ms:moment}:Array.isArray(v)?v.map(stamp):Object.fromEntries(Object.entries(v).map(([k,x])=>[k,stamp(x)])):v;
  const snap=path=>({exists:()=>rows.has(path),data:()=>hydrate(pack(rows.get(path)?.data || null)),metadata:{fromCache:false,hasPendingWrites:false}});
  function write(path,data) {rows.set(path,{version:(rows.get(path)?.version||0)+1,data:stamp(pack(data))});for(const cb of listeners.get(path)||[])queueMicrotask(()=>cb(snap(path)));}
  const api={
    doc:(_db,...parts)=>parts.join('/'),serverTimestamp:()=>({__timestamp:true}),
    setDoc:async(ref,data)=>{if(offline)throw new Error('Offline');write(ref,data);},
    onSnapshot:(ref,opts,cb)=>{const set=listeners.get(ref)||new Set();set.add(cb);listeners.set(ref,set);queueMicrotask(()=>cb(snap(ref)));return()=>set.delete(cb);},
    runTransaction:async(_db,fn)=>{
      if(offline)throw new Error('Offline');
      for(let retry=0;retry<20;retry++){
        attempts++;const versions=new Map(),writes=[];
        const tx={get:async path=>{assert.equal(writes.length,0,'all reads precede writes');versions.set(path,rows.get(path)?.version||0);const result=snap(path);await Promise.resolve();return result;},set:(path,data)=>writes.push([path,data])};
        const result=await fn(tx);
        if([...versions].some(([path,v])=>(rows.get(path)?.version||0)!==v))continue;
        for(const [path,data]of writes)write(path,data);return result;
      }
      throw new Error('Retry budget exhausted');
    }
  };
  write('lobbies/test',{gameType:GAME_TYPE,status:'started',players:['Sean','Alex'],host:'Sean'});
  return{api,write,get:path=>snap(path).data(),set moment(v){moment=v},get attempts(){return attempts},set offline(v){offline=v}};
}
async function party(t) {
  const db=database();let id=0;
  const store=name=>createCoopStore({api:db.api,db:{},gameId:'test',username:name,uid:`uid-${name}`,rules,newRun:()=>({id:`run-${++id}`,seed:5,now:100000}),now:()=>200000});
  const sean=store('Sean'),alex=store('Alex');t.after(()=>{sean.close();alex.close()});
  await Promise.all([sean.connect(()=>{},()=>{}),alex.connect(()=>{},()=>{})]);
  return{db,sean,alex,state:()=>db.get('lobbies/test/roguelike/run')};
}

test('simultaneous initialization creates one shared run and binds both sessions',async t=>{
  const {state,db}=await party(t);const s=state();assert.equal(s.players.length,2);
  assert.equal(s.players[0].uid,'uid-Sean');assert.equal(s.players[1].uid,'uid-Alex');assert.ok(db.attempts>=3);
});

test('simultaneous identical verses have exactly one winner and never double counterattack',async t=>{
  const {state,db,sean,alex}=await party(t);let s=state();s.enemy.hp=s.enemy.maxHp=1000;s.enemy.weaknesses=[{concept:'love',multiplier:1.3}];db.write('lobbies/test/roguelike/run',s);
  const results=await Promise.allSettled([sean.dispatch(command(s,'attack',{reference:'John 3:16'})),alex.dispatch(command(s,'attack',{reference:'John 3:16',playerId:'p1'}))]);
  assert.equal(results.filter(r=>r.status==='fulfilled').length,1);assert.match(results.find(r=>r.status==='rejected').reason.message,/used by/);
  assert.equal(Object.keys(state().used).length,1);assert.equal(state().enemy.turn,1);assert.equal(state().players.filter(p=>p.hp<100).length,0);
});

test('different concurrent verses both preserve damage and personal HP updates',async t=>{
  const {state,db,sean,alex}=await party(t);let s=state();s.enemy.hp=s.enemy.maxHp=1000;s.enemy.weaknesses=[{concept:'love',multiplier:1.3}];db.write('lobbies/test/roguelike/run',s);
  await Promise.all([sean.dispatch(command(s,'attack',{reference:'John 3:16'})),alex.dispatch(command(s,'attack',{reference:'John 8:32',playerId:'p1'}))]);
  const after=state(),damage=after.events.filter(e=>e.reference && e.target==='enemy').reduce((n,e)=>n+e.amount,0);
  assert.equal(after.enemy.hp,1000-damage);assert.equal(Object.keys(after.used).length,2);assert.equal(after.enemy.turn,2);assert.equal(after.revision,2);
  assert.equal(after.players[0].hp,100);assert.ok(after.players[1].hp<100);
  const answers=after.events.filter(e=>e.effect==='answer');assert.equal(answers.length,2);
  assert.equal(answers.find(e=>e.playerId==='p0').correct,true);assert.equal(answers.find(e=>e.playerId==='p1').correct,false);
});

test('concurrent finishing blows pay one reward; replayed action stays idempotent',async t=>{
  const {state,db,sean,alex}=await party(t);let s=state();s.enemy.hp=1;db.write('lobbies/test/roguelike/run',s);
  const a=command(s,'attack',{reference:'John 3:16'}),b=command(s,'attack',{reference:'John 8:32',playerId:'p1'});
  const result=await Promise.allSettled([sean.dispatch(a),alex.dispatch(b)]);
  assert.equal(result.filter(r=>r.status==='fulfilled').length,1);assert.equal(state().phase,'path');assert.equal(state().players[0].gold,40);assert.equal(state().players[1].gold,40);
  const winning=result[0].status==='fulfilled'?{store:sean,action:a}:{store:alex,action:b};const before=state();
  await winning.store.dispatch(winning.action);assert.deepEqual(state(),before);
});

test('stale purchases, double room rewards and double path choices serialize safely',async t=>{
  const {state,db,sean,alex}=await party(t);let s=state();s.phase='path';s.paths=['treasure','shop'];db.write('lobbies/test/roguelike/run',s);
  const picks=await Promise.allSettled([sean.dispatch(command(s,'path',{path:'treasure'})),alex.dispatch(command(s,'path',{path:'shop',playerId:'p1'}))]);
  assert.equal(picks.filter(r=>r.status==='fulfilled').length,1);assert.equal(state().floor,2);
  if(state().phase==='treasure'){
    s=state();const results=await Promise.allSettled([sean.dispatch(command(s,'claim',{choice:'gold'})),sean.dispatch(command(s,'claim',{choice:'gold'}))]);
    assert.equal(results.filter(r=>r.status==='fulfilled').length,1);assert.equal(state().players[0].gold,57);
  }
});

test('host departure does not block room continuation and rejoining preserves a build',async t=>{
  const {state,db,sean,alex}=await party(t);let s=state();s.phase='path';s.paths=['shop'];db.write('lobbies/test/roguelike/run',s);
  await alex.dispatch(command(s,'path',{playerId:'p1',path:'shop'}));
  sean.close();await Promise.resolve();s=state();
  await alex.dispatch(command(s,'continue',{playerId:'p1'}));assert.equal(state().phase,'path');
  const reconnect=createCoopStore({api:db.api,db:{},gameId:'test',username:'Sean',uid:'uid-Sean',rules,newRun:()=>({id:'unused',seed:3,now:100001})});t.after(()=>reconnect.close());
  await reconnect.connect(()=>{},()=>{});assert.equal(state().id,s.id);assert.equal(state().players[0].gold,s.players[0].gold);
});

test('offline errors never mutate local/shared state and wrong UID cannot claim a slot',async t=>{
  const {state,db,sean}=await party(t);const before=state();db.offline=true;
  await assert.rejects(sean.dispatch(command(before,'attack',{reference:'John 3:16'})),/Offline/);assert.deepEqual(state(),before);db.offline=false;
  const wrong=createCoopStore({api:db.api,db:{},gameId:'test',username:'Sean',uid:'another-session',rules,newRun:()=>({id:'unused',seed:3})});t.after(()=>wrong.close());
  await assert.rejects(wrong.connect(()=>{},()=>{}),/another signed-in/);
});

test('simultaneous rematch requests start only one new run with the same roster and bindings',async t=>{
  const {state,db,sean,alex}=await party(t);const old=state();old.phase='ended';db.write('lobbies/test/roguelike/run',old);
  const results=await Promise.all([sean.restart(old.id),alex.restart(old.id)]);assert.equal(results[0].id,results[1].id);assert.notEqual(state().id,old.id);
  assert.equal(state().players[1].uid,'uid-Alex');assert.equal(state().players[1].hp,100);
});
