import test from 'node:test';
import assert from 'node:assert/strict';
import {newSave} from './core.mjs';
import {snapshotTeam} from './social-combat.mjs';
import {SocialController} from './social-controller.mjs';
import {SocialStore,createMemorySocialAdapter} from './social-store.mjs';

globalThis.document={hidden:false};
const settle=async predicate=>{for(let i=0;i<100;i++){if(predicate())return;await new Promise(resolve=>setImmediate(resolve));}assert.ok(predicate(),'The live crew model did not settle.');};
async function fixture(t){
 const state=newSave('Alice Trail','capuchin','gps',123),other=newSave('Bob Trail','langur','gps',456),now=1800000000000,base=createMemorySocialAdapter(),watches=new Map();
 const adapter={...base,watchDoc(path,callback,error){watches.set(path,(watches.get(path)||0)+1);const stop=base.watchDoc(path,callback,error);return()=>{watches.set(path,watches.get(path)-1);stop();};}};
 const bob=new SocialStore(adapter,{uid:'bob',now:()=>now,id:()=>crypto.randomUUID()});await bob.register({saveId:'bob-save',name:other.name,team:snapshotTeam(other)});
 const controller=new SocialController({getState:()=>state,getVault:()=>({id:'alice-save'}),persist:async()=>{},connect:async()=>({uid:'alice',adapter}),now:()=>now});
 await controller.start();t.after(()=>controller.stop());assert.equal(controller.model.status,'ready');
 return{controller,state,other,bob,adapter,watches,now};
}

test('tribe starting crews update from live player snapshots with one listener per member',async t=>{
 const f=await fixture(t),tribe=await f.controller.store.createTribe({name:'Canopy Friends'});await f.bob.joinTribe(tribe.code);
 await settle(()=>f.controller.model.members.some(p=>p.uid==='bob'));
 assert.equal(f.watches.get('players/bob'),1);
 f.other.run.monkey.level=12;await f.bob.register({saveId:'bob-save',name:'Bob Trail',team:snapshotTeam(f.other)});
 await settle(()=>f.controller.model.members.find(p=>p.uid==='bob')?.team[0].level===12);
 assert.equal(f.controller.model.members.find(p=>p.uid==='bob').team[0].species,'langur');
 assert.equal(f.watches.get('players/bob'),1);
 await f.bob.leaveTribe();await settle(()=>!f.controller.model.members.some(p=>p.uid==='bob'));
 assert.equal(f.watches.get('players/bob'),0);
});

test('nearby discovery loads eligible crew profiles and unsubscribes when a player leaves range',async t=>{
 const f=await fixture(t),point={lat:40,lon:-74,accuracy:80,at:f.now};
 f.controller.discoverable=true;f.controller.latestLocation=point;
 await f.bob.heartbeat({discoverable:true,location:point});await f.controller.heartbeat(true);
 await settle(()=>f.controller.model.nearby.find(p=>p.uid==='bob')?.team?.length===1);
 assert.equal(f.controller.model.nearby[0].team[0].species,'langur');assert.equal(f.watches.get('players/bob'),1);
 f.other.run.monkey.level=8;await f.bob.register({saveId:'bob-save',name:'Bob Trail',team:snapshotTeam(f.other)});
 await settle(()=>f.controller.model.nearby[0]?.team[0].level===8);
 await f.bob.heartbeat({discoverable:true,location:{...point,lat:45}});
 await settle(()=>f.controller.model.nearby.length===0);assert.equal(f.watches.get('players/bob'),0);
 await f.controller.stop();assert.equal([...f.watches.values()].reduce((a,b)=>a+b,0),0);
});

test('crew edits publish immediately while unchanged and locked battle crews are not rewritten',async t=>{
 const f=await fixture(t),first=await f.controller.store.getPlayer();
 await f.controller.heartbeat();assert.equal((await f.controller.store.getPlayer()).version,first.version);
 f.state.run.monkey.level=7;await f.controller.heartbeat();
 const upgraded=await f.controller.store.getPlayer();assert.equal(upgraded.team[0].level,7);
 await f.controller.heartbeat(true);assert.equal((await f.controller.store.getPlayer()).version,upgraded.version,'Presence-only refresh should not rewrite the crew.');
 f.controller.model.raid={status:'active'};f.state.run.monkey.level=9;await f.controller.heartbeat(true);
 assert.deepEqual((await f.controller.store.getPlayer()).team,upgraded.team,'A battle keeps its published starting crew.');
 f.controller.model.raid=null;await f.controller.heartbeat();assert.equal((await f.controller.store.getPlayer()).team[0].level,9);
});

test('overlapping heartbeats preserve the latest crew edit without concurrent profile writes',async t=>{
 const f=await fixture(t),original=f.controller.store.heartbeat.bind(f.controller.store);let release,entered,active=0,maxActive=0;
 const enteredPromise=new Promise(resolve=>entered=resolve),blocked=new Promise(resolve=>release=resolve);
 f.controller.store.heartbeat=async options=>{active++;maxActive=Math.max(maxActive,active);if(options.team?.[0].level===3){entered();await blocked;}try{return await original(options);}finally{active--;}};
 f.state.run.monkey.level=3;const first=f.controller.heartbeat();await enteredPromise;
 f.state.run.monkey.level=10;void f.controller.heartbeat();release();await first;
 await settle(()=>f.controller.model.me?.team[0].level===10);
 assert.equal(maxActive,1);assert.equal((await f.controller.store.getPlayer()).team[0].level,10);
});

test('a concurrent battle lock cannot acknowledge a skipped crew update after returning',async t=>{
 const f=await fixture(t),store=f.controller.store,read=store.getPlayer.bind(store);let lock=true;
 store.getPlayer=async(...args)=>{const p=await read(...args);if(lock){lock=false;await f.adapter.transaction(async tx=>{const current=await tx.get('players/alice');current.activeRaid='concurrent-raid';tx.set('players/alice',current);});}return p;};
 f.state.run.monkey.level=11;await f.controller.heartbeat();
 await settle(()=>f.controller.model.me?.activeRaid==='concurrent-raid');
 assert.equal((await read()).team[0].level,1,'The concurrent battle lock must keep the original published crew.');
 await f.adapter.transaction(async tx=>{const current=await tx.get('players/alice');current.activeRaid=null;tx.set('players/alice',current);});
 await settle(()=>!f.controller.model.me?.activeRaid);
 await f.controller.heartbeat();
 assert.equal((await read()).team[0].level,11,'Returning must publish the unacknowledged crew even inside the presence throttle.');
});

test('Firestore map key ordering does not cause unchanged crews to be republished',async t=>{
 const f=await fixture(t),reorder=value=>Array.isArray(value)?value.map(reorder):value&&typeof value==='object'?Object.fromEntries(Object.entries(value).reverse().map(([key,item])=>[key,reorder(item)])):value;
 await f.adapter.transaction(async tx=>{const p=await tx.get('players/alice');p.team=reorder(p.team);p.version++;tx.set('players/alice',p);});
 const expected=(await f.controller.store.getPlayer()).version;await settle(()=>f.controller.model.me?.version===expected);
 await f.controller.heartbeat(true);assert.equal((await f.controller.store.getPlayer()).version,expected);
});

test('tribe leaderboard loads on connection and refreshes public summaries without crew listeners',async t=>{
 const f=await fixture(t);await settle(()=>f.controller.model.rankingsStatus==='ready');assert.deepEqual(f.controller.model.tribeRankings,[]);
 await f.adapter.transaction(async tx=>{tx.set('tribes/LEADERS',{id:'LEADERS',name:'Canopy leaders',members:['unrelated-private-player'],boss:{level:8}});});
 await f.controller.refreshTribeRankings();assert.equal(f.controller.model.tribeRankings[0].bossesDefeated,7);assert.equal(f.controller.model.rankingsUpdatedAt,f.now);
 assert.equal(f.watches.has('players/unrelated-private-player'),false);
 const saved=structuredClone(f.controller.model.tribeRankings),read=f.controller.store.listTribeRankings;
 f.controller.store.listTribeRankings=async()=>{throw Error('Offline fixture');};await f.controller.refreshTribeRankings();
 assert.equal(f.controller.model.status,'ready');assert.equal(f.controller.model.rankingsStatus,'error');assert.match(f.controller.model.rankingsError,/Offline fixture/);assert.deepEqual(f.controller.model.tribeRankings,saved);
 f.controller.store.listTribeRankings=read;await f.controller.refreshTribeRankings();assert.equal(f.controller.model.rankingsStatus,'ready');assert.equal(f.controller.model.rankingsError,'');
});

test('leaderboard refreshes coalesce and a disconnected request cannot repopulate the next session',async t=>{
 const f=await fixture(t);await settle(()=>f.controller.model.rankingsStatus==='ready');let release,calls=0;
 f.controller.store.listTribeRankings=()=>{calls++;return new Promise(resolve=>release=resolve);};
 const first=f.controller.refreshTribeRankings(),second=f.controller.refreshTribeRankings();assert.equal(calls,1);assert.equal(f.controller.model.rankingsStatus,'loading');
 await f.controller.stop();release([{id:'STALE',bossLevel:99}]);await Promise.all([first,second]);
 assert.deepEqual(f.controller.model.tribeRankings,[]);assert.equal(f.controller.model.rankingsStatus,'idle');assert.equal(f.controller.model.rankingsUpdatedAt,null);
});
