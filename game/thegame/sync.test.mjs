import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {webcrypto} from 'node:crypto';
import {revisionOf,sameVersion,decorateCommit} from './sync.mjs';

const html=readFileSync(new URL('../thegame.html',import.meta.url),'utf8');
const source=html.match(/<script type="module">([\s\S]*?)<\/script>/)[1]
  .replace(/import\s+[\s\S]*?\s+from\s+"[^"]+";/g,'').split('$optClassic.addEventListener')[0];
function element() {return {style:{setProperty(){}},dataset:{},classList:{add(){},remove(){},toggle(){}},appendChild(){},append(){},setAttribute(){},addEventListener(){},querySelector:element,children:[],animate(){return {}},textContent:''};}
function database(state) {
  let data=structuredClone(state),queue=Promise.resolve(),writes=0;
  return {get data(){return structuredClone(data)},get writes(){return writes},
    async run(db,cb) {
      const task=queue.then(async()=>{
        const before=structuredClone(data);let next;
        await cb({get:async()=>({exists:()=>Boolean(before),data:()=>structuredClone(before)}),
          update:(ref,patch)=>{next={...before,...patch}},set:(ref,value)=>{next=value}});
        if(next){data=structuredClone(next);writes++;}
      });
      queue=task.catch(()=>{});return task;
    }
  };
}
function client(db,name='A',initial=db.data) {
  const notices=[],listeners=[];
  const context=vm.createContext({console,structuredClone,URLSearchParams,Date,Math,Number,Object,Array,String,Boolean,JSON,crypto:webcrypto,
    revisionOf,sameVersion,decorateCommit,initializeApp:()=>({}),getFirestore:()=>({}),getAuth:()=>({}),
    doc:(...parts)=>parts.slice(1).join('/'),getDoc:async()=>({exists:()=>true,data:()=>({host:'A',players:['A','B']})}),
    runTransaction:db.run.bind(db),serverTimestamp:()=>123,
    onSnapshot:(ref,options,success,error)=>{const l={success,error,closed:false};listeners.push(l);return ()=>{l.closed=true}},
    navigator:{onLine:true},location:{search:`?gameId=test&username=${name}`},window:{addEventListener(){}},
    document:{createElement:element,createTextNode:x=>x,getElementById:element,querySelector:element,hidden:false},
    matchMedia:()=>({matches:true}),setTimeout:()=>0,clearTimeout(){},alert(){}});
  vm.runInContext(source,context);
  context.initial=initial;context.notices=notices;
  vm.runInContext(`G=initial;serverReady=true;toast=(message)=>notices.push(message);renderAll=()=>{};renderControls=()=>{};renderConnecting=()=>{};closePowersSheet=()=>{};animateMatchChange=()=>{};`,context);
  return {context,notices,listeners,run:code=>vm.runInContext(code,context),refresh(){context.fresh=db.data;vm.runInContext('G=fresh;serverReady=true;',context)}};
}
function match(overrides={}) {
  return {revision:5,matchId:'match',updatedAt:1,phase:'game2',mode:'game2',host:'A',players:['A','B'],turnIndex:0,turnNum:1,turnPlays:0,
    deck:[80,90],hands:{A:[20,30,'FLIP','BUNGEE','SWAP','TIMEWARP'],B:[40]},piles:{U1:{id:'U1',dir:'up',top:1,effects:[]},U2:{id:'U2',dir:'up',top:1,effects:[]},D1:{id:'D1',dir:'down',top:120,effects:[]},D2:{id:'D2',dir:'down',top:120,effects:[]}},debts:{},log:[],...overrides};
}
test('two tabs submitting the same card commit once',async()=>{
  const db=database(match()),a=client(db),b=client(db);
  await Promise.all([a.run('playNumber(20,"U1")'),b.run('playNumber(20,"U1")')]);
  assert.equal(db.writes,1);assert.equal(db.data.turnPlays,1);assert.equal(db.data.revision,6);
  assert.match(b.notices[0],/match changed/);
});
test('stale different action cannot consume a second copy of a power',async()=>{
  const db=database(match({hands:{A:['FLIP','FLIP'],B:[40]}})),a=client(db),b=client(db);
  await a.run('playFlip("U1")');await b.run('playFlip("U1")');
  assert.equal(db.writes,1);assert.equal(db.data.piles.U1.dir,'down');assert.deepEqual(db.data.hands.A,['FLIP']);
});
test('refresh restores the committed hand and allows the next action',async()=>{
  const db=database(match());await client(db).run('playNumber(20,"U1")');
  await client(db).run('playNumber(30,"U1")');assert.equal(db.writes,2);assert.equal(db.data.piles.U1.top,30);
});
test('offline input does not write or queue a move',async()=>{
  const db=database(match()),a=client(db);await a.run('navigator.onLine=false;playNumber(20,"U1")');assert.equal(db.writes,0);
  a.run('navigator.onLine=true;serverReady=false;');await a.run('playNumber(20,"U1")');assert.equal(db.writes,0);
});
test('all powers and turn ending increment revision through the shared guard',async()=>{
  for (const action of ['playFlip("U1")','playBungee("U1")','playSwap("U1","D1")','playTrade("B")','playTimeWarp()','endTurn()']) {
    const db=database(match({turnPlays:2,hands:{A:[20,'FLIP','BUNGEE','SWAP','TRADE','TIMEWARP'],B:[40]}}));
    await client(db).run(action);assert.equal(db.data.revision,6,action);assert.equal(db.writes,1,action);
  }
});
test('noncurrent player and invalid same-stack swap do not write',async()=>{
  const db=database(match());await client(db,'B').run('playNumber(40,"U1")');await client(db).run('playSwap("U1","U1")');assert.equal(db.writes,0);
});
test('last power card resolves victory for every viewer',async()=>{
  const db=database(match({deck:[],hands:{A:['FLIP'],B:[]}}));await client(db).run('playFlip("U1")');assert.equal(db.data.phase,'won');
});
test('forward twenty does not consume a Bungee charge',async()=>{
  const initial=match({hands:{A:[21],B:[40]}});initial.piles.U1.effects=[{type:'bungee',charges:1}];
  const db=database(initial);await client(db).run('playNumber(21,"U1")');assert.equal(db.data.piles.U1.effects[0].charges,1);
});
test('empty hands are skipped after the deck is exhausted',async()=>{
  const db=database(match({deck:[],hands:{A:[30],B:[]},turnPlays:1}));await client(db).run('endTurn()');assert.equal(db.data.turnIndex,0);
});
test('duplicate restart cannot overwrite the newly opened vote',async()=>{
  const db=database(match({phase:'won'})),a=client(db),b=client(db);await Promise.all([a.run('restartGame()'),b.run('restartGame()')]);
  assert.equal(db.writes,1);assert.equal(db.data.phase,'voting');assert.equal(db.data.revision,6);assert.notEqual(db.data.matchId,'match');
});
test('restart rejects active match even for the host',async()=>{
  const db=database(match());await client(db).run('restartGame()');assert.equal(db.writes,0);
});
test('concurrent votes merge and resolve only once',async()=>{
  const db=database(match({phase:'voting',votes:{},deadline:Date.now()+20000})),a=client(db),b=client(db,'B');
  await Promise.all([a.run('castVote("classic")'),b.run('castVote("classic")')]);
  assert.equal(db.data.phase,'game');assert.equal(db.data.revision,8);assert.equal(db.data.players.length,2);
});
test('room creation does not replace an existing match',async()=>{
  const db=database(match());await client(db).run('ensureRoom()');assert.equal(db.writes,0);
});
test('cached, pending and older snapshots cannot replace confirmed state',()=>{
  const db=database(match()),a=client(db);a.run('startSubscriptions()');const listen=a.listeners.at(-1);
  const emit=(data,fromCache=false,hasPendingWrites=false)=>listen.success({exists:()=>true,data:()=>data,metadata:{fromCache,hasPendingWrites}});
  emit(match({revision:8,updatedAt:8}));emit(match({revision:6}),true);assert.equal(a.run('G.revision'),8);assert.equal(a.run('serverReady'),false);
  emit(match({revision:9}),false,true);assert.equal(a.run('G.revision'),8);
  emit(match({revision:7}));assert.equal(a.run('G.revision'),8);
  emit(match({revision:9,updatedAt:9}));assert.equal(a.run('G.revision'),9);assert.equal(a.run('serverReady'),true);
  a.run('startSubscriptions()');assert.equal(listen.closed,true);assert.equal(a.listeners.filter(l=>!l.closed).length,1);
});
test('listener error stops writes until retry and confirmed snapshot',async()=>{
  const db=database(match()),a=client(db);a.run('startSubscriptions()');a.listeners[0].error(new Error('permission denied'));
  await a.run('playNumber(20,"U1")');assert.equal(db.writes,0);assert.equal(a.run('listenerFailed'),true);
});
