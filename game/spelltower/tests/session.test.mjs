import test from 'node:test';
import assert from 'node:assert/strict';
import {createGame, getActivePlayer, applyAction, SPELLS} from '../engine.mjs';
import {connectRoom, saveRun, loadRun, makeLocalNames, SAVE_KEY, INACTIVE_TURN_MS} from '../session.mjs';

const clone = value => value === undefined ? undefined : JSON.parse(JSON.stringify(value));
const ROOM = 'lobbies/test-room';
const STATE = `${ROOM}/spelltower/state`;

// Transactions are serialized; callbacks can be retried before committing.
// Listeners see committed copies, never the mutable transaction data. This tests
// the actual adapter and engine together without writing to a public match.
function firestore(initial = {}) {
  const documents = new Map(Object.entries(initial).map(([key,value])=>[key,clone(value)]));
  const watchers = new Map();
  const commits = [];
  let tail = Promise.resolve(), callbacks = 0, pending = 0, retries = 0;
  const snapshot = (key, metadata = {}) => {
    const data = clone(documents.get(key));
    return {exists:()=>data !== undefined, data:()=>clone(data), metadata:{fromCache:false,hasPendingWrites:false,...metadata}};
  };
  function schedule(listener, snap) {
    pending++;
    queueMicrotask(()=>{try {listener.next(snap);} catch(error) {listener.error?.(error);} finally {pending--;}});
  }
  function emit(key, metadata) {
    const snap = snapshot(key,metadata);
    for (const listener of watchers.get(key) || []) schedule(listener,snap);
  }
  const sdk = {
    doc:(_db,...parts)=>parts.join('/'),
    onSnapshot(key, options, next, error) {
      const listener = typeof options === 'function' ? {next:options,error:next} : {next,error};
      if (!watchers.has(key)) watchers.set(key,new Set());
      watchers.get(key).add(listener);
      schedule(listener,snapshot(key));
      return ()=>watchers.get(key)?.delete(listener);
    },
    runTransaction(_db, callback) {
      const transaction = tail.then(async()=>{
        pending++;
        try {
          let result, writes;
          do {
            writes = [];
            const tx = {
              get:async key=>snapshot(key),
              set:(key,value)=>writes.push({key,value:clone(value),operation:'set'}),
              update:(key,value)=>writes.push({key,value:clone(value),operation:'update'}),
              delete:key=>writes.push({key,operation:'delete'})
            };
            callbacks++;
            result = await callback(tx);
            if (!retries) break;
            retries--;
          } while (true);
          for (const {key,value,operation} of writes) {
            if (operation === 'delete') documents.delete(key);
            else documents.set(key,operation === 'update' ? {...documents.get(key),...value} : value);
          }
          if (writes.length) {
            commits.push(clone(writes));
            for (const key of new Set(writes.map(write=>write.key))) emit(key);
          }
          return result;
        } finally {pending--;}
      });
      tail = transaction.catch(()=>{});
      return transaction;
    }
  };
  return {
    sdk, commits, watchers,
    get:key=>clone(documents.get(key)),
    put(key,value) {documents.set(key,clone(value));emit(key);},
    emit,
    retryNext(count = 1) {retries=count;},
    get callbacks(){return callbacks;},
    async settle() {
      for (let i=0;i<30;i++) {
        const previous = callbacks;
        await tail;
        await new Promise(resolve=>setImmediate(resolve));
        if (!pending && previous === callbacks) return;
      }
      throw new Error('Unexpected listener/transaction write loop.');
    }
  };
}

async function fixture(t, players = ['Aster','Bramble']) {
  const db = firestore({[ROOM]:{gameType:'spelltower',status:'started',players,host:players[0]}});
  const clock = {value:1000};
  const sessions = [];
  async function join(username) {
    const states=[],errors=[],connections=[];
    const session = await connectRoom({sdk:db.sdk,db:{},gameId:'test-room',username,
      now:()=>clock.value,onState:(game,metadata)=>states.push({game,metadata}),
      onError:error=>errors.push(error),onConnection:status=>connections.push(status)});
    sessions.push(session);
    await db.settle();
    return {session,states,errors,connections,get game(){return states.at(-1)?.game;}};
  }
  t.after(()=>sessions.forEach(session=>session.close()));
  return {db,clock,join};
}

test('two online clients share a single initialized match and committed casts', async t=>{
  const {db,join}=await fixture(t);
  const a=await join('Aster'),b=await join('Bramble');
  assert.equal(db.commits.length,1,'presence snapshots must not write back unchanged state');
  assert.deepEqual(a.game,b.game);
  const before=clone(a.game),card=a.game.hands.Aster.find(card=>card.key==='water');
  await a.session.dispatch({type:'cast',cardId:card.id,x:1,y:2},before.revision);
  await db.settle();
  assert.equal(a.game.revision,before.revision+1);
  assert.deepEqual(a.game,b.game);
  assert.equal(a.game.mana,before.mana-1);
  assert.ok(a.game.terrain['1,2'].wet>0);
  assert.equal(a.game.hands.Aster.find(item=>item.id===card.id).charges,card.charges-1);
  b.game.hp=1;
  assert.equal(db.get(STATE).game.hp,before.hp,'a rendered copy cannot mutate stored state');
  assert.equal(a.errors.length,0);
  assert.equal(b.errors.length,0);
});

test('wrong players and simultaneous stale actions cannot spend the shared turn twice', async t=>{
  const {db,join}=await fixture(t);
  const a=await join('Aster'),b=await join('Bramble');
  const before=clone(a.game);
  await assert.rejects(b.session.dispatch({type:'endTurn'},before.revision),/Wait for your turn/);
  const card=before.hands.Aster.find(card=>card.key==='water');
  const action={type:'cast',cardId:card.id,x:1,y:2};
  const results=await Promise.allSettled([
    a.session.dispatch(action,before.revision),a.session.dispatch(action,before.revision)
  ]);
  await db.settle();
  assert.equal(results.filter(result=>result.status==='fulfilled').length,1);
  assert.match(results.find(result=>result.status==='rejected').reason.message,/battlefield changed/);
  assert.equal(db.get(STATE).game.mana,before.mana-1);
  assert.equal(db.get(STATE).game.revision,before.revision+1);
});

test('transaction retries publish one result and no uncommitted visual events', async t=>{
  const {db,join}=await fixture(t);
  const a=await join('Aster');
  const before=clone(a.game),seen=a.states.length,commits=db.commits.length;
  db.retryNext(2);
  const card=before.hands.Aster.find(card=>card.key==='nature');
  await a.session.dispatch({type:'cast',cardId:card.id,x:2,y:3},before.revision);
  await db.settle();
  assert.equal(db.commits.length,commits+1);
  assert.equal(a.states.length,seen+1);
  assert.equal(a.game.revision,before.revision+1);
  assert.equal(a.game.mana,before.mana-1);
});

test('nonmembers, other game rooms and old schemas reject without replacing data', async t=>{
  const {db,join}=await fixture(t);
  await assert.rejects(join('Intruder'),/no longer in this room/);
  assert.equal(db.get(STATE),undefined);
  db.put(ROOM,{...db.get(ROOM),gameType:'other'});
  await assert.rejects(join('Aster'),/different game/);
  db.put(ROOM,{...db.get(ROOM),gameType:'spelltower'});
  db.put(STATE,{version:1,game:{saved:'legacy run'}});
  await assert.rejects(join('Aster'),/older Spell Tower version/);
  assert.deepEqual(db.get(STATE),{version:1,game:{saved:'legacy run'}});
});

test('a waiting room cannot initialize a match before the host starts it', async t=>{
  const {db,join}=await fixture(t);
  db.put(ROOM,{...db.get(ROOM),status:'waiting'});
  await assert.rejects(join('Aster'),/Wait for the host to start/);
  assert.equal(db.get(STATE),undefined);
  assert.equal(db.commits.length,0);
  db.put(ROOM,{...db.get(ROOM),status:'started'});
  const a=await join('Aster');
  assert.equal(a.game.status,'playing');
  assert.equal(db.commits.length,1);
});

test('late arrivals receive cards and enter the next complete party round', async t=>{
  const {db,join}=await fixture(t);
  const a=await join('Aster'),b=await join('Bramble');
  db.put(ROOM,{...db.get(ROOM),players:['Aster','Bramble','Clover']});
  const c=await join('Clover');
  await db.settle();
  assert.deepEqual(a.game.players,['Aster','Bramble','Clover']);
  assert.ok(c.game.hands.Clover.length>=7);
  assert.equal(getActivePlayer(c.game),'Aster');
  assert.equal(c.game.pendingPlayers.includes('Clover'),false);
  await a.session.dispatch({type:'endTurn'},a.game.revision);await db.settle();
  await b.session.dispatch({type:'endTurn'},b.game.revision);await db.settle();
  assert.equal(c.game.round,2);
  assert.ok(c.game.pendingPlayers.includes('Clover'));
  await a.session.dispatch({type:'endTurn'},a.game.revision);await db.settle();
  await b.session.dispatch({type:'endTurn'},b.game.revision);await db.settle();
  assert.equal(getActivePlayer(c.game),'Clover');
});

test('leaving the current host transfers the turn and ownership; the last player closes the room', async t=>{
  const {db,join}=await fixture(t);
  const a=await join('Aster'),b=await join('Bramble');
  await a.session.leave();await db.settle();
  assert.deepEqual(db.get(ROOM).players,['Bramble']);
  assert.equal(db.get(ROOM).host,'Bramble');
  assert.equal(b.session.metadata.host,'Bramble');
  assert.deepEqual(b.game.players,['Bramble']);
  assert.equal(getActivePlayer(b.game),'Bramble');
  assert.equal(b.game.hands.Aster,undefined);
  await assert.rejects(a.session.dispatch({type:'endTurn'},b.game.revision),/connection has closed/);
  await b.session.leave();await db.settle();
  assert.equal(db.get(ROOM),undefined);
  assert.equal(db.get(STATE),undefined);
  assert.equal([...db.watchers.values()].reduce((n,listeners)=>n+listeners.size,0),0);
});

test('lobby membership changes reconcile a missing host without another player getting stuck', async t=>{
  const {db,join}=await fixture(t,['Aster','Bramble','Clover']);
  const a=await join('Aster'),b=await join('Bramble');
  db.put(ROOM,{...db.get(ROOM),players:['Bramble','Clover'],host:'Aster'});
  await db.settle();
  assert.deepEqual(b.game.players,['Bramble','Clover']);
  assert.equal(getActivePlayer(b.game),'Bramble');
  assert.equal(b.session.metadata.host,'Bramble');
  await assert.rejects(a.session.dispatch({type:'endTurn'},b.game.revision),/no longer in this room/);
  assert.ok(a.errors.some(error=>/left this room/.test(error.message)));
});

test('only the current host restarts and revisions invalidate an old queued action', async t=>{
  const {db,join,clock}=await fixture(t);
  const a=await join('Aster'),b=await join('Bramble');
  const before=clone(a.game);
  await assert.rejects(b.session.restart(),/host can start/);
  clock.value+=1000;
  await a.session.restart();await db.settle();
  assert.equal(a.game.revision,before.revision+1);
  assert.equal(a.game.round,1);
  assert.deepEqual(a.game,b.game);
  await assert.rejects(a.session.dispatch({type:'endTurn'},before.revision),/battlefield changed/);
  await a.session.leave();await db.settle();
  await b.session.restart();await db.settle();
  assert.equal(getActivePlayer(b.game),'Bramble');
  assert.deepEqual(b.game.players,['Bramble']);
});

test('the host may pass another inactive turn only after 90 seconds without a move', async t=>{
  const {db,join,clock}=await fixture(t);
  const a=await join('Aster'),b=await join('Bramble');
  await a.session.dispatch({type:'endTurn'},a.game.revision);await db.settle();
  assert.equal(getActivePlayer(a.game),'Bramble');
  await assert.rejects(b.session.skipInactive(),/Only the host/);
  clock.value+=INACTIVE_TURN_MS-1;
  await assert.rejects(a.session.skipInactive(),/Wait 90 seconds/);
  clock.value++;
  await a.session.skipInactive();await db.settle();
  assert.equal(getActivePlayer(b.game),'Aster');
  clock.value+=INACTIVE_TURN_MS;
  await assert.rejects(a.session.skipInactive(),/End turn to pass your own turn/);
});

test('pending local snapshots are ignored, reconnection renders the committed board, and close unsubscribes', async t=>{
  const {db,join}=await fixture(t);
  const a=await join('Aster');
  const before=a.states.length;
  db.emit(STATE,{hasPendingWrites:true});await db.settle();
  assert.equal(a.states.length,before);
  db.emit(STATE,{fromCache:true});await db.settle();
  assert.equal(a.connections.at(-1),'Reconnecting…');
  assert.equal(a.states.at(-1).metadata.fromCache,true);
  db.emit(STATE);await db.settle();
  assert.equal(a.connections.at(-1),'Connected');
  assert.equal(a.states.at(-1).metadata.fromCache,false);
  const seen=a.states.length;
  a.session.close();
  db.emit(STATE);await db.settle();
  assert.equal(a.states.length,seen);
});

test('session commands cannot bypass lobby membership management', async t=>{
  const {join}=await fixture(t);
  const a=await join('Aster');
  await assert.rejects(a.session.dispatch({type:'join',player:'Clover'},a.game.revision),/Use the lobby/);
  await assert.rejects(a.session.dispatch({type:'leave',player:'Aster'},a.game.revision),/Use the lobby/);
});

function storage() {
  const values=new Map();
  return {values,getItem:key=>values.get(key)||null,setItem:(key,value)=>values.set(key,value),removeItem:key=>values.delete(key)};
}

test('solo and hot-seat games round-trip locally while online games never replace that save', () => {
  const local=storage();
  for (const [mode,players] of [['solo',['Aster']],['hotseat',['Aster','Bramble']]]) {
    const game=createGame({mode,players,seed:12});
    assert.equal(saveRun(local,game),true);
    assert.deepEqual(loadRun(local),game);
    assert.equal(saveRun(local,createGame({mode:'online',players,seed:34})),false);
    assert.deepEqual(loadRun(local),game);
  }
  assert.equal(saveRun(local,{...loadRun(local),status:'won'}),true);
  assert.equal(local.getItem(SAVE_KEY),null);
});

test('blocked, malformed, older and oversized local saves fail without interrupting play', () => {
  const blocked={getItem(){throw Error('blocked');},setItem(){throw Error('quota');},removeItem(){throw Error('blocked');}};
  const game=createGame({players:['Aster'],seed:9});
  assert.equal(saveRun(blocked,game),false);
  assert.equal(loadRun(blocked),null);
  const local=storage();
  const invalid=['not json','x'.repeat(500001),JSON.stringify({version:1,game}),JSON.stringify({version:2,game:{...game,activeIndex:3}}),JSON.stringify({version:2,game:{...game,players:['__proto__']}})];
  for (const content of invalid) {local.setItem(SAVE_KEY,content);assert.equal(loadRun(local),null);}
});

test('hot-seat names are unique and unsafe object keys cannot become player seats', () => {
  const names=makeLocalNames(4,['Aster','Aster','__proto__','']);
  assert.deepEqual(names,['Aster','Aster 2','Mage 3','Mage 4']);
  assert.equal(makeLocalNames(8).length,4);
  assert.deepEqual(makeLocalNames(0),['Mage 1']);
  assert.deepEqual(makeLocalNames(4,['Constructor','A\u007fB','__PROTO__','Aster']),['Mage 1','Mage 2','Mage 3','Aster']);
});

test('a deleted room can be left locally without recreating or writing any document', async t => {
  const {db,join}=await fixture(t);
  const a=await join('Aster');
  db.put(ROOM,undefined); await db.settle();
  const writes=db.commits.length, stored=db.get(STATE);
  await a.session.leave(); await db.settle();
  assert.equal(db.commits.length,writes);
  assert.deepEqual(db.get(STATE),stored);
  assert.equal(db.get(ROOM),undefined);
  assert.equal([...db.watchers.values()].reduce((count,listeners)=>count+listeners.size,0),0);
  await assert.rejects(a.session.dispatch({type:'endTurn'},stored.game.revision),/connection has closed/);
});

test('a player removed elsewhere can exit without touching the remaining party', async t => {
  const {db,join}=await fixture(t);
  const a=await join('Aster'),b=await join('Bramble');
  db.put(ROOM,{...db.get(ROOM),players:['Bramble'],host:'Bramble'}); await db.settle();
  const writes=db.commits.length, stored=db.get(STATE), lobby=db.get(ROOM);
  await a.session.leave(); await db.settle();
  assert.equal(db.commits.length,writes);
  assert.deepEqual(db.get(STATE),stored); assert.deepEqual(db.get(ROOM),lobby);
  assert.equal(getActivePlayer(b.game),'Bramble');
  await assert.rejects(a.session.dispatch({type:'endTurn'},stored.game.revision),/connection has closed/);
});

test('leaving also works when an existing party has returned to waiting', async t => {
  const {db,join}=await fixture(t);
  const a=await join('Aster');
  db.put(ROOM,{...db.get(ROOM),status:'waiting'}); await db.settle();
  await a.session.leave(); await db.settle();
  assert.deepEqual(db.get(ROOM).players,['Bramble']);
  assert.equal(db.get(ROOM).status,'waiting');
  assert.equal(db.get(ROOM).host,'Bramble');
});

test('invalid saved cards, simulation fields, enemies and terrain are rejected before rendering', () => {
  const local=storage(), original=createGame({players:['Aster'],seed:51});
  const corruptions=[
    ['engine version',g=>g.version=1], ['difficulty',g=>g.difficulty='unknown'],
    ['difficulty type',g=>g.difficulty=['normal']], ['missing pending players',g=>delete g.pendingPlayers],
    ['unknown pending player',g=>g.pendingPlayers=['Nobody']], ['empty pending players',g=>g.pendingPlayers=[]],
    ['unknown card',g=>g.hands.Aster[0].key='missing-spell'], ['array card key',g=>g.hands.Aster[0].key=['fire']],
    ['missing card id',g=>delete g.hands.Aster[0].id], ['duplicate card id',g=>g.hands.Aster[1].id=g.hands.Aster[0].id],
    ['negative charges',g=>g.hands.Aster[0].charges=-1], ['future cast marker',g=>g.hands.Aster[0].usedTurn=5],
    ['zero max health',g=>g.maxHP=0], ['dead playing run',g=>g.hp=0],
    ['negative mana',g=>g.mana=-1], ['too much mana',g=>g.mana=g.maxMana+1],
    ['invalid seed state',g=>g.rng=null], ['fractional revision',g=>g.revision=1.5],
    ['unknown wave',g=>g.wave=11], ['spawn phase',g=>g.roundsInWave=4],
    ['unknown enemy',g=>g.enemies[0].key='missing-creature'], ['enemy outside board',g=>g.enemies[0].x=4],
    ['enemy at tower',g=>g.enemies[0].y=9], ['missing enemy status',g=>delete g.enemies[0].wet],
    ['invalid enemy health',g=>g.enemies[0].hp=g.enemies[0].maxHP+1],
    ['missing spawn queue',g=>delete g.spawnQueue], ['unknown queued creature',g=>g.spawnQueue=[{key:'missing'}]],
    ['off-board terrain',g=>g.terrain['4,0']={wet:2}], ['noncanonical terrain',g=>g.terrain['00,0']={wet:2}],
    ['invalid terrain timer',g=>g.terrain['1,1']={wet:'two'}], ['empty terrain',g=>g.terrain['1,1']={}],
    ['nonfinite effect',g=>g.events[0].amount=null], ['unknown hands',g=>g.hands.Nobody=[]]
  ];
  for (const [label,change] of corruptions) {
    const game=clone(original); change(game);
    local.setItem(SAVE_KEY,JSON.stringify({version:2,game}));
    assert.equal(loadRun(local),null,label);
  }
});

test('all real spell results, queued spawns and advancing party rounds remain resumable', () => {
  const local=storage();
  for (const mode of ['solo','hotseat']) for (const key of Object.keys(SPELLS)) {
    let game=createGame({mode,players:mode==='solo'?['Aster']:['Aster','Bramble'],seed:42});
    // The same card shape the engine creates when forging an advanced spell.
    const card=game.hands.Aster[0]; card.key=key;
    game=applyAction(game,'Aster',{type:'cast',cardId:card.id,x:1,y:2});
    assert.equal(saveRun(local,game),true);
    assert.deepEqual(loadRun(local),game,`${mode} ${key}`);
    game=applyAction(game,'Aster',{type:'endTurn'});
    assert.equal(saveRun(local,game),true);
    assert.deepEqual(loadRun(local),game,`${mode} ${key} next turn`);
  }
  let game=createGame({mode:'hotseat',players:['Aster','Bramble','Clover','Dew'],seed:56});
  for (let turn=0;turn<100&&game.status==='playing';turn++) {
    game=applyAction(game,getActivePlayer(game),{type:'endTurn'});
    saveRun(local,game);
    assert.deepEqual(loadRun(local),game.status==='playing'?game:null);
  }
});
