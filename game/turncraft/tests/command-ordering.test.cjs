const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

function controller(epoch=1){const run=simulation();run(`lobbyId='ordered-room';stateRef='state';username='Player 0';auth.currentUser={uid:'host'};controllerLease={ownerUid:'host',epoch:${epoch},expiresAtMs:testClock+60000};isController=true;globalThis.applied=[];applyCommand=c=>{applied.push(c.type);return {count:1,total:1}};`);return run}
function command(seq,type,extra={}){return {id:`peer_${seq}`,clientCommandId:`peer_${seq}`,clientStream:'peer_session',clientSeq:seq,by:'Player 1',slot:1,createdAtMs:100000,type,...extra}}
function enqueue(run,c){run(`enqueueCommand(${JSON.stringify(c)})`)}
function permutations(a){return a.length?a.flatMap((v,i)=>permutations(a.filter((_,j)=>j!==i)).map(t=>[v,...t])):[[]]}

test('all reordered deliveries preserve distinct actions and the final stop; duplicates never replay',()=>{
  for(const order of permutations([1,2,3])){
    const run=controller(),commands=[command(1,'move'),command(2,'ability'),command(3,'stop')];
    for(const n of order){enqueue(run,commands[n-1]);enqueue(run,commands[n-1])}
    assert.deepEqual(run.plain('applied'),['move','ability','stop'],order.join(','));
    assert.equal(run('commandStreams.get("peer_session").seq'),3);
    assert.equal(run('commandResults.length'),3);
    enqueue(run,{...commands[0],id:'duplicate-with-another-doc-id'});
    assert.deepEqual(run.plain('applied'),['move','ability','stop']);
  }
});

test('a sequence gap waits without skipping an ability, including after more than ten seconds',()=>{
  const run=controller();enqueue(run,command(3,'stop'));enqueue(run,command(2,'ability'));
  assert.deepEqual(run.plain('applied'),[]);assert.equal(run.network.deletes.length,0);
  run('testClock+=60000');enqueue(run,command(1,'move'));
  assert.deepEqual(run.plain('applied'),['move','ability','stop']);
});

test('command documents survive failed snapshot writes and are removed only after a committed state',async()=>{
  const run=controller();enqueue(run,command(1,'move'));enqueue(run,command(2,'stop'));
  assert.equal(run.network.deletes.length,0);
  run(`globalThis.realSetDoc=setDoc;setDoc=async()=>{throw new Error('offline')};reportNetworkFailure=()=>{};console={...console,warn:()=>{}}`);
  assert.equal(await run('writeState(true)'),false);assert.equal(run.network.deletes.length,0);
  run('setDoc=realSetDoc');await run('writeState(true)');
  const saved=run.network.writes.find(w=>w.ref==='state').data;
  assert.equal(saved.commandStreams.peer_session.seq,2);
  assert.equal(saved.commandResults.at(-1).id,'peer_2');
  assert.deepEqual(run.network.deletes.sort(),['lobbies/ordered-room/chess_starfront_commands/peer_1','lobbies/ordered-room/chess_starfront_commands/peer_2']);
});

test('migration restores the durable cursor before processing old documents or new actions',()=>{
  const first=controller();enqueue(first,command(1,'ability'));enqueue(first,command(2,'stop'));
  const snapshot=first.plain('serializeState()'),next=controller(2);
  assert.equal(next(`applyRemoteState(${JSON.stringify(snapshot)})`),true);
  enqueue(next,command(2,'stop'));enqueue(next,command(1,'ability'));enqueue(next,command(3,'move'));
  assert.deepEqual(next.plain('applied'),['move']);
  assert.equal(next('commandStreams.get("peer_session").seq'),3);
});

test('a command applied after the last saved snapshot remains replayable by a takeover controller',()=>{
  const first=controller(),snapshot=first.plain('serializeState()');enqueue(first,command(1,'ability'));
  assert.equal(first.network.deletes.length,0);
  const next=controller(2);next(`applyRemoteState(${JSON.stringify(snapshot)})`);enqueue(next,command(1,'ability'));enqueue(next,command(1,'ability'));
  assert.deepEqual(next.plain('applied'),['ability']);
});

test('epoch changes accept a lower tick, reject late old snapshots, and preserve cursors in old-format snapshots',()=>{
  const run=controller(),base=run.plain('serializeState()');enqueue(run,command(1,'stop'));
  const newer=run.plain('serializeState()');newer.controllerEpoch=2;newer.simulationTick=1;
  assert.equal(run(`applyRemoteState(${JSON.stringify(newer)})`),true);assert.equal(run('simulationTick'),1);
  assert.equal(run(`applyRemoteState(${JSON.stringify({...base,simulationTick:9999})})`),false);
  const legacy={...newer,simulationTick:2};delete legacy.commandProtocol;delete legacy.commandStreams;delete legacy.commandResults;
  assert.equal(run(`applyRemoteState(${JSON.stringify(legacy)})`),true);enqueue(run,command(1,'stop'));
  assert.deepEqual(run.plain('applied'),['stop']);
});

test('a peer acknowledges locally at issue time but remains pending until authoritative state acknowledges it',async()=>{
  const run=simulation();run(`lobbyId='ordered-room';username='Player 0';auth.currentUser={uid:'peer'};controllerLease={epoch:1};isController=false;globalThis.unit=addEntity('unit','rifleman',0,5,5);`);
  await run(`sendCommand({type:'move',entityId:unit.id,x:12,y:5})`);
  assert.equal(run('unackedCommands.size'),1);assert.equal(run('orderFeedbackEntries.get(orderFeedbackLatest).status'),'pending');
  assert.equal(run('unit.x'),5,'presentation feedback does not move the simulation');
  const snapshot=run.plain(`({...serializeState(),commandStreams:{[clientStream]:{slot:0,by:username,seq:clientSequence}},commandResults:[{id:[...unackedCommands.keys()][0],status:'rejected',count:0,total:1,reason:'No legal destination'}]})`);
  run(`applyRemoteState(${JSON.stringify(snapshot)})`);
  assert.equal(run('unackedCommands.size'),0);assert.equal(run('orderFeedbackEntries.get(orderFeedbackLatest).status'),'rejected');
});

test('the per-tab outbox survives reload, retries an existing create-only document without rewriting it',async()=>{
  const run=simulation();run(`lobbyId='ordered-room';username='Player 0';auth.currentUser={uid:'peer'};isController=false;globalThis.unit=addEntity('unit','rifleman',0,5,5);`);
  await run(`sendCommand({type:'stop',entityId:unit.id})`);
  const original=run.plain('[...unackedCommands.values()][0]'),store=run.context.localStorage;
  const reload=simulation();reload.context.localStorage=store;reload.context.name=run.context.name;reload.context.getDoc=async()=>({exists:()=>true,data:()=>original});
  reload(`lobbyId='ordered-room';username='Player 0';auth.currentUser={uid:'peer'};isController=false;ensureCommandSession()`);
  assert.equal(reload('clientStream'),original.clientStream);assert.equal(reload('clientSequence'),original.clientSeq);assert.equal(reload('unackedCommands.size'),1);
  await reload('publishCommand([...unackedCommands.values()][0],true)');assert.equal(reload.network.writes.length,0);
  await reload(`sendCommand({type:'hold',entityId:1})`);
  assert.equal(reload.network.writes.at(-1).data.clientSeq,original.clientSeq+1);
});

test('commands with a forged army owner never reach simulation',()=>{
  const run=controller();enqueue(run,command(1,'move',{slot:0}));
  assert.deepEqual(run.plain('applied'),[]);assert.equal(run('commandResults.at(-1).status'),'rejected');
});

test('cloned tab storage allocates a separate stream and solo orders cannot create a multiplayer gap',async()=>{
  const first=simulation();first(`username='Player 0';auth.currentUser={uid:'peer'};globalThis.unit=addEntity('unit','rifleman',0,5,5)`);
  await first(`sendCommand({type:'stop',entityId:unit.id})`);
  first(`lobbyId='ordered-room';isController=false`);await first(`sendCommand({type:'stop',entityId:unit.id})`);
  assert.equal(first('clientSequence'),1);
  const stream=first('clientStream'),clone=simulation();clone.context.localStorage=first.context.localStorage;
  clone(`lobbyId='ordered-room';username='Player 0';auth.currentUser={uid:'peer'};ensureCommandSession()`);
  assert.notEqual(clone('clientStream'),stream);assert.equal(clone('clientSequence'),0);
});

test('restoring a legacy snapshot cannot retain speculative unsaved command cursors',()=>{
  const run=controller(),old=run.plain('serializeState()');delete old.commandProtocol;delete old.commandStreams;delete old.commandResults;
  enqueue(run,command(1,'ability'));old.controllerEpoch=2;old.simulationTick=1;
  run(`applyRemoteState(${JSON.stringify(old)})`);enqueue(run,command(1,'ability'));
  assert.deepEqual(run.plain('applied'),['ability','ability'],'the unsaved action must replay against the rolled-back battlefield');
});

test('an evicted result acknowledges processing without claiming an unavailable acceptance result',async()=>{
  const run=simulation();run(`lobbyId='ordered-room';username='Player 0';auth.currentUser={uid:'peer'};isController=false;globalThis.unit=addEntity('unit','rifleman',0,5,5);`);
  await run(`sendCommand({type:'stop',entityId:unit.id})`);
  const snapshot=run.plain(`({...serializeState(),commandStreams:{[clientStream]:{slot:0,by:username,seq:clientSequence}},acks:[],commandResults:[]})`);
  run(`applyRemoteState(${JSON.stringify(snapshot)})`);
  assert.equal(run('unackedCommands.size'),0);
  assert.equal(run('orderFeedbackEntries.get(orderFeedbackLatest).status'),'acknowledged');
});
