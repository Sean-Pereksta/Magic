const {test} = require('node:test');
const assert = require('node:assert/strict');
const {simulation} = require('./helpers/expansion-harness.cjs');

function miningGame(faction = 'frontier') {
  const run = simulation(faction);
  run(`globalThis.fac=FACTIONS[players[0].factionId];
    addEntity('structure',fac.base,0,10,10);
    globalThis.workers=[[5,9],[5,10],[5,11]].map(([x,y])=>addEntity('unit',fac.worker,0,x,y));
    globalThis.patches=[[13,10],[13,11],[13,9]].map(([x,y])=>addTestNode('mineral',x,y,1800));`);
  return run;
}

function advanceMining(run, seconds = 60) {
  run(`for(let i=0;i<${Math.round(seconds * 10)};i++){testClock+=100;updatePersistentOrders();updateMining()}`);
}

for (const faction of ['frontier', 'ascendant', 'brood']) {
  test(`${faction}: one group order distributes workers to distinct open mineral patches and earns income`, () => {
    const run = miningGame(faction);
    run(`applyCommand({type:'groupMine',slot:0,entityIds:workers.map(w=>w.id),nodeId:patches[0].id})`);
    const orders = run.plain(`workers.map(w=>({node:w.economyNodeId,target:w.moveDestination||{x:w.x,y:w.y}}))`);
    assert.equal(new Set(orders.map(o => o.node)).size, 3);
    assert.deepEqual(new Set(orders.map(o => o.node)), new Set(run.plain('patches.map(n=>n.id)')));
    assert.equal(new Set(orders.map(o => `${o.target.x},${o.target.y}`)).size, 3);
    advanceMining(run);
    assert.equal(run('workers.every(w=>w.miningNodeId===w.economyNodeId&&!w.moveDestination)'), true);
    assert.equal(run('new Set(workers.map(w=>w.miningNodeId)).size'), 3);
    assert.equal(run('patches.every(n=>n.amount<1800&&workers.some(w=>w.id===n.reservedBy))'), true);
    assert.ok(run('players[0].minerals') > 5000);
  });

  test(`${faction}: gas group uses distinct unblocked adjacent spots and skips another incoming worker`, () => {
    const run = miningGame(faction);
    run(`resourceNodes=[];globalThis.gas=addTestNode('gas',13,10,1800);
      addEntity('structure',fac.gas,0,gas.x,gas.y);
      globalThis.incoming=addEntity('unit',fac.worker,0,15,5);
      incoming.moveReadyAt=testClock+100000;
      issueMoveOrder(incoming,13,9);incoming.economyNodeId=gas.id;tileAt(14,10).blocked=true;
      applyCommand({type:'groupMine',slot:0,entityIds:workers.slice(0,2).map(w=>w.id),nodeId:gas.id});`);
    const targets = run.plain('workers.slice(0,2).map(w=>w.moveDestination||{x:w.x,y:w.y})');
    assert.equal(new Set(targets.map(p => `${p.x},${p.y}`)).size, 2);
    assert.deepEqual(new Set(targets.map(p => `${p.x},${p.y}`)), new Set(['12,10', '13,11']));
    assert.equal(run('workers.slice(0,2).every(w=>w.economyNodeId===gas.id)'), true);
    advanceMining(run);
    assert.equal(run('workers.slice(0,2).every(w=>w.miningNodeId===gas.id&&!w.moveDestination)'), true);
    assert.ok(run('players[0].gas') > 5000);
    assert.deepEqual(run.plain('incoming.moveDestination'), {x: 13, y: 9});
  });

  test(`${faction}: arrival keeps the mining assignment when combat runs before the mining tick`, () => {
    const run = miningGame(faction);
    run(`resourceNodes.splice(1);globalThis.worker=workers[0];globalThis.patch=patches[0];
      applyCommand({type:'groupMine',slot:0,entityIds:[worker.id],nodeId:patch.id});
      for(let i=0;i<600&&worker.moveDestination;i++){testClock+=100;updatePersistentOrders()}
      globalThis.enemy=addEntity('unit','probe',1,patch.x+1,patch.y);enemy.attackReadyAt=testClock+100000;`);
    assert.equal(run('worker.x===patch.x&&worker.y===patch.y&&!worker.moveDestination&&!worker.miningNodeId'), true);
    assert.equal(run('canTarget(worker,enemy)'), true);
    run('updateCombat(.1);updatePersistentOrders()');
    assert.equal(run('worker.targetId'), null);
    assert.equal(run('worker.miningOrder?.nodeId'), run('patch.id'));
    run('updateMining();testClock+=2000;updatePersistentOrders();updateCombat(.1);updateMining()');
    assert.equal(run('worker.miningNodeId'), run('patch.id'));
    assert.ok(run('players[0].minerals') > 5000);
  });
}

test('an occupied clicked patch yields nearby open minerals before any busy fallback', () => {
  const run = miningGame();
  run(`globalThis.incumbent=addEntity('unit',fac.worker,0,patches[0].x,patches[0].y);updateMining();
    applyCommand({type:'groupMine',slot:0,entityIds:workers.slice(0,2).map(w=>w.id),nodeId:patches[0].id});`);
  assert.deepEqual(new Set(run.plain('workers.slice(0,2).map(w=>w.economyNodeId)')), new Set(run.plain('patches.slice(1).map(n=>n.id)')));
  advanceMining(run);
  assert.equal(run('incumbent.miningNodeId===patches[0].id&&patches[0].reservedBy===incumbent.id'), true);
  assert.equal(run('workers.slice(0,2).every(w=>w.miningNodeId&&w.miningNodeId!==patches[0].id)'), true);
});

test('incoming mineral assignments count as occupied even before the worker arrives', () => {
  const run = miningGame();
  run(`globalThis.incoming=addEntity('unit',fac.worker,0,15,5);incoming.moveReadyAt=testClock+100000;
    applyCommand({type:'mine',slot:0,entityId:incoming.id,nodeId:patches[0].id});
    applyCommand({type:'groupMine',slot:0,entityIds:workers.slice(0,2).map(w=>w.id),nodeId:patches[0].id});`);
  assert.equal(run('incoming.economyNodeId===patches[0].id'), true);
  assert.deepEqual(new Set(run.plain('workers.slice(0,2).map(w=>w.economyNodeId)')), new Set(run.plain('patches.slice(1).map(n=>n.id)')));
});

test('when every nearby mineral is occupied the group approaches without evicting current miners', () => {
  const run = miningGame();
  run(`globalThis.incumbents=patches.map(n=>addEntity('unit',fac.worker,0,n.x,n.y));updateMining();
    applyCommand({type:'groupMine',slot:0,entityIds:workers.map(w=>w.id),nodeId:patches[0].id});`);
  assert.equal(run('workers.every(w=>!!w.economyNodeId)'), true);
  advanceMining(run);
  assert.equal(run('incumbents.every((w,i)=>w.miningNodeId===patches[i].id&&patches[i].reservedBy===w.id)'), true);
  assert.equal(run('workers.every(w=>!incumbents.some(i=>w.x===i.x&&w.y===i.y))'), true);
  assert.ok(run('players[0].minerals') > 5000);
});

test('unreachable open minerals do not prevent using reachable nearby patches', () => {
  const run = miningGame();
  run(`tileAt(patches[0].x,patches[0].y).blocked=true;
    applyCommand({type:'groupMine',slot:0,entityIds:workers.slice(0,2).map(w=>w.id),nodeId:patches[0].id});`);
  assert.deepEqual(new Set(run.plain('workers.slice(0,2).map(w=>w.economyNodeId)')), new Set(run.plain('patches.slice(1).map(n=>n.id)')));
  advanceMining(run);
  assert.equal(run('workers.slice(0,2).every(w=>!!w.miningNodeId)'), true);
});

test('a worker escaping a blocked start does not share cross-wall reachability with another worker', () => {
  const run = miningGame();
  run(`resourceNodes=[];patches=[[13,10],[14,10]].map(([x,y])=>addTestNode('mineral',x,y,1800));
    for(let y=0;y<BOARD_H;y++)tileAt(12,y).blocked=true;
    workers[0].x=12;workers[0].y=10;workers[1].x=11;workers[1].y=10;
    applyCommand({type:'groupMine',slot:0,entityIds:workers.slice(0,2).map(w=>w.id),nodeId:patches[0].id});`);
  assert.equal(run('workers[0].miningOrder?.waiting'), false);
  assert.equal(run('workers[0].economyNodeId'), run('patches[0].id'));
  assert.equal(run('workers[1].miningOrder?.waiting'), true);
  assert.equal(run('workers[1].miningOrder.x<12'), true);
  advanceMining(run, 30);
  assert.equal(run('workers[0].miningNodeId'), run('patches[0].id'));
  assert.equal(run('workers[1].x<12&&!workers[1].miningNodeId&&workers[1].miningOrder?.waiting'), true);
  assert.equal(run('patches[1].amount'), 1800);
});

test('mineral redistribution stays in the clicked cluster rather than taking a distant base resource', () => {
  const run = miningGame();
  run(`resourceNodes.splice(1);globalThis.incumbent=addEntity('unit',fac.worker,0,patches[0].x,patches[0].y);
    addEntity('structure',fac.base,0,30,20);globalThis.distant=addTestNode('mineral',32,20,1800);updateMining();
    applyCommand({type:'groupMine',slot:0,entityIds:workers.map(w=>w.id),nodeId:patches[0].id});`);
  assert.equal(run('workers.every(w=>w.economyNodeId!==distant.id)'), true);
});

test('waiting mineral workers take a newly freed patch while existing miners keep earning', () => {
  const run = miningGame();
  run(`resourceNodes.splice(1);globalThis.patch=patches[0];
    globalThis.incumbent=addEntity('unit',fac.worker,0,patch.x,patch.y);updateMining();
    applyCommand({type:'groupMine',slot:0,entityIds:workers.map(w=>w.id),nodeId:patch.id});`);
  advanceMining(run, 25);
  assert.equal(run('incumbent.miningNodeId===patch.id'), true);
  assert.equal(run('workers.every(w=>w.miningOrder?.waiting&&!w.miningNodeId)'), true);
  run('killEntity(incumbent)');
  advanceMining(run, 25);
  assert.equal(run('workers.filter(w=>w.miningNodeId===patch.id).length'), 1);
  assert.ok(run('patch.amount') < 1800);
});

test('saturated gas stages workers in distinct nearby spots until an adjacent mining spot opens', () => {
  const run = miningGame();
  run(`resourceNodes=[];globalThis.gas=addTestNode('gas',13,10,1800);addEntity('structure',fac.gas,0,13,10);
    globalThis.incumbents=ORTHO.map(([dx,dy])=>addEntity('unit',fac.worker,0,13+dx,10+dy));updateMining();
    applyCommand({type:'groupMine',slot:0,entityIds:workers.slice(0,2).map(w=>w.id),nodeId:gas.id});`);
  assert.equal(run('workers.slice(0,2).every(w=>w.miningOrder?.waiting)'), true);
  assert.equal(run('new Set(workers.slice(0,2).map(w=>w.miningOrder.x+","+w.miningOrder.y)).size'), 2);
  advanceMining(run, 25);
  assert.equal(run('incumbents.every(w=>w.miningNodeId===gas.id)'), true);
  assert.ok(run('players[0].gas') > 5000);
  run('killEntity(incumbents[0])');
  advanceMining(run, 25);
  assert.equal(run('workers.slice(0,2).filter(w=>w.miningNodeId===gas.id).length'), 1);
  assert.equal(run('workers.slice(0,2).filter(w=>w.miningOrder?.waiting).length'), 1);
});

test('reissuing a group order releases its own previous reservations before planning', () => {
  const run = miningGame();
  run(`applyCommand({type:'groupMine',slot:0,entityIds:workers.map(w=>w.id),nodeId:patches[0].id});
    applyCommand({type:'groupMine',slot:0,entityIds:workers.map(w=>w.id).reverse(),nodeId:patches[1].id});`);
  assert.equal(run('new Set(workers.map(w=>w.economyNodeId)).size'), 3);
  assert.equal(run('workers.every(w=>w.miningOrder&&!w.miningOrder.waiting)'), true);
  advanceMining(run);
  assert.equal(run('workers.every(w=>!!w.miningNodeId)'), true);
});

test('a displaced mining worker without a route resumes its accepted assignment', () => {
  const run = miningGame();
  run(`resourceNodes.splice(1);globalThis.worker=workers[0];globalThis.patch=patches[0];
    applyCommand({type:'groupMine',slot:0,entityIds:[worker.id],nodeId:patch.id});`);
  advanceMining(run);
  assert.equal(run('worker.miningNodeId'), run('patch.id'));
  run(`releaseMining(worker);worker.x=patch.x-1;worker.y=patch.y;clearMoveDestination(worker);
    worker.moveReadyAt=testClock+1000;updatePersistentOrders()`);
  assert.equal(run('worker.miningOrder?.nodeId'), run('patch.id'));
  assert.deepEqual(run.plain('worker.moveDestination'), run.plain('({x:patch.x,y:patch.y})'));
  advanceMining(run, 10);
  assert.equal(run('worker.x===patch.x&&worker.y===patch.y&&!worker.moveDestination'), true);
  assert.equal(run('worker.miningNodeId'), run('patch.id'));
  assert.equal(run('patch.reservedBy'), run('worker.id'));
});

test('a commanded mineral beside an extractor remains mineral mining rather than switching to gas', () => {
  const run = miningGame();
  run(`resourceNodes=[];globalThis.gas=addTestNode('gas',13,10,1800);addEntity('structure',fac.gas,0,13,10);
    patches=[[13,9],[12,10]].map(([x,y])=>addTestNode('mineral',x,y,1800));
    applyCommand({type:'groupMine',slot:0,entityIds:workers.slice(0,2).map(w=>w.id),nodeId:patches[0].id});`);
  advanceMining(run);
  assert.equal(run('workers.slice(0,2).every(w=>w.miningNodeId===w.economyNodeId&&patches.some(n=>n.id===w.miningNodeId))'), true);
  assert.equal(run('gas.amount'), 1800);
  assert.equal(run('players[0].gas'), 5000);
  assert.ok(run('players[0].minerals') > 5000);
});

test('manual move, stop, hold, attack, attack-move and construction release a worker mining reservation', () => {
  for (const command of ['move', 'stop', 'hold', 'attack', 'attackMove', 'build']) {
    const run = miningGame();
    const result = run.plain(`(()=>{resourceNodes.splice(1);const patch=patches[0],worker=workers[0];
      const enemy=addEntity('unit','probe',1,8,15),gas=addTestNode('gas',12,13,1800);
      applyCommand({type:'mine',slot:0,entityId:worker.id,nodeId:patch.id});
      applyCommand({type:${JSON.stringify(command)},slot:0,entityId:worker.id,x:${command === 'build' ? 12 : 7},y:${command === 'build' ? 13 : 16},targetId:enemy.id,buildType:fac.gas});
      const released=!worker.miningOrder&&!worker.economyNodeId;
      applyCommand({type:'groupMine',slot:0,entityIds:workers.slice(1).map(w=>w.id),nodeId:patch.id});
      return {released,direct:workers.slice(1).filter(w=>w.miningOrder&&!w.miningOrder.waiting&&w.economyNodeId===patch.id).length}})()`);
    assert.equal(result.released, true, command);
    assert.equal(result.direct, 1, command);
  }
});

test('authoritative mining ignores duplicate, invalid, enemy and non-worker IDs and is deterministic', () => {
  const commandResult = reversed => {
    const run = miningGame();
    return run.plain(`(()=>{const enemy=addEntity('unit','probe',1,6,12),troop=addEntity('unit','rifleman',0,6,13),dead=addEntity('unit',fac.worker,0,6,14),passenger=addEntity('unit',fac.worker,0,6,15);
      dead.alive=false;passenger.garrisonedIn=99;
      const ids=workers.map(w=>w.id);${reversed ? 'ids.reverse();' : ''}
      applyCommand({type:'groupMine',slot:0,nodeId:patches[0].id,entityIds:[...ids,workers[0].id,enemy.id,troop.id,dead.id,passenger.id,-1,999999]});
      return {workers:workers.map(w=>({id:w.id,node:w.economyNodeId,target:w.moveDestination||{x:w.x,y:w.y},x:w.x,y:w.y})),excluded:[enemy,troop,dead,passenger].map(w=>({target:w.moveDestination,node:w.economyNodeId||null,x:w.x,y:w.y}))}})()`);
  };
  const forward = commandResult(false), reversed = commandResult(true);
  assert.deepEqual(forward, reversed);
  assert.equal(new Set(forward.workers.map(w => w.node)).size, 3);
  assert.ok(forward.excluded.every(w => w.target === null && w.node === null));
});

test('remote player group mining validates ownership and produces the same assignments as local control', () => {
  const run = miningGame();
  run(`addEntity('structure','nexus',1,25,20);globalThis.remote=[addEntity('unit','probe',1,20,20),addEntity('unit','probe',1,20,21)];
    globalThis.remoteNodes=[addTestNode('mineral',28,20,1800),addTestNode('mineral',28,21,1800)];
    applyCommand({type:'groupMine',slot:0,entityIds:remote.map(w=>w.id),nodeId:remoteNodes[0].id});`);
  assert.equal(run('remote.every(w=>!w.moveDestination&&!w.economyNodeId)'), true);
  run(`applyCommand({type:'groupMine',slot:1,entityIds:[...remote.map(w=>w.id),workers[0].id],nodeId:remoteNodes[0].id})`);
  assert.equal(run('new Set(remote.map(w=>w.economyNodeId)).size'), 2);
  assert.equal(run('workers.every(w=>!w.moveDestination&&!w.economyNodeId)'), true);
  advanceMining(run);
  assert.equal(run('remote.every(w=>!!w.miningNodeId)'), true);
  assert.ok(run('players[1].minerals') > 5000);
});

test('invalid resources reject group mining without clearing existing worker orders', () => {
  for (const invalid of ['hidden', 'disconnected', 'incompleteBase', 'depleted', 'missingNode', 'missingExtractor', 'enemyExtractor', 'incompleteExtractor']) {
    const run = miningGame();
    const result = run.plain(`(()=>{const invalid=${JSON.stringify(invalid)};
      let node=patches[0];if(invalid.includes('Extractor'))node=addTestNode('gas',13,10,1800);
      if(invalid==='hidden')tileVisibleToFaction=()=>false;
      if(invalid==='disconnected')entities.find(isBase).alive=false;
      if(invalid==='incompleteBase')entities.find(isBase).complete=false;
      if(invalid==='depleted')node.amount=0;
      if(invalid==='enemyExtractor')addEntity('structure','assimilator',1,node.x,node.y);
      if(invalid==='incompleteExtractor')addEntity('structure',fac.gas,0,node.x,node.y,{complete:false});
      for(const w of workers){w.moveReadyAt=testClock+100000;issueMoveOrder(w,7,w.y);w.miningStopped=true}
      const before=workers.map(w=>({x:w.x,y:w.y,target:w.moveDestination,miningStopped:w.miningStopped}));
      applyCommand({type:'groupMine',slot:0,entityIds:workers.map(w=>w.id),nodeId:invalid==='missingNode'?-1:node.id});
      return {before,after:workers.map(w=>({x:w.x,y:w.y,target:w.moveDestination,miningStopped:w.miningStopped})),claimed:workers.some(w=>w.economyNodeId)}})()`);
    assert.deepEqual(result.after, result.before, invalid);
    assert.equal(result.claimed, false, invalid);
  }
});

test('accepted group mining cancels previous build, attack and formation orders and survives a shared snapshot', () => {
  const run = miningGame();
  run(`globalThis.vent=addTestNode('gas',12,13,1800);issueBuildOrder(workers[0],fac.gas,vent.x,vent.y);
    workers[1].targetId=999;workers[1].attackMoveDestination={x:30,y:20};workers[1].attackMode=true;
    for(const w of workers){w.formationId='old';w.miningStopped=true}
    applyCommand({type:'groupMine',slot:0,entityIds:workers.map(w=>w.id),nodeId:patches[0].id});
    globalThis.workerIds=workers.map(w=>w.id);globalThis.orderBefore=workers.map(w=>({node:w.economyNodeId,target:w.moveDestination}));`);
  assert.equal(run('workers.every(w=>!w.pendingBuild&&!w.targetId&&!w.attackMoveDestination&&!w.attackMode&&!w.formationId&&!w.miningStopped)'), true);
  run(`applyRemoteState(JSON.parse(JSON.stringify(serializeState())));workers=workerIds.map(entityById)`);
  assert.deepEqual(run.plain('workers.map(w=>({node:w.economyNodeId,target:w.moveDestination}))'), run.plain('orderBefore'));
  advanceMining(run);
  assert.equal(run('workers.every(w=>!!w.miningNodeId)'), true);
  assert.equal(run('entities.some(e=>e.type===fac.gas)'), false);
});

function clickGame() {
  const run = miningGame();
  run(`screenToTile=(x,y)=>({x,y});globalThis.testCommands=[];
    sendCommand=c=>{testCommands.push(c);applyCommand({...c,slot:PLAYER})};
    worldToScreen=(x,y)=>({x:x*30,y:y*30});selectableEntityAtScreen=(x,y,cycle=false)=>selectableEntityAt(x,y,cycle);
    entityScreenHitScore=(e,x,y)=>e.x===x&&e.y===y?0:Infinity;W=1500;H=1500;selectEntities(workers)`);
  return run;
}

test('contextual mineral and friendly extractor taps retain the worker group and send one mining command', () => {
  const run = clickGame();
  run('handlePolishTap(patches[0].x,patches[0].y)');
  assert.equal(run('testCommands.length'), 1);
  assert.equal(run('testCommands[0].type'), 'groupMine');
  assert.deepEqual(run.plain('selectedIds'), run.plain('workers.map(w=>w.id)'));
  run(`globalThis.gas=addTestNode('gas',12,13,1800);addEntity('structure',fac.gas,0,gas.x,gas.y);handlePolishTap(gas.x,gas.y)`);
  assert.equal(run('testCommands.length'), 2);
  assert.equal(run('testCommands[1].type'), 'groupMine');
  assert.equal(run('testCommands[1].nodeId'), run('gas.id'));
  assert.deepEqual(run.plain('selectedIds'), run.plain('workers.map(w=>w.id)'));
});

test('armed group Mine affects selected workers while preserving combat unit orders', () => {
  const run = clickGame();
  run(`globalThis.troop=addEntity('unit','rifleman',0,4,12);troop.moveReadyAt=testClock+100000;issueMoveOrder(troop,7,12);
    selectEntities([troop,...workers]);interactionMode='mine';handlePolishTap(patches[0].x,patches[0].y)`);
  assert.equal(run('testCommands.length'), 1);
  assert.equal(run('testCommands[0].type'), 'groupMine');
  assert.deepEqual(new Set(run.plain('testCommands[0].entityIds')), new Set(run.plain('workers.map(w=>w.id)')));
  assert.deepEqual(run.plain('troop.moveDestination'), {x: 7, y: 12});
  assert.equal(run('interactionMode'), null);
});
