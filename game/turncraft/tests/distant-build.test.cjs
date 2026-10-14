const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

for(const faction of ['frontier','ascendant','brood']){
  test(`${faction}: a base can be ordered ten squares away on empty ground for 300 minerals`,()=>{
    const run=simulation(faction);
    const queued=run.plain(`(()=>{const fac=FACTIONS[players[0].factionId];window.testWorker=addEntity('unit',fac.worker,0,3,5);players[0].minerals=300;players[0].gas=0;const accepted=issueBuildOrder(testWorker,fac.base,13,5);return {accepted,queued:!!testWorker.pendingBuild,minerals:players[0].minerals,alive:testWorker.alive!==false,structures:entities.filter(e=>e.kind==='structure').length,site:validBuildSite(testWorker,fac.base,13,5),adjacent:validBuildTile(testWorker,fac.base,13,5),cost:STRUCTURES[fac.base].cost}})()`);
    assert.equal(queued.accepted,true);assert.equal(queued.queued,true);assert.equal(queued.minerals,300);assert.equal(queued.alive,true);assert.equal(queued.structures,0);assert.equal(queued.site,true);assert.equal(queued.adjacent,false);assert.deepEqual(queued.cost,[300,0]);
    const started=run.plain(`(()=>{const fac=FACTIONS[players[0].factionId];let base;for(let i=0;i<120;i++){testClock+=500;updatePersistentOrders();base=entities.find(e=>e.type===fac.base);if(base)break}return {started:!!base,complete:base?.complete,cost:300-players[0].minerals,alive:testWorker.alive!==false,pending:!!testWorker.pendingBuild,adjacent:!!base&&dist(testWorker,base)<=1.5,distinct:!!base&&(testWorker.x!==base.x||testWorker.y!==base.y),duration:base&&(base.buildEnd-base.buildStart)/1000,expected:scaledTime(STRUCTURES[fac.base].time)}})()`);
    assert.equal(started.started,true);assert.equal(started.complete,false);assert.equal(started.cost,300);assert.equal(started.alive,faction!=='brood');assert.equal(started.pending,false);assert.equal(started.adjacent,true);assert.equal(started.distinct,true);assert.equal(started.duration,started.expected);
    run.advance(20);
    assert.equal(run(`entities.find(e=>e.type===FACTIONS[players[0].factionId].base)?.complete`),true);
    assert.equal(run('players[0].minerals'),0,'construction charges once');
  });

  test(`${faction}: distant non-base construction retains its normal rules and worker lifecycle`,()=>{
    const run=simulation(faction);
    const result=run.plain(`(()=>{const faction=players[0].factionId,type={frontier:'supplyDepot',ascendant:'gateway',brood:'spawningPool'}[faction];if(faction==='ascendant')addEntity('structure','pylon',0,15,7);if(faction==='brood')addEntity('structure','hatchery',0,15,7);const worker=addEntity('unit',FACTIONS[faction].worker,0,3,5),initial=players[0].minerals,ok=issueBuildOrder(worker,type,15,5),pending=!!worker.pendingBuild,initialSpent=initial-players[0].minerals;for(let i=0;i<120;i++){testClock+=500;updatePersistentOrders();updateProduction(.5)}const built=entities.find(e=>e.type===type);return {ok,pending,initialSpent,built:!!built,complete:built?.complete,spent:initial-players[0].minerals,expected:STRUCTURES[type].cost[0],workerAlive:worker.alive!==false}})()`);
    assert.equal(result.ok,true);assert.equal(result.pending,true);assert.equal(result.initialSpent,0);assert.equal(result.built,true);assert.equal(result.complete,true);assert.equal(result.spent,result.expected);assert.equal(result.workerAlive,faction!=='brood');
  });
}

test('remote player construction commands cannot use another player or a different faction worker',()=>{
  const run=simulation();
  const result=run.plain(`(()=>{const worker=addEntity('unit','probe',1,3,5);applyCommand({type:'build',slot:0,entityId:worker.id,buildType:'nexus',x:15,y:5});const forged=!!worker.pendingBuild;applyCommand({type:'build',slot:1,entityId:worker.id,buildType:'commandHub',x:15,y:5});const wrongFaction=!!worker.pendingBuild;applyCommand({type:'build',slot:1,entityId:worker.id,buildType:'nexus',x:15,y:5});const accepted=!!worker.pendingBuild;for(let i=0;i<120;i++){testClock+=500;updatePersistentOrders()}return {forged,wrongFaction,accepted,built:entities.some(e=>e.type==='nexus'&&e.faction===1),remoteSpent:5000-players[1].minerals,localSpent:5000-players[0].minerals}})()`);
  assert.equal(result.forged,false);assert.equal(result.wrongFaction,false);assert.equal(result.accepted,true);assert.equal(result.built,true);assert.equal(result.remoteSpent,300);assert.equal(result.localSpent,0);
});

for(const invalid of ['unaffordable','hidden','occupied','terrain','mineral','gas','unreachable','fractional'])test(`distant construction rejects ${invalid} sites without moving or charging`,()=>{
  const run=simulation();
  const result=run.plain(`(()=>{const worker=addEntity('unit','engineer',0,3,5),invalid=${JSON.stringify(invalid)};if(invalid==='unaffordable')players[0].minerals=299;if(invalid==='hidden')tileVisibleToFaction=()=>false;if(invalid==='occupied')addEntity('unit','probe',1,15,5);if(invalid==='terrain')tileAt(15,5).blocked=true;if(invalid==='mineral'||invalid==='gas')addTestNode(invalid,15,5,1800);if(invalid==='unreachable')for(const [dx,dy] of DIR8)tileAt(15+dx,5+dy).blocked=true;const initial=players[0].minerals,ok=issueBuildOrder(worker,'commandHub',invalid==='fractional'?15.5:15,5);return {ok,pending:!!worker.pendingBuild,x:worker.x,y:worker.y,spent:initial-players[0].minerals}})()`);
  assert.equal(result.ok,false);assert.equal(result.pending,false);assert.equal(result.x,3);assert.equal(result.y,5);assert.equal(result.spent,0);
});

test('technology, power, and creep requirements also apply to distant sites',()=>{
  for(const [faction,type,setup] of [['frontier','factory',''],['ascendant','gateway',"addEntity('structure','pylon',0,4,5)"],['brood','spawningPool',"addEntity('structure','hatchery',0,4,5)"]]){
    const run=simulation(faction);
    const result=run.plain(`(()=>{${setup};const worker=addEntity('unit',FACTIONS[players[0].factionId].worker,0,3,5),initial=players[0].minerals,ok=issueBuildOrder(worker,${JSON.stringify(type)},15,5);return {ok,pending:!!worker.pendingBuild,spent:initial-players[0].minerals}})()`);
    assert.equal(result.ok,false,faction);assert.equal(result.pending,false,faction);assert.equal(result.spent,0,faction);
  }
});

test('normal construction sites are reserved by pending workers and released by cancellation',()=>{
  const run=simulation();
  const result=run.plain(`(()=>{const first=addEntity('unit','engineer',0,3,5),second=addEntity('unit','engineer',0,3,6),accepted=issueBuildOrder(first,'commandHub',15,5),duplicate=issueBuildOrder(second,'supplyDepot',15,5);applyCommand({type:'stop',slot:0,entityId:first.id});const afterCancel=issueBuildOrder(second,'supplyDepot',15,5);for(let i=0;i<120;i++){testClock+=500;updatePersistentOrders()}return {accepted,duplicate,afterCancel,count:entities.filter(e=>e.kind==='structure'&&e.x===15&&e.y===5).length,type:entities.find(e=>e.kind==='structure'&&e.x===15&&e.y===5)?.type,spent:5000-players[0].minerals}})()`);
  assert.equal(result.accepted,true);assert.equal(result.duplicate,false);assert.equal(result.afterCancel,true);assert.equal(result.count,1);assert.equal(result.type,'supplyDepot');assert.equal(result.spent,100);
});

test('movement, combat, Stop, Hold, mining, replacement builds and death cancel distant construction without charge',()=>{
  for(const command of ['move','groupMove','attack','groupAttack','attackMove','groupAttackMove','stop','groupStop','hold','groupHold','mine','replacement','death']){
    const run=simulation();
    const result=run.plain(`(()=>{const worker=addEntity('unit','engineer',0,3,5),enemy=addEntity('unit','zergling',1,7,9);addEntity('structure','commandHub',0,4,9);const mineral=addTestNode('mineral',5,9,1800);issueBuildOrder(worker,'supplyDepot',15,5);const command=${JSON.stringify(command)};if(command==='death')killEntity(worker);else if(command==='replacement')issueBuildOrder(worker,'barracks',15,8);else applyCommand({type:command,slot:0,entityId:worker.id,entityIds:[worker.id],x:5,y:8,targetId:enemy.id,nodeId:mineral.id});const oldPending=worker.pendingBuild?.x===15&&worker.pendingBuild?.y===5;for(let i=0;i<120;i++){testClock+=500;updatePersistentOrders()}return {oldPending,built:entities.some(e=>e.type==='supplyDepot'),spent:5000-players[0].minerals,replacement:entities.some(e=>e.type==='barracks')}})()`);
    assert.equal(result.oldPending,false,command);assert.equal(result.built,false,command);assert.equal(result.spent,command==='replacement'?150:0,command);assert.equal(result.replacement,command==='replacement',command);
  }
});

for(const change of ['occupied','unaffordable','blocked','power','creep','technology'])test(`a queued distant build rechecks ${change} before construction`,()=>{
  const faction=change==='power'?'ascendant':change==='creep'?'brood':'frontier',run=simulation(faction);
  const result=run.plain(`(()=>{const faction=players[0].factionId,change=${JSON.stringify(change)},type=change==='power'?'gateway':change==='creep'?'spawningPool':change==='technology'?'factory':'commandHub';let support;if(change==='power')support=addEntity('structure','pylon',0,15,7);if(change==='creep')support=addEntity('structure','hatchery',0,15,7);if(change==='technology')support=addEntity('structure','barracks',0,5,7);const worker=addEntity('unit',FACTIONS[faction].worker,0,3,5),queued=issueBuildOrder(worker,type,15,5);if(change==='occupied')addEntity('unit','probe',1,15,5);if(change==='unaffordable')players[0].minerals=0;if(change==='blocked')for(const [dx,dy] of DIR8)tileAt(15+dx,5+dy).blocked=true;if(support)support.alive=false;const initial=players[0].minerals;for(let i=0;i<160;i++){testClock+=500;updatePersistentOrders()}return {queued,pending:!!worker.pendingBuild,built:entities.some(e=>e.type===type&&e.x===15&&e.y===5),spent:initial-players[0].minerals,alive:worker.alive!==false}})()`);
  assert.equal(result.queued,true);assert.equal(result.pending,false);assert.equal(result.built,false);assert.equal(result.spent,0);assert.equal(result.alive,true);
});

test('distant normal construction survives multiplayer snapshots with its reservation and single charge',()=>{
  const run=simulation();
  const result=run.plain(`(()=>{const worker=addEntity('unit','engineer',0,3,5);issueBuildOrder(worker,'commandHub',30,20);const id=worker.id,snapshot=JSON.parse(JSON.stringify(serializeState())),saved=!!snapshot.entities.find(e=>e.id===id)?.pendingBuild;applyRemoteState(snapshot);const second=addEntity('unit','engineer',0,3,6),duplicate=issueBuildOrder(second,'supplyDepot',30,20);for(let i=0;i<200;i++){testClock+=500;updatePersistentOrders()}return {saved,duplicate,pending:!!entityById(id)?.pendingBuild,built:entities.filter(e=>e.type==='commandHub'&&e.x===30&&e.y===20).length,spent:5000-players[0].minerals}})()`);
  assert.equal(result.saved,true);assert.equal(result.duplicate,false);assert.equal(result.pending,false);assert.equal(result.built,1);assert.equal(result.spent,300);
});

test('a worker standing on the chosen normal site steps aside before starting the building',()=>{
  const run=simulation();
  const result=run.plain(`(()=>{const worker=addEntity('unit','engineer',0,15,5),ok=issueBuildOrder(worker,'supplyDepot',15,5),site=entities.find(e=>e.type==='supplyDepot');return {ok,built:!!site,aside:!!site&&(worker.x!==site.x||worker.y!==site.y),near:!!site&&dist(worker,site)<=1.5,spent:5000-players[0].minerals}})()`);
  assert.equal(result.ok,true);assert.equal(result.built,true);assert.equal(result.aside,true);assert.equal(result.near,true);assert.equal(result.spent,100);
});

test('one travelling builder can cross another reserved site without cancelling either job',()=>{
  const run=simulation();
  const result=run.plain(`(()=>{const first=addEntity('unit','engineer',0,3,5),second=addEntity('unit','engineer',0,1,15);const firstAccepted=issueBuildOrder(first,'commandHub',15,5),secondAccepted=issueBuildOrder(second,'supplyDepot',9,5);let crossed=false,pendingDuringCrossing=false;for(let i=0;i<150;i++){testClock+=500;updatePersistentOrders();if(first.x===9&&first.y===5){crossed=true;pendingDuringCrossing||=!!second.pendingBuild}updateProduction(.5)}return {firstAccepted,secondAccepted,crossed,pendingDuringCrossing,bases:entities.filter(e=>e.type==='commandHub'&&e.x===15&&e.y===5&&e.complete).length,depots:entities.filter(e=>e.type==='supplyDepot'&&e.x===9&&e.y===5&&e.complete).length,spent:5000-players[0].minerals,pending:!!first.pendingBuild||!!second.pendingBuild}})()`);
  assert.equal(result.firstAccepted,true);assert.equal(result.secondAccepted,true);assert.equal(result.crossed,true,'the first worker actually crosses the reserved depot square');assert.equal(result.pendingDuringCrossing,true);assert.equal(result.bases,1);assert.equal(result.depots,1);assert.equal(result.spent,400);assert.equal(result.pending,false);
});

test('an arrived builder waits for troop traffic on its site and starts once it clears',()=>{
  const run=simulation();
  const result=run.plain(`(()=>{const worker=addEntity('unit','engineer',0,3,5),accepted=issueBuildOrder(worker,'commandHub',15,5);worker.x=14;worker.y=5;clearMoveDestination(worker);const traffic=addEntity('unit','engineer',0,15,5);for(let i=0;i<6;i++){testClock+=500;updatePersistentOrders()}const waiting=!!worker.pendingBuild,spentWhileOccupied=5000-players[0].minerals,builtWhileOccupied=entities.some(e=>e.type==='commandHub');issueMoveOrder(traffic,19,5);for(let i=0;i<60;i++){testClock+=500;updatePersistentOrders();updateProduction(.5)}return {accepted,waiting,spentWhileOccupied,builtWhileOccupied,count:entities.filter(e=>e.type==='commandHub'&&e.x===15&&e.y===5).length,complete:entities.find(e=>e.type==='commandHub')?.complete,pending:!!worker.pendingBuild,spent:5000-players[0].minerals}})()`);
  assert.equal(result.accepted,true);assert.equal(result.waiting,true);assert.equal(result.spentWhileOccupied,0);assert.equal(result.builtWhileOccupied,false);assert.equal(result.count,1);assert.equal(result.complete,true);assert.equal(result.pending,false);assert.equal(result.spent,300);
});
