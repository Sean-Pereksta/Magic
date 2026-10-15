const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

// Test authoritative ground orders directly; pointer and keyboard paths are
// covered separately so target choice cannot accidentally depend on selection.
function game(faction='frontier'){
  const run=simulation(faction);
  run(`globalThis.commandGround=(unit,x=35,y=25,slot=unit.faction)=>applyCommand({type:'attackGround',entityId:unit.id,x,y,slot});
    globalThis.freeze=(...units)=>units.forEach(e=>{e.moveReadyAt=testClock+60000;e.attackReadyAt=testClock+60000});`);
  return run;
}

test('each troop in a ground-attack group chooses its own nearest enemy, independent of the clicked ground',()=>{
  const run=game(),r=run.plain(`(()=>{
    const left=addEntity('unit','rifleman',0,6,10),right=addEntity('unit','rifleman',0,24,10);
    const leftEnemy=addEntity('unit','zealot',1,9,10),rightEnemy=addEntity('unit','zealot',1,21,10),clickEnemy=addEntity('unit','zealot',1,14,23);
    freeze(left,right,leftEnemy,rightEnemy,clickEnemy);
    applyCommand({type:'groupAttackGround',entityIds:[left.id,right.id],x:14,y:24,slot:0});
    return {targets:[left.targetId,right.targetId],expected:[leftEnemy.id,rightEnemy.id],clickEnemy:clickEnemy.id,destinations:[left.attackMoveDestination,right.attackMoveDestination]};
  })()`);
  assert.deepEqual(r.targets,r.expected);
  assert.ok(!r.targets.includes(r.clickEnemy));
  assert.ok(r.destinations.every(Boolean),'attacks retain a destination to resume afterwards');
});

test('nearest enemy wins over a farther enemy with a stronger weapon bonus, including subsequent combat ticks',()=>{
  const run=game(),r=run.plain(`(()=>{
    const troop=addEntity('unit','marauder',0,10,10),near=addEntity('unit','zergling',1,13,10),bonus=addEntity('unit','roach',1,15,10);
    freeze(troop,near,bonus);commandGround(troop);const first=troop.targetId;
    updateCombat(.1);return {first,next:troop.targetId,near:near.id,bonus:bonus.id};
  })()`);
  assert.equal(r.first,r.near);assert.equal(r.next,r.near);
});

for(const [type,distance]of [['engineer',8],['rifleman',10],['siegeTank',14]]){
  test(`${type} initially acquires enemies at twice its normal aggro distance`,()=>{
    const run=game(),r=run.plain(`(()=>{
      const troop=addEntity('unit',${JSON.stringify(type)},0,3,10),edge=addEntity('unit','zealot',1,${3+distance},10);
      freeze(troop,edge);commandGround(troop);return {target:troop.targetId,edge:edge.id};
    })()`);
    assert.equal(r.target,r.edge);
  });
  test(`${type} does not initially acquire an enemy one tile beyond twice aggro`,()=>{
    const run=game(),r=run.plain(`(()=>{
      const troop=addEntity('unit',${JSON.stringify(type)},0,3,10),outside=addEntity('unit','zealot',1,${4+distance},10);
      freeze(troop,outside);commandGround(troop);return {target:troop.targetId,destination:troop.attackMoveDestination};
    })()`);
    assert.equal(r.target,null);assert.deepEqual(r.destination,{x:35,y:25});
  });
}

test('enemies beyond twice aggro, even beside the click, leave a compact group moving toward the destination',()=>{
  const run=game(),r=run.plain(`(()=>{
    const workers=[addEntity('unit','engineer',0,2,10),addEntity('unit','engineer',0,2,11),addEntity('unit','engineer',0,3,11)];
    const enemy=addEntity('unit','zealot',1,31,20);freeze(...workers,enemy);
    applyCommand({type:'groupAttackGround',entityIds:workers.map(e=>e.id),x:30,y:20,slot:0});
    return {targets:workers.map(e=>e.targetId),destinations:workers.map(e=>e.attackMoveDestination),positions:workers.map(e=>[e.x,e.y])};
  })()`);
  assert.deepEqual(r.targets,[null,null,null]);
  assert.equal(new Set(r.destinations.map(p=>`${p.x},${p.y}`)).size,3);
  assert.deepEqual(r.destinations.map(p=>(p.x-30)**2+(p.y-20)**2).sort((a,b)=>a-b),[0,1,1]);
  assert.deepEqual(r.positions,[[2,10],[2,11],[3,11]]);
});

test('initial acquisition rejects invisible, allied, neutral, dead, eliminated, burrowed and transported enemies and invalid air targets',()=>{
  const run=game(),r=run.plain(`(()=>{
    const worker=addEntity('unit','engineer',0,10,10),friend=addEntity('unit','rifleman',0,10,11),ally=addEntity('unit','zealot',1,11,10);
    const neutral=addEntity('structure','neutralRock',-1,10,9),hidden=addEntity('unit','zealot',2,12,10),dead=addEntity('unit','zergling',2,9,10),transported=addEntity('unit','zergling',2,11,11),air=addEntity('unit','phoenix',2,9,11),burrowed=addEntity('unit','drone',2,9,9),enemy=addEntity('unit','zealot',2,15,10);
    players[0].teamId='blue';players[1].teamId='blue';dead.alive=false;transported.garrisonedIn=123;burrowed.burrowedUntil=testClock+5000;
    tileVisibleToFaction=(f,x,y)=>!(x===hidden.x&&y===hidden.y);freeze(worker,friend,ally,hidden,transported,air,enemy);
    commandGround(worker);return {target:worker.targetId,expected:enemy.id};
  })()`);
  assert.equal(r.target,r.expected);
  const eliminated=game();
  assert.equal(eliminated(`(()=>{const troop=addEntity('unit','rifleman',0,10,10);addEntity('unit','zealot',1,12,10);players[1].eliminated=true;freeze(troop);commandGround(troop);return troop.targetId})()`),null);
});

test('a travelling ground order acquires a newly visible enemy at normal aggro range, not the initial doubled radius',()=>{
  const run=game(),r=run.plain(`(()=>{
    const troop=addEntity('unit','rifleman',0,3,10);freeze(troop);commandGround(troop,30,10);
    const enemy=addEntity('unit','zealot',1,9,10);freeze(enemy);updateCombat(.1);const outside=troop.targetId;
    enemy.x=8;updateCombat(.1);return {outside,inside:troop.targetId,enemy:enemy.id,destination:troop.attackMoveDestination};
  })()`);
  assert.equal(r.outside,null);assert.equal(r.inside,r.enemy);assert.deepEqual(r.destination,{x:30,y:10});
});

test('a nearer enemy entering normal aggro replaces a more distant initial target without losing the destination',()=>{
  const run=game(),r=run.plain(`(()=>{
    const troop=addEntity('unit','marauder',0,10,10),far=addEntity('unit','roach',1,18,10);
    freeze(troop,far);commandGround(troop,30,20);const initial=troop.targetId;
    const near=addEntity('unit','zergling',1,13,10);freeze(near);updateCombat(.1);
    return {initial,far:far.id,target:troop.targetId,near:near.id,destination:troop.attackMoveDestination};
  })()`);
  assert.equal(r.initial,r.far);assert.equal(r.target,r.near);assert.deepEqual(r.destination,{x:30,y:20});
});

for(const [faction,type]of [['frontier','engineer'],['ascendant','probe'],['brood','drone']]){
  test(`${type} pursues a chosen ground-attack enemy beyond its weapon range`,()=>{
    const run=game(faction);run(`globalThis.worker=addEntity('unit',${JSON.stringify(type)},0,4,10);globalThis.enemy=addEntity('unit','zealot',1,10,10);freeze(enemy);commandGround(worker,30,20);`);
    assert.equal(run('worker.targetId'),run('enemy.id'));
    run.advance(2);
    assert.ok(run('worker.x>4'),'workers must pursue instead of remaining pinned to weapon range');
    assert.equal(run('worker.targetId'),run('enemy.id'));
  });
}

for(const loss of ['death','fog','transport','alliance']){
  test(`ground attack resumes its clicked destination after target ${loss}`,()=>{
    const run=game(),r=run.plain(`(()=>{
      const troop=addEntity('unit','rifleman',0,10,10),enemy=addEntity('unit','zealot',1,13,10);
      freeze(troop,enemy);commandGround(troop,30,20);const chosen=troop.targetId;
      ${loss==='death'?'enemy.alive=false':loss==='fog'?'tileVisibleToFaction=(f,x,y)=>!(x===enemy.x&&y===enemy.y)':loss==='transport'?'enemy.garrisonedIn=123':"players[0].teamId=players[1].teamId='blue'"};
      updateCombat(.1);return {chosen,enemy:enemy.id,target:troop.targetId,destination:troop.attackMoveDestination,move:troop.moveDestination};
    })()`);
    assert.equal(r.chosen,r.enemy);assert.equal(r.target,null);assert.deepEqual(r.destination,{x:30,y:20});assert.deepEqual(r.move,r.destination);
  });
}

test('issuing or repeating ground attacks preserves independent running cooldowns',()=>{
  const run=game(),r=run.plain(`(()=>{
    const troop=addEntity('unit','rifleman',0,10,10),enemy=addEntity('unit','zealot',1,13,10);
    Object.assign(troop,{moveReadyAt:testClock+5678,attackReadyAt:testClock+1234,miningReadyAt:testClock+3456,abilityReadyAt:testClock+9876});
    const before=[troop.moveReadyAt,troop.attackReadyAt,troop.miningReadyAt,troop.abilityReadyAt];
    commandGround(troop);commandGround(troop);updateCombat(.1);
    return {before,after:[troop.moveReadyAt,troop.attackReadyAt,troop.miningReadyAt,troop.abilityReadyAt],impacts:pendingImpacts.length,target:troop.targetId,enemy:enemy.id};
  })()`);
  assert.deepEqual(r.after,r.before);assert.equal(r.impacts,0);assert.equal(r.target,r.enemy);
});

for(const next of ['stop','hold','move','attack']){
  test(`${next} replaces a prior ground attack and cannot revive its old destination`,()=>{
    const run=game(),r=run.plain(`(()=>{
      const troop=addEntity('unit','rifleman',0,10,10),enemy=addEntity('unit','zealot',1,13,10),other=addEntity('unit','zealot',1,22,10);
      freeze(troop,enemy,other);commandGround(troop,30,20);
      applyCommand({type:${JSON.stringify(next)},entityId:troop.id,targetId:other.id,x:4,y:20,slot:0});
      enemy.alive=false;updateCombat(.1);return {target:troop.targetId,other:other.id,destination:troop.attackMoveDestination,move:troop.moveDestination,order:troop.order};
    })()`);
    assert.equal(r.destination,null);
    if(next==='move')assert.deepEqual(r.move,{x:4,y:20});
    else if(next==='attack')assert.equal(r.target,r.other);
    else assert.equal(r.move,null);
  });
}

test('accepted transport and movement abilities cancel ground attacks without reviving their destinations',()=>{
  const transport=game(),loaded=transport.plain(`(()=>{
    const troop=addEntity('unit','rifleman',0,10,10),ship=addEntity('unit','dropship',0,10,11),enemy=addEntity('unit','zealot',1,13,10);
    freeze(troop,ship,enemy);commandGround(troop,30,20);const accepted=loadPassenger(ship,troop);
    unloadDropship(ship,{x:8,y:12});enemy.alive=false;updateCombat(.1);
    return {accepted,transported:!!troop.garrisonedIn,destination:troop.attackMoveDestination,move:troop.moveDestination};
  })()`);
  assert.equal(loaded.accepted,true);assert.equal(loaded.transported,false);assert.equal(loaded.destination,null);assert.equal(loaded.move,null);
  const ability=game(),leaped=ability.plain(`(()=>{
    const troop=addEntity('unit','reaper',0,10,10),enemy=addEntity('unit','zealot',1,13,10);
    freeze(troop,enemy);commandGround(troop,30,20);const accepted=useAbility(troop,'jetLeap',{x:10,y:13});enemy.alive=false;updateCombat(.1);
    return {accepted,x:troop.x,y:troop.y,destination:troop.attackMoveDestination,move:troop.moveDestination};
  })()`);
  assert.equal(leaped.accepted,true);assert.deepEqual([leaped.x,leaped.y],[10,13]);assert.equal(leaped.destination,null);assert.equal(leaped.move,null);
});

test('ground orders validate ownership and eligibility while deduplicating group troop IDs',()=>{
  const run=game(),r=run.plain(`(()=>{
    const own=addEntity('unit','rifleman',0,10,10),foreign=addEntity('unit','rifleman',1,10,11),building=addEntity('structure','barracks',0,9,10),passenger=addEntity('unit','rifleman',0,9,11),enemy=addEntity('unit','zealot',2,13,10);
    players[0].teamId=players[1].teamId='blue';
    passenger.garrisonedIn=123;freeze(own,foreign,passenger,enemy);
    applyCommand({type:'attackGround',entityId:own.id,x:30,y:20,slot:1});const forged=own.targetId;
    applyCommand({type:'groupAttackGround',entityIds:[own.id,own.id,foreign.id,building.id,passenger.id],x:30,y:20,slot:0});
    return {forged,target:own.targetId,enemy:enemy.id,foreign:foreign.targetId,building:building.targetId,passenger:passenger.targetId,destination:own.attackMoveDestination};
  })()`);
  assert.equal(r.forged,null);assert.equal(r.target,r.enemy);assert.equal(r.foreign,null);assert.equal(r.building,null);assert.equal(r.passenger,null);assert.deepEqual(r.destination,{x:30,y:20});
});

test('serialized ground-attack state survives a controller handoff and resumes after the enemy is lost',()=>{
  const run=game(),r=run.plain(`(()=>{
    const troop=addEntity('unit','rifleman',0,10,10),enemy=addEntity('unit','zealot',1,13,10);
    freeze(troop,enemy);commandGround(troop,30,20);const id=troop.id,enemyId=enemy.id;
    const snapshot=JSON.parse(JSON.stringify(serializeState()));applyRemoteState(snapshot);
    const restored=entityById(id),before={target:restored.targetId,destination:restored.attackMoveDestination};
    entityById(enemyId).alive=false;updateCombat(.1);
    return {before,enemy:enemyId,after:{target:restored.targetId,destination:restored.attackMoveDestination,move:restored.moveDestination}};
  })()`);
  assert.equal(r.before.target,r.enemy);assert.deepEqual(r.before.destination,{x:30,y:20});assert.equal(r.after.target,null);assert.deepEqual(r.after.destination,r.before.destination);assert.deepEqual(r.after.move,r.before.destination);
});

test('a remote ground-attack command is applied once by the controller and is acknowledged in snapshots',()=>{
  const run=game();run(`globalThis.troop=addEntity('unit','rifleman',1,10,10);globalThis.enemy=addEntity('unit','zealot',0,13,10);freeze(troop,enemy);lobbyId='ground-orders';isController=true;controllerLease={ownerUid:'host',epoch:1};manageCommandListener();`);
  const command=run.plain(`({type:'attackGround',entityId:troop.id,x:30,y:20,slot:1,createdAtMs:testClock,controllerEpochKnown:1})`);
  const change={type:'added',doc:{id:'ground-order-1',data:()=>command}};
  run.network.listener({docChanges:()=>[change,change]});
  assert.equal(run('troop.targetId'),run('enemy.id'));
  assert.deepEqual(run.plain('serializeState().acks'),['ground-order-1']);
  run(`troop.targetId=null;troop.attackMoveDestination=null;troop.moveDestination=null;`);
  run.network.listener({docChanges:()=>[change]});
  assert.equal(run('troop.targetId'),null,'a repeated remote document must not reissue the attack');
});
