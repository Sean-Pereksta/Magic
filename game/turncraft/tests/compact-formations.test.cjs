const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

test('mixed troops reserve the nearest compact footprint without large-unit or support gaps',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const types=['rifleman','engineer','battlecruiser','medic','marauder','siegeTank','goliath','rifleman','rifleman','engineer','medic','rifleman','marauder','goliath','siegeTank','rifleman'];
    const units=types.map((type,i)=>addEntity('unit',type,0,2+i%4,2+Math.floor(i/4),{moveReadyAt:1e9}));
    const accepted=moveGroup(units.map(u=>u.id),20,15,0),destinations=units.map(u=>u.moveDestination);
    return {accepted,unique:new Set(destinations.map(p=>p.x+','+p.y)).size,radii:destinations.map(p=>(p.x-20)**2+(p.y-15)**2).sort((a,b)=>a-b),expected:closestFormationTiles({x:20,y:15}).slice(0,units.length).map(p=>(p.x-20)**2+(p.y-15)**2),centers:units.map(u=>u.formationAnchor)};
  })()`);
  assert.equal(result.accepted,true);assert.equal(result.unique,16);
  assert.deepEqual(result.radii,result.expected);
  assert.ok(result.centers.every(p=>p.x===20&&p.y===15));
});

test('selected troops already occupying optimal stands stay put instead of swapping',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const units=[addEntity('unit','rifleman',0,9,10),addEntity('unit','rifleman',0,10,10),addEntity('unit','engineer',0,11,10),addEntity('unit','medic',0,10,9),addEntity('unit','marauder',0,10,11)];
    const before=units.map(u=>({x:u.x,y:u.y,ready:u.moveReadyAt})),accepted=moveGroup(units.map(u=>u.id).reverse(),10,10,0);
    return {accepted,before,after:units.map(u=>({x:u.x,y:u.y,ready:u.moveReadyAt})),destinations:units.map(u=>u.moveDestination)};
  })()`);
  assert.equal(result.accepted,true);assert.deepEqual(result.after,result.before);
  assert.ok(result.destinations.every(p=>p===null));
});

test('a cooling-down aircraft already at the command keeps its stand beside selected ground troops',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const ground=addEntity('unit','rifleman',0,19,15),air=addEntity('unit','wraith',0,20,15,{moveReadyAt:testClock+6000}),ready=air.moveReadyAt;
    moveGroup([ground.id,air.id],20,15,0);for(let i=0;i<80;i++){testClock+=250;updatePersistentOrders()}
    return {radii:[ground,air].map(u=>(u.x-20)**2+(u.y-15)**2).sort((a,b)=>a-b),positions:[{x:ground.x,y:ground.y},{x:air.x,y:air.y}],ready,afterReady:air.moveReadyAt,moving:[ground,air].filter(u=>u.moveDestination).length};
  })()`);
  assert.deepEqual(result.radii,[0,1]);assert.deepEqual(result.positions,[{x:19,y:15},{x:20,y:15}]);
  assert.equal(result.afterReady,result.ready);assert.equal(result.moving,0);
});

test('all destinations are planned before initial steps mutate selected occupied tiles',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const a=addEntity('unit','rifleman',0,4,10),b=addEntity('unit','rifleman',0,5,10),c=addEntity('unit','rifleman',0,6,10);
    const original=issueMoveOrder,issued=[];issueMoveOrder=(u,x,y)=>{issued.push({id:u.id,x,y});const value=original(u,x,y);if(issued.length===1)addEntity('unit','rifleman',1,10,9);return value};
    moveGroup([a.id,b.id,c.id],10,10,0);return {issued,unique:new Set(issued.map(p=>p.x+','+p.y)).size};
  })()`);
  assert.equal(result.unique,3);
  assert.deepEqual(result.issued.map(p=>[p.x,p.y]).sort(),[[9,10],[10,9],[10,10]].sort());
});

test('nearby open slots behind an impassable wall are excluded from ground destinations',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    for(let y=8;y<=12;y++)for(let x=8;x<=12;x++)if(Math.max(Math.abs(x-10),Math.abs(y-10))===2)tileAt(x,y).blocked=true;
    const units=[addEntity('unit','rifleman',0,3,10,{moveReadyAt:1e9}),addEntity('unit','engineer',0,3,11,{moveReadyAt:1e9})];
    moveGroup(units.map(u=>u.id),10,10,0);return units.map(u=>u.moveDestination);
  })()`);
  assert.ok(result.every(p=>(p.x-10)**2+(p.y-10)**2===9));
});

test('large blocked landing areas search beyond the old truncated candidate list',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    for(const t of terrain)if(Math.hypot(t.x-20,t.y-15)<=6)t.blocked=true;
    const units=[addEntity('unit','rifleman',0,3,10,{moveReadyAt:1e9}),addEntity('unit','engineer',0,3,11,{moveReadyAt:1e9})];
    const accepted=moveGroup(units.map(u=>u.id),20,15,0);return {accepted,radii:units.map(u=>(u.moveDestination.x-20)**2+(u.moveDestination.y-15)**2)};
  })()`);
  assert.equal(result.accepted,true);assert.deepEqual(result.radii,[37,37]);
});

test('occupied tiles, resources and another group incoming reservations keep unique nearby slots',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const units=Array.from({length:5},(_,i)=>addEntity('unit','rifleman',0,3,i+6,{moveReadyAt:1e9}));
    const incoming=addEntity('unit','rifleman',0,35,20,{moveReadyAt:1e9});incoming.moveDestination={x:20,y:15};
    addEntity('unit','rifleman',1,19,15);addEntity('structure','barracks',0,21,15);addTestNode('mineral',20,14,900);addTestNode('gas',20,16,1800);
    moveGroup(units.map(u=>u.id),20,15,0);return units.map(u=>u.moveDestination);
  })()`);
  assert.equal(new Set(result.map(p=>`${p.x},${p.y}`)).size,5);
  assert.ok(result.every(p=>(p.x-20)**2+(p.y-15)**2>=2&&(p.x-20)**2+(p.y-15)**2<=4));
});

test('mixed cooldowns and body sizes actually arrive at the nearest group footprint',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const units=['rifleman','engineer','medic','marauder','goliath','siegeTank','rifleman','marauder','rifleman'].map((type,i)=>addEntity('unit',type,0,3+i%3,6+Math.floor(i/3),{moveReadyAt:testClock+i*220}));
    moveGroup(units.map(u=>u.id),20,15,0);for(let i=0;i<300;i++){testClock+=250;updatePersistentOrders()}
    return {radii:units.map(u=>(u.x-20)**2+(u.y-15)**2).sort((a,b)=>a-b),moving:units.filter(u=>u.moveDestination).length,unique:new Set(units.map(u=>u.x+','+u.y)).size};
  })()`);
  assert.deepEqual(result.radii,[0,1,1,1,1,2,2,2,2]);assert.equal(result.moving,0);assert.equal(result.unique,9);
});

for(const attackMove of [false,true])test(`${attackMove?'attack-move':'move'} blocked destinations recover near the original group command`,()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const units=Array.from({length:4},(_,i)=>addEntity('unit','rifleman',0,3,i+7,{moveReadyAt:testClock+500}));
    orderFormation(units.map(u=>u.id),20,15,0,${attackMove});addEntity('unit','engineer',0,20,15);
    for(let i=0;i<300;i++){testClock+=250;${attackMove?'updateAttackMoveOrders()':'updatePersistentOrders()'}}
    return {radii:units.map(u=>(u.x-20)**2+(u.y-15)**2).sort((a,b)=>a-b),moving:units.filter(u=>u.moveDestination||u.attackMoveDestination).length,unique:new Set(units.map(u=>u.x+','+u.y)).size};
  })()`);
  assert.deepEqual(result.radii,[1,1,1,1]);assert.equal(result.moving,0);assert.equal(result.unique,4);
});

for(const attackMove of [false,true])for(const obstacle of ['terrain','enemyWall','resource'])test(`${attackMove?'attack-move':'move'} replaces a stand made invalid by new ${obstacle}`,()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const units=Array.from({length:4},(_,i)=>addEntity('unit','rifleman',0,3,i+7,{moveReadyAt:testClock+500}));
    orderFormation(units.map(u=>u.id),20,15,0,${attackMove});
    if('${obstacle}'==='terrain')tileAt(20,15).blocked=true;
    else if('${obstacle}'==='enemyWall')addTemporaryObstacle({kind:'shieldWall',faction:1,x:20,y:15,hp:100,endAt:testClock+200000});
    else addNode('mineral',20,15,900);
    for(let i=0;i<300;i++){testClock+=250;${attackMove?'updateAttackMoveOrders()':'updatePersistentOrders()'}}
    return {radii:units.map(u=>(u.x-20)**2+(u.y-15)**2).sort((a,b)=>a-b),moving:units.filter(u=>u.moveDestination||u.attackMoveDestination).length,unique:new Set(units.map(u=>u.x+','+u.y)).size};
  })()`);
  assert.deepEqual(result.radii,[1,1,1,1]);assert.equal(result.moving,0);assert.equal(result.unique,4);
});

test('standalone AI mineral routes remain valid resource destinations',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    addEntity('structure',FACTIONS.frontier.base,0,10,10);addNode('mineral',13,10,900);const node=resourceNodes.at(-1),worker=addEntity('unit','engineer',0,5,10);worker.economyNodeId=node.id;
    issueMoveOrder(worker,node.x,node.y);for(let i=0;i<160;i++){testClock+=250;updatePersistentOrders();updateMining()}
    return {x:worker.x,y:worker.y,mining:worker.miningNodeId,economy:worker.economyNodeId,node:node.id};
  })()`);
  assert.equal(result.x,13);assert.equal(result.y,10);assert.equal(result.mining,result.node);assert.equal(result.economy,result.node);
});

test('queued group orders preserve a committed step and Stop clears compact formation metadata',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const units=[addEntity('unit','rifleman',0,3,4),addEntity('unit','engineer',0,3,6)];issueMoveOrder(units[0],10,4);
    const before={x:units[0].x,y:units[0].y,px:units[0].px,py:units[0].py,ready:units[0].moveReadyAt};
    moveGroup(units.map(u=>u.id),15,15,0);const after={x:units[0].x,y:units[0].y,px:units[0].px,py:units[0].py,ready:units[0].moveReadyAt};
    applyCommand({type:'groupStop',slot:0,entityIds:units.map(u=>u.id)});return {before,after,stopped:units.every(u=>!u.moveDestination&&!u.attackMoveDestination&&!u.formationAnchor&&!u.formationId&&u.order==='stop')};
  })()`);
  assert.deepEqual(result.after,result.before);assert.equal(result.stopped,true);
});

test('group movement rejects foreign and immobile troops and deduplicates accepted IDs',()=>{
  const run=simulation('frontier'),result=run.plain(`(()=>{
    const own=addEntity('unit','rifleman',0,3,4,{moveReadyAt:1e9}),foreign=addEntity('unit','rifleman',1,3,5),tank=addEntity('unit','siegeTank',0,3,6),fabricator=addEntity('unit','forwardFabricator',0,3,7);tank.siegeMode=true;fabricator.fabricatorDeployed=true;
    const accepted=moveGroup([own.id,own.id,foreign.id,tank.id,fabricator.id,999],15,15,0),onlyOwn=!!own.moveDestination&&!foreign.moveDestination&&!tank.moveDestination&&!fabricator.moveDestination;
    const before=JSON.stringify(own);players[0].eliminated=true;const rejected=moveGroup([own.id],20,20,0);return {accepted,onlyOwn,rejected,unchanged:before===JSON.stringify(own)};
  })()`);
  assert.equal(result.accepted,true);assert.equal(result.onlyOwn,true);assert.equal(result.rejected,false);assert.equal(result.unchanged,true);
});

test('ground reachability and aircraft destinations share a unique compact plan',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    for(let y=0;y<BOARD_H;y++)tileAt(10,y).blocked=true;
    const ground=addEntity('unit','rifleman',0,3,10,{moveReadyAt:1e9}),air=addEntity('unit','wraith',0,3,11,{moveReadyAt:1e9});
    moveGroup([air.id,ground.id],15,10,0);return {ground:ground.moveDestination,air:air.moveDestination};
  })()`);
  assert.deepEqual(result.air,{x:15,y:10});assert.deepEqual(result.ground,{x:9,y:10});
});

test('compact anchors and assigned stands survive multiplayer snapshots',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const units=[addEntity('unit','rifleman',0,3,4,{moveReadyAt:1e9}),addEntity('unit','engineer',0,3,5,{moveReadyAt:1e9})];
    moveGroup(units.map(u=>u.id),15,15,0);const before=units.map(u=>({id:u.id,stand:u.moveDestination,anchor:u.formationAnchor,formation:u.formationId}));
    applyRemoteState(JSON.parse(JSON.stringify(serializeState())));return {before,after:before.map(u=>{const e=entityById(u.id);return {id:e.id,stand:e.moveDestination,anchor:e.formationAnchor,formation:e.formationId}})};
  })()`);
  assert.deepEqual(result.after,result.before);
});
