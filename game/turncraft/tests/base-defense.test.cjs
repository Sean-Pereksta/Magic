const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

function raid(faction='frontier'){
  const run=simulation(faction,{factions:[faction,'frontier','brood']});
  const base={frontier:'commandHub',ascendant:'nexus',brood:'hatchery'}[faction];
  const troop={frontier:'rifleman',ascendant:'dragoon',brood:'hydralisk'}[faction];
  run(`players[0].ai=true;chosenDifficulty='easy';
    globalThis.home=addEntity('structure','${base}',0,6,10);
    globalThis.defender=addEntity('unit','${troop}',0,8,13);
    globalThis.raider=addEntity('unit','siegeTank',1,14,10);raider.siegeMode=true;
    issueAttack(raider,home);`);
  return run;
}

test('all faction armies intercept ranged base attackers outside the old five-tile trigger',()=>{
  for(const faction of ['frontier','ascendant','brood']){
    const run=raid(faction);
    run(`aiArmyCycle(0,playerUnits(0).filter(u=>!isWorker(u)),DIFFICULTY.easy)`);
    assert.equal(run(`players[0].strategy.state`),'defending',faction);
    assert.equal(run(`defender.directTargetId===raider.id&&defender.attackMode`),true,faction);
    const before=run(`raider.hp`);
    run.advance(30);
    assert.ok(run(`raider.hp`)<before,`${faction} troops must actually approach and damage the attacker`);
  }
});

test('production buildings and construction sites receive troop defense without pulling workers',()=>{
  const run=simulation();
  run(`players[0].ai=true;addEntity('structure','commandHub',0,3,3);
    globalThis.site=addEntity('structure','barracks',0,20,10);site.complete=false;
    globalThis.worker=addEntity('unit','engineer',0,19,10);worker.buildTargetId=site.id;worker.order='building';
    globalThis.defender=addEntity('unit','rifleman',0,21,14);
    globalThis.raider=addEntity('unit','roach',2,25,10);
    aiReactToBaseRaid(0,playerUnits(0));`);
  assert.equal(run(`defender.directTargetId===raider.id`),true);
  assert.equal(run(`worker.buildTargetId===site.id&&worker.order==='building'&&!worker.directTargetId`),true);
});

test('recent base and structure hits recall troops even after the attacker backs outside the perimeter',()=>{
  for(const type of ['commandHub','barracks']){
    const run=simulation();
    run(`players[0].ai=true;globalThis.asset=addEntity('structure','${type}',0,6,10);
      globalThis.defender=addEntity('unit','rifleman',0,8,13);
      globalThis.raider=addEntity('unit','siegeTank',1,16,10);
      recordThreat(asset,raider,20);aiReactToBaseRaid(0,playerUnits(0));`);
    assert.equal(run(`defender.directTargetId===raider.id`),true,type);
    assert.equal(run(`players[0].lastDefense`),0,type);
  }
});

test('defensive responses run before slow macro ticks and preserve existing action cooldowns',()=>{
  const run=raid();
  run(`players[0].lastAi=0;performance.now=()=>1000;
    defender.moveReadyAt=testClock+5000;defender.attackReadyAt=testClock+6000;defender.abilityReadyAt=testClock+7000;
    globalThis.originalTimers=[defender.moveReadyAt,defender.attackReadyAt,defender.abilityReadyAt];
    aiThinkAll();updatePersistentOrders();updateCombat(.1);`);
  assert.equal(run(`players[0].lastAi`),0,'the easy macro tick should still be waiting');
  assert.equal(run(`defender.directTargetId===raider.id`),true);
  assert.deepEqual(run.plain(`[defender.moveReadyAt,defender.attackReadyAt,defender.abilityReadyAt]`),run.plain(`originalTimers`));
  assert.deepEqual(run.plain(`({x:defender.x,y:defender.y})`),{x:8,y:13});
  run.advance(5.1);
  assert.ok(run(`defender.x!==8||defender.y!==13`),'the next move begins after its existing timer expires');
});

test('direct defense pursuit follows a moving attacker rather than a stale attack-move coordinate',()=>{
  const run=raid();
  run(`aiReactToBaseRaid(0,playerUnits(0));raider.x=13;raider.y=12;
    defender.moveReadyAt=testClock+5000;aiReactToBaseRaid(0,playerUnits(0));updateCombat(.1);`);
  assert.equal(run(`defender.directTargetId===raider.id`),true);
  assert.deepEqual(run.plain(`[defender.lastKnownTargetX,defender.lastKnownTargetY]`),[13,12]);
  assert.equal(run(`defender.attackMoveDestination`),null);
});

test('air raids recruit compatible weapons and leave melee, channels and deployed logistics intact',()=>{
  const run=simulation();
  run(`players[0].ai=true;addEntity('structure','commandHub',0,6,10);
    globalThis.melee=addEntity('unit','zealot',0,8,11);
    globalThis.antiAir=addEntity('unit','goliath',0,10,12);
    globalThis.channel=addEntity('unit','pulseOperative',0,9,11);channel.channelEffectId='active_channel';
    globalThis.fabricator=addEntity('unit','forwardFabricator',0,10,11);fabricator.fabricatorDeployed=true;
    globalThis.raider=addEntity('unit','wraith',1,13,10);
    aiReactToBaseRaid(0,playerUnits(0));`);
  assert.equal(run(`antiAir.directTargetId===raider.id`),true);
  assert.equal(run(`!melee.aiDefenseId&&!melee.directTargetId`),true);
  assert.equal(run(`channel.channelEffectId`),'active_channel');
  assert.equal(run(`fabricator.fabricatorDeployed&&!fabricator.directTargetId`),true);
});

test('a deployed siege defender selects an attacker it can fire on without canceling its siege action',()=>{
  const run=simulation();
  run(`players[0].ai=true;globalThis.home=addEntity('structure','commandHub',0,6,10);
    globalThis.siege=addEntity('unit','siegeTank',0,7,13);siege.siegeMode=true;
    globalThis.near=addEntity('unit','roach',2,11,12);
    globalThis.far=addEntity('unit','siegeTank',1,15,10);
    recordThreat(home,far,35);aiReactToBaseRaid(0,playerUnits(0));`);
  assert.equal(run(`siege.directTargetId===near.id&&siege.siegeMode`),true);
  run(`updateCombat(.1)`);
  assert.equal(run(`pendingImpacts.some(hit=>hit.attackerId===siege.id&&hit.targetId===near.id)`),true);
});

test('local defense keeps a distant offensive wave when enough troops already protect home',()=>{
  const run=raid();
  run(`globalThis.local=addEntity('unit','battlecruiser',0,10,13);
    globalThis.offense=[];for(let i=0;i<5;i++)offense.push(addEntity('unit','battlecruiser',0,35+i,24));
    players[0].strategy.wave={ids:offense.map(u=>u.id),initialPower:armyPower(offense),startedAt:testClock,target:{x:40,y:20}};
    for(const u of offense)issueAttackMoveOrder(u,40,20);
    globalThis.originalWave=players[0].strategy.wave.ids.slice();
    aiReactToBaseRaid(0,playerUnits(0));`);
  assert.deepEqual(run.plain(`players[0].strategy.wave.ids`),run.plain(`originalWave`));
  assert.equal(run(`local.directTargetId===raider.id`),true);
  assert.equal(run(`offense.every(u=>!u.aiDefenseId&&u.attackMoveDestination)`),true);
});

test('strong raids recall the nearest necessary reinforcements and retain the remaining wave',()=>{
  const run=simulation();
  run(`players[0].ai=true;addEntity('structure','commandHub',0,6,10);
    globalThis.close=addEntity('unit','rifleman',0,8,13);
    globalThis.reinforcements=[];for(let i=0;i<6;i++)reinforcements.push(addEntity('unit','battlecruiser',0,21+i*3,23));
    players[0].strategy.wave={ids:reinforcements.map(u=>u.id),initialPower:armyPower(reinforcements),startedAt:testClock,target:{x:40,y:20}};
    globalThis.raider=addEntity('unit','ultralisk',2,13,10);
    aiReactToBaseRaid(0,playerUnits(0));`);
  assert.equal(run(`reinforcements[0].directTargetId===raider.id`),true);
  assert.ok(run(`players[0].strategy.wave.ids.length`)>0);
  assert.ok(run(`players[0].strategy.wave.ids.length`)<6);
});

test('fog damage defends the known asset without exposing a hidden attacker',()=>{
  const run=raid();
  run(`recordThreat(home,raider,30);tileVisibleToFaction=()=>false;aiReactToBaseRaid(0,playerUnits(0));`);
  assert.equal(run(`defender.directTargetId`),null);
  assert.equal(run(`defender.aiDefenseId==='guard_'+home.id`),true);
  assert.equal(run(`players[0].strategy.defenseTarget.id`),null);
  assert.ok(run(`dist(defender.attackMoveDestination,home)`)<4);
  assert.notDeepEqual(run.plain(`defender.attackMoveDestination`),{x:14,y:10});
});

test('expired threats release defenders to their base and do not trigger unrelated distant combat',()=>{
  const run=raid();
  run(`recordThreat(home,raider,30);aiReactToBaseRaid(0,playerUnits(0));
    raider.x=30;raider.y=20;testClock+=10000;aiReactToBaseRaid(0,playerUnits(0));`);
  assert.equal(run(`players[0].strategy.defenseTarget`),null);
  assert.equal(run(`defender.aiDefenseId`),undefined);
  assert.equal(run(`defender.directTargetId`),null);
  assert.equal(run(`defender.order`),'move');
  assert.deepEqual(run.plain(`defender.moveDestination`),{x:6,y:10});
});
