const {test} = require('node:test');
const assert = require('node:assert/strict');
const {simulation} = require('./helpers/expansion-harness.cjs');

const UNITS = [
  ['frontier', 'suppressorExosuit', 'factory'],
  ['frontier', 'pulseOperative', 'barracks'],
  ['frontier', 'forwardFabricator', 'factory'],
  ['ascendant', 'latticeSkiff', 'stargate'],
  ['ascendant', 'chronoweaver', 'gateway'],
  ['ascendant', 'voidLancer', 'robotics'],
  ['brood', 'tunnelMaw', null],
  ['brood', 'sporeback', null],
  ['brood', 'broodMatriarch', null]
];

function ready(run, type, producer) {
  run(`(()=>{const type=${JSON.stringify(type)},d=UNITS[type];
    for(const required of d.requires||[])addEntity('structure',required,0,8,8);
    for(let i=0;i<8;i++)addEntity('structure',FACTIONS[players[0].factionId].supply,0,1+i,1);
    if(players[0].factionId==='ascendant')addEntity('structure','pylon',0,8,9);
    globalThis.subject=${producer ? `addEntity('structure',${JSON.stringify(producer)},0,8,8)` : `addEntity('larva','larva',0,8,8)`};
  })()`);
}

for (const [faction, type, producer] of UNITS) {
  test(`${type}: authoritative training spends its cost, reserves supply and produces the expected ID`, () => {
    const run = simulation(faction);
    ready(run, type, producer);
    const result = run.plain(`(()=>{const type=${JSON.stringify(type)},d=UNITS[type],before={minerals:players[0].minerals,gas:players[0].gas},id=subject.id;
      applyCommand({type:${JSON.stringify(producer ? 'train' : 'morph')},slot:0,entityId:id,unitType:type});
      const accepted=${producer ? `subject.queue.length===1&&subject.queue[0].type===type` : `subject.kind==='cocoon'&&subject.morphType===type`};
      const reserved=${producer ? 'queuedSupply(0)' : 'supplyUsed(0)'};
      const spent=[before.minerals-players[0].minerals,before.gas-players[0].gas];
      const snapshot=JSON.parse(JSON.stringify(serializeState()));applyRemoteState(snapshot);
      const restored=entityById(id),persisted=${producer ? `restored.queue[0]?.type===type` : `restored.morphType===type`};
      testClock+=scaledTime(d.time)*1000+10;updateProduction(.1);
      const built=entities.filter(e=>e.alive!==false&&e.kind==='unit'&&e.type===type);
      updateProduction(.1);return {accepted,spent,cost:d.cost,reserved,supply:d.supply,persisted,built:built.length,used:supplyUsed(0),sameId:${producer ? 'true' : 'built[0]?.id===id'},after:entities.filter(e=>e.kind==='unit'&&e.type===type&&e.alive!==false).length};})()`);
    assert.equal(result.accepted, true);
    assert.deepEqual(result.spent, result.cost);
    assert.equal(result.reserved, result.supply);
    assert.equal(result.persisted, true);
    assert.equal(result.built, 1);
    assert.equal(result.after, 1);
    assert.equal(result.used, result.supply);
    assert.equal(result.sameId, true);
  });

  test(`${type}: missing tech, foreign commands, supply and affordability reject without charges`, () => {
    for (const invalid of ['tech', 'owner', 'supply', 'minerals', 'gas']) {
      const run = simulation(faction);
      ready(run, type, producer);
      const result = run(`(()=>{const type=${JSON.stringify(type)},d=UNITS[type],invalid=${JSON.stringify(invalid)};
        if(invalid==='tech')for(const e of entities)if((d.requires||[]).includes(e.type))e.complete=false;
        if(invalid==='supply')for(let i=0;i<MAX_SUPPLY;i++)addEntity('unit',FACTIONS[players[0].factionId].worker,0,35,25);
        if(invalid==='minerals')players[0].minerals=d.cost[0]-1;
        if(invalid==='gas')players[0].gas=d.cost[1]-1;
        const before=[players[0].minerals,players[0].gas];
        applyCommand({type:${JSON.stringify(producer ? 'train' : 'morph')},slot:invalid==='owner'?1:0,entityId:subject.id,unitType:type});
        return {accepted:${producer ? 'subject.queue.length>0' : "subject.kind==='cocoon'"},unchanged:before[0]===players[0].minerals&&before[1]===players[0].gas};})()`);
      assert.equal(result.accepted, false, invalid);
      assert.equal(result.unchanged, true, invalid);
    }
  });

  test(`${type}: move, hold, stop, attack-move and faction defensive research remain usable`, () => {
    const run = simulation(faction);
    const result = run(`(()=>{const unit=addEntity('unit',${JSON.stringify(type)},0,3,5),d=UNITS[unit.type];
      applyCommand({type:'move',slot:0,entityId:unit.id,x:6,y:5});testClock+=1000;updatePersistentOrders();
      const moved=unit.x!==3||unit.y!==5;
      applyCommand({type:'hold',slot:0,entityId:unit.id});const held=unit.order==='hold'&&!unit.moveDestination;
      applyCommand({type:'attackMove',slot:0,entityId:unit.id,x:10,y:5});const attackMove=!!unit.attackMoveDestination;
      applyCommand({type:'stop',slot:0,entityId:unit.id});const stopped=!unit.moveDestination&&!unit.attackMoveDestination&&!unit.attackMode;
      players[0].research.defensiveSystems=1;applyResearchStats(unit,false);
      const research=players[0].factionId==='ascendant'?unit.maxShield>d.shield:unit.maxHp>d.hp;
      return {moved,held,attackMove,stopped,research};})()`);
    for (const [key, value] of Object.entries(result)) assert.equal(value, true, key);
  });
}

test('new unit queues use the established cancellation refund and reject the wrong producer', () => {
  for (const [faction, type, producer] of UNITS.filter(row => row[2])) {
    const run = simulation(faction);
    ready(run, type, producer);
    const result = run.plain(`(()=>{const d=UNITS[${JSON.stringify(type)}],before=[players[0].minerals,players[0].gas];
      const wrong=addEntity('structure',FACTIONS[players[0].factionId].base,0,15,15),rejected=!queueUnit(wrong,${JSON.stringify(type)});
      queueUnit(subject,${JSON.stringify(type)});applyCommand({type:'cancelQueue',slot:0,entityId:subject.id});
      return {rejected,queue:subject.queue.length,spent:[before[0]-players[0].minerals,before[1]-players[0].gas],net:d.cost.map(c=>c-Math.round(c*.75))};})()`);
    assert.equal(result.rejected, true, type);
    assert.equal(result.queue, 0, type);
    assert.deepEqual(result.spent, result.net, type);
  }
});

test('multiplayer clients submit abilities once and the controller deduplicates repeated command delivery', async () => {
  const run = simulation('brood');
  const id = run(`(()=>{const unit=addEntity('unit','broodMatriarch',0,8,8);lobbyId='qa-lobby';isController=false;return unit.id})()`);
  await run(`sendCommand({type:'ability',entityId:${id},abilityId:'bloodClutch'})`);
  assert.equal(run("entities.filter(e=>e.type==='broodling'&&e.alive!==false).length"), 0);
  assert.equal(run.network.writes.length, 1);
  const command = run.network.writes[0].data;
  assert.equal(command.entityId, id);
  assert.equal(command.slot, 0);
  run('isController=true;manageCommandListener()');
  const change = {type: 'added', doc: {id: command.id, data: () => command}};
  run.network.listener({docChanges: () => [change, change]});
  assert.equal(run("entities.filter(e=>e.type==='broodling'&&e.alive!==false).length"), 3);
  assert.equal(run(`entityById(${id}).hp`), run('UNITS.broodMatriarch.hp-25'));
  const snapshot = run.plain('serializeState()');
  assert.equal(snapshot.acks.filter(ack => ack === command.id).length, 1);
  run.network.listener({docChanges: () => [change]});
  assert.equal(run("entities.filter(e=>e.type==='broodling'&&e.alive!==false).length"), 3);
  const remote = simulation('brood');
  remote.context.incomingState = snapshot;
  remote('applyRemoteState(incomingState)');
  assert.deepEqual(remote.plain('entities.map(e=>[e.id,e.type,e.abilityReadyAt,e.temporarySummon,e.expiresAt])'), run.plain('entities.map(e=>[e.id,e.type,e.abilityReadyAt,e.temporarySummon,e.expiresAt])'));
  assert.equal(remote(`addEntity('unit','drone',0,1,1).id`), Math.max(...snapshot.entities.map(e => e.id)) + 1);
});

test('temporary Broodlings and tunnel exits cannot capture Conquest or carry the Central Relic', () => {
  const run = simulation('brood');
  const result = run(`(()=>{chosenGameMode='conquest';initializeObjectiveState();const site=modeState.conquest[0];
    const matriarch=addEntity('unit','broodMatriarch',0,site.x-5,site.y);useAbility(matriarch,'bloodClutch');
    const summons=entities.filter(e=>e.type==='broodling');for(const e of summons){e.x=site.x;e.y=site.y}
    const exit=addEntity('structure','tunnelExit',0,site.x,site.y);exit.temporaryLogistics=true;exit.tunnelSourceId=matriarch.id;exit.expiresAt=testClock+15000;
    updateObjectives(1);const temporaryScore=modeState.scores[0],sole=soleController(site.x,site.y,2.5),structures=playerStructures(0).includes(exit);
    const real=addEntity('unit','roach',0,site.x,site.y);updateObjectives(1);const realScore=modeState.scores[0];real.alive=false;
    chosenGameMode='relic';initializeObjectiveState();for(const e of summons){e.x=modeState.relic.x;e.y=modeState.relic.y}exit.x=modeState.relic.x;exit.y=modeState.relic.y;
    updateObjectives(.1);const picked=modeState.relic.carrierId;
    modeState.relic.carrierId=summons[0].id;updateObjectives(.1);return {summons:summons.length,temporaryScore,sole,structures,realScore,picked,forged:modeState.relic.carrierId};})()`);
  assert.equal(result.summons, 3);
  assert.equal(result.temporaryScore, 0);
  assert.equal(result.sole, null);
  assert.equal(result.structures, false);
  assert.equal(result.realScore, 1);
  assert.equal(result.picked, null);
  assert.equal(result.forged, null);
});

test('stale multiplayer snapshots cannot roll back a spawned swarm or its active cooldown', () => {
  const run = simulation('brood');
  const result = run(`(()=>{const m=addEntity('unit','broodMatriarch',0,8,8),id=m.id,old=JSON.parse(JSON.stringify(serializeState()));
    useAbility(m,'bloodClutch');const current=JSON.parse(JSON.stringify(serializeState())),ready=m.abilityReadyAt;
    applyRemoteState(current);applyRemoteState(old);return {summons:entities.filter(e=>e.type==='broodling').length,ready:entityById(id).abilityReadyAt,expected:ready,tick:simulationTick,expectedTick:current.simulationTick};})()`);
  assert.equal(result.summons, 3);
  assert.equal(result.ready, result.expected);
  assert.equal(result.tick, result.expectedTick);
});

test('missing entities in train and morph commands reject safely without changing the economy', () => {
  const run = simulation('brood');
  const before = run.plain('players.map(p=>[p.minerals,p.gas])');
  assert.doesNotThrow(() => run("applyCommand({type:'train',slot:0,entityId:99999,unitType:'sporeback'})"));
  assert.doesNotThrow(() => run("applyCommand({type:'morph',slot:0,entityId:99999,unitType:'sporeback'})"));
  assert.deepEqual(run.plain('players.map(p=>[p.minerals,p.gas])'), before);
});

test('Disruption Pulse prevents all four logistics abilities from bypassing the shared ability lock', () => {
  for (const [faction, type, ability, payload, setup] of [
    ['frontier', 'forwardFabricator', 'fieldMuster', '{}', ''],
    ['ascendant', 'latticeSkiff', 'warpConduit', '{x:14,y:8}', "addEntity('structure','pylon',0,14,8);addEntity('unit','zealot',0,13,8);"],
    ['brood', 'tunnelMaw', 'tunnelBreach', '{x:14,y:8}', "addEntity('structure','hatchery',0,8,7);"],
    ['brood', 'broodMatriarch', 'bloodClutch', '{}', '']
  ]) {
    const run = simulation(faction, {factions: [faction, 'frontier', 'brood']});
    const result = run(`(()=>{const unit=addEntity('unit',${JSON.stringify(type)},0,8,8);${setup}
      const op=addEntity('unit','pulseOperative',1,10,8),hit=useAbility(op,'disruptionPulse',{x:8,y:8});
      const hp=unit.hp,accepted=useAbility(unit,${JSON.stringify(ability)},${payload});
      return {hit,locked:unit.abilityLockedUntil>testClock,accepted,hpUnchanged:unit.hp===hp,cooldown:unit.abilityReadyAt};})()`);
    assert.equal(result.hit, true, type);
    assert.equal(result.locked, true, type);
    assert.equal(result.accepted, false, type);
    assert.equal(result.hpUnchanged, true, type);
    assert.equal(result.cooldown, 0, type);
  }
});

test('an active Warp Conduit survives host migration without duplicate troops or noncontroller movement', () => {
  const run = simulation('ascendant');
  const result = run.plain(`(()=>{const skiff=addEntity('unit','latticeSkiff',0,8,8),pylon=addEntity('structure','pylon',0,14,8),first=addEntity('unit','zealot',0,13,8),second=addEntity('unit','dragoon',0,14,7);
    const accepted=useAbility(skiff,'warpConduit',{x:pylon.x,y:pylon.y}),ids=[first.id,second.id],snapshot=JSON.parse(JSON.stringify(serializeState()));
    const saved=snapshot.entities.find(e=>e.id===skiff.id),effect=snapshot.areaEffects.find(a=>a.kind==='warpConduit');
    applyRemoteState(snapshot);lobbyId='migration';isController=false;testClock+=2001;updateExpansionLogistics();
    const clientStill=ids.every(id=>dist(entityById(id),pylon)<=2.1);
    isController=true;updateExpansionLogistics();const landed=ids.map(id=>({id,x:entityById(id).x,y:entityById(id).y}));updateExpansionLogistics();
    return {accepted,savedTargets:saved.warpChannel.targetIds,effectTargets:effect.targetIds,ids,clientStill,channelDone:!entityById(skiff.id).warpChannel,landed,unique:new Set(landed.map(p=>p.x+','+p.y)).size,troops:entities.filter(e=>e.kind==='unit').length,nearSkiff:ids.every(id=>dist(entityById(id),skiff)<=2.9)};})()`);
  assert.equal(result.accepted, true);
  assert.deepEqual(result.savedTargets, result.ids);
  assert.deepEqual(result.effectTargets, result.ids);
  assert.equal(result.clientStill, true);
  assert.equal(result.channelDone, true);
  assert.equal(result.unique, 2);
  assert.equal(result.troops, 3);
  assert.equal(result.nearSkiff, true);
});
