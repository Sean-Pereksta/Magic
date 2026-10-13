const test=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

function fabricatorGame(){
  const run=simulation('frontier');
  run(`addEntity('structure','academy',0,5,5);addEntity('structure','supplyDepot',0,5,6);
    globalThis.fabricator=addEntity('unit','forwardFabricator',0,15,15);`);
  return run;
}
function warpGame(){
  const run=simulation('ascendant');
  run(`globalThis.skiff=addEntity('unit','latticeSkiff',0,15,15);
    globalThis.pylon=addEntity('structure','pylon',0,9,15);
    globalThis.first=addEntity('unit','zealot',0,8,15);
    globalThis.second=addEntity('unit','dragoon',0,9,16);`);
  return run;
}
function tunnelGame(){
  const run=simulation('brood');
  run(`addEntity('structure','hatchery',0,5,5);
    globalThis.maw=addEntity('unit','tunnelMaw',0,8,5);
    globalThis.traveler=addEntity('unit','zergling',0,8,6);`);
  return run;
}

test('Field Muster deploys for three seconds, reserves supply, surcharges training, and packs with normal refunds',()=>{
  const run=fabricatorGame();
  assert.equal(run(`useAbility(fabricator,'fieldMuster')`),true);
  assert.equal(run(`issueMoveOrder(fabricator,20,15)`),false);
  assert.equal(run(`queueUnit(fabricator,'rifleman')`),false);
  run(`testClock+=2999;updateExpansionLogistics()`);
  assert.equal(run(`!!fabricator.fabricatorDeployed`),false);
  run(`testClock+=1;updateExpansionLogistics()`);
  assert.equal(run(`fabricator.fabricatorDeployed`),true);
  assert.equal(run(`queueUnit(fabricator,'medic')`),true);
  assert.deepEqual(run.plain(`fabricator.queue[0].cost`),[90,60]);
  assert.equal(run(`fabricator.queue[0].finishAt-fabricator.queue[0].startAt`),run(`scaledTime(UNITS.medic.time)*1250`));
  assert.equal(run(`queuedSupply(0)`),1);
  assert.equal(run(`queueUnit(fabricator,'siegeTank')`),false);
  assert.equal(run(`setStructureRally(fabricator,22,15)`),true);
  run(`testClock=fabricator.queue[0].finishAt;updateExpansionLogistics()`);
  assert.equal(run(`playerUnits(0).filter(u=>u.type==='medic').length`),1);
  assert.equal(run(`playerUnits(0).find(u=>u.type==='medic').moveDestination.x`),22);
  assert.equal(run(`queueUnit(fabricator,'rifleman')`),true);
  const before=run(`players[0].minerals`);
  assert.equal(run(`useAbility(fabricator,'fieldMuster')`),true);
  assert.equal(run(`players[0].minerals`),before+68);
  assert.equal(run(`queuedSupply(0)`),0);
});

test('Field Muster uses paid cancellation cost and rejects protected or enclosed deployment',()=>{
  const run=fabricatorGame();
  run(`modeState={conquest:[{x:15,y:15}],relic:null}`);
  assert.equal(run(`fabricatorCanDeploy(fabricator)`),false);
  run(`modeState=null;globalThis.enemyBase=addEntity('structure','nexus',1,17,15)`);
  assert.equal(run(`fabricatorCanDeploy(fabricator)`),false);
  run(`enemyBase.alive=false;useAbility(fabricator,'fieldMuster');testClock+=3000;updateExpansionLogistics();queueUnit(fabricator,'medic');globalThis.beforePack=players[0].minerals;`);
  assert.equal(run(`packFabricator(fabricator)`),true);
  assert.equal(run(`players[0].minerals-beforePack`),68);
  assert.equal(run(`players[0].gas`),4985);
  assert.equal(run(`fabricator.queue.length`),0);
  assert.equal(run(`fabricator.fabricatorDeployed`),false);
  run(`fabricator.abilityReadyAt=0;for(let y=13;y<=17;y++)for(let x=13;x<=17;x++)if(x!==15||y!==15)addEntity('unit','rifleman',0,x,y)`);
  assert.equal(run(`fabricatorCanDeploy(fabricator)`),false);
});

test('Warp Conduit validates a friendly Pylon and channels into distinct safe landings',()=>{
  const run=warpGame();
  run(`globalThis.worker=addEntity('unit','probe',0,8,16);globalThis.massive=addEntity('unit','carrier',0,9,14);
    globalThis.relicCarrier=addEntity('unit','zealot',0,10,15);modeState={conquest:[],relic:{carrierId:relicCarrier.id,x:10,y:15}};`);
  assert.deepEqual(run.plain(`warpConduitPreview(skiff,pylon).troops.map(u=>u.id)`),run.plain(`[first.id,second.id]`));
  assert.equal(run(`useAbility(skiff,'warpConduit',{x:pylon.x,y:pylon.y})`),true);
  const original=run.plain(`[first.x,first.y,second.x,second.y]`);
  assert.equal(run(`moveEntity(skiff,16,15)`),false);
  run(`testClock+=1999;updateExpansionLogistics()`);
  assert.deepEqual(run.plain(`[first.x,first.y,second.x,second.y]`),original);
  run(`testClock+=1;updateExpansionLogistics()`);
  assert.equal(run(`dist(first,skiff)<=2.83&&dist(second,skiff)<=2.83`),true);
  assert.equal(run(`first.x!==second.x||first.y!==second.y`),true);
  assert.equal(run(`!!skiff.warpChannel`),false);
  assert.deepEqual(run.plain(`[worker.x,worker.y,relicCarrier.x,relicCarrier.y]`),[8,16,10,15]);
});

test('Warp Conduit rechecks Pylon availability and late landing occupancy without overwriting units',()=>{
  const run=warpGame();
  run(`useAbility(skiff,'warpConduit',{targetId:pylon.id});pylon.complete=false;testClock+=2000;updateExpansionLogistics()`);
  assert.deepEqual(run.plain(`[first.x,first.y]`),[8,15]);
  assert.equal(run(`!!skiff.warpChannel`),false);
  run(`pylon.complete=true;skiff.abilityReadyAt=0;useAbility(skiff,'warpConduit',{targetId:pylon.id});
    for(let y=13;y<=17;y++)for(let x=13;x<=17;x++)if(x!==15||y!==15)addEntity('unit','zealot',0,x,y);
    testClock+=2000;updateExpansionLogistics()`);
  assert.deepEqual(run.plain(`[first.x,first.y,second.x,second.y]`),[8,15,9,16]);
  assert.equal(run(`!!skiff.warpChannel`),false);
});

test('Tunnel Breach uses creep, a three-second telegraph, real adjacent travelers and five-use lifetime',()=>{
  const run=tunnelGame();
  assert.equal(run(`useAbility(maw,'tunnelBreach',{x:14,y:5})`),true);
  assert.equal(run(`issueMoveOrder(maw,9,5)`),false);
  run(`testClock+=2999;updateExpansionLogistics()`);
  assert.equal(run(`!!maw.tunnelExitId`),false);
  run(`testClock+=1;updateExpansionLogistics()`);
  assert.equal(run(`entityById(maw.tunnelExitId).type`),'tunnelExit');
  assert.equal(run(`maw.tunnelRemainingUses`),5);
  assert.equal(run(`tunnelTravelUnit(maw,traveler)`),true);
  assert.equal(run(`dist(traveler,{x:14,y:5})<=2.83`),true);
  assert.equal(run(`maw.tunnelRemainingUses`),4);
  assert.equal(run(`tunnelTravelUnit(maw,traveler)`),false);
  run(`testClock+=500;globalThis.worker=addEntity('unit','drone',0,8,6);globalThis.massive=addEntity('unit','ultralisk',0,7,5)`);
  assert.equal(run(`tunnelTravelUnit(maw,worker)`),false);
  assert.equal(run(`tunnelTravelUnit(maw,massive)`),false);
  run(`testClock=maw.tunnelExpiresAt;updateExpansionLogistics()`);
  assert.equal(run(`!!maw.tunnelExitId`),false);
  assert.equal(run(`entities.some(e=>e.alive!==false&&e.type==='tunnelExit')`),false);
});

test('Tunnel Breach rejects non-creep, occupied exits, protected objectives and more than two connections',()=>{
  const run=tunnelGame();
  run(`maw.x=20;maw.y=20`);
  assert.equal(run(`useAbility(maw,'tunnelBreach',{x:24,y:20})`),false);
  run(`maw.x=8;maw.y=5;modeState={conquest:[{x:14,y:5}],relic:null}`);
  assert.equal(run(`useAbility(maw,'tunnelBreach',{x:14,y:5})`),false);
  run(`modeState=null;globalThis.other=addEntity('unit','tunnelMaw',0,7,5);globalThis.third=addEntity('unit','tunnelMaw',0,6,5)`);
  assert.equal(run(`useAbility(maw,'tunnelBreach',{x:14,y:5})`),true);
  assert.equal(run(`useAbility(other,'tunnelBreach',{x:13,y:7})`),true);
  assert.equal(run(`useAbility(third,'tunnelBreach',{x:12,y:3})`),false);
  run(`addEntity('unit','zergling',0,14,5);testClock+=3000;updateExpansionLogistics()`);
  assert.equal(run(`!!maw.tunnelExitId`),false);
  assert.equal(run(`!!maw.tunnelDigging`),false);
});

test('Tunnel exit destruction, source death and stop commands release connections immediately',()=>{
  const run=tunnelGame();
  run(`useAbility(maw,'tunnelBreach',{x:14,y:5});testClock+=3000;updateExpansionLogistics();killEntity(entityById(maw.tunnelExitId))`);
  assert.equal(run(`!!maw.tunnelExitId`),false);
  run(`maw.abilityReadyAt=0;useAbility(maw,'tunnelBreach',{x:14,y:5});applyCommand({type:'stop',entityId:maw.id,slot:0})`);
  assert.equal(run(`!!maw.tunnelDigging`),false);
  run(`maw.abilityReadyAt=0;useAbility(maw,'tunnelBreach',{x:14,y:5});testClock+=3000;updateExpansionLogistics();killEntity(maw)`);
  assert.equal(run(`entities.some(e=>e.alive!==false&&e.type==='tunnelExit')`),false);
});

test('Blood Clutch sacrifices exactly 25 HP, caps six supply-free summons and expires without refunds',()=>{
  const run=simulation('brood');
  run(`globalThis.matriarch=addEntity('unit','broodMatriarch',0,15,15);globalThis.initialHp=matriarch.hp;globalThis.initialSupply=supplyUsed(0)`);
  assert.equal(run(`useAbility(matriarch,'bloodClutch')`),true);
  assert.equal(run(`matriarch.hp`),run(`initialHp-25`));
  assert.equal(run(`playerUnits(0).filter(u=>u.type==='broodling').length`),3);
  assert.equal(run(`supplyUsed(0)`),run(`initialSupply`));
  run(`matriarch.abilityReadyAt=0`);
  assert.equal(run(`useAbility(matriarch,'bloodClutch')`),true);
  run(`matriarch.abilityReadyAt=0;globalThis.hpBefore=matriarch.hp;globalThis.cooldownBefore=matriarch.abilityReadyAt`);
  assert.equal(run(`useAbility(matriarch,'bloodClutch')`),false);
  assert.equal(run(`matriarch.hp`),run(`hpBefore`));
  assert.equal(run(`matriarch.abilityReadyAt`),run(`cooldownBefore`));
  run(`testClock+=10000;updateExpansionLogistics()`);
  assert.equal(run(`playerUnits(0).filter(u=>u.type==='broodling').length`),0);
  assert.deepEqual(run.plain(`[players[0].minerals,players[0].gas]`),[5000,5000]);
});

test('Blood Clutch rejects lethal health or insufficient room atomically and cannot transport its summons',()=>{
  const run=simulation('brood');
  run(`globalThis.matriarch=addEntity('unit','broodMatriarch',0,15,15);matriarch.hp=25`);
  assert.equal(run(`useAbility(matriarch,'bloodClutch')`),false);
  run(`matriarch.hp=100;for(let y=14;y<=16;y++)for(let x=14;x<=16;x++)if(x!==15||y!==15)addEntity('unit','zergling',0,x,y)`);
  assert.equal(run(`useAbility(matriarch,'bloodClutch')`),false);
  assert.equal(run(`matriarch.hp`),100);
  run(`entities.filter(e=>e.type==='zergling').forEach(e=>e.alive=false);useAbility(matriarch,'bloodClutch');globalThis.summon=playerUnits(0).find(e=>e.type==='broodling');globalThis.maw=addEntity('unit','tunnelMaw',0,18,15);globalThis.ship=addEntity('unit','dropship',0,19,15)`);
  assert.equal(run(`logisticsGroundPassenger(summon,maw)`),false);
  assert.equal(run(`validTransportPassenger(summon,ship)`),false);
  assert.equal(run(`bunkerForUnit(summon)`),null);
  assert.equal(run(`morphLarva(summon,'zergling')`),false);
  run(`killEntity(matriarch)`);
  assert.equal(run(`playerUnits(0).filter(u=>u.type==='broodling').length`),0);
});

test('Disruption forbids logistics casts and interrupts an existing Warp Conduit channel',()=>{
  const run=warpGame();
  run(`skiff.abilityLockedUntil=testClock+4000`);
  assert.equal(run(`useAbility(skiff,'warpConduit',{targetId:pylon.id})`),false);
  assert.deepEqual(run.plain(`legalAbilityTiles(skiff,'warpConduit')`),[]);
  run(`skiff.abilityLockedUntil=0;useAbility(skiff,'warpConduit',{targetId:pylon.id});skiff.abilityLockedUntil=testClock+4000;expansionCancelChannel(skiff);testClock+=2000;updateExpansionLogistics()`);
  assert.equal(run(`!!skiff.warpChannel`),false);
  assert.deepEqual(run.plain(`[first.x,first.y]`),[8,15]);
});


test('Five adjacent combat troops can use one tunnel together, closing it before a sixth',()=>{
  const run=tunnelGame();
  run(`globalThis.group=[traveler,...[[7,4],[8,4],[9,4],[9,5],[9,6]].map(([x,y])=>addEntity('unit','zergling',0,x,y))];useAbility(maw,'tunnelBreach',{x:14,y:5});testClock+=3000;updateExpansionLogistics()`);
  assert.deepEqual(run.plain(`group.map(u=>tunnelTravelUnit(maw,u))`),[true,true,true,true,true,false]);
  assert.equal(run(`!!maw.tunnelExitId`),false);
  assert.equal(run(`entities.some(e=>e.alive!==false&&e.type==='tunnelExit')`),false);
  assert.deepEqual(run.plain(`[group[5].x,group[5].y]`),[9,6]);
});
