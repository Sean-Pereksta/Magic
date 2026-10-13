const assert=require('node:assert/strict');
const test=require('node:test');
const {simulation}=require('./helpers/expansion-harness.cjs');

function game(faction='frontier'){
  const fragments=process.env.TURNCRAFT_COMBAT_FRAGMENTS?process.env.TURNCRAFT_COMBAT_FRAGMENTS.split(':'):undefined;
  return simulation(faction,{fragments});
}

test('Suppression Sweep hits only visible enemy ground units in its forward cone',()=>{
  const run=game();
  run(`globalThis.suit=addEntity('unit','suppressorExosuit',0,10,10);
    globalThis.front=addEntity('unit','rifleman',1,12,10);globalThis.flank=addEntity('unit','rifleman',1,10,13);
    globalThis.far=addEntity('unit','rifleman',1,15,10);globalThis.air=addEntity('unit','wraith',1,12,11);
    globalThis.friend=addEntity('unit','rifleman',0,13,10);globalThis.hidden=addEntity('unit','rifleman',1,13,11);
    tileVisibleToFaction=(f,x,y)=>!(x===13&&y===11);
    globalThis.before=entities.map(e=>e.hp);`);
  assert.equal(run(`useAbility(suit,'suppressionSweep',{x:14,y:10})`),true);
  assert.equal(run(`before[1]-front.hp`),12);
  assert.equal(run(`front.suppressedUntil-testClock`),5000);
  assert.deepEqual(run.plain(`entities.slice(2).map((e,i)=>before[i+2]-e.hp)`),[0,0,0,0,0]);
  assert.equal(run(`useAbility(suit,'suppressionSweep',{x:14,y:10})`),false);
});

test('Suppression and overlapping temporal fields slow already running timers once, without movement or HP loss',()=>{
  const run=game('ascendant');
  run(`globalThis.first=addEntity('unit','chronoweaver',0,10,10);globalThis.second=addEntity('unit','chronoweaver',0,11,10);
    globalThis.target=addEntity('unit','siegeTank',1,13,10);target.attackReadyAt=testClock+4000;target.abilityReadyAt=testClock+10000;
    target.moveReadyAt=testClock+3000;globalThis.hp=target.hp;globalThis.moveCd=effectiveMoveCd(target);
    useAbility(first,'temporalDistortion',{x:13,y:10});useAbility(second,'temporalDistortion',{x:13,y:10});
    target.suppressionStartAt=testClock;target.suppressedUntil=testClock+5000;testClock+=1000;updateAreaEffects();`);
  assert.equal(run(`target.attackReadyAt-testClock`),3300);
  assert.equal(run(`target.abilityReadyAt-testClock`),9300);
  assert.equal(run(`target.moveReadyAt-testClock`),2000);
  assert.equal(run(`target.hp-hp`),0);
  assert.equal(run(`effectiveMoveCd(target)-moveCd`),0);
  run(`target.x=20;target.y=20;target.suppressedUntil=0;testClock+=1000;updateAreaEffects();`);
  assert.equal(run(`target.attackReadyAt-testClock`),2300);
  assert.equal(run(`target.abilityReadyAt-testClock`),8300);
});

test('A slowed timer that would otherwise expire between updates becomes ready at the correct recovered time',()=>{
  const run=game('ascendant');
  run(`globalThis.caster=addEntity('unit','chronoweaver',0,10,10);globalThis.target=addEntity('unit','rifleman',1,12,10);
    target.attackReadyAt=testClock+1000;useAbility(caster,'temporalDistortion',{x:12,y:10});
    testClock+=1300;updateAreaEffects();`);
  assert.ok(run(`target.attackReadyAt>testClock`));
  run(`testClock+=200;updateAreaEffects();`);
  assert.ok(run(`target.attackReadyAt<=testClock`));
  assert.ok(Math.abs(run(`target.attackReadyAt-100000`)-1000/.7)<.001);
});

test('Disruption drains shields without HP damage, blocks existing and structure abilities, and grants lock immunity',()=>{
  const run=game();
  run(`globalThis.first=addEntity('unit','pulseOperative',0,10,10);globalThis.second=addEntity('unit','pulseOperative',0,10,11);
    globalThis.third=addEntity('unit','pulseOperative',0,10,12);globalThis.medic=addEntity('unit','medic',1,12,10);
    globalThis.base=addEntity('structure','commandHub',1,12,11);base.shield=100;base.maxShield=100;globalThis.hp=base.hp;
    players[1].factionId='frontier';players[1].research.orbitalRecon=1;useAbility(first,'disruptionPulse',{x:12,y:10});`);
  assert.equal(run(`base.shield`),50);
  assert.equal(run(`base.hp-hp`),0);
  assert.equal(run(`useAbility(medic,'fieldSurge')`),false);
  assert.equal(run(`useOrbitalScan(base,12,10)`),false);
  assert.equal(run(`queueUnit(base,'engineer')`),true);
  assert.equal(run(`useAbility(second,'disruptionPulse',{x:12,y:10});medic.abilityLockedUntil-testClock`),4000);
  run(`testClock+=4500;updateAreaEffects();`);
  assert.equal(run(`expansionAbilityLocked(medic)`),false);
  assert.equal(run(`useAbility(third,'disruptionPulse',{x:12,y:10});expansionAbilityLocked(medic)`),false);
  assert.equal(run(`useAbility(medic,'fieldSurge')`),true);
  assert.equal(run(`useOrbitalScan(base,12,10)`),true);
});

test('Void Spear requires shields, visible enemy structures in range, and charges before impact damage',()=>{
  const run=game('ascendant');
  run(`globalThis.lancer=addEntity('unit','voidLancer',0,10,10);globalThis.structure=addEntity('structure','bunker',1,15,10);
    globalThis.infantry=addEntity('unit','rifleman',1,12,10);globalThis.friend=addEntity('structure','bunker',0,11,10);
    globalThis.far=addEntity('structure','bunker',1,17,10);globalThis.before=structure.hp;
    lancer.shield=69;`);
  assert.equal(run(`useAbility(lancer,'voidSpear',{x:15,y:10})`),false);
  run(`lancer.shield=185;tileVisibleToFaction=()=>false;`);
  assert.equal(run(`useAbility(lancer,'voidSpear',{x:15,y:10})`),false);
  run(`tileVisibleToFaction=()=>true;`);
  assert.equal(run(`useAbility(lancer,'voidSpear',{x:12,y:10})`),false);
  assert.equal(run(`useAbility(lancer,'voidSpear',{x:11,y:10})`),false);
  assert.equal(run(`useAbility(lancer,'voidSpear',{x:17,y:10})`),false);
  assert.equal(run(`useAbility(lancer,'voidSpear',{x:15,y:10})`),true);
  assert.equal(run(`lancer.shield`),115);
  assert.ok(run(`lancer.channelEffectId`));
  run(`fireAt(lancer,infantry);testClock+=1999;updateAreaEffects();`);
  assert.equal(run(`pendingImpacts.length`),0);
  assert.equal(run(`structure.hp-before`),0);
  run(`testClock+=1;updateAreaEffects();`);
  assert.equal(run(`lancer.channelEffectId`),null);
  assert.equal(run(`pendingImpacts.length`),1);
  assert.equal(run(`structure.hp-before`),0);
  run(`testClock+=420;resolveProjectileImpacts();`);
  assert.equal(run(`before-structure.hp`),75);
});

for(const interrupt of ['move','stop','death','disruption']){
  test(`Void charge cancels on ${interrupt} and keeps its shield/cooldown cost`,()=>{
    const run=game('ascendant');
    run(`globalThis.lancer=addEntity('unit','voidLancer',0,10,10);globalThis.structure=addEntity('structure','bunker',1,15,10);
      globalThis.pulse=addEntity('unit','pulseOperative',1,11,11);globalThis.hp=structure.hp;
      useAbility(lancer,'voidSpear',{x:15,y:10});`);
    if(interrupt==='move')run(`issueMoveOrder(lancer,10,13)`);
    if(interrupt==='stop')run(`applyCommand({type:'stop',slot:0,entityId:lancer.id})`);
    if(interrupt==='death')run(`killEntity(lancer)`);
    if(interrupt==='disruption')run(`useAbility(pulse,'disruptionPulse',{x:10,y:10})`);
    run(`testClock+=3000;updateAreaEffects();resolveProjectileImpacts();`);
    assert.equal(run(`structure.hp-hp`),0);
    assert.equal(run(`lancer.channelEffectId`),null);
    assert.ok(run(`lancer.shield<=115`));
    assert.ok(run(`lancer.abilityReadyAt>testClock`));
  });
}

test('Void Spear can hit temporary enemy barriers with reduced damage',()=>{
  const run=game('ascendant');
  run(`globalThis.lancer=addEntity('unit','voidLancer',0,10,10);
    globalThis.barrier=addTemporaryObstacle({id:'barrier',kind:'shieldWall',faction:1,x:14,y:10,hp:100,shield:0,endAt:testClock+10000});`);
  assert.equal(run(`useAbility(lancer,'voidSpear',{x:14,y:10})`),true);
  run(`testClock+=2000;updateAreaEffects();testClock+=420;resolveProjectileImpacts();`);
  assert.equal(run(`barrier.hp`),60);
});

test('Spore Bloom deals 30 damage over five seconds only to enemy aircraft, without stacking overlapping clouds',()=>{
  const run=game('brood');
  run(`globalThis.first=addEntity('unit','sporeback',0,10,10);globalThis.second=addEntity('unit','sporeback',0,10,11);
    globalThis.air=addEntity('unit','wraith',1,13,10);globalThis.ground=addEntity('unit','rifleman',1,13,11);
    globalThis.friend=addEntity('unit','wraith',0,13,10);globalThis.outside=addEntity('unit','wraith',1,16,10);
    globalThis.before=entities.map(e=>e.hp);useAbility(first,'sporeBloom',{x:13,y:10});useAbility(second,'sporeBloom',{x:13,y:10});
    for(let i=0;i<50;i++){testClock+=100;updateAreaEffects()}`);
  assert.equal(run(`before[2]-air.hp`),30);
  assert.deepEqual(run.plain(`entities.slice(3).map((e,i)=>before[i+3]-e.hp)`),[0,0,0]);
  assert.equal(run(`areaEffects.filter(a=>a.kind==='sporeBloom').length`),0);
});

test('Spore Cannon uses delayed air-only splash and extremely weak ground attacks',()=>{
  const run=game('brood');
  run(`globalThis.spore=addEntity('unit','sporeback',0,10,10);globalThis.air=addEntity('unit','wraith',1,13,10);
    globalThis.nearAir=addEntity('unit','wraith',1,13,11);globalThis.ground=addEntity('unit','rifleman',1,14,10);
    globalThis.before=entities.map(e=>e.hp);fireAt(spore,air);`);
  assert.deepEqual(run.plain(`entities.slice(1).map((e,i)=>before[i+1]-e.hp)`),[0,0,0]);
  run(`testClock+=1000;resolveProjectileImpacts();`);
  assert.deepEqual(run.plain(`entities.slice(1).map((e,i)=>before[i+1]-e.hp)`),[12,5,0]);
  run(`spore.attackReadyAt=0;fireAt(spore,ground);testClock+=1000;resolveProjectileImpacts();`);
  assert.equal(run(`before[3]-ground.hp`),2);
  assert.equal(run(`before[2]-nearAir.hp`),5);
});

test('Combat state serializes and a noncontroller cannot activate direct abilities or advance their damage',()=>{
  const run=game('brood');
  run(`globalThis.spore=addEntity('unit','sporeback',0,10,10);globalThis.air=addEntity('unit','wraith',1,13,10);
    globalThis.before=air.hp;useAbility(spore,'sporeBloom',{x:13,y:10});globalThis.state=serializeState();`);
  assert.equal(run(`state.areaEffects[0].kind`),'sporeBloom');
  assert.equal(run(`state.entities.find(e=>e.id===spore.id).lastAbilityId`),'sporeBloom');
  run(`lobbyId='test';isController=false;spore.abilityReadyAt=0;testClock+=1000;`);
  assert.equal(run(`useAbility(spore,'sporeBloom',{x:13,y:10})`),false);
  run(`expansionUpdateCombatEffects();`);
  assert.equal(run(`air.hp-before`),0);
});
