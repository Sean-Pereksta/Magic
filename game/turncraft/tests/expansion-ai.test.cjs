const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

test('each faction exposes exactly its three new trainable units with producer and tech requirements',()=>{
  const run=simulation();
  const definitions=run.plain(`(()=>{const out={};for(const f of Object.keys(FACTIONS))out[f]=ROSTER_ORDER[f].filter(type=>EXPANSION_UNIT_TYPES.has(type));return out})()`);
  assert.deepEqual(definitions,{frontier:['suppressorExosuit','pulseOperative','forwardFabricator'],ascendant:['latticeSkiff','chronoweaver','voidLancer'],brood:['tunnelMaw','sporeback','broodMatriarch']});
  assert.equal(run(`unitUnlocked(0,'pulseOperative')`),false);
  run(`addEntity('structure','academy',0,5,5)`);
  assert.equal(run(`unitUnlocked(0,'pulseOperative')`),true);
  assert.equal(run(`unitUnlocked(0,'broodling')`),false);
  assert.equal(run(`structureUnlocked(2,'tunnelExit')`),false);
  assert.equal(run(`STRUCTURES.factory.produces.includes('suppressorExosuit')&&STRUCTURES.factory.produces.includes('forwardFabricator')&&STRUCTURES.barracks.produces.includes('pulseOperative')`),true);
  assert.equal(run(`STRUCTURES.stargate.produces.includes('latticeSkiff')&&STRUCTURES.gateway.produces.includes('chronoweaver')&&STRUCTURES.robotics.produces.includes('voidLancer')`),true);
  for(const f of ['ascendant','brood']){
    const faction=simulation(f);
    assert.equal(faction(`unitUnlocked(0,'broodling')`),false);
    assert.equal(faction(`structureUnlocked(0,'tunnelExit')`),false);
    if(f==='ascendant'){
      faction(`addEntity('structure','gateway',0,5,5);addEntity('structure','robotics',0,5,6);addEntity('structure','stargate',0,5,7);addEntity('structure','forge',0,5,8)`);
      assert.equal(faction(`unitUnlocked(0,'latticeSkiff')`),true);
      assert.equal(faction(`unitUnlocked(0,'chronoweaver')||unitUnlocked(0,'voidLancer')`),false);
      faction(`addEntity('structure','citadel',0,5,9)`);
      assert.equal(faction(`unitUnlocked(0,'chronoweaver')&&unitUnlocked(0,'voidLancer')`),true);
    }else{
      faction(`addEntity('structure','hydraDen',0,5,5)`);
      assert.equal(faction(`unitUnlocked(0,'sporeback')`),false);
      faction(`addEntity('structure','evolutionNest',0,5,6)`);
      assert.equal(faction(`unitUnlocked(0,'sporeback')`),true);
      assert.equal(faction(`unitUnlocked(0,'tunnelMaw')||unitUnlocked(0,'broodMatriarch')`),false);
      faction(`addEntity('structure','hive',0,5,7)`);
      assert.equal(faction(`unitUnlocked(0,'tunnelMaw')&&unitUnlocked(0,'broodMatriarch')`),true);
    }
  }
});

test('AI production adapts to visible aircraft and shield casters and counts queued support',()=>{
  const run=simulation('brood');
  const calm=run(`aiCounterScore(0,'sporeback')`);
  run(`addEntity('unit','phoenix',1,20,10);addEntity('unit','phoenix',1,20,11)`);
  assert.ok(run(`aiCounterScore(0,'sporeback')`)>calm+30);
  run(`tileVisibleToFaction=()=>false`);
  assert.equal(run(`aiCounterScore(0,'sporeback')`),calm);
  const frontier=simulation();
  const normal=frontier(`aiCounterScore(0,'pulseOperative')`);
  frontier(`addEntity('unit','templar',1,20,10);addEntity('unit','sentry',1,20,11)`);
  assert.ok(frontier(`aiCounterScore(0,'pulseOperative')`)>normal+20);
  frontier(`for(let i=0;i<7;i++)addEntity('unit','rifleman',0,5+i,5);globalThis.factory=addEntity('structure','factory',0,5,8);factory.queue.push({type:'forwardFabricator',cost:[225,125],startAt:testClock,finishAt:testClock+20000})`);
  assert.equal(frontier(`aiCounterScore(0,'forwardFabricator')`),-1000);
  assert.notEqual(frontier(`aiChooseArmyUnit(0,['forwardFabricator','suppressorExosuit'])`),'forwardFabricator');
});

test('AI combat abilities never select hidden opponents',()=>{
  for(const [f,type,enemy] of [['frontier','pulseOperative','templar'],['ascendant','voidLancer','bunker'],['brood','sporeback','phoenix']]){
    const run=simulation(f);
    run(`globalThis.caster=addEntity('unit','${type}',0,10,10);addEntity('${enemy==='bunker'?'structure':'unit'}','${enemy}',1,13,10);tileVisibleToFaction=()=>false`);
    assert.equal(run(`aiExpansionTactics(0,{commandLimit:6})`),0);
    assert.equal(run(`caster.abilityReadyAt>testClock`),false);
  }
});

test('Frontier AI suppresses ground groups and disrupts enemy shield casters through shared abilities',()=>{
  const run=simulation();
  run(`globalThis.suit=addEntity('unit','suppressorExosuit',0,10,10);globalThis.operative=addEntity('unit','pulseOperative',0,10,16);globalThis.ling1=addEntity('unit','zergling',2,13,10);globalThis.ling2=addEntity('unit','zergling',2,13,11);globalThis.templar=addEntity('unit','templar',1,13,16)`);
  assert.equal(run(`aiExpansionTactics(0,{commandLimit:4})`),2);
  assert.equal(run(`ling1.suppressedUntil>testClock&&ling2.suppressedUntil>testClock`),true);
  assert.equal(run(`templar.abilityLockedUntil>testClock&&templar.shield===40`),true);
  assert.equal(run(`suit.lastAbilityId==='suppressionSweep'&&operative.lastAbilityId==='disruptionPulse'`),true);
});

test('Ascendant AI creates temporal fields and charges shield-paid structure siege',()=>{
  const run=simulation('ascendant');
  run(`globalThis.chrono=addEntity('unit','chronoweaver',0,10,10);globalThis.lancer=addEntity('unit','voidLancer',0,10,16);addEntity('unit','siegeTank',1,14,10);addEntity('unit','marauder',1,14,11);globalThis.bunker=addEntity('structure','bunker',1,15,16)`);
  assert.equal(run(`aiExpansionTactics(0,{commandLimit:4})`),2);
  assert.equal(run(`areaEffects.some(a=>a.kind==='temporalDistortion')`),true);
  assert.equal(run(`lancer.shield`),115);
  assert.equal(run(`areaEffects.some(a=>a.kind==='voidSpearChannel'&&a.targetId===bunker.id)`),true);
});

test('Brood AI denies visible airspace and respects its shared six-summon cap',()=>{
  const run=simulation('brood');
  run(`globalThis.spore=addEntity('unit','sporeback',0,10,6);addEntity('unit','phoenix',1,14,6);addEntity('unit','phoenix',1,14,7);globalThis.mother1=addEntity('unit','broodMatriarch',0,10,15);globalThis.mother2=addEntity('unit','broodMatriarch',0,10,21);globalThis.mother3=addEntity('unit','broodMatriarch',0,10,25);addEntity('unit','rifleman',1,14,15);addEntity('unit','rifleman',1,14,21);addEntity('unit','rifleman',1,14,25)`);
  assert.equal(run(`aiExpansionTactics(0,{commandLimit:8})`),3);
  assert.equal(run(`areaEffects.some(a=>a.kind==='sporeBloom')`),true);
  assert.equal(run(`entities.filter(e=>e.alive!==false&&e.type==='broodling').length`),6);
  assert.equal(run(`mother1.hp===205&&mother2.hp===205&&mother3.hp===230`),true);
  assert.equal(run(`UNITS.broodling.supply`),0);
});

test('AI deploys and trains from a Fabricator during a safe supported forward push',()=>{
  const run=simulation();
  run(`addEntity('structure','commandHub',0,5,10);addEntity('structure','academy',0,5,12);globalThis.fab=addEntity('unit','forwardFabricator',0,18,10);for(const [x,y] of [[19,10],[19,11],[18,11]])addEntity('unit','rifleman',0,x,y);addEntity('structure','bunker',1,25,10)`);
  assert.equal(run(`aiExpansionTactics(0,{commandLimit:4})`),1);
  assert.equal(run(`fab.fabricatorDeployAt===testClock+3000`),true);
  run(`testClock+=3100;updateExpansionLogistics()`);
  assert.equal(run(`fab.fabricatorDeployed`),true);
  assert.equal(run(`aiExpansionTactics(0,{commandLimit:4})`),1);
  assert.deepEqual(run.plain(`fab.queue.map(q=>({type:q.type,cost:q.cost}))`),[{type:'medic',cost:[90,60]}]);
  run(`addEntity('unit','zergling',2,20,10)`);
  assert.equal(run(`aiExpansionTactics(0,{commandLimit:4})`),1);
  assert.equal(run(`fab.fabricatorDeployed`),false);
  assert.equal(run(`fab.queue.length`),0);
});

test('AI Warp Conduit uses real eligible Pylon troops and resolves validated landing positions',()=>{
  const run=simulation('ascendant');
  run(`globalThis.pylon=addEntity('structure','pylon',0,10,10);globalThis.skiff=addEntity('unit','latticeSkiff',0,16,10);globalThis.zealot1=addEntity('unit','zealot',0,11,10);globalThis.zealot2=addEntity('unit','zealot',0,10,11);addEntity('unit','rifleman',1,20,10)`);
  assert.equal(run(`aiExpansionTactics(0,{commandLimit:4})`),1);
  assert.equal(run(`skiff.warpChannel.pylonId===pylon.id&&skiff.warpChannel.targetIds.length===2`),true);
  run(`testClock+=2100;updateExpansionLogistics()`);
  assert.equal(run(`dist(skiff,zealot1)<=3&&dist(skiff,zealot2)<=3&&!(zealot1.x===zealot2.x&&zealot1.y===zealot2.y)`),true);
});

test('AI opens creep invasion tunnels and sends existing combat troops through them',()=>{
  const run=simulation('brood');
  run(`addEntity('structure','hatchery',0,6,10);globalThis.maw=addEntity('unit','tunnelMaw',0,10,10);globalThis.ling=addEntity('unit','zergling',0,11,10);addEntity('unit','rifleman',1,17,10)`);
  assert.equal(run(`aiExpansionTactics(0,{commandLimit:4})`),1);
  assert.equal(run(`!!maw.tunnelDigging`),true);
  run(`testClock+=3100;updateExpansionLogistics()`);
  assert.equal(run(`!!entityById(maw.tunnelExitId)`),true);
  assert.equal(run(`aiExpansionTactics(0,{commandLimit:4})`),1);
  assert.equal(run(`maw.tunnelRemainingUses`),4);
  assert.equal(run(`dist(maw,ling)>2&&!!ling.attackMoveDestination`),true);
});
