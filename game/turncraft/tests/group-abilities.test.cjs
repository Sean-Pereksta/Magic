const {test} = require('node:test');
const assert = require('node:assert/strict');
const {simulation} = require('./helpers/expansion-harness.cjs');

test('single Dragoon Blink retains range, cooldown, visibility and immediate teleport presentation', () => {
  const run = simulation('ascendant');
  const result = run.plain(`(()=>{
    const u=addEntity('unit','dragoon',0,8,8);u.moveDestination={x:20,y:8};u.order='move';
    const ok=applyCommand({type:'ability',slot:0,entityId:u.id,abilityId:'phaseStep',x:10,y:8});
    return {ok,x:u.x,y:u.y,px:u.px,py:u.py,ready:u.abilityReadyAt,expected:nowMs()+effectiveAbilityCd('phaseStep',u)*1000,destination:u.moveDestination,last:u.lastAbilityId};
  })()`);
  assert.equal(result.ok, true);
  assert.deepEqual([result.x,result.y,result.px,result.py], [10,8,10,8]);
  assert.equal(result.ready, result.expected);
  assert.equal(result.destination, null);
  assert.equal(result.last, 'phaseStep');
});

test('ten Dragoons Blink backward together and land on unique, individually legal squares', () => {
  const run = simulation('ascendant');
  const result = run.plain(`(()=>{
    const troops=Array.from({length:10},(_,i)=>addEntity('unit','dragoon',0,12+i%5,10+Math.floor(i/5)));
    const before=troops.map(u=>({id:u.id,x:u.x,y:u.y})),plan=planGroupAbility(troops,'phaseStep',{x:2,y:10});
    const repeat=planGroupAbility(troops.slice().reverse(),'phaseStep',{x:2,y:10});
    const ok=applyCommand({type:'groupAbility',slot:0,entityIds:troops.map(u=>u.id).reverse(),abilityId:'phaseStep',x:2,y:10});
    return {ok,plan,repeat,before,after:troops.map(u=>({id:u.id,x:u.x,y:u.y,ready:u.abilityReadyAt}))};
  })()`);
  assert.equal(result.ok, true);
  assert.equal(result.plan.casts.length, 10);
  assert.deepEqual(result.plan.casts, result.repeat.casts, 'selection order cannot change assignment');
  assert.equal(new Set(result.after.map(p=>`${p.x},${p.y}`)).size, 10);
  for (let i=0;i<10;i++) {
    const from=result.before[i],to=result.after[i];
    assert.ok(to.x<from.x);
    assert.ok(Math.hypot(to.x-from.x,to.y-from.y)<=2.3);
    assert.ok(to.ready>100000);
  }
});

test('far destinations clamp every caster to its own existing range', () => {
  const run = simulation('ascendant');
  const result=run.plain(`(()=>{const troops=[addEntity('unit','dragoon',0,5,5),addEntity('unit','dragoon',0,5,9)];return planGroupAbility(troops,'phaseStep',{x:BOARD_W-1,y:7}).casts.map(p=>({x:p.x,y:p.y,sourceY:entityById(p.casterId).y}))})()`);
  assert.equal(result.length,2);
  for(const p of result){assert.equal(p.x,7);assert.ok(Math.hypot(p.x-5,p.y-p.sourceY)<=2.3)}
});

test('landing assignments can use squares vacated by successful simultaneous Blinks', () => {
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{
    terrain.forEach(t=>t.blocked=true);for(let x=5;x<=9;x++)tileAt(x,5).blocked=false;
    const a=addEntity('unit','dragoon',0,5,5),b=addEntity('unit','dragoon',0,7,5);
    const plan=planGroupAbility([a,b],'phaseStep',{x:10,y:5});
    const ok=applyCommand({type:'groupAbility',slot:0,entityIds:[a.id,b.id],abilityId:'phaseStep',x:10,y:5});
    return {plan,ok,positions:[a,b].map(u=>[u.x,u.y])};
  })()`);
  assert.equal(result.ok,true);
  assert.deepEqual(result.positions,[[7,5],[9,5]]);
});

test('a caster that cannot move keeps its origin reserved and consumes no cooldown', () => {
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{
    terrain.forEach(t=>t.blocked=true);tileAt(5,5).blocked=false;tileAt(6,5).blocked=false;
    const a=addEntity('unit','dragoon',0,5,5),b=addEntity('unit','dragoon',0,6,5);
    const plan=planGroupAbility([a,b],'phaseStep',{x:10,y:5});
    const ok=applyCommand({type:'groupAbility',slot:0,entityIds:[a.id,b.id],abilityId:'phaseStep',x:10,y:5});
    return {ok,count:plan.casts.length,positions:[a,b].map(u=>[u.x,u.y,u.abilityReadyAt])};
  })()`);
  assert.equal(result.ok,false);
  assert.equal(result.count,0);
  assert.deepEqual(result.positions,[[5,5,0],[6,5,0]]);
});

test('walls, buildings, enemies, allies, temporary blockers and map edges stay protected', () => {
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{
    const troops=[addEntity('unit','dragoon',0,1,1),addEntity('unit','dragoon',0,2,1),addEntity('unit','dragoon',0,1,2)];
    tileAt(0,1).blocked=true;addEntity('structure','pylon',0,0,2);addEntity('unit','zergling',1,0,0);addEntity('unit','zealot',0,1,0);
    addTemporaryObstacle({kind:'forcefield',faction:1,x:0,y:3,hp:1,endAt:nowMs()+6000});
    const plan=planGroupAbility(troops,'phaseStep',{x:0,y:0});
    return plan.casts.map(p=>({x:p.x,y:p.y,inBounds:inBounds(p.x,p.y),terrain:groundBlocked(p.x,p.y),occupied:entitiesAt(p.x,p.y).some(e=>!troops.includes(e)),clear:lineClear(entityById(p.casterId).x,entityById(p.casterId).y,p.x,p.y,p.casterId)}));
  })()`);
  assert.ok(result.length>0);
  assert.equal(new Set(result.map(p=>`${p.x},${p.y}`)).size,result.length);
  assert.ok(result.every(p=>p.inBounds&&!p.terrain&&!p.occupied&&p.clear));
});

test('permanent terrain and temporary obstacles cannot be Blinked through', () => {
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{
    const u=addEntity('unit','dragoon',0,5,5);for(let y=0;y<BOARD_H;y++)tileAt(6,y).blocked=true;
    const wall=planGroupAbility([u],'phaseStep',{x:9,y:5});
    for(let y=0;y<BOARD_H;y++){tileAt(6,y).blocked=false;addTemporaryObstacle({kind:'forcefield',faction:1,x:6,y,hp:1,endAt:nowMs()+6000})}
    const temporary=planGroupAbility([u],'phaseStep',{x:9,y:5});return {wall:wall.casts.length,temporary:temporary.casts.length};
  })()`);
  assert.deepEqual(result,{wall:0,temporary:0});
});

test('mixed ownership, cooldowns, duplicate IDs and unavailable casters are validated independently', () => {
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{
    const a=addEntity('unit','dragoon',0,8,8),cooling=addEntity('unit','dragoon',0,8,9),locked=addEntity('unit','dragoon',0,8,10),enemy=addEntity('unit','dragoon',1,8,11),other=addEntity('unit','zealot',0,9,8);
    cooling.abilityReadyAt=nowMs()+10000;locked.abilityLockedUntil=nowMs()+5000;
    const ids=[a.id,cooling.id,locked.id,enemy.id,other.id,a.id];
    applyCommand({type:'groupAbility',slot:0,entityIds:ids,abilityId:'phaseStep',x:2,y:8});
    return [a,cooling,locked,enemy,other].map(e=>({x:e.x,y:e.y,ready:e.abilityReadyAt,last:e.lastAbilityId||null}));
  })()`);
  assert.equal(result[0].last,'phaseStep');
  assert.equal(result[1].ready,110000);
  assert.deepEqual(result.slice(1).map(e=>e.last),[null,null,null,null]);
  assert.deepEqual(result.slice(1).map(e=>[e.x,e.y]),[[8,9],[8,10],[8,11],[9,8]]);
});

for (const [faction,type,id] of [['frontier','rifleman','burstFire'],['ascendant','dragoon','phaseLance'],['brood','zergling','adrenalRush']]) {
  test(`${faction} self abilities activate every ready eligible caster`,()=>{
    const run=simulation(faction);
    const result=run.plain(`(()=>{const troops=Array.from({length:3},(_,i)=>addEntity('unit',${JSON.stringify(type)},0,5+i,5));troops[2].abilityReadyAt=nowMs()+9000;applyCommand({type:'groupAbility',slot:0,entityIds:troops.map(e=>e.id),abilityId:${JSON.stringify(id)}});return troops.map(e=>e.lastAbilityId||null)})()`);
    assert.deepEqual(result,[id,id,null]);
  });
}

test('ground attacks retain their selected area and only in-range casters activate',()=>{
  const run=simulation('brood');
  const result=run.plain(`(()=>{const troops=[addEntity('unit','ravager',0,6,7),addEntity('unit','ravager',0,7,7),addEntity('unit','ravager',0,20,20)];applyCommand({type:'groupAbility',slot:0,entityIds:troops.map(e=>e.id),abilityId:'corrosiveBile',x:10,y:10});return {fields:areaEffects.map(a=>[a.kind,a.x,a.y,a.impactAt-nowMs()]),casts:troops.map(e=>e.lastAbilityId||null)}})()`);
  assert.deepEqual(result.fields,[['bileWarning',10,10,1500],['bileWarning',10,10,1500]]);
  assert.deepEqual(result.casts,['corrosiveBile','corrosiveBile',null]);
});

for (const ability of ['forcefield','shieldWall']) {
  test(`${ability} placements reserve distinct complete footprints`,()=>{
    const run=simulation('ascendant');
    const result=run.plain(`(()=>{const troops=[addEntity('unit','sentry',0,5,5),addEntity('unit','sentry',0,5,6),addEntity('unit','sentry',0,6,5)];const plan=planGroupAbility(troops,${JSON.stringify(ability)},{x:8,y:5});applyCommand({type:'groupAbility',slot:0,entityIds:troops.map(e=>e.id),abilityId:${JSON.stringify(ability)},x:8,y:5});return {count:plan.casts.length,obstacles:temporaryObstacles.map(o=>[o.x,o.y]),casts:troops.map(e=>e.lastAbilityId||null)}})()`);
    assert.equal(result.count,3);
    assert.equal(result.obstacles.length,ability==='shieldWall'?9:3);
    assert.equal(new Set(result.obstacles.map(p=>p.join(','))).size,result.obstacles.length);
    assert.ok(result.casts.every(id=>id===ability));
  });
}

test('specific friendly troop abilities pick one eligible lowest-ID caster and the explicit troop',()=>{
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{const a=addEntity('unit','sentinel',0,5,5),b=addEntity('unit','sentinel',0,6,5),target=addEntity('unit','dragoon',0,7,5);const plan=planGroupAbility([b,a],'guardianLink',{x:target.x,y:target.y,targetId:target.id});applyCommand({type:'groupAbility',slot:0,entityIds:[b.id,a.id],abilityId:'guardianLink',x:target.x,y:target.y,targetId:target.id});return {planned:plan.casts.map(c=>c.casterId),source:target.guardianLinkSourceId,ids:[a.id,b.id],last:[a,b].map(e=>e.lastAbilityId||null)}})()`);
  assert.deepEqual(result.planned,[result.ids[0]]);
  assert.equal(result.source,result.ids[0]);
  assert.deepEqual(result.last,['guardianLink',null]);
});

test('explicit unit targets override shared tile ambiguity and invalid target IDs cannot retarget',()=>{
  const run=simulation('frontier');
  const result=run.plain(`(()=>{const a=addEntity('unit','medic',0,5,5),b=addEntity('unit','medic',0,5,6),ground=addEntity('unit','rifleman',0,7,5),air=addEntity('unit','mutalisk',0,7,5);ground.hp-=10;air.hp-=10;applyCommand({type:'groupAbility',slot:0,entityIds:[a.id,b.id],abilityId:'emergencyStabilization',x:7,y:5,targetId:air.id});const selected=areaEffects[0]?.targetId;testClock+=30000;const rejected=applyCommand({type:'groupAbility',slot:0,entityIds:[a.id,b.id],abilityId:'emergencyStabilization',x:7,y:5,targetId:99999});return {selected,expected:air.id,rejected,count:areaEffects.length,casters:areaEffects.map(e=>e.sourceId)}})()`);
  assert.equal(result.selected,result.expected);
  assert.equal(result.rejected,false);
  assert.equal(result.count,1);
});

test('enemy troop targeting is single-caster and cannot target a friendly, massive or Relic troop',()=>{
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{const a=addEntity('unit','phoenix',0,5,5),b=addEntity('unit','phoenix',0,6,5),target=addEntity('unit','roach',1,7,5),massive=addEntity('unit','ultralisk',1,8,5);applyCommand({type:'groupAbility',slot:0,entityIds:[b.id,a.id],abilityId:'gravitonSnare',x:7,y:5,targetId:target.id});const snared=target.snaredUntil;testClock+=30000;const own=planGroupAbility([a,b],'gravitonSnare',{targetId:a.id}),large=planGroupAbility([a,b],'gravitonSnare',{targetId:massive.id});modeState={relic:{carrierId:target.id}};const relic=planGroupAbility([a,b],'gravitonSnare',{targetId:target.id});return {snared,casters:[a,b].map(e=>e.lastAbilityId||null),invalid:[own,large,relic].map(p=>p.casts.length)}})()`);
  assert.equal(result.snared,102800);
  assert.deepEqual(result.casters,['gravitonSnare',null]);
  assert.deepEqual(result.invalid,[0,0,0]);
});

test('repeated commands cannot fire after cooldown, including after a controller snapshot handoff',()=>{
  const run=simulation('frontier');
  const result=run.plain(`(()=>{const u=addEntity('unit','rifleman',0,5,5),command={type:'groupAbility',slot:0,entityIds:[u.id],abilityId:'burstFire',id:'group_receipt_1'};const first=applyCommand(command),ready=u.abilityReadyAt;const snapshot=serializeState();testClock+=30000;const repeated=applyCommand(command);abilityCommandReceipts.clear();isController=false;const synced=applyRemoteState(snapshot);isController=true;const afterMigration=applyCommand(command);return {first,repeated,synced,afterMigration,ready,current:entityById(u.id).abilityReadyAt,receipts:snapshot.abilityCommandReceipts}})()`);
  assert.equal(result.first,true);
  assert.equal(result.repeated,false);
  assert.equal(result.synced,true);
  assert.equal(result.afterMigration,false);
  assert.equal(result.current,result.ready);
  assert.deepEqual(result.receipts,['group_receipt_1']);
});

test('the existing Firestore listener deduplicates delivery and remote peers cannot execute abilities',()=>{
  const run=simulation('frontier');
  run(`globalThis.listenerUnit=addEntity('unit','rifleman',0,5,5);lobbyId='test';isController=true;manageCommandListener();`);
  const command=run.plain(`({type:'groupAbility',slot:0,entityIds:[listenerUnit.id],abilityId:'burstFire',id:'network_group_1',createdAtMs:nowMs()})`);
  const snapshot={docChanges:()=>[{type:'added',doc:{id:command.id,data:()=>command}}]};
  run.network.listener(snapshot);
  const ready=run('listenerUnit.abilityReadyAt');
  run('testClock+=30000');
  run.network.listener(snapshot);
  assert.equal(run('listenerUnit.abilityReadyAt'),ready);
  assert.equal(run('processedCommands.size'),1);
  assert.equal(run(`isController=false;applyCommand({type:'groupAbility',slot:0,entityIds:[listenerUnit.id],abilityId:'burstFire',id:'new_remote_command'})`),false);
});

test('a remote commander receives a partial Blink summary once and old snapshots remain quiet',()=>{
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{
    const notices=[];toast=text=>notices.push(text);lobbyId='shared';isController=true;PLAYER=0;
    const a=addEntity('unit','dragoon',1,10,10),b=addEntity('unit','dragoon',1,10,11);b.abilityReadyAt=nowMs()+5000;
    const command={type:'groupAbility',slot:1,entityIds:[a.id,b.id],abilityId:'phaseStep',x:2,y:10,id:'remote_partial'};
    applyCommand(command);const hostNotices=notices.length;processedCommands.set(command.id,nowMs());const state=serializeState();
    PLAYER=1;isController=false;unackedCommands.set(command.id,command);const accepted=applyRemoteState(state);applyRemoteState(state);
    const firstNotices=notices.slice(),pending=unackedCommands.size;
    seenGroupAbilityResults.clear();applyRemoteState(state);const oldNotices=notices.length;
    return {hostNotices,accepted,pending,firstNotices,oldNotices,summary:state.groupAbilityResults};
  })()`);
  assert.equal(result.hostNotices,0);
  assert.equal(result.accepted,true);
  assert.equal(result.pending,0);
  assert.deepEqual(result.firstNotices,['1/2 Dragoons Blinked']);
  assert.equal(result.oldNotices,1);
  assert.equal(result.summary.length,1);
  assert.deepEqual([result.summary[0].slot,result.summary[0].total,result.summary[0].count],[1,2,1]);
});

test('fog visibility, readiness and specific troop eligibility are shared by highlights and casts',()=>{
  const run=simulation('frontier');
  const result=run.plain(`(()=>{const a=addEntity('unit','medic',0,5,5),b=addEntity('unit','medic',0,15,15),healthy=addEntity('unit','rifleman',0,6,5),hurt=addEntity('unit','rifleman',0,7,5);hurt.hp-=10;b.abilityReadyAt=nowMs()+5000;selectedIds=[a.id,b.id];selectedId=a.id;const visible=legalAbilityTilesForSelection('emergencyStabilization');tileVisibleToFaction=(f,x,y)=>x!==7;const hidden=planGroupAbility([a,b],'emergencyStabilization',{targetId:hurt.id});return {visible:visible.map(p=>[p.x,p.y]),hidden:hidden.casts.length}})()`);
  assert.deepEqual(result.visible,[[7,5]]);
  assert.equal(result.hidden,0);
});

test('a full Forcefield cap excludes that caster until an obstacle expires',()=>{
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{const a=addEntity('unit','sentry',0,5,5),b=addEntity('unit','sentry',0,6,5);for(let x=3;x<=4;x++)addTemporaryObstacle({kind:'forcefield',faction:0,sourceId:a.id,x,y:5,hp:1,endAt:nowMs()+1000});const ready=abilityCommandReady(a,'forcefield');applyCommand({type:'groupAbility',slot:0,entityIds:[a.id,b.id],abilityId:'forcefield',x:8,y:5});testClock+=2000;return {ready,aCd:a.abilityReadyAt,bCd:b.abilityReadyAt,recovered:abilityCommandReady(a,'forcefield')}})()`);
  assert.equal(result.ready,false);
  assert.equal(result.aCd,0);
  assert.ok(result.bCd>100000);
  assert.equal(result.recovered,true);
});

test('Intercept with no free landing fails without spending its cooldown',()=>{
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{const caster=addEntity('unit','immortal',0,5,5),target=addEntity('unit','templar',0,8,5);for(let y=3;y<=7;y++)for(let x=6;x<=10;x++)if(!entityAt(x,y))addEntity('unit','zealot',0,x,y);const ok=applyCommand({type:'groupAbility',slot:0,entityIds:[caster.id],abilityId:'intercept',x:target.x,y:target.y,targetId:target.id});return {ok,ready:caster.abilityReadyAt,charges:caster.barrierCharges||0,x:caster.x,y:caster.y}})()`);
  assert.deepEqual(result,{ok:false,ready:0,charges:0,x:5,y:5});
});

test('large-army planners remain deterministic without synchronizing local cosmetic state',()=>{
  const run=simulation('ascendant');
  const result=run.plain(`(()=>{const troops=Array.from({length:120},(_,i)=>addEntity('unit','dragoon',0,8+i%15,8+Math.floor(i/15)));const a=planGroupAbility(troops,'phaseStep',{x:1,y:10}),b=planGroupAbility(troops.slice().reverse(),'phaseStep',{x:1,y:10});return {count:a.casts.length,distinct:new Set(a.casts.map(p=>p.x+','+p.y)).size,equal:JSON.stringify(a.casts)===JSON.stringify(b.casts),state:serializeState()}})()`);
  assert.equal(result.count,120);
  assert.equal(result.distinct,120);
  assert.equal(result.equal,true);
  assert.equal('groupFormationValidation' in result.state,false);
  assert.equal('particles' in result.state,false);
});
