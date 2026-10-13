const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

function match(mode='annihilation',faction='frontier'){
  const run=simulation(faction);
  run(`chosenGameMode=${JSON.stringify(mode)};initializeObjectiveState();matchStartedAt=40000;modeState.matchStartedAt=40000;
    players.forEach(p=>addEntity('structure',FACTIONS[p.factionId].base,p.idx,5+p.idx*12,5));`);
  return run;
}
const result=run=>run.plain(`({over:gameOver,result:matchResult,shown:presentedMatchResult,title:endTitle.textContent,text:endText.textContent,display:endOverlay.style.display,faction:endOverlay.dataset.faction})`);

test('annihilation reports the winner once, persists finish time, and includes the finishing kill and damage',()=>{
  const run=match();
  run(`players[2].eliminated=true;const target=playerStructures(1).find(isBase),attacker=addEntity('unit','rifleman',0,16,5);applyDamage(target,9999,attacker);`);
  const final=result(run);
  assert.equal(final.over,true);assert.equal(final.title,'Victory');assert.equal(final.result.winnerIdx,0);assert.equal(final.result.finishedAt,100000);
  assert.equal(final.shown.reportSnapshot[0].stats.structuresDestroyed,1);assert.ok(final.shown.reportSnapshot[0].stats.damageDealt>0);
  const report=run.elements.get('postMatchReport').innerHTML;
  run(`testClock+=90000;finishMatch(players[1],'wrong winner');renderPostMatchReport();`);
  assert.equal(run.elements.get('postMatchReport').innerHTML,report);
  assert.equal(result(run).result.winnerIdx,0);assert.equal(result(run).result.finishedAt,100000);
  assert.equal(run.plain('serializeState()').matchResult.winnerIdx,0);
});

test('simultaneous final projectile impacts produce a draw, and baseless unchecked players never win',()=>{
  for(const trigger of ['impacts','base-scan']){
    const run=match();
    run(`players[2].eliminated=true;entities=entities.filter(e=>e.faction!==2);const a=playerStructures(0).find(isBase),b=playerStructures(1).find(isBase);
      ${trigger==='impacts'?`pendingImpacts=[{id:'a',targetId:a.id,sourceId:b.id,sourceFaction:1,sourceType:b.type,damage:9999,impactAt:testClock,launchX:b.x,launchY:b.y,targetX:a.x,targetY:a.y},{id:'b',targetId:b.id,sourceId:a.id,sourceFaction:0,sourceType:a.type,damage:9999,impactAt:testClock,launchX:a.x,launchY:a.y,targetX:b.x,targetY:b.y}];resolveProjectileImpacts();`:`a.alive=false;b.alive=false;checkBaseStates();`}`);
    const final=result(run);assert.equal(final.over,true,trigger);assert.equal(final.result.winnerIdx,null,trigger);assert.equal(final.title,'Match Complete',trigger);
  }
});

test('Conquest and Central Relic both finish with the correct persistent objective winner',()=>{
  const conquest=match('conquest');
  conquest(`const site=modeState.conquest[0];addEntity('unit','rifleman',0,site.x,site.y);modeState.scores[0]=119.99;updateObjectives(.05);`);
  assert.equal(result(conquest).result.winnerIdx,0);assert.equal(result(conquest).result.modeId,'conquest');assert.match(result(conquest).text,/120 conquest points/);
  const relic=match('relic','ascendant');
  relic(`const base=playerStructures(1).find(isBase),carrier=addEntity('unit','rifleman',1,base.x+1,base.y);modeState.scores[1]=2;modeState.relic.carrierId=carrier.id;updateObjectives(.05);`);
  assert.equal(result(relic).result.winnerIdx,1);assert.equal(result(relic).result.modeId,'relic');assert.equal(result(relic).title,'Defeat');assert.match(result(relic).text,/3 times/);
});

test('an eliminated faction cannot win an objective during the finishing impact batch',()=>{
  const run=match('conquest');run(`players[2].eliminated=true;modeState.scores[1]=120;killEntity(playerStructures(1).find(isBase));updateObjectives(.05);`);
  assert.equal(result(run).result.winnerIdx,0);assert.equal(result(run).title,'Victory');
});

test('multiplayer final snapshots show each client its own outcome and fixed report without reopening after spectate',()=>{
  const host=match('conquest');host(`finishMatch(players[1],'Player 1 held the impact zones.');`);
  const snapshot=host.plain('serializeState()');
  const client=match('conquest');client(`resetMatchPresentation();PLAYER=1;testClock+=20000;`);
  client(`applyRemoteState(${JSON.stringify(snapshot)});`);
  assert.equal(result(client).title,'Victory');assert.equal(result(client).faction,'ascendant');assert.equal(result(client).result.finishedAt,100000);
  const report=client.elements.get('postMatchReport').innerHTML;
  client(`testClock+=100000;applyRemoteState(${JSON.stringify(snapshot)});`);
  assert.equal(client.elements.get('postMatchReport').innerHTML,report);
  client('spectateBtn.onclick();');assert.equal(result(client).display,'none');
  client(`applyRemoteState(${JSON.stringify({...snapshot,simulationTick:snapshot.simulationTick+1})});`);
  assert.equal(result(client).display,'none');assert.equal(client('spectating'),true);
});

test('legacy finished snapshots infer objective and elimination winners',()=>{
  for(const mode of ['annihilation','conquest','relic']){
    const host=match(mode);host(`gameOver=true;matchResult=null;${mode==='annihilation'?"players[0].eliminated=players[2].eliminated=true;":`modeState.scores[1]=GAME_MODES[chosenGameMode].scoreToWin;`}`);
    const snapshot=host.plain('serializeState()');delete snapshot.matchResult;
    const client=match(mode);client(`resetMatchPresentation();PLAYER=1;applyRemoteState(${JSON.stringify(snapshot)});`);
    assert.equal(result(client).title,'Victory',mode);assert.equal(result(client).result.winnerIdx,1,mode);assert.equal(result(client).result.finishedAt,snapshot.updatedAtMs,mode);
  }
});

test('an eliminated commander gets immediate defeat while survivors continue and every command entry rejects their orders',async()=>{
  const run=match();run(`globalThis.worker=addEntity('unit','engineer',0,5,6);killEntity(playerStructures(0).find(isBase));`);
  assert.equal(result(run).title,'Defeat');assert.equal(result(run).over,false);assert.equal(result(run).shown.playerEliminated,true);
  run(`applyCommand({type:'move',slot:0,entityId:worker.id,x:7,y:6});applyCommand({type:'attackMove',slot:0,entityId:worker.id,x:7,y:6});applyCommand({type:'setRally',slot:0,entityId:worker.id,x:7,y:6});`);
  await run(`sendCommand({type:'move',entityId:worker.id,x:7,y:6})`);
  assert.equal(run('worker.moveDestination'),null);assert.equal(run('worker.attackMoveDestination'),undefined);assert.equal(run('worker.rallyPoint'),undefined);
  const snapshot=run.plain('serializeState()');const client=match();client(`resetMatchPresentation();applyRemoteState(${JSON.stringify(snapshot)});`);
  assert.equal(result(client).title,'Defeat');assert.equal(result(client).over,false);
  run('spectateBtn.onclick();');run(`finishMatch(players[2],'Player 2 wins.');`);assert.equal(result(run).display,'none');
});

test('new local match detaches shared listeners, resets slot and presentation, then can report a new result',()=>{
  const run=match();run(`PLAYER=2;finishMatch(players[2]);globalThis.unsubscriptions=0;stateUnsub=lobbyUnsub=presenceUnsub=commandUnsub=()=>unsubscriptions++;lobbyId='shared';isController=true;`);
  run('renderStartMenu=()=>{};restartBtn.onclick();');assert.equal(run('unsubscriptions'),4);assert.equal(run('PLAYER'),0);assert.equal(run('lobbyId'),'');assert.equal(run('gameOver'),false);assert.equal(run('spectating'),false);assert.equal(run('presentedMatchResult'),null);
  run(`createGame(buildSoloSlots());finishMatch(players[0]);`);assert.equal(result(run).title,'Victory');assert.equal(result(run).display,'flex');
});

test('commander names remain plain text in match reports',()=>{
  const run=match();run(`players[0].name='<img src=x onerror=alert(1)>';finishMatch(players[0]);`);
  const report=run.elements.get('postMatchReport').innerHTML;assert.match(report,/&lt;img src=x onerror=alert\(1\)&gt;/);assert.doesNotMatch(report,/<img src=x/);
});

test('finishing during an in-flight custom write publishes a fresh terminal snapshot to other clients',async()=>{
  const host=match();host(`lobbyId='shared';stateRef='shared-state';isController=true;globalThis.published=[];globalThis.releaseFirstWrite=null;setDoc=(ref,data)=>{published.push(JSON.parse(JSON.stringify(data)));return published.length===1?new Promise(resolve=>releaseFirstWrite=resolve):Promise.resolve();};void writeState(true);finishMatch(players[0],'Custom match complete.');`);
  assert.equal(host('published.length'),1);assert.equal(host('published[0].gameOver'),false);host('releaseFirstWrite();');await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));
  const snapshot=host.plain('published.at(-1)');assert.equal(snapshot.gameOver,true);assert.equal(snapshot.matchResult.winnerIdx,0);assert.ok(host('published.length')>=2);
  const guest=match();guest(`PLAYER=1;isController=false;applyRemoteState(${JSON.stringify(snapshot)});`);assert.equal(result(guest).title,'Defeat');assert.equal(guest('gameOver'),true);
});

test('return to lobby accepts same-origin absolute URLs and rejects external redirect destinations',async()=>{
  const run=match();run(`returnTo='http://turncraft.test/game/turncraft-lobby.html?lobbyId=confirmed';`);await run('exitGame()');assert.equal(run.context.location.href,'http://turncraft.test/game/turncraft-lobby.html?lobbyId=confirmed');
  run.context.location.href='http://turncraft.test/game/turncraft.html';run(`returnTo='https://unrelated.example/redirect';`);await run('exitGame()');assert.equal(run.context.location.href,'/game/index.html');
});
