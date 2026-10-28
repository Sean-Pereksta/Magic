const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const {simulation,GAME_FILE}=require('./helpers/expansion-harness.cjs');
const launchBody=fs.readFileSync(GAME_FILE,'utf8').match(/startSkirmish\(options=\{\}\)\{([\s\S]*?)\r?\n\s*\},\r?\n\s*configureSolo/)[1];

test('the graphical screen adapter launches the actual engine once with all eight configured commanders',()=>{
  const run=simulation(),options={factionId:'ascendant',difficulty:'hard',style:'cautious',aiCount:7,mapId:'rift-dominion',gameMode:'relic',opponents:[{factionId:'brood',difficulty:'pro',style:'aggressive'},{factionId:'frontier',difficulty:'easy',style:'cautious'},...Array(5).fill({factionId:'ascendant',difficulty:'',style:''})]};
  const result=run.plain(`(()=>{const sfHud={hidden:false};const launch=function(options={}){${launchBody}};const accepted=launch(${JSON.stringify(options)});return {accepted,gameStarted,gameOver,paused,spectating,overlay:overlay.style.display,end:endOverlay.style.display,state:serializeState(),saved:JSON.parse(localStorage.getItem('csf.aiCommanders'))}})()`);
  assert.equal(result.accepted,true);assert.equal(result.gameStarted,true);assert.equal(result.gameOver,false);assert.equal(result.paused,false);assert.equal(result.overlay,'none');assert.equal(result.end,'none');
  assert.equal(result.state.players.length,8);assert.equal(result.state.chosenMapId,'rift-dominion');assert.equal(result.state.chosenGameMode,'relic');assert.equal(result.state.players[0].factionId,'ascendant');assert.equal(result.state.players[1].aiDifficulty,'pro');assert.equal(result.state.players[1].aiStyle,'aggressive');assert.equal(result.state.players[2].aiDifficulty,'easy');assert.equal(result.state.players[3].aiDifficulty,'hard');assert.equal(result.state.players[3].aiStyle,'cautious');assert.equal(result.saved.length,7);
});

test('solo screen entry cannot replace a live multiplayer lobby',()=>{
  const run=simulation();assert.throws(()=>run(`(()=>{const sfHud={hidden:false};const launch=function(options={}){${launchBody}};lobbyId='active-lobby';return launch({})})()`),/Leave the multiplayer lobby/);
});
