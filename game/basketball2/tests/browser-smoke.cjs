/* Run with node game/basketball2/tests/browser-smoke.cjs.
 * Optional: THREE_BUNDLE points to the game's existing Three 0.160 UMD bundle
 * for offline execution; BASKETBALL_CHROME selects a system Chrome executable.
 */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'../../..');

(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME) options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage({viewport:{width:1280,height:800},hasTouch:false});
    const errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory()) file=path.join(file,'index.html');
      if(!fs.existsSync(file)) return route.fulfill({status:404,body:'Not found'});
      // Keep real browser frames available for screenshots while advancing the
      // actual game simulation explicitly and reproducibly in each assertion.
      if(file.endsWith('bootstrap.js')) return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      return route.fulfill({path:file,contentType:file.endsWith('.html')?'text/html':file.endsWith('.css')?'text/css':file.endsWith('.webp')?'image/webp':file.endsWith('.glb')?'model/gltf-binary':'text/javascript'});
    });
    if(process.env.THREE_BUNDLE) await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    if(process.env.BASKETBALL_LIVE_MODELS!=='1') await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({
      path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),
      contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}
    }));
    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>!!window.THREE&&!!renderer);
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    for(const id of ['primaryQuickPlayBtn','primary1v1Btn','primary2v2Btn','primary3v3Btn','primaryLocalBtn','primaryTournamentBtn']){
      assert.ok(await page.locator('#'+id).isVisible(),id+' main menu entry');
    }
    await page.locator('#primary3v3Btn').click();
    assert.equal(await page.evaluate(()=>teamSize),3);
    const modes=await page.evaluate(()=>{
      const results=[];
      for(const [size,local,assignment] of [[1,false,'competitive'],[1,true,'competitive'],[2,false,'competitive'],[2,true,'cooperative'],[2,true,'competitive'],[3,false,'competitive'],[3,true,'cooperative'],[3,true,'competitive']]){
        teamSize=size;twoPlayerMode=local;localTeamMode=assignment;startGame();
        for(let i=0;i<720&&!gameOver;i++) simulateBasketball(1/60);
        const finite=matchPlayers.every(player=>player.group.position.toArray().every(Number.isFinite)&&player.velocity.toArray().every(Number.isFinite))&&ball.position.toArray().every(Number.isFinite);
        const ownership=matchPlayers.filter(player=>player.hasBall).map(player=>player.id);
        results.push({size,local,assignment,count:matchPlayers.length,teams:[0,1].map(team=>matchPlayers.filter(player=>player.teamId===team).length),humans:matchPlayers.filter(player=>player.controllerSlot).length,finite,ownership,ballOwner,ballState});
      }
      return results;
    });
    for(const mode of modes){
      assert.equal(mode.count,mode.size*2,JSON.stringify(mode));
      assert.deepEqual(mode.teams,[mode.size,mode.size]);
      assert.equal(mode.humans,mode.local?2:1);
      assert.ok(mode.finite,'finite simulation in '+JSON.stringify(mode));
      assert.ok(mode.ownership.length<=1,'single owner');
      if(mode.ballState==='dribble') assert.deepEqual(mode.ownership,[mode.ballOwner]);
    }
    console.log('PASS: eight solo/local/team configurations, AI simulation and single ownership');
    const startingSpace=await page.evaluate(()=>{
      const rows=[];
      for(const style of ['modern','classic'])for(const size of [1,2,3])for(const startingSlot of ['p1','p2']){
        inputManager.clearAll();gameSettings.set('controlStyle',style);
        teamSize=size;twoPlayerMode=true;localTeamMode='competitive';startGame();
        const owner=matchPlayers.find(player=>player.controllerSlot===startingSlot);
        const defender=matchPlayers.find(player=>player.controllerSlot&&player.teamId!==owner.teamId);
        resetPossession(owner.id,true);
        const slot=defender.controllerSlot;
        const move=style==='classic'?(slot==='p1'?'KeyW':'KeyI'):(slot==='p1'?'KeyS':'KeyK');
        const sprint=slot==='p1'?'ShiftLeft':'ShiftRight',hand=slot==='p1'?'KeyE':'KeyO';
        const key=(type,code)=>{window.dispatchEvent(new KeyboardEvent(type,{code,bubbles:true,cancelable:true}));inputManager.tick();};
        for(const code of [move,sprint,hand])key('keydown',code);
        let minimum=Infinity;
        for(let frame=0;frame<59;frame++){
          processUnifiedInputActions(defender,inputManager.getState(slot));
          simulateBasketball(1/60);
          minimum=Math.min(minimum,...getOpponents(owner).map(player=>flatDistance(player.group.position,owner.group.position)));
        }
        const toOwner=owner.group.position.clone().sub(defender.group.position).setY(0).normalize();
        const inward=defender.velocity.dot(toOwner),queued=defender.swipeTime;
        for(const code of [move,sprint,hand])key('keyup',code);
        inputManager.clearAll();simulateBasketball(1/60);simulateBasketball(1/60);
        rows.push({style,size,startingSlot,minimum,inward,queued,owner:ballOwner,expectedOwner:owner.id,steals:matchPlayers.reduce((sum,player)=>sum+player.stats.steals,0)});
      }
      gameSettings.set('controlStyle','modern');return rows;
    });
    for(const row of startingSpace){
      assert.ok(row.minimum>=3.2-1e-7,JSON.stringify(row));
      assert.ok(row.inward<=1e-7,JSON.stringify(row));assert.equal(row.queued,0);
      assert.equal(row.owner,row.expectedOwner);assert.equal(row.steals,0);
    }
    console.log('PASS: both starting slots in 1v1/2v2/3v3 keep a few steps of space through held sprint/steal inputs in modern and classic controls');
    const playbooks=await page.evaluate(()=>{
      const results=[];
      for(const style of Object.keys(OFFENSIVE_STYLES)){
        teamSize=3;twoPlayerMode=false;teamOffensiveStyles=[style,'balanced'];startGame();
        countdownTimer=0;possessionFrozen=false;
        getOpponents(p1).forEach(player=>player.aiBrain.actionCooldown=99);
        const phases=new Set(),roles=new Set();
        for(let i=0;i<480;i++){
          simulateBasketball(1/60);
          if(aiTeamPlans[0]) phases.add(aiTeamPlans[0].phase);
          getTeammates(p1).forEach(player=>roles.add(player.aiBrain.role));
        }
        results.push({style,phases:[...phases],roles:[...roles],finite:matchPlayers.every(player=>player.group.position.toArray().every(Number.isFinite))});
      }
      return results;
    });
    assert.ok(playbooks.every(play=>play.finite));
    const motion=playbooks.find(play=>play.style==='motion'),screen=playbooks.find(play=>play.style==='pick_roll'),drive=playbooks.find(play=>play.style==='drive_kick');
    assert.ok(motion.phases.includes('cut')&&motion.phases.includes('fill'),'motion runs cut/fill phases');
    assert.ok(screen.phases.includes('screen')&&screen.phases.includes('roll'),'pick and roll advances coordinated phases');
    assert.ok(drive.phases.includes('drive')&&drive.phases.includes('kick'),'drive and kick advances coordinated phases');
    console.log('PASS: all '+playbooks.length+' offense selections remain finite; motion, screen/roll and drive/kick phases execute in the real simulation');
    const autoplay=await page.evaluate(()=>{
      teamSize=3;twoPlayerMode=false;startGame();countdownTimer=0;possessionFrozen=false;
      const originalControls=updateHumanControls;
      matchPlayers.forEach(player=>player.controllerSlot=null);
      updateHumanControls=dt=>{if(shotCharging&&chargingPlayer) chargingPlayer.moveMeta.chargeHeld=(chargingPlayer.moveMeta.chargeHeld||0)+dt;};
      let passes=0,previousState=ballState;
      try{
        for(let i=0;i<9000&&!gameOver;i++){
          simulateBasketball(1/60);
          if(ballState==='pass'&&previousState!=='pass') passes++;
          previousState=ballState;
        }
        return {points:scoreP1+scoreP2,attempts:matchPlayers.reduce((n,p)=>n+p.stats.fga,0),passes,finite:matchPlayers.every(p=>p.group.position.toArray().every(Number.isFinite))};
      }finally{updateHumanControls=originalControls;}
    });
    assert.ok(autoplay.finite);assert.ok(autoplay.attempts>4,'AI creates shots');assert.ok(autoplay.points>0,'physical shots can score');assert.ok(autoplay.passes>0,'AI shares possession');
    console.log('PASS: continuous 3v3 AI possessions, passes, physical shots and scoring',autoplay);
    const pass=await page.evaluate(()=>{
      teamSize=3;twoPlayerMode=true;localTeamMode='cooperative';startGame();countdownTimer=0;possessionFrozen=false;
      const teammate=getTeammates(p1).find(player=>player!==p2);
      p1.group.position.set(0,0,0);p1.angle=Math.PI/2;p1.group.rotation.y=p1.angle;
      p2.group.position.set(4,0,0);teammate.group.position.set(-4,0,0);
      getOpponents(p1).forEach((player,index)=>player.group.position.set(-7+index,0,-9));
      matchPlayers.forEach(player=>player.velocity.set(0,0,0));scene.updateMatrixWorld(true);
      takePossession(p1,{silent:true});
      const selected=getPassTarget(p1).id,started=tryDirectionalPass(p1),immediateOwner=ballOwner;
      for(let i=0;i<90&&ballState==='pass';i++) updatePassBall(1/120);
      const caught=ballOwner;
      p1.angle=-Math.PI/2;
      const opposite=getPassTarget(p1).id;
      const passer=p1;takePossession(passer,{silent:true});passer.passCooldown=0;
      passer.angle=Math.PI/2;passer.group.rotation.y=passer.angle;
      const defender=getOpponents(passer)[0];defender.group.position.set(2,0,0);
      scene.updateMatrixWorld(true);tryDirectionalPass(passer);
      for(let i=0;i<90&&ballState==='pass';i++) updatePassBall(1/120);
      const intercepted=ballOwner===defender.id;
      return {selected,started,immediateOwner,caught,opposite,teammate:teammate.id,p2:p2.id,intercepted,ownership:matchPlayers.filter(player=>player.hasBall).length};
    });
    assert.ok(pass.started);assert.equal(pass.immediateOwner,null);assert.equal(pass.selected,pass.p2);assert.equal(pass.caught,pass.p2);
    assert.equal(pass.opposite,pass.teammate);assert.ok(pass.intercepted,'defender intercepts exposed lane');assert.equal(pass.ownership,1);
    console.log('PASS: facing selects receivers, travel-time catches and defender interception');
    const transitions=await page.evaluate(()=>{
      const teammate=getTeammates(p1).find(player=>!player.controllerSlot);
      takePossession(teammate,{silent:true});const oldP1=p1;
      shotCharging=true;chargingPlayer=teammate;teammate.moveMeta.chargeHeld=.4;
      switchControlledPlayer('p1');const switched=p1===teammate&&oldP1.controllerSlot===null&&teammate.controllerSlot==='p1';
      const chargeCleared=!shotCharging&&chargingPlayer===null&&teammate.moveMeta.chargeHeld===0;
      lastScored=false;scoreP1=0;scoreP2=0;awardBasket(teammate,3,'Jumper');scheduleScoredPossession(teammate.id,200);
      const scored=scoreP1===3&&scoreP2===0;
      setPaused(true);const remaining=pendingInbound.remaining;
      const stable=gamePaused&&pendingInbound.remaining===remaining;
      setPaused(false);for(let i=0;i<13;i++) simulateBasketball(1/60);
      const inbound=ballState==='dribble'&&teamOf(ballOwner)===1;
      rematchSamePlayers();const reset=scoreP1===0&&scoreP2===0&&!gameOver&&pendingInbound===null;
      return {switched,chargeCleared,scored,stable,inbound,reset};
    });
    assert.deepEqual(transitions,{switched:true,chargeCleared:true,scored:true,stable:true,inbound:true,reset:true});
    console.log('PASS: optional switching, AI teammate team score, paused inbound and rematch');
    // Exercise real keyboard events with independent movement/control frames.
    await page.evaluate(()=>{teamSize=2;twoPlayerMode=true;localTeamMode='competitive';startGame();countdownTimer=0;possessionFrozen=false;});
    await page.keyboard.down('d');
    const input=await page.evaluate(()=>{inputManager.tick();const first=inputManager.getState('p1').moveX,second=inputManager.getState('p2').moveX;for(let i=0;i<30;i++) simulateBasketball(1/60);return {first,second,x:p1.group.position.x};});
    await page.keyboard.up('d');
    assert.equal(input.first,1);assert.equal(input.second,0);assert.ok(input.x>0);
    console.log('PASS: real keyboard isolation');
    for(const split of ['vertical','horizontal']){
      await page.setViewportSize({width:1281,height:801});
      const aspects=await page.evaluate(layout=>{splitLayout=layout;onResize();updateCameras(1);updateHUD();renderScene();return [camP1.aspect,camP2.aspect];},split);
      assert.ok(aspects.every(Number.isFinite));
    }
    await page.evaluate(()=>{teamSize=3;twoPlayerMode=true;localTeamMode='cooperative';splitLayout='vertical';startGame();countdownTimer=0;possessionFrozen=false;for(let i=0;i<50;i++)simulateBasketball(1/60);updateCameras(1);updateHUD();renderScene();});
    if(process.env.BASKETBALL_SCREENSHOTS) await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'basketball2-desktop.png')});
    await page.setViewportSize({width:844,height:390});
    await page.emulateMedia({reducedMotion:'reduce'});
    const touch=await page.evaluate(()=>{teamSize=3;twoPlayerMode=false;startGame();document.body.classList.add('touch-controls-active');countdownTimer=0;possessionFrozen=false;onResize();updateCameras(1);updateHUD();renderScene();return {buttons:document.querySelectorAll('.touch-actions button').length,stick:!!document.querySelector('#moveStick'),skill:!!document.querySelector('#skillStick')};});
    assert.deepEqual(touch,{buttons:2,stick:true,skill:false});
    if(process.env.BASKETBALL_SCREENSHOTS) await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'basketball2-mobile.png')});
    await page.evaluate(()=>{setPaused(true);document.getElementById('pauseMenuBtn').click();});
    assert.ok(await page.locator('#primaryMenuScreen').isVisible());
    assert.deepEqual(errors,[],'no browser runtime exceptions');
    console.log('PASS: split resize, landscape touch layout, pause/menu lifecycle; no browser exceptions');
    const tour=await page.evaluate(()=>{
      const quick=localStorage.getItem('basketball2MatchV1');
      startNewTournament();startTournamentRound();
      const oneOnOne=teamSize===1&&!twoPlayerMode&&matchPlayers.length===2;
      scoreP1=11;scoreP2=4;endGame('p1');
      const saved=JSON.parse(localStorage.getItem(TOURNAMENT_SAVE_KEY));
      tournamentState=null;loadTournament();
      const restored=tournamentState.currentStopIndex===saved.currentStopIndex&&tournamentState.playerWins===1&&tournamentState.completedGames.length===1;
      showPrimaryMenu();showQuickPlaySetup();
      return {oneOnOne,restored,quickPreserved:localStorage.getItem('basketball2MatchV1')===quick,modeRestored:teamSize===JSON.parse(quick).teamSize};
    });
    assert.deepEqual(tour,{oneOnOne:true,restored:true,quickPreserved:true,modeRestored:true});
    assert.deepEqual(errors,[]);
    console.log('PASS: National Tour progression/save/load and independent quick-play preferences');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
