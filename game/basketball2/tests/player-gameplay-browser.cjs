/* Real GLB rendering and real keyboard shooting through the full game loop.
 * Uses the same optional environment variables as browser-smoke.cjs.
 * BASKETBALL_LIVE_MODELS=1 tests the actual public R2 host and its CORS policy. */
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
    const page=await browser.newPage({viewport:{width:1440,height:900},hasTouch:false});
    const errors=[],warnings=[];
    page.on('pageerror',error=>errors.push(error.stack));
    page.on('console',message=>{if(message.type()==='warning'&&message.text().includes('player models'))warnings.push(message.text());});
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.webp':'image/webp','.glb':'model/gltf-binary','.json':'application/json'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    if(process.env.BASKETBALL_LIVE_MODELS!=='1')await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({
      path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),
      contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}
    }));
    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    const sources=await page.evaluate(()=>[...basketballPlayerModels.assets.values()].map(asset=>asset.label));
    assert.ok(sources.every(source=>source.startsWith('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/')),'all player builds use the primary hosted source');
    if(process.env.BASKETBALL_LIVE_MODELS==='1')console.log('PASS: actual public R2 host loads all three GLBs through browser CORS, with no bundled fallback');
    const rigs=await page.evaluate(()=>{
      teamSize=3;twoPlayerMode=true;localTeamMode='cooperative';startGame();
      return matchPlayers.map(char=>({kind:getBasketballPlayerModelKind(char),bones:char.playerModel.bones.size,clips:char.playerModel.actions.size,
        skinned:char.playerModel.nodes.reduce((sum,node)=>sum+node.children.filter(child=>child.isSkinnedMesh).length,0),
        legacyHidden:char.legacyVisualNodes.every(node=>!node.visible),marker:char.teamRing.visible}));
    });
    assert.ok(rigs.every(rig=>rig.bones===24&&rig.clips===12&&rig.skinned>=1&&rig.legacyHidden&&rig.marker));
    const motion=await page.evaluate(()=>{
      const char=p1;char.hasBall=false;takePossession(getOpponents(char)[0],{silent:true});
      char.velocity.set(3.8,0,0);char.onGround=true;char.blockTime=0;char.handsUpTime=0;
      const points=[];
      for(let i=0;i<45;i++){updatePlayerModel(char,1/60);points.push(getModelJointWorld(char,'foot_L').z);}
      return {range:Math.max(...points)-Math.min(...points),mode:char.playerModel.mode};
    });
    assert.ok(motion.range>.08,'authored foot bones move during locomotion');
    console.log('PASS: six real skinned GLB players, 24-bone rigs, 12 clips, shared assets and animated feet');

    // Keep the actual input, animation, gather, collision and scoring systems.
    // Hold opponents outside the shot so open-shot results measure shot mechanics.
    await page.evaluate(()=>{globalThis.savedTeamAI=updateTeamAI;updateTeamAI=()=>{};gameSettings.set('controlStyle','modern');});
    const attempts=[];
    for(const profile of ['flash','smooth','tower']){
      await page.evaluate(key=>{selectedP1=key;teamSize=1;twoPlayerMode=false;isTournamentGame=false;startGame();},profile);
      const cases=[
        ...[-70,0,70].flatMap(angle=>[.6,1.6,2.7].map(distance=>({distance,angle,held:.08,expected:'layup'}))),
        ...[4.2,5.8,7.6].flatMap(distance=>[.545,.635,.725].map(pct=>({distance,angle:0,held:pct*1.35,expected:'setshot'})))
      ];
      for(const config of cases){
        await page.evaluate(config=>{
          inputManager.clearAll();gameOver=false;gamePaused=false;pendingInbound=null;scoreP1=0;scoreP2=0;
          matchPlayers.forEach(resetBody);countdownTimer=0;possessionFrozen=false;
          const angle=config.angle*Math.PI/180;
          p1.group.position.set(Math.sin(angle)*config.distance,0,HOOP_POS.z+Math.cos(angle)*config.distance);
          p1.angle=angleToward(p1.group.position,HOOP_POS);p1.group.rotation.set(0,p1.angle,0);
          p1.stamina=1;p1.sprint=1;Object.keys(p1.stats).forEach(key=>p1.stats[key]=0);
          getOpponents(p1).forEach(player=>{player.group.position.set(8,0,8);player.blockTime=0;player.handsUpTime=0;});
          takePossession(p1,{silent:true});updatePlayerModel(p1,1/60);scene.updateMatrixWorld(true);
        },config);
        await page.keyboard.down('Space');
        await page.evaluate(held=>{
          inputManager.tick();processUnifiedInputActions(p1,inputManager.getState('p1'));
          for(let i=0;i<Math.round(held*60);i++)simulateBasketball(1/60);
        },config.held);
        await page.keyboard.up('Space');
        const result=await page.evaluate(()=>{
          inputManager.tick();processUnifiedInputActions(p1,inputManager.getState('p1'));
          for(let i=0;i<240&&!lastScored;i++)simulateBasketball(1/60);
          return {made:scoreP1>0,attempts:p1.stats.fga,grade:p1.moveMeta.lastTimingGrade,type:p1.moveMeta.lastShotType,ballState,
            finite:ball.position.toArray().every(Number.isFinite),model:getBasketballPlayerModelKind(p1)};
        });
        attempts.push({profile,...config,...result});
        assert.ok(result.finite,JSON.stringify(attempts.at(-1)));
        assert.equal(result.type,config.expected,JSON.stringify(attempts.at(-1)));
        assert.equal(result.attempts,1,JSON.stringify(attempts.at(-1)));
        assert.ok(result.made,'open real-keyboard shot should score: '+JSON.stringify(attempts.at(-1)));
      }
    }
    console.log('PASS: '+attempts.length+' real-keyboard open layups/jumpers across all three player builds and both green edges');
    await page.evaluate(()=>{updateTeamAI=globalThis.savedTeamAI;teamSize=3;twoPlayerMode=true;localTeamMode='competitive';selectedP1='smooth';startGame();countdownTimer=0;possessionFrozen=false;for(let i=0;i<180;i++)simulateBasketball(1/60);for(let i=0;i<90;i++)updateCameras(1/60);onResize();updateHUD();renderScene();});
    if(process.env.BASKETBALL_SCREENSHOTS)await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'basketball2-glb-gameplay.png')});
    for(const layout of ['vertical','horizontal']){
      await page.setViewportSize({width:1281,height:801});
      await page.evaluate(value=>{splitLayout=value;onResize();updateCameras(1/60);renderScene();},layout);
    }
    await page.setViewportSize({width:844,height:390});
    await page.evaluate(()=>{teamSize=3;twoPlayerMode=false;startGame();countdownTimer=0;possessionFrozen=false;document.body.classList.add('touch-controls-active');for(let i=0;i<90;i++)updateCameras(1/60);onResize();updateHUD();renderScene();});
    if(process.env.BASKETBALL_SCREENSHOTS)await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'basketball2-glb-mobile.png')});
    const disposal=await page.evaluate(()=>{
      const before=renderer.info.memory.geometries;
      for(let i=0;i<5;i++){startGame();countdownTimer=0;possessionFrozen=false;updateCharacters(1/60);renderScene();}
      return {before,after:renderer.info.memory.geometries,ready:matchPlayers.every(char=>!!char.playerModel)};
    });
    assert.ok(disposal.ready);assert.ok(disposal.after<=disposal.before+5,'rematches retain shared buffers and release per-player resources: '+JSON.stringify(disposal));
    assert.deepEqual(warnings,[]);assert.deepEqual(errors,[]);
    console.log('PASS: split/mobile rendering and repeated player-model lifecycle, no fallback or browser exceptions');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
