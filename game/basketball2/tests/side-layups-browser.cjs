/* Real held keyboard approaches, active GLB gathers, release and swept rim
 * flight. Movement is never supplied by a preset velocity or shot success stub. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'../../..');
const samples=Math.max(3,Math.min(40,Number(process.env.SIDE_LAYUP_SAMPLES)||12));
(async()=>{
 const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
 if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
 const browser=await chromium.launch(options);
 try{
  const page=await browser.newPage({viewport:{width:1280,height:800}}),errors=[];page.on('pageerror',e=>errors.push(e.stack));
  await page.route('http://basketball.local/**',route=>{
   let file=path.join(root,new URL(route.request().url()).pathname);if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
   if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
   const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.glb':'model/gltf-binary','.webp':'image/webp','.json':'application/json'};
   if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
   return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
  });
  if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
  await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
  await page.goto('http://basketball.local/game/basketball2/');await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
  await page.evaluate(()=>{
   updateTeamAI=()=>{};customRosterDraft.enabled=false;gameSettings.set('controlStyle','modern');gameSettings.set('gameplayPreset','simulation');
   globalThis.measureRunningLayups=({profile,degrees=45,sprint=false,holdMovement=true,held=.08,pressDistance=3.35,offsetZ=0,condition='open',stopOnPress=false,samples})=>{
    const originalRandom=Math.random;let seed=42871;Math.random=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};
    selectedP1=profile==='rookie'?'crafty':profile;selectedP2='tower';teamSize=1;twoPlayerMode=false;isTournamentGame=false;tutorialMode=false;startGame();targetScore=100000;
    if(profile==='rookie'){
      const state=RiseToGlory.create({seed:24}),athlete=RiseToGlory.player(state,state.captainId);
      disposeMatchPlayers();p1=createCharacter(RiseToGlory.toProfile(athlete,state.team),P1_OFFENSE_START,'p1');
      p2=createCharacter({...profiles.tower},P2_DEFENSE_START,'p2');scene.add(p1.group);scene.add(p2.group);setupMatchPlayers();
    }
    const key=(type,code,key)=>{window.dispatchEvent(new KeyboardEvent(type,{code,key,bubbles:true,cancelable:true}));inputManager.tick();processUnifiedInputActions(p1,inputManager.getState('p1'));};
    const movement=degrees===0?[['KeyW','w']]:degrees===-45?[['KeyD','d'],['KeyW','w']]:degrees===45?[['KeyA','a'],['KeyW','w']]:degrees<0?[['KeyD','d']]:[['KeyA','a']];
    const results=[];
    for(let attempt=0;attempt<samples;attempt++){
     inputManager.clearAll();gameOver=false;gamePaused=false;pendingInbound=null;possessionFrozen=false;countdownTimer=0;
     matchPlayers.forEach(resetBody);p1.stamina=condition==='fatigued'?.25:1;p1.sprint=1;Object.keys(p1.stats).forEach(key=>p1.stats[key]=0);
     const angle=degrees*Math.PI/180;p1.group.position.set(Math.sin(angle)*6,0,HOOP_POS.z+Math.cos(angle)*6+offsetZ);
     p1.angle=angleToward(p1.group.position,HOOP_POS);p1.group.rotation.set(0,p1.angle,0);p1.velocity.set(0,0,0);
     p2.group.position.set(8,0,8);takePossession(p1,{silent:true});updateCharacters(0);scene.updateMatrixWorld(true);
     for(const [code,value]of movement)key('keydown',code,value);if(sprint)key('keydown','ShiftLeft','Shift');
     let approachFrames=0;
     while(flatDistance(p1.group.position,HOOP_POS)>pressDistance&&approachFrames++<180)simulateBasketball(1/60);
     const press={distance:flatDistance(p1.group.position,HOOP_POS),speed:p1.velocity.length(),type:chooseShotType(p1),position:p1.group.position.clone()};
     key('keydown','Space',' ');
     if(stopOnPress){for(const [code,value]of movement)key('keyup',code,value);if(sprint)key('keyup','ShiftLeft','Shift');}
     for(let frame=0;frame<Math.round(held*60);frame++)simulateBasketball(1/60);key('keyup','Space',' ');
     const gatherStart=p1.group.position.clone(),gatherType=ballState;
     if(condition==='contested'||condition==='moderate'){
       const toward=HOOP_POS.clone().sub(p1.group.position).setY(0).normalize(),across=new THREE.Vector3(-toward.z,0,toward.x);
       p2.group.position.copy(p1.group.position).addScaledVector(toward,condition==='moderate'?2.7:1.8).addScaledVector(across,.7);
       p2.angle=angleToward(p2.group.position,p1.group.position);p2.group.rotation.y=p2.angle;p2.handsUpTime=5;
     }
     const gatherContest=calculateShotContest(p1).value;
     if(!holdMovement){for(const [code,value]of movement)key('keyup',code,value);if(sprint)key('keyup','ShiftLeft','Shift');}
     seed=42871+attempt*997;
     let release=null,firstContact=null,gatherMode=null,rootClosest=100,blocked=false,gatherTravel=0;
     for(let frame=0;frame<180&&!lastScored;frame++){
      simulateBasketball(1/60);rootClosest=Math.min(rootClosest,flatDistance(p1.group.position,HOOP_POS));
      blocked=blocked||layupBlocked;
      if(layupActive){gatherMode=p1.playerModel.mode;gatherTravel=Math.max(gatherTravel,flatDistance(p1.group.position,gatherStart));}
      if(!release&&ballState==='shot')release={frame,position:lastShotReleasePos.toArray(),root:p1.group.position.toArray(),rootDistance:flatDistance(p1.group.position,HOOP_POS),distance:flatDistance(lastShotReleasePos,HOOP_POS),speed:p1.velocity.length(),type:p1.moveMeta.lastShotType,grade:p1.moveMeta.lastTimingGrade,spread:p1.moveMeta.lastAimSpread,travel:flatDistance(p1.group.position,gatherStart)};
      if(!firstContact&&(rimTouchCount||shotHasHitRimOrBoard))firstContact={frame,rim:rimTouchCount,board:shotHasHitRimOrBoard};
      if(frame===50){for(const [code,value]of movement)key('keyup',code,value);if(sprint)key('keyup','ShiftLeft','Shift');}
     }
     results.push({made:lastScored,attempts:p1.stats.fga,press:{distance:press.distance,speed:press.speed,type:press.type},gatherType,gatherMode,release,firstContact,rootClosest,gatherTravel,gatherContest,blocked,
      finalState:ballState,finite:ball.position.toArray().every(Number.isFinite)&&p1.group.position.toArray().every(Number.isFinite)});
    }
    Math.random=originalRandom;
    return {profile,degrees,sprint,holdMovement,held,pressDistance,offsetZ,condition,samples,made:results.filter(r=>r.made).length,attempts:results.reduce((s,r)=>s+r.attempts,0),results};
   };
  });
  const rows=[];
  for(const profile of ['flash','smooth','tower'])for(const sprint of [false,true])for(const degrees of [-90,-45,0,45,90]){
   const row=await page.evaluate(config=>measureRunningLayups(config),{profile,degrees,sprint,holdMovement:true,samples});rows.push(row);
   console.log(JSON.stringify({profile,degrees,sprint,made:row.made,samples,example:row.results[0]}));
  }
  const released=await page.evaluate(config=>measureRunningLayups(config),{profile:'smooth',degrees:90,sprint:true,holdMovement:false,samples});
  console.log('Release movement comparison:',JSON.stringify(released));
  const unusual=[];
  for(const profile of ['smooth','rookie'])for(const degrees of [45,90])for(const held of [.08,.22,.25,.35,.5,.9]){
    const row=await page.evaluate(config=>measureRunningLayups(config),{profile,degrees,sprint:true,holdMovement:true,samples,held});unusual.push(row);
    console.log('Approach variation:',JSON.stringify({profile,degrees,held,made:row.made,samples,example:row.results[0]}));
  }
  const controls=[];
  for(const condition of ['moderate','contested','fatigued','overheld']){
    const row=await page.evaluate(config=>measureRunningLayups(config),{profile:'rookie',degrees:90,sprint:true,holdMovement:true,samples,held:condition==='overheld'?1.6:.35,stopOnPress:condition==='overheld',condition});controls.push(row);
    console.log('Control:',JSON.stringify({condition,made:row.made,samples,example:row.results[0]}));
  }
  if(process.env.BASKETBALL_SCREENSHOTS){fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});fs.writeFileSync(path.join(process.env.BASKETBALL_SCREENSHOTS,'side-layup-results.json'),JSON.stringify({samples,rows,released,unusual,controls},null,2));}
  for(const row of [...rows,released,...unusual,...controls]){
    assert.equal(row.attempts,samples,'exactly one real attempt per keyboard action');
    for(const attempt of row.results){
      assert.ok(attempt.finite&&attempt.press.speed>1.2,'finite shot after a real keyboard running approach: '+JSON.stringify({row,attempt}));
      assert.equal(attempt.press.type,'layup');assert.equal(attempt.gatherType,'layup');
      assert.ok(attempt.gatherTravel<=.55,'only the gather solver advances the active finish: '+JSON.stringify(attempt));
      if(!attempt.blocked){assert.equal(attempt.release?.type,'layup');assert.equal(attempt.gatherMode,'layup');}
    }
  }
  for(const row of rows)assert.ok(row.made>=samples*.8,'open side/front taps remain reliable across held movement, sprint and GLB builds: '+JSON.stringify(row));
  for(const profile of ['smooth','rookie'])for(const degrees of [45,90]){
    const group=unusual.filter(row=>row.profile===profile&&row.degrees===degrees),short=group.find(row=>row.held===.22);
    for(const row of group.filter(row=>row.held<=.35))assert.ok(row.made>=samples*.7,'ordinary short holds preserve a playable open layup: '+JSON.stringify(row));
    assert.ok(group.find(row=>row.held===.25).made>=short.made-samples*.2,'an extra release frame does not discard the gather touch');
    assert.ok(group.find(row=>row.held===.35).made>=short.made-samples*.25,'touch falls smoothly across common short holds');
  }
  const clean=unusual.find(row=>row.profile==='rookie'&&row.degrees===90&&row.held===.35);
  const moderate=controls.find(row=>row.condition==='moderate'),covered=controls.find(row=>row.condition==='contested'),tired=controls.find(row=>row.condition==='fatigued'),overheld=controls.find(row=>row.condition==='overheld');
  assert.ok(moderate.results.every(shot=>shot.gatherContest>.12&&shot.gatherContest<.5),'control has real moderate visible pressure');
  assert.ok(moderate.made<clean.made&&covered.made<clean.made,'coverage and hands-up contact retain physical risk');
  assert.ok(covered.results.some(shot=>shot.blocked),'a real defender can interrupt the gathered ball');
  const spread=row=>row.results.reduce((sum,shot)=>sum+(shot.release?.spread||0),0)/row.results.length;
  assert.ok(spread(tired)>spread(clean)+.03,'fatigue still increases actual launch dispersion');
  assert.ok(overheld.made<=samples*.35,'overholding retains normal timing mistakes');
  assert.ok(unusual.some(row=>row.held>=.5&&row.made<samples),'normal physical layups can still miss');
  assert.deepEqual(errors,[]);
  console.log('PASS: real held human approaches at both wings/baselines, all three GLB builds, sprint, rookie short/green holds; controlled gather travel, continuous timing, and visible contest/block/fatigue/overhold risk.');
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
