/* Held keyboard input, charged GLB hands and the ordinary physical shot flight.
 * Runtime overrides match player-gameplay-browser.cjs. SHOT_ANGLE_SAMPLES can
 * increase the paired seeded sample count from the default 100 (maximum 300). */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'../../..');
const samples=Math.max(100,Math.min(300,Number(process.env.SHOT_ANGLE_SAMPLES)||100));
(async()=>{
 const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
 if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
 const browser=await chromium.launch(options);
 try{
  const page=await browser.newPage({viewport:{width:1200,height:800}}),errors=[];page.on('pageerror',e=>errors.push(e.stack));
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
   globalThis.measureHeldMidrange=({profile,specialist=false,degrees=0,condition='clean',samples})=>{
    const random=Math.random;let seed=42871;Math.random=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};
    selectedP1=profile;selectedP2='tower';teamSize=1;twoPlayerMode=false;isTournamentGame=false;tutorialMode=false;startGame();targetScore=100000;
    let made=0,attempts=0,finite=true,model=null,releaseMode=null,contestTotal=0,speedTotal=0,spreadTotal=0,wrongZone=0,types={};
    const key=(type,code,key)=>{window.dispatchEvent(new KeyboardEvent(type,{code,key,bubbles:true,cancelable:true}));inputManager.tick();processUnifiedInputActions(p1,inputManager.getState('p1'));};
    for(let attempt=0;attempt<samples;attempt++){
     inputManager.clearAll();gameOver=false;gamePaused=false;pendingInbound=null;possessionFrozen=false;countdownTimer=0;
     matchPlayers.forEach(resetBody);p1.stamina=condition==='fatigued'?.25:1;p1.sprint=1;
     p1.profile.ratings=Object.fromEntries(RiseToGlory.ATTRIBUTE_KEYS.map(key=>[key,specialist?88:60]));
     p1.profile.abilities=specialist?{midrangeAssassin:1}:{};
     Object.keys(p1.stats).forEach(key=>p1.stats[key]=0);
     const angle=degrees*Math.PI/180;p1.group.position.set(Math.sin(angle)*5.8,0,HOOP_POS.z+Math.cos(angle)*5.8);
     p1.angle=angleToward(p1.group.position,HOOP_POS);p1.group.rotation.set(0,p1.angle,0);p1.velocity.set(0,0,0);
     p2.group.position.set(8,0,8);p2.angle=p1.angle+Math.PI;p2.group.rotation.set(0,p2.angle,0);
     if(condition==='contested'){p2.group.position.copy(p1.group.position).addScaledVector(getForward(p1.angle),1.9);p2.handsUpTime=10;}
     takePossession(p1,{silent:true});updateCharacters(0);scene.updateMatrixWorld(true);
     const meter=getShotMeterProfile('setshot',p1,calculateShotContest(p1));
     const held=(meter.greenStart+meter.greenEnd)*.5*1.35/meter.meterFillSpeed;
     key('keydown','Space',' ');
     if(!shotCharging||chargingPlayer!==p1)throw Error('The keyboard did not begin a human shot');
     const frames=Math.round(held*60);
     for(let frame=0;frame<frames;frame++){
      if(condition==='moving'&&frame===frames-10)key('keydown','KeyA','a');
      simulateBasketball(1/60);
     }
     model=getBasketballPlayerModelKind(p1);releaseMode=p1.playerModel.mode;
     contestTotal+=calculateShotContest(p1).value;speedTotal+=p1.velocity.length();
     seed=42871+attempt*997;key('keyup','Space',' ');
     if(condition==='moving')key('keyup','KeyA','a');
     if(p1.stats.fga!==1)throw Error('Keyboard release must create exactly one normal attempt: '+JSON.stringify({profile,degrees,condition,stats:p1.stats,mode:releaseMode}));
     if(lastShotValue!==2)wrongZone++;
     types[p1.moveMeta.lastShotType]=(types[p1.moveMeta.lastShotType]||0)+1;spreadTotal+=p1.moveMeta.lastAimSpread;
     for(let frame=0;frame<150&&!lastScored;frame++){
      updateBall(1/60);loosePickupDelay=Math.max(0,loosePickupDelay-1/60);ballCollisionCooldown=Math.max(0,ballCollisionCooldown-1/60);backboardCollisionCooldown=Math.max(0,backboardCollisionCooldown-1/60);
     }
     made+=lastScored?1:0;attempts+=p1.stats.fga;
     finite=finite&&ball.position.toArray().every(Number.isFinite)&&lastShotReleasePos.toArray().every(Number.isFinite);
    }
    Math.random=random;return {profile,model,specialist,degrees,condition,samples,made,attempts,rate:made/samples,finite,releaseMode,contest:contestTotal/samples,speed:speedTotal/samples,spread:spreadTotal/samples,wrongZone,types};
   };
  });
  const rows=[];
  for(const profile of ['flash','smooth','tower'])for(const specialist of [false,true]){
   const group=[];
   for(const degrees of [0,-60,60,-85,85]){
    const row=await page.evaluate(config=>measureHeldMidrange(config),{profile,specialist,degrees,samples});rows.push(row);group.push(row);
   }
   console.log(JSON.stringify(group));
  }
  const controls=[];
  for(const condition of ['contested','moving','fatigued'])controls.push(await page.evaluate(config=>measureHeldMidrange(config),{profile:'smooth',degrees:0,condition,samples}));
  console.log('Controls:',JSON.stringify(controls));
  if(process.env.BASKETBALL_SCREENSHOTS){fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});fs.writeFileSync(path.join(process.env.BASKETBALL_SCREENSHOTS,'side-midrange-results.json'),JSON.stringify({samples,rows,controls},null,2));}
  for(const row of [...rows,...controls]){
   assert.ok(row.finite&&row.releaseMode==='charge','real charged GLB pose and finite flight: '+JSON.stringify(row));
   assert.equal(row.attempts,samples);assert.equal(row.wrongZone,0,'all baseline/wing attempts are actual two-point midrange shots');
  }
  const average=values=>values.reduce((sum,row)=>sum+row.rate,0)/values.length;
  for(const profile of ['flash','smooth','tower']){
   const regular=rows.filter(r=>r.profile===profile&&!r.specialist),special=rows.filter(r=>r.profile===profile&&r.specialist);
   assert.ok(average(regular)>=.56&&average(regular)<=.8,'open average held midrange remains playable: '+JSON.stringify(regular));
   assert.ok(average(special)>average(regular)+.045,'specialist development improves actual shooting: '+JSON.stringify({regular,special}));
   for(const group of [regular,special]){
    const front=group.find(r=>r.degrees===0).rate;
    assert.ok(group.every(r=>r.rate>.5&&r.rate<.94),'open side shots include playable physical makes and misses: '+JSON.stringify(group));
    assert.ok(group.every(r=>Math.abs(r.rate-front)<.18),'side accuracy stays comparable to the same front shot: '+JSON.stringify(group));
   }
  }
  const clean=rows.find(r=>r.profile==='smooth'&&!r.specialist&&r.degrees===0);
  for(const row of controls)assert.ok(row.rate<clean.rate-.08,'coverage, motion and fatigue keep their penalties: '+JSON.stringify({clean,row}));
  assert.ok(controls.find(r=>r.condition==='contested').contest>.2);
  assert.ok(controls.find(r=>r.condition==='moving').speed>2);
  assert.deepEqual(errors,[]);
  console.log('PASS: held human midrange at front/left/right wings and baselines across three GLB builds; specialist gains, normal misses and coverage/motion/fatigue penalties.');
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
