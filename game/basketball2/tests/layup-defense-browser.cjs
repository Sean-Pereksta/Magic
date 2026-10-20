/* Actual keyboard approaches, visible GLB defenders, gather and normal ball
 * flight. The fixture never changes the shot target, velocity or scoring. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {installLegacyGraphicsFixture}=require('./legacy-graphics-fixture.cjs');
const root=path.resolve(__dirname,'../../..');
const samples=Math.max(6,Math.min(40,Number(process.env.LAYUP_DEFENSE_SAMPLES)||18));
(async()=>{
 const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
 if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
 const browser=await chromium.launch(options);
 try{
  const page=await browser.newPage({viewport:{width:1280,height:800}}),errors=[];
  page.on('pageerror',error=>errors.push(error.stack));
  await page.route('http://basketball.local/**',route=>{
   let file=path.join(root,new URL(route.request().url()).pathname);
   if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
   if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
   const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.glb':'model/gltf-binary','.webp':'image/webp','.json':'application/json'};
   if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
   return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
  });
  if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
  await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
  await installLegacyGraphicsFixture(page);

  await page.goto('http://basketball.local/game/basketball2/');
  await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
  await page.evaluate(()=>{
   updateTeamAI=()=>{};customRosterDraft.enabled=false;gameSettings.set('controlStyle','modern');gameSettings.set('gameplayPreset','simulation');
   globalThis.measureLayupDefense=({profile='smooth',condition='open',degrees=0,samples,defenderOffset})=>{
    const originalRandom=Math.random;let seed=53091;
    Math.random=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};
    selectedP1=profile==='rookie'?'crafty':profile;selectedP2='tower';teamSize=1;twoPlayerMode=false;isTournamentGame=false;tutorialMode=false;startGame();targetScore=100000;
    if(profile==='rookie'){
     const state=RiseToGlory.create({seed:24}),athlete=RiseToGlory.player(state,state.captainId);
     disposeMatchPlayers();p1=createCharacter(RiseToGlory.toProfile(athlete,state.team),P1_OFFENSE_START,'p1');
     p2=createCharacter({...profiles.tower},P2_DEFENSE_START,'p2');scene.add(p1.group);scene.add(p2.group);setupMatchPlayers();
    }
    const key=(type,code,value)=>{window.dispatchEvent(new KeyboardEvent(type,{code,key:value,bubbles:true,cancelable:true}));inputManager.tick();processUnifiedInputActions(p1,inputManager.getState('p1'));};
    const movement=degrees===0?[['KeyW','w']]:degrees<0?[['KeyD','d']]:[['KeyA','a']];
    const results=[];
    for(let attempt=0;attempt<samples;attempt++){
     inputManager.clearAll();gameOver=false;gamePaused=false;pendingInbound=null;possessionFrozen=false;countdownTimer=0;
     matchPlayers.forEach(resetBody);p1.stamina=p2.stamina=1;p1.sprint=p2.sprint=1;
     Object.keys(p1.stats).forEach(key=>p1.stats[key]=0);Object.keys(p2.stats).forEach(key=>p2.stats[key]=0);
     const angle=degrees*Math.PI/180;p1.group.position.set(Math.sin(angle)*6,0,HOOP_POS.z+Math.cos(angle)*6);
     p1.angle=angleToward(p1.group.position,HOOP_POS);p1.group.rotation.set(0,p1.angle,0);p1.velocity.set(0,0,0);
     p2.group.position.set(8,0,8);p2.velocity.set(0,0,0);takePossession(p1,{silent:true});updateCharacters(0);scene.updateMatrixWorld(true);
     for(const [code,value]of movement)key('keydown',code,value);
     let approachFrames=0;while(flatDistance(p1.group.position,HOOP_POS)>3.1&&approachFrames++<180)simulateBasketball(1/60);
     const press={distance:flatDistance(p1.group.position,HOOP_POS),speed:p1.velocity.length(),type:chooseShotType(p1)};
     key('keydown','Space',' ');for(let frame=0;frame<5;frame++)simulateBasketball(1/60);
     const toward=HOOP_POS.clone().sub(p1.group.position).setY(0).normalize(),across=new THREE.Vector3(-toward.z,0,toward.x);
     if(condition!=='open'){
      const offsets={trailing:[-1.2,.15],side:[-.25,1.35],squared:[1.65,.35],timed:[1.45,.1],lateDive:[-1.3,.3]};
      const [forward,side]=defenderOffset||offsets[condition];p2.group.position.copy(p1.group.position).addScaledVector(toward,forward).addScaledVector(across,side);
      p2.angle=angleToward(p2.group.position,p1.group.position);p2.group.rotation.y=p2.angle;
      if(condition==='squared')raiseHands(p2);
     }
     updateCharacters(0);scene.updateMatrixWorld(true);
     const preview=calculateShotContest(p1).value;
     key('keyup','Space',' ');
     seed=53091+attempt*997;
     let release=null,blocked=false,blockContact=null,blockScheduled=false,maxJumpTravel=0,jumpStart=null,contact=null;
     const originalDeflection=applyBlockDeflection,originalLayupDeflection=deflectLayupBall;
     const segmentDistance=(a,b,point)=>{
      const direction=b.clone().sub(a),time=THREE.MathUtils.clamp(point.clone().sub(a).dot(direction)/Math.max(direction.lengthSq(),.0001),0,1);
      return a.clone().addScaledVector(direction,time).distanceTo(point);
     };
     const recordContact=defender=>({distance:Math.min(getWorldHand(defender,'left').distanceTo(ball.position),getWorldHand(defender,'right').distanceTo(ball.position)),sweptDistance:Math.min(segmentDistance(previousBallPosition,ball.position,getWorldHand(defender,'left')),segmentDistance(previousBallPosition,ball.position,getWorldHand(defender,'right'))),stepLength:previousBallPosition.distanceTo(ball.position),previous:previousBallPosition.toArray(),position:ball.position.toArray(),left:getWorldHand(defender,'left').toArray(),right:getWorldHand(defender,'right').toArray()});
     applyBlockDeflection=(defender,shooter,hit)=>{blockContact={...recordContact(defender),verifiedDistance:hit?.distance,verifiedRadius:hit?.radius,verifiedPoint:hit?.point?.toArray()};return originalDeflection(defender,shooter,hit);};
     deflectLayupBall=(owner,defender,hit)=>{contact={type:hit?.type,point:hit?.point?.toArray(),...recordContact(defender)};return originalLayupDeflection(owner,defender,hit);};
     try{
      for(let frame=0;frame<180&&!lastScored;frame++){
       if((condition==='timed'||condition==='lateDive')&&!blockScheduled&&frame===7){jumpStart=p2.group.position.clone();tryJumpBlock(p2,condition==='lateDive');blockScheduled=true;}
       if(condition==='squared'&&frame<30)raiseHands(p2);
       simulateBasketball(1/60);blocked=blocked||layupBlocked||p2.stats.blocks>0;
       if(jumpStart&&!p2.onGround)maxJumpTravel=Math.max(maxJumpTravel,flatDistance(p2.group.position,jumpStart));
       if(!release&&ballState==='shot')release={frame,type:p1.moveMeta.lastShotType,grade:p1.moveMeta.lastTimingGrade,spread:p1.moveMeta.lastAimSpread,position:ball.position.toArray()};
       if(frame===45)for(const [code,value]of movement)key('keyup',code,value);
      }
     }finally{applyBlockDeflection=originalDeflection;deflectLayupBall=originalLayupDeflection;}
     results.push({made:lastScored,attempts:p1.stats.fga,press,preview,release,blocked,blockStats:p2.stats.blocks,blockContact,contact,maxJumpTravel,finite:ball.position.toArray().every(Number.isFinite)&&p1.group.position.toArray().every(Number.isFinite)});
    }
    Math.random=originalRandom;
    return {profile,condition,degrees,samples,made:results.filter(result=>result.made).length,blocked:results.filter(result=>result.blocked).length,blockStats:results.reduce((total,result)=>total+result.blockStats,0),results};
   };
  });
  const rows=[];
  const configurations=process.env.LAYUP_DEFENSE_CONFIGS?JSON.parse(process.env.LAYUP_DEFENSE_CONFIGS):['smooth','tower','rookie'].flatMap(profile=>[-90,0,90].flatMap(degrees=>['open','trailing','side','squared','timed','lateDive'].map(condition=>({profile,degrees,condition}))));
  for(const configuration of configurations){
   const row=await page.evaluate(config=>measureLayupDefense(config),{...configuration,samples});rows.push(row);
   const {profile,degrees,condition}=configuration;
   console.log(JSON.stringify({profile,degrees,condition,made:row.made,blocked:row.blocked,samples,contest:row.results[0].preview,example:row.results[0]}));
  }
  if(process.env.BASKETBALL_SCREENSHOTS){fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});fs.writeFileSync(path.join(process.env.BASKETBALL_SCREENSHOTS,'layup-defense-results.json'),JSON.stringify({samples,rows},null,2));}
  assert.deepEqual(errors,[]);
   for(const row of rows){
    assert.ok(row.results.every(result=>result.finite&&result.attempts===1&&result.press.speed>1.2&&result.press.type==='layup'),'one finite real running attempt: '+JSON.stringify(row));
    for(const result of row.results){
     if(result.blockStats>0)assert.ok(result.blockContact||result.contact?.type==='hand','a block needs a real recorded hand contact: '+JSON.stringify(result));
     if(result.blockContact){
      assert.ok(result.blockContact.verifiedDistance<=result.blockContact.verifiedRadius&&result.blockContact.verifiedRadius<=.335,'blocking uses visible palm and ball size: '+JSON.stringify(result));
      assert.ok(result.blockContact.stepLength<=.65,'a block cannot sweep a stale pickup or whole drive segment: '+JSON.stringify(result));
     }
     if(result.contact?.type==='hand')assert.ok(result.contact.distance<=.335,'gather hand contact touches the current ball rather than a pose jump: '+JSON.stringify(result));
     if(result.contact?.type==='body')assert.equal(result.blockStats,0,'a body deflection does not award an invisible hand block');
     if(row.condition==='lateDive')assert.ok(result.maxJumpTravel<=1.1,'a stationary sprint jump has a short committed lunge: '+JSON.stringify(result));
    }
   }
   const groups=new Map();
   for(const row of rows){const key=row.profile+':'+row.degrees;if(!groups.has(key))groups.set(key,[]);groups.get(key).push(row);}
   for(const group of groups.values()){
    const find=condition=>group.find(row=>row.condition===condition),open=find('open'),trailing=find('trailing'),side=find('side'),squared=find('squared'),timed=find('timed'),lateDive=find('lateDive');
    if(open)assert.ok(open.made>=samples*.8,'open running finishes are reliable with real ball flight: '+JSON.stringify(open));
    for(const row of [trailing,side].filter(Boolean)){
     assert.ok(row.results.every(result=>result.preview<.16),'a beaten or off-lane defender does not create front pressure: '+JSON.stringify(row));
     assert.ok(row.made>=samples*.7,'having a clear finishing step remains a real scoring advantage: '+JSON.stringify(row));
    }
    if(lateDive)assert.ok(lateDive.made>=samples*.6,'a beaten jump cannot erase a protected layup from behind: '+JSON.stringify(lateDive));
    if(squared&&trailing){
     assert.ok(squared.results.every(result=>result.preview>.3),'hands-up control guards the actual lane');
     assert.ok(squared.made<trailing.made,'squared-up hands-up coverage lowers physical makes: '+JSON.stringify({squared,trailing}));
    }
    if(timed&&open)assert.ok(timed.made<open.made,'a front timed jump remains useful defense: '+JSON.stringify({timed,open}));
   }
   const timed=rows.filter(row=>row.condition==='timed');
   if(timed.length>1){
    assert.ok(timed.reduce((total,row)=>total+row.blockStats,0)>=samples,'well-timed front jumps still make actual visible hand blocks');
    assert.ok(new Set(timed.filter(row=>row.blockStats>0).map(row=>row.degrees)).size>=2,'real hand blocks remain possible from multiple court approaches');
   }
  console.log('PASS: real keyboard layups keep a step past trailing/off-lane defenders; front hands-up and visible timed hand blocks remain valuable, without stale sweeps or long sprint-block recoveries.');
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});

