/* Live teammate passes and role decisions through the normal control, meter,
 * animation and ball/hoop collision loop. No shot success or AI action stubs. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'../../..');
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
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.webp':'image/webp','.glb':'model/gltf-binary','.json':'application/json'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    await page.evaluate(()=>{
      globalThis.roleFixture=(kind='open',seed=42)=>{
        selectedDifficulty='impossible';selectedP1='crafty';selectedP2='bruiser';
        showQuickPlaySetup(3,false);startGame();inputManager.clearAll();
        gameOver=false;gamePaused=false;pendingInbound=null;countdownTimer=0;possessionFrozen=false;scoreP1=0;scoreP2=0;
        gameSettings.set('controlStyle','modern');gameSettings.set('gameplayPreset','simulation');
        matchPlayers.forEach((p,i)=>{resetBody(p);p.controllerSlot='fixture';p.group.position.set(i%2?8:-8,0,5+i);p.stamina=1;p.sprint=1;Object.keys(p.stats).forEach(k=>p.stats[k]=0);});
        const place=(p,x,z)=>{p.group.position.set(x,0,HOOP_POS.z+z);p.angle=angleToward(p.group.position,HOOP_POS);p.group.rotation.y=p.angle;};
        const friends=getTeammates(p1),guards=getOpponents(p1),receiver=friends[0];
        globalThis.roleReceiver=receiver;globalThis.roleGuard=guards[0];
        const ratings=Object.fromEntries(RiseToGlory.ATTRIBUTE_KEYS.map(key=>[key,75]));
        receiver.profile={...receiver.profile,archetype:'all_rounder',role:'shooter',ratings,abilities:{catchAndShoot:2,quickRelease:2,deepRange:1},shot:1.25};
        receiver.controllerSlot=null;p1.controllerSlot='p1';
        place(p1,-4,6.4);place(receiver,3,7.1);place(friends[1],-8,12);
        place(guards[0],-7,1);place(guards[1],7,1);place(guards[2],0,11.5);
        if(kind==='guarded')place(guards[0],3,5.9);
        teamOffensiveStyles[0]='pick_roll';teamOffensiveStyles[1]='balanced';
        takePossession(p1,{silent:true});initBotBrain();
        p1.angle=angleToward(p1.group.position,receiver.group.position);p1.group.rotation.y=p1.angle;
        let state=seed;Math.random=()=>{state=(Math.imul(state,1664525)+1013904223)>>>0;return state/4294967296;};
        scene.updateMatrixWorld(true);const passed=tryDirectionalPass(p1,receiver);
        return {passed,receiver:receiver.id,passer:p1.id,models:matchPlayers.every(p=>!!p.playerModel)};
      };
      globalThis.runRoleCatch=frames=>{
        let catchFrame=null,chargeFrame=null,releaseFrame=null,catchIntent=null,shot=null,catchPosition=null;
        const receiver=roleReceiver,trace=[];
        for(let frame=0;frame<frames&&!lastScored;frame++){
          simulateBasketball(1/60);
          if(catchFrame===null&&Number.isFinite(receiver.lastCatchTime)&&receiver.lastCatchTime>0){catchFrame=frame;catchPosition=receiver.group.position.clone();catchIntent=receiver.aiBrain.decision;}
          if(chargeFrame===null&&shotCharging&&chargingPlayer===receiver)chargeFrame=frame;
          if(releaseFrame===null&&receiver.stats.fga){releaseFrame=frame;shot={type:receiver.moveMeta.lastShotType,distance:flatDistance(receiver.group.position,HOOP_POS),travel:catchPosition?flatDistance(receiver.group.position,catchPosition):null,active:[...(receiver.moveMeta.activeAbilities||[])]};}
          if(frame%6===0)trace.push({frame,decision:receiver.aiBrain.decision,state:ballState,role:receiver.aiBrain.role,speed:receiver.velocity.length()});
        }
        return {catchFrame,chargeFrame,releaseFrame,catchIntent,shot,trace,attempts:receiver.stats.fga,threes:receiver.stats.tpa,points:scoreP1,
          owner:getBallHandler()?.id,passer:p1.controllerSlot,finite:matchPlayers.every(p=>p.group.position.toArray().every(Number.isFinite))&&ball.position.toArray().every(Number.isFinite)};
      };
    });
    const clean=[];
    for(const seed of [1,17,42,73,101,221,503,1019,2003,8011,12007,15013]){
      const fixture=await page.evaluate(seed=>roleFixture('open',seed),seed);
      assert.ok(fixture.passed&&fixture.models,'real human pass and skinned players: '+JSON.stringify(fixture));
      const result=await page.evaluate(()=>runRoleCatch(210));clean.push(result);
      assert.ok(result.catchFrame!==null,'pass reaches the actual teammate: '+JSON.stringify(result));
      assert.ok(result.chargeFrame!==null&&result.chargeFrame-result.catchFrame<=24,'open shooter begins catch shot within 0.4s: '+JSON.stringify(result));
      assert.equal(result.threes,1,'actual three-point shot attempt: '+JSON.stringify(result));
      assert.ok(['catchAndShoot','quickRelease','deepRange'].every(id=>result.shot.active.includes(id)),'learned skills affect the normal live shot: '+JSON.stringify(result));
      assert.ok(result.shot.distance>THREE_RADIUS_FROM_RULES&&result.shot.travel<.7,'shooter holds the arc instead of dribbling inside: '+JSON.stringify(result));
      assert.equal(result.passer,'p1','pass does not take control from the human');assert.ok(result.finite);
    }
    const makes=clean.filter(r=>r.points===3).length;
    assert.ok(makes>0&&makes<clean.length,'actual physics produces both makes and misses: '+makes+'/'+clean.length);
    console.log('PASS: 12 real passes become quick catch-and-shoot threes; '+makes+'/12 physically score, with human control retained.');
    await page.evaluate(()=>roleFixture('guarded',42));
    const guarded=await page.evaluate(()=>runRoleCatch(40));
    assert.ok(guarded.catchFrame!==null,'faceguard fixture still completes the pass: '+JSON.stringify(guarded));
    assert.ok(guarded.chargeFrame===null||guarded.chargeFrame-guarded.catchFrame>24,'a defender in the face cancels the quick shot: '+JSON.stringify(guarded));
    console.log('PASS: faceguarded receiver reads the contest instead of launching the quick catch three.');
    const post=await page.evaluate(()=>{
      roleFixture('open',42);passFlight.active=false;
      const big=p2,guard=p1;big.controllerSlot=null;roleReceiver.controllerSlot='fixture';
      big.profile={...big.profile,archetype:'interior',role:'anchor',ratings:{...roleReceiver.profile.ratings,strength:90,threePoint:30,midrange:45,closeShot:75},abilities:{powerFinisher:2,contactSpecialist:1}};
      const place=(p,x,z)=>{p.group.position.set(x,0,HOOP_POS.z+z);p.angle=angleToward(p.group.position,HOOP_POS);p.group.rotation.y=p.angle;};
      matchPlayers.filter(p=>p!==big&&p!==guard).forEach((p,i)=>place(p,i%2?8:-8,12+i));
      place(big,1.7,3.5);place(guard,1.5,2.2);takePossession(big,{silent:true});initBotBrain();
      const start=big.group.position.clone(),trace=[];big.aiBrain.moveCooldown=0;
      for(let frame=0;frame<60;frame++){simulateBasketball(1/60);if(frame%3===0)trace.push({frame,decision:big.aiBrain.decision,state:big.moveState,protect:big.aiBrain.protect,read:big.aiBrain.postRead});}
      return {trace,travel:flatDistance(start,big.group.position),finite:big.group.position.toArray().every(Number.isFinite),shots:big.stats.fga};
    });
    assert.ok(post.trace.some(p=>p.decision==='post'&&p.protect),'bruiser establishes a protected interior play: '+JSON.stringify(post));
    assert.ok(post.trace.some(p=>p.state==='postup'&&p.read==='backdown'),'common post-up action executes: '+JSON.stringify(post));
    assert.ok(post.travel>.1&&post.finite,'post-up uses real movement');
    console.log('PASS: interior anchor establishes a protected post and executes the normal backdown movement.');
    assert.deepEqual(errors,[],'no runtime exceptions');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
const THREE_RADIUS_FROM_RULES=7.15;
