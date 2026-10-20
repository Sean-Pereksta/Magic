/* Bot decisions through the real movement, passing, gather and scoring loop.
 * The optional browser environment variables match browser-smoke.cjs.
 * Fixtures freeze other players through normal controller ownership, not AI,
 * shot, collision or physics stubs. All renders use the actual skinned GLBs. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {installLegacyGraphicsFixture}=require('./legacy-graphics-fixture.cjs');
const root=path.resolve(__dirname,'../../..');
(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage({viewport:{width:1280,height:800}});
    const errors=[];page.on('pageerror',error=>errors.push(error.stack));
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.webp':'image/webp','.glb':'model/gltf-binary','.json':'application/json'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({
      path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),
      contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}
    }));
    await installLegacyGraphicsFixture(page);

    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    await page.evaluate(()=>{
      globalThis.aiFixture=config=>{
        selectedDifficulty=config.difficulty||'impossible';selectedP2=config.profile||'smooth';
        teamSize=config.teamSize||1;twoPlayerMode=false;isTournamentGame=false;startGame();
        inputManager.clearAll();gameOver=false;gamePaused=false;pendingInbound=null;
        countdownTimer=0;possessionFrozen=false;scoreP1=0;scoreP2=0;
        gameSettings.set('controlStyle','modern');
        matchPlayers.forEach((char,index)=>{
          resetBody(char);char.controllerSlot=char===p2?null:'fixture';
          char.group.position.set((index%2?1:-1)*8,0,9+index*.8);
          char.stamina=1;char.sprint=1;Object.keys(char.stats).forEach(key=>char.stats[key]=0);
        });
        p1.controllerSlot='p1';
        const offense=getTeammates(p2),defense=getOpponents(p2);
        const place=(char,position)=>{
          char.group.position.set(position[0],0,HOOP_POS.z+position[1]);
          char.angle=angleToward(char.group.position,HOOP_POS);char.group.rotation.y=char.angle;
        };
        place(p2,config.handler);
        (config.offense||[]).forEach((position,index)=>place(offense[index],position));
        (config.defense||[]).forEach((position,index)=>place(defense[index],position));
        teamOffensiveStyles[p2.teamId]=config.style||'balanced';
        takePossession(p2,{silent:true});initBotBrain();
        if(config.velocity)p2.velocity.set(config.velocity[0],0,config.velocity[1]);
        p2.aiBrain.thinkIn=0;p2.aiBrain.actionCooldown=0;p2.aiBrain.moveCooldown=10;
        p2.aiBrain.possessionAge=config.age??1;
        p2.aiBrain.lastHandler=p2.id;
        let seed=config.seed||42;
        Math.random=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};
        scene.updateMatrixWorld(true);
        return {handler:p2.id,teammates:offense.map(char=>char.id),opponents:defense.map(char=>char.id)};
      };
      globalThis.runAIFixture=frames=>{
        const trace=[];let gather=null,pass=null,firstShot=null;
        const start=p2.group.position.clone();
        for(let frame=0;frame<frames&&!lastScored;frame++){
          const before=p2.stats.fga;
          simulateBasketball(1/60);
          if(frame%6===0)trace.push({frame,role:p2.aiBrain.role,decision:p2.aiBrain.decision,
            x:p2.group.position.x,z:p2.group.position.z-HOOP_POS.z,speed:p2.velocity.length(),
            target:[p2.aiBrain.target.x,p2.aiBrain.target.z-HOOP_POS.z],state:ballState});
          if(!pass&&ballState==='pass')pass={frame,passer:passFlight.passerId,target:passFlight.targetId,
            directionalTarget:getPassTarget(p2)?.id};
          if(!gather&&(layupActive&&layupOwner===p2.id))gather={frame,speed:p2.velocity.length(),
            distance:flatDistance(p2.group.position,HOOP_POS),position:p2.group.position.toArray()};
          if(!firstShot&&p2.stats.fga>before)firstShot={frame,type:layupActive?'layup':dunkActive?'dunk':p2.moveMeta.lastShotType,
            distance:flatDistance(p2.group.position,HOOP_POS),speed:p2.velocity.length(),travel:flatDistance(start,p2.group.position)};
        }
        return {made:scoreP2>0,points:scoreP2,attempts:p2.stats.fga,threeAttempts:p2.stats.tpa,gather,firstShot,pass,
          travel:flatDistance(start,p2.group.position),trace,grade:p2.moveMeta.lastTimingGrade,
          owner:getBallHandler()?.id,handlerTurnovers:p2.stats.turnovers,
          finite:matchPlayers.every(char=>char.group.position.toArray().every(Number.isFinite))&&ball.position.toArray().every(Number.isFinite)};
      };
    });
    const attempts=[];
    const finishCases=[
      ...['flash','smooth','tower'].map(profile=>({profile,handler:[0,1.6]})),
      ...['flash','tower'].flatMap(profile=>[[-1.6,4.8],[1.6,5.8]].map(handler=>({profile,handler}))),
      {profile:'smooth',handler:[1.6,4.8],velocity:[-1.1,-3.4]}
    ];
    for(const config of finishCases){
        await page.evaluate(config=>aiFixture(config),{...config,seed:42});
        const result=await page.evaluate(()=>runAIFixture(420));
        attempts.push({...config,...result});
        assert.ok(result.finite,JSON.stringify(attempts.at(-1)));
        assert.ok(['layup','dunk'].includes(result.firstShot?.type),'open bot should seek the rim with a legal finish: '+JSON.stringify(attempts.at(-1)));
        assert.ok(result.made,'open bot layup should physically score: '+JSON.stringify(attempts.at(-1)));
        if(config.handler[1]>4){
          assert.ok(result.firstShot.travel>1.5,'bot attacks clear lane before shooting: '+JSON.stringify(attempts.at(-1)));
          assert.ok((result.gather?.speed||result.firstShot?.speed)>1,'running finish retains actual movement: '+JSON.stringify(attempts.at(-1)));
        }
    }
    console.log('PASS: '+attempts.length+' actual bot rim finishes across all three GLB builds, with running clear-lane drives and physical scores');

    // Open, green shots intentionally retain launch variance. Validate a seeded
    // sample rather than requiring an arbitrary single attempt to always score.
    for(const distance of [6,7.8]){
      const samples=await page.evaluate(distance=>Array.from({length:24},(_,index)=>{
        aiFixture({profile:'smooth',handler:[0,distance],seed:42+index*109});
        return runAIFixture(200);
      }),distance);
      assert.ok(samples.every(result=>result.firstShot?.type==='setshot'&&result.finite),'settled specialist recognizes open jumper');
      const makes=samples.filter(result=>result.made).length;
      assert.ok(makes>=6&&makes<24,'open shooting creates real, nonautomatic makes: '+makes+'/24');
      if(distance>7.15){
        assert.ok(samples.every(result=>result.threeAttempts===1),'open threes count as three-point attempts');
        assert.ok(samples.filter(result=>result.made).every(result=>result.points===3),'made threes physically score three points');
      }
      console.log('PASS: open settled specialist '+(distance>7.15?'threes':'midrange')+' physically scores '+makes+'/24 seeded attempts');
    }

    for(const config of [
      {profile:'smooth',handler:[0,7.8],defense:[[0,6.5]]},
      {profile:'tower',handler:[0,9.8]}
    ]){
      await page.evaluate(config=>aiFixture({...config,seed:42}),config);
      const read=await page.evaluate(()=>runAIFixture(24));
      assert.equal(read.attempts,0,'covered three or poor-shooter long shot should not be forced: '+JSON.stringify(read));
      assert.ok(['drive','pass','setup'].includes(read.trace[0].decision),'recognize the bad jumper and improve the possession: '+JSON.stringify(read));
    }
    console.log('PASS: covered three and out-of-range poor shooter improve the possession instead of forcing bad jumpers');

    const fixture=await page.evaluate(()=>aiFixture({teamSize:3,handler:[0,6.2],
      offense:[[-2.5,1.4],[5,6.2]],defense:[[0,4.2],[1.7,1.8],[4,7.8]],seed:42}));
    const rimFeed=await page.evaluate(()=>runAIFixture(90));
    assert.equal(rimFeed.pass?.target,fixture.teammates[0],'blocked drive should feed the safe open rim teammate: '+JSON.stringify(rimFeed));
    assert.equal(rimFeed.owner,fixture.teammates[0],'the real pass should reach the open rim teammate');
    assert.equal(rimFeed.handlerTurnovers,0,'safe rim feed should not be intercepted');
    assert.equal(rimFeed.attempts,0,'handler should not take a covered jumper while a rim feed is open');
    console.log('PASS: blocked paint produces a real pass to the open rim finisher instead of a covered shot');

    const aligned=await page.evaluate(()=>{
      const fixture=aiFixture({teamSize:3,handler:[0,7],offense:[[0,1.5],[1.1,4.5]],
        defense:[[3,7],[2.3,4.5],[-5,3]],seed:42});
      const closeTeammate=getPlayerById(fixture.teammates[1]);
      p2.angle=angleToward(p2.group.position,closeTeammate.group.position);p2.group.rotation.y=p2.angle;
      return fixture;
    });
    const intendedPass=await page.evaluate(()=>runAIFixture(90));
    assert.equal(intendedPass.pass?.directionalTarget,aligned.teammates[1],
      'fixture exposes the ambiguous facing-based near teammate: '+JSON.stringify(intendedPass));
    assert.equal(intendedPass.pass?.target,aligned.teammates[0],'AI must preserve its intended safe rim receiver');
    assert.equal(intendedPass.owner,aligned.teammates[0],'the intended receiver catches the actual pass');
    assert.equal(intendedPass.handlerTurnovers,0,'the intended safe pass reaches its recipient');
    console.log('PASS: nearly aligned teammates do not redirect an intended safe rim feed to the closer covered teammate');

    await page.evaluate(()=>aiFixture({teamSize:3,handler:[0,6],offense:[[-1.5,3.4],[1.5,3.4]],
      defense:[[0,4.5],[-1.5,4.5],[1.5,4.5]],seed:42}));
    const occupied=await page.evaluate(()=>{
      const result=runAIFixture(24),first=result.trace[0];
      const start=new THREE.Vector3(0,0,HOOP_POS.z+6);
      const target=new THREE.Vector3(first.target[0],0,HOOP_POS.z+first.target[1]);
      const clearance=Math.min(...getOpponents(p2).map(defender=>aiReadSegment(defender.group.position,start,target).distance));
      return {...result,clearance};
    });
    assert.equal(occupied.attempts,0,'do not force a bad contested jumper into a packed paint');
    assert.ok(occupied.pass||occupied.clearance>.95,'with every lane blocked, pass or move to a safe setup angle: '+JSON.stringify(occupied));
    console.log('PASS: fully occupied paint produces a safe setup rather than forcing a drive or contested jumper');

    const defenseReads=[];
    for(const difficulty of ['rookie','impossible']){
      const result=await page.evaluate(difficulty=>{
        aiFixture({difficulty,handler:[1,3.6],defense:[[0,5.4]],seed:42});
        takePossession(p1,{silent:true});p1.velocity.set(0,0,-3.6);
        initBotBrain();p2.aiBrain.thinkIn=0;updateTeamAI(1/60);
        return {difficulty,role:p2.aiBrain.role,target:p2.aiBrain.target.toArray(),reaction:p2.aiBrain.thinkIn,
          speed:p2.profile.speed,jump:p2.profile.jump};
      },difficulty);
      defenseReads.push(result);
    }
    assert.equal(defenseReads[0].role,'guard','rookie keeps the basic current-position guard');
    assert.equal(defenseReads[1].role,'contain','top difficulty recognizes and contains a moving rim attack');
    assert.ok(defenseReads[1].reaction<defenseReads[0].reaction*.5,'top difficulty reads and adjusts sooner');
    assert.equal(defenseReads[0].speed,defenseReads[1].speed,'strategy does not change player speed');
    assert.equal(defenseReads[0].jump,defenseReads[1].jump,'strategy does not change player jumping');
    console.log('PASS: impossible predicts the driving gap and reacts sooner than rookie, using identical player attributes');

    const finishContest=await page.evaluate(()=>{
      aiFixture({teamSize:3,handler:[0,2.5],velocity:[0,-2.5],defense:[[8,10],[.9,1.2],[-8,10]],seed:42});
      p2.controllerSlot='fixture';
      const defender=getOpponents(p2)[1];defender.controllerSlot=null;
      initBotBrain();defender.aiBrain.thinkIn=0;defender.aiBrain.actionCooldown=0;
      handleActionButton(p2);p2.moveMeta.chargeHeld=.1;releaseShootOrFake(p2);
      const states=[];
      for(let frame=0;frame<18;frame++){
        simulateBasketball(1/60);
        states.push({role:defender.aiBrain.role,block:defender.blockTime,hands:defender.handsUpTime,
          jumping:!defender.onGround,gather:layupActive,blocked:layupBlocked});
      }
      return states;
    });
    assert.ok(finishContest.some(state=>state.gather&&(state.block>0||state.hands>0)),
      'active hard defender should contest the actual layup gather: '+JSON.stringify(finishContest));
    console.log('PASS: active top-difficulty defender contests a real running layup gather rather than treating it as a rebound');

    assert.deepEqual(errors,[]);
    console.log('PASS: no browser exceptions or nonfinite movement/ball states');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
