/* Size mismatches and lane attacks through the real AI and scoring loop.
 * The optional browser environment variables match browser-smoke.cjs.
 * Fixtures freeze other players through normal controller ownership, not AI,
 * shot, collision or physics stubs. All renders use the actual skinned GLBs. */
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
    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    await page.evaluate(()=>{
      globalThis.aiFixture=config=>{
        selectedDifficulty=config.difficulty||'impossible';selectedP1=config.defenderProfile||'flash';selectedP2=config.profile||'smooth';
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
        const trace=[];let gather=null,pass=null,firstShot=null,blocked=false;
        const start=p2.group.position.clone();
        for(let frame=0;frame<frames&&!lastScored;frame++){
          const before=p2.stats.fga;
          simulateBasketball(1/60);
          blocked=blocked||layupBlocked;
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
        return {made:scoreP2>0,points:scoreP2,blocked,attempts:p2.stats.fga,threeAttempts:p2.stats.tpa,gather,firstShot,pass,
          travel:flatDistance(start,p2.group.position),trace,grade:p2.moveMeta.lastTimingGrade,
          owner:getBallHandler()?.id,handlerTurnovers:p2.stats.turnovers,
          finite:matchPlayers.every(char=>char.group.position.toArray().every(Number.isFinite))&&ball.position.toArray().every(Number.isFinite)};
      };
    });
    const mismatch=[];
    for(const seed of [1,3,7,11,19,31,42,77,101,221,503,1019]){
      await page.evaluate(seed=>aiFixture({profile:'tower',defenderProfile:'flash',handler:[0,1.9],defense:[[0,.5]],seed}),seed);
      assert.ok(await page.evaluate(()=>matchPlayers.every(char=>char.playerModel?.skeletons.length)),
        'mismatch fixture uses actual loaded skinned GLBs');
      const result=await page.evaluate(()=>runAIFixture(45));
      mismatch.push({seed,...result});
    }
    const quickAttempts=mismatch.filter(result=>result.firstShot?.frame<36&&result.firstShot.type==='layup');
    assert.ok(quickAttempts.length>=10,'big finisher should often attack the lone small defender right at the rim: '+JSON.stringify(mismatch));
    console.log('PASS: '+quickAttempts.length+'/'+mismatch.length+' big-on-small matchups choose a real immediate rim finish');

    await page.evaluate(()=>aiFixture({profile:'tower',defenderProfile:'flash',handler:[0,1.9],defense:[[0,.5]],seed:42}));
    const contactMake=await page.evaluate(()=>runAIFixture(160));
    assert.equal(contactMake.firstShot?.type,'layup','size mismatch starts a real layup');
    assert.ok(contactMake.made,'high release can physically finish over the passive smaller body: '+JSON.stringify(contactMake));
    assert.ok(contactMake.finite,'contact finish keeps real ball and actor state finite');
    console.log('PASS: mismatch layup can physically score over a smaller player with actual GLB hand and ball collision');

    const caution=[];
    for(const config of [
      {defenderProfile:'tower',defense:[[0,.5]]},
      {teamSize:3,defenderProfile:'flash',defense:[[0,.5],[.7,1.1],[-.7,1.1]]},
      {defenderProfile:'flash',defense:[[0,.5]],activeBlock:true}
    ]){
      await page.evaluate(config=>{
        aiFixture({profile:'tower',handler:[0,1.9],seed:42,...config});
        if(config.activeBlock){
          p2.controllerSlot='fixture';tryJumpBlock(p1,false);
          for(let frame=0;frame<8;frame++)simulateBasketball(1/60);
          p2.controllerSlot=null;p2.aiBrain.thinkIn=0;
        }
      },config);
      const result=await page.evaluate(()=>runAIFixture(8));
      caution.push({...config,...result});
      assert.equal(result.attempts,0,'do not blindly use size into equal rim protection, multiple helpers or an active timed jump: '+JSON.stringify(caution.at(-1)));
      assert.notEqual(result.trace[0].decision,'finish','read the actual rim contest first: '+JSON.stringify(caution.at(-1)));
    }
    console.log('PASS: equal rim protector, multiple helpers and active timed block remove the immediate size-over finish');

    const driveCases=[
      {profile:'flash',handler:[0,6.4]},
      {profile:'tower',handler:[0,7.8]},
      {profile:'smooth',handler:[0,6.4],velocity:[0,-2.8]},
      {profile:'deeprange',handler:[0,7.8],velocity:[0,-2.8]}
    ];
    for(const config of driveCases){
      await page.evaluate(config=>aiFixture({...config,seed:42}),config);
      const result=await page.evaluate(()=>runAIFixture(420));
      assert.equal(result.trace[0].decision,'drive','obvious empty lane should produce a decisive drive: '+JSON.stringify({config,...result}));
      const approach=result.trace.filter(point=>point.frame<(result.gather?.frame??result.firstShot?.frame??80)&&point.frame<=72);
      assert.ok(approach.every(point=>point.decision==='drive'||point.decision==='finish'),
        'clear-lane approach should persist rather than wavering into setup or premature jumpers: '+JSON.stringify({config,approach}));
      assert.equal(result.firstShot?.type,'layup','clear lane should reach an actual running layup: '+JSON.stringify({config,...result}));
      assert.ok(result.firstShot.travel>2,'bot actually covers the lane');
      assert.ok(result.gather?.speed>1,'running layup retains real movement into the gather');
      assert.ok(result.made,'uncontested real running finish should physically score: '+JSON.stringify({config,...result}));
      assert.ok(result.finite,'drive and finish remain finite');
    }
    console.log('PASS: four builds decisively drive open 6-8m lanes, sustain the approach, and physically make running layups');

    const cleanThrees=await page.evaluate(()=>Array.from({length:24},(_,index)=>{
      aiFixture({profile:'deeprange',handler:[0,7.8],seed:42+index*109});return runAIFixture(220);
    }));
    assert.ok(cleanThrees.every(result=>result.firstShot?.type==='setshot'&&result.threeAttempts===1),'settled elite shooter recognizes a valuable open three');
    const madeThrees=cleanThrees.filter(result=>result.points===3).length;
    assert.ok(madeThrees>=6&&madeThrees<24,'physical threes remain valuable and nonautomatic: '+madeThrees+'/24');
    console.log('PASS: settled elite shooter recognizes clean threes and physically scores '+madeThrees+'/24 seeded attempts');

    await page.evaluate(()=>{
      // The small defender's actual head intersects the exposed gather side;
      // standing merely near the protected ball no longer earns a deflection.
      aiFixture({profile:'tower',defenderProfile:'flash',handler:[0,1.9],defense:[[-.3,.7]],seed:42});
      beginAIShot(p2,p2.aiBrain);
    });
    const contactBlock=await page.evaluate(()=>{
      const deflect=deflectLayupBall;let contact=null;
      deflectLayupBall=(owner,defender,hit)=>{
        contact={type:hit.type,distance:ball.position.distanceTo(hit.point),radius:hit.radius};
        return deflect(owner,defender,hit);
      };
      const result=runAIFixture(18);deflectLayupBall=deflect;
      return {...result,contact};
    });
    assert.ok(contactBlock.blocked,'size must not bypass a real ball-head/body collision: '+JSON.stringify(contactBlock));
    assert.equal(contactBlock.contact?.type,'body');
    assert.ok(contactBlock.contact.distance<=contactBlock.contact.radius,'actual ball contact, not a nearby defender: '+JSON.stringify(contactBlock.contact));
    assert.equal(contactBlock.points,0,'collision does not force a make');
    console.log('PASS: real gather contact still deflects a big player; size advantage never bypasses basketball physics');
    assert.deepEqual(errors,[]);
    console.log('PASS: no browser exceptions in mismatch, lane or contest fixtures');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
