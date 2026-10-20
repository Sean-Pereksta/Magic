/* Actual moving human cuts, directional catches, layup flight and scored
 * outcomes train the ordinary AI target and movement pipeline. */
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
    const page=await browser.newPage({viewport:{width:1200,height:800}}),errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.glb':'model/gltf-binary','.webp':'image/webp','.json':'application/json'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
    await installLegacyGraphicsFixture(page);

    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    const results=[];
    for(const difficulty of ['normal','impossible']){
      const result=await page.evaluate(difficulty=>{
        customRosterDraft.enabled=false;gameSettings.set('controlStyle','modern');gameSettings.set('gameplayPreset','simulation');
        selectedP1='smooth';selectedP2='smooth';selectedDifficulty=difficulty;teamSize=2;twoPlayerMode=true;localTeamMode='cooperative';
        isTournamentGame=false;tutorialMode=false;startGame();targetScore=100000;
        let seed=7349;Math.random=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};
        p2.profile.ratings={layups:88,jumping:65,dunking:35,contactFinishing:85,aerialControl:85,releaseStability:80,consistency:80};
        const physical=JSON.stringify(getOpponents(p1).map(p=>p.profile));
        const settings=aiSettings(),defender=getOpponents(p1)[1];
        function prepare(side=-1,preview=false){
          inputManager.clearAll();pendingInbound=null;countdownTimer=0;possessionFrozen=false;gameOver=false;gamePaused=false;
          for(const player of matchPlayers){resetBody(player);player.stamina=player.sprint=1;player.aiBrain=createTeamBrain(player);}
          p1.group.position.set(2,0,HOOP_POS.z+5);p2.group.position.set(side*3,0,HOOP_POS.z+5.5);
          p1.angle=angleToward(p1.group.position,HOOP_POS);p1.group.rotation.y=p1.angle;
          p2.angle=angleToward(p2.group.position,HOOP_POS);p2.group.rotation.y=p2.angle;
          getOpponents(p1).forEach((player,index)=>player.group.position.set(preview?(index?0:3):8,0,HOOP_POS.z+(preview?1.5:18+index*3)));
          for(const plan of Object.values(aiDefensePlans))plan.coverage=null;
          aiTeamPlans=Object.create(null);aiActiveOffenseTeam=null;takePossession(p1,{silent:true});updateCharacters(0);scene.updateMatrixWorld(true);
        }
        function preview(){
          prepare(-1,true);const position=defender.group.position.clone(),inputs=JSON.stringify([inputManager.players.p1,inputManager.players.p2]);
          decideAIDefense(defender,defender.aiBrain,p1,settings);
          return {target:defender.aiBrain.target.toArray(),travel:flatDistance(position,defender.aiBrain.target),
            positionUnchanged:position.distanceTo(defender.group.position)<1e-9,inputsUnchanged:inputs===JSON.stringify([inputManager.players.p1,inputManager.players.p2]),
            read:aiDefensePlans[1]?aiDefenseRouteRead(aiDefensePlans[1],p2,settings,'cut'):null};
        }
        function cutAndFinish(side){
          prepare(side);const madeBefore=p2.stats.fgm;let passed=false,gather=false,released=false,catchSeen=false;
          for(let frame=0;frame<300&&!lastScored;frame++){
            const input=inputManager.players.p2;input.source='fixture';input.sprint=false;input.protect=false;input.moveY=0;
            input.moveX=gather?0:1;input.worldDirection=HOOP_POS.clone().sub(p2.group.position).setY(0).normalize();
            if(!passed&&flatDistance(p2.group.position,HOOP_POS)<4.3){
              p1.angle=angleToward(p1.group.position,p2.group.position);p1.group.rotation.y=p1.angle;
              passed=tryDirectionalPass(p1,p2);
            }
            if(playerHasBall(p2)&&passed){
              catchSeen=true;
              if(!gather&&flatDistance(p2.group.position,HOOP_POS)<2.9){handleActionButton(p2);gather=shotCharging&&chargingPlayer===p2;}
            }
            if(gather&&!released&&(p2.moveMeta.chargeHeld||0)>=.1){releaseShootOrFake(p2);released=true;}
            simulateBasketball(1/60);
          }
          return {side,passed,catchSeen,gather,released,made:p2.stats.fgm>madeBefore,score:scoreP1,
            routes:JSON.parse(JSON.stringify(aiDefensePlans[1]?.tendencies?.[p2.id]?.scoringRoutes||{})),
            read:aiDefensePlans[1]?aiDefenseRouteRead(aiDefensePlans[1],p2,settings,'cut'):null};
        }
        const before=preview(),cuts=[];
        for(let i=0;i<3;i++)cuts.push(cutAndFinish(-1));
        const learned=preview(),counters=[];
        for(let i=0;i<3;i++)counters.push(cutAndFinish(1));
        const counter=preview(),physicalUnchanged=physical===JSON.stringify(getOpponents(p1).map(p=>p.profile));
        const moveStart=defender.group.position.clone();moveAIPlayer(defender,defender.aiBrain,1/60);
        const movement={distance:moveStart.distanceTo(defender.group.position),speed:defender.velocity.length()};
        startGame();
        const reset=Object.values(aiDefensePlans).every(plan=>!plan.tendencies||Object.values(plan.tendencies).every(memory=>!memory.scoringRoutes||Object.keys(memory.scoringRoutes).length===0));
        return {difficulty,before,cuts,learned,counters,counter,physicalUnchanged,movement,reset};
      },difficulty);
      console.log(JSON.stringify(result));results.push(result);
      for(const cut of [...result.cuts,...result.counters])assert.ok(cut.passed&&cut.catchSeen&&cut.gather&&cut.released&&cut.made,'actual pass/catch/gather/physical make: '+JSON.stringify(cut));
      assert.equal(result.cuts[0].read,null,'one scored cut does not instantly teach a lane');
      assert.equal(result.cuts[1].read,null,'two scored cuts remain below confidence threshold');
      assert.equal(result.learned.read?.side,-1);assert.ok(result.learned.travel<result.before.travel-.1,'learned rimward positioning shortens the actual recovery path');
      assert.equal(result.counter.read?.side,1,'successful opposite cuts replace the old lane');
      assert.ok(result.before.positionUnchanged&&result.learned.positionUnchanged&&result.counter.positionUnchanged);
      assert.ok(result.before.inputsUnchanged&&result.learned.inputsUnchanged&&result.counter.inputsUnchanged);
      assert.ok(result.movement.distance<.2&&result.movement.speed<8,'ordinary movement moves the defender without a teleport or speed override');
      assert.equal(result.physicalUnchanged,true);assert.equal(result.reset,true,'starting a new match clears learned scoring routes');
    }
    assert.deepEqual(errors,[]);
    console.log('PASS: normal and impossible defenses learn from three actual scored cuts, yield to successful counter-routes and reset with a new match; human controls and physical stats stay unchanged.');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
