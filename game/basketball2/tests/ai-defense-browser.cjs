/* Identical public-input drives through simulateBasketball, including the real
 * modern acceleration, contact, stamina, AI, gather and shot/scoring solvers.
 * No player movement or collision function is replaced. Contact instrumentation
 * records overlaps immediately before the actual common collision solver. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {installLegacyGraphicsFixture}=require('./legacy-graphics-fixture.cjs');
const root=path.resolve(__dirname,'../../..');
const attempts=Number(process.env.BASKETBALL_DRIVE_ATTEMPTS||20);
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
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({
      path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),
      contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}
    }));
    await installLegacyGraphicsFixture(page);

    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    await page.evaluate(()=>{
      const solveContacts=resolveBodyCollisions;
      const shootFinish=releaseShot;
      globalThis.driveContact=null;
      releaseShot=function(player,type,options){
        if(driveContact&&player===p1&&type==='layup')driveContact.releaseContest=calculateShotContest(player).value;
        return shootFinish(player,type,options);
      };
      resolveBodyCollisions=function(dt){
        if(driveContact){
          let overlapping=false;
          const defense=getOpponents(p1);
          for(let i=0;i<defense.length;i++)for(let j=i+1;j<defense.length;j++){
            const a=defense[i],b=defense[j],gap=flatDistance(a.group.position,b.group.position);
            driveContact.minGap=Math.min(driveContact.minGap,gap);
            if(gap<a.bodyRadius+b.bodyRadius-.025)overlapping=true;
          }
          driveContact.frames+=overlapping?1:0;
          driveContact.streak=overlapping?driveContact.streak+1:0;
          driveContact.maxStreak=Math.max(driveContact.maxStreak,driveContact.streak);
        }
        return solveContacts(dt);
      };
      globalThis.driveSeries=(config,count)=>{
        selectedDifficulty=config.difficulty;selectedP1='sky';selectedP2='claw';
        teamSize=config.size;twoPlayerMode=false;isTournamentGame=false;startGame();
        gameSettings.set('controlStyle','modern');
        const profileSnapshot=matchPlayers.map(p=>({id:p.id,speed:p.profile.speed,jump:p.profile.jump,block:p.profile.block}));
        const results=[];
        for(let attempt=0;attempt<count;attempt++){
          let seed=100+attempt;Math.random=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};
          inputManager.clearAll();gameOver=false;gamePaused=false;countdownTimer=0;possessionFrozen=false;
          pendingInbound=null;scoreP1=scoreP2=0;takePossession(p1,{silent:true});
          aiTeamPlans=Object.create(null);aiActiveOffenseTeam=null;
          for(const plan of Object.values(aiDefensePlans)){plan.coverage=null;plan.observation=null;plan.trap=null;plan.handlerAge=0;}
          for(const player of matchPlayers){
            resetBody(player);player.stamina=1;player.sprint=1;player.aiScreen=false;
            player.contactTimer=0;player.contactCooldown=0;player.bodyUpTimer=0;player.plantTimer=0;
            player.jumpCooldown=0;player.aiBrain=createTeamBrain(player);player.aiBrain.thinkIn=0;
            Object.keys(player.stats).forEach(key=>player.stats[key]=0);
          }
          const side=config.route==='left'?-1:1,baseline=config.route==='baseline';
          p1.group.position.set(baseline?4.8:0,0,HOOP_POS.z+(baseline?6:8.6));
          getTeammates(p1).forEach((player,index)=>player.group.position.set(index?5.8:-5.8,0,HOOP_POS.z+(index?6.8:4.8)));
          getOpponents(p1).forEach((player,index)=>{
            player.group.position.set(index===0?(baseline?4.2:0):index===1?-2.7:4.6,0,
              HOOP_POS.z+(index===0?(baseline?4.3:6):index===1?3.4:4.5));
            player.angle=angleToward(player.group.position,p1.group.position);player.group.rotation.y=player.angle;
          });
          p1.angle=angleToward(p1.group.position,HOOP_POS);p1.group.rotation.y=p1.angle;
          scene.updateMatrixWorld(true);
          driveContact={frames:0,streak:0,maxStreak:0,minGap:100};
          let finish=null,turnover=false,frames=0,maxSpeed=0,minimumRim=100;
          for(let frame=0;frame<360&&!lastScored;frame++){
            frames++;
            const input=inputManager.players.p1;
            input.source='fixture';input.sprint=true;input.protect=false;input.moveX=1;input.moveY=0;
            let waypoint;
            if(baseline)waypoint=frame<48?new THREE.Vector3(4.8,0,HOOP_POS.z+.9):HOOP_POS.clone().setY(0);
            else if(config.route==='crossover')waypoint=frame<38?new THREE.Vector3(3.5,0,HOOP_POS.z+4.7):
              frame<76?new THREE.Vector3(-3,0,HOOP_POS.z+3.2):HOOP_POS.clone().setY(0);
            else waypoint=frame<55?new THREE.Vector3(side*3.4,0,HOOP_POS.z+4.8):HOOP_POS.clone().setY(0);
            input.worldDirection=waypoint.sub(p1.group.position).setY(0).normalize();
            if(finish){input.moveX=0;input.sprint=false;}
            const rim=flatDistance(p1.group.position,HOOP_POS);minimumRim=Math.min(minimumRim,rim);
            if(!finish&&rim<2.75&&getBallHandler()===p1&&ballState==='dribble'){
              const contest=calculateShotContest(p1);
              handleActionButton(p1);p1.moveMeta.chargeHeld=.4;shotPower=.63;releaseShootOrFake(p1);
              if(layupActive||dunkActive)finish={frame,contest:contest.value,label:contest.label,
                nearest:Math.min(...getOpponents(p1).map(p=>flatDistance(p.group.position,p1.group.position))),
                type:layupActive?'layup':'dunk'};
            }
            simulateBasketball(1/60);maxSpeed=Math.max(maxSpeed,p1.velocity.length());
            if(finish?.type==='dunk'&&dunkActive&&dunkTimer>=.58&&driveContact.releaseContest===undefined)
              driveContact.releaseContest=calculateShotContest(p1).value;
            if(!finish&&getBallHandler()!==p1){turnover=true;break;}
            if(finish&&frame>finish.frame+150)break;
          }
          const memory=aiDefensePlans[1]?.tendencies?.[p1.id];
          if(finish)finish.releaseContest=driveContact.releaseContest??null;
          const uncontested=!!finish&&(finish.contest<.12||finish.releaseContest!==null&&finish.releaseContest<.12);
          results.push({attempt,finish,uncontested,turnover,
            made:scoreP1>0,frames,minimumRim,maxSpeed,collisionFrames:driveContact.frames,
            longestCollision:driveContact.maxStreak,minDefenderGap:driveContact.minGap,
            collisionBreakdown:uncontested&&driveContact.maxStreak>=9,
            shade:aiDefensePlans[1]?aiDefenseShade(aiDefensePlans[1],p1,aiSettings()):0,
            tendencies:memory?{right:memory.right,left:memory.left,drives:memory.drives}:null,
            finite:matchPlayers.every(p=>p.group.position.toArray().every(Number.isFinite))&&ball.position.toArray().every(Number.isFinite)});
          driveContact=null;
        }
        return {config,profileSnapshot,fixture:{handler:config.route==='baseline'?[4.8,6]:[0,8.6],
          teammates:[[-5.8,4.8],[5.8,6.8]].slice(0,config.size-1),
          defenders:config.route==='baseline'?[[4.2,4.3],[-2.7,3.4],[4.6,4.5]].slice(0,config.size):
            [[0,6],[-2.7,3.4],[4.6,4.5]].slice(0,config.size),
          input:'normal human sprint, fixed frame-based waypoint directions; shoot upon reaching 2.75m'},results};
      };
    });
    for(const mode of ['switch','drop','human']){
      const result=await page.evaluate(mode=>{
        selectedDifficulty='impossible';selectedP1='sky';selectedP2='claw';teamSize=2;twoPlayerMode=mode==='human';
        localTeamMode='cooperative';teamOffensiveStyles[1]=mode==='drop'?'balanced':'motion';startGame();
        inputManager.clearAll();gameSettings.set('controlStyle','modern');gameOver=false;gamePaused=false;
        pendingInbound=null;countdownTimer=0;possessionFrozen=false;takePossession(p1,{silent:true});
        for(const player of matchPlayers){resetBody(player);player.stamina=player.sprint=1;}
        const screener=getTeammates(p1)[0],primary=getOpponents(p1)[0],partner=getOpponents(p1)[1];
        const place=(p,x,z)=>p.group.position.set(x,0,HOOP_POS.z+z);
        place(p1,0,6);place(screener,0,5.35);place(primary,0,4.7);place(partner,1.1,5.4);
        initBotBrain();matchPlayers.forEach(player=>player.aiBrain.lastHandler=p1.id);
        if(mode!=='human'){
          screener.aiBrain.role='screen';screener.aiBrain.roleUntil=teamAITime+3;
          screener.aiBrain.target.copy(screener.group.position);screener.aiBrain.thinkIn=10;screener.aiScreen=true;
        }
        const trace=[];
        for(let frame=0;frame<30;frame++){
          simulateBasketball(1/60);
          const c=aiDefensePlans[1]?.coverage;
          trace.push({frame,primary:c?.primaryId,roles:c?.roles,assignments:c?.assignments,
            gap:flatDistance(primary.group.position,partner.group.position),
            planted:mode==='human'?aiDefensePlantedScreen(screener,aiDefensePlans[1]):screener.aiScreen});
        }
        return {trace,partner:partner.id,primary:primary.id,screener:screener.id,
          partnerProfile:{speed:partner.profile.speed,width:partner.profile.width},humanSlot:screener.controllerSlot};
      },mode);
      assert.ok(result.trace.some(t=>t.planted),'the actual screener stays planted');
      if(mode!=='drop')assert.ok(result.trace.some(t=>t.primary===result.partner&&t.assignments[result.primary]===result.screener),
        'real mobile defenders exchange assignments around a physical planted screen: '+JSON.stringify(result));
      else assert.ok(result.trace.some(t=>t.primary===result.primary&&t.roles[result.partner]==='drop'),
        'real slow interior partner protects below a physical screen: '+JSON.stringify(result));
      assert.ok(result.trace.every(t=>Object.values(t.assignments||{}).length===2&&new Set(Object.values(t.assignments)).size===2));
      if(mode==='human')assert.equal(result.humanSlot,'p2','a physical human screen retains its controller owner');
      console.log('PASS: actual '+mode+' coverage at a planted screen with unique assignments');
    }
    const trapRead=await page.evaluate(()=>{
      selectedDifficulty='impossible';selectedP1='sky';selectedP2='claw';teamSize=3;twoPlayerMode=false;
      teamOffensiveStyles[1]='balanced';startGame();inputManager.clearAll();gameSettings.set('controlStyle','modern');
      gameOver=false;gamePaused=false;pendingInbound=null;countdownTimer=0;possessionFrozen=false;
      takePossession(p1,{silent:true});initBotBrain();
      const offense=[p1,...getTeammates(p1)],defense=getOpponents(p1);
      for(const player of matchPlayers){resetBody(player);player.stamina=player.sprint=1;player.aiBrain.lastHandler=p1.id;}
      const spots=[[7,6],[3,5.6],[-4,5],[6.4,4.7],[4,4.8],[-2,5]];
      [...offense,...defense].forEach((player,index)=>{
        player.group.position.set(spots[index][0],0,HOOP_POS.z+spots[index][1]);
        player.aiBrain.target.copy(player.group.position);
        if(player.teamId===0){player.aiBrain.role='outlet';player.aiBrain.thinkIn=10;}
      });
      p1.angle=Math.PI/2;
      const trace=[];
      for(let frame=0;frame<150;frame++){
        simulateBasketball(1/60);const c=aiDefensePlans[1]?.coverage,trap=aiDefensePlans[1]?.trap;
        trace.push({frame,trap:!!trap,doubles:frame===0?0:defense.filter(p=>p.aiBrain.role==='double').length,
          rotations:defense.filter(p=>p.aiBrain.role==='pass_lane').length,
          near:defense.filter(p=>flatDistance(p.group.position,p1.group.position)<3).length,
          gap:Math.min(...defense.flatMap((p,i)=>defense.slice(i+1).map(q=>flatDistance(p.group.position,q.group.position))))});
      }
      return trace;
    });
    assert.ok(trapRead.some(t=>t.trap&&t.doubles===1&&t.rotations===1&&t.near===2),
      'a real sideline stall creates a two-defender trap and a third-player rotation: '+JSON.stringify(trapRead));
    assert.ok(trapRead.every(t=>t.doubles<=1),'a deliberate double never sends a second helper');
    assert.ok(trapRead.at(-1).trap===false,'the actual trap expires and defenders recover');
    console.log('PASS: actual bounded sideline double with third-player rotation and recovery');
    const matrix=[];
    for(const size of [2,3])for(const difficulty of ['rookie','hard','impossible']){
      for(const route of difficulty==='impossible'?['right','left','baseline','crossover']:['right']){
        const series=await page.evaluate(({config,count})=>driveSeries(config,count),{config:{size,difficulty,route},count:attempts});
        matrix.push(series);
        const summary={size,difficulty,route,attempts,finishes:series.results.filter(r=>r.finish).length,
          uncontested:series.results.filter(r=>r.uncontested).length,made:series.results.filter(r=>r.made).length,
          turnovers:series.results.filter(r=>r.turnover).length,collisionBreakdowns:series.results.filter(r=>r.collisionBreakdown).length,
          collisionFrames:series.results.reduce((n,r)=>n+r.collisionFrames,0),
          longestCollision:Math.max(...series.results.map(r=>r.longestCollision)),
          finalShade:series.results.at(-1).shade,learnedDrives:series.results.at(-1).tendencies?.drives||0};
        console.log(JSON.stringify(summary));
        assert.ok(series.results.every(r=>r.finite),'finite actual simulation: '+JSON.stringify(series));
      }
    }
    for(const series of matrix.filter(s=>s.config.difficulty==='impossible')){
      const open=series.results.filter(r=>r.uncontested).length;
      if(series.config.route==='right')assert.ok(open<=Math.ceil(attempts*.1),
        'repeated standard right drive cannot create more than 2/20 open finishes: '+JSON.stringify(series));
      if(attempts>=10&&series.config.route==='right')assert.ok(series.results.at(-1).shade>.1,
        'the repeated actual right drive is learned during the series: '+JSON.stringify(series));
      assert.ok(series.results.filter(r=>r.collisionBreakdown).length<=Math.ceil(attempts*.1),
        'teammate collisions cannot repeatedly leave the basket open: '+JSON.stringify(series));
      const controls=matrix.filter(s=>s.config.size===series.config.size);
      assert.ok(controls.every(s=>JSON.stringify(s.profileSnapshot)===JSON.stringify(controls[0].profileSnapshot)),
        'difficulty keeps the same actual player speed, jump and block attributes');
    }
    assert.deepEqual(errors,[]);
    console.log('PASS: '+matrix.reduce((n,s)=>n+s.results.length,0)+' actual repeated-drive possessions across 2v2/3v3; real acceleration, contact, gathers and shots; unchanged physical attributes');
    if(process.env.BASKETBALL_DEFENSE_REPORT)fs.writeFileSync(process.env.BASKETBALL_DEFENSE_REPORT,JSON.stringify(matrix,null,2));
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
