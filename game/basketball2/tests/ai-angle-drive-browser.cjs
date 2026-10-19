/* Side and trailing pursuit through the real AI and scoring loop.
 * Browser environment variables match browser-smoke.cjs. Optional
 * BASKETBALL_AI_READS_BUNDLE serves a saved read layer for before/after checks.
 * The human pursuer receives one normal public-input movement update per tick.
 * Other players are frozen through controller ownership; gameplay, collision,
 * shot and scoring physics are unmodified. Actual skinned GLBs are loaded. */
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
      if(file.endsWith('ai-reads.js')&&process.env.BASKETBALL_AI_READS_BUNDLE)return route.fulfill({path:process.env.BASKETBALL_AI_READS_BUNDLE,contentType:'text/javascript'});
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
        if(config.pursuit){
          const lateral=p1.bodyRadius+p2.bodyRadius+.22;
          p1.group.position.set(config.side*lateral,0,HOOP_POS.z+config.handler[1]+.15);
          p1.velocity.set(0,0,-3.2);
        }
        if(config.control){
          const body=p1.bodyRadius+p2.bodyRadius;
          if(config.control==='front')p1.group.position.set(0,0,HOOP_POS.z+config.handler[1]-body-.25);
          if(config.control==='rimHelp'){
            p1.group.position.set(0,0,HOOP_POS.z+1.2);
            const pursuer=defense[1];
            pursuer.group.position.set(config.side*(pursuer.bodyRadius+p2.bodyRadius+.22),0,HOOP_POS.z+config.handler[1]+.15);
          }
          if(config.control==='rotation'){
            p1.group.position.set(-config.side*(body+.3),0,HOOP_POS.z+config.handler[1]+.15);
            p1.velocity.set(config.side*4,0,-3.2);
            const pursuer=defense[1];
            pursuer.group.position.set(config.side*(pursuer.bodyRadius+p2.bodyRadius+.22),0,HOOP_POS.z+config.handler[1]+.15);
          }
        }
        scene.updateMatrixWorld(true);
        globalThis.pursuitFixture=config.pursuit;
        globalThis.pursuitControl=config.control;
        globalThis.pursuitSide=config.side;
        return {handler:p2.id,teammates:offense.map(char=>char.id),opponents:defense.map(char=>char.id)};
      };
      globalThis.runAIFixture=frames=>{
        const trace=[];let gather=null,pass=null,firstShot=null,blocked=false;
        const start=p2.group.position.clone(),pursuerStart=p1.group.position.clone();
        for(let frame=0;frame<frames&&!lastScored;frame++){
          const before=p2.stats.fga;
          if(pursuitFixture){
            const input=inputManager.players.p1;
            input.source='fixture';input.moveY=1;input.moveX=0;input.sprint=false;
            input.worldDirection=new THREE.Vector3(0,0,-1);
          }
          if(pursuitControl==='rotation'){
            const input=inputManager.players.p1;
            input.source='fixture';input.moveY=1;input.moveX=0;input.sprint=false;
            input.worldDirection=new THREE.Vector3(pursuitSide,0,-.8).normalize();
          }
          simulateBasketball(1/60);
          blocked=blocked||layupBlocked;
          trace.push({frame,role:p2.aiBrain.role,decision:p2.aiBrain.decision,
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
          travel:flatDistance(start,p2.group.position),pursuerTravel:flatDistance(pursuerStart,p1.group.position),trace,grade:p2.moveMeta.lastTimingGrade,
          owner:getBallHandler()?.id,handlerTurnovers:p2.stats.turnovers,
          finite:matchPlayers.every(char=>char.group.position.toArray().every(Number.isFinite))&&ball.position.toArray().every(Number.isFinite)};
      };
    });
    const describe=(config,initial,result)=>JSON.stringify({...result,config,initial,
      trace:result.trace.filter(point=>point.frame<=(result.gather?.frame??60)&&point.frame%3===0)});
    const pursuitResults=[];
    for(const profile of ['flash','smooth','tower']){
      for(const side of [-1,1]){
        for(const distance of [3.05,6.4]){
          const config={profile,defenderProfile:profile,handler:[0,distance],velocity:[0,-3.2],pursuit:true,side,seed:42};
          const initial=await page.evaluate(config=>{
            aiFixture(config);
            return {body:p2.bodyRadius+p1.bodyRadius,separation:flatDistance(p1.group.position,p2.group.position),
              models:matchPlayers.every(char=>char.playerModel?.skeletons.length)};
          },config);
          const result=await page.evaluate(()=>runAIFixture(240));
          pursuitResults.push({config,initial,...result});
          const detail=()=>describe(config,initial,result);
          assert.ok(initial.models,'pursuit fixture uses actual loaded skinned GLBs');
          assert.ok(initial.separation>initial.body+.15,'the shoulder is physically clear at departure: '+detail());
          assert.equal(result.trace[0].decision,distance<4?'finish':'drive',
            'runner commits immediately to the won shoulder angle: '+detail());
          const approach=result.trace.filter(point=>point.frame<result.gather?.frame);
          assert.ok(approach.length&&approach.every(point=>point.decision==='drive'||point.decision==='finish'),
            'parallel pursuit must not make a committed runner reset, pass or abandon the rim: '+detail());
          assert.equal(result.pass,null,'clean shoulder pursuit does not trigger an unnecessary pass: '+detail());
          assert.equal(result.firstShot?.type,'layup','the real drive reaches a running layup: '+detail());
          assert.ok(result.gather?.speed>1,'actual momentum survives the running gather: '+detail());
          assert.ok(result.pursuerTravel>.2,'the human pursuer physically moves through the normal control loop: '+detail());
          if(distance<4){
            assert.ok(result.gather.frame<=12,'a clear shoulder near the rim does not cause an extra hesitation or detour: '+detail());
          }else{
            assert.ok(result.firstShot.travel>2.5,'the long approach actually covers the lane: '+detail());
          }
          assert.ok(result.finite,'pursuit keeps actual actor and ball state finite: '+detail());
        }
      }
    }
    const made=pursuitResults.filter(result=>result.made).length;
    assert.ok(made>=6,'won shoulder drives create reliable physical finishing opportunities: '+made+'/12');
    console.log('PASS: 12 real parallel-pursuit drives across both shoulders and flash/smooth/tower builds commit without reset/pass, retain running gathers; '+made+'/12 physically score');

    const controls=[];
    for(const profile of ['flash','smooth','tower']){
      for(const side of [-1,1]){
        for(const control of ['front','rimHelp','rotation']){
          const config={profile,defenderProfile:profile,teamSize:control==='front'?1:2,
            offense:control==='front'?[]:[[-7,8]],handler:[0,3.05],velocity:[0,-3.2],control,side,seed:42};
          const initial=await page.evaluate(config=>{
            aiFixture(config);return {finishOpen:aiAssessShot(p2,aiSettings()).finishOpen};
          },config);
          const result=await page.evaluate(()=>runAIFixture(12));
          controls.push({config,initial,...result});
          const detail=()=>describe(config,initial,result);
          assert.equal(initial.finishOpen,false,'front body, rim help or visible rotation still owns the lane: '+detail());
          assert.notEqual(result.trace[0].decision,'finish','a genuine defender threat removes the immediate running finish: '+detail());
          assert.equal(result.gather,null,'do not blindly gather into real body/rim/rotating help: '+detail());
          assert.equal(result.attempts,0,'read the threat and improve the possession before forcing a shot: '+detail());
          assert.ok(result.finite,'the control fixture keeps real movement and ball state finite: '+detail());
        }
      }
    }
    console.log('PASS: 18 front-body, rim-help and moving-recovery controls preserve caution for both shoulders and all three builds');
    assert.deepEqual(errors,[]);
    console.log('PASS: no browser exceptions or nonfinite pursuit states');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
