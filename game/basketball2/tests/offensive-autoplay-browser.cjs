/* Full active 2v2/3v3 games through the real movement, passing, defense,
 * collision, gather, shot and inbound loop. No gameplay function is stubbed. */
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
    const page=await browser.newPage({viewport:{width:1280,height:800}}),errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory()) file=path.join(file,'index.html');
      if(!fs.existsSync(file)) return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js')) return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.webp':'image/webp','.glb':'model/gltf-binary','.json':'application/json'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE) await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({
      path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),
      contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}
    }));
    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    const results=[];
    const difficulty=process.env.BASKETBALL_AUTOPLAY_DIFFICULTY||'impossible';
    for(const size of [2,3]) for(const lineup of ['inside','perimeter']){
      if(process.env.AUTOPLAY_ONLY&&process.env.AUTOPLAY_ONLY!==`${size}-${lineup}`) continue;
      const result=await page.evaluate(({size,lineup,difficulty})=>{
        if(!Object.hasOwn(difficultySettings,difficulty)) throw new Error('Unknown autoplay difficulty: '+difficulty);
        selectedDifficulty=difficulty;selectedP1=lineup==='inside'?'tower':'deeprange';selectedP2='flash';
        teamSize=size;twoPlayerMode=false;isTournamentGame=false;startGame();
        inputManager.clearAll();gameOver=false;gamePaused=false;pendingInbound=null;
        countdownTimer=0;possessionFrozen=false;scoreP1=0;scoreP2=0;targetScore=100;
        teamOffensiveStyles[0]='balanced';teamOffensiveStyles[1]='balanced';
        gameSettings.set('controlStyle','modern');
        const identities={
          playmaker:{archetype:'playmaker',passing:1.45,handle:1.3,shot:1,layup:1,width:1,height:1,speed:1,sprint:1},
          sharpshooter:{archetype:'sharpshooter',passing:1,handle:1,shot:1.5,layup:1,width:1,height:1,speed:1,sprint:1},
          slasher:{archetype:'slasher',passing:1,handle:1.15,shot:.85,layup:1.4,width:.95,height:1,speed:1.1,sprint:1.25},
          interior:{archetype:'interior',passing:.9,handle:.85,shot:.75,layup:1.35,width:1.3,height:1.2,speed:.9,sprint:.9},
          defender:{archetype:'defender',passing:1,handle:1,shot:.9,layup:1,width:1.1,height:1.05,speed:1,sprint:1,block:1.5,steal:1.4}
        };
        const offense=lineup==='inside'?['playmaker','interior','slasher']:['sharpshooter','sharpshooter','playmaker'];
        for(const teamId of [0,1]){
          const team=matchPlayers.filter(char=>char.teamId===teamId);
          team.forEach((char,slot)=>{
            resetBody(char);char.controllerSlot=null;
            char.profile={...char.profile,...identities[(teamId===0?offense:['defender','slasher','playmaker'])[slot]]};
            char.group.position.set(slot===0?0:slot===1?-5:5,0,HOOP_POS.z+(teamId===0?(slot===0?8:5.5):(slot===0?5.6:4.3)));
            char.angle=angleToward(char.group.position,HOOP_POS);char.group.rotation.y=char.angle;
            char.stamina=1;char.sprint=1;Object.keys(char.stats).forEach(key=>char.stats[key]=0);
          });
        }
        let seed=1234+size;Math.random=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};
        takePossession(p1,{silent:true});initBotBrain();
        scene.updateMatrixWorld(true);
        const attempts=()=>matchPlayers.reduce((sum,char)=>sum+char.stats.fga,0);
        const totals={size,lineup,difficulty,attempts:0,teamAttempts:[0,0],turnovers:0,teamPossessions:[0,0],passes:0,completedPasses:0,plantedScreens:0,cuts:0,score:[0,0],
          actions:{},roles:{},shotTypes:{},passReasons:{},maxNoShotSeconds:0,maxTeamPossessionSeconds:0,maxLooseSeconds:0,maxConsecutivePasses:0,finite:true,ticks:0};
        let lastAttempts=0,noShot=0,loose=0,consecutive=0,lastPass=null,lastScreen=new Set(),lastCuts=new Set(),previousState=ballState;
        const lastFGA=new Map(matchPlayers.map(char=>[char.id,0]));
        const previousPlans=new Map();
        const trace=[];
        const start=performance.now();
        for(let frame=0;frame<3600;frame++){
          simulateBasketball(1/60);totals.ticks++;
          const activePlan=aiTeamPlans[getBallHandler()?.teamId];
          if(activePlan) totals.maxTeamPossessionSeconds=Math.max(totals.maxTeamPossessionSeconds,activePlan.age);
          if(activePlan&&previousPlans.get(activePlan.teamId)!==activePlan){
            totals.teamPossessions[activePlan.teamId]++;previousPlans.set(activePlan.teamId,activePlan);
          }
          loose=ballState==='loose'?loose+1/60:0;totals.maxLooseSeconds=Math.max(totals.maxLooseSeconds,loose);
          if(!possessionFrozen&&countdownTimer<=0){
            noShot+=1/60;totals.maxNoShotSeconds=Math.max(totals.maxNoShotSeconds,noShot);
          }
          const currentAttempts=attempts();
          if(currentAttempts>lastAttempts){
            for(const char of matchPlayers){
              const count=char.stats.fga-lastFGA.get(char.id);
              const type=layupActive&&layupOwner===char.id?'layup':dunkActive&&dunkOwner===char.id?'dunk':char.moveMeta?.lastShotType||'finish';
              if(count>0) totals.shotTypes[type]=(totals.shotTypes[type]||0)+count;
              lastFGA.set(char.id,char.stats.fga);
            }
            noShot=0;consecutive=0;lastAttempts=currentAttempts;
          }
          if(ballState==='pass'&&previousState!=='pass'&&passFlight.active){
            totals.passes++;consecutive++;totals.maxConsecutivePasses=Math.max(totals.maxConsecutivePasses,consecutive);
            lastPass={target:passFlight.targetId};
            const passer=getPlayerById(passFlight.passerId),history=aiTeamPlans[passer?.teamId]?.passHistory;
            const reason=history?.[history.length-1]?.reason||'unrecorded';
            totals.passReasons[reason]=(totals.passReasons[reason]||0)+1;
          }
          if(previousState==='pass'&&ballState==='dribble'&&lastPass){
            if(getBallHandler()?.id===lastPass.target) totals.completedPasses++;
            lastPass=null;
          }
          const screens=new Set(matchPlayers.filter(char=>char.aiScreen).map(char=>char.id));
          for(const id of screens) if(!lastScreen.has(id)) totals.plantedScreens++;
          lastScreen=screens;
          const cuts=new Set(matchPlayers.filter(char=>['cut','roll','break'].includes(char.aiBrain?.role)).map(char=>char.id));
          for(const id of cuts) if(!lastCuts.has(id)) totals.cuts++;
          lastCuts=cuts;
          if(frame%30===0){
            const owner=getBallHandler(),plan=owner&&aiTeamPlans[owner.teamId];
            if(owner?.teamId===0&&plan) totals.actions[aiPlanAction(plan)]=(totals.actions[aiPlanAction(plan)]||0)+1;
            for(const char of matchPlayers) totals.roles[char.aiBrain?.role]=(totals.roles[char.aiBrain?.role]||0)+1;
            if(!matchPlayers.every(char=>char.group.position.toArray().every(Number.isFinite))||!ball.position.toArray().every(Number.isFinite)){totals.finite=false;break;}
          }
          if(frame%300===0) trace.push({seconds:frame/60,attempts:currentAttempts,passes:totals.passes,state:ballState,
            owner:getBallHandler()?.id,phase:aiTeamPlans[getBallHandler()?.teamId]?.phase,
            age:aiTeamPlans[getBallHandler()?.teamId]?.age,handlerAge:getBallHandler()?.aiBrain?.possessionAge,
            read:getBallHandler()?(()=>{const r=aiAssessShot(getBallHandler(),aiSettings());return {space:r.space,quality:r.quality,settledQuality:r.settledQuality,pressure:r.pressure,speed:getBallHandler().velocity.length(),value:r.value,distance:r.distance,inRange:r.inRange};})():null,
            players:matchPlayers.map(char=>({id:char.id,role:char.aiBrain.role,decision:char.aiBrain.decision,x:+char.group.position.x.toFixed(1),z:+(char.group.position.z-HOOP_POS.z).toFixed(1)}))});
          previousState=ballState;
        }
        totals.elapsedMs=+(performance.now()-start).toFixed(1);
        totals.msPerTick=+(totals.elapsedMs/totals.ticks).toFixed(3);
        totals.attempts=attempts();totals.score=[scoreP1,scoreP2];
        totals.teamAttempts=[0,1].map(team=>matchPlayers.filter(char=>char.teamId===team).reduce((sum,char)=>sum+char.stats.fga,0));
        totals.turnovers=matchPlayers.reduce((sum,char)=>sum+char.stats.turnovers,0);
        totals.maxNoShotSeconds=+totals.maxNoShotSeconds.toFixed(2);
        totals.maxTeamPossessionSeconds=+totals.maxTeamPossessionSeconds.toFixed(2);
        totals.maxLooseSeconds=+totals.maxLooseSeconds.toFixed(2);
        return {...totals,trace};
      },{size,lineup,difficulty});
      results.push(result);
      console.log('AUTOPLAY '+JSON.stringify({...result,trace:undefined}));
      if(process.env.BASKETBALL_AUTOPLAY_TRACE) console.log('TRACE '+JSON.stringify(result.trace));
    }
    for(const result of results){
      assert.ok(result.finite,'all positions and ball states stay finite');
      assert.ok(result.attempts>=4,'every full active minute produces at least four actual shot attempts: '+JSON.stringify(result));
      assert.ok(result.maxTeamPossessionSeconds<18,'offense develops a decision without an 18-second same-team hold: '+JSON.stringify(result));
      assert.ok(result.maxNoShotSeconds<18,'active play keeps producing attempts rather than repeated empty resets: '+JSON.stringify(result));
      assert.ok(result.maxLooseSeconds<10,'loose balls do not create a new kind of no-shot stall: '+JSON.stringify(result));
      assert.ok(result.maxConsecutivePasses<10,'passes must develop into a shot: '+JSON.stringify(result));
      assert.equal(result.passReasons.unrecorded,undefined,'every actual AI pass retains its offensive purpose');
      assert.ok(result.cuts>0,'small-team playbook produces actual cuts/rolls in motion');
    }
    assert.ok(results.every(result=>result.passes>0),'all team sizes and lineups produce purposeful real passes');
    assert.ok(results.reduce((sum,result)=>sum+result.completedPasses,0)>2,'real passes reach moving teammates');
    assert.ok(results.filter(result=>result.lineup==='inside').some(result=>result.plantedScreens>0),'inside combinations plant actual screens');
    for(const size of [2,3]){
      const inside=results.find(result=>result.size===size&&result.lineup==='inside');
      const perimeter=results.find(result=>result.size===size&&result.lineup==='perimeter');
      if(!inside||!perimeter) continue;
      assert.notDeepEqual(inside.actions,perimeter.actions,'lineup chemistry changes the shared offense');
    }
    assert.deepEqual(errors,[]);
    console.log('PASS: '+results.length+' full active archetype matchups pass, screen, cut and create shots without long stalls; tick timings above are informational');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
