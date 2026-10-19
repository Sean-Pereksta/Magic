/* Verify rim-height dunk reaches and scored landing on real rigs and fallback.
 * THREE_BUNDLE and BASKETBALL_CHROME can provide offline test dependencies. */
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
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.glb':'model/gltf-binary','.webp':'image/webp','.json':'application/json'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE) await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3).catch(error=>{console.error('Page errors:',errors);throw error;});

    const results=await page.evaluate(()=>{
      const results=[],savedRandom=Math.random,assets=new Map(basketballPlayerModels.assets);
      updateTeamAI=()=>{};
      for(const fallback of [false,true]) for(const profile of ['flash','smooth','tower']) for(const hang of [false,true]){
        if(fallback)basketballPlayerModels.assets.clear();
        Math.random=()=>hang?0:.99;
        teamSize=1;twoPlayerMode=false;selectedP1=profile;startGame();countdownTimer=0;possessionFrozen=false;
        p1.profile.ratings={dunking:82,jumping:90,strength:78,standingDunk:80,contactFinishing:82,aerialControl:85};
        p1.group.position.set(0,0,HOOP_POS.z+1.3);p1.velocity.set(0,0,-4.2);p1.angle=Math.PI;p1.group.rotation.set(0,Math.PI,0);p2.group.position.set(8,0,8);
        takePossession(p1,{silent:true});updateCharacters(0);startDunk(p1);
        const samples=[];let scoreSeen=false,pauseStable=false,landedBeforeInbound=false,hangFrames=0,lastRoot=p1.group.position.y,maxStep=0,peakKnee=-Infinity,peakHand=-Infinity,release=null;
        for(let frame=0;frame<150;frame++){
          simulateBasketball(1/60);
          const root=p1.group.position.y;
          if(pendingInbound||dunkActive){maxStep=Math.max(maxStep,Math.abs(root-lastRoot));lastRoot=root;}
          if(dunkActive){
            const phase=p1.moveMeta.dunk.posePhase;
            const knee=p1.playerModel?Math.max(getModelJointWorld(p1,'shin_L').y,getModelJointWorld(p1,'shin_R').y):Math.max(p1.leftLeg.getWorldPosition(new THREE.Vector3()).y,p1.rightLeg.getWorldPosition(new THREE.Vector3()).y);
            const hand=getWorldHand(p1,p1.moveMeta.dunk.side),ballGap=hand.distanceTo(ball.position);
            peakKnee=Math.max(peakKnee,knee);peakHand=Math.max(peakHand,hand.y);
            if(phase>.54&&phase<.76)samples.push({root,knee,hand:hand.y,ball:ball.position.y,ballGap});
          }
          if(rimHangActive)hangFrames++;
          if(dunkScored&&!scoreSeen){
            scoreSeen=true;release={root,knee:p1.playerModel?Math.max(getModelJointWorld(p1,'shin_L').y,getModelJointWorld(p1,'shin_R').y):p1.leftLeg.getWorldPosition(new THREE.Vector3()).y,hand:getWorldHand(p1,p1.moveMeta.dunk.side).y,ball:ball.position.y};
            setPaused(true);const snapshot=JSON.stringify({dunkTimer,timer:pendingInbound.remaining,root:p1.group.position.y,ball:ball.position.toArray(),hang:rimHangTimer});
            for(let i=0;i<12;i++)simulateBasketball(1/60);
            pauseStable=snapshot===JSON.stringify({dunkTimer,timer:pendingInbound.remaining,root:p1.group.position.y,ball:ball.position.toArray(),hang:rimHangTimer});setPaused(false);
          }
          if(scoreSeen&&!dunkActive&&pendingInbound)landedBeforeInbound=p1.onGround&&Math.abs(root)<.02;
          if(scoreSeen&&!pendingInbound)break;
        }
        results.push({profile,fallback,hang,rim:HOOP_POS.y,peakKnee,peakHand,maxStep,release,samples,score:scoreP1,attempts:p1.stats.fga,pauseStable,landedBeforeInbound,hangFrames,active:dunkActive,pending:!!pendingInbound});
      }
      Math.random=savedRandom;for(const [kind,asset] of assets)basketballPlayerModels.assets.set(kind,asset);
      return results;
    });
    for(const result of results){
      const label=JSON.stringify({...result,samples:undefined});
      assert.ok(result.peakKnee<result.rim-.45,'knees remain below the rim: '+label);
      assert.ok(result.samples.length>=8,'finish contains a sustained reaching pose: '+label);
      assert.ok(result.samples.every(sample=>sample.hand>result.rim-.18&&sample.hand<result.rim+.5&&sample.ballGap<.42),'extended hand stays with the ball just above the rim: '+label);
      assert.ok(result.release.ball>result.rim&&result.release.ball<result.rim+.25,'ball finishes immediately above the hoop: '+label);
      assert.ok(result.release.knee<result.rim-.45,'body does not rise to rim height: '+label);
      assert.ok(result.maxStep<.18,'gather, rise and landing remain smooth: '+label);
      assert.equal(result.score,2,label);assert.equal(result.attempts,1,label);assert.ok(result.pauseStable,'paused finish and inbound timers are unchanged: '+label);
      assert.ok(result.landedBeforeInbound,'scored dunk lands during inbound delay: '+label);assert.equal(result.active,false,label);assert.equal(result.pending,false,label);
      assert.equal(result.hangFrames>0,result.hang,label);
    }
    console.log('PASS: '+results.length+' real guard/wing/center and procedural dunks: knees below rim, hand/ball just above it, smooth landing, optional rim hang, one score, pause and inbound lifecycle');
    console.log(JSON.stringify(results.filter(result=>!result.fallback&&!result.hang).map(({profile,peakKnee,peakHand,release,maxStep})=>({profile,peakKnee,peakHand,release,maxStep}))));
    const signatures=await page.evaluate(()=>{
      const signatures=[];
      for(const variant of [
        {name:'power',profile:'tower',abilities:{powerFinisher:2},style:'twohand'},
        {name:'flight',profile:'flash',abilities:{highFlyer:2},style:'tomahawk'},
        {name:'reverse',profile:'flash',abilities:{highFlyer:2,aerialAcrobat:2},angle:Math.PI-.95,style:'reverse'},
        {name:'windmill',profile:'flash',abilities:{highFlyer:2,aerialAcrobat:2},style:'windmill'},
        {name:'contact',profile:'tower',abilities:{contactSpecialist:2},defender:true,style:'contact'},
        {name:'poster',profile:'flash',abilities:{posterizer:2,powerFinisher:2},defender:true,style:'poster'}
      ]){
        selectedP1=variant.profile;startGame();countdownTimer=0;possessionFrozen=false;
        p1.profile.ratings={dunking:90,jumping:94,strength:86,standingDunk:85,contactFinishing:90,aerialControl:92};p1.profile.abilities=variant.abilities;
        p1.group.position.set(0,0,HOOP_POS.z+1.3);p1.velocity.set(0,0,-4.2);p1.angle=variant.angle||Math.PI;p1.group.rotation.set(0,p1.angle,0);
        p2.group.position.set(variant.defender?1.7:8,0,variant.defender?HOOP_POS.z+.8:8);
        takePossession(p1,{silent:true});updateCharacters(0);startDunk(p1);
        const style=p1.moveMeta.dunk?.style;
        let finite=true;
        for(let frame=0;frame<90&&!dunkScored;frame++){
          simulateBasketball(1/60);
          const hand=getWorldHand(p1,p1.moveMeta.dunk.side);
          finite=finite&&Number.isFinite(hand.x)&&Number.isFinite(hand.y)&&Number.isFinite(hand.z);
        }
        signatures.push({name:variant.name,style,expected:variant.style,finite,score:scoreP1,dunks:p1.stats.dunks});
      }
      return signatures;
    });
    for(const signature of signatures){
      assert.equal(signature.style,signature.expected,JSON.stringify(signature));
      assert.equal(signature.finite,true,JSON.stringify(signature));
      assert.equal(signature.score,2,JSON.stringify(signature));
      assert.equal(signature.dunks,1,JSON.stringify(signature));
    }
    console.log('PASS: six signature finishes execute on real GLB rigs:',JSON.stringify(signatures));
    const blocked=await page.evaluate(()=>{
      teamSize=1;twoPlayerMode=false;selectedP1='flash';selectedP2='tower';startGame();countdownTimer=0;possessionFrozen=false;
      p1.profile.ratings={dunking:86,jumping:90,strength:78,standingDunk:80,contactFinishing:82,aerialControl:85};
      p1.profile.abilities={posterizer:3,powerFinisher:3};
      p1.group.position.set(0,0,HOOP_POS.z+1.3);p1.velocity.set(0,0,-4.2);p1.angle=Math.PI;p1.group.rotation.set(0,Math.PI,0);
      p2.group.position.set(0,0,HOOP_POS.z+.68);p2.velocity.set(0,0,0);p2.jumpCooldown=0;p2.angle=0;p2.group.rotation.set(0,0,0);
      const apply=applyBlockDeflection;let contactDistance=null;
      applyBlockDeflection=(defender,shooter,contact)=>{contactDistance=contact?.distance;return apply(defender,shooter,contact);};
      takePossession(p1,{silent:true});updateCharacters(0);startDunk(p1);let jumped=false;
      for(let frame=0;frame<70&&dunkActive;frame++){
        // Meet the low, exposed gather before the dunker has protected it above
        // the rim. A later dive no longer gets an enlarged invisible reach.
        if(!jumped&&dunkTimer>=.12){tryJumpBlock(p2,false);jumped=true;}
        simulateBasketball(1/60);
      }
      applyBlockDeflection=apply;
      return {blocks:p2.stats.blocks,score:scoreP1,attempts:p1.stats.fga,active:dunkActive,state:ballState,style:p1.moveMeta.dunk?.style,contactDistance};
    });
    const {contactDistance,...blockedOutcome}=blocked;
    assert.deepEqual(blockedOutcome,{blocks:1,score:0,attempts:1,active:false,state:'loose',style:'poster'},'a live timed defender can still physically block an elite Posterizer');
    assert.ok(contactDistance>=0&&contactDistance<=.335,'the visible hand actually touches the ball: '+contactDistance);
    console.log('PASS: real timed defender hand contact still blocks the dunk without awarding a basket; palm distance '+contactDistance.toFixed(3)+'m');
    const calibrationSamples=Math.max(300,Math.min(2000,Number(process.env.BASKETBALL_SHOT_SAMPLES)||300));
    const calibration=await page.evaluate(samples=>{
      selectedP1='smooth';startGame();countdownTimer=0;possessionFrozen=false;targetScore=100000;
      const savedRandom=Math.random;
      function measure({specialist=false,pressure=0,pct=null,speed=0,stamina=1}={}){
        let seed=42871,made=0;Math.random=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};
        for(let attempt=0;attempt<samples;attempt++){
          pendingInbound=null;possessionFrozen=false;resetBody(p1);resetBody(p2);
          p1.profile.ratings=specialist?{midrange:88,releaseStability:84,consistency:82}:{midrange:60,releaseStability:60,consistency:60};
          p1.profile.abilities=specialist?{midrangeAssassin:1}:{};p1.stamina=stamina;
          p1.group.position.set(0,0,HOOP_POS.z+5.8);p1.angle=Math.PI;p1.group.rotation.set(0,Math.PI,0);p1.velocity.set(0,0,-speed);
          p2.group.position.set(8,0,8);p2.angle=0;
          // This real tall rim protector needs more spacing than the average
          // test defender for its reach to grade as a moderate (.28-.48) contest.
          if(pressure){p2.group.position.copy(p1.group.position).add(new THREE.Vector3(0,0,pressure===1?-.72:-2.25));p2.handsUpTime=.5;}
          takePossession(p1,{silent:true});updateCharacters(0);
          if(pressure===.5){
            let near=.8,far=3.08;
            for(let step=0;step<12;step++){
              const spacing=(near+far)*.5;p2.group.position.z=p1.group.position.z-spacing;
              if(calculateShotContest(p1).value>.4)near=spacing;else far=spacing;
            }
            p2.group.position.z=p1.group.position.z-(near+far)*.5;
            updatePlayerModel(p2,0);scene.updateMatrixWorld(true);
          }
          const contest=calculateShotContest(p1),meter=getShotMeterProfile('setshot',p1,contest);
          if(pressure===.5&&(contest.value<.28||contest.value>.48))throw new Error('Expected moderate real-rig coverage, got '+contest.value);
          p1.moveMeta.chargeHeld=(pct??(meter.greenStart+meter.greenEnd)*.5)*1.35/meter.meterFillSpeed;
          shotPower=.65;shootBall(p1,'setshot');
          for(let frame=0;frame<150&&!lastScored;frame++){
            updateBall(1/60);loosePickupDelay=Math.max(0,loosePickupDelay-1/60);
            ballCollisionCooldown=Math.max(0,ballCollisionCooldown-1/60);backboardCollisionCooldown=Math.max(0,backboardCollisionCooldown-1/60);
          }
          if(lastScored)made++;
        }
        return made/samples;
      }
      const result={average:measure(),specialist:measure({specialist:true}),moderate:measure({pressure:.5}),heavy:measure({pressure:1}),
        poorTiming:measure({pct:.15}),moving:measure({speed:5}),fatigued:measure({stamina:.25})};
      Math.random=savedRandom;return result;
    },calibrationSamples);
    console.log('Real GLB release / full physical-flight calibration ('+calibrationSamples+' attempts/condition):',JSON.stringify(calibration));
    assert.ok(calibration.average>=.58&&calibration.average<=.7,JSON.stringify(calibration));
    assert.ok(calibration.specialist>=.68&&calibration.specialist<=.82,JSON.stringify(calibration));
    assert.ok(calibration.moderate<calibration.average-.08);
    assert.ok(calibration.heavy<calibration.moderate-.05);
    assert.ok(calibration.poorTiming<.12);
    assert.ok(calibration.moving<calibration.average-.1);
    assert.ok(calibration.fatigued<calibration.average-.06);
    if(process.env.BASKETBALL_SCREENSHOTS){
      await page.evaluate(()=>{
        teamSize=1;twoPlayerMode=false;selectedP1='flash';startGame();countdownTimer=0;possessionFrozen=false;
        p1.profile.ratings={dunking:86,jumping:90,strength:78,standingDunk:80,contactFinishing:82,aerialControl:85};
        p1.group.position.set(0,0,HOOP_POS.z+1.3);p1.velocity.set(0,0,-4.2);p1.angle=Math.PI;p1.group.rotation.set(0,Math.PI,0);p2.group.position.set(8,0,8);takePossession(p1,{silent:true});updateCharacters(0);startDunk(p1);
        for(let frame=0;frame<30;frame++)simulateBasketball(1/60);
        camMain.position.set(4.7,3.6,-5.3);camMain.lookAt(0,2.1,HOOP_POS.z+.1);renderScene();
      });
      await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'basketball-dunk-finish.png')});
    }
    assert.deepEqual(errors,[]);
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
