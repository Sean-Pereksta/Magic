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
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);

    const results=await page.evaluate(()=>{
      const results=[],savedRandom=Math.random,assets=new Map(basketballPlayerModels.assets);
      updateTeamAI=()=>{};
      for(const fallback of [false,true]) for(const profile of ['flash','smooth','tower']) for(const hang of [false,true]){
        if(fallback)basketballPlayerModels.assets.clear();
        Math.random=()=>hang?0:.99;
        teamSize=1;twoPlayerMode=false;selectedP1=profile;startGame();countdownTimer=0;possessionFrozen=false;
        p1.group.position.set(0,0,HOOP_POS.z+1.8);p1.angle=Math.PI;p1.group.rotation.set(0,Math.PI,0);p2.group.position.set(8,0,8);
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
    const blocked=await page.evaluate(()=>{
      teamSize=1;twoPlayerMode=false;selectedP1='flash';selectedP2='tower';startGame();countdownTimer=0;possessionFrozen=false;
      p1.group.position.set(0,0,HOOP_POS.z+1.8);p1.angle=Math.PI;p1.group.rotation.set(0,Math.PI,0);
      p2.group.position.set(0,0,HOOP_POS.z+.68);p2.angle=0;p2.group.rotation.set(0,0,0);
      takePossession(p1,{silent:true});updateCharacters(0);startDunk(p1);let jumped=false;
      for(let frame=0;frame<70&&dunkActive;frame++){
        if(!jumped&&dunkTimer>=.2){tryJumpBlock(p2,false);jumped=true;}
        simulateBasketball(1/60);
      }
      return {blocks:p2.stats.blocks,score:scoreP1,attempts:p1.stats.fga,active:dunkActive,state:ballState};
    });
    assert.deepEqual(blocked,{blocks:1,score:0,attempts:1,active:false,state:'loose'},'a live timed defender can still physically block the finish');
    console.log('PASS: real timed defender hand contact still blocks the dunk without awarding a basket');
    if(process.env.BASKETBALL_SCREENSHOTS){
      await page.evaluate(()=>{
        teamSize=1;twoPlayerMode=false;selectedP1='flash';startGame();countdownTimer=0;possessionFrozen=false;
        p1.group.position.set(0,0,HOOP_POS.z+1.8);p1.angle=Math.PI;p1.group.rotation.set(0,Math.PI,0);p2.group.position.set(8,0,8);takePossession(p1,{silent:true});updateCharacters(0);startDunk(p1);
        for(let frame=0;frame<30;frame++)simulateBasketball(1/60);
        camMain.position.set(4.7,3.6,-5.3);camMain.lookAt(0,2.1,HOOP_POS.z+.1);renderScene();
      });
      await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'basketball-dunk-finish.png')});
    }
    assert.deepEqual(errors,[]);
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
