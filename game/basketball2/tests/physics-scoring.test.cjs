const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

class Vector3 {
  constructor(x=0,y=0,z=0){ this.set(x,y,z); }
  set(x,y,z){ this.x=x; this.y=y; this.z=z; return this; }
  setY(y){ this.y=y; return this; }
  copy(v){ return this.set(v.x,v.y,v.z); }
  clone(){ return new Vector3(this.x,this.y,this.z); }
  add(v){ this.x+=v.x; this.y+=v.y; this.z+=v.z; return this; }
  sub(v){ this.x-=v.x; this.y-=v.y; this.z-=v.z; return this; }
  multiplyScalar(s){ this.x*=s; this.y*=s; this.z*=s; return this; }
  addScaledVector(v,s){ this.x+=v.x*s; this.y+=v.y*s; this.z+=v.z*s; return this; }
  lengthSq(){ return this.x*this.x+this.y*this.y+this.z*this.z; }
  length(){ return Math.sqrt(this.lengthSq()); }
  normalize(){ return this.multiplyScalar(1/(this.length()||1)); }
  dot(v){ return this.x*v.x+this.y*v.y+this.z*v.z; }
  distanceToSquared(v){ return this.clone().sub(v).lengthSq(); }
  distanceTo(v){ return Math.sqrt(this.distanceToSquared(v)); }
  lerp(v,t){ return this.addScaledVector(v.clone().sub(this),t); }
  lerpVectors(a,b,t){ return this.copy(a).lerp(b,t); }
}

function setup(){
  const ctx=vm.createContext({
    THREE:{Vector3,MathUtils:{clamp:(v,a,b)=>Math.max(a,Math.min(b,v)),lerp:(a,b,t)=>a+(b-a)*t,degToRad:d=>d*Math.PI/180}},
    console,performance:{now:()=>5000}
  });
  for(const file of ["state","rendering","team-play","animation-contact","ball-physics","scoring","presentation"]){
    vm.runInContext(fs.readFileSync(path.join(__dirname,`../js/${file}.js`),"utf8"),ctx);
  }
  vm.runInContext(`
    function flatDistance(a,b){return Math.hypot(a.x-b.x,a.z-b.z)}
    function getForward(a){return new THREE.Vector3(Math.sin(a),0,Math.cos(a))}
    function angleToward(a,b){return Math.atan2(b.x-a.x,b.z-a.z)}
    function smoothstep(t){return t*t*(3-2*t)}
    function getScreenSideVectorForChar(p,side){return new THREE.Vector3(side==='left'?-1:1,0,0)}
    function keepInCourt(){} function updateMomentum(){} function updateScoreboard(){}
    function flash(){} function triggerShotSplash(){} function triggerCrowdReaction(){}
    function showShotBreakdown(){} function playShotSound(){} function addTakeover(){}
    function playBlockSound(){} function showShotFeedback(){}
    function playSwishSound(){} function playCrowdPulse(){} function pulseArenaLights(){}
    function getWorldHand(p,side){return p.testHand ? p.testHand.clone() : p.group.position.clone().add(new THREE.Vector3(side==='left'?-.35:.35,1.4,0))}
    function attachBallToHand(p,side){ball.position.copy(getWorldHand(p,side))}
    function create(id,team,x,z){return {
      id,teamId:team,controllerSlot:null,hasBall:false,angle:Math.PI,
      group:{position:new THREE.Vector3(x,0,z),rotation:new THREE.Vector3(),scale:new THREE.Vector3(1,1,1)},
      profile:{name:id,width:1,height:1,shot:1,layup:1,handle:1,jump:1,steal:1,block:1},
      stamina:1,sprint:1,velocity:new THREE.Vector3(),anim:{},moveMeta:{},movement:{contactStrength:1,recoverySpeed:1},
      stats:{fgm:0,fga:0,tpm:0,tpa:0,perfect:0,steals:0,blocks:0,turnovers:0},
      bodyRadius:.55,blockTime:0,handsUpTime:0,swipeTime:0,reachRecovery:0,stumbledTimer:0,
      onGround:true,leftArm:{rotation:new THREE.Vector3()},rightArm:{rotation:new THREE.Vector3()},
      leftForearm:{getWorldPosition(v){return v.copy(getWorldHand(this.owner,'left'))}},
      rightForearm:{getWorldPosition(v){return v.copy(getWorldHand(this.owner,'right'))}}
    }}
    matchPlayers=[create('p1',0,0,-5),create('p2',0,4,-5),create('a1',0,-4,-5),create('b1',1,0,-9),create('b2',1,4,-9),create('b3',1,-4,-9)];
    for(const p of matchPlayers){p.leftForearm.owner=p;p.rightForearm.owner=p;}
    p1=matchPlayers[0];p2=matchPlayers[1];p1.controllerSlot='p1';p2.controllerSlot='p2';
    twoPlayerMode=true;teamSize=3;
    inputManager={vibrate(){}};
    function getGameplayTuning(){return {stealWindow:1,contestStrength:1,takeoverRate:1}}
    ball={position:new THREE.Vector3(),rotation:new THREE.Vector3(),scale:new THREE.Vector3(1,1,1)};
    ballShadow=null;
    gameStarted=true;
  `,ctx);
  return ctx;
}

const run=(ctx,source)=>vm.runInContext(source,ctx);

test('feet determine the three-point and development zone while the hand still launches the physical ball',()=>{
  for(const [ground,hand,expected] of [[7.6,6.9,3],[6.9,7.6,2]]){
    const ctx=setup();
    vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/player-development.js'),'utf8'),ctx);
    const result=run(ctx,`
      for(const other of matchPlayers)if(other.teamId===1)other.group.position.set(8,0,8);
      p1.group.position.set(0,0,HOOP_POS.z+${ground});
      p1.profile.ratings={threePoint:92,midrange:30,releaseStability:75,consistency:75};
      p1.profile.abilities={deepRange:2,catchAndShoot:1};
      simulationTime=10;takePossession(p1,{silent:true});p1.lastCatchTime=9.9;p1.lastPassQuality=1;
      p1.moveMeta.chargeHeld=.8;
      const handPoint=new THREE.Vector3(.1,2.4,HOOP_POS.z+${hand});
      releaseShot(p1,'setshot',{releasePoint:handPoint,random:()=>.5});
      ({value:lastShotValue,threes:p1.stats.tpa,attempts:p1.stats.fga,active:p1.moveMeta.activeAbilities,
        releaseError:ball.position.distanceTo(handPoint),sweepError:previousBallPosition.distanceTo(handPoint),releaseRecordError:lastShotReleasePos.distanceTo(handPoint),
        flight:[ballVelocity.x,ballVelocity.y,ballVelocity.z]});
    `);
    assert.equal(result.value,expected);assert.equal(result.threes,expected===3?1:0);assert.equal(result.attempts,1);
    assert.equal(result.active.includes('deepRange'),expected===3,'rating/perk zone follows the same legal foot location');
    assert.equal(result.active.includes('catchAndShoot'),true);
    assert.equal(result.releaseError,0);assert.equal(result.releaseRecordError,0);assert.ok(result.flight.every(Number.isFinite));
    assert.equal(result.sweepError,0,'the first shot sweep starts at the actual release, never a previous pickup');
  }
});

test('settled-shot contest previews ignore a prior fadeaway and match the actual release',()=>{
  const ctx=setup();
  const result=run(ctx,`
    p1.group.position.set(0,0,HOOP_POS.z+5.8);p1.angle=Math.PI;p1.moveState='normal';
    for(const other of matchPlayers)if(other.teamId===1)other.group.position.set(8,0,8);
    const defender=matchPlayers[3];defender.group.position.copy(p1.group.position).add(new THREE.Vector3(0,0,-1.7));
    defender.handsUpTime=.5;defender.angle=0;
    takePossession(p1,{silent:true});shotCharging=true;chargingPlayer=p1;
    p1.moveMeta.lastShotType='fadeaway';p1.moveMeta.fadeawayArmed=false;
    const preview=calculateShotContest(p1).value;
    const selected=chooseShotType(p1);
    p1.moveMeta.chargeHeld=.8;releaseShot(p1,selected);
    const released=calculateShotContest(p1).value;
    ({selected,preview,released});
  `);
  assert.equal(result.selected,'setshot');assert.ok(result.preview>.28&&result.preview<.48,'a hands-up defender at 1.7m earns a moderate contest');
  assert.equal(result.preview,result.released,'meter and launch see the same current contest');
});

test('jump-shot pressure needs a close squared defender to become smothering',()=>{
  const ctx=setup();
  const result=run(ctx,`(()=>{
    p1.group.position.set(0,0,HOOP_POS.z+5.8);p1.angle=Math.PI;p1.moveState='normal';
    for(const other of matchPlayers)if(other.teamId===1)other.group.position.set(8,0,8);
    takePossession(p1,{silent:true});
    const defender=matchPlayers[3];defender.handsUpTime=.5;
    function pressure(distance,angle=0){
      defender.group.position.copy(p1.group.position).add(new THREE.Vector3(0,0,-distance));
      defender.angle=angle;return calculateShotContest(p1).value;
    }
    const close=pressure(.5),moderate=pressure(1.7),distant=pressure(2.65),turned=pressure(.5,Math.PI);
    defender.handsUpTime=0;const passive=pressure(.5);
    return {close,moderate,distant,turned,passive};
  })()`);
  assert.ok(result.close>.7,JSON.stringify(result));
  assert.ok(result.moderate>.28&&result.moderate<.48,JSON.stringify(result));
  assert.equal(result.distant,0);
  assert.ok(result.turned<result.close*.4&&result.passive<result.close,JSON.stringify(result));
});

test('close contests reward the defender holding the rimward lane rather than a beaten trailing position',()=>{
  const ctx=setup();
  const result=run(ctx,`
    p1.group.position.set(0,0,HOOP_POS.z+2.5);p1.angle=Math.PI;p1.velocity.set(0,0,-4);p1.moveState='normal';
    for(const other of matchPlayers)if(other.teamId===1)other.group.position.set(8,0,8);
    takePossession(p1,{silent:true});
    const defender=matchPlayers[3];defender.handsUpTime=.4;defender.blockTime=.3;
    function pressure(x,z){
      defender.group.position.copy(p1.group.position).add(new THREE.Vector3(x,0,z));
      defender.angle=angleToward(defender.group.position,p1.group.position);
      return calculateShotContest(p1).value;
    }
    const front=pressure(0,-1.15),trailing=pressure(0,1.15),hip=pressure(1.15,0),distant=pressure(0,-2.7);
    defender.handsUpTime=0;defender.blockTime=0;const down=pressure(0,-1.15);
    defender.handsUpTime=.4;defender.blockTime=.3;
    ballState='layup';layupOwner=p1.id;p1.moveMeta.lastShotType='fadeaway';const gather=pressure(0,-1.15);
    ({front,trailing,hip,distant,down,gather});
  `);
  assert.ok(result.front>.4,'square hands-up defense still pressures the finish: '+JSON.stringify(result));
  assert.ok(result.trailing<.16&&result.hip<.24,'getting a step earns a real shooting advantage: '+JSON.stringify(result));
  assert.ok(result.distant<.1,'a remote defender cannot contest the close release: '+JSON.stringify(result));
  assert.ok(result.front>result.down*1.3,'disciplined active hands are more useful than proximity alone');
  assert.equal(result.gather,result.front,'the active gather uses close positioning rather than an old shot type');
});

test('open running layup launch trajectories stay continuous across a quarter-second button release',()=>{
  const ctx=setup();
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/player-development.js'),'utf8'),ctx);
  const result=run(ctx,`
    function gatherLaunch(held,pressure=0){
      for(const other of matchPlayers)if(other.teamId===1)other.group.position.set(8,0,8);
      p1.group.position.set(2.3,.7,HOOP_POS.z);p1.velocity.set(-5.8,0,0);p1.profile.height=.95;
      p1.profile.ratings={layups:41,closeShot:43,releaseStability:43,consistency:45,aerialControl:45};
      p1.profile.abilities={};p1.stamina=.85;takePossession(p1,{silent:true});
      p1.moveMeta.chargeHeld=held;
      calculateShotContest=()=>({value:pressure,label:pressure?'Heavy Contest':'Open'});
      releaseShot(p1,'layup',{releasePoint:new THREE.Vector3(1.98,3,HOOP_POS.z),random:()=>.5});
      return {velocity:ballVelocity.clone(),grade:p1.moveMeta.lastTimingGrade};
    }
    const a=gatherLaunch(.24),b=gatherLaunch(.25),c=gatherLaunch(.35),covered=gatherLaunch(.25,.7),overheld=gatherLaunch(1.6);
    const light=gatherLaunch(.25,.2),moderate=gatherLaunch(.25,.3),edgeA=gatherLaunch(.25,.249),edgeB=gatherLaunch(.25,.251);
    ({boundaryJump:a.velocity.distanceTo(b.velocity),laterChange:b.velocity.distanceTo(c.velocity),
      pressureDifference:b.velocity.distanceTo(covered.velocity),lateDifference:b.velocity.distanceTo(overheld.velocity),
      moderateDifference:b.velocity.distanceTo(moderate.velocity),pressureEdge:edgeA.velocity.distanceTo(edgeB.velocity),
      lightDifference:b.velocity.distanceTo(light.velocity),heavyDifference:moderate.velocity.distanceTo(covered.velocity),
      openGrade:b.grade,finite:[a,b,c,covered,overheld].every(shot=>[shot.velocity.x,shot.velocity.y,shot.velocity.z].every(Number.isFinite))});
  `);
  assert.ok(result.boundaryJump<.08,JSON.stringify(result));
  assert.ok(result.laterChange<.6,'ordinary short holds change touch gradually: '+JSON.stringify(result));
  assert.ok(result.pressureDifference>.1,'real pressure still changes the launch: '+JSON.stringify(result));
  assert.ok(result.moderateDifference<.65,'a nearby defender does not discard all gather touch: '+JSON.stringify(result));
  assert.ok(result.lightDifference<result.moderateDifference&&result.heavyDifference>.1,'pressure has a continuous meaningful cost');
  assert.ok(result.pressureEdge<.03,'crossing quarter pressure cannot abruptly spoil a finish');
  assert.ok(result.lateDifference>.6,'overholding still pays the normal timing penalty: '+JSON.stringify(result));
  assert.equal(result.openGrade,'Open Finish');assert.ok(result.finite);
});

test('a fresh possession reset clears old contact, catch and drive advantages while retaining match stats',()=>{
  const ctx=setup();
  run(ctx,`p1.contactTimer=.5;p1.contactCooldown=.2;p1.contactStrength=1.4;p1.bodyUpTimer=.3;
    p1.lastCatchTime=simulationTime;p1.lastPassQuality=1;p1.moveMeta.approachSpeed=5;p1.moveMeta.activeAbilities=['catchAndShoot'];
    p1.stats.fga=3;resetBody(p1);`);
  assert.equal(run(ctx,'p1.contactTimer+p1.contactCooldown+p1.contactStrength+p1.bodyUpTimer'),0);
  assert.equal(run(ctx,'p1.lastCatchTime'),-Infinity);
  assert.equal(run(ctx,'p1.lastPassQuality'),0);
  assert.equal(run(ctx,'p1.moveMeta.approachSpeed'),0);
  assert.equal(run(ctx,'p1.moveMeta.activeAbilities.length'),0);
  assert.equal(run(ctx,'p1.stats.fga'),3);
});

test('stationary humans and AI use the same upgraded screen anchor and earn screens only from real defender contact',()=>{
  const ctx=setup();
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/player-development.js'),'utf8'),ctx);
  const result=run(ctx,`
    function screenPush(rating,rank,human){
      const screener=matchPlayers[1],defender=matchPlayers[3];
      matchPlayers=[p1,screener,defender];
      p1.group.position.set(4,0,-2);screener.group.position.set(0,0,-2);defender.group.position.set(9,0,-2);
      for(const player of matchPlayers){player.velocity.set(0,0,0);player.moveState='normal';player.onGround=true;player.contactTimer=0;player.contactCooldown=0;}
      screener.profile.ratings={screening:rating};screener.profile.abilities={screenSetter:rank};
      screener.controllerSlot=human?'p2':null;screener.aiScreen=!human;screener.screenSetTime=0;screener.stats.screens=0;screener.lastScreenCreditTime=-10;
      simulationTime=10;takePossession(p1,{silent:true});
      for(let i=0;i<13;i++)resolveBodyCollisions(1/60);
      const beforeContact=screener.stats.screens;
      defender.group.position.set(.96,0,-2);defender.velocity.set(-2,0,0);
      resolveBodyCollisions(1/60);
      const pushed=Math.abs(screener.group.position.x),screens=screener.stats.screens,planted=screener.plantedScreen;
      defender.group.position.set(screener.group.position.x+.96,0,-2);defender.velocity.set(-2,0,0);screener.velocity.set(0,0,0);
      resolveBodyCollisions(1/60);
      const afterRepeated=screener.stats.screens;
      screener.velocity.set(2,0,0);resolveBodyCollisions(1/60);
      const movingPlanted=screener.plantedScreen;
      return {pushed,screens,beforeContact,afterRepeated,planted,movingPlanted};
    }
    const originalPlayers=matchPlayers;
    const rookie=screenPush(40,0,true);matchPlayers=originalPlayers;
    const expert=screenPush(88,3,true);matchPlayers=originalPlayers;
    const bot=screenPush(88,3,false);matchPlayers=originalPlayers;
    ({rookie,expert,bot});
  `);
  assert.ok(result.expert.pushed<result.rookie.pushed*.65,JSON.stringify(result));
  assert.ok(Math.abs(result.expert.pushed-result.bot.pushed)<1e-10,JSON.stringify(result));
  for(const state of [result.rookie,result.expert,result.bot]){
    assert.equal(state.beforeContact,0,'standing near a handler does not earn XP');
    assert.equal(state.screens,1,'a planted screen stopping a real defender earns credit');
    assert.equal(state.afterRepeated,1,'repeated bumps share the screen-credit cooldown');
    assert.equal(state.movingPlanted,false,'moving players cannot retain a human screen anchor');
  }
  assert.equal(result.expert.planted,true);
});

test('an AI intended pass keeps its validated recipient and rejects an opponent or unfaced target',()=>{
  const ctx=setup();
  const result=run(ctx,`
    globalThis.gamePaused=false;
    p1.group.position.set(0,0,-2);p1.angle=0;
    p2.group.position.set(Math.sin(.22)*4,0,-2+Math.cos(.22)*4);
    matchPlayers[2].group.position.set(0,0,6);
    takePossession(p1,{silent:true});
    const automatic=getPassTarget(p1).id;
    const invalid=tryDirectionalPass(p1,matchPlayers[3]);
    p1.angle=Math.PI;
    const unfaced=tryDirectionalPass(p1,p2);
    p1.angle=0;
    const started=tryDirectionalPass(p1,p2);
    ({automatic,invalid,unfaced,started,target:passFlight.targetId});
  `);
  assert.equal(result.automatic,'a1');assert.equal(result.invalid,false);assert.equal(result.unfaced,false);
  assert.equal(result.started,true);assert.equal(result.target,'p2');
});

test("release timing produces repeatable physical misses while pressure keeps green shots difficult",()=>{
  const ctx=setup();
  run(ctx,`
    const actualContest=calculateShotContest;
    let pressure=0;
    calculateShotContest=()=>({value:pressure,label:pressure?'Smothered':'Open'});
    function shot(pct,contest=0,moving=0,distance=5.8){
      const shooter=matchPlayers[2];
      shooter.group.position.set(0,0,HOOP_POS.z+distance);
      shooter.velocity.set(moving,0,0);
      takePossession(shooter,{silent:true});
      pressure=contest;
      const meter=getShotMeterProfile('setshot',shooter,{value:contest});
      const meterPct=pct==='center'?(meter.greenStart+meter.greenEnd)*.5:pct;
      shooter.moveMeta.chargeHeld=meterPct*1.35/meter.meterFillSpeed;
      shotPower=.65;
      releaseShot(shooter,'setshot',{releasePoint:shooter.group.position.clone().setY(2.1)});
      const t=(ballVelocity.y+Math.sqrt(ballVelocity.y*ballVelocity.y-2*(-GRAVITY)*(HOOP_POS.y-ball.position.y)))/(-GRAVITY);
      return ball.position.clone().addScaledVector(ballVelocity,t).add(new THREE.Vector3(0,.5*GRAVITY*t*t,0));
    }
  `);
  const aimDistance=v=>Math.hypot(v.x,v.z+10.8);
  const open=run(ctx,"shot('center')");
  const early=run(ctx,"shot(.3)");
  const covered=run(ctx,"shot('center',1)");
  assert.ok(aimDistance(open)<.15);
  assert.ok(aimDistance(early)>1);
  assert.ok(aimDistance(covered)>.6);
  assert.deepEqual(covered,run(ctx,"shot('center',1)"));
  assert.ok(aimDistance(run(ctx,"shot('center',0,6,12)"))>aimDistance(open)+.3);
});

test("the live ball integrator carries an open green release through the hoop and counts the basket",()=>{
  const ctx=setup();
  run(ctx,`
    graphicsQuality='low';
    const shooter=matchPlayers[2];shooter.group.position.set(0,0,HOOP_POS.z+5.8);
    for(const other of matchPlayers)if(other!==shooter)other.group.position.set(8,0,8);
    takePossession(shooter,{silent:true});
    const meter=getShotMeterProfile('setshot',shooter,calculateShotContest(shooter));
    shooter.moveMeta.chargeHeld=(meter.greenStart+meter.greenEnd)*.5*1.35/meter.meterFillSpeed;
    shotPower=.65;
    releaseShot(shooter,'setshot',{releasePoint:shooter.group.position.clone().setY(2.1)});
    let simulatedTime=0;
    while(ballState==='shot' && simulatedTime<2){
      updateBall(1/60);simulatedTime+=1/60;
    }
  `);
  assert.equal(run(ctx,"scoreP1"),2);
  assert.equal(run(ctx,"scoreP2"),0);
  assert.equal(run(ctx,"matchPlayers[2].stats.fgm"),1);
  assert.equal(run(ctx,"matchPlayers[2].stats.fga"),1);
  assert.equal(run(ctx,"ballState"),"dead");
  assert.equal(run(ctx,"lastScored"),true);
  assert.ok(run(ctx,"flatDistance(ball.position,HOOP_POS)")<.2);
  assert.equal(run(ctx,"pendingInbound.scorerId"),"a1");
});

test("every scorer credits their own team and basket awards cannot duplicate",()=>{
  const ctx=setup();
  run(ctx,"awardBasket(matchPlayers[2],3)");
  assert.equal(run(ctx,"scoreP1"),3);
  assert.equal(run(ctx,"scoreP2"),0);
  assert.equal(run(ctx,"matchPlayers[2].stats.fgm"),1);
  assert.equal(run(ctx,"matchPlayers[2].stats.tpm"),1);
  assert.equal(run(ctx,"awardBasket(matchPlayers[2],3)"),false);
  run(ctx,"lastScored=false; awardBasket(matchPlayers[5],2)");
  assert.equal(run(ctx,"scoreP2"),2);
  assert.equal(run(ctx,"matchPlayers.filter(p=>p.hasBall).length"),0);
  assert.equal(run(ctx,"ballOwner"),null);
});

test("layup gathers release a real shot and count exactly one attempt",()=>{
  const ctx=setup();
  run(ctx,`
    const shooter=matchPlayers[2];shooter.group.position.set(0,0,-8);
    for(const other of matchPlayers)if(other!==shooter)other.group.position.set(10,0,10);
    takePossession(shooter,{silent:true});shooter.moveMeta.chargeHeld=.945;
    startLayup(shooter);updateLayup(.36);
  `);
  assert.equal(run(ctx,"ballState"),"shot");
  assert.equal(run(ctx,"layupActive"),false);
  assert.equal(run(ctx,"ballOwner"),null);
  assert.equal(run(ctx,"matchPlayers[2].stats.fga"),1);
  assert.ok(run(ctx,"ballVelocity.y")>0);
});

test('vertical and layup touch raise the actual gathered release while preserving one physical shot',()=>{
  const results=[];
  for(const rating of [45,95]){
    const ctx=setup();
    vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/player-development.js'),'utf8'),ctx);
    results.push(run(ctx,`
      for(const other of matchPlayers)if(other!==p1)other.group.position.set(9,0,9);
      p1.group.position.set(0,0,HOOP_POS.z+2.7);p1.profile.ratings={jumping:${rating},layups:${rating}};
      takePossession(p1,{silent:true});p1.moveMeta.chargeHeld=.72;startLayup(p1);
      const launch=p1.yVel;
      for(let i=0;i<22;i++){updateJumpPhysics(p1,1/60);updateLayup(1/60);}
      ({launch,height:ball.position.y,feet:p1.group.position.y,state:ballState,attempts:p1.stats.fga,elevation:p1.moveMeta.finishElevation});
    `));
  }
  assert.equal(results[0].state,'shot');assert.equal(results[1].state,'shot');
  assert.equal(results[0].attempts,1);assert.equal(results[1].attempts,1);
  assert.ok(results[1].launch>results[0].launch);assert.ok(results[1].feet>results[0].feet);
  assert.ok(results[1].height>results[0].height+.1);assert.ok(results[1].height-results[0].height<.25,JSON.stringify(results));
});

test('finishing elevation changes continuously between taps and longer holds',()=>{
  const ctx=setup();
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/player-development.js'),'utf8'),ctx);
  const results=run(ctx,`(()=>{
    p1.profile.ratings={jumping:95,layups:95,dunking:95};
    return ['layup','dunk'].map(type=>{
      const samples=[];
      for(let held=.20;held<=.61;held+=.001){
        p1.moveMeta.chargeHeld=held;samples.push(getFinishingElevation(p1,type));
      }
      return samples;
    });
  })()`);
  for(const samples of results){
    for(let i=1;i<samples.length;i++)assert.ok(Math.abs(samples[i]-samples[i-1])<.001,'an extra millisecond cannot abruptly change the reachable ball height');
    assert.ok(samples.every(value=>value>0&&value<=.14));
  }
});

test("inbounds select teams correctly for standard, make-it-take-it and losers-ball rules",()=>{
  const ctx=setup();
  assert.equal(run(ctx,"teamOf(nextPossessionAfterScore('a1'))"),1);
  assert.equal(run(ctx,"gameMode.makeItTakeIt=true;nextPossessionAfterScore('a1')"),"a1");
  run(ctx,"gameMode.makeItTakeIt=false;gameMode.losersBall=true;scoreP1=8;scoreP2=2");
  assert.equal(run(ctx,"teamOf(nextPossessionAfterScore('b3'))"),1);
  run(ctx,"scoreP1=2;scoreP2=8");
  assert.equal(run(ctx,"teamOf(nextPossessionAfterScore('b3'))"),0);
});

test("score and out-of-bounds delays advance only with simulation and resets cancel stale inbounds",()=>{
  const ctx=setup();
  run(ctx,"scheduleScoredPossession('a1',850)");
  assert.equal(run(ctx,"updateScoredPossession(.1)"),true);
  assert.ok(run(ctx,"pendingInbound.remaining")>.7);
  run(ctx,"resetPossession('a1',false)");
  assert.equal(run(ctx,"pendingInbound"),null);
  assert.equal(run(ctx,"updateScoredPossession(2)"),false);
  run(ctx,"scheduleDeadPossession('b3',420);updateScoredPossession(.5)");
  assert.equal(run(ctx,"ballOwner"),"b3");
  assert.equal(run(ctx,"matchPlayers.filter(p=>p.hasBall).length"),1);
});

test("a defender blocking the ball out returns possession to offense without changing the shot scorer",()=>{
  const ctx=setup();
  run(ctx,`
    getGameplayTuning=()=>({stealWindow:1,contestStrength:1,takeoverRate:1,outOfBounds:true});
    lastShotShooter='a1';lastTouchPlayerId='a1';ballState='shot';hasShotThisPossession=true;
    ball.position.set(2,2,-5);
    applyBlockDeflection(matchPlayers[5],matchPlayers[2]);
    ball.position.x=COURT_W/2+1;
    keepBallInBounds();
  `);
  assert.equal(run(ctx,"lastTouchPlayerId"),"b3");
  assert.equal(run(ctx,"lastShotShooter"),"a1");
  assert.equal(run(ctx,"teamOf(pendingInbound.ownerId)"),0);
  assert.equal(run(ctx,"ballState"),"dead");
  run(ctx,"updateScoredPossession(.5)");
  assert.equal(run(ctx,"teamOf(ballOwner)"),0);

  const redirected=setup();
  run(redirected,`
    lastShotShooter='a1';ballState='shot';hasShotThisPossession=true;
    ball.position.set(2,2,-5);applyBlockDeflection(matchPlayers[5],matchPlayers[2]);
    ball.position.copy(HOOP_POS).add(new THREE.Vector3(0,-.1,0));ballVelocity.set(0,-2,0);checkScore();
  `);
  assert.equal(run(redirected,"lastTouchPlayerId"),"b3");
  assert.equal(run(redirected,"lastShotShooter"),"a1");
  assert.equal(run(redirected,"scoreP1"),2);
  assert.equal(run(redirected,"scoreP2"),0);
  assert.equal(run(redirected,"matchPlayers[2].stats.fgm"),1);
});

test("loose rebounds can be secured by the sixth roster player without duplicate owners",()=>{
  const ctx=setup();
  run(ctx,`
    ballState='loose';loosePickupDelay=0;ballVelocity.set(1,0,0);
    ball.position.set(7,.3,1);matchPlayers[5].group.position.set(7,0,1);
    matchPlayers[0].hasBall=true;handleLooseBallPickup();
  `);
  assert.equal(run(ctx,"ballOwner"),"b3");
  assert.equal(run(ctx,"possession"),"b3");
  assert.equal(run(ctx,"matchPlayers.filter(p=>p.hasBall).length"),1);
  assert.equal(run(ctx,"ballState"),"dribble");
});

test("a steal needs an opponent, exposed ball, facing direction and the live swipe interval",()=>{
  const ctx=setup();
  run(ctx,`
    takePossession(p1,{silent:true});p1.group.position.set(0,0,0);
    ball.position.set(.8,.65,0);
    p2.testHand=ball.position.clone();p2.swipeTime=.18;
    const defender=matchPlayers[5];defender.group.position.set(1.8,0,0);defender.angle=-Math.PI/2;
    defender.testHand=new THREE.Vector3(.9,.65,0);defender.swipeTime=.33;
    checkSteals();
  `);
  assert.equal(run(ctx,"ballOwner"),"p1");
  run(ctx,"matchPlayers[5].swipeTime=.18;matchPlayers[5].reachRecovery=.2;checkSteals()");
  assert.equal(run(ctx,"ballOwner"),"p1");
  run(ctx,"matchPlayers[5].reachRecovery=0;checkSteals()");
  assert.equal(run(ctx,"ballOwner"),"b3");
  assert.equal(run(ctx,"matchPlayers[5].stats.steals"),1);
  assert.equal(run(ctx,"p2.stats.steals"),0);
  assert.equal(run(ctx,"matchPlayers.filter(p=>p.hasBall).length"),1);
});

test("shot coverage ignores teammates and includes a nearby help defender",()=>{
  const ctx=setup();
  run(ctx,`
    p2.group.position.copy(p1.group.position);p2.blockTime=.2;
    for(const other of matchPlayers)if(other.teamId===1)other.group.position.set(8,0,8);
  `);
  assert.equal(run(ctx,"calculateShotContest(p1).value"),0);
  run(ctx,"matchPlayers[5].group.position.copy(p1.group.position).add(new THREE.Vector3(0,0,-.8));matchPlayers[5].angle=0;matchPlayers[5].handsUpTime=.2");
  assert.ok(run(ctx,"calculateShotContest(p1).value")>.4);
  assert.equal(run(ctx,"calculateShotContest(p1).defender.id"),"b3");
});

test("teammates cannot block shots while any correctly positioned opponent can",()=>{
  const ctx=setup();
  run(ctx,`
    lastShotShooter='a1';ballState='shot';ball.position.set(0,2.3,-4);previousBallPosition.copy(ball.position);
    ballVelocity.set(0,3,-8);p2.blockTime=.2;p2.testHand=ball.position.clone();
    checkBlocksAndCatches();
  `);
  assert.equal(run(ctx,"ballState"),"shot");
  run(ctx,`
    const blocker=matchPlayers[4];blocker.group.position.set(0,0,-4);
    blocker.blockTime=.2;blocker.onGround=false;blocker.testHand=ball.position.clone();
    checkBlocksAndCatches();
  `);
  assert.equal(run(ctx,"ballState"),"loose");
  assert.equal(run(ctx,"matchPlayers[4].stats.blocks"),1);
  assert.equal(run(ctx,"p2.stats.blocks"),0);
});

test("all roster pairs occupy space and a planted screen resists displacement",()=>{
  const ctx=setup();
  run(ctx,`
    for(let i=0;i<matchPlayers.length;i++)matchPlayers[i].group.position.set(i*3,0,0);
    matchPlayers[2].group.position.set(0,0,8);matchPlayers[5].group.position.set(.8,0,8);
    matchPlayers[2].aiScreen=true;ballState='loose';possession='p1';
    resolveBodyCollisions(1/60);
  `);
  const screenMove=Math.abs(run(ctx,"matchPlayers[2].group.position.x"));
  const otherMove=Math.abs(run(ctx,"matchPlayers[5].group.position.x")-.8);
  assert.ok(otherMove>screenMove*4);
  run(ctx,`
    p1.group.position.set(0,0,0);p2.group.position.set(.8,0,0);
    p1.velocity.set(3,0,0);resolveBodyCollisions(1/60);
  `);
  assert.ok(run(ctx,"flatDistance(p1.group.position,p2.group.position)")>.8);
  assert.ok(run(ctx,"p1.velocity.x")<3);
});

test("a missed pass dropping through the rim cannot score as a stale shot",()=>{
  const ctx=setup();
  run(ctx,"ballState='loose';hasShotThisPossession=false;ball.position.copy(HOOP_POS).add(new THREE.Vector3(0,-.1,0));ballVelocity.set(0,-2,0);checkScore()");
  assert.equal(run(ctx,"scoreP1+scoreP2"),0);
});
