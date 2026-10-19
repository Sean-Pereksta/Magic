const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

class Vector3 {
  constructor(x=0,y=0,z=0){this.set(x,y,z)}
  set(x,y,z){this.x=x;this.y=y;this.z=z;return this}
  setY(y){this.y=y;return this}
  copy(v){return this.set(v.x,v.y,v.z)}
  clone(){return new Vector3(this.x,this.y,this.z)}
  add(v){this.x+=v.x;this.y+=v.y;this.z+=v.z;return this}
  sub(v){this.x-=v.x;this.y-=v.y;this.z-=v.z;return this}
  multiplyScalar(s){this.x*=s;this.y*=s;this.z*=s;return this}
  addScaledVector(v,s){this.x+=v.x*s;this.y+=v.y*s;this.z+=v.z*s;return this}
  dot(v){return this.x*v.x+this.y*v.y+this.z*v.z}
  lengthSq(){return this.dot(this)}
  length(){return Math.sqrt(this.lengthSq())}
  normalize(){return this.multiplyScalar(1/(this.length()||1))}
  distanceToSquared(v){return this.clone().sub(v).lengthSq()}
  distanceTo(v){return Math.sqrt(this.distanceToSquared(v))}
  lerp(v,t){return this.addScaledVector(v.clone().sub(this),t)}
  reflect(n){return this.addScaledVector(n,-2*this.dot(n))}
}

function setup(){
  const ctx=vm.createContext({THREE:{Vector3,MathUtils:{clamp:(v,a,b)=>Math.max(a,Math.min(b,v)),lerp:(a,b,t)=>a+(b-a)*t,degToRad:d=>d*Math.PI/180}},performance:{now:()=>5000},console});
  for(const file of ['state','rendering','player-development','team-play','animation-contact','ball-physics','scoring','presentation']){
    vm.runInContext(fs.readFileSync(path.join(__dirname,`../js/${file}.js`),'utf8'),ctx);
  }
  vm.runInContext(`
    function flatDistance(a,b){return Math.hypot(a.x-b.x,a.z-b.z)}
    function getForward(a){return new THREE.Vector3(Math.sin(a),0,Math.cos(a))}
    function angleToward(a,b){return Math.atan2(b.x-a.x,b.z-a.z)}
    function smoothstep(t){return t*t*(3-2*t)}
    function getScreenSideVectorForChar(p,s){return new THREE.Vector3(s==='left'?-1:1,0,0)}
    function keepInCourt(){} function updateMomentum(){} function updateScoreboard(){}
    function flash(){} function triggerShotSplash(){} function triggerCrowdReaction(){}
    function showShotBreakdown(){} function playShotSound(){} function playDribbleSound(){}
    function playSwishSound(){} function playCrowdPulse(){} function pulseArenaLights(){}
    function playRimSound(){} function spawnParticle(){} function createShotTrail(){}
    function showShotFeedback(){} function showMoveFeedback(){} function applyCameraShake(){}
    function playBlockSound(){} function addTakeover(){}
    let randomSeed=42871;
    Math.random=()=>{randomSeed=(Math.imul(randomSeed,1664525)+1013904223)>>>0;return randomSeed/4294967296;};
    function getWorldHand(p,side){
      return p.testHand ? p.testHand.clone() : p.group.position.clone().add(new THREE.Vector3(side==='left'?-.3:.3,.95*p.profile.height,0));
    }
    function attachBallToHand(p,s){ball.position.copy(getWorldHand(p,s))}
    function create(id,team){return {
      id,teamId:team,controllerSlot:null,hasBall:false,angle:Math.PI,moveState:'normal',
      group:{position:new THREE.Vector3(8,0,8),rotation:new THREE.Vector3(),scale:new THREE.Vector3(1,1,1)},
      profile:{name:id,width:1,height:1,shot:1,layup:1,handle:1,jump:1,steal:1,block:1},
      stamina:1,sprint:1,velocity:new THREE.Vector3(),anim:{},moveMeta:{},movement:{contactStrength:1,recoverySpeed:1},
      stats:{fgm:0,fga:0,tpm:0,tpa:0,perfect:0,steals:0,blocks:0,turnovers:0},
      bodyRadius:.55,blockTime:0,handsUpTime:0,swipeTime:0,reachRecovery:0,stumbledTimer:0,
      onGround:true,jumpCooldown:0,yVel:0,leftArm:{rotation:new THREE.Vector3()},rightArm:{rotation:new THREE.Vector3()},
      leftForearm:{getWorldPosition(v){return v.copy(getWorldHand(this.owner,'left'))}},
      rightForearm:{getWorldPosition(v){return v.copy(getWorldHand(this.owner,'right'))}}
    }}
    matchPlayers=[create('p1',0),create('p2',1)];p1=matchPlayers[0];p2=matchPlayers[1];
    for(const p of matchPlayers){p.leftForearm.owner=p;p.rightForearm.owner=p}
    p1.controllerSlot='p1';inputManager={vibrate(){}};
    getGameplayTuning=()=>({stealWindow:1,contestStrength:1,takeoverRate:1,ballBounce:1,outOfBounds:false});
    ball={position:new THREE.Vector3(),rotation:new THREE.Vector3(),scale:new THREE.Vector3(1,1,1)};
    ballShadow=null;backboard={};graphicsQuality='low';gameStarted=true;targetScore=99999;
    function playAttempt(config={}){
      const {distance=2.5,angle=0,held=.08,meterPct=null,speed=0,dt=1/60,pressure=0,ratings=null,abilities=[]}=config;
      p1.profile.ratings=ratings||{};p1.profile.abilities=abilities;p1.stamina=config.stamina??1;
      pendingInbound=null;possessionFrozen=false;
      p1.group.position.set(Math.sin(angle)*distance,0,HOOP_POS.z+Math.cos(angle)*distance);
      p1.angle=angleToward(p1.group.position,HOOP_POS);
      p1.velocity.copy(HOOP_POS).sub(p1.group.position).setY(0).normalize().multiplyScalar(speed);
      p1.moveState='normal';p1.moveMeta={};p1.onGround=true;p1.yVel=0;p1.testHand=null;
      p2.group.position.set(8,0,8);p2.handsUpTime=0;p2.blockTime=0;
      if(pressure){p2.group.position.copy(p1.group.position).addScaledVector(getForward(p1.angle),pressure===1?.72:1.7);p2.angle=p1.angle+Math.PI;p2.handsUpTime=.5;}
      takePossession(p1,{silent:true});
      const expectedType=chooseShotType(p1);
      const meter=getShotMeterProfile(expectedType,p1,calculateShotContest(p1));
      p1.moveMeta.chargeHeld=meterPct===null ? held : meterPct==='center' ? (meter.greenStart+meter.greenEnd)*.5*1.35/meter.meterFillSpeed : meterPct==='start' ? meter.greenStart*1.35/meter.meterFillSpeed : meterPct==='end' ? meter.greenEnd*1.35/meter.meterFillSpeed : meterPct*1.35/meter.meterFillSpeed;
      const previousMakes=p1.stats.fgm,previousAttempts=p1.stats.fga;
      shotPower=.65;releaseOffenseMove(p1);
      let elapsed=0,releaseHeight=null,maxHeight=ball.position.y;
      while(!lastScored && elapsed<2.5){
        updateJumpPhysics(p1,dt);
        updateBall(dt);
        updateLayup(dt);
        loosePickupDelay=Math.max(0,loosePickupDelay-dt);
        ballCollisionCooldown=Math.max(0,ballCollisionCooldown-dt);
        backboardCollisionCooldown=Math.max(0,backboardCollisionCooldown-dt);
        if(ballState==='shot'&&releaseHeight===null)releaseHeight=lastShotReleasePos.y;
        maxHeight=Math.max(maxHeight,ball.position.y);elapsed+=dt;
        if(ballState==='dribble' && elapsed>.6)break;
      }
      return {made:p1.stats.fgm>previousMakes,attempts:p1.stats.fga-previousAttempts,grade:p1.moveMeta.lastTimingGrade,type:expectedType,releaseHeight,maxHeight,lastTouch:lastTouchPlayerId,shooter:lastShotShooter,state:ballState,spread:p1.moveMeta.lastAimSpread};
    }
  `,ctx);
  return ctx;
}
function run(ctx,source){return vm.runInContext(source,ctx)}
function attempt(ctx,options){return run(ctx,`playAttempt(${JSON.stringify(options)})`)}

test('tapped open layups use the complete choose, gather, release and physical flight pipeline',()=>{
  const ctx=setup();
  for(const distance of [1.6,2.4,3.2])for(const degrees of [-75,-40,0,40,75])for(const speed of [2,4.5]){
    const config={distance,angle:degrees*Math.PI/180,speed,held:.08};
    const result=attempt(ctx,config);
    assert.equal(result.type,'layup',JSON.stringify({config,result}));
    assert.equal(result.made,true,JSON.stringify({config,result}));
    assert.equal(result.attempts,1);
    assert.equal(result.grade,'Open Finish');
    assert.ok(result.releaseHeight>=2.4);
    assert.ok(result.maxHeight>3.28);
    assert.equal(result.lastTouch,'p1');
    assert.equal(result.shooter,'p1');
  }
});

test('standing close finishes and open jumper timing bands remain playable without automatic makes',()=>{
  const ctx=setup();
  for(const distance of [.4,.8,1.2,1.7,2.7]){
    const result=attempt(ctx,{distance,speed:0,held:.08});
    assert.equal(result.type,'layup');assert.equal(result.made,true,JSON.stringify(result));
  }
  let made=0,total=0;
  for(const distance of [4.2,5.8,7.6])for(const degrees of [-65,0,65])for(const meterPct of ['start','center','end',.5,.8]){
    const config={distance,angle:degrees*Math.PI/180,meterPct};
    const result=attempt(ctx,config);
    assert.equal(result.type,'setshot');
    total++;if(result.made)made++;
    assert.equal(result.attempts,1);
    assert.ok(result.releaseHeight>=2.2);
  }
  assert.ok(made/total>.35&&made/total<.85,`physical make rate ${made}/${total}`);
});

test('open finishes are stable across frame intervals while smothered tap finishes can fail',()=>{
  const ctx=setup();
  for(const dt of [1/30,1/60,1/120]){
    assert.equal(attempt(ctx,{distance:2.5,speed:2,held:.08,dt}).made,true);
    run(ctx,'randomSeed=42871');
    const shot=attempt(ctx,{distance:5.8,meterPct:'center',dt});
    assert.equal(shot.attempts,1);
    assert.equal(shot.grade,'Green Release');
  }
  const crowded=attempt(ctx,{distance:2.5,speed:2,held:.08,pressure:1});
  assert.equal(crowded.made,false,JSON.stringify(crowded));
  assert.equal(crowded.attempts,1);
  assert.notEqual(crowded.grade,'Open Finish');
  assert.equal(attempt(ctx,{distance:5.8,meterPct:.05}).made,false);
  assert.equal(attempt(ctx,{distance:5.8,meterPct:1}).made,false);
});

test('seeded full-flight calibration rewards midrange skill, space, timing and balance',()=>{
  const ctx=setup();
  const results=run(ctx,`
    function measure(config,n=900){randomSeed=42871;let made=0;for(let i=0;i<n;i++)made+=playAttempt({...config,distance:5.8,meterPct:config.meterPct??'center'}).made;return made/n;}
    const average={ratings:{midrange:60,releaseStability:60,consistency:60}};
    const specialist={ratings:{midrange:88,releaseStability:84,consistency:82},abilities:['midrangeAssassin']};
    ({average:measure(average),specialist:measure(specialist),moderate:measure({...average,pressure:.5}),
      heavy:measure({...average,pressure:1}),poorTiming:measure({...average,meterPct:.15}),
      moving:measure({...average,speed:5}),fatigued:measure({...average,stamina:.25})});
  `);
  console.log('Midrange physical calibration:',JSON.stringify(results));
  assert.ok(results.average>=.5&&results.average<=.6,JSON.stringify(results));
  assert.ok(results.specialist>=.6&&results.specialist<=.7,JSON.stringify(results));
  assert.ok(results.moderate<results.average-.09,JSON.stringify(results));
  assert.ok(results.heavy<results.moderate-.08,JSON.stringify(results));
  assert.ok(results.poorTiming<.12,JSON.stringify(results));
  assert.ok(results.moving<results.average-.12,JSON.stringify(results));
  assert.ok(results.fatigued<results.average-.08,JSON.stringify(results));
});

test('a fast ball scores only on a downward swept hoop crossing with full ball clearance',()=>{
  const ctx=setup();
  run(ctx,`takePossession(p1,{silent:true});lastShotShooter='p1';hasShotThisPossession=true;ballState='shot';
    previousBallPosition.copy(HOOP_POS).add(new THREE.Vector3(.02,.5,.03));
    ball.position.copy(HOOP_POS).add(new THREE.Vector3(.03,-.8,.04));ballVelocity.set(0,-20,0);checkScore();`);
  assert.equal(run(ctx,'scoreP1'),2);
  run(ctx,`lastScored=false;ballState='shot';previousBallPosition.copy(HOOP_POS).add(new THREE.Vector3(0,-.2,0));ball.position.copy(HOOP_POS).add(new THREE.Vector3(0,-.25,0));checkScore();`);
  assert.equal(run(ctx,'scoreP1'),2,'a ball already below the rim does not score again');
  run(ctx,`previousBallPosition.copy(HOOP_POS).add(new THREE.Vector3(.4,.5,0));ball.position.copy(HOOP_POS).add(new THREE.Vector3(.4,-.8,0));checkScore();`);
  assert.equal(run(ctx,'scoreP1'),2,'an overlap with the rim is not a clean basket');
  run(ctx,`previousBallPosition.copy(HOOP_POS).add(new THREE.Vector3(0,-.2,0));ball.position.copy(HOOP_POS).add(new THREE.Vector3(0,.2,0));ballVelocity.y=10;checkScore();`);
  assert.equal(run(ctx,'scoreP1'),2,'rising from beneath the rim is not a basket');
});

test('the static glass slab sweeps fast impacts and applies exactly one outward bounce',()=>{
  const ctx=setup();
  run(ctx,`ballState='shot';previousBallPosition.set(0,HOOP_GEOMETRY.boardCenterY,HOOP_GEOMETRY.boardCenterZ+1.2);
    ball.position.set(0,HOOP_GEOMETRY.boardCenterY,HOOP_GEOMETRY.boardCenterZ-.5);ballVelocity.set(2,-3,-20);
    resolveBackboardBallCollision(.085);`);
  assert.ok(Math.abs(run(ctx,'ballVelocity.z')-14.4)<1e-10);
  assert.ok(Math.abs(run(ctx,'ballVelocity.x')-1.92)<1e-10);
  assert.equal(run(ctx,'shotHasHitRimOrBoard'),true);
  assert.ok(run(ctx,'ball.position.z>HOOP_GEOMETRY.boardCenterZ+HOOP_GEOMETRY.boardThickness*.5+BALL_RADIUS'));
  run(ctx,'resolveBackboardBallCollision(.085)');
  assert.ok(Math.abs(run(ctx,'ballVelocity.z')-14.4)<1e-10,'cooldown cannot invert a bank rebound back through the glass');
  run(ctx,`backboardCollisionCooldown=0;previousBallPosition.set(0,HOOP_GEOMETRY.boardCenterY,HOOP_GEOMETRY.boardCenterZ-1.2);
    ball.position.set(0,HOOP_GEOMETRY.boardCenterY,HOOP_GEOMETRY.boardCenterZ+.5);ballVelocity.set(0,0,20);resolveBackboardBallCollision(.085);`);
  assert.ok(Math.abs(run(ctx,'ballVelocity.z')+14.4)<1e-10,'rear impacts rebound toward the baseline');
});

test('rim contact uses the tube normal in three dimensions without artificial upward kicks',()=>{
  const ctx=setup();
  run(ctx,`ballState='shot';ball.position.copy(HOOP_POS).add(new THREE.Vector3(RIM_RADIUS,.23,0));ballVelocity.set(0,-5,0);handleBackboardAndRim();`);
  assert.ok(run(ctx,'ballVelocity.y')>0,'a top contact bounces upward');
  run(ctx,`rimCollisionCooldown=0;ball.position.copy(HOOP_POS).add(new THREE.Vector3(RIM_RADIUS,-.23,0));ballVelocity.set(0,5,0);handleBackboardAndRim();`);
  assert.ok(run(ctx,'ballVelocity.y')<0,'an underside contact rebounds downward');
  run(ctx,`rimCollisionCooldown=0;ball.position.copy(HOOP_POS).add(new THREE.Vector3(RIM_RADIUS+.23,0,0));ballVelocity.set(-5,-2,0);handleBackboardAndRim();`);
  assert.ok(run(ctx,'ballVelocity.x')>0,'an outer contact reflects horizontally');
  assert.ok(run(ctx,'ballVelocity.y')<0,'horizontal contact retains downward momentum');
});

test('a real high bank trajectory rebounds off the glass and can then fall through the hoop',()=>{
  const ctx=setup();
  run(ctx,`
    takePossession(p1,{silent:true});p1.hasBall=false;ballOwner=null;ballState='shot';
    hasShotThisPossession=true;lastShotShooter='p1';lastShotValue=2;p1.stats.fga=1;
    ball.position.set(0,3.05,HOOP_POS.z+1.8);previousBallPosition.copy(ball.position);
    const boardFace=HOOP_GEOMETRY.boardCenterZ+HOOP_GEOMETRY.boardThickness*.5+BALL_RADIUS;
    const incomingTime=.6;
    const incomingZ=(boardFace-ball.position.z)/incomingTime;
    const outgoingTime=(HOOP_POS.z-boardFace)/(-incomingZ*HOOP_GEOMETRY.boardRestitution);
    const damping=HOOP_GEOMETRY.boardTangentialDamping;
    const incomingY=(HOOP_POS.y-ball.position.y-.5*GRAVITY*incomingTime*incomingTime-GRAVITY*incomingTime*damping*outgoingTime-.5*GRAVITY*outgoingTime*outgoingTime)/(incomingTime+damping*outgoingTime);
    ballVelocity.set(0,incomingY,incomingZ);
    let bankMadeContact=false;
    for(let i=0;i<240&&!lastScored;i++){
      updateBall(1/120);backboardCollisionCooldown=Math.max(0,backboardCollisionCooldown-1/120);
      if(shotHasHitRimOrBoard)bankMadeContact=true;
    }
  `);
  assert.equal(run(ctx,'bankMadeContact'),true);
  assert.equal(run(ctx,'scoreP1'),2);
  assert.equal(run(ctx,'p1.stats.fgm'),1);
  assert.equal(run(ctx,'p1.stats.fga'),1);
});
