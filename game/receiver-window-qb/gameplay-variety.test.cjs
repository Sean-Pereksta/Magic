// Execute the real runtime/Three.js math with only DOM and GPU I/O substituted.
// This covers state/physics integration; browser-smoke remains the rendering gate.
const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const THREE=require('./vendor/three.min.js'),P=require('./progression.js'),V=require('./variety.js');
const source=fs.readFileSync(__dirname+'/game.js','utf8');
function game(){
  const elements=new Map(),storage=new Map();
  function element(){return {style:{},dataset:{},hidden:false,children:[],classList:{toggle(){},add(){},remove(){}},
    setAttribute(){},addEventListener(){},appendChild(child){this.children.push(child)},replaceChildren(){this.children=[]},
    querySelector(){return element()},getContext(){return {fillText(){},clearRect(){},fillRect(){}}},getBoundingClientRect(){return {left:0,top:0,width:1200,height:800}}};}
  const document={body:element(),getElementById(id){if(!elements.has(id))elements.set(id,element());return elements.get(id)},
    createElement:element,querySelectorAll:()=>[],addEventListener(){},exitPointerLock(){}};
  class Renderer{constructor(){this.shadowMap={};}setPixelRatio(n){this.pixelRatio=n}setSize(){}render(){}}
  const context=vm.createContext({THREE:{...THREE,WebGLRenderer:Renderer},QBProgression:P,QBVariety:V,QBFranchise:require('./franchise.js'),document,
    localStorage:{getItem:k=>storage.get(k),setItem:(k,v)=>storage.set(k,v)},performance:{now:()=>0},
    innerWidth:1200,innerHeight:800,devicePixelRatio:1,requestAnimationFrame(){},addEventListener(){},setInterval(){},
    matchMedia:()=>({matches:false}),console,Math:Object.create(Math)});
  const hook=`globalThis.q={resumeGame,setupPlay,beginCountdown,update,throwBall,checkBallContact,resolveCatch,finishPlayAtSpot,activateFranchise,recordAttempt,
    pathFor,routePosition,defenderTarget,updateDefender,catchPlacement,animatePlayerContact,sampleContactRig,playerBallContacts,sweepLimb,deflectBall,finishSecuringCatch,wantsHighPoint,updateJump,updateBallCarrier,applyFieldTheme,setQuality,updateEnvironment,
    captureReplay,startReplay,updateReplay,finishReplay,scenePose,drawTrajectory,assignAudibleRoute,tryJuke,updateRunAfterCatch,clearGoalLane,startDivingTackle,tackleContact,triggerTackle,updateTackle,runnerFinishAbility,effortDivePlan,tryEffortDive,carriedBallFrontZ,poseFinishPlayer,markCatch,scheduleResult,loop,showMainMenu,
    state:()=>({franchise,receivers,defenders,playState,ballLive,currentDefense,snapMemory,replayFrames,replay,transition,qualityLevel,rain,renderer,arcGeo}),
    get ball(){return ball},get ballPrev(){return ballPrev},get ballVel(){return ballVel},get camera(){return camera},
    run(code){return eval(code)}};`;
  vm.runInContext(fs.readFileSync(__dirname+'/presentation.js','utf8'),context);
  vm.runInContext(source.replace(/\}\)\(\);\s*$/,hook+'})();'),context);context.q.resumeGame();return context;
}
test('all routes and coverage identities run finite live physics near midfield and goal line',()=>{
  const c=game(),q=c.q;
  for(const spot of [0,45])for(let round=1;round<=8;round++){
    q.run(`franchise.round=${round};ballSpotYards=${spot};setupPlay(true);playState='live';`);
    const routes=['Fade','Curl','Double Move','Wheel'];
    q.state().receivers.forEach((r,i)=>{r.route=routes[i];r.path=q.pathFor(r.route,r.start)});
    for(let n=0;n<180;n++){q.run('gameTime+=1000/60');q.update(1/60,n*1000/60);}
    for(const a of [...q.state().receivers,...q.state().defenders]){
      assert.ok(Number.isFinite(a.mesh.position.length()));assert.ok(a.velocity.length()<15);
    }
  }
});
test('presnap resets preserve defense; attempts update next-snap reads only once; saves retain them',()=>{
  const c=game(),q=c.q,coverage=q.state().currentDefense;
  q.setupPlay();assert.equal(q.state().currentDefense,coverage);
  q.run("attemptPending=true;ball.position.z=-20");q.recordAttempt(q.state().receivers[0]);q.recordAttempt(q.state().receivers[0]);
  assert.equal(q.state().franchise.scouting.length,1);assert.equal(q.state().snapMemory.depth,0);
  q.run("franchise.scouting=Array.from({length:12},()=>({depth:30,target:0,route:'Go'}));setupPlay(true)");
  assert.equal(q.state().snapMemory.depth,3);
  const saved=P.validateSave(JSON.stringify(q.state().franchise));q.activateFranchise(saved,0);q.resumeGame();
  assert.equal(q.state().snapMemory.depth,3);
});
test('zone defenders use observed movement and cannot see hidden ball destinations or called routes',()=>{
  const c=game(),q=c.q;
  q.run("currentDefense=defenses.find(d=>d.id==='underzone');playState='live'");
  const d=q.state().defenders[0],before=q.defenderTarget(d,0);
  for(const r of q.state().receivers){r.route='Double Move';r.path=[new THREE.Vector3(99,0,99)];}
  assert.ok(before.distanceTo(q.defenderTarget(d,0))<1e-8);
  q.run("ballLive=true;throwTime=0");d.heading.set(0,0,-1);q.ball.position.set(d.mesh.position.x,2,d.mesh.position.z+40);
  const hidden=q.defenderTarget(d,0);q.ballVel.set(70,0,0);assert.ok(hidden.distanceTo(q.defenderTarget(d,0))<1e-8);
});
test('catches use momentum and sideline catches end at their real spot',()=>{
  const c=game(),q=c.q;c.Math.random=()=>0;
  function arrange(x=0){q.setupPlay();q.run("playState='thrown';ballLive=true;attemptPending=true");const r=q.state().receivers[0];r.mesh.position.set(x,0,10);r.heading.set(0,0,-1);r.velocity.set(0,0,-8);r.trackingBall=true;for(const d of q.state().defenders)d.mesh.position.set(80,0,80);q.ball.position.set(x,1.4,9.5);q.ballPrev.copy(q.ball.position);return r;}
  let r=arrange();q.resolveCatch(r,false);assert.equal(q.state().playState,'run');assert.ok(r.velocity.length()>7.7);
  r=arrange();q.ball.position.z=10.6;q.resolveCatch(r,false);assert.ok(r.velocity.length()<5);
  r=arrange(25);const z=r.mesh.position.z;q.resolveCatch(r,false);assert.equal(q.state().playState,'sidelinecatch');q.update(.016,16);assert.equal(r.mesh.position.z,z);q.state().transition.fn();assert.equal(q.state().playState,'dead');
});
test('packed replay ring, trajectory buffers and model geometry are bounded and isolated',()=>{
  const c=game(),q=c.q;const geometry=q.state().receivers[0].mesh.userData.body.geometry;
  q.setupPlay();assert.equal(q.state().receivers[0].mesh.userData.body.geometry,geometry);
  const array=q.state().arcGeo.attributes.position.array;for(let i=0;i<100;i++)q.drawTrajectory(new THREE.Vector3(0,2,40),new THREE.Vector3(0,6,-30));
  assert.equal(q.state().arcGeo.attributes.position.array,array);
  q.run('replayRecording=true');for(let i=0;i<500;i++){q.run('gameTime+=1000/30');q.ball.position.z=-i/10;q.captureReplay(true);}
  const frames=q.state().replayFrames;assert.ok(frames.length<=360);assert.ok(frames[0].ball.z===0);assert.equal(frames.at(-1).ball.z,-49.9);assert.equal(frames[0].transforms.BYTES_PER_ELEMENT,4);
  const cash=q.state().franchise.cash,pos=q.state().receivers[0].mesh.position.clone();let completed=0;
  q.startReplay(frames,()=>completed++);q.updateReplay(.05);q.finishReplay();q.finishReplay();assert.equal(completed,1);
  assert.equal(q.state().franchise.cash,cash);assert.ok(pos.distanceTo(q.state().receivers[0].mesh.position)<.00001);
});
test('weather stays per-match, low graphics removes rain/shadows, auto scales only sustained slow play',()=>{
  const c=game(),q=c.q;q.run('franchise.matchWeather=4');q.applyFieldTheme();q.setupPlay();assert.equal(q.state().franchise.matchWeather,4);
  q.setQuality('low');assert.equal(q.state().rain.visible,false);assert.equal(q.state().renderer.shadowMap.enabled,false);
  q.setQuality('high');assert.equal(q.state().rain.visible,true);q.setQuality('auto');q.run("playState='live'");
  for(let i=0;i<180;i++)q.updateEnvironment(1/30,33);assert.equal(q.state().qualityLevel,1);
});

function arrangeCarrier(c,x=0,z=10){
  const q=c.q;q.setupPlay();c.Math.random=()=>.999;
  const r=q.state().receivers[0];r.mesh.position.set(x,0,z);r.heading.set(0,0,-1);r.velocity.set(0,0,-8);r.maxSpeed=8;
  r.profile.strength=60;r.profile.evasion=70;r.jukeCooldown=10;r.trackingBall=true;
  for(const d of q.state().defenders){d.mesh.position.set(90,0,90);d.strength=75;d.velocity.set(0,0,0);}
  q.ball.position.set(x,1.4,z-.5);q.ballPrev.copy(q.ball.position);q.resolveCatch(r,false);r.velocity.set(0,0,-8);
  return r;
}
test('close rear pursuit tracks the runner, and wrap reach prevents endless tailgating',()=>{
  const c=game(),q=c.q,r=arrangeCarrier(c),d=q.state().defenders[0];
  r.history=[{t:0,p:new THREE.Vector3(0,0,18),v:new THREE.Vector3(0,0,-8)}];
  d.mesh.position.set(0,0,11.05);d.heading.set(0,0,-1);d.velocity.set(0,0,-8);
  assert.ok(q.defenderTarget(d,100).z<r.mesh.position.z,'cannot aim at the old point behind the carrier');
  q.run('gameTime+=1000/60;updateRunAfterCatch(1/60,gameTime)');
  assert.equal(q.state().playState,'tackle');assert.ok(r.mesh.position.z>9,'tackle occurs at contact');
});
test('swept tackles catch crossing contact that discrete endpoints miss',()=>{
  const c=game(),q=c.q,r=arrangeCarrier(c),d=q.state().defenders[0];
  r.tacklePrevious=r.mesh.position.clone();d.tacklePrevious=new THREE.Vector3(-2,0,10);d.mesh.position.set(2,0,10);
  const contact=q.tackleContact(d,r);assert.ok(contact);assert.ok(contact.time<.5);assert.equal(contact.distance,0);
  d.fakeUntil=9999;assert.ok(q.tackleContact(d,r),'a juke cannot switch off real body contact');
});
test('nearby pursuer launches a dive and finishes the tackle; a lateral escape makes it miss',()=>{
  const c=game(),q=c.q,r=arrangeCarrier(c),d=q.state().defenders[0];
  d.mesh.position.set(0,0,11.9);d.heading.set(0,0,-1);d.velocity.set(0,0,-7.4);d.maxSpeed=7.4;
  assert.ok(q.startDivingTackle(d,r));assert.ok(d.diveTime>0);
  for(let i=0;i<30&&q.state().playState==='run';i++)q.run('gameTime+=1000/60;updateRunAfterCatch(1/60,gameTime)');
  assert.equal(q.state().playState,'tackle');assert.ok(d.finishedDive);
  const r2=arrangeCarrier(c),d2=q.state().defenders[0];d2.mesh.position.set(0,0,11.9);d2.heading.set(0,0,-1);d2.velocity.set(0,0,-7.4);d2.maxSpeed=7.4;
  assert.ok(q.startDivingTackle(d2,r2));const launched=d2.velocity.clone();r2.mesh.position.x=4;
  for(let i=0;i<18;i++)q.updateDefender(d2,new THREE.Vector3(40,0,0),1/60);
  assert.ok(d2.diveRecovery>0);assert.equal(d2.mesh.position.x,0,'committed dive cannot home sideways');
  assert.ok(d2.tackleCooldown>0);assert.equal(q.startDivingTackle(d2,r2),false);assert.equal(q.tackleContact(d2,r2),null);
  assert.ok(launched.length()>d2.maxSpeed,'brief launch impulse, not a permanent pursuit boost');
});
test('one hesitation cannot freeze an entire group; support remains able to tackle',()=>{
  const c=game(),q=c.q,r=arrangeCarrier(c);r.jukeCooldown=0;r.profile.tricks=100;r.profile.evasion=100;c.Math.random=()=>0;
  q.state().defenders.slice(0,4).forEach((d,i)=>{d.mesh.position.set((i-1.5)*.5,0,7);d.velocity.set(0,0,5);});
  assert.ok(q.tryJuke(r,true));const affected=q.state().defenders.filter(d=>d.fakeUntil>q.run('gameTime'));
  assert.equal(affected.length,1);const support=q.state().defenders.find(d=>!affected.includes(d));
  support.mesh.position.copy(r.mesh.position).add(new THREE.Vector3(.5,0,0));assert.ok(q.tackleContact(support,r));
});
test('an open goal lane takes a wide receiver straight in while a blocker still triggers avoidance',()=>{
  const c=game(),q=c.q,r=arrangeCarrier(c,20,-54);
  for(const d of q.state().defenders)d.mesh.position.set(18,0,-49);
  assert.ok(q.clearGoalLane(r));
  for(let i=0;i<30;i++){q.run('gameTime+=1000/60');q.updateBallCarrier(r,1/60);}
  assert.ok(Math.abs(r.mesh.position.x-20)<.01);assert.ok(r.mesh.position.z<-57.5);
  const d=q.state().defenders[0];d.mesh.position.set(20,0,r.mesh.position.z-1);assert.equal(q.clearGoalLane(r),false);
});
test('tackle before the goal line wins over a later crossing in the same frame',()=>{
  const c=game(),q=c.q,r=arrangeCarrier(c,0,-59.9),d=q.state().defenders[0];
  d.mesh.position.set(0,0,-59);d.heading.set(0,0,-1);d.velocity.set(0,0,-8);
  q.run('gameTime+=35;updateRunAfterCatch(.035,gameTime)');
  assert.equal(q.state().playState,'tackle');assert.ok(r.mesh.position.z>-60);
});
test('short catch plus long YAC qualifies, replay retains snap and finish beyond the old 12-second limit',()=>{
  const c=game(),q=c.q,r=arrangeCarrier(c,0,36);
  q.run("playState='live';ballLive=false;ballCarrier=null;replayFrames=[];replayRecording=true;gameTime=1000;captureReplay(true)");
  q.throwBall();q.ball.position.set(0,1.4,35.5);r.placement=null;q.resolveCatch(r,false);
  assert.equal(q.run('replayEligible'),false,'short catch alone is not a big play');
  for(let i=1;i<=1800;i++){
    r.mesh.position.z=36-26*i/1800;
    q.run('gameTime+=1000/30;attachBallToCarrier();captureReplay(false,1/30)');
  }
  const totalBefore=q.run('replayFrames.length+replayPool.length');assert.ok(totalBefore<=360);
  q.finishPlayAtSpot(10);q.state().transition.fn();const replay=q.state().replay;
  assert.ok(replay);assert.equal(replay.frames[0].time,1);assert.ok(replay.frames.at(-1).time>60);
  assert.equal(replay.frames.at(-1).focus.z,10);assert.ok(replay.frames.length<=360);
  const cash=q.state().franchise.cash,down=q.run('down');q.updateReplay((replay.frames.at(-1).time-1)/.72);
  assert.ok(q.state().replay,'last frame is displayed before ending');assert.ok(Math.abs(r.mesh.position.z-10)<.001);
  q.updateReplay(1);assert.equal(q.state().replay,null);assert.equal(q.state().franchise.cash,cash);assert.equal(q.run('down'),down);
});
test('touchdowns qualify from any distance, final tackle pose is recorded, and skip continues only once',()=>{
  const c=game(),q=c.q,r=arrangeCarrier(c,0,-59);
  q.run('replayRecording=true;captureReplay(true);gameTime+=100;');r.mesh.position.z=-60;q.finishPlayAtSpot(-60);
  q.state().transition.fn();assert.ok(q.state().replay);q.finishReplay();
  const r2=arrangeCarrier(c,0,38),d=q.state().defenders[0];q.run('replayRecording=true;captureReplay(true)');q.triggerTackle(d);
  q.run('gameTime+=720');q.updateTackle(.72);
  const frames=q.run('replayFrames');assert.ok(frames.length>=2);assert.equal(frames.at(-1).phase,'tackle');
  let calls=0;const cash=q.state().franchise.cash;q.startReplay(frames,()=>calls++);q.finishReplay();q.finishReplay();
  assert.equal(calls,1);assert.equal(q.state().franchise.cash,cash);assert.ok(Math.abs(r2.mesh.userData.visualRig.rotation.x)>.9,'jointed fall is captured on the visual rig');
});

// Contact regression fixtures use actual meshes, transforms and the same collision code as gameplay.
function contactScene(c){
  const q=c.q;q.setupPlay();q.run("playState='thrown';ballLive=true;spiralQuality=1;contactStep=1/120;previousBallAxis.set(0,0,1)");
  const r=q.state().receivers[0];
  for(const a of [...q.state().receivers,...q.state().defenders])a.mesh.position.set(90,0,90);
  r.mesh.position.set(0,0,10);r.mesh.rotation.set(0,0,0);r.mesh.scale.set(1,1,1);
  r.velocity.set(0,0,0);r.heading.set(0,0,1);r.trackingBall=true;
  q.animatePlayerContact(r,0);q.ball.quaternion.identity();q.ball.userData.spin.rotation.set(0,0,0);
  q.ballVel.set(0,0,-20);return r;
}
function ballAtHand(q,r,index=0){
  r.mesh.updateMatrixWorld(true);q.ball.position.copy(r.mesh.userData.hands[index].getWorldPosition(new THREE.Vector3()));
  q.ballPrev.copy(q.ball.position);q.sampleContactRig(r,true);
}
test('chest, helmet and nearby air cannot catch, tip or stop a pass',()=>{
  const c=game(),q=c.q,r=contactScene(c);c.Math.random=()=>0;
  for(const [x,y,z] of [[0,1.34,10],[0,1.9,10],[1.05,1.2,10]]){
    q.ball.position.set(x,y,z);q.ballPrev.copy(q.ball.position);const velocity=q.ballVel.clone();
    assert.equal(q.checkBallContact(),false);assert.equal(q.run('pendingCatch'),null);
    assert.equal(q.state().ballLive,true);assert.ok(q.ballVel.equals(velocity));
  }
});
test('a glove contact needs a brief control window before awarding a catch',()=>{
  const c=game(),q=c.q,r=contactScene(c);c.Math.random=()=>0;ballAtHand(q,r);
  assert.ok(q.checkBallContact());assert.equal(q.state().playState,'thrown');assert.equal(r.hasBall,false);
  assert.equal(q.run('pendingCatch.actor'),r);q.finishSecuringCatch(.08);
  assert.equal(q.state().playState,'run');assert.equal(r.hasBall,true);assert.equal(q.run('catches'),1);
});
test('fast passes sweep through hands even when neither ball endpoint overlaps',()=>{
  const c=game(),q=c.q,r=contactScene(c);c.Math.random=()=>0;ballAtHand(q,r);
  q.ballPrev.z+=2;q.ball.position.z-=2;q.ballVel.set(0,0,-80);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch.actor'),r);
  assert.ok(q.ball.position.z>9.5,'stop at the first physical contact, not the end of the step');
});
test('moving arms can touch a stationary ball between frame endpoints',()=>{
  const c=game(),q=c.q,r=contactScene(c);r.trackingBall=false;ballAtHand(q,r);
  r.mesh.position.x-=1;q.sampleContactRig(r,true);r.mesh.position.x+=2;q.ballVel.set(0,0,0);
  assert.ok(q.checkBallContact());assert.equal(q.state().ballLive,true);assert.ok(q.ballVel.length()>0);
});
test('forearm impacts rebound according to the impact side and stay live',()=>{
  for(const side of [-1,1]){
    const c=game(),q=c.q,r=contactScene(c);r.trackingBall=false;q.sampleContactRig(r,true);
    const arm=r.mesh.userData.arms[0];arm.getWorldPosition(q.ball.position);q.ball.position.x+=side*.20;
    q.ballPrev.copy(q.ball.position);q.ballVel.set(-side*15,0,0);
    const hit=q.playerBallContacts(r).sort((a,b)=>a.time-b.time)[0];assert.ok(hit);const approach=q.ballVel.dot(hit.normal);
    assert.ok(q.checkBallContact());assert.equal(q.state().ballLive,true);assert.equal(r.hasBall,false);
    assert.ok(approach<0&&q.ballVel.dot(hit.normal)>0,'normal component should rebound away from the contacted surface');
    const after=q.ballVel.clone();assert.equal(q.checkBallContact(),false,'overlap cannot repeatedly add impulses');assert.ok(q.ballVel.equals(after));
  }
});
test('receiver/defender contact order follows time of impact, not array or team priority',()=>{
  for(const defenseFirst of [false,true]){
    const c=game(),q=c.q,r=contactScene(c),d=q.state().defenders[0];c.Math.random=()=>0;
    d.mesh.position.copy(r.mesh.position);d.mesh.scale.copy(r.mesh.scale);d.mesh.rotation.copy(r.mesh.rotation);d.ballSeen=true;
    q.animatePlayerContact(d,0);r.mesh.position.z=defenseFirst?9:11;d.mesh.position.z=defenseFirst?11:9;
    // Equal resting poses put one glove on each side of the flight segment.
    r.mesh.userData.hands[0].position.set(-.53,1.05,.1);d.mesh.userData.hands[0].position.copy(r.mesh.userData.hands[0].position);
    q.sampleContactRig(r,true);q.sampleContactRig(d,true);
    q.run('contactStep=.3');q.ballPrev.set(-.53,1.05,13);q.ball.position.set(-.53,1.05,7);
    assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch.actor'),defenseFirst?d:r);
  }
});
test('simultaneous competing hands cause a live contested tip; proximity alone does not',()=>{
  const c=game(),q=c.q,r=contactScene(c),d=q.state().defenders[0];c.Math.random=()=>.999;
  ballAtHand(q,r);d.mesh.position.copy(r.mesh.position);d.mesh.scale.copy(r.mesh.scale);d.mesh.rotation.copy(r.mesh.rotation);d.ballSeen=true;
  d.mesh.userData.hands[0].position.copy(r.mesh.userData.hands[0].position);q.sampleContactRig(d,true);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch'),null);assert.equal(q.state().ballLive,true);
  c.Math.random=()=>0;const r2=contactScene(c),d2=q.state().defenders[0];d2.mesh.position.set(0,0,11.2);ballAtHand(q,r2);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch.actor'),r2);
});
test('another actual hand contact breaks a provisional catch and can be intercepted after separation',()=>{
  const c=game(),q=c.q,r=contactScene(c),d=q.state().defenders[0];c.Math.random=()=>0;ballAtHand(q,r);
  assert.ok(q.checkBallContact());d.mesh.position.copy(r.mesh.position);d.mesh.scale.copy(r.mesh.scale);d.mesh.rotation.copy(r.mesh.rotation);d.ballSeen=true;
  d.mesh.userData.hands[0].position.copy(r.mesh.userData.hands[0].position);q.sampleContactRig(d,true);q.ballPrev.copy(q.ball.position);c.Math.random=()=>.999;
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch'),null);assert.equal(q.state().ballLive,true);assert.equal(r.hasBall,false);
  r.mesh.position.set(90,0,90);q.ball.position.set(0,5,0);q.ballPrev.copy(q.ball.position);q.playerBallContacts(d);
  c.Math.random=()=>0;ballAtHand(q,d);assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch.actor'),d);
  const before=q.run('seriesDefense');q.finishSecuringCatch(.08);assert.equal(q.run('seriesDefense'),before+1);
});
test('low dives cannot award possession until a rendered glove touches the football',()=>{
  const c=game(),q=c.q,r=contactScene(c);c.Math.random=()=>0;r.catchDive={time:.15,direction:new THREE.Vector3(0,0,1)};
  q.ball.position.set(1.6,.25,10);q.ballPrev.copy(q.ball.position);assert.equal(q.checkBallContact(),false);
  ballAtHand(q,r);assert.ok(q.checkBallContact());q.finishSecuringCatch(.08);
  assert.equal(q.state().playState,'divecatch');assert.equal(r.hasBall,true);
});
test('rotated and scaled rigs still collide at the rendered glove',()=>{
  const c=game(),q=c.q,r=contactScene(c);c.Math.random=()=>0;r.mesh.scale.set(1.1,1.25,.95);r.mesh.rotation.set(.2,1.1,.1);r.jumpY=.8;r.mesh.position.y=.8;
  ballAtHand(q,r);assert.ok(q.checkBallContact());q.finishSecuringCatch(.08);assert.equal(q.state().playState,'run');
});
test('athleticism changes real jump height, high-point timing and landing recovery',()=>{
  const c=game(),q=c.q,r=contactScene(c);const peaks=[];
  for(const ath of [10,100]){
    r.profile.athleticism=ath;r.jumpY=0;r.jumpVel=0;r.jumpCooldown=0;let peak=0;
    q.updateJump(r,1/120,true);for(let i=0;i<150;i++){q.updateJump(r,1/120);peak=Math.max(peak,r.jumpY);}peaks.push(peak);
    assert.equal(r.jumpY,0);assert.equal(r.jumpVel,0);
  }
  assert.ok(peaks[1]>peaks[0]*1.6);
  r.profile.athleticism=100;r.jumpCooldown=0;r.velocity.set(0,0,0);q.ball.position.set(0,4.3,14.99);q.ballVel.set(0,0,-10);
  assert.ok(q.wantsHighPoint(r),'launch before the pass reaches the receiver');
  r.profile.athleticism=10;assert.equal(q.wantsHighPoint(r),false,'shorter jump cannot reach that pass');
  r.jumpY=.01;r.jumpVel=-2;q.updateJump(r,.02);assert.equal(r.landingPose,1);
});
test('animated elbows remain attached to bounded arms and replay restores every joint',()=>{
  const c=game(),q=c.q,r=contactScene(c);q.ball.position.set(.7,2.8,10.4);
  for(let i=0;i<30;i++)q.animatePlayerContact(r,1/60);
  const {hands,arms,forearms}=r.mesh.userData;
  for(let i=0;i<2;i++){
    const shoulder=new THREE.Vector3(i?.48:-.48,1.48,.02);
    assert.ok(hands[i].position.distanceTo(shoulder)<=.901);
    const upperEnd=new THREE.Vector3(0,.22,0).applyQuaternion(arms[i].quaternion).add(arms[i].position);
    const lowerStart=new THREE.Vector3(0,-.23,0).applyQuaternion(forearms[i].quaternion).add(forearms[i].position);
    assert.ok(upperEnd.distanceTo(lowerStart)<1e-6);
  }
  const saved=forearms[0].quaternion.clone();q.run('replayRecording=true;captureReplay(true);gameTime+=100');
  q.ball.position.set(-.6,1,10.3);q.animatePlayerContact(r,.1);q.captureReplay(true);const before=forearms[0].quaternion.clone();
  q.startReplay(q.state().replayFrames,()=>{});q.updateReplay(0);assert.ok(forearms[0].quaternion.angleTo(saved)<.001);
  q.finishReplay();assert.ok(forearms[0].quaternion.angleTo(before)<.001);
});

test('ordinary moving-receiver passes complete through real hand tracking at 30, 60 and 120 Hz',()=>{
  for(const hz of [30,60,120]){
    const c=game(),q=c.q,r=contactScene(c);c.Math.random=()=>.1;
    r.heading.set(0,0,-1);r.mesh.rotation.y=Math.PI;r.velocity.set(0,0,-8);r.maxSpeed=8;
    r.start.copy(r.mesh.position);r.path=[r.mesh.position.clone(),new THREE.Vector3(0,0,-70)];r.distance=0;r.runIntensity=.8;
    q.ball.position.set(0,2.25,25);q.ballPrev.copy(q.ball.position);q.ballVel.set(0,(1.6-2.25+4.905*.36)/.6,(-8*.6-15)/.6);
    let ticks=0;while(q.state().ballLive&&ticks<hz*2){ticks++;q.run(`gameTime+=${1000/hz}`);q.update(1/hz,ticks*1000/hz);}
    assert.equal(q.state().playState,'run',`${hz} Hz: ${q.state().playState}, tips ${q.run('throwBobbles')}, y ${q.ball.position.y}`);
    assert.equal(r.hasBall,true);
  }
});
test('ground contact ends the play before a later touch and resets provisional possession',()=>{
  const c=game(),q=c.q;contactScene(c);
  for(const a of [...q.state().receivers,...q.state().defenders])a.mesh.position.set(90,0,90);
  q.ball.position.set(0,.35,0);q.ballPrev.copy(q.ball.position);q.ballVel.set(0,-50,0);
  q.update(1/30,33);assert.equal(q.state().ballLive,false);assert.equal(q.run('pendingCatch'),null);assert.equal(q.state().playState,'dead');
});

test('a high pass is attacked in flight and caught by elevated hands',()=>{
  const c=game(),q=c.q,r=contactScene(c);c.Math.random=()=>.1;r.profile.athleticism=100;r.profile.catching=100;
  r.heading.set(0,0,-1);r.mesh.rotation.y=Math.PI;r.velocity.set(0,0,-8);r.maxSpeed=8;
  r.start.copy(r.mesh.position);r.path=[r.mesh.position.clone(),new THREE.Vector3(0,0,-70)];r.distance=0;r.runIntensity=.8;
  q.ball.position.set(0,2.5,25);q.ballPrev.copy(q.ball.position);q.ballVel.set(0,(3.1-2.5+4.905*.65*.65)/.65,(-8*.65-15)/.65);
  let peak=0,ticks=0;while(q.state().ballLive&&ticks<180){ticks++;q.run('gameTime+=1000/60');q.update(1/60,ticks*1000/60);peak=Math.max(peak,r.jumpY);}
  assert.ok(peak>.65,'jump has to happen before the catch');assert.equal(q.state().playState,'run');assert.equal(r.hasBall,true);
  assert.ok(q.ball.position.y>2.4,'the hands secure the pass above standing height');
});

test('a ball already on the turf cannot be rescued by overlapping hands',()=>{
  const c=game(),q=c.q,r=contactScene(c);c.Math.random=()=>0;
  r.mesh.position.y=-.9;ballAtHand(q,r);q.ball.position.y=.1;q.ballPrev.copy(q.ball.position);q.ballVel.set(0,-1,0);
  q.update(1/120,9);assert.equal(q.state().ballLive,false);assert.equal(r.hasBall,false);
});

function effortScene(c,z=-7.4,goal=false){
  const q=c.q,r=arrangeCarrier(c,0,z),d=q.state().defenders[0];
  q.run(`lineToGainYards=${goal?50:25};ballSpotYards=${goal?45:20};snapSpotYards=ballSpotYards`);
  Object.assign(r.profile,{athleticism:95,speed:90,strength:85,turning:90,evasion:90});r.catchStyleTime=0;r.securedHands=null;r.jukeAnim=0;r.mesh.rotation.y=Math.PI;
  r.mesh.scale.set(1,1,1);r.velocity.set(0,0,-8);r.heading.set(0,0,-1);q.animatePlayerContact(r,0);q.run('attachBallToCarrier()');
  d.mesh.position.set(0,0,z+1.7);d.velocity.set(0,0,-10);d.maxSpeed=10;d.heading.set(0,0,-1);d.technique=.5;d.fakeUntil=0;d.diveRecovery=0;
  return {r,d};
}
function finishMotion(q,hz=60){let ticks=0;while(q.state().playState==='tackle'&&ticks<hz*3){ticks++;q.run(`gameTime+=${1000/hz}`);q.updateTackle(1/hz);}assert.ok(ticks<hz*3,'finish animation must terminate');}

test('effort dives require a useful nearby marker, athleticism, forward speed and imminent contact',()=>{
  const c=game(),q=c.q,{r,d}=effortScene(c);
  assert.ok(q.effortDivePlan(r));d.mesh.position.z+=10;assert.equal(q.effortDivePlan(r),null);d.mesh.position.z-=10;
  r.mesh.position.z=5;assert.equal(q.effortDivePlan(r),null);r.mesh.position.z=-10.5;assert.equal(q.effortDivePlan(r),null);r.mesh.position.z=-7.4;
  r.profile.athleticism=50;assert.equal(q.effortDivePlan(r),null);r.profile.athleticism=95;
  r.velocity.z=-2;assert.equal(q.effortDivePlan(r),null);r.velocity.z=-8;
  r.mesh.position.x=25;assert.equal(q.effortDivePlan(r),null);r.mesh.position.x=0;
  d.mesh.position.z=r.mesh.position.z+.8;assert.equal(q.effortDivePlan(r),null,'no late dive after contact is already established');
});
test('failed dive decisions are not retried every frame; a fresh play resets the opportunity',()=>{
  const c=game(),q=c.q,{r}=effortScene(c);let rolls=0;c.Math.random=()=>{rolls++;return .99;};
  assert.equal(q.tryEffortDive(r),false);for(let i=0;i<120;i++)assert.equal(q.tryEffortDive(r),false);assert.equal(rolls,1);
  q.setupPlay();assert.equal(q.state().receivers[0].effortConsidered,undefined);
});
test('a dive commits its direction and extends the held ball through an attainable first down',()=>{
  const c=game(),q=c.q,{r,d}=effortScene(c);c.Math.random=()=>0;assert.ok(q.tryEffortDive(r));
  const launchZ=r.mesh.position.z;d.mesh.position.set(90,0,90);q.updateTackle(.22);
  assert.ok(r.mesh.position.z<launchZ-1);assert.ok(r.finishPose.extend>.9);assert.ok(q.ball.position.z<r.mesh.position.z-.8);
  assert.ok(Math.abs(r.mesh.position.x)<.001);finishMotion(q);
  assert.equal(q.run('down'),1);assert.equal(q.run('lineToGainYards'),50);assert.ok(q.run('ballSpotYards')>=25);
});
test('goal dives work after already passing the first-down marker, and score only once after landing',()=>{
  const c=game(),q=c.q,{r,d}=effortScene(c,-57.7);q.run('ballSpotYards=40;snapSpotYards=40');
  assert.equal(q.effortDivePlan(r).goal,true);c.Math.random=()=>0;assert.ok(q.tryEffortDive(r));d.mesh.position.set(90,0,90);
  const before=q.run('seriesOffense');let crossed=false;
  for(let i=0;i<90&&q.state().playState==='tackle';i++){
    q.run('gameTime+=1000/120');q.updateTackle(1/120);
    if(r.finishMotion.result==='TOUCHDOWN'){crossed=true;if(q.state().playState==='tackle')assert.equal(q.run('seriesOffense'),before);}
  }
  assert.ok(crossed);finishMotion(q);assert.equal(q.run('seriesOffense'),before+1);q.updateTackle(.5);assert.equal(q.run('seriesOffense'),before+1);
});
test('a head-on defender can stop an airborne dive short of the marker',()=>{
  const c=game(),q=c.q,{r,d}=effortScene(c,-7.0);c.Math.random=()=>0;assert.ok(q.tryEffortDive(r));
  d.mesh.position.set(0,0,-7.9);d.heading.set(0,0,1);d.velocity.set(0,0,11);d.strength=100;d.diveTime=0;d.tackleCooldown=10;
  q.updateTackle(1/120);assert.equal(r.finishMotion.freeDive,false);assert.equal(q.run('tackler'),d);
  finishMotion(q);assert.ok(q.run('ballSpotYards')<25);assert.equal(q.run('down'),2);
});
test('tackle poses follow contact angle and speed; fast carriers retain bounded momentum',()=>{
  const outcomes=[];
  for(const kind of ['drag','side','hit','trip']){
    const c=game(),q=c.q,r=arrangeCarrier(c),d=q.state().defenders[0];r.mesh.rotation.y=Math.PI;r.catchStyleTime=0;
    d.mesh.position.copy(r.mesh.position).add(new THREE.Vector3(kind==='side'?.95:0,0,kind==='side'?0:kind==='drag'||kind==='trip'?1:-1));
    d.velocity.set(0,0,kind==='hit'?10:-7);d.divingThisStep=kind==='trip';q.run('attachBallToCarrier()');q.triggerTackle(d);
    assert.equal(r.finishMotion.kind,kind);const z=r.mesh.position.z;q.updateTackle(.18);outcomes.push([kind,r.finishPose.pitch,r.finishPose.roll]);
    if(kind==='drag')assert.ok(r.mesh.position.z<z-.2,'momentum carries the receiver through contact');
    assert.ok(r.mesh.position.distanceTo(r.tacklePrevious)<.2);assert.ok(r.finishMotion.velocity.length()<=7);
    assert.equal(d.finishPose.wrapTarget,r);
  }
  assert.ok(Math.abs(outcomes[1][2])>Math.abs(outcomes[0][2]),'side contact rolls the body');
  assert.ok(outcomes[2][1]<0,'head-on impact can knock the receiver backward');
});
test('the down spot is frozen before rolling/sliding, without a free yard for falling',()=>{
  const c=game(),q=c.q,r=arrangeCarrier(c,0,-.2),d=q.state().defenders[0];q.run('ballSpotYards=20;snapSpotYards=20;lineToGainYards=25');
  r.velocity.set(0,0,0);r.mesh.rotation.y=Math.PI;r.catchStyleTime=0;q.animatePlayerContact(r,0);q.run('attachBallToCarrier()');
  d.mesh.position.set(0,0,-1);d.velocity.set(0,0,0);q.triggerTackle(d);q.updateTackle(.4);
  assert.ok(r.finishMotion.down);const spot=r.finishMotion.spotZ;const position=r.mesh.position.clone();
  q.updateTackle(.15);assert.equal(r.finishMotion.spotZ,spot);finishMotion(q);
  assert.ok(q.run('ballSpotYards')<21,'no old minimum-one-yard spot bonus');assert.ok(Math.abs(q.run('ballSpotYards')-(40-spot)/2)<1e-6);
});
test('fall/dive outcomes are stable across 30, 60 and 120 Hz and shared joint geometry stays cached',()=>{
  const spots=[];
  for(const hz of [30,60,120]){
    const c=game(),q=c.q,{r,d}=effortScene(c);c.Math.random=()=>0;const geo=r.mesh.userData.arms[0].geometry;
    assert.ok(q.tryEffortDive(r));d.mesh.position.set(90,0,90);finishMotion(q,hz);spots.push(q.run('ballSpotYards'));
    assert.equal(r.mesh.userData.arms[0].geometry,geo);assert.ok(Number.isFinite(r.mesh.position.length()));
  }
  assert.ok(Math.max(...spots)-Math.min(...spots)<.03);
});
test('ball contact/catch rules survive finish-play animation and replay captures the final landing',()=>{
  const c=game(),q=c.q,{r,d}=effortScene(c);c.Math.random=()=>0;
  q.run('replayRecording=true;captureReplay(true)');assert.ok(q.tryEffortDive(r));d.mesh.position.set(90,0,90);
  while(q.state().playState==='tackle'){q.run('gameTime+=1000/60');q.updateTackle(1/60);q.captureReplay(false,1/60);}
  q.state().transition.fn();assert.ok(q.state().replay);const last=q.state().replay.frames.at(-1);assert.equal(last.phase,'tackle');
  const pose=r.mesh.userData.visualRig.quaternion.clone(),cash=q.state().franchise.cash,down=q.run('down');
  q.updateReplay(.2);q.finishReplay();q.finishReplay();assert.equal(q.state().franchise.cash,cash);assert.equal(q.run('down'),down);
  assert.equal(q.state().playState,'call');
});

test('stepping out before a goal-line reach ends the play short, without a touchdown',()=>{
  const c=game(),q=c.q,{r,d}=effortScene(c,-58.8,true);c.Math.random=()=>0;assert.ok(q.tryEffortDive(r));
  d.mesh.position.set(90,0,90);r.mesh.position.x=25.58;r.finishMotion.velocity.set(8,0,-2);q.run('attachBallToCarrier()');
  const score=q.run('seriesOffense');q.updateTackle(1/60);
  assert.equal(r.finishMotion.result,'OUT OF BOUNDS');const spot=r.finishMotion.spotZ;assert.ok(spot>-60);
  finishMotion(q);assert.equal(q.run('seriesOffense'),score);assert.ok(q.run('ballSpotYards')<50);
});
test('no extension after the down event can turn a failed reach into a first down',()=>{
  const c=game(),q=c.q,{r,d}=effortScene(c);c.Math.random=()=>0;assert.ok(q.tryEffortDive(r));d.mesh.position.set(90,0,90);
  r.finishMotion.velocity.set(0,0,0);r.finishMotion.downAt=.025;r.finishMotion.duration=.35;
  q.updateTackle(.05);assert.ok(r.finishMotion.down);const spot=r.finishMotion.spotZ;assert.ok(spot>-10);
  q.updateTackle(.2);assert.equal(r.finishMotion.spotZ,spot);finishMotion(q);assert.equal(q.run('down'),2);assert.ok(q.run('ballSpotYards')<25);
});
test('ratings influence effort frequency and resisted momentum without new save attributes',()=>{
  const c=game(),q=c.q,{r,d}=effortScene(c);const high=q.effortDivePlan(r).chance;
  Object.assign(r.profile,{athleticism:65,strength:30,turning:30,evasion:30});assert.ok(q.effortDivePlan(r).chance<high-.2);
  const speeds=[];
  for(const strength of [20,100]){
    const r2=arrangeCarrier(c),d2=q.state().defenders[0];r2.profile.strength=strength;r2.profile.turning=70;r2.profile.evasion=70;r2.profile.size=50;
    d2.mesh.position.copy(r2.mesh.position).add(new THREE.Vector3(0,0,1));d2.velocity.set(0,0,-7);d2.strength=70;
    q.triggerTackle(d2);speeds.push(r2.finishMotion.velocity.length());
  }
  assert.ok(speeds[1]>speeds[0]);const saved=P.validateSave(JSON.stringify(q.state().franchise));assert.equal(saved.team.length,4);
});
test('new falls keep limbs above turf, football at the hands, and geometry cached',()=>{
  const c=game(),q=c.q,{r,d}=effortScene(c);c.Math.random=()=>0;assert.ok(q.tryEffortDive(r));d.mesh.position.set(90,0,90);
  const geometryCount=q.run('playerGeometryCache.size');
  for(let i=0;i<65&&q.state().playState==='tackle';i++){
    q.updateTackle(1/120);r.mesh.updateMatrixWorld(true);
    const hand=r.mesh.userData.hands.map(h=>h.getWorldPosition(new THREE.Vector3()));
    assert.ok(q.ball.position.distanceTo(hand[0].clone().lerp(hand[1],.5))<.01,'extension stays between the gloves');
    for(const foot of r.mesh.userData.feet)assert.ok(foot.getWorldPosition(new THREE.Vector3()).y>0);
  }
  assert.equal(q.run('playerGeometryCache.size'),geometryCount);
});

test('saved market discounts are stable and signing charges the displayed reduced price',()=>{
  const {q}=game();
  q.run(`for(const k of P.stats)franchise.market[0][k]=100;franchise.market[0].price=13000;
    localStorage.setItem(SAVE_KEY,JSON.stringify(franchise));franchise=loadFranchise();`);
  const id=q.state().franchise.market[0].id;
  assert.equal(q.state().franchise.market[0].price,8950);
  q.run('activateFranchise(franchise,0);activateFranchise(franchise,0)');
  assert.equal(q.state().franchise.market[0].price,8950);
  q.run('franchise.cash=10000;selectedRosterIndex=0;signReceiver(0)');
  assert.equal(q.state().franchise.cash,1050);assert.equal(q.state().franchise.team[0].id,id);
  q.run('franchise.market[0].price=140;normalizeMarketReceiver(franchise.market[0])');
  assert.equal(q.state().franchise.market[0].price,140);
});

test('expanded concepts have finite routes, distinct alignments and bounded motion',()=>{
  const q=game().q;
  assert.equal(q.run('plays.length'),72);
  for(let i=8;i<72;i++)for(const spot of [0,46]){
    q.run(`selectedPlay=${i};ballSpotYards=${spot};setupPlay(false);beginCountdown();gameTime=snapTime;update(.016,gameTime)`);
    for(const r of q.state().receivers){assert.ok(r.path.every(p=>Number.isFinite(p.length())&&Math.abs(p.x)<=24.7));}
    if(i===16)assert.equal(q.state().receivers[1].start.x,4);
    if(i===17)assert.equal(q.state().receivers[2].start.x,-4);
  }
});
test('screens settle behind the line and block strength produces brief, bounded leverage',()=>{
  const q=game().q;
  q.run("selectedPlay=14;setupPlay(false);playState='live';");
  for(let i=0;i<150;i++)q.run('gameTime+=16;updateReceiver(receivers[1],.016,gameTime)');
  const r=q.state().receivers[1];assert.ok(r.mesh.position.z>r.start.z);
  const weak=V.blockOutcome({strength:30,size:30,defenseStrength:90,defenseSize:85,alignment:1,momentum:0});
  const strong=V.blockOutcome({strength:95,size:95,defenseStrength:60,defenseSize:55,alignment:1,momentum:2});
  assert.ok(strong.duration>weak.duration&&strong.slow<weak.slow);assert.ok(strong.duration<=.85&&weak.duration>=.12);
});
test('blocking requires contact, expires and cannot immediately re-lock a defender',()=>{
  const q=game().q;q.run(`setupPlay(false);ballCarrier=receivers[0];playState='run';
    receivers.forEach((r,i)=>r.mesh.position.set(i*10,0,0));defenders.forEach(d=>d.mesh.position.set(20,0,-30));
    receivers[1].mesh.position.set(0,0,-2);receivers[1].heading.set(0,0,-1);defenders[0].mesh.position.set(0,0,-2.9);defenders[0].heading.set(0,0,1);
    updateBlocking(.016);`);
  const d=q.state().defenders[0];assert.ok(d.blockTime>0);assert.ok(d.blockCooldown>d.blockTime);
  q.run('updateBlocking(1)');assert.equal(d.blockTime,0);assert.ok(d.blockCooldown>0);
  q.run('updateBlocking(.016)');assert.equal(d.blockTime,0);
});
test('rating physique controls the rendered dimensions independently of cosmetic saved build',()=>{
  const q=game().q;
  q.run('franchise.team[0].size=20;franchise.team[0].strength=20;franchise.team[1].size=95;franchise.team[1].strength=95;setupPlay(false)');
  const [small,big]=q.state().receivers;assert.ok(big.mesh.scale.x>small.mesh.scale.x*1.2);assert.ok(big.mesh.scale.y>small.mesh.scale.y);
});

test('ordinary plays remain rewatchable after next snap setup without advancing franchise state',()=>{
  const q=game().q;
  q.run("replayEligible=false;replayRecording=true;gameTime=1000;captureReplay(true);gameTime=2000;receivers[0].mesh.position.z-=5;captureReplay(true);scheduleResult(()=>setupPlay(true),0);const next=transition.fn;transition=null;next();");
  assert.equal(q.run('canWatchReplay()'),true);
  const before=q.run('JSON.stringify(franchise)'),clock=q.run('gameTime'),pos=q.state().receivers[0].mesh.position.clone();
  const archive=q.run('lastReplay');assert.ok(archive.frames.every(f=>f.objects.every(Boolean)));
  assert.notEqual(archive.frames[0].objects[0],q.state().receivers[0].mesh);
  for(let i=0;i<3;i++){
    q.run('watchLastReplay()');assert.ok(q.state().replay);q.updateReplay(.4);q.finishReplay();
    assert.equal(q.run('JSON.stringify(franchise)'),before);assert.equal(q.run('gameTime'),clock);
    assert.ok(q.state().receivers[0].mesh.position.distanceTo(pos)<.00001);assert.equal(archive.group.visible,false);
  }
});
test('replay cameras cycle QB, sideline and angled overhead without moving simulation',()=>{
  const q=game().q;q.run('replayRecording=true;captureReplay(true);gameTime+=1000;captureReplay(true);startReplay(replayFrames,()=>{});updateReplay(0)');
  const qb=q.camera.position.clone(),state=q.state().receivers[0].mesh.position.clone();
  q.run('cycleReplayCamera(1)');assert.equal(q.state().replay.angle,'sideline');assert.equal(q.camera.position.x,32);
  q.run('cycleReplayCamera(1)');assert.equal(q.state().replay.angle,'overhead');assert.equal(q.camera.position.y,32);
  q.run('cycleReplayCamera(1)');assert.equal(q.state().replay.angle,'qb');assert.ok(q.camera.position.distanceTo(qb)<.00001);
  q.run('cycleReplayCamera(-1)');assert.equal(q.state().replay.angle,'overhead');assert.ok(q.state().receivers[0].mesh.position.distanceTo(state)<.00001);
});
test('replay archive replaces owned resources and blocks manual playback during live action',()=>{
  const q=game().q;q.run('replayRecording=true;captureReplay(true);gameTime+=1000;captureReplay(true);archiveReplay(replayFrames)');
  const old=q.run('lastReplay');let disposed=0;old.geometries.forEach(g=>g.addEventListener('dispose',()=>disposed++));
  q.run('archiveReplay(replayFrames)');assert.equal(disposed,old.geometries.length);assert.equal(old.group.parent,null);
  q.run("playState='live';watchLastReplay()");assert.equal(q.state().replay,null);
  q.run('clearLastReplay()');assert.equal(q.run('lastReplay'),null);
});

test('opponent tour has 520 unique teams and continues into additional leagues',()=>{
  const q=game().q,names=new Set();
  for(let n=1;n<=1040;n++){const o=q.run(`opponentForRound(${n})`);assert.ok(Number.isFinite(o.skill)&&o.skill>=0&&o.skill<=1);assert.ok(!names.has(o.name),o.name);names.add(o.name);}
  assert.equal(q.run('opponentForRound(1).name'),'Rookie Secondary');
});
test('rematches use original difficulty, award one fifth cash, and never advance campaign',()=>{
  const q=game().q;q.run('franchise.round=12;franchise.wins=11;franchise.cash=0;managerLocked=true;browsedOpponent=2;selectOpponent();');
  assert.equal(q.run('activeRound()'),2);assert.equal(q.run('currentSkill()'),.13);
  assert.equal(q.state().franchise.round,12);assert.ok(q.state().defenders[0].technique<.4);
  q.run('seriesOffense=3;endMatchup(true)');assert.equal(q.state().franchise.cash,P.payout(true,2,3)/5);assert.equal(q.state().franchise.round,12);
  assert.equal(q.state().franchise.rematchRound,null);assert.equal(q.state().franchise.opponentResults[2].wins,2);
  q.run('transition=null;managerLocked=true;browsedOpponent=2;selectOpponent();seriesOffense=1;endMatchup(false)');
  assert.equal(q.state().franchise.cash,(P.payout(true,2,3)+P.payout(false,2,1))/5);assert.equal(q.state().franchise.round,12);
});
test('opponent selection is blocked after snap and saved rematches restore original matchup',()=>{
  const q=game().q;q.run('franchise.round=9;managerLocked=true;browsedOpponent=3;selectOpponent();managerLocked=false;beginCountdown();');
  assert.equal(q.state().franchise.matchInProgress,true);
  q.run('browsedOpponent=1;selectOpponent()');assert.equal(q.run('activeRound()'),3);
  const loaded=P.validateSave(q.state().franchise);assert.equal(loaded.rematchRound,3);assert.equal(loaded.matchInProgress,true);
  q.activateFranchise(loaded,0);assert.equal(q.run('activeRound()'),3);assert.equal(q.run('canSelectOpponent()'),false);
});
test('legacy saves unlock earlier victories and reject invalid rematch identifiers',()=>{
  const f=P.normalizeCompetition({round:400,rematchRound:999,opponentResults:{bad:{wins:7},3:{wins:-4,losses:2}}});
  assert.equal(f.rematchRound,null);assert.equal(P.matchRound(f),400);assert.equal(f.opponentResults[3].wins,0);assert.equal(f.opponentResults.bad,undefined);
  const q=game().q;q.run('franchise.round=400;managerLocked=true;browsedOpponent=399;selectOpponent()');assert.equal(q.run('activeRound()'),399);
  q.run('browsedOpponent=400;selectOpponent();seriesOffense=3;endMatchup(true)');assert.equal(q.state().franchise.round,401);
});
test('every literal game UI reference has a real HTML element',()=>{
  const html=fs.readFileSync(__dirname+'/../receiver-window-qb.html','utf8');
  for(const [,id] of source.matchAll(/\$\('([^']+)'\)/g))assert.ok(html.includes(`id="${id}"`),`Missing #${id}`);
  for(const id of ['watchReplayBtn','watchReplayMenu','watchReplayTeam','opponentPrevious','opponentNext','selectOpponent'])assert.ok(html.includes(`id="${id}"`));
});

test('seven populated play families keep screen assignments and route interactions distinct',()=>{
  const q=game().q,plays=q.run('plays');
  for(const category of V.categories)assert.ok(plays.filter(p=>V.category(p)===category).length>=4,category);
  assert.equal(new Set(plays.map(p=>p.name)).size,plays.length);
  for(const p of plays.filter(p=>p.screen!=null)){assert.equal(p.routes.filter(r=>r==='Lead').length,2);assert.notEqual(p.routes[p.screen],'Lead');}
});
test('all new plays simulate through motion and live route movement without non-finite actors',()=>{
  const q=game().q;
  for(let i=18;i<q.run('plays.length');i++){
    q.run(`selectedPlay=${i};setupPlay(true);beginCountdown();gameTime=snapTime;update(.016,gameTime)`);
    for(let frame=0;frame<150;frame++)q.run('gameTime+=1000/60;update(1/60,gameTime)');
    for(const a of [...q.state().receivers,...q.state().defenders])assert.ok(Number.isFinite(a.mesh.position.length())&&a.velocity.length()<16,`play ${i}`);
  }
});
test('packed front coverage cannot cause multi-yard voluntary retreat at 30/60/120 Hz',()=>{
  for(const hz of [30,60,120]){
    const c=game(),q=c.q;c.Math.random=()=>.999999;
    q.run("playState='run';ballCarrier=receivers[0];receivers[0].mesh.position.set(0,0,0);receivers[0].heading.set(0,0,1);receivers[0].velocity.set(0,0,8);receivers[0].style='YAC Specialist';receivers[0].bestRunZ=0;defenders.forEach((d,i)=>{d.mesh.position.set((i-2)*1.3,0,-2);d.velocity.set(0,0,0)});");
    const r=q.state().receivers[0];let maxLoss=0;
    for(let i=0;i<hz*4;i++){q.run(`gameTime+=1000/${hz};updateBallCarrier(receivers[0],1/${hz})`);maxLoss=Math.max(maxLoss,r.mesh.position.z-r.bestRunZ);}
    assert.ok(maxLoss<=3.21,`retreated ${maxLoss/2} yards at ${hz} Hz`);assert.ok(r.mesh.position.z<0,'eventually attacks forward');
  }
});
test('lane choice prefers clear forward gaps, tighter marker progress and sideline safety',()=>{
  let r=V.lane({x:0,z:0,bestZ:0,defenders:[{x:0,z:-3}],style:'YAC Specialist',markerZ:-30});
  assert.ok(r.z<0&&Math.abs(r.x)>.1,'goes around the defender');
  r=V.lane({x:0,z:0,bestZ:-1,defenders:[{x:0,z:-1}],style:'YAC Specialist',markerZ:-2});assert.ok(r.z<=0&&r.limit===1.2);
  r=V.lane({x:24,z:0,defenders:[],style:'Power Receiver',markerZ:-30});assert.ok(r.x<=0&&r.z<0);
});
test('screen acceleration is designated, temporary and lost when the target is audibled',()=>{
  const c=game(),q=c.q;c.Math.random=()=>.999999;
  function sample(screen,age){q.run(`selectedPlay=14;setupPlay(false);playState='run';ballCarrier=receivers[1];gameTime=10000;defenders.forEach(d=>d.mesh.position.set(90,0,90));receivers[1].mesh.position.set(0,0,0);receivers[1].velocity.set(0,0,0);receivers[1].heading.set(0,0,-1);receivers[1].screenTarget=${screen};receivers[1].screenCatchAt=gameTime-${age};receivers[1].profile.cutting=80;receivers[1].maxSpeed=8;`);q.updateBallCarrier(q.state().receivers[1],.05);return q.state().receivers[1].velocity.length();}
  const regular=sample(false,0),fresh=sample(true,0),expired=sample(true,1900);assert.ok(fresh>regular*1.15);assert.ok(Math.abs(expired-regular)<1e-8);
  q.run("selectedPlay=14;setupPlay(false);audibleReceiverIndex=1;assignAudibleRoute('Go')");assert.equal(q.state().receivers[1].screenTarget,false);
});
test('blockers claim separate threats, establish carrier-side leverage and release holds',()=>{
  const q=game().q;q.run("selectedPlay=14;setupPlay(false);playState='run';ballCarrier=receivers[1];receivers.forEach((r,i)=>r.mesh.position.set(i*2,0,0));defenders.forEach((d,i)=>d.mesh.position.set(i*2,0,-4));updateBlocking(.016)");
  const blockers=q.state().receivers.filter(r=>r.blockAim);assert.ok(blockers.length>=2);assert.equal(new Set(blockers.map(r=>r.blockTarget)).size,blockers.length);
  for(const b of blockers)assert.ok(b.blockAim.z>b.blockTarget.mesh.position.z,'between runner and defender');
});
test('option routes read deep and underneath coverage once; normal routes never change',()=>{
  const c=game(),q=c.q;c.Math.random=()=>0;
  for(const [dz,expected] of [[-7,'Curl'],[2,'Go'],[-2,'Out']]){
    q.run("selectedPlay=plays.findIndex(p=>p.name==='Choice Stick');setupPlay(false);playState='live';receivers[1].distance=12;receivers[1].profile.cutting=100;receivers[1].profile.turning=100;defenders.forEach(d=>d.mesh.position.set(90,0,90))");
    const r=q.state().receivers[1],d=q.state().defenders[1];d.mesh.position.copy(r.mesh.position).add(new THREE.Vector3(1,0,dz));q.run('readOptionRoute(receivers[1])');assert.equal(r.route,expected);q.run('readOptionRoute(receivers[1])');assert.equal(r.route,expected);
    const normal=q.state().receivers[0],route=normal.route;normal.distance=20;q.run('readOptionRoute(receivers[0])');assert.equal(normal.route,route);
  }
});
test('pump cooldown and matchup memory resist repeat targets, concepts and disciplined opponents',()=>{
  const c=game(),q=c.q;c.Math.random=()=>0;q.run("playState='live';gameTime=5000");
  assert.equal(q.run('pumpFake()'),true);assert.equal(q.run('pumpFake()'),false);assert.equal(q.state().franchise.pumpMemory.length,1);
  const initial=V.pumpChance([],0,'Sluggo',.2),same=Array.from({length:6},()=>({target:0,concept:'Sluggo'}));
  assert.ok(V.pumpChance(same,0,'Sluggo',.2)<initial*.02);assert.ok(V.pumpChance([],0,'Sluggo',.95)<initial);
  assert.ok(q.state().defenders.filter(d=>d.fakeUntil>5000).length<=1);
  q.setupPlay();assert.equal(q.state().franchise.pumpMemory.length,1);q.run('endMatchup(false)');assert.equal(q.state().franchise.pumpMemory.length,0);
});
test('formation adjustments are bounded, preserve defense and cannot edit a live play',()=>{
  const q=game().q,d=q.state().currentDefense;q.run('audibleReceiverIndex=0;for(let i=0;i<8;i++)adjustFormation("wide")');assert.equal(q.state().receivers[0].start.x,-22);assert.equal(q.state().currentDefense,d);
  q.run('playState="live";adjustFormation("tight")');assert.equal(q.state().receivers[0].start.x,-22);
  q.run('selectedPlay=16;setupPlay(false);audibleReceiverIndex=1;adjustFormation("motion");beginCountdown();gameTime=snapTime;update(.016,gameTime)');assert.equal(q.state().receivers[1].start.x,2.2);
});
test('new matchup memories migrate safely and survive save round trips',()=>{
  const q=game().q;const f=q.state().franchise;f.pumpMemory=[null,{target:0,concept:'Mesh'},{target:8,concept:'invalid'}];f.conceptMemory=['Mesh',null,8];
  const save=P.validateSave(JSON.stringify(f));assert.deepEqual(save.pumpMemory,[{target:0,concept:'Mesh'}]);assert.deepEqual(save.conceptMemory,['Mesh']);
  assert.equal(V.weather('Day').cut,1);assert.ok(V.weather('Light Rain').hands<=.03);assert.ok(V.weather('Cold').contact<1.05);assert.ok(V.weather('Windy').wind<.4);
});


test('strength and athleticism scale their own moves; evasion improves timely use',()=>{
  const p={strength:30,athleticism:30,evasion:30};
  const stiff=V.escapeOdds(p,'stiff'),hurdle=V.escapeOdds(p,'hurdle');
  assert.ok(V.escapeOdds({...p,strength:95},'stiff').success>stiff.success+.4);
  assert.equal(V.escapeOdds({...p,strength:95},'hurdle').success,hurdle.success);
  assert.ok(V.escapeOdds({...p,athleticism:95},'hurdle').success>hurdle.success+.4);
  assert.ok(V.escapeOdds({...p,evasion:95},'stiff').timing>stiff.timing+.4);
  const screen=V.escapeOdds({strength:85,athleticism:85,evasion:85},'hurdle',80,.8,true);
  assert.ok(screen.timing*screen.success>.75);assert.ok(screen.success<1);
});
test('stiff arm pushes one front defender, cools down, and cannot protect against support',()=>{
  const c=game(),q=c.q;c.Math.random=()=>0;
  q.run("playState='run';ballCarrier=receivers[0];receivers[0].hasBall=true;receivers[0].mesh.position.set(0,0,0);receivers[0].heading.set(0,0,-1);receivers[0].velocity.set(0,0,-7);defenders[0].mesh.position.set(0,0,-1);defenders[1].mesh.position.set(.2,0,-.4)");
  assert.equal(q.run("attemptCarrierMove(receivers[0],defenders[0],'stiff')"),true);
  assert.ok(q.state().defenders[0].mesh.position.z<-1.8);assert.ok(q.state().receivers[0].stiffArmTime>0);
  assert.equal(q.run("attemptCarrierMove(receivers[0],defenders[1],'stiff')"),false);
  assert.ok(q.tackleContact(q.state().defenders[1],q.state().receivers[0]),'support still has contact');
  q.run('animatePlayerContact(receivers[0],.016);attachBallToCarrier()');
  assert.ok(q.ball.position.distanceTo(q.state().receivers[0].mesh.userData.hands[1].getWorldPosition(new THREE.Vector3()))<1e-8);
});
test('hurdles require a low threat, actual height at impact, and leave standing support dangerous',()=>{
  const c=game(),q=c.q;c.Math.random=()=>0;
  q.run("playState='run';ballCarrier=receivers[0];receivers[0].mesh.position.set(0,0,0);receivers[0].velocity.set(0,0,-7);defenders[0].mesh.position.set(0,0,-3)");
  assert.equal(q.run("attemptCarrierMove(receivers[0],defenders[0],'hurdle')"),false,'no leap over upright defender');
  q.run('defenders[0].diveTime=.28');assert.equal(q.run("attemptCarrierMove(receivers[0],defenders[0],'hurdle')"),true);
  q.run('updateJump(receivers[0],.14,false)');assert.ok(q.state().receivers[0].jumpY>.48);
  q.run('defenders[0].mesh.position.set(0,0,-.5);defenders[0].divingThisStep=true;receivers[0].tacklePreviousY=0');
  assert.ok(q.tackleContact(q.state().defenders[0],q.state().receivers[0]),'contact before clearance wins');
  q.run('receivers[0].tacklePreviousY=.6');assert.equal(q.tackleContact(q.state().defenders[0],q.state().receivers[0]),null);
  q.run('defenders[1].mesh.position.set(.1,0,-.5)');assert.ok(q.tackleContact(q.state().defenders[1],q.state().receivers[0]));
});
test('failed timely-use check is not retried every frame and new actors reset move state',()=>{
  const c=game(),q=c.q;let rolls=0;c.Math.random=()=>{rolls++;return .999;};
  q.run("playState='run';ballCarrier=receivers[0];receivers[0].mesh.position.set(0,0,0);defenders[0].mesh.position.set(0,0,-1);receivers[0].profile.evasion=1");
  rolls=0;for(let i=0;i<120;i++)assert.equal(q.run("attemptCarrierMove(receivers[0],defenders[0],'stiff')"),false);assert.equal(rolls,1);
  q.setupPlay();assert.equal(q.state().receivers[0].stiffReads,undefined);
});

test('new screen options read early at low ratings, preserve the outlet and freeze after the throw',()=>{
  const c=game(),q=c.q;c.Math.random=()=>0;
  q.run("selectedPlay=plays.findIndex(p=>p.name==='Read Screen Right');setupPlay(false);playState='live';defenders.forEach(d=>d.mesh.position.set(90,0,90));receivers[1].profile.cutting=10;receivers[1].profile.turning=10;");
  const r=q.state().receivers[1],d=q.state().defenders[1];d.mesh.position.set(r.start.x+4,0,r.start.z-2);
  for(let frame=0;frame<180&&!r.optionRead;frame++)q.run('gameTime+=1000/60;updateReceiver(receivers[1],1/60,gameTime)');
  assert.equal(r.optionRead,true);assert.equal(r.route,'Tunnel');assert.equal(r.screenTarget,true);
  assert.ok(r.path.every(p=>p.z>=r.start.z-5),'screen cannot acquire a deep stem');
  q.run("receivers[1].route='Screen Choice';receivers[1].optionRead=false;receivers[1].distance=20;ballLive=true;readOptionRoute(receivers[1])");assert.equal(r.route,'Screen Choice');
  q.run("ballLive=false;playState='call';audibleReceiverIndex=0;assignAudibleRoute('Choice');playState='live';receivers[0].distance=12;readOptionRoute(receivers[0])");assert.equal(q.state().receivers[0].optionRead,true);
});
test('seam and choice reads react to visible leverage without running a second stem',()=>{
  assert.equal(V.optionDecision({kind:'Seam Read',x:9,z:0,defenders:[{x:18,z:-8}]}),'Go');
  assert.equal(V.optionDecision({kind:'Seam Read',x:9,z:0,defenders:[{x:9,z:-10}]}),'Post');
  assert.equal(V.optionDecision({kind:'Seam Read',x:9,z:0,defenders:[{x:9,z:-10},{x:0,z:-12}]}),'Out');
  assert.equal(V.optionDecision({kind:'Choice',x:8,z:0,toGo:5,awareness:90,defenders:[{x:6,z:-1}]}),'Whip');
  const c=game(),q=c.q;c.Math.random=()=>0;q.run("selectedPlay=plays.findIndex(p=>p.name==='Double Stick Choice');setupPlay(false);playState='live';receivers[1].distance=12;defenders.forEach(d=>d.mesh.position.set(90,0,90));receivers[1].profile.cutting=100;receivers[1].profile.turning=100;");
  const r=q.state().receivers[1];q.state().defenders[1].mesh.position.copy(r.mesh.position).add(new THREE.Vector3(r.mesh.position.x>0?-1:1,0,-2));q.run('readOptionRoute(receivers[1])');assert.equal(r.route,'Out');assert.ok(r.path.at(-1).z>=r.mesh.position.z-3);
});
test('orbit, return and paired motion stay bounded, settle at release and preserve the chosen play on reload',()=>{
  const q=game().q;
  for(const name of ['Orbit Slip','Return Choice','Twin Shift Choice','Trade Motion Mesh']){
    q.run(`selectedPlay=plays.findIndex(p=>p.name===${JSON.stringify(name)});setupPlay(false);beginCountdown()`);
    const play=q.run('plays[selectedPlay]'),actors=q.state().receivers,motions=V.motions(play),starts=actors.map(r=>r.start.clone());let rearArc=false;
    const duration=q.run('motionDuration');
    for(let elapsed=0;elapsed<duration;elapsed+=1000/60){
      const previous=actors.map(r=>r.mesh.position.clone());q.run(`gameTime=snapTime-motionDuration+${elapsed};update(1/60,gameTime)`);
      for(const m of motions){const r=actors[m.slot];assert.ok(Math.abs(r.mesh.position.x)<=23);assert.ok(r.mesh.position.distanceTo(previous[m.slot])<=r.maxSpeed*1.15/60+.001);rearArc ||= r.mesh.position.z>starts[m.slot].z+2;}
    }
    q.run('gameTime=snapTime;update(1/60,gameTime)');assert.equal(q.state().playState,'live');
    for(const m of motions){assert.equal(actors[m.slot].start.x,m.to);assert.equal(actors[m.slot].path[0].x,m.to);}
    if(name.includes('Orbit'))assert.equal(rearArc,true);
  }
  q.run("selectedPlay=71;setupPlay(false);checkpoint();activateFranchise(P.validateSave(JSON.stringify(franchise)),0);resumeGame()");assert.equal(q.run('selectedPlay'),71);
});
test('screen outlets seek uncovered space and blocker reads release immediately into a free lane',()=>{
  const base={anchor:{x:8,z:2},losZ:0,defenders:[{x:8,z:2}],awareness:90};
  const outlet=V.screenOutlet(base);assert.ok(Math.abs(outlet.x-8)>1.5);assert.ok(outlet.z>=-1&&outlet.z<=5);
  assert.deepEqual(V.screenOutlet(base),outlet,'static coverage produces a stable outlet');
  const edge=V.screenOutlet({...base,anchor:{x:25,z:2},defenders:[]});assert.ok(edge.x<=23.4);
  const context={x:0,z:0,elapsed:.2,blockers:[{x:0,z:-2,target:'edge',engaged:false}],defenders:[{id:'edge',x:1,z:-5}],awareness:90};
  let read=V.screenRead(context);assert.equal(read.phase,'PRESS BLOCK');assert.ok(read.lead.x<0);assert.equal(read.pace,.86);
  read=V.screenRead({...context,elapsed:1.2});assert.equal(read.pace,1);
  read=V.screenRead({...context,defenders:[]});assert.equal(read.phase,'BURST');assert.equal(read.pace,1);assert.equal(read.lead,null);
  read=V.screenRead({...context,defenders:[{id:'edge',x:0,z:-1.5}]});assert.equal(read.pace,1,'do not wait for a block during immediate contact');
});
test('lead blockers ignore nearby screen passes but remain eligible for a direct catch',()=>{
  const q=game().q;q.run("selectedPlay=14;setupPlay(false);playState='thrown';ballLive=true;defenders.forEach(d=>d.mesh.position.set(90,0,90));globalThis.plans=0;receiverBallPlan=r=>{globalThis.plans++;return {point:r.mesh.position.clone().add(new THREE.Vector3(0,0,-1)),time:.1,facing:1,routeGap:0}};ballApproach=()=>({time:.6,dist:.2});receivers[0].route='Lead';receivers[0].blockAim=receivers[0].mesh.position.clone().add(new THREE.Vector3(0,0,-4));updateReceiver(receivers[0],1/60,0)");
  const r=q.state().receivers[0];assert.equal(r.trackingBall,false);assert.equal(q.run('globalThis.plans'),0);assert.ok(r.velocity.z<0);
  q.run('ballApproach=()=>({time:.1,dist:.2});updateReceiver(receivers[0],1/60,17)');assert.equal(r.trackingBall,true);assert.equal(q.run('globalThis.plans'),1);
});
test('screen carriers attack forward gaps at 30, 60 and 120 Hz without following blocks backward',()=>{
  for(const hz of [30,60,120]){
    const c=game(),q=c.q,r=arrangeCarrier(c,0,0);r.screenTarget=true;r.bestRunZ=0;r.heading.set(0,0,1);r.velocity.set(0,0,8);
    q.state().defenders.forEach((d,i)=>d.mesh.position.set((i-2)*1.2,0,-3));let retreat=0;
    for(let i=0;i<hz*3;i++){q.run(`gameTime+=1000/${hz};updateBallCarrier(receivers[0],1/${hz})`);retreat=Math.max(retreat,r.mesh.position.z-r.bestRunZ);}
    assert.ok(retreat<=1.61);assert.ok(r.mesh.position.z<0);assert.ok(r.velocity.length()<r.maxSpeed*1.3);
  }
});
test('context selects eight evasion techniques while contact, cooldowns and movement retain authority',()=>{
  const contexts=[{distance:1.8},{distance:4},{distance:3},{distance:3,nearSideline:true},{distance:2.5,crowded:true,style:'Power Receiver'},{distance:3,lateral:3},{distance:4,screen:true},{distance:3,closing:6,athleticism:90}];
  assert.equal(new Set(contexts.map(x=>V.evasionMove(x).name)).size,8);
  assert.notEqual(V.evasionMove({distance:3,last:'HARD CUT'}).name,'HARD CUT');
  const c=game(),q=c.q,r=arrangeCarrier(c,22,0);c.Math.random=()=>0;r.jukeCooldown=0;
  q.state().defenders[0].mesh.position.set(22,0,-3);q.state().defenders[1].mesh.position.set(21,0,-3.5);const start=r.mesh.position.clone();
  assert.equal(q.tryJuke(r),true);assert.equal(r.jukeMove,'SPEED CUT');assert.ok(r.jukeSide<0);assert.ok(r.impactVel.length()<5);assert.ok(r.mesh.position.equals(start),'move cannot teleport');assert.equal(q.tryJuke(r),false);assert.ok(q.state().defenders.filter(d=>d.fakeUntil>q.run('gameTime')).length<=1);
});
test('nine secured catch poses stay finite, keep the ball attached and reset on the next play',()=>{
  const c=game(),q=c.q,names=new Set(),poses=new Set();
  const variants=[{kind:'TOE TAP'},{kind:'HIGH POINT'},{kind:'LOW CATCH'},{kind:'CONTACT CATCH'},{kind:'CATCH AND TURN',oneHand:true},{kind:'CATCH AND TURN',screen:true},{kind:'BACK SHOULDER'},{kind:'OVER THE SHOULDER'},{kind:'CATCH AND TURN'}];
  for(const variant of variants){const r=arrangeCarrier(c);r.catchAnimation=V.catchAnimation({...variant,catching:90});r.catchAnimationTime=r.catchAnimation.duration;names.add(r.catchAnimation.name);
    for(let i=0;i<9;i++)q.run('animatePlayerContact(receivers[0],1/60);attachBallToCarrier()');
    const u=r.mesh.userData;poses.add([u.visualRig.rotation.x,u.visualRig.rotation.y,u.visualRig.rotation.z,u.visualRig.position.y,...u.hands.flatMap(h=>h.position.toArray())].map(x=>x.toFixed(4)).join(','));
    for(const h of u.hands)assert.ok(Number.isFinite(h.position.length()));assert.ok(q.ball.position.distanceTo(r.mesh.position)<3);
    assert.equal(q.state().ballLive,false);
  }
  assert.equal(names.size,9);assert.equal(poses.size,9);q.setupPlay();assert.equal(q.state().receivers[0].catchAnimation,undefined);
});

// Forgiving receiver catches: all possession tests still use real rendered contact shapes.
test('ordinary receiver hand control is reliable and bullet penalties are much smaller',()=>{
  const c=game(),q=c.q,r=contactScene(c);
  q.run('franchise.matchWeather=0;spiralQuality=1;throwBobbles=0');
  Object.assign(r.profile,{catching:70,athleticism:70});r.signature='';r.stagger=0;r.comebackActive=false;
  const control=speed=>q.run(`receiverCatchControl(receivers[0],${speed},false,false)`);
  assert.ok(control(24)>=.98);assert.ok(control(40)>=.98);assert.ok(control(50)>=.96);
  assert.ok(control(24)-control(50)<.02,'a hard but catchable throw is not a large random-drop penalty');
  assert.ok(q.run('receiverCatchControl(receivers[0],24,true,false)')>control(24));
  assert.ok(q.run('receiverCatchControl(receivers[0],50,false,true)')<control(50),'real defense still matters');
  r.profile.catching=100;assert.ok(control(50)>.97);
  r.profile.catching=20;assert.ok(control(50)<.95,'attributes still affect difficult catches');
});
test('weather, pressure, placement and actual bobble recovery keep bounded receiver odds',()=>{
  const c=game(),q=c.q,r=contactScene(c);q.run('franchise.matchWeather=0;spiralQuality=1;throwBobbles=0');
  Object.assign(r.profile,{catching:70,athleticism:70});r.signature='';r.stagger=0;r.comebackActive=false;
  const base=q.run('receiverCatchControl(receivers[0],40,false,false)');
  r.stagger=.1;r.comebackActive=true;r.underthrowDifficulty=.3;
  const difficult=q.run('receiverCatchControl(receivers[0],40,false,false)');
  assert.ok(difficult<base&&base-difficult<.07);
  q.run('throwBobbles=1');assert.ok(q.run('receiverCatchControl(receivers[0],40,false,false)')>difficult);
  for(const catching of [1,70,100,140])for(const weather of [0,1,2,3,4]){
    r.profile.catching=catching;q.run(`franchise.matchWeather=${weather}`);
    const probability=q.run('receiverCatchControl(receivers[0],90,true,true)');
    assert.ok(Number.isFinite(probability)&&probability>=.55&&probability<=.94);
  }
});
test('recent ball tracking survives a short planner gap but cannot grant blind catches forever',()=>{
  const c=game(),q=c.q;c.Math.random=()=>0;
  let r=contactScene(c);r.trackingBall=false;r.catchTrackingUntil=.20;q.run('throwTime=.10');ballAtHand(q,r);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch.actor'),r);
  r=contactScene(c);r.trackingBall=false;r.catchTrackingUntil=.20;q.run('throwTime=.30');ballAtHand(q,r);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch'),null);assert.equal(r.hasBall,false);
});
test('a new throw clears all tracking and wrist-gather grace from previous plays',()=>{
  const c=game(),q=c.q;q.resumeGame();
  for(const a of [...q.state().receivers,...q.state().defenders]){a.catchTrackingUntil=100;a.gatherUntil=100;a.gatherUsed=true;}
  q.run("playState='live';chargeStart=gameTime-680");q.throwBall();
  for(const a of [...q.state().receivers,...q.state().defenders]){
    assert.equal(a.catchTrackingUntil,0);assert.equal(a.gatherUntil,0);assert.equal(a.gatherUsed,false);
  }
});
test('the second hand can join on a later step and shorten receiver securing without phantom contact',()=>{
  const c=game(),q=c.q,r=contactScene(c);c.Math.random=()=>0;ballAtHand(q,r);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch.twoHands'),false);
  assert.equal(q.run('pendingCatch.remaining'),.040);q.checkBallContact();assert.equal(q.run('pendingCatch.twoHands'),false);
  r.mesh.userData.hands[1].position.copy(r.mesh.userData.hands[0].position);q.sampleContactRig(r,true);
  q.checkBallContact();assert.equal(q.run('pendingCatch.twoHands'),true);assert.equal(r.catchOneHand,false);
  q.finishSecuringCatch(.020);assert.equal(r.hasBall,true);assert.equal(q.state().playState,'run');
});
test('a receiver can win simultaneous actual hand contact without rerolling the same overlap',()=>{
  const c=game(),q=c.q,r=contactScene(c),d=q.state().defenders[0];let rolls=0;c.Math.random=()=>{rolls++;return 0;};
  ballAtHand(q,r);d.mesh.position.copy(r.mesh.position);d.mesh.scale.copy(r.mesh.scale);d.mesh.rotation.copy(r.mesh.rotation);d.ballSeen=true;
  d.mesh.userData.hands[0].position.copy(r.mesh.userData.hands[0].position);q.sampleContactRig(d,true);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch.actor'),r);assert.equal(d.contactLock,true);
  const before=rolls;for(let i=0;i<20;i++)q.checkBallContact();assert.equal(rolls,before);
  q.finishSecuringCatch(.040);assert.equal(r.hasBall,true);
});
test('provisional receiver catches can withstand a real defender touch, once per separated contact',()=>{
  const c=game(),q=c.q,r=contactScene(c),d=q.state().defenders[0];c.Math.random=()=>0;ballAtHand(q,r);
  assert.ok(q.checkBallContact());
  d.mesh.position.copy(r.mesh.position);d.mesh.scale.copy(r.mesh.scale);d.mesh.rotation.copy(r.mesh.rotation);d.ballSeen=true;
  d.mesh.userData.hands[0].position.copy(r.mesh.userData.hands[0].position);q.sampleContactRig(d,true);q.ballPrev.copy(q.ball.position);
  const position=q.ball.position.clone();assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch.actor'),r);
  assert.ok(q.ball.position.equals(position),'winning the contest must not move a held ball off its glove');
  assert.equal(d.contactLock,true);q.finishSecuringCatch(.040);assert.equal(r.hasBall,true);
});
function wristGatherScene(c){
  const q=c.q,r=contactScene(c);q.run('throwTime=0;throwBobbles=0');r.mesh.updateMatrixWorld(true);
  const limb=q.sampleContactRig(r,true).find(l=>l.mesh===r.mesh.userData.forearms[0]);
  const hand=r.mesh.userData.hands[0].getWorldPosition(new THREE.Vector3()),axis=limb.b.clone().sub(limb.a).normalize();
  for(const distance of [.22,.26,.29])for(const radial of [.23,.25])for(const candidate of [[1,0,0],[-1,0,0],[0,0,1],[0,0,-1]]){
    const normal=new THREE.Vector3(...candidate);normal.addScaledVector(axis,-normal.dot(axis));if(normal.length()<.1)continue;normal.normalize();
    q.ball.position.copy(hand).addScaledVector(axis,-distance).addScaledVector(normal,radial);q.ballPrev.copy(q.ball.position);q.ballVel.copy(normal).multiplyScalar(-14);
    const hit=q.playerBallContacts(r).sort((a,b)=>a.time-b.time)[0];
    if(hit?.limb===limb&&hit.point.distanceTo(hand)<=.30)return {r,position:q.ball.position.clone(),velocity:q.ballVel.clone()};
  }
  assert.fail('fixture must find a real wrist-side forearm contact, not a glove or upper arm');
}
test('wrist-side contacts get one brief physical gather but only gloves can award possession',()=>{
  const c=game(),q=c.q,{r}=wristGatherScene(c);c.Math.random=()=>0;
  const position=q.ball.position.clone(),speed=q.ballVel.length();
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch'),null);assert.equal(r.hasBall,false);
  assert.ok(q.ball.position.equals(position),'a forearm gather must not teleport the ball');assert.ok(q.ballVel.length()<speed);
  assert.equal(q.run('throwBobbles'),0);assert.ok(r.gatherUntil>q.run('throwTime'));
  ballAtHand(q,r);assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch.actor'),r);
  q.finishSecuringCatch(.040);assert.equal(r.hasBall,true);
});
test('a missed wrist gather expires, stays loose, and cannot repeat indefinitely',()=>{
  const c=game(),q=c.q,{r,position,velocity}=wristGatherScene(c);c.Math.random=()=>0;
  assert.ok(q.checkBallContact());assert.equal(r.gatherUsed,true);q.run('throwTime=.08');
  q.ball.position.copy(position);q.ballPrev.copy(position);q.ballVel.copy(velocity);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch'),null);assert.equal(r.hasBall,false);assert.equal(q.run('throwBobbles'),1);
});
test('ordinary missed hand catches rebound gently, while defender tips remain stronger',()=>{
  const c=game(),q=c.q,r=contactScene(c);c.Math.random=()=>.999;ballAtHand(q,r);q.ballVel.set(0,0,-40);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch'),null);assert.ok(q.run('ballTumble')<=8);assert.equal(q.run('spiralQuality'),.70);
  const softSpeed=q.ballVel.length(),d=q.state().defenders[0];r.mesh.position.set(90,0,90);d.mesh.position.set(0,0,10);d.mesh.scale.set(1,1,1);d.mesh.rotation.set(0,0,0);d.ballSeen=true;d.velocity.set(0,0,0);
  q.run('spiralQuality=1');ballAtHand(q,d);q.ballVel.set(0,0,-40);assert.ok(q.checkBallContact());
  assert.equal(q.run('spiralQuality'),.35);assert.ok(q.ballVel.length()>softSpeed);
});
test('forgiving routine hand catches still work through real moving-player physics at 30, 60 and 120 Hz',()=>{
  for(const hz of [30,60,120]){
    const c=game(),q=c.q,r=contactScene(c);c.Math.random=()=>.95;q.run('franchise.matchWeather=0');Object.assign(r.profile,{catching:70,athleticism:70});r.signature='';
    r.heading.set(0,0,-1);r.mesh.rotation.y=Math.PI;r.velocity.set(0,0,-8);r.maxSpeed=8;
    r.start.copy(r.mesh.position);r.path=[r.mesh.position.clone(),new THREE.Vector3(0,0,-70)];r.distance=0;r.runIntensity=.8;
    q.ball.position.set(0,2.25,25);q.ballPrev.copy(q.ball.position);q.ballVel.set(0,(1.6-2.25+4.905*.36)/.6,(-8*.6-15)/.6);
    let ticks=0;while(q.state().ballLive&&ticks<hz*2){ticks++;q.run(`gameTime+=${1000/hz}`);q.update(1/hz,ticks*1000/hz);}
    assert.equal(q.state().playState,'run',`${hz} Hz: ${q.state().playState}, tips ${q.run('throwBobbles')}`);assert.equal(r.hasBall,true);
  }
});
