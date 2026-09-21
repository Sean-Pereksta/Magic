// Execute the real runtime/Three.js math with only DOM and GPU I/O substituted.
// This covers state/physics integration; browser-smoke remains the rendering gate.
const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const THREE=require('./vendor/three.min.js'),P=require('./progression.js'),V=require('./variety.js');
const source=fs.readFileSync(__dirname+'/game.js','utf8');
function game(){
  const elements=new Map(),storage=new Map();
  function element(){return {style:{},dataset:{},hidden:false,children:[],classList:{toggle(){},add(){},remove(){}},
    addEventListener(){},appendChild(child){this.children.push(child)},replaceChildren(){this.children=[]},
    querySelector(){return element()},getContext(){return {fillText(){}}},getBoundingClientRect(){return {left:0,top:0,width:1200,height:800}}};}
  const document={body:element(),getElementById(id){if(!elements.has(id))elements.set(id,element());return elements.get(id)},
    createElement:element,querySelectorAll:()=>[],addEventListener(){},exitPointerLock(){}};
  class Renderer{constructor(){this.shadowMap={};}setPixelRatio(n){this.pixelRatio=n}setSize(){}render(){}}
  const context=vm.createContext({THREE:{...THREE,WebGLRenderer:Renderer},QBProgression:P,QBVariety:V,document,
    localStorage:{getItem:k=>storage.get(k),setItem:(k,v)=>storage.set(k,v)},performance:{now:()=>0},
    innerWidth:1200,innerHeight:800,devicePixelRatio:1,requestAnimationFrame(){},addEventListener(){},setInterval(){},
    matchMedia:()=>({matches:false}),console,Math:Object.create(Math)});
  const hook=`globalThis.q={resumeGame,setupPlay,beginCountdown,update,throwBall,checkBallContact,resolveCatch,finishPlayAtSpot,activateFranchise,recordAttempt,
    pathFor,routePosition,defenderTarget,updateDefender,catchPlacement,animatePlayerContact,sampleContactRig,playerBallContacts,sweepLimb,deflectBall,finishSecuringCatch,wantsHighPoint,updateJump,updateBallCarrier,applyFieldTheme,setQuality,updateEnvironment,
    captureReplay,startReplay,updateReplay,finishReplay,scenePose,drawTrajectory,assignAudibleRoute,tryJuke,updateRunAfterCatch,clearGoalLane,startDivingTackle,tackleContact,triggerTackle,updateTackle,runnerFinishAbility,effortDivePlan,tryEffortDive,carriedBallFrontZ,poseFinishPlayer,markCatch,scheduleResult,loop,showMainMenu,
    state:()=>({franchise,receivers,defenders,playState,ballLive,currentDefense,snapMemory,replayFrames,replay,transition,qualityLevel,rain,renderer,arcGeo}),
    get ball(){return ball},get ballPrev(){return ballPrev},get ballVel(){return ballVel},get camera(){return camera},
    run(code){return eval(code)}};`;
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
  const c=game(),q=c.q,r=contactScene(c),d=q.state().defenders[0];c.Math.random=()=>0;
  ballAtHand(q,r);d.mesh.position.copy(r.mesh.position);d.mesh.scale.copy(r.mesh.scale);d.mesh.rotation.copy(r.mesh.rotation);d.ballSeen=true;
  d.mesh.userData.hands[0].position.copy(r.mesh.userData.hands[0].position);q.sampleContactRig(d,true);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch'),null);assert.equal(q.state().ballLive,true);
  const r2=contactScene(c),d2=q.state().defenders[0];d2.mesh.position.set(0,0,11.2);ballAtHand(q,r2);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch.actor'),r2);
});
test('another actual hand contact breaks a provisional catch and can be intercepted after separation',()=>{
  const c=game(),q=c.q,r=contactScene(c),d=q.state().defenders[0];c.Math.random=()=>0;ballAtHand(q,r);
  assert.ok(q.checkBallContact());d.mesh.position.copy(r.mesh.position);d.mesh.scale.copy(r.mesh.scale);d.mesh.rotation.copy(r.mesh.rotation);d.ballSeen=true;
  d.mesh.userData.hands[0].position.copy(r.mesh.userData.hands[0].position);q.sampleContactRig(d,true);q.ballPrev.copy(q.ball.position);
  assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch'),null);assert.equal(q.state().ballLive,true);assert.equal(r.hasBall,false);
  r.mesh.position.set(90,0,90);q.ball.position.set(0,5,0);q.ballPrev.copy(q.ball.position);q.playerBallContacts(d);
  ballAtHand(q,d);assert.ok(q.checkBallContact());assert.equal(q.run('pendingCatch.actor'),d);
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
