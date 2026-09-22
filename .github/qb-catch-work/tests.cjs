
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
