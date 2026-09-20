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
    pathFor,routePosition,defenderTarget,updateDefender,catchPlacement,tryBobble,updateBallCarrier,applyFieldTheme,setQuality,updateEnvironment,
    captureReplay,startReplay,updateReplay,finishReplay,scenePose,drawTrajectory,assignAudibleRoute,tryJuke,updateRunAfterCatch,clearGoalLane,startDivingTackle,tackleContact,triggerTackle,updateTackle,markCatch,scheduleResult,loop,showMainMenu,
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
test('catches use momentum, bobbles remain live, and sideline catches end at their real spot',()=>{
  const c=game(),q=c.q;c.Math.random=()=>0;
  function arrange(x=0){q.setupPlay();q.run("playState='thrown';ballLive=true;attemptPending=true");const r=q.state().receivers[0];r.mesh.position.set(x,0,10);r.heading.set(0,0,-1);r.velocity.set(0,0,-8);r.trackingBall=true;for(const d of q.state().defenders)d.mesh.position.set(80,0,80);q.ball.position.set(x,1.4,9.5);q.ballPrev.copy(q.ball.position);return r;}
  let r=arrange();q.resolveCatch(r,false);assert.equal(q.state().playState,'run');assert.ok(r.velocity.length()>7.7);
  r=arrange();q.ball.position.z=10.6;q.resolveCatch(r,false);assert.ok(r.velocity.length()<5);
  r=arrange();assert.equal(q.tryBobble(r),true);assert.equal(q.state().ballLive,true);assert.equal(r.hasBall,false);assert.equal(q.tryBobble(r),false);
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
  assert.equal(calls,1);assert.equal(q.state().franchise.cash,cash);assert.ok(r2.mesh.rotation.x<-.9);
});
