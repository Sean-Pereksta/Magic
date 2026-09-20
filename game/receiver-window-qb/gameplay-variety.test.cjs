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
    captureReplay,startReplay,updateReplay,finishReplay,scenePose,drawTrajectory,assignAudibleRoute,
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
  q.run('replayRecording=true');for(let i=0;i<500;i++){q.ball.position.z=-i/10;q.captureReplay(true);}
  const frames=q.state().replayFrames;assert.equal(frames.length,360);assert.equal(frames[0].transforms.BYTES_PER_ELEMENT,4);
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
