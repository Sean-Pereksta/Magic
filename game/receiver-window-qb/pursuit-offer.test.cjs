const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const THREE=require('./vendor/three.min.js'),P=require('./progression.js');
const source=fs.readFileSync(__dirname+'/game.js','utf8');
function extract(name){const start=source.indexOf('function '+name+'(');assert.ok(start>=0);const next=source.indexOf('\nfunction ',start+1);return source.slice(start,next<0?undefined:next);}
function context(){
  const c=vm.createContext({THREE,P,ballLive:true,ball:{position:new THREE.Vector3()},ballVel:new THREE.Vector3(),throwTime:0,
    routePosition:()=>new THREE.Vector3(),updateTricks:()=>{},receiverContactFactors:()=>({speed:1,accel:1}),
    updateJump:()=>{},animatePlayerContact:()=>{},ballApproach:()=>null,predictedLanding:new THREE.Vector3(),predictedFlightTime:0});
  for(const name of ['getBallLanding','receiverBallPlan','updateReceiver'])vm.runInContext(extract(name),c);
  return c;
}
function receiver(){return {profile:{speed:60,turning:60,cutting:60,catching:60,athleticism:60,size:60},
  mesh:{position:new THREE.Vector3(),scale:new THREE.Vector3(1,1,1),rotation:{y:Math.PI},userData:{}},
  heading:new THREE.Vector3(0,0,-1),velocity:new THREE.Vector3(0,0,-8),impactVel:new THREE.Vector3(),
  path:[],distance:0,speed:8,maxSpeed:8,history:[],shoveCooldown:0,shoveSlow:0,stagger:0,comebackActive:false,comebackPlant:0};}
test('in-stride ball overrides a route turning away, preserving momentum across frames',()=>{
  const c=context(),r=receiver();c.routePosition=()=>new THREE.Vector3(6,0,3);
  c.ball.position.set(0,2,4);c.ballVel.set(0,4.905,-16);
  for(let i=0;i<28;i++){
    const before=r.mesh.position.clone();c.updateReceiver(r,1/60,i*1000/60);
    assert.equal(r.trackingBall,true);assert.equal(r.comebackActive,false);
    assert.ok(r.velocity.z < -7.9);assert.ok(Math.abs(r.velocity.x)<.001);
    assert.ok(r.mesh.position.distanceTo(before)<.14,'no teleport');
    c.ball.position.addScaledVector(c.ballVel,1/60);c.ball.position.y-=.5*9.81/(60*60);c.ballVel.y-=9.81/60;c.throwTime+=1/60;
  }
});
test('committed target survives a missed reachability sample without reverting to route',()=>{
  const c=context(),r=receiver();c.ball.position.set(0,2,4);c.ballVel.set(0,4.905,-16);
  assert.ok(c.receiverBallPlan(r));r.mesh.position.x=30;c.routePosition=()=>new THREE.Vector3(100,0,100);
  const plan=c.receiverBallPlan(r);assert.ok(plan);assert.ok(plan.point.x===0);
  c.throwTime=r.ballPursuit.expires+.01;assert.equal(c.receiverBallPlan(r),null);
});
test('unrelated cross-field pass does not acquire pursuit',()=>{
  const c=context(),r=receiver();c.ball.position.set(35,2,4);c.ballVel.set(0,4.905,-16);
  assert.equal(c.receiverBallPlan(r),null);
});
test('underthrow still requires a plant and finite turn; dead ball restores route control',()=>{
  const c=context(),r=receiver();c.ball.position.set(0,2,3);c.ballVel.set(0,1,-1);
  c.updateReceiver(r,1/60,0);assert.equal(r.trackingBall,true);assert.equal(r.comebackActive,true);assert.ok(r.comebackPlant>0);
  assert.ok(r.heading.z<-.99,'cannot instantly reverse');assert.ok(r.velocity.length()<8);
  c.ballLive=false;c.updateReceiver(r,1/60,17);assert.equal(r.trackingBall,false);assert.equal(r.ballPursuit,null);assert.equal(r.comebackActive,false);
});
test('contact still slows an in-stride receiver',()=>{
  const c=context(),r=receiver();c.ball.position.set(0,2,4);c.ballVel.set(0,4.905,-16);
  c.receiverContactFactors=()=>({speed:.66,accel:.54});c.updateReceiver(r,1/60,0);
  assert.ok(r.velocity.length()<8);
});
function matchContext(overrides={}){
  const els={cloudOffer:{hidden:true}};const c=vm.createContext({P,franchise:{round:1,wins:0,cash:0,...overrides},
    seriesOffense:1,seriesDefense:3,tournamentStage:0,opponentForRound:()=>({name:'Test'}),
    resetDrive:()=>{},freshMarket:()=>[],checkpoint:()=>{},saveFranchise:()=>{},openManager:()=>{},
    $:id=>els[id]});c.scheduleResult=fn=>c.pending=fn;vm.runInContext(extract('endMatchup'),c);return {c,els};
}
for(const won of [true,false])test(`first completed ${won?'win':'loss'} offers cloud once and survives serialization`,()=>{
  const {c,els}=matchContext();assert.equal(els.cloudOffer.hidden,true);
  c.endMatchup(won);assert.equal(els.cloudOffer.hidden,true);c.pending();assert.equal(els.cloudOffer.hidden,false);
  const saved=JSON.parse(JSON.stringify(c.franchise));assert.equal(saved.cloudSaveOffered,true);
  const restored=matchContext(saved);restored.c.endMatchup(false);restored.c.pending();assert.equal(restored.els.cloudOffer.hidden,true);
});
test('existing progress and already-saved franchises do not receive first-match offer',()=>{
  for(const f of [{round:4,wins:3},{losses:1},{cloudSaveOffered:true}]){
    const {c,els}=matchContext(f);c.endMatchup(false);c.pending();assert.equal(els.cloudOffer.hidden,true);
  }
});
test('offer actions dismiss inline offer and open existing saves only on Save',()=>{
  const els={cloudOffer:{hidden:false},cloudOfferSave:{},cloudOfferDismiss:{},cloudName:{focus(){this.focused=true}}};
  let opens=0;const c=vm.createContext({$:id=>els[id],openSaves:()=>opens++});
  for(const id of ['cloudOfferSave','cloudOfferDismiss'])vm.runInContext(source.split('\n').find(l=>l.startsWith(`$('${id}').onclick=`)),c);
  els.cloudOfferDismiss.onclick();assert.equal(opens,0);assert.equal(els.cloudOffer.hidden,true);
  els.cloudOffer.hidden=false;els.cloudOfferSave.onclick();assert.equal(opens,1);assert.equal(els.cloudOffer.hidden,true);assert.equal(els.cloudName.focused,true);
});
