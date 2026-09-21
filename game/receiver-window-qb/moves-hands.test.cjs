const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const THREE=require('./vendor/three.min.js'),P=require('./progression.js');
const source=fs.readFileSync(__dirname+'/game.js','utf8');
function extract(name){const start=source.indexOf('function '+name+'('),end=source.indexOf('\nfunction ',start+1);return source.slice(start,end<0?undefined:end);}
function actor(){
  const mesh=new THREE.Group(),rig=new THREE.Group();mesh.add(rig);
  const hands=[new THREE.Object3D(),new THREE.Object3D()],arms=hands.map(()=>new THREE.Object3D());
  hands.forEach((h,i)=>{h.position.set(i? .53:-.53,.86,.02);rig.add(h)});arms.forEach(a=>rig.add(a));
  mesh.userData={visualRig:rig,hands,arms,body:new THREE.Object3D(),feet:[],legs:[]};
  return {mesh,profile:P.migratePlayer({name:'Test',speed:80,cutting:80,turning:80,evasion:80,catching:80,strength:80,tricks:80}),velocity:new THREE.Vector3(),impactVel:new THREE.Vector3(),trackingBall:true,jumpY:0,stagger:0,fakeUntil:0};
}
function context(){const c=vm.createContext({THREE,P,V:require('./variety.js'),franchise:{conceptMemory:[]},plays:[{name:'Mesh'}],selectedPlay:0,activeRound:()=>1,ballLive:true,ball:{position:new THREE.Vector3()},ballVel:new THREE.Vector3(),gameTime:1000,defenders:[],ballCarrier:null,currentSkill:()=>.8,flashResult:()=>{},Math:Object.create(Math)});for(const name of ['poseJointedLimbs','receiverHandTarget','trackReceiverHands','animatePlayerContact','tryJuke','defenderTarget'])vm.runInContext(extract(name),c);return c;}
test('hands reach left, right, low and high with bounded shoulders',()=>{
  for(const [x,y] of [[-1,1.4],[1,1.4],[0,.4],[0,2.7]]){
    const c=context(),r=actor();c.ball.position.set(x,y,.25);
    for(let i=0;i<30;i++)c.trackReceiverHands(r,1/60);
    const avg=r.mesh.userData.hands.reduce((n,h)=>n+h.position.x,0)/2;
    if(x)assert.ok(avg*x>0);
    for(const [i,h] of r.mesh.userData.hands.entries()){
      assert.ok(h.position.distanceTo(new THREE.Vector3(i?.48:-.48,1.48,.02))<=.951);
      if(y<1)assert.ok(h.position.y<1);if(y>2)assert.ok(h.position.y>2);
    }
  }
});
test('hand motion is smooth and follows world position through rotation and scale',()=>{
  const c=context(),r=actor();r.mesh.position.set(4,.5,3);r.mesh.scale.set(1.1,1.2,.9);r.mesh.rotation.y=Math.PI/2;
  r.mesh.updateMatrixWorld(true);c.ball.position.copy(r.mesh.localToWorld(new THREE.Vector3(.8,1.4,.3)));
  const before=r.mesh.userData.hands[0].position.clone();c.trackReceiverHands(r,1/60);
  assert.ok(r.mesh.userData.hands[0].position.distanceTo(before)<=8/60+.0001);
  for(let i=0;i<30;i++)c.trackReceiverHands(r,1/60);
  assert.ok(r.mesh.userData.hands.every(h=>h.position.x>0));
  c.ballLive=false;c.trackReceiverHands(r,1/60);assert.equal(r.handTargets,null);
});
test('unrelated balls and secured catches cannot acquire hand tracking',()=>{
  const c=context(),r=actor();c.ball.position.set(20,2,20);assert.equal(c.receiverHandTarget(r),null);
  c.ball.position.set(0,1,0);r.hasBall=true;assert.equal(c.receiverHandTarget(r),null);
  r.hasBall=false;r.trackingBall=false;assert.equal(c.receiverHandTarget(r),null);
});
test('successful juke physically redirects a defender and full model spins, then resets',()=>{
  const c=context(),r=actor(),d=actor();delete d.profile;c.ballLive=false;c.Math.random=()=>0;d.mesh.position.set(0,0,-1.5);c.defenders=[d];c.ballCarrier=r;
  assert.equal(c.tryJuke(r,true),true);assert.ok(d.fakeUntil>c.gameTime);assert.ok(d.impactVel.length()>0);
  assert.ok(c.defenderTarget(d,c.gameTime).distanceTo(d.fakeTarget)<.0001);
  const before=r.mesh.position.clone();r.jukeAnim=r.jukeDuration*.5;c.animatePlayerContact(r,1/60);
  assert.ok(Math.abs(r.mesh.userData.visualRig.rotation.y)>3);
  assert.ok(r.mesh.position.distanceTo(before)===0);
  c.animatePlayerContact(d,1/60);assert.ok(Math.abs(d.mesh.userData.visualRig.rotation.z)>0);
  assert.equal(c.tryJuke(r,true),false);r.jukeAnim=0;c.animatePlayerContact(r,1/60);assert.ok(Math.abs(r.mesh.userData.visualRig.rotation.y)<.0001);
});
test('defenders can resist and airborne passes cannot trigger jukes',()=>{
  const c=context(),r=actor(),d=actor();delete d.profile;d.mesh.position.set(0,0,-2.5);c.defenders=[d];c.Math.random=()=>.999;
  assert.equal(c.tryJuke(r),false);c.ballLive=false;assert.equal(c.tryJuke(r),true);assert.equal(d.fakeUntil,0);assert.equal(d.impactVel.length(),0);
});
