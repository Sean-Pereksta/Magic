'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const THREE=require('./vendor/three.min.js'),S=require('./player-scale.js'),follow=require('./camera-follow.js');
const game=fs.readFileSync(__dirname+'/game.js','utf8'),modes=fs.readFileSync(__dirname+'/mode-runtime.js','utf8');
function extract(source,name){const start=source.indexOf('function '+name+'(');assert.ok(start>=0,'Missing '+name);let end=source.indexOf('{',start),depth=1;for(end++;depth;end++){if(source[end]==='{')depth++;if(source[end]==='}')depth--;}return source.slice(start,end);}
function fixture(){
  const camera=new THREE.PerspectiveCamera(67,1,.08,220),ball=new THREE.Group();ball.userData.spin=new THREE.Group();
  const c={THREE,PLAYER_SCALE:S.actorScale,QB_EYE_HEIGHT:S.eyeHeight,QBPlayerScale:S,QBCameraFollow:follow,camera,
    flightCameraState:null,flightCameraOrigin:null,yaw:0,pitch:0,worldZForYards:y=>44-y*2,ballSpotYards:12,
    modeRuntime:{active:false,release(){},config:()=>null},replay:false,ball,ballVel:new THREE.Vector3(),ballPrev:new THREE.Vector3(),
    ballLive:false,playState:'live',ballCarrier:null,throwTime:0,ENDZONE_BACK_Z:-70,FIELD_START_Z:44,
    playLog:null,F:{newPlay:()=>({})},plays:[{name:'Vertical'}],selectedPlay:0,replayRecording:true,down:1,replayEligible:false,
    captureReplay(){},currentHeld:()=>.8,throwStats:()=>({power:.8,quality:1}),receivers:[],defenders:[],loftBias:0,
    predictedLanding:new THREE.Vector3(),drawTrajectory:()=>null,showMessage(){}};
  vm.createContext(c);
  vm.runInContext(['applyCamera','resetAim','seedFlightCamera','updateFollowingCamera','centerThrowDirection','throwBall'].map(n=>extract(game,n)).join('\n'),c);
  c.idealLaunchVelocity=()=>c.centerThrowDirection().multiplyScalar(32);c.resetAim();return c;
}
test('the actual snap reset and automatic QB release share the taller eye height',()=>{
  const c=fixture();
  for(const active of [false,true]){
    c.modeRuntime.active=active;c.camera.position.set(18,13,-30);c.flightCameraState={old:true};c.flightCameraOrigin=new THREE.Vector3(1,2,3);c.yaw=.8;c.pitch=.3;
    c.resetAim();assert.equal(c.flightCameraState,null);assert.equal(c.flightCameraOrigin,null);assert.equal(c.yaw,0);assert.equal(c.pitch,-.08);
    assert.ok(Math.abs(c.camera.position.y-3.15)<1e-12);assert.equal(c.camera.position.z,c.worldZForYards(c.ballSpotYards)+(active?10:14));
    assert.ok(c.centerThrowDirection().distanceTo(new THREE.Vector3(0,Math.sin(-.08),-Math.cos(.08)))<1e-12);
  }
  const context={T:THREE,root:{QBPlayerScale:S},ctx:{state:()=>({snapSpotYards:12}),worldZ:c.worldZForYards}};
  vm.createContext(context);vm.runInContext(extract(modes,'passOrigin'),context);
  const origin=context.passOrigin();assert.ok(Math.abs(origin.y-S.eyeHeight)<1e-12);assert.equal(origin.z,c.camera.position.z);
});
test('actual throw release seeds continuous ball following without changing its launch ray or physics',()=>{
  const c=fixture();c.yaw=.17;c.pitch=.12;c.applyCamera();
  const eye=c.camera.position.clone(),direction=c.centerThrowDirection(),release=eye.clone().addScaledVector(direction,1.05),velocity=direction.clone().multiplyScalar(32);
  c.throwBall();assert.ok(c.ball.position.distanceTo(release)<1e-12);assert.ok(c.ballVel.distanceTo(velocity)<1e-12);
  assert.notEqual(c.flightCameraOrigin,c.ball.position);assert.ok(c.flightCameraOrigin.distanceTo(release)<1e-12);
  assert.ok(c.flightCameraState.position.distanceTo(eye)<1e-12,'The follow camera begins at the real release camera');
  for(let i=1;i<=72;i++){
    c.throwTime=i/60;c.ball.position.copy(release).addScaledVector(velocity,c.throwTime);c.ball.position.y-=4.905*c.throwTime*c.throwTime;
    const before=c.ball.position.clone(),previous=c.camera.position.clone();c.updateFollowingCamera(1/60);
    assert.ok(c.ball.position.distanceTo(before)<1e-12);assert.ok(c.ballVel.distanceTo(velocity)<1e-12,'Camera tracking never rewrites launch velocity');
    assert.ok(c.camera.position.distanceTo(previous)<=85/60+1e-8);assert.ok(c.camera.position.y>=3.2);
  }
  assert.ok(c.camera.position.distanceTo(eye)>10,'Camera follows the pass downfield');assert.ok(c.flightCameraOrigin.distanceTo(release)<1e-12);
  assert.equal(c.yaw,.17);assert.equal(c.pitch,.12);
});
test('actual possession following frames the enlarged chest and preserves replay camera ownership',()=>{
  const c=fixture();c.throwBall();c.updateFollowingCamera(1/60);
  c.ballLive=false;c.playState='run';c.ballCarrier={mesh:new THREE.Group(),velocity:new THREE.Vector3(0,0,-6)};c.ballCarrier.mesh.position.set(4,0,-8);
  for(let i=0;i<120;i++)c.updateFollowingCamera(1/60);
  assert.ok(Math.abs(c.flightCameraState.targetLookAt.y-1.35*S.actorScale)<1e-12,'The carrier focus rises with the enlarged athlete');
  const carrierPosition=c.ballCarrier.mesh.position.clone(),velocity=c.ballCarrier.velocity.clone();
  c.replay=true;c.camera.position.set(-12,19,22);const replayPosition=c.camera.position.clone(),rotation=c.camera.quaternion.clone();
  c.updateFollowingCamera(1/60);assert.ok(c.camera.position.distanceTo(replayPosition)<1e-12);assert.ok(c.camera.quaternion.angleTo(rotation)<1e-7);
  assert.ok(c.ballCarrier.mesh.position.distanceTo(carrierPosition)<1e-12);assert.ok(c.ballCarrier.velocity.distanceTo(velocity)<1e-12);
  c.replay=false;c.playState='call';c.ballCarrier=null;c.updateFollowingCamera(1/60);assert.ok(c.camera.position.distanceTo(replayPosition)<1e-12);
  c.resetAim();assert.ok(Math.abs(c.camera.position.y-S.eyeHeight)<1e-12);assert.equal(c.flightCameraState,null);
});
