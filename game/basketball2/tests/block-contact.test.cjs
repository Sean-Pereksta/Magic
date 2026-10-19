const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

class Vector3 {
  constructor(x=0,y=0,z=0){this.set(x,y,z);}
  set(x,y,z){this.x=x;this.y=y;this.z=z;return this;}
  setY(y){this.y=y;return this;}
  copy(v){return this.set(v.x,v.y,v.z);}
  clone(){return new Vector3(this.x,this.y,this.z);}
  add(v){return this.addScaledVector(v,1);}
  sub(v){return this.addScaledVector(v,-1);}
  addScaledVector(v,s){this.x+=v.x*s;this.y+=v.y*s;this.z+=v.z*s;return this;}
  multiplyScalar(s){this.x*=s;this.y*=s;this.z*=s;return this;}
  lengthSq(){return this.dot(this);}
  length(){return Math.sqrt(this.lengthSq());}
  normalize(){return this.multiplyScalar(1/(this.length()||1));}
  dot(v){return this.x*v.x+this.y*v.y+this.z*v.z;}
  distanceToSquared(v){return this.clone().sub(v).lengthSq();}
  distanceTo(v){return Math.sqrt(this.distanceToSquared(v));}
  reflect(n){return this.addScaledVector(n,-2*this.dot(n));}
}
function setup(){
  const ctx=vm.createContext({THREE:{Vector3,MathUtils:{clamp:(v,a,b)=>Math.max(a,Math.min(b,v)),lerp:(a,b,t)=>a+(b-a)*t}},console});
  for(const file of ['state','animation-contact','ball-physics','scoring','presentation'])vm.runInContext(fs.readFileSync(path.join(__dirname,'../js',file+'.js'),'utf8'),ctx);
  vm.runInContext(`
    function flatDistance(a,b){return Math.hypot(a.x-b.x,a.z-b.z)}
    function getForward(a){return new THREE.Vector3(Math.sin(a),0,Math.cos(a))}
    function getBodyRightVector(p){const f=getForward(p.angle);return new THREE.Vector3(f.z,0,-f.x)}
    function getOpponents(p){return matchPlayers.filter(other=>other.teamId!==p.teamId)}
    function getPlayerById(id){return matchPlayers.find(p=>p.id===id)}
    function keepInCourt(){}function startContactAnimation(){}function clearShotCreationStates(){}
    function flash(){}function showShotFeedback(){}function triggerCrowdReaction(){}function addTakeover(){}function playBlockSound(){}
    function getWorldHand(p,side){return p.hands[side].clone()}
    function make(id,team,x=0,z=0){return {id,teamId:team,group:{position:new THREE.Vector3(x,0,z),rotation:new THREE.Vector3()},
      velocity:new THREE.Vector3(),profile:{height:1,width:1,block:1,jump:1,handle:1,steal:1},bodyRadius:.55,moveMeta:{},
      hands:{left:new THREE.Vector3(-.3,2.4,0),right:new THREE.Vector3(.3,2.4,0)},
      stats:{blocks:0},onGround:true,yVel:0,jumpCooldown:0,blockTime:0,handsUpTime:0,blockBoost:false,sprint:1,angle:0,confidence:0};}
    p1=make('p1',0,0,2);p2=make('p2',1,0,0);matchPlayers=[p1,p2];
    inputManager={vibrate(){}};ball={position:new THREE.Vector3(0,2.4,0)};ballVelocity=new THREE.Vector3(0,3,-7);
    lastShotShooter='p1';ballState='shot';previousBallPosition.copy(ball.position);
  `,ctx);
  return ctx;
}
const run=(ctx,code)=>vm.runInContext(code,ctx);

test('sprint and upgraded block ratings never create contact beyond the visible palm and ball',()=>{
  const ctx=setup();
  const result=run(ctx,`
    p2.blockTime=.2;p2.onGround=false;p2.blockBoost=true;p2.profile.block=2.05;
    p2.hands.left.set(-.68,2.4,0);p2.hands.right.set(.68,2.4,0);
    checkBlocksAndCatches();({state:ballState,blocks:p2.stats.blocks,contact:!!getShotBlockContact(p2,p1,ball.position,ball.position)});
  `);
  assert.equal(result.state,'shot');assert.equal(result.blocks,0);assert.equal(result.contact,false);
});

test('a real exposed hand contact blocks a fast shot even when both segment endpoints miss the palm',()=>{
  const ctx=setup();
  const result=run(ctx,`
    p2.blockTime=.2;p2.onGround=false;p2.hands.left.set(0,2.4,0);p2.hands.right.set(5,2.4,0);
    previousBallPosition.set(-.8,2.4,0);ball.position.set(.8,2.4,0);
    const swept=!!getShotBlockContact(p2,p1,previousBallPosition,ball.position);checkBlocksAndCatches();
    ({swept,state:ballState,blocks:p2.stats.blocks,contactX:ball.position.x});
  `);
  assert.equal(result.swept,true);assert.equal(result.state,'loose');assert.equal(result.blocks,1);assert.equal(result.contactX,0);
});

test('a trailing defender can block an exposed high ball but cannot reach through the finisher torso',()=>{
  const ctx=setup();
  const result=run(ctx,`
    p1.group.position.set(0,0,0);p2.group.position.set(0,0,1);p2.blockTime=.2;p2.onGround=false;
    p2.hands.left.set(.08,1.58,0);p2.hands.right.set(5,2.4,0);
    const low=new THREE.Vector3(.34,1.58,0),protectedBall=!!getShotBlockContact(p2,p1,low,low);
    p2.hands.left.set(.2,2.65,0);const high=new THREE.Vector3(0,2.65,0);
    const exposed=!!getShotBlockContact(p2,p1,high,high);({protectedBall,exposed});
  `);
  assert.equal(result.protectedBall,false);assert.equal(result.exposed,true);
});

test('a torso ricochet with distant hands stays a body deflection and earns no block',()=>{
  const ctx=setup();
  const result=run(ctx,`
    p2.blockTime=.2;p2.onGround=false;p2.hands.left.set(-2,2.6,0);p2.hands.right.set(2,2.6,0);
    ball.position.set(.25,1.5,0);previousBallPosition.copy(ball.position);ballVelocity.set(-2,2,-5);
    checkBallPlayerBodyCollision(p2);({state:ballState,blocks:p2.stats.blocks,speed:ballVelocity.length(),touch:lastTouchPlayerId});
  `);
  assert.equal(result.blocks,0);assert.equal(result.state,'shot');assert.ok(result.speed>0);assert.equal(result.touch,'p2');
});

test('layup gathers ignore phantom long pose sweeps while retaining nearby actual body and hand contact',()=>{
  const ctx=setup();
  const result=run(ctx,`
    p1.group.position.set(0,0,2);p2.group.position.set(0,0,0);p2.hands.left.set(5,2.5,0);p2.hands.right.set(5,2.5,0);
    const low=new THREE.Vector3(0,.25,0),high=new THREE.Vector3(0,3,0);
    const poseJump=checkLayupDefenderCollision(p1,p2,low,high);
    const torso=new THREE.Vector3(.2,1.2,0),wall=checkLayupDefenderCollision(p1,p2,torso,torso);
    p2.blockTime=.2;p2.onGround=false;p2.hands.left.set(.1,3,0);
    const hand=checkLayupDefenderCollision(p1,p2,high,high);
    p2.hands.left.set(.7,3,0);p2.profile.block=2.05;
    const distant=checkLayupDefenderCollision(p1,p2,high,high);
    ({poseJump:poseJump.hit,wall:wall.type,hand:hand.type,distant:distant.hit});
  `);
  assert.equal(result.poseJump,false);assert.equal(result.wall,'body');assert.equal(result.hand,'hand');assert.equal(result.distant,false);
});

test('attached layups never sweep against stale pickup positions from the earlier drive',()=>{
  const ctx=setup();
  const result=run(ctx,`
    layupActive=true;layupOwner='p1';ballState='layup';p2.onGround=false;p2.blockTime=.2;
    p2.hands.left.set(0,2.4,0);p2.hands.right.set(5,2.4,0);
    previousBallPosition.set(-3,2.4,0);ball.position.set(3,2.4,0);checkBlocksAndCatches();
    ({state:ballState,active:layupActive,blocks:p2.stats.blocks});
  `);
  assert.equal(result.state,'layup');assert.equal(result.active,true);assert.equal(result.blocks,0);
});

test('a block leap owns fixed takeoff movement, has bounded travel and a real landing recovery',()=>{
  const ctx=setup();
  const result=run(ctx,`
    p1.group.position.set(8,0,8);p2.group.position.set(0,0,0);p2.profile.jump=1.42;p2.velocity.set(0,0,9);
    tryJumpBlock(p2,true);const owner=activeBlockJumpOwnsMovement(p2),launchSpeed=p2.moveMeta.blockFlight.velocity.length();
    let maxDistance=0,maxBoost=0,frames=0;
    while(!p2.onGround&&frames++<240){
      p2.angle=Math.PI/2;p2.velocity.set(20,0,0);updateJumpPhysics(p2,1/60);
      maxDistance=Math.max(maxDistance,flatDistance(p2.group.position,new THREE.Vector3()));
      maxBoost=Math.max(maxBoost,p2.moveMeta.blockFlight?.boostTravel||0);
    }
    const cooldown=p2.jumpCooldown;tryJumpBlock(p2,true);
    const immediateJump=!p2.onGround;updateJumpPhysics(p2,.19);tryJumpBlock(p2,false);
    ({owner,launchSpeed,maxDistance,maxBoost,frames,x:p2.group.position.x,z:p2.group.position.z,cooldown,
      immediateJump,laterJump:!p2.onGround,angle:p2.angle});
  `);
  assert.equal(result.owner,true);assert.ok(result.launchSpeed<=1.85);assert.ok(result.maxDistance<=1.801);assert.ok(result.maxBoost<=.5501);
  assert.ok(result.z>1,'committed sprint momentum remains useful');assert.equal(result.x,0,'air input cannot steer');
  assert.equal(result.angle,0);assert.ok(result.cooldown>=.18);assert.equal(result.immediateJump,false);assert.equal(result.laterJump,true);
});

test('a committed block leap cannot pass through a body to recover behind it',()=>{
  const ctx=setup();
  const result=run(ctx,`
    p1.group.position.set(0,0,1.2);p2.group.position.set(0,0,0);p2.velocity.set(0,0,8);tryJumpBlock(p2,true);
    for(let i=0;i<40&&!p2.onGround;i++)updateJumpPhysics(p2,1/60);
    ({z:p2.group.position.z,distance:flatDistance(p1.group.position,p2.group.position)});
  `);
  assert.ok(result.z<.11,JSON.stringify(result));assert.ok(result.distance>=1.099,JSON.stringify(result));
});

test('a real failed airborne pickup preserves the committed jump until landing',()=>{
  const ctx=setup();
  const result=run(ctx,`
    p1.group.position.set(8,0,8);p2.velocity.set(0,0,6);tryJumpBlock(p2,true);updateJumpPhysics(p2,.1);
    ballState='loose';loosePickupDelay=0;ball.position.set(.65,2,0);ballVelocity.set(16,0,0);
    const eligible=canPickupLooseBall(p2),quality=getLooseBallPickupQuality(p2),flight=p2.moveMeta.blockFlight;
    handleLooseBallPickup();const bobble=p2.moveState,retained=p2.moveMeta.blockFlight===flight;
    p2.angle=Math.PI/2;p2.velocity.set(20,0,0);updateJumpPhysics(p2,.1);
    const stillOwns=activeBlockJumpOwnsMovement(p2),angle=p2.angle,x=p2.group.position.x;
    for(let i=0;i<180&&!p2.onGround;i++)updateJumpPhysics(p2,1/60);
    ({eligible,quality,bobble,retained,stillOwns,angle,x,landed:p2.onGround,released:!p2.moveMeta.blockFlight});
  `);
  assert.equal(result.eligible,true);assert.ok(result.quality<.36);assert.equal(result.bobble,'bobble');assert.equal(result.retained,true);
  assert.equal(result.stillOwns,true);assert.equal(result.angle,0);assert.equal(result.x,0);assert.equal(result.landed,true);assert.equal(result.released,true);
});

test('Rim Guardian raises the real block leap by five percent per rank without extending hand contact or lateral travel',()=>{
  const ctx=setup();
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/player-development.js'),'utf8'),ctx);
  const results=run(ctx,`
    p1.group.position.set(8,0,8);
    [0,1,3].map(rank=>{
      p2.group.position.set(0,0,0);p2.onGround=true;p2.jumpCooldown=0;p2.sprint=1;p2.velocity.set(0,0,7);
      p2.profile.ratings={blocking:65};p2.profile.abilities={rimGuardian:rank};
      p2.hands.left.set(.1,2.4,0);p2.hands.right.set(5,2.4,0);ball.position.set(0,2.4,0);
      tryJumpBlock(p2,true);
      const launch=p2.yVel,radius=getShotBlockContact(p2,p1,ball.position,ball.position).radius,maxTravel=p2.moveMeta.blockFlight.maxTravel;
      let peak=0,travel=0;
      for(let i=0;i<1800&&!p2.onGround;i++){
        updateJumpPhysics(p2,1/600);peak=Math.max(peak,p2.group.position.y);travel=Math.max(travel,flatDistance(p2.group.position,new THREE.Vector3()));
      }
      return {rank,launch,peak,radius,maxTravel,travel};
    });
  `);
  for(const result of results){
    assert.ok(Math.abs(result.launch/results[0].launch-Math.sqrt(1+result.rank*.05))<1e-9);
    assert.ok(Math.abs(result.peak/results[0].peak-(1+result.rank*.05))<.002,JSON.stringify(results));
    assert.equal(result.radius,results[0].radius);assert.equal(result.maxTravel,1.8);assert.ok(result.travel<=1.801);
  }
});
