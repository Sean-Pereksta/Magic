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
  add(v){this.x+=v.x;this.y+=v.y;this.z+=v.z;return this;}
  sub(v){this.x-=v.x;this.y-=v.y;this.z-=v.z;return this;}
  addScaledVector(v,s){this.x+=v.x*s;this.y+=v.y*s;this.z+=v.z*s;return this;}
  multiplyScalar(s){this.x*=s;this.y*=s;this.z*=s;return this;}
  divideScalar(s){return this.multiplyScalar(1/s);}
  lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z;}
  length(){return Math.sqrt(this.lengthSq());}
  normalize(){return this.divideScalar(this.length()||1);}
  dot(v){return this.x*v.x+this.y*v.y+this.z*v.z;}
  lerp(v,t){this.x+=(v.x-this.x)*t;this.y+=(v.y-this.y)*t;this.z+=(v.z-this.z)*t;return this;}
}
function player(id,teamId,x,z,controllerSlot=null){
  return {id,teamId,controllerSlot,profile:{name:id,speed:1,sprint:1,shot:1,handle:1,width:1,block:1,height:1},
    group:{position:new Vector3(x,0,z),rotation:{y:0}},angle:Math.PI,velocity:new Vector3(),
    movement:{stopSpeed:1,recoverySpeed:1},moveMeta:{},anim:{},
    moveState:'normal',stamina:1,sprint:1,onGround:true,stumbledTimer:0,reachRecovery:0,
    pumpFakeCooldown:0,sideHopCooldown:0,plantTimer:0,hasBall:false};
}
function harness(size=3,cooperative=false){
  const random=Object.create(Math);random.random=()=>.99;
  const sandbox={Math:random,THREE:{Vector3,MathUtils:{clamp:(n,a,b)=>Math.max(a,Math.min(b,n)),lerp:(a,b,t)=>a+(b-a)*t,randFloat:(a,b)=>(a+b)/2}},
    gameStarted:true,gameOver:false,gamePaused:false,possessionFrozen:false,countdownTimer:0,
    ballState:'dribble',shotCharging:false,chargingPlayer:null,shotPower:0,botBrain:null,
    HOOP_POS:new Vector3(0,3.28,-10.8),ball:{position:new Vector3(0,1,3)},ballVelocity:new Vector3(),passFlight:{active:false},
    layupActive:false,dunkActive:false,layupOwner:null,dunkOwner:null,ballSideValue:1,
    selectedDifficulty:'normal',SPRINT_MIN:.12,SPRINT_DRAIN:.62,SPRINT_RECOVER:.24,
    moveCalls:[],passCalls:[],shotCalls:[],blockCalls:[],stealCalls:[],
    getGameplayTuning:()=>({lateralSpeed:1,sprintSpeed:1,reversePenalty:.48,acceleration:1,deceleration:1,staminaDrain:1}),
    setAnimationState(){},updateMoveState(){},raiseHands(){},punishBadReach(){},startPumpFake(){},attemptCrossover(){},
    chooseShotType:()=> 'setshot',calculateShotContest:()=>({value:0}),
    getShotMeterProfile:()=>({greenStart:.58,greenEnd:.69,meterFillSpeed:1}),
    getForward:angle=>new Vector3(Math.sin(angle),0,Math.cos(angle)),
    flatDistance:(a,b)=>Math.hypot(a.x-b.x,a.z-b.z),
    keepInCourt(v){v.x=Math.max(-8,Math.min(8,v.x));v.z=Math.max(-12,Math.min(12,v.z));},
    lerpAngle(a,b,t){return a+Math.atan2(Math.sin(b-a),Math.cos(b-a))*t;}
  };
  const ctx=vm.createContext(sandbox);
  const state=fs.readFileSync(path.join(__dirname,'../js/state.js'),'utf8');
  vm.runInContext(state.slice(state.indexOf('const difficultySettings ='),state.indexOf('const botPersonalities ='))+'\nglobalThis.difficultySettings=difficultySettings;',ctx);
  ctx.p1=player('p1',0,0,3,'p1');
  ctx.p2=player('p2',cooperative?0:1,cooperative?5:0,cooperative?-3:1,cooperative?'p2':null);
  ctx.matchPlayers=[ctx.p1,ctx.p2];
  for(let team=0;team<2;team++){
    while(ctx.matchPlayers.filter(p=>p.teamId===team).length<size){
      const index=ctx.matchPlayers.filter(p=>p.teamId===team).length;
      ctx.matchPlayers.push(player(`team${team}_${index}`,team,index%2?-5:5,-3-index));
    }
  }
  ctx.possession=ctx.p1.id;ctx.ballOwner=ctx.p1.id;
  ctx.getBallHandler=()=>ctx.matchPlayers.find(p=>p.id===(ctx.ballOwner||ctx.possession));
  ctx.getPlayerById=id=>ctx.matchPlayers.find(p=>p.id===id);
  ctx.getTeammates=char=>ctx.matchPlayers.filter(p=>p!==char&&p.teamId===char.teamId);
  ctx.getOpponents=char=>ctx.matchPlayers.filter(p=>p.teamId!==char.teamId);
  ctx.getNearestOpponent=char=>ctx.getOpponents(char).reduce((best,p)=>!best||ctx.flatDistance(p.group.position,char.group.position)<ctx.flatDistance(best.group.position,char.group.position)?p:best,null);
  ctx.getBetweenPlayerAndRimTarget=(defender,offense,spacing)=>offense.group.position.clone().addScaledVector(ctx.HOOP_POS.clone().sub(offense.group.position).setY(0).normalize(),spacing);
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/input-gameplay.js'),'utf8'),ctx);
  const actualMove=ctx.updateModernPlayerControl;
  ctx.updateModernPlayerControl=(char,input,dt)=>{ctx.moveCalls.push(char.id);actualMove(char,input,dt);};
  ctx.handleActionButton=char=>{if(ctx.shotCharging) return;ctx.shotCharging=true;ctx.chargingPlayer=char;char.moveMeta.chargeHeld=0;};
  ctx.releaseShootOrFake=char=>{ctx.shotCalls.push(char.id);ctx.shotCharging=false;ctx.chargingPlayer=null;ctx.ballState='shot';};
  ctx.tryDirectionalPass=char=>{ctx.passCalls.push({id:char.id,angle:char.angle});ctx.ballState='pass';return true;};
  ctx.tryJumpBlock=char=>ctx.blockCalls.push(char.id);
  ctx.handleHandAction=char=>ctx.stealCalls.push(char.id);
  for(const file of ['ai-reads.js','ai-coordination.js','ai.js']) vm.runInContext(fs.readFileSync(path.join(__dirname,'../js',file),'utf8'),ctx);
  vm.runInContext('initBotBrain()',ctx);
  return ctx;
}

test('solo and cooperative 1v1/2v2/3v3 update every bot exactly once and leave human slots alone',()=>{
  for(const size of [1,2,3]) for(const cooperative of [false,true]){
    if(size===1&&cooperative) continue;
    const ctx=harness(size,cooperative);
    ctx.updateTeamAI(1/60);
    assert.deepEqual(ctx.moveCalls.sort(),ctx.matchPlayers.filter(p=>!p.controllerSlot).map(p=>p.id).sort());
    assert.equal(ctx.p1.group.position.z,3);
    if(cooperative) assert.equal(ctx.p2.group.position.x,5);
  }
});

test('3v3 offense uses separate wings and defense assigns one unique opponent to each player',()=>{
  const ctx=harness();
  const wings=ctx.getTeammates(ctx.p1);
  for(const wing of wings) ctx.decideAIOffBall(wing,wing.aiBrain,ctx.p1,ctx.difficultySettings.normal);
  assert.ok(wings[0].aiBrain.target.x*wings[1].aiBrain.target.x<0);
  assert.ok(Math.abs(wings[0].aiBrain.target.x-wings[1].aiBrain.target.x)>8);
  const defenders=ctx.getOpponents(ctx.p1);
  const assignments=defenders.map(p=>ctx.aiAssignedOpponent(p).id);
  assert.equal(new Set(assignments).size,3);
});

test('only one teammate cuts at a time, and a completed screen rolls toward the basket',()=>{
  const ctx=harness();ctx.Math.random=()=>0;
  ctx.p2.group.position.set(0,0,-2);
  const wings=ctx.getTeammates(ctx.p1);
  for(const wing of wings){wing.aiBrain.moveCooldown=0;ctx.decideAIOffBall(wing,wing.aiBrain,ctx.p1,ctx.difficultySettings.hard);}
  assert.equal(wings.filter(p=>p.aiBrain.role==='cut').length,1);
  const screener=wings[1];screener.aiBrain.role='screen';screener.aiBrain.roleUntil=0;
  ctx.decideAIOffBall(screener,screener.aiBrain,ctx.p1,ctx.difficultySettings.hard);
  assert.equal(screener.aiBrain.role,'roll');
  assert.ok(ctx.flatDistance(screener.aiBrain.target,ctx.HOOP_POS)<3.2);
});

test('AI physical speed and stamina do not change with difficulty or a legacy speed argument',()=>{
  const snapshots=[];
  for(const difficulty of ['rookie','normal','impossible']){
    const ctx=harness(1);ctx.selectedDifficulty=difficulty;
    for(let i=0;i<60;i++) ctx.moveBotToward(ctx.p2,new Vector3(7,0,-5),1000,1/60);
    snapshots.push([ctx.p2.group.position.x,ctx.p2.group.position.z,ctx.p2.stamina,ctx.p2.sprint]);
  }
  assert.deepEqual(snapshots[0],snapshots[1]);assert.deepEqual(snapshots[1],snapshots[2]);
});

test('a pressured handler chooses an open teammate and visibly turns before a directional pass',()=>{
  const ctx=harness(2);
  const handler=ctx.getTeammates(ctx.p1)[0];
  handler.group.position.set(0,0,-4);handler.angle=Math.PI;
  ctx.p1.group.position.set(5,0,-5);
  ctx.p2.group.position.set(0,0,-4.9);
  const other=ctx.getOpponents(handler).find(p=>p!==ctx.p2);other.group.position.set(-6,0,2);
  ctx.possession=handler.id;ctx.ballOwner=handler.id;
  handler.aiBrain.possessionAge=2;handler.aiBrain.actionCooldown=0;
  ctx.decideAIHandler(handler,handler.aiBrain,ctx.difficultySettings.normal);
  assert.equal(handler.aiBrain.passTargetId,'p1');
  ctx.updateAIShotAndPass(handler,handler.aiBrain,handler,1/60);
  assert.equal(ctx.passCalls.length,0);
  for(let i=0;i<30&&ctx.ballState==='dribble';i++) ctx.updateAIShotAndPass(handler,handler.aiBrain,handler,1/60);
  assert.equal(ctx.passCalls.length,1);
  const toward=ctx.p1.group.position.clone().sub(handler.group.position).normalize();
  assert.ok(ctx.getForward(ctx.passCalls[0].angle).dot(toward)>.97);
});

test('AI shots charge and release the current teammate owner, with equal timing variation across difficulties',()=>{
  const releaseTimes=[];
  for(const difficulty of ['rookie','impossible']){
    const ctx=harness(3,true);ctx.selectedDifficulty=difficulty;
    const shooter=ctx.matchPlayers.find(p=>p.teamId===0&&!p.controllerSlot);
    ctx.possession=shooter.id;ctx.ballOwner=shooter.id;
    ctx.beginAIShot(shooter,shooter.aiBrain);
    releaseTimes.push(shooter.aiBrain.releaseAt);
    assert.equal(ctx.chargingPlayer,shooter);
    shooter.moveMeta.chargeHeld=shooter.aiBrain.releaseAt-.02;
    ctx.updateAIShotAndPass(shooter,shooter.aiBrain,shooter,1/60);
    assert.equal(ctx.shotCalls.length,0);
    shooter.moveMeta.chargeHeld+=.03;
    ctx.updateAIShotAndPass(shooter,shooter.aiBrain,shooter,1/60);
    assert.deepEqual(ctx.shotCalls,[shooter.id]);
    assert.equal(ctx.shotCharging,false);
  }
  assert.equal(releaseTimes[0],releaseTimes[1]);
});

test('rebound roles select one chaser per team and retain outlet or box-out coverage',()=>{
  const ctx=harness();ctx.ballState='loose';ctx.ball.position.set(0,1,-8);ctx.ballVelocity.set(0,-1,0);
  for(const p of ctx.matchPlayers){ctx.decideAIRebound(p,p.aiBrain,ctx.difficultySettings.normal);}
  for(const team of [0,1]){
    const players=ctx.matchPlayers.filter(p=>p.teamId===team);
    assert.equal(players.filter(p=>p.aiBrain.role==='rebound').length,1);
    assert.ok(players.filter(p=>p.aiBrain.role==='outlet'||p.aiBrain.role==='box_out').length===2);
  }
});

test('help defense protects a beaten paint matchup and returns to its assignment after the drive leaves',()=>{
  const ctx=harness();
  ctx.p1.group.position.set(0,0,-8);ctx.p2.group.position.set(6,0,1);
  const helpers=ctx.getOpponents(ctx.p1).filter(p=>p!==ctx.p2);
  helpers[0].group.position.set(-2,0,-8);helpers[1].group.position.set(5,0,-2);
  ctx.decideAIDefense(helpers[0],helpers[0].aiBrain,ctx.p1,ctx.difficultySettings.hard);
  assert.equal(helpers[0].aiBrain.role,'help');
  ctx.p1.group.position.set(0,0,2);
  ctx.decideAIDefense(helpers[0],helpers[0].aiBrain,ctx.p1,ctx.difficultySettings.hard);
  assert.equal(helpers[0].aiBrain.role,'guard');
});

test('an open bot finish uses a short running gather instead of holding a jumper meter',()=>{
  const ctx=harness(1);ctx.selectedDifficulty='impossible';
  ctx.p2.group.position.set(0,0,ctx.HOOP_POS.z+3.2);ctx.p2.velocity.set(0,0,-3);
  ctx.p1.group.position.set(8,0,5);ctx.ballOwner=ctx.p2.id;ctx.possession=ctx.p2.id;
  ctx.p2.aiBrain.possessionAge=1;ctx.p2.aiBrain.actionCooldown=0;
  ctx.chooseShotType=()=> 'layup';
  ctx.decideAIHandler(ctx.p2,ctx.p2.aiBrain,ctx.difficultySettings.impossible);
  assert.equal(ctx.p2.aiBrain.decision,'finish');assert.equal(ctx.chargingPlayer,ctx.p2);
  assert.ok(ctx.p2.aiBrain.releaseAt<=.12);
  assert.ok(ctx.p2.aiBrain.target.z<ctx.p2.group.position.z,'retain the rimward running approach');
});

test('the AI finish owner leaves gather movement to the finish solver',()=>{
  const ctx=harness(1);ctx.ballOwner=ctx.p2.id;ctx.possession=ctx.p2.id;
  ctx.ballState='layup';ctx.layupActive=true;ctx.layupOwner=ctx.p2.id;
  const before=ctx.p2.group.position.clone();ctx.updateTeamAI(1/60);
  assert.ok(!ctx.moveCalls.includes(ctx.p2.id),'do not chase the ball held in the shooter own gather');
  assert.equal(ctx.p2.aiBrain.role,'finish');assert.equal(ctx.p2.group.position.z,before.z);
});

test('defenders continue contesting a live layup gather instead of dispatching to rebounds',()=>{
  const ctx=harness(2);ctx.selectedDifficulty='impossible';
  const defender=ctx.getTeammates(ctx.p1)[0];
  ctx.p2.group.position.set(0,0,ctx.HOOP_POS.z+2.5);
  defender.group.position.set(.9,0,ctx.HOOP_POS.z+1.2);defender.aiBrain.actionCooldown=0;
  ctx.ballState='layup';ctx.layupActive=true;ctx.layupOwner=ctx.p2.id;
  ctx.decideTeamPlayer(defender,defender.aiBrain,ctx.p2,ctx.difficultySettings.impossible,false);
  assert.notEqual(defender.aiBrain.role,'rebound');assert.ok(ctx.blockCalls.includes(defender.id));
});

test('a planned pass is cancelled if the receiver becomes covered before release',()=>{
  const ctx=harness(2);ctx.selectedDifficulty='impossible';
  const handler=ctx.getTeammates(ctx.p1)[0];
  handler.group.position.set(0,0,-4);ctx.p1.group.position.set(4,0,-7);
  ctx.p2.group.position.copy(ctx.p1.group.position).add(new Vector3(.25,0,0));
  ctx.ballOwner=handler.id;ctx.possession=handler.id;
  handler.aiBrain.passTargetId=ctx.p1.id;handler.aiBrain.passUntil=1;
  ctx.updateAIShotAndPass(handler,handler.aiBrain,handler,1/60);
  assert.equal(ctx.passCalls.length,0);assert.equal(handler.aiBrain.passTargetId,null);
  assert.equal(handler.aiBrain.thinkIn,0,'read the new opening at the next decision opportunity');
});

test('scripted setup routes also avoid occupied bodies when the paint is closed',()=>{
  const ctx=harness(3);ctx.selectedDifficulty='impossible';ctx.teamOffensiveStyles=['motion','balanced'];
  const handler=ctx.getTeammates(ctx.p1)[0];
  handler.group.position.set(0,0,ctx.HOOP_POS.z+10);ctx.ballOwner=handler.id;ctx.possession=handler.id;
  ctx.getTeammates(handler).forEach((char,index)=>char.group.position.set(5,0,ctx.HOOP_POS.z+9-index*2));
  const opponents=ctx.getOpponents(handler);
  opponents[0].group.position.set(1.35,0,ctx.HOOP_POS.z+8.8);
  opponents[1].group.position.set(0,0,ctx.HOOP_POS.z+1.7);
  opponents[2].group.position.set(-4,0,ctx.HOOP_POS.z+4.5);
  handler.aiBrain.actionCooldown=.5;handler.aiBrain.possessionAge=1;
  const plan=ctx.aiGetTeamPlan(handler);assert.equal(plan.phase,'setup');
  ctx.decideAIHandler(handler,handler.aiBrain,ctx.difficultySettings.impossible);
  assert.equal(ctx.aiRouteBlocked(handler,handler.aiBrain.target),false);
  assert.equal(handler.aiBrain.sprint,false);
});

test('a clear direct drive is kept over a lateral midrange pass, then kicked out when help closes it',()=>{
  const ctx=harness(2);ctx.selectedDifficulty='impossible';
  const handler=ctx.getTeammates(ctx.p1)[0];
  handler.group.position.set(0,0,ctx.HOOP_POS.z+6);
  ctx.p1.group.position.set(4,0,ctx.HOOP_POS.z+4.5);ctx.p1.profile.shot=1.3;
  ctx.getOpponents(handler).forEach((char,index)=>char.group.position.set(index?8:-8,0,ctx.HOOP_POS.z+10));
  ctx.ballOwner=handler.id;ctx.possession=handler.id;
  handler.aiBrain.actionCooldown=0;handler.aiBrain.possessionAge=1;
  ctx.decideAIHandler(handler,handler.aiBrain,ctx.difficultySettings.impossible);
  assert.equal(handler.aiBrain.decision,'drive');assert.equal(handler.aiBrain.passTargetId,null);
  assert.ok(handler.aiBrain.target.z<handler.group.position.z);
  ctx.getOpponents(handler)[0].group.position.set(0,0,ctx.HOOP_POS.z+3.3);
  ctx.decideAIHandler(handler,handler.aiBrain,ctx.difficultySettings.impossible);
  assert.equal(handler.aiBrain.decision,'pass');assert.equal(handler.aiBrain.passTargetId,ctx.p1.id);
});
