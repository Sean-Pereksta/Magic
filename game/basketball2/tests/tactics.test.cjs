const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

class Vector3{
  constructor(x=0,y=0,z=0){this.set(x,y,z);}
  set(x,y,z){this.x=x;this.y=y;this.z=z;return this;}
  setY(y){this.y=y;return this;}
  copy(v){return this.set(v.x,v.y,v.z);}
  clone(){return new Vector3(this.x,this.y,this.z);}
  add(v){return this.addScaledVector(v,1);}
  sub(v){return this.addScaledVector(v,-1);}
  addScaledVector(v,s){this.x+=v.x*s;this.y+=v.y*s;this.z+=v.z*s;return this;}
  length(){return Math.hypot(this.x,this.y,this.z);}
  divideScalar(s){this.x/=s;this.y/=s;this.z/=s;return this;}
  normalize(){return this.divideScalar(this.length()||1);}
  dot(v){return this.x*v.x+this.y*v.y+this.z*v.z;}
  lerp(v,t){this.x+=(v.x-this.x)*t;this.y+=(v.y-this.y)*t;this.z+=(v.z-this.z)*t;return this;}
}

function fixture(style='motion',size=3){
  const settings={defensiveIQ:.8,spacingIQ:.8,patience:.7,shotDiscipline:.7,reactionMin:.2,reactionMax:.3,
    crossoverUse:0,pumpFakeUse:0,stealDiscipline:0,mistakeChance:0,fakeBiteChance:0,contestTiming:.7};
  const ctx=vm.createContext({Math,THREE:{Vector3,MathUtils:{clamp:(n,a,b)=>Math.max(a,Math.min(b,n)),lerp:(a,b,t)=>a+(b-a)*t,randFloat:(a,b)=>(a+b)/2}},
    HOOP_POS:new Vector3(0,3.28,-10.8),COURT_W:18,teamOffensiveStyles:[style,'balanced'],
    difficultySettings:{normal:settings},selectedDifficulty:'normal',botBrain:null,
    gameStarted:true,gamePaused:false,gameOver:false,possessionFrozen:false,
    ballState:'dribble',ball:{position:new Vector3()},ballVelocity:new Vector3(),passFlight:{active:false},
    shotCharging:false,chargingPlayer:null,layupActive:false,dunkActive:false,ballSideValue:1,
    flatDistance:(a,b)=>Math.hypot(a.x-b.x,a.z-b.z),
    keepInCourt(v){v.x=Math.max(-8.2,Math.min(8.2,v.x));v.z=Math.max(-12.2,Math.min(12.2,v.z));},
    getForward:a=>new Vector3(Math.sin(a),0,Math.cos(a)),
    lerpAngle:(a,b,t)=>a+(b-a)*t,
    moveCalls:[],updateModernPlayerControl(char){ctx.moveCalls.push(char.id);},
    attemptCrossover(){},startPumpFake(){},handleActionButton(){},tryJumpBlock(){},raiseHands(){},handleHandAction(){},punishBadReach(){}});
  ctx.matchPlayers=[];
  for(let teamId=0;teamId<2;teamId++) for(let slot=0;slot<size;slot++){
    ctx.matchPlayers.push({id:`${teamId}_${slot}`,teamId,controllerSlot:teamId===0&&slot===0?'p1':null,
      group:{position:new Vector3(slot===0?0:slot===1?-5:5,0,slot===0?1:-3),rotation:{y:0}},
      velocity:new Vector3(),angle:Math.PI,profile:{shot:1},moveMeta:{},stamina:1,sprint:1,onGround:true,moveState:'normal'});
  }
  ctx.p1=ctx.matchPlayers[0];ctx.p2=ctx.matchPlayers[size];ctx.owner=ctx.p1;
  ctx.getBallHandler=()=>ctx.owner;
  ctx.getPlayerById=id=>ctx.matchPlayers.find(player=>player.id===id);
  ctx.getTeammates=char=>ctx.matchPlayers.filter(player=>player.teamId===char.teamId&&player!==char);
  ctx.getOpponents=char=>ctx.matchPlayers.filter(player=>player.teamId!==char.teamId);
  ctx.getNearestOpponent=char=>ctx.getOpponents(char).reduce((best,p)=>!best||ctx.flatDistance(p.group.position,char.group.position)<ctx.flatDistance(best.group.position,char.group.position)?p:best,null);
  ctx.getBetweenPlayerAndRimTarget=(defender,offense,spacing)=>offense.group.position.clone().addScaledVector(ctx.HOOP_POS.clone().sub(offense.group.position).setY(0).normalize(),spacing);
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/ai.js'),'utf8'),ctx);
  vm.runInContext('initBotBrain();globalThis.tick=(dt,frozen=false)=>{teamAITime+=dt;updateAITactics(getBallHandler(),dt,frozen);};globalThis.plans=()=>({offense:aiTeamPlans,defense:aiDefensePlans});',ctx);
  ctx.settings=settings;
  ctx.offBall=()=>ctx.getTeammates(ctx.owner).filter(player=>!player.controllerSlot).forEach(player=>ctx.decideAIOffBall(player,player.aiBrain,ctx.owner,settings));
  ctx.defend=(reverse=false)=>{
    const defense=ctx.getOpponents(ctx.owner);
    if(reverse) defense.reverse();
    for(const defender of defense) if(!defender.controllerSlot) ctx.decideAIDefense(defender,defender.aiBrain,ctx.owner,settings);
    return defense;
  };
  return ctx;
}

test('motion assigns one cutter and fills the floor; pick-and-roll coordinates the screen, turn and roll',()=>{
  const motion=fixture('motion');motion.tick(.6);motion.offBall();
  const wings=motion.getTeammates(motion.owner);
  assert.equal(wings.filter(player=>player.aiBrain.role==='cut').length,1);
  assert.equal(wings.filter(player=>player.aiBrain.role==='space').length,1);
  motion.tick(1.6);motion.offBall();
  assert.ok(wings.every(player=>player.aiBrain.role==='fill'));

  const pick=fixture('pick_roll');pick.tick(.01);pick.offBall();
  const plan=pick.plans().offense[0];
  const screener=pick.getPlayerById(plan.actorId);
  assert.equal(screener.aiBrain.role,'screen_setup');
  screener.group.position.copy(plan.screenTarget);
  pick.tick(.45);pick.offBall();
  assert.equal(plan.phase,'screen');assert.equal(screener.aiBrain.role,'screen');
  const handlerBrain=pick.owner.aiBrain;
  const toRim=pick.HOOP_POS.clone().sub(pick.owner.group.position).setY(0).normalize();
  pick.aiPlayHandlerTarget(pick.owner,handlerBrain,plan,toRim);
  const turn=handlerBrain.target.clone().sub(plan.screenTarget);
  assert.ok(turn.dot(toRim)>2,'handler turns beyond the screen toward the rim');
  assert.ok(Math.abs(turn.x*toRim.z-turn.z*toRim.x)>1,'handler goes around the screen side');
  pick.tick(1.1);pick.offBall();
  assert.equal(screener.aiBrain.role,'roll');
  assert.ok(pick.flatDistance(screener.aiBrain.target,pick.HOOP_POS)<3);
  assert.equal(pick.getTeammates(pick.owner).filter(player=>player.aiBrain.role==='roll').length,1);
  assert.ok(pick.getTeammates(pick.owner).some(player=>player.aiBrain.role==='space'&&Math.abs(player.aiBrain.target.x)>5));
});

test('drive-and-kick spreads outlets and changes its shared drive into a kick read when help collapses',()=>{
  const ctx=fixture('drive_kick');ctx.tick(.6);ctx.offBall();
  const plan=ctx.plans().offense[0];assert.equal(plan.phase,'drive');
  const wings=ctx.getTeammates(ctx.owner);
  assert.ok(wings.every(player=>player.aiBrain.role==='outlet'));
  assert.ok(wings[0].aiBrain.target.x*wings[1].aiBrain.target.x<0);
  assert.ok(Math.abs(wings[0].aiBrain.target.z-wings[1].aiBrain.target.z)>3);
  const defenders=ctx.getOpponents(ctx.owner);
  defenders[0].group.position.copy(ctx.owner.group.position).add(new Vector3(-1,0,0));
  defenders[1].group.position.copy(ctx.owner.group.position).add(new Vector3(1,0,0));
  ctx.tick(.1);assert.equal(plan.phase,'kick');
  const target=ctx.owner.aiBrain;
  ctx.aiPlayHandlerTarget(ctx.owner,target,plan,new Vector3(0,0,-1));
  assert.ok(target.target.z>ctx.HOOP_POS.z+6,'handler moves to keep the kick-out lane open');
});

test('a pass retains the selected offense, sends the AI passer on one cut, and turnovers/inbounds start fresh',()=>{
  const ctx=fixture('motion');ctx.p1.controllerSlot=null;
  ctx.tick(.8);const first=ctx.owner;
  const receiver=ctx.getTeammates(first)[0];ctx.owner=receiver;ctx.tick(.1);ctx.offBall();
  let plan=ctx.plans().offense[0];
  assert.equal(plan.style,'motion');assert.equal(plan.passerId,first.id);
  assert.equal(first.aiBrain.role,'cut');
  assert.equal(ctx.getTeammates(receiver).filter(player=>player.aiBrain.role==='cut').length,1);
  ctx.tick(.4,true);assert.equal(Object.keys(ctx.plans().offense).length,0);
  ctx.tick(.1);plan=ctx.plans().offense[0];assert.equal(plan.passerId,null);assert.equal(plan.phase,'setup');
  ctx.owner=ctx.p2;ctx.tick(.1);assert.equal(ctx.plans().offense[0],undefined);
  ctx.owner=receiver;ctx.tick(.1);assert.equal(ctx.plans().offense[0].passerId,null);
});

test('1v1 never invents a screener, cutter or receiver, and human support players are never assigned a scripted action',()=>{
  for(const style of ['motion','pick_roll','drive_kick']){
    const ctx=fixture(style,1);ctx.tick(1);
    assert.equal(ctx.plans().offense[0].phase,'solo');assert.equal(ctx.plans().offense[0].actorId,null);
  }
  const ctx=fixture('pick_roll');const human=ctx.getTeammates(ctx.owner)[0];human.controllerSlot='p2';
  const before=human.group.position.clone();ctx.updateTeamAI(.1);
  assert.notEqual(ctx.plans().offense[0].actorId,human.id);
  assert.equal(ctx.moveCalls.includes(human.id),false);
  assert.deepEqual(human.group.position,before);
});

function sidelineFixture(){
  const ctx=fixture();ctx.owner.group.position.set(7,0,-5);ctx.owner.dribbleStyle='sprint';
  const offense=ctx.getTeammates(ctx.owner);offense[0].group.position.set(-5,0,-3);offense[1].group.position.set(4,0,1);
  const defense=ctx.getOpponents(ctx.owner);
  defense[0].group.position.set(7,0,-6);defense[1].group.position.set(6,0,-4);defense[2].group.position.set(3,0,-5);
  return ctx;
}

test('a sideline trap has one shared helper regardless of decision order, while the third defender covers passing lanes',()=>{
  for(const reverse of [false,true]){
    const ctx=sidelineFixture();ctx.tick(.1);const defense=ctx.defend(reverse);
    const trap=ctx.plans().defense[1].trap;assert.ok(trap);
    assert.equal(defense.filter(player=>['double','help'].includes(player.aiBrain.role)).length,1);
    const rotation=defense.find(player=>player.aiBrain.role==='rotate');assert.ok(rotation);
    assert.ok(ctx.flatDistance(rotation.aiBrain.target,ctx.owner.group.position)>=2.8);
    const primary=ctx.getPlayerById(trap.primaryId),helper=ctx.getPlayerById(trap.helperId);
    assert.ok(ctx.flatDistance(primary.aiBrain.target,helper.aiBrain.target)>1);
    assert.equal(new Set(defense.map(player=>ctx.aiAssignedOpponent(player).id)).size,3);
  }
});

test('traps release on a pass, escape and timeout, recover assignments, and respect a cooldown',()=>{
  for(const release of ['pass','escape','timeout']){
    const ctx=sidelineFixture();ctx.tick(.1);ctx.defend();const plan=ctx.plans().defense[1];
    assert.ok(plan.trap);
    if(release==='pass'){ctx.ballState='pass';ctx.passFlight={active:true,passerId:ctx.owner.id,targetId:ctx.getTeammates(ctx.owner)[0].id};}
    else if(release==='escape') ctx.owner.group.position.x=0;
    ctx.tick(release==='timeout'?2:.1);ctx.defend();
    assert.equal(plan.trap,null);
    assert.ok(ctx.getOpponents(ctx.owner).every(player=>player.aiBrain.role==='guard'));
    ctx.ballState='dribble';ctx.passFlight.active=false;ctx.owner.group.position.x=7;ctx.tick(.2);
    assert.equal(plan.trap,null,'cannot immediately trap the same player again');
    ctx.tick(3);assert.ok(plan.trap,'a later exposed sideline dribble can trigger another trap');
  }
});

test('a human occupying the helper spot prevents an AI third defender, including during an active trap',()=>{
  for(const alreadyActive of [false,true]){
    const ctx=sidelineFixture();if(alreadyActive){ctx.tick(.1);assert.ok(ctx.plans().defense[1].trap);}
    const human=ctx.getOpponents(ctx.owner)[2];human.controllerSlot='p2';human.group.position.set(6,0,-5);
    ctx.tick(.1);ctx.defend();assert.equal(ctx.plans().defense[1].trap,null);
    assert.ok(ctx.getOpponents(ctx.owner).filter(player=>!player.controllerSlot).every(player=>player.aiBrain.role==='guard'));
  }
});

test('a protected center-court handle and an open rim threat do not invite an indiscriminate double',()=>{
  const center=fixture();center.tick(3);center.defend();assert.equal(center.plans().defense[1].trap,null);
  const ctx=sidelineFixture();
  for(const teammate of ctx.getTeammates(ctx.owner)) teammate.group.position.set(0,0,-9);
  ctx.tick(.1);ctx.defend();assert.equal(ctx.plans().defense[1].trap,null);
});

test('one defender attacks an interceptable pass while the others recover their individual coverage',()=>{
  const ctx=sidelineFixture();ctx.ballState='pass';ctx.ball.position.set(6,1,-5);
  ctx.passFlight={active:true,passerId:ctx.owner.id,targetId:ctx.getTeammates(ctx.owner)[0].id};
  const defense=ctx.getOpponents(ctx.owner);
  for(const defender of defense) ctx.decideTeamPlayer(defender,defender.aiBrain,ctx.owner,ctx.settings,false);
  assert.equal(defense.filter(player=>player.aiBrain.role==='intercept').length,1);
  assert.equal(defense.filter(player=>player.aiBrain.role==='guard').length,2);
});
