// Every AI player uses the human acceleration, stamina, collision and shot
// systems. Difficulty changes perception and decisions, never physical stats.
let teamAITime=0;
const OFFENSIVE_STYLES={
  balanced:{label:"Balanced",description:"Read the defense, space the wings, and mix cuts with ball screens."},
  motion:{label:"Motion",description:"Pass and cut, rotate into empty spots, and keep a weak-side outlet."},
  pick_roll:{label:"Pick & Roll",description:"Set a ball screen, turn the corner, then find the roller or weak-side wing."},
  drive_kick:{label:"Drive & Kick",description:"Spread the floor, attack a lane, and kick to an open outlet when help arrives."}
};
let aiTeamPlans=Object.create(null);
let aiDefensePlans=Object.create(null);
let aiActiveOffenseTeam=null;
let aiWasFrozen=false;

function createTeamBrain(char){
  return {
    target:char.group.position.clone(),role:"space",roleUntil:0,thinkIn:Math.random()*.18,
    actionCooldown:.5,moveCooldown:1,passTargetId:null,passUntil:0,
    possessionAge:0,lastHandler:null,releaseAt:0,driveSide:Math.random()<.5?-1:1,
    assignedId:null,protect:false,sprint:false,fakeBiteChance:.25,
    recentMoves:[],preferredDriveSide:1,decision:"setup",driveUntil:0,shotKind:null,
    input:{moveX:0,moveY:0,sprint:false,protect:false,source:"ai",worldDirection:new THREE.Vector3()}
  };
}

function initBotBrain(){
  teamAITime=0;
  aiTeamPlans=Object.create(null);
  aiDefensePlans=Object.create(null);
  aiActiveOffenseTeam=null;
  aiWasFrozen=false;
  (matchPlayers.length?matchPlayers:[p1,p2]).filter(Boolean).forEach(char=>{
    char.aiBrain=createTeamBrain(char);
    char.aiInput=char.aiBrain.input;
    char.aiScreen=false;
  });
  botBrain=p2?.aiBrain||null;
  configureBotBrainFromDifficulty();
}

function configureBotBrainFromDifficulty(){
  if(botBrain) botBrain.fakeBiteChance=(difficultySettings[selectedDifficulty]||difficultySettings.normal).fakeBiteChance;
}

function aiRoster(){ return matchPlayers.length?matchPlayers:[p1,p2].filter(Boolean); }
function aiIsHuman(char){ return !!char.controllerSlot; }
function aiSettings(){ return difficultySettings[selectedDifficulty]||difficultySettings.normal; }

function aiOffensiveStyle(teamId){
  const selected=typeof teamOffensiveStyles!=="undefined"?teamOffensiveStyles[teamId]:"balanced";
  return OFFENSIVE_STYLES[selected]?selected:"balanced";
}

function aiNewTeamPlan(handler,previous){
  const style=aiOffensiveStyle(handler.teamId);
  const sameTeamPass=previous&&previous.handlerId!==handler.id&&getPlayerById(previous.handlerId)?.teamId===handler.teamId;
  return {teamId:handler.teamId,handlerId:handler.id,style,age:0,phase:"setup",phaseAge:0,cycle:0,
    actorId:null,actorSide:1,screenSide:handler.group.position.x>0?-1:1,screenTarget:null,
    passerId:sameTeamPass?previous.handlerId:null,passCutUntil:sameTeamPass?teamAITime+1.75:0};
}

function aiGetTeamPlan(handler){
  let plan=aiTeamPlans[handler.teamId];
  if(!plan||plan.handlerId!==handler.id||plan.style!==aiOffensiveStyle(handler.teamId)){
    plan=aiTeamPlans[handler.teamId]=aiNewTeamPlan(handler,plan);
  }
  const support=aiRoster().filter(player=>player.teamId===handler.teamId&&player!==handler&&!aiIsHuman(player));
  if(!support.length){ plan.phase="solo";plan.actorId=null;return plan; }
  if(!support.some(player=>player.id===plan.actorId)){
    const actor=support[plan.cycle%support.length];
    plan.actorId=actor.id;plan.actorSide=actor.group.position.x<0?-1:1;
  }
  if(plan.style==="pick_roll"&&!plan.screenTarget){
    const defender=getNearestOpponent(handler);
    const toRim=HOOP_POS.clone().sub(handler.group.position).setY(0).normalize();
    const actor=getPlayerById(plan.actorId);
    const sidePosition=actor.group.position.clone().sub(handler.group.position);
    plan.screenSide=sidePosition.x*toRim.z-sidePosition.z*toRim.x>=0?1:-1;
    plan.screenTarget=(defender?defender.group.position:handler.group.position).clone();
    plan.screenTarget.x+=toRim.z*plan.screenSide*.8;
    plan.screenTarget.z-=toRim.x*plan.screenSide*.8;
    plan.screenTarget.y=0;
    keepInCourt(plan.screenTarget);
  }
  return plan;
}

function aiAdvancePlay(plan,next){ plan.phase=next;plan.phaseAge=0; }

function updateAITactics(handler,dt,frozen){
  if(frozen){
    if(!aiWasFrozen){
      aiTeamPlans=Object.create(null);aiActiveOffenseTeam=null;
      for(const plan of Object.values(aiDefensePlans)){
        if(plan.trap) aiReleaseTrap(plan);
        plan.handlerAge=0;plan.lastTime=-1;
      }
      for(const player of aiRoster()) if(player.aiBrain){
        player.aiBrain.possessionAge=0;player.aiBrain.passTargetId=null;player.aiBrain.thinkIn=0;
      }
    }
    aiWasFrozen=true;
    return;
  }
  aiWasFrozen=false;
  const passer=ballState==="pass"&&passFlight?.active?getPlayerById(passFlight.passerId):null;
  const offense=passer||handler;
  if(offense&&(ballState==="dribble"||passer)){
    if(aiActiveOffenseTeam!==null&&aiActiveOffenseTeam!==offense.teamId) aiTeamPlans=Object.create(null);
    aiActiveOffenseTeam=offense.teamId;
    const plan=aiGetTeamPlan(offense);
    plan.age+=dt;plan.phaseAge+=dt;
    if(passer&&plan.passerId!==passer.id){plan.passerId=passer.id;plan.passCutUntil=teamAITime+1.75;}
    if(plan.phase!=="solo"){
      if(plan.style==="motion"){
        if(plan.phase==="setup"&&plan.phaseAge>.55) aiAdvancePlay(plan,"cut");
        else if(plan.phase==="cut"&&plan.phaseAge>1.5) aiAdvancePlay(plan,"fill");
        else if(plan.phase==="fill"&&plan.phaseAge>1.2){plan.cycle++;plan.actorId=null;aiAdvancePlay(plan,"setup");}
      }else if(plan.style==="pick_roll"){
        const screener=getPlayerById(plan.actorId);
        const ready=screener&&flatDistance(screener.group.position,plan.screenTarget)<.85;
        if(plan.phase==="setup"&&(ready&&plan.phaseAge>.4||plan.phaseAge>2.8)) aiAdvancePlay(plan,"screen");
        else if(plan.phase==="screen"&&plan.phaseAge>1.05) aiAdvancePlay(plan,"roll");
        else if(plan.phase==="roll"&&plan.phaseAge>1.75) aiAdvancePlay(plan,"reset");
        else if(plan.phase==="reset"&&plan.phaseAge>.85){plan.cycle++;plan.actorId=null;plan.screenTarget=null;aiAdvancePlay(plan,"setup");}
      }else if(plan.style==="drive_kick"){
        const pressure=getOpponents(offense).filter(player=>flatDistance(player.group.position,offense.group.position)<2.5).length;
        if(plan.phase==="setup"&&plan.phaseAge>.5) aiAdvancePlay(plan,"drive");
        else if(plan.phase==="drive"&&(pressure>1||plan.phaseAge>2.5)) aiAdvancePlay(plan,"kick");
        else if(plan.phase==="kick"&&plan.phaseAge>1.3){plan.screenSide*=-1;plan.cycle++;aiAdvancePlay(plan,"setup");}
      }
    }
  }else{
    // A rebound starts a fresh play. Retain a plan only while its pass is live.
    if(aiActiveOffenseTeam!==null) aiTeamPlans=Object.create(null);
    aiActiveOffenseTeam=null;
  }
  for(const teamId of new Set(aiRoster().map(player=>player.teamId))){
    if(handler&&handler.teamId!==teamId&&ballState==="dribble") aiGetDefensePlan(teamId,handler,aiSettings(),dt);
    else if(aiDefensePlans[teamId]?.trap) aiReleaseTrap(aiDefensePlans[teamId]);
  }
}

function aiReleaseTrap(plan){
  plan.trap=null;
  plan.cooldownUntil=teamAITime+2.8;
  aiRoster().filter(player=>player.teamId===plan.teamId&&!aiIsHuman(player)&&player.aiBrain).forEach(player=>{player.aiBrain.thinkIn=0;});
}

function aiTrapSpace(handler,primary){
  const position=handler.group.position;
  const rimDistance=flatDistance(position,HOOP_POS);
  const halfWidth=typeof COURT_W!=="undefined"?COURT_W/2:9;
  if(Math.abs(position.x)>halfWidth-2.6&&rimDistance<10) return "sideline";
  if(rimDistance<4.6&&(!primary||flatDistance(primary.group.position,position)>2.2)) return "paint";
  return null;
}

function aiGetDefensePlan(teamId,handler,settings,dt=0){
  let plan=aiDefensePlans[teamId];
  if(!plan) plan=aiDefensePlans[teamId]={teamId,handlerId:handler.id,handlerAge:0,cooldownUntil:0,trap:null,lastTime:-1};
  if(plan.handlerId!==handler.id){
    if(plan.trap) aiReleaseTrap(plan);
    plan.handlerId=handler.id;plan.handlerAge=0;
  }
  const defense=aiRoster().filter(player=>player.teamId===teamId);
  const primary=defense.find(player=>aiAssignedOpponent(player)===handler);
  const space=aiTrapSpace(handler,primary);
  if(plan.trap){
    const humanHelp=defense.some(player=>player.id!==plan.trap.primaryId&&aiIsHuman(player)&&flatDistance(player.group.position,handler.group.position)<2.6);
    if(ballState!=="dribble"||teamAITime>=plan.trap.until||!space||humanHelp||flatDistance(handler.group.position,plan.trap.origin)>3.1){
      aiReleaseTrap(plan);
    }
    return plan;
  }
  // One shared decision per frame, regardless of roster/update order.
  if(plan.lastTime===teamAITime) return plan;
  plan.lastTime=teamAITime;plan.handlerAge+=dt;
  if(ballState!=="dribble"||defense.length<2||!space||teamAITime<plan.cooldownUntil||settings.defensiveIQ<=.42) return plan;
  const exposed=handler.dribbleStyle==="sprint"||handler.dribbleStyle==="crossover"||handler.moveState==="sidehop";
  const stalled=handler.velocity.length()<.8&&plan.handlerAge>1.4;
  const toRim=HOOP_POS.clone().sub(handler.group.position).setY(0).normalize();
  const driving=handler.velocity.length()>1.5&&handler.velocity.clone().setY(0).normalize().dot(toRim)>.65;
  if(space!=="paint"&&!exposed&&!stalled&&!driving) return plan;
  // A human already helping occupies the second spot; never send a third body.
  if(defense.some(player=>player!==primary&&aiIsHuman(player)&&flatDistance(player.group.position,handler.group.position)<2.6)) return plan;
  let helper=null,best=Infinity;
  for(const defender of defense){
    if(defender===primary||aiIsHuman(defender)) continue;
    const distance=flatDistance(defender.group.position,handler.group.position);
    if(distance>5.8) continue;
    const assignment=aiAssignedOpponent(defender);
    const threat=assignment?flatDistance(assignment.group.position,HOOP_POS):20;
    // Don't abandon an open rim finisher to trap a harmless sideline dribble.
    if(space!=="paint"&&threat<3.2) continue;
    const value=distance+(threat<5?3:0)+(assignment?.profile.shot>1.12?1:0);
    if(value<best){best=value;helper=defender;}
  }
  if(helper){
    plan.trap={handlerId:handler.id,primaryId:primary?.id,helperId:helper.id,space,
      origin:handler.group.position.clone(),until:teamAITime+(space==="paint"?1.35:1.9)};
    defense.filter(player=>!aiIsHuman(player)&&player.aiBrain).forEach(player=>{player.aiBrain.thinkIn=0;});
  }
  return plan;
}

function updateTeamAI(dt,options={}){
  if(!gameStarted||gameOver||gamePaused) return;
  teamAITime+=dt;
  const roster=aiRoster();
  const settings=aiSettings();
  const handler=getBallHandler();
  const frozen=options.countdown||possessionFrozen;
  updateAITactics(handler,dt,frozen);
  for(const char of roster){
    if(aiIsHuman(char)) { char.aiScreen=false; continue; }
    const brain=char.aiBrain||(char.aiBrain=createTeamBrain(char));
    char.aiInput=brain.input;
    brain.thinkIn-=dt;
    brain.actionCooldown=Math.max(0,brain.actionCooldown-dt);
    brain.moveCooldown=Math.max(0,brain.moveCooldown-dt);
    if(brain.lastHandler!==(handler?.id||null)){
      brain.lastHandler=handler?.id||null;
      brain.possessionAge=0;
      brain.thinkIn=0;
      brain.passTargetId=null;
      brain.driveUntil=0;
    }else if(handler===char&&ballState==="dribble"&&!frozen){
      brain.possessionAge+=dt;
    }
    // Gather movement belongs to the finish solver, not a rebound chase after
    // the shooter's own hand. Preserve the running takeoff's real velocity.
    if(!frozen&&(layupActive&&layupOwner===char.id||dunkActive&&dunkOwner===char.id)){
      brain.role="finish";brain.decision="finish";
      brain.input.moveX=0;brain.input.moveY=0;brain.input.sprint=false;
      continue;
    }
    if(brain.thinkIn<=0){
      // A visible reaction delay remains even at the top difficulty.
      brain.thinkIn=THREE.MathUtils.clamp(THREE.MathUtils.randFloat(settings.reactionMin,settings.reactionMax),.075,.7);
      decideTeamPlayer(char,brain,handler,settings,frozen);
    }
    char.aiScreen=brain.role==="screen"&&brain.roleUntil>teamAITime&&char.onGround&&char.moveState==="normal"&&char.velocity.length()<1&&flatDistance(char.group.position,brain.target)<.65;
    moveAIPlayer(char,brain,dt);
    if(frozen) continue;
    updateAIShotAndPass(char,brain,handler,dt);
  }
}

function decideTeamPlayer(char,brain,handler,settings,frozen){
  brain.protect=false;
  brain.sprint=false;
  if(frozen){
    brain.role="ready";
    brain.target.copy(char.group.position);
    if(handler&&handler.teamId!==char.teamId){
      const assigned=aiAssignedOpponent(char);
      if(assigned) brain.target.copy(getBetweenPlayerAndRimTarget(char,assigned,2.5));
    }
    return;
  }
  if(ballState==="layup"||ballState==="dunk"){
    const finisher=getPlayerById(ballState==="layup"?layupOwner:dunkOwner);
    if(finisher&&finisher.teamId!==char.teamId) decideAIDefense(char,brain,finisher,settings);
    else decideAIRebound(char,brain,settings);
    return;
  }
  if(ballState==="loose"||ballState==="shot"){
    decideAIRebound(char,brain,settings);
    return;
  }
  if(ballState==="pass"&&passFlight){
    if(passFlight.targetId===char.id){
      brain.role="receive";
      // Keep the planned cut, but stop overrunning a pass once it is nearby.
      if(flatDistance(char.group.position,ball.position)<2.2) brain.target.copy(ball.position).setY(0);
      brain.sprint=flatDistance(char.group.position,ball.position)>3;
      return;
    }
    const passer=getPlayerById(passFlight.passerId);
    if(passer?.teamId!==char.teamId){
      const receiver=getPlayerById(passFlight.targetId);
      const reach=flatDistance(char.group.position,ball.position);
      const interceptor=aiRoster().filter(player=>player.teamId===char.teamId).reduce((best,player)=>{
        const distance=flatDistance(player.group.position,ball.position);
        const value=distance-(aiAssignedOpponent(player)===receiver ? .45 : 0);
        return !best||value<best.value?{player,value}:best;
      },null)?.player;
      if(receiver&&reach<3.6&&interceptor===char){
        brain.role="intercept";
        brain.target.copy(ball.position).addScaledVector(ballVelocity,.16).setY(0);
        brain.sprint=reach>1.5;
        return;
      }
    }
    if(passer?.teamId===char.teamId) decideAIOffBall(char,brain,passer,settings);
    else decideAIDefense(char,brain,passer,settings);
    return;
  }
  if(!handler){ brain.target.copy(char.group.position); return; }
  if(handler===char) decideAIHandler(char,brain,settings);
  else if(handler.teamId===char.teamId) decideAIOffBall(char,brain,handler,settings);
  else decideAIDefense(char,brain,handler,settings);
}

function moveAIPlayer(char,brain,dt){
  const input=brain.input;
  input.worldDirection.copy(brain.target).sub(char.group.position).setY(0);
  const distance=input.worldDirection.length();
  const amount=char.aiScreen?0:THREE.MathUtils.clamp((distance-.22)/1.2,0,1);
  if(distance>.001) input.worldDirection.divideScalar(distance);
  input.moveX=amount;
  input.moveY=0;
  input.protect=brain.protect;
  input.sprint=brain.sprint&&char.stamina>.32&&char.sprint>.2&&distance>1.2;
  updateModernPlayerControl(char,input,dt);
}

function aiAssignedOpponent(char){
  const roster=aiRoster();
  const team=roster.filter(player=>player.teamId===char.teamId);
  const opponents=getOpponents(char);
  if(!opponents.length) return null;
  const assigned=opponents[team.indexOf(char)%opponents.length];
  if(char.aiBrain) char.aiBrain.assignedId=assigned.id;
  return assigned;
}

function aiNearestDistance(char){
  const opponent=getNearestOpponent(char);
  return opponent?flatDistance(char.group.position,opponent.group.position):8;
}

function aiShotQuality(char){
  return aiAssessShot(char,aiSettings()).quality;
}

function aiPassLaneClearance(passer,receiver){
  const a=passer.group.position,b=receiver.group.position;
  const dx=b.x-a.x,dz=b.z-a.z;
  const lengthSq=dx*dx+dz*dz;
  if(lengthSq<.1) return 0;
  let clearance=8;
  for(const defender of getOpponents(passer)){
    const p=defender.group.position;
    const t=THREE.MathUtils.clamp(((p.x-a.x)*dx+(p.z-a.z)*dz)/lengthSq,0,1);
    if(t<.1||t>.94) continue;
    clearance=Math.min(clearance,Math.hypot(p.x-a.x-t*dx,p.z-a.z-t*dz));
  }
  return clearance;
}

function decideAIHandler(char,brain,settings){
  brain.role="handle";
  const plan=aiGetTeamPlan(char);
  const position=char.group.position;
  const read=aiAssessShot(char,settings);
  const lane=aiFindDriveLane(char,settings);
  const {distance,space,quality}=read;
  const shotValue=read.value??quality;
  const vision=settings.courtVision??settings.spacingIQ;
  const toRim=HOOP_POS.clone().sub(position).setY(0).normalize();
  const committed=brain.driveUntil>teamAITime||char.velocity.dot(toRim)>2.2;
  const driveValue=aiDriveOpportunity(char,lane,distance);
  const drivePreferred=lane.drive&&distance>2.5&&(committed||driveValue>shotValue+.025||!read.inRange);
  const clearRun=lane.direct&&lane.openness>.82&&lane.finishSpace>2.65&&!lane.blocked;
  // A running finish retains its approach; a jumper first settles its feet.
  if(shotCharging&&chargingPlayer===char){
    if(brain.shotKind==="layup"||brain.shotKind==="dunk") brain.target.copy(position).addScaledVector(toRim,2);
    else brain.target.copy(position);
    return;
  }
  if(brain.passTargetId&&brain.passUntil>teamAITime){ brain.target.copy(position);return; }
  let receiver=null,bestRead=null,bestValue=-Infinity;
  for(const teammate of getTeammates(char)){
    const pass=aiAssessPass(char,teammate,settings);
    if(!pass.safe) continue;
    const receivingAction=teammate.aiBrain?.role==="dump_off"||teammate.id===plan.actorId&&(plan.phase==="roll"||plan.phase==="cut");
    const actionBonus=receivingAction ? .14 : plan.style==="drive_kick"&&plan.phase==="kick" ? .1 : 0;
    const value=pass.value+actionBonus+(teammate.controllerSlot?.length ? .025 : 0);
    if(value>bestValue){bestValue=value;receiver=teammate;bestRead=pass;}
  }
  const ready=brain.actionCooldown<=0&&brain.possessionAge>(settings.quickReadTime??.4);
  // Take the easy basket immediately rather than waiting for a scripted play.
  if(ready&&read.closeFinish&&(read.finishOpen||read.finishFavorable)){
    brain.decision="finish";brain.role="finish";
    brain.finishRead=read.finishFavorable&&!read.finishOpen?"size_advantage":"open";
    brain.target.copy(position).addScaledVector(toRim,2);
    beginAIShot(char,brain);
    return;
  }
  const patient=brain.possessionAge>(settings.quickReadTime??.4);
  const rimFeed=bestRead?.rimFeed&&vision>.42&&bestValue>shotValue-.04;
  const share=brain.possessionAge>2.2&&bestValue>shotValue+.02;
  const kickRead=plan.style==="drive_kick"&&plan.phase==="kick"&&bestValue>shotValue-.05;
  const passImprovesDrive=bestRead?.rimFeed||bestRead?.points===3&&bestRead.catchQuality>.82&&bestValue>driveValue+.06||bestValue>driveValue+.16;
  const smartPass=receiver&&patient&&(!drivePreferred||!clearRun||passImprovesDrive)&&
    (rimFeed||bestValue>shotValue+(settings.passAdvantage??.1)||space<1.5&&bestValue>.48||share||kickRead);
  if(smartPass&&brain.actionCooldown<=0){
    brain.passTargetId=receiver.id;
    brain.passUntil=teamAITime+.8;
    brain.decision="pass";brain.driveUntil=0;
    brain.target.copy(position);
    return;
  }
  if(drivePreferred){
    brain.decision="drive";brain.role="drive";brain.target.copy(lane.target);
    brain.driveUntil=teamAITime+.35+(settings.driveCommitment??.65)*.45;
    brain.sprint=distance>3.7&&char.stamina>.45;
    brain.protect=space<1.25;
    keepInCourt(brain.target);
    return;
  }
  const openEnough=space>THREE.MathUtils.lerp(1.25,2.05,settings.shotDiscipline);
  const qualityEnough=quality>THREE.MathUtils.lerp(.52,.76,settings.shotDiscipline);
  const forced=brain.possessionAge>10&&space>.85&&quality>.3;
  const runningAction=plan.style!=="balanced"&&plan.phase==="setup"&&plan.age<2.8;
  if(ready&&read.inRange&&((!runningAction&&openEnough&&qualityEnough)||forced)){
    brain.decision="shoot";
    brain.target.copy(position);
    if(char.velocity.length()<1.6||forced) beginAIShot(char,brain);
    return;
  }
  brain.decision="setup";brain.driveUntil=0;
  brain.target.copy(lane.setupTarget||lane.target);
  const coordinated=aiPlayHandlerTarget(char,brain,plan,toRim);
  // An occupied paint is a reason to create an angle, not to sprint into it.
  if(!lane.drive&&coordinated&&(plan.phase!=="setup"||aiRouteBlocked(char,brain.target))){
    brain.target.copy(lane.setupTarget||lane.target);
  }
  if(space<2.35){
    const side=brain.driveSide;
    // Use the same crossover action and cooldowns as a human.
    if(brain.moveCooldown<=0&&space<1.6&&Math.random()<settings.crossoverUse*.55){
      attemptCrossover(char,side<0?"left":"right");
      brain.moveCooldown=1.25;
      brain.driveSide*=-1;
    }else if(brain.moveCooldown<=0&&space<1.5&&distance<4.5&&Math.random()<settings.pumpFakeUse*.4){
      startPumpFake(char);
      brain.moveCooldown=1.5;
    }
  }
  // A tired handler slows down and shares rather than receiving a speed boost.
  brain.sprint=lane.drive&&space>1.7&&distance>3.7&&char.stamina>.45;
  brain.protect=space<1.25;
  keepInCourt(brain.target);
}

function aiDriveOpportunity(char,lane,distance){
  const finishing=.81+THREE.MathUtils.clamp(((char.profile.layup||1)-1)*.16,-.08,.12);
  const directBonus=lane.direct ? .05 : 0;
  const fatigue=(1-(char.stamina??1))*.1;
  const travel=Math.max(0,distance-7)*.02;
  return THREE.MathUtils.clamp((finishing+directBonus-fatigue-travel)*lane.openness,0,1);
}

function aiRouteBlocked(char,target){
  for(const other of aiRoster()){
    if(other===char) continue;
    const corridor=aiReadSegment(other.group.position,char.group.position,target);
    const radius=(char.bodyRadius||.55)+(other.bodyRadius||.55)+.1;
    if(corridor.t>.06&&corridor.distance<radius) return true;
  }
  return false;
}

function aiPlayHandlerTarget(char,brain,plan,toRim){
  if(plan.phase==="solo"||plan.style==="balanced") return false;
  if(plan.style==="pick_roll"&&plan.screenTarget){
    const side=plan.screenSide;
    if(plan.phase==="setup"){
      brain.target.copy(plan.screenTarget).addScaledVector(toRim,-1.6);
      brain.target.x+=toRim.z*side*.6;brain.target.z-=toRim.x*side*.6;
    }else if(plan.phase==="screen"){
      brain.target.copy(plan.screenTarget).addScaledVector(toRim,2.1);
      brain.target.x+=toRim.z*side*1.35;brain.target.z-=toRim.x*side*1.35;
    }else if(plan.phase==="roll"){
      brain.target.set(-side*2.4,0,HOOP_POS.z+3.5);
    }
    return true;
  }
  if(plan.style==="drive_kick"){
    if(plan.phase==="setup"||plan.phase==="kick") brain.target.set(plan.screenSide*1.7,0,HOOP_POS.z+7.1);
    else brain.target.set(plan.screenSide*2.65,0,HOOP_POS.z+2.8);
    return true;
  }
  if(plan.style==="motion"){
    brain.target.set(plan.actorSide*2.7,0,HOOP_POS.z+(plan.phase==="cut"?6.2:7.6));
    return true;
  }
  return false;
}

function beginAIShot(char,brain){
  handleActionButton(char);
  if(!shotCharging||chargingPlayer!==char) return;
  const type=chooseShotType(char);
  brain.shotKind=type;
  if(type==="layup"||type==="dunk"){
    // Use the same tap/gather action available to humans. Holding a full
    // jumper meter here used to turn an open running layup into a stop shot.
    brain.releaseAt=.10;
    brain.actionCooldown=.65;
    return;
  }
  const profile=getShotMeterProfile(type,char,calculateShotContest(char));
  // Identical release variation at every difficulty; no guaranteed greens.
  const center=(profile.greenStart+profile.greenEnd)*.5;
  brain.releaseAt=Math.max(.25,(center+(Math.random()-.5)*.18)*1.35/profile.meterFillSpeed);
  brain.actionCooldown=.9;
}

function updateAIShotAndPass(char,brain,handler,dt){
  if(shotCharging&&chargingPlayer===char){
    const desired=Math.atan2(HOOP_POS.x-char.group.position.x,HOOP_POS.z-char.group.position.z);
    char.angle=lerpAngle(char.angle,desired,1-Math.exp(-dt*13));char.group.rotation.y=char.angle;
    if((char.moveMeta.chargeHeld||0)>=brain.releaseAt){
      releaseShootOrFake(char);
      brain.releaseAt=0;
      brain.possessionAge=0;
    }
    return;
  }
  if(!brain.passTargetId||handler!==char||ballState!=="dribble") return;
  const receiver=getPlayerById(brain.passTargetId);
  if(!receiver||receiver.teamId!==char.teamId||brain.passUntil<teamAITime||!aiAssessPass(char,receiver,aiSettings()).safe){
    brain.passTargetId=null;brain.thinkIn=0;return;
  }
  const desired=Math.atan2(receiver.group.position.x-char.group.position.x,receiver.group.position.z-char.group.position.z);
  char.angle=lerpAngle(char.angle,desired,1-Math.exp(-dt*13));
  char.group.rotation.y=char.angle;
  const direction=getForward(char.angle);
  const targetDirection=brain.input.worldDirection.copy(receiver.group.position).sub(char.group.position).setY(0).normalize();
  if(direction.dot(targetDirection)>.97){
    const passed=tryDirectionalPass(char,receiver);
    brain.passTargetId=null;
    brain.actionCooldown=passed ? .48 : 0;
    brain.possessionAge=0;
  }
}

function decideAIOffBall(char,brain,handler,settings){
  const plan=aiGetTeamPlan(handler);
  if(plan.style==="balanced"){
    decideAIBalancedOffBall(char,brain,handler,settings);
    aiCoordinateOffBall(char,brain,handler,settings,plan);
    return;
  }
  const support=aiRoster().filter(player=>player.teamId===char.teamId&&player!==handler);
  const slot=Math.max(0,support.indexOf(char));
  const side=slot%2===0?-1:1;
  const activePasser=getPlayerById(plan.passerId);
  const passCut=plan.style==="motion"&&activePasser&&!aiIsHuman(activePasser)&&plan.passCutUntil>teamAITime;
  brain.role="space";brain.roleUntil=0;
  brain.target.set(side*(support.length>1?5.4:4.9),0,HOOP_POS.z+(slot===1?5.2:7.8));
  if(passCut&&plan.passerId===char.id){
    brain.role="cut";brain.roleUntil=plan.passCutUntil;
    brain.target.set(char.group.position.x<0?1.2:-1.2,0,HOOP_POS.z+2.1);brain.sprint=true;
  }else if(plan.style==="motion"){
    if(char.id===plan.actorId&&plan.phase==="cut"&&!passCut){
      brain.role="cut";brain.roleUntil=teamAITime+.6;
      brain.target.set(-side*1.1,0,HOOP_POS.z+2.2);brain.sprint=true;
    }else if(plan.phase==="fill"||passCut){
      brain.role="fill";
      brain.target.set(side*5.3,0,HOOP_POS.z+8.5);
    }
  }else if(plan.style==="pick_roll"){
    if(char.id===plan.actorId){
      if(plan.phase==="setup"||plan.phase==="screen"){
        brain.role=plan.phase==="screen"?"screen":"screen_setup";brain.roleUntil=teamAITime+.6;
        brain.target.copy(plan.screenTarget);
      }else if(plan.phase==="roll"){
        brain.role="roll";brain.roleUntil=teamAITime+.6;
        brain.target.set(plan.screenSide*1.25,0,HOOP_POS.z+2.1);brain.sprint=true;
      }
    }else{
      brain.target.set(plan.screenSide*5.9,0,HOOP_POS.z+4.2);
    }
  }else if(plan.style==="drive_kick"){
    brain.role="outlet";
    const weakSide=-plan.screenSide;
    brain.target.set((slot===0?weakSide:plan.screenSide)*6,0,HOOP_POS.z+(slot===0?3.1:6.6));
    // When the drive crosses the outlet, lift behind it to keep a clear lane.
    if(flatDistance(handler.group.position,brain.target)<3) brain.target.z=handler.group.position.z+3.8;
  }
  aiCoordinateOffBall(char,brain,handler,settings,plan);
  keepInCourt(brain.target);
}

function decideAIBalancedOffBall(char,brain,handler,settings){
  const teammates=getTeammates(char);
  const otherRoles=teammates.filter(player=>player!==handler&&player.aiBrain);
  const cutters=otherRoles.some(player=>player.aiBrain.role==="cut"&&player.aiBrain.roleUntil>teamAITime);
  const screeners=otherRoles.some(player=>player.aiBrain.role==="screen"&&player.aiBrain.roleUntil>teamAITime);
  const position=char.group.position;
  const team=aiRoster().filter(player=>player.teamId===char.teamId);
  const slot=team.filter(player=>player!==handler).indexOf(char);
  const side=slot===0?-1:1;
  const spacing=team.length===3?5.1:4.5;
  const defender=getNearestOpponent(char);
  const handlerPressure=aiNearestDistance(handler);
  if(brain.roleUntil>teamAITime&&(brain.role==="cut"||brain.role==="screen"||brain.role==="roll")){
    if(brain.role==="screen"){
      const onBallDefender=getNearestOpponent(handler);
      if(onBallDefender&&flatDistance(position,brain.target)>.8){
        const towardHandler=handler.group.position.clone().sub(onBallDefender.group.position).setY(0).normalize();
        brain.target.copy(onBallDefender.group.position).addScaledVector(towardHandler,-.7);
      }
    }
    brain.sprint=brain.role!=="screen";
    return;
  }
  if(brain.role==="screen"){
    brain.role="roll";
    brain.roleUntil=teamAITime+1.35;
    brain.target.set(side*1.4,0,HOOP_POS.z+2.6);
    brain.sprint=true;
    return;
  }
  brain.role="space";
  brain.target.set(side*spacing,0,HOOP_POS.z+(slot===1?5.5:7.7));
  if(teammates.some(player=>player!==handler&&flatDistance(player.group.position,brain.target)<2)) brain.target.z+=2.2;
  if(handler.group.position.x*side>2) brain.target.z=HOOP_POS.z+8.9;
  // Put the wing outside the defender's shadow to create an actual pass lane.
  if(defender&&aiPassLaneClearance(handler,char)<1.1){
    brain.target.z+=defender.group.position.z>position.z?-1.2:1.2;
    brain.target.x+=side*.65;
  }
  if(!cutters&&brain.moveCooldown<=0&&flatDistance(handler.group.position,HOOP_POS)>4&&handlerPressure>1.2&&Math.random()<.1+settings.spacingIQ*.22){
    brain.role="cut";
    brain.roleUntil=teamAITime+1.35;
    brain.moveCooldown=4.5;
    brain.target.set(-side*1.1,0,HOOP_POS.z+2.2);
    brain.sprint=true;
  }else if(!screeners&&brain.moveCooldown<=0&&handlerPressure<2&&flatDistance(handler.group.position,HOOP_POS)>4.4&&Math.random()<settings.spacingIQ*.2){
    const onBallDefender=getNearestOpponent(handler);
    if(onBallDefender){
      brain.role="screen";
      brain.roleUntil=teamAITime+2;
      brain.moveCooldown=6;
      const direction=handler.group.position.clone().sub(onBallDefender.group.position).setY(0).normalize();
      brain.target.copy(onBallDefender.group.position).addScaledVector(direction,-.7);
    }
  }
  keepInCourt(brain.target);
}

function decideAIDefense(char,brain,handler,settings){
  const assigned=aiAssignedOpponent(char)||handler;
  if(!assigned){ brain.target.copy(char.group.position); return; }
  brain.role="guard";
  brain.protect=true;
  const guardingHandler=assigned===handler;
  const spacing=guardingHandler?THREE.MathUtils.lerp(1.9,1.25,settings.defensiveIQ):1.8;
  brain.target.copy(getBetweenPlayerAndRimTarget(char,assigned,spacing));
  const assignedRimDistance=flatDistance(assigned.group.position,HOOP_POS);
  if(!guardingHandler&&handler){
    const trap=aiGetDefensePlan(char.teamId,handler,settings).trap;
    if(trap&&trap.helperId===char.id){
      brain.role=trap.space==="paint"?"help":"double";
      // Close the escape side while the primary contains the basket side.
      const side=trap.space==="sideline"?(handler.group.position.x>0?-1:1):(char.group.position.x>handler.group.position.x?1:-1);
      brain.target.copy(handler.group.position);
      brain.target.x+=side*1.05;brain.target.z+=trap.space==="paint"?-.3:.25;brain.target.y=0;
    }else if(trap){
      brain.role="rotate";
      const helper=getPlayerById(trap.helperId);
      const uncovered=helper?aiAssignedOpponent(helper):null;
      if(uncovered&&uncovered!==handler){
        const openTarget=getBetweenPlayerAndRimTarget(char,uncovered,1.4);
        const threat=flatDistance(uncovered.group.position,HOOP_POS)<assignedRimDistance?.7:.4;
        brain.target.lerp(openTarget,threat);
        // Remain in the passing lanes, outside the two-player trap pocket.
        if(flatDistance(brain.target,handler.group.position)<2.8){
          brain.target.copy(assigned.group.position).lerp(uncovered.group.position,.5);
          if(flatDistance(brain.target,handler.group.position)<2.8) brain.target.z+=3.1;
        }
      }
    }else{
      // Shade toward visible passing direction; never inspect human input.
      const aim=getForward(handler.angle);
      const toAssigned=assigned.group.position.clone().sub(handler.group.position).setY(0).normalize();
      if(aim.dot(toAssigned)>.83&&assignedRimDistance<8.6){
        brain.target.lerp(assigned.group.position,.3+settings.spacingIQ*.2);
      }
    }
  }
  aiCoordinateDefense(char,brain,handler,settings);
  keepInCourt(brain.target);
  const recovery=flatDistance(char.group.position,brain.target);
  brain.sprint=recovery>2.6;
  if(!handler) return;
  const range=flatDistance(char.group.position,handler.group.position);
  const gathering=shotCharging&&chargingPlayer===handler&&(handler.moveMeta.chargeHeld||0)>.42;
  const finishing=layupActive&&layupOwner===handler.id||dunkActive&&dunkOwner===handler.id;
  const fake=handler.moveState==="pumpfake";
  if(range<2.5&&brain.actionCooldown<=0&&(gathering||finishing||fake&&Math.random()<settings.fakeBiteChance)){
    const jump=finishing||gathering&&Math.random()<.25+settings.contestTiming*.45||fake;
    if(jump&&char.onGround){ tryJumpBlock(char,false); brain.actionCooldown=.8; }
    else raiseHands(char);
  }else if(range<2.7&&(gathering||finishing)){
    raiseHands(char);
  }
  if(range<1.35&&ballState==="dribble"&&brain.actionCooldown<=0){
    const exposed=handler.dribbleStyle==="sprint"||handler.dribbleStyle==="crossover"||handler.moveState==="sidehop";
    const reachMistake=Math.random()<settings.mistakeChance*.12;
    if(exposed&&Math.random()<settings.stealDiscipline*.25||reachMistake){
      handleHandAction(char,ballSideValue<0?"left":"right","ai-steal");
      brain.actionCooldown=.85;
      if(reachMistake) punishBadReach(char);
    }
  }
}

function decideAIRebound(char,brain,settings){
  brain.role="rebound";
  const y=Math.max(.1,ball.position.y-.65);
  const fallTime=THREE.MathUtils.clamp((ballVelocity.y+Math.sqrt(ballVelocity.y*ballVelocity.y+19.6*y))/9.8,0,.8);
  brain.target.copy(ball.position).addScaledVector(ballVelocity,fallTime*.75).setY(0);
  if(ballState!=="loose"&&ball.position.y>HOOP_POS.y+.5) brain.target.lerp(HOOP_POS,.4);
  keepInCourt(brain.target);
  const team=aiRoster().filter(player=>player.teamId===char.teamId);
  const chaser=team.reduce((best,player)=>!best||flatDistance(player.group.position,brain.target)<flatDistance(best.group.position,brain.target)?player:best,null);
  if(chaser!==char){
    const assigned=aiAssignedOpponent(char);
    if(assigned&&flatDistance(assigned.group.position,brain.target)<4.5){
      brain.role="box_out";
      const towardBall=brain.target.clone().sub(assigned.group.position).setY(0).normalize();
      brain.target.copy(assigned.group.position).addScaledVector(towardBall,.85);
      brain.protect=true;
    }else{
      brain.role="outlet";
      brain.target.z=Math.max(brain.target.z+3.2,HOOP_POS.z+6.5);
      brain.target.x=char.group.position.x<0?-4:4;
    }
  }
  brain.sprint=chaser===char;
  const distance=flatDistance(char.group.position,ball.position);
  if(chaser===char&&distance<1.6&&ball.position.y>1.15&&ball.position.y<2.6&&ballVelocity.y<0&&char.onGround&&brain.actionCooldown<=0){
    tryJumpBlock(char,false);
    brain.actionCooldown=.8;
  }
}

// Compatibility for tournament/debug helpers. These still use the common
// movement controller, so legacy callers cannot restore difficulty speed boosts.
function updateBot(dt){ updateTeamAI(dt); }
function moveBotToward(bot,target,speed,dt){
  if(!bot) return;
  const brain=bot.aiBrain||(bot.aiBrain=createTeamBrain(bot));
  brain.target.copy(target);
  brain.sprint=false;
  bot.aiInput=brain.input;
  moveAIPlayer(bot,brain,dt);
}
function getDefensiveTarget(defender,handler,intent="balanced_defense"){
  return getBetweenPlayerAndRimTarget(defender,handler,intent==="pressure_steal"?.85:1.65);
}
function shouldBotJumpContest(defender,shooter,shotType,timingPct){
  return flatDistance(defender.group.position,shooter.group.position)<2.5&&timingPct>.45&&Math.random()<.3+aiSettings().contestTiming*.4;
}
