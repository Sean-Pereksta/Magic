// Every AI player uses the human acceleration, stamina, collision and shot
// systems. Difficulty changes perception and decisions, never physical stats.
let teamAITime=0;

function createTeamBrain(char){
  return {
    target:char.group.position.clone(),role:"space",roleUntil:0,thinkIn:Math.random()*.18,
    actionCooldown:.5,moveCooldown:1,passTargetId:null,passUntil:0,
    possessionAge:0,lastHandler:null,releaseAt:0,driveSide:Math.random()<.5?-1:1,
    assignedId:null,protect:false,sprint:false,fakeBiteChance:.25,
    recentMoves:[],preferredDriveSide:1,
    input:{moveX:0,moveY:0,sprint:false,protect:false,source:"ai",worldDirection:new THREE.Vector3()}
  };
}

function initBotBrain(){
  teamAITime=0;
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

function updateTeamAI(dt,options={}){
  if(!gameStarted||gameOver||gamePaused) return;
  teamAITime+=dt;
  const roster=aiRoster();
  const settings=aiSettings();
  const handler=getBallHandler();
  const frozen=options.countdown||possessionFrozen;
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
    }else if(handler===char&&ballState==="dribble"&&!frozen){
      brain.possessionAge+=dt;
    }
    if(brain.thinkIn<=0){
      // A visible reaction delay remains even at the top difficulty.
      brain.thinkIn=THREE.MathUtils.clamp(THREE.MathUtils.randFloat(settings.reactionMin,settings.reactionMax),.14,.7);
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
  if(ballState==="loose"||ballState==="shot"||ballState==="layup"||ballState==="dunk"){
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
      if(receiver&&reach<3.6){
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
  const distance=flatDistance(char.group.position,HOOP_POS);
  const space=aiNearestDistance(char);
  const shooting=char.profile.shot||1;
  const preferredRange=shooting>1.12?7.5:shooting>.95?5.8:4.2;
  const rangePenalty=Math.max(0,distance-preferredRange)*.11;
  const pressurePenalty=Math.max(0,2.35-space)*.23;
  return .8-rangePenalty-pressurePenalty-Math.min(.18,char.velocity.length()*.025)+(distance<2.7?.13:0);
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
  const position=char.group.position;
  const distance=flatDistance(position,HOOP_POS);
  const nearest=getNearestOpponent(char);
  const space=nearest?flatDistance(position,nearest.group.position):8;
  const quality=aiShotQuality(char);
  let receiver=null,bestValue=-Infinity;
  for(const teammate of getTeammates(char)){
    const lane=aiPassLaneClearance(char,teammate);
    const separation=flatDistance(position,teammate.group.position);
    if(lane<.8||separation<2||separation>15) continue;
    const value=aiShotQuality(teammate)+Math.min(.12,(lane-1)*.05)+(teammate.controllerSlot?.length ? .06 : 0);
    if(value>bestValue){ bestValue=value; receiver=teammate; }
  }
  const patient=brain.possessionAge>.65+settings.patience*.7;
  const share=brain.possessionAge>2.4&&bestValue>quality-.08;
  const smartPass=receiver&&patient&&(bestValue>quality+.1||space<1.5||share);
  if(smartPass&&!shotCharging&&brain.actionCooldown<=0){
    brain.passTargetId=receiver.id;
    brain.passUntil=teamAITime+.65;
    brain.target.copy(position);
    return;
  }
  if(brain.passTargetId&&brain.passUntil>teamAITime){ brain.target.copy(position); return; }
  const openEnough=space>THREE.MathUtils.lerp(1.35,2.05,settings.shotDiscipline);
  const qualityEnough=quality>THREE.MathUtils.lerp(.39,.61,settings.shotDiscipline);
  const inRange=distance<(char.profile.shot>1.12?8.2:6.5);
  const forced=brain.possessionAge>7.5;
  if(!shotCharging&&brain.actionCooldown<=0&&brain.possessionAge>.35&&inRange&&((openEnough&&qualityEnough)||distance<2.5&&space>1.35||forced)){
    brain.target.copy(position);
    beginAIShot(char,brain);
    return;
  }
  if(shotCharging&&chargingPlayer===char){ brain.target.copy(position); return; }
  const toRim=brain.input.worldDirection.copy(HOOP_POS).sub(position).setY(0).normalize();
  brain.target.copy(position).addScaledVector(toRim,3);
  if(nearest&&space<2.35){
    const side=brain.driveSide;
    brain.target.x+=toRim.z*side*2;
    brain.target.z-=toRim.x*side*2;
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
  brain.sprint=space>1.7&&distance>2.8&&char.stamina>.45;
  brain.protect=space<1.25;
  keepInCourt(brain.target);
}

function beginAIShot(char,brain){
  handleActionButton(char);
  if(!shotCharging||chargingPlayer!==char) return;
  const type=chooseShotType(char);
  const profile=getShotMeterProfile(type,char,calculateShotContest(char));
  // Identical release variation at every difficulty; no guaranteed greens.
  const center=(profile.greenStart+profile.greenEnd)*.5;
  brain.releaseAt=Math.max(.25,(center+(Math.random()-.5)*.18)*1.35/profile.meterFillSpeed);
  brain.actionCooldown=.9;
}

function updateAIShotAndPass(char,brain,handler,dt){
  if(shotCharging&&chargingPlayer===char){
    if((char.moveMeta.chargeHeld||0)>=brain.releaseAt){
      releaseShootOrFake(char);
      brain.releaseAt=0;
      brain.possessionAge=0;
    }
    return;
  }
  if(!brain.passTargetId||handler!==char||ballState!=="dribble") return;
  const receiver=getPlayerById(brain.passTargetId);
  if(!receiver||brain.passUntil<teamAITime||aiPassLaneClearance(char,receiver)<.65){ brain.passTargetId=null; return; }
  const desired=Math.atan2(receiver.group.position.x-char.group.position.x,receiver.group.position.z-char.group.position.z);
  char.angle=lerpAngle(char.angle,desired,1-Math.exp(-dt*13));
  char.group.rotation.y=char.angle;
  const direction=getForward(char.angle);
  const targetDirection=brain.input.worldDirection.copy(receiver.group.position).sub(char.group.position).setY(0).normalize();
  if(direction.dot(targetDirection)>.97){
    tryDirectionalPass(char);
    brain.passTargetId=null;
    brain.actionCooldown=.7;
    brain.possessionAge=0;
  }
}

function decideAIOffBall(char,brain,handler,settings){
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
  const handlerRimDistance=handler?flatDistance(handler.group.position,HOOP_POS):20;
  const assignedRimDistance=flatDistance(assigned.group.position,HOOP_POS);
  if(!guardingHandler&&handler){
    const handlerDefender=aiRoster().find(player=>player.teamId===char.teamId&&aiAssignedOpponent(player)===handler);
    const beaten=!handlerDefender||flatDistance(handlerDefender.group.position,handler.group.position)>2.2;
    const helpCandidates=aiRoster().filter(player=>player.teamId===char.teamId&&player!==handlerDefender);
    const closestHelper=helpCandidates.reduce((best,player)=>!best||flatDistance(player.group.position,HOOP_POS)<flatDistance(best.group.position,HOOP_POS)?player:best,null);
    const help=closestHelper===char&&beaten&&handlerRimDistance<4.6&&settings.defensiveIQ>.42;
    if(help){
      brain.role="help";
      brain.target.copy(getBetweenPlayerAndRimTarget(char,handler,1.05));
    }else{
      // Shade toward visible passing direction; never inspect human input.
      const aim=getForward(handler.angle);
      const toAssigned=assigned.group.position.clone().sub(handler.group.position).setY(0).normalize();
      if(aim.dot(toAssigned)>.83&&assignedRimDistance<8.6){
        brain.target.lerp(assigned.group.position,.3+settings.spacingIQ*.2);
      }
    }
  }
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
