// Shared coverage is refreshed at a bounded tactical interval. It only reads
// visible positions, velocities, planted screens and already released actions.
// Destinations still use the common acceleration, collision and stamina solver.
function aiUseTeamDefense(settings){ return (settings?.defensiveIQ || 0)>=.62; }
function aiDefenseDefaultAssignment(char){
  const roster=aiRoster(),team=roster.filter(player=>player.teamId===char.teamId);
  const opponents=roster.filter(player=>player.teamId!==char.teamId);
  return opponents[team.indexOf(char)%opponents.length] || null;
}
function aiDefenseAssignment(char){
  const plan=aiDefensePlans[char.teamId];
  if(!plan?.coverage||plan.coverage.handlerId!==getBallHandler()?.id) return null;
  const id=plan.coverage.assignments[char.id];
  const assigned=id?getPlayerById(id):null;
  if(assigned&&char.aiBrain) char.aiBrain.assignedId=assigned.id;
  return assigned;
}
function aiDefenseMemory(plan,id){
  if(!plan.tendencies) plan.tendencies=Object.create(null);
  return plan.tendencies[id]||(plan.tendencies[id]={left:0,right:0,baseline:0,drives:0,crossovers:0,
    shots:0,outsideShots:0,passes:0,receivers:Object.create(null),passZones:Object.create(null),afterMoveShots:0});
}
function aiDefenseEvent(memory,key,amount=1){
  // A small recent sample can change; a lifetime counter cannot pin a player
  // forever after they deliberately counter the original tendency.
  for(const name of ["left","right","baseline","drives","crossovers","shots","outsideShots","passes","afterMoveShots"])
    memory[name]*=.97;
  for(const values of [memory.receivers,memory.passZones])
    for(const id of Object.keys(values || {})) values[id]*=.97;
  memory[key]+=amount;
}
function aiObserveTeamDefense(handler,dt){
  const roster=aiRoster(),state=typeof ballState!=="undefined"?ballState:"dribble";
  for(const teamId of new Set(roster.map(player=>player.teamId))){
    let plan=aiDefensePlans[teamId];
    const offense=handler&&handler.teamId!==teamId?handler:null;
    if(!plan&&!offense) continue;
    if(!plan) plan=aiDefensePlans[teamId]={teamId,handlerId:offense.id,handlerAge:0,cooldownUntil:0,trap:null,lastTime:-1};
    if(offense&&state==="dribble"){
      const memory=aiDefenseMemory(plan,offense.id),position=offense.group.position;
      const velocity=offense.velocity,speed=Math.hypot(velocity.x,velocity.z);
      let observation=plan.observation;
      if(!observation||observation.handlerId!==offense.id||observation.released){
        observation=plan.observation={handlerId:offense.id,start:position.clone(),age:0,quiet:0,driveCounted:false,baselineCounted:false,
          lastSide:0,lastCross:-99,released:false};
      }
      observation.age+=dt;
      observation.quiet=speed<.75?observation.quiet+dt:0;
      if(observation.quiet>.65){
        observation.start.copy(position);observation.driveCounted=false;observation.baselineCounted=false;
        observation.age=0;observation.lastSide=0;observation.lastCross=-99;
      }
      const side=Math.abs(velocity.x)>1.25?Math.sign(velocity.x):0;
      if(side&&observation.lastSide&&side!==observation.lastSide&&observation.age-observation.lastCross>.32){
        aiDefenseEvent(memory,"crossovers");observation.lastCross=observation.age;
      }
      if(side) observation.lastSide=side;
      const toRim=HOOP_POS.clone().sub(position).setY(0).normalize();
      const progress=velocity.x*toRim.x+velocity.z*toRim.z;
      if(!observation.driveCounted&&observation.age>.18&&speed>1.3&&progress>.45&&
        flatDistance(position,observation.start)>1.0){
        aiDefenseEvent(memory,"drives");
        const lateral=position.x-observation.start.x;
        if(Math.abs(lateral)>.3) memory[lateral>0?"right":"left"]++;
        observation.driveCounted=true;
      }
      if(observation.driveCounted&&!observation.baselineCounted&&position.z<HOOP_POS.z+2.8&&Math.abs(position.x)>2){
        memory.baseline++;observation.baselineCounted=true;
      }
      plan.lastObservedHandler=offense.id;
    }else if(plan.observation&&!plan.observation.released){
      const observed=getPlayerById(plan.observation.handlerId);
      if(!observed) continue;
      const memory=aiDefenseMemory(plan,observed.id);
      if(state==="pass"&&typeof passFlight!=="undefined"&&passFlight?.passerId===observed.id){
        aiDefenseEvent(memory,"passes");
        memory.receivers[passFlight.targetId]=(memory.receivers[passFlight.targetId]||0)+1;
        const zone=(observed.group.position.x<0?"left":"right")+(flatDistance(observed.group.position,HOOP_POS)<5?"_paint":"_wing");
        memory.passZones[zone]=(memory.passZones[zone]||0)+1;
        plan.observation.released=true;
      }else if(["shot","layup","dunk"].includes(state)){
        aiDefenseEvent(memory,"shots");
        if(flatDistance(observed.group.position,HOOP_POS)>6) memory.outsideShots++;
        if(plan.observation.age-plan.observation.lastCross<1.1) memory.afterMoveShots++;
        plan.observation.released=true;
      }else if(state!=="dribble") plan.observation.released=true;
    }
  }
}
function aiDefenseShade(plan,handler,settings){
  const memory=plan.tendencies?.[handler.id];
  if(!memory||settings.defensiveIQ<.82) return 0;
  const samples=memory.left+memory.right;
  if(samples<2.4) return 0;
  return THREE.MathUtils.clamp((memory.right-memory.left)/samples,-1,1)*
    Math.min(.42,samples*.055)*(settings.defensiveIQ-.65)/.35;
}
function aiDefenseRimRead(observer,player,settings){
  return aiCoordRead(observer,player,settings,aiCoordLookAhead(settings));
}
function aiDefenseSeparation(target,other,minDistance,side=1){
  const delta=target.clone().sub(other).setY(0),distance=delta.length();
  if(distance>=minDistance) return target;
  if(distance<.001) delta.set(side,0,0);else delta.normalize();
  target.copy(other).addScaledVector(delta,minDistance);
  return target;
}
function aiDefensePlantedScreen(screener,plan,observe=false){
  if(screener.onGround===false) return false;
  if(screener.aiScreen||!aiIsHuman(screener)&&screener.aiBrain?.role==="screen"&&
    screener.aiBrain.roleUntil>teamAITime&&screener.velocity.length()<.85) return true;
  if(!aiIsHuman(screener)||!plan) return false;
  const observations=plan.screenBodies||(plan.screenBodies=Object.create(null));
  const stationary=screener.velocity.length()<.5&&(!screener.moveState||screener.moveState==="normal");
  if(!stationary){delete observations[screener.id];return false;}
  let body=observations[screener.id];
  if(body&&flatDistance(body.origin,screener.group.position)>.35){delete observations[screener.id];body=null;}
  if(!body&&observe) body=observations[screener.id]={since:teamAITime,origin:screener.group.position.clone()};
  // A human's held position is a real screen after a short visible dwell.
  // Their controls, brain and movement remain entirely human-owned.
  return !!body&&teamAITime-body.since>=.18;
}
function aiDefenseScreen(defense,handler,primary,assignments){
  if(!primary) return null;
  const plan=aiDefensePlans[primary.teamId];
  const opponents=aiRoster().filter(player=>player.teamId===handler.teamId&&player!==handler);
  for(const screener of opponents){
    if(flatDistance(screener.group.position,handler.group.position)>3){
      if(plan?.screenBodies) delete plan.screenBodies[screener.id];continue;
    }
    if(!aiDefensePlantedScreen(screener,plan,true)) continue;
    const obstructs=aiCoordDistanceToLane(screener.group.position,primary.group.position,handler.group.position)<1.25;
    const partner=defense.find(player=>assignments[player.id]===screener.id&&!aiIsHuman(player));
    if(obstructs&&partner&&partner!==primary) return {screener,partner};
  }
  return null;
}
function aiDefenseTrap(plan,defense,primary,handler,assignments,settings){
  const humanHelp=defense.some(player=>player!==primary&&aiIsHuman(player)&&
    flatDistance(player.group.position,handler.group.position)<2.7);
  if(plan.trap){
    const helper=getPlayerById(plan.trap.helperId);
    if(plan.trap.handlerId===handler.id&&plan.trap.primaryId===primary.id&&helper&&!aiIsHuman(helper)&&
      !humanHelp&&teamAITime<plan.trap.until&&flatDistance(handler.group.position,plan.trap.origin)<2.5)
      return plan.trap;
    plan.trap=null;plan.cooldownUntil=teamAITime+3;
  }
  if(settings.defensiveIQ<.82||humanHelp||aiIsHuman(primary)||teamAITime<plan.cooldownUntil||plan.handlerAge<1.15)
    return null;
  const position=handler.group.position,speed=handler.velocity.length();
  const sideline=Math.abs(position.x)>(typeof COURT_W!=="undefined"?COURT_W/2:9)-2.6;
  const memory=plan.tendencies?.[handler.id];
  const predictable=(memory?.passes || 0)>2.5||Math.max(memory?.left || 0,memory?.right || 0)>3;
  // Trap a visible sideline stall or a repeatedly telegraphed sideline move.
  // A clear middle drive still receives staggered containment, not a double.
  if(!sideline||flatDistance(position,HOOP_POS)>10||speed>(predictable?2.7:1.15)) return null;
  const helper=defense.filter(player=>player!==primary&&!aiIsHuman(player)&&
    flatDistance(player.group.position,position)<5.4).filter(player=>{
      const assigned=getPlayerById(assignments[player.id]);
      return assigned&&flatDistance(assigned.group.position,HOOP_POS)>3.6&&(assigned.profile.shot || 1)<1.3;
    }).sort((a,b)=>flatDistance(a.group.position,position)-flatDistance(b.group.position,position)||
      String(a.id).localeCompare(String(b.id)))[0];
  if(!helper) return null;
  return plan.trap={handlerId:handler.id,primaryId:primary.id,helperId:helper.id,space:"sideline",
    origin:position.clone(),until:teamAITime+.95};
}
function aiGetCoordinatedDefensePlan(teamId,handler,settings,dt=0){
  let plan=aiDefensePlans[teamId];
  if(!plan) plan=aiDefensePlans[teamId]={teamId,handlerId:handler.id,handlerAge:0,cooldownUntil:0,trap:null,lastTime:-1};
  // A pass changes the on-ball responsibility while keeping the exchanged
  // matchups from the screen. The new read replaces targets, not assignments.
  if(plan.handlerId!==handler.id){plan.handlerId=handler.id;plan.handlerAge=0;plan.trap=null;}
  if(plan.lastTime!==teamAITime){plan.handlerAge+=dt;plan.lastTime=teamAITime;}
  if(plan.coverage&&plan.coverage.handlerId===handler.id&&plan.coverage.until>teamAITime) return plan;
  const defense=aiRoster().filter(player=>player.teamId===teamId);
  if(!defense.length) return plan;
  const previous=plan.coverage;
  const assignments=Object.create(null);
  for(const defender of defense){
    const old=previous?.assignments[defender.id];
    assignments[defender.id]=(old&&getPlayerById(old)?.teamId!==teamId)?old:aiDefenseDefaultAssignment(defender)?.id;
  }
  let primary=defense.find(player=>assignments[player.id]===handler.id);
  if(!primary) primary=defense[0];
  if(plan.screenSwitch){
    const formerScreen=getPlayerById(plan.screenSwitch.screenerId);
    const planted=formerScreen&&aiDefensePlantedScreen(formerScreen,plan);
    if(!planted||plan.screenSwitch.handlerId!==handler.id||flatDistance(formerScreen.group.position,handler.group.position)>3.5)
      plan.screenSwitch=null;
  }
  const screen=aiDefenseScreen(defense,handler,primary,assignments);
  let switched=false,screenDrop=null;
  if(screen&&!aiIsHuman(primary)&&plan.screenSwitch?.screenerId!==screen.screener.id&&teamAITime>(previous?.switchUntil || 0)){
    const partner=screen.partner;
    // Mobile defenders exchange the two real assignments. A slow center stays
    // below the screen and tags the roll while the guard navigates over it.
    const mobile=(partner.profile.speed || 1)>.95&&(partner.profile.width || 1)<1.2;
    if(mobile&&flatDistance(partner.group.position,handler.group.position)<3.25){
      assignments[primary.id]=screen.screener.id;assignments[partner.id]=handler.id;
      primary=partner;switched=true;plan.screenSwitch={screenerId:screen.screener.id,handlerId:handler.id};
    }else screenDrop=partner;
  }
  const read=aiCoordDriveRead(primary,handler,settings),rimDistance=flatDistance(read.position,HOOP_POS);
  const primaryRim=flatDistance(primary.group.position,HOOP_POS);
  const beaten=read.driving&&primaryRim>rimDistance+.65&&flatDistance(primary.group.position,read.position)>1.25;
  // Handoff only once a real teammate has reached the ball, not just because
  // a projected target looked nearer. The beaten defender recovers to that man.
  if(beaten&&!screen&&!aiIsHuman(primary)&&teamAITime>(previous?.switchUntil || 0)){
    const replacement=defense.filter(player=>player!==primary&&!aiIsHuman(player)&&
      flatDistance(player.group.position,read.position)<2.15&&
      flatDistance(player.group.position,HOOP_POS)<rimDistance+.2)
      .sort((a,b)=>flatDistance(a.group.position,read.position)-flatDistance(b.group.position,read.position)||String(a.id).localeCompare(String(b.id)))[0];
    if(replacement&&flatDistance(primary.group.position,read.position)>flatDistance(replacement.group.position,read.position)+1.0){
      const recover=assignments[replacement.id];assignments[replacement.id]=handler.id;assignments[primary.id]=recover;
      primary=replacement;switched=true;
    }
  }
  const coverage={handlerId:handler.id,until:teamAITime+THREE.MathUtils.clamp(.23-settings.defensiveIQ*.12,.10,.19),
    assignments,primaryId:primary.id,helperId:null,roles:Object.create(null),targets:Object.create(null),
    switchUntil:switched?teamAITime+.9:previous?.switchUntil || 0,screenId:screen?.screener.id || null};
  const primaryRead=aiCoordDriveRead(primary,handler,settings);
  const distance=flatDistance(primaryRead.position,HOOP_POS);
  const charging=typeof shotCharging!=="undefined"&&shotCharging&&chargingPlayer===handler;
  // Hold the basket side and lead visible lateral momentum. Deep containment
  // buys a normal-speed defender time to turn against a sprinting attacker.
  const memory=plan.tendencies?.[handler.id];
  const expectsJumper=(memory?.outsideShots || 0)>2&&(memory.outsideShots/(memory.shots || 1))>.55;
  const cushion=primaryRead.driving?THREE.MathUtils.clamp(distance*.29,.62,2.0):charging?.78:expectsJumper?.9:1.25;
  const primaryTarget=primaryRead.position.clone().addScaledVector(primaryRead.toward,cushion);
  primaryTarget.x+=aiDefenseShade(plan,handler,settings);
  coverage.targets[primary.id]=primaryTarget;
  coverage.roles[primary.id]=primaryRead.driving?"contain":charging?"closeout":"guard";
  const remaining=defense.filter(player=>player!==primary),danger=primaryRead.driving&&distance<8.5;
  const gap=primaryRead.position.clone().addScaledVector(primaryRead.toward,Math.min(3.0,Math.max(1.35,distance*.5)));
  const trap=screen?null:aiDefenseTrap(plan,defense,primary,handler,assignments,settings);
  let helper=trap?getPlayerById(trap.helperId):screenDrop;
  if(!helper&&remaining.length){
    const eligible=remaining.filter(player=>{
      const assigned=getPlayerById(assignments[player.id]);
      // A rim receiver remains that defender's priority. In 2v2 the same
      // player can contain the gap and tag the receiver from the basket side.
      return !aiIsHuman(player)&&assigned&&((assigned.profile.shot || 1)<1.3||beaten||distance<4.3);
    });
    helper=eligible.sort((a,b)=>{
      const value=player=>flatDistance(player.group.position,gap)+(player.profile.block>1.3?-.65:0)+
        Math.max(0,(getPlayerById(assignments[player.id])?.profile.shot || 1)-1)*2;
      return value(a)-value(b)||String(a.id).localeCompare(String(b.id));
    })[0] || null;
  }
  if(helper&&(danger&&(beaten||distance<5.4)||screenDrop||trap)) coverage.helperId=helper.id;
  for(const defender of remaining){
    const assigned=getPlayerById(assignments[defender.id]);
    if(!assigned) continue;
    const receiver=aiDefenseRimRead(primary,assigned,settings),receiverDistance=flatDistance(receiver,HOOP_POS);
    const receiverToward=HOOP_POS.clone().sub(receiver).setY(0).normalize();
    let target=receiver.clone().addScaledVector(receiverToward,.85),role=switched&&previous?.primaryId===defender.id?"recover":"guard";
    if(trap&&trap.helperId===defender.id){
      role="double";
      const side=defender.group.position.x<handler.group.position.x?-1:1;
      target.copy(primaryRead.position).addScaledVector(primaryRead.toward,.15);target.x+=side*1.55;
    }else if(receiverDistance<3.6){role="tag";}
    else if(coverage.helperId===defender.id){
      role=screenDrop===defender?"drop":"rim_help";
      const threat=receiverDistance<5?0.3:0;
      target.copy(gap).lerp(receiver,threat);
      // Help arrives below the on-ball defender, on the helper's own side.
      const side=defender.group.position.x<primaryTarget.x?-1:1;
      aiDefenseSeparation(target,primaryTarget,1.8,side);
      if(target.z>primaryTarget.z-.1&&distance>2.5) target.z=primaryTarget.z-1.6;
    }else{
      const direction=receiver.clone().sub(handler.group.position).setY(0).normalize();
      const memory=plan.tendencies?.[handler.id];
      const favorite=(memory?.receivers[assigned.id] || 0)>=3;
      if(getForward(handler.angle).dot(direction)>.8||favorite){
        role="deny";target.lerp(handler.group.position,.12+(favorite?.06:0));
      }else if((danger||trap)&&remaining.length>1){
        // The third defender splits the helper's man and their own pass lane.
        const helperMan=helper?getPlayerById(assignments[helper.id]):null;
        if(helperMan&&helperMan!==assigned){target.lerp(helperMan.group.position,trap?.36:.22);role="pass_lane";}
      }
    }
    coverage.targets[defender.id]=target;coverage.roles[defender.id]=role;
  }
  // Reserve actual distinct destinations. Priority is ball, rim, then recovery;
  // no roster/update ordering can create three defenders in the same pocket.
  const ordered=[primary,...remaining.filter(player=>player===helper),...remaining.filter(player=>player!==helper)];
  const reserved=[];
  for(const defender of ordered){
    const target=coverage.targets[defender.id];if(!target) continue;
    for(const other of reserved) aiDefenseSeparation(target,other.target,(defender.bodyRadius || .55)+(other.player.bodyRadius || .55)+.42,
      defender.group.position.x<other.target.x?-1:1);
    keepInCourt(target);reserved.push({player:defender,target});
  }
  plan.coverage=coverage;
  return plan;
}
function aiTeamDefense(char,brain,handler,settings){
  if(!handler||handler.teamId===char.teamId||aiIsHuman(char)) return false;
  const plan=aiGetCoordinatedDefensePlan(char.teamId,handler,settings),coverage=plan.coverage;
  const target=coverage?.targets[char.id];
  if(!target) return false;
  brain.target.copy(target);brain.role=coverage.roles[char.id];
  brain.assignedId=coverage.assignments[char.id];brain.defensiveRole=brain.role;
  const travel=flatDistance(char.group.position,target),running=handler.velocity.length()>2.3;
  brain.protect=travel<1.55;
  brain.sprint=travel>1.65&&(running||["recover","closeout","double"].includes(brain.role));
  return true;
}
function aiMovementPriority(char,brain){
  if(aiIsHuman(char)) return 100;
  if(char.aiScreen) return 90;
  if(["contain","guard","closeout"].includes(brain?.role)&&brain?.assignedId===getBallHandler()?.id) return 80;
  if(["rim_help","drop","tag","double"].includes(brain?.role)) return 65;
  if(["handle","drive","finish"].includes(brain?.role)) return 75;
  return 35;
}
function aiSteerTeammates(char,brain,direction){
  if(char.aiScreen||direction.length()<.001) return 1;
  const teammates=aiRoster().filter(other=>other!==char&&other.teamId===char.teamId);
  if(!teammates.length) return 1;
  const origin=char.group.position,desired=direction.clone(),priority=aiMovementPriority(char,brain);
  let amount=1;
  for(const other of teammates){
    const separation=flatDistance(origin,other.group.position),body=(char.bodyRadius || .55)+(other.bodyRadius || .55);
    if(separation>3.0) continue;
    const otherPriority=aiMovementPriority(other,other.aiBrain);
    const yields=priority<otherPriority||priority===otherPriority&&String(char.id)>String(other.id);
    const forecast=other.group.position.clone().addScaledVector(other.velocity,.24);
    const offset=forecast.clone().sub(origin).setY(0),forward=offset.dot(desired);
    const lateral=desired.x*offset.z-desired.z*offset.x;
    if(forward<-.15||forward>2.55||Math.abs(lateral)>body+.42) continue;
    // A deliberate planted screen stays physical. A teammate crossing a route
    // is passed on a stable side; lower-priority support briefly yields speed.
    const side=brain.avoidanceId===other.id?brain.avoidanceSide:
      Math.abs(lateral)>.12?(lateral>0?-1:1):(String(char.id)<String(other.id)?-1:1);
    brain.avoidanceId=other.id;brain.avoidanceSide=side;
    const urgency=THREE.MathUtils.clamp((body+.5-Math.abs(lateral))/(body+.5),0,1)*
      THREE.MathUtils.clamp((2.55-forward)/1.8,0,1);
    const steer=urgency*(yields?.95:.32);
    direction.x+=-desired.z*side*steer;direction.z+=desired.x*side*steer;
    if(yields&&separation<body+.55) amount=Math.min(amount,.42+Math.max(0,separation-body)*.65);
  }
  if(direction.length()>.001) direction.normalize();
  return amount;
}
