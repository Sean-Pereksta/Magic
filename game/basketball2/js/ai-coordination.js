// These reads refine the existing playbook at a player's normal decision tick.
// They only choose destinations: movement, contact and reaction delays stay shared.
function aiCoordIQ(settings,defense=false){
  return THREE.MathUtils.clamp((defense?settings.defensiveIQ:settings.offensiveIQ??settings.spacingIQ) || 0,0,1);
}

function aiCoordRead(observer,player,settings,horizon=0){
  if(typeof aiReadPosition==="function"){
    const result=aiReadPosition(observer,player,settings).setY(0);
    if(horizon>0){
      const projection=aiReadPosition(observer,player,settings,horizon).setY(0).sub(result);
      const length=projection.length();
      result.addScaledVector(projection,length>1.15?1.15/length:1);
    }
    return result;
  }
  const result=player.group.position.clone().setY(0);
  const projection=player.velocity?.clone().setY(0);
  if(projection){
    const length=projection.length()*horizon;
    result.addScaledVector(projection,horizon*(length>1.15?1.15/length:1));
  }
  return result;
}

function aiCoordLookAhead(settings){
  return THREE.MathUtils.clamp(settings.lookAhead ?? .18,0,.4)*THREE.MathUtils.clamp(settings.courtVision ?? .7,0,1);
}

function aiCoordDistanceToLane(position,start,end){
  const dx=end.x-start.x,dz=end.z-start.z,lengthSq=dx*dx+dz*dz;
  const t=lengthSq>.001?THREE.MathUtils.clamp(((position.x-start.x)*dx+(position.z-start.z)*dz)/lengthSq,0,1):0;
  return Math.hypot(position.x-start.x-t*dx,position.z-start.z-t*dz);
}

function aiCoordLaneSpace(observer,start,end,settings){
  let clearance=8;
  for(const defender of getOpponents(observer)){
    const p=aiCoordRead(observer,defender,settings,aiCoordLookAhead(settings));
    clearance=Math.min(clearance,aiCoordDistanceToLane(p,start,end));
  }
  return clearance;
}

function aiCoordHasRimRole(player,plan){
  if(plan.style==="motion"&&plan.passerId&&plan.passCutUntil>teamAITime){
    const passer=typeof getPlayerById==="function"?getPlayerById(plan.passerId):null;
    if(passer&&!aiIsHuman(passer)) return player.id===plan.passerId;
  }
  const brain=player.aiBrain;
  if(brain&&(brain.role==="cut"||brain.role==="roll")&&brain.roleUntil>teamAITime) return true;
  return plan.actorId===player.id&&(plan.phase==="cut"||plan.phase==="roll");
}

function aiCoordFindReceivingRead(handler,settings,plan){
  const support=aiRoster().filter(player=>player.teamId===handler.teamId&&player!==handler);
  const bots=support.filter(player=>!aiIsHuman(player));
  if(!bots.length) return null;
  const position=handler.group.position,handlerDistance=flatDistance(position,HOOP_POS);
  if(handlerDistance<2.5||handlerDistance>10.5) return null;
  const toward=HOOP_POS.clone().sub(position).setY(0).normalize();
  const velocity=handler.velocity.clone().setY(0),speed=velocity.length();
  const driving=speed>1.1&&velocity.normalize().dot(toward)>.38&&handlerDistance<7.3;
  const pressure=getOpponents(handler).filter(player=>flatDistance(aiCoordRead(handler,player,settings),position)<2.25).length;
  const rimActors=bots.filter(player=>aiCoordHasRimRole(player,plan));
  // A set screen must finish before its player becomes a receiver. Motion also
  // retains its one selected cutter rather than sending a second support inside.
  const setup=plan.style!=="balanced"&&(plan.phase==="setup"||plan.phase==="screen");
  if(setup||!rimActors.length&&(!driving||pressure<1)) return null;
  const candidates=rimActors.length?rimActors:bots.filter(player=>{
    const brain=player.aiBrain;
    return brain&&brain.role!=="screen"&&brain.role!=="screen_setup"&&
      (brain.moveCooldown<=0||plan.receivingRead?.selected?.playerId===player.id);
  });
  let best=null;
  for(const player of candidates){
    if(support.some(other=>other!==player&&(flatDistance(other.group.position,HOOP_POS)<2.85||aiCoordHasRimRole(other,plan)))) continue;
    for(const side of [-1,1]) for(const depth of [1.35,2.05]){
      // Stand beside the cylinder, not on the dribbler's route to the basket.
      const target=new THREE.Vector3(side*(depth<1.5?1.7:1.25),0,HOOP_POS.z+depth);
      const passingDistance=flatDistance(position,target);
      const travel=flatDistance(player.group.position,target);
      if(passingDistance<2||travel>8.5||aiCoordDistanceToLane(target,position,HOOP_POS)<1.05) continue;
      const clearance=aiCoordLaneSpace(handler,position,target,settings);
      const catchSpace=Math.min(8,...getOpponents(handler).map(defender=>flatDistance(aiCoordRead(handler,defender,settings,aiCoordLookAhead(settings)),target)));
      if(clearance<1.0||catchSpace<1.3) continue;
      const weakSide=position.x*side<0?.24:0;
      const value=Math.min(clearance,3)*.75+Math.min(catchSpace,3)*.55-travel*.13+weakSide+(aiCoordHasRimRole(player,plan)?.4:0);
      if(!best||value>best.value||value===best.value&&String(player.id)<String(best.playerId)) best={playerId:player.id,target,value};
    }
  }
  return best;
}

function aiCoordinateOffBall(char,brain,handler,settings,plan){
  const iq=aiCoordIQ(settings);
  if(!handler||handler===char||handler.teamId!==char.teamId||aiIsHuman(char)||iq<.62||!plan) return false;
  if(brain.role==="screen"||brain.role==="screen_setup") return false;
  // Reserve one receiving pocket for the team for a short read interval. A
  // different update order cannot create two cutters on opposite sides.
  if(!plan.receivingRead||plan.receivingRead.handlerId!==handler.id||plan.receivingRead.until<=teamAITime){
    const selected=aiCoordFindReceivingRead(handler,settings,plan);
    plan.receivingRead={handlerId:handler.id,until:teamAITime+.32,selected};
  }
  const selected=plan.receivingRead.selected;
  if(selected?.playerId===char.id){
    const rimRole=aiCoordHasRimRole(char,plan);
    if(!rimRole){brain.role="dump_off";brain.moveCooldown=Math.max(brain.moveCooldown,.85);}
    brain.roleUntil=Math.max(brain.roleUntil,teamAITime+.55);
    brain.target.copy(selected.target);
    brain.sprint=flatDistance(char.group.position,brain.target)>1.6;
    keepInCourt(brain.target);
    return true;
  }
  if(aiCoordHasRimRole(char,plan)) return false;
  const rimDistance=flatDistance(handler.group.position,HOOP_POS);
  const nearDrive=flatDistance(brain.target,handler.group.position)<2.6||
    rimDistance<7&&aiCoordDistanceToLane(brain.target,handler.group.position,HOOP_POS)<1.7;
  const crowdingPocket=selected&&flatDistance(brain.target,selected.target)<3;
  if(!nearDrive&&!crowdingPocket) return false;
  const support=aiRoster().filter(player=>player.teamId===char.teamId&&player!==handler);
  const slot=Math.max(0,support.indexOf(char));
  const side=slot%2===0?-1:1;
  brain.target.set(side*5.8,0,HOOP_POS.z+(handler.group.position.x*side>0?7.0:4.6));
  brain.role="outlet";
  brain.sprint=false;
  keepInCourt(brain.target);
  return true;
}

function aiCoordDriveRead(observer,handler,settings){
  const position=aiCoordRead(observer,handler,settings,aiCoordLookAhead(settings));
  const toward=HOOP_POS.clone().sub(position).setY(0).normalize();
  const velocity=handler.velocity.clone().setY(0),speed=velocity.length();
  return {position,toward,speed,driving:speed>1.25&&velocity.normalize().dot(toward)>.4};
}

function aiCoordinateDefense(char,brain,handler,settings){
  const iq=aiCoordIQ(settings,true);
  if(!handler||handler.teamId===char.teamId||aiIsHuman(char)||iq<.62) return false;
  const assigned=aiAssignedOpponent(char);
  if(!assigned) return false;
  const trap=typeof aiDefensePlans!=="undefined"?aiDefensePlans[char.teamId]?.trap:null;
  if(trap||["help","double","rotate","intercept"].includes(brain.role)) return false;
  const read=aiCoordDriveRead(char,handler,settings);
  if(assigned===handler){
    const rimDistance=flatDistance(read.position,HOOP_POS);
    const charging=typeof shotCharging!=="undefined"&&shotCharging&&chargingPlayer===handler;
    const spacing=read.driving?Math.min(1.65,Math.max(.7,rimDistance*.24)):charging?.8:1.05;
    brain.target.copy(read.position).addScaledVector(read.toward,spacing);
    brain.role=read.driving?"contain":charging?"closeout":"guard";
    brain.sprint=flatDistance(char.group.position,brain.target)>2.6;
    keepInCourt(brain.target);
    return true;
  }
  const receiver=aiCoordRead(char,assigned,settings,aiCoordLookAhead(settings));
  const receiverRimDistance=flatDistance(receiver,HOOP_POS);
  const toReceiver=receiver.clone().sub(handler.group.position).setY(0).normalize();
  const aim=getForward(handler.angle).dot(toReceiver);
  const cutting=assigned.velocity.length()>1.15&&receiverRimDistance<4.1;
  if(receiverRimDistance<3.35||cutting){
    brain.target.copy(receiver).addScaledVector(HOOP_POS.clone().sub(receiver).setY(0).normalize(),.8);
    brain.role="tag";
  }else if(aim>.82&&receiverRimDistance<9.2){
    // Close the likely catch from the basket side; don't jump on an unseen pass.
    brain.target.copy(receiver).addScaledVector(HOOP_POS.clone().sub(receiver).setY(0).normalize(),.95);
    brain.role="deny";
  }else{
    // One nearby weak-side defender can show in a driving gap, without chasing
    // the ball or abandoning an existing rim receiver or high-value shooter.
    const defenders=aiRoster().filter(player=>player.teamId===char.teamId);
    const primary=defenders.find(player=>aiAssignedOpponent(player)===handler);
    // Use the primary's read for the shared helper choice. Independent small
    // perception errors must not let two defenders each elect themselves.
    const helpRead=primary?aiCoordDriveRead(primary,handler,settings):read;
    const rimDistance=flatDistance(helpRead.position,HOOP_POS);
    const beaten=primary&&flatDistance(primary.group.position,HOOP_POS)>rimDistance+.45;
    const gap=helpRead.position.clone().addScaledVector(helpRead.toward,1.9);
    const helpers=defenders.filter(player=>{
      const opponent=aiAssignedOpponent(player);
      return !aiIsHuman(player)&&opponent&&opponent!==handler&&flatDistance(opponent.group.position,HOOP_POS)>4.2&&
        (opponent.profile.shot || 1)<=1.12&&flatDistance(player.group.position,gap)<4.0;
    }).sort((a,b)=>flatDistance(a.group.position,gap)-flatDistance(b.group.position,gap)||String(a.id).localeCompare(String(b.id)));
    if(iq<.8||!helpRead.driving||!beaten||rimDistance>6.5||rimDistance<3.3||helpers[0]!==char) return false;
    const helpTarget=brain.target.clone().lerp(gap,.42);
    if(flatDistance(helpTarget,read.position)<1.85) return false;
    brain.target.copy(helpTarget);
    brain.role="gap_help";
  }
  brain.sprint=flatDistance(char.group.position,brain.target)>2.6;
  keepInCourt(brain.target);
  return true;
}
