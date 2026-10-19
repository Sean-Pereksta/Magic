// Team membership, possession and passes share one authoritative match roster.
function getPlayerById(id){
  if(id && typeof id === "object") return id;
  return matchPlayers.find(player=>player.id===id) || (p1?.id===id?p1:p2?.id===id?p2:null);
}
function teamOf(id){
  const player = getPlayerById(id);
  return player?.teamId ?? (id==="p2"?1:0);
}
function getHumanPlayer(slot){ return slot==="p1"||slot===1?p1:twoPlayerMode?p2:null; }
function getBallHandler(){ return getPlayerById(ballOwner || possession); }
function getTeammates(char){ return matchPlayers.filter(player=>player!==char&&player.teamId===char.teamId); }
function getOpponents(char){ return matchPlayers.filter(player=>player.teamId!==char.teamId); }
function getNearestOpponent(char){
  let nearest=null, distance=Infinity;
  for(const player of matchPlayers){
    if(player.teamId===char.teamId) continue;
    const d=flatDistance(player.group.position,char.group.position);
    if(d<distance){ nearest=player; distance=d; }
  }
  return nearest;
}

function disposeMatchPlayers(){
  const geometries=new Set(), materials=new Set(), textures=new Set();
  for(const player of matchPlayers.length?matchPlayers:[p1,p2].filter(Boolean)){
    if(typeof disposePlayerModel==="function") disposePlayerModel(player);
    scene.remove(player.group);
    player.group.traverse(node=>{
      if(node.geometry&&!node.userData.sharedPlayerGeometry) geometries.add(node.geometry);
      for(const material of Array.isArray(node.material)?node.material:[node.material]){
        if(!material) continue;
        materials.add(material);
        if(material.map) textures.add(material.map);
      }
    });
  }
  textures.forEach(texture=>texture.dispose());
  materials.forEach(material=>material.dispose());
  geometries.forEach(geometry=>geometry.dispose());
  matchPlayers.length=0;
  passFlight.active=false;
}

function setupMatchPlayers(){
  matchPlayers=[p1,p2];
  p1.teamId=0; p1.controllerSlot="p1";
  p2.teamId=twoPlayerMode&&teamSize>1&&localTeamMode==="cooperative"?0:1;
  p2.controllerSlot=twoPlayerMode?"p2":null;
  const keys=Object.keys(profiles);
  for(let team=0;team<2;team++){
    let count=matchPlayers.filter(player=>player.teamId===team).length;
    while(count<teamSize){
      const key=basketballComplementaryProfile(team,count,keys);
      const profile=typeof riseMatch!=="undefined"&&riseMatch ? riseRuntimeProfile((team===0?riseMatch.home:riseMatch.away)[count],team) : {...profiles[key]};
      const player=createCharacter(profile,new THREE.Vector3(),`team${team}_${count}`,team);
      player.teamId=team; player.controllerSlot=null;
      matchPlayers.push(player); scene.add(player.group); count++;
    }
  }
  for(const player of matchPlayers){
    player.hasBall=false;
    player.previousPosition=player.group.position.clone();
    player.simulationPosition=player.group.position.clone();
    player.previousAngle=player.group.rotation.y;
    player.simulationAngle=player.group.rotation.y;
    player.passCooldown=0;
    player.passAnimation=0;
    player.catchAnimation=0;
    player.reachRecovery=0;
    player.teamRing=new THREE.Mesh(new THREE.RingGeometry(.64,.72,28),new THREE.MeshBasicMaterial({
      color:player.teamId===0?0x62d9ff:0xff795c,side:THREE.DoubleSide,transparent:true,opacity:.7,depthWrite:false}));
    player.teamRing.rotation.x=-Math.PI/2;
    player.teamRing.position.y=.026;
    player.group.add(player.teamRing);
    player.controlLabel=makeTextSprite(player.controllerSlot?.toUpperCase()||"",player.teamId===0?"#62d9ff":"#ff795c",64);
    player.controlLabel.position.set(0,2.85*player.profile.height,0);
    player.controlLabel.scale.set(.6,.3,1);
    player.controlLabel.visible=!!player.controllerSlot;
    player.group.add(player.controlLabel);
    player.receiverRing=new THREE.Mesh(new THREE.RingGeometry(.81,.88,28),new THREE.MeshBasicMaterial({
      color:0x8effa2,side:THREE.DoubleSide,transparent:true,opacity:.85,depthWrite:false}));
    player.receiverRing.rotation.x=-Math.PI/2;
    player.receiverRing.position.y=.03; player.receiverRing.visible=false;
    player.group.add(player.receiverRing);
  }
  simulationAccumulator=0; simulationTime=0;
  passFlight.active=false;
}

function basketballComplementaryProfile(team,count,keys=Object.keys(profiles)){
  const captain=matchPlayers.find(player=>player.teamId===team);
  const identity=captain?.profile.archetype||"all_rounder";
  const style=teamOffensiveStyles[team]||"balanced";
  const combinations={
    motion:["playmaker","sharpshooter"],pick_roll:["interior","sharpshooter"],
    pick_pop:["sharpshooter","interior"],drive_kick:["sharpshooter","slasher"],
    isolation:["sharpshooter","defender"],give_go:["playmaker","slasher"],
    post_up:["interior","sharpshooter"],fast_break:["slasher","defender"]
  };
  const balanced=identity==="interior"?["playmaker","sharpshooter"]:
    identity==="sharpshooter"?["playmaker","interior"]:["interior","sharpshooter"];
  let desired=(combinations[style]||balanced)[Math.max(0,count-1)%2];
  if(desired===identity&&count===1) desired=identity==="sharpshooter"?"playmaker":"sharpshooter";
  const used=new Set(matchPlayers.filter(player=>player.teamId===team).map(player=>player.profile.name));
  return keys.find(key=>profiles[key].archetype===desired&&!used.has(profiles[key].name))||
    keys.find(key=>!used.has(profiles[key].name))||keys[(count*2+team+2)%keys.length];
}

function takePossession(char,options={}){
  char=getPlayerById(char);
  if(!char) return false;
  for(const player of matchPlayers) player.hasBall=player===char;
  char.hasBall=true;
  possession=char.id; ballOwner=char.id; ballState="dribble";
  lastShotShooter=char.id; lastScored=false;
  lastTouchPlayerId=char.id;
  shotCharging=false; chargingPlayer=null; shotPower=0;
  hasShotThisPossession=false;
  layupActive=false; dunkActive=false; rimHangActive=false;
  layupBlocked=false; layupScored=false; dunkScored=false;
  layupOwner=null; dunkOwner=null; passFlight.active=false;
  crossover.active=false; crossover.player=null;
  rimTouchCount=0; shotHasHitRimOrBoard=false; predictedReboundPoint=null;
  rimCollisionCooldown=0; backboardCollisionCooldown=0;
  reboundPredictionTimer=0; foulMeter=0;
  ballSide="right"; ballSideValue=1;
  ballVelocity.set(0,0,0);
  ballSpin.set(0,0,0);
  clearShotCreationStates();
  attachBallToHand(char,"right");
  previousBallPosition.copy(ball.position);
  lastBallVisualPos.copy(ball.position);
  loosePickupDelay=.12; ballCollisionCooldown=.1;
  char.catchAnimation=.28;
  if(char.moveMeta) char.moveMeta.chargeHeld=0;
  if(!options.silent) flash(`${char.controllerSlot?.toUpperCase()||char.profile.name} has the ball`);
  return true;
}

function resetTeamPositions(ownerId){
  const owner=getPlayerById(ownerId);
  if(!owner) return;
  const offense=matchPlayers.filter(player=>player.teamId===owner.teamId);
  const defense=matchPlayers.filter(player=>player.teamId!==owner.teamId);
  const spacing=teamSize===3?5.2:4.4;
  const others=offense.filter(player=>player!==owner);
  owner.group.position.set(0,0,4.8);
  others.forEach((player,index)=>player.group.position.set((index===0?-1:1)*spacing,0,teamSize===3?-2.4:-1.5));
  defense.forEach((player,index)=>{
    const mark=index===0?owner:others[index-1];
    if(mark) player.group.position.copy(mark.group.position).add(new THREE.Vector3(0,0,-2.2));
  });
  for(const player of matchPlayers){
    player.velocity.set(0,0,0); player.yVel=0; player.onGround=true;
    player.moveState="normal"; player.moveTimer=0;
    player.angle=angleToward(player.group.position,player.teamId===owner.teamId?HOOP_POS:owner.group.position);
    player.group.rotation.set(0,player.angle,0);
    if(player.previousPosition) player.previousPosition.copy(player.group.position);
    if(player.simulationPosition) player.simulationPosition.copy(player.group.position);
    player.previousAngle=player.angle; player.simulationAngle=player.angle;
  }
  ballPreviousPosition.copy(ball.position); ballSimulationPosition.copy(ball.position);
}

function getPassTarget(char){
  if(!char) return null;
  const facing=getForward(char.angle);
  let best=null, bestScore=-Infinity;
  for(const teammate of matchPlayers){
    if(teammate===char||teammate.teamId!==char.teamId) continue;
    const dx=teammate.group.position.x-char.group.position.x;
    const dz=teammate.group.position.z-char.group.position.z;
    const distance=Math.hypot(dx,dz);
    if(distance<.8) continue;
    const alignment=(dx*facing.x+dz*facing.z)/distance;
    // Direction dominates; a shorter pass breaks ties without silently choosing behind you.
    const score=alignment*5-distance*.025;
    if(alignment>-.15&&score>bestScore){ best=teammate; bestScore=score; }
  }
  return best;
}

function tryDirectionalPass(char,intendedTarget=null){
  if(possessionFrozen||gamePaused||!playerHasBall(char)||char.passCooldown>0) return false;
  const target=intendedTarget||getPassTarget(char);
  if(!target){ flash(teamSize===1?"No teammate in 1v1":"Face a teammate to pass"); return false; }
  if(intendedTarget){
    if(target===char||target.teamId!==char.teamId||!matchPlayers.includes(target)) return false;
    const direction=target.group.position.clone().sub(char.group.position).setY(0);
    if(direction.length()<.8||getForward(char.angle).dot(direction.normalize())<.97) return false;
  }
  const start=getWorldHand(char,ballSideValue<0?"left":"right");
  start.y=Math.max(1.25,start.y);
  const distance=flatDistance(start,target.group.position);
  const passingSpeed=char.profile.ratings?10+(char.profile.ratings.passing??50)*.08:14;
  const duration=THREE.MathUtils.clamp(distance/passingSpeed,.22,.85);
  const vision=char.profile.ratings?.courtVision;
  const general=Number(char.profile.abilities?.floorGeneral)||0;
  const lead=target.velocity.clone().multiplyScalar(duration*(vision!==undefined ? .3+vision*.0084+general*.02 : .72));
  passFlight.start.copy(start);
  passFlight.end.copy(target.group.position).add(lead); keepInCourt(passFlight.end);
  passFlight.end.y=1.35*target.profile.height;
  const forward=getForward(char.angle);
  const alignment=forward.dot(passFlight.end.clone().sub(start).setY(0).normalize());
  const defender=getNearestOpponent(char);
  const pressure=defender?Math.max(0,1-flatDistance(defender.group.position,char.group.position)/2):0;
  const passing=THREE.MathUtils.clamp(char.profile.passing||char.profile.handle||1,.55,1.5);
  const movementError=Math.min(.16,char.velocity.length()*.012)*(1.2-Math.min(1,passing)*.35);
  const fatigueError=Math.max(0,.55-(char.stamina??1))*.12;
  const accuracy=char.profile.ratings?(.55+(char.profile.ratings.passingAccuracy??50)*.009):1;
  const error=((1-alignment)*.28+pressure*.2+Math.max(0,distance-8)*.018+movementError+fatigueError)/(passing*accuracy);
  // Repeatable pressure/angle error rather than a hidden successful-pass roll.
  passFlight.end.x+=forward.z*error; passFlight.end.z-=forward.x*error;
  passFlight.duration=duration; passFlight.elapsed=0;
  passFlight.quality=THREE.MathUtils.clamp(1-error,0,1);
  passFlight.passerId=char.id; passFlight.targetId=target.id; passFlight.active=true;
  passFlight.previous.copy(start);
  ball.position.copy(start); ballState="pass"; ballOwner=null;
  lastTouchPlayerId=char.id;
  matchPlayers.forEach(player=>player.hasBall=false);
  shotCharging=false; chargingPlayer=null; shotPower=0;
  char.passAnimation=.32; char.passMotionTime=.32; char.passCooldown=.42;
  char.stats.passAttempts=(char.stats.passAttempts||0)+1;
  ballVelocity.copy(passFlight.end).sub(start).multiplyScalar(1/duration);
  playShotSound();
  return true;
}

function distanceToPassSegment(position,start,end){
  const dx=end.x-start.x, dy=end.y-start.y, dz=end.z-start.z;
  const length=dx*dx+dy*dy+dz*dz;
  const t=length?THREE.MathUtils.clamp(((position.x-start.x)*dx+(position.y-start.y)*dy+(position.z-start.z)*dz)/length,0,1):0;
  return Math.hypot(position.x-start.x-dx*t,position.y-start.y-dy*t,position.z-start.z-dz*t);
}

function updatePassBall(dt){
  if(!passFlight.active){ ballState="loose"; return; }
  passFlight.previous.copy(ball.position);
  passFlight.elapsed+=dt;
  const t=Math.min(1,passFlight.elapsed/passFlight.duration);
  ball.position.lerpVectors(passFlight.start,passFlight.end,t);
  ball.position.y+=Math.sin(t*Math.PI)*.38;
  ball.rotation.z+=dt*10;
  for(const player of matchPlayers){
    if(player.id===passFlight.passerId&&t<.65) continue;
    const chest=player.group.position.clone(); chest.y+=1.35*player.profile.height;
    const isOpponent=player.teamId!==teamOf(passFlight.passerId);
    const radius=isOpponent?.48+(player.swipeTime>0?.18:0):.65;
    if(distanceToPassSegment(chest,passFlight.previous,ball.position)>radius) continue;
    if(player.stumbledTimer>0||player.reachRecovery>0) continue;
    const passer=getPlayerById(passFlight.passerId);
    takePossession(player,{silent:true});
    if(isOpponent){ player.stats.steals++; if(passer) passer.stats.turnovers++; flash("INTERCEPTED"); }
    else {
      player.catchAnimation=.34;
      player.lastCatchTime=simulationTime;
      player.lastPassQuality=Math.min(1,(passFlight.quality??1)+(Number(passer?.profile.abilities?.floorGeneral)||0)*.05);
      player.assistPasserId=passer?.id;
      player.assistCatchTime=simulationTime;
      if(passer) passer.stats.passes=(passer.stats.passes||0)+1;
      flash("Good pass");
    }
    return;
  }
  if(t>=1){
    passFlight.active=false; ballState="loose";
    ballVelocity.multiplyScalar(.35); ballVelocity.y=-1.4;
    loosePickupDelay=.06;
  }
}

function switchControlledPlayer(slot){
  const current=getHumanPlayer(slot);
  if(!current||shotCharging&&chargingPlayer===current) return;
  const owner=getBallHandler();
  const candidates=getTeammates(current).filter(player=>!player.controllerSlot);
  const next=candidates.includes(owner)?owner:candidates.sort((a,b)=>flatDistance(a.group.position,ball.position)-flatDistance(b.group.position,ball.position))[0];
  if(!next) return;
  // Taking over an AI gather needs a fresh human press, otherwise the AI's
  // held shot has no matching human release edge and can strand possession.
  if(chargingPlayer===next){
    shotCharging=false; chargingPlayer=null; shotPower=0;
    next.moveMeta.chargeHeld=0;
  }
  const controller=current.controllerSlot;
  current.controllerSlot=null; next.controllerSlot=controller;
  if(controller==="p1") p1=next; else p2=next;
  current.controlLabel.visible=false;
  setSpriteText(next.controlLabel,controller.toUpperCase(),next.teamId===0?"#62d9ff":"#ff795c",64);
  next.controlLabel.visible=true;
}

function updateTeamPresentation(dt){
  const owner=getBallHandler();
  const target=ballState==="dribble"&&owner?.controllerSlot?getPassTarget(owner):null;
  for(const player of matchPlayers){
    player.passCooldown=Math.max(0,player.passCooldown-dt);
    player.passAnimation=Math.max(0,player.passAnimation-dt);
    player.catchAnimation=Math.max(0,player.catchAnimation-dt);
    player.reachRecovery=Math.max(0,player.reachRecovery-dt);
    player.receiverRing.visible=target===player;
    player.teamRing.material.opacity=player.controllerSlot?.85:.38;
    if(player.passAnimation>0||player.catchAnimation>0){
      const blend=Math.sin(Math.min(1,(player.passAnimation||player.catchAnimation)/.34)*Math.PI);
      player.leftArm.rotation.x=-.9*blend; player.rightArm.rotation.x=-.9*blend;
    }
  }
}
