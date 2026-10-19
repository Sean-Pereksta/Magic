let pendingInbound=null;

function releaseShot(shooter,shotType,options={}){
  if(possession !== shooter.id || ballState !== "dribble") return false;
  shooter.hasBall = false;
  ballOwner = null;
  clearShotCreationStates();
  lastShotShooter = shooter.id;
  lastTouchPlayerId = shooter.id;
  ballState = "shot";
  hasShotThisPossession = true;
  loosePickupDelay = 0.18;
  ballCollisionCooldown = 0.08;
  backboardCollisionCooldown = 0;
  rimTouchCount = 0;
  shotHasHitRimOrBoard = false;
  rimCollisionCooldown = 0;
  shooter.shotMotionTime = .46;
  shooter.moveMeta.lastShotType = shotType;
  shooter.lastShotSlot=shooter.controllerSlot || null;

  const releasePoint = options.releasePoint || getShotReleasePoint(shooter,shotType);
  ball.position.copy(releasePoint);
  // An attached gather/dribble has no free-flight history. Starting the sweep
  // at an old pickup position could let a trailing hand block the entire drive.
  previousBallPosition.copy(ball.position);
  const releaseDist = flatDistance(releasePoint,HOOP_POS);
  // The player's feet define the scoring/rating zone. A normal forward hand
  // extension must not turn a legal arc jumper into a two-point attempt.
  const groundDist = flatDistance(shooter.group.position,HOOP_POS);
  lastShotValue = groundDist > THREE_RADIUS ? 3 : 2;
  if(shotType === "hook") lastShotValue = 2;
  lastShotReleasePos.copy(releasePoint);
  lastShotWasHook = shotType === "hook";
  const dist = releaseDist;
  const held = shooter.moveMeta.chargeHeld || 0;
  const contest = calculateShotContest(shooter);
  const timing = calculateShotTiming(held,shotType,shooter,contest);
  const meterProfile = timing.profile;
  const center=(meterProfile.greenStart+meterProfile.greenEnd)*.5;
  const halfWindow=Math.max(.025,(meterProfile.greenEnd-meterProfile.greenStart)*.5);
  // Short close gathers supply touch continuously. An extra frame of button
  // hold must not abruptly turn an open layup into an early full-meter miss.
  // Timing takes over as the hold lengthens. Pressure reduces touch smoothly:
  // light coverage must not turn an ordinary running gather into an early jumper.
  const tapBlend=1-smoothstep(THREE.MathUtils.clamp((held-.20)/.40,0,1));
  // A short gather retains touch through ordinary coverage. Pressure also
  // widens aim below; only smothering defense should strongly spoil tap timing.
  const pressureTouch=1-.92*smoothstep(THREE.MathUtils.clamp((contest.value-.12)/.83,0,1));
  const gatherTouch=shotType==="layup"&&dist<=3.6 ? tapBlend*pressureTouch : 0;
  if(gatherTouch>.9) timing.grade=contest.value<.12?"Open Finish":"Controlled Finish";
  const releaseError=(timing.pct-center)*(1-gatherTouch);
  const rangePenalty=Math.max(0,dist-(shotType==="layup" ? 2.5 : 4.8))*.037;
  const movementPenalty=shotType==="layup" ? Math.max(0,shooter.velocity.length()-4)*.018 : shooter.velocity.length()*.047;
  const fatiguePenalty=(1-shooter.stamina)*.24;
  const rating=shotType==="hook" ? (shooter.profile.hook || .8) : shotType==="layup" ? shooter.profile.layup : shooter.profile.shot;
  const typePenalty=shotType==="fadeaway" ? .16 : shotType==="hook" ? .1 : shotType==="pullup" ? .05 : 0;
  const difficulty=THREE.MathUtils.clamp(contest.value*.85+rangePenalty+movementPenalty+fatiguePenalty+typePenalty-(rating-1)*.23,0,1.5);
  const target = HOOP_POS.clone();
  const shotLine = HOOP_POS.clone().sub(releasePoint).setY(0).normalize();
  const shotSide = new THREE.Vector3(-shotLine.z,0,shotLine.x);
  // The same timing, positioning and momentum produce the same aim error.
  // Early releases run long, late releases finish short; pressure moves the aim off line.
  // Timing error is continuous across the green band. Dividing it by the very
  // narrow band made even a green edge release aim outside the physical rim.
  const outsideGreen=Math.max(0,Math.abs(releaseError)-halfWindow);
  const insideGreen=Math.min(Math.abs(releaseError),halfWindow)*.32;
  const timingMagnitude=(insideGreen+outsideGreen*(.7+Math.max(0,outsideGreen-.06)*20))*(.9+Math.min(dist,12)*.02);
  const timingOffset=-Math.sign(releaseError)*timingMagnitude*(shotType==="layup" ? .3 : 1);
  target.addScaledVector(shotLine,THREE.MathUtils.clamp(timingOffset,-2.4,2.4));
  const motionSide=shooter.velocity.dot(shotSide);
  const sideSign=motionSide<-.15 ? -1 : 1;
  let developmentShot=null;
  if(typeof BasketballDevelopment!=="undefined"){
    const clutch=Math.abs(scoreP1-scoreP2)<=3&&Math.max(scoreP1,scoreP2)>=targetScore-4;
    developmentShot=BasketballDevelopment.shotProfile(shooter,{distance:groundDist,type:shotType,contest:contest.value,
      timingError:releaseError,halfWindow,now:simulationTime,clutch});
    const aim=BasketballDevelopment.sampleAim(developmentShot.spread*(1-gatherTouch*.55),options.random || Math.random);
    // Dispersion is applied at release. Rim/backboard contacts, blocks and the
    // downward swept crossing decide the result; there is no make/miss roll.
    target.copy(HOOP_POS);
    target.addScaledVector(shotSide,aim.side+motionSide*developmentShot.movementBias);
    target.addScaledVector(shotLine,aim.depth+developmentShot.timingBias+developmentShot.depthBias);
    shooter.shotMotionTime=developmentShot.releaseDuration;
    shooter.moveMeta.shotReleaseDuration=developmentShot.releaseDuration;
    shooter.moveMeta.activeAbilities=developmentShot.active;
    shooter.moveMeta.lastAimSpread=developmentShot.spread;
    if(contest.value<.28&&Math.abs(releaseError)<halfWindow*1.4) shooter.stats.goodShots=(shooter.stats.goodShots||0)+1;
  }else{
    target.addScaledVector(shotSide,sideSign*difficulty*.62+motionSide*(shotType==="layup" ? .008 : .025));
    target.addScaledVector(shotLine,-difficulty*.38);
  }
  const contestDefender = contest.defender;
  if(contestDefender&&contest.value>.1&&!developmentShot){
    const contestPush = shooter.group.position.clone().sub(contestDefender.group.position).setY(0);
    if(contestPush.lengthSq()>.001) target.addScaledVector(contestPush.normalize(),contest.value*.16);
  }
  const power = THREE.MathUtils.clamp(shotPower,.12,1);
  const arcLift=THREE.MathUtils.clamp(getArcBonus(shooter,shotType)+(options.arcBonus || 0),0,4);
  const flightTime = (shotType==="layup" ? .64 : .88+Math.min(dist,14)*.022)+(shotType==="hook" ? .12 : 0)+arcLift*.045;
  const needed = target.clone().sub(releasePoint);
  ballVelocity.x = needed.x / flightTime;
  ballVelocity.z = needed.z / flightTime;
  ballVelocity.y = (needed.y - .5*GRAVITY*flightTime*flightTime) / flightTime;
  ballSpin.set(shooter.velocity.z*.08,-(7.2+power*2.4),-shooter.velocity.x*.08);
  if(shotType === "hook"){
    triggerShotSplash(releasePoint.clone(),"blue");
    spawnCourtThemedParticle(releasePoint.clone(),"hook");
  }

  shotPower = 0;
  shooter.moveMeta.chargeHeld = 0;
  shooter.moveMeta.lastTimingGrade=timing.grade;
  shooter.moveMeta.lastShotDifficulty=difficulty;
  if(options.countAttempt!==false){
    shooter.stats.fga += 1;
    if(lastShotValue === 3) shooter.stats.tpa += 1;
  }
  if(timing.grade === "Green Release"){
    shooter.stats.perfect += 1;
    triggerShotSplash(releasePoint,"green");
    triggerCrowdReaction("green",0.8);
  }
  const shotLabel = shotType === "layup" ? "Layup" : shotType === "hook" ? "Hook Shot" : shotType === "fadeaway" ? "Fadeaway" : shotType === "jumper" ? "High Jumper" : shotType === "setshot" ? "Set Shot" : (shotType === "pullup" ? "Pull-Up" : shotType === "stepback" ? "Step-Back" : "Jumper");
  const abilityLabel=developmentShot?.active.length ? developmentShot.active[0].replace(/([A-Z])/g," $1").replace(/^./,s=>s.toUpperCase()) : "";
  showShotBreakdown(timing.grade,shotLabel,contest.label,abilityLabel?`In Flight • ${abilityLabel}`:"In Flight",shooter.id);
  playShotSound();
  flash(`${shooter.id.toUpperCase()} ${shotLabel.toLowerCase()}${abilityLabel?` • ${abilityLabel}`:""}!`);
  return true;
}

function shootBall(char,shotType="setshot"){
  if(possession !== char.id || ballState !== "dribble") return;
  shotCharging = false;
  chargingPlayer = null;
  if(shotType === "hook"){
    char.anim.hookWindup = 1;
    char.anim.hookRelease = 1;
    char.moveMeta.lastShotType = "hook";
    showMoveFeedback("HOOK SHOT!");
    pulseCourtTheme("hook");
  }
  if(shotType === "fadeaway"){
    lastShotWasHook = false;
    char.moveMeta.lastShotType = "fadeaway";
    char.moveMeta.fadeawayArmed = false;
    char.moveMeta.fadeawayWindow = 0;
    showMoveFeedback("FADEAWAY!");
  }
  const options = {};
  const flavor = getDribbleFlavor(char);
  if(shotType === "hook"){
    options.releasePoint = getHookReleasePoint(char);
    options.arcBonus = 0.86;
    options.blockResistance = 0.25;
    options.accuracyPenalty = 0.16;
  }
  if(shotType === "jumper" || shotType === "setshot" || shotType === "pullup" || shotType === "stepback") {
    const gatherSpeed = THREE.MathUtils.clamp((0.96 + (char.profile.handle-1)*0.14) / flavor.gather,0.74,1.28);
    const gatherBlend = THREE.MathUtils.clamp(0.58 + gatherSpeed*0.25,0.52,0.9);
    const handPoint = getWorldHand(char,(ballSideValue < 0 ? "left" : "right"));
    const pocket = getShotReleasePoint(char,"jumper").clone().add(new THREE.Vector3(0,-0.1 + char.profile.height*0.04,0.08));
    options.releasePoint = handPoint.clone().lerp(pocket,gatherBlend);
    options.releasePoint.y=Math.max(options.releasePoint.y,char.group.position.y+2.2*char.profile.height);
  }
  if(shotType === "fadeaway"){
    options.releasePoint = getShotReleasePoint(char,"jumper").clone().add(new THREE.Vector3(0,0.25 + char.profile.height * 0.18,0));
    options.arcBonus = 0.55;
  }
  releaseShot(char,shotType,options);
}

function botShoot(char=getBallHandler()){
  if(!char) return;
  shotPower = .65;
  shootBall(char);
}

function getFinishingElevation(char,type="layup"){
  const development=typeof BasketballDevelopment!=="undefined";
  const jump=development ? BasketballDevelopment.rating(char,"jumping",BasketballDevelopment.legacyRating(char,"jump")) : 60+(char.profile.jump-1)*65;
  const touch=development ? BasketballDevelopment.rating(char,type==="dunk"?"dunking":"layups",BasketballDevelopment.legacyRating(char,"layup")) : 60+((char.profile.layup||1)-1)*65;
  const held=char.moveMeta.chargeHeld||0;
  const tap=1-smoothstep(THREE.MathUtils.clamp((held-.20)/.40,0,1));
  const timing=.9*tap+THREE.MathUtils.clamp(1-Math.abs(held-.72)*.9,.55,1)*(1-tap);
  // A few centimetres of real elevation reward touch and vertical together.
  // This moves the release/hand path, leaving an exposed ball blockable.
  return .14*THREE.MathUtils.clamp((jump-35)/60,0,1)*THREE.MathUtils.clamp((touch-35)/60,0,1)*timing;
}

function startLayup(char){
  if(!char || possession!==char.id || ballOwner!==char.id || ballState !== "dribble") return;
  shotCharging = false;
  chargingPlayer = null;
  hasShotThisPossession = true;
  layupActive = true;
  layupOwner = char.id;
  lastShotShooter = char.id;
  lastShotValue = 2;
  lastShotWasHook = false;
  lastShotReleasePos.copy(char.group.position);
  ballOwner = char.id;
  ballState = "layup";
  layupTimer = 0;
  layupScored = false;
  layupBlocked = false;
  layupBlockWindow = .34;
  char.moveMeta.gatherType = "layup";
  char.moveMeta.finishElevation=getFinishingElevation(char,"layup");
  char.stats.fga+=1;
  char.yVel = 3.9 * char.profile.jump+char.moveMeta.finishElevation;
  char.onGround = false;
  flash(`${char.id.toUpperCase()} layup!`);
}

function startDunk(char){
  if(!char || possession!==char.id || ballOwner!==char.id || ballState !== "dribble") return;
  if(typeof BasketballDevelopment!=="undefined"&&!BasketballDevelopment.canDunk(char,HOOP_POS,getNearestOpponent(char))) return startLayup(char);
  shotCharging = false;
  chargingPlayer = null;
  hasShotThisPossession = true;
  ballState = "dunk";
  dunkActive = true;
  dunkOwner = char.id;
  lastShotShooter = char.id;
  lastShotValue = 2;
  lastShotWasHook = false;
  lastShotReleasePos.copy(char.group.position);
  dunkTimer = 0;
  dunkScored = false;
  rimHangActive = false;
  rimHangTimer = 0;
  char.yVel = 4.6 * char.profile.jump;
  char.onGround = false;
  rimTouchCount = 0;
  rimCollisionCooldown = 0;
  backboardCollisionCooldown = 0;
  shotHasHitRimOrBoard = false;
  char.moveMeta.gatherType = "dunk";
  char.stats.fga+=1;
  const toRim = HOOP_POS.clone().sub(char.group.position).setY(0);
  const toRimDir = toRim.lengthSq() > 0.0001 ? toRim.clone().normalize() : getForward(char.angle).clone().normalize();
  const right = getBodyRightVector(char).normalize();
  const sideSign = right.dot(toRimDir) > 0 ? 1 : -1;
  const momentum=char.velocity.dot(toRimDir);
  const gatherAngle=getForward(char.angle).dot(toRimDir);
  const nearbyDefender=getNearestOpponent(char);
  const finishProfile=typeof BasketballDevelopment!=="undefined" ? BasketballDevelopment.dunkProfile(char,{momentum,angle:gatherAngle,
    defender:nearbyDefender&&flatDistance(nearbyDefender.group.position,char.group.position)<2.1?nearbyDefender:null}) : null;
  const style = finishProfile?.style ||
    ((char.profile.width > 1.1 || char.profile.height > 1.07) ? "twohand" :
    (char.profile.jump > 1.08 || char.profile.sprint > 1.13) ? "tomahawk" : "quick");
  const twoHands=["twohand","poster","standing","contact"].includes(style);
  const finishElevation=getFinishingElevation(char,"dunk");
  // The path moves the player's feet. A rim-height root puts their knees above
  // the hoop; solve the rise from the actual shoulder and extended arm instead.
  const rimFront = new THREE.Vector3(HOOP_POS.x,char.group.position.y,HOOP_POS.z+RIM_RADIUS+char.bodyRadius*.25);
  const takeoff = char.group.position.clone().add(toRimDir.clone().multiplyScalar(Math.min(1.05,toRim.length()*0.38)));
  const handTargets={},handStarts={};
  const attackSide=sideSign>0?"right":"left";
  let rootHeight=0,rootMaxY=Infinity;
  for(const side of ["left","right"]){
    handTargets[side]=HOOP_POS.clone().add(new THREE.Vector3(twoHands?(side==="left"?-.12:.12):0,.16+finishElevation,.26));
    if(char.group.worldToLocal) handStarts[side]=char.group.worldToLocal(getWorldHand(char,side).clone());
    if(!twoHands&&side!==attackSide) continue;
    const reach=typeof getBasketballDunkReach==="function"&&getBasketballDunkReach(char,side) ||
      {shoulder:new THREE.Vector3(0,1.58*char.profile.height,0),length:.72*char.profile.height,knee:.75*char.profile.height};
    const horizontal=Math.hypot(handTargets[side].x-rimFront.x-reach.shoulder.x,handTargets[side].z-rimFront.z-reach.shoulder.z);
    const vertical=Math.sqrt(Math.max(.04,Math.pow(reach.length*.97,2)-horizontal*horizontal));
    rootHeight=Math.max(rootHeight,handTargets[side].y-reach.shoulder.y-vertical);
    rootMaxY=Math.min(rootMaxY,HOOP_POS.y-reach.knee-.28);
  }
  const peak = rimFront.clone();peak.y=THREE.MathUtils.clamp(rootHeight,char.group.position.y,rootMaxY);
  const attack = peak.clone();
  const landing = rimFront.clone().add(new THREE.Vector3(-sideSign*0.1,0,-0.36));
  char.moveMeta.dunk = {
    start: char.group.position.clone(),
    takeoff,
    peak,
    attack,
    landing,
    side: sideSign > 0 ? "right" : "left",
    startFacing: char.angle,
    style,
    twoHands,finishProfile,finishElevation,
    gatherTiming:char.moveMeta.chargeHeld>.25?Math.max(.25,1-Math.abs(char.moveMeta.chargeHeld-.72)*1.15):.78,
    baseY: char.group.position.y,
    rootMaxY,handTargets,handStarts:Object.keys(handStarts).length===2?handStarts:null,posePhase:0,
    landingTime:0,landingDuration:.38,landingProgress:0,
    gatherSink: 0.1 + (style === "twohand" ? 0.03 : 0),
    forwardBlend: style === "quick" ? 0.3 : 0.24
  };
  char.moveMeta.activeAbilities=finishProfile ? ["powerFinisher","highFlyer","contactSpecialist","aerialAcrobat","posterizer"].filter(id=>BasketballDevelopment.ability(char,id)>0) : [];
  flash(`${char.id.toUpperCase()} ${style==="quick"?"dunk":style+" dunk"}!`);
}

function updateDunk(dt){
  if(!dunkActive) return;
  const owner = getPlayerById(dunkOwner);
  if(!owner){ dunkActive=false; return; }
  dunkTimer += dt;
  const t = THREE.MathUtils.clamp(dunkTimer / dunkDuration,0,1);
  const meta = owner.moveMeta && owner.moveMeta.dunk ? owner.moveMeta.dunk : null;
  if(!meta) return;
  const gather = THREE.MathUtils.clamp(t/0.2,0,1);
  const flight = THREE.MathUtils.clamp((t-0.2)/0.34,0,1);
  const rimPhase = THREE.MathUtils.clamp((t-0.54)/0.22,0,1);
  const slam = THREE.MathUtils.clamp((t-0.76)/0.24,0,1);
  const gatherPos = meta.start.clone().lerp(meta.takeoff,smoothstep(gather));
  gatherPos.y = meta.baseY - Math.sin(gather*Math.PI) * meta.gatherSink;
  const flightPos = meta.takeoff.clone().lerp(meta.peak,smoothstep(flight));
  const rimPos = meta.peak.clone().lerp(meta.attack,smoothstep(rimPhase));
  if(dunkScored&&!rimHangActive) meta.landingTime+=dt;
  meta.landingProgress=THREE.MathUtils.clamp(meta.landingTime/meta.landingDuration,0,1);
  meta.posePhase=dunkScored?.79+.21*meta.landingProgress:t;
  const slamPos = meta.attack.clone().lerp(meta.landing,smoothstep(dunkScored?meta.landingProgress:0));
  if(t < 0.2) owner.group.position.copy(gatherPos);
  else if(t < 0.54) owner.group.position.copy(flightPos);
  else if(t < 0.76) owner.group.position.copy(rimPos);
  else owner.group.position.copy(slamPos);
  owner.group.position.y = THREE.MathUtils.clamp(owner.group.position.y,meta.baseY-0.12,meta.rootMaxY);
  owner.group.position.z = Math.max(owner.group.position.z, HOOP_POS.z + RIM_RADIUS + owner.bodyRadius*.2);
  owner.group.rotation.x = -0.16 * Math.sin(gather*Math.PI) - 0.06 * Math.sin(flight*Math.PI) + 0.05 * Math.sin(slam*Math.PI);
  owner.group.rotation.y = THREE.MathUtils.lerp(owner.group.rotation.y,meta.startFacing,0.18);
  if(meta.style==="reverse"||meta.style==="windmill") owner.group.rotation.y=meta.startFacing+(meta.style==="reverse"?Math.PI:Math.PI*.65)*Math.sin(Math.min(1,flight)*Math.PI*.5)*(1-slam);
  const knee = Math.sin((flight+rimPhase*0.5)*Math.PI);
  owner.leftLeg.rotation.x = -0.24;
  owner.rightLeg.rotation.x = -0.24;
  if(t < 0.2){
    owner.leftLeg.rotation.x -= 0.25 * Math.sin(gather*Math.PI);
    owner.rightLeg.rotation.x -= 0.25 * Math.sin(gather*Math.PI);
  }else if(t < 0.76){
    if(meta.side === "right"){
      owner.rightLeg.rotation.x = 0.36 * knee;
      owner.leftLeg.rotation.x = -0.34 - 0.24 * knee;
    }else{
      owner.leftLeg.rotation.x = 0.36 * knee;
      owner.rightLeg.rotation.x = -0.34 - 0.24 * knee;
    }
  }else{
    owner.leftLeg.rotation.x = 0.22 * Math.sin(slam*Math.PI);
    owner.rightLeg.rotation.x = 0.22 * Math.sin(slam*Math.PI);
  }
  keepInCourt(owner.group.position);
  // The root just moved; refresh this pose at zero animation time before using
  // its hand for the real ball and block-contact point.
  if(typeof updatePlayerModel==="function"&&owner.playerModel) updatePlayerModel(owner,0);
  else if(typeof poseBasketballDunkHands==="function"){poseDunkArms(owner,meta.posePhase);poseBasketballDunkHands(owner);}
  if(t < 0.54){
    if(meta.twoHands || meta.style === "twohand") attachBallBetweenHands(owner,new THREE.Vector3(0,.1,-.12));
    else attachBallToHand(owner,meta.side,new THREE.Vector3(0,.1,-.12));
    ballVelocity.set(0,0,0);
  }else if(t < 0.76){
    const handBias = meta.side === "right" ? "right" : "left";
    const rimHand = getWorldHand(owner,handBias);
    const targetAboveRim = HOOP_POS.clone().add(new THREE.Vector3(0,0.2+meta.finishElevation*.5,0));
    ball.position.copy(rimHand.lerp(targetAboveRim,0.58));
    ballVelocity.set(0,-0.25,-0.05);
  }else{
    const punch = smoothstep(slam);
    ball.position.copy(HOOP_POS.clone().add(new THREE.Vector3(0,0.2 - punch*0.62,0)));
    ballVelocity.set(0,(-4.4 - punch*0.5)*(meta.finishProfile?.force||1),-0.08);
  }
  if(t>.28 && t<.79 && !dunkScored){
    for(const defender of getOpponents(owner)){
      if(defender.blockTime<=0 && defender.handsUpTime<=0) continue;
      // Finishing ability can protect the ball during a gather; a timed hand
      // reaching the actual ball still stops even the strongest Posterizer.
      const contact=getShotBlockContact(defender,owner,ball.position,ball.position);
      if(contact){
        applyBlockDeflection(defender,owner,contact);
        addBlockStat(defender,12);
        showMoveFeedback("DUNK BLOCKED!",.8);
        triggerCrowdReaction("block",.8);
        return;
      }
    }
  }
  if(t >= .79 && !dunkScored){
    const contest=calculateShotContest(owner);
    const finishStrength=owner.profile.layup*.55+owner.profile.jump*.25+owner.profile.width*.2;
    const rimProtector=contest.defender;
    const development=meta.finishProfile;
    const defense=development&&rimProtector ? (BasketballDevelopment.rating(rimProtector,'interiorDefense',60)*.55+
      BasketballDevelopment.rating(rimProtector,'strength',60)*.25+BasketballDevelopment.rating(rimProtector,'blocking',60)*.2)/100 : 0;
    const positioned=!!rimProtector&&isDefenderBetweenBallAndRim(rimProtector,meta.start)&&flatDistance(rimProtector.group.position,HOOP_POS)<1.45;
    const wall=development&&positioned&&contest.value>.45&&defense+contest.value*.26>development.finish+development.resistance*.23+meta.gatherTiming*.1;
    const spoiled=development&&contest.value>.68&&development.finish+development.resistance*.18<.79;
    if(wall||spoiled||(!development&&contest.value>.72 && finishStrength<1.14 && owner.stamina<.65)){
      dunkActive=false;
      owner.hasBall=false;
      ballOwner=null;
      ballState="loose";
      ball.position.copy(HOOP_POS).add(new THREE.Vector3(.45,.2,.4));
      ballVelocity.set(1.5,1.8,2.2);
      loosePickupDelay=.22;
      showMoveFeedback("CONTESTED FINISH",.7);
      return;
    }
    dunkScored = true;
    owner.stats.dunks=(owner.stats.dunks||0)+1;
    shotHasHitRimOrBoard = true;
    const points = 2;
    awardBasket(owner,points,"Dunk");
    showShotBreakdown(meta.style==="reverse"?"Aerial Acrobat":meta.style==="poster"?"Posterizer":"Power Finish","Dunk",contest.label,"Made");
    animateNetSwish();
    triggerNetWobble((meta.twoHands?1.55:1.35)*(development?.force||1),HOOP_POS.clone().add(new THREE.Vector3(0,-0.1,0)),new THREE.Vector3(0.02,-1,-0.06));
    playCrowdPulse();
    triggerShotSplash(HOOP_POS.clone(),"orange");
    showMoveFeedback(meta.style==="poster"?"POSTERIZER!":meta.style==="reverse"?"REVERSE SLAM!":meta.style==="windmill"?"WINDMILL!":meta.style==="contact"?"CONTACT FINISH!":"SLAM!",0.9);
    crowdWave = Math.max(crowdWave,1.1);
    triggerCrowdReaction("dunk",1.05);
    inputManager.vibrate(owner.id,1,120);
    for(const defender of getOpponents(owner)) inputManager.vibrate(defender.id,.26,60);
    if(meta.style !== "quick") applyCameraShake(.24,"dunk");
    pulseCourtTheme("dunk");
    spawnCourtThemedParticle(HOOP_POS.clone(),"dunk");
    spawnCourtThemedParticle(HOOP_POS.clone().add(new THREE.Vector3(0.15,0.12,0)),"dunk");
    spawnCourtThemedParticle(HOOP_POS.clone().add(new THREE.Vector3(-0.15,0.12,0)),"dunk");
    const hangChance =
      0.1 +
      owner.profile.height * 0.1 +
      owner.profile.layup * 0.08 +
      owner.velocity.length() * 0.03;
    if(Math.random() < hangChance){
      rimHangActive = true;
      rimHangTimer = rimHangDuration * (meta.style === "twohand" ? 1 : 0.85);
      triggerCrowdReaction("rim_hang",1.15);
    }
    scheduleScoredPossession(owner.id,620);
  }
  if(rimHangActive){
    rimHangTimer -= dt;
    if(rimHangTimer <= 0) rimHangActive = false;
  }
  if(t >= 1 && !rimHangActive && (!dunkScored || meta.landingProgress>=1)){
    dunkActive = false;
    owner.group.position.y=meta.baseY;owner.yVel=0;owner.onGround=true;
    if(dunkScored){
      ballState = "dead";
      ballVelocity.set(0,-2.8,0);
    }else{
      ballState = "loose";
      ballVelocity.set(0,1.1,1.3);
    }
  }
}

function getReachHitPoint(char,handSide){
  const forward = getForward(char.angle).normalize();
  const right = getBodyRightVector(char).normalize();
  const sign = handSide === "left" ? -1 : 1;
  return char.group.position.clone()
    .add(new THREE.Vector3(0,1.05*char.profile.height,0))
    .add(forward.clone().multiplyScalar(0.75 + char.profile.steal * 0.18))
    .add(right.clone().multiplyScalar(sign * 0.32 * char.profile.width));
}
function segmentIntersectsSphere(a,b,center,radius){
  const ab = b.clone().sub(a);
  const t = THREE.MathUtils.clamp(center.clone().sub(a).dot(ab) / Math.max(ab.lengthSq(),0.00001),0,1);
  const p = a.clone().add(ab.multiplyScalar(t));
  return p.distanceTo(center) <= radius;
}
function hasCleanStealPath(stealer,defender,handWorldPos,ballWorldPos){
  const blockers = [
    {c:defender.group.position.clone().add(new THREE.Vector3(0,1.2*defender.profile.height,0)),r:0.5*defender.profile.width},
    {c:getWorldHand(defender,"left"),r:0.18*defender.profile.width},
    {c:getWorldHand(defender,"right"),r:0.18*defender.profile.width},
    {c:defender.group.position.clone().add(new THREE.Vector3(0,0.74*defender.profile.height,0)),r:0.34*defender.profile.width}
  ];
  for(const b of blockers){
    if(segmentIntersectsSphere(handWorldPos,ballWorldPos,b.c,b.r)) return false;
  }
  return true;
}

function isFacingTarget(char,target,minDot=.25){
  const forward = getForward(char.angle);
  const toTarget = target.clone().sub(char.group.position);
  toTarget.y = 0;
  if(toTarget.lengthSq() < .0001) return true;
  toTarget.normalize();
  return forward.dot(toTarget) >= minDot;
}

function getShotBlockContact(blocker,shooter,from,to){
  if(blocker.blockTime<=0 && blocker.handsUpTime<=0) return null;
  if(shooter){
    const finishing=ballState==="layup"||ballState==="dunk"||
      (ballState==="shot"&&shooter.moveMeta?.lastShotType==="layup"&&flatDistance(to,shooter.group.position)<2);
    if(finishing){
      const toRim=HOOP_POS.clone().sub(shooter.group.position).setY(0).normalize();
      const behind=blocker.group.position.clone().sub(shooter.group.position).setY(0).dot(toRim)<-.18;
      const leapTime=blocker.moveMeta?.blockFlight?.elapsed ?? .42-blocker.blockTime;
      // A chase-down needs a committed leap meeting the exposed ball near the
      // middle of the reach. Holding hands up or arriving after the swipe does
      // not erase a finisher who has already earned a step toward the basket.
      if(behind&&(blocker.onGround||blocker.blockTime<=0||leapTime<.075||leapTime>.31)) return null;
    }
  }
  // The ball and the visible palm set contact size. Blocking ratings improve
  // the jump and defensive decisions, never an invisible halo around the hand.
  const radius=BALL_RADIUS+THREE.MathUtils.clamp(.07*blocker.profile.width,.06,.09);
  const path=to.clone().sub(from),lengthSq=path.lengthSq();
  for(const side of ["left","right"]){
    const hand=getWorldHand(blocker,side);
    const t=THREE.MathUtils.clamp(hand.clone().sub(from).dot(path)/Math.max(.00001,lengthSq),0,1);
    const point=from.clone().addScaledVector(path,t),distance=hand.distanceTo(point);
    if(distance>radius) continue;
    if(shooter){
      // A hand reaching through the finisher's torso cannot touch a protected
      // ball on the far side. High, genuinely exposed releases stay blockable.
      const shielded=[{y:1.16,r:.34},{y:1.58,r:.29}].some(shield=>
        segmentSphereIntersect(hand,point,shooter.group.position.clone().add(new THREE.Vector3(0,shield.y*shooter.profile.height,0)),shield.r*shooter.profile.width));
      if(shielded) continue;
    }
    return {point,hand,side,distance,radius};
  }
  return null;
}

function applyBlockDeflection(blocker,shooter,contact=null){
  lastTouchPlayerId=blocker.id;
  const blockerForward = getForward(blocker.angle);
  const handSide = contact?.side || (getWorldHand(blocker,"left").distanceToSquared(ball.position)<getWorldHand(blocker,"right").distanceToSquared(ball.position) ? "left" : "right");
  const handPoint = contact?.hand || getWorldHand(blocker,handSide);
  if(contact) ball.position.copy(contact.point);
  const away = ball.position.clone().sub(handPoint);
  if(away.lengthSq() < 0.0001) away.copy(ball.position).sub(blocker.group.position);
  away.normalize();
  const power = 2.4 + blocker.profile.block * 1.8 + (blocker.blockBoost ? 1.4 : 0) + Math.max(0,blocker.yVel) * 0.15;
  ballVelocity.copy(away.multiplyScalar(power).add(blockerForward.multiplyScalar(1.2)));
  ballVelocity.y = Math.max(1.8,2.0 + blocker.profile.block * 0.5);
  ballState = "loose";
  clearShotCreationStates();
  layupActive = false;
  layupOwner = null;
  dunkActive = false;
  dunkOwner = null;
  ballOwner = null;
  for(const char of matchPlayers) char.hasBall=false;
  loosePickupDelay = Math.max(loosePickupDelay,.22);
}

function checkSteals(){
  if(ballState !== "dribble") return;
  const ballHandler=getBallHandler();
  if(!ballHandler) return;
  for(const defender of getOpponents(ballHandler)){
    // Only the middle of the swipe is live, after the readable wind-up.
    if(defender.swipeTime<.11 || defender.swipeTime>.25 || defender.swipeConnected || defender.reachRecovery>0) continue;
    const side = defender.swipeSide || "right";
    const hand = getWorldHand(defender,side);
    const forearm = side === "left" ? defender.leftForearm : defender.rightForearm;
    const forearmWorld = (typeof getModelForearmWorld==="function" && getModelForearmWorld(defender,side)) || forearm.getWorldPosition(new THREE.Vector3());
    const reachPoint = getReachHitPoint(defender,side);
    const reachDist = Math.min(hand.distanceTo(ball.position),reachPoint.distanceTo(ball.position),forearmWorld.distanceTo(ball.position));
    if(!isFacingTarget(defender,ball.position,.35)) continue;
    if(!hasCleanStealPath(defender,ballHandler,hand,ball.position)) continue;
    const exposed=ball.position.y<1.05 || (crossover.active && crossover.player===ballHandler && crossover.riskWindow>0);
    const security=typeof BasketballDevelopment!=="undefined"?BasketballDevelopment.rating(ballHandler,'ballSecurity',60):60;
    const pickpocket=exposed&&typeof BasketballDevelopment!=="undefined"?BasketballDevelopment.ability(defender,'pickpocket'):0;
    const reachWindow=(exposed ? .48 : .34)*defender.profile.steal*getGameplayTuning().stealWindow*THREE.MathUtils.clamp(1-(security-60)*.003,.86,1.15)*(1+pickpocket*.05);
    if(reachDist>=reachWindow) continue;
    defender.swipeConnected=true;
    gainPossession(defender.id);
    flash(`${defender.id.toUpperCase()} steal!`);
    showShotFeedback("Stripped!");
    triggerCrowdReaction("steal",0.75);
    defender.confidence = Math.min(1,defender.confidence + .08);
    defender.stats.steals += 1;
    ballHandler.stats.turnovers += 1;
    addTakeover(10,defender);
    inputManager.vibrate(defender.id,.42,70);
    inputManager.vibrate(ballHandler.id,.24,55);
    return;
  }
}

function checkBlocksAndCatches(){
  // Attached gathers test their current hand motion in updateLayup. The global
  // flight segment is stale during a dribble/gather and must not sweep a block
  // across the whole drive from the previous possession pickup point.
  if(ballState !== "shot") return;

  const shooter=getPlayerById(lastShotShooter);
  if(!shooter) return;
  for(const blocker of getOpponents(shooter)){
  if(blocker.blockTime <= 0 && blocker.handsUpTime <= 0) continue;

  const contact=getShotBlockContact(blocker,shooter,previousBallPosition,ball.position);
  if(!contact) continue;
  if(contact){
    const catchQuality=getLooseBallPickupQuality(blocker);
    if(ballVelocity.length()<4.2 && ballVelocity.y<0 && ball.position.y<HOOP_POS.y-.18 && catchQuality>.72 && contact.distance<contact.radius*.65){
      gainPossession(blocker.id);
      flash(`${blocker.id.toUpperCase()} caught the shot!`);
    }else{
      applyBlockDeflection(blocker,shooter,contact);
      flash(`${blocker.id.toUpperCase()} blocked it!`);
      showShotFeedback("Blocked");
      addBlockStat(blocker,10);
      triggerCrowdReaction("block",0.88);
      playBlockSound();
      inputManager.vibrate(blocker.id,.78,95);
      inputManager.vibrate(shooter.id,.4,75);
    }
    return;
  }
  }
}

function checkFouls(dt){
  if(ballState !== "dribble") return;
  const owner=getBallHandler();
  if(!owner) return;
  const illegalContact=getOpponents(owner).some(defender=>flatDistance(owner.group.position,defender.group.position)<.94 && defender.swipeTime>0 && !isFacingTarget(defender,owner.group.position,.1));
  if(illegalContact) foulMeter += dt*.3;
  else foulMeter = Math.max(0,foulMeter - dt*.08);

  if(foulMeter >= 1){
    flash("Foul! Possession stays.");
    resetPossession(possession,true);
  }
}

function awardBasket(shooter,points,kind="Jumper"){
  if(!shooter || lastScored || gameOver) return false;
  lastScored=true;
  if(shooter.teamId===0) scoreP1+=points;
  else scoreP2+=points;
  shooter.stats.fgm+=1;
  shooter.stats.points=(shooter.stats.points||0)+points;
  if(points===3) shooter.stats.tpm+=1;
  if(shooter.assistPasserId&&simulationTime-(shooter.assistCatchTime||0)<5){
    const passer=getPlayerById(shooter.assistPasserId);
    if(passer&&passer!==shooter&&passer.teamId===shooter.teamId) passer.stats.assists=(passer.stats.assists||0)+1;
  }
  shooter.assistPasserId=null;
  for(const char of matchPlayers) char.hasBall=false;
  ballOwner=null;
  ballState="dead";
  possessionFrozen=true;
  rimTouchCount=0;
  rimCollisionCooldown=0;
  backboardCollisionCooldown=0;
  flash(`${shooter.id.toUpperCase()} scores ${points}!`);
  updateMomentum(shooter,true,points,shooter.lastContestLabel);
  updateScoreboard();
  checkGameWinner();
  return true;
}

function scheduleScoredPossession(scorerId,delay=850){
  pendingInbound={remaining:delay/1000,scorerId};
}

function scheduleDeadPossession(ownerId,delay=420){
  possessionFrozen=true;
  pendingInbound={remaining:delay/1000,ownerId};
}

function updateScoredPossession(dt){
  if(!pendingInbound) return false;
  if(typeof gamePaused!=="undefined"&&gamePaused) return true;
  // A scored dunk keeps its brief hang and landing while the authoritative
  // inbound timer runs. The main simulation returns here, so these advance once.
  if(dunkActive){updateDunk(dt);updateCharacters(dt);}
  pendingInbound.remaining-=dt;
  if(pendingInbound.remaining<=0 && gameStarted && !gameOver){
    const ownerId=pendingInbound.ownerId || nextPossessionAfterScore(pendingInbound.scorerId);
    pendingInbound=null;
    resetPossession(ownerId,true);
  }
  return true;
}

function checkScore(){
  if(lastScored || !hasShotThisPossession || (ballState!=="shot" && ballState!=="loose")) return;
  if(ballVelocity.y>=0) return;
  // Test where this flight segment crosses the hoop, not a small post-step
  // window that fast shots can skip entirely. The whole ball must fit inside.
  const crossing=previousBallPosition.y>=HOOP_POS.y && ball.position.y<HOOP_POS.y;
  const initialized=previousBallPosition.lengthSq()>.000001;
  const fraction=crossing ? (previousBallPosition.y-HOOP_POS.y)/(previousBallPosition.y-ball.position.y) : 1;
  const crossingPoint=previousBallPosition.clone().lerp(ball.position,fraction);
  const rimTube=typeof HOOP_GEOMETRY!=="undefined" ? HOOP_GEOMETRY.rimTubeRadius : .045;
  const clearRadius=RIM_RADIUS-rimTube-BALL_RADIUS;
  const legacyDrop=!initialized && ball.position.y<HOOP_POS.y && ball.position.y>HOOP_POS.y-.42;
  if((crossing || legacyDrop) && flatDistance(crossingPoint,HOOP_POS)<clearRadius){
    const shooterId = lastShotShooter;
    const shooter=getPlayerById(shooterId);
    if(!shooter) return;
    const points = shotValueFor(shooterId);
      const shotKind = shooter.moveMeta.lastShotType === "hook" ? "Hook Shot" : shooter.moveMeta.lastShotType==="layup" ? "Layup" : "Jumper";
      if(!awardBasket(shooter,points,shotKind)) return;
      showShotBreakdown(shooter.moveMeta.lastTimingGrade || "Good",shotKind,shooter.lastContestLabel,"Made");
      animateNetSwish();
      triggerNetWobble(points===3 ? 1.1 : 0.95);
      playSwishSound();
      playCrowdPulse();
      triggerShotSplash(HOOP_POS.clone(),points===3?"gold":(lastShotWasHook?"blue":"green"));
      triggerCrowdReaction(points===3?"three":(lastShotWasHook?"hook_make":"make"),points===3?0.95:0.72);
      scheduleScoredPossession(shooterId,850);
  }
}

function checkGameWinner(){
  if(scoreP1 >= targetScore || scoreP2 >= targetScore){
    if(Math.abs(scoreP1 - scoreP2) >= winBy){
      endGame(scoreP1 > scoreP2 ? "p1" : "p2");
    }
  }
}

function triggerNetWobble(strength = 1,contactPoint=null,impactDir=null){
  const s = Math.max(0.2,strength);
  netWobbleStrength = Math.max(netWobbleStrength,s);
  rimWobbleStrength = Math.max(rimWobbleStrength,s);
  netWobbleTimer = Math.max(netWobbleTimer,0.28 + s*0.32);
  rimWobbleTimer = Math.max(rimWobbleTimer,0.2 + s*0.24);
  if(contactPoint) netContactPoint.copy(contactPoint);
  if(impactDir && impactDir.lengthSq() > 0.0001) netImpactDirection.copy(impactDir).normalize();
  netVelocity.addScaledVector(netImpactDirection,0.5*s);
}

function updateHoopReaction(dt){
  if(backboard){
    if(backboardImpactTimer > 0){
      backboardImpactTimer = Math.max(0,backboardImpactTimer - dt);
      const life = backboardImpactTimer / 0.16;
      const shake = Math.sin(backboardImpactTimer * 80) * backboardImpactStrength * life;
      backboard.position.copy(backboardBasePosition);
      backboard.position.z += shake;
    }else{
      backboard.position.copy(backboardBasePosition);
      backboardImpactStrength = 0;
    }
  }

  if(rim && rimBasePosition){
    if(rimWobbleTimer > 0){
      rimWobbleTimer = Math.max(0,rimWobbleTimer - dt);
      const t = performance.now()*0.015;
      const amp = 0.015 * rimWobbleStrength * (rimWobbleTimer/(0.2 + rimWobbleStrength*0.24));
      rim.position.set(
        rimBasePosition.x + Math.sin(t*1.7)*amp*0.7,
        rimBasePosition.y + Math.cos(t*2.1)*amp*0.35,
        rimBasePosition.z
      );
      rim.rotation.z = Math.sin(t*2.4) * amp * 2.6;
    }else{
      rim.position.copy(rimBasePosition);
      rim.rotation.z *= Math.max(0,1 - dt*14);
      rimWobbleStrength = 0;
    }
  }

  if(netLines.length){
    if(netWobbleTimer > 0){
      netWobbleTimer = Math.max(0,netWobbleTimer - dt);
      const t = performance.now()*0.02;
      const life = netWobbleTimer/(0.28 + Math.max(netWobbleStrength,0.2)*0.32);
      const amp = 0.09 * netWobbleStrength * life;
      for(let i=0;i<netLines.length;i++){
        const line = netLines[i];
        const pos = line.geometry.attributes.position;
        const basePoints = line.userData.netBasePoints;
        const phase = i*0.55;
        const impact = netImpactDirection.clone().multiplyScalar(amp*0.7);
        for(let j=0;j<basePoints.length;j++){
          const bp = basePoints[j];
          const tSeg = j/(basePoints.length-1);
          const distInfluence = THREE.MathUtils.clamp(1 - bp.distanceTo(netContactPoint)*1.4,0,1);
          const swayX = Math.sin(t + phase + j*0.4) * amp * tSeg;
          const swayZ = Math.cos(t*1.15 + phase + j*0.35) * amp * 0.6 * tSeg;
          const drop = amp * 0.32 * distInfluence * (0.3 + tSeg);
          pos.setXYZ(j,bp.x + swayX + impact.x*distInfluence*tSeg,bp.y - drop,bp.z + swayZ + impact.z*distInfluence*tSeg);
        }
        pos.needsUpdate = true;
      }
    }else{
      for(const line of netLines){
        const pos = line.geometry.attributes.position;
        const basePoints = line.userData.netBasePoints;
        for(let j=0;j<basePoints.length;j++){ const bp = basePoints[j]; pos.setXYZ(j,bp.x,bp.y,bp.z); }
        pos.needsUpdate = true;
      }
      netWobbleStrength = 0;
    }
  }
  matchPlayers.forEach(pl=>{
    if(!pl) return;
    const hand = pl.group.localToWorld(new THREE.Vector3(pl.profile.width*.36,1.46*pl.profile.height,0));
    const d = hand.distanceTo(HOOP_POS);
    if(d < 0.55 && pl.velocity.y > -0.2){
      const dunkBoost = (pl.dunking || ballState === "dunk") ? 0.62 : 0.28;
      triggerNetWobble(dunkBoost,hand,pl.velocity.clone().setY(0.08));
    }
  });
}
function nextPossessionAfterScore(scorer){
  if(gameMode.makeItTakeIt) return scorer;
  const scorerTeam=teamOf(scorer);
  const nextTeam=gameMode.losersBall ? (scoreP1===scoreP2 ? scorerTeam : scoreP1<scoreP2 ? 0 : 1) : 1-scorerTeam;
  const receiver=matchPlayers.find(char=>char.teamId===nextTeam && char.controllerSlot) || matchPlayers.find(char=>char.teamId===nextTeam);
  return receiver ? receiver.id : scorer;
}
function endGame(winner){
  gameOver = true;
  possessionFrozen = true;
  if(typeof finishRiseMatch==='function' && finishRiseMatch(winner)) return;
  if(isTournamentGame){
    finishTournamentGame(winner);
    return;
  }
  showWinnerOverlay(winner);
  screenShake = .28; applyCameraShake(.22,"block"); triggerSlowMo(.45,.55);
}

function shotValueFor(ownerId){
  if(ballState === "layup" || dunkActive) return 2;
  if(lastShotWasHook) return 2;
  return lastShotValue || 2;
}

function gainPossession(who){
  const newOwner=getPlayerById(who);
  if(!newOwner || gameOver) return;
  takePossession(newOwner);
}

function resetPossession(who,reposition,countdown=1){
  if(gameOver) return;
  pendingInbound=null;
  const owner=getPlayerById(who) || matchPlayers[0];
  if(!owner) return;
  for(const char of matchPlayers) resetBody(char);
  if(reposition) resetTeamPositions(owner.id);
  takePossession(owner,{silent:true});
  lastScored=false;
  lastShotValue = 2;
  lastShotWasHook = false;
  lastShotReleasePos.set(0,0,0);
  loosePickupDelay = 0;
  botStrategy=owner.controllerSlot ? "defense" : chooseBotStrategy();
  startCountdown(countdown);
}

function rematchSamePlayers(){
  if(typeof riseMatch!=="undefined"&&riseMatch){showRiseCampaign();return;}
  if(!gameStarted) return;
  inputManager.clearAll();
  resetPlayerShotFeedback();
  scoreP1 = 0; scoreP2 = 0; gameOver = false; possessionFrozen = false;
  hideWinnerOverlay();
  updateScoreboard();
  initBotBrain();
  for(const char of matchPlayers){
    char.confidence=0; char.hotStreak=0; char.moveState="normal";
    char.sideHopCooldown=0; char.pumpFakeCooldown=0; char.reachRecovery=0;
    for(const key of Object.keys(char.stats)) char.stats[key]=0;
  }
  resetPossession(matchPlayers[0].id,true,3);
}
function returnToCharacterMenu(){
  if(typeof riseMatch!=="undefined"&&riseMatch){showRiseCampaign();return;}
  gameOver = false;
  hideWinnerOverlay();
  showQuickPlaySetup();
}
function showWinnerOverlay(winner){
  document.getElementById("winnerText").textContent = matchPlayers.length>2 ? `TEAM ${winner==="p1" ? 1 : 2} WINS!` : winner === "p1" ? "P1 WINS!" : "P2 WINS!";
  showWinnerStats();
  document.getElementById("winnerOverlay").style.display = "flex";
}
function hideWinnerOverlay(){
  document.getElementById("winnerOverlay").style.display = "none";
}
function updatePlayerStats(char,key,amount=1){
  if(!char || !char.stats) return;
  char.stats[key] = (char.stats[key] || 0) + amount;
}
function showWinnerStats(){
  if(!matchPlayers.length) return;
  const lines=[0,1].map(team=>{
    const totals={fgm:0,fga:0,steals:0,blocks:0};
    for(const char of matchPlayers) if(char.teamId===team) for(const key of Object.keys(totals)) totals[key]+=char.stats[key] || 0;
    return `${matchPlayers.length>2 ? "Team " : "P"}${team+1} ${totals.fgm}/${totals.fga} FG, STL ${totals.steals}, BLK ${totals.blocks}`;
  });
  const line=lines.join(" | ");
  document.getElementById("winnerSub").textContent = line;
}
function buildBroadcastScorebug(){}

function resetBody(char){
  char.group.position.y = 0;
  char.yVel = 0;
  char.onGround = true;
  char.blockBoost = false;
  if(char.moveMeta) delete char.moveMeta.blockFlight;
  char.blockTime = 0;
  char.handsUpTime = 0;
  char.swipeTime = 0;
  char.shotMotionTime = 0;
  char.moveState = "normal";
  char.moveTimer = 0;
  char.moveDuration = 0;
  char.stumbledTimer = 0;
  char.contactTimer=0;char.contactCooldown=0;char.contactStrength=0;char.bodyUpTimer=0;
  char.screenSetTime=0;char.plantedScreen=false;
  char.reachRecovery=0;
  char.swipeConnected=false;
  char.passMotionTime=0;
  if(char.moveMeta){char.moveMeta.approachSpeed=0;char.moveMeta.activeAbilities=[];}
  char.lastCatchTime=-Infinity;char.lastPassQuality=0;
  char.velocity.set(0,0,0);
  }

function chooseBotStrategy(){
  const r = Math.random();
  const attack = difficultySettings[selectedDifficulty].attack;
  if(attack < .25) return "drive";
  if(r < .25) return "drive";
  if(r < .5) return "pullUp";
  if(r < .75) return "post";
  return "crossover";
}

function startCountdown(duration=3){
  countdownTimer = duration;
  possessionFrozen = true;
}
