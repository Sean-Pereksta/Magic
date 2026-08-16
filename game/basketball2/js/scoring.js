function releaseShot(shooter,shotType,options={}){
  if(possession !== shooter.id || ballState !== "dribble") return false;
  shooter.hasBall = false;
  ballOwner = null;
  clearShotCreationStates();
  lastShotShooter = shooter.id;
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

  const releasePoint = options.releasePoint || getShotReleasePoint(shooter,shotType);
  ball.position.copy(releasePoint);
  const releaseDist = flatDistance(releasePoint,HOOP_POS);
  lastShotValue = releaseDist > THREE_RADIUS ? 3 : 2;
  if(shotType === "hook") lastShotValue = 2;
  lastShotReleasePos.copy(releasePoint);
  lastShotWasHook = shotType === "hook";
  const dist = flatDistance(releasePoint,HOOP_POS);
  const held = shooter.moveMeta.chargeHeld || 0;
  const contest = calculateShotContest(shooter);
  const timing = calculateShotTiming(held,shotType,shooter,contest);
  const timingQuality = getShotTimingQuality(held,shotType,shooter,contest);
  const meterProfile = timing.profile;
  const distancePenalty = getDistancePenalty(shooter);
  const movementPenalty = shooter.velocity.length() * 0.035;
  const rushedPenalty = held < 0.2 ? 0.08 : 0;
  const fatiguePenalty = (1 - shooter.stamina) * 0.18;
  const highJumperPenalty = shotType === "jumper" ? 0.06 : 0;
  const fadeawayPenalty = shotType === "fadeaway" ? 0.08 : 0;

  let finalAccuracy;
  if(shotType === "hook"){
    const hookRating = shooter.profile.hook || 0.75;
    const closeRangeBonus = THREE.MathUtils.clamp((5.3 - dist) * 0.045,0,0.12);
    finalAccuracy = hookRating * 0.48 + timingQuality * 0.32 + closeRangeBonus - contest.value * 0.65 - 0.16;
  }else{
    finalAccuracy =
      shooter.profile.shot * 0.45 +
      timingQuality * 0.4 + meterProfile.baseMakeBonus -
      getContestPenalty(shooter) -
      distancePenalty -
      movementPenalty -
      rushedPenalty -
      fatiguePenalty -
      highJumperPenalty -
      fadeawayPenalty;
  }
  if(timing.grade === "Green Release") finalAccuracy += meterProfile.baseMakeBonus + (1-contest.value)*0.12;
  if(contest.value > 0.72 && timing.grade === "Green Release") finalAccuracy -= 0.08;
  finalAccuracy = THREE.MathUtils.clamp(finalAccuracy,0.05,0.985);
  const target = HOOP_POS.clone();
  if(shotType === "layup") target.y += 0.14;
  else if(shotType === "hook") target.y += 0.1;
  else target.y += 0.05;
  const missAmount = 1-finalAccuracy;
  const shotLine = HOOP_POS.clone().sub(releasePoint).setY(0).normalize();
  const shotSide = new THREE.Vector3(-shotLine.z,0,shotLine.x);
  const early = timing.grade.includes("Early");
  const late = timing.grade.includes("Late");
  const timingDirection = early ? 1 : late ? -1 : 0;
  const timingSeverity = Math.abs(timing.pct-((meterProfile.greenStart+meterProfile.greenEnd)*.5));
  target.addScaledVector(shotLine,timingDirection*timingSeverity*1.8*missAmount);
  target.addScaledVector(shotSide,(Math.random()-.5)*(1.25+fatiguePenalty*2.4)*missAmount);
  target.addScaledVector(shooter.velocity.clone().setY(0),.045*missAmount);
  const contestDefender = shooter.id==="p1"?p2:p1;
  if(contestDefender&&contest.value>.1){
    const contestPush = shooter.group.position.clone().sub(contestDefender.group.position).setY(0);
    if(contestPush.lengthSq()>.001) target.addScaledVector(contestPush.normalize(),contest.value*.42*missAmount);
  }
  target.x += (Math.random()-.5)*.42*missAmount;
  target.z += (Math.random()-.5)*.42*missAmount;
  const power = THREE.MathUtils.clamp(shotPower,.12,1);
  const flightTime = THREE.MathUtils.lerp(.84,1.42,power) + (shotType === "jumper" ? .18 : 0) + (shotType === "hook" ? .12 : 0) + (shotType === "fadeaway" ? .08 : 0);
  const needed = target.clone().sub(releasePoint);
  ballVelocity.x = needed.x / flightTime;
  ballVelocity.z = needed.z / flightTime;
  ballVelocity.y = (needed.y - .5*GRAVITY*flightTime*flightTime) / flightTime;
  ballVelocity.y += getArcBonus(shooter,shotType) + (options.arcBonus || 0);
  ballSpin.set(shooter.velocity.z*.08,-(7.2+power*2.4),-shooter.velocity.x*.08);
  if(shotType === "hook"){
    triggerShotSplash(releasePoint.clone(),"blue");
    spawnCourtThemedParticle(releasePoint.clone(),"hook");
  }

  shotPower = 0;
  shooter.moveMeta.chargeHeld = 0;
  shooter.stats.fga += 1;
  if(lastShotValue === 3) shooter.stats.tpa += 1;
  if(timing.grade === "Green Release"){
    shooter.stats.perfect += 1;
    triggerShotSplash(releasePoint,"green");
    triggerCrowdReaction("green",0.8);
  }
  const shotLabel = shotType === "hook" ? "Hook Shot" : shotType === "fadeaway" ? "Fadeaway" : shotType === "jumper" ? "High Jumper" : shotType === "setshot" ? "Set Shot" : (shotType === "pullup" ? "Pull-Up" : shotType === "stepback" ? "Step-Back" : "Jumper");
  showShotBreakdown(timing.grade,shotLabel,contest.label,"In Flight");
  playShotSound();
  flash(`${shooter.id.toUpperCase()} ${shotLabel.toLowerCase()}!`);
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
  }
  if(shotType === "fadeaway"){
    options.releasePoint = getShotReleasePoint(char,"jumper").clone().add(new THREE.Vector3(0,0.25 + char.profile.height * 0.18,0));
    options.arcBonus = 0.55;
  }
  releaseShot(char,shotType,options);
}

function botShoot(){
  shotPower = .65;
  shootBall(p2);
}

function startLayup(char){
  if(ballState !== "dribble") return;
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
  char.yVel = 3.9 * char.profile.jump;
  char.onGround = false;
  flash(`${char.id.toUpperCase()} layup!`);
}

function startDunk(char){
  if(ballState !== "dribble") return;
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
  const toRim = HOOP_POS.clone().sub(char.group.position).setY(0);
  const toRimDir = toRim.lengthSq() > 0.0001 ? toRim.clone().normalize() : getForward(char.angle).clone().normalize();
  const right = getBodyRightVector(char).normalize();
  const sideSign = right.dot(toRimDir) > 0 ? 1 : -1;
  const style =
    (char.profile.width > 1.1 || char.profile.height > 1.07) ? "twohand" :
    (char.profile.jump > 1.08 || char.profile.sprint > 1.13) ? "tomahawk" : "quick";
  const rimFront = HOOP_POS.clone().add(new THREE.Vector3(0,0,RIM_RADIUS + char.bodyRadius + 0.1));
  const takeoff = char.group.position.clone().add(toRimDir.clone().multiplyScalar(Math.min(1.05,toRim.length()*0.38)));
  const jumpBoost = THREE.MathUtils.clamp((char.profile.jump - 1) * 0.5,0,0.24);
  const profileLift = style === "twohand" ? 0.76 : style === "tomahawk" ? 0.84 : 0.68;
  const peak = rimFront.clone().add(new THREE.Vector3(sideSign*0.1,profileLift + jumpBoost,0.03));
  const attack = rimFront.clone().add(new THREE.Vector3(sideSign*0.03,0.36 + jumpBoost*0.35,-0.01));
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
    baseY: char.group.position.y,
    rootMaxY: HOOP_POS.y - (0.6 + 0.08*char.profile.height),
    gatherSink: 0.1 + (style === "twohand" ? 0.03 : 0),
    forwardBlend: style === "quick" ? 0.3 : 0.24
  };
  flash(`${char.id.toUpperCase()} dunk!`);
}

function updateDunk(dt){
  if(!dunkActive) return;
  const owner = dunkOwner === "p1" ? p1 : p2;
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
  const slamPos = meta.attack.clone().lerp(meta.landing,smoothstep(slam));
  if(t < 0.2) owner.group.position.copy(gatherPos);
  else if(t < 0.54) owner.group.position.copy(flightPos);
  else if(t < 0.76) owner.group.position.copy(rimPos);
  else owner.group.position.copy(slamPos);
  owner.group.position.y = THREE.MathUtils.clamp(owner.group.position.y,meta.baseY-0.12,meta.rootMaxY);
  owner.group.position.z = Math.max(owner.group.position.z, HOOP_POS.z + RIM_RADIUS + owner.bodyRadius + 0.04);
  owner.group.rotation.x = -0.16 * Math.sin(gather*Math.PI) - 0.06 * Math.sin(flight*Math.PI) + 0.05 * Math.sin(slam*Math.PI);
  owner.group.rotation.y = THREE.MathUtils.lerp(owner.group.rotation.y,meta.startFacing,0.18);
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
  if(t < 0.54){
    if(meta.style === "twohand") attachBallBetweenHands(owner,new THREE.Vector3(0,0.36,-0.2));
    else attachBallToHand(owner,meta.side,new THREE.Vector3(0,0.4,-0.2));
    ballVelocity.set(0,0,0);
  }else if(t < 0.76){
    const handBias = meta.side === "right" ? "right" : "left";
    const rimHand = getWorldHand(owner,handBias);
    const targetAboveRim = HOOP_POS.clone().add(new THREE.Vector3(0,0.2,0));
    ball.position.copy(rimHand.lerp(targetAboveRim,0.58));
    ballVelocity.set(0,-0.25,-0.05);
  }else{
    const punch = smoothstep(slam);
    ball.position.copy(HOOP_POS.clone().add(new THREE.Vector3(0,0.2 - punch*0.62,0)));
    ballVelocity.set(0,-4.4 - punch*0.5,-0.08);
  }
  if(t >= .79 && !dunkScored){
    dunkScored = true;
    shotHasHitRimOrBoard = true;
    const points = 2;
    if(dunkOwner === "p1"){
      scoreP1 += points;
      p1.stats.fgm += 1;
      showShotBreakdown("Power Finish","Dunk","Heavy Contest","Made");
    }else{
      scoreP2 += points;
      p2.stats.fgm += 1;
      showShotBreakdown("Power Finish","Dunk","Heavy Contest","Made");
    }
    updateScoreboard();
    animateNetSwish();
    triggerNetWobble(meta.style === "twohand" ? 1.55 : 1.35,HOOP_POS.clone().add(new THREE.Vector3(0,-0.1,0)),new THREE.Vector3(0.02,-1,-0.06));
    playCrowdPulse();
    triggerShotSplash(HOOP_POS.clone(),"orange");
    showMoveFeedback(Math.random() < 0.5 ? "POSTER DUNK!" : "SLAM!",0.9);
    crowdWave = Math.max(crowdWave,1.1);
    triggerCrowdReaction("dunk",1.05);
    inputManager.vibrate(owner.id,1,120);
    inputManager.vibrate(owner.id==="p1"?"p2":"p1",.36,70);
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
    checkGameWinner();
    setTimeout(()=>resetPossession(nextPossessionAfterScore(dunkOwner),true),620);
  }
  if(rimHangActive){
    rimHangTimer -= dt;
    owner.group.position.y = Math.min(meta.rootMaxY - 0.04,Math.max(owner.group.position.y,HOOP_POS.y - 0.62));
    if(rimHangTimer <= 0) rimHangActive = false;
  }
  if(t >= 1 && !rimHangActive){
    dunkActive = false;
    if(dunkScored){
      ballState = "shot";
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

function applyBlockDeflection(blocker,shooter){
  const blockerForward = getForward(blocker.angle);
  const handSide = Math.random() < .5 ? "left" : "right";
  const handPoint = getWorldHand(blocker,handSide);
  const away = ball.position.clone().sub(handPoint);
  if(away.lengthSq() < 0.0001) away.copy(ball.position).sub(blocker.group.position);
  away.normalize();
  const power = 2.4 + blocker.profile.block * 1.8 + (blocker.blockBoost ? 1.4 : 0) + Math.max(0,blocker.yVel) * 0.15;
  ballVelocity.copy(away.multiplyScalar(power).add(blockerForward.multiplyScalar(1.2)));
  ballVelocity.y = Math.max(1.8,2.0 + blocker.profile.block * 0.5);
  ballState = "loose";
  clearShotCreationStates();
  layupActive = false;
  loosePickupDelay = Math.max(loosePickupDelay,.22);
}

function checkSteals(){
  if(ballState !== "dribble") return;
  const defender = possession === "p1" ? p2 : p1;
  const ballHandler = defender === p1 ? p2 : p1;
  if(defender.swipeTime <= 0) return;
  const side = defender.swipeSide || "right";
  const hand = getWorldHand(defender,side);
  const forearm = side === "left" ? defender.leftForearm : defender.rightForearm;
  const forearmWorld = forearm.getWorldPosition(new THREE.Vector3());
  const reachPoint = getReachHitPoint(defender,side);
  const reachDist = Math.min(hand.distanceTo(ball.position),reachPoint.distanceTo(ball.position),forearmWorld.distanceTo(ball.position));
  if(!isFacingTarget(defender,ball.position,0.15)) return;
  if(!hasCleanStealPath(defender,ballHandler,hand,ball.position)) return;
  if(reachDist < .52 * defender.profile.steal * getGameplayTuning().stealWindow){
    gainPossession(defender.id);
    flash(`${defender.id.toUpperCase()} steal!`);
    showShotFeedback("Stripped!");
    triggerCrowdReaction("steal",0.75);
    defender.confidence = Math.min(1,defender.confidence + .08);
    defender.stats.steals += 1;
    addTakeover(10,defender);
    inputManager.vibrate(defender.id,.42,70);
    inputManager.vibrate(ballHandler.id,.24,55);
    screenShake = Math.min(.22,screenShake + .08); applyCameraShake(.09,"block");
  }
}

function checkBlocksAndCatches(){
  if(ballState !== "shot" && ballState !== "layup") return;

  const shooterId = ballState === "layup" ? layupOwner : lastShotShooter;
  const blocker = shooterId === "p1" ? p2 : p1;
  if(blocker.blockTime <= 0 && blocker.handsUpTime <= 0) return;

  const handL = getWorldHand(blocker,"left");
  const handR = getWorldHand(blocker,"right");
  const handRadius = .14 + blocker.profile.block*.045;
  const sweptHandContact = segmentSphereIntersect(previousBallPosition,ball.position,handL,handRadius) ||
    segmentSphereIntersect(previousBallPosition,ball.position,handR,handRadius);
  const near = sweptHandContact ? 0 : Math.min(handL.distanceTo(ball.position),handR.distanceTo(ball.position));
  const shooter = shooterId === "p1" ? p1 : p2;
  let threshold = (ballState === "layup" ? .68 : .62) * blocker.profile.block + (blocker.blockBoost ? .24 : 0);
  const shotTiming = shooter && shooter.moveMeta && shooter.moveMeta.chargeHeld ? THREE.MathUtils.clamp(shooter.moveMeta.chargeHeld/1.2,0,1):.55;
  const sDiff = difficultySettings[selectedDifficulty];
  const timingError = Math.abs(shotTiming - (ballState === "layup" ? .62 : .58));
  threshold *= THREE.MathUtils.clamp(1.06 - timingError * (0.35 + (1-sDiff.contestTiming)*0.22),0.82,1.08);
  if(ballState === "shot" && shooter && shooter.moveMeta && shooter.moveMeta.lastShotType === "jumper") threshold *= .86;

  if(ballState === "layup"){
    const perfect = layupBlockWindow > 0 && layupTimer > .17 && layupTimer < .58;
    if(near < threshold && perfect){
      layupBlocked = true;
      layupScored = false;
      layupActive = false;
      ballState = "loose";
      layupOwner = null;
      applyBlockDeflection(blocker,shooter);
      flash("LAYUP BLOCKED!");
      showShotFeedback("Blocked");
      addBlockStat(blocker,12);
      triggerCrowdReaction("block",0.95);
      playBlockSound();
      inputManager.vibrate(blocker.id,.86,105);
      inputManager.vibrate(shooter.id,.46,85);
      screenShake = Math.min(.3,screenShake + .14); applyCameraShake(.16,"block");
    }
    return;
  }

  if(near < threshold){
    const catchChance = .18 + (blocker.profile.block-1)*.2 + (blocker.profile.steal-1)*.12 + (blocker.handsUpTime>0 ? .18 : 0);
    if(ballVelocity.y < 1.5 && ball.position.y < HOOP_POS.y - 0.18 && Math.random() < catchChance){
      gainPossession(blocker.id);
      flash(`${blocker.id.toUpperCase()} caught the shot!`);
    }else{
      applyBlockDeflection(blocker,shooter);
      flash(`${blocker.id.toUpperCase()} blocked it!`);
      showShotFeedback("Blocked");
      addBlockStat(blocker,10);
      triggerCrowdReaction("block",0.88);
      playBlockSound();
      inputManager.vibrate(blocker.id,.78,95);
      inputManager.vibrate(shooter.id,.4,75);
      screenShake = Math.min(.28,screenShake + .12); applyCameraShake(.13,"block");
    }
  }
}

function checkFouls(dt){
  if(ballState !== "dribble") return;
  const dist = flatDistance(p1.group.position,p2.group.position);
  const bodying = (p1.bodyUpTimer > 0.08 || p2.bodyUpTimer > 0.08);
  if(dist < 1.03) foulMeter += dt * (bodying ? .12 : .18);
  else foulMeter = Math.max(0,foulMeter - dt*.08);

  if(foulMeter >= 1){
    flash("Foul! Possession stays.");
    resetPossession(possession,true);
  }
}

function checkScore(){
  if(lastScored) return;
  const dist = flatDistance(ball.position,HOOP_POS);

  if(dist < RIM_RADIUS*.72 && ball.position.y < HOOP_POS.y && ball.position.y > HOOP_POS.y-.42 && ballVelocity.y < 0){
    lastScored = true;
    rimTouchCount = 0;
    rimCollisionCooldown = 0;
    backboardCollisionCooldown = 0;

    const shooterId = ballState === "layup" ? layupOwner : (dunkActive ? dunkOwner : lastShotShooter);
    const points = shotValueFor(shooterId);
    if(shooterId === "p1"){
      scoreP1 += points;
      p1.stats.fgm += 1;
      if(points===3) p1.stats.tpm += 1;
      flash(`P1 scores ${points}!`);
      const shotKind = p1.moveMeta.lastShotType === "hook" ? "Hook Shot" : (points===3?"Jumper":"Layup");
      showShotBreakdown("Good",shotKind,p1.lastContestLabel,"Made");
      animateNetSwish();
      triggerNetWobble(points===3 ? 1.1 : 0.95);
      playSwishSound();
      playCrowdPulse();
      triggerShotSplash(HOOP_POS.clone(),points===3?"gold":(p1.moveMeta.lastShotType==="hook"?"blue":"green"));
      triggerCrowdReaction(points===3?"three":(p1.moveMeta.lastShotType==="hook"?"hook_make":"make"),points===3?0.95:0.72);
      updateMomentum(p1, true, points, p1.lastContestLabel);
      updateMomentum(p2, false, 0, p1.lastContestLabel);
      setTimeout(() => resetPossession(nextPossessionAfterScore("p1"),true),900);
    }else{
      scoreP2 += points;
      p2.stats.fgm += 1;
      if(points===3) p2.stats.tpm += 1;
      flash(`P2 scores ${points}!`);
      const shotKind = p2.moveMeta.lastShotType === "hook" ? "Hook Shot" : (points===3?"Jumper":"Layup");
      showShotBreakdown("Good",shotKind,p2.lastContestLabel,"Made");
      animateNetSwish();
      triggerNetWobble(points===3 ? 1.1 : 0.95);
      playSwishSound();
      playCrowdPulse();
      triggerShotSplash(HOOP_POS.clone(),points===3?"gold":(p2.moveMeta.lastShotType==="hook"?"blue":"green"));
      triggerCrowdReaction(points===3?"three":(p2.moveMeta.lastShotType==="hook"?"hook_make":"make"),points===3?0.95:0.72);
      updateMomentum(p2, true, points, p2.lastContestLabel);
      updateMomentum(p1, false, 0, p2.lastContestLabel);
      setTimeout(() => resetPossession(nextPossessionAfterScore("p2"),true),900);
    }

    updateScoreboard();
    checkGameWinner();
    setTimeout(() => lastScored = false,1000);
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
  [p1,p2].forEach(pl=>{
    if(!pl) return;
    const hand = pl.group.localToWorld(new THREE.Vector3(pl.w*0.36,1.46*pl.h,0));
    const d = hand.distanceTo(HOOP_POS);
    if(d < 0.55 && pl.velocity.y > -0.2){
      const dunkBoost = (pl.dunking || ballState === "dunk") ? 0.62 : 0.28;
      triggerNetWobble(dunkBoost,hand,pl.velocity.clone().setY(0.08));
    }
  });
}
function nextPossessionAfterScore(scorer){
  if(gameMode.makeItTakeIt) return scorer;
  if(gameMode.losersBall) return scorer === "p1" ? (scoreP1 <= scoreP2 ? "p1":"p2") : (scoreP2 <= scoreP1 ? "p2":"p1");
  return scorer === "p1" ? "p2" : "p1";
}
function endGame(winner){
  gameOver = true;
  possessionFrozen = true;
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
  const newOwner = who === "p1" ? p1 : p2;
  const oldOwner = who === "p1" ? p2 : p1;
  possession = who;
  ballOwner = who;
  lastShotShooter = who;
  ballState = "dribble";
  clearShotCreationStates();
  ballVelocity.set(0,0,0);
  ballSpin.set(0,0,0);
  shotCharging = false;
  chargingPlayer = null;
  shotPower = 0;
  hasShotThisPossession = false;
  layupActive = false;
  layupOwner = null;
  layupScored = false;
  dunkActive = false;
  dunkOwner = null;
  foulMeter = 0;
  loosePickupDelay = .12;
  ballCollisionCooldown = 0;
  rimCollisionCooldown = 0;
  backboardCollisionCooldown = 0;
  rimTouchCount = 0;
  shotHasHitRimOrBoard = false;
  crossover.active = false;
  crossover.progress = 1;

  p1.hasBall = who === "p1";
  p2.hasBall = who === "p2";
  if(oldOwner) oldOwner.hasBall = false;

  ballSideValue = 1;
  ballSide = "right";

  const secureHand = getWorldHand(newOwner,"right");
  const chest = newOwner.group.position.clone()
    .add(new THREE.Vector3(0,1.24*newOwner.profile.height,0))
    .add(getForward(newOwner.angle).multiplyScalar(0.22));
  ball.position.copy(secureHand.lerp(chest,.55));
}

function resetPossession(who,reposition){
  if(gameOver) return;
  possession = who;
  ballOwner = who;
  lastShotShooter = who;
  ballState = "dribble";
  clearShotCreationStates();
  lastShotValue = 2;
  lastShotWasHook = false;
  lastShotReleasePos.set(0,0,0);
  ballSide = "right";
  ballSideValue = 1;
  ballVelocity.set(0,0,0);
  ballSpin.set(0,0,0);
  shotCharging = false;
  chargingPlayer = null;
  shotPower = 0;
  hasShotThisPossession = false;
  layupActive = false;
  dunkActive = false;
  dunkOwner = null;
  foulMeter = 0;
  loosePickupDelay = 0;
  ballCollisionCooldown = 0;
  rimCollisionCooldown = 0;
  backboardCollisionCooldown = 0;
  rimTouchCount = 0;
  shotHasHitRimOrBoard = false;
  crossover.active = false;
  crossover.progress = 1;

  p1.hasBall = who === "p1";
  p2.hasBall = who === "p2";

  resetBody(p1);
  resetBody(p2);

  if(reposition){
    if(who === "p1"){
      p1.group.position.copy(P1_OFFENSE_START);
      p2.group.position.copy(P2_DEFENSE_START);

      p1.angle = Math.PI;
      p2.angle = angleToward(p2.group.position,p1.group.position);

      botStrategy = "defense";
    }else{
      p2.group.position.copy(P2_OFFENSE_START);
      p1.group.position.copy(P1_DEFENSE_START);

      p2.angle = Math.PI;
      p1.angle = angleToward(p1.group.position,p2.group.position);

      botStrategy = chooseBotStrategy();
    }
  }

  startCountdown();
}

function rematchSamePlayers(){
  if(!gameStarted) return;
  scoreP1 = 0; scoreP2 = 0; gameOver = false; possessionFrozen = false;
  hideWinnerOverlay();
  updateScoreboard();
  initBotBrain();
  p1.confidence = 0; p2.confidence = 0; p1.hotStreak = 0; p2.hotStreak = 0;
  p1.moveState = p2.moveState = "normal";
  p1.sideHopCooldown = p2.sideHopCooldown = 0;
  p1.pumpFakeCooldown = p2.pumpFakeCooldown = 0;
  resetPossession("p1",true);
}
function returnToCharacterMenu(){
  gameOver = false;
  hideWinnerOverlay();
  showQuickPlaySetup();
}
function showWinnerOverlay(winner){
  document.getElementById("winnerText").textContent = winner === "p1" ? "P1 WINS!" : "P2 WINS!";
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
  if(!p1 || !p2) return;
  const line = `P1 ${p1.stats.fgm}/${p1.stats.fga} FG, STL ${p1.stats.steals}, BLK ${p1.stats.blocks} | P2 ${p2.stats.fgm}/${p2.stats.fga} FG, STL ${p2.stats.steals}, BLK ${p2.stats.blocks}`;
  document.getElementById("winnerSub").textContent = line;
}
function buildBroadcastScorebug(){}

function resetBody(char){
  char.group.position.y = 0;
  char.yVel = 0;
  char.onGround = true;
  char.blockBoost = false;
  char.blockTime = 0;
  char.handsUpTime = 0;
  char.swipeTime = 0;
  char.shotMotionTime = 0;
  char.moveState = "normal";
  char.moveTimer = 0;
  char.moveDuration = 0;
  char.stumbledTimer = 0;
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

function startCountdown(){
  countdownTimer = 3;
  possessionFrozen = true;
}
