function updateBall(dt){
  rimCollisionCooldown = Math.max(0, rimCollisionCooldown - dt);
  reboundPredictionTimer = Math.max(0, reboundPredictionTimer - dt);
  outOfBoundsCooldown = Math.max(0,outOfBoundsCooldown-dt);
  boxOutFeedbackTimer = Math.max(0, boxOutFeedbackTimer - dt);
  tipFeedbackTimer = Math.max(0, tipFeedbackTimer - dt);
  scrambleState.timer = Math.max(0, scrambleState.timer - dt);

  // Safety fix: create ballShadow if this version of the file never created it in buildBall().
  // You should still declare `let ballShadow;` near the top with your other globals.
  if (typeof ballShadow === "undefined") {
    window.ballShadow = null;
  }

  if (!ballShadow && scene && ball) {
    ballShadow = new THREE.Mesh(
      new THREE.CircleGeometry(BALL_RADIUS * 2.15, 32),
      new THREE.MeshBasicMaterial({
        color: 0x000000,
        transparent: true,
        opacity: 0.34,
        depthWrite: false
      })
    );
    ballShadow.rotation.x = -Math.PI / 2;
    ballShadow.position.set(ball.position.x, 0.022, ball.position.z);
    scene.add(ballShadow);
  }

  if(ballState === "dribble"){
    const owner = getOwner();
    if (!owner) return;

    const forward = getForward(owner.angle);
    const right = new THREE.Vector3(forward.z, 0, -forward.x);

    let sideValue = ballSideValue;
    let diagonalPush = 0;

    if(crossover.active && crossover.player === owner){
      const handleTightness = THREE.MathUtils.clamp((owner.profile.handle - 0.86) * 0.85, 0, 0.55);
      crossover.progress += dt * (2.35 + handleTightness * 2.6) * owner.profile.handle;
      crossover.riskWindow = Math.max(0, crossover.riskWindow - dt);

      const eased = smoothstep(THREE.MathUtils.clamp(crossover.progress, 0, 1));
      sideValue = THREE.MathUtils.lerp(crossover.from, crossover.to, eased);
      diagonalPush = Math.sin(eased * Math.PI) * 0.62;

      if(crossover.progress >= 1){
        ballSideValue = crossover.to;
        ballSide = ballSideValue < 0 ? "left" : "right";
        crossover.active = false;
        crossover.progress = 1;
        crossover.riskWindow = 0;
      }
    }

    const speed = owner.velocity.length();
    const sprinting = owner.sprintMul > 1.18 || speed > 4.65;
    const backpedal = owner.inputZ < -0.2;
    const sideHop = owner.moveState === "sidehop" || owner.moveState === "sidejump";
    const speedMode = speed < 1.2 ? "idle" : sprinting ? "sprint" : backpedal ? "backpedal" : "walk";

    const handleQuality = THREE.MathUtils.clamp((owner.profile.handle - 0.8) / 0.45, 0, 1);
    const handleLoose = 1 - handleQuality;
    const flavor = getDribbleFlavor(owner);

    const bouncePhase =
      owner.dribbleBounce *
      (speedMode === "idle" ? 0.82 :
       speedMode === "sprint" ? 1.24 :
       speedMode === "backpedal" ? 0.96 : 1.05) *
      flavor.bounceSpeed;

    const bounceWave = Math.sin(bouncePhase);
    const bounce = Math.abs(bounceWave);
    const bounceCurve = Math.pow(bounce, speedMode === "sprint" ? 0.74 : speedMode === "backpedal" ? 1.1 : 0.9);

    const sprintPush =
      speedMode === "sprint" ? 0.26 :
      speedMode === "walk" ? 0.13 :
      speedMode === "idle" ? 0.06 : 0.08;

    const crossSnap =
      crossover.active && crossover.player === owner
        ? Math.sin(THREE.MathUtils.clamp(crossover.progress, 0, 1) * Math.PI)
        : 0;

    const lateralBase =
      (0.11 + handleLoose * 0.08 + (speedMode === "sprint" ? 0.05 : 0)) *
      owner.profile.width;

    const wobble =
      Math.sin(owner.dribbleBounce * 1.8 + speed * 0.45) *
      (0.018 + handleLoose * 0.045) *
      (sprinting ? 1.35 : 1) *
      flavor.wobble;

    const lateral =
      lateralBase * sideValue +
      wobble +
      crossSnap * 0.06 * crossover.to * flavor.cross;

    const forwardPush =
      (0.08 + sprintPush + diagonalPush * 0.52 + crossSnap * 0.08 + (owner.profile.height - 1) * 0.03) *
      flavor.forward;

    const handName = sideValue < 0 ? "left" : "right";

    const dribbleOffset = right
      .clone()
      .multiplyScalar(lateral)
      .add(forward.clone().multiplyScalar(forwardPush));

    attachBallToHand(owner, handName, dribbleOffset);

    const lowY = owner.group.position.y + BALL_RADIUS + 0.05;
    const minSafeY = owner.group.position.y + BALL_RADIUS + 0.03;
    const highY =
      owner.group.position.y +
      THREE.MathUtils.clamp(
        1.02 + (owner.profile.height - 1) * 0.16 + (speedMode === "sprint" ? 0.1 : speedMode === "idle" ? -0.04 : 0),
        0.94,
        1.2
      );

    const bounceY = THREE.MathUtils.lerp(lowY, highY, bounceCurve);
    const handCarryY = ball.position.y;
    const blendToHand = THREE.MathUtils.clamp(
      0.18 +
      bounceCurve * 0.72 +
      (speedMode === "sprint" ? 0.06 : 0) +
      (sideHop ? 0.06 : 0),
      0.14,
      0.94
    );

    const targetY = THREE.MathUtils.lerp(bounceY, handCarryY, blendToHand);

    ball.position.y = THREE.MathUtils.lerp(
      ball.position.y,
      targetY,
      THREE.MathUtils.clamp(dt * (16 + handleQuality * 10), 0, 1)
    );

    ball.position.y = Math.max(minSafeY, ball.position.y);

    const dribbleNearFloor = ball.position.y <= (owner.group.position.y + BALL_RADIUS + 0.08);
    if(dribbleNearFloor){
      ball.scale.set(1.055,0.945,1.055);
      // Hook for future bounce SFX at dribble impact.
    }

    owner.anim.crossLean = crossSnap * crossover.to * (0.14 + handleLoose * 0.07);
    owner.anim.crossPlant = crossSnap * (0.18 + handleLoose * 0.08);
    owner.anim.crossFake = crossSnap * (-crossover.to) * (0.08 + flavor.cross * 0.02);
  }

  if(ballState === "shot" || ballState === "loose"){
    previousBallPosition.copy(ball.position);
    if(ballState === "shot" && ballVelocity.length() > 7.6 && Math.random() < dt * 18){
      createShotTrail(lastShotWasHook ? "hook" : "shot");
    }

    ballVelocity.y += GRAVITY * dt;
    ball.position.add(ballVelocity.clone().multiplyScalar(dt));
    ball.rotation.x += ballSpin.x*dt;
    ball.rotation.y += ballSpin.y*dt;
    ball.rotation.z += ballSpin.z*dt;
    ballSpin.multiplyScalar(Math.max(.94,1-dt*.55));

    keepBallInBounds();

    if(ball.position.y < BALL_RADIUS){
      ball.position.y = BALL_RADIUS;
      const bounceTuning=getGameplayTuning().ballBounce;
      ballVelocity.copy(reflectVelocity(new THREE.Vector3(0, 1, 0), 0.58*bounceTuning, 0.76));
      ballSpin.multiplyScalar(.78);

      if(Math.abs(ballVelocity.y) < 1.2 && ballState === "shot"){
        ballState = "loose";
      }

      ball.scale.set(1.08, 0.92, 1.08);
      playDribbleSound();
      updatePredictedReboundPoint(true);
    }

    resolveBackboardBallCollision(dt);
    handleBackboardAndRim();
    checkBallPlayerBodyCollision(p1);
    checkBallPlayerBodyCollision(p2);
    checkScore();
    handleLooseBallPickup();

    if(ballState === "loose" && flatDistance(ball.position, HOOP_POS) > 2.8){
      rimTouchCount = 0;
    }
  }

  if(ballState === "dribble"){
    const owner = getOwner();
    const floorY = owner ? owner.group.position.y + BALL_RADIUS : BALL_RADIUS;
    const floorGap = ball.position.y - floorY;
    if(floorGap < 0.085){
      const compress = THREE.MathUtils.clamp((0.085 - floorGap) / 0.085, 0, 1);
      const xzStretch = 1 + compress * 0.065;
      const ySquash = 1 - compress * 0.06;
      ball.scale.set(xzStretch, ySquash, xzStretch);
    }
  }

  const moveDelta = ball.position.clone().sub(lastBallVisualPos);
  const moveLen = moveDelta.length();
  if(Number.isFinite(moveLen) && moveLen > 0.0001){
    const rollAmount = moveLen / BALL_RADIUS;
    ball.rotation.x += THREE.MathUtils.clamp(moveDelta.z / BALL_RADIUS, -0.38, 0.38) * (1.25 + rollAmount * 0.24);
    ball.rotation.z -= THREE.MathUtils.clamp(moveDelta.x / BALL_RADIUS, -0.38, 0.38) * (1.25 + rollAmount * 0.24);

    const spinFromY = THREE.MathUtils.clamp(ballVelocity.y * 0.015, -0.12, 0.12);
    const spinFromLateral = THREE.MathUtils.clamp((ballVelocity.x + ballVelocity.z) * 0.008, -0.1, 0.1);
    ball.rotation.y += spinFromY + spinFromLateral;

    if(!Number.isFinite(ball.rotation.x) || !Number.isFinite(ball.rotation.y) || !Number.isFinite(ball.rotation.z)){
      ball.rotation.set(0,0,0);
    }
  }
  lastBallVisualPos.copy(ball.position);

    // Strong visible ground shadow for judging ball height/distance.
  if(ballShadow){
    ballShadow.position.set(ball.position.x, 0.012, ball.position.z);

    const height = Math.max(0, ball.position.y - BALL_RADIUS);
    const shadowScale = THREE.MathUtils.clamp(0.58 + height * 0.22, 0.58, 1.24);
    const targetOpacity = THREE.MathUtils.clamp(0.5 - height * 0.1, 0.13, 0.5);

    ballShadow.scale.set(shadowScale, shadowScale * 0.72, shadowScale);
    ballShadow.material.opacity = THREE.MathUtils.lerp(ballShadow.material.opacity, targetOpacity, dt * 8);
  }

  ball.scale.lerp(new THREE.Vector3(1, 1, 1), dt * 12);
}

function resolveBackboardBallCollision(dt){
  if(!backboard) return;
  if(backboardCollisionCooldown > 0) return;

  const boardBox = new THREE.Box3().setFromObject(backboard);
  const min = boardBox.min;
  const max = boardBox.max;
  const frontPlaneZ = max.z + BALL_RADIUS;
  const backPlaneZ = min.z - BALL_RADIUS;

  const prevPos = previousBallPosition;
  const currPos = ball.position;

  const withinX = currPos.x >= min.x - BALL_RADIUS && currPos.x <= max.x + BALL_RADIUS;
  const withinY = currPos.y >= min.y - BALL_RADIUS && currPos.y <= max.y + BALL_RADIUS;
  if(!withinX || !withinY) return;

  const crossedFront = prevPos.z > frontPlaneZ && currPos.z <= frontPlaneZ;
  const crossedBack = prevPos.z < backPlaneZ && currPos.z >= backPlaneZ;
  if(!crossedFront && !crossedBack){
    const expanded = boardBox.clone().expandByScalar(BALL_RADIUS);
    if(!expanded.containsPoint(currPos)) return;
  }

  const hitFromFront = prevPos.z > max.z;
  const normalZ = hitFromFront ? 1 : -1;

  ball.position.z = hitFromFront ? max.z + BALL_RADIUS + 0.025 : min.z - BALL_RADIUS - 0.025;
  ballVelocity.z = Math.abs(ballVelocity.z || 0.01) * normalZ * 0.72;
  ballVelocity.x *= 0.92;
  ballVelocity.y *= 0.82;

  if(!Number.isFinite(ballVelocity.x) || !Number.isFinite(ballVelocity.y) || !Number.isFinite(ballVelocity.z)){
    ballVelocity.set(0,0,0);
  }

  shotHasHitRimOrBoard = true;
  backboardCollisionCooldown = 0.08;
  triggerBackboardImpact(0.035,0.16);
  updatePredictedReboundPoint(true);
  handleBankShot();
  spawnParticle(ball.position,0x88d7ff);
  playRimSound();
}

function triggerBackboardImpact(strength = 0.035,duration = 0.16){
  backboardImpactTimer = Math.max(backboardImpactTimer,duration);
  backboardImpactStrength = Math.max(backboardImpactStrength,strength);
}

function keepBallInBounds(){
  const minX = -COURT_W/2 + BALL_RADIUS + .15;
  const maxX = COURT_W/2 - BALL_RADIUS - .15;
  const minZ = -COURT_L/2 + BALL_RADIUS + .15;
  const maxZ = COURT_L/2 - BALL_RADIUS - .15;

  const outside = ball.position.x<minX || ball.position.x>maxX || ball.position.z<minZ || ball.position.z>maxZ;
  if(outside&&getGameplayTuning().outOfBounds&&outOfBoundsCooldown<=0){
    outOfBoundsCooldown=.8;
    const nextOwner=lastShotShooter==="p1"?"p2":"p1";
    ball.position.x=THREE.MathUtils.clamp(ball.position.x,minX,maxX);
    ball.position.z=THREE.MathUtils.clamp(ball.position.z,minZ,maxZ);
    ballVelocity.set(0,0,0);
    ballSpin.multiplyScalar(.2);
    ballState="dead";
    updateLEDBoards("OUT OF BOUNDS");
    setTimeout(()=>{ if(gameStarted&&!gameOver) resetPossession(nextOwner,true); },420);
    return;
  }

  if(ball.position.x < minX){
    ball.position.x = minX;
    ballVelocity.copy(reflectVelocity(new THREE.Vector3(1,0,0),.72,.92));
  }
  if(ball.position.x > maxX){
    ball.position.x = maxX;
    ballVelocity.copy(reflectVelocity(new THREE.Vector3(-1,0,0),.72,.92));
  }
  if(ball.position.z < minZ){
    ball.position.z = minZ;
    ballVelocity.copy(reflectVelocity(new THREE.Vector3(0,0,1),.72,.92));
  }
  if(ball.position.z > maxZ){
    ball.position.z = maxZ;
    ballVelocity.copy(reflectVelocity(new THREE.Vector3(0,0,-1),.72,.92));
  }
}

function handleBackboardAndRim(){
  const rimFlat = new THREE.Vector3(ball.position.x-HOOP_POS.x,0,ball.position.z-HOOP_POS.z);
  const rimDist = rimFlat.length();
  const yDiff = Math.abs(ball.position.y-HOOP_POS.y);

  const centeredDrop = rimDist < RIM_RADIUS*.56 && ball.position.y < HOOP_POS.y + .4 && ballVelocity.y < 0;
  if(centeredDrop) return;
  if(rimCollisionCooldown <= 0 && rimDist < RIM_RADIUS + BALL_RADIUS && rimDist > RIM_RADIUS - BALL_RADIUS && yDiff < .23){
    const n = rimFlat.normalize();
    rimCollisionCooldown = .09;
    rimTouchCount++;
    shotHasHitRimOrBoard = true;
    ball.position.add(n.clone().multiplyScalar(.08));
    ballVelocity.copy(reflectVelocity(new THREE.Vector3(n.x,0,n.z),.68,.9));
    ballVelocity.y = Math.max(Math.abs(ballVelocity.y)*.45 + .8,.9);
    updatePredictedReboundPoint(true);
    handleRimRattle();
    triggerNetWobble(0.45,ball.position,ballVelocity);
    spawnParticle(ball.position,0xff9f5a);
    playRimSound();
    inputManager.vibrate(lastShotShooter,.24,48);
  }
  if(rimTouchCount > 4){
    ballState = "loose";
    const away = ball.position.clone().sub(HOOP_POS);
    away.y = 0;
    if(away.lengthSq() < .001) away.set((Math.random()-.5),0,1);
    away.normalize();
    ballVelocity.copy(away.multiplyScalar(2.2));
    ballVelocity.y = 1.4;
    rimTouchCount = 0;
    updatePredictedReboundPoint(true);
  }
}

function updatePredictedReboundPoint(force=false){
  if(!force && reboundPredictionTimer > 0) return;
  if(ballState !== "shot" && ballState !== "loose") return;
  const simPos = ball.position.clone();
  const simVel = ballVelocity.clone();
  const dt = 0.06;
  const minX = -COURT_W/2 + BALL_RADIUS + .18;
  const maxX = COURT_W/2 - BALL_RADIUS - .18;
  const minZ = -COURT_L/2 + BALL_RADIUS + .18;
  const maxZ = COURT_L/2 - BALL_RADIUS - .18;
  for(let i=0;i<54;i++){
    simVel.y += GRAVITY * dt;
    simPos.addScaledVector(simVel,dt);
    if(simPos.y <= BALL_RADIUS){
      simPos.y = BALL_RADIUS;
      break;
    }
    if(simPos.x < minX || simPos.x > maxX) simVel.x *= -0.72;
    if(simPos.z < minZ || simPos.z > maxZ) simVel.z *= -0.74;
    simPos.x = THREE.MathUtils.clamp(simPos.x,minX,maxX);
    simPos.z = THREE.MathUtils.clamp(simPos.z,minZ,maxZ);
  }
  predictedReboundPoint = simPos.clone();
  reboundPredictionTimer = 0.14;
}

function reflectVelocity(normal,bounce=.65,friction=.88){
  const reflected = ballVelocity.clone().reflect(normal).multiplyScalar(bounce);
  reflected.x *= friction;
  reflected.z *= friction;
  return reflected;
}

function checkBallPlayerBodyCollision(char){
  if(!char || ballCollisionCooldown > 0) return;
  if(ballState !== "shot" && ballState !== "loose") return;
  const bodyCenter = char.group.position.clone().add(new THREE.Vector3(0,1.12*char.profile.height,0));
  const radius = 0.42*char.profile.width + BALL_RADIUS;
  const diff = ball.position.clone().sub(bodyCenter);
  if(diff.length() < radius){
    const shooter = possession === "p1" ? p1 : p2;
    const defender = shooter === p1 ? p2 : p1;
    const isBlocking = char.blockTime > 0 || char.handsUpTime > 0 || !char.onGround;
    if(ballState === "shot" && char === defender && isBlocking){
      applyBlockDeflection(defender,shooter);
      addBlockStat(defender,10);
      showShotFeedback("BLOCKED");
      triggerCrowdReaction("block",0.85);
      return;
    }
    const n = diff.lengthSq() < 0.0001 ? getForward(char.angle).clone().multiplyScalar(-1) : diff.normalize();
    ball.position.copy(bodyCenter.clone().add(n.clone().multiplyScalar(radius + 0.02)));
    ballVelocity.copy(reflectVelocity(n,.58,.88));
    ballVelocity.y = Math.max(ballVelocity.y,0.8);
    if(ballState === "shot" && (rimTouchCount > 0 || shotHasHitRimOrBoard || (ballVelocity.y < -1 && ball.position.y < HOOP_POS.y + 0.35))){
      ballState = "loose";
    }
    loosePickupDelay = Math.max(loosePickupDelay,.18);
    ballCollisionCooldown = .06;
    flash("Deflected!");
  }
}

function canPickupLooseBall(char){
  const liveRebound = ballState === "shot" && (rimTouchCount > 0 || shotHasHitRimOrBoard || (ballVelocity.y < -1 && ball.position.y < HOOP_POS.y + 0.35));
  if(ballState !== "loose" && !liveRebound) return false;
  if(loosePickupDelay > 0) return false;

  const charPos = char.group.position;
  const flatDist = flatDistance(charPos,ball.position);
  const height = char.profile.height;
  const width = char.profile.width;

  const name = (char.profile.name || "").toLowerCase();
  const longBoard = name.includes("flash guard") || name.includes("deep range");
  const paintBoard = name.includes("glass tower") || name.includes("paint bruiser");
  const claw = name.includes("the claw");
  const groundPickupRange = (0.9 + (longBoard ? 0.16 : 0) - (paintBoard ? 0.04 : 0)) * width;
  const bodyCatchRange = 1.05 * width;
  const handCatchRange = 0.62 + char.profile.steal * 0.10 + char.profile.handle * 0.10 + (longBoard ? 0.08 : 0);
  const ballY = ball.position.y;

  if(ballY <= 0.9 && flatDist <= groundPickupRange) return true;
  if(ballY > 0.9 && ballY <= 1.85 * height && flatDist <= bodyCatchRange) return true;

  const leftHand = getWorldHand(char,"left");
  const rightHand = getWorldHand(char,"right");
  if(leftHand.distanceTo(ball.position) <= handCatchRange) return true;
  if(rightHand.distanceTo(ball.position) <= handCatchRange) return true;

  const reachCenter = char.group.position.clone().add(new THREE.Vector3(0,1.55 * height,0));
  const reachRadius = 0.72 * width + char.profile.block * 0.20 + char.profile.jump * 0.12 + (paintBoard ? 0.18 : 0) + (claw ? 0.1 : 0);
  if(ballY <= 2.45 * height && reachCenter.distanceTo(ball.position) <= reachRadius) return true;

  return false;
}

function getLooseBallPickupQuality(char){
  const speed = ballVelocity.length();
  const ballY = ball.position.y;
  let quality = 1.0;
  quality -= Math.max(0,speed - 4.5) * 0.08;
  if(ballY > 1.85 * char.profile.height) quality -= 0.18;
  if(ballY < 0.35) quality -= 0.06;
  quality += (char.profile.handle - 1) * 0.22;
  quality += (char.profile.jump - 1) * 0.08;
  quality += (char.profile.block - 1) * 0.06;
  quality += (char.profile.steal - 1) * 0.14;
  quality -= Math.max(0,char.profile.width - 1) * 0.08;
  if(char.velocity && char.velocity.length() > 5.5) quality -= 0.12;
  return THREE.MathUtils.clamp(quality,0.15,0.98);
}

function getPickupPriorityScore(char){
  const flatDist = flatDistance(char.group.position,ball.position);
  const handDist = Math.min(
    getWorldHand(char,"left").distanceTo(ball.position),
    getWorldHand(char,"right").distanceTo(ball.position)
  );
  let score = 0;
  score += (2.2 - flatDist) * 1.8;
  score += (1.5 - handDist) * 1.2;
  score += char.profile.handle * 0.35;
  score += char.profile.jump * 0.12;
  score += Math.random() * 0.15;
  return score;
}

function bobbleLooseBall(char){
  const forward = getForward(char.angle);
  const right = getBodyRightVector(char);
  const bobbleDir = forward.clone()
    .multiplyScalar(0.8)
    .add(right.clone().multiplyScalar((Math.random() - 0.5) * 1.4))
    .normalize();
  ballVelocity.copy(bobbleDir.multiplyScalar(2.3 + Math.random() * 1.6));
  ballVelocity.y = 0.8 + Math.random() * 1.4;
  loosePickupDelay = 0.18;

  char.moveState = "bobble";
  char.moveTimer = 0;
  char.moveDuration = 0.32;
  char.moveMeta = {};
  flash(`${char.id.toUpperCase()} bobbled it!`);
}

function attemptLooseBallGather(char){
  const gatherType = ball.position.y > 1.0 ? "catch" : "pickup";
  const quality = getLooseBallPickupQuality(char);
  const contested = flatDistance(p1.group.position,p2.group.position) < 1.45 && ball.position.y < 1.22;
  if(contested){
    scrambleState.active = true;
    scrambleState.timer = 0.28;
    if(scrambleState.lastText <= 0){ flash("LOOSE BALL"); scrambleState.lastText = 0.55; }
    scrambleState.lastText = Math.max(0,scrambleState.lastText - 0.08);
    char.moveState = "gather";
    char.moveTimer = 0;
    char.moveDuration = 0.26;
    char.moveMeta = char.moveMeta || {};
    char.moveMeta.gatherType = "pickup";
  }
  if(Math.random() <= quality){
    gainPossession(char.id);
    char.moveState = "gather";
    char.moveTimer = 0;
    char.moveDuration = 0.22;
    char.moveMeta = char.moveMeta || {};
    char.moveMeta.gatherType = gatherType;
    flash(gatherType === "catch" ? `${char.id.toUpperCase()} catches it!` : `${char.id.toUpperCase()} picks it up!`);
    if(ball.position.y > 1.05 && tipFeedbackTimer <= 0){
      flash("REBOUND");
      tipFeedbackTimer = 0.45;
    }
    return;
  }
  if(ball.position.y > 1.15 && char.profile.jump + char.profile.block > 2.22 && Math.random() < 0.55){
    const tipDir = getForward(char.angle).multiplyScalar(1.1 + Math.random()*1.7);
    tipDir.x += (Math.random() - 0.5) * 1.4;
    tipDir.z += (Math.random() - 0.5) * 1.4;
    ballVelocity.copy(tipDir);
    ballVelocity.y = 1.6 + Math.random()*1.5;
    ballState = "loose";
    loosePickupDelay = 0.14;
    if(tipFeedbackTimer <= 0){ flash("TIP"); tipFeedbackTimer = 0.55; }
    return;
  }
  bobbleLooseBall(char);
}

function handleLooseBallPickup(){
  if(ballState !== "loose" && ballState !== "shot") return;
  if(loosePickupDelay > 0) return;

  const p1Can = canPickupLooseBall(p1);
  const p2Can = canPickupLooseBall(p2);

  if(!p1Can && !p2Can) return;

  if(p1Can && p2Can){
    const p1Score = getPickupPriorityScore(p1);
    const p2Score = getPickupPriorityScore(p2);
    if(p1Score >= p2Score) attemptLooseBallGather(p1);
    else attemptLooseBallGather(p2);
    return;
  }

  if(p1Can) attemptLooseBallGather(p1);
  else if(p2Can) attemptLooseBallGather(p2);
}

function segmentSphereIntersect(a,b,center,radius){
  const ab = b.clone().sub(a);
  const t = THREE.MathUtils.clamp(center.clone().sub(a).dot(ab) / Math.max(ab.lengthSq(),0.0001),0,1);
  const closest = a.clone().add(ab.multiplyScalar(t));
  return closest.distanceTo(center) <= radius;
}

function checkLayupDefenderCollision(owner,defender,prevBallPos,currentBallPos){
  const bodyCenter = defender.group.position.clone().add(new THREE.Vector3(0,1.15*defender.profile.height,0));
  const bodyRadius = 0.48*defender.profile.width;
  const headCenter = defender.group.position.clone().add(new THREE.Vector3(0,2.05*defender.profile.height,0));
  const headRadius = 0.28*defender.profile.width;
  const leftHand = getWorldHand(defender,"left");
  const rightHand = getWorldHand(defender,"right");
  if(segmentSphereIntersect(prevBallPos,currentBallPos,bodyCenter,bodyRadius + BALL_RADIUS)) return {hit:true,type:"body",point:bodyCenter};
  if(segmentSphereIntersect(prevBallPos,currentBallPos,headCenter,headRadius + BALL_RADIUS)) return {hit:true,type:"body",point:headCenter};
  if(segmentSphereIntersect(prevBallPos,currentBallPos,leftHand,0.28*defender.profile.block + BALL_RADIUS)) return {hit:true,type:"hand",point:leftHand};
  if(segmentSphereIntersect(prevBallPos,currentBallPos,rightHand,0.28*defender.profile.block + BALL_RADIUS)) return {hit:true,type:"hand",point:rightHand};
  return {hit:false};
}

function isDefenderBetweenBallAndRim(defender,ballPos){
  const a = ballPos.clone();
  const b = HOOP_POS.clone();
  const bodyCenter = defender.group.position.clone().add(new THREE.Vector3(0,1.15*defender.profile.height,0));
  return segmentSphereIntersect(a,b,bodyCenter,0.52*defender.profile.width + BALL_RADIUS);
}

function deflectLayupBall(owner,defender,hit){
  layupBlocked = true;
  layupActive = false;
  layupScored = false;
  ballState = "loose";
  layupOwner = null;
  clearShotCreationStates();
  owner.hasBall = false;
  const away = ball.position.clone().sub(hit.point);
  away.y = 0.25;
  if(away.lengthSq() < 0.0001){
    away.copy(owner.group.position).sub(defender.group.position);
    away.y = 0.25;
  }
  away.normalize();
  const ownerForward = getForward(owner.angle);
  const randomness = new THREE.Vector3((Math.random()-.5)*1.2,Math.random()*.8,(Math.random()-.5)*1.2);
  const force = hit.type === "hand" ? 4.8 : 3.4;
  ballVelocity.copy(away.multiplyScalar(force).add(ownerForward.multiplyScalar(1.2)).add(randomness));
  ballVelocity.y = hit.type === "hand" ? 2.6 + Math.random()*1.2 : 1.8 + Math.random()*0.8;
  flash(hit.type === "hand" ? "LAYUP BLOCKED!" : "LAYUP DEFLECTED!");
  applyCameraShake(hit.type === "hand" ? .2 : .13,"rim");
  if(hit.type === "hand") addBlockStat(defender);
  if(hit.type === "body" && defender.blockTime <= 0 && defender.handsUpTime <= 0){
    foulMeter = Math.min(1,foulMeter + 0.12);
  }
  loosePickupDelay = Math.max(loosePickupDelay,.22);
}

function updateLayup(dt){
  if(layupBlocked) return;
  if(!layupActive) return;
  layupTimer += dt;
  layupBlockWindow = Math.max(0,layupBlockWindow - dt);

  const owner = layupOwner === "p1" ? p1 : p2;
  const defender = layupOwner === "p1" ? p2 : p1;
  const progress = THREE.MathUtils.clamp(layupTimer/layupDuration,0,1);
  const prevBallPos = ball.position.clone();

  owner.group.position.add(getForward(owner.angle).multiplyScalar(2.15*dt));
  owner.group.rotation.x = -Math.sin(progress*Math.PI)*0.1;
  owner.leftArm.rotation.z = -0.25;
  owner.rightArm.rotation.z = 0.25;
  keepInCourt(owner.group.position);

  const handSide = getBestHandForScreenSide(owner,getScreenSideVectorForChar(owner,"right").dot(HOOP_POS.clone().sub(owner.group.position)) > 0 ? "right" : "left");
  const hand = getWorldHand(owner,handSide);
  const target = HOOP_POS.clone().add(new THREE.Vector3(0,.12,.08));
  const contactLayup = owner.bodyUpTimer > 0.08 || owner.contactTimer > 0.08;
  const arcHeight = Math.sin(progress*Math.PI) * (contactLayup ? 1.08 : .9);
  if(contactLayup){ owner.group.rotation.z = THREE.MathUtils.lerp(owner.group.rotation.z,-0.12 * (owner.contactSide || 1),0.12); }

  ball.position.copy(hand.lerp(target,smoothstep(progress)));
  ball.position.y += arcHeight;
  const collision = checkLayupDefenderCollision(owner,defender,prevBallPos,ball.position.clone());
  if(collision.hit){
    deflectLayupBall(owner,defender,collision);
    return;
  }

  if(progress > .78 && layupActive && !layupBlocked && !layupScored){
    if(isDefenderBetweenBallAndRim(defender,ball.position)){
      deflectLayupBall(owner,defender,{type:"body",point:defender.group.position.clone().add(new THREE.Vector3(0,1.15*defender.profile.height,0))});
      return;
    }
    if(flatDistance(ball.position,HOOP_POS) < .9){
      const contactPenalty = contactLayup ? (0.11 - (owner.profile.layup-1)*0.1 - (owner.profile.width-1)*0.08 - (owner.contactFinishBonusOrPenalty||0)) : 0;
      if(Math.random() < THREE.MathUtils.clamp(0.92 - Math.max(0,contactPenalty),0.62,0.97)){
      layupScored = true;
      if(layupOwner === "p1"){
        scoreP1 += 2;
        flash("P1 layup!");
        updateMomentum(p1,true,2,"Good Contest");
        setTimeout(() => resetPossession(nextPossessionAfterScore("p1"),true),650);
      }else{
        scoreP2 += 2;
        flash("P2 layup!");
        updateMomentum(p2,true,2,"Good Contest");
        setTimeout(() => resetPossession(nextPossessionAfterScore("p2"),true),650);
      }
      updateScoreboard();
      triggerNetWobble(0.9);
      triggerCrowdReaction("make",0.62);
      spawnCourtThemedParticle(HOOP_POS.clone(),"make");
      checkGameWinner();
      }
    }
  }

  if(progress >= 1 && layupActive && !layupBlocked && !layupScored){
    layupActive = false;
    ballState = "loose";
    ballVelocity.set((Math.random()-.5)*1.2,1.4,1.2);
    loosePickupDelay = Math.max(loosePickupDelay,.2);
    triggerCrowdReaction("miss",0.26);
  }
}

function releaseShootOrFake(char){
  const held = char.moveMeta.chargeHeld || 0;
  if(held < .18){
    shotPower = Math.max(shotPower,0.35);
  }
  releaseOffenseMove(char);
}

function releaseOffenseMove(char){
  if(gameOver) return;
  if(char.moveState === "backjump" || (char.moveMeta && char.moveMeta.fadeawayArmed)){
    shootBall(char,"fadeaway");
    return;
  }
  const backHeld = char.id === "p1" ? !!keys["KeyS"] : !!keys["KeyK"];
  if(backHeld && char.moveState === "normal"){
    startStepBack(char);
    return;
  }
  const shotType = chooseShotType(char);
  if(shotType === "dunk") startDunk(char);
  else if(shotType === "hook") shootBall(char,"hook");
  else if(shotType === "layup") startLayup(char);
  else shootBall(char,shotType);
}

function triggerShotSplash(position,type="green"){
  const labels = {green:"GREEN!",gold:"SPLASH!",blue:"HIGH ARC!",orange:"SLAM!"};
  const colors = {green:0x35ff6b,gold:0xffd84d,blue:0x50b7ff,orange:0xff8b2b};
  spawnParticle(position,colors[type] || 0x35ff6b);
  flash(labels[type] || "SPLASH!");
  pulseCourtTheme(type);
  spawnCourtThemedParticle(position,type);
}
function getCourtEffectTheme(){
  const theme = (courts[selectedCourt] || courts.classicArena).theme || "classic";
  if(theme === "neon" || theme === "cyber") return "neon";
  if(theme === "nightlife") return "neon";
  if(theme === "rain" || theme === "bay" || theme === "mountain") return "ice";
  if(theme === "beach" || theme === "sunset" || theme === "desert") return "sun";
  if(theme === "lava") return "lava";
  if(theme === "ice") return "ice";
  if(theme === "galaxy") return "galaxy";
  if(theme === "jungle") return "jungle";
  if(theme === "royal" || theme === "gold") return "gold";
  if(theme === "street" || theme === "city" || theme === "warehouse" || theme === "fieldhouse" || theme === "legacy") return "urban";
  return "classic";
}
function getThemeColorForEvent(eventType="make"){
  const court = courts[selectedCourt] || courts.classicArena;
  const fx = getCourtEffectTheme();
  if(fx === "neon") return eventType === "green" ? 0xff42f9 : 0x65f0ff;
  if(fx === "sun") return 0xffc46b;
  if(fx === "lava") return 0xff6e2b;
  if(fx === "ice") return 0x8fe9ff;
  if(fx === "galaxy") return 0x9d6cff;
  if(fx === "jungle") return 0x8eff5e;
  if(fx === "gold") return 0xffd56a;
  if(fx === "urban") return 0x9ec3ff;
  return court.trimColor || 0x65d9ff;
}
function spawnCourtThemedParticle(position,eventType){
  if(!qualitySettings[graphicsQuality].particles) return;
  const q = graphicsQuality === "high" ? 3 : graphicsQuality === "medium" ? 2 : 1;
  for(let i=0;i<q;i++){
    const jitter = new THREE.Vector3((Math.random()-.5)*0.35,Math.random()*0.22,(Math.random()-.5)*0.35);
    spawnParticle(position.clone().add(jitter),getThemeColorForEvent(eventType));
  }
}
function pulseCourtTheme(eventType){
  const base = eventType === "dunk" ? 1 : eventType === "green" ? 0.88 : 0.55;
  const qualityMul = graphicsQuality === "high" ? 1 : graphicsQuality === "medium" ? .78 : .45;
  courtThemePulse = Math.max(courtThemePulse,base*qualityMul);
}
function triggerCrowdReaction(type,intensity=1){
  const baseIntensity = {
    make:0.45, hook_make:0.52, block:0.75, dunk:1.0, three:0.9, steal:0.5, green:0.72, rim_hang:1.0, miss:0.2, game_point:0.82, contact:0.36
  }[type] || 0.4;
  const finalIntensity = THREE.MathUtils.clamp(baseIntensity * intensity,0.15,1.2);
  const typeBoost = {
    make:0.16,hook_make:0.2,green:0.26,dunk:0.38,three:0.33,rim_hang:0.42,block:0.32,steal:0.2,miss:0.06,game_point:0.3,contact:0.1
  }[type] || 0.12;
  crowdReactionState = type === "game_point" ? "gamePoint" : type === "dunk" || type === "green" ? "hype" : type === "miss" ? "stunned" : "cheering";
  crowdReactionTimer = Math.max(crowdReactionTimer,type === "game_point" ? 3.4 : type === "dunk" ? 2.1 : 1.4);
  crowdReactionStrength = Math.max(crowdReactionStrength, type === "dunk" ? 1 : type === "block" || type === "game_point" ? 0.9 : type === "steal" ? 0.7 : 0.55);
  arenaExcitement = THREE.MathUtils.clamp(arenaExcitement + typeBoost + finalIntensity*0.1,0,1);
  crowdWave = Math.max(crowdWave,finalIntensity + arenaExcitement*0.35);
  cameraEffects.shake = Math.max(cameraEffects.shake,finalIntensity * 0.2 + arenaExcitement*0.08);
  if(type === "dunk" || type === "block") applyCameraZoomPulse(0.08 + finalIntensity*0.04);
  pulseArenaLights(Math.min(1.45,finalIntensity + arenaExcitement*0.55));
  if(Math.abs(scoreP1-scoreP2) <= 1 && (scoreP1 >= targetScore-2 || scoreP2 >= targetScore-2)) crowdWave = Math.max(crowdWave,finalIntensity + 0.35);
  pulseCourtTheme(type);
  if(type === "dunk" || type === "block" || type === "green" || type === "steal" || type === "game_point" || type === "miss") spawnCourtThemedParticle(HOOP_POS.clone(),type);
  if(ledBoardCtx && Math.random() < 0.25 + arenaExcitement*0.35){
    const msg = type === "dunk" ? "SLAM!" : type === "block" ? "BIG BLOCK" : type === "three" ? "BANK SHOT" : type === "green" ? "GREEN RELEASE" : type === "steal" ? "ANKLE BREAKER" : type === "game_point" ? "GAME POINT" : type === "miss" ? "DEFENSE" : "HOME COURT";
    updateLEDBoards(msg);
  }
}
