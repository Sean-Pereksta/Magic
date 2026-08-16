function onKeyDown(e){
  inputManager.onKeyDown(e);
  keys[e.code] = true;
  keys[e.key.toLowerCase()] = true;

  if(e.key.toLowerCase() === "h" && gameStarted){
    toggleUI();
    return;
  }
  if(e.code === "F3"){
    debugDirections = !debugDirections;
    if(debugDirHelpers){
      debugDirHelpers.group.visible = debugDirections;
    }
    flash(debugDirections ? "Direction debug ON" : "Direction debug OFF");
    return;
  }
}

function onKeyUp(e){
  inputManager.onKeyUp(e);
  keys[e.code] = false;
  keys[e.key.toLowerCase()] = false;
}

function processUnifiedInputActions(char,state){
  if(!char || gameOver || gamePaused) return;
  const isOffense = possession === char.id && ballState === "dribble";

  if(state.pressed.shoot && isOffense) handleActionButton(char);
  if(state.released.shoot && shotCharging && chargingPlayer === char) releaseShootOrFake(char);

  if(state.pressed.block && !isOffense){
    if(state.protect) raiseHands(char);
    else if(char.onGround) tryJumpBlock(char,state.sprint);
  }
  if(state.block && state.protect && !isOffense) char.handsUpTime = Math.max(char.handsUpTime,.12);

  const useSkill = screenSide=>{
    if(!char.unifiedTap) char.unifiedTap = {left:-99,right:-99};
    const now = performance.now()*.001;
    const double = now-char.unifiedTap[screenSide] < DOUBLE_TAP_WINDOW;
    char.unifiedTap[screenSide] = now;
    if(isOffense && double) startSideJump(char,screenSide);
    else handleHandAction(char,screenSide,`unified-${screenSide}`);
  };
  if(state.pressed.leftSkill) useSkill("left");
  if(state.pressed.rightSkill) useSkill("right");

  if(state.pressed.steal && !isOffense){
    if(char.swipeTime > .12){
      punishBadReach(char);
    }else{
      const exposedSide = ballSideValue < 0 ? "left" : "right";
      handleHandAction(char,exposedSide,"unified-steal");
    }
  }

  if(state.protect && !isOffense){
    const lateral = Math.abs(state.moveX)>.12 ? (state.moveX<0?"left":"right") : null;
    char.defensiveShadeSide = lateral;
    char.defensiveShadeTime = lateral ? Math.min(1.35,(char.defensiveShadeTime||0)+.05) : Math.max(0,(char.defensiveShadeTime||0)-.08);
  }

  if(state.pressed.takeover && char.takeover>=100) activateTakeover(char);

  const flick = inputManager.consumeFlick(char.id);
  if(flick && isOffense){
    if(flick.direction === "back") startBackJump(char);
    else if(flick.direction === "forward") startHesitation(char);
    else if(flick.double) startSideJump(char,flick.direction);
    else attemptCrossover(char,flick.direction);
  }
}

function queueHandAction(char,screenSide,inputKey){
  if(!char) return;
  if(possession === char.id && ballState === "dribble"){
    handleHandAction(char,screenSide,inputKey);
    return;
  }
  char.pendingStealTap = performance.now() * .001;
  char.pendingStealSide = getBestHandForScreenSide(char,screenSide);
  char.pendingStealScreenSide = screenSide;
  char.pendingStealCode = inputKey;
}

function finalizeHandActionOnRelease(char,inputCode){
  if(!char || !char.pendingStealTap || char.pendingStealCode !== inputCode) return;
  const held = performance.now() * .001 - char.pendingStealTap;
  if(held < DEFENSIVE_SHADE_HOLD){
    handleHandAction(char,char.pendingStealScreenSide,char.pendingStealCode);
  }
  clearPendingSteal(char,inputCode);
}

function clearPendingSteal(char,inputCode=null){
  if(!char) return;
  if(inputCode && char.pendingStealCode !== inputCode) return;
  char.pendingStealTap = null;
  char.pendingStealSide = null;
  char.pendingStealScreenSide = null;
  char.pendingStealCode = null;
}

function handleActionButton(char){
  if(gameOver || (char.moveState !== "normal" && char.moveState !== "backjump")) return;
  if(possession === char.id && ballState === "dribble" && !hasShotThisPossession){
    if(char.moveState === "backjump"){
      shotPower = Math.max(shotPower,0.56);
      shootBall(char,"fadeaway");
      return;
    }
    if(!shotCharging){
      shotCharging = true;
      chargingPlayer = char;
      shotPower = .08;
      chargeDir = 1;
      char.moveMeta.chargeHeld = 0;
    }
  }else{
    if(char.onGround) tryJumpBlock(char, isSprinting(char));
    else raiseHands(char);
  }
}

function handleHandAction(char,screenSide,inputKey){
  if(gameOver) return;
  const handSide = getBestHandForScreenSide(char,screenSide);
  if(screenSide === "left") char.debugLeftInputTimer = .2;
  else char.debugRightInputTimer = .2;

  if(possession === char.id && ballState === "dribble"){
    attemptCrossover(char,screenSide);
  }else{
    char.swipeSide = handSide;
    char.swipeScreenSide = screenSide;
    char.swipeTime = .34;
    char.swipeReachScale = char.defensiveShadeSide ? 0.78 : 1;
    char.defensiveContainLabelTime = 0.3;
    char.defensiveShadeTime = 0;
    char.defensiveShadeSide = null;
  }
}

function processTapInputs(e){
  const key = e.key.toLowerCase();
  const code = e.code;
  const now = performance.now() * .001;
  if(key === "q"){
    if(now - tapMemory.p1.q < DOUBLE_TAP_WINDOW && playerHasBall(p1)) startSideJump(p1,"left");
    tapMemory.p1.q = now;
  }
  if(key === "e"){
    if(now - tapMemory.p1.e < DOUBLE_TAP_WINDOW && playerHasBall(p1)) startSideJump(p1,"right");
    tapMemory.p1.e = now;
  }
  if(key === "s"){
    if(now - tapMemory.p1.s < DOUBLE_TAP_WINDOW && playerHasBall(p1)) startBackJump(p1);
    tapMemory.p1.s = now;
  }
  if(twoPlayerMode){
    if(key === "u"){
      if(now - tapMemory.p2.u < DOUBLE_TAP_WINDOW && playerHasBall(p2)) startSideJump(p2,"left");
      tapMemory.p2.u = now;
    }
    if(key === "o"){
      if(now - tapMemory.p2.o < DOUBLE_TAP_WINDOW && playerHasBall(p2)) startSideJump(p2,"right");
      tapMemory.p2.o = now;
    }
    if(key === "k"){
      if(now - tapMemory.p2.k < DOUBLE_TAP_WINDOW && playerHasBall(p2)) startBackJump(p2);
      tapMemory.p2.k = now;
    }
    if(code === "Numpad4"){
      if(now - tapMemory.p2.numpad4 < DOUBLE_TAP_WINDOW && playerHasBall(p2)) startSideJump(p2,"left");
      tapMemory.p2.numpad4 = now;
    }
    if(code === "Numpad6"){
      if(now - tapMemory.p2.numpad6 < DOUBLE_TAP_WINDOW && playerHasBall(p2)) startSideJump(p2,"right");
      tapMemory.p2.numpad6 = now;
    }
  }
}

function attemptCrossover(char,screenSide){
  const handSide = getBestHandForScreenSide(char,screenSide);
  const desiredSide = handSide === "left" ? -1 : 1;
  if(crossover.active && crossover.player === char){
    const unsafe = crossover.progress < .72 || crossover.riskWindow > 0;
    if(unsafe){
      const chainPenalty = THREE.MathUtils.clamp((.74 - crossover.progress) * .35 + crossover.riskWindow*.4,0,.35);
      const speedPenalty = char.velocity.length() > 5 ? .14 : 0;
      const risk = .36 - (char.profile.handle - 1) * .22 + speedPenalty + chainPenalty;
      if(Math.random() < Math.max(0,chainPenalty - (char.profile.handle-1)*.12)){
        char.anim.crossStumble = .32;
      }
      if(Math.random() < risk){
        loseBallOnCrossover(char);
        return;
      }
    }

    crossover.from = ballSideValue;
    crossover.to = desiredSide;
    crossover.progress = 0;
    crossover.riskWindow = .28;
    flash(`${char.id.toUpperCase()} risky crossover!`);
    return;
  }

  if(Math.sign(ballSideValue) === desiredSide && !crossover.active){
    flash("Ball already there.");
    return;
  }

  crossover.active = true;
  crossover.player = char;
  crossover.from = ballSideValue;
  crossover.to = desiredSide;
  crossover.progress = 0;
  crossover.riskWindow = .28;
  crossover.screenSide = screenSide;
  flash(`${char.id.toUpperCase()} crossover!`);
}

function loseBallOnCrossover(char){
  crossover.active = false;
  crossover.progress = 1;
  crossover.riskWindow = 0;

  ballState = "loose";
  char.hasBall = false;
  clearShotCreationStates();

  const forward = getForward(char.angle);
  const right = new THREE.Vector3(forward.z,0,-forward.x);
  const looseDir = forward.clone()
    .multiplyScalar(1.6)
    .add(right.clone().multiplyScalar((Math.random()-.5)*2.0))
    .normalize();

  ballVelocity.copy(looseDir.multiplyScalar(3.5));
  ballVelocity.y = 1.05;

  flash("Lost the handle! Loose ball!");
}

function animate(){
  requestAnimationFrame(animate);
  const dt = Math.min(clock.getDelta(),.033);
  inputManager.tick();

  if(gameStarted){
    const pausePressed = inputManager.getState("p1").pressed.pause || (twoPlayerMode && inputManager.getState("p2").pressed.pause);
    if(pausePressed) togglePause();
    messageTimer -= dt;
    if(!gamePaused) updateCountdown(dt);
    else possessionFrozen = true;
    updateTouchPresentation();

    if(!gameOver && !gamePaused){
      if(possessionFrozen){
        updateCountdownMovement(dt);
      }else{
        updateHumanControls(dt);
        if(!twoPlayerMode) updateBot(dt);
        updateJumpPhysics(p1,dt);
        updateJumpPhysics(p2,dt);
        resolveBodyCollisions();
        resolveHoopPlayerCollision(p1);
        resolveHoopPlayerCollision(p2);
        updateCharacters(dt);
        updateBall(dt);
        updateLayup(dt);
        updateDunk(dt);
        updateHoopReaction(dt);
        loosePickupDelay = Math.max(0,loosePickupDelay - dt);
        ballCollisionCooldown = Math.max(0,ballCollisionCooldown - dt);
        backboardCollisionCooldown = Math.max(0,backboardCollisionCooldown - dt);
        checkSteals();
        checkBlocksAndCatches();
        checkFouls(dt);
        updateTakeover(dt);
        updateCrowd(dt);
        updateShotTrail(dt);
        updateParticles(dt);
        updateConfetti(dt);
        updateCameraEffects(dt);
        updateReboundAI(dt);
        ledBoardTimer -= dt;
        if(ledBoardTimer <= 0){
          ledBoardTimer = 2.8;
          const msgs = ["GAME POINT","ANKLE BREAKER","BLOCKED","HE'S HEATING UP",`FIRST TO ${targetScore}`];
          updateLEDBoards(msgs[Math.floor(Math.random()*msgs.length)]);
        }
        pulseArenaLights(.35 + crowdWave*.5);
      }
    }
    tutorialCoach.update(dt);
    adaptiveQuality.sample(dt);
  }

  updateDebugDirections(dt);
  updateShotMeterVisuals(dt);
  if(!gameStarted && document.getElementById("menu").style.display !== "none"){
    refreshSneakerSelectorUI();
    refreshJerseySelectorUI();
    refreshCourtSelectorUI();
    if(document.getElementById("tournamentSetupScreen").classList.contains("active")) refreshTournamentSelectorUI();
  }
  updateCameras(dt);
  updateHUD();
  renderScene();
}

function updateCountdown(dt){
  if(gamePaused){ possessionFrozen=true; return; }
  if(countdownTimer <= 0){
    possessionFrozen = false;
    document.getElementById("countdown").style.display = "none";
    return;
  }

  possessionFrozen = true;
  countdownTimer -= dt;

  const cd = document.getElementById("countdown");
  cd.style.display = "flex";

  if(countdownTimer > 2) cd.textContent = "3";
  else if(countdownTimer > 1) cd.textContent = "2";
  else if(countdownTimer > 0) cd.textContent = "1";
  else{
    cd.textContent = "GO!";
    setTimeout(() => cd.style.display = "none",250);
    possessionFrozen = false;
  }
}

function updateHumanControls(dt){
  const p1State = inputManager.getState("p1");
  processUnifiedInputActions(p1,p1State);
  updatePlayerControl(p1, {
    state:p1State,
    left:"KeyA", right:"KeyD", forward:"KeyW", back:"KeyS", sprint:"ShiftLeft", handLeft:"KeyQ", handRight:"KeyE"
  }, dt);

  if(twoPlayerMode){
    const p2State = inputManager.getState("p2");
    processUnifiedInputActions(p2,p2State);
    updatePlayerControl(p2, {
      state:p2State,
      left:"KeyJ", right:"KeyL", forward:"KeyI", back:"KeyK", sprint:"ShiftRight", handLeft:["KeyU","Numpad4"], handRight:["KeyO","Numpad6"]
    }, dt);
  }

  if(shotCharging && chargingPlayer){
    chargingPlayer.moveMeta.chargeHeld = (chargingPlayer.moveMeta.chargeHeld || 0) + dt;
    const chargeShotType = chooseShotType(chargingPlayer);
    const chargeContest = calculateShotContest(chargingPlayer);
    const profile = getShotMeterProfile(chargeShotType,chargingPlayer,chargeContest);
    shotPower = THREE.MathUtils.clamp(.12 + chargingPlayer.moveMeta.chargeHeld * .82 * profile.meterFillSpeed, .12, 1);
  }
}

function updateCountdownMovement(dt){
  updateHumanControls(dt);
  if(!twoPlayerMode) updateBotCountdown(dt);
  resolveBodyCollisions();
  updateCharacters(dt);
  const handler = possession === "p1" ? p1 : p2;
  const defender = possession === "p1" ? p2 : p1;
  if(handler && defender && ballState === "dribble"){
    enforceCountdownDefenderGap(defender,handler,COUNTDOWN_DEFENDER_MIN_DISTANCE);
  }
}

function enforceCountdownDefenderGap(defender,ballHandler,minDistance){
  const push = defender.group.position.clone().sub(ballHandler.group.position);
  push.y = 0;
  if(push.lengthSq() < 0.0001){
    push.copy(defender.group.position).sub(HOOP_POS);
    push.y = 0;
  }
  if(push.lengthSq() < 0.0001) push.set(1,0,0);
  const dist = push.length();
  if(dist >= minDistance) return;
  push.normalize().multiplyScalar(minDistance);
  defender.group.position.copy(ballHandler.group.position.clone().add(push));
  keepInCourt(defender.group.position);
}

function updateShotMeterVisuals(dt){
  if(!shotMeter) return;
  if(!gameSettings.get("shotMeter") || !shotCharging || !chargingPlayer){
    shotMeter.group.visible = false;
    return;
  }
  const held = chargingPlayer.moveMeta.chargeHeld || 0;
  const shotType = chooseShotType(chargingPlayer);
  const contest = calculateShotContest(chargingPlayer);
  const timing = calculateShotTiming(held,shotType,chargingPlayer,contest);
  const profile = timing.profile;
  shotMeter.group.visible = true;
  shotMeter.group.position.copy(chargingPlayer.group.position.clone().add(new THREE.Vector3(0, 2.65 * chargingPlayer.profile.height, 0)));
  const cam = getActiveCameraForChar(chargingPlayer);
  shotMeter.group.lookAt(cam.position);
  shotMeter.fill.position.x = -0.6 + timing.pct * 1.2;
  shotMeter.greenRegion.position.x = -0.6 + ((profile.greenStart + profile.greenEnd)*.5) * 1.2;
  shotMeter.greenRegion.scale.x = Math.max(.15,(profile.greenEnd-profile.greenStart)*6.2);
  shotMeter.fill.material.color.setHex(timing.color);
  shotMeter.glow.position.x = shotMeter.fill.position.x;
  const inGreen = timing.grade === "Green Release";
  const alphaBase = 0.2 + Math.max(0,0.8-contest.value)*0.22;
  shotMeter.greenRegion.material.opacity = inGreen ? 0.5 + Math.sin(performance.now()*.02)*.2 : alphaBase;
  shotMeter.glow.material.opacity = inGreen ? .55 + Math.sin(performance.now()*.02)*.25 : 0;
  if(tutorialMode && canStartHookShot(chargingPlayer)){
    const el = document.getElementById("shotFeedback");
    if(el && el.style.display !== "block"){
      el.style.display = "block";
      el.textContent = "HOOK SHOT";
      setTimeout(()=>{ if(el.textContent === "HOOK SHOT") el.style.display = "none"; },220);
    }
  }
}

function isKeyDown(codeOrCodes){
  if(Array.isArray(codeOrCodes)) return codeOrCodes.some(code => !!keys[code]);
  return !!keys[codeOrCodes];
}

function updateModernPlayerControl(char,input,dt){
  const profile = char.profile;
  const tuning = getGameplayTuning();
  if(char.stumbledTimer>0) char.stumbledTimer=Math.max(0,char.stumbledTimer-dt);
  char.pumpFakeCooldown=Math.max(0,char.pumpFakeCooldown-dt);
  char.sideHopCooldown=Math.max(0,char.sideHopCooldown-dt);
  if(char.moveMeta?.fadeawayArmed){
    char.moveMeta.fadeawayWindow=Math.max(0,(char.moveMeta.fadeawayWindow||0)-dt);
    if(char.moveMeta.fadeawayWindow<=0) char.moveMeta.fadeawayArmed=false;
  }
  if(char.moveState!=="normal"){
    updateMoveState(char,dt);
    return;
  }

  const countdownDribble=possessionFrozen&&countdownTimer>0&&ballState==="dribble";
  const isBallHandler=possession===char.id;
  if(countdownDribble&&isBallHandler){
    char.velocity.multiplyScalar(Math.max(0,1-dt*18));
    setAnimationState(char,"idle");
    return;
  }

  const amount=THREE.MathUtils.clamp(Math.hypot(input.moveX,input.moveY),0,1);
  const camera=char.id==="p1"?(twoPlayerMode?camP1:camMain):camP2;
  const cameraForward=new THREE.Vector3();
  if(camera) camera.getWorldDirection(cameraForward); else cameraForward.copy(getForward(char.angle));
  cameraForward.y=0;
  if(cameraForward.lengthSq()<.001) cameraForward.copy(getForward(char.angle));
  cameraForward.normalize();
  const cameraRight=new THREE.Vector3(-cameraForward.z,0,cameraForward.x).normalize();
  const desiredDirection=cameraForward.multiplyScalar(input.moveY).add(cameraRight.multiplyScalar(input.moveX));
  if(desiredDirection.lengthSq()>.001) desiredDirection.normalize();

  const balancedSpeed=THREE.MathUtils.lerp(1,profile.speed,.76);
  const balancedSprint=THREE.MathUtils.lerp(1,profile.sprint,.76);
  const offense=isBallHandler&&ballState==="dribble";
  const defenderTarget=char.id==="p1"?p2:p1;
  const wantsSprint=input.sprint&&amount>.18&&char.sprint>SPRINT_MIN;
  const baseSpeed=4.3*balancedSpeed*tuning.lateralSpeed;
  const sprintSpeed=7.05*balancedSpeed*balancedSprint*tuning.sprintSpeed;
  const defensiveSpeed=4.05*balancedSpeed*tuning.lateralSpeed;
  let targetSpeed=(offense?baseSpeed:defensiveSpeed)*amount;
  if(wantsSprint) targetSpeed=sprintSpeed*amount;
  if(input.protect) targetSpeed*=offense ? .72 : .82;
  if(char.stamina<.2) targetSpeed*=.82;
  if(char.stumbledTimer>0) targetSpeed*=.42;

  const facing=getForward(char.angle);
  const currentDirection=char.velocity.lengthSq()>.04?char.velocity.clone().normalize():facing;
  const reversal=amount>.1?currentDirection.dot(desiredDirection):-1;
  char.plantTimer=Math.max(0,(char.plantTimer||0)-dt);
  if(amount>.35&&char.velocity.length()>3.2&&reversal<-.34&&char.plantTimer<=0){
    char.plantTimer=.16+(profile.width>1.08?.07:0);
    char.anim.cutPlant=Math.max(char.anim.cutPlant||0,.82);
    char.anim.cutDir=input.moveX<0?-1:1;
  }
  if(char.plantTimer>0) targetSpeed*=tuning.reversePenalty;

  const targetVelocity=amount>.04?desiredDirection.clone().multiplyScalar(targetSpeed):new THREE.Vector3();
  const sizeAgility=THREE.MathUtils.clamp(1.12-(profile.width-1)*.55+(profile.handle-1)*.2,.7,1.35);
  const response=amount>.04?(5.4*sizeAgility*tuning.acceleration):(7.2*char.movement.stopSpeed*tuning.deceleration);
  const blend=1-Math.exp(-response*dt);
  char.velocity.lerp(targetVelocity,blend);
  if(char.plantTimer>0) char.velocity.multiplyScalar(1-dt*(profile.width>1.08?3.8:2.7));

  if(wantsSprint){
    char.sprint=Math.max(0,char.sprint-SPRINT_DRAIN*tuning.staminaDrain*dt);
    char.stamina=Math.max(0,char.stamina-(.29+(profile.width-1)*.09)*tuning.staminaDrain*dt);
  }else{
    char.sprint=Math.min(1,char.sprint+SPRINT_RECOVER*char.movement.recoverySpeed*dt);
    char.stamina=Math.min(1,char.stamina+(amount>.15?.11:.24)*dt*(profile.width>1.1?.78:1.05));
  }

  if(offense&&amount>.08){
    const targetAngle=Math.atan2(desiredDirection.x,desiredDirection.z);
    char.angle=lerpAngle(char.angle,targetAngle,dt*(wantsSprint?9.2:11.5)*sizeAgility);
  }else if(!offense&&defenderTarget){
    const toHandler=defenderTarget.group.position.clone().sub(char.group.position).setY(0);
    if(toHandler.lengthSq()>.001){
      const targetAngle=Math.atan2(toHandler.x,toHandler.z);
      char.angle=lerpAngle(char.angle,targetAngle,dt*(input.protect?12:8.5));
    }
  }else if(amount>.08){
    char.angle=lerpAngle(char.angle,Math.atan2(desiredDirection.x,desiredDirection.z),dt*9);
  }

  char.inputX=input.moveX;
  char.inputZ=input.moveY;
  char.sprintMul=wantsSprint?1.35:1;
  const relativeForward=char.velocity.lengthSq()>.02?char.velocity.clone().normalize().dot(getForward(char.angle)):0;
  const animationState=!amount?"idle":(!offense&&Math.abs(input.moveX)>.25?"defense":relativeForward<-.28?"backpedal":wantsSprint?"sprint":"run");
  setAnimationState(char,animationState);

  char.group.position.addScaledVector(char.velocity,dt);
  if(countdownDribble&&!isBallHandler){
    const handler=possession==="p1"?p1:p2;
    if(handler) enforceCountdownDefenderGap(char,handler,COUNTDOWN_DEFENDER_MIN_DISTANCE);
  }
  keepInCourt(char.group.position);
  char.group.rotation.y=char.angle;
}

function updatePlayerControl(char,ctrl,dt){
  const input=ctrl.state||inputManager.getState(char.id);
  if(gameSettings.get("controlStyle")==="modern"||input.source!=="keyboard"){
    updateModernPlayerControl(char,input,dt);
    return;
  }
  const p = char.profile;
  if(char.stumbledTimer > 0){
    char.stumbledTimer -= dt;
  }
  char.pumpFakeCooldown = Math.max(0,char.pumpFakeCooldown - dt);
  char.sideHopCooldown = Math.max(0,char.sideHopCooldown - dt);
  if(char.moveMeta && char.moveMeta.fadeawayArmed){
    char.moveMeta.fadeawayWindow = Math.max(0,(char.moveMeta.fadeawayWindow || 0) - dt);
    if(char.moveMeta.fadeawayWindow <= 0){
      char.moveMeta.fadeawayArmed = false;
    }
  }

  const move = char.movement;
  const balancedSpeed = THREE.MathUtils.lerp(1.0,p.speed,0.75);
  const balancedSprint = THREE.MathUtils.lerp(1.0,p.sprint,0.75);
  const turnSpeed = move.turnSpeed * (balancedSpeed > 1 ? 1.08 : .98);
  const baseSpeed = 4.15 * balancedSpeed + move.acceleration*.32;
  const sprintSpeed = 6.95 * balancedSpeed * balancedSprint;
  const backSpeed = 2.1 * balancedSpeed + move.stopSpeed*.2;
  updateDefensiveShade(char,ctrl,dt);
  if(char.moveState !== "normal"){
    updateMoveState(char,dt);
    return;
  }

  const countdownDribble = possessionFrozen && countdownTimer > 0 && ballState === "dribble";
  const isBallHandler = possession === char.id;
  if(countdownDribble && isBallHandler){
    char.velocity.set(0,0,0);
    setAnimationState(char,"idle");
    char.group.rotation.y = char.angle;
    return;
  }

  if(isKeyDown(ctrl.left)) char.angle += turnSpeed * dt;
  if(isKeyDown(ctrl.right)) char.angle -= turnSpeed * dt;

  char.velocity.set(0,0,0);
  const forward = getForward(char.angle);

  const wantsSprint = isKeyDown(ctrl.sprint) && isKeyDown(ctrl.forward);
  if(wantsSprint && char.sprint > SPRINT_MIN){
    char.velocity.add(forward.clone().multiplyScalar(sprintSpeed));
    char.sprint = Math.max(0,char.sprint - SPRINT_DRAIN * dt);
    char.stamina = Math.max(0,char.stamina - (.32 + (1-char.profile.width)*.08)*dt);
  }else{
    if(isKeyDown(ctrl.forward)) char.velocity.add(forward.clone().multiplyScalar(baseSpeed));
    char.sprint = Math.min(1,char.sprint + SPRINT_RECOVER * dt);
    char.stamina = Math.min(1,char.stamina + (isKeyDown(ctrl.forward) ? .12 : .24)*dt*(char.profile.width>1.1?.8:1.05));
  }

  if(isKeyDown(ctrl.back)) char.velocity.add(forward.clone().multiplyScalar(-backSpeed));
  if(char.defensiveShadeSide && possession !== char.id && ballState === "dribble"){
    const screenVec = getScreenSideVectorForChar(char,char.defensiveShadeSide);
    const shadeSlide = screenVec.multiplyScalar((0.36 + char.movement.defensiveWidth*0.1) * dt);
    char.group.position.add(shadeSlide);
    const handler = possession === "p1" ? p1 : p2;
    if(handler){
      const toBall = handler.group.position.clone().sub(char.group.position);
      toBall.y = 0;
      if(toBall.lengthSq() > 0.001){
        const targetAngle = Math.atan2(toBall.x,toBall.z);
        char.angle = lerpAngle(char.angle,targetAngle,dt*6.4);
      }
    }
  }
  setAnimationState(char, isKeyDown(ctrl.back) ? "backpedal" : (isKeyDown(ctrl.forward) ? "run" : "idle"));
  if(char.stumbledTimer > 0) char.velocity.multiplyScalar(.45);
  if(char.stamina < .2) char.velocity.multiplyScalar(.82);
  if(char.hasBall && ballState === "dribble"){
    const defender = char.id === "p1" ? p2 : p1;
    if(defender){
      const toRim = HOOP_POS.clone().sub(char.group.position).setY(0).normalize();
      const defToBall = char.group.position.clone().sub(defender.group.position).setY(0);
      const between = isDefenderBetweenBallAndRim(defender,char.group.position) && defToBall.length() < 1.45;
      if(between){
        defender.bodyUpTimer = Math.min(0.5,(defender.bodyUpTimer||0) + dt*2.2);
        const lateral = getBodyRightVector(char).setY(0).normalize().dot(defToBall.clone().normalize());
        const ride = THREE.MathUtils.clamp(0.09 + (defender.profile.width-char.profile.width)*0.05,0.04,0.16);
        char.velocity.multiplyScalar(1 - ride);
        char.velocity.add(getBodyRightVector(char).multiplyScalar(-lateral * 0.85 * Math.max(0,1.05-char.profile.handle) * dt));
        char.bodyUpTimer = Math.min(0.45,(char.bodyUpTimer||0) + dt*2);
      }else{ defender.bodyUpTimer = Math.max(0,(defender.bodyUpTimer||0)-dt*2.5); char.bodyUpTimer = Math.max(0,(char.bodyUpTimer||0)-dt*3.2); }
    }
  }
  if(char.hasBall && canStartDunk(char)){
    const now = performance.now()*.001;
    if(now - lastDunkReadyFlash[char.id] > 0.8){
      showMoveFeedback("DUNK READY",0.45);
      lastDunkReadyFlash[char.id] = now;
    }
  }

  char.group.position.add(char.velocity.clone().multiplyScalar(dt));
  if(countdownDribble && !isBallHandler){
    const handler = possession === "p1" ? p1 : p2;
    if(handler) enforceCountdownDefenderGap(char,handler,COUNTDOWN_DEFENDER_MIN_DISTANCE);
  }
  keepInCourt(char.group.position);
  char.group.rotation.y = char.angle;
}

function updateDefensiveShade(char,ctrl,dt){
  if(!char) return;
  char.defensiveContainLabelTime = Math.max(0,char.defensiveContainLabelTime - dt);
  if(possession === char.id || ballState !== "dribble"){
    clearPendingSteal(char);
    char.defensiveShadeSide = null;
    char.defensiveShadeTime = 0;
    char.defensiveContainBonus = 0;
    return;
  }
  if(char.pendingStealTap && char.pendingStealScreenSide){
    const held = performance.now() * .001 - char.pendingStealTap;
    if(held >= DEFENSIVE_SHADE_HOLD && keys[char.pendingStealCode]){
      char.defensiveShadeSide = char.pendingStealScreenSide;
      char.defensiveShadeTime = Math.min(1.2,char.defensiveShadeTime + dt*2.4);
      char.defensiveContainBonus = 0.2 + char.defensiveShadeTime*0.33;
      char.defensiveContainLabelTime = 0.24;
      clearPendingSteal(char,char.pendingStealCode);
    }
  }
  const holdingLeft = char.id === "p1" ? !!keys["KeyQ"] : (!!keys["KeyU"] || !!keys["Numpad4"]);
  const holdingRight = char.id === "p1" ? !!keys["KeyE"] : (!!keys["KeyO"] || !!keys["Numpad6"]);
  const desired = holdingLeft && !holdingRight ? "left" : holdingRight && !holdingLeft ? "right" : null;
  if(!desired){
    char.defensiveShadeTime = Math.max(0,char.defensiveShadeTime - dt*3.4);
    if(char.defensiveShadeTime <= 0.02) char.defensiveShadeSide = null;
  }else if(char.defensiveShadeSide === desired){
    char.defensiveShadeTime = Math.min(1.35,char.defensiveShadeTime + dt*2.2);
    char.defensiveContainBonus = 0.18 + char.defensiveShadeTime*0.35;
  }
}

function updateBotCountdown(dt){
  if(!p1 || !p2 || ballState !== "dribble" || possession !== "p1") return;
  const containTarget = getDefensiveTarget(p2,p1,"contain_drive");
  moveBotToward(p2,containTarget,difficultySettings[selectedDifficulty].speed * 0.82,dt);
}
