function updateJumpPhysics(char,dt){
  char.jumpCooldown -= dt;
  if(!char.onGround){
    char.yVel += GRAVITY * .76 * dt;
    char.group.position.y += char.yVel * dt;
    if(char.blockBoost){
      char.group.position.add(getForward(char.angle).multiplyScalar(3.6*dt));
    }
    if(char.group.position.y <= 0){
      char.group.position.y = 0;
      char.yVel = 0;
      char.onGround = true;
      char.blockBoost = false;
      char.handsUpTime = 0;
    }
  }
}

function tryJumpBlock(char,sprintBlock){
  if(char.jumpCooldown > 0 || !char.onGround) return;
  let jumpPower = 4.9 * char.profile.jump;

  if(sprintBlock && char.sprint > .22){
    char.sprint = Math.max(0,char.sprint - .24);
    jumpPower *= 1.12;
    char.blockBoost = true;
  }

  char.yVel = jumpPower;
  char.onGround = false;
  char.jumpCooldown = .88;
  char.blockTime = .42;
  char.handsUpTime = .38;
}

function raiseHands(char){
  char.blockTime = Math.max(char.blockTime,.42);
  char.handsUpTime = Math.max(char.handsUpTime,.42);
}

function resolveBodyCollisions(){
  const delta = p1.group.position.clone().sub(p2.group.position); delta.y = 0;
  const dist = Math.max(0.0001,delta.length());
  const minDist = p1.bodyRadius + p2.bodyRadius;
  p1.contactTimer = Math.max(0,(p1.contactTimer || 0) - 0.016);
  p2.contactTimer = Math.max(0,(p2.contactTimer || 0) - 0.016);
  p1.contactCooldown = Math.max(0,(p1.contactCooldown || 0) - 0.016);
  p2.contactCooldown = Math.max(0,(p2.contactCooldown || 0) - 0.016);
  if(dist >= minDist) return;
  const n = delta.normalize();
  const pen = minDist - dist;
  const relVel = p1.velocity.clone().sub(p2.velocity);
  const closing = Math.max(0,relVel.dot(n));
  const m1 = 1 / Math.max(0.65,p1.profile.width * 0.95 + p1.profile.block * 0.25);
  const m2 = 1 / Math.max(0.65,p2.profile.width * 0.95 + p2.profile.block * 0.25);
  const split1 = m1/(m1+m2), split2 = m2/(m1+m2);
  const corr = THREE.MathUtils.clamp(pen * 0.62,0.004,0.09);
  p1.group.position.add(n.clone().multiplyScalar(corr * split1));
  p2.group.position.add(n.clone().multiplyScalar(-corr * split2));
  const impulse = THREE.MathUtils.clamp(0.08 + closing*0.05 + pen*0.22,0.02,0.22);
  p1.velocity.add(n.clone().multiplyScalar( impulse * split1));
  p2.velocity.add(n.clone().multiplyScalar(-impulse * split2));
  keepInCourt(p1.group.position); keepInCourt(p2.group.position);
  const offense = possession === "p1" ? p1 : p2;
  const defense = offense === p1 ? p2 : p1;
  let newContact=false;
  if(offense.contactCooldown<=0&&defense.contactCooldown<=0){
    applyBodyContact(offense,defense);
    offense.contactCooldown=.11;
    defense.contactCooldown=.11;
    newContact=true;
  }
  const hard = offense.contactType === "hard_bump" || offense.contactType === "stonewall";
  screenShake = Math.min(.22,screenShake + (hard ? .07 : .03)); applyCameraShake(hard ? .1 : .05);
  if(hard&&newContact){
    inputManager.vibrate(offense.id,.34,55);
    inputManager.vibrate(defense.id,.28,50);
  }
}
function resolveHoopPlayerCollision(char){
  if(!char || !char.group) return;
  if(dunkActive && dunkOwner === char.id) return;

  const pos = char.group.position;
  const rimRadius = RIM_RADIUS + 0.35;
  const rimBottom = HOOP_POS.y - 1.05;
  const rimTop = HOOP_POS.y + 0.35;
  const bodyRadius = (char.bodyRadius || 0.55) * 0.92;

  if(pos.y + char.profile.height > rimBottom && pos.y < rimTop){
    const dx = pos.x - HOOP_POS.x;
    const dz = pos.z - HOOP_POS.z;
    let dist = Math.hypot(dx,dz);
    if(dist < 0.0001) dist = 0.0001;
    const overlap = rimRadius + bodyRadius - dist;
    if(overlap > 0){
      const pushStrength = THREE.MathUtils.clamp(0.6 + overlap * 0.5,0.55,1.05);
      pos.x += (dx / dist) * overlap * pushStrength;
      pos.z += (dz / dist) * overlap * pushStrength;

      if(!char.onGround && char.yVel > 0 && pos.y + char.profile.height * 0.58 > HOOP_POS.y - 0.12){
        char.yVel = Math.min(char.yVel,0.15);
      }
      if(!char.onGround) pos.y -= Math.min(0.02 + overlap * 0.035,0.08);
      keepInCourt(pos);
    }
  }

  if(!backboard) return;
  const boardHalfW = 3.4 * 0.5;
  const boardHalfH = 2.05 * 0.5;
  const boardHalfT = 0.25 * 0.5;
  const boardX = backboard.position.x;
  const boardY = backboard.position.y;
  const boardZ = backboard.position.z;
  const dx = pos.x - boardX;
  const dy = pos.y + char.profile.height * 0.5 - boardY;
  const dz = pos.z - boardZ;
  const overlapX = boardHalfW + bodyRadius - Math.abs(dx);
  const overlapY = boardHalfH + char.profile.height * 0.46 - Math.abs(dy);
  const overlapZ = boardHalfT + bodyRadius - Math.abs(dz);

  if(overlapX > 0 && overlapY > 0 && overlapZ > 0){
    const pushDir = dz >= 0 ? 1 : -1;
    const zPush = overlapZ * THREE.MathUtils.clamp(0.72 + overlapZ * 0.25,0.72,1.08);
    pos.z += pushDir * zPush;
    if(char.yVel < 0 && Math.abs(dy) < boardHalfH * 0.9) char.yVel *= 0.75;
    keepInCourt(pos);
  }
}

function updateCharacters(dt){
  updateMoveFeedback(dt);
  animateCharacter(p1,dt);
  animateCharacter(p2,dt);
  updateHookIndicator(p1,dt);
  updateHookIndicator(p2,dt);
  updateFloatingIndicators(p1,dt);
  updateFloatingIndicators(p2,dt);
}


function getPlayerIndicatorText(char){
  const charging = shotCharging && chargingPlayer === char;
  if(char.moveMeta && char.moveMeta.lastShotType === "fadeaway" && char.shotMotionTime > 0.1) return "FADEAWAY";
  if(canStartDunk(char) && possession === char.id && ballState === "dribble") return "DUNK";
  if(canHookShot(char) && possession === char.id && ballState === "dribble") return "HOOK";
  if(canLayup(char) && possession === char.id && ballState === "dribble") return "LAYUP";
  if(charging){
    const isJumper = getRequestedShotType(char) === "jumper";
    if(!isInsideArc(char)) return "3PT";
    return isJumper ? "JUMPER" : "SHOT";
  }
  return "";
}

function updateFloatingIndicators(char,dt){
  if(!char || !char.shotIndicator || !char.staminaIndicator) return;
  const txt = getPlayerIndicatorText(char);
  const sprite = char.shotIndicator;
  if(txt){
    sprite.visible = true;
    if(sprite.userData.lastText !== txt){
      const color = txt === "DUNK" ? "#ff9a2f" : txt === "HOOK" ? "#8fe9ff" : txt === "3PT" ? "#65f0ff" : "#ffffff";
      setSpriteText(sprite,txt,color,86);
      sprite.userData.lastText = txt;
    }
    sprite.position.set(0,2.5*char.profile.height,0);
    sprite.scale.setScalar((txt === "DUNK" ? .55:.42) + Math.sin(performance.now()*.01 + (char.id === "p1" ? 0:1))*0.03);
    sprite.material.opacity = Math.min(1,sprite.material.opacity + dt*8);
    sprite.lookAt(getActiveCameraForChar(char).position);
  }else{
    sprite.material.opacity = Math.max(0,sprite.material.opacity - dt*8);
    if(sprite.material.opacity <= .02) sprite.visible = false;
  }
  if(char.defenseIndicator){
    const defTxt = char.defensiveShadeSide ? (char.defensiveShadeSide === "left" ? "SHADE LEFT" : "SHADE RIGHT") : (char.defensiveContainLabelTime > 0 ? "CONTAIN" : "");
    if(defTxt){
      char.defenseIndicator.visible = true;
      if(char.defenseIndicator.userData.lastText !== defTxt){
        setSpriteText(char.defenseIndicator,defTxt,"#a5e4ff",64);
        char.defenseIndicator.userData.lastText = defTxt;
      }
      char.defenseIndicator.position.set(0,2.86*char.profile.height,0);
      char.defenseIndicator.scale.set(.5,.26,.26);
      char.defenseIndicator.material.opacity = Math.min(0.95,(char.defenseIndicator.material.opacity || 0) + dt*6.5);
      char.defenseIndicator.lookAt(getActiveCameraForChar(char).position);
    }else{
      char.defenseIndicator.material.opacity = Math.max(0,(char.defenseIndicator.material.opacity || 0) - dt*6);
      if(char.defenseIndicator.material.opacity <= .03) char.defenseIndicator.visible = false;
    }
  }

  drawStaminaIndicator(char);
  const st = THREE.MathUtils.clamp(char.stamina,0,1);
  char.staminaIndicator.position.set(0,2.15*char.profile.height,0);
  char.staminaIndicator.lookAt(getActiveCameraForChar(char).position);
  const emph = (isSprinting(char) || st < .35) ? 1 : .78;
  char.staminaIndicator.material.opacity = THREE.MathUtils.lerp(char.staminaIndicator.material.opacity || .7, emph * (st > .92 ? .42:.9), dt*6);
}

function updateMoveState(char,dt){
  if(char.moveState === "normal") return;

  if(char.moveState === "gather"){
    char.moveTimer += dt;
    const pct = THREE.MathUtils.clamp(char.moveTimer / Math.max(.001,char.moveDuration),0,1);
    animateGatherBall(char,pct,char.moveMeta.gatherType || "pickup");
    if(pct >= 1){
      char.moveState = "normal";
      char.moveTimer = 0;
      char.moveDuration = 0;
      char.group.scale.y = 1;
    }
    return;
  }

  if(char.moveState === "bobble"){
    char.moveTimer += dt;
    const pct = THREE.MathUtils.clamp(char.moveTimer / Math.max(.001,char.moveDuration),0,1);
    animateBobble(char,pct);
    if(pct >= 1){
      char.moveState = "normal";
      char.moveTimer = 0;
      char.moveDuration = 0;
      char.group.scale.y = 1;
    }
    return;
  }

  char.moveTimer += dt;
  const pct = THREE.MathUtils.clamp(char.moveTimer / Math.max(.001,char.moveDuration),0,1);
  if(char.moveState === "stepback") updateStepBack(char,dt,pct);
  if(char.moveState === "pumpfake") updatePumpFake(char,dt,pct);
  if(char.moveState === "sidehop") updateSideHop(char,dt,pct);
  if(char.moveState === "sidejump") updateSideJump(char,dt,pct);
  if(char.moveState === "backjump") updateBackJump(char,dt,pct);
  if(char.moveState === "postup") updatePostUp(char,dt);
  if(pct >= 1){
    char.moveState = "normal";
    char.moveTimer = 0;
    char.moveDuration = 0;
    char.visualJumpY = 0;
    char.anim.sideLean = 0;
    char.anim.legKick = 0;
    char.anim.fadeLean = 0;
  }
}
function startStepBack(char){
  if(char.moveState !== "normal" || char.sprint < .15) return;
  char.moveState = "stepback"; char.moveTimer = 0; char.moveDuration = 0.42; char.stepBackQuality = THREE.MathUtils.randFloat(.4,1);
  char.sprint = Math.max(0,char.sprint - .18); showMoveFeedback("Step-back!");
}
function updateStepBack(char,dt,pct){
  const back = getForward(char.angle).multiplyScalar(-2.9 * (1-pct*.55));
  char.group.position.add(back.multiplyScalar(dt));
  keepInCourt(char.group.position);
  if(pct > .72 && !char.moveMeta.stepShotReleased){ char.moveMeta.stepShotReleased = true; shotPower = Math.max(.42,shotPower); shootBall(char,"stepback"); }
  if(pct >= 1){ char.moveMeta.stepShotReleased = false; }
}
function startPumpFake(char){
  if(char.moveState !== "normal") return;
  char.moveState = "pumpfake"; char.moveTimer = 0; char.moveDuration = .42; char.pumpFakeCooldown = .45; char.lastPumpFakeTime = performance.now()*.001;
  showMoveFeedback("Pump fake!");
  if(!twoPlayerMode && possession === "p1" && Math.random() < botBrain.fakeBiteChance){ tryJumpBlock(p2,false); showMoveFeedback("Defender bit!"); }
}
function updatePumpFake(char,dt,pct){
  char.velocity.multiplyScalar(.25);
}
function startSideHop(char,screenSide){
  if(char.moveState !== "normal" || char.sideHopCooldown > 0) return;
  const sideVec = getScreenSideVectorForChar(char,screenSide);
  const handSide = getBestHandForScreenSide(char,screenSide);
  char.moveState = "sidehop"; char.moveTimer = 0; char.moveDuration = .36; char.sideHopCooldown = .55;
  char.moveMeta.sideHop = screenSide;
  char.moveMeta.sideHopVec = sideVec.clone();
  char.moveMeta.sideHopDistance = 1.25 + char.profile.handle * 0.25;
  char.moveMeta.sideHopStart = char.group.position.clone();
  char.moveMeta.sideHopHand = handSide;
  char.sprint = Math.max(0,char.sprint-.12);
  ballSide = handSide;
  ballSideValue = handSide === "left" ? -1 : 1;
  showMoveFeedback("SIDE HOP!");
  const defender = char.id === "p1" ? p2 : p1;
  triggerAnkleBreaker(defender,char);
}
function moveWithCollisionDampen(char,nextPos){
  const defender = char.id === "p1" ? p2 : p1;
  if(!defender) return nextPos;
  const minDist = char.bodyRadius + defender.bodyRadius;
  const delta = nextPos.clone().sub(defender.group.position);
  delta.y = 0;
  const dist = delta.length();
  if(dist >= minDist || dist <= .0001) return nextPos;
  const normal = delta.normalize();
  const dampened = defender.group.position.clone().add(normal.multiplyScalar(minDist + .01));
  dampened.y = nextPos.y;
  startContactAnimation(char,"light_bump");
  showMoveFeedback("CONTACT!",0.25);
  return dampened;
}
function updateSideHop(char,dt,pct){
  const sideVec = (char.moveMeta.sideHopVec || getScreenSideVectorForChar(char,char.moveMeta.sideHop || "right")).clone();
  const dist = char.moveMeta.sideHopDistance || (1.25 + char.profile.handle * 0.25);
  const speed = dist * (2.8 * (1-Math.abs(pct-.5)));
  char.group.position.add(sideVec.multiplyScalar(speed * dt));
  keepInCourt(char.group.position);
}
function startSideJump(char,screenSide){
  if(!playerHasBall(char)) return;
  if(char.moveState !== "normal" || char.sideHopCooldown > 0 || char.sprint < .12) return;
  const sideVec = getScreenSideVectorForChar(char,screenSide);
  const handSide = screenSide === "left" ? "left" : "right";
  char.moveState = "sidejump";
  char.moveTimer = 0;
  char.moveDuration = 0.48;
  char.sideHopCooldown = 0.72;
  char.moveMeta.sideJump = screenSide;
  char.moveMeta.sideJumpVec = sideVec.clone();
  char.moveMeta.sideJumpDistance = 2.45 + char.profile.handle * 0.35 + char.profile.speed * 0.2;
  char.moveMeta.sideJumpStart = char.group.position.clone();
  char.moveMeta.sideJumpHand = handSide;
  char.moveMeta.sideJumpLift = 0.34 + char.profile.jump * 0.08;
  char.sprint = Math.max(0, char.sprint - 0.22);
  ballSide = handSide;
  ballSideValue = handSide === "left" ? -1 : 1;
  showMoveFeedback("BIG SIDE JUMP!");
}
function updateSideJump(char,dt,pct){
  const hang = Math.sin(pct * Math.PI);
  const travelEase = 1 - Math.pow(1 - pct, 2.2);
  const start = char.moveMeta.sideJumpStart || char.group.position.clone();
  const vec = (char.moveMeta.sideJumpVec || getBodyRightVector(char)).clone();
  const target = start.clone().add(vec.multiplyScalar(char.moveMeta.sideJumpDistance || 2.7));
  const next = start.clone().lerp(target,travelEase);
  const resolved = moveWithCollisionDampen(char,next);
  char.group.position.x = resolved.x;
  char.group.position.z = resolved.z;
  keepInCourt(char.group.position);
  char.visualJumpY = (char.moveMeta.sideJumpLift || .36) * hang;
  char.anim.sideLean = (char.moveMeta.sideJump === "left" ? 1 : -1) * hang;
  char.anim.legKick = hang;
}
function startBackJump(char){
  if(!playerHasBall(char)) return;
  if(char.moveState !== "normal" || char.sprint < .12) return;
  char.moveState = "backjump";
  char.moveTimer = 0;
  char.moveDuration = 0.52;
  char.moveMeta.backJumpStart = char.group.position.clone();
  char.moveMeta.backJumpVec = getForward(char.angle).multiplyScalar(-1);
  char.moveMeta.backJumpDistance = 2.75 + char.profile.handle * 0.22 + char.profile.jump * 0.18;
  char.moveMeta.backJumpLift = 0.42 + char.profile.jump * 0.12;
  char.moveMeta.fadeawayWindow = 0.75;
  char.moveMeta.fadeawayArmed = true;
  char.moveMeta.lastShotType = "fadeaway";
  char.sprint = Math.max(0, char.sprint - 0.24);
  showMoveFeedback("BACK JUMP!");
}
function updateBackJump(char,dt,pct){
  const hang = Math.sin(pct * Math.PI);
  const travelEase = 1 - Math.pow(1 - pct, 2.0);
  const start = char.moveMeta.backJumpStart || char.group.position.clone();
  const vec = (char.moveMeta.backJumpVec || getForward(char.angle).multiplyScalar(-1)).clone();
  const target = start.clone().add(vec.multiplyScalar(char.moveMeta.backJumpDistance || 3.0));
  const next = start.clone().lerp(target,travelEase);
  const resolved = moveWithCollisionDampen(char,next);
  char.group.position.x = resolved.x;
  char.group.position.z = resolved.z;
  keepInCourt(char.group.position);
  char.visualJumpY = (char.moveMeta.backJumpLift || .45) * hang;
  char.anim.fadeLean = hang;
  char.anim.legKick = hang;
}
function checkDoubleTap(char,side,inputKey){
  const now = performance.now() * .001;
  const key = inputKey || (side === "left" ? "KeyQ" : "KeyE");
  const prev = char.tapTimes && char.tapTimes[key] != null ? char.tapTimes[key] : -99;
  const ok = now - prev <= DOUBLE_TAP_WINDOW;
  if(!char.tapTimes) char.tapTimes = {};
  char.tapTimes[key] = now;
  if(side === "left") char.lastLeftTapTime = now;
  else char.lastRightTapTime = now;
  return ok;
}
function clearShotCreationStates(){
  [p1,p2].forEach(char => {
    if(!char || !char.moveMeta) return;
    char.moveMeta.fadeawayArmed = false;
    char.moveMeta.fadeawayWindow = 0;
    char.moveMeta.lastShotType = null;
    if(char.moveState === "backjump" && !playerHasBall(char)){
      char.moveState = "normal";
      char.moveTimer = 0;
      char.moveDuration = 0;
      char.visualJumpY = 0;
      char.anim.fadeLean = 0;
      char.anim.legKick = 0;
    }
  });
}
function triggerAnkleBreaker(defender,attacker){
  const movingWrongWay = defender.velocity.length() > .8 && defender.velocity.clone().normalize().dot(getForward(attacker.angle)) < -.15;
  const chance = THREE.MathUtils.clamp(.08 + (attacker.profile.handle-1)*.18 + (movingWrongWay?.12:0) - (defender.movement.recoverySpeed-1)*.2, .02, .34);
  if(Math.random() < chance){
    defender.stumbledTimer = THREE.MathUtils.randFloat(.4,.9);
    showMoveFeedback("Ankle breaker!");
    triggerCrowdReaction("green",0.7);
  }
}
function applyBodyContact(offense,defense){
  const contact = detectDriveContact(offense,defense);
  if(contact) applyContactPhysics(offense,defense,contact);
  const d = flatDistance(offense.group.position,defense.group.position);
  if(d > offense.bodyRadius + defense.bodyRadius + .22) return;
  const diff = defense.movement.contactStrength - offense.movement.contactStrength;
  if(diff > 0) offense.velocity.multiplyScalar(1 - Math.min(.2,diff*.08));
}
function detectDriveContact(offense,defense){
  const d = flatDistance(offense.group.position,defense.group.position);
  if(d > 1.08) return null;
  const toOff = offense.group.position.clone().sub(defense.group.position).setY(0).normalize();
  const defenseFace = getPlayerForward(defense).setY(0).normalize();
  const inFront = defenseFace.dot(toOff) > 0.25;
  const shadeWall = defense.defensiveShadeSide ? 0.14 + defense.defensiveContainBonus*0.12 : 0;
  const strengthEdge = defense.movement.contactStrength - offense.movement.contactStrength + shadeWall;
  const blowBy = calculateBlowByChance(offense,defense);
  if(strengthEdge > 0.28 && inFront) return d < .84 ? "stonewall" : "hard_bump";
  if(blowBy > 0.56 && !inFront) return "blow_by";
  return d < .88 ? "hard_bump" : "light_bump";
}
function applyContactPhysics(offense,defense,contactType){
  const contactScale = contactType === "stonewall" ? 1.15 : contactType === "hard_bump" ? .9 : contactType === "blow_by" ? .55 : .4;
  offense.stamina = Math.max(0,offense.stamina - (0.03 + contactScale*0.04));
  offense.velocity.multiplyScalar(contactType === "blow_by" ? 0.9 : (1 - contactScale*0.14));
  offense.anim.contactReact = Math.max(offense.anim.contactReact,0.2 + contactScale*0.5);
  defense.anim.contactReact = Math.max(defense.anim.contactReact,contactType === "blow_by" ? 0.2 : 0.22 + contactScale*0.34);
  offense.contactType = contactType;
  offense.contactTimer = Math.max(offense.contactTimer || 0,0.34 + contactScale*0.22);
  defense.contactTimer = Math.max(defense.contactTimer || 0,0.24 + contactScale*0.16);
  offense.contactStrength = contactScale * (defense.movement.contactStrength / Math.max(0.7,offense.movement.contactStrength));
  defense.contactStrength = contactScale * (offense.movement.contactStrength / Math.max(0.7,defense.movement.contactStrength));
  offense.contactSide = Math.sign(getBodyRightVector(offense).dot(defense.group.position.clone().sub(offense.group.position)));
  defense.contactSide = -offense.contactSide;
  offense.contactFinishBonusOrPenalty = THREE.MathUtils.clamp((offense.profile.layup-1)*0.14 + (offense.profile.width-defense.profile.width)*0.16 - offense.contactStrength*0.12,-0.28,0.2);
  defense.contactType = contactType;
  offense.contactSlide = Math.max(offense.contactSlide || 0,0.2 + contactScale*0.4);
  defense.contactSlide = Math.max(defense.contactSlide || 0,0.08 + contactScale*0.2);
  startContactAnimation(offense,contactType);
  if(contactType !== "blow_by") startContactAnimation(defense,contactType === "stonewall" ? "hard_bump" : "light_bump");
  spawnCourtThemedParticle(offense.group.position.clone(),"contact");
  showMoveFeedback(contactType === "stonewall" ? "STONEWALL" : contactType === "blow_by" ? "BLOW BY" : (Math.random() > 0.5 ? "BUMP" : "CONTACT"),0.2);
  if(flatDistance(offense.group.position,HOOP_POS) < 4.4 && (contactType === "stonewall" || contactType === "hard_bump")) triggerCrowdReaction("contact",0.35);
}
function startContactAnimation(char,type){
  char.anim.contactReact = Math.max(char.anim.contactReact,type === "stonewall" ? .86 : type === "hard_bump" ? .75 : .35);
}
function calculateBlowByChance(offense,defense){
  return THREE.MathUtils.clamp(.24 + (offense.profile.speed-defense.profile.speed)*.35 + (offense.profile.handle-defense.profile.steal)*.18, .06,.78);
}

function startPostUp(char){ char.moveState = "postup"; char.moveTimer = 0; char.moveDuration = .55; }
function updatePostUp(char,dt){
  const defender = char.id === "p1" ? p2 : p1;
  const toDef = defender.group.position.clone().sub(char.group.position); toDef.y=0;
  if(toDef.length() < 1.35){
    const push = getForward(char.angle).multiplyScalar((char.movement.contactStrength-defender.movement.contactStrength)*.35*dt);
    char.group.position.add(push);
  }
}
function startDropStep(char,side){ startSideHop(char,side==="left"?"left":"right"); }
function shootHookShot(char){ shotPower = .55; shootBall(char,"hook"); }
function shootFadeaway(char){ startStepBack(char); }
function startHesitation(char){ char.dribbleStyle = "hesitation"; char.stamina = Math.max(0,char.stamina-.04); }
function startBehindBack(char,side){ attemptCrossover(char,side==="left"?"left":"right"); }
function startSpinMove(char,side){ char.angle += (side==="left"?1:-1)*Math.PI*.65; }
function startEuroStep(char,side){ startSideHop(char,side==="left"?"left":"right"); }
function shootFloater(char){ shotPower = .44; shootBall(char,"floater"); }
function shootRunner(char){ shotPower = .52; shootBall(char,"runner"); }
function isDefenderInGoodPosition(defender,offense){ return flatDistance(defender.group.position,offense.group.position) < 1.7; }
function getContestConeStrength(defender,shooter){ return isDefenderInGoodPosition(defender,shooter) ? .65 : .2; }
function startCloseout(defender){ defender.velocity.add(getForward(defender.angle).multiplyScalar(1.1)); }
function startRecoveryStep(defender){ defender.stumbledTimer = Math.max(defender.stumbledTimer,.18); }
function punishBadReach(defender){ defender.stumbledTimer = .45; defender.stamina = Math.max(0,defender.stamina-.1); }
function calculateShotContest(shooter){
  const defender = shooter.id === "p1" ? p2 : p1;
  const d = flatDistance(shooter.group.position,defender.group.position);
  const between = isDefenderBetweenBallAndRim(defender, shooter.group.position);
  const toShooter = shooter.group.position.clone().sub(defender.group.position).setY(0).normalize();
  const faceDot = Math.max(0,getForward(defender.angle).dot(toShooter));
  let value = THREE.MathUtils.clamp((2.9 - d) / 2.9,0,1) * THREE.MathUtils.clamp(defender.profile.block*.55 + defender.profile.steal*.35, .35,1.8);
  value *= getGameplayTuning().contestStrength;
  value *= 0.82 + faceDot*0.28;
  if(between) value *= 1.14;
  if(shooter.moveMeta && shooter.moveMeta.lastShotType === "stepback") value *= .82;
  if(shooter.moveMeta && shooter.moveMeta.lastShotType === "fadeaway") value *= .72;
  if(shooter.contactTimer > 0.02) value *= 1.08 + Math.max(0,shooter.contactStrength||0)*0.12;
  if(shooter.bodyUpTimer > 0.05) value *= 1.06;
  const label = value < .12 ? "Open" : value < .28 ? "Light Contest" : value < .48 ? "Good Contest" : value < .7 ? "Heavy Contest" : "Smothered";
  return {value,label};
}
function calculateCoverage(shooter,defender){ return calculateShotContest(shooter).label; }
function calculateReleaseGrade(power,idealPower){
  const d = Math.abs(power-idealPower);
  if(d < .04) return "Perfect";
  if(d < .08) return power < idealPower ? "Slightly Early" : "Slightly Late";
  if(d < .14) return "Good";
  return power < idealPower ? "Early" : "Late";
}
function showShotBreakdown(release,shotType,coverage,result){
  if(!tutorialMode) return;
  const el = document.getElementById("shotFeedback");
  el.style.display = "block";
  el.textContent = `${release} • ${shotType} • ${coverage} • ${result}`;
  setTimeout(()=>{ el.style.display = "none"; }, 1500);
}
function showShotFeedback(text){ showMoveFeedback(text,.8); }
function showMoveFeedback(text,duration=1){
  moveFeedbackTimers.push({text,time:duration});
  flash(text);
}
function updateMoveFeedback(dt){
  for(const m of moveFeedbackTimers) m.time -= dt;
  while(moveFeedbackTimers.length && moveFeedbackTimers[0].time <= 0) moveFeedbackTimers.shift();
}
function createShotTrail(type="shot"){
  if(!qualitySettings[graphicsQuality].particles) return;
  if(!shotTrailPool.length){
    for(let i=0;i<24;i++){
      const p = new THREE.Mesh(new THREE.SphereGeometry(.06,8,8),new THREE.MeshBasicMaterial({color:0x8affee,transparent:true,opacity:0}));
      scene.add(p); shotTrailPool.push(p);
    }
  }
  const p = shotTrailPool.pop();
  if(!p) return;
  p.position.copy(ball.position);
  p.material.color.setHex(type === "hook" ? getThemeColorForEvent("green") : 0x8affee);
  p.material.opacity = type === "hook" ? .8 : .65;
  p.userData.life = type === "hook" ? .4 : .34;
  p.userData.type = type;
  shotTrailActive.push(p);
}
function updateShotTrail(dt){
  for(let i=shotTrailActive.length-1;i>=0;i--){
    const p = shotTrailActive[i];
    const maxLife = p.userData.type === "hook" ? .4 : .34;
    p.userData.life -= dt;
    p.material.opacity = Math.max(0,p.userData.life/maxLife);
    if(p.userData.type === "hook"){
      p.position.y += dt*0.25;
      p.scale.multiplyScalar(1.005);
    }else{
      p.scale.multiplyScalar(.98);
    }
    if(p.userData.life <= 0){ p.material.opacity = 0; p.scale.set(1,1,1); shotTrailActive.splice(i,1); shotTrailPool.push(p); }
  }
}
function animateNetSwish(){
  pulseArenaLights(.8);
}

function animateCharacter(char,dt){
  char.handsUpTime = Math.max(0,char.handsUpTime - dt);
  char.dribbleBounce += dt * 7.8;
  updateAnimationBlend(char,dt);
  animateTorsoLean(char,dt);
  animateLanding(char,dt);
  animateContactReaction(char,dt);
  updateDribbleStyle(char,dt);

  animateRunCycle(char,dt);
  updatePlayerLookAndShadow(char,dt);

  resetArmDefaults(char);
  if(char.onGround) char.group.position.y = Math.max(0,char.visualJumpY || 0);

  if(char.shotMotionTime > 0){
    if(char.hasBall && char.shotMotionTime > 0.4 && ballOwner === char.id){
      releaseShot(char,"jumper",{forced:true});
    }
    char.shotMotionTime -= dt;
    const pct = THREE.MathUtils.clamp(1 - char.shotMotionTime/.46,0,1);
    animateShotTimeline(char,pct,char.moveMeta.lastShotType || "jumper");
    return;
  }
  if(dunkActive && dunkOwner === char.id){
    const pct = THREE.MathUtils.clamp(dunkTimer/Math.max(0.001,dunkDuration),0,1);
    poseDunkArms(char,pct);
    return;
  }

  if(char.blockTime > 0 || char.handsUpTime > 0){
    char.blockTime = Math.max(0,char.blockTime - dt);
    const blockPct = 1 - THREE.MathUtils.clamp(char.blockTime/.42,0,1);
    const reach = Math.sin(blockPct*Math.PI);
    animateDefensiveStance(char,dt);
    poseBlockArms(char,reach);
    return;
  }

  if(char.swipeTime > 0){
    char.swipeTime -= dt;
    const pct = 1 - THREE.MathUtils.clamp(char.swipeTime/.34,0,1);
    const reach = Math.sin(pct*Math.PI);
    poseSwipeArm(char,char.swipeSide || "right",reach);
    return;
  }
  if(char.defensiveShadeSide && possession !== char.id && ballState === "dribble"){
    animateDefensiveStance(char,dt);
  }

  if(char.hasBall && ballState === "dribble"){
    animateDribble(char,dt);
  }
  if(char.moveState === "sidehop") animateSideHop(char,THREE.MathUtils.clamp(char.moveTimer/Math.max(.001,char.moveDuration),0,1));
  if(char.moveState === "sidejump") animateSideJump(char,THREE.MathUtils.clamp(char.moveTimer/Math.max(.001,char.moveDuration),0,1));
  if(char.moveState === "stepback") animateStepBack(char,THREE.MathUtils.clamp(char.moveTimer/Math.max(.001,char.moveDuration),0,1));
  if(char.moveState === "backjump") animateBackJump(char,THREE.MathUtils.clamp(char.moveTimer/Math.max(.001,char.moveDuration),0,1));
  if(char.moveState === "pumpfake") animatePumpFake(char,THREE.MathUtils.clamp(char.moveTimer/Math.max(.001,char.moveDuration),0,1));
  if(char.moveState === "gather") animateGatherBall(char,THREE.MathUtils.clamp(char.moveTimer/Math.max(.001,char.moveDuration),0,1),char.moveMeta.gatherType || "pickup");
  if(char.moveState === "bobble") animateBobble(char,THREE.MathUtils.clamp(char.moveTimer/Math.max(.001,char.moveDuration),0,1));
}
function animateShotTimeline(char,pct,type){
  if(type === "stepback"){ char.group.position.y = Math.max(char.group.position.y, Math.sin(Math.min(1,pct/.35)*Math.PI)*.12); }
  if(type === "jumper"){
    const gather = Math.sin(Math.min(1,pct/.34)*Math.PI);
    const releaseLift = Math.sin(Math.min(1,Math.max(0,(pct-.2)/.6))*Math.PI);
    char.group.position.y = Math.max(char.group.position.y, releaseLift*.22 + gather*.05);
    char.leftLeg.rotation.x = -.2 + gather*.25;
    char.rightLeg.rotation.x = -.2 + gather*.25;
  }
  if(type === "hook"){
    const toRim = HOOP_POS.clone().sub(char.group.position).setY(0).normalize();
    const right = getPlayerRight(char).setY(0).normalize();
    const rimSide = Math.sign(right.dot(toRim));
    const hookSide = rimSide > 0 ? -1 : 1;
    const shootingArm = hookSide > 0 ? char.rightArm : char.leftArm;
    const shootingForearm = hookSide > 0 ? char.rightForearm : char.leftForearm;
    const shootingHand = hookSide > 0 ? char.rightHand : char.leftHand;
    const guideArm = hookSide > 0 ? char.leftArm : char.rightArm;
    const wind = Math.sin(Math.min(1,pct/.62) * Math.PI);
    const shoulderTurn = smoothstep(Math.min(1,pct/.24));
    shootingArm.rotation.x = -1.25 - wind*0.35;
    shootingArm.rotation.z = -hookSide * (0.8 + shoulderTurn*0.4);
    shootingArm.position.x += hookSide * (0.18 + shoulderTurn*0.18);
    shootingForearm.rotation.x = -1.2 - wind * 0.42;
    shootingHand.rotation.x = -0.6;
    guideArm.rotation.x = -0.45;
    guideArm.rotation.z = hookSide * 0.65;
    char.group.rotation.z = -hookSide * 0.12 * wind;
    char.group.rotation.y += hookSide * (0.08*shoulderTurn + 0.04*wind);
    if(char.hookIndicator){
      char.hookIndicator.visible = true;
      char.hookIndicator.material.opacity = Math.max(char.hookIndicator.material.opacity,0.6 * wind);
      char.hookIndicator.position.set(hookSide * .62 * char.profile.width,1.92*char.profile.height,0.26);
    }
  }else{
    poseShotArms(char,pct);
  }
  if(type === "fadeaway"){
    const hang = Math.sin(Math.min(1,pct/.88)*Math.PI);
    char.group.rotation.x = -0.12 - hang*0.14;
    char.leftLeg.rotation.x = -0.22 + hang*0.4;
    char.rightLeg.rotation.x = -0.22 + hang*0.4;
  }
}
function animateDribble(char,dt){
  char.anim.crossStumble = Math.max(0,(char.anim.crossStumble || 0) - dt*1.8);
  const crossLean = char.anim.crossLean || 0;
  const crossPlant = char.anim.crossPlant || 0;
  const crossFake = char.anim.crossFake || 0;
  char.group.rotation.z += crossLean;
  char.group.rotation.y += crossFake;
  char.leftLeg.rotation.x -= crossPlant;
  char.rightLeg.rotation.x -= crossPlant*0.78;
  if(char.anim.crossStumble > 0){
    const s = Math.sin((1-char.anim.crossStumble/0.32)*Math.PI);
    char.group.rotation.z += (ballSideValue < 0 ? -1 : 1) * 0.18 * s;
    char.group.rotation.x -= 0.08 * s;
  }
  const handPos = getDribbleHandPosition(char);
  if(ballSideValue < 0){
    char.leftForearm.rotation.x = Math.sin(char.dribbleBounce)*.45;
    char.leftHand.position.z = handPos.z;
    char.leftHand.position.x = handPos.x;
    char.rightArm.rotation.z += .08;
  }else{
    char.rightForearm.rotation.x = Math.sin(char.dribbleBounce)*.45;
    char.rightHand.position.z = handPos.z;
    char.rightHand.position.x = -handPos.x;
    char.leftArm.rotation.z -= .08;
  }
}
function updateDribbleStyle(char,dt){
  if(!char.hasBall || ballState !== "dribble") return;
  const speed = char.velocity.length();
  if(char.moveState === "stepback") char.dribbleStyle = "stepback_gather";
  else if(char.moveState === "sidehop" || char.moveState === "sidejump") char.dribbleStyle = "crossover";
  else if(speed > 6) char.dribbleStyle = "sprint";
  else if(speed < 1.4) char.dribbleStyle = "protect";
  else char.dribbleStyle = "normal";
  if(char.dribbleStyle === "protect") animateProtectDribble(char,dt);
  if(char.dribbleStyle === "hesitation") animateHesitationDribble(char,dt);
  if(char.dribbleStyle === "crossover") animateLowCrossover(char,dt);
}
function getDribbleHandPosition(char){
  const beat = Math.sin(char.dribbleBounce);
  const depth = char.dribbleStyle === "sprint" ? -.24 : char.dribbleStyle === "protect" ? -.05 : -.14;
  const width = char.dribbleStyle === "protect" ? -.58*char.profile.width : -.65*char.profile.width;
  return {x:width + beat*.02,z:depth + Math.max(0,-beat)*-.12};
}
function animateProtectDribble(char,dt){ char.rightArm.rotation.y = .3; }
function updatePlayerLookAndShadow(char,dt){
  const defender = char.id === "p1" ? p2 : p1;
  const lookTarget = possession === char.id
    ? (char.velocity.length() > 0.45 ? char.group.position.clone().add(char.velocity.clone().setY(0)) : HOOP_POS.clone())
    : (defender && defender.hasBall ? defender.group.position.clone() : ball.position.clone());
  const dx = lookTarget.x - char.group.position.x;
  const dz = lookTarget.z - char.group.position.z;
  const lookYaw = Math.atan2(dx,dz) - char.angle;
  const targetHeadYaw = THREE.MathUtils.clamp(lookYaw,-0.5,0.5);
  char.anim.headYaw = smoothAnimValue(char.anim.headYaw || 0,targetHeadYaw,5,dt);
  if(char.head) char.head.rotation.y = char.anim.headYaw;
  if(char.shadow){
    const airborne = char.group.position.y > 0.2;
    char.shadow.scale.set(airborne ? 0.9 : 1.15,airborne ? 0.65 : 0.82,airborne ? 0.9 : 1.15);
    char.shadow.material.opacity = airborne ? 0.18 : 0.34;
  }
}
function animateHesitationDribble(char,dt){ char.anim.targetLean = Math.sin(char.dribbleBounce)>0 ? -.04 : .18; }
function animateLowCrossover(char,dt){ char.leftHand.position.y = Math.min(char.leftHand.position.y,.58*char.profile.height); char.rightHand.position.y = Math.min(char.rightHand.position.y,.58*char.profile.height); }
function animateGatherBall(char,pct,type){
  const w = char.profile.width;
  const h = char.profile.height;
  const t = smoothstep(pct);
  resetArmDefaults(char);

  if(type === "catch"){
    char.leftArm.position.set(-0.34*w,1.48*h,-0.16);
    char.rightArm.position.set(0.34*w,1.48*h,-0.16);
    char.leftForearm.position.set(-0.22*w,1.32*h,-0.38);
    char.rightForearm.position.set(0.22*w,1.32*h,-0.38);
    char.leftHand.position.set(-0.16*w,1.22*h,-0.52);
    char.rightHand.position.set(0.16*w,1.22*h,-0.52);
  }else if(type === "shot"){
    const bend = Math.sin(t*Math.PI);
    char.group.scale.y = 1 - bend * 0.08;
    char.leftForearm.position.y -= (0.1 + bend*0.12) * h;
    char.rightForearm.position.y -= (0.1 + bend*0.12) * h;
    char.leftHand.position.y -= (0.16 + bend*0.16) * h;
    char.rightHand.position.y -= (0.16 + bend*0.16) * h;
    char.leftHand.position.z -= 0.18 + bend*0.12;
    char.rightHand.position.z -= 0.18 + bend*0.12;
  }else{
    char.group.scale.y = 1 - Math.sin(t * Math.PI) * 0.06;
    char.leftForearm.position.y -= 0.18 * h;
    char.rightForearm.position.y -= 0.18 * h;
    char.leftHand.position.y -= 0.28 * h;
    char.rightHand.position.y -= 0.28 * h;
    char.leftHand.position.z -= 0.28;
    char.rightHand.position.z -= 0.28;
  }
}
function animateBobble(char,pct){
  const w = char.profile.width;
  const shake = Math.sin(pct * Math.PI * 5) * 0.08;
  resetArmDefaults(char);
  char.leftHand.position.x -= 0.14*w + shake;
  char.rightHand.position.x += 0.14*w - shake;
  char.leftForearm.rotation.z -= 0.25;
  char.rightForearm.rotation.z += 0.25;
}
function animateDefensiveStance(char,dt){
  const stance = char.anim.defenseStanceBlend || 0;
  const shuffle = char.anim.shuffleCycle || 0;
  const cutoffLean = char.anim.cutoffLean || 0;
  char.group.position.y = Math.max(0,char.group.position.y - dt*(.18 + stance*.16));
  char.group.scale.y = Math.max(.9,1 - stance*.06);
  char.group.rotation.x = clampLean(char.group.rotation.x + 0.03 + stance*0.05,-0.14,0.16);
  const shadeDir = char.defensiveShadeSide === "left" ? -1 : char.defensiveShadeSide === "right" ? 1 : 0;
  char.leftLeg.rotation.z = -.16 - stance*0.1; char.rightLeg.rotation.z = .16 + stance*0.1;
  char.leftLeg.rotation.x += stance*0.12;
  char.rightLeg.rotation.x += stance*0.12;
  char.leftShoe.position.x = (char.leftShoe.userData.base?.x || char.leftShoe.position.x) - stance*0.02;
  char.rightShoe.position.x = (char.rightShoe.userData.base?.x || char.rightShoe.position.x) + stance*0.02;
  if(stance > .08){
    const slide = Math.sin(shuffle) * 0.08 * char.profile.width;
    char.leftShoe.position.z += slide;
    char.rightShoe.position.z -= slide;
  }
  if(shadeDir){
    char.leftArm.rotation.z = -0.5 - shadeDir*0.08;
    char.rightArm.rotation.z = 0.5 - shadeDir*0.08;
    char.group.rotation.x = clampLean(Math.max(char.group.rotation.x,0.05 + char.defensiveShadeTime*0.04 + stance*0.05),-0.14,0.16);
    char.group.rotation.z = clampLean(char.group.rotation.z + cutoffLean * 0.03,-0.12,0.12);
  }
}
function animateRunCycle(char,dt){
  const moving = char.velocity.length() > .12;
  const cyc = char.anim.runCycle || 0;
  const specialActive = shotCharging || char.blockTime > 0 || char.swipeTime > 0 || char.moveMeta.lastShotType === "hook" || (dunkActive && dunkOwner === char.id) || (layupActive && layupOwner === char.id);
  const walkBlend = char.anim.walkBlend || 0;
  const sprintBlend = char.anim.sprintBlend || 0;
  const backBlend = char.anim.backpedalBlend || 0;
  const strideStrength = char.anim.strideStrength || 0;
  const stepWave = Math.sin(cyc);
  const legWave = specialActive ? 0 : stepWave;
  const leftBase = char.leftShoe.userData.base || (char.leftShoe.userData.base = char.leftShoe.position.clone());
  const rightBase = char.rightShoe.userData.base || (char.rightShoe.userData.base = char.rightShoe.position.clone());

  const walkStride = 0.16 + walkBlend * 0.17;
  const sprintStride = sprintBlend * (0.29 + strideStrength*0.13);
  const backStride = backBlend * 0.12;
  const stride = walkStride + sprintStride + backStride;
  const cutPlant = char.anim.cutPlant || 0;
  const cutDir = char.anim.cutDir || 0;
  const outsideLeft = cutDir > 0;
  const kneeLift = 0.024 + walkBlend*0.024 + sprintBlend*0.052;
  const footTravel = (0.028 + walkBlend*0.026 + sprintBlend*0.048 - backBlend*0.01) * char.profile.width;

  char.leftLeg.rotation.x = legWave * (stride - backStride*0.45);
  char.rightLeg.rotation.x = -legWave * (stride + backStride*0.45);
  char.leftLeg.rotation.z = -0.03 * backBlend;
  char.rightLeg.rotation.z = 0.03 * backBlend;
  char.leftShoe.position.z = leftBase.z + legWave * footTravel;
  char.rightShoe.position.z = rightBase.z - legWave * footTravel;
  char.leftShoe.position.y = leftBase.y + Math.max(0,legWave) * kneeLift;
  char.rightShoe.position.y = rightBase.y + Math.max(0,-legWave) * kneeLift;
  char.leftShoe.rotation.x = char.leftLeg.rotation.x * 0.78;
  char.rightShoe.rotation.x = char.rightLeg.rotation.x * 0.78;
  if(cutPlant > 0.05){
    const bend = cutPlant * 0.2;
    char.leftLeg.rotation.x += bend;
    char.rightLeg.rotation.x += bend;
    if(outsideLeft){
      char.leftShoe.position.z = leftBase.z + legWave * footTravel * 0.25;
      char.leftShoe.position.y = leftBase.y + Math.max(0,legWave) * kneeLift * 0.35;
    }else{
      char.rightShoe.position.z = rightBase.z - legWave * footTravel * 0.25;
      char.rightShoe.position.y = rightBase.y + Math.max(0,-legWave) * kneeLift * 0.35;
    }
  }

  if(!moving || specialActive){
    char.leftShoe.position.lerp(leftBase,dt*8);
    char.rightShoe.position.lerp(rightBase,dt*8);
  }
  if(!shotCharging && char.blockTime <= 0 && char.swipeTime <= 0){
    const armDrive = 0.03 + walkBlend*0.03 + sprintBlend*0.11 - backBlend*0.005;
    const ballSide = char.hasBall ? ((char.dribbleHand || "right") === "right" ? 1 : -1) : 0;
    char.leftArm.rotation.z += Math.sin(cyc + Math.PI) * armDrive + (ballSide > 0 ? 0.03 : 0);
    char.rightArm.rotation.z += Math.sin(cyc) * armDrive - (ballSide < 0 ? 0.03 : 0);
  }
  char.group.position.y += Math.sin(cyc*2) * .0009 * (char.anim.verticalBob || 0);
}
function animateSideHop(char,pct){
  const sideSign = (char.moveMeta.sideHop==="left")?-1:1;
  char.group.rotation.z = sideSign * Math.sin(pct*Math.PI)*.06;
  const protectHand = char.moveMeta.sideHopHand || getBestHandForScreenSide(char,char.moveMeta.sideHop || "right");
  poseSwipeArm(char,protectHand,Math.sin(pct*Math.PI)*.42);
}
function animateSideJump(char,pct){
  const sideSign = (char.moveMeta.sideJump === "left") ? -1 : 1;
  const hang = Math.sin(pct*Math.PI);
  char.leftLeg.rotation.x = -0.12 + hang * 0.42;
  char.rightLeg.rotation.x = -0.12 + hang * 0.42;
  char.leftLeg.rotation.z = sideSign * -0.16 * hang;
  char.rightLeg.rotation.z = sideSign * 0.16 * hang;
}
function animateStepBack(char,pct){ char.group.rotation.x = -Math.sin(pct*Math.PI)*.08; }
function animateBackJump(char,pct){
  const hang = Math.sin(pct*Math.PI);
  char.leftLeg.rotation.x = -0.2 + hang * 0.34;
  char.rightLeg.rotation.x = -0.2 + hang * 0.34;
  char.group.rotation.x = -hang * 0.18;
}
function animatePumpFake(char,pct){ poseShotArms(char,Math.min(.45,pct*.9)); }

function getDribbleFlavor(char){
  const name = (char.profile.name || "").toLowerCase();
  const flavor = {bounceSpeed:1,bounceAmp:1,forward:1,cross:1,wobble:1,gather:1};
  if(name.includes("flash guard")){ flavor.bounceSpeed=1.16; flavor.bounceAmp=.82; flavor.cross=1.26; flavor.gather=.9; }
  else if(name.includes("deep range")){ flavor.forward=.9; flavor.bounceAmp=.88; flavor.cross=1.08; flavor.gather=.86; }
  else if(name.includes("rim reaper") || name.includes("paint bruiser") || name.includes("glass tower")){ flavor.forward=1.12; flavor.bounceAmp=1.14; flavor.wobble=1.22; flavor.gather=1.12; }
  else if(name.includes("smooth shooter")){ flavor.bounceAmp=.9; flavor.cross=1.02; flavor.gather=.84; }
  else if(name.includes("the claw")){ flavor.forward=1.03; flavor.cross=1.06; flavor.wobble=.82; }
  else if(name.includes("crafty lefty")){ flavor.cross=1.34; flavor.wobble=.92; flavor.forward=1.02; }
  return flavor;
}
