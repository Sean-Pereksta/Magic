function updateBot(dt){
  if(!botBrain) initBotBrain();
  updateBotMemoryOfPlayer();
  adjustBotPersonalityByDifficulty();
  if(updateBotLooseBall(dt)) return;
  updateBotReactionMemory(dt);
  if(possession === "p1") updateBotDefense(dt);
  else updateBotOffense(dt);
}

function updateBotLooseBall(dt){
  const liveRebound = ballState === "shot" && (rimTouchCount > 0 || shotHasHitRimOrBoard || (ballVelocity.y < -1 && ball.position.y < HOOP_POS.y + 0.35));
  if(ballState !== "loose" && !liveRebound) return false;

  const target = ball.position.clone();
  target.y = 0;
  moveBotToward(p2,target,difficultySettings[selectedDifficulty].speed * 1.22,dt);

  if(ball.position.y > 1.1 && ball.position.y < 2.6 && flatDistance(p2.group.position,ball.position) < 1.4){
    if(p2.onGround && p2.jumpCooldown <= 0 && Math.random() < dt * difficultySettings[selectedDifficulty].defensiveIQ){
      tryJumpBlock(p2,false);
    }else{
      raiseHands(p2);
    }
  }

  return true;
}

function moveBotToward(bot,target,speed,dt){
  const to = target.clone().sub(bot.group.position);
  to.y = 0;
  const dist = to.length();
  if(dist > .1){
    const desired = Math.atan2(to.x,to.z);
    bot.angle = lerpAngle(bot.angle,desired,dt*4.2);
  }

  let mult = 1;
  const settings = difficultySettings[selectedDifficulty];
  if(bot.sprint > SPRINT_MIN && Math.random() < settings.sprint && dist > 1.2){
    mult = 1.45;
    bot.sprint = Math.max(0,bot.sprint - SPRINT_DRAIN * dt);
  }else{
    bot.sprint = Math.min(1,bot.sprint + SPRINT_RECOVER * dt);
  }

  bot.velocity.set(0,0,0);
  if(dist > .35) bot.velocity.add(getForward(bot.angle).multiplyScalar(speed * mult));
  bot.group.position.add(bot.velocity.clone().multiplyScalar(dt));
  keepInCourt(bot.group.position);
  bot.group.rotation.y = bot.angle;
}

function initBotBrain(){
  botBrain = {
    intent:"size_up", intentTimer:0, decisionCooldown:0, reactionTimer:0,
    lastSeenPlayerPosition:p1 ? p1.group.position.clone() : new THREE.Vector3(),
    lastSeenBallPosition:ball ? ball.position.clone() : new THREE.Vector3(),
    aggression:.5, patience:.5, confidence:.5, fakeBiteChance:.25, mistakeChance:.12, defensiveSpacing:1.7,
    preferredDriveSide:Math.random()<.5?-1:1, recentMoves:[], crossedRecently:0, gotBeatRecently:0, shotClockMentalTimer:0,
    blockCooldown:0, stealCooldown:0, crossoverCooldown:0, pumpFakeCooldown:0, stepBackCooldown:0, sideHopCooldown:0, helpDelay:0,
    defenseIntent:"balanced_defense", defenseIntentTimer:.6
  };
  configureBotBrainFromDifficulty();
}
function configureBotBrainFromDifficulty(){
  const s = difficultySettings[selectedDifficulty];
  if(!s || !botBrain) return;
  botBrain.aggression = s.aggression;
  botBrain.patience = s.patience;
  botBrain.fakeBiteChance = s.fakeBiteChance;
  botBrain.mistakeChance = s.mistakeChance;
  botBrain.confidence = s.composure;
}
function getBotPersonality(){
  const map={sky:"rim_runner",flash:"ankle_breaker",smooth:"sharpshooter",bruiser:"post_bully",claw:"lockdown",rocket:"slasher",crafty:"fake_master",tower:"paint_anchor"};
  return botPersonalities[map[selectedP2] || "slasher"];
}
function chooseBotIntent(){
  const s = difficultySettings[selectedDifficulty];
  const w = {...getBotPersonality()};
  const dist = flatDistance(p2.group.position,HOOP_POS);
  const defDist = flatDistance(p2.group.position,p1.group.position);
  if(dist < 2.9){
    const rimPressure = flatDistance(p1.group.position,HOOP_POS);
    const profileFinisher = (p2.profile.layup || 1) > (p2.profile.shot || 1) * 1.08;
    w.attack_rim = (w.attack_rim || w.drive || .22) + (profileFinisher ? .42 : .24);
    w.layup = (w.layup || .2) + (profileFinisher ? .32 : .12);
    if(rimPressure < 1.5 && s.defensiveIQ > .84){
      w.step_back = (w.step_back || w.stepBack || 0) + s.stepBackUse * .8;
      w.pull_up = (w.pull_up || w.pullUp || 0) + s.shotDiscipline * .55;
      w.attack_rim *= .62;
    }
  }
  if(defDist < 1.3){ w.step_back = (w.step_back || w.stepBack || 0) + s.stepBackUse*.5; w.pull_up = (w.pull_up || w.pullUp || 0) + s.pumpFakeUse*.45; }
  if(defDist > 1.7 && s.shotDiscipline > .78 && (p2.profile.shot || 1) > (p2.profile.layup || 1)*.95){
    w.pull_up = (w.pull_up || w.pullUp || 0) + .24 + s.shotDiscipline * .35;
    w.step_back = (w.step_back || w.stepBack || 0) + s.stepBackUse * .38;
  }
  if((p2.stamina || 1) < (1 - s.staminaManagement * .55)){
    w.reset_dribble = (w.reset_dribble || w.reset || 0) + .34;
    w.attack_rim = (w.attack_rim || w.drive || .2) * .72;
  }
  if(p1.stumbledTimer > 0){ w.attack_rim = (w.attack_rim || w.drive || .2) + .28; w.pull_up = (w.pull_up || w.pullUp || 0) + .22; }
  const recentPenalty = new Set(botBrain.recentMoves.slice(-3));
  Object.keys(w).forEach(k=>{ if(recentPenalty.has(k)) w[k] *= (selectedDifficulty==="rookie"?.9:.45); });
  const entries = Object.entries(w).map(([k,v])=>[k,Math.max(0.01,v)]);
  const total = entries.reduce((a,[,v])=>a+v,0);
  let r = Math.random()*total;
  for(const [key,val] of entries){ r -= val; if(r<=0) return normalizeIntentName(key); }
  return "size_up";
}
function normalizeIntentName(key){
  const map={pullUp:"pull_up",stepBack:"step_back",pumpFake:"pull_up",sideHop:"side_hop_right",drive:"attack_rim",layup:"attack_rim",post:"post_up",crossover:"crossover_left",safeDrive:"drive_right",backDown:"back_down",hesitation:"hesitation",reset:"reset_dribble"};
  return map[key] || key;
}
function executeBotIntent(dt){
  const bot = p2;
  const intent = botBrain.intent;
  if(bot.moveState !== "normal"){ updateMoveState(bot,dt); return; }
  if(intent.includes("drive") || intent==="attack_rim"){
    const side = intent.includes("left")?-1:1;
    const toRim = HOOP_POS.clone().sub(bot.group.position); toRim.y=0; toRim.normalize();
    const sideV = new THREE.Vector3(toRim.z,0,-toRim.x).multiplyScalar(side*1.3);
    moveBotToward(bot, bot.group.position.clone().add(toRim.multiplyScalar(2.2)).add(sideV), difficultySettings[selectedDifficulty].speed, dt);
    const rimProtecting = flatDistance(p1.group.position,HOOP_POS) < 1.5;
    if(isCloseLayupRange(bot) && Math.random()<dt*1.3){
      if(rimProtecting && difficultySettings[selectedDifficulty].defensiveIQ > .84 && Math.random() < difficultySettings[selectedDifficulty].stepBackUse){
        startStepBack(bot);
      }else startLayup(bot);
    }
  }else if(intent==="step_back"){ startStepBack(bot); }
  else if(intent==="pump_fake"){
    if(!p1.onGround && difficultySettings[selectedDifficulty].shotDiscipline > .75) botBrain.intent = "attack_rim";
    else botBrain.intent = "pull_up";
  }
  else if(intent.includes("side_hop")){ startSideHop(bot,intent.includes("left")?"left":"right"); }
  else if(intent.includes("crossover")){ attemptCrossover(bot,intent.includes("left")?"left":"right"); }
  else if(intent==="pull_up" && flatDistance(bot.group.position,HOOP_POS)<8.4){ shotPower = .58 + Math.random()*.24; shootBall(bot,"pullup"); }
  else if(intent==="post_up" || intent==="back_down"){
    moveBotToward(bot,new THREE.Vector3(0,0,-8.7),difficultySettings[selectedDifficulty].speed*.86,dt);
    const hookChance =
      0.08 +
      (bot.profile.hook || 0.75) * 0.08 +
      difficultySettings[selectedDifficulty].shotDiscipline * 0.06;
    if(canStartHookShot(bot) && Math.random() < dt * hookChance * (selectedDifficulty === "rookie" ? 0.45 : 1)){
      shotPower = 0.56 + Math.random() * 0.16;
      shootBall(bot,"hook");
    }
  }
  else { moveBotToward(bot,new THREE.Vector3(2.2*Math.sin(performance.now()*.0017),0,-7.4),difficultySettings[selectedDifficulty].speed*.72,dt); }
}
function getDefenderRecoveryTarget(defender,ballHandler,s){
  const handlerRimDist = flatDistance(ballHandler.group.position,HOOP_POS);
  const beaten = flatDistance(defender.group.position,ballHandler.group.position) > (1.65 + (1-s.defensiveIQ)*0.45);
  const rimProtectBias = THREE.MathUtils.clamp((defender.profile.block - 1) * 0.45 + (1-handlerRimDist/8) * 0.35,0,1);
  const pressureBias = THREE.MathUtils.clamp((defender.profile.steal - 1) * 0.4 + s.stealDiscipline * 0.35,0,1);
  if(handlerRimDist < 2.2 || (beaten && rimProtectBias > pressureBias + 0.08)){
    const toBall = ballHandler.group.position.clone().sub(HOOP_POS).setY(0);
    if(toBall.lengthSq() < .0001) return HOOP_POS.clone();
    toBall.normalize();
    return HOOP_POS.clone().add(toBall.multiplyScalar(0.7 + (1-rimProtectBias)*0.35));
  }
  return getDefensiveTarget(defender,ballHandler,"cutoff_drive");
}

function updateBotDefense(dt){
  const s = difficultySettings[selectedDifficulty];
  botBrain.defenseIntentTimer = (botBrain.defenseIntentTimer || 0) - dt;
  if(!botBrain.defenseIntent || botBrain.defenseIntentTimer <= 0){
    botBrain.defenseIntent = chooseBotDefensiveIntent();
    botBrain.defenseIntentTimer = THREE.MathUtils.randFloat(.35,1.05);
  }
  const laneAnchor = getBetweenPlayerAndRimTarget(p2,p1,1.04 - s.spacingIQ * 0.22);
  const intentTarget = getDefensiveTarget(p2,p1,botBrain.defenseIntent);
  const recoveryTarget = getDefenderRecoveryTarget(p2,p1,s);
  const beaten = flatDistance(p2.group.position,p1.group.position) > (1.8 + (1-s.defensiveIQ)*0.45);
  const recoverBlend = beaten ? THREE.MathUtils.clamp(.45 + p2.profile.block*0.18,0.38,0.9) : 0;
  const containTarget = laneAnchor.clone().lerp(intentTarget,THREE.MathUtils.clamp(.28 + s.spacingIQ * .62,.3,.9)).lerp(recoveryTarget,recoverBlend);
  moveBotToward(p2,containTarget,s.speed*(.86+s.defensiveIQ*.18),dt);

  if(ballState === "dribble" && flatDistance(p2.group.position,p1.group.position) < (1.35 + p2.movement.defensiveWidth*.42) && botBrain.stealCooldown <= 0){
    const dist = flatDistance(p2.group.position,p1.group.position);
    const badHandle = (p1.profile.handle || 1) < 0.96;
    const sprintExpose = p1.velocity.length() > 4.9 && badHandle;
    const looseWindow = p1.dribbleStyle === "sprint" || p1.dribbleStyle === "crossover" || p1.moveState === "sidehop";
    const exposed = looseWindow || sprintExpose || (p1.bodyUpTimer > 0.03 && dist < 1.15);
    const discipline = THREE.MathUtils.clamp(s.stealDiscipline * 0.75 + s.composure * 0.35,0.1,1);
    const reachyMistake = Math.random() < dt * (1-discipline) * 1.2;
    if((exposed && Math.random() < dt*s.steal*THREE.MathUtils.lerp(.6,1.2,discipline)) || reachyMistake){
      p2.swipeSide = Math.random()<.5?"left":"right"; p2.swipeTime = .34;
      botBrain.stealCooldown = THREE.MathUtils.lerp(.38,.8,discipline);
      if(reachyMistake) punishBadReach(p2);
    }
  }

  const contestedType = dunkActive ? "dunk" : layupActive ? "layup" : getRequestedShotType(p1);
  const timingPct = shotCharging ? THREE.MathUtils.clamp((p1.moveMeta.chargeHeld || 0) / 1.2,0,1) : .2;
  if((shotCharging || layupActive || dunkActive) && flatDistance(p2.group.position,p1.group.position) < 3.8 && botBrain.blockCooldown <= 0){
    if(shouldBotJumpContest(p2,p1,contestedType,timingPct)){ tryJumpBlock(p2, p2.sprint > .25 && Math.random() < s.sprint); botBrain.blockCooldown = .62; }
    else if(!p2.onGround && Math.random() < dt * s.block) raiseHands(p2);
  }
  if(ballState === "loose" && ball.position.y > 1.2 && flatDistance(p2.group.position,ball.position) < 1.65 && p2.onGround){
    if(shouldBotJumpContest(p2,p1,"loose",.3)) tryJumpBlock(p2,false);
  }
}
function updateBotOffense(dt){
  const s = difficultySettings[selectedDifficulty];
  botBrain.intentTimer -= dt; botBrain.decisionCooldown -= dt;
  if(botBrain.intentTimer <= 0 && botBrain.decisionCooldown <= 0){
    botBrain.intent = chooseBotIntent();
    botBrain.recentMoves.push(botBrain.intent); if(botBrain.recentMoves.length > 8) botBrain.recentMoves.shift();
    botBrain.intentTimer = THREE.MathUtils.randFloat(.35,1.4);
    botBrain.decisionCooldown = THREE.MathUtils.randFloat(.16,.34) * (1.2-s.composure*.35);
  }
  executeBotIntent(dt);
}
function updateBotReactionMemory(dt){
  if(!botBrain) return;
  const s = difficultySettings[selectedDifficulty];
  botBrain.reactionTimer -= dt; botBrain.blockCooldown = Math.max(0,botBrain.blockCooldown - dt); botBrain.stealCooldown = Math.max(0,botBrain.stealCooldown - dt);
  if(botBrain.reactionTimer <= 0){
    botBrain.lastSeenPlayerPosition.copy(p1.group.position);
    botBrain.lastSeenBallPosition.copy(ball.position);
    botBrain.reactionTimer = THREE.MathUtils.randFloat(s.reactionMin,s.reactionMax);
  }
}
function updateBotMemoryOfPlayer(){
  if(!botBrain) return;
  botBrain.playerTendency = getPlayerTendency();
}
function getPlayerTendency(){
  if(!p1) return "balanced";
  const shotRate = p1.stats.fga > 0 ? p1.stats.tpa / Math.max(1,p1.stats.fga) : .3;
  if(shotRate > .45) return "perimeter";
  if(p1.stats.ankle > 1) return "dribble-heavy";
  return "drive";
}
function chooseCounterMove(){
  const t = botBrain && botBrain.playerTendency;
  if(t === "perimeter") return "closeout";
  if(t === "dribble-heavy") return "body-up";
  return "paint-wall";
}
function chooseBotDefensiveIntent(){
  const options = ["paint_wall","pressure_steal","sag_contest","cutoff_drive","bait_jumper","balanced_defense"];
  const weights = {paint_wall:.25,pressure_steal:.15,sag_contest:.16,cutoff_drive:.16,bait_jumper:.1,balanced_defense:.18};
  if(selectedDifficulty === "rookie" || selectedDifficulty === "easy"){
    weights.paint_wall -= .06; weights.bait_jumper += .06;
  }
  if(selectedDifficulty === "hard" || selectedDifficulty === "elite" || selectedDifficulty === "legend" || selectedDifficulty === "pro" || selectedDifficulty === "superstar" || selectedDifficulty === "hallOfFame" || selectedDifficulty === "impossible"){
    weights.cutoff_drive += .07; weights.paint_wall += .06;
  }
  let r = Math.random() * options.reduce((a,k)=>a+weights[k],0);
  for(const k of options){ r -= weights[k]; if(r <= 0) return k; }
  return "balanced_defense";
}
function shouldBotJumpContest(defender, shooter, shotType, timingPct){
  const s = difficultySettings[selectedDifficulty];
  const ideal = shotType === "dunk" ? .74 : shotType === "layup" ? .63 : .58;
  const timingWindow = THREE.MathUtils.lerp(.22,.05,s.contestTiming);
  const timingError = Math.abs(timingPct - ideal);
  if((shotType === "layup" || shotType === "dunk" || shotType === "jumper") && timingError > timingWindow && Math.random() < s.composure) return false;
  const dist = flatDistance(defender.group.position, shooter.group.position);
  const between = isDefenderBetweenBallAndRim(defender, ball.position);
  let chance = 0;
  if(dist < 2.2) chance += .25;
  if(dist < 1.5) chance += .25;
  if(between) chance += .18;
  chance += s.contestTiming * .35;
  chance += (defender.profile.block - 1) * .18;
  chance += (1 - Math.min(1,timingError*1.8)) * .16;
  if(shotType === "jumper") chance -= .1 + s.fakeBiteChance*0.1;
  if(shotType === "pump_fake" && s.contestTiming > .72) chance *= (1 - s.composure*0.45);
  chance -= THREE.MathUtils.clamp(timingError - timingWindow,0,0.35) * (0.3 + s.fakeBiteChance*0.2);
  if(shotType === "dunk" || shotType === "layup") chance += .22;
  return Math.random() < THREE.MathUtils.clamp(chance, .05, .88);
}
function adjustBotPersonalityByDifficulty(){
  if(!botBrain) return;
  const d = selectedDifficulty;
  botBrain.adaptiveness = d === "impossible" ? .99 : d === "hallOfFame" ? .97 : d === "superstar" ? .96 : d === "pro" ? .95 : d === "legend" ? .95 : d === "elite" ? .78 : d === "hard" ? .58 : .35;
}
function getDefensiveTarget(defender,ballHandler,intent="balanced_defense"){
  const spacing = intent === "pressure_steal" ? .7 : intent === "sag_contest" ? 1.95 : 1.28;
  const base = getBetweenPlayerAndRimTarget(defender,ballHandler,spacing);
  const toHoop = HOOP_POS.clone().sub(ballHandler.group.position); toHoop.y = 0;
  if(toHoop.lengthSq() < .0001) return base;
  toHoop.normalize();
  const lane = new THREE.Vector3(toHoop.z,0,-toHoop.x);
  if(intent === "cutoff_drive"){
    return base.add(lane.multiplyScalar((botBrain.preferredDriveSide||1) * .65));
  }
  if(intent === "bait_jumper"){
    return base.add(toHoop.clone().multiplyScalar(.35));
  }
  if(intent === "balanced_defense"){
    return base.add(lane.multiplyScalar(Math.sin(performance.now()*.0015)*.2));
  }
  return base;
}
