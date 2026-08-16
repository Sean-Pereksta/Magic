function updateCameras(dt){
  if(!p1){
    camMain.position.set(0,5,9);
    camMain.lookAt(0,1,0);
    return;
  }

  updateFollowCamera(camP1,p1,dt,"p1",true);
  updateFollowCamera(camP2,p2,dt,"p2",true);
  updateFollowCamera(camMain,p1,dt,"main",false);
  screenShake = Math.max(0,screenShake - dt*.75);
}

function updateFollowCamera(cam,char,dt,cameraKey="main",isSplit=false){
  if(!char) return;
  const cState = cameraState[cameraKey] || cameraState.main;
  const forward = getForward(char.angle);
  const speed = char.velocity ? Math.min(1.45,char.velocity.length()/6.5) : 0;
  const ballPos = ball.position.clone();
  const toHoop = HOOP_POS.clone().sub(char.group.position);
  const nearRim = flatDistance(char.group.position,HOOP_POS) < 5.2;
  const driveLeadAmt = 0.45 + speed*1.55;
  const driveLead = forward.clone().multiplyScalar(driveLeadAmt * (isSprinting(char) ? 1 : 0.55));
  cState.lead.lerp(driveLead,THREE.MathUtils.clamp(dt*4.8,0,1));

  let zoomTarget = 0;
  if(speed > 0.62) zoomTarget -= 0.14;
  if(ballState === "loose") zoomTarget -= 0.12;
  if(shotCharging && ballOwner === char.id) zoomTarget += 0.1;
  if(ballState === "shot") zoomTarget -= 0.04;
  if(dunkActive && dunkOwner === char.id) zoomTarget -= 0.28;
  if(isSplit) zoomTarget = THREE.MathUtils.clamp(zoomTarget,-0.14,0.08);
  cState.zoom = THREE.MathUtils.lerp(cState.zoom,zoomTarget,THREE.MathUtils.clamp(dt*3.2,0,1));

  const playerSpread = p1&&p2 ? flatDistance(p1.group.position,p2.group.position) : 0;
  const narrowBoost = cam.aspect<1.15 ? (1.15-cam.aspect)*2.4 : 0;
  const backDist = ((5.4 + cameraEffects.zoomPulse*2.2) * (1-cState.zoom)) + Math.min(1.25,playerSpread*.12) + narrowBoost;
  const height = 3.35 + (ballState === "shot" ? 0.4 : 0) + (nearRim ? 0.15 : 0);
  const target = char.group.position.clone().add(cState.lead).add(forward.clone().multiplyScalar(-backDist)).add(new THREE.Vector3(0,height,0));

  if(nearRim) target.add(toHoop.normalize().multiplyScalar(0.8));
  if(ballState === "shot" || ballState === "loose" || dunkActive) target.add(ballPos.clone().sub(char.group.position).multiplyScalar(0.12));

  target.x = THREE.MathUtils.clamp(target.x,-8.8,8.8);
  target.z = THREE.MathUtils.clamp(target.z,-13.2,12.8);
  target.y = THREE.MathUtils.clamp(target.y,2.2,6.3);
  if(Math.abs(target.z - HOOP_POS.z) < 1.35 && Math.abs(target.x) < 1.95){
    target.z += target.z > HOOP_POS.z ? 0.9 : -0.9;
    target.y += 0.35;
  }

  cam.position.lerp(target,THREE.MathUtils.clamp(dt*5.2,0,1));

  const look = char.group.position.clone().add(forward.clone().multiplyScalar(3.1)).add(new THREE.Vector3(0,1.35,0));
  if(!isSplit&&p1&&p2){
    const matchupCenter=p1.group.position.clone().lerp(p2.group.position,.5).add(new THREE.Vector3(0,1.2,0));
    look.lerp(matchupCenter,.24);
  }
  if(nearRim) look.lerp(HOOP_POS.clone().add(new THREE.Vector3(0,0.25,0)),0.34);
  if(ballState === "shot" || ballState === "loose") look.lerp(ballPos,0.4);
  if(dunkActive && dunkOwner === char.id){
    look.lerp(HOOP_POS.clone().add(new THREE.Vector3(0,0.42,0)),0.55);
    look.lerp(ballPos,0.42);
  }
  cState.look.lerp(look,THREE.MathUtils.clamp(dt*6.8,0,1));

  if(screenShake > 0 || cameraEffects.shake > 0){
    const shakeAmt = screenShake*0.6 + cameraEffects.shake;
    cam.position.x += (Math.random()-.5)*shakeAmt;
    cam.position.y += (Math.random()-.5)*shakeAmt*0.45;
  }

  cam.lookAt(cState.look);
}

function renderScene(){
  const w = window.innerWidth;
  const h = window.innerHeight;

  renderer.setScissorTest(false);
  renderer.clear();

  if(twoPlayerMode && gameStarted){
    renderer.setScissorTest(true);

    renderer.setViewport(0,0,w/2,h);
    renderer.setScissor(0,0,w/2,h);
    camP1.aspect = (w/2)/h;
    camP1.updateProjectionMatrix();
    renderer.render(scene,camP1);

    renderer.setViewport(w/2,0,w/2,h);
    renderer.setScissor(w/2,0,w/2,h);
    camP2.aspect = (w/2)/h;
    camP2.updateProjectionMatrix();
    renderer.render(scene,camP2);

    renderer.setScissorTest(false);
  }else{
    renderer.setViewport(0,0,w,h);
    camMain.aspect = w/h;
    camMain.updateProjectionMatrix();
    renderer.render(scene,camMain);
  }
}

function updateHUD(){
  const score = document.getElementById("scoreText");
  if(score) score.innerHTML = `${scoreP1} <span>—</span> ${scoreP2}`;
  if(p1) document.getElementById("p1HudName").textContent = p1.profile.name;
  if(p2) document.getElementById("p2HudName").textContent = p2.profile.name;
  if(messageTimer<=0&&!tutorialMode) document.getElementById("msg").textContent = "";
}

function toggleUI(){
  uiHidden = !uiHidden;
  document.getElementById("hud").classList.toggle("hidden",uiHidden);
  document.getElementById("msg").classList.toggle("hidden",uiHidden);
  document.getElementById("hideTip").classList.toggle("hidden",uiHidden);
}

function isCloseLayupRange(char){
  return flatDistance(char.group.position,HOOP_POS) < 3.2 && char.group.position.z < HOOP_POS.z + 2.8;
}

function getOwner(){
  return ballOwner === "p1" ? p1 : p2;
}

function getHandWorldPosition(char,handName){
  const local = handName === "left" ? char.leftHand.position.clone() : char.rightHand.position.clone();
  return char.group.localToWorld(local);
}
function getWorldHand(char,side){
  return getHandWorldPosition(char,side);
}
function attachBallToHand(char,handName,offset=new THREE.Vector3()){
  const p = getHandWorldPosition(char,handName);
  ball.position.copy(p.add(offset));
}
function attachBallBetweenHands(char,offset=new THREE.Vector3()){
  const l = getHandWorldPosition(char,"left");
  const r = getHandWorldPosition(char,"right");
  ball.position.copy(l.lerp(r,.5).add(offset));
}

function isSprinting(char){
  const state=inputManager.getState(char===p1?"p1":"p2");
  return !!state.sprint&&Math.hypot(state.moveX,state.moveY)>.18;
}

function updateMomentum(char,made,points,contestLabel){
  if(made){
    char.hotStreak = Math.min(4,char.hotStreak + 1);
    char.confidence = Math.min(1,char.confidence + (.05 + (contestLabel === "Heavy Contest" || contestLabel === "Smothered" ? .06 : 0)));
  }else{
    char.hotStreak = Math.max(-4,char.hotStreak - 1);
    char.confidence = Math.max(-1,char.confidence - .05);
  }
  if(char.hotStreak >= 3) showMoveFeedback("Heating up");
  if(char.hotStreak <= -2) showMoveFeedback("Cold");
  if(char.confidence > .5) showMoveFeedback("Locked in");
  addTakeover(made ? (points===3?15:11) : -6,char);
  if(made) crowdWave = Math.min(1,crowdWave + .45);
}

function addBlockStat(char,takeoverAmount=10){
  if(!char) return;
  char.stats.blocks += 1;
  addTakeover(takeoverAmount,char);
}

function addTakeover(amount,char){
  const mult = getGameplayTuning().takeoverRate;
  char.takeover = THREE.MathUtils.clamp(char.takeover + amount*mult,0,100);
  if(char.takeover >= 100 && !char.takeoverActive) flash(`${char.id.toUpperCase()} TAKEOVER READY`);
}
function activateTakeover(char){
  if(char.takeover < 100 || char.takeoverActive) return;
  char.takeoverActive = true;
  char.takeoverTimer = 12;
  char.takeover = 100;
  applyCameraShake(.15);
  updateLEDBoards("HE'S HEATING UP");
}
function updateTakeover(dt){
  [p1,p2].forEach(c=>{
    if(!c) return;
    if(c.takeoverActive){
      c.takeoverTimer -= dt;
      c.takeover = THREE.MathUtils.clamp(c.takeover - dt*9,0,100);
      if(c.takeoverTimer <= 0 || c.takeover <= 0){ c.takeoverActive = false; c.takeover = Math.max(0,c.takeover); }
    }
  });
}
function drawTakeoverUI(){}

function applyCameraShake(amount,type="generic"){
  const scale = type === "rim" ? 0.45 : type === "block" ? 0.72 : type === "dunk" ? 1.15 : 1;
  const boosted = amount*scale*(gameMode.arcade?1.3:1);
  cameraEffects.impulse = Math.min(1.4,cameraEffects.impulse + boosted*2.4);
}
function applyCameraZoomPulse(amount){ cameraEffects.zoomPulse = Math.min(.36,cameraEffects.zoomPulse + amount); }
function updateCameraEffects(dt){
  const k = 10;
  cameraEffects.shakeVel += (cameraEffects.impulse - cameraEffects.shake)*k*dt;
  cameraEffects.shakeVel *= Math.max(0,1-dt*5.5);
  cameraEffects.shake += cameraEffects.shakeVel*dt;
  cameraEffects.impulse = Math.max(0,cameraEffects.impulse - dt*2.8);
  cameraEffects.shake = Math.max(0,Math.min(.5,cameraEffects.shake));
  cameraEffects.zoomPulse = Math.max(0,cameraEffects.zoomPulse - dt*.9);
  if(activeSlowMo.timer > 0){
    activeSlowMo.timer -= dt;
    const s = activeSlowMo.scale;
    clock.elapsedTime += dt*(s-1);
  }
}
function triggerSlowMo(duration,scale){ activeSlowMo = {timer:duration,scale}; }

function createParticlePool(){
  for(let i=0;i<40;i++){
    const p = new THREE.Mesh(new THREE.SphereGeometry(.05,6,6),new THREE.MeshBasicMaterial({color:0xffd56a,transparent:true,opacity:0}));
    scene.add(p); particlePool.push(p);
  }
}
function createConfettiPool(){
  for(let i=0;i<90;i++){
    const c = new THREE.Mesh(new THREE.PlaneGeometry(.08,.14),new THREE.MeshBasicMaterial({color:0xffd56a,side:THREE.DoubleSide,transparent:true,opacity:0}));
    c.visible = false;
    scene.add(c);
    confettiPool.push(c);
  }
}
function spawnConfetti(){
  const burst = graphicsQuality === "low" ? 26 : 70;
  for(let i=0;i<burst;i++){
    const p = confettiPool.pop();
    if(!p) break;
    p.visible = true;
    p.material.opacity = .9;
    p.material.color.setHex([0xffd56a,0x65f0ff,0xff4d7a,0x8eff5e,0xffffff][Math.floor(Math.random()*5)]);
    p.position.set((Math.random()-.5)*8,4.8+Math.random()*2.5,-1 + (Math.random()-.5)*6);
    p.userData.vel = new THREE.Vector3((Math.random()-.5)*1.4,-(1.6+Math.random()*2.2),(Math.random()-.5)*1.2);
    p.userData.spin = (Math.random()-.5)*8;
    p.userData.life = 3 + Math.random()*2;
    confettiActive.push(p);
  }
}
function updateConfetti(dt){
  for(let i=confettiActive.length-1;i>=0;i--){
    const p = confettiActive[i];
    p.userData.life -= dt;
    p.position.addScaledVector(p.userData.vel,dt);
    p.rotation.z += p.userData.spin * dt;
    if(p.userData.life <= 0 || p.position.y < -0.2){
      p.visible = false;
      p.material.opacity = 0;
      confettiActive.splice(i,1);
      confettiPool.push(p);
    }
  }
}
function spawnParticle(pos,color=0xffd56a){
  if(!qualitySettings[graphicsQuality].particles) return;
  const p = particlePool.pop(); if(!p) return;
  p.position.copy(pos); p.material.color.setHex(color); p.material.opacity=.75;
  p.userData.vel = new THREE.Vector3((Math.random()-.5)*1.2,Math.random()*1.8,(Math.random()-.5)*1.2);
  p.userData.life = .45;
  particlesActive.push(p);
}
function updateParticles(dt){
  for(let i=particlesActive.length-1;i>=0;i--){
    const p = particlesActive[i];
    p.userData.life -= dt;
    p.position.addScaledVector(p.userData.vel,dt);
    p.material.opacity = Math.max(0,p.userData.life/.45);
    if(p.userData.life<=0){ p.material.opacity=0; particlesActive.splice(i,1); particlePool.push(p); }
  }
}
function setGraphicsQuality(level){
  graphicsQuality = level in qualitySettings ? level : "medium";
  if(!renderer) return;
  renderer.shadowMap.enabled = graphicsQuality !== "low";
  renderer.shadowMap.type = graphicsQuality === "high" || graphicsQuality === "ultra" ? THREE.PCFSoftShadowMap : THREE.PCFShadowMap;
  crowdMembers.forEach(member=>{ member.visible=qualitySettings[graphicsQuality].crowd; });
  renderer.domElement.style.imageRendering=graphicsQuality==="low"?"auto":"";
  [p1,p2,ball,rim,backboard].forEach(obj=>{
    if(obj && obj.group){
      obj.group.traverse(n=>{ if(n.isMesh){ n.castShadow = graphicsQuality !== "low"; n.receiveShadow = graphicsQuality !== "low"; }});
    }else if(obj && obj.isMesh){
      obj.castShadow = graphicsQuality !== "low";
      obj.receiveShadow = graphicsQuality === "high" || graphicsQuality === "ultra";
    }
  });
}

function calculateReboundVector(){ return new THREE.Vector3((Math.random()-.5)*4,2.2,Math.random()*3.8); }
function updateReboundAI(dt){
  if(!gameStarted || twoPlayerMode) return;
  if(ballState !== "loose" && ballState !== "shot") return;
  updatePredictedReboundPoint();
  const targetBase = predictedReboundPoint ? predictedReboundPoint.clone() : ball.position.clone();
  const opponent = p1;
  const iq = p2.profile.shot || 1;
  const name = (p2.profile.name || "").toLowerCase();
  const longBoard = name.includes("flash guard") || name.includes("deep range");
  const paintBoard = name.includes("glass tower") || name.includes("paint bruiser");
  if(paintBoard) targetBase.lerp(HOOP_POS,0.28);
  if(longBoard && ballVelocity.length() > 3.5) targetBase.add(ballVelocity.clone().setY(0).multiplyScalar(0.26));
  const toSpot = targetBase.clone().sub(opponent.group.position).setY(0);
  const toOpp = p2.group.position.clone().sub(opponent.group.position).setY(0);
  const between = toSpot.lengthSq() > 0.01 && toOpp.lengthSq() > 0.01 && toSpot.normalize().dot(toOpp.normalize()) > 0.65;
  if(iq > 0.9 && between && flatDistance(p2.group.position,targetBase) < 2.3){
    const boxPos = opponent.group.position.clone().add(toSpot.clone().multiplyScalar(0.65));
    moveBotToward(p2,boxPos,difficultySettings[selectedDifficulty].speed*0.95,dt);
    p2.group.rotation.z = THREE.MathUtils.lerp(p2.group.rotation.z,0.06,dt*8);
    p2.leftArm.rotation.z = THREE.MathUtils.lerp(p2.leftArm.rotation.z,-0.22,dt*10);
    p2.rightArm.rotation.z = THREE.MathUtils.lerp(p2.rightArm.rotation.z,0.22,dt*10);
    if(boxOutFeedbackTimer <= 0){ flash("BOX OUT"); boxOutFeedbackTimer = 0.8; }
  }else{
    moveBotToward(p2,targetBase,difficultySettings[selectedDifficulty].speed*(longBoard ? 1.14 : 1.03),dt);
  }
}
function handleRimRattle(){ ballVelocity.multiplyScalar(.92); ballVelocity.y += .25; }
function handleBankShot(){ ballVelocity.z *= -.7; }

function flash(text){
  if(tutorialMode){
    document.getElementById("msg").textContent = text;
    messageTimer = 1.55;
  }else if(ledBoardCtx){
    updateLEDBoards(String(text).slice(0,28).toUpperCase());
  }
}

let audioCtx;
function tone(freq=240,duration=.06,type="sine",gain=.03){
  if(!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
  const o = audioCtx.createOscillator();
  const g = audioCtx.createGain();
  o.type = type; o.frequency.value = freq; g.gain.value = gain;
  o.connect(g).connect(audioCtx.destination);
  o.start();
  o.stop(audioCtx.currentTime + duration);
}
function playDribbleSound(){ tone(132,.035,"triangle",.012); }
function playShoeSqueak(){ tone(840,.025,"sawtooth",.008); }
function playShotSound(){ tone(520,.05,"triangle",.02); }
function playRimSound(){ tone(300,.06,"square",.015); }
function playSwishSound(){ tone(980,.04,"sine",.01); }
function playBlockSound(){ tone(180,.08,"square",.024); }
function playCrowdPulse(){ tone(110,.12,"sawtooth",.015); }
function playBuzzer(){ tone(98,.3,"square",.02); }

function keepInCourt(pos){
  pos.x = THREE.MathUtils.clamp(pos.x,-COURT_W/2+.8,COURT_W/2-.8);
  pos.z = THREE.MathUtils.clamp(pos.z,-COURT_L/2+.8,COURT_L/2-.8);
}

function getForward(angle){
  return new THREE.Vector3(Math.sin(angle),0,Math.cos(angle)).normalize();
}

function angleToward(from,to){
  const dx = to.x - from.x;
  const dz = to.z - from.z;
  return Math.atan2(dx,dz);
}

function flatDistance(a,b){
  const dx = a.x - b.x;
  const dz = a.z - b.z;
  return Math.sqrt(dx*dx + dz*dz);
}

function lerpAngle(a,b,t){
  let diff = ((b-a+Math.PI)%(Math.PI*2))-Math.PI;
  return a + diff * THREE.MathUtils.clamp(t,0,1);
}

function smoothstep(x){
  x = THREE.MathUtils.clamp(x,0,1);
  return x*x*(3-2*x);
}

function onResize(){
  renderer.setSize(window.innerWidth,window.innerHeight);
  adaptiveQuality.applyPixelRatio();
}
