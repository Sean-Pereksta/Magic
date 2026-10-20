// Camera position and focus translate together: this offset never depends on
// facing, possession, velocity, or a highlight effect.
const COURT_CAMERA_OFFSET = new THREE.Vector3(0,9,12.6);
const courtCameraFocus = new THREE.Vector3();
const courtCameraCrowd = new THREE.Vector3();

function updateCameras(dt){
  if(!p1){
    courtCameraFocus.set(0,1.2,-3);
    camMain.position.copy(courtCameraFocus).add(COURT_CAMERA_OFFSET);
    camMain.lookAt(courtCameraFocus);
    return;
  }

  if(twoPlayerMode && gameStarted){
    updateFollowCamera(camP1,p1,dt,"p1",true);
    updateFollowCamera(camP2,p2,dt,"p2",true);
  }else updateFollowCamera(camMain,p1,dt,"main",false);
  screenShake = 0;
}

function updateFollowCamera(cam,char,dt,cameraKey="main",isSplit=false){
  if(!char || !cam) return;
  const cState = cameraState[cameraKey] || cameraState.main;
  const playerPos = char.group.position;
  // Retain the basket as an anchor while following the controlled player and
  // ball. Nearby players gently influence framing without pulling the view.
  courtCameraFocus.copy(playerPos);
  if(ball) courtCameraFocus.lerp(ball.position,.28);
  courtCameraFocus.lerp(HOOP_POS,.26);
  courtCameraCrowd.set(0,0,0);
  let nearby = 0;
  const roster = typeof matchPlayers !== "undefined" && matchPlayers.length ? matchPlayers : [p1,p2];
  for(const player of roster){
    if(!player || player === char) continue;
    const pos = player.group.position;
    const dx = pos.x-playerPos.x, dz = pos.z-playerPos.z;
    if(dx*dx+dz*dz > 49) continue;
    courtCameraCrowd.add(pos);
    nearby++;
  }
  if(nearby) courtCameraFocus.lerp(courtCameraCrowd.multiplyScalar(1/nearby),.1);
  courtCameraFocus.x = THREE.MathUtils.clamp(courtCameraFocus.x,-5.4,5.4);
  courtCameraFocus.z = THREE.MathUtils.clamp(courtCameraFocus.z,Math.max(-8.8,playerPos.z-8.4),4.2);
  courtCameraFocus.y = 1.2 + (ball ? THREE.MathUtils.clamp(ball.position.y-2,0,5)*.08 : 0);
  cState.look.lerp(courtCameraFocus,1-Math.exp(-3.8*Math.min(dt,.05)));
  cam.position.copy(cState.look).add(COURT_CAMERA_OFFSET);
  cam.lookAt(cState.look);
}

function renderScene(){
  const w = window.innerWidth;
  const h = window.innerHeight;

  renderer.setScissorTest(false);
  renderer.clear();
  // Both viewports share the same scene; refresh its shadows only on the first render.
  if(renderer.shadowMap?.enabled) renderer.shadowMap.needsUpdate=true;

  if(twoPlayerMode && gameStarted){
    renderer.setScissorTest(true);
    const horizontal = typeof splitLayout !== "undefined" && splitLayout === "horizontal";
    const viewW = horizontal ? w : Math.floor(w/2);
    const viewH = horizontal ? Math.floor(h/2) : h;
    // WebGL's origin is bottom-left, so player 1 occupies the upper view.
    renderCourtViewport(camP1,0,horizontal ? h-viewH : 0,viewW,viewH);
    renderCourtViewport(camP2,horizontal ? 0 : viewW,0,horizontal ? w : w-viewW,horizontal ? h-viewH : h);
    renderer.setScissorTest(false);
  }else{
    renderCourtViewport(camMain,0,0,w,h);
  }
}

function renderCourtViewport(cam,x,y,width,height){
  renderer.setViewport(x,y,width,height);
  renderer.setScissor(x,y,width,height);
  const aspect = width/Math.max(1,height);
  if(cam.aspect !== aspect){ cam.aspect = aspect; cam.updateProjectionMatrix(); }
  renderer.render(scene,cam);
}

let hudScoreP1 = null, hudScoreP2 = null;
let hudSplitLayout = null;
const playerShotFeedback = {p1:{element:null,timer:null},p2:{element:null,timer:null}};

function resetPlayerShotFeedback(){
  for(const feedback of Object.values(playerShotFeedback)){
    if(feedback.timer) clearTimeout(feedback.timer);
    feedback.timer=null;
    if(feedback.element) feedback.element.style.display="none";
  }
  hudScoreP1 = hudScoreP2 = null;
  hudSplitLayout = null;
}

function showPlayerShotFeedback(shooterId,text,duration=1.5){
  const shooter = typeof getPlayerById === "function" ? getPlayerById(shooterId) : (shooterId==="p1"?p1:p2);
  if(!shooter) return;
  let slot = shooter.lastShotSlot || shooter.controllerSlot;
  if(!slot){
    if(p1?.teamId===shooter.teamId) slot="p1";
    else if(twoPlayerMode && p2?.teamId===shooter.teamId) slot="p2";
  }
  const feedback = playerShotFeedback[slot];
  if(!feedback) return;
  if(!feedback.element){
    feedback.element = document.createElement("div");
    feedback.element.id = `shotFeedback-${slot}`;
    feedback.element.className = "splitLabel player-shot-feedback";
    feedback.element.setAttribute("role","status");
    feedback.element.setAttribute("aria-live","polite");
    Object.assign(feedback.element.style,{
      bottom:"auto",transform:"translateX(-50%)",fontSize:"11px",
      padding:"7px 10px",textAlign:"center",pointerEvents:"none",
      color:"#fff",borderColor:shooter.teamId===0?"#65b9ff":"#ff8b7b"
    });
    document.body.appendChild(feedback.element);
  }
  const label = shooter.lastShotSlot || shooter.controllerSlot || (shooter.teamId===0?"BLUE":"CORAL");
  feedback.element.textContent = `${String(label).toUpperCase()} · ${text}`;
  feedback.element.style.display = uiHidden ? "none" : "block";
  positionShotFeedback(slot,feedback.element);
  if(feedback.timer) clearTimeout(feedback.timer);
  feedback.timer = setTimeout(()=>{
    feedback.element.style.display="none";
    feedback.timer=null;
  },Math.max(.4,duration)*1000);
}

function positionShotFeedback(slot,element){
  const split = twoPlayerMode && gameStarted;
  const horizontal = split && splitLayout==="horizontal";
  element.style.left = !split || horizontal ? "50%" : slot==="p1" ? "25%" : "75%";
  element.style.top = horizontal && slot==="p2" ? "calc(50% + 18px)" : "72px";
  element.style.maxWidth = split&&!horizontal ? "calc(50vw - 24px)" : "calc(100vw - 24px)";
}

function updateHUD(){
  const score = document.getElementById("scoreText");
  if(score && (hudScoreP1!==scoreP1 || hudScoreP2!==scoreP2)){
    score.innerHTML = `${scoreP1} <span>—</span> ${scoreP2}`;
    hudScoreP1 = scoreP1; hudScoreP2 = scoreP2;
  }
  const teams = typeof teamSize !== "undefined" && teamSize>1;
  setHudText("p1HudName",teams ? "BLUE TEAM" : p1?.profile.name || "PLAYER 1");
  const opponent = typeof matchPlayers !== "undefined" ? matchPlayers.find(player=>player.teamId===1) : p2;
  setHudText("p2HudName",teams ? "CORAL TEAM" : opponent?.profile.name || p2?.profile.name || "PLAYER 2");
  const showSplit = twoPlayerMode && gameStarted && !uiHidden;
  const layout = showSplit ? splitLayout : "none";
  const firstLabel = document.getElementById("p1Label");
  const secondLabel = document.getElementById("p2Label");
  if(layout!==hudSplitLayout){
    for(const label of [firstLabel,secondLabel]){
      if(!label) continue;
      label.style.display = showSplit ? "block" : "none";
      label.style.top = "auto";
      label.style.right = "auto";
      label.style.maxWidth = layout==="horizontal" ? "calc(100vw - 32px)" : "calc(50vw - 32px)";
      label.style.fontSize = "11px";
      label.style.pointerEvents = "none";
    }
    if(firstLabel){
      firstLabel.style.left = "12px";
      firstLabel.style.bottom = layout==="horizontal" ? "calc(50% + 12px)" : "12px";
    }
    if(secondLabel){
      secondLabel.style.left = layout==="horizontal" ? "12px" : "calc(50% + 12px)";
      secondLabel.style.bottom = "12px";
    }
    hudSplitLayout = layout;
    for(const slot of ["p1","p2"]){
      const feedback=playerShotFeedback[slot];
      if(!feedback.element) continue;
      positionShotFeedback(slot,feedback.element);
      feedback.element.style.display=feedback.timer && gameStarted && !uiHidden ? "block" : "none";
    }
  }
  if(showSplit){
    if(p1) setHudText("p1Label",`P1 · ${p1.teamId===0?"BLUE":"CORAL"} · ${p1.profile.name}`);
    if(p2) setHudText("p2Label",`P2 · ${p2.teamId===0?"BLUE":"CORAL"} · ${p2.profile.name}`);
  }
  if(messageTimer<=0&&!tutorialMode) setHudText("msg","");
}

function setHudText(id,text){
  const element = document.getElementById(id);
  if(element && element.textContent!==text) element.textContent = text;
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
  return typeof getBallHandler === "function" ? getBallHandler() : (ballOwner === "p1" ? p1 : ballOwner === "p2" ? p2 : null);
}

function getHandWorldPosition(char,handName){
  if(typeof getModelHandWorld==="function"){
    const modelHand=getModelHandWorld(char,handName);
    if(modelHand) return modelHand;
  }
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
  if(!char) return false;
  const state=char.controllerSlot ? inputManager.getState(char.controllerSlot) : char.aiInput || (char===p1 ? inputManager.getState("p1") : createInputFrame());
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
  const roster = typeof matchPlayers !== "undefined" && matchPlayers.length ? matchPlayers : [p1,p2];
  roster.forEach(c=>{
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
  // Highlights use animation, sound, and court feedback without displacing
  // the fixed camera or changing the player's control frame.
  cameraEffects.impulse = 0;
}
function applyCameraZoomPulse(amount){ cameraEffects.zoomPulse = 0; }
function updateCameraEffects(dt){
  cameraEffects.shake = cameraEffects.shakeVel = cameraEffects.impulse = cameraEffects.zoomPulse = 0;
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
  setBasketballCrowdQuality();
  const shadowSize={low:512,medium:1024,high:1536,ultra:2048}[graphicsQuality];
  scene.traverse(light=>{
    if(!light.isLight || !light.shadow) return;
    light.castShadow=!!light.userData.basketballKeyLight && graphicsQuality!=="low";
    if(!light.userData.basketballKeyLight) return;
    if(light.shadow.mapSize.x!==shadowSize || light.shadow.mapSize.y!==shadowSize){
      if(light.shadow.map){light.shadow.map.dispose();light.shadow.map=null;}
      if(light.shadow.mapPass){light.shadow.mapPass.dispose();light.shadow.mapPass=null;}
      light.shadow.mapSize.set(shadowSize,shadowSize);
    }
    light.shadow.needsUpdate=true;
  });
  renderer.shadowMap.needsUpdate=true;
  renderer.domElement.style.imageRendering=graphicsQuality==="low"?"auto":"";
  const roster = typeof matchPlayers !== "undefined" && matchPlayers.length ? matchPlayers : [p1,p2];
  [...roster,ball,rim,backboard].forEach(obj=>{
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
  // Rebound assignments and movement belong to updateTeamAI for every mode.
}
// Ball response is resolved once by the physical rim/backboard solver.
function handleRimRattle(){}
function handleBankShot(){}

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
