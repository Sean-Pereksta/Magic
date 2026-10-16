let tutorialMode = false;
let gamePaused = false;

class TutorialCoach {
  constructor(){
    this.active = false;
    this.index = 0;
    this.moveTime = 0;
    this.baseline = {};
    this.steps = [
      {title:"Movement",body:"Use movement to circle the court. Up always moves toward the basket; left and right stay consistent.",check:(dt,state)=>{ this.moveTime += Math.hypot(state.moveX,state.moveY)>.45 ? dt : 0; return this.moveTime>1.2; }},
      {title:"Sprint",body:"Push the touch joystick to its outer edge, or hold Sprint on keyboard or controller, to accelerate into open space.",check:(dt,state)=>state.sprint&&Math.hypot(state.moveX,state.moveY)>.4},
      {title:"Shot timing",body:"Hold Shoot, then release near the center of the compact meter.",check:()=>p1?.stats.fga>(this.baseline.fga||0)},
      {title:"Finishing",body:"Drive toward the rim and use Shoot to trigger a layup or dunk.",check:()=>scoreP1>(this.baseline.score||0)},
      {title:"Create space",body:"Cut left and right to beat your defender. Keyboard and controller skill inputs also offer crossovers.",check:(dt,state)=>{ this.moveTime += Math.abs(state.moveX)>.5 ? dt : 0; return crossover.active||this.moveTime>.8; }},
      {title:"Defensive position",body:"Stay between your opponent and the basket. Movement automatically keeps you facing the action.",check:(dt,state)=>possession!==p1?.id&&Math.hypot(state.moveX,state.moveY)>.15},
      {title:"Steal",body:"Reach only when the ball is exposed. A bad reach costs recovery time.",check:()=>p1?.stats.steals>(this.baseline.steals||0)||p1?.swipeTime>0},
      {title:"Contest",body:"Use Block / Contest to challenge the release with your hands.",check:()=>p1?.blockTime>0||p1?.handsUpTime>0},
      {title:"Team basketball",body:"In 2v2 and 3v3, face a teammate and press Pass: C, controller A / Cross, or the touch Pass button. V or RB / R1 switches teammates.",check:(dt)=>{ this.moveTime+=dt; return this.moveTime>3; }},
      {title:"Ready",body:"Tutorial complete. The court is yours.",check:()=>false,complete:true}
    ];
  }

  start(){
    this.active = true;
    this.index = 0;
    this.moveTime = 0;
    this.baseline = {fga:p1?.stats.fga||0,score:scoreP1,steals:p1?.stats.steals||0};
    this.render();
  }

  update(dt){
    if(!this.active || !tutorialMode) return;
    const state = inputManager.getState("p1");
    const step = this.steps[this.index];
    if(step && !step.complete && step.check(dt,state)){
      this.index = Math.min(this.steps.length-1,this.index+1);
      this.baseline = {fga:p1?.stats.fga||0,score:scoreP1,steals:p1?.stats.steals||0};
      this.moveTime = 0;
      this.render();
    }
  }

  render(){
    const overlay = document.getElementById("tutorialOverlay");
    const step = this.steps[this.index];
    if(!overlay || !step) return;
    overlay.classList.toggle("visible",this.active);
    overlay.innerHTML = `<div class="tutorial-step-count">${this.index+1} / ${this.steps.length}</div><strong>${step.title}</strong><span>${step.body}</span>${step.complete?'<button id="tutorialFinishBtn" class="modeBtn selected">Finish Tutorial</button>':''}`;
    document.getElementById("tutorialFinishBtn")?.addEventListener("click",()=>{
      this.stop();
      gameStarted = false;
      gameOver = false;
      showPrimaryMenu("Tutorial complete.");
    });
  }

  stop(){
    this.active = false;
    document.getElementById("tutorialOverlay")?.classList.remove("visible");
  }
}

const tutorialCoach = new TutorialCoach();

function startTutorial(){
  tutorialMode = true;
  isTournamentGame = false;
  isChampionshipGame = false;
  twoPlayerMode = false;
  teamSize = 1;
  localTeamMode = "competitive";
  inputManager.clearAll();
  selectedDifficulty = "rookie";
  selectedCourt = "classicArena";
  gameSettings.set("gameplayPreset","simulation");
  startGame();
  tutorialCoach.start();
}

function setPaused(paused){
  if(!gameStarted || gameOver) return;
  gamePaused = !!paused;
  if(gamePaused) inputManager.clearAll();
  possessionFrozen = gamePaused || countdownTimer>0;
  const overlay = document.getElementById("pauseOverlay");
  overlay?.classList.toggle("visible",gamePaused);
  if(gamePaused && isTournamentGame) saveTournament(false);
}

function togglePause(){ setPaused(!gamePaused); }

function showControlsScreen(returnTo="primaryMenuScreen"){
  const screen = document.getElementById("controlsScreen");
  if(!screen) return;
  screen.dataset.returnTo = returnTo;
  showMenuScreen("controlsScreen");
  updateInputDeviceUI();
}

function updateInputDeviceUI(){
  document.querySelectorAll("[data-device-panel]").forEach(panel=>{
    const active = panel.dataset.devicePanel === inputManager.lastDevice ||
      (inputManager.lastDevice === "gamepad" && panel.dataset.devicePanel === "xbox") ||
      (inputManager.lastDevice === "playstation" && panel.dataset.devicePanel === "playstation");
    panel.classList.toggle("detected",active);
  });
  const pads = inputManager.getConnectedGamepads();
  const status = document.getElementById("controllerStatus");
  if(status){
    if(!pads.length) status.textContent = "No controller connected";
    else status.textContent = pads.map((pad,index)=>`Controller ${index+1}: ${pad.id.replace(/\s*\([^)]*\)\s*/g," ").trim()}`).join(" · ");
  }
  const assignment = document.getElementById("deviceAssignmentStatus");
  if(assignment){
    if(pads.length>=2) assignment.textContent = "Player 1: Controller 1 · Player 2: Controller 2";
    else if(pads.length===1&&twoPlayerMode) assignment.textContent = "Player 1: Keyboard · Player 2: Controller 1";
    else if(pads.length===1) assignment.textContent = "Player 1: Controller 1 or Keyboard";
    else assignment.textContent = "Player 1: WASD · Player 2: IJKL";
  }
}

function updateTouchPresentation(){
  const touchCapable = matchMedia("(pointer: coarse)").matches || inputManager.lastDevice==="touch";
  document.body.classList.toggle("touch-capable",touchCapable);
  document.body.classList.toggle("touch-controls-active",touchCapable&&gameStarted&&!gameOver&&!gamePaused);
  const controlled = typeof getHumanPlayer === "function" ? getHumanPlayer("p1") : p1;
  const possessionPlayer = typeof getPlayerById === "function" ? getPlayerById(possession) : (possession==="p1" ? p1 : p2);
  const owner = getOwner() || possessionPlayer;
  const offense = !!controlled && !!owner && (controlled.teamId !== undefined ? controlled.teamId===owner.teamId : controlled.id===possession);
  const primary = document.getElementById("touchPrimary");
  const secondary = document.getElementById("touchSecondary");
  if(primary){
    primary.dataset.touchAction = offense ? "shoot" : "block";
    primary.textContent = offense ? "SHOOT" : "BLOCK";
    primary.setAttribute("aria-label",offense ? "Hold and release to shoot" : "Contest or block");
  }
  if(secondary){
    secondary.dataset.touchAction = offense ? "pass" : "steal";
    secondary.textContent = offense ? "PASS" : "STEAL";
    secondary.setAttribute("aria-label",offense ? "Pass toward the teammate you face" : "Attempt a steal");
  }
}

function applySettingsControls(){
  const controlStyle = document.getElementById("controlStyleSelect");
  const preset = document.getElementById("gameplayPresetSelect");
  const shotMeterSetting = document.getElementById("shotMeterSetting");
  const scale = document.getElementById("mobileScaleSetting");
  const opacity = document.getElementById("mobileOpacitySetting");
  const layout = document.getElementById("mobileLayoutSetting");
  if(controlStyle){ controlStyle.value=gameSettings.get("controlStyle"); controlStyle.onchange=()=>gameSettings.set("controlStyle",controlStyle.value); }
  if(preset){ preset.value=gameSettings.get("gameplayPreset"); preset.onchange=()=>gameSettings.set("gameplayPreset",preset.value); }
  if(shotMeterSetting){ shotMeterSetting.checked=!!gameSettings.get("shotMeter"); shotMeterSetting.onchange=()=>gameSettings.set("shotMeter",shotMeterSetting.checked); }
  if(scale){ scale.value=gameSettings.get("mobileScale"); scale.oninput=()=>gameSettings.set("mobileScale",Number(scale.value)); }
  if(opacity){ opacity.value=gameSettings.get("mobileOpacity"); opacity.oninput=()=>gameSettings.set("mobileOpacity",Number(opacity.value)); }
  if(layout){ layout.value=gameSettings.get("mobileLayout"); layout.onchange=()=>gameSettings.set("mobileLayout",layout.value); }
}

function setupControlsMenu(){
  document.getElementById("primaryControlsBtn")?.addEventListener("click",()=>showControlsScreen("primaryMenuScreen"));
  document.getElementById("primaryTutorialBtn")?.addEventListener("click",startTutorial);
  document.getElementById("controlsBackBtn")?.addEventListener("click",event=>{
    const target = event.currentTarget.closest(".menuScreen")?.dataset.returnTo || "primaryMenuScreen";
    if(target==="__pause__"){
      document.getElementById("menu").style.display="none";
      setPaused(true);
    }else showMenuScreen(target);
  });
  document.querySelectorAll("[data-control-tab]").forEach(button=>{
    button.addEventListener("click",()=>{
      document.querySelectorAll("[data-control-tab]").forEach(item=>item.classList.toggle("selected",item===button));
      document.querySelectorAll(".control-layout").forEach(layout=>layout.classList.toggle("active",layout.id===`controls-${button.dataset.controlTab}`));
    });
  });
  window.addEventListener("basketball-input-device",updateInputDeviceUI);
  window.addEventListener("basketball-gamepads-changed",updateInputDeviceUI);
  document.getElementById("pauseResumeBtn")?.addEventListener("click",()=>setPaused(false));
  document.getElementById("pauseControlsBtn")?.addEventListener("click",()=>{
    showControlsScreen("__pause__");
  });
  document.getElementById("pauseMenuBtn")?.addEventListener("click",()=>{
    setPaused(false);
    gameStarted = false;
    tutorialMode = false;
    tutorialCoach.stop();
    inputManager.clearAll();
    showPrimaryMenu();
  });
  applySettingsControls();
  updateInputDeviceUI();
}

function bootstrapIntegrations(){
  setupControlsMenu();
  inputManager.attachTouchControls();
  gameSettings.apply();
  adaptiveQuality.applyRequestedLevel();
  updateTouchPresentation();
}
