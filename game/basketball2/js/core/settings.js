const SETTINGS_KEY = "basketball2SettingsV2";

const GAMEPLAY_PRESETS = {
  simulation: {
    label: "Simulation",
    acceleration: 0.9,
    deceleration: 1.12,
    lateralSpeed: 0.94,
    sprintSpeed: 0.94,
    reversePenalty: 0.48,
    contestStrength: 1.08,
    staminaDrain: 1.08,
    stealWindow: 0.92,
    ballBounce: 0.9,
    outOfBounds: true,
    takeoverRate: 0.82
  },
  arcade: {
    label: "Arcade",
    acceleration: 1.2,
    deceleration: 1.2,
    lateralSpeed: 1.1,
    sprintSpeed: 1.12,
    reversePenalty: 0.76,
    contestStrength: 0.9,
    staminaDrain: 0.72,
    stealWindow: 1.08,
    ballBounce: 1.08,
    outOfBounds: false,
    takeoverRate: 1.35
  }
};

class BasketballSettings {
  constructor(){
    this.defaults = {
      controlStyle: "modern",
      gameplayPreset: "simulation",
      quality: "auto",
      shotMeter: true,
      mobileScale: 1,
      mobileOpacity: 0.72,
      mobileLayout: "standard"
    };
    this.values = {...this.defaults};
    this.load();
  }

  load(){
    try{
      const saved = JSON.parse(localStorage.getItem(SETTINGS_KEY) || "{}");
      this.values = {...this.defaults,...saved};
    }catch(error){
      this.values = {...this.defaults};
    }
  }

  save(){
    localStorage.setItem(SETTINGS_KEY,JSON.stringify(this.values));
  }

  get(key){ return this.values[key]; }

  set(key,value){
    if(!(key in this.defaults)) return;
    this.values[key] = value;
    this.save();
    this.apply();
  }

  apply(){
    const preset = this.values.gameplayPreset === "arcade" ? "arcade" : "simulation";
    gameMode.arcade = preset === "arcade";
    document.documentElement.style.setProperty("--touch-scale",String(this.values.mobileScale));
    document.documentElement.style.setProperty("--touch-opacity",String(this.values.mobileOpacity));
    document.body?.classList.toggle("touch-layout-mirrored",this.values.mobileLayout === "mirrored");
    if(shotMeter) shotMeter.group.visible = !!this.values.shotMeter && shotCharging;
  }
}

class AdaptiveQualityManager {
  constructor(){
    this.samples = [];
    this.elapsed = 0;
    this.cooldown = 0;
    this.actualLevel = "medium";
  }

  chooseInitialLevel(){
    const memory = Number(navigator.deviceMemory || 4);
    const cores = Number(navigator.hardwareConcurrency || 4);
    const mobile = matchMedia("(pointer: coarse)").matches;
    const maxDimension = Math.max(screen.width || 0,screen.height || 0);
    if(mobile && (memory <= 4 || cores <= 4)) return "low";
    if(mobile || memory <= 6 || cores <= 6 || maxDimension < 1500) return "medium";
    if(memory >= 12 && cores >= 10) return "ultra";
    return "high";
  }

  applyRequestedLevel(){
    const requested = gameSettings.get("quality");
    this.actualLevel = requested === "auto" ? this.chooseInitialLevel() : requested;
    setGraphicsQuality(this.actualLevel);
    this.applyPixelRatio();
    this.updateLabel();
  }

  applyPixelRatio(){
    if(!renderer) return;
    const caps = {low: 1,medium: 1.35,high: 1.75,ultra: 2};
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1,caps[this.actualLevel] || 1.35));
  }

  sample(dt){
    if(gameSettings.get("quality") !== "auto" || !gameStarted) return;
    this.cooldown = Math.max(0,this.cooldown-dt);
    this.elapsed += dt;
    this.samples.push(dt);
    if(this.elapsed < 4 || this.cooldown > 0) return;
    const average = this.samples.reduce((sum,value)=>sum+value,0) / Math.max(1,this.samples.length);
    const fps = 1 / Math.max(average,0.001);
    this.samples.length = 0;
    this.elapsed = 0;
    const levels = ["low","medium","high","ultra"];
    let index = levels.indexOf(this.actualLevel);
    if(fps < 48 && index > 0) index -= 1;
    else if(fps > 58 && index < levels.length-1) index += 1;
    else return;
    this.actualLevel = levels[index];
    setGraphicsQuality(this.actualLevel);
    this.applyPixelRatio();
    this.updateLabel();
    this.cooldown = 10;
  }

  updateLabel(){
    const label = document.getElementById("qualityStatus");
    if(label) label.textContent = gameSettings.get("quality") === "auto" ? `AUTO · ${this.actualLevel.toUpperCase()}` : this.actualLevel.toUpperCase();
  }
}

const gameSettings = new BasketballSettings();
const adaptiveQuality = new AdaptiveQualityManager();

function getGameplayTuning(){
  return GAMEPLAY_PRESETS[gameSettings.get("gameplayPreset")] || GAMEPLAY_PRESETS.simulation;
}
