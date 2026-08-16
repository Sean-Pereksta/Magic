let scene, renderer, clock;
let camMain, camP1, camP2;
let p1, p2, ball, ballShadow, rim, backboard;
let netLines = [];
let scoreboardPlane, scoreboardTexture, scoreboardCtx;
let arcLine;
let arenaWalls = [];
let arenaThemeRefs = {court:null,lineMats:[],trimMats:[],logoCanvases:[],crowdMats:[]};
let ledBoardMesh, ledBoardTexture, ledBoardCtx;
let crowdMembers = [];
let crowdWave = 0;
let arenaExcitement = 0;
let arenaLightStrips = [];
let arenaMoodLight = null;
let shotTrailPool = [];
let shotTrailActive = [];
let particlePool = [];
let particlesActive = [];

let keys = {};
let gameStarted = false;
let twoPlayerMode = false;
let uiHidden = false;
let isTournamentGame = false;
let tournamentState = null;
let currentTournamentEnemy = null;
let isChampionshipGame = false;
const TOURNAMENT_SAVE_KEY = "basketballNationalTourV2";
const LEGACY_TOURNAMENT_SAVE_KEY = "basketballTournamentSave";
const TOURNAMENT_PLAYER_ID = "player";
const tournamentDifficultyStrength = {rookie:1,easy:2,normal:3,hard:4,elite:5,nightmare:6,legend:7,pro:8,superstar:9,hallOfFame:10,impossible:11};
let confettiPool = [];
let confettiActive = [];

let selectedP1 = "sky";
let selectedP2 = "claw";
let selectedP1Sneaker = "classic";
let selectedP2Sneaker = "classic";
let selectedP1Jersey = "classicBlue";
let selectedP2Jersey = "firestorm";
let selectedCourt = "classicArena";
let selectedDifficulty = "normal";

let scoreP1 = 0;
let scoreP2 = 0;
let targetScore = 11;
let winBy = 2;
let gameOver = false;

let possession = "p1";
let ballOwner = "p1";
let lastShotShooter = "p1";
let ballState = "dribble"; // dribble, shot, loose, layup, dunk
let lastShotValue = 2;
let lastShotReleasePos = new THREE.Vector3();
let lastShotWasHook = false;
let ballSide = "right";
let ballSideValue = 1;
let ballVelocity = new THREE.Vector3();
let ballSpin = new THREE.Vector3();
let lastBallVisualPos = new THREE.Vector3();

let shotCharging = false;
let chargingPlayer = null;
let shotPower = 0;
let chargeDir = 1;
let hasShotThisPossession = false;

let layupActive = false;
let layupOwner = null;
let layupTimer = 0;
let layupDuration = 0.68;
let layupScored = false;
let layupBlocked = false;
let layupBlockWindow = 0;
let loosePickupDelay = 0;
let ballCollisionCooldown = 0;
let outOfBoundsCooldown = 0;
let rimCollisionCooldown = 0;
let backboardCollisionCooldown = 0;
const previousBallPosition = new THREE.Vector3();
let netWobbleTimer = 0;
let rimWobbleTimer = 0;
let rimBasePosition = null;
let netWobbleStrength = 0;
let rimWobbleStrength = 0;
let rimTouchCount = 0;
let backboardImpactTimer = 0;
let backboardImpactStrength = 0;
const backboardBasePosition = new THREE.Vector3();
let netContactPoint = new THREE.Vector3();
let netImpactDirection = new THREE.Vector3(0,0,1);
let netVelocity = new THREE.Vector3();
let shotHasHitRimOrBoard = false;
let predictedReboundPoint = null;
let reboundPredictionTimer = 0;
let scrambleState = {active:false,timer:0,lastText:0};
let boxOutFeedbackTimer = 0;
let tipFeedbackTimer = 0;
let debugDirections = false;
let debugDirHelpers = null;
let shotMeter = null;

let dunkActive = false;
let dunkOwner = null;
let dunkTimer = 0;
let dunkDuration = 0.86;
let dunkScored = false;
let rimHangActive = false;
let rimHangTimer = 0;
let rimHangDuration = 0.24;
let lastDunkReadyFlash = {p1:0,p2:0};

let foulMeter = 0;
let messageTimer = 0;
let lastScored = false;

let countdownTimer = 0;
const COUNTDOWN_DEFENDER_MIN_DISTANCE = 1.9;
let possessionFrozen = false;

let botStrategy = "drive";
let screenShake = 0;
const DOUBLE_TAP_WINDOW = 0.28;
const tapMemory = {
  p1:{q:0,e:0,s:0},
  p2:{u:0,o:0,k:0,numpad4:0,numpad6:0,numpad0:0}
};
let botBrain = null;
const moveFeedbackTimers = [];
let ledBoardTimer = 0;
let activeSlowMo = {timer:0, scale:1};
let cameraEffects = {shake:0, zoomPulse:0, shakeVel:0, impulse:0};
const cameraState = {
  main:{look:new THREE.Vector3(), zoom:0, lead:new THREE.Vector3(), lastOwner:null},
  p1:{look:new THREE.Vector3(), zoom:0, lead:new THREE.Vector3(), lastOwner:null},
  p2:{look:new THREE.Vector3(), zoom:0, lead:new THREE.Vector3(), lastOwner:null}
};
let crowdReactionState = "normal";
let crowdReactionTimer = 0;
let crowdReactionStrength = 0;
let courtThemePulse = 0;
const DEFENSIVE_SHADE_HOLD = 0.16;
let gameMode = {targetScore:11, winBy2:true, makeItTakeIt:false, losersBall:false, arcade:false};
const qualitySettings = {
  low:{crowd:false,particles:false,reflections:false,glow:false},
  medium:{crowd:true,particles:true,reflections:false,glow:true},
  high:{crowd:true,particles:true,reflections:true,glow:true},
  ultra:{crowd:true,particles:true,reflections:true,glow:true}
};
let graphicsQuality = "medium";

let crossover = {
  active:false,
  player:null,
  from:1,
  to:-1,
  progress:1,
  riskWindow:0,
  diagonal:0
};

const BALL_RADIUS = 0.23;
const GRAVITY = -14.5;
const COURT_W = 18;
const COURT_L = 26;
const HOOP_POS = new THREE.Vector3(0,3.28,-10.8);
const RIM_RADIUS = 0.62;
const THREE_RADIUS = 7.15;

const P1_OFFENSE_START = new THREE.Vector3(0,0,4.8);
const P2_DEFENSE_START = new THREE.Vector3(0,0,1.6);
const P2_OFFENSE_START = new THREE.Vector3(0,0,4.8);
const P1_DEFENSE_START = new THREE.Vector3(0,0,1.6);

const SPRINT_DRAIN = 0.62;
const SPRINT_RECOVER = 0.24;
const SPRINT_MIN = 0.12;

const profiles = {
  sky:{
    name:"Sky Riser",desc:"Tall finisher with a high release and elite rim finishing.",bodyType:"Tall",
    jersey:0x1d6cff,shorts:0x0b2b78,skin:0xf1c27d,hair:0x1e1209,
    height:1.16,width:1.02,speed:1.0,sprint:.98,jump:1.15,layup:1.34,handle:.68,shot:.86,block:1.48,steal:.72,hook:1.12,number:"32",hairStyle:"curlyTop"
  },
  flash:{
    name:"Flash Guard",desc:"Fastest guard with sharp change-of-pace dribbles, weaker through contact.",bodyType:"Small/Fast",
    jersey:0xff3d6e,shorts:0x74162e,skin:0x9b5a35,hair:0x111111,
    height:.9,width:.84,speed:1.2,sprint:1.3,jump:.9,layup:.82,handle:1.52,shot:.95,block:.56,steal:1.35,hook:.55,number:"3",hairStyle:"spiky"
  },
  smooth:{
    name:"Smooth Shooter",desc:"Largest green windows on jumpers and set shots, weaker at rim contact.",bodyType:"Shooter",
    jersey:0x29c77a,shorts:0x0d6037,skin:0xd49a6a,hair:0x2b160d,
    height:1.0,width:.92,speed:1.0,sprint:.98,jump:.88,layup:.9,handle:1.04,shot:1.55,block:.72,steal:.82,hook:.72,number:"11",hairStyle:"shortFade"
  },
  bruiser:{
    name:"Paint Bruiser",desc:"Strong contact finisher with powerful hooks and slower lateral movement.",bodyType:"Bruiser",
    jersey:0xffa42b,shorts:0x7a4200,skin:0x5d3425,hair:0x0b0705,
    height:1.1,width:1.24,speed:.96,sprint:.93,jump:.9,layup:1.48,handle:.54,shot:.72,block:1.12,steal:.62,hook:1.35,number:"55",hairStyle:"afro"
  },
  claw:{
    name:"The Claw",desc:"Best steal and block timing with long reach, lower shot consistency.",bodyType:"Defender",
    jersey:0x9d6cff,shorts:0x3b1f77,skin:0x704327,hair:0x080808,
    height:1.06,width:1.04,speed:.98,sprint:.98,jump:1.02,layup:.86,handle:.9,shot:.78,block:1.7,steal:1.55,hook:.84,number:"2",hairStyle:"braids"
  },
  rocket:{
    name:"Rocket Wing",desc:"Explosive first step slasher with chase-down block upside.",bodyType:"Wing",
    jersey:0xff4d1e,shorts:0x802111,skin:0xc8845b,hair:0x20100a,
    height:1.02,width:.98,speed:1.08,sprint:1.34,jump:1.2,layup:1.08,handle:.85,shot:.78,block:1.22,steal:.9,hook:.7,number:"8",hairStyle:"headband"
  },
  crafty:{
    name:"Crafty Lefty",desc:"Best hook setup and tricky angles with safe side-hop/cross moves.",bodyType:"Playmaker",
    jersey:0xffffff,shorts:0x222222,skin:0xe1aa7a,hair:0x332012,
    height:.96,width:.9,speed:1.03,sprint:.96,jump:.78,layup:1.02,handle:1.62,shot:1.08,block:.52,steal:1.05,hook:1.05,number:"7",hairStyle:"buzz"
  },
  tower:{
    name:"Glass Tower",desc:"Highest release and strongest blocks, very slow with low handle.",bodyType:"Tall",
    jersey:0x00b8d9,shorts:0x005a6a,skin:0x3d2418,hair:0x080808,
    height:1.24,width:1.16,speed:.91,sprint:.87,jump:.95,layup:1.22,handle:.38,shot:.58,block:2.05,steal:.48,hook:1.22,number:"44",hairStyle:"afro"
  },
  rimreaper:{
    name:"Rim Reaper",desc:"Elite vertical slasher with violent dunks, strong contact finishing, and chase-down block upside. Weak jumper and only average handle.",bodyType:"Dunker",
    jersey:0xff4d1f,shorts:0x1a1a1a,skin:0x8f5737,hair:0x14100d,
    height:1.12,width:1.08,speed:1.04,sprint:1.08,jump:1.42,layup:1.62,handle:.82,shot:.68,block:1.34,steal:.7,hook:.85,number:"21",hairStyle:"headband"
  },
  deeprange:{
    name:"Deep Range",desc:"Quick-release shooter with the biggest jumper green window, deep shot confidence, and good handle. Weak at the rim and poor shot blocker.",bodyType:"Sniper",
    jersey:0x5aa7ff,shorts:0x111111,skin:0xc6865a,hair:0x16100d,
    height:.94,width:.86,speed:1.04,sprint:1.0,jump:.78,layup:.74,handle:1.2,shot:1.78,block:.45,steal:.82,hook:.45,number:"30",hairStyle:"shortFade"
  }
};

const jerseys = {
  classicBlue:{name:"Classic Blue",primary:0x1d6cff,secondary:0xffffff,trim:0xffd56a,pattern:"wave",style:"Legacy"},
  firestorm:{name:"Firestorm",primary:0xff3d1f,secondary:0x111111,trim:0xffd56a,pattern:"flames",style:"Attack"},
  neonNight:{name:"Neon Night",primary:0x101820,secondary:0x65f0ff,trim:0xff3dff,pattern:"pulse",style:"Neon"},
  royalGold:{name:"Royal Gold",primary:0x4b2cff,secondary:0xffd56a,trim:0xffffff,pattern:"crown",style:"Royal"},
  streetCamo:{name:"Street Camo",primary:0x263524,secondary:0x8fa36c,trim:0xffffff,pattern:"camo",style:"Urban"},
  blackout:{name:"Blackout",primary:0x050505,secondary:0x222222,trim:0xff4444,pattern:"geo",style:"Stealth"},
  skyline:{name:"Skyline",primary:0xffffff,secondary:0x65d9ff,trim:0x1d6cff,pattern:"city",style:"Downtown"},
  galaxy:{name:"Galaxy",primary:0x151030,secondary:0x9d6cff,trim:0x65f0ff,pattern:"stars",style:"Cosmic"},
  lightningStrike:{name:"Lightning Strike",primary:0x15253f,secondary:0xffffff,trim:0xfff15c,pattern:"lightning",style:"Storm"},
  inferno:{name:"Inferno",primary:0x1a0b08,secondary:0xff6b2d,trim:0xffd56a,pattern:"flames",style:"Heat"},
  frostbite:{name:"Frostbite",primary:0xdff6ff,secondary:0x4fc3ff,trim:0xffffff,pattern:"ice",style:"Winter"},
  toxicWave:{name:"Toxic Wave",primary:0x101810,secondary:0x39ff14,trim:0xb6ff00,pattern:"slime",style:"Toxic"},
  stormPulse:{name:"Storm Pulse",primary:0x0f1625,secondary:0x65f0ff,trim:0xff3dff,pattern:"pulse",style:"Energy"},
  tigerFire:{name:"Tiger Fire",primary:0x1a1209,secondary:0xff8c1a,trim:0xffffff,pattern:"claw",style:"Wild"},
  chromeElite:{name:"Chrome Elite",primary:0xd8e4ff,secondary:0x5a6a7f,trim:0x111111,pattern:"chrome",style:"Tech"},
  cityRush:{name:"City Rush",primary:0x1f2430,secondary:0x5aa7ff,trim:0xffffff,pattern:"city",style:"Metro"},
  solarFlare:{name:"Solar Flare",primary:0x4a1e05,secondary:0xffae00,trim:0xfff1a8,pattern:"sunburst",style:"Solar"},
  venom:{name:"Venom",primary:0x050505,secondary:0x39ff14,trim:0xffffff,pattern:"fang",style:"Venom"},
  roseHeat:{name:"Rose Heat",primary:0x6f1133,secondary:0xff5f8f,trim:0xffffff,pattern:"petal",style:"Rose"},
  prism:{name:"Prism",primary:0x202040,secondary:0x9d6cff,trim:0x65f0ff,pattern:"prism",style:"Prismatic"},
  royalLegacy:{name:"Royal Legacy",primary:0x2f1662,secondary:0x1a1034,trim:0xffd56a,pattern:"royal_trim",style:"Royal"},
  skylineSlash:{name:"Skyline Slash",primary:0x141a26,secondary:0xd7e5ff,trim:0x5aa7ff,pattern:"slash_city",style:"City"},
  frostedSteel:{name:"Frosted Steel",primary:0xdbefff,secondary:0x8ebce8,trim:0xffffff,pattern:"frost",style:"Ice"},
  toxicNeon:{name:"Toxic Neon",primary:0x0a1309,secondary:0x39ff14,trim:0xb6ff00,pattern:"slime",style:"Neon"},
  chromeSlash:{name:"Chrome Slash",primary:0xc9d5e8,secondary:0x5f6f82,trim:0x101620,pattern:"slash",style:"Chrome"},
  darkClaw:{name:"Brown Beast Claw",primary:0x3a2316,secondary:0x1a130f,trim:0xf2e4c8,pattern:"claw",style:"Beast"},
  beastMode:{name:"Beast Mode",primary:0x0c0d10,secondary:0x2a1a10,trim:0xff5a1f,pattern:"beast",style:"Predator"},
  lightningBeast:{name:"Lightning Beast",primary:0x101f45,secondary:0xf2dd2e,trim:0xffffff,pattern:"lightning",style:"Storm Beast"},
  iceFang:{name:"Ice Fang",primary:0xeaf7ff,secondary:0x2a3b52,trim:0x69f7ff,pattern:"ice",style:"Fang"},
  venomFang:{name:"Venom Fang",primary:0x050505,secondary:0x18d64d,trim:0xe7f7df,pattern:"venom",style:"Toxic"}
};
const jerseyKeys = Object.keys(jerseys);

const sneakers = {
  classic:{name:"Classic Whites",primary:0xffffff,sole:0x111111,accent:0xdadada,type:"low",pattern:"chrome",style:"Baseline"},
  lightning:{name:"Lightning Lows",primary:0xf6f6f6,sole:0x111111,accent:0xffd84d,type:"low",pattern:"lightning",style:"Quick",sprintRecover:.02},
  lockdown:{name:"High-Top Lockdowns",primary:0x222833,sole:0x050505,accent:0x65d9ff,type:"high",pattern:"geo",style:"Defense",defense:.02},
  riser:{name:"Sky Risers",primary:0xffffff,sole:0x2f3b4f,accent:0x7ce7ff,type:"boost",pattern:"wave",style:"Lift",jump:.02},
  street:{name:"Street Reds",primary:0xd62828,sole:0x111111,accent:0xffffff,type:"low",pattern:"claw",style:"Street"},
  tower:{name:"Glass Towers",primary:0xeaf8ff,sole:0x1b344d,accent:0x67d6ff,type:"high",pattern:"ice",style:"Tower"},
  galaxyFoams:{name:"Galaxy Foams",primary:0x151030,sole:0x65f0ff,accent:0xff4dff,type:"glow",pattern:"stars",style:"Cosmic"},
  lavaBurst:{name:"Lava Bursts",primary:0x111111,sole:0xff3d1f,accent:0xffd56a,type:"high",pattern:"flames",style:"Heat"},
  iceGrip:{name:"Ice Grips",primary:0xeaf8ff,sole:0x4fc3ff,accent:0xffffff,type:"low",pattern:"ice",style:"Ice"},
  toxicSlime:{name:"Toxic Slimes",primary:0x101810,sole:0x39ff14,accent:0xb6ff00,type:"boost",pattern:"slime",style:"Toxic"},
  royalRush:{name:"Royal Rush",primary:0x3b1f77,sole:0xffd56a,accent:0xffffff,type:"high",pattern:"crown",style:"Royal"},
  chromeFlash:{name:"Chrome Flash",primary:0xd8e4ff,sole:0x111111,accent:0x65f0ff,type:"low",pattern:"chrome",style:"Chrome"},
  tigerClaw:{name:"Tiger Claws",primary:0xff8c1a,sole:0x111111,accent:0xffffff,type:"low",pattern:"claw",style:"Wild"},
  midnightPulse:{name:"Midnight Pulse",primary:0x050510,sole:0x1d6cff,accent:0xff3dff,type:"glow",pattern:"pulse",style:"Night"},
  thunderStep:{name:"Thunder Step",primary:0x15253f,sole:0xfff15c,accent:0xffffff,type:"high",pattern:"lightning",style:"Storm"},
  infernoX:{name:"Inferno X",primary:0x111111,sole:0xff4d1f,accent:0xffd56a,type:"glow",pattern:"flames",style:"Inferno"},
  frostRunner:{name:"Frost Runner",primary:0xeaf8ff,sole:0x4fc3ff,accent:0xffffff,type:"low",pattern:"ice",style:"Frost"},
  cityDash:{name:"City Dash",primary:0x1f2430,sole:0x5aa7ff,accent:0xd9e6ff,type:"low",pattern:"city",style:"Metro"},
  neonVenom:{name:"Neon Venom",primary:0x050505,sole:0x39ff14,accent:0xb6ff00,type:"glow",pattern:"drip",style:"Neon"},
  solarJump:{name:"Solar Jump",primary:0x4a1e05,sole:0xffae00,accent:0xfff1a8,type:"boost",pattern:"sunburst",style:"Solar"},
  prismDrive:{name:"Prism Drive",primary:0x202040,sole:0x9d6cff,accent:0x65f0ff,type:"glow",pattern:"prism",style:"Prism"},
  roseBlaze:{name:"Rose Blaze",primary:0x6f1133,sole:0xff5f8f,accent:0xffffff,type:"low",pattern:"petal",style:"Rose"},
  neonGlow:{name:"Neon Glow",primary:0x121226,sole:0x39ff14,accent:0x65f0ff,type:"glow",pattern:"pulse",style:"Neon"},
  royalHi:{name:"Royal High-Tops",primary:0x2f1662,sole:0xffd56a,accent:0xffffff,type:"high",pattern:"crown",style:"Royal"},
  streetLow:{name:"Street Low-Tops",primary:0x222222,sole:0x111111,accent:0xff6b2d,type:"low",pattern:"slash",style:"Street"},
  solarOrange:{name:"Solar Orange",primary:0x4a1e05,sole:0xff8b2b,accent:0xffd56a,type:"boost",pattern:"sunburst",style:"Solar"},
  iceAura:{name:"Ice Aura",primary:0xeaf8ff,sole:0x7ad7ff,accent:0xffffff,type:"glow",pattern:"frost",style:"Ice"},
  toxicGreen:{name:"Toxic Green",primary:0x091209,sole:0x2aff37,accent:0xb6ff00,type:"low",pattern:"slime",style:"Toxic"},
  beastClaws:{name:"Beast Claws",primary:0x3a2316,sole:0x0b0b0b,accent:0xf2e4c8,type:"high",pattern:"claw",style:"Beast"},
  predatorSteps:{name:"Predator Steps",primary:0x050505,sole:0x4d0e0b,accent:0xff8c2b,type:"high",pattern:"scratch",style:"Aggressive"},
  thunderBeasts:{name:"Thunder Beasts",primary:0x10244f,sole:0xfff15c,accent:0xffffff,type:"boost",pattern:"lightning",style:"Explosive"},
  iceFangHighs:{name:"Ice Fang Highs",primary:0xf3fbff,sole:0x57e7ff,accent:0x6a90b4,type:"high",pattern:"fang",style:"Defensive"},
  toxicTalons:{name:"Toxic Talons",primary:0x060806,sole:0x39ff14,accent:0x7dff52,type:"glow",pattern:"talon",style:"Neon"}
};
const sneakerKeys = Object.keys(sneakers);

const courts = {
  classicArena:{name:"Classic Arena",woodA:"#cf8b45",woodB:"#9b5a2b",lineColor:0xffffff,trimColor:0xffd56a,logoText:"1V1 ARENA",theme:"classic",style:"Pro Hardwood",fog:0x0f1725,crowd:0x2a3852,mood:0xffd56a},
  neonStreet:{name:"Neon Street",woodA:"#111827",woodB:"#05070c",lineColor:0x65f0ff,trimColor:0xff3dff,logoText:"NEON RUN",theme:"neon",style:"Night Neon",fog:0x090b15,crowd:0x1b1430,mood:0x65f0ff},
  beachCourt:{name:"Beach Court",woodA:"#e4c17a",woodB:"#c9984a",lineColor:0xffffff,trimColor:0x4fc3ff,logoText:"BOARDWALK",theme:"beach",style:"Sun & Sand",fog:0x182740,crowd:0x2c4458,mood:0xffcd6f},
  royalCourt:{name:"Royal Court",woodA:"#3b1f77",woodB:"#1b103d",lineColor:0xffd56a,trimColor:0xffffff,logoText:"ROYAL HOOPS",theme:"royal",style:"Crown League",fog:0x120d28,crowd:0x2a1b46,mood:0xffd56a},
  blacktop:{name:"Blacktop",woodA:"#202020",woodB:"#080808",lineColor:0xf2f2f2,trimColor:0xff4444,logoText:"BLACKTOP",theme:"street",style:"Urban Dark",fog:0x0a0b0f,crowd:0x262626,mood:0xff6b2d},
  galaxyCourt:{name:"Galaxy Court",woodA:"#151030",woodB:"#050510",lineColor:0x9d6cff,trimColor:0x65f0ff,logoText:"GALAXY RUN",theme:"galaxy",style:"Deep Space",fog:0x080515,crowd:0x1c1736,mood:0x9d6cff},
  lavaRun:{name:"Lava Run",woodA:"#2c0d07",woodB:"#4a1208",lineColor:0xffb347,trimColor:0xff4d1f,logoText:"LAVA RUN",theme:"lava",style:"Molten",fog:0x140807,crowd:0x34140f,mood:0xff5c1f},
  thunderCourt:{name:"Thunder Court",woodA:"#18243c",woodB:"#0b1220",lineColor:0x9fd8ff,trimColor:0xfff15c,logoText:"THUNDER",theme:"lightning",style:"Electric",fog:0x0b1325,crowd:0x202d44,mood:0x9fd8ff},
  sunsetPark:{name:"Sunset Park",woodA:"#d88b52",woodB:"#9b4d36",lineColor:0xffffff,trimColor:0xffd56a,logoText:"SUNSET PARK",theme:"sunset",style:"Golden Hour",fog:0x2a1f2a,crowd:0x4e3e54,mood:0xffbf73},
  icePalace:{name:"Ice Palace",woodA:"#dff6ff",woodB:"#bde7ff",lineColor:0x4fc3ff,trimColor:0xffffff,logoText:"ICE PALACE",theme:"ice",style:"Frozen",fog:0x0f2740,crowd:0x214762,mood:0x8fe9ff},
  cyberGrid:{name:"Cyber Grid",woodA:"#0b0f1a",woodB:"#151c2f",lineColor:0x65f0ff,trimColor:0xff3dff,logoText:"CYBER GRID",theme:"cyber",style:"Digital",fog:0x090d16,crowd:0x251a3a,mood:0x65f0ff},
  jungleRun:{name:"Jungle Run",woodA:"#34512f",woodB:"#1d3218",lineColor:0xe9f7d7,trimColor:0x8eff5e,logoText:"JUNGLE RUN",theme:"jungle",style:"Wild",fog:0x0e1b13,crowd:0x23341f,mood:0x8eff5e},
  goldLeague:{name:"Gold League",woodA:"#4a3b14",woodB:"#221908",lineColor:0xfff2b0,trimColor:0xffd56a,logoText:"GOLD LEAGUE",theme:"gold",style:"Championship",fog:0x161106,crowd:0x3a2e12,mood:0xffd56a},
  midnightCity:{name:"Midnight City",woodA:"#10131d",woodB:"#05070c",lineColor:0xd9e6ff,trimColor:0x5aa7ff,logoText:"MIDNIGHT CITY",theme:"city",style:"Metro Night",fog:0x070a14,crowd:0x1b2637,mood:0x5aa7ff},
  candyShock:{name:"Candy Shock",woodA:"#ff77b7",woodB:"#8b4dff",lineColor:0xffffff,trimColor:0x65f0ff,logoText:"CANDY SHOCK",theme:"candy",style:"Arcade",fog:0x251033,crowd:0x3b1f4f,mood:0xff77b7},
  desertStorm:{name:"Desert Storm",woodA:"#c59a59",woodB:"#8e6d3a",lineColor:0xffffff,trimColor:0x5c3b1f,logoText:"DESERT STORM",theme:"desert",style:"Dust Bowl",fog:0x2a2118,crowd:0x4a3a2c,mood:0xd4a56a}
  ,seattleRooftop:{name:"Seattle Rain Roof",woodA:"#263747",woodB:"#111c29",lineColor:0xccecff,trimColor:0x55bfff,logoText:"SEATTLE 1V1",theme:"rain",style:"Rainy Rooftop",fog:0x172332,crowd:0x23384a,mood:0x65c7ff}
  ,portlandWarehouse:{name:"Portland Warehouse",woodA:"#6e5742",woodB:"#3a3029",lineColor:0xe6e1d8,trimColor:0xe06645,logoText:"ROSE CITY",theme:"warehouse",style:"Industrial Hardwood",fog:0x211c19,crowd:0x3a312b,mood:0xe77a58}
  ,bayBlacktop:{name:"Bay Blacktop",woodA:"#252d31",woodB:"#111719",lineColor:0xdce9ec,trimColor:0xf15b43,logoText:"THE BAY",theme:"bay",style:"Fogline Blacktop",fog:0x142127,crowd:0x29363b,mood:0x77d4e4}
  ,laSunset:{name:"Los Angeles Sunset",woodA:"#c7674d",woodB:"#733b4e",lineColor:0xfff1dc,trimColor:0xffb04f,logoText:"WEST COAST",theme:"sunset",style:"Sunset Outdoor",fog:0x3d2338,crowd:0x52304a,mood:0xffa95e}
  ,phoenixDesert:{name:"Phoenix Desert Run",woodA:"#c99052",woodB:"#8d4f32",lineColor:0xfff1d0,trimColor:0xff6c37,logoText:"VALLEY HEAT",theme:"desert",style:"Desert Court",fog:0x3b2119,crowd:0x5e3528,mood:0xff8b4d}
  ,denverSummit:{name:"Denver Summit",woodA:"#53616a",woodB:"#29353c",lineColor:0xe9f7ff,trimColor:0x79c6ff,logoText:"MILE HIGH",theme:"mountain",style:"Mountain Arena",fog:0x1a2730,crowd:0x344650,mood:0xbce7ff}
  ,dallasFieldhouse:{name:"Dallas Fieldhouse",woodA:"#a66f3f",woodB:"#613b25",lineColor:0xf7ead9,trimColor:0x4ca6ff,logoText:"LONE STAR",theme:"fieldhouse",style:"Texas Fieldhouse",fog:0x1f2730,crowd:0x303c49,mood:0x6ab4ff}
  ,chicagoHardwood:{name:"Chicago Hardwood",woodA:"#b4773c",woodB:"#703f25",lineColor:0xf6f3e9,trimColor:0xd43131,logoText:"WINDY CITY",theme:"classic",style:"Classic Urban Hardwood",fog:0x191d24,crowd:0x313640,mood:0xff4d4d}
  ,atlantaNight:{name:"Atlanta Night Run",woodA:"#3b214b",woodB:"#160f23",lineColor:0xffd9f2,trimColor:0xff5ba7,logoText:"ATL NIGHT",theme:"nightlife",style:"Southern Night",fog:0x140c1d,crowd:0x352342,mood:0xff69b8}
  ,miamiNeon:{name:"Miami Neon Beach",woodA:"#0d4c5a",woodB:"#102438",lineColor:0xfff1dd,trimColor:0xff4da6,logoText:"SOUTH BEACH",theme:"neon",style:"Beach Nightlife",fog:0x071a2a,crowd:0x17364b,mood:0x4fffe1}
  ,newYorkCage:{name:"New York Cage",woodA:"#272727",woodB:"#0d0d0d",lineColor:0xf0efe8,trimColor:0xf1c232,logoText:"THE CAGE",theme:"street",style:"Dense City Blacktop",fog:0x0c0d10,crowd:0x24262b,mood:0xffd84a}
  ,bostonLegacy:{name:"Boston Legacy Gym",woodA:"#9b6b35",woodB:"#5f3f20",lineColor:0xf5f1df,trimColor:0x39a56b,logoText:"LEGACY",theme:"legacy",style:"Historic Gym",fog:0x15201b,crowd:0x2b4035,mood:0x58c98a}
  ,vegasFinals:{name:"Las Vegas National Finals",woodA:"#2d2412",woodB:"#0e0b07",lineColor:0xfff1a6,trimColor:0xffd23f,logoText:"NATIONAL FINAL",theme:"gold",style:"Championship Arena",fog:0x120e08,crowd:0x493814,mood:0xffd34f}
};
const courtKeys = Object.keys(courts);

const crowdArchetypes = {
  tshirtFan:{top:"tee",pose:"seated"}, hoodieFan:{top:"hoodie",pose:"seated"}, jerseyFan:{top:"jersey",pose:"standing"},
  jacketFan:{top:"jacket",pose:"seated"}, capFan:{hat:"cap",pose:"standing"}, beanieFan:{hat:"beanie",pose:"seated"},
  longHairFan:{hair:"long",pose:"seated"}, kidFan:{scale:0.82,pose:"seated"}, largeFan:{scale:1.18,pose:"standing"},
  hypeFan:{top:"jersey",pose:"hype"}, chillFan:{top:"hoodie",pose:"seated"}
};

const difficultySettings = {
  rookie:{reactionMin:.45,reactionMax:.65,steal:.18,block:.08,speed:2.2,sprint:.08,attack:.16,aggression:.35,patience:.28,mistakeChance:.28,fakeBiteChance:.56,shotDiscipline:.35,defensiveIQ:.32,crossoverUse:.2,pumpFakeUse:.24,stepBackUse:.08,sideHopUse:.12,contestTiming:.32,stealDiscipline:.25,staminaManagement:.3,spacingIQ:.26,composure:.24},
  easy:{reactionMin:.35,reactionMax:.5,steal:.35,block:.2,speed:2.6,sprint:.2,attack:.3,aggression:.45,patience:.36,mistakeChance:.22,fakeBiteChance:.45,shotDiscipline:.48,defensiveIQ:.45,crossoverUse:.28,pumpFakeUse:.22,stepBackUse:.1,sideHopUse:.18,contestTiming:.42,stealDiscipline:.34,staminaManagement:.42,spacingIQ:.44,composure:.42},
  normal:{reactionMin:.25,reactionMax:.38,steal:.9,block:.6,speed:3.2,sprint:.48,attack:.58,aggression:.58,patience:.52,mistakeChance:.16,fakeBiteChance:.32,shotDiscipline:.58,defensiveIQ:.58,crossoverUse:.34,pumpFakeUse:.3,stepBackUse:.18,sideHopUse:.24,contestTiming:.56,stealDiscipline:.52,staminaManagement:.56,spacingIQ:.6,composure:.56},
  hard:{reactionMin:.18,reactionMax:.28,steal:1.5,block:1.15,speed:3.8,sprint:.66,attack:.78,aggression:.68,patience:.62,mistakeChance:.12,fakeBiteChance:.22,shotDiscipline:.68,defensiveIQ:.72,crossoverUse:.4,pumpFakeUse:.38,stepBackUse:.26,sideHopUse:.34,contestTiming:.68,stealDiscipline:.68,staminaManagement:.66,spacingIQ:.72,composure:.68},
  elite:{reactionMin:.12,reactionMax:.2,steal:1.95,block:1.55,speed:4.1,sprint:.8,attack:.94,aggression:.74,patience:.72,mistakeChance:.1,fakeBiteChance:.16,shotDiscipline:.78,defensiveIQ:.82,crossoverUse:.46,pumpFakeUse:.45,stepBackUse:.34,sideHopUse:.4,contestTiming:.78,stealDiscipline:.78,staminaManagement:.74,spacingIQ:.82,composure:.8},
  nightmare:{reactionMin:.09,reactionMax:.16,steal:2.4,block:2,speed:4.45,sprint:.9,attack:1.08,aggression:.84,patience:.68,mistakeChance:.09,fakeBiteChance:.12,shotDiscipline:.86,defensiveIQ:.9,crossoverUse:.5,pumpFakeUse:.52,stepBackUse:.42,sideHopUse:.45,contestTiming:.86,stealDiscipline:.86,staminaManagement:.8,spacingIQ:.9,composure:.86},
  legend:{reactionMin:.06,reactionMax:.12,steal:3,block:2.6,speed:4.8,sprint:1,attack:1.22,aggression:.9,patience:.82,mistakeChance:.08,fakeBiteChance:.08,shotDiscipline:.92,defensiveIQ:.95,crossoverUse:.62,pumpFakeUse:.58,stepBackUse:.5,sideHopUse:.55,contestTiming:.92,stealDiscipline:.92,staminaManagement:.9,spacingIQ:.94,composure:.94},
  pro:{reactionMin:.10,reactionMax:.18,steal:2.2,block:1.9,speed:4.35,sprint:.86,attack:1.0,aggression:.76,patience:.78,mistakeChance:.075,fakeBiteChance:.11,shotDiscipline:.84,defensiveIQ:.88,crossoverUse:.54,pumpFakeUse:.48,stepBackUse:.42,sideHopUse:.45,contestTiming:.86,stealDiscipline:.84,staminaManagement:.84,spacingIQ:.88,composure:.86},
  superstar:{reactionMin:.075,reactionMax:.13,steal:2.55,block:2.25,speed:4.5,sprint:.92,attack:1.12,aggression:.82,patience:.84,mistakeChance:.055,fakeBiteChance:.08,shotDiscipline:.90,defensiveIQ:.93,crossoverUse:.62,pumpFakeUse:.55,stepBackUse:.50,sideHopUse:.52,contestTiming:.91,stealDiscipline:.90,staminaManagement:.90,spacingIQ:.92,composure:.92},
  hallOfFame:{reactionMin:.055,reactionMax:.10,steal:2.85,block:2.55,speed:4.65,sprint:.97,attack:1.2,aggression:.86,patience:.90,mistakeChance:.035,fakeBiteChance:.055,shotDiscipline:.95,defensiveIQ:.97,crossoverUse:.68,pumpFakeUse:.62,stepBackUse:.58,sideHopUse:.58,contestTiming:.96,stealDiscipline:.95,staminaManagement:.95,spacingIQ:.96,composure:.96},
  impossible:{reactionMin:.035,reactionMax:.075,steal:3.1,block:2.8,speed:4.75,sprint:1.0,attack:1.28,aggression:.90,patience:.94,mistakeChance:.02,fakeBiteChance:.035,shotDiscipline:.98,defensiveIQ:1.0,crossoverUse:.74,pumpFakeUse:.68,stepBackUse:.65,sideHopUse:.64,contestTiming:1.0,stealDiscipline:.98,staminaManagement:.98,spacingIQ:1.0,composure:1.0}
};

const botPersonalities = {
  rim_runner: { drive:.38, layup:.34, pullUp:.08, pumpFake:.08, stepBack:.02, post:.10 },
  ankle_breaker: { crossover:.28, hesitation:.22, drive:.28, pullUp:.10, sideHop:.08, pumpFake:.04 },
  sharpshooter: { pullUp:.30, stepBack:.26, pumpFake:.20, drive:.12, hesitation:.08, layup:.04 },
  post_bully: { post:.34, backDown:.28, layup:.24, pumpFake:.10, pullUp:.04 },
  lockdown: { safeDrive:.22, pullUp:.12, reset:.20, layup:.16, crossover:.10, defensePressure:.40 },
  slasher: { drive:.38, sideHop:.18, hesitation:.14, layup:.22, pullUp:.06, stepBack:.02 },
  fake_master: { pumpFake:.24, hesitation:.22, crossover:.18, sideHop:.14, pullUp:.12, layup:.10 },
  paint_anchor: { post:.36, layup:.32, backDown:.22, pullUp:.04, pumpFake:.06 }
};
