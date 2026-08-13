  const canvas = document.getElementById('game');
  const ctx = canvas.getContext('2d');
  let VIEW_W = 960;
  let VIEW_H = 540;
  let DPR = 1;
  const WORLD_W = 2400;
  const WORLD_H = 1600;

  const goldEl = document.getElementById('gold');
  const mineralsEl = document.getElementById('minerals');
  const levelEl = document.getElementById('level');
  const bestLevelEl = document.getElementById('bestLevel');
  const pCountEl = document.getElementById('pCount');
  const eCountEl = document.getElementById('eCount');
  const workersEl = document.getElementById('workers');
  const nextSpellTimerEl = document.getElementById('nextSpellTimer');
  const bannerEl = document.getElementById('messageBanner');
  const spellsListEl = document.getElementById('spellsList');
  const spellHintEl = document.getElementById('spellHint');

  const btnSpawnRate = document.getElementById('btnSpawnRate');
  const btnGroupSize = document.getElementById('btnGroupSize');
  const btnHP = document.getElementById('btnHP');
  const btnDMG = document.getElementById('btnDMG');
  const btnSPD = document.getElementById('btnSPD');
  const btnWorker = document.getElementById('btnWorker');
  const btnCarry = document.getElementById('btnCarry');

  const costSpawnRateEl = document.getElementById('costSpawnRate');
  const costGroupSizeEl = document.getElementById('costGroupSize');
  const costHPEl = document.getElementById('costHP');
  const costDMGEl = document.getElementById('costDMG');
  const costSPEl = document.getElementById('costSPD');
  const costWorkerEl = document.getElementById('costWorker');
  const costCarryEl = document.getElementById('costCarry');

  const btnLoadRun = document.getElementById('btnLoadRun');
  const btnSaveRun = document.getElementById('btnSaveRun');
  const btnRestartRun = document.getElementById('btnRestartRun');
  const btnCenterCam = document.getElementById('btnCenterCam');
  const btnFullscreen = document.getElementById('btnFullscreen');

  const modePickerEl = document.getElementById('modePicker');
  const modeOptionEls = Array.from(document.querySelectorAll('.modeOption'));

  const zoomOutBtn = document.getElementById('zoomOut');
  const zoomInBtn = document.getElementById('zoomIn');
  const zoomLabelEl = document.getElementById('zoomLabel');
  const doctrineEl = document.getElementById('doctrine');
  const btnTech = document.getElementById('btnTech');
  const techDrawerEl = document.getElementById('techDrawer');
  const rewardOverlayEl = document.getElementById('rewardOverlay');
  const rewardChoicesEl = document.getElementById('rewardChoices');

  const SAVE_KEY = 'swarmMineralSaveV1';
  const BEST_KEY = 'swarmMineralBestLevelV1';

  // camera + zoom
  const cam = { x:0, y:0 };
  let zoom = 1;
  const MIN_ZOOM = 0.6;
  const MAX_ZOOM = 1.6;
  const ZOOM_STEP = 0.2;

  let dragging = false;
  let dragStartX = 0;
  let dragStartY = 0;
  let dragStartCamX = 0;
  let dragStartCamY = 0;

  // touch tracking
  let lastTouchClientX = 0;
  let lastTouchClientY = 0;
  let touchMoved = false;
  let pointerDownClientX = 0;
  let pointerDownClientY = 0;
  let pointerMoved = false;
  let pinchStartDist = 0;
  let pinchStartZoom = 1;
  let pinchWorldX = 0;
  let pinchWorldY = 0;
  let pinching = false;

  // game state
  let troops = [];       // soldiers, reapers, workers, god
  let effects = [];      // visual + spells
  let structures = [];   // HQ + outposts
  let minerals = [];     // mineral patches
  let visualParticles = [];
  let screenShake = 0;
  let gamePaused = false;
  let uiRefreshTimer = 0;
  let enemyDoctrine = 'RUSH';
  const troopGrid = new Map();
  const GRID_CELL = 56;
  let terrainDecor = null;

  let playerHQ = null;
  let enemyHQ = null;

  let level = 1;
  let bestLevel = 1;
  let lastTime = performance.now();
  let messageTimer = 0;
  let autoSaveTimer = 0;
  let loopStarted = false;

  let playerTroopCount = 0;
  let enemyTroopCount = 0;
  let playerWorkerCount = 0;
  let enemyWorkerCount = 0;

  const MAX_PLAYER_TROOPS = 600;
  const MAX_ENEMY_TROOPS = 600;

  let playerMinerals = 0;
  let enemyMinerals = 0;

  const BASE_TROOP_HP = 12;
  const BASE_TROOP_DMG = 3;
  const BASE_TROOP_SPEED = 38;
