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

let troops = [];
let effects = [];
let structures = [];
let minerals = [];
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

const playerStats = {
  gold: 0, spawnRateLevel: 1, groupSizeLevel: 1, hpLevel: 1,
  dmgLevel: 1, speedLevel: 1, workerCarryLevel: 1,
  get spawnRate(){ return 3 * this.spawnRateLevel; },
  get groupSize(){ return this.groupSizeLevel; },
  get troopHpMult(){ return 1 + 0.35*(this.hpLevel-1); },
  get troopDmgMult(){ return 1 + 0.35*(this.dmgLevel-1); },
  get troopSpeedMult(){ return 1 + 0.18*(this.speedLevel-1); },
  get workerCarryAmount(){ return 20 + (this.workerCarryLevel-1)*6; }
};
const enemyStats = { spawnRate:2.5, groupSize:1, hpMult:1, dmgMult:1, speedMult:1 };

function applyDifficulty(){
  const doctrines=['RUSH','FORTRESS','RAIDER','INDUSTRIAL','OVERLORD'];
  enemyDoctrine=doctrines[(Math.max(1,level)-1)%doctrines.length];
  enemyStats.spawnRate=2.5+level*.6;
  enemyStats.groupSize=1+Math.floor(level/2);
  enemyStats.hpMult=1+.25*(level-1);
  enemyStats.dmgMult=1+.22*(level-1);
  enemyStats.speedMult=1+.10*(level-1);
  if(enemyDoctrine==='RUSH'){enemyStats.spawnRate*=1.18;enemyStats.speedMult*=1.10;enemyStats.hpMult*=.96;}
  else if(enemyDoctrine==='FORTRESS'){enemyStats.spawnRate*=.92;enemyStats.hpMult*=1.18;enemyStats.dmgMult*=1.08;}
  else if(enemyDoctrine==='RAIDER'){enemyStats.spawnRate*=1.02;enemyStats.speedMult*=1.18;enemyStats.dmgMult*=1.08;}
  else if(enemyDoctrine==='INDUSTRIAL'){enemyStats.spawnRate*=.94;enemyStats.hpMult*=1.05;}
  else if(enemyDoctrine==='OVERLORD'){enemyStats.spawnRate*=1.08;enemyStats.groupSize+=1;enemyStats.dmgMult*=1.10;}
  if(doctrineEl) doctrineEl.textContent=enemyDoctrine.charAt(0)+enemyDoctrine.slice(1).toLowerCase();
}
function enemyWorkerPeriod(){ return Math.max(8,25/(1+.25*(level-1))); }
function costSpawnRate(){const l=playerStats.spawnRateLevel;return Math.round(80*l*l*1.1);}
function costGroupSize(){const l=playerStats.groupSizeLevel;return Math.round(120*l*l*1.35);}
function costHP(){const l=playerStats.hpLevel;return Math.round(90*l*l*1.15);}
function costDMG(){const l=playerStats.dmgLevel;return Math.round(90*l*l*1.15);}
function costSPD(){const l=playerStats.speedLevel;return Math.round(110*l*l*1.2);}
function costWorkerMinerals(){return 20+10*playerWorkerCount;}
function costWorkerGold(){return 40+15*playerWorkerCount;}
function costCarry(){const l=playerStats.workerCarryLevel;return Math.round(70*l*Math.pow(1.5,l-1));}
const SPELL_TYPES=['catapult','napalm','laser','summon','god'];
let spellTimer=0,availableSpells=[],pendingSpellIndex=null,pendingSpellType=null,selectedOutpost=null,pendingOutcome=null,enemyTechTimer=0;
function showMessage(text){bannerEl.textContent=text;bannerEl.style.opacity='1';messageTimer=2.2;}
function updateBanner(dt){if(messageTimer>0){messageTimer-=dt;if(messageTimer<=0)bannerEl.style.opacity='0';}}
function rndChoice(arr){return arr[Math.floor(Math.random()*arr.length)];}
function dist2(ax,ay,bx,by){const dx=bx-ax,dy=by-ay;return dx*dx+dy*dy;}
function clamp(v,min,max){return v<min?min:v>max?max:v;}
function effectiveMinZoom(){return Math.min(MAX_ZOOM,Math.max(.55,VIEW_W/WORLD_W,VIEW_H/WORLD_H));}
function resizeViewport(){const w=Math.max(320,window.innerWidth||960),h=Math.max(320,window.innerHeight||540);DPR=Math.min(2,Math.max(1,window.devicePixelRatio||1));VIEW_W=w;VIEW_H=h;canvas.width=Math.round(w*DPR);canvas.height=Math.round(h*DPR);canvas.style.width=w+'px';canvas.style.height=h+'px';zoom=Math.max(zoom,effectiveMinZoom());clampCam();if(selectedOutpost)positionModePickerFor(selectedOutpost);}
function gridKey(cx,cy){return cx+','+cy;}
function rebuildTroopGrid(){troopGrid.clear();for(let i=0;i<troops.length;i++){const t=troops[i];if(t.dead||t.hp<=0)continue;t._gridIndex=i;const k=gridKey(Math.floor(t.x/GRID_CELL),Math.floor(t.y/GRID_CELL));let bucket=troopGrid.get(k);if(!bucket){bucket=[];troopGrid.set(k,bucket);}bucket.push(t);}}
function nearbyTroops(t,range){const out=[],minX=Math.floor((t.x-range)/GRID_CELL),maxX=Math.floor((t.x+range)/GRID_CELL),minY=Math.floor((t.y-range)/GRID_CELL),maxY=Math.floor((t.y+range)/GRID_CELL);for(let gx=minX;gx<=maxX;gx++)for(let gy=minY;gy<=maxY;gy++){const b=troopGrid.get(gridKey(gx,gy));if(b)out.push(...b);}return out;}
function spawnParticles(x,y,color,count=5,speed=55){const budget=Math.max(0,260-visualParticles.length);count=Math.min(count,budget);for(let i=0;i<count;i++){const a=Math.random()*Math.PI*2,v=speed*(.35+Math.random()*.75);visualParticles.push({x,y,vx:Math.cos(a)*v,vy:Math.sin(a)*v,life:.22+Math.random()*.38,size:1+Math.random()*2.2,color});}}
function updateVisualParticles(dt){const next=[];for(const p of visualParticles){p.life-=dt;if(p.life<=0)continue;p.x+=p.vx*dt;p.y+=p.vy*dt;p.vx*=Math.pow(.05,dt);p.vy*=Math.pow(.05,dt);next.push(p);}visualParticles=next;screenShake=Math.max(0,screenShake-dt*24);}
function ensureTerrainDecor(){if(terrainDecor)return;terrainDecor=[];let seed=734287;const rand=()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296;};for(let i=0;i<150;i++)terrainDecor.push({x:rand()*WORLD_W,y:rand()*WORLD_H,r:3+rand()*24,type:rand()<.32?'crater':rand()<.58?'shard':'dust',rot:rand()*Math.PI*2});}
function centerCamera(){cam.x=WORLD_W/2-VIEW_W/(2*zoom);cam.y=WORLD_H/2-VIEW_H/(2*zoom);clampCam();}
function closeModePicker(){selectedOutpost=null;if(modePickerEl)modePickerEl.classList.remove('open');}
function positionModePickerFor(outpost){if(!outpost||!modePickerEl)return;const sx=(outpost.x+outpost.w/2-cam.x)*zoom,sy=(outpost.y+outpost.h/2-cam.y)*zoom,edge=92;modePickerEl.style.left=`${clamp(sx,edge,VIEW_W-edge)}px`;modePickerEl.style.top=`${clamp(sy,edge,VIEW_H-edge)}px`;for(const el of modeOptionEls)el.classList.toggle('active',el.dataset.mode===outpost.mode);modePickerEl.classList.add('open');}
function openModePicker(outpost){selectedOutpost=outpost;positionModePickerFor(outpost);}
function outpostAt(wx,wy){for(const s of structures){if(s.type!=='outpost'||s.owner!=='player')continue;if(wx>=s.x&&wx<=s.x+s.w&&wy>=s.y&&wy<=s.y+s.h)return s;}return null;}
function clampCam(){const viewWorldW=VIEW_W/zoom,viewWorldH=VIEW_H/zoom,maxCamX=Math.max(0,WORLD_W-viewWorldW),maxCamY=Math.max(0,WORLD_H-viewWorldH);cam.x=clamp(cam.x,0,maxCamX);cam.y=clamp(cam.y,0,maxCamY);}
function setZoom(newZoom){const oldZoom=zoom;zoom=clamp(newZoom,effectiveMinZoom(),MAX_ZOOM);if(zoom===oldZoom)return;const centerWorldX=cam.x+(VIEW_W/oldZoom)/2,centerWorldY=cam.y+(VIEW_H/oldZoom)/2;cam.x=centerWorldX-(VIEW_W/zoom)/2;cam.y=centerWorldY-(VIEW_H/zoom)/2;clampCam();if(zoomLabelEl)zoomLabelEl.textContent=Math.round(zoom*100)+'%';if(selectedOutpost)positionModePickerFor(selectedOutpost);}
