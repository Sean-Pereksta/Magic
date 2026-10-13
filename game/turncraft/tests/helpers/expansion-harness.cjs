const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const GAME_FILE = path.resolve(__dirname, '../../../turncraft.html');
const MAP_FILE = path.resolve(__dirname, '../../maps.js');
const noop = () => {};

// Run the real inline simulation with rendering, boot, timers and Firebase disabled.
// Tests evaluate inside its IIFE so they exercise authoritative functions and state.
function simulation(faction = 'frontier', options = {}) {
  const paint = new Proxy({
    measureText: text => ({width: String(text).length * 6}),
    createLinearGradient: () => ({addColorStop: noop}),
    createRadialGradient: () => ({addColorStop: noop}),
    getImageData: () => ({data: new Uint8ClampedArray(4)})
  }, {get: (target, key) => target[key] || noop});
  function element() {
    return {
      style: {setProperty: noop}, dataset: {}, children: [],
      classList: {add: noop, remove: noop, toggle: noop, contains: () => false},
      appendChild: noop, append: noop, remove: noop, setAttribute: noop,
      getAttribute: () => null, addEventListener: noop, removeEventListener: noop,
      querySelector: () => null, querySelectorAll: () => [], getContext: () => paint,
      getBoundingClientRect: () => ({left: 0, top: 700, width: 1280, height: 100, right: 1280, bottom: 800}),
      insertAdjacentHTML: noop, focus: noop, click: noop,
      textContent: '', innerHTML: '', scrollTop: 0, scrollLeft: 0
    };
  }
  const elements = new Map();
  const document = {
    getElementById: id => {
      if (!elements.has(id)) elements.set(id, element());
      return elements.get(id);
    },
    createElement: element, querySelector: () => null, querySelectorAll: () => [],
    addEventListener: noop, body: element(), documentElement: element()
  };
  const storage = new Map();
  const network = {writes: [], deletes: [], listener: null};
  const sandbox = {
    console, document, URL, URLSearchParams,
    location: {search: '', href: 'http://turncraft.test/game/turncraft.html'},
    localStorage: {getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, String(value))},
    innerWidth: 1280, innerHeight: 900, devicePixelRatio: 1,
    performance: {now: () => 0}, setTimeout: () => 0, clearTimeout: noop,
    setInterval: () => 0, clearInterval: noop, requestAnimationFrame: noop,
    matchMedia: () => ({matches: false, addEventListener: noop}),
    HTMLInputElement: class {}, HTMLSelectElement: class {}, Image: class {},
    initializeApp: () => ({}), getAuth: () => ({}), getFirestore: () => ({}),
    doc: (...args) => args.slice(1).join('/'), collection: (...args) => args.slice(1).join('/'),
    setDoc: async (ref, data) => network.writes.push({ref, data}),
    deleteDoc: async ref => network.deletes.push(ref),
    onSnapshot: (ref, callback) => {network.listener = callback; return noop},
    serverTimestamp: () => ({testTimestamp: true}),
    navigator: {}, screen: {}, addEventListener: noop, testClock: 100000
  };
  sandbox.window = sandbox;
  const html = fs.readFileSync(options.sourceFile || GAME_FILE, 'utf8');
  const module = html.match(/<script type="module">([\s\S]*?)<\/script>/);
  if (!module) throw new Error('Turncraft inline simulation module was not found');
  let code = module[1].replace(/^import .*;$/gm, '');
  if (options.fragments?.length) {
    const fragments = options.fragments.map(file => fs.readFileSync(file, 'utf8')).join('\n');
    code = code.replace('// ---------- Main loop ----------', `${fragments}\n// ---------- Main loop ----------`);
  }
  if (!code.includes('\nboot();')) throw new Error('Turncraft boot boundary was not found');
  code = code.replace('\nboot();', `\nrefreshUI=()=>{};showCommandMarker=()=>{};pushBattleAlert=()=>{};toast=()=>{};
globalThis.addTestNode=(...args)=>{addNode(...args);return resourceNodes.at(-1)};
globalThis.testEvaluate=source=>eval(source);`);
  const context = vm.createContext(sandbox);
  vm.runInContext(fs.readFileSync(MAP_FILE, 'utf8'), context, {timeout: 30000, filename: 'turncraft/maps.js'});
  vm.runInContext(code, context, {timeout: 30000, filename: 'turncraft.html'});
  const run = source => vm.runInContext(`testEvaluate(${JSON.stringify(source)})`, context, {timeout: 30000});
  const factions = options.factions || [faction, faction === 'ascendant' ? 'frontier' : 'ascendant', 'brood'];
  run(`nowMs=()=>testClock;entities=[];resourceNodes=[];nextEntityId=1;nextNodeId=1;
    players=${JSON.stringify(factions)}.map((id,i)=>makePlayer(i,'Player '+i,id,false));
    players.forEach(p=>{p.minerals=5000;p.gas=5000});
    terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
    gameStarted=true;gameOver=false;paused=false;PLAYER=0;selectedId=null;selectedIds=[];
    modeState=null;pendingImpacts=[];areaEffects=[];temporaryObstacles=[];scanEffects=[];
    visionCache=new Map();visionCacheAt=0;initialCameraSet=true;lobbyId='';
    tileVisibleToFaction=()=>true;
  `);
  run.context = context;
  run.elements = elements;
  run.network = network;
  run.plain = source => JSON.parse(JSON.stringify(run(source)));
  run.advance = (seconds, step = .1) => run(`(()=>{const end=testClock+${Number(seconds)*1000};while(testClock<end){const dt=Math.min(${Number(step)},(end-testClock)/1000);testClock+=dt*1000;updatePersistentOrders();updateCombat(dt);resolveProjectileImpacts();updateProduction(dt);updateRegeneration(dt);updateAreaEffects();updateObjectives(dt)}return testClock})()`);
  return run;
}

module.exports = {simulation, GAME_FILE};
