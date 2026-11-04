'use strict';
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const sourcePath = path.resolve(__dirname, '../game/deep_sea.html');
function instrument(html) {
  const marker = html.lastIndexOf('})();');
  if (marker < 0) throw new Error('Deep Sea inline script was not found');
  const hook = `
  const renderSamples = [], updateSamples = [];
  const originalRender = render;
  render = function(){ const t = performance.now(); originalRender(); renderSamples.push(performance.now()-t); if(renderSamples.length>1200) renderSamples.shift(); };
  const originalUpdateFish = updateFish;
  updateFish = function(dt){ const t = performance.now(); originalUpdateFish(dt); updateSamples.push(performance.now()-t); if(updateSamples.length>1200) updateSamples.shift(); };
  globalThis.__deep = {
    state, keys, ui, fishTypes, UPGRADE_DEFS, MAX_FISH, MAX_PARTICLES, MAX_BUBBLES,
    reset, start, tick, render, updateFish, updatePlayer, updateBites, updateCamera, updateUI, updatePredatorPanic,
    updateObstacles, updatePlants, updateBubbles, updateWaterParticles,
    spawnFish, eatFish, resolveFishPredation, fishCanEatFish, fishCanThreatenFish,
    fishPredatorIntent, applyStarterUpgrade, applyUpgrade, biteLunge,
    togglePause: typeof togglePause === 'function' ? togglePause : null,
    floorHeight, waterColumnY, forwardFromAngles, dist3, sprintMax,
    renderSamples, updateSamples,
    rebuild: () => {
      if(typeof rebuildSpatialIndexes === 'function') rebuildSpatialIndexes();
      if(typeof rebuildSpatialGrids === 'function') rebuildSpatialGrids();
      if(typeof rebuildSpatialGrid === 'function') rebuildSpatialGrid();
      if(typeof rebuildFishGrid === 'function') rebuildFishGrid();
      if(typeof rebuildObstacleGrid === 'function') rebuildObstacleGrid();
    },
    nearbyFish: typeof nearbyFish === 'function' ? nearbyFish : null,
    nearbyObstacles: typeof nearbyObstacles === 'function' ? nearbyObstacles : null,
    attack: typeof updatePredatorAttack === 'function' ? updatePredatorAttack : null,
    setObstacleSpawner: fn => { const previous=spawnObstacle; spawnObstacle=fn; return previous; }
  };
`;
  return html.slice(0, marker) + hook + html.slice(marker);
}
function seededMath(seed) {
  const math = Object.create(Math);
  math.random = () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; };
  return math;
}
function loadGame(seed = 42, file = sourcePath) {
  const ctx = new Proxy({ createLinearGradient: () => ({ addColorStop(){} }), createRadialGradient: () => ({ addColorStop(){} }), measureText: s => ({width: s.length*8}) }, { get: (o,k) => k in o ? o[k] : () => {} });
  const makeElement = () => ({
    style: {setProperty(){}}, classList: {add(){}, remove(){}, toggle(){}},
    children: [], textContent: '', innerHTML: '', dataset: {},
    appendChild(c){this.children.push(c);}, setAttribute(){}, addEventListener(){},
    querySelector(){return makeElement();}, querySelectorAll(){return [];},
    getContext(){return ctx;}, getBoundingClientRect(){return {left:0,top:0,width:1280,height:720};},
    requestPointerLock(){}, setPointerCapture(){}, releasePointerCapture(){},
  });
  const elements = new Map();
  const document = {hidden:false, visibilityState:'visible', body:makeElement(), documentElement:makeElement(),
    getElementById(id){if(!elements.has(id)) elements.set(id,makeElement()); return elements.get(id);},
    createElement:makeElement, addEventListener(){}, exitPointerLock(){},
  };
  const context = { Math:seededMath(seed), document, innerWidth:1280, innerHeight:720,
    devicePixelRatio:1, navigator:{maxTouchPoints:0}, matchMedia:()=>({matches:false,addEventListener(){}}),
    addEventListener(){}, requestAnimationFrame(){}, cancelAnimationFrame(){},
    performance:{now:()=>performance.now()}, console, setTimeout, clearTimeout,
    location:{search:''}, URLSearchParams,
  };
  context.window = context;
  const html = instrument(fs.readFileSync(file,'utf8'));
  const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
  vm.runInNewContext(script, context, {filename:'deep_sea.html',timeout:10000});
  return context.__deep;
}
function play(game) {
  game.start();
  game.applyStarterUpgrade(game.UPGRADE_DEFS.find(u=>u.id==='streamline') || game.UPGRADE_DEFS[0]);
  game.state.player.nextUpgradeAt = 1e9;
  game.rebuild();
}
module.exports = { loadGame, play, instrument, sourcePath };
