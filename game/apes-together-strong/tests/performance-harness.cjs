'use strict';
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { performance } = require('node:perf_hooks');

const MODULES = ['world', 'navigation', 'settlements', 'forces', 'sim'];
const SCENARIOS = [
  { id: 'A', label: '100 followers in dense forest', apes: 100, humans: 0 },
  { id: 'B', label: '200 followers in dense forest', apes: 200, humans: 0 },
  { id: 'C', label: '150 apes fighting 80 humans', apes: 150, humans: 80, combat: true },
  { id: 'D', label: '200 apes, 150 humans, vehicles and alarms', apes: 200, humans: 150, combat: true, vehicles: 4, helis: 2, alarm: true },
  { id: 'E', label: '180 residents during a major settlement raid', apes: 180, humans: 100, combat: true, vehicles: 2, settlement: true },
  { id: 'F', label: 'rapid procedural exploration', apes: 100, humans: 0, exploration: true },
  { id: 'G', label: '300 apes against combined arms with tanks, APCs and cannon', apes: 300, humans: 100, combat: true, vehicles: 8, helis: 2, alarm: true, military: true }
];

function loadEngine(directory = path.resolve(__dirname, '..')) {
  const math = Object.create(Math);
  let n = 0x923cd91;
  math.random = () => { n = Math.imul(1664525, n) + 1013904223 | 0; return (n >>> 0) / 4294967296; };
  const context = vm.createContext({ console, Math: math, Map, Set, performance });
  context.window = context;
  for (const name of MODULES) vm.runInContext(fs.readFileSync(path.join(directory, name + '.js'), 'utf8'), context, { filename: name + '.js' });
  return context;
}

function setupScenario(context, scenario) {
  const g = new context.ATSGame('FOREST-A', 'survival');
  // Keep the specified workloads stable throughout the benchmark; ordinary
  // damage, roles, movement, alarms and projectile collision still execute.
  g.king.hp = g.king.maxHp = 1000000;
  g.spawnSites = () => {};
  for (let i = 0; i < scenario.apes; i++) {
    const a = g.makeApe(-30 - i % 16 * 7, (i - scenario.apes / 2) * 2.5, 'follow');
    if (scenario.combat) a.hp = a.maxHp = 1000000;
  }
  if (!scenario.combat) Object.assign(g.king, g.findOpen(720, 0, 12));
  if (scenario.settlement) {
    const original = g.world.getSites;
    g.world.getSites = () => [];
    g.food = 1000;
    if (!g.command('settleAll')) throw new Error('Unable to set up stress settlement');
    g.world.getSites = original;
    const s = g.settlements[0];
    s.attack = s.known = true;
    s.food = 10000;
    s.housing = scenario.apes + 12;
    s.policy = 'fortify';
  }
  const site = { id: 'stress-garrison', x: 370, y: 0, tier: 4, objects: [], strength: 150, spawned: true, guards: scenario.humans, cleared: false, alarm: !!scenario.alarm, alarmUntil: 90, nextOperation: 1000, lastRaid: 0 };
  if (scenario.combat) g.world.sites.set(site.id, site);
  for (let i = 0; i < scenario.humans; i++) {
    const h = g.makeHuman(90 + i % 12 * 26, (Math.floor(i / 12) - 5) * 30, site);
    h.hp = h.maxHp = 1000000;
    h.state = 'combat';
    h.reported = true;
    h.suspicion = 1;
    h.targetId = g.apes[i % g.apes.length].id;
    h.lastX = -70; h.lastY = 0;
    if (scenario.settlement) h.raidTarget = g.settlements[0].id;
  }
  for (let i = 0; i < (scenario.vehicles || 0); i++) {
    const p = g.findOpen(230 + i * 90, 170, 21);
    g.vehicles.push({ id: 'vehicle-stress-' + i, ...p, dir: Math.PI, kind: i % 2 ? 'jeep' : 'armored', hp: 1000000, maxHp: 1000000, siteId: site.id, state: 'raid', target: scenario.settlement ? g.settlements[0] : g.king, shootTimer: .1, phase: 0 });
  }
  if(scenario.military){g.updateResponseStage();const militaryRoles=['leader','rifleman','rifleman','ranger','heavy','grenadier','medic','sniper','engineer','rifleman','heavy','rifleman'];g.humans.forEach((h,i)=>{g.forces.assign(h,site,militaryRoles[i%12]);h.hp=h.maxHp=1000000});const kinds=['tank','tank','apc','ifv','apc','truck','truck','jeep'];g.vehicles.forEach((v,i)=>{v.kind=v.vehicleClass=kinds[i];g.forces.initVehicle(v,kinds[i],0);v.hp=v.maxHp=1000000;v.cannonTimer=0;v.cannonCooldown=0});for(let i=0;i<g.humans.length;i+=12)g.forces.createSquad(g.humans.slice(i,i+12),{x:-70,y:0},{order:'Suppress',vehicleId:g.vehicles[Math.floor(i/12)%5].id});g.forces.hazards.push({id:'stress-grenade',type:'grenade',x:-70,y:0,start:0,life:1.8,fuse:1.8,radius:67});}
  for (let i = 0; i < (scenario.helis || 0); i++) g.helis.push({ id: 'heli-stress-' + i, x: 500, y: i * 200, spotX: 450, spotY: i * 200, dir: Math.PI, hp: 250, phase: 0, kind: scenario.military ? 'gunship' : 'recon', target:{x:0,y:0}, armed: true, shootTimer: .1 });
  if (scenario.alarm) {
    g.alert(g.humans[0], 'alarm');
    // Alert intentionally uses real search behavior; officers keep radios.
    g.noise(0, 0, 900, 'alert');
  }
  g.apeGrid.rebuild([g.king, ...g.apes]);
  g.humanGrid.rebuild(g.humans);
  return g;
}

function instrument(game) {
  const measures = {};
  const wrap = (object, name, label) => {
    if (typeof object[name] !== 'function') return;
    const original = object[name];
    const entry = measures[label] = { calls: 0, ms: 0 };
    object[name] = function (...args) { const start = performance.now(); entry.calls++; try { return original.apply(this, args); } finally { entry.ms += performance.now() - start; } };
  };
  wrap(game.world, 'blocked', 'blocked');
  wrap(game.world, 'lineClear', 'lineOfSight');
  wrap(game.world, 'ensure', 'worldEnsure');
  wrap(game.world, '_generateChunk', 'chunkGeneration');
  wrap(game.navigation, 'clearSegment', 'clearSegment');
  wrap(game.navigation, 'findPath', 'pathRequests');
  wrap(game.navigation, 'move', 'navigationMove');
  for (const name of ['updateApe', 'spreadApes', 'updateHuman', 'updateBullets', 'tickSecond']) wrap(game, name, name);
  return measures;
}

function percentile(values, fraction) { return values.slice().sort((a, b) => a - b)[Math.min(values.length - 1, Math.floor(values.length * fraction))] || 0; }
function runScenario(context, scenario, frames = 360, profile = false) {
  const g = setupScenario(context, scenario);
  const measures = profile ? instrument(g) : {};
  const times = [], initialChunks = g.world.chunks.size;
  let peakShells=0,peakCannonWarnings=0,peakGrenades=0,peakBullets = 0, peakEffects = 0, maxSearches = 0, maxThink = 0, maxLos = 0, maxStreamSteps = 0, maxExpanded = 0, maxQueue = 0;
  const initialHealth = g.apes.reduce((n, a) => n + a.hp, 0) + g.humans.reduce((n, h) => n + h.hp, 0);
  for (let frame = 0; frame < frames; frame++) {
    if (scenario.exploration) {
      // Advance one chunk every half-second to exercise queue saturation.
      if (frame % 30 === 0) { g.king.x += g.world.chunkSize; g.king.y += g.world.chunkSize / 3; }
    }
    const previousSearches = g.navigation.stats.searches;
    const start = performance.now();
    g.update(1 / 60, {});
    times.push(performance.now() - start);
    maxSearches = Math.max(maxSearches, g.navigation.stats.searches - previousSearches);
    maxExpanded = Math.max(maxExpanded, g.navigation.stats.frameExpanded || 0);
    maxQueue = Math.max(maxQueue, g.navigation.stats.queueLength || 0);
    maxThink = Math.max(maxThink, g.performance?.counters.aiThinks || 0);
    maxLos = Math.max(maxLos, g.performance?.counters.losTests || 0);
    maxStreamSteps = Math.max(maxStreamSteps, g.world.stats?.streamSteps || 0);
    peakShells=Math.max(peakShells,g.forces.hazards.filter(h=>h.type==='shell').length);peakGrenades=Math.max(peakGrenades,g.forces.hazards.filter(h=>h.type==='grenade').length);peakCannonWarnings=Math.max(peakCannonWarnings,g.vehicles.filter(v=>v.cannonTarget).length);
    peakBullets = Math.max(peakBullets, g.bullets.length);
    peakEffects = Math.max(peakEffects, g.effects.length);
    if (g.ended) throw new Error('Stress scene ended unexpectedly: ' + scenario.id);
  }
  const health = g.apes.reduce((n, a) => n + a.hp, 0) + g.humans.reduce((n, h) => n + h.hp, 0);
  const sample = Math.min(60, times.length), average = list => list.reduce((a, b) => a + b, 0) / list.length;
  return { game: g, report: { id: scenario.id, label: scenario.label, frames, meanMs: average(times), firstWindowMeanMs: average(times.slice(0, sample)), lastWindowMeanMs: average(times.slice(-sample)), p95Ms: percentile(times, .95), maxMs: Math.max(...times), framesOver50Ms: times.filter(t => t > 50).length, maxSearches, maxExpanded, maxQueue, maxThink, maxLos, maxStreamSteps, searches: g.navigation.stats.searches, cacheHits: g.navigation.stats.cacheHits, initialApes: scenario.apes, apes: g.apes.length, humans: g.humans.length, vehicles: g.vehicles.length, helis: g.helis.length, peakShells,peakCannonWarnings,peakGrenades,peakBullets, peakEffects, chunksAdded: g.world.chunks.size - initialChunks, damage: initialHealth - health, methods: measures } };
}
module.exports = { MODULES, SCENARIOS, loadEngine, setupScenario, runScenario, percentile };
