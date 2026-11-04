'use strict';
// Reproducible simulation benchmark, without browser rendering or test observers.
// Run by itself: node tests/performance-test.cjs [project-root] [seconds-per-case]
// Timing is diagnostic, not a machine-dependent correctness gate or an FPS claim.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { performance } = require('node:perf_hooks');
const root = path.resolve(process.argv[2] || path.join(__dirname, '..'));
const seconds = Math.max(1, Number(process.argv[3]) || 30);
const selected = process.env.IRONFIELD_PERF_PRESET;
if (!selected) {
  // A fresh VM for each case avoids prior maps changing V8's specialization
  // and makes the before/after cases reproducible in the same order.
  const { spawnSync } = require('node:child_process');
  const cases = ['charge', 'basin', 'epic', 'grand40k'].map(id => {
    const run = spawnSync(process.execPath, [__filename, root, String(seconds)], {
      env: { ...process.env, IRONFIELD_PERF_PRESET: id }, encoding: 'utf8'
    });
    assert.equal(run.status, 0, run.stderr || run.stdout || 'Benchmark child failed');
    return JSON.parse(run.stdout);
  });
  console.log(JSON.stringify({ ...cases[0], isolation: 'One fresh Node process per scenario',
    results: cases.flatMap(result => result.results) }, null, 2));
  process.exit(0);
}
global.window = global;
require(path.join(root, 'src/data.js'));
if (fs.existsSync(path.join(root, 'src/tactics.js'))) require(path.join(root, 'src/tactics.js'));
for (const name of ['impacts', 'siege', 'ramparts']) {
  const file = path.join(root, 'src', name + '.js');
  if (require('node:fs').existsSync(file)) require(file);
}
require(path.join(root, 'src/engine.js'));
const round = n => Math.round(n * 1000) / 1000;
const percentile = (xs, fraction) => xs[Math.min(xs.length - 1, Math.floor(xs.length * fraction))];
const phases = times => {
  const sorted = [...times].sort((a, b) => a - b);
  return { steps: times.length, meanMS: round(times.reduce((a, b) => a + b, 0) / times.length),
    medianMS: round(percentile(sorted, .5)), p95MS: round(percentile(sorted, .95)), maximumMS: round(sorted.at(-1)) };
};
const results = [];
for (const id of selected ? [selected] : ['charge', 'basin', 'epic']) {
  const source = id === 'grand40k' || id === 'grand40k-contact' ? {
    map: 'custom', seed: 430247,
    customMap: { width: id === 'grand40k' ? 16000 : 6400, height: id === 'grand40k' ? 11000 : 4400, biome: 'grassland', regions: [] },
    armies: [0,1].map(() => ({ roster: { swordsman: 10000, pikeman: 5000, archer: 3000, cavalry: 2000 } }))
  } : GameData.PRESETS.find(p => p.id === id);
  assert(source, 'Unknown benchmark preset ' + id);
  const e = new BattleEngine(JSON.parse(JSON.stringify(source)));
  const start = performance.now(); e.start(); const startMS = performance.now() - start;
  const times = [], buckets = [], fastCounts = [];
  for (let tick = 0; tick < Math.round(seconds * 30) && e.phase === 'running'; tick++) {
    const began = performance.now(); e.step(1 / 30); times.push(performance.now() - began);
    fastCounts.push(e.fastMarchUnits || 0);
    if ((tick + 1) % 300 === 0 || tick + 1 === Math.round(seconds * 30)) buckets.push({ throughSeconds: round(e.time), ...phases(times.slice(Math.floor(tick / 300) * 300)) });
  }
  for (const u of e.units) {
    assert(Number.isFinite(u.x) && Number.isFinite(u.y) && Number.isFinite(u.hp));
    if (u.alive && !u.routed) assert(u.x >= 0 && u.x <= e.width && u.y >= 0 && u.y <= e.height);
  }
  const teams = e.getStats().teams;
  for (const team of teams) assert.equal(team.initial, team.alive + team.dead + team.routed);
  const result = { preset: id, map: e.config.map, seed: e.seed, units: e.units.length,
    formations: e.regiments.length, simulatedSeconds: round(e.time), startMS: round(startMS),
    ...phases(times), buckets, casualties: teams.map(t => t.dead),
    meanCertifiedMarchSoldiers: round(fastCounts.reduce((sum,n)=>sum+n,0)/Math.max(1,fastCounts.length)),
    peakCertifiedMarchSoldiers: Math.max(0,...fastCounts),
    limitation: id === 'grand40k' ? 'Colossal open-ground approach; this does not measure dense melee, siege traffic, browser rendering, or guarantee 30 FPS.' : id === 'grand40k-contact' ? 'Dense Standard-size deployment; inspect casualties/attacks to distinguish approach from actual fighting.' : 'Authored scenario simulation.',
    soldiersWhoAttacked: e.units.filter(u=>u._lastAttack>0).length,
    physicalStateSHA: crypto.createHash('sha256').update(JSON.stringify(e.units.map(u => [u.x, u.y, u.hp, u.alive]))).digest('hex') };
  results.push(result);
}
const files = ['data.js', 'tactics.js', 'impacts.js', 'siege.js', 'engine.js'].filter(name => fs.existsSync(path.join(root, 'src', name)));
const sourceHashes = Object.fromEntries(files.map(name => [name, crypto.createHash('sha256').update(fs.readFileSync(path.join(root, 'src', name))).digest('hex')]));
console.log(JSON.stringify({ root, node: process.version, secondsPerCase: seconds, sourceHashes,
  limitation: 'Node simulation timings only; run alone on the same host. Includes natural strategy, traffic, movement, combat and morale work, but excludes browser drawing and UI.', results }, null, 2));
