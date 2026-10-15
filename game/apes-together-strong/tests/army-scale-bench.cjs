'use strict';
const fs = require('node:fs'), path = require('node:path'), os = require('node:os');
const { performance } = require('node:perf_hooks');
const { loadEngine, runScenario } = require('./performance-harness.cjs');
const args = process.argv.slice(2), value = name => args.includes(name) ? args[args.indexOf(name) + 1] : undefined;
const baseline = value('--baseline'), output = value('--output'), frames = Number(value('--frames') || 180);
const sources = baseline ? [['before', path.resolve(baseline)], ['after', path.resolve(__dirname, '..')]] : [['after', path.resolve(__dirname, '..')]];
const scenarios = [50, 100, 250, 500, 1000].map(apes => ({ id: `follow-${apes}`, label: `${apes} real followers in generated dense forest`, apes, humans: 0 }));
scenarios.push({ id: 'battle-250', label: '250 charging apes against 150 armed humans', apes: 250, humans: 150, combat: true }, { id: 'battle-500', label: '500 apes against tanks, armored vehicles, infantry and explosives', apes: 500, humans: 250, combat: true, tanks: 4, armored: 6, helis: 2, alarm: true, military: true, mortars: true });
const document = { kind: 'simulation-only matched real-actor scale matrix; inclusive method timing overlaps; initial cold start included', node: process.version, cpu: os.cpus()[0]?.model, frames, step: '1/60 second', seed: 'FOREST-A', randomSeed: '0x923cd91', memory: 'process heap deltas include caches and generated geometry, not retained-memory leak measurements', reports: [] };
for (const scenario of scenarios) for (const [version, directory] of sources) {
  global.gc?.(); const initialHeap = process.memoryUsage().heapUsed;
  const { game, report } = runScenario(loadEngine(directory), scenario, frames, true);
  const heapDeltaBytes = process.memoryUsage().heapUsed - initialHeap;
  game.commandCD = 0; const start = performance.now(); const commandAccepted = game.command('charge', { x: 1, y: 0 }); const commandMs = commandAccepted ? performance.now() - start : null;
  document.reports.push({ version, ...report, heapDeltaBytes, commandAccepted, commandMs, formationContractions: game.navigation.stats.formationContractions || 0, separationFastHits: game.navigation.stats.separationFastHits || 0 });
  if (output) fs.writeFileSync(output, JSON.stringify(document, null, 2));
  console.log(`${version} ${scenario.id}: mean ${report.meanMs.toFixed(2)}ms, p95 ${report.p95Ms.toFixed(2)}ms, A* ${report.searches}, shared ${report.sharedHits}, charge ${commandMs === null ? 'deferred during recovery' : commandMs.toFixed(2) + 'ms'}`);
}
