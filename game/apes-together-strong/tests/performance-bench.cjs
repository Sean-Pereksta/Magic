'use strict';
const fs = require('node:fs');
const { SCENARIOS, loadEngine, runScenario } = require('./performance-harness.cjs');
const args = process.argv.slice(2);
const value = name => args.includes(name) ? args[args.indexOf(name) + 1] : undefined;
const frames = Number(value('--frames') || 360);
const only = value('--scenario');
const reports = [];
for (const scenario of SCENARIOS.filter(s => !only || only.includes(s.id))) {
  const { report } = runScenario(loadEngine(value('--source')), scenario, frames, args.includes('--profile'));
  reports.push(report);
  console.log(`${report.id}: mean ${report.meanMs.toFixed(2)} ms, p95 ${report.p95Ms.toFixed(2)} ms, max ${report.maxMs.toFixed(2)} ms; A* ${report.searches}; ${report.apes} apes / ${report.humans} humans; ${report.chunksAdded} new chunks`);
  if (frames >= 600) console.log(`First/last 60-tick means: ${report.firstWindowMeanMs.toFixed(2)} / ${report.lastWindowMeanMs.toFixed(2)} ms; peak bullets ${report.peakBullets}, effects ${report.peakEffects}, queue ${report.maxQueue}; peak A* starts ${report.maxSearches}, expansions ${report.maxExpanded}, AI ${report.maxThink}, LOS ${report.maxLos}`);
  if (args.includes('--profile')) console.table(Object.entries(report.methods).map(([name, m]) => ({ name, calls: m.calls, inclusiveMs: Math.round(m.ms) })).sort((a, b) => b.inclusiveMs - a.inclusiveMs));
}
const output = value('--output');
if (output) fs.writeFileSync(output, JSON.stringify({ kind: 'simulation-only; inclusive method timing overlaps; stable combat populations use high health', node: process.version, platform: process.platform, reports }, null, 2));
