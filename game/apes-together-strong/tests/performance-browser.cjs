/* Optional real-canvas A–L benchmark, including 1,000 apes and extreme war. */
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { chromium } = require('playwright');
const { MODULES, SCENARIOS, setupScenario } = require('./performance-harness.cjs');
const args = process.argv.slice(2);
const value = name => args.includes(name) ? args[args.indexOf(name) + 1] : undefined;
const directory = path.resolve(value('--source') || path.join(__dirname, '..'));
const frames = Number(value('--frames') || 120);
const only = value('--scenario');

(async () => {
  const browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}), args: ['--disable-dev-shm-usage'] });
  const reports = [], errors = [];
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 });
    page.on('pageerror', e => errors.push(e.message));
    await page.setContent('<style>html,body{margin:0;background:#071216}canvas{display:block;width:1440px;height:900px}</style><canvas id="game"></canvas>');
    await page.addScriptTag({ content: 'window.resetStressRandom=()=>{let randomSeed=0x923cd91;Math.random=()=>{randomSeed=Math.imul(1664525,randomSeed)+1013904223|0;return(randomSeed>>>0)/4294967296}};resetStressRandom();' });
    for (const name of [...MODULES, 'render', 'render-details']) await page.addScriptTag({ content: fs.readFileSync(path.join(directory, name + '.js'), 'utf8') });
    await page.addScriptTag({ content: 'window.setupStressScene=' + setupScenario.toString() + ';' });
    for (const scenario of SCENARIOS.filter(s => !only || only.includes(s.id))) {
      const report = await page.evaluate(async ({ scenario, frames }) => {
        window.resetStressRandom();
        const g = window.setupStressScene(window, scenario), renderer = new ATSRenderer(document.getElementById('game'));
        renderer.quality = 'high';
        const simulation = [], render = [], intervals = [], total = [];
        let previous, peakVisible = 0, maxSearches = 0, maxRenderLos = 0, maxRenderRays = 0, peakMortars = 0, peakGrenades = 0, peakAbstractApes = 0;
        for (let i = 0; i < frames; i++) {
          const time = await new Promise(resolve => requestAnimationFrame(resolve));
          if (previous !== undefined) intervals.push(time - previous);
          previous = time;
          if (scenario.exploration && i % 30 === 0) { g.king.x += g.world.chunkSize; g.king.y += g.world.chunkSize / 3; }
          const oldSearches = g.navigation.stats.searches;
          const start = performance.now();
          g.update(1 / 60, {});
          const afterSim = performance.now();
          renderer.draw(g, 1 / 60);
          const finish = performance.now();
          g.performance?.frame(finish - start, afterSim - start, finish - afterSim, 1 / 60, true);
          simulation.push(afterSim - start); render.push(finish - afterSim); total.push(finish - start);
          maxSearches = Math.max(maxSearches, g.navigation.stats.searches - oldSearches);
          peakVisible = Math.max(peakVisible, g.performance?.counters.visibleActors || renderer.stats?.visibleActors || 0);
          maxRenderLos = Math.max(maxRenderLos, g.performance?.counters.renderLosTests || 0);
          maxRenderRays = Math.max(maxRenderRays, g.performance?.counters.renderRays || 0);
          peakMortars = Math.max(peakMortars, g.forces.hazards.filter(h => h.type === 'mortar').length);
          peakGrenades = Math.max(peakGrenades, g.forces.hazards.filter(h => h.type === 'grenade').length);
          peakAbstractApes = Math.max(peakAbstractApes, g.apes.filter(a => a._simTier === 2).length);
        }
        const stats = list => ({ meanMs: list.reduce((a, b) => a + b, 0) / list.length, p95Ms: list.slice().sort((a, b) => a - b)[Math.floor(list.length * .95)], maxMs: Math.max(...list), over50Ms: list.filter(ms => ms > 50).length });
        window.stressGame = g;
        return { id: scenario.id, label: scenario.label, frames, simulation: stats(simulation), render: stats(render), work: stats(total), animationFrameInterval: stats(intervals), maxSearches, maxRenderLos, maxRenderRays, peakVisible, peakMortars, peakGrenades, peakAbstractApes, qualityLevel: g.performance?.qualityLevel || 0, apes: g.apes.length, followers: g.followers.length, settled: g.apes.filter(a => a.settlementId && a.hp > 0).length, humans: g.humans.length, tanks: g.vehicles.filter(v => v.vehicleClass === 'tank').length, armored: g.vehicles.filter(v => ['apc', 'ifv'].includes(v.vehicleClass)).length, helis: g.helis.length, errors: g.ended ? ['game ended'] : [] };
      }, { scenario, frames });
      reports.push(report);
      assert.deepEqual(report.errors, []);
      assert.ok(report.maxSearches <= 3, 'A* request frame budget');
      assert.ok(report.maxRenderLos <= 96, 'rendering line-of-sight frame budget');
      assert.ok(report.maxRenderRays <= 96, 'rendering ray frame budget');
      assert.equal(report.apes, scenario.apes, 'render detail changes preserve all real apes');
      assert.equal(report.humans, scenario.humans);
      if (scenario.settled) { assert.equal(report.followers, scenario.following); assert.equal(report.settled, scenario.settled); }
      if (scenario.military) { assert.equal(report.tanks, scenario.tanks); assert.equal(report.armored, scenario.armored); }
      if (scenario.mortars) { assert.ok(report.peakMortars > 0); assert.ok(report.peakGrenades > 0); }
      console.log(`${report.id}: simulation ${report.simulation.meanMs.toFixed(2)}ms, render ${report.render.meanMs.toFixed(2)}ms; total p95 ${report.work.p95Ms.toFixed(2)}ms, max ${report.work.maxMs.toFixed(2)}ms; rAF mean ${report.animationFrameInterval.meanMs.toFixed(2)}ms`);
      if (process.env.QA_ARTIFACT_DIR) {
        fs.mkdirSync(process.env.QA_ARTIFACT_DIR, { recursive: true });
        await page.screenshot({ path: path.join(process.env.QA_ARTIFACT_DIR, 'stress-' + scenario.id + '.png') });
      }
    }
    assert.deepEqual(errors, []);
    const output = value('--output');
    if (output) fs.writeFileSync(output, JSON.stringify({ kind: 'real canvas, headless Chromium, 1440x900 DPR1; synthetic sustained combat health', browser: browser.version(), reports }, null, 2));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
