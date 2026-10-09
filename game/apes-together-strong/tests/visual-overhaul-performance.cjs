/* Actual Canvas benchmark. Uses the same source order as the playable bundle. */
'use strict';
const fs = require('node:fs'), path = require('node:path'), assert = require('node:assert/strict');
const os = require('node:os');
const { chromium } = require('playwright');
const { SCENARIOS, setupScenario } = require('./performance-harness.cjs');
const args = process.argv.slice(2);
const option = (name, fallback) => args.includes(name) ? args[args.indexOf(name) + 1] : fallback;
const directory = path.resolve(option('--source', path.join(__dirname, '..')));
const frames = Number(option('--frames', 180)), only = option('--scenario', 'AGK');
const quality = option('--quality', 'high');
const fixedQuality = args.includes('--fixed-quality');
const profile = args.includes('--profile');
const viewport = { width: Number(option('--width', 1440)), height: Number(option('--height', 900)) };
const build = fs.readFileSync(path.join(directory, 'build.cjs'), 'utf8');
const names = [...build.match(/const parts=\[([^\]]+)\]/)[1].matchAll(/'([^']+)'/g)].map(m => m[1]).filter(n => n !== 'app' && !n.endsWith('-ui'));

(async () => {
  const browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}), args: ['--disable-dev-shm-usage', '--enable-precise-memory-info'] });
  const reports = [], errors = [];
  try {
    const page = await browser.newPage({ viewport, deviceScaleFactor: 1 });
    page.on('pageerror', e => errors.push(e.message));
    await page.setContent(`<style>html,body{margin:0;background:#071216}canvas{display:block;width:${viewport.width}px;height:${viewport.height}px}</style><canvas id="game"></canvas>`);
    await page.addScriptTag({ content: 'window.resetStressRandom=()=>{let seed=0x923cd91;Math.random=()=>{seed=Math.imul(1664525,seed)+1013904223|0;return(seed>>>0)/4294967296}};resetStressRandom();' });
    if (names.includes('visual-assets')) {
      const manifest = {}, sources = {}, root = path.join(directory, 'assets', 'visual');
      for (const family of fs.readdirSync(root).filter(name => fs.existsSync(path.join(root, name, 'manifest.json')))) {
        manifest[family] = JSON.parse(fs.readFileSync(path.join(root, family, 'manifest.json'), 'utf8'));
        for (const atlas of manifest[family].atlases) sources[atlas.id] = 'data:image/' + path.extname(atlas.file).slice(1) + ';base64,' + fs.readFileSync(path.join(root, atlas.file)).toString('base64');
      }
      await page.evaluate(bundle => { window.ATS_VISUAL_BUNDLE = bundle; }, { manifest, sources });
    }
    for (const name of names) await page.addScriptTag({ content: fs.readFileSync(path.join(directory, name + '.js'), 'utf8') });
    await page.evaluate(async () => { if (window.ATSVisualAssets) { await ATSVisualAssets.ready; if (ATSVisualAssets.status !== 'ready') throw new Error('Incomplete art preload: ' + ATSVisualAssets.failures); } });
    await page.addScriptTag({ content: 'window.setupStressScene=' + setupScenario.toString() + ';' });
    const scenarios = SCENARIOS.concat({ id: 'N', label: 'King alone in procedural forest', apes: 0, humans: 0 }, { id: 'O', label: '240 connected fortress wall and gate modules across four tiers', apes: 0, humans: 0, walls: true });
    for (const scenario of scenarios.filter(s => only.includes(s.id))) {
      const report = await page.evaluate(async ({ scenario, frames, quality, fixedQuality, profile }) => {
        resetStressRandom();
        const g = setupStressScene(window, scenario), r = new ATSRenderer(document.getElementById('game'));
        r.quality = quality;
        const methods = {};
        if(profile)for(const name of ['drawGround','drawGroundChunk','drawLights','drawApe','drawHuman','drawVehicle','drawHeli','drawEffects','drawAtmosphere','drawSettlement','drawConstruction','drawObject']){
          const original=r[name];if(typeof original!=='function')continue;const entry=methods[name]={calls:0,ms:0};
          r[name]=function(...args){const start=performance.now();entry.calls++;try{return original.apply(this,args)}finally{entry.ms+=performance.now()-start}};
        }
        if(scenario.walls){
          g.king.x=g.king.y=0;g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();
          g.world.ensure=()=>{};g.world.stream=()=>{};g.world._streaming=false;
          g.world.terrain=()=>({biome:'forest',water:false});g.world.getSites=()=>[];
          let index=0;
          for(let castle=0;castle<4;castle++)for(let edge=0;edge<4;edge++)for(let segment=0;segment<15;segment++){
            const horizontal=edge%2===0,along=-112+segment*16,cx=castle%2?220:-220,cy=castle<2?-180:180;
            const x=cx+(horizontal?along:edge===1?120:-120),y=cy+(horizontal?edge===0?-120:120:along);
            const gate=index%20===10,id='qa-fortress-'+index++;
            const object={id,type:gate?'gate':'wall',x,y,w:horizontal?16:8,h:horizontal?8:16,hp:500,maxHp:500,wallTier:castle+1,collision:'rect',solid:true,dead:false,gateState:gate?'closed':undefined};
            g.world.objects.set(id,object);g.world._indexObject(object);
          }
          g.world.navRevision=(g.world.navRevision||0)+1;
        }
        const sim = [], render = [], work = [], intervals = [];
        let previous, peakVisible = 0, maxSearches = 0, maxLos = 0, maxRays = 0, maxParticles = 0;
        const initialHeapBytes = performance.memory?.usedJSHeapSize ?? null;
        let peakHeapBytes = initialHeapBytes, heapDrops = 0, largestHeapDropBytes = 0, previousHeap = initialHeapBytes;
        const initialPopulation = g.apes.length;
        for (let i = 0; i < frames; i++) {
          const stamp = await new Promise(requestAnimationFrame);
          if (previous !== undefined) intervals.push(stamp - previous);
          previous = stamp;
          if (scenario.exploration && i % 30 === 0) { g.king.x += g.world.chunkSize; g.king.y += g.world.chunkSize / 3; }
          const searches = g.navigation.stats.searches, start = performance.now();
          if(fixedQuality)g.performance.qualityLevel=0;
          g.update(1 / 60, {});
          const after = performance.now();
          r.draw(g, 1 / 60);
          const finish = performance.now();
          g.performance?.frame(finish - start, after - start, finish - after, 1 / 60, true);
          sim.push(after - start); render.push(finish - after); work.push(finish - start);
          maxSearches = Math.max(maxSearches, g.navigation.stats.searches - searches);
          maxLos = Math.max(maxLos, g.performance?.counters.renderLosTests || 0);
          maxRays = Math.max(maxRays, g.performance?.counters.renderRays || 0);
          peakVisible = Math.max(peakVisible, r.stats?.visibleActors || g.performance?.counters.visibleActors || 0);
          maxParticles = Math.max(maxParticles, r._environmentArt?.active || 0);
          const heap = performance.memory?.usedJSHeapSize;
          if(heap!==undefined){peakHeapBytes=Math.max(peakHeapBytes,heap);if(heap<previousHeap-1024*1024){heapDrops++;largestHeapDropBytes=Math.max(largestHeapDropBytes,previousHeap-heap)}previousHeap=heap}
        }
        const stats = a => ({ meanMs: a.reduce((s, n) => s + n, 0) / a.length, p95Ms: a.slice().sort((a, b) => a - b)[Math.floor(a.length * .95)], maxMs: Math.max(...a), over50Ms: a.filter(n => n > 50).length });
        return { id: scenario.id, label: scenario.label, frames, quality, fixedQuality, methods, initialPopulation, apes: g.apes.length, humans: g.humans.length, ended: g.ended, simulation: stats(sim), render: stats(render), work: stats(work), animationFrameInterval: stats(intervals), memory: { initialHeapBytes, peakHeapBytes, finalHeapBytes: previousHeap, heapDrops, largestHeapDropBytes, note: 'Heap drops are allocation/collection observations, not direct GC pause measurements.' }, maxSearches, maxLos, maxRays, maxParticles, peakVisible, qualityLevel: g.performance?.qualityLevel || 0, characterArtworkDraws: r.characterArtworkDraws || 0, particlePoolCapacity: r._environmentArt?.pool?.length || 0, sharedCoatAtlases: r.characterCoats?.size || 0, cachedTerrainPixels: r.groundPixels || 0, wallTextureCount:r._environmentArt?.wallTextures?.size||0,wallTexturePixels:r._environmentArt?.wallTexturePixels||0,fortressModules:scenario.walls?g.world.objects.size:0 };
      }, { scenario, frames, quality, fixedQuality, profile });
      reports.push(report);
      assert.equal(report.apes, scenario.apes, 'all real apes retained');
      assert.equal(report.humans, scenario.humans, 'all real humans retained');
      assert.equal(report.ended, false);
      assert.ok(report.maxSearches <= 3 && report.maxLos <= 96 && report.maxRays <= 96, 'bounded navigation and lighting work');
      if (names.includes('character-art')) assert.ok(report.characterArtworkDraws > 0, 'authored sprites are actually drawn by the benchmark');
      assert.ok(report.particlePoolCapacity <= 96, `particle pool capacity exceeded: ${report.particlePoolCapacity}`);
      assert.ok(report.maxParticles <= 96, `visible artwork particles exceeded: ${report.maxParticles}`);
      assert.ok(report.sharedCoatAtlases <= 4, `shared coat atlases exceeded the two variations × two ape sheets: ${report.sharedCoatAtlases}`);
      assert.ok(report.wallTextureCount<=128&&report.wallTexturePixels<=2000000,'wall textures respect both cache bounds');
      if(scenario.walls)assert.equal(report.fortressModules,240,'connected fortress modules remain present');
      console.log(`${report.id} (${quality}): simulation ${report.simulation.meanMs.toFixed(2)}ms, render ${report.render.meanMs.toFixed(2)}ms; work p95 ${report.work.p95Ms.toFixed(2)}ms; rAF ${report.animationFrameInterval.meanMs.toFixed(2)}ms`);
      if (process.env.QA_ARTIFACT_DIR) { fs.mkdirSync(process.env.QA_ARTIFACT_DIR, { recursive: true }); await page.screenshot({ path: path.join(process.env.QA_ARTIFACT_DIR, `stress-${scenario.id}-${quality}.png`) }); }
    }
    assert.deepEqual(errors, []);
    if (option('--output')) fs.writeFileSync(option('--output'), JSON.stringify({ kind: 'real Canvas, headless Chromium; full production artwork; synthetic sustained combat health', browser: browser.version(), cpu: os.cpus()[0]?.model, logicalProcessors: os.cpus().length, node: process.version, os: os.release(), viewport, dpr: 1, seed: 'FOREST-A', randomSeed: '0x923cd91', warmupFrames: 0, includesColdStart: true, frames, quality, fixedQuality, sources: names, reports }, null, 2));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
