// Real WebGL integration checks. Closure hooks are injected only by this test server.
// NEON_ASSET_FIXTURES=/path/to/original-pack node tests/neon-tiles-browser.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const { chromium } = require('playwright');
const repo = path.resolve(__dirname, '..'), fixtures = process.env.NEON_ASSET_FIXTURES;
const atlasFor = { grid: 'breach_ring', pastel: 'black_ice_cryolab', reactor: 'drowned_powerworks', terminal: 'derelict_station', foundry: 'drowned_powerworks', outpost: 'xenofall_wastes' };
const hooks = `
window.__NEON_TILES_TEST__ = {game,player,get player2(){return player2},get bots(){return bots},get walls(){return walls},get platforms(){return platforms},get map(){return survivalMapRuntime},get progress(){return survivalProgress},get models(){return neonModelRenderer},get gl(){return gl},draw,equip,updateHud,markBlocksDirty,makeStructurePiece,importedModelCamera,makeAlien,rayEntity};
const tileQA=window.__NEON_TILES_TEST__,originalSurfaceRender=survivalMapRuntime.renderSurfaces.bind(survivalMapRuntime);
tileQA.surfaceCalls=0;survivalMapRuntime.renderSurfaces=function(...args){tileQA.surfaceCalls++;return originalSurfaceRender(...args)};
${process.env.NEON_TILES_TRACE ? `tileQA.glErrors=[];const qaNativeDraw=gl.drawArrays.bind(gl);gl.drawArrays=function(...args){const program=gl.getParameter(gl.CURRENT_PROGRAM),before=gl.getError();qaNativeDraw(...args);const after=gl.getError();if(before||after){const attribs=[];for(let i=0;i<gl.getProgramParameter(program,gl.ACTIVE_ATTRIBUTES);i++){const a=gl.getActiveAttrib(program,i),index=gl.getAttribLocation(program,a.name);attribs.push({name:a.name,index,enabled:gl.getVertexAttrib(index,gl.VERTEX_ATTRIB_ARRAY_ENABLED),stride:gl.getVertexAttrib(index,gl.VERTEX_ATTRIB_ARRAY_STRIDE),size:gl.getVertexAttrib(index,gl.VERTEX_ATTRIB_ARRAY_SIZE),offset:gl.getVertexAttribOffset(index,gl.VERTEX_ATTRIB_ARRAY_POINTER)});}tileQA.glErrors.push({before,after,args,attribs,stack:new Error().stack});}};` : ''}
`;

function serve(req, res) {
  try {
    const pathname = new URL(req.url, 'http://localhost').pathname;
    let file, allowed = repo;
    if (/^\/fixture-assets\/characters\/v3\/[\w]+\.(glb|webp)$/.test(pathname)) {
      file = path.join(repo, 'game/neon-arena/assets/characters', path.basename(pathname));
    } else if (pathname.startsWith('/fixture-assets/') && fixtures) {
      allowed = path.resolve(fixtures);
      file = path.resolve(allowed, '.' + pathname.slice('/fixture-assets'.length));
    } else file = path.resolve(repo, '.' + (pathname.endsWith('/') ? pathname + 'index.html' : pathname));
    if (!file.startsWith(allowed + path.sep)) throw Error('Outside test root');
    let data = fs.readFileSync(file);
    if (path.basename(file) === 'neon-arena.js') data = Buffer.from(data.toString().replace(/\}\)\(\);\s*$/, hooks + '})();'));
    const type = { '.js': 'text/javascript', '.mjs': 'text/javascript', '.json': 'application/json', '.css': 'text/css', '.html': 'text/html', '.svg': 'image/svg+xml', '.glb': 'model/gltf-binary', '.png': 'image/png', '.webp': 'image/webp' }[path.extname(file)] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': type, 'Access-Control-Allow-Origin': '*' }); res.end(data);
  } catch (_) { res.writeHead(404); res.end('Not found'); }
}

async function inspectSurfaces(page, label) {
  const result = await page.evaluate(() => {
    const t = __NEON_TILES_TEST__, beforeCalls = t.surfaceCalls;
    t.draw();
    const initial = t.map.surfaceStats(t.gl), passes = t.surfaceCalls - beforeCalls;
    const wall = t.makeStructurePiece('wall', { x: 25, z: 25, y: 1.5, w: 5.8, d: .4, h: 3, c: '#65f7ff', builtBy: 'p1' });
    const platform = t.makeStructurePiece('platform', { x: 25, z: 25, y: 3, w: 5.8, d: 5.8, h: .35, c: '#75ddff', builtBy: 'p1' });
    t.walls.push(wall); t.platforms.push(platform); t.markBlocksDirty(); t.draw();
    const added = t.map.surfaceStats(t.gl);
    t.draw(); const unchanged = t.map.surfaceStats(t.gl);
    wall.active = false; t.markBlocksDirty(); t.draw(); const removedWall = t.map.surfaceStats(t.gl);
    platform.active = false; t.markBlocksDirty(); t.draw(); const removedBoth = t.map.surfaceStats(t.gl);
    return { initial, added, unchanged, removedWall, removedBoth, passes, error: t.gl.getError(), tracedErrors: t.glErrors, atlas: t.map.current?.id, loaded: t.map.loaded, ready: t.map.canRenderSurfaces, expectedPieces: [...t.walls, ...t.platforms].filter(p => p.active !== false && !p.survivalEnvironment).length };
  });
  assert.equal(result.loaded, true, label + ': atlas loaded');
  assert.equal(result.ready, true, label + ': generic surface renderer ready');
  assert.equal(result.error, 0, label + ': no WebGL errors');
  assert.ok(!result.tracedErrors?.length, label + ': traced WebGL errors ' + JSON.stringify(result.tracedErrors?.slice(-3)));
  assert.ok(result.passes >= 1, label + ': arena invokes textured surface rendering');
  assert.equal(result.initial.pieces, result.expectedPieces, label + ': all active arena structures enter textured rendering');
  assert.equal(result.added.pieces, result.initial.pieces + 2, label + ': built wall and platform receive texture geometry');
  assert.ok(result.added.drawCalls > 0, label + ': textured build chunks actually draw');
  assert.ok(result.added.uploads > result.initial.uploads, label + ': additions upload affected chunks');
  assert.equal(result.unchanged.geometryBuilds, result.added.geometryBuilds, label + ': unchanged frame reuses geometry');
  assert.equal(result.unchanged.uploads, result.added.uploads, label + ': unchanged frame reuses GPU buffers');
  assert.equal(result.removedWall.pieces, result.initial.pieces + 1, label + ': destroyed wall disappears');
  assert.equal(result.removedBoth.pieces, result.initial.pieces, label + ': destroyed platform disappears');
  assert.ok(result.removedWall.uploads > result.added.uploads, label + ': destruction replaces the affected texture chunk');
  assert.ok(result.removedBoth.vertices < result.added.vertices, label + ': destroyed geometry leaves GPU buffers');
  return { atlas: result.atlas, pieces: result.initial.pieces, passes: result.passes };
}

async function inspectAtlasPixels(page, label) {
  const result = await page.evaluate(() => {
    const t = __NEON_TILES_TEST__, canvas = document.createElement('canvas');
    canvas.width = canvas.height = 128;
    const gl = canvas.getContext('webgl', { antialias: false, preserveDrawingBuffer: true });
    if (!gl) throw Error('Real WebGL unavailable for texture pixel probe');
    gl.viewport(0, 0, 128, 128); gl.enable(gl.DEPTH_TEST); gl.depthFunc(gl.LEQUAL); gl.disable(gl.BLEND); gl.disable(gl.CULL_FACE); gl.clearColor(0, 0, 0, 1); gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);
    const camera = { position: [0, 1, 0], right: [1, 0, 0], up: [0, 1, 0], forward: [0, 0, 1], aspect: 1 };
    const piece = { id: 'texture-pixel-probe', type: 'wall', x: 0, y: 1, z: 4, w: 3, h: 2.5, d: .2, c: '#ffffff', active: true };
    const rendered = t.map.renderSurfaces(gl, camera, [piece], { time: 0, quality: 'high' });
    const pixels = new Uint8Array(64 * 64 * 4); gl.readPixels(32, 32, 64, 64, gl.RGBA, gl.UNSIGNED_BYTE, pixels);
    const colors = new Set(); let lit = 0;
    for (let i = 0; i < pixels.length; i += 4) if (pixels[i] + pixels[i + 1] + pixels[i + 2] > 6) { lit++; colors.add([pixels[i] >> 2, pixels[i + 1] >> 2, pixels[i + 2] >> 2].join(',')); }
    return { rendered, colors: colors.size, lit, error: gl.getError() };
  });
  assert.equal(result.rendered, true, label + ': atlas probe draws');
  assert.equal(result.error, 0, label + ': texture sampler works on real GPU');
  assert.ok(result.lit > 3000, label + ': material covers the sampled wall');
  assert.ok(result.colors > 30, label + ': material has actual atlas detail, not a flat color');
  return result.colors;
}

async function inspectRaptors(page, label) {
  const results = await page.evaluate(() => {
    const t = __NEON_TILES_TEST__, canvas = document.createElement('canvas'); canvas.width = canvas.height = 128;
    const gl = canvas.getContext('webgl', { antialias: false, preserveDrawingBuffer: true });
    if (!gl) throw Error('Real WebGL unavailable for raptor probe');
    const renderer = NeonModels.createRenderer(gl, { cache: t.models.cache }), results = [];
    const camera = { position: [0, 1.5, -8], right: [1, 0, 0], up: [0, 1, 0], forward: [0, 0, 1], aspect: 1 };
    for (const kind of ['blue', 'yellow', 'red', 'green', 'stripe', 'purple', 'climber']) {
      const actor = t.makeAlien(kind, 10); actor.x = 0; actor.y = 1.86; actor.z = 0; actor.spawnGrace = 0; actor.onGround = true;
      const modelPath = 'characters/alien_' + kind + '.glb', model = t.models.get(modelPath);
      if (!model) throw Error('Missing preloaded raptor ' + kind);
      const fit = NeonModels.actorFit(model, actor), head = model.anchors.head.map((v, i) => [actor.x, actor.y - 1.86, actor.z][i] + fit.offset[i] + v * fit.scale[i]);
      const cast = origin => {
        const delta = head.map((v, i) => v - origin[i]), distance = Math.hypot(...delta);
        return t.rayEntity({ x: origin[0], y: origin[1], z: origin[2] }, { x: delta[0] / distance, y: delta[1] / distance, z: delta[2] / distance }, actor, 40);
      };
      gl.viewport(0, 0, 128, 128); gl.clearColor(0, 0, 0, 1); gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);
      renderer.beginView(camera, 0); const drawn = renderer.drawActor(modelPath, actor, { now: 0, animate: false });
      const pixels = new Uint8Array(128 * 128 * 4); gl.readPixels(0, 0, 128, 128, gl.RGBA, gl.UNSIGNED_BYTE, pixels);
      let lit = 0; for (let i = 0; i < pixels.length; i += 4) if (pixels[i] + pixels[i + 1] + pixels[i + 2] > 8) lit++;
      results.push({ kind, scale: actor.alienVisualScale, drawn, lit, level: cast([0, head[1], -12])?.head, steep: cast([0, 9, -5])?.head, ground: model.bounds.min[1] * fit.scale[1] + fit.offset[1], error: gl.getError() });
    }
    renderer.dispose(); return results;
  });
  for (const raptor of results) {
    assert.equal(raptor.scale, 1.30, label + ': ' + raptor.kind + ' visual scale');
    assert.equal(raptor.drawn, true, label + ': ' + raptor.kind + ' model draws');
    assert.ok(raptor.lit > 60, label + ': ' + raptor.kind + ' appears in real rendered pixels');
    assert.equal(raptor.level, true, label + ': ' + raptor.kind + ' visible head accepts a level shot');
    assert.equal(raptor.steep, true, label + ': ' + raptor.kind + ' visible head accepts a downward shot');
    assert.ok(Math.abs(raptor.ground) < 1e-5, label + ': ' + raptor.kind + ' soles remain grounded');
    assert.equal(raptor.error, 0, label + ': ' + raptor.kind + ' GPU draw');
  }
  return results.length;
}

async function capturePreview(page) {
  await page.evaluate(() => {
    // Deliberately staged QA preview using the real actor renderer and engine
    // structures. Pausing combat keeps the screenshot reproducible.
    const t = __NEON_TILES_TEST__; t.game.over = true; t.game.survivorWave = 10; t.game.waveTotal = 1; t.game.waveKilled = 0; t.game.waveDelay = 0; t.game.waveSpawning = true;
    t.player.x = 0; t.player.y = 3.5; t.player.z = -12; t.player.yaw = 0; t.player.pitch = .22; t.player.vx = t.player.vy = t.player.vz = 0; t.equip(t.player, 'assault', false);
    const raptor = t.makeAlien('blue', 10); raptor.x = -1.5; raptor.y = 1.86; raptor.z = -6; raptor.yaw = raptor.faceYaw = Math.PI; raptor.spawnGrace = 0; raptor.onGround = true;
    t.bots.length = 0; t.bots.push(raptor);
    t.walls.push(t.makeStructurePiece('wall', { x: 4.5, z: 0, y: 1.5, w: 5.8, d: .55, h: 3, c: '#76a0b0', builtBy: 'p1' }));
    t.platforms.push(t.makeStructurePiece('platform', { x: 4.5, z: 2.6, y: 2.65, w: 5.8, d: 5.8, h: .35, c: '#76a0b0', builtBy: 'p1' }));
    document.getElementById('msg').textContent = ''; document.getElementById('feed').style.display = 'none';
    t.markBlocksDirty(); t.updateHud(); t.draw();
  });
  fs.mkdirSync(process.env.NEON_CAPTURE_DIR, { recursive: true });
  await page.screenshot({ path: path.join(process.env.NEON_CAPTURE_DIR, 'Neon-Breach-Raptors-Tilesets.png') });
  await page.evaluate(() => { document.getElementById('feed').style.display = ''; });
}

async function main() {
  const server = http.createServer(serve); await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = 'http://127.0.0.1:' + server.address().port;
  let browser;
  try {
    browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}), args: ['--no-sandbox', '--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'] });
    const reports = [];
    const devices = [{ name: 'desktop', width: 1280, height: 800, touch: false, split: false }, { name: 'mobile', width: 390, height: 844, touch: true, split: false }, { name: 'split', width: 1280, height: 800, touch: false, split: true }];
    for (const device of devices.filter(d => (!process.env.NEON_TILES_DEVICE || d.name === process.env.NEON_TILES_DEVICE) && (!process.env.NEON_TILES_CAPTURE_ONLY || d.name === 'desktop'))) {
      const context = await browser.newContext({ viewport: { width: device.width, height: device.height }, hasTouch: device.touch, isMobile: device.touch });
      const page = await context.newPage(), errors = [], atlasRequests = [];
      page.on('pageerror', error => errors.push(error.message));
      page.on('request', request => { if (/atlas_color(?:_mobile)?\.webp(?:\?|$)/.test(request.url())) atlasRequests.push(request.url()); });
      await page.addInitScript(base => { window.NEON_ASSET_CONFIG = { ASSET_BASE_URL: base, UI_ASSET_BASE_URL: './assets', version: '' }; HTMLCanvasElement.prototype.requestPointerLock = function () {}; }, fixtures ? origin + '/fixture-assets' : undefined);
      await page.goto(origin + '/game/neon-arena/');
      await page.waitForFunction(() => !!window.__NEON_TILES_TEST__?.map?.renderSurfaces, null, { timeout: 15000 }).catch(error => { throw Error(device.name + ' failed to initialize: ' + errors.join('; ') + ' ' + error.message); });
      await page.evaluate(split => { document.getElementById('secondPlayer').value = split ? 'keyboard' : 'off'; document.getElementById('botCount').value = '0'; __NEON_TILES_TEST__.progress.mergeRemote({ highestWave: 100 }); }, device.split);
      const survivalMaps = JSON.parse(fs.readFileSync(path.join(repo, 'game/neon-arena/assets/survival/manifest.json'), 'utf8')).maps.map(item => item.id);
      const cases = process.env.NEON_TILES_CAPTURE_ONLY ? [{ mode: 'survivor', map: 'breach_ring' }] : [...Object.keys(atlasFor).map(map => ({ mode: 'dm', map })), { mode: 'tdm', map: 'grid' }, { mode: 'frag', map: 'grid' }, { mode: 'siege', map: 'grid' }, ...survivalMaps.map(map => ({ mode: 'survivor', map }))];
      const checked = [], detailedAtlases = new Set(); let raptors = 0;
      for (const entry of cases) {
        const label = device.name + ' ' + entry.mode + '/' + entry.map;
        await page.locator('[data-mode="' + entry.mode + '"]').click();
        if (entry.mode === 'survivor') await page.locator('[data-survival-map="' + entry.map + '"]').click();
        else await page.evaluate(map => { const select = document.getElementById('map'); select.value = map; select.dispatchEvent(new Event('change')); }, entry.map);
        await page.locator('#start').click();
        await page.waitForFunction(() => __NEON_TILES_TEST__.game.started && !__NEON_TILES_TEST__.game.over && __NEON_TILES_TEST__.map.loaded, null, { timeout: 30000 }).catch(async error => { throw Error(label + ' did not start: ' + await page.locator('#mapDescription').textContent() + ' ' + errors.join('; ') + ' ' + error.message); });
        const expected = entry.mode === 'survivor' ? entry.map : atlasFor[entry.map], checkedSurfaces = await inspectSurfaces(page, label);
        assert.equal(checkedSurfaces.atlas, expected, label + ': correct material atlas');
        assert.equal(await page.evaluate(() => !!__NEON_TILES_TEST__.player2), device.split, label + ': local player setup');
        assert.ok(checkedSurfaces.passes >= (device.split ? 2 : 1), label + ': each local viewport textures its surfaces');
        if (!detailedAtlases.has(expected)) { await inspectAtlasPixels(page, label); detailedAtlases.add(expected); }
        if (entry.mode === 'survivor' && !raptors) raptors = await inspectRaptors(page, label);
        if (process.env.NEON_CAPTURE_DIR && device.name === 'desktop' && entry.mode === 'survivor' && entry.map === 'breach_ring') await capturePreview(page);
        checked.push({ mode: entry.mode, map: entry.map, atlas: expected, genericPieces: checkedSurfaces.pieces });
        await page.evaluate(() => document.getElementById('returnMenu').click());
        assert.equal(await page.locator('#menu').isVisible(), true, label + ': returns to menu');
      }
      assert.ok(atlasRequests.length >= detailedAtlases.size, device.name + ': atlas images were requested');
      assert.ok(atlasRequests.every(url => device.touch ? url.includes('atlas_color_mobile.webp') : !url.includes('atlas_color_mobile.webp')), device.name + ': correct atlas resolution');
      assert.deepEqual(errors, [], device.name + ': no runtime errors');
      reports.push({ device: device.name, cases: checked, texturedAtlases: detailedAtlases.size, raptors, realWebGL: true, errors: 0 });
      await context.close();
    }
    console.log(JSON.stringify({ passed: reports }, null, 2));
  } finally { if (browser) await browser.close(); await new Promise(resolve => server.close(resolve)); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
