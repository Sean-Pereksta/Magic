// Uses optional local test tooling; adds no game runtime dependency.
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const assert = require('node:assert/strict');
const {chromium} = require(process.env.PLAYWRIGHT_PATH || 'playwright');
const root = process.env.RUNNER_TEST_ROOT || path.resolve(__dirname, '../..');
const output = process.env.RUNNER_CAPTURE_DIR;
const server = http.createServer((req, res) => {
  const file = path.resolve(root, '.' + decodeURIComponent(req.url.split('?')[0]));
  if (!file.startsWith(root + path.sep)) {res.writeHead(403).end(); return;}
  fs.readFile(file, (error, data) => {
    if (error) {res.writeHead(404).end(); return;}
    res.setHeader('Content-Type', file.endsWith('.html') ? 'text/html' : file.endsWith('.js') ? 'text/javascript' : 'application/octet-stream');
    res.end(data);
  });
});

(async () => {
  if (output) fs.mkdirSync(output, {recursive: true});
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const browser = await chromium.launch({headless: true, ...(process.env.CHROME_PATH ? {executablePath: process.env.CHROME_PATH} : {}), args: ['--use-angle=swiftshader', '--enable-unsafe-swiftshader']});
  const errors = [], results = [];
  try {
    for (const mobile of [false, true]) {
      const context = await browser.newContext(mobile ? {viewport: {width: 430, height: 860}, isMobile: true, hasTouch: true, deviceScaleFactor: 1} : {viewport: {width: 1280, height: 800}});
      const page = await context.newPage();
      page.on('pageerror', error => errors.push(error.message));
      await page.route('https://**/*', route => {
        if (route.request().url().includes('three@0.160.0')) return route.fulfill({path: process.env.THREE_SOURCE, contentType: 'text/javascript'});
        return route.abort();
      });
      await page.goto(`http://127.0.0.1:${server.address().port}/game/chrisrunner.html`, {waitUntil: 'load'});
      await page.evaluate(() => {startGame(2); gameStarted = false;});
      assert.equal(await page.evaluate(() => players.length), 2);
      for (const realm of process.env.RUNNER_TEST_REALMS ? process.env.RUNNER_TEST_REALMS.split(',') : process.env.RUNNER_TEST_ROOT ? ['forest', 'dream'] : ['forest', 'city', 'rooftops', 'construction', 'industrial', 'tunnel', 'ruins', 'snow', 'desert', 'dream', 'carnival', 'volcanic', 'crystal', 'neon', 'harbor', 'clockwork']) {
        const metrics = await page.evaluate(realm => {
          for (const objects of [enemies, dogs, spiritDogs, policeOfficers, desertStalkers]) for (const actor of objects) disposeRunnerObject(actor.rig);
          for (const c of worldChunks) disposeRunnerObject(c);
          worldChunks = []; walls = []; pickups = []; grassPatches = []; enemies = []; dogs = []; spiritDogs = []; policeOfficers = []; desertStalkers = []; dreamHazards = []; fallingStars = [];
          if (typeof resetRunnerEvolution === 'function') resetRunnerEvolution(271);
          setReplaySeed(271);
          const route = typeof runnerRealmForIndex === 'function' ? Array.from({length:16}, (_, i) => runnerRealmForIndex(i)) : ['forest', 'city', 'rooftops', 'construction', 'industrial', 'tunnel', 'ruins', 'snow', 'desert', 'dream'];
          const start = route.indexOf(realm) * CHUNK_LENGTH * 6;
          distance = start; player.rig.position.set(0, 0, start); players[1].rig.position.set(2.6, 0, start);
          for (let i = 0; i < 9; i++) generateChunk(start + i * CHUNK_LENGTH);
          if (realm === 'dream') {const dog = createSpiritDog(-3, start + 12); dog.heading = Math.PI; dog.rig.rotation.y = Math.PI; spiritDogs.push(dog);}
          if (typeof updateRunnerWorld === 'function') updateRunnerWorld(.016);
          for (const actor of enemies) animateRunner(actor, .016, false, true);
          snapCameraToPlayers(); updateEnvironmentSky(2); updateHUD();
          renderer.render(scene, camera);
          const timings = [];
          for (let i = 0; i < 45; i++) {const t = performance.now(); renderer.render(scene, camera); timings.push(performance.now() - t);}
          timings.sort((a, b) => a - b);
          return {realm, draws: renderer.info.render.calls, triangles: renderer.info.render.triangles, geometries: renderer.info.memory.geometries, medianSubmitMs: timings[22], p95SubmitMs: timings[42]};
        }, realm);
        results.push({mobile, ...metrics});
        if (output) await page.screenshot({path: path.join(output, `${mobile ? 'mobile' : 'desktop'}-${realm}.png`)});
      }
      if (output && !mobile && !process.env.RUNNER_TEST_ROOT) {
        for (const shot of ['enemy-lineup', 'spirit-outline']) {
          await page.evaluate(shot => {
            const stage = new THREE.Scene(); stage.background = new THREE.Color(0x211635); stage.fog = new THREE.Fog(0x211635, 20, 80);
            stage.add(new THREE.HemisphereLight(0xd8edff, 0x3e294e, 2.3));
            const light = new THREE.DirectionalLight(0xffffff, 2.4); light.position.set(-5, 12, 8); stage.add(light);
            const ground = new THREE.Mesh(new THREE.PlaneGeometry(80, 80), new THREE.MeshStandardMaterial({color: 0x332745, roughness: .9})); ground.rotation.x = -Math.PI / 2; ground.position.y = -.05; stage.add(ground);
            const view = new THREE.PerspectiveCamera(38, 1280 / 800, .1, 100);
            if (shot === 'enemy-lineup') {
              view.position.set(0, 9, 31); view.lookAt(0, 2.3, 0);
              ['chaser', 'fast', 'heavy', 'ambusher', 'blocker', 'jumper', 'ranged', 'elite'].forEach((role, i) => {
                const made = buildRunnerEnemyModel(role, i % 6); stage.add(made.rig);
                const actor = {...made, role, activated: role !== 'ambusher', animTime: i * .6, torchCooldown: .4};
                animateRunner(actor, 1/60, true, false);
                for (let f = 0; f < 20; f++) {actor.rig.position.z += (role === 'heavy' ? 8 : 15) / 60; animateRunner(actor, 1/60, true, false);}
                actor.rig.position.x = (i - 3.5) * 3.7; actor.rig.position.z = 0;
              });
            } else {
              view.position.set(0, 4.5, 13); view.lookAt(0, 1, 0);
              const visible = createSpiritDog(-2, 0); visible.rig.rotation.y = .5; stage.add(visible.rig);
              const hidden = createSpiritDog(3, -4); stage.add(hidden.rig);
              const wall = new THREE.Mesh(new THREE.BoxGeometry(4, 4, 1), new THREE.MeshStandardMaterial({color: 0x77558d})); wall.position.set(3, 2, -1); stage.add(wall);
            }
            for (const id of ['hud','twoPlayerScoreboard','roundToast','mobileControls']) document.getElementById(id).style.visibility = 'hidden';
            window.runnerCaptureRender = renderer.render.bind(renderer);
            renderer.render = () => window.runnerCaptureRender(stage, view);
            renderer.render(); window.runnerCaptureStage = stage;
          }, shot);
          await page.screenshot({path: path.join(output, shot + '.png')});
          await page.evaluate(() => {renderer.render = window.runnerCaptureRender; disposeRunnerObject(window.runnerCaptureStage); for (const id of ['hud','twoPlayerScoreboard','roundToast','mobileControls']) document.getElementById(id).style.visibility = '';});
        }
      }
      // Exercise real key events, live animation, resize, and five abilities.
      for (const key of ['chris', 'dawit', 'jay', 'anthony', 'kevon']) {
        await page.evaluate(key => {selectedCharacterKey = key; startGame(1);}, key);
        await page.keyboard.down('w'); await page.keyboard.down('Shift');
        await page.waitForTimeout(120); await page.keyboard.press('Space');
        await page.keyboard.up('Shift'); await page.keyboard.up('w');
        assert.ok(await page.evaluate(() => player.rig.position.toArray().every(Number.isFinite)));
      }
      await context.close();
    }
    assert.deepEqual(errors, []);
    if (output) fs.writeFileSync(path.join(output, 'render-metrics.json'), JSON.stringify(results, null, 2));
    console.log(JSON.stringify({pageErrors: errors, results}, null, 2));
  } finally {await browser.close(); server.close();}
})().catch(error => {console.error(error); server.close(); process.exitCode = 1;});
