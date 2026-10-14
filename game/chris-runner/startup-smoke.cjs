// Optional end-to-end tooling; this adds no browser dependency to the game.
// PLAYWRIGHT_PATH and CHROME_PATH select locally installed browser tooling.
// RUNNER_TEST_ROOT selects the repository; RUNNER_ASSET_ROOT selects the original
// cartoon-v1 pack folder containing characters/, troops/, tiles/, etc.
// THREE_SOURCE supplies three.min.js for deterministic offline scenarios.
// RUNNER_LIVE_ASSETS=1 adds a real R2/CORS startup after the fixture scenarios.
// RUNNER_CAPTURE_DIR receives screenshots and a concise JSON result report.
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { chromium } = require(process.env.PLAYWRIGHT_PATH || 'playwright');

const root = path.resolve(process.env.RUNNER_TEST_ROOT || path.resolve(__dirname, '../..'));
const assetRoot = process.env.RUNNER_ASSET_ROOT && path.resolve(process.env.RUNNER_ASSET_ROOT);
const captureDir = process.env.RUNNER_CAPTURE_DIR && path.resolve(process.env.RUNNER_CAPTURE_DIR);
const threeSource = process.env.THREE_SOURCE;
const appOrigin = 'https://www.catnmice.com';
const appURL = `${appOrigin}/game/chrisrunner.html`;
const manifest = JSON.parse(fs.readFileSync(path.join(root, 'game/chris-runner/asset-manifest.json'), 'utf8'));
const graphicsSource = fs.readFileSync(path.join(root, 'game/chris-runner/graphics.js'), 'utf8');
const assetBase = graphicsSource.match(/const BASE_URL\s*=\s*['"]([^'"]+)['"]/)[1];
const assetOrigin = new URL(assetBase).origin;
const assetPrefix = new URL(assetBase).pathname;
const assetPaths = new Set(manifest.assets.map(entry => entry.path));
const heldPath = manifest.assets[manifest.assets.length - 1].path;
const failedPath = manifest.assets[0].path;
const results = [];
const timeout = 90000;

assert.equal(manifest.assetCount, 262);
assert(assetRoot && fs.existsSync(assetRoot), 'Set RUNNER_ASSET_ROOT to the original cartoon-v1 GLB pack folder');
assert(threeSource && fs.existsSync(threeSource), 'Set THREE_SOURCE to a local Three.js 0.160.0 browser build');
if (captureDir) fs.mkdirSync(captureDir, { recursive: true });

function resolveInside(directory, relative) {
  const target = path.resolve(directory, relative);
  assert(target.startsWith(directory + path.sep), `Fixture path left its source folder: ${relative}`);
  return target;
}

async function capture(page, name) {
  if (captureDir) await page.screenshot({ path: path.join(captureDir, `${name}.png`) });
}

async function frames(page, count = 3) {
  await page.evaluate(count => new Promise(resolve => {
    const tick = () => --count <= 0 ? resolve() : requestAnimationFrame(tick);
    requestAnimationFrame(tick);
  }), count);
}

async function fixturePage(browser, name, options = {}) {
  const context = await browser.newContext({
    viewport: options.mobile ? { width: 390, height: 844 } : { width: 1280, height: 800 },
    deviceScaleFactor: 1,
    reducedMotion: 'reduce',
    ...(options.mobile ? { isMobile: true, hasTouch: true } : {})
  });
  const page = await context.newPage();
  page.setDefaultTimeout(timeout);
  const errors = [];
  const startupErrors = [];
  const counts = new Map();
  let manifestRequests = 0;
  let fail = !!options.failure;
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  page.on('pageerror', error => errors.push(error.message));
  page.on('console', message => {
    if (message.type() === 'error' && message.text().startsWith('Chris Runner startup:')) startupErrors.push(message.text());
  });

  // Fulfilling the app route keeps these tests independent of the deployed site.
  // Live mode only continues GLB requests, so the actual game performs the CORS
  // fetch and SHA-256 validation against the public Cloudflare endpoint.
  await page.route('**/*', async route => {
    const url = new URL(route.request().url());
    if (url.origin === appOrigin) {
      if (url.pathname === '/favicon.ico') return route.fulfill({ status: 204, body: '' });
      const file = resolveInside(root, '.' + decodeURIComponent(url.pathname));
      if (!fs.existsSync(file) || !fs.statSync(file).isFile()) return route.fulfill({ status: 404, body: 'Missing app fixture' });
      if (file.endsWith('asset-manifest.json')) manifestRequests++;
      const contentType = file.endsWith('.html') ? 'text/html' : file.endsWith('.css') ? 'text/css' : file.endsWith('.js') ? 'text/javascript' : file.endsWith('.json') ? 'application/json' : 'application/octet-stream';
      return route.fulfill({ path: file, contentType, headers: { 'cache-control': 'no-store' } });
    }
    if (url.href.includes('three@0.160.0')) return route.fulfill({ path: threeSource, contentType: 'text/javascript' });
    if (url.origin === assetOrigin) {
      assert(url.pathname.startsWith(assetPrefix), 'Asset request escaped the configured pack prefix');
      const relative = decodeURIComponent(url.pathname.slice(assetPrefix.length));
      assert(assetPaths.has(relative), `Unexpected graphics request: ${relative}`);
      counts.set(relative, (counts.get(relative) || 0) + 1);
      if (options.hold && relative === heldPath) await gate;
      if (fail && relative === failedPath) return route.fulfill({
        status: 404, body: 'Deliberately missing test model',
        headers: { 'access-control-allow-origin': appOrigin, 'cache-control': 'no-store' }
      });
      if (options.live) return route.continue();
      return route.fulfill({
        path: resolveInside(assetRoot, relative), contentType: 'model/gltf-binary',
        headers: { 'access-control-allow-origin': appOrigin, 'cache-control': 'no-store' }
      });
    }
    // Leaderboard/network services are outside this startup test's scope.
    return route.abort();
  });
  await page.goto(appURL, { waitUntil: 'load' });
  return {
    context, page, counts, errors, startupErrors, name,
    release: () => release(), allowMissing: () => { fail = false; },
    manifestRequests: () => manifestRequests,
    requests: () => [...counts.values()].reduce((sum, count) => sum + count, 0)
  };
}

async function bootState(page) {
  return page.evaluate(() => ({
    graphics: window.RunnerGraphics.status,
    initialized: typeof scene !== 'undefined' && !!scene,
    playerInitialized: typeof player !== 'undefined' && !!player,
    previewInitialized: typeof characterPreviewRenderer !== 'undefined' && !!characterPreviewRenderer,
    playerCount: typeof players === 'undefined' ? 0 : players.length,
    overlayVisible: !document.getElementById('runnerLoadOverlay').hidden,
    menuDisabled: [...document.querySelectorAll('#menuButtons button')].every(button => button.disabled),
    gameStarted: typeof gameStarted === 'undefined' ? false : gameStarted
  }));
}

async function ready(page) {
  try {
    await page.waitForFunction(() => window.RunnerGraphics?.status.phase === 'error' || (window.RunnerGraphics?.ready && typeof scene !== 'undefined' && !!scene && document.getElementById('runnerLoadOverlay').hidden), null, { timeout });
    assert.equal(await page.evaluate(() => window.RunnerGraphics.status.phase), 'ready', 'Graphics startup failed');
  } catch (error) {
    error.message += '\nStartup diagnostics: ' + JSON.stringify(await page.evaluate(() => ({
      graphics: window.RunnerGraphics?.status,
      detail: document.getElementById('runnerLoadDetail')?.textContent,
      scene: typeof scene !== 'undefined' && !!scene,
      player: typeof player !== 'undefined' && !!player
    })));
    throw error;
  }
  await page.waitForFunction(() => document.querySelectorAll('.characterCard[data-menu-styled="true"]').length === 5, null, { timeout });
  await frames(page);
  const status = await bootState(page);
  assert.equal(status.graphics.phase, 'ready');
  assert.equal(status.graphics.loaded, 262);
  assert.equal(status.graphics.total, 262);
  assert.equal(status.overlayVisible, false);
  assert.equal(status.menuDisabled, false);
  assert.equal(status.initialized, true);
  assert.equal(status.previewInitialized, true);
}

async function verifyMenu(page) {
  assert.equal(await page.locator('.characterCard').count(), 5);
  assert.equal(await page.locator('.runnerCardTopline').count(), 5);
  assert.equal(await page.getByRole('button', { name: 'Run solo, one player', exact: true }).isEnabled(), true);
  assert.equal(await page.getByRole('button', { name: 'Local versus, two players on one keyboard', exact: true }).isEnabled(), true);
  assert.equal(await page.getByRole('button', { name: 'Load a saved replay' }).isEnabled(), true);
  const sizes = await page.evaluate(() => {
    const menu = document.getElementById('menuCard');
    const canvas = document.querySelector('#characterPreviewCanvas canvas');
    return {
      horizontalOverflow: menu.scrollWidth > menu.clientWidth + 1,
      cardTextOverflow: [...document.querySelectorAll('.characterCard')].some(card => card.scrollHeight > card.clientHeight + 2),
      previewWidth: canvas.getBoundingClientRect().width,
      previewHeight: canvas.getBoundingClientRect().height,
      asset: characterPreviewModel.userData.runnerAsset.path,
      drawCalls: characterPreviewRenderer.info.render.calls
    };
  });
  assert.equal(sizes.horizontalOverflow, false);
  assert.equal(sizes.cardTextOverflow, false);
  assert(sizes.previewWidth > 0 && sizes.previewHeight > 0 && sizes.drawCalls > 0, 'Menu must render its GLB preview');
  assert(sizes.asset.startsWith('characters/'), 'Preview must use an authored GLB');

  await page.locator('[data-character="chris"]').focus();
  await page.keyboard.press('ArrowRight');
  assert.equal(await page.locator('.characterCard[aria-pressed="true"]').getAttribute('data-character'), 'dawit');
  assert.equal(await page.locator('#runnerPreviewBadge').textContent(), 'Speedster');
  await page.keyboard.press('End');
  assert.equal(await page.locator('.characterCard[aria-pressed="true"]').getAttribute('data-character'), 'kevon');
  await page.locator('[data-character="anthony"]').click();
  assert.equal(await page.locator('#previewCharacterName').textContent(), 'Anthony');
  await page.locator('[data-character="chris"]').click();
  await page.locator('.runnerControlsSummary').click();
  assert.equal(await page.locator('#menuControls details').getAttribute('open'), '');
  await page.locator('.runnerControlsSummary').click();
  // Tab must stay in the visible menu, including after asynchronous roster rebuild.
  await page.locator('.runnerControlsSummary').focus();
  await page.keyboard.press('Tab');
  assert.equal(await page.evaluate(() => document.activeElement.dataset.character), 'chris');

  const motion = await page.evaluate(async () => {
    renderCharacterPreview();
    const before = [characterPreviewModel.rotation.y, characterPreviewModel.position.y];
    let childMutations = 0;
    const observer = new MutationObserver(records => { childMutations += records.filter(record => record.type === 'childList').length; });
    observer.observe(document.getElementById('characterGrid'), { childList: true, subtree: true });
    await new Promise(resolve => { let remaining = 4; const tick = () => --remaining ? requestAnimationFrame(tick) : resolve(); requestAnimationFrame(tick); });
    renderCharacterPreview();
    observer.disconnect();
    return { before, after: [characterPreviewModel.rotation.y, characterPreviewModel.position.y], childMutations, reducedMotion: matchMedia('(prefers-reduced-motion: reduce)').matches };
  });
  assert.equal(motion.reducedMotion, true);
  assert.deepEqual(motion.before, motion.after, 'Reduced motion must stop preview rotation and bobbing');
  assert.equal(motion.childMutations, 0, 'Async menu decoration must settle without a MutationObserver loop');
  return sizes;
}

async function gatedStartup(browser) {
  const test = await fixturePage(browser, 'gated-desktop', { hold: true });
  try {
    await test.page.waitForFunction(() => window.RunnerGraphics?.status.loaded === 261, null, { timeout });
    const before = await bootState(test.page);
    assert.equal(before.graphics.phase, 'loading');
    assert.equal(before.initialized, false);
    assert.equal(before.playerInitialized, false);
    assert.equal(before.previewInitialized, false);
    assert.equal(before.playerCount, 0);
    assert.equal(before.overlayVisible, true);
    assert.equal(before.menuDisabled, true);
    await test.page.evaluate(() => startGame(1));
    assert.equal((await bootState(test.page)).gameStarted, false, 'Programmatic play must also respect the preload gate');
    await capture(test.page, 'startup-overlay');
    test.release();
    await ready(test.page);
    const menu = await verifyMenu(test.page);
    assert.equal(await test.page.locator('#mobileControls').isVisible(), false, 'Touch controls must stay hidden while the menu is open');
    await test.page.evaluate(() => document.activeElement?.blur());
    await capture(test.page, 'modern-menu-desktop');
    assert.equal(test.requests(), 262);
    assert.equal(test.counts.size, 262);
    assert.equal(test.manifestRequests(), 1);
    await test.page.getByRole('button', { name: 'Run solo, one player', exact: true }).click();
    assert.deepEqual(await test.page.evaluate(() => ({ started: gameStarted, mode: gameMode, hidden: document.getElementById('menuOverlay').style.display === 'none' })), { started: true, mode: 1, hidden: true });
    assert.deepEqual(test.errors, []);
    assert.deepEqual(test.startupErrors, []);
    results.push({ test: test.name, passed: true, graphicsRequests: test.requests(), menu });
  } finally { test.release(); await test.context.close(); }
}

async function mobileStartup(browser) {
  const test = await fixturePage(browser, 'mobile-menu', { mobile: true });
  try {
    await ready(test.page);
    const menu = await verifyMenu(test.page);
    assert.equal(await test.page.locator('#mobileControls').isVisible(), false, 'Touch controls must stay hidden while the menu is open');
    await test.page.evaluate(() => { document.getElementById('menuCard').scrollTop = 0; document.activeElement?.blur(); });
    await capture(test.page, 'modern-menu-mobile');
    const button = test.page.getByRole('button', { name: 'Local versus, two players on one keyboard', exact: true });
    const box = await button.boundingBox();
    assert(box && box.y + box.height < 844, 'Mobile play choices should be visible without scrolling');
    await button.click();
    assert.deepEqual(await test.page.evaluate(() => ({ started: gameStarted, mode: gameMode, players: players.length })), { started: true, mode: 2, players: 2 });
    assert.equal(await test.page.locator('#mobileControls').isVisible(), true, 'Touch controls should appear after the game starts');
    assert.equal(test.requests(), 262);
    assert.deepEqual(test.errors, []);
    assert.deepEqual(test.startupErrors, []);
    results.push({ test: test.name, passed: true, graphicsRequests: test.requests(), menu });
  } finally { await test.context.close(); }
}

async function failedStartup(browser) {
  const test = await fixturePage(browser, 'missing-model-retry', { failure: true });
  try {
    await test.page.waitForFunction(() => window.RunnerGraphics?.status.phase === 'error' && !document.getElementById('runnerLoadRetry').hidden, null, { timeout });
    const failed = await bootState(test.page);
    assert.equal(failed.graphics.loaded, 261);
    assert.equal(failed.initialized, false);
    assert.equal(failed.playerInitialized, false);
    assert.equal(failed.overlayVisible, true);
    assert.equal(failed.menuDisabled, true);
    assert.equal(test.counts.get(failedPath), 3, 'A missing model gets exactly three initial attempts');
    assert.equal(test.requests(), 264);
    await capture(test.page, 'startup-retry');
    const before = new Map(test.counts);
    test.allowMissing();
    await test.page.getByRole('button', { name: 'Try again', exact: true }).click();
    await ready(test.page);
    assert.equal(test.counts.get(failedPath), 4);
    assert.equal(test.requests(), 265, 'Retry must download only the missing model');
    for (const [file, count] of before) if (file !== failedPath) assert.equal(test.counts.get(file), count, `Retry downloaded an already loaded asset: ${file}`);
    assert.equal(test.manifestRequests(), 1, 'Retry must keep the already verified manifest');
    assert.equal(await test.page.locator('.runnerCardTopline').count(), 5);
    assert.deepEqual(test.errors, []);
    assert.equal(test.startupErrors.length, 1, 'Only the deliberately missing model should produce a startup error');
    results.push({ test: test.name, passed: true, initialAttempts: 3, retryRequests: 1, graphicsRequests: test.requests() });
  } finally { await test.context.close(); }
}

async function liveStartup(browser) {
  const test = await fixturePage(browser, 'live-cloudflare-cors', { live: true });
  try {
    await ready(test.page);
    assert.equal(test.requests(), 262);
    assert.equal(test.counts.size, 262);
    assert.deepEqual(test.errors, []);
    assert.deepEqual(test.startupErrors, []);
    results.push({ test: test.name, passed: true, assetBase, graphicsRequests: test.requests() });
  } finally { await test.context.close(); }
}

(async () => {
  const browser = await chromium.launch({
    headless: true,
    ...(process.env.CHROME_PATH ? { executablePath: process.env.CHROME_PATH } : {}),
    args: ['--use-angle=swiftshader', '--enable-unsafe-swiftshader']
  });
  try {
    await gatedStartup(browser);
    await mobileStartup(browser);
    await failedStartup(browser);
    if (process.env.RUNNER_LIVE_ASSETS === '1') await liveStartup(browser);
    if (captureDir) fs.writeFileSync(path.join(captureDir, 'startup-results.json'), JSON.stringify(results, null, 2) + '\n');
    console.log(JSON.stringify(results, null, 2));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
