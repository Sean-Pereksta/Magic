/* Optional browser integration check. Requires Playwright and Chromium. */
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const { pathToFileURL } = require('node:url');
const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({
    headless: true,
    ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}),
    args: ['--no-sandbox', '--disable-dev-shm-usage']
  });
  const errors = [];
  const url = pathToFileURL(path.resolve(__dirname, '../../apes-together-strong.html')).href;
  const watch = page => {
    page.on('pageerror', e => errors.push(e.message));
    page.on('console', message => { if (message.type() === 'error') errors.push(message.text()); });
  };
  const screenshot = async (page, name) => {
    if (!process.env.QA_ARTIFACT_DIR) return;
    fs.mkdirSync(process.env.QA_ARTIFACT_DIR, { recursive: true });
    await page.screenshot({ path: path.join(process.env.QA_ARTIFACT_DIR, name + '.png') });
  };
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    watch(page);
    await page.goto(url);
    await page.waitForFunction(() => window.ATS?.screen === 'menu');
    assert.equal(await page.locator('.lobby-link').getAttribute('href'), '../lobby/lobby.html');
    await screenshot(page, 'menu');
    await page.locator('#seedInput').fill('FOREST-BROWSER');
    await page.locator('#newRun').click();
    await page.waitForFunction(() => ATS.game?.time > .2);
    const before = await page.evaluate(() => ({ ...ATS.game.king }));
    await page.keyboard.down('d');
    await page.waitForTimeout(450);
    await page.keyboard.up('d');
    const after = await page.evaluate(() => ({ ...ATS.game.king }));
    assert.ok(Math.hypot(after.x - before.x, after.y - before.y) > 8, 'keyboard movement');
    await page.keyboard.press('Escape');
    assert.equal(await page.evaluate(() => ATS.screen), 'pause');
    const pausedTime = await page.evaluate(() => ATS.game.time);
    await page.waitForTimeout(120);
    assert.equal(await page.evaluate(() => ATS.game.time), pausedTime, 'pause freezes simulation');

    await page.keyboard.press('F3');
    assert.equal(await page.locator('#performanceMonitor').isVisible(), true, 'debug monitor toggles on');
    await page.waitForFunction(() => typeof ATS.performance?.simulationMs === 'number');
    await page.keyboard.press('F3');
    assert.equal(await page.locator('#performanceMonitor').isVisible(), false, 'debug monitor stays hidden in ordinary play');
    const hidden = await page.evaluate(async () => {
      const renderer = ATS.renderer, original = renderer.draw;
      let renders = 0;
      renderer.draw = function (...args) { renders++; return original.apply(this, args); };
      Object.defineProperty(document, 'hidden', { configurable: true, get: () => true });
      document.dispatchEvent(new Event('visibilitychange'));
      const time = ATS.game.time;
      await new Promise(resolve => setTimeout(resolve, 180));
      const result = { renders, unchanged: ATS.game.time === time, pausedAudio: ATS.audio.paused };
      delete document.hidden;
      document.dispatchEvent(new Event('visibilitychange'));
      renderer.draw = original;
      return result;
    });
    assert.deepEqual(hidden, { renders: 0, unchanged: true, pausedAudio: true }, 'hidden tab skips rendering, simulation and audio');
    await page.locator('#resumeRun').click();
    const catchup = await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => {
      const time = ATS.game.time, start = performance.now();
      while (performance.now() - start < 250) {} // A real main-thread stall.
      requestAnimationFrame(() => resolve(ATS.game.time - time));
    })));
    assert.ok(catchup <= .05 + 1e-8, 'long browser frame performs at most three fixed simulation ticks');
    await page.keyboard.press('Escape');
    assert.equal(await page.evaluate(() => ATS.screen), 'pause');

    const setup = await page.evaluate(() => {
      const g = ATS.game;
      g.king.x = g.king.y = 0;
      const cage = [...g.world.objects.values()].find(o => o.siteId === 'opening-rescue' && o.type === 'cage');
      g.damageObject(cage, 999, g.king);
      g.commandCD = 0;
      g.command('call');
      const freed = g.followers.length;
      for (let i = 0; i < 15; i++) g.makeApe(0, 0, 'follow');
      g.food = 150;
      g.commandCD = 0;
      const settled = g.command('settleAll');
      const s = g.settlements[0];
      g.colonies.action(s.id, 'fortify');
      // Let distant aggregate construction advance real game time before
      // displaying the mature camp; fresh homes now have visible work stages.
      const crown = { x:g.king.x, y:g.king.y };g.king.x = s.x + 5000;s.wood = 100;
      for (let i = 0; i < 90; i++) { g.time++;g.refreshSettlements();g.colonies.tick(s); }
      Object.assign(g.king,crown);
      // Render every new force, hazard and fall pose using the real renderer.
      let i = 0;
      for (const role of Object.keys(ATSHumanRoles)) {
        const h = g.makeHuman(-180 + (i % 5) * 90, -170 + Math.floor(i / 5) * 85, null);
        delete h.role;
        g.forces.assign(h, null, role);
        h.state = 'patrol';
        if (role === 'sniper') h.aiming = { x: 70, y: 80, until: g.time + 1.35 };
        i++;
      }
      for (let j = 0; j < 5; j++) {
        const a = g.makeApe(-100 + j * 45, 130, 'free');
        g.hurt(a, 999, g.king);
        g.corpses.at(-1).fallVariant = j;
        g.corpses.at(-1).age = .3;
      }
      g.forces.hazards.push({ type: 'grenade', x: 140, y: 40, fromX: -130, fromY: -120, start: g.time, fuse: 1.8, life: 1.8, radius: 67 });
      g.forces.hazards.push({ type: 'flare', x: -90, y: 40, start: g.time, life: 12, radius: 150 });
      g.animateAttack(g.king);
      ATS.renderer.camera.x = ATS.renderer.camera.y = 0;
      ATS.renderer.draw(g, 0);
      return { freed, settled, population: s.population, defense: s.maxDefense };
    });
    assert.equal(setup.freed, 2);
    assert.equal(setup.settled, true);
    assert.ok(setup.population >= 17);
    assert.ok(setup.defense > 0);
    await screenshot(page, 'combat');
    // Remove the test encounter before resuming the player's menu flow.
    await page.evaluate(() => { ATS.game.humans = []; ATS.game.forces.hazards = []; });
    await page.locator('#resumeRun').click();
    await page.keyboard.press('m');
    assert.equal(await page.evaluate(() => ATS.screen), 'overview');
    await page.locator('.settlement-row select').selectOption('forage');
    assert.equal(await page.evaluate(() => ATS.game.settlements[0].policy), 'forage');
    await page.getByRole('button', { name: 'Deliver up to 30 food' }).click();
    await screenshot(page, 'settlements');
    await page.locator('#closeOverview').click();
    await page.keyboard.press('Escape');
    await page.locator('#settingsButton').click();
    await page.locator('#soundToggle').uncheck();
    await page.locator('#reducedMotion').check();
    await page.locator('#qualitySelect').selectOption('low');
    assert.equal(await page.evaluate(() => ATS.audio.enabled), false);
    assert.equal(await page.evaluate(() => ATS.renderer.reducedMotion), true);
    await page.locator('#backSettings').click();
    await page.locator('#saveRun').click();
    await page.reload();
    await page.locator('#continueRun').click();
    await page.waitForFunction(() => ATS.screen === 'play');
    assert.equal(await page.evaluate(() => ATS.game.settlements[0].policy), 'forage');
    await page.evaluate(() => ATS.game.hurt(ATS.game.king, 999));
    await page.waitForFunction(() => ATS.screen === 'death', { timeout: 10000 });
    assert.equal(await page.evaluate(() => localStorage.getItem('ats-crown-save-v1')), null);
    assert.ok(await page.evaluate(() => JSON.parse(localStorage.getItem('ats-crown-legacy-v1')).length > 0));
    await page.locator('#deathMenu').click();
    assert.equal(await page.locator('#continueRun').isDisabled(), true);

    const mobile = await browser.newPage({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
    watch(mobile);
    await mobile.goto(url);
    await mobile.locator('#newRun').tap();
    await mobile.waitForFunction(() => ATS.screen === 'play');
    assert.equal(await mobile.locator('#joystick').isVisible(), true);
    await mobile.locator('#commandToggle').tap();
    await mobile.locator('[data-command="charge"]').tap();
    await mobile.locator('#gameCanvas').tap({ position: { x: 240, y: 360 } });
    await mobile.locator('#mapButton').tap();
    assert.equal(await mobile.evaluate(() => ATS.screen), 'overview');
    assert.equal(await mobile.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'mobile has no horizontal overflow');
    await screenshot(mobile, 'mobile-map');
    await mobile.locator('#closeOverview').tap();
    await mobile.locator('#attackButton').tap();
    await screenshot(mobile, 'mobile-play');
    assert.deepEqual(errors, []);
    console.log('PASS: desktop/mobile menus, movement, rescue, settlements, all force/animation rendering, hidden-tab pause, debug monitor, bounded long-frame recovery, save/reload, settings and permadeath. No browser errors.');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
