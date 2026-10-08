/* Reproducible ground-contact evidence from actual running gameplay. */
'use strict';
const fs = require('node:fs'), path = require('node:path'), assert = require('node:assert/strict');
const { pathToFileURL } = require('node:url'), { chromium } = require('playwright');
const output = process.env.QA_ARTIFACT_DIR;
if (!output) throw new Error('Set QA_ARTIFACT_DIR to save contact screenshots.');
(async () => {
  const browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}) });
  const errors = [];
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(pathToFileURL(path.resolve(__dirname, '../../apes-together-strong.html')).href);
    await page.waitForFunction(() => ATSVisualAssets.status === 'ready');
    await page.locator('#seedInput').fill('FOREST-VISUAL-QA');
    await page.locator('#newRun').click(); await page.waitForFunction(() => ATS.game?.time > .2);
    await page.evaluate(() => { ATS.renderer.camera.zoom = 1.3; ATS.game.king.dir = Math.PI / 4; });
    fs.mkdirSync(output, { recursive: true });
    await page.waitForTimeout(100);
    await page.screenshot({ path: path.join(output, 'king-grounded-idle.png') });
    const before = await page.evaluate(() => ({ x: ATS.game.king.x, y: ATS.game.king.y }));
    await page.keyboard.down('s'); await page.waitForTimeout(200);
    assert.equal(await page.evaluate(() => ATS.game.king.moving), true, 'running image comes from actual player movement');
    await page.screenshot({ path: path.join(output, 'king-grounded-running.png') });
    await page.keyboard.up('s');
    const after = await page.evaluate(() => ({ x: ATS.game.king.x, y: ATS.game.king.y, direction: ATSCharacterArt.direction(ATS.game.king.dir).name }));
    assert.equal(after.direction, 'south');
    assert.ok(Math.hypot(after.x - before.x, after.y - before.y) > 5);
    await page.evaluate(() => {
      const g = ATS.game, cage = [...g.world.objects.values()].find(o => o.type === 'cage' && o.siteId === 'opening-rescue');
      if (!cage) throw new Error('Opening rescue cage is missing');
      Object.assign(g.king, g.findOpen(cage.x - 75, cage.y + 75, 14));
      g.king.dir = Math.atan2(cage.y - g.king.y, cage.x - g.king.x);
      ATS.renderer.camera.x = g.king.x; ATS.renderer.camera.y = g.king.y; ATS.renderer.camera.zoom = 1.65;
    });
    await page.waitForTimeout(100);
    await page.screenshot({ path: path.join(output, 'opening-rescue-cage.png') });
    assert.deepEqual(errors, []);
    console.log('PASS: idle and actual running King at zoom 1.3, facing down screen, plus opening rescue cage at zoom 1.65, with normal HUD.');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
