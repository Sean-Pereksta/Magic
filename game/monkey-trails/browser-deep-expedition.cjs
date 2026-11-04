const fs = require('node:fs'),
  path = require('node:path'),
  assert = require('node:assert/strict');
const { chromium } = require(process.env.MONKEY_TRAILS_PLAYWRIGHT || 'playwright');
const root = path.resolve(__dirname, '../..');
(async () => {
  const browser = await chromium.launch({
    headless: true,
    executablePath: process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE,
  });
  try {
    const page = await browser.newPage({ viewport: { width: 390, height: 844 } }),
      errors = [];
    page.on('pageerror', (e) => errors.push(e.message));
    await page.route('**/*', async (route) => {
      const url = new URL(route.request().url()),
        file = path.resolve(root, '.' + decodeURIComponent(url.pathname));
      if (url.hostname !== 'localhost' || !file.startsWith(root + path.sep) || !fs.existsSync(file))
        return route.abort();
      return route.fulfill({
        body: fs.readFileSync(file),
        contentType:
          { '.html': 'text/html', '.mjs': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' }[
            path.extname(file)
          ] || 'application/octet-stream',
      });
    });
    await page.goto('http://localhost/game/monkey-trails.html');
    await page.evaluate(async () => {
      const C = await import('./monkey-trails/core.mjs'),
        { Vault } = await import('./monkey-trails/storage.mjs');
      const v = new Vault(() => {}),
        s = await v.open('Trail Practice', 'practice-only-not-ranked', {
          mode: 'practice',
          create: true,
          species: 'capuchin',
        });
      s.world.totalDistance = 310000;
      C.landProgress(s);
      C.selectLand(s, 'bellreef-atolls');
      for (const id of ['nacreon', 'vitreus']) {
        const p = structuredClone(s.run.monkey);
        Object.assign(p, {
          id,
          species: id,
          types: C.SPECIES[id].types,
          loadout: ['strike', C.SPECIES[id].signature, 'brace', 'breathe'],
        });
        p.hp = C.stats(p).hp;
        p.energy = C.stats(p).maxEnergy;
        s.collection[id] = p;
      }
      C.setParty(s, ['nacreon']);
      s.run.nextEncounter = 1;
      await v.checkpoint(s, true);
      await v.sync();
    });
    await page.locator('.field-guide summary').click();
    await page.click('[data-action="element-help"]');
    assert.equal(await page.locator('.element-table tbody td').count(), 64);
    await page.selectOption('#element-first', 'leaf');
    await page.selectOption('#element-second', 'ice');
    assert.match(await page.locator('#counter-results').innerText(), /Fire\s+4×/);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    await page.click('[data-action="close-dialog"]');
    await page.click('#practice-button');
    await page.click('[data-action="deep-journal"]');
    await page
      .locator('#dialog-content details')
      .filter({ hasText: 'Bellreef Song' })
      .locator('summary')
      .click();
    await page.click('[data-action="deep-pin"][data-id="6"]');
    await page.waitForSelector('.deep-panel svg');
    assert.match(await page.locator('.deep-panel').innerText(), /tuned shells/);
    await page.click('[data-action="deep-pairs"]');
    await page.selectOption('#sidekick-nacreon', 'vitreus');
    await page.click('[data-action="deep-attach"]');
    await page.click('[data-action="map-step"][data-id="E"]');
    const invitation = page.locator(
      '[data-action="encounter-begin"],[data-action="pending-begin"],[data-action="encounter-start"]',
    );
    await invitation.first().click();
    await page.click('[data-action="ready"]');
    await page.click('[data-action="move"][data-id="KingTide"]');
    await page.selectOption('#rider-move', 'GlassSun');
    await page.click('[data-action="deep-turn"]');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    assert.deepEqual(errors, []);
    console.log(
      'Mobile practice passed: journal pin, quest art, attachment, two selected abilities, no overflow or page errors.',
    );
  } finally {
    await browser.close();
  }
})().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
