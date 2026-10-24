/* Shipped-page checks for the integrated tactical presentation and management. */
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), http = require('node:http');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..'), errors = [];
const server = http.createServer((req, res) => {
  const file = path.resolve(root, '.' + new URL(req.url, 'http://local').pathname);
  if (!file.startsWith(root + path.sep)) return res.writeHead(403).end();
  fs.readFile(file, (error, body) => { if (error) return res.writeHead(404).end(); res.setHeader('Content-Type', file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html'); res.end(body); });
});
let browser;
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ headless: true, ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}), args: ['--no-sandbox', '--disable-gpu'] });
  const context = await browser.newContext({ viewport: { width: 1280, height: 1000 }, hasTouch: true });
  const page = await context.newPage(); page.setDefaultTimeout(10000);
  page.on('pageerror', error => errors.push(error.stack));
  await page.route('https://www.gstatic.com/**', route => route.abort());
  await page.goto(`http://127.0.0.1:${server.address().port}/game/roguecard.html`);
  await page.waitForFunction(() => window.TinyTroopsPresentation && window.TinyTroopsManagement && window.TinyTroopsEquipment);
  await page.fill('#menuUser', 'TacticalQA'); await page.fill('#menuPass', 'qa-code'); await page.click('#menuNew');
  await page.evaluate(() => {
    window.qaStop = () => { ttClearTimers(); TinyTroopsPolish.timer = null; };
    window.qaTeam = () => {
      reset(); qaStop(); document.getElementById('mainMenu').classList.remove('open'); close();
      S.round = 12; S.phase = 'recruit'; S.battle = null; S.tt.draft = S.tt.wavePlan = S.tt.event = null;
      S.tt.draftedRound = S.round; S.evoTarget = S.ascTarget = S.starChoiceTarget = S.ultimate10Target = null;
      S.placing = S.placingEffect = null; S.placingUpgrade = S.placingSpecialization = false; S.pendingStarChoices = [];
      S.squad = Array(16).fill(null); ['Archer', 'Medic', 'Squire', 'Wizard', 'Yeti', 'Gunner', 'Dragon', 'Monk'].forEach((name, index) => { const u = clone(heroes.find(h => h.n === name)); S.squad[[0, 1, 3, 4, 7, 8, 10, 11][index]] = u; });
      S.bench = ['Frog', 'Engineer', 'Lancer'].map(name => clone(heroes.find(h => h.n === name))); render();
    }; qaTeam();
  });
  assert.equal(await page.locator('#ttBenchBar').evaluate(el => el.parentElement.classList.contains('main')), true);
  assert.equal(await page.locator('#ttBenchBar .tt-reserve-card').count(), 3);
  await page.click('.cell[data-i="0"]');
  assert.equal(await page.locator('#ttQuickInspect').isVisible(), true);
  assert.equal(await page.locator('#drawer').evaluate(el => el.classList.contains('open')), false, 'quick inspection keeps battlefield accessible');
  await page.click('[data-tt-quick="move"]'); await page.click('.cell[data-i="2"]');
  assert.equal(await page.evaluate(() => S.squad[2]?.n === 'Archer' && !S.squad[0]), true);
  await page.click('[data-tt-quick="close"]');
  await page.locator('#ttBenchBar [data-tt-mgmt-unit="bench:0"]').click(); await page.click('.cell[data-i="0"]');
  assert.equal(await page.evaluate(() => S.squad[0]?.n === 'Frog' && !S.bench[0]), true);
  console.log('PASS compact inspection, move, and reserve tap deployment');

  await page.click('#ttTeamTools'); await page.click('[data-tt-mgmt-tab="formation"]');
  await page.click('[data-tt-mgmt-save="0"]');
  const saved = await page.evaluate(() => S.squad.map(u => u?.ttId || null));
  await page.click('#close');
  await page.evaluate(() => { TinyTroopsManagement.selectBoard(2); tapCell(5); TinyTroopsManagement.loadFormation(0); });
  assert.deepEqual(await page.evaluate(() => S.squad.map(u => u?.ttId || null)), saved);
  assert.equal(await page.evaluate(() => { const snap = stableSnapshot(); restoreSnapshot(snap); qaStop(); render(); return S.tt.formations[0].board.filter(Boolean).length; }), saved.filter(Boolean).length);
  console.log('PASS formation preset survives rearrangement and save/restore');

  await page.click('#ttVisualSettings'); await page.selectOption('#ttEffectQuality', 'reduced'); await page.locator('#ttEffectQuality').focus(); await page.keyboard.press('Escape');
  assert.equal(await page.evaluate(() => TinyTroopsPresentation.quality), 'reduced');
  await page.click('#ttVisualSettings'); await page.selectOption('#ttEffectQuality', 'full'); await page.click('#ttSettingsDone');
  await page.evaluate(() => {
    fight(); qaStop();
    const a = S.squad.find(Boolean), e = S.enemies[0];
    for (let i = 0; i < 150; i++) attackVisual(a, e, ['fire', 'frost', 'storm', 'poison'][i % 4], 'Special attack');
  });
  assert.ok(await page.evaluate(() => TinyTroopsPresentation.activeEffects <= 48));
  await page.click('#ttPause'); await page.evaluate(() => TinyTroopsPresentation.refresh());
  assert.equal(await page.evaluate(() => TinyTroopsPresentation.activeEffects), 0);
  await page.click('#ttPause'); await page.evaluate(() => qaStop());
  await page.emulateMedia({ reducedMotion: 'reduce' });
  assert.equal(await page.evaluate(() => { const a = S.squad.find(Boolean), e = S.enemies[0]; attackVisual(a, e, 'storm'); return TinyTroopsPresentation.activeEffects; }), 0);
  await page.emulateMedia({ reducedMotion: 'no-preference' });
  const survived = await page.evaluate(() => {
    const a = S.squad.find(Boolean), e = S.enemies[0], node = elForUnit(a), measure = node.getBoundingClientRect;
    TinyTroopsPresentation.clear(); node.getBoundingClientRect = () => { throw new Error('simulated renderer failure'); };
    try { attackVisual(a, e, 'fire'); } finally { node.getBoundingClientRect = measure; }
    const tick = S.battle.tick; TinyTroopsPolish.step(); qaStop(); return !TinyTroopsPolish.recovery && S.battle.tick === tick + 1;
  });
  assert.equal(survived, true, 'render failures never interrupt combat calculations');
  console.log('PASS capped/reused effects, pause cleanup, reduced motion, and renderer exception isolation');
  await page.evaluate(() => { qaTeam(); fight(); qaStop(); document.getElementById('ttPause').click(); render(); detail(S.squad[3]); document.querySelectorAll('.tt-toast').forEach(node => node.remove()); });
  const screenshotDir = process.env.TT_SCREENSHOT_DIR;
  if (screenshotDir) { fs.mkdirSync(screenshotDir, { recursive: true }); await page.screenshot({ path: path.join(screenshotDir, 'tiny-troops-desktop.png'), fullPage: true }); }
  for (const viewport of [{ width: 390, height: 844 }, { width: 360, height: 640 }, { width: 768, height: 1024 }]) {
    await page.setViewportSize(viewport);
    await page.waitForTimeout(100);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'no horizontal overflow');
    await page.locator('#ttTeamTools').tap(); await page.locator('#close').tap();
    await page.locator('#ttArmoryTools').tap(); await page.locator('#close').tap();
    await page.locator('#ttVisualSettings').tap(); await page.locator('#close').tap();
    await page.waitForFunction(() => document.getElementById('grid').getBoundingClientRect().bottom <= document.querySelector('footer').getBoundingClientRect().top + 2);
    const bounds = await page.evaluate(() => { const g = document.getElementById('grid').getBoundingClientRect(), f = document.querySelector('footer').getBoundingClientRect(); return { grid: g.width, bottom: g.bottom, footer: f.top }; });
    assert.ok(bounds.grid > 150 && bounds.bottom <= bounds.footer + 2, JSON.stringify(bounds));
    if (screenshotDir && viewport.width === 390) await page.screenshot({ path: path.join(screenshotDir, 'tiny-troops-mobile.png'), fullPage: true });
  }
  console.log('PASS tablet and touch layouts, dismissal, battlefield bounds');
  assert.deepEqual(errors, []); console.log('All integrated tactical browser checks passed.');
})().catch(error => { console.error(error); if (errors.length) console.error(errors); process.exitCode = 1; }).finally(async () => { await browser?.close(); await new Promise(resolve => server.close(resolve)); });
