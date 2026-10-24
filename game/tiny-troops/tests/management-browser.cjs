/* Actual desktop and touch formation/evolution interactions. */
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), http = require('node:http');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..'), errors = [];
const server = http.createServer((req, res) => {
  const file = path.resolve(root, '.' + new URL(req.url, 'http://local').pathname);
  if (!file.startsWith(root + path.sep)) return res.writeHead(403).end();
  fs.readFile(file, (error, data) => { if (error) return res.writeHead(404).end(); res.setHeader('Content-Type', file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html'); res.end(data); });
});
let browser;
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ headless: true, ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}), args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 }, hasTouch: true }); page.setDefaultTimeout(8000);
  page.on('pageerror', error => errors.push(error.stack)); await page.route('https://www.gstatic.com/**', route => route.abort());
  await page.goto(`http://127.0.0.1:${server.address().port}/game/roguecard.html`); await page.waitForFunction(() => window.TinyTroopsManagement);
  await page.fill('#menuUser', 'ManagementQA'); await page.fill('#menuPass', 'qa-code'); await page.click('#menuNew');
  await page.evaluate(() => {
    window.qaFreeze = () => { ttClearTimers(); TinyTroopsPolish.timer = null; };
    window.qaPrep = () => {
      reset(); qaFreeze(); close(); S.phase = 'recruit'; S.round = 180; S.battle = null; S.tt.mode = 'PLAYING'; S.tt.event = S.tt.pathTarget = null;
      S.tt.deferredChoices = []; S.tt.wavePlan = null; S.evoTarget = S.ascTarget = S.starChoiceTarget = S.ultimate10Target = null; S.pendingStarChoices = [];
      S.placing = S.placingEffect = null; S.placingUpgrade = S.placingSpecialization = false;
      S.squad = Array(16).fill(null); S.squad[0] = clone(heroes.find(u => u.n === 'Squire')); S.squad[3] = clone(heroes.find(u => u.n === 'Archer'));
      S.bench = [clone(heroes.find(u => u.n === 'Medic')), null, null]; S.bossSwapAvailable = false; render();
    }; qaPrep();
  });
  assert.equal(await page.locator('#ttBenchBar .tt-reserve-card').count(), 3);
  assert.equal(await page.locator('#ttBenchBar [data-tt-mgmt-unit="bench:0"] .tt-bench-emoji').innerText(), await page.evaluate(() => S.bench[0].e), 'base troops use their artwork instead of native evolution placeholders');
  await page.evaluate(() => { S.bench[1] = clone(heroes.find(u => u.n === 'Wolf')); render(); });
  await page.click('#ttBenchBar [data-tt-mgmt-unit="bench:0"]'); await page.click('#ttBenchBar [data-tt-mgmt-unit="bench:1"]');
  assert.equal(await page.evaluate(() => TinyTroopsManagement.selected().unit === S.bench[1] && S.bench[0].n === 'Medic' && S.bench[1].n === 'Wolf'), true, 'tapping another reserve changes selection without swapping troops');
  await page.evaluate(() => { TinyTroopsManagement.cancelPlacement(); S.bench[1] = null; render(); });
  await page.click('#ttBenchBar [data-tt-mgmt-unit="bench:0"]'); assert.ok(await page.locator('.tt-legal-tile').count() > 0);
  await page.click('.cell[data-i="1"]'); assert.equal(await page.evaluate(() => S.squad[1].n === 'Medic' && S.bench[0] === null && S.phase === 'recruit'), true);
  await page.evaluate(() => TinyTroopsManagement.selectBoard(1)); await page.click('#ttBenchBar [data-tt-mgmt-unit="bench:2"]');
  assert.equal(await page.evaluate(() => S.bench[2].n === 'Medic' && S.squad[1] === null), true);
  await page.evaluate(() => TinyTroopsManagement.selectBoard(0)); await page.click('.cell[data-i="3"]');
  assert.equal(await page.evaluate(() => S.squad[0].n === 'Archer' && S.squad[3].n === 'Squire'), true);
  await page.click('#ttBenchBar [data-tt-mgmt-unit="bench:2"]'); await page.click('.cell[data-i="3"]');
  assert.equal(await page.evaluate(() => S.squad[3].n === 'Squire' && S.bench[2].n === 'Medic'), true);
  await page.click('[data-tt-mgmt="cancel"]'); assert.equal(await page.evaluate(() => TinyTroopsManagement.selected()), null);
  console.log('PASS tap deployment, bench return, free deployed swap, boss-switch rule, and cancel');

  await page.evaluate(() => { TinyTroopsManagement.open('formation'); });
  await page.click('[data-tt-mgmt-save="0"]'); await page.click('#close');
  await page.evaluate(() => { window.qaFormation = S.squad.map(u => u?.ttId || null); TinyTroopsManagement.autoArrange(); TinyTroopsManagement.loadFormation(0); });
  assert.deepEqual(await page.evaluate(() => S.squad.map(u => u?.ttId || null)), await page.evaluate(() => qaFormation));
  await page.evaluate(() => { const snap = stableSnapshot(); restoreSnapshot(snap); qaFreeze(); close(); });
  assert.equal(await page.evaluate(() => S.tt.formations[0].board.filter(Boolean).length), 2);
  console.log('PASS preset save, auto arrange, recall, and native save migration');

  await page.evaluate(() => { qaPrep(); });
  await page.locator('#ttBenchBar [data-tt-mgmt-unit="bench:0"]').dragTo(page.locator('.cell[data-i="2"]'));
  assert.equal(await page.evaluate(() => S.squad[2]?.n === 'Medic' && S.bench[0] === null), true);
  await page.locator('.cell[data-i="2"]').dragTo(page.locator('#ttBenchBar [data-tt-mgmt-unit="bench:1"]'));
  assert.equal(await page.evaluate(() => S.bench[1]?.n === 'Medic' && S.squad[2] === null), true);
  await page.locator('#ttBenchBar [data-tt-mgmt-unit="bench:1"]').dragTo(page.locator('.cell[data-i="0"]'));
  assert.equal(await page.evaluate(() => S.squad[0].n === 'Squire' && S.bench[1].n === 'Medic'), true);
  assert.equal(await page.evaluate(() => TinyTroopsManagement.selected()), null);
  console.log('PASS drag to valid tile and bench, invalid drop retains formation');

  const rosterCheck = await page.evaluate(() => {
    const failures = [];
    for (const hero of heroes.filter(validHero)) {
      qaPrep(); const reserve = clone(hero); S.bench[0] = reserve; render();
      TinyTroopsManagement.selectBench(0); tapCell(2);
      if (S.squad[2] !== reserve || S.bench[0] !== null) failures.push(hero.n);
      TinyTroopsManagement.selectBoard(2); document.querySelector('#ttBenchBar [data-tt-mgmt-unit="bench:1"]').click();
      if (S.bench[1] !== reserve || S.squad[2] !== null) failures.push(hero.n + ' return');
    }
    return { failures, count: heroes.filter(validHero).length };
  });
  assert.deepEqual(rosterCheck.failures, []); console.log('PASS all ' + rosterCheck.count + ' valid native troop types deploy and return with their original identity');

  await page.evaluate(() => { qaPrep(); const u = S.squad[0]; u.star = 5; window.qaBeforeEvolution = JSON.stringify(u); openEvolution(u); });
  assert.equal(await page.locator('.tt-evolution-comparison section').count(), 2);
  await page.click('[data-tt-evolution-preview="1"]'); assert.equal(await page.evaluate(() => JSON.stringify(S.squad[0]) === qaBeforeEvolution), true);
  await page.click('[data-tt-mgmt="later"]'); assert.equal(await page.evaluate(() => !S.evoTarget && !S.squad[0].evo && S.phase === 'recruit'), true);
  assert.ok(await page.evaluate(() => TinyTroopsPolish.pendingDecisionCount()) >= 1);
  await page.evaluate(() => { TinyTroopsPolish.openDeferredChoice(); });
  // Deferred choices also route through preview; a saved native view can initially contain the old choice buttons.
  if (await page.locator('[data-tt-evolution-preview]').count() === 0) await page.evaluate(() => openEvolution(S.evoTarget));
  await page.click('[data-tt-evolution-preview="2"]');
  const expected = await page.evaluate(() => { const u = S.evoTarget, option = S.evoChoices[2], next = JSON.parse(JSON.stringify(u)); option.apply(next); applySelectedUpgradeBoost(next, 5, 'evolution'); next.hp = next.maxHp; next.shield = (next.shield || 0) + 35; return { hp: next.maxHp, atk: next.atk, spd: next.spd, shield: next.shield, evo: next.evo }; });
  await page.click('[data-tt-evolution-confirm]');
  assert.deepEqual(await page.evaluate(() => { const u = S.squad[0]; return { hp: u.maxHp, atk: u.atk, spd: u.spd, shield: u.shield, evo: u.evo }; }), expected);
  assert.equal(await page.evaluate(() => S.phase === 'recruit' && !S.evoTarget && !S.battle), true);
  assert.equal(await page.evaluate(() => TinyTroopsPolish.pendingDecisionCount()), 0);
  await page.evaluate(() => { const snap = stableSnapshot(); restoreSnapshot(snap); qaFreeze(); close(); });
  assert.equal(await page.evaluate(() => !!S.squad[0].evo && S.squad[0].baseMaxHp === S.squad[0].maxHp), true);
  console.log('PASS real native evolution previews, Later, deferred reopen, explicit confirm, stats, and save consistency');

  await page.evaluate(() => { S.squad[0].star = 6; S.phase = 'recruit'; openEvolution(S.squad[0]); });
  assert.match(await page.locator('#dt').innerText(), /Mastery/); await page.click('[data-tt-evolution-preview="1"]');
  const masteryExpected = await page.evaluate(() => { const u = S.ascTarget, next = JSON.parse(JSON.stringify(u)); S.ascChoices[1].apply(next); next.shield = (next.shield || 0) + 70; return { hp: next.maxHp, atk: next.atk, spd: next.spd, shield: next.shield, ascension: next.ascension }; });
  await page.click('#close'); assert.equal(await page.evaluate(() => !S.ascTarget && !S.squad[0].ascension && !document.getElementById('drawer').classList.contains('open')), true);
  await page.evaluate(() => { TinyTroopsPolish.openDeferredChoice(); }); await page.click('[data-tt-evolution-preview="1"]'); await page.click('[data-tt-evolution-confirm]');
  assert.deepEqual(await page.evaluate(() => { const u = S.squad[0]; return { hp: u.maxHp, atk: u.atk, spd: u.spd, shield: u.shield, ascension: u.ascension }; }), masteryExpected);
  assert.equal(await page.evaluate(() => S.phase === 'recruit' && !S.battle), true);
  console.log('PASS mastery X postponement, side-by-side exact native stats, explicit confirmation, and manual battle start');

  for (const width of [390, 360]) {
    await page.setViewportSize({ width, height: 844 }); await page.evaluate(() => { qaPrep(); });
    await page.tap('#ttBenchBar [data-tt-mgmt-unit="bench:0"]'); await page.tap('.cell[data-i="2"]');
    assert.equal(await page.evaluate(() => S.squad[2].n === 'Medic'), true);
    await page.tap('#ttBenchBar [data-tt-mgmt="open"]'); assert.equal(await page.locator('[data-tt-mgmt-tab]').count(), 5); await page.tap('#close');
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1); assert.equal(overflow, false);
  }
  assert.deepEqual(errors, []); console.log('PASS touch deployment and dismissible team panels on narrow mobile screens');
})().catch(error => { console.error(error); if (errors.length) console.error(errors); process.exitCode = 1; }).finally(async () => { await browser?.close(); await new Promise(resolve => server.close(resolve)); });
