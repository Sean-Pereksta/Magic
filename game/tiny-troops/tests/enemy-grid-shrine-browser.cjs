/* Real scrolling/access to every enemy and shrine bonuses in native combat. */
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
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 }, hasTouch: true }); page.setDefaultTimeout(10000);
  page.on('pageerror', error => errors.push(error.stack)); await page.route('https://www.gstatic.com/**', route => route.abort());
  await page.goto(`http://127.0.0.1:${server.address().port}/game/roguecard.html`);
  await page.waitForFunction(() => window.TinyTroopsPresentation && window.TinyTroopsNavigation);
  await page.fill('#menuUser', 'EnemyGridShrineQA'); await page.fill('#menuPass', 'qa-code'); await page.click('#menuNew');
  await page.evaluate(() => {
    window.qaFreeze = () => { ttClearTimers(); TinyTroopsPolish.timer = null; };
    window.qaPrep = (name = 'Archer') => {
      reset(); qaFreeze(); close(); document.getElementById('mainMenu').classList.remove('open');
      S.round = 12; S.phase = 'recruit'; S.battle = null; S.tt.mode = 'PLAYING';
      S.tt.event = S.tt.wavePlan = S.tt.draft = S.tt.pendingPlacement = null; S.tt.draftedRound = S.round;
      S.tt.eventBonuses = []; S.tt.deferredDecisions = []; S.pendingStarChoices = [];
      S.evoTarget = S.ascTarget = S.starChoiceTarget = S.ultimate10Target = null;
      S.placing = S.placingEffect = null; S.placingUpgrade = S.placingSpecialization = false;
      S.squad = Array(16).fill(null); const source = S.squad[3] = clone(heroes.find(h => h.n === name));
      source.baseMaxHp = source.hp = source.maxHp = 10000; source.baseAtk = source.atk = 20;
      S.bench = [null, null, null]; render(); return source;
    };
    window.qaBattle = (count = 3) => {
      fight(); qaFreeze(); if (S.phase !== 'battle') throw new Error('Battle fixture could not start');
      const planned = mkEnemy(enemyDefs.find(enemy => enemy.n === region().main[0]), 1, false);
      S.enemies = Array.from({ length: count }, (_, i) => {
        const enemy = TinyTroopsRules.copy(planned);
        enemy.hp = enemy.maxHp = 100000 + i; enemy.shield = enemy.armor = 0; return enemy;
      });
      render(); return S.squad[3];
    };
    window.qaShrine = () => { S.tt.event = { id: 'shrine' }; openRecruit(); };
    qaPrep(); qaBattle(34); document.getElementById('ttPause').click(); document.querySelectorAll('.tt-toast').forEach(toast => toast.remove());
  });
  const screenshotDir = process.env.TT_SCREENSHOT_DIR;
  const touch = await page.context().newCDPSession(page);
  if (screenshotDir) fs.mkdirSync(screenshotDir, { recursive: true });
  for (const viewport of [{ width: 1280, height: 900 }, { width: 768, height: 1024 }, { width: 390, height: 844 }, { width: 360, height: 640 }]) {
    await page.setViewportSize(viewport); await page.locator('#enemyRow').scrollIntoViewIfNeeded();
    const geometry = await page.locator('#enemyRow').evaluate(list => {
      list.scrollTop = 0; const style = getComputedStyle(list), box = list.getBoundingClientRect(), cards = [...list.children];
      const rows = [...new Set(cards.map(card => Math.round(card.getBoundingClientRect().top)))];
      const second = cards.find(card => card.getBoundingClientRect().top > rows[0] + 1);
      return { display: style.display, columns: style.gridTemplateColumns.split(' ').length, height: list.clientHeight, scrollHeight: list.scrollHeight, width: list.clientWidth, scrollWidth: list.scrollWidth, overflow: style.overflowY, secondBottom: second?.getBoundingClientRect().bottom, bottom: box.bottom, count: cards.length };
    });
    assert.equal(geometry.display, 'grid'); assert.ok(geometry.columns >= 2); assert.equal(geometry.count, 34);
    assert.ok(geometry.secondBottom <= geometry.bottom, 'at least two complete rows must be visible: ' + JSON.stringify(geometry));
    assert.equal(geometry.overflow, 'auto'); assert.ok(geometry.scrollHeight > geometry.height);
    assert.ok(geometry.scrollWidth <= geometry.width + 1, 'grid must scroll vertically, never sideways');
    if (viewport.width <= 390) {
      const box = await page.locator('#enemyRow').boundingBox(), x = box.x + box.width / 2;
      await touch.send('Input.dispatchTouchEvent', { type: 'touchStart', touchPoints: [{ x, y: box.y + box.height - 12 }] });
      for (let step = 1; step <= 8; step++) await touch.send('Input.dispatchTouchEvent', { type: 'touchMove', touchPoints: [{ x, y: box.y + box.height - 12 - (box.height - 24) * step / 8 }] });
      await touch.send('Input.dispatchTouchEvent', { type: 'touchEnd', touchPoints: [] });
      await page.waitForFunction(() => document.getElementById('enemyRow').scrollTop > 0);
    }
    await page.locator('#enemyRow').hover(); await page.mouse.wheel(0, 400);
    await page.waitForFunction(() => document.getElementById('enemyRow').scrollTop > 0);
    await page.locator('#enemyRow').focus(); await page.keyboard.press('End');
    await page.waitForFunction(() => { const list = document.getElementById('enemyRow'); return list.scrollTop + list.clientHeight >= list.scrollHeight - 2; });
    const last = page.locator('#enemyRow .enemy').last(); await last.tap();
    assert.match(await page.locator('#ttQuickInspect').innerText(), /100033/);
    await page.locator('[data-tt-quick="close"]').tap();
    await page.locator('#enemyRow').evaluate(list => list.scrollTop = 0); await page.locator('#enemyRow').scrollIntoViewIfNeeded();
    if (screenshotDir && (viewport.width === 1280 || viewport.width === 390)) await page.screenshot({ path: path.join(screenshotDir, 'enemy-grid-' + (viewport.width === 1280 ? 'desktop' : 'mobile') + '.png'), fullPage: true });
  }
  console.log('PASS desktop/tablet/phone grids show multiple rows, scroll vertically, and inspect the final enemy');

  await page.setViewportSize({ width: 1280, height: 900 });
  await page.evaluate(() => { qaPrep(); qaShrine(); });
  assert.equal(await page.locator('[data-tt-event]').count(), 9, 'eight blessings and the coin option');
  await page.click('#close'); await page.evaluate(() => TinyTroopsPolish.openDeferredChoice());
  assert.equal(await page.locator('[data-tt-event]').count(), 9, 'postponement keeps every choice');
  await page.click('#close');
  const specs = [
    ['ember', 'Pyro'], ['winter', 'Frost Archer'], ['storm', 'Archer'], ['venom', 'Frog'],
    ['dawn', 'Medic'], ['arcane', 'Wizard'], ['gale', 'Archer'], ['valor', 'Rogue']
  ];
  const measurements = {};
  for (const [id, name] of specs) {
    const samples = [];
    for (const blessed of [false, true]) {
      await page.evaluate(({ name, id, blessed }) => {
        const source = qaPrep(name); if (id === 'storm') source.t = ['Storm', 'Blade'];
        if (blessed) qaShrine();
      }, { name, id, blessed });
      if (blessed) {
        const coins = await page.evaluate(() => S.coins); await page.click(`[data-tt-event="${id}"]`);
        assert.equal(await page.evaluate(() => S.coins), coins, 'blessings spend no coins');
        await page.evaluate(() => { restoreSnapshot(stableSnapshot()); qaFreeze(); close(); });
      }
      samples.push(await page.evaluate(id => {
        const source = qaBattle(), originalChance = chance;
        const sum = () => S.enemies.reduce((total, enemy) => total + enemy.hp, 0);
        try {
          chance = () => false;
          if (id === 'gale') return { speed: bonus(source).spd || 0, base: source.baseSpd };
          if (id === 'winter') return { shield: bonus(source).shield || 0 };
          if (id === 'dawn') { source.hp = 100; heal(source, source, 20); return { healed: source.hp - 100 }; }
          if (id === 'arcane') { const hp = sum(); dmg(source, S.enemies[0], 100, 'hit'); return { damage: hp - sum() }; }
          if (id === 'storm') {
            const contribution = S.tt.eventBonuses.find(b => b.tag === 'LIGHTNING')?.chain || 0;
            const threshold = (bonus(source).chain || 0) - contribution + .04;
            chance = probability => probability > threshold;
          }
          const hp = sum(); allyAtk(source); const first = hp - sum();
          const burn = S.enemies.reduce((n, enemy) => n + (enemy.burn || 0), 0), poison = S.enemies.reduce((n, enemy) => n + (enemy.poison || 0), 0);
          const hitTargets = S.enemies.filter(enemy => enemy.hp < enemy.maxHp).length;
          const beforeSecond = sum(); allyAtk(source);
          return { first, second: beforeSecond - sum(), burn, poison, hitTargets };
        } finally { chance = originalChance; qaFreeze(); }
      }, id));
    }
    measurements[id] = samples;
  }
  const near = (actual, expected) => assert.ok(Math.abs(actual - expected) < .00001, `${actual} != ${expected}`);
  assert.ok(measurements.ember[1].burn > measurements.ember[0].burn);
  near(measurements.winter[1].shield - measurements.winter[0].shield, 8);
  assert.ok(measurements.storm[1].hitTargets > measurements.storm[0].hitTargets, JSON.stringify(measurements.storm));
  near(measurements.venom[1].poison - measurements.venom[0].poison, 1);
  near(measurements.dawn[1].healed / measurements.dawn[0].healed, 1.10);
  near(measurements.arcane[1].damage / measurements.arcane[0].damage, 1.06);
  near(measurements.gale[1].speed - measurements.gale[0].speed, measurements.gale[1].base * .06);
  near(measurements.valor[1].first / measurements.valor[0].first, 1.15);
  near(measurements.valor[1].second, measurements.valor[0].second);
  console.log('PASS all eight shrine blessings retain their real combat effects after save/restore');

  await page.setViewportSize({ width: 360, height: 640 });
  await page.evaluate(() => { qaPrep(); qaShrine(); });
  await page.locator('[data-tt-event="valor"]').tap();
  assert.equal(await page.evaluate(() => S.tt.eventBonuses.some(b => b.tag === 'FAST' && b.opening === .15)), true);
  await page.evaluate(() => { close(); qaShrine(); }); await page.locator('#drawer').evaluate(drawer => drawer.scrollTop = drawer.scrollHeight);
  await page.locator('#close').tap();
  assert.equal(await page.evaluate(() => !document.getElementById('drawer').classList.contains('open')), true);
  assert.deepEqual(errors, []); console.log('PASS touch selection and sticky X with the expanded shrine');
})().catch(error => { console.error(error); if (errors.length) console.error(errors); process.exitCode = 1; }).finally(async () => { await browser?.close(); await new Promise(resolve => server.close(resolve)); });
