/* Real placement, reserve training, transactions, and saved bench slots. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..'), errors = [];
const server = http.createServer((req, res) => {
  const filename = path.resolve(root, '.' + new URL(req.url, 'http://local').pathname);
  if (!filename.startsWith(root + path.sep)) return res.writeHead(403).end();
  fs.readFile(filename, (error, body) => { if (error) return res.writeHead(404).end(); res.setHeader('Content-Type', filename.endsWith('.js') ? 'text/javascript' : filename.endsWith('.css') ? 'text/css' : 'text/html'); res.end(body); });
});
let browser;
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ headless: true, ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}), args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } }); page.setDefaultTimeout(8000);
  page.on('pageerror', e => errors.push(e.stack));
  await page.route('https://www.gstatic.com/**', route => route.abort());
  await page.goto(`http://127.0.0.1:${server.address().port}/game/roguecard.html`); await page.waitForFunction(() => TinyTroopsPolish?.ready);
  await page.fill('#menuUser', 'BenchQA'); await page.fill('#menuPass', 'qa-code'); await page.click('#menuNew');
  await page.evaluate(() => {
    window.qaFreeze = () => { ttClearTimers(); TinyTroopsPolish.timer = null; };
    window.qaTroop = name => clone(heroes.find(h => h.n === name));
    window.qaPrep = (board = ['Squire'], reserve = [], wave = 180) => {
      reset(); qaFreeze(); close(); S.phase = 'recruit'; S.round = wave; S.tt.event = S.tt.wavePlan = null; S.tt.draftedRound = null;
      S.placing = S.placingEffect = S.evoTarget = S.ascTarget = S.starChoiceTarget = S.ultimate10Target = null; S.placingUpgrade = S.placingSpecialization = false; S.pendingStarChoices = [];
      S.squad = Array(wave >= 250 ? 20 : 16).fill(null); board.forEach((n, i) => S.squad[i] = qaTroop(n));
      S.bench = Array.from({ length: 3 }, (_, i) => reserve[i] ? qaTroop(reserve[i]) : null); render();
    };
    window.qaCard = (type = 'recruit', name = 'Archer') => {
      const c = type === 'recruit' ? { type, unit: qaTroop(name), n: name } : { type, n: 'Star Training', e: '⭐' };
      S.tt.draft = S.choices = [c]; S.tt.offerRound = S.round; choose(0);
    };
  });
  await page.evaluate(() => { qaPrep(); S.squad[0].fx = [{ id: 'shield', rank: 3 }]; S.squad[0].personalGrowthStacks = { kills: 9 }; S.squad[0].dead = true; S.squad[0].hp = 0; window.qaOld = S.squad[0]; qaCard(); });
  await page.click('.cell[data-i="0"]'); assert.match(await page.locator('#dt').innerText(), /Make room/);
  await page.click('[data-tt-replacement="cancel"]');
  assert.equal(await page.evaluate(() => S.squad[0] === qaOld && S.placing.n === 'Archer' && S.bench.every(u => !u)), true);
  await page.click('.cell[data-i="0"]'); await page.click('[data-tt-replacement="bench"]'); await page.evaluate(() => qaFreeze());
  assert.deepEqual(await page.evaluate(() => ({ board: S.squad[0].n, bench: S.bench[0].n, same: S.bench[0] === qaOld, effects: S.bench[0].fx, growth: S.bench[0].personalGrowthStacks })), { board: 'Archer', bench: 'Squire', same: true, effects: [{ id: 'shield', rank: 3 }], growth: { kills: 9 } });
  assert.equal(await page.evaluate(() => S.tt.checkpoint.state.bench[0].n), 'Squire');
  assert.equal(await page.evaluate(() => !S.bench[0].dead && S.bench[0].hp === S.bench[0].maxHp), true);
  console.log('PASS occupied-board choice, cancel, preserved troop identity/equipment/growth, and battle checkpoint');

  await page.evaluate(() => { qaPrep(['Squire'], ['Medic', 'Wolf', 'Wizard']); S.coins = 20; window.qaValue = troopSaleValue(S.squad[0]); qaCard(); });
  await page.click('.cell[data-i="0"]'); assert.equal(await page.locator('[data-tt-replacement="bench"]').isDisabled(), true);
  await page.evaluate(() => { const b = document.querySelector('[data-tt-replacement="sell"]'); b.click(); b.click(); qaFreeze(); });
  assert.equal(await page.evaluate(() => S.tt.checkpoint.state.coins === 20 + qaValue && S.squad[0].n === 'Archer' && S.bench.filter(Boolean).length === 3), true);
  console.log('PASS full bench, sale alternative, and single sale payout on repeated clicks');

  await page.evaluate(() => { qaPrep(); qaCard(); }); await page.click('#benchBtn');
  assert.equal(await page.locator('[data-tt-bench-slot]').count(), 3);
  await page.click('[data-tt-bench-slot="2"]'); await page.evaluate(() => qaFreeze());
  assert.deepEqual(await page.evaluate(() => stableSnapshot().bench.map(u => u?.n || null)), [null, null, 'Archer']);
  await page.evaluate(() => { window.qaSaved = stableSnapshot(); restoreSnapshot(qaSaved); qaFreeze(); });
  assert.deepEqual(await page.evaluate(() => S.bench.map(u => u?.n || null)), [null, null, 'Archer']);
  console.log('PASS new recruit placed in chosen bench slot, including save/restore with empty slots');

  await page.evaluate(() => { qaPrep(['Squire'], [null, 'Archer']); window.qaReserve = S.bench[1]; qaCard('recruit', 'Archer'); });
  await page.click('#benchBtn'); await page.click('[data-tt-bench-slot="1"]'); await page.evaluate(() => qaFreeze());
  assert.equal(await page.evaluate(() => S.bench[1] === qaReserve && S.bench[1].star === 3 && S.bench[1].starProg === 0 && S.squad[0].star === 1), true);
  await page.evaluate(() => { qaPrep(['Squire'], [null, 'Archer']); qaCard('upgrade'); }); await page.click('#benchBtn');
  assert.equal(await page.locator('[data-tt-bench-slot="0"]').isDisabled(), true);
  await page.click('[data-tt-bench-slot="1"]'); await page.evaluate(() => qaFreeze());
  assert.equal(await page.evaluate(() => S.bench[1].star === 2 && !S.placingUpgrade && S.squad[0].star === 1), true);
  console.log('PASS exact duplicate gives +2 star progress; Star Training gives +1 to the selected reserve');

  await page.evaluate(() => { qaPrep(['Squire'], ['Archer'], 4); S.squad[0].ttPath = TinyTroopsRules.pathsFor(S.squad[0])[0].id; close(); openRecruit(); });
  assert.equal(await page.evaluate(() => S.choices.some(c => c.type === 'tt-path')), false);
  await page.evaluate(() => { qaPrep([], ['Medic'], 4); close(); openRecruit(); });
  assert.equal(await page.evaluate(() => S.choices.length === 3 && S.choices.every(c => c.type === 'recruit')), true);
  console.log('PASS drafts always have applicable targets when only reserves lack a specialization or the board is empty');

  await page.evaluate(() => { qaPrep(['Squire'], ['Medic']); window.qaReserve = S.bench[0]; qaCard(); }); await page.click('#benchBtn'); await page.click('[data-tt-bench-slot="0"]');
  await page.click('[data-tt-replacement="bench"]'); await page.evaluate(() => qaFreeze());
  assert.equal(await page.evaluate(() => S.bench[0].n === 'Archer' && S.bench[1] === qaReserve), true);
  console.log('PASS occupied-bench replacement also offers Bench or Sell');

  const paths = await page.evaluate(() => {
    const failures = [];
    for (const name of ['Archer', 'Medic']) {
      qaPrep(['Squire'], [null, name]); const u = S.bench[1]; u.star = 4; u.starProg = starNeed(u) - 1; qaCard('upgrade'); openBench(); document.querySelector('[data-tt-bench-slot="1"]').click(); qaFreeze();
      if (S.evoTarget !== u || S.phase !== 'evolve') failures.push(name + ': missing bench evolution');
      pickEvolution(0); qaFreeze(); if (!u.evo || S.phase !== 'battle') failures.push(name + ': unresolved bench evolution');
      S.phase = 'recruit'; S.battle = null; S.tt.checkpoint = null; u.star = 5; u.starProg = starNeed(u) - 1; qaCard('upgrade'); openBench(); document.querySelector('[data-tt-bench-slot="1"]').click(); qaFreeze();
      if (S.ascTarget !== u) failures.push(name + ': missing bench mastery');
      pickAscension(0); qaFreeze(); if (!u.ascension || S.phase !== 'battle') failures.push(name + ': unresolved bench mastery');
      for (const star of [7, 8, 9, 10, 11, 12, 13]) {
        S.phase = 'recruit'; S.battle = null; S.tt.checkpoint = null; u.star = star; u.id ||= u.ttId || name + '-reserve'; S.pendingStarChoices = [{ unitId: u.id, unit: u, star }]; close();
        processNextStarChoice(); if (S.starChoiceTarget !== u || !S.starChoiceChoices.length) failures.push(name + ': missing ' + star + '-star bench choice');
        pickStarChoice(0); qaFreeze(); if (S.starChoiceTarget) failures.push(name + ': unresolved ' + star + '-star bench choice');
      }
    }
    return failures;
  }); assert.deepEqual(paths, []);
  await page.evaluate(() => { qaPrep(['Squire'], [null, 'Medic']); const u = S.bench[1]; u.star = 8; u.evo = 'qa'; u.ascension = 'qa'; u.id = 'reserve-star-id'; u.lateChoiceNames = { 7: 'Already chosen' }; S.pendingStarChoices = [{ unitId: u.id, unit: u, star: 8 }]; restoreSnapshot(stableSnapshot()); qaFreeze(); close(); processNextStarChoice(); });
  const restoredChoice = await page.evaluate(() => ({ same: S.starChoiceTarget === S.bench[1], star: S.starChoiceStar, target: S.starChoiceTarget?.n, benchId: S.bench[1]?.id, queued: S.pendingStarChoices, phase: S.phase, title: document.getElementById('dt').textContent }));
  assert.equal(restoredChoice.same && restoredChoice.star === 8, true, JSON.stringify(restoredChoice));
  await page.evaluate(() => { pickStarChoice(0); qaFreeze(); });
  await page.evaluate(() => {
    S.phase = 'recruit'; close(); const u = S.bench[1]; u.star = 9; S.pendingStarChoices = [{ unitId: u.id, unit: u, star: 9 }]; processNextStarChoice();
    const snapshot = stableSnapshot(); restoreSnapshot(snapshot); qaFreeze(); processNextStarChoice();
  });
  assert.equal(await page.evaluate(() => S.starChoiceTarget === S.bench[1] && S.starChoiceStar === 9), true);
  await page.evaluate(() => { pickStarChoice(0); qaFreeze(); });
  console.log('PASS 5–13 star bench upgrades for damage/support troops and restored queued upgrade targets');

  await page.evaluate(() => { qaPrep([], []); qaCard(); }); await page.click('#benchBtn'); await page.click('[data-tt-bench-slot="1"]');
  assert.equal(await page.evaluate(() => S.phase === 'recruit' && S.tt.draftedRound === S.round), true);
  await page.click('[data-tt-bench-slot="1"]'); await page.click('[data-tt-bench-deploy="1"]'); await page.click('[data-tt-bench-board="3"]');
  assert.equal(await page.evaluate(() => S.squad[3].n === 'Archer' && S.bench[1] === null && !S.bossSwapAvailable), true);
  await page.click('#playNext'); await page.evaluate(() => qaFreeze()); assert.equal(await page.evaluate(() => S.phase), 'battle');
  await page.evaluate(() => { qaPrep(['Squire'], ['Archer']); S.bossSwapAvailable = true; }); await page.click('#benchBtn'); await page.click('[data-tt-bench-slot="0"]'); await page.click('[data-tt-bench-deploy="0"]'); await page.click('[data-tt-bench-board="0"]');
  assert.equal(await page.evaluate(() => S.squad[0].n === 'Archer' && S.bench[0].n === 'Squire' && !S.bossSwapAvailable), true);
  console.log('PASS first recruit on bench remains deployable, free empty-square deployment, and one post-boss swap');

  await page.evaluate(() => { qaPrep(['Squire'], ['Archer']); fight(); qaFreeze(); }); await page.click('#benchBtn');
  assert.equal(await page.evaluate(() => S.tt.mode), 'PAUSED'); await page.click('[data-tt-bench-slot="0"]');
  assert.equal(await page.locator('[data-tt-bench-sell]').count(), 0); await page.click('#close'); assert.equal(await page.evaluate(() => S.tt.mode), 'PLAYING'); await page.evaluate(() => qaFreeze());
  console.log('PASS battle bench inspection pauses/resumes and cannot mutate reserves during combat');

  for (const viewport of [{ width: 1280, height: 900 }, { width: 390, height: 844 }, { width: 360, height: 640 }]) {
    await page.setViewportSize(viewport); await page.evaluate(() => { qaPrep(['Squire'], ['Archer']); qaCard(); });
    assert.equal(await page.locator('#benchBtn').isVisible(), true); await page.click('#benchBtn');
    const bounds = await page.locator('.tt-bench-slots').evaluate(el => { const r = el.getBoundingClientRect(); const d = document.getElementById('drawer'), ds = getComputedStyle(d); return { left: r.left, right: r.right, width: innerWidth, drawer: d.getBoundingClientRect().toJSON(), boxSizing: ds.boxSizing, cssWidth: ds.width, minWidth: ds.minWidth, bodyWidth: getComputedStyle(document.getElementById('body')).width, transform: ds.transform };  });
    assert.ok(bounds.left >= 0 && bounds.right <= bounds.width + 1, JSON.stringify(bounds));
    await page.click('[data-tt-bench-slot="2"]'); await page.evaluate(() => qaFreeze());
  }
  console.log('PASS visible, clickable bench button and three-slot layout on desktop and mobile');
  await page.evaluate(() => openMainMenu()); await page.reload(); await page.waitForFunction(() => TinyTroopsPolish?.ready);
  await page.click('[data-tt-load-save="benchqa"]'); await page.evaluate(() => { ttClearTimers(); TinyTroopsPolish.timer = null; });
  assert.deepEqual(await page.evaluate(() => S.bench.map(u => u?.n || null)), ['Archer', null, 'Archer']);
  assert.equal(await page.evaluate(() => S.tt.draftedRound === S.round), true);
  assert.deepEqual(errors, []); console.log('PASS real page reload and saved-file loading retain bench slots and the consumed draft');
})().catch(error => { console.error(error); if (errors.length) console.error(errors); process.exitCode = 1; }).finally(async () => { await browser?.close(); await new Promise(resolve => server.close(resolve)); });
