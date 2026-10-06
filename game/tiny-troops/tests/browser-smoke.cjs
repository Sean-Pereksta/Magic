/* Run with Playwright installed; BROWSER_EXECUTABLE can select an existing Chromium. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..');
const failures = [];
const server = http.createServer((req, res) => {
  const filename = path.resolve(root, '.' + decodeURIComponent(new URL(req.url, 'http://local').pathname));
  if (!filename.startsWith(root + path.sep)) { res.writeHead(403).end(); return; }
  fs.readFile(filename, (error, content) => {
    if (error) { res.writeHead(404).end(); return; }
    res.setHeader('Content-Type', filename.endsWith('.js') ? 'text/javascript' : filename.endsWith('.css') ? 'text/css' : 'text/html'); res.end(content);
  });
});
let browser;
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ headless: true, ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}), args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } }); page.setDefaultTimeout(8000);
  page.on('pageerror', error => failures.push(error.stack));
  page.on('console', message => { if (message.type() === 'error' && !/net::|Failed to load resource/.test(message.text())) failures.push(message.text()); });
  // Verify the complete local-save flow independently of Firebase availability.
  await page.route('https://www.gstatic.com/**', route => route.abort());
  await page.goto(`http://127.0.0.1:${server.address().port}/game/roguecard.html`);
  await page.waitForFunction(() => window.TinyTroopsPolish?.ready);
  assert.deepEqual(failures, [], 'Boot must not throw');
  assert.equal(await page.evaluate(() => S.tt.mode), 'MENU');
  await page.fill('#menuUser', 'TinyTroopsQA'); await page.fill('#menuPass', 'qa-code'); await page.click('#menuNew');
  assert.equal(await page.locator('#body .choice').count(), 3);
  const draft = await page.evaluate(() => S.choices.map(c => c.n));
  await page.click('#close'); await page.click('#recruit'); assert.deepEqual(await page.evaluate(() => S.choices.map(c => c.n)), draft);
  await page.click('#body .choice:first-child'); await page.click('.cell[data-i="3"]');
  await page.waitForFunction(() => S.battle?.tick >= 1);
  const battleId = await page.evaluate(() => S.battle.id);
  await page.click('#ttPause'); const pausedTick = await page.evaluate(() => S.battle.tick); await page.waitForTimeout(450);
  assert.equal(await page.evaluate(() => S.battle.tick), pausedTick);
  for (const speed of [1, 2, 3]) { await page.click(`[data-tt-speed="${speed}"]`); assert.equal(await page.evaluate(() => S.battle.tick), pausedTick); assert.equal(await page.evaluate(() => S.battle.id), battleId); }
  await page.click('#ttPause'); await page.waitForFunction(t => S.battle?.tick > t, pausedTick);
  const checkpoint = await page.evaluate(() => { saveProfile('QA checkpoint'); return stableSnapshot(); });
  assert.equal(checkpoint.round, 1); assert.equal(checkpoint.squad.filter(Boolean).length, 1);
  await page.evaluate(() => { S.coins += 123; S.kills += 12; restoreSnapshot(stableSnapshot()); }); await page.waitForTimeout(150);
  assert.equal(await page.evaluate(() => S.coins), checkpoint.coins);
  assert.equal(await page.evaluate(() => S.kills), checkpoint.kills);
  assert.equal(await page.evaluate(() => S.tt.draftedRound), 1);
  assert.equal(await page.evaluate(() => document.getElementById('drawer').classList.contains('open')), false);
  await page.click('#playNext'); assert.equal(await page.evaluate(() => S.phase), 'battle');
  await page.click('#ttPause'); await page.click('#restart'); await page.click('[data-tt-action="new"]');
  const held = await page.evaluate(() => ({ tick: S.battle.tick, hp: S.squad[3].hp, id: S.battle.id }));
  await page.click('#ttResume'); assert.deepEqual(await page.evaluate(() => ({ tick: S.battle.tick, hp: S.squad[3].hp, id: S.battle.id })), held);
  await page.evaluate(() => { S.__staleTestFlag = true; openMainMenu(); }); await page.click('#menuNew');
  assert.equal(await page.evaluate(() => S.__staleTestFlag), undefined); assert.equal(await page.evaluate(() => S.round), 1);
  console.log('PASS start/restart, saved drafts, pause, all speeds, checkpoint rollback, menu resume');

  // Controlled fixtures use the shipped combat functions, including native abilities and growth.
  await page.evaluate(() => {
    window.qaArmy = function (round = 1, names = ['Squire', 'Archer', 'Medic']) {
      reset(); document.getElementById('mainMenu').classList.remove('open'); ttClearTimers(); TinyTroopsPolish.timer = null; S.phase = 'recruit'; S.round = round; S.tt.wavePlan = null; S.tt.draft = null; S.tt.event = null; S.tt.draftedRound = null; S.choices = [];
      S.placing = S.placingEffect = S.evoTarget = S.ascTarget = S.starChoiceTarget = S.ultimate10Target = null; S.placingUpgrade = S.placingSpecialization = false; S.pendingStarChoices = [];
      S.squad = Array(round >= 250 ? 20 : 16).fill(null); names.forEach((n, i) => S.squad[i * 4 + 3] = clone(heroes.find(h => h.n === n))); close(); render();
    };
    window.qaStart = function () { fight(); ttClearTimers(); TinyTroopsPolish.timer = null; };
  });
  await page.evaluate(() => {
    qaArmy(20, ['Archer']); const oldArmy = stableSnapshot(); oldArmy.squad.length = 20; oldArmy.squad[19] = oldArmy.squad[3]; oldArmy.squad[3] = null; oldArmy.boardRowsUnlocked = false;
    restoreSnapshot(oldArmy); ttClearTimers(); render();
    if (S.squad.length !== 20 || !S.squad[19]) throw new Error('Occupied legacy fifth row was lost');
    qaArmy(180, ['Archer']); S.squad[3].star = 7; S.squad[3].starProg = 2; S.tt.draftedRound = 180;
    restoreSnapshot(stableSnapshot()); ttClearTimers(); openRecruit();
    if (S.phase !== 'evolve') throw new Error('Legacy high-star evolution did not reopen');
    pickEvolution(0); if (S.phase !== 'ascend') throw new Error('Legacy high-star mastery did not reopen');
    pickAscension(0); ttClearTimers(); TinyTroopsPolish.timer = null;
    if (S.phase !== 'battle' || S.squad[3].star !== 7 || S.squad[3].starProg !== 2) throw new Error('Legacy evolution repair lost star progress');
  });
  console.log('PASS older saves with an occupied fifth row and unfinished high-star evolution');
  const roster = await page.evaluate(() => {
    const pool = [...new Map(heroes.filter(validHero).map(h => [h.n, h])).values()];
    for (const h of pool) {
      qaArmy(2, [h.n, 'Squire']); qaStart(); const u = S.squad[3];
      u.hp = u.maxHp; allyAtk(u); TinyTroopsPolish.step();
      if (![u.hp, u.atk, u.spd].every(Number.isFinite)) throw new Error('Invalid stats: ' + h.n);
      if (!TinyTroopsRules.tags(u).length) throw new Error('Missing role: ' + h.n);
      detail(u); close(); ttClearTimers(); TinyTroopsPolish.timer = null;
    }
    return pool.length;
  });
  console.log(`PASS ${roster} recruits, role guides, targeting, and combat actions`);
  const equipment = await page.evaluate(() => {
    for (const ef of effects) { qaArmy(2, ['Squire']); S.placingEffect = ef; tapCell(3); ttClearTimers(); TinyTroopsPolish.timer = null; if (S.phase !== 'battle') throw new Error('Effect did not start battle: ' + ef.id); TinyTroopsPolish.step(); }
    for (const id of Object.keys(abilityDefs)) { qaArmy(101); S.abilities[id] = 2; qaStart(); S.abilityTimers[id] = 0; TinyTroopsPolish.step(); if (!(S.abilityTimers[id] > 0)) throw new Error('Ability did not tick: ' + id); }
    for (const p of TinyTroopsRules.PATHS) { qaArmy(2, [({ ARMORED: 'Squire', RANGED: 'Archer', FAST: 'Wolf', MAGIC: 'Wizard', MELEE: 'Samurai', SUPPORT: 'Medic' })[p.role]]); S.squad[3].ttPath = p.id; qaStart(); TinyTroopsPolish.step(); }
    return { effects: effects.length, abilities: Object.keys(abilityDefs).length, paths: TinyTroopsRules.PATHS.length };
  });
  console.log('PASS equipment/abilities/specializations', equipment);
  const upgrades = await page.evaluate(() => {
    qaArmy(180, ['Archer']); const u = S.squad[3]; u.star = 4; u.starProg = starNeed(u) - 1; S.placingUpgrade = true; tapCell(3);
    if (S.phase !== 'evolve') throw new Error('5-star evolution missing'); pickEvolution(0); ttClearTimers(); TinyTroopsPolish.timer = null;
    if (!u.evo || S.phase !== 'battle') throw new Error('Evolution did not use the canonical battle flow');
    end(true); ttClearTimers(); S.phase = 'recruit'; S.tt.event = null; u.star = 5; u.starProg = starNeed(u) - 1; S.placingUpgrade = true; tapCell(3);
    if (S.phase !== 'ascend') throw new Error('6-star ascension missing'); pickAscension(0); ttClearTimers(); TinyTroopsPolish.timer = null;
    if (!u.ascension || S.phase !== 'battle') throw new Error('Ascension did not start combat');
    for (const n of ['Archer', 'Medic']) {
      qaArmy(180, [n]); const x = S.squad[3];
      for (const star of [7, 8, 9, 10, 11, 12, 13]) {
        x.star = star; S.phase = 'recruit'; S.battle = null; S.pendingStarChoices = [{ unit: x, unitId: x.id, star }];
        processNextStarChoice();
        if (!S.starChoiceTarget || !S.starChoiceChoices.length) throw new Error(`${n}: missing ${star}-star choice`);
        pickStarChoice(0); ttClearTimers();
        if (S.starChoiceTarget) throw new Error(`${n}: unresolved ${star}-star choice`);
      }
    }
    return '5–13 stars, damage and support paths';
  });
  console.log('PASS upgrades:', upgrades);
  const rewards = await page.evaluate(() => {
    qaArmy(5); qaStart(); S.enemies.forEach(e => { e.dead = true; e.hp = 0; }); TinyTroopsPolish.step(); ttClearTimers(); openRelic();
    if (S.phase !== 'relic' || !S.relicChoices.length) throw new Error('Boss reward missing');
    const s = stableSnapshot(); restoreSnapshot(s); ttClearTimers(); openRelic(); const draft = S.relicChoices.map(r => r.id); openRelic();
    if (JSON.stringify(draft) !== JSON.stringify(S.relicChoices.map(r => r.id))) throw new Error('Reopened relic draft changed');
    pickRelic(0); if (S.phase !== 'shop') throw new Error('Relic did not open store');
    S.coins = 10000; const item = S.items[0], before = S.coins; buy(0); if (S.coins !== before - item.c) throw new Error('Incorrect store cost');
    doneShop(); if (S.round !== 6 || S.phase !== 'recruit') throw new Error('Store did not advance exactly one wave');
    qaArmy(3); qaStart(); S.enemies.forEach(e => { e.dead = true; e.hp = 0; }); TinyTroopsPolish.step(); ttClearTimers(); openRecruit();
    if (S.phase !== 'tt-event') throw new Error('Event missing'); const id = S.tt.event.id; restoreSnapshot(stableSnapshot()); ttClearTimers(); openRecruit();
    if (S.tt.event.id !== id) throw new Error('Event lost on reload'); document.querySelector('[data-tt-event="supplies"]').click();
    if (S.tt.event || S.phase !== 'recruit') throw new Error('Event did not resolve');
    return true;
  });
  assert.ok(rewards); console.log('PASS boss/relic/save/store/event flows');

  const endless = await page.evaluate(() => {
    const data = [];
    for (const wave of [20, 60, 100, 120, 150, 180, 249, 250, 251, 300, 301, 601]) {
      qaArmy(wave); qaStart();
      if (S.phase !== 'battle' || !S.enemies.length) throw new Error('No wave at ' + wave + ': ' + S.phase + ' · ' + S.log[0] + ' · ' + JSON.stringify({ planned: S.tt.wavePlan?.enemies.map(e => e.n), allowed: region().boss }));
      data.push({ wave, enemies: S.enemies.length, slots: S.squad.length, region: region().n });
      S.enemies.forEach(e => { e.dead = true; e.hp = 0; }); TinyTroopsPolish.step(); ttClearTimers();
      if (S.tt.finished || S.tt.mode === 'RESULTS') throw new Error('Endless run ended at ' + wave);
      if (S.phase === 'relic') { openRelic(); if (S.phase === 'relic') pickRelic(0); doneShop(); }
      if (S.round !== wave + 1) throw new Error('Wave did not advance at ' + wave);
    }
    return data;
  });
  console.log('PASS endless regions through wave 601:', endless);
  const stale = await page.evaluate(() => {
    qaArmy(2); qaStart(); const u = S.squad[3], e = S.enemies[0]; u.hp = 1; u.shield = 0; u.poison = 100000; S.battle.tick = 2; TinyTroopsPolish.step();
    const kills = S.kills; const hp = e.hp; allyAtk(u); dmg(u, e, NaN); dmg(u, { ...e }, 10000); if (e.hp !== hp) throw new Error('Dead or invalid source attacked');
    if (S.kills !== kills) throw new Error('Invalid damage gave rewards');
    qaArmy(2); qaStart(); S.squad.forEach(u => { if (u) { u.dead = true; u.hp = 0; } }); TinyTroopsPolish.step();
    const history = TinyTroopsPolish.meta.runs.length; end(false); if (TinyTroopsPolish.meta.runs.length !== history) throw new Error('Duplicate result');
    if (S.tt.mode !== 'RESULTS' || !S.tt.summary || ttRunTimers.size) throw new Error('Defeat left an active timer');
    openMainMenu(); reset(); return { history, mode: S.tt.mode, wave: S.round };
  });
  assert.equal(stale.mode, 'PLAYING'); assert.equal(stale.wave, 1); console.log('PASS status deaths, invalid targets/numbers, results/history, fresh run');
  const performanceResult = await page.evaluate(() => {
    qaArmy(601, ['Archer', 'Medic', 'Squire']);
    for (let i = 0; i < S.squad.length; i++) { const u = S.squad[i] ||= clone(heroes.find(h => h.n === ['Archer', 'Squire', 'Wizard'][i % 3])); u.star = 13; u.evo = 'test'; u.ascension = 'test'; u.latePaths = { 11: 'fortress', 12: 'prime', 13: 'mythic_guardian' }; u.path11 = 'fortress'; u.path12 = 'prime'; u.path13 = 'mythic_guardian'; u.baseMaxHp = u.maxHp = u.hp = 1e9; u.baseAtk = u.atk = 1; }
    qaStart(); S.enemies.forEach(e => { e.hp = e.maxHp = 1e9; e.atk = 1; });
    const grid = document.getElementById('grid'), nodes = [...grid.children]; const start = performance.now();
    for (let i = 0; i < 800; i++) { TinyTroopsPolish.step(); if (S.phase !== 'battle') throw new Error('Long-run fixture ended prematurely'); }
    const elapsed = performance.now() - start;
    if (S.battle.tick !== 800) throw new Error('Simulation did not advance 800 steps');
    if (nodes.some((n, i) => n !== grid.children[i])) throw new Error('Board rebuilt during simulation');
    if (TinyTroopsPolish.fx.size > 32 || ttRunTimers.size > 250 || window.__bonusDmgQueue.length > 50) throw new Error('Unbounded effects or timers: ' + JSON.stringify({effects:TinyTroopsPolish.fx.size,timers:ttRunTimers.size,queue:window.__bonusDmgQueue.length,elapsed}));
    return { steps: 800, simulationSeconds: 200, elapsedMs: Math.round(elapsed), nodes: document.querySelectorAll('*').length, timers: ttRunTimers.size, effects: TinyTroopsPolish.fx.size };
  });
  console.log('PASS large-army performance:', performanceResult);
  await page.evaluate(() => { openMainMenu(); reset(); document.getElementById('mainMenu').classList.remove('open'); close(); render(); });
  for (const size of [{ width: 1280, height: 900 }, { width: 390, height: 844 }, { width: 768, height: 1024 }, { width: 360, height: 640 }]) {
    await page.setViewportSize(size);
    const bounds = await page.evaluate(() => { const g = document.getElementById('grid').getBoundingClientRect(), f = document.querySelector('footer').getBoundingClientRect(); return { width: document.documentElement.scrollWidth, viewport: innerWidth, gridBottom: g.bottom, footerTop: f.top, gridWidth: g.width, height: g.height }; });
    if (bounds.width > bounds.viewport) console.log('Overflow elements:', await page.evaluate(() => [...document.querySelectorAll('*')].map(el => ({ id: el.id, c: el.className, right: el.getBoundingClientRect().right, left: el.getBoundingClientRect().left })).filter(x => x.right > innerWidth + 2 || x.left < -2).slice(0, 20)));
    assert.ok(bounds.width <= bounds.viewport, JSON.stringify(bounds)); assert.ok(bounds.gridBottom <= bounds.footerTop + 2, JSON.stringify(bounds));
    await page.click('#help'); await page.locator('#drawer.open').waitFor({ state: 'visible' }); await page.click('#close');
  }
  console.log('PASS desktop/tablet/mobile layout and one-click dialogs');
  assert.deepEqual(failures, [], 'No game exceptions or combat errors are allowed');
  console.log('All Tiny Troops browser checks passed.');
})().catch(error => { console.error(error); if (failures.length) console.error(failures); process.exitCode = 1; }).finally(async () => { await browser?.close(); await new Promise(resolve => server.close(resolve)); });
