/* Exercise required-choice collisions through actual mouse, keyboard, and touch. */
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), http = require('node:http');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..'), errors = [];
const server = http.createServer((req, res) => {
  const file = path.resolve(root, '.' + new URL(req.url, 'http://local').pathname);
  if (!file.startsWith(root + path.sep)) return res.writeHead(403).end();
  fs.readFile(file, (error, body) => {
    if (error) return res.writeHead(404).end();
    res.setHeader('Content-Type', file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html'); res.end(body);
  });
});
let browser;
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ headless: true, ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}), args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 }, hasTouch: true }); page.setDefaultTimeout(8000);
  page.on('pageerror', error => errors.push(error.stack));
  await page.route('https://www.gstatic.com/**', route => route.abort());
  await page.goto(`http://127.0.0.1:${server.address().port}/game/roguecard.html`); await page.waitForFunction(() => TinyTroopsPolish?.ready);
  await page.fill('#menuUser', 'PopupQA'); await page.fill('#menuPass', 'qa-code'); await page.click('#menuNew');
  await page.evaluate(() => {
    window.qaFreeze = () => { ttClearTimers(); TinyTroopsPolish.timer = null; };
    window.qaPrep = (name = 'Archer', wave = 180) => {
      reset(); qaFreeze(); close(); S.phase = 'recruit'; S.round = wave;
      S.tt.event = S.tt.wavePlan = S.tt.draft = null; S.tt.draftedRound = S.tt.offerRound = null;
      S.placing = S.placingEffect = S.evoTarget = S.ascTarget = S.starChoiceTarget = S.ultimate10Target = null;
      S.placingUpgrade = S.placingSpecialization = false; S.pendingStarChoices = []; S.tt.pathTarget = null;
      S.squad = Array(16).fill(null); S.squad[3] = clone(heroes.find(h => h.n === name)); S.bench = Array(3).fill(null); render();
    };
    window.qaEvent = id => { S.tt.event = { id }; openRecruit(); };
  });
  const title = () => page.locator('#dt').innerText();
  const freeze = () => page.evaluate(() => qaFreeze());

  // This is the reported freeze: a delayed event replaced an unfinished evolution,
  // then its first click consumed the event while leaving every visible option dead.
  await page.evaluate(() => { qaPrep(); const u = S.squad[3]; u.star = 5; openEvolution(u); S.tt.event = { id: 'armory' }; ttTimeout(openRecruit, 50); });
  await page.waitForTimeout(150); assert.match(await title(), /Evolution/);
  assert.equal(await page.evaluate(() => S.tt.event.id), 'armory');
  await page.evaluate(() => ttTimeout(openRecruit, 100)); await page.click('#close'); await page.waitForTimeout(200);
  assert.equal(await page.evaluate(() => document.getElementById('drawer').classList.contains('open')), false, 'a delayed draft does not undo Close');
  await page.click('#recruit');
  await page.click('#body .choice:first-child'); await freeze(); assert.match(await title(), /Abandoned Armory/);
  assert.equal(await page.evaluate(() => !!S.squad[3].evo && !S.evoTarget && S.phase === 'tt-event'), true);
  await page.click('[data-tt-event="supplies"]'); assert.match(await title(), /Build your army/);
  assert.equal(await page.evaluate(() => !S.tt.event && S.phase === 'recruit' && !S.evoTarget), true);
  console.log('PASS delayed event/evolution collision resolves both rewards without dead buttons');

  const events = await page.evaluate(() => TinyTroopsRules.EVENTS.map(e => ({ id: e.id, name: e.name, options: e.options })));
  for (const event of events) for (const option of event.options) {
    await page.evaluate(id => { qaPrep(); S.coins = 100; qaEvent(id); }, event.id);
    const choices = await page.locator('#body').innerHTML();
    assert.equal(await page.locator('#close').isEnabled(), true); await page.click('#close');
    assert.equal(await page.evaluate(() => !document.getElementById('drawer').classList.contains('open') && !!S.tt.event), true);
    assert.equal(await page.locator('#recruit').innerText(), 'Continue choice'); await page.click('#recruit');
    assert.equal(await page.locator('#body').innerHTML(), choices);
    // Recover the legitimate choice even if an older callback changed only the phase.
    await page.evaluate(() => S.phase = 'recruit'); await page.click(`[data-tt-event="${option.id}"] strong`);
    assert.equal(await page.evaluate(() => !S.tt.event && S.phase === 'recruit'), true);
    assert.equal(await page.evaluate(() => S.coins), 100 + (option.coins || 0));
    if (option.bonus) assert.deepEqual(await page.evaluate(() => S.tt.eventBonuses), [option.bonus]);
  }
  await page.evaluate(() => { qaPrep(); S.tt.eventBonuses = [null, { tag: 'ARMORED', shield: '8' }]; qaEvent('armory'); });
  await page.click('[data-tt-event="shield"]'); assert.deepEqual(await page.evaluate(() => S.tt.eventBonuses), [{ tag: 'ARMORED', shield: 16 }]);
  console.log('PASS all nine event options, close/reopen, nested clicks, stale phases, and migrated bonuses');

  await page.evaluate(() => { qaPrep(); S.coins = 100; qaEvent('armory'); window.qaOldEvent = { title: document.getElementById('dt').textContent, hint: document.getElementById('dhint').textContent, html: document.getElementById('body').innerHTML }; });
  await page.click('[data-tt-event="supplies"]');
  await page.evaluate(() => show(qaOldEvent.title, qaOldEvent.hint, qaOldEvent.html));
  await page.click('[data-tt-event="supplies"]'); assert.equal(await page.evaluate(() => S.coins), 118); assert.match(await title(), /Build your army/);
  await page.evaluate(() => { qaPrep(); S.coins = 100; qaEvent('shrine'); });
  await page.click('[data-tt-action="skip-event"]'); assert.equal(await page.evaluate(() => S.coins === 100 && !S.tt.event && S.phase === 'recruit'), true);
  await page.evaluate(() => { qaPrep(); S.phase = 'tt-event'; S.tt.event = { id: 'obsolete-event' }; openRecruit(); });
  assert.match(await title(), /Build your army/); assert.equal(await page.evaluate(() => !S.tt.event && S.phase === 'recruit'), true);
  console.log('PASS stale event buttons cannot pay twice, explicit skip, and obsolete-event recovery');

  for (const kind of ['evolution', 'mastery', 'star8', 'star10', 'support11', 'path', 'relic']) {
    await page.evaluate(kind => {
      qaPrep(kind === 'support11' ? 'Medic' : 'Archer'); const u = S.squad[3];
      if (kind === 'evolution') { u.star = 5; openEvolution(u); }
      else if (kind === 'mastery') { u.star = 6; u.evo = 'qa'; openEvolution(u); }
      else if (kind === 'path') { S.placingSpecialization = true; tapCell(3); }
      else if (kind === 'relic') { S.round = 5; S.phase = 'relic'; S.shopOpen = true; openRelic(); }
      else { const star = kind === 'star8' ? 8 : kind === 'star10' ? 10 : 11; u.star = star; u.evo = u.ascension = 'qa'; u.id ||= u.ttId; S.pendingStarChoices = [{ unitId: u.id, unit: u, star }]; processNextStarChoice(); }
      qaFreeze();
    }, kind);
    const beforeTitle = await title(), beforeHtml = await page.locator('#body').innerHTML();
    await page.keyboard.press('Escape'); await page.waitForTimeout(160); assert.equal(await page.evaluate(() => document.getElementById('drawer').classList.contains('open')), false, kind + ' stays dismissed');
    assert.equal(await page.locator('#recruit').isEnabled(), true); await page.click('#recruit');
    assert.equal(await title(), beforeTitle, kind); assert.equal(await page.locator('#body').innerHTML(), beforeHtml, kind);
    await page.click('#close'); await page.click('#restart'); await page.click('[data-tt-action="new"]');
    assert.equal(await page.locator('#ttResume').isVisible(), true); await page.click('#ttResume');
    assert.equal(await title(), beforeTitle, kind); assert.equal(await page.locator('#body').innerHTML(), beforeHtml, kind);
    await page.evaluate(() => { S.phase = 'recruit'; }); await page.click('#body .choice:first-child'); await freeze();
    const resolved = await page.evaluate(kind => kind === 'evolution' ? !!S.squad[3].evo && !S.evoTarget : kind === 'mastery' ? !!S.squad[3].ascension && !S.ascTarget : kind === 'path' ? !!S.squad[3].ttPath && !S.tt.pathTarget : kind === 'relic' ? S.relics.length === 1 && S.phase === 'shop' : !S.starChoiceTarget && !S.ultimate10Target, kind);
    assert.equal(resolved, true, kind);
  }
  console.log('PASS evolution, mastery, stars 8/10/11, specialization, and relics close/resume with identical choices and reachable menus');

  for (const wave of [1, 3, 5, 6, 300]) {
    await page.evaluate(wave => { qaPrep('Archer', wave); fight(); qaFreeze(); end(true); qaFreeze(); }, wave);
    assert.equal(await page.locator('#playNext').isVisible(), true, 'next step after wave ' + wave);
    assert.equal(await page.locator('#playNext').isEnabled(), true, 'actionable after wave ' + wave);
    if (wave % 5 === 0) {
      assert.equal(await page.locator('#playNext').innerText(), 'Continue choice'); await page.click('#playNext'); assert.match(await title(), /Boss relic/);
      await page.click('#body .choice:first-child'); await page.click('#close');
      assert.equal(await page.locator('#playNext').innerText(), 'Finish store →'); await page.click('#playNext');
      assert.equal(await page.evaluate(() => S.round), wave + 1); assert.equal(await page.evaluate(() => S.shopOpen), false);
    } else if (wave % 3 === 0) {
      assert.equal(await page.locator('#playNext').innerText(), 'Continue choice'); await page.click('#playNext'); await page.click('[data-tt-event="supplies"]');
    }
    if (await page.evaluate(() => document.getElementById('drawer').classList.contains('open'))) await page.click('#close');
    assert.equal(await page.locator('#playNext').innerText(), '▶ Start wave'); await page.click('#playNext'); await freeze();
    assert.equal(await page.evaluate(() => S.phase), 'battle');
    const id = await page.evaluate(() => S.battle.id); await page.evaluate(() => startNextFight());
    assert.equal(await page.evaluate(() => S.battle.id), id, 'repeated start does not restart the wave');
    await page.click('#ttPause'); assert.equal(await page.locator('#playNext').innerText(), '▶ Resume'); await page.click('#playNext'); await freeze();
    assert.equal(await page.evaluate(() => S.tt.mode), 'PLAYING');
  }
  await page.evaluate(() => { qaPrep(); S.squad.fill(null); render(); });
  assert.equal(await page.locator('#playNext').innerText(), 'Choose a recruit'); await page.click('#playNext'); await page.click('#body .choice:first-child');
  assert.equal(await page.locator('#playNext').isVisible(), true); assert.equal(await page.locator('#playNext').isDisabled(), true);
  assert.equal(await page.locator('#playNext').innerText(), 'Place your card'); await page.click('.cell[data-i="3"]'); await freeze();
  await page.evaluate(() => { qaPrep(); S.bench[0] = S.squad[3]; S.squad.fill(null); render(); });
  assert.equal(await page.locator('#playNext').innerText(), 'Deploy a troop'); await page.click('#playNext'); assert.equal(await page.locator('[data-tt-bench-slot]').count(), 3);
  await page.click('#close'); await page.evaluate(() => { S.tt.finished = true; S.phase = 'dead'; render(); });
  assert.equal(await page.locator('#playNext').innerText(), 'Start new run'); await page.click('#playNext'); assert.equal(await page.locator('#mainMenu').isVisible(), true);
  await page.click('#menuNew'); await freeze();
  console.log('PASS visible next step after normal, event, and boss victories; store completion, one battle start, pause/resume, empty-board recruitment/deployment, and finished runs');

  for (const viewport of [{ width: 1280, height: 900 }, { width: 390, height: 844 }, { width: 360, height: 640 }, { width: 360, height: 420 }]) {
    await page.setViewportSize(viewport); await page.evaluate(() => { qaPrep(); qaEvent('hospital'); });
    await page.locator('#drawer').evaluate(el => el.scrollTop = el.scrollHeight);
    const reachable = await page.locator('#close').evaluate(el => { const r = el.getBoundingClientRect(); return r.top >= 0 && r.bottom <= innerHeight && document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2) === el; });
    assert.equal(reachable, true, JSON.stringify(viewport)); await page.tap('#close'); await page.tap('#recruit'); await page.tap('[data-tt-event="medics"]');
    assert.equal(await page.evaluate(() => !S.tt.event), true);
    await page.tap('#close');
    const nextBounds = await page.locator('#playNext').evaluate(el => { const r = el.getBoundingClientRect(); return { top: r.top, bottom: r.bottom, height: innerHeight, width: r.width, footer: el.closest('footer').getBoundingClientRect().width }; });
    assert.ok(nextBounds.top >= 0 && nextBounds.bottom <= nextBounds.height && nextBounds.width >= nextBounds.footer * .8, JSON.stringify(nextBounds));
    await page.evaluate(() => qaEvent('armory')); await page.tap('#shade', { position: { x: 8, y: 8 } });
    assert.equal(await page.evaluate(() => !document.getElementById('drawer').classList.contains('open') && !!S.tt.event), true);
    await page.tap('#recruit'); await page.tap('[data-tt-action="skip-event"]');
  }
  console.log('PASS clickable, sticky Close and touch event choices on desktop, mobile, and short screens');

  await page.setViewportSize({ width: 1280, height: 900 });
  const savedKey = await page.evaluate(() => { qaPrep(); S.squad[3].star = 5; openEvolution(S.squad[3]); S.tt.event = { id: 'armory' }; saveProfile('pending popup reload'); return cleanUsername(document.getElementById('menuUser').value); });
  await page.reload(); await page.waitForFunction(() => TinyTroopsPolish?.ready);
  await page.click(`[data-tt-load-save="${savedKey}"]`); await page.waitForFunction(() => S.evoTarget && document.getElementById('drawer').classList.contains('open'));
  assert.match(await title(), /Evolution/); await page.click('#body .choice:first-child'); await page.evaluate(() => { ttClearTimers(); TinyTroopsPolish.timer = null; });
  assert.match(await title(), /Abandoned Armory/); await page.click('[data-tt-event="supplies"]'); assert.equal(await page.evaluate(() => !S.tt.event && !!S.squad[3].evo), true);
  assert.deepEqual(errors, []); console.log('PASS real save/reload restores overlapping rewards in order without UI errors');
  console.log('All Tiny Troops popup and next-wave checks passed.');
})().catch(error => { console.error(error); if (errors.length) console.error(errors); process.exitCode = 1; }).finally(async () => { await browser?.close(); await new Promise(resolve => server.close(resolve)); });
