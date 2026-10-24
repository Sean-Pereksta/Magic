/* Browser history, including asynchronous close/open races and real mobile Back. */
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), http = require('node:http');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..'), errors = [];
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://local');
  if (url.pathname === '/previous') { res.setHeader('Content-Type', 'text/html'); return res.end('<!doctype html><title>Previous page</title><p id="previous">Previous page</p>'); }
  const file = path.resolve(root, '.' + url.pathname);
  if (!file.startsWith(root + path.sep)) return res.writeHead(403).end();
  fs.readFile(file, (error, data) => { if (error) return res.writeHead(404).end(); res.setHeader('Content-Type', file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html'); res.end(data); });
});
let browser, page;
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ headless: true, ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}), args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] });
  page = await browser.newPage({ viewport: { width: 390, height: 844 }, hasTouch: true }); page.setDefaultTimeout(8000);
  page.on('pageerror', error => errors.push(error.stack)); await page.route('https://www.gstatic.com/**', route => route.abort());
  const origin = `http://127.0.0.1:${server.address().port}`;
  const waitClean = () => page.waitForFunction(() => { const state = TinyTroopsNavigation.status(); return !state.entry && !state.cleaning && !state.active; });
  const waitEntry = kind => page.waitForFunction(kind => { const state = TinyTroopsNavigation.status(); return state.entry && !state.cleaning && state.active === kind; }, kind);
  await page.goto(origin + '/previous'); await page.goto(origin + '/game/roguecard.html'); await page.waitForFunction(() => window.TinyTroopsNavigation);
  await page.evaluate(() => {
    window.qaFreeze = () => { ttClearTimers(); TinyTroopsPolish.timer = null; };
    window.qaPrep = () => {
      document.getElementById('mainMenu').classList.remove('open'); reset(); qaFreeze(); close();
      S.phase = 'recruit'; S.round = 180; S.tt.mode = 'PLAYING'; S.tt.started = true; S.battle = null;
      S.tt.event = S.tt.pathTarget = S.tt.pendingPlacement = null; S.tt.deferredDecisions = []; S.pendingStarChoices = [];
      S.evoTarget = S.ascTarget = S.starChoiceTarget = S.ultimate10Target = null; S.placing = S.placingEffect = null; S.placingUpgrade = S.placingSpecialization = false;
      S.squad = Array(16).fill(null); S.squad[3] = clone(heroes.find(u => u.n === 'Squire')); S.bench = [clone(heroes.find(u => u.n === 'Medic')), null, null];
      S.coins = 100; S.bossSwapAvailable = false; render();
    };
    window.qaCard = () => { const c = { type: 'recruit', unit: clone(heroes.find(u => u.n === 'Archer')), n: 'Archer' }; S.tt.draft = S.choices = [c]; S.tt.offerRound = S.round; choose(0); };
    qaPrep();
  });
  await waitClean();
  await page.evaluate(() => { history.replaceState({ host: 'preserved', nested: { value: 7 } }, '', location.href + '?navigation-qa=1'); window.qaLength = history.length; show('Ability information', 'Read without spending resources.', '<p>Ability details</p>'); });
  await waitEntry('dialog'); await page.goBack(); await waitClean();
  assert.equal(await page.evaluate(() => document.getElementById('drawer').classList.contains('open')), false);
  assert.deepEqual(await page.evaluate(() => history.state), { host: 'preserved', nested: { value: 7 } });
  assert.equal(await page.evaluate(() => S.coins), 100); assert.match(page.url(), /navigation-qa=1/);
  console.log('PASS real mobile Back dismisses a dialog and preserves host URL, state, and resources');

  await page.evaluate(() => { TinyTroopsManagement.open('formation'); }); await waitEntry('dialog'); await page.goBack(); await waitClean();
  await page.evaluate(() => { TinyTroopsEquipment.openInventory(); }); await waitEntry('dialog'); await page.goBack(); await waitClean();
  assert.equal(await page.evaluate(() => S.coins), 100);
  console.log('PASS formation and equipment interfaces dismiss with Back');

  await page.evaluate(() => { qaPrep(); S.squad[3].star = 5; openEvolution(S.squad[3]); }); await waitEntry('dialog'); await page.goBack(); await waitClean();
  assert.equal(await page.evaluate(() => !S.evoTarget && !S.squad[3].evo && S.coins === 100 && TinyTroopsPolish.pendingDecisionCount() === 1), true);
  await page.evaluate(() => { qaPrep(); qaCard(); }); await waitEntry('placement'); await page.goBack(); await waitClean();
  assert.equal(await page.evaluate(() => !S.placing && !S.tt.pendingPlacement && S.tt.draft[0].n === 'Archer' && S.squad.filter(Boolean).length === 1 && S.coins === 100), true);
  await page.evaluate(() => { TinyTroopsManagement.selectBench(0); }); await waitEntry('placement'); await page.goBack(); await waitClean();
  assert.equal(await page.evaluate(() => !TinyTroopsManagement.selected() && S.bench[0].n === 'Medic' && S.squad[3].n === 'Squire'), true);
  await page.evaluate(() => detail(S.squad[3])); await waitEntry('inspect'); await page.goBack(); await waitClean();
  assert.equal(await page.locator('#ttQuickInspect').isVisible(), false);
  console.log('PASS Back postpones evolution and cancels native or reserve placement without troop, draft, or coin loss');

  await page.evaluate(() => { qaPrep(); qaCard(); }); await waitEntry('placement'); await page.evaluate(() => openBench()); await waitEntry('dialog');
  await page.goBack(); await waitEntry('placement');
  assert.equal(await page.evaluate(() => !document.getElementById('drawer').classList.contains('open') && S.placing.n === 'Archer'), true);
  await page.goBack(); await waitClean();
  console.log('PASS Back closes the top dialog before canceling its underlying placement');

  await page.evaluate(() => show('First panel', '', '<p>First</p>')); await waitEntry('dialog');
  await page.evaluate(() => {
    close(); TinyTroopsNavigation.sync();
    for (let i = 0; i < 12; i++) { show('Replacement ' + i, '', '<p>Replacement</p>'); close(); TinyTroopsNavigation.sync(); }
    show('Newest panel', '', '<p>The pending cleanup must leave this open.</p>');
  });
  await waitEntry('dialog'); assert.equal(await page.locator('#dt').innerText(), 'Newest panel');
  await page.goBack(); await waitClean();
  await page.evaluate(() => show('Private close', '', '<p>Private close path</p>')); await waitEntry('dialog');
  await page.evaluate(() => { document.getElementById('drawer').classList.remove('open'); document.getElementById('shade').classList.remove('open'); }); await waitClean();
  console.log('PASS asynchronous close/open races retain the newest panel and private forceClose paths release history');

  for (let i = 0; i < 12; i++) {
    await page.evaluate(i => show('Dismiss ' + i, '', '<p>Dismissible</p>'), i); await waitEntry('dialog'); await page.tap('#close'); await waitClean();
  }
  assert.ok(await page.evaluate(() => history.length <= qaLength + 1), 'repeated X does not accumulate history entries');
  assert.deepEqual(await page.evaluate(() => history.state), { host: 'preserved', nested: { value: 7 } });
  await page.evaluate(() => show('Reloaded panel', '', '<p>Reload safely releases this temporary history entry.</p>')); await waitEntry('dialog');
  await page.reload(); await page.waitForFunction(() => window.TinyTroopsNavigation); await waitClean();
  assert.deepEqual(await page.evaluate(() => history.state), { host: 'preserved', nested: { value: 7 } });
  await page.goBack(); await page.waitForSelector('#previous'); assert.equal(page.url(), origin + '/previous');
  assert.deepEqual(errors, []); console.log('PASS repeated X and reload leave no Back traps; Back navigates away when the game has no temporary UI');

  const fallback = await browser.newPage(); fallback.setDefaultTimeout(8000); fallback.on('pageerror', error => errors.push(error.stack));
  await fallback.route('https://www.gstatic.com/**', route => route.abort());
  await fallback.route('**/tiny-troops/navigation.js*', route => route.fulfill({ status: 200, contentType: 'text/javascript', body: '/* Optional navigation module intentionally absent in this compatibility check. */' }));
  await fallback.goto(origin + '/game/roguecard.html'); await fallback.waitForFunction(() => window.TinyTroopsManagement);
  await fallback.evaluate(() => {
    document.getElementById('mainMenu').classList.remove('open'); reset(); ttClearTimers(); TinyTroopsPolish.timer = null; close();
    S.phase = 'recruit'; S.tt.mode = 'PLAYING'; S.battle = null; S.squad = Array(16).fill(null); S.bench = [clone(heroes.find(u => u.n === 'Medic')), null, null]; render();
    history.pushState({ legacy: true }, ''); TinyTroopsManagement.selectBench(0);
  });
  assert.equal(await fallback.evaluate(() => !window.TinyTroopsNavigation && !!TinyTroopsManagement.selected()), true);
  await fallback.goBack(); assert.equal(await fallback.evaluate(() => !TinyTroopsManagement.selected() && S.bench[0].n === 'Medic'), true);
  await fallback.close(); assert.deepEqual(errors, []); console.log('PASS standalone management Back fallback remains functional when navigation is unavailable');
})().catch(async error => { console.error(error); if (errors.length) console.error(errors); if (page) console.error(await page.evaluate(() => ({ navigation: window.TinyTroopsNavigation?.status(), placing: typeof S !== 'undefined' ? S.placing?.n : null, title: document.getElementById('dt')?.textContent, state: history.state, url: location.href }))); process.exitCode = 1; }).finally(async () => { await browser?.close(); await new Promise(resolve => server.close(resolve)); });
