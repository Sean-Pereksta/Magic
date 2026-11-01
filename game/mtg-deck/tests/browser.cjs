/* Deterministic browser regression: fixture quotes are test data, never production prices. */
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
let chromium;
try { ({ chromium } = require('playwright')); }
catch {
  if (!process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES) throw new Error('Install the repository devDependencies to run Playwright browser tests.');
  ({ chromium } = require(path.join(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES, 'playwright')));
}

const root = path.resolve(__dirname, '../../..');
const bolt = {
  object: 'card', id: '11111111-1111-4111-8111-111111111111', name: 'Lightning Bolt',
  type_line: 'Instant', mana_cost: '{R}', cmc: 1, colors: ['R'], color_identity: ['R'],
  oracle_text: 'Lightning Bolt deals 3 damage to any target.', finishes: ['nonfoil', 'foil'],
  prices: { usd: '2.00', usd_foil: '7.00', usd_etched: null }, set: 'tst', set_name: 'Test Set',
  collector_number: '123', rarity: 'common', games: ['paper'], scryfall_uri: 'https://scryfall.com/card/tst/123',
};
const unknown = {
  ...bolt, id: '22222222-2222-4222-8222-222222222222', name: 'Unknown Relic',
  mana_cost: '{2}', cmc: 2, colors: [], color_identity: ['R'], type_line: 'Artifact',
  oracle_text: 'An unpriced fixture with a quoted foil.', prices: { usd: null, usd_foil: '15.00', usd_etched: null },
};
const gold = {
  ...bolt, id: '33333333-3333-4333-8333-333333333333', name: 'Two Color Scout',
  mana_cost: '{1}{W}{U}', cmc: 3, colors: ['W', 'U'], type_line: 'Creature — Scout',
  prices: { usd: '3.50', usd_foil: null }, finishes: ['nonfoil'],
};
const modal = {
  ...bolt, id: '44444444-4444-4444-8444-444444444444', name: 'River Spell // River Shore',
  layout: 'modal_dfc', type_line: 'Sorcery // Land', mana_cost: undefined, colors: undefined, cmc: 3,
  card_faces: [
    { name: 'River Spell', type_line: 'Sorcery', mana_cost: '{2}{U}', colors: ['U'], oracle_text: 'Draw a card.' },
    { name: 'River Shore', type_line: 'Land', colors: [], oracle_text: '{T}: Add {U}.' },
  ],
};
const foilOnly = { ...bolt, id: '55555555-5555-4555-8555-555555555555', name: 'Foil Treasure', finishes: ['foil'], prices: { usd: null, usd_foil: '8.00' } };
const candidates = [bolt, unknown, gold, modal, foilOnly];
let apiCalls = [];

const list = (data, extra = {}) => ({ object: 'list', data, total_cards: data.length, has_more: false, ...extra });
const server = http.createServer((req, res) => {
  const filename = path.resolve(root, '.' + decodeURIComponent(req.url.split('?')[0]));
  if (!filename.startsWith(root + path.sep)) { res.writeHead(403).end(); return; }
  try {
    res.setHeader('Content-Type', filename.endsWith('.html') ? 'text/html' : filename.endsWith('.css') ? 'text/css' : 'text/javascript');
    res.end(fs.readFileSync(filename));
  } catch { res.writeHead(404).end('Not found'); }
});

async function routeApi(route) {
  const req = route.request();
  const url = new URL(req.url());
  apiCalls.push({ path: url.pathname, q: url.searchParams.get('q'), method: req.method() });
  const fulfill = data => route.fulfill({ contentType: 'application/json', body: JSON.stringify(data) });
  if (url.pathname === '/cards/collection') {
    const identifiers = req.postDataJSON().identifiers;
    return fulfill({ ...list(identifiers.map(({ id }) => candidates.find(card => card.id === id)).filter(Boolean).map(card => ({ ...card, prices: { ...card.prices, usd: card.id === bolt.id ? '4.00' : card.prices.usd } }))), not_found: [] });
  }
  const query = url.searchParams.get('q') || '';
  if (query.includes('Failure')) return route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ details: 'Temporary test outage' }) });
  if (query.includes('Missing')) return route.fulfill({ status: 404, contentType: 'application/json', body: JSON.stringify({ details: 'No card found' }) });
  if (query.includes('Slow')) {
    await new Promise(resolve => setTimeout(resolve, 400));
    return fulfill(list([{ ...bolt, name: 'Slow response' }]));
  }
  if (query.includes('Many')) {
    if (url.searchParams.get('page') === '2') return fulfill(list([{ ...bolt, id: 'last-card', name: 'Last card' }], { total_cards: 11 }));
    return fulfill(list(Array.from({ length: 10 }, (_, i) => ({ ...bolt, id: 'many-' + i, name: 'Many ' + i })), {
      total_cards: 11, has_more: true, next_page: 'https://api.scryfall.com/cards/search?q=game%3Apaper%20Many&page=2',
    }));
  }
  if (/name:|!"/.test(query)) {
    const card = candidates.find(card => query.toLowerCase().includes(card.name.toLowerCase())) || (query.includes('Lightning') ? bolt : null);
    if (card) return fulfill(list([card]));
  }
  return fulfill(list(candidates));
}

async function boot(browser, width = 1280, initialStorage = {}) {
  const context = await browser.newContext({ viewport: { width, height: 1000 }, locale: 'en-US' });
  await context.addInitScript(storage => {
    if (sessionStorage.getItem('fixtures-loaded')) return;
    Object.entries(storage).forEach(([key, value]) => localStorage.setItem(key, value));
    sessionStorage.setItem('fixtures-loaded', '1');
  }, initialStorage);
  await context.route('https://api.scryfall.com/**', routeApi);
  await context.route('https://cards.scryfall.io/**', route => route.abort());
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto('http://127.0.0.1:' + server.address().port + '/game/MTG_Catalog_Deck_Builder.html');
  await page.waitForFunction(() => document.querySelector('#deckGuidance').textContent.length > 0);
  return { context, page, errors };
}
async function search(page, name) {
  await page.locator('#quickQuery').fill(name);
  await page.locator('#quickForm button[type=submit]').click();
  await page.locator('#quickResults .search-card').first().waitFor();
}
async function add(page, name, finish) {
  await search(page, name);
  if (finish) await page.locator('#quickResults select').first().selectOption(finish);
  await page.locator('#quickResults [data-result-action=add]').first().click();
}
async function saved(page) {
  return page.evaluate(() => JSON.parse(localStorage.getItem('mtg.draft.v2')));
}
async function count(page, count) {
  await page.waitForFunction(n => document.querySelector('#tabDeckCount').textContent === String(n), count);
}

(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'], ...(process.env.MTG_CHROMIUM_PATH ? { executablePath: process.env.MTG_CHROMIUM_PATH } : {}) });
  try {
    const { page, context, errors } = await boot(browser);
    assert.equal(apiCalls.length, 0, 'draft startup must not fetch the catalog');
    await search(page, 'Lightning Bolt');
    assert.equal(await page.locator('#tabDeckCount').textContent(), '0', 'search must not add cards');
    assert.match(await page.locator('#quickResults').textContent(), /Lightning Bolt deals 3 damage/);
    assert.match(await page.locator('#quickResults').textContent(), /Red/);
    assert.match(await page.locator('#quickResults .price').first().textContent(), /\$2\.00/);
    await page.locator('#quickResults [data-result-action=details]').first().click();
    assert.equal(await page.locator('#cardDialog').evaluate(el => el.open), true);
    assert.equal(await page.locator('#tabDeckCount').textContent(), '0', 'opening details must not add cards');
    await page.locator('#cardDialog .close-dialog').click();
    await page.locator('#quickResults [data-result-action=add]').first().click();
    await count(page, 1);
    await page.locator('#deckList [data-deck-action=plus]').click();
    await count(page, 2);
    assert.equal(await page.locator('#deckValue').textContent(), '$4.00');
    assert.match(await page.locator('#curveRows tr').nth(1).textContent(), /2 \/ 2/);
    await page.locator('#quickResults select').selectOption('foil');
    await page.locator('#quickResults [data-result-action=add]').click();
    await count(page, 3);
    assert.equal(await page.locator('#deckList .deck-card').count(), 2);
    assert.equal(await page.locator('#deckValue').textContent(), '$11.00');
    const normal = page.locator('[data-entry-key="' + bolt.id + ':nonfoil:main"]');
    await normal.locator('[data-deck-action=move]').click();
    await count(page, 1);
    assert.equal(await page.locator('#deckValue').textContent(), '$7.00');
    assert.equal(await page.locator('#sideZoneCount').textContent(), '2');
    await page.locator('#sideZone').click();
    await page.locator('#deckList [data-deck-action=move]').click();
    await count(page, 3);
    await page.locator('#mainZone').click();
    await add(page, 'Unknown Relic');
    await count(page, 4);
    assert.equal(await page.locator('#deckValue').textContent(), '$11.00', 'unknown normal quote cannot use foil price');
    assert.match(await page.locator('#priceCoverage').textContent(), /1 unpriced/);
    await add(page, 'Two Color Scout');
    await count(page, 5);
    assert.match(await page.locator('#colorBreakdown').textContent(), /Multicolor.*20%/);
    await page.locator('#landAddQty').fill('17');
    await page.locator('[data-basic=R]').click();
    await count(page, 22);
    assert.match(await page.locator('#landCount').textContent(), /17 \/ 17/);
    assert.match(await page.locator('#curveRows tr').first().textContent(), /0 \/ 0/, 'lands do not enter zero-mana bucket');
    assert.match(await page.locator('#priceCoverage').textContent(), /18 unpriced/);
    await add(page, 'River Spell // River Shore');
    await count(page, 23);
    assert.match(await page.locator('#landCount').textContent(), /17 \/ 17/);
    const mdfc = page.locator('.deck-card').filter({ hasText: 'River Spell' });
    await mdfc.locator('select[data-deck-action=role]').selectOption('land');
    assert.match(await page.locator('#landCount').textContent(), /18 \/ 17/);
    await page.locator('#editPlan').click();
    await page.locator('#planCurve1').fill('4');
    assert.match(await page.locator('#planLiveTotal').textContent(), /42 \/ 40.*2 over/);
    await page.locator('#planForm button[type=submit]').click();
    assert.equal(await page.locator('#planWarning').isVisible(), true);
    await page.reload();
    await count(page, 23);
    assert.equal((await saved(page)).plan.curve[1], 4);
    await page.locator('#refreshPrices').click();
    await page.waitForFunction(() => document.querySelector('#refreshPrices').textContent === 'Refresh prices');
    assert.equal(await page.locator('#deckValue').textContent(), '$20.50');
    await page.locator('#quickQuery').fill('Failure');
    await page.locator('#quickForm button[type=submit]').click();
    await page.waitForFunction(() => document.querySelector('#quickStatus').textContent.includes('Temporary test outage'));
    await count(page, 23);
    assert.equal((await saved(page)).entries.length, 6, 'failed search preserves every entry');
    await search(page, 'Many');
    assert.equal(await page.locator('#quickResults .search-card').count(), 8);
    const beforeMore = apiCalls.length;
    await page.locator('#quickMore').click();
    assert.equal(await page.locator('#quickResults .search-card').count(), 10);
    assert.equal(apiCalls.length, beforeMore, 'revealing already-loaded cards must not fetch');
    await page.locator('#quickMore').click();
    await page.waitForFunction(() => document.querySelectorAll('#quickResults .search-card').length === 11);
    await page.locator('#quickQuery').fill('Slow');
    await page.locator('#quickForm button[type=submit]').click();
    await page.locator('#quickQuery').fill('Foil Treasure');
    await page.locator('#quickForm button[type=submit]').click();
    await page.waitForFunction(() => document.querySelector('#quickResults').textContent.includes('Foil Treasure'));
    assert.doesNotMatch(await page.locator('#quickResults').textContent(), /Slow response/);
    assert.equal(await page.locator('#quickResults select').inputValue(), 'foil');
    await page.locator('#catalogTab').click();
    await page.locator('#catalogResults .search-card').first().waitFor();
    assert.equal(await page.locator('#deckPanel').isVisible(), false);
    assert.match(apiCalls.at(-1).q, /game:paper.*usd>=0/);
    await page.locator('#backToDeck').click();
    const beforeCut = (await saved(page)).entries[0];
    await page.locator('#deckList [data-deck-action=remove]').first().click();
    await page.locator('#undoBtn').click();
    assert.ok((await saved(page)).entries.some(entry => entry.key === beforeCut.key));
    assert.deepEqual(errors, []);
    await context.close();
    console.log('PASS desktop: explicit add, finish-specific value, curve, colors, sideboard, lands, goals, reload, refresh, API errors, pagination, cancellation, catalog, undo.');

    const legacy = { [bolt.id]: { card: bolt, qty: 2 }, [unknown.id]: { card: unknown, qty: 1 } };
    const mobile = await boot(browser, 390, { 'mtg.deck': JSON.stringify(legacy) });
    await count(mobile.page, 3);
    assert.equal(await mobile.page.locator('#deckValue').textContent(), '$4.00');
    for (const panel of ['.search-panel', '.curve-panel', '.cards-panel', '.colors-panel', '.lands-panel']) {
      assert.equal(await mobile.page.locator(panel).isVisible(), true, panel + ' must remain available on phones');
    }
    const bounds = await mobile.page.evaluate(() => ({
      overflow: document.documentElement.scrollWidth > window.innerWidth,
      search: document.querySelector('.search-panel').getBoundingClientRect().top,
      curve: document.querySelector('.curve-panel').getBoundingClientRect().top,
      cards: document.querySelector('.cards-panel').getBoundingClientRect().top,
    }));
    assert.equal(bounds.overflow, false, 'phone layout must not scroll horizontally');
    assert.ok(bounds.search < bounds.curve && bounds.curve < bounds.cards, 'phone flow is search → curve → deck');
    await mobile.page.locator('#landAddQty').fill('9');
    await mobile.page.locator('[data-basic=U]').click();
    await count(mobile.page, 12);
    assert.equal(await mobile.page.evaluate(() => JSON.parse(localStorage.getItem('mtg.deck'))[Object.keys(JSON.parse(localStorage.getItem('mtg.deck')))[0]].qty), 2, 'legacy source stays intact');
    await mobile.page.reload();
    await count(mobile.page, 12);
    assert.deepEqual(mobile.errors, []);
    await mobile.page.locator('.tools-menu summary').click();
    await mobile.page.locator('#importBtn').click();
    const beforeBadImport = await saved(mobile.page);
    const badBackup = { ...beforeBadImport, entries: [{ ...beforeBadImport.entries[0], card: { ...bolt, id: 123 } }] };
    await mobile.page.locator('#importFile').setInputFiles({ name: 'bad-backup.json', mimeType: 'application/json', buffer: Buffer.from(JSON.stringify(badBackup)) });
    await mobile.page.locator('#importSubmit').click();
    await mobile.page.waitForFunction(() => document.querySelector('#importStatus').textContent.includes('invalid card entries'));
    assert.deepEqual(await saved(mobile.page), beforeBadImport, 'invalid backups must not replace good cards');
    await mobile.page.locator('#importDialog .close-dialog').click();
    const otherTab = await mobile.context.newPage();
    await otherTab.goto(mobile.page.url());
    await otherTab.waitForFunction(() => document.querySelector('#tabDeckCount').textContent === '12');
    await otherTab.locator('[data-basic=G]').click();
    await count(otherTab, 13);
    await mobile.page.waitForFunction(() => document.querySelector('#storageWarning').textContent.includes('Another tab'));
    await mobile.page.locator('#landAddQty').fill('1');
    await mobile.page.locator('[data-basic=W]').click();
    const storedFromOtherTab = await saved(mobile.page);
    assert.ok(storedFromOtherTab.entries.some(entry => entry.card.id === 'basic:G'));
    assert.ok(!storedFromOtherTab.entries.some(entry => entry.card.id === 'basic:W'), 'conflicting old tab must not overwrite new save');
    assert.match(await mobile.page.locator('#saveState').textContent(), /Not saved/);
    await otherTab.close();
    await mobile.context.close();
    console.log('PASS phone: all panels visible, no horizontal overflow, clear panel order, legacy migration, persistent changes, bad import rejection and cross-tab save protection.');

    const damaged = await boot(browser, 390, { 'mtg.draft.v2': '{bad json' });
    assert.equal(await damaged.page.locator('#storageWarning').isVisible(), true);
    await damaged.page.locator('[data-basic=W]').click();
    await count(damaged.page, 1);
    assert.equal(await damaged.page.evaluate(() => localStorage.getItem('mtg.draft.v2')), '{bad json', 'corrupt source must not be overwritten');
    assert.match(await damaged.page.locator('#saveState').textContent(), /Not saved/);
    await damaged.context.close();
    console.log('PASS corrupt save: memory-only recovery preserves original data and makes unsaved state visible.');
  } finally {
    await browser.close();
    server.close();
  }
})().catch(error => { console.error(error); server.close(); process.exitCode = 1; });
