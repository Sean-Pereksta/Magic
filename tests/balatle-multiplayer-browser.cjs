const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');

const root = path.resolve(__dirname, '..');

// Run the real HTML, CSS, card artwork and interactions while replacing the
// Firebase transport. No credentials, external requests, or live rooms are used.
async function main() {
  const browser = await chromium.launch({
    headless: true,
    executablePath: process.env.CHROME_BIN || undefined,
    args: ['--no-sandbox']
  });
  try {
    const page = await browser.newPage({ viewport: { width: 1366, height: 768 }, reducedMotion: 'reduce' });
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    const original = fs.readFileSync(path.join(root, 'game/balatle.html'), 'utf8');
    let script = Array.from(original.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi), match => match[1])
      .find(source => source.includes('function playSelected('));
    assert.ok(script, 'The real multiplayer game script must exist');
    script = script.replace(/\bimport\s+[\s\S]*?\sfrom\s+"[^"]+";\s*/g, '');
    const boot = script.lastIndexOf('\n  boot();');
    assert.ok(boot >= 0, 'The client boot must be instrumentable');
    script = script.slice(0, boot) + `
      window.__multi = { game, me, renderAll, applyRoundConfig };
      startNewRunLocal();
      applyRoundConfig(1, { type: 'Small', target: 300 });
    ` + script.slice(boot + '\n  boot();'.length);
    const html = original.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '')
      .replace(/<link[^>]*rel="stylesheet"[^>]*>/g, '');
    await page.route('**/*', route => route.abort());
    await page.setContent(html);
    await page.addStyleTag({ path: path.join(root, 'game/balatle/table.css') });
    await page.addScriptTag({ path: path.join(root, 'game/balatle/core.js') });
    await page.addScriptTag({ path: path.join(root, 'game/balatle/table.js') });
    const firebaseStub = `
      const initializeApp=()=>({}), getFirestore=()=>({}), getAuth=()=>({}),
        doc=(...args)=>args.join('/'), collection=(...args)=>args.join('/'),
        setDoc=async()=>{}, getDoc=async()=>({exists:()=>false}), onSnapshot=()=>()=>{},
        runTransaction=async()=>{}, serverTimestamp=()=>({}), arrayUnion=value=>value,
        signInAnonymously=async()=>{}, onAuthStateChanged=()=>()=>{};
    `;
    await page.addScriptTag({ content: firebaseStub + script });
    await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    assert.equal(await page.locator('#hand .card').count(), 8, 'Eight cards should be dealt');
    assert.equal(await page.locator('#btnPlay').isDisabled(), true, 'Play needs a selection');
    await page.locator('#hand .card').nth(0).click();
    await page.locator('#hand .card').nth(1).click();
    assert.equal(await page.locator('#btnPlay').isDisabled(), false);
    assert.notEqual(await page.locator('#selectionName').textContent(), 'Choose your cards');
    await page.locator('#btnPlay').click();
    await page.waitForFunction(() => !window.__multi.game.scoring);
    assert.equal(await page.locator('#hand .card').first().isDisabled(), false, 'Cards unlock after scoring');
    assert.equal(await page.locator('#handsCount').textContent(), '3');

    await page.locator('#btnDrawer').click();
    assert.equal(await page.locator('#statsPanel').getAttribute('aria-hidden'), 'false');
    await page.locator('#btnCloseDrawer').click();
    assert.equal(await page.locator('#statsPanel').getAttribute('aria-hidden'), 'true');
    await page.evaluate(() => { window.__multi.game.blindScore = 300; window.__multi.renderAll(); });
    await page.locator('#btnFinish').click();
    await page.waitForFunction(() => window.__multi.game.mode === 'shop');
    assert.ok(await page.locator('.shop-card').count() > 0, 'Shop offers are visible');
    assert.equal(await page.locator('.playing-card-offer .playing-card-art .card-inner').count(), 1,
      'An illustrated playable card is available in the multiplayer shop');
    const buyCard = page.locator('.playing-card-offer [data-buy]');
    assert.equal(await buyCard.isEnabled(), true, 'The offered card can be purchased');
    await buyCard.click();
    const purchasedCardId = await page.evaluate(() => window.__multi.game.discard.find(card => card.editionName)?.id);
    assert.ok(purchasedCardId, 'A shop card purchase adds the illustrated edition to the run deck');
    await page.keyboard.press('Escape');
    assert.equal(await page.locator('#btnOpenShop').isVisible(), true, 'Shop can be reopened');
    await page.locator('#btnOpenShop').click();
    await page.locator('#btnReady').click();
    assert.equal(await page.locator('#shopBack').getAttribute('aria-hidden'), 'true');
    assert.equal(await page.evaluate(() => window.__multi.game.mode), 'wait');
    await page.evaluate(() => {
      window.__multi.me.status = 'play';
      window.__multi.applyRoundConfig(2, { type: 'Big', target: 500 });
    });
    assert.equal(await page.evaluate(id => {
      const { game } = window.__multi;
      return [...game.hand, ...game.deck].filter(card => card.id === id).length;
    }, purchasedCardId), 1, 'The purchased card is in the next blind without duplicates');

    const layouts = [];
    for (const viewport of [{ width: 1366, height: 768 }, { width: 390, height: 844 }, { width: 844, height: 390 }]) {
      await page.setViewportSize(viewport);
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const bounds = await page.evaluate(() => ({
        width: innerWidth, height: innerHeight,
        scrollWidth: document.documentElement.scrollWidth,
        actionsBottom: document.querySelector('.table-actions').getBoundingClientRect().bottom,
        handBottom: document.querySelector('#hand').getBoundingClientRect().bottom
      }));
      assert.ok(bounds.scrollWidth <= bounds.width, 'The table must not overflow horizontally');
      assert.ok(bounds.actionsBottom <= bounds.height, 'Desktop, phone, and landscape controls must fit in view');
      layouts.push(bounds);
      if (process.env.BALATLE_SCREENSHOTS) {
        fs.mkdirSync(process.env.BALATLE_SCREENSHOTS, { recursive: true });
        await page.screenshot({ path: path.join(process.env.BALATLE_SCREENSHOTS, `multiplayer-${viewport.width}.png`) });
      }
    }
    await page.setViewportSize({ width: 390, height: 844 });
    await page.locator('#hand .card').nth(0).click();
    await page.locator('#btnPlay').click();
    await page.waitForFunction(() => !window.__multi.game.scoring);
    assert.deepEqual(errors, [], 'No browser runtime errors should occur');
    console.log('Balatle multiplayer browser smoke passed:', JSON.stringify(layouts));
  } finally {
    await browser.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
