// Functional browser checks only: no generated previews, screenshots or assets.
import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const require = createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES ? `${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json` : import.meta.url);
const { chromium } = require('playwright');
const root = fileURLToPath(new URL('../../../', import.meta.url));
const server = http.createServer(async (req, res) => {
  try {
    let pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
    if (pathname.endsWith('/')) pathname += 'index.html';
    const file = path.resolve(root, '.' + pathname);
    if (!file.startsWith(root)) throw new Error('outside root');
    const mime = { '.html': 'text/html', '.mjs': 'text/javascript', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json' }[path.extname(file)] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': mime }); res.end(await readFile(file));
  } catch { res.writeHead(404); res.end('Not found'); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = `http://127.0.0.1:${server.address().port}`;
let browser;
try {
  for (const viewport of [{ width: 1280, height: 850 }, { width: 390, height: 844 }, { width: 844, height: 390 }]) {
    browser = await chromium.launch({ headless: true, executablePath: process.env.IRON_THRONE_CHROMIUM || undefined, args: ['--no-sandbox', ...(process.env.IRON_THRONE_CHROMIUM ? ['--single-process', '--no-zygote', '--disable-dev-shm-usage', '--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'] : [])] });
    const context = await browser.newContext({ viewport, hasTouch: viewport.width < 900 });
    const page = await context.newPage(), errors = [], external = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', r => { if (!r.url().startsWith(base)) external.push(r.url()); });
    await page.goto(`${base}/game/iron-throne/index.html`);
    await page.locator('#start-game').click();
    assert.equal(await page.locator('#turn').textContent(), 'Turn 1');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'viewport must not overflow horizontally');
    await page.locator('#map').focus(); await page.keyboard.press('ArrowDown');
    await page.locator('[data-build="farm"]').click();
    assert.match(await page.locator('#panel').textContent(), /Farm underway/);
    await page.locator('#end-turn').click(); assert.equal(await page.locator('#turn').textContent(), 'Turn 2');
    assert.ok(!(await page.locator('#panel').textContent()).includes('Farm underway'));
    await page.locator('[data-tab="realm"]').click(); await page.locator('[data-goto="5,6"]').first().click();
    await page.locator('[data-recruit="levy"]').click(); assert.match(await page.locator('#panel').textContent(), /36 troops/);
    await page.locator('[data-order]').first().click(); await page.locator('#map').focus(); await page.keyboard.press('ArrowDown'); await page.locator('#end-turn').click();
    const saved = await page.evaluate(() => JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    assert.equal(saved.armies.find(a => a.owner === 'ashen').tile, '5,7');
    await page.locator('[data-tab="council"]').click(); await page.locator('[data-talk="wintermere"]').click();
    await page.locator('#chat-message').fill('An alliance for 60 gold'); await page.locator('#send-chat').click();
    await page.waitForFunction(() => !document.getElementById('send-chat').disabled);
    assert.match(await page.locator('#chat-notice').textContent(), /Scripted council/);
    await page.locator('[data-ratify]').first().click();
    assert.match(await page.locator('#messages').textContent(), /ratified/);
    await page.locator('[data-close="diplomacy"]').click(); await page.locator('[data-tab="ledger"]').click();
    assert.match(await page.locator('#panel').textContent(), /alliance/);
    await page.reload(); await page.locator('#resume').click();
    assert.equal(await page.locator('#turn').textContent(), 'Turn 3');
    const resumed = await page.evaluate(() => JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    assert.equal(resumed.treaties.find(t => t.type === 'alliance').parties[1], 'wintermere');
    // A touch drag pans without selecting a tile; two pointers may pinch together.
    const before = await page.locator('#coordinates').textContent();
    await page.locator('#map').evaluate(canvas => {
      const rect = canvas.getBoundingClientRect();
      for (const [type, x, y, id] of [['pointerdown', 110, 100, 1], ['pointermove', 170, 140, 1], ['pointerup', 170, 140, 1]]) canvas.dispatchEvent(new PointerEvent(type, { pointerId: id, pointerType: 'touch', clientX: rect.left + x, clientY: rect.top + y, bubbles: true }));
    });
    assert.equal(await page.locator('#coordinates').textContent(), before);
    assert.deepEqual(errors, []); assert.deepEqual(external, [], 'scripted mode should make no outside requests');
    console.log(`PASS ${viewport.width}×${viewport.height}: build, recruit, march, turns, council, treaty, autosave/resume, layout`);
    await context.close();
    await browser.close(); browser = null;
  }
} finally { await browser?.close(); await new Promise(resolve => server.close(resolve)); }
