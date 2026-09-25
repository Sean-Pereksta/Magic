import { createGame as legacyGame } from './fixtures/legacy-game.mjs';
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
    const mime = { '.html': 'text/html', '.mjs': 'text/javascript', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.svg': 'image/svg+xml' }[path.extname(file)] || 'application/octet-stream';
    const body = await readFile(file);
    res.writeHead(200, { 'Content-Type': mime }); res.end(body);
  } catch { res.writeHead(404); res.end('Not found'); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = `http://127.0.0.1:${server.address().port}`;
let browser;
try {
  browser = await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
  for (const viewport of [{width:1280,height:850},{width:390,height:844}]) {
    const context = await browser.newContext({viewport});
    await context.addInitScript(initial=>localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(initial)),legacyGame());
    const page = await context.newPage(), errors=[];
    page.on('pageerror',e=>errors.push(e.message));
    await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    await page.goto(`${base}/game/iron-throne/index.html`);
    await page.locator('#resume').click();
    await page.evaluate(()=>{
      window.progressSeen=[];window.framesDuringTurn=0;
      new MutationObserver(()=>window.progressSeen.push(document.querySelector('#turn-progress-label').textContent))
        .observe(document.querySelector('#turn-progress-label'),{childList:true});
      const tick=()=>{if(document.querySelector('#end-turn').disabled)window.framesDuringTurn++;requestAnimationFrame(tick);};tick();
      document.querySelector('#end-turn').click();
      document.querySelector('#end-turn').click();
    });
    await page.waitForFunction(()=>document.querySelector('#turn-progress-label').textContent==='Your turn',{},{timeout:90000});
    assert.equal(await page.locator('#turn').textContent(),'Turn 2','double click resolves only one round');
    assert.equal(await page.locator('#end-turn').isEnabled(),true);
    const feedback=await page.evaluate(()=>({seen:progressSeen,frames:framesDuringTurn,overflow:document.documentElement.scrollWidth>innerWidth}));
    assert.ok(feedback.seen.some(x=>/AI turns: [1-5] \/ 5/.test(x)),JSON.stringify(feedback));
    assert.ok(feedback.frames>0,'browser paints while AI runs');
    assert.equal(feedback.overflow,false);
    // Module-worker loading failure leaves the saved campaign intact and unlocks retry.
    const saved=await page.evaluate(()=>localStorage.getItem('catnmice.iron-throne.v1'));
    await page.route('**/turn-worker.mjs',route=>route.abort());
    await page.locator('#end-turn').click();
    await page.waitForFunction(()=>!document.querySelector('#end-turn').disabled);
    assert.equal(await page.locator('#turn').textContent(),'Turn 2');
    assert.equal(await page.evaluate(()=>localStorage.getItem('catnmice.iron-throne.v1')),saved);
    assert.match(await page.locator('#toast').textContent(),/campaign is unchanged/);
    assert.deepEqual(errors,[]);
    await context.close();
    console.log(`Turn progress browser checks passed: ${viewport.width}px`);
  }
} finally { await browser?.close(); await new Promise(resolve=>server.close(resolve)); }
