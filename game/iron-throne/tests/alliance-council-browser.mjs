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

  for(const viewport of [{width:1440,height:1000},{width:390,height:844}]){
  browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox', ...(process.env.IRON_THRONE_CHROMIUM ? ['--single-process','--no-zygote','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader'] : [])]});
    const s=legacyGame();
    for(const id of ['wintermere','thornwall'])s.treaties.push({id:`alliance-${id}`,type:'alliance',parties:['ashen',id],expires:11});
    s.diplomacy.messages.regular=2;
    s.conversations.wintermere=[
      {role:'ruler',turn:1,kind:'border',text:'House Ashen armies approach House Wintermere territory.',dispatch:{mode:'ai-initiated',turn:1,reason:'border',armyOwner:'ashen',territoryOwner:'wintermere'}},
      {role:'council',turn:1,kind:'relationship',text:'WARINESS +9 / FEAR +21'}
    ];
    const context=await browser.newContext({viewport});
    await context.addInitScript(s=>localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s)),s);
    const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',r=>r.fulfill({json:{}}));
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
    await page.locator('[data-alliance]').first().click();
    assert.equal(await page.locator('#alliance-council').isVisible(),true);
    assert.equal(await page.locator('.alliance-members>span').count(),3);
    await page.locator('#alliance-message').fill('Attack Vesper.');await page.locator('.alliance-compose button[type=submit]').click();
    await page.waitForFunction(()=>document.querySelector('.alliance-history').textContent.includes('terms before me'));
    assert.equal(await page.locator('.alliance-compose button[type=submit]').isDisabled(),true);
    const requested=page.locator('.alliance-history [data-alliance-terms]').first(),ruler=await requested.getAttribute('data-alliance-terms');
    await requested.click();
    await page.waitForFunction(()=>document.getElementById('offer-type').value==='JOINT_WAR');
    assert.equal(await page.locator('#offer-target').inputValue(),'vesper');
    assert.equal(await page.locator('#offer-form button[type=submit]').isEnabled(),true);
    await page.locator('#give-amount').fill('80');await page.locator('#offer-form button[type=submit]').click();
    await page.waitForFunction(()=>document.getElementById('send-chat').textContent==='Send envoy →');
    const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    assert.equal(saved.diplomacy.messages.regular,3,'requested proposal did not spend another envoy');
    assert.equal(saved.proposalFollowups?.[`ashen:${ruler}`],undefined,'one-use credit consumed');
    assert.equal(await page.locator('#proposals .proposal').count()>0,true);
    await page.locator('.treaty-drawer-close').click();
    assert.equal(await page.locator('#diplomacy').isVisible(),false);assert.equal(await page.locator('#alliance-council').isVisible(),true);
    await page.locator('#alliance-council .close').click();
    await page.locator('[data-dispatch="wintermere"]').click();assert.equal(await page.locator('#chat-form').isVisible(),true);
    assert.match(await page.locator('#messages .dispatch-divider').textContent(),/NEW DISPATCH · TURN 1.*Border Concern/);
    assert.match(await page.locator('#messages .relationship-notice').textContent(),/WARINESS \+9 \/ FEAR \+21/);
    assert.deepEqual(errors,[]);console.log(`Council and requested Treaty Desk flow passed at ${viewport.width}px`);
    await context.close();await browser.close();browser=null;
  }
} finally {await browser?.close();await new Promise(resolve=>server.close(resolve));}
