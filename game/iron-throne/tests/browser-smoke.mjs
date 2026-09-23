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
    res.writeHead(200, { 'Content-Type': mime }); res.end(await readFile(file));
  } catch { res.writeHead(404); res.end('Not found'); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = `http://127.0.0.1:${server.address().port}`;
let browser;
try {
  for (const viewport of (process.env.IRON_THRONE_CHAT_ONLY ? [] : [{ width: 1280, height: 850 }, { width: 390, height: 844 }, { width: 844, height: 390 }])) {
    browser = await chromium.launch({ headless: true, executablePath: process.env.IRON_THRONE_CHROMIUM || undefined, args: ['--no-sandbox', ...(process.env.IRON_THRONE_CHROMIUM ? ['--single-process', '--no-zygote', '--disable-dev-shm-usage', '--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'] : [])] });
    const context = await browser.newContext({ viewport, hasTouch: viewport.width < 900 });
    const page = await context.newPage(), errors = [], external = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', r => { if (!r.url().startsWith(base)) external.push(r.url()); });
    await page.route('**/game/iron-throne/config.json', route => route.fulfill({ json: {} }));
    await page.goto(`${base}/game/iron-throne/index.html`);
    await page.locator('#start-game').click();
    assert.equal(await page.locator('#turn').textContent(), 'Turn 1');
    await page.locator('#map').focus(); await page.keyboard.press('ArrowRight'); await page.locator('#home').click();
    await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    const bounds=await page.locator('#map').boundingBox(), zoom=bounds.width<600?.85:1.25;
    await page.mouse.click(bounds.x+bounds.width/2,bounds.y+bounds.height/2+26*zoom);
    assert.match(await page.locator('#coordinates').textContent(), /5,6$/, 'clicking the lower edge of an army banner selects its real tile');
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
    assert.equal(await page.locator('#use-gemini').isChecked(), false);
    assert.equal(await page.locator('#use-gemini').isDisabled(), true);
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
  // Real browser behavior with fake verification/model responses: never consumes live quota.
  for (const verificationFails of [false, true]) {
    browser = await chromium.launch({ headless: true, executablePath: process.env.IRON_THRONE_CHROMIUM || undefined, args: ['--no-sandbox', ...(process.env.IRON_THRONE_CHROMIUM ? ['--single-process', '--no-zygote', '--disable-dev-shm-usage'] : [])] });
    const context = await browser.newContext({ viewport: { width: 390, height: 844 }, reducedMotion: 'reduce' });
    const page = await context.newPage(), errors = []; let modelCalls=0, scriptLoads=0, configRoute;
    page.on('pageerror', e => errors.push(e.message));
    // Hold configuration until a council is already open to exercise startup races.
    await page.route('**/game/iron-throne/config.json', route => { configRoute=route; });
    await page.route('https://challenges.cloudflare.com/**', async route => {
      scriptLoads++;
      if(verificationFails) return route.abort();
      await route.fulfill({ contentType:'text/javascript', body:`let options;window.testVerificationCount=0;const done=()=>{options.callback('test-token');window.testVerificationCount++;};window.turnstile={render(el,o){options=o;setTimeout(done,0);return 'widget';},reset(){setTimeout(done,0);}};` });
    });
    await page.route('https://worker.example/diplomacy', async route => {
      if(route.request().method()==='OPTIONS') return route.fulfill({status:204,headers:{'Access-Control-Allow-Origin':'*','Access-Control-Allow-Headers':'content-type'}});
      modelCalls++;
      assert.equal(route.request().postDataJSON().turnstileToken,'test-token');
      await route.fulfill(modelCalls===1?{headers:{'Access-Control-Allow-Origin':'*'},json:{reply:'The banners of Wintermere hear your envoy.',tone:'neutral',intents:[]}}:{status:429,headers:{'Access-Control-Allow-Origin':'*','Retry-After':'300'},body:''});
    });
    await page.goto(`${base}/game/iron-throne/index.html`);
    await page.locator('#start-game').click();
    await page.locator('[data-tab="council"]').click(); await page.locator('[data-talk="wintermere"]').click();
    assert.equal(modelCalls,0);assert.equal(scriptLoads,0);
    await configRoute.fulfill({json:{diplomacyEndpoint:'https://worker.example/diplomacy',turnstileSiteKey:'public-test-key'}});
    await page.waitForFunction(() => !document.getElementById('use-gemini').disabled);
    assert.equal(await page.locator('#use-gemini').isChecked(),true);
    assert.equal(await page.locator('#privacy').isVisible(),true);
    await page.waitForFunction(fails => document.getElementById('chat-notice').textContent.includes(fails?'could not load':'Gemini ready'),verificationFails);
    assert.equal(modelCalls,0,'opening a council must not consume Gemini quota');
    await page.locator('#chat-message').fill('Greetings, Queen.');await page.locator('#send-chat').click();
    await page.waitForFunction(() => !document.getElementById('send-chat').disabled);
    if(verificationFails){assert.equal(modelCalls,0);assert.match(await page.locator('#chat-notice').textContent(),/verification/i);}
    else {
      assert.equal(modelCalls,1, await page.locator('#chat-notice').textContent());assert.match(await page.locator('#messages').textContent(),/banners of Wintermere/);
      await page.waitForFunction(() => window.testVerificationCount>=2);
      await page.locator('#chat-message').fill('Would you consider peace?');await page.locator('#send-chat').click();
      await page.waitForFunction(() => !document.getElementById('send-chat').disabled);
      assert.equal(modelCalls,2);assert.match(await page.locator('#chat-notice').textContent(),/quota reached/);assert.match(await page.locator('#messages').textContent(),/Wintermere/);
      assert.match(await page.locator('#ai-status').textContent(),/Scripted/);
    }
    await page.locator('#use-gemini').uncheck();const callsBefore=modelCalls;
    await page.locator('#chat-message').fill('An alliance for 60 gold');await page.locator('#send-chat').click();
    await page.waitForFunction(() => !document.getElementById('send-chat').disabled);
    assert.equal(modelCalls,callsBefore);assert.equal(await page.locator('#privacy').isVisible(),false);
    await page.locator('[data-close="diplomacy"]').click();await page.locator('[data-talk="wintermere"]').click();
    assert.equal(await page.locator('#use-gemini').isChecked(),false,'manual opt-out survives reopening the council');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth<=innerWidth),true);
    await page.unroute('**/game/iron-throne/config.json');
    await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{diplomacyEndpoint:'https://worker.example/diplomacy',turnstileSiteKey:'public-test-key'}}));
    await page.reload();await page.locator('#resume').click();
    await page.waitForFunction(() => !document.getElementById('use-gemini').disabled);
    assert.equal(await page.locator('#use-gemini').isChecked(),true,'a new session defaults to Gemini on resume');
    assert.equal(modelCalls,callsBefore);
    assert.deepEqual(errors,[]);
    console.log(`PASS Gemini default: delayed config, ${verificationFails?'verification failure':'verified reply and quota fallback'}, opt-out, resume, no automatic model calls`);
    await context.close(); await browser.close(); browser=null;
  }
  browser = await chromium.launch({ headless: true, executablePath: process.env.IRON_THRONE_CHROMIUM || undefined, args: ['--no-sandbox', ...(process.env.IRON_THRONE_CHROMIUM ? ['--single-process', '--no-zygote', '--disable-dev-shm-usage'] : [])] });
  const artPage=await browser.newPage();
  await artPage.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
  await artPage.goto(`${base}/game/iron-throne/index.html`);
  const graphics=await artPage.evaluate(async()=>{
    const {MapArt}=await import('../iron-throne/art.mjs');const {WorldMap}=await import('../iron-throne/map.mjs');const {createGame}=await import('../iron-throne/core.mjs');
    const art=new MapArt(),canvas=document.createElement('canvas');canvas.width=100;canvas.height=100;const c=canvas.getContext('2d');
    const fingerprint=()=>Array.from(c.getImageData(0,0,100,100).data).reduce((a,v,i)=>(a+v*(i%97+1))>>>0,0);
    const terrains=['plains','forest','mountain','hills','water','coast'].map(terrain=>{c.clearRect(0,0,100,100);art.ground(c,{terrain,q:1,r:2},50,50);art.terrain(c,{terrain,q:1,r:2},50,50);return fingerprint();});
    const buildings=['city','town','fort','farm','lumber','quarry','mine'].map(building=>{c.clearRect(0,0,100,100);art.building(c,{building},50,50,'#dfa94f');return fingerprint();});
    c.clearRect(0,0,100,100);art.building(c,{building:'city',walls:60,market:true,workshop:true},50,50,'#dfa94f');const upgraded=fingerprint();
    for(let i=0;i<1000;i++)art.ground(c,{terrain:'plains',q:i,r:i%30},50,50);
    const state=createGame(),map=Object.create(WorldMap.prototype);Object.assign(map,{canvas,ctx:c,art,getState:()=>state,width:100,height:100,dpr:1,zoom:.25,x:800,y:500,motion:{matches:true},selected:'5,6',armyId:state.armies[0].id});
    const begin=performance.now();for(let i=0;i<12;i++)map.render();const elapsed=performance.now()-begin;
    return {terrains,buildings,upgraded,cacheSize:art.cache.size,elapsed};
  });
  assert.equal(new Set(graphics.terrains).size,6);assert.equal(new Set(graphics.buildings).size,7);
  assert.notEqual(graphics.upgraded,graphics.buildings[0]);assert.ok(graphics.cacheSize<=192);
  console.log(`PASS graphics: six terrains, seven buildings, upgrades, bounded cache; 12 cached map draws ${Math.round(graphics.elapsed)}ms`);
  await artPage.close();
} finally { await browser?.close(); await new Promise(resolve => server.close(resolve)); }
