// Functional checks only. This script does not generate previews or screenshots.
import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright');
const root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{
  try {
    const file=path.resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://localhost').pathname));
    if(!file.startsWith(root+path.sep))throw Error('outside root');
    const body=await readFile(file);
    res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);
  }catch{res.writeHead(404);res.end('Not found');}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const base=`http://127.0.0.1:${server.address().port}`;
let browser;
try {
  browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox']});
  for(const viewport of [{width:1280,height:850},{width:390,height:844},{width:844,height:390}]) {
    const context=await browser.newContext({viewport,hasTouch:viewport.width<900}),page=await context.newPage(),errors=[];
    page.on('pageerror',e=>errors.push(e.message));
    await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    await page.goto(`${base}/game/iron-throne/index.html`);
    await page.locator('#start-game').waitFor({state:'visible'});
    await page.locator('#game-size').selectOption('12');await page.locator('#start-game').click();
    let saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    assert.equal(saved.width,64);assert.equal(saved.height,44);assert.equal(saved.kingdoms.length,12);assert.deepEqual(saved.fog.houses,{});
    assert.match(await page.locator('#panel').textContent(),/12 HOUSES/);await page.locator('#fit-map').click();
    const site=await page.evaluate(async()=>{const {planFoundings}=await import('./founding.mjs');return planFoundings(JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')))[0].capital;});
    const [q,r]=site.split(',').map(Number);await page.locator('#map').focus();
    for(let i=5;i!==q;i+=Math.sign(q-i))await page.keyboard.press(q>i?'ArrowRight':'ArrowLeft');
    for(let i=6;i!==r;i+=Math.sign(r-i))await page.keyboard.press(r>i?'ArrowDown':'ArrowUp');
    await page.locator('[data-found-city]').click();
    saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));assert.equal(saved.phase,'playing');assert.equal(saved.armies.length,12);
    await page.locator('[data-tab="council"]').click();assert.equal(await page.locator('.house-card').count(),11);
    await page.evaluate(async()=>{
      const {createGame}=await import('./tests/fixtures/legacy-game.mjs');const {refreshKnowledge}=await import('./fog.mjs');
      const s=createGame(311),own=s.armies[0],enemy=s.armies[1],t=s.tiles['12,6'];
      Object.assign(t,{terrain:'plains',owner:'wintermere',building:'fort',name:'Remembered Frontier Fort',levels:{fort:1}});own.tile=t.id;enemy.tile=t.id;refreshKnowledge(s);
      own.tile='5,6';enemy.tile='13,6';enemy.units.levy=98765;t.name='SECRET_NEW_FORT';t.levels.fort=3;s.turn++;
      s.tiles['30,6'].name='UNKNOWN_SECRET_SITE';s.tiles['30,6'].building='fort';s.tiles['30,6'].owner='vesper';s.tiles['30,6'].levels={fort:1};
      localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));
    });
    await page.reload();await page.locator('#resume').click();await page.locator('#map').focus();
    for(let i=0;i<7;i++)await page.keyboard.press('ArrowRight');
    assert.match(await page.locator('#panel').textContent(),/Remembered Frontier Fort/);assert.match(await page.locator('#panel').textContent(),/Last seen T1/);assert.match(await page.locator('#panel').textContent(),/LAST OBSERVED TURN 1/);
    assert.doesNotMatch(await page.locator('#panel').textContent(),/SECRET_NEW_FORT|98765|Durability|Undefended/);
    await page.locator('#map').focus();for(let i=0;i<18;i++)await page.keyboard.press('ArrowRight');
    assert.match(await page.locator('#panel').textContent(),/UNEXPLORED/);assert.doesNotMatch(await page.locator('#panel').textContent(),/UNKNOWN_SECRET_SITE/);
    for(const tab of ['realm','council','intelligence','war-room','ledger']){const button=page.locator(`[data-tab="${tab}"]`);if(await button.count())await button.click();assert.doesNotMatch(await page.locator('#panel').textContent(),/SECRET_NEW_FORT|UNKNOWN_SECRET_SITE|98765/);}
    await page.locator('[data-tab="council"]').click();await page.locator('.house-card [data-talk="wintermere"]').click();
    await page.locator('#offer-type').selectOption('PLEDGE_ATTACK');assert.doesNotMatch(await page.locator('#offer-target').textContent(),/98765|13,6|SECRET/);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    assert.deepEqual(errors,[]);console.log(`12-House setup, founding fog, stale sightings and private panels passed at ${viewport.width}×${viewport.height}`);await context.close();
  }
}finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
