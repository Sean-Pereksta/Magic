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
    res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(await readFile(file));
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
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#start-game').click();
    await page.evaluate(async()=>{
      const {createGame,declareWar}=await import('./core.mjs');const s=createGame(100);
      const t=s.tiles['5,6'];t.intelligenceOffice=true;t.levels.intelligenceOffice=3;
      for(const k of s.kingdoms){k.commands=0;k.resources.gold=400;}s.kingdoms[0].commands=8;
      s.armies=s.armies.filter(a=>a.owner==='ashen');s.armies[0].tile='18,3';declareWar(s,'ashen','wintermere');
      localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));
    });
    await page.reload();await page.locator('#resume').click();
    await page.locator('[data-tab="realm"]').click();await page.locator('[data-goto="18,3"]').click();
    await page.locator('[data-structure-type="lumber"][data-structure-mode="attack"]').click();
    await page.locator('#end-turn').click();
    let save=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    assert.ok(save.tiles['18,3'].structureDamage.lumber>0);assert.equal(save.armies[0].order,'attack');
    await page.locator('[data-tab="intelligence"]').click();await page.locator('[data-recruit-spy]').click();
    const id=await page.locator('[data-assign-spy]').getAttribute('data-assign-spy');
    await page.locator(`[data-spy-host="${id}"]`).selectOption('wintermere');await page.locator(`[data-spy-mission="${id}"]`).selectOption('plans');await page.locator(`[data-assign-spy="${id}"]`).click();
    save=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    assert.equal(save.intelligence.agents[0].status,'Traveling');assert.equal(save.intelligence.agents[0].mission,'plans');
    assert.match(await page.locator('#panel').textContent(),/FOREIGN NETWORKS|Foreign Networks/i);
    await page.locator(`[data-recall-spy="${id}"]`).click();
    await page.locator('[data-tab="council"]').click();assert.ok(await page.locator('[aria-label="Foreign relations"]').count()>=5);
    await page.locator('[data-open-intelligence="wintermere"]').click();assert.equal(await page.locator('#panel h2').textContent(),'Intelligence');
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    assert.deepEqual(errors,[]);console.log(`Intrigue controls passed at ${viewport.width}×${viewport.height}`);await context.close();
  }
}finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
