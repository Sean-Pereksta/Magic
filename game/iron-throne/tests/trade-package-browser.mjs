// Functional DOM checks only; no screenshots or generated HTML previews.
import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {createRequire} from 'node:module';
import {createGame} from './fixtures/legacy-game.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright'),root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{
 try{const file=path.resolve(root,'.'+new URL(req.url,'http://localhost').pathname);if(!file.startsWith(root+path.sep))throw Error('path');const body=await readFile(file);res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);}catch{res.writeHead(404);res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;let browser;
try{
 browser=await chromium.launch({headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
 for(const viewport of [{width:1280,height:850},{width:390,height:844}]){
  const s=createGame();for(const k of s.kingdoms)for(const r of Object.keys(k.resources))k.resources[r]=500;
  const context=await browser.newContext({viewport,hasTouch:viewport.width<700});
  await context.addInitScript(s=>localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s)),s);
  await context.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));
  await context.route('**/game/iron-throne/config.json',r=>r.fulfill({json:{}}));
  const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
  await page.locator('[data-tab="realm"]').click();assert.match(await page.locator('body').textContent(),/Food Security/);
  await page.locator('[data-tab="council"]').click();await page.locator('[data-talk="wintermere"]').first().click();
  await page.locator('#quick-offer').click();await page.locator('#use-gemini').uncheck({force:true}).catch(()=>{});
  await page.locator('#give-resource').selectOption('iron');await page.locator('#give-amount').fill('15');
  await page.locator('[data-add-resource="give"]').click();
  const second=page.locator('#give-items [data-trade-row]').nth(1);await second.locator('select').selectOption('wood');await second.locator('input').fill('30');
  await page.locator('#receive-resource').selectOption('food');await page.locator('#receive-amount').fill('100');
  assert.equal(await page.locator('#give-items [data-trade-row]').count(),2);
  assert.equal(await second.locator('option[value="iron"]').evaluate(option=>option.disabled),true);
  const before=JSON.parse(await page.evaluate(()=>localStorage.getItem('catnmice.iron-throne.v1'))).kingdoms.map(k=>k.resources);
  await page.locator('#offer-form button[type="submit"]').click();await page.locator('[data-modify]').first().waitFor();
  assert.match(await page.locator('#proposals').textContent(),/30 wood \+ 15 iron/);
  assert.deepEqual(JSON.parse(await page.evaluate(()=>localStorage.getItem('catnmice.iron-throne.v1'))).kingdoms.map(k=>k.resources),before);
  await page.locator('[data-modify]').first().click();assert.equal(await page.locator('#give-items [data-trade-row]').count(),2);
  await page.locator('#give-items [data-remove-resource]').first().click();assert.equal(await page.locator('#give-items [data-trade-row]').count(),1);assert.equal(await page.locator('#give-resource').count(),1);
  await page.locator('#offer-type').selectOption('AID');assert.equal(await page.locator('[data-add-resource="give"]').isVisible(),false);
  await page.locator('#offer-type').selectOption('EXCHANGE');assert.equal(await page.locator('[data-add-resource="give"]').isVisible(),true);
  assert.deepEqual(errors,[]);await context.close();
 }
 console.log('Trade package desktop/mobile DOM checks passed.');
}finally{await browser?.close();await new Promise(r=>server.close(r));}
