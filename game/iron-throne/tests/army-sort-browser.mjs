// Functional desktop/mobile checks. No screenshots, generated HTML previews or live model calls.
import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {createRequire} from 'node:module';
import {createGame} from './fixtures/legacy-game.mjs';
import {refreshGeneralCandidates,hireGeneral,assignGeneral} from '../generals.mjs';
import {refreshKnowledge} from '../fog.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright'),root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{
 try{const url=new URL(req.url,'http://localhost'),file=path.resolve(root,'.'+url.pathname);if(!file.startsWith(root+path.sep))throw Error('path');const body=await readFile(file);res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);}catch{res.writeHead(404);res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;
import {UNITS} from '../data.mjs';
let browser;
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
 for(const viewport of [{width:1280,height:850},{width:390,height:844}]){
  const s=createGame();s.turn=8;s.kingdoms[0].resources.gold=10000;s.commanders.nextOffer.ashen=8;refreshGeneralCandidates(s);
  const g=s.commanders.candidates.find(g=>g.owner==='ashen');hireGeneral(s,'ashen',g.id);const a=s.armies[0];assignGeneral(s,'ashen',g.id,a.id);
  s.commanders.nextOffer.ashen=8;refreshGeneralCandidates(s);const h=s.commanders.candidates.find(g=>g.owner==='ashen');hireGeneral(s,'ashen',h.id);
  a.units={...Object.fromEntries(Object.keys(UNITS).map(u=>[u,0])),spearman:45,archer:22,knight:12};refreshKnowledge(s);
  const context=await browser.newContext({viewport,hasTouch:viewport.width<700});
  await context.addInitScript(s=>{if(!localStorage.getItem('catnmice.iron-throne.v1'))localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));},s);
  await context.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));
  await context.route('**/game/iron-throne/config.json',r=>r.fulfill({json:{}}));
  const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));page.on('dialog',d=>void d.accept());
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
  await page.locator('[data-tab="realm"]').click();await page.locator(`[data-army="${a.id}"]`).first().click();
  const saved=()=>page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1'))),before=await saved();
  await page.locator('[data-sort-army]').click();assert.equal(await page.locator('.sort-formation').count(),2);
  assert.equal(await page.locator('#army-sort').evaluate(e=>e.scrollWidth<=e.clientWidth+1),true);
  await page.locator('[data-unit="spearman"]').click();await page.locator('[data-split-even]').click();
  assert.equal(await page.locator('[data-drop="0"] [data-unit="spearman"] b').textContent(),'22');assert.equal(await page.locator('[data-drop="1"] [data-unit="spearman"] b').textContent(),'23');
  assert.deepEqual(await saved(),before,'draft moves do not write campaign state');
  await page.locator('[data-sort-reset]').click();assert.equal(await page.locator('[data-drop="0"] [data-unit="spearman"] b').textContent(),'45');assert.equal(await page.locator('[data-drop="1"] [data-unit]').count(),0);
  await page.locator('[data-unit="spearman"]').click();await page.locator('#sort-amount').fill('15');assert.equal(await page.locator('#sort-remain').inputValue(),'30');await page.locator('[data-prepare-packet]').click();
  assert.match(await page.locator('[data-packet]').textContent(),/15/);
  if(viewport.width>700){await page.locator('[data-packet]').dragTo(page.locator('[data-drop="1"] .sort-grid'));}
  else {await page.locator('[data-drop-button="1"]').click();}
  assert.equal(await page.locator('[data-drop="1"] [data-unit="spearman"] b').textContent(),'15');
  await page.locator('[data-sort-close]').last().click();assert.deepEqual(await saved(),before,'Cancel preserves original game');
  await page.locator('[data-sort-army]').click();
  if(viewport.width>700)await page.locator('[data-unit="archer"]').dragTo(page.locator('[data-drop="1"] .sort-grid'));
  else {await page.locator('[data-unit="archer"]').click();await page.locator('[data-move-all]').click();}
  await page.locator('[data-name="1"]').fill('Eastern Column');await page.locator('[data-pick-general="1"]').click();await page.locator(`[data-general-select="${h.id}"]`).click();
  await page.locator('[data-add-formation]').click();assert.equal(await page.locator('.sort-formation').count(),3);
  await page.locator('[data-drop="0"] [data-unit="knight"]').click();await page.locator('#sort-destination').selectOption('2');await page.locator('[data-move-all]').click();
  await page.locator('[data-sort-confirm]').click();await page.locator('#army-sort').waitFor({state:'hidden'});
  let state=await saved(),forces=state.armies.filter(x=>x.owner==='ashen');assert.equal(forces.length,3);assert.equal(forces.reduce((n,a)=>n+Object.values(a.units).reduce((n,x)=>n+x,0),0),79);
  assert.equal(forces.find(x=>x.id===a.id).commandId,g.commandId);assert.equal(forces.find(x=>x.name==='Eastern Column').commandId,h.commandId);
  await page.locator(`[data-general-open="${g.id}"]:not([data-show-orders])`).click();assert.equal(await page.locator('#general-order-form').isVisible(),false);assert.equal(await page.locator('.general-portrait').count()>0,true);
  await page.locator('#general-chat-form textarea').fill('Report your forces.');await page.locator('#general-chat-form button').first().click();await page.waitForFunction(()=>document.querySelector('.general-history').textContent.includes('Report your forces.'));
  const history=await page.locator('.general-history').textContent();assert.match(history,/45 Spearmen/);
  await page.locator('[data-general-close]').click();await page.locator('[data-tab="realm"]').click();await page.locator(`[data-general-open="${g.id}"]:not([data-show-orders])`).click();assert.equal(await page.locator('.general-history').textContent(),history);
  await page.locator('[data-general-close]').click();
  await page.reload();await page.locator('#resume').click();assert.equal((await saved()).armies.filter(a=>a.owner==='ashen').length,3);
  await page.locator('[data-tab="realm"]').click();await page.locator(`[data-army="${a.id}"]`).first().click();await page.locator('[data-merge]').first().click();
  await page.locator(`[data-merge-general="${g.id}"]`).click();assert.equal((await saved()).armies.filter(a=>a.owner==='ashen').length,1);
  await page.locator('[data-quick-split]').click();await page.locator('[data-preset="quarter"]').click();await page.locator('[data-sort-confirm]').click();assert.equal((await saved()).armies.filter(a=>a.owner==='ashen').length,2);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.deepEqual(errors,[]);
  console.log(`PASS ${viewport.width}px: full/partial transfers, exact inputs, reset/cancel isolation, 3 formations, commander selection, shared chat, merge choice, quick split and reload`);await context.close();
 }
}finally{await browser?.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
