// Functional checks only; no screenshots, rendered previews or generated artwork.
import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createRequire } from 'node:module';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright'),root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{
 try{const file=path.resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://localhost').pathname));if(!file.startsWith(root+path.sep))throw Error('path');const body=await readFile(file);res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.json':'application/json','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);}catch{res.writeHead(404);res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--no-zygote','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
async function select(page,id){
 const from=(await page.locator('#coordinates').textContent()).match(/(\d+),(\d+)$/).slice(1).map(Number),to=id.split(',').map(Number);
 await page.locator('#map').focus();
 for(const [i,plus,minus]of [[0,'ArrowRight','ArrowLeft'],[1,'ArrowDown','ArrowUp']])for(let n=0;n<Math.abs(to[i]-from[i]);n++)await page.keyboard.press(to[i]>from[i]?plus:minus);
}
try{
 for(const viewport of [{width:1280,height:850},{width:390,height:844},{width:844,height:390}]){
  const context=await browser.newContext({viewport,hasTouch:viewport.width<900}),page=await context.newPage(),errors=[];
  page.on('pageerror',e=>errors.push(e.message));page.on('dialog',dialog=>{assert.match(dialog.message(),/^Found Emberkeep here/);void dialog.accept();});
  await page.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));await page.route('**/game/iron-throne/config.json',r=>r.fulfill({json:{}}));
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#preset').selectOption('great-basin');await page.locator('#start-game').click();
  assert.equal(await page.locator('#turn').textContent(),'Founding');assert.equal(await page.locator('#end-turn').isDisabled(),true);assert.equal(await page.locator('#preset option').count(),7);
  const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));assert.equal(saved.mapProfile,'great-basin');assert.equal(saved.turn,0);
  await select(page,'0,0');assert.equal(await page.locator('[data-found-city]').isDisabled(),true);assert.match(await page.locator('#panel').textContent(),/cannot be founded on water/);
  await page.reload();await page.locator('#resume').click();assert.equal(await page.locator('#turn').textContent(),'Founding');
  const site=await page.evaluate(async()=>{const {planFoundings}=await import('./founding.mjs');const s=JSON.parse(localStorage.getItem('catnmice.iron-throne.v1'));return planFoundings(s)[0].capital;});
  await select(page,site);assert.match(await page.locator('#panel').textContent(),/FOUNDING OUTLOOK/);assert.equal(await page.locator('[data-found-city]').isEnabled(),true);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
  await page.locator('[data-found-city]').click();await page.waitForFunction(()=>document.getElementById('turn').textContent==='Turn 1');
  const founded=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));assert.equal(Object.values(founded.founding.houses).filter(h=>h.founded).length,6);assert.equal(founded.founding.houses.ashen.capital,site);
  const natural=s=>Object.values(s.tiles).map(t=>[t.id,t.terrain,t.resource,t.quality,t.river]);assert.deepEqual(natural(founded),natural(saved));
  await page.locator('[data-recruit="levy"]').click();assert.match(await page.locator('#panel').textContent(),/36 troops/);
  await page.locator('#end-turn').click();assert.equal(await page.locator('#turn').textContent(),'Turn 2');
  await page.reload();await page.locator('#resume').click();assert.equal(await page.locator('#turn').textContent(),'Turn 2');
  assert.deepEqual(errors,[]);console.log(`PASS ${viewport.width}×${viewport.height}: map selection, founding preview, invalid land, confirmation, six capitals, untouched terrain, recruit, turn and resume`);await context.close();
 }
}finally{await browser.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
