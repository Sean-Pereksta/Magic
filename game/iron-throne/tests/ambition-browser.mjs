import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createRequire } from 'node:module';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES ? `${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json` : import.meta.url);
const {chromium}=require('playwright'),root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{
  try{const file=path.resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://localhost').pathname));if(!file.startsWith(root+path.sep))throw Error('path');const body=await readFile(file);res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.json':'application/json','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);}catch{res.writeHead(404);res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
async function select(page,id){
  const from=(await page.locator('#coordinates').textContent()).match(/(\d+),(\d+)$/).slice(1).map(Number),to=id.split(',').map(Number);
  await page.locator('#map').focus();
  for(const [i,plus,minus]of [[0,'ArrowRight','ArrowLeft'],[1,'ArrowDown','ArrowUp']])for(let n=0;n<Math.abs(to[i]-from[i]);n++)await page.keyboard.press(to[i]>from[i]?plus:minus);
}
try{
  for(const viewport of [{width:1280,height:850},{width:390,height:844},{width:844,height:390}]){
    const context=await browser.newContext({viewport}),page=await context.newPage(),errors=[];
    page.on('pageerror',e=>errors.push(e.message));await page.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));await page.route('**/game/iron-throne/config.json',r=>r.fulfill({json:{}}));
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#start-game').click();
    const site=await page.evaluate(async()=>{const {planFoundings}=await import('./founding.mjs');return planFoundings(JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')))[0].capital;});
    await select(page,site);await page.locator('[data-found-city]').click();await page.waitForFunction(()=>document.getElementById('turn').textContent==='Turn 1');
    assert.equal(await page.locator('[data-decree]').count(),3);assert.match(await page.locator('.royal-opening').textContent(),/after turn 6/);assert.match(await page.locator('.charter-status').textContent(),/25%/);
    const commands=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).kingdoms[0].commands);
    await page.locator('[data-decree="mobilization"]').click();assert.equal(await page.locator('[data-decree]').count(),0);
    let saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));assert.equal(saved.kingdoms[0].opening.decree,'mobilization');assert.equal(saved.kingdoms[0].commands,commands);
    await page.locator('[data-tab="realm"]').click();assert.match(await page.locator('.royal-opening>summary').textContent(),/6 turns left/);assert.equal(await page.locator('.opening-briefing').count(),1);
    const discovered=await page.evaluate(async()=>{
      const key='catnmice.iron-throne.v1',s=JSON.parse(localStorage.getItem(key)),{refreshKnowledge}=await import('./fog.mjs'),{recordKingdomDevelopments}=await import('./kingdom-developments.mjs');
      const region=Object.values(s.premiumRegions)[0],scout=s.armies.find(a=>a.owner==='ashen'&&a.scout);scout.tile=region.center;
      refreshKnowledge(s);recordKingdomDevelopments(s,['ashen']);localStorage.setItem(key,JSON.stringify(s));return {tile:region.center,name:region.name};
    });
    await page.reload();await page.locator('#resume').click();await page.locator('#developments-button').click();
    assert.equal(await page.locator('#developments-dialog').evaluate(el=>el.open),true);assert.match(await page.locator('#developments-body').textContent(),new RegExp(discovered.name));
    await page.locator('#developments-body .development-card').filter({hasText:discovered.name}).locator('[data-goto]').first().click();assert.equal(await page.locator('#developments-dialog').evaluate(el=>el.open),false);
    assert.match(await page.locator('.premium-region').textContent(),new RegExp(discovered.name));assert.match(await page.locator('.premium-region').textContent(),/normal base quality/);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.equal(await page.locator('#panel').evaluate(el=>el.scrollWidth<=el.clientWidth+1),true);
    if(process.env.IRON_THRONE_UI_SCREENSHOT&&viewport.width===1280)await page.screenshot({path:process.env.IRON_THRONE_UI_SCREENSHOT});
    await page.locator('#end-turn').click();await page.waitForFunction(()=>document.getElementById('turn').textContent==='Turn 2'&&!document.getElementById('end-turn').disabled);
    saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));assert.equal(saved.kingdoms[0].opening.decree,'mobilization');assert.equal(saved.kingdomDevelopments.version,1);
    assert.deepEqual(errors,[]);console.log(`PASS ${viewport.width}×${viewport.height}: founding decree choice, exact duration and charter, save/load, briefing, discovered rich-region inspection, interactive developments, responsive layout and End Turn`);await context.close();
  }
}finally{await browser.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
