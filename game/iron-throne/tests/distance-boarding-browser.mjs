// Functional checks only: no generated previews or screenshots.
import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { createGame } from './fixtures/legacy-game.mjs';
import { emptyUnits } from '../economy.mjs';
import { refreshKnowledge } from '../fog.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright');
const root=fileURLToPath(new URL('../../../',import.meta.url));
const server=http.createServer(async(req,res)=>{
  try{
    const file=path.resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://localhost').pathname));
    if(!file.startsWith(root))throw Error('path');
    const body=await readFile(file);res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);
  }catch{res.writeHead(404);res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));let browser;
try{
  browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
  for(const viewport of [{width:1280,height:850},{width:390,height:844}]){
    const s=createGame(),a=s.armies[0];a.tile='6,6';a.units={...emptyUnits(),levy:25};
    for(const t of Object.values(s.tiles))if(t.q>=1&&t.q<=7&&t.r>=5&&t.r<=7){t.terrain='plains';t.owner='ashen';t.road=false;t.river=t.r===6;if(t.levels)delete t.levels.road;}
    const f={id:`fleet-${s.nextId++}`,owner:'ashen',tile:'1,6',node:'river:1,6',ships:[{id:`ship-${s.nextId++}`,type:'transport',hp:70,crew:6,cargo:[]}],cargo:[],morale:1,path:[],target:null,order:'hold',landing:null,movementTurn:1,movementSpent:0,resolvedTurn:0};s.fleets.push(f);refreshKnowledge(s);
    const context=await browser.newContext({viewport,hasTouch:viewport.width<700});
    await context.addInitScript(s=>{
      if(!localStorage.getItem('catnmice.iron-throne.v1'))localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));
      window.__boardingBadges=[];const fill=CanvasRenderingContext2D.prototype.fillText;
      CanvasRenderingContext2D.prototype.fillText=function(text,...args){if(this.strokeStyle==='#8ee8ad'&&/^\d+$/.test(String(text)))window.__boardingBadges.push(String(text));return fill.call(this,text,...args);};
    },s);
    await context.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));
    await context.route('**/game/iron-throne/config.json',r=>r.fulfill({json:{}}));
    const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.goto(`http://127.0.0.1:${server.address().port}/game/iron-throne/index.html`);await page.locator('#resume').click();
    await page.locator('[data-tab="realm"]').click();await page.locator(`[data-army="${a.id}"]`).click();
    await page.locator(`[data-order="${a.id}"]`).click();
    const box=await page.locator('#map').boundingBox(),zoom=viewport.width<600?.85:1.25;
    await page.mouse.click(box.x+box.width/2-25*Math.sqrt(3)*5*zoom,box.y+box.height/2);
    const saved=()=>page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    let next=await saved();assert.equal(next.armies[0].embarkOrder.fleet,f.id);assert.ok(next.armies[0].path.length);assert.equal(next.fleets[0].cargo.length,0);
    await page.waitForFunction(()=>window.__boardingBadges.includes('1'));
    await page.reload();await page.locator('#resume').click();next=await saved();assert.equal(next.armies[0].embarkOrder.fleet,f.id);
    await page.locator('#end-turn').click();await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).turn===2);
    next=await saved();assert.equal(next.fleets[0].cargo.length,0);assert.equal(next.armies[0].embarkOrder.fleet,f.id);
    await page.locator('[data-tab="realm"]').click();await page.locator(`[data-army="${a.id}"]`).click();
    await page.waitForFunction(()=>window.__boardingBadges.includes('0'));
    assert.match(await page.locator('#panel').textContent(),/Boarding 25 troops this turn/);
    await page.locator('#end-turn').click();await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).turn===3);
    next=await saved();assert.equal(next.armies.some(x=>x.id===a.id),false);assert.equal(next.fleets[0].cargo[0].id,a.id);assert.equal(next.fleets[0].cargo[0].units.levy,25);
    assert.deepEqual(errors,[]);console.log(`PASS distant March click, numeric 1/0 boarding badges, reload and automatic boarding ${viewport.width}`);
    await context.close();
  }
}finally{await browser?.close();await new Promise(r=>server.close(r));}
