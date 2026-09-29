// Functional UI checks only; no screenshots or generated HTML previews.
import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { createGame } from './fixtures/legacy-game.mjs';
import { emptyUnits } from '../economy.mjs';
import { refreshKnowledge } from '../fog.mjs';
const require=createRequire(import.meta.url),{chromium}=require('playwright');
const root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{
  try{const file=path.resolve(root,'.'+new URL(req.url,'http://localhost').pathname);if(!file.startsWith(root+path.sep))throw Error('path');const body=await readFile(file);res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);}catch{res.writeHead(404);res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;
let browser;
try{
  browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
  for(const viewport of [{width:1280,height:850},{width:390,height:844}]){
    const s=createGame(),port=s.tiles['6,6'];port.building='shipyard';port.levels={shipyard:1};s.armies[0].tile=port.id;
    for(const id of ['1,6','2,6','3,6','4,6','5,6','6,6'])s.tiles[id].river=true;
    s.armies[0].units={...emptyUnits(),levy:47};s.kingdoms[0].resources.wood=500;
    const f={id:`fleet-${s.nextId++}`,owner:'ashen',tile:port.id,node:`river:${port.id}`,ships:[{id:`ship-${s.nextId++}`,type:'transport',hp:70,crew:6,cargo:[]}],cargo:[],morale:1,path:[],target:null,order:'hold',landing:null,movementTurn:1,movementSpent:0,resolvedTurn:0};s.fleets.push(f);refreshKnowledge(s);
    const context=await browser.newContext({viewport,hasTouch:viewport.width<700});
    await context.addInitScript(s=>localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s)),s);
    await context.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
    await context.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
    const saved=()=>page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    await page.locator('[data-tab="realm"]').click();await page.locator(`[data-army="${s.armies[0].id}"]`).click();
    assert.equal(await page.locator('[data-naval-action="embark"]').isDisabled(),true);
    assert.match(await page.locator('#panel').textContent(),/47 troops.*2 Transports required/s);
    await page.locator('[data-naval-action="build"][data-ship="transport"]').click();
    assert.equal((await saved()).shipQueues.length,1);assert.equal((await saved()).kingdoms[0].population,74);
    await page.locator('#end-turn').click();await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).turn===2);
    assert.equal((await saved()).fleets[0].ships.length,2);
    await page.locator('[data-naval-action="embark"]').click();
    let next=await saved();assert.equal(next.fleets[0].cargo[0].units.levy,47);assert.equal(next.armies.some(a=>a.id===s.armies[0].id),false);
    await page.locator('[data-naval-action="unload"]').click();const canvas=await page.locator('#map').boundingBox();await page.mouse.click(canvas.x+canvas.width/2,canvas.y+canvas.height/2);
    assert.equal((await saved()).fleets[0].order,'unload');
    await page.locator('#end-turn').click();await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).turn===3);
    next=await saved();assert.equal(next.fleets[0].node,`river:${port.id}`);assert.equal(next.fleets[0].cargo.length,0);assert.equal(next.armies.find(a=>a.id===s.armies[0].id).tile,port.id);
    assert.equal(await page.locator('#panel').evaluate(el=>el.scrollWidth<=el.clientWidth+1),true);
    assert.deepEqual(errors,[]);console.log(`PASS standalone shipyard construction, capacity, embark, unload, fallback rendering and panel width ${viewport.width}`);
    await context.close();
  }
}finally{await browser?.close();await new Promise(r=>server.close(r));}
