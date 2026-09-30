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
    s.armies[0].units={...emptyUnits(),levy:70};s.kingdoms[0].resources.wood=500;
    const f={id:`fleet-${s.nextId++}`,owner:'ashen',tile:port.id,node:`river:${port.id}`,ships:[{id:`ship-${s.nextId++}`,type:'transport',hp:70,crew:6,cargo:[]}],cargo:[],morale:1,path:[],target:null,order:'hold',landing:null,movementTurn:1,movementSpent:0,resolvedTurn:0};s.fleets.push(f);refreshKnowledge(s);
    const context=await browser.newContext({viewport,hasTouch:viewport.width<700});
    await context.addInitScript(s=>{localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));window.__routes=[];const stroke=CanvasRenderingContext2D.prototype.stroke;CanvasRenderingContext2D.prototype.stroke=function(...args){if(['#84ddff','#8ee8ad','#ffd17d','#ff626a'].includes(this.strokeStyle))window.__routes.push(this.strokeStyle);return stroke.apply(this,args);};window.__orderLabels=[];const fill=CanvasRenderingContext2D.prototype.fillText;CanvasRenderingContext2D.prototype.fillText=function(text,...args){if(typeof text==='string'&&/END TURN|TURNS/.test(text)){window.__orderLabels.push(text);window.__orderLabels=window.__orderLabels.slice(-100);}return fill.call(this,text,...args);};},s);
    await context.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
    await context.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
    const saved=()=>page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    await page.locator('[data-tab="realm"]').click();await page.locator(`[data-army="${s.armies[0].id}"]`).click();
    await page.locator(`[data-order="${s.armies[0].id}"]`).click();
    {const box=await page.locator('#map').boundingBox();await page.mouse.click(box.x+box.width/2-43.3,box.y+box.height/2);}
    assert.equal((await saved()).turn,1);assert.equal((await saved()).armies[0].tile,port.id);assert.ok((await saved()).armies[0].path.length);
    await page.waitForFunction(()=>window.__routes.includes('#84ddff'));
    await page.locator('[data-tab="realm"]').click();await page.locator(`[data-army="${s.armies[0].id}"]`).click();await page.locator(`[data-hold="${s.armies[0].id}"]`).click();
    assert.equal(await page.locator('[data-naval-action="embark"]').isDisabled(),false);
    assert.match(await page.locator('#panel').textContent(),/70 troops ashore · 25 can board · 45 stay ashore/);
    await page.locator(`[data-order="${s.armies[0].id}"]`).click();{const box=await page.locator('#map').boundingBox();await page.mouse.click(box.x+box.width/2,box.y+box.height/2);}
    assert.equal((await saved()).armies[0].embarkOrder.count,25);assert.equal((await saved()).fleets[0].cargo.length,0);
    await page.waitForFunction(()=>window.__routes.includes('#8ee8ad'));
    await page.locator(`[data-hold="${s.armies[0].id}"]`).click();
    for(let i=0;i<2;i++)await page.locator('[data-naval-action="build"][data-ship="transport"]').click();
    assert.equal((await saved()).shipQueues.length,2);assert.equal((await saved()).kingdoms[0].population,68);
    for(const turn of [2,3]){await page.locator('#end-turn').click();await page.waitForFunction(turn=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).turn===turn,turn);}
    assert.equal((await saved()).fleets[0].ships.length,3);assert.match(await page.locator('#panel').textContent(),/0\/75 troops aboard/);
    await page.locator(`[data-order="${s.armies[0].id}"]`).click();{const box=await page.locator('#map').boundingBox();await page.mouse.click(box.x+box.width/2,box.y+box.height/2);}
    let next=await saved();assert.equal(next.armies[0].embarkOrder.count,70);assert.equal(next.fleets[0].cargo.length,0);
    await page.locator('[data-tab="realm"]').click();await page.waitForFunction(()=>window.__routes.includes('#8ee8ad'));
    await page.locator('#end-turn').click();await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).turn===4);
    next=await saved();assert.equal(next.fleets[0].cargo[0].units.levy,70);assert.equal(next.armies.some(a=>a.id===s.armies[0].id),false);
    await page.locator(`[data-goto="${port.id}"]`).last().click();
    await page.locator('[data-naval-action="move"]').click();const canvas=await page.locator('#map').boundingBox();await page.mouse.click(canvas.x+canvas.width/2-86.6,canvas.y+canvas.height/2);
    assert.equal((await saved()).fleets[0].order,'move');await page.waitForFunction(()=>window.__routes.includes('#84ddff'));
    await page.locator('[data-tab="realm"]').click();await page.locator(`[data-goto="${port.id}"]`).last().click();
    await page.locator('[data-naval-action="unload"]').click();await page.mouse.click(canvas.x+canvas.width/2,canvas.y+canvas.height/2);
    assert.equal((await saved()).fleets[0].order,'unload');await page.waitForFunction(()=>window.__routes.includes('#ffd17d'));
    await page.locator('#end-turn').click();await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).turn===5);
    next=await saved();assert.equal(next.fleets[0].node,`river:${port.id}`);assert.equal(next.fleets[0].cargo.length,0);assert.equal(next.armies.find(a=>a.id===s.armies[0].id).tile,port.id);
    assert.equal(await page.locator('#panel').evaluate(el=>el.scrollWidth<=el.clientWidth+1),true);
    assert.deepEqual(await page.evaluate(()=>window.__orderLabels),[]);assert.deepEqual(errors,[]);console.log(`PASS partial boarding, 75-seat fleet, queued loading, immediate text-free marching/sailing/unload lines, fallback rendering and panel width ${viewport.width}`);
    await context.close();
    // Exercise actual ranged target selection, not only simulation helpers.
    const ranged=createGame(),shore=ranged.tiles['6,6'];ranged.armies[0].tile=shore.id;ranged.armies[0].units={...emptyUnits(),archer:20};
    for(const id of ['4,6','5,6','6,6'])ranged.tiles[id].river=true;
    ranged.wars.push('ashen:wintermere');
    const addFleet=(owner,tile)=>{const f={id:`fleet-${ranged.nextId++}`,owner,tile,node:`river:${tile}`,ships:[{id:`ship-${ranged.nextId++}`,type:'warship',hp:150,crew:10,cargo:[]}],cargo:[],morale:1,path:[],target:null,order:'hold',landing:null,movementTurn:1,movementSpent:0,resolvedTurn:0};ranged.fleets.push(f);return f;};
    const friendly=addFleet('ashen',shore.id),enemy=addFleet('wintermere','4,6');refreshKnowledge(ranged);
    const combat=await browser.newContext({viewport,hasTouch:viewport.width<700});
    await combat.addInitScript(s=>{localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));window.__routes=[];const stroke=CanvasRenderingContext2D.prototype.stroke;CanvasRenderingContext2D.prototype.stroke=function(...args){if(['#84ddff','#8ee8ad','#ffd17d','#ff626a'].includes(this.strokeStyle))window.__routes.push(this.strokeStyle);return stroke.apply(this,args);};window.__orderLabels=[];const fill=CanvasRenderingContext2D.prototype.fillText;CanvasRenderingContext2D.prototype.fillText=function(text,...args){if(typeof text==='string'&&/END TURN/.test(text))window.__orderLabels.push(text);return fill.call(this,text,...args);};},ranged);
    await combat.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));await combat.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    const battle=await combat.newPage(),combatErrors=[];battle.on('pageerror',e=>combatErrors.push(e.message));
    await battle.goto(`${base}/game/iron-throne/index.html`);await battle.locator('#resume').click();
    await battle.locator('[data-tab="realm"]').click();await battle.locator(`[data-army="${ranged.armies[0].id}"]`).click();
    await battle.locator('[data-naval-action="attack"]').click();const area=await battle.locator('#map').boundingBox();await battle.mouse.click(area.x+area.width/2-86.6,area.y+area.height/2);
    await battle.waitForFunction(()=>window.__routes.includes('#ff626a'));
    let orders=await battle.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));assert.equal(orders.fleets.find(f=>f.id===friendly.id).attackTile,enemy.tile);
    await battle.locator('[data-tab="realm"]').click();await battle.locator(`[data-army="${ranged.armies[0].id}"]`).click();
    assert.equal(await battle.locator('[data-ranged-order]').count(),0);await battle.mouse.click(area.x+area.width/2-86.6,area.y+area.height/2);
    await battle.waitForFunction(()=>window.__routes.includes('#ff626a'));
    orders=await battle.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));assert.equal(orders.armies[0].order,'ranged');assert.equal(orders.armies[0].target,enemy.tile);
    await battle.locator('#end-turn').click();await battle.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).turn===2);
    orders=await battle.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));assert.ok(orders.militaryEvents.filter(e=>e.attacker==='ashen'&&e.ranged).length>=2);
    assert.deepEqual(await battle.evaluate(()=>window.__orderLabels),[]);assert.deepEqual(combatErrors,[]);console.log(`PASS warship and contextual land ranged targeting, compact attack symbols and end-turn fire ${viewport.width}`);await combat.close();
  }
}finally{await browser?.close();await new Promise(r=>server.close(r));}
