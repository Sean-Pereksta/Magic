// Functional checks only; no HTML previews or screenshots. Remote PNGs are mocked.
import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {ALL_ART_PATHS} from '../asset-manifest.mjs';
import {SPRITE_PATHS} from '../sprite-animations.mjs';
import {createGame as legacyGame} from './fixtures/legacy-game.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES ? `${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json` : import.meta.url),{chromium}=require('playwright');
const root=fileURLToPath(new URL('../../../',import.meta.url));
const server=http.createServer(async(req,res)=>{try{const p=path.join(root,new URL(req.url,'http://localhost').pathname);res.setHeader('Content-Type',({'.mjs':'text/javascript','.html':'text/html','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'})[path.extname(p)]||'text/plain');res.end(await readFile(p));}catch{res.writeHead(404);res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox']});
const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aOuoAAAAASUVORK5CYII=','base64');
try{
for(const width of [1280,390]){
 const page=await browser.newPage({viewport:{width,height:844},hasTouch:width===390});const requests=[],errors=[];
 await page.addInitScript(initial=>localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(initial)),legacyGame());
 page.on('pageerror',e=>errors.push(e.message));
 await page.route('https://pub-*.r2.dev/**',async route=>{requests.push(route.request().url());if(route.request().url().endsWith('plains_04.png'))return route.fulfill({status:404,body:''});await route.fulfill({contentType:'image/png',body:process.env.IRON_ART_PROBE&&route.request().url().endsWith('lumber_1.png')?await readFile(process.env.IRON_ART_PROBE):png});});
 await page.route('**/config.json',r=>r.fulfill({json:{}}));
 await page.goto(`http://127.0.0.1:${server.address().port}/game/iron-throne/index.html`);
 await page.waitForFunction(()=>document.getElementById('art-status')?.textContent.includes('ready'));
 await page.waitForFunction(()=>!document.getElementById('start-game').disabled);
 assert.equal(new Set(requests.filter(url=>!new URL(url).pathname.startsWith('/sprites/'))).size,ALL_ART_PATHS.length);assert.ok(requests[0].endsWith('buildings/lumber_1.png'));
 assert.match(await page.locator('#art-status').textContent(),/1 unavailable/);
 await page.locator('#resume').click();await page.waitForTimeout(200);
 assert.equal(await page.locator('#turn').textContent(),'Turn 1');
 assert.equal(await page.locator('#resources img').count(),8);
 await page.locator('#map').click({position:{x:120,y:140}});
 await page.mouse.move(180,240);await page.mouse.down();await page.mouse.move(250,290);await page.mouse.up();
 await page.locator('#home').click();await page.waitForTimeout(100);
 const staticRequests=requests.filter(url=>!new URL(url).pathname.startsWith('/sprites/'));
 assert.equal(new Set(staticRequests).size,ALL_ART_PATHS.length);
 const spriteRequests=requests.filter(url=>new URL(url).pathname.startsWith('/sprites/'));
 assert.ok(spriteRequests.every(url=>SPRITE_PATHS.includes(new URL(url).pathname.slice(1))));
 assert.equal(new Set(spriteRequests).size,spriteRequests.length);assert.deepEqual(errors,[]);
 console.log(`PASS ${width}px: preload, missing fallback, campaign, HUD, selection, pan, no repeat PNGs`);
 await page.close();
}
}finally{await browser.close();await new Promise(r=>server.close(r));}
