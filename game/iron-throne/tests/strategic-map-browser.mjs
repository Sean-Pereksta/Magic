import { clickChatAction, revealChatAction } from './fixtures/chat-actions.mjs';
// Functional desktop/touch checks. No previews, screenshots or generated HTML.
import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createGame } from './fixtures/legacy-game.mjs';
import { relation } from '../core.mjs';
import { refreshKnowledge, knowledgeView } from '../fog.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright');
const root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{try{const file=path.resolve(root,'.'+new URL(req.url,'http://localhost').pathname);if(!file.startsWith(root+path.sep))throw Error('outside root');const body=await readFile(file);res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);}catch{res.writeHead(404);res.end();}});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const base=`http://127.0.0.1:${server.address().port}`;
let browser;
try{
  browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
  for(const viewport of [{width:1280,height:850},{width:390,height:844},{width:844,height:390}]){
    const mobile=viewport.width<900,s=createGame(311);s.treaties.push({id:'test-ally',type:'alliance',parties:['ashen','wintermere'],expires:100});relation(s,'ashen','wintermere').trust=relation(s,'wintermere','ashen').trust=80;refreshKnowledge(s);
    const target=Object.values(s.tiles).find(t=>t.owner==='ashen'&&!t.building&&t.terrain==='plains'),unknown=Object.values(knowledgeView(s,'ashen').tiles).find(t=>t.fog==='unknown'&&!t.knownCapital&&t.q>4&&t.r>4);
    const context=await browser.newContext({viewport,hasTouch:mobile}),page=await context.newPage(),errors=[];page.on('pageerror',e=>{errors.push(e.message);console.error('PAGE ERROR:',e.message);});
    await context.addInitScript(s=>{if(!localStorage.getItem('catnmice.iron-throne.v1'))localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));},s);
    await page.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));await page.route('**/config.json',r=>r.fulfill({json:{}}));
    await page.goto(`${base}/game/iron-throne/index.html`);try{await page.locator('#resume').click();}catch(e){console.error('LOAD:',await page.locator('#load-warning').textContent(),errors);throw e;}await page.locator('[data-tab="war-room"]').click();
    await page.locator('summary').filter({hasText:'PLAN AN OPERATION'}).click();
    await page.locator('[name=operationName]').fill('Operation Browser Pass');await page.locator('[name=objectiveType]').selectOption('defend');
    const pick=page.locator('[data-select-map="Select operation objective"]'),dialog=page.locator('#strategic-map-picker'),canvas=dialog.locator('canvas');
    const clickHex=async t=>{
      await dialog.locator('[data-map-fit]').click();const box=await canvas.boundingBox(),bottom={x:25*Math.sqrt(3)*(s.width-1+(s.height-1)/2),y:37.5*(s.height-1)},zoom=Math.min(box.width/(bottom.x+100),box.height/(bottom.y+100));
      const x=box.x+box.width/2+(25*Math.sqrt(3)*(t.q+t.r/2)-bottom.x/2)*zoom,y=box.y+box.height/2+(37.5*t.r-bottom.y/2)*zoom;
      if(mobile)await page.touchscreen.tap(x,y);else await page.mouse.click(x,y);
    };
    const original=await page.locator('[name=objective]').inputValue();await pick.click();await clickHex(target);
    assert.match(await dialog.locator('.strategic-map-info').textContent(),new RegExp(`Hex ${target.id}`));
    await dialog.locator('footer [data-map-cancel]').click();assert.equal(await page.locator('[name=objective]').inputValue(),original);assert.equal(await page.locator('[name=operationName]').inputValue(),'Operation Browser Pass');
    await pick.click();await clickHex(unknown);assert.match(await dialog.locator('.strategic-map-info').textContent(),/Unexplored Location/);assert.doesNotMatch(await dialog.locator('.strategic-map-info').textContent(),/House|forest|iron|fort/);
    await dialog.locator('[data-map-confirm]').click();assert.equal(await page.locator('[name=objective]').inputValue(),unknown.id);
    await pick.click();await clickHex(target);
    const selectedText=await dialog.locator('.strategic-map-info').textContent();
    if(!mobile){const box=await canvas.boundingBox();await page.mouse.move(box.x+box.width/2,box.y+box.height/2);await page.mouse.down();await page.mouse.move(box.x+box.width/2+50,box.y+box.height/2+20,{steps:4});await page.mouse.up();assert.equal(await dialog.locator('.strategic-map-info').textContent(),selectedText);await canvas.focus();await page.keyboard.press('ArrowRight');assert.notEqual(await dialog.locator('.strategic-map-info').textContent(),selectedText);await clickHex(target);}
    await dialog.locator('[data-map-zoom="1.25"]').click();const button=await dialog.locator('[data-map-confirm]').boundingBox();assert.ok(button.y>=0&&button.y+button.height<=viewport.height);
    await dialog.locator('[data-map-confirm]').click();assert.equal(await page.locator('[name=objective]').inputValue(),target.id);
    const own=page.locator('[data-operation-house="ashen"]'),partner=page.locator('[data-operation-house="wintermere"]');await own.locator('[name=troops]').fill('10');await partner.locator('[name=include]').check();await partner.locator('[name=troops]').fill('10');
    await own.locator('[data-select-map]').click();await clickHex(target);await dialog.locator('[data-map-confirm]').click();assert.equal(await own.locator('[name=rally]').inputValue(),target.id);
    await page.locator('#operation-form [type=submit]').click();
    const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1'))),op=saved.cooperation.operations.find(o=>o.name==='Operation Browser Pass');assert.ok(op);assert.equal(op.targetTile,target.id);assert.equal(op.objectiveType,'defend');assert.equal(op.targetHouse,null);
    await page.locator('[data-alliance]').first().click();await page.locator('#alliance-message').fill('Hold this position with me.');await (await revealChatAction(page,'.alliance-action')).selectOption('hold');await clickChatAction(page,'.alliance-map');await clickHex(target);await dialog.locator('[data-map-confirm]').click();assert.equal(await page.locator('#alliance-message').inputValue(),'Hold this position with me.');
    await page.locator('.alliance-compose [type=submit]').click();await page.waitForFunction(()=>document.querySelector('[data-council-location]'));
    const councilSaved=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));assert.deepEqual(councilSaved.allianceCouncils[0].messages[0].location,{targetTile:target.id,objectiveType:'hold'});
    await page.locator('[data-council-location]').first().click();assert.equal(await page.locator('[name=objective]').inputValue(),target.id);assert.equal(await page.locator('[name=objectiveType]').inputValue(),'hold');
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.deepEqual(errors,[]);
    console.log(`Strategic selection, cancel/change, fog, rally, Council and save passed at ${viewport.width}×${viewport.height}`);await context.close();
  }
}finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
