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
    const body=await readFile(file);
    res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);
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
    await page.goto(`${base}/game/iron-throne/index.html`);
    await page.evaluate(async()=>{
      const {createGame}=await import('./tests/fixtures/legacy-game.mjs');
      const {proposeCooperation}=await import('./strategic-diplomacy.mjs');
      const {createOperation,defaultRally}=await import('./operations.mjs');
      const {settlements,relation}=await import('./core.mjs');
      const s=createGame(311);
      for(const k of s.kingdoms){k.commands=0;k.resources.food=k.resources.gold=400;}
      s.treaties.push({id:'test-alliance',type:'alliance',parties:['ashen','wintermere'],expires:100});
      relation(s,'wintermere','ashen').trust=relation(s,'ashen','wintermere').trust=80;
      proposeCooperation(s,'thornwall','ashen','trade',{reason:'Build trade across the frontier.'});
      s.treaties.push({id:'secret-alliance',type:'alliance',parties:['sunspire','redharbor'],expires:100});
      const targetTile=settlements(s,'thornwall')[0].id;
      const hidden=createOperation(s,'sunspire',{name:'UNSEEN CAMPAIGN',targetTile,attackStart:9,attackEnd:12,participants:['sunspire','redharbor'].map((house,i)=>({house,role:i?'flank':'assault',rally:defaultRally(s,house,targetTile),requiredTroops:10,requiredSiege:0,food:0}))});
      if(!hidden.ok)throw new Error(hidden.error);
      localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));
    });
    await page.reload();await page.locator('#resume').click();await page.locator('[data-tab="war-room"]').click();
    assert.equal(await page.locator('#panel h2').textContent(),'War Room');
    assert.doesNotMatch(await page.locator('#panel').textContent(),/UNSEEN CAMPAIGN/);
    await page.locator('[data-cooperation-answer][data-decision="counter"]').click();
    assert.match(await page.locator('#panel').textContent(),/six-turn agreement/);
    await page.locator('summary').filter({hasText:'PLAN AN OPERATION'}).click();
    await page.locator('[name="operationName"]').fill('Operation Browser Gate');
    await page.locator('[name="objective"]').selectOption({label:(await page.locator('[name="objective"] option').allTextContents()).find(t=>t.includes('House Vesper'))});
    const own=page.locator('[data-operation-house="ashen"]'),partner=page.locator('[data-operation-house="wintermere"]');
    await own.locator('[name=troops]').fill('10');await partner.locator('[name=include]').check();await partner.locator('[name=role]').selectOption('supply');await partner.locator('[name=troops]').fill('0');await partner.locator('[name=food]').fill('30');
    await page.locator('#operation-form [type=submit]').click();
    assert.match(await page.locator('#panel').textContent(),/Operation Browser Gate/);
    let save=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    const operation=save.cooperation.operations.find(o=>o.name==='Operation Browser Gate');assert.ok(operation);assert.equal(operation.participants[1].status,'invited');
    await page.locator('#end-turn').click();await page.locator('#end-turn').click();
    save=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    const updated=save.cooperation.operations.find(o=>o.id===operation.id);assert.equal(updated.participants[1].status,'accepted');assert.equal(save.pledges.find(p=>p.operationId===operation.id&&p.operationTask==='supply').status,'fulfilled');
    await page.locator(`[data-operation-leave="${operation.id}"]`).click();
    save=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));assert.equal(save.cooperation.operations.find(o=>o.id===operation.id).status,'Abandoned');
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    assert.deepEqual(errors,[]);console.log(`War Room proposals, roles, supplies, privacy and withdrawal passed at ${viewport.width}×${viewport.height}`);await context.close();
  }
}finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
