// Functional council checks only; no HTML previews, screenshots, or generated art.
import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createGame } from './fixtures/legacy-game.mjs';
import { kingdom } from '../core.mjs';
import { commitDeal, deliverPledge, validateIntent } from '../diplomacy.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright');
const root=fileURLToPath(new URL('../../../',import.meta.url));
const server=http.createServer(async(req,res)=>{
  try{
    const pathname=decodeURIComponent(new URL(req.url,'http://localhost').pathname),file=path.resolve(root,'.'+pathname);
    if(!file.startsWith(root))throw new Error('outside root');
    const body=await readFile(file),mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream';
    res.writeHead(200,{'Content-Type':mime});res.end(body);
  }catch{res.writeHead(404);res.end('Not found');}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const base=`http://127.0.0.1:${server.address().port}`;
const initial=createGame();kingdom(initial,'ashen').resources.gold=2000;
for(let turn=2;turn<=10;turn+=2){initial.turn=turn;assert.equal(commitDeal(initial,'wintermere',validateIntent({type:'PROMISE',giveAmount:60,duration:2})).ok,true);assert.equal(deliverPledge(initial,initial.pledges.at(-1).id).ok,true);}
initial.treaties.push({id:'earned-alliance',parties:['ashen','wintermere'],type:'alliance',expires:50});
let browser;
try{
  browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox']});
  for(const viewport of [{width:1280,height:850},{width:390,height:844}]){
    const context=await browser.newContext({viewport,hasTouch:viewport.width<900});
    await context.addInitScript(initial=>{if(!localStorage.getItem('catnmice.iron-throne.v1'))localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(initial));},initial);
    const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
    await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
    const open=async()=>{await page.locator('[data-tab="council"]').click();await page.locator('[data-talk="wintermere"]').click();};
    const send=async text=>{await page.locator('#chat-message').fill(text);await page.locator('#send-chat').click();await page.waitForFunction(()=>document.getElementById('send-chat').textContent==='Send envoy →');};
    const saved=()=>page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    await open();assert.equal(await page.locator('#offer-type option[value="MARRIAGE"]').count(),0);
    await send('Would you consider joining our families? I seek the hand of your daughter.');
    assert.match(await page.locator('#messages').textContent(),/later turn/);assert.equal((await saved()).royalBonds.marriages.length,0);
    assert.equal(await page.locator('#offer-type option[value="MARRIAGE"]').count(),1);
    await page.locator('[data-close="diplomacy"]').click();await page.locator('#end-turn').click();
    await page.waitForFunction(()=>document.getElementById('turn').textContent==='Turn 11');
    await open();await send('Let us discuss the marriage settlement.');
    await page.locator('#proposals [data-modify]').first().click();
    assert.equal(await page.locator('#marriage-fields').isVisible(),true);
    assert.equal(await page.locator('#marriage-rulerMember').inputValue(),'daughter');
    await page.locator('#marriage-shipmentResource').selectOption('iron');
    await page.locator('#marriage-shipmentAmount').fill('2');await page.locator('#marriage-shipmentTurns').fill('3');
    await page.locator('#offer-form button[type="submit"]').click();
    await page.waitForFunction(()=>document.getElementById('send-chat').textContent==='Send envoy →');
    assert.match(await page.locator('#proposals').textContent(),/2 iron per turn for 3 turns/);
    const before=await saved();await page.locator('#proposals [data-ratify], #proposals [data-ratify-counter]').first().click();
    const after=await saved();assert.equal(after.royalBonds.marriages.length,1);
    const m=after.royalBonds.marriages[0];assert.equal(m.status,'active');assert.equal(m.terms.shipmentAmount,2);assert.equal(m.terms.shipmentTurns,3);
    assert.equal(after.kingdoms[0].resources.gold,before.kingdoms[0].resources.gold-m.terms.giveAmount);
    assert.match(await page.locator('#council-records-body').textContent(),/Royal marriage.*active/s);
    await page.reload();await page.locator('#resume').click();await open();
    assert.match(await page.locator('#relations-summary').textContent(),/Royal marriage · active/);
    assert.equal((await saved()).royalBonds.marriages.length,1);assert.deepEqual(errors,[]);
    await context.close();
  }
  console.log('Marriage council passed on desktop and mobile: discussion, deliberation, editable settlement, ratification, ledger, reload.');
}finally{await browser?.close();server.close();}
