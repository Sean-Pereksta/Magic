// Functional interaction checks only: no screenshots, HTML previews or art output.
import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createGame } from './fixtures/legacy-game.mjs';
import { kingdom } from '../core.mjs';
import { commitDeal, deliverPledge, validateIntent } from '../diplomacy.mjs';
import { recordRulerSpeech } from '../ruler-knowledge.mjs';
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
const initial=createGame();kingdom(initial,'ashen').resources.gold=2500;
for(let turn=2;turn<=10;turn+=2){
  initial.turn=turn;assert.equal(commitDeal(initial,'wintermere',validateIntent({type:'PROMISE',giveAmount:60,duration:2})).ok,true);
  assert.equal(deliverPledge(initial,initial.pledges.at(-1).id).ok,true);
}
initial.treaties.push({id:'earned-alliance',parties:['ashen','wintermere'],type:'alliance',expires:50});
recordRulerSpeech(initial,'sunspire','I want to overthrow Ashen. Will you help me?','wintermere');
let browser;
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox']});
 for(const viewport of [{width:1280,height:850},{width:390,height:844}]){
  const context=await browser.newContext({viewport,hasTouch:viewport.width<900});
  await context.addInitScript(initial=>{if(!localStorage.getItem('catnmice.iron-throne.v1'))localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(initial));},initial);
  const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
  await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
  await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
  const open=async ruler=>{await page.locator('[data-tab="council"]').click();await page.locator(`[data-talk="${ruler}"]`).click();};
  const saved=()=>page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
  const settled=()=>page.waitForFunction(()=>document.getElementById('send-chat').textContent==='Send envoy →');
  const send=async text=>{await page.locator('#chat-message').fill(text);await page.locator('#send-chat').click();await settled();};
  const review=()=>page.locator('.correspondence-cards').getByRole('button',{name:'Review Terms',exact:true}).first().click();
  const close=async()=>{if(await page.locator('#diplomacy').evaluate(el=>el.classList.contains('treaty-open')))await page.locator('.treaty-drawer-close').click();await page.locator('[data-close="diplomacy"]').click();};
  const ratify=()=>page.locator('#proposals [data-ratify], #proposals [data-ratify-counter]').first().click();
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();

  // An intelligence quotation is not a gift, and private sources are not shown
  // while the player is merely browsing its exact scope and price.
  await open('sunspire');
  await send('Has anyone spoken to you about overthrowing my House?');
  await send('Could I pay 25 gold to know who?');
  assert.doesNotMatch(await page.locator('#messages').textContent(),/House Wintermere/);
  const before=await saved(),quote=before.rulerKnowledge.offers.find(q=>q.status==='open');assert.ok(quote);
  assert.equal(before.rulerKnowledge.receipts.length,0);
  await review();assert.equal(await page.locator('#offer-form').isVisible(),false);
  await ratify();
  const purchased=await saved();assert.equal(purchased.rulerKnowledge.receipts.length,1);
  assert.equal(purchased.kingdoms[0].resources.gold,before.kingdoms[0].resources.gold-quote.price);
  assert.match(await page.locator('#council-records-body').textContent(),/House Wintermere.*approached our court/);
  await close();

  // Marriage can start directly in the Treaty Desk, without a magic chat phrase.
  await open('wintermere');await page.locator('#quick-offer').click();
  await page.locator('#offer-type').selectOption('MARRIAGE');
  assert.equal(await page.locator('#offer-form').isVisible(),true);
  await page.locator('#marriage-actorMember').selectOption('ruler');await page.locator('#marriage-rulerMember').selectOption('ruler');
  await page.locator('#give-amount').fill('100');await page.locator('#duration').fill('12');
  await page.locator('#offer-form button[type="submit"]').click();await settled();
  assert.equal((await saved()).royalBonds.marriages.length,0);
  assert.match(await page.locator('#marriage-readiness').textContent(),/turn 11/);
  await close();await page.locator('#end-turn').click();await page.waitForFunction(()=>document.getElementById('turn').textContent==='Turn 11');
  await open('wintermere');await review();
  const marriageBefore=await saved();await ratify();
  const married=await saved(),bond=married.royalBonds.marriages[0];assert.ok(bond);assert.equal(bond.status,'active');
  assert.deepEqual(bond.members.map(m=>m.role),['ruler','ruler']);
  assert.equal(married.kingdoms[0].resources.gold,marriageBefore.kingdoms[0].resources.gold-bond.terms.giveAmount);
  await page.reload();await page.locator('#resume').click();await open('sunspire');
  assert.match(await page.locator('#council-records-body').textContent(),/House Wintermere.*approached our court/);
  assert.equal((await saved()).royalBonds.marriages.length,1);
  assert.deepEqual(errors,[]);await context.close();
  console.log(`PASS ${viewport.width}x${viewport.height}: private quote → exact payment → dated receipt; direct named marriage → deliberation → ratification → reload.`);
 }
}finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
