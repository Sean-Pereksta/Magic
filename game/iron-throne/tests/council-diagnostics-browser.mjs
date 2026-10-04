import { clickChatAction } from './fixtures/chat-actions.mjs';
import { appendCouncil } from '../council-state.mjs';
import { initiateCouncilDiscussions } from '../alliance-council.mjs';
import { createGame as legacyGame } from './fixtures/legacy-game.mjs';
// Functional browser checks only: no generated previews, screenshots or assets.
import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const require = createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES ? `${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json` : import.meta.url);
const { chromium } = require('playwright');
const root = fileURLToPath(new URL('../../../', import.meta.url));
const server = http.createServer(async (req, res) => {
  try {
    let pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
    if (pathname.endsWith('/')) pathname += 'index.html';
    const file = path.resolve(root, '.' + pathname);
    if (!file.startsWith(root)) throw new Error('outside root');
    const mime = { '.html': 'text/html', '.mjs': 'text/javascript', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.svg': 'image/svg+xml' }[path.extname(file)] || 'application/octet-stream';
    const body = await readFile(file);
    res.writeHead(200, { 'Content-Type': mime }); res.end(body);
  } catch { res.writeHead(404); res.end('Not found'); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = `http://127.0.0.1:${server.address().port}`;
let browser;
try {
  for(const viewport of [{width:1440,height:1000},{width:390,height:844}]){
    browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox',...(process.env.IRON_THRONE_CHROMIUM?['--disable-dev-shm-usage']:[])]});
    const s=legacyGame();s.treaties.push({id:'alliance-test',type:'alliance',parties:['ashen','wintermere'],expires:11});
    const context=await browser.newContext({viewport});
    await context.addInitScript(s=>{
      localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));
      const realNow=Date.now;globalThis.testTimeShift=0;Date.now=()=>realNow()+globalThis.testTimeShift;
      let verification;globalThis.turnstile={render:(_el,options)=>{verification=options;queueMicrotask(()=>options.callback('test-token'));return 1;},reset:()=>queueMicrotask(()=>verification.callback('fresh-test-token'))};
    },s);
    const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));let calls=0,sessions=0;
    await page.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',r=>r.fulfill({json:{diplomacyEndpoint:'https://worker.example/diplomacy',turnstileSiteKey:'test-public-key'}}));
    await page.route('https://worker.example/session',async r=>{sessions++;return r.fulfill({json:{token:'test-session',expires:await page.evaluate(()=>Date.now()+1800000)}});});
    await page.route('https://worker.example/diplomacy',r=>{
      calls++;if(r.request().postDataJSON().world.formalDecision)return r.fulfill({json:{responses:[{speakerHouseId:'wintermere',message:'Your offered aid is welcome. The recorded agreement stands.'}]}});
      return calls===1?r.fulfill({status:503,headers:{'Retry-After':'1','Access-Control-Expose-Headers':'Retry-After'},json:{diagnostics:{version:1,code:'GEMINI_SCHEMA',providerStatus:400,checks:{GEMINI_API_KEY:'present',TURNSTILE_SECRET:'verified',BUDGET:'verified'}}}})
        :r.fulfill({json:{responses:[{speakerHouseId:'wintermere',message:'Your northern relief force is welcome; my scouts will watch the pass.'}]}});
    });
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
    await page.locator('[data-alliance]').first().click();
    await page.waitForFunction(()=>document.getElementById('ai-status').textContent==='Gemini council connected');
    await page.locator('#alliance-message').fill('I will aid your northern frontier.');await page.locator('.alliance-compose button[type=submit]').click();
    await page.waitForFunction(()=>document.querySelector('.alliance-ai-status').textContent.includes('GEMINI_SCHEMA'));
    assert.match(await page.locator('.alliance-ai-status').textContent(),/GEMINI_SCHEMA/);
    const failedState=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    assert.equal(failedState.allianceCouncils[0].messages.some(m=>m.speakerHouseId==='wintermere'),false,'failed Gemini never inserts a local ruler answer');
    assert.doesNotMatch(await page.locator('.alliance-history').textContent(),/Local dialogue/);
    await clickChatAction(page,'.alliance-diagnostics');
    assert.equal(await page.locator('#gemini-diagnostics-dialog').isVisible(),true);
    assert.match(await page.locator('#diagnostics-report').inputValue(),/Error code: GEMINI_SCHEMA/);
    assert.match(await page.locator('#diagnostics-report').inputValue(),/Google HTTP status: 400/);
    assert.doesNotMatch(await page.locator('#diagnostics-report').inputValue(),/test-session|test-token/);
    await page.locator('#gemini-diagnostics-dialog .close').click();
    // A request-format failure imposes no cooldown on the next envoy.
    await page.locator('#alliance-message').fill('My relief force will approach the northern pass.');await page.locator('.alliance-compose button[type=submit]').click();
    await page.waitForFunction(()=>document.querySelector('.alliance-history').textContent.includes('Your northern relief force'));
    assert.equal(await page.locator('.alliance-diagnostics').evaluate(el=>!el.hidden),false);assert.equal(calls,2);
    assert.match(await page.locator('.alliance-history').textContent(),/Gemini/);
    // A formal decision must restart verification itself after session expiry.
    await page.evaluate(()=>globalThis.testTimeShift=1810000);
    await clickChatAction(page,'.alliance-offer-request');
    const builder=page.locator('#formal-proposal-builder');
    await builder.locator('[name=type]').selectOption('AID');await builder.locator('[name=direction]').selectOption('offer');
    await builder.locator('[name=resource0]').selectOption('food');await builder.locator('[name=amount0]').fill('1');await builder.locator('[type=submit]').click();
    await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).cooperation?.formalProposals?.[0]?.responses.wintermere.voice?.includes('offered aid'));
    assert.equal(sessions,2,'formal voice renewed verification without reopening Council');assert.equal(calls,3);
    assert.doesNotMatch(await page.locator('.alliance-formal-proposals').textContent(),/Considering/);
    assert.deepEqual(errors,[]);console.log(`Council diagnostics, cooldown and Gemini recovery passed at ${viewport.width}px`);
    await context.close();await browser.close();browser=null;
  }

  // Every pending turn-start exchange is queued before verification and survives rate limiting.
  {
    browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox',...(process.env.IRON_THRONE_CHROMIUM?['--disable-dev-shm-usage']:[])]});
    const s=legacyGame();s.treaties.push({id:'expiry-test',type:'alliance',parties:['ashen','wintermere'],expires:2});initiateCouncilDiscussions(s);
    appendCouncil(s,s.allianceCouncils[0],'wintermere','Food supplies will need attention.',{initiated:true,source:'scripted',reason:'food'});
    const context=await browser.newContext();await context.addInitScript(s=>{
      localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));
      globalThis.turnstile={render:(_el,o)=>{globalThis.finishVerification=()=>o.callback('test-token');return 1;},reset:()=>{}};
    },s);
    const page=await context.newPage();let calls=0;
    await page.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',r=>r.fulfill({json:{diplomacyEndpoint:'https://worker.example/diplomacy',turnstileSiteKey:'test-public-key'}}));
    await page.route('https://worker.example/session',r=>r.fulfill({json:{token:'test-session',expires:Date.now()+1800000}}));
    await page.route('https://worker.example/diplomacy',r=>{
      calls++;const body=r.request().postDataJSON();assert.equal(body.world.conversationMode,'ai-initiated-council');
      assert.ok(['expiry','food'].includes(body.world.dispatch.reason));
      if(calls===1)return r.fulfill({status:429,headers:{'Retry-After':'1','Access-Control-Expose-Headers':'Retry-After'},json:{diagnostics:{version:1,code:'CLIENT_RATE_LIMIT',checks:{BUDGET:'verified'}}}});
      return r.fulfill({json:{responses:body.world.dispatch.entries.map(e=>({speakerHouseId:e.speakerHouseId,message:'Let our renewed pact protect the northern frontier.'}))}});
    });
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();await page.locator('[data-alliance]').first().click();
    await page.waitForFunction(()=>typeof globalThis.finishVerification==='function');
    assert.equal(calls,0);assert.match(await page.locator('.alliance-history').textContent(),/Waiting for .*Gemini reply/);
    assert.doesNotMatch(await page.locator('.alliance-history').textContent(),/Local dialogue|Food supplies will need attention/,'automatic dispatch facts are not shown as fabricated ruler speech');
    await page.evaluate(()=>globalThis.finishVerification());
    await page.waitForFunction(()=>{const s=JSON.parse(localStorage.getItem('catnmice.iron-throne.v1'));return s.allianceCouncils[0].messages.every(m=>m.source==='gemini');});
    assert.equal(calls,3);const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
    assert.equal(saved.diplomacy.messages.regular,0);assert.equal(saved.allianceCouncils[0].messages[0].source,'gemini');
    await page.locator('#alliance-council .close').click();await page.locator('[data-alliance]').first().click();assert.equal(calls,3);
    await context.close();console.log('Automatic council Gemini event passed without spending a player dispatch');
  }
} finally {await browser?.close();await new Promise(resolve=>server.close(resolve));}
