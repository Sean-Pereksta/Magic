import { clickChatAction, revealChatAction } from './fixtures/chat-actions.mjs';
// Functional Council/private interaction checks; no HTML previews or screenshots.
import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {createGame} from './fixtures/legacy-game.mjs';
import {relation} from '../core.mjs';
import {refreshKnowledge} from '../fog.mjs';
import {validateIntent} from '../diplomacy.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright'),root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{try{const file=path.resolve(root,'.'+new URL(req.url,'http://localhost').pathname);if(!file.startsWith(root+path.sep))throw Error('path');res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(await readFile(file));}catch{res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;let browser;
function setup(){const s=createGame(311);for(const h of ['wintermere','redharbor','thornwall']){s.treaties.push({id:`ally-${h}`,type:'alliance',parties:['ashen',h],expires:100});Object.assign(relation(s,h,'ashen'),{trust:95,opinion:95,reliability:95,grievance:0});}for(const k of s.kingdoms)k.resources.food=k.resources.iron=k.resources.gold=500;refreshKnowledge(s);return s;}
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
 for(const viewport of [{width:1280,height:850},{width:390,height:844}])for(const succeeds of [false,true]){
  const context=await browser.newContext({viewport,hasTouch:viewport.width<700}),page=await context.newPage(),errors=[];let calls=0,probes=0;
  page.on('pageerror',e=>errors.push(e.message));
  await context.addInitScript(s=>{localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));window.turnstile={render:(_el,o)=>{queueMicrotask(()=>o.callback('browser-test-token'));return 1;},reset:()=>{}};},setup());
  await page.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));await page.route('**/config.json',r=>r.fulfill({json:{diplomacyEndpoint:'https://worker.example/diplomacy',turnstileSiteKey:'public-test-key'}}));
  await page.route('https://worker.example/session',r=>r.fulfill({json:{token:'browser-test-session',expires:Date.now()+3600000}}));
  await page.route('https://worker.example/diplomacy',r=>{
   calls++;const body=r.request().postDataJSON(),probe=body.message.startsWith('Connection test.');if(probe){probes++;assert.deepEqual(body.world,{});assert.deepEqual(body.history,[]);assert.ok(r.request().postData().length<700);}
   return probe&&succeeds?r.fulfill({json:{reply:'PROBE_TEXT_MUST_NOT_ENTER_CHAT',tone:'neutral',intents:[]}}):r.fulfill({status:503,headers:{'Retry-After':'60'},json:{retryAfter:60,diagnostics:{version:1,code:'GEMINI_UNAVAILABLE',providerStatus:503,model:'gemini-3.5-flash',requestFormat:'pre-council-queue-v1',checks:{GEMINI_API_KEY:'present',TURNSTILE_SECRET:'present',BUDGET:'verified'}}}});
  });
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();await page.locator('[data-tab=council]').click();await page.locator('[data-talk=wintermere]').click();await page.waitForFunction(()=>document.getElementById('chat-notice').textContent.includes('Gemini ready'));
  await page.locator('#chat-message').fill('CAMPAIGN_CONTENT_CANARY');await page.locator('#send-chat').click();await (await revealChatAction(page,'#gemini-diagnostics')).waitFor({state:'visible'});
  const before=await page.evaluate(()=>localStorage.getItem('catnmice.iron-throne.v1'));await clickChatAction(page,'#gemini-diagnostics');assert.equal(calls,1,'opening diagnostics sends no request');
  const original=await page.locator('#diagnostics-report').inputValue();assert.match(original,/Game context bytes: \d+/);assert.match(original,/Retry after:/);assert.doesNotMatch(original,/CAMPAIGN_CONTENT_CANARY/);
  await page.locator('#test-gemini-connection').click();await page.waitForFunction(()=>/Minimal private test (?:succeeded|failed)/.test(document.getElementById('gemini-test-result').textContent));
  assert.equal(calls,2);assert.equal(probes,1);const text=await page.locator('#gemini-test-result').textContent();assert.match(text,succeeds?/succeeded/:/without campaign context/);
  const report=await page.locator('#diagnostics-report').inputValue();assert.ok(report.startsWith(original),'original failure remains intact');assert.match(report,/Manual connection test:/);assert.doesNotMatch(report,/PROBE_TEXT_MUST_NOT_ENTER_CHAT|CAMPAIGN_CONTENT_CANARY|browser-test-session/);
  assert.equal(await page.evaluate(()=>localStorage.getItem('catnmice.iron-throne.v1')),before,'probe does not edit or append to the campaign');
  if(!succeeds){await page.locator('#test-gemini-connection').click();await page.waitForFunction(()=>!document.getElementById('test-gemini-connection').disabled);assert.equal(calls,2,'early probe retry honors its own delay');}
  assert.deepEqual(errors,[]);console.log(`PASS ${viewport.width}px: minimal probe ${succeeds?'success':'503'}, safe report, no game mutation or automatic requests`);await context.close();
 }
}catch(error){console.error(error);throw error;}finally{await browser?.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
