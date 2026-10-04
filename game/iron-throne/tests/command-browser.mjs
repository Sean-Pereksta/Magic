// Functional desktop/mobile checks. No screenshots, generated HTML previews or live model calls.
import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {createRequire} from 'node:module';
import {createGame} from './fixtures/legacy-game.mjs';
import {refreshGeneralCandidates} from '../generals.mjs';
import {refreshKnowledge} from '../fog.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright'),root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{
 try{const url=new URL(req.url,'http://localhost'),file=path.resolve(root,'.'+url.pathname);if(!file.startsWith(root+path.sep))throw Error('path');const body=await readFile(file);res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);}catch{res.writeHead(404);res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;
let browser;
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
 for(const viewport of [{width:1280,height:850},{width:390,height:844}]){
  const s=createGame();s.turn=8;s.commanders.nextOffer.ashen=8;refreshGeneralCandidates(s);
  const g=s.commanders.candidates.find(g=>g.owner==='ashen');g.quality=1;g.specialty='movement';
  g.history.push({turn:s.turn,role:'general',text:'LEGACY SCRIPTED GENERAL GREETING'});
  const army=s.armies[0],target=Object.values(s.tiles).find(t=>t.owner==='ashen'&&t.id!==army.tile&&!['water','mountain'].includes(t.terrain));
  s.treaties.push({id:'vassal-test',type:'vassalage',parties:['ashen','wintermere'],liege:'ashen',vassal:'wintermere',expires:100});refreshKnowledge(s);
  const context=await browser.newContext({viewport,hasTouch:viewport.width<700});
  await context.addInitScript(s=>localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s)),s);
  const page=await context.newPage(),errors=[];let sessions=0,models=0,failNextGeneral=false;
  page.on('pageerror',error=>{errors.push(error.message);console.error(error.message);});page.on('dialog',dialog=>void dialog.accept());
  page.on('requestfailed',r=>{if(r.url().startsWith(base))console.error('Request failed',r.url(),r.failure());});
  page.on('response',r=>{if(r.url().startsWith(base)&&r.status()>=400)console.error('HTTP',r.status(),r.url());});
  await context.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
  await context.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{diplomacyEndpoint:'https://proxy.example/diplomacy',turnstileSiteKey:'test-key'}}));
  await context.route('https://challenges.cloudflare.com/**',route=>route.fulfill({contentType:'text/javascript',body:"window.turnstile={render(node,options){node.textContent='Verification ready';setTimeout(()=>options.callback('verified-test-token'),0);return 'widget';},reset(){}};"}));
  await context.route('https://proxy.example/**',async route=>{
   const headers={'Access-Control-Allow-Origin':base,'Access-Control-Allow-Headers':'authorization,content-type'};
   if(route.request().method()==='OPTIONS'){await route.fulfill({status:204,headers});return;}
   if(route.request().url().endsWith('/session')){sessions++;await route.fulfill({headers,json:{token:'signed-test-session',expires:Date.now()+3600000}});return;}
   models++;const body=route.request().postDataJSON();assert.equal(body.mode,'general');assert.equal(body.generalId,g.id);
   if(failNextGeneral){failNextGeneral=false;await route.fulfill({status:503,headers,json:{diagnostics:{version:1,code:'GEMINI_TIMEOUT',checks:{GEMINI_API_KEY:'present',TURNSTILE_SECRET:'verified',BUDGET:'verified'}}}});return;}
   await route.fulfill({headers,json:{reply:'I propose gathering at the designated location. These are proposed orders, awaiting your approval.',order:{kind:'rally',targets:[target.id],lossLimit:35,allowSplit:false}}});
  });
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click().catch(async error=>{console.error(await page.locator('#load-warning').textContent());throw error;});
  await page.locator('.general-notice summary').click();assert.match(await page.locator('.general-notice').textContent(),/125 gold.*1 gold per round/s);
  await page.locator('[data-general-hire]').click();
  const saved=()=>page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
  assert.equal((await saved()).kingdoms[0].resources.gold,s.kingdoms[0].resources.gold-125);
  await page.locator('[data-tab="realm"]').click();assert.match(await page.locator('.vassal-group').textContent(),/Your Vassals.*Wintermere/s);
  await page.locator(`[data-army="${army.id}"]`).click();await page.locator('[data-general-assign]').click();
  await page.locator('[data-general-open]:not([data-show-orders])').click();
  await page.waitForFunction(()=>document.getElementById('chat-notice').textContent.includes('session is active'));
  assert.equal(sessions,1);assert.equal(models,0,'opening a commander verifies but never requests model narration');
  assert.equal(await page.locator('.commander-sigil').getAttribute('aria-label'),'Commander');
  assert.doesNotMatch(await page.locator('.general-history').textContent(),/LEGACY SCRIPTED GENERAL GREETING|I am ready for an army/);
  assert.equal(await page.locator('#general-orders').evaluate(el=>el.scrollWidth<=el.clientWidth+1),true);
  await page.locator('#general-chat-form textarea').fill(`Rally at ${target.id}`);await page.locator('#general-chat-form button').click();
  await page.locator('[data-general-approve]').waitFor();assert.equal(models,1);
  let state=await saved();assert.equal(state.armies[0].path.length,0);assert.deepEqual(state.diplomacy.messages,s.diplomacy.messages);
  await page.locator('[data-general-approve]').click();state=await saved();assert.equal(state.armies[0].target,target.id);
  await page.locator('[data-general-close]').click();await page.locator('#chat-form #use-gemini').waitFor({state:'attached'});
  await page.locator('[data-hold]').click();state=await saved();assert.equal(state.armies[0].playerOverride,'8:ashen');assert.equal(state.armies[0].order,'hold');
  await page.locator('[data-general-open]:not([data-show-orders])').click();
  const beforeFailure=(await saved()).commanders.roster[0].history;failNextGeneral=true;
  await page.locator('#general-chat-form textarea').fill('Why have you stopped?');await page.locator('#general-chat-form button').click();
  await page.waitForFunction(()=>document.querySelector('.general-chat-notice').textContent.includes('No Gemini response'));
  assert.equal(models,2);assert.equal(await page.locator('#general-chat-form textarea').inputValue(),'Why have you stopped?');
  assert.deepEqual((await saved()).commanders.roster[0].history,beforeFailure,'a failed request never inserts local or empty speech');
  assert.doesNotMatch(await page.locator('.general-history').textContent(),/Your manual orders remain authoritative/);
  await page.locator('#general-chat-form button').click();await page.locator('[data-general-approve]').waitFor();
  assert.equal(models,3);assert.equal((await saved()).commanders.roster[0].history.at(-1).source,'gemini');
  assert.equal(await page.locator('#general-chat-form textarea').inputValue(),'');assert.equal((await saved()).armies[0].order,'hold');
  await page.locator('[data-general-dismiss-draft]').click();
  if(!await page.locator('#general-orders').evaluate(el=>el.classList.contains('show-orders')))await page.locator('[data-toggle-orders]').click();
  await page.locator('#general-order-form [name=kind]').selectOption('move');await page.locator('#general-order-form [name=targets]').selectOption(target.id);
  await page.locator('#general-order-form [type=submit]').click();state=await saved();assert.equal(state.armies[0].target,target.id);assert.equal(state.commanders.roster[0].objective.source,'explicit');assert.equal(await page.locator('[data-general-approve]').count(),0);
  await page.locator('#general-chat-form textarea').fill(`Hold ${army.tile}`);await page.locator('#general-chat-form button').click();await page.locator('.interpreted-order').waitFor();assert.equal((await saved()).armies[0].target,target.id);
  await page.locator('[data-general-dismiss-draft]').click();assert.equal((await saved()).armies[0].target,target.id);
  await page.locator('[data-general-close]').click();await page.locator('[data-tab="realm"]').click();
  const before=(await saved()).armies.map(a=>a.units);await page.locator('[data-general-dismiss]').click();state=await saved();
  assert.equal(state.commanders.roster.length,0);assert.deepEqual(state.armies.map(a=>a.units),before);assert.equal(state.armies[0].commandId,undefined);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.deepEqual(errors,[]);
  console.log(`PASS ${viewport.width}px: candidate costs, vassal grouping, own-general verification/session, proposed orders, approval, Gemini-only failure/retry, hidden legacy speech, manual override and dismissal`);
  await context.close();
 }
}finally{await browser?.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
