// Functional layout checks only: no previews, screenshots or live provider calls.
import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {createGame} from './fixtures/legacy-game.mjs';
import {relation} from '../core.mjs';
import {refreshKnowledge} from '../fog.mjs';
import {ownCouncil,appendCouncil} from '../council-state.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright'),root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{try{const file=path.resolve(root,'.'+new URL(req.url,'http://localhost').pathname);if(!file.startsWith(root+path.sep))throw Error('path');res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(await readFile(file));}catch{res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;let browser;
function setup(){
 const s=createGame(311);s.presentation={reducedEffects:true};
 for(const h of ['wintermere','redharbor','thornwall']){s.treaties.push({id:`ally-${h}`,type:'alliance',parties:['ashen',h],expires:100});Object.assign(relation(s,h,'ashen'),{trust:95,opinion:95,reliability:95,grievance:0});}
 refreshKnowledge(s);s.conversations.wintermere=[];const c=ownCouncil(s,'ashen',true);
 for(let i=0;i<18;i++){const message=`Dispatch ${i+1}: Our scouts have returned from the border. We should discuss the roads and supplies before committing our armies to the next campaign.`;s.conversations.wintermere.push({role:'ruler',text:message,turn:1,source:'gemini'});appendCouncil(s,c,'wintermere',message,{source:'gemini'});}
 return s;
}
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
 for(const viewport of [{width:1280,height:850},{width:390,height:844},{width:844,height:430}])for(const mode of ['private','council']){
  const context=await browser.newContext({viewport,hasTouch:viewport.width<700}),page=await context.newPage(),errors=[];let calls=0;
  page.on('pageerror',e=>errors.push(e.message));
  await context.addInitScript(s=>{localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));window.turnstile={render:(el,o)=>{window.widgetOptions=o;window.completeVerification=()=>o.callback('test-token');el.innerHTML='<button type="button" id="test-verification">Verify for test</button>';el.querySelector('button').onclick=window.completeVerification;return 1;},reset:()=>{}};},setup());
  await page.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));await page.route('**/config.json',r=>r.fulfill({json:{diplomacyEndpoint:'https://worker.example/diplomacy',turnstileSiteKey:'public-test-key'}}));
  await page.route('https://worker.example/session',r=>r.fulfill({json:{token:'test-session',expires:Date.now()+3600000}}));
  await page.route('https://worker.example/diplomacy',r=>{calls++;const body=r.request().postDataJSON();return r.fulfill({json:body.mode==='allianceCouncil'?{responses:[{speakerHouseId:body.world.participants.find(p=>p.ai&&p.id!==body.actorHouseId).id,message:'Our scouts will report what they observe.'}]}:{reply:'Our scouts will report what they observe.',tone:'neutral',intents:[]}});});
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
  if(mode==='council')await page.locator('[data-alliance]').first().click();else{await page.locator('[data-tab=council]').click();await page.locator('[data-talk=wintermere]').click();}
  const dialog=page.locator(mode==='council'?'#alliance-council':'#diplomacy'),textarea=dialog.locator('textarea').first(),menu=dialog.locator('.chat-options'),summary=menu.locator(':scope > summary'),history=dialog.locator(mode==='council'?'.alliance-history':'.correspondence-scroll');
  assert.equal(await textarea.evaluate(el=>el===document.activeElement),false,'opening chat does not open the keyboard/composer');
  assert.equal(await textarea.evaluate(el=>Math.round(el.getBoundingClientRect().height)),40);
  await page.locator('#test-verification').waitFor({state:'visible'});assert.equal(await page.evaluate(()=>window.widgetOptions.appearance),'interaction-only');
  await page.locator('#test-verification').click();await page.waitForFunction(()=>document.getElementById('turnstile').hidden);
  if(mode==='council')await page.waitForFunction(()=>document.querySelector('.alliance-connection-status').textContent.includes('Connected'));
  const room=await history.boundingBox(),bounds=await dialog.boundingBox();assert.ok(room.height/bounds.height>(viewport.height<500?.35:.55),`history has ${room.height}/${bounds.height}px`);
  assert.equal(await page.locator(mode==='council'?'.alliance-map':'#quick-offer').isVisible(),false);
  await textarea.click();const expanded=await textarea.boundingBox();assert.ok(expanded.height>=(viewport.height<500?80:88));
  await textarea.fill('Unsent draft\nSecond line of the draft');await history.click({position:{x:10,y:10}});
  assert.equal(await textarea.evaluate(el=>Math.round(el.getBoundingClientRect().height)),40);assert.equal(await textarea.inputValue(),'Unsent draft\nSecond line of the draft');
  await summary.click();assert.equal(await menu.evaluate(el=>el.open),true);
  const panel=await menu.locator('.chat-options-panel').boundingBox();assert.ok(panel.y>=bounds.y&&panel.y+panel.height<=bounds.y+bounds.height,'options stay inside chat viewport');
  if(mode==='private'){
   for(const id of ['quick-offer','quick-request','quick-promises','court-marriage','court-intelligence','expand-council','use-gemini'])assert.equal(await menu.locator(`#${id}`).isVisible(),true,id);
   await page.locator('#quick-promises').click();await page.locator('#treaty-drawer').waitFor({state:'visible'});assert.equal(await menu.evaluate(el=>el.open),false);assert.equal(await page.locator('#council-records').evaluate(el=>el.open),true);
   await page.locator('.treaty-drawer-close').click();assert.equal(await summary.evaluate(el=>document.activeElement===el),true,'drawer returns focus to visible menu trigger');
   await summary.click();await page.locator('#court-intelligence').click();assert.equal(await textarea.inputValue(),'Unsent draft\nSecond line of the draft','shortcut preserves existing draft');assert.ok((await textarea.boundingBox()).height>=(viewport.height<500?80:88));
  }else{
   for(const selector of ['.alliance-action','.alliance-map','.alliance-gemini','[data-alliance-terms]'])assert.equal(await menu.locator(selector).first().isVisible(),true,selector);
   await menu.press('Escape');assert.equal(await menu.evaluate(el=>el.open),false);assert.equal(await dialog.evaluate(el=>el.open),true,'Escape closes only options');
  }
  await summary.click();if(!await menu.evaluate(el=>el.open))await summary.click();
  await page.locator(mode==='council'?'.alliance-offer-request':'#private-offer-request').click();await page.locator('#formal-proposal-builder').waitFor({state:'visible'});assert.equal(await menu.evaluate(el=>el.open),false);
  await page.locator('#formal-proposal-builder [data-formal-close]').first().click();assert.equal(await summary.evaluate(el=>document.activeElement===el),true,'proposal dialog returns focus to the menu trigger');assert.equal(await textarea.inputValue(),'Unsent draft\nSecond line of the draft');
  await history.evaluate(el=>{el.scrollTop=60;});const top=await history.evaluate(el=>el.scrollTop);await summary.click();await summary.click();assert.equal(await history.evaluate(el=>el.scrollTop),top,'menu does not jump history');
  await textarea.fill('Report what your scouts have observed.');await dialog.locator('.chat-composer button[type=submit]').click();await page.waitForFunction(mode=>mode==='private'?!document.getElementById('send-chat').disabled:!document.querySelector('.alliance-compose button[type=submit]').disabled,mode);
  assert.ok(calls>0);assert.equal(await textarea.inputValue(),'');assert.equal(await textarea.evaluate(el=>Math.round(el.getBoundingClientRect().height)),40);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.deepEqual(errors,[]);
  console.log(`PASS ${viewport.width}px ${mode}: ${Math.round(room.height)}px history, compact composer/menu, verification, drafts and sending`);await context.close();
 }
}finally{await browser?.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
