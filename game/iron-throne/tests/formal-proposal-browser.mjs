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
 for(const viewport of [{width:1280,height:850},{width:390,height:844}])for(const mode of ['council','private']){
  console.log(`Start ${viewport.width}px ${mode}`);const s=setup();if(mode==='council'){s.treaties.push({id:'protect',type:'non-aggression',parties:['redharbor','sunspire'],expires:100});s.pledges.push({id:'test-duty',debtor:'thornwall',creditor:'ashen',intent:validateIntent({type:'POSITION',targetId:'5,6'}),created:1,deadline:10,status:'pending',held:0});}
  const context=await browser.newContext({viewport,hasTouch:viewport.width<700}),page=await context.newPage(),errors=[];
  page.on('pageerror',e=>{errors.push(e.message);console.error(e.message);});
  await context.addInitScript(s=>{if(!localStorage.getItem('catnmice.iron-throne.v1'))localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));},s);
  await page.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));await page.route('**/config.json',r=>r.fulfill({json:{}}));
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
  const saved=()=>page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1'))),builder=page.locator('#formal-proposal-builder');
  if(mode==='council'){
   await page.locator('[data-alliance]').first().click();await page.locator('.alliance-offer-request').click();
   await builder.locator('[name=type]').selectOption('JOINT_WAR');await builder.locator('[name=target]').selectOption('sunspire');await builder.locator('[name=duration]').fill('10');await builder.locator('[type=submit]').click();
   await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).cooperation.formalProposals?.[0]?.status==='resolved');
   let state=await saved(),p=state.cooperation.formalProposals[0];assert.equal(p.source,'explicit');assert.equal(p.responses.wintermere.status,'accepted');assert.equal(p.responses.redharbor.status,'alternative');assert.equal(p.responses.thornwall.status,'alternative');
   assert.ok(state.wars.includes('sunspire:wintermere'));assert.equal(await page.locator('[data-formal-ratify]').count(),0);
   assert.match(await page.locator('.alliance-formal-proposals').textContent(),/Accepted · Active/);
   const food=state.kingdoms[0].resources.food;await page.locator('[data-formal-answer][data-house=redharbor][data-decision=accept]').click();state=await saved();assert.ok(state.kingdoms[0].resources.food>food);assert.equal(state.cooperation.formalProposals[0].responses.redharbor.offerAnswered,'accepted');
   await page.locator('#alliance-message').fill('Meet me at 5,6 within five turns.');await page.locator('.alliance-compose [type=submit]').click();await page.locator('[data-formal-ratify]').waitFor();
   const before=(await saved()).pledges.length;await page.locator('[data-formal-dismiss]').click();assert.equal((await saved()).pledges.length,before);
   await page.locator('#alliance-council .close').click();await page.locator('[data-alliance]').first().click();assert.match(await page.locator('.alliance-formal-proposals').textContent(),/counteroffer accepted/);
  }else{
   await page.locator('[data-tab=council]').click();await page.locator('[data-talk=wintermere]').click();await page.locator('#private-offer-request').click();
   await builder.locator('[name=type]').selectOption('AID');await builder.locator('[name=direction]').selectOption('offer');await builder.locator('[name=resource0]').selectOption('iron');await builder.locator('[name=amount0]').fill('20');await builder.locator('[type=submit]').click();
   await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).cooperation.formalProposals?.[0]?.status==='resolved');
   let state=await saved();assert.equal(state.kingdoms[0].resources.iron,480);assert.equal(await page.locator('[data-formal-ratify]').count(),0);
   await page.locator('#chat-message').fill('I want you to attack Solstice within ten turns.');await page.locator('#send-chat').click();await page.locator('[data-formal-ratify]').waitFor();
   state=await saved();assert.equal(state.pledges.length,0);assert.equal(state.cooperation.formalProposals.at(-1).source,'conversation_inferred');
   await page.locator('[data-formal-modify]').click();await builder.locator('[name=duration]').fill('5');await builder.locator('[type=submit]').click();
   await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).pledges.some(p=>p.intent.type==='PLEDGE_ATTACK'));
   state=await saved();assert.equal(state.pledges[0].intent.targetId,'32,21');assert.equal(state.pledges[0].deadline,6);assert.equal(state.pledges[0].debtor,'wintermere');assert.equal(await page.locator('[data-formal-ratify]').count(),0);
  }
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.deepEqual(errors,[]);
  console.log(`PASS ${viewport.width}px ${mode}: formal decisions, active commitments, support or modified draft, no redundant confirmation`);await context.close();
 }
}catch(error){console.error(error);throw error;}finally{await browser?.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
