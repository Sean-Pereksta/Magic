// Functional Council/private interaction checks; no HTML previews or screenshots.
import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {createGame} from './fixtures/legacy-game.mjs';
import {relation,declareWar} from '../core.mjs';
import {refreshKnowledge} from '../fog.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright'),root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{try{const file=path.resolve(root,'.'+new URL(req.url,'http://localhost').pathname);if(!file.startsWith(root+path.sep))throw Error('path');res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(await readFile(file));}catch{res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;let browser;
function setup(){
 const s=createGame(311);s.presentation={reducedEffects:true};
 for(const h of ['wintermere','redharbor','thornwall']){s.treaties.push({id:`ally-${h}`,type:'alliance',parties:['ashen',h],expires:100});Object.assign(relation(s,h,'ashen'),{trust:95,opinion:95,reliability:95,grievance:0});}
 declareWar(s,'ashen','sunspire');declareWar(s,'wintermere','sunspire');declareWar(s,'redharbor','sunspire');
 for(const h of ['wintermere','redharbor'])for(const [a,b] of [[h,'sunspire'],['sunspire',h]])Object.assign(relation(s,a,b),{trust:95,opinion:95,reliability:95,grievance:0,aggression:0,wariness:0});
 refreshKnowledge(s);return s;
}
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
 for(const viewport of [{width:1280,height:850},{width:390,height:844}])for(const mode of ['council','private'])for(const entry of ['builder','chat']){
  const s=setup(),context=await browser.newContext({viewport,hasTouch:viewport.width<700}),page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await context.addInitScript(s=>{if(!localStorage.getItem('catnmice.iron-throne.v1'))localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));},s);
  await page.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));await page.route('**/config.json',r=>r.fulfill({json:{}}));
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
  const saved=()=>page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1'))),builder=page.locator('#formal-proposal-builder');
  if(mode==='council')await page.locator('[data-alliance]').first().click();else{await page.locator('[data-tab=council]').click();await page.locator('[data-talk=wintermere]').click();}
  if(entry==='builder'){
   await page.locator(mode==='council'?'.alliance-offer-request':'#private-offer-request').click();
   await builder.locator('[name=type]').selectOption({label:'Make peace with a faction'});
   assert.equal(await builder.locator('[name=direction]').inputValue(),'request');assert.equal(await builder.locator('[name=direction]').isDisabled(),true);assert.equal(await builder.locator('.formal-resources').isHidden(),true);
   if(mode==='council'){
    await builder.locator('[name=target]').selectOption('redharbor');assert.equal(await builder.locator('[name=house][value=redharbor]').isDisabled(),true);assert.equal(await builder.locator('[name=house][value=redharbor]').isChecked(),false);
   }else assert.equal(await builder.locator('[name=target] option[value=wintermere]').count(),0);
   await builder.locator('[name=target]').selectOption('sunspire');
   for(const h of ['redharbor','thornwall'])if(mode==='council')await builder.locator(`[name=house][value=${h}]`).uncheck();
   await builder.locator('[name=house][value=wintermere]').check();await builder.locator('[name=duration]').fill('8');await builder.locator('[type=submit]').click();
  }else{
   await page.locator(mode==='council'?'#alliance-message':'#chat-message').fill('Wintermere, please make peace with House Sunspire for eight turns.');
   await page.locator(mode==='council'?'.alliance-compose [type=submit]':'#chat-form [type=submit]').click();
   const ratify=page.locator('[data-formal-ratify]');await ratify.waitFor();let before=await saved();assert.equal(before.cooperation.formalProposals[0].approved,false);assert.ok(before.wars.includes('sunspire:wintermere'));
   if(mode==='council')await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).allianceCouncils[0].activeSequence.status!=='pending');
   await page.locator('[data-formal-modify]').click();assert.equal(await builder.locator('[name=type]').inputValue(),'MAKE_PEACE');assert.equal(await builder.locator('[name=target]').inputValue(),'sunspire');await builder.locator('[data-formal-close]').first().click();
   await ratify.click();
  }
  await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).cooperation.formalProposals?.[0]?.responses.wintermere?.status==='accepted');
  const after=await saved(),p=after.cooperation.formalProposals[0];assert.equal(p.direction,'request');assert.equal(p.intent.type,'PEACE');assert.equal(p.intent.targetId,'sunspire');assert.equal(p.intent.duration,8);assert.deepEqual(p.requestedHouses,['wintermere']);
  assert.equal(after.wars.includes('sunspire:wintermere'),false);assert.ok(after.wars.includes('ashen:sunspire'));assert.ok(after.wars.includes('redharbor:sunspire'));
  assert.equal(after.treaties.find(t=>t.type==='peace'&&t.parties.includes('wintermere')&&t.parties.includes('sunspire')).expires,after.turn+8);
  assert.deepEqual(after.kingdoms.map(k=>k.resources),s.kingdoms.map(k=>k.resources));
  assert.match(await page.locator(`[data-formal-card="${p.id}"]`).textContent(),/Accepted · Active/);
  await page.reload();await page.locator('#resume').click();assert.equal((await saved()).wars.includes('sunspire:wintermere'),false);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.deepEqual(errors,[]);
  console.log(`PASS ${viewport.width}px ${mode}: ${entry}, target/recipient controls, peace treaty and save/reload`);await context.close();
 }
}finally{await browser?.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
