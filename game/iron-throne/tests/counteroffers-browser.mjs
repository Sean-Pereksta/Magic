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
import {ownCouncil} from '../council-state.mjs';
import {submitFormalProposal,resolveFormalResponse,recordFormalVoice} from '../formal-proposals.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright'),root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const server=http.createServer(async(req,res)=>{try{const file=path.resolve(root,'.'+new URL(req.url,'http://localhost').pathname);if(!file.startsWith(root+path.sep))throw Error('path');res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(await readFile(file));}catch{res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;let browser;
function setup(){const s=createGame(311);for(const h of ['wintermere','redharbor','thornwall']){s.treaties.push({id:`ally-${h}`,type:'alliance',parties:['ashen',h],expires:100});Object.assign(relation(s,h,'ashen'),{trust:95,opinion:95,reliability:95,grievance:0});}for(const k of s.kingdoms)k.resources.food=k.resources.iron=k.resources.gold=500;refreshKnowledge(s);return s;}
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
 for(const viewport of [{width:1280,height:850},{width:390,height:844}])for(const mode of ['council','private'])for(const decision of ['accept','decline','modify']){
  const s=setup();for(const k of s.kingdoms)for(const key of Object.keys(k.resources))k.resources[key]=500;
  const c=ownCouncil(s,'ashen',true),raw={requestedHouses:['wintermere'],direction:'request',...(mode==='council'?{councilId:c.id}:{}),intent:{type:'EXCHANGE',giveItems:[{resource:'food',amount:50},{resource:'iron',amount:10},{resource:'wood',amount:10},{resource:'stone',amount:10}],receiveItems:[{resource:'gold',amount:1},{resource:'horses',amount:1}]}};
  const sent=submitFormalProposal(s,'ashen',raw);assert.equal(sent.ok,true,sent.error);
  resolveFormalResponse(s,'ashen',sent.proposalId,'wintermere');recordFormalVoice(s,'ashen',sent.proposalId,'wintermere','','failed');
  const original=s.cooperation.formalProposals[0],counter=structuredClone(original.responses.wintermere.counterIntent);assert.equal(original.responses.wintermere.status,'counter');assert.ok(counter.giveItems.length>3);assert.ok(counter.receiveItems.length>1);
  if(mode==='council'){
   const newer=submitFormalProposal(s,'ashen',{councilId:c.id,requestedHouses:['thornwall'],direction:'offer',intent:{type:'AID',giveResource:'gold',giveAmount:1}});assert.equal(newer.ok,true,newer.error);
   resolveFormalResponse(s,'ashen',newer.proposalId,'thornwall');recordFormalVoice(s,'ashen',newer.proposalId,'thornwall','','failed');
  }
  const context=await browser.newContext({viewport,hasTouch:viewport.width<700}),page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await context.addInitScript(s=>{if(!localStorage.getItem('catnmice.iron-throne.v1'))localStorage.setItem('catnmice.iron-throne.v1',JSON.stringify(s));},s);
  await page.route('https://pub-*.r2.dev/**',r=>r.fulfill({status:404,body:''}));await page.route('**/config.json',r=>r.fulfill({json:{}}));
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
  if(mode==='council')await page.locator('[data-alliance]').first().click();else {await page.locator('[data-tab=council]').click();await page.locator('[data-talk=wintermere]').click();}
  const card=page.locator(`[data-formal-card="${original.id}"]`),saved=()=>page.evaluate(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')));
  for(const label of ['Accept Counteroffer','Decline','Modify / New Offer'])await card.getByRole('button',{name:label,exact:true}).waitFor({state:'visible'});
  if(mode==='council'){assert.equal(await page.locator('.formal-tracker').count(),1);assert.equal(await page.locator('.alliance-pending-offers [data-formal-card]').count(),1,'older unanswered counter remains accessible');}
  const resources=JSON.stringify((await saved()).kingdoms.map(k=>k.resources));
  if(decision==='modify'){
   await card.getByRole('button',{name:'Modify / New Offer',exact:true}).click();const builder=page.locator('#formal-proposal-builder');
   assert.equal(await builder.locator('[name=house]').count(),1);assert.equal(await builder.locator('[name=house]').inputValue(),'wintermere');assert.equal(await builder.locator('[name=direction]').inputValue(),'request');
   for(const [n,item] of counter.giveItems.entries()){assert.equal(await builder.locator(`[name=resource${n}]`).inputValue(),item.resource);assert.equal(await builder.locator(`[name=amount${n}]`).inputValue(),String(item.amount));}
   for(const [n,item] of counter.receiveItems.entries()){assert.equal(await builder.locator(`[name=receiveResource${n||''}]`).inputValue(),item.resource);assert.equal(await builder.locator(`[name=receiveAmount${n||''}]`).inputValue(),String(item.amount));}
   await builder.locator('[data-formal-close]').first().click();assert.equal((await saved()).cooperation.formalProposals[0].responses.wintermere.offerAnswered,undefined,'cancelling modification leaves counter open');
   await card.getByRole('button',{name:'Modify / New Offer',exact:true}).click();await builder.locator('[name=amount0]').fill(String(counter.giveItems[0].amount-1));await builder.locator('[type=submit]').click();
   await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).cooperation.formalProposals[0].responses.wintermere.offerAnswered==='modified');
   const next=(await saved()).cooperation.formalProposals.at(-1);assert.deepEqual(next.replyTo,{proposalId:original.id,house:'wintermere'});assert.deepEqual(next.requestedHouses,['wintermere']);assert.equal(next.direction,'request');assert.equal(next.intent.giveItems[0].amount,counter.giveItems[0].amount-1);assert.deepEqual(next.intent.giveItems.slice(1),counter.giveItems.slice(1));assert.deepEqual(next.intent.receiveItems,counter.receiveItems);
  }else {
   await card.locator(`[data-decision=${decision}]`).click();const after=await saved();assert.equal(after.cooperation.formalProposals[0].responses.wintermere.offerAnswered,decision==='accept'?'accepted':'declined');
   if(decision==='decline')assert.equal(JSON.stringify(after.kingdoms.map(k=>k.resources)),resources);else assert.notEqual(JSON.stringify(after.kingdoms.map(k=>k.resources)),resources);
  }
  assert.equal(await page.locator(`[data-formal-answer="${original.id}"]`).count(),0,'answered original cannot transfer resources again');
  await page.reload();await page.locator('#resume').click();assert.equal((await saved()).cooperation.formalProposals[0].responses.wintermere.offerAnswered,decision==='modify'?'modified':decision==='accept'?'accepted':'declined');
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.deepEqual(errors,[]);
  console.log(`PASS ${viewport.width}px ${mode}: ${decision}, retained counter controls, exact package and save/reload`);await context.close();
 }
}catch(error){console.error(error);throw error;}finally{await browser?.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
