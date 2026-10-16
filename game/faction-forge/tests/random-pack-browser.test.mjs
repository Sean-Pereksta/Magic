import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require('playwright');
const {ALL_TROOPS}=require('../../../server/faction-forge/domain.cjs');
const root=path.resolve(import.meta.dirname,'..');

test('random store pack previews its unlocked pool and reveals the actual transaction cards after one purchase',async()=>{
 const server=http.createServer(async(req,res)=>{
  try{const requested=new URL(req.url,'http://localhost').pathname;
   if(requested==='/'){res.setHeader('Content-Type','text/html');res.end('<!doctype html><meta name="viewport" content="width=device-width"><link rel="stylesheet" href="/expansion.css"><body></body>');return;}
   const file=path.resolve(root,'.'+requested);if(!file.startsWith(root+path.sep))throw Error('path');
   res.setHeader('Content-Type',file.endsWith('.css')?'text/css':'text/javascript');res.end(await fs.readFile(file));
  }catch{res.writeHead(404);res.end('missing');}
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 let browser;
 try{
  browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
  const page=await browser.newPage({viewport:{width:390,height:844}}),errors=[];page.on('pageerror',error=>errors.push(error.message));
  await page.goto('http://127.0.0.1:'+server.address().port);
  const initialize=async keys=>{
   const [{createExpansionUI},{newSoloProfile,createFirebaseProgress}]=await Promise.all([import('/ui.js'),import('/firebase-progress.js')]);
   const troops=Object.fromEntries(keys.map((key,index)=>[key,{name:key,faction:index<9?'Vanir':index<17?'Aesir':'Helions',unlockable:index>=26}]));
   const initial=newSoloProfile('owner','Commander',troops);initial.forgeDust=50;
   const docs=new Map(JSON.parse(localStorage.getItem('random-pack.docs')||'null')||[['factionForgeSoloProfiles/owner',initial]]);window.docs=docs;let serial=Promise.resolve();
   const api={doc:(_db,...parts)=>parts.join('/'),onSnapshot:()=>()=>{},runTransaction:(_db,fn)=>{
    const operation=serial.then(async()=>{const writes=[];const value=await fn({get:async ref=>({exists:()=>docs.has(ref),data:()=>structuredClone(docs.get(ref))}),set:(ref,data)=>writes.push([ref,structuredClone(data)])});for(const [ref,data] of writes)docs.set(ref,data);localStorage.setItem('random-pack.docs',JSON.stringify([...docs]));if(window.failAfterCommit){window.failAfterCommit=false;throw Object.assign(Error('Connection lost after saving'),{code:'unavailable'});}return value;});serial=operation.catch(()=>{});return operation;
   }};
   window.progress=createFirebaseProgress({auth:{currentUser:{uid:'owner'}},db:{},api,username:'Commander',troops,storage:localStorage});
   await progress.loadProfile();window.active=false;window.purchases=[];
   window.ui=createExpansionUI({assets:{enabled:()=>false},troops,getProfile:()=>progress.profile,getPendingPackRequest:id=>progress.getPendingPackRequest(id),isActiveMatch:()=>window.active,onBuyPack:async(id,options)=>{const result=await progress.buyPack(id,options);purchases.push(result.transaction);return result;}});
   ui.showForge('rifleman','packs');
  };
  await page.evaluate(initialize,ALL_TROOPS);
  const card=page.locator('[data-pack-id="random-reinforcements"]');
  assert.match(await card.textContent(),/50 Forge Dust · 15 cards/);
  assert.match(await card.locator('.ff-pack-contents').textContent(),/3 random unlocked troop types × 5 cards/);
  assert.match(await card.textContent(),/all 26 unlocked troop types across every faction/);
  assert.match(await card.textContent(),/Contents revealed after opening/);
  assert.equal(await page.locator('.ff-pack-receipt').count(),0);
  await card.getByRole('button',{name:'Buy Random Reinforcements',exact:true}).click();
  await page.waitForFunction(()=>purchases.length===1&&progress.profile.forgeDust===0);
  const receipt=await page.evaluate(()=>purchases[0]);
  assert.equal(receipt.cards.length,3);assert.equal(new Set(receipt.cards.map(reward=>reward.troopKey)).size,3);
  assert.equal(receipt.cards.reduce((total,reward)=>total+reward.quantity,0),15);
  const revealed=await page.locator('.ff-pack-receipt').textContent();
  for(const reward of receipt.cards)assert.ok(revealed.includes(`5 ${reward.troopKey} cards`));
  assert.equal(await card.getByRole('button',{name:'Buy Random Reinforcements',exact:true}).isDisabled(),true);
  assert.equal(await page.evaluate(()=>Object.values(progress.profile.troops).reduce((total,troop)=>total+troop.cards,0)),15);
  assert.ok(await page.evaluate(()=>document.querySelector('.ff-expansion').scrollWidth<=innerWidth));
  // A committed purchase whose reply is lost spends the last 50 Dust. Its retry
  // must remain actionable at zero Dust after the entire page has reloaded.
  await page.evaluate(async()=>{docs.get('factionForgeSoloProfiles/owner').forgeDust=50;await progress.loadProfile();window.failAfterCommit=true;ui.showForge('rifleman','packs');});
  await card.getByRole('button',{name:'Buy Random Reinforcements',exact:true}).click();
  await page.waitForFunction(()=>progress.getPendingPackRequest('random-reinforcements')&&docs.get('factionForgeSoloProfiles/owner').forgeDust===0);
  const committed=await page.evaluate(()=>docs.get('factionForgeSoloProfiles/owner/purchases/'+progress.getPendingPackRequest('random-reinforcements')));
  await page.reload();await page.evaluate(initialize,ALL_TROOPS);
  assert.equal(await page.evaluate(()=>progress.profile.forgeDust),0);
  const finish=card.getByRole('button',{name:'Finish opening Random Reinforcements',exact:true});
  assert.equal(await finish.isDisabled(),false);await finish.click();
  await page.waitForFunction(()=>purchases.length===1&&!progress.getPendingPackRequest('random-reinforcements'));
  assert.deepEqual(await page.evaluate(()=>purchases[0]),committed);
  assert.equal(await page.evaluate(()=>progress.profile.forgeDust),0);
  assert.equal(await page.evaluate(()=>Object.values(progress.profile.troops).reduce((total,troop)=>total+troop.cards,0)),30);
  assert.equal(await card.getByRole('button',{name:'Buy Random Reinforcements',exact:true}).isDisabled(),true);
  await page.evaluate(()=>{window.active=true;ui.showForge('rifleman','packs');});
  assert.equal(await card.getByRole('button',{name:'Buy Random Reinforcements',exact:true}).isDisabled(),true);
  assert.match(await page.locator('.ff-match-lock').textContent(),/after the match/);assert.deepEqual(errors,[]);
 }finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
});
