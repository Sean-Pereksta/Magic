import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {resolve,extname} from 'node:path';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
let chromium;try{({chromium}=require('playwright'));}catch{}
const directory=resolve(fileURLToPath(new URL('..',import.meta.url)));

test('deck library and builder save, swap, enforce unlocks, and select five troops on desktop and mobile',{skip:!chromium},async()=>{
  const server=createServer(async(req,res)=>{try{const path=resolve(directory,'.'+new URL(req.url,'http://localhost').pathname);if(!path.startsWith(directory))throw Error('bad path');res.setHeader('Content-Type',extname(path)==='.css'?'text/css':'text/javascript');res.end(await readFile(path));}catch{res.writeHead(404);res.end();}});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));let browser;
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
    const page=await browser.newPage({viewport:{width:1366,height:900}}),errors=[];page.on('pageerror',error=>errors.push(error.message));const base=`http://127.0.0.1:${server.address().port}`;
    await page.goto(base+'/decks.js');await page.setContent(`<link rel="stylesheet" href="${base}/expansion.css"><body></body>`);
    await page.evaluate(async base=>{
      const {createExpansionUI}=await import(base+'/ui.js'),{FACTION_TROOPS}=await import(base+'/decks.js');
      const names={vguard:'Vanir Guard',marksman:'Marksman',suppressor:'Suppressor',vrocket:'Rockets',medic:'Field Medic',guardian:'Guardian',phaser:'Phasers',storm:'Storm Seer',runebinder:'Rune Binder',wardmaker:'Ward Maker',hellspawn:'Hell Spawn',roach:'Roach',defiler:'Defiler',spitter:'Spitter',blood:'BloodBurst',railbreaker:'Railbreaker',signalhawk:'Signal Hawk'};
      const troops=Object.fromEntries(Object.entries(FACTION_TROOPS).flatMap(([faction,keys])=>keys.map(key=>[key,{name:names[key]||key,faction,role:['vguard','guardian','hellspawn','defiler'].includes(key)?'front':'back',unlockable:key==='railbreaker'}])));
      window.profile={uid:'deck-user',username:'Commander',troops:Object.fromEntries(Object.keys(troops).map(key=>[key,{unlocked:key!=='railbreaker',forgeTier:key==='marksman'?3:2,cards:0}])),decks:[],selectedDeckId:'starter-vanir'};window.saved=[];window.selected=[];window.online=[];window.active=false;
      const sheet=document.createElement('canvas');sheet.width=sheet.height=64;const ctx=sheet.getContext('2d');ctx.fillStyle='#95c3de';ctx.fillRect(10,5,44,56);
      window.ui=createExpansionUI({troops,assets:{enabled:()=>true,unit:async()=>({image:sheet,spec:{frameWidth:64,frameHeight:64,columns:1,animations:{idle:[0]}}}),asset:async()=>null},getProfile:()=>profile,isActiveMatch:()=>window.active,unlockDescriptions:{railbreaker:'Chapter 1 boss or 15 blueprints (3/15)'},onSaveDeck:async deck=>{saved.push(deck);profile.decks=profile.decks.filter(item=>item.id!==deck.id).concat(deck);return {profile};},onSelectDeck:async deck=>{selected.push(deck);profile.selectedDeckId=deck.id;return {profile};},onFindMatch:async deck=>{online.push(deck);return {ok:true};}});ui.showDecks();
    },base);
    assert.equal(await page.locator('.ff-deck-card').count(),3);assert.equal(await page.locator('.ff-deck-mini-troop').count(),15);await page.waitForFunction(()=>document.querySelectorAll('.ff-deck-mini-troop canvas').length===15);assert.equal(await page.locator('.ff-deck-current h2').textContent(),'Iron Vanguard');assert.equal(await page.locator('.ff-deck-current .ff-tier-3').count(),1);
    await page.getByRole('button',{name:'Duplicate Iron Vanguard',exact:true}).click();await page.getByLabel('Deck name',{exact:true}).fill('Precision Wing');
    assert.equal(await page.locator('.ff-deck-slot-filled').count(),5);assert.equal(await page.getByRole('button',{name:'Save deck',exact:true}).isEnabled(),true);assert.match(await page.locator('.ff-deck-builder-combos').textContent(),/Suppression Bombardment/);
    const locked=page.locator('[data-troop="railbreaker"]');assert.equal(await locked.isDisabled(),true);assert.match(await locked.textContent(),/Chapter 1 boss or 15 blueprints/);
    await page.locator('[data-slot="0"] .ff-deck-slot-pick').click();await page.locator('[data-troop="signalhawk"]').click();assert.match(await page.locator('[data-slot="0"]').textContent(),/Signal Hawk/);assert.match(await page.locator('.ff-deck-builder-combos').textContent(),/✓ Guided Barrage/);
    // Dragging an existing member swaps slots, preserving five different types.
    await page.locator('[data-slot="0"] .ff-deck-slot-pick').dragTo(page.locator('[data-slot="1"]'));
    assert.match(await page.locator('[data-slot="1"]').textContent(),/Signal Hawk/);assert.match(await page.locator('[data-slot="0"]').textContent(),/Marksman/);
    await page.getByRole('button',{name:'Save deck',exact:true}).click();await page.waitForFunction(()=>window.saved.length===1);const custom=await page.evaluate(()=>saved[0]);assert.equal(custom.name,'Precision Wing');assert.equal(custom.troops.length,5);assert.equal(new Set(custom.troops).size,5);assert.equal(custom.faction,'Vanir');assert.equal(await page.locator('.ff-deck-card').count(),2,'library stays filtered to saved faction');
    await page.getByRole('button',{name:'Select Precision Wing',exact:true}).click();await page.waitForFunction(()=>selected.length===1);assert.equal(await page.locator('.ff-deck-current h2').textContent(),'Precision Wing');
    await page.getByRole('button',{name:'Edit Precision Wing',exact:true}).click();await page.getByRole('button',{name:'Remove Marksman from deck',exact:true}).click();assert.equal(await page.getByRole('button',{name:'Save deck',exact:true}).isDisabled(),true);await page.locator('[data-troop="vguard"]').click();await page.getByRole('button',{name:'Save deck',exact:true}).click();await page.waitForFunction(()=>saved.length===2);assert.equal(await page.evaluate(()=>profile.decks.length),1,'editing replaces the custom deck instead of duplicating it');
    await page.evaluate(()=>{profile.decks[0]={...profile.decks[0],name:'Updated on another device'};ui.updateProfile();});assert.equal(await page.locator('.ff-deck-card h2').filter({hasText:'Updated on another device'}).count(),1,'cloud profile updates replace acknowledged local copies');
    for(const width of [390,320]){
      await page.setViewportSize({width,height:844});await page.evaluate(()=>ui.showDecks());assert.ok(await page.evaluate(()=>document.querySelector('.ff-expansion').scrollWidth<=innerWidth));
      await page.getByRole('button',{name:'Edit Arcane Tempest',exact:true}).click();assert.equal(await page.locator('.ff-deck-slot').count(),5);assert.equal(await page.locator('.ff-title').textContent(),'Deck Builder');assert.equal(await page.locator('.ff-forge-fab').isVisible(),false,'Forge shortcut does not obscure the mobile save button');assert.ok(await page.evaluate(()=>document.querySelector('.ff-expansion').scrollWidth<=innerWidth),'builder fits mobile viewport');
      await page.getByLabel('Deck name',{exact:true}).fill('Mobile Tempest');await page.getByRole('button',{name:'Remove Guardian from deck',exact:true}).click();await page.locator('[data-troop="blade"]').click();assert.match(await page.locator('[data-slot="0"]').textContent(),/blade/);assert.equal(await page.getByRole('button',{name:'Save deck',exact:true}).isEnabled(),true);
      await page.locator('.ff-deck-filters').getByRole('button',{name:/Helions/}).click();assert.equal(await page.locator('.ff-deck-slot-filled').count(),0);assert.equal(await page.getByRole('button',{name:'Save deck',exact:true}).isDisabled(),true);
    }
    await page.evaluate(()=>{window.active=true;ui.showDecks();});assert.equal(await page.getByRole('button',{name:'Edit Iron Vanguard',exact:true}).isDisabled(),true);assert.match(await page.locator('.ff-match-lock').textContent(),/locked/);
    await page.evaluate(()=>{window.active=false;ui.showMenu();});await page.getByRole('button',{name:'Play Online',exact:false}).click();assert.equal(await page.locator('.ff-deck-card').count(),4);assert.equal(await page.evaluate(()=>online.length),0);await page.getByRole('button',{name:'Select and continue with Blight Swarm',exact:true}).click();await page.waitForFunction(()=>online.length===1);assert.equal(await page.evaluate(()=>online[0].faction),'Helions');
    await page.evaluate(()=>{ui.showDecks();profile={...profile,uid:'another-account',decks:[],selectedDeckId:'starter-aesir'};ui.updateProfile();});assert.equal(await page.locator('.ff-deck-card').count(),3,'profile changes discard the previous account’s local custom decks');assert.equal(await page.locator('.ff-deck-current h2').textContent(),'Arcane Tempest');
    assert.deepEqual(errors,[]);
  }finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
});
