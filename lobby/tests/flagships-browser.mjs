// Real hub/loader DOM checks. Firebase and artwork are isolated fixtures;
// this never logs into or writes to the production service. No screenshots.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const {chromium} = await import(process.env.PLAYWRIGHT_MODULE || 'playwright');
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const types = {'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.css':'text/css'};
const server = createServer(async(req,res)=>{
  try {
    const name=decodeURIComponent(new URL(req.url,'http://localhost').pathname);
    const target=path.resolve(root,'.'+name);
    if(!target.startsWith(root+path.sep))throw new Error('Invalid path');
    res.setHeader('Content-Type',types[path.extname(target)]||'text/plain');
    res.end(await readFile(target));
  } catch {res.writeHead(404);res.end('Not found');}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin=`http://127.0.0.1:${server.address().port}`;
const firebase=`
const stored=new Map();
export const initializeApp=()=>({}),getFirestore=()=>({});
export const getAuth=()=>({currentUser:null});
export const onAuthStateChanged=()=>()=>{};
export const signInAnonymously=async()=>{throw new Error('Offline test fixture');};
export const collection=(db,...p)=>({path:p.join('/')});
export const doc=(db,...p)=>({path:p.join('/')});
const snap=ref=>({id:ref.path.split('/').at(-1),exists:()=>stored.has(ref.path),data:()=>stored.get(ref.path)});
export const setDoc=async(ref,data)=>{stored.set(ref.path,{...stored.get(ref.path),...data});};
export const addDoc=async(ref,data)=>{const created={id:'layout-room',path:ref.path+'/layout-room'};stored.set(created.path,data);return created;};
export const getDoc=async ref=>snap(ref);
export const getDocs=async()=>({docs:[],empty:true,size:0,forEach(){}});
export const getCountFromServer=async()=>({data:()=>({count:0})});
export const onSnapshot=(ref,fn)=>{if(stored.has(ref.path))queueMicrotask(()=>fn(snap(ref)));return ()=>{};};
export const updateDoc=async(ref,data)=>{stored.set(ref.path,{...stored.get(ref.path),...data});};
export const deleteDoc=async ref=>stored.delete(ref.path);
export const runTransaction=async(db,fn)=>fn({get:ref=>getDoc(ref),set:setDoc,update:updateDoc,delete:deleteDoc});
export const serverTimestamp=()=>0,increment=n=>n;
export const query=ref=>ref,where=()=>({}),limit=()=>({}),orderBy=()=>({});
`;
const browser = await chromium.launch({headless:true, ...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});
const results=[];
async function open({width=1280,user='',brokenArt=false,turncraftCover=false,reducedMotion='no-preference'}={}) {
  const context=await browser.newContext({viewport:{width,height:900},reducedMotion});
  let turncraftCoverRequests=0;
  await context.addInitScript(name=>{if(name)localStorage.setItem('gh.username',name);},user);
  await context.route('**/*',route=>{
    const url=new URL(route.request().url());
    if(url.hostname==='www.gstatic.com')return route.fulfill({contentType:'text/javascript',body:firebase});
    if(url.href==='https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/effects/ui/turncraft-lobby.webp') {
      turncraftCoverRequests++;
      return turncraftCover?route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="800"><rect width="1200" height="800" fill="#394a62"/></svg>'}):route.abort();
    }
    if(url.origin!==origin)return route.abort();
    if(url.pathname.startsWith('/graphics/'))return brokenArt?route.abort():route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="640" height="400"><rect width="640" height="400" fill="#30483e"/></svg>'});
    if(url.pathname.startsWith('/game/turncraft/assets/factions/'))return brokenArt?route.abort():route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="1536" height="1024"><rect width="1536" height="1024" fill="#3b5063"/></svg>'});
    if(url.pathname.startsWith('/game/'))return route.fulfill({contentType:'text/html',body:'<!doctype html><title>Verified game navigation</title>'});
    return route.continue();
  });
  const page=await context.newPage();
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  await page.goto(origin+'/lobby/lobby.html');
  await page.locator('#cnmFlagshipGrid .cnm-pick').nth(3).waitFor();
  await page.waitForFunction(()=>document.querySelector('#view-discover').classList.contains('active'));
  return {context,page,errors,coverRequests:()=>turncraftCoverRequests};
}
try {
  const {context,page,errors}=await open();
  assert.deepEqual(await page.locator('#cnmFlagshipGrid .cnm-pick').evaluateAll(cards=>cards.map(card=>card.dataset.gameKey)),['apestogetherstrong','warrealms','cat','ironthrone']);
  assert.equal(await page.locator('#modalLayer').isVisible(),false);
  assert.equal(await page.locator('#cnmFlagshipGrid [data-game-key="cat"] [data-game-action="single"]').count(),0);
  assert.equal(await page.locator('#cnmFlagshipGrid [data-game-key="apestogetherstrong"] [data-game-action="lobby"]').count(),0);
  assert.equal(await page.locator('#cnmFlagshipGrid .download-game-btn').count(),1);
  await page.locator('#cnmFlagshipGrid [data-game-key="cat"] [data-game-action="lobby"]').click();
  assert.equal(await page.locator('#accountGateModal').isVisible(),true);
  await page.locator('#gateNotNow').click();
  await page.locator('#cnmFlagshipGrid [data-game-key="cat"] .cnm-find-lobby').click();
  assert.equal(await page.locator('#view-multiplayer').evaluate(node=>node.classList.contains('active')),true);
  assert.equal(await page.locator('#lobbyGameFilter').inputValue(),'cat');
  assert.equal(new URL(page.url()).pathname,'/lobby/lobby.html');
  await page.locator('.main-nav [data-view="singleplayer"]').click();
  const fullCount=await page.locator('#singleGameGrid .game-card').count();
  assert.ok(fullCount>40);
  await page.locator('#gameSearch').fill('Apes Together Strong');
  await page.waitForFunction(()=>document.querySelectorAll('#singleGameGrid .game-card').length===1);
  assert.equal(await page.locator('#cnmLibraryPicks').isVisible(),false);
  await page.locator('#gameSearch').fill('');
  await page.waitForFunction(count=>document.querySelectorAll('#singleGameGrid .game-card').length===count,fullCount);
  assert.equal(await page.locator('#cnmLibraryPicks').isVisible(),true);
  await page.locator('#gameSearch').fill('TurnCraft');
  const turncraft=page.locator('#singleGameGrid [data-game-key="turncraft"]');
  assert.equal(await turncraft.count(),1);
  assert.deepEqual(await turncraft.locator('.tc-portrait img').evaluateAll(images=>images.map(image=>new URL(image.src).pathname)),['/game/turncraft/assets/factions/frontier.webp','/game/turncraft/assets/factions/ascendant.webp','/game/turncraft/assets/factions/brood.webp']);
  await page.waitForFunction(()=>[...document.querySelectorAll('#singleGameGrid .tc-portrait img')].every(image=>image.complete&&image.naturalWidth>0));
  assert.equal(await turncraft.locator('.tc-cover').isVisible(),false);
  assert.deepEqual(await turncraft.locator('[data-game-action]').evaluateAll(buttons=>buttons.map(button=>[button.dataset.gameAction,button.textContent])),[['single','Play Offline'],['lobby','Create Lobby']]);
  await turncraft.locator('[data-game-action="lobby"]').click();
  assert.equal(await page.locator('#accountGateModal').isVisible(),true);
  await page.locator('#gateNotNow').click();
  results.push('TurnCraft: three faction images load before remote upload; original offline and account-gated lobby actions remain');
  await page.locator('.main-nav [data-view="discover"]').click();
  await page.locator('#cnmSearch').fill('War Realms');
  await page.locator('#cnmSearchForm button').click();
  assert.equal(await page.locator('#gameSearch').inputValue(),'War Realms');
  assert.equal(await page.locator('#view-singleplayer').evaluate(node=>node.classList.contains('active')),true);
  assert.deepEqual(errors,[]);results.push('Desktop: four flagships, preserved catalog, search, guest gate, correct lobby filtering, no auto-join');
  await context.close();

  const member=await open({user:'LayoutTester'});
  assert.equal(await member.page.locator('#cnmProfileOffer').isVisible(),false);
  await member.page.locator('#cnmFlagshipGrid [data-game-key="apestogetherstrong"] .favorite-btn').click();
  await member.page.waitForFunction(()=>document.querySelector('#cnmFlagshipGrid [data-game-key="apestogetherstrong"] .favorite-btn').getAttribute('aria-pressed')==='true');
  assert.equal(await member.page.locator('#cnmLibraryGrid [data-game-key="apestogetherstrong"] .favorite-btn').getAttribute('aria-pressed'),'true');
  await member.page.locator('.main-nav [data-view="singleplayer"]').click();
  await member.page.locator('#singleGameGrid [data-game-key="turncraft"] .favorite-btn').click();
  await member.page.waitForFunction(()=>document.querySelector('#favoritesStrip [data-game-key="turncraft"] .favorite-btn')?.getAttribute('aria-pressed')==='true');
  assert.equal(await member.page.locator('#singleGameGrid [data-game-key="turncraft"] .favorite-btn').getAttribute('aria-pressed'),'true');
  assert.equal(await member.page.locator('#favoritesStrip [data-game-key="turncraft"]').evaluate(node=>node.classList.contains('tc-tile')),true);
  assert.equal(member.coverRequests(),1,'favorite rerenders must not retry a missing cover');
  await member.page.locator('.main-nav [data-view="discover"]').click();
  await member.page.locator('#cnmFlagshipGrid [data-game-key="cat"] [data-game-action="lobby"]').click();
  await member.page.locator('#newLobbyName').fill('Layout check');
  await member.page.locator('#createLobbySubmit').click();
  await member.page.waitForFunction(()=>document.querySelector('#view-multiplayer').classList.contains('active'));
  await member.page.waitForFunction(()=>document.querySelector('#currentLobbyName').textContent==='Layout check');
  assert.equal(new URL(member.page.url()).pathname,'/lobby/lobby.html');
  assert.equal(await member.page.locator('#currentLobbyContent').isVisible(),true);
  assert.equal(await member.page.locator('#startGameBtn').isVisible(),true);
  assert.deepEqual(member.errors,[]);results.push('Account fixture: shared favorite state; successful create opens the current lobby without starting');
  await member.page.evaluate(()=>window.logout());
  await member.page.locator('.main-nav [data-view="discover"]').click();
  assert.equal(await member.page.locator('#cnmProfileOffer').isVisible(),true);
  await member.page.locator('#cnmCreateProfile').click();
  assert.equal(await member.page.locator('#createPane').isVisible(),true);
  assert.deepEqual(member.errors,[]);results.push('Logout refreshes the inline guest profile offer and its account action');
  await member.context.close();

  const solo=await open();
  await solo.page.locator('#cnmFlagshipGrid [data-game-key="warrealms"] [data-game-action="single"]').click();
  await solo.page.waitForURL('**/game/warrealms.html?mode=singleplayer');
  assert.deepEqual(solo.errors,[]);results.push('Native solo launch preserves WarRealms single-player route');
  await solo.context.close();

  const turncraftSolo=await open({turncraftCover:true});
  await turncraftSolo.page.locator('.main-nav [data-view="singleplayer"]').click();
  await turncraftSolo.page.locator('#gameSearch').fill('TurnCraft');
  await turncraftSolo.page.waitForFunction(()=>document.querySelector('#singleGameGrid .tc-tile')?.classList.contains('tc-cover-ready'));
  assert.equal(await turncraftSolo.page.locator('#singleGameGrid .tc-cover').isVisible(),true);
  await turncraftSolo.page.locator('#gameSearch').fill('');
  await turncraftSolo.page.locator('#gameSearch').fill('TurnCraft');
  await turncraftSolo.page.waitForFunction(()=>{
    const card=document.querySelector('#singleGameGrid .tc-tile'),cover=card?.querySelector('.tc-cover');
    return card?.classList.contains('tc-cover-ready')&&cover?.complete&&cover.naturalWidth>0&&!cover.hidden;
  });
  assert.equal(await turncraftSolo.page.locator('#singleGameGrid .tc-cover').isVisible(),true,'uploaded cover survives search rerenders');
  await turncraftSolo.page.locator('#singleGameGrid [data-game-key="turncraft"] [data-game-action="single"]').click();
  await turncraftSolo.page.waitForURL('**/game/turncraft.html');
  assert.deepEqual(turncraftSolo.errors,[]);results.push('TurnCraft: new uploaded cover remains decoded after search rerenders, and Play Offline preserves the native route');
  await turncraftSolo.context.close();

  for(const width of [320,390,768,1440]) {
    const mobile=await open({width,brokenArt:true,reducedMotion:'reduce'});
    const geometry=await mobile.page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth,buttons:[...document.querySelectorAll('#cnmFlagshipGrid button,#cnmFlagshipGrid a')].filter(node=>node.getClientRects().length).map(node=>node.getBoundingClientRect().height)}));
    assert.ok(geometry.scroll<=geometry.width+1,`${width}px horizontal overflow: ${geometry.scroll}`);
    assert.ok(geometry.buttons.every(height=>height>=43.9),`${width}px undersized touch target`);
    if(width<761){assert.equal(await mobile.page.locator('.mobile-nav [data-view]').count(),4);await mobile.page.locator('.mobile-nav [data-view="singleplayer"]').click();}
    else await mobile.page.locator('.main-nav [data-view="singleplayer"]').click();
    assert.ok(await mobile.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
    assert.equal(await mobile.page.locator('#cnmLibraryGrid .cnm-pick').count(),4);
    const tcGeometry=await mobile.page.locator('#singleGameGrid [data-game-key="turncraft"]').evaluate(card=>({
      width:card.clientWidth,scroll:card.scrollWidth,
      targets:[...card.querySelectorAll('button')].map(button=>({width:button.getBoundingClientRect().width,height:button.getBoundingClientRect().height})),
      description:getComputedStyle(card.querySelector('.game-description')).fontSize,
      hiddenCover:card.querySelector('.tc-cover').hidden,
      labels:[...card.querySelectorAll('.tc-factions>span')].map(node=>node.textContent.trim())
    }));
    assert.ok(tcGeometry.scroll<=tcGeometry.width+1,`${width}px TurnCraft card overflow`);
    assert.ok(tcGeometry.targets.every(target=>target.height>=43.9&&target.width>=43.9),`${width}px TurnCraft targets smaller than 44px`);
    assert.ok(parseFloat(tcGeometry.description)>=12,`${width}px TurnCraft description too small`);
    assert.equal(tcGeometry.hiddenCover,true);
    assert.deepEqual(tcGeometry.labels,['Frontier','Ascendant','Brood']);
    assert.deepEqual(mobile.errors,[]);results.push(`${width}px: no horizontal overflow, 44px flagship actions, library access, missing-art fallback, reduced motion`);
    await mobile.context.close();
  }
  console.log(JSON.stringify({passed:results.length,checks:results},null,2));
} finally {await browser.close();await new Promise(resolve=>server.close(resolve));}
