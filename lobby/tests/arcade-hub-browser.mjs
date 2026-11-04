// Actual hub and launcher with deterministic Firebase fixtures. No production writes.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import {fileURLToPath,pathToFileURL} from 'node:url';
const browserModule=process.env.PLAYWRIGHT_MODULE||'playwright';
const {chromium}=await import(path.isAbsolute(browserModule)?pathToFileURL(browserModule).href:browserModule);
const root=path.resolve(fileURLToPath(new URL('../..',import.meta.url)));
const firebase=String.raw`
const fixture=globalThis.__fixture,clean=value=>JSON.parse(JSON.stringify(value));
const snap=ref=>({ref,id:ref.path.split('/').at(-1),exists:()=>Object.hasOwn(fixture.data,ref.path),data:()=>clean(fixture.data[ref.path])});
const matches=(data,f)=>{if(f.kind!=='where')return true;const value=data[f.field];if(f.operator==='==')return value===f.value;if(f.operator==='array-contains')return(value||[]).includes(f.value);return true;};
const list=ref=>{const docs=Object.keys(fixture.data).filter(key=>key.startsWith(ref.path+'/')&&key.split('/').length===ref.path.split('/').length+1).map(key=>snap({path:key})).filter(doc=>(ref.filters||[]).every(f=>matches(doc.data(),f)));return{docs,empty:!docs.length,size:docs.length,forEach:fn=>docs.forEach(fn),docChanges:()=>[]};};
const emit=()=>{for(const item of fixture.listeners)queueMicrotask(()=>item.fn(item.ref.kind==='doc'?snap(item.ref):list(item.ref)));};
export const initializeApp=()=>({}),getFirestore=()=>({});
const auth={currentUser:{uid:'fixture-user'}};
export const getAuth=()=>auth,signInAnonymously=async()=>({user:auth.currentUser});
export const onAuthStateChanged=(_auth,fn)=>{queueMicrotask(()=>fn(auth.currentUser));return()=>{};};
export const collection=(_db,...p)=>({kind:'collection',path:p.join('/')}),doc=(_db,...p)=>({kind:'doc',path:p.join('/')});
export const query=(ref,...filters)=>({...ref,filters}),where=(field,operator,value)=>({kind:'where',field,operator,value});
export const limit=value=>({kind:'limit',value}),orderBy=(field,direction)=>({kind:'order',field,direction});
export const serverTimestamp=()=>Date.now(),increment=value=>({__increment:value});
export const getDoc=async ref=>snap(ref),getDocs=async ref=>list(ref),getCountFromServer=async ref=>({data:()=>({count:list(ref).size})});
function put(ref,patch,merge=true){const prior=fixture.data[ref.path]||{},next=merge?{...prior}:{};for(const[key,value]of Object.entries(patch))next[key]=value?.__increment?Number(prior[key]||0)+value.__increment:value;fixture.data[ref.path]=clean(next);emit();}
export const setDoc=async(ref,patch,options)=>put(ref,patch,!!options?.merge),updateDoc=async(ref,patch)=>put(ref,patch);
export const addDoc=async(ref,value)=>{const target={kind:'doc',path:ref.path+'/created-'+(++fixture.sequence)};put(target,value,false);return{...target,id:target.path.split('/').at(-1)};};
export const deleteDoc=async ref=>{delete fixture.data[ref.path];emit();};
export const onSnapshot=(ref,fn)=>{const item={ref,fn};fixture.listeners.push(item);queueMicrotask(()=>fn(ref.kind==='doc'?snap(ref):list(ref)));return()=>{fixture.listeners=fixture.listeners.filter(x=>x!==item);};};
export const runTransaction=async(_db,fn)=>fn({get:getDoc,update:(ref,patch)=>put(ref,patch),set:(ref,patch)=>put(ref,patch),delete:ref=>{delete fixture.data[ref.path];emit();}});
export const writeBatch=()=>({delete:ref=>{delete fixture.data[ref.path];},commit:async()=>emit()});
`;
const mime={'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.css':'text/css','.png':'image/png','.webp':'image/webp','.svg':'image/svg+xml'};
const server=http.createServer((req,res)=>{
  const name=path.resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://fixture').pathname));
  if(!name.startsWith(root+path.sep)||!fs.existsSync(name)||!fs.statSync(name).isFile()){res.writeHead(404);return res.end('Not found');}
  res.setHeader('content-type',mime[path.extname(name)]||'application/octet-stream');res.end(fs.readFileSync(name));
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});
const checks=[],contexts=[],errors=[];
const check=(message,value=true)=>{assert.ok(value,message);checks.push(message);};
async function open({width=1440,user='',saved='discover',recent=[],favorites=[],reduced=false,missingArt=false,missingCatalog=false,failedArcadeUpgrade=false,query=''}={}){
  const context=await browser.newContext({viewport:{width,height:1000},reducedMotion:reduced?'reduce':'no-preference'});contexts.push(context);context.setDefaultTimeout(12000);
  await context.addInitScript(({user,saved,recent,favorites})=>{
    globalThis.__fixture={data:{'users/Tester':{username:'Tester',password:'fixture-password',displayName:'Tester',favoriteGames:favorites,recentlyPlayed:recent,gamePlayCounts:{warrealms:99,cat:20}},'users/Friend':{username:'Friend',password:'fixture-password'},'gameStats/warrealms':{plays:99},'gameStats/cat':{plays:20},'lobbies/friend-room':{name:'Friend room',gameType:'cat',status:'waiting',players:['Friend'],host:'Friend',visibility:'public',createdAt:Date.now()}},listeners:[],sequence:0};
    localStorage.setItem('gh.lastTab',saved);localStorage.setItem('gh.profilePromptSeen','1');localStorage.setItem('gh.recentGames',JSON.stringify(recent));localStorage.setItem('gh.favorites',JSON.stringify(favorites));if(user)localStorage.setItem('gh.username',user);
  },{user,saved,recent,favorites});
  await context.route('**/*',route=>{
    const url=new URL(route.request().url());
    if(url.hostname==='www.gstatic.com'&&url.pathname.includes('/firebasejs/'))return route.fulfill({contentType:'text/javascript',body:firebase});
    if(url.origin!==origin)return route.abort();
    if(missingCatalog&&url.pathname==='/lobby/lobby-catalog-organization.js')return route.abort();
    if(failedArcadeUpgrade&&url.pathname==='/lobby/lobby-arcade.js')return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(path.join(root,'lobby/lobby-arcade.js'),'utf8')+'\nglobalThis.GameHubArcade={...globalThis.GameHubArcade,upgradeHtml(){throw new Error("Fixture: arcade transformation unavailable");}};'});
    if(url.pathname.startsWith('/game/')&&(/\.html$/.test(url.pathname)||url.pathname.endsWith('/')))return route.fulfill({contentType:'text/html',body:'<!doctype html><title>Launched</title><main id="launched">Game route reached</main>'});
    if(missingArt&&/\.(png|webp|jpe?g|svg)$/.test(url.pathname))return route.abort();
    return route.continue();
  });
  const page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
  await page.goto(origin+'/lobby/lobby.html'+query);
  await page.locator('#cnmFlagshipGrid .game-card').nth(3).waitFor({state:'attached'});
  if(user)await page.waitForFunction(()=>document.getElementById('accountButtonText').textContent==='Tester');
  return page;
}
async function nav(page,view){const selector=await page.locator('.main-nav').isVisible()?'.main-nav':'.mobile-nav';await page.locator(`${selector} [data-view="${view}"]`).click();await page.locator(`#view-${view}.active`).waitFor();}
async function keys(page){return page.locator('#singleGameGrid .game-card').evaluateAll(cards=>cards.map(card=>card.dataset.gameKey));}
try{
  let page=await open({saved:'singleplayer'});
  await page.locator('#singleGameGrid .game-card').nth(71).waitFor({state:'attached'});
  check('all 72 catalog entries remain available',(await keys(page)).length===72);
  const full=await keys(page);
  await page.locator('#gameStatus').selectOption('prototype');
  check('Experimental filter reveals actual prototype cards',await page.locator('#singleGameGrid .game-card').evaluateAll(cards=>cards.length>0&&cards.every(card=>card.dataset.catalogStatus==='prototype'&&card.getClientRects().length>0)));
  await page.locator('#gameStatus').selectOption('tools');
  check('Tools filter includes all five tools',(await keys(page)).length===5&&(await keys(page)).includes('mtgcatalog'));
  await page.locator('#gameMode').selectOption('multi');
  check('combined unsupported mode and status produces a useful empty state',(await keys(page)).length===0&&(await page.locator('#singleGameGrid').innerText()).length>20);
  check('result count announces zero matches',/\b0\b/.test(await page.locator('#gameResultCount').innerText()));
  await page.locator('#clearGameFilters').click();
  check('Clear restores every game and resets status/mode/genre/search',(await keys(page)).length===72&&await page.locator('#gameStatus').inputValue()==='all'&&await page.locator('#gameMode').inputValue()==='all'&&await page.locator('#gameSearch').inputValue()==='');
  await page.locator('#gameStatus').selectOption('playable');
  check('In production remains discoverable',await page.locator('#singleGameGrid .game-card').evaluateAll(cards=>cards.length>0&&cards.every(card=>card.dataset.catalogStatus==='playable')));
  await page.locator('#gameStatus').selectOption('showcase');
  check('Complete filter matches editorial status',await page.locator('#singleGameGrid .game-card').evaluateAll(cards=>cards.length>0&&cards.every(card=>card.dataset.catalogStatus==='showcase')));
  await page.locator('#clearGameFilters').click();await page.locator('#gameSort').selectOption('title');
  const titles=await page.locator('#singleGameGrid .game-name').allTextContents();
  check('title sort is honored across development statuses',titles.every((title,index)=>index===0||titles[index-1].replace(/\s*\(Incomplete\)\s*/gi,' ').trim().localeCompare(title.replace(/\s*\(Incomplete\)\s*/gi,' ').trim())<=0));
  check('all three original downloads retain their exact targets',JSON.stringify(await page.locator('#singleGameGrid .download-game-btn').evaluateAll(links=>links.map(link=>link.getAttribute('href')).sort()))===JSON.stringify(['/game/apes-together-strong.html','/game/hearthmouse/Hearthmouse-Browser.zip','/game/ironfield/IRONFIELD.html'].sort()));
  await page.locator('#gameMode').selectOption('multi');
  await page.locator('#categoryPills .category-btn').filter({hasText:/^Strategy$/}).click();
  check('genre and multiplayer filters combine',(await keys(page)).includes('chesswarlord')&&!(await keys(page)).includes('arcanewilds')&&!(await keys(page)).includes('cat'));
  check('active filters are visible',(await page.locator('#gameActiveFilters').innerText()).trim().length>0);
  await nav(page,'discover');await nav(page,'singleplayer');
  check('returning to library retains the displayed genre filter',(await page.locator('#gameActiveFilters').innerText()).includes('Strategy'));
  await page.locator('#gameSearch').fill('Chess Warlord');check('name search combines with active filters',(await keys(page)).join(',')==='chesswarlord');
  await page.locator('#clearGameFilters').click();check('clear keeps selected sort',await page.locator('#gameSort').inputValue()==='title');
  await page.locator('#gameSearch').fill('unmatched-search-87269');check('no results includes a clear next action',await page.locator('#singleGameGrid button,#singleGameGrid a').count()>0);
  await page.context().close();

  page=await open({user:'Tester',recent:[{key:'cat',playedAt:Date.now()},'warrealms','cat','deleted'],favorites:['warrealms']});
  check('Continue playing deduplicates and excludes stale recents',JSON.stringify(await page.locator('#cnmRecentGrid [data-game-key]').evaluateAll(cards=>cards.map(card=>card.dataset.gameKey)))===JSON.stringify(['cat','warrealms']));
  await nav(page,'mygames');
  check('My games keeps legacy online and solo recents',await page.locator('#cnmMyRecentGrid [data-game-key]').count()===2);
  check('personal library explains per-game saves',/progress.*each game/i.test(await page.locator('#view-mygames').innerText()));
  await page.locator('#cnmMyFavoritesGrid [data-game-key="warrealms"] .favorite-btn').click();
  await page.waitForFunction(()=>JSON.parse(localStorage.getItem('gh.favorites')).length===0);
  check('favorite removal persists and leaves an actionable empty state',await page.locator('#cnmMyFavoritesGrid [data-game-key]').count()===0&&await page.locator('#cnmMyFavoritesGrid button,#cnmMyFavoritesGrid a').count()>0);
  await nav(page,'singleplayer');await page.locator('#gameSearch').fill('Cat & Mouse');
  await page.locator('#singleGameGrid .favorite-btn').click();await page.waitForFunction(()=>JSON.parse(localStorage.getItem('gh.favorites')).includes('cat'));
  await nav(page,'mygames');check('favorite addition refreshes personal cards',await page.locator('#cnmMyFavoritesGrid [data-game-key="cat"] .favorite-btn').getAttribute('aria-pressed')==='true');
  await nav(page,'multiplayer');await page.locator('#lobbyGrid .lobby-card .join').first().click();await page.locator('#currentLobbyContent:not([hidden])').waitFor();
  check('Join waits for host and retains lobby URL',new URL(page.url()).pathname==='/lobby/lobby.html'&&await page.locator('#waitingForHost').isVisible()&&!(await page.locator('#startGameBtn').isVisible()));
  await page.locator('#leaveLobbyBtn').click();
  await page.locator('#openCreateLobby').click();await page.locator('#newLobbyGame').selectOption('cat');await page.locator('#newLobbyName').fill('Acceptance room');await page.locator('#createLobbySubmit').click();
  await page.waitForFunction(()=>document.getElementById('currentLobbyName').textContent==='Acceptance room');
  check('Create leaves host in an unstarted room',new URL(page.url()).pathname==='/lobby/lobby.html'&&await page.locator('#startGameBtn').isVisible());
  await page.locator('#invitePlayerBtn').click();await page.locator('#inviteUsername').fill('Friend');await page.locator('#sendInviteSubmit').click();
  await page.waitForFunction(()=>Object.keys(__fixture.data).some(key=>key.startsWith('lobbyInvites/')));
  check('host invitation retains recipient and room identity',await page.evaluate(()=>Object.entries(__fixture.data).some(([key,value])=>key.startsWith('lobbyInvites/')&&value.toUsername==='Friend'&&value.lobbyName==='Acceptance room'&&value.gameType==='cat')));
  await page.locator('#modalLayer').waitFor({state:'hidden'});
  await page.locator('#startGameBtn').click();await page.waitForURL('**/game/catandmouse.html?*');
  const started=new URL(page.url());check('host Start carries actual lobby and username to the original route',started.searchParams.get('gameId')==='created-1'&&started.searchParams.get('username')==='Tester');
  await page.context().close();

  page=await open({user:'Tester',saved:'singleplayer',recent:['cat','warrealms']});
  await page.locator('#gameSort').selectOption('recent');check('recent sort retains stored recency order',JSON.stringify((await keys(page)).slice(0,2))===JSON.stringify(['cat','warrealms']));
  await page.locator('#gameSort').selectOption('popular');check('popular sort follows recorded play counts',JSON.stringify((await keys(page)).slice(0,2))===JSON.stringify(['warrealms','cat']));
  await nav(page,'profile');await page.locator('#editProfileBtn').click();await page.locator('#profileDisplayNameInput').fill('Arcade Tester');await page.locator('#saveProfileSubmit').click();await page.waitForFunction(()=>document.getElementById('profileName').textContent==='Arcade Tester');
  check('profile editor retains its save behavior');
  await nav(page,'multiplayer');await page.locator('#onlineWorldGrid [data-game-key="gridbound"] .join-online-world').click();await page.waitForURL('**/game/Gridbound_RPG_MMO.html?*');
  const realm=new URL(page.url());check('online world action keeps the persistent realm identity',realm.searchParams.get('realm')==='public-gridbound-realm'&&realm.searchParams.get('username')==='Tester');await page.context().close();

  page=await open();
  const guestAction=page.locator('#cnmFlagshipGrid [data-game-key="cat"] [data-game-action="lobby"]');
  await guestAction.focus();await page.keyboard.press('Enter');
  check('guest multiplayer retains the account gate',await page.locator('#accountGateModal').isVisible());
  const gate=page.locator('#accountGateModal'),gateControls=gate.locator('button:visible:not([disabled]),input:visible:not([disabled]),select:visible:not([disabled]),textarea:visible:not([disabled]),a[href]:visible,[tabindex="0"]:visible');
  assert.equal(await gate.getAttribute('role'),'dialog');assert.equal(await gate.getAttribute('aria-modal'),'true');
  assert.ok(await gate.getAttribute('aria-labelledby'));
  await gateControls.last().focus();await page.keyboard.press('Tab');
  assert.equal(await gateControls.first().evaluate(node=>node===document.activeElement),true);
  await page.keyboard.press('Shift+Tab');
  assert.equal(await gateControls.last().evaluate(node=>node===document.activeElement),true);
  await page.keyboard.press('Escape');await gate.waitFor({state:'hidden'});
  check('guest dialog traps keyboard focus and Escape restores its original action',await guestAction.evaluate(node=>node===document.activeElement));
  await page.locator('#inviteToggle').click();
  assert.equal(await page.locator('#inviteToggle').getAttribute('aria-expanded'),'true');
  assert.equal(await page.locator('#invitePanel').isVisible(),true);
  await page.keyboard.press('Escape');await page.locator('#invitePanel').waitFor({state:'hidden'});
  check('invite dropdown exposes expanded state and Escape restores its trigger',await page.locator('#inviteToggle').getAttribute('aria-expanded')==='false'&&await page.locator('#inviteToggle').evaluate(node=>node===document.activeElement));
  await nav(page,'multiplayer');await page.locator('[data-multi-target="lobbySearch"]').click();
  check('multiplayer shortcut focuses search without removing it from the Tab order',await page.locator('#lobbySearch').evaluate(node=>node===document.activeElement&&node.tabIndex===0));
  await page.locator('[data-multi-target="onlineWorldSection"]').focus();await page.keyboard.press('Tab');
  check('lobby search remains reachable through normal keyboard navigation',await page.locator('#lobbySearch').evaluate(node=>node===document.activeElement));
  await nav(page,'mygames');check('first-visit personal empty states have next actions',await page.locator('#cnmMyRecentGrid button,#cnmMyRecentGrid a').count()>0&&await page.locator('#cnmMyFavoritesGrid button,#cnmMyFavoritesGrid a').count()>0);
  await nav(page,'discover');await page.locator('#cnmFlagshipGrid [data-game-key="warrealms"] [data-game-action="single"]').click();await page.waitForURL('**/game/warrealms.html?mode=singleplayer');
  check('solo action retains exact War Realms URL');await page.context().close();

  page=await open({user:'Tester',query:'?lobby=friend-room'});await page.locator('#linkLobbyModal:not([hidden])').waitFor();
  check('invite link previews without joining',await page.evaluate(()=>!__fixture.data['lobbies/friend-room'].players.includes('Tester')));
  await page.locator('#linkLobbyJoinBtn').click();await page.locator('#currentLobbyContent:not([hidden])').waitFor();
  check('invite confirmation joins only after explicit action',await page.evaluate(()=>__fixture.data['lobbies/friend-room'].players.includes('Tester')));await page.context().close();

  page=await open({saved:'singleplayer',missingCatalog:true});
  await page.locator('#singleGameGrid .game-card').nth(71).waitFor({state:'attached'});
  check('missing catalog helper leaves all 72 games available',(await keys(page)).length===72);
  check('missing catalog helper disables only the unavailable status filter',await page.locator('#gameStatus').isDisabled()&&/unavailable/i.test(await page.locator('#gameStatus option:checked').innerText())&&await page.locator('#gameSearch').isEnabled());
  await nav(page,'discover');await page.locator('#cnmSearch').fill('War Realms');await page.locator('#cnmSearchForm [type="submit"]').click();
  check('search still opens the matching game when the catalog helper is missing',(await keys(page)).join(',')==='warrealms');
  await page.locator('#clearGameFilters').click();
  check('clearing search restores all 72 games without the catalog helper',(await keys(page)).length===72);
  await nav(page,'profile');check('profile navigation still renders without the catalog helper',await page.locator('#profileGuest').isVisible());
  await nav(page,'multiplayer');check('multiplayer navigation still renders without the catalog helper',await page.locator('#lobbySearch').isVisible());
  await page.context().close();

  page=await open({saved:'singleplayer',failedArcadeUpgrade:true});
  await page.locator('#singleGameGrid .catalog-shelf-heading').first().waitFor({state:'attached'});
  check('rejected arcade transformation keeps the legacy layout despite the API global',await page.evaluate(()=>!!globalThis.GameHubArcade&&!document.body.classList.contains('arcade-hub')&&!document.querySelector('link[href*="lobby-arcade.css"]')));
  check('rejected arcade transformation preserves legacy catalog grouping and every game',(await keys(page)).length===72&&await page.locator('#singleGameGrid .catalog-status-badge').count()===72&&await page.locator('#singleGameGrid .catalog-prototype-toggle').count()===1);
  await page.locator('#singleGameGrid .catalog-prototype-toggle').click();
  check('legacy experimental controls remain functional after a rejected transformation',await page.locator('#singleGameGrid .catalog-prototype-card').evaluateAll(cards=>cards.length>0&&cards.every(card=>card.getClientRects().length>0)));
  await page.context().close();

  for(const width of [360,768,1440]){
    page=await open({width,reduced:true,missingArt:true});
    for(const view of ['discover','singleplayer','mygames','multiplayer','profile']){
      if(view!=='discover')await nav(page,view);
      check(`${width}px ${view} fits viewport`,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
    }
    await nav(page,'singleplayer');await page.locator('#gameSearch').focus();await page.keyboard.press('Tab');
    check(`${width}px keyboard focus stays visible`,await page.evaluate(()=>{const e=document.activeElement,s=getComputedStyle(e);return e!==document.body&&(s.outlineStyle!=='none'||s.boxShadow!=='none');}));
    check(`${width}px reduced motion has no long running animation`,await page.evaluate(()=>document.getAnimations().filter(a=>a.playState==='running').every(a=>a.effect?.getTiming().duration<=1)));
    check(`${width}px failed artwork keeps launch actions enabled`,await page.locator('#singleGameGrid [data-game-key="apestogetherstrong"] [data-game-action="single"]').isEnabled());
    await page.context().close();
  }
  check('no uncaught browser errors',errors.length===0);
  console.log(JSON.stringify({passed:checks.length,checks},null,2));
}catch(error){console.error(JSON.stringify({passed:checks.length,last:checks.at(-1),errors},null,2));throw error;}
finally{await Promise.allSettled(contexts.map(context=>context.close()));await browser.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
