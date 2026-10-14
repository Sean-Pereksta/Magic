import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';

// Actual loader, core and presentation files; only Firebase and network are isolated.
// This is a browser integration fixture, not production auth/Firestore verification.
const require=createRequire(import.meta.url);
const {chromium}=require(process.env.PLAYWRIGHT_MODULE||'playwright');
const args=process.argv.slice(2);
const option=(key,fallback)=>args.includes(key)?args[args.indexOf(key)+1]:fallback;
const repo=path.resolve(option('--root',fileURLToPath(new URL('../..',import.meta.url))));
const assetRoot=path.resolve(option('--asset-root',repo));
const output=path.resolve(option('--output',path.join(repo,'..','qa-artifacts')));
const label=option('--label','after');
const measureOnly=args.includes('--measure-only');
const functionalOnly=args.includes('--functional-only');
const allowMissingArt=args.includes('--allow-missing-art');
const widths=option('--widths','').split(',').filter(Boolean).map(Number);
fs.mkdirSync(output,{recursive:true});

const firebase=String.raw`
const fixture=globalThis.__fixture;
const log=(op,ref)=>fixture.calls.push({op,path:ref?.path||'',at:performance.now()});
const clean=value=>JSON.parse(JSON.stringify(value));
const snap=ref=>({ref,id:ref.path.split('/').at(-1),exists:()=>Object.hasOwn(fixture.data,ref.path),data:()=>clean(fixture.data[ref.path])});
const matches=(data,f)=>{if(f.kind!=='where')return true;const value=data[f.field];if(f.operator==='==')return value===f.value;if(f.operator==='array-contains')return (value||[]).includes(f.value);if(f.operator==='in')return f.value.includes(value);if(f.operator==='<')return value<f.value;if(f.operator==='<=')return value<=f.value;if(f.operator==='>')return value>f.value;if(f.operator==='>=')return value>=f.value;return true;};
const list=ref=>{const docs=Object.keys(fixture.data).filter(key=>key.startsWith(ref.path+'/')&&key.split('/').length===ref.path.split('/').length+1).map(key=>snap({path:key})).filter(doc=>(ref.filters||[]).every(f=>matches(doc.data(),f)));return {docs,empty:!docs.length,size:docs.length,forEach:callback=>docs.forEach(callback),docChanges:()=>[]};};
const emit=()=>{for(const item of fixture.listeners)queueMicrotask(()=>item.callback(item.ref.kind==='doc'?snap(item.ref):list(item.ref)));};
export const initializeApp=()=>({});
export const getFirestore=()=>({});
const auth={currentUser:fixture.authFailure?null:{uid:'fixture-auth-user'}};
export const getAuth=()=>auth;
export const signInAnonymously=async()=>{if(fixture.authFailure)throw new Error('Fixture authentication unavailable');return {user:auth.currentUser};};
export const onAuthStateChanged=(_auth,callback)=>{queueMicrotask(()=>callback(auth.currentUser));return ()=>{};};
export const collection=(_db,...parts)=>({kind:'collection',path:parts.join('/')});
export const doc=(_db,...parts)=>({kind:'doc',path:parts.join('/')});
export const query=(ref,...filters)=>({...ref,filters});
export const where=(field,operator,value)=>({kind:'where',field,operator,value});
export const limit=value=>({kind:'limit',value});
export const orderBy=(field,direction)=>({kind:'order',field,direction});
export const serverTimestamp=()=>Date.now();
export const increment=value=>({__increment:value});
export const getDoc=async ref=>{log('getDoc',ref);return snap(ref);};
export const getDocs=async ref=>{log('getDocs',ref);return list(ref);};
export const getCountFromServer=async ref=>({data:()=>({count:list(ref).size})});
function put(ref,patch,merge=true){const prior=fixture.data[ref.path]||{},value=merge?{...prior}:{};for(const [key,item] of Object.entries(patch)){value[key]=item?.__increment?Number(prior[key]||0)+item.__increment:item;}fixture.data[ref.path]=clean(value);emit();}
export const setDoc=async(ref,patch,options)=>{log('setDoc',ref);put(ref,patch,!!options?.merge);};
export const updateDoc=async(ref,patch)=>{log('updateDoc',ref);put(ref,patch);};
export const addDoc=async(ref,value)=>{const target={kind:'doc',path:ref.path+'/created-'+(++fixture.sequence)};log('addDoc',target);put(target,value,false);return {...target,id:target.path.split('/').at(-1)};};
export const deleteDoc=async ref=>{log('deleteDoc',ref);delete fixture.data[ref.path];emit();};
export const onSnapshot=(ref,callback)=>{log('onSnapshot',ref);const item={ref,callback};fixture.listeners.push(item);queueMicrotask(()=>callback(ref.kind==='doc'?snap(ref):list(ref)));return ()=>{fixture.listeners=fixture.listeners.filter(value=>value!==item);};};
export const runTransaction=async(_db,callback)=>{log('transaction');return callback({get:getDoc,update:(ref,patch)=>put(ref,patch),set:(ref,patch)=>put(ref,patch),delete:ref=>{delete fixture.data[ref.path];emit();}});};
export const writeBatch=()=>({delete:ref=>{delete fixture.data[ref.path];},commit:async()=>emit()});
`;

const server=http.createServer((request,response)=>{
  const pathname=decodeURIComponent(new URL(request.url,'http://fixture').pathname);
  let resolved=path.resolve(repo,'.'+pathname);
  if(!fs.existsSync(resolved)&&/\.(png|webp|jpe?g|svg)$/i.test(pathname))resolved=path.resolve(assetRoot,'.'+pathname);
  if((!resolved.startsWith(repo+path.sep)&&!resolved.startsWith(assetRoot+path.sep))||!fs.existsSync(resolved)||!fs.statSync(resolved).isFile()){
    response.writeHead(404);response.end('Fixture file unavailable');return;
  }
  const ext=path.extname(resolved);
  const types={'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.css':'text/css','.png':'image/png','.webp':'image/webp','.svg':'image/svg+xml','.jpg':'image/jpeg'};
  response.writeHead(200,{'content-type':types[ext]||'application/octet-stream','cache-control':'no-store'});
  response.end(fs.readFileSync(resolved));
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const base=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:process.platform==='win32'?{channel:'msedge'}:{})});
const results={label,environment:{browser:await browser.version(),firebase:'in-memory fixture; remote traffic blocked',root:path.basename(repo)},measurements:[],checks:[]};
const check=(name,value=true)=>{assert.ok(value,name);results.checks.push(name);};
const errors=[];
const contexts=[];

async function open({width=1440,height=1000,user='',saved='discover',reduced=false,missingArt=false,query='',data={},shell=false,lowPower=false,noFlagships=false,authFailure=false,emptyLobbies=false}={}){
  const context=await browser.newContext({viewport:{width,height},reducedMotion:reduced?'reduce':'no-preference'});
  context.setDefaultTimeout(10000);
  contexts.push(context);
  await context.addInitScript(({user,saved,data,lowPower,authFailure,emptyLobbies})=>{
    if(lowPower)Object.defineProperty(navigator,'hardwareConcurrency',{value:2});
    const users=Object.fromEntries(['Sean','Tester','Friend'].map((username,index)=>['users/'+username,{username,password:'fixture-password',displayName:username+' Arcade',wins:3-index,lastLogin:Date.now()-index*60000,createdAt:Date.now()-86400000,favoriteCategory:'Strategy',favoriteGames:['warrealms'],recentlyPlayed:[{key:'apestogetherstrong',playedAt:Date.now()}],gamePlayCounts:{apestogetherstrong:5,warrealms:2},avatar:'🐭'}]));
    globalThis.__fixture={data:{...users,'lobbies/friend-room':{name:'Friend room',gameType:'cat',status:'waiting',host:'Friend',players:['Friend'],visibility:'public',createdAt:Date.now()},...data},calls:[],listeners:[],sequence:0};
    globalThis.__fixture.authFailure=authFailure;if(emptyLobbies)delete globalThis.__fixture.data['lobbies/friend-room'];
    globalThis.__qa={longTasks:[],lcp:[],intervals:{},raf:0};
    try{new PerformanceObserver(list=>globalThis.__qa.longTasks.push(...list.getEntries().map(item=>({start:item.startTime,duration:item.duration})))).observe({type:'longtask',buffered:true});}catch{}
    try{new PerformanceObserver(list=>globalThis.__qa.lcp.push(...list.getEntries().map(item=>({start:item.startTime,size:item.size,url:item.url})))).observe({type:'largest-contentful-paint',buffered:true});}catch{}
    const originalSet=globalThis.setInterval.bind(globalThis),originalClear=globalThis.clearInterval.bind(globalThis);
    globalThis.setInterval=(callback,delay,...rest)=>{const id=originalSet(callback,delay,...rest);globalThis.__qa.intervals[id]={delay,calls:0};return id;};
    globalThis.clearInterval=id=>{delete globalThis.__qa.intervals[id];return originalClear(id);};
    const originalRaf=globalThis.requestAnimationFrame.bind(globalThis);
    globalThis.requestAnimationFrame=callback=>{globalThis.__qa.raf++;return originalRaf(callback);};
    localStorage.setItem('gh.lastTab',saved);localStorage.setItem('gh.profilePromptSeen','1');
    if(user)localStorage.setItem('gh.username',user);else localStorage.removeItem('gh.username');
  },{user,saved,data,lowPower,authFailure,emptyLobbies});
  await context.route('**/*',async route=>{
    const url=new URL(route.request().url());
    if(url.hostname==='www.gstatic.com'&&url.pathname.includes('/firebasejs/'))return route.fulfill({status:200,contentType:'text/javascript',body:firebase});
    if(noFlagships&&url.pathname==='/lobby/lobby-flagships.js')return route.fulfill({status:200,contentType:'text/javascript',body:''});
    if(url.pathname.startsWith('/game/')&&url.pathname.endsWith('.html'))return route.fulfill({status:200,contentType:'text/html',body:'<!doctype html><title>Fixture route reached</title><p id="launched">Game navigation recorded</p>'});
    if(shell&&url.hostname==='catnmice.com')return route.fulfill({response:await route.fetch({url:base+url.pathname+url.search})});
    if(!url.href.startsWith(base))return route.abort('blockedbyclient');
    if(missingArt&&/\.(png|webp|jpg|jpeg|svg)(\?|$)/i.test(url.href))return route.fulfill({status:404,body:''});
    return route.continue();
  });
  const page=await context.newPage();
  page.on('pageerror',error=>errors.push(error.message));
  const started=performance.now();
  await page.goto((shell?'https://catnmice.com/index.html':base+'/lobby/lobby.html')+query,{waitUntil:'load'});
  if(shell)await page.frameLocator('#contentFrame').locator('#cnmFlagshipGrid .game-card').first().waitFor({state:'attached'});
  else await page.waitForSelector(noFlagships?'#view-multiplayer.active':'#cnmFlagshipGrid .game-card',{state:'attached'});
  if(authFailure)await page.waitForFunction(()=>document.getElementById('cnmLiveGrid')?.textContent.includes('temporarily unavailable'));
  else await page.waitForFunction(()=>globalThis.__fixture.calls.some(call=>call.op==='getDocs'));
  if(user&&!shell)await page.waitForFunction(user=>document.getElementById('accountButtonText').textContent===user,user);
  await page.waitForTimeout(350);
  page.__ready=performance.now()-started;
  return page;
}

async function measure(page,name){
  const metrics=await page.evaluate(()=>{
    const visible=node=>!!(node.getClientRects().length&&getComputedStyle(node).visibility!=='hidden');
    const art=Array.from(document.images).filter(visible).map(img=>({src:img.getAttribute('src'),width:Math.round(img.getBoundingClientRect().width),height:Math.round(img.getBoundingClientRect().height),naturalWidth:img.naturalWidth,naturalHeight:img.naturalHeight,loaded:img.complete&&img.naturalWidth>0}));
    const resources=performance.getEntriesByType('resource');
    const images=resources.filter(item=>item.initiatorType==='img');
    return {width:innerWidth,height:innerHeight,scrollWidth:document.documentElement.scrollWidth,domNodes:document.querySelectorAll('*').length,visibleCards:document.querySelectorAll('#view-discover .game-card').length,firstContentfulPaintMs:performance.getEntriesByName('first-contentful-paint')[0]?.startTime||null,lcp:globalThis.__qa.lcp.at(-1)||null,longTasks:globalThis.__qa.longTasks,images:{requests:images.length,transferBytes:images.reduce((n,item)=>n+item.transferSize,0),encodedBytes:images.reduce((n,item)=>n+item.encodedBodySize,0),visible:art},timers:Object.values(globalThis.__qa.intervals).map(item=>item.delay),raf:globalThis.__qa.raf,runningAnimations:document.getAnimations().filter(item=>item.playState==='running').length,activeView:document.querySelector('.view.active')?.id};
  });
  metrics.readyWallMs=Math.round(page.__ready);metrics.name=name;
  results.measurements.push(metrics);
  await capture(page,name);
  check(name+': no horizontal overflow',metrics.scrollWidth<=metrics.width+1);
  return metrics;
}

async function capture(page,name){
  const original=await page.evaluate(()=>scrollY);
  const height=await page.evaluate(()=>document.documentElement.scrollHeight);
  for(let top=0;top<height;top+=Math.max(220,page.viewportSize().height*.8)){await page.evaluate(top=>scrollTo({top,behavior:'instant'}),top);await page.waitForTimeout(60);}
  await page.evaluate(async()=>{await Promise.race([Promise.all(Array.from(document.images).filter(img=>img.getClientRects().length).map(img=>img.decode().catch(()=>{}))),new Promise(resolve=>setTimeout(resolve,1500))]);});
  await page.evaluate(()=>scrollTo({top:0,behavior:'instant'}));await page.waitForTimeout(100);
  await page.screenshot({path:path.join(output,`${label}-${name}.png`),fullPage:true});
  if(name==='desktop'||name==='width-390')await page.screenshot({path:path.join(output,`${label}-${name}-viewport.png`)});
  await page.evaluate(top=>scrollTo({top,behavior:'instant'}),original);
}

async function runtime(page,name){
  const cdp=await page.context().newCDPSession(page);await cdp.send('Performance.enable');
  const read=async()=>Object.fromEntries((await cdp.send('Performance.getMetrics')).metrics.map(item=>[item.name,item.value]));
  const before=await read();
  await page.waitForTimeout(1500);
  const after=await read();
  const sample={name,sampleMs:1500,taskMs:Math.round((after.TaskDuration-before.TaskDuration)*1000),scriptMs:Math.round((after.ScriptDuration-before.ScriptDuration)*1000),layoutMs:Math.round((after.LayoutDuration-before.LayoutDuration)*1000),jsHeapBytes:after.JSHeapUsedSize,domNodes:after.Nodes};
  (results.runtime||=[]).push(sample);await cdp.detach();
}

async function navigation(page,view){await page.locator(`[data-view="${view}"]:visible`).first().click();await page.waitForSelector(`#view-${view}.active`);}
async function closeModal(page){await page.locator('.modal:not([hidden]) [data-close-modal]').first().click();}

try{
  for(const [width,height,name] of (functionalOnly?[]:[[1440,1000,'desktop'],[1920,1080,'widescreen'],[768,1000,'width-768'],[844,390,'landscape-mobile'],[390,844,'width-390'],[320,844,'width-320']]).filter(([width])=>!widths.length||widths.includes(width))){
    const page=await open({width,height});
    await measure(page,name);
    if(width===1440||width===390)await runtime(page,name);
    check(`width ${width}: guest startup has no modal`,await page.locator('.modal:not([hidden])').count()===0);
    check(`width ${width}: four discover cards`,await page.locator('#cnmFlagshipGrid .game-card').count()===4);
    if(!allowMissingArt)check(`width ${width}: flagship artwork decodes`,await page.locator('#cnmFlagshipGrid .game-art img').evaluateAll(nodes=>nodes.length===4&&nodes.every(node=>node.complete&&node.naturalWidth>0)));
    if(await page.locator('.cnm-runner').count())check(`width ${width}: bounded decorative characters`,await page.locator('.cnm-runner').evaluateAll(nodes=>nodes.filter(node=>node.getClientRects().length).length<=(innerWidth<=760?3:8)));
    if(width<=520){
      check(`width ${width}: flagship title retains a large mobile size`,await page.locator('.cnm-hero .game-name').evaluate(node=>parseFloat(getComputedStyle(node).fontSize)>30));
      check(`width ${width}: flagship title and actions remain inside their card`,await page.locator('.cnm-hero').evaluate(card=>{const bounds=card.getBoundingClientRect();return [...card.querySelectorAll('.game-name,.game-actions .btn')].every(node=>{const box=node.getBoundingClientRect();return box.left>=bounds.left&&box.right<=bounds.right+1&&box.top>=bounds.top&&box.bottom<=bounds.bottom+1;});}));
    }
    await page.context().close();
  }
  if(!measureOnly){
    let page=await open();
    const callsAtStart=await page.evaluate(()=>globalThis.__fixture.calls);
    check('Discover defers building the hidden complete Library',await page.locator('#singleGameGrid .game-card').count()===0);
    check('startup does not create a lobby',!callsAtStart.some(call=>call.op==='addDoc'&&call.path.startsWith('lobbies/')));
    check('Cat & Mouse has no fabricated solo action',await page.locator('#cnmFlagshipGrid [data-game-key="cat"] [data-game-action="single"]').count()===0);
    await page.locator('#cnmFlagshipGrid [data-game-key="cat"] [data-game-action="lobby"]').click();
    check('guest multiplayer is account gated',await page.locator('#accountGateModal').isVisible());await closeModal(page);
    await page.locator('#cnmFlagshipGrid [data-game-key="warrealms"] .favorite-btn').click();
    check('guest favorite is account gated',await page.locator('#accountGateModal').isVisible());await closeModal(page);
    await page.locator('#cnmSearch').fill('Cat & Mouse');await page.locator('#cnmSearchForm [type="submit"]').click();
    check('search changes to library',await page.locator('#view-singleplayer.active').isVisible());
    check('search includes multiplayer-only games',await page.locator('#singleGameGrid [data-game-key="cat"]').count()===1);
    check('search hides personal quick picks',!(await page.locator('#cnmLibraryPicks').isVisible()));
    await page.locator('#gameSearch').fill('no-such-game-98765');
    check('empty search explains result',await page.locator('#singleGameGrid').innerText().then(text=>/No games match/i.test(text)));
    await page.locator('#gameSearch').fill('');
    await page.locator('#categoryPills .category-btn').filter({hasText:/^Strategy$/}).click();
    check('category retains multiplayer strategy games',await page.locator('#singleGameGrid [data-game-key="chesswarlord"]').count()===1);
    await page.locator('#categoryPills .category-btn').filter({hasText:/^All$/}).click();
    check('clearing filter restores quick picks',await page.locator('#cnmLibraryPicks').isVisible());
    await page.locator('#gameSort').selectOption('title');
    const titleOrder=await page.locator('#singleGameGrid .game-name').allTextContents();
    check('alphabetical sort orders the complete collection',titleOrder.length>50&&titleOrder.every((value,index)=>!index||titleOrder[index-1].localeCompare(value)<=0));
    await page.locator('#gameSearch').fill('Village');
    await page.locator('#singleGameGrid [data-game-key="village"] [data-game-action="single"]').click();
    check('account-required solo game retains its gate',await page.locator('#accountGateModal').isVisible());await closeModal(page);
    await navigation(page,'discover');
    await page.locator('#cnmFlagshipGrid [data-game-key="warrealms"] .cnm-find-lobby').click();
    check('find lobby sets correct game/open filters',await page.locator('#lobbyGameFilter').inputValue()==='warrealms'&&await page.locator('#lobbySlotFilter').inputValue()==='open');
    check('find lobby creates nothing',!(await page.evaluate(()=>globalThis.__fixture.calls)).some(call=>call.op==='addDoc'&&call.path.startsWith('lobbies/')));
    await navigation(page,'discover');
    await page.locator('#cnmFlagshipGrid [data-game-key="apestogetherstrong"] [data-game-action="single"]').click();
    await page.waitForURL('**/game/apes-together-strong.html');check('Apes Play uses original solo route');
    await page.context().close();

    for(const [key,action,expected] of [['warrealms','single','/game/warrealms.html?mode=singleplayer&username=Tester'],['turncraft','online','/game/turncraft-lobby.html?mode=browse&username=Tester'],['crowncouncil','single','/game/crowncouncil.html?mode=solo&username=Tester'],['gridbound','online','/game/Gridbound_RPG_MMO.html?username=Tester&realm=public-gridbound-realm']]){
      page=await open({user:'Tester',saved:'singleplayer'});
      await page.locator('#gameSearch').fill(key==='gridbound'?'Gridbound':key==='crowncouncil'?'Faction Forge':key==='turncraft'?'TurnCraft':'War Realms');
      await page.locator(`#singleGameGrid [data-game-key="${key}"] [data-game-action="${action}"]`).click();
      await page.waitForURL(base+expected);check(`${key}: original ${action} route and parameters`);await page.context().close();
    }

    page=await open({user:'Tester'});
    await page.locator('#cnmFlagshipGrid [data-game-key="warrealms"] .favorite-btn').click();
    await page.waitForFunction(()=>!JSON.parse(localStorage.getItem('gh.favorites')).includes('warrealms'));
    check('member favorite persists via original hook');
    await navigation(page,'singleplayer');
    check('deferred Library receives favorite changes made on Discover',await page.locator('#favoritesStrip [data-game-key="warrealms"]').count()===0&&await page.locator('#singleGameGrid [data-game-key="warrealms"] .favorite-btn').getAttribute('aria-pressed')==='false');
    check('deferred Library receives saved recent history',await page.locator('#continueStrip [data-game-key="apestogetherstrong"]').count()===1);
    await page.locator('#gameSort').selectOption('recent');check('recent sorting puts the saved game first',await page.locator('#singleGameGrid .game-card').first().getAttribute('data-game-key')==='apestogetherstrong');
    await navigation(page,'discover');
    await page.locator('#cnmFlagshipGrid [data-game-key="cat"] [data-game-action="lobby"]').click();
    await page.locator('#newLobbyName').fill('Fixture hosted room');await page.locator('#createLobbySubmit').click();
    await page.waitForSelector('#currentLobbyContent:not([hidden])');
    check('create lobby remains in hub until Start',page.url().includes('/lobby/lobby.html'));
    check('successful create opens multiplayer',await page.locator('#view-multiplayer.active').isVisible());
    check('created lobby has host start control',await page.locator('#startGameBtn').isVisible());
    await page.locator('#invitePlayerBtn').click();await page.locator('#inviteUsername').fill('Friend');await page.locator('#sendInviteSubmit').click();
    await page.waitForFunction(()=>Object.keys(globalThis.__fixture.data).some(key=>key.startsWith('lobbyInvites/')));
    check('host invitation writes through original action',await page.evaluate(()=>Object.values(globalThis.__fixture.data).some(item=>item.toUsername==='Friend'&&item.status==='pending')));
    if(await page.locator('#inviteModal').isVisible())await closeModal(page);
    await navigation(page,'profile');check('member profile renders display name',(await page.locator('#profileName').innerText())==='Tester Arcade');
    check('non-Sean profile cannot see admin panel',!(await page.locator('#adminMembersPanel').isVisible()));
    await navigation(page,'multiplayer');await page.locator('#startGameBtn').click();await page.waitForURL('**/game/catandmouse.html?*');
    check('explicit host Start launches the original Cat & Mouse route',new URL(page.url()).searchParams.get('username')==='Tester');
    await page.context().close();

    page=await open({user:'Tester'});await page.locator('#cnmFlagshipGrid [data-game-key="warrealms"] [data-game-action="lobby"]').click();await page.locator('#newLobbyName').fill('War Realms minimum');await page.locator('#createLobbySubmit').click();
    await page.waitForSelector('#currentLobbyContent:not([hidden])');check('War Realms host cannot start with only one player',await page.locator('#startGameBtn').isDisabled());await page.context().close();

    page=await open({user:'Tester',saved:'multiplayer',data:{'lobbies/saved-campaign':{name:'Saved campaign',gameType:'ironthrone',status:'started',host:'Tester',hostUid:'fixture-auth-user',players:['Tester'],members:{'fixture-auth-user':'Tester'},visibility:'private',createdAt:Date.now()}}});
    await page.locator('[data-resume-iron="saved-campaign"]').waitFor();check('member saved Iron Thrones campaign remains available');
    check('saved campaign does not launch automatically',page.url().includes('/lobby/lobby.html'));
    await page.locator('[data-resume-iron="saved-campaign"]').click();await page.waitForURL('**/game/iron-throne/index.html?*');
    check('campaign Resume preserves game ID and return route',new URL(page.url()).searchParams.get('gameId')==='saved-campaign'&&new URL(page.url()).searchParams.get('returnUrl')==='/lobby/lobby.html');await page.context().close();

    page=await open({user:'Tester',saved:'multiplayer'});
    await page.locator('#lobbyGrid .lobby-card .join').first().click();
    await page.waitForSelector('#currentLobbyContent:not([hidden])');
    check('joining does not navigate before host Start',page.url().includes('/lobby/lobby.html'));
    check('joined nonhost sees waiting message',await page.locator('#waitingForHost').isVisible());
    check('joined nonhost cannot Start',!(await page.locator('#startGameBtn').isVisible()));
    await page.context().close();

    page=await open({user:'Sean',saved:'profile'});
    check('remembered Sean starts with admin locked',await page.locator('#adminMembersLocked').isVisible()&&!(await page.locator('#adminMembersContent').isVisible()));
    await page.locator('#adminPassword').fill('wrong');await page.locator('#adminUnlock').click();
    await page.waitForFunction(()=>!document.getElementById('adminUnlock').disabled);
    check('wrong admin password remains locked',!(await page.locator('#adminMembersContent').isVisible()));
    check('admin password cleared after failed attempt',await page.locator('#adminPassword').inputValue()==='');
    await page.locator('#adminPassword').fill('fixture-password');await page.locator('#adminUnlock').click();
    await page.waitForSelector('#adminMembersContent:not([hidden])');
    check('verified admin can inspect fixture members',await page.locator('#adminMemberRows tr').count()===3);
    await page.locator('#adminMemberSearch').fill('Tester');check('admin member filter operates',await page.locator('#adminMemberRows tr').count()===1);
    await page.locator('#adminLock').click();check('admin Lock removes member rows',await page.locator('#adminMemberRows tr').count()===0);
    await page.context().close();

    page=await open({width:390});
    await page.locator('#accountButton').click();await page.locator('#loginUsername').fill('Tester');await page.locator('#loginPassword').fill('fixture-password');await page.locator('#loginSubmit').click();
    await page.waitForFunction(()=>document.getElementById('accountButtonText').textContent==='Tester');
    check('mobile login uses original account UI');
    for(const view of ['singleplayer','multiplayer','profile']){await navigation(page,view);await measure(page,'mobile-'+view);}
    await page.locator('#editProfileBtn').click();await page.locator('#profileDisplayNameInput').fill('Updated Player');await page.locator('#saveProfileSubmit').click();
    await page.waitForFunction(()=>document.getElementById('profileName').textContent==='Updated Player');check('profile editor persists updated display name');
    await page.locator('#profileLogoutBtn').click();await page.waitForFunction(()=>document.getElementById('accountButtonText').textContent==='Log In');check('logout restores guest account state');
    await navigation(page,'discover');await page.locator('#cnmCreateProfile').click();await page.locator('#createUsername').fill('NewFixtureUser');await page.locator('#createPassword').fill('fixture-password');await page.locator('#createConfirm').fill('fixture-password');await page.locator('#createSubmit').click();
    await page.waitForFunction(()=>document.getElementById('accountButtonText').textContent==='NewFixtureUser');check('mobile signup uses original account creation flow');
    await page.context().close();

    page=await open({width:320,user:'Tester',saved:'profile'});
    for(const view of ['profile','multiplayer','singleplayer']){
      if(view!=='profile')await navigation(page,view);
      check(`320px member ${view}: no horizontal overflow`,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
    }
    await navigation(page,'profile');await capture(page,'small-mobile-profile');await page.context().close();

    page=await open({user:'Tester',query:'?lobby=friend-room'});
    await page.waitForSelector('#linkLobbyModal:not([hidden])');
    check('invitation URL requires confirmation before joining',await page.evaluate(()=>!globalThis.__fixture.data['lobbies/friend-room'].players.includes('Tester')));
    await page.locator('#linkLobbyJoinBtn').click();await page.waitForSelector('#currentLobbyContent:not([hidden])');
    check('invitation URL joins after confirmation',await page.evaluate(()=>globalThis.__fixture.data['lobbies/friend-room'].players.includes('Tester')));
    await page.context().close();

    page=await open({user:'Tester',saved:'multiplayer',data:{'lobbies/full-room':{name:'Full War Realms',gameType:'warrealms',status:'waiting',host:'Friend',players:['Friend','One','Two','Three'],visibility:'public',createdAt:Date.now()+1000},'lobbies/private-room':{name:'Hidden private room',gameType:'cat',status:'waiting',host:'Friend',players:['Friend'],visibility:'private',createdAt:Date.now()}}});
    check('private lobby stays hidden for uninvited member',!(await page.locator('#lobbyGrid').innerText()).includes('Hidden private room'));
    if(await page.locator('#cnmLiveGrid').count())check('Discover activity uses the same private lobby rules',!(await page.locator('#cnmLiveGrid').innerText()).includes('Hidden private room'));
    await page.locator('#lobbySlotFilter').selectOption('all');
    check('full lobby cannot be joined',await page.locator('#lobbyGrid .lobby-card').filter({hasText:'Full War Realms'}).locator('.join').isDisabled());
    await page.context().close();

    page=await open({emptyLobbies:true});
    if(await page.locator('#cnmLiveGrid').count())check('empty activity uses an honest no-lobby state',await page.locator('#cnmLiveGrid .lobby-card').count()===0&&(await page.locator('#cnmLiveGrid').innerText()).includes('No waiting lobbies'));
    await page.context().close();
    page=await open({emptyLobbies:true,authFailure:true});
    check('authentication failure replaces connecting activity with unavailable',await page.locator('#cnmLiveGrid').innerText().then(text=>text.includes('temporarily unavailable')&&!text.includes('Connecting')));
    check('backend failure leaves solo game actions available',await page.locator('#cnmFlagshipGrid [data-game-key="apestogetherstrong"] [data-game-action="single"]').isEnabled());
    await page.context().close();

    page=await open({width:390,reduced:true,missingArt:true});
    await measure(page,'reduced-motion-missing-art');
    check('missing flagship artwork keeps actions available',await page.locator('#cnmFlagshipGrid [data-game-key="apestogetherstrong"] [data-game-action="single"]').isVisible());
    check('failed artwork reaches the graphical fallback',await page.locator('#cnmFlagshipGrid .game-visual.image-missing').count()===4);
    check('reduced motion prevents carousel timer',!(await page.evaluate(()=>Object.values(globalThis.__qa.intervals).map(item=>item.delay))).includes(6500));
    check('reduced motion disables active CSS animation',await page.evaluate(()=>document.getAnimations().filter(animation=>animation.playState==='running').every(animation=>animation.effect?.getTiming().duration<=1)));
    await page.context().close();

    page=await open({width:390,lowPower:true});
    if(await page.locator('.cnm-runner').count()){
      check('constrained hardware uses the low-power presentation',await page.locator('body.cnm-low-power').count()===1);
      check('low-power decoration remains static',await page.locator('.cnm-runner').evaluateAll(nodes=>nodes.every(node=>getComputedStyle(node).animationName==='none')));
      check('low-power decoration stops child animation',await page.locator('.cnm-ambient').evaluate(node=>node.getAnimations({subtree:true}).every(animation=>animation.playState!=='running')));
    }
    await page.context().close();

    page=await open({saved:'mygames',noFlagships:true});
    check('missing presentation script retains a usable saved-view fallback',await page.locator('#view-multiplayer.active').isVisible());
    await navigation(page,'singleplayer');
    check('presentation fallback preserves the full catalog',await page.locator('#singleGameGrid .game-card').count()===68);
    await page.context().close();

    page=await open({saved:'singleplayer'});
    if(await page.locator('#cnmBuiltOutGrid').count()){
      await page.waitForFunction(()=>document.querySelectorAll('#singleGameGrid .game-card[data-catalog-status]').length===68);
      const tierState=await page.locator('#singleGameGrid .game-card').evaluateAll(cards=>{
        const counts={};for(const card of cards)counts[card.dataset.catalogStatus]=(counts[card.dataset.catalogStatus]||0)+1;
        const visible=cards.filter(card=>card.getClientRects().length),best=visible.filter(card=>card.dataset.catalogStatus==='showcase'),others=visible.filter(card=>card.dataset.catalogStatus!=='showcase');
        return {counts,bestBottom:Math.max(...best.map(card=>card.getBoundingClientRect().bottom)),otherTop:Math.min(...others.map(card=>card.getBoundingClientRect().top))};
      });
      results.qualityTiers=tierState.counts;
      check('built-out games appear above every other catalog tier',tierState.bestBottom<=tierState.otherTop+1);
      for(const [status,label] of [['showcase','Best Games · Built Out'],['playable','In Production'],['prototype','Experimental']])check(`catalog exposes ${label}`,await page.locator(`#singleGameGrid [data-catalog-shelf="${status}"]`).innerText().then(text=>text.includes(label)));
      check('Experimental games are collapsed by default',await page.locator('#singleGameGrid [data-catalog-status="prototype"]').first().isHidden());
      await page.locator('#singleGameGrid .catalog-prototype-toggle').click();check('Experimental games remain available on request',await page.locator('#singleGameGrid [data-catalog-status="prototype"]').first().isVisible());
      await page.locator('#singleGameGrid .catalog-prototype-toggle').click();
      await navigation(page,'discover');
      const promotions=await page.locator('#cnmFlagshipGrid .game-card,#cnmPremiumGrid .game-card,#cnmBuiltOutGrid .game-card').evaluateAll(cards=>cards.map(card=>card.dataset.gameKey));
      const best=await page.evaluate(()=>[...GameHubShowcase.flagships,...GameHubShowcase.premium,...GameHubShowcase.builtOut].map(item=>item.key));
      check('Discover promotes only explicitly curated built-out games',JSON.stringify(promotions)===JSON.stringify(best));
      const statuses=await page.locator('#singleGameGrid .game-card').evaluateAll(cards=>Object.fromEntries(cards.map(card=>[card.dataset.gameKey,card.dataset.catalogStatus])));
      check('Discover promotions contain no In Production or Experimental projects',promotions.every(key=>statuses[key]==='showcase'));
    }
    await page.context().close();

    page=await open();
    const newDesign=await page.locator('#view-mygames').count()>0;
    if(newDesign){
      const premiumKeys=await page.locator('#cnmPremiumGrid [data-game-key]').evaluateAll(nodes=>nodes.map(node=>node.dataset.gameKey));
      check('all eight requested premium games appear',JSON.stringify(premiumKeys)===JSON.stringify(['turncraft','crowncouncil','neonbreach','arcanewilds','chrisrunner','spacetyrants','chesswarlord','hearthmouse']));
      check('premium games retain favorite and supported launch controls',await page.locator('#cnmPremiumGrid .game-card').evaluateAll(nodes=>nodes.every(node=>node.querySelector('.favorite-btn')&&node.querySelector('[data-game-action]'))));
      await page.locator('#cnmSearch').fill('war');
      await page.waitForSelector('#cnmSearchResults:not([hidden]) .cnm-search-result');
      check('instant search supplies graphical game suggestions',await page.locator('#cnmSearchResults img').count()>0);
      await page.locator('#cnmSearch').press('Escape');check('Escape dismisses instant search',!(await page.locator('#cnmSearchResults').isVisible()));
      await page.locator('[data-quick="solo"]').click();
      check('Play solo quick access filters by supported mode',await page.locator('#gameMode').inputValue()==='single'&&await page.locator('#singleGameGrid [data-game-key="cat"]').count()===0);
      await navigation(page,'discover');await page.locator('[data-quick="favorites"]').click();
      check('favorites quick access opens My games',await page.locator('#view-mygames.active').isVisible());
      await navigation(page,'discover');
      const counts=await page.evaluate(()=>[...document.querySelectorAll('.cnm-runner')].filter(node=>node.getClientRects().length).length);
      check('desktop decoration is bounded to eight characters',counts<=8&&(counts>=3||await page.locator('body.cnm-low-power').count()>0));
      check('decoration cannot intercept user actions',await page.locator('.cnm-ambient').evaluate(node=>getComputedStyle(node).pointerEvents==='none'));
      const initial=await page.locator('*').count();
      for(let index=0;index<10;index++){await navigation(page,'singleplayer');await navigation(page,'mygames');await navigation(page,'profile');await navigation(page,'discover');}
      const final=await page.locator('*').count();
      results.navigation={changes:40,initialLiveNodes:initial,finalLiveNodes:final};
      check('repeated navigation does not accumulate DOM nodes',final<=initial+20);
      await runtime(page,'after-repeated-navigation');
    }
    await page.evaluate(()=>{Object.defineProperty(document,'hidden',{configurable:true,get:()=>true});document.dispatchEvent(new Event('visibilitychange'));});
    check('hidden tab clears carousel/polling timers',await page.evaluate(()=>!Object.values(globalThis.__qa.intervals).some(item=>[6500,4000,1000].includes(item.delay))));
    if(newDesign)check('hidden tab pauses decorative animation',await page.locator('.cnm-runner').evaluateAll(nodes=>nodes.every(node=>getComputedStyle(node).animationPlayState==='paused'||getComputedStyle(node).animationName==='none')));
    if(newDesign)check('hidden tab pauses decorative child animation',await page.locator('.cnm-ambient').evaluate(node=>node.getAnimations({subtree:true}).every(animation=>animation.playState!=='running')));
    await page.context().close();

    page=await open({width:390,shell:true,data:{'chat/fixture-ad':{username:'Friend',type:'lobby_ad',text:'Looking for players!',timestamp:Date.now(),lobbyId:'friend-room',lobbyName:'Friend room',gameType:'cat',gameTitle:'Cat & Mouse',playerCount:1,maxPlayers:8,host:'Friend'}}});
    check('parent shell embeds the actual upgraded lobby',await page.frameLocator('#contentFrame').locator('#view-discover.active').isVisible());
    await page.locator('#toggleChat').click();check('parent chat opens',await page.locator('#chatContainer').getAttribute('aria-hidden')==='false');
    await page.locator('#closeChat').click();check('parent chat closes',await page.locator('#chatContainer').getAttribute('aria-hidden')==='true');
    check('parent chat guest composer remains account gated',await page.locator('#chatMessage').isDisabled());
    await page.frameLocator('#contentFrame').locator('#cnmFlagshipGrid [data-game-key="apestogetherstrong"] [data-game-action="single"]').click();
    await page.frameLocator('#contentFrame').locator('#launched').waitFor();check('parent iframe accepts original game navigation');
    await page.locator('#returnToLobby').click();await page.frameLocator('#contentFrame').locator('#cnmFlagshipGrid .game-card').first().waitFor();check('parent Lobby control returns from a game');
    await page.evaluate(()=>{globalThis.__qa.frameLoads=0;document.getElementById('contentFrame').addEventListener('load',()=>globalThis.__qa.frameLoads++);});
    await page.locator('#reloadLobby').click();await page.waitForFunction(()=>globalThis.__qa.frameLoads>0);check('parent Reload control reloads the embedded lobby');
    if(await page.evaluate(()=>document.fullscreenEnabled)){
      await page.locator('#fullscreenLobby').click();await page.waitForFunction(()=>document.fullscreenElement?.id==='contentFrame');check('parent Fullscreen control targets the game iframe');
      await page.evaluate(()=>document.exitFullscreen());await page.waitForFunction(()=>!document.fullscreenElement);
    }
    await page.evaluate(()=>{
      const toast=document.getElementById('toast');toast.textContent='Fixture notification preview';toast.classList.add('show');
      // Isolate the active visual style from asynchronous native fullscreen events.
      const button=document.getElementById('fullscreenLobby').cloneNode(true);button.id='qaFullscreenContrast';button.classList.add('fullscreen-active');button.style.cssText='position:fixed;left:-9999px;top:0;visibility:hidden;transition:none';document.body.appendChild(button);
    });
    await page.waitForTimeout(200);
    const contrast=async selector=>page.locator(selector).evaluate(node=>{
      const style=getComputedStyle(node),rgb=color=>(color.match(/[\d.]+/g)||[]).slice(0,3).map(Number),luminance=color=>rgb(color).map(value=>{value/=255;return value<=.04045?value/12.92:((value+.055)/1.055)**2.4;}).reduce((sum,value,index)=>sum+value*[.2126,.7152,.0722][index],0),a=luminance(style.backgroundColor),b=luminance(style.color);
      return {background:style.backgroundColor,color:style.color,ratio:(Math.max(a,b)+.05)/(Math.min(a,b)+.05)};
    });
    results.parentShell={toast:await contrast('#toast'),fullscreen:await contrast('#qaFullscreenContrast')};
    await page.locator('#qaFullscreenContrast').evaluate(node=>node.remove());
    check('parent notification text has readable contrast',results.parentShell.toast.ratio>=4.5);
    check('parent active fullscreen text has readable contrast',results.parentShell.fullscreen.ratio>=4.5);
    await page.locator('#toggleChat').click();await page.waitForSelector('.lobby-ad-name');check('parent chat renders existing lobby advertisements');
    await capture(page,'parent-shell-chat');await page.locator('#closeChat').click();
    await capture(page,'parent-shell');
    check('parent shell has no horizontal overflow',await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
    await page.context().close();
  }
  check('no uncaught browser errors',errors.length===0);
}catch(error){
  results.failure={message:error.message,stack:error.stack};
  const page=contexts.at(-1)?.pages()?.[0];
  if(page&&!page.isClosed())await page.screenshot({path:path.join(output,`${label}-failure.png`),fullPage:true}).catch(()=>{});
  process.exitCode=1;
}finally{
  results.pageErrors=errors;
  fs.writeFileSync(path.join(output,`${label}-results.json`),JSON.stringify(results,null,2));
  await Promise.allSettled(contexts.map(context=>context.close()));
  await browser.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));
  console.log(JSON.stringify({label,checks:results.checks.length,measurements:results.measurements.map(item=>({name:item.name,readyWallMs:item.readyWallMs,domNodes:item.domNodes,images:item.images.requests,imageBytes:item.images.transferBytes})),failure:results.failure||null,pageErrors:errors},null,2));
}
