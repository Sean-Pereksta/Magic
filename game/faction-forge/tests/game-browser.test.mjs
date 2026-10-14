import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
let chromium;try{({chromium}=require('playwright'));}catch{({chromium}=require(path.join(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES||'/opt/codex/runtimes/codex-primary-runtime/dependencies/node/node_modules','playwright')));}
const root=path.resolve(import.meta.dirname,'../../..');
const firebaseMocks={
'firebase-app.js':`export const initializeApp=()=>({});export const getApp=()=>({});export const getApps=()=>[];`,
'firebase-auth.js':`const auth={currentUser:{uid:'owner-uid',getIdToken:async()=> 'test-token'}};export const getAuth=()=>auth;export const signInAnonymously=async()=>({user:auth.currentUser});export const signInWithEmailAndPassword=async()=>({user:auth.currentUser});export const onAuthStateChanged=(a,fn)=>{setTimeout(()=>fn(a.currentUser),0);return ()=>{}};`,
'firebase-firestore.js':`const docs=new Map(JSON.parse(localStorage.getItem('__forgeMockDocs')||'[]'));export const getFirestore=()=>({});export const doc=(...parts)=>({path:parts.slice(1).join('/')});export const collection=doc;export const query=(...x)=>x;export const where=(...x)=>x;export const getDoc=async ref=>({exists:()=>docs.has(ref.path),data:()=>docs.get(ref.path)||{}});export const setDoc=(ref,data)=>{if(globalThis.__denySolo&&ref.path.startsWith('factionForgeSoloProfiles/'))throw Object.assign(new Error('Denied'),{code:'permission-denied'});if(ref.path.startsWith('lobbies/'))globalThis.__fireWrites=(globalThis.__fireWrites||0)+1;else globalThis.__soloWrites=(globalThis.__soloWrites||0)+1;docs.set(ref.path,data);localStorage.setItem('__forgeMockDocs',JSON.stringify([...docs]));};export const updateDoc=setDoc;export const runTransaction=async(db,fn)=>fn({get:getDoc,set:setDoc,update:updateDoc});export const onSnapshot=(ref,fn)=>{setTimeout(()=>ref instanceof Array?fn({docs:[]}):fn({exists:()=>docs.has(ref.path),data:()=>docs.get(ref.path)||{}}),0);return ()=>{}};`
};
async function server(){
 const app=http.createServer(async(req,res)=>{try{const pathname=new URL(req.url,'http://localhost').pathname;const file=path.join(root,pathname);if(!file.startsWith(root+path.sep))throw Error('bad path');const data=await fs.readFile(file);res.setHeader('Content-Type',file.endsWith('.html')?'text/html':file.endsWith('.css')?'text/css':'text/javascript');res.end(data);}catch{res.writeHead(404);res.end('missing');}});
 await new Promise(r=>app.listen(0,'127.0.0.1',r));return {app,url:'http://127.0.0.1:'+app.address().port};
}
async function mockBrowser(context){
 await context.route('https://www.gstatic.com/firebasejs/**',route=>{const name=new URL(route.request().url()).pathname.split('/').at(-1);return route.fulfill({contentType:'text/javascript',body:firebaseMocks[name]||''});});
 await context.route('**/graphics/**',route=>route.fulfill({status:404,body:''}));
 await context.route('**/faction-forge/config.js',route=>route.fulfill({contentType:'text/javascript',body:`export const FACTION_FORGE_CONFIG={assetBaseUrl:'',progressionApiUrl:'',matchmakingTimeoutMs:30000};`}));
}

test('solo menu appears before auth and save loading, without opening any lobby',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  for(const scenario of ['auth-pending','auth-failed','profile-pending','profile-denied']){
   const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
   await context.addInitScript(()=>{localStorage.setItem('gh.lastLobbyId','old-forge-room');localStorage.setItem('gh.lastGameType','crowncouncil');});
   if(scenario.startsWith('auth-'))await context.route('**/firebase-auth.js',route=>route.fulfill({contentType:'text/javascript',body:`const auth={currentUser:null};export const getAuth=()=>auth;export const signInAnonymously=async()=>{throw Error('Authentication unavailable')};export const signInWithEmailAndPassword=async()=>{};export const onAuthStateChanged=(a,fn)=>{${scenario==='auth-failed'?'setTimeout(()=>fn(null),0);':''}return ()=>{}};`}));
   if(scenario==='profile-pending')await context.route('**/firebase-firestore.js',route=>route.fulfill({contentType:'text/javascript',body:firebaseMocks['firebase-firestore.js'].replace('export const runTransaction=async(db,fn)=>fn({get:getDoc,set:setDoc,update:updateDoc});','export const runTransaction=()=>new Promise(()=>{});')}));
   if(scenario==='profile-denied')await context.addInitScript(()=>globalThis.__denySolo=true);
   await context.route('**/game/crowncouncil.html*',async route=>{let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');html=html.replace('/* Fullscreen */','globalThis.__forgeEntry={get gameId(){return gameId},get mode(){return executionMode},get listeners(){return listenersStarted}};\n/* Fullscreen */');await route.fulfill({contentType:'text/html',body:html});});
   const query=scenario==='auth-pending'?'':'?mode=solo&gameId=old-forge-room';
   await page.goto(url+'/game/crowncouncil.html'+query);await page.getByRole('heading',{name:'FACTION FORGE',exact:true}).waitFor({timeout:5000});
   assert.equal(await page.locator('#assetGate').isVisible(),false,scenario);assert.deepEqual(await page.evaluate(()=>({id:__forgeEntry.gameId,mode:__forgeEntry.mode,listeners:__forgeEntry.listeners})),{id:null,mode:'menu',listeners:false});
   await page.getByRole('button',{name:'How to Play',exact:false}).click();await page.getByRole('heading',{name:'Command the Forge',exact:true}).waitFor();assert.equal(await page.locator('#assetGate').isVisible(),false);assert.equal(await page.evaluate(()=>globalThis.__fireWrites||0),0);assert.deepEqual(errors,[]);await context.close();
  }
 }finally{await browser.close();await new Promise(resolve=>app.close(resolve));}
});

test('solo entry offers an old online session for explicit resume and multiplayer entry keeps the readiness gate',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),sessionRequests=[];
  await context.route('**/faction-forge/config.js',route=>route.fulfill({contentType:'text/javascript',body:`export const FACTION_FORGE_CONFIG={assetBaseUrl:'',progressionApiUrl:'${url}/api/faction-forge',matchmakingTimeoutMs:30000};`}));
  await context.route('**/api/faction-forge/**',route=>{const p=new URL(route.request().url()).pathname;if(p.includes('/sessions/'))sessionRequests.push(p);return route.fulfill({contentType:'application/json',body:JSON.stringify({profile:{uid:'owner-uid',username:'Commander',activeSessionId:'old-online-match',troops:{},campaign:{missions:{}}}})});});
  await page.goto(url+'/game/crowncouncil.html?mode=solo&gameId=old-forge-room');await page.getByRole('button',{name:'Resume match',exact:true}).waitFor();assert.equal(await page.locator('#assetGate').isVisible(),false);assert.deepEqual(sessionRequests,[]);await context.close();
  const multi=await browser.newContext();await mockBrowser(multi);const game=await multi.newPage();
  await multi.route('**/game/crowncouncil.html*',async route=>{let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');html=html.replace('/* Fullscreen */','globalThis.__forgeEntry={get mode(){return executionMode},get listeners(){return listenersStarted},get gameId(){return gameId}};\n/* Fullscreen */');await route.fulfill({contentType:'text/html',body:html});});
  await game.goto(url+'/game/crowncouncil.html?mode=multi&gameId=real-room');await game.waitForFunction(()=>globalThis.__forgeEntry?.listeners);assert.deepEqual(await game.evaluate(()=>({id:__forgeEntry.gameId,mode:__forgeEntry.mode})),{id:'real-room',mode:'legacy-online'});assert.equal(await game.locator('#assetGate').isVisible(),true);assert.equal(await game.locator('.ff-view-menu').count(),0);await multi.close();
 }finally{await browser.close();await new Promise(resolve=>app.close(resolve));}
});

test('full game opens solo menu, drafts against AI, and moves on the original grid without Firebase writes',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await context.route('**/game/crowncouncil.html*',async route=>{let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');html=html.replace('/* Fullscreen */','globalThis.__forgeTest={get state(){return S},get battle(){return B},chooseCard,chooseFaction,startPractice,returnToForgeMenu};\n/* Fullscreen */');await route.fulfill({contentType:'text/html',body:html});});
  await page.goto(url+'/game/crowncouncil.html');await page.getByRole('button',{name:'Practice against AI',exact:true}).click();await page.getByRole('button',{name:/Vanir Industrial/}).click();
  await page.waitForFunction(()=>globalThis.__forgeTest?.state?.phase==='draft');
  assert.equal(await page.locator('#cardRow .card').count(),3);
  const info=page.locator('#cardRow .cardInfoBtn').first();await info.click();assert.equal(await page.locator('#cardInfoModal').isVisible(),true);assert.equal(await page.evaluate(()=>__forgeTest.state.selections.p0[0]??null),null);await page.locator('.cardInfoClose').click();
  for(let i=0;i<3;i++){await page.waitForFunction(i=>__forgeTest.state.draftIndex===i&&__forgeTest.state.phase==='draft',i);await page.evaluate(()=>__forgeTest.chooseCard(0));}
  await page.waitForFunction(()=>__forgeTest.state.phase==='battle',{timeout:20000});
  const before=await page.evaluate(()=>__forgeTest.battle.units.map(u=>({id:u.id,x:u.x,y:u.y})));
  await page.waitForFunction(previous=>__forgeTest.battle.units.some(u=>previous.find(p=>p.id===u.id&&(p.x!==u.x||p.y!==u.y))),before,{timeout:10000});
  assert.ok(await page.locator('.vu').count()>0);assert.equal(await page.evaluate(()=>globalThis.__fireWrites||0),0);assert.deepEqual(errors,[]);
  await page.setViewportSize({width:390,height:844});await page.getByRole('button',{name:'Menu',exact:true}).click();await page.getByRole('button',{name:/The Forge/}).first().click();assert.equal(await page.locator('.ff-collection-card').count(),32);assert.equal(await page.locator('.ff-locked-card').count(),6);
 }finally{await browser.close();await new Promise(r=>app.close(r));}
});

test('local campaign starts with each faction army and reaches battle without progression service',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  for(const faction of ['Vanir','Aesir','Helions']){
   const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
   await context.route('**/game/crowncouncil.html*',async route=>{let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');html=html.replace('/* Fullscreen */','globalThis.__forgeTest={get state(){return S},chooseCard};\n/* Fullscreen */');await route.fulfill({contentType:'text/html',body:html});});
   await page.goto(url+'/game/crowncouncil.html');await page.getByRole('button',{name:'Campaign',exact:false}).first().click();await page.locator('.ff-mission').first().click();await page.locator('.ff-faction-choice').filter({hasText:faction}).click();
   await page.waitForFunction(()=>globalThis.__forgeTest?.state?.phase==='draft');
   const state=await page.evaluate(()=>({faction:__forgeTest.state.factions.p0,roster:__forgeTest.state.roster.p0,mission:__forgeTest.state.missionId}));
   assert.equal(state.faction,faction);assert.equal(state.mission,'forge-01');assert.ok(state.roster.length>=2,'campaign player has a starting army');
   for(let i=0;i<3;i++){await page.waitForFunction(i=>__forgeTest.state.draftIndex===i&&__forgeTest.state.phase==='draft',i);await page.evaluate(()=>__forgeTest.chooseCard(0));}
   await page.waitForFunction(()=>__forgeTest.state.phase==='battle');assert.equal(await page.evaluate(()=>globalThis.__fireWrites||0),0);assert.deepEqual(errors,[]);await context.close();
  }
 }finally{await browser.close();await new Promise(r=>app.close(r));}
});

test('Firebase-only campaign saves rewards, reloads mission unlocks, and applies a permanent Forge tier in battle',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[],apiRequests=[];page.on('pageerror',e=>errors.push(e.message));page.on('request',req=>{if(req.url().includes('/api/faction-forge'))apiRequests.push(req.url());});
  await context.route('**/game/crowncouncil.html*',async route=>{let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');html=html.replace('/* Fullscreen */','globalThis.__forgeTest={get state(){return S},get profile(){return forgeProfile},get battle(){return B},chooseCard,get ui(){return expansionUI},complete(){stopHostLoops();S.phase="game-over";S.winner=username;S.lives=[4,0];render();}};\n/* Fullscreen */');await route.fulfill({contentType:'text/html',body:html});});
  await page.goto(url+'/game/crowncouncil.html');await page.getByText('Progress saved with Firebase',{exact:true}).waitFor();
  // Favor one troop, then clear five missions: 15 cards and 100 dust afford Recruit -> Veteran.
  await page.evaluate(()=>__forgeTest.ui.showForge('rifleman'));await page.getByRole('button',{name:'Favor this troop in rewards',exact:true}).click();await page.waitForFunction(()=>__forgeTest.profile.rewardTarget==='rifleman');
  for(let mission=0;mission<5;mission++){
   await page.evaluate(()=>__forgeTest.ui.showCampaign());await page.locator('.ff-mission').nth(mission).click();await page.locator('.ff-faction-choice').filter({hasText:'Vanir'}).click();await page.waitForFunction(()=>__forgeTest.state?.phase==='draft');
   if(mission===0)await page.evaluate(()=>globalThis.__denySolo=true);
   await page.evaluate(()=>__forgeTest.complete());
   if(mission===0){await page.getByRole('button',{name:'Retry Firebase save',exact:true}).waitFor();assert.equal(await page.evaluate(()=>__forgeTest.profile.forgeDust),0);await page.evaluate(()=>globalThis.__denySolo=false);await page.getByRole('button',{name:'Retry Firebase save',exact:true}).click();}
   await page.getByText('Spoils of battle',{exact:true}).waitFor();
  }
  assert.equal(await page.evaluate(()=>__forgeTest.profile.forgeDust),100);
  await page.reload();await page.getByText('Progress saved with Firebase',{exact:true}).waitFor();assert.equal(await page.evaluate(()=>Object.keys(__forgeTest.profile.campaign.missions).length),5);
  await page.evaluate(()=>__forgeTest.ui.showForge('rifleman'));await page.getByRole('button',{name:'Forge Veteran',exact:true}).click();await page.getByRole('button',{name:'Confirm upgrade',exact:true}).click();await page.waitForFunction(()=>__forgeTest.profile.troops.rifleman.forgeTier===2);
  assert.equal(await page.evaluate(()=>__forgeTest.profile.forgeDust),0);assert.equal(await page.evaluate(()=>__forgeTest.profile.troops.rifleman.cards),5);
  await page.reload();await page.getByText('Progress saved with Firebase',{exact:true}).waitFor();await page.evaluate(()=>__forgeTest.ui.showCampaign());assert.equal(await page.locator('.ff-mission:not(:disabled)').count(),6);await page.locator('.ff-mission').first().click();await page.locator('.ff-faction-choice').filter({hasText:'Vanir'}).click();await page.waitForFunction(()=>__forgeTest.state?.phase==='draft');assert.equal(await page.evaluate(()=>__forgeTest.state.forgeTiers.p0.rifleman),2);
  for(let i=0;i<3;i++){await page.waitForFunction(i=>__forgeTest.state.draftIndex===i&&__forgeTest.state.phase==='draft',i);await page.evaluate(()=>__forgeTest.chooseCard(0));}await page.waitForFunction(()=>__forgeTest.state.phase==='battle');assert.ok(await page.evaluate(()=>__forgeTest.battle.units.some(unit=>unit.p===0&&unit.type==='rifleman'&&unit.forgeTier===2)));
  assert.equal(await page.evaluate(()=>globalThis.__fireWrites||0),0);assert.deepEqual(apiRequests,[]);assert.deepEqual(errors,[]);
 }finally{await browser.close();await new Promise(r=>app.close(r));}
});

test('configured secure campaign submits only legal pick inputs and renders the authoritative result',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[],picks=[];page.on('pageerror',e=>errors.push(e.message));
  const profile={uid:'owner-uid',username:'Commander',crowncouncilElo:1300,forgeDust:0,troops:{rifleman:{forgeTier:2,cards:4,unlocked:true}},campaign:{missions:{}}};
  const card={type:'troop',key:'rifleman',name:'Riflemen',role:'back'};
  let session={id:'secure-one',mode:'campaign',revision:1,phase:'draft',round:1,draftIndex:0,meIndex:0,players:['Commander','IronWarden'],factions:{p0:'Vanir',p1:'Vanir'},roster:{p0:[],p1:[]},relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]},formations:{p0:'balanced',p1:'balanced'},lives:[4,1],mmr:[1300,1300],currentPacks:{p0:[card,card,card],p1:[]},deadlineAt:Date.now()+60000};
  await context.route('**/faction-forge/config.js',route=>route.fulfill({contentType:'text/javascript',body:`export const FACTION_FORGE_CONFIG={assetBaseUrl:'',progressionApiUrl:'${url}/api/faction-forge',matchmakingTimeoutMs:20};`}));
  await context.route('**/api/faction-forge/**',async route=>{
    const req=route.request(),pathname=new URL(req.url()).pathname;
    if(pathname.endsWith('/pick')){const body=req.postDataJSON();picks.push(body);session={...session,revision:2,phase:'game-over',winner:'Commander',winnerSide:0,mmrChange:[0,0]};profile.forgeDust=20;profile.campaign.missions['forge-01']={completed:true,stars:3};}
    const data=pathname.endsWith('/profile')?{profile}:{session,profile,receipt:session.phase==='game-over'?{dust:20,cards:[{troopKey:'rifleman',quantity:3}],stars:3}:undefined};
    await route.fulfill({contentType:'application/json',body:JSON.stringify(data)});
  });
  await page.goto(url+'/game/crowncouncil.html');await page.getByRole('button',{name:'Campaign',exact:false}).first().click();await page.locator('.ff-mission').first().click();await page.getByRole('button',{name:/Vanir Industrial/}).click();await page.waitForFunction(()=>document.querySelector('#cardRow')?.style.display==='flex');
  await page.locator('#cardRow .card').first().click();await page.getByRole('heading',{name:'Victory',exact:true}).waitFor();
  assert.equal(picks.length,1);assert.equal(picks[0].index,0);assert.equal(picks[0].sequence,1);assert.deepEqual(Object.keys(picks[0]).sort(),['index','requestId','sequence']);assert.equal(await page.evaluate(()=>globalThis.__fireWrites||0),0);assert.deepEqual(errors,[]);
 }finally{await browser.close();await new Promise(r=>app.close(r));}
});

test('restored durable auth survives reload and joined lobbies receive the verified profile name on a fresh device',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[],tokens=[];page.on('pageerror',e=>errors.push(e.message));
  await context.route('**/firebase-auth.js',route=>route.fulfill({contentType:'text/javascript',body:`
   const durable={uid:'owner-uid',getIdToken:async()=> 'durable-token'},auth={currentUser:null};
   export const getAuth=()=>auth;
   export const signInAnonymously=async()=>{globalThis.__anonymousCalls=(globalThis.__anonymousCalls||0)+1;auth.currentUser={uid:'anonymous',getIdToken:async()=> 'anonymous-token'};return {user:auth.currentUser};};
   export const signInWithEmailAndPassword=async()=>({user:durable});
   export const onAuthStateChanged=(a,fn)=>{setTimeout(()=>{a.currentUser=a.currentUser||durable;fn(a.currentUser)},100);return ()=>{};};
  `}));
  await context.route('**/firebase-firestore.js',route=>route.fulfill({contentType:'text/javascript',body:`
   const room={name:'Open Forge',gameType:'crowncouncil',status:'waiting',visibility:'public',players:['Host'],host:'Host',hostUid:'host-uid',members:{'host-uid':'Host'},forgeRanked:true};
   export const getFirestore=()=>({});export const doc=(...parts)=>({path:parts.slice(1).join('/')});export const collection=doc;export const query=(...x)=>x;export const where=(...x)=>x;
   export const getDoc=async ref=>({exists:()=>ref.path==='lobbies/open-room',data:()=>ref.path==='lobbies/open-room'?room:{crowncouncilElo:1250}});
   export const setDoc=async()=>{};export const updateDoc=setDoc;export const runTransaction=async(db,fn)=>fn({get:getDoc,set:setDoc,update:updateDoc});
   export const onSnapshot=(ref,fn)=>{setTimeout(()=>ref instanceof Array?fn({docs:[{id:'open-room',data:()=>room}]}):fn({exists:()=>false}),0);return ()=>{};};
  `}));
  await context.route('**/faction-forge/config.js',route=>route.fulfill({contentType:'text/javascript',body:`export const FACTION_FORGE_CONFIG={assetBaseUrl:'',progressionApiUrl:'${url}/api/faction-forge',matchmakingTimeoutMs:30000};`}));
  await context.route('**/api/faction-forge/profile',async route=>{tokens.push(route.request().headers().authorization);await route.fulfill({contentType:'application/json',body:JSON.stringify({profile:{uid:'owner-uid',username:'VerifiedCommander',crowncouncilElo:1300,forgeDust:0,troops:{},campaign:{missions:{}}}})});});
  await context.route('**/lobby/lobby.html?forgeJoin=*',route=>route.fulfill({contentType:'text/html',body:'<p>Lobby destination</p>'}));
  await page.goto(url+'/game/crowncouncil.html');await page.getByText('Profile connected',{exact:true}).waitFor();assert.equal(await page.evaluate(()=>globalThis.__anonymousCalls||0),0);assert.equal(await page.evaluate(()=>localStorage.getItem('gh.username')),null);
  await page.reload();await page.getByText('Profile connected',{exact:true}).waitFor();assert.equal(await page.evaluate(()=>globalThis.__anonymousCalls||0),0);assert.deepEqual(tokens,['Bearer durable-token','Bearer durable-token']);
  await page.getByRole('button',{name:'Play Online',exact:false}).first().click();await page.getByRole('button',{name:'Join',exact:true}).click();await page.waitForURL('**/lobby/lobby.html?forgeJoin=open-room');
  assert.deepEqual(await page.evaluate(()=>({name:localStorage.getItem('gh.username'),room:sessionStorage.getItem('ff.handoffLobby')})),{name:'VerifiedCommander',room:'open-room'});assert.deepEqual(errors,[]);
 }finally{await browser.close();await new Promise(r=>app.close(r));}
});






