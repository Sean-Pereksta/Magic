import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createRequire} from 'node:module';
import {STARTER_DECKS,starterDeck} from '../decks.js';
const require=createRequire(import.meta.url);
let chromium;try{({chromium}=require('playwright'));}catch{({chromium}=require(path.join(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES||'/opt/codex/runtimes/codex-primary-runtime/dependencies/node/node_modules','playwright')));}
const root=path.resolve(import.meta.dirname,'../../..');
const firebaseMocks={
'firebase-app.js':`export const initializeApp=()=>({});export const getApp=()=>({});export const getApps=()=>[];`,
'firebase-auth.js':`const auth={currentUser:{uid:localStorage.getItem('__forgeAuthUid')||'owner-uid',getIdToken:async()=> 'test-token'}};export const getAuth=()=>auth;export const signInAnonymously=async()=>({user:auth.currentUser});export const signInWithEmailAndPassword=async()=>({user:auth.currentUser});export const onAuthStateChanged=(a,fn)=>{setTimeout(()=>fn(a.currentUser),0);return ()=>{}};`,
'firebase-firestore.js':`const docs=new Map(JSON.parse(localStorage.getItem('__forgeMockDocs')||'[]'));export const getFirestore=()=>({});export const doc=(...parts)=>({path:parts.slice(1).join('/')});export const collection=doc;export const query=(...x)=>x;export const where=(...x)=>x;export const getDoc=async ref=>({exists:()=>docs.has(ref.path),data:()=>docs.get(ref.path)||{}});export const setDoc=(ref,data)=>{if(globalThis.__denySolo&&(ref.path.startsWith('factionForgeSoloProfiles/')||ref.path.includes('/factionForge/')))throw Object.assign(new Error('Denied'),{code:'permission-denied'});if(ref.path.startsWith('lobbies/'))globalThis.__fireWrites=(globalThis.__fireWrites||0)+1;else globalThis.__soloWrites=(globalThis.__soloWrites||0)+1;docs.set(ref.path,data);localStorage.setItem('__forgeMockDocs',JSON.stringify([...docs]));};export const updateDoc=setDoc;export const runTransaction=async(db,fn)=>fn({get:getDoc,set:setDoc,update:updateDoc});export const onSnapshot=(ref,fn)=>{setTimeout(()=>ref instanceof Array?fn({docs:[]}):fn({exists:()=>docs.has(ref.path),data:()=>docs.get(ref.path)||{}}),0);return ()=>{}};`
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

test('Sean reclaims the legacy Commander save without losing dust and other hub accounts load separately',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
  await context.addInitScript(()=>{
   if(!localStorage.getItem('__forgeMockDocs')){
    localStorage.setItem('gh.username','Sean');
    localStorage.setItem('__forgeMockDocs',JSON.stringify([['users/Sean',{username:'Sean'}],['users/Other',{username:'Other'}],['factionForgeSoloProfiles/owner-uid',{schemaVersion:1,uid:'owner-uid',username:'Commander',forgeDust:940,troops:{rifleman:{cards:75,forgeTier:2,unlocked:true},vguard:{cards:0,forgeTier:1,unlocked:true},medic:{cards:0,forgeTier:1,unlocked:true}},campaign:{missions:{'forge-01':{completed:true,stars:3,clears:4}},endlessUnlocked:false,endlessBest:0},wins:4,losses:1,rewardTarget:null,rewardDay:'',rewardCounts:{},updatedAt:1}]]));
   }
  });
  await context.route('**/game/crowncouncil.html*',async route=>{
   let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');
   html=html.replace('/* Fullscreen */','globalThis.__forgeAccount={get profile(){return forgeProfile},get username(){return username},get ui(){return expansionUI},get state(){return S},chooseCard,finishRound(){hostBattleState.units.forEach(unit=>{if(unit.p===1)unit.hp=0})}};\n/* Fullscreen */');
   await route.fulfill({contentType:'text/html',body:html});
  });
  await page.goto(url+'/game/crowncouncil.html?mode=solo&username=Commander');
  await page.waitForFunction(()=>__forgeAccount.profile?.uid&&__forgeAccount.profile.username==='Sean');
  assert.equal(await page.evaluate(()=>__forgeAccount.username),'Sean');
  assert.equal(await page.locator('.ff-player-summary strong').textContent(),'Sean');
  assert.equal(await page.evaluate(()=>__forgeAccount.profile.forgeDust),940);
  assert.equal(await page.evaluate(()=>__forgeAccount.profile.troops.rifleman.cards),75);
  assert.equal(await page.evaluate(()=>__forgeAccount.profile.campaign.missions['forge-01'].stars),3);
  await page.evaluate(()=>localStorage.setItem('gh.username','Other'));await page.reload();
  await page.waitForFunction(()=>__forgeAccount.profile?.uid&&__forgeAccount.profile.username==='Other');
  assert.equal(await page.evaluate(()=>__forgeAccount.profile.forgeDust),0);
  await page.evaluate(()=>localStorage.setItem('gh.username','Sean'));await page.reload();
  await page.waitForFunction(()=>__forgeAccount.profile?.uid&&__forgeAccount.profile.username==='Sean');
  assert.equal(await page.evaluate(()=>__forgeAccount.profile.forgeDust),940);
  await page.setViewportSize({width:390,height:844});
  await page.getByRole('button',{name:/The Forge/}).first().click();
  await page.getByRole('tab',{name:'Buy packs',exact:true}).click();
  await page.getByRole('button',{name:'Buy Vanir Reinforcements',exact:true}).click();
  await page.waitForFunction(()=>__forgeAccount.profile.forgeDust===890);
  assert.equal(await page.evaluate(()=>__forgeAccount.profile.troops.rifleman.cards),80);
  assert.equal(await page.evaluate(()=>__forgeAccount.profile.troops.vguard.cards),5);
  assert.equal(await page.evaluate(()=>__forgeAccount.profile.troops.medic.cards),5);
  await page.reload();await page.waitForFunction(()=>__forgeAccount.profile?.uid&&__forgeAccount.profile.forgeDust===890);
  await page.evaluate(()=>__forgeAccount.ui.showForge('rifleman'));await page.getByRole('button',{name:'Favor this troop in rewards',exact:true}).click();
  await page.waitForFunction(()=>__forgeAccount.profile.rewardTarget==='rifleman');await page.evaluate(()=>__forgeAccount.ui.showMenu());
  await page.getByRole('button',{name:'Practice against AI',exact:true}).click();await page.getByRole('button',{name:'Select and continue with Iron Vanguard',exact:true}).click();
  for(let index=0;index<3;index++){await page.waitForFunction(index=>__forgeAccount.state?.phase==='draft'&&__forgeAccount.state.draftIndex===index,index);await page.evaluate(()=>__forgeAccount.chooseCard(0));}
  await page.waitForFunction(()=>__forgeAccount.state.phase==='battle');await page.evaluate(()=>__forgeAccount.finishRound());
  await page.waitForFunction(()=>__forgeAccount.profile.troops.rifleman.cards===83);
  assert.equal(await page.evaluate(()=>__forgeAccount.state.lives[1]),3);
  await page.evaluate(()=>localStorage.setItem('__forgeAuthUid','replacement-anonymous-uid'));await page.reload();
  await page.waitForFunction(()=>__forgeAccount.profile?.uid==='replacement-anonymous-uid');
  assert.deepEqual(await page.evaluate(()=>({dust:__forgeAccount.profile.forgeDust,cards:__forgeAccount.profile.troops.rifleman.cards,tier:__forgeAccount.profile.troops.rifleman.forgeTier,stars:__forgeAccount.profile.campaign.missions['forge-01'].stars})),{dust:890,cards:83,tier:2,stars:3});
  assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('__forgeMockDocs')).some(([path])=>path.startsWith('lobbies/'))),false);
  await page.evaluate(()=>{const docs=JSON.parse(localStorage.getItem('__forgeMockDocs'));docs.push(['users/Guest',{username:'Guest'}],['users/Guest/factionForge/profile',{...__forgeAccount.profile,username:'Guest',forgeDust:999}]);localStorage.setItem('__forgeMockDocs',JSON.stringify(docs));localStorage.removeItem('gh.username');dispatchEvent(new Event('focus'));});
  await page.waitForFunction(()=>globalThis.__forgeAccount?.profile?.username==='Guest');
  assert.equal(await page.evaluate(()=>new URL(location.href).searchParams.has('username')),false);
  assert.equal(await page.evaluate(()=>__forgeAccount.profile.forgeDust),0);
  assert.deepEqual(errors,[]);await context.close();
 }finally{await browser.close();await new Promise(resolve=>app.close(resolve));}
});

test('profile selection failures stay visible and can reconnect without reloading',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
  await context.addInitScript(()=>{localStorage.setItem('gh.username','Sean');localStorage.setItem('__forgeMockDocs',JSON.stringify([['users/Sean',{username:'Sean'}],['factionForgeSoloProfiles/owner-uid',{schemaVersion:1,uid:'owner-uid',username:'Sean',forgeDust:73,troops:{},campaign:{missions:{},endlessUnlocked:false,endlessBest:0},wins:0,losses:0,rewardTarget:null,rewardDay:'',rewardCounts:{},updatedAt:1}]]));globalThis.__denySolo=true;});
  await context.route('**/game/crowncouncil.html*',async route=>{let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');html=html.replace('/* Fullscreen */','globalThis.__forgeRetry={get profile(){return forgeProfile},reconnect(){profileConnectionFailed=true;return restoreExpansionProfile(auth.currentUser)}};\n/* Fullscreen */');await route.fulfill({contentType:'text/html',body:html});});
  await page.goto(url+'/game/crowncouncil.html?mode=solo');
  await page.getByRole('button',{name:'Retry profile connection',exact:true}).waitFor();
  assert.match(await page.locator('.ff-connection').textContent(),/permissions need updating/);
  await page.evaluate(()=>globalThis.__denySolo=false);await page.getByRole('button',{name:'Retry profile connection',exact:true}).click();
  await page.getByText('Progress saved with Firebase',{exact:true}).waitFor();
  assert.equal(await page.locator('.ff-player-summary strong').textContent(),'Sean');
  assert.equal(await page.evaluate(()=>__forgeRetry.profile.forgeDust),73);
  assert.equal(await page.getByRole('button',{name:'Retry profile connection',exact:true}).count(),0);
  const error=await page.evaluate(()=>{globalThis.__denySolo=true;return __forgeRetry.reconnect().catch(error=>error.message);});
  assert.match(error,/updated Firestore rules/);assert.equal(await page.evaluate(()=>__forgeRetry.profile.forgeDust),73,'a denied reconnect keeps the last confirmed save visible');
  await page.evaluate(()=>globalThis.__denySolo=false);await page.getByRole('button',{name:'Retry profile connection',exact:true}).click();await page.getByText('Progress saved with Firebase',{exact:true}).waitFor();
  assert.equal(await page.evaluate(()=>__forgeRetry.profile.forgeDust),73);
  assert.deepEqual(errors,[]);await context.close();
 }finally{await browser.close();await new Promise(resolve=>app.close(resolve));}
});

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

test('solo ignores stale external progression configuration and multiplayer keeps its Firestore readiness gate',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),apiRequests=[];
  await context.route('**/faction-forge/config.js',route=>route.fulfill({contentType:'text/javascript',body:"export const FACTION_FORGE_CONFIG={assetBaseUrl:'',progressionApiUrl:'https://obsolete.invalid/api/faction-forge',matchmakingTimeoutMs:30000};"}));
  page.on('request',request=>{if(request.url().includes('/api/faction-forge'))apiRequests.push(request.url());});
  await page.goto(url+'/game/crowncouncil.html?mode=solo&gameId=old-forge-room');await page.getByText('Progress saved with Firebase',{exact:true}).waitFor();assert.equal(await page.locator('#assetGate').isVisible(),false);assert.deepEqual(apiRequests,[]);await context.close();
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
  await page.goto(url+'/game/crowncouncil.html');await page.getByRole('button',{name:'Practice against AI',exact:true}).click();await page.getByRole('button',{name:'Select and continue with Iron Vanguard',exact:true}).click();
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
   await page.goto(url+'/game/crowncouncil.html');await page.getByRole('button',{name:'Campaign',exact:false}).first().click();await page.locator('.ff-mission').first().click();await page.locator('.ff-deck-card').filter({hasText:faction}).getByRole('button',{name:/Select and continue with/}).click();
   await page.waitForFunction(()=>globalThis.__forgeTest?.state?.phase==='draft');
   const state=await page.evaluate(()=>({faction:__forgeTest.state.factions.p0,roster:__forgeTest.state.roster.p0,mission:__forgeTest.state.missionId}));
   assert.equal(state.faction,faction);assert.equal(state.mission,'forge-01');assert.ok(state.roster.length>=2,'campaign player has a starting army');
   for(let i=0;i<3;i++){await page.waitForFunction(i=>__forgeTest.state.draftIndex===i&&__forgeTest.state.phase==='draft',i);await page.evaluate(()=>__forgeTest.chooseCard(0));}
   await page.waitForFunction(()=>__forgeTest.state.phase==='battle');assert.equal(await page.evaluate(()=>globalThis.__fireWrites||0),0);assert.deepEqual(errors,[]);await context.close();
  }
 }finally{await browser.close();await new Promise(r=>app.close(r));}
});

test('local troop evolution and combo enhancements persist through battle, loser bonus and the next draft',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
  await context.route('**/game/crowncouncil.html*',async route=>{
   let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');
   html=html.replace('/* Fullscreen */',`globalThis.__forgeEvolutionTest={get state(){return S},get battle(){return B},
    prepare(){stopHostLoops();S.roster.p0=[{type:'vguard',count:1,lvl:1},{type:'vrocket',count:1,lvl:1},{type:'suppressor',count:1,lvl:1}];S.roster.p1=[{type:'defiler',count:1,lvl:1}];S.factions={p0:'Vanir',p1:'Helions'};S.decks={p0:FFDecks.starterDeck('Vanir'),p1:FFDecks.starterDeck('Helions')};S.evolutions={p0:{},p1:{}};S.comboEnhancements={p0:[],p1:[]};},
    async issue(card,phase,index=0,side='p0'){stopHostLoops();S.phase=phase;S.draftIndex=index;S.bonusFor=phase==='loser-bonus'?side:null;S.currentPacks={p0:[{type:'troop',key:'marksman',name:'Marksman'}],p1:[{type:'troop',key:'hellspawn',name:'Hell Spawn'}]};S.currentPacks[side]=[card];S.selections={p0:[],p1:[]};S.selections.p0[index]=0;S.selections.p1[index]=0;S.phaseEndsAt=Date.now()+60000;await processHostPhase();},
    finishRound(){for(const unit of hostBattleState.units)if(unit.p===0)unit.hp=0;}};\n/* Fullscreen */`);
   await route.fulfill({contentType:'text/html',body:html});
  });
  await page.goto(url+'/game/crowncouncil.html');await page.getByRole('button',{name:'Practice against AI',exact:true}).click();await page.getByRole('button',{name:'Select and continue with Iron Vanguard',exact:true}).click();
  await page.waitForFunction(()=>__forgeEvolutionTest.state?.phase==='draft');
  await page.evaluate(async()=>{const t=__forgeEvolutionTest;t.prepare();await t.issue({type:'evolution',key:'vrocket',for:'vrocket',path:'b'},'draft',0);});
  assert.equal(await page.evaluate(()=>__forgeEvolutionTest.state.evolutions.p0.vrocket),'b');
  assert.ok(await page.evaluate(()=>__forgeEvolutionTest.state.currentPacks.p0.every(card=>card.type!=='evolution'||card.key!=='vrocket')),'selected specialization is excluded from fresh offers');
  await page.evaluate(()=>__forgeEvolutionTest.issue({type:'combo',key:'suppression-bombardment'},'draft',1));
  await page.evaluate(()=>__forgeEvolutionTest.issue({type:'special',special:'upgrade',for:'vguard'},'draft',2));
  await page.waitForFunction(()=>__forgeEvolutionTest.state.phase==='battle');
  assert.deepEqual(await page.evaluate(()=>({path:__forgeEvolutionTest.state.evolutions.p0.vrocket,combos:__forgeEvolutionTest.state.comboEnhancements.p0,livePath:__forgeEvolutionTest.battle.units.find(unit=>unit.p===0&&unit.type==='vrocket')?.evolution})),{path:'b',combos:['suppression-bombardment'],livePath:'b'});
  await page.evaluate(()=>__forgeEvolutionTest.finishRound());await page.waitForFunction(()=>__forgeEvolutionTest.state.phase==='loser-bonus');
  await page.evaluate(()=>__forgeEvolutionTest.issue({type:'evolution',key:'vguard',for:'vguard',path:'b'},'loser-bonus'));
  assert.equal(await page.evaluate(()=>__forgeEvolutionTest.state.phase),'draft-after');
  for(let index=0;index<3;index++)await page.evaluate(index=>__forgeEvolutionTest.issue({type:'troop',key:'marksman',name:'Marksman'},'draft-after',index),index);
  await page.waitForFunction(()=>__forgeEvolutionTest.state.phase==='battle');
  const persisted=await page.evaluate(()=>({paths:__forgeEvolutionTest.state.evolutions.p0,combos:__forgeEvolutionTest.state.comboEnhancements.p0,live:__forgeEvolutionTest.battle.units.filter(unit=>unit.p===0&&['vguard','vrocket'].includes(unit.type)).map(unit=>({type:unit.type,path:unit.evolution}))}));
  assert.deepEqual(persisted.paths,{vrocket:'b',vguard:'b'});assert.deepEqual(persisted.combos,['suppression-bombardment']);assert.ok(persisted.live.every(unit=>unit.path==='b'));
  await page.evaluate(()=>__forgeEvolutionTest.issue({type:'evolution',key:'defiler',for:'defiler',path:'b'},'loser-bonus',0,'p1'));
  assert.equal(await page.evaluate(()=>__forgeEvolutionTest.state.evolutions.p1.defiler),'b','the opposing loser bonus writes to the correct side');
  assert.ok(await page.evaluate(()=>__forgeEvolutionTest.state.currentPacks.p1.every(card=>card.type!=='evolution'||card.key!=='defiler')));
  for(let index=0;index<3;index++)await page.evaluate(index=>__forgeEvolutionTest.issue({type:'troop',key:'marksman',name:'Marksman'},'draft-after',index),index);
  await page.waitForFunction(()=>__forgeEvolutionTest.state.phase==='battle');
  assert.equal(await page.evaluate(()=>__forgeEvolutionTest.battle.units.find(unit=>unit.p===1&&unit.type==='defiler')?.evolution),'b');
  assert.deepEqual(await page.evaluate(()=>__forgeEvolutionTest.state.evolutions.p0),{vrocket:'b',vguard:'b'});
  assert.equal(await page.evaluate(()=>globalThis.__fireWrites||0),0);assert.deepEqual(errors,[]);await context.close();
 }finally{await browser.close();await new Promise(resolve=>app.close(resolve));}
});

test('a real finishing missile or BloodBurst survives the logical round transition and then cleans up',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  for(const type of ['vrocket','blood']){
   const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
   // Read-only instrumentation exposes the actual pooled families so the test
   // can distinguish an impact explosion from unrelated living-unit effects.
   await context.route('**/faction-forge/effects.js',async route=>{
    const source=await fs.readFile(path.join(root,'game/faction-forge/effects.js'),'utf8');
    await route.fulfill({contentType:'text/javascript',body:source.replace('active:pool.active.length,free:','families:pool.active.map(effect=>({family:effect.family,start:effect.start,end:effect.start+effect.duration})),active:pool.active.length,free:')});
   });
   await context.route('**/game/crowncouncil.html*',async route=>{
    let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');
    html=html.replace('/* Fullscreen */',`globalThis.__forgeFinisherTest={get state(){return S},get renderer(){return spriteRenderer},
     async prepare(type){stopHostLoops();spriteRenderer.clear();spriteRenderer.configure({intensity:'high',reducedMotion:false,audio:false});S.phase='battle';S.lives=type==='blood'?[4,1]:[4,4];S.factions={p0:type==='blood'?'Helions':'Vanir',p1:'Helions'};S.decks={p0:FFDecks.starterDeck(S.factions.p0),p1:FFDecks.starterDeck('Helions')};S.roster={p0:[{type,count:1,lvl:1}],p1:[{type:'hellspawn',count:1,lvl:1}]};S.evolutions={p0:{},p1:{}};S.comboEnhancements={p0:[],p1:[]};const st={...S,field:fieldEmpty()};rebuildFieldFromRoster(st);const attacker=st.field.units.find(unit=>unit.p===0),target=st.field.units.find(unit=>unit.p===1);attacker.id='finishing-attacker';target.id='finishing-target';attacker.x=3;attacker.y=8;target.x=3;target.y=type==='blood'?7:5;attacker.dmg=20;target.hp=1;target.nextAtk=target.nextStep=Date.now()+60000;attacker.nextStep=Date.now()+60000;attacker.nextAtk=Date.now()+220;st.field.units=[attacker,target];st.field.startedAt=Date.now();applyHostBattleVisual(st.field);await runBattle(st.field);},
     snapshot(){return {phase:S.phase,bonusFor:S.bonusFor,at:performance.now(),deadline:spriteRenderer.impactDeadline('finishing-target'),stats:spriteRenderer.stats,overlayOpacity:getComputedStyle($('overlay')).opacity,cardVisibility:getComputedStyle($('cardRow')).visibility};}};
     const __finisherWrite=writeStatePatch;writeStatePatch=async function(patch){const result=await __finisherWrite(patch);if(['loser-bonus','game-over'].includes(patch?.phase)){const test=__forgeFinisherTest;test.transition=test.snapshot();setTimeout(()=>test.impact=test.snapshot(),300);setTimeout(()=>test.finished=test.snapshot(),1150);}return result;};\n/* Fullscreen */`);
    await route.fulfill({contentType:'text/html',body:html});
   });
   await page.goto(url+'/game/crowncouncil.html');await page.getByRole('button',{name:'Practice against AI',exact:true}).click();await page.getByRole('button',{name:'Select and continue with Iron Vanguard',exact:true}).click();
   await page.waitForFunction(()=>__forgeFinisherTest.state?.phase==='draft');
   await page.evaluate(type=>__forgeFinisherTest.prepare(type),type);
   await page.waitForFunction(()=>__forgeFinisherTest.transition);
   const ending=await page.evaluate(()=>__forgeFinisherTest.transition);
   assert.equal(ending.phase,type==='blood'?'game-over':'loser-bonus');if(type==='vrocket')assert.equal(ending.bonusFor,'p1');
   assert.ok(ending.stats.active>0,type+' final attack remains visible after logical transition');assert.ok(ending.deadline>ending.at,type+' target health/death is held until impact');
   await page.waitForFunction(()=>__forgeFinisherTest.impact);
   const impact=await page.evaluate(()=>__forgeFinisherTest.impact);
   assert.ok(impact.stats.families.some(effect=>effect.family==='explosion'&&effect.start<=impact.at&&effect.end>impact.at),type+' real explosion plays after the finishing hit');
   assert.equal(impact.overlayOpacity,'0');assert.equal(impact.cardVisibility,'hidden');
   await page.waitForFunction(()=>__forgeFinisherTest.finished);
   assert.equal(await page.evaluate(()=>__forgeFinisherTest.finished.stats.active),0,type+' finished effects are automatically cleared');
   assert.deepEqual(errors,[]);await context.close();
  }
 }finally{await browser.close();await new Promise(resolve=>app.close(resolve));}
});

test('Firebase-only campaign saves rewards, reloads mission unlocks, and applies a permanent Forge tier in battle',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[],apiRequests=[];page.on('pageerror',e=>errors.push(e.message));page.on('request',req=>{if(req.url().includes('/api/faction-forge'))apiRequests.push(req.url());});
  await context.route('**/game/crowncouncil.html*',async route=>{let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');html=html.replace('/* Fullscreen */','globalThis.__forgeTest={get state(){return S},get profile(){return forgeProfile},get battle(){return B},chooseCard,get ui(){return expansionUI},complete(){stopHostLoops();S.phase="game-over";S.winner=username;S.lives=[4,0];render();}};\n/* Fullscreen */');await route.fulfill({contentType:'text/html',body:html});});
  await page.goto(url+'/game/crowncouncil.html');await page.getByText('Progress saved with Firebase',{exact:true}).waitFor();
  // Favor one troop, then clear five missions: 30 cards and 100 dust afford Recruit -> Veteran.
  await page.evaluate(()=>__forgeTest.ui.showForge('marksman'));await page.getByRole('button',{name:'Favor this troop in rewards',exact:true}).click();await page.waitForFunction(()=>__forgeTest.profile.rewardTarget==='marksman');
  for(let mission=0;mission<5;mission++){
   await page.evaluate(()=>__forgeTest.ui.showCampaign());await page.locator('.ff-mission').nth(mission).click();await page.getByRole('button',{name:'Select and continue with Iron Vanguard',exact:true}).click();await page.waitForFunction(()=>__forgeTest.state?.phase==='draft');
   if(mission===0)await page.evaluate(()=>globalThis.__denySolo=true);
   await page.evaluate(()=>__forgeTest.complete());
   if(mission===0){await page.getByRole('button',{name:'Retry Firebase save',exact:true}).waitFor();assert.equal(await page.evaluate(()=>__forgeTest.profile.forgeDust),0);await page.evaluate(()=>globalThis.__denySolo=false);await page.getByRole('button',{name:'Retry Firebase save',exact:true}).click();}
   await page.getByText('Spoils of battle',{exact:true}).waitFor();
  }
  assert.equal(await page.evaluate(()=>__forgeTest.profile.forgeDust),100);
  await page.reload();await page.getByText('Progress saved with Firebase',{exact:true}).waitFor();assert.equal(await page.evaluate(()=>Object.keys(__forgeTest.profile.campaign.missions).length),5);
  await page.evaluate(()=>__forgeTest.ui.showForge('marksman'));await page.getByRole('button',{name:'Forge Veteran',exact:true}).click();await page.getByRole('button',{name:'Confirm upgrade',exact:true}).click();await page.waitForFunction(()=>__forgeTest.profile.troops.marksman.forgeTier===2);
  assert.equal(await page.evaluate(()=>__forgeTest.profile.forgeDust),0);assert.equal(await page.evaluate(()=>__forgeTest.profile.troops.marksman.cards),20);
  await page.reload();await page.getByText('Progress saved with Firebase',{exact:true}).waitFor();await page.evaluate(()=>__forgeTest.ui.showCampaign());assert.equal(await page.locator('.ff-mission:not(:disabled)').count(),6);await page.locator('.ff-mission').first().click();await page.getByRole('button',{name:'Select and continue with Iron Vanguard',exact:true}).click();await page.waitForFunction(()=>__forgeTest.state?.phase==='draft');assert.equal(await page.evaluate(()=>__forgeTest.state.forgeTiers.p0.marksman),2);
  for(let i=0;i<3;i++){await page.waitForFunction(i=>__forgeTest.state.draftIndex===i&&__forgeTest.state.phase==='draft',i);await page.evaluate(()=>__forgeTest.chooseCard(0));}await page.waitForFunction(()=>__forgeTest.state.phase==='battle');assert.ok(await page.evaluate(()=>__forgeTest.battle.units.some(unit=>unit.p===0&&unit.type==='marksman'&&unit.forgeTier===2)));
  assert.equal(await page.evaluate(()=>globalThis.__fireWrites||0),0);assert.deepEqual(apiRequests,[]);assert.deepEqual(errors,[]);
 }finally{await browser.close();await new Promise(r=>app.close(r));}
});

test('campaign deck selection waits for restored Auth and a delayed Firestore profile',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
  await context.route('**/firebase-auth.js',route=>route.fulfill({contentType:'text/javascript',body:"const auth={currentUser:null};export const getAuth=()=>auth;export const signInAnonymously=async()=>{throw Error('Do not replace restored account')};export const signInWithEmailAndPassword=async()=>{};export const onAuthStateChanged=(a,fn)=>{globalThis.__restoreAuth=()=>{a.currentUser={uid:'owner-uid'};fn(a.currentUser)};return ()=>{}};"}));
  await context.route('**/firebase-firestore.js',route=>route.fulfill({contentType:'text/javascript',body:firebaseMocks['firebase-firestore.js'].replace('export const runTransaction=async(db,fn)=>fn({get:getDoc,set:setDoc,update:updateDoc});','export const runTransaction=async(db,fn)=>{if(!globalThis.__profileReleased)await new Promise(resolve=>{globalThis.__releaseProfile=()=>{globalThis.__profileReleased=true;resolve()}});return fn({get:getDoc,set:setDoc,update:updateDoc});};')}));
  await context.route('**/game/crowncouncil.html*',async route=>{let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');html=html.replace('/* Fullscreen */','globalThis.__forgeEntry={get state(){return S},get profile(){return forgeProfile}};\n/* Fullscreen */');await route.fulfill({contentType:'text/html',body:html});});
  await page.goto(url+'/game/crowncouncil.html?mode=solo');await page.getByRole('button',{name:'Campaign',exact:false}).first().click();await page.locator('.ff-mission').first().click();await page.getByRole('button',{name:'Select and continue with Iron Vanguard',exact:true}).click();
  assert.equal(await page.evaluate(()=>__forgeEntry.state),null);assert.doesNotMatch(await page.locator('.ff-status').first().textContent(),/still connecting|Try starting/);
  await page.evaluate(()=>__restoreAuth());await page.waitForFunction(()=>typeof __releaseProfile==='function');assert.equal(await page.evaluate(()=>__forgeEntry.state),null);
  await page.evaluate(()=>__releaseProfile());await page.waitForFunction(()=>__forgeEntry.state?.phase==='draft');assert.equal(await page.evaluate(()=>__forgeEntry.state.soloProgress),true);assert.equal(await page.evaluate(()=>__forgeEntry.profile.uid),'owner-uid');assert.deepEqual(errors,[]);
 }finally{await browser.close();await new Promise(resolve=>app.close(resolve));}
});

test('restored durable auth survives reload and Firestore lobbies receive the saved profile name',async()=>{
 const {app,url}=await server();const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
  const context=await browser.newContext();await mockBrowser(context);const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
  await context.addInitScript(()=>{localStorage.setItem('gh.username','SavedCommander');localStorage.setItem('__forgeMockDocs',JSON.stringify([['users/SavedCommander',{username:'SavedCommander'}],['lobbies/open-room',{name:'Open Forge',gameType:'crowncouncil',status:'waiting',visibility:'public',players:['Host'],host:'Host',hostUid:'host-uid',members:{'host-uid':'Host'},forgeRanked:true}]]));});
  await context.route('**/firebase-auth.js',route=>route.fulfill({contentType:'text/javascript',body:"const durable={uid:'owner-uid'},auth={currentUser:null};export const getAuth=()=>auth;export const signInAnonymously=async()=>{globalThis.__anonymousCalls=(globalThis.__anonymousCalls||0)+1;auth.currentUser={uid:'anonymous'};return {user:auth.currentUser};};export const signInWithEmailAndPassword=async()=>({user:durable});export const onAuthStateChanged=(a,fn)=>{setTimeout(()=>{a.currentUser=durable;fn(a.currentUser)},100);return ()=>{};};"}));
  const room={name:'Open Forge',gameType:'crowncouncil',status:'waiting',visibility:'public',players:['Host'],host:'Host',hostUid:'host-uid',members:{'host-uid':'Host'},forgeRanked:true};
  await context.route('**/firebase-firestore.js',route=>route.fulfill({contentType:'text/javascript',body:firebaseMocks['firebase-firestore.js'].replace('ref instanceof Array?fn({docs:[]})','ref instanceof Array?fn({docs:[{id:"open-room",data:()=>('+JSON.stringify(room)+')}]})')}));
  await context.route('**/lobby/lobby.html?forgeJoin=*',route=>route.fulfill({contentType:'text/html',body:'<p>Lobby destination</p>'}));
  await page.goto(url+'/game/crowncouncil.html');await page.getByText('Progress saved with Firebase',{exact:true}).waitFor();assert.equal(await page.evaluate(()=>globalThis.__anonymousCalls||0),0);
  await page.reload();await page.getByText('Progress saved with Firebase',{exact:true}).waitFor();assert.equal(await page.evaluate(()=>globalThis.__anonymousCalls||0),0);
  await page.getByRole('button',{name:'Play Online',exact:false}).first().click();await page.getByRole('button',{name:'Select and continue with Iron Vanguard',exact:true}).click();await page.getByRole('button',{name:'Join',exact:true}).click();await page.waitForURL('**/lobby/lobby.html?forgeJoin=open-room');
  assert.deepEqual(await page.evaluate(()=>({name:localStorage.getItem('gh.username'),room:sessionStorage.getItem('ff.handoffLobby')})),{name:'SavedCommander',room:'open-room'});assert.deepEqual(errors,[]);
 }finally{await browser.close();await new Promise(resolve=>app.close(resolve));}
});
