const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const repo=path.resolve(__dirname,'../../../..');

const appModule=`export const getApps=()=>[{}],getApp=()=>({}),initializeApp=()=>({});`;
const authModule=`
export const getAuth=()=>window.sdk.auth;
export async function signInAnonymously(){window.sdk.signIns++;}
export function onAuthStateChanged(auth,next){const row={active:true,next};window.sdk.authListeners.push(row);queueMicrotask(()=>{if(row.active)next(auth.currentUser);});return()=>{if(row.active){row.active=false;window.sdk.authUnsubscribes++;}};}
`;
const storeModule=`
export const getFirestore=()=>({});
export const doc=(db,...parts)=>({path:parts.join('/')});
export const collection=doc,orderBy=(field,direction)=>({field,direction}),limit=count=>({count}),query=(ref,...clauses)=>({...ref,clauses});
export async function getDocs(ref){return {docs:Object.entries(window.sdk.accounts).filter(([path])=>path.startsWith(ref.path+'/')).map(([path,data])=>({id:path.split('/').at(-1),data:()=>structuredClone(data)}))};}
export async function getDoc(ref){window.sdk.reads.push(ref.path);return window.sdk.snapshot(ref.path);}
export async function deleteDoc(ref){delete window.sdk.accounts[ref.path];}
export function onSnapshot(ref,options,next,error){if(typeof options==='function'){error=next;next=options;options={}}const row={path:ref.path,options,next,error,active:true};window.sdk.docListeners.push(row);queueMicrotask(()=>{if(row.active)next(window.sdk.snapshot(row.path));});return()=>{if(row.active){row.active=false;window.sdk.docUnsubscribes++;}};}
export async function runTransaction(db,operation){
 const row={reads:[],updates:[],sets:[]};window.sdk.transactions.push(row);
 if(window.sdk.blockTransactions)await new Promise(resolve=>window.sdk.releaseTransactions.push(resolve));
 if(window.sdk.failTransactions)throw Object.assign(new Error('Missing permissions'),{code:'permission-denied'});
 const value=await operation({get:async ref=>{row.reads.push(ref.path);return window.sdk.snapshot(ref.path);},update(ref,patch){row.updates.push({path:ref.path,patch:structuredClone(patch)});},set(ref,data){row.sets.push({path:ref.path,data:structuredClone(data)});}});
 for(const {path,patch} of row.updates){const account=window.sdk.accounts[path];for(const [field,value] of Object.entries(patch)){const segments=field.split('.');let target=account;for(const segment of segments.slice(0,-1))target=target[segment]||=( {} );target[segments.at(-1)]=structuredClone(value);}}
 for(const {path,data} of row.sets)window.sdk.accounts[path]=data;
 return value;
}
`;
const fixture=menu=>`<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="overlay"></div><script type="module">
import * as profile from '/game/turncraft/shattered-front/profile.mjs';
window.profile=profile;window.profileEvents=[];window.stopFirst=profile.watchProfile(value=>profileEvents.push(structuredClone(value)));
${menu?`const {CATALOG}=await import('/game/turncraft/shattered-front/campaign.mjs');window.TurncraftExpansion={catalog:CATALOG,getFactions:()=>[{id:'frontier',name:'Frontier Directorate',strengths:'Range',color:'#73cfff'}],getUnits:()=>[],getIdentity:()=>({lobbyId:''}),setProfile:()=>{}};await import('/game/turncraft/shattered-front/menu.mjs');`:''}
window.fixtureReady=true;
</script>`;

async function setup({deferAuth=false,menu=false,queued=true}={}){
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']}),page=await browser.newPage(),errors=[],apiRequests=[];
 page.on('pageerror',error=>errors.push(error.message));page.on('request',request=>{if(new URL(request.url()).pathname.startsWith('/api/'))apiRequests.push(request.url());});
 await page.addInitScript(({deferAuth,menu,queued})=>{
  const account=(wave)=>({avatar:'',gameStats:{anotherGame:{wins:41},turncraft:{mmr:1200,rankedMatches:9,survival:{highestWave:wave,recordsByMap:{}},campaign:{completedMissions:{}},achievements:{}}}});
  window.sdk={accounts:{'users/AccountA':account(17),'users/AccountB':account(4)},docListeners:[],authListeners:[],docUnsubscribes:0,authUnsubscribes:0,signIns:0,reads:[],transactions:[],releaseTransactions:[],blockTransactions:menu&&queued,fetchCalls:[],intervalCalls:0,handlers:{storage:new Set(),focus:new Set(),visibilitychange:new Set()}};
  sdk.snapshot=path=>({exists:()=>!!sdk.accounts[path],data:()=>structuredClone(sdk.accounts[path])});
  sdk.auth={currentUser:{uid:'existing-auth-uid',displayName:'Existing account'},authStateReady:()=>{sdk.authReadyCalls=(sdk.authReadyCalls||0)+1;return deferAuth?new Promise(resolve=>sdk.releaseAuth=resolve):Promise.resolve();}};
  localStorage.setItem('gh.username','AccountA');
  const nativeFetch=window.fetch;window.fetch=(...args)=>{sdk.fetchCalls.push(String(args[0]));return nativeFetch(...args);};
  const nativeInterval=window.setInterval;window.setInterval=(...args)=>{sdk.intervalCalls++;return nativeInterval(...args);};
  const add=window.addEventListener.bind(window),remove=window.removeEventListener.bind(window);window.addEventListener=(name,callback,...args)=>{if(sdk.handlers[name])sdk.handlers[name].add(callback);return add(name,callback,...args);};window.removeEventListener=(name,callback,...args)=>{sdk.handlers[name]?.delete(callback);return remove(name,callback,...args);};
  const docAdd=document.addEventListener.bind(document),docRemove=document.removeEventListener.bind(document);document.addEventListener=(name,callback,...args)=>{if(name==='visibilitychange')sdk.handlers[name].add(callback);return docAdd(name,callback,...args);};document.removeEventListener=(name,callback,...args)=>{if(name==='visibilitychange')sdk.handlers[name].delete(callback);return docRemove(name,callback,...args);};
  if(menu&&queued){for(const [name,wave,difficulty]of [['AccountA',19,'easy'],['AccountB',8,'hard']])localStorage.setItem(`turncraft.shattered-front.progress.${name}.v1`,JSON.stringify({campaign:{completedMissions:{}},survival:{recordsByMap:{}},achievements:{},pendingResults:[{mode:'survival',arenaId:'amber-wastes',wave,durationSeconds:90,enemiesDefeated:24,factionId:'frontier',difficulty}]}));}
 },{deferAuth,menu,queued});
 await page.route('**/*',route=>{
  const url=new URL(route.request().url());
  if(url.hostname==='www.gstatic.com')return route.fulfill({contentType:'text/javascript',body:url.pathname.endsWith('firebase-app.js')?appModule:url.pathname.endsWith('firebase-auth.js')?authModule:storeModule});
  if(url.hostname!=='turncraft.test')return route.fulfill({status:404,body:''});
  if(url.pathname==='/fixture')return route.fulfill({contentType:'text/html',body:fixture(menu)});
  const file=path.resolve(repo,'.'+decodeURIComponent(url.pathname));if(file.startsWith(repo+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file,contentType:file.endsWith('.mjs')?'text/javascript':undefined});
  return route.fulfill({status:404,body:''});
 });
 await page.goto('http://turncraft.test/fixture');await page.waitForFunction(()=>window.fixtureReady);return {browser,page,errors,apiRequests};
}
const current=page=>page.evaluate(()=>profileEvents.at(-1));
const counts=page=>page.evaluate(()=>({docs:sdk.docListeners.filter(row=>row.active).map(row=>row.path),auth:sdk.authListeners.filter(row=>row.active).length,handlers:Object.fromEntries(Object.entries(sdk.handlers).map(([key,set])=>[key,set.size])),intervals:sdk.intervalCalls,signIns:sdk.signIns,fetch:sdk.fetchCalls}));

test('profile follows username changes, ignores stale snapshots, and rejects cross-account saves',async()=>{
 const {browser,page,errors,apiRequests}=await setup();try{
  await page.waitForFunction(()=>profileEvents.at(-1)?.identity?.username==='AccountA'&&profileEvents.at(-1).identity.linked);
  assert.equal((await current(page)).profile.survival.highestWave,17);assert.deepEqual((await counts(page)).docs,['users/AccountA']);
  await page.evaluate(()=>{localStorage.setItem('gh.username','AccountB');dispatchEvent(new StorageEvent('storage',{key:'gh.username',oldValue:'AccountA',newValue:'AccountB'}));});
  await page.waitForFunction(()=>profileEvents.at(-1)?.identity?.username==='AccountB'&&profileEvents.at(-1).identity.linked);
  assert.equal((await current(page)).profile.survival.highestWave,4);assert.deepEqual((await counts(page)).docs,['users/AccountB']);assert.equal(await page.evaluate(()=>sdk.docUnsubscribes),1);
  await page.evaluate(()=>sdk.docListeners[0].next(sdk.snapshot('users/AccountA')));assert.equal((await current(page)).identity.username,'AccountB');
  await page.evaluate(()=>{localStorage.setItem('gh.username','AccountA');dispatchEvent(new Event('focus'));});await page.waitForFunction(()=>profileEvents.at(-1)?.identity?.username==='AccountA'&&profileEvents.at(-1).identity.linked);
  await page.evaluate(()=>{localStorage.setItem('gh.username','AccountB');document.dispatchEvent(new Event('visibilitychange'));});await page.waitForFunction(()=>profileEvents.at(-1)?.identity?.username==='AccountB'&&profileEvents.at(-1).identity.linked);
  const denied=await page.evaluate(async()=>{const before=sdk.transactions.length;try{await profile.saveCampaign({missionId:'amber-wastes-1',won:true,stars:3},{username:'AccountA'});return {accepted:true};}catch(error){return {message:error.message,before,after:sdk.transactions.length};}});assert.match(denied.message,/account changed/i);assert.equal(denied.after,denied.before);
  const saved=await page.evaluate(async()=>{await profile.saveSurvival({arenaId:'amber-wastes',factionId:'brood',difficulty:'hard',wave:12,durationSeconds:180,enemiesDefeated:33},{username:'AccountB'});return {transactions:sdk.transactions,accounts:sdk.accounts};});assert.deepEqual(saved.transactions.flatMap(row=>row.reads),['users/AccountB']);assert.equal(saved.accounts['users/AccountA'].gameStats.turncraft.survival.highestWave,17);assert.equal(saved.accounts['users/AccountB'].gameStats.turncraft.survival.highestWave,12);assert.deepEqual(saved.accounts['users/AccountB'].gameStats.anotherGame,{wins:41});
  await page.evaluate(()=>{window.stopSecond=profile.watchProfile(()=>{});stopFirst();});assert.equal((await counts(page)).auth,1);assert.deepEqual((await counts(page)).docs,['users/AccountB']);await page.evaluate(()=>stopSecond());
  const final=await counts(page);assert.deepEqual(final.docs,[]);assert.equal(final.auth,0);assert.deepEqual(final.handlers,{storage:0,focus:0,visibilitychange:0});assert.equal(final.intervals,0);assert.equal(final.signIns,0);assert.deepEqual(final.fetch,[]);assert.deepEqual(apiRequests,[]);assert.deepEqual(errors,[]);
 }finally{await browser.close();}
});

test('unsubscribing during SDK startup leaves no document or auth listeners behind',async()=>{
 const {browser,page,errors}=await setup({deferAuth:true});try{await page.waitForFunction(()=>sdk.authReadyCalls===1);await page.evaluate(async()=>{stopFirst();sdk.releaseAuth();await profile.firebase();});await page.waitForTimeout(20);const final=await counts(page);assert.deepEqual(final.docs,[]);assert.equal(final.auth,0);assert.deepEqual(final.handlers,{storage:0,focus:0,visibilitychange:0});assert.equal(final.intervals,0);assert.deepEqual(errors,[]);}finally{await browser.close();}
});

test('queued menu saves keep their account and automatically flush the new account after switching',async()=>{
 const {browser,page,errors,apiRequests}=await setup({menu:true});try{
  await page.waitForFunction(()=>sdk.transactions.length===1&&sdk.releaseTransactions.length===1);
  await page.evaluate(()=>{localStorage.setItem('gh.username','AccountB');dispatchEvent(new StorageEvent('storage',{key:'gh.username',newValue:'AccountB'}));});await page.waitForFunction(()=>profileEvents.at(-1)?.identity?.username==='AccountB'&&profileEvents.at(-1).identity.linked);
  await page.evaluate(()=>sdk.releaseTransactions.shift()());await page.waitForFunction(()=>sdk.transactions.length===2&&sdk.releaseTransactions.length===1);
  const owner=await page.evaluate(()=>({reads:sdk.transactions.flatMap(row=>row.reads),a:sdk.accounts['users/AccountA'].gameStats.turncraft.survival.highestWave,b:sdk.accounts['users/AccountB'].gameStats.turncraft.survival.highestWave,bPending:JSON.parse(localStorage.getItem('turncraft.shattered-front.progress.AccountB.v1')).pendingResults}));assert.deepEqual(owner.reads,['users/AccountA']);assert.equal(owner.a,17,'account switch cancels a late write');assert.equal(owner.b,4);assert.equal(owner.bPending[0].wave,8);
  await page.evaluate(()=>{sdk.blockTransactions=false;sdk.releaseTransactions.shift()();});await page.waitForFunction(()=>JSON.parse(localStorage.getItem('turncraft.shattered-front.progress.AccountB.v1')).pendingResults.length===0);
  const synced=await page.evaluate(()=>({reads:sdk.transactions.flatMap(row=>row.reads),a:sdk.accounts['users/AccountA'].gameStats.turncraft.survival.highestWave,b:sdk.accounts['users/AccountB'].gameStats.turncraft.survival.highestWave}));assert.deepEqual(synced.reads,['users/AccountA','users/AccountB']);assert.equal(synced.a,17);assert.equal(synced.b,8);await page.evaluate(()=>{localStorage.setItem('gh.username','AccountA');dispatchEvent(new Event('focus'));});await page.waitForFunction(()=>sdk.accounts['users/AccountA'].gameStats.turncraft.survival.highestWave===19&&JSON.parse(localStorage.getItem('turncraft.shattered-front.progress.AccountA.v1')).pendingResults.length===0);assert.deepEqual(apiRequests,[]);assert.equal((await counts(page)).intervals,0);assert.deepEqual(errors,[]);
 }finally{await browser.close();}
});

test('logout cannot restore a stale URL profile or save a late checkpoint',async()=>{
 const {browser,page,errors}=await setup();try{
  await page.waitForFunction(()=>profileEvents.at(-1)?.identity?.linked);
  await page.evaluate(()=>{history.replaceState({},'', '?username=AccountA');localStorage.removeItem('gh.username');dispatchEvent(new StorageEvent('storage',{key:'gh.username',oldValue:'AccountA',newValue:null}));});
  await page.waitForFunction(()=>profileEvents.at(-1)?.identity?.username===null);
  const saved=await page.evaluate(async()=>{const before=sdk.transactions.length;try{await profile.saveCheckpoint({savedAt:100,username:'AccountA',resultId:'old'},'AccountA');return {accepted:true};}catch(error){return {before,after:sdk.transactions.length,selected:profile.selectedUsername()};}});
  assert.equal(saved.selected,null);assert.equal(saved.after,saved.before);assert.deepEqual(errors,[]);
 }finally{await browser.close();}
});

test('cloud checkpoints and history reload under their profile and an older result cannot clear a new run',async()=>{
 const {browser,page,errors}=await setup();try{
  await page.waitForFunction(()=>profileEvents.at(-1)?.identity?.linked);
  const saved=await page.evaluate(async()=>{
    const checkpoint={version:1,savedAt:200,username:'AccountA',resultId:'new-run',simulation:{players:[{name:'AccountA'}]}};
    await profile.saveCheckpoint(checkpoint,'AccountA');
    await profile.saveCheckpoint(null,'AccountA','old-run');
    await profile.saveCheckpoint({...checkpoint,savedAt:100,resultId:'stale'},'AccountA');
    sdk.accounts['users/AccountA/turncraftHistory/match-one']={id:'match-one',completedAt:300,mode:'ranked',outcome:'win',mmrDelta:25};
    sdk.accounts['users/AccountB/turncraftHistory/other']={id:'other',completedAt:400,mode:'ranked',outcome:'loss'};
    const reopened=await import('/game/turncraft/shattered-front/profile.mjs?reopen');
    const restored=await reopened.getCheckpoint();
    await reopened.saveCheckpoint(null,'AccountA','new-run');
    return {checkpoint:restored,cleared:await reopened.getCheckpoint(),history:await reopened.getHistory(),profile:(await reopened.getProfile()).profile};
  });
  assert.equal(saved.checkpoint.resultId,'new-run');assert.equal(saved.checkpoint.savedAt,200);
  assert.equal(saved.cleared.cleared,true);assert.equal(saved.cleared.completedRunId,'new-run');
  assert.deepEqual(saved.history.map(row=>row.id),['match-one']);assert.equal(saved.profile.survival.highestWave,17);assert.deepEqual(errors,[]);
 }finally{await browser.close();}
});

test('a fresh browser auth session settles cloud-pending history under the same hub profile',async()=>{
 const {browser,page,errors}=await setup();try{
  await page.waitForFunction(()=>profileEvents.at(-1)?.identity?.linked);
  await page.evaluate(async()=>{
    sdk.auth.currentUser={uid:'replacement-auth-uid'};
    sdk.accounts['lobbies/recovered-duel']={gameType:'turncraft',turncraftVersion:1,status:'finished',ranked:true,finishedAt:500,players:['AccountA','Opponent'],members:{original:'AccountA',peer:'Opponent'},turncraftSeats:['original','peer'],turncraftPlayers:{original:{name:'AccountA',factionId:'frontier'},peer:{name:'Opponent',factionId:'brood'}},settings:{maxPlayers:2,fillAi:false,mapId:'classic',gameMode:'annihilation'},turncraftResult:{winnerIdx:0,players:[{name:'AccountA',factionId:'frontier'},{name:'Opponent',factionId:'brood'}],durationSeconds:100}};
    sdk.accounts['users/AccountA/turncraftPending/recovered-duel']={lobbyId:'recovered-duel',username:'AccountA',createdAt:100};
    localStorage.removeItem('turncraft.pendingMatches.AccountA');
    await profile.getProfile();
  });
  await page.waitForFunction(()=>sdk.accounts['users/AccountA/turncraftHistory/recovered-duel']&&!sdk.accounts['users/AccountA/turncraftPending/recovered-duel']);
  const saved=await page.evaluate(()=>({history:sdk.accounts['users/AccountA/turncraftHistory/recovered-duel'],profile:sdk.accounts['users/AccountA'].gameStats.turncraft}));
  assert.equal(saved.history.uid,'original');assert.equal(saved.history.outcome,'win');assert.equal(saved.profile.mmr,1225);assert.equal(saved.profile.rankedMatches,10);assert.deepEqual(errors,[]);
 }finally{await browser.close();}
});

test('a cached back-navigation page keeps the menu profile listener and follows the selected account',async()=>{
 const {browser,page,errors}=await setup({menu:true});try{
  await page.waitForFunction(()=>sdk.transactions.length===1&&sdk.releaseTransactions.length===1);
  await page.evaluate(()=>{sdk.blockTransactions=false;sdk.releaseTransactions.shift()();});
  await page.waitForFunction(()=>JSON.parse(localStorage.getItem('turncraft.shattered-front.progress.AccountA.v1')).pendingResults.length===0);
  await page.evaluate(()=>{dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}));dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}));localStorage.setItem('gh.username','AccountB');dispatchEvent(new StorageEvent('storage',{key:'gh.username',newValue:'AccountB'}));});
  await page.waitForFunction(()=>document.querySelector('.sf-account')?.textContent.includes('AccountB'));
  assert.deepEqual((await counts(page)).docs,['users/AccountB']);assert.equal((await counts(page)).auth,1);assert.deepEqual(errors,[]);
 }finally{await browser.close();}
});

test('a denied profile listener preserves the saved profile and reattaches on reconnect',async()=>{
 const {browser,page,errors}=await setup();try{
  await page.waitForFunction(()=>profileEvents.at(-1)?.identity?.linked);
  await page.evaluate(()=>sdk.docListeners.at(-1).error(Object.assign(new Error('Denied'),{code:'permission-denied'})));
  const denied=await current(page);assert.equal(denied.profile.survival.highestWave,17);assert.match(denied.error,/verify the deployed rules match/);
  await page.evaluate(()=>dispatchEvent(new Event('online')));
  await page.waitForFunction(()=>sdk.docListeners.length===2&&profileEvents.at(-1)?.connectionStatus==='connected'&&!profileEvents.at(-1).error);
  assert.equal((await current(page)).profile.survival.highestWave,17);
  assert.equal(await page.evaluate(()=>sdk.docListeners.at(-1).options.includeMetadataChanges),true);
  assert.deepEqual(errors,[]);
 }finally{await browser.close();}
});

test('failed checkpoint uploads retain a retry action until the checkpoint itself saves',async()=>{
 const {browser,page,errors}=await setup({menu:true,queued:false});try{
  await page.waitForFunction(()=>profileEvents.at(-1)?.identity?.linked&&document.querySelector('.sf-account'));
  await page.evaluate(()=>{
   sdk.failTransactions=true;
   const checkpoint={version:1,username:'AccountA',savedAt:900,resultId:'offline-checkpoint',simulation:{players:[]}};
   localStorage.setItem('turncraft.shattered-front.checkpoint.v1.AccountA',JSON.stringify(checkpoint));
   dispatchEvent(new CustomEvent('turncraft:checkpoint',{detail:{checkpoint}}));
  });
  await page.getByRole('button',{name:'Retry account sync',exact:true}).waitFor();
  assert.match(await page.locator('#sfStatus').textContent(),/Firestore denied/);
  await page.evaluate(()=>sdk.docListeners.find(row=>row.active).next(sdk.snapshot('users/AccountA')));
  assert.equal(await page.getByRole('button',{name:'Retry account sync',exact:true}).count(),1,'an unrelated profile snapshot does not acknowledge a checkpoint');
  await page.evaluate(()=>{sdk.failTransactions=false;dispatchEvent(new Event('online'));});
  await page.waitForFunction(()=>sdk.accounts['users/AccountA/turncraftSaves/active']?.savedAt===900);
  assert.equal(await page.getByRole('button',{name:'Retry account sync',exact:true}).count(),0);
  assert.equal(await page.evaluate(()=>JSON.parse(sdk.accounts['users/AccountA/turncraftSaves/active'].packed).resultId),'offline-checkpoint');assert.deepEqual(errors,[]);
 }finally{await browser.close();}
});
