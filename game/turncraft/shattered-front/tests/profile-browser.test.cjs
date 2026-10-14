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
export async function getDoc(ref){window.sdk.reads.push(ref.path);return window.sdk.snapshot(ref.path);}
export function onSnapshot(ref,next,error){const row={path:ref.path,next,error,active:true};window.sdk.docListeners.push(row);queueMicrotask(()=>{if(row.active)next(window.sdk.snapshot(row.path));});return()=>{if(row.active){row.active=false;window.sdk.docUnsubscribes++;}};}
export async function runTransaction(db,operation){
 const row={reads:[],updates:[]};window.sdk.transactions.push(row);
 if(window.sdk.blockTransactions)await new Promise(resolve=>window.sdk.releaseTransactions.push(resolve));
 const value=await operation({get:async ref=>{row.reads.push(ref.path);return window.sdk.snapshot(ref.path);},update(ref,patch){row.updates.push({path:ref.path,patch:structuredClone(patch)});}});
 for(const {path,patch} of row.updates){const account=window.sdk.accounts[path];for(const [field,value] of Object.entries(patch)){const segments=field.split('.');let target=account;for(const segment of segments.slice(0,-1))target=target[segment]||=( {} );target[segments.at(-1)]=structuredClone(value);}}
 return value;
}
`;
const fixture=menu=>`<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="overlay"></div><script type="module">
import * as profile from '/game/turncraft/shattered-front/profile.mjs';
window.profile=profile;window.profileEvents=[];window.stopFirst=profile.watchProfile(value=>profileEvents.push(structuredClone(value)));
${menu?`const {CATALOG}=await import('/game/turncraft/shattered-front/campaign.mjs');window.TurncraftExpansion={catalog:CATALOG,getFactions:()=>[{id:'frontier',name:'Frontier Directorate',strengths:'Range',color:'#73cfff'}],getUnits:()=>[],getIdentity:()=>({lobbyId:''}),setProfile:()=>{}};await import('/game/turncraft/shattered-front/menu.mjs');`:''}
window.fixtureReady=true;
</script>`;

async function setup({deferAuth=false,menu=false}={}){
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']}),page=await browser.newPage(),errors=[],apiRequests=[];
 page.on('pageerror',error=>errors.push(error.message));page.on('request',request=>{if(new URL(request.url()).pathname.startsWith('/api/'))apiRequests.push(request.url());});
 await page.addInitScript(({deferAuth,menu})=>{
  const account=(wave)=>({avatar:'',gameStats:{anotherGame:{wins:41},turncraft:{mmr:1200,rankedMatches:9,survival:{highestWave:wave,recordsByMap:{}},campaign:{completedMissions:{}},achievements:{}}}});
  window.sdk={accounts:{'users/AccountA':account(17),'users/AccountB':account(4)},docListeners:[],authListeners:[],docUnsubscribes:0,authUnsubscribes:0,signIns:0,reads:[],transactions:[],releaseTransactions:[],blockTransactions:menu,fetchCalls:[],intervalCalls:0,handlers:{storage:new Set(),focus:new Set(),visibilitychange:new Set()}};
  sdk.snapshot=path=>({exists:()=>!!sdk.accounts[path],data:()=>structuredClone(sdk.accounts[path])});
  sdk.auth={currentUser:{uid:'existing-auth-uid',displayName:'Existing account'},authStateReady:()=>{sdk.authReadyCalls=(sdk.authReadyCalls||0)+1;return deferAuth?new Promise(resolve=>sdk.releaseAuth=resolve):Promise.resolve();}};
  localStorage.setItem('gh.username','AccountA');
  const nativeFetch=window.fetch;window.fetch=(...args)=>{sdk.fetchCalls.push(String(args[0]));return nativeFetch(...args);};
  const nativeInterval=window.setInterval;window.setInterval=(...args)=>{sdk.intervalCalls++;return nativeInterval(...args);};
  const add=window.addEventListener.bind(window),remove=window.removeEventListener.bind(window);window.addEventListener=(name,callback,...args)=>{if(sdk.handlers[name])sdk.handlers[name].add(callback);return add(name,callback,...args);};window.removeEventListener=(name,callback,...args)=>{sdk.handlers[name]?.delete(callback);return remove(name,callback,...args);};
  const docAdd=document.addEventListener.bind(document),docRemove=document.removeEventListener.bind(document);document.addEventListener=(name,callback,...args)=>{if(name==='visibilitychange')sdk.handlers[name].add(callback);return docAdd(name,callback,...args);};document.removeEventListener=(name,callback,...args)=>{if(name==='visibilitychange')sdk.handlers[name].delete(callback);return docRemove(name,callback,...args);};
  if(menu){for(const [name,wave,difficulty]of [['AccountA',19,'easy'],['AccountB',8,'hard']])localStorage.setItem(`turncraft.shattered-front.progress.${name}.v1`,JSON.stringify({campaign:{completedMissions:{}},survival:{recordsByMap:{}},achievements:{},pendingResults:[{mode:'survival',arenaId:'amber-wastes',wave,durationSeconds:90,enemiesDefeated:24,factionId:'frontier',difficulty}]}));}
 },{deferAuth,menu});
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
  const owner=await page.evaluate(()=>({reads:sdk.transactions.flatMap(row=>row.reads),a:sdk.accounts['users/AccountA'].gameStats.turncraft.survival.highestWave,b:sdk.accounts['users/AccountB'].gameStats.turncraft.survival.highestWave,bPending:JSON.parse(localStorage.getItem('turncraft.shattered-front.progress.AccountB.v1')).pendingResults}));assert.deepEqual(owner.reads,['users/AccountA']);assert.equal(owner.a,19);assert.equal(owner.b,4);assert.equal(owner.bPending[0].wave,8);
  await page.evaluate(()=>{sdk.blockTransactions=false;sdk.releaseTransactions.shift()();});await page.waitForFunction(()=>JSON.parse(localStorage.getItem('turncraft.shattered-front.progress.AccountB.v1')).pendingResults.length===0);
  const synced=await page.evaluate(()=>({reads:sdk.transactions.flatMap(row=>row.reads),a:sdk.accounts['users/AccountA'].gameStats.turncraft.survival.highestWave,b:sdk.accounts['users/AccountB'].gameStats.turncraft.survival.highestWave}));assert.deepEqual(synced.reads,['users/AccountA','users/AccountB']);assert.equal(synced.a,19);assert.equal(synced.b,8);assert.deepEqual(apiRequests,[]);assert.equal((await counts(page)).intervals,0);assert.deepEqual(errors,[]);
 }finally{await browser.close();}
});
