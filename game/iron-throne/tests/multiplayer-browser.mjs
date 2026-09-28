// Real Firebase SDK + Auth/Firestore emulators; no screenshots or HTML previews.
import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {createRequire} from 'node:module';
import {planFoundings} from '../founding.mjs';
import {decodePayload} from '../multiplayer-state.mjs';
const require=createRequire(process.env.IRON_FIREBASE_TEST_MODULES?`${process.env.IRON_FIREBASE_TEST_MODULES}/package.json`:import.meta.url);
const playwrightRequire=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=playwrightRequire('playwright'),{build}=require('esbuild');
const {initializeTestEnvironment}=require('@firebase/rules-unit-testing');
const {doc,setDoc,getDoc,updateDoc,Timestamp}=require('firebase/firestore');
const root=fileURLToPath(new URL('../../../',import.meta.url));
const env=await initializeTestEnvironment({projectId:'demo-iron-thrones',firestore:{host:'127.0.0.1',port:8089,rules:await readFile(path.join(root,'firestore.rules'),'utf8')}});
const sdk=await build({stdin:{contents:`
 import * as app from 'firebase/app';import * as auth from 'firebase/auth';import * as fs from 'firebase/firestore';
 export * from 'firebase/app';export * from 'firebase/auth';export * from 'firebase/firestore';
 const databases=new WeakSet(),identities=new WeakSet();
 export function initializeApp(){return app.getApps()[0]||app.initializeApp({projectId:'demo-iron-thrones',apiKey:'demo-test-key',authDomain:'localhost'});}
 export function getFirestore(a){const db=fs.getFirestore(a);if(!databases.has(db)){fs.connectFirestoreEmulator(db,'127.0.0.1',8089);databases.add(db);}return db;}
 export function getAuth(a){const identity=auth.getAuth(a);if(!identities.has(identity)){auth.connectAuthEmulator(identity,'http://127.0.0.1:9099',{disableWarnings:true});identities.add(identity);}return identity;}
 `,resolveDir:path.dirname(require.resolve('firebase/package.json'))},bundle:true,format:'esm',write:false,platform:'browser'});
const server=http.createServer(async(req,res)=>{
 const pathname=decodeURIComponent(new URL(req.url,'http://localhost').pathname);
 if(pathname==='/__test/firebase.mjs'){res.writeHead(200,{'Content-Type':'text/javascript'});res.end(sdk.outputFiles[0].text);return;}
 if(pathname==='/__test/login'){res.writeHead(200,{'Content-Type':'text/html'});res.end('<!doctype html><title>Emulator test login</title>');return;}
 try{const file=path.resolve(root,'.'+pathname);if(!file.startsWith(root))throw Error('path');res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(await readFile(file));}catch{res.writeHead(404);res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;
let browser;const errors=[];
const admin=fn=>env.withSecurityRulesDisabled(ctx=>fn(ctx.firestore()));
const read=async p=>{let data;await admin(async db=>{data=(await getDoc(doc(db,p))).data();});return data;};
async function eventually(check,timeout=30000){const until=Date.now()+timeout;while(Date.now()<until){if(await check())return;await new Promise(r=>setTimeout(r,100));}throw new Error('Condition timed out.');}
async function selectTile(page,id){
 const from=(await page.locator('#coordinates').textContent()).match(/(\d+),(\d+)$/).slice(1).map(Number),to=id.split(',').map(Number);
 await page.locator('#map').focus();
 for(const [i,plus,minus]of [[0,'ArrowRight','ArrowLeft'],[1,'ArrowDown','ArrowUp']])for(let n=0;n<Math.abs(to[i]-from[i]);n++)await page.keyboard.press(to[i]>from[i]?plus:minus);
}
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox',...(process.env.IRON_THRONE_CHROMIUM?['--no-zygote','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']:[])]});
 async function ruler(viewport){
  const context=await browser.newContext({viewport,hasTouch:viewport.width<900}),page=await context.newPage();
  page.on('dialog',dialog=>void dialog.accept());
  page.on('pageerror',e=>{errors.push(e.message);console.error('Browser error:',e.message);});
  await context.route('https://www.gstatic.com/firebasejs/**',route=>route.fulfill({contentType:'text/javascript',body:`export * from '${base}/__test/firebase.mjs';`}));
  await context.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
  await context.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
  await page.goto(`${base}/__test/login`);
  const uid=await page.evaluate(async()=>{const f=await import('/__test/firebase.mjs');const app=f.initializeApp(),auth=f.getAuth(app);return (await f.signInAnonymously(auth)).user.uid;});
  return {context,page,uid};
 }
 const a=await ruler({width:1280,height:850}),b=await ruler({width:390,height:844}),id='browser-two',prefix=`lobbies/${id}`;assert.notEqual(a.uid,b.uid);
 await admin(db=>setDoc(doc(db,prefix),{name:'Browser campaign',gameType:'ironthrone',host:'Sean',hostUid:a.uid,players:['Sean','Alex'],members:{[a.uid]:'Sean',[b.uid]:'Alex'},status:'started',visibility:'private'}));
 for(const r of [a,b])await r.page.goto(`${base}/game/iron-throne/index.html?gameId=${id}&username=${r===a?'Sean':'Alex'}`);
 await Promise.all([a.page.locator('[data-claim="ashen"]').waitFor(),b.page.locator('[data-claim="ashen"]').waitFor()]);
 // A real transaction race in independent browser contexts.
 await Promise.all([a.page.locator('[data-claim="ashen"]').click(),b.page.locator('[data-claim="ashen"]').click()]);
 await eventually(async()=>!!(await read(`${prefix}/iron_throne/meta`)).seats.ashen.uid);
 let m=await read(`${prefix}/iron_throne/meta`);
 assert.equal(Object.values(m.seats).filter(s=>s.uid).length,1);
 if(m.seats.ashen.uid===b.uid){await b.page.locator('[data-claim="wintermere"]').click();await eventually(async()=>(await read(`${prefix}/iron_throne/meta`)).seats.ashen.kind==='open');await a.page.locator('[data-claim="ashen"]').click();}
 else await b.page.locator('[data-claim="wintermere"]').click();
 await eventually(async()=>{const m=await read(`${prefix}/iron_throne/meta`);return m.seats.ashen.uid===a.uid&&m.seats.wintermere.uid===b.uid;});
 console.log('Seats claimed in separate browser contexts');
 await a.page.locator('#online-settings [name="seed"]').fill('5');
 await a.page.locator('#online-settings [name="preset"]').selectOption('heartlands');
 await a.page.locator('#online-settings [name="difficulty"]').selectOption('hard');
 await a.page.locator('#online-settings button').click();
 await eventually(async()=>(await read(`${prefix}/iron_throne/meta`)).options.seed===5);
 await a.page.locator('#online-start').click();
 await Promise.all([a.page.locator('#online-lobby').waitFor({state:'hidden'}),b.page.locator('#online-lobby').waitFor({state:'hidden'})]);
 await eventually(async()=>(await b.page.locator('#turn').textContent())==='Founding');
 let founding=await decodePayload((await read(`${prefix}/iron_throne/state`)).payload);
 const capA=planFoundings(founding)[0].capital;
 await selectTile(a.page,capA);await a.page.locator('[data-found-city]').click();
 await eventually(async()=>{const s=await decodePayload((await read(`${prefix}/iron_throne/world`)).payload);return s.founding.houses.ashen.founded;});
 await b.page.reload();await b.page.locator('#online-lobby').waitFor({state:'hidden'});await eventually(async()=>(await b.page.locator('#panel').textContent()).includes('Emberkeep founded'));
 await selectTile(b.page,capA);assert.equal(await b.page.locator('[data-found-city]').isDisabled(),true);
 assert.equal(await b.page.locator('#end-turn').isDisabled(),true);
 founding=await decodePayload((await read(`${prefix}/iron_throne/state`)).payload);
 const capB=planFoundings(founding).find(p=>p.owner==='wintermere').capital;
 await selectTile(b.page,capB);await b.page.locator('[data-found-city]').click();
 await eventually(async()=>(await read(`${prefix}/iron_throne/meta`)).phase==='planning');
 await eventually(async()=>(await a.page.locator('#turn').textContent())==='Turn 1'&&(await b.page.locator('#turn').textContent())==='Turn 1');
 assert.equal((await decodePayload((await read(`${prefix}/iron_throne/state`)).payload)).mapProfile,founding.mapProfile);
 console.log('PASS shared founding, visible capital, conflicting tile disabled, reconnect during founding, and atomic all-six transition');
 for(const r of [a,b]){assert.equal(await r.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.equal(await r.page.evaluate(()=>localStorage.getItem('catnmice.iron-throne.v1')),null);}
 await a.page.locator('[data-tab="realm"]').click();await b.page.locator('[data-tab="realm"]').click();
 await a.page.locator(`[data-goto="${capA}"]`).first().click();await b.page.locator(`[data-goto="${capB}"]`).first().click();
 await Promise.all([a.page.locator('[data-recruit="levy"]').click(),b.page.locator('[data-recruit="levy"]').click()]);
 await eventually(async()=>{const s=await decodePayload((await read(`${prefix}/iron_throne/state`)).payload);return s.kingdoms[0].population===72&&s.kingdoms[1].population===80;});
 assert.equal((await read(`${prefix}/iron_throne/meta`)).activeHouse,'ashen');
 assert.equal(await b.page.locator('#end-turn').isDisabled(),true);
 console.log('Only the active browser recruitment persisted');await a.page.locator('[data-dispatch="wintermere"]').click();await b.page.locator('[data-dispatch="ashen"]').click();
 await a.page.locator('#chat-message').fill('Let us form an alliance.');await a.page.locator('#send-chat').click();
 await eventually(async()=>(await b.page.locator('#messages').textContent()).includes('Let us form an alliance.'));
 assert.equal((await decodePayload((await read(`${prefix}/iron_throne/state`)).payload)).treaties.length,0);
 await a.page.locator('#quick-offer').click();await a.page.locator('#offer-type').selectOption('ALLIANCE');await a.page.locator('#give-amount').fill('0');await a.page.locator('#offer-form button[type="submit"]').click();
 await b.page.locator('#quick-offer').click();
 await b.page.locator('[data-human-accept]').waitFor();await b.page.locator('[data-human-accept]').click();
 assert.equal((await decodePayload((await read(`${prefix}/iron_throne/state`)).payload)).treaties.length,0);
 await a.page.locator('[data-close="diplomacy"]').click();
 await a.page.locator('#end-turn').click();
 await eventually(async()=>(await read(`${prefix}/iron_throne/meta`)).activeHouse==='wintermere');
 await eventually(async()=>b.page.locator('#end-turn').isEnabled());
 await b.page.locator('[data-human-accept]').click();
 await eventually(async()=>!!(await decodePayload((await read(`${prefix}/iron_throne/state`)).payload)).treaties.find(t=>t.type==='alliance'));
 await b.page.locator('.treaty-drawer-close').click();
 await b.page.locator('[data-close="diplomacy"]').click();
 await b.page.locator('[data-tab="realm"]').click();await b.page.locator(`[data-goto="${capB}"]`).first().click();
 await b.page.locator('[data-recruit="levy"]').click();
 await eventually(async()=>(await decodePayload((await read(`${prefix}/iron_throne/state`)).payload)).kingdoms[1].population===72);
 await eventually(async()=>b.page.locator('#end-turn').isEnabled());await b.page.locator('#end-turn').click();
 await eventually(async()=>(await read(`${prefix}/iron_throne/meta`)).turn===2,60000);
 let canonical=await decodePayload((await read(`${prefix}/iron_throne/state`)).payload);assert.equal(canonical.strategy.history.at(-1).houses.length,4);
 assert.equal(canonical.difficulty,'hard');assert.equal(canonical.sequential.id,7);assert.equal(canonical.roundFinished,1);
 console.log('PASS two independent browsers: only active House can recruit or ratify; six activations, four AI turns and one economy boundary; mobile layout');
 // Native offline/online behavior: no divergent local save and no enabled orders.
 await b.context.setOffline(true);await eventually(async()=>(await b.page.locator('.online-bar').textContent()).includes('RECONNECTING'));
 assert.equal(await b.page.locator('#end-turn').isDisabled(),true);
 await b.context.setOffline(false);await eventually(async()=>!(await b.page.locator('.online-bar').textContent()).includes('RECONNECTING'));
 const reconnectUid=b.uid;await b.page.reload();await b.page.locator('#online-lobby').waitFor({state:'hidden'});await eventually(async()=>(await b.page.locator('#coordinates').textContent()).includes(capB)&&!(await b.page.locator('.online-bar').textContent()).includes('RECONNECTING'));
 assert.equal((await read(`${prefix}/iron_throne/meta`)).seats.wintermere.uid,reconnectUid);
 // Leave the second human active before closing the controller. Waiting for a
 // lease never grants another activation or bypasses the House turn gate.
 await a.page.locator('#end-turn').click();
 await eventually(async()=>(await read(`${prefix}/iron_throne/meta`)).activeHouse==='wintermere');
 await eventually(async()=>b.page.locator('#end-turn').isEnabled());
 // Closing the actual controller browser exercises lease expiry and takeover.
 m=await read(`${prefix}/iron_throne/meta`);const oldEpoch=m.epoch,oldVersion=m.stateVersion,oldTurn=m.turn;
 assert.equal(m.lease.uid,a.uid);await a.context.close();
 await eventually(async()=>{const m=await read(`${prefix}/iron_throne/meta`);return m.lease.uid===b.uid&&m.epoch>oldEpoch;},45000);
 m=await read(`${prefix}/iron_throne/meta`);assert.equal(m.turn,oldTurn);assert.equal(m.stateVersion,oldVersion);
 await b.page.locator('[data-tab="realm"]').click();await b.page.locator('#tax').selectOption('low');
 await eventually(async()=>(await read(`${prefix}/iron_throne/meta`)).stateVersion>oldVersion);
 canonical=await decodePayload((await read(`${prefix}/iron_throne/state`)).payload);assert.equal(canonical.kingdoms[1].tax,'low');
 console.log('PASS real disconnect: offline orders blocked, same UID reconnects, controller replaced without a turn restart, new controller accepts orders');
 await b.context.close();
 // Six humans run the same production UI and Firebase transport.
 const six=[];for(let i=0;i<6;i++)six.push(await ruler({width:1024,height:768}));
 const sixId='browser-six',sixRoot=`lobbies/${sixId}`,houses=['ashen','wintermere','thornwall','sunspire','vesper','redharbor'];
 await admin(db=>setDoc(doc(db,sixRoot),{name:'Six humans',gameType:'ironthrone',host:'R0',hostUid:six[0].uid,players:six.map((_,i)=>`R${i}`),members:Object.fromEntries(six.map((r,i)=>[r.uid,`R${i}`])),status:'started'}));
 for(let i=0;i<6;i++){await six[i].page.goto(`${base}/game/iron-throne/index.html?gameId=${sixId}`);await six[i].page.locator(`[data-claim="${houses[i]}"]`).click();await eventually(async()=>(await read(`${sixRoot}/iron_throne/meta`)).seats[houses[i]].uid===six[i].uid);}
 await eventually(async()=>Object.values((await read(`${sixRoot}/iron_throne/meta`)).seats).every(s=>s.kind==='human'));
 await six[0].page.locator('#online-start').click();
 for(let i=0;i<six.length;i++){
  const r=six[i];await r.page.locator('#online-lobby').waitFor({state:'hidden'});await eventually(async()=>(await r.page.locator('#turn').textContent())==='Founding');
  const s=await decodePayload((await read(`${sixRoot}/iron_throne/state`)).payload),site=planFoundings(s).find(p=>p.owner===houses[i]).capital;
  await selectTile(r.page,site);await r.page.locator('[data-found-city]').click();
  await eventually(async()=>!!(await decodePayload((await read(`${sixRoot}/iron_throne/state`)).payload)).founding.houses[houses[i]].founded);
 }
 for(let i=0;i<6;i++){
  const meta=await read(`${sixRoot}/iron_throne/meta`),r=six[houses.indexOf(meta.activeHouse)],activation=meta.activationId;
  for(const waiting of six.filter(x=>x!==r))assert.equal(await waiting.page.locator('#end-turn').isDisabled(),true);
  await eventually(async()=>r.page.locator('#end-turn').isEnabled());await r.page.locator('#end-turn').click();
  await eventually(async()=>(await read(`${sixRoot}/iron_throne/meta`)).activationId>activation);
 }
 await eventually(async()=>(await read(`${sixRoot}/iron_throne/meta`)).turn===2,45000);
 canonical=await decodePayload((await read(`${sixRoot}/iron_throne/state`)).payload);assert.equal(canonical.strategy.history.reduce((n,r)=>n+r.houses.length,0),0);assert.equal(new Set(Object.values(canonical.controllers).map(s=>s.uid)).size,6);
 console.log('PASS six independent browsers: six different UID seats, one synchronized round, no human House executes AI strategy');
 for(const r of six)await r.context.close();
 assert.deepEqual(errors,[]);
}catch(error){console.error(error);console.error('Meta:',await read('lobbies/browser-two/iron_throne/meta'));console.error('Six-player meta:',await read('lobbies/browser-six/iron_throne/meta'));for(const ctx of browser?.contexts()||[])for(const p of ctx.pages())console.error('Page:',await p.evaluate(()=>({toast:document.getElementById('toast')?.textContent,status:document.getElementById('online-lobby-status')?.textContent})));throw error;}finally{await browser?.close();await env.cleanup();server.closeAllConnections();await new Promise(r=>server.close(r));}
