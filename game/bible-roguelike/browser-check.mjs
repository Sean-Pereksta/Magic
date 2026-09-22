// Optional browser integration check. Uses mocked Firebase; never writes a live lobby.
// NODE_PATH=/path/to/node_modules node game/bible-roguelike/browser-check.mjs
// BIBLE_TEST_CSV=/path/to/web.csv uses the full production translation locally.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {resolve,extname} from 'node:path';
import {verses,make} from './test-fixtures.mjs';
const require=createRequire(import.meta.url),{chromium}=require('playwright');
const root=fileURLToPath(new URL('../../',import.meta.url));
const csv=process.env.BIBLE_TEST_CSV ? await readFile(process.env.BIBLE_TEST_CSV,'utf8') : 'VerseID,BookName,Chapter,Verse,Text\n'+verses.map(v=>`${v.id},${v.book},${v.chapter},${v.verse},"${v.text.replace(/"/g,'""')}"`).join('\n');
const rows=new Map();
const stamp=value=>value&&typeof value==='object'?value.__timestamp?{__ms:Date.now()}:Array.isArray(value)?value.map(stamp):Object.fromEntries(Object.entries(value).map(([k,v])=>[k,stamp(v)])):value;
function put(path,data){rows.set(path,{version:(rows.get(path)?.version||0)+1,data:stamp(structuredClone(data))});}
put('lobbies/browser-coop',{gameType:'bibleroguelike',status:'started',players:['Sean','Alex','John','Mary','Pat'],host:'Sean'});
const firestore=`
const hydrate=v=>v&&typeof v==='object'?v.__ms!=null?{toMillis:()=>v.__ms}:Array.isArray(v)?v.map(hydrate):Object.fromEntries(Object.entries(v).map(([k,x])=>[k,hydrate(x)])):v;
const snapshot=row=>({exists:()=>!!row,data:()=>hydrate(row?.data),metadata:{fromCache:false,hasPendingWrites:false}});
export const getFirestore=()=>({}),doc=(_,...parts)=>parts.join('/'),serverTimestamp=()=>({__timestamp:true});
export const getDoc=async ref=>snapshot(await window.testGet(ref));
export const setDoc=async(ref,data)=>window.testSet(ref,data);
export function onSnapshot(ref,options,fn){if(typeof options==='function')fn=options;let last='',done=false;const poll=async()=>{const row=await window.testGet(ref),key=JSON.stringify(row);if(!done&&key!==last){last=key;fn(snapshot(row))}};void poll();const timer=setInterval(poll,50);return()=>{done=true;clearInterval(timer)}}
export async function runTransaction(db,fn){for(let retry=0;retry<20;retry++){const versions={},writes=[];const result=await fn({get:async ref=>{if(writes.length)throw Error('read after write');const row=await window.testGet(ref);versions[ref]=row?.version||0;return snapshot(row)},set:(ref,data)=>writes.push([ref,data])});if(await window.testCommit(versions,writes))return result;}throw Error('Too many conflicts')}
`;
const server=createServer(async(req,res)=>{
  try{const path=resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://localhost').pathname));if(!path.startsWith(root))throw Error('bad path');const data=await readFile(path);res.setHeader('Content-Type',({'.html':'text/html','.mjs':'application/javascript','.js':'application/javascript','.css':'text/css'})[extname(path)]||'text/plain');res.end(data);}catch{res.writeHead(404);res.end('Not found');}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const base=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({headless:true,executablePath:process.env.BIBLE_TEST_CHROMIUM||undefined,args:['--no-sandbox','--disable-gpu','--disable-dev-shm-usage']});const errors=[];
async function context(name,viewport){
  const c=await browser.newContext({viewport});
  await c.exposeBinding('testGet',(_source,path)=>structuredClone(rows.get(path)||null));
  await c.exposeBinding('testSet',(_source,path,data)=>put(path,data));
  await c.exposeBinding('testCommit',(_source,versions,writes)=>{if(Object.entries(versions).some(([path,version])=>(rows.get(path)?.version||0)!==version))return false;writes.forEach(([path,data])=>put(path,data));return true;});
  await c.addInitScript(name=>{window.__uid='uid-'+name;},name);
  await c.route('https://www.gstatic.com/firebasejs/**',async route=>{
    const url=route.request().url();let body='';
    if(url.includes('firebase-app'))body='export const initializeApp=()=>({});';
    else if(url.includes('firebase-auth'))body='const auth={currentUser:{uid:window.__uid},authStateReady:async()=>{}};export const getAuth=()=>auth;export const signInAnonymously=async()=>({user:auth.currentUser});export const onAuthStateChanged=(_,fn)=>{queueMicrotask(()=>fn(auth.currentUser));return()=>{}};';
    else body=firestore;
    await route.fulfill({status:200,contentType:'application/javascript',body});
  });
  await c.route('https://raw.githubusercontent.com/**',route=>route.fulfill({status:200,contentType:'text/csv',body:csv}));
  await c.route('https://pub-15a649bcaae84f2ca6c610e6ef0dde51.r2.dev/**',route=>route.fulfill({status:200,contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="100" height="150"><rect width="100" height="150" fill="#48657c"/></svg>'}));
  const page=await c.newPage();page.on('pageerror',e=>errors.push(e.message));page.setDefaultTimeout(10000);
  return{c,page};
}
async function layout(page){
  const bounds=await page.evaluate(()=>{const root=document.querySelector('#rpgView'),attack=document.querySelector('#rqAttack'),arena=document.querySelector('#rqArena');return{overflowX:root.scrollWidth-root.clientWidth,overflowY:root.scrollHeight-root.clientHeight,attack:attack.getBoundingClientRect().toJSON(),arena:arena.getBoundingClientRect().toJSON(),width:innerWidth,height:innerHeight,party:[...document.querySelectorAll('.rq-party-member')].map(e=>e.getBoundingClientRect().toJSON())}});
  assert.ok(bounds.overflowX<=1,JSON.stringify(bounds));assert.ok(bounds.overflowY<=1,JSON.stringify(bounds));assert.ok(bounds.attack.bottom<=bounds.height+1,JSON.stringify(bounds));assert.ok(bounds.attack.right<=bounds.width+1,JSON.stringify(bounds));assert.ok(bounds.arena.height>=100,JSON.stringify(bounds));for(const card of bounds.party)assert.ok(card.left>=0&&card.right<=bounds.width+1,JSON.stringify(bounds));
}
try{
  const {c,page}=await context('Solo',{width:1440,height:900});
  await page.goto(base+'/game/biblegame.html?username=Solo&mode=roguelike');await page.waitForSelector('#rqEnemyName:not(:empty)');await layout(page);
  await page.locator('[data-act=finder]').first().click();await page.locator('#rqSearch').fill('John 3:16');
  await page.locator('[data-act=select-verse][data-key="john 3:16"]').waitFor();await page.locator('[data-act=favorite][data-key="john 3:16"]').click();
  await page.locator('[data-act=select-verse][data-key="john 3:16"]').click();assert.equal(await page.locator('#rqReference').inputValue(),'John 3:16');
  await page.locator('#rqAttack').click();await page.waitForFunction(()=>JSON.parse(localStorage.getItem('bibleSolo:solo:rpgRunSave:v3')).state.revision===1);
  const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('bibleSolo:solo:rpgRunSave:v3')).state);assert.ok(saved.used['john 3:16']);
  await page.reload();await page.waitForSelector('#rqEnemyName:not(:empty)');assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('bibleSolo:solo:rpgRunSave:v3')).state.id),saved.id);
  if(await page.locator('#rqDialog').evaluate(e=>e.open))await page.locator('[data-act=close]').click();
  await page.locator('[data-act=finder]').first().click();await page.locator('#rqSearch').fill('John 3:16');await page.waitForSelector('.rq-used');assert.ok(await page.locator('.rq-used').textContent().then(t=>t.includes('USED BY SOLO')));
  await page.locator('#rqScope').selectOption('favorites');await page.waitForFunction(()=>document.querySelectorAll('.rq-verse').length===1);assert.equal(await page.locator('.rq-verse').count(),1);
  await page.locator('[data-act=close]').click();
  for(const size of [{width:390,height:844},{width:320,height:568},{width:844,height:390}]){await page.setViewportSize(size);await layout(page);await page.locator('[data-act=finder]').first().click();assert.ok(await page.locator('#rqSearch').isVisible());await page.keyboard.press('Escape');await page.waitForFunction(()=>!document.querySelector('#rqDialog').open);}
  console.log('PASS solo: full Bible search, favorites, used labels, save/resume, desktop/phone/landscape bounds');await c.close();
  const a=await context('Sean',{width:1280,height:800}),b=await context('Alex',{width:390,height:844});
  await Promise.all([a.page.goto(base+'/game/biblegame.html?gameId=browser-coop&username=Sean'),b.page.goto(base+'/game/biblegame.html?gameId=browser-coop&username=Alex')]);
  await Promise.all([a.page.waitForSelector('#rqParty .rq-party-member:nth-child(2)'),b.page.waitForSelector('#rqParty .rq-party-member:nth-child(2)')]);
  await Promise.all([layout(a.page),layout(b.page)]);
  const state=rows.get('lobbies/browser-coop/roguelike/run').data;state.enemy.hp=state.enemy.maxHp=500;put('lobbies/browser-coop/roguelike/run',state);
  await Promise.all([a.page.waitForFunction(()=>document.querySelector('#rqEnemyHP').textContent==='500 / 500'),b.page.waitForFunction(()=>document.querySelector('#rqEnemyHP').textContent==='500 / 500')]);
  await Promise.all([a.page.locator('#rqReference').fill('John 3:16'),b.page.locator('#rqReference').fill('John 3:16')]);
  await Promise.all([a.page.locator('#rqAttack').click(),b.page.locator('#rqAttack').click()]);
  await a.page.waitForFunction(()=>document.querySelector('#rqEnemyHP').textContent!=='500 / 500');
  const after=rows.get('lobbies/browser-coop/roguelike/run').data;assert.equal(Object.keys(after.used).length,1);assert.equal(after.enemy.turn,1);
  await b.page.locator('[data-act=finder]').first().click();await b.page.locator('#rqSearch').fill('John 3:16');await b.page.waitForSelector('.rq-used');
  assert.ok((await b.page.locator('.rq-used').textContent()).includes('USED BY'));
  await b.page.reload();await b.page.waitForSelector('#rqParty .rq-party-member:nth-child(2)');assert.equal(rows.get('lobbies/browser-coop/roguelike/run').data.id,after.id);
  assert.equal(rows.get('lobbies/browser-coop/roguelike/run').data.players[1].uid,'uid-Alex');
  console.log('PASS co-op: two isolated browser sessions, five-player HUD, same-verse race, shared HP, used labels, reconnect');
  await a.c.close();await b.c.close();
  assert.deepEqual(errors,[]);console.log('PASS no uncaught browser errors');
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
