import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createRequire} from 'node:module';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';

const require=createRequire(import.meta.url);
let chromium;try{({chromium}=require('playwright'));}catch{}
const assetBase=(process.env.FACTION_FORGE_R2_URL||'').replace(/\/+$/,'');
const root=path.resolve(import.meta.dirname,'../../..');
const execFileAsync=promisify(execFile);
const firebaseMocks={
  'firebase-app.js':`export const initializeApp=()=>({});export const getApp=()=>({});export const getApps=()=>[];`,
  'firebase-auth.js':`const auth={currentUser:{uid:'owner-uid',getIdToken:async()=> 'test-token'}};export const getAuth=()=>auth;export const signInAnonymously=async()=>({user:auth.currentUser});export const signInWithEmailAndPassword=async()=>({user:auth.currentUser});export const onAuthStateChanged=(a,fn)=>{setTimeout(()=>fn(a.currentUser),0);return ()=>{}};`,
  'firebase-firestore.js':`export const getFirestore=()=>({});export const doc=(...parts)=>({path:parts.slice(1).join('/')});export const collection=doc;export const query=(...x)=>x;export const where=(...x)=>x;export const getDoc=async()=>({exists:()=>false,data:()=>({})});export const setDoc=async ref=>{const counter=ref.path.startsWith('lobbies/')?'__fireWrites':'__soloWrites';globalThis[counter]=(globalThis[counter]||0)+1};export const updateDoc=setDoc;export const runTransaction=async(db,fn)=>fn({get:getDoc,set:setDoc,update:updateDoc});export const onSnapshot=(ref,fn)=>{setTimeout(()=>ref instanceof Array?fn({docs:[]}):fn({exists:()=>false,data:()=>({})}),0);return ()=>{}};`
};

async function startupServer({graphicsTimeout=20000}={}){
  const app=http.createServer(async(req,res)=>{try{
    const file=path.resolve(root,'.'+new URL(req.url,'http://localhost').pathname);if(!file.startsWith(root+path.sep))throw Error('bad path');
    let body=await fs.readFile(file);
    if(file.endsWith('crowncouncil.html'))body=Buffer.from(body.toString().replace('baseUrl:FACTION_FORGE_CONFIG.assetBaseUrl,requestTimeout:20000',`baseUrl:FACTION_FORGE_CONFIG.assetBaseUrl,requestTimeout:${graphicsTimeout}`).replace('/* Fullscreen */','globalThis.__forgeStartupTest={get assets(){return forgeAssets},get mode(){return executionMode},get ui(){return expansionUI}};\n/* Fullscreen */'));
    res.setHeader('Content-Type',file.endsWith('.html')?'text/html':file.endsWith('.css')?'text/css':'text/javascript');res.end(body);
  }catch{res.writeHead(404);res.end();}});
  await new Promise(resolve=>app.listen(0,'127.0.0.1',resolve));return {app,url:`http://127.0.0.1:${app.address().port}`};
}
async function startupRoutes(context){
  await context.route('https://www.gstatic.com/firebasejs/**',route=>route.fulfill({contentType:'text/javascript',body:firebaseMocks[new URL(route.request().url()).pathname.split('/').at(-1)]||''}));
  await context.route('**/graphics/**',route=>route.fulfill({status:404,body:''}));
  await context.route('**/faction-forge/config.js',route=>route.fulfill({contentType:'text/javascript',body:`export const FACTION_FORGE_CONFIG={assetBaseUrl:'https://forge-assets.test',progressionApiUrl:'',matchmakingTimeoutMs:30000};`}));
}
const startupImage='<svg xmlns="http://www.w3.org/2000/svg" width="768" height="768"><rect width="768" height="768" fill="#83a7d0"/></svg>';

test('opening waits for every Cloudflare graphic to decode before entering the solo menu or multiplayer game', {skip:!chromium},async()=>{
  const {app,url}=await startupServer();let browser;
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
    for(const multiplayer of [false,true]){
      const context=await browser.newContext({viewport:{width:390,height:844}});await startupRoutes(context);
      await context.addInitScript(()=>{
        const nativeDecode=HTMLImageElement.prototype.decode;
        HTMLImageElement.prototype.decode=async function(){
          await nativeDecode.call(this);
          if(this.src.endsWith('/units/rifleman-tier1.webp')){globalThis.__decodeHeld=true;await new Promise(resolve=>{globalThis.__releaseGraphicsDecode=resolve;});}
        };
      });
      const requested=new Set(),page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
      await context.route('https://forge-assets.test/**',route=>{requested.add(route.request().url());return route.fulfill({contentType:'image/svg+xml',body:startupImage});});
      await page.goto(url+'/game/crowncouncil.html'+(multiplayer?'?gameId=existing-room&username=Guest':''));
      await page.waitForFunction(()=>globalThis.__decodeHeld&&__forgeStartupTest?.assets.stats.loads===138);
      assert.equal(requested.size,139);assert.equal(await page.locator('body.ff-starting').count(),1);
      assert.equal(await page.locator('#assetGate').isVisible(),true);assert.equal(await page.locator('.ff-expansion').count(),0);
      assert.equal(await page.locator('.topbar').evaluate(node=>node.inert),true);
      assert.ok(await page.locator('#assetGatePlayers').textContent().then(value=>value.includes(multiplayer?'multiplayer':'main menu')));
      assert.ok(await page.locator('#assetGate').evaluate(node=>node.scrollWidth<=innerWidth));
      await page.evaluate(()=>__releaseGraphicsDecode());await page.waitForFunction(()=>!document.body.classList.contains('ff-starting'));
      assert.equal(await page.evaluate(()=>__forgeStartupTest.assets.preloadedSize),139);
      assert.equal(await page.locator('.topbar').evaluate(node=>node.inert),false);
      if(multiplayer){assert.equal(await page.evaluate(()=>__forgeStartupTest.mode),'legacy-online');assert.equal(await page.evaluate(()=>__forgeStartupTest.ui.view),'');}
      else{await page.getByRole('heading',{name:'FACTION FORGE',exact:true}).waitFor();assert.equal(await page.locator('#assetGate').isVisible(),false);}
      assert.deepEqual(errors,[]);await context.close();
    }
  }finally{await browser?.close();await new Promise(resolve=>app.close(resolve));}
});

test('missing Cloudflare artwork opens solo and multiplayer with an exact diagnostic and nonblocking retry', {skip:!chromium},async()=>{
  const {app,url}=await startupServer();let browser;
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
    for(const multiplayer of [false,true]){
      const context=await browser.newContext({viewport:{width:390,height:844}});await startupRoutes(context);const page=await context.newPage(),requested=new Map(),errors=[];let failing=true;
      page.on('pageerror',error=>errors.push(error.message));
      await context.route('https://forge-assets.test/**',route=>{
        const path=new URL(route.request().url()).pathname;requested.set(path,(requested.get(path)||0)+1);
        return path==='/units/rifleman-tier1.webp'&&failing?route.fulfill({status:503,headers:{'Access-Control-Allow-Origin':'*'},body:'unavailable'}):route.fulfill({contentType:'image/svg+xml',body:startupImage});
      });
      await page.goto(url+'/game/crowncouncil.html'+(multiplayer?'?gameId=existing-room&username=Guest':''));
      await page.waitForFunction(()=>!document.body.classList.contains('ff-starting'));
      await page.waitForFunction(()=>FactionForgeGraphicsReport?.failures?.[0]?.httpStatus===503);
      assert.equal(await page.evaluate(()=>__forgeStartupTest.assets.preloadedSize),138);assert.equal(requested.size,139);
      if(multiplayer)assert.equal(await page.evaluate(()=>__forgeStartupTest.mode),'legacy-online');
      else{
        await page.getByRole('heading',{name:'FACTION FORGE',exact:true}).waitFor();
        assert.ok(await page.locator('.ff-portrait-symbol').count()>0,'missing troop portraits retain their symbol fallback');
        await page.locator('.ff-expansion button').last().focus();await page.keyboard.press('Tab');
        assert.equal(await page.evaluate(()=>document.activeElement.id),'ffGraphicsWarningSummary','graphics details are reachable from the menu keyboard focus cycle');
      }
      await page.locator('#ffGraphicsWarningSummary').click();
      assert.match(await page.locator('#ffGraphicsWarningList').textContent(),/units\/rifleman-tier1.webp.*download.*HTTP 503/s);
      assert.equal(await page.locator('#ffGraphicsWarningList a').getAttribute('href'),'https://forge-assets.test/units/rifleman-tier1.webp');
      assert.ok(await page.locator('#ffGraphicsWarnings').evaluate(node=>node.getBoundingClientRect().right<=innerWidth));
      const beforeRetry=requested.get('/units/rifleman-tier1.webp');failing=false;
      await page.getByRole('button',{name:'Retry missing graphics',exact:true}).click();
      await page.waitForFunction(()=>FactionForgeGraphicsReport?.failures.length===0&&__forgeStartupTest.assets.preloadedSize===139);
      assert.equal(requested.get('/units/rifleman-tier1.webp'),beforeRetry+1);assert.equal(requested.get('/units/vguard-tier3.webp'),1);
      assert.equal(await page.locator('#ffGraphicsWarnings').isVisible(),false);assert.equal(await page.locator('body.ff-starting').count(),0);assert.deepEqual(errors,[]);
      await context.close();
    }
  }finally{await browser?.close();await new Promise(resolve=>app.close(resolve));}
});

test('multiplayer guest profile connection failure offers a working startup retry', {skip:!chromium},async()=>{
  const {app,url}=await startupServer();let browser;
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
    const context=await browser.newContext();await startupRoutes(context);const page=await context.newPage();
    await context.route('https://forge-assets.test/**',route=>route.fulfill({contentType:'image/svg+xml',body:startupImage}));
    await context.route('**/firebase-auth.js',route=>route.fulfill({contentType:'text/javascript',body:`
      const auth={currentUser:null};let notify;export const getAuth=()=>auth;
      export const onAuthStateChanged=(a,fn)=>{notify=fn;setTimeout(()=>fn(a.currentUser),0);return ()=>{}};
      export const signInAnonymously=async()=>{globalThis.__guestAttempts=(globalThis.__guestAttempts||0)+1;if(__guestAttempts===1)throw new Error('Connection unavailable');auth.currentUser={uid:'owner-uid'};setTimeout(()=>notify(auth.currentUser),0);return {user:auth.currentUser}};
      export const signInWithEmailAndPassword=signInAnonymously;
    `}));
    await page.goto(url+'/game/crowncouncil.html?gameId=existing-room');await page.getByRole('button',{name:'Retry loading',exact:true}).waitFor();
    assert.match(await page.locator('#assetGateStatus').textContent(),/connect your profile/);assert.equal(await page.locator('.ff-expansion').isVisible(),false);
    await page.getByRole('button',{name:'Retry loading',exact:true}).click();await page.waitForFunction(()=>!document.body.classList.contains('ff-starting'));
    assert.equal(await page.evaluate(()=>__guestAttempts),2);assert.equal(await page.locator('body.ff-starting').count(),0);
  }finally{await browser?.close();await new Promise(resolve=>app.close(resolve));}
});

test('failed and stalled image decodes finish loading attempts and open both destinations', {skip:!chromium},async()=>{
  const {app,url}=await startupServer({graphicsTimeout:500});let browser;
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
    for(const multiplayer of [false,true]){
      const context=await browser.newContext();await startupRoutes(context);
      await context.addInitScript(()=>{
        const nativeDecode=HTMLImageElement.prototype.decode;
        HTMLImageElement.prototype.decode=async function(){
          await nativeDecode.call(this);
          if(this.src.endsWith('/units/rifleman-tier1.webp'))throw new DOMException('Image encoding unsupported','EncodingError');
          if(this.src.endsWith('/units/vguard-tier1.webp'))await new Promise(()=>{});
        };
      });
      await context.route('https://forge-assets.test/**',route=>route.fulfill({contentType:'image/svg+xml',body:startupImage}));
      const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
      await page.goto(url+'/game/crowncouncil.html'+(multiplayer?'?gameId=existing-room&username=Guest':''));
      await page.waitForFunction(()=>!document.body.classList.contains('ff-starting'));
      const report=await page.evaluate(()=>FactionForgeGraphicsReport),byPath=Object.fromEntries(report.failures.map(entry=>[entry.path,entry]));
      assert.equal(report.completed,139);assert.equal(report.loaded,137);assert.equal(report.failures.length,2);
      assert.equal(byPath['units/rifleman-tier1.webp'].stage,'decode');assert.match(byPath['units/rifleman-tier1.webp'].message,/Image encoding unsupported/);
      assert.equal(byPath['units/vguard-tier1.webp'].stage,'timeout');assert.equal(byPath['units/vguard-tier1.webp'].during,'decode');
      assert.match(byPath['units/vguard-tier1.webp'].message,/500 ms while decoding/);
      assert.equal(await page.evaluate(()=>__forgeStartupTest.mode),multiplayer?'legacy-online':'menu');assert.deepEqual(errors,[]);await context.close();
    }
  }finally{await browser?.close();await new Promise(resolve=>app.close(resolve));}
});

test('cross-origin display works without CORS or an uploaded manifest', {skip:!chromium},async()=>{
  const app=http.createServer(async(req,res)=>{try{const file=path.resolve(root,'.'+new URL(req.url,'http://localhost').pathname);if(!file.startsWith(root+path.sep))throw Error('bad path');res.setHeader('Content-Type','text/javascript');res.end(await fs.readFile(file));}catch{res.writeHead(404);res.end();}});
  await new Promise(r=>app.listen(0,'127.0.0.1',r));let browser;
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
    const page=await browser.newPage(),requests=[];page.on('request',r=>{if(r.url().startsWith('https://forge-assets.test/'))requests.push(r.url());});
    await page.route('https://forge-assets.test/**',route=>route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="768" height="768"><rect width="768" height="768" fill="#83a7d0"/></svg>'}));
    const base=`http://127.0.0.1:${app.address().port}`;await page.goto(base+'/game/faction-forge/assets.js');
    const result=await page.evaluate(async base=>{
      const {createAssetLoader}=await import(base+'/game/faction-forge/assets.js'),{createBattleRenderer}=await import(base+'/game/faction-forge/renderer.js');
      const assets=createAssetLoader({baseUrl:'https://forge-assets.test'}),sheet=await assets.unit('rifleman',1),host=document.createElement('div');document.body.appendChild(host);host.innerHTML='<div class="troop-art">fallback</div>';
      const renderer=createBattleRenderer({assets}),unit={id:'r2-display',type:'rifleman',p:0,x:0,y:0,hp:10,forgeTier:1};renderer.renderUnit(host,unit,0);await new Promise(resolve=>setTimeout(resolve,0));renderer.renderUnit(host,unit,120);const canvas=host.querySelector('canvas');let readbackBlocked=false;try{canvas.getContext('2d').getImageData(0,0,1,1);}catch(error){readbackBlocked=error.name==='SecurityError';}return {sheetWidth:sheet.image.width,crossOrigin:sheet.image.crossOrigin,width:canvas.width,height:canvas.height,readbackBlocked};
    },base);
    assert.equal(result.sheetWidth,768);assert.equal(result.crossOrigin,null);assert.equal(result.width,128);assert.equal(result.height,128);assert.equal(result.readbackBlocked,true);assert.deepEqual(requests,['https://forge-assets.test/units/rifleman-tier1.webp']);
  }finally{await browser?.close();await new Promise(r=>app.close(r));}
});

test('live folder-only R2 graphics render in the menu, draft and battle without CORS or a hosted manifest', {skip:!assetBase||!chromium,timeout:90000},async()=>{
  const app=http.createServer(async(req,res)=>{try{
    const pathname=new URL(req.url,'http://localhost').pathname,file=path.resolve(root,'.'+pathname);if(!file.startsWith(root+path.sep))throw Error('bad path');
    let data=await fs.readFile(file);
    if(pathname.endsWith('/config.js'))data=Buffer.from(`export const FACTION_FORGE_CONFIG={assetBaseUrl:${JSON.stringify(assetBase)},progressionApiUrl:'',matchmakingTimeoutMs:30000};`);
    if(pathname.endsWith('/crowncouncil.html'))data=Buffer.from(data.toString().replace('/* Fullscreen */','globalThis.__forgeLive={get state(){return S},get battle(){return B},chooseCard};\n/* Fullscreen */'));
    res.setHeader('Content-Type',file.endsWith('.html')?'text/html':file.endsWith('.css')?'text/css':file.endsWith('.webp')?'image/webp':file.endsWith('.png')?'image/png':'text/javascript');res.end(data);
  }catch{res.writeHead(404);res.end('missing');}});
  await new Promise(r=>app.listen(0,'127.0.0.1',r));let browser;
  try{
    const proxyUrl=process.env.FACTION_FORGE_BROWSER_PROXY;
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage'],...(proxyUrl?{proxy:{server:proxyUrl,bypass:'localhost,127.0.0.1'}}:{})});
    const page=await browser.newPage({ignoreHTTPSErrors:true}),errors=[],requests=[],failures=[],consoleErrors=[];
    page.on('pageerror',e=>errors.push(e.message));page.on('console',m=>{if(m.type()==='error')consoleErrors.push(m.text());});page.on('request',r=>{if(r.url().startsWith(assetBase))requests.push(r.url());});page.on('requestfailed',r=>{if(r.url().startsWith(assetBase))failures.push({url:r.url(),error:r.failure()?.errorText});});
    await page.route('https://www.gstatic.com/firebasejs/**',route=>route.fulfill({contentType:'text/javascript',body:firebaseMocks[new URL(route.request().url()).pathname.split('/').at(-1)]||''}));
    if(process.env.FACTION_FORGE_ASSET_RELAY==='curl')await page.route(assetBase+'/**',async route=>{
      // Relay real public bytes through the environment's HTTP transport when
      // Chromium itself cannot use the workspace's network proxy.
      try{const {stdout}=await execFileAsync('curl',['-f','-sS','--max-time','15',route.request().url()],{encoding:null,maxBuffer:8*1024*1024});await route.fulfill({contentType:'image/webp',body:stdout});}catch{await route.abort('failed');}
    });
    await page.goto(`http://127.0.0.1:${app.address().port}/game/crowncouncil.html`,{waitUntil:'domcontentloaded'});
    await page.getByRole('heading',{name:'FACTION FORGE',exact:true}).waitFor({timeout:60000});
    await page.locator('.ff-faction-showcase').scrollIntoViewIfNeeded();
    await page.waitForFunction(()=>document.querySelectorAll('.ff-faction-showcase .ff-portrait-canvas').length===3,null,{timeout:20000}).catch(async error=>{throw new Error(error.message+' '+JSON.stringify({errors,requests:requests.slice(0,12),failures:failures.slice(0,8),console:consoleErrors.slice(0,6),canvases:await page.locator('canvas').count()}));});
    assert.equal(await page.locator('.ff-faction-showcase .ff-portrait-canvas').first().getAttribute('width'),'128');
    assert.ok(await page.evaluate(()=>getComputedStyle(document.querySelector('.ff-expansion')).backgroundImage.includes('ui/main-menu.webp')));
    await page.getByRole('button',{name:/The Forge/}).first().click();await page.waitForFunction(()=>document.querySelector('.ff-forge-detail .ff-portrait-canvas'),null,{timeout:20000});
    assert.equal(await page.locator('.ff-collection-card').count(),32);assert.equal(await page.locator('.ff-locked-card').count(),6);
    await page.getByRole('button',{name:'← Menu',exact:true}).click();await page.getByRole('button',{name:'Practice against AI',exact:true}).click();await page.getByRole('button',{name:'Select and continue with Iron Vanguard',exact:true}).click();
    await page.waitForFunction(()=>globalThis.__forgeLive?.state?.phase==='draft',null,{timeout:20000});
    for(let i=0;i<3;i++){await page.waitForFunction(i=>__forgeLive.state.draftIndex===i&&__forgeLive.state.phase==='draft',i);await page.evaluate(()=>__forgeLive.chooseCard(0));}
    await page.waitForFunction(()=>__forgeLive.state.phase==='battle',null,{timeout:20000});
    await page.waitForFunction(()=>document.querySelectorAll('.ff-unit-sprite').length>0,null,{timeout:20000});
    const rendered=await page.locator('.ff-unit-sprite').evaluateAll(nodes=>nodes.map(n=>({width:n.width,height:n.height})));assert.ok(rendered.every(n=>n.width===128&&n.height===128));
    assert.equal(await page.evaluate(()=>globalThis.__fireWrites||0),0);assert.equal(await page.evaluate(()=>globalThis.__soloWrites||0),3);assert.deepEqual(errors,[]);assert.deepEqual(failures,[]);assert.equal(requests.filter(url=>url.endsWith('/manifest.json')).length,0);
  }finally{await browser?.close();await new Promise(r=>app.close(r));}
});
