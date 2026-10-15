import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createRequire} from 'node:module';

const require=createRequire(import.meta.url);
let chromium;
try{({chromium}=require('playwright'));}
catch{({chromium}=require(path.join(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES||'/opt/codex/runtimes/codex-primary-runtime/dependencies/node/node_modules','playwright')));}
const root=path.resolve(import.meta.dirname,'../../..');

test('the entire battlefield stays visible across desktop, portrait, landscape and HUD resizing',async()=>{
  const server=http.createServer(async(req,res)=>{
    try{
      const file=path.join(root,new URL(req.url,'http://localhost').pathname);
      if(!file.startsWith(root+path.sep))throw Error('bad path');
      res.setHeader('Content-Type',file.endsWith('.html')?'text/html':file.endsWith('.css')?'text/css':'text/javascript');
      res.end(await fs.readFile(file));
    }catch{res.writeHead(404);res.end();}
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  let browser;
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
    const context=await browser.newContext({viewport:{width:1366,height:900}});
    const firebase={
      'firebase-app.js':`export const initializeApp=()=>({});export const getApp=()=>({});export const getApps=()=>[];`,
      'firebase-auth.js':`const auth={currentUser:{uid:'layout-user',displayName:'Sean',email:'sean@example.test',isAnonymous:false,getIdToken:async()=> 'test-token'}};export const getAuth=()=>auth;export const signInAnonymously=async()=>({user:auth.currentUser});export const signInWithEmailAndPassword=async()=>({user:auth.currentUser});export const onAuthStateChanged=(a,fn)=>{setTimeout(()=>fn(a.currentUser),0);return ()=>{}};`,
      'firebase-firestore.js':`const docs=new Map();export const getFirestore=()=>({});export const doc=(...parts)=>({path:parts.slice(1).join('/')});export const collection=doc;export const query=(...x)=>x;export const where=(...x)=>x;export const getDoc=async ref=>({exists:()=>docs.has(ref.path),data:()=>docs.get(ref.path)||{}});export const setDoc=(ref,data)=>docs.set(ref.path,data);export const updateDoc=setDoc;export const runTransaction=async(db,fn)=>fn({get:getDoc,set:setDoc,update:updateDoc});export const onSnapshot=(ref,fn)=>{setTimeout(()=>ref instanceof Array?fn({docs:[]}):fn({exists:()=>docs.has(ref.path),data:()=>docs.get(ref.path)||{}}),0);return ()=>{}};`
    };
    await context.addInitScript(()=>localStorage.setItem('gh.username','Sean'));
    await context.route('https://www.gstatic.com/firebasejs/**',route=>route.fulfill({contentType:'text/javascript',body:firebase[new URL(route.request().url()).pathname.split('/').at(-1)]||''}));
    await context.route('**/graphics/**',route=>route.fulfill({status:404,body:''}));
    await context.route('**/faction-forge/config.js',route=>route.fulfill({contentType:'text/javascript',body:`export const FACTION_FORGE_CONFIG={assetBaseUrl:'',progressionApiUrl:'',matchmakingTimeoutMs:30000};`}));
    await context.route('**/game/crowncouncil.html*',async route=>{
      let html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');
      html=html.replace('/* Fullscreen */','globalThis.__forgeLayoutTest={get state(){return S},chooseCard};\n/* Fullscreen */');
      await route.fulfill({contentType:'text/html',body:html});
    });
    const page=await context.newPage(),errors=[];
    page.on('pageerror',error=>errors.push(error.message));
    await page.goto('http://127.0.0.1:'+server.address().port+'/game/crowncouncil.html');
    await page.getByRole('button',{name:'Practice against AI',exact:true}).click();
    await page.getByRole('button',{name:/Vanir Industrial/}).click();
    for(let index=0;index<3;index++){
      await page.waitForFunction(index=>__forgeLayoutTest.state?.phase==='draft'&&__forgeLayoutTest.state.draftIndex===index,index);
      await page.evaluate(()=>__forgeLayoutTest.chooseCard(0));
    }
    await page.waitForFunction(()=>__forgeLayoutTest.state.phase==='battle',{timeout:20000});
    const sizes=[{width:1366,height:900},{width:1024,height:600},{width:390,height:844},{width:360,height:640},{width:844,height:390},{width:320,height:480}];
    for(const size of sizes){
      await page.setViewportSize(size);
      await page.waitForFunction(()=>{
        const board=document.getElementById('cells').getBoundingClientRect(),view=document.getElementById('viewport').getBoundingClientRect();
        return board.left>=view.left&&board.right<=view.right&&board.top>=view.top&&board.bottom<=view.bottom&&view.bottom<=innerHeight+1;
      });
      const dimensions=await page.evaluate(()=>{
        const rect=id=>{const r=document.getElementById(id).getBoundingClientRect();return {left:r.left,top:r.top,right:r.right,bottom:r.bottom,width:r.width,height:r.height};};
        return {board:rect('cells'),view:rect('viewport'),pageWidth:document.documentElement.scrollWidth,pageHeight:document.documentElement.scrollHeight,screenWidth:innerWidth,screenHeight:innerHeight,cells:document.querySelectorAll('#cells>.cell').length};
      });
      assert.equal(dimensions.cells,128);
      assert.ok(dimensions.board.width>0&&dimensions.board.height>0,JSON.stringify({size,dimensions}));
      assert.ok(dimensions.pageWidth<=size.width,JSON.stringify({size,dimensions}));
      assert.ok(dimensions.pageHeight<=size.height,JSON.stringify({size,dimensions}));
      assert.equal(await page.locator('.ff-forge-fab').isVisible(),false,'Forge shortcut does not cover the battlefield');
    }
    // A long account name changes HUD wrapping without changing window dimensions.
    await page.locator('#p0Badge').evaluate(node=>node.textContent='SeanTheVictoriousWithAnExtremelyLongName — ❤❤❤❤');
    await page.waitForFunction(()=>document.getElementById('viewport').getBoundingClientRect().bottom<=innerHeight+1);
    const before=await page.locator('#cells').boundingBox();
    await page.mouse.move(before.x+before.width/2,before.y+before.height/2);
    await page.mouse.down();await page.mouse.move(10,10);await page.mouse.up();
    const after=await page.locator('#cells').boundingBox();
    assert.deepEqual(after,before,'dragging cannot move any battlefield edge off screen');
    assert.deepEqual(errors,[]);
  }finally{
    await browser?.close();
    await new Promise(resolve=>server.close(resolve));
  }
});
