// Run with Playwright installed: node tests/turncraft-warfare.cjs
// Exercises the real inline game code in headless Chromium; no preview or network writes.
const fs=require('node:fs');
const path=require('node:path');
const assert=require('node:assert/strict');
const {chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,args:['--no-sandbox']});
 const page=await browser.newPage({viewport:{width:1280,height:900}});
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/*',route=>route.abort());
 const html=fs.readFileSync(path.join(__dirname,'../game/turncraft.html'),'utf8');
 let code=html.match(/<script type="module">([\s\S]*?)<\/script>/)[1].replace(/^import .*;$/gm,'');
 // Use a local blank page with persistent storage and stub only Firebase initialization.
 await page.unroute('**/*');
 await page.route('http://turncraft.test/',r=>r.fulfill({contentType:'text/html',body:html.replace(/<script type="module">[\s\S]*?<\/script>/,'')}));
 await page.route('https://**/*',r=>r.abort());
 await page.goto('http://turncraft.test/');
 code='const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});\n'+code;
 code=code.replace('\nboot();',`\nwindow.warfareTest={run:async function(){${fs.readFileSync(path.join(__dirname,'turncraft-warfare-scenarios.js'),'utf8')}}};`);
 await page.addScriptTag({content:code});
 const report=await page.evaluate(()=>warfareTest.run());
 assert.deepEqual(errors,[]);console.log(JSON.stringify(report,null,2));
 await browser.close();
})().catch(e=>{console.error(e);process.exit(1)});
