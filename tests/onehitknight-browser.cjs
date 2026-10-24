const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const playwright=require('playwright');
const out=path.resolve(process.env.OHK_TEST_OUTPUT||path.join(require('node:os').tmpdir(),'onehitknight-test-results'));fs.mkdirSync(out,{recursive:true});
const {pathToFileURL}=require('node:url');
const input=path.resolve(process.argv[2]||path.join(__dirname,'../game/onehitknight.html'));
const scratch=path.resolve(process.argv[3]||path.join(out,'instrumented.html'));
let html=fs.readFileSync(input,'utf8');
html=html.replace('<script src="onehitknight/embedded-art.js"></script>',()=>'<script>'+fs.readFileSync(path.join(path.dirname(input),'onehitknight/embedded-art.js'),'utf8')+'</script>');
const end=html.lastIndexOf('})();');assert(end>0,'IIFE test boundary exists');
html=html.slice(0,end)+"window.__ohkTest={exec:code=>eval(code)};\n"+html.slice(end);fs.writeFileSync(scratch,html);
async function firebaseMock(){
 const records=window.__firebaseRecords={adds:[],deletes:[],reads:0};
 const collection={orderBy(){return this;},limit(){return this;},async get(){records.reads++;return {docs:[{id:'seed',data:()=>({name:'TEST KNIGHT',score:10,wave:1,createdMs:1}),ref:{id:'seed'}}]};},async add(v){records.adds.push(v);return {id:'test-score'};}};
 const db={collection:()=>({doc:()=>({collection:()=>collection})}),batch:()=>({delete:r=>records.deletes.push(r),commit:async()=>{}})};
 const firestore=()=>db;firestore.FieldValue={serverTimestamp:()=>({testTimestamp:true})};
 window.firebase={apps:[],initializeApp(){this.apps.push({});},auth:()=>({signInAnonymously:async()=>({user:{uid:'TEST-UID'}})}),firestore};
}
(async()=>{
 const browser=await playwright.chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH});
 const context=await browser.newContext({viewport:{width:1280,height:800}});await context.addInitScript(firebaseMock);
 await context.route('https://www.gstatic.com/firebasejs/**',r=>r.fulfill({status:200,contentType:'text/javascript',body:'/* deterministic scoring adapter supplied by test */'}));
 const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(pathToFileURL(scratch).href);await page.waitForFunction(()=>window.__ohkTest&&window.OHKAssets);await page.evaluate(async()=>{await OHKAssets.ready;return true;});
 const exec=async code=>page.evaluate(code=>__ohkTest.exec(code),code),checks=[];
 async function check(name,fn){await fn();checks.push(name);console.log('PASS '+name);}
 await check('game opens directly from disk with its included artwork and no local server',async()=>{const direct=await context.newPage();direct.on('pageerror',e=>errors.push('direct file: '+e.message));await direct.goto(pathToFileURL(input).href);await direct.waitForFunction(()=>window.OHKAssets);await direct.evaluate(async()=>{await OHKAssets.ready;return true;});await direct.locator('#classicBtn').click();await direct.waitForFunction(()=>document.getElementById('overlay').style.display==='none');await direct.close();});
 await check('real sprite boot and animated title',async()=>{assert.equal(await page.locator('#classicBtn').count(),1);assert(await page.evaluate(()=>OHKAssets.progress)>0);assert.equal(await page.locator('#menuHero').count(),1);});
 for(const [button,mode] of [['classicBtn','CLASSIC'],['chaosBtn','CHAOS'],['minuteBtn','60 SECOND']])await check(mode+' starts',async()=>{await exec('restoreMenu()');await page.locator('#'+button).click();await page.waitForTimeout(120);assert.equal(await exec('mode'),mode);assert.equal(await exec('running'),true);});
 await check('settings, pause, resume and reduced effects',async()=>{await page.locator('#pauseBtn').click();assert.equal(await exec('pausedForChoice'),true);await page.locator('#pauseSettings').click();await page.locator('#reduceShake').check();await page.locator('#reduceFlash').check();await page.locator('#settingsBack').click();assert.equal(await exec('pausedForChoice'),false);assert(await page.evaluate(()=>OHKSettings.shake)<1);assert(await page.evaluate(()=>OHKSettings.flash)<1);});
 await check('keyboard movement and locked four-direction swing',async()=>{await exec("reset('CLASSIC')");await exec('enemies=[];wavePause=999;spawnTimer=999');const x=await exec('player.x');await page.keyboard.down('KeyD');await page.waitForTimeout(150);await page.keyboard.up('KeyD');assert(await exec('player.x')>x);await page.keyboard.press('Space');assert(await exec('!!player.attack'));});
 await require('./onehitknight-combat-browser.cjs')(page,exec,check,assert);
 await check('online adapter reads and writes expected score payload',async()=>{await exec("stats.score=54321;stats.wave=4;stats.kills=22;mode='CHAOS';saveOnlineScore('Sir <Knight>')");const data=await page.evaluate(()=>__firebaseRecords.adds.at(-1));assert.equal(data.score,54321);assert.equal(data.uid,'TEST-UID');assert.equal(data.mode,'CHAOS');assert.equal(data.name,'SIR KNIGHT');assert(await page.evaluate(()=>__firebaseRecords.reads)>0);});
 await check('restart clears completed and pending run state',async()=>{await exec("reset('CLASSIC')");await exec('stats.score=99;stats.combo=12');await exec("reset('CHAOS')");assert.equal(await exec('stats.score'),0);assert.equal(await exec('stats.combo'),0);assert.equal(await exec('gameOver'),false);assert.equal(await exec('pausedForChoice'),false);});
 await exec('restoreMenu()');await page.screenshot({path:path.resolve(out,'one-hit-knight-menu.png')});
 await exec("reset('CHAOS')");await exec("player.inv=999;enemies.forEach((e,i)=>{e.x=70+i*48;e.y=95+(i%2)*65;});");await page.waitForTimeout(250);await page.screenshot({path:path.resolve(out,'one-hit-knight-gameplay.png')});
 const mobile=await browser.newContext({viewport:{width:390,height:844},isMobile:true,hasTouch:true});await mobile.addInitScript(firebaseMock);await mobile.route('https://www.gstatic.com/firebasejs/**',r=>r.fulfill({status:200,contentType:'text/javascript',body:''}));const mp=await mobile.newPage();mp.on('pageerror',e=>errors.push('mobile: '+e.message));await mp.goto(pathToFileURL(scratch).href);await mp.evaluate(async()=>{await OHKAssets.ready;return true;});await mp.locator('#classicBtn').tap();await mp.waitForTimeout(100);
 await check('mobile stick, slash and dash are reachable',async()=>{assert(await mp.locator('#pad').isVisible());assert(await mp.locator('#slashBtn').isVisible());const box=await mp.locator('#pad').boundingBox();await mp.dispatchEvent('#pad','pointerdown',{pointerId:7,pointerType:'touch',clientX:box.x+box.width*.75,clientY:box.y+box.height/2});await mp.waitForTimeout(100);await mp.dispatchEvent('#pad','pointerup',{pointerId:7,pointerType:'touch'});await mp.locator('#slashBtn').tap();await mp.locator('#dashBtn').tap();assert(await mp.evaluate(()=>__ohkTest.exec('player.dashCool'))>0);});
 await mp.screenshot({path:path.resolve(out,'one-hit-knight-mobile.png')});
 assert.deepEqual(errors,[],'no browser exceptions');
 const assetCases=process.env.OHK_ART_ROOT?await require('./onehitknight-assets-browser.cjs')(browser,scratch,check,assert):[];
 fs.writeFileSync(path.resolve(out,'browser-test-results.json'),JSON.stringify({passed:checks.length,checks,errors,assetCases,online:'deterministic Firebase adapter; production database was not written'},null,2));
 await browser.close();
})().catch(err=>{console.error(err);process.exit(1);});
