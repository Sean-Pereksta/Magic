// Real app and encrypted vault coverage for permanent progression and recaps.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
const name='Trail Practice',password='practice-only-not-ranked';
const saved=page=>page.evaluate(async({name,password})=>{const S=await import('/game/monkey-trails/storage.mjs'),id=await S.identity(name,'practice'),envelope=JSON.parse(localStorage.getItem('monkey-trails.v1.'+id));return S.unseal(envelope,await S.derive(password,envelope.salt),id);},{name,password});
(async()=>{const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),report=[];
try{for(const width of [320,390,1280]){
 const context=await browser.newContext({viewport:{width,height:900},hasTouch:width<700,isMobile:width<700});
 await context.route('**/*',async route=>{const u=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(u.pathname));if(u.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file)){await route.abort();return;}await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});
 const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));await page.goto('http://127.0.0.1:7777/game/monkey-trails.html');
 await page.evaluate(async()=>{
  const C=await import('/game/monkey-trails/core.mjs'),L=await import('/game/monkey-trails/legacy.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs'),v=new Vault(()=>{}),s=await v.open('Trail Practice','practice-only-not-ranked',{create:true,species:'capuchin',mode:'practice'});
  s.meta.trailWisdom.earned=20;s.run.distance=6000*C.FEET;s.run.wins=4;s.run.legacy.earned=7;s.run.legacy.captured=2;s.run.legacy.equipmentCrafted=3;s.run.legacy.ultras=1;s.run.legacy.locations=['test-temple'];
  L.completeLegacyExpedition(s,{score:8000,level:3,form:'Capuchin'});s.run.phase='dead';for(const p of C.team(s))p.hp=0;await v.checkpoint(s,true);
 });
 await page.locator('#practice-button').click();await page.locator('.expedition-complete').waitFor();
 assert.equal(await page.locator('.recap-stat-grid article').count(),8);assert.match(await page.locator('.recap-stat-grid').innerText(),/6,000/);assert.match(await page.locator('.recap-permanent').innerText(),/Trail Wisdom Earned/);
 const before=await saved(page),history=before.meta.history;
 await page.locator('.expedition-complete [data-action="legacy-open"]').click();assert.equal(await page.locator('.legacy-upgrade').count(),5);
 await page.locator('[data-action="legacy-buy"][data-id="trailblazer"]').click();await page.waitForFunction(()=>document.querySelector('.legacy-upgrade')?.innerText.includes('Rank 1 / 4'));
 await page.locator('#dialog-content [data-action="close-dialog"]').click();await page.locator('[data-action="legacy-history"]').first().click();assert.equal(await page.locator('.history-expedition').count(),1);assert.match(await page.locator('.history-expedition').innerText(),/2 captures/);await page.locator('.dialog-close').click();
 await page.reload();await page.locator('#practice-button').click();await page.locator('.expedition-complete').waitFor();const reopened=await saved(page);
 assert.deepEqual(reopened.meta.history,history);assert.equal(reopened.meta.trailWisdom.earned,before.meta.trailWisdom.earned);assert.equal(reopened.meta.trailWisdom.spent,3);assert.equal(reopened.meta.trailWisdom.ranks.trailblazer,1);
 if(process.env.MONKEY_TRAILS_SCREENSHOTS){fs.mkdirSync(process.env.MONKEY_TRAILS_SCREENSHOTS,{recursive:true});await page.screenshot({path:path.join(process.env.MONKEY_TRAILS_SCREENSHOTS,`expedition-${width}.png`),fullPage:true});}
 assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),`${width}: recap fits screen`);
 await page.locator('[data-action="restart"]').click();await page.waitForFunction(()=>document.querySelector('.distance-block')?.innerText.includes('02'));
 await page.waitForFunction(async()=>{const S=await import('/game/monkey-trails/storage.mjs'),id=await S.identity('Trail Practice','practice'),envelope=JSON.parse(localStorage.getItem('monkey-trails.v1.'+id)),s=await S.unseal(envelope,await S.derive('practice-only-not-ranked',envelope.salt),id);return s.meta.runs===2;});
 const restarted=await saved(page);assert.equal(restarted.run.distance,0);assert.equal(restarted.run.startingDepth,250*.3048);assert.equal(restarted.meta.trailWisdom.earned,before.meta.trailWisdom.earned);assert.deepEqual(restarted.meta.history,history);assert.equal(restarted.run.fruit,8);assert.equal(restarted.run.discoveries,0);
 assert.deepEqual(errors,[]);report.push({width,recap:true,purchase:true,reload:true,restart:true,overflow:0});await context.close();
}console.log(JSON.stringify(report,null,2));}finally{await browser.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
