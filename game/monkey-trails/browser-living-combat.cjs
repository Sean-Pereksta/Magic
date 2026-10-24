const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),report=[];
 try{for(const scenario of ['manual','assisted','reduced']){
  const context=await browser.newContext({viewport:{width:320,height:844},isMobile:true,hasTouch:true,reducedMotion:scenario==='reduced'?'reduce':'no-preference'});
  await context.route('**/*',async route=>{const u=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(u.pathname));if(u.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();return route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});
  const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));await page.goto('http://127.0.0.1:7777/game/monkey-trails.html');
  await page.evaluate(async scenario=>{
   localStorage.setItem('monkey-trails-display-v2',JSON.stringify({precisionTiming:true,assistedTiming:scenario==='assisted'}));
   const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs'),v=new Vault(()=>{}),s=await v.open('Trail Practice','practice-only-not-ranked',{create:true,mode:'practice'}),p=s.run.monkey;
   Object.assign(p,{species:'sunscar',types:['fire','earth'],stage:3,mastery:'fire',level:60,learned:[],loadout:['strike','emberJuke','crownfireComet','stoneCounter']});C.syncProgression(p);p.hp=C.stats(p).hp;p.energy=C.stats(p).maxEnergy;
   C.advance(s,C.ENCOUNTER_STEP);C.beginPendingEncounter(s);C.readyBattle(s);const b=s.run.battle;b.enemy.hp=10000;b.enemy.energy=0;b.enemy.loadout=['breathe'];b.enemyAction={type:'move',id:'breathe'};b.intent='breathe';await v.checkpoint(s,true);
  },scenario);
  await page.reload();await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});await page.locator('[data-action="ready"]').click();
  await page.locator('[data-action="move"][data-id="emberJuke"]').click();
  if(scenario==='manual'){await page.locator('.timing-track').waitFor();assert.match(await page.locator('#dialog-content').innerText(),/miss still performs the normal move/);await page.locator('[data-action="timing-skip"]').click();}
  else assert.equal(await page.locator('.timing-track').count(),0);
  await page.waitForTimeout(700);assert.match(await page.locator('.battle-title .tag').innerText(),/ROUND 2/);
  if(scenario==='assisted')assert.match(await page.locator('#view').innerText(),/Perfect Timing/);
  if(scenario==='manual'){
   await page.locator('[data-action="move"][data-id="crownfireComet"]').click();await page.locator('[data-action="timing-skip"]').click();await page.waitForTimeout(700);assert.match(await page.locator('.battle-title .tag').innerText(),/ROUND 3/);
   const release=page.locator('[data-action="move"][data-id="crownfireComet"]');assert.equal(await release.isEnabled(),true);assert.match(await release.innerText(),/0 EN/);
   await page.reload();await page.locator('#practice-button').click();await page.locator('[data-action="ready"]').click();assert.equal(await release.isEnabled(),true);await release.click();await page.locator('[data-action="timing-skip"]').click();await page.waitForTimeout(700);assert.match(await page.locator('.battle-title .tag').innerText(),/ROUND 4/);
   assert.equal(await release.isDisabled(),true);
  }
  const sizing=await page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth}));assert.ok(sizing.scroll<=sizing.width,JSON.stringify(sizing));assert.deepEqual(errors,[]);report.push({scenario,rounds:scenario==='manual'?4:2,errors:0,overflow:0});await context.close();
 }}finally{await browser.close();}console.log(JSON.stringify(report,null,2));
})().catch(error=>{console.error(error);process.exitCode=1;});
