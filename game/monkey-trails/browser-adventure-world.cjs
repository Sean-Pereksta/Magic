// Exercise the real game, encrypted practice save, and new illustrated surfaces.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),out=process.env.MONKEY_TRAILS_OUTPUT;
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
const name='Trail Practice',password='practice-only-not-ranked';
async function readSave(page){return page.evaluate(async({name,password})=>{const S=await import('/game/monkey-trails/storage.mjs'),id=await S.identity(name,'practice'),e=JSON.parse(localStorage.getItem('monkey-trails.v1.'+id));return S.unseal(e,await S.derive(password,e.salt),id);},{name,password});}
async function flush(page){await page.waitForFunction(()=>/Saved on this device · separate practice/.test(document.getElementById('save-status').textContent));}
async function noOverflow(page,label){assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),label+' fits viewport');assert.ok(await page.evaluate(()=>{const el=document.querySelector('#inspect-dialog[open]');return !el||el.scrollWidth<=el.clientWidth+1;}),label+' dialog fits');}
async function capture(page,name){if(out){fs.mkdirSync(out,{recursive:true});await page.screenshot({path:path.join(out,name+'.png'),fullPage:true});}}
async function openGame(page){await page.goto('http://127.0.0.1:7777/game/monkey-trails.html');await page.locator('#practice-button').click();await page.locator('.land-scene').waitFor();}
async function seedGame(page,kind){
 await page.goto('http://127.0.0.1:7777/game/monkey-trails.html');
 await page.evaluate(async kind=>{
  const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs'),v=new Vault(()=>{}),s=await v.open('Trail Practice','practice-only-not-ranked',{create:true,mode:'practice',species:'capuchin'});
  if(kind==='home'){s.world.totalDistance=990;s.world.regions['emerald-jungle'].distance=990;s.meta.cosmetics={'canopy-banner':true};}
  if(kind==='camp'){C.advance(s,6000*C.FEET);s.run.fruit=40;}
  if(kind==='battle'){
   const p=structuredClone(s.run.monkey);p.id='teammate';p.species='macaque';p.types=[...C.SPECIES.macaque.types];p.memories={};p.loadout=['strike','brace','breathe'];s.collection[p.id]=p;s.run.party.push(p.id);s.run.monkey.loadout=['strike','brace','breathe'];
   C.advance(s,C.ENCOUNTER_STEP);C.beginPendingEncounter(s);
  }
  await v.checkpoint(s,true);
 },kind);
 await page.locator('#practice-button').click();
}
async function contextFor(browser,width){const context=await browser.newContext({viewport:{width,height:900},hasTouch:width<700,isMobile:width<700,timezoneId:'America/New_York',reducedMotion:'reduce'});await context.route('**/*',async route=>{const u=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(u.pathname));if(u.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file)){await route.abort();return;}await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});return context;}

async function main(){
 const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})});
 const report=[];
 try{
  for(const width of [320,390,1280]){
   const context=await contextFor(browser,width),page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
   await page.clock.setFixedTime(new Date('2026-10-29T16:00:00Z'));
   await openGame(page);
   assert.equal(await page.locator('.adv-route-card').count(),3);
   assert.equal(await page.locator('.adv-route-card svg[role="img"]').count(),3,'each route has accessible original artwork');
   await noOverflow(page,'Walk '+width);
   await capture(page,'adventure-walk-'+width);
   await page.locator('[data-action="adventure-route"][data-id="ancient-ruins"]').click();await flush(page);
   assert.equal((await readSave(page)).run.trailRoute.id,'ancient-ruins');
   await page.reload();await page.locator('#practice-button').click();await page.locator('.adv-route-current').waitFor();
   assert.match(await page.locator('.adv-route-current').innerText(),/Ancient Ruins/);assert.equal(await page.locator('.adv-route-card').count(),0);
   await page.locator('[data-action="adventure-forecast"]').first().click();
   assert.match(await page.locator('.adv-calendar-note').innerText(),/America\/New_York/);
   assert.equal(await page.locator('.adv-forecast-entry').count(),7);assert.ok(await page.locator('.adv-quiet').count()>0);
   assert.match(await page.locator('.adv-forecast-entry').first().innerText(),/Fruit Bloom.*Starts.*Oct 30.*8:00 PM EDT.*Ends/is);
   await noOverflow(page,'Forecast '+width);await capture(page,'adventure-forecast-'+width);
   await page.clock.setFixedTime(new Date('2026-10-31T00:00:00Z'));
   await page.waitForFunction(()=>document.querySelector('.adv-forecast-entry.is-active h3')?.textContent==='Fruit Bloom');
   assert.equal(await page.locator('.adv-world-banner.is-active').count(),1);assert.equal(await page.locator('.adv-atmosphere-fruit-bloom').count(),1);
   await page.clock.setFixedTime(new Date('2026-11-01T00:00:00Z'));
   await page.waitForFunction(()=>!document.querySelector('.adv-forecast-entry.is-active'));
   assert.equal(await page.locator('.adv-world-banner.is-active').count(),0);
   await page.locator('.dialog-close').click();await page.locator('[data-action="adventure-journal"]').first().click();
   await page.locator('[data-action="adventure-goal-track"][data-id="first-paths"]').click();await flush(page);
   await noOverflow(page,'Journal '+width);await capture(page,'adventure-journal-'+width);
   await page.locator('.dialog-close').click();assert.equal(await page.locator('.adv-followed-goal').count(),1);
   await page.locator('[data-action="lands"]').first().click();await page.locator('[data-action="lands-preview"][data-id="emerald-jungle"]').click();
   await page.locator('[data-action="adventure-track"]').first().click();assert.equal(await page.locator('#inspect-dialog[open]').count(),1,'existing regional adventure actions remain available');await page.locator('.dialog-close').click();
   await page.locator('[data-action="tab"][data-id="sanctuary"]').click();await page.locator('.adv-decorate summary').click();
   await page.locator('[data-action="adventure-decoration-place"][data-id="welcome-stone"][data-slot="left"]').click();await flush(page);
   assert.equal((await readSave(page)).meta.adventureLife.placements.left,'welcome-stone');assert.equal(await page.locator('.adv-decorate[open]').count(),1,'decorating stays open after a placement');
   await noOverflow(page,'Sanctuary '+width);await capture(page,'adventure-sanctuary-'+width);
   await page.reload();await page.locator('#practice-button').click();await page.locator('.adv-home-slot-left svg').waitFor();
   assert.equal((await readSave(page)).meta.adventureLife.placements.left,'welcome-stone');
   assert.deepEqual(errors,[]);
   report.push({width,routes:true,forecastBoundaries:true,journal:true,sanctuarySave:true,overflow:0});await context.close();
  }
  for(const kind of ['home','camp','battle']){
   const context=await contextFor(browser,390),page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
   await page.clock.setFixedTime(new Date(kind==='camp'?'2026-11-12T12:00:00Z':'2026-10-29T16:00:00Z'));await seedGame(page,kind);
   if(kind==='home'){
    await page.locator('[data-action="adventure-journal"]').first().click();
    const claim=page.locator('[data-action="adventure-goal-claim"][data-id="first-paths"]');assert.equal(await claim.isDisabled(),true);
    await page.locator('[data-action="adventure-goal-track"][data-id="first-paths"]').click();
    await page.locator('.adv-journal-log summary').click();
    const focused=page.locator('[data-action="adventure-goal-track"][aria-pressed="true"]');await focused.focus();
    // Feed accepted practice movement while the journal remains open, just as a GPS callback does.
    await page.locator('[data-action="map-step"][data-id="N"]').evaluate(el=>el.click());
    await page.waitForFunction(()=>!document.querySelector('[data-action="adventure-goal-claim"][data-id="first-paths"]').disabled);
    assert.equal(await page.locator('.adv-journal-log[open]').count(),1);assert.equal(await page.evaluate(()=>document.activeElement?.dataset.action),'adventure-goal-track');
    await claim.click();await flush(page);
    assert.ok((await readSave(page)).meta.adventureLife.claimed.includes('first-paths'));await page.locator('.dialog-close').click();
    await page.locator('[data-action="tab"][data-id="sanctuary"]').click();await page.locator('[data-action="adventure-sanctuary-area"][data-id="river"]').click();
    await page.locator('.adv-decorate summary').click();await page.locator('[data-action="adventure-decoration-place"][data-id="trail-lantern"][data-slot="center"]').click();await flush(page);
    assert.equal((await readSave(page)).meta.adventureLife.area,'river');assert.equal((await readSave(page)).meta.cosmetics['canopy-banner'],true);await capture(page,'adventure-river-home');
   }else if(kind==='camp'){
    await page.locator('.camp-panel').waitFor();assert.equal(await page.locator('[data-action="camp-leave"]').count(),1);assert.equal(await page.locator('[data-action="adventure-camp"]').count(),4);
    await page.locator('[data-action="adventure-camp"][data-id="meal"]').click();await flush(page);assert.equal((await readSave(page)).run.adventureLife.mealRounds,3);
    assert.equal(await page.locator('[data-action="adventure-camp"][data-id="study"]').isDisabled(),true);await capture(page,'adventure-camp');
    await page.locator('[data-action="adventure-camp"][data-id="merchant"]').click();await page.locator('#dialog-content [data-action="adventure-merchant-trade"][data-id="berry-basket"]').click();await flush(page);
    assert.equal((await readSave(page)).run.adventureLife.merchantReceipts.length,1);
    await page.clock.setFixedTime(new Date('2026-11-13T00:00:00Z'));await page.waitForFunction(()=>document.querySelector('#dialog-content .adv-merchant')?.innerText.includes('departed'));
    assert.equal(await page.locator('#dialog-content [data-action="adventure-merchant-trade"]').first().isDisabled(),true);
   }else{
    await page.locator('[data-action="ready"]').click();await page.locator('[data-action="move"][data-id="brace"]').click();
    await page.waitForFunction(()=>document.querySelector('.battle-title .tag')?.textContent.includes('2'));
    await page.locator('[data-action="move"][data-id="brace"]:not(:disabled)').waitFor();
    await page.locator('[data-action="switch-menu"]').click();await page.locator('[data-action="switch-preview"][data-id="teammate"]').click();await page.locator('[data-action="switch-confirm"][data-id="teammate"]').click();
    await page.locator('[data-action="adventure-technique"][data-id="canopy-relay"]:not(:disabled)').waitFor();
    await capture(page,'adventure-team-ready');await page.locator('[data-action="adventure-technique"][data-id="canopy-relay"]').click();await flush(page);
    assert.equal((await readSave(page)).run.lastTeamTechnique.id,'canopy-relay');assert.equal(await page.locator('.adv-technique-feedback').count(),1);await capture(page,'adventure-team-effect');
   }
   await noOverflow(page,kind);assert.deepEqual(errors,[]);report.push({scenario:kind,passed:true});await context.close();
  }
  console.log(JSON.stringify(report,null,2));
 }finally{await browser.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;});
