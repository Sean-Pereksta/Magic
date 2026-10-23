const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),mime={'.html':'text/html','.mjs':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
const shotDir=process.env.MONKEY_TRAILS_SCREENSHOT_DIR;
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),report=[];
 try{for(const width of [320,390,1280]){
  const context=await browser.newContext({viewport:{width,height:width<500?844:1000},isMobile:width<500,hasTouch:width<500});
  await context.route('**/*',async route=>{const url=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(url.pathname));if(url.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file)){await route.abort();return;}await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});
  const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
  await page.goto('http://127.0.0.1:7777/game/monkey-trails.html');
  await page.evaluate(async()=>{
   const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs'),vault=new Vault(()=>{}),save=await vault.open('Trail Practice','practice-only-not-ranked',{create:true,mode:'practice'});
   for(const [species,stage]of [['claypot',0],['lumenmoth',0],['chronogibbon',3]]){
    const p=structuredClone(save.run.monkey);p.id=species;p.species=species;p.types=[...C.SPECIES[species].types];p.level=10+stage;p.stage=stage;p.learned=[];if(stage===3)p.mastery=p.types[0];p.learned=C.unlocked(p);p.loadout=p.learned.slice(0,4);p.hp=C.stats(p).hp;p.highestForm={stage,types:p.types};save.collection[p.id]=p;
   }
   C.setParty(save,['claypot','lumenmoth','chronogibbon']);await vault.checkpoint(save,true);
  });
  await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});
  await page.locator('[data-action="tab"][data-id="team"]').click();await page.locator('[data-action="companion-section"][data-id="abilities"]').click();
  assert.equal(await page.locator('.loadout [data-action="ability-open"]').count(),4);
  assert.equal(await page.locator('.loadout select').count(),0);
  await page.locator('.loadout [data-ability-slot="1"]').click();await page.locator('.ability-picker').waitFor();
  assert.equal(await page.locator('.ability-comparison .ability-card').count(),2);
  assert.equal(await page.locator('[data-action="ability-equip"][data-id="strike"]').isDisabled(),true);
  const option=page.locator('[data-action="ability-equip"]:not([disabled])').first(),move=await option.getAttribute('data-id');
  await page.locator(`[data-action="ability-compare"][data-id="${move}"]`).click();
  assert.equal(await page.locator('#ability-proposed .ability-card').count(),1);
  await option.click();await page.locator('#inspect-dialog').waitFor({state:'hidden'});
  assert.equal(await page.locator('.loadout [data-ability-slot="1"]').getAttribute('data-id'),move);
  await page.locator('.loadout [data-ability-slot="0"]').click();
  assert.equal(await page.locator('[data-action="ability-equip"]:not([disabled])').count(),0);
  await page.keyboard.press('Escape');
  await page.locator('.loadout [data-ability-slot="2"]').click();
  assert.equal(await page.locator(`[data-action="ability-equip"][data-id="${move}"]`).isDisabled(),true);
  await page.locator('.ability-picker-actions [data-action="close-dialog"]').click();
  await page.reload();await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});
  await page.locator('[data-action="tab"][data-id="team"]').click();await page.locator('[data-action="companion-section"][data-id="abilities"]').click();
  assert.equal(await page.locator('.loadout [data-ability-slot="1"]').getAttribute('data-id'),move);
  await page.locator('[data-action="tab"][data-id="sanctuary"]').click();await page.locator('[data-action="collection-filter"][data-id="all"]').click();
  const identities=new Set();for(let pageIndex=0;pageIndex<8;pageIndex++){for(const id of await page.locator('.collection-card').evaluateAll(cards=>cards.map(card=>card.dataset.id)))identities.add(id);const next=page.locator('[data-action="collection-page"][data-id="1"]');if(await next.isDisabled())break;await next.click();}
  assert.equal(identities.size,90);
  await page.locator('#collection-rarity').selectOption('ultra');assert.equal(await page.locator('.collection-card').count(),12);
  await page.locator('#collection-element').selectOption('storm');assert.ok(await page.locator('.collection-card').count()>0);
  await page.locator('[data-action="collection-reset"]').click();await page.locator('#collection-search').fill('lumenmoth');
  assert.equal(await page.locator('.collection-card').count(),1);await page.locator('.collection-card').click();
  assert.equal(await page.locator('#selected-companion .loadout [data-action="ability-open"]').count(),4);
  await page.locator('#selected-companion .loadout [data-ability-slot="1"]').click();
  assert.ok(await page.locator('.ability-locked').count());
  const sizes=await page.evaluate(()=>({width:innerWidth,page:document.documentElement.scrollWidth,dialog:document.querySelector('#inspect-dialog').clientWidth,scroll:document.querySelector('#inspect-dialog').scrollWidth}));
  assert.ok(sizes.page<=sizes.width,JSON.stringify(sizes));assert.ok(sizes.scroll<=sizes.dialog+1,JSON.stringify(sizes));
  await page.locator('#inspect-dialog').evaluate(async dialog=>{await Promise.all(dialog.getAnimations().filter(a=>a.effect?.getTiming().iterations!==Infinity).map(a=>a.finished.catch(()=>{})));});
  if(shotDir&&width!==320){fs.mkdirSync(shotDir,{recursive:true});await page.screenshot({path:path.join(shotDir,`ability-picker-${width}.png`),fullPage:false});}
  await page.keyboard.press('Escape');await page.locator('[data-action="collection-reset"]').click();await page.locator('#collection-sort').selectOption('level');
  assert.equal(await page.locator('.collection-card').first().getAttribute('data-id'),'chronogibbon');
  await page.locator('#collection-sort').selectOption('recent');assert.equal(await page.locator('.collection-card').first().getAttribute('data-id'),'chronogibbon');
  if(width===390){
   await page.reload();
   await page.evaluate(async()=>{const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs'),v=new Vault(()=>{}),s=await v.open('Trail Practice','practice-only-not-ranked',{mode:'practice'});C.setParty(s,['chronogibbon','claypot','lumenmoth']);s.run.activeId='chronogibbon';s.run.monkey=s.collection.chronogibbon;const p=s.run.monkey;p.loadout=['strike','zeroHourRelay','brace','breathe'];p.energy=C.stats(p).maxEnergy;C.advance(s,C.ENCOUNTER_STEP,0,0);C.beginPendingEncounter(s);C.readyBattle(s);const b=s.run.battle;b.enemy.hp=10000;b.enemy.loadout=['strike','breathe'];b.enemyAction={type:'move',id:'breathe'};b.intent='breathe';await v.checkpoint(s,true);});
   await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});
   await page.locator('[data-action="ready"]').click();
   assert.equal(await page.locator('[data-action="move"][data-id="zeroHourRelay"]').count(),1,await page.locator('#view').innerText());
   await page.locator('[data-action="move"][data-id="zeroHourRelay"]').click();await page.locator('[data-action="switch-preview"][data-id="claypot"]').click();await page.locator('[data-action="switch-confirm"]').click();
   await page.locator('.fighter:not(.enemy-fighter) [data-species="chronogibbon"][data-state="signature"]').waitFor();
   assert.equal((await page.locator('.battle-title .tag').innerText()).trim(),'ROUND 1');
   await page.locator('.fighter:not(.enemy-fighter) [data-species="claypot"]').waitFor();
   assert.equal((await page.locator('.battle-title .tag').innerText()).trim(),'ROUND 2');
   report.push({relaySignature:'outgoing Chronogibbon',roundsSpent:1});
  }
  assert.deepEqual(errors,[]);report.push({width,roster:identities.size,persistedReplacement:move,errors:0,overflow:sizes.page-sizes.width});await context.close();
 }}finally{await browser.close();}
 console.log(JSON.stringify(report,null,2));
})().catch(error=>{console.error(error);process.exitCode=1;});
