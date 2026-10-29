// Pack journeys: preparation, keyboard navigation, exact recipe spending, and safe saves.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),url='http://127.0.0.1:7777/game/monkey-trails.html';
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
const fixtureName='Trail Practice',fixturePassword='practice-only-not-ranked';
const section=(page,id)=>page.locator('[role="tab"][data-action="pack-section"][data-id="'+id+'"]');
const card=(page,id)=>page.locator('[data-supply-card="'+id+'"]');
const recipe=(page,id)=>page.locator('[data-supply-recipe="'+id+'"]');
const count=(page,id)=>page.locator('[data-supply-count="'+id+'"]');

async function readSaved(page){return page.evaluate(async({name,password})=>{
 const {identity,derive,unseal}=await import('/game/monkey-trails/storage.mjs'),id=await identity(name,'practice'),envelope=JSON.parse(localStorage.getItem('monkey-trails.v1.'+id));
 if(!envelope)throw Error('Encrypted fixture missing.');return unseal(envelope,await derive(password,envelope.salt),id);
},{name:fixtureName,password:fixturePassword});}
async function waitForSaved(page,predicate,label){
 let saved;const deadline=Date.now()+12000;
 while(Date.now()<deadline){saved=await readSaved(page);if(predicate(saved))return saved;await page.waitForTimeout(60);}
 throw Error('Encrypted save did not reach '+label+'; '+await page.locator('#save-status').innerText());
}
async function seed(page,scenario){return page.evaluate(async({name,password,scenario})=>{
 const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs'),v=new Vault(()=>{}),s=await v.open(name,password,{create:true,species:'capuchin',mode:'practice'});
 s.run.phase='trail';s.run.fruit=50;s.run.items.charmMaterial=8;
 for(const id of Object.keys(C.ELEMENTS))s.run.essence[id]=0;s.run.essence.water=8;s.run.essence.fire=8;
 for(const id of ['berry','revive','bondCharm'])s.run.items[id]=0;s.run.items.trailRibbon=1;
 const lead=s.run.monkey;lead.nickname='Lead';lead.hp=1;
 const fallen=structuredClone(C.newSave('Fallen','macaque','practice',17).run.monkey);fallen.id='pack-fallen';fallen.nickname='Fallen';s.collection[fallen.id]=fallen;C.setParty(s,[lead.id,fallen.id]);fallen.hp=0;
 s.run.injuries[fallen.id]={hp:0,energy:fallen.energy,status:{}};
 if(scenario==='capture'){
  s.run.fruit=2;s.run.items.charmMaterial=1;s.run.phase='befriend';
  const candidate=structuredClone(C.newSave('Candidate','macaque','practice',19).run.monkey);candidate.id='pack-candidate';
  s.run.capture={id:'pack-capture',candidates:[candidate],attempts:0,maxAttempts:3,guaranteed:true,ultra:false,claimed:false};
 }
 await v.checkpoint(s,true);if(!v.localOK)throw Error('Fixture could not save.');return{lead:lead.id,fallen:fallen.id};
},{name:fixtureName,password:fixturePassword,scenario});}
async function openPractice(page){await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});}
async function openPack(page){await page.locator('[data-action="tab"][data-id="pack"]').click();await page.locator('#supply-target').waitFor();}
async function openRecipe(page,id){await card(page,id).locator('.supply-craft[data-action="supply-recipe"]').click();await recipe(page,id).waitFor();}
async function assertCount(page,id,expected){assert.match(await count(page,id).innerText(),new RegExp('(?:^|\\D)'+expected+'(?:\\D|$)'),id+' visible owned count');}
async function assertRecipe(page,id,essence){
 const expected=await page.evaluate(async({state,id,essence})=>{
  const C=await import('/game/monkey-trails/core.mjs'),r=C.CONSUMABLE_RECIPES[id];
  return{fruit:[state.run.fruit,r.fruit],...(r.material?{material:[state.run.items.charmMaterial,r.material]}:{}),...(r.essence?{essence:[state.run.essence[essence],r.essence]}:{})};
 },{state:await readSaved(page),id,essence});
 for(const[k,[owned,required]]of Object.entries(expected)){
  const row=recipe(page,id).locator('[data-recipe-ingredient="'+k+'"]');
  assert.equal(await row.getAttribute('data-owned'),String(owned),id+' '+k+' owned quantity');assert.equal(await row.getAttribute('data-required'),String(required),id+' '+k+' recipe requirement');
  assert.ok((await row.innerText()).includes(String(owned)),id+' '+k+' visibly shows owned amount');assert.ok((await row.innerText()).includes(String(required)),id+' '+k+' visibly shows cost');
 }
}
async function checkNavigation(page){
 const tabs=page.locator('.pack-tabs [role="tab"]');assert.deepEqual(await tabs.allTextContents(),['Supplies','Equipment','Forge','Materials']);
 assert.equal(await section(page,'supplies').getAttribute('aria-selected'),'true','Pack starts on Supplies');
 await section(page,'supplies').focus();await page.keyboard.press('ArrowRight');assert.equal(await section(page,'equipment').getAttribute('aria-selected'),'true');assert.equal(await section(page,'equipment').evaluate(el=>el===document.activeElement),true);
 assert.equal(await section(page,'equipment').evaluate(el=>getComputedStyle(el).outlineStyle!=='none'&&parseFloat(getComputedStyle(el).outlineWidth)>=2),true,'Keyboard focus remains visible');
 for(const tab of await tabs.all()){const size=await tab.boundingBox();assert.ok(size.width>=44&&size.height>=44,'Navigation supports phone tap targets');}
 await page.keyboard.press('End');assert.equal(await section(page,'materials').getAttribute('aria-selected'),'true');
 await page.keyboard.press('Home');assert.equal(await section(page,'supplies').getAttribute('aria-selected'),'true');
 await page.keyboard.press('ArrowLeft');assert.equal(await section(page,'materials').getAttribute('aria-selected'),'true','Left wraps to last section');
 await section(page,'equipment').click();await page.locator('.inventory-craft-link').click();await page.locator('[data-forge-panel]').waitFor();assert.equal(await section(page,'forge').getAttribute('aria-selected'),'true','Equipment opens the actual Forge section');
 await section(page,'supplies').click();assert.equal(await page.locator('#supply-target').count(),1);
}
async function checkPreparation(page,fixture,width){
 await openPack(page);await checkNavigation(page);
 for(const id of ['berry','revive','bondCharm']){
  await assertCount(page,id,0);assert.equal(await card(page,id).locator('[data-action="supply-recipe"]').isEnabled(),true,id+' can be crafted when none are owned');
 }
 assert.equal(await card(page,'bondCharm').locator('[data-action="item"]').count(),0,'Charm explains recruitment instead of presenting an unusable Use action');assert.match(await card(page,'bondCharm').innerText(),/after.*battle|post.battle/i);
 await openRecipe(page,'berry');await page.locator('#consumable-essence').selectOption('leaf');await assertRecipe(page,'berry','leaf');
 assert.equal(await recipe(page,'berry').locator('[data-action="craft-consumable"]').isDisabled(),true);assert.match(await recipe(page,'berry').locator('[data-consumable-reason]').innerText(),/not enough.*leaf.*essence/i);
 await page.locator('#consumable-essence').selectOption('water');await assertRecipe(page,'berry','water');assert.equal(await recipe(page,'berry').locator('[data-action="craft-consumable"]').isEnabled(),true);
 assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),width+'px open recipe fits horizontally');
 const output=process.env.MONKEY_TRAILS_SCREENSHOT_DIR;if(output){fs.mkdirSync(output,{recursive:true});await page.screenshot({path:path.join(output,width<700?'pack-phone.png':'pack-desktop.png'),fullPage:false});}
 await recipe(page,'berry').locator('[data-action="craft-consumable"]').click();await assertCount(page,'berry',1);assert.equal(await recipe(page,'berry').isVisible(),true,'Crafting preserves the open recipe');assert.equal(await page.locator('#consumable-essence').inputValue(),'water');
 let saved=await waitForSaved(page,s=>s.run.items.berry===1,'crafted berry');assert.equal(saved.run.fruit,46);assert.equal(saved.run.essence.water,6);assert.equal(saved.run.essence.fire,8);await assertRecipe(page,'berry','water');
 await openRecipe(page,'revive');await page.locator('#consumable-essence').selectOption('fire');await assertRecipe(page,'revive','fire');await recipe(page,'revive').locator('[data-action="craft-consumable"]').click();await assertCount(page,'revive',1);
 saved=await waitForSaved(page,s=>s.run.items.revive===1,'crafted revival seed');assert.equal(saved.run.fruit,40);assert.equal(saved.run.items.charmMaterial,6);assert.equal(saved.run.essence.fire,5);
 await openRecipe(page,'bondCharm');await assertRecipe(page,'bondCharm');await recipe(page,'bondCharm').locator('[data-action="craft-consumable"]').click();await assertCount(page,'bondCharm',1);
 saved=await waitForSaved(page,s=>s.run.items.bondCharm===1,'crafted charm');assert.equal(saved.run.fruit,38);assert.equal(saved.run.items.charmMaterial,5);
 await page.locator('#supply-target').selectOption(fixture.lead);await card(page,'berry').locator('[data-action="item"]').click();await assertCount(page,'berry',0);
 saved=await waitForSaved(page,s=>s.run.items.berry===0,'used berry');assert.ok(saved.collection[fixture.lead].hp>1);assert.equal(saved.collection[fixture.fallen].hp,0,'Berry affects selected teammate only');
 assert.equal(await card(page,'revive').locator('[data-action="item"]').isDisabled(),true,'Conscious target cannot consume a Revival Seed');assert.match(await card(page,'revive').innerText(),/conscious|knocked.out/i);
 await page.locator('#supply-target').selectOption(fixture.fallen);assert.equal(await card(page,'revive').locator('[data-action="item"]').isEnabled(),true);await card(page,'revive').locator('[data-action="item"]').click();await assertCount(page,'revive',0);
 saved=await waitForSaved(page,s=>s.run.items.revive===0,'used revival seed');assert.ok(saved.collection[fixture.fallen].hp>0);
 const activate=card(page,'trailRibbon').locator('[data-action="item"]');assert.equal(await activate.innerText(),'Activate');await activate.focus();await page.keyboard.press('Enter');await assertCount(page,'trailRibbon',0);saved=await waitForSaved(page,s=>s.run.items.trailRibbon===0,'activated ribbon');assert.ok(saved.run.bonuses.ribbon>0);assert.equal(await page.locator('#supply-target').evaluate(el=>el===document.activeElement),true,'Using the last walking supply moves keyboard focus to the recovery target');
 // An open recipe and selected essence survive visiting a different Pack section.
 await openRecipe(page,'berry');await page.locator('#consumable-essence').selectOption('water');await section(page,'materials').click();assert.match(await page.locator('#pack-content').innerText(),/Crafting materials/);await section(page,'supplies').click();assert.equal(await recipe(page,'berry').isVisible(),true);assert.equal(await page.locator('#consumable-essence').inputValue(),'water');
 await section(page,'materials').click();await page.locator('[data-action="pack-material"][data-id="essence:water"]').click();assert.match(await page.locator('.pack-material-detail').innerText(),/6 owned/);await page.locator('#dialog-content [data-action="consumable-open-craft"][data-id="berry"]').click();assert.equal(await recipe(page,'berry').isVisible(),true);assert.equal(await page.locator('#consumable-essence').inputValue(),'water','Inspecting essence finds a recipe using that essence');
 await section(page,'materials').click();await page.locator('[data-action="pack-material"][data-id="item:charmMaterial"]').click();assert.match(await page.locator('.pack-material-detail').innerText(),/5 owned/);await page.locator('#dialog-content [data-action="consumable-open-craft"][data-id="bondCharm"]').click();assert.equal(await recipe(page,'bondCharm').isVisible(),true,'Charm material leads directly to Bond Charm crafting');
 await page.reload();await openPractice(page);await openPack(page);await assertCount(page,'berry',0);await assertCount(page,'revive',0);await assertCount(page,'bondCharm',1);const reloaded=await readSaved(page);assert.equal(reloaded.run.fruit,38);assert.equal(reloaded.run.items.charmMaterial,5);assert.equal(reloaded.run.essence.water,6);assert.equal(reloaded.run.essence.fire,5);assert.equal(reloaded.collection[fixture.fallen].hp,saved.collection[fixture.fallen].hp);
}
async function checkQuickCharm(page){
 const nickname=page.locator('#capture-name-pack-candidate');await nickname.fill('Willow');await page.locator('[data-action="craft-capture-bond"]').click();assert.equal(await nickname.inputValue(),'Willow','Quick crafting preserves the typed nickname');
 assert.equal(await page.locator('[data-capture-charm-count]').innerText(),'1');assert.equal(await page.locator('[data-capture-fruit]').innerText(),'0');assert.equal(await page.locator('[data-capture-material]').innerText(),'0');assert.equal(await page.locator('[data-action="craft-capture-bond"]').isDisabled(),true);
 const saved=await waitForSaved(page,s=>s.run.items.bondCharm===1,'quick charm');assert.equal(saved.run.capture.id,'pack-capture');assert.equal(saved.run.capture.attempts,0);assert.equal(saved.run.phase,'befriend');
 await page.locator('[data-action="befriend"][data-id="pack-candidate"]').click();await waitForSaved(page,s=>Object.values(s.collection).some(p=>p.nickname==='Willow'),'recruited nickname');
}

(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),report=[];
 try{for(const[scenario,width]of [['preparation',320],['preparation',1280],['capture',390]]){
  const context=await browser.newContext({viewport:{width,height:844},isMobile:width<700,hasTouch:width<700});
  try{
   await context.route('**/*',async route=>{const request=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(request.pathname));if(request.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});
   const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));await page.goto(url);await page.locator('#practice-button').waitFor();const fixture=await seed(page,scenario);await openPractice(page);
   if(scenario==='preparation')await checkPreparation(page,fixture,width);else await checkQuickCharm(page);
   assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),width+'px has no horizontal overflow');assert.deepEqual(errors,[]);report.push({scenario,width,errors:0,overflow:0});
  }finally{await context.close();}
 }}finally{await browser.close();}
 console.log(JSON.stringify(report,null,2));
})().catch(error=>{console.error(error);process.exitCode=1;});
