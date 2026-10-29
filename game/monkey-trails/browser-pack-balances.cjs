// Guard every existing essence balance through Pack browsing, previews and save reloads.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),url='http://127.0.0.1:7777/game/monkey-trails.html';
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
const name='Trail Practice',password='practice-only-not-ranked';
const section=(page,id)=>page.locator('[role="tab"][data-action="pack-section"][data-id="'+id+'"]');
const recipe=(page,id)=>page.locator('[data-supply-recipe="'+id+'"]');
async function saved(page){return page.evaluate(async({name,password})=>{
 const {identity,derive,unseal}=await import('/game/monkey-trails/storage.mjs'),id=await identity(name,'practice'),envelope=JSON.parse(localStorage.getItem('monkey-trails.v1.'+id));
 if(!envelope)throw Error('Missing encrypted checkpoint.');return unseal(envelope,await derive(password,envelope.salt),id);
},{name,password});}
async function assertSaved(page,expected,label){assert.deepEqual((await saved(page)).run.essence,expected,label+' preserves every essence balance');}
async function waitSaved(page,expected,berryCount){
 const deadline=Date.now()+12000;let actual;
 while(Date.now()<deadline){actual=await saved(page);if(actual.run.items.berry===berryCount){assert.deepEqual(actual.run.essence,expected,'A confirmed recipe spends only its selected essence');return;}await page.waitForTimeout(50);}
 throw Error('Craft checkpoint was not saved: '+JSON.stringify(actual.run.essence));
}
async function openPractice(page){await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});}
async function openPack(page){await page.locator('[data-action="tab"][data-id="pack"]').click();await page.locator('#supply-target').waitFor();}
async function assertMaterials(page,expected){
 await section(page,'materials').click();
 for(const[id,quantity]of Object.entries(expected)){
  const tile=page.locator('[data-action="pack-material"][data-id="essence:'+id+'"]');
  assert.ok((await tile.getAttribute('aria-label')).includes(quantity+' owned'),id+' accessible owned quantity');
  assert.match(await tile.innerText(),new RegExp('×'+quantity+'(?:\\D|$)'),id+' visible owned quantity');
 }
}
async function check(page,width){
 await page.goto(url);await page.locator('#practice-button').waitFor();
 const fixture=await page.evaluate(async({name,password})=>{
  const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs'),vault=new Vault(()=>{}),state=await vault.open(name,password,{create:true,species:'capuchin',mode:'practice'});
  const expected=Object.fromEntries(Object.keys(C.ELEMENTS).map((id,index)=>[id,11+index*7]));
  state.run.essence={...expected};state.run.fruit=200;state.run.items.charmMaterial=40;state.run.items.berry=0;
  await vault.checkpoint(state,true);if(!vault.localOK)throw Error('Could not persist stocked fixture.');
  return{essence:expected,names:Object.fromEntries(Object.entries(C.ELEMENTS).map(([id,e])=>[id,e.name])),berryCost:C.CONSUMABLE_RECIPES.berry.essence};
 },{name,password});
 const expected={...fixture.essence};assert.equal(new Set(Object.values(expected)).size,8,'Each essence starts with a distinct nonzero balance');
 await openPractice(page);await openPack(page);await assertSaved(page,expected,'Opening the existing save and Pack');
 for(const id of ['equipment','materials','supplies','forge','materials','equipment','supplies']){await section(page,id).click();await assertSaved(page,expected,'Switching to '+id);}
 for(const id of ['berry','revive','bondCharm']){
  await page.locator('[data-supply-card="'+id+'"] .supply-craft').click();await recipe(page,id).waitFor();
  if(id!=='bondCharm')for(const[element,quantity]of Object.entries(expected)){
   await page.locator('#consumable-essence').selectOption(element);
   assert.equal(await recipe(page,id).locator('[data-recipe-ingredient="essence"]').getAttribute('data-owned'),String(quantity),element+' recipe displays existing balance');
   await assertSaved(page,expected,'Changing recipe essence to '+element);
  }
  await recipe(page,id).locator('[aria-label^="Close"]').click();await assertSaved(page,expected,'Closing '+id+' recipe');
 }
 await assertMaterials(page,expected);
 for(const[element,quantity]of Object.entries(expected)){
  await page.locator('[data-action="pack-material"][data-id="essence:'+element+'"] ').click();
  assert.ok((await page.locator('.pack-material-detail').innerText()).includes('×'+quantity+' owned'),element+' inspection shows owned balance');
  await page.locator('#inspect-dialog .dialog-close').click();await assertSaved(page,expected,'Inspecting and closing '+element);
 }
 await section(page,'forge').click();await page.locator('[data-forge-filter="essence"]').click();
 for(const[element,quantity]of Object.entries(expected)){
  const tile=page.locator('[data-forge-item="essence:'+element+'"]');await tile.scrollIntoViewIfNeeded();await tile.click();
  assert.equal(await page.locator('[data-forge-picker-number]').getAttribute('max'),String(quantity),element+' forge picker retains full stock');
  await page.locator('[data-forge-picker-number]').fill(String(quantity));await page.locator('[data-forge-picker-cancel]').click();
  await assertSaved(page,expected,'Cancelling '+element+' forge selection');
 }
 // Loading ingredients is a preview reservation; it must not consume saved stock.
 await page.locator('[data-forge-item="essence:water"]').click();await page.locator('[data-forge-picker-number]').fill(String(expected.water));await page.locator('[data-forge-picker-confirm]').click();
 await assertSaved(page,expected,'Loading all water into an uncommitted forge mix');await assertMaterials(page,expected);
 await section(page,'forge').click();await page.locator('[data-action="forge-clear"]').click();await assertMaterials(page,expected);
 await section(page,'supplies').click();await page.locator('[data-supply-card="berry"] .supply-craft').click();
 let berryCount=0;
 for(const element of Object.keys(expected)){
  await page.locator('#consumable-essence').selectOption(element);await recipe(page,'berry').locator('[data-action="craft-consumable"]').click();expected[element]-=fixture.berryCost;berryCount++;
  await waitSaved(page,expected,berryCount);
  assert.equal(await recipe(page,'berry').locator('[data-recipe-ingredient="essence"]').getAttribute('data-owned'),String(expected[element]),element+' updates immediately after confirmed crafting');
 }
 await assertMaterials(page,expected);await page.reload();await openPractice(page);await openPack(page);await assertSaved(page,expected,'Reloading the saved Pack');await assertMaterials(page,expected);
 await page.locator('.header-settings[data-action="settings"]').click();await page.locator('#dialog-content [data-action="start-menu"]').click();await page.locator('#welcome').waitFor({state:'visible'});
 await openPractice(page);await openPack(page);await assertSaved(page,expected,'Returning to the menu and reopening the same save');await assertMaterials(page,expected);
 assert.equal((await saved(page)).run.items.berry,8,'All eight intentional recipes persist without resetting other balances');
 assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),width+'px has no horizontal overflow');
 return{width,essences:8,navigationAndPreviewPreserve:true,selectedCraftSpending:true,reloadPreserves:true,menuReopenPreserves:true};
}
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),report=[];
 try{for(const width of [390,1280]){
  const context=await browser.newContext({viewport:{width,height:844},isMobile:width<700,hasTouch:width<700});
  try{
   await context.route('**/*',async route=>{const request=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(request.pathname));if(request.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});
   const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));report.push(await check(page,width));assert.deepEqual(errors,[],'No browser errors');
  }finally{await context.close();}
 }}finally{await browser.close();}
 console.log(JSON.stringify(report,null,2));
})().catch(error=>{console.error(error);process.exitCode=1;});
