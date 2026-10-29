// Review and export encrypted recovery copies without replacing the current save.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),url='http://127.0.0.1:7777/game/monkey-trails.html';
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
const name='Trail Practice',password='practice-only-not-ranked';
const copies=page=>page.locator('.save-recovery .recovery-copy');
const exportButton=(page,source)=>page.locator('[data-action="recovery-export"][data-id="'+source+'"]');
async function storageRows(page,key){return page.evaluate(key=>Object.fromEntries(['','.backup','.recovery','.reopen-backup'].map(suffix=>[suffix,localStorage.getItem(key+suffix)])),key);}
async function decrypt(page,envelope){return page.evaluate(async({envelope,password})=>{
 const {derive,unseal}=await import('/game/monkey-trails/storage.mjs');return unseal(envelope,await derive(password,envelope.salt),envelope.id);
},{envelope,password});}
async function seed(page){return page.evaluate(async({name,password})=>{
 const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs'),vault=new Vault(()=>{}),state=await vault.open(name,password,{create:true,species:'capuchin',mode:'practice'});
 const balances=Object.fromEntries(Object.keys(C.ELEMENTS).map((id,index)=>[id,17+index*13])),uncommon=Object.fromEntries(Object.keys(C.ELEMENTS).map((id,index)=>['essence:'+id,index+1]));
 state.run.essence={...balances};state.run.forgeMaterials={...uncommon};state.run.fruit=90;await vault.checkpoint(state,true);
 const key='monkey-trails.v1.'+vault.id,richRaw=localStorage.getItem(key),richEnvelope=JSON.parse(richRaw);
 state.run.essence=Object.fromEntries(Object.keys(C.ELEMENTS).map(id=>[id,0]));state.run.forgeMaterials={};state.run.fruit=7;await vault.checkpoint(state,true);
 localStorage.setItem(key+'.backup',richRaw);localStorage.setItem(key+'.recovery',richRaw);
 if(!vault.localOK)throw Error('Recovery fixture could not save.');
 return{key,id:vault.id,balances,uncommon,richEnvelope,names:Object.fromEntries(Object.entries(C.ELEMENTS).map(([id,e])=>[id,e.name]))};
},{name,password});}
async function openPractice(page){await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});}
async function review(page){await page.locator('.header-settings[data-action="settings"]').click();await page.locator('#dialog-content [data-action="save-recovery"]').click();await page.locator('.save-recovery').waitFor();}
async function downloadedJSON(download){
 const stream=await download.createReadStream();if(!stream)throw Error('Backup download could not be read.');const chunks=[];for await(const chunk of stream)chunks.push(chunk);return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}
async function check(page,width){
 await page.goto(url);await page.locator('#practice-button').waitFor();const fixture=await seed(page);await openPractice(page);
 const beforeReview=await storageRows(page,fixture.key);await review(page);assert.deepEqual(await storageRows(page,fixture.key),beforeReview,'Reviewing backups writes no checkpoint');
 assert.equal(await copies(page).count(),3,'Previous, recovery and pre-reopen checkpoints are offered');
 for(const source of ['backup','recovery','reopen-backup']){
  const button=exportButton(page,source),copy=copies(page).filter({has:button});assert.match(await button.getAttribute('data-checkpoint'),/^[a-f0-9]{64}$/,'Export pins the reviewed encrypted checkpoint');
  assert.equal(await copy.locator('tbody tr').count(),8,'Every essence is compared');
  for(const[id,balance]of Object.entries(fixture.balances)){
   const row=copy.locator('tbody tr').filter({has:page.locator('th[scope="row"]').filter({hasText:new RegExp('^'+fixture.names[id]+'$')})});
   assert.deepEqual(await row.locator('td').allTextContents(),[balance+' + '+fixture.uncommon['essence:'+id]+' uncommon','0'],id+' shows exact backup and current balances');
  }
  const beforeExport=await storageRows(page,fixture.key),downloadEvent=page.waitForEvent('download');await button.click();const download=await downloadEvent;
  const exported=await downloadedJSON(download);assert.equal(exported.game,'monkey-trails');assert.equal(exported.id,fixture.id);assert.ok(download.suggestedFilename().endsWith('-'+source+'.json'));
  for(const field of ['salt','iv','cipher'])assert.equal(exported[field],fixture.richEnvelope[field],field+' exports the reviewed original envelope');
  const restored=await decrypt(page,exported);assert.deepEqual(restored.run.essence,fixture.balances,'Export decrypts to the original complete essence stock');assert.deepEqual(restored.run.forgeMaterials,fixture.uncommon,'Uncommon essence is preserved in the exported copy');
  assert.equal(restored.run.fruit,90);assert.deepEqual(await storageRows(page,fixture.key),beforeExport,'Exporting changes neither current nor retained checkpoints');
 }
 // A backup that rotated after review cannot silently export a different copy.
 const currentRow=JSON.parse((await storageRows(page,fixture.key))['']);await page.evaluate(({key,raw})=>localStorage.setItem(key+'.recovery',raw),{key:fixture.key,raw:JSON.stringify(currentRow)});
 const beforeStaleExport=await storageRows(page,fixture.key),unexpectedDownloads=[];page.on('download',download=>unexpectedDownloads.push(download));await exportButton(page,'recovery').click();
 await page.waitForFunction(()=>document.querySelector('#toast')?.textContent.includes('Review saved backups again'));
 assert.deepEqual(unexpectedDownloads,[],'Stale backup review never downloads the wrong checkpoint');assert.deepEqual(await storageRows(page,fixture.key),beforeStaleExport,'Refusing a stale export writes no checkpoint');
 await page.locator('.save-recovery [data-action="settings"]').click();await page.locator('#inspect-dialog .dialog-close').click();
 await page.locator('[data-action="tab"][data-id="pack"]').click();await page.locator('[role="tab"][data-action="pack-section"][data-id="materials"]').click();
 for(const id of Object.keys(fixture.balances))assert.match(await page.locator('[data-action="pack-material"][data-id="essence:'+id+'"] ').getAttribute('aria-label'),/, 0 owned\./,id+' current game remains unchanged after export');
 assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),width+'px has no page overflow');
 return{width,comparedEssences:8,encryptedExports:3,currentSavePreserved:true,staleExportBlocked:true};
}
(async()=>{
 const downloadsPath=path.resolve(root,'work/recovery-downloads');fs.mkdirSync(downloadsPath,{recursive:true});
 const browser=await chromium.launch({headless:true,downloadsPath,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),report=[];
 try{for(const width of [390,1280]){
  const context=await browser.newContext({viewport:{width,height:844},isMobile:width<700,hasTouch:width<700,acceptDownloads:true});
  try{
   await context.route('**/*',async route=>{const request=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(request.pathname));if(request.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});
   const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));report.push(await check(page,width));assert.deepEqual(errors,[],'No browser errors');
  }finally{await context.close();}
 }}finally{await browser.close();}
 console.log(JSON.stringify(report,null,2));
})().catch(error=>{console.error(error);process.exitCode=1;});
