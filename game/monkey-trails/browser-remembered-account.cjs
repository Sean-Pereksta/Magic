// Real IndexedDB/CryptoKey persistence and starting-menu interactions. Cloud
// access is replaced at the connection boundary; no Firebase service is used.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),origin='http://127.0.0.1:7778';
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
let online=true;const cloud=new Map();
async function login(page,name,{remember=true,create=false}={}){
  await page.locator(`[data-action="welcome-${create?'new':'continue'}"]`).click();
  await page.locator('#creature-name').fill(name);await page.locator('#creature-password').fill('browser-test-password');
  await page.locator('#remember-account').setChecked(remember);
  await page.locator(`#save-form [value="${create?'create':'open'}"]`).click();
  await page.locator('#game').waitFor({state:'visible'});await page.waitForFunction(()=>!globalThis.__rememberDebug.busy);
}
async function startingMenu(page){
  await page.locator('.header-settings').click();await page.locator('[data-action="start-menu"]').click();
  await page.locator('#welcome').waitFor({state:'visible'});
}
async function staleRefresh(page){
  await page.evaluate(async()=>{
    const d=globalThis.__rememberDebug,load=d.deviceAccount.load.bind(d.deviceAccount),old=await load();
    d.deviceAccount.load=()=>new Promise(resolve=>{d.releaseStaleRead=()=>resolve(old);});
    d.staleRefresh=d.refreshRememberedAccount();d.deviceAccount.load=load;
  });
}
async function releaseStaleRefresh(page){await page.evaluate(async()=>{const d=globalThis.__rememberDebug;d.releaseStaleRead();await d.staleRefresh;});}
(async()=>{
  const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})});
  try{
    const context=await browser.newContext({viewport:{width:390,height:844},isMobile:true,hasTouch:true});
    await context.exposeFunction('__rememberCloudRead',id=>{if(!online)throw Error('offline');return cloud.get(id)||null;});
    await context.exposeFunction('__rememberCloudCommit',(id,base,envelope)=>{if(!online)throw Error('offline');const revision=(cloud.get(id)?.revision||0)+1;assert.equal(revision,base+1);cloud.set(id,{...envelope,revision});return revision;});
    await context.addInitScript(()=>{
      globalThis.__rememberTestCloud=async()=>({read:id=>globalThis.__rememberCloudRead(id),commit:(id,base,envelope)=>globalThis.__rememberCloudCommit(id,base,envelope)});
    });
    await context.route('**/*',async route=>{
      const url=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(url.pathname));
      if(url.origin!==origin||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();
      let body=fs.readFileSync(file);
      if(url.pathname==='/game/monkey-trails/app.mjs'){
        let source=body.toString();
        assert.ok(source.includes('new Vault(saveStatus)'));assert.ok(source.includes('new SocialController({'));
        source=source.replaceAll('new Vault(saveStatus)','new Vault(saveStatus,localStorage,globalThis.__rememberTestCloud)');
        source=source.replace('new SocialController({',"new SocialController({connect:async()=>{throw Error('Offline browser fixture');},");
        source+='\nglobalThis.__rememberDebug={get state(){return state;},get vault(){return vault;},get busy(){return busy;},get remembered(){return rememberedAccount;},deviceAccount,refreshRememberedAccount};\n';body=source;
      }
      await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body});
    });
    const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
    await page.goto(origin+'/game/monkey-trails.html');await page.locator('#practice-button').waitFor();
    assert.equal(await page.locator('#remembered-account').isVisible(),false);
    await login(page,'Mossfoot',{create:true});
    const first=await page.evaluate(async()=>{
      const d=globalThis.__rememberDebug,row=await d.deviceAccount.load();
      d.state.run.fruit+=23;await d.vault.checkpoint(d.state);await d.vault.sync();
      return {keys:Object.keys(row).sort(),extractable:row.key.extractable,name:row.name,fruit:d.state.run.fruit};
    });
    assert.equal(first.extractable,false);assert.equal(first.name,'Mossfoot');assert.ok(!first.keys.includes('password'));
    online=false;await page.reload();await page.locator('#remembered-account').waitFor({state:'visible'});
    assert.equal(await page.locator('#game').isVisible(),false);assert.equal(await page.locator('#welcome-setup').isVisible(),false);
    assert.equal(await page.locator('#reopen-account').textContent(),'Reopen Mossfoot');
    await page.locator('#reopen-account').click();await page.locator('#game').waitFor({state:'visible'});
    assert.equal(await page.evaluate(()=>globalThis.__rememberDebug.state.run.fruit),first.fruit);
    assert.equal(await page.locator('#creature-password').inputValue(),'');
    await startingMenu(page);await page.locator('#remembered-account').waitFor({state:'visible'});
    await staleRefresh(page);
    await page.locator('#different-account').click();assert.equal(await page.locator('#creature-name').inputValue(),'');
    assert.equal(await page.locator('#creature-password').inputValue(),'');
    online=true;await login(page,'Leafwalker',{create:true});await startingMenu(page);
    await page.locator('#remembered-account').waitFor({state:'visible'});await releaseStaleRefresh(page);
    assert.equal(await page.locator('#remembered-account-name').textContent(),'Leafwalker');
    assert.equal(await page.evaluate(()=>globalThis.__rememberDebug.remembered.name),'Leafwalker');
    await page.reload();
    await page.locator('#remembered-account').waitFor({state:'visible'});assert.equal(await page.locator('#remembered-account-name').textContent(),'Leafwalker');
    await staleRefresh(page);
    await page.locator('#forget-account').click();await page.waitForFunction(()=>!globalThis.__rememberDebug.busy);
    await releaseStaleRefresh(page);assert.equal(await page.evaluate(()=>globalThis.__rememberDebug.remembered),null);
    assert.equal(await page.locator('#remembered-account').isVisible(),false);
    assert.match(await page.locator('#login-message').textContent(),/encrypted saves were kept/);
    await page.reload();await page.locator('#practice-button').waitFor();assert.equal(await page.locator('#remembered-account').isVisible(),false);
    await login(page,'Leafwalker');await startingMenu(page);await page.locator('#remembered-account').waitFor({state:'visible'});
    await staleRefresh(page);await login(page,'Leafwalker',{remember:false});await startingMenu(page);await releaseStaleRefresh(page);
    assert.equal(await page.evaluate(()=>globalThis.__rememberDebug.remembered),null);
    await page.reload();await page.locator('#practice-button').waitFor();
    assert.equal(await page.locator('#remembered-account').isVisible(),false);assert.equal(await page.locator('#game').isVisible(),false);
    await login(page,'Mossfoot');assert.equal(await page.evaluate(()=>globalThis.__rememberDebug.state.run.fruit),first.fruit);
    const dimensions=await page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth}));
    assert.ok(dimensions.scroll<=dimensions.width,'Mobile viewport should not overflow');assert.deepEqual(errors,[]);
    await context.close();console.log('Remembered account browser checks passed: persisted non-exportable key, explicit offline reopen, start menu, account switch, forget, opt-out, retained saves, stale-refresh generations.');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
