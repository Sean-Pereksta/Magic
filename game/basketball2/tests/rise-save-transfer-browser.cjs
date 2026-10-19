/* Portable saves through the real file controls. Runtime overrides match rise-browser.cjs. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'../../..');

(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage({viewport:{width:1440,height:950},acceptDownloads:true}),errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.route('https://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      return route.fulfill({path:file,contentType:file.endsWith('.html')?'text/html':file.endsWith('.css')?'text/css':file.endsWith('.glb')?'model/gltf-binary':file.endsWith('.webp')?'image/webp':'text/javascript'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
    await page.goto('https://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>typeof RiseToGlory!=='undefined'&&typeof buildRiseUI==='function');
    await page.locator('#primaryRiseBtn').click();
    assert.equal(await page.locator('#riseCreationForm').count(),1);
    assert.ok(await page.locator('.rise-art-heading [data-rise=import-open]').isVisible(),'new devices offer import before creating a club');
    await page.locator('#riseCreationForm [name=name]').fill('Travel Comets');
    await page.locator('#riseCreationForm button[type=submit]').click();
    await page.evaluate(()=>{
      const captain=RiseToGlory.player(riseState,riseState.captainId);
      RiseToGlory.setRole(riseState,captain.id,'shooter',3);captain.abilities.catchAndShoot=2;captain.abilities.quickRelease=1;captain.upgradePoints=9;
      const game=RiseToGlory.nextGame(riseState);
      RiseToGlory.recordGame(riseState,{gameId:game.id,won:true,homeScore:game.targetScore,awayScore:5,players:[{id:captain.id,stats:{points:9,threeMade:3,threeAttempts:4}}]});
      RiseToGlory.save(riseState);localStorage.setItem('basketballNationalTourV2','tour unchanged');showRiseCampaign('hub');
    });
    const downloadPromise=page.waitForEvent('download');await page.locator('[data-rise=export-save]').click();const download=await downloadPromise;
    assert.match(download.suggestedFilename(),/^basketball2-travel-comets-season-1\.json$/);
    const stream=await download.createReadStream(),chunks=[];for await(const chunk of stream)chunks.push(chunk);
    const portable=Buffer.concat(chunks),exported=JSON.parse(portable.toString('utf8'));
    assert.equal(exported.save.team.name,'Travel Comets');assert.equal(exported.save.players[exported.save.captainId].role,'shooter');
    const current=await page.evaluate(()=>{
      riseState=RiseToGlory.create({name:'Keep My Club',seed:31});RiseToGlory.save(riseState);showRiseCampaign('hub');
      return {save:localStorage.getItem(RiseToGlory.SAVE_KEY),backup:localStorage.getItem(RiseToGlory.BACKUP_KEY)};
    });
    const upload=()=>page.locator('#riseImportFile').setInputFiles({name:'travel-save.json',mimeType:'application/json',buffer:portable});
    await upload();await page.locator('#riseImportHeading').waitFor();
    if(process.env.BASKETBALL_SCREENSHOTS){fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'rise-import-desktop.png')});}
    assert.match(await page.locator('.rise-transfer-preview').innerText(),/Travel Comets/);
    assert.match(await page.locator('.rise-transfer-preview').innerText(),/replace Keep My Club/);
    assert.equal(await page.evaluate(()=>riseState.team.name),'Keep My Club','preview does not replace the live team');
    assert.equal(await page.evaluate(()=>localStorage.getItem(RiseToGlory.SAVE_KEY)),current.save);
    await page.locator('[data-rise=import-cancel]').click();assert.equal(await page.locator('.rise-transfer-preview').count(),0);
    assert.equal(await page.evaluate(()=>localStorage.getItem(RiseToGlory.BACKUP_KEY)),current.backup);
    await page.locator('#riseImportFile').setInputFiles({name:'broken.json',mimeType:'application/json',buffer:Buffer.from('{broken')});
    await page.waitForFunction(()=>document.querySelector('.rise-notice').textContent.includes('not valid'));
    assert.equal(await page.evaluate(()=>riseState.team.name),'Keep My Club');
    assert.equal(await page.evaluate(()=>localStorage.getItem(RiseToGlory.SAVE_KEY)),current.save);
    assert.equal(await page.evaluate(()=>localStorage.getItem(RiseToGlory.BACKUP_KEY)),current.backup);
    await page.setViewportSize({width:390,height:844});await upload();await page.locator('#riseImportHeading').waitFor();
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'phone has no horizontal overflow');
    if(process.env.BASKETBALL_SCREENSHOTS){fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'rise-import-mobile.png')});}
    await page.locator('[data-rise=import-confirm]').click();
    assert.equal(await page.evaluate(()=>riseState.team.name),'Travel Comets');
    assert.equal(await page.evaluate(()=>RiseToGlory.player(riseState,riseState.captainId).abilities.catchAndShoot),2);
    assert.equal(await page.evaluate(()=>RiseToGlory.player(riseState,riseState.captainId).role),'shooter');
    assert.equal(await page.evaluate(()=>riseState.season.completed.length),1);
    assert.equal(await page.evaluate(()=>localStorage.getItem(RiseToGlory.BACKUP_KEY)),current.save);
    await page.reload();await page.waitForFunction(()=>typeof riseState!=='undefined'&&!!riseState);
    assert.equal(await page.evaluate(()=>riseState.team.name),'Travel Comets');
    assert.equal(await page.evaluate(()=>localStorage.getItem('basketballNationalTourV2')),'tour unchanged');
    // A fresh browser can import instead of creating a club.
    await page.evaluate(()=>{localStorage.removeItem(RiseToGlory.SAVE_KEY);localStorage.removeItem(RiseToGlory.BACKUP_KEY);riseState=null;showRiseCampaign();});
    await upload();await page.locator('#riseImportHeading').waitFor();
    assert.equal(await page.locator('[data-rise=import-confirm]').innerText(),'Import campaign');
    await page.locator('[data-rise=import-confirm]').click();
    assert.equal(await page.evaluate(()=>riseState.team.name),'Travel Comets');
    assert.deepEqual(errors,[]);
    console.log('PASS: export download; import preview/cancel/replace; rejected files preserve saves; phone layout; imported progress survives reload.');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
