/* Roster cuts through actual campaign controls, confirmation, storage and reload. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {installLegacyGraphicsFixture}=require('./legacy-graphics-fixture.cjs');
const root=path.resolve(__dirname,'../../..');
(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage({viewport:{width:1440,height:950}}),errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      return route.fulfill({path:file,contentType:file.endsWith('.html')?'text/html':file.endsWith('.css')?'text/css':file.endsWith('.glb')?'model/gltf-binary':file.endsWith('.webp')?'image/webp':'text/javascript'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
    await installLegacyGraphicsFixture(page);

    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>typeof buildRiseUI==='function'&&basketballPlayerModels.assets.size===3);
    await page.locator('#primaryRiseBtn').click();
    await page.locator('#riseCreationForm button[type=submit]').click();
    await page.evaluate(()=>{riseState.currency=5000;RiseToGlory.save(riseState);showRiseCampaign('roster');});
    const captainId=await page.evaluate(()=>riseState.captainId);
    const cut=id=>page.locator(`[data-rise=release][data-id="${id}"]`);
    assert.equal(await cut(captainId).isDisabled(),true);
    assert.match(await cut(captainId).locator('..').innerText(),/captain stays/);
    const initialNonCaptain=await page.evaluate(()=>riseState.roster.find(id=>id!==riseState.captainId));
    assert.equal(await cut(initialNonCaptain).isDisabled(),true);
    assert.match(await cut(initialNonCaptain).locator('..').innerText(),/at least three/);
    await page.evaluate(()=>showRiseCampaign('market'));
    const recruits=[];
    for(let i=0;i<2;i++){
      const sign=page.locator('[data-rise=sign]:enabled').first();recruits.push(await sign.getAttribute('data-id'));await sign.click();
    }
    await page.evaluate(()=>showRiseCampaign('roster'));
    const [starterId,benchId]=recruits;
    const replacementId=await page.locator('[data-rise-change=lineup][data-slot="2"]').inputValue();
    await page.locator('[data-rise-change=lineup][data-slot="2"]').selectOption(starterId);
    await page.evaluate(id=>{riseState.team.matchups[id]='rival-center';RiseToGlory.save(riseState);},starterId);
    const before=await page.evaluate(id=>({credits:riseState.currency,preview:RiseToGlory.releasePreview(riseState,id),name:riseState.players[id].name,save:localStorage.getItem(RiseToGlory.SAVE_KEY)}),starterId);
    await cut(starterId).click();
    assert.match(await page.locator('#riseCutHeading').innerText(),new RegExp(before.name.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')));
    assert.equal(await page.locator('#riseCutHeading').evaluate(element=>document.activeElement===element),true);
    const confirmation=await page.locator('.rise-cut-confirm').innerText();
    assert.ok(confirmation.includes(before.preview.refund.toLocaleString()+' C'));assert.match(confirmation,/30%/);
    assert.ok(confirmation.includes(await page.evaluate(id=>riseState.players[id].name,replacementId)));
    assert.equal(await page.evaluate(()=>localStorage.getItem(RiseToGlory.SAVE_KEY)),before.save,'opening confirmation cannot mutate the save');
    await page.locator('[data-rise=cancel-release]').click();
    assert.equal(await page.locator('.rise-cut-confirm').count(),0);
    assert.equal(await page.evaluate(()=>localStorage.getItem(RiseToGlory.SAVE_KEY)),before.save,'keeping a player cannot mutate the save');
    assert.equal(await cut(starterId).evaluate(element=>document.activeElement===element),true,'cancel restores the roster control');
    await cut(starterId).click();await page.locator('[data-rise=confirm-release]').click();
    const after=await page.evaluate(id=>({roster:riseState.roster,starters:riseState.starters,captainId:riseState.captainId,credits:riseState.currency,matchup:riseState.team.matchups[id],fielded:RiseToGlory.prepareMatch(riseState).home.map(player=>player.id)}),starterId);
    assert.equal(after.roster.includes(starterId),false);assert.equal(after.starters.includes(starterId),false);
    assert.equal(after.starters.includes(replacementId),true);assert.equal(after.starters.length,3);
    assert.equal(after.credits,before.credits+before.preview.refund);assert.equal(after.matchup,undefined);
    assert.equal(after.captainId,captainId);assert.equal(after.fielded.includes(starterId),false);
    assert.equal(await cut(starterId).count(),0);assert.match(await page.locator('.rise-notice').innerText(),/cut from the roster/);
    await page.reload();await page.waitForFunction(()=>!!riseState&&typeof showRiseCampaign==='function');
    assert.equal(await page.evaluate(id=>riseState.roster.includes(id),starterId),false,'confirmed cut survives a reload');
    assert.equal(await page.evaluate(()=>riseState.currency),after.credits);
    assert.deepEqual(await page.evaluate(()=>riseState.starters),after.starters);
    await page.evaluate(()=>showRiseCampaign('roster'));
    await page.locator(`[data-rise=develop][data-id="${benchId}"]`).click();
    assert.equal(await cut(benchId).isEnabled(),true,'player detail also offers the roster cut');
    await cut(benchId).click();await page.locator('[data-rise=confirm-release]').click();
    assert.equal(await page.evaluate(()=>riseState.roster.length),3);
    assert.equal(await page.locator('[data-rise-change=selected] option').count(),3,'selected player safely falls back after cutting the detail athlete');
    await page.evaluate(()=>showRiseCampaign('roster'));await page.setViewportSize({width:390,height:844});
    assert.equal(await page.locator('[data-rise=release]:enabled').count(),0,'remaining minimum squad is protected');
    assert.match(await cut(initialNonCaptain).locator('..').innerText(),/at least three/);
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'cut controls fit the mobile roster');
    assert.deepEqual(errors,[]);
    console.log('PASS: roster/detail cut controls, named refund/replacement confirmation, cancel, captain/minimum protection, lineup/matchup cleanup and reload persistence');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
