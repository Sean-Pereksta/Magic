/* Playable offline bundle: tribe choices, existing artwork, advanced commands,
 * selected-unit tap behavior, persistence and bounded modular overlay caches. */
'use strict';
const assert=require('node:assert/strict'),path=require('node:path'),fs=require('node:fs');
const {pathToFileURL}=require('node:url'),{chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,args:['--no-sandbox','--disable-dev-shm-usage'],...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});
 const errors=[],network=[];
 try{
  const context=await browser.newContext({viewport:{width:1440,height:1000}}),page=await context.newPage();
  page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>{if(/^https?:/.test(r.url()))network.push(r.url())});
  for(const id of ['ironback','shadowhand','emberwood']){
   await page.goto(pathToFileURL(path.resolve(__dirname,'../../apes-together-strong.html')).href);
   await page.waitForFunction(()=>window.ATSVisualAssets?.status==='ready');
   assert.equal(await page.locator('#tribeChoices input').count(),3);
   await page.locator('input[name=startingTribe][value='+id+']').check();
   // Verify the large painted King, not only the radio button or small tribe card,
   // switches to the matching tribe-specific outfit without starting a run.
   const portrait=await page.evaluate(()=>{
    const renderer=ATS.renderer,draw=renderer.drawApe;let sideKing=null;
    try{
     renderer.drawApe=function(ctx,ape,king,...args){
      if(king)sideKing={tribeId:ape.tribeId,atlas:ATSOutfitArt.atlasFor(ape,{king:true,tribeId:ape.tribeId})};
      return draw.call(this,ctx,ape,king,...args);
     };
     renderer.drawMenu(0);
    }finally{renderer.drawApe=draw;}
    return{selected:renderer.menuTribeId,sideKing};
   });
   assert.equal(portrait.selected,id);
   assert.equal(portrait.sideKing?.tribeId,id);
   assert.equal(portrait.sideKing?.atlas,'armor-'+id+'-base');
   if(process.env.QA_ARTIFACT_DIR&&id==='ironback'){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,'tribal-menu.png'),fullPage:true})}
   await page.locator('#newRun').click();await page.waitForFunction(()=>ATS.game?.time>.1);
   assert.equal(await page.evaluate(()=>ATS.game.tribe.id),id);
   await page.evaluate(()=>{
    const g=ATS.game;g.spawnSites=()=>{};g.humans=[];g.vehicles=[];g.helis=[];g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;g.king.hp=g.king.maxHp=10000;
    for(const species of ATSSiegeSpecies){const a=g.makeApe(g.king.x+20,g.king.y+20,'follow');a.species=species;g.siege.balance(a,true);g.champions.promote(a,{prisonChampion:true,sourceId:'test-'+species})}
    g.syncIndexes();g.siege.selectClimbers();ATS.army.update(true);
   });
   await page.locator('[data-tribal=infiltrate]').click();
   assert.ok(await page.evaluate(()=>ATS.game.siege.selectedMembers().every(a=>!!a.infiltration)));
   await page.getByLabel('When infiltrators are discovered').selectOption('retreat');
   assert.ok(await page.evaluate(()=>ATS.game.siege.selectedMembers().every(a=>a.infiltration.response==='retreat')));
   if(id==='shadowhand'){await page.locator('[data-tribal=ghost]').click();assert.ok(await page.evaluate(()=>ATS.game.siege.selectedMembers().every(a=>a.infiltration?.ghost)))}
   await page.locator('[data-tribal=distract]').click();assert.equal(await page.evaluate(()=>ATS.army.mode),'distract');
   await page.mouse.move(800,480);await page.waitForTimeout(50);assert.ok(await page.evaluate(()=>!!ATS.renderer.distractionPreview));
   await page.keyboard.press('Escape');
   const saved=await page.evaluate(()=>{const data=JSON.parse(JSON.stringify(ATS.game.serialize())),copy=ATSGame.fromJSON(data);return{id:copy.tribe.id,champions:copy.apes.filter(a=>a.champion?.prisonChampion).length,cache:ATS.renderer.tribalOverlays?.size||0}});
   assert.equal(saved.id,id);assert.equal(saved.champions,6);assert.ok(saved.cache<=128);
  }
  assert.deepEqual(network,[],'standalone game is entirely offline');assert.deepEqual(errors,[]);
  console.log('Tribal browser checks passed: all three openings, embedded artwork, infiltration/response UI, Ghost Entry, noise preview, champion saves, zero external requests.');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
