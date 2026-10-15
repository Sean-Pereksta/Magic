/* Full production artwork, command replacement, bounded crowds and touch HUD. */
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {pathToFileURL}=require('node:url'),{chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});
 const errors=[],url=pathToFileURL(path.resolve(__dirname,'../../apes-together-strong.html')).href;
 try{
  for(const mobile of [false,true]){
   const page=await browser.newPage({viewport:mobile?{width:390,height:844}:{width:1440,height:900},isMobile:mobile,hasTouch:mobile});
   page.on('pageerror',error=>errors.push(error.message));
   await page.goto(url);await page.waitForFunction(()=>window.ATSVisualAssets?.status==='ready');
   await page.locator('#seedInput').fill('COMMAND-UPGRADE');await page.locator('#newRun').click();
   await page.waitForFunction(()=>ATS.game?.time>.1);await page.keyboard.press('Escape');
   const result=await page.evaluate(()=>{
    const g=ATS.game,r=ATS.renderer,k=g.king;g.apes=[];g.effects=[];g.syncIndexes();
    for(let i=0;i<500;i++){const a=g.makeApe(k.x-100+(i%25)*8,k.y-60+Math.floor(i/25)*8,'follow');a.species='gorilla';}
    g.syncIndexes();g.apeGrid.rebuild(g.apes,g.king);g.siege.select(['gorilla']);
    const observed=[];
    for(const [type,expected]of [['charge','charge'],['follow','follow'],['hold','hold'],['defend','defend'],['retreat','retreat'],['attack','attack'],['move','movement']]){
     g.effects=[];const ok=g.siege.issue(['gorilla'],[{id:'qa-'+type,type,x:k.x+170,y:k.y+60}]);
     const a=g.apes[0];a.moving=['charge','follow','retreat','move'].includes(type);g.time+=.4;
     r.draw(g,0);ATSCommandHUD.update(g);
     const pose=ATSCharacterArt.resolve(a,g.time,{game:g});
     observed.push({type,expected,ok,order:ATSCommandAnimation.orderFor(a,g),pose:pose.state,markers:r.commandMarkerDraws,dust:r.commandDustDraws,confirmations:g.effects.filter(e=>e.type==='command').length,status:document.getElementById('selectedCommandState').textContent});
    }
    for(let i=0;i<30;i++)g.siege.issue(['gorilla'],[{id:'rapid-'+i,type:i%2?'hold':'charge',x:k.x+120,y:k.y+60}]);
    r.draw(g,0);const rapid={order:ATSCommandAnimation.orderFor(g.apes[0],g),markers:r.commandMarkerDraws};
    r.reducedMotion=true;r.draw(g,0);const reducedDust=r.commandDustDraws;r.reducedMotion=false;
    g.effects=[];r.draw(g,0);
    const kinds=ATSCommandVisuals.kinds,c=r.ctx;
    for(let i=0;i<kinds.length;i++){
     const x=k.x-260+i*86,y=k.y+150-i*28;r.drawCommandMarker(kinds[i],x,y,{alpha:1});
     const p=r.project(x,y);c.fillStyle='#f3dfac';c.font='12px Arial';c.textAlign='center';c.fillText(kinds[i],p.x,p.y+24);
    }
    const restored=ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize())));
    document.getElementById('pause').hidden=true;r.draw=()=>{};
    return {observed,rapid,reducedDust,cache:ATSCommandVisuals.status(),restored:restored.apes.length,restoredOrder:ATSCommandAnimation.orderFor(restored.apes[0],restored)};
   });
   for(const row of result.observed){assert.equal(row.ok,true);assert.equal(row.order,row.expected);assert.ok(row.markers<=8);assert.ok(row.dust<=24);assert.equal(row.confirmations,1,'one confirmation for 500 apes');assert.ok(row.status.includes('500 selected'));}
   assert.equal(result.rapid.order,'hold');assert.ok(result.rapid.markers<=8);assert.equal(result.reducedDust,0);assert.ok(result.cache.cachedSprites<=31);assert.equal(result.restored,500);assert.equal(result.restoredOrder,'hold');
   await page.waitForTimeout(180);
   if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'command-mobile.png':'command-gallery.png')});}
   console.log((mobile?'Touch viewport':'Desktop')+': seven command states, 500-ape effect bounds, rapid replacements, reduced motion and save/load passed');
   await page.close();
  }
  assert.deepEqual(errors,[]);
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
