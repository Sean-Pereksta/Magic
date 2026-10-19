/* Real browser campaign acceptance: NODE_PATH, THREE_BUNDLE and BASKETBALL_CHROME
 * can point at existing local runtimes. No production test hook is required. */
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
    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>typeof RiseToGlory!=='undefined'&&typeof buildRiseUI==='function'&&basketballPlayerModels.assets.size===3);
    assert.deepEqual(errors,[],'startup');
    await page.evaluate(()=>localStorage.setItem('basketballNationalTourV2','{"version":2,"xp":777,"unlocked":["nationalChampion"]}'));
    await page.locator('#primaryRiseBtn').click();
    await page.locator('#riseCreationForm [name=name]').fill('Northside Test Club');
    await page.locator('#riseCreationForm [name=captainName]').fill('Captain Nova');
    await page.locator('#riseCreationForm [name=jerseyStyle]').selectOption('chevron');
    await page.locator('#riseCreationForm button[type=submit]').click();
    assert.equal(await page.evaluate(()=>riseState.team.name),'Northside Test Club');
    assert.equal(await page.evaluate(()=>RiseToGlory.roster(riseState).length),3);
    await page.evaluate(()=>showRiseCampaign('develop'));
    const upgrade=page.locator('[data-rise=upgrade]:enabled').first();
    const id=await upgrade.getAttribute('data-id'),key=await upgrade.getAttribute('data-key');
    const before=await page.evaluate(({id,key})=>RiseToGlory.player(riseState,id).ratings[key],{id,key});
    await upgrade.click();
    assert.ok(await page.evaluate(({id,key,before})=>RiseToGlory.player(riseState,id).ratings[key]>before,{id,key,before}));
    await page.evaluate(()=>showRiseCampaign('market'));
    const sign=page.locator('[data-rise=sign]:enabled').first(),signedId=await sign.getAttribute('data-id');
    await sign.click();
    assert.ok(await page.evaluate(id=>riseState.roster.includes(id),signedId));
    await page.evaluate(id=>{
      const ids=[...riseState.starters];ids[2]=id;RiseToGlory.setStarters(riseState,ids);
      RiseToGlory.save(riseState);showRiseCampaign('hub');
    },signedId);
    if(process.env.BASKETBALL_SCREENSHOTS){
      fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});
      await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'rise-desktop.png')});
    }
    await page.evaluate(()=>{gameSettings.set('gameplayPreset','arcade');startRiseMatch();});
    assert.equal(await page.evaluate(()=>gameSettings.get('gameplayPreset')),'simulation','campaign uses the calibrated shared simulation preset');
    const actual=await page.evaluate(()=>({ids:matchPlayers.filter(p=>p.teamId===0).map(p=>p.campaignPlayerId),
      ratings:matchPlayers.filter(p=>p.teamId===0).map(p=>p.profile.ratings),models:matchPlayers.every(p=>!!p.playerModel),size:teamSize}));
    assert.ok(actual.ids.includes(signedId),'signed athlete actually enters court');assert.equal(actual.size,3);assert.ok(actual.models);
    assert.ok(actual.ratings[actual.ids.indexOf(id)][key]>before,'the purchased attribute upgrade reaches the actual match profile');
    const sub=await page.evaluate(()=>{
      const slot=matchPlayers.find(p=>p.teamId===0),bench=RiseToGlory.roster(riseState).find(p=>!matchPlayers.some(c=>c.campaignPlayerId===p.id));
      slot.stats.assists=2;slot.stats.blocks=2;
      const old=slot.campaignPlayerId;gamePaused=true;possessionFrozen=true;
      const changed=riseSubstitute(slot.id,bench.id);gamePaused=false;
      return {changed,old,replaced:getPlayerById(slot.id).campaignPlayerId,bench:bench.id};
    });
    assert.ok(sub.changed);assert.equal(sub.replaced,sub.bench);
    await page.evaluate(()=>{countdownTimer=0;possessionFrozen=false;for(let i=0;i<600&&!gameOver;i++)simulateBasketball(1/60);});
    assert.ok(await page.evaluate(()=>matchPlayers.every(p=>p.group.position.toArray().every(Number.isFinite))));
    const currency=await page.evaluate(()=>riseState.currency);
    await page.evaluate(()=>{scoreP1=targetScore;scoreP2=3;endGame('p1');});
    assert.ok(await page.evaluate(before=>riseState.currency>before,currency));
    assert.ok(await page.evaluate(id=>RiseToGlory.player(riseState,id).stats.assists>=2,sub.old),'subbed athlete gets contribution XP/stats');
    assert.equal(await page.evaluate(()=>riseState.season.completed.length),1);
    assert.equal(await page.evaluate(()=>gameSettings.get('gameplayPreset')),'arcade','returning from campaign restores the exhibition preset');
    await page.reload();
    await page.waitForFunction(()=>typeof buildRiseUI==='function'&&!!riseState);
    assert.equal(await page.evaluate(()=>riseState.season.completed.length),1);
    assert.equal(await page.evaluate(()=>localStorage.getItem('basketballNationalTourV2')),'{"version":2,"xp":777,"unlocked":["nationalChampion"]}');
    // Training is played through the same fixed-step movement/collision code.
    const drill=await page.evaluate(()=>{
      startRiseTraining('dribbling',riseState.captainId);
      const player=matchPlayers.find(p=>p.campaignPlayerId===riseState.captainId),before=RiseToGlory.player(riseState,player.campaignPlayerId).xp,level=RiseToGlory.player(riseState,player.campaignPlayerId).level;
      const cones=[[-4,-3],[4,-3],[-3,1],[3,1],[0,-5],[-4,-3]];
      for(const [x,z]of cones){player.group.position.set(x,0,z);updateRiseRuntime(1/60);}
      return {finished:riseTraining===null,before,level,after:RiseToGlory.player(riseState,riseState.captainId).xp,afterLevel:RiseToGlory.player(riseState,riseState.captainId).level};
    });
    assert.ok(drill.finished);assert.ok(drill.after>drill.before||drill.afterLevel>drill.level);
    await page.evaluate(()=>{startRiseTraining('passing',riseState.captainId);});
    assert.equal(await page.evaluate(()=>matchPlayers.length),6,'passing training has both complete teams after a rivalry fixture');
    await page.evaluate(()=>finishRiseTraining(false));
    for(const viewport of [{width:1440,height:950},{width:390,height:844},{width:844,height:390}]){
      await page.setViewportSize(viewport);
      for(const tab of ['hub','roster','develop','market','schedule','strategy','training','stats']){
        await page.evaluate(tab=>showRiseCampaign(tab),tab);
        const dimensions=await page.evaluate(()=>{
          const active=document.querySelector('.menuScreen.active'),menu=document.getElementById('menuBox');
          return {document:document.documentElement.scrollWidth,viewport:innerWidth,screen:active.scrollWidth,width:active.clientWidth,menu:menu.scrollWidth,menuWidth:menu.clientWidth};
        });
        assert.ok(dimensions.document<=dimensions.viewport+1,tab+' document overflow '+JSON.stringify(dimensions));
        assert.ok(dimensions.screen<=dimensions.width+1,tab+' pane overflow '+JSON.stringify(dimensions));
        assert.ok(dimensions.menu<=dimensions.menuWidth+1,tab+' menu overflow '+JSON.stringify(dimensions));
      }
      if(viewport.width===390&&process.env.BASKETBALL_SCREENSHOTS){
        await page.evaluate(()=>showRiseCampaign('hub'));
        await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'rise-mobile.png')});
      }
    }
    const modes=await page.evaluate(()=>{
      const result=[];showPrimaryMenu();
      for(const size of [1,2,3])for(const local of [false,true]){
        showQuickPlaySetup(size,local);startGame();
        for(let i=0;i<60;i++)simulateBasketball(1/60);
        result.push({size,count:matchPlayers.length,upgraded:matchPlayers.some(p=>!!p.campaignPlayerId),local,humans:matchPlayers.filter(p=>p.controllerSlot).length});
      }return result;
    });
    for(const mode of modes){assert.equal(mode.count,mode.size*2);assert.equal(mode.upgraded,false);assert.equal(mode.humans,mode.local?2:1);}
    assert.deepEqual(errors,[],'no runtime/page errors throughout campaign and existing modes');
    console.log('PASS: UI franchise creation, real attribute upgrade/signing/lineup, GLB court roster, substitution box scores, result rewards, reload/Tour isolation, live drills, all eight panes at desktop/mobile/landscape, six Quick Play configurations.');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
