/* Real custom roster integration: optional THREE_BUNDLE, BASKETBALL_CHROME and
 * BASKETBALL_SCREENSHOTS use the same offline setup as the other browser checks. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {installLegacyGraphicsFixture}=require('./legacy-graphics-fixture.cjs');
const root=path.resolve(__dirname,'../../..');

(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME) options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage({viewport:{width:1440,height:1000}}),errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory()) file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.glb':'model/gltf-binary','.webp':'image/webp'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
    await installLegacyGraphicsFixture(page);

    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>typeof BasketballCustomRosters!=='undefined'&&!!renderer);
    await page.evaluate(()=>{
      riseState=RiseToGlory.create({name:'Budget Ball Club',captainName:'Catch Shooter'});
      const shooter=RiseToGlory.player(riseState,riseState.captainId);
      shooter.role='shooter';shooter.abilities={catchAndShoot:2,quickRelease:3};shooter.ratings.threePoint=85;
      RiseToGlory.save(riseState);
      showQuickPlaySetup(3,false);
    });
    assert.equal(await page.locator('#customRosterEnabled').isChecked(),true,'Quick Play opens the team builder');
    assert.equal(await page.locator('#customRosterEnabled').isVisible(),false,'the compatibility toggle is not a second setup choice');
    assert.equal(await page.locator('#customRosterCap').inputValue(),'250');
    assert.equal(await page.locator('#customRosterTeams select').count(),6);
    assert.equal(await page.locator('#p1Cards').isVisible(),false);
    const captainId=await page.evaluate(()=>riseState.captainId);
    // Set each slot directly, then verify the actual fielded identities below.
    const expected={blue:[`rise:${captainId}`,'classic:smooth','classic:bruiser'],coral:['classic:claw','classic:crafty','classic:deeprange']};
    await page.evaluate(expected=>{
      customRosterDraft.teams=[expected.blue,expected.coral];saveCustomRosterDraft();refreshCustomRosterSetup();
    },expected);
    const totals=await page.evaluate(()=>BasketballCustomRosters.validate(customRosterDraft,teamSize,customRosterCatalog()).totals);
    const exactCap=Math.max(...totals);
    await page.locator('#customRosterCap').fill(String(exactCap));
    assert.equal(await page.locator('#startBtn').isEnabled(),true,'a team exactly at the cap can start');
    await page.locator('#customRosterCap').fill(String(exactCap-1));
    assert.equal(await page.locator('#startBtn').isDisabled(),true,'an over-cap team blocks start');
    assert.match(await page.locator('#customRosterStatus').textContent(),/over the team cap/);
    assert.equal(await page.evaluate(()=>{const before=matchPlayers;const result=startGame();return result===false&&matchPlayers===before&&!gameStarted;}),true,'direct start cannot bypass validation or dispose the existing roster');
    await page.locator('#customRosterCap').fill('250');
    if(process.env.BASKETBALL_SCREENSHOTS){fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});await page.locator('.custom-roster-card').scrollIntoViewIfNeeded();await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'custom-rosters-desktop.png')});}
    for(const [width,height]of [[390,844],[320,640],[844,390]]){
      await page.setViewportSize({width,height});
      const sizes=await page.evaluate(()=>({viewport:innerWidth,doc:document.documentElement.scrollWidth,menu:document.getElementById('menuBox').clientWidth,content:document.getElementById('menuBox').scrollWidth}));
      assert.ok(sizes.doc<=sizes.viewport+1&&sizes.content<=sizes.menu+1,'responsive custom setup '+JSON.stringify(sizes));
      for(const selector of ['#customRosterCap','[data-roster-browse][data-team="0"][data-slot="0"]','[data-roster-browse][data-team="1"][data-slot="2"]','#startBtn']){
        await page.locator(selector).scrollIntoViewIfNeeded();
        const r=await page.locator(selector).boundingBox();assert.ok(r&&r.x>=0&&r.x+r.width<=width+1&&r.height>=44,'usable mobile control '+selector);
      }
      if(width===390&&process.env.BASKETBALL_SCREENSHOTS){await page.locator('[data-roster-browse][data-team="0"][data-slot="0"]').scrollIntoViewIfNeeded();await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'custom-rosters-mobile.png')});}
    }
    console.log('PASS: both team totals, exact cap, blocked invalid start and responsive roster controls');
    const campaignBefore=await page.evaluate(()=>JSON.stringify(riseState));
    for(const [size,local,mode] of [[1,false,'competitive'],[2,false,'competitive'],[3,false,'competitive'],[2,true,'competitive'],[2,true,'cooperative'],[3,true,'cooperative']]){
      await page.evaluate(({size,local,mode,expected})=>{
        showQuickPlaySetup(size,local);localTeamMode=mode;
        customRosterDraft.teams=[expected.blue,expected.coral];customRosterDraft.cap=250;customRosterDraft.enabled=true;
        refreshMatchSetup();saveMatchSetup();saveCustomRosterDraft();startGame();
      },{size,local,mode,expected});
      const actual=await page.evaluate(()=>({started:gameStarted,teams:[0,1].map(team=>matchPlayers.filter(p=>p.teamId===team).map(p=>p.profile.customRosterId)),
        totals:[0,1].map(team=>matchPlayers.filter(p=>p.teamId===team).reduce((sum,p)=>sum+p.profile.customOverall,0)),
        humans:matchPlayers.filter(p=>p.controllerSlot).map(p=>({slot:p.controllerSlot,team:p.teamId,id:p.profile.customRosterId})),
        shooter:matchPlayers.find(p=>p.profile.customRosterId.startsWith('rise:')).profile,
        riseMatch,models:matchPlayers.every(p=>!!p.playerModel)}));
      assert.equal(actual.started,true);assert.equal(actual.riseMatch,null);assert.ok(actual.models);
      assert.deepEqual(actual.teams.map(ids=>ids.sort()),[expected.blue.slice(0,size).sort(),expected.coral.slice(0,size).sort()]);
      assert.ok(actual.totals.every(total=>total<=250));
      assert.equal(actual.shooter.role,'shooter');assert.equal(actual.shooter.ratings.threePoint,85);assert.equal(actual.shooter.abilities.quickRelease,3);
      assert.equal(actual.humans.length,local?2:1);
      if(local) assert.deepEqual(actual.humans.find(p=>p.slot==='p2'),{slot:'p2',team:mode==='cooperative'?0:1,id:mode==='cooperative'?expected.blue[1]:expected.coral[0]});
      await page.evaluate(()=>{countdownTimer=0;possessionFrozen=false;for(let frame=0;frame<120;frame++)simulateBasketball(1/60);showPrimaryMenu();});
    }
    assert.equal(await page.evaluate(()=>JSON.stringify(riseState)),campaignBefore,'custom games award no campaign progress and never mutate saved athletes');
    console.log('PASS: selected real athletes, upgrades and role fielded in solo/local/co-op 1v1, 2v2 and 3v3');
    const stored=await page.evaluate(()=>localStorage.getItem(BasketballCustomRosters.SAVE_KEY));
    await page.evaluate(()=>{startRiseMatch();showPrimaryMenu();showQuickPlaySetup();});
    assert.equal(await page.evaluate(()=>localStorage.getItem(BasketballCustomRosters.SAVE_KEY)),stored,'campaign preserves custom draft');
    assert.equal(await page.locator('#customRosterTeam0Slot0').inputValue(),`rise:${captainId}`);
    await page.reload();await page.waitForFunction(()=>!!renderer);
    await page.evaluate(()=>showQuickPlaySetup());
    assert.equal(await page.locator('#customRosterEnabled').isChecked(),true);assert.equal(await page.locator('#customRosterTeam0Slot0').inputValue(),`rise:${captainId}`);
    const revalidated=await page.evaluate(()=>{
      for(const id of riseState.roster) for(const key of RiseToGlory.ATTRIBUTE_KEYS) riseState.players[id].ratings[key]=95;
      customRosterDraft.cap=150;refreshCustomRosterSetup();return document.getElementById('startBtn').disabled;
    });
    assert.equal(revalidated,true,'updated franchise ratings revalidate a stored selection against live OVR');
    const imported=await page.evaluate(()=>{
      const incoming=RiseToGlory.create({seed:77,name:'Imported Club',captainName:'Imported Ace'}),athlete=RiseToGlory.player(incoming,incoming.captainId);
      athlete.role='shooter';athlete.abilities={catchAndShoot:2,quickRelease:1};athlete.ratings.threePoint=athlete.caps.threePoint;
      const exported=RiseToGlory.exportSave(incoming);if(!exported.ok)throw Error(exported.message);
      const result=RiseToGlory.importSave(exported.serialized);if(!result.ok)throw Error(result.message);
      riseState=result.state;showQuickPlaySetup(3,false);
      const staleBlocked=document.getElementById('startBtn').disabled,pool=customRosterCatalog();
      customRosterDraft.teams[0][0]=`rise:${riseState.captainId}`;customRosterDraft.cap=250;
      saveCustomRosterDraft();refreshCustomRosterSetup();startGame();
      const profile=matchPlayers.find(p=>p.profile.customRosterId===`rise:${riseState.captainId}`).profile;
      return {staleBlocked,id:riseState.captainId,name:profile.name,role:profile.role,abilities:profile.abilities,
        ratings:profile.ratings,expectedRatings:RiseToGlory.player(riseState,riseState.captainId).ratings,
        overall:profile.customOverall,expectedOverall:RiseToGlory.overall(RiseToGlory.player(riseState,riseState.captainId)),
        currentPool:pool.filter(p=>p.id.startsWith('rise:')).map(p=>p.id)};
    });
    assert.equal(imported.staleBlocked,true,'a replaced franchise invalidates unavailable saved roster IDs');
    assert.ok(imported.currentPool.every(id=>id.startsWith('rise:rtg-77-')),'the custom pool uses the imported franchise');
    assert.equal(imported.name,'Imported Ace');assert.equal(imported.role,'shooter');
    assert.equal(imported.abilities.catchAndShoot,2);assert.equal(imported.abilities.quickRelease,1);
    assert.deepEqual(imported.ratings,imported.expectedRatings);assert.equal(imported.overall,imported.expectedOverall);
    await page.evaluate(()=>{showPrimaryMenu();startNewTournament();continueTournamentFlow();});
    assert.equal(await page.evaluate(()=>isTournamentGame&&customMatchRoster===null&&matchPlayers.every(p=>!p.profile.customRosterId)),true,'Tour ignores a saved custom roster');
    await page.evaluate(()=>showQuickPlaySetup());
    await page.evaluate(()=>{customRosterDraft.enabled=false;saveCustomRosterDraft();refreshCustomRosterSetup();startGame();});
    assert.equal(await page.evaluate(()=>customMatchRoster===null&&matchPlayers.every(p=>!p.profile.customRosterId)),true,'standard play stays available after a rejected custom draft');
    assert.deepEqual(errors,[]);
    console.log('PASS: campaign isolation, persistent draft, imported identities/skills/live OVR, Tour and standard Quick Play compatibility');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
