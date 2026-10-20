/* Run with node game/basketball2/tests/menu-layout.cjs.
 * THREE_BUNDLE and BASKETBALL_CHROME support the same offline browser setup as
 * browser-smoke.cjs. BASKETBALL_SCREENSHOTS optionally captures each menu size.
 */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {installLegacyGraphicsFixture}=require('./legacy-graphics-fixture.cjs');
const root=path.resolve(__dirname,'../../..');
const primaryActions=['primaryQuickPlayBtn','primary1v1Btn','primary2v2Btn','primary3v3Btn','primaryLocalBtn','primaryTournamentBtn','primaryTutorialBtn','primaryControlsBtn'];
const contentTypes={'.html':'text/html','.css':'text/css','.js':'text/javascript','.webp':'image/webp','.png':'image/png','.jpg':'image/jpeg','.svg':'image/svg+xml'};

async function installRoutes(page){
  await page.route('http://basketball.local/**',route=>{
    let file=path.join(root,new URL(route.request().url()).pathname);
    if(fs.existsSync(file)&&fs.statSync(file).isDirectory()) file=path.join(file,'index.html');
    if(!fs.existsSync(file)) return route.fulfill({status:404,body:'Not found'});
    if(file.endsWith('bootstrap.js')) return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
    return route.fulfill({path:file,contentType:contentTypes[path.extname(file)]||'application/octet-stream'});
  });
  if(process.env.THREE_BUNDLE) await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
  await installLegacyGraphicsFixture(page);
}

async function openMatchSettings(page){
  if(!await page.locator('#quickPlayMatchSettings').evaluate(details=>details.open))
    await page.locator('#quickPlayMatchSettings > summary').click();
}

async function assertNoHorizontalOverflow(page,label){
  const bounds=await page.evaluate(()=>{
    const menu=document.getElementById('menuBox');
    const active=document.querySelector('.menuScreen.active');
    return {
      viewport:innerWidth,
      document:document.documentElement.scrollWidth,
      menuWidth:menu.clientWidth,
      menuContent:menu.scrollWidth,
      screenWidth:active.clientWidth,
      screenContent:active.scrollWidth
    };
  });
  assert.ok(bounds.document<=bounds.viewport+1,label+' document width '+JSON.stringify(bounds));
  assert.ok(bounds.menuContent<=bounds.menuWidth+1,label+' menu width '+JSON.stringify(bounds));
  assert.ok(bounds.screenContent<=bounds.screenWidth+1,label+' active screen width '+JSON.stringify(bounds));
}

async function assertReachable(page,id,label){
  const action=page.locator('#'+id);
  assert.ok(await action.isVisible(),label+' '+id+' visible');
  await action.scrollIntoViewIfNeeded();
  const geometry=await action.evaluate(button=>{
    const rect=button.getBoundingClientRect();
    const menu=document.getElementById('menuBox').getBoundingClientRect();
    return {left:rect.left,top:rect.top,right:rect.right,bottom:rect.bottom,width:rect.width,height:rect.height,viewportWidth:innerWidth,viewportHeight:innerHeight,menuTop:menu.top,menuBottom:menu.bottom,name:button.getAttribute('aria-label')||button.textContent.trim()};
  });
  assert.ok(geometry.name,label+' '+id+' has accessible text');
  assert.ok(geometry.width>=32&&geometry.height>=32,label+' '+id+' usable action size '+JSON.stringify(geometry));
  assert.ok(geometry.left>=-1&&geometry.right<=geometry.viewportWidth+1,label+' '+id+' horizontal bounds '+JSON.stringify(geometry));
  assert.ok(geometry.top>=Math.max(0,geometry.menuTop)-1&&geometry.bottom<=Math.min(geometry.viewportHeight,geometry.menuBottom)+1,label+' '+id+' scroll reachable '+JSON.stringify(geometry));
}

async function assertTouchTargets(page,label){
  const undersized=await page.locator('.menuScreen.active button,.menuScreen.active select').evaluateAll(actions=>actions.filter(action=>!action.hidden&&getComputedStyle(action).display!=='none'&&action.getClientRects().length).map(action=>({id:action.id,text:action.textContent.trim().slice(0,30),width:action.getBoundingClientRect().width,height:action.getBoundingClientRect().height})).filter(action=>action.width<44||action.height<44));
  assert.deepEqual(undersized,[],label+' touch actions at least 44x44');
}

async function touchSwipe(page,x,startY,endY){
  const session=await page.context().newCDPSession(page);
  const point=y=>({x,y,id:1,radiusX:1,radiusY:1,force:1});
  await session.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[point(startY)]});
  for(let step=1;step<=12;step++){
    await session.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[point(startY+(endY-startY)*step/12)]});
    await new Promise(resolve=>setTimeout(resolve,24));
  }
  await session.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});
  await session.detach();
}

(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME) options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage({viewport:{width:1440,height:900}});
    const errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await installRoutes(page);

    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>!!window.THREE&&!!renderer);
    const art=await page.locator('.menu-hero-art').evaluate(async element=>{
      const background=getComputedStyle(element).backgroundImage;
      const match=background.match(/url\(["']?([^"')]+)["']?\)/);
      if(!match) return {background,loaded:false};
      const response=await fetch(match[1]);
      const image=new Image();
      image.src=match[1];
      await image.decode();
      return {url:match[1],status:response.status,type:response.headers.get('content-type'),loaded:image.naturalWidth>0&&image.naturalHeight>0,width:image.naturalWidth,height:image.naturalHeight};
    });
    assert.ok(art.loaded,'menu artwork decodes '+JSON.stringify(art));
    assert.ok(art.url.endsWith('/assets/graphics-v2/ui/on-court.jpg'),'menu uses the rendered on-court artwork');
    assert.equal(art.type,'image/jpeg');
    console.log('PASS: authored menu artwork loads and decodes',art.width+'x'+art.height);

    for(const [label,width,height] of [['desktop',1440,900],['landscape',844,390],['portrait',390,844],['narrow',320,640]]){
      await page.setViewportSize({width,height});
      await page.evaluate(()=>showPrimaryMenu());
      await assertNoHorizontalOverflow(page,label+' primary');
      for(const id of primaryActions) await assertReachable(page,id,label);
      const focusable=await page.locator('#primaryMenuScreen button:not(:disabled)').evaluateAll(buttons=>buttons.map(button=>button.id));
      const focused=new Set();
      await page.locator('#primaryQuickPlayBtn').focus();
      for(let step=0;step<focusable.length+2;step++){
        await page.keyboard.press('Tab');
        const focus=await page.evaluate(()=>({id:document.activeElement.id,visible:document.activeElement.matches(':focus-visible'),outline:getComputedStyle(document.activeElement).outlineStyle}));
        if(focusable.includes(focus.id)){
          focused.add(focus.id);
          assert.ok(focus.visible&&focus.outline!=='none',label+' '+focus.id+' visible keyboard focus');
        }
      }
      for(const id of primaryActions) assert.ok(focused.has(id),label+' '+id+' reachable by Tab');
      await page.locator('#primary3v3Btn').focus();
      await page.keyboard.press('Enter');
      assert.ok(await page.locator('#quickPlayScreen').isVisible(),label+' keyboard activates team mode');
      await assertNoHorizontalOverflow(page,label+' setup');
      await openMatchSettings(page);
      for(const id of ['blueOffenseSelect','coralOffenseSelect','startBtn','quickPlayBackBtn']) await assertReachable(page,id,label+' setup');
      await page.locator('#quickPlayBackBtn').click();
      if(process.env.BASKETBALL_SCREENSHOTS&&(label==='desktop'||label==='portrait')){
        await page.locator('.menu-header').scrollIntoViewIfNeeded();
        await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'basketball2-menu-'+(label==='portrait'?'mobile':'desktop')+'.png')});
      }
      await page.locator('#primaryControlsBtn').click();
      await assertNoHorizontalOverflow(page,label+' controls');
      for(const id of ['controlStyleSelect','gameplayPresetSelect','controlsBackBtn']) await assertReachable(page,id,label+' controls');
      await page.locator('#controlsBackBtn').click();
      await page.locator('#primaryTournamentBtn').click();
      await assertNoHorizontalOverflow(page,label+' Tour setup');
      for(const id of ['startTournamentBtn','tournamentSetupBackBtn']) await assertReachable(page,id,label+' Tour setup');
      await page.locator('#tournamentSetupBackBtn').click();
      console.log('PASS: '+label+' '+width+'x'+height+' menus, settings, Tour setup and keyboard navigation');
    }

    await page.setViewportSize({width:1440,height:900});
    await page.evaluate(()=>showPrimaryMenu());
    await page.locator('#primaryControlsBtn').click();
    await page.locator('#controlStyleSelect').selectOption('classic');
    await page.locator('#mobileLayoutSetting').selectOption('mirrored');
    await page.locator('#shotMeterSetting').uncheck();
    const settings=await page.evaluate(()=>localStorage.getItem(SETTINGS_KEY));
    await page.locator('#controlsBackBtn').click();
    await page.locator('#primary3v3Btn').click();
    const styles=await page.evaluate(()=>Object.keys(OFFENSIVE_STYLES));
    for(let index=0;index<styles.length;index++){
      await openMatchSettings(page);
      const selected=[styles[index],styles[(index+1)%styles.length]];
      await page.locator('#blueOffenseSelect').selectOption(selected[0]);
      await page.locator('#coralOffenseSelect').selectOption(selected[1]);
      assert.deepEqual(await page.evaluate(()=>JSON.parse(localStorage.getItem('basketball2MatchV1')).offensiveStyles),selected,'both team styles saved');
      await page.reload();
      await page.waitForFunction(()=>!!window.THREE&&!!renderer);
      await page.locator('#primaryQuickPlayBtn').click();
      assert.equal(await page.locator('#blueOffenseSelect').inputValue(),selected[0]);
      assert.equal(await page.locator('#coralOffenseSelect').inputValue(),selected[1]);
    }
    await page.locator('#quickPlayBackBtn').click();
    await page.locator('#primary1v1Btn').click();
    assert.ok(await page.locator('#teamTacticsCard').isHidden(),'team tactics hidden for 1v1');
    await page.locator('#quickPlayBackBtn').click();
    await page.locator('#primary3v3Btn').click();
    const quick=await page.evaluate(()=>localStorage.getItem('basketball2MatchV1'));
    const tour=await page.evaluate(()=>{
      startNewTournament();startTournamentRound();
      const oneOnOne=teamSize===1&&!twoPlayerMode&&matchPlayers.length===2;
      scoreP1=11;scoreP2=4;endGame('p1');
      const saved=JSON.parse(localStorage.getItem(TOURNAMENT_SAVE_KEY));
      tournamentState=null;loadTournament();
      const restored=tournamentState.currentStopIndex===saved.currentStopIndex&&tournamentState.playerWins===1&&tournamentState.completedGames.length===1;
      showPrimaryMenu();showQuickPlaySetup();
      return {oneOnOne,restored,quick:localStorage.getItem('basketball2MatchV1'),settings:localStorage.getItem(SETTINGS_KEY),styles:[document.getElementById('blueOffenseSelect').value,document.getElementById('coralOffenseSelect').value]};
    });
    assert.ok(tour.oneOnOne);assert.ok(tour.restored);
    assert.equal(tour.quick,quick,'Tour preserves quick-play styles and matchup');
    assert.equal(tour.settings,settings,'menu and Tour preserve control settings');
    assert.deepEqual(tour.styles,JSON.parse(quick).offensiveStyles,'team style choices restored after Tour');
    assert.deepEqual(errors,[],'no browser exceptions');
    console.log('PASS: all '+styles.length+' offense selections persist; 1v1, National Tour and controls remain independent');

    const touchPage=await browser.newPage({viewport:{width:390,height:844},hasTouch:true,isMobile:true});
    touchPage.on('pageerror',error=>errors.push(error.stack));
    await installRoutes(touchPage);
    await touchPage.goto('http://basketball.local/game/basketball2/');
    await touchPage.waitForFunction(()=>!!window.THREE&&!!renderer);
    assert.ok(await touchPage.evaluate(()=>matchMedia('(pointer: coarse)').matches),'real coarse pointer emulation');
    await assertNoHorizontalOverflow(touchPage,'coarse primary');
    await assertTouchTargets(touchPage,'coarse primary');
    await touchPage.locator('#primary3v3Btn').tap();
    await assertNoHorizontalOverflow(touchPage,'coarse setup');
    await assertTouchTargets(touchPage,'coarse setup');
    const initialScroll=await touchPage.locator('#menuBox').evaluate(menu=>menu.scrollTop);
    // Dispatch a browser-native touch gesture, rather than setting scrollTop or
    // using a mouse wheel, so ancestor touch-action regressions are exercised.
    await touchSwipe(touchPage,350,650,250);
    await touchPage.waitForFunction(before=>document.getElementById('menuBox').scrollTop>before+100,initialScroll,{timeout:3000});
    const afterSwipe=await touchPage.locator('#menuBox').evaluate(menu=>menu.scrollTop);
    await assertNoHorizontalOverflow(touchPage,'coarse setup after swipe');
    await touchPage.locator('#quickPlayBackBtn').tap();
    if(process.env.BASKETBALL_SCREENSHOTS){
      await touchPage.locator('.menu-header').scrollIntoViewIfNeeded();
      await touchPage.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'basketball2-menu-mobile.png')});
    }
    await touchPage.locator('#primaryControlsBtn').tap();
    await assertNoHorizontalOverflow(touchPage,'coarse controls');
    await assertTouchTargets(touchPage,'coarse controls');
    await touchPage.locator('#controlsBackBtn').tap();
    await touchPage.locator('#primaryTournamentBtn').tap();
    await assertNoHorizontalOverflow(touchPage,'coarse Tour setup');
    await assertTouchTargets(touchPage,'coarse Tour setup');
    assert.deepEqual(errors,[],'no coarse touch browser exceptions');
    console.log('PASS: coarse 390x844 touch targets, native touch scrolling ('+Math.round(afterSwipe-initialScroll)+'px), menus and settings');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
