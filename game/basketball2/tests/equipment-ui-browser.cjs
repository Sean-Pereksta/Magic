/* Real menu/profile storage, isolated GLB appearances and unchanged input flow. */
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
    const page=await browser.newPage({viewport:{width:1280,height:800}}),errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory()) file=path.join(file,'index.html');
      if(!fs.existsSync(file)) return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js')) return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.webp':'image/webp','.glb':'model/gltf-binary','.json':'application/json'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE) await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
    await installLegacyGraphicsFixture(page);

    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    await page.locator('#primary3v3Btn').click();
    await page.locator('#p1Equipment summary').click();
    await page.locator('#p1Equipment [data-region="jersey"]').check();
    await page.locator('#p1Equipment [data-color="jersey"]').fill('#2255ee');
    await page.locator('#p1Equipment [data-region="shorts"]').check();
    await page.locator('#p1Equipment [data-color="shorts"]').fill('#11aa66');
    await page.locator('#p1Equipment [data-region="shoes"]').check();
    await page.locator('#p1Equipment [data-color="shoes"]').fill('#ee2233');
    await page.locator('#p1Equipment [data-region="headband"]').check();
    await page.locator('#p1Equipment [data-color="headband"]').fill('#ffdd11');
    await page.locator('#p2Equipment summary').click();
    await page.locator('#p2Equipment [data-region="shoes"]').check();
    await page.locator('#p2Equipment [data-color="shoes"]').fill('#66ddff');
    await page.locator('#blueOffenseSelect').selectOption('pick_pop');
    await page.locator('#coralOffenseSelect').selectOption('post_up');
    await page.reload();
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    await page.locator('#primaryQuickPlayBtn').click();
    assert.equal(await page.locator('#blueOffenseSelect').inputValue(),'pick_pop');
    assert.equal(await page.locator('#coralOffenseSelect').inputValue(),'post_up');
    assert.equal(await page.evaluate(()=>selectedP1Equipment.shorts.color),'#11aa66');
    assert.equal(await page.locator('#p1Equipment [data-color="shoes"]').inputValue(),'#ee2233');
    await page.locator('#startBtn').click();
    const models=await page.evaluate(()=>{
      const colors=char=>Object.fromEntries(char.playerModel.materials.map(material=>[material.name,material.color.getHex()]));
      return matchPlayers.map(char=>({id:char.id,team:char.teamId,colors:colors(char),skin:char.profile.skin,ring:char.teamRing.visible}));
    });
    for(const player of models){
      assert.equal(player.colors.skin,player.skin,'equipment keeps skin separate');
      assert.equal(player.colors.sneaker,player.team===0?0xee2233:0x66ddff,'AI and human equipment use their team');
      assert.ok(player.ring,'team identification remains visible');
      if(player.team===0){assert.equal(player.colors.jersey,0x2255ee);assert.equal(player.colors.shorts,0x11aa66);}
    }
    console.log('PASS: independent jersey/shorts/shoes persist through reload and render on six real GLBs');
    const instant=await page.evaluate(()=>{
      const color=char=>char.playerModel.materials.find(material=>material.name==='sneaker').color.getHex();
      const other=getTeammates(p1)[0],before=color(other),skin=p1.playerModel.materials.find(m=>m.name==='skin').color.getHex();
      updateBasketballPlayerEquipment(p1,{shoes:{color:0x9911aa},shorts:{color:0x1100ff}});
      return {changed:color(p1),before,after:color(other),skin,skinAfter:p1.playerModel.materials.find(m=>m.name==='skin').color.getHex()};
    });
    assert.equal(instant.changed,0x9911aa);assert.equal(instant.before,instant.after);assert.equal(instant.skin,instant.skinAfter);
    console.log('PASS: live equipment update changes one player without shared material or skin bleed');
    const calls=await page.evaluate(()=>{
      countdownTimer=0;possessionFrozen=false;takePossession(p1,{silent:true});
      onKeyDown({code:'KeyB',key:'b',repeat:false,shiftKey:false,altKey:false});
      const screen=aiTeamPlans[p1.teamId].action;
      onKeyUp({code:'KeyB',key:'b'});
      onKeyDown({code:'KeyB',key:'b',repeat:false,shiftKey:true,altKey:false});
      const cut={action:aiTeamPlans[p1.teamId].action,phase:aiTeamPlans[p1.teamId].phase};
      onKeyUp({code:'KeyB',key:'b'});
      return {screen,cut};
    });
    assert.ok(['pick_roll','pick_pop'].includes(calls.screen),'B requests a screen');
    assert.deepEqual(calls.cut,{action:'give_go',phase:'cut'},'Shift+B requests a cut');
    await page.evaluate(()=>{returnToCharacterMenu();showQuickPlaySetup(3,true);localTeamMode='cooperative';startGame();});
    const cooperative=await page.evaluate(()=>({sameTeam:p1.teamId===p2.teamId,p1:p1.appearance.regions.shoes.color,p2:p2.appearance.regions.shoes.color}));
    assert.ok(cooperative.sameTeam);assert.equal(cooperative.p1,0xee2233);assert.equal(cooperative.p2,0x66ddff);
    await page.evaluate(()=>{returnToCharacterMenu();showQuickPlaySetup(3,false);});
    await page.setViewportSize({width:390,height:844});
    await page.locator('#p1Equipment summary').click();
    const overflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1);
    assert.equal(overflow,false,'expanded equipment fits portrait mobile');
    if(process.env.BASKETBALL_SCREENSHOTS){
      await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'equipment-mobile.png')});
    }
    await page.locator('#p1Cards .card:has([data-profile="flash"])').click();
    const quickProfile=await page.evaluate(()=>localStorage.getItem('basketball2MatchV1'));
    await page.evaluate(()=>showPrimaryMenu());
    await page.locator('#primaryTournamentBtn').click();
    await page.locator('#tourP1Cards .card:has([data-profile="tower"])').click();
    assert.equal(await page.evaluate(()=>localStorage.getItem('basketball2MatchV1')),quickProfile,'Tour setup keeps Quick Play profile save');
    await page.evaluate(()=>showPrimaryMenu());
    await page.locator('#primaryQuickPlayBtn').click();
    assert.equal(await page.evaluate(()=>selectedP1),'flash');
    assert.equal(await page.locator('#p1Cards .card.selected canvas').getAttribute('data-profile'),'flash','highlight matches the player Start will create');
    assert.equal(await page.locator('#p1Equipment [data-color="shoes"]').inputValue(),'#ee2233','restored equipment controls match saved model colors');
    assert.deepEqual(errors,[]);
    console.log('PASS: cooperative equipment, play calls, mobile layout and independent Tour/Quick Play profiles');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
