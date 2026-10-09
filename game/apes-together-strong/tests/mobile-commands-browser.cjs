/* Real touchscreen events through the playable app. Run with Playwright installed. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{chromium}=require('playwright');
const root=path.resolve(__dirname,'..');
const names=JSON.parse(fs.readFileSync(path.join(root,'build.cjs'),'utf8').match(/const parts=(\[[^;]+\]);/)[1].replace(/'/g,'"'));
const html=fs.readFileSync(path.join(root,'shell.html'),'utf8').replace('<!-- CAMPAIGN_MUSIC -->','<audio id="campaignMusic"></audio>').replace('</body>',names.map(n=>'<script>'+fs.readFileSync(path.join(root,n+'.js'),'utf8')+'</script>').join('\n')+'</body>');
(async()=>{
 const browser=await chromium.launch({headless:true,args:['--no-sandbox','--disable-dev-shm-usage'],...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});
 try{
  const context=await browser.newContext({viewport:{width:430,height:860},hasTouch:true,isMobile:true}),page=await context.newPage(),errors=[];
  page.setDefaultTimeout(8000);page.on('pageerror',e=>errors.push(e.message));await page.route('https://ats-mobile.test/**',r=>r.fulfill({contentType:'text/html',body:html}));await page.goto('https://ats-mobile.test/');
  await page.locator('#newRun').click();await page.waitForFunction(()=>ATS.game?.time>.1);
  await page.evaluate(()=>{const g=ATS.game;g.humans=[];g.vehicles=[];g.helis=[];g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.spawnSites=()=>{};g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;g.king.hp=g.king.maxHp;g.progression.acknowledgedTier=2;window.commands=[];window.strikes=0;const command=g.command.bind(g);g.command=(cmd,...args)=>{commands.push(cmd);return command(cmd,...args)};const attack=g.attack.bind(g);g.attack=(...args)=>{strikes++;return attack(...args)}});
  const cdp=await context.newCDPSession(page),send=(type,points)=>cdp.send('Input.dispatchTouchEvent',{type,touchPoints:points}),start={x:210,y:370,id:1};
  const clear=()=>page.evaluate(()=>{commands=[];strikes=0;ATS.game.commandCD=0});
  const open=async(point=start)=>{await send('touchStart',[point]);await page.waitForTimeout(300);assert.equal(await page.locator('#mobileCommandWheel').isVisible(),true)};
  const pick=async(id)=>{const p=await page.evaluate(id=>{const w=ATS.mobileCommands,g=w.gesture,i=g.items.findIndex(x=>x[0]===id);if(i<0)throw Error(id+' missing');const a=g.offset+i*Math.PI*2/g.items.length;return{x:g.cx+82*Math.cos(a),y:g.cy+82*Math.sin(a),id:1}},id);await send('touchMove',[p]);return p};
  const release=()=>send('touchEnd',[]),calls=()=>page.evaluate(()=>commands);
  assert.equal(await page.locator('#armyDock').isVisible(),false);assert.equal(await page.locator('#mobileSettlementButton').isVisible(),false);
  for(const id of ['nearestTarget','hold','call','recall']){await clear();await open();await pick(id);await release();assert.deepEqual(await calls(),[id]);assert.equal(await page.evaluate(()=>strikes),0)}
  await clear();await open();await pick('nearestTarget');await page.waitForTimeout(310);await release();assert.deepEqual(await calls(),['nearestHuman']);
  for(const id of ['recall','recallField','recallAll']){await clear();await open();await pick('recall');await page.waitForTimeout(650);assert.deepEqual(await calls(),[]);await pick(id);await release();assert.deepEqual(await calls(),[id])}
  await clear();await open();await pick('call');await pick('hold');await release();assert.deepEqual(await calls(),['hold']);
  await clear();await open();await pick('recall');await send('touchMove',[start]);await page.waitForTimeout(650);await release();assert.deepEqual(await calls(),[]);
  await clear();await open();await pick('call');await send('touchCancel',[]);assert.equal(await page.locator('#mobileCommandWheel').isVisible(),false);assert.deepEqual(await calls(),[]);
  await clear();await send('touchStart',[start]);await page.waitForTimeout(70);await release();assert.equal(await page.evaluate(()=>strikes),1);assert.deepEqual(await calls(),[]);
  // A pending drag must never become a long press later.
  await clear();await send('touchStart',[start]);await send('touchMove',[{x:250,y:390,id:1}]);await page.waitForTimeout(300);assert.equal(await page.locator('#mobileCommandWheel').isVisible(),false);await release();assert.equal(await page.evaluate(()=>strikes),0);
  // Captured release over Strike is a wheel command, never a Strike click.
  await clear();await open();await send('touchMove',[{x:360,y:785,id:1}]);await release();assert.equal(await page.evaluate(()=>strikes),0);assert.equal((await calls()).length,1);
  // Both controls own their touch. A second finger cannot stop continuous Strike.
  for(const selector of ['#joystick','#attackButton']){
   await clear();const b=await page.locator(selector).boundingBox(),p={x:b.x+b.width/2,y:b.y+b.height/2,id:2};await send('touchStart',[p]);await page.waitForTimeout(320);assert.equal(await page.locator('#mobileCommandWheel').isVisible(),false);
   await send('touchStart',[p,start]);await page.waitForTimeout(300);assert.equal(await page.locator('#mobileCommandWheel').isVisible(),false);await send('touchEnd',[start]);
   if(selector==='#attackButton'){const before=await page.evaluate(()=>strikes);await page.waitForTimeout(80);assert.ok(await page.evaluate(()=>strikes)>before)}await release();assert.deepEqual(await calls(),[]);
  }
  await clear();await open();await page.evaluate(()=>window.dispatchEvent(new Event('blur')));await release();assert.deepEqual(await calls(),[]);assert.equal(await page.locator('#mobileCommandWheel').isVisible(),false);assert.equal(await page.evaluate(()=>ATS.screen),'pause',JSON.stringify(await page.evaluate(()=>({screen:ATS.screen,error:document.getElementById('errorMessage').textContent}))));await page.locator('#resumeRun').click();
  await clear();await open();await page.setViewportSize({width:860,height:430});await release();assert.deepEqual(await calls(),[]);await page.setViewportSize({width:430,height:860});
  // Edge hold remains cancellable and every visible sector stays on screen.
  await clear();await open({x:12,y:300,id:1});const rect=await page.locator('#mobileCommandWheel').boundingBox();assert.ok(rect.x>=0&&rect.x+rect.width<=430);await release();assert.deepEqual(await calls(),[]);
  await open();await pick('settlement');await release();assert.equal(await page.locator('#mobileSettlementActions').isVisible(),true);assert.equal(await page.locator('[data-mobile-action=build]').isDisabled(),true);
  await page.locator('[data-mobile-action=army]').tap();assert.equal(await page.locator('#armyDock').isVisible(),true);await page.locator('#mobileCloseArmy').tap();assert.equal(await page.locator('#armyDock').isVisible(),false);
  // Preserve selected-unit taps and pan/pinch, without striking or opening wheel.
  await page.evaluate(()=>{ATS.game.makeApe(ATS.game.king.x+30,ATS.game.king.y,'follow');ATS.game.siege.select(ATSSiegeSpecies);ATS.army.mode='route';ATS.army.update(true)});
  await clear();await send('touchStart',[start]);await release();assert.equal(await page.evaluate(()=>strikes),0);assert.ok(await page.evaluate(()=>Object.values(ATS.game.siege.drafts).some(x=>x.length>0)));
  await send('touchStart',[start]);await send('touchStart',[start,{x:300,y:370,id:2}]);await send('touchMove',[{x:180,y:370,id:1},{x:340,y:370,id:2}]);await page.waitForTimeout(300);assert.equal(await page.locator('#mobileCommandWheel').isVisible(),false);await release();
  // Context uses the existing 90-unit lodge range and opens existing build menu.
  await page.evaluate(()=>{const g=ATS.game,s={id:'mobile-home',name:'Mobile Home',x:g.king.x,y:g.king.y,radius:100,population:1,level:1,food:100,wood:100,age:0,known:false,attack:false,birthTimer:0,starveTimer:0,lastRaid:-120,nextWarn:0};g.settlements.push(s);g.colonies.init(s);g.makeApe(s.x,s.y,'settled',s.id);g.refreshSettlements()});
  await page.waitForFunction(()=>!document.getElementById('mobileSettlementButton').hidden);await page.locator('#mobileSettlementButton').tap();assert.equal(await page.evaluate(()=>ATS.screen),'build');assert.equal(await page.locator('#resourceExpeditions').isVisible(),true);await page.locator('#closeBuild').click();
  await page.evaluate(()=>ATS.game.king.x+=400);await page.waitForFunction(()=>document.getElementById('mobileSettlementButton').hidden);
  await clear();await page.keyboard.press('e');await page.waitForFunction(()=>commands.includes('nearestTarget'));assert.deepEqual(await calls(),['nearestTarget']);
  await clear();await page.keyboard.press('e');await page.keyboard.press('e');await page.waitForFunction(()=>commands.includes('spreadCharge'));await page.waitForTimeout(320);assert.deepEqual(await calls(),['spreadCharge'],'double E suppresses a delayed single command');
  await clear();await page.keyboard.down('e');await page.waitForFunction(()=>commands.includes('nearestHuman'));await page.keyboard.up('e');assert.deepEqual(await calls(),['nearestHuman']);
  await clear();await page.keyboard.press('e');await page.keyboard.down('e');await page.waitForFunction(()=>commands.includes('nearestHuman'));await page.keyboard.up('e');await page.waitForTimeout(320);assert.deepEqual(await calls(),['nearestHuman'],'second-press hold issues one combat-target command');
  await clear();await page.keyboard.press('e');await page.keyboard.press('Escape');assert.equal(await page.evaluate(()=>ATS.screen),'pause');await page.locator('#resumeRun').click();await page.waitForTimeout(320);assert.deepEqual(await calls(),[],'pause cancels an unresolved single E tap');
  await clear();await page.keyboard.press('e');await page.keyboard.down('e');await page.evaluate(()=>window.dispatchEvent(new Event('blur')));await page.keyboard.up('e');assert.equal(await page.evaluate(()=>ATS.screen),'pause');await page.locator('#resumeRun').click();await page.waitForTimeout(320);assert.deepEqual(await calls(),[],'blur cancels both presses before an E command');
  assert.deepEqual(errors,[]);console.log('Mobile command wheel passed: five commands, E tap/double-tap/hold, second-press hold and pause/blur cancellation, all recall scopes, center/cancel/blur/resize, direction changes, edge placement, control isolation, normal taps, selected routes, pinch, settlement range/menu.');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
