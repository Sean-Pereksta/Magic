/* Real keyboard and touch gestures against the playable app (Playwright/Chrome). */
'use strict';
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{pathToFileURL}=require('node:url'),{chromium}=require('playwright');
const root=path.resolve(__dirname,'..');
async function fresh(page){await page.evaluate(()=>{
 const g=ATS.game;g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.settlements=[];g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;g.king.x=g.king.y=0;g.king.hp=g.king.maxHp;g.commandCD=0;g.siege.select([]);g.siege.groups={};g.time=1;g.progression.acknowledgedTier=2;
 const s={id:'input-home',name:'Input Home',x:1000,y:0,radius:100,population:0,level:1,food:100,wood:100,age:0,known:false,attack:false,birthTimer:0,starveTimer:0,lastRaid:-120,nextWarn:0};g.settlements.push(s);g.colonies.init(s);
 for(let i=0;i<5;i++){g.makeApe(100+i*20,50,'free');g.makeApe(70+i*20,0,'hold');g.makeApe(2000+i*20,0,'hold');g.makeApe(1000+i*20,0,'settled',s.id)}g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild([]);g.refreshSettlements();ATS.army.update(true);
 window.inputCommands=[];if(!g._inputOriginal){g._inputOriginal=g.command;g.command=function(cmd,...args){const result=this._inputOriginal(cmd,...args);if(['call','recall','recallField','recallAll'].includes(cmd)&&result)inputCommands.push({...this.lastHordeCommand});return result}}
 });}
async function commands(page){return page.evaluate(()=>inputCommands.map(c=>({cmd:c.cmd,count:c.count,mobilized:c.mobilized})))}
async function nearbyResidents(page){await page.evaluate(()=>{const g=ATS.game;for(const [i,a] of g.apes.filter(a=>a.settlementId).slice(0,2).entries()){a.x=200+i*40;a.y=0;a.job='builder'}g.apeGrid.rebuild([g.king,...g.apes])})}
async function checkRecruitment(page){assert.deepEqual(await commands(page),[{cmd:'call',count:7,mobilized:2}]);assert.deepEqual(await page.evaluate(()=>({residents:ATS.game.apes.filter(a=>a.settlementId).length,mobilized:ATS.game.apes.filter(a=>a.previousSettlementAssignment&&a.state==='follow'&&!a.job).length,holding:ATS.game.apes.filter(a=>a.state==='hold').length})),{residents:3,mobilized:2,holding:10})}
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']}),errors=[];let sourceHtml;
 try{
  const page=await browser.newPage({viewport:{width:1440,height:900}});page.on('pageerror',e=>{errors.push(e.message);console.error('Browser:',e.message)});
  if(process.argv.includes('--source')){
   const parts=fs.readFileSync(path.join(root,'build.cjs'),'utf8').match(/const parts=(\[[^;]+\]);/)[1],names=JSON.parse(parts.replace(/'/g,'"')).filter(name=>fs.existsSync(path.join(root,name+'.js')));
   const html=sourceHtml=fs.readFileSync(path.join(root,'shell.html'),'utf8').replace('<!-- CAMPAIGN_MUSIC -->','<audio id="campaignMusic" loop preload="none"></audio>').replace('</body>',names.map(name=>'<script>'+fs.readFileSync(path.join(root,name+'.js'),'utf8')+'</script>').join('\n')+'</body>');
   await page.route('https://ats-input.test/**',route=>route.fulfill({contentType:'text/html',body:html}));await page.goto('https://ats-input.test/');
  }else await page.goto(pathToFileURL(path.resolve(root,'../apes-together-strong.html')).href);
  await page.waitForFunction(()=>ATSVisualAssets.status==='ready');await page.locator('#seedInput').fill('HORDE-INPUT');await page.locator('#newRun').click();await page.waitForFunction(()=>ATS.screen==='play'&&ATS.game.time>.1);
  await fresh(page);await nearbyResidents(page);await page.keyboard.press('q');await checkRecruitment(page);
  await fresh(page);await page.keyboard.press('r');assert.deepEqual(await commands(page),[{cmd:'recall',count:5,mobilized:0}]);
  await fresh(page);await page.keyboard.down('r');await page.waitForFunction(()=>document.getElementById('commandHoldProgress').value>0,undefined,{timeout:5000});assert.equal(await page.locator('#commandHoldIndicator').isVisible(),true);assert.ok(await page.locator('#commandHoldProgress').evaluate(el=>el.value)>0,JSON.stringify(await page.evaluate(()=>({screen:ATS.screen,hidden:document.hidden,now:performance.now(),error:document.getElementById('errorMessage').textContent,time:ATS.game.time,progress:document.getElementById('commandHoldProgress').value}))));await page.keyboard.down('r');await page.waitForTimeout(650);await page.keyboard.up('r');assert.deepEqual(await commands(page),[{cmd:'recallField',count:10,mobilized:0}]);
  await fresh(page);await page.keyboard.press('t');assert.deepEqual(await commands(page),[{cmd:'recallField',count:10,mobilized:0}]);
  await fresh(page);await page.keyboard.down('t');await page.waitForTimeout(350);assert.deepEqual(await commands(page),[]);assert.match(await page.locator('#commandHoldLabel').innerText(),/Mobilize settlements/);await page.waitForTimeout(330);await page.keyboard.up('t');assert.deepEqual(await commands(page),[{cmd:'recallAll',count:15,mobilized:5}]);assert.equal(await page.evaluate(()=>ATS.game.apes.filter(a=>a.previousSettlementAssignment).length),5);
  await fresh(page);await page.keyboard.down('t');await page.waitForTimeout(180);await page.keyboard.press('Escape');await page.keyboard.up('t');assert.equal(await page.evaluate(()=>ATS.screen),'pause');assert.deepEqual(await commands(page),[]);await page.locator('#resumeRun').click();assert.equal(await page.locator('#commandHoldIndicator').isVisible(),false);
  await fresh(page);await page.keyboard.down('t');await page.waitForTimeout(120);await page.evaluate(()=>window.dispatchEvent(new Event('blur')));await page.keyboard.up('t');assert.deepEqual(await commands(page),[]);await page.locator('#resumeRun').click();
  const context=await browser.newContext({viewport:{width:800,height:850},hasTouch:true,isMobile:true}),mobile=await context.newPage();mobile.on('pageerror',e=>errors.push(e.message));
  // The source harness remains entirely local; copy its fulfilled document.
  if(process.argv.includes('--source')){await mobile.route('https://ats-input.test/**',route=>route.fulfill({contentType:'text/html',body:sourceHtml}));await mobile.goto('https://ats-input.test/')}else await mobile.goto(pathToFileURL(path.resolve(root,'../apes-together-strong.html')).href);
  await mobile.waitForFunction(()=>window.ATS?.screen==='menu');await mobile.locator('#newRun').click();await mobile.waitForFunction(()=>ATS.game?.time>.1);await mobile.locator('#armyDock .army-expand').tap();
  const cdp=await context.newCDPSession(mobile),touch=async(key,hold)=>{const rect=await mobile.locator('[data-army-command="'+key+'"]').boundingBox();assert.ok(rect);const point={x:rect.x+rect.width/2,y:rect.y+rect.height/2};await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[point]});await mobile.waitForTimeout(hold);await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await mobile.waitForTimeout(80)};
  await fresh(mobile);await nearbyResidents(mobile);await touch('call',80);await checkRecruitment(mobile);
  await fresh(mobile);await touch('recall',80);assert.deepEqual(await commands(mobile),[{cmd:'recall',count:5,mobilized:0}]);
  await fresh(mobile);await touch('recall',700);assert.deepEqual(await commands(mobile),[{cmd:'recallField',count:10,mobilized:0}]);
  await fresh(mobile);await touch('recallField',90);assert.deepEqual(await commands(mobile),[{cmd:'recallField',count:10,mobilized:0}]);
  await fresh(mobile);await touch('recallField',700);assert.deepEqual(await commands(mobile),[{cmd:'recallAll',count:15,mobilized:5}]);
  await fresh(mobile);const box=await mobile.locator('[data-army-command="recallField"]').boundingBox();await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:box.x+20,y:box.y+20}]});await mobile.waitForTimeout(180);await cdp.send('Input.dispatchTouchEvent',{type:'touchCancel',touchPoints:[]});await mobile.waitForTimeout(650);assert.deepEqual(await commands(mobile),[]);
  assert.deepEqual(errors,[]);console.log('Horde browser QA passed: Q/R/T real keyboard taps/holds/repeat; pause/blur cancellation; real touch taps/holds/cancel; one command per gesture.');
 }finally{await browser.close()}
})().catch(error=>{console.error(error);process.exitCode=1});
