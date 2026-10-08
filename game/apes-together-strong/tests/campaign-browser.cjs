/* Real bundled HTML: soundtrack, campaign controls, E gestures, persistence and touch. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const{pathToFileURL}=require('node:url'),{chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--no-sandbox']});const errors=[];
 const shot=async(p,name)=>{if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await p.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,name+'.png')});}};
 try{
  const page=await browser.newPage({viewport:{width:1440,height:960}});page.on('pageerror',e=>errors.push(e.message));
  const url=pathToFileURL(path.resolve(__dirname,'../../apes-together-strong.html')).href;await page.goto(url);
  assert.equal(await page.locator('#campaignMusic').evaluate(a=>a.paused),true);await page.locator('#newRun').click();
  await page.waitForFunction(()=>!document.getElementById('campaignMusic').paused&&document.getElementById('campaignMusic').currentTime>.05);
  assert.ok(await page.locator('#campaignMusic').evaluate(a=>a.volume<=.1&&a.loop&&a.readyState>=2));
  await page.evaluate(()=>{const g=ATS.game;g.king.hp=g.king.maxHp=1e6;g.makeApe(g.king.x+30,g.king.y,'follow');window.issued=[];const command=g.command.bind(g);g.command=(name,...args)=>{issued.push(name);return command(name,...args)}});
  await page.keyboard.press('e');assert.deepEqual(await page.evaluate(()=>issued.splice(0)),['nearestTarget']);assert.equal(await page.evaluate(()=>ATS.game.apes[0].nearestOrder?.kind),'all');
  await page.keyboard.down('e');await page.waitForTimeout(380);assert.deepEqual(await page.evaluate(()=>issued.slice()),['nearestHuman']);assert.equal(await page.evaluate(()=>ATS.game.apes[0].nearestOrder?.kind),'human');await page.keyboard.up('e');assert.deepEqual(await page.evaluate(()=>issued.splice(0)),['nearestHuman'],'hold never issues tap on release');
  await page.keyboard.down('e');await page.keyboard.press('Escape');await page.keyboard.up('e');assert.deepEqual(await page.evaluate(()=>issued.splice(0)),[],'pause cancels pending gesture');
  assert.equal(await page.locator('#campaignMusic').evaluate(a=>a.paused),true);
  await page.locator('#settingsButton').click();await page.locator('#musicToggle').uncheck();await page.locator('#backSettings').click();await page.locator('#resumeRun').click();assert.equal(await page.locator('#campaignMusic').evaluate(a=>a.paused),true);
  await page.keyboard.press('Escape');await page.locator('#settingsButton').click();await page.locator('#musicToggle').check();await page.locator('#backSettings').click();await page.locator('#resumeRun').click();await page.waitForFunction(()=>!document.getElementById('campaignMusic').paused);
  await page.evaluate(()=>{const g=ATS.game;g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false,road:false});for(let i=0;i<24;i++)g.makeApe(g.king.x+i%5*6,g.king.y+Math.floor(i/5)*6,'follow');g.food=500;g.commandCD=0;g.command('settleAll');const s=g.settlements[0];s.food=500;s.wood=200;for(let i=0;i<40;i++)g.makeApe(g.king.x+50+i%7*9,g.king.y+Math.floor(i/7)*9,'follow');const a=g.apes.at(-1);a.species='gorilla';g.siege.balance(a,true);g.champions.promote(a,{archetype:'wallbreaker',name:'Flint Wallbreaker'});g.world.reveal(g.king.x,g.king.y,900);g.syncIndexes();});
  await page.keyboard.press('m');await page.getByLabel('Division',{exact:true}).selectOption('vanguard');await page.getByRole('button',{name:'Form division',exact:true}).click();
  await page.getByLabel('Stance',{exact:true}).selectOption('assault');await page.getByLabel('Formation',{exact:true}).selectOption('line');
  await page.locator('.settlement-row select[aria-label$=" specialization"]').selectOption('sanctuary');await page.getByRole('button',{name:'Set rally point',exact:true}).click();
  assert.equal(await page.evaluate(()=>ATS.game.champions.divisions.vanguard.stance),'assault');assert.equal(await page.evaluate(()=>ATS.game.settlements[0].specialization),'sanctuary');
  await page.locator('#championCouncil summary').click();await page.locator('#championCouncil').scrollIntoViewIfNeeded();await shot(page,'campaign-council');
  await page.locator('#closeOverview').click();await page.keyboard.press('Escape');await page.locator('#saveRun').click();await page.reload();await page.locator('#continueRun').click();
  assert.equal(await page.evaluate(()=>ATS.game.champions.divisions.vanguard.formation),'line');assert.equal(await page.evaluate(()=>ATS.game.settlements[0].specialization),'sanctuary');
  await page.evaluate(()=>{const g=ATS.game;g.king.hp=g.king.maxHp=1e6;g.spawnSites=()=>{};g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world._buildSite({objects:[],sites:[]},{id:'qa-blacksite',x:g.king.x,y:g.king.y,name:'Blacksite K-12',type:'armoredDepot',prisonKind:'blacksite',military:true,tier:4,guards:12,count:44,radius:540,extentX:466,extentY:300,layout:0});const s=g.world.sites.get('qa-blacksite');s.known=true;g.world.getSites=()=>[s];g._weather={kind:'storm',until:g.time+120,wind:.5,thunderAt:g.time+20};ATS.renderer.camera.zoom=.8;});
  await page.keyboard.press('m');await page.locator('#prisonOperations').scrollIntoViewIfNeeded();assert.match(await page.locator('#prisonOperations').innerText(),/Blacksite K-12.*Research Compound/s);assert.match(await page.locator('#prisonOperations').innerText(),/power locked|control locked/);await shot(page,'campaign-operation');await page.locator('#closeOverview').click();await page.waitForTimeout(120);await shot(page,'campaign-blacksite');
  await page.keyboard.press('Escape');await page.locator('#settingsButton').click();await page.locator('#reducedMotion').check();await page.locator('#qualitySelect').selectOption('low');await page.locator('#backSettings').click();await page.locator('#resumeRun').click();await page.waitForTimeout(100);assert.equal(await page.evaluate(()=>ATS.screen),'play');
  const touch=await browser.newPage({viewport:{width:390,height:844},isMobile:true,hasTouch:true});touch.on('pageerror',e=>errors.push(e.message));await touch.goto(url);await touch.locator('#newRun').tap();await touch.locator('#mapButton').tap();await touch.locator('#championCouncil').scrollIntoViewIfNeeded();assert.equal(await touch.locator('#championCouncil').isVisible(),true);assert.ok(await touch.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));await shot(touch,'campaign-touch');
  assert.deepEqual(errors,[]);console.log('PASS: real embedded music decode/play/pause, independent mute, tap/hold E, council/division controls, specialization, saved elites/orders, prison recon, storm and low-detail rendering, touch layout; no browser errors.');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
