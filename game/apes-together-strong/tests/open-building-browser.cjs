/* Exercise the real Build buttons on desktop and touch, then render circular plots. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {pathToFileURL}=require('node:url'),{chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{for(const mobile of [false,true]){
  const page=await browser.newPage({viewport:mobile?{width:390,height:844}:{width:1440,height:900},isMobile:mobile,hasTouch:mobile}),errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  await page.goto(pathToFileURL(path.resolve(__dirname,'../../apes-together-strong.html')).href);
  await page.waitForFunction(()=>ATSVisualAssets.status==='ready');await page.locator('#seedInput').fill('OPEN-BUILDING');await page.locator('#newRun').click();await page.waitForFunction(()=>ATS.screen==='play');
  await page.evaluate(()=>{
   const g=ATS.game;g.update=()=>{};g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.settlements=[];g.effects=[];
   g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;g.world.settlementPlot=()=>({valid:true,trees:[],rocks:[],blocked:[]});
   g.king.x=g.king.y=0;g.king.hp=g.king.maxHp;g.time=100;g.progression.tier=0;
   const s={id:'open-build-gallery',name:'Circlewood',x:0,y:0,population:1,level:1,radius:100,food:100000,wood:100000,livingFounding:true};g.settlements.push(s);
   g.makeApe(0,10,'settled',s.id);g.refreshSettlements();g.colonies.init(s);window.buildHome=s;
  });
  if(mobile){await page.locator('#armyDock .army-expand').tap();await page.locator('[data-army-command=build]').tap()}else await page.keyboard.press('b');
  await page.waitForFunction(()=>ATS.screen==='build');
  for(let i=0;i<20;i++)await page.locator('[data-structure=spearTower]').click();
  for(let i=0;i<20;i++)await page.locator('[data-structure=hut]').click();
  await page.locator('[data-structure=canopyHut]').click();
  const orders=await page.evaluate(()=>({count:buildHome.projects.length,wood:buildHome.wood,food:buildHome.food}));
  assert.deepEqual(orders,{count:41,wood:100000-20*26-20*8-55,food:100000-20*12-30});
  assert.equal(await page.locator('[data-structure=spearTower]').isEnabled(),true);
  assert.match(await page.locator('#buildCatalog').innerText(),/No building limit/);
  assert.match(await page.locator('#buildResources').innerText(),/41 queued commissions/);
  await page.evaluate(()=>ATS.game.world.settlementPlot=()=>({valid:false,pending:true,trees:[],blocked:[]}));
  await page.locator('[data-structure=training]').click();
  assert.match(await page.locator('#buildQueue').innerText(),/Finding the next open plot/);
  assert.equal(await page.evaluate(()=>buildHome.projects.at(-1).awaitingPlot),true);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'open-building-menu-mobile.png':'open-building-menu-desktop.png')})}
  const placed=await page.evaluate(mobile=>{
   const g=ATS.game,s=buildHome,paid=s.wood;g.world.settlementPlot=()=>({valid:true,trees:[],rocks:[],blocked:[]});
   for(let i=0;i<300&&s.projects.some(p=>p.awaitingPlot);i++)g.colonies.locateCommissions(s);
   for(const p of s.projects)g.colonies.complete(s,p);s.projects=[];g.world.syncSettlementBuildings(s,g);
   ATS.renderer.camera.x=0;ATS.renderer.camera.y=0;ATS.renderer.camera.zoom=mobile?.24:.57;g.viewRadius=2200;
   return{paidOnce:paid===s.wood,homes:s.huts.length,towers:s.facilities.filter(f=>f.kind==='spearTower').length,radius:s.developedRadius};
  },mobile);
  assert.equal(placed.paidOnce,true);assert.equal(placed.homes,21);assert.equal(placed.towers,20);assert.ok(placed.radius>500);
  await page.keyboard.press('Escape');await page.waitForFunction(()=>ATS.screen==='play');
  await page.evaluate(()=>document.getElementById('toastArea').replaceChildren());await page.waitForTimeout(150);
  if(process.env.QA_ARTIFACT_DIR)await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'open-building-rings-mobile.png':'open-building-rings-desktop.png')});
  assert.deepEqual(errors,[]);await page.close();
 }
 console.log('PASS: desktop/touch Build buttons accept 42 affordable orders at one resident, advanced housing, pending plots, exact payment, concentric growth, no overflow or browser errors.');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
