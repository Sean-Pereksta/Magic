/* Actual desktop and touch selection, route, council and blueprint controls. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {pathToFileURL}=require('node:url'),{chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{for(const mobile of [false,true]){
  const page=await browser.newPage({viewport:mobile?{width:430,height:860}:{width:1440,height:900},isMobile:mobile,hasTouch:mobile}),errors=[];page.on('pageerror',e=>errors.push(e.message));page.setDefaultTimeout(15000);
  const press=selector=>mobile?page.locator(selector).tap():page.locator(selector).click();
  await page.goto(pathToFileURL(path.resolve(__dirname,'../../apes-together-strong.html')).href);await page.waitForFunction(()=>ATSVisualAssets.status==='ready');await press('#newRun');await page.waitForFunction(()=>ATS.screen==='play');
  await page.evaluate(()=>{
   const g=ATS.game;g.update=()=>{};g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.settlements=[];g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;g.world._streaming=false;g.king.x=g.king.y=0;
   for(const species of ATSSiegeSpecies){const a=g.makeApe(-60,30,'follow');a.species=species;g.siege.balance(a,true)}
   const s={id:'strategy-home',name:'Willow',x:0,y:0,food:500,wood:0,population:12,livingFounding:true};g.settlements.push(s);for(let i=0;i<12;i++)g.makeApe(60,i*6,'settled',s.id);g.refreshSettlements();g.colonies.init(s);g.syncIndexes();g.siege.select('gorilla');ATS.army.update(true);ATS.renderer.camera.x=ATS.renderer.camera.y=0;ATS.renderer.camera.zoom=1;window.home=s;
  });
  if(mobile){
   const session=await page.context().newCDPSession(page);await session.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:210,y:340,id:1}]});await page.waitForTimeout(300);
   const point=await page.evaluate(()=>{const g=ATS.mobileCommands.gesture,i=g.items.findIndex(x=>x[0]==='settlement'),a=g.offset+i*Math.PI*2/g.items.length;return{x:g.cx+82*Math.cos(a),y:g.cy+82*Math.sin(a),id:1}});
   await session.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[point]});await session.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await press('[data-mobile-action=army]');
  }else await press('#armyDock .army-expand');
  await press('[data-army-command=climb]');assert.deepEqual((await page.evaluate(()=>ATS.game.siege.selected)).sort(),['capuchin','chimpanzee','gibbon']);assert.equal(await page.evaluate(()=>ATS.game.siege.selectedMembers().length),3);assert.equal(await page.evaluate(()=>ATS.army.mode),'route');
  if(mobile)assert.equal(await page.locator('#armyDock').isVisible(),false,'selection returns to the map for normal route input');
  const point=await page.evaluate(()=>ATS.renderer.project(120,-100));if(mobile)await page.touchscreen.tap(point.x,point.y);else await page.mouse.click(point.x,point.y);
  assert.deepEqual(await page.evaluate(()=>ATS.game.siege.drafts.gibbon.map(o=>o.type)),['move']);assert.equal(await page.evaluate(()=>ATS.game.siege.drafts.gorilla?.length||0),0);assert.deepEqual(errors,[]);
  await page.keyboard.press('o');await page.keyboard.press('b');await page.waitForFunction(()=>ATS.screen==='build');
  assert.match(await page.locator('#buildResources').innerText(),/Tier I Outpost.*Nursery \+0%.*Undetected/);assert.equal(await page.locator('[data-structure=perimeter]').isEnabled(),true);
  await press('[data-structure=perimeter]');assert.match(await page.locator('#buildFeedback').innerText(),/blueprint/);assert.equal(await page.evaluate(()=>home.wood),0);assert.equal(await page.evaluate(()=>home.barriers.length),0);assert.equal(await page.locator('[data-structure=perimeter]').isEnabled(),false);assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'strategy-mobile.png':'strategy-desktop.png')})}
  assert.deepEqual(errors,[]);await page.close();
 }console.log('PASS: desktop/touch climber-only selection, ordinary map move drafts, resident exclusion, council tiers/threat/growth, zero-resource perimeter blueprint, no overflow or browser errors.');}
 finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
