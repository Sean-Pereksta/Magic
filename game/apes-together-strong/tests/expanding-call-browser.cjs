/* Expanding Q through the real keyboard, dock and touch wheel. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {pathToFileURL}=require('node:url'),{chromium}=require('playwright');
const url=pathToFileURL(path.resolve(__dirname,'../../apes-together-strong.html')).href;
async function fresh(page){await page.evaluate(()=>{
 const g=ATS.game;g.update=()=>{};g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.settlements=[];g.siege.groups={};g.siege.select([]);g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.terrain=()=>({biome:'forest',water:false});g.king.x=g.king.y=0;g.king.hp=g.king.maxHp;g.commandCD=0;
 const home={id:'call-home',name:'Call Home',x:1500,y:0,radius:100,population:1,level:1,food:100,wood:100,age:0,known:false,attack:false,birthTimer:0,starveTimer:0,lastRaid:-120,nextWarn:0};g.settlements.push(home);g.colonies.init(home);
 window.callFixture={};for(const [name,x,state]of [['near',90,'free'],['middle',650,'free'],['outer',1500,'free'],['outside',1700,'free'],['field',20000,'hold'],['resident',1500,'settled'],['captive',400,'free']]){const a=g.makeApe(x,0,state,state==='settled'?home.id:null);callFixture[name]=a.id;if(name==='captive')a.captive=true}
 g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.refreshSettlements();ATS.army.update(true);window.callCommands=[];
 if(!g._callOriginal){g._callOriginal=g.command;g.command=function(cmd,...args){const ok=this._callOriginal(cmd,...args);if(ok&&['call','recallAll'].includes(cmd))callCommands.push({...this.lastHordeCommand});return ok}}
});}
const commands=page=>page.evaluate(()=>callCommands.map(c=>({cmd:c.cmd,count:c.count,mobilized:c.mobilized})));
const preview=page=>page.evaluate(()=>ATS.renderer.callPreview?.radius||0);
async function fullResult(page){assert.deepEqual(await commands(page),[{cmd:'recallAll',count:2,mobilized:1}]);assert.deepEqual(await page.evaluate(()=>['near','middle','outer','outside','captive'].map(n=>ATS.game.apesById.get(callFixture[n]).state)),['free','free','free','free','free']);}

(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});const errors=[];
 try{
  const page=await browser.newPage({viewport:{width:1440,height:900}});page.on('pageerror',e=>errors.push(e.message));await page.goto(url);await page.locator('#newRun').click();await page.waitForFunction(()=>ATS.screen==='play');
  await fresh(page);await page.keyboard.press('q');assert.deepEqual(await commands(page),[{cmd:'call',count:1,mobilized:0}]);
  await page.waitForTimeout(400);await fresh(page);await page.keyboard.down('q');assert.ok(await preview(page)<=180,'Q starts with a small circle');await page.waitForFunction(()=>ATS.renderer.callPreview?.radius>600,undefined,{timeout:1000});const first=await preview(page);assert.deepEqual(await commands(page),[]);await page.waitForTimeout(300);assert.ok(await preview(page)>first+200);
  await page.keyboard.up('q');assert.deepEqual(await commands(page),[{cmd:'call',count:2,mobilized:0}]);assert.equal(await preview(page),0);
  if(process.env.QA_ARTIFACT_DIR){await fresh(page);await page.keyboard.down('q');await page.waitForTimeout(300);fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,'expanding-q.png')});await page.keyboard.press('Escape');await page.keyboard.up('q');await page.locator('#resumeRun').click();}
  await fresh(page);await page.keyboard.down('q');await page.keyboard.down('q');await page.waitForTimeout(1280);await fullResult(page);await page.keyboard.up('q');await fullResult(page);assert.equal(await preview(page),0);
  await fresh(page);await page.keyboard.down('q');await page.waitForTimeout(300);await page.keyboard.press('Escape');await page.keyboard.up('q');await page.waitForTimeout(1250);assert.deepEqual(await commands(page),[]);assert.equal(await preview(page),0);await page.locator('#resumeRun').click();
  await fresh(page);await page.keyboard.down('q');await page.waitForTimeout(200);await page.evaluate(()=>{Object.defineProperty(document,'hidden',{configurable:true,get:()=>true});document.dispatchEvent(new Event('visibilitychange'))});await page.keyboard.up('q');await page.evaluate(()=>{delete document.hidden;document.dispatchEvent(new Event('visibilitychange'))});assert.deepEqual(await commands(page),[]);assert.equal(await preview(page),0);await page.locator('#resumeRun').click();
  // A newer direct UI command cancels E's pending single tap.
  await page.evaluate(()=>{window.controlCommands=[];const g=ATS.game,original=g.command;g.command=function(cmd,...args){controlCommands.push(cmd);return original.call(this,cmd,...args)}});
  await page.keyboard.press('e');await page.locator('[data-army-command="hold"]').click();await page.waitForTimeout(320);assert.deepEqual(await page.evaluate(()=>controlCommands),['hold']);
  await page.evaluate(()=>controlCommands.length=0);await page.keyboard.press('e');await page.locator('[data-species="gibbon"]').click();await page.waitForTimeout(320);assert.deepEqual(await page.evaluate(()=>controlCommands),[]);

  const context=await browser.newContext({viewport:{width:800,height:850},hasTouch:true,isMobile:true}),mobile=await context.newPage();mobile.on('pageerror',e=>errors.push(e.message));await mobile.goto(url);await mobile.locator('#newRun').tap();await mobile.waitForFunction(()=>ATS.screen==='play');await mobile.evaluate(()=>ATS.mobileCommands.toggleArmy(true));
  const cdp=await context.newCDPSession(mobile),send=(type,points=[])=>cdp.send('Input.dispatchTouchEvent',{type,touchPoints:points});
  async function dockStart(){const b=await mobile.locator('[data-army-command="call"]').boundingBox();await send('touchStart',[{x:b.x+b.width/2,y:b.y+b.height/2,id:1}]);}
  await fresh(mobile);await dockStart();await mobile.waitForTimeout(620);assert.ok(await preview(mobile)>800);await send('touchEnd');assert.deepEqual(await commands(mobile),[{cmd:'call',count:2,mobilized:0}]);
  await fresh(mobile);await dockStart();await mobile.waitForTimeout(1280);await fullResult(mobile);await send('touchEnd');await fullResult(mobile);
  await fresh(mobile);await dockStart();await mobile.waitForTimeout(250);await send('touchCancel');await mobile.waitForTimeout(1250);assert.deepEqual(await commands(mobile),[]);assert.equal(await preview(mobile),0);
  await mobile.evaluate(()=>ATS.mobileCommands.toggleArmy(false));
  async function wheelCall(){await send('touchStart',[{x:400,y:390,id:1}]);await mobile.waitForTimeout(300);const p=await mobile.evaluate(()=>{const g=ATS.mobileCommands.gesture,i=g.items.findIndex(x=>x[0]==='call'),angle=g.offset+i*Math.PI*2/g.items.length;return{x:g.cx+Math.cos(angle)*80,y:g.cy+Math.sin(angle)*80,id:1}});await send('touchMove',[p]);}
  await fresh(mobile);await wheelCall();await mobile.waitForTimeout(620);assert.ok(await preview(mobile)>800);await send('touchEnd');assert.deepEqual(await commands(mobile),[{cmd:'call',count:2,mobilized:0}]);
  await fresh(mobile);await wheelCall();await mobile.waitForTimeout(1280);await fullResult(mobile);await send('touchEnd');await fullResult(mobile);
  await fresh(mobile);await wheelCall();await mobile.waitForTimeout(300);await send('touchMove',[{x:400,y:390,id:1}]);await mobile.waitForFunction(()=>!ATS.renderer.callPreview);await send('touchEnd');assert.deepEqual(await commands(mobile),[]);
  await mobile.evaluate(()=>{window.mobileOrders=[];const g=ATS.game,original=g.command;g.command=function(cmd,...args){mobileOrders.push(cmd);return original.call(this,cmd,...args)}});await mobile.keyboard.press('e');await send('touchStart',[{x:400,y:390,id:1}]);await mobile.waitForTimeout(320);await send('touchCancel');assert.deepEqual(await mobile.evaluate(()=>mobileOrders),[]);
  assert.deepEqual(errors,[]);console.log('PASS: growing Q circle, partial area, unchanged full recall, repeat/pause/visibility cancellation, pending E override, and real touch dock/wheel partial/full/cancel.');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
