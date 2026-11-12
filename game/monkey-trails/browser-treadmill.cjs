const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..');
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
const saveName='Treadmill QA',password='treadmill-test-password';

// Real app, real encrypted saves, deterministic motion/permission inputs. No
// clock-driven distance is substituted for the phone's motion events.
async function setup(browser,options={}){
  const context=await browser.newContext({viewport:{width:options.width||390,height:844},isMobile:true,hasTouch:true});
  await context.route('**/*',async route=>{
    const url=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(url.pathname));
    if(url.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file)||!fs.statSync(file).isFile())return route.abort();
    await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});
  });
  const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.addInitScript(options=>{
    const watches=[],cleared=[],requests=[],motionRequests=[];
    const geolocation={getCurrentPosition(success,error){requests.push({success,error});},watchPosition(success,error){watches.push({success,error});return watches.length;},clearWatch(id){cleared.push(id);}};
    Object.defineProperty(navigator,'geolocation',{configurable:true,value:geolocation});
    Object.defineProperty(navigator,'permissions',{configurable:true,value:{query:async()=>({state:'prompt',addEventListener(){},removeEventListener(){}})}});
    class Motion extends Event {static requestPermission(){motionRequests.push({activation:navigator.userActivation?.isActive});return Promise.resolve(options.permission||'granted');}}
    Object.defineProperty(window,'DeviceMotionEvent',{configurable:true,value:options.unsupported?undefined:Motion});
    window.__sensors={watches,cleared,requests,motionRequests,
      emit(z=9.81){const event=new Event('devicemotion');Object.defineProperties(event,{accelerationIncludingGravity:{value:{x:0,y:0,z}},acceleration:{value:{x:0,y:0,z:z-9.81}}});window.dispatchEvent(event);},
      gps(index,step){watches[index].success({coords:{latitude:40+step*10/111195,longitude:-74,accuracy:8,speed:1.25},timestamp:Date.now()});},
      hide(){Object.defineProperty(document,'hidden',{configurable:true,value:true});document.dispatchEvent(new Event('visibilitychange'));},
      show(){Object.defineProperty(document,'hidden',{configurable:true,value:false});document.dispatchEvent(new Event('visibilitychange'));}
    };
  },options);
  await page.goto('http://127.0.0.1:7777/game/monkey-trails.html');
  await page.clock.install();
  return {page,context,errors};
}
async function openTrail(page,{encounterMeters}={}){
  if(encounterMeters!==undefined)await page.evaluate(async({saveName,password,encounterMeters})=>{
    const {Vault}=await import('/game/monkey-trails/storage.mjs');
    const vault=new Vault(()=>{},localStorage,async()=>{throw Error('Offline test');});
    const state=await vault.open(saveName,password,{create:true,mode:'gps'});state.run.nextEncounter=encounterMeters;
    await vault.checkpoint(state,true);vault.retire();
  },{saveName,password,encounterMeters});
  await page.locator(`[data-action="welcome-${encounterMeters===undefined?'new':'continue'}"]`).click();
  await page.locator('#creature-name').fill(saveName);await page.locator('#creature-password').fill(password);
  await page.locator(`#save-form button[value="${encounterMeters===undefined?'create':'open'}"]`).click();
  await page.locator('#game').waitFor({state:'visible'});
}
const feet=async page=>Number((await page.locator('.distance-block strong').innerText()).replace(/[^\d]/g,''));
const steps=async page=>Number((await page.locator('[data-treadmill-steps]').innerText()).replace(/[^\d]/g,''));
const position=page=>page.locator('.terrain-map').evaluate(el=>({east:Number(el.dataset.mapEast),north:Number(el.dataset.mapNorth)}));
const counts=page=>page.evaluate(()=>({gps:__sensors.watches.length,cleared:__sensors.cleared.length,location:__sensors.requests.length,motion:__sensors.motionRequests.length}));
async function walk(page,ms=3000){
  await page.evaluate(()=>{let elapsed=0;__sensors.wave=setInterval(()=>{elapsed+=20;__sensors.emit(9.81+2*Math.sin(2*Math.PI*elapsed/500));},20);});
  await page.clock.runFor(ms);await page.evaluate(()=>clearInterval(__sensors.wave));
}
async function still(page,ms=1200){
  await page.evaluate(()=>{__sensors.wave=setInterval(()=>__sensors.emit(9.81),20);});
  await page.clock.runFor(ms);await page.evaluate(()=>clearInterval(__sensors.wave));
}
async function savedState(page){
  return page.evaluate(async({saveName,password})=>{
    const {Vault}=await import('/game/monkey-trails/storage.mjs');
    const vault=new Vault(()=>{},localStorage,async()=>{throw Error('Offline test');});
    const state=await vault.open(saveName,password,{mode:'gps'});vault.retire();
    return {distance:state.run.distance,phase:state.run.phase,xp:state.run.monkey.xp};
  },{saveName,password});
}
async function checkClean(page,errors){
  assert.deepEqual(errors,[]);
  const size=await page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth}));
  assert.ok(size.scroll<=size.width,`Mobile overflow: ${size.scroll-size.width}px`);
}

(async()=>{
  const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),report=[];
  const run=async(name,options,test)=>{const {page,context,errors}=await setup(browser,options);try{await test(page);await checkClean(page,errors);report.push({scenario:name,errors:0});}finally{await context.close();}};
  try{
    await run('steps move the selected heading, while idle and steering do not',{},async page=>{
      await openTrail(page);assert.equal(await page.locator('#treadmill-mode').isChecked(),false,'GPS remains the default');
      const row=await page.locator('#treadmill-mode').evaluate(el=>{const a=el.closest('label').getBoundingClientRect(),b=document.querySelector('#tracking-button').getBoundingClientRect();return {toggleTop:a.top,buttonTop:b.top};});
      assert.ok(Math.abs(row.toggleTop-row.buttonTop)<30,'The treadmill toggle sits beside Start walking');
      await page.locator('#treadmill-mode').check();assert.equal((await counts(page)).gps,0);assert.equal((await counts(page)).motion,0,'The toggle never starts tracking');
      const start=await position(page),distance=await feet(page),stick=page.locator('[data-treadmill-stick]');
      await stick.focus();await stick.press('ArrowRight');
      assert.match(await page.locator('[data-treadmill-heading]').innerText(),/East/);
      assert.equal(await feet(page),distance,'Selecting a heading does not earn distance');
      await page.locator('#tracking-button').click();
      assert.equal((await counts(page)).gps,0);assert.equal((await counts(page)).location,0);
      assert.equal(await page.evaluate(()=>__sensors.motionRequests[0].activation),true,'Motion permission uses the Start walking gesture');
      await still(page);assert.equal(await feet(page),distance,'A stationary phone earns no distance');
      await stick.focus();await walk(page);const count=await steps(page),after=await position(page);
      assert.equal(await stick.evaluate(el=>el===document.activeElement),true,'Detected steps preserve keyboard focus on the direction stick');
      assert.ok(count>=4&&count<=7,`Walking waveform produces actual estimated steps: ${count}`);
      assert.ok(Math.abs(after.east-start.east-count*.762)<.01,'Every step advances the map by the same estimated stride');
      assert.ok(Math.abs(after.north-start.north)<.01,'Eastward steps preserve north position');
      assert.ok(Math.abs((await feet(page))-count*2.5)<=1,'Distance and step count agree');
      const beforeSteer=await feet(page),box=await stick.boundingBox();
      await page.mouse.move(box.x+box.width/2,box.y+box.height/2);await page.mouse.down();
      await page.mouse.move(box.x+box.width/2,box.y+8);await page.mouse.up();
      assert.match(await page.locator('[data-treadmill-heading]').innerText(),/North/);
      assert.equal(await feet(page),beforeSteer,'Dragging and releasing the stick never grants movement');
      await walk(page,1500);const north=await position(page);
      assert.ok(north.north>after.north,'The released stick keeps its north heading on later steps');
      assert.ok(Math.abs(north.east-after.east)<.01,'Changing to north stops eastward movement');
      await page.locator('#tracking-button').click();const paused=await feet(page);await walk(page,1500);
      assert.equal(await feet(page),paused,'Pause detaches motion input');
      await page.clock.runFor(60000);await page.locator('#tracking-button').click();await still(page,1000);
      assert.equal(await feet(page),paused,'Resume never credits a hidden or paused gap');
      await walk(page,1000);assert.ok(await feet(page)>paused);
    });
    await run('switching tracking sources pauses both and discards stale GPS callbacks',{width:320},async page=>{
      await openTrail(page);await page.locator('#tracking-button').click();assert.equal((await counts(page)).gps,1);
      for(let i=0;i<4;i++){await page.clock.runFor(8000);await page.evaluate(i=>__sensors.gps(0,i),i);}
      const outside=await feet(page);assert.ok(outside>0,'The original GPS tracker still credits accepted walking');
      await page.locator('#treadmill-mode').check();assert.equal((await counts(page)).cleared,1);assert.equal(await page.locator('#tracking-button').innerText(),'Start walking');
      await page.clock.runFor(8000);await page.evaluate(()=>__sensors.gps(0,5));assert.equal(await feet(page),outside,'A stopped GPS callback cannot add treadmill distance');
      await page.locator('#tracking-button').click();await walk(page,1500);const inside=await feet(page);assert.ok(inside>outside);
      await page.locator('#treadmill-mode').uncheck();assert.equal(await page.locator('#tracking-button').innerText(),'Start walking');
      await walk(page,1500);assert.equal(await feet(page),inside,'Motion cannot continue after switching back to GPS');
      assert.equal((await counts(page)).gps,1,'Changing the mode never automatically starts a GPS watch');
      await page.locator('#treadmill-mode').check();await page.reload();await page.locator('#reopen-account').click();await page.locator('#game').waitFor({state:'visible'});
      assert.equal(await page.locator('#treadmill-mode').isChecked(),true,'The chosen tracking mode survives reload');
      assert.equal((await counts(page)).gps,0);assert.equal((await counts(page)).motion,0,'Reload remains paused');
      await page.locator('[data-action="settings"]').click();
      const settingsToggle=page.locator('#dialog-content [data-treadmill-toggle]');
      assert.equal(await settingsToggle.isChecked(),true,'Settings reflects the walking-screen mode');
      await settingsToggle.uncheck();assert.equal(await page.locator('#treadmill-mode').isChecked(),false,'Changing Settings updates the walking-screen toggle');
      await page.locator('.dialog-close').click();await page.locator('#treadmill-mode').check();
      await page.locator('[data-action="settings"]').click();assert.equal(await settingsToggle.isChecked(),true,'Changing the walking-screen toggle updates Settings');
      await page.locator('.dialog-close').click();
      assert.equal((await counts(page)).motion,0,'Settings changes remain paused');
    });
    await run('hidden game and actual combat pause treadmill tracking',{},async page=>{
      await openTrail(page,{encounterMeters:1.524});await page.locator('#treadmill-mode').check();await page.locator('#tracking-button').click();await walk(page,2000);
      await page.locator('[data-action="pending-begin"]').waitFor();
      assert.equal(await page.locator('#tracking-button').innerText(),'Pause walking','A waiting invitation keeps counting steps');
      await page.evaluate(()=>__sensors.hide());await page.evaluate(()=>__sensors.show());
      assert.equal(await page.locator('#tracking-button').innerText(),'Start walking');
      const paused=await feet(page);await walk(page,1000);assert.equal(await feet(page),paused,'Showing the game never resumes automatically');
      await page.locator('#tracking-button').click();await page.locator('[data-action="pending-begin"]').click();
      await page.locator('[data-action="ready"]').waitFor();const combat=await savedState(page);
      assert.equal(combat.phase,'battle');assert.ok(combat.xp>0,'Accepted treadmill steps award regular walking XP');
      await walk(page,1000);
      await page.evaluate(()=>{const event=new Event('pagehide');window.dispatchEvent(event);});
      assert.equal((await savedState(page)).distance,combat.distance,'Motion during combat cannot advance expedition distance');
      assert.equal((await counts(page)).gps,0,'Treadmill encounters do not start location tracking');
    });
    for(const [name,options,pattern] of [
      ['denied motion permission',{permission:'denied'},/denied|allow motion/i],
      ['unsupported motion browser',{unsupported:true},/unavailable|phone browser|motion/i]
    ])await run(name,options,async page=>{
      await openTrail(page);await page.locator('#treadmill-mode').check();await page.locator('#tracking-button').click();
      await page.waitForFunction(()=>document.querySelector('#tracking-button')?.textContent==='Start walking');
      assert.match(await page.locator('#gps-status').innerText(),pattern);
      assert.equal(await feet(page),0);assert.equal((await counts(page)).gps,0,'A failed pedometer never silently starts GPS');
    });
    await run('missing sensor data and practice remain explicit',{},async page=>{
      await openTrail(page);await page.locator('#treadmill-mode').check();await page.locator('#tracking-button').click();
      await page.clock.runFor(6500);assert.equal(await page.locator('#tracking-button').innerText(),'Start walking');
      assert.match(await page.locator('#gps-status').innerText(),/no motion data|sensors/i);assert.equal(await feet(page),0);
    });
    await run('practice controls remain separate',{},async page=>{
      await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});
      assert.equal(await page.locator('#treadmill-mode:visible').count(),0);assert.equal(await page.locator('#tracking-button:visible').count(),0);
      await page.locator('[data-action="map-step"][data-id="E"]').click();assert.ok(await feet(page)>0);
      assert.deepEqual(await counts(page),{gps:0,cleared:0,location:0,motion:0});
    });
    console.log(JSON.stringify(report,null,2));
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
