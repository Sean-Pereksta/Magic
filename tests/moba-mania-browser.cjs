'use strict';
/* Real browser checks. Inspection hooks exist only in this test server's response. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const {execFileSync} = require('node:child_process');
const {chromium} = require('playwright');
const repo = path.resolve(__dirname, '..');
const qaOutput = process.env.MOBA_QA_OUTPUT;
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'moba-pack-test-'));
const hooks = '\nwindow.__mobaTest={get game(){return game},get renderer(){return renderer},get aim(){return aim},startMatch,openTalents,refreshHUD};\n';
const mime = {'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.json':'application/json'};
const missing = [];
function buildOfflinePack() {
  for (const file of ['build-packs.py', 'catalog.json', 'pack-index.json']) fs.copyFileSync(path.join(repo,'desktop-hub/content',file),path.join(temp,file));
  execFileSync(process.env.PYTHON || 'python3',[path.join(temp,'build-packs.py'),repo,repo,'--only','mobamania'],{stdio:'pipe'});
  const pack = path.join(temp,'packs/mobamania');
  const entry = fs.readFileSync(path.join(pack,'index.html'),'utf8');
  assert(!entry.includes('/hub/play-signal.mjs'),'offline pack must not call the web hub');
  for(const module of ['data.mjs','engine.mjs','hero-skills.mjs','renderer.mjs','ui.mjs','style.css']) assert(fs.existsSync(path.join(pack,'moba-mania',module)),`offline pack missing ${module}`);
  assert(!fs.existsSync(path.join(pack,'moba-mania/CONTRACT.md')),'implementation coordination notes are not shipped');
  return pack;
}
const server = http.createServer((req,res)=>{
  try {
    const pathname = decodeURIComponent(new URL(req.url,'http://localhost').pathname);
    if(pathname==='/hub/play-signal.mjs'){res.writeHead(200,{'Content-Type':'text/javascript'});res.end('export {};');return;}
    if(pathname==='/favicon.ico'){res.writeHead(204);res.end();return;}
    const base = pathname.startsWith('/offline/') ? path.join(temp,'packs/mobamania') : repo;
    const relative = pathname.startsWith('/offline/') ? pathname.slice('/offline'.length) : pathname;
    const file = path.resolve(base,'.'+relative);
    if(!file.startsWith(base+path.sep))throw Error('outside test root');
    let data = fs.readFileSync(file);
    if(path.basename(file)==='ui.mjs')data=Buffer.from(data.toString()+hooks);
    res.writeHead(200,{'Content-Type':mime[path.extname(file)]||'application/octet-stream','Cache-Control':'no-store'});res.end(data);
  } catch(e) {missing.push(req.url);res.writeHead(404);res.end('Not found');}
});
async function inspectBounds(page, selectors) {
  const report = await page.evaluate(selectors=>selectors.map(selector=>{
    const el=document.querySelector(selector),r=el?.getBoundingClientRect(),style=el&&getComputedStyle(el);
    return {selector,visible:!!el&&style.display!=='none'&&style.visibility!=='hidden'&&r.width>=0&&r.height>0,x:r?.x,y:r?.y,w:r?.width,h:r?.height,vw:innerWidth,vh:innerHeight};
  }),selectors);
  for(const r of report){assert(r.visible,r.selector+' should be visible');assert(r.x>=-1&&r.y>=-1&&r.x+r.w<=r.vw+1&&r.y+r.h<=r.vh+1,r.selector+' escapes viewport: '+JSON.stringify(r));}
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false,'no horizontal page overflow');
}
async function finishMatch(page) {
  await page.evaluate(()=>{
    const g=__mobaTest.game;
    for(const u of g.units)if(u.kind==='tower'&&u.team===1){u.alive=false;u.hp=0;}
    const core=g.units.find(u=>u.kind==='core'&&u.team===1);
    g.damage(core,1e7,g.player,'test');
  });
  await page.locator('#end-dialog').waitFor({state:'visible'});
  assert.equal(await page.evaluate(()=>__mobaTest.game.winner),0,'destroying enemy crystal wins');
}
async function clickBattlefield(page,device,unitId=null,offset=null) {
  const point=await page.evaluate(({unitId,offset})=>{
    const g=__mobaTest.game;
    const target=unitId?g.units.find(u=>u.id===unitId):{x:g.player.x+offset.x,y:g.player.y+offset.y,r:0};
    return __mobaTest.renderer.worldToScreen(target.x,target.y-target.r*.45);
  },{unitId,offset});
  if(device.touch)await page.touchscreen.tap(point.x,point.y);
  else await page.mouse.click(point.x,point.y);
}
async function verifyAttackOrders(page,context,device) {
  const targets=await page.evaluate(()=>{
    const g=__mobaTest.game,h=g.player;
    g.cancelPlayerOrder();g.waveIn=g.beastIn=10000;
    for(const u of g.units)if(u!==h){u.status.stun={remaining:10000};if(u.kind==='hero')u.bot=false;}
    Object.assign(h,{x:1600,y:1100,attackTimer:0,status:{}});
    const near=g.addUnit('footman',1,h.x+90,h.y+25,{name:'Near target',hp:5000,speed:0,damage:0,status:{stun:{remaining:10000}}});
    const far=g.addUnit('footman',1,h.x+160,h.y-30,{name:'Focus target',hp:5000,speed:0,damage:0,status:{stun:{remaining:10000}}});
    g.rebuildGrid();return {near:near.id,far:far.id};
  });
  await page.waitForTimeout(300);
  // Aim away from both targets; basic attacks must be selected by distance.
  await page.mouse.move(device.width*.15,device.height*.45);
  if(device.touch)await page.locator('#attack-button').tap();
  else await page.keyboard.press('Space');
  await page.waitForFunction(id=>__mobaTest.game.player.targetId===id,targets.near);
  await page.waitForFunction(id=>__mobaTest.game.units.find(u=>u.id===id).hp<5000,targets.near);
  assert(await page.evaluate(()=>!!__mobaTest.game.attackOrder),'attack persists after the control is released');
  assert.equal(await page.locator('#attack-button').getAttribute('aria-pressed'),'true');
  await clickBattlefield(page,device,targets.far);
  await page.waitForTimeout(200);
  assert.equal(await page.evaluate(()=>__mobaTest.game.player.targetId),targets.far,'enemy click/tap overrides a nearer enemy');
  await page.evaluate(id=>{const g=__mobaTest.game;g.damage(g.units.find(u=>u.id===id),1e6,g.player);},targets.far);
  await page.waitForFunction(id=>__mobaTest.game.player.targetId===id,targets.near);
  assert(await page.evaluate(()=>!!__mobaTest.game.attackOrder),'defeating a focused enemy continues the attack order');
  if(device.touch){
    const box=await page.locator('#joystick').boundingBox(),session=await context.newCDPSession(page);
    const touch={x:box.x+box.width/2,y:box.y+box.height/2,id:11};
    await session.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[touch]});
    await session.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{...touch,x:touch.x-30}]});
    await page.waitForTimeout(100);
    await session.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await session.detach();
  }else{await page.keyboard.down('a');await page.waitForTimeout(100);await page.keyboard.up('a');}
  await page.waitForTimeout(150);
  assert.equal(await page.evaluate(()=>__mobaTest.game.attackOrder),null,'manual movement cancels the attack order after release');
  assert.equal(await page.evaluate(()=>__mobaTest.game.player.targetId),null,'manual movement clears focus');
  await clickBattlefield(page,device,targets.near);
  await page.waitForFunction(id=>__mobaTest.game.player.targetId===id,targets.near);
  await clickBattlefield(page,device,null,{x:-130,y:60});
  await page.waitForFunction(()=>!__mobaTest.game.attackOrder&&!!__mobaTest.game.moveTarget);
  assert.equal(await page.evaluate(()=>__mobaTest.game.player.targetId),null,'ground tap/click replaces focused attack with movement');
  await page.evaluate(ids=>{
    const g=__mobaTest.game;g.cancelPlayerOrder();
    g.units=g.units.filter(u=>!ids.includes(u.id));
    for(const u of g.units){delete u.status.stun;if(u.kind==='hero')u.bot=!u.isPlayer;}
    g.waveIn=19;g.beastIn=55;g.rebuildGrid();
  },[targets.near,targets.far]);
}
async function main(){
  buildOfflinePack();
  await new Promise(r=>server.listen(0,'127.0.0.1',r));
  const origin='http://127.0.0.1:'+server.address().port;
  const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--no-sandbox']});
  try{
    for(const device of [
      {name:'desktop',width:1280,height:800,touch:false,hero:'arc'},
      {name:'phone-portrait',width:390,height:844,touch:true,hero:'dragon'},
      {name:'phone-landscape',width:844,height:390,touch:true,hero:'ranger'}
    ]){
      const context=await browser.newContext({viewport:{width:device.width,height:device.height},hasTouch:device.touch,isMobile:device.touch});
      const page=await context.newPage(),errors=[];page.setDefaultTimeout(15000);
      page.on('pageerror',e=>errors.push(e.message));
      await page.goto(origin+'/game/CatLanes.html');
      await page.waitForFunction(()=>!!window.__mobaTest);
      assert.equal(await page.locator('#hero-grid [data-hero]').count(),8,'all eight heroes are selectable');
      assert.equal(await page.locator('#hero-grid [data-hero="mouse"]').count(),0,'mouse removed from roster');
      await page.locator(`#hero-grid [data-hero="${device.hero}"]`).click();
      await page.locator('#difficulty').selectOption('hard');
      await page.locator('#start-match').click();
      await page.locator('#hud').waitFor({state:'visible'});
      const setup=await page.evaluate(()=>({hero:__mobaTest.game.player.heroId,count:__mobaTest.game.units.filter(u=>u.kind==='hero').length}));
      assert.equal(setup.hero,device.hero);assert.equal(setup.count,6);
      await inspectBounds(page,['#health-fill','#mana-fill','#xp-fill','#ability-bar','#pause-button','#talent-button']);
      await page.evaluate(()=>{const g=__mobaTest.game;g.player.x=1600;g.player.y=1100;});
      await page.waitForTimeout(350);
      const before=await page.evaluate(()=>({x:__mobaTest.game.player.x,camera:__mobaTest.renderer.camera.x}));
      if(device.touch){
        await inspectBounds(page,['#joystick','#attack-button']);
        const box=await page.locator('#joystick').boundingBox(),session=await context.newCDPSession(page);
        const touch={x:box.x+box.width/2,y:box.y+box.height/2,id:1};
        await session.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[touch]});
        await session.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{...touch,x:touch.x+38}]});
        await page.waitForTimeout(700);
        await session.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});
        await session.detach();
      }else{
        await page.keyboard.down('d');await page.waitForTimeout(700);await page.keyboard.up('d');
      }
      const after=await page.evaluate(()=>({x:__mobaTest.game.player.x,camera:__mobaTest.renderer.camera.x}));
      assert(after.x>before.x+30,'movement input advances the hero');
      assert(after.camera>before.camera+15,'camera follows hero movement');
      await verifyAttackOrders(page,context,device);
      const manaBefore=await page.evaluate(()=>__mobaTest.game.player.mana);
      if(device.touch){
        const box=await page.locator('#ability-bar [data-slot="0"]').boundingBox(),attackBox=await page.locator('#attack-button').boundingBox(),session=await context.newCDPSession(page);
        const attack={x:attackBox.x+attackBox.width/2,y:attackBox.y+attackBox.height/2,id:3};
        const touch={x:box.x+box.width/2,y:box.y+box.height/2,id:4};
        await page.evaluate(()=>{const g=__mobaTest.game,enemy=g.units.find(u=>u.kind==='hero'&&u.team===1);Object.assign(enemy,{x:g.player.x+100,y:g.player.y,bot:false});g.rebuildGrid();});
        await session.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[attack]});
        await session.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[attack,touch]});
        await session.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[attack,{...touch,x:touch.x-65,y:touch.y-55}]});
        await page.waitForTimeout(120);
        const aimed=await page.evaluate(()=>({x:__mobaTest.aim.x-__mobaTest.game.player.x,y:__mobaTest.aim.y-__mobaTest.game.player.y}));
        assert(aimed.x< -20&&aimed.y< -20,'holding ATTACK must not overwrite a manually aimed spell');
        await session.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await session.detach();
        const shot=await page.evaluate(()=>__mobaTest.game.projectiles.filter(p=>p.ownerId===__mobaTest.game.player.id&&!p.basic).at(-1)?.angle);
        assert(Number.isFinite(shot)&&Math.cos(shot)<0&&Math.sin(shot)<0,'touch release fires in the chosen direction');
      }else{await page.mouse.move(device.width*.65,device.height*.45);await page.keyboard.press('q');}
      await page.waitForTimeout(80);
      assert(await page.evaluate(()=>__mobaTest.game.player.cooldowns[0]>0),'keyboard/touch ability input casts');
      assert(await page.evaluate(()=>__mobaTest.game.player.mana)<manaBefore,'cast spends mana');
      await page.locator('#pause-button').click();
      await page.locator('#pause-dialog').waitFor({state:'visible'});
      const paused=await page.evaluate(()=>__mobaTest.game.time);await page.waitForTimeout(120);
      assert.equal(await page.evaluate(()=>__mobaTest.game.time),paused,'pause freezes simulation');
      await page.locator('#resume-match').click();
      await page.evaluate(()=>{const g=__mobaTest.game;while(g.player.level<3)g.gainXP(g.player,g.player.xpNext);__mobaTest.refreshHUD();});
      await page.locator('#talent-button').click();
      await page.locator('#talent-dialog').waitFor({state:'visible'});
      assert.equal(await page.evaluate(()=>__mobaTest.game.paused),true,'talent choices pause solo match');
      assert.equal(await page.locator('[data-talent-choice][data-tier="3"]').count(),3,'every tier offers three talent paths');
      assert.equal(await page.locator('.talent-option canvas').count(),12,'all twelve talent choices have icons');
      const talent=page.locator('[data-talent-choice][data-tier="3"]').first();
      const choice=await talent.getAttribute('data-talent-choice');await talent.click();
      assert.equal(await page.evaluate(()=>__mobaTest.game.player.talents[3]),choice,'talent choice reaches simulation');
      if(await page.locator('#close-talents').isVisible())await page.locator('#close-talents').click();
      await page.waitForFunction(()=>__mobaTest.game.paused===false,{},{timeout:3000});
      assert.equal(await page.evaluate(()=>__mobaTest.game.paused),false,'closing tree resumes');
      if(qaOutput){fs.mkdirSync(qaOutput,{recursive:true});await page.screenshot({path:path.join(qaOutput,device.name+'.png')});}
      await finishMatch(page);
      await page.locator('#play-again').click();
      await page.locator('#hud').waitFor({state:'visible'});
      assert.equal(await page.evaluate(()=>__mobaTest.game.winner),null,'replay starts fresh match');
      assert.equal(await page.evaluate(()=>__mobaTest.game.player.level),1,'replay resets progression');
      assert.deepEqual(errors,[],device.name+' must not emit runtime errors');
      console.log(JSON.stringify({device:device.name,hero:device.hero,move:true,camera:true,nearestAttack:true,enemyFocus:true,autoRetarget:true,moveCancelsAttack:true,talentIcons:12,replay:true,errors:0}));
      await context.close();
    }
    const context=await browser.newContext({viewport:{width:1280,height:800}}),page=await context.newPage(),errors=[];
    page.on('pageerror',e=>errors.push(e.message));
    await page.goto(origin+'/offline/index.html');await page.waitForFunction(()=>!!window.__mobaTest);
    for(const hero of ['druid','angel','bulwark','ember','seraph']){
      await page.locator(`#hero-grid [data-hero="${hero}"]`).click();await page.locator('#start-match').click();
      await page.locator('#hud').waitFor({state:'visible'});assert.equal(await page.evaluate(()=>__mobaTest.game.player.heroId),hero);
      assert.equal(await page.locator('#ability-bar canvas').count(),4,'every hero has four skill icons');
      await finishMatch(page);await page.locator('#change-hero').click();await page.locator('#menu-screen').waitFor({state:'visible'});
    }
    assert.deepEqual(errors,[]);await context.close();assert.deepEqual(missing,[],'no missing static modules/resources');
    console.log(JSON.stringify({offlinePack:true,otherHeroes:['druid','angel','bulwark','ember','seraph'],missingResources:0,errors:0}));
  }finally{await browser.close();await new Promise(r=>server.close(r));}
}
main().catch(e=>{console.error(e);process.exitCode=1;server.close();}).finally(()=>fs.rmSync(temp,{recursive:true,force:true}));
