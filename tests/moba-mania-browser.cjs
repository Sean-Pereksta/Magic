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
const hooks = '\nwindow.__mobaTest={get game(){return game},get renderer(){return renderer},get aim(){return aim},get pinnedGoal(){return pinnedGoal},get profile(){return profile},get controls(){return controls},startMatch,openTalents,openShop,renderShop,refreshHUD,saveCurrent,itemPlan,writeProfile,storage};\n';
const mime = {'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.json':'application/json'};
const missing = [];
function buildOfflinePack() {
  for (const file of ['build-packs.py', 'catalog.json', 'pack-index.json']) fs.copyFileSync(path.join(repo,'desktop-hub/content',file),path.join(temp,file));
  execFileSync(process.env.PYTHON || 'python3',[path.join(temp,'build-packs.py'),repo,repo,'--only','mobamania'],{stdio:'pipe'});
  const pack = path.join(temp,'packs/mobamania');
  const entry = fs.readFileSync(path.join(pack,'index.html'),'utf8');
  assert(!entry.includes('/hub/play-signal.mjs'),'offline pack must not call the web hub');
  for(const module of ['data.mjs','items.mjs','profile.mjs','engine.mjs','hero-skills.mjs','renderer.mjs','ui.mjs','style.css']) assert(fs.existsSync(path.join(pack,'moba-mania',module)),`offline pack missing ${module}`);
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
async function verifyShop(page,device,items) {
  // Earn the first coins through actual combat. Assist XP must never mint gold.
  const earned=await page.evaluate(()=>{
    const g=__mobaTest.game,h=g.player;
    g.cancelPlayerOrder();
    const helper=g.units.find(u=>u.kind==='hero'&&u.team===h.team&&u!==h);
    const bystander=g.units.find(u=>u.kind==='hero'&&u.team===h.team&&u!==h&&u!==helper);
    const total=u=>({level:u.level,xp:u.xp});
    const before=total(h),idleBefore=total(bystander),gold=h.gold;
    const assisted=g.addUnit('footman',1,h.x+70,h.y,{hp:100});
    g.damage(assisted,10,h);g.damage(assisted,200,helper);
    const assistGold=h.gold,after=total(h);
    const finished=g.addUnit('footman',1,h.x+80,h.y,{hp:20});
    g.damage(finished,100,h);__mobaTest.refreshHUD();
    return {gold,assistGold,afterGold:h.gold,before,after,idleBefore,idleAfter:total(bystander),gain:h.lastGoldGain,
      floating:g.texts.some(t=>/GOLD/.test(t.text))};
  });
  assert.equal(earned.assistGold,earned.gold,'an actual assist earns no gold');
  assert(earned.after.level>earned.before.level||earned.after.xp>earned.before.xp,'an actual contributor earns personal XP');
  assert.deepEqual(earned.idleAfter,earned.idleBefore,'uninvolved nearby teammate gets no XP');
  assert(earned.afterGold>earned.gold&&earned.gain.amount>0&&earned.floating,'a final hit earns visible gold');
  await inspectBounds(page,['#shop-button','#shop-gold']);
  if(device.touch)await page.locator('#shop-button').tap();else await page.keyboard.press('i');
  await page.locator('#shop-dialog').waitFor({state:'visible'});
  const paused=await page.evaluate(()=>__mobaTest.game.time);
  await page.waitForTimeout(120);
  assert.equal(await page.evaluate(()=>__mobaTest.game.time),paused,'shopping pauses combat');
  await inspectBounds(page,['#shop-dialog','#shop-balance','#close-shop']);
  for(const tier of [1,2,3]) {
    await page.locator(`[data-shop-tier="${tier}"]`).click();
    assert.equal(await page.locator('#shop-items [data-item-id]').count(),10,`tier ${tier} has ten items`);
    assert.equal(await page.locator('#shop-items .shop-item-heading > canvas').count(),10,`tier ${tier} has ten item icons`);
  }
  const legendary=items.find(i=>i.tier===3);
  const upgrade=items.find(i=>i.id===legendary.requires.find(id=>items.find(i=>i.id===id)?.tier===2));
  const foundation=items.find(i=>i.tier===1&&i.bonuses.hp);
  await page.evaluate(()=>{__mobaTest.game.player.gold=10000;__mobaTest.renderShop();__mobaTest.refreshHUD();});
  await page.locator('[data-shop-tier="2"]').click();
  assert(await page.locator(`[data-buy-item="${upgrade.id}"]`).isDisabled(),'gold alone cannot bypass upgrade prerequisites');
  await page.locator('[data-shop-tier="3"]').click();
  assert(await page.locator(`[data-buy-item="${legendary.id}"]`).isDisabled(),'advanced items require both ingredients');
  const buyIds=[...new Set([foundation.id,...upgrade.requires,...legendary.requires.filter(id=>id!==upgrade.id),upgrade.id,legendary.id])];
  let expectedGold=10000;
  const originalHp=await page.evaluate(()=>__mobaTest.game.player.maxHp);
  for(const id of buyIds) {
    const item=items.find(i=>i.id===id);
    await page.locator(`[data-shop-tier="${item.tier}"]`).click();
    const buy=page.locator(`[data-buy-item="${id}"]`);
    assert(await buy.isEnabled(),item.name+' becomes buyable with its prerequisites');
    await buy.click();expectedGold-=item.cost;
    const state=await page.evaluate(()=>({gold:__mobaTest.game.player.gold,items:[...__mobaTest.game.player.items]}));
    assert.equal(state.gold,expectedGold,'purchase charges the displayed additional cost exactly once');
    assert(state.items.includes(id),'purchase immediately activates '+item.name);
    assert(await buy.isDisabled(),'owned upgrade cannot be bought again');
    if(device.height<550&&device.width>560)await page.waitForFunction(id=>{
      const list=document.getElementById('shop-items').getBoundingClientRect();
      const card=document.querySelector(`#shop-items [data-item-id="${id}"]`);
      const heading=card.querySelector('.shop-item-heading').getBoundingClientRect();
      const action=card.querySelector('[data-buy-item]').getBoundingClientRect();
      return heading.top>=list.top-1&&action.bottom<=list.bottom+1;
    },id,{timeout:3000});
  }
  assert(await page.evaluate(()=>__mobaTest.game.player.maxHp)>originalHp,'buying health changes actual maximum health');
  assert.deepEqual(await page.evaluate(()=>__mobaTest.game.player.items),buyIds,'prerequisites remain active after advanced upgrades');
  assert((await page.locator('#shop-inventory').textContent()).length>0,'owned items appear in inventory');
  if(qaOutput){fs.mkdirSync(qaOutput,{recursive:true});await page.screenshot({path:path.join(qaOutput,device.name+'-shop.png')});}
  await page.evaluate(()=>{__mobaTest.game.player.gold=0;__mobaTest.renderShop();});
  await page.locator('[data-shop-tier="1"]').click();
  const unaffordable=items.find(i=>i.tier===1&&!buyIds.includes(i.id));
  assert(await page.locator(`[data-buy-item="${unaffordable.id}"]`).isDisabled(),'unaffordable purchases are disabled');
  if(device.touch)await page.locator('#close-shop').tap();else await page.keyboard.press('Escape');
  await page.waitForFunction(()=>!document.getElementById('shop-dialog').open&&!__mobaTest.game.paused);
  await page.locator('#shop-button').click();
  assert.deepEqual(await page.evaluate(()=>__mobaTest.game.player.items),buyIds,'closing and reopening retains purchases');
  await page.locator('#close-shop').click();
  await page.waitForFunction(()=>!__mobaTest.game.paused);
  await page.evaluate(()=>{const g=__mobaTest.game;g.gainXP(g.player,1e7);__mobaTest.refreshHUD();});
  assert.equal(await page.evaluate(()=>__mobaTest.game.player.level),18,'level cap is eighteen');
  assert.equal(await page.locator('#xp-fill').evaluate(el=>el.style.width),'100%','max-level XP bar is complete');
}
async function verifyExperience(page,context,device){
  await page.locator('#shop-button').click();
  const recommended=page.locator('[data-recommend-item]').first();
  const goal=await recommended.getAttribute('data-recommend-item');await recommended.click();
  const plan=await page.evaluate(()=>__mobaTest.itemPlan(__mobaTest.game.player,__mobaTest.pinnedGoal));
  assert.equal(await page.evaluate(()=>__mobaTest.pinnedGoal),goal);
  assert.match(await page.locator('#shop-goal-cost').textContent(),new RegExp(String(plan.cost)+' gold'));
  if(plan.next){
    await page.locator('#shop-goal-next').click();
    assert.equal(await page.locator(`[data-item-id="${plan.next.id}"]`).count(),1,'goal opens the next missing ingredient');
    await page.evaluate(cost=>{__mobaTest.game.player.gold=cost;__mobaTest.refreshHUD();},plan.next.cost);
    assert(await page.locator('#shop-ready').isVisible(),'affordable next component lights the HUD badge');
  }
  await page.locator('#shop-items .shop-compare summary').first().click();
  assert.equal(await page.locator('#shop-items .shop-compare[open] table tr').count(),11,'actual stats and all four cooldowns are compared');
  await page.locator('#close-shop').click();
  await page.locator('#pause-button').click();await page.locator('#pause-controls').click();
  await page.locator('#controls-dialog').waitFor({state:'visible'});
  await page.locator('[data-control-preset="large"]').click();
  await page.locator('#control-handed').selectOption('left');
  assert.equal(await page.evaluate(()=>__mobaTest.controls.handed),'left');
  await page.locator('#close-controls').click();await page.locator('#pause-dialog').waitFor({state:'visible'});await page.locator('#resume-match').click();
  if(device.touch)await inspectBounds(page,['#joystick','#bottom-controls','#attack-button','#ability-bar']);
  const map=await page.locator('#minimap').boundingBox();
  if(device.touch){
    const session=await context.newCDPSession(page),touch={x:map.x+map.width*.7,y:map.y+map.height*.3,id:22};
    await session.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[touch]});await page.waitForTimeout(250);
    assert(await page.evaluate(()=>!!__mobaTest.renderer.peek),'holding minimap peeks at that position');
    await session.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await session.detach();
  }else{
    await page.mouse.move(map.x+map.width*.7,map.y+map.height*.3);await page.mouse.down();await page.waitForTimeout(250);
    assert(await page.evaluate(()=>!!__mobaTest.renderer.peek));await page.mouse.up();
  }
  assert.equal(await page.evaluate(()=>__mobaTest.renderer.peek),null,'releasing minimap returns camera to hero');
  assert.equal(await page.locator('#map-dialog').evaluate(el=>el.open),false,'peek release does not also open orders');
  await page.evaluate(()=>{const g=__mobaTest.game;g.cancelPlayerOrder();const beast=g.addUnit('beast',-1,g.player.x+45,g.player.y,{name:'Test beast',hp:100,tier:1,lane:1});g.damage(beast,10000,g.player);__mobaTest.refreshHUD();});
  await page.locator('#beast-choice').waitFor({state:'visible'});await page.locator('[data-beast-lane="0"]').click();
  assert(await page.evaluate(()=>__mobaTest.game.units.some(u=>u.kind==='beast'&&u.team===__mobaTest.game.player.team&&u.lane===0)),'captured beast accepts selected lane');
  await page.locator('#pause-button').click();await page.locator('#save-match').click();
  assert.match(await page.locator('#pause-message').textContent(),/saved/i);
  const snapshot=await page.evaluate(()=>({time:__mobaTest.game.time,hero:__mobaTest.game.player.heroId,level:__mobaTest.game.player.level,items:[...__mobaTest.game.player.items],gold:__mobaTest.game.player.gold,pinned:__mobaTest.pinnedGoal}));
  await page.locator('#quit-match').click();await page.locator('#menu-screen').waitFor({state:'visible'});
  await page.locator('#start-match').click();await page.locator('#replace-save-dialog').waitFor({state:'visible'});await page.locator('#keep-saved-match').click();
  assert(await page.locator('#resume-saved-match').isVisible(),'declining replacement keeps the saved match');
  await page.reload();await page.waitForFunction(()=>!!window.__mobaTest);await page.locator('#resume-saved-match').click();
  await page.locator('#pause-dialog').waitFor({state:'visible'});await page.waitForTimeout(150);
  const restored=await page.evaluate(()=>({time:__mobaTest.game.time,hero:__mobaTest.game.player.heroId,level:__mobaTest.game.player.level,items:[...__mobaTest.game.player.items],gold:__mobaTest.game.player.gold,pinned:__mobaTest.pinnedGoal}));
  assert.deepEqual(restored,snapshot,'reload restores complete personal progression and build target');
  assert.equal(await page.evaluate(()=>__mobaTest.game.paused),true,'resumed match waits for explicit player input');
  assert.equal(await page.evaluate(()=>__mobaTest.controls.handed),'left','control preferences survive reload');
  await page.locator('#resume-match').click();
  await page.locator('#pause-button').click();await page.locator('#pause-controls').click();await page.locator('[data-control-preset="default"]').click();await page.locator('#control-handed').selectOption('right');await page.locator('#close-controls').click();await page.locator('#resume-match').click();
}
async function main(){
  const {ITEMS}=await import('../game/moba-mania/items.mjs');
  const {HEROES}=await import('../game/moba-mania/data.mjs');
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
      await inspectBounds(page,['#health-fill','#mana-fill','#xp-fill','#ability-bar','#pause-button','#talent-button','#shop-button']);
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
      assert.equal(await page.locator('.talent-option canvas').count(),HEROES[device.hero].talents.reduce((n,t)=>n+t.choices.length,0),'every talent choice has an icon');
      const talent=page.locator('[data-talent-choice][data-tier="3"]').first();
      const choice=await talent.getAttribute('data-talent-choice');await talent.click();
      assert.equal(await page.evaluate(()=>__mobaTest.game.player.talents[3]),choice,'talent choice reaches simulation');
      if(await page.locator('#close-talents').isVisible())await page.locator('#close-talents').click();
      await page.waitForFunction(()=>__mobaTest.game.paused===false,{},{timeout:3000});
      assert.equal(await page.evaluate(()=>__mobaTest.game.paused),false,'closing tree resumes');
      await verifyShop(page,device,ITEMS);
      await verifyExperience(page,context,device);
      if(qaOutput){fs.mkdirSync(qaOutput,{recursive:true});await page.screenshot({path:path.join(qaOutput,device.name+'.png')});}
      await finishMatch(page);
      assert.equal(await page.locator('#end-stats > div').count(),16,'all personal contribution metrics are shown');
      assert(await page.locator('#end-badges > span').count()>0,'match badges are visible');
      assert.match(await page.locator('#end-mastery').textContent(),/mastery XP/);
      assert.equal(await page.evaluate(()=>localStorage.getItem('moba-mania.match.v1')),null,'completed match clears its save slot');
      await page.locator('#play-again').click();
      await page.locator('#hud').waitFor({state:'visible'});
      assert.equal(await page.evaluate(()=>__mobaTest.game.winner),null,'replay starts fresh match');
      assert.equal(await page.evaluate(()=>__mobaTest.game.player.level),1,'replay resets progression');
      assert.deepEqual(await page.evaluate(()=>({gold:__mobaTest.game.player.gold,items:__mobaTest.game.player.items})),{gold:0,items:[]},'replay resets gold and inventory');
      assert.deepEqual(errors,[],device.name+' must not emit runtime errors');
      console.log(JSON.stringify({device:device.name,hero:device.hero,move:true,camera:true,nearestAttack:true,enemyFocus:true,autoRetarget:true,moveCancelsAttack:true,talentIcons:HEROES[device.hero].talents.reduce((n,t)=>n+t.choices.length,0),saveResume:true,pinnedBuild:true,controlSettings:true,minimapPeek:true,mastery:true,shopItems:30,prerequisites:true,purchases:true,personalRewards:true,maxLevel:18,replay:true,errors:0}));
      await context.close();
    }
    const context=await browser.newContext({viewport:{width:1280,height:800}}),page=await context.newPage(),errors=[];
    page.on('pageerror',e=>errors.push(e.message));
    await page.goto(origin+'/offline/index.html');await page.waitForFunction(()=>!!window.__mobaTest);
    for(const hero of ['druid','angel','bulwark','ember','seraph']){
      await page.locator(`#hero-grid [data-hero="${hero}"]`).click();await page.locator('#start-match').click();
      await page.locator('#hud').waitFor({state:'visible'});assert.equal(await page.evaluate(()=>__mobaTest.game.player.heroId),hero);
      assert.equal(await page.locator('#ability-bar canvas').count(),4,'every hero has four skill icons');
      await page.locator('#shop-button').click();await page.locator('#shop-dialog').waitFor({state:'visible'});
      assert.equal(await page.locator('#shop-items canvas').count(),10,'offline item icons load without extra assets');
      await page.locator('#close-shop').click();await page.waitForFunction(()=>!__mobaTest.game.paused);
      if(hero==='druid'){await page.locator('#pause-button').click();await page.locator('#quit-match').click();await page.locator('#resume-saved-match').click();await page.locator('#pause-dialog').waitFor({state:'visible'});assert.equal(await page.evaluate(()=>__mobaTest.game.paused),true);await page.locator('#resume-match').click();}
      await finishMatch(page);await page.locator('#change-hero').click();await page.locator('#menu-screen').waitFor({state:'visible'});
    }
    assert.deepEqual(errors,[]);await context.close();assert.deepEqual(missing,[],'no missing static modules/resources');
    console.log(JSON.stringify({offlinePack:true,otherHeroes:['druid','angel','bulwark','ember','seraph'],missingResources:0,errors:0}));
  }finally{await browser.close();await new Promise(r=>server.close(r));}
}
main().catch(e=>{console.error(e);process.exitCode=1;server.close();}).finally(()=>fs.rmSync(temp,{recursive:true,force:true}));

