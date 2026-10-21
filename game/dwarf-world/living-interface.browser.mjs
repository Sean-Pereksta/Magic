// Run against a local static server: DWARFWORLD_URL=http://localhost:8768/game/DwarfWorld.html
// Requires the repository's optional Playwright development dependency and Chrome.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdirSync,readFileSync} from 'node:fs';
import path from 'node:path';
const require=createRequire(import.meta.url);
const {chromium}=require(process.env.PLAYWRIGHT_MODULE_PATH||'playwright');
const target=process.env.DWARFWORLD_URL||'http://127.0.0.1:8768/game/DwarfWorld.html';
const output=path.resolve(process.env.DWARFWORLD_SCREENSHOTS||'work/interface-screenshots');
mkdirSync(output,{recursive:true});
const browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHANNEL||'chrome',headless:true});
const checks=[];
try {
  for(const mobile of [false,true]) {
    const context=await browser.newContext(mobile?{viewport:{width:390,height:844},isMobile:true,hasTouch:true,deviceScaleFactor:1}:{viewport:{width:1440,height:900}});
    if(process.env.DWARFWORLD_OFFLINE==='1')await context.route(new URL(target).origin+'/**',route=>{
      const local=path.resolve(new URL('../..',import.meta.url).pathname.replace(/^\/(\w:)/,'$1'),'.'+new URL(route.request().url()).pathname);
      try{return route.fulfill({status:200,contentType:local.endsWith('.html')?'text/html':local.endsWith('.js')?'text/javascript':local.endsWith('.png')?'image/png':local.endsWith('.webp')?'image/webp':'text/plain',body:readFileSync(local)});}catch{return route.fulfill({status:404,body:'Not found'});}
    });
    const page=await context.newPage(),errors=[];
    page.on('pageerror',e=>{errors.push(e.message);console.error('PAGE ERROR:',e.message);});
    page.on('console',m=>{if(m.type()==='error')console.error('BROWSER:',m.text());});
    await page.goto(target);
    await page.getByRole('button',{name:/Start New World/}).click();
    const frame=page.frameLocator('#gameFrame');
    try{await frame.locator('#lmDock').waitFor({state:'visible',timeout:15000});}catch(error){console.error('SHELL STATE:',await page.locator('body').innerText(),page.frames().map(f=>f.url()));console.error('FRAME STATE:',await frame.locator('body').innerText().catch(()=>''));throw error;}
    const game=page.frames().find(f=>f!==page.mainFrame());
    await game.waitForFunction(()=>window.DwarfWorldLiving?.dwarves.length>0);
    assert.equal(await frame.locator('#lmPanel').isVisible(),false,'No permanent panel on initial map');
    assert.equal(await frame.locator('#panelLeft').isVisible(),false);
    assert.equal(await frame.locator('#panelInv').isVisible(),false);
    assert.equal(await frame.locator('#lmDock button:visible').count(),mobile?5:7);
    assert.equal(await page.locator('body').evaluate(el=>el.scrollWidth>innerWidth),false,'Shell fits viewport');
    assert.equal(await game.evaluate(()=>document.body.scrollWidth>innerWidth),false,'Game fits viewport');
    const before=await game.evaluate(()=>DwarfWorldLiving.dwarves.map(d=>[d.x,d.y]));
    await page.waitForTimeout(1500);
    const after=await game.evaluate(()=>DwarfWorldLiving.dwarves.map(d=>[d.x,d.y]));
    assert.notDeepEqual(before,after,'Dwarves continue moving after boot');
    await page.screenshot({path:path.join(output,mobile?'mobile-map.png':'desktop-map.png')});
    await frame.locator('#lmDock [data-lm-open="build"]').click();
    assert.equal(await frame.locator('#lmPanel').isVisible(),true);
    assert.equal(await page.locator('#cloudDock').isVisible(),false,'Cloud dock yields to contextual panel');
    assert.equal(await game.evaluate(()=>Array.from(document.querySelectorAll('[data-lm-view]')).filter(e=>!e.hidden).length),1);
    const bounds=await frame.locator('#lmPanel').boundingBox();
    assert.ok(bounds.width<=(mobile?390:365),'One narrow panel');
    if(mobile)assert.ok(bounds.height<=844*.55,'Ordinary mobile sheet leaves the world visible');
    await page.screenshot({path:path.join(output,mobile?'mobile-build.png':'desktop-build.png')});
    await frame.locator('#lmRoomType').selectOption('garden');
    await frame.locator('#lmRoomDesignate').click();
    assert.equal(await frame.locator('#lmPanel').isVisible(),false);
    assert.equal(await frame.locator('#lmPlacement').isVisible(),true);
    const spot=await game.evaluate(()=>{const api=DwarfWorldLiving,f=api.fortress;for(let y=f.y-10;y<=f.y+10;y++)for(let x=f.x-10;x<=f.x+10;x++)if(api.preview('garden',x,y,3,3).ok)return{x,y};return null;});
    assert.ok(spot,'Starting fortress contains connected room space');
    const screen=await game.evaluate(({x,y})=>{const c=DwarfWorldInterface.camera;return{x:innerWidth/2+(x+1-c.x)*c.tileSize,y:innerHeight/2+(y+1-c.y)*c.tileSize};},spot);
    if(mobile)await page.touchscreen.tap(screen.x,screen.y);else await page.mouse.click(screen.x,screen.y);
    assert.equal(await game.evaluate(()=>DwarfWorldLiving.state.rooms.length),1,'Pointer/touch room designation reaches real simulation');
    assert.equal(await frame.locator('#lmPlacement').isVisible(),false,'Successful room placement exits tool mode');
    await frame.locator('#lmDock [data-lm-open="dwarves"]').click();
    await frame.locator('[data-dwarf-id]').first().click();
    await frame.locator('#lmProfession').selectOption('farmer');
    assert.equal(await game.evaluate(()=>DwarfWorldLiving.dwarves[0].living.profession),'farmer');
    assert.equal(await frame.locator('#slotWeapon').isVisible(),true,'Existing equipment controls remain accessible');
    await frame.locator('#lmDock [data-lm-open="inventory"]').click();
    assert.equal(await frame.locator('#invSearch').isVisible(),true);
    await frame.locator('#lmDock [data-lm-open="more"]').click();
    await frame.locator('[data-lm-view="more"] [data-lm-open="forge"]').click();
    assert.equal(await frame.locator('#tab-forge').isVisible(),true);
    await frame.locator('#lmDock [data-lm-open="military"]').click();
    await frame.locator('#lmReadiness').selectOption('alert');
    assert.equal(await game.evaluate(()=>DwarfWorldLiving.state.readiness),'alert');
    await frame.locator('#lmDock [data-lm-open="more"]').click();
    await frame.locator('[data-lm-view="more"] [data-lm-open="economy"]').click();
    await frame.locator('[data-lm-priority="food"]').selectOption('3');
    assert.equal(await game.evaluate(()=>DwarfWorldLiving.state.priorities.food),3);
    const placedInfra=await game.evaluate(()=>{const api=DwarfWorldLiving,f=api.fortress;for(let y=f.y-8;y<f.y+8;y++)for(let x=f.x-8;x<f.x+8;x++){if(api.dwarves.some(d=>Math.hypot(d.x-x,d.y-y)<1.5))continue;const r=api.placeInfrastructure('road',x,y);if(r.ok){DwarfWorldInterface.inspect(x,y);return r.infrastructure.id;}}return null;});
    assert.ok(placedInfra,'Can place road beside the fortress');
    await frame.locator('[data-lm-remove-infra]').click();
    assert.equal(await game.evaluate(id=>DwarfWorldLiving.state.infrastructure.some(i=>i.id===id),placedInfra),false,'Dismantle UI removes infrastructure');
    await frame.locator('#lmDock [data-lm-open="more"]').click();
    await frame.locator('#btnSave').click();
    await game.waitForFunction(()=>document.querySelector('#toast')?.textContent.startsWith('Saved locally.'),null,{timeout:20000});
    const savedRooms=await game.evaluate(()=>DwarfWorldLiving.state.rooms.length);
    await game.evaluate(()=>DwarfWorldLiving.setPriority('food',0));
    await frame.locator('#btnLoad').click();
    await game.waitForFunction(()=>DwarfWorldLiving.state.priorities.food===3,null,{timeout:20000});
    assert.equal(await game.evaluate(()=>DwarfWorldLiving.state.rooms.length),savedRooms);
    await frame.locator('#lmPanelClose').click();
    if(mobile){
      // Long press uses the existing pan/pinch input, and must not synthesize an
      // extra click after opening details.
      await page.touchscreen.tap(180,300);
      await game.evaluate(()=>{const c=document.querySelector('#game');const t=new Touch({identifier:7,target:c,clientX:195,clientY:422});c.dispatchEvent(new TouchEvent('touchstart',{bubbles:true,cancelable:true,touches:[t],targetTouches:[t],changedTouches:[t]}));});
      await page.waitForTimeout(650);
      assert.equal(await frame.locator('#lmPanel').isVisible(),true,'Long press opens contextual details');
      await game.evaluate(()=>document.querySelector('#game').dispatchEvent(new TouchEvent('touchend',{bubbles:true,cancelable:true,touches:[],changedTouches:[]})));
    }
    assert.deepEqual(errors,[],`${mobile?'Mobile':'Desktop'} page errors`);
    checks.push({viewport:mobile?'390×844 touch':'1440×900 desktop',roomDesignated:true,professionChanged:true,prioritySavedAndRestored:true,errors});
    await context.close();
  }
  console.log(JSON.stringify({ok:true,checks,screenshots:output},null,2));
} finally {await browser.close();}
