// Playwright serves repository/local artwork fixtures through request interception.
// No server is required; all instrumentation stays in a disposable browser context.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
const {chromium}=require('playwright');
const base=process.env.DWARF_TEST_URL||'http://127.0.0.1:8768/game/DwarfWorld.html';
const repository=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..');
const assetDirectory=process.env.DWARF_ART_DIR||path.resolve(repository,'../../outputs/dwarf-world-assets');
const screenshotDirectory=process.env.DWARF_STRESS_OUTPUT_DIR;
const diagnostic=`
window.__livingStress={start:()=>{
  gold=100000;
  for(let y=fortress.y-20;y<=fortress.y+20;y++)for(let x=fortress.x-20;x<=fortress.x+20;x++){tType[idx(x,y)]=TILE_EMPTY;tHp[idx(x,y)]=0;tSeen[idx(x,y)]=1;}
  const types=['garden','garden','garden','kitchen','kitchen','kitchen','smelter','smelter','smelter','smithy','smithy','smithy','stoneworks','stoneworks','dining','dining','armory','granary','quarters','medical'];
  for(let i=0;i<types.length;i++){
    const result=lmDesignate(types[i],fortress.x-18+(i%7)*5,fortress.y-18+Math.floor(i/7)*5,3,3);
    if(!result.ok)throw Error('Stress room: '+result.reason);
    if(i<18){result.room.active=true;result.room.progress=1;}
  }
  lm.stores[0].inventory.stone=500;lm.stores[0].inventory.ore=500;lm.stores[0].inventory.meal=100;
  lmEngine.deposit(fortress.x-8,fortress.y+8,'stone',200);lmEngine.deposit(fortress.x+7,fortress.y+9,'ore',200);
  for(let y=fortress.y-8;y<=fortress.y+8;y++){tType[idx(fortress.x-21,y)]=TILE_ROCK;tHp[idx(fortress.x-21,y)]=120;tSeen[idx(fortress.x-21,y)]=1;}
  lmDigArea(fortress.x-21,fortress.y-8,1,17);
  while(dwarves.length<100){const d=makeDwarf();applyDwarfStats(d);dwarves.push(d);}
  const professions=[...Array(30).fill('miner'),...Array(20).fill('hauler'),...Array(10).fill('farmer'),...Array(10).fill('cook'),...Array(10).fill('smith'),...Array(10).fill('mason'),...Array(10).fill('soldier')];
  for(let i=0;i<dwarves.length;i++){
    const d=dwarves[i];d.x=fortress.x-12+(i%12);d.y=fortress.y+2+Math.floor(i/12);
    lmSetProfession(d.id,professions[i]);d.weapon=null;d.pickaxe=null;
    if(professions[i]==='soldier'){d.weapon=mkItemWeapon('rare');d.x=fortress.x+14;d.y=fortress.y+(i%10)-5;}
    if(professions[i]==='miner')d.pickaxe=mkItemPickaxe('rare');
    applyDwarfStats(d);d.hp=d.maxHp=100000;d.think=0;d.path=null;
  }
  for(let i=0;i<100;i++){const e=makeEnemy(fortress.x+15+i%5,fortress.y+(Math.floor(i/5)%17)-8,20);e.dmg=0;e.hp=e.maxHp=100000;enemies.push(e);}
  // Keep the synthetic workload constant even when percentage-based specials fire.
  const originalRemoveDead=removeDead;removeDead=function(){for(const d of dwarves)d.hp=Math.max(d.hp,d.maxHp);return originalRemoveDead();};
  resetLivingCaches();refreshVisibility(0);lmEngine.reindex();
  const sim=[],render=[],frames=[],renderCosts=[];let lastFrame=performance.now(),frameCosts={};
  if(${process.env.DWARF_PROFILE_RENDER==='1'}){
    const measure=(fn,key)=>function(...args){const start=performance.now();try{return fn.apply(this,args)}finally{frameCosts[key]=(frameCosts[key]||0)+performance.now()-start;}};
    drawCombatFx=measure(drawCombatFx,'combatEffects');drawCachedTerrain=measure(drawCachedTerrain,'terrain');
    drawDwarfSprite=measure(drawDwarfSprite,'dwarfSprites');drawEnemySprite=measure(drawEnemySprite,'enemySprites');
    window.DwarfWorldLiving.drawFloor=measure(window.DwarfWorldLiving.drawFloor,'rooms');window.DwarfWorldLiving.drawOverlay=measure(window.DwarfWorldLiving.drawOverlay,'overlays');
  }
  const originalSim=updateSpawnersAndCombat,originalDraw=draw;
  updateSpawnersAndCombat=function(dt){const begin=performance.now();originalSim(dt);sim.push(performance.now()-begin);};
  draw=function(){frameCosts={};const begin=performance.now();frames.push(begin-lastFrame);lastFrame=begin;originalDraw();render.push(performance.now()-begin);renderCosts.push(frameCosts);};
  const summary=a=>{const sorted=a.slice(20).sort((a,b)=>a-b);return{samples:sorted.length,medianMs:+(sorted[Math.floor(sorted.length*.5)]||0).toFixed(2),p95Ms:+(sorted[Math.floor(sorted.length*.95)]||0).toFixed(2),maxMs:+(sorted.at(-1)||0).toFixed(2)}};
  window.__livingStress.result=()=>({dwarves:dwarves.length,enemies:enemies.length,rooms:lm.rooms.length,activeRooms:lm.rooms.filter(r=>r.active).length,production:lm.stats.produced,haulDeliveries:lm.stats.hauled,activeJobs:dwarves.filter(d=>d.living?.job).length,simulation:summary(sim),render:summary(render),renderComponents:${process.env.DWARF_PROFILE_RENDER==='1'}?Object.fromEntries(['combatEffects','terrain','dwarfSprites','enemySprites','rooms','overlays'].map(key=>[key,summary(renderCosts.map(v=>v[key]||0))])):undefined,frameIntervals:summary(frames),navigation:{...livingNav.stats},queued:livingNav.pending.length,terrainChunks:terrainChunks.size,effects:combatFx.length,heapMiB:performance.memory?+(performance.memory.usedJSHeapSize/1048576).toFixed(1):null,art:window.DWARF_ART?.status?.()||null});
}};
`;
const browser=await chromium.launch({headless:true,...(process.env.PLAYWRIGHT_CHANNEL?{channel:process.env.PLAYWRIGHT_CHANNEL}:{})});
const results=[];
try{
  for(const viewport of [{width:1280,height:800},{width:390,height:844}].slice(0,process.env.DWARF_STRESS_DESKTOP_ONLY==='1'?1:2)){
    const context=await browser.newContext({viewport,isMobile:viewport.width<600,hasTouch:viewport.width<600});const page=await context.newPage(),errors=[];
    await context.addInitScript(({origin})=>{window.DWARF_ART_CONFIG={assetBase:origin,assetPrefix:'art-test/'};},{origin:new URL(base).origin});
    page.on('pageerror',error=>errors.push(error.message));
    await page.route('**/*',async route=>{
      const url=new URL(route.request().url());
      if(url.origin!==new URL(base).origin)return route.abort();
      const asset=url.pathname.startsWith('/art-test/'),root=asset?assetDirectory:repository;
      const target=path.resolve(root,asset?decodeURIComponent(url.pathname.slice('/art-test/'.length)):'.'+decodeURIComponent(url.pathname));
      if(!target.startsWith(root+path.sep))return route.fulfill({status:403,body:''});
      try{
        let body=await fs.readFile(target);
        if(target.endsWith('DwarfWorld-core.html'))body=body.toString().replace(/\r\n?/g,'\n').replace('\n})();\n</script>',diagnostic+'\n})();\n</script>');
        const type=target.endsWith('.html')?'text/html':target.endsWith('.js')?'text/javascript':target.endsWith('.webp')?'image/webp':target.endsWith('.png')?'image/png':'text/plain';
        await route.fulfill({body,contentType:type});
      }catch{await route.fulfill({status:404,body:'Missing local fixture'});}
    });
    await page.goto(base);await page.locator('#newWorld').click();
    await page.waitForFunction(()=>document.querySelector('#gameFrame')?.contentWindow?.DwarfWorldLiving?.state,null,{timeout:20000}).catch(async error=>{
      console.error(JSON.stringify({errors,toast:await page.locator('#toast').textContent(),frames:page.frames().map(f=>f.url())}));throw error;
    });
    const frame=page.frames().find(frame=>frame!==page.mainFrame());assert.ok(frame);
    await frame.evaluate(async()=>{if(window.DWARF_ART?.ready)await window.DWARF_ART.ready;});
    if(process.env.REQUIRE_DWARF_ART==='1')assert.ok(await frame.evaluate(()=>!!window.DWARF_ART),'art runtime must be installed for the final test');
    await frame.evaluate(()=>window.__livingStress.start());
    await page.waitForTimeout(Number(process.env.DWARF_STRESS_MS||30000));
    const result=await frame.evaluate(()=>window.__livingStress.result());
    result.hauledUnits=result.haulDeliveries;delete result.haulDeliveries;
    if(screenshotDirectory){
      await fs.mkdir(screenshotDirectory,{recursive:true});
      await page.screenshot({path:path.join(screenshotDirectory,viewport.width<600?'living-mountain-mobile.png':'living-mountain-desktop.png')});
    }
    result.panelResponseMs=await frame.evaluate(()=>new Promise(resolve=>{const start=performance.now();document.querySelector('#lmDock [data-lm-open="dwarves"]').click();requestAnimationFrame(()=>requestAnimationFrame(()=>resolve(+(performance.now()-start).toFixed(2))));}));
    assert.deepEqual(errors,[],'full shell runtime errors');
    assert.ok(result.simulation.samples>50,'simulation continues under stress');
    assert.ok(result.rooms>=20);assert.ok(result.activeJobs>0,'production/hauling/construction jobs remain active');
    if(process.env.REQUIRE_DWARF_ART==='1'){assert.equal(result.art?.enabled,true);assert.deepEqual(result.art?.failed,[]);assert.ok(result.art?.loaded.includes('dwarves'));}
    assert.ok(result.queued<=96);assert.ok(result.terrainChunks<=112);assert.ok(result.effects<=180);
    results.push({measuredAt:new Date().toISOString(),browserVersion:browser.version(),platform:process.platform,architecture:process.arch,rendererComponentProfiling:process.env.DWARF_PROFILE_RENDER==='1',viewport,coarsePointer:viewport.width<600,...result,note:'Headless desktop Chrome: 100 dwarves, 20 rooms, production/construction, and 100 enemies. Narrow/coarse-pointer emulation is not a physical mobile-device benchmark. Heap excludes native image/canvas/GPU memory; panel response is click-to-second-animation-frame, not hardware latency. Component profiling, when enabled, adds per-call timing overhead; durations measure CPU drawing work rather than isolated GPU time.'});
    await context.close();
  }
  const json=JSON.stringify(results,null,2);
  if(process.env.DWARF_STRESS_JSON)await fs.writeFile(process.env.DWARF_STRESS_JSON,json+'\n');
  console.log(json);
}finally{await browser.close();}
