const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {assetPath}=require('./helpers/cloudflare-assets.cjs');
const repo=path.resolve(__dirname,'../../..');
const expose=`window.tcStrategic={run:source=>eval(source)};window.strategicClock=100000;nowMs=()=>strategicClock;resize();window.TurncraftStartup?.complete();`;
const source=fs.readFileSync(path.join(repo,'game/turncraft.html'),'utf8').replace(/^import .*;$/gm,'').replace('\nboot();','\n'+expose).replace('<script type="module">','<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');

test('four and eight commander fields render actual terrain with bounded caches at desktop and phone zooms',async t=>{
  const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']}),page=await browser.newPage({viewport:{width:1360,height:900}}),errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  await page.route('**/*',route=>{const url=route.request().url(),art=assetPath(url);if(art)return route.fulfill({path:art,contentType:'image/webp'});const pathname=decodeURIComponent(new URL(url).pathname);if(pathname==='/game/turncraft.html')return route.fulfill({contentType:'text/html',body:source});const file=path.resolve(repo,'.'+pathname);if(file.startsWith(repo+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file,contentType:file.endsWith('.mjs')?'text/javascript':undefined});return route.fulfill({status:404,body:''})});
  try{
    await page.goto('http://turncraft.test/game/turncraft.html');
    await page.evaluate(()=>tcStrategic.run(`spriteRuntime.loadManifest('/game/turncraft/assets/animation-manifest.json')`));
    for(const [map,count]of [['fourfold-divide',4],['rift-dominion',8]]){
      await page.evaluate(({map,count})=>tcStrategic.run(`chosenMapId=${JSON.stringify(map)};chosenAiCount=${count-1};createGame(buildSoloSlots());spectating=true;tileVisibleToFaction=()=>true;spriteRuntime.quality='medium';for(const p of players){const home=startPositions()[p.idx];for(let i=0;i<20;i++){const place=nearestFreeAround(home.x,home.y,null,8);if(place)addEntity('unit',FACTIONS[p.factionId].worker,p.idx,place.x,place.y)}}cam.zoom=.25;centerOn(BOARD_W/2,BOARD_H/2);draw();terrainRenderer.load(chosenMapId);`),{map,count});
      await page.waitForFunction(()=>tcStrategic.run(`terrainRenderer.stats.loaded.length>=3&&spriteRuntime.cache.stats().pending===0&&spriteRuntime.cache.stats().ready>0`),{},{timeout:30000});
      const measured=await page.evaluate(()=>tcStrategic.run(`(()=>{draw();const warmed=terrainRenderer.stats.builds,samples=[];for(let i=0;i<12;i++){strategicClock+=16.7;const start=performance.now();draw();samples.push(performance.now()-start)}samples.sort((a,b)=>a-b);return{players:players.length,units:entities.filter(e=>e.kind==='unit').length,terrain:terrainRenderer.stats,extraBuilds:terrainRenderer.stats.builds-warmed,sprite:spriteRuntime.cache.stats(),medianMs:samples[Math.floor(samples.length/2)],snapshotBytes:JSON.stringify(serializeState()).length}})()`));
      assert.equal(measured.players,count);assert.ok(measured.units>=count*20);assert.ok(measured.terrain.chunkBytes<=32*1024*1024);assert.equal(measured.extraBuilds,0,'steady overview must not rebuild cached chunks');assert.ok(measured.sprite.bytes<=measured.sprite.limit);assert.ok(measured.snapshotBytes<950000,'snapshots stay below Firestore document limit');
      t.diagnostic(`${map}: ${measured.units} units, median overview draw ${measured.medianMs.toFixed(1)}ms, terrain ${(measured.terrain.chunkBytes/1048576).toFixed(1)}MiB, snapshot ${measured.snapshotBytes} bytes`);
      const normal=await page.evaluate(()=>tcStrategic.run(`(()=>{spriteRuntime.quality='high';cam.zoom=1;centerOn(BOARD_W/2,BOARD_H/2);draw();const warmed=terrainRenderer.stats.builds;for(let i=0;i<3;i++){strategicClock+=16.7;draw()}const result={extraBuilds:terrainRenderer.stats.builds-warmed,terrain:terrainRenderer.stats};spriteRuntime.quality='medium';return result})()`));
      assert.equal(normal.extraBuilds,0,'normal high-quality desktop view must reuse every visible terrain chunk');assert.ok(normal.terrain.chunkBytes<=32*1024*1024);
    }
    await page.setViewportSize({width:390,height:844});
    const phone=await page.evaluate(()=>tcStrategic.run(`resize();cam.zoom=.7;centerOn(startPositions()[0].x,startPositions()[0].y);draw();({width:W,height:H,terrain:terrainRenderer.stats})`));assert.equal(phone.width,390);assert.ok(phone.terrain.chunkBytes<=32*1024*1024);assert.deepEqual(errors,[]);
    if(process.env.TURNCRAFT_QA_SCREENSHOT){await page.setViewportSize({width:1360,height:900});await page.waitForFunction(()=>tcStrategic.run('innerWidth===1360&&W===1360'));await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));await page.evaluate(()=>tcStrategic.run(`cam.zoom=.25;centerOn(BOARD_W/2,BOARD_H/2);draw();`));await page.screenshot({path:process.env.TURNCRAFT_QA_SCREENSHOT});}
  }finally{await browser.close()}
});
