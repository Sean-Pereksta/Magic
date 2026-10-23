const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),out=process.env.MONKEY_TRAILS_OUTPUT;
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
(async()=>{
 if(out)fs.mkdirSync(out,{recursive:true});
 const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})});
 try{
  const context=await browser.newContext({viewport:{width:390,height:900},isMobile:true,hasTouch:true});
  await context.route('**/*',async route=>{const url=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(url.pathname));if(url.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file)){await route.abort();return;}await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});
  const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  const enter=async()=>{await page.goto('http://127.0.0.1:7777/game/monkey-trails.html');await page.locator('#practice-button').click();await page.locator('.land-scene').waitFor();};
  await enter();
  assert.match(await page.locator('.land-heading h2').innerText(),/Emerald Jungle/);
  assert.equal(await page.locator('.land-rail').count(),0,'region picker stays out of the main screen');
  assert.equal(await page.locator('.land-scene .terrain-map').count(),1);
  assert.equal(await page.locator('.map-compass').count(),1,'small compass is always visible');
  if(out)await page.screenshot({path:path.join(out,'monkey-trails-lands-mobile.png'),fullPage:true});
  await page.locator('[data-action="compass"]').click();
  await page.locator('#inspect-dialog[open] .radar').waitFor();
  await page.locator('.dialog-close').click();
  const start=await page.locator('.terrain-map').evaluate(el=>({east:Number(el.dataset.mapEast),north:Number(el.dataset.mapNorth)}));
  for(const [id,east,north]of [['N',start.east,start.north+15],['E',start.east+15,start.north+15],['S',start.east+15,start.north],['W',start.east,start.north]]){
   await page.locator('[data-action="map-step"][data-id="'+id+'"]').click();
   const position=await page.locator('.terrain-map').evaluate(el=>({east:Number(el.dataset.mapEast),north:Number(el.dataset.mapNorth)}));
   assert.deepEqual(position,{east,north},id+' moves through the actual map coordinates');
  }
  assert.equal(await page.locator('.map-compass').count(),1);
  await page.locator('.practice-tools summary').click();
  await page.locator('[data-action="practice-step"][data-id="N"]').click();
  await page.locator('[data-action="lands"]').first().click();
  assert.equal(await page.locator('.land-atlas .land-tile').count(),10);
  await page.locator('.land-atlas [data-id="moonlit-forest"]').click();
  assert.equal(await page.locator('[data-action="land-select"]').count(),0);
  assert.match(await page.locator('.land-unlock-copy').innerText(),/72 km/);
  assert.equal(await page.evaluate(()=>document.activeElement?.tagName),'H2');
  await page.locator('.dialog-close').click();
  // Seed verified lifetime progress below the new2km gate, then cross it with
  // an actual accepted15m practice step (without traversing unrelated battles).
  await page.evaluate(async()=>{const {Vault}=await import('/game/monkey-trails/storage.mjs'),v=new Vault(()=>{}),s=await v.open('Trail Practice','practice-only-not-ranked',{mode:'practice'});s.world.totalDistance=1990;s.world.regions['emerald-jungle'].distance=1990;await v.checkpoint(s,true);});
  await enter();await page.locator('[data-action="map-step"][data-id="N"]').click();
  await page.locator('[data-action="lands"]').first().click();
  await page.locator('.land-atlas [data-id="sunstone-savannah"]').click();
  assert.match(await page.locator('[data-action="land-select"]').innerText(),/Visit Sunstone Savannah/);
  assert.doesNotMatch(await page.locator('[data-action="land-select"]').innerText(),/>|disabled/);
  await page.locator('[data-action="land-select"]').click();
  assert.equal(await page.locator('.land-heading h2').innerText(),'Sunstone Savannah');
  await page.waitForFunction(()=>/Practice saved on this device/.test(document.getElementById('save-status').textContent));
  await enter();
  assert.equal(await page.locator('.land-heading h2').innerText(),'Sunstone Savannah','selected land survives reload');
  await page.evaluate(async()=>{const {Vault}=await import('/game/monkey-trails/storage.mjs'),v=new Vault(()=>{}),s=await v.open('Trail Practice','practice-only-not-ranked',{mode:'practice'});s.world.totalDistance=140000;await v.checkpoint(s,true);});
  await enter();
  const visited=[];
  for(const id of ['emerald-jungle','sunstone-savannah','crystal-caverns','whispering-marsh','frostpeak-mountains','thunder-highlands','ember-volcano','moonlit-forest','celestial-ruins','ancient-monkey-kingdom']){
   await page.locator('[data-action="lands"]').first().click();
   await page.locator('.land-atlas [data-id="'+id+'"]').click();
   const invalid=await page.locator('.land-preview-art').evaluate(el=>new DOMParser().parseFromString(el.innerHTML,'image/svg+xml').querySelector('parsererror')?.textContent);
   assert.equal(invalid,undefined,id+' valid SVG');
   await page.locator('[data-action="land-select"]').click();
   assert.ok(await page.locator('.land-scene .terrain-map').count());visited.push(id);
  }
  await page.locator('[data-action="lands"]').first().click();
  if(out)await page.screenshot({path:path.join(out,'monkey-trails-lands-atlas-mobile.png'),fullPage:true});
  await page.locator('.dialog-close').click();
  const viewports=[];
  for(const width of [320,360,390,820,1280]){await page.setViewportSize({width,height:900});const size=await page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth}));assert.ok(size.scroll<=size.width,'no overflow at '+width);viewports.push(size);}
  await page.setViewportSize({width:390,height:900});
  await page.evaluate(()=>{localStorage.setItem('monkey-trails-display-v2',JSON.stringify({reducedMotion:true}));});await enter();
  assert.equal(await page.locator('.land-scene').evaluate(el=>el.getAnimations({subtree:true}).length),0,'reduced motion stops scenery and traveler');
  assert.deepEqual(errors,[]);
  console.log(JSON.stringify({visited,viewports,reloadKeepsSelection:true,lockedPreview:true,cardinalMovement:true,compassVisible:true,popupOnly:true,reducedMotion:true,pageErrors:errors},null,2));
  await context.close();
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
