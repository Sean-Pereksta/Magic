/* Deterministic, offline graphics-loader lifecycle regression.
 * Run: THREE_BUNDLE=/path/to/three.min.js node game/basketball2/tests/graphics-v2-lifecycle-browser.cjs
 * THREE_BUNDLE must contain the game's Three.js r160 global bundle.
 * BASKETBALL_CHROME optionally selects a Chromium executable, as in browser-smoke.cjs.
 *
 * Uses the integrated index/config/runtime, real bundled player GLBs, and a
 * controllable GLTFLoader fixture for environment decoding. No asset network
 * requests are allowed through. The fixture host intentionally has no prefix:
 * manifest.json, players/, courts/, arenas/, and hoops/ are at the upload root.
 */
const fs=require('node:fs');
const path=require('node:path');
const assert=require('node:assert/strict');
const {chromium}=require('playwright');

const root=path.resolve(__dirname,'../../..');
const gameRoot=path.join(root,'game/basketball2');
const bundle=process.env.THREE_BUNDLE;
assert.ok(bundle && fs.existsSync(bundle),'Set THREE_BUNDLE to a local Three.js r160 global bundle for this offline test.');
const fixtureBase='https://pack.fixture/';
const environments=Object.fromEntries(['pro-arena','streetball','training-gym','championship'].map(name=>[
  name,{court:{low:`courts/${name}-low.glb`,high:`courts/${name}-high.glb`},arena:{low:`arenas/${name}-low.glb`,high:`arenas/${name}-high.glb`}}
]));
const manifest={
  release:'basketball2-v2',
  players:Object.fromEntries(['guard','wing','center'].map(kind=>[
    kind,{low:`players/player-${kind}-low.glb`,standard:`players/player-${kind}-standard.glb`}
  ])),
  environments,
  hoop:{low:'hoops/regulation-low.glb',high:'hoops/regulation-high.glb'}
};
const mockLoader=`
export class GLTFLoader {
  async parseAsync(bytes) {
    const source=new TextDecoder().decode(bytes),id=++window.__decodeId;
    const scene=new THREE.Group(),geometry=new THREE.BoxGeometry(1,1,1);
    const texture=new THREE.Texture({width:1,height:1,close(){window.__disposed.push({id,source,kind:'bitmap'});}});
    const material=new THREE.MeshStandardMaterial({map:texture});
    for(const [resource,kind] of [[geometry,'geometry'],[material,'material'],[texture,'texture']])
      resource.addEventListener('dispose',()=>window.__disposed.push({id,source,kind}));
    scene.add(new THREE.Mesh(geometry,material));
    scene.name=source;scene.userData.fixtureId=id;scene.userData.fixtureSource=source;
    window.__created.push({id,source});
    if(window.__holdDecodes)
      return new Promise(resolve=>window.__pendingDecodes.push({id,source,finish:()=>resolve({scene})}));
    return {scene};
  }
}`;
const mime=file=>({
  '.html':'text/html','.css':'text/css','.js':'text/javascript','.mjs':'text/javascript',
  '.webp':'image/webp','.jpg':'image/jpeg','.png':'image/png','.svg':'image/svg+xml',
  '.glb':'model/gltf-binary','.json':'application/json'
}[path.extname(file)] || 'application/octet-stream');

(async()=>{
  const launch={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME)launch.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(launch);
  const openPages=new Set();
  async function pageFor(options={}){
    const page=await browser.newPage({viewport:{width:900,height:650}});
    openPages.add(page);
    const errors=[],requests=[],unexpected=[];
    page.on('pageerror',error=>errors.push(error.stack || error.message));
    await page.addInitScript(({quality,hold})=>{
      localStorage.setItem('basketball2SettingsV2',JSON.stringify({quality}));
      window.__holdDecodes=hold;
      window.__pendingDecodes=[];
      window.__created=[];
      window.__disposed=[];
      window.__decodeId=0;
    },{quality:options.quality || 'medium',hold:!!options.hold});
    // Later, narrower routes win. This final guard makes the test truly offline.
    await page.route('**/*',route=>{
      unexpected.push(route.request().url());
      return route.abort('blockedbyclient');
    });
    await page.route('http://basketball.local/**',route=>{
      const url=new URL(route.request().url());
      if(url.pathname.endsWith('/assets/graphics-v2-config.js')){
        const config={
          enabled:true,baseURL:fixtureBase,fallbackBase:'assets/graphics-v2/',playerLOD:'auto',
          timeoutMs:3000,environmentTimeoutMs:options.environmentTimeout || 5000,
          loadingTimeoutMs:options.loadingTimeout || 7000
        };
        return route.fulfill({contentType:'text/javascript',body:`window.BASKETBALL2_GRAPHICS=${JSON.stringify(config)};`});
      }
      if(url.pathname.endsWith('/vendor/GLTFLoader.mjs'))
        return route.fulfill({contentType:'text/javascript',body:mockLoader});
      let file=path.join(root,url.pathname);
      if(fs.existsSync(file) && fs.statSync(file).isDirectory())file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Fixture file not found'});
      // Exercise the real functions manually without a background RAF loop.
      if(file.endsWith('bootstrap.js'))
        return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      return route.fulfill({path:file,contentType:mime(file)});
    });
    await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',
      route=>route.fulfill({path:path.resolve(bundle),contentType:'text/javascript'}));
    await page.route(fixtureBase+'**',route=>{
      const pathname=new URL(route.request().url()).pathname;
      requests.push(pathname);
      const headers={'Access-Control-Allow-Origin':'*'};
      if(pathname==='/manifest.json')
        return route.fulfill({contentType:'application/json',body:JSON.stringify(manifest),headers});
      const player=pathname.match(/^\/players\/player-(guard|wing|center)-(low|standard)\.glb$/);
      if(player)return route.fulfill({
        path:path.join(gameRoot,`assets/graphics-v2/players/player-${player[1]}-low.glb`),
        contentType:'model/gltf-binary',headers
      });
      if(/^\/(?:courts|arenas|hoops)\/[a-z-]+\.glb$/.test(pathname))
        return route.fulfill({body:pathname,contentType:'model/gltf-binary',headers});
      return route.fulfill({status:404,body:'Unexpected upload path',headers});
    });
    await page.goto('http://basketball.local/game/basketball2/index.html');
    await page.waitForFunction(()=>globalThis.Basketball2Graphics?.players==='ready');
    return {page,errors,requests,unexpected};
  }
  async function finishDecodes(page){
    await page.evaluate(()=>{
      window.__holdDecodes=false;
      window.__pendingDecodes.splice(0).forEach(entry=>entry.finish());
    });
  }
  async function assertDisposed(page,ids){
    await page.waitForFunction(ids=>ids.every(id=>window.__disposed.some(entry=>entry.id===id && entry.kind==='geometry')),ids);
    const entries=await page.evaluate(ids=>window.__disposed.filter(entry=>ids.includes(entry.id)),ids);
    for(const id of ids){
      assert.deepEqual(entries.filter(entry=>entry.id===id).map(entry=>entry.kind).sort(),
        ['bitmap','geometry','material','texture'],`obsolete decoded asset ${id} releases each resource exactly once`);
    }
  }
  async function assertScoreboard(page){
    assert.deepEqual(await page.evaluate(()=>({
      direct:scoreboardPlane.parent===scene,
      visible:scoreboardPlane.visible && scene.visible,
      outsideFallback:!Basketball2Graphics.legacyHoop.children.includes(scoreboardPlane)
    })),{direct:true,visible:true,outsideFallback:true},'live scoreboard survives both modern and fallback hoop visibility');
  }
  async function closeFixture(fixture){
    assert.deepEqual(fixture.errors,[],'no uncaught browser errors');
    assert.deepEqual(fixture.unexpected,[],'all browser requests are fulfilled locally');
    assert.ok(fixture.requests.includes('/manifest.json'),'manifest resolves at the upload root');
    assert.ok(fixture.requests.every(url=>!url.startsWith('/basketball2-v2/')),'no release folder is added to the configured upload root');
    openPages.delete(fixture.page);
    await fixture.page.close();
  }
  try{
    const deadline=await pageFor({hold:true,environmentTimeout:900});
    await deadline.page.waitForFunction(()=>window.__pendingDecodes.length>0);
    const locked=await deadline.page.evaluate(()=>{
      startGame();
      const before=simulationTime;
      togglePause();simulateBasketball(.25);
      const escape={paused:gamePaused,loading:Basketball2Graphics.loading,unchanged:simulationTime===before};
      setPaused(false);
      const resumeStillLocked=gamePaused;
      setPaused(true);
      return {escape,resumeStillLocked};
    });
    assert.deepEqual(locked,{escape:{paused:true,loading:true,unchanged:true},resumeStillLocked:true});
    await deadline.page.waitForFunction(()=>!Basketball2Graphics.loading);
    assert.deepEqual(await deadline.page.evaluate(()=>({
      paused:gamePaused,environment:Basketball2Graphics.environment,
      fallback:Basketball2Graphics.legacyArena.visible,
      deadline:Basketball2Graphics.errors.some(error=>error.includes('Environment loading deadline exceeded'))
    })),{paused:true,environment:null,fallback:true,deadline:true});
    const timedOutIds=await deadline.page.evaluate(()=>window.__pendingDecodes.map(entry=>entry.id));
    await finishDecodes(deadline.page);
    await assertDisposed(deadline.page,timedOutIds);
    assert.equal(await deadline.page.evaluate(()=>Basketball2Graphics.activeScene),null,'late decode cannot replace the deadline fallback');
    await assertScoreboard(deadline.page);
    await closeFixture(deadline);
    console.log('PASS: simulation and pause lock, environment decode deadline, fallback, and complete late-result disposal.');

    const startup=await pageFor({hold:true,environmentTimeout:5000,loadingTimeout:700});
    await startup.page.waitForFunction(()=>window.__pendingDecodes.length>0);
    await startup.page.evaluate(()=>startGame());
    await startup.page.waitForFunction(()=>!Basketball2Graphics.loading);
    assert.equal(await startup.page.evaluate(()=>Basketball2Graphics.errors.some(error=>error.includes('Startup timed out'))),true,
      'the overall startup deadline ends an otherwise unresolved decode');
    assert.equal(await startup.page.evaluate(()=>Basketball2Graphics.legacyArena.visible),true);
    const startupIds=await startup.page.evaluate(()=>window.__pendingDecodes.map(entry=>entry.id));
    await finishDecodes(startup.page);
    await assertDisposed(startup.page,startupIds);
    assert.equal(await startup.page.evaluate(()=>Basketball2Graphics.activeScene),null);
    await closeFixture(startup);
    console.log('PASS: overall startup deadline cancels a longer environment job and disposes its late decode.');

    const blurred=await pageFor({hold:true});
    await blurred.page.waitForFunction(()=>window.__pendingDecodes.length>0);
    assert.deepEqual(await blurred.page.evaluate(()=>{
      startGame();window.dispatchEvent(new Event('blur'));
      return {loading:Basketball2Graphics.loading,paused:gamePaused,suspended:inputManager.suspended};
    }),{loading:true,paused:true,suspended:true});
    await finishDecodes(blurred.page);
    await blurred.page.waitForFunction(()=>!Basketball2Graphics.loading);
    assert.equal(await blurred.page.evaluate(()=>gamePaused),true,'decode completion cannot resume an unfocused match');
    await blurred.page.evaluate(()=>window.dispatchEvent(new Event('focus')));
    assert.deepEqual(await blurred.page.evaluate(()=>({paused:gamePaused,suspended:inputManager.suspended})),{paused:true,suspended:false},
      'focus restores input without automatically resuming the match');
    await closeFixture(blurred);
    console.log('PASS: blur during loading remains paused after graphics complete and focus returns.');

    const menu=await pageFor({hold:true});
    await menu.page.waitForFunction(()=>window.__pendingDecodes.length>0);
    await menu.page.evaluate(()=>{
      startGame();showPrimaryMenu();
    });
    const menuState=()=>menu.page.evaluate(()=>({
      started:gameStarted,loading:Basketball2Graphics.loading,
      curtainHidden:document.getElementById('bb2GraphicsLoading').hidden,
      menuVisible:document.getElementById('menu').style.display==='flex',
      primary:document.getElementById('primaryMenuScreen').classList.contains('active')
    }));
    const cancelled={started:false,loading:false,curtainHidden:true,menuVisible:true,primary:true};
    assert.deepEqual(await menuState(),cancelled);
    await finishDecodes(menu.page);
    await menu.page.evaluate(async()=>{await Basketball2Graphics.environmentReady;await Promise.resolve();});
    assert.deepEqual(await menuState(),cancelled,'a cancelled start cannot reopen the match after its decode completes');
    await closeFixture(menu);
    console.log('PASS: returning to the menu cancels the loading curtain and late completion cannot restart the match.');

    const changes=await pageFor({quality:'low'});
    await changes.page.waitForFunction(()=>Basketball2Graphics.activeScene);
    const lowRequests=changes.requests.filter(request=>request.startsWith('/players/'));
    assert.equal(lowRequests.length,3);
    assert.ok(lowRequests.every(request=>request.endsWith('-low.glb')),'Low is a player LOD hard cap');
    assert.equal(await changes.page.evaluate(()=>Basketball2Graphics.diagnostics().environmentLOD),'low');
    await assertScoreboard(changes.page);
    await changes.page.evaluate(()=>{gameSettings.set('quality','medium');setGraphicsQuality('medium');});
    await changes.page.waitForFunction(()=>Basketball2Graphics.diagnostics().environmentLOD==='high');
    await changes.page.evaluate(()=>{gameSettings.set('quality','low');setGraphicsQuality('low');});
    await changes.page.waitForFunction(()=>Basketball2Graphics.diagnostics().environmentLOD==='low');
    const replacedIds=await changes.page.evaluate(()=>Basketball2Graphics.activeScene.children.map(child=>child.userData.fixtureId));
    await changes.page.evaluate(()=>{window.__holdDecodes=true;selectedCourt='blacktop';applyCourtTheme(selectedCourt);});
    await changes.page.waitForFunction(()=>window.__pendingDecodes.some(entry=>entry.source.includes('streetball')));
    const obsoleteIds=await changes.page.evaluate(()=>window.__pendingDecodes.filter(entry=>entry.source.includes('streetball')).map(entry=>entry.id));
    await changes.page.evaluate(()=>{selectedCourt='goldLeague';applyCourtTheme(selectedCourt);});
    await changes.page.waitForFunction(()=>window.__pendingDecodes.some(entry=>entry.source.includes('championship')));
    await finishDecodes(changes.page);
    await changes.page.waitForFunction(()=>Basketball2Graphics.environment==='championship');
    await assertDisposed(changes.page,[...replacedIds,...obsoleteIds]);
    await assertScoreboard(changes.page);
    await changes.page.evaluate(()=>{BASKETBALL2_GRAPHICS.environment='missing-environment';Basketball2Graphics.reloadEnvironment();});
    await changes.page.waitForFunction(()=>Basketball2Graphics.environment===null);
    assert.equal(await changes.page.evaluate(()=>Basketball2Graphics.legacyArena.visible && Basketball2Graphics.legacyHoop.visible),true);
    await assertScoreboard(changes.page);
    await closeFixture(changes);
    console.log('PASS: root upload paths, Low player cap, scene LOD changes, superseded-resource disposal, fallback, and live scoreboard.');
  }finally{
    await Promise.allSettled([...openPages].map(page=>page.close()));
    await browser.close();
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
