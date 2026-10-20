/* Run: node game/basketball2/tests/graphics-v2-browser.cjs
 * Uses the configured CDN. GRAPHICS_ASSET_DIR optionally supplies a local copy
 * of the uploaded root. THREE_BUNDLE / BASKETBALL_CHROME support offline runs.
 * GRAPHICS_REPORT / GRAPHICS_SCREENSHOT optionally save diagnostics / gameplay.
 */
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'../../..');
const config=fs.readFileSync(path.join(root,'game/basketball2/assets/graphics-v2-config.js'),'utf8');
const baseURL=JSON.parse(config.slice(config.indexOf('{'),config.lastIndexOf('}')+1)).baseURL;
const mime=file=>file.endsWith('.html')?'text/html':file.endsWith('.css')?'text/css':file.endsWith('.json')?'application/json':file.endsWith('.glb')?'model/gltf-binary':file.endsWith('.jpg')?'image/jpeg':file.endsWith('.svg')?'image/svg+xml':'text/javascript';
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.BASKETBALL_CHROME,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']});
 const report={baseURL,remote:!process.env.GRAPHICS_ASSET_DIR,rendering:'Chromium SwiftShader; draw counts are not a real-device FPS benchmark',errors:[],variants:[]};
 try{
  async function open({failure=false,mobile=false}={}){
   const page=await browser.newPage({viewport:mobile?{width:390,height:844}:{width:1440,height:900},isMobile:mobile,hasTouch:mobile});
   page.on('pageerror',e=>report.errors.push(e.stack));
   await page.addInitScript(()=>localStorage.setItem('basketball2SettingsV2',JSON.stringify({quality:'medium'})));
   await page.route('http://basketball.local/**',route=>{
    const file=path.join(root,new URL(route.request().url()).pathname);
    if(!fs.existsSync(file))return route.fulfill({status:404,body:'not found'});
    if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
    return route.fulfill({path:file,contentType:mime(file)});
   });
   if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
   if(failure || process.env.GRAPHICS_ASSET_DIR)await page.route(baseURL+'**',route=>{
    if(failure)return route.fulfill({status:503,body:'Intentional failure test',headers:{'Access-Control-Allow-Origin':'*'}});
    const relative=new URL(route.request().url()).pathname.slice(new URL(baseURL).pathname.length),file=path.join(process.env.GRAPHICS_ASSET_DIR,relative);
    return route.fulfill({path:file,headers:{'Access-Control-Allow-Origin':'*'},contentType:mime(file)});
   });
   await page.goto('http://basketball.local/game/basketball2/index.html');
   await page.waitForFunction(()=>Basketball2Graphics.players==='ready',null,{timeout:45000});
   await page.evaluate(async()=>{await Basketball2Graphics.environmentReady;teamSize=3;twoPlayerMode=false;startGame();});
   await page.waitForFunction(()=>!Basketball2Graphics.loading,null,{timeout:45000});
   await page.evaluate(()=>{gamePaused=false;for(let i=0;i<30;i++)updateCameras(1/60);renderScene();});
   return page;
  }
  async function record(page,kind){
   const row=await page.evaluate(kind=>({kind,...Basketball2Graphics.diagnostics(),rigs:matchPlayers.map(c=>({bones:c.playerModel?.bones.size})),loadedLODs:[...basketballPlayerModels.assets.values()].map(a=>a.gltf.asset.extras.lod)}),kind);
   report.variants.push(row);return row;
  }
  const page=await open(),initial=await record(page,'desktop-medium');
  assert.equal(initial.environment,'pro-arena');assert.equal(initial.environmentLOD,'high');assert.equal(initial.players,'ready');
  assert.ok(initial.rigs.every(r=>r.bones===24));
  const presentation=await page.evaluate(()=>{
   const shadowAccessories=[];
   matchPlayers.forEach(c=>c.group.traverse(n=>{if(n.isMesh && (n.name.startsWith('equipment_')||n.name==='jersey_numbers') && n.castShadow)shadowAccessories.push(n.name);}));
   let shadowLights=0;scene.traverse(n=>{if(n.isLight && n.castShadow)shadowLights++;});
   const skins=matchPlayers.map(c=>({profile:c.profile.skin,color:c.playerModel.materials.find(m=>m.name==='skin')?.color.getHex()}));
   return {shadowAccessories,shadowLights,skins,camera:{fov:camMain.fov,offset:COURT_CAMERA_OFFSET.toArray()}};
  });
  assert.deepEqual(presentation.shadowAccessories,[]);assert.equal(presentation.shadowLights,1);
  assert.deepEqual(presentation.camera,{fov:70,offset:[0,9,12.6]});
  assert.ok(presentation.skins.every(s=>s.color===s.profile),'each athlete uses their profile skin color');
  report.presentation=presentation;
  await page.evaluate(()=>{
   const positions=[[-1.8,-2.7],[0,-5.2],[3.5,-4.7],[-1.6,-4.7],[3.2,-6.6],[1,-8.2]];
   matchPlayers.forEach((c,i)=>{c.group.position.set(positions[i][0],0,positions[i][1]);c.angle=c.teamId===0?Math.PI:0;c.group.rotation.y=c.angle;updatePlayerModel(c,1/60);});
   ball.position.set(-1.8,1.1,-2.5);for(let i=0;i<80;i++)updateCameras(1/60);renderScene();
  });
  if(process.env.GRAPHICS_SCREENSHOT)await page.screenshot({path:process.env.GRAPHICS_SCREENSHOT});
  const reactions=await page.evaluate(()=>{
   const visual=Basketball2Graphics.activeScene,rimPivot=visual.getObjectByName('graphics_v2_rim_reaction'),netPivot=visual.getObjectByName('graphics_v2_net_reaction');
   const before=rimPivot.position.clone(),beforeNet=netPivot.quaternion.clone(),physicsRim=rim;
   triggerNetWobble(1.4);updateHoopReaction(1/60);
   return {rimMoved:before.distanceTo(rimPivot.position)>0,netMoved:beforeNet.angleTo(netPivot.quaternion)>0,physicsUnchanged:physicsRim===rim};
  });
  assert.deepEqual(reactions,{rimMoved:true,netMoved:true,physicsUnchanged:true});report.reactions=reactions;
  for(const [key,expected] of [['newYorkCage','streetball'],['bostonLegacy','training-gym'],['vegasFinals','championship'],['classicArena','pro-arena']]){
   await page.evaluate(async key=>{selectedCourt=key;applyCourtTheme(key);await Basketball2Graphics.environmentReady;renderScene();},key);
   const row=await record(page,key);assert.equal(row.environment,expected);assert.equal(row.environmentLOD,'high');
  }
  await page.evaluate(async()=>{gameSettings.set('quality','low');setGraphicsQuality('low');await Basketball2Graphics.environmentReady;renderScene();});
  assert.equal((await record(page,'desktop-low')).environmentLOD,'low');
  assert.equal(await page.evaluate(()=>renderer.shadowMap.enabled),false);
  await page.evaluate(()=>startGame());await page.waitForFunction(()=>!Basketball2Graphics.loading);
  assert.ok((await record(page,'low-rematch')).loadedLODs.every(l=>l==='low'));
  await page.close();
  const failed=await open({failure:true}),fallback=await record(failed,'remote-unavailable');
  assert.equal(fallback.players,'ready');assert.equal(fallback.environment,null);assert.ok(fallback.rigs.every(r=>r.bones===24));
  assert.equal(await failed.evaluate(()=>Basketball2Graphics.legacyArena.visible),true);await failed.close();
  const mobile=await open({mobile:true}),mobileState=await record(mobile,'mobile');
  assert.ok(mobileState.loadedLODs.every(l=>l==='low'));assert.equal(mobileState.environmentLOD,'low');await mobile.close();
  assert.deepEqual(report.errors,[]);
  if(process.env.GRAPHICS_REPORT)fs.writeFileSync(process.env.GRAPHICS_REPORT,JSON.stringify(report,null,2));
  console.log('PASS: real assets, four courts, profile colors, camera, hoop reactions, shadows, Low rematch, mobile and CDN failure fallback');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
