/* Real player GLBs plus controlled static-scene fixtures. No R2 dependency.
 * Supports BASKETBALL_CHROME and THREE_BUNDLE like the other browser checks. */
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'../../..'),graphics=path.join(root,'game/basketball2/assets/graphics-v2');
const mockLoader=`export class GLTFLoader { async parseAsync(bytes) {
  const scene=new THREE.Group(),file=new TextDecoder().decode(bytes);
  const names=file.includes('/hoops/')?['hoop_backboard','hoop_frame','hoop_rim','rim_enamel','hoop_net']:['fixture_court'];
  names.forEach(name=>{const mesh=new THREE.Mesh(new THREE.BoxGeometry(.1,.1,.1),new THREE.MeshStandardMaterial());mesh.name=name;scene.add(mesh);});
  return {scene};
} }`;
(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage(),errors=[],held=[];
    let holdLow=false;
    page.on('pageerror',error=>errors.push(error.stack));
    await page.addInitScript(()=>localStorage.setItem('basketball2SettingsV2',JSON.stringify({quality:'medium'})));
    await page.route('http://basketball.local/**',route=>{
      const url=new URL(route.request().url());
      // Ordinary parses must not race the explicit timeout scenario on busy CI.
      if(url.pathname.endsWith('/assets/graphics-v2-config.js'))return route.fulfill({contentType:'text/javascript',body:'window.BASKETBALL2_GRAPHICS={enabled:true,baseURL:"https://graphics.fixture/",loadingTimeoutMs:5000};'});
      if(url.pathname.endsWith('/vendor/GLTFLoader.mjs'))return route.fulfill({contentType:'text/javascript',body:mockLoader});
      let file=path.join(root,url.pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.json':'application/json','.glb':'model/gltf-binary'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://graphics.fixture/**',route=>{
      const url=new URL(route.request().url()),headers={'Access-Control-Allow-Origin':'*'};
      if(url.pathname==='/manifest.json')return route.fulfill({path:path.join(graphics,'manifest.json'),contentType:'application/json',headers});
      const player=url.pathname.match(/players\/player-(guard|wing|center)-(low|standard)\.glb$/);
      if(player){
        // Both requested detail levels use real bundled rigs; source labels
        // distinguish selection without checking in redundant standard assets.
        const response={path:path.join(graphics,`players/player-${player[1]}-low.glb`),contentType:'model/gltf-binary',headers};
        if(holdLow&&player[2]==='low'){held.push(()=>route.fulfill(response));return;}
        return route.fulfill(response);
      }
      return route.fulfill({body:url.pathname,contentType:'model/gltf-binary',headers});
    });
    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>Basketball2Graphics.players==='ready'&&Basketball2Graphics.activeScene);
    await page.evaluate(()=>{teamSize=3;startGame();});
    await page.waitForFunction(()=>!Basketball2Graphics.loading);
    assert.equal(await page.evaluate(()=>matchPlayers.every(char=>{
      const nodes=[];char.playerModel.group.traverse(node=>{if(node.userData.basketballNoShadow)nodes.push(node);});
      return nodes.length>0&&nodes.every(node=>!node.castShadow);
    })),true,'starting a match preserves accessory and jersey-number shadow suppression');
    const reaction=await page.evaluate(()=>{
      const originalRim=rim,originalBoard=backboard,originalNet=netLines;
      const rimPivot=Basketball2Graphics.activeScene.getObjectByName('graphics_v2_rim_reaction');
      const netPivot=Basketball2Graphics.activeScene.getObjectByName('graphics_v2_net_reaction');
      const boardPivot=Basketball2Graphics.activeScene.getObjectByName('graphics_v2_board_reaction');
      triggerNetWobble(1);backboardImpactTimer=.16;backboardImpactStrength=.05;updateHoopReaction(.05);
      const moved={rim:rimPivot.position.distanceTo(rimBasePosition)>0,net:netPivot.scale.y>1,board:boardPivot.position.distanceTo(backboardBasePosition)>0};
      updateHoopReaction(1);updateHoopReaction(1);
      return {moved,reset:netPivot.scale.y===1,physicsUnchanged:rim===originalRim&&backboard===originalBoard&&netLines===originalNet};
    });
    assert.deepEqual(reaction,{moved:{rim:true,net:true,board:true},reset:true,physicsUnchanged:true});
    console.log('PASS: accessory shadow policy and basket visual reactions preserve gameplay references.');

    await page.evaluate(()=>{
      window.__oldRig=p1.playerModel;window.__oldDisposed=0;
      const geometry=__oldRig.graphicsV2Asset.meshes[0][0].geometry;
      geometry.addEventListener('dispose',()=>window.__oldDisposed++);
      gameSettings.values.quality='low';setGraphicsQuality('low');
    });
    assert.equal(await page.evaluate(()=>p1.playerModel===__oldRig),true,'changing quality midmatch retains the active rig');
    await page.evaluate(()=>startGame());
    await page.waitForFunction(()=>!Basketball2Graphics.loading);
    assert.equal(await page.evaluate(()=>matchPlayers.every(char=>char.playerModel.graphicsV2Asset.label.endsWith('-low.glb'))),true,'next match selects low player assets');
    assert.equal(await page.evaluate(()=>__oldDisposed),1,'unused shared high-detail geometry is disposed once');

    await page.evaluate(()=>{gameSettings.values.quality='medium';setGraphicsQuality('medium');startGame();});
    await page.waitForFunction(()=>!Basketball2Graphics.loading);
    assert.equal(await page.evaluate(()=>matchPlayers.every(char=>char.playerModel.graphicsV2Asset.label.endsWith('-standard.glb'))),true);
    holdLow=true;
    await page.evaluate(()=>{gameSettings.values.quality='low';setGraphicsQuality('low');startGame();window.__deadlineRig=p1.playerModel;});
    await page.waitForFunction(()=>!Basketball2Graphics.loading);
    assert.equal(held.length,3);
    assert.equal(await page.evaluate(()=>Basketball2Graphics.errors.some(error=>error.includes('Startup timed out'))),true,'held responses exercise the startup deadline');
    await Promise.all(held.splice(0).map(finish=>finish()));holdLow=false;
    await page.waitForFunction(()=>[...basketballPlayerModels.assets.values()].every(asset=>asset.label.endsWith('-low.glb')));
    assert.equal(await page.evaluate(()=>p1.playerModel===__deadlineRig),true,'late asset results do not replace active skeletons');
    await page.evaluate(()=>startGame());await page.waitForFunction(()=>!Basketball2Graphics.loading);
    assert.equal(await page.evaluate(()=>matchPlayers.every(char=>char.playerModel.graphicsV2Asset.label.endsWith('-low.glb'))),true,'late loaded detail is used on the following match');
    assert.deepEqual(errors,[]);
    console.log('PASS: next-match player LOD, shared-asset disposal, loading deadline, and no midmatch skeleton swaps.');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
