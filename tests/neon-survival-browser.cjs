/* Integration QA against the real game. Test hooks are injected into the HTTP response only. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const http=require('node:http');
const path=require('node:path');
const {chromium}=require('playwright');
const repo=path.resolve(__dirname,'..'),fixtures=process.env.NEON_ASSET_FIXTURES;
const hooks=`
window.__NEON_SURVIVAL_TEST__={game,player,get player2(){return player2},get bots(){return bots},get walls(){return walls},get platforms(){return platforms},get map(){return survivalMapRuntime},get controller(){return survivalController},get equipment(){return survivalEquipment},get progress(){return survivalProgress},get ui(){return survivalInterface},get gl(){return gl},draw,reset,endMatch,kill,damage,makeAlien,spawnSurvivorWave,updateSurvivor,closeSurvivorShop,clearWave(wave){bots=bots.filter(b=>!b.alien);game.survivorWave=wave;game.waveKilled=game.waveTotal=1;game.waveSpawning=false;game.shopPending=false;game.waveDelay=0;game.survivorOrder=null;game.waveSpawnIndex=0;updateSurvivor(.016);}};
`;
function serve(req,res){
  try{const pathname=new URL(req.url,'http://localhost').pathname;let file;
    if(pathname.startsWith('/fixture-assets/')&&fixtures)file=path.resolve(fixtures,'.'+pathname.slice('/fixture-assets'.length));
    else file=path.resolve(repo,'.'+(pathname.endsWith('/')?pathname+'index.html':pathname));
    const allowed=pathname.startsWith('/fixture-assets/')&&fixtures?path.resolve(fixtures):repo;
    if(!file.startsWith(allowed+path.sep))throw Error('outside');
    let data=fs.readFileSync(file);if(path.basename(file)==='neon-arena.js')data=Buffer.from(data.toString().replace(/\}\)\(\);\s*$/,hooks+'})();'));
    const type={'.js':'text/javascript','.mjs':'text/javascript','.json':'application/json','.css':'text/css','.html':'text/html','.svg':'image/svg+xml','.glb':'model/gltf-binary','.png':'image/png','.webp':'image/webp'}[path.extname(file)]||'application/octet-stream';res.writeHead(200,{'Content-Type':type,'Access-Control-Allow-Origin':'*'});res.end(data);
  }catch(_){res.writeHead(404);res.end('not found')}
}
async function main(){
  const server=http.createServer(serve);await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
  const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--no-sandbox','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
  const reports=[];
  try{
    for(const device of [{name:'desktop',width:1280,height:800,touch:false,split:false},{name:'mobile',width:390,height:844,touch:true,split:false},{name:'split screen',width:1280,height:800,touch:false,split:true}].filter(device=>!process.env.NEON_SURVIVAL_DEVICE||device.name===process.env.NEON_SURVIVAL_DEVICE)){
      const context=await browser.newContext({viewport:{width:device.width,height:device.height},hasTouch:device.touch,isMobile:device.touch});const page=await context.newPage(),errors=[];
      page.on('pageerror',error=>errors.push(error.message));
      await page.addInitScript(base=>{window.NEON_ASSET_CONFIG={ASSET_BASE_URL:base,UI_ASSET_BASE_URL:'./assets',version:''};HTMLCanvasElement.prototype.requestPointerLock=function(){}},fixtures?origin+'/fixture-assets':'');
      await page.goto(origin+'/game/neon-arena/');await page.waitForFunction(()=>window.__NEON_SURVIVAL_TEST__?.ui,null,{timeout:15000}).catch(error=>{throw Error(device.name+' initialization failed: '+errors.join('; ')+' '+error.message)});
      await page.locator('[data-mode="survivor"]').click();
      assert.equal(await page.locator('[data-survival-map]').count(),8,'all locked arenas stay visible');assert.equal(await page.locator('[data-survival-map]:not([disabled])').count(),1,'only Breach Ring starts unlocked');
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,device.name+' menu overflow');
      if(device.split)await page.evaluate(()=>{document.getElementById('secondPlayer').value='keyboard';});
      await page.locator('#start').click();await page.waitForFunction(()=>__NEON_SURVIVAL_TEST__.game.started&&!__NEON_SURVIVAL_TEST__.game.over&&__NEON_SURVIVAL_TEST__.map.loaded,null,{timeout:20000});
      assert.equal(await page.evaluate(()=>!!__NEON_SURVIVAL_TEST__.player2),device.split);
      if(device.split){await page.evaluate(()=>{const t=__NEON_SURVIVAL_TEST__;t.kill(t.player2,t.makeAlien('blue',1));t.updateSurvivor(.016)});assert.equal(await page.evaluate(()=>__NEON_SURVIVAL_TEST__.game.over),false,'a split-screen run continues while one survivor lives');}
      await page.evaluate(()=>{const t=__NEON_SURVIVAL_TEST__;t.progress.recordEvent('kills',17);t.progress.recordEvent('bosses',1);t.progress.addTime(70);t.clearWave(5);t.draw()});
      const milestone=await page.evaluate(()=>{const t=__NEON_SURVIVAL_TEST__;return {map:t.game.survivalMap,current:t.map.current.id,unlocked:t.progress.isUnlocked('derelict_station'),highest:t.progress.getState().highestWave,started:t.game.started,over:t.game.over,shop:t.game.shopOpen,gl:t.gl.getError()}});
      assert.equal(milestone.map,'breach_ring');assert.equal(milestone.current,'breach_ring');assert.equal(milestone.unlocked,true);assert.equal(milestone.highest,5);assert.equal(milestone.started,true);assert.equal(milestone.over,false);assert.equal(milestone.shop,true);assert.equal(milestone.gl,0);
      assert.equal(await page.locator('#survivalUnlockNotice').isVisible(),true,'mid-run unlock notice is visible');
      await page.evaluate(()=>{const t=__NEON_SURVIVAL_TEST__;t.kill(t.player,t.makeAlien('blue',5));if(t.player2&&!t.player2.dead)t.kill(t.player2,t.makeAlien('blue',5));t.updateSurvivor(.016)});
      await page.locator('#survivalResults').waitFor({state:'visible',timeout:3000});
      assert.equal(await page.locator('#scoreboardOverlay').isVisible(),true);assert.equal(await page.locator('#survivalResults').isVisible(),true);
      assert.match(await page.locator('#survivalResults').textContent(),/17/);assert.match(await page.locator('#survivalResults').textContent(),/Derelict Station/);
      await page.locator('[data-survival-action="retry"]').click();await page.waitForFunction(()=>!__NEON_SURVIVAL_TEST__.game.over&&__NEON_SURVIVAL_TEST__.game.survivalMap==='breach_ring'&&__NEON_SURVIVAL_TEST__.game.survivorWave===0);await page.waitForTimeout(600);assert.equal(await page.evaluate(()=>__NEON_SURVIVAL_TEST__.game.over),false,'previous death callback cannot end the retry');
      await page.evaluate(()=>__NEON_SURVIVAL_TEST__.endMatch('Retry action verified'));await page.locator('[data-survival-action="maps"]').click();
      assert.equal(await page.locator('#menu').isVisible(),true);assert.equal(await page.locator('#survivalArenaSelector').isVisible(),true);
      await page.reload();await page.waitForFunction(()=>window.__NEON_SURVIVAL_TEST__?.ui);await page.locator('[data-mode="survivor"]').click();
      assert.equal(await page.locator('[data-survival-map]:not([disabled])').count(),2,'wave unlock survives a page reload');assert.equal(await page.evaluate(()=>__NEON_SURVIVAL_TEST__.progress.getState().stats.kills),17);
      // Unlock all worlds only in the fixture, then load and render each actual map bundle.
      await page.evaluate(()=>__NEON_SURVIVAL_TEST__.progress.mergeRemote({highestWave:100}));
      const mapIds=await page.evaluate(()=>NeonSurvivalProgress.maps.map(map=>map.id));
      for(const id of mapIds){
        await page.locator('[data-survival-map="'+id+'"]').click();await page.locator('#start').click();
        await page.waitForFunction(id=>__NEON_SURVIVAL_TEST__.game.started&&!__NEON_SURVIVAL_TEST__.game.over&&__NEON_SURVIVAL_TEST__.map.current?.id===id,id,{timeout:20000}).catch(async error=>{throw Error(id+' failed: '+errors.join('; ')+' '+error.message)});
        const result=await page.evaluate(()=>{const t=__NEON_SURVIVAL_TEST__;t.draw();return {loaded:t.map.loaded,finite:Number.isFinite(t.player.x+t.player.y+t.player.z),walls:t.walls.length,platforms:t.platforms.length,gl:t.gl.getError()}});
        assert.equal(result.loaded,true,id+' atlas loaded');assert.equal(result.finite,true,id+' valid player spawn');assert(result.walls+result.platforms>10,id+' authored structures');assert.equal(result.gl,0,id+' shader/draw error');
        await page.evaluate(()=>__NEON_SURVIVAL_TEST__.endMatch('Arena bundle verified'));await page.locator('[data-survival-action="maps"]').click();
      }
      await page.locator('.survivalLocker summary').click();await page.locator('[data-survival-reward-kind="finish"]').selectOption('apex_predator');
      assert.equal(await page.evaluate(()=>__NEON_SURVIVAL_TEST__.progress.getSelection().finish),'apex_predator');
      await page.locator('#start').click();await page.waitForFunction(()=>__NEON_SURVIVAL_TEST__.game.started&&!__NEON_SURVIVAL_TEST__.game.over);
      await page.evaluate(()=>__NEON_SURVIVAL_TEST__.endMatch('Main menu action verified'));await page.locator('[data-survival-action="menu"]').click();assert.equal(await page.locator('#menu').isVisible(),true);
      assert.deepEqual(errors,[],device.name+' runtime errors');reports.push({device:device.name,arenasLoaded:8,milestonePersisted:true,resultsActions:3,webgl:'real',errors:0});await context.close();
    }
    console.log(JSON.stringify({passed:reports,models:fixtures?'supplied GLB assets':'configured source'},null,2));
  }finally{await browser.close();await new Promise(resolve=>server.close(resolve))}
}
main().catch(error=>{console.error(error);process.exitCode=1});
