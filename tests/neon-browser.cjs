// Requires Playwright. Tests serve the actual game; private closure hooks are
// added to the HTTP response only and never shipped to the production page.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const {chromium} = require('playwright');
const repo = path.resolve(__dirname, '..');
const fixtures = process.env.NEON_ASSET_FIXTURES;
const hooks = `\nwindow.__NEON_TEST__ = {game, player, get player2(){return player2}, get bots(){return bots}, get platforms(){return platforms}, get walls(){return walls}, mobileInput, WEAPONS, reset, draw, physics, spawn, endMatch, getBuildPreviewFor, placeBuildFor, setBuildOverrideFor, humanBuildState, requestHumanJump, handleHumanButtons, updateMobileAimAndFire, mobileAimInfo, makeAlien, kill, switchWeapon, setActorAt, get models(){return neonModelRenderer}, get gl(){return gl}};\n`;
function serve(req, res) {
  try {
    const pathname = new URL(req.url, 'http://localhost').pathname;
    let file;
    if (pathname.startsWith('/assets/') && fixtures) file = path.resolve(fixtures, '.' + pathname.slice(7));
    else file = path.resolve(repo, '.' + (pathname.endsWith('/') ? pathname + 'index.html' : pathname));
    const allowed = fixtures && pathname.startsWith('/assets/') ? path.resolve(fixtures) : repo;
    if (!file.startsWith(allowed + path.sep)) throw new Error('Outside fixture root');
    let data = fs.readFileSync(file);
    if (path.basename(file) === 'neon-arena.js') data = Buffer.from(data.toString().replace(/\}\)\(\);\s*$/, hooks + '})();'));
    const type = {'.js':'text/javascript','.mjs':'text/javascript','.json':'application/json','.css':'text/css','.html':'text/html','.glb':'model/gltf-binary','.png':'image/png','.webp':'image/webp'}[path.extname(file)] || 'application/octet-stream';
    res.writeHead(200, {'Content-Type':type, 'Access-Control-Allow-Origin':'*'}); res.end(data);
  } catch {res.writeHead(404); res.end('Not found');}
}
async function main() {
  const server = http.createServer(serve); await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = 'http://127.0.0.1:' + server.address().port;
  const browser = await chromium.launch({headless:true, ...(process.env.CHROMIUM_PATH ? {executablePath:process.env.CHROMIUM_PATH} : {}), args:['--no-sandbox','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
  const reports=[];
  try {
    for (const device of [{name:'desktop',width:1280,height:800,touch:false},{name:'phone portrait',width:390,height:844,touch:true},{name:'phone landscape',width:844,height:390,touch:true}]) {
      const context = await browser.newContext({viewport:{width:device.width,height:device.height}, hasTouch:device.touch, isMobile:device.touch});
      const page = await context.newPage(), errors=[];
      page.on('pageerror', e=>errors.push(e.message));
      if(process.env.NEON_MOCK_WEBGL==='1')await page.addInitScript(()=>{
        const getContext=HTMLCanvasElement.prototype.getContext;
        HTMLCanvasElement.prototype.getContext=function(type,...args){
          if(type!=='webgl')return getContext.call(this,type,...args);
          let next=1;const enums={},attributes={};
          const gl={canvas:this,getShaderParameter:()=>true,getProgramParameter:()=>true,getShaderInfoLog:()=>'',getProgramInfoLog:()=>'',getError:()=>0,getUniformLocation:(_p,n)=>n,getAttribLocation:(_p,n)=>attributes[n]??(attributes[n]=Object.keys(attributes).length),createShader:()=>({}),createProgram:()=>({}),createBuffer:()=>({})};
          return new Proxy(gl,{get(object,key){if(key in object)return object[key];if(typeof key==='string'&&key===key.toUpperCase())return enums[key]??(enums[key]=next++);return ()=>{};}});
        };
        HTMLCanvasElement.prototype.requestPointerLock=function(){};
      });
      if (fixtures) await page.addInitScript(base => {window.NEON_ASSET_CONFIG={ASSET_BASE_URL:base,UI_ASSET_BASE_URL:'./assets',version:''};}, origin+'/assets');
      else await page.addInitScript(()=>{window.NEON_ASSET_CONFIG={ASSET_BASE_URL:'',UI_ASSET_BASE_URL:'./assets',version:''};});
      await page.goto(origin+'/game/neon-arena/');
      await page.waitForFunction(()=>!!window.__NEON_TEST__,null,{timeout:8000}).catch(error=>{throw new Error(device.name+' failed to initialize: '+errors.join('; ')+' '+error.message)});
      await page.waitForFunction(()=>document.querySelector('.menuLogo').classList.contains('asset-loaded'));assert.equal(await page.evaluate(()=>getComputedStyle(document.querySelector('.menuLogo img')).display),'block');
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,device.name+' overflow');
      assert.equal(await page.locator('[data-mode]').count(),5,'five selectable modes');
      for (const mode of ['dm','tdm','frag','siege','survivor']) {
        await page.locator('[data-mode="'+mode+'"]').click();
        await page.locator('#start').click();
        await page.waitForFunction(()=>__NEON_TEST__.game.started&&!__NEON_TEST__.game.over);
        const state=await page.evaluate(()=>{const t=__NEON_TEST__;t.draw();return {mode:t.game.mode,input:t.game.p1Input,finite:Number.isFinite(t.player.x+t.player.y+t.player.z),error:t.gl.getError()}});
        assert.equal(state.mode,mode);assert.equal(state.finite,true);assert.equal(state.error,0);
        if(device.touch)assert.equal(state.input,'touch');
        await page.evaluate(()=>__NEON_TEST__.endMatch('Browser regression test'));
        assert.equal(await page.locator('#scoreboardOverlay').isVisible(),true);
        if(mode==='survivor')await page.locator('[data-survival-action="menu"]').click();else await page.locator('#returnMenu').click();
      }
      // Every normal arena must start and preserve two separate local actors.
      for(const map of ['grid','pastel','reactor']) {
        await page.locator('[data-mode="dm"]').click();
        await page.evaluate(map=>{const select=document.getElementById('map');select.value=map;select.dispatchEvent(new Event('change'));document.getElementById('secondPlayer').value='keyboard';},map);
        await page.locator('#start').click();await page.waitForFunction(()=>__NEON_TEST__.game.started&&!__NEON_TEST__.game.over);
        assert.equal(await page.evaluate(()=>{const t=__NEON_TEST__;t.draw();return t.player2!==t.player&&Number.isFinite(t.player2.x+t.player2.y+t.player2.z)&&t.gl.getError()===0}),true,map+' split');
        await page.evaluate(()=>__NEON_TEST__.endMatch('Split test'));
        await page.locator('#returnMenu').click();
      }
      await page.evaluate(()=>{document.getElementById('secondPlayer').value='off';});
      await page.locator('#start').click();await page.waitForFunction(()=>__NEON_TEST__.game.started&&!__NEON_TEST__.game.over);
      if(device.touch) {
        assert.equal(await page.locator('#mobileControls').isVisible(),true);
        assert.equal(await page.locator('#mobileJump').getAttribute('aria-label'),'Jump');
        assert.equal(await page.evaluate(()=>getComputedStyle(document.getElementById('feed')).display),'none');
        // A cancel must never build; short tap and held picker are separate paths.
        const before=await page.evaluate(()=>__NEON_TEST__.humanBuildState(__NEON_TEST__.player).charges);
        const bounds=await page.locator('#mobileBuild').boundingBox(),cdp=await context.newCDPSession(page);
        await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:bounds.x+bounds.width/2,y:bounds.y+bounds.height/2}]});
        await cdp.send('Input.dispatchTouchEvent',{type:'touchCancel',touchPoints:[]});
        assert.equal(await page.evaluate(()=>__NEON_TEST__.humanBuildState(__NEON_TEST__.player).charges),before);
        await cdp.detach();
      }
      const weapons = await page.evaluate(()=>{const t=__NEON_TEST__;for(const key of Object.keys(t.WEAPONS)){t.player.weapon=key;t.draw();}return {count:Object.keys(t.WEAPONS).length,error:t.gl.getError()}});
      assert.equal(weapons.count,14);assert.equal(weapons.error,0);
      if(fixtures){
        await page.evaluate(()=>{for(const path of [...Object.values(NeonAssets.weapons),...Object.values(NeonAssets.characters)])__NEON_TEST__.models.get(path)});
        await page.waitForFunction(()=>__NEON_TEST__.models.cache.status().parsed===48);
        const stats=await page.evaluate(()=>{const t=__NEON_TEST__;t.game.over=true;for(const key of Object.keys(t.WEAPONS)){t.player.weapon=key;t.draw();}return t.models.stats()});
        assert.equal(stats.parsed,48);assert.equal(stats.failed,0);assert.ok(stats.gpuModels>=14,'cached GPU weapon resources');
      }
      assert.deepEqual(errors,[],device.name+' runtime errors');
      reports.push({device:device.name,modes:5,splitMaps:3,weapons:14,errors:0});
      await context.close();
    }
    console.log(JSON.stringify({passed:reports,assets:fixtures?'local supplied GLBs':'hosted GLBs',webgl:process.env.NEON_MOCK_WEBGL==='1'?'mock (DOM/input/simulation only)':'real'},null,2));
  } finally {await browser.close();await new Promise(resolve=>server.close(resolve));}
}
main().catch(error=>{console.error(error);process.exitCode=1});
