// Hosted asset delivery tests. Requests are intercepted; no asset host is contacted.
const fs=require('node:fs');
const path=require('node:path');
const {pathToFileURL}=require('node:url');
module.exports=async function assetsBrowserChecks(browser,scratch,check,assert){
  const root=path.resolve(process.env.OHK_ART_ROOT||path.join(__dirname,'../outputs/one-hit-knight-arcade'));
  const manifest=JSON.parse(fs.readFileSync(path.join(root,'manifest.json'),'utf8'));
  const knightPath=manifest.sprites.knight.image;
  const cache=new Map(),contexts=[],summaries=[];
  const headers={'access-control-allow-origin':'*','cache-control':'no-store'};
  async function openCase(state={}){
    const context=await browser.newContext({viewport:{width:960,height:640}});contexts.push(context);
    await context.addInitScript(()=>{window.ONE_HIT_KNIGHT_ASSET_BASE='https://ohk-art.test/';window.ONE_HIT_KNIGHT_ASSET_VERSION='arcade-test';});
    await context.route('https://www.gstatic.com/firebasejs/**',route=>route.fulfill({status:200,contentType:'text/javascript',body:'/* offline scoring test */'}));
    const requests=[],consoleErrors=[];
    await context.route('https://ohk-art.test/**',async route=>{
      const url=new URL(route.request().url()),relative=decodeURIComponent(url.pathname).replace(/^\//,'');requests.push(relative);
      if(relative==='manifest.json'){
        if(state.manifestDelay)await new Promise(resolve=>setTimeout(resolve,state.manifestDelay));
        if(state.failManifest)return route.fulfill({status:503,headers,body:'Asset host unavailable'});
        const body=JSON.stringify(state.emptyManifest?{version:'broken',sprites:{}}:state.invalidManifest?{...manifest,version:''}:state.changedPath?{...manifest,sprites:{...manifest.sprites,knight:{...manifest.sprites.knight,image:'missing-knight.png'}}}:manifest);
        return route.fulfill({status:200,contentType:'application/json',headers,body});
      }
      if(relative==='missing-knight.png'||state.failImage&&relative===knightPath)return route.fulfill({status:503,headers,body:'Image delivery failed'});
      const filename=path.resolve(root,relative);assert(filename.startsWith(root+path.sep),'requested image remains in package');
      if(!cache.has(filename))cache.set(filename,fs.readFileSync(filename));
      return route.fulfill({status:200,contentType:relative.endsWith('.png')?'image/png':'application/octet-stream',headers,body:cache.get(filename)});
    });
    const page=await context.newPage();page.on('console',message=>{if(message.type()==='error')consoleErrors.push(message.text());});
    await page.goto(pathToFileURL(path.resolve(scratch)).href);await page.waitForFunction(()=>window.OHKAssets);
    await page.evaluate(async()=>{await OHKAssets.ready;return true;});
    return {page,requests,consoleErrors,state};
  }
  async function pixels(page){return page.evaluate(()=>{
    const canvas=document.createElement('canvas');canvas.width=128;canvas.height=128;const c=canvas.getContext('2d');
    const drawn=OHKAssets.draw(c,'knight','idle',64,105,{scale:2,dir:0,progress:0});
    return {drawn,pixels:Array.from(c.getImageData(0,0,128,128).data).filter((v,i)=>i%4===3&&v>0).length};
  });}
  try{
    await check('hosted manifest and real PNG critical boot',async()=>{
      const c=await openCase();assert.equal(await c.page.evaluate(()=>OHKAssets.hosted),true);assert(c.requests.includes('manifest.json'));assert(c.requests.includes(knightPath));
      const visible=await pixels(c.page);assert.equal(visible.drawn,true);assert(visible.pixels>300,'actual knight pixels render from hosted PNG');
      await c.page.evaluate(async()=>{await OHKAssets.prepareWave(11);return true;});
      assert(c.requests.some(p=>/graveyard|goblin-camp/.test(p)),'future arena images preload before introduction');summaries.push({case:'hosted',requests:c.requests.length,knightPixels:visible.pixels});
    });
    await check('hosted image failure uses included art and exposes retry',async()=>{
      const c=await openCase({failImage:true});assert.equal(await c.page.evaluate(()=>OHKAssets.hosted),true);assert((await c.page.evaluate(()=>OHKAssets.errors)).includes(knightPath));
      const visible=await pixels(c.page);assert(visible.drawn&&visible.pixels>300);assert(c.consoleErrors.some(e=>e.includes('[One Hit Knight assets]')&&e.includes(knightPath)));
      assert(await c.page.locator('#ohkAssetStatus').isVisible());assert.equal(await c.page.locator('#ohkAssetStatus [data-retry]').count(),1);
      c.state.failImage=false;c.state.manifestDelay=180;
      await c.page.evaluate(()=>{window.__ohkAssetRetry=OHKAssets.retry();return true;});
      const during=await pixels(c.page);assert(during.drawn&&during.pixels>300,'retry retains loaded sprites while host request is pending');
      await c.page.evaluate(async()=>{await window.__ohkAssetRetry;return true;});assert.equal((await c.page.evaluate(()=>OHKAssets.errors)).length,0);
      const recovered=await pixels(c.page);assert(recovered.drawn&&recovered.pixels>300);summaries.push({case:'image fallback and retry',fallbackPixels:visible.pixels,retryPixels:during.pixels});
    });
    await check('hosted manifest failure falls back to included package',async()=>{
      const c=await openCase({failManifest:true});assert.equal(await c.page.evaluate(()=>OHKAssets.hosted),false);assert(c.consoleErrors.some(e=>e.includes('manifest.json')));const visible=await pixels(c.page);assert(visible.drawn&&visible.pixels>300);assert(await c.page.locator('#ohkAssetStatus').isVisible());summaries.push({case:'manifest fallback',knightPixels:visible.pixels});
    });
    await check('invalid unversioned manifest is rejected with included-art recovery',async()=>{
      const c=await openCase({invalidManifest:true});assert.equal(await c.page.evaluate(()=>OHKAssets.hosted),false);assert(c.consoleErrors.some(e=>e.includes('version and sprites')));const visible=await pixels(c.page);assert(visible.drawn&&visible.pixels>300);summaries.push({case:'invalid manifest fallback',knightPixels:visible.pixels});
    });
    await check('incomplete critical manifest falls back before a run starts',async()=>{const c=await openCase({emptyManifest:true});assert.equal(await c.page.evaluate(()=>OHKAssets.hosted),false);assert(c.consoleErrors.some(e=>e.includes('critical sprite')));const visible=await pixels(c.page);assert(visible.drawn&&visible.pixels>300);summaries.push({case:'incomplete manifest fallback'});});
    await check('renamed missing critical PNG falls back to the complete included manifest',async()=>{const c=await openCase({changedPath:true});assert.equal(await c.page.evaluate(()=>OHKAssets.hosted),false);const visible=await pixels(c.page);assert(visible.drawn&&visible.pixels>300);summaries.push({case:'critical package fallback'});});
    return summaries;
  }finally{for(const context of contexts)await context.close();}
};
