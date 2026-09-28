// Canvas integration checks only; does not generate HTML previews or screenshots.
import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES ? `${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json` : import.meta.url);
const {chromium}=require('playwright'),root=fileURLToPath(new URL('../../../',import.meta.url));
const server=http.createServer(async(req,res)=>{
  try {
    const file=path.resolve(root,'.'+new URL(req.url,'http://localhost').pathname);
    if(!file.startsWith(root)) throw Error('outside root');
    const body=await readFile(file);
    res.writeHead(200,{'Content-Type':({'.mjs':'text/javascript','.html':'text/html','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'})[path.extname(file)]||'text/plain'});res.end(body);
  }catch{res.writeHead(404);res.end();}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
let browser;
try {
  browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
  for(const remote of (process.env.IRON_GEOGRAPHY_PACK?[false,true]:[false])) for(const width of [1280,390]) {
    const page=await browser.newPage({viewport:{width,height:844}}),errors=[];
    page.on('pageerror',e=>errors.push(e.message));
    // Pixel assertions need origin-clean test images; the production map only displays them.
    if(remote) await page.addInitScript(()=>{const OriginalImage=Image;window.Image=class extends OriginalImage {constructor(...args){super(...args);this.crossOrigin='anonymous';}};});
    await page.route('https://pub-*.r2.dev/**',async route=>{
      const pathname=new URL(route.request().url()).pathname;
      if(remote&&pathname.startsWith('/geography-v2/')) return route.fulfill({headers:{'Access-Control-Allow-Origin':'*'},contentType:'image/png',body:await readFile(path.join(process.env.IRON_GEOGRAPHY_PACK,path.basename(pathname)))});
      return route.fulfill({status:404,body:''});
    });
    await page.route('**/config.json',route=>route.fulfill({json:{}}));
    await page.goto(`http://127.0.0.1:${server.address().port}/game/iron-throne/index.html`);
    await page.locator('#start-game').click();
    const result=await page.evaluate(async(remote)=>{
      const {GeographyArt,edgePort}=await import('../iron-throne/geography-art.mjs');
      const {HEX_DIRECTIONS,oppositeEdge,maskEdges}=await import('../iron-throne/geography.mjs');
      const {AssetCache,ART,loadArt}=await import('../iron-throne/asset-manifest.mjs');
      const assets=remote?new AssetCache():undefined;
      if(remote) await Promise.all(Object.values(ART.geography).map(url=>loadArt(url)));
      const canvas=document.createElement('canvas');canvas.width=canvas.height=512;
      const c=canvas.getContext('2d'),art=new GeographyArt(),failures=[];
      const paint=(g,x,y)=>art.draw(c,g,x,y,assets,ART.geography);
      const seaAt=(x,y)=>{
        // The illustrated water contains white foam. Require blue water in
        // a 5px neighborhood (1.25 map units), smaller than the river width.
        const pixels=c.getImageData(Math.round(x)-2,Math.round(y)-2,5,5).data;
        for(let i=0;i<pixels.length;i+=4)
          if(pixels[i+2]>pixels[i]+10&&pixels[i+1]>pixels[i]+25) return true;
        return false;
      };
      // Sample every shared river port on both sides of all six boundaries.
      for(let edge=0;edge<6;edge++) {
        c.setTransform(1,0,0,1,0,0);c.fillStyle='#87935e';c.fillRect(0,0,512,512);
        c.setTransform(4,0,0,4,256,256);
        const [q,r]=HEX_DIRECTIONS[edge],dx=25*Math.sqrt(3)*(q+r/2),dy=37.5*r;
        paint({coastMask:0,riverMask:1<<edge,mouthMask:0,hasRiver:true,ground:'plains'},0,0);
        paint({coastMask:0,riverMask:1<<oppositeEdge(edge),mouthMask:0,hasRiver:true,ground:'plains'},dx,dy);
        const port=edgePort(edge),length=Math.hypot(...port);
        for(const offset of [-1,0,1]) {
          const x=256+4*(port[0]+port[0]/length*offset),y=256+4*(port[1]+port[1]/length*offset);
          if(!seaAt(x,y)) failures.push(`river ${edge}/${offset}`);
        }
      }
      // Every supported illustrated river shape must meet its stated ports.
      for(const mask of [1,2,4,8,16,32,5,10,20,40,17,34,9,18,36,21,42]) {
        c.setTransform(1,0,0,1,0,0);c.fillStyle='#87935e';c.fillRect(0,0,512,512);c.setTransform(4,0,0,4,256,256);
        paint({coastMask:0,riverMask:mask,mouthMask:0,hasRiver:true,ground:'plains'},0,0);
        for(const edge of maskEdges(mask)) {
          const p=edgePort(edge),k=(Math.hypot(...p)-.75)/Math.hypot(...p);
          if(!seaAt(256+4*p[0]*k,256+4*p[1]*k)) failures.push(`shape ${mask}/edge ${edge}`);
        }
      }
      // Mouths must cut through the beach on every orientation.
      for(let edge=0;edge<6;edge++) {
        c.setTransform(1,0,0,1,0,0);c.fillStyle='#87935e';c.fillRect(0,0,512,512);c.setTransform(4,0,0,4,256,256);
        paint({coastMask:1<<edge,riverMask:(1<<edge)|(1<<oppositeEdge(edge)),mouthMask:1<<edge,hasRiver:true,ground:'plains'},0,0);
        const port=edgePort(edge),length=Math.hypot(...port);
        for(const radius of [9,13,16,19,21]) if(!seaAt(256+4*port[0]/length*radius,256+4*port[1]/length*radius)) failures.push(`mouth ${edge}/${radius}`);
      }
      // Generate all shore masks plus enough combinations to exercise eviction.
      for(let coastMask=0;coastMask<64;coastMask++) for(let riverMask=0;riverMask<6;riverMask++) paint({coastMask,riverMask,hasRiver:!!riverMask,mouthMask:0,ground:'plains'},0,0);
      return {failures,cacheSize:art.cache.size};
    },remote);
    if(remote) assert.ok(result.cacheSize>0,'unsupported masks retain native fallback');
    assert.deepEqual(result.failures,[]);assert.ok(result.cacheSize<=256);assert.deepEqual(errors,[]);
    await page.locator('#home').click();await page.locator('#map').click({position:{x:100,y:100}});
    assert.deepEqual(errors,[]);
    console.log(`PASS ${width}px ${remote?'Cloudflare PNGs':'fallback'}: campaign rendering, reciprocal river pixels, six river mouths, all shoreline masks, bounded cache, map selection`);
    await page.close();
  }
}finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
