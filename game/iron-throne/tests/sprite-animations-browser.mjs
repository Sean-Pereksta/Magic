// Defaults to synthetic fixtures; IRON_THRONE_SPRITE_DIR also validates an extracted art delivery.
import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {SPRITE_ANIMATIONS,SPRITE_PATHS} from '../sprite-animations.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright'),root=fileURLToPath(new URL('../../../',import.meta.url));
const production=process.env.IRON_THRONE_SPRITE_DIR?new Map(await Promise.all(SPRITE_PATHS.map(async p=>[p,await readFile(path.join(process.env.IRON_THRONE_SPRITE_DIR,p))]))):null;
const server=http.createServer(async(req,res)=>{
  try {
    const pathname=new URL(req.url,'http://localhost').pathname;
    if(pathname==='/sprite-test'){res.setHeader('Content-Type','text/html');res.end('<canvas id="map" style="width:640px;height:480px"></canvas>');return;}
    const file=path.resolve(root,'.'+pathname);if(!file.startsWith(root))throw Error('outside root');
    res.setHeader('Content-Type',({'.mjs':'text/javascript','.svg':'image/svg+xml','.json':'application/json'})[path.extname(file)]||'text/plain');res.end(await readFile(file));
  }catch{res.writeHead(404);res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
let browser;
try {
  browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox']});
  const maker=await browser.newPage();
  const images=await maker.evaluate(specs=>Object.fromEntries(Object.entries(specs).map(([family,entries])=>{
    const spec=Object.values(entries)[0],c=document.createElement('canvas');c.width=spec.cell.width*8;c.height=spec.cell.height*spec.rows;
    const ctx=c.getContext('2d');for(let row=0;row<spec.rows;row++)for(let col=0;col<8;col++){ctx.fillStyle=`rgb(${50+col*20},${40+row*70},90)`;ctx.fillRect(col*spec.cell.width+20,row*spec.cell.height+20,spec.cell.width-40,spec.cell.height-40);}
    return [family,c.toDataURL().split(',')[1]];
  })),SPRITE_ANIMATIONS);await maker.close();
  if(production) {
    const page=await browser.newPage();
    await page.route('https://pub-*.r2.dev/**',route=>{
      const body=production.get(new URL(route.request().url()).pathname.slice(1));
      return body?route.fulfill({contentType:'image/png',body}):route.fulfill({status:404,body:''});
    });
    await page.goto(`http://127.0.0.1:${server.address().port}/sprite-test`);
    const result=await page.evaluate(async ({specs,dataUrls})=>{
      const {AssetCache,loadArt,ironThronesAsset}=await import('/game/iron-throne/asset-manifest.mjs');
      const cache=new AssetCache(),canvas=document.createElement('canvas');let count=0;
      for(const spec of Object.values(specs).flatMap(g=>Object.values(g))) {
        const img=await loadArt(ironThronesAsset(spec.path));
        if(!img||img.naturalWidth!==spec.cell.width*8||img.naturalHeight!==spec.cell.height*spec.rows)throw Error(`Invalid delivered atlas: ${spec.path}`);
        // R2 drawing intentionally requires no CORS. Inspect the same bytes via
        // a data URL so pixel readback does not require changing bucket policy.
        const readable=new Image();readable.src=dataUrls[spec.path];await readable.decode();
        canvas.width=spec.cell.width;canvas.height=spec.cell.height;const ctx=canvas.getContext('2d',{willReadFrequently:true});
        for(const [action,a]of Object.entries(spec.actions)) {
          const seen=new Set();
          for(let f=0;f<8;f++) {
            ctx.clearRect(0,0,canvas.width,canvas.height);
            if(!cache.drawSprite(ctx,spec,action,(f+.1)*1000/a.fps,0,0,canvas.width,canvas.height))throw Error(`Draw failed: ${spec.path}`);
            canvas.width=canvas.width;
            ctx.drawImage(readable,f*spec.cell.width,a.row*spec.cell.height,spec.cell.width,spec.cell.height,0,0,canvas.width,canvas.height);
            const pixels=ctx.getImageData(0,0,canvas.width,canvas.height).data;let visible=0,empty=0;
            for(let y=0;y<canvas.height;y++)for(let x=0;x<canvas.width;x++){
              const alpha=pixels[(y*canvas.width+x)*4+3];if(alpha)visible++;else empty++;
              if((x<2||y<2||x>=canvas.width-2||y>=canvas.height-2)&&alpha)throw Error(`Frame boundary bleed: ${spec.path} ${action} ${f}`);
            }
            if(visible<100||empty<pixels.length/16)throw Error(`Empty or opaque frame: ${spec.path}`);
            seen.add(canvas.toDataURL());count++;
          }
          if(seen.size!==8)throw Error(`Repeated frames: ${spec.path} ${action}`);
        }
        if(!cache.drawSprite(ctx,spec,'idle',0,0,0,40,40,{color:'#ce4b45'}))throw Error(`Outline draw failed: ${spec.path}`);
      }
      return {sheets:Object.values(specs).reduce((n,g)=>n+Object.keys(g).length,0),frames:count,outlinePixels:cache.atlasOutlines.pixels};
    },{specs:SPRITE_ANIMATIONS,dataUrls:Object.fromEntries([...production].map(([p,b])=>[p,`data:image/png;base64,${b.toString('base64')}`]))});
    assert.equal(result.sheets,21);assert.equal(result.frames,456);assert.ok(result.outlinePixels<=16*1024*1024);
    console.log(`PASS delivered artwork: ${result.sheets} sheets, ${result.frames} unique-per-action cropped frames, transparency, clear cell boundaries, all ownership outlines`);
    await page.close();
  }
  for(const available of [false,true]) {
    const page=await browser.newPage(),errors=[],requests=[];page.on('pageerror',e=>errors.push(e.message));
    await page.route('https://pub-*.r2.dev/**',route=>{
      const pathname=new URL(route.request().url()).pathname.slice(1);requests.push(pathname);
      const body=available&&SPRITE_PATHS.includes(pathname)?production?.get(pathname)||Buffer.from(images[pathname.split('/')[1]],'base64'):null;
      return body?route.fulfill({contentType:'image/png',body}):route.fulfill({status:404,body:''});
    });
    await page.goto(`http://127.0.0.1:${server.address().port}/sprite-test`);
    await page.evaluate(async()=>{
      const {WorldMap}=await import('/game/iron-throne/map.mjs');
      const tiles=Object.fromEntries(Array.from({length:25},(_,i)=>{const q=i%5,r=Math.floor(i/5),id=`${q},${r}`;return [id,{id,q,r,terrain:q===4?'water':'plains',fog:'visible',owner:'ashen',levels:{},resource:null}];}));
      tiles['0,0'].building='city';tiles['0,0'].levels.city=1;tiles['1,1'].project={remaining:3,total:6};
      window.state={seed:1,turn:1,width:5,height:5,tiles,kingdoms:[{id:'ashen',color:'#ce4b45',sigil:'A'}],armies:[{id:'a',owner:'ashen',tile:'1,1',units:{levy:12},path:[],order:'hold'}],fleets:[{id:'f',owner:'ashen',tile:'4,1',node:'sea:4,1',ships:[{type:'warship'}],path:[],order:'hold',cargo:[]}],militaryEvents:[],wars:[],treaties:[]};
      window.map=new WorldMap(document.querySelector('canvas'),{getState:()=>window.state,onSelect:()=>{}});map.resize();map.center('2,2');
      window.draws=[];window.frames=0;window.durations=[];
      const render=map.render.bind(map);map.render=()=>{frames++;const start=performance.now();render();durations.push(performance.now()-start);};
      const drawSprite=map.assets.drawSprite.bind(map.assets);map.assets.drawSprite=(...args)=>{const ok=drawSprite(...args);draws.push({path:args[1].path,action:args[2],ok});return ok;};
    });
    await page.waitForTimeout(1400);
    assert.deepEqual(errors,[]);
    const initial=await page.evaluate(()=>({draws,hits:map.hits}));
    assert.ok(initial.hits.some(h=>h.tile==='1,1'));assert.ok(initial.hits.some(h=>h.tile==='4,1'));
    assert.equal(initial.draws.some(d=>d.ok),available);
    assert.ok(initial.draws.some(d=>d.path==='sprites/construction/stage_2.png'));
    const sprites=requests.filter(p=>p.startsWith('sprites/'));assert.equal(new Set(sprites).size,sprites.length,'share image requests across frames');
    await page.evaluate(()=>{frames=0;draws=[];});await page.waitForTimeout(1100);
    const idleFrames=await page.evaluate(()=>frames);assert.ok(idleFrames<=12,`idle whole-map renders must be throttled: ${idleFrames}`);
    await page.evaluate(()=>{draws=[];state.armies[0].tile='2,1';map.draw();});await page.waitForTimeout(200);
    assert.ok((await page.evaluate(()=>draws)).some(d=>d.path.includes('/troops/')&&d.action==='march'));
    await page.evaluate(()=>{draws=[];state.militaryEvents.push({id:1,turn:1,action:'battle',attacker:'ashen',defender:'wintermere',attackerArmyId:'a',tile:'2,1'});map.draw();});await page.waitForTimeout(200);
    assert.ok((await page.evaluate(()=>draws)).some(d=>d.path.includes('/troops/')&&d.action==='attack'));
    await page.waitForTimeout(2200);await page.evaluate(()=>{draws=[];map.draw();});await page.waitForTimeout(100);
    assert.ok((await page.evaluate(()=>draws)).some(d=>d.path.includes('/troops/')&&d.action==='idle'));
    await page.evaluate(()=>{state.tiles['1,1'].project=null;state.tiles['2,1'].fog='unknown';state.tiles['4,1'].fog='explored';draws=[];map.draw();});await page.waitForTimeout(250);
    assert.equal((await page.evaluate(()=>draws)).length,0,'fog and completed construction must stop animation rendering');
    await page.evaluate(()=>{state.tiles['2,1'].fog='visible';state.tiles['4,1'].fog='visible';map.draw();});
    await page.emulateMedia({reducedMotion:'reduce'});await page.waitForTimeout(300);await page.evaluate(()=>{frames=0;});await page.waitForTimeout(600);
    assert.equal(await page.evaluate(()=>frames),0,'reduced motion must not idle-redraw');
    await page.emulateMedia({reducedMotion:'no-preference'});await page.waitForTimeout(100);
    await page.evaluate(()=>{Object.defineProperty(document,'hidden',{configurable:true,value:true});document.dispatchEvent(new Event('visibilitychange'));frames=0;});await page.waitForTimeout(350);
    assert.equal(await page.evaluate(()=>frames),0,'hidden tab cancels coordinated animation');
    await page.evaluate(()=>{Object.defineProperty(document,'hidden',{configurable:true,value:false});document.dispatchEvent(new Event('visibilitychange'));});await page.waitForTimeout(150);
    assert.ok(await page.evaluate(()=>frames>0));assert.deepEqual(errors,[]);
    console.log(`PASS sprites ${available?'available':'missing'}: cropping, movement, battle, expiry, construction, fog, badges, throttling, reduced motion, hidden/resume; idle renders=${idleFrames}`);
    if(available) {
      const result=await page.evaluate(()=>{
        state.armies=Array.from({length:500},(_,i)=>({id:`army-${i}`,tile:`${i%4},${Math.floor(i/4)%5}`,owner:'ashen',units:{levy:12},path:[],order:'hold'}));
        map.render();durations=[];for(let n=0;n<20;n++)map.render();return {average:durations.reduce((a,b)=>a+b,0)/durations.length,outlinePixels:map.assets.atlasOutlines.pixels};
      });
      assert.ok(result.outlinePixels<=16*1024*1024);console.log(`500-army grouped-render sample: ${result.average.toFixed(2)}ms/frame, ${result.outlinePixels} cached outline pixels`);
    }
    await page.close();
  }
}finally{await browser?.close();await new Promise(r=>server.close(r));}
