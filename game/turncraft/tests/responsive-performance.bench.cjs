'use strict';
// Comparable baseline/current fixture. TURNCRAFT_BENCH_REAL_ART=1 uses the
// verified artwork cache; the default isolates simulation/protocol cost.
const fs=require('node:fs'),path=require('node:path'),{chromium}=require('playwright');
const repo=path.resolve(__dirname,'../../..');
const input=process.env.TURNCRAFT_BENCH_SOURCE||path.join(repo,'game/turncraft.html');
const formation=process.env.TURNCRAFT_BENCH_FORMATION==='1',orders=formation?'orderFormation(entities.map(e=>e.id),100,24,0);':'for(const u of entities)issueMoveOrder(u,100,u.y);';
const realArt=process.env.TURNCRAFT_BENCH_REAL_ART==='1',assetPath=realArt?require('./helpers/cloudflare-assets.cjs').assetPath:null;
const baselineRef=process.env.TURNCRAFT_BENCH_BASE_REF||'787753f316cacbeb7b5a8225c4da5fe26fd189b6';
const baselineAnimation=process.env.TURNCRAFT_BENCH_SOURCE?require('node:child_process').execFileSync('git',['show',`${baselineRef}:game/turncraft/animation.js`],{cwd:repo}):null;
const expose=`window.tcBench={run:source=>eval(source)};window.benchClock=100000;nowMs=()=>benchClock;resize();window.TurncraftStartup?.complete();`;
const source=fs.readFileSync(input,'utf8').replace(/^import .*;$/gm,'').replace('\nboot();','\n'+expose).replace('<script type="module">','<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/6kQAAAAASUVORK5CYII=','base64');
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']}),page=await browser.newPage({viewport:{width:1360,height:900}}),errors=[];
 page.on('pageerror',error=>errors.push(error.message));
 await page.route('**/*',route=>{const request=route.request(),url=new URL(request.url()),art=assetPath?.(url.href);if(art)return route.fulfill({path:art,contentType:'image/webp'});if(request.resourceType()==='image'&&(!realArt||url.hostname!=='turncraft.test'))return route.fulfill({body:png,contentType:'image/png'});if(url.pathname==='/game/turncraft.html')return route.fulfill({body:source,contentType:'text/html'});if(baselineAnimation&&url.pathname==='/game/turncraft/animation.js')return route.fulfill({body:baselineAnimation,contentType:'text/javascript'});const file=path.resolve(repo,'.'+decodeURIComponent(url.pathname));if(file.startsWith(repo+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file});return route.fulfill({status:404,body:''});});
 try{
  await page.goto('http://turncraft.test/game/turncraft.html');
  await page.waitForFunction(()=>!!window.tcBench);
  if(realArt)await page.evaluate(()=>tcBench.run(`spriteRuntime.loadManifest('/game/turncraft/assets/animation-manifest.json')`));
  const results=[];
  for(const count of [128,256]){
   await page.evaluate(()=>tcBench.run(`benchClock=100000;if(spriteRuntime)spriteRuntime.reset();particles=[];shots=[];floating=[];shatters=[];`));
   await page.evaluate(({count,orders})=>tcBench.run(`chosenMapId='rift-dominion';setBoardDimensions();mapSeed=71;activeMapSeed=71;terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));entities=[];resourceNodes=[];nextEntityId=1;players=[makePlayer(0,'Bench','frontier',false),makePlayer(1,'Foe','ascendant',false)];gameStarted=true;gameOver=false;paused=false;PLAYER=0;lobbyId='';modeState=null;areaEffects=[];pendingImpacts=[];temporaryObstacles=[];scanEffects=[];tileVisibleToFaction=()=>true;spectating=true;overlay.style.display='none';for(let i=0;i<${count};i++)addEntity('unit',['rifleman','medic','siegeTank','goliath'][i%4],0,5+i%16,15+Math.floor(i/16));${orders}cam.zoom=.4;centerOn(64,32);if(spriteRuntime)spriteRuntime.quality='medium';`),{count,orders});
   if(realArt){await page.evaluate(()=>tcBench.run(`cam.zoom=.65;centerOn(20,25);draw();terrainRenderer.load(chosenMapId)`));try{await page.waitForFunction(()=>tcBench.run(`spriteRuntime.cache.stats().pending===0&&spriteRuntime.cache.stats().ready>0&&terrainRenderer.stats.loaded.length>=3`));}catch(error){throw new Error(error.message+' '+JSON.stringify(await page.evaluate(()=>tcBench.run(`({sprites:spriteRuntime.cache.stats(),terrain:terrainRenderer.stats})`))))}}
   const measured=await page.evaluate(()=>tcBench.run(`(()=>{const samples=[],draws=[],updates=[];for(let i=0;i<240;i++){benchClock+=1000/60;const start=performance.now();updatePersistentOrders();updateCombat(1/60);resolveProjectileImpacts();const middle=performance.now();draw();const end=performance.now();if(i>=30){samples.push(end-start);draws.push(end-middle);updates.push(middle-start)}}const stats=a=>{a.sort((x,y)=>x-y);return{medianMs:a[Math.floor(a.length*.5)],p95Ms:a[Math.floor(a.length*.95)]}};const bytes=new TextEncoder().encode(JSON.stringify(serializeState())).length;return{units:entities.length,frame:stats(samples),draw:stats(draws),simulation:stats(updates),snapshotBytes:bytes,writeIntervalMs:STATE_WRITE_MS,maxStateWritesPerSecond:1000/STATE_WRITE_MS,navigationBuilds:navigationCache.builds,navigationHits:navigationCache.hits}})()`));
   measured.raf=await page.evaluate(()=>tcBench.run(`(async()=>{const gaps=[];let previous=await new Promise(requestAnimationFrame),first=previous;for(let i=0;i<180;i++){const ts=await new Promise(requestAnimationFrame),dt=(ts-previous)/1000;previous=ts;benchClock+=Math.min(.05,dt)*1000;updatePersistentOrders();updateCombat(Math.min(.05,dt));resolveProjectileImpacts();draw();if(i>=20)gaps.push(ts-first);first=ts}gaps.sort((a,b)=>a-b);return{fps:1000*gaps.length/gaps.reduce((a,b)=>a+b,0),medianIntervalMs:gaps[Math.floor(gaps.length/2)],p95IntervalMs:gaps[Math.floor(gaps.length*.95)]}})()`));
   results.push(measured);
  }
  if(errors.length)throw new Error(errors.join('\n'));
  const out={fixture:`1360x900 headless Chromium; 128x128 open field; medium quality; ${realArt?'verified actual artwork':'placeholder artwork'}; ${process.env.TURNCRAFT_BENCH_FORMATION==='1'?'normal formation group command':'individual lane commands'}; 240 timed frames (30 warmup), then 180 requestAnimationFrame frames (20 warmup)`,source:path.basename(input),results};
  if(process.env.TURNCRAFT_BENCH_OUTPUT)fs.writeFileSync(process.env.TURNCRAFT_BENCH_OUTPUT,JSON.stringify(out,null,2)+'\n');
  console.log(JSON.stringify(out,null,2));
 }finally{await browser.close()}
})().catch(error=>{console.error(error);process.exitCode=1});
