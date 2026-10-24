import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile,mkdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {createRequire} from 'node:module';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright');
const root=path.resolve(fileURLToPath(new URL('../../../',import.meta.url)));
const remotePackURL='https://pub-47f679f65f034fbda4c4b2ee31b3818a.r2.dev/battle/v1/pack.json';
const packBody=await readFile(path.join(root,'game/iron-throne/assets/battle/v1/pack.json'),'utf8');
const server=http.createServer(async(req,res)=>{try{const url=new URL(req.url,'http://localhost');if(url.pathname==='/test'){res.writeHead(200,{'Content-Type':'text/html'});res.end('<style>html,body{margin:0;height:100%;background:#123}canvas{width:100%;height:100%}</style><canvas id="battle"></canvas>');return;}const filename=path.resolve(root,'.'+url.pathname);if(!filename.startsWith(root+path.sep))throw Error('outside root');const data=await readFile(filename);res.writeHead(200,{'Content-Type':filename.endsWith('.mjs')?'text/javascript':filename.endsWith('.json')?'application/json':'application/octet-stream'});res.end(data);}catch{res.writeHead(404);res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
try{
  const page=await browser.newPage({viewport:{width:1280,height:800}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  let remoteStatus=200,remoteRequests=0,localRequests=0;
  page.on('request',request=>{if(new URL(request.url()).pathname==='/game/iron-throne/assets/battle/v1/pack.json')localRequests++;});
  await page.route(remotePackURL,route=>{remoteRequests++;return route.fulfill({status:remoteStatus,headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'},body:remoteStatus===200?packBody:'unavailable'});});
  await page.goto(`http://127.0.0.1:${server.address().port}/test`);
  const result=await page.evaluate(async()=>{
    const {createBattleRenderer}=await import('/game/iron-throne/battle-renderer.mjs'),{UNITS,CAMPAIGN_HOUSES}=await import('/game/iron-throne/data.mjs');
    const types=Object.keys(UNITS),units=types.map((type,i)=>({id:'u'+i,armyId:'a',type,family:UNITS[type].family,owner:CAMPAIGN_HOUSES[i%12].id,side:0,status:'active',x:(i%5-2)*4,z:Math.floor(i/5)*6,y:0,facing:Math.PI,hp:100,maxHp:100,speed:1,action:i%3===0?{kind:'attack',elapsed:.3,duration:.8}:null}));
    window.battle={time:1,playerSide:0,commanderId:'u2',units,armies:[],projectiles:[],layout:{width:160,depth:160,terrain:'plains',obstacles:[{kind:'tree',id:'t1',x:-22,z:20,radius:1.4,height:7},{kind:'building',x:22,z:28,width:9,depth:8,height:6}],walls:[{id:'w',x:-12,z:26,width:16,depth:2,height:6,hp:180,maxHp:220,state:'damaged'},{id:'b',x:12,z:26,width:16,depth:2,height:6,hp:0,maxHp:220,state:'breached'}],gate:{x:0,z:26,width:8,depth:2,height:6,hp:100,maxHp:150},ladders:[],objective:{x:0,z:38}}};
    window.renderer=await createBattleRenderer(document.querySelector('canvas'),battle);
    renderer.render(battle,{mode:'tactical',target:{x:0,y:1,z:5},yaw:0,pitch:.6,distance:32});
    const point=renderer.pick(640,400);return{stats:renderer.stats,point};
  });
  assert.ok(result.stats.instanced);assert.equal(result.stats.models,16);assert.ok(result.point&&Math.abs(result.point.x)<.01);assert.ok(result.stats.drawCalls>15);
  assert.equal(remoteRequests,1,'a battlefield loads the default Cloudflare pack');
  assert.equal(localRequests,0,'a successful default remote load does not fetch the local fallback');
  if(process.env.BATTLE_SCREENSHOT){await mkdir(path.dirname(process.env.BATTLE_SCREENSHOT),{recursive:true});await page.screenshot({path:process.env.BATTLE_SCREENSHOT});}
  const wallPick=await page.evaluate(()=>{
    battle.layout.stairs=[{id:'w-stairs',wallId:'w',x:-12,z:32,topX:-12,topZ:26,height:6,width:3,depth:6}];
    battle.units.push({...battle.units[4],id:'parapet-defender',type:'archer',family:'ranged',x:-12,z:26,y:6,wallId:'w',speed:0,action:null});
    battle.units.push({...battle.units[2],id:'climbing-stairs',type:'menAtArms',family:'infantry',mounted:false,x:-12,z:29,y:3,wallId:null,speed:0,action:{kind:'stairs',ascending:true,elapsed:2,duration:4,fromX:-12,fromZ:32,targetX:-12,targetZ:26}});
    renderer.render(battle,{mode:'tactical',target:{x:-12,y:6.95,z:26},yaw:Math.PI,pitch:.5,distance:23});
    return renderer.pick(640,400);
  });
  assert.equal(wallPick?.unitId,'parapet-defender','wall defenders are picked at their visible elevated position');assert.equal(wallPick.y,6);
  if(process.env.BATTLE_SCREENSHOT)await page.screenshot({path:process.env.BATTLE_SCREENSHOT.replace(/\.png$/, '-stairs.png')});
  await page.evaluate(()=>{Object.assign(battle.layout.walls[0],{hp:0,state:'breached'});renderer.render(battle,{mode:'tactical',target:{x:-12,y:0,z:26},yaw:Math.PI,pitch:.7,distance:23});});
  if(process.env.BATTLE_BENCH){
    const benchmark=await page.evaluate(async()=>{
      const {createBattle}=await import('/game/iron-throne/battle-sim.mjs');
      const units={levy:8,spearman:8,menAtArms:8,heavyInfantry:6,archer:8,crossbow:6,lightCavalry:8,knight:4,ram:2,catapult:1,trebuchet:1};
      const sample=createBattle({id:'renderer-benchmark',seed:321,playerOwner:'ashen',playerSide:0,tile:{terrain:'plains',walls:0,fortIntegrity:0},participants:[{armyId:'a',owner:'ashen',side:0,units,sourceTile:'a',morale:1,entryDirection:'north'},{armyId:'d',owner:'wintermere',side:1,units,sourceTile:'d',morale:1,entryDirection:'south'}]});
      const gl=document.querySelector('canvas').getContext('webgl'),times=[];
      for(let i=0;i<25;i++){sample.time=i*.05;const started=performance.now();renderer.render(sample,{mode:'tactical',target:{x:0,y:0,z:0},yaw:0,pitch:1.1,distance:100});gl.finish();if(i>=5)times.push(performance.now()-started);}
      times.sort((a,b)=>a-b);
      return{activeUnits:sample.units.filter(u=>u.status==='active').length,frames:times.length,medianMs:+times[Math.floor(times.length/2)].toFixed(2),p95Ms:+times[Math.floor(times.length*.95)].toFixed(2),renderer:'headless Chromium / SwiftShader software WebGL',viewport:[innerWidth,innerHeight]};
    });console.log('Observed renderer benchmark:',JSON.stringify(benchmark));
  }
  await page.evaluate(()=>{battle.time+=.3;renderer.render(battle,{mode:'third',target:{x:0,y:0,z:0},yaw:Math.PI,pitch:.3,distance:7});renderer.dispose();renderer.dispose();});
  assert.deepEqual(errors,[]);
  const downloaded=await page.evaluate(async()=>{const {downloadBattleAssets,BATTLE_ASSET_BASE,BATTLE_ASSET_PATH}=await import('/game/iron-throne/battle-assets.mjs');const result=await downloadBattleAssets();const cache=await caches.open('iron-thrones-battle-v1');return{...result,remoteCached:!!await cache.match(`${BATTLE_ASSET_BASE}/${BATTLE_ASSET_PATH}`)};});
  assert.equal(downloaded.cached,true,'Download battlefield models accepts the default remote cache');assert.equal(downloaded.remoteCached,true);assert.equal(downloaded.models,16);
  assert.equal(remoteRequests,2,'explicit download refreshes the remote package');assert.equal(localRequests,0);
  // A populated remote asset cache permits a new battlefield while disconnected.
  await page.context().setOffline(true);
  const cached=await page.evaluate(async()=>{const {loadBattleAssets,releaseBattleAssets}=await import('/game/iron-throne/battle-assets.mjs');releaseBattleAssets();return(await loadBattleAssets()).version;});assert.equal(cached,1);
  assert.equal(remoteRequests,2,'an offline cache hit needs no remote request');assert.equal(localRequests,0);
  await page.context().setOffline(false);
  remoteStatus=503;
  const fallback=await page.evaluate(async()=>{const {loadBattleAssets,clearBattleAssetCache}=await import('/game/iron-throne/battle-assets.mjs');await clearBattleAssetCache();return(await loadBattleAssets({retry:true})).version;});
  assert.equal(fallback,1,'a failed Cloudflare request falls back to the bundled package');assert.equal(remoteRequests,3);assert.equal(localRequests,1);
  await page.route('**/assets/battle/v1/pack.json',route=>route.fulfill({status:503,body:'unavailable'}));
  const message=await page.evaluate(async()=>{const {loadBattleAssets,clearBattleAssetCache}=await import('/game/iron-throne/battle-assets.mjs');await clearBattleAssetCache();try{await loadBattleAssets({retry:true});return null;}catch(e){return e.message;}});assert.match(message,/Retry or resolve/);
  assert.equal(remoteRequests,4);assert.equal(localRequests,2);assert.deepEqual(errors,[]);
  console.log('Battle renderer: all 15 troop classes, instanced geometry, cameras, picking, disposal, default Cloudflare load/download, offline remote cache, local fallback, and failure recovery passed.');
}finally{await browser.close();await new Promise(r=>server.close(r));}
