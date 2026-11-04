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
const hardware=process.env.BATTLE_GPU==='hardware';
const args=hardware?['--use-gl=angle','--use-angle=d3d11','--ignore-gpu-blocklist','--disable-software-rasterizer']:['--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader'];
const browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox',...args]});
try{
  const page=await browser.newPage({viewport:{width:1280,height:800}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  let remoteStatus=200,remoteRequests=0,localRequests=0;
  page.on('request',request=>{if(new URL(request.url()).pathname==='/game/iron-throne/assets/battle/v1/pack.json')localRequests++;});
  await page.route(remotePackURL,route=>{remoteRequests++;return route.fulfill({status:remoteStatus,headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'},body:remoteStatus===200?packBody:'unavailable'});});
  await page.goto(`http://127.0.0.1:${server.address().port}/test`);
  const adapter=await page.evaluate(()=>{
    const gl=document.createElement('canvas').getContext('webgl');
    if(!gl)return null;const info=gl.getExtension('WEBGL_debug_renderer_info');
    return {vendor:gl.getParameter(info?info.UNMASKED_VENDOR_WEBGL:gl.VENDOR),renderer:gl.getParameter(info?info.UNMASKED_RENDERER_WEBGL:gl.RENDERER)};
  });
  console.log('WebGL adapter:',JSON.stringify(adapter));
  if(!adapter&&hardware){
    const cdp=await browser.newBrowserCDPSession(),info=await cdp.send('SystemInfo.getInfo');
    console.log('Detected graphics devices:',JSON.stringify(info.gpu?.devices));throw new Error('D3D11 WebGL is unavailable on this host.');
  }
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
  const deployment=await page.evaluate(()=>{
    const sample=structuredClone(battle);sample.phase='deployment';sample.layout.deploymentZones=[{side:0,minX:-66,maxX:66,minZ:-72,maxZ:-44},{side:1,minX:-66,maxX:66,minZ:44,maxZ:72}];
    const camera={mode:'tactical',target:{x:0,y:0,z:0},yaw:0,pitch:1.45,distance:145};renderer.render(sample,camera);const visible=renderer.stats.deploymentZones;
    const gl=document.querySelector('canvas').getContext('webgl'),pixels=new Uint8Array(gl.drawingBufferWidth*gl.drawingBufferHeight*4);gl.readPixels(0,0,gl.drawingBufferWidth,gl.drawingBufferHeight,gl.RGBA,gl.UNSIGNED_BYTE,pixels);let tealPixels=0,redPixels=0;
    sample.phase='combat';renderer.render(sample,camera);const combat=renderer.stats.deploymentZones;
    const plain=new Uint8Array(pixels.length);gl.readPixels(0,0,gl.drawingBufferWidth,gl.drawingBufferHeight,gl.RGBA,gl.UNSIGNED_BYTE,plain);
    for(let i=0;i<pixels.length;i+=4){if(pixels[i]===plain[i]&&pixels[i+1]===plain[i+1]&&pixels[i+2]===plain[i+2])continue;if(pixels[i+1]>pixels[i]*1.3&&pixels[i+2]>pixels[i]*1.3)tealPixels++;if(pixels[i]>pixels[i+1]*1.04&&pixels[i]>pixels[i+2]*1.04)redPixels++;}
    sample.phase='deployment';delete sample.layout.deploymentZones;renderer.render(sample,camera);const legacy=renderer.stats.deploymentZones;
    sample.layout.deploymentZones=[{side:0,minX:-66,maxX:66,minZ:-72,maxZ:-44},{side:1,minX:-66,maxX:66,minZ:44,maxZ:72}];renderer.render(sample,camera);
    return{visible,combat,legacy,tealPixels,redPixels};
  });
  if(process.env.BATTLE_SCREENSHOT)await page.screenshot({path:process.env.BATTLE_SCREENSHOT.replace(/\.png$/, '-zones.png')});
  assert.equal(deployment.visible,2);assert.equal(deployment.combat,0,'deployment boundaries disappear when combat begins');assert.equal(deployment.legacy,0,'legacy scenes without zone metadata remain renderable');assert.ok(deployment.tealPixels>20&&deployment.redPixels>20,`both deployment boundaries reach the framebuffer: ${JSON.stringify(deployment)}`);
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
  const combat=await page.evaluate(async()=>{
    const {battleActionPose}=await import('/game/iron-throne/battle-renderer.mjs');
    const original=battle,base={armyId:'a',type:'menAtArms',family:'infantry',owner:'ashen',side:0,status:'active',y:0,hp:100,maxHp:100,speed:0,weapon:'sword',shield:true};
    window.combatSample={...battle,time:4,commanderId:'swordsman',events:[],projectiles:[],units:[{...base,id:'swordsman',x:-.3,z:0,facing:0,action:{kind:'attack',elapsed:.3,windup:.3,recovery:.45,duration:.75}},{...base,id:'target',owner:'wintermere',side:1,x:.3,z:1.8,facing:Math.PI,action:{kind:'attack',elapsed:.2,windup:.3,recovery:.45,duration:.75}},{...base,id:'fallen',x:2,z:1,facing:1.2,status:'dead',hp:0,deathTime:2,deathFacing:1.2}]};
    window.combatCamera={mode:'third',target:{x:.25,y:0,z:.6},yaw:1.1,pitch:.28,distance:6.5};
    const actor=combatSample.units[0],poses=[.05,.3,.7].map(elapsed=>battleActionPose({...actor,action:{...actor.action,elapsed}}));
    const spear=battleActionPose({...actor,weapon:'spear'});
    const hit={eventId:1,type:'hit',time:3.86,x:.3,z:1.8,y:1.2,unitId:'target',sourceId:'swordsman',damage:24,damageApplied:24,targetFamily:'infantry',directionX:0,directionZ:1};
    const pickCamera={mode:'tactical',target:{x:actor.x,y:.95,z:actor.z},yaw:Math.PI,pitch:.5,distance:8};renderer.render(combatSample,pickCamera);
    const bodyPick=renderer.pick(640,400),groundPick=renderer.pick(640,400,{groundOnly:true});
    const draw=events=>{combatSample.events=events;renderer.render(combatSample,combatCamera);return{...renderer.stats};};
    const gl=document.querySelector('canvas').getContext('webgl'),pixels=()=>{const out=new Uint8Array(gl.drawingBufferWidth*gl.drawingBufferHeight*4);gl.readPixels(0,0,gl.drawingBufferWidth,gl.drawingBufferHeight,gl.RGBA,gl.UNSIGNED_BYTE,out);return out;};
    draw([]);const plain=pixels();const hitStats=draw([hit]),blood=pixels();let visibleBloodPixels=0;
    for(let i=0;i<blood.length;i+=4)if(blood[i]>blood[i+1]*3&&blood[i]>blood[i+2]*3&&blood[i]>45&&(blood[i]!==plain[i]||blood[i+1]!==plain[i+1]))visibleBloodPixels++;
    const blocked=draw([{...hit,blocked:true},{...hit,type:'block'}]).bloodParticles;
    const scenery=draw([{...hit,type:'siegeImpact'},{...hit,type:'hit',targetFamily:'siege'}]).bloodParticles;
    const zero=draw([{...hit,damage:0,damageApplied:0}]).bloodParticles;
    const distant=draw([{...hit,x:70}]).bloodParticles;
    const expired=draw([{...hit,time:2}]).bloodParticles;
    const bounded=draw(Array.from({length:40},(_,i)=>({...hit,eventId:i}))).bloodParticles;
    draw([hit]);window.presentationFixture={original,hit};
    return{stages:poses.map(p=>p.stage),strikeTipForward:Math.sin(poses[1].arm+poses[1].forearm+poses[1].weapon),thrustTipForward:Math.sin(spear.arm+spear.forearm+spear.weapon),hitStats,visibleBloodPixels,blocked,scenery,zero,distant,expired,bounded,bodyPick,groundPick};
  });
  if(process.env.BATTLE_SCREENSHOT)await page.screenshot({path:process.env.BATTLE_SCREENSHOT.replace(/\.png$/, '-contact.png')});
  assert.deepEqual(combat.stages,['windup','strike','recovery']);assert.ok(combat.strikeTipForward>.95,'the blade points toward the enemy at contact');assert.ok(combat.thrustTipForward>.99,'the spear thrust points forward');
  assert.equal(combat.bodyPick.unitId,'swordsman','normal picking selects the commander at the reticle');assert.equal(combat.groundPick.unitId,null,'ground aiming ignores the commander');assert.equal(combat.groundPick.y,0);assert.ok(Math.hypot(combat.groundPick.x-combat.bodyPick.x,combat.groundPick.z-combat.bodyPick.z)>.5,'ground aiming continues beyond the intercepted body');
  assert.equal(combat.hitStats.bloodParticles,8);assert.equal(combat.hitStats.corpses,1);assert.equal(combat.hitStats.detailedCorpses,1);assert.equal(combat.hitStats.attacking,2);assert.ok(combat.visibleBloodPixels>8,'confirmed hits produce visible red particles in the actual WebGL framebuffer');
  for(const key of ['blocked','scenery','zero','distant','expired'])assert.equal(combat[key],0,`${key} does not emit blood`);
  assert.equal(combat.bounded,96,'particles have a fixed rendering budget');
  const siege=await page.evaluate(async()=>{
    const {createBattleRenderer}=await import('/game/iron-throne/battle-renderer.mjs');
    const {createBattle}=await import('/game/iron-throne/battle-sim.mjs');
    const field=createBattle({id:'West Wall Assault',seed:701,playerOwner:'ashen',playerSide:0,tile:{building:'fort',walls:100,fortIntegrity:45,terrain:'plains'},participants:[
      {armyId:'a',owner:'ashen',side:0,units:{menAtArms:40,spearman:16,archer:16,ram:1}},
      {armyId:'d',owner:'wintermere',side:1,units:{menAtArms:16,spearman:16,archer:16}}
    ]});
    const canvas=document.querySelector('canvas'),cam={mode:'tactical',target:{x:-20,y:3,z:10},yaw:0,pitch:.18,distance:17};
    field.time=2;
    const attackers=field.units.filter(u=>u.side===0&&u.type==='menAtArms'),ladder=field.layout.ladders[0],ram=field.units.find(u=>u.type==='ram');
    Object.assign(attackers[0],{x:ladder.x,z:ladder.foot.z+1.1,y:3,facing:0,action:{kind:'climb',phase:'rungs',ladderId:ladder.id,progress:.43,elapsed:2.5,duration:5.8}});
    Object.assign(ram,{x:-12,y:0,z:8.2,facing:0,crewCount:6,action:{kind:'ramStrike',windup:1.25,elapsed:1.19,duration:3.05}});
    const {ramHandle}=await import('/game/iron-throne/battle-siege.mjs');
    for(let i=0;i<6;i++)Object.assign(attackers[i+1],ramHandle(ram,i),{y:0,facing:0,order:{kind:'crew',ramId:ram.id,handle:i},atHandle:true});
    const results={};
    for(const blood of ['off','reduced','full']){
      const scene=await createBattleRenderer(canvas,field,{blood});
      field.events=[{...presentationFixture.hit,x:-19,z:8,y:1,time:1.75}];
      scene.render(field,{...cam,targetKind:'ladder'});const ladder=scene.pick(640,400,{targetKind:'ladder'});
      scene.render(field,{...cam,target:{x:0,y:2,z:12},targetKind:'breach'});
      const gate=scene.pick(640,400,{targetKind:'breach'}),ground=scene.pick(640,400,{targetKind:'ground'});
      const gl=canvas.getContext('webgl');
      results[blood]={particles:scene.stats.bloodParticles,stains:scene.stats.stains,ladder:ladder?.targetId,gate:gate?.targetId,ground,glError:gl.getError()};
      scene.setBlood('off');scene.render(field,cam);
      if(scene.stats.stains||scene.stats.bloodParticles)throw new Error('Turning blood off during battle must clear existing blood.');
      scene.setBlood(blood);
      scene.render(field,{mode:'third',target:{x:-20,y:0,z:-3},yaw:0,pitch:.08,distance:6});
      if(blood==='full')window.siegePresentation=scene;else scene.dispose();
    }
    return results;
  });
  assert.deepEqual(Object.values(siege).map(r=>r.particles),[0,3,8]);
  assert.deepEqual(Object.values(siege).map(r=>r.stains),[0,1,1]);
  for(const result of Object.values(siege)){
    assert.equal(result.ladder,'ladder-0');assert.equal(result.gate,'gate');assert.equal(result.ground,null);assert.equal(result.glError,0);
  }
  if(process.env.BATTLE_SCREENSHOT)await page.screenshot({path:process.env.BATTLE_SCREENSHOT.replace(/\.png$/, '-west-wall.png')});
  await page.evaluate(()=>siegePresentation.dispose());
  const city=await page.evaluate(async()=>{
    const {createBattle}=await import('/game/iron-throne/battle-sim.mjs'),{createBattleRenderer}=await import('/game/iron-throne/battle-renderer.mjs');
    const roster={menAtArms:60,spearman:48,archer:48,crossbow:18,cavalry:24,ram:2};
    const field=createBattle({id:'royal-city',seed:171,playerOwner:'ashen',playerSide:0,tile:{terrain:'hills',building:'city',levels:{city:4,wall:3},walls:180,fortIntegrity:120},participants:[
      {armyId:'a',owner:'ashen',side:0,units:roster},{armyId:'d',owner:'wintermere',side:1,units:roster}
    ]},{maxActive:400});
    const canvas=document.querySelector('canvas'),gl=canvas.getContext('webgl'),scene=await createBattleRenderer(canvas,field);
    const cam={mode:'tactical',target:{x:0,y:4,z:31},yaw:-.34,pitch:.63,distance:118};
    const timings=[],pixel=new Uint8Array(4);
    for(let i=0;i<8;i++){
      field.time=i*.05;const start=performance.now();scene.render(field,cam);
      gl.readPixels(4,4,1,1,gl.RGBA,gl.UNSIGNED_BYTE,pixel);if(i>=3)timings.push(performance.now()-start);
    }
    const overview={...scene.stats};
    scene.render(field,{mode:'third',target:{x:0,y:1,z:-48},yaw:Math.PI,pitch:.19,distance:6});
    const near={...scene.stats};scene.render(field,cam);window.cityPresentation=scene;window.cityField=field;
    return {count:field.units.filter(u=>u.status==='active').length,tier:field.layout.castle?.tier,glError:gl.getError(),timings,overview,near};
  });
  assert.equal(city.count,400);assert.equal(city.tier,4);assert.equal(city.glError,0);
  for(const frame of [city.overview,city.near]){
    assert.ok(frame.detailedTroops<=24);assert.ok(frame.silhouettes>0);
    assert.equal(frame.detailedTroops+frame.silhouettes+frame.culledTroops,400);
  }
  assert.ok(city.near.detailedTroops>0);assert.ok(city.near.culledTroops>0);
  console.log('400 active in the royal citadel, 1280 × 800 scene, 5 synchronized frame times (ms):',city.timings.map(n=>Math.round(n)).join(', '));
  if(process.env.BATTLE_SCREENSHOT)await page.screenshot({path:process.env.BATTLE_SCREENSHOT.replace(/\.png$/, '-citadel.png')});
  if(process.env.BATTLE_SCREENSHOT){
    await page.evaluate(()=>cityPresentation.render(cityField,{mode:'tactical',target:{x:1,y:7,z:44},yaw:.33,pitch:.45,distance:66}));
    await page.screenshot({path:process.env.BATTLE_SCREENSHOT.replace(/\.png$/, '-citadel-close.png')});
  }
  await page.evaluate(()=>cityPresentation.dispose());
  for(const terrain of ['pass','forest','river']){
    const result=await page.evaluate(async(terrain)=>{
      const {createBattle}=await import('/game/iron-throne/battle-sim.mjs'),{createBattleRenderer}=await import('/game/iron-throne/battle-renderer.mjs');
      const field=createBattle({id:'royal-settlement',seed:119,playerOwner:'ashen',playerSide:0,
        tile:{terrain,building:'city',levels:{city:4,wall:3},walls:180,fortIntegrity:120},
        participants:[{armyId:'a',owner:'ashen',side:0,units:{spearman:30,menAtArms:22,ram:1}},
          {armyId:'d',owner:'wintermere',side:1,units:{archer:24,spearman:32,menAtArms:20}}]});
      field.phase='combat';
      const outer=field.layout.castle.outer,views={
        gate:{mode:'third',target:{x:-20,y:0,z:outer.front-9},yaw:.64,pitch:.10,distance:7},
        courtyard:{mode:'third',target:{x:-17,y:0,z:outer.front+15},yaw:.24,pitch:.09,distance:7}
      };
      const canvas=document.querySelector('canvas'),gl=canvas.getContext('webgl'),scene=await createBattleRenderer(canvas,field);
      // Compare the gate and courtyard against the same field without natural obstacles.
      const natural=o=>['rock','tree','water'].includes(o.kind),clear=structuredClone(field);
      clear.layout.obstacles=clear.layout.obstacles.filter(o=>!natural(o));clear.layout.bridges=[];
      const bare=await createBattleRenderer(canvas,clear),changes={};
      function area(){
        const x=Math.floor(canvas.width*.12),w=Math.floor(canvas.width*.76),y=Math.floor(canvas.height*.10),h=Math.floor(canvas.height*.52);
        const pixels=new Uint8Array(w*h*4);gl.readPixels(x,y,w,h,gl.RGBA,gl.UNSIGNED_BYTE,pixels);return pixels;
      }
      for(const [name,camera] of Object.entries(views)){
        scene.render(field,camera);const actual=area();bare.render(clear,camera);const empty=area();let changed=0;
        for(let i=0;i<actual.length;i+=4)if(Math.abs(actual[i]-empty[i])+Math.abs(actual[i+1]-empty[i+1])+Math.abs(actual[i+2]-empty[i+2])>8)changed++;
        changes[name]=changed;
      }
      bare.dispose();views.overview={mode:'tactical',target:{x:0,y:4,z:17},yaw:.21,pitch:.64,distance:120};
      window.settlementFrame=(name)=>scene.render(field,views[name]);window.settlementScene=scene;
      return {changes,tier:field.layout.castle.tier,natural:field.layout.obstacles.filter(natural).length,glError:gl.getError()};
    },terrain);
    assert.equal(result.tier,4);assert.ok(result.natural>0);assert.equal(result.glError,0);
    assert.deepEqual(result.changes,{gate:0,courtyard:0},`${terrain} scenery stays clear of the gate approach and royal courtyard`);
    if(process.env.BATTLE_SCREENSHOT)for(const view of ['gate','courtyard','overview']){
      await page.evaluate(view=>settlementFrame(view),view);
      await page.screenshot({path:process.env.BATTLE_SCREENSHOT.replace(/\.png$/, '-'+terrain+'-citadel-'+view+'.png')});
    }
    await page.evaluate(()=>settlementScene.dispose());
  }
  const polearms=await page.evaluate(()=>{
    const base=combatSample.units[0],pose={kind:'attack',elapsed:.52,windup:.52,recovery:.7,duration:1.22};
    const units=[{x:-2.5},{x:-.85,braced:true},{x:.85,action:pose},{x:3,mounted:true}].map((u,i)=>({
      ...base,action:null,facing:0,z:i===3?1.2:0,shield:true,type:i===3?'cavalry':'spearman',
      family:i===3?'mounted':'infantry',weapon:i===3?'lance':'spear',id:'polearm-'+i,...u
    }));
    const sample={...combatSample,commanderId:units[0].id,units,events:[]};
    const camera={mode:'third',target:{x:0,y:-.33,z:.25},yaw:Math.PI+.20,pitch:.17,distance:8};
    renderer.render(sample,camera);return renderer.stats.detailedTroops;
  });
  assert.equal(polearms,4);
  if(process.env.BATTLE_SCREENSHOT)await page.screenshot({path:process.env.BATTLE_SCREENSHOT.replace(/\.png$/, '-spears.png')});
  const corpses=await page.evaluate(()=>{
    const sample=structuredClone(combatSample),dead=sample.units[2];sample.events=[];sample.units=Array.from({length:200},(_,i)=>({...dead,id:`corpse-${i}`,x:i%20-10,z:Math.floor(i/20)*2-5}));
    renderer.render(sample,{mode:'tactical',target:{x:0,y:0,z:5},yaw:0,pitch:.8,distance:42});const present={...renderer.stats};sample.time+=120;renderer.render(sample,{mode:'tactical',target:{x:0,y:0,z:5},yaw:0,pitch:.8,distance:42});return{present,later:{...renderer.stats}};
  });
  assert.equal(corpses.present.corpses,160);assert.equal(corpses.present.detailedCorpses,24);assert.equal(corpses.later.corpses,160,'fallen bodies persist independently of the live troop budget');
  if(process.env.BATTLE_SCREENSHOT)await page.screenshot({path:process.env.BATTLE_SCREENSHOT.replace(/\.png$/, '-corpses.png')});
  await page.evaluate(()=>{battle=presentationFixture.original;renderer.render(battle,{mode:'tactical',target:{x:0,y:0,z:0},yaw:0,pitch:1,distance:60});});
  if(process.env.BATTLE_BENCH){
    const report=await page.evaluate(async()=>{
      const {createBattle}=await import('/game/iron-throne/battle-sim.mjs'),{createBattleRenderer}=await import('/game/iron-throne/battle-renderer.mjs');
      const roster={menAtArms:60,spearman:48,archer:48,crossbow:18,cavalry:24,ram:2};
      const canvas=document.querySelector('canvas'),gl=canvas.getContext('webgl'),timer=gl.getExtension('EXT_disjoint_timer_query'),pixel=new Uint8Array(4);
      const cam={mode:'tactical',target:{x:0,y:4,z:31},yaw:-.34,pitch:.63,distance:118},cases=[];
      const summary=list=>{list.sort((a,b)=>a-b);return {median:+list[Math.floor(list.length/2)].toFixed(2),p95:+list[Math.floor(list.length*.95)].toFixed(2)};};
      for(const cap of [100,400]){
        const field=createBattle({id:'same-citadel',seed:171,playerOwner:'ashen',playerSide:0,tile:{terrain:'hills',building:'city',levels:{city:4,wall:3},walls:180,fortIntegrity:120},participants:[
          {armyId:'a',side:0,owner:'ashen',units:roster},{armyId:'d',side:1,owner:'wintermere',units:roster}
        ]},{maxActive:cap});field.phase='combat';
        const scene=await createBattleRenderer(canvas,field),submit=[],endToEnd=[],gpu=[],queries=[];
        for(let i=0;i<11;i++){
          const q=timer&&i>=3?timer.createQueryEXT():null;
          if(q)timer.beginQueryEXT(timer.TIME_ELAPSED_EXT,q);
          field.time=i*.05;const start=performance.now();scene.render(field,cam);
          const submitted=performance.now();if(q)timer.endQueryEXT(timer.TIME_ELAPSED_EXT);
          gl.readPixels(0,0,1,1,gl.RGBA,gl.UNSIGNED_BYTE,pixel);const end=performance.now();
          if(i>=3){submit.push(submitted-start);endToEnd.push(end-start);if(q)queries.push(q);}
        }
        if(queries.length){await new Promise(r=>setTimeout(r,40));for(const q of queries){
          if(timer.getQueryObjectEXT(q,timer.QUERY_RESULT_AVAILABLE_EXT)&&!gl.getParameter(timer.GPU_DISJOINT_EXT))gpu.push(timer.getQueryObjectEXT(q,timer.QUERY_RESULT_EXT)/1e6);
          timer.deleteQueryEXT(q);
        }}
        cases.push({active:field.units.filter(u=>u.status==='active').length,buffer:[gl.drawingBufferWidth,gl.drawingBufferHeight],frames:submit.length,submitMs:summary(submit),synchronizedMs:summary(endToEnd),gpuMs:gpu.length?summary(gpu):null,detail:scene.stats.detailedTroops,silhouettes:scene.stats.silhouettes});scene.dispose();
      }
      return cases;
    });console.log('Matched royal citadel benchmark:',JSON.stringify(report));
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
  console.log('Battle renderer: all 15 troop classes, forward-facing weapon contact, visible close-hit blood with culling, persistent bounded corpses, deployment boundaries, instanced geometry, cameras, body/wall/ground picking, disposal, default Cloudflare load/download, offline remote cache, local fallback, and failure recovery passed.');
}finally{await browser.close();await new Promise(r=>server.close(r));}
