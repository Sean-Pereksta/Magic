/* Real-canvas military gallery and integration assertions. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const{chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{
  const page=await browser.newPage({viewport:{width:1440,height:900}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setContent('<style>html,body{margin:0;background:#071216}canvas{display:block;width:1440px;height:900px}</style><canvas id="game"></canvas>');
  for(const name of ['world','navigation','settlements','forces','sim','render','render-details','audio'])await page.addScriptTag({content:fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8')});
  const result=await page.evaluate(()=>{
   const g=new ATSGame('MILITARY-GALLERY','survival');g.time=20;g.apes=[];g.humans=[];g.helis=[];g.vehicles=[];g.corpses=[];g.effects=[];g.king.x=g.king.y=0;
   g.world.getObjects=()=>[];g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',road:false});g.world.lineClear=()=>true;
   const armor=[['tank',-100,310],['apc',108,108],['truck',320,-106],['ifv',88,-349]];
   for(const[kind,x,y]of armor){const v={id:'gallery-'+kind,kind,vehicleClass:kind,x,y,hp:kind==='tank'?760:400,maxHp:kind==='tank'?1100:650,dir:0,turretDir:0,troops:5,capacity:10,dismounting:kind==='apc',moving:kind==='truck',engineDamage:kind==='tank'?56:0,mobilityDamage:kind==='tank'?63:0,overrun:kind==='tank',cannonFlash:kind==='ifv'?.12:0};if(kind==='ifv')v.cannonTarget={x:180,y:-100,start:18.8,duration:1.8,until:20.6};g.vehicles.push(v)}
   for(let i=0;i<20;i++){const a=g.makeApe(-100+Math.cos(i/20*Math.PI*2)*55,310+Math.sin(i/20*Math.PI*2)*55,'attack');if(i<10){a.climbingVehicleId='gallery-tank';a.climbUntil=21}if(i===16)a.staggerUntil=21}
   let i=0;for(const role of ['rifleman','ranger','heavy','engineer','leader','grenadier','medic','sniper']){const h=g.makeHuman(145+(i%4)*50,85+Math.floor(i/4)*55,null);h.role=role;h.state='combat';h.kind=role==='heavy'?'machine':'assault';h.squadId='gallery-squad';h.squadOrder='protectVehicle';i++}
   for(const[kind,x,y]of [['recon',-326,36],['scout',-175,-187],['gunship',-5,-405]])g.helis.push({id:'gallery-'+kind,kind,x,y,dir:0,hp:300,maxHp:450,armed:kind!=='recon',attackTimer:kind==='gunship'?.1:0});
   g.forces.hazards=[{type:'shell',x:80,y:-90,fromX:88,fromY:-349,targetX:180,targetY:-100,start:19.7,duration:.7,life:.4,radius:60}];g.effects.push({type:'tankImpact',x:120,y:300,life:.72,maxLife:1.05,radius:104});
   const r=new ATSRenderer(document.getElementById('game'));r.camera.x=r.camera.y=0;r.quality='high';r.draw(g,0);
   const c=r.ctx;c.font='700 27px system-ui';c.fillStyle='#ead5a3';c.textAlign='left';c.fillText('MILITARY MOBILIZATION',48,54);c.font='14px system-ui';c.fillStyle='#9fb5a6';c.fillText('Tracked armor, troop deployment, support aircraft and committed cannon warnings',48,81);
   const labels=[['TANK: OVERRUN',286,699],['APC: DISMOUNT',647,664],['TROOP TRUCK',1001,655],['IFV: CANNON LOCK',1020,279],['RECON',372,273],['ARMED SCOUT',659,241],['GUNSHIP',990,219]];c.font='700 12px system-ui';c.fillStyle='#dbc998';for(const[t,x,y]of labels)c.fillText(t,x,y);
   window.militaryGallery={g,r};return{cache:r.vehicleSprites.size,los:g.performance.counters.renderLosTests,rays:g.performance.counters.renderRays,actors:g.performance.counters.visibleActors};
  });
  assert.equal(result.cache,4);assert.ok(result.los<=96);assert.ok(result.rays<=96);assert.deepEqual(errors,[]);
  if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,'military-gallery.png')})}
  console.log(JSON.stringify(result));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
