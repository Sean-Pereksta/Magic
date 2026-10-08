/* Native Canvas gallery, animated-pixel checks and integrated military render budget. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {chromium}=require('playwright'),root=path.resolve(__dirname,'..');
const output=process.env.QA_ARTIFACT_DIR;
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{
  const page=await browser.newPage({viewport:{width:1920,height:1530},deviceScaleFactor:1}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setContent('<style>html,body{margin:0;background:#13221f}canvas{display:block;width:1920px;height:1530px}</style><canvas id="game"></canvas>');
  const manifest={},sources={},assetRoot=path.join(root,'assets/visual');
  for(const family of fs.readdirSync(assetRoot).filter(x=>fs.existsSync(path.join(assetRoot,x,'manifest.json')))){
   manifest[family]=JSON.parse(fs.readFileSync(path.join(assetRoot,family,'manifest.json')));
   for(const a of manifest[family].atlases)sources[a.id]='data:image/'+path.extname(a.file).slice(1)+';base64,'+fs.readFileSync(path.join(assetRoot,a.file)).toString('base64');
  }
  await page.evaluate(b=>window.ATS_VISUAL_BUNDLE=b,{manifest,sources});
  const build=fs.readFileSync(path.join(root,'build.cjs'),'utf8'),parts=[...build.match(/const parts=\[([^\]]+)\]/)[1].matchAll(/'([^']+)'/g)].map(m=>m[1]).filter(n=>n!=='app'&&!n.endsWith('-ui'));
  for(const name of parts)await page.addScriptTag({content:fs.readFileSync(path.join(root,name+'.js'),'utf8')});
  await page.evaluate(()=>ATSVisualAssets.ready);assert.equal(await page.evaluate(()=>ATSVisualAssets.status),'ready');
  const gallery=await page.evaluate(()=>{
   const r=new ATSRenderer(document.getElementById('game')),c=r.ctx;r.time=4;r.quality='high';r.vehicleEffectBudget=0;r.vehicleLampBudget=0;r.vehicleTextureBudget=1000;
   c.fillStyle='#1b302a';c.fillRect(0,0,1920,1530);c.fillStyle='#e3d3a4';c.font='700 28px system-ui';c.fillText('APES TOGETHER STRONG  /  MILITARY ARTWORK',42,48);
   const kinds=['jeep','command','truck','armored','apc','ifv','tank','recon','scout','gunship'];
   c.font='13px system-ui';c.fillStyle='#b4cab9';c.fillText('Eight screen headings. Separate turret bearings and main / tail rotor layers. Original transparent PNG artwork.',42,77);
   for(let row=0;row<kinds.length;row++){
    const y=146+row*135,kind=kinds[row];c.fillStyle=row%2?'#20382f':'#1b302a';c.fillRect(0,y-42,1920,135);
    c.fillStyle='#d6d2a8';c.font='700 15px system-ui';c.fillText(kind.toUpperCase(),30,y+23);
    for(let sector=0;sector<8;sector++){
     const x=265+sector*210,sx=Math.cos(sector*Math.PI/4)/.8,sy=Math.sin(sector*Math.PI/4)/.42,dir=Math.atan2((sy-sx)/2,(sy+sx)/2);
     const a={id:(row<7?'vehicle-':'heli-')+row+'-'+sector,kind,vehicleClass:row<7?kind:undefined,x:0,y:0,dir,turretDir:dir,hp:100,maxHp:100,phase:4,moving:true};
     c.save();c.translate(x,y+40);
     if(row<7)r.drawVehicle(c,a);else{const old=r.renderPoint;r.renderPoint=(a,lift)=>({x:0,y:lift?-32:46});r.drawHeli(c,a);r.renderPoint=old}c.restore();
     if(row===0){c.font='11px system-ui';c.fillStyle='#88a993';c.fillText(ATSVehicleArt.direction(dir).name,x-24,y+78)}
    }
   }
   window.vehicleGalleryRenderer=r;return{atlases:ATSVisualAssets.manifest.vehicles.atlases.length,frames:Object.keys(ATSVisualAssets.manifest.vehicles.frames).length};
  });
  assert.equal(gallery.frames,85);
  if(output){fs.mkdirSync(output,{recursive:true});await page.screenshot({path:path.join(output,'vehicle-eight-directions.png')})}
  const report=await page.evaluate(()=>{
   const r=vehicleGalleryRenderer,c=r.ctx,h={id:'heli-stationary',kind:'gunship',x:0,y:0,dir:0,hp:360,maxHp:360,phase:4,moving:false};
   r.renderPoint=(a,lift)=>({x:300,y:lift?150:260});r.vehicleEffectBudget=0;r.vehicleLampBudget=0;
   function rotorPixels(time){r.time=time;c.clearRect(0,0,600,400);r.drawHeli(c,h);return Array.from(c.getImageData(180,100,240,110).data)}
   const a=rotorPixels(4),b=rotorPixels(4.14);let changed=0;for(let i=0;i<a.length;i++)if(a[i]!==b[i])changed++;
   const g=new ATSGame('VEHICLE-VISUAL-QA','survival');g.time=20;g.apes=[];g.humans=[];g.corpses=[];g.effects=[];g.king.x=g.king.y=0;
   g.world.getObjects=()=>[];g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',road:true});g.world.lineClear=()=>true;
   const kinds=['jeep','command','armored','truck','apc','ifv','tank'];
   g.vehicles=Array.from({length:28},(_,i)=>{const kind=kinds[i%7];return{id:'vehicle-qa-'+i,kind,vehicleClass:kind,x:(i%7-3)*100,y:(Math.floor(i/7)-1.5)*135,dir:i*Math.PI/7,turretDir:-i*Math.PI/7,hp:i%4===0?25:100,maxHp:100,phase:i,moving:true,engineDamage:i%4===0?70:0,variant:i===6?'repeater':i===13?'bombard':i===20?'cyclone':null}});
   g.helis=['recon','scout','gunship','gunship'].map((kind,i)=>({id:'heli-qa-'+i,kind,x:-220+i*150,y:-220,dir:i*Math.PI/2,hp:300,maxHp:360,phase:4+i,spotX:-220+i*150,spotY:-220,armed:kind!=='recon'}));
   const live=new ATSRenderer(document.getElementById('game'));live.quality='high';const samples=[];
   for(let i=0;i<180;i++){g.time=20+i/60;const start=performance.now();live.draw(g,1/60);if(i>30)samples.push(performance.now()-start)}
   samples.sort((a,b)=>a-b);window.vehicleLive={g,r:live};return{...{changedRotorChannels:changed},renderMeanMs:samples.reduce((a,b)=>a+b,0)/samples.length,renderP95Ms:samples[Math.floor(samples.length*.95)],vehicles:g.vehicles.length,helicopters:g.helis.length,rotorCache:live.vehicleRotors.size,tintCache:live.vehicleTextures?.size||0,lightingRays:g.performance.counters.renderRays||0,remainingEffects:live.vehicleEffectBudget};
  });
  assert.ok(report.changedRotorChannels>100,'actual rotor pixels change while hovering');assert.ok(report.rotorCache<=32);assert.ok(report.tintCache<=96);assert.ok(report.lightingRays<=96);assert.ok(report.remainingEffects>=0);assert.equal(report.vehicles,28);assert.equal(report.helicopters,4);assert.deepEqual(errors,[]);
  if(output){await page.screenshot({path:path.join(output,'vehicle-live-stress.png')});fs.writeFileSync(path.join(output,'vehicle-art-browser.json'),JSON.stringify(report,null,2))}
  console.log(JSON.stringify(report,null,2));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
