/* Verify actual painted pixels against the same timing rules used on release. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'../../..');

(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage({viewport:{width:1440,height:950}}),errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.glb':'model/gltf-binary','.webp':'image/webp'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    await page.evaluate(()=>{riseState=RiseToGlory.create();startRiseMatch();gameSettings.set('shotMeter',true);countdownTimer=0;possessionFrozen=false;});
    const rows=await page.evaluate(()=>{
      const baseline=JSON.parse(JSON.stringify(p1.profile)),rows=[];
      for(const config of [{name:'rookie',speed:0,contest:false,quick:0},{name:'quick release',speed:0,contest:false,quick:3},
        {name:'moving',speed:4,contest:false,quick:0},{name:'contested',speed:0,contest:true,quick:0},{name:'moving quick contested',speed:4,contest:true,quick:3}]){
        p1.profile=JSON.parse(JSON.stringify(baseline));p1.profile.abilities.quickRelease=config.quick;
        p1.group.position.set(5,0,HOOP_POS.z+3);p1.angle=angleToward(p1.group.position,HOOP_POS);p1.group.rotation.y=p1.angle;
        p1.velocity.set(config.speed,0,0);p1.stamina=1;p1.moveState='normal';p1.moveMeta={};
        getOpponents(p1).forEach(player=>{player.group.position.set(-8,0,8);player.blockTime=0;player.handsUpTime=0;});
        if(config.contest){p2.group.position.copy(p1.group.position).addScaledVector(getForward(p1.angle),1.3);p2.angle=p1.angle+Math.PI;p2.handsUpTime=.5;}
        takePossession(p1,{silent:true});shotCharging=true;chargingPlayer=p1;
        const kind=chooseShotType(p1),contest=calculateShotContest(p1),profile=getShotMeterProfile(kind,p1,contest);
        p1.moveMeta.chargeHeld=(profile.greenStart+profile.greenEnd)*.5*1.35/profile.meterFillSpeed;
        updateShotMeterVisuals(0);
        const pixels=shotMeter.sctx.getImageData(0,Math.floor(shotMeter.stripCanvas.height/2),shotMeter.stripCanvas.width,1).data;
        const w=shotMeter.stripCanvas.width;let brightGreen=0,falseGreen=0,missingGreen=0;
        for(let x=0;x<w;x++){
          const green=pixels[x*4]===53&&pixels[x*4+1]===255&&pixels[x*4+2]===107;
          const grade=calculateShotTiming((x+.5)/w*1.35/profile.meterFillSpeed,kind,p1,contest).grade;
          if(green){brightGreen++;if(grade!=='Green Release')falseGreen++;}
          if(x>Math.ceil(profile.greenStart*w)+3&&x<Math.floor(profile.greenEnd*w)-3&&grade==='Green Release'&&!green)missingGreen++;
        }
        const before=shotMeter.stripTexture.version,paintedAgain=paintShotMeterProfile({...profile});
        rows.push({name:config.name,brightGreen,falseGreen,missingGreen,paintedAgain,unchangedVersion:before===shotMeter.stripTexture.version,
          overlayStart:(shotMeter.greenRegion.position.x-shotMeter.greenRegion.scale.x*.1+.6)/1.2,
          overlayEnd:(shotMeter.greenRegion.position.x+shotMeter.greenRegion.scale.x*.1+.6)/1.2,start:profile.greenStart,end:profile.greenEnd,
          centerGrade:calculateShotTiming(p1.moveMeta.chargeHeld,kind,p1,contest).grade,kind});
      }
      return rows;
    });
    for(const row of rows){
      assert.equal(row.kind,'setshot');assert.ok(row.brightGreen>15,JSON.stringify(row));assert.equal(row.falseGreen,0,JSON.stringify(row));
      assert.equal(row.missingGreen,0,JSON.stringify(row));assert.equal(row.centerGrade,'Green Release');
      assert.equal(row.paintedAgain,false);assert.equal(row.unchangedVersion,true);
      assert.ok(Math.abs(row.overlayStart-row.start)<1e-10&&Math.abs(row.overlayEnd-row.end)<1e-10,JSON.stringify(row));
    }
    assert.ok(rows[1].end-rows[1].start<rows[0].end-rows[0].start,'Quick Release narrowing appears in the painted strip');
    assert.ok(rows[3].end-rows[3].start<rows[0].end-rows[0].start,'defensive pressure narrowing appears in the painted strip');
    console.log('PASS: painted green pixels and overlay match release timing for rookie, Quick Release, moving, contested and combined cases; unchanged profiles reuse their texture.');
    if(process.env.BASKETBALL_SCREENSHOTS){
      fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});
      await page.evaluate(()=>{p1.velocity.set(0,0,0);p1.profile.abilities.quickRelease=0;getOpponents(p1).forEach(player=>player.group.position.set(-8,0,8));
        const kind=chooseShotType(p1),contest=calculateShotContest(p1),profile=getShotMeterProfile(kind,p1,contest);p1.moveMeta.chargeHeld=(profile.greenStart+profile.greenEnd)*.5*1.35/profile.meterFillSpeed;
        for(let frame=0;frame<120;frame++)updateCameras(1/60);onResize();updateHUD();updateShotMeterVisuals(0);renderScene();});
      await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'shot-meter-desktop.png')});
      await page.setViewportSize({width:390,height:844});await page.evaluate(()=>{onResize();for(let frame=0;frame<120;frame++)updateCameras(1/60);updateShotMeterVisuals(0);renderScene();});
      await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'shot-meter-mobile.png')});
    }
    assert.deepEqual(errors,[]);
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
