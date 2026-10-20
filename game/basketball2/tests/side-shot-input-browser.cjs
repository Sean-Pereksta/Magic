/* Real keyboard approaches preserve a running finish while the player stops
 * to release. Covers both human control styles and an actual low-rated rookie. */
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{chromium}=require('playwright');
const {installLegacyGraphicsFixture}=require('./legacy-graphics-fixture.cjs');
const root=path.resolve(__dirname,'../../..');
(async()=>{
 const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
 if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
 const browser=await chromium.launch(options);
 try{
  const page=await browser.newPage({viewport:{width:1200,height:800}}),errors=[];page.on('pageerror',e=>errors.push(e.stack));
  await page.route('http://basketball.local/**',route=>{
   let file=path.join(root,new URL(route.request().url()).pathname);if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
   if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
   const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.glb':'model/gltf-binary','.webp':'image/webp','.json':'application/json'};
   if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
   return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
  });
  if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
  await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
  await installLegacyGraphicsFixture(page);

  await page.goto('http://basketball.local/game/basketball2/');await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
  const results=await page.evaluate(()=>{
   updateTeamAI=()=>{};customRosterDraft.enabled=false;
   const rookie=RiseToGlory.rookiePool(24).find(p=>p.position==='PG');
   profiles.inputTestRookie={...profiles.crafty,...RiseToGlory.toProfile(rookie)};
   const event=(type,code,key)=>{window.dispatchEvent(new KeyboardEvent(type,{code,key,bubbles:true,cancelable:true}));inputManager.tick();processUnifiedInputActions(p1,inputManager.getState('p1'));};
   const rows=[];
   for(const style of ['modern','classic'])for(const profile of ['flash','crafty','inputTestRookie'])for(const degrees of [0,-85,85])for(const distance of [3.35,3.55,4.1])for(const hold of [.1,.25,.6,.85]){
    selectedP1=profile;selectedP2='tower';teamSize=1;twoPlayerMode=false;isTournamentGame=false;tutorialMode=false;startGame();gameSettings.set('controlStyle',style);
    inputManager.clearAll();countdownTimer=0;possessionFrozen=false;pendingInbound=null;p2.group.position.set(8,0,8);
    const angle=degrees*Math.PI/180;p1.group.position.set(Math.sin(angle)*5,0,HOOP_POS.z+Math.cos(angle)*5);
    const code=style==='classic'||degrees===0?'KeyW':degrees<0?'KeyD':'KeyA',key=code==='KeyW'?'w':code==='KeyD'?'d':'a';
    p1.angle=angleToward(p1.group.position,HOOP_POS);p1.group.rotation.set(0,p1.angle,0);takePossession(p1,{silent:true});
    event('keydown',code,key);let warm=0;
    while(flatDistance(p1.group.position,HOOP_POS)>distance&&warm++<240)simulateBasketball(1/60);
    const press={distance:flatDistance(p1.group.position,HOOP_POS),speed:p1.velocity.length(),type:chooseShotType(p1)};
    event('keydown','Space',' ');
    if(distance>3.6){let frames=0;while(flatDistance(p1.group.position,HOOP_POS)>3.35&&frames++<120)simulateBasketball(1/60);}
    const entry={distance:flatDistance(p1.group.position,HOOP_POS),speed:p1.velocity.length(),type:chooseShotType(p1)};
    event('keyup',code,key);
    for(let frame=0;frame<Math.round(hold*60);frame++)simulateBasketball(1/60);
    const release={distance:flatDistance(p1.group.position,HOOP_POS),speed:p1.velocity.length(),type:chooseShotType(p1)};
    event('keyup','Space',' ');
    const started=layupActive&&layupOwner===p1.id;
    let releasedType=null;
    for(let frame=0;frame<150&&!lastScored;frame++){simulateBasketball(1/60);if(ballState==='shot'&&lastShotShooter===p1.id)releasedType||=p1.moveMeta.lastShotType;}
    rows.push({style,profile,degrees,distance,hold,press,entry,release,started,type:releasedType,attempts:p1.stats.fga,points:scoreP1,
     finite:p1.group.position.toArray().every(Number.isFinite)&&ball.position.toArray().every(Number.isFinite)});
   }
   return {rows,rookie:{name:rookie.name,layups:rookie.ratings.layups,stability:rookie.ratings.releaseStability}};
  });
  for(const row of results.rows){
   assert.equal(row.press.type,row.distance>3.6?'setshot':'layup','actual approach recognizes its starting range: '+JSON.stringify(row));
   assert.equal(row.entry.type,'layup','holding shoot can recognize a legitimate entry into finishing range: '+JSON.stringify(row));
   assert.equal(row.release.type,'layup','stopping during the same hold must not change the expected finish: '+JSON.stringify(row));
   assert.ok(row.started,'keyboard release starts the real layup gather: '+JSON.stringify(row));
   assert.equal(row.type,'layup','gather releases a physical layup: '+JSON.stringify(row));
   assert.equal(row.attempts,1);assert.ok(row.finite);
  }
  assert.ok(results.rookie.layups<50&&results.rookie.stability<50,'the actual rookie keeps modest development ratings');
  if(process.env.BASKETBALL_SCREENSHOTS){fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});fs.writeFileSync(path.join(process.env.BASKETBALL_SCREENSHOTS,'side-shot-input-results.json'),JSON.stringify(results,null,2));}
  assert.deepEqual(errors,[]);
  console.log('PASS: '+results.rows.length+' actual modern/classic keyboard front/side approaches retain the selected layup through short and meter-length stops; real rookie '+JSON.stringify(results.rookie));
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
