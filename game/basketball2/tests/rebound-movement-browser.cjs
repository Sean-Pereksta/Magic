/* Real board collision followed by public keyboard/controller and AI rebound
 * movement. The last shooter's possession ID deliberately remains in place. */
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{chromium}=require('playwright');
const root=path.resolve(__dirname,'../../..');
(async()=>{
 const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
 if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
 const browser=await chromium.launch(options);
 try{
  const page=await browser.newPage({viewport:{width:1280,height:800}}),errors=[];page.on('pageerror',error=>errors.push(error.stack));
  await page.addInitScript(()=>{
   window.virtualPads=[];Object.defineProperty(navigator,'getGamepads',{configurable:true,value:()=>virtualPads});
   window.reboundPad=()=>{
    const pad={index:0,id:'Rebound regression pad',mapping:'standard',connected:true,timestamp:performance.now(),axes:[0,0,0,0],buttons:Array.from({length:18},()=>({pressed:false,touched:false,value:0}))};
    virtualPads[0]=pad;const event=new Event('gamepadconnected');Object.defineProperty(event,'gamepad',{value:pad});window.dispatchEvent(event);return pad;
   };
  });
  await page.route('http://basketball.local/**',route=>{
   let file=path.join(root,new URL(route.request().url()).pathname);if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
   if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
   if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
   const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.glb':'model/gltf-binary','.webp':'image/webp','.json':'application/json'};
   return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
  });
  if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
  await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
  await installLegacyGraphicsFixture(page);

  await page.goto('http://basketball.local/game/basketball2/');await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
  const result=await page.evaluate(()=>{
   const rows=[],realAI=updateTeamAI,random=Math.random;Math.random=()=>.5;customRosterDraft.enabled=false;
   const key=(type,code)=>window.dispatchEvent(new KeyboardEvent(type,{code,key:code==='KeyW'?'w':'Shift',bubbles:true,cancelable:true}));
   const start=(shooterTeam,style)=>{
    for(const pad of virtualPads.filter(Boolean)){pad.axes.fill(0);pad.buttons.forEach((_,index)=>pad.buttons[index]={pressed:false,touched:false,value:0});}
    inputManager.clearAll();selectedP1=selectedP2='flash';teamSize=1;twoPlayerMode=false;isTournamentGame=false;tutorialMode=false;startGame();
    gameSettings.set('controlStyle',style);gameOver=gamePaused=false;countdownTimer=0;possessionFrozen=false;pendingInbound=null;updateTeamAI=()=>{};
    p1.group.position.set(-4,0,0);p2.group.position.set(4,0,0);for(const player of [p1,p2]){player.velocity.set(0,0,0);player.angle=style==='classic'?Math.PI:0;player.group.rotation.y=player.angle;}
    const shooter=shooterTeam===0?p1:p2;takePossession(shooter,{silent:true});shooter.moveMeta.chargeHeld=.1;releaseShot(shooter,'setshot');
    // A deliberately high miss travels into the real backboard solver. This
    // keeps the test focused on the rebound state rather than aim variation.
    ball.position.set(.9,HOOP_GEOMETRY.boardCenterY+.1,HOOP_GEOMETRY.boardCenterZ+.8);ballVelocity.set(0,0,-8);previousBallPosition.copy(ball.position);
    for(let frame=0;frame<12&&!shotHasHitRimOrBoard;frame++)simulateBasketball(1/60);
    if(!shotHasHitRimOrBoard)throw Error('fixture did not physically hit the board');
    return {position:p1.group.position.clone(),staleOwner:possession,shooter:shooter.id};
   };
   try{
    for(const source of ['keyboard','gamepad'])for(const style of ['modern','classic'])for(const shooterTeam of [0,1])for(const sprint of [false,true]){
     const before=start(shooterTeam,style);
     if(source==='keyboard'){key('keydown','KeyW');if(sprint)key('keydown','ShiftLeft');}
     else{const pad=reboundPad();controllerConfig.setAssignment('p1',{index:0,id:pad.id});inputManager.tick();gamePaused=false;pad.axes[1]=-1;if(sprint)pad.buttons[7]={pressed:true,touched:true,value:1};}
     for(let frame=0;frame<45;frame++){inputManager.tick();simulateBasketball(1/60);}
     const animation=chooseBasketballPlayerAnimation(p1);
     rows.push({source,style,shooterTeam,sprint,boardHit:shotHasHitRimOrBoard,staleOwner:before.staleOwner,shooter:before.shooter,
      distance:p1.group.position.distanceTo(before.position),speed:p1.velocity.length(),facing:getForward(p1.angle).dot(p1.velocity.clone().normalize()),animation:animation.name,ballState});
     if(source==='keyboard'){key('keyup','KeyW');key('keyup','ShiftLeft');}
    }
    const before=start(0,'modern');p1.controllerSlot=null;p2.controllerSlot=null;updateTeamAI=realAI;
    for(const player of [p1,p2]){player.velocity.set(0,0,0);player.angle=0;player.aiBrain=createTeamBrain(player);}
    const origins=[p1.group.position.clone(),p2.group.position.clone()];
    for(let frame=0;frame<45;frame++){inputManager.tick();simulateBasketball(1/60);}
    const ai=[p1,p2].map((player,index)=>({team:player.teamId,distance:player.group.position.distanceTo(origins[index]),speed:player.velocity.length(),
     facing:getForward(player.angle).dot(player.velocity.clone().normalize()),animation:chooseBasketballPlayerAnimation(player).name,role:player.aiBrain.role}));
    return {rows,ai};
   }finally{Math.random=random;updateTeamAI=realAI;}
  });
  for(const row of result.rows){
   assert.ok(row.boardHit);assert.equal(row.staleOwner,row.shooter);assert.ok(['shot','loose'].includes(row.ballState),JSON.stringify(row));
   assert.ok(row.speed>(row.sprint?6:4),'both teams can reach ordinary open-court running speed: '+JSON.stringify(row));
   assert.ok(row.facing>.98,'rebound chase faces movement: '+JSON.stringify(row));assert.equal(row.animation,row.speed>4.7?'sprint':'run',JSON.stringify(row));
   const other=result.rows.find(other=>other.source===row.source&&other.style===row.style&&other.sprint===row.sprint&&other.shooterTeam!==row.shooterTeam);
   assert.ok(Math.abs(row.speed-other.speed)<1e-8&&Math.abs(row.distance-other.distance)<1e-8,'changing the prior shooter cannot change rebound movement: '+JSON.stringify([row,other]));
  }
  for(const player of result.ai){assert.ok(player.speed>6,JSON.stringify(player));assert.ok(player.facing>.98,JSON.stringify(player));assert.equal(player.animation,'sprint');assert.equal(player.role,'rebound');}
  assert.ok(Math.abs(result.ai[0].speed-result.ai[1].speed)<.04,'both AI teams use the same chase motor');
  assert.deepEqual(errors,[]);
  if(process.env.BASKETBALL_SCREENSHOTS){fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});fs.writeFileSync(path.join(process.env.BASKETBALL_SCREENSHOTS,'rebound-movement-results.json'),JSON.stringify(result,null,2));}
  console.log('PASS: '+result.rows.length+' real board-rebound keyboard/controller chases, both control styles and prior possession teams, plus both AI teams run and sprint fairly');
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
