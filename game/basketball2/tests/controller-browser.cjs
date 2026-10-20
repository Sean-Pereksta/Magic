/* Real-browser controller setup and gameplay checks. Only the physical Gamepad
 * API is simulated: the game, input manager, settings UI and shot physics run.
 * Optional THREE_BUNDLE / BASKETBALL_CHROME match browser-smoke.cjs.
 * BASKETBALL_SCREENSHOTS saves the desktop and narrow-screen settings previews.
 */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {installLegacyGraphicsFixture}=require('./legacy-graphics-fixture.cjs');
const root=path.resolve(__dirname,'../../..');

(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage({viewport:{width:1440,height:1000}});
    const errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.addInitScript(()=>{
      window.virtualPads=[];
      Object.defineProperty(navigator,'getGamepads',{configurable:true,value:()=>window.virtualPads});
      window.connectVirtualPad=(index,id='Xbox Wireless Controller '+index)=>{
        const pad={index,id,mapping:'standard',connected:true,timestamp:performance.now(),
          axes:[0,0,0,0],buttons:Array.from({length:18},()=>({pressed:false,touched:false,value:0}))};
        window.virtualPads[index]=pad;
        const event=new Event('gamepadconnected');Object.defineProperty(event,'gamepad',{value:pad});window.dispatchEvent(event);
        return pad;
      };
      window.disconnectVirtualPad=index=>{
        const pad=window.virtualPads[index];pad.connected=false;window.virtualPads[index]=null;
        const event=new Event('gamepaddisconnected');Object.defineProperty(event,'gamepad',{value:pad});window.dispatchEvent(event);
      };
      window.virtualButton=(index,button,value=1)=>{
        const pad=window.virtualPads[index];pad.buttons[button]={pressed:value>.5,touched:value>0,value};pad.timestamp=performance.now();
      };
      window.neutralVirtualPads=()=>window.virtualPads.filter(Boolean).forEach(pad=>{
        pad.axes.fill(0);pad.buttons.forEach((_,index)=>{pad.buttons[index]={pressed:false,touched:false,value:0};});
      });
    });
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.webp':'image/webp','.glb':'model/gltf-binary','.json':'application/json'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({
      path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),
      contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}
    }));
    const open=async()=>{
      await installLegacyGraphicsFixture(page);

      await page.goto('http://basketball.local/game/basketball2/');
      await page.waitForFunction(()=>typeof controllerConfig!=='undefined'&&basketballPlayerModels.assets.size===3);
      await page.evaluate(()=>{
        // Advance the actual animation handler on demand, including pause,
        // input polling, simulation and rendering, without uncontrolled RAFs.
        const realFrame=window.requestAnimationFrame.bind(window);
        window.requestAnimationFrame=callback=>callback===animate?0:realFrame(callback);clock.getDelta=()=>1/60;
        window.controllerTestFrames=count=>{
          // A simulated batch still polls every frame; draw its final state.
          const draw=renderScene;renderScene=()=>{};
          try{for(let i=0;i<count;i++)animate();}finally{renderScene=draw;}
          renderScene();
        };
        window.controllerTestStart=(local=false,size=3)=>{
          neutralVirtualPads();inputManager.clearAll();gameSettings.set('controlStyle','modern');
          twoPlayerMode=local;teamSize=size;localTeamMode='competitive';isTournamentGame=false;startGame();
          gameOver=false;gamePaused=false;countdownTimer=0;possessionFrozen=false;pendingInbound=null;
          scoreP1=scoreP2=0;simulationAccumulator=0;
          updateTeamAI=()=>{};
          getOpponents(p1).forEach((player,index)=>player.group.position.set(7+index,0,7));
          p1.group.position.set(0,0,0);p1.angle=angleToward(p1.group.position,HOOP_POS);p1.group.rotation.y=p1.angle;
          takePossession(p1,{silent:true});inputManager.tick();
        };
      });
    };
    await open();
    await page.evaluate(()=>{connectVirtualPad(0);inputManager.tick();});
    assert.deepEqual(await page.evaluate(()=>Object.fromEntries(Object.entries(inputManager.getAssignments()).map(([slot,pad])=>[slot,pad?.index??null]))),{p1:0,p2:null});
    await page.evaluate(()=>{controllerTestStart(true);inputManager.tick();});
    assert.deepEqual(await page.evaluate(()=>Object.fromEntries(Object.entries(inputManager.getAssignments()).map(([slot,pad])=>[slot,pad?.index??null]))),{p1:null,p2:0});
    await page.evaluate(()=>{connectVirtualPad(1);inputManager.tick();});
    assert.deepEqual(await page.evaluate(()=>Object.fromEntries(Object.entries(inputManager.getAssignments()).map(([slot,pad])=>[slot,pad?.index??null]))),{p1:1,p2:0});
    console.log('PASS: one-controller solo/local defaults and stable second-controller hotplug assignment');

    await page.evaluate(()=>{
      controllerConfig.setAssignment('p1',{index:0,id:virtualPads[0].id});
      controllerConfig.setAssignment('p2',{index:1,id:virtualPads[1].id});
      inputManager.tick();controllerTestStart(true);
    });
    const movement=await page.evaluate(()=>{
      const before=[p1.group.position.clone(),p2.group.position.clone()];
      virtualPads[0].axes[0]=.9;virtualPads[1].axes[1]=-.9;virtualButton(0,7,.4);
      controllerTestFrames(24);
      return {p1:p1.group.position.distanceTo(before[0]),p2:p2.group.position.distanceTo(before[1]),
        one:inputManager.getState('p1'),two:inputManager.getState('p2')};
    });
    assert.ok(movement.p1>.4&&movement.p2>.4,JSON.stringify(movement));
    assert.ok(movement.one.moveX>.8&&movement.two.moveY>.8&&movement.one.sprint);
    assert.equal(Math.abs(movement.one.moveY),0);assert.equal(Math.abs(movement.two.moveX),0);
    console.log('PASS: two explicitly assigned controllers move separate players, including analog trigger sprint');

    await page.evaluate(()=>controllerTestStart(false));
    const shot=await page.evaluate(()=>{
      p1.group.position.set(0,0,HOOP_POS.z+1.6);p1.group.rotation.y=p1.angle=angleToward(p1.group.position,HOOP_POS);
      virtualButton(0,2);controllerTestFrames(6);
      const charging=shotCharging&&chargingPlayer===p1;
      virtualButton(0,2,0);controllerTestFrames(180);
      return {charging,attempts:p1.stats.fga,made:scoreP1,type:p1.moveMeta.lastShotType};
    });
    assert.ok(shot.charging);assert.equal(shot.attempts,1);assert.ok(shot.made>0);
    console.log('PASS: controller hold/release drives actual gather, physical layup and scoring');

    await page.evaluate(()=>controllerTestStart(false));
    const pass=await page.evaluate(()=>{
      const teammate=getTeammates(p1)[0];teammate.group.position.set(2,0,-1);
      p1.group.rotation.y=p1.angle=angleToward(p1.group.position,teammate.group.position);
      virtualButton(0,0);controllerTestFrames(1);
      return {state:ballState,attempts:p1.stats.fga};
    });
    assert.equal(pass.state,'pass');assert.equal(pass.attempts,0);
    await page.evaluate(()=>{neutralVirtualPads();controllerTestFrames(1);});
    const switched=await page.evaluate(()=>{const previous=p1.id;virtualButton(0,5);controllerTestFrames(1);return {previous,current:p1.id};});
    assert.notEqual(switched.current,switched.previous);
    await page.evaluate(()=>controllerTestStart(false));
    const defense=await page.evaluate(()=>{
      const opponent=getOpponents(p1)[0];takePossession(opponent,{silent:true});
      virtualButton(0,3);controllerTestFrames(1);
      const block=p1.blockTime>0&&!p1.onGround;
      neutralVirtualPads();controllerTestFrames(1);resetBody(p1);
      virtualButton(0,1);controllerTestFrames(1);
      return {block,steal:p1.swipeTime>0};
    });
    assert.ok(defense.block&&defense.steal,JSON.stringify(defense));
    const skills=await page.evaluate(()=>{
      neutralVirtualPads();controllerTestFrames(1);resetBody(p1);
      virtualButton(0,6,.6);virtualButton(0,3);controllerTestFrames(1);
      const handsUp=p1.onGround&&p1.handsUpTime>0;
      neutralVirtualPads();controllerTestFrames(1);resetBody(p1);takePossession(p1,{silent:true});
      crossover.active=false;ballSideValue=getBestHandForScreenSide(p1,'left')==='left'?1:-1;
      virtualButton(0,4);controllerTestFrames(1);const left=crossover.active&&crossover.player===p1;
      neutralVirtualPads();controllerTestFrames(1);crossover.active=false;
      const beforeSkill=p1.stamina;virtualPads[0].axes[3]=-.95;controllerTestFrames(1);
      const stick=p1.stamina<beforeSkill-.02;
      neutralVirtualPads();controllerTestFrames(1);p1.takeover=100;
      virtualButton(0,10);virtualButton(0,11);controllerTestFrames(1);
      return {handsUp,left,stick,takeover:p1.takeoverActive>0||p1.takeoverTimer>0};
    });
    assert.ok(skills.handsUp&&skills.left&&skills.stick&&skills.takeover,JSON.stringify(skills));
    await page.evaluate(()=>{neutralVirtualPads();controllerTestFrames(1);virtualButton(0,9);controllerTestFrames(1);});
    assert.ok(await page.evaluate(()=>gamePaused));
    await page.evaluate(()=>{neutralVirtualPads();controllerTestFrames(1);virtualButton(0,9);controllerTestFrames(1);});
    assert.equal(await page.evaluate(()=>gamePaused),false);
    console.log('PASS: pass, switch, block, steal, hands-up stance, bumper/stick skills, takeover and Start pause/resume reach gameplay');

    // Settings, capture, persistence and safety assertions are below so the
    // same physical inputs can be checked through both setup and gameplay.
    await page.evaluate(()=>{neutralVirtualPads();controllerTestStart(false);setPaused(true);showControlsScreen('__pause__');});
    await page.locator('[data-controller-player="p1"]').click();
    await page.locator('#controllerAssignment').selectOption('pad:0');
    await page.locator('[data-bind-action="shoot"]').click();
    await page.waitForFunction(()=>document.getElementById('controllerCaptureHint').textContent.startsWith('Press a button'));
    await page.evaluate(()=>{virtualButton(0,0);});
    await page.waitForFunction(()=>controllerConfig.getPlayer('p1').bindings.shoot?.index===0);
    assert.notEqual(await page.evaluate(()=>controllerConfig.getPlayer('p1').bindings.pass?.index),0,'remapping Shoot to A must resolve the Pass conflict');
    const captured=await page.evaluate(()=>({attempts:p1.stats.fga,charging:shotCharging,state:ballState}));
    assert.equal(captured.attempts,0);assert.equal(captured.charging,false);assert.equal(captured.state,'dribble');
    await page.evaluate(()=>{neutralVirtualPads();});
    console.log('PASS: entered controller button changes Shoot through Settings without leaking a shot or pass');

    const savedPass=await page.evaluate(()=>controllerConfig.getPlayer('p1').bindings.pass);
    await page.locator('[data-bind-action="pass"]').focus();
    await page.keyboard.press('Enter');
    assert.ok(await page.locator('#controllerCapturePanel').isVisible());
    await page.keyboard.press('Escape');
    assert.equal(await page.locator('#controllerCapturePanel').isVisible(),false);
    assert.ok(await page.locator('[data-bind-action="pass"]').evaluate(node=>node===document.activeElement));
    assert.deepEqual(await page.evaluate(()=>controllerConfig.getPlayer('p1').bindings.pass),savedPass);
    await page.locator('[data-controller-player="p2"]').click();
    await page.locator('#controllerAssignment').selectOption('pad:0');
    assert.equal(await page.evaluate(()=>controllerConfig.getPlayer('p1').assignment),'none');
    assert.equal(await page.evaluate(()=>controllerConfig.getPlayer('p2').assignment.index),0);
    await page.locator('#controllerAssignment').selectOption('pad:1');
    await page.locator('[data-bind-action="shoot"]').click();
    await page.waitForFunction(()=>document.getElementById('controllerCaptureHint').textContent.startsWith('Press a button'));
    await page.evaluate(()=>virtualButton(1,3));
    await page.waitForFunction(()=>controllerConfig.getPlayer('p2').bindings.shoot?.index===3);
    await page.evaluate(()=>neutralVirtualPads());
    await page.locator('[data-controller-player="p1"]').click();
    await page.locator('#controllerAssignment').selectOption('pad:0');
    assert.equal(await page.evaluate(()=>controllerConfig.getPlayer('p1').bindings.shoot.index),0);
    console.log('PASS: binding capture can be cancelled and assigning a device to another player prevents duplicate control');

    await page.locator('[data-stick-preset="skill"]').selectOption('off');
    await page.locator('[data-stick-preset="move"]').selectOption('right');
    assert.deepEqual(await page.evaluate(()=>controllerConfig.getPlayer('p1').axes.move),{x:2,y:3,invertX:false,invertY:true});
    await page.evaluate(()=>{virtualPads[0].axes[3]=-.9;});
    assert.ok(await page.evaluate(()=>inputManager.readGamepad(virtualPads[0],'p1').moveY>.8),'ordinary stick up moves forward');
    await page.locator('[data-invert-kind="move"][data-invert-component="invertY"]').check();
    assert.ok(await page.evaluate(()=>inputManager.readGamepad(virtualPads[0],'p1').moveY<-.8),'Reverse up/down reverses the actual movement input');
    await page.locator('[data-invert-kind="move"][data-invert-component="invertY"]').uncheck();
    await page.locator('[data-stick-preset="move"]').selectOption('custom');
    await page.locator('[data-axis-kind="move"][data-axis-component="x"]').selectOption('0');
    await page.locator('[data-axis-kind="move"][data-axis-component="y"]').selectOption('1');
    assert.deepEqual(await page.evaluate(()=>controllerConfig.getPlayer('p1').axes.move),{x:0,y:1,invertX:false,invertY:true});
    await page.locator('[data-stick-preset="move"]').selectOption('off');
    await page.evaluate(()=>{virtualPads[0].axes[0]=.9;virtualPads[0].axes[1]=-.9;});
    const disabled=await page.evaluate(()=>{const state=inputManager.readGamepad(virtualPads[0],'p1');return Math.hypot(state.moveX,state.moveY);});
    assert.equal(disabled,0);
    await page.evaluate(()=>neutralVirtualPads());
    await page.locator('[data-stick-preset="move"]').selectOption('left');
    await page.locator('[data-stick-preset="skill"]').selectOption('right');
    console.log('PASS: movement/skill stick selection, axis-by-axis customization, inversion and disabling affect actual input');

    await page.locator('.controller-manual summary').click();
    await page.locator('#controllerManualAction').selectOption('sprint');
    await page.locator('#controllerManualInput').selectOption('button:12');
    await page.locator('#controllerManualApply').click();
    assert.equal(await page.evaluate(()=>controllerConfig.getPlayer('p1').bindings.sprint.index),12);
    await page.evaluate(()=>virtualButton(0,12));
    assert.ok(await page.evaluate(()=>inputManager.readGamepad(virtualPads[0],'p1').sprint));
    await page.evaluate(()=>neutralVirtualPads());
    await page.locator('.controller-manual summary').click();
    await page.locator('#controllerComboCapture').check();
    await page.locator('[data-bind-action="takeover"]').click();
    await page.waitForFunction(()=>document.getElementById('controllerCaptureHint').textContent.startsWith('Press two inputs'));
    await page.evaluate(()=>{virtualButton(0,8);virtualButton(0,10);});
    await page.waitForFunction(()=>controllerConfig.getPlayer('p1').bindings.takeover?.inputs?.some(input=>input.index===8));
    assert.equal(await page.locator('.controller-bind-button.is-listening').count(),0);
    assert.equal(await page.locator('#controllerCapturePanel').isVisible(),false);
    await page.evaluate(()=>neutralVirtualPads());
    await page.locator('#controllerComboCapture').uncheck();
    console.log('PASS: manual D-pad binding and recorded two-input combos save and feed the real input reader');

    if(process.env.BASKETBALL_SCREENSHOTS){
      fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});
      await page.evaluate(()=>{document.getElementById('menuBox').scrollTop=0;});
      await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'basketball2-controller-settings.png'),fullPage:true});
    }
    await page.locator('#controlsBackBtn').click();
    await page.locator('#pauseResumeBtn').click();
    const remapped=await page.evaluate(()=>{
      p1.group.position.set(0,0,HOOP_POS.z+1.6);p1.group.rotation.y=p1.angle=angleToward(p1.group.position,HOOP_POS);
      inputManager.tick();virtualButton(0,0);controllerTestFrames(6);
      const charging=shotCharging;virtualButton(0,0,0);controllerTestFrames(180);
      return {charging,attempts:p1.stats.fga,made:scoreP1};
    });
    assert.ok(remapped.charging);assert.equal(remapped.attempts,1);assert.ok(remapped.made>0);
    console.log('PASS: newly entered A binding takes a real made layup instead of passing');

    const secondPlayer=await page.evaluate(()=>{
      controllerTestStart(true);getOpponents(p2).forEach(player=>player.group.position.set(8,0,8));
      p2.group.position.set(0,0,HOOP_POS.z+1.6);p2.group.rotation.y=p2.angle=angleToward(p2.group.position,HOOP_POS);
      takePossession(p2,{silent:true});virtualButton(1,3);controllerTestFrames(6);
      const charging=shotCharging&&chargingPlayer===p2;virtualButton(1,3,0);controllerTestFrames(180);
      return {charging,attempts:p2.stats.fga,made:scoreP2,one:controllerConfig.getPlayer('p1').bindings.shoot.index,two:controllerConfig.getPlayer('p2').bindings.shoot.index};
    });
    assert.ok(secondPlayer.charging&&secondPlayer.made>0,JSON.stringify(secondPlayer));assert.equal(secondPlayer.attempts,1);
    assert.equal(secondPlayer.one,0);assert.equal(secondPlayer.two,3);
    console.log('PASS: Player 2 captures an independent Y binding and makes a layup with the second controller');

    await page.evaluate(()=>controllerTestStart(false));
    const disconnected=await page.evaluate(()=>{
      virtualButton(0,0);controllerTestFrames(6);const charging=shotCharging;
      disconnectVirtualPad(0);controllerTestFrames(1);
      return {charging,paused:gamePaused,cancelled:!shotCharging,attempts:p1.stats.fga};
    });
    assert.ok(disconnected.charging&&disconnected.paused&&disconnected.cancelled);assert.equal(disconnected.attempts,0);
    const reconnect=await page.evaluate(()=>{
      connectVirtualPad(0);virtualButton(0,0);controllerTestFrames(1);
      setPaused(false);controllerTestFrames(4);
      const heldIgnored=!shotCharging&&p1.stats.fga===0;
      virtualButton(0,0,0);controllerTestFrames(2);
      virtualButton(0,0);controllerTestFrames(3);
      const freshPress=shotCharging;virtualButton(0,0,0);controllerTestFrames(1);
      return {heldIgnored,freshPress};
    });
    assert.ok(reconnect.heldIgnored&&reconnect.freshPress,JSON.stringify(reconnect));
    console.log('PASS: unplug cancels charged shots and pauses; reconnect requires neutral then a fresh press');

    await page.evaluate(()=>{controllerTestStart(false);setPaused(true);showControlsScreen('__pause__');});
    await page.locator('[data-bind-action="shoot"]').click();
    await page.keyboard.down('Escape');
    assert.equal(await page.locator('#controllerCapturePanel').isVisible(),false);
    await page.locator('#controlsBackBtn').click();await page.locator('#pauseResumeBtn').click();
    await page.keyboard.down('Escape'); // A key-repeat event while Escape stays held.
    await page.evaluate(()=>controllerTestFrames(2));
    assert.equal(await page.evaluate(()=>gamePaused),false,'held Escape used to cancel capture must not pause after returning to the game');
    await page.keyboard.up('Escape');await page.evaluate(()=>controllerTestFrames(1));
    await page.keyboard.down('Escape');await page.evaluate(()=>controllerTestFrames(1));
    assert.equal(await page.evaluate(()=>gamePaused),true,'fresh Escape pauses after the held key is released');
    await page.keyboard.up('Escape');
    console.log('PASS: Escape capture cancel stays blocked through mouse Back/Resume and key repeat until release');

    await page.evaluate(()=>{controllerTestStart(false);gameSettings.set('controlStyle','classic');setPaused(true);showControlsScreen('__pause__');});
    await page.keyboard.down('w');await page.keyboard.down('Space');
    await page.locator('#controlsBackBtn').click();await page.locator('#pauseResumeBtn').click();
    const heldKeyboard=await page.evaluate(()=>{const position=p1.group.position.clone();controllerTestFrames(3);return {distance:p1.group.position.distanceTo(position),charging:shotCharging};});
    assert.ok(heldKeyboard.distance<.001&&!heldKeyboard.charging,JSON.stringify(heldKeyboard));
    await page.keyboard.up('w');await page.keyboard.up('Space');
    await page.evaluate(()=>controllerTestFrames(1));
    await page.keyboard.down('w');
    const freshKeyboard=await page.evaluate(()=>{const position=p1.group.position.clone();controllerTestFrames(12);return p1.group.position.distanceTo(position);});
    await page.keyboard.up('w');assert.ok(freshKeyboard>.1);
    console.log('PASS: held keyboard inputs cannot leak from Settings into Classic controls; a fresh press works');

    await open();
    assert.equal(await page.evaluate(()=>controllerConfig.getPlayer('p1').bindings.shoot.index),0);
    await page.evaluate(()=>{connectVirtualPad(0);connectVirtualPad(1);inputManager.tick();});
    assert.deepEqual(await page.evaluate(()=>Object.fromEntries(Object.entries(inputManager.getAssignments()).map(([slot,pad])=>[slot,pad?.index??null]))),{p1:0,p2:null});
    await page.evaluate(()=>{twoPlayerMode=true;inputManager.tick();});
    assert.deepEqual(await page.evaluate(()=>Object.fromEntries(Object.entries(inputManager.getAssignments()).map(([slot,pad])=>[slot,pad?.index??null]))),{p1:0,p2:1});
    console.log('PASS: bindings and explicit player assignments persist after page reload');

    await page.locator('#primaryControlsBtn').click();
    await page.locator('[data-controller-player="p1"]').click();
    await page.locator('#controllerReset').click();
    assert.equal(await page.evaluate(()=>controllerConfig.getPlayer('p1').bindings.shoot.index),2);
    await page.setViewportSize({width:390,height:844});
    await page.locator('[data-bind-action="shoot"]').scrollIntoViewIfNeeded();
    const layout=await page.evaluate(()=>({viewport:innerWidth,width:document.documentElement.scrollWidth,
      controls:[...document.querySelectorAll('#controllerSettingsHost button,#controllerSettingsHost select')].filter(node=>node.getBoundingClientRect().width).map(node=>({left:node.getBoundingClientRect().left,right:node.getBoundingClientRect().right}))}));
    assert.ok(layout.width<=layout.viewport+1,JSON.stringify(layout));
    assert.ok(layout.controls.every(rect=>rect.left>=-1&&rect.right<=layout.viewport+1),JSON.stringify(layout));
    if(process.env.BASKETBALL_SCREENSHOTS)await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'basketball2-controller-mobile.png'),fullPage:true});
    assert.deepEqual(errors,[]);
    console.log('PASS: reset restores Xbox defaults and controller settings fit a narrow mobile viewport without browser exceptions');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
