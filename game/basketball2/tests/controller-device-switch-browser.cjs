/* Real-browser input-device selection checks. Only the physical Gamepad
 * API is simulated: Settings, keyboard events, gameplay and shot physics run.
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
    const settings=async(player='p1')=>{
      await page.evaluate(()=>{neutralVirtualPads();setPaused(true);showControlsScreen('__pause__');});
      await page.locator(`[data-controller-player="${player}"]`).click();
    };
    const start=async(local=false,style='modern')=>page.evaluate(({local,style})=>{
      controllerTestStart(local);gameSettings.set('controlStyle',style);
      // Each independent approach begins from rest, far from court boundaries.
      for(const player of [p1,p2]){
        player.group.position.set(player===p1?-2:2,0,0);
        player.velocity.set(0,0,0);
      }
      inputManager.tick();
    },{local,style});
    const resume=async()=>{
      await page.locator('#controlsBackBtn').click();
      await page.locator('#pauseResumeBtn').click();
      await page.evaluate(()=>{neutralVirtualPads();controllerTestFrames(2);});
    };
    await open();
    await page.locator('#primaryControlsBtn').click();
    assert.equal(await page.locator('#controllerInputMode').inputValue(),'auto');
    await page.locator('#controllerAssignment').selectOption('none');
    await page.locator('#controllerInputMode').selectOption('controller');
    assert.equal(await page.evaluate(()=>controllerConfig.getPlayer('p1').inputMode),'controller');
    assert.equal(await page.evaluate(()=>controllerConfig.getPlayer('p1').assignment),'auto','switching from no controller enables automatic discovery');
    assert.match(await page.locator('#controllerAssignmentHint').innerText(),/connect|waiting|available/i);
    assert.equal(await page.evaluate(()=>inputManager.getAssignments().p1),null);
    await page.evaluate(()=>connectVirtualPad(0));
    await page.waitForFunction(()=>document.getElementById('controllerConnectionBadge').textContent.includes('1 controller'));
    await page.locator('#controllerInputMode').selectOption('keyboard');
    await page.locator('[data-use-controller="0"]').click();
    assert.equal(await page.locator('#controllerInputMode').inputValue(),'controller');
    assert.equal(await page.locator('#controllerAssignment').inputValue(),'pad:0');
    console.log('PASS: Settings can select Controller before hotplug, then Use this controller assigns the detected device');

    await page.locator('[data-bind-action="shoot"]').click();
    await page.waitForFunction(()=>document.getElementById('controllerCaptureHint').textContent.startsWith('Press a button'));
    await page.evaluate(()=>virtualButton(0,0));
    await page.waitForFunction(()=>controllerConfig.getPlayer('p1').bindings.shoot?.index===0);
    await page.evaluate(()=>neutralVirtualPads());
    assert.notEqual(await page.evaluate(()=>controllerConfig.getPlayer('p1').bindings.pass?.index),0);
    const savedPass=await page.evaluate(()=>controllerConfig.getPlayer('p1').bindings.pass);
    await page.locator('[data-bind-action="pass"]').click();
    assert.ok(await page.locator('#controllerCapturePanel').isVisible());
    await page.locator('#controllerInputMode').selectOption('keyboard');
    assert.equal(await page.locator('#controllerCapturePanel').isVisible(),false);
    assert.deepEqual(await page.evaluate(()=>controllerConfig.getPlayer('p1').bindings.pass),savedPass);
    await page.locator('#controllerInputMode').selectOption('controller');
    console.log('PASS: Shoot can be remapped through the selected controller without retaining an overlapping Pass');

    for(const style of ['modern','classic']){
      await start(false,style);
      await page.keyboard.down('w');await page.keyboard.down('Space');
      const keyboardIgnored=await page.evaluate(()=>{
        const before=p1.group.position.clone();controllerTestFrames(18);
        return {distance:p1.group.position.distanceTo(before),charging:shotCharging,attempts:p1.stats.fga,state:inputManager.getState('p1')};
      });
      await page.keyboard.up('w');await page.keyboard.up('Space');
      assert.ok(keyboardIgnored.distance<.01&&!keyboardIgnored.charging&&keyboardIgnored.attempts===0,`${style}: ${JSON.stringify(keyboardIgnored)}`);
      assert.equal(keyboardIgnored.state.moveY,0);
      const movement=await page.evaluate(()=>{
        const before=p1.group.position.clone();virtualPads[0].axes[0]=.9;controllerTestFrames(24);
        return {distance:p1.group.position.distanceTo(before),state:inputManager.getState('p1')};
      });
      assert.ok(movement.distance>.4&&movement.state.moveX>.8,`${style}: ${JSON.stringify(movement)}`);
      assert.match(movement.state.source,/gamepad|playstation/);
      await page.evaluate(()=>{neutralVirtualPads();controllerTestFrames(2);});
    }
    await start();
    const shot=await page.evaluate(()=>{
      p1.group.position.set(0,0,HOOP_POS.z+1.6);p1.group.rotation.y=p1.angle=angleToward(p1.group.position,HOOP_POS);
      virtualButton(0,0);controllerTestFrames(6);const charging=shotCharging&&chargingPlayer===p1;
      virtualButton(0,0,0);controllerTestFrames(24);const mode=p1.moveMeta.lastShotType;
      controllerTestFrames(156);
      return {charging,attempts:p1.stats.fga,mode};
    });
    assert.ok(shot.charging);assert.equal(shot.attempts,1);assert.ok(['layup','dunk'].includes(shot.mode),JSON.stringify(shot));
    console.log('PASS: Controller mode ignores keyboard movement/shooting in Modern and Classic, while the stick and remapped shot reach gameplay');

    await start();
    await page.keyboard.down('Escape');await page.evaluate(()=>controllerTestFrames(1));await page.keyboard.up('Escape');
    assert.equal(await page.evaluate(()=>gamePaused),true,'Escape remains available for recovery in Controller mode');
    await page.evaluate(()=>controllerTestFrames(1));
    await page.keyboard.down('Escape');await page.evaluate(()=>controllerTestFrames(1));await page.keyboard.up('Escape');
    assert.equal(await page.evaluate(()=>gamePaused),false);
    await settings();
    await page.locator('#controllerInputMode').selectOption('keyboard');
    await resume();
    for(const style of ['modern','classic']){
      await start(false,style);
      const padIgnored=await page.evaluate(()=>{
        const before=p1.group.position.clone();virtualPads[0].axes[0]=.9;virtualButton(0,0);controllerTestFrames(18);
        return {distance:p1.group.position.distanceTo(before),charging:shotCharging,attempts:p1.stats.fga,state:inputManager.getState('p1')};
      });
      assert.ok(padIgnored.distance<.01&&!padIgnored.charging&&padIgnored.attempts===0,`${style}: ${JSON.stringify(padIgnored)}`);
      assert.equal(padIgnored.state.moveX,0);
      await page.evaluate(()=>{neutralVirtualPads();controllerTestFrames(1);});
      await page.keyboard.down('w');
      const distance=await page.evaluate(()=>{const before=p1.group.position.clone();controllerTestFrames(24);return p1.group.position.distanceTo(before);});
      await page.keyboard.up('w');assert.ok(distance>.4,`${style}: keyboard distance ${distance}`);
    }
    console.log('PASS: Keyboard / touch mode ignores a connected controller and restores keyboard movement in both control styles');

    await settings();
    await page.locator('#controllerInputMode').selectOption('controller');
    await page.locator('#controllerAssignment').selectOption('pad:0');
    await page.locator('[data-controller-player="p2"]').click();
    await page.locator('#controllerInputMode').selectOption('keyboard');
    await start(true);
    await page.keyboard.down('w');await page.keyboard.down('i');
    const local=await page.evaluate(()=>{
      const before=[p1.group.position.clone(),p2.group.position.clone()];
      virtualPads[0].axes[0]=.9;controllerTestFrames(24);
      return {one:inputManager.getState('p1'),two:inputManager.getState('p2'),distance:[p1.group.position.distanceTo(before[0]),p2.group.position.distanceTo(before[1])],
        modes:['p1','p2'].map(slot=>controllerConfig.getPlayer(slot).inputMode),assignments:Object.fromEntries(Object.entries(inputManager.getAssignments()).map(([slot,pad])=>[slot,pad?.index??null]))};
    });
    await page.keyboard.up('w');await page.keyboard.up('i');await page.evaluate(()=>neutralVirtualPads());
    assert.deepEqual(local.modes,['controller','keyboard']);assert.deepEqual(local.assignments,{p1:0,p2:null});
    assert.ok(local.distance.every(distance=>distance>.4),JSON.stringify(local));
    assert.ok(local.one.moveX>.8&&Math.abs(local.one.moveY)<.001,JSON.stringify(local));
    assert.ok(local.two.moveY>.8&&Math.abs(local.two.moveX)<.001,JSON.stringify(local));
    console.log('PASS: Player 1 controller and Player 2 keyboard act independently in the same local match');

    await open();
    assert.equal(await page.evaluate(()=>controllerConfig.getPlayer('p1').inputMode),'controller');
    assert.equal(await page.evaluate(()=>controllerConfig.getPlayer('p2').inputMode),'keyboard');
    assert.equal(await page.evaluate(()=>controllerConfig.getPlayer('p1').bindings.shoot.index),0);
    await page.evaluate(()=>{connectVirtualPad(0);inputManager.tick();});
    await start();
    const disconnect=await page.evaluate(()=>{
      virtualButton(0,0);controllerTestFrames(6);const charging=shotCharging;
      disconnectVirtualPad(0);controllerTestFrames(1);
      return {charging,paused:gamePaused,cancelled:!shotCharging,attempts:p1.stats.fga,state:inputManager.getState('p1')};
    });
    assert.ok(disconnect.charging&&disconnect.paused&&disconnect.cancelled,JSON.stringify(disconnect));
    assert.equal(disconnect.attempts,0);assert.equal(Math.hypot(disconnect.state.moveX,disconnect.state.moveY),0);
    const reconnect=await page.evaluate(()=>{
      connectVirtualPad(0);virtualButton(0,0);controllerTestFrames(1);setPaused(false);controllerTestFrames(4);
      const heldIgnored=!shotCharging&&p1.stats.fga===0;
      virtualButton(0,0,0);controllerTestFrames(2);virtualButton(0,0);controllerTestFrames(3);
      const freshPress=shotCharging;virtualButton(0,0,0);controllerTestFrames(1);
      return {heldIgnored,freshPress};
    });
    assert.ok(reconnect.heldIgnored&&reconnect.freshPress,JSON.stringify(reconnect));
    console.log('PASS: per-player modes and custom inputs survive reload; unplug pauses and cancels charging, and reconnect needs a fresh press');

    await settings();
    assert.equal(await page.locator('#controllerInputMode').inputValue(),'controller');
    await page.locator('[data-controller-player="p2"]').click();
    assert.equal(await page.locator('#controllerInputMode').inputValue(),'keyboard');
    await page.locator('[data-controller-player="p1"]').click();
    if(process.env.BASKETBALL_SCREENSHOTS){
      fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});
      await page.evaluate(()=>document.getElementById('menuBox').scrollTop=0);
      await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'controller-device-switch-desktop.png'),fullPage:true});
    }
    await page.setViewportSize({width:390,height:844});
    await page.locator('#controllerInputMode').scrollIntoViewIfNeeded();
    const layout=await page.evaluate(()=>({width:document.documentElement.scrollWidth,viewport:innerWidth,
      controls:[...document.querySelectorAll('#controllerSettingsHost button,#controllerSettingsHost select')].filter(node=>node.getBoundingClientRect().width).map(node=>({left:node.getBoundingClientRect().left,right:node.getBoundingClientRect().right}))}));
    assert.ok(layout.width<=layout.viewport+1&&layout.controls.every(rect=>rect.left>=-1&&rect.right<=layout.viewport+1),JSON.stringify(layout));
    if(process.env.BASKETBALL_SCREENSHOTS){
      await page.evaluate(()=>document.getElementById('menuBox').scrollTop=0);
      await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'controller-device-switch-mobile.png'),fullPage:true});
    }
    assert.deepEqual(errors,[]);
    console.log('PASS: saved input choices are shown on each player tab and device selection fits desktop and narrow Settings without browser errors');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
