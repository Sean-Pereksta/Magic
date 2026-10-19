const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

class Vector3 {
  constructor(x=0,y=0,z=0){ this.set(x,y,z); }
  set(x,y,z){ this.x=x; this.y=y; this.z=z; return this; }
  copy(v){ return this.set(v.x,v.y,v.z); }
  clone(){ return new Vector3(this.x,this.y,this.z); }
  add(v){ this.x+=v.x; this.y+=v.y; this.z+=v.z; return this; }
  sub(v){ this.x-=v.x; this.y-=v.y; this.z-=v.z; return this; }
  multiplyScalar(s){ this.x*=s; this.y*=s; this.z*=s; return this; }
  lerp(v,t){ this.x+=(v.x-this.x)*t; this.y+=(v.y-this.y)*t; this.z+=(v.z-this.z)*t; return this; }
  setY(y){ this.y=y; return this; }
  lengthSq(){ return this.x*this.x+this.y*this.y+this.z*this.z; }
  length(){ return Math.sqrt(this.lengthSq()); }
  normalize(){ return this.multiplyScalar(1/(this.length()||1)); }
  dot(v){ return this.x*v.x+this.y*v.y+this.z*v.z; }
  addScaledVector(v,s){ this.x+=v.x*s; this.y+=v.y*s; this.z+=v.z*s; return this; }
  lerpVectors(a,b,t){ return this.copy(a).lerp(b,t); }
}

function element(id,action){
  const handlers = {};
  const classes = new Set();
  const knob = {style:{}};
  return {
    id,dataset:{touchAction:action},style:{},textContent:"",
    classList:{remove:(c)=>classes.delete(c),add:(c)=>classes.add(c),contains:(c)=>classes.has(c),toggle:(c,on)=>on?classes.add(c):classes.delete(c)},
    addEventListener(name,handler){ handlers[name]=handler; },
    fire(type,pointerId=1,extra={}){ handlers[type]?.({type,pointerId,clientX:50,clientY:50,preventDefault(){},...extra}); },
    setPointerCapture(){},querySelector:()=>knob,
    getBoundingClientRect:()=>({left:0,top:0,width:100,height:100}),
    setAttribute(name,value){ this[name]=value; }
  };
}

function context(){
  const nodes = {
    moveStick:element("moveStick"),
    touchPrimary:element("touchPrimary","shoot"),
    touchSecondary:element("touchSecondary","pass"),
    p1HudName:element("p1HudName"),p2HudName:element("p2HudName"),
    p1Label:element("p1Label"),p2Label:element("p2Label"),scoreText:element("scoreText"),msg:element("msg")
  };
  const events = {};
  const documentEvents = {};
  const sandbox = {
    THREE:{Vector3,MathUtils:{clamp:(n,a,b)=>Math.max(a,Math.min(b,n)),lerp:(a,b,t)=>a+(b-a)*t}},
    window:{innerWidth:1281,innerHeight:721,addEventListener:(n,f)=>{events[n]=f;},dispatchEvent(){}},
    document:{
      hidden:false,body:element("body"),
      addEventListener:(n,f)=>{documentEvents[n]=f;},
      getElementById:(id)=>nodes[id]||null,
      createElement:()=>element(""),
      querySelectorAll:(selector)=>selector==="[data-touch-action]"?[nodes.touchPrimary,nodes.touchSecondary]:[]
    },
    navigator:{maxTouchPoints:1,getGamepads:()=>[]},
    performance:{now:()=>1000},
    CustomEvent:class{},
    setTimeout:()=>1,clearTimeout(){},
    matchMedia:()=>({matches:true}),
    keys:{},gameStarted:true,gameOver:false,gamePaused:false,
    twoPlayerMode:false,teamSize:3,localTeamMode:"cooperative",splitLayout:"vertical",
    shotCharging:false,chargingPlayer:null,shotPower:0,uiHidden:false,
    scoreP1:0,scoreP2:0,messageTimer:0,tutorialMode:false,
    ballOwner:"p1",possession:"p1",screenShake:1,
    cameraEffects:{shake:1,zoomPulse:1,shakeVel:1,impulse:1},
    cameraState:Object.fromEntries(["main","p1","p2"].map(key=>[key,{look:new Vector3(),lead:new Vector3()}])),
    HOOP_POS:new Vector3(0,3.28,-10.8),
    ball:{position:new Vector3(0,1,4)},
    nodes,events,documentEvents
  };
  sandbox.document.body.appendChild=(node)=>{ nodes[node.id]=node; };
  const ctx = vm.createContext(sandbox);
  vm.runInContext(fs.readFileSync(path.join(__dirname,"../js/core/controller-config.js"),"utf8"),ctx);
  vm.runInContext(fs.readFileSync(path.join(__dirname,"../js/core/input-manager.js"),"utf8"),ctx);
  vm.runInContext("globalThis.manager = inputManager",ctx);
  return ctx;
}

function character(id,x=0,z=0,slot=id){
  return {id,profile:{name:id},controllerSlot:slot,teamId:slot==="p1"?0:1,group:{position:new Vector3(x,0,z),rotation:{y:0}},angle:0};
}

function loadPresentation(ctx){
  ctx.p1=character("p1",2,4);
  ctx.p2=character("p2",-2,0);
  ctx.matchPlayers=[ctx.p1,ctx.p2];
  const camera=()=>({position:new Vector3(),aspect:1,lookAt(v){ this.focus=v.clone(); },updateProjectionMatrix(){ this.projections=(this.projections||0)+1; }});
  ctx.camMain=camera();ctx.camP1=camera();ctx.camP2=camera();
  ctx.renderer={views:[],setScissorTest(){},clear(){},setViewport(...rect){this.views.push(rect);},setScissor(){},render(){}};
  ctx.scene={};
  vm.runInContext(fs.readFileSync(path.join(__dirname,"../js/presentation.js"),"utf8"),ctx);
}

test("court cameras retain one orientation and distance during turns, shots, and possession changes",()=>{
  const ctx=context();loadPresentation(ctx);
  ctx.twoPlayerMode=true;
  let offset;
  for(let i=0;i<120;i++){
    ctx.p1.angle=i*.4;
    ctx.p2.angle=-i*.3;
    ctx.p1.group.position.set(Math.sin(i*.1)*6,0,Math.cos(i*.08)*8);
    ctx.ball.position.set(Math.sin(i*.13)*5,Math.sin(i*.1)*3+4,-i%11);
    ctx.possession=i%2?"p1":"p2";
    vm.runInContext("updateCameras(1/60)",ctx);
    for(const cam of [ctx.camP1,ctx.camP2]){
      const delta=cam.position.clone().sub(cam.focus);
      if(!offset) offset=delta;
      assert.ok(Math.abs(delta.x-offset.x)<1e-12);
      assert.ok(Math.abs(delta.y-offset.y)<1e-12);
      assert.ok(Math.abs(delta.z-offset.z)<1e-12);
    }
  }
  assert.equal(ctx.screenShake,0);
});

test("close fixed camera retains the ball, basket, controlled body, and immediate defender at court extremes",()=>{
  const ctx=context();loadPresentation(ctx);
  const project=(point,cam,aspect)=>{
    const delta=point.clone().sub(cam.position);
    const offset=cam.position.clone().sub(cam.focus),length=offset.length();
    const depth=(-delta.y*offset.y-delta.z*offset.z)/length;
    const vertical=(delta.y*offset.z-delta.z*offset.y)/length;
    const tangent=Math.tan(70*Math.PI/360);
    return {x:delta.x/(depth*tangent*aspect),y:vertical/(depth*tangent)};
  };
  for(const aspect of [844/390,640/720,1280/360]){
    for(const x of [-7.6,7.6]) for(const z of [-11.2,11.2]) for(const highShot of [false,true]){
      ctx.p1.group.position.set(x,0,z);
      ctx.p2.group.position.set(x-Math.sign(x)*5,0,z-1);
      ctx.ball.position.copy(highShot?ctx.HOOP_POS:ctx.p1.group.position).setY(highShot?8:1.4);
      for(let frame=0;frame<240;frame++) vm.runInContext('updateFollowCamera(camMain,p1,1/60)',ctx);
      const points=[ctx.ball.position,ctx.HOOP_POS,ctx.p1.group.position.clone().setY(1),ctx.p2.group.position.clone().setY(1),ctx.camMain.focus];
      for(const point of points){
        const screen=project(point,ctx.camMain,aspect);
        assert.ok(Math.abs(screen.x)<1&&Math.abs(screen.y)<1,JSON.stringify({aspect,x,z,highShot,point,screen}));
      }
    }
  }
});

test("split rendering fills odd-sized windows in both layouts with correct camera aspects",()=>{
  const ctx=context();loadPresentation(ctx);ctx.twoPlayerMode=true;
  vm.runInContext("renderScene()",ctx);
  assert.deepEqual(ctx.renderer.views,[[0,0,640,721],[640,0,641,721]]);
  assert.equal(ctx.camP1.aspect,640/721);
  ctx.renderer.views=[];ctx.splitLayout="horizontal";
  vm.runInContext("renderScene()",ctx);
  assert.deepEqual(ctx.renderer.views,[[0,361,1281,360],[0,0,1281,361]]);
  assert.equal(ctx.camP2.aspect,1281/361);
});

test("keyboard and controller pass/switch inputs retain independent local assignments",()=>{
  const ctx=context(), m=ctx.manager;
  m.keys.KeyC=true;m.keys.Period=true;
  assert.equal(m.readKeyboard("p1").pass,true);
  assert.equal(m.readKeyboard("p1").switchPlayer,false);
  assert.equal(m.readKeyboard("p2").pass,false);
  assert.equal(m.readKeyboard("p2").switchPlayer,true);
  const pad=(index)=>({index,id:"Xbox",axes:[0,0,0,0],buttons:Array.from({length:12},()=>({pressed:false,value:0}))});
  const a=pad(0),b=pad(1);a.buttons[0].pressed=true;b.buttons[5].pressed=true;
  ctx.twoPlayerMode=true;
  assert.equal(m.getAssignedGamepad("p1",[a]),null);
  assert.equal(m.getAssignedGamepad("p2",[a]),a);
  assert.equal(m.getAssignedGamepad("p1",[a,b]),b);
  assert.equal(m.getAssignedGamepad("p2",[a,b]),a);
  assert.equal(m.readGamepad(a,"p1").pass,true);
  assert.equal(m.readGamepad(b,"p2").switchPlayer,true);
  ctx.teamSize=1;
  assert.equal(m.readGamepad(b,"p2").switchPlayer,false);
  assert.equal(m.readGamepad(b,"p2").rightSkill,true);
});

test("touch captures held actions across label changes and sprints only at the outer edge",()=>{
  const ctx=context(),m=ctx.manager;
  m.attachTouchControls();
  ctx.nodes.touchPrimary.fire("pointerdown",7);
  m.tick();assert.equal(m.getState("p1").pressed.shoot,true);
  ctx.nodes.touchPrimary.dataset.touchAction="block";
  ctx.nodes.touchPrimary.fire("pointerup",7);
  m.tick();
  assert.equal(m.getState("p1").released.shoot,true);
  assert.equal(m.getState("p1").block,false);
  m.touch.moveX=.6;assert.equal(m.readTouch("p1").sprint,false);
  m.touch.moveX=.9;assert.equal(m.readTouch("p1").sprint,true);
  assert.equal(m.readTouch("p2").moveX,0);
});

test("touch cancellation and focus loss clear every held input without shooting",()=>{
  const ctx=context(),m=ctx.manager;m.attachTouchControls();
  ctx.nodes.touchPrimary.fire("pointerdown",8);
  ctx.nodes.moveStick.fire("pointerdown",9,{clientX:92,clientY:50});
  m.keys.KeyC=true;m.pulse("p2","pass");m.tick();
  ctx.shotCharging=true;ctx.chargingPlayer={id:"p1"};ctx.shotPower=.7;
  ctx.nodes.touchPrimary.fire("pointercancel",8);
  assert.equal(ctx.shotCharging,false);
  assert.equal(ctx.chargingPlayer,null);
  m.tick();
  for(const slot of ["p1","p2"]){
    assert.equal(m.getState(slot).moveX,0);
    assert.equal(m.getState(slot).pass,false);
    assert.equal(m.getState(slot).released.shoot,false);
  }
  m.keys.KeyW=true;ctx.events.blur();m.tick();
  assert.equal(m.getState("p1").moveY,0);
  ctx.events.focus();m.tick();
  assert.equal(m.getState("p1").moveY,0);
});

test("focus loss pauses active games until the user resumes and unknown slots stay idle",()=>{
  const ctx=context(),m=ctx.manager;
  ctx.setPaused=(paused)=>{ctx.gamePaused=paused;};
  m.keys.Space=true;m.tick();
  assert.equal(m.getState("p1").shoot,true);
  assert.equal(m.getState("unknown-ai").shoot,false);
  ctx.events.blur();assert.equal(ctx.gamePaused,true);
  ctx.events.focus();assert.equal(ctx.gamePaused,true);
  ctx.gamePaused=false;ctx.document.hidden=true;
  ctx.documentEvents.visibilitychange();assert.equal(ctx.gamePaused,true);
});

test("multiple fingers on one action cannot clear another finger's hold",()=>{
  const ctx=context(),m=ctx.manager;m.attachTouchControls();
  ctx.nodes.touchPrimary.fire("pointerdown",1);
  ctx.nodes.touchPrimary.fire("pointerdown",2);
  ctx.nodes.touchPrimary.fire("pointerup",1);
  assert.equal(m.readTouch("p1").shoot,true);
  ctx.nodes.touchPrimary.fire("pointerup",2);
  assert.equal(m.readTouch("p1").shoot,false);
});

test("haptics follow individual controller slots after switches and ignore AI players",()=>{
  const ctx=context(),m=ctx.manager;ctx.twoPlayerMode=true;
  const pulses=[];
  const pad=(index)=>({index,vibrationActuator:{playEffect:()=>{pulses.push(index);return Promise.resolve();}}});
  ctx.navigator.getGamepads=()=>[pad(0),pad(1)];
  ctx.getPlayerById=(id)=>({controllerSlot:id==="t0a1"?"p1":id==="t1a1"?"p2":null});
  m.vibrate("t0a1");m.vibrate("t1a1");m.vibrate("p1");
  assert.deepEqual(pulses,[0,1]);
});

test("touch buttons follow team possession when an AI teammate carries the ball",()=>{
  const ctx=context();loadPresentation(ctx);
  ctx.p1.teamId=0;ctx.p2.teamId=1;
  ctx.aiTeammate={id:"t0a1",teamId:0};
  ctx.getHumanPlayer=()=>ctx.p1;
  ctx.getPlayerById=(id)=>id==="t0a1"?ctx.aiTeammate:ctx.p2;
  ctx.getBallHandler=()=>ctx.aiTeammate;ctx.possession="t0a1";
  vm.runInContext(fs.readFileSync(path.join(__dirname,"../js/core/integrations.js"),"utf8"),ctx);
  vm.runInContext("updateTouchPresentation()",ctx);
  assert.equal(ctx.nodes.touchPrimary.dataset.touchAction,"shoot");
  assert.equal(ctx.nodes.touchSecondary.dataset.touchAction,"pass");
  ctx.getBallHandler=()=>ctx.p2;
  vm.runInContext("updateTouchPresentation()",ctx);
  assert.equal(ctx.nodes.touchPrimary.dataset.touchAction,"block");
  assert.equal(ctx.nodes.touchSecondary.dataset.touchAction,"steal");
});

test("desktop touch hardware stays hidden until touch input is used",()=>{
  const ctx=context();loadPresentation(ctx);
  ctx.matchMedia=()=>({matches:false});ctx.navigator.maxTouchPoints=10;
  vm.runInContext(fs.readFileSync(path.join(__dirname,"../js/core/integrations.js"),"utf8"),ctx);
  vm.runInContext("updateTouchPresentation()",ctx);
  assert.equal(ctx.document.body.classList.contains("touch-controls-active"),false);
  ctx.events.pointerdown({pointerType:"touch"});
  vm.runInContext("updateTouchPresentation()",ctx);
  assert.equal(ctx.document.body.classList.contains("touch-controls-active"),true);
  ctx.events.pointerdown({pointerType:"mouse"});
  vm.runInContext("updateTouchPresentation()",ctx);
  assert.equal(ctx.document.body.classList.contains("touch-controls-active"),false);
});

test("controller play mode prevents raw keyboard movement, shot modifiers and tactical requests",()=>{
  const ctx=context();ctx.possessionFrozen=false;
  vm.runInContext(fs.readFileSync(path.join(__dirname,"../js/input-gameplay.js"),"utf8"),ctx);
  vm.runInContext('controllerConfig.setInputMode("p1","controller")',ctx);
  let requests=0,uiToggles=0;
  ctx.getHumanPlayer=()=>({teamId:0});ctx.getBallHandler=()=>({teamId:0});
  ctx.aiRequestOffensiveAction=()=>{requests++;return true;};ctx.flash=()=>{};ctx.toggleUI=()=>{uiToggles++;};
  const press=(code,key,extra={})=>ctx.onKeyDown({code,key,repeat:false,preventDefault(){},...extra});
  for(const [code,key]of [["KeyW","w"],["KeyS","s"],["KeyQ","q"],["KeyE","e"],["KeyB","b"]]){
    press(code,key);assert.equal(!!ctx.keys[code],false,`${code} cannot reach legacy controls`);
  }
  assert.equal(requests,0);
  press("KeyI","i");assert.equal(ctx.keys.KeyI,true,"the other player's keyboard stays independent");
  press("Escape","Escape");ctx.manager.tick();assert.equal(ctx.manager.getState("p1").pause,true,"keyboard pause remains available for recovery");
  press("KeyH","h");assert.equal(uiToggles,1,"UI shortcuts remain available");
  ctx.onKeyUp({code:"KeyI",key:"i",preventDefault(){}});assert.equal(ctx.keys.KeyI,false);
  vm.runInContext('controllerConfig.setInputMode("p2","controller")',ctx);
  for(const code of ["Numpad4","Numpad6"]){press(code,code.endsWith("4")?"4":"6");assert.equal(!!ctx.keys[code],false,"legacy Player 2 skill aliases are blocked too");}
});

test("controller play mode hides touch controls on touch hardware and keyboard mode restores them",()=>{
  const ctx=context();loadPresentation(ctx);
  vm.runInContext(fs.readFileSync(path.join(__dirname,"../js/core/integrations.js"),"utf8"),ctx);
  vm.runInContext('controllerConfig.setInputMode("p1","controller"); updateTouchPresentation()',ctx);
  assert.equal(ctx.document.body.classList.contains("touch-controls-active"),false);
  vm.runInContext('controllerConfig.setInputMode("p1","keyboard"); updateTouchPresentation()',ctx);
  assert.equal(ctx.document.body.classList.contains("touch-controls-active"),true);
});

test("device assignment status names the chosen play mode even without a controller",()=>{
  const ctx=context();ctx.nodes.deviceAssignmentStatus=element("deviceAssignmentStatus");
  vm.runInContext(fs.readFileSync(path.join(__dirname,"../js/core/integrations.js"),"utf8"),ctx);
  vm.runInContext('controllerConfig.setInputMode("p1","controller"); updateInputDeviceUI()',ctx);
  assert.match(ctx.nodes.deviceAssignmentStatus.textContent,/Player 1: Controller · waiting for connection/);
  vm.runInContext('controllerConfig.setInputMode("p1","keyboard"); updateInputDeviceUI()',ctx);
  assert.match(ctx.nodes.deviceAssignmentStatus.textContent,/Player 1: Keyboard \/ touch/);
  vm.runInContext('controllerConfig.setInputMode("p1","auto"); updateInputDeviceUI()',ctx);
  assert.match(ctx.nodes.deviceAssignmentStatus.textContent,/Player 1: Automatic · Keyboard \/ touch/);
});

test("cooperative HUD identifies teams, keeps slot labels in their views, and skips unchanged score writes",()=>{
  const ctx=context();loadPresentation(ctx);ctx.twoPlayerMode=true;
  ctx.p2.teamId=0;ctx.matchPlayers.push(character("opponent",0,0,"ai"));
  let scoreWrites=0;
  Object.defineProperty(ctx.nodes.scoreText,"innerHTML",{set(){scoreWrites++;}});
  vm.runInContext("updateHUD(); updateHUD()",ctx);
  assert.equal(scoreWrites,1);
  assert.equal(ctx.nodes.p1HudName.textContent,"BLUE TEAM");
  assert.equal(ctx.nodes.p2HudName.textContent,"CORAL TEAM");
  assert.match(ctx.nodes.p2Label.textContent,/P2 · BLUE/);
  assert.equal(ctx.nodes.p2Label.style.left,"calc(50% + 12px)");
  ctx.splitLayout="horizontal";ctx.scoreP1=2;
  vm.runInContext("updateHUD()",ctx);
  assert.equal(scoreWrites,2);
  assert.equal(ctx.nodes.p1Label.style.bottom,"calc(50% + 12px)");
  assert.equal(ctx.nodes.p2Label.style.left,"12px");
});

test("shot feedback follows the releasing slot after switching and resets cleanly for rematches",()=>{
  const ctx=context();loadPresentation(ctx);ctx.twoPlayerMode=true;
  const shooter=character("teammate",0,0,null);shooter.teamId=0;shooter.lastShotSlot="p2";
  ctx.getPlayerById=()=>shooter;
  vm.runInContext('showPlayerShotFeedback("teammate","Green Release · Open · Made")',ctx);
  const feedback=ctx.nodes["shotFeedback-p2"];
  assert.equal(feedback.style.left,"75%");
  assert.match(feedback.textContent,/P2 · Green Release/);
  ctx.splitLayout="horizontal";vm.runInContext("updateHUD()",ctx);
  assert.equal(feedback.style.top,"calc(50% + 18px)");
  vm.runInContext("resetPlayerShotFeedback()",ctx);
  assert.equal(feedback.style.display,"none");
});

function loadMovement(ctx){
  loadPresentation(ctx);
  Object.assign(ctx,{
    possessionFrozen:false,countdownTimer:0,ballState:"dribble",SPRINT_MIN:.12,SPRINT_DRAIN:.62,SPRINT_RECOVER:.24,
    COURT_W:18,COURT_L:26,
    getGameplayTuning:()=>({lateralSpeed:1,sprintSpeed:1,reversePenalty:.48,acceleration:1,deceleration:1,staminaDrain:1}),
    getBallHandler:()=>ctx.p1,getNearestOpponent:()=>ctx.p2,setAnimationState(){},
    setPaused:()=>{ctx.gamePaused=true;}
  });
  vm.runInContext(fs.readFileSync(path.join(__dirname,"../js/input-gameplay.js"),"utf8"),ctx);
  for(const char of ctx.matchPlayers){
    Object.assign(char,{
      profile:{...char.profile,speed:1,sprint:1,width:1,handle:1},
      movement:{stopSpeed:1,recoverySpeed:1},moveMeta:{},anim:{},velocity:new Vector3(),
      moveState:"normal",stamina:1,sprint:1,stumbledTimer:0,reachRecovery:0,
      pumpFakeCooldown:0,sideHopCooldown:0,plantTimer:0,
      previousPosition:char.group.position.clone(),simulationPosition:char.group.position.clone(),
      previousAngle:0,simulationAngle:0
    });
  }
}

test("movement remains court-relative after a turn or possession change and accepts AI world direction",()=>{
  const ctx=context();loadMovement(ctx);
  for(const angle of [0,Math.PI/2,Math.PI]){
    ctx.p1.angle=angle;ctx.p1.velocity.set(0,0,0);ctx.p1.plantTimer=0;
    vm.runInContext('updateModernPlayerControl(p1,{moveX:0,moveY:1},1/60)',ctx);
    assert.equal(ctx.p1.velocity.x,0);assert.ok(ctx.p1.velocity.z<0);
  }
  ctx.possession="p2";ctx.getBallHandler=()=>ctx.p2;ctx.p1.velocity.set(0,0,0);
  vm.runInContext('updateModernPlayerControl(p1,{moveX:1,moveY:0},1/60)',ctx);
  assert.ok(ctx.p1.velocity.x>0);assert.equal(ctx.p1.velocity.z,0);
  ctx.p1.velocity.set(0,0,0);ctx.p1.plantTimer=0;
  vm.runInContext('updateModernPlayerControl(p1,{moveX:0,moveY:1,worldDirection:new THREE.Vector3(-1,0,0)},1/60)',ctx);
  assert.ok(ctx.p1.velocity.x<0);assert.equal(ctx.p1.velocity.z,0);
});

test("shot and loose-ball chases give both teams equal running speed, direction and sprint controls",()=>{
  for(const state of ["shot","loose"])for(const sprint of [false,true]){
    const ctx=context();loadMovement(ctx);ctx.ballState=state;
    ctx.p1.teamId=0;ctx.p2.teamId=1;ctx.getBallHandler=()=>ctx.p1;
    // The player with poor lateral defense should still run normally to a
    // rebound, using the same speed and acceleration as an identical opponent.
    for(const player of [ctx.p1,ctx.p2]){
      player.profile.ratings={lateralMovement:20,acceleration:50,stamina:50};
      player.group.position.set(player===ctx.p1?-4:4,0,0);player.angle=0;
    }
    const animations=new Map();ctx.setAnimationState=(player,name)=>animations.set(player.id,name);
    for(let frame=0;frame<45;frame++)for(const player of [ctx.p1,ctx.p2]) ctx.updateModernPlayerControl(player,{moveX:0,moveY:1,sprint},1/60);
    assert.ok(Math.abs(ctx.p1.velocity.length()-ctx.p2.velocity.length())<1e-12);
    assert.ok(ctx.p2.velocity.length()>(sprint?6:4));
    assert.ok(Math.abs(ctx.p1.angle-ctx.p2.angle)<1e-12,"both turn toward their run instead of facing a stale owner");
    assert.equal(animations.get(ctx.p2.id),sprint?"sprint":"run");
  }
});

test("live defenders still use lateral movement and face the actual handler",()=>{
  const ctx=context();loadMovement(ctx);ctx.p1.teamId=0;ctx.p2.teamId=1;
  ctx.p1.group.position.set(0,0,-4);ctx.p2.group.position.set(0,0,0);ctx.p2.angle=0;
  ctx.p2.profile.ratings={lateralMovement:20,acceleration:50};
  let animation;ctx.setAnimationState=(_,name)=>animation=name;
  ctx.updateModernPlayerControl(ctx.p2,{moveX:1,moveY:0},1/60);
  assert.equal(ctx.isDefendingLiveBall(ctx.p2),true);assert.equal(animation,"defense");
  assert.ok(Math.abs(ctx.p2.angle)>0,"the defender turns toward the handler while shuffling");
  ctx.ballState="shot";assert.equal(ctx.isDefendingLiveBall(ctx.p2),false);
});

test("human and AI movement motors leave active block flight to jump physics until landing",()=>{
  const ctx=context();loadMovement(ctx);ctx.activeBlockJumpOwnsMovement=player=>!player.onGround&&!!player.moveMeta.blockFlight;
  ctx.p1.onGround=false;ctx.p1.moveMeta.blockFlight={};ctx.p1.velocity.set(1,0,-2);
  const before=JSON.stringify({position:ctx.p1.group.position,velocity:ctx.p1.velocity});
  ctx.updateModernPlayerControl(ctx.p1,{moveX:1,moveY:1,sprint:true},1/60);
  ctx.updatePlayerControl(ctx.p1,{state:{source:"keyboard"}},1/60);
  assert.equal(JSON.stringify({position:ctx.p1.group.position,velocity:ctx.p1.velocity}),before);
  ctx.p1.onGround=true;ctx.updateModernPlayerControl(ctx.p1,{moveX:1,moveY:0},1/60);
  assert.notEqual(JSON.stringify({position:ctx.p1.group.position,velocity:ctx.p1.velocity}),before,"landing restores ordinary movement");
});

test("render frame rates consume equal fixed simulation time and restore authoritative transforms",()=>{
  const positions=[];
  for(const fps of [30,60,120]){
    const ctx=context();loadMovement(ctx);
    Object.assign(ctx,{
      simulationAccumulator:0,simulationTime:0,SIMULATION_STEP:1/60,
      ballPreviousPosition:ctx.ball.position.clone(),ballSimulationPosition:ctx.ball.position.clone(),
      clock:{getDelta:()=>1/fps},requestAnimationFrame(){},
      tutorialCoach:{update(){}},adaptiveQuality:{sample(){}},
      updateTouchPresentation(){},processUnifiedInputActions(){},updateDebugDirections(){},updateShotMeterVisuals(){},
      updateCameras(){},updateHUD(){},renderScene(){},
      simulateBasketball(dt){ ctx.simulationTime+=dt;ctx.p1.group.position.x+=dt; }
    });
    const start=ctx.p1.group.position.x;
    for(let frame=0;frame<fps;frame++) vm.runInContext("animate()",ctx);
    assert.ok(Math.abs(ctx.simulationTime-1)<1e-12);
    assert.ok(Math.abs(ctx.p1.group.position.x-ctx.p1.simulationPosition.x)<1e-12);
    positions.push(ctx.p1.group.position.x-start);
  }
  assert.ok(positions.every(position=>Math.abs(position-1)<1e-12));
});
