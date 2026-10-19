const test=require("node:test");
const assert=require("node:assert/strict");
const fs=require("node:fs");
const path=require("node:path");
const vm=require("node:vm");

function harness(saved=null,storageThrows=false){
  const listeners=new Map(),storage=new Map(),pads=[];
  if(saved!==null) storage.set("basketball2.controllerSettings.v1",saved);
  const ctx=vm.createContext({
    window:{addEventListener(name,fn){if(!listeners.has(name))listeners.set(name,[]);listeners.get(name).push(fn);},dispatchEvent(event){for(const fn of listeners.get(event.type)||[])fn(event);}},
    document:{addEventListener(){},hidden:false,body:{classList:{contains:()=>false}},querySelectorAll:()=>[]},
    navigator:{getGamepads:()=>pads},performance:{now:()=>1000},
    CustomEvent:class{constructor(type,options={}){this.type=type;this.detail=options.detail;}},
    localStorage:{getItem(key){if(storageThrows)throw Error("blocked");return storage.get(key)||null;},setItem(key,value){if(storageThrows)throw Error("blocked");storage.set(key,value);}},
    gameStarted:false,gameOver:false,gamePaused:false,twoPlayerMode:false,teamSize:3,
    keys:{},shotCharging:false,chargingPlayer:null,shotPower:0,
    setPaused(value){ctx.gamePaused=value;ctx.manager?.clearAll();}
  });
  for(const file of ["controller-config.js","input-manager.js"])vm.runInContext(fs.readFileSync(path.join(__dirname,"../js/core",file),"utf8"),ctx);
  vm.runInContext("globalThis.manager=inputManager;globalThis.config=controllerConfig;",ctx);
  ctx.pads=pads;ctx.storage=storage;return ctx;
}
function pad(index=0,id="Xbox Wireless Controller"){
  return {index,id,connected:true,mapping:"standard",axes:[0,0,0,0,0,0,0,0],buttons:Array.from({length:20},()=>({pressed:false,value:0}))};
}
function button(gamepad,index,down){gamepad.buttons[index].pressed=down;gamepad.buttons[index].value=down?1:0;}
function key(manager,code,down){manager[down?"onKeyDown":"onKeyUp"]({code,key:code==="Escape"?"Escape":code,preventDefault(){}});}

test("controller settings validate saves, survive blocked storage, and persist each player's maps",()=>{
  for(const saved of ["{broken",JSON.stringify({version:1,players:{p1:{assignment:{index:-1,id:"Xbox"},deadzone:"bad",bindings:{shoot:{type:"button",index:999}}}}})]){
    const ctx=harness(saved);assert.equal(ctx.config.getPlayer("p1").bindings.shoot.index,2);assert.equal(ctx.config.getPlayer("p1").assignment,"auto");
  }
  const blocked=harness(null,true);assert.equal(blocked.config.setDeadzone("p1",.2),true);
  const ctx=harness();ctx.config.setBinding("p1","shoot",{type:"button",index:0});
  ctx.config.setAxes("p2","move",{x:4,y:5,invertX:true,invertY:false});ctx.config.setDeadzone("p2",.24);
  ctx.config.setAssignment("p2",{index:3,id:"Xbox"});
  const restored=harness(ctx.storage.get("basketball2.controllerSettings.v1"));
  assert.equal(restored.config.getPlayer("p1").bindings.shoot.index,0);assert.equal(restored.config.getPlayer("p1").bindings.pass.index,2);
  assert.equal(restored.config.getPlayer("p2").axes.move.x,4);assert.equal(restored.config.getPlayer("p2").deadzone,.24);
  assert.equal(restored.config.getPlayer("p2").assignment.index,3);
  const copy=restored.config.getPlayer("p1");copy.bindings.shoot.index=18;assert.equal(restored.config.getPlayer("p1").bindings.shoot.index,0);
  assert.equal(ctx.config.setAxes("p1","move",{x:0,y:0}),false);assert.equal(ctx.config.setBinding("p1","pause",{type:"chord",buttons:[1,1]}),false);
  assert.equal(ctx.config.getPlayer("__proto__"),null);assert.equal(ctx.config.setAssignment("toString","auto"),false);assert.equal(ctx.config.resetPlayer("__proto__"),false);
  ctx.config.setAxes("p1","skill",{x:-1,y:-1});assert.equal(ctx.config.getPlayer("p1").axes.skill.x,-1);
  ctx.config.setAxes("p1","skill",{x:6});assert.equal(ctx.config.getPlayer("p1").axes.skill.y,-1);
  const disabled=harness(ctx.storage.get("basketball2.controllerSettings.v1"));assert.equal(disabled.config.getPlayer("p1").axes.skill.y,-1);
  ctx.config.setAxes("p1","move",{x:4,y:5});assert.equal(ctx.config.getPlayer("p1").axes.move.invertY,true);
  assert.equal(ctx.config.setAxes("p1","move",{x:5}),false);
});

test("Xbox defaults preserve every action and contextual RB without changing keyboard or touch",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();
  const defaults={sprint:7,shoot:2,pass:0,switchPlayer:5,block:3,steal:1,leftSkill:4,pause:9,protect:6};
  for(const [action,index]of Object.entries(defaults)){
    button(a,index,true);const state=m.readGamepad(a,"p1");assert.equal(state[action],true,action);button(a,index,false);
  }
  button(a,5,true);assert.equal(m.readGamepad(a,"p1").rightSkill,false);ctx.teamSize=1;
  assert.equal(m.readGamepad(a,"p1").rightSkill,true);assert.equal(m.readGamepad(a,"p1").switchPlayer,false);button(a,5,false);
  button(a,10,true);assert.equal(m.readGamepad(a,"p1").takeover,false);button(a,11,true);assert.equal(m.readGamepad(a,"p1").takeover,true);
  key(m,"Space",true);m.touch.buttons.pass=true;m.tick();assert.equal(m.getState("p1").shoot,true);assert.equal(m.getState("p1").pass,true);
});

test("input mode is validated, additive to old saves, and persists independently for both players",()=>{
  const legacy=harness(JSON.stringify({version:1,players:{p1:{assignment:"none"},p2:{inputMode:"invalid"}}}));
  assert.equal(legacy.config.getPlayer("p1").inputMode,"auto");assert.equal(legacy.config.getPlayer("p1").assignment,"none");
  assert.equal(legacy.config.getPlayer("p2").inputMode,"auto");
  assert.equal(legacy.config.setInputMode("__proto__","controller"),false);assert.equal(legacy.config.setInputMode("p1","bogus"),false);
  legacy.config.setInputMode("p1","controller");legacy.config.setInputMode("p2","keyboard");
  const restored=harness(legacy.storage.get("basketball2.controllerSettings.v1"));
  assert.equal(restored.config.getPlayer("p1").inputMode,"controller");assert.equal(restored.config.getPlayer("p2").inputMode,"keyboard");
  restored.config.resetPlayer("p1");assert.equal(restored.config.getPlayer("p1").inputMode,"auto");assert.equal(restored.config.getPlayer("p2").inputMode,"keyboard");
});

test("Controller mode accepts the mapped pad and excludes keyboard, touch and disabled-source skills",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();ctx.pads.push(a);ctx.config.setInputMode("p1","controller");m.tick();
  assert.equal(m.onKeyDown({code:"KeyW",key:"w"}),false);key(m,"Space",true);
  m.touch.moveY=1;m.touch.buttons.pass=true;m.queueFlick("p1",1,0,"touch");m.pulse("p1","steal");
  a.axes[0]=.8;button(a,2,true);m.tick();const state=m.getState("p1");
  assert.ok(state.moveX>.7);assert.equal(Math.abs(state.moveY),0);assert.equal(state.shoot,true);assert.equal(state.pass,false);assert.equal(state.steal,false);
  assert.equal(m.consumeFlick("p1"),null);assert.equal(m.readTouch("p1").moveY,0);assert.equal(m.readKeyboard("p1").shoot,false);
  key(m,"Escape",true);m.tick();assert.equal(m.getState("p1").pressed.pause,true);
  key(m,"KeyI",true);m.tick();assert.equal(m.getState("p2").moveY,1,'the other keyboard player remains independent');
});

test("Keyboard mode leaves a connected controller available while excluding pad gameplay",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();ctx.pads.push(a);ctx.config.setInputMode("p1","keyboard");
  a.axes[0]=1;a.axes[2]=1;button(a,2,true);key(m,"KeyW",true);m.tick();
  assert.equal(m.getAssignments().p1,null);assert.equal(m.getState("p1").moveX,0);assert.equal(m.getState("p1").moveY,1);assert.equal(m.getState("p1").shoot,false);
  m.queueFlick("p1",1,0,"gamepad");assert.equal(m.consumeFlick("p1"),null);
  m.setConfigurationMode(true);assert.equal(m.getAssignments().p1,a,'settings can still configure and test the connected pad');
  m.setConfigurationMode(false);m.touch.moveX=.6;m.pulse("p1","pass");m.tick();assert.equal(m.getState("p1").moveX,.6);assert.equal(m.getState("p1").pass,true);
});

test("explicit Controller mode takes the one local pad ahead of Automatic and Keyboard slots",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();ctx.twoPlayerMode=true;ctx.pads.push(a);
  assert.equal(m.getAssignments().p2,a,'legacy automatic local assignment remains unchanged');
  ctx.config.setInputMode("p1","controller");assert.equal(m.getAssignments().p1,a);assert.equal(m.getAssignments().p2,null);
  ctx.config.setInputMode("p1","keyboard");ctx.config.setInputMode("p2","controller");assert.equal(m.getAssignments().p1,null);assert.equal(m.getAssignments().p2,a);
});

test("switching input mode cancels a charge without a release and requires fresh input",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();ctx.pads.push(a);m.tick();ctx.gameStarted=true;
  key(m,"Space",true);button(a,2,true);m.tick();ctx.shotCharging=true;ctx.chargingPlayer={id:"p1"};ctx.shotPower=.7;
  ctx.config.setInputMode("p1","controller");m.tick();
  assert.equal(ctx.shotCharging,false);assert.equal(ctx.chargingPlayer,null);assert.equal(ctx.shotPower,0);assert.equal(ctx.gamePaused,true);
  assert.equal(m.getState("p1").shoot,false);assert.equal(m.getState("p1").released.shoot,false);
  button(a,2,false);m.tick();button(a,2,true);m.tick();assert.equal(m.getState("p1").pressed.shoot,true);
  ctx.config.setInputMode("p1","keyboard");key(m,"Space",true);m.tick();assert.equal(m.getState("p1").shoot,false,'held disabled keys cannot reappear when reenabled');
  key(m,"Space",false);key(m,"Space",true);m.tick();assert.equal(m.getState("p1").pressed.shoot,true);
});

test("Controller mode waits for hotplug without falling back and preserves disconnected charge safety",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();ctx.config.setInputMode("p1","controller");key(m,"KeyW",true);m.tick();
  assert.equal(m.getState("p1").moveY,0);assert.equal(m.getAssignments().p1,null);
  button(a,2,true);ctx.pads.push(a);m.tick();assert.equal(m.getState("p1").shoot,false);
  button(a,2,false);m.tick();button(a,2,true);m.tick();assert.equal(m.getState("p1").pressed.shoot,true);
  ctx.gameStarted=true;ctx.shotCharging=true;ctx.chargingPlayer={id:"p1"};ctx.pads.length=0;m.tick();
  assert.equal(ctx.gamePaused,true);assert.equal(ctx.shotCharging,false);assert.equal(m.getState("p1").released.shoot,false);
  assert.equal(ctx.config.getPlayer("p1").inputMode,"controller");assert.equal(m.getState("p1").moveY,0);
});

test("rebinding contextual actions swaps controls without hiding other actions in 1v1",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();
  ctx.config.setBinding("p1","switchPlayer",{type:"button",index:0});ctx.teamSize=1;
  button(a,5,true);assert.equal(m.readGamepad(a,"p1").pass,true);assert.equal(m.readGamepad(a,"p1").rightSkill,false);
  // An overlapping unqualified RB mapping has no distinct old control to swap.
  ctx.config.resetPlayer("p1");ctx.config.setBinding("p1","switchPlayer",{type:"button",index:5});
  assert.equal(ctx.config.getPlayer("p1").bindings.rightSkill,null);
});

test("recorded shooting combos cannot also fire Pass or Steal through their component buttons",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();
  ctx.config.setBinding("p1","shoot",{type:"chord",buttons:[0,1]});
  button(a,0,true);assert.equal(m.readGamepad(a,"p1").pass,false);assert.equal(m.readGamepad(a,"p1").shoot,false);
  button(a,1,true);const state=m.readGamepad(a,"p1");assert.equal(state.shoot,true);assert.equal(state.pass,false);assert.equal(state.steal,false);
  assert.equal(ctx.config.getPlayer("p1").bindings.pass.index,2);assert.equal(ctx.config.getPlayer("p1").bindings.steal,null);
});

test("two identical Xbox controllers keep separate assignments across local modes and hotplug",()=>{
  const ctx=harness(),m=ctx.manager,a=pad(0),b=pad(1);ctx.pads.push(a);
  assert.equal(m.getAssignments().p1,a);ctx.twoPlayerMode=true;
  assert.equal(m.getAssignments().p2,a);assert.equal(m.getAssignments().p1,null);
  ctx.pads.push(b);assert.equal(m.getAssignments().p1,b);assert.equal(m.getAssignments().p2,a);
  ctx.pads.splice(1,1);assert.equal(m.getAssignments().p1,null);assert.equal(m.getAssignments().p2,a);
  ctx.pads.push(b);ctx.config.setAssignment("p1",{index:0,id:a.id});
  assert.equal(m.getAssignments().p1,a);assert.equal(m.getAssignments().p2,b);
  ctx.config.setAssignment("p2",{index:0,id:a.id});assert.equal(ctx.config.getPlayer("p1").assignment,"none");
  assert.equal(m.getAssignments().p1,null);assert.equal(m.getAssignments().p2,a);
  ctx.config.setAssignment("p2",{index:0,id:"Different pad"});assert.equal(m.getAssignments().p2,null);
  ctx.twoPlayerMode=false;assert.equal(m.getAssignments().p2,null);
});

test("settings can configure and test Player 2 before local play without enabling a second solo player",()=>{
  const ctx=harness(),m=ctx.manager,a=pad(0),b=pad(1);ctx.pads.push(a,b);
  ctx.config.setAssignment("p2",{index:1,id:b.id});assert.equal(m.getAssignments().p2,null);
  m.setConfigurationMode(true);assert.equal(m.getAssignments().p1,a);assert.equal(m.getAssignments().p2,b);
  m.setConfigurationMode(false);m.tick();assert.equal(m.getAssignments().p2,null);
  button(b,0,true);m.tick();assert.equal(m.getState("p2").pass,false);
  ctx.twoPlayerMode=true;assert.equal(m.getAssignments().p2,b);
});

test("first input after starting a new local mode survives delayed assignment polling",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();ctx.pads.push(a);m.tick();
  ctx.twoPlayerMode=true;m.clearAll();key(m,"KeyD",true);key(m,"Space",true);m.touch.moveY=.5;
  m.tick();assert.equal(m.getState("p1").moveX,1);assert.equal(m.getState("p1").pressed.shoot,true);assert.equal(m.readTouch("p1").moveY,.5);
  ctx.shotCharging=true;ctx.chargingPlayer={id:"p1"};ctx.twoPlayerMode=false;m.tick();
  assert.equal(ctx.shotCharging,false);assert.equal(m.getState("p1").pressed.shoot,false);assert.equal(m.getState("p1").released.shoot,false);
  key(m,"Space",false);m.tick();assert.equal(ctx.shotCharging,false);
});

test("opening and closing settings cannot move a surviving local controller to the disconnected slot",()=>{
  const ctx=harness(),m=ctx.manager,a=pad(0),b=pad(1);ctx.twoPlayerMode=true;ctx.pads.push(a,b);m.tick();
  assert.equal(m.getAssignments().p1,a);ctx.pads.splice(1,1);m.tick();assert.equal(m.getAssignments().p1,a);assert.equal(m.getAssignments().p2,null);
  m.setConfigurationMode(true);assert.equal(m.getAssignments().p1,a);assert.equal(m.getAssignments().p2,null);
  m.setConfigurationMode(false);assert.equal(m.getAssignments().p1,a);assert.equal(m.getAssignments().p2,null);
});

test("remapped buttons, analog triggers, axes, inversion and chords produce independent edges",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();ctx.pads.push(a);
  ctx.config.setBinding("p1","shoot",{type:"button",index:0});
  ctx.config.setBinding("p1","protect",{type:"axis",index:6,direction:1,threshold:.6});
  ctx.config.setBinding("p1","takeover",{type:"chord",inputs:[{type:"button",index:8},{type:"axis",index:7,direction:-1,threshold:.7}]});
  ctx.config.setAxes("p1","move",{x:4,y:5,invertX:true,invertY:false});ctx.config.setDeadzone("p1",.2);
  m.tick();button(a,0,true);a.axes[4]=.6;a.axes[5]=-.4;a.axes[6]=.8;button(a,8,true);a.axes[7]=-.9;m.tick();
  let state=m.getState("p1");assert.equal(state.pressed.shoot,true);assert.equal(state.pass,false);assert.equal(state.protect,true);assert.equal(state.takeover,true);
  assert.ok(state.moveX<0&&state.moveY<0);m.tick();assert.equal(m.getState("p1").pressed.shoot,false);
  button(a,0,false);m.tick();assert.equal(m.getState("p1").released.shoot,true);
  button(a,2,true);m.tick();assert.equal(m.getState("p1").pressed.pass,true);
  a.axes[4]=.05;a.axes[5]=.05;assert.equal(m.readGamepad(a,"p1").moveX,0);
  ctx.config.setBinding("p1","sprint",{type:"button",index:7,threshold:.45});a.buttons[7].value=.65;assert.equal(m.readGamepad(a,"p1").sprint,true);
  ctx.config.setBinding("p1","sprint",{type:"button",index:7,threshold:.8});a.buttons[7].pressed=true;assert.equal(m.readGamepad(a,"p1").sprint,false);
});

test("skill stick remapping keeps directional flicks on the assigned player",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();ctx.twoPlayerMode=true;ctx.pads.push(a);
  ctx.config.setAxes("p2","skill",{x:4,y:5,invertX:true,invertY:true});m.tick();
  a.axes[4]=1;m.tick();assert.equal(m.consumeFlick("p2").direction,"left");assert.equal(m.consumeFlick("p1"),null);
  m.tick();assert.equal(m.consumeFlick("p2"),null);a.axes[4]=0;m.tick();a.axes[5]=-1;m.tick();assert.equal(m.consumeFlick("p2").direction,"forward");
});

test("one player's skill flick never counts as the other player's double flick",()=>{
  const ctx=harness(),m=ctx.manager;
  m.queueFlick("p1",1,0,"gamepad");assert.equal(m.consumeFlick("p1").double,false);
  m.queueFlick("p2",1,0,"gamepad");assert.equal(m.consumeFlick("p2").double,false);
  m.queueFlick("p2",1,0,"gamepad");assert.equal(m.consumeFlick("p2").double,true);
});

test("disconnect cancels a charged shot and pauses without release, and surviving pads never move slots",()=>{
  const ctx=harness(),m=ctx.manager,a=pad(0),b=pad(1);ctx.twoPlayerMode=true;ctx.pads.push(a,b);m.tick();
  ctx.gameStarted=true;button(a,2,true);m.tick();assert.equal(m.getState("p1").shoot,true);
  ctx.shotCharging=true;ctx.chargingPlayer={id:"p1"};ctx.shotPower=.7;ctx.pads.splice(0,1);m.tick();
  assert.equal(ctx.gamePaused,true);assert.equal(ctx.shotCharging,false);assert.equal(ctx.chargingPlayer,null);assert.equal(ctx.shotPower,0);
  assert.equal(m.getState("p1").released.shoot,false);assert.equal(m.getAssignments().p1,null);assert.equal(m.getAssignments().p2,b);
  ctx.pads.unshift(a);m.tick();assert.equal(m.getState("p1").shoot,false);button(a,2,false);m.tick();button(a,2,true);m.tick();assert.equal(m.getState("p1").pressed.shoot,true);
});

test("capture and remapping cancel held shots and require pad and keyboard release before returning",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();ctx.pads.push(a);m.tick();ctx.gameStarted=true;
  button(a,2,true);key(m,"Space",true);m.tick();ctx.shotCharging=true;ctx.chargingPlayer={id:"p1"};
  m.setConfigurationMode(true);assert.equal(ctx.shotCharging,false);assert.equal(ctx.gamePaused,true);
  key(m,"Escape",true);m.tick();assert.equal(m.getState("p1").pause,false);
  ctx.config.setBinding("p1","shoot",{type:"button",index:0});button(a,0,true);m.setConfigurationMode(false);
  m.tick();assert.equal(m.getState("p1").shoot,false);assert.equal(m.getState("p1").released.shoot,false);assert.equal(m.getState("p1").pause,false);
  key(m,"Space",true);key(m,"Escape",true);m.tick();assert.equal(m.getState("p1").shoot,false);assert.equal(m.getState("p1").pause,false);
  button(a,0,false);button(a,2,false);key(m,"Space",false);key(m,"Escape",false);m.tick();
  button(a,0,true);m.tick();assert.equal(m.getState("p1").pressed.shoot,true);
  ctx.config.setAssignment("p1","none");assert.equal(ctx.shotCharging,false);m.tick();assert.equal(m.getState("p1").released.shoot,false);
});

test("an action shared between keyboard and controller releases only after both are released",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();ctx.pads.push(a);m.tick();button(a,2,true);key(m,"Space",true);m.tick();
  key(m,"Space",false);m.tick();assert.equal(m.getState("p1").shoot,true);assert.equal(m.getState("p1").released.shoot,false);
  button(a,2,false);m.tick();assert.equal(m.getState("p1").released.shoot,true);
});

test("restarting or resuming cannot replay held inputs or a held trigger until neutral",()=>{
  const ctx=harness(),m=ctx.manager,a=pad();ctx.pads.push(a);m.tick();button(a,7,true);button(a,2,true);m.tick();m.clearAll();
  m.tick();assert.equal(m.getState("p1").sprint,false);assert.equal(m.getState("p1").shoot,false);
  button(a,2,false);m.tick();assert.equal(m.getState("p1").released.shoot,false);
  button(a,7,false);m.tick();button(a,2,true);m.tick();assert.equal(m.getState("p1").pressed.shoot,true);
});
