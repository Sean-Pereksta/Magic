const INPUT_ACTIONS = [
  "sprint","shoot","block","steal","leftSkill","rightSkill","takeover","pause","protect"
];

function createInputFrame(){
  const frame = {moveX:0,moveY:0,skillX:0,skillY:0,source:"keyboard",pressed:{},released:{}};
  for(const action of INPUT_ACTIONS) frame[action] = false;
  return frame;
}

class BasketballInputManager {
  constructor(){
    this.keys = Object.create(null);
    this.players = {p1:createInputFrame(),p2:createInputFrame()};
    this.touch = {
      moveX:0,moveY:0,skillX:0,skillY:0,
      buttons:Object.create(null),movePointer:null,skillPointer:null
    };
    this.pulses = {p1:Object.create(null),p2:Object.create(null)};
    this.flicks = {p1:null,p2:null};
    this.lastFlick = {time:0,direction:""};
    this.gamepadSkillLatch = new Map();
    this.lastDevice = "keyboard";
    this.touchReady = false;
    this.deadzone = 0.16;
    this._touchMoveGuard = event => {
      if(gameStarted && document.body.classList.contains("touch-controls-active")) event.preventDefault();
    };
    window.addEventListener("gamepadconnected",event=>this.handleGamepadConnection(event,true));
    window.addEventListener("gamepaddisconnected",event=>this.handleGamepadConnection(event,false));
  }

  handleGamepadConnection(event,connected){
    this.gamepadSkillLatch.delete(event.gamepad.index);
    window.dispatchEvent(new CustomEvent("basketball-gamepads-changed",{
      detail:{connected,index:event.gamepad.index,id:event.gamepad.id}
    }));
  }

  onKeyDown(event){
    this.keys[event.code] = true;
    this.keys[event.key?.toLowerCase()] = true;
    if(this.isGameplayKey(event.code)){
      this.markDevice("keyboard");
      if(gameStarted) event.preventDefault();
    }
  }

  onKeyUp(event){
    this.keys[event.code] = false;
    this.keys[event.key?.toLowerCase()] = false;
    if(this.isGameplayKey(event.code) && gameStarted) event.preventDefault();
  }

  isGameplayKey(code){
    return [
      "KeyW","KeyA","KeyS","KeyD","ShiftLeft","Space","KeyF","KeyQ","KeyE","KeyR","ControlLeft",
      "KeyI","KeyJ","KeyK","KeyL","ShiftRight","Enter","Numpad0","Numpad1","Numpad2","KeyU","KeyO","Escape"
    ].includes(code);
  }

  markDevice(device){
    if(this.lastDevice === device) return;
    this.lastDevice = device;
    window.dispatchEvent(new CustomEvent("basketball-input-device",{detail:{device}}));
  }

  getConnectedGamepads(){
    if(!navigator.getGamepads) return [];
    return Array.from(navigator.getGamepads()).filter(Boolean).sort((a,b)=>a.index-b.index);
  }

  getAssignedGamepad(playerId,pads){
    if(!pads.length) return null;
    if(!twoPlayerMode) return playerId === "p1" ? pads[0] : null;
    if(pads.length === 1) return playerId === "p2" ? pads[0] : null;
    return playerId === "p1" ? pads[0] : pads[1];
  }

  applyDeadzone(x,y){
    const magnitude = Math.hypot(x,y);
    if(magnitude <= this.deadzone) return {x:0,y:0,magnitude:0};
    const scaled = Math.min(1,(magnitude-this.deadzone)/(1-this.deadzone));
    return {x:(x/magnitude)*scaled,y:(y/magnitude)*scaled,magnitude:scaled};
  }

  readKeyboard(playerId){
    const p1Keys = playerId === "p1";
    const left = this.keys[p1Keys?"KeyA":"KeyJ"] ? 1 : 0;
    const right = this.keys[p1Keys?"KeyD":"KeyL"] ? 1 : 0;
    const forward = this.keys[p1Keys?"KeyW":"KeyI"] ? 1 : 0;
    const back = this.keys[p1Keys?"KeyS":"KeyK"] ? 1 : 0;
    const result = createInputFrame();
    result.moveX = right-left;
    result.moveY = forward-back;
    const magnitude = Math.hypot(result.moveX,result.moveY);
    if(magnitude > 1){ result.moveX /= magnitude; result.moveY /= magnitude; }
    result.sprint = !!this.keys[p1Keys?"ShiftLeft":"ShiftRight"];
    result.shoot = p1Keys ? !!this.keys.Space : (!!this.keys.Enter || !!this.keys.Numpad0);
    result.block = p1Keys ? !!this.keys.KeyF : !!this.keys.Numpad2;
    result.steal = p1Keys ? false : !!this.keys.Numpad1;
    result.leftSkill = p1Keys ? !!this.keys.KeyQ : !!this.keys.KeyU;
    result.rightSkill = p1Keys ? !!this.keys.KeyE : !!this.keys.KeyO;
    result.takeover = p1Keys ? !!this.keys.KeyR : !!this.keys.NumpadAdd;
    result.protect = p1Keys ? !!this.keys.ControlLeft : !!this.keys.ControlRight;
    result.pause = !!this.keys.Escape;
    result.source = "keyboard";
    return result;
  }

  readGamepad(gamepad,playerId){
    const result = createInputFrame();
    if(!gamepad) return result;
    const move = this.applyDeadzone(gamepad.axes[0] || 0,-(gamepad.axes[1] || 0));
    const skill = this.applyDeadzone(gamepad.axes[2] || 0,-(gamepad.axes[3] || 0));
    result.moveX = move.x;
    result.moveY = move.y;
    result.skillX = skill.x;
    result.skillY = skill.y;
    result.sprint = (gamepad.buttons[7]?.value || 0) > 0.25;
    result.shoot = !!gamepad.buttons[2]?.pressed;
    result.block = !!gamepad.buttons[3]?.pressed;
    result.steal = !!gamepad.buttons[1]?.pressed;
    result.protect = (gamepad.buttons[6]?.value || 0) > 0.25;
    result.leftSkill = !!gamepad.buttons[4]?.pressed;
    result.rightSkill = !!gamepad.buttons[5]?.pressed;
    result.takeover = !!gamepad.buttons[10]?.pressed && !!gamepad.buttons[11]?.pressed;
    result.pause = !!gamepad.buttons[9]?.pressed;
    result.source = gamepad.id.toLowerCase().includes("playstation") || gamepad.id.toLowerCase().includes("dual") ? "playstation" : "gamepad";

    const previousMagnitude = this.gamepadSkillLatch.get(gamepad.index) || 0;
    if(skill.magnitude > 0.68 && previousMagnitude < 0.35){
      this.queueFlick(playerId,skill.x,skill.y,"gamepad");
    }
    this.gamepadSkillLatch.set(gamepad.index,skill.magnitude);
    if(move.magnitude > 0.08 || skill.magnitude > 0.3 || result.sprint || result.shoot || result.block || result.steal){
      this.markDevice(result.source);
    }
    return result;
  }

  readTouch(playerId){
    const result = createInputFrame();
    if(playerId !== "p1") return result;
    result.moveX = this.touch.moveX;
    result.moveY = this.touch.moveY;
    result.skillX = this.touch.skillX;
    result.skillY = this.touch.skillY;
    for(const action of INPUT_ACTIONS) result[action] = !!this.touch.buttons[action];
    result.source = "touch";
    return result;
  }

  mergeFrames(frames){
    const result = createInputFrame();
    let strongest = frames[0] || result;
    for(const frame of frames){
      if(Math.hypot(frame.moveX,frame.moveY) > Math.hypot(strongest.moveX,strongest.moveY)) strongest = frame;
      for(const action of INPUT_ACTIONS) result[action] ||= !!frame[action];
      if(Math.hypot(frame.skillX,frame.skillY) > Math.hypot(result.skillX,result.skillY)){
        result.skillX = frame.skillX;
        result.skillY = frame.skillY;
      }
    }
    result.moveX = strongest.moveX;
    result.moveY = strongest.moveY;
    result.source = strongest.source;
    return result;
  }

  tick(){
    const pads = this.getConnectedGamepads();
    for(const playerId of ["p1","p2"]){
      const previous = this.players[playerId];
      const frames = [this.readKeyboard(playerId)];
      const assignedPad = this.getAssignedGamepad(playerId,pads);
      if(assignedPad) frames.push(this.readGamepad(assignedPad,playerId));
      if(playerId === "p1") frames.push(this.readTouch(playerId));
      const next = this.mergeFrames(frames);
      for(const action of INPUT_ACTIONS){
        if(this.pulses[playerId][action]) next[action] = true;
        next.pressed[action] = !!next[action] && !previous[action];
        next.released[action] = !next[action] && !!previous[action];
      }
      this.players[playerId] = next;
      this.pulses[playerId] = Object.create(null);
    }
  }

  getState(playerId){ return this.players[playerId] || this.players.p1; }

  pulse(playerId,action){
    if(INPUT_ACTIONS.includes(action)) this.pulses[playerId][action] = true;
  }

  queueFlick(playerId,x,y,source="touch"){
    const direction = Math.abs(x) > Math.abs(y) ? (x < 0 ? "left" : "right") : (y < 0 ? "back" : "forward");
    const now = performance.now() * 0.001;
    const isDouble = this.lastFlick.direction === direction && now-this.lastFlick.time < 0.28;
    this.lastFlick = {time:now,direction};
    this.flicks[playerId] = {x,y,direction,double:isDouble,source};
    this.markDevice(source === "touch" ? "touch" : "gamepad");
  }

  consumeFlick(playerId){
    const flick = this.flicks[playerId];
    this.flicks[playerId] = null;
    return flick;
  }

  attachTouchControls(){
    if(this.touchReady) return;
    const movePad = document.getElementById("moveStick");
    const skillPad = document.getElementById("skillStick");
    if(!movePad || !skillPad) return;
    this.touchReady = true;
    const bindStick = (element,type)=>{
      const knob = element.querySelector(".touch-stick-knob");
      const update = event=>{
        const pointerKey = type === "move" ? "movePointer" : "skillPointer";
        if(this.touch[pointerKey] !== event.pointerId) return;
        const rect = element.getBoundingClientRect();
        const radius = Math.max(1,Math.min(rect.width,rect.height)*0.42);
        const dx = event.clientX-(rect.left+rect.width/2);
        const dy = event.clientY-(rect.top+rect.height/2);
        const distance = Math.hypot(dx,dy);
        const scale = distance > radius ? radius/distance : 1;
        const nx = dx*scale/radius;
        const ny = -dy*scale/radius;
        knob.style.transform = `translate(${dx*scale}px,${dy*scale}px)`;
        if(type === "move"){
          this.touch.moveX = nx;
          this.touch.moveY = ny;
        }else{
          this.touch.skillX = nx;
          this.touch.skillY = ny;
        }
        event.preventDefault();
      };
      const finish = event=>{
        const pointerKey = type === "move" ? "movePointer" : "skillPointer";
        if(this.touch[pointerKey] !== event.pointerId) return;
        if(type === "skill" && Math.hypot(this.touch.skillX,this.touch.skillY) > 0.34){
          this.queueFlick("p1",this.touch.skillX,this.touch.skillY,"touch");
        }
        this.touch[pointerKey] = null;
        if(type === "move") this.touch.moveX = this.touch.moveY = 0;
        else this.touch.skillX = this.touch.skillY = 0;
        knob.style.transform = "translate(0,0)";
        event.preventDefault();
      };
      element.addEventListener("pointerdown",event=>{
        const pointerKey = type === "move" ? "movePointer" : "skillPointer";
        this.touch[pointerKey] = event.pointerId;
        element.setPointerCapture?.(event.pointerId);
        this.markDevice("touch");
        update(event);
      });
      element.addEventListener("pointermove",update);
      element.addEventListener("pointerup",finish);
      element.addEventListener("pointercancel",finish);
      element.addEventListener("lostpointercapture",finish);
    };
    bindStick(movePad,"move");
    bindStick(skillPad,"skill");

    document.querySelectorAll("[data-touch-action]").forEach(button=>{
      const activePointers = new Map();
      const release = event=>{
        const action = activePointers.get(event.pointerId);
        if(!action) return;
        activePointers.delete(event.pointerId);
        this.touch.buttons[action] = false;
        button.classList.remove("pressed");
        event.preventDefault();
      };
      button.addEventListener("pointerdown",event=>{
        const action = button.dataset.touchAction;
        activePointers.set(event.pointerId,action);
        this.touch.buttons[action] = true;
        button.classList.add("pressed");
        button.setPointerCapture?.(event.pointerId);
        this.markDevice("touch");
        event.preventDefault();
      });
      button.addEventListener("pointerup",release);
      button.addEventListener("pointercancel",release);
      button.addEventListener("lostpointercapture",release);
    });
    document.addEventListener("touchmove",this._touchMoveGuard,{passive:false});
  }

  vibrate(playerId,strength=0.35,duration=70){
    const pads = this.getConnectedGamepads();
    const pad = this.getAssignedGamepad(playerId,pads);
    const actuator = pad?.vibrationActuator || pad?.hapticActuators?.[0];
    if(!actuator) return;
    if(typeof actuator.playEffect === "function"){
      actuator.playEffect("dual-rumble",{
        startDelay:0,duration,
        weakMagnitude:Math.min(1,strength*.72),
        strongMagnitude:Math.min(1,strength)
      }).catch(()=>{});
    }else if(typeof actuator.pulse === "function"){
      actuator.pulse(Math.min(1,strength),duration).catch(()=>{});
    }
  }
}

const inputManager = new BasketballInputManager();
