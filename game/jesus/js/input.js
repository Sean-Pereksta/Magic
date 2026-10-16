const DEFAULT_KEYS=Object.freeze({left:"a",right:"d",jump:" ",dash:"shift",ability:"q"});
const ARROWS={left:"arrowleft",right:"arrowright",jump:"arrowup"};
export class InputController{
  constructor(root=document){
    this.root=root;this.held=new Set();this.pressed=new Set();this.touchHeld=new Map();this.axis=0;this.enabled=false;this.touchDown=false;this.joystickPointer=null;this.captureAction=null;
    try{this.bindings={...DEFAULT_KEYS,...JSON.parse(localStorage.getItem("jesusRunnerKeysV1")||"{}")};}catch{this.bindings={...DEFAULT_KEYS};}
    for(const action of Object.keys(DEFAULT_KEYS))if(typeof this.bindings[action]!=="string")this.bindings[action]=DEFAULT_KEYS[action];
    window.addEventListener("keydown",event=>{
      const key=event.key.toLowerCase();
      if(this.captureAction){event.preventDefault();const{action,done}=this.captureAction;this.captureAction=null;const arrowConflict=Object.values(ARROWS).includes(key)&&ARROWS[action]!==key;if(!["escape","tab","meta","control","alt","arrowdown"].includes(key)&&!arrowConflict){const previous=this.bindings[action];for(const a of Object.keys(this.bindings))if(a!==action&&this.bindings[a]===key)this.bindings[a]=previous;this.bindings[action]=key;try{localStorage.setItem("jesusRunnerKeysV1",JSON.stringify(this.bindings));}catch{}}this.reset();done?.();return;}
      if(event.target?.matches("input,textarea,select,[contenteditable=true]"))return;
      if(this.enabled&&[...Object.values(this.bindings),"arrowleft","arrowright","arrowup","arrowdown"," "].includes(key))event.preventDefault();
      if(!event.repeat)this.pressed.add(key);this.held.add(key);
    });
    window.addEventListener("keyup",e=>this.held.delete(e.key.toLowerCase()));window.addEventListener("blur",()=>this.reset());document.addEventListener("visibilitychange",()=>{if(document.hidden)this.reset();});
  }
  reset(){this.held.clear();this.pressed.clear();this.touchHeld.clear();this.axis=0;this.touchDown=false;this.joystickPointer=null;if(this.stick)this.stick.style.transform="translate(0,0)";}
  rebind(action,done){this.reset();this.captureAction={action,done};}
  cancelRebind(){this.captureAction=null;}
  consume(key){if(!this.pressed.has(key))return false;this.pressed.delete(key);return true;}
  consumeAny(keys){let found=false;for(const key of new Set(keys))if(this.consume(key))found=true;return found;}
  aliases(action,keys){return keys.filter(key=>!Object.entries(this.bindings).some(([other,binding])=>other!==action&&binding===key));}
  actionHeld(action){return!!this.touchHeld.get(action)?.size||this.held.has(this.bindings[action])||!!(ARROWS[action]&&this.held.has(ARROWS[action]));}
  horizontal(){return this.enabled?(Math.abs(this.axis)>.08?this.axis:Number(this.actionHeld("right"))-Number(this.actionHeld("left"))):0;}
  jumpPressed(){return this.enabled&&this.consumeAny(["jump",this.bindings.jump,"arrowup",...this.aliases("jump",["w"])]);}
  jumpHeld(){return this.enabled&&(this.actionHeld("jump")||this.aliases("jump",["w"]).some(key=>this.held.has(key)));}
  dashPressed(){return this.enabled&&this.consumeAny(["dash",this.bindings.dash,...this.aliases("dash",["x"])]);}
  abilityPressed(){return this.enabled&&this.consumeAny(["ability",this.bindings.ability,...this.aliases("ability",["q","e"])]);}
  downHeld(){return this.enabled&&(this.aliases("down",["s"]).some(key=>this.held.has(key))||this.held.has("arrowdown")||this.touchDown);}
  clearFrame(){this.pressed.clear();}
  bindMobile(joystick,stick,jumpButton,dashButton,abilityButton){
    this.stick=stick;const move=event=>{const r=joystick.getBoundingClientRect(),radius=r.width*.34,dx=event.clientX-r.left-r.width/2,dy=event.clientY-r.top-r.height/2,length=Math.hypot(dx,dy)||1,factor=Math.min(radius,length)/length,x=dx*factor,y=dy*factor;stick.style.transform=`translate(${x}px,${y}px)`;this.axis=Math.abs(x/radius)<.14?0:x/radius;this.touchDown=y>radius*.6;};
    joystick.addEventListener("pointerdown",e=>{e.preventDefault();if(!this.enabled||this.joystickPointer!==null)return;this.joystickPointer=e.pointerId;joystick.setPointerCapture(e.pointerId);move(e);});joystick.addEventListener("pointermove",e=>{if(e.pointerId===this.joystickPointer)move(e);});
    const release=e=>{if(e.pointerId!==this.joystickPointer)return;this.joystickPointer=null;this.axis=0;this.touchDown=false;stick.style.transform="translate(0,0)";};for(const event of["pointerup","pointercancel","lostpointercapture"])joystick.addEventListener(event,release);
    this.bindTouchButton(jumpButton,"jump");this.bindTouchButton(dashButton,"dash");this.bindTouchButton(abilityButton,"ability");
  }
  bindTouchButton(button,action){button.addEventListener("pointerdown",e=>{e.preventDefault();if(!this.enabled)return;const pointers=this.touchHeld.get(action)||new Set();pointers.add(e.pointerId);this.touchHeld.set(action,pointers);this.pressed.add(action);button.setPointerCapture(e.pointerId);});const release=e=>{this.touchHeld.get(action)?.delete(e.pointerId);};for(const event of["pointerup","pointercancel","lostpointercapture"])button.addEventListener(event,release);}
}
