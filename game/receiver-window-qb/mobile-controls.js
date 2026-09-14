(()=>{
'use strict';

// Mobile-only split controls for Receiver Window QB.
// Left half: look. Right half: hold to charge, drag vertically for loft/bullet, release to throw.
if(!('PointerEvent' in window))return;

const isTouchLike=()=>matchMedia('(pointer: coarse)').matches||navigator.maxTouchPoints>0;
if(!isTouchLike())return;

let capturedCamera=null;
if(window.THREE?.PerspectiveCamera&&!window.THREE.__receiverWindowMobileCameraHook){
  const BaseCamera=window.THREE.PerspectiveCamera;
  class ReceiverWindowPerspectiveCamera extends BaseCamera{
    constructor(...args){
      super(...args);
      capturedCamera=this;
      window.__receiverWindowQBCamera=this;
    }
  }
  Object.setPrototypeOf(ReceiverWindowPerspectiveCamera,BaseCamera);
  window.THREE.PerspectiveCamera=ReceiverWindowPerspectiveCamera;
  window.THREE.__receiverWindowMobileCameraHook=true;
}

const canvas=()=>document.getElementById('game');
const phase=()=>document.body?.dataset?.phase||'';
const clamp=(v,min,max)=>Math.max(min,Math.min(max,v));
const keyState={Space:false,KeyW:false,KeyS:false};

function emitKey(code,down){
  if(keyState[code]===down)return;
  keyState[code]=down;
  const key=code==='Space'?' ':code==='KeyW'?'w':'s';
  window.dispatchEvent(new KeyboardEvent(down?'keydown':'keyup',{
    code,key,bubbles:true,cancelable:true,repeat:false
  }));
}

function clearLoftKeys(){emitKey('KeyW',false);emitKey('KeyS',false)}

let lookPointer=null;
let lookLastX=0;
let lookLastY=0;
let lookYaw=0;
let lookPitch=-.08;
let throwPointer=null;
let throwStartY=0;
let throwArmed=false;
let throwArmTimer=0;
let loftMode='neutral';

function currentCamera(){return window.__receiverWindowQBCamera||capturedCamera}
function syncLookFromCamera(){
  const camera=currentCamera();
  if(!camera)return;
  lookYaw=Number.isFinite(camera.rotation.y)?camera.rotation.y:0;
  lookPitch=Number.isFinite(camera.rotation.x)?camera.rotation.x:-.08;
}
function applyLook(dx,dy){
  const camera=currentCamera();
  if(!camera)return;
  lookYaw=clamp(lookYaw-dx*.0045,-1.14,1.14);
  lookPitch=clamp(lookPitch-dy*.0041,-.72,.46);
  camera.rotation.order='YXZ';
  camera.rotation.y=lookYaw;
  camera.rotation.x=lookPitch;
  camera.rotation.z=0;
}
function setLoftMode(mode){
  if(mode===loftMode)return;
  loftMode=mode;
  if(mode==='loft'){
    emitKey('KeyS',false);
    emitKey('KeyW',true);
  }else if(mode==='bullet'){
    emitKey('KeyW',false);
    emitKey('KeyS',true);
  }else clearLoftKeys();
}
function tryArmThrow(){
  if(throwPointer==null||throwArmed||phase()!=='live')return;
  throwArmed=true;
  emitKey('Space',true);
}
function beginThrowPointer(e){
  throwPointer=e.pointerId;
  throwStartY=e.clientY;
  throwArmed=false;
  setLoftMode('neutral');
  tryArmThrow();
  clearInterval(throwArmTimer);
  throwArmTimer=setInterval(tryArmThrow,40);
}
function releaseThrow(){
  clearInterval(throwArmTimer);
  throwArmTimer=0;
  setLoftMode('neutral');
  if(throwArmed)emitKey('Space',false);
  else keyState.Space=false;
  throwArmed=false;
  throwPointer=null;
}
function claim(e){
  e.preventDefault();
  e.stopImmediatePropagation();
}

// Capture before the game's legacy single-touch handler so the two halves never compete.
document.addEventListener('pointerdown',e=>{
  if(e.pointerType!=='touch'||e.target!==canvas())return;
  // Keep the existing pre-snap receiver-tap / audible interaction unchanged.
  if(!phase()||phase()==='call')return;
  claim(e);
  if(e.clientX<innerWidth*.5){
    if(lookPointer!=null)return;
    lookPointer=e.pointerId;
    lookLastX=e.clientX;
    lookLastY=e.clientY;
    syncLookFromCamera();
  }else{
    if(throwPointer!=null)return;
    beginThrowPointer(e);
  }
},{capture:true,passive:false});

document.addEventListener('pointermove',e=>{
  if(e.pointerType!=='touch')return;
  if(e.pointerId===lookPointer){
    claim(e);
    const dx=e.clientX-lookLastX,dy=e.clientY-lookLastY;
    lookLastX=e.clientX;
    lookLastY=e.clientY;
    applyLook(dx,dy);
    return;
  }
  if(e.pointerId===throwPointer){
    claim(e);
    tryArmThrow();
    const dy=e.clientY-throwStartY;
    // Up = more loft. Down = lower/faster bullet. A small center dead-zone prevents accidental changes.
    if(dy<-14)setLoftMode('loft');
    else if(dy>14)setLoftMode('bullet');
    else setLoftMode('neutral');
  }
},{capture:true,passive:false});

function finishPointer(e){
  if(e.pointerType!=='touch')return;
  if(e.pointerId===lookPointer){
    claim(e);
    lookPointer=null;
    return;
  }
  if(e.pointerId===throwPointer){
    claim(e);
    releaseThrow();
  }
}
document.addEventListener('pointerup',finishPointer,{capture:true,passive:false});
document.addEventListener('pointercancel',finishPointer,{capture:true,passive:false});

addEventListener('blur',()=>{
  lookPointer=null;
  if(throwPointer!=null)releaseThrow();
  clearLoftKeys();
});
})();
