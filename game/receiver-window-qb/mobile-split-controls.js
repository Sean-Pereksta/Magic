(()=>{
'use strict';

const canvas=document.getElementById('game');
if(!canvas||!('PointerEvent' in window))return;

const isTouchDevice=()=>matchMedia('(pointer:coarse)').matches||navigator.maxTouchPoints>0;
if(!isTouchDevice())return;

let look=null;
let throwTouch=null;
let loftKey=null;
let loftReleaseTimer=0;
let lastLookReset=0;

const phase=()=>document.body.dataset.phase||'';
const live=()=>phase()==='live';
const blocked=()=>{
  const start=document.getElementById('startLayer');
  const saves=document.getElementById('saveLayer');
  const manager=document.getElementById('managerLayer');
  return (start&&getComputedStyle(start).display!=='none')||
    (saves&&!saves.hidden)||
    (manager&&getComputedStyle(manager).display==='flex')||
    document.body.dataset.replay==='true';
};

function key(type,code){
  window.dispatchEvent(new KeyboardEvent(type,{code,key:code==='Space'?' ':code==='KeyW'?'w':'s',bubbles:true,cancelable:true}));
}

function releaseLoft(){
  if(loftKey){key('keyup',loftKey);loftKey=null;}
  if(loftReleaseTimer){clearTimeout(loftReleaseTimer);loftReleaseTimer=0;}
}

function setLoftDirection(code){
  const opposite=code==='KeyW'?'KeyS':'KeyW';
  if(loftKey!==code){
    if(loftKey)key('keyup',loftKey);
    key('keyup',opposite);
    key('keydown',code);
    loftKey=code;
  }
  if(loftReleaseTimer)clearTimeout(loftReleaseTimer);
  loftReleaseTimer=setTimeout(releaseLoft,72);
}

function syntheticPointer(type,state,x=state.x,y=state.y){
  const ev=new PointerEvent(type,{
    pointerId:state.id,
    pointerType:'touch',
    isPrimary:true,
    clientX:x,
    clientY:y,
    buttons:type==='pointerup'||type==='pointercancel'?0:1,
    bubbles:true,
    cancelable:true
  });
  canvas.dispatchEvent(ev);
}

function resetLookTimer(force=false){
  if(!look||!live()||blocked())return;
  const now=performance.now();
  if(!force&&now-lastLookReset<115)return;
  syntheticPointer('pointerdown',look);
  lastLookReset=now;
}

// The game's legacy touch handler begins a throw after a ~220 ms hold. Refreshing
// the left pointer below keeps that handler permanently in its aiming state, so a
// long camera drag can never accidentally charge or release a pass.
const lookGuard=setInterval(()=>resetLookTimer(false),90);

canvas.addEventListener('pointerdown',e=>{
  if(!e.isTrusted||e.pointerType!=='touch'||blocked())return;

  // Preserve the game's pre-snap receiver/audible tapping exactly as it is.
  if(phase()==='call')return;

  const onLeft=e.clientX<innerWidth*.5;
  if(onLeft){
    if(look)return;
    e.preventDefault();
    e.stopImmediatePropagation();
    look={id:e.pointerId,x:e.clientX,y:e.clientY};
    lastLookReset=performance.now();
    syntheticPointer('pointerdown',look);
    return;
  }

  // Right side is a dedicated throwing surface during a live play. Holding builds
  // power; vertical movement only changes loft/bullet and never turns the camera.
  if(!live()||throwTouch)return;
  e.preventDefault();
  e.stopImmediatePropagation();
  throwTouch={id:e.pointerId,x:e.clientX,y:e.clientY,lastY:e.clientY};
  try{canvas.setPointerCapture?.(e.pointerId)}catch{}
  key('keydown','Space');
},{capture:true,passive:false});

canvas.addEventListener('pointermove',e=>{
  if(!e.isTrusted||e.pointerType!=='touch')return;

  if(look&&e.pointerId===look.id){
    e.preventDefault();
    e.stopImmediatePropagation();
    look.x=e.clientX;look.y=e.clientY;
    resetLookTimer(false);
    syntheticPointer('pointermove',look,e.clientX,e.clientY);
    return;
  }

  if(throwTouch&&e.pointerId===throwTouch.id){
    e.preventDefault();
    e.stopImmediatePropagation();
    const dy=e.clientY-throwTouch.lastY;
    throwTouch.lastY=e.clientY;
    throwTouch.x=e.clientX;throwTouch.y=e.clientY;
    if(dy<=-1.5)setLoftDirection('KeyW');
    else if(dy>=1.5)setLoftDirection('KeyS');
    return;
  }
},{capture:true,passive:false});

canvas.addEventListener('pointerup',e=>{
  if(!e.isTrusted||e.pointerType!=='touch')return;

  if(look&&e.pointerId===look.id){
    e.preventDefault();
    e.stopImmediatePropagation();
    look.x=e.clientX;look.y=e.clientY;
    // Reset the legacy hold timer immediately before ending the left gesture, so
    // releasing a long look drag never satisfies its old hold-to-throw threshold.
    resetLookTimer(true);
    syntheticPointer('pointerup',look,e.clientX,e.clientY);
    look=null;
    return;
  }

  if(throwTouch&&e.pointerId===throwTouch.id){
    e.preventDefault();
    e.stopImmediatePropagation();
    releaseLoft();
    key('keyup','Space');
    throwTouch=null;
  }
},{capture:true,passive:false});

canvas.addEventListener('pointercancel',e=>{
  if(!e.isTrusted||e.pointerType!=='touch')return;

  if(look&&e.pointerId===look.id){
    e.stopImmediatePropagation();
    // End the legacy look pointer safely instead of forwarding pointercancel,
    // because the old cancel path also clears the global throw charge state.
    resetLookTimer(true);
    syntheticPointer('pointerup',look,look.x,look.y);
    look=null;
  }

  if(throwTouch&&e.pointerId===throwTouch.id){
    e.stopImmediatePropagation();
    releaseLoft();
    // A cancelled OS gesture should not leave W/S pressed. Space is released so
    // the game's own input state cannot remain charging indefinitely.
    key('keyup','Space');
    throwTouch=null;
  }
},{capture:true,passive:false});

addEventListener('blur',()=>{
  releaseLoft();
  look=null;
  throwTouch=null;
});
addEventListener('beforeunload',()=>clearInterval(lookGuard));
})();
