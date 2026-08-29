'use strict';

/* Mobile-only input/runtime stabilization.
 * Loaded after systems.js and before ui.js so touch controls bind through this cheaper path.
 */
(function(){
  if(!isTouch||window.__arcaneWildsMobileRuntimeLoaded)return;
  window.__arcaneWildsMobileRuntimeLoaded=true;
  document.documentElement.classList.add('arcane-mobile-lite');

  /* The original stick handler called getBoundingClientRect() and changed CSS on every
     pointermove. Phones can deliver many pointer events per frame, forcing repeated
     layout + paint work while the canvas is also rendering combat. Cache geometry at
     pointer-down and collapse visual updates to one animation frame. */
  bindStick=function(zoneId,stick,knobId){
    const zone=$(zoneId),knob=$(knobId);
    let cx=0,cy=0,pendingX=0,pendingY=0,raf=0;

    function refreshCenter(){
      const r=zone.getBoundingClientRect();
      cx=r.left+r.width/2;
      cy=r.top+r.height/2;
    }

    function flush(){
      raf=0;
      const dx=pendingX-cx,dy=pendingY-cy,len=Math.hypot(dx,dy),max=43,k=Math.min(1,len/max);
      stick.x=len?dx/len*k:0;
      stick.y=len?dy/len*k:0;
      stick.active=true;
      knob.style.transform=`translate3d(calc(-50% + ${stick.x*34}px),calc(-50% + ${stick.y*34}px),0)`;
    }

    function queuePoint(e){
      pendingX=e.clientX;
      pendingY=e.clientY;
      if(!raf)raf=requestAnimationFrame(flush);
    }

    zone.addEventListener('pointerdown',e=>{
      stick.pointer=e.pointerId;
      refreshCenter();
      zone.setPointerCapture(e.pointerId);
      queuePoint(e);
      e.preventDefault();
    },{passive:false});

    zone.addEventListener('pointermove',e=>{
      if(stick.pointer!==e.pointerId)return;
      queuePoint(e);
      e.preventDefault();
    },{passive:false});

    const end=e=>{
      if(stick.pointer!==e.pointerId)return;
      if(raf){cancelAnimationFrame(raf);raf=0;}
      stick.pointer=null;
      stick.x=stick.y=0;
      stick.active=false;
      knob.style.transform='translate3d(-50%,-50%,0)';
      e.preventDefault();
    };

    zone.addEventListener('pointerup',end,{passive:false});
    zone.addEventListener('pointercancel',end,{passive:false});
    zone.addEventListener('lostpointercapture',end,{passive:false});
  };
})();
