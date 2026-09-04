'use strict';

/* Mobile-only input/runtime stabilization.
 * Loaded after systems.js and before ui.js so touch controls bind through this cheaper path.
 */
(function(){
  if(!isTouch||window.__arcaneWildsMobileRuntimeLoaded)return;
  window.__arcaneWildsMobileRuntimeLoaded=true;
  document.documentElement.classList.add('arcane-mobile-lite');

  /* ui.js performs one synchronous render as soon as it finishes loading. On touch
     devices that happens before the late performance governor is installed, so the
     expensive base/visual-polish town renderer can hit the phone at full cost behind
     the start overlay. Keep that bootstrap paint intentionally trivial; the normal
     renderer resumes once the governor is loaded or gameplay is actually running. */
  const baseStartupRender=render;
  render=function(){
    if(!running&&!window.__arcaneWildsPerformanceLoaded){
      ctx.save();
      ctx.setTransform(1,0,0,1,0,0);
      ctx.globalCompositeOperation='source-over';
      ctx.fillStyle='#03060b';
      ctx.fillRect(0,0,canvas.width||Math.max(1,W),canvas.height||Math.max(1,H));
      ctx.restore();
      return;
    }
    return baseStartupRender();
  };

  /* The final performance governor is intentionally loaded after all content wrappers.
     On desktop that late load is harmless, but a fast tap on a phone can otherwise enter
     beginWorld() before the governor has replaced the expensive render path. Keep the
     start screen responsive and delay only the transition into live simulation until the
     local governor script has completed. */
  const baseBeginWorld=beginWorld;
  let queuedWorldStart=false;
  beginWorld=function(){
    if(window.__arcaneWildsPerformanceLoaded)return baseBeginWorld();
    if(queuedWorldStart)return;
    queuedWorldStart=true;
    const waitForGovernor=()=>{
      if(window.__arcaneWildsPerformanceLoaded){
        queuedWorldStart=false;
        baseBeginWorld();
        return;
      }
      requestAnimationFrame(waitForGovernor);
    };
    requestAnimationFrame(waitForGovernor);
  };

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
