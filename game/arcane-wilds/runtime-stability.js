'use strict';

/* Arcane Wilds runtime crash containment.
 * Loaded after the cosmetic/performance/navigation wrappers so a malformed optional
 * visual can never permanently kill the requestAnimationFrame chain.
 */
(function(){
  if(window.__arcaneWildsRuntimeStabilityLoaded)return;
  window.__arcaneWildsRuntimeStabilityLoaded=true;

  let effectFaults=0;
  const faults=new Map();
  let lastRenderedAt=0;

  function finite(v){return Number.isFinite(v)}

  /* The Rift Puppeteer content-pack effect never supplied `branch`, while the shared
     Worldroot/Puppet renderer feeds it into Math.sin(). That becomes NaN and strict
     mobile canvas implementations can reject the resulting lineTo coordinates. */
  function normalizeEffect(e){
    if(!e||typeof e!=='object')return false;
    if(e.kind==='puppetString'&&!finite(e.branch))e.branch=0;
    if(!finite(e.x)||!finite(e.y))return false;
    return true;
  }

  if(typeof drawEffect==='function'){
    const baseDrawEffect=drawEffect;
    drawEffect=function(e,front){
      if(!normalizeEffect(e)){
        if(e&&typeof e==='object')e.life=0;
        return;
      }
      try{
        return baseDrawEffect(e,front);
      }catch(err){
        effectFaults++;
        if(e&&typeof e==='object')e.life=0;
        if(effectFaults<=3||effectFaults%60===0)console.error('Arcane Wilds skipped a broken cosmetic effect',e?.kind,err);
      }
    };
  }

  function purgeInvalidCosmetics(){
    const effects=game?.effects;
    if(Array.isArray(effects)){
      for(let i=effects.length-1;i>=0;i--){
        const e=effects[i];
        if(!e||!finite(e.x)||!finite(e.y)||!finite(e.life))effects.splice(i,1);
      }
    }
    const particles=game?.particles;
    if(Array.isArray(particles)){
      for(let i=particles.length-1;i>=0;i--){
        const p=particles[i];
        if(!p||!finite(p.x)||!finite(p.y)||!finite(p.life))particles.splice(i,1);
      }
    }
  }

  // One frame owner. Optional input, presentation and HUD faults must not prevent
  // the simulation stage from running; render faults must not kill the RAF chain.
  function stage(name,fn){
    try{fn();return true;}
    catch(err){
      const count=(faults.get(name)||0)+1;faults.set(name,count);
      if(window.arcaneWildsPerformance)window.arcaneWildsPerformance.low=true;
      if(count<=3||count%60===0)console.error('Arcane Wilds '+name+' exception',err);
      return false;
    }
  }
  window.AWRuntime={
    resetClock(){lastRenderedAt=0;},
    frame(now){
      if(!running)return;
      const dt=Math.max(0,Math.min(.033,(now-last)/1000||0));last=now;
      try{
        stage('input',()=>window.AWInput?.poll());
        const presented=stage('presentation',()=>window.AWPresentation?.frame(now));
        if(!presented)window.AWPresentation?.releaseFreeze();
        if(!window.AWPresentation?.frozen)stage('simulation',()=>update(dt));
        stage('HUD',()=>{if(game.player)window.AWModernUI?.tick();});
        const interval=isTouch?1000/(window.arcaneWildsPerformance?.renderFps||60):0;
        if(!lastRenderedAt||now-lastRenderedAt>=interval-.5){
          lastRenderedAt=now;
          if(!stage('render',()=>render()))purgeInvalidCosmetics();
        }
      }finally{if(running)requestAnimationFrame(loop);}
    }
  };
})();
