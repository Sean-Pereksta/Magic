'use strict';

/* Arcane Wilds runtime crash containment.
 * Loaded after the cosmetic/performance/navigation wrappers so a malformed optional
 * visual can never permanently kill the requestAnimationFrame chain.
 */
(function(){
  if(window.__arcaneWildsRuntimeStabilityLoaded)return;
  window.__arcaneWildsRuntimeStabilityLoaded=true;

  let effectFaults=0;
  let loopFaults=0;
  let lastLoopFaultAt=0;

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

  /* Both the original and mobile-optimized loops schedule the next frame only after
     update + render finish. One exception therefore used to stop Arcane Wilds forever.
     Re-arm the frame chain after a fault; repeated faults also force cosmetic low mode. */
  if(typeof loop==='function'){
    const baseLoop=loop;
    loop=function(now){
      try{
        const result=baseLoop(now);
        loopFaults=0;
        return result;
      }catch(err){
        const faultAt=typeof performance!=='undefined'&&performance.now?performance.now():Date.now();
        loopFaults=faultAt-lastLoopFaultAt<2000?loopFaults+1:1;
        lastLoopFaultAt=faultAt;
        purgeInvalidCosmetics();
        if(window.arcaneWildsPerformance)window.arcaneWildsPerformance.low=true;
        if(loopFaults<=3||loopFaults%60===0)console.error('Arcane Wilds recovered from a frame exception',err);
        if(typeof toastMsg==='function'&&loopFaults===1){
          try{toastMsg('Recovered from a graphics error.')}catch(_){}
        }
        if(running)requestAnimationFrame(loop);
      }
    };
  }
})();
