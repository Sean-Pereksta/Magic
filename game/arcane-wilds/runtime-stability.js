'use strict';

/* Arcane Wilds runtime crash containment.
 * Loaded after the cosmetic/performance/navigation wrappers so a malformed optional
 * visual or transient movement state can never permanently kill the requestAnimationFrame chain.
 */
(function(){
  if(window.__arcaneWildsRuntimeStabilityLoaded)return;
  window.__arcaneWildsRuntimeStabilityLoaded=true;

  let effectFaults=0;
  let loopFaults=0;
  let lastLoopFaultAt=0;

  function finite(v){return Number.isFinite(v)}
  function finiteVector(v){return !!v&&typeof v==='object'&&finite(v.x)&&finite(v.y)}

  /* playerMovement switches to p.dodgeDir while dodgeTime is active, then immediately
     dereferences dir.x/dir.y. Older/transient player state can have dodgeTime > 0 without
     a dodgeDir, which used to throw every frame and leave the world visually frozen while
     menus still worked. Repair only the missing directional state before simulation. */
  function normalizeMovementState(){
    const p=game?.player;
    if(p&&typeof p==='object'){
      if(!finiteVector(p.facing))p.facing={x:1,y:0};
      if(p.dodgeTime>0&&!finiteVector(p.dodgeDir))p.dodgeDir={x:p.facing.x,y:p.facing.y};
    }

    /* Input-manager normally owns these vectors, but startup/hot-reload timing should not
       make core movement unsafe if a partial AWInput object is briefly visible. */
    const input=window.AWInput;
    if(input&&typeof input==='object'&&!finiteVector(input.move)){
      try{input.move={x:0,y:0}}catch(_){}
    }
    if(input?.aim&&typeof input.aim==='object'){
      if(!finite(input.aim.x))input.aim.x=0;
      if(!finite(input.aim.y))input.aim.y=0;
    }
  }

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
     Normalize essential movement state before entering that wrapped loop, then retain
     the existing last-resort frame re-arm for unrelated faults. */
  if(typeof loop==='function'){
    const baseLoop=loop;
    loop=function(now){
      try{
        normalizeMovementState();
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
