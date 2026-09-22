'use strict';
/* Apply after the campaign and regional content catalogs. These are travel/view
   changes only: the existing movement, dismount, input and save paths still own play. */
(() => {
  if(window.AWTravelTuning)return;
  const mountSpeedBoost=1.8,mobileViewScale=.88;
  const mounts=window.AWCampaignData?.mounts;
  if(mounts){
    // Tune the shared catalog, not saved player stats. Shops, old saves and every
    // mount all use the same speed, retaining terrain bonuses and mount abilities.
    for(const mount of Object.values(mounts)){
      if(Number.isFinite(mount.speed)&&mount.speed>0){
        mount.speed=Math.round(mount.speed*mountSpeedBoost*100)/100;
      }
    }
  }
  const camera=window.AWPresentation?.camera;
  if(typeof isTouch!=='undefined'&&isTouch&&camera&&typeof camera.tick==='function'){
    const tick=camera.tick;
    camera.zoom*=mobileViewScale;
    camera.tick=function(dt){
      // The existing spring owns combat/riding zoom and impact kicks. Give it its
      // unscaled value, then apply the mobile view once (never compound per frame).
      // Projection, pointer aiming and world culling all read this same camera.zoom.
      this.zoom/=mobileViewScale;
      try{return tick.call(this,dt);}finally{this.zoom*=mobileViewScale;}
    };
  }
  window.AWTravelTuning=Object.freeze({mountSpeedBoost,mobileViewScale});
})();
