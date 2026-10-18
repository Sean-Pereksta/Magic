(function(root){
  'use strict';
  // Shared material vocabulary for authored prefabs and the arena atlas surfaces.
  const SURFACES=Object.freeze({
    metal:{color:'#ffe1a0',kind:'spark',count:7,life:.25,speed:6,sound:1700},
    concrete:{color:'#b9aea1',kind:'dust',count:5,life:.42,speed:2.8,sound:280},
    glass:{color:'#b6f4ff',kind:'chip',count:7,life:.30,speed:4.2,sound:2700},
    organic:{color:'#acdf63',kind:'organic',count:5,life:.36,speed:3.2,sound:140},
    shield:{color:'#68cfff',kind:'ring',count:2,life:.22,speed:0,sound:840}
  });
  const FIRING=Object.freeze({
    whisker:{frequency:118,decay:.095,smoke:1,halo:.72},clawbreaker:{frequency:54,decay:.22,smoke:3,halo:1},
    purr90:{frequency:205,decay:.055,smoke:1,halo:.46},burrower:{frequency:285,decay:.045,smoke:1,halo:.25},
    squeakshot:{frequency:85,decay:.17,smoke:2,halo:.8},thunderpaw:{frequency:66,decay:.14,smoke:2,halo:.9},
    pistol:{frequency:150,decay:.10,smoke:1,halo:.7},assault:{frequency:110,decay:.09,smoke:1,halo:.7},
    smg:{frequency:190,decay:.06,smoke:0,halo:.5},burst:{frequency:170,decay:.08,smoke:1,halo:.65},
    shotgun:{frequency:62,decay:.18,smoke:3,halo:1},sniper:{frequency:72,decay:.20,smoke:2,halo:.95},
    arc:{frequency:690,decay:.12,smoke:0,halo:.8},plasma:{frequency:340,decay:.16,smoke:0,halo:.9},
    laser:{frequency:1040,decay:.05,smoke:0,halo:.45},mender:{frequency:560,decay:.05,smoke:0,halo:.3},
    rocket:{frequency:45,decay:.22,smoke:3,halo:1},mortar:{frequency:40,decay:.24,smoke:3,halo:1},
    punch:{frequency:90,decay:.12,smoke:0,halo:.6},harpoon:{frequency:230,decay:.11,smoke:1,halo:.6}
  });
  // Physical gun effects are independent of damage and collision. Compact trails
  // travel to the already resolved hit instead of leaving a full-screen beam.
  const BALLISTICS=Object.freeze({
    whisker:{shape:'assault',flash:.76,life:.060,kick:[.008,.0024],speed:640,tail:6.5,width:1.8},
    clawbreaker:{shape:'shotgun',flash:1.30,life:.090,kick:[.030,.0065],speed:430,tail:3.2,width:1.25},
    purr90:{shape:'smg',flash:.52,life:.045,kick:[.0038,.0032],speed:540,tail:4.5,width:1.4},
    burrower:{shape:'suppressed',flash:.20,life:.035,kick:[.0058,.0018],speed:610,tail:3.5,width:1.0},
    squeakshot:{shape:'sniper',flash:.95,life:.075,kick:[.025,.0036],speed:920,tail:10,width:2.1},
    thunderpaw:{shape:'heavy',flash:1.10,life:.080,kick:[.013,.0040],speed:700,tail:8,width:2.0}
  });
  function tracerWindow(shot){
    const length=Math.max(0,shot.len||0);
    if(!shot.ballistic)return {start:0,end:length,alpha:1};
    const elapsed=Math.max(0,(shot.maxLife||0)-(shot.life||0)),speed=Math.max(1,shot.speed||600),tail=Math.max(.1,shot.tail||5);
    const head=elapsed*speed+Math.min(tail,length),start=Math.min(length,Math.max(0,head-tail)),end=Math.min(length,head);
    return {start,end,alpha:start>=end?0:Math.min(1,(length-start)/Math.min(tail,length||1))};
  }
  function stereoPan(listener,source){
    if(!listener||!source)return 0;
    const dx=source.x-listener.x,dz=source.z-listener.z,distance=Math.hypot(dx,dz);
    return distance<1.2?0:Math.max(-.82,Math.min(.82,(dx*Math.cos(listener.yaw||0)-dz*Math.sin(listener.yaw||0))/distance*.82));
  }
  function surfaceFor(block,mapId=''){
    const p=block?.structure||block||{},name=[p.material,p.materialId,p.surface,p.tile,p.survivalMaterial,p.prefab,p.prefabId,p.environmentId,p.id].filter(Boolean).join(' ').toLowerCase();
    if(/shield|forcefield/.test(name))return 'shield';
    if(/glass|window|containment/.test(name))return 'glass';
    if(/organic|hive|flesh|bone|rib|tendril|egg|membrane|heart/.test(name)||/living_hive|swarm_heart/.test(mapId))return 'organic';
    if(/concrete|stone|rock|cliff|sand|crystal|ice|terrain|garden|floor/.test(name)||mapId==='xenofall_wastes')return 'concrete';
    return 'metal';
  }
  function feedbackKind({head=false,shield=false,armor=0,killed=false}={}){return killed?'kill':head?'headshot':shield?'shield':armor>0?'armor':'health'}
  function attenuation(distance,range=52){return Math.max(0,1-distance/range)**2}
  const api={SURFACES,FIRING,BALLISTICS,tracerWindow,stereoPan,surfaceFor,feedbackKind,attenuation};
  root.NeonCombatFeedback=api;if(typeof module==='object')module.exports=api;
})(typeof globalThis==='object'?globalThis:this);
