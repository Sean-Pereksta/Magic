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
    pistol:{frequency:150,decay:.10,smoke:1,halo:.7},assault:{frequency:110,decay:.09,smoke:1,halo:.7},
    smg:{frequency:190,decay:.06,smoke:0,halo:.5},burst:{frequency:170,decay:.08,smoke:1,halo:.65},
    shotgun:{frequency:62,decay:.18,smoke:3,halo:1},sniper:{frequency:72,decay:.20,smoke:2,halo:.95},
    arc:{frequency:690,decay:.12,smoke:0,halo:.8},plasma:{frequency:340,decay:.16,smoke:0,halo:.9},
    laser:{frequency:1040,decay:.05,smoke:0,halo:.45},mender:{frequency:560,decay:.05,smoke:0,halo:.3},
    rocket:{frequency:45,decay:.22,smoke:3,halo:1},mortar:{frequency:40,decay:.24,smoke:3,halo:1},
    punch:{frequency:90,decay:.12,smoke:0,halo:.6},harpoon:{frequency:230,decay:.11,smoke:1,halo:.6}
  });
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
  const api={SURFACES,FIRING,surfaceFor,feedbackKind,attenuation};
  root.NeonCombatFeedback=api;if(typeof module==='object')module.exports=api;
})(typeof globalThis==='object'?globalThis:this);
