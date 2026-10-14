(function(root){
  'use strict';
  const MODES=['dm','tdm','frag','siege','survivor'],MAPS=['grid','pastel','reactor','terminal','foundry','outpost'],SURVIVAL_MAPS=['breach_ring','derelict_station','xenofall_wastes','black_ice_cryolab','living_hive','drowned_powerworks','orbital_graveyard','swarm_heart'];
  const DURATIONS=[60,180,300,600,900,1200],TARGETS=[5,10,15,20,30,50,75,100],RESPAWNS=[.5,.75,1,1.5,2,3,5];
  const finite=(value,fallback)=>Number.isFinite(Number(value))?Number(value):fallback,clamp=(n,a,b)=>Math.max(a,Math.min(b,n)),nearest=(value,choices,fallback)=>{const n=finite(value,fallback);return choices.reduce((best,item)=>Math.abs(item-n)<Math.abs(best-n)?item:best,choices[0])};
  function modeSettings(mode,value={}){
    if(!value||typeof value!=='object')value={};
    if(!MODES.includes(mode))mode='dm';const fallback={botCount:mode==='frag'?1:mode==='survivor'?0:6,difficulty:'normal',matchLen:300,scoreTarget:mode==='tdm'?30:mode==='frag'?10:15,respawnDelay:mode==='frag'?.75:1};
    return {botCount:mode==='survivor'?0:Math.round(clamp(finite(value.botCount,fallback.botCount),mode==='siege'?2:0,mode==='frag'?1:16)),difficulty:['easy','normal','hard','nightmare'].includes(value.difficulty)?value.difficulty:fallback.difficulty,matchLen:nearest(value.matchLen,DURATIONS,fallback.matchLen),scoreTarget:nearest(value.scoreTarget,TARGETS,fallback.scoreTarget),respawnDelay:nearest(value.respawnDelay,RESPAWNS,fallback.respawnDelay)};
  }
  function preferences(value={}){
    if(!value||typeof value!=='object')value={};
    const mode=MODES.includes(value.mode)?value.mode:'dm',common=value.common||{},maps={},modes={};for(const id of MODES){const choices=id==='survivor'?SURVIVAL_MAPS:MAPS;maps[id]=choices.includes(value.maps?.[id])?value.maps[id]:choices[0];modes[id]=modeSettings(id,value.modes?.[id])}
    return {mode,maps,modes,common:{secondPlayer:['off','keyboard','controller'].includes(common.secondPlayer)?common.secondPlayer:'off',lookSensitivity:clamp(finite(common.lookSensitivity,1),.25,2.5),volume:clamp(finite(common.volume,100),0,100),visualQuality:['auto','high','medium','low'].includes(common.visualQuality)?common.visualQuality:'auto'}};
  }
  const api={MODES,MAPS,SURVIVAL_MAPS,DURATIONS,TARGETS,RESPAWNS,modeSettings,preferences};root.NeonMatchSettings=api;if(typeof module==='object')module.exports=api;
})(typeof globalThis==='object'?globalThis:this);
