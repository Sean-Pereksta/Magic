'use strict';

/* The initial Living Wilds artwork uses the existing enemy combat and wave
 * controllers. Content selection is transient; save and boss formats stay owned
 * by the campaign. Load after regional-enemies, enemy-abilities and world-danger.
 */
(() => {
  if(window.AWLivingWildsEnemies)return;
  const definitions={
    lw_thornrunner_stalker:{name:'Thornrunner Stalker',region:'verdant',biomes:['meadow','forest','thornwild'],min:1,hp:44,speed:2.05,damage:9,r:.33,ai:'regional_beast',color:'#87af69',proj:'#b3df87',xp:11,combatTags:['BEAST','PLANT']},
    lw_spore_hound:{name:'Spore Hound',region:'verdant',biomes:['forest','swamp','thornwild'],min:3,hp:62,speed:1.3,damage:10,r:.39,ai:'regional_spore',color:'#a7ae6d',proj:'#d6df8c',xp:16,combatTags:['BEAST','PLANT']},
    lw_briar_warden:{name:'Briar Warden',region:'verdant',biomes:['forest','thornwild','ruins'],min:4,hp:125,speed:.7,damage:15,r:.49,ai:'regional_moss',color:'#7c9970',proj:'#c3d98d',xp:25,combatTags:['HUMANOID','ARMORED','PLANT']},
    lw_lantern_wasp:{name:'Lantern Wasp',region:'verdant',biomes:['meadow','forest','swamp'],min:2,hp:34,speed:2.15,damage:8,r:.26,ai:'regional_wasp',color:'#e2b55b',proj:'#ffe7a2',xp:12,combatTags:['BEAST','FLYING']},
    lw_prism_duelist:{name:'Prism Duelist',region:'meridian',biomes:['crystal','desert','frost'],min:9,hp:92,speed:1.8,damage:17,r:.34,ai:'bladeDancer',color:'#afd5e8',proj:'#e0c5ff',xp:33,combatTags:['HUMANOID','ARMORED','ARCANE']},
    lw_thundercoil_skimmer:{name:'Thundercoil Skimmer',region:'meridian',biomes:['stormlands','crystal'],min:10,hp:83,speed:1.65,damage:16,r:.35,ai:'stormknight',color:'#6cbad3',proj:'#a3f1ff',xp:33,combatTags:['BEAST','FLYING','LIGHTNING']},
    lw_ash_reclaimer:{name:'Ash Reclaimer',region:'meridian',biomes:['volcanic','desert'],min:11,hp:145,speed:.95,damage:21,r:.44,ai:'executioner',color:'#bc6d54',proj:'#ffb279',xp:43,combatTags:['HUMANOID','ARMORED','FIRE']},
    lw_obsidian_bulwark:{name:'Obsidian Bulwark',region:'meridian',biomes:['volcanic','crystal'],min:12,hp:205,speed:.6,damage:22,r:.57,ai:'shield',color:'#65536b',proj:'#f2a070',xp:49,combatTags:['CONSTRUCT','ARMORED','FIRE']},
    lw_rift_marauder:{name:'Rift Marauder',region:'gloam',biomes:['gloam','crypt'],min:18,hp:130,speed:1.55,damage:23,r:.35,ai:'regional_stalker',color:'#ac90da',proj:'#d2a8ff',xp:46,combatTags:['HUMANOID','ARMORED','SHADOW']},
    lw_veil_stalker:{name:'Veil Stalker',region:'gloam',biomes:['gloam','bloodroot'],min:18,hp:105,speed:1.9,damage:21,r:.34,ai:'dangerAssassin',color:'#9c7fb7',proj:'#d0a4ed',xp:42,combatTags:['BEAST','SHADOW']},
    lw_soul_collector:{name:'Soul Collector',region:'gloam',biomes:['crypt','bloodroot','celestial'],min:19,hp:140,speed:.9,damage:22,r:.39,ai:'regional_shepherd',color:'#b58dd2',proj:'#dfc2f5',xp:49,combatTags:['UNDEAD','ARCANE','SHADOW']},
    lw_astral_executioner:{name:'Astral Executioner',region:'gloam',biomes:['celestial','crypt','volcanic'],min:20,hp:225,speed:.8,damage:27,r:.54,ai:'executioner',color:'#c0b7d5',proj:'#fff0ba',xp:62,combatTags:['HUMANOID','ARMORED','ARCANE']}
  };
  Object.assign(ENEMY_TYPES,definitions);
  const ids=Object.keys(definitions),regions={verdant:[],meridian:[],gloam:[]};
  for(const id of ids)regions[definitions[id].region].push(id);
  function eligible(node,room){
    if(!node||node.shadow||room?.boss||room?.town)return [];
    const gate=Math.max(Number(node.threat||1)+3,Number(game.level||1)+Math.floor(Number(room?.difficulty||node.threat||1)*.8));
    return (regions[node.continent]||[]).filter(id=>definitions[id].biomes.includes(node.biome)&&definitions[id].min<=gate);
  }
  // Public pools are used by objectives and reinforcement selection. The
  // campaign's private initial squads are adapted below without another clock.
  const basePool=AWCampaign.pool;
  AWCampaign.pool=function(node){return [...new Set([...basePool(node),...eligible(node)])];};
  let spawning=null;
  const baseSpawn=spawnEnemy;
  spawnEnemy=function(id,pos,elite=false,scale=1){
    if(spawning&&scale===1&&!definitions[id]&&ENEMY_TYPES[id]?.damage>0){
      const order=spawning.ordinal++;
      // Keep half the established squad, including its support roles. Bosses
      // and scaled summons never enter this selection scope.
      if(order%2===0)id=spawning.ids[(spawning.offset+Math.floor(order/2))%spawning.ids.length];
    }
    return baseSpawn(id,pos,elite,scale);
  };
  const baseRoomSpawn=spawnRoomEnemies;
  spawnRoomEnemies=function(room){
    const node=AWCampaignData.nodes[room.campaignNode],available=eligible(node,room),previous=spawning;
    spawning=available.length?{ids:available,ordinal:0,offset:(node.index||0)+(room.campaignRoom||0)}:null;
    try{
      const result=baseRoomSpawn(room),encounter=intensityState().encounter;
      if(available.length&&encounter?.roomKey===room.key&&!encounter.boss){
        const existing=encounter.pool||AWCampaign.pool(node);
        encounter.pool=[...new Set([...existing,...available])];
      }
      return result;
    }finally{spawning=previous;}
  };
  window.AWLivingWildsEnemies={ids,regions,definitions,eligible};
})();
