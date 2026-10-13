/* Endless Breach permanent progression. No network or game globals are required. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.NeonSurvivalProgress = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
  'use strict';
  const VERSION = 2, KEY = 'neon-breach.survival.v2', BACKUP = KEY + '.backup';
  const maps = Object.freeze([
    {id:'breach_ring',name:'Breach Ring',unlockWave:0,tag:'FORTIFY & DEFEND',description:'Hold the military perimeter, build a stronghold, and answer every breach.',accent:'#50d9ef'},
    {id:'derelict_station',name:'Derelict Station',unlockWave:5,tag:'TUNNELS & FLANKS',description:'Abandoned trains divide the platforms. Watch the maintenance routes.',accent:'#ef69da'},
    {id:'xenofall_wastes',name:'Xenofall Wastes',unlockWave:10,tag:'HUNTERS & CANYONS',description:'Crystal canyons and giant bones shelter burrowing threats in the open desert.',accent:'#ffb45d'},
    {id:'black_ice_cryolab',name:'Black Ice Cryolab',unlockWave:15,tag:'FROST & CONTAINMENT',description:'Control connected laboratories while cooling failures cloud the corridors.',accent:'#9eefff'},
    {id:'living_hive',name:'The Living Hive',unlockWave:20,tag:'CLIMBERS & GROWTH',description:'Living tunnels and raised membranes reshape the routes through the hive.',accent:'#d08aff'},
    {id:'drowned_powerworks',name:'Drowned Powerworks',unlockWave:25,tag:'WATER & ELECTRICITY',description:'Fight over reactor bridges, shallow channels, and warned electrical surges.',accent:'#70e9bc'},
    {id:'orbital_graveyard',name:'Orbital Graveyard',unlockWave:30,tag:'WRECKAGE & LOW GRAVITY',description:'Cross broken docking arms above a distant planet as aerial packs descend.',accent:'#8daaff'},
    {id:'swarm_heart',name:'The Swarm Heart',unlockWave:40,tag:'COMMANDERS & BIOLOGY',description:'Break into the invasion’s pulsing core. Advanced squads defend its living chambers.',accent:'#ff637e'}
  ].map(Object.freeze));
  const rewards = Object.freeze([
    {id:'first_breach',wave:5,name:'First Breach',kind:'badge',art:'first_breach.svg'},
    {id:'raptor_hunter',wave:10,name:'Raptor Hunter',kind:'finish',art:'finish_raptor_hunter.svg',palette:['#153a35','#45e2a5','#e9d383']},
    {id:'frostbite',wave:15,name:'Frostbite',kind:'armor',art:'armor_frostbite.svg',palette:['#e2f8ff','#68b6df','#c5e3ef']},
    {id:'decoy_projector',wave:20,name:'Decoy Projector',kind:'equipment',art:'equipment_decoy.svg'},
    {id:'reactor',wave:25,name:'Reactor',kind:'finish',art:'finish_reactor.svg',palette:['#302a1c','#ffae24','#8df9fa']},
    {id:'veteran_survivor',wave:30,name:'Veteran Survivor',kind:'title',art:'veteran_survivor.svg'},
    {id:'hivebreaker',wave:40,name:'Hivebreaker',kind:'armor',art:'armor_hivebreaker.svg',palette:['#3d173d','#eb75fa','#70fcd1']},
    {id:'legendary_survivor',wave:50,name:'Legendary Survivor',kind:'badge',art:'legendary_survivor.svg'},
    {id:'legendary_elimination',wave:50,name:'Legendary Elimination',kind:'effect',art:'effect_legendary.svg',palette:['#fddd70','#8afaff','#ffffff']},
    {id:'apex_predator',wave:75,name:'Apex Predator',kind:'finish',art:'finish_apex_predator.svg',palette:['#211822','#fa4168','#eee5d5']},
    {id:'immortal_defender',wave:100,name:'Immortal Defender',kind:'title',art:'immortal_defender.svg'},
    {id:'immortal_trophy',wave:100,name:'Immortal Defender Trophy',kind:'trophy',art:'trophy_immortal.svg'}
  ].map(Object.freeze));
  const achievements = Object.freeze([
    {id:'fortress_defender',name:'Fortress Defender',description:'In one run, build 8 structures, eliminate 30 aliens near your fort, and clear Wave 10.',art:'fortress_defender.svg'},
    {id:'boss_slayer',name:'Boss Slayer',description:'Defeat 10 bosses across survival runs.',art:'boss_slayer.svg'},
    {id:'unbreakable',name:'Unbreakable',description:'Clear 5 consecutive waves without taking health or armor damage.',art:'unbreakable.svg'},
    {id:'demolition_expert',name:'Demolition Expert',description:'Destroy 20 structures across survival runs.',art:'demolition_expert.svg'},
    {id:'sky_hunter',name:'Sky Hunter',description:'Eliminate 50 flying aliens across survival runs.',art:'sky_hunter.svg'},
    {id:'last_stand',name:'Last Stand',description:'Clear a wave as the last living player in a two-player run.',art:'last_stand.svg'},
    {id:'architect',name:'Architect',description:'Build 25 structures across survival runs.',art:'architect.svg'},
    {id:'world_survivor',name:'World Survivor',description:'Clear Wave 5 in all eight arenas.',art:'world_survivor.svg'}
  ].map(Object.freeze));
  // Full character meshes are cosmetic and usable in every mode once earned.
  const characters = Object.freeze([
    {id:'player_one',name:'Breacher',tag:'ASSAULT EXOSUIT',description:'A broad armored frontline operator with a reinforced helmet and heavy boots.',accent:'#61eaff',unlockWave:0,unlockText:'Available from the start'},
    {id:'player_two',name:'Sentinel',tag:'TACTICAL EXOSUIT',description:'A plated tactical operator with gold armor and angular shoulders.',accent:'#ffd475',unlockWave:0,unlockText:'Available from the start'},
    {id:'character_bulwark',name:'Bulwark',tag:'SIEGE JUGGERNAUT',description:'Towering blast armor, slab shoulders, and a glowing reactor core.',accent:'#ffb856',unlockWave:5,unlockText:'Clear Wave 5 in Survivor'},
    {id:'character_ronin',name:'Ronin',tag:'CYBER SAMURAI',description:'A heavy cyber samurai with a crested helmet and layered battle plates.',accent:'#ff607f',unlockWave:10,unlockText:'Clear Wave 10 in Survivor'},
    {id:'character_warden',name:'Warden',tag:'HAZARD WARDEN',description:'A heavy industrial enforcer with a sealed respirator and acid-green reactor armor.',accent:'#b8ff64',unlockWave:15,unlockAchievement:'fortress_defender',unlockText:'Clear Wave 15 or earn Fortress Defender'},
    {id:'character_revenant',name:'Revenant',tag:'PHANTOM HUNTER',description:'A dark armored hunter with a skeletal visor and spectral energy fins.',accent:'#b88aff',unlockWave:25,unlockAchievement:'sky_hunter',unlockText:'Clear Wave 25 or earn Sky Hunter'},
    {id:'character_praetorian',name:'Praetorian',tag:'ELITE COMMANDER',description:'A massive elite commander with a crownlike crest and ornate armor.',accent:'#ffd475',unlockWave:40,unlockAchievement:'boss_slayer',unlockText:'Clear Wave 40 or earn Boss Slayer'},
    {id:'character_overlord',name:'Overlord',tag:'BREACH WARLORD',description:'The ultimate breach warlord: spiked armor, enormous pauldrons, and a blazing core.',accent:'#ff5454',unlockWave:50,unlockText:'Clear Wave 50 in Survivor'}
  ].map(c=>Object.freeze({...c,modelPath:'characters/'+c.id+'.glb'})));
  // Run-only purchases. Permanent progression grants choice, never passive combat stats.
  const equipment = Object.freeze([
    {id:'decoy',name:'Decoy Projector',unlockWave:20,cost:26,cooldown:22,duration:9,maxActive:1,radius:21,hp:80,charges:1,art:'equipment_decoy.svg',description:'A visible hologram draws susceptible hunters for 9 seconds. Bosses and artillery resist.'},
    {id:'shockFence',name:'Shock Fence',unlockWave:0,cost:22,cooldown:18,duration:14,maxActive:2,radius:4.5,damage:12,charges:1,art:'equipment_shock_fence.svg',description:'Deploy a short electrical line. It slows and shocks smaller ground aliens; brutes can break it.'},
    {id:'empMine',name:'EMP Mine',unlockWave:10,cost:24,cooldown:16,duration:30,maxActive:2,radius:9,disruptDuration:5,charges:1,art:'equipment_emp_mine.svg',description:'A proximity pulse interrupts ranged fire, shields, and special abilities for 5 seconds.'},
    {id:'sentry',name:'Automated Sentry',unlockWave:15,cost:38,cooldown:32,duration:18,maxActive:1,radius:27,damage:9,fireInterval:.42,hp:90,charges:1,art:'equipment_sentry.svg',description:'A fragile 18-second turret covers a firing lane. It needs a clear line of sight and can be destroyed.'},
    {id:'shieldBeacon',name:'Emergency Shield Beacon',unlockWave:25,cost:34,cooldown:28,duration:12,maxActive:1,radius:7,hp:160,charges:1,art:'equipment_shield_beacon.svg',description:'A visible defensive field absorbs at most 160 incoming damage for nearby survivors, then collapses.'}
  ].map(Object.freeze));
  const fields = ['kills','bosses','seconds','flyingKills','structuresBuilt','structuresDestroyed','defenseKills','damageTaken','lastSurvivorWaves'];
  const mapIds = new Set(maps.map(m=>m.id)), rewardIds = new Set(rewards.map(r=>r.id)), achievementIds = new Set(achievements.map(a=>a.id)), characterIds = new Set(characters.map(c=>c.id));
  const emptyStats = () => Object.fromEntries(fields.map(k=>[k,0]));
  const clone = value => JSON.parse(JSON.stringify(value));
  const number = n => typeof n === 'number' && Number.isFinite(n) ? Math.min(1e12,Math.max(0,n)) : 0;
  const integer = n => Math.floor(number(n));
  const knownList = (list, ids) => Array.isArray(list) ? [...new Set(list.filter(id=>ids.has(id)))] : [];
  const characterEarned = (character,state) => character.unlockWave<=state.highestWave||!!(character.unlockAchievement&&state.achievements.includes(character.unlockAchievement));
  function fresh() {
    return {version:VERSION,revision:0,updatedAt:0,selectedUpdatedAt:0,characterUpdatedAt:0,highestWave:0,stats:emptyStats(),arenas:Object.fromEntries(maps.map(m=>[m.id,{highestWave:0,kills:0,bosses:0,seconds:0,runs:0}])),unlockedMaps:['breach_ring'],unlockedRewards:[],unlockedCharacters:['player_one','player_two'],achievements:[],selected:{finish:'default',armor:'default',title:'default',effect:'default',character:'player_one'},recentRuns:[]};
  }
  function sanitize(raw) {
    const state=fresh();
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return state;
    state.revision=integer(raw.revision); state.updatedAt=integer(raw.updatedAt);state.selectedUpdatedAt=integer(raw.selectedUpdatedAt);state.characterUpdatedAt=integer(raw.characterUpdatedAt);
    const stats=raw.stats||raw.totals||{};
    for(const field of fields) state.stats[field]=number(stats[field]);
    const arenas=raw.arenas||raw.records||{};
    for(const map of maps){const record=arenas[map.id]||{};for(const k of ['highestWave','kills','bosses','seconds','runs'])state.arenas[map.id][k]=number(record[k]);state.arenas[map.id].highestWave=integer(record.highestWave);}
    state.highestWave=Math.max(integer(raw.highestWave||raw.highestWaveCleared),...Object.values(state.arenas).map(a=>a.highestWave));
    state.unlockedMaps=knownList(raw.unlockedMaps,mapIds);
    for(const map of maps)if(map.unlockWave<=state.highestWave&&!state.unlockedMaps.includes(map.id))state.unlockedMaps.push(map.id);
    state.unlockedRewards=knownList(raw.unlockedRewards,rewardIds);
    for(const reward of rewards)if(reward.wave<=state.highestWave&&!state.unlockedRewards.includes(reward.id))state.unlockedRewards.push(reward.id);
    state.achievements=knownList(raw.achievements,achievementIds);
    state.unlockedCharacters=knownList(raw.unlockedCharacters,characterIds);
    for(const character of characters)if(characterEarned(character,state)&&!state.unlockedCharacters.includes(character.id))state.unlockedCharacters.push(character.id);
    for(const kind of ['finish','armor','title','effect']){const id=raw.selected?.[kind];if(id==='default'||rewards.some(r=>r.id===id&&r.kind===kind&&state.unlockedRewards.includes(id)))state.selected[kind]=id;}
    if(state.unlockedCharacters.includes(raw.selected?.character))state.selected.character=raw.selected.character;
    state.recentRuns=Array.isArray(raw.recentRuns)?raw.recentRuns.slice(-12).filter(r=>r&&mapIds.has(r.mapId)).map(r=>({id:String(r.id||'').slice(0,80),mapId:r.mapId,waves:integer(r.waves),kills:integer(r.kills),bosses:integer(r.bosses),seconds:number(r.seconds),endedAt:integer(r.endedAt)})):[];
    return state;
  }
  function createStore(options={}) {
    let storage=options.storage;
    if(storage===undefined)try{storage=root.localStorage}catch(_){storage=null}
    const now=typeof options.now==='function'?options.now:Date.now, listeners=new Set();
    let persistenceAvailable=!!storage, run=null, lastWrite=0, remote=null, syncBusy=false, syncAgain=false;
    function read(key){try{const value=storage?.getItem(key);return value?sanitize(JSON.parse(value)):null}catch(_){return null}}
    let state=read(KEY)||read(BACKUP)||read('neon-breach.survival.v1')||fresh();
    let lastCharacter=state.selected.character;
    function snapshot(){return clone(state)}
    function emit(batch){
      if(lastCharacter!==state.selected.character){lastCharacter=state.selected.character;const character=getEquippedCharacter();if(typeof root.dispatchEvent==='function'&&typeof root.CustomEvent==='function')root.dispatchEvent(new root.CustomEvent('neon-character-changed',{detail:{id:character.id,modelPath:character.modelPath,character}}));}
      for(const listener of listeners)try{listener(snapshot(),batch||null)}catch(error){root.console?.warn?.('Survival progression listener failed',error)}
    }
    function persist(immediate=true){
      if(!immediate&&now()-lastWrite<15000)return;
      state.updatedAt=now();state.revision++;lastWrite=now();
      try{if(storage){const data=JSON.stringify(state);storage.setItem(KEY,data);storage.setItem(BACKUP,data);persistenceAvailable=true}}catch(_){persistenceAvailable=false}
      emit();queueSync();
    }
    // A host may provide its already-authorized profile read/write adapter. We never guess Firebase paths.
    async function queueSync(){
      if(!remote)return;if(syncBusy){syncAgain=true;return}syncBusy=true;
      try{await remote.write(snapshot())}catch(_){/* offline remains locally authoritative */}
      finally{syncBusy=false;if(syncAgain){syncAgain=false;queueSync()}}
    }
    function commitStats(){
      if(!run)return;
      const record=state.arenas[run.mapId];
      for(const field of fields){const delta=Math.max(0,run.stats[field]-run.counted[field]);state.stats[field]=Math.min(1e12,state.stats[field]+delta);if(['kills','bosses','seconds'].includes(field))record[field]=Math.min(1e12,record[field]+delta);run.counted[field]=run.stats[field];}
    }
    function earn(batch){
      for(const map of maps)if(map.unlockWave<=state.highestWave&&!state.unlockedMaps.includes(map.id)){state.unlockedMaps.push(map.id);batch.maps.push(map.id)}
      for(const reward of rewards)if(reward.wave<=state.highestWave&&!state.unlockedRewards.includes(reward.id)){state.unlockedRewards.push(reward.id);batch.rewards.push(reward.id)}
      const tests={fortress_defender:run&&run.waves>=10&&run.stats.structuresBuilt>=8&&run.stats.defenseKills>=30,boss_slayer:state.stats.bosses>=10,unbreakable:run&&run.flawlessWaves>=5,demolition_expert:state.stats.structuresDestroyed>=20,sky_hunter:state.stats.flyingKills>=50,last_stand:run&&run.stats.lastSurvivorWaves>=1,architect:state.stats.structuresBuilt>=25,world_survivor:maps.every(m=>state.arenas[m.id].highestWave>=5)};
      for(const award of achievements)if(tests[award.id]&&!state.achievements.includes(award.id)){state.achievements.push(award.id);batch.achievements.push(award.id)}
      for(const character of characters)if(characterEarned(character,state)&&!state.unlockedCharacters.includes(character.id)){state.unlockedCharacters.push(character.id);batch.characters.push(character.id)}
      if(run){for(const kind of ['maps','rewards','achievements','characters'])for(const id of batch[kind])if(!run.earned[kind].includes(id))run.earned[kind].push(id);}
    }
    function batch(){return {maps:[],rewards:[],achievements:[],characters:[],records:[]}}
    function beginRun(mapId,settings={}){
      if(!mapIds.has(mapId)||!state.unlockedMaps.includes(mapId))return null;
      if(run&&!run.finished)finishRun();
      run={id:now().toString(36)+'-'+Math.random().toString(36).slice(2,9),mapId,teamSize:settings.teamSize===2?2:1,waves:0,stats:emptyStats(),counted:emptyStats(),flawlessWaves:0,lastWaveDamage:0,finished:false,startedAt:now(),previousBest:state.arenas[mapId].highestWave,earned:{maps:[],rewards:[],achievements:[],characters:[]},recordNames:[]};
      state.arenas[mapId].runs++;persist();return getRun();
    }
    function getRun(){return run?clone(run):null}
    function recordEvent(name,amount=1){if(!run||run.finished||!fields.includes(name))return false;run.stats[name]=Math.min(1e12,run.stats[name]+number(amount));return true}
    function addTime(dt){if(!run||run.finished)return;recordEvent('seconds',dt);if(now()-lastWrite>=15000)checkpoint()}
    function checkpoint(stats){if(!run||run.finished)return; if(stats)for(const k of fields)if(k in stats)run.stats[k]=Math.max(run.stats[k],number(stats[k]));commitStats();const earned=batch();earn(earned);persist();if(earned.maps.length||earned.rewards.length||earned.achievements.length||earned.characters.length)emit(earned);return earned}
    function clearWave(wave,details={}){
      const result=batch();wave=integer(wave);
      if(!run||run.finished||wave<=run.waves)return result;
      if(details.stats)for(const k of fields)if(k in details.stats)run.stats[k]=Math.max(run.stats[k],number(details.stats[k]));
      // Skipping straight to a high test wave cannot grant consecutive flawless waves.
      run.flawlessWaves=run.stats.damageTaken===run.lastWaveDamage&&wave===run.waves+1?run.flawlessWaves+1:0;
      run.lastWaveDamage=run.stats.damageTaken;run.waves=wave;
      if(run.teamSize===2&&details.alivePlayers===1)recordEvent('lastSurvivorWaves');
      if(wave>state.highestWave){state.highestWave=wave;result.records.push('Overall wave record')}
      if(wave>state.arenas[run.mapId].highestWave){state.arenas[run.mapId].highestWave=wave;result.records.push('Arena wave record')}
      for(const name of result.records)if(!run.recordNames.includes(name))run.recordNames.push(name);
      commitStats();earn(result);persist();emit(result);return result;
    }
    function finishRun(stats){
      if(!run)return null;
      if(!run.finished){checkpoint(stats);run.finished=true;state.recentRuns.push({id:run.id,mapId:run.mapId,waves:run.waves,kills:integer(run.stats.kills),bosses:integer(run.stats.bosses),seconds:run.stats.seconds,endedAt:now()});state.recentRuns=state.recentRuns.slice(-12);persist();}
      return {id:run.id,mapId:run.mapId,waves:run.waves,kills:integer(run.stats.kills),bosses:integer(run.stats.bosses),seconds:run.stats.seconds,previousBest:run.previousBest,highestWave:state.arenas[run.mapId].highestWave,records:clone(run.recordNames),earned:clone(run.earned),persistenceAvailable};
    }
    function setSelection(kind,id){if(!['finish','armor','title','effect'].includes(kind))return false;if(id!=='default'&&!rewards.some(r=>r.id===id&&r.kind===kind&&state.unlockedRewards.includes(id)))return false;state.selected[kind]=id;state.selectedUpdatedAt=now();persist();return true}
    function getCharacterCatalog(){return characters.map(character=>({...character,unlocked:state.unlockedCharacters.includes(character.id),equipped:state.selected.character===character.id}));}
    function getEquippedCharacter(){return clone(characters.find(character=>character.id===state.selected.character)||characters[0]);}
    function isCharacterUnlocked(id){return characterIds.has(id)&&state.unlockedCharacters.includes(id);}
    function equipCharacter(id){if(!isCharacterUnlocked(id))return false;if(state.selected.character===id)return true;state.selected.character=id;state.characterUpdatedAt=now();persist();return true;}
    function getEquippedCharacterModelPath(){return getEquippedCharacter().modelPath;}
    function mergeRemote(raw){
      const incoming=sanitize(raw);
      for(const field of fields)state.stats[field]=Math.max(state.stats[field],incoming.stats[field]);
      for(const map of maps)for(const field of ['highestWave','kills','bosses','seconds','runs'])state.arenas[map.id][field]=Math.max(state.arenas[map.id][field],incoming.arenas[map.id][field]);
      state.highestWave=Math.max(state.highestWave,incoming.highestWave);
      for(const key of ['unlockedMaps','unlockedRewards','achievements','unlockedCharacters'])state[key]=[...new Set([...state[key],...incoming[key]])];
      if(incoming.selectedUpdatedAt>state.selectedUpdatedAt){for(const kind of ['finish','armor','title','effect'])state.selected[kind]=incoming.selected[kind];state.selectedUpdatedAt=incoming.selectedUpdatedAt;}
      if(incoming.characterUpdatedAt>state.characterUpdatedAt){state.selected.character=incoming.selected.character;state.characterUpdatedAt=incoming.characterUpdatedAt;}
      const earned=batch();earn(earned);persist();return snapshot();
    }
    async function attachProfileAdapter(adapter){if(!adapter||typeof adapter.read!=='function'||typeof adapter.write!=='function')return false;remote=adapter;try{const loaded=await adapter.read();if(loaded)mergeRemote(loaded);else queueSync();return true}catch(_){return false}}
    return {beginRun,getRun,recordEvent,addTime,checkpoint,clearWave,finishRun,getState:snapshot,isUnlocked:id=>state.unlockedMaps.includes(id),setSelection,getSelection:()=>clone(state.selected),getCharacterCatalog,getEquippedCharacter,isCharacterUnlocked,equipCharacter,getEquippedCharacterModelPath,isEquipmentUnlocked:id=>equipment.some(e=>e.id===id&&e.unlockWave<=state.highestWave),subscribe(fn){listeners.add(fn);return()=>listeners.delete(fn)},mergeRemote,attachProfileAdapter,detachProfileAdapter(){remote=null;syncAgain=false},syncProfile:queueSync,get persistenceAvailable(){return persistenceAvailable}};
  }
  const store=createStore();
  return {VERSION,KEY,BACKUP,maps,rewards,achievements,characters,equipment,createStore,sanitize,store,getCharacterCatalog:()=>store.getCharacterCatalog(),getEquippedCharacter:()=>store.getEquippedCharacter(),isCharacterUnlocked:id=>store.isCharacterUnlocked(id),equipCharacter:id=>store.equipCharacter(id),getEquippedCharacterModelPath:()=>store.getEquippedCharacterModelPath()};
});
