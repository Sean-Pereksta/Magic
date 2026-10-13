const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const P=require('../game/neon-arena/neon-survival-progress.js');
function memory(seed={}){const map=new Map(Object.entries(seed));return {getItem:key=>map.get(key)||null,setItem:(key,value)=>map.set(key,value),map}}

test('all milestones permanently unlock their worlds and rewards without changing the run arena',()=>{
  const storage=memory(),store=P.createStore({storage});store.beginRun('breach_ring');
  for(const wave of [5,10,15,20,25,30,40,50,75,100]){const result=store.clearWave(wave);for(const id of result.maps)assert(store.isUnlocked(id));assert.equal(store.getRun().mapId,'breach_ring');}
  assert.equal(store.getState().unlockedMaps.length,8);assert.equal(store.getState().unlockedRewards.length,12);
  const restored=P.createStore({storage});assert.equal(restored.getState().highestWave,100);assert.equal(restored.getState().unlockedMaps.length,8);
  assert.equal(restored.isEquipmentUnlocked('decoy'),true);
});
test('cleared-wave records differ from wave reached and are independently tracked per arena',()=>{
  const store=P.createStore({storage:memory()});assert.equal(store.beginRun('swarm_heart'),null);
  store.beginRun('breach_ring');store.clearWave(5);let summary=store.finishRun();assert.equal(summary.waves,5);
  store.beginRun('derelict_station');store.clearWave(2);summary=store.finishRun();
  assert.equal(summary.highestWave,2);assert.equal(store.getState().highestWave,5);assert.equal(store.getState().arenas.breach_ring.highestWave,5);
  assert.equal(store.isUnlocked('xenofall_wastes'),false);
});
test('checkpoints, duplicate wave callbacks, and repeated results cannot double-count stats',()=>{
  const store=P.createStore({storage:memory()});store.beginRun('breach_ring');store.recordEvent('kills',17);store.recordEvent('bosses',1);store.addTime(65.5);
  store.checkpoint();store.clearWave(5);assert.deepEqual(store.clearWave(5),{maps:[],rewards:[],achievements:[],records:[]});
  const a=store.finishRun(),b=store.finishRun();assert.deepEqual(a,b);assert.equal(store.getState().stats.kills,17);assert.equal(store.getState().stats.seconds,65.5);
  store.beginRun('breach_ring');store.recordEvent('kills',3);store.finishRun();assert.equal(store.getState().stats.kills,20);assert.equal(store.getState().arenas.breach_ring.runs,2);
});
test('corrupt storage recovers from backup and malformed fields are sanitized',()=>{
  const storage=memory({[P.KEY]:'{broken',[P.BACKUP]:JSON.stringify({version:1,highestWave:25,stats:{kills:-15,bosses:'bad',seconds:30},unlockedMaps:['bogus'],unlockedRewards:['reactor','unknown'],selected:{finish:'unknown'},arenas:{breach_ring:{highestWave:20,runs:-1}}})});
  const store=P.createStore({storage});assert.equal(store.getState().highestWave,25);assert(store.isUnlocked('drowned_powerworks'));assert.equal(store.getState().stats.kills,0);assert.equal(store.getSelection().finish,'default');assert(!store.getState().unlockedRewards.includes('unknown'));
  assert.equal(P.sanitize(null).version,2);assert.equal(P.sanitize(['bad']).unlockedMaps[0],'breach_ring');
});
test('offline and storage-denied play retain progression in memory',()=>{
  const store=P.createStore({storage:{getItem(){throw Error('denied')},setItem(){throw Error('quota')}}});
  store.beginRun('breach_ring');store.recordEvent('kills',4);store.clearWave(20);assert(store.isUnlocked('living_hive'));assert.equal(store.finishRun().kills,4);assert.equal(store.persistenceAvailable,false);
});
test('special awards require their actual counters and flawless consecutive waves',()=>{
  const store=P.createStore({storage:memory()});store.beginRun('breach_ring',{teamSize:2});store.recordEvent('structuresBuilt',25);store.recordEvent('structuresDestroyed',20);store.recordEvent('bosses',10);store.recordEvent('flyingKills',50);store.recordEvent('defenseKills',30);
  for(let wave=1;wave<=5;wave++)store.clearWave(wave,{alivePlayers:wave===5?1:2});
  assert(store.getState().achievements.includes('unbreakable'));assert(store.getState().achievements.includes('last_stand'));
  store.clearWave(10);const ids=store.getState().achievements;for(const id of ['fortress_defender','boss_slayer','demolition_expert','sky_hunter','architect'])assert(ids.includes(id));
  const damaged=P.createStore({storage:memory()});damaged.beginRun('breach_ring');for(let wave=1;wave<=7;wave++){damaged.recordEvent('damageTaken',1);damaged.clearWave(wave)}assert(!damaged.getState().achievements.includes('unbreakable'));
  const skipped=P.createStore({storage:memory()});skipped.beginRun('breach_ring');skipped.clearWave(100);assert(!skipped.getState().achievements.includes('unbreakable'));
});
test('World Survivor requires reaching Wave 5 in every arena',()=>{
  const store=P.createStore({storage:memory()});store.beginRun('breach_ring');store.clearWave(40);store.finishRun();
  assert(!store.getState().achievements.includes('world_survivor'));
  for(const map of P.maps.slice(1)){store.beginRun(map.id);store.clearWave(5);store.finishRun()}
  assert(store.getState().achievements.includes('world_survivor'));
});
test('appearance choices must be unlocked, persist, and grant no stat bonus',()=>{
  const storage=memory(),store=P.createStore({storage});assert.equal(store.setSelection('finish','apex_predator'),false);store.beginRun('breach_ring');store.clearWave(10);assert.equal(store.setSelection('finish','raptor_hunter'),true);assert.equal(store.setSelection('armor','raptor_hunter'),false);
  assert.equal(P.createStore({storage}).getSelection().finish,'raptor_hunter');assert.equal(store.getState().stats.kills,0);
});
test('explicit profile adapter merges unlocks without regressing local records',async()=>{
  const store=P.createStore({storage:memory()});store.beginRun('breach_ring');store.clearWave(20);store.finishRun();const writes=[];
  await store.attachProfileAdapter({read:async()=>({highestWave:10,stats:{kills:50}}),write:async state=>writes.push(state)});
  assert.equal(store.getState().highestWave,20);assert.equal(store.getState().stats.kills,50);assert(store.isUnlocked('living_hive'));assert(writes.length>0);
});
test('cloud appearance merges restore the newest intentional choice, preserving newer local choices',()=>{
  let tick=100;const store=P.createStore({storage:memory(),now:()=>tick});store.mergeRemote({highestWave:75,selectedUpdatedAt:50,selected:{finish:'apex_predator'}});assert.equal(store.getSelection().finish,'apex_predator');
  store.setSelection('finish','reactor');tick=200;store.mergeRemote({highestWave:75,selectedUpdatedAt:60,selected:{finish:'apex_predator'}});assert.equal(store.getSelection().finish,'reactor');
});
test('all catalog references have real SVG art, and equipment has explicit bounds',()=>{
  const assets=path.join(__dirname,'../game/neon-arena/assets/survival/ui');
  for(const entry of [...P.rewards,...P.achievements,...P.equipment]){const source=fs.readFileSync(path.join(assets,entry.art),'utf8');assert(source.includes('<svg'));assert(source.includes('<title'));assert(!source.includes('<image'));}
  for(const item of P.equipment){assert(item.cost>0);assert(item.cooldown>0);assert(item.duration>0);assert(item.maxActive<=2);assert(item.charges===1);}
});
