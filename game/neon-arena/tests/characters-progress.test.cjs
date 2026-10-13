/* Run: node --test game/neon-arena/tests/characters-progress.test.cjs */
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const P=require('../neon-survival-progress.js');
const memory=()=>{const values=new Map();return {getItem:key=>values.get(key)||null,setItem:(key,value)=>values.set(key,value)}};
const create=(options={})=>{let tick=100;return P.createStore({storage:memory(),now:()=>++tick,...options})};

test('two starter characters and six distinct unlockable model paths',()=>{
  const store=create(),catalog=store.getCharacterCatalog();
  assert.equal(catalog.length,8);
  assert.equal(new Set(catalog.map(c=>c.modelPath)).size,8);
  assert.deepEqual(catalog.filter(c=>c.unlocked).map(c=>c.id),['player_one','player_two']);
  assert.equal(store.getEquippedCharacterModelPath(),'characters/player_one.glb');
  assert.equal(store.equipCharacter('character_overlord'),false);
  assert.equal(store.equipCharacter('../bad.glb'),false);
  assert.equal(store.equipCharacter('player_two'),true);
  assert.equal(store.getEquippedCharacterModelPath(),'characters/player_two.glb');
  catalog[0].name='Changed';
  assert.equal(store.getCharacterCatalog()[0].name,'Breacher');
});

test('wave milestones unlock full models exactly once and retain all existing rewards',()=>{
  const store=create();store.beginRun('breach_ring');
  assert.deepEqual(store.clearWave(4).characters,[]);
  for(const [wave,id] of [[5,'character_bulwark'],[10,'character_ronin'],[15,'character_warden'],[25,'character_revenant'],[40,'character_praetorian'],[50,'character_overlord']]){
    const earned=store.clearWave(wave);
    assert.ok(earned.characters.includes(id),`${id} unlocks at wave ${wave}`);
    assert.equal(store.isCharacterUnlocked(id),true);
    assert.deepEqual(store.clearWave(wave).characters,[]);
    assert.equal(store.equipCharacter(id),true);
    assert.equal(store.getEquippedCharacterModelPath(),`characters/${id}.glb`);
  }
  store.clearWave(100);
  const state=store.getState(),summary=store.finishRun();
  assert.equal(state.unlockedMaps.length,P.maps.length);
  assert.equal(state.unlockedRewards.length,P.rewards.length);
  assert.equal(state.unlockedCharacters.length,P.characters.length);
  assert.equal(summary.earned.characters.length,6);
  assert.equal(store.setSelection('armor','hivebreaker'),true);
  assert.equal(store.setSelection('finish','apex_predator'),true);
  assert.equal(store.isEquipmentUnlocked('shieldBeacon'),true);
  assert.equal(store.getSelection().character,'character_overlord');
});

test('earned achievements offer early character unlock routes',()=>{
  const store=create();store.beginRun('breach_ring');
  store.recordEvent('flyingKills',50);
  store.recordEvent('bosses',10);
  const earned=store.checkpoint();
  assert.ok(earned.achievements.includes('sky_hunter'));
  assert.ok(earned.achievements.includes('boss_slayer'));
  assert.ok(earned.characters.includes('character_revenant'));
  assert.ok(earned.characters.includes('character_praetorian'));
  assert.equal(store.getState().highestWave,0);
  assert.equal(store.isCharacterUnlocked('character_warden'),false);
  store.recordEvent('structuresBuilt',8);store.recordEvent('defenseKills',30);
  const fortress=store.clearWave(10);
  assert.ok(fortress.achievements.includes('fortress_defender'));
  assert.ok(fortress.characters.includes('character_warden'));
  assert.equal(store.isCharacterUnlocked('character_overlord'),false);
  assert.deepEqual(store.checkpoint().characters,[]);
});

test('legacy saves recover earned character unlocks without losing selected rewards',()=>{
  const storage=memory();
  storage.setItem('neon-breach.survival.v1',JSON.stringify({highestWaveCleared:25,selected:{finish:'reactor',armor:'frostbite'},achievements:['boss_slayer'],stats:{kills:321}}));
  const store=create({storage}),state=store.getState();
  assert.equal(state.stats.kills,321);
  assert.equal(state.selected.finish,'reactor');
  assert.equal(state.selected.armor,'frostbite');
  assert.equal(state.selected.character,'player_one');
  for(const id of ['character_bulwark','character_ronin','character_warden','character_revenant','character_praetorian'])assert.equal(store.isCharacterUnlocked(id),true);
  assert.equal(store.isCharacterUnlocked('character_overlord'),false);
});

test('character equipment persists and falls back to backup after a corrupt primary save',()=>{
  const storage=memory(),store=create({storage});store.beginRun('breach_ring');store.clearWave(5);
  assert.equal(store.equipCharacter('character_bulwark'),true);
  assert.equal(create({storage}).getEquippedCharacter().id,'character_bulwark');
  storage.setItem(P.KEY,'{broken');
  const restored=create({storage});
  assert.equal(restored.getEquippedCharacterModelPath(),'characters/character_bulwark.glb');
  assert.equal(restored.getState().highestWave,5);
});

test('unrecognized characters and locked selections are sanitized',()=>{
  const state=P.sanitize({selected:{character:'character_overlord'},unlockedCharacters:['unknown','player_two','player_two'],characterUpdatedAt:Infinity});
  assert.equal(state.selected.character,'player_one');
  assert.deepEqual(state.unlockedCharacters,['player_two','player_one']);
  assert.equal(state.characterUpdatedAt,0);
  assert.equal(P.sanitize(null).selected.character,'player_one');
});

test('profile merging preserves characters across older cosmetic-only client saves',()=>{
  const store=create();store.beginRun('breach_ring');store.clearWave(15);store.equipCharacter('character_warden');
  const characterTime=store.getState().characterUpdatedAt;
  store.mergeRemote({highestWave:25,selectedUpdatedAt:10000,selected:{armor:'frostbite',finish:'reactor'}});
  assert.equal(store.getEquippedCharacter().id,'character_warden');
  assert.equal(store.getState().characterUpdatedAt,characterTime);
  assert.equal(store.getSelection().armor,'frostbite');
  assert.equal(store.getSelection().finish,'reactor');
  store.mergeRemote({highestWave:50,characterUpdatedAt:20000,selected:{character:'character_overlord'}});
  assert.equal(store.getEquippedCharacter().id,'character_overlord');
  assert.equal(store.getSelection().armor,'frostbite');
  assert.equal(store.isCharacterUnlocked('character_revenant'),true);
  store.mergeRemote({highestWave:5,characterUpdatedAt:10,selected:{character:'character_bulwark'}});
  assert.equal(store.getEquippedCharacter().id,'character_overlord');
});

test('selection events supply the preview and renderer with the actual model path',()=>{
  const events=[],context=vm.createContext({module:{exports:{}},console,dispatchEvent:event=>events.push(event),CustomEvent:class{constructor(type,options){this.type=type;this.detail=options.detail;}}});
  vm.runInContext(fs.readFileSync(require.resolve('../neon-survival-progress.js'),'utf8'),context);
  const store=context.module.exports.createStore({storage:null,now:()=>100});
  store.equipCharacter('player_two');store.equipCharacter('player_two');store.equipCharacter('character_overlord');
  assert.equal(events.length,1);
  assert.equal(events[0].type,'neon-character-changed');
  assert.equal(events[0].detail.id,'player_two');
  assert.equal(events[0].detail.modelPath,'characters/player_two.glb');
  store.mergeRemote({highestWave:50,characterUpdatedAt:200,selected:{character:'character_overlord'}});
  assert.equal(events.length,2);
  assert.equal(events[1].detail.modelPath,'characters/character_overlord.glb');
});

test('unavailable browser storage leaves unlocked character choices usable in memory',()=>{
  const store=create({storage:{getItem(){throw Error('Denied')},setItem(){throw Error('Denied')}}});
  assert.equal(store.equipCharacter('player_two'),true);
  assert.equal(store.persistenceAvailable,false);
  store.beginRun('breach_ring');store.clearWave(5);
  assert.equal(store.equipCharacter('character_bulwark'),true);
  assert.equal(store.getEquippedCharacter().id,'character_bulwark');
});
