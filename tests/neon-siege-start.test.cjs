const test=require('node:test'),assert=require('node:assert/strict');
const {standardFixture,source,standardMaps}=require('../game/neon-arena/tests/map-fixtures.cjs');
const functionBetween=(start,end)=>source.slice(source.indexOf('function '+start+'('),source.indexOf('function '+end+'('));
for(const map of standardMaps)test(map+': Siege defenders and opening supplies stay clear, central and visible',()=>{
  const f=standardFixture(map);
  f.evaluate(`
    game.mode='siege';game.secondPlayer=true;
    const TAU=Math.PI*2,BUILD_MAX=10;
    function feed(){}function msg(){}function styleGun(){}function resetHumanJump(){}function pushOutXZ(){}
    function pick(a){return a[0]}function createUpgradeState(){return {}}function resetWeaponInventory(a,w){a.weapon=w}
    function makeHumanPlayer(){return {human:true,r:.5,maxHp:100,maxArmor:100}}
    function actorEyePosition(a){return a}function blocksAlongRay(){return blocksNear(0,0,200)}
    const hypot2=(x,z)=>Math.hypot(x,z);
    ${functionBetween('addPickup','addArmorPickup')}
    ${functionBetween('setActorAt','spawnSiegeOpeningWeapons')}
    ${functionBetween('spawnSiegeOpeningWeapons','spawnSiegeEnemies')}
    ${functionBetween('setupSiegeRound','finishSiegeRound')}
    ${functionBetween('rayBox','rayWorld')}
    ${functionBetween('rayWorld','damageStructure')}
    ${functionBetween('lineClear','visibleOnMinimap')}
    ${functionBetween('nearestWeaponPickup','takeWeaponPickup')}
    setupSiegeRound(false);
  `);
  const state=f.evaluate(`({players:[player,player2],supplies:pickups.filter(p=>p.siegeOpeningWeapon),game})`);
  assert.equal(state.supplies.length,3);assert.equal(state.game.siegePhase,'build');assert.equal(state.game.siegeBuildTime,60);
  for(const actor of state.players){assert.ok(f.safeSpawnCandidate({r:.5},actor.x,actor.z,actor.y-1.86),map+': standing defender spawn');assert.ok(Math.hypot(actor.x,actor.z)<23,'Fortification starts near the center');assert.equal(actor.y,1.86,'A gallery overhead does not teleport defenders to its roof');}
  for(const p of state.supplies){
    assert.ok(f.safeSpawnCandidate({r:.5},p.x,p.z,p.y-.7),'Supply occupies a clear standing site');assert.ok(Math.hypot(p.x,p.z)<23,'Supply stays central');
    const clear=f.evaluate(`(()=>{const p=pickups.find(p=>p.x===${p.x}&&p.z===${p.z});const a={x:p.x,z:p.z,y:p.y+.7,r:.5};return nearestWeaponPickup(a)===p&&lineClear(a.x,a.y,a.z,p.x,p.y+.15,p.z)})()`);
    assert.equal(clear,true,'A nearby player can see and interact with each supply');
  }
});
