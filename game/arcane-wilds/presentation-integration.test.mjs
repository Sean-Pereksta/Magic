import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';
for(const touch of [false,true])test(`${touch?'mobile':'desktop'} integrated journey, five-slot HUD, settings, loot and save compatibility`,()=>{
 const h=runtime(touch);try{
 h.run('startNewGame()');h.step(5);assert.equal(h.run('running&&!paused&&!modalPause'),true);
 assert.equal(h.w.document.querySelectorAll('#spells .spell-slot').length,3);
 h.run('game.player.armorGear.spellSlotBonus=2;recomputePlayerStats(false);game.player.activeSpells=["firebolt","frostnova","chain","meteor","starfall"];game.player.unlocked=[...game.player.activeSpells];updateHUD(true)');h.step(5);
 assert.equal(h.w.document.querySelectorAll('#spells .spell-slot').length,5);
 h.run('game.player.activeSpells[4]=null;renderSpellBar()');h.w.document.querySelectorAll('#spells .spell-slot')[4].click();assert.equal(h.w.document.querySelector('#replaceOverlay').classList.contains('hidden'),false);h.run('game.player.activeSpells[4]="starfall";closeOverlay("replaceOverlay");renderSpellBar()');
 const first=h.w.document.querySelector('#spells .spell-slot');h.step(5);assert.equal(first,h.w.document.querySelector('#spells .spell-slot'));
 h.run('window.AWInput.press("spell4")');assert.ok(h.run('game.player.spellState.meteor.cd')>0);h.step(4);
 h.run('window.AWModernUI.openSettings()');assert.equal(h.run('paused&&modalPause'),true);h.run('window.AWModernUI.closeSettings()');assert.equal(h.run('paused||modalPause'),false);
 h.run('game.loot=makeRandomGear({rarity:"Legendary",forceSlot:"weapon"});window.AWModernUI.offerLoot(game.loot)');h.step(4);assert.equal(h.run('modalPause'),false);h.run('window.AWModernUI.inspectLoot()');assert.ok(h.w.document.querySelector('.aw-comparison'));h.run('closeOverlay("lootOverlay")');
 h.run('saveGame()');const before=JSON.parse(h.run('localStorage.getItem(SAVE_KEY)'));h.run('loadGame()');assert.equal(h.run('game.player.activeSpells.length'),5);assert.deepEqual(JSON.parse(h.run('JSON.stringify(game.player.upgrades)')),before.player.upgrades);assert.equal(h.run('game.loot.name'),before.pendingLoot.name);
 // Old saves lack the new optional pending-loot field and still restore the journey.
 h.run('const old=JSON.parse(localStorage.getItem(SAVE_KEY));delete old.pendingLoot;localStorage.setItem(SAVE_KEY,JSON.stringify(old));loadGame()');assert.equal(h.run('game.loot'),null);assert.equal(h.run('game.player.activeSpells.length'),5);
 assert.deepEqual(h.errors,[]);assert.ok(h.draws()>1000);
 }finally{h.close();}
});
test('boss intro freezes simulation, releases it, and phase/death feedback stays cosmetic',()=>{
 const h=runtime();try{h.run('startNewGame();game.roomData.town=false;spawnBoss(BOSSES[0],game.roomData);window.AWPresentation.roomChanged()');h.step(1);assert.equal(h.run('window.AWPresentation.cinematic'),true);const x=h.run('game.enemies[0].x');h.step(30);assert.equal(h.run('game.enemies[0].x'),x);h.step(100);assert.equal(h.run('window.AWPresentation.cinematic'),false);h.run('game.enemies[0].hp=game.enemies[0].maxHp*.45');h.step(3);assert.match(h.w.document.querySelector('#awNotice').textContent,/PHASE II/);h.run('damageEnemy(game.enemies[0],1e9)');h.step(2);assert.ok(h.run('window.AWPresentation.fx.deaths.items.some(e=>e.life>0)'));assert.deepEqual(h.errors,[]);}finally{h.close();}
});
test('every spell cast and enemy family can render through the integrated registry',()=>{
 const h=runtime();try{h.run('startNewGame();game.player.maxHp=game.player.hp=1e9;game.level=25;game.pendingLevelUps=0');const ids=h.run('Object.keys(SPELLS)');for(const id of ids){h.run(`paused=false;modalPause=false;game.player.activeSpells[0]=${JSON.stringify(id)};game.player.spellState[${JSON.stringify(id)}]={cd:0};castSpell(0);render();`);h.step(1);}h.run('game.enemies=[];for(const id of Object.keys(ENEMY_TYPES))spawnEnemy(id,{x:6,y:7});render()');assert.deepEqual(h.errors,[]);}finally{h.close();}
});
test('quality degrades from real frame times without culling gameplay arrays',()=>{
 const h=runtime();try{h.run('startNewGame();window.AWPresentation.settings.quality="auto"');h.step(90,40);assert.equal(h.run('window.AWPresentation.quality'),'low');h.run('const referenceEnemies=game.enemies,referenceAttacks=game.projectiles,referenceHazards=game.hazards;window.AWPresentation.settings.particles="off";render()');assert.equal(h.run('game.enemies===referenceEnemies&&game.projectiles===referenceAttacks&&game.hazards===referenceHazards'),true);assert.deepEqual(h.errors,[]);}finally{h.close();}
});

