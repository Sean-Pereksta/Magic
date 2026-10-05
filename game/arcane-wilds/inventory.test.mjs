import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';
const start=touch=>{const h=runtime(touch);h.run('startNewGame()');return h;};
for(const touch of [false,true])test(`${touch?'touch':'desktop'} inventory slots inspect affixes, equip, swap and unequip without losing gear`,()=>{
 const h=start(touch);try{
  h.run(`var originalWeapon=game.player.weapon.id;var found=AWCampaign.craftItem('thornstaff');AWInventory.receive(found);AWCampaignUI.open('Inventory')`);
  assert.equal(h.w.document.querySelectorAll('.aw-inv-equipped .aw-inv-slot').length,5);
  assert.equal(h.w.document.querySelectorAll('#awInventoryGrid .aw-inv-slot').length,24);
  const tile=[...h.w.document.querySelectorAll('[data-item]')].find(b=>b.dataset.item===h.run('found.id'));assert.ok(tile);assert.ok(tile.querySelector('.aw-inv-suffix'));assert.ok(tile.style.getPropertyValue('--rarity'));tile.click();
  assert.match(h.w.document.querySelector('#awInventoryDetail').textContent,/Suffix/);
  h.w.document.querySelector('#awInventoryDetail button').click();assert.equal(h.run('game.player.weapon.id===found.id'),true);assert.ok(h.run('game.inventory.items.some(i=>i.id===originalWeapon)'));
  h.w.document.querySelector('#awInventoryDetail button').click();assert.equal(h.run('game.player.weapon.inventoryEmpty'),true);assert.equal(h.run('game.inventory.items.length'),3);
  h.run(`AWInventory.unequip('armor');AWCampaignUI.close();autoAttack(.5);render();saveGame();loadGame();beginWorld()`);h.step(3);
  assert.ok(h.run('Number.isFinite(game.player.hp+game.player.maxHp+game.player.speed+weaponDamage())'));assert.equal(h.run('game.player.weapon.inventoryEmpty&&game.player.armorGear.inventoryEmpty'),true);
  assert.equal(h.run('AWInventory.equip(originalWeapon)'),true);assert.equal(h.run('game.inventory.items.length'),3);assert.deepEqual(h.errors,[]);
 }finally{h.close();}
});
test('trinket moves cannot duplicate bonuses and removing slot gear retains learned spells',()=>{
 const h=start();try{
  h.run(`var charm=AWCampaign.craftItem('emeraldCodex');AWInventory.receive(charm);AWInventory.equip(charm.id,'trinket0');game.player.unlocked.push('meteor');AWCampaignUI.open('Spellbook');AWCampaignUI.assign('meteor',3)`);
  assert.equal(h.run('awSpellSlotLimit()'),4);assert.equal(h.run(`AWInventory.equip(charm.id,'trinket2')`),true);
  assert.equal(h.run('game.player.trinkets.filter(Boolean).length'),1);assert.equal(h.run('awSpellSlotLimit()'),4);
  h.run(`AWInventory.unequip('trinket2')`);assert.equal(h.run('awSpellSlotLimit()'),3);assert.equal(h.run(`game.player.unlocked.includes('meteor')`),true);
  h.run(`AWInventory.equip(charm.id,'trinket1');saveGame();loadGame();beginWorld()`);assert.equal(h.run('awSpellSlotLimit()'),4);assert.equal(h.run('game.inventory.items.filter(i=>i.id===charm.id).length'),1);
  assert.deepEqual(h.errors,[]);
 }finally{h.close();}
});
test('legacy equipped gear, pending loot, named rewards and Shadow overflow migrate once',()=>{
 const h=start();try{
  h.run(`game.player.weapon.forge=3;game.loot=AWCampaign.craftItem('thornstaff');game.campaign.rewards=['heartwood'];AWShadow.state().pendingLoot=[AWCampaign.craftItem('shadowsteel')];saveGame();var old=JSON.parse(localStorage.getItem(SAVE_KEY));delete old.inventory;localStorage.setItem(SAVE_KEY,JSON.stringify(old));loadGame();beginWorld()`);
  assert.equal(h.run('game.inventory.items.length'),5);assert.equal(h.run('game.loot'),null);assert.equal(h.run('game.campaign.rewards.length'),0);assert.equal(h.run('AWShadow.state().pendingLoot.length'),0);
  assert.equal(h.run('game.inventory.items.find(i=>i.id===game.player.weapon.id).forge'),3);
  h.run(`saveGame();loadGame();beginWorld();beginWorld();AWRegionalContent.grantItems()`);assert.equal(h.run('game.inventory.items.length'),5);
  assert.equal(h.run('JSON.parse(JSON.parse(awCloudCurrentBundle()).base).inventory.items.length'),5);
  h.run('startNewGame()');assert.equal(h.run('game.inventory.items.length'),2);assert.equal(h.run('game.inventory.claims.length'),0);
 }finally{h.close();}
});
test('region reward stays gated, goes directly into inventory, records origin and cannot be farmed',()=>{
 const h=start();try{
  h.run(`AWCampaign.enter('verdant-shrine');AWCampaignUI.open('Map');AWInventoryUI.region(document.getElementById('inventoryContent'),AWCampaign.current())`);
  const collect=[...h.w.document.querySelectorAll('button')].find(b=>b.textContent==='Collect area reward');assert.ok(collect);collect.click();
  assert.equal(h.run(`game.inventory.items.filter(i=>i.regionalId==='bloomheartCharm').length`),1);
  assert.equal(h.run(`AWInventory.pickups('verdant-shrine').some(i=>i.regionalId==='bloomheartCharm')`),true);
  assert.match(h.w.document.querySelector('.aw-region-pickups').textContent,/Picked up here/);
  const count=h.run('game.inventory.items.length');h.run(`AWCampaign.claimSite();loadRoom();saveGame();loadGame();beginWorld()`);assert.equal(h.run('game.inventory.items.length'),count);
  h.run(`AWCampaign.enter('verdant-boss1');AWCampaign.claimSite();AWRegionalContent.grantItems()`);assert.equal(h.run(`game.inventory.items.some(i=>i.regionalId==='thornkeeperRobes')`),false);
  assert.deepEqual(h.errors,[]);
 }finally{h.close();}
});
test('paged inventory retains large collections and shared supply quantities',()=>{
 const h=start();try{
  h.run(`for(let k=0;k<60;k++)dropGearNow('enemy');game.materials.timber=42;AWHome.state().seeds.lanternberry=7;AWCampaignUI.open('Inventory')`);
  assert.equal(h.run('game.inventory.items.length'),62);assert.equal(h.w.document.querySelectorAll('#awInventoryGrid [data-item]').length,24);
  assert.match(h.w.document.querySelector('#awInventoryPaging').textContent,/Page 1\/3/);
  const filter=h.w.document.querySelector('.aw-inv-filters select');filter.value='Materials';filter.dispatchEvent(new h.w.Event('change'));assert.match(h.w.document.querySelector('#awInventoryGrid').textContent,/42/);
  filter.value='Seeds';filter.dispatchEvent(new h.w.Event('change'));assert.match(h.w.document.querySelector('#awInventoryGrid').textContent,/7/);
  h.run('saveGame();loadGame();beginWorld()');assert.equal(h.run('game.inventory.items.length'),62);assert.deepEqual(h.errors,[]);
 }finally{h.close();}
});
