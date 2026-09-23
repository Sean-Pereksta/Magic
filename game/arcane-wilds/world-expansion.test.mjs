import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';

function start(touch=false){const h=runtime(touch);h.run('startNewGame();game.player.invuln=1000;');return h;}
function equip(h,id){h.run(`{const item=AWCampaign.craftItem('${id}');if(item.slot==='weapon')game.player.weapon=item;else if(item.slot==='armor')game.player.armorGear=item;else game.player.trinkets=[item,null,null];recomputePlayerStats(true);}`);}

test('expanded geography is reciprocal, spatially correct and traversable without mounts',()=>{
  const h=start();try{
    assert.equal(h.run('AWCampaignData.continents.map(c=>c.nodes.length).join()'),'57,70,84');
    assert.equal(h.run(`Object.values(AWCampaignData.nodes).filter(n=>n.type!=='ruler').every(n=>Object.entries(n.exits).every(([d,id])=>{const t=AWCampaignData.nodes[id];return t.exits[AWCampaignData.opposite[d]]===n.id&&(d==='N'?t.x===n.x&&t.y<n.y:d==='S'?t.x===n.x&&t.y>n.y:d==='E'?t.y===n.y&&t.x>n.x:t.y===n.y&&t.x<n.x);}));`),true);
    assert.equal(h.run(`AWCampaignData.continents.every(c=>{const seen=new Set([c.start]),q=[c.start];while(q.length){const n=AWCampaignData.nodes[q.shift()];for(const [dir,id] of Object.entries(n.exits))if(!n.routeRequirements?.[dir]&&!seen.has(id)){seen.add(id);q.push(id);}}return c.nodes.every(id=>id===c.boss||seen.has(id));})`),true);
    assert.equal(h.run(`AWCampaignData.continents.every(c=>new Set(c.nodes.map(id=>AWCampaignData.nodes[id]).filter(n=>n.town).map(n=>n.region)).size>=4)`),true,'settlements span regions');
    assert.equal(h.run(`Object.values(AWCampaignData.towns).every(t=>t.stock.every(id=>AWCampaignData.items[id]))`),true);
  }finally{h.close();}
});

for(const touch of [false,true])test(`${touch?'touch':'desktop'} walking exits travel immediately and enter from the opposite side`,()=>{
  const h=start(touch);try{
    h.run(`game.player.x=ROOM_W+.1;game.player.y=ROOM_H/2;tryRoomExit();`);
    assert.equal(h.run('game.campaign.current'),'verdant-road');
    assert.ok(h.run('game.player.x')<1);
    assert.equal(h.w.document.getElementById('inventoryOverlay').classList.contains('hidden'),true);
    h.run(`game.enemies=[];markRoomCleared();transitionRoom(-1,0,'W');`);
    assert.equal(h.run('game.campaign.current'),'verdant-city');
    assert.ok(h.run('game.player.x')>17);
    h.run(`{const n=Object.values(AWCampaignData.nodes).find(n=>n.type!=='dungeon'&&n.type!=='ruler'&&!n.exits.N);AWCampaign.enter(n.id);game.roomData.cleared=true;game.player.y=-.1;transitionRoom(0,-1,'N');}`);
    assert.ok(h.run('game.player.y')>=.3);
    assert.equal(h.run('doorOpen("N")'),false);
    h.run('AWCampaignUI.open("Map")');
    assert.match(h.w.document.getElementById('inventoryOverlay').textContent,/YOU ARE HERE/);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('dungeons expose every surrounding world road through the matching outer chamber',()=>{
  const h=start();try{
    h.run(`AWCampaign.enter('verdant-dungeon');game.campaign.dungeonEntrances['verdant-dungeon']={node:AWCampaign.current().exits.W,side:'W'};`);
    const actual=h.run(`(()=>{const exits={};for(let room=0;room<5;room++){game.campaign.room=room;for(const [dir,e] of Object.entries(AWTravel.exits()))if(e.id!=='verdant-dungeon')exits[dir]=e.id;}return JSON.stringify(exits);})()`);
    assert.deepEqual(JSON.parse(actual),JSON.parse(h.run('JSON.stringify(AWCampaign.current().exits)')));
  }finally{h.close();}
});

test('all forty named items have obtainable sources and valid equipment, including capped bonus spell slots',()=>{
  const h=start();try{
    assert.equal(h.run('AWRegionalContent.gearIds.length'),40);
    assert.equal(h.run(`AWRegionalContent.gearIds.every(id=>{const d=AWCampaignData.items[id],i=AWCampaign.craftItem(id);return i.name===d.name&&i.special===d.special&&(AWCampaignData.towns[d.source]?.stock.includes(id)||AWCampaignData.nodes[d.source]?.rewardItems.includes(id));})`),true);
    equip(h,'grandArcanum');assert.equal(h.run('awSpellSlotLimit()'),5);
    equip(h,'emeraldCodex');assert.equal(h.run('awSpellSlotLimit()'),4);
    equip(h,'bloomheartCharm');h.run('game.player.hp=game.player.maxHp;game.player.shield=0;healPlayer(20)');assert.ok(Math.abs(h.run('game.player.shield')-20)<1e-9);
    equip(h,'crystalHeart');h.run(`game.player.shield=0;game.player.activeSpells[0]='ward';game.player.spellState.ward={cd:0};castSpell(0)`);assert.ok(h.run('game.player.shield')>45);
    equip(h,'eventideStaff');h.run(`game.hazards=[];SPELL_CASTS.voidrift('voidrift',SPELLS.voidrift,spellMods('voidrift'));`);assert.ok(h.run('game.hazards[0].r')>=2.125);
    h.run(`game.campaign.cleared.push('verdant-boss1');AWRegionalContent.grantItems();AWRegionalContent.grantItems()`);assert.equal(h.run(`game.campaign.rewards.filter(id=>id==='thornkeeperRobes').length`),1);
  }finally{h.close();}
});

test('three expedition mounts require their quests and open optional directional shortcuts',()=>{
  const h=start();try{
    for(const [c,mount,quest] of [['verdant','verdantElk','verdant-shrine'],['meridian','stormclaw','meridian-boss1'],['gloam','astralGryphon','gloam-boss4']]){
      h.run(`AWCampaign.enter('${c}-mount')`);assert.equal(h.run('AWCampaign.claimSite()'),false);
      h.run(`game.campaign.cleared.push('${quest}')`);assert.equal(h.run('AWCampaign.claimSite()'),true);assert.ok(h.run(`game.campaign.mounts.includes('${mount}')`));
      h.run(`{const n=Object.values(AWCampaignData.nodes).find(n=>n.continent==='${c}'&&n.routeRequirements);AWCampaign.enter(n.id);game.roomData.cleared=true;}`);
      assert.equal(h.run(`Object.keys(AWCampaign.current().routeRequirements).every(dir=>!!AWTravel.exits()[dir])`),true);
      h.run(`game.campaign.mounts=game.campaign.mounts.filter(m=>m!=='${mount}')`);
      assert.equal(h.run(`Object.keys(AWCampaign.current().routeRequirements).every(dir=>!AWTravel.exits()[dir])`),true);
    }
  }finally{h.close();}
});

test('all twenty regional enemies run their combat and rendering; tactical counters work',()=>{
  const h=start();try{
    assert.equal(h.run('AWRegionalEnemies.ids.length'),20);
    h.run(`for(const id of AWRegionalEnemies.ids){game.enemies=[];game.projectiles=[];const e=spawnEnemy(id,{x:12,y:7});game.player.x=8;game.player.y=7;for(let i=0;i<90;i++){updateEnemies(.1);updateEnemyEffects(.1);updateProjectiles(.1);if(i%15===0)render();}if(!Number.isFinite(e.x+e.y+e.hp))throw Error(id);}`);
    h.run(`game.enemies=[];const shaman=spawnEnemy('groveShaman',{x:10,y:7});shaman.attack=0;updateEnemyAI(shaman,{x:-1,y:0},4,1,.1);`);
    assert.ok(h.run('game.enemies.some(e=>e.type==="regionalHealPlant")'));
    h.run(`const ram=spawnEnemy('thunderRam',{x:.6,y:5});Object.assign(ram,{x:.6,y:5,state:'regionalRush',stateTime:.5,regionalDir:{x:-1,y:0}});updateEnemyAI(ram,{x:-1,y:0},5,2,.2);`);
    assert.ok(h.run('ram.stun')>0);
    h.run(`const leech=spawnEnemy('soulLeech',{x:8,y:7});leech.regionalAttached=true;game.player.dodgeTime=.2;updateEnemyAI(leech,{x:1,y:0},0,1,.1);`);assert.equal(h.run('leech.regionalAttached'),false);
    h.run(`game.enemies=[];const eater=spawnEnemy('gloamDevourer',{x:10,y:7});const food=spawnEnemy('sporeling',{x:10.2,y:7});eater.x=10;eater.y=7;food.x=10.2;food.y=7;eater.regionalHunger=0;updateEnemyAI(eater,{x:-1,y:0},5,.7,.1);`);assert.equal(h.run('food.dead'),true);assert.equal(h.run('eater.regionalMeals'),1);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('opened caches stay opened after the room cache is pruned and a save is restored',()=>{
  const h=start();try{
    h.run(`const cacheNode=Object.values(AWCampaignData.nodes).find(n=>{const p=AWCampaign.coordinates(n.id);return !n.town&&getRoomData(p.x,p.y).chests.length;});AWCampaign.enter(cacheNode.id);game.roomData.chests.forEach(c=>c.opened=true);AWTravel.pruneRooms();for(const n of Object.values(AWCampaignData.nodes).filter(n=>!n.town).slice(2,60))AWCampaign.enter(n.id);saveGame();loadGame();beginWorld();AWCampaign.enter(cacheNode.id);`);
    assert.ok(h.run('Object.keys(game.rooms).length')<=48);
    assert.ok(h.run('game.roomData.chests.length>0&&game.roomData.chests.every(c=>c.opened)'));
  }finally{h.close();}
});
