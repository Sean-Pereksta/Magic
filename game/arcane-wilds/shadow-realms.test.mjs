import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';

function start(touch=false){const h=runtime(touch);h.run('startNewGame();game.player.invuln=10000;');return h;}
function unlock(h){h.run(`game.campaign.unlocked=['verdant','meridian','gloam'];game.campaign.defeated=['verdant-ruler','meridian-ruler','gloam-ruler'];AWCampaign.enter('gloam-city');AWShadow.enter();`);}
function at(h,depth,lane=0){h.run(`AWShadow.windowAt(${depth});AWCampaign.enter('shadow:${depth}:${lane}');`);}
function clear(h){h.run(`game.enemies=[];if(game.roomData.shadowEncounter)Object.assign(game.roomData.shadowEncounter,{stage:3,elapsed:20,pending:0});markRoomCleared();`);}

test('only the third ruler opens the Shadow Realms; entry and checkpoints respect combat',()=>{
  const h=start();try{
    assert.equal(h.run('AWShadow.enter()'),false);
    h.run(`game.campaign.defeated=['verdant-ruler','meridian-ruler'];AWShadow.unlock()`);assert.equal(h.run('AWShadow.state().unlocked'),false);
    h.run(`game.campaign.unlocked=['verdant','meridian','gloam'];AWCampaign.enter('gloam-ruler');game.enemies=[];markRoomCleared();`);
    assert.equal(h.run('AWShadow.state().unlocked'),true);
    h.run(`AWCampaign.enter('gloam-city')`);assert.equal(h.run('AWShadow.enter()'),true);assert.equal(h.run('game.campaign.current'),'shadow:1:0');
    assert.equal(h.run('transitionRoom(0,-1,"N")'),true);assert.equal(h.run('game.campaign.current'),'shadow:2:0');assert.ok(h.run('game.player.y')>13);
    assert.equal(h.run('AWShadow.travel("shadow:3:0")'),false);assert.equal(h.run('AWShadow.resume(1)'),false);assert.equal(h.run('AWShadow.leave()'),false);
    clear(h);assert.equal(h.run('AWShadow.travel("shadow:3:0")'),true);
  }finally{h.close();}
});

test('generated regions have deterministic reciprocal roads, boss tiers and real risk branches',()=>{
  const h=start();try{unlock(h);
    const before=h.run(`JSON.stringify(AWShadow.definition(73,1))`);h.run('AWShadow.windowAt(1000);AWShadow.windowAt(73)');assert.equal(h.run('JSON.stringify(AWShadow.definition(73,1))'),before);
    assert.equal(h.run(`(()=>{for(let d=1;d<140;d++){AWShadow.windowAt(d);for(const id of AWShadow.continent.nodes){const n=AWCampaignData.nodes[id];for(const [dir,to] of Object.entries(n.exits)){const p=AWShadow.parse(to),t=AWShadow.definition(p.depth,p.lane);if(t.exits[AWCampaignData.opposite[dir]]!==id)return false;}}}return true;})()`),true);
    assert.equal(h.run(`[10,25,50,100].map(d=>AWShadow.definition(d).bossRank).join('|')`),'Elite Guardian|Shadow Boss|Greater Shadow Lord|Legendary Realm Boss');
    assert.ok(h.run(`Array.from({length:100},(_,i)=>AWShadow.definition(i+1,1)).some(n=>n.modifier==='Greed')`));
  }finally{h.close();}
});

for(const touch of [false,true])test(`${touch?'touch':'desktop'} Shadow enemies, bosses, variants, map and HUD run`,()=>{
  const h=start(touch);try{unlock(h);
    assert.equal(h.run('AWShadow.exclusiveIds.length'),12);
    for(const depth of [2,10,25,50,100,214]){
      at(h,depth);assert.ok(h.run('game.enemies.some(e=>e.shadow)'));
      h.run(`for(let i=0;i<80;i++){updateEnemies(.1);updateEnemyEffects(.1);updateProjectiles(.1);updateSpellEntities(.1);if(i%10===0)render();}updateHUD(true);`);h.step(30);
      assert.match(h.w.document.getElementById('awShadowCounter').textContent,new RegExp(`Depth ${depth}`));
    }
    for(const id of ['shadowFireball','eclipseLightning'])h.run(`game.player.activeSpells[0]='${id}';game.player.spellState['${id}']={cd:0};game.player.upgrades['${id}']=UPGRADE_POOLS['${id}'].map(u=>u[0]);castSpell(0);for(let i=0;i<40;i++){updateProjectiles(.1);updateSpellEntities(.1);}render();`);
    h.run('AWCampaignUI.open("Map")');assert.ok(h.w.document.querySelector('.aw-shadow-map'));h.run('AWCampaignUI.open("Glory")');assert.match(h.w.document.getElementById('inventoryOverlay').textContent,/Highest Shadow Depth/);
    assert.equal(h.run('game.enemies.every(e=>Number.isFinite(e.hp+e.x+e.y))'),true);assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('Glory and elite rewards cannot be farmed by revisiting, and waves must finish',()=>{
  const h=start();try{unlock(h);at(h,7);
    h.run('game.enemies=[];markRoomCleared()');assert.equal(h.run('game.roomData.cleared'),false);
    h.run('update(1.1)');assert.equal(h.run('game.roomData.shadowEncounter.stage'),2);assert.ok(h.run('game.enemies.length')>0);
    clear(h);const glory=h.run('AWShadow.state().glory');assert.ok(glory>0);
    at(h,7);h.run('markRoomCleared()');assert.equal(h.run('AWShadow.state().glory'),glory);assert.equal(h.run('game.enemies.length'),0);
    h.run('AWShadow.award(6000)');assert.ok(h.run(`game.player.unlocked.includes('shadowFireball')&&game.player.unlocked.includes('eclipseLightning')&&game.campaign.mounts.includes('shadowGryphon')`));
  }finally{h.close();}
});

test('save and cloud bundles retain depth, Glory, milestones, variants, gear and pending loot; death retains Glory',()=>{
  const h=start();try{unlock(h);at(h,25);clear(h);h.run(`AWShadow.activate();AWShadow.award(3000);AWShadow.loot(AWShadow.definition(24,1),true);saveGame();`);
    const glory=h.run('AWShadow.state().glory'),seed=h.run('AWShadow.state().seed'),loot=h.run('game.loot.regionalId');
    h.run('loadGame();beginWorld()');assert.equal(h.run('AWShadow.state().glory'),glory);assert.equal(h.run('AWShadow.state().seed'),seed);assert.equal(h.run('game.campaign.current'),'shadow:25:0');assert.equal(h.run('game.loot.regionalId'),loot);assert.equal(h.run('AWShadow.state().pendingLoot.length'),1);
    assert.equal(h.run('JSON.parse(JSON.parse(awCloudCurrentBundle()).base).campaign.shadow.glory'),glory);
    assert.equal(h.run('AWShadow.state().waystones.includes(25)'),true);assert.equal(h.run('AWShadow.state().milestones.includes(25)'),true);
    at(h,28);const banked=h.run('AWShadow.state().glory');h.run('game.player.hp=0;playerDeath()');assert.equal(h.run('game.campaign.current'),'shadow:25:0');assert.equal(h.run('AWShadow.state().glory'),banked);assert.equal(h.run('AWShadow.state().expedition'),0);
  }finally{h.close();}
});

test('deep expeditions keep descriptors, room snapshots, discovery and overflow loot bounded',()=>{
  const h=start();try{unlock(h);
    h.run(`for(let d=1;d<=12000;d+=13){AWShadow.windowAt(d);AWShadow.bit('shadow:'+d+':0','seen',true);AWShadow.bit('shadow:'+d+':0','cleared',true);}`);
    assert.ok(h.run('AWShadow.continent.nodes.length')<=39);assert.ok(h.run('Object.keys(AWCampaignData.nodes).length')<=249);assert.ok(h.run('Object.keys(AWShadow.state().ledger).length')<=32);
    assert.ok(h.run('JSON.stringify(AWShadow.state()).length')<10000);assert.equal(h.run('AWShadow.bit("shadow:2:0","claimed")'),true,'archived nodes cannot award again');
    at(h,12000);clear(h);h.run(`game.loot=AWCampaign.craftItem('shadowsteel');const goldBeforeOverflow=game.gold;for(let d=12001;d<=12016;d++)AWShadow.loot(AWShadow.definition(d,1),true);`);
    assert.equal(h.run('AWShadow.state().pendingLoot.length'),12);assert.ok(h.run('game.gold>goldBeforeOverflow'));
    for(const depth of [12001,12002,12003]){at(h,depth);h.run('saveGame();loadGame();beginWorld()');}
    assert.ok(h.run('Object.values(game.rooms).filter(r=>r.y===2000000).length')<=1);assert.ok(h.run('game.campaign.visited.every(id=>!id.startsWith("shadow:"))'));
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('treasure interaction uses Shadow rewards once, and escort objectives require accompanying the merchant',()=>{
  const h=start();try{unlock(h);
    h.run(`function findShadow(kind,event){for(let d=2;d<500;d++){AWShadow.windowAt(d);const n=AWShadow.continent.nodes.map(id=>AWCampaignData.nodes[id]).find(n=>n.type===kind&&(!event||n.event===event));if(n){AWCampaign.enter(n.id);return n;}}throw Error('Missing '+kind);}
      findShadow('treasure');game.player.x=9;game.player.y=6;interact();`);
    assert.equal(h.run('currentInteraction().type'),'shadowSite');
    const claim=[...h.w.document.querySelectorAll('#npcBody button')].find(b=>/Claim discovery/.test(b.textContent));assert.ok(claim);claim.click();
    assert.ok(h.run('AWShadow.bit(game.campaign.current,"claimed")'));assert.ok(h.run('game.loot.shadowDepth>0'));
    const glory=h.run('AWShadow.state().glory');assert.equal(h.run('AWShadow.claim()'),false);assert.equal(h.run('AWShadow.state().glory'),glory);
    h.run(`findShadow('event','escort');game.enemies=[];game.roomData.shadowEncounter.stage=3;markRoomCleared();`);assert.equal(h.run('game.roomData.cleared'),false);
    h.run(`paused=true;update(2)`);assert.equal(h.run('game.roomData.shadowEncounter.ally.progress'),0);
    h.run(`paused=false;modalPause=false;const escort=game.roomData.shadowEncounter;game.player.x=escort.ally.x;game.player.y=escort.ally.y;AWTravel.updateObjective(escort,14.1);markRoomCleared();`);assert.equal(h.run('game.roomData.cleared'),true);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
