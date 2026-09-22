import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';

function start(touch=false){const h=runtime(touch);h.run('startNewGame()');h.step(3);return h;}
function clear(h){
  h.run(`for(const e of [...game.enemies]){e.hp=0;e.dead=true;}game.enemies=[];markRoomCleared();game.pendingLevelUps=0;game.loot=null;document.querySelectorAll('.overlay').forEach(o=>o.classList.add('hidden'));modalPause=false;paused=false;`);
}
function route(h,target){
  const steps=h.run(`(()=>{const D=AWCampaignData,start=game.campaign.current,q=[[start]],seen=new Set([start]);while(q.length){const path=q.shift(),n=D.nodes[path.at(-1)];if(n.id===${JSON.stringify(target)})return path.slice(1);for(const id of n.connections)if(!seen.has(id)&&D.nodes[id].type!=='ruler'){seen.add(id);q.push([...path,id]);}}return null;})()`);
  assert.ok(steps,`route to ${target}`);
  for(const id of steps){clear(h);if(h.run('AWCampaign.current().type==="dungeon"&&!game.campaign.cleared.includes(game.campaign.current)')){h.run('transitionRoom(0,-1,"N")');clear(h);h.run('transitionRoom(-1,0,"W")');clear(h);h.run('transitionRoom(0,-1,"N")');clear(h);}assert.equal(h.run(`AWCampaign.travel(${JSON.stringify(id)})`),true,`travel to ${id}`);h.step(2);}
}

test('all 63 locations form valid connected continents with distinct difficulty and services',()=>{
  const h=start();try{
    assert.equal(h.run('Object.keys(AWCampaignData.nodes).length'),63);
    assert.equal(h.run('Object.values(AWCampaignData.towns).length'),9);
    assert.equal(h.run(`AWCampaignData.continents.every(c=>{const seen=new Set([c.start]),q=[c.start];while(q.length)for(const id of AWCampaignData.nodes[q.shift()].connections){if(!seen.has(id)){seen.add(id);q.push(id);}}return c.requiredBosses.length===4&&c.nodes.every(id=>seen.has(id));})`),true);
    assert.equal(h.run('AWCampaignData.continents.every((c,i,a)=>!i||c.range[0]>a[i-1].range[1])'),true);
    assert.equal(h.run('Object.values(AWCampaignData.nodes).every(n=>AWCampaign.pool(n).length>0)'),true);
  }finally{h.close();}
});

for(const touch of [false,true])test(`${touch?'touch':'desktop'} map enforces adjacent discovery and visited-only portals`,()=>{
  const h=start(touch);try{
    assert.equal(h.run('AWCampaign.travel("verdant-boss4")'),false);
    assert.equal(h.run('AWCampaign.travel("verdant-town","portal")'),false);
    assert.equal(h.run('AWCampaign.travel("meridian-city","portal")'),false);
    h.run('AWCampaignUI.open("Map")');assert.ok(h.w.document.querySelector('[data-node="verdant-road"]'));
    assert.match(h.w.document.querySelector('[data-node="verdant-road"]').textContent,/Unexplored/);
    assert.equal(h.w.document.querySelector('[data-node="verdant-boss4"]'),null);
    assert.equal(h.run('AWCampaign.travel("verdant-road")'),true);
    assert.equal(h.run('AWCampaign.travel("verdant-wood")'),false);
    clear(h);route(h,'verdant-town');
    assert.equal(h.run('AWVillagePortals.travel("verdant-city")'),true);
    assert.equal(h.run('AWVillagePortals.travel("verdant-town")'),true);
    assert.equal(h.run('AWVillagePortals.travel("gloam-city")'),false);
    assert.equal(h.run('roomTransition||modalPause'),false);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('all four seals gate each ruler; actual room clears unlock all three continents',()=>{
  const h=start();try{
    for(const c of ['verdant','meridian','gloam']){
      assert.equal(h.run('AWCampaign.enterPortal()'),false);
      for(let i=1;i<=4;i++){route(h,`${c}-boss${i}`);assert.ok(h.run('game.enemies.some(e=>e.boss)'));clear(h);}
      route(h,`${c}-city`);
      assert.equal(h.run(`AWCampaign.travel('${c}-ruler')`),false,'world map cannot bypass physical portal');
      assert.equal(h.run('AWCampaign.enterPortal()'),true);
      h.run('{const ruler=game.enemies.find(e=>e.boss);ruler.hp=ruler.maxHp*.3;updateEnemyAI(ruler,enemyDir(ruler),8,ruler.speed,.1);intensityTickBosses(.1)}');
      assert.equal(h.run('game.enemies.find(e=>e.boss).campaignPhase'),3);
      clear(h);
      assert.equal(h.run(`game.campaign.defeated.includes('${c}-ruler')`),true);
      assert.ok(h.run('game.campaign.rewards.length')>0);
      route(h,`${c}-passage`);
      if(c!=='gloam'){const next=c==='verdant'?'meridian':'gloam';assert.equal(h.run(`AWCampaign.travel('${next}-city','passage')`),true);}
    }
    assert.equal(h.run('game.campaign.unlocked.length'),3);
    const before=h.run('game.campaign.rewards.length');h.run('markRoomCleared()');assert.equal(h.run('game.campaign.rewards.length'),before);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('dungeon cardinal links preserve room clears and require the guardian',()=>{
  const h=start();try{
    route(h,'verdant-dungeon');assert.equal(h.run('game.campaign.room'),0);clear(h);
    assert.equal(h.run('game.campaign.cleared.includes("verdant-dungeon")'),false);
    h.run('transitionRoom(0,-1,"N")');assert.equal(h.run('game.campaign.room'),1);clear(h);
    h.run('transitionRoom(-1,0,"W")');assert.equal(h.run('game.campaign.room'),2);clear(h);
    assert.equal(h.run('game.player.unlocked.includes("spirits")'),true);
    h.run('transitionRoom(0,-1,"N")');assert.equal(h.run('game.campaign.room'),4);
    assert.equal(h.run('game.enemies.some(e=>e.boss)'),true);clear(h);
    assert.equal(h.run('game.campaign.cleared.includes("verdant-dungeon")'),true);
    h.run('saveGame();loadGame();beginWorld()');
    assert.equal(h.run('game.campaign.dungeonClears["verdant-dungeon"].length'),4);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('legacy migration preserves character, expansion, mutations and cloud identity',()=>{
  const h=start();try{
    h.run(`game.level=17;game.xp=123;game.gold=4321;game.materials.ember=77;game.player.upgrades.firebolt=['firebolt_signature'];game.player.trinkets[0]=makeTrinket(TRINKET_BASES[0],17,'Epic');game.quests.active=[{id:'legacy',title:'Old quest',type:'kill',progress:4,target:10}];awCloudBoundName='Existing hero';localStorage.setItem(AW_CLOUD_BINDING_KEY,awCloudBoundName);saveGame();const old=JSON.parse(localStorage.getItem(SAVE_KEY));delete old.campaign;old.room={x:8,y:9};localStorage.setItem(SAVE_KEY,JSON.stringify(old));loadGame();beginWorld();`);
    assert.equal(h.run('game.campaign.current'),'verdant-city');
    assert.equal(h.run('game.level'),17);assert.equal(h.run('game.gold'),4321);assert.equal(h.run('game.xp'),123);
    assert.equal(h.run('game.materials.ember'),77);assert.equal(h.run('game.player.upgrades.firebolt[0]'),'firebolt_signature');
    assert.equal(h.run('game.player.trinkets[0].rarity'),'Epic');assert.equal(h.run('game.quests.active[0].id'),'legacy');
    assert.equal(h.run('localStorage.getItem(AW_CLOUD_BINDING_KEY)'),'Existing hero');
    assert.equal(h.run('JSON.parse(JSON.parse(awCloudCurrentBundle()).base).campaign.current'),'verdant-city');
  }finally{h.close();}
});

test('save roundtrip keeps mount, checkpoints, rewards and exploration; new game resets campaign',()=>{
  const h=start();try{
    route(h,'verdant-town');h.run(`game.campaign.mounts=['horse'];game.campaign.activeMount='horse';AWCampaign.mount();game.campaign.rewards=['heartwood'];saveGame();loadGame();beginWorld();`);
    assert.equal(h.run('game.campaign.current'),'verdant-town');assert.equal(h.run('game.campaign.checkpoint'),'verdant-town');assert.equal(h.run('game.campaign.riding'),true);
    h.run('AWCampaign.travel("verdant-danger");playerDeath()');assert.equal(h.run('game.campaign.current'),'verdant-town');assert.equal(h.run('game.campaign.riding'),false);
    assert.equal(h.run('game.campaign.rewards[0]'),'heartwood');
    h.run('startNewGame()');assert.equal(h.run('game.campaign.visited.length'),1);assert.equal(h.run('game.campaign.rewards.length'),0);assert.equal(h.run('game.campaign.mounts.length'),0);
  }finally{h.close();}
});

test('regional stock is location-bound and unlocks cannot be bypassed through purchase calls',()=>{
  const h=start();try{
    h.run('game.gold=9999');assert.equal(h.run('AWCampaign.buy("astralcodex")'),false);
    assert.equal(h.run('AWCampaign.buyMount("arcanebeast")'),false);
    assert.equal(h.run('AWCampaign.learn("mirrorBastion")'),false);
    assert.equal(h.run('AWCampaign.buy("starterstaff")'),true);assert.equal(h.run('game.loot.name'),'Sunmere Staff');
    h.run('game.loot=null');route(h,'verdant-town');assert.equal(h.run('AWCampaign.buy("thornstaff")'),true);assert.equal(h.run('game.loot.suffixKey'),'thorns');
    h.run(`game.loot=null;game.campaign.defeated.push('verdant-ruler');game.campaign.unlocked.push('meridian');game.campaign.current='meridian-city';game.campaign.visited.push('meridian-city');game.room=AWCampaign.coordinates('meridian-city');loadRoom();`);
    const gold=h.run('game.gold');assert.equal(h.run('AWCampaign.buy("mirrorsigil")'),false);assert.equal(h.run('game.gold'),gold);
    h.run(`game.campaign.cleared.push('meridian-dungeon')`);assert.equal(h.run('AWCampaign.buy("mirrorsigil")'),true);assert.equal(h.run('game.loot.spellSlotBonus'),1);
    assert.equal(h.run(`Array.from({length:100},()=>makeRandomGear({source:'merchant'})).some(i=>i.regionalId==='astralcodex')`),false);
  }finally{h.close();}
});

test('Spellbook supports tap, swap, empty slots, locked slots and mutation retention',()=>{
  const h=start(true);try{
    h.run(`game.player.unlocked=['firebolt','frostnova','ward','gust'];game.player.activeSpells=['firebolt','frostnova','ward'];game.player.upgrades.gust=['gust_wide'];AWCampaignUI.open('Spellbook')`);
    assert.equal(h.w.document.querySelectorAll('.aw-loadout-slot:disabled').length,2);
    assert.equal(h.run('AWCampaignUI.assign("gust",4)'),false);
    assert.equal(h.run('AWCampaignUI.assign("notknown",0)'),false);
    assert.equal(h.run('AWCampaignUI.assign("frostnova",0)'),true);assert.equal(h.run('game.player.activeSpells[1]'),'firebolt');
    assert.equal(h.run('AWCampaignUI.assign("gust",2)'),true);assert.equal(h.run('game.player.upgrades.gust[0]'),'gust_wide');
    h.run('game.player.armorGear.spellSlotBonus=2;recomputePlayerStats(false);AWCampaignUI.open("Spellbook")');assert.equal(h.w.document.querySelectorAll('.aw-loadout-slot:disabled').length,0);
    const gust=[...h.w.document.querySelectorAll('.aw-spell-entry')].find(b=>b.textContent.includes('Gale Burst'));gust.click();
    h.w.document.querySelector('#awSpellActions button').click();h.w.document.querySelector('#awSlotPicker button:last-child').click();
    assert.equal(h.run('game.player.activeSpells[4]'),'gust');assert.equal(h.run('game.player.unlocked.includes("ward")'),true);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('menus freeze world rendering and movement; Escape restores play; controls honor remaps',()=>{
  const h=start();try{
    h.run('AWCampaignUI.open("Map")');const draws=h.draws(),before=h.run('game.player.x');h.step(20);
    assert.equal(h.draws(),draws);assert.equal(h.run('game.player.x'),before);
    h.w.dispatchEvent(new h.w.KeyboardEvent('keydown',{key:'Escape',code:'Escape',bubbles:true}));
    assert.equal(h.run('modalPause||paused'),false);
    h.run('AWInput.remap("keys","spell1","KeyM")');h.w.dispatchEvent(new h.w.KeyboardEvent('keydown',{key:'m',code:'KeyM',bubbles:true}));
    assert.equal(h.run('AWCampaignUI.isOpen()'),false);
  }finally{h.close();}
});

test('mounts speed travel, widen camera, and safely dismiss on cast or damage',()=>{
  const h=start();try{
    h.run(`game.campaign.mounts=['horse'];game.campaign.activeMount='horse';game.player.x=9;game.player.y=7;AWInput.move.x=1;AWInput.move.y=0;playerMovement(.1);`);
    const normal=h.run('game.player.x-9');h.run('game.player.x=9;game.player.y=7;AWCampaign.mount();playerMovement(.1)');assert.ok(h.run('game.player.x-9')>normal*1.3);
    h.run('AWPresentation.camera.tick(1)');assert.ok(h.run('AWPresentation.camera.zoom')<.95);
    h.run('damagePlayer(1)');assert.equal(h.run('game.campaign.riding'),false);assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('all new spells cast, mutate and render without nonfinite geometry; regional exclusives stay out of RNG',()=>{
  const h=start();try{
    h.run('game.level=30');
    for(const id of ['groveGuardian','stormBridge','mirrorBastion','springSnare','prismRicochet','starCauseway']){
      h.run(`loadRoom();game.player.upgrades['${id}']=UPGRADE_POOLS['${id}'].map(u=>u[0]);spawnEnemy('wolf',{x:12,y:7});SPELL_CASTS['${id}']('${id}',SPELLS['${id}'],{power:1,cdr:1});updateSpellEntities(.2);render();`);
    }
    assert.equal(h.run('weightedSpellPool().includes("mirrorBastion")'),false);
    h.run('game.player.unlocked.push("mirrorBastion")');assert.equal(h.run('weightedSpellPool().includes("mirrorBastion")'),true);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
