import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';

for(const touch of [false,true])test(`${touch?'touch':'desktop'} portals use visited villages, survive saves and reject locked targets`,()=>{
  const h=runtime(touch);try{
    h.run('startNewGame()');h.step(3);
    assert.equal(h.run('game.interactables.filter(o=>o.type==="villagePortal").length'),1);
    h.run('AWVillagePortals.open()');assert.match(h.w.document.querySelector('#npcBody').textContent,/No other villages/);h.run('closeOverlay("npcPanel")');
    h.run(`const hometown=game.roomData;game.rooms['4,0']={...hometown,key:'4,0',x:4,y:0,name:'Second Village',seen:false};game.rooms['5,0']={...hometown,key:'5,0',x:5,y:0,name:'Locked Village',seen:false};game.rooms['6,0']={...hometown,key:'6,0',x:6,y:0,town:false,seen:true};`);
    assert.equal(h.run('AWVillagePortals.travel("4,0")'),false);
    h.run('game.room={x:4,y:0};loadRoom();saveGame();loadGame();loadRoom()');
    h.run('const portal=game.interactables.find(o=>o.type==="villagePortal");game.player.x=portal.x;game.player.y=portal.y;updateHUD(true)');
    if(touch){assert.match(h.w.document.querySelector('#mobileInteract').textContent,/TRAVEL/);h.w.document.querySelector('#mobileInteract').click();}
    else h.run('interact()');
    assert.equal(h.w.document.querySelectorAll('[data-village]').length,1);
    assert.equal(h.w.document.querySelector('[data-village]').dataset.village,'0,0');
    h.run('game.effects.push({kind:"meteor",life:1});game.projectiles.push({owner:"enemy"});game.summons.push({});');
    h.w.document.querySelector('[data-village]').click();
    assert.equal(h.run('game.room.x'),0);
    assert.equal(h.run('game.projectiles.length+game.effects.length+game.summons.length'),0);
    assert.equal(h.run('roomTransition||modalPause'),false);
    assert.equal(h.run('game.player.x===ROOM_W/2&&game.player.y>ROOM_H/2'),true);
    assert.equal(h.run('JSON.parse(localStorage.getItem(SAVE_KEY)).room.x'),0);
    assert.equal(h.run('AWVillagePortals.travel("5,0")||AWVillagePortals.travel("6,0")||AWVillagePortals.travel("missing")'),false);
    assert.equal(h.run('AWVillagePortals.travel("4,0")'),true);
    h.run('game.roomData.town=false');assert.equal(h.run('AWVillagePortals.travel("0,0")'),false);
    h.run('startNewGame()');assert.equal(h.run('AWVillagePortals.destinations().length'),0);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

for(const mode of ['active','bonus slot','replace','mastery','new spell','ordinary level'])test(`Seer and level-up flow: ${mode}`,()=>{
  const h=runtime();try{
    h.run(`startNewGame();game.player.activeSpells=['firebolt','frostnova','chain'];game.player.unlocked=['firebolt','frostnova','chain','gust'];game.player.upgrades.gust=['gust_wide'];game.pendingLevelUps=0;`);
    if(mode==='active')h.run('game.player.activeSpells[2]="gust"');
    if(mode==='bonus slot')h.run('game.player.armorGear.spellSlotBonus=2;recomputePlayerStats(false)');
    if(mode==='mastery')h.run('game.player.upgrades.gust=UPGRADE_POOLS.gust.map(u=>u[0])');
    if(mode==='new spell')h.run('game.player.unlocked=game.player.unlocked.filter(id=>id!=="gust");delete game.player.upgrades.gust');
    const free=mode!=='ordinary level';
    h.run(`game.pendingLevelUps=${free?0:1};openLevelChoice(${free});selectSpellChoice('gust')`);
    if(mode!=='active'&&mode!=='bonus slot'){
      assert.equal(h.w.document.querySelector('#replaceOverlay').classList.contains('hidden'),false);
      h.w.document.querySelector('#replaceCards').firstElementChild.click();
    }
    assert.equal(h.run('awIsSpellActive("gust")'),true);
    const upgrade=mode!=='new spell'&&mode!=='ordinary level';
    assert.equal(!h.w.document.querySelector('#upgradeOverlay').classList.contains('hidden'),upgrade);
    if(upgrade){
      const before=h.run('game.player.weapon.power');
      h.w.document.querySelector('#upgradeCards').firstElementChild.click();
      if(mode==='mastery')assert.equal(h.run('game.player.weapon.power'),before*1.12);
      else assert.equal(h.run('game.player.upgrades.gust.length'),2);
    }
    assert.equal(h.run('modalPause'),false);
    assert.equal(h.run('game.player.unlocked.includes("firebolt")'),true);
    if(mode!=='new spell')assert.equal(h.run('game.player.upgrades.gust.includes("gust_wide")'),true);
    h.run('saveGame();loadGame()');assert.equal(h.run('awIsSpellActive("gust")'),true);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('all 46 spells gain a unique named mutation with observable behavior beyond numeric potency',()=>{
  const h=runtime();try{
    h.run('startNewGame()');
    assert.equal(h.run('Object.keys(SPELLS).length'),46);
    assert.equal(h.run('AWSpellMutations.ids.length'),46);
    assert.equal(h.run('new Set(AWSpellMutations.ids.map(id=>UPGRADE_POOLS[id].at(-1)[1])).size'),46);
    const ids=h.run('Object.keys(SPELLS)');
    for(const id of ids){
      const snapshots=[];
      for(const enabled of [false,true]){
        h.run(`loadRoom();game.enemies=[];game.projectiles=[];game.effects=[];game.hazards=[];game.summons=[];game.player.x=8;game.player.y=7;game.player.hp=50;game.player.shield=0;game.player.dodgeCd=3;game.level=10;game.player.upgrades={};Math.random=()=>.5;spellAim=()=>({x:1,y:0});
          for(const pos of [{x:11,y:7},{x:11,y:8},{x:10,y:6},{x:12,y:7},{x:8,y:9.5},{x:8,y:4.5},{x:6,y:7},{x:8,y:7},{x:10,y:7}]){spawnEnemy('wolf',pos);const e=game.enemies.at(-1);e.x=pos.x;e.y=pos.y;e.hp=e.maxHp=100000;}
          enemyProjectile(game.enemies[0],{x:-1,y:0},2);game.projectiles[0].x=9;game.projectiles[0].y=7;
          game.player.upgrades[${JSON.stringify(id)}]=${enabled?`[${JSON.stringify(id+'_signature')}]`:'[]'};
          SPELL_CASTS[SPELLS[${JSON.stringify(id)}].cast](${JSON.stringify(id)},SPELLS[${JSON.stringify(id)}],{power:1,cdr:1});
          for(const q of [...game.projectiles])if(q.owner==='player'&&q.onHit)q.onHit(game.enemies[0],q);
          if(${JSON.stringify(id)}==='poison')game.enemies[0].hp=0;
          if(${JSON.stringify(id)}==='soulflame')nearestEnemy(game.player,9).hp=0;
          for(let i=0;i<24;i++)updateSpellEntities(.25);
        `);
        snapshots.push(h.run(`JSON.stringify({enemies:game.enemies.map(e=>[e.hp,e.x,e.y,e.stun]),hp:game.player.hp,shield:game.player.shield,dodge:game.player.dodgeCd,shots:game.projectiles.map(q=>[q.owner,q.kind,q.damage,q.vx,q.vy]),hazards:game.hazards.map(q=>[q.kind,q.damage]),jobs:AWSpellMutations.pending()})`));
        h.run('render()');
      }
      assert.notEqual(snapshots[0],snapshots[1],`${id} must produce a behavior change with identical potency`);
    }
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('mutation jobs pause with simulation, clear on travel and interception prevents collisions',()=>{
  const h=runtime();try{
    h.run(`startNewGame();game.player.upgrades.firebolt=['firebolt_signature'];SPELL_CASTS.firebolt('firebolt',SPELLS.firebolt,{power:1});modalPause=true;`);
    const n=h.run('AWSpellMutations.pending()');assert.ok(n>0);h.step(50);assert.equal(h.run('AWSpellMutations.pending()'),n);
    h.run('modalPause=false;loadRoom()');assert.equal(h.run('AWSpellMutations.pending()'),0);
    h.run(`game.player.upgrades.frostnova=['frostnova_signature'];game.player.shield=0;game.player.hp=50;enemyProjectile({x:game.player.x,y:game.player.y,damage:999,proj:'#fff'},{x:0,y:0});SPELL_CASTS.frostnova('frostnova',SPELLS.frostnova,{power:1});updateSpellEntities(.01);updateProjectiles(.01)`);
    assert.equal(h.run('game.player.hp'),50);
    assert.equal(h.run('game.projectiles.some(q=>q.owner==="enemy")'),false);
  }finally{h.close();}
});

test('enemy anatomy recognizes species, renders both facings, and preserves collision state',()=>{
  const h=runtime();try{
    h.run('startNewGame()');
    for(const [id,family] of Object.entries({wolf:'beast',beetle:'insect',serpent:'serpent',harpy:'winged',golem:'golem',mage:'humanoid',slime:'slime',voideye:'spirit'})){
      assert.equal(h.run(`AWEnemyAnatomy.family({...ENEMY_TYPES[${JSON.stringify(id)}],type:${JSON.stringify(id)}})`),family);
    }
    h.run(`game.enemies=[];for(const id of Object.keys(ENEMY_TYPES))spawnEnemy(id,{x:6,y:7});`);
    const before=h.run('JSON.stringify(game.enemies.map(e=>[e.x,e.y,e.r,e.hp]))');
    h.run(`for(const e of game.enemies){e.facing={x:1,y:0};drawEnemy(e);e.facing={x:-1,y:0};e.flash=.1;drawEnemy(e);}`);
    assert.equal(h.run('JSON.stringify(game.enemies.map(e=>[e.x,e.y,e.r,e.hp]))'),before);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
