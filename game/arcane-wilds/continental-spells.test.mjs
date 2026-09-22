import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';
function start(touch=false){const h=runtime(touch);h.run(`startNewGame();game.enemies=[];game.player.x=8;game.player.y=7;spellAim=()=>({x:1,y:0});Math.random=()=>.5;`);return h;}
function cast(h,id){h.run(`game.player.activeSpells[0]='${id}';game.player.spellState['${id}']={cd:0};castSpell(0);`);}
function enemy(h,x=12,y=7){h.run(`{const e=spawnEnemy('wolf',{x:${x},y:${y}});e.x=${x};e.y=${y};e.hp=e.maxHp=10000;e.attack=0;}`);}
for(const touch of [false,true])test(`${touch?'touch':'desktop'}: all 30 additions cast, update and render with every mutation`,()=>{
  const h=start(touch);try{
    assert.equal(h.run('AWContinentalSpells.ids.length'),30);
    assert.equal(h.run('Object.keys(SPELLS).length'),84);
    for(const id of h.run('AWContinentalSpells.ids'))for(const mutated of [false,true]){
      h.run(`loadRoom();game.enemies=[];game.player.x=8;game.player.y=7;game.player.hp=70;game.player.invuln=100;running=true;paused=false;modalPause=false;roomTransition=false;game.player.upgrades['${id}']=${mutated?`UPGRADE_POOLS['${id}'].map(u=>u[0])`:'[]'};`);
      enemy(h);enemy(h,11,8);cast(h,id);
      h.run(`for(let i=0;i<150;i++){updateSpellEntities(.1);updateEnemies(.1);updateProjectiles(.1);updatePlayerTimers(.1);if(i%10===0)render();}`);
      assert.equal(h.run('Number.isFinite(game.player.x+game.player.y+game.player.hp)&&game.enemies.every(e=>Number.isFinite(e.hp+e.x+e.y))'),true,id);
      assert.deepEqual(h.errors,[],id);
    }
  }finally{h.close();}
});

test('Rewind recasts through the shared input queue, preserves cooldowns, expires and never revives',()=>{
  const h=start();try{
    h.run(`game.player.hp=80;game.player.shield=20;game.player.shieldTime=4;`);cast(h,'rewind');
    const cd=h.run('game.player.spellState.rewind.cd');
    h.run(`game.player.x=14;game.player.hp=35;game.player.shield=0;game.player.spellState.firebolt={cd:9};AWInput.press('spell1');`);
    assert.equal(h.run('game.player.x'),8);assert.equal(h.run('game.player.hp'),80);assert.equal(h.run('game.player.shield'),20);
    assert.equal(h.run('game.player.spellState.rewind.cd'),cd);assert.equal(h.run('game.player.spellState.firebolt.cd'),9);
    cast(h,'rewind');h.run('paused=true;updateSpellEntities(9)');assert.ok(h.run('AWContinentalSpells.state().anchor'));
    h.run('paused=false;updateSpellEntities(6)');assert.equal(h.run('AWContinentalSpells.state().anchor'),null);
    cast(h,'rewind');h.run('game.player.hp=0;castSpell(0)');assert.equal(h.run('game.player.hp'),0);
    h.run('loadRoom()');assert.equal(h.run('AWContinentalSpells.state().anchor'),null);
  }finally{h.close();}
});

test('barriers intercept swept shots before player collision and can be destroyed',()=>{
  const h=start();try{cast(h,'crystalBarricade');enemy(h,13,7);
    h.run(`enemyProjectile(game.enemies[0],{x:-1,y:0},100,'enemyOrb',{damage:10000});const q=game.projectiles.at(-1);q.x=13;q.y=7;updateProjectiles(.05);`);
    assert.equal(h.run('game.projectiles.filter(q=>q.owner==="enemy").length'),0);
    assert.ok(h.run('AWContinentalSpells.state().actors.some(a=>a.hp<=0)'));
    h.run('updateSpellEntities(.1)');assert.equal(h.run('AWContinentalSpells.state().actors.length'),2);
  }finally{h.close();}
});

test('Soul Chain shares actual damage once, mark buffs weapons and refunds cooldown on kill',()=>{
  const h=start();try{enemy(h,11,7);enemy(h,12,7);cast(h,'soulChain');
    h.run(`damageEnemy(game.enemies[0],100,'test');`);
    assert.equal(h.run('game.enemies[0].hp'),9900);assert.equal(h.run('game.enemies[1].hp'),9975);
    h.run('loadRoom();game.enemies=[]');enemy(h,11,7);cast(h,'predatorsMark');
    h.run(`damageEnemy(game.enemies[0],100,'weapon');`);assert.equal(h.run('game.enemies[0].hp'),9870);
    const cd=h.run('game.player.spellState.predatorsMark.cd');h.run(`damageEnemy(game.enemies[0],20000,'weapon')`);
    assert.equal(h.run('game.player.spellState.predatorsMark.cd'),cd*.5);
  }finally{h.close();}
});

test('Briar Cage catches crossings, poison stacks, and Emerald Rain sustains poison and heals summons',()=>{
  const h=start();try{enemy(h,12,7);cast(h,'briarCage');h.run('game.enemies[0].x=14.5;updateSpellEntities(.1)');
    assert.ok(h.run('game.enemies[0].hp<10000'));assert.ok(h.run('game.enemies[0].x<14'));
    h.run('loadRoom();game.enemies=[]');enemy(h,12,7);cast(h,'sporeburst');
    h.run('game.projectiles.at(-1).x=12;game.projectiles.at(-1).y=7;game.projectiles.at(-1).life=0;updateSpellEntities(.1);updateSpellEntities(.5);');
    assert.ok(h.run('AWContinentalSpells.state().poisons.get(game.enemies[0]).stacks>=2'));
    cast(h,'guardianTreant');h.run('AWContinentalSpells.state().actors[0].hp=10;game.player.x=10;game.player.hp=20;');cast(h,'emeraldRain');
    h.run('AWContinentalSpells.state().actors[0].x=12;updateSpellEntities(.5)');
    assert.ok(h.run('game.player.hp>20'));assert.ok(h.run('AWContinentalSpells.state().actors[0].hp>10'));
    assert.ok(h.run('AWContinentalSpells.state().poisons.get(game.enemies[0]).life>=2.5'));
  }finally{h.close();}
});

test('Umbral Passage disables attacks and casts, prevents damage, then bursts on exit',()=>{
  const h=start();try{enemy(h,9,7);cast(h,'umbralPassage');const hp=h.run('game.player.hp');
    h.run(`damagePlayer(30);autoAttack(1);game.player.activeSpells[1]='thunderstep';game.player.spellState.thunderstep={cd:0};castSpell(1);`);
    assert.equal(h.run('game.player.hp'),hp);assert.equal(h.run('game.projectiles.length'),0);assert.equal(h.run('game.player.spellState.thunderstep.cd'),0);
    h.run('updateSpellEntities(2.1)');assert.ok(h.run('game.enemies[0].hp<10000'));
  }finally{h.close();}
});

test('regional sources, retroactive rewards and save/load preserve ownership and mutations',()=>{
  const h=start();try{
    h.run('game.level=30');assert.equal(h.run('weightedSpellPool().includes("rewind")'),false);assert.equal(h.run('weightedSpellPool().includes("thunderstep")'),false);
    assert.equal(h.run('weightedSpellPool().includes("wildstep")'),true);
    assert.equal(h.run(`Object.values(AWCampaignData.towns).filter(t=>t.spells.some(id=>AWContinentalSpells.ids.includes(id))).every(t=>t.roles.some(r=>['Arcanist','Spell Scribe','Enchanter','Weaponsmith'].includes(r)))`),true);
    assert.equal(h.run(`AWContinentalSpells.ids.every(id=>{const s=SPELLS[id],D=AWCampaignData;return D.towns[s.source]?.spells.includes(id)||D.nodes[s.source]?.rewardSpells.includes(id)})`),true);
    h.run(`game.campaign.cleared.push('verdant-boss2');AWContinentalSpells.grantEarned();game.player.upgrades.guardianTreant=['guardianTreant_reach'];saveGame();loadGame();loadRoom();`);
    assert.equal(h.run('game.player.unlocked.includes("guardianTreant")'),true);
    assert.equal(h.run('hasUpgrade("guardianTreant","reach")'),true);
    assert.equal(h.run('weightedSpellPool().includes("guardianTreant")'),true);
  }finally{h.close();}
});

test('Wildstep second charge refills on one timer; mines pause and room travel clears combat objects',()=>{
  const h=start();try{
    h.run(`game.player.upgrades.wildstep=['wildstep_charge'];`);cast(h,'wildstep');
    assert.equal(h.run('game.player.spellState.wildstep.cd'),0);
    h.run('castSpell(0)');assert.ok(h.run('game.player.spellState.wildstep.cd>0'));
    const x=h.run('game.player.x');h.run('castSpell(0)');assert.equal(h.run('game.player.x'),x);
    h.run('updateSpellEntities(8)');assert.equal(h.run('game.player.spellState.wildstep.cd'),0);
    cast(h,'hourglassMine');enemy(h,15,7);
    const life=h.run('AWContinentalSpells.state().fields[0].life');h.run('paused=true;updateSpellEntities(3)');
    assert.equal(h.run('AWContinentalSpells.state().fields[0].life'),life);
    h.run('paused=false;loadRoom()');assert.equal(h.run('AWContinentalSpells.state().fields.length'),0);
  }finally{h.close();}
});

test('Prism Rebound changes direction at edges and grows damage; magnetic shots weaken only once',()=>{
  const h=start();try{cast(h,'prismRebound');
    h.run('game.projectiles[0].x=ROOM_W-.4;updateProjectiles(.05)');
    assert.ok(h.run('game.projectiles[0].vx<0'));assert.ok(h.run('game.projectiles[0].damage>SPELLS.prismRebound.damage'));
    h.run('game.projectiles=[]');cast(h,'magneticField');enemy(h,11,7);
    h.run(`enemyProjectile(game.enemies[0],{x:0,y:1},1,'enemyOrb',{damage:100});updateProjectiles(.05);updateProjectiles(.05);`);
    assert.equal(h.run('game.projectiles[0].damage'),65);
  }finally{h.close();}
});

test('treants attract and absorb attacks; sentinels prioritize telegraphed threats',()=>{
  const h=start();try{cast(h,'guardianTreant');enemy(h,10.2,7);
    const hp=h.run('AWContinentalSpells.state().actors[0].hp');
    h.run('updateEnemyAI(game.enemies[0],{x:-1,y:0},3,1,.1)');
    assert.ok(h.run('AWContinentalSpells.state().actors[0].hp')<hp);
    h.run('loadRoom();game.enemies=[]');enemy(h,10,7);enemy(h,13,7);
    h.run(`game.enemies[1].telegraph={kind:'charge'};`);cast(h,'astralSentinel');h.run('updateSpellEntities(.1)');
    assert.ok(h.run('game.enemies[1].hp<10000'));
  }finally{h.close();}
});

test('town purchase and claimed shrine rewards use the normal campaign flows',()=>{
  const h=start();try{
    h.run('game.gold=1000');assert.equal(h.run('AWCampaign.learn("wildstep")'),true);
    assert.equal(h.run('game.player.unlocked.includes("wildstep")'),true);
    h.run(`game.campaign.current='verdant-shrine';game.campaign.visited.push('verdant-shrine');game.room=AWCampaign.coordinates('verdant-shrine');loadRoom();`);
    assert.equal(h.run('AWCampaign.claimSite()'),true);assert.equal(h.run('game.player.unlocked.includes("pollenVeil")'),true);
    h.run('saveGame();loadGame();loadRoom()');assert.equal(h.run('game.player.unlocked.includes("pollenVeil")'),true);
  }finally{h.close();}
});
