import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';
function setup(){
  const h=runtime();h.run('startNewGame();game.enemies=[];game.effects=[];game.projectiles=[];game.summons=[];');
  return h;
}
for(const [element,tag,multiplier] of [
  ['fire','ICE',1.35],['fire','PLANT',1.35],['frost','FIRE',1.3],['lightning','ARMORED',1.3],
  ['lightning','WET',1.4],['physical','CONSTRUCT',1.3],['solar','UNDEAD',1.4],
  ['solar','SHADOW',1.4],['shadow','ARCANE',1.3],['poison','BEAST',1.25],
  ['poison','HUMANOID',1.25],['poison','UNDEAD',.8],['poison','CONSTRUCT',.8],
  ['fire','FIRE',.85],['frost','ICE',.85],['lightning','LIGHTNING',.85],
  ['fire','BEAST',1]
])test(element+' vs '+tag+' applies one bounded affinity multiplier',()=>{
  const h=setup();try{
    h.run("var target={type:'unknown',combatTags:['"+tag+"'],hp:10000,maxHp:10000,shield:0,x:5,y:7,r:.3,ai:'melee',stun:0,slow:0,flash:0,dead:false};game.enemies=[target];");
    assert.equal(h.run("AWCombatAffinity.resolve('"+element+"',target).multiplier"),multiplier);
    h.run("damageEnemy(target,100,'"+element+"',true);");
    assert.ok(Math.abs(h.run('10000-target.hp')-100*multiplier)<1e-7);
  }finally{h.close();}
});
test('multiple enemy tags choose one weakness instead of stacking bonuses',()=>{
  const h=setup();try{
    h.run("var e={type:'test',combatTags:['ICE','PLANT']};");
    assert.equal(h.run("AWCombatAffinity.resolve('fire',e).multiplier"),1.35);
    h.run("e.combatTags=['WET','ARMORED'];e._combatTags=null;");
    assert.equal(h.run("AWCombatAffinity.resolve('lightning',e).multiplier"),1.4);
  }finally{h.close();}
});
test('dynamic wet status and post-spawn shadow traits participate in affinity lookup',()=>{
  const h=setup();try{
    h.run("var e=spawnEnemy('bandit',{x:5,y:7});e.wet=3;");
    assert.equal(h.run("AWCombatAffinity.resolve('lightning',e).multiplier"),1.4);
    h.run("e.wet=0;e.shadow=true;e.shadowTraits=['Frozen','Armored'];");
    assert.equal(h.run("AWCombatAffinity.resolve('fire',e).multiplier"),1.35);
    assert.equal(h.run("AWCombatAffinity.resolve('solar',e).multiplier"),1.4);
  }finally{h.close();}
});
test('real firebolt projectiles carry fire identity through the existing hit pipeline',()=>{
  const h=setup();try{
    h.run("var e=spawnEnemy('frostwitch',{x:10,y:7});e.hp=e.maxHp=10000;e.shield=0;SPELL_CASTS.firebolt('firebolt',SPELLS.firebolt,{power:1});var q=game.projectiles[0];");
    assert.equal(h.run('q.combatElement'),'fire');
    h.run("q.x=e.x;q.y=e.y;q.vx=q.vy=0;q.splash=0;updateProjectiles(.001);");
    assert.ok(Math.abs(h.run('10000-e.hp')-32*1.35)<1e-7);
  }finally{h.close();}
});
test('upgraded storm spear preserves lightning while retaining the iceExecute upgrade tag',()=>{
  const h=setup();try{
    h.run("game.player.upgrades.stormSpear=['execute'];SPELL_CASTS[SPELLS.stormSpear.cast]('stormSpear',SPELLS.stormSpear,{power:1});");
    assert.equal(h.run('game.projectiles[0].tag'),'iceExecute');
    assert.equal(h.run('game.projectiles[0].combatElement'),'lightning');
  }finally{h.close();}
});
test('solar beam purifies undead, ground fields retain their source school, and ordinary spears remain physical',()=>{
  const h=setup();try{
    h.run("var e=spawnEnemy('skeleton',{x:10,y:7});e.hp=e.maxHp=10000;game.player.facing={x:1,y:0};SPELL_CASTS.solarBeam('solarLance',SPELLS.solarLance,{power:1});");
    const damage=h.run('SPELLS.solarLance.damage');
    assert.ok(Math.abs(h.run('10000-e.hp')-damage*1.4)<1e-7);
    h.run("SPELL_CASTS.starCauseway('starCauseway',SPELLS.starCauseway,{power:1});");
    assert.equal(h.run('game.hazards.at(-1).combatElement'),'solar');
    h.run("var q=magicProjectile({kind:'sunLance',trail:'weapon'});");
    assert.equal(h.run('q.combatElement'),'physical');
  }finally{h.close();}
});
test('frost shortens fire effectiveness, fire strips frost shields, and arcane breaks wards',()=>{
  const h=setup();try{
    h.run("var e=spawnEnemy('bomber',{x:5,y:7});e.hp=e.maxHp=10000;damageEnemy(e,20,'frost');");
    assert.ok(h.run('e.combatSuppressed>0'));
    h.run("e=spawnEnemy('awx_frostguard',{x:6,y:7});e.hp=e.maxHp=10000;e.shield=100;damageEnemy(e,20,'fire',true);");
    assert.ok(Math.abs(h.run('e.shield')-68)<1e-7);
    h.run("e=spawnEnemy('mage',{x:7,y:7});e.hp=e.maxHp=10000;e.shield=100;damageEnemy(e,20,'arcane',true);");
    assert.ok(Math.abs(h.run('e.shield')-73)<1e-7);
  }finally{h.close();}
});
test('lightning and plant-fire propagation cannot recurse through an entire pack',()=>{
  const h=setup();try{
    h.run("game.enemies=[];for(let i=0;i<10;i++){const e=spawnEnemy('stormknight',{x:5+i*.1,y:7});e.hp=e.maxHp=10000;e.shield=0;}damageEnemy(game.enemies[0],100,'lightning');");
    assert.equal(h.run('game.enemies.filter(e=>e.hp<10000).length'),2);
    assert.ok(h.run('game.enemies[0].stun>0'));
  }finally{h.close();}
});
test('summoned actors carry target tags, and invalid damage cannot poison health state',()=>{
  const h=setup();try{
    h.run("SPELL_CASTS.guardianTreant('guardianTreant',SPELLS.guardianTreant,{power:1});var tree=AWContinentalSpells.state().actors[0];");
    assert.equal(h.run("AWCombatAffinity.tags(tree).has('PLANT')"),true);
    h.run("var e=spawnEnemy('wolf',{x:5,y:7});var hp=e.hp;damageEnemy(e,NaN,'fire');damageEnemy(e,-10,'fire');");
    assert.equal(h.run('e.hp'),h.run('hp'));
  }finally{h.close();}
});

test('strong matchups enlarge their damage number while the existing number toggle remains effective',()=>{
  const h=setup();try{
    h.run("var e=spawnEnemy('frostwitch',{x:5,y:7});e.hp=e.maxHp=10000;e.shield=0;$('floatLayer').innerHTML='';damageEnemy(e,100,'fire');");
    assert.equal(h.run("[...$('floatLayer').children].find(el=>/^\\d+$/.test(el.textContent)).style.fontSize"),'19px');
    h.run("$('floatLayer').innerHTML='';AWPresentation.settings.damageNumbers=false;damageEnemy(e,100,'fire');");
    assert.equal(h.run("[...$('floatLayer').children].some(el=>/^\\d+$/.test(el.textContent))"),false);
  }finally{h.close();}
});

test('projectile splash retains its explicit element even when its legacy visual tag differs',()=>{
  const h=setup();try{
    h.run("var e=spawnEnemy('skeleton',{x:8,y:7});e.x=8;e.y=7;e.hp=e.maxHp=10000;var other=spawnEnemy('skeleton',{x:8.9,y:7});other.x=8.9;other.y=7;other.hp=other.maxHp=10000;var q=magicProjectile({x:8,y:7,vx:0,vy:0,damage:100,splash:1.5,combatElement:'solar',tag:'fire'});updateProjectiles(.001);");
    assert.ok(Math.abs(h.run('10000-other.hp')-100*.55*1.4)<1e-7);
  }finally{h.close();}
});

test('native enemy projectiles and direct attacks use ally affinities consistently',()=>{
  const h=setup();try{
    h.run("SPELL_CASTS[SPELLS.guardianTreant.cast]('guardianTreant',SPELLS.guardianTreant,{power:1});var tree=AWContinentalSpells.state().actors.find(a=>a.kind==='treant');var e=spawnEnemy('bomber',{x:4,y:4});e.damage=10;enemyProjectile(e,{x:1,y:0},4.6,'ember',{damage:10});var q=game.projectiles.at(-1);q.x=tree.x;q.y=tree.y;q.vx=q.vy=0;var hp=tree.hp;");
    assert.equal(h.run('q.combatElement'),'fire');
    h.run('updateProjectiles(.001);');
    assert.ok(Math.abs(h.run('hp-tree.hp')-13.5)<1e-7);
    h.run('hp=tree.hp;e.x=tree.x+.2;e.y=tree.y;e.attack=0;e.ai="melee";updateEnemyAI(e,enemyDir(e),dist(e,game.player),0,.016);');
    assert.ok(Math.abs(h.run('hp-tree.hp')-13.5)<1e-7);
  }finally{h.close();}
});

test('physical weakness cracks stay cosmetic during subsequent combat frames',()=>{
  const h=setup();try{
    h.run("var e=spawnEnemy('golem',{x:5,y:7});e.hp=e.maxHp=10000;e.shield=0;var other=spawnEnemy('bandit',{x:5.5,y:7});other.hp=other.maxHp=10000;damageEnemy(e,100,'physical');var hp=e.hp;for(let i=0;i<12;i++)updateSpellEntities(.016);render();");
    assert.equal(h.run('e.hp'),h.run('hp'));
    assert.equal(h.run('other.hp'),10000);
    assert.ok(h.run('AWPresentation.fx.decals.items.some(e=>e.kind==="crack")'));
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
