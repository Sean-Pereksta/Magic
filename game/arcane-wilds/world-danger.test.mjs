import test from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {runtime} from './runtime-test-helper.mjs';
const require=createRequire(import.meta.url),Home=require('./homestead-core.js');
const json=(h,source)=>JSON.parse(h.run('JSON.stringify('+source+')'));
function start(touch=false){
  const h=runtime(touch);
  h.run('var testRandom=712;Math.random=()=>{testRandom=(Math.imul(testRandom,1664525)+1013904223)>>>0;return testRandom/4294967296;};startNewGame();game.xpNeed=1e9;game.player.hp=game.player.maxHp=10000;game.player.armor=0;game.player.attackTimer=10000;');
  return h;
}
function advance(h,seconds){for(let i=0;i<Math.ceil(seconds/.03);i++)h.run('update(.03)');}
function battle(h){
  h.run("game.enemies=[];game.effects=[];game.projectiles=[];game.summons=[];game.roomData={...game.roomData,key:'danger-test',biome:'ruins',difficulty:12,town:false,cleared:false};intensityState().encounter=null;intensityState().hazards=[];game.player.x=9;game.player.y=7;game.player.invuln=0;AWDangerCombat.reset();playerMovement(0);updateEnemyEffects(1.1);");
}
function enterPuzzle(h,kind){
  h.run("var puzzleNode=Object.values(AWCampaignData.nodes).find(n=>n.dangerPuzzle==="+JSON.stringify(kind)+");AWCampaign.enter(puzzleNode.id);game.player.hp=game.player.maxHp=10000;game.player.attackTimer=10000;");
}
function usePuzzle(h,id){
  h.run("var clicked=game.interactables.find(o=>o.type==='challengeObject'&&o.id==="+JSON.stringify(id)+");game.player.x=clicked.x;game.player.y=clicked.y;interact();");
}
function enterProblem(h,kind){
  h.run("var problemNode=Object.values(AWCampaignData.nodes).find(n=>n.dangerProblem==="+JSON.stringify(kind)+");AWCampaign.enter(problemNode.id);game.enemies=[];intensityState().encounter=null;game.player.hp=game.player.maxHp=10000;game.player.invuln=9999;game.player.attackTimer=10000;");
}
function useProblem(h,action,index=0){
  h.run("var clicked=AWWorldDanger.objects().find(o=>o.action==="+JSON.stringify(action)+"&&"+(action==='water'?'true':"o.index==="+index)+");game.player.x=clicked.x;game.player.y=clicked.y;interact();");
}
function optionalSite(h){
  h.run("var optionalNode=Object.values(AWCampaignData.nodes).find(n=>n.type==='landmark'&&!AWWorldDanger.state().legends[n.id]);AWCampaign.enter(optionalNode.id);game.enemies=[];game.roomData.cleared=true;intensityState().encounter=null;game.player.invuln=9999;");
}

test('content includes all eleven affixes, five pursuit archetypes and seven obtainable puzzle kinds',()=>{
  const h=start();try{
    assert.equal(h.run('Object.keys(AWDangerContent.affixes).length'),11);
    assert.equal(h.run('Object.keys(AWDangerContent.pursuits).length'),5);
    assert.deepEqual(json(h,'[...new Set(Object.values(AWCampaignData.nodes).map(n=>n.dangerPuzzle).filter(Boolean))].sort()'),['combat','elements','memory','mirrors','plates','runes','timing']);
    for(const mods of Object.values(json(h,'AWDangerContent.relics')).map(x=>x[1]))for(const value of Object.values(mods))assert.ok(value<.3);
  }finally{h.close();}
});
test('world conditions are deterministic, vary by seed, persist and have bounded ledgers',()=>{
  const h=start();try{
    assert.deepEqual(json(h,'AWDangerContent.fresh(77)'),json(h,'AWDangerContent.fresh(77)'));
    const variants=new Set();for(let i=0;i<20;i++)variants.add(h.run('AWDangerContent.fresh('+i+').modifiers.join()'));assert.ok(variants.size>5);
    const modifiers=json(h,'AWWorldDanger.state().modifiers');h.run('saveGame();loadGame();beginWorld();');
    assert.deepEqual(json(h,'AWWorldDanger.state().modifiers'),modifiers);
    h.run("var dirty={...AWDangerContent.fresh(game.seed),claims:Array(900).fill('same'),puzzles:{garbage:{kind:'runes'}},legends:{garbage:{id:'roc'}}};var clean=AWDangerContent.normalize(dirty,game.seed);");
    assert.equal(h.run('clean.claims.length'),1);assert.equal(h.run('Object.keys(clean.puzzles).length+Object.keys(clean.legends).length'),0);
    h.run('AWWorldDanger.state().claims.push("old");startNewGame();');
    assert.equal(h.run('AWWorldDanger.state().claims.includes("old")'),false);
  }finally{h.close();}
});
test('world modifiers change patrol identity and resource yield without increasing opening counts',()=>{
  const h=start();try{
    h.run("AWWorldDanger.state().modifiers=['goblinUprising','ageOfPlenty'];var n=Object.values(AWCampaignData.nodes).find(n=>n.type==='wildland'&&n.threat>=3&&!n.shadow);AWCampaign.enter(n.id);");
    assert.ok(h.run("game.enemies.some(e=>e.type==='dangerRaider')"));
    assert.ok(h.run('game.enemies.length<=9'));
    assert.equal(h.run('AWWorldDanger.resourceBonus()'),3);
    h.run('AWWorldDanger.state().modifiers=["longWinter","monsterMigration"];');
    assert.equal(h.run('AWWorldDanger.resourceBonus()'),0);
  }finally{h.close();}
});
for(const touch of [false,true])test((touch?'touch':'desktop')+' pursuit signatures warn, draw finite geometry and keep frozen strike lanes',()=>{
  const h=start(touch);try{
    battle(h);
    for(const [type,id] of [['dangerHunter','hunterDash'],['dangerDasher','chainDash'],['dangerBeast','pounceWave'],['dangerAssassin','shadowAmbush'],['dangerRaider','hunterDash']]){
      h.run("game.enemies=[];game.effects=[];updateEnemyEffects(1);var foe=spawnEnemy("+JSON.stringify(type)+",{x:4,y:7});foe.attack=100;foe.hp=foe.maxHp=10000;var a=AWEnemyCombat.start(foe,"+JSON.stringify(id)+");");
      assert.ok(h.run('a.wind>=.7'));
      const marks=json(h,'a.marks'),hp=h.run('game.player.hp');
      h.run('a.age=a.wind-.1;drawTelegraphs();drawEnemy(foe);updateEnemyEffects(.03);game.player.x=2;game.player.y=2;');
      assert.equal(h.run('game.player.hp'),hp);
      assert.deepEqual(json(h,'a.marks'),marks);
      h.run('a.age=a.wind;updateEnemyEffects(.2);drawTelegraphs();render();');
      assert.deepEqual(h.errors,[]);
      h.run('game.player.x=9;game.player.y=7;');
    }
  }finally{h.close();}
});
test('chain pursuit retargets only between legs and advertises each new lane for at least .7s',()=>{
  const h=start();try{
    battle(h);h.run("var foe=spawnEnemy('dangerDasher',{x:4,y:7});foe.attack=100;var a=AWEnemyCombat.start(foe,'chainDash');a.age=a.wind;updateEnemyEffects(.48);");
    assert.equal(h.run('a.chainLeg'),1);assert.ok(h.run('a.chainStart-(a.age-a.wind)>=.7'));
    const lane=json(h,'a.dashes[1]');
    h.run('game.player.x=1;game.player.y=1;updateEnemyEffects(.3);');
    assert.deepEqual(json(h,'a.dashes[1]'),lane);
    assert.ok(h.run('foe.x===a.dashes[1].x&&foe.y===a.dashes[1].y'));
  }finally{h.close();}
});
test('a pounce shockwave has a safe center and grants actual-overlap dodge counterplay',()=>{
  const h=start();try{
    battle(h);h.run("var foe=spawnEnemy('dangerBeast',{x:4,y:7});foe.attack=100;var a=AWEnemyCombat.start(foe,'pounceWave');a.age=a.wind+.8;");
    assert.equal(h.run('AWEnemyCombat.shapes(a)[0].shape'),'ring');
    assert.equal(h.run('AWEnemyCombat.inShape(AWEnemyCombat.shapes(a)[0],a.jumpEnd,.1)'),false);
    h.run('drawEffect(a,true);drawTelegraphs();');
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
test('Juggernaut blocks frontal attacks; rear strikes and Earth provide real counters',()=>{
  const h=start();try{
    battle(h);h.run("var foe=spawnEnemy('bandit',{x:8,y:7});foe.hp=foe.maxHp=10000;foe.facing={x:1,y:0};AWDangerCombat.affix(foe,['Juggernaut']);damageEnemy(foe,100,'weapon',false,{x:10,y:7,combatElement:'physical'});var front=10000-foe.hp;foe.hp=10000;damageEnemy(foe,100,'weapon',false,{x:6,y:7,combatElement:'physical'});var rear=10000-foe.hp;");
    assert.ok(h.run('rear>front*3'));
    h.run("foe.hp=10000;damageEnemy(foe,100,'earth',false,{x:10,y:7,combatElement:'earth'});");
    assert.ok(h.run('10000-foe.hp>rear'));assert.ok(h.run('foe.dangerArmorBroken>0'));
  }finally{h.close();}
});
test('wind interrupts an owned attack without leaving the enemy stuck in its attack state',()=>{
  const h=start();try{
    battle(h);h.run("var foe=spawnEnemy('dangerHunter',{x:4,y:7});foe.hp=foe.maxHp=10000;AWEnemyCombat.start(foe,'hunterDash');damageEnemy(foe,10,'gust');");
    assert.equal(h.run('foe.combatAbility'),null);assert.equal(h.run('foe.state'),'idle');
    assert.equal(h.run("AWCombatAffinity.spellElement('gust')"),'wind');
    assert.equal(h.run("AWCombatAffinity.spellElement('quake')"),'earth');
  }finally{h.close();}
});
test('Vampiric heals from health damage, while a dodge denies healing',()=>{
  const h=start();try{
    battle(h);h.run("var foe=spawnEnemy('bandit',{x:8,y:7});foe.hp=50;foe.maxHp=100;AWDangerCombat.affix(foe,['Vampiric']);AWRegionalContent.incoming=foe;damagePlayer(10);");
    assert.ok(h.run('foe.hp>50'));
    const hp=h.run('foe.hp');h.run('game.player.invuln=0;game.player.dodgeTime=.2;damagePlayer(10);AWRegionalContent.incoming=null;');
    assert.equal(h.run('foe.hp'),hp);
  }finally{h.close();}
});
test('Necromancer resurrects one recent corpse and a real interrupt cancels the ritual',()=>{
  for(const interrupt of [false,true]){
    const h=start();try{
      battle(h);h.run("var dead=spawnEnemy('wolf',{x:5,y:5});killEnemy(dead);game.enemies=game.enemies.filter(e=>!e.dead);var foe=spawnEnemy('necro',{x:6,y:5});foe.hp=foe.maxHp=10000;foe.attack=100;AWDangerCombat.affix(foe,['Necromancer']);foe.dangerCd=0;updateEnemyEffects(.03);");
      assert.ok(h.run('!!foe.dangerRitual'));
      if(interrupt)h.run("damageEnemy(foe,10,'gust');");
      h.run('updateEnemyEffects(1.5);');
      assert.equal(h.run('game.enemies.filter(e=>e.dangerResurrected).length'),interrupt?0:1);
      if(!interrupt)assert.equal(h.run('game.enemies.find(e=>e.dangerResurrected).xp'),0);
    }finally{h.close();}
  }
});
test('Blazing and Frostborn create warned hazards; pausing freezes all affix timers',()=>{
  const h=start();try{
    battle(h);h.run("var foe=spawnEnemy('bandit',{x:4,y:4},true);AWDangerCombat.affix(foe,['Blazing','Frostborn']);foe.dangerTrailCd=0;updateEnemyEffects(.03);");
    assert.ok(h.run("AWEnemyCombat.actions().some(a=>a.ability==='pool'&&a.wind>=.7)"));
    const cd=h.run('foe.dangerTrailCd');h.run('paused=true;updateEnemyEffects(1);');assert.equal(h.run('foe.dangerTrailCd'),cd);
    h.run('paused=false;AWCampaign.enter("verdant-city");');assert.equal(h.run('AWEnemyCombat.actions().length'),0);
  }finally{h.close();}
});
test('Mirror copies an offensive school as a warned enemy cast',()=>{
  const h=start();try{
    battle(h);h.run("var foe=spawnEnemy('bandit',{x:4,y:7});foe.attack=100;foe.hp=foe.maxHp=10000;AWDangerCombat.affix(foe,['Mirror']);game.player.activeSpells[0]='firebolt';game.player.spellState.firebolt={cd:0};castSpell(0);foe.dangerCd=0;updateEnemyEffects(.03);");
    assert.equal(h.run('foe.combatAbility.ability'),'flameWave');assert.equal(h.run('foe.combatAbility.element'),'fire');assert.ok(h.run('foe.combatAbility.wind>=.7'));
  }finally{h.close();}
});
test('Commander supports formation and falls out of squad decisions immediately on death',()=>{
  const h=start();try{
    battle(h);h.run("var command=spawnEnemy('bandit',{x:6,y:4},true);AWDangerCombat.affix(command,['Commander']);var foe=spawnEnemy('bandit',{x:5,y:5});var archer=spawnEnemy('archer',{x:4,y:4});foe.attack=100;updateEnemyAI(foe,enemyDir(foe),8,foe.speed,.03);");
    assert.equal(h.run('foe.dangerCommander===command'),true);
    h.run('command.dead=true;updateEnemyAI(foe,enemyDir(foe),8,foe.speed,.03);');assert.equal(h.run('foe.dangerCommander'),null);
  }finally{h.close();}
});
for(const touch of [false,true])test((touch?'touch':'desktop')+' physical rune solution gives one reward, survives reopen, and exposes clues/reset',()=>{
  const h=start(touch);try{
    enterPuzzle(h,'runes');usePuzzle(h,'clue');
    assert.match(h.w.document.getElementById('npcBody').textContent,/Visit the stones/);
    assert.ok(h.w.document.querySelector('#npcBody button'));
    h.run("closeOverlay('npcPanel');");const order=json(h,'AWChallengeRooms.state().order');
    for(const i of order)usePuzzle(h,'rune'+i);
    assert.equal(h.run('AWChallengeRooms.record().solved'),true);
    const count=h.run('game.inventory.items.length'),gold=h.run('game.gold');
    h.run('AWChallengeRooms.solve();saveGame();loadGame();beginWorld();');
    assert.equal(h.run('AWChallengeRooms.record().solved'),true);
    assert.equal(h.run('game.inventory.items.length'),count);assert.equal(h.run('game.gold'),gold);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
test('wrong runes reset progress and warn before dealing any trap damage',()=>{
  const h=start();try{
    enterPuzzle(h,'runes');const wrong=h.run('(AWChallengeRooms.state().order[0]+1)%4'),hp=h.run('game.player.hp');
    usePuzzle(h,'rune'+wrong);assert.equal(h.run('AWChallengeRooms.record().step'),0);assert.equal(h.run('AWChallengeRooms.record().mistakes'),1);
    assert.equal(h.run('game.player.hp'),hp);assert.ok(h.run("AWEnemyCombat.actions().some(a=>a.wind===1)"));
  }finally{h.close();}
});
test('mirror puzzle traces actual reflection paths and solves only when the beam reaches its receiver',()=>{
  const h=start();try{
    enterPuzzle(h,'mirrors');assert.equal(h.run('AWChallengeRooms.trace().lit'),false);
    for(const [i,target] of [[0,1],[1,1],[2,0]])while(h.run('AWChallengeRooms.record().rotation['+i+']')!==target)usePuzzle(h,'mirror'+i);
    assert.equal(h.run('AWChallengeRooms.record().solved'),true);assert.equal(h.run('AWChallengeRooms.trace().lit'),true);
    h.run('render();');assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
test('partial puzzle saves restore finite mirrors and preserve every required elemental seal',()=>{
  const h=start();try{
    enterPuzzle(h,'mirrors');h.run('AWChallengeRooms.record().rotation=[];saveGame();loadGame();beginWorld();');
    assert.deepEqual(json(h,'AWChallengeRooms.record().rotation'),[3,2,1]);
    usePuzzle(h,'mirror0');assert.ok(h.run('Number.isFinite(AWChallengeRooms.record().rotation[0])'));
    h.run("AWCampaign.state().defeated.push(AWCampaignData.continent('verdant').boss);AWCampaign.state().unlocked.push('meridian');");
    enterPuzzle(h,'elements');h.run('AWChallengeRooms.record().seals=[true,,true];saveGame();loadGame();beginWorld();');
    assert.deepEqual(json(h,'AWChallengeRooms.record().seals'),[true,false,true]);
    usePuzzle(h,'seal0');assert.equal(h.run('AWChallengeRooms.record().solved'),false);
    usePuzzle(h,'conduit');usePuzzle(h,'seal1');assert.equal(h.run('AWChallengeRooms.record().solved'),true);
    h.run('render();');assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
test('crates must occupy both plates together and their positions persist through save',()=>{
  const h=start();try{
    enterPuzzle(h,'plates');usePuzzle(h,'crate0');h.run('game.player.x=9;game.player.y=4;interact();');
    assert.equal(h.run('AWChallengeRooms.record().solved'),false);
    assert.equal(h.run('AWChallengeRooms.record().crates[0].x'),9);
    usePuzzle(h,'crate1');h.run('game.player.x=12;game.player.y=4;interact();');
    assert.equal(h.run('AWChallengeRooms.record().solved'),true);
  }finally{h.close();}
});
test('elemental puzzle can be solved with its conduit even with no offensive spells equipped',()=>{
  const h=start();try{
    enterPuzzle(h,'elements');h.run('game.player.activeSpells=[];');
    usePuzzle(h,'seal0');assert.equal(h.run('AWChallengeRooms.record().seals[0]'),true);
    usePuzzle(h,'seal2');assert.equal(h.run('!!AWChallengeRooms.record().seals[2]'),false);
    usePuzzle(h,'conduit');usePuzzle(h,'seal1');usePuzzle(h,'conduit');usePuzzle(h,'seal2');
    assert.equal(h.run('AWChallengeRooms.record().solved'),true);
  }finally{h.close();}
});
test('memory demonstrates a pattern, blocks premature inputs, and supports replay',()=>{
  const h=start();try{
    enterPuzzle(h,'memory');usePuzzle(h,'replay');const pattern=json(h,'AWChallengeRooms.state().order');
    usePuzzle(h,'rune'+pattern[0]);assert.equal(h.run('AWChallengeRooms.record().step'),0);
    advance(h,7);for(const i of pattern)usePuzzle(h,'rune'+i);assert.equal(h.run('AWChallengeRooms.record().solved'),true);
  }finally{h.close();}
});
test('timed puzzle expires, pauses behind menus, and can be retried to completion',()=>{
  const h=start();try{
    enterPuzzle(h,'timing');usePuzzle(h,'lever0');const time=h.run('AWChallengeRooms.timer');
    h.run('paused=true;update(2);paused=false;');assert.equal(h.run('AWChallengeRooms.timer'),time);
    advance(h,15);assert.equal(h.run('AWChallengeRooms.record().seals.filter(Boolean).length'),0);
    for(const i of [0,1,2])usePuzzle(h,'lever'+i);
    assert.equal(h.run('AWChallengeRooms.record().solved'),true);
  }finally{h.close();}
});
test('combat puzzle armor requires three real charge-pillar collisions, then permits the kill',()=>{
  const h=start();try{
    enterPuzzle(h,'combat');usePuzzle(h,'trial');
    h.run("var guard=game.enemies.find(e=>e.dangerPuzzleGuard);var guardHp=guard.hp;damageEnemy(guard,99999,'earth');");
    assert.equal(h.run('guard.hp'),h.run('guardHp'));
    for(let i=0;i<3;i++){
      h.run("game.effects=[];AWEnemyCombat.cancel(guard);guard.stun=0;guard.x=AWChallengeRooms.state().pillars["+i+"].x-2;guard.y=AWChallengeRooms.state().pillars["+i+"].y;game.player.x=guard.x+4;game.player.y=guard.y;updateEnemyEffects(1);var strike=AWEnemyCombat.start(guard,'hunterDash');strike.age=strike.wind;updateEnemyEffects(.16);");
      assert.equal(h.run('AWChallengeRooms.state().pillars['+i+'].broken'),true);
    }
    h.run("damageEnemy(guard,99999,'earth');updateEnemyEffects(.03);");
    assert.equal(h.run('AWChallengeRooms.record().solved'),true);assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
test('a puzzle reset removes its guardian and restores a solvable layout without claiming loot',()=>{
  const h=start();try{
    enterPuzzle(h,'combat');usePuzzle(h,'trial');const count=h.run('game.inventory.items.length');
    assert.equal(h.run('AWChallengeRooms.reset()'),true);
    assert.equal(h.run('game.enemies.some(e=>e.dangerPuzzleGuard)'),false);assert.equal(h.run('game.inventory.items.length'),count);
    assert.equal(h.run('AWChallengeRooms.state().pillars.filter(o=>o.broken).length'),0);
  }finally{h.close();}
});
test('world events cannot clear or reward while their objectives remain unfinished',()=>{
  const h=start();try{
    enterProblem(h,'fire');const gold=h.run('game.gold');h.run('markRoomCleared();');
    assert.equal(h.run('game.roomData.cleared'),false);assert.equal(h.run('game.gold'),gold);
  }finally{h.close();}
});
for(const kind of ['fire','mine','nest','seal','flood','caravan'])test(kind+' world problem completes through its real interactions and rewards once',()=>{
  const h=start();try{
    enterProblem(h,kind);
    if(kind==='fire'){useProblem(h,'water');for(let i=0;i<3;i++)useProblem(h,'fire',i);for(let i=3;i<5;i++)useProblem(h,'rescue',i);}
    if(kind==='mine'){for(let i=0;i<2;i++){useProblem(h,'debris',i);advance(h,1.3);}for(let i=2;i<4;i++)useProblem(h,'mechanism',i);useProblem(h,'miner',4);}
    if(kind==='nest')for(let i=0;i<3;i++){useProblem(h,'egg',i);advance(h,.9);}
    if(kind==='seal')for(const i of json(h,'AWWorldDanger.problem().order'))useProblem(h,'sealRune',i);
    if(kind==='flood'){useProblem(h,'valve',1);assert.equal(h.run('!!AWWorldDanger.problem().record.steps[1]'),false);useProblem(h,'valve',0);useProblem(h,'valve',1);useProblem(h,'ruinVault',2);}
    if(kind==='caravan')for(let i=0;i<640;i++)h.run('game.player.x=AWWorldDanger.problem().wagon.x;game.player.y=7;update(.03);');
    assert.equal(h.run('AWWorldDanger.problemReady()'),true);h.run('markRoomCleared();');
    assert.equal(h.run('game.roomData.cleared'),true);assert.equal(h.run('AWWorldDanger.problem().record.done'),true);
    const gold=h.run('game.gold'),count=h.run('game.inventory.items.length');h.run('markRoomCleared();update(.03);');
    assert.equal(h.run('game.gold'),gold);assert.equal(h.run('game.inventory.items.length'),count);assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
test('mine/egg work cancels when movement or damage interrupts the interaction',()=>{
  const h=start();try{
    enterProblem(h,'mine');useProblem(h,'debris',0);h.run('game.player.x+=2;update(.03);');
    assert.equal(h.run('AWWorldDanger.problem().channel'),null);assert.equal(h.run('!!AWWorldDanger.problem().record.steps[0]'),false);
    useProblem(h,'debris',0);h.run('game.player.invuln=0;damagePlayer(10);');
    assert.equal(h.run('AWWorldDanger.problem().channel'),null);
  }finally{h.close();}
});
test('flood blocks the submerged chamber until both valves are open',()=>{
  const h=start();try{
    enterProblem(h,'flood');h.run('game.player.x=9;game.player.y=4;playerMovement(.03);');assert.ok(h.run('game.player.y>=5'));
    useProblem(h,'valve',0);useProblem(h,'valve',1);h.run('game.player.y=4;playerMovement(.03);');assert.ok(h.run('game.player.y<5'));
  }finally{h.close();}
});
test('every legendary creature has a seeded optional site; discovery and victory survive saves',()=>{
  const h=start();try{
    const entries=json(h,'Object.entries(AWWorldDanger.state().legends)');
    assert.equal(entries.length,6);
    for(const [id,l] of entries){
      h.run('AWCampaign.enter('+JSON.stringify(id)+');game.enemies=[];game.roomData.cleared=true;intensityState().encounter=null;');
      assert.equal(h.run('AWWorldDanger.startBattle("legendary")'),true);
      assert.equal(h.run('game.enemies.find(e=>e.boss).dangerLegend'),l.id);
      h.run('for(const e of [...game.enemies])killEnemy(e);game.enemies=[];markRoomCleared();');
      assert.equal(h.run('AWWorldDanger.state().legends['+JSON.stringify(id)+'].defeated'),true);
      const count=h.run('game.inventory.items.length');h.run('markRoomCleared();');
      assert.equal(h.run('game.inventory.items.length'),count);
    }
    assert.equal(h.run('Object.values(AWWorldDanger.state().legends).filter(l=>l.defeated).length'),6);
    const saved=json(h,'AWWorldDanger.state().legends');h.run('saveGame();loadGame();beginWorld();');assert.deepEqual(json(h,'AWWorldDanger.state().legends'),saved);
  }finally{h.close();}
});
test('optional fights permit retreat through a real directional road and never bypass continent locks',()=>{
  const h=start();try{
    optionalSite(h);assert.equal(h.run('AWWorldDanger.startBattle("risk")'),true);
    assert.equal(h.run('game.roomData.cleared'),false);
    h.run('var exit=Object.keys(AWTravel.exits())[0];var destination=AWTravel.exits()[exit].id;transitionRoom(0,0,exit);');
    assert.equal(h.run('AWCampaign.current().id===destination'),true);assert.equal(h.run('AWWorldDanger.activity()'),null);
    assert.equal(h.run('AWCampaign.travel("gloam-city","waystone")'),false);
  }finally{h.close();}
});
for(const [i,kind] of ['survival','noHit','elemental','mobility','bossRush'].entries())test(kind+' optional challenge has its own working objective and one-time reward',()=>{
  const h=start();try{
    optionalSite(h);h.run("var originalHash=AWDangerContent.hash;AWDangerContent.hash=(seed,text)=>text===AWCampaign.current().id?"+i+":originalHash(seed,text);");
    assert.equal(h.run('AWWorldDanger.startBattle("trial")'),true);assert.equal(h.run('AWWorldDanger.activity().trial'),kind);
    if(kind==='survival'){h.run('game.enemies=[];markRoomCleared();');assert.equal(h.run('game.roomData.cleared'),false);advance(h,20.1);}
    if(kind==='noHit'){h.run('game.player.invuln=0;damagePlayer(10);for(const e of [...game.enemies])killEnemy(e);game.enemies=[];markRoomCleared();');assert.equal(h.run('AWWorldDanger.activity().noHit'),false);}
    if(kind==='elemental')h.run("for(const e of [...game.enemies]){const kind=e.type==='frostwitch'?'fire':e.type==='drake'?'frost':'solar';damageEnemy(e,99999,kind);}game.enemies=[];markRoomCleared();");
    if(kind==='mobility'){for(let i=0;i<3;i++)useProblem(h,'trialSwitch',i);h.run("var finish=AWWorldDanger.objects().find(o=>o.action==='trialFinish');game.player.x=finish.x;game.player.y=finish.y;interact();");}
    if(kind==='bossRush')for(let round=0;round<3;round++){h.run('for(const e of [...game.enemies])killEnemy(e);game.enemies=[];markRoomCleared();');advance(h,2);}
    assert.equal(h.run('AWWorldDanger.activity().finished'),true);assert.ok(h.run('AWWorldDanger.state().claims.includes("optional:"+AWCampaign.current().id)'));
    const count=h.run('game.inventory.items.length');h.run('markRoomCleared();');assert.equal(h.run('game.inventory.items.length'),count);assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
test('boss phases change patterns and a charge into a real pillar exposes armor',()=>{
  const h=start();try{
    h.run("AWCampaign.enter('verdant-boss2');var boss=game.enemies.find(e=>e.boss);");
    const first=json(h,'boss.dangerProfile');h.run('intensityBossPhase(boss,2);');assert.notDeepEqual(json(h,'boss.dangerProfile'),first);
    h.run('intensityBossPhase(boss,3);');assert.ok(h.run('boss.dangerProfile.includes("chainDash")||boss.dangerProfile.includes("pounceWave")'));
    h.run("game.effects=[];boss.stun=0;boss.x=2;boss.y=4;game.player.x=7;game.player.y=4;updateEnemyEffects(1);var charge=AWEnemyCombat.start(boss,'charge');charge.age=charge.wind;updateEnemyEffects(.25);");
    assert.ok(h.run('boss.dangerArmorBroken>0'));assert.ok(h.run('AWDangerCombat.objects().some(o=>o.life<=0)'));
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
test('boss pillars require active movement and each new boss receives its own arena',()=>{
  const h=start();try{
    h.run("AWCampaign.enter('verdant-boss2');var boss=game.enemies.find(e=>e.boss);var pillar=AWDangerCombat.objects().find(o=>o.kind==='pillar');boss.x=pillar.x;boss.y=pillar.y;boss.stun=0;game.player.x=9;game.player.y=7;updateEnemyEffects(1);var a=AWEnemyCombat.start(boss,'charge');updateEnemyEffects(.03);");
    assert.ok(h.run('pillar.life>0'));assert.equal(h.run('boss.dangerArmorBroken||0'),0);
    h.run("var second=spawnEnemy('golem',{x:9,y:7});second.boss=true;AWDangerCombat.arena(second,'void');");
    assert.equal(h.run("AWDangerCombat.objects().filter(o=>o.owner===second&&o.kind==='shieldCrystal').length"),4);
  }finally{h.close();}
});
test('two affixes compose their attack profiles without receiving a hidden legacy modifier',()=>{
  const h=start();try{
    battle(h);h.run("var foe=spawnEnemy('bandit',{x:4,y:7},true);AWDangerCombat.affix(foe,['Predator','Teleporter']);foe.attack=100;var a=AWEnemyCombat.start(foe,'hunterDash');");
    assert.ok(h.run('AWEnemyCombat.profile(foe).includes("shadowAmbush")'));
    assert.ok(h.run('AWEnemyCombat.profile(foe).includes("pounceWave")'));
    assert.equal(h.run('a.modifier'),'');
    h.run("AWEnemyCombat.cancel(foe);AWDangerCombat.affix(foe,['Stormbound']);");
    assert.ok(h.run('AWEnemyCombat.profile(foe).includes("chain")'));
    assert.ok(h.run('AWEnemyCombat.profile(foe).includes("hunterDash")'));
  }finally{h.close();}
});
test('region hazards and ice drift pause with simulation; travel cancels transient ground jobs',()=>{
  const h=start();try{
    h.run("var n=Object.values(AWCampaignData.nodes).find(n=>n.biome==='frost'&&n.type==='wildland');AWCampaign.enter(n.id);game.player.invuln=9999;game.player.attackTimer=10000;");
    const next=h.run('AWWorldDanger.environment().next');h.run('modalPause=true;update(2);modalPause=false;');assert.equal(h.run('AWWorldDanger.environment().next'),next);
    h.run('game.player.x=6;game.player.y=6;');advance(h,1.8);
    assert.ok(h.run('AWWorldDanger.environment().patches[0].reset>0'));
    h.run('AWCampaign.enter("verdant-city");');assert.equal(h.run('AWWorldDanger.environment()'),null);assert.equal(h.run('AWEnemyCombat.actions().length'),0);
  }finally{h.close();}
});
test('environmental explosions warn first and then damage enemies inside the promised radius',()=>{
  const h=start();try{
    h.run("var n=Object.values(AWCampaignData.nodes).find(n=>n.biome==='ruins'&&n.type==='wildland'&&n.threat>=3);AWCampaign.enter(n.id);game.enemies=[];intensityState().encounter=null;game.roomData.cleared=false;var barrel=AWWorldDanger.objects().find(o=>o.action==='barrel');var foe=spawnEnemy('bandit',{x:barrel.x,y:barrel.y});foe.hp=foe.maxHp=10000;foe.attack=100;game.player.x=barrel.x;game.player.y=barrel.y;interact();");
    assert.equal(h.run('foe.hp'),10000);h.run('game.player.x=1;game.player.y=1;');advance(h,1.05);assert.ok(h.run('foe.hp<10000'));
  }finally{h.close();}
});
test('completed world problems and optional fights stop scheduling environmental hazards',()=>{
  const h=start();try{
    enterProblem(h,'mine');
    for(let i=0;i<2;i++){useProblem(h,'debris',i);advance(h,1.3);}
    for(let i=2;i<4;i++)useProblem(h,'mechanism',i);useProblem(h,'miner',4);
    h.run("AWWorldDanger.state().modifiers=['arcaneStorm'];game.effects=[];AWWorldDanger.environment().next=0;AWWorldDanger.environment().conditionNext=0;");
    advance(h,2);assert.equal(h.run('AWEnemyCombat.actions().length'),0);
    optionalSite(h);h.run("AWWorldDanger.startBattle('risk');for(const e of [...game.enemies])killEnemy(e);game.enemies=[];markRoomCleared();game.effects=[];AWWorldDanger.environment().conditionNext=0;");
    assert.equal(h.run('AWWorldDanger.activity().finished'),true);
    advance(h,2);assert.equal(h.run('AWEnemyCombat.actions().length'),0);
  }finally{h.close();}
});
test('settlement situations are earned from distinct expedition sites and cannot double-pay',()=>{
  let home=Home.fresh('journey',1000),wallet={gold:200,materials:{iron:20,hide:20,herbs:20,dust:20}};
  for(let i=0;i<3;i++){const r=Home.apply(home,wallet,{type:'claimExpedition',node:'site'+i,requestId:'claim'+i},1000,{earned:true,multiWave:true});home=r.home;wallet=r.wallet;}
  assert.equal(home.dangerEvent.kind,'banditRaid');
  const id=home.dangerEvent.id,result=Home.apply(home,wallet,{type:'resolveDangerEvent',eventId:id,solution:'help',requestId:'resolve'},1000,{atHome:true});
  assert.equal(result.home.dangerEvent,null);assert.ok(result.wallet.gold>wallet.gold);
  const duplicate=Home.apply(result.home,result.wallet,{type:'resolveDangerEvent',eventId:id,solution:'help',requestId:'resolve'},1000,{atHome:true});
  assert.equal(duplicate.duplicate,true);assert.deepEqual(duplicate.wallet,result.wallet);
  assert.throws(()=>Home.apply(result.home,result.wallet,{type:'resolveDangerEvent',eventId:id,solution:'help',requestId:'again'},1000,{atHome:true}),/no longer active/);
});
test('all thirteen settlement events validate costs, placed upgrades, victory and failed commands atomically',()=>{
  for(const [kind,d] of Object.entries(Home.dangerEvents)){
    const home=Home.fresh('journey',1000);home.dangerEvent={id:'event',kind};
    const wallet={gold:1000,materials:{iron:100,hide:100,herbs:100,dust:100,timber:100}};
    assert.throws(()=>Home.apply(home,wallet,{type:'resolveDangerEvent',eventId:'event',solution:'prepared',requestId:'bad'},1000,{atHome:true}),/required settlement upgrade/);
    home.items.push({id:'station',kind:d.station,packed:false,level:1,evaluatedAt:1000});
    const resolved=Home.apply(home,wallet,{type:'resolveDangerEvent',eventId:'event',solution:'prepared',requestId:'ok'},1000,{atHome:true});
    assert.equal(resolved.home.dangerEvent,null);
    assert.equal(home.dangerEvent.id,'event');assert.equal(wallet.gold,1000);
    if(d.raid)assert.throws(()=>Home.apply(home,wallet,{type:'resolveDangerEvent',eventId:'event',solution:'defend',requestId:'unearned'},1000,{atHome:true}),/Win the optional defense/);
  }
});
test('a repeated expedition cannot create a queued settlement event after an earlier event is resolved',()=>{
  let home=Home.fresh('journey',1000),wallet={gold:200,materials:{hide:20}};
  for(let i=0;i<6;i++){const r=Home.apply(home,wallet,{type:'claimExpedition',node:'site'+i,requestId:'claim'+i},1000,{earned:true,multiWave:true});home=r.home;wallet=r.wallet;}
  const resolved=Home.apply(home,wallet,{type:'resolveDangerEvent',eventId:home.dangerEvent.id,solution:'help',requestId:'resolve'},1000,{atHome:true});
  const repeated=Home.apply(resolved.home,resolved.wallet,{type:'claimExpedition',node:'site0',requestId:'retry'},1000,{earned:true});
  assert.equal(repeated.home.dangerEvent,null);
  assert.deepEqual(repeated.wallet,resolved.wallet);
  const next=Home.apply(repeated.home,repeated.wallet,{type:'claimExpedition',node:'site6',requestId:'new'},1000,{earned:true});
  assert.equal(next.home.dangerEvent.kind,'monsterRaid');
});
test('actual settlement defense protects home mutations and claims through the existing home transaction adapter',async()=>{
  const h=start();try{
    h.run("AWCampaign.enter('verdant-hearthglade');AWHome.state().dangerEvent={id:'raid-1',kind:'banditRaid'};");
    assert.equal(h.run('AWWorldDanger.startBattle("settlement")'),true);
    assert.equal(h.run('AWHome.context().dangerDefenseActive'),true);
    assert.equal(await h.run('AWHome.action("craft",{kind:"hearth"})'),false);
    h.run('for(const e of [...game.enemies])killEnemy(e);game.enemies=[];markRoomCleared();');
    assert.equal(h.run('AWWorldDanger.settlementVictory()'),'raid-1');
    const gold=h.run('game.gold');
    assert.equal(await h.run('AWHome.action("resolveDangerEvent",{eventId:"raid-1",solution:"defend"})'),true);
    assert.equal(h.run('AWHome.state().dangerEvent'),null);assert.ok(h.run('game.gold')>gold);
    assert.equal(h.run("game.interactables.filter(o=>o.action==='settlement').length"),1);
    h.run('AWHome.rebuild();AWHome.rebuild();');
    assert.equal(h.run("game.interactables.filter(o=>o.action==='settlement').length"),1);
  }finally{h.close();}
});
