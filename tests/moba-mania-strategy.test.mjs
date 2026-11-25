import test from 'node:test';
import assert from 'node:assert/strict';
import { Game } from '../game/moba-mania/engine.mjs';
import { WORLD, DIFFICULTIES } from '../game/moba-mania/data.mjs';
import { xpToNextLevel } from '../game/moba-mania/data.mjs';
import { updateVision, assignTeamJobs, strength } from '../game/moba-mania/strategy.mjs';
import { scheduleImpact } from '../game/moba-mania/progression.mjs';

function arena(hero='angel'){
  const game=new Game({hero,difficulty:'hard',seed:416}),player=game.player;
  game.units=[player];game.campStates=[];game.waveIn=game.beastIn=game.planIn=1e8;
  Object.assign(player,{x:1450,y:1100,attackTimer:0});game.rebuildGrid();return {game,player};
}
function hero(game,id,team,x=1520,y=1100){const h=game.makeHero(id,team,1);Object.assign(h,{x,y,bot:false,attackTimer:0});game.rebuildGrid();return h;}
function advance(game,seconds){for(let t=0;t<seconds-1e-9;t+=.05)game.update(Math.min(.05,seconds-t));}

test('useful healing and absorbed shields earn one personal hero assist, never gold',()=>{
  const {game,player}=arena(),ally=hero(game,'dragon',0),enemy=hero(game,'ranger',1,1570);
  game.damage(ally,100,enemy);game.damage(enemy,1,ally);
  const healed=game.heal(ally,60,player);assert.equal(healed,60);
  game.shield(ally,50,5,player);game.damage(ally,20,enemy);
  assert.equal(player.metrics.shieldingDone,20);assert.equal(player.metrics.healingDone,60);
  game.damage(enemy,10000,ally);
  assert.equal(player.assists,1);assert.equal(player.metrics.assists,1);assert.equal(player.xp,81);
  assert.equal(player.gold,0);assert.equal(player.kills,0);assert.equal(ally.gold,190);
});

test('full-health heals, unabsorbed shields, self-support, and expired aid give no support assist',()=>{
  for(const mode of ['overheal','unused-shield','expired']){
    const {game,player}=arena(),ally=hero(game,'dragon',0),enemy=hero(game,'ranger',1,1570);
    game.damage(enemy,1,ally);
    if(mode==='overheal')assert.equal(game.heal(ally,60,player),0);
    else if(mode==='unused-shield')game.shield(ally,50,5,player);
    else{ally.hp-=60;game.heal(ally,60,player);game.time=11;}
    game.damage(enemy,10000,ally);assert.equal(player.assists,0,mode);assert.equal(player.xp,0,mode);assert.equal(player.gold,0,mode);
  }
});

test('shutdowns cap at300 extra gold and reward only the final hitter',()=>{
  const {game,player}=arena('dragon'),assistant=hero(game,'ranger',0),enemy=hero(game,'ember',1,1570);
  enemy.streak=30;game.damage(enemy,1,assistant);game.damage(enemy,10000,player);
  assert.equal(player.gold,190+300);assert.equal(assistant.gold,0);assert.equal(enemy.streak,0);
  assert.equal(player.streak,1);assert.equal(player.metrics.bestStreak,1);assert.equal(player.metrics.shutdowns,1);
  assert.equal(player.metrics.lastHits,1);assert.equal(player.metrics.goldEarned,490);
});

test('owned damage and finishing blows populate separate hero, tower and farming metrics',()=>{
  const {game,player}=arena('arc'),enemy=hero(game,'dragon',1,1580);
  const tower=game.addUnit('tower',1,1680,1100,{hp:1000,tier:0,lane:1});
  const minion=game.addUnit('footman',1,1500,1100,{hp:10});
  const camp=game.addUnit('camp',-1,1530,1100,{hp:10});
  const heroDamage=game.damage(enemy,23,player),towerDamage=game.damage(tower,31,player);game.damage(minion,100,player);game.damage(camp,100,player);
  assert.equal(player.metrics.damageToHeroes,heroDamage);assert.equal(player.metrics.damageToStructures,towerDamage);
  assert.equal(player.metrics.damageDealt,heroDamage+towerDamage+20);assert.equal(player.metrics.minionLastHits,1);assert.equal(player.metrics.jungleLastHits,1);
});

test('an expired ranged summon keeps its reward owner without impersonating hero basic attacks',()=>{
  const {game,player}=arena('arc'),enemy=hero(game,'dragon',1,1640);
  game.summon(player,'spirit',1,{x:1490,y:1100},20);const spirit=game.units.find(u=>u.kind==='summon');
  Object.assign(spirit,{x:1490,y:1100,range:200,attackTimer:0,damage:20});game.rebuildGrid();
  assert(game.basicAttack(spirit,enemy));const projectile=game.projectiles[0];assert.equal(projectile.sourceKind,'summon');
  const hp=enemy.hp;game.units=game.units.filter(u=>u!==spirit);game.rebuildGrid();game.tickProjectiles(.5);
  assert.equal(enemy.hp,hp-13);assert.equal(player.metrics.damageToHeroes,13,'summon damage uses the same PvP scale after its source expires');
  assert.equal(player.evolution.counts?.attacks||0,0,'summon hit cannot charge a hero basic attack passive');
});

test('brush hides enemies from automatic targeting and bots until shared vision or reveal',()=>{
  const {game,player}=arena('arc'),brush=WORLD.brush[0],enemy=hero(game,'ember',1,brush.x,brush.y);
  Object.assign(player,{x:brush.x-230,y:brush.y});game.rebuildGrid();
  assert.equal(game.canSee(0,enemy),false);assert.equal(game.attackTarget(enemy.id),false);
  assert.equal(game.nearestPlayerEnemy(),null);assert.equal(game.findTarget(player,400),null);
  enemy.revealedUntil=game.time+2;assert.equal(game.canSee(0,enemy),true);assert.equal(game.nearestPlayerEnemy().id,enemy.id);
  updateVision(game);const seen={...game.lastSeen[0][enemy.id]};game.time=3;enemy.x+=15;updateVision(game);
  assert.deepEqual(game.lastSeen[0][enemy.id],seen,'unseen movement does not update remembered location');
  const scout=game.addUnit('footman',0,brush.x-30,brush.y);assert.equal(game.canSee(0,enemy),true,'allied minion sight is shared');
  scout.alive=false;game.time=9;updateVision(game);assert.equal(game.lastSeen[0][enemy.id],undefined);
  player.x=enemy.x-70;assert.equal(game.canSee(0,enemy),true,'close proximity reveals brush');
});

test('visibility applies symmetrically and hidden enemy heroes do not enter bot plans',()=>{
  const {game,player}=arena('ember'),brush=WORLD.brush[0],bot=hero(game,'dragon',1,brush.x-240,brush.y);
  Object.assign(player,{x:brush.x,y:brush.y});bot.bot=true;game.rebuildGrid();
  assert.equal(game.canSee(1,player),false);game.time=70;game.planTeams();
  assert.notEqual(game.teamPlans[1].focusId,player.id);game.thinkBot(bot,DIFFICULTIES.hard);assert.notEqual(bot.brain.targetId,player.id);
  player.revealedUntil=game.time+2;assert.equal(game.canSee(1,player),true);
});

test('Maw has a delayed directional bite while Colossus creates staggered avoidable rocks',()=>{
  for(const index of [0,1]){
    const {game,player}=arena('dragon'),beast=game.spawnBeast(index);
    Object.assign(player,{x:beast.x+95,y:beast.y,hp:10000,maxHp:10000});Object.assign(beast,{aggroId:player.id,aggroUntil:10,slamIn:0,attackTimer:99});game.rebuildGrid();
    game.tickNeutral(beast,.05);const hp=player.hp;
    assert.equal(game.zones.length,index===0?1:3);assert.equal(game.zones[0].shape,index===0?'cone':'circle');
    game.tickZones(.1);assert.equal(player.hp,hp,'warning precedes damage');
    if(index===0){player.x=beast.x;player.y=beast.y-110;game.rebuildGrid();game.tickZones(.8);assert.equal(player.hp,hp,'sidestepping the cone avoids the bite');}
    else{assert(game.zones[0].tickIn<game.zones[1].tickIn&&game.zones[1].tickIn<game.zones[2].tickIn);player.y+=230;game.rebuildGrid();game.tickZones(2);assert.equal(player.hp,hp);}
  }
});

test('captured beast chooses its lane once before dispatch and keeps different siege roles',()=>{
  const {game,player}=arena('dragon');const neutral=game.spawnBeast(1);game.kill(neutral,player);
  const beast=game.units.find(u=>u.siege);assert(game.pendingBeastChoice);assert(game.chooseBeastLane(2));
  assert.equal(beast.lane,2);assert.equal(beast.dispatchIn,0);assert.equal(game.chooseBeastLane(0),false);
  const wave=game.addUnit('footman',0,beast.x+40,beast.y,{hp:300});game.rebuildGrid();beast.siegeIn=0;game.tickSiege(beast,.05);assert(wave.shield>0);
  const {game:other,player:killer}=arena('dragon');const maw=other.spawnBeast(0);other.kill(maw,killer);const captured=other.units.find(u=>u.siege);
  assert(captured.speed>beast.speed);other.time=other.pendingBeastChoice.expiresAt+.1;assert.equal(other.chooseBeastLane(2),false);
});

test('bots save for coherent advanced recipes instead of spending on unrelated starter items',()=>{
  const {game}=arena(),bot=hero(game,'seraph',1);bot.bot=true;bot.gold=100;
  assert(game.botShop(bot));assert.deepEqual(bot.items,['iron-edge']);
  bot.gold=120;assert.equal(game.botShop(bot),false,'save toward Tempered Blade');assert.equal(bot.gold,120);
  bot.gold=240;assert(game.botShop(bot));assert.equal(bot.items.at(-1),'tempered-blade');
  bot.gold=100;assert(game.botShop(bot));assert.equal(bot.items.at(-1),'feather-bow');
  bot.gold=600;assert(game.botShop(bot));assert.equal(bot.items.at(-1),'worldsplitter');assert.equal(bot.buildPlan[0],'worldsplitter');
});

test('later farming rewards grow only on personal kills or contributions and never generate idle income',()=>{
  const xp=unit=>Array.from({length:unit.level-1},(_,i)=>xpToNextLevel(i+1)).reduce((sum,n)=>sum+n,unit.xp);
  const {game,player}=arena('arc'),bystander=hero(game,'angel',0,1490),helper=hero(game,'dragon',0,1540);
  const early=game.addUnit('footman',1,1500,1100,{hp:100});game.damage(early,1000,player);
  assert.equal(xp(player),22);assert.equal(player.gold,18);
  game.time=660;const before=xp(player),late=game.addUnit('footman',1,1500,1100,{hp:100});game.damage(late,1,helper);game.damage(late,1000,player);
  assert.equal(xp(player)-before,88,'late wave reward reaches the capped4x value');assert.equal(xp(helper),44);
  assert.equal(player.gold,36);assert.equal(helper.gold,0);assert.equal(xp(bystander),0);assert.equal(bystander.gold,0);
  const saved={xp:xp(player),gold:player.gold};game.time+=600;
  assert.deepEqual({xp:xp(player),gold:player.gold},saved,'time alone grants nothing');
});

test('structures resist early unattended waves while heroes retain stronger siege contribution',()=>{
  const {game,player}=arena('arc'),tower=game.addUnit('tower',1,1650,1100,{hp:10000,tier:0,lane:1});
  const minion=game.addUnit('footman',0,1600,1100),summon=game.addUnit('summon',0,1610,1100,{ownerId:player.id});
  const heroHit=game.damage(tower,100,player),waveHit=game.damage(tower,100,minion),summonHit=game.damage(tower,100,summon);
  assert(Math.abs(heroHit-55)<1e-9);assert(waveHit<heroHit*.4);assert(summonHit<waveHit);
  game.time=600;assert.equal(game.damage(tower,100,player),100,'early fortification wears off naturally');
});

test('a lower-level bot takes a safe jungle route to earn its own catch-up farm',()=>{
  const {game,player}=arena('bulwark');player.bot=true;player.isPlayer=false;
  const first=hero(game,'arc',0,1350),second=hero(game,'ember',0,1300);first.level=7;second.level=8;
  const site=WORLD.camps.find(c=>c.id==='blue-wolves');
  const camp=game.addUnit('camp',-1,site.x,site.y,{hp:300,campId:site.id,homeX:site.x,homeY:site.y});
  game.time=110;game.teamPlans[0]={type:'push',lane:1};game.rebuildGrid();game.thinkBot(player,DIFFICULTIES.hard);
  assert.equal(player.brain.action,'jungle');assert.equal(player.brain.targetId,camp.id);assert.equal(player.gold,0);
});

test('both teams scout the same unknown camp regardless of hidden occupancy and remember visible emptiness',()=>{
  for(const team of [0,1]){
    const setup=empty=>{
      const game=new Game({hero:'arc',difficulty:'hard',seed:418});
      const bot=game.units.find(u=>u.kind==='hero'&&u.team===team);
      Object.assign(bot,{...WORLD.bases[team].well,bot:true,role:'jungler'});
      const site=[...WORLD.camps].filter(c=>c.team===team).sort((a,b)=>Math.hypot(a.x-bot.x,a.y-bot.y)-Math.hypot(b.x-bot.x,b.y-bot.y))[0];
      game.units=game.units.filter(u=>u===bot||(u.kind==='camp'&&(!empty||u.campId!==site.id)));
      game.time=110;game.teamPlans[team]={type:'lanes',lane:1};game.rebuildGrid();
      game.thinkBot(bot,DIFFICULTIES.hard);return {game,bot,site};
    };
    const occupied=setup(false),empty=setup(true),site=empty.site;
    assert.equal(occupied.game.canSee(team,occupied.game.units.find(u=>u.campId===site.id)),false);
    assert.deepEqual(empty.bot.brain,occupied.bot.brain,'unseen clears cannot change the scouting decision');
    assert.deepEqual(empty.bot.brain.goal,{x:site.x,y:site.y});assert.equal(empty.bot.brain.targetId,null);
    Object.assign(empty.bot,{x:site.x,y:site.y});empty.game.rebuildGrid();empty.game.thinkBot(empty.bot,DIFFICULTIES.hard);
    assert.notDeepEqual(empty.bot.brain.goal,{x:site.x,y:site.y},'seeing the empty clearing changes the route');
    const memory=empty.game.campStates.find(c=>c.id===site.id).scoutedEmptyUntil;
    assert.equal(memory[team],120);assert.equal(memory[1-team],0,'scouting memory is team-specific');
    Object.assign(empty.bot,WORLD.bases[team].well);empty.game.rebuildGrid();empty.game.thinkBot(empty.bot,DIFFICULTIES.hard);
    assert.notDeepEqual(empty.bot.brain.goal,{x:site.x,y:site.y},'the empty site remains remembered after leaving vision');
    empty.game.time=121;empty.game.thinkBot(empty.bot,DIFFICULTIES.hard);
    assert.deepEqual(empty.bot.brain.goal,{x:site.x,y:site.y},'expired observations permit scouting again without reading respawn state');
    Object.assign(occupied.bot,{x:site.x,y:site.y});occupied.game.rebuildGrid();occupied.game.thinkBot(occupied.bot,DIFFICULTIES.hard);
    assert(occupied.game.units.some(u=>u.id===occupied.bot.brain.targetId&&u.campId===site.id),'visible occupants can become attack targets');
  }
});

test('team jobs cover a threatened secondary lane and tanks peel for injured teammates',()=>{
  const {game,player}=arena('arc'),tank=hero(game,'bulwark',0,1500),support=hero(game,'angel',0,1440);
  tank.bot=support.bot=true;
  const tower=game.addUnit('tower',0,900,400,{lane:0,hp:1900});
  for(let i=0;i<4;i++)game.addUnit('footman',1,900+i*10,430,{lane:0});
  game.rebuildGrid();assignTeamJobs(game,0,{type:'push',lane:1});assert.equal(game.assignments[0][tank.id].type,'hold');
  game.assignments[0]={};player.hp=player.maxHp*.4;const enemy=hero(game,'ember',1,1540);game.teamPlans[0]={type:'push',lane:1};game.rebuildGrid();
  game.thinkBot(tank,DIFFICULTIES.hard);assert.equal(tank.brain.action,'peel');assert.equal(tank.brain.targetId,enemy.id);
  const old=strength(game,tank);tank.items=['ruby-heart','guardian-plate','sovereign-aegis'];game.refreshStats(tank);tank.hp=tank.maxHp;assert(strength(game,tank)>old);
});

test('consecutive stuns diminish without extending an existing stronger stun',()=>{
  const {game,player}=arena('dragon');game.applyStatus(player,'stun',1);assert.equal(player.status.stun.remaining,1);
  game.time=.9;player.status.stun.remaining=.1;game.applyStatus(player,'stun',1);assert.equal(player.status.stun.remaining,.55);
  game.time=5;delete player.status.stun;game.applyStatus(player,'stun',1);assert.equal(player.status.stun.remaining,1);
});

test('save/resume restores RNG, targets, projectiles, zones, delayed impacts, builds, cosmetics and timers',()=>{
  const {game,player}=arena('arc'),enemy=hero(game,'dragon',1,1660);enemy.hp=enemy.maxHp=10000;
  player.level=8;game.refreshStats(player);player.hp=player.maxHp;player.mana=player.maxMana;player.gold=700;
  player.cosmetics={armorTint:'#abc',trail:'stars',portrait:'veteran',victory:'glow'};game.matchId='resume-regression';
  game.buyItem(player,'ruby-heart');game.cast(player,1,{x:player.x,y:player.y});game.basicAttack(player,enemy);
  scheduleImpact(game,player,{kind:'winterfall',x:enemy.x,y:enemy.y,r:90,delay:.6,damage:20});
  const snapshot=game.serialize(),restored=Game.restore(JSON.parse(JSON.stringify(snapshot)));
  assert.deepEqual(restored.serialize(),snapshot);assert(restored.projectiles[0].hit instanceof Set);assert(restored.zones[0].applied instanceof Set);
  assert.equal(restored.matchId,game.matchId);assert.deepEqual(restored.player.cosmetics,player.cosmetics);
  for(let i=0;i<20;i++){game.update(.05);restored.update(.05);}
  assert.deepEqual(restored.serialize(),game.serialize(),'next simulation actions remain deterministic');
});

test('a real running match resumes with the same bot decisions and battle state',()=>{
  const game=new Game({hero:'arc',difficulty:'normal',seed:731});
  for(let i=0;i<240;i++)game.update(.25,i<20?{move:{x:1,y:0}}:{});
  const restored=Game.restore(JSON.parse(JSON.stringify(game.serialize())));
  for(let i=0;i<20;i++){game.update(.05);restored.update(.05);}
  assert.deepEqual(restored.serialize(),game.serialize());
});

test('corrupt, future-version, oversized and prototype-bearing saves are rejected safely',()=>{
  const game=new Game({seed:123});
  for(const alter of [s=>s.version=99,s=>s.state.units[0].hp=NaN,s=>s.state.playerId=-1,s=>s.state.stats.damage=null,s=>s.state.units.push(s.state.units[0]),s=>s.state.units.find(u=>u.kind==='hero').items=['unknown-item'],s=>s.state.scheduledImpacts=[{ownerId:999,executeAt:1,x:1,y:1,r:1}]]){
    const s=game.serialize();alter(s);assert.throws(()=>Game.restore(s),/Invalid saved match/);
  }
  assert.throws(()=>Game.restore('{"version":1,"state":{"__proto__":{}}}'),/Invalid saved match/);
  assert.throws(()=>Game.restore(' '.repeat(3_000_001)),/Invalid saved match/);
  for(const observation of [[0],[-1,0],[Infinity,0],['soon',0]]){
    const snapshot=game.serialize();snapshot.state.campStates[0].scoutedEmptyUntil=observation;
    assert.throws(()=>Game.restore(snapshot),/Invalid saved match/);
  }
});
