import test from 'node:test';
import assert from 'node:assert/strict';
import { Game } from '../game/moba-mania/engine.mjs';
import { HEROES, WORLD, DIFFICULTIES } from '../game/moba-mania/data.mjs';
import { tickProgression } from '../game/moba-mania/progression.mjs';
import { traceExpansionMovement } from '../game/moba-mania/hero-expansion.mjs';
import { selectTimeWardTarget } from '../game/moba-mania/solstice-runtime.mjs';

const close=(a,b,message='')=>assert(Math.abs(a-b)<1e-6,`${message}: ${a} != ${b}`);
const point=u=>({x:u.x,y:u.y});
const clone=v=>JSON.parse(JSON.stringify(v));
function arena(id,level=18){
  const game=new Game({hero:id,difficulty:'hard',seed:411}),hero=game.player;
  game.units=[hero];game.campStates=[];game.effects=[];game.waveIn=game.beastIn=game.planIn=1e8;
  Object.assign(hero,{x:1400,y:1100,level,attackTimer:0,lastDamageAt:0});game.refreshStats(hero);hero.hp=hero.maxHp;hero.mana=hero.maxMana;
  const add=(dx,dy=0,team=1,kind='footman')=>game.addUnit(kind,team,hero.x+dx,hero.y+dy,{hp:100000,r:17,speed:0,damage:0,range:20,attackTimer:1e8,name:'Practice target'});
  const addHero=(id='dragon',dx=140,dy=0,team=1)=>{const h=game.makeHero(id,team,1);Object.assign(h,{x:hero.x+dx,y:hero.y+dy,bot:false,hp:100000,maxHp:100000,speed:0,attackTimer:1e8,lastDamageAt:game.time});return h;};
  return {game,hero,add,addHero};
}
function advance(game,seconds){for(let left=seconds;left>1e-8;left-=.025)game.update(Math.min(.025,left));}
function cast(game,hero,slot,at=hero){hero.cooldowns[slot]=0;hero.mana=hero.maxMana;game.rebuildGrid();assert(game.cast(hero,slot,point(at)));}
function talent(game,hero,key){const tier=HEROES[hero.heroId].talents.find(t=>t.choices.some(c=>key in c.bonuses));assert(tier,`talent ${key} exists`);hero.talents[tier.level]=tier.choices.find(c=>key in c.bonuses).id;game.refreshStats(hero);}
const spellPower=(game,hero)=>(1+(hero.level-1)*.075)*(1+game.bonus(hero,'damage'))*(1+game.bonus(hero,'spellDamage'));
const shieldPower=(game,hero,n)=>n*(1+game.bonus(hero,'shield'))*(1+(hero.level-1)*.08);

test('all four heroes draft deterministically, and Impossible has four distinct enemies with ordinary stats',()=>{
  const seen=new Set(),lineups=new Set();
  for(const id of ['rivet','brinehook','vela','solstice'])for(let seed=1;seed<=24;seed++){
    const game=new Game({hero:id,difficulty:'impossible',seed:seed*7919}),again=new Game({hero:id,difficulty:'impossible',seed:seed*7919});
    const heroes=game.units.filter(u=>u.kind==='hero'),enemies=heroes.filter(u=>u.team===1);
    assert.equal(heroes.filter(u=>u.team===0).length,3);assert.equal(enemies.length,4);assert.equal(new Set(enemies.map(u=>u.heroId)).size,4);
    assert.deepEqual(enemies.map(u=>u.heroId),again.units.filter(u=>u.kind==='hero'&&u.team===1).map(u=>u.heroId));
    for(const enemy of enemies){seen.add(enemy.heroId);close(enemy.damage,HEROES[enemy.heroId].damage);close(enemy.maxHp,HEROES[enemy.heroId].hp);}
    lineups.add(enemies.map(u=>u.heroId).join(','));
    if(seed===1)assert.equal(Game.restore(game.serialize()).units.filter(u=>u.kind==='hero').length,7);
  }
  assert.equal(seen.size,12);assert(lineups.size>30);
  for(const difficulty of Object.keys(DIFFICULTIES).filter(d=>d!=='impossible'))assert.equal(new Game({difficulty,seed:2}).units.filter(u=>u.kind==='hero').length,6);
});

test('Screwshot hits once per target and its extra ricochet obeys visible nearest-target rules',()=>{
  const {game,hero,add}=arena('rivet');talent(game,hero,'rivetScrewDamage');talent(game,hero,'rivetDoubleThread');
  const first=add(110),hidden=add(135,25),second=add(205),third=add(300),normalSee=game.canSee.bind(game);
  game.canSee=(team,u)=>u.id!==hidden.id&&normalSee(team,u);
  cast(game,hero,0,{x:hero.x+430,y:hero.y});advance(game,1);
  const p=spellPower(game,hero);close(first.maxHp-first.hp,95*1.2*p);close(second.maxHp-second.hp,45*p);close(third.maxHp-third.hp,30*p);close(hidden.hp,hidden.maxHp);
  assert.equal(game.projectiles.length,0);
});

test('sentries assemble, hold position, honor explicit focus, replace oldest and credit only their owner',()=>{
  const {game,hero,add}=arena('rivet'),near=add(120),focus=add(200);
  cast(game,hero,1,{x:hero.x+30,y:hero.y});const first=game.units.find(u=>u.subtype==='sentry');
  advance(game,.55);close(near.hp,near.maxHp);assert.equal(game.projectiles.length,0);
  game.attackTarget(focus.id);hero.attackTimer=1e8;advance(game,.4);assert(focus.hp<focus.maxHp);close(near.hp,near.maxHp);close(first.x,1430);assert(first.noCollision);
  cast(game,hero,1,{x:hero.x+35,y:hero.y+35});cast(game,hero,1,{x:hero.x+35,y:hero.y-35});
  assert(!first.alive);assert.equal(game.units.filter(u=>u.alive&&u.subtype==='sentry').length,2);
  assert(game.effects.some(e=>e.type==='summon-depart'));
  const killer=game.units.find(u=>u.alive&&u.subtype==='sentry');focus.hp=1;game.attackTarget(focus.id);hero.attackTimer=1e8;const before=hero.gold;
  advance(game,1.4);assert(!focus.alive);assert.equal(hero.gold-before,18);assert.equal(hero.metrics.lastHits,1);
  assert(killer.alive);
});

test('Field Mechanic repairs only the most damaged owned sentry, without healing credit or gold',()=>{
  const {game,hero,add}=arena('rivet'),foe=add(80),otherRivet=game.makeHero('rivet',0,1);
  Object.assign(otherRivet,{x:hero.x+30,y:hero.y,bot:false});talent(game,hero,'rivetRepairPower');talent(game,hero,'rivetSharedTools');
  cast(game,hero,1,{x:hero.x+30,y:hero.y-30});cast(game,hero,1,{x:hero.x+30,y:hero.y+30});
  const devices=game.units.filter(u=>u.subtype==='sentry');devices[0].hp=100;devices[1].hp=200;
  const alien=game.addUnit('summon',0,hero.x+10,hero.y,{subtype:'sentry',ownerId:otherRivet.id,hp:1,maxHp:1000,attackTimer:1e8});
  game.rebuildGrid();hero.attackTimer=0;assert(game.basicAttack(hero,foe));game.tickProjectiles(.2);
  close(devices[0].hp,100+(10+hero.level)*1.4);close(devices[1].hp,200);close(alien.hp,1);close(hero.shield,shieldPower(game,hero,20));
  assert.equal(hero.metrics.healingDone,0);assert.equal(hero.gold,0);
  hero.attackTimer=0;game.basicAttack(hero,foe);game.tickProjectiles(.2);close(devices[0].hp,100+(10+hero.level)*1.4,'one repair per second');
});

test('mines have no vision, arm before triggering, obey two-device limit and never explode on expiry',()=>{
  const {game,hero,addHero}=arena('rivet'),enemy=addHero('dragon',100);
  talent(game,hero,'rivetMineRadius');talent(game,hero,'rivetSurvey');talent(game,hero,'rivetExitPlan');
  cast(game,hero,2,{x:hero.x+100,y:hero.y});const mine=game.units.find(u=>u.subtype==='mine');assert(mine.noVision);assert.equal(mine.triggerRadius,80);assert.equal(mine.blastRadius,100);
  hero.x=300;hero.y=300;advance(game,1);close(enemy.hp,enemy.maxHp);assert(mine.alive);assert(!game.canSee(0,enemy));assert(!game.pointVisible(0,mine.x,mine.y));
  hero.x=mine.x-200;hero.y=mine.y;advance(game,.05);assert(!mine.alive);assert(enemy.hp<enemy.maxHp);assert(enemy.status.slow);assert(enemy.revealedUntil>game.time);assert(hero.status.haste.factor>=.2);
  enemy.x=2600;enemy.y=1800;cast(game,hero,2,hero);cast(game,hero,2,hero);const oldest=game.units.filter(u=>u.alive&&u.subtype==='mine')[0];cast(game,hero,2,hero);
  assert(!oldest.alive);const remaining=game.units.filter(u=>u.alive&&u.subtype==='mine');assert.equal(remaining.length,2);
  game.effects=[];for(const device of remaining){device.lifetime=.01;game.applyStatus(device,'stun',1);}
  advance(game,.1);assert(remaining.every(u=>!u.alive));assert(!game.effects.some(e=>e.type==='shock-mine'));
});

test('Rivet sentry talents and all three Overclock capstones modify real existing devices',()=>{
  for(const capstone of ['rivetRedline','rivetFortified','rivetRelocation']){
    const {game,hero}=arena('rivet');talent(game,hero,'rivetSentryHealth');talent(game,hero,'rivetSentryHaste');talent(game,hero,capstone);
    cast(game,hero,1,{x:hero.x+40,y:hero.y});const first=game.units.find(u=>u.subtype==='sentry');close(first.maxHp,230*1.2*(1+17*.08));close(first.attackRate,.9/1.15);
    hero.cooldowns[1]=9;cast(game,hero,3);assert(first.status.haste.attack===.5);assert(hero.status.haste.attack===.2);assert(hero.overclockUntil===game.time+6);
    if(capstone==='rivetRedline')assert.equal(first.overclockDamage,.2);
    if(capstone==='rivetFortified')close(first.shield,shieldPower(game,hero,100));
    if(capstone==='rivetRelocation'){close(hero.cooldowns[1],5);assert.equal(hero.status.haste.factor,.2);}
    cast(game,hero,1,{x:hero.x-30,y:hero.y});const newer=game.units.filter(u=>u.subtype==='sentry').at(-1);assert(!newer.status.haste);assert.equal(newer.overclockUntil,0);
  }
  const {game,hero,add}=arena('rivet');talent(game,hero,'rivetEmergencyPatch');cast(game,hero,1,hero);const sentry=game.units.find(u=>u.subtype==='sentry'),enemy=add(70);
  game.damage(sentry,sentry.maxHp*.7,enemy);assert(sentry.emergencyPatchUsed);assert(sentry.shield>0);sentry.shield=0;sentry.shieldLayers=[];game.damage(sentry,1,enemy);assert.equal(sentry.shield,0);
});

test('Crossfire requires a current explicit focus and recharges per sentry',()=>{
  const {game,hero,add}=arena('rivet'),enemy=add(80);talent(game,hero,'rivetCrossfire');cast(game,hero,1,hero);const sentry=game.units.find(u=>u.subtype==='sentry');
  game.rebuildGrid();sentry.attackTimer=0;game.basicAttack(sentry,enemy);game.tickProjectiles(.2);const base=enemy.maxHp-enemy.hp;
  game.attackTarget(enemy.id);sentry.attackTimer=0;game.basicAttack(sentry,enemy);game.tickProjectiles(.2);close(enemy.maxHp-enemy.hp,base*2+sentry.crossfireDamage);
  sentry.attackTimer=0;game.basicAttack(sentry,enemy);game.tickProjectiles(.2);close(enemy.maxHp-enemy.hp,base*3+sentry.crossfireDamage);
});

test('Anchorline warns, hits the first visible body and rewards only actual hero displacement',()=>{
  const {game,hero,add,addHero}=arena('brinehook'),interceptor=add(160),enemy=addHero('vela',260);
  cast(game,hero,0,{x:hero.x+440,y:hero.y});advance(game,.24);close(interceptor.hp,interceptor.maxHp);close(hero.shield,0);
  advance(game,.35);assert(interceptor.hp<interceptor.maxHp);assert(interceptor.x<hero.x+80);close(enemy.hp,enemy.maxHp);close(hero.shield,0,'minion displacement gives no passive shield');
  interceptor.alive=false;enemy.x=hero.x+180;talent(game,hero,'brineMarkedCatch');talent(game,hero,'brineCalmWaters');hero.hp-=100;
  cast(game,hero,0,enemy);advance(game,.6);assert(enemy.x<hero.x+90);assert(enemy.status.stun);close(hero.shield,shieldPower(game,hero,80));assert(enemy.revealedUntil>=game.time+2);close(hero.hp,hero.maxHp-65);
});

test('pulls stop at terrain and camp leash, never displace beasts, and share a half-second recovery',()=>{
  const {game,hero,add}=arena('brinehook'),obstacle=WORLD.obstacles[1],enemy=add(100);
  Object.assign(enemy,{x:obstacle.x+obstacle.r+enemy.r+20,y:obstacle.y});const start=enemy.x;
  const move=traceExpansionMovement(game,enemy,Math.PI,300,{pull:true});assert(move.travelled>0&&move.travelled<25);assert(enemy.x>obstacle.x+obstacle.r+enemy.r);assert(enemy.x<start);
  const after=enemy.x;traceExpansionMovement(game,enemy,0,100,{pull:true});close(enemy.x,after,'recovery prevents a second immediate displacement');
  const camp=game.addUnit('camp',-1,1400,1100,{hp:1000,r:14,homeX:1150,homeY:1100});traceExpansionMovement(game,camp,0,130,{pull:true});assert(camp.x-camp.homeX<270);
  const beast=game.addUnit('beast',-1,1500,1100,{hp:1000,r:34}),before=point(beast);traceExpansionMovement(game,beast,Math.PI,130,{pull:true});assert.deepEqual(point(beast),before);
});

test('a new projectile hits a target before a wall and never tunnels to a target behind it',()=>{
  const {game,hero,add}=arena('brinehook'),rock=WORLD.obstacles.find(o=>o.r>40)||WORLD.obstacles[0];
  Object.assign(hero,{x:rock.x-rock.r-180,y:rock.y});const near=add(90),behind=add(rock.r*2+240);
  cast(game,hero,0,{x:hero.x+440,y:hero.y});game.time=.25;tickProgression(game,0);game.rebuildGrid();game.tickProjectiles(.5);
  assert(near.hp<near.maxHp,'the collision before terrain resolves first');close(behind.hp,behind.maxHp);assert.equal(hero.shield,0);
  near.alive=false;cast(game,hero,0,{x:hero.x+440,y:hero.y});game.time+=.25;tickProgression(game,0);game.rebuildGrid();game.tickProjectiles(.5);close(behind.hp,behind.maxHp);
});

test('Sea Legs cleanses current slow layers, and Undertow never repeats its damage or pull',()=>{
  const {game,hero,addHero}=arena('brinehook'),ally=addHero('vela',30,0,0),enemy=addHero('dragon',230);
  talent(game,hero,'brineSeaShield');talent(game,hero,'brineCrewCover');game.applyStatus(hero,'slow',3,{factor:.2});game.applyStatus(hero,'slow',1,{factor:.5});hero.ccRecoveryUntil=9;
  cast(game,hero,1);assert(!hero.status.slow);close(hero.shield,shieldPower(game,hero,180)*1.25);close(ally.shield,shieldPower(game,hero,60));assert.equal(hero.ccRecoveryUntil,9);
  game.applyStatus(hero,'slow',1,{factor:.3});assert(hero.status.slow);
  talent(game,hero,'brinePoolRadius');talent(game,hero,'brineRoughCurrent');const center={x:hero.x+180,y:hero.y};cast(game,hero,2,center);assert.equal(game.scheduledImpacts.at(-1).r,130);
  advance(game,.59);close(enemy.hp,enemy.maxHp);advance(game,.05);const hp=enemy.hp,x=enemy.x;assert(hp<enemy.maxHp);assert(x<hero.x+210);
  advance(game,1);close(enemy.hp,hp);close(enemy.x,x);assert(game.zones.some(z=>z.type==='rough-current'));
});

test('Captain’s Wake gathers sideways and its three capstones retain the warning and control times',()=>{
  for(const key of ['brineGrandWake','brineSafeHarbor','brineBoardingParty']){
    const {game,hero,addHero}=arena('brinehook');talent(game,hero,key);const enemy=addHero('vela',180,45),ally=addHero('rivet',130,0,0),x=enemy.x;
    cast(game,hero,3,{x:hero.x+360,y:hero.y});const impact=game.scheduledImpacts.at(-1);close(impact.delay,.75);close(impact.width,key==='brineGrandWake'?60:50);
    advance(game,.74);close(enemy.hp,enemy.maxHp);close(ally.shield,0);advance(game,.04);assert(enemy.hp<enemy.maxHp);close(enemy.x,x);close(enemy.y,hero.y);assert(enemy.status.stun);
    if(key==='brineSafeHarbor')assert(ally.shield>0);
    if(key==='brineBoardingParty')assert.equal(ally.status.haste.attack,.2);
  }
});

test('Vela launches six shots, applies the sixth bonus once, blocks attacks during reload and resumes without a free shot',()=>{
  const {game,hero,add}=arena('vela'),enemy=add(60);game.rebuildGrid();
  for(let n=0;n<6;n++){hero.attackTimer=0;assert(game.basicAttack(hero,enemy));assert.equal(hero.magazine.rounds,5-n);game.tickProjectiles(.15);}
  close(enemy.maxHp-enemy.hp,hero.damage*6.2);assert(hero.magazine.reloadRemaining>1.3);hero.attackTimer=0;assert(!game.basicAttack(hero,enemy));
  const before=enemy.hp;advance(game,1.35);assert(hero.magazine.reloadRemaining>0);close(enemy.hp,before);advance(game,.06);assert.equal(hero.magazine.rounds,6);assert.equal(hero.magazine.reloadRemaining,0);close(enemy.hp,before);
  hero.attackTimer=0;assert(game.basicAttack(hero,enemy));assert.equal(hero.magazine.rounds,5);
});

test('Sidestep restores rounds or shortens reload without touching the basic timer, and Steady Aim expires cleanly',()=>{
  const {game,hero}=arena('vela');talent(game,hero,'velaCleanChambers');talent(game,hero,'velaSafeLanding');hero.magazine.rounds=3;hero.attackTimer=.61;
  cast(game,hero,1,{x:hero.x+160,y:hero.y});assert.equal(hero.magazine.rounds,5);close(hero.attackTimer,.61);assert(hero.shield>0);
  hero.magazine.rounds=0;hero.magazine.reloadRemaining=1;cast(game,hero,1,{x:hero.x-140,y:hero.y});close(hero.magazine.reloadRemaining,.55);assert.equal(hero.magazine.rounds,0);close(hero.attackTimer,.61);
  talent(game,hero,'velaPatientAim');const range=hero.range;cast(game,hero,2);close(hero.range,range*1.15);close(game.speed(hero),hero.speed*.95);assert.equal(hero.status.haste.attack,.2);
  advance(game,4.05);close(hero.range,range);close(game.speed(hero),hero.speed);
});

test('Split Shot uses no ammunition and its second same-victim hit uses the reduced damage',()=>{
  const {game,hero,add}=arena('vela'),enemy=add(160);enemy.r=30;talent(game,hero,'velaSplitDamage');talent(game,hero,'velaCoveringShot');hero.magazine.rounds=4;
  cast(game,hero,0,enemy);advance(game,.4);close(enemy.maxHp-enemy.hp,100*spellPower(game,hero)*1.2);assert.equal(hero.magazine.rounds,4);assert(enemy.status.slow);
  const pair=hero.expansion.splitPairs[0];assert.equal(pair.hits.length,1);assert(pair.firstHit);
});

test('Silver Volley fires exactly six fixed-direction bullets and each cancellation stops unlaunched shots',()=>{
  for(const cancel of [null,'move','stun','cast']){
    const {game,hero,add}=arena('vela'),first=add(150),behind=add(300);let launches=0;const fire=game.fireSkill.bind(game);
    game.fireSkill=(owner,angle,props)=>{if(props.abilityId==='silver-volley')launches++;return fire(owner,angle,props);};
    cast(game,hero,3,behind);assert(hero.channel);advance(game,.39);assert.equal(launches,0);close(first.hp,first.maxHp);advance(game,.12);assert.equal(launches,1);
    if(cancel==='move')game.update(.025,{move:{x:0,y:1}});
    if(cancel==='stun')game.applyStatus(hero,'stun',.4,{sourceId:first.id});
    if(cancel==='cast')cast(game,hero,0,behind);
    advance(game,1.5);assert.equal(launches,cancel?1:6);assert(!hero.channel);assert.equal(hero.magazine.rounds,6);close(behind.hp,behind.maxHp,'minions intercept every line shot');
    if(!cancel)close(first.maxHp-first.hp,6*45*spellPower(game,hero));
  }
});

test('Vela talents affect reload cadence, sixth hits, actual kill refunds and low-health escape only',()=>{
  const {game,hero,addHero}=arena('vela'),enemy=addHero('rivet',60);talent(game,hero,'velaFastHands');talent(game,hero,'velaHeavySixth');talent(game,hero,'velaPerfectRhythm');hero.magazine.rounds=1;game.rebuildGrid();
  assert(game.basicAttack(hero,enemy));close(hero.magazine.reloadRemaining,1.2);game.tickProjectiles(.15);close(enemy.maxHp-enemy.hp,hero.damage*1.4);close(hero.shield,shieldPower(game,hero,70));
  talent(game,hero,'velaOpeningPair');advance(game,1.21);assert.equal(hero.magazine.openingShots,2);hero.attackTimer=0;game.basicAttack(hero,enemy);close(hero.attackTimer,hero.attackRate*.85);
  talent(game,hero,'velaSpareMagazine');hero.magazine.rounds=0;hero.magazine.reloadRemaining=1;enemy.hp=1;game.damage(enemy,1000,hero);close(hero.magazine.reloadRemaining,.5);
  const other=addHero('brinehook',100);hero.magazine.reloadRemaining=1;other.hp=1;game.damage(other,1000,hero);close(hero.magazine.reloadRemaining,1,'kill refund recharges for six seconds');
  talent(game,hero,'velaNeverCornered');hero.shield=0;hero.shieldLayers=[];hero.cooldowns[1]=7;hero.hp=hero.maxHp*.31;const attacker=addHero('dragon',110);game.damage(hero,hero.maxHp*.02,attacker);close(hero.cooldowns[1],4);assert.equal(hero.status.haste.factor,.25);
  game.damage(hero,1,attacker);close(hero.cooldowns[1],4);game.damage(hero,1e6,attacker);assert.equal(hero.magazine.rounds,6);assert.equal(hero.magazine.reloadRemaining,0);
});

test('Time Ward targets a real ally, tracks absorbed and actual health damage, and heals once at expiry',()=>{
  const {game,hero,addHero}=arena('solstice'),ally=addHero('rivet',100,0,0),enemy=addHero('dragon',160);const invalid={x:hero.x,y:hero.y+300},mana=hero.mana;
  assert.equal(selectTimeWardTarget(game,hero,point(ally)),ally);assert(!game.cast(hero,1,invalid));close(hero.mana,mana);
  hero.cooldowns[0]=5;cast(game,hero,1,ally);const ward=ally.timeWard,remainingMana=hero.mana;
  game.damage(ally,30,enemy);assert.equal(ward.absorbed,30);close(hero.mana,remainingMana+8);close(hero.cooldowns[0],4);assert(ward.triggered);assert.equal(ally.timeWard.healthLost,0);
  game.damage(ally,ally.shield+200,enemy);close(ward.healthLost,200);const wounded=ally.hp;
  advance(game,3.02);assert(!ally.timeWard);close(ally.hp,wounded+50);assert(hero.metrics.healingDone>=50);assert(hero.metrics.shieldingDone>=120);assert.equal(hero.gold,0);
  const after=ally.hp;advance(game,.5);close(ally.hp,after);
});

test('overwritten, late-damaged, shield-overlapped and dead Time Wards cannot farm Perfect Timing or heal',()=>{
  const {game,hero,addHero}=arena('solstice'),ally=addHero('vela',100,0,0),enemy=addHero('dragon',170);
  game.shield(ally,1000,6,enemy);cast(game,hero,1,ally);const mana=hero.mana;game.damage(ally,40,enemy);assert.equal(ally.timeWard.absorbed,0);close(hero.mana,mana);
  ally.shield=0;ally.shieldLayers=[];cast(game,hero,1,ally);game.damage(ally,ally.shield+120,enemy);const wounded=ally.hp;cast(game,hero,1,ally);advance(game,3.02);close(ally.hp,wounded,'recast discards previous delayed healing');
  cast(game,hero,1,ally);advance(game,1.05);hero.cooldowns[0]=4;const lateMana=hero.mana;game.damage(ally,40,enemy);close(hero.mana,lateMana);close(hero.cooldowns[0],4,'late absorption gives no refund');
  game.damage(ally,1e6,enemy);assert(!ally.alive);assert(!ally.timeWard);advance(game,3.1);assert(!ally.alive);close(ally.hp,0);
});

test('Unwind removes every current slow and only half a second of stun without resetting recovery or damage-over-time',()=>{
  const {game,hero,addHero}=arena('solstice'),ally=addHero('rivet',120,0,0),enemy=addHero('dragon',200);talent(game,hero,'solsticeQuickRecovery');
  game.applyStatus(ally,'slow',4,{factor:.2});game.applyStatus(ally,'slow',1,{factor:.5});game.applyStatus(ally,'stun',.9,{sourceId:enemy.id});game.applyStatus(ally,'poison',3,{dps:3,sourceId:enemy.id});const recovery=ally.ccRecoveryUntil;
  cast(game,hero,2,ally);assert(!ally.status.slow);close(ally.status.stun.remaining,.4);assert.equal(ally.ccRecoveryUntil,recovery);assert(ally.status.poison);assert(ally.shield>0);assert.equal(ally.status.haste.factor,.15);
  game.applyStatus(ally,'slow',1,{factor:.3});assert(ally.status.slow);game.applyStatus(ally,'stun',.9,{sourceId:enemy.id});assert(ally.ccResilient);
});

test('Borrowed Seconds refunds only the first three slots once, respecting base cooldown limits and the floor',()=>{
  const {game,hero,addHero}=arena('solstice'),ally=addHero('angel',100,0,0),enemy=addHero('dragon',130);talent(game,hero,'solsticeGenerousSeconds');
  ally.cooldowns=[10,8,.8,17];ally.evolution.readyAt={item:20};cast(game,hero,3,ally);const ultimate=hero.cooldowns[3];game.time=.7;game.rebuildGrid();tickProgression(game,0);
  assert.deepEqual(ally.cooldowns,[8,5.5,.6,17]);assert.equal(hero.cooldowns[3],ultimate);assert.equal(ally.evolution.readyAt.item,20);assert(enemy.hp<enemy.maxHp);assert.equal(enemy.status.slow.factor,.35);
  const cooldowns=[...ally.cooldowns];tickProgression(game,0);assert.deepEqual(ally.cooldowns,cooldowns);
});

test('Hourhand talent aftermath uses the actual impact point, with a warned echo and harmless occupied-space slow',()=>{
  const {game,hero,add}=arena('solstice'),target=add(120);talent(game,hero,'solsticeSandDuration');talent(game,hero,'solsticeLingeringSand');talent(game,hero,'solsticeGlassEcho');
  cast(game,hero,0,target);advance(game,.25);assert(target.status.slow);assert(target.status.slow.remaining>2.5);const echo=game.scheduledImpacts.find(i=>i.kind==='glass-echo');assert(echo);close(echo.x,target.x);close(echo.delay,.6);
  const afterFirst=target.hp;advance(game,echo.executeAt-game.time-.05);close(target.hp,afterFirst);advance(game,.1);close(afterFirst-target.hp,45*spellPower(game,hero));
  const entrant=add(130,35);game.rebuildGrid();advance(game,.15);assert(entrant.status.slow);close(entrant.hp,entrant.maxHp,'sand causes no damage');
});

test('Time Ward talent success retains the passive recharge and snapshots healing and early damage reduction',()=>{
  const {game,hero,addHero}=arena('solstice'),ally=addHero('vela',100,0,0),other=addHero('rivet',130,20,0),enemy=addHero('dragon',180);
  talent(game,hero,'solsticeWardPower');talent(game,hero,'solsticePreciseTiming');talent(game,hero,'solsticeSharedMoment');talent(game,hero,'solsticeSteadyHour');hero.cooldowns[0]=5;
  cast(game,hero,1,ally);close(ally.shield,shieldPower(game,hero,120)*1.25);const mana=hero.mana;game.damage(ally,40,enemy);close(ally.timeWard.absorbed,36);close(hero.mana,mana+12);assert(other.shield>0||hero.shield>0);
  const cooldown=hero.cooldowns[0];cast(game,hero,1,ally);game.damage(ally,40,enemy);close(hero.cooldowns[0],cooldown,'recasting does not bypass passive recharge');
});

test('active devices, split shots, reloads, channels, wards and delayed impacts resume deterministically',()=>{
  for(const id of ['rivet','brinehook','vela','solstice']){
    const {game,hero,add,addHero}=arena(id),enemy=add(180);addHero('dragon',190,45);const ally=addHero('angel',90,50,0);
    if(id==='rivet'){cast(game,hero,1,hero);cast(game,hero,2,{x:hero.x+260,y:hero.y});cast(game,hero,0,enemy);}
    if(id==='brinehook'){cast(game,hero,0,enemy);cast(game,hero,2,enemy);}
    if(id==='vela'){hero.magazine.rounds=0;hero.magazine.reloadRemaining=1.2;cast(game,hero,0,enemy);cast(game,hero,3,enemy);}
    if(id==='solstice'){cast(game,hero,1,ally);game.damage(ally,50,enemy);cast(game,hero,3,enemy);}
    const restored=Game.restore(game.serialize());advance(game,2);advance(restored,2);assert.deepEqual(restored.serialize(),game.serialize(),id);
  }
});

test('restore rejects malformed expansion state and accepts older optional fields and long-dead structures',()=>{
  const {game,hero,add}=arena('vela'),legacy=clone(game.serialize());delete legacy.state.units.find(u=>u.id===hero.id).magazine;
  assert.equal(Game.restore(legacy).player.magazine.rounds,6);
  const invalid=clone(game.serialize());invalid.state.units.find(u=>u.id===hero.id).magazine.rounds=7;assert.throws(()=>Game.restore(invalid),/magazine/);
  const tower=add(200,0,1,'tower');tower.tier=0;game.damage(tower,1e8,hero);advance(game,20);assert.equal(tower.deathTime,0);assert.doesNotThrow(()=>Game.restore(game.serialize()));
});
