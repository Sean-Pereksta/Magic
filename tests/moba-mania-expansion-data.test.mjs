import test from 'node:test';
import assert from 'node:assert/strict';
import { HEROES, DIFFICULTIES } from '../game/moba-mania/data.mjs';
import { Game } from '../game/moba-mania/engine.mjs';
import { ITEM_BY_ID } from '../game/moba-mania/items.mjs';
import { normalizeProfile, heroCosmetics, recommendedItems } from '../game/moba-mania/profile.mjs';
import { canSee, assignTeamJobs, nextBuildItem, chooseBotTalent, expansionBotAim, expansionBotMovement, expansionTargetBias } from '../game/moba-mania/strategy.mjs';

const expansion=['rivet','brinehook','vela','solstice'];
function arena(id){
  const game=new Game({hero:id,difficulty:'hard',seed:740}),hero=game.player;
  game.units=[hero];game.campStates=[];game.waveIn=game.beastIn=game.planIn=1e8;
  Object.assign(hero,{x:1400,y:1100,level:18,attackTimer:0,bot:false});game.refreshStats(hero);hero.hp=hero.maxHp;hero.mana=hero.maxMana;
  game.rebuildGrid();return {game,hero};
}
function addHero(game,id,team,x,y=1100){const h=game.makeHero(id,team,1);Object.assign(h,{x,y,bot:false,level:18});game.refreshStats(h);h.hp=h.maxHp;game.rebuildGrid();return h;}
const aim=(game,hero,slot,target=null,retreat=false)=>expansionBotAim(game,hero,game.getAbility(hero,slot),target,retreat,DIFFICULTIES.hard);

test('the expansion supplies four complete identities and84 exclusive talents without changing controls',()=>{
  assert.equal(Object.keys(HEROES).length,12);
  const abilityIds=new Set(),talentIds=new Set();let count=0;
  for(const hero of Object.values(HEROES)){
    assert.deepEqual(hero.abilities.map(a=>a.unlock),[1,2,4,6]);
    assert.deepEqual(hero.talents.map(t=>t.level),[3,5,8,10,12,15,18]);
    for(const a of hero.abilities){assert(!abilityIds.has(a.id));abilityIds.add(a.id);}
    for(const tier of hero.talents){assert.equal(tier.choices.length,3);for(const choice of tier.choices){assert(!talentIds.has(choice.id));talentIds.add(choice.id);assert(Object.values(choice.bonuses).every(n=>Number.isFinite(n)&&n>0));}}
    if(!expansion.includes(hero.id))continue;
    assert(hero.passive.name&&hero.passive.description&&hero.description&&hero.title);
    for(const a of hero.abilities){assert.equal(a.icon,a.id);assert(['copper','tide','ivory','time'].includes(a.theme));assert(a.cooldown>0&&a.mana>0);}
    count+=hero.talents.reduce((n,t)=>n+t.choices.length,0);
  }
  assert.equal(count,84);assert.equal(talentIds.size,252);assert.equal(abilityIds.size,48);
  assert.equal(HEROES.solstice.abilities[1].type,'ally');
  assert(HEROES.vela.abilities[1].mobility);assert(!HEROES.brinehook.abilities.some(a=>a.mobility));
});

test('each new hero starts a normal six-combatant match and keeps one choice per tier',()=>{
  for(const id of expansion){
    const game=new Game({hero:id,seed:27});assert.equal(game.player.heroId,id);assert.equal(game.units.filter(u=>u.kind==='hero').length,6);
    const h=game.player;h.level=18;game.refreshStats(h);
    for(const tier of HEROES[id].talents){assert(game.chooseTalent(h,tier.level,tier.choices[0].id));assert(!game.chooseTalent(h,tier.level,tier.choices[1].id));}
    assert.equal(Object.keys(h.talents).length,7);
  }
});

test('old mastery profiles retain progress while adding clean cosmetic identities for all four heroes',()=>{
  const old={schema:1,heroes:{arc:{xp:360,matches:8,wins:4,equipped:'astral'},ranger:{xp:150,matches:3,wins:1,equipped:'verdant'}},completed:['old-match']};
  const profile=normalizeProfile(old);
  assert.equal(profile.heroes.arc.xp,360);assert.equal(profile.heroes.arc.equipped,'astral');assert.deepEqual(profile.completed,['old-match']);
  for(const id of expansion){assert.deepEqual(profile.heroes[id],{xp:0,matches:0,wins:0,equipped:'classic'});assert.equal(heroCosmetics(profile,id).portrait,'classic');}
  assert.deepEqual(old.heroes.arc,{xp:360,matches:8,wins:4,equipped:'astral'});
});

test('new bot builds complete valid dependency-first recipes and choose coherent talents',()=>{
  const cores={rivet:'elderwood-staff',brinehook:'sovereign-aegis',vela:'thunderbolt-crossbow',solstice:'phoenix-crown'};
  for(const id of expansion){
    const hero={heroId:id,items:[]};
    for(let i=0;i<16&&!hero.items.includes(cores[id]);i++){
      const next=nextBuildItem(hero);assert(next,id);assert(next.requires.every(required=>hero.items.includes(required)),`${id}: prerequisite before ${next.id}`);hero.items.push(next.id);
    }
    assert(hero.items.includes(cores[id]));assert.equal(hero.buildPlan[0],cores[id]);
    const recommendations=recommendedItems(hero);assert.equal(recommendations.length,3);assert(recommendations.every(item=>ITEM_BY_ID[item.id]?.tier===3));assert(recommendations.some(item=>item.id===cores[id]));
    for(const tier of HEROES[id].talents)assert(tier.choices.includes(chooseBotTalent(hero,tier)));
  }
});

test('new team jobs express defense, catch, marksman and reactive support',()=>{
  const types={rivet:'defend',brinehook:'catch',vela:'marksman',solstice:'support'};
  for(const id of expansion){const {game,hero}=arena(id);hero.bot=true;assignTeamJobs(game,0,{type:'lanes',lane:1});assert.equal(game.assignments[0][hero.id].type,types[id]);}
});

test('a visible mine never supplies sight while an ordinary sentry scouts normally',()=>{
  const {game,hero}=arena('rivet');hero.x=700;
  const enemy=addHero(game,'arc',1,1600),mine=game.addUnit('summon',0,1550,1100,{subtype:'mine',hp:70,ownerId:hero.id,noVision:true,stationary:true});
  game.rebuildGrid();assert.equal(canSee(game,0,enemy),false);assert.equal(canSee(game,0,mine),true);
  const sentry=game.addUnit('summon',0,1550,1100,{subtype:'sentry',hp:230,ownerId:hero.id,stationary:true});game.rebuildGrid();assert.equal(canSee(game,0,enemy),true);
  sentry.alive=false;assert.equal(canSee(game,0,enemy),false);
});

test('Brinehook checks the complete visible hook line and avoids chaining an already held target',()=>{
  const {game,hero}=arena('brinehook'),enemy=addHero(game,'arc',1,1680);
  assert(aim(game,hero,0,enemy));
  const blocker=game.addUnit('footman',1,1540,1100,{hp:200,r:12});game.rebuildGrid();assert.equal(aim(game,hero,0,enemy),null,'a minion intercepts the hook');
  blocker.alive=false;game.rebuildGrid();assert(aim(game,hero,0,enemy));
  game.applyStatus(enemy,'stun',1,{sourceId:hero.id});assert.equal(aim(game,hero,0,enemy),null);
  delete enemy.status.stun;enemy.x=2400;game.rebuildGrid();assert.equal(aim(game,hero,0,enemy),null);assert.equal(expansionTargetBias(game,hero,enemy),0,'hidden candidates have no preference');
});

test('Vela holds a fixed channel and only strafes during a real reload',()=>{
  const {game,hero}=arena('vela'),enemy=addHero(game,'dragon',1,1620);
  const original={x:hero.x,y:hero.y};hero.channel={kind:'silver-volley'};
  assert(expansionBotMovement(game,hero,enemy,DIFFICULTIES.hard,.05));assert.deepEqual({x:hero.x,y:hero.y},original);
  delete hero.channel;hero.magazine={rounds:6,reloadRemaining:0};assert.equal(expansionBotMovement(game,hero,enemy,DIFFICULTIES.hard,.05),false);
  hero.magazine={rounds:0,reloadRemaining:1};assert(expansionBotMovement(game,hero,enemy,DIFFICULTIES.hard,.05));assert(hero.x<original.x,'reload movement creates distance');
  assert(aim(game,hero,1,enemy,true),'an exposed reload gets a defensive sidestep');
});

test('Solstice bot wards observed danger, cleanses current impairment and values only refundable slots',()=>{
  const {game,hero}=arena('solstice'),ally=addHero(game,'ranger',0,1430),enemy=addHero(game,'arc',1,1850);ally.hp=ally.maxHp*.45;
  assert.equal(aim(game,hero,1),null,'missing health alone is not imminent damage');
  enemy.x=1600;enemy.targetId=ally.id;game.rebuildGrid();assert.deepEqual(aim(game,hero,1),{x:ally.x,y:ally.y});
  assert.equal(aim(game,hero,2),null);game.applyStatus(ally,'slow',2,{factor:.3,sourceId:enemy.id});assert.deepEqual(aim(game,hero,2),{x:ally.x,y:ally.y});
  enemy.alive=false;delete ally.status.slow;game.rebuildGrid();hero.cooldowns=[0,0,0,20];ally.cooldowns=[0,0,0,20];assert.equal(aim(game,hero,3),null,'an ultimate timer is never refunded');
  ally.cooldowns=[5,7,9,20];assert(aim(game,hero,3),'meaningful first-three-slot cooldowns justify the cast');
});
