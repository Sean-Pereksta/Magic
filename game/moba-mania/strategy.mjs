import { HEROES, WORLD } from './data.mjs';
import { ITEM_BY_ID } from './items.mjs';

const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const structure=u=>u.kind==='tower'||u.kind==='core';
export const makeMetrics=()=>({damageDealt:0,damageToHeroes:0,damageToStructures:0,damageTaken:0,healingDone:0,shieldingDone:0,assists:0,heroKills:0,deaths:0,goldEarned:0,lastHits:0,minionLastHits:0,jungleLastHits:0,towersDestroyed:0,beastsCaptured:0,bestStreak:0,shutdowns:0});
export const brushAt=unit=>WORLD.brush.findIndex(p=>distance(unit,p)<p.r);

/** Both teams use the same information. Terrain/structures are known; units need sight. */
export function canSee(game,team,unit){
  if(!unit||!(team===0||team===1))return false;
  if(unit.team===team)return true;
  if(!unit.alive)return false;
  if(structure(unit))return true;
  const brush=brushAt(unit),revealed=(unit.revealedUntil||0)>game.time;
  return game.units.some(observer=>{
    if(!observer.alive||observer.team!==team)return false;
    const radius=observer.kind==='hero'?580:structure(observer)?520:observer.kind==='summon'?360:observer.siege?430:300;
    const d=distance(observer,unit);
    if(d>radius+unit.r)return false;
    return brush<0||revealed||d<=95||brushAt(observer)===brush;
  });
}

export function updateVision(game){
  game.lastSeen??=[{},{}];
  for(const team of [0,1]){
    for(const unit of game.units)if(unit.kind==='hero'&&unit.team!==team&&canSee(game,team,unit)){
      game.lastSeen[team][unit.id]={id:unit.id,heroId:unit.heroId,team:unit.team,x:unit.x,y:unit.y,time:game.time};
    }
    for(const [id,seen] of Object.entries(game.lastSeen[team]))if(game.time-seen.time>8)delete game.lastSeen[team][id];
  }
}

/** Camp locations are public; occupancy is learned only by scouting their clearings. */
export function scoutJungle(game,team){
  const sight=game.visionSources(team);
  const visible=game.units.filter(u=>u.alive&&u.kind==='camp'&&canSee(game,team,u));
  return WORLD.camps.filter(site=>site.team===team).flatMap(site=>{
    const state=game.campStates.find(c=>c.id===site.id),targets=visible.filter(u=>u.campId===site.id);
    // The camp spawn clearings have no brush. Require sight of the whole clearing,
    // rather than one edge, before remembering that there was nothing to fight.
    const emptySeen=!targets.length&&sight.some(observer=>distance(observer,site)+60<=observer.r);
    let until=state?.scoutedEmptyUntil?.[team];
    if(!Number.isFinite(until)||until>game.time+10)until=0;
    if(targets.length)until=0;
    else if(emptySeen)until=game.time+10;
    if(state){
      if(!Array.isArray(state.scoutedEmptyUntil)||state.scoutedEmptyUntil.length!==2)state.scoutedEmptyUntil=[0,0];
      state.scoutedEmptyUntil[team]=until;
    }
    return emptySeen||until>game.time?[]:[{...site,targets}];
  });
}

/** A compact fighting-strength estimate uses the actual build and currently ready kit. */
export function strength(game,unit){
  if(!unit?.alive)return 0;
  const health=Math.max(.1,(unit.hp+(unit.shield||0)*.65)/Math.max(1,unit.maxHp));
  if(unit.kind!=='hero')return (unit.siege?3:structure(unit)?4:.28)*health;
  const ready=unit.cooldowns.reduce((n,c,i)=>n+(unit.level>=HEROES[unit.heroId].abilities[i].unlock?1-Math.min(1,c/Math.max(1,game.getAbility(unit,i).cooldown)):0),0);
  const damage=unit.damage/Math.max(.2,unit.attackRate),level=1+(unit.level-1)*.035;
  const durability=Math.sqrt(unit.maxHp/Math.max(1,HEROES[unit.heroId].hp+(unit.level-1)*62));
  return Math.min(9,(1.4+damage/65+ready*.2+game.bonus(unit,'spellDamage'))*health*level*durability);
}

export function assignTeamJobs(game,team,plan){
  game.assignments??=[{},{}];
  const jobs=game.assignments[team]={};
  const heroes=game.units.filter(u=>u.alive&&u.kind==='hero'&&u.team===team);
  const bots=heroes.filter(u=>u.bot);
  for(const hero of bots)jobs[hero.id]={type:hero.heroId==='bulwark'?'frontline':hero.heroId==='ember'?'flank':hero.heroId==='angel'||hero.heroId==='seraph'?'support':'advance',lane:plan.lane};
  // A single strong defender can hold a secondary threatened lane while the others act.
  if(plan.type==='push'||plan.type==='beast'){
    const danger=game.units.filter(u=>u.alive&&u.team===team&&u.kind==='tower'&&u.lane!==plan.lane).map(tower=>({tower,foes:game.units.filter(u=>u.alive&&u.team===1-team&&!structure(u)&&canSee(game,team,u)&&distance(u,tower)<460)})).filter(x=>x.foes.length>=3).sort((a,b)=>b.foes.length-a.foes.length)[0];
    if(danger&&heroes.length>=3){
      const holder=bots.filter(u=>u.heroId!=='angel').sort((a,b)=>distance(a,danger.tower)-distance(b,danger.tower))[0];
      if(holder)jobs[holder.id]={type:'hold',lane:danger.tower.lane,x:danger.tower.x,y:danger.tower.y};
    }
  }
}

const BUILD_PATHS={
  arc:['starfire-scepter','eternity-hourglass','sovereign-aegis'],
  dragon:['worldsplitter','crimson-relic','sovereign-aegis'],
  druid:['elderwood-staff','phoenix-crown','celestial-font'],
  angel:['phoenix-crown','eternity-hourglass','celestial-font'],
  ranger:['thunderbolt-crossbow','crimson-relic','tempest-wings'],
  bulwark:['sovereign-aegis','eternity-hourglass','crimson-relic'],
  ember:['worldsplitter','tempest-wings','crimson-relic'],
  seraph:['worldsplitter','sovereign-aegis','tempest-wings'],
};
export function nextBuildItem(hero){
  const owned=new Set(hero.items||[]),path=hero.buildPlan?.length?hero.buildPlan:(BUILD_PATHS[hero.heroId]||BUILD_PATHS.arc);
  hero.buildPlan=[...path];
  const next=id=>{const item=ITEM_BY_ID[id];if(!item||owned.has(id))return null;for(const requirement of item.requires){const missing=next(requirement);if(missing)return missing;}return item;};
  for(const id of path){const item=next(id);if(item){hero.buildGoal=id;return item;}}
  // After completing the coherent core, fill remaining recipes in stable order.
  for(const item of Object.values(ITEM_BY_ID)){const missing=next(item.id);if(missing){hero.buildGoal=item.id;return missing;}}
  return null;
}

export function chooseBotTalent(hero,tier){
  const preferred=hero.heroId==='bulwark'?['hp','shield','knightStun']:hero.heroId==='angel'?['healing','shield','cooldown']:hero.heroId==='druid'?['summonPower','summonHaste','bearCount','wolfCount']:hero.heroId==='seraph'?['sweepDamage','sweepRadius','attackSpeed','winterDamage']:hero.heroId==='ember'?['executionDamage','heartseekerDamage','damage','speed']:hero.heroId==='ranger'?['damage','attackSpeed','poisonDamage']:['spellDamage','cooldown','stormDamage'];
  return [...tier.choices].sort((a,b)=>preferred.reduce((n,key)=>n+(b.bonuses?.[key]||0)-(a.bonuses?.[key]||0),0)*5||a.id.localeCompare(b.id))[0];
}
