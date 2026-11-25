import { HEROES, WORLD, segmentDistance } from './data.mjs';
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
    if(!observer.alive||observer.team!==team||observer.noVision)return false;
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
  for(const hero of bots)jobs[hero.id]={type:hero.heroId==='rivet'?'defend':hero.heroId==='brinehook'?'catch':hero.heroId==='vela'?'marksman':['bulwark','ferrite'].includes(hero.heroId)?'frontline':hero.heroId==='ember'?'flank':['angel','seraph','solstice','cadence'].includes(hero.heroId)?'support':'advance',lane:plan.lane};
  // A single strong defender can hold a secondary threatened lane while the others act.
  if(plan.type==='push'||plan.type==='beast'){
    const danger=game.units.filter(u=>u.alive&&u.team===team&&u.kind==='tower'&&u.lane!==plan.lane).map(tower=>({tower,foes:game.units.filter(u=>u.alive&&u.team===1-team&&!structure(u)&&canSee(game,team,u)&&distance(u,tower)<460)})).filter(x=>x.foes.length>=3).sort((a,b)=>b.foes.length-a.foes.length)[0];
    if(danger&&heroes.length>=3){
      const holder=bots.filter(u=>!['angel','solstice','cadence'].includes(u.heroId)).sort((a,b)=>(distance(a,danger.tower)-(a.heroId==='rivet'?300:0))-(distance(b,danger.tower)-(b.heroId==='rivet'?300:0)))[0];
      if(holder)jobs[holder.id]={type:'hold',lane:danger.tower.lane,x:danger.tower.x,y:danger.tower.y};
    }
  }
}

const BUILD_PATHS={
  arc:['starfire-scepter','eternity-hourglass','sovereign-aegis'],
  dragon:['inferno-heart','worldsplitter','sovereign-aegis'],
  druid:['elderwood-staff','phoenix-crown','celestial-font'],
  angel:['phoenix-crown','eternity-hourglass','celestial-font'],
  ranger:['pandemic-engine','thunderbolt-crossbow','tempest-wings'],
  bulwark:['sovereign-aegis','eternity-hourglass','crimson-relic'],
  ember:['worldsplitter','tempest-wings','crimson-relic'],
  seraph:['worldsplitter','sovereign-aegis','tempest-wings'],
  rivet:['elderwood-staff','eternity-hourglass','sovereign-aegis'],
  brinehook:['sovereign-aegis','eternity-hourglass','crimson-relic'],
  vela:['kings-ransom','thunderbolt-crossbow','crimson-relic'],
  solstice:['phoenix-crown','eternity-hourglass','sovereign-aegis'],
  ferrite:['sovereign-aegis','winters-dominion','crimson-relic'],
  cadence:['ascendant-codex','eternity-hourglass','sovereign-aegis'],
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
  const expansionPath={
    rivet:['rivet-reinforced-casing','rivet-repair-routine','rivet-hot-rivets','rivet-quick-assembly','rivet-crossfire','rivet-shared-tools','rivet-fortified-position'],
    brinehook:['brinehook-thick-coat','brinehook-weatherproof','brinehook-steady-footing','brinehook-iron-captain','brinehook-crew-cover','brinehook-calm-waters','brinehook-safe-harbor'],
    vela:['vela-quick-draw','vela-fast-hands','vela-clean-chambers','vela-field-medic','vela-opening-pair','vela-spare-magazine','vela-perfect-rhythm'],
    ferrite:['ferrite-tempered-scrap','ferrite-grounding','ferrite-layered-alloy','ferrite-deep-core','ferrite-shared-alloy','ferrite-insulated-core','ferrite-monolith'],
    cadence:['cadence-soft-chime','cadence-long-phrase','cadence-broad-stage','cadence-fast-measure','cadence-soft-landing','cadence-overtone','cadence-grand-shelter'],
    solstice:['solstice-thicker-glass','solstice-long-reach','solstice-gentle-return','solstice-golden-shelter','solstice-shared-moment','solstice-steady-hour','solstice-golden-interval']
  }[hero.heroId];
  const chosen=expansionPath&&tier.choices.find(c=>c.id===expansionPath[[3,5,8,10,12,15,18].indexOf(tier.level)]);
  if(chosen)return chosen;
  const preferred=hero.heroId==='bulwark'?['hp','shield','knightStun']:hero.heroId==='angel'?['healing','shield','cooldown']:hero.heroId==='druid'?['summonPower','summonHaste','bearCount','wolfCount']:hero.heroId==='seraph'?['sweepDamage','sweepRadius','attackSpeed','winterDamage']:hero.heroId==='ember'?['executionDamage','heartseekerDamage','damage','speed']:hero.heroId==='ranger'?['damage','attackSpeed','poisonDamage']:['spellDamage','cooldown','stormDamage'];
  return [...tier.choices].sort((a,b)=>preferred.reduce((n,key)=>n+(b.bonuses?.[key]||0)-(a.bonuses?.[key]||0),0)*5||a.id.localeCompare(b.id))[0];
}

export const isExpansionHero=hero=>['rivet','brinehook','vela','solstice','ferrite','cadence'].includes(typeof hero==='string'?hero:hero?.heroId);
const point=u=>({x:u.x,y:u.y});
const visibleFoes=(game,hero,r=650)=>game.units.filter(u=>u.alive&&u.team!==hero.team&&!game.isProtected(u)&&distance(hero,u)<r+u.r&&canSee(game,hero.team,u));
const alliedHeroes=(game,hero)=>game.units.filter(u=>u.alive&&u.kind==='hero'&&u.team===hero.team);
const ownedDevices=(game,hero,subtype)=>game.units.filter(u=>u.alive&&u.ownerId===hero.id&&u.kind==='summon'&&u.subtype===subtype);
function firstVisibleOnLine(game,hero,at,range,width){
  const angle=Math.atan2(at.y-hero.y,at.x-hero.x),dx=Math.cos(angle),dy=Math.sin(angle),end={x:hero.x+dx*range,y:hero.y+dy*range};
  return visibleFoes(game,hero,range+50).filter(u=>{
    const projection=(u.x-hero.x)*dx+(u.y-hero.y)*dy;
    return projection>=0&&projection<=range+u.r&&segmentDistance(u.x,u.y,hero,end)<=width+u.r&&game.clearLine(hero,u,4);
  }).sort((a,b)=>((a.x-hero.x)*dx+(a.y-hero.y)*dy)-((b.x-hero.x)*dx+(b.y-hero.y)*dy)||a.id-b.id)[0]||null;
}
function safeCastPoint(game,hero,at,range){
  const d=distance(hero,at),p=d>range?{x:hero.x+(at.x-hero.x)*range/d,y:hero.y+(at.y-hero.y)*range/d}:point(at);
  const safe=game.safePoint(p,hero.r);
  return distance(hero,safe)<=range+1&&game.clearLine(hero,safe,hero.r)&&!game.towerThreatOnPath(hero,safe,null,false)?safe:null;
}
function threatened(game,hero,ally,foes){
  if(foes.some(u=>!structure(u)&&(u.targetId===ally.id||u.kind==='hero'&&distance(u,ally)<u.range+ally.r+55)))return true;
  return game.projectiles.some(p=>p.team!==hero.team&&p.targetId===ally.id&&game.units.some(u=>u.id===p.sourceId&&canSee(game,hero.team,u)));
}
function refundValue(game,ally){
  return (ally.cooldowns||[]).slice(0,3).reduce((sum,cooldown,slot)=>sum+Math.max(0,Math.min(2,HEROES[ally.heroId].abilities[slot].cooldown*.25,cooldown-.6)),0);
}

/** Additive target preference: inputs are visible candidates, never hidden state. */
export function expansionTargetBias(game,hero,target){
  if(!isExpansionHero(hero)||!target?.alive||!canSee(game,hero.team,target))return 0;
  if(hero.heroId==='rivet')return ownedDevices(game,hero,'sentry').some(u=>distance(u,target)<=u.range+target.r)?-45:0;
  if(hero.heroId==='brinehook'&&target.kind==='hero'){
    const victim=alliedHeroes(game,hero).find(a=>a.id!==hero.id&&a.id===target.targetId&&a.hp/a.maxHp<.7);
    return (victim?-90:0)+(target.range>150?-35:target.hp/target.maxHp>.7?45:0);
  }
  if(hero.heroId==='vela')return (target.kind==='hero'&&target.range<120&&target.hp/target.maxHp>.6?35:0)+(distance(hero,target)<=hero.range+target.r?-25:0);
  if(hero.heroId==='ferrite')return target.kind==='hero'&&target.range>150?-35:0;
  if(hero.heroId==='cadence')return alliedHeroes(game,hero).some(a=>a.id!==hero.id&&a.hp/a.maxHp<.65&&target.targetId===a.id)?-60:0;
  if(hero.heroId==='solstice')return alliedHeroes(game,hero).some(a=>a.id!==hero.id&&a.hp/a.maxHp<.65&&target.targetId===a.id)?-65:0;
  return 0;
}

/** Expansion casts use current shared sight and the same difficulty reaction gate. */
export function expansionBotAim(game,hero,a,target,retreat,cfg){
  if(!isExpansionHero(hero))return null;
  // Do not casually interrupt a committed volley with a newly ready attack spell.
  // A defensive Sidestep may still abandon it when a visible enemy closes in.
  if(hero.channel?.kind==='silver-volley'&&a.id!=='sidestep')return null;
  if(target&&(!target.alive||!canSee(game,hero.team,target)))target=null;
  const foes=visibleFoes(game,hero),friends=alliedHeroes(game,hero).filter(u=>distance(hero,u)<=Math.max(350,a.range+a.radius));
  const lead=target?{x:target.x+(target.vx||0)*.2*cfg.lead,y:target.y+(target.vy||0)*.2*cfg.lead}:point(hero);
  const near=!!target&&distance(hero,target)<=a.range+target.r,close=foes.filter(u=>!structure(u)&&distance(hero,u)<300);
  switch(a.id){
    case 'rail-spike':case 'piercing-note':return target&&near&&(!retreat||distance(hero,target)<240)?lead:null;
    case 'scrap-barrier': {
      // Tower fire is dangerous even after all nearby troops have died. It must
      // count as pressure rather than disappearing with the non-structure list.
      const towerFire=foes.some(u=>structure(u)&&(u.targetId===hero.id||u.aggroId===hero.id&&u.aggroUntil>game.time)&&distance(hero,u)<=u.range+hero.r+35);
      const incoming=game.projectiles.some(p=>p.team!==hero.team&&p.targetId===hero.id&&game.units.some(u=>u.id===p.sourceId&&canSee(game,hero.team,u)));
      const pressure=close.length>0||towerFire||incoming;
      return pressure&&(towerFire||incoming||hero.hp/hero.maxHp<.82||threatened(game,hero,hero,foes))&&((hero.resonance?.scrap||0)>0||hero.hp/hero.maxHp<.45)?point(hero):null;
    }
    case 'polarity-pulse': {
      const inside=close.filter(u=>distance(hero,u)<=a.radius+u.r&&!u.stationary&&u.kind!=='beast');
      if(!inside.length)return null;
      const peel=retreat||hero.hp/hero.maxHp<.4||friends.some(u=>u.id!==hero.id&&u.hp/u.maxHp<.45&&inside.some(t=>distance(t,u)<130));
      const desired=peel?'push':'pull';
      return hero.resonance?.polarity!==desired?point(hero):null;
    }
    case 'lodestone-storm':return target&&near&&!retreat&&(hero.resonance?.scrap||0)>=2&&hero.hp/hero.maxHp>.4&&(target.kind==='hero'||target.kind==='beast'||close.length>=3)?lead:null;
    case 'sheltering-chime': {
      const ally=friends.filter(u=>distance(hero,u)<=a.range&&threatened(game,hero,u,foes)).sort((x,y)=>x.hp/x.maxHp-y.hp/y.maxHp)[0];
      return ally?point(ally):null;
    }
    case 'skipbeat': {
      const pursuer=close.filter(u=>u.kind==='hero').sort((x,y)=>distance(hero,x)-distance(hero,y))[0];
      if(!pursuer)return null;
      const needsNote=!(hero.resonance?.notes||[]).some(n=>n.slot===2);
      if(!retreat&&distance(hero,pursuer)>160&&!(needsNote&&close.filter(u=>u.kind==='hero').length>=2))return null;
      const d=distance(hero,pursuer)||1,side=hero.id%2?1:-1;
      return safeCastPoint(game,hero,{x:hero.x+(hero.x-pursuer.x)/d*a.range*.65-(hero.y-pursuer.y)/d*a.range*.4*side,y:hero.y+(hero.y-pursuer.y)/d*a.range*.65+(hero.x-pursuer.x)/d*a.range*.4*side},a.range);
    }
    case 'grand-finale': {
      if(retreat)return null;
      const notes=(hero.resonance?.notes||[]).filter(n=>game.time-n.at<=8+game.bonus(hero,'cadenceNoteWindow'));
      if(!notes.length)return null;
      const weak=friends.filter(u=>distance(hero,u)<=a.range&&u.hp/u.maxHp<.6&&threatened(game,hero,u,foes)).sort((x,y)=>x.hp/x.maxHp-y.hp/y.maxHp)[0];
      const desired=weak?1:close.filter(u=>u.kind==='hero').length>=2?2:0;
      if(notes.at(-1).slot!==desired&&hero.cooldowns[desired]<=0&&hero.mana>=game.getAbility(hero,desired).mana+a.mana)return null;
      if(notes.at(-1).slot===1&&weak)return point(weak);
      return target&&near&&(target.kind==='hero'||target.kind==='beast'||close.length>=3)?lead:null;
    }
    case 'screwshot':case 'hourhand':return target&&near&&(!retreat||distance(hero,target)<240)?lead:null;
    case 'bolt-sentry': {
      if(!target||distance(hero,target)>420||retreat)return null;
      const devices=ownedDevices(game,hero,'sentry');
      if(devices.filter(u=>distance(u,target)<u.range+80&&u.lifetime>4).length>=2)return null;
      const angle=Math.atan2(target.y-hero.y,target.x-hero.x),front=game.units.filter(u=>u.alive&&u.team===hero.team&&u.id!==hero.id&&!u.stationary&&!structure(u)&&distance(hero,u)<260&&distance(u,target)<distance(hero,target)).sort((x,y)=>distance(x,target)-distance(y,target))[0];
      const anchor=front||hero;
      for(const offset of [0,1,-1]){
        const at={x:anchor.x-Math.cos(angle)*55+Math.cos(angle+Math.PI/2)*offset*110,y:anchor.y-Math.sin(angle)*55+Math.sin(angle+Math.PI/2)*offset*110};
        const safe=safeCastPoint(game,hero,at,a.range);
        if(!safe||devices.some(u=>distance(u,safe)<90))continue;
        const warned=(game.scheduledImpacts||[]).some(z=>z.team!==hero.team&&z.executeAt>game.time&&distance(safe,z)<z.r+35&&game.units.some(u=>u.id===z.ownerId&&canSee(game,hero.team,u)));
        if(!warned)return safe;
      }
      return null;
    }
    case 'shock-mine': {
      const pursuer=close.filter(u=>u.kind==='hero').sort((x,y)=>distance(hero,x)-distance(hero,y))[0];
      if(!pursuer&&(!target||target.kind!=='beast'||cfg.coordination<.8))return null;
      if(!retreat&&pursuer&&distance(hero,pursuer)>280)return null;
      const toward=pursuer||target,d=distance(hero,toward)||1;
      const at={x:hero.x+(toward.x-hero.x)/d*Math.min(95,d*.5),y:hero.y+(toward.y-hero.y)/d*Math.min(95,d*.5)};
      return ownedDevices(game,hero,'mine').some(u=>distance(u,at)<105)?null:safeCastPoint(game,hero,at,a.range);
    }
    case 'overclock':return close.length>0&&(ownedDevices(game,hero,'sentry').some(u=>distance(hero,u)<=a.radius&&foes.some(t=>distance(u,t)<u.range+t.r))||target&&distance(hero,target)<=hero.range+target.r)?point(hero):null;
    case 'anchorline': {
      if(!target||!near||structure(target)||target.siege||target.kind==='beast'||(target.status?.stun?.remaining||0)>.3)return null;
      if(firstVisibleOnLine(game,hero,lead,a.range,a.radius)?.id!==target.id)return null;
      const peel=friends.some(u=>u.id!==hero.id&&u.hp/u.maxHp<.7&&distance(u,target)<220);
      const enemyPower=close.reduce((sum,u)=>sum+strength(game,u),0),friendlyPower=friends.filter(u=>distance(hero,u)<330).reduce((sum,u)=>sum+strength(game,u),0);
      if(!peel&&enemyPower>friendlyPower*1.25)return null;
      if(!peel&&target.kind==='hero'&&target.range<120&&target.hp/target.maxHp>.6&&friends.some(u=>u.id!==hero.id&&u.range>150&&u.hp/u.maxHp<.65&&distance(hero,u)<190))return null;
      return !retreat||peel?lead:null;
    }
    case 'sea-legs':return close.length&&(hero.hp/hero.maxHp<.88||hero.status?.slow||friends.some(u=>u.hp/u.maxHp<.65&&distance(hero,u)<130))?point(hero):null;
    case 'undertow':return target&&near&&(target.status?.stun?.remaining||0)<.3&&(!retreat||target.kind==='hero')?lead:null;
    case 'captains-wake':return target&&near&&!retreat&&(target.status?.stun?.remaining||0)<.3&&(target.kind==='hero'||close.length>=2)?lead:null;
    case 'split-shot':return target&&near&&(!retreat||distance(hero,target)<230)?lead:null;
    case 'sidestep': {
      const threat=close.filter(u=>u.kind==='hero').sort((x,y)=>distance(hero,x)-distance(hero,y))[0];
      if(threat&&(retreat||distance(hero,threat)<170||hero.magazine?.reloadRemaining>0&&distance(hero,threat)<240)){
        const d=distance(hero,threat)||1;
        return safeCastPoint(game,hero,{x:hero.x+(hero.x-threat.x)*a.range/d,y:hero.y+(hero.y-threat.y)*a.range/d},a.range);
      }
      if(!retreat&&target?.kind==='hero'&&target.hp/target.maxHp<.2&&hero.hp/hero.maxHp>.7&&distance(hero,target)>hero.range+target.r&&foes.filter(u=>u.kind==='hero').length===1)return safeCastPoint(game,hero,lead,a.range);
      return null;
    }
    case 'steady-aim':return target&&!retreat&&distance(hero,target)<=hero.range*1.15+target.r&&hero.hp/hero.maxHp>.48&&!close.some(u=>u.kind==='hero'&&distance(hero,u)<140)?point(hero):null;
    case 'silver-volley':return target&&near&&!retreat&&(target.kind==='hero'||target.kind==='beast'||target.siege)&&!close.some(u=>u.kind==='hero'&&distance(hero,u)<175)&&firstVisibleOnLine(game,hero,lead,a.range,a.radius)?.id===target.id?lead:null;
    case 'time-ward': {
      const candidate=friends.filter(u=>distance(hero,u)<=a.range&&(!u.timeWard||u.timeWard.expiresAt<=game.time+.25)&&threatened(game,hero,u,foes)).sort((x,y)=>x.hp/x.maxHp-y.hp/y.maxHp||x.id-y.id)[0];
      return candidate?point(candidate):null;
    }
    case 'unwind': {
      const impaired=friends.filter(u=>distance(hero,u)<=a.range&&((u.status?.stun?.remaining||0)>.1||u.status?.slow)).sort((x,y)=>(y.status?.stun?.remaining||0)-(x.status?.stun?.remaining||0)||x.hp/x.maxHp-y.hp/y.maxHp)[0];
      return impaired?point(impaired):null;
    }
    case 'borrowed-seconds': {
      if(retreat)return null;
      const positions=[...friends.filter(u=>distance(hero,u)<=a.range&&refundValue(game,u)>=1.2),...(target&&near?[target]:[])];
      const scored=positions.map(at=>({at,score:friends.filter(u=>distance(at,u)<=a.radius&&refundValue(game,u)>=1.2).reduce((n,u)=>n+refundValue(game,u),0)+foes.filter(u=>!structure(u)&&distance(at,u)<=a.radius).reduce((n,u)=>n+(u.kind==='hero'?2.5:.4),0)})).sort((x,y)=>y.score-x.score);
      const best=scored[0];return best&&best.score>=2.5?point(best.at):null;
    }
    default:return null;
  }
}

/** A fixed volley stands still; an empty magazine creates a real spacing window. */
export function expansionBotMovement(game,hero,target,cfg,dt){
  if(hero.heroId!=='vela')return false;
  if(hero.channel?.kind==='silver-volley')return true;
  if(!(hero.magazine?.reloadRemaining>0)||!target?.alive||!canSee(game,hero.team,target))return false;
  if(cfg.coordination<=.6)return true;
  const dx=hero.x-target.x,dy=hero.y-target.y,d=Math.hypot(dx,dy)||1,side=hero.id%2?1:-1;
  const point={x:hero.x+dx/d*85-dy/d*side*65,y:hero.y+dy/d*85+dx/d*side*65};
  const safe=safeCastPoint(game,hero,point,120);
  if(safe)game.moveToward(hero,safe,dt);
  hero.aiState='spacing during reload';return true;
}
