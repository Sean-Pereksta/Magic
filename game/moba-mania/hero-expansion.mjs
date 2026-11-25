/** Four expansion kits. Persistent mechanics use bounded, ordinary JSON state. */
import { WORLD, segmentDistance } from './data.mjs';
import { scheduleImpact } from './progression.mjs';
import { modifyItemStatus } from './items.mjs';
import { castSolsticeSkill, resolveSolsticeImpact, onSolsticeProjectileHit, tickSolstice } from './solstice-runtime.mjs';

const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const structure=u=>u?.kind==='tower'||u?.kind==='core';
const point=u=>({x:u.x,y:u.y});
const ownerOf=(game,u)=>u?.kind==='hero'?u:game.units.find(h=>h.kind==='hero'&&h.id===u?.ownerId);
const bonus=(game,h,key)=>game.bonus(h,key)||0;
const power=(game,h)=>(1+(h.level-1)*.075)*(1+bonus(game,h,'damage'))*(1+bonus(game,h,'spellDamage'));
const shieldPower=(game,h,n)=>n*(1+bonus(game,h,'shield'))*(1+(h.level-1)*.08);
const foe=(game,hero,u)=>u.alive&&u.team!==hero.team&&!game.isProtected(u)&&game.canSee(hero.team,u);
const expansionId=id=>['rivet','brinehook','vela','solstice'].includes(id);
function slowSnapshot(game,hero,factor,duration){
  const scaled=modifyItemStatus(game,{team:1-hero.team},'slow',duration,{factor,sourceId:hero.id});
  return {slow:scaled.extra.factor,slowDuration:scaled.duration,itemScaled:true};
}
function state(hero){const s=hero.expansion??={};s.readyAt??={};return s;}
function ready(game,hero,key,seconds){const s=state(hero);if((s.readyAt[key]||0)>game.time)return false;s.readyAt[key]=game.time+seconds;return true;}
function effect(game,hero,type,at,r,life=.6,extra={}){
  return game.effect(type,at.x,at.y,r,{rivet:'#c97a45',brinehook:'#3fb8ac',vela:'#f4efdc',solstice:'#e7b951'}[hero.heroId],life,
    {ownerId:hero.id,team:hero.team,theme:game.combatTheme(hero),...extra});
}
function visibleEffect(game,hero,type,target,r,life,extra={}){
  return effect(game,hero,type,target,r,life,{visibilityId:target.id,visibleTo:target.alive?game.visualAudience(target):target.visualDeath?.visibleTo||[],...extra});
}
function validEnemies(game,hero,at,r){return game.units.filter(u=>foe(game,hero,u)&&distance(u,at)<=r+u.r&&game.clearLine(at,u,4));}
function inLine(unit,line){
  const dx=Math.cos(line.angle),dy=Math.sin(line.angle),projection=(unit.x-line.x)*dx+(unit.y-line.y)*dy;
  return projection>=-unit.r&&projection<=line.range+unit.r&&segmentDistance(unit.x,unit.y,line,{x:line.x+dx*line.range,y:line.y+dy*line.range})<=line.width+unit.r;
}

export function initExpansionHero(hero){
  if(hero.heroId==='vela')hero.magazine??={rounds:6,reloadRemaining:0,reloadDuration:1.4,openingShots:0};
}
export function adjustExpansionAbility(game,hero,ability){
  const a={...ability};
  if(a.id==='anchorline')a.range*=1+bonus(game,hero,'brineAnchorRange');
  if(a.id==='undertow')a.radius+=bonus(game,hero,'brinePoolRadius')*(1+bonus(game,hero,'area'));
  if(a.id==='captains-wake'){a.radius*=1+bonus(game,hero,'brineGrandWake');a.width=a.radius*2;}
  if(a.id==='sidestep')a.range*=1+bonus(game,hero,'velaStride');
  if(a.id==='shock-mine'&&bonus(game,hero,'rivetMineRadius')){a.radius=100*(1+bonus(game,hero,'area'));a.triggerRadius=80;}
  return a;
}

/** Displacements test the complete path, the map bounds, and ordinary camp leashes. */
export function traceExpansionMovement(game,unit,angle,length,{pull=false}={}){
  const start=point(unit),dx=Math.cos(angle),dy=Math.sin(angle);let travelled=0;
  if(pull&&(!unit.alive||structure(unit)||unit.kind==='beast'||unit.stationary||(unit.displacementRecoveryUntil||0)>game.time))return {start,end:point(unit),travelled};
  for(let left=Math.max(0,length);left>0;){
    const step=Math.min(6,left),next={x:unit.x+dx*step,y:unit.y+dy*step};
    if(next.x<unit.r+15||next.y<unit.r+15||next.x>WORLD.width-unit.r-15||next.y>WORLD.height-unit.r-15)break;
    if(WORLD.obstacles.some(o=>distance(next,o)<o.r+unit.r+1)||!game.clearLine(unit,next,unit.r))break;
    if(pull&&unit.kind==='camp'&&distance(next,{x:unit.homeX,y:unit.homeY})>=270)break;
    unit.x=next.x;unit.y=next.y;travelled+=step;left-=step;
  }
  if(travelled>0){
    unit.path=null;unit.pathGoal=null;unit.navPath=null;unit.navGoal=null;unit.navTime=0;unit.recallIn=0;unit.moving=true;
    if(pull)unit.displacementRecoveryUntil=game.time+.5;
    cancelExpansionChannel(game,unit);
  }
  return {start,end:point(unit),travelled};
}
function pullToward(game,hero,target,at,maximum,castId){
  const d=distance(target,at);if(d<1)return false;
  const movement=traceExpansionMovement(game,target,Math.atan2(at.y-target.y,at.x-target.x),Math.min(maximum,d),{pull:true});
  if(!movement.travelled)return false;
  const s=state(hero);
  if(bonus(game,hero,'brineHeavyLanding')&&s.heavyLandingCast!==castId){s.heavyLandingCast=castId;s.heavyLandingUntil=game.time+3;s.heavyLandingDamage=35*power(game,hero);}
  if(target.kind==='hero'&&hero.alive&&ready(game,hero,'saltwater-resolve',8)){
    game.shield(hero,shieldPower(game,hero,80),3,hero);
    if(bonus(game,hero,'brineCalmWaters'))game.heal(hero,35,hero);
    effect(game,hero,'sea-legs',hero,hero.r+22,.65,{followId:hero.id,abilityId:'saltwater-resolve'});
  }
  return true;
}

function expireDevice(game,device){
  if(!device.alive)return;
  game.presentation('summon-depart',device,.65,{subtype:device.subtype,ownerId:device.ownerId,theme:'copper'});
  device.alive=false;device.hp=0;device.deathTime=.4;device.targetId=null;
}
function placeDevice(game,hero,a,at,spellPower,shieldAmount){
  const subtype=a.id==='bolt-sentry'?'sentry':'mine',sentry=subtype==='sentry';
  const older=game.units.filter(u=>u.alive&&u.kind==='summon'&&u.ownerId===hero.id&&u.subtype===subtype).sort((u,v)=>(u.createdAt||0)-(v.createdAt||0)||u.id-v.id);
  while(older.length>=2)expireDevice(game,older.shift());
  const scale=(1+(hero.level-1)*.08)*(1+bonus(game,hero,'summonPower')),r=sentry?15:12,atSafe=game.safePoint(at,r);
  const lifetime=sentry?18+bonus(game,hero,'rivetSentryDuration'):12;
  const device=game.addUnit('summon',hero.team,atSafe.x,atSafe.y,{subtype,ownerId:hero.id,name:sentry?'Bolt Sentry':'Shock Mine',stationary:true,noCollision:true,noVision:!sentry,
    r,hp:(sentry?230*(1+bonus(game,hero,'rivetSentryHealth')):70)*scale,damage:sentry?26*scale:80*spellPower*(1+bonus(game,hero,'summonPower')),
    speed:0,range:sentry?250:0,attackRate:sentry?.9/(1+bonus(game,hero,'summonHaste')+bonus(game,hero,'rivetSentryHaste')):1,
    createdAt:game.time,assembleUntil:game.time+(sentry?.6:0),armAt:game.time+(sentry?.6:.8),lifetime,maxLifetime:lifetime,
    triggerRadius:a.triggerRadius||65,blastRadius:a.radius,lane:hero.lane,level:hero.level,deviceScale:scale,
    emergencyShield:shieldAmount(45),crossfireDamage:30*scale,overclockUntil:0,...(sentry?{}:slowSnapshot(game,hero,.35,2))});
  device.visualSpawn={at:game.time,duration:sentry?.6:.8,kind:'summon',subtype};
  game.presentation('summon-arrive',device,sentry?.65:.8,{subtype,ownerId:hero.id,theme:'copper',r:r+22});
  // Equipment still treats a sentry as a real owned summon. Mines do not receive creature-entry bonuses.
  if(sentry)game.expansionSummonCreated?.(hero,device);
  return device;
}
function focusedTarget(game,hero){
  if(!hero?.alive)return null;
  const id=hero.isPlayer?(game.attackOrder?.explicit?game.attackOrder.targetId:null):(hero.brain?.targetId??hero.targetId);
  return game.units.find(u=>u.id===id&&foe(game,hero,u))||null;
}
export function tickExpansionDevice(game,device,dt){
  if(!device.stationary||!['sentry','mine'].includes(device.subtype))return false;
  if(device.subtype==='sentry'&&game.time<device.assembleUntil)return true;
  device.lifetime-=dt;if(device.lifetime<=0){expireDevice(game,device);return true;}
  if(device.status.stun)return true;
  const hero=game.units.find(h=>h.id===device.ownerId&&h.kind==='hero');
  if(device.subtype==='mine'){
    if(game.time<device.armAt||!hero)return true;
    const trigger=validEnemies(game,device,device,device.triggerRadius).sort((a,b)=>distance(a,device)-distance(b,device)||a.id-b.id)[0];
    if(!trigger)return true;
    const targets=validEnemies(game,device,device,device.blastRadius);
    for(const target of targets){
      game.damage(target,device.damage*(structure(target)?.35:1),device,'magic',false,{theme:'copper',abilityId:'shock-mine'});
      if(target.alive&&!structure(target))game.applyStatus(target,'slow',device.slowDuration??2,{factor:device.slow??.35,sourceId:hero.id,itemScaled:!!device.itemScaled});
      if(target.alive&&target.kind==='hero'&&bonus(game,hero,'rivetSurvey'))target.revealedUntil=Math.max(target.revealedUntil||0,game.time+2);
    }
    visibleEffect(game,hero,'shock-mine',device,device.blastRadius,.65,{abilityId:'shock-mine'});
    game.groundMark('slam',device,device.blastRadius,'copper',{ownerId:hero.id,team:hero.team,visibilityId:device.id,visibleTo:game.visualAudience(device)});
    if(hero.alive&&distance(hero,device)<=300&&bonus(game,hero,'rivetExitPlan')&&ready(game,hero,'exit-plan',6))game.applyStatus(hero,'haste',2,{factor:.2,attack:0,sourceId:hero.id});
    expireDevice(game,device);return true;
  }
  const focus=focusedTarget(game,hero),valid=u=>u&&foe(game,device,u)&&distance(u,device)<=device.range+u.r+device.r*.35&&game.clearLine(device,u,4);
  const target=valid(focus)?focus:game.units.filter(valid).sort((a,b)=>distance(device,a)-distance(device,b)||a.id-b.id)[0];
  device.targetId=target?.id??null;if(target)game.basicAttack(device,target);return true;
}

export function cancelExpansionChannel(game,hero){
  if(hero?.channel?.kind!=='silver-volley')return;
  delete hero.channel;
  game.scheduledImpacts=game.scheduledImpacts.filter(i=>i.ownerId!==hero.id||i.kind!=='silver-volley');
  if(hero.visualAction?.abilityId==='silver-volley')hero.visualAction.duration=Math.min(hero.visualAction.duration,Math.max(0,game.time-hero.visualAction.startedAt));
}
function completeReload(game,hero){
  const magazine=hero.magazine;magazine.rounds=6;magazine.reloadRemaining=0;magazine.openingShots=bonus(game,hero,'velaOpeningPair')?2:0;
}
function beginReload(game,hero){
  const magazine=hero.magazine;magazine.reloadDuration=1.4-bonus(game,hero,'velaFastHands');magazine.reloadRemaining=magazine.reloadDuration;magazine.openingShots=0;
  effect(game,hero,'reload',hero,hero.r+11,.3,{abilityId:'six-round-rhythm',followId:hero.id});
}
export function tickExpansionHero(game,hero,dt){
  if(!expansionId(hero.heroId))return;
  initExpansionHero(hero);
  if(hero.heroId==='vela'){
    if(hero.magazine.reloadRemaining>0){hero.magazine.reloadRemaining=Math.max(0,hero.magazine.reloadRemaining-dt);if(hero.magazine.reloadRemaining===0)completeReload(game,hero);}
    if(hero.expansionAimActive&&!hero.status.steadyAim){delete hero.expansionAimActive;game.refreshStats(hero);}
    if(hero.status.stun)cancelExpansionChannel(game,hero);
  }
  if(hero.expansion?.splitPairs)hero.expansion.splitPairs=hero.expansion.splitPairs.filter(pair=>pair.expiresAt>game.time);
}
export function expansionAttackPreview(game,unit){
  let damage=unit.damage;
  if(unit.heroId==='vela'){
    if(unit.channel||unit.magazine?.reloadRemaining>0)return 0;
    if(unit.magazine?.rounds===1)damage*=1.2+bonus(game,unit,'velaHeavySixth');
  }
  if(unit.heroId==='brinehook'&&unit.expansion?.heavyLandingUntil>game.time)damage+=unit.expansion.heavyLandingDamage||0;
  if(unit.subtype==='sentry'&&unit.overclockUntil>game.time)damage*=1+(unit.overclockDamage||0);
  return damage;
}
/** Called only after the ordinary attack gate succeeds: one launch spends one round. */
export function prepareExpansionAttack(game,unit){
  if(unit.heroId==='vela'){
    initExpansionHero(unit);const m=unit.magazine;
    if(unit.channel||m.reloadRemaining>0)return false;
    if(m.rounds<=0){beginReload(game,unit);return false;}
    const sixthRound=m.rounds===1,damage=expansionAttackPreview(game,unit),intervalFactor=m.openingShots>0?.85:1;
    m.rounds--;if(m.openingShots>0)m.openingShots--;if(m.rounds===0)beginReload(game,unit);
    return {damage,intervalFactor,sixthRound,projectile:{type:'silver-bullet',abilityId:'six-round-rhythm',sixthRound}};
  }
  if(unit.heroId==='brinehook'){
    const damage=expansionAttackPreview(game,unit);if(unit.expansion)unit.expansion.heavyLandingUntil=0;return {damage};
  }
  if(unit.subtype==='sentry')return {damage:expansionAttackPreview(game,unit),projectile:{type:'sentry-bolt',abilityId:'bolt-sentry',expansion:true,visibleOnly:true,terrainCollision:true}};
  return null;
}

export function castExpansionSkill(game,hero,a,{at,angle,power:spellPower,shieldAmount}){
  if(hero.heroId==='solstice')return castSolsticeSkill(game,hero,a,{at,angle,power:spellPower,shieldAmount});
  const b=key=>bonus(game,hero,key);
  const projectile=extra=>game.fireSkill(hero,angle,{color:a.color,abilityId:a.id,expansion:true,visibleOnly:true,terrainCollision:true,damageType:'physical',pierce:0,theme:game.combatTheme(hero),...extra});
  switch(a.id){
    case 'screwshot':
      projectile({type:'screwshot',r:a.radius,range:a.range,speed:580,damage:95*spellPower*(1+b('rivetScrewDamage')),bounceDamages:b('rivetDoubleThread')?[45*spellPower,30*spellPower]:[45*spellPower],bouncedIds:[]});return true;
    case 'bolt-sentry':case 'shock-mine':placeDevice(game,hero,a,at,spellPower,shieldAmount);return true;
    case 'overclock':{
      hero.overclockUntil=game.time+6;
      game.applyStatus(hero,'haste',6,{factor:0,attack:.2,sourceId:hero.id,color:a.color});game.shield(hero,shieldAmount(90),6,hero);
      for(const sentry of game.units)if(sentry.alive&&sentry.subtype==='sentry'&&sentry.ownerId===hero.id&&distance(hero,sentry)<=300){
        game.applyStatus(sentry,'haste',6,{factor:0,attack:.5,sourceId:hero.id,color:a.color});sentry.overclockUntil=game.time+6;sentry.overclockDamage=b('rivetRedline')?.2:0;
        if(b('rivetFortified'))game.shield(sentry,shieldAmount(100),6,hero);
      }
      if(b('rivetRelocation')){hero.cooldowns[1]=Math.max(0,hero.cooldowns[1]-4);game.applyStatus(hero,'haste',3,{factor:.2,attack:0,sourceId:hero.id});}
      effect(game,hero,'overclock',hero,300,.8,{abilityId:a.id});return true;
    }
    case 'anchorline':
      scheduleImpact(game,hero,{kind:a.id,shape:'line',x:hero.x,y:hero.y,r:a.range,range:a.range,width:a.radius,angle,delay:.25,color:a.color,damage:90*spellPower*(1+b('brineAnchorDamage')),damageType:'physical'});return true;
    case 'sea-legs':
      delete hero.status.slow;game.shield(hero,shieldAmount(180)*(1+b('brineSeaShield')),4,hero);
      if(b('brineCrewCover'))for(const ally of game.units)if(ally.alive&&ally.kind==='hero'&&ally.id!==hero.id&&ally.team===hero.team&&distance(hero,ally)<=130+ally.r)game.shield(ally,shieldAmount(60),3,hero);
      effect(game,hero,'sea-legs',hero,hero.r+25,.7,{abilityId:a.id,followId:hero.id});return true;
    case 'undertow':{
      const rough=slowSnapshot(game,hero,.2,.1);
      scheduleImpact(game,hero,{kind:a.id,x:at.x,y:at.y,r:a.radius,delay:.6,damage:80*spellPower*(1+b('brineUndertowDamage')),color:a.color,damageType:'magic',slow:.3,slowDuration:2.5,
        roughSlow:rough.slow,roughSlowDuration:rough.slowDuration,roughItemScaled:true});return true;
    }
    case 'captains-wake':
      scheduleImpact(game,hero,{kind:a.id,shape:'line',x:hero.x,y:hero.y,r:a.range,range:a.range,width:a.radius,angle,delay:.75,damage:145*spellPower,color:a.color,damageType:'magic',safeHarbor:b('brineSafeHarbor')?shieldAmount(110):0});return true;
    case 'split-shot':{
      const s=state(hero),pair={id:game.nextId,expiresAt:game.time+2,hits:[],firstHit:false};s.splitPairs??=[];s.splitPairs=s.splitPairs.filter(p=>p.expiresAt>game.time).slice(-3);s.splitPairs.push(pair);
      for(const offset of [-.055,.055])game.fireSkill(hero,angle+offset,{type:'silver-bullet',abilityId:a.id,expansion:true,visibleOnly:true,terrainCollision:true,color:a.color,theme:'ivory',r:a.radius,range:a.range,speed:700,damage:65*spellPower*(1+b('velaSplitDamage')),secondDamage:35*spellPower*(1+b('velaSplitDamage')),pairId:pair.id,pierce:0,damageType:'physical',coveringShot:!!b('velaCoveringShot'),slow:b('velaCoveringShot')?.2:0,slowDuration:1.5});
      return true;
    }
    case 'sidestep':{
      const movement=traceExpansionMovement(game,hero,angle,Math.min(a.range,distance(hero,at))),m=hero.magazine;
      if(m.reloadRemaining>0){m.reloadRemaining=Math.max(0,m.reloadRemaining-.45);if(m.reloadRemaining===0)completeReload(game,hero);}
      else m.rounds=Math.min(6,m.rounds+(b('velaCleanChambers')?2:1));
      if(b('velaSafeLanding'))game.shield(hero,shieldAmount(60),2,hero);
      effect(game,hero,'dash',movement.start,22,.45,{abilityId:a.id,x2:hero.x,y2:hero.y,angle});return true;
    }
    case 'steady-aim':{
      const duration=4+b('velaAimDuration');game.applyStatus(hero,'steadyAim',duration,{rangeBonus:.15,penalty:b('velaPatientAim')?.05:.15,sourceId:hero.id});
      game.applyStatus(hero,'haste',duration,{factor:0,attack:.2,sourceId:hero.id,color:a.color});hero.expansionAimActive=true;game.refreshStats(hero);
      effect(game,hero,'steady-aim',hero,hero.r+20,.65,{abilityId:a.id,followId:hero.id});return true;
    }
    case 'silver-volley':{
      hero.channel={kind:a.id,startedAt:game.time,fireAt:game.time+.4,endsAt:game.time+1.6,nextShot:0,shotCount:6,angle,x:hero.x,y:hero.y,range:a.range,width:a.radius,damage:45*spellPower*(1+b('velaVolleyDamage')),color:a.color};
      scheduleImpact(game,hero,{kind:a.id,shape:'line',x:hero.x,y:hero.y,r:a.range,range:a.range,width:a.radius,angle,delay:.4,color:a.color,damage:0});
      hero.visualAction.duration=1.85;return true;
    }
    default:return false;
  }
}

export function resolveExpansionImpact(game,hero,impact){
  if(resolveSolsticeImpact(game,hero,impact))return true;
  if(impact.kind==='silver-volley')return true;
  if(impact.kind==='anchorline'){
    game.fireSkill(hero,impact.angle,{type:'anchorline',abilityId:'anchorline',expansion:true,visibleOnly:true,terrainCollision:true,color:impact.color,theme:'tide',fromX:hero.x,fromY:hero.y,range:impact.range,r:impact.width,speed:630,damage:impact.damage,damageType:'physical',pierce:0,castId:impact.id});return true;
  }
  if(!['undertow','captains-wake'].includes(impact.kind))return false;
  const wave=impact.kind==='captains-wake',targets=validEnemies(game,hero,impact,wave?impact.range+impact.width:impact.r).filter(u=>!wave||inLine(u,impact));let heroHit=false;
  if(wave&&impact.safeHarbor)for(const ally of game.units)if(ally.alive&&ally.kind==='hero'&&ally.team===hero.team&&inLine(ally,impact))game.shield(ally,impact.safeHarbor,4,hero);
  for(const target of targets){
    heroHit||=target.kind==='hero';game.damage(target,impact.damage*(structure(target)?.35:1),hero,impact.damageType,false,{theme:'tide',abilityId:impact.kind,angle:impact.angle});
    if(!target.alive||structure(target))continue;
    if(wave){
      const along=Math.max(0,Math.min(impact.range,(target.x-impact.x)*Math.cos(impact.angle)+(target.y-impact.y)*Math.sin(impact.angle)));
      pullToward(game,hero,target,{x:impact.x+Math.cos(impact.angle)*along,y:impact.y+Math.sin(impact.angle)*along},90,impact.id);
      game.applyStatus(target,'stun',.9,{sourceId:hero.id});
    }else{
      pullToward(game,hero,target,impact,65,impact.id);game.applyStatus(target,'slow',impact.slowDuration??2.5,{factor:impact.slow??.3,sourceId:hero.id,itemScaled:!!impact.itemScaled});
    }
  }
  if(!wave&&bonus(game,hero,'brineRoughCurrent'))game.zones.push({type:'rough-current',x:impact.x,y:impact.y,r:impact.r,life:2,maxLife:2,tickIn:0,createdAt:game.time,ownerId:hero.id,team:hero.team,color:impact.color,theme:'tide',slow:impact.roughSlow??.2,slowDuration:impact.roughSlowDuration??.1,itemScaled:!!impact.roughItemScaled});
  if(wave&&heroHit&&bonus(game,hero,'brineBoardingParty'))for(const ally of game.units)if(ally.alive&&ally.team===hero.team&&!structure(ally)&&distance(hero,ally)<=300+ally.r)game.applyStatus(ally,'haste',4,{factor:0,attack:.2,sourceId:hero.id,color:impact.color});
  effect(game,hero,impact.kind,impact,wave?impact.width:impact.r,.75,{abilityId:impact.kind,shape:impact.shape,range:impact.range,width:impact.width,angle:impact.angle,x2:wave?impact.x+Math.cos(impact.angle)*impact.range:impact.x,y2:wave?impact.y+Math.sin(impact.angle)*impact.range:impact.y});
  return true;
}

/** Only expansion projectiles add stricter visibility/terrain acquisition. */
export function expansionProjectileDamage(game,p,source,target){
  if(p.abilityId==='split-shot'){
    const pair=source?.expansion?.splitPairs?.find(pair=>pair.id===p.pairId);
    if(pair?.hits.includes(target.id))return p.secondDamage;
  }
  if(source?.subtype==='sentry'){
    const hero=ownerOf(game,source),focus=focusedTarget(game,hero);
    if(focus?.id===target.id&&bonus(game,hero,'rivetCrossfire')&&(source.crossfireReadyAt||0)<=game.time){source.crossfireReadyAt=game.time+5;return p.damage+(source.crossfireDamage||30);}
  }
  return p.damage;
}
export function onExpansionProjectileHit(game,p,source,target,actual){
  const hero=ownerOf(game,source);if(!hero)return;
  if(hero.heroId==='solstice')onSolsticeProjectileHit(game,p,source,target,actual);
  if(p.abilityId==='screwshot'&&p.bounceDamages?.length){
    const bouncedIds=[...(p.bouncedIds||[]),target.id],next=game.units.filter(u=>foe(game,hero,u)&&!bouncedIds.includes(u.id)&&distance(target,u)<=120&&game.clearLine(target,u,4)).sort((a,b)=>distance(target,a)-distance(target,b)||a.id-b.id)[0];
    if(next){const angle=Math.atan2(next.y-target.y,next.x-target.x);game.fireSkill(hero,angle,{type:'screwshot',abilityId:'screwshot',expansion:true,visibleOnly:true,terrainCollision:true,x:target.x,y:target.y,prevX:target.x,prevY:target.y,fromX:target.x,fromY:target.y,targetId:next.id,range:160,r:13,speed:650,damage:p.bounceDamages[0],bounceDamages:p.bounceDamages.slice(1),bouncedIds,damageType:'physical',color:p.color,theme:'copper',pierce:0,hit:new Set(bouncedIds)});}
  }
  if(p.abilityId==='anchorline'){
    if(target.alive&&!structure(target)){
      pullToward(game,hero,target,hero,130,p.castId);game.applyStatus(target,'stun',.55,{sourceId:hero.id});
      if(bonus(game,hero,'brineMarkedCatch'))target.revealedUntil=Math.max(target.revealedUntil||0,game.time+3);
    }
    if(target.kind==='hero'&&hero.alive&&bonus(game,hero,'brineFollowRope'))game.applyStatus(hero,'haste',2,{factor:.15,attack:0,sourceId:hero.id});
    effect(game,hero,'anchor-cable',{x:p.fromX,y:p.fromY},18,.4,{abilityId:'anchorline',x2:target.x,y2:target.y,angle:p.angle,visibilityId:target.id,visibleTo:target.alive?game.visualAudience(target):target.visualDeath?.visibleTo||[]});
  }
  if(p.abilityId==='split-shot'){
    const pair=hero.expansion?.splitPairs?.find(pair=>pair.id===p.pairId);
    if(pair){
      if(!pair.firstHit&&target.alive&&(p.coveringShot??!!bonus(game,hero,'velaCoveringShot')))game.applyStatus(target,'slow',p.slowDuration??1.5,{factor:p.slow??.2,sourceId:hero.id,itemScaled:!!p.itemScaled});
      pair.firstHit=true;if(!pair.hits.includes(target.id))pair.hits.push(target.id);
    }
  }
  if(p.basic&&source?.id===hero.id&&hero.alive){
    if(hero.heroId==='rivet'){
      const sentry=game.units.filter(u=>u.alive&&u.kind==='summon'&&u.subtype==='sentry'&&u.ownerId===hero.id&&u.hp<u.maxHp&&distance(u,hero)<=260).sort((a,b)=>a.hp/a.maxHp-b.hp/b.maxHp||a.id-b.id)[0];
      if(sentry&&ready(game,hero,'field-mechanic',1)){
        sentry.hp=Math.min(sentry.maxHp,sentry.hp+(10+hero.level)*(1+bonus(game,hero,'rivetRepairPower')));
        visibleEffect(game,hero,'repair',sentry,sentry.r+9,.45,{followId:sentry.id,abilityId:'field-mechanic'});
        if(bonus(game,hero,'rivetSharedTools')&&ready(game,hero,'shared-tools',3))game.shield(hero,shieldPower(game,hero,20),2,hero);
      }
    }
    if(hero.heroId==='vela'&&p.sixthRound&&target.kind==='hero'&&bonus(game,hero,'velaPerfectRhythm'))game.shield(hero,shieldPower(game,hero,70),2,hero);
  }
}
export function onExpansionHealthLoss(game,target,source,actual){
  if(!(actual>0)||target.hp<=0)return;
  if(target.subtype==='sentry'&&!target.emergencyPatchUsed&&target.hp<target.maxHp*.35){
    const hero=ownerOf(game,target);if(hero&&bonus(game,hero,'rivetEmergencyPatch')){target.emergencyPatchUsed=true;game.shield(target,target.emergencyShield||shieldPower(game,hero,45),3,hero);}
  }
  if(target.heroId==='vela'&&target.hp<target.maxHp*.3&&bonus(game,target,'velaNeverCornered')&&ready(game,target,'never-cornered',30)){
    game.applyStatus(target,'haste',2,{factor:.25,attack:0,sourceId:target.id});target.cooldowns[1]=Math.max(0,target.cooldowns[1]-3);
  }
}
export function onExpansionKill(game,hero,target){
  if(hero?.alive&&hero.heroId==='vela'&&target.kind==='hero'&&hero.magazine?.reloadRemaining>0&&bonus(game,hero,'velaSpareMagazine')&&ready(game,hero,'spare-magazine',6)){
    hero.magazine.reloadRemaining=Math.max(0,hero.magazine.reloadRemaining-.5);if(hero.magazine.reloadRemaining===0)completeReload(game,hero);
  }
}
export function onExpansionDeath(game,target){
  cancelExpansionChannel(game,target);
  delete target.overclockUntil;
  if(target.heroId==='vela'){target.magazine={rounds:6,reloadRemaining:0,reloadDuration:1.4-bonus(game,target,'velaFastHands'),openingShots:0};delete target.expansionAimActive;}
}
export function tickExpansion(game,dt){
  for(const hero of game.units){
    const channel=hero.channel;if(!channel)continue;
    if(!hero.alive||hero.status.stun||hero.moving||hero.recallIn>0||distance(hero,channel)>.5){cancelExpansionChannel(game,hero);continue;}
    while(channel.nextShot<channel.shotCount&&game.time+1e-8>=channel.fireAt+channel.nextShot*(1.2/5)){
      game.fireSkill(hero,channel.angle,{type:'silver-bullet',abilityId:'silver-volley',expansion:true,visibleOnly:true,terrainCollision:true,color:channel.color,theme:'ivory',range:channel.range,r:channel.width,speed:840,damage:channel.damage,damageType:'physical',pierce:0});channel.nextShot++;
    }
    if(channel.nextShot>=channel.shotCount)delete hero.channel;
  }
  for(const zone of game.zones)if(zone.type==='rough-current'){
    const hero=game.units.find(h=>h.id===zone.ownerId);if(hero&&zone.life>0)for(const target of validEnemies(game,hero,zone,zone.r))if(!structure(target))game.applyStatus(target,'slow',zone.slowDuration??.1,{factor:zone.slow??.2,sourceId:hero.id,itemScaled:!!zone.itemScaled});
  }
  tickSolstice(game,dt);
}
