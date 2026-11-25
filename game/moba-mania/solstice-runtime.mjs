/** Solstice's tracked protection and one-shot cooldown refunds. All state is JSON. */
import { HEROES } from './data.mjs';
import { scheduleImpact, ensureEvolution } from './progression.mjs';

const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const structure=u=>u?.kind==='tower'||u?.kind==='core';
const b=(game,hero,key)=>game.bonus(hero,key)||0;
const ownerOf=(game,ward)=>game.units.find(u=>u.id===ward?.ownerId&&u.kind==='hero');
const activeWard=(game,target)=>target?.timeWard&&game.time<target.timeWard.expiresAt?target.timeWard:null;
const shieldPower=(game,hero)=>(1+b(game,hero,'shield'))*(1+(hero.level-1)*.08);
const visibleEnemy=(game,hero,target)=>target.alive&&target.team!==hero.team&&!game.isProtected(target)&&game.canSee(hero.team,target);
const wardEffect=(game,hero,target,abilityId,kind='shield',r=target.r+20)=>game.effect(kind,target.x,target.y,r,'#f4dea0',.6,{team:hero.team,ownerId:hero.id,followId:target.id,visibilityId:target.id,theme:'time',abilityId});

/** Shared by cast validation and the aim preview; only allied living heroes qualify. */
export function selectTimeWardTarget(game,hero,at){
  if(!hero?.alive||!Number.isFinite(at?.x)||!Number.isFinite(at?.y))return null;
  const a=game.getAbility(hero,1);
  if(a?.id!=='time-ward')return null;
  return game.units.filter(u=>u.alive&&u.kind==='hero'&&u.team===hero.team&&distance(hero,u)<=a.range&&distance(at,u)<=75)
    .sort((x,y)=>distance(at,x)-distance(at,y)||x.id-y.id)[0]||null;
}

/** Remove only this ward's unused shield, preserving other casters' layers. */
export function clearSolsticeWard(game,target){
  const ward=target?.timeWard;if(!ward)return;
  let removed=0;
  target.shieldLayers=(target.shieldLayers||[]).filter(layer=>{
    if(layer.timeWardId!==ward.shieldId)return true;
    removed+=Math.max(0,layer.amount);return false;
  });
  target.shield=Math.max(0,(target.shield||0)-removed);
  if(target.shield<=0){target.shieldTime=0;delete target.shieldVisual;}
  delete target.timeWard;
}

export function castSolsticeSkill(game,hero,a,{at,angle,power,shieldAmount}){
  if(hero.heroId!=='solstice')return false;
  switch(a.id){
    case 'hourhand':
      game.fireSkill(hero,angle,{type:'hourhand',abilityId:a.id,expansion:true,visibleOnly:true,terrainCollision:true,theme:'time',color:a.color,
        r:a.radius,range:a.range,speed:560,damage:90*power*(1+b(game,hero,'solsticeHourDamage')),damageType:'magic',pierce:0,
        slow:.25,slowDuration:2+b(game,hero,'solsticeSandDuration'),echoDamage:45*power,lingeringSand:!!b(game,hero,'solsticeLingeringSand'),glassEcho:!!b(game,hero,'solsticeGlassEcho')});
      return true;
    case 'time-ward': {
      const target=selectTimeWardTarget(game,hero,at);if(!target)return false;
      clearSolsticeWard(game,target); // An overwritten ward never pays its delayed heal.
      const ward={ownerId:hero.id,startedAt:game.time,expiresAt:game.time+3,shieldId:`${hero.id}:${target.id}:${game.time}`,
        absorbed:0,healthLost:0,triggered:false,shieldPower:shieldPower(game,hero),healPower:1+b(game,hero,'healing'),
        healCap:70+b(game,hero,'solsticeHealCap'),guardFactor:b(game,hero,'solsticeSteadyHour')?.9:1};
      target.timeWard=ward;
      const previous=new Set(target.shieldLayers||[]);
      game.shield(target,shieldAmount(120)*(1+b(game,hero,'solsticeWardPower')),3,hero);
      for(const layer of target.shieldLayers||[])if(!previous.has(layer)&&layer.sourceId===hero.id){layer.timeWardId=ward.shieldId;layer.startedAt=game.time;}
      wardEffect(game,hero,target,a.id);
      return true;
    }
    case 'unwind':
      game.effect('ring',at.x,at.y,a.radius,a.color,.7,{team:hero.team,ownerId:hero.id,abilityId:a.id,theme:'time'});
      for(const ally of game.units)if(ally.alive&&ally.team===hero.team&&!structure(ally)&&distance(at,ally)<=a.radius+ally.r){
        delete ally.status.slow;
        if(ally.status.stun){ally.status.stun.remaining=Math.max(0,ally.status.stun.remaining-.5);if(ally.status.stun.remaining<=0)delete ally.status.stun;}
        // ccRecoveryUntil is intentionally untouched: cleansing cannot reset stun resilience.
        game.shield(ally,shieldAmount(60),2,hero);
        if(b(game,hero,'solsticeQuickRecovery'))game.applyStatus(ally,'haste',2,{factor:.15,attack:0,sourceId:hero.id,color:a.color});
        wardEffect(game,hero,ally,a.id);
      }
      return true;
    case 'borrowed-seconds':
      scheduleImpact(game,hero,{kind:a.id,x:at.x,y:at.y,r:a.radius,shape:'circle',color:a.color,theme:'time',delay:.7,
        damage:90*power*(b(game,hero,'solsticeSunsetDial')?1.25:1),slow:b(game,hero,'solsticeSunsetDial')?.4:.35,slowDuration:3,
        refundLimit:2+b(game,hero,'solsticeGenerousSeconds'),shield:b(game,hero,'solsticeGoldenInterval')?shieldAmount(100):0,duration:3});
      return true;
    default:return false;
  }
}

/** Runs once per scheduled impact, so remaining in the circle never refunds again. */
export function resolveSolsticeImpact(game,hero,impact){
  if(!['borrowed-seconds','glass-echo'].includes(impact.kind))return false;
  game.effect('ring',impact.x,impact.y,impact.r,impact.color,.65,{team:hero.team,ownerId:hero.id,abilityId:impact.kind,theme:'time'});
  for(const target of game.units){
    if(!target.alive||distance(target,impact)>impact.r+target.r)continue;
    if(target.team!==hero.team){
      if(!visibleEnemy(game,hero,target)||!game.clearLine(impact,target,4))continue;
      const context={theme:'time',abilityId:impact.kind,angle:Math.atan2(target.y-impact.y,target.x-impact.x)};
      // The echo is a secondary spell result and cannot recursively trigger item procs.
      if(impact.kind==='glass-echo')game.progressionDepth=(game.progressionDepth||0)+1;
      try{game.damage(target,impact.damage*(structure(target)?.35:1),hero,'magic',false,context);}
      finally{if(impact.kind==='glass-echo')game.progressionDepth--;}
      if(target.alive&&impact.slow)game.applyStatus(target,'slow',impact.slowDuration,{factor:impact.slow,sourceId:hero.id});
    }else if(impact.kind==='borrowed-seconds'&&target.kind==='hero'){
      for(let slot=0;slot<3;slot++){
        const remaining=target.cooldowns[slot],raw=HEROES[target.heroId].abilities[slot];
        if(remaining>.6)target.cooldowns[slot]=Math.max(.6,remaining-Math.min(impact.refundLimit,raw.cooldown*.25));
      }
      if(impact.shield)game.shield(target,impact.shield,impact.duration,hero);
      wardEffect(game,hero,target,impact.kind,'ring',target.r+18);
    }
  }
  return true;
}

export function onSolsticeProjectileHit(game,p,source,target,actual){
  if(p.abilityId!=='hourhand'||source?.heroId!=='solstice'||target.team===source.team)return;
  if(target.alive&&!structure(target))game.applyStatus(target,'slow',p.slowDuration||2,{factor:p.slow||.25,sourceId:source.id});
  if(p.lingeringSand){
    // A normal cooldown cannot fill this pool; the cap also bounds forced debug casts.
    const owned=game.zones.filter(z=>z.type==='time-sand'&&z.ownerId===source.id);
    if(owned.length>=3)game.zones=game.zones.filter(z=>z!==owned[0]);
    game.zones.push({type:'time-sand',x:target.x,y:target.y,r:85,ownerId:source.id,team:source.team,createdAt:game.time,expiresAt:game.time+2,
      life:2,maxLife:2,tickIn:2,nextPulseAt:game.time,slow:.2,theme:'time',color:'#bfa5e2'});
  }
  if(p.glassEcho)scheduleImpact(game,source,{kind:'glass-echo',x:target.x,y:target.y,r:60,shape:'circle',color:'#f4dea0',theme:'time',delay:.6,
    interruptible:false,damage:p.echoDamage||0,proc:true});
}

/** Only tagged, actually absorbed hostile damage can trigger Perfect Timing. */
export function onSolsticeAbsorb(game,target,layer,value,attacker){
  const ward=activeWard(game,target);
  if(!ward||layer.timeWardId!==ward.shieldId||!(value>0)||!attacker||attacker.team===target.team||game.time-ward.startedAt>1+1e-8)return;
  ward.absorbed+=value;
  if(ward.triggered||ward.absorbed<30)return;
  const hero=ownerOf(game,ward);if(!hero?.alive||hero.heroId!=='solstice')return;
  const state=ensureEvolution(hero);
  if((state.readyAt['perfect-timing']||0)>game.time)return;
  ward.triggered=true;state.readyAt['perfect-timing']=game.time+6;
  hero.mana=Math.min(hero.maxMana,hero.mana+8+b(game,hero,'solsticePreciseTiming'));
  hero.cooldowns[0]=Math.max(0,hero.cooldowns[0]-1-b(game,hero,'solsticeSecondHand'));
  wardEffect(game,hero,target,'perfect-timing','ring',target.r+28);
  if(b(game,hero,'solsticeSharedMoment')){
    const ally=game.units.filter(u=>u.alive&&u.kind==='hero'&&u.team===hero.team&&u.id!==target.id&&distance(u,target)<=160)
      .sort((x,y)=>x.hp/x.maxHp-y.hp/y.maxHp||x.id-y.id)[0];
    if(ally){game.shield(ally,35*shieldPower(game,hero),2,hero);wardEffect(game,hero,ally,'perfect-timing');}
  }
}

export function onSolsticeHealthLoss(game,target,source,actual){
  const ward=activeWard(game,target);
  if(ward&&actual>0&&source?.team!==target.team)ward.healthLost+=actual;
}

export function solsticeDamageFactor(game,target){
  const ward=activeWard(game,target);
  return ward&&game.time-ward.startedAt<1?ward.guardFactor||1:1;
}

export function tickSolstice(game,dt){
  for(const target of game.units){
    const ward=target.timeWard;if(!ward)continue;
    if(!target.alive){clearSolsticeWard(game,target);continue;}
    if(game.time+1e-8<ward.expiresAt)continue;
    const hero=ownerOf(game,ward);
    clearSolsticeWard(game,target);
    if(!hero||!(ward.healthLost>0))continue;
    // Snapshot the ward's healing strength, then let the existing heal API cap
    // missing health, record support credit and apply item healing hooks once.
    const amount=Math.min(ward.healthLost*.25,ward.healCap)*ward.healPower/(1+b(game,hero,'healing'));
    const healed=game.heal(target,amount,hero);
    if(healed>0)game.effect('heal',target.x,target.y,target.r+24,'#f4dea0',.75,{team:hero.team,ownerId:hero.id,followId:target.id,visibilityId:target.id,
      x2:hero.x,y2:hero.y,theme:'time',abilityId:'time-return'});
  }
  // Ordinary tickZones owns life decrement/filtering; this hook only applies the
  // field's occupied-space slow and never damages or reveals a concealed unit.
  for(const field of game.zones){
    if(field.type!=='time-sand'||field.life<=0||game.time>=field.expiresAt||game.time+1e-8<field.nextPulseAt)continue;
    field.nextPulseAt=game.time+.1;
    const hero=game.units.find(u=>u.id===field.ownerId);if(!hero)continue;
    for(const target of game.units)if(!structure(target)&&visibleEnemy(game,hero,target)&&distance(target,field)<=field.r+target.r&&game.clearLine(field,target,4))
      game.applyStatus(target,'slow',.15,{factor:.2,sourceId:hero.id});
  }
}
