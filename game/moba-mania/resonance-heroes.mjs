/** Magnetic resources and musical sequences. All lasting state is bounded JSON. */
import { scheduleImpact } from './progression.mjs';
import { traceExpansionMovement } from './hero-expansion.mjs';
import { modifyItemStatus } from './items.mjs';

const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const structure=u=>u?.kind==='tower'||u?.kind==='core';
const bonus=(game,hero,key)=>game.bonus(hero,key)||0;
const power=(game,hero)=>(1+(hero.level-1)*.075)*(1+bonus(game,hero,'damage'))*(1+bonus(game,hero,'spellDamage'));
const shieldPower=(game,hero,n)=>n*(1+bonus(game,hero,'shield'))*(1+(hero.level-1)*.08);
const theme=hero=>hero.heroId==='ferrite'?'magnet':'resonance';
const color=hero=>hero.heroId==='ferrite'?'#65d9e6':'#ef9da8';
const visibleFoes=(game,hero,at,r)=>game.units.filter(u=>u.alive&&u.team!==hero.team&&!game.isProtected(u)&&game.canSee(hero.team,u)&&distance(u,at)<=r+u.r&&game.clearLine(at,u,4));
const friends=(game,hero,at,r)=>game.units.filter(u=>u.alive&&u.kind==='hero'&&u.team===hero.team&&distance(u,at)<=r+u.r&&game.clearLine(at,u,4));
function visual(game,hero,type,at,r,life=.6,extra={}){
  return game.effect(type,at.x,at.y,r,color(hero),life,{team:hero.team,ownerId:hero.id,theme:theme(hero),abilityId:type,...extra});
}
function hit(game,hero,target,n,abilityId,proc=false){
  const deal=()=>game.damage(target,n*(structure(target)?.35:1),hero,'magic',false,{abilityId,theme:theme(hero),proc});
  if(!proc)return deal();
  game.progressionDepth++;try{return deal();}finally{game.progressionDepth--;}
}
function blast(game,hero,at,r,n,abilityId,proc=false){
  const targets=visibleFoes(game,hero,at,r);for(const target of targets)hit(game,hero,target,n,abilityId,proc);return targets;
}

export function initResonanceHero(hero){
  if(hero?.heroId==='ferrite')hero.resonance??={scrap:0,polarity:'push',scrapReadyAt:0};
  if(hero?.heroId==='cadence')hero.resonance??={notes:[],chordUntil:0,finaleNote:0,chordNote:0};
}
const score=(game,hero)=>hero.resonance.notes.filter(n=>game.time-n.at<=8+bonus(game,hero,'cadenceNoteWindow'));
const fullChord=notes=>notes.length===3&&new Set(notes.map(n=>n.slot)).size===3;
function note(game,hero,slot){
  const state=hero.resonance;state.notes=[...score(game,hero),{slot,at:game.time}].slice(-3);state.finaleNote=slot;
  if(fullChord(state.notes)){
    state.chordUntil=game.time+8;state.chordNote=slot;
    visual(game,hero,'charged-chord',hero,hero.r+27,.65,{followId:hero.id,finaleNote:slot});
    if(hero.isPlayer)game.float(hero,'CHORD READY','#ffedbf');
  }
}
function slowSnapshot(game,hero,factor,duration){
  if(!factor)return {slow:0,slowDuration:duration,itemScaled:true};
  const scaled=modifyItemStatus(game,{team:1-hero.team},'slow',duration,{factor,sourceId:hero.id});
  return {slow:scaled.extra.factor,slowDuration:scaled.duration,itemScaled:true};
}
function salvage(game,hero){
  if(!hero?.alive||hero.heroId!=='ferrite')return false;
  initResonanceHero(hero);const state=hero.resonance;
  if(state.scrap>=3||state.scrapReadyAt>game.time)return false;
  state.scrap++;state.scrapReadyAt=game.time+.65;
  visual(game,hero,'scrap-orbit',hero,hero.r+25,.4,{followId:hero.id,scrap:state.scrap,polarity:state.polarity});return true;
}
function movePole(game,target,at,polarity,maximum){
  if(!target.alive||structure(target)||target.kind==='beast'||target.stationary)return;
  const d=distance(target,at);if(d<1)return;
  const toward=Math.atan2(at.y-target.y,at.x-target.x),pull=polarity==='pull';
  traceExpansionMovement(game,target,pull?toward:toward+Math.PI,pull?Math.min(maximum,Math.max(0,d-target.r-24)):maximum,{pull:true});
}

export function adjustResonanceAbility(game,hero,ability){
  const a={...ability};
  if(a.id==='rail-spike')a.range*=1+bonus(game,hero,'ferriteRailRange');
  if(a.id==='polarity-pulse')a.radius*=1+bonus(game,hero,'ferritePulseArea');
  if(a.id==='sheltering-chime')a.radius*=1+bonus(game,hero,'cadenceChimeArea');
  if(a.id==='skipbeat')a.range*=1+bonus(game,hero,'cadenceStepRange');
  if(a.id==='grand-finale'&&hero.resonance&&fullChord(score(game,hero)))a.radius*=1.2;
  return a;
}

export function castResonanceSkill(game,hero,a,{angle,at,power:spellPower,shieldAmount}){
  if(!['ferrite','cadence'].includes(hero.heroId))return false;
  initResonanceHero(hero);const state=hero.resonance,b=key=>bonus(game,hero,key);
  switch(a.id){
    case 'rail-spike':
      game.fireSkill(hero,angle,{type:a.id,abilityId:a.id,expansion:true,visibleOnly:true,terrainCollision:true,color:a.color,theme:'magnet',range:a.range,r:a.radius,speed:650,damage:90*spellPower*(1+b('ferriteRailDamage')),damageType:'physical',pierce:b('ferritePierce'),resonanceSalvaged:false,...slowSnapshot(game,hero,b('ferriteStaticPoint')?.2:0,1.5)});return true;
    case 'scrap-barrier':{
      const spent=state.scrap;state.scrap=0;
      const amount=shieldAmount((100+spent*(50+b('ferriteScrapShield')))*(1+b('ferriteBarrierPower')));
      game.shield(hero,amount,4,hero);
      if(b('ferriteSharedAlloy'))for(const ally of friends(game,hero,hero,a.radius))if(ally.id!==hero.id)game.shield(ally,amount*.4,4,hero);
      if(b('ferriteInsulated')&&(!hero.status.guard||hero.status.guard.factor>=.85))game.applyStatus(hero,'guard',2,{factor:.85,sourceId:hero.id});
      if(spent===3&&b('ferriteLighten'))game.applyStatus(hero,'haste',2,{factor:.2,attack:0,sourceId:hero.id});
      visual(game,hero,a.id,hero,hero.r+25,.8,{followId:hero.id,scrap:spent,polarity:state.polarity});return true;
    }
    case 'polarity-pulse':
      state.polarity=state.polarity==='pull'?'push':'pull';
      scheduleImpact(game,hero,{kind:a.id,x:hero.x,y:hero.y,r:a.radius,delay:.35,color:a.color,theme:'magnet',damage:75*spellPower,polarity:state.polarity,scrap:state.scrap,induction:!!b('ferriteInduction')});
      return true;
    case 'lodestone-storm':{
      const spent=state.scrap;state.scrap=0;
      scheduleImpact(game,hero,{kind:a.id,x:at.x,y:at.y,r:a.radius,delay:.8,color:a.color,theme:'magnet',damage:(150+35*spent)*spellPower,polarity:state.polarity,scrap:spent,
        push:70+20*spent+(b('ferriteMassDriver')?45:0),slow:.3,slowDuration:2+(b('ferriteMassDriver')?1:0),aftershock:!!b('ferriteAftershock'),monolith:b('ferriteMonolith')?shieldAmount(90+35*spent):0});
      return true;
    }
    case 'piercing-note':
      note(game,hero,0);game.fireSkill(hero,angle,{type:a.id,abilityId:a.id,expansion:true,visibleOnly:true,terrainCollision:true,color:a.color,theme:'resonance',range:a.range,r:a.radius,speed:590,damage:75*spellPower*(1+b('cadenceNoteDamage')),damageType:'magic',pierce:2,resonanceEchoed:false,resonanceEchoAvailable:!!b('cadenceReverberation'),echoDamage:25*spellPower,...slowSnapshot(game,hero,b('cadenceHoldingNote')?.2:0,1.2)});return true;
    case 'sheltering-chime':
      note(game,hero,1);
      for(const ally of friends(game,hero,at,a.radius)){
        game.shield(ally,shieldAmount(75*(1+b('cadenceChimePower'))),3,hero);
        if(b('cadenceOvertone'))game.applyStatus(ally,'haste',2,{factor:0,attack:.15,sourceId:hero.id});
      }
      visual(game,hero,a.id,at,a.radius,.8,{finaleNote:1});return true;
    case 'skipbeat':{
      note(game,hero,2);
      const moved=traceExpansionMovement(game,hero,angle,Math.min(a.range,distance(hero,at)));
      game.applyStatus(hero,'haste',2+b('cadenceStepDuration'),{factor:.2,attack:0,sourceId:hero.id});
      if(b('cadenceSoftLanding'))game.shield(hero,shieldAmount(50),2,hero);
      visual(game,hero,a.id,moved.start,24,.6,{x2:hero.x,y2:hero.y,angle,finaleNote:2});return true;
    }
    case 'grand-finale':{
      const notes=score(game,hero),mode=notes.at(-1)?.slot??0,charged=fullChord(notes);
      state.notes=[];state.chordUntil=0;state.finaleNote=mode;
      scheduleImpact(game,hero,{kind:a.id,x:at.x,y:at.y,r:a.radius,delay:.65,color:a.color,theme:'resonance',damage:120*spellPower,finaleNote:mode,charged,
        echoDamage:(80+b('cadenceEncore'))*spellPower,shield:shieldAmount(160*(b('cadenceGrandShelter')?1.25:1)),cleanse:!!b('cadenceGrandShelter'),duration:4+b('cadenceLastDance'),slow:mode===2?.35:0,slowDuration:.15});return true;
    }
    default:return false;
  }
}

export function resolveResonanceImpact(game,hero,impact){
  if(!['polarity-pulse','lodestone-storm','lodestone-echo','grand-finale','finale-echo','note-echo'].includes(impact.kind))return false;
  const targets=blast(game,hero,impact,impact.r,impact.damage,impact.kind,!!impact.proc);
  if(impact.kind==='polarity-pulse'){
    for(const target of targets)movePole(game,target,impact,impact.polarity,impact.polarity==='pull'?75:100);
    if(impact.induction&&targets.some(u=>!structure(u)))salvage(game,hero);
  }
  if(impact.kind==='lodestone-storm'){
    for(const target of targets)if(target.alive&&!structure(target)){
      movePole(game,target,impact,impact.polarity,impact.push);
      game.applyStatus(target,'slow',impact.slowDuration,{factor:impact.slow,sourceId:hero.id,itemScaled:!!impact.itemScaled});
    }
    if(impact.monolith&&hero.alive)game.shield(hero,impact.monolith,4,hero);
    if(impact.aftershock)scheduleImpact(game,hero,{kind:'lodestone-echo',x:impact.x,y:impact.y,r:impact.r*.75,delay:.6,color:impact.color,theme:'magnet',damage:impact.damage*.55,proc:true,interruptible:false,polarity:impact.polarity,scrap:impact.scrap});
  }
  if(impact.kind==='grand-finale'){
    if(impact.finaleNote===0)scheduleImpact(game,hero,{kind:'finale-echo',x:impact.x,y:impact.y,r:impact.r,delay:.55,color:'#ef9da8',theme:'resonance',damage:impact.echoDamage,proc:true,interruptible:false,finaleNote:0});
    if(impact.finaleNote===1)for(const ally of friends(game,hero,impact,impact.r)){
      if(impact.cleanse)delete ally.status.slow;
      game.shield(ally,impact.shield,4,hero);
    }
    if(impact.finaleNote===2){
      // One stage per owner: replacing it cannot stack haste or extend an old field.
      game.zones=game.zones.filter(z=>z.type!=='resonance-stage'||z.ownerId!==hero.id);
      game.zones.push({type:'resonance-stage',x:impact.x,y:impact.y,r:impact.r,life:impact.duration,maxLife:impact.duration,tickIn:0,createdAt:game.time,team:hero.team,ownerId:hero.id,color:'#ebb9f0',theme:'resonance',finaleNote:2,slow:impact.slow,slowDuration:impact.slowDuration,itemScaled:!!impact.itemScaled});
      visual(game,hero,'dance-stage',impact,impact.r,impact.duration,{finaleNote:2});
    }
  }
  visual(game,hero,impact.kind,impact,impact.r,.7,{polarity:impact.polarity,scrap:impact.scrap,finaleNote:impact.finaleNote});return true;
}

/** The projectile owns its one-shot flags, so pierces cannot multiply resources. */
export function onResonanceProjectileHit(game,p,source,target,actual){
  if(!(actual>0)||!source?.heroId)return;
  const hero=source;
  if(hero.heroId==='ferrite'&&p.abilityId==='rail-spike'){
    if(!structure(target)&&!p.resonanceSalvaged){p.resonanceSalvaged=true;salvage(game,hero);}
    if(target.alive&&!structure(target)&&p.slow)game.applyStatus(target,'slow',p.slowDuration,{factor:p.slow,sourceId:hero.id,itemScaled:!!p.itemScaled});
  }
  if(hero.heroId==='cadence'&&p.abilityId==='piercing-note'){
    if(target.alive&&!structure(target)&&p.slow)game.applyStatus(target,'slow',p.slowDuration,{factor:p.slow,sourceId:hero.id,itemScaled:!!p.itemScaled});
    if(target.kind==='hero'&&!p.resonanceEchoed&&p.resonanceEchoAvailable){
      p.resonanceEchoed=true;scheduleImpact(game,hero,{kind:'note-echo',x:target.x,y:target.y,r:65,delay:.4,color:'#ef9da8',theme:'resonance',damage:p.echoDamage,proc:true,interruptible:false,finaleNote:0});
    }
  }
}

export function onResonanceDamage(game,source,target,actual,context={}){
  if(!(actual>0)||!source?.alive||source.kind!=='hero'||!context.basic||context.proc||context.periodic||structure(target)||game.progressionDepth)return;
  initResonanceHero(source);
  if(source.heroId==='ferrite')salvage(game,source);
  if(source.heroId==='cadence'&&source.resonance.chordUntil>game.time){
    const state=source.resonance;state.chordUntil=0;
    blast(game,source,target,95,45*power(game,source)*(1+bonus(game,source,'cadenceChordDamage')),'charged-chord',true);
    if(bonus(game,source,'cadenceSharedRhythm'))game.applyStatus(source,'haste',2,{factor:.15,attack:.15,sourceId:source.id});
    visual(game,source,'charged-chord',target,95,.6,{finaleNote:state.chordNote});
  }
}

export function tickResonanceHero(game,hero,dt){
  initResonanceHero(hero);
  if(hero.heroId==='cadence'){
    hero.resonance.notes=score(game,hero);
    if(hero.resonance.chordUntil<=game.time)hero.resonance.chordUntil=0;
  }
}
export function tickResonance(game,dt){
  for(const zone of game.zones)if(zone.type==='resonance-stage'&&zone.life>0){
    const hero=game.units.find(u=>u.kind==='hero'&&u.id===zone.ownerId);if(!hero)continue;
    for(const ally of friends(game,hero,zone,zone.r))game.applyStatus(ally,'haste',.15,{factor:.2,attack:.25,sourceId:hero.id});
    for(const target of visibleFoes(game,hero,zone,zone.r))if(!structure(target))game.applyStatus(target,'slow',zone.slowDuration,{factor:zone.slow,sourceId:hero.id,itemScaled:!!zone.itemScaled});
  }
}
export function onResonanceDeath(game,hero){
  if(hero?.heroId==='ferrite')hero.resonance={scrap:0,polarity:'push',scrapReadyAt:0};
  if(hero?.heroId==='cadence')hero.resonance={notes:[],chordUntil:0,finaleNote:0,chordNote:0};
}
