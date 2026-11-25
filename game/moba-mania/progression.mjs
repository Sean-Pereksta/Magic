/** Hero identities, late evolutions and equipment effects. State is plain JSON for match resume. */
import { HEROES, WORLD, segmentDistance } from './data.mjs';
import { modifyItemStatus } from './items.mjs';

const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const structure=u=>u?.kind==='tower'||u?.kind==='core';
const aliveHero=u=>u?.kind==='hero'&&u.alive;
const has=(hero,id)=>hero?.items?.includes(id);
const bonus=(game,hero,key)=>game.bonus(hero,key)||0;
const power=hero=>1+(hero.level-1)*.055;
const owns=(game,source)=>source?.kind==='hero'?source:game.units.find(u=>u.id===source?.ownerId&&u.kind==='hero');
const color=hero=>HEROES[hero.heroId]?.color||'#ffe6a8';

export function ensureEvolution(hero){
  if(!hero)return null;
  const state=hero.evolution??={};
  state.readyAt??={};state.counts??={};state.marks??={};state.castHistory??=[];
  return state;
}
function ready(game,hero,key,seconds){
  const state=ensureEvolution(hero);
  if((state.readyAt[key]??-Infinity)>game.time)return false;
  state.readyAt[key]=game.time+seconds;return true;
}
function count(hero,key){const state=ensureEvolution(hero);return state.counts[key]=(state.counts[key]||0)+1;}
function proc(game,fn){game.progressionDepth=(game.progressionDepth||0)+1;try{return fn();}finally{game.progressionDepth--;}}
function enemies(game,hero,at,r){return game.units.filter(u=>u.alive&&u.team!==hero.team&&!game.isProtected(u)&&distance(u,at)<=r+u.r);}
function allies(game,hero,at,r){return game.units.filter(u=>u.alive&&u.team===hero.team&&!structure(u)&&distance(u,at)<=r+u.r);}
function hit(game,hero,target,amount,type='magic'){
  if(!target?.alive||target.team===hero.team||game.isProtected(target))return 0;
  return proc(game,()=>game.damage(target,amount*(structure(target)?.3:1),hero,type));
}
function heal(game,hero,target,amount){return proc(game,()=>game.heal(target,amount,hero,true));}
function ward(game,hero,target,amount,seconds=3){return proc(game,()=>game.shield(target,amount,seconds,hero));}
function ring(game,hero,at,r,amount,type='magic'){
  game.effect('ring',at.x,at.y,r,color(hero),.45,{team:hero.team,theme:game.combatTheme(hero,type)});
  for(const target of enemies(game,hero,at,r))hit(game,hero,target,amount,type);
}
function protect(game,hero,at,r,amount,seconds=3){
  for(const ally of allies(game,hero,at,r))ward(game,hero,ally,amount,seconds);
  game.effect('shield',at.x,at.y,r,color(hero),.5,{team:hero.team,theme:game.combatTheme(hero)});
}
function healingPulse(game,hero,at,r,amount){
  for(const ally of allies(game,hero,at,r))heal(game,hero,ally,amount);
  game.effect('heal',at.x,at.y,r,color(hero),.65,{team:hero.team,theme:game.combatTheme(hero)});
}
function haste(game,hero,at,r,factor,attack,duration){
  for(const ally of allies(game,hero,at,r))game.applyStatus(ally,'haste',duration,{factor,attack,sourceId:hero.id,color:color(hero)});
}
function slowRing(game,hero,at,r,factor,duration){
  for(const target of enemies(game,hero,at,r))if(!structure(target))game.applyStatus(target,'slow',duration,{factor,sourceId:hero.id,frost:hero.heroId==='seraph'});
  game.effect(hero.heroId==='seraph'?'frost':'ring',at.x,at.y,r,color(hero),.6,{team:hero.team,theme:game.combatTheme(hero)});
}
function shove(game,target,angle,length){
  if(!target.alive||structure(target))return;
  const dx=Math.cos(angle),dy=Math.sin(angle);
  for(let left=length;left>0;){
    const step=Math.min(8,left),x=target.x+dx*step,y=target.y+dy*step;
    if(x<target.r+15||y<target.r+15||x>WORLD.width-target.r-15||y>WORLD.height-target.r-15||WORLD.obstacles.some(o=>Math.hypot(x-o.x,y-o.y)<o.r+target.r+1))break;
    target.x=x;target.y=y;left-=step;
  }
  target.path=null;target.pathGoal=null;target.navPath=null;target.navGoal=null;
}

/** No functions/closures are stored: warnings and future impacts survive JSON save/restore. */
export function scheduleImpact(game,hero,details){
  details={...details};
  if(!details.itemScaled){
    const hostile={team:1-hero.team};
    for(const type of ['burn','poison'])if(details[type]){
      const adjusted=modifyItemStatus(game,hostile,type,details[type+'Duration']||3,{dps:details[type],sourceId:hero.id});
      details[type]=adjusted.extra.dps;details[type+'Duration']=adjusted.duration;
    }
    if(details.slow){
      const adjusted=modifyItemStatus(game,hostile,'slow',details.slowDuration||3,{factor:details.slow,sourceId:hero.id});
      details.slow=adjusted.extra.factor;details.slowDuration=adjusted.duration;
    }
    details.itemScaled=true;
  }
  const delay=Math.max(.08,Number(details.delay)||.35);
  const impact={id:game.nextId++,ownerId:hero.id,team:hero.team,x:hero.x,y:hero.y,r:80,shape:'circle',color:color(hero),theme:game.combatTheme(hero,details.damageType||'magic'),
    interruptible:true,...details,delay,executeAt:game.time+delay};
  if(hero.visualAction?.kind==='cast'&&hero.visualAction.startedAt===game.time&&hero.visualAction.abilityId===impact.kind){
    hero.visualAction.impactAt=impact.executeAt;hero.visualAction.duration=delay+.3;
  }
  (game.scheduledImpacts??=[]).push(impact);return impact;
}
function inShape(target,impact){
  if(impact.shape==='line'){
    const end={x:impact.x+Math.cos(impact.angle)*(impact.range||impact.r),y:impact.y+Math.sin(impact.angle)*(impact.range||impact.r)};
    const projection=(target.x-impact.x)*Math.cos(impact.angle)+(target.y-impact.y)*Math.sin(impact.angle);
    return projection>=0&&projection<=(impact.range||impact.r)+target.r&&segmentDistance(target.x,target.y,impact,end)<=(impact.width||18)+target.r;
  }
  if(distance(target,impact)>impact.r+target.r)return false;
  if(impact.shape!=='cone')return true;
  const angle=Math.atan2(target.y-impact.y,target.x-impact.x),difference=Math.atan2(Math.sin(angle-impact.angle),Math.cos(angle-impact.angle));
  return Math.abs(difference)<=(impact.arc||Math.PI*.65)/2+Math.asin(Math.min(1,target.r/Math.max(target.r,distance(target,impact))));
}
function resolveImpact(game,hero,impact){
  if(game.resolveHeroImpact?.(hero,impact))return;
  if(impact.kind==='heal-bloom'){
    healingPulse(game,hero,impact,impact.r,impact.healing||0);
    if(impact.shield)protect(game,hero,impact,impact.r,impact.shield,impact.duration||3);
    return;
  }
  const visual=['winterfall','winter-field'].includes(impact.kind)?'frost':impact.kind==='inferno-chain'?'flame':impact.kind==='pandemic-burst'?'poison':impact.kind==='royal-quake'?'quake':impact.shape==='line'?'slash':impact.shape==='cone'?'sweep':'ring';
  game.effect(visual,impact.x,impact.y,impact.r,impact.color,.65,{team:hero.team,ownerId:hero.id,abilityId:impact.kind,theme:impact.theme||game.combatTheme(hero),angle:impact.angle,arc:impact.arc,
    x2:impact.x+Math.cos(impact.angle||0)*(impact.range||impact.r),y2:impact.y+Math.sin(impact.angle||0)*(impact.range||impact.r)});
  const radius=impact.shape==='line'?(impact.range||impact.r)+30:impact.r;
  let spreadCount=0;
  const targets=enemies(game,hero,impact,radius).sort((a,b)=>distance(a,impact)-distance(b,impact)||a.id-b.id);
  for(const target of targets)if(inShape(target,impact)&&game.clearLine(impact,target,4)){
    const deal=()=>game.damage(target,(impact.damage||0)*(structure(target)?.35:1),hero,impact.damageType||'magic',false,{theme:impact.theme,angle:impact.shape==='line'||impact.shape==='cone'?impact.angle:Math.atan2(target.y-impact.y,target.x-impact.x)});
    if(impact.proc)proc(game,deal);else deal();
    if(!target.alive||structure(target))continue;
    if(impact.stun)game.applyStatus(target,'stun',impact.stun,{sourceId:hero.id});
    if(impact.slow)game.applyStatus(target,'slow',impact.slowDuration||3,{factor:impact.slow,frost:['winterfall','winter-field'].includes(impact.kind),sourceId:hero.id,itemScaled:!!impact.itemScaled});
    const spreadEligible=impact.maxSpread===undefined||(target.id!==impact.primaryId&&spreadCount<impact.maxSpread);
    if(spreadEligible&&(impact.burn||impact.poison)){
      if(impact.burn)game.applyStatus(target,'burn',impact.burnDuration||3,{dps:impact.burn,sourceId:hero.id,proc:!!impact.proc,itemScaled:!!impact.itemScaled});
      if(impact.poison)game.applyStatus(target,'poison',impact.poisonDuration||3,{dps:impact.poison,sourceId:hero.id,proc:!!impact.proc,itemScaled:!!impact.itemScaled});
      spreadCount++;
    }
    if(impact.push)shove(game,target,impact.shape==='cone'?impact.angle:Math.atan2(target.y-impact.y,target.x-impact.x),impact.push);
  }
  if(impact.shield)protect(game,hero,impact,impact.r,impact.shield,impact.duration||3);
  if(impact.guard)for(const ally of allies(game,hero,impact,impact.r))game.applyStatus(ally,'guard',impact.duration||2,{factor:impact.guard,sourceId:hero.id});
}
export function tickProgression(game,dt){
  if(!game.scheduledImpacts?.length)return;
  const due=[],pending=[];
  for(const impact of game.scheduledImpacts){
    const hero=game.units.find(u=>u.id===impact.ownerId);
    if(!hero||(impact.interruptible&&(!hero.alive||hero.status.stun)))continue;
    (impact.executeAt<=game.time+1e-8?due:pending).push(impact);
  }
  game.scheduledImpacts=pending;
  for(const impact of due){
    const hero=game.units.find(u=>u.id===impact.ownerId);
    if(hero&&game.winner===null)resolveImpact(game,hero,impact);
  }
}

export function onCast(game,hero,ability,{at,angle,slot=0,power:spellPower}={}){
  if(!aliveHero(hero))return;
  const state=ensureEvolution(hero),b=key=>bonus(game,hero,key),p=power(hero),point=at||hero;
  const castNumber=count(hero,'casts');
  state.castHistory=state.castHistory.filter(c=>game.time-c.time<=6).slice(-15);state.castHistory.push({slot,time:game.time});
  const distinctCasts=new Set(state.castHistory.map(c=>c.slot)).size;
  const mobility=ability.mobility||['rift-step','sky-cleave'].includes(ability.id);
  if(mobility){
    state.mobilityReadyUntil=game.time+3.5;
    if(has(hero,'tempest-wings')){state.tempestReadyUntil=game.time+4;game.applyStatus(hero,'haste',3,{factor:.25,attack:.15,sourceId:hero.id,color:'#a1efda'});}
  }
  if(has(hero,'eternity-hourglass')&&distinctCasts>=3&&ready(game,hero,'hourglass',12)){
    hero.cooldowns=hero.cooldowns.map((cd,index)=>index===slot||cd<=.5?cd:Math.max(.5,cd-2.5));state.castHistory=[];
    game.effect('ring',hero.x,hero.y,85,'#8fe3e8',.65,{team:hero.team,theme:'time'});game.float(hero,'TIME SURGE','#8fe3e8');
  }
  if(has(hero,'celestial-font')&&castNumber%3===0&&ready(game,hero,'font',5)){
    hero.mana=Math.min(hero.maxMana,hero.mana+hero.maxMana*.10);healingPulse(game,hero,hero,190,55+hero.level*4);protect(game,hero,hero,190,45*p,3);
  }
  if(hero.heroId==='arc'){
    if(castNumber%3===0){
      ring(game,hero,point,85,b('arcConduit')?62*p:38*p);hero.mana=Math.min(hero.maxMana,hero.mana+14);
      if(b('arcSanctum'))protect(game,hero,point,140,65*p,3);
    }
    if(ability.id==='blue-storm'&&b('arcEcho'))scheduleImpact(game,hero,{kind:'echo',x:point.x,y:point.y,r:ability.radius*.7,delay:1.05,damage:44*p,proc:true,interruptible:false});
    if(ability.id==='forcefield'){
      if(b('arcWardHaste'))haste(game,hero,point,ability.radius,.17,.1,3);
      if(b('arcWardHeal'))healingPulse(game,hero,point,ability.radius,45*p);
      if(b('arcSanctum'))protect(game,hero,point,ability.radius,85*p,4);
    }
    if(ability.id==='rift-step'&&b('arcCataclysm'))scheduleImpact(game,hero,{kind:'rift-nova',x:hero.x,y:hero.y,r:165,delay:.45,damage:100*p,slow:.3,slowDuration:2,proc:true});
  }else if(hero.heroId==='dragon'){
    if(b('dragonTrail')&&ability.id==='sky-cleave')scheduleImpact(game,hero,{kind:'embers',x:hero.x,y:hero.y,r:130,delay:.65,damage:40*p,burn:12*p,burnDuration:3,proc:true});
    if(ability.id==='dragon-shield'&&b('dragonWard'))protect(game,hero,hero,160,65*p,4);
    if(b('dragonHeal')&&ready(game,hero,'hearth',6))healingPulse(game,hero,hero,130,30*p);
    if(b('dragonHaste')&&ability.id==='sky-cleave')haste(game,hero,hero,170,.18,.12,3);
    if(b('dragonWorldfire')&&['dragon-flight','sky-cleave'].includes(ability.id))scheduleImpact(game,hero,{kind:'worldfire',x:point.x,y:point.y,r:150,delay:.65,damage:65*p,burn:16*p,proc:true});
    if(b('dragonMeteor'))state.dragonMeteorUntil=game.time+4;
  }else if(hero.heroId==='druid'){
    if(ability.id==='healing-tree'){
      if(b('druidRoot'))slowRing(game,hero,point,ability.radius,.35,2.5);
      if(b('druidBark'))protect(game,hero,point,ability.radius,55*p,4);
      if(b('druidAncient'))scheduleImpact(game,hero,{kind:'heal-bloom',x:point.x,y:point.y,r:ability.radius*1.25,delay:1,healing:90*p,shield:75*p,interruptible:false});
    }
    if(b('druidCycle')&&['treants','bear','wolves'].includes(ability.id))hero.cooldowns[0]=Math.max(0,hero.cooldowns[0]-1.5);
  }else if(hero.heroId==='angel'){
    if(ability.id==='inspire'){
      if(b('angelChorus'))haste(game,hero,point,ability.radius,.3,.36,5);
      if(b('angelChoir')){haste(game,hero,point,ability.radius,.35,.42,5);healingPulse(game,hero,point,ability.radius,50*p);}
    }
    if(ability.id==='sanctuary'){
      if(b('angelShelter'))for(const ally of allies(game,hero,point,ability.radius))game.applyStatus(ally,'guard',2,{factor:.87,sourceId:hero.id});
      if(b('angelSanctum'))healingPulse(game,hero,point,ability.radius,75*p);
    }
    if(ability.id==='seraph-lance'&&b('angelLance'))scheduleImpact(game,hero,{kind:'heal-bloom',x:point.x,y:point.y,r:135,delay:.5,healing:70*p,interruptible:false});
    if(b('angelDawn')&&castNumber%3===0){ring(game,hero,hero,180,65*p);healingPulse(game,hero,hero,180,45*p);}
  }else if(hero.heroId==='ranger'){
    if(ability.id==='long-shot'&&(b('rangerPierce')||b('rangerDeadfall')))scheduleImpact(game,hero,{kind:'phantom-arrow',shape:'line',x:hero.x,y:hero.y,r:ability.range,range:ability.range,width:14,angle,delay:b('rangerDeadfall')?.35:.6,damage:(b('rangerDeadfall')?100:55)*p,proc:true,interruptible:false});
    if(ability.id==='stun-bomb'&&b('rangerEscape'))haste(game,hero,hero,0,.25,0,2.5);
    if(ability.id==='venom-arrow'&&b('rangerBlight'))scheduleImpact(game,hero,{kind:'blight',x:point.x,y:point.y,r:110,delay:.6,damage:30*p,poison:14*p,slow:.3,slowDuration:3,proc:true});
  }else if(hero.heroId==='bulwark'){
    if(ability.id==='amethyst-guard'){
      if(b('bulwarkKindness'))healingPulse(game,hero,hero,ability.radius,40*p);
      if(b('bulwarkShelter'))for(const ally of allies(game,hero,hero,ability.radius))if(ally.id!==hero.id)game.applyStatus(ally,'guard',2.5,{factor:.85,sourceId:hero.id});
      if(b('bulwarkUnbroken'))state.unbrokenUntil=game.time+5;
    }
    if(ability.id==='shield-bash'&&b('bulwarkWideBash'))protect(game,hero,hero,140,35*p,2.5);
    if(ability.id==='rampart-rush'&&b('bulwarkRushSlow'))slowRing(game,hero,hero,130,.35,2);
  }else if(hero.heroId==='ember'){
    if(mobility&&b('emberAfterimage'))ward(game,hero,hero,35*p,2.5);
    if(mobility&&b('emberSmoke'))slowRing(game,hero,hero,100,.35,2);
    if(ability.id==='execution-cut'&&b('emberQuickstep'))haste(game,hero,hero,0,.24,0,2.5);
    if(b('emberRhythm')&&distinctCasts>=3&&ready(game,hero,'ember-rhythm',9))hero.cooldowns[1]=Math.max(0,hero.cooldowns[1]-2);
  }else if(hero.heroId==='seraph'){
    if(ability.id==='wingburst'&&b('seraphIceTrail'))slowRing(game,hero,hero,120,.4,2.5);
    if(ability.id==='northern-inspire'){
      if(b('seraphRallyGuard'))for(const ally of allies(game,hero,point,ability.radius))game.applyStatus(ally,'guard',2,{factor:.88,sourceId:hero.id});
      if(b('seraphAurora'))healingPulse(game,hero,point,ability.radius,65*p);
      if(b('seraphHeaven')){protect(game,hero,point,ability.radius,90*p,4);haste(game,hero,point,ability.radius,.32,.36,5);}
    }
    if(ability.id==='glacial-sweep'&&b('seraphSwordEcho'))scheduleImpact(game,hero,{kind:'frost-echo',shape:'cone',x:hero.x,y:hero.y,r:ability.radius*.9,angle,arc:ability.arc,delay:.35,damage:42*p,slow:.2,slowDuration:1.5,proc:true,interruptible:false});
    if(ability.id==='winterfall'&&b('seraphWinterKing'))scheduleImpact(game,hero,{kind:'winter-echo',x:point.x,y:point.y,r:ability.radius*1.1,delay:1.15,damage:60*p,slow:.35,slowDuration:2,proc:true});
  }
}

export function onSummon(game,hero,summon,index=0){
  if(!aliveHero(hero)||!summon?.alive)return;
  const b=key=>bonus(game,hero,key),p=power(hero);
  if(index===0&&has(hero,'elderwood-staff')){
    summon.maxHp*=1.4;summon.hp=summon.maxHp;summon.damage*=1.25;summon.elderGuardian=true;summon.scale=(summon.scale||1)*1.25;
    ward(game,hero,summon,100*p,6);game.effect('shield',summon.x,summon.y,summon.r+20,'#b8e69d',.85,{followId:summon.id,team:hero.team,theme:'nature'});
  }
  if(hero.heroId==='arc'){
    if(b('arcSummonWard'))ward(game,hero,summon,50*p,5);
    if(index===0&&b('arcCaptain')){summon.maxHp*=1.25;summon.hp=summon.maxHp;summon.damage*=1.3;summon.range=175;summon.scale=(summon.scale||1)*1.2;summon.captain=true;}
  }
  if(hero.heroId==='druid'){
    if(index===0){ward(game,hero,summon,35*p,4);heal(game,hero,hero,14*p);}
    if(b('druidGuardian')&&index===0){summon.maxHp*=1.18;summon.hp=summon.maxHp;ward(game,hero,summon,50*p,5);}
    if(b('druidHunt')&&summon.subtype==='wolf')game.applyStatus(summon,'haste',5,{factor:.15,attack:.15,sourceId:hero.id});
    if(b('druidMoon')&&summon.subtype==='wolf')summon.lifetime+=4;
    if(b('druidSovereign')&&summon.subtype==='bear'&&index===0){summon.maxHp*=1.3;summon.hp=summon.maxHp;summon.damage*=1.25;summon.scale=(summon.scale||1)*1.2;}
    if(b('druidConvergence')){game.applyStatus(summon,'haste',6,{factor:.12,attack:.2,sourceId:hero.id});summon.lifetime+=3;}
  }
}

export function onDamage(game,source,target,actual,{basic=false,periodic=false}={}){
  if(!(actual>0))return;
  // Defensive identities react to real health damage, including damage from non-heroes.
  if(aliveHero(target)){
    const victim=ensureEvolution(target),vp=power(target),vb=key=>bonus(game,target,key);
    if(target.heroId==='bulwark'){
      victim.retaliateUntil=game.time+4;
      if(vb('bulwarkUnbroken')&&victim.unbrokenUntil>game.time&&target.hp<target.maxHp*.35&&ready(game,target,'unbroken',30)){
        ward(game,target,target,95*vp,3);heal(game,target,target,target.maxHp*.08);victim.unbrokenUntil=0;
      }
    }
    if(target.heroId==='dragon'&&vb('dragonPhoenix')&&target.hp<target.maxHp*.3&&ready(game,target,'phoenix-scale',32)){ward(game,target,target,100*vp,3);healingPulse(game,target,target,120,30*vp);}
    if(target.heroId==='ember'&&vb('emberSurvival')&&target.hp<target.maxHp*.3&&ready(game,target,'last-cinder',28)){ward(game,target,target,85*vp,3);haste(game,target,target,0,.25,0,2);}
  }
  const hero=owns(game,source);if(!aliveHero(hero)||target.team===hero.team)return;
  const state=ensureEvolution(hero),b=key=>bonus(game,hero,key),p=power(hero),direct=source?.id===hero.id;
  if(source?.kind==='summon'){
    if(source.elderGuardian&&has(hero,'elderwood-staff')&&basic&&ready(game,hero,'elder-pulse',3)){
      for(const foe of enemies(game,hero,target,105))if(foe.id!==target.id&&!structure(foe)&&game.clearLine(target,foe,4))hit(game,hero,foe,source.damage*.5,'magic');
      game.effect('quake',target.x,target.y,105,'#b8e69d',.5,{team:hero.team,theme:'nature'});
    }
    if(hero.heroId==='arc'&&b('arcSummonSiphon')&&ready(game,hero,'spirit-siphon',2))heal(game,hero,hero,12*p);
    if(hero.heroId==='druid'&&ready(game,hero,'wild-bond',4)){const ally=allies(game,hero,hero,220).filter(u=>u.kind==='hero'&&u.hp<u.maxHp).sort((a,b)=>a.hp/a.maxHp-b.hp/b.maxHp)[0];if(ally)heal(game,hero,ally,12*p);}
  }
  if(!direct)return;
  if(has(hero,'starfire-scepter')&&!basic&&!periodic&&ready(game,hero,'starfire',5))scheduleImpact(game,hero,{kind:'starfire',x:target.x,y:target.y,r:135,delay:.45,damage:80*p,proc:true,interruptible:false,color:'#c3adff',theme:'arcane'});
  if(has(hero,'winters-dominion')&&!structure(target)&&!basic&&!periodic&&ready(game,hero,'winter-field',7))scheduleImpact(game,hero,{kind:'winter-field',x:target.x,y:target.y,r:145,delay:.55,damage:45*p,slow:.30,slowDuration:2.5,proc:true,interruptible:false,color:'#b1edff',theme:'frost'});
  if(periodic&&!structure(target)){
    for(const [id,type,key,ratio,r,damage,cooldown,theme,ink] of [
      ['inferno-heart','burn','inferno-chain',.5,135,55,6,'fire','#ffb079'],
      ['pandemic-engine','poison','pandemic-burst',.6,150,65,7,'poison','#c6e991'],
    ]){
      const effect=target.status?.[type];
      if(has(hero,id)&&effect?.sourceId===hero.id&&!effect.proc&&effect.dps>0&&effect.remaining>0&&ready(game,hero,key,cooldown)){
        scheduleImpact(game,hero,{kind:key,x:target.x,y:target.y,r,delay:.5,damage:damage*p,damageType:type==='burn'?'fire':'poison',
          [type]:effect.dps*ratio,[type+'Duration']:Math.min(3,effect.remaining),itemScaled:true,maxSpread:3,primaryId:target.id,
          proc:true,interruptible:false,color:ink,theme});
      }
    }
  }
  if(basic){
    if(has(hero,'worldsplitter')&&count(hero,'worldsplitter')%3===0){
      const angle=Math.atan2(target.y-hero.y,target.x-hero.x);
      hit(game,hero,target,hero.damage*.35,'physical');
      for(const foe of enemies(game,hero,hero,175))if(foe.id!==target.id&&inShape(foe,{...hero,r:175,shape:'cone',angle,arc:Math.PI*.7})&&game.clearLine(hero,foe,4))hit(game,hero,foe,hero.damage*.75,'physical');
      game.effect('sweep',hero.x,hero.y,175,'#f6d18b',.55,{angle,arc:Math.PI*.7,team:hero.team,theme:'steel'});
    }
    if(has(hero,'thunderbolt-crossbow')&&count(hero,'crossbow')%3===0){
      hit(game,hero,target,35*p);
      const chained=enemies(game,hero,target,230).filter(u=>u.id!==target.id&&!structure(u)&&game.canSee(hero.team,u)&&game.clearLine(target,u,4)).sort((a,b)=>distance(target,a)-distance(target,b)||a.id-b.id).slice(0,3);
      for(const foe of chained){hit(game,hero,foe,55*p);game.effect('slash',target.x,target.y,15,'#ffd585',.4,{x2:foe.x,y2:foe.y,angle:Math.atan2(foe.y-target.y,foe.x-target.x),team:hero.team,theme:'arcane'});}
    }
    if(has(hero,'tempest-wings')&&state.tempestReadyUntil>game.time){state.tempestReadyUntil=0;hit(game,hero,target,60*p,'physical');game.applyStatus(target,'slow',2,{factor:.25,sourceId:hero.id});game.effect('explosion',target.x,target.y,58,'#a1efda',.5,{team:hero.team,theme:'tide'});}
    if(has(hero,'crimson-relic')&&target.kind==='hero'&&hero.hp<hero.maxHp*.6&&ready(game,hero,'crimson',7)){heal(game,hero,hero,hero.maxHp*.10);ward(game,hero,hero,85*p,3);game.effect('ring',hero.x,hero.y,65,'#f59eb4',.6,{team:hero.team,theme:'holy'});}
    if(hero.heroId==='dragon'&&target.status?.burn){
      if(count(hero,'cinder-bite')%3===0)ring(game,hero,target,85,(b('dragonBite')?34:20)*p,'fire');
      if(b('dragonExecute')&&target.hp<target.maxHp*.35&&ready(game,hero,'dragon-execute',4))hit(game,hero,target,28*p,'fire');
    }
    if(hero.heroId==='dragon'&&state.dragonMeteorUntil>game.time&&b('dragonMeteor')){state.dragonMeteorUntil=0;ring(game,hero,target,110,45*p,'fire');}
    if(hero.heroId==='ranger'){
      if(state.lastMarkedId!==target.id){state.lastMarkedId=target.id;state.counts['ranger-mark']=0;}
      if(count(hero,'ranger-mark')%3===0){
        game.applyStatus(target,'poison',3,{dps:7*p,sourceId:hero.id,proc:true});target.revealedUntil=Math.max(target.revealedUntil||0,game.time+(b('rangerScout')?5:2.5));
        if(b('rangerNightHunter')){hit(game,hero,target,38*p,'physical');heal(game,hero,hero,22*p);}
      }
      if(b('rangerVenomSlow')&&target.status.poison&&ready(game,hero,'venom-slow',4))game.applyStatus(target,'slow',1.5,{factor:.25,sourceId:hero.id});
      if(b('rangerPredator')&&target.kind==='hero'&&target.hp<target.maxHp*.35&&ready(game,hero,'predator',4))hit(game,hero,target,32*p,'physical');
    }
    if(hero.heroId==='bulwark'&&state.retaliateUntil>game.time&&ready(game,hero,'resolve',7)){
      state.retaliateUntil=0;ward(game,hero,hero,(b('bulwarkRetaliation')?65:38)*p,3);
      if(b('bulwarkResolve'))hit(game,hero,target,30*p,'physical');
    }
    if(hero.heroId==='ember'){
      if(state.mobilityReadyUntil>game.time){state.mobilityReadyUntil=0;hit(game,hero,target,18*p,'physical');haste(game,hero,hero,0,.12,0,1.5);if(b('emberDance'))ring(game,hero,target,100,35*p,'physical');}
      if(b('emberIsolation')&&target.kind==='hero'&&!game.units.some(u=>u.alive&&u.id!==target.id&&u.team===target.team&&u.kind==='hero'&&distance(u,target)<180)&&ready(game,hero,'isolated',3))hit(game,hero,target,24*p,'physical');
    }
    if(hero.heroId==='seraph'&&count(hero,'winter-rhythm')%3===0){
      game.applyStatus(target,'slow',1.4,{factor:.2,sourceId:hero.id,frost:true});
      if(target.alive)hit(game,hero,target,(b('seraphFrostblade')?36:18)*p);
      haste(game,hero,hero,130,.1,.08,1.8);
      if(b('seraphGlacialCrown'))protect(game,hero,hero,160,45*p,2.5);
    }
    if(hero.heroId==='angel'&&b('angelJudgement')&&state.judgementUntil>game.time){state.judgementUntil=0;hit(game,hero,target,35*p);}
  }
  if(hero.heroId==='ranger'&&b('rangerToxicChain')&&target.status?.poison&&ready(game,hero,'toxic-chain',5)){
    const next=enemies(game,hero,target,150).find(u=>u.id!==target.id&&!structure(u));if(next){game.applyStatus(next,'poison',3,{dps:10*p,sourceId:hero.id,proc:true});game.effect('poison',next.x,next.y,35,color(hero),.5);}
  }
  if(hero.heroId==='ember'&&b('emberExecution')&&!periodic&&target.kind==='hero'&&target.alive&&target.hp<target.maxHp*.25&&ready(game,hero,'execution',6))hit(game,hero,target,35*p,'physical');
}

export function onHeal(game,source,target,actual){
  const hero=owns(game,source);if(!aliveHero(hero)||!(actual>=6)||target.team!==hero.team)return;
  const state=ensureEvolution(hero),p=power(hero),b=key=>bonus(game,hero,key);
  if(has(hero,'phoenix-crown')&&target.kind==='hero'&&ready(game,hero,'phoenix-crown',5)){
    ward(game,hero,target,90*p,4);
    const ally=allies(game,hero,target,210).filter(u=>u.kind==='hero'&&u.id!==target.id&&u.hp<u.maxHp).sort((a,b)=>a.hp/a.maxHp-b.hp/b.maxHp||a.id-b.id)[0];
    if(ally)heal(game,hero,ally,40*p);
    game.effect('heal',target.x,target.y,95,'#ffba91',.8,{team:hero.team,theme:'holy'});
  }
  if(hero.heroId==='angel'){
    state.judgementUntil=game.time+4;
    if(count(hero,'devotion')%3===0&&ready(game,hero,'devotion',3)){hero.mana=Math.min(hero.maxMana,hero.mana+10);haste(game,hero,target,110,.12,.08,2);}
    if(b('angelWard')&&ready(game,hero,'grace-ward',4))ward(game,hero,target,30*p,3);
    if(b('angelEcho')&&ready(game,hero,'grace-echo',5)){
      const ally=allies(game,hero,target,170).filter(u=>u.id!==target.id&&u.kind==='hero'&&u.hp<u.maxHp).sort((a,b)=>a.hp/a.maxHp-b.hp/b.maxHp)[0];if(ally)heal(game,hero,ally,Math.min(actual*.4,45*p));
    }
  }
}
export function onShield(game,source,target,added){
  const hero=owns(game,source);if(!aliveHero(hero)||!(added>=12)||target.team!==hero.team)return;
  const p=power(hero);
  if(has(hero,'sovereign-aegis')&&ready(game,hero,'sovereign',7)){
    const recipients=allies(game,hero,target,240).filter(u=>u.kind==='hero'&&u.id!==target.id).sort((a,b)=>a.hp/a.maxHp-b.hp/b.maxHp||a.id-b.id).slice(0,2);
    for(const ally of recipients){ward(game,hero,ally,90*p,4);if(!ally.status.guard||ally.status.guard.factor>.85)game.applyStatus(ally,'guard',2.5,{factor:.85,sourceId:hero.id});}
    if(recipients.length)game.effect('shield',target.x,target.y,120,'#d1b1f3',.7,{team:hero.team,theme:'violet'});
  }
  if(hero.heroId==='angel'&&count(hero,'shield-devotion')%3===0&&ready(game,hero,'devotion',3))haste(game,hero,target,110,.12,.08,2);
}
/** Kill buffs contain no damage and may safely run for a finisher from an item proc. */
export function onItemKill(game,hero,target){
  if(!aliveHero(hero)||target?.kind!=='hero')return;
  if(has(hero,'ascendant-codex')&&ready(game,hero,'ascension',12)){
    ward(game,hero,hero,100*power(hero),4);
    hero.cooldowns=hero.cooldowns.map((cd,slot)=>slot===3||cd<=.5?cd:Math.max(.5,cd-2));
    game.effect('level',hero.x,hero.y,90,'#b3c4ff',.85,{team:hero.team,theme:'arcane'});game.float(hero,'ASCENSION','#b3c4ff');
  }
  if(has(hero,'kings-ransom')&&ready(game,hero,'victors-spoils',10)){
    game.applyStatus(hero,'haste',4,{factor:.25,attack:.20,sourceId:hero.id,color:'#ffe092'});
    game.effect('ring',hero.x,hero.y,75,'#ffe092',.75,{team:hero.team,theme:'time'});game.float(hero,'VICTOR\'S SPOILS','#ffe092');
  }
}
export function onKill(game,hero,target){
  if(!aliveHero(hero)||target?.kind!=='hero')return;
  if(hero.heroId==='ember'&&bonus(game,hero,'emberReset')){hero.cooldowns[1]=0;hero.cooldowns[2]*=.5;game.float(hero,'FRESH OPENING','#ffca83');}
  if(hero.heroId==='seraph'&&bonus(game,hero,'seraphTriumph'))healingPulse(game,hero,hero,200,55*power(hero));
}
