/** New frontline kits. Uses the same combat, status and effect primitives as the engine. */
import { WORLD, segmentDistance } from './data.mjs';
import { scheduleImpact } from './progression.mjs';

const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const structure=u=>u.kind==='tower'||u.kind==='core';
const enemies=(game,hero)=>game.units.filter(u=>u.alive&&u.team!==hero.team&&!game.isProtected(u));
const point=(u)=>({x:u.x,y:u.y});
const angleDifference=(a,b)=>Math.atan2(Math.sin(a-b),Math.cos(a-b));

// Trace the whole displacement, not only its endpoint, so dashes and knockbacks
// cannot tunnel through jungle trees or rocks. Eight-pixel steps are narrower
// than even the smallest map obstacle; all centers remain inside map bounds.
function travel(game,unit,angle,length){
  const start=point(unit),dx=Math.cos(angle),dy=Math.sin(angle);
  let travelled=0;
  for(let remaining=Math.max(0,length);remaining>0;){
    const step=Math.min(8,remaining),next={x:unit.x+dx*step,y:unit.y+dy*step};
    if(next.x<unit.r+15||next.y<unit.r+15||next.x>WORLD.width-unit.r-15||next.y>WORLD.height-unit.r-15)break;
    if(WORLD.obstacles.some(o=>distance(next,o)<o.r+unit.r+1))break;
    unit.x=next.x;unit.y=next.y;travelled+=step;remaining-=step;
  }
  unit.path=null;unit.pathGoal=null;unit.navPath=null;unit.navGoal=null;
  if(travelled>0){unit.moving=true;unit.recallIn=0;}
  return {start,end:point(unit),travelled};
}
function push(game,target,angle,length){
  if(!target.alive||structure(target))return;
  travel(game,target,angle,length);
}
function inArc(hero,target,angle,radius,arc){
  const d=distance(hero,target);
  return d<=radius+target.r&&Math.abs(angleDifference(Math.atan2(target.y-hero.y,target.x-hero.x),angle))<=arc/2+Math.asin(Math.min(1,target.r/Math.max(target.r,d)));
}
function lineTargets(game,hero,start,angle,range,width){
  const dx=Math.cos(angle),dy=Math.sin(angle),end={x:start.x+dx*range,y:start.y+dy*range};
  return enemies(game,hero).filter(u=>{
    const projection=(u.x-start.x)*dx+(u.y-start.y)*dy;
    return projection>=0&&projection<=range+u.r&&segmentDistance(u.x,u.y,start,end)<=width+u.r&&game.clearLine(start,u,4);
  }).sort((a,b)=>((a.x-start.x)*dx+(a.y-start.y)*dy)-((b.x-start.x)*dx+(b.y-start.y)*dy));
}
function hit(game,hero,target,amount,type='physical'){
  return game.damage(target,amount*(structure(target)?.35:1),hero,type);
}
function dashEffect(game,travelled,color,angle){
  game.effect('dash',travelled.start.x,travelled.start.y,22,color,.5,{x2:travelled.end.x,y2:travelled.end.y,angle});
}

export function castHeroSkill(game,hero,a,{angle,at,power,shieldAmount}){
  const bonus=key=>game.bonus(hero,key);
  switch(a.id){
    case 'shield-bash': {
      const arc=(a.arc??Math.PI*.58)*(bonus('bulwarkWideBash')?1.3:1),targets=enemies(game,hero).filter(u=>inArc(hero,u,angle,a.radius,arc)&&game.clearLine(hero,u,4));
      game.effect('sweep',hero.x,hero.y,a.radius,a.color,.4,{angle,arc});
      for(const target of targets){
        hit(game,hero,target,85*power);
        game.applyStatus(target,'stun',.8+bonus('knightStun'),{sourceId:hero.id});
        push(game,target,angle,105+bonus('bashPush'));
      }
      return true;
    }
    case 'amethyst-guard': {
      const duration=5+bonus('guardDuration'),shield=shieldAmount(210);
      game.shield(hero,shield,duration,hero);game.applyStatus(hero,'guard',duration,{factor:.6,sourceId:hero.id});
      for(const ally of game.units)if(ally.alive&&ally.team===hero.team&&ally.id!==hero.id&&!structure(ally)&&distance(hero,ally)<=a.radius+ally.r){
        game.shield(ally,shield*(.5+bonus('allyBarrier')),duration,hero);
        game.effect('shield',ally.x,ally.y,ally.r+14,a.color,.65,{followId:ally.id});
      }
      game.effect('shield',hero.x,hero.y,hero.r+28,a.color,1,{followId:hero.id});
      game.effect('ring',hero.x,hero.y,a.radius,a.color,.6);
      return true;
    }
    case 'rampart-rush': {
      const movement=travel(game,hero,angle,Math.min(a.range,distance(hero,at)));
      dashEffect(game,movement,a.color,angle);
      const targets=lineTargets(game,hero,movement.start,angle,movement.travelled,a.radius);
      for(const target of targets){
        hit(game,hero,target,105*power*(1+bonus('rushDamage')));
        game.applyStatus(target,'stun',.5+bonus('knightStun'),{sourceId:hero.id});
        push(game,target,angle,90);
      }
      return true;
    }
    case 'royal-quake': {
      const fissure=!!bonus('bulwarkFissure'),fortress=!!bonus('bulwarkFortress');
      scheduleImpact(game,hero,{kind:'royal-quake',x:hero.x,y:hero.y,r:fissure?a.radius*1.7:a.radius,shape:fissure?'cone':'circle',angle,arc:Math.PI*.55,
        color:a.color,delay:.45,damage:135*power*(1+bonus('quakeDamage')),damageType:'physical',stun:1.25+bonus('knightStun'),slow:.3,
        slowDuration:3.5+bonus('knightStun'),push:fissure?125:80,shield:fortress?shieldAmount(90):0,guard:fortress?.82:0,duration:2.5});
      return true;
    }
    case 'heartseeker': {
      const target=lineTargets(game,hero,hero,angle,a.range,a.radius)[0];
      const end=target||{x:hero.x+Math.cos(angle)*a.range,y:hero.y+Math.sin(angle)*a.range};
      game.effect('slash',hero.x,hero.y,30,a.color,.3,{angle,x2:end.x,y2:end.y});
      if(target)hit(game,hero,target,145*power*(1+bonus('heartseekerDamage')));
      return true;
    }
    case 'cinder-dash': {
      const movement=travel(game,hero,angle,Math.min(a.range,distance(hero,at)));
      dashEffect(game,movement,a.color,angle);
      const target=lineTargets(game,hero,movement.start,angle,movement.travelled,a.radius)[0];
      if(target&&movement.travelled>0)hit(game,hero,target,85*power*(1+bonus('dashDamage')));
      game.applyStatus(hero,'haste',2+bonus('dashDuration'),{factor:.4,attack:0,color:a.color});
      return true;
    }
    case 'execution-cut': {
      const target=lineTargets(game,hero,hero,angle,a.range,a.radius)[0];
      const end=target||{x:hero.x+Math.cos(angle)*a.range,y:hero.y+Math.sin(angle)*a.range};
      game.effect('slash',hero.x,hero.y,45,a.color,.4,{angle,x2:end.x,y2:end.y});
      if(target){
        const missing=Math.max(0,Math.min(1,1-target.hp/target.maxHp));
        hit(game,hero,target,155*power*(1+missing*.9)*(1+bonus('executionDamage')));
        game.effect('ring',target.x,target.y,target.r+13,a.color,.35);
      }
      return true;
    }
    case 'flicker-strike': {
      const target=lineTargets(game,hero,hero,angle,a.range,a.radius)[0];
      const approach=target?Math.max(0,((target.x-hero.x)*Math.cos(angle)+(target.y-hero.y)*Math.sin(angle))-hero.r-target.r-8):distance(hero,at);
      const movement=travel(game,hero,angle,Math.min(a.range,approach));
      dashEffect(game,movement,a.color,angle);
      if(target&&distance(hero,target)<=hero.r+target.r+75&&game.clearLine(hero,target,8)){
        hit(game,hero,target,225*power*(1+bonus('flickerDamage')));
        game.effect('sweep',hero.x,hero.y,95,a.color,.45,{angle,arc:Math.PI*.45});
        if(!target.alive){
          hero.cooldowns[1]=0;
          if(bonus('killBarrier'))game.shield(hero,shieldAmount(bonus('killBarrier')),4,hero);
          if(hero.isPlayer)game.float(hero,'DASH READY',a.color);
        }
      }
      return true;
    }
    case 'glacial-sweep': {
      const arc=a.arc??Math.PI*.85;
      game.effect('sweep',hero.x,hero.y,a.radius,a.color,.6,{angle,arc});
      for(const target of enemies(game,hero).filter(u=>inArc(hero,u,angle,a.radius,arc)&&game.clearLine(hero,u,4))){
        hit(game,hero,target,100*power*(1+bonus('sweepDamage')),'magic');
        game.applyStatus(target,'slow',2.6+bonus('frostDuration'),{factor:.35,frost:true,sourceId:hero.id});
      }
      return true;
    }
    case 'wingburst': {
      const movement=travel(game,hero,angle,Math.min(a.range,distance(hero,at)));
      dashEffect(game,movement,a.color,angle);
      const duration=3.5+bonus('wingDuration');
      game.applyStatus(hero,'haste',duration,{factor:.45,attack:0,color:a.color});
      game.shield(hero,shieldAmount(90),duration,hero);
      game.effect('shield',hero.x,hero.y,hero.r+16,a.color,.7,{followId:hero.id});
      return true;
    }
    case 'northern-inspire': {
      const duration=5+bonus('inspireDuration');
      game.effect('ring',at.x,at.y,a.radius,a.color,1.1);
      for(const ally of game.units)if(ally.alive&&ally.team===hero.team&&!structure(ally)&&distance(at,ally)<=a.radius+ally.r){
        game.applyStatus(ally,'haste',duration,{factor:.25,attack:.3,color:a.color});
        game.shield(ally,shieldAmount(65)*(1+bonus('inspireShield')),duration,hero);
        game.effect('shield',ally.x,ally.y,ally.r+10,a.color,.6,{followId:ally.id});
      }
      return true;
    }
    case 'winterfall': {
      scheduleImpact(game,hero,{kind:'winterfall',x:at.x,y:at.y,r:a.radius,color:a.color,delay:.5,damage:140*power*(1+bonus('winterDamage')),
        stun:.65+bonus('freezeStun'),slow:.5+bonus('frostSlow'),slowDuration:4+bonus('frostDuration')});
      return true;
    }
    default:return false;
  }
}

/** New bots preserve their combat target and use escape skills toward their well. */
export function botSkillAim(game,hero,a,target,retreat,cfg){
  const friends=game.units.filter(u=>u.alive&&u.team===hero.team&&!structure(u)&&distance(hero,u)<350);
  const foes=enemies(game,hero).filter(u=>!structure(u)&&distance(hero,u)<350&&(!game.canSee||game.canSee(hero.team,u)));
  if(target&&game.canSee&&!game.canSee(hero.team,target))target=null;
  const lead=target?{x:target.x+(target.vx||0)*.15*cfg.lead,y:target.y+(target.vy||0)*.15*cfg.lead}:point(hero);
  const near=!!target&&target.alive&&distance(hero,target)<=a.range+target.r;
  switch(a.id){
    case 'amethyst-guard':return foes.length>0&&(hero.hp/hero.maxHp<.85||friends.some(u=>u.kind==='hero'&&u.hp/u.maxHp<.7))?point(hero):null;
    case 'shield-bash':return target&&distance(hero,target)<=a.radius+target.r?lead:null;
    case 'royal-quake':return foes.some(u=>distance(hero,u)<a.radius+u.r)&&(!retreat||foes.some(u=>u.kind==='hero'))?point(hero):null;
    case 'rampart-rush':return target&&!retreat&&near&&distance(hero,target)>95&&hero.hp/hero.maxHp>.48?lead:null;
    case 'heartseeker':case 'execution-cut':return target&&!retreat&&near?lead:null;
    case 'cinder-dash':case 'wingburst':
      if(retreat&&foes.length)return point(WORLD.bases[hero.team].well);
      return target&&!retreat&&near&&distance(hero,target)>135&&hero.hp/hero.maxHp>.55?lead:null;
    case 'flicker-strike':return target&&!retreat&&near&&hero.hp/hero.maxHp>.55&&!structure(target)?lead:null;
    case 'glacial-sweep':return target&&distance(hero,target)<=a.radius+target.r?lead:null;
    case 'northern-inspire':return target&&distance(hero,target)<350&&friends.length>1?point(hero):null;
    case 'winterfall':return target&&near&&(target.kind==='hero'||foes.length>=2)?lead:null;
    default:return null;
  }
}

