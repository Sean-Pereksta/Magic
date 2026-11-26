import { FRONTIER_HEROES } from './hero-frontier-data.mjs';
import { scheduleImpact } from './progression.mjs';

const distance = (a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const inRange = (a,b,range)=>distance(a,b)<=range+(b.r||0);
const heroesNear = (game,hero,at,r)=>game.units.filter(unit=>
  unit.kind==='hero' && unit.alive && unit.team===hero.team && distance(unit,at)<=r+unit.r);

function travel(game, hero, at) {
  const start={x:hero.x,y:hero.y};
  const steps=Math.max(1,Math.ceil(distance(start,at)/12));
  for(let i=1;i<=steps;i++){
    const next={x:start.x+(at.x-start.x)*i/steps,y:start.y+(at.y-start.y)*i/steps};
    const safe=game.safePoint(next,hero.r);
    if(distance(safe,next)>1||!game.clearLine(hero,next,hero.r))break;
    hero.x=next.x;
    hero.y=next.y;
  }
  game.effect('slash',start.x,start.y,48,FRONTIER_HEROES[hero.heroId].color,.4,{x2:hero.x,y2:hero.y,angle:hero.angle});
}

export function castFrontierSkill(game,hero,ability,{at,angle,power,zone,area,shieldAmount}) {
  if(!FRONTIER_HEROES[hero.heroId])return false;
  const {id,color,range,radius}=ability;
  const impact=(point,details)=>scheduleImpact(game,hero,{
    kind:id,x:point.x,y:point.y,r:radius,color,...details
  });
  const fire=(direction,details)=>game.fireSkill(hero,direction,{
    type:'bolt',color,r:radius,range,speed:560,pierce:0,damageType:'magic',...details
  });
  const status=(unit,type,duration,extra)=>game.applyStatus(unit,type,duration,{sourceId:hero.id,...extra});
  switch(id){
    case 'spore-dart':
      fire(angle,{damage:56*power,poison:13*power,poisonDuration:3,damageType:'poison'});
      break;
    case 'bramble-bed':
      zone('tree',4,{healing:0,damage:20*power,slow:.3,slowDuration:1.15,color});
      game.effect('tree',at.x,at.y,radius,color,4,{team:hero.team});
      break;
    case 'marsh-remedy':
      for(const ally of heroesNear(game,hero,at,radius))game.heal(ally,80+hero.level*7,hero);
      game.effect('heal',at.x,at.y,radius,color,.8,{team:hero.team});
      break;
    case 'bloom-tide':
      impact(at,{delay:.65,damage:105*power,poison:19*power,poisonDuration:3,damageType:'poison'});
      break;
    case 'bronze-peal':
      impact(hero,{shape:'cone',angle,arc:1.35,r:Math.min(range,radius),delay:.18,damage:82*power,slow:.28,slowDuration:1.7});
      break;
    case 'bell-guard':
      for(const ally of heroesNear(game,hero,hero,radius)){
        game.shield(ally,shieldAmount(80),3,hero);
        status(ally,'guard',1.4,{factor:.85});
      }
      game.effect('shield',hero.x,hero.y,radius,color,.6,{team:hero.team});
      break;
    case 'clapper-charge':
      travel(game,hero,at);
      area(hero,radius,95*power,'physical');
      game.effect('quake',hero.x,hero.y,radius,color,.55,{team:hero.team});
      break;
    case 'great-silence':
      impact(hero,{delay:.75,damage:142*power,stun:1.1});
      break;
    case 'returning-quill': {
      const length=Math.max(35,Math.min(range,distance(hero,at)));
      fire(angle,{range:length,damage:69*power,pierce:5,damageType:'physical'});
      const end={x:hero.x+Math.cos(angle)*length,y:hero.y+Math.sin(angle)*length};
      if(game.clearLine(hero,end,4)){
        impact(end,{shape:'line',range:length,r:length,width:15,angle:angle+Math.PI,delay:length/560+.22,damage:48*power,damageType:'physical',interruptible:false});
      }
      break;
    }
    case 'wing-vault':
      travel(game,hero,at);
      area(hero,radius,45*power,'physical',{type:'slow',duration:1.15,factor:.26});
      game.effect('ring',hero.x,hero.y,radius,color,.55,{team:hero.team});
      break;
    case 'slipstream':
      status(hero,'haste',3,{factor:.27,attack:.35});
      game.effect('ring',hero.x,hero.y,65,color,.55,{team:hero.team,followId:hero.id});
      break;
    case 'gale-cross':
      impact(hero,{shape:'cone',angle,arc:1.5,r:Math.min(range,radius),delay:.45,damage:125*power,push:82,damageType:'physical'});
      break;
    case 'facet-bolt':
      for(const offset of [-.19,0,.19])fire(angle+offset,{damage:48*power});
      break;
    case 'mirror-veil':
      game.shield(hero,shieldAmount(108),3,hero);
      status(hero,'haste',1.6,{factor:.22});
      game.effect('shield',hero.x,hero.y,65,color,.5,{team:hero.team,followId:hero.id});
      break;
    case 'glass-fault':
      impact(at,{delay:.55,damage:105*power,slow:.3,slowDuration:1.4});
      break;
    case 'spectrum-cross':
      for(const heading of [angle,angle+Math.PI/2]){
        const start={x:at.x-Math.cos(heading)*radius,y:at.y-Math.sin(heading)*radius};
        impact(start,{shape:'line',angle:heading,range:radius*2,r:radius*2,width:24,delay:.8,damage:92*power});
      }
      break;
    default:return false;
  }
  return true;
}

export function frontierBotAim(game, hero, ability, aim, target, retreat) {
  const adjacent=game.units.some(unit=>unit.alive&&unit.team!==hero.team&&unit.kind!=='tower'&&unit.kind!=='core'&&
    inRange(hero,unit,ability.radius||235)&&game.canSee(hero.team,unit));
  const injured=heroesNear(game,hero,hero,ability.range||ability.radius)
    .filter(unit=>unit.hp/unit.maxHp<.8).sort((a,b)=>a.hp/a.maxHp-b.hp/b.maxHp)[0];
  switch(ability.id){
    case 'marsh-remedy':return injured;
    case 'bell-guard':return injured&&adjacent?hero:null;
    case 'mirror-veil':return injured&&adjacent?hero:null;
    case 'great-silence':return adjacent&&!retreat?hero:null;
    case 'slipstream':return adjacent?hero:null;
    case 'clapper-charge':case 'wing-vault':
      return target&&!retreat&&hero.hp/hero.maxHp>.53&&distance(hero,target)>105&&inRange(hero,target,ability.range)?aim:null;
    default:
      return target&&!retreat&&inRange(hero,target,ability.range)&&game.canSee(hero.team,target)?aim:null;
  }
}
