import { WORLD, segmentDistance } from './data.mjs';
import { scheduleImpact } from './progression.mjs';

export const PYLON_RADIUS=17;
const PYLON_LIFETIME=24,MAX_PYLONS=2,GLASS_LINK=350,CROSS_LINK=520;
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const along=(origin,angle,length)=>({x:origin.x+Math.cos(angle)*length,y:origin.y+Math.sin(angle)*length});
const direction=(a,b)=>Math.atan2(b.y-a.y,b.x-a.x);
const offsetAngle=(a,b)=>Math.atan2(Math.sin(a-b),Math.cos(a-b));
const structure=unit=>unit.kind==='tower'||unit.kind==='core';

export function prismPylons(game,hero){
  return game.units.filter(unit=>unit.alive&&unit.subtype==='prism-pylon'&&unit.ownerId===hero.id)
    .sort((a,b)=>a.id-b.id);
}

export function prismAimPoint(hero,aim,range){
  const length=distance(hero,aim)||1,reach=Math.min(length,range);
  const point={x:hero.x+(aim.x-hero.x)/length*reach,y:hero.y+(aim.y-hero.y)/length*reach};
  return {x:Math.max(28,Math.min(WORLD.width-28,point.x)),y:Math.max(28,Math.min(WORLD.height-28,point.y))};
}

export function prismPlacement(game,hero,point,range){
  if(point.x<PYLON_RADIUS+15||point.y<PYLON_RADIUS+15||point.x>WORLD.width-PYLON_RADIUS-15||point.y>WORLD.height-PYLON_RADIUS-15)return null;
  const reach=distance(hero,point);
  if(reach<38||reach>range+.01||!game.clearLine(hero,point,PYLON_RADIUS))return null;
  if(prismPylons(game,hero).some(pylon=>distance(pylon,point)<PYLON_RADIUS*2+10))return null;
  return point;
}

export function prismRelayOnLine(game,hero,angle,range,width){
  const end=along(hero,angle,range);
  return prismPylons(game,hero).filter(pylon=>{
    const forward=(pylon.x-hero.x)*Math.cos(angle)+(pylon.y-hero.y)*Math.sin(angle);
    return forward>hero.r&&forward<=range+PYLON_RADIUS&&segmentDistance(pylon.x,pylon.y,hero,end)<width+pylon.r&&game.clearLine(hero,pylon,Math.min(width,8));
  }).sort((a,b)=>distance(hero,a)-distance(hero,b)||a.id-b.id)[0];
}

function linkedPylons(game,hero,point,limit){
  return prismPylons(game,hero).filter(pylon=>distance(pylon,point)<=limit&&game.clearLine(pylon,point,4));
}
export function prismGlassRings(game,hero,at){
  return linkedPylons(game,hero,at,GLASS_LINK);
}
export function prismCrossBeams(game,hero,at,angle,radius){
  const beams=[angle,angle+Math.PI/2].map(heading=>({
    start:along(at,heading,-radius),angle:heading,length:radius*2,width:24
  }));
  for(const pylon of linkedPylons(game,hero,at,CROSS_LINK)){
    const length=distance(pylon,at);
    if(length>18)beams.push({start:pylon,angle:direction(pylon,at),length,width:15,pylonId:pylon.id});
  }
  return beams;
}

function fadePylon(game,pylon){
  game.effect('prism-flare',pylon.x,pylon.y,30,'#c9b1ff',.5,{team:pylon.team});
  pylon.alive=false;
  pylon.hp=0;
  pylon.deathTime=.25;
}
export function tickPrismPylon(game,pylon,dt){
  if(pylon.subtype!=='prism-pylon')return false;
  pylon.lifetime-=dt;
  if(pylon.lifetime<=0)fadePylon(game,pylon);
  return true;
}

function fireShard(game,origin,hero,angle,props){
  return game.fireSkill(origin,angle,{
    type:'prism-shard',color:'#bdf8ff',r:8,speed:610,pierce:0,damageType:'magic',theme:'violet',
    terrainCollision:true,visibleOnly:true,ownerId:hero.id,sourceId:hero.id,sourceKind:'hero',
    abilityId:'facet-bolt',fromX:origin.x,fromY:origin.y,...props
  });
}
export function refractPrismBolt(game,pylon,shot,hero){
  if(hero?.heroId!=='prism')return;
  const range=340,candidates=game.units.filter(unit=>unit.alive&&unit.team!==hero.team&&!structure(unit)
    &&distance(unit,pylon)<=range+unit.r&&Math.abs(offsetAngle(direction(pylon,unit),shot.angle))<Math.PI*.68
    &&game.canSee(hero.team,unit)&&game.clearLine(pylon,unit,8));
  candidates.sort((a,b)=>Number(b.kind==='hero')-Number(a.kind==='hero')||distance(pylon,a)-distance(pylon,b)||a.id-b.id);
  const first=candidates[0]?direction(pylon,candidates[0]):shot.angle-.27;
  const second=candidates[1]?direction(pylon,candidates[1]):first+(offsetAngle(first,shot.angle)<0?.36:-.36);
  for(const [index,heading] of [first,second].entries()){
    fireShard(game,pylon,hero,heading,{range,damage:shot.damage*.85,color:index?'#f0a6ff':'#a1f5ff'});
  }
  game.effect('prism-flare',pylon.x,pylon.y,37,'#c8f8ff',.42,{team:hero.team,followId:pylon.id,angle:shot.angle});
}

export function castPrismSkill(game,hero,ability,{at,angle,power,shieldAmount}){
  const {id,color,range,radius}=ability;
  switch(id){
    case 'facet-bolt':
      for(const offset of [-.19,0,.19]){
        fireShard(game,hero,hero,angle+offset,{range,r:radius,damage:48*power,prismRelay:offset===0,
          color:offset===0?'#e3c8ff':color});
      }
      break;
    case 'mirror-veil': {
      const pylons=prismPylons(game,hero);
      if(pylons.length>=MAX_PYLONS)fadePylon(game,pylons[0]);
      const pylon=game.addUnit('summon',hero.team,at.x,at.y,{
        name:'Mirror Pylon',subtype:'prism-pylon',ownerId:hero.id,stationary:true,noCollision:true,noVision:true,
        r:PYLON_RADIUS,hp:155*(1+(hero.level-1)*.08),speed:0,damage:0,range:0,lifetime:PYLON_LIFETIME
      });
      pylon.visualSpawn={at:game.time,duration:.45,kind:'summon',subtype:pylon.subtype};
      game.shield(hero,shieldAmount(90),3,hero);
      game.applyStatus(hero,'haste',1.25,{factor:.16,sourceId:hero.id,color});
      game.effect('prism-flare',at.x,at.y,47,color,.55,{team:hero.team,followId:pylon.id});
      break;}
    case 'glass-fault':
      scheduleImpact(game,hero,{kind:id,x:at.x,y:at.y,r:radius,color,delay:.55,damage:105*power,slow:.3,slowDuration:1.4});
      for(const pylon of prismGlassRings(game,hero,at)){
        scheduleImpact(game,hero,{kind:'prism-echo',x:pylon.x,y:pylon.y,r:68,color:'#a1f5ff',delay:.7,
          damage:43*power,slow:.24,slowDuration:1.3,pylonId:pylon.id,proc:true});
      }
      break;
    case 'spectrum-cross':
      for(const {start,angle:heading,length,width,pylonId} of prismCrossBeams(game,hero,at,angle,radius)){
        scheduleImpact(game,hero,{kind:pylonId?'prism-cross-ray':id,x:start.x,y:start.y,shape:'line',angle:heading,
          range:length,r:length,width,color:pylonId?'#a1f5ff':color,delay:.8,damage:(pylonId?38:92)*power,pylonId,proc:!!pylonId});
      }
      break;
    default:return false;
  }
  return true;
}

export function prismBotAim(game,hero,ability,aim,target,retreat){
  if(ability.id==='mirror-veil'){
    const foe=target||(retreat?game.findTarget(hero,420,false):null);
    if(!foe||!game.canSee(hero.team,foe)||distance(hero,foe)>530)return null;
    const heading=direction(hero,foe);
    const reach=retreat?65:Math.max(48,Math.min(170,distance(hero,foe)*.55));
    const point=along(hero,heading+(retreat?Math.PI:.34),reach);
    return prismPlacement(game,hero,point,ability.range);
  }
  if(!target||retreat||!game.canSee(hero.team,target))return null;
  if(ability.id==='facet-bolt'){
    const relay=prismPylons(game,hero).find(pylon=>{
      const heading=direction(hero,pylon);
      return distance(pylon,target)<330&&Math.abs(offsetAngle(direction(pylon,target),heading))<Math.PI*.6
        &&prismRelayOnLine(game,hero,heading,ability.range,ability.radius)?.id===pylon.id&&game.clearLine(pylon,target,8);
    });
    if(relay)return relay;
  }
  return distance(hero,target)<=ability.range+target.r?aim:null;
}
