// Expansion battlefields share their geometry with the simulation, navigation,
// main renderer and minimap. Coordinates here are already in world pixels.
import {enrichTerrain} from './terrain.js';

export const EXPANSION_MAPS={
  canyon:{id:'canyon',name:'Stapler Canyon',color:'#dfbc86'},
  conveyor:{id:'conveyor',name:'Conveyor Chaos',color:'#ccd4cc'}
};
const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
const inside=(x,y,b)=>x>=b.x&&x<=b.x+b.w&&y>=b.y&&y<=b.y+b.h;
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const GROUND_TYPES=new Set(['twig','rock','paper','scissors','sniper','mage','divider','rollingrock','shears']);

export function createBattlefield(id,players=2){
  if(!EXPANSION_MAPS[id])return null;
  const map={...EXPANSION_MAPS[id],width:4800,height:3200,players,obstacles:[],lanes:[],expansions:[],conveyors:[],stations:[],bridges:[],canyons:[]};
  map.camps=players===4?[{x:640,y:560},{x:4160,y:560},{x:4160,y:2640},{x:640,y:2640}]:players===3?[{x:640,y:640},{x:4160,y:640},{x:4160,y:2640}]:[{x:640,y:1600},{x:4160,y:1600}];
  if(id==='canyon'){
    map.canyons=[{x:2240,y:0,w:320,h:480},{x:2240,y:700,w:320,h:800},{x:2240,y:1700,w:320,h:800},{x:2240,y:2860,w:320,h:340}].map(o=>({...o,type:'canyon',terrain:true}));
    map.obstacles.push(...map.canyons);
    map.bridges=[{x:2100,y:480,w:600,h:220,type:'rulerbridge',label:'Northern ruler bridge'},{x:2140,y:2500,w:520,h:360,type:'cardboardbridge',label:'Southern winding crossing'}];
    map.gate={x:2400,y:1600,w:320,h:200,hp:1800,label:'Central Stapler Gate'};
    map.gateObstacle={x:2240,y:1500,w:320,h:200,type:'gatebarrier',terrain:true};
    map.obstacles.push(map.gateObstacle,
      {x:2010,y:2380,w:230,h:310,type:'book'},
      {x:2560,y:2730,w:230,h:300,type:'cardboard'},
      {x:1150,y:1000,w:230,h:150,type:'book'},
      {x:3420,y:2050,w:230,h:150,type:'book'});
    map.terrainLayouts=[[1680,830,450,300,'West bridge overlook'],[2670,830,450,300,'East bridge overlook'],[1510,2020,470,300,'West canyon shelf'],[2820,2020,470,300,'East canyon shelf']];
    map.expansions=[{x:1760,y:570},{x:3040,y:570},{x:1880,y:1600},{x:2920,y:1600},{x:1800,y:2810},{x:3020,y:2530}];
    map.lanes=[[[640,1600],[1720,1600],[1980,590],[2400,590],[2820,590],[3080,1600],[4160,1600]],[[640,1600],[1700,2220],[1940,2760],[2400,2690],[2800,2530],[3160,2220],[4160,1600]],[[640,1600],[1900,1600],[2400,1600],[2900,1600],[4160,1600]]];
  }else{
    const belt=(id,x,y,w,h,dx,dy,label)=>({id,x,y,w,h,dx,dy,label,owner:-1,reversedUntil:0});
    map.conveyors=[belt('cross-east',1040,1500,2720,200,1,0,'Central eastbound'),belt('cross-north',2300,800,200,1600,0,-1,'Central northbound'),belt('outer-north',640,430,3520,190,1,0,'Outer north'),belt('outer-east',3970,620,190,1960,0,1,'Outer east'),belt('outer-south',640,2580,3520,190,-1,0,'Outer south'),belt('outer-west',640,620,190,1960,0,-1,'Outer west'),belt('transfer-west',830,1120,1030,160,1,0,'West transfer'),belt('transfer-east',2940,1920,1030,160,-1,0,'East transfer')];
    map.stations=[{id:'north-control',x:2140,y:740,beltId:'outer-north'},{id:'south-control',x:2660,y:2460,beltId:'outer-south'},{id:'central-control',x:2070,y:1390,beltId:'cross-east'},{id:'vertical-control',x:2690,y:1810,beltId:'cross-north'},{id:'west-control',x:1110,y:1390,beltId:'transfer-west'},{id:'east-control',x:3690,y:1810,beltId:'transfer-east'},{id:'left-loop-control',x:950,y:2200,beltId:'outer-west'},{id:'right-loop-control',x:3850,y:1000,beltId:'outer-east'}].map(s=>({...s,owner:-1,captureTeam:-1,captureProgress:0,cooldownUntil:0}));
    map.obstacles=[{x:1430,y:790,w:330,h:220,type:'recycler'},{x:3040,y:2190,w:330,h:220,type:'recycler'},{x:1720,y:1770,w:280,h:280,type:'machine'},{x:2800,y:1130,w:280,h:280,type:'machine'},{x:1110,y:2280,w:340,h:140,type:'book'},{x:3350,y:780,w:340,h:140,type:'book'}];
    map.terrainLayouts=[[1480,290,540,280,'Factory west gantry'],[2780,2630,540,280,'Factory east gantry']];
    map.expansions=[{x:1960,y:1080},{x:2840,y:2120},{x:2130,y:1830},{x:2670,y:1370},{x:1160,y:740},{x:3640,y:2460}];
    map.lanes=[[[640,1600],[1350,1600],[2400,1600],[3450,1600],[4160,1600]],[[640,1600],[740,530],[2400,530],[4070,530],[4160,1600]],[[640,1600],[740,2680],[2400,2680],[4070,2680],[4160,1600]],[[2400,800],[2400,1600],[2400,2400]]];
  }
  return enrichTerrain(map);
}

export function initBattlefield(game){
  if(game.map.gate){
    const g=game.map.gate,e=game._spawn('staplergate',-1,g.x,g.y,{neutral:true});
    g.entityId=e.id;game.map.gateObstacle.entityId=e.id;
  }
  for(const station of game.map.stations||[]){
    const e=game._spawn('station',-1,station.x,station.y,{neutral:true,stationId:station.id});
    station.entityId=e.id;
  }
}

export function reverseConveyor(game,entityId,team=0){
  const station=(game.map.stations||[]).find(s=>s.entityId===entityId||s.id===entityId);
  if(!station||station.owner!==team)return {ok:false,reason:'Capture this conveyor station first.'};
  if(game.time<station.cooldownUntil)return {ok:false,reason:`Station recharging (${Math.ceil(station.cooldownUntil-game.time)}s).`};
  const belt=game.map.conveyors.find(b=>b.id===station.beltId);
  belt.reversedUntil=game.time+18;station.cooldownUntil=game.time+24;
  game._barrierRevision=(game._barrierRevision||0)+1;
  game._effect?.('capture',station.x,station.y,{team,life:1,total:1});
  return {ok:true};
}

export function updateBattlefield(game,dt){
  const gate=game.map.gate;
  if(gate&&!gate.open&&!game.getEntity(gate.entityId)){
    gate.open=true;game.map.obstacles=game.map.obstacles.filter(o=>o!==game.map.gateObstacle);
    game._barrierRevision=(game._barrierRevision||0)+1;game._navigation=null;
    game._effect?.('gateCollapse',gate.x,gate.y,{life:1.3,total:1.3});
  }
  for(const station of game.map.stations||[]){
    // Control consoles are shared map infrastructure, even after their owner
    // loses its headquarters in a three- or four-player battle.
    if(!game.getEntity(station.entityId)||game.teams?.[station.owner]?.eliminated){
      let entity=game.getEntity(station.entityId);
      if(!entity){entity=game._spawn('station',-1,station.x,station.y,{neutral:true,stationId:station.id});station.entityId=entity.id;}
      entity.team=-1;station.owner=-1;station.captureTeam=-1;station.captureProgress=0;station.cooldownUntil=0;
      const belt=game.map.conveyors.find(b=>b.id===station.beltId);belt.owner=-1;belt.reversedUntil=0;game._fogDirty=true;
    }
    let team=-1,contested=false;
    for(const e of game.entities){
      if(e.hp<=0||e.team<0||e.transportedBy||!GROUND_TYPES.has(e.type)||(e.x-station.x)**2+(e.y-station.y)**2>=22500)continue;
      if(team<0)team=e.team;else if(team!==e.team){contested=true;break;}
    }
    if(team<0||contested){station.captureProgress=Math.max(0,station.captureProgress-dt*.35);continue;}
    if(team===station.owner){station.captureProgress=0;station.captureTeam=team;continue;}
    if(station.captureTeam!==team){station.captureTeam=team;station.captureProgress=0;}
    station.captureProgress+=dt/4;
    if(station.captureProgress>=1){
      station.owner=team;station.captureProgress=0;station.cooldownUntil=0;
      const entity=game.getEntity(station.entityId);if(entity){entity.team=team;game._fogDirty=true;}
      const belt=game.map.conveyors.find(b=>b.id===station.beltId);belt.owner=team;
      reverseConveyor(game,station.entityId,team);
      if(team===0)game._event?.('toast',{message:`${belt.label} captured. Belt reversed for 18 seconds.`});
    }
  }
  if(game.time>=45&&game.time>=(game._battlefieldThink||0)&&game.command){
    game._battlefieldThink=game.time+3;
    game._battlefieldPlans??=[];
    for(let team=1;team<(game.players||0);team++){
      if(game.teams[team].eliminated||game.time<(game._battlefieldPlans[team]||0))continue;
      const troops=game.entities.filter(e=>e.hp>0&&e.team===team&&e.type!=='twig'&&GROUND_TYPES.has(e.type)&&!e.transportedBy&&!e.dash&&!e.roll&&!e.aim);
      const gateEntity=gate&&!gate.open?game.getEntity(gate.entityId):null;
      if(gateEntity&&game.isVisible(gateEntity,team)){
        const attackers=troops.filter(e=>distance(e,gateEntity)<850&&game.canTarget(e,gateEntity));
        if(attackers.length){game.command(attackers.slice(0,8).map(e=>e.id),{type:'attack',targetId:gateEntity.id});game._battlefieldPlans[team]=game.time+12;continue;}
      }
      const options=[];
      for(const station of game.map.stations||[]){
        if(station.owner===team||!game.isExplored(team,station.x,station.y))continue;
        const candidates=troops.filter(e=>['idle','attackMove','move'].includes(e.order.type)&&distance(e,station)<1300).sort((a,b)=>distance(a,station)-distance(b,station));
        if(candidates.length)options.push({station,candidates,score:distance(candidates[0],station)});
      }
      options.sort((a,b)=>a.score-b.score);
      if(options.length){const {station,candidates}=options[0];game.command(candidates.slice(0,2).map(e=>e.id),{type:'attackMove',x:station.x,y:station.y});game._battlefieldPlans[team]=game.time+20;}
    }
  }
}

export function conveyorAt(game,x,y){return (game.map.conveyors||[]).find(b=>inside(x,y,b));}
export function conveyorVelocity(game,e,vx,vy,dt=1){
  if(e.type==='cd'||e.type==='airplane'||e.flying)return {vx,vy};
  const belt=conveyorAt(game,e.x,e.y),length=Math.hypot(vx,vy);if(!belt||!length)return {vx,vy};
  const sign=belt.reversedUntil>game.time?-1:1,dx=belt.dx*sign,dy=belt.dy*sign;
  const dot=(vx*dx+vy*dy)/length,multiplier=1+.45*Math.max(0,dot)+.25*Math.min(0,dot),drift=length*.12*(1-Math.abs(dot));
  return {vx:vx*multiplier+dx*drift,vy:vy*multiplier+dy*drift};
}
export function conveyorTravelCost(game,x,y,dx,dy){
  const belt=conveyorAt(game,x,y);if(!belt)return 1;
  const sign=belt.reversedUntil>game.time?-1:1,dot=(dx*belt.dx+dy*belt.dy)*sign/Math.hypot(dx,dy);
  return 1/(1+.45*Math.max(0,dot)+.25*Math.min(0,dot));
}

function pointSegmentDistanceSquared(px,py,ax,ay,bx,by){
  const dx=bx-ax,dy=by-ay,length2=dx*dx+dy*dy,t=length2?clamp(((px-ax)*dx+(py-ay)*dy)/length2,0,1):0;
  const x=px-ax-t*dx,y=py-ay-t*dy;return x*x+y*y;
}
function segmentsCross(ax,ay,bx,by,cx,cy,dx,dy){
  const rX=bx-ax,rY=by-ay,sX=dx-cx,sY=dy-cy,den=rX*sY-rY*sX;
  if(Math.abs(den)<1e-9)return false;
  const cX=cx-ax,cY=cy-ay,t=(cX*sY-cY*sX)/den,u=(cX*rY-cY*rX)/den;return t>=0&&t<=1&&u>=0&&u<=1;
}
const EMPTY_FIELDS=Object.freeze([]),wallCache=new WeakMap();
const isActive=field=>field.remaining>0&&!(field.casting>0);
function wallPoints(field){
  const cached=wallCache.get(field);if(cached&&cached.x===field.x&&cached.y===field.y&&cached.r===field.r)return cached.points;
  const points=[0,1,2].map(i=>{const angle=-Math.PI/2+i*Math.PI*2/3;return {x:field.x+Math.cos(angle)*field.r,y:field.y+Math.sin(angle)*field.r};});
  wallCache.set(field,{x:field.x,y:field.y,r:field.r,points});return points;
}
// Sorting destinations must clear the future walls even while the division is
// still a non-solid casting preview. A constant angular margin is insufficient
// near the center, especially for heavy Tier 3 troops.
export function divisionPlacementValid(field,x,y,r=17){
  const dx=x-field.x,dy=y-field.y,inner=field.r-r-4,clearance=(r+4)**2;
  if(inner<=0||dx*dx+dy*dy>=inner*inner)return false;
  for(const p of wallPoints(field))if(pointSegmentDistanceSquared(x,y,field.x,field.y,p.x,p.y)<=clearance)return false;
  return true;
}
function ringBlocked(field,x,y,r){
  const dx=x-field.x,dy=y-field.y,distance2=dx*dx+dy*dy,inner=field.r-r-3,outer=field.r+r+3;
  return (inner<0||distance2>inner*inner)&&distance2<outer*outer;
}
function ringCrossed(field,ax,ay,bx,by,r){
  const outer=field.r+r+2;
  if(Math.max(ax,bx)<field.x-outer||Math.min(ax,bx)>field.x+outer||Math.max(ay,by)<field.y-outer||Math.min(ay,by)>field.y+outer)return false;
  const dax=ax-field.x,day=ay-field.y,dbx=bx-field.x,dby=by-field.y,da2=dax*dax+day*day,db2=dbx*dbx+dby*dby,inner=field.r-r-2,inner2=inner*inner,outer2=outer*outer;
  if(inner>0&&da2<inner2&&db2<inner2)return false;
  if(da2>outer2&&db2>outer2)return pointSegmentDistanceSquared(field.x,field.y,ax,ay,bx,by)<outer2;
  // A newly cast wall may intersect a troop. It may move away from the wall on
  // its original side, but cannot exploit the overlap to change sides.
  const overlapInner=field.r-r-3,overlapOuter=field.r+r+3;
  if((overlapInner<=0||da2>=overlapInner*overlapInner)&&da2<=overlapOuter*overlapOuter){return da2<field.r*field.r?db2>=da2&&db2>inner2:db2<=da2&&db2<outer2;}
  return true;
}
export function dynamicBarrierBlocked(game,x,y,r=17){
  for(const field of game.forceFields||EMPTY_FIELDS)if(isActive(field)&&(x-field.x)**2+(y-field.y)**2<(field.r+r+2)**2)return true;
  const clearance=(r+3)**2;
  for(const field of game.divisions||EMPTY_FIELDS){
    if(!isActive(field)||Math.abs(x-field.x)>field.r+r+3||Math.abs(y-field.y)>field.r+r+3)continue;
    if(ringBlocked(field,x,y,r))return true;
    for(const p of wallPoints(field))if(pointSegmentDistanceSquared(x,y,field.x,field.y,p.x,p.y)<clearance)return true;
  }
  return false;
}
export function divisionBarrierCrossed(game,ax,ay,bx,by,r=0){
  const clearance=(r+2)**2;
  for(const field of game.divisions||EMPTY_FIELDS){
    const outer=field.r+r+3;
    if(!isActive(field)||Math.max(ax,bx)<field.x-outer||Math.min(ax,bx)>field.x+outer||Math.max(ay,by)<field.y-outer||Math.min(ay,by)>field.y+outer)continue;
    if(ringCrossed(field,ax,ay,bx,by,r))return true;
    for(const p of wallPoints(field)){
      if(segmentsCross(ax,ay,bx,by,field.x,field.y,p.x,p.y))return true;
      if(r>0&&(pointSegmentDistanceSquared(bx,by,field.x,field.y,p.x,p.y)<clearance||pointSegmentDistanceSquared(field.x,field.y,ax,ay,bx,by)<clearance||pointSegmentDistanceSquared(p.x,p.y,ax,ay,bx,by)<clearance))return true;
    }
  }
  return false;
}
export function dynamicBarrierCrossed(game,ax,ay,bx,by,r=17){
  for(const field of game.forceFields||EMPTY_FIELDS){
    if(!isActive(field))continue;
    const outer=(field.r+r+2)**2,from=(ax-field.x)**2+(ay-field.y)**2,to=(bx-field.x)**2+(by-field.y)**2;
    // Unexpected interior arrivals can escape outward; the safety push moves
    // their full footprint clear before normal ground movement resumes.
    if(from<outer&&to>=from)continue;
    if(pointSegmentDistanceSquared(field.x,field.y,ax,ay,bx,by)<outer)return true;
  }
  return divisionBarrierCrossed(game,ax,ay,bx,by,r);
}
export function divisionShotBlocked(game,a,b){return divisionBarrierCrossed(game,a.x,a.y,b.x,b.y,0);}
