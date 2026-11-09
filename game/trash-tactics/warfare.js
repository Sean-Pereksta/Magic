// Shared family rules and specialist abilities. Distances are world pixels.
import {conveyorVelocity,divisionPlacementValid} from './battlefields.js';
export const WARFARE_TYPES={
  sniper:{label:'Scissors Sniper',family:'scissors',tier:2,cost:150,hp:150,r:18,speed:126,trainTime:11,supply:2,range:420,damage:17,interval:1.7},
  airplane:{label:'Paper Airplane',family:'paper',tier:2,vision:900,cost:130,hp:140,r:22,speed:245,trainTime:9,supply:2,range:265,damage:22,interval:.9,flying:true},
  mage:{label:'Rock Mage',family:'rock',tier:2,cost:170,hp:200,r:20,speed:112,trainTime:12,supply:2,range:210,damage:18,interval:1.6},
  divider:{label:'Paper Divider',family:'paper',tier:3,cost:500,hp:280,r:27,speed:103,trainTime:25,supply:6,range:225,damage:25,interval:1.8},
  rollingrock:{label:'Rolling Rock',family:'rock',tier:3,cost:350,hp:640,r:33,speed:91,trainTime:16,supply:4,range:58,damage:78,interval:2},
  shears:{label:'Shadow Shears',family:'scissors',tier:3,cost:325,hp:300,r:22,speed:185,trainTime:15,supply:3,range:55,damage:56,interval:1.1},
  workshop:{label:'Prototype Workshop',tier:2,cost:275,hp:650,r:51,buildTime:16,supplyCap:0,building:true,requires:['printer']},
  laboratory:{label:'Shredder Laboratory',tier:3,cost:400,hp:850,r:62,buildTime:20,supplyCap:0,building:true,requires:['printer','workshop']},
  staplergate:{label:'Stapler Gate',hp:1800,r:180,building:true,neutral:true,supplyCap:0},
  station:{label:'Conveyor control',hp:500,r:25,building:true,neutral:true,supplyCap:0}
};
const FAMILY={rock:'rock',paper:'paper',scissors:'scissors',...Object.fromEntries(Object.entries(WARFARE_TYPES).filter(([,t])=>t.family).map(([id,t])=>[id,t.family]))};
export const familyOf=type=>FAMILY[typeof type==='string'?type:type?.type]||null;
export const isCombat=type=>!!familyOf(type);
export const canTransport=type=>isCombat(type)&&!WARFARE_TYPES[typeof type==='string'?type:type?.type]?.flying&&(WARFARE_TYPES[typeof type==='string'?type:type?.type]?.tier||1)<3;
export const familyCanDamage=(a,b)=>!familyOf(a)||!familyOf(b)||familyOf(a)===familyOf(b)||({rock:'scissors',paper:'rock',scissors:'paper'}[familyOf(a)]===familyOf(b));
export const PRODUCTION={desk:['twig'],printer:['rock','paper','scissors'],cdreader:['cd'],workshop:['sniper','airplane','mage'],laboratory:['divider','rollingrock','shears']};
export const FORCE_FIELD_RADIUS=80;
export const ABILITIES={snipe:{type:'sniper',cooldown:18,range:700},intercept:{type:'airplane'},patrol:{type:'airplane'},crash:{type:'airplane'},forcefield:{type:'mage',range:420},division:{type:'divider',cooldown:38,range:600},roll:{type:'rollingrock',cooldown:17,range:750},shadowpath:{type:'shears',cooldown:18,range:900}};
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
const dist=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const fail=reason=>({ok:false,reason});
const success=extra=>({ok:true,...extra});
const point=p=>p&&Number.isFinite(p.x)&&Number.isFinite(p.y);
const staticShot=(g,a,b)=>g._lineFree(a.x,a.y,b.x,b.y,0,b.id,false,true,true);
const reset=e=>{e.path=null;e.detour=null;e.combatTarget=null;e.formationGoal=null;e.aim=null;delete e.pendingAbility;};
function queueCast(e,name,target,position){
  reset(e);e.pendingAbility={name,target:{...target},range:ABILITIES[name].range};
  e.order={type:'cast',ability:name,x:position.x,y:position.y,...(target.targetId?{targetId:target.targetId}:{})};
  return success({casterId:e.id,queued:true,ability:name});
}
function stopQueuedCast(g,e,reason){
  delete e.pendingAbility;if(e.order.type==='cast')e.order={type:'idle'};e.path=null;e.detour=null;
  if(reason&&e.team===0)g._event?.('toast',{message:reason,team:e.team});
}
// Plan every displacement before spending a charge. Only clear, continuous
// ground segments qualify, so a dome cannot throw a troop through scenery,
// an existing barrier, or a wall into an inaccessible terrain pocket.
function fieldPushPlan(g,field){
  const ground=g.entities.filter(e=>e.hp>0&&!e.transportedBy&&!e.leap&&(isCombat(e.type)||e.type==='twig')&&!WARFARE_TYPES[e.type]?.flying);
  const affected=ground.filter(e=>dist(e,field)<field.r+e.r+4).sort((a,b)=>b.r-a.r||a.id-b.id),affectedIds=new Set(affected.map(e=>e.id));
  if(!affected.length)return [];
  const reserved=ground.filter(e=>!affectedIds.has(e.id)).map(e=>({x:e.x,y:e.y,r:e.r})),plan=[];
  for(const troop of affected){
    const angle=dist(troop,field)>.01?Math.atan2(troop.y-field.y,troop.x-field.x):(troop.id*2.399963229728653),choices=[];
    for(const extra of [0,24,48,80,120,160,200])for(let i=0;i<96;i++){
      const turn=i===0?0:Math.ceil(i/2)*(i%2?1:-1)*Math.PI/48,radius=field.r+troop.r+5+extra;
      const p={x:field.x+Math.cos(angle+turn)*radius,y:field.y+Math.sin(angle+turn)*radius};
      choices.push({...p,score:(p.x-troop.x)**2+(p.y-troop.y)**2});
    }
    choices.sort((a,b)=>a.score-b.score);
    const destination=choices.find(p=>!g._blocked(p.x,p.y,troop.r)&&reserved.every(other=>dist(p,other)>=troop.r+other.r+2)&&g._lineFree(troop.x,troop.y,p.x,p.y,troop.r));
    if(!destination)return null;
    reserved.push({...destination,r:troop.r});plan.push({troop,x:destination.x,y:destination.y});
  }
  return plan;
}
function applyFieldPushes(g,field,pushes){
  for(const {troop,x,y} of pushes){
    const fromX=troop.x,fromY=troop.y;troop.x=x;troop.y=y;troop.path=null;troop.detour=null;troop.moveSample=null;troop.knockback=.2;
    g._effect('forcefieldPush',x,y,{team:field.team,fromX,fromY,radius:troop.r,life:.4,total:.4});
  }
  if(pushes.length)g._fogDirty=true;
}
function segmentDistance(p,a,b){const dx=b.x-a.x,dy=b.y-a.y,t=clamp(((p.x-a.x)*dx+(p.y-a.y)*dy)/(dx*dx+dy*dy||1),0,1);return Math.hypot(p.x-a.x-dx*t,p.y-a.y-dy*t);}
function clearMovement(g,e,p,ignoreDivision=false){
  const divisions=g.divisions;
  if(ignoreDivision)g.divisions=divisions.filter(d=>d.id!==e.divisionId);
  try{return g._lineFree(e.x,e.y,p.x,p.y,e.r);}finally{g.divisions=divisions;}
}
function moveAlong(g,e,p,step,air=false,ignoreDivision=false){
  const length=dist(e,p);if(length<.01)return true;
  const amount=Math.min(length,step);let vx=(p.x-e.x)/length*amount,vy=(p.y-e.y)/length*amount;
  if(e.roll)({vx,vy}=conveyorVelocity(g,e,vx,vy));
  if(e.roll){const remaining=Math.min(length,e.roll.remainingDistance),magnitude=Math.hypot(vx,vy);if(magnitude>remaining){vx*=remaining/magnitude;vy*=remaining/magnitude;}}
  const nx=e.x+vx,ny=e.y+vy;
  if(!air&&!clearMovement(g,e,{x:nx,y:ny},ignoreDivision))return 'blocked';
  if(air){const divisions=g.divisions;if(ignoreDivision)g.divisions=divisions.filter(d=>d.id!==e.divisionId);let blocked;try{blocked=g._divisionAirBlocked?.(e,{x:nx,y:ny});}finally{g.divisions=divisions;}if(blocked)return 'blocked';}
  e.facing=Math.atan2(p.y-e.y,p.x-e.x);e.desiredVX=(p.x-e.x)/length*step;e.desiredVY=(p.y-e.y)/length*step;e.x=nx;e.y=ny;
  return length<=amount+.01;
}
export function initializeWarfare(g){g.forceFields=[];g.divisions=[];g._warfareId=1;g._barrierRevision=0;}
export function useAbility(g,ids,name,target={}){
  const def=ABILITIES[name];if(!def)return fail('Unknown ability.');
  let units=[...new Set(ids)].map(id=>g.getEntity(id)).filter(e=>e?.type===def.type&&!e.transportedBy&&!e.leap&&!e.shadow&&!e.roll&&!e.divisionPull&&!e.crash?.diving);
  if(!units.length)return fail('Select troops with this ability.');
  const team=units[0].team;units=units.filter(e=>e.team===team);
  if(name==='intercept'){
    const enemy=g.getEntity(target.targetId);if(!enemy||!g.canTarget(units[0],enemy))return fail('Choose a visible enemy aircraft this family can attack.');
    for(const e of units){reset(e);e.crash=null;e.order={type:'attack',targetId:enemy.id};}return success({count:units.length});
  }
  if(name==='patrol'){
    const points=(target.points||[]).filter(point).map(p=>({x:clamp(p.x,25,g.width-25),y:clamp(p.y,25,g.height-25)}));
    if(points.length<2)return fail('Choose at least two patrol points.');
    for(const e of units){reset(e);e.crash=null;e.order={type:'patrol',points:points.map(p=>({...p})),index:0};}return success({count:units.length});
  }
  units=units.filter(e=>(e.abilityCooldown||0)<=0&&!e.aim);
  if(!units.length)return fail('Wait for an ability to recharge.');
  if(name==='snipe'){
    const victim=g.getEntity(target.targetId);
    if(!victim||familyOf(victim)!=='paper'||victim.paperInvulnerable>0)return fail('Choose a vulnerable enemy Paper troop.');
    const eligible=units.filter(e=>g.canTarget(e,victim)).sort((a,b)=>dist(a,victim)-dist(b,victim)||a.id-b.id);
    const caster=eligible.find(e=>dist(e,victim)<=def.range&&staticShot(g,e,victim))||eligible.find(e=>dist(e,victim)>def.range);
    if(!caster)return fail('Choose a visible Paper target with a clear shot.');
    if(dist(caster,victim)>def.range)return queueCast(caster,name,{targetId:victim.id},victim);
    reset(caster);caster.order={type:'idle'};caster.aim={targetId:victim.id,remaining:1.5,total:1.5};caster.abilityCooldown=18;return success({casterId:caster.id});
  }
  if(name==='shadowpath')return shadowPath(g,units,target);
  if(!point(target))return fail('Choose a battlefield location.');
  const center={x:clamp(target.x,25,g.width-25),y:clamp(target.y,25,g.height-25)};
  if(name==='forcefield'||name==='division'){
    const caster=units.filter(e=>name!=='forcefield'||(e.mageCharges??2)>0).sort((a,b)=>dist(a,center)-dist(b,center)||a.id-b.id)[0];
    if(!caster)return fail('Wait for a mage charge to recharge.');
    if(dist(caster,center)>def.range||!g.canSee(team,center.x,center.y))return queueCast(caster,name,center,center);
    if(name==='forcefield'){
      const field={id:g._warfareId,x:center.x,y:center.y,r:FORCE_FIELD_RADIUS,team,remaining:10,total:10},pushes=fieldPushPlan(g,field);
      if(!pushes)return fail('The dome needs clear ground to push every troop safely outside.');
      reset(caster);caster.order={type:'idle'};
      caster.mageCasting=Math.max(caster.mageCasting||0,.65);caster.mageCharges=(caster.mageCharges??2)-1;if(!(caster.mageRecharge>0))caster.mageRecharge=5;
      applyFieldPushes(g,field,pushes);
      g._warfareId++;g.forceFields.push(field);barriersChanged(g);g._effect('forcefield',center.x,center.y,{team,radius:FORCE_FIELD_RADIUS});g._fogDirty=true;
    }else{
      reset(caster);caster.order={type:'idle'};
      caster.abilityCooldown=38;caster.divisionCast=1.5;
      g.divisions.push({id:g._warfareId++,x:center.x,y:center.y,r:350,team,remaining:9,total:9,casting:1.5,casterId:caster.id});g._effect('division',center.x,center.y,{team,radius:350,life:1.5,total:1.5});
    }
    return success({casterId:caster.id});
  }
  if(name==='crash'){
    const radius=units.length===1?0:Math.min(285,35*Math.sqrt(units.length));
    units.forEach((e,i)=>{reset(e);const angle=i*2.399963229728653,rad=units.length===1?0:radius*Math.sqrt((i+.5)/units.length);e.crash={x:clamp(center.x+Math.cos(angle)*rad,e.r,g.width-e.r),y:clamp(center.y+Math.sin(angle)*rad,e.r,g.height-e.r),diving:false,remaining:.65};e.order={type:'crash'};});
    return success({count:units.length});
  }
  if(name==='roll'){
    units.forEach((e,i)=>{reset(e);const a=Math.atan2(center.y-e.y,center.x-e.x),lane=(i-(units.length-1)/2)*76,dx=center.x-Math.sin(a)*lane-e.x,dy=center.y+Math.cos(a)*lane-e.y,len=Math.hypot(dx,dy),distance=Math.min(750,len);e.roll={x:e.x+dx/(len||1)*distance,y:e.y+dy/(len||1)*distance,elapsed:0,remainingDistance:distance,hits:new Map()};e.abilityCooldown=17;e.order={type:'idle'};});return success({count:units.length});
  }
  return fail('This ability is unavailable.');
}
function shadowPath(g,units,target){
  const route=(target.points||[]).filter(point);if(!route.length)return fail('Draw a route or choose waypoints.');
  units.forEach((e,i)=>{
    const points=[],lane=(i-(units.length-1)/2)*48;let previous={x:e.x,y:e.y},budget=900;
    for(let j=0;j<route.length&&budget>0;j++){
      const next=route[j],before=route[j-1]||e,angle=Math.atan2(next.y-before.y,next.x-before.x),desired={x:clamp(next.x-Math.sin(angle)*lane,e.r,g.width-e.r),y:clamp(next.y+Math.cos(angle)*lane,e.r,g.height-e.r)},length=dist(previous,desired);
      if(length<2)continue;const scale=Math.min(1,budget/length),p={x:previous.x+(desired.x-previous.x)*scale,y:previous.y+(desired.y-previous.y)*scale};points.push(p);budget-=length*scale;previous=p;
    }
    if(points.length){reset(e);e.shadow={points,index:0,hits:new Set()};e.abilityCooldown=18;e.order={type:'idle'};}
  });return success({count:units.length});
}
function barriersChanged(g){g._barrierRevision++;g._navigation=null;for(const e of g.entities){e.path=null;e.pathRefresh=0;e.detour=null;}}
export function tickWarfare(g,dt){
  let changed=false;
  for(const field of g.forceFields){field.remaining-=dt;if(field.remaining<=0){changed=true;g._effect('forcefieldEnd',field.x,field.y,{team:field.team,radius:field.r});}}
  g.forceFields=g.forceFields.filter(f=>f.remaining>0);
  // Landing rocks and other late arrivals must not create a trapped interior.
  // Normal production and transport placement already exclude the solid disk.
  for(const field of [...g.forceFields]){
    field.pushCheck=(field.pushCheck||0)-dt;if(field.pushCheck>0)continue;field.pushCheck=.1;
    const fields=g.forceFields;g.forceFields=fields.filter(other=>other!==field);let pushes;
    try{pushes=fieldPushPlan(g,field);}finally{g.forceFields=fields;}
    if(pushes)applyFieldPushes(g,field,pushes);
    else{field.remaining=0;changed=true;g._effect('forcefieldEnd',field.x,field.y,{team:field.team,radius:field.r});}
  }
  g.forceFields=g.forceFields.filter(f=>f.remaining>0);
  for(const division of g.divisions){
    if(division.casting>0){
      const caster=g.getEntity(division.casterId);if(!caster){division.remaining=0;continue;}
      division.casting-=dt;if(division.casting<=0){sortDivision(g,division);division.casting=0;changed=true;}
    }else division.remaining-=dt;
    if(division.remaining<=0){changed=true;g._effect('divisionEnd',division.x,division.y,{team:division.team,radius:division.r,life:1,total:1});for(const e of g.entities)if(e.divisionId===division.id){if(e.divisionSavedOrder)e.order=e.divisionSavedOrder;delete e.divisionSavedOrder;delete e.divisionId;delete e.divisionSector;delete e.divisionPull;}}
  }
  g.divisions=g.divisions.filter(d=>d.remaining>0);if(changed)barriersChanged(g);
}
function sortDivision(g,d){
  const victims=g.entities.filter(e=>e.hp>0&&isCombat(e.type)&&!e.transportedBy&&dist(e,d)<=d.r),reserved=[],candidates=new Map();
  // Resolve large footprints first on a hex lattice. Continuous pull routes
  // respect real scenery, and the walls become solid after routes are planned.
  d.casting=.000001;
  for(const e of victims.sort((a,b)=>b.r-a.r||a.id-b.id)){
    const sector=['rock','paper','scissors'].indexOf(familyOf(e)),key=sector+':'+e.r;let choices=candidates.get(key);
    if(!choices){
      choices=[];const spacing=e.r,rows=Math.ceil(d.r*2/(spacing*.8660254));
      for(let row=0;row<=rows;row++)for(let col=0;col<Math.ceil(d.r*2/spacing);col++){
        const x=-d.r+e.r+7+col*spacing+(row%2)*spacing/2,y=-d.r+e.r+7+row*spacing*.8660254,radius=Math.hypot(x,y),angle=(Math.atan2(y,x)+Math.PI/2+Math.PI*2)%(Math.PI*2),local=angle-sector*Math.PI*2/3;
        if(radius>d.r-e.r-4||local<=0||local>=Math.PI*2/3||Math.min(Math.sin(local),Math.sin(Math.PI*2/3-local))*radius<e.r+4)continue;
        choices.push({x:d.x+x,y:d.y+y});
      }
      choices.sort((a,b)=>dist(b,d)-dist(a,d));candidates.set(key,choices);
    }
    let destination=null,path=null;
    for(const p of choices){
      if(!divisionPlacementValid(d,p.x,p.y,e.r)||!WARFARE_TYPES[e.type]?.flying&&g._blocked(p.x,p.y,e.r))continue;
      if(reserved.some(v=>dist(v,p)<v.r+e.r+2))continue;
      const route=WARFARE_TYPES[e.type]?.flying?[p]:g._lineFree(e.x,e.y,p.x,p.y,e.r)?[p]:g._path(e,p.x,p.y);
      if(!route.length||dist(route.at(-1),p)>10||route.some(p=>dist(p,d)>d.r-e.r))continue;
      destination=p;path=route;break;
    }
    if(!destination)continue;
    reset(e);e.divisionSavedOrder??={...e.order};e.order={type:'idle'};e.divisionId=d.id;e.divisionSector=sector;e.divisionPull={points:path,index:0};reserved.push({...destination,r:e.r});
  }
}
function hurtAlong(g,e,a,b,damage,hits,repeat=false){
  for(const t of g.entities){
    if(t.hp<=0||t.id===e.id||t.transportedBy||!isCombat(t.type)&&t.type!=='twig'||WARFARE_TYPES[t.type]?.flying||t.type==='cd'||segmentDistance(t,a,b)>e.r+t.r+9)continue;
    const friendly=t.team===e.team;if(!friendly&&!familyCanDamage(e,t))continue;
    if(!friendly){const previous=repeat?hits.get(t.id):hits.has(t.id);if(repeat?previous!==undefined&&g.time-previous<.65:previous)continue;g._damage(t,repeat&&previous!==undefined?40:damage,e);if(repeat)hits.set(t.id,g.time);else hits.add(t.id);g._effect(e.shadow?'shadowSlash':'rollhit',t.x,t.y,{team:e.team,radius:t.r+15});}
    if(repeat){const side=((t.x-a.x)*(b.y-a.y)-(t.y-a.y)*(b.x-a.x))>0?-1:1,angle=Math.atan2(b.y-a.y,b.x-a.x)+Math.PI/2*side,p={x:t.x+Math.cos(angle)*32,y:t.y+Math.sin(angle)*32};if(g._lineFree(t.x,t.y,p.x,p.y,t.r)){t.x=p.x;t.y=p.y;t.knockback=.2;}}
  }
}
export function tickSpecialist(g,e,dt){
  if(e.type==='mage'){
    e.mageCasting=Math.max(0,(e.mageCasting||0)-dt);
    e.mageCharges??=2;if(e.mageRecharge>0){e.mageRecharge=Math.max(0,e.mageRecharge-dt);if(e.mageRecharge===0)e.mageCharges=2;}
  }
  if(e.divisionPull){
    const pull=e.divisionPull,p=pull.points[pull.index],result=moveAlong(g,e,p,Math.max(450,dist(e,p)*2)*dt,!!WARFARE_TYPES[e.type]?.flying,true);
    if(result==='blocked'){delete e.divisionPull;return true;}if(result===true&&++pull.index>=pull.points.length)delete e.divisionPull;return true;
  }
  if(e.pendingAbility){
    const pending=e.pendingAbility,def=ABILITIES[pending.name];
    if(e.order.type!=='cast'||e.order.ability!==pending.name){delete e.pendingAbility;return false;}
    const victim=pending.target.targetId?g.getEntity(pending.target.targetId):null,position=victim||pending.target;
    const valid=pending.name==='snipe'?victim&&familyOf(victim)==='paper'&&victim.paperInvulnerable<=0&&g.canTarget(e,victim):point(position);
    if(!valid){stopQueuedCast(g,e,'Queued cast canceled: the target is no longer visible or valid.');return true;}
    if((e.abilityCooldown||0)>0||pending.name==='forcefield'&&(e.mageCharges??2)<=0){stopQueuedCast(g,e,'Queued cast canceled: the ability is no longer ready.');return true;}
    e.order.x=position.x;e.order.y=position.y;
    const visible=pending.name==='snipe'||g.canSee(e.team,position.x,position.y);
    if(dist(e,position)>def.range||!visible){
      if(!pending.lastPosition||dist(e,pending.lastPosition)>4){pending.lastPosition={x:e.x,y:e.y};pending.lastMoveAt=g.time;}
      if(g.time-pending.lastMoveAt>8){stopQueuedCast(g,e,'Queued cast canceled: no clear route to the target.');return true;}
      g._move(e,position.x,position.y,dt,visible?def.range-8:8,victim?.id);return true;
    }
    delete e.pendingAbility;e.order={type:'idle'};e.path=null;e.detour=null;
    const result=useAbility(g,[e.id],pending.name,pending.target);
    if(!result.ok&&e.team===0)g._event?.('toast',{message:result.reason,team:e.team});
    return true;
  }
  if(e.aim){
    const aim=e.aim,target=g.getEntity(aim.targetId);
    if(!target||target.paperInvulnerable>0||!g.canTarget(e,target)||dist(e,target)>700||!staticShot(g,e,target)){e.aim=null;g._effect('snipeInterrupted',e.x,e.y,{team:e.team});return true;}
    e.facing=Math.atan2(target.y-e.y,target.x-e.x);aim.remaining-=dt;
    if(aim.remaining<=0){g._damage(target,target.hp,e);g._effect('snipe',target.x,target.y,{team:e.team,fromX:e.x,fromY:e.y,life:.35,total:.35});e.aim=null;}return true;
  }
  if(e.divisionCast>0){e.divisionCast-=dt;return true;}
  if(e.crash){
    const c=e.crash;if(!c.diving&&dist(e,c)<=155){c.diving=true;c.fromX=e.x;c.fromY=e.y;c.total=.65;}
    if(c.diving){
      c.remaining-=dt;const t=1-Math.max(0,c.remaining)/.65;e.x=c.fromX+(c.x-c.fromX)*t;e.y=c.fromY+(c.y-c.fromY)*t;
      if(c.remaining<=0){for(const enemy of g.entities){const distance=dist(c,enemy);if(enemy.hp>0&&enemy.team!==e.team&&(enemy.team>=0||enemy.type==='staplergate')&&enemy.type!=='station'&&!enemy.transportedBy&&distance<75+enemy.r&&familyCanDamage(e,enemy))g._damage(enemy,145*Math.max(.15,1-distance/(75+enemy.r)),e);}g._effect('crash',c.x,c.y,{team:e.team,radius:75,life:1.4,total:1.4});e.hp=0;e.crash=null;}
    }else moveAlong(g,e,c,WARFARE_TYPES.airplane.speed*dt,true);
    return true;
  }
  if(e.roll){
    const r=e.roll;r.elapsed+=dt;const step=Math.min(r.remainingDistance,300*Math.min(1,r.elapsed)*dt),before={x:e.x,y:e.y};
    const paper=g.entities.find(t=>t.hp>0&&t.team>=0&&t.team!==e.team&&familyOf(t)==='paper'&&!t.transportedBy&&!WARFARE_TYPES[t.type]?.flying&&dist(e,t)<e.r+t.r+9);
    const result=paper?'blocked':moveAlong(g,e,r,step);
    if(result==='blocked'){e.roll=null;g._effect('rollhit',e.x,e.y,{team:e.team,radius:40});return true;}
    r.remainingDistance-=dist(before,e);hurtAlong(g,e,before,e,130,r.hits,true);
    if(result===true||r.remainingDistance<=.1){e.roll=null;e.order={type:'idle'};}return true;
  }
  if(e.shadow){
    const s=e.shadow;let budget=650*dt;
    while(budget>0&&s.index<s.points.length){const p=s.points[s.index],before={x:e.x,y:e.y},result=moveAlong(g,e,p,budget);const moved=dist(before,e);budget-=moved;hurtAlong(g,e,before,e,115,s.hits);if(result==='blocked'){s.index=s.points.length;break;}if(result===true)s.index++;else break;}
    if(s.index>=s.points.length){e.shadow=null;g._effect('shadowFinish',e.x,e.y,{team:e.team,radius:34});}return true;
  }
  return false;
}
export function moveAircraft(g,e,x,y,dt,stop=8){
  const distance=Math.hypot(x-e.x,y-e.y);if(distance<=stop)return true;
  return moveAlong(g,e,{x:clamp(x,e.r,g.width-e.r),y:clamp(y,e.r,g.height-e.r)},Math.min(WARFARE_TYPES[e.type].speed*dt,distance-stop+1),true)===true;
}
export function specialistCombat(g,e,dt){
  const spec=WARFARE_TYPES[e.type];if(!spec?.family)return false;
  const order=e.order,explicit=order.type==='attack';let target=explicit?g.getEntity(order.targetId):order.type!=='move'&&e.combatTarget?g.getEntity(e.combatTarget):null;
  if(target&&(!g.canTarget(e,target)||!explicit&&dist(e,target)>Math.max(430,spec.range+160))){target=null;if(explicit)e.order={type:'idle'};}
  if(!target&&order.type!=='move')target=g._nearest(e,t=>g.canTarget(e,t)&&dist(e,t)<Math.max(350,spec.range+60));
  e.combatTarget=target?.id;
  if(target){
    const distance=dist(e,target),range=spec.range+target.r;e.facing=Math.atan2(target.y-e.y,target.x-e.x);
    if(e.type==='sniper'&&distance<185){const angle=Math.atan2(e.y-target.y,e.x-target.x),p={x:e.x+Math.cos(angle)*90,y:e.y+Math.sin(angle)*90};if(g._lineFree(e.x,e.y,p.x,p.y,e.r))g._move(e,p.x,p.y,dt,5);}
    else if(distance>range)g._move(e,target.x,target.y,dt,range-4,target.id);
    else if(e.cooldown<=0&&staticShot(g,e,target)){
      if(e.type==='airplane'){g.projectiles.push({type:'dart',x:e.x,y:e.y,fromX:e.x,fromY:e.y,team:e.team,sourceId:e.id,source:{id:e.id,type:e.type,team:e.team,x:e.x,y:e.y,hp:1},targetId:target.id,damage:spec.damage,life:2,total:2,angle:e.facing});e.attackAnim=.25;}
      else {g._damage(target,spec.damage,e);g._effect(e.type==='sniper'?'snipeShot':'magic',target.x,target.y,{team:e.team,fromX:e.x,fromY:e.y});}e.cooldown=spec.interval;
    }
    return true;
  }
  if(order.type==='patrol'){
    const p=order.points[order.index];if(moveAircraft(g,e,p.x,p.y,dt,16))order.index=(order.index+1)%order.points.length;
  }else if(order.type==='move'||order.type==='attackMove'){
    if(g._move(e,order.x,order.y,dt,order.arrivalRadius||10))e.order={type:'idle'};
  }
  return true;
}
export function updateDart(g,p,dt){
  const target=g.getEntity(p.targetId);p.life-=dt;
  if(p.life<=0||!target||!g.canTarget(p.source,target))return false;
  const length=dist(p,target),step=600*dt;p.angle=Math.atan2(target.y-p.y,target.x-p.x);
  if(length<=step+target.r){g._damage(target,p.damage,p.source);g._effect('dartImpact',target.x,target.y,{team:p.team});return false;}
  p.x+=(target.x-p.x)/length*step;p.y+=(target.y-p.y)/length*step;return true;
}
export function specialistBot(g,team){
  const cfg=g.botConfig||{micro:1,workshopAt:65,laboratoryAt:150},own=g.entities.filter(e=>e.hp>0&&e.team===team),workers=own.filter(e=>e.type==='twig'&&e.order.type!=='build'&&!e.aiTask),start=g.starts[team],bank=g.teams[team];
  const buildNear=type=>{if(!workers.length)return false;for(let ring=1;ring<6;ring++)for(let i=0;i<16;i++){const a=i/16*Math.PI*2,x=start.x+Math.cos(a)*ring*95,y=start.y+Math.sin(a)*ring*95;if(g.canBuild(type,x,y)&&g.canSee(team,x,y)&&g.build(workers.map(e=>e.id),type,x,y).ok)return true;}return false;};
  const workshop=own.find(e=>e.type==='workshop'),laboratory=own.find(e=>e.type==='laboratory');
  const transportReserve=g._bots[team].transportReserve||0;
  if(g.time>cfg.workshopAt&&!workshop&&bank.trash>=275+transportReserve)buildNear('workshop');
  if(g.time>cfg.laboratoryAt&&workshop&&!workshop.constructing&&!laboratory&&bank.trash>=400+transportReserve)buildNear('laboratory');
  // Independent rotations and alternating economic priority keep both tiers
  // in the army. Lower-tier queues reserve the chosen purchase and six supply.
  const labs=own.filter(e=>e.type==='laboratory'&&!e.constructing),workshops=own.filter(e=>e.type==='workshop'&&!e.constructing),advanced=!!workshop,state=g._bots[team];
  state.nextTierProduction??=3;
  const visibleAircraft=g.entities.filter(e=>e.hp>0&&e.team>=0&&e.team!==team&&WARFARE_TYPES[e.type]?.flying&&g.isVisible(e,team));
  const nextUnit=b=>cfg.micro>=2&&b.type==='workshop'&&visibleAircraft.length>=3&&own.filter(e=>e.type==='sniper').length<Math.ceil(visibleAircraft.length/2)?'sniper':PRODUCTION[b.type][(b.aiProductionIndex||0)%3];
  for(const building of labs){
    if(state.nextTierProduction===2&&workshops.length||building.queue.length)continue;
    const type=nextUnit(building);
    if(bank.trash>=WARFARE_TYPES[type].cost+transportReserve&&g.train(building.id,type).ok){building.aiProductionIndex=(building.aiProductionIndex||0)+1;state.nextTierProduction=2;}
  }
  let wanted=state.nextTierProduction===2&&workshops.length?workshops:labs;
  const plan={reserveTrash:labs.length&&wanted.length?Math.min(...wanted.map(b=>WARFARE_TYPES[nextUnit(b)].cost)):laboratory?500:0,reserveSupply:advanced?6:0};
  for(const building of workshops){
    if(building.queue.length||g.time>cfg.laboratoryAt&&!laboratory)continue;
    if(labs.length&&state.nextTierProduction!==2)continue;
    const type=nextUnit(building),spec=WARFARE_TYPES[type],supply=g.getSupply(team);
    if(bank.trash-spec.cost<transportReserve||!labs.length&&bank.trash-spec.cost<plan.reserveTrash+transportReserve||supply.used+spec.supply+plan.reserveSupply>supply.cap)continue;
    if(g.train(building.id,type).ok){building.aiProductionIndex=(building.aiProductionIndex||0)+1;if(labs.length)state.nextTierProduction=3;}
  }
  wanted=state.nextTierProduction===2&&workshops.length?workshops:labs;
  if(labs.length&&wanted.length)plan.reserveTrash=Math.min(...wanted.map(b=>WARFARE_TYPES[nextUnit(b)].cost));
  if(!cfg.micro)return plan;
  const enemies=g.entities.filter(t=>t.hp>0&&t.team>=0&&t.team!==team&&isCombat(t.type)&&g.isVisible(t,team));
  const crashes=[],rolls=[],claimedSnipe=new Set(own.filter(e=>e.aim||e.pendingAbility?.name==='snipe').map(e=>e.aim?.targetId||e.pendingAbility.target.targetId));
  for(const e of own.filter(e=>isCombat(e.type)&&!e.transportedBy&&!e.pendingAbility&&!e.aim&&!e.roll&&!e.shadow&&!e.divisionCast&&!e.divisionPull&&!['retreat','scout','evacuate'].includes(e.aiTask))){
    const nearby=enemies.filter(t=>dist(e,t)<650),papers=nearby.filter(t=>familyOf(t)==='paper').sort((a,b)=>(WARFARE_TYPES[b.type]?.tier||1)-(WARFARE_TYPES[a.type]?.tier||1)||dist(e,a)-dist(e,b));
    if(e.type==='sniper'&&papers.length){const target=papers.find(t=>!claimedSnipe.has(t.id));if(target&&g.useAbility([e.id],'snipe',{targetId:target.id}).ok)claimedSnipe.add(target.id);}
    if(e.type==='mage'&&nearby.filter(t=>dist(e,t)<400).length>=(cfg.micro>=2?3:5)){
      const cluster=nearby.filter(t=>dist(e,t)<400),enemy={x:cluster.reduce((n,t)=>n+t.x,0)/cluster.length,y:cluster.reduce((n,t)=>n+t.y,0)/cluster.length},angle=Math.atan2(enemy.y-e.y,enemy.x-e.x),p={x:enemy.x+Math.cos(angle)*100,y:enemy.y+Math.sin(angle)*100};
      // Cut off enemy reinforcements behind their front line, leaving our
      // own formation and its immediate approach clear of the solid dome.
      const blocksFriends=own.some(a=>a.id!==e.id&&!a.transportedBy&&!WARFARE_TYPES[a.type]?.flying&&(isCombat(a.type)||a.type==='twig')&&dist(a,p)<FORCE_FIELD_RADIUS+a.r+35);
      if(!blocksFriends&&dist(e,p)<=420&&!g.forceFields.some(f=>dist(f,p)<FORCE_FIELD_RADIUS*2+25))g.useAbility([e.id],'forcefield',p);
    }
    if(e.type==='divider'&&nearby.length>=(cfg.micro>=3?5:cfg.micro>=2?7:10)&&new Set(nearby.map(t=>familyOf(t))).size>=2){const cluster=nearby.slice(0,15),x=cluster.reduce((n,t)=>n+t.x,0)/cluster.length,y=cluster.reduce((n,t)=>n+t.y,0)/cluster.length;g.useAbility([e.id],'division',{x,y});}
    if(e.type==='rollingrock'&&nearby.filter(t=>familyOf(t)==='scissors').length>=2&&!nearby.some(t=>familyOf(t)==='paper'&&dist(e,t)<150))rolls.push({unit:e,target:nearby.find(t=>familyOf(t)==='scissors')});
    if(e.type==='shears'&&papers.length){const t=papers[0],angle=Math.atan2(t.y-e.y,t.x-e.x);g.useAbility([e.id],'shadowpath',{points:[{x:e.x-Math.sin(angle)*110,y:e.y+Math.cos(angle)*110},{x:t.x+Math.cos(angle)*80,y:t.y+Math.sin(angle)*80}]});}
    if(e.type==='airplane'&&!e.crash){const air=g._nearest(e,t=>t.team>=0&&t.team!==team&&(t.type==='cd'||WARFARE_TYPES[t.type]?.flying)&&g.canTarget(e,t)&&dist(e,t)<750);if(air)g.useAbility([e.id],'intercept',{targetId:air.id});else if(nearby.filter(t=>familyCanDamage(e,t)&&!WARFARE_TYPES[t.type]?.flying).length>=5||e.hp<e.maxHp*.4375&&nearby.filter(t=>familyCanDamage(e,t)).length>=2)crashes.push(e);else if(e.order.type==='idle'&&!e.aiSquad)g.useAbility([e.id],'patrol',{points:[{x:start.x,y:start.y},{x:clamp(start.x+(g.center.x-start.x)*.55,30,g.width-30),y:clamp(start.y+(g.center.y-start.y)*.55,30,g.height-30)}]});}
  }
  if(rolls.length){const target=rolls[0].target,group=rolls.filter(r=>dist(r.unit,rolls[0].unit)<500);g.useAbility(group.map(r=>r.unit.id),'roll',{x:target.x,y:target.y});}
  if(crashes.length){const targets=enemies.filter(t=>familyCanDamage(crashes[0],t));if(targets.length){const t=targets.sort((a,b)=>dist(a,crashes[0])-dist(b,crashes[0]))[0];g.useAbility(crashes.map(e=>e.id),'crash',{x:t.x,y:t.y});}}
  return plan;
}
