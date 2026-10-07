/* Human doctrine and armored combat use observed positions, finite troops and bounded thinking. */
(function(){
'use strict';
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
const turn=(from,to,step)=>from+clamp(Math.atan2(Math.sin(to-from),Math.cos(to-from)),-step,step);
const ROLES={
 guard:{label:'Guard',hp:70,weapon:'pistol',range:230,speed:1},
 tracker:{label:'Tracker',hp:65,weapon:'rifle',range:330,speed:1.15},
 officer:{label:'Radio officer',hp:80,weapon:'pistol',range:300,speed:1},
 shield:{label:'Shield guard',hp:115,weapon:'shotgun',range:230,speed:.82},
 sniper:{label:'Marksman',hp:58,weapon:'sniper',range:510,speed:.8},
 grenadier:{label:'Grenadier',hp:85,weapon:'rifle',range:310,speed:.95},
 medic:{label:'Field medic',hp:65,weapon:'pistol',range:260,speed:1},
 flanker:{label:'Assault scout',hp:70,weapon:'assault',range:290,speed:1.22},
 gunner:{label:'Heavy gunner',hp:100,weapon:'machine',range:330,speed:.75},
 rifleman:{label:'Military rifleman',hp:85,weapon:'rifle',range:365,speed:1,accuracy:.8,weight:1.5},
 ranger:{label:'Ranger',hp:82,weapon:'assault',range:350,speed:1.3,accuracy:.65,weight:1.5},
 heavy:{label:'Heavy assault gunner',hp:112,weapon:'machine',range:380,speed:.78,accuracy:.85,weight:2},
 engineer:{label:'Combat engineer',hp:80,weapon:'rifle',range:320,speed:1,weight:1.5},
 leader:{label:'Squad leader',hp:90,weapon:'assault',range:380,speed:1.03,accuracy:.78,weight:1.5}
};
const VEHICLES={
 jeep:{label:'Scout jeep',hp:230,speed:95,radius:21,weight:4,capacity:0,range:300,weapon:'rifle',reload:1.2,front:1,side:1},
 command:{label:'Command vehicle',hp:260,speed:82,radius:21,weight:4,capacity:0,range:320,weapon:'rifle',reload:1.1,front:.8,side:1},
 armored:{label:'Armored patrol',hp:400,speed:70,radius:23,weight:6,capacity:0,range:330,weapon:'machine',reload:.35,front:.45,side:.75},
 truck:{label:'Troop truck',hp:250,speed:86,radius:24,weight:4,capacity:12,minCapacity:10,maxCapacity:16,range:0,front:1,side:1},
 apc:{label:'Armored personnel carrier',hp:550,speed:76,radius:25,weight:7,capacity:8,minCapacity:6,maxCapacity:10,range:370,weapon:'machine',reload:.25,front:.3,side:.65},
 ifv:{label:'Infantry fighting vehicle',hp:730,speed:64,radius:27,weight:9,capacity:5,minCapacity:4,maxCapacity:6,range:440,weapon:'machine',reload:.3,front:.3,side:.6,cannonReload:3.1,cannonDamage:48,cannonRadius:62},
 tank:{label:'Main battle tank',hp:1100,speed:50,radius:31,weight:12,capacity:0,range:570,weapon:'machine',reload:.22,front:.2,side:.5,cannonReload:6.5,cannonDamage:135,cannonRadius:108}
};
for(const spec of Object.values(VEHICLES))spec.budget=spec.weight;
class Forces {
 constructor(game){this.game=game;this.hazards=[];this.squads=new Map()}
 assign(h,site,requested){
  if(!requested&&h.role&&ROLES[h.role])return;
  const tier=Math.max(site?.tier||1,this.game.tier-1),n=ATSUtil.hash(h.id+this.game.seed)%100;let role=requested||'guard';
  if(!requested){if(tier<=1)role=n<20?'tracker':n<32?'officer':'guard';else if(tier===2)role=n<20?'shield':n<40?'tracker':n<55?'officer':n<70?'medic':'guard';else if(tier===3)role=['shield','sniper','grenadier','medic','officer','flanker','gunner','tracker'][n%8];else role=['rifleman','rifleman','ranger','heavy','grenadier','medic','engineer','leader'][n%8]}
  const spec=ROLES[role]||ROLES.guard;h.role=ROLES[role]?role:'guard';h.kind=spec.weapon;h.hp=h.maxHp=spec.hp;h.hasRadio=role==='officer'||role==='leader'||h.hasRadio;h.specialAt=this.game.time+4+Math.random()*4;h.flankSide=ATSUtil.hash(h.id)%2?1:-1;h.label=spec.label;h.accuracyMultiplier=spec.accuracy||1;
 }
 range(h){return ROLES[h.role]?.range||(h.kind==='pistol'?230:300)}
 armor(h,damage,source){if(h.role!=='shield'||!source)return damage;const g=this.game,angle=Math.atan2(source.y-h.y,source.x-h.x),frontal=Math.cos(angle-h.dir)>.45,swarm=g.apeGrid.near(h.x,h.y,52).length;if(frontal&&swarm<4){g.effect('text',h.x,h.y,{text:'BLOCK',life:.45,color:'#abc5cf'});return damage*.42}return damage}
 createSquad(members,objective,options={}){
  const g=this.game,id=options.id||'squad-'+g.nextId++,point={x:objective?.x??members[0]?.x??0,y:objective?.y??members[0]?.y??0},order=options.order||'Search';
  const squad={id,members:members.map(h=>h.id),objective:point,order,vehicleId:options.vehicleId||null,siteId:options.siteId||members[0]?.siteId,operationId:options.operationId||null,leaderId:members.find(h=>h.role==='leader'||h.role==='officer')?.id,reportAt:options.reportAt??g.time,nextThink:g.time,cohesionLossUntil:0,density:0};
  this.squads.set(id,squad);
  members.forEach((h,i)=>{h.squadId=id;h.squadSlot=i;h.squadRole=h.role;h.squadObjective={...point};h.squadOrder=order;h.squadVehicleId=squad.vehicleId;h.squadOperationId=squad.operationId;h.squadReportAt=squad.reportAt;h.navCohort=id;h.lastX=point.x;h.lastY=point.y});return squad;
 }
 restoreSquads(saved=[]){
  this.squads.clear();const groups=new Map(),records=new Map((Array.isArray(saved)?saved:[]).map(s=>[s.id,s])),g=this.game,people=[...g.humans];for(const s of g.world.sites.values())people.push(...(s.sleepingHumans||[]));
  for(const h of people)if(h.hp>0&&h.squadId){if(!groups.has(h.squadId))groups.set(h.squadId,[]);groups.get(h.squadId).push(h)}
  for(const [id,members]of groups){members.sort((a,b)=>(a.squadSlot||0)-(b.squadSlot||0));const h=members[0],saved=records.get(id),s=this.createSquad(members,saved?.objective||h.squadObjective||{x:h.lastX,y:h.lastY},{id,order:saved?.order||h.squadOrder,vehicleId:saved?.vehicleId||h.squadVehicleId,operationId:saved?.operationId||h.squadOperationId,reportAt:saved?.reportAt??h.squadReportAt});s.cohesionLossUntil=Math.max(saved?.cohesionLossUntil||0,...members.map(h=>h.cohesionLossUntil||0));s.nextReport=saved?.nextReport||0}
 }
 onDeath(a){
  const g=this.game;if(a.squadId){const squad=this.squads.get(a.squadId);if(squad?.leaderId===a.id){squad.leaderId=null;squad.cohesionLossUntil=g.time+7;for(const id of squad.members){const h=g.humansById.get(id);if(h&&h.hp>0){h.cohesionLossUntil=g.time+7;h.shootTimer=Math.max(h.shootTimer,.8);h.squadOrder='Regroup'}}g.effect('text',a.x,a.y,{text:'LEADER DOWN',life:1.3,color:'#e9bb8a'})}}
  if(a.id?.startsWith('vehicle')){a.troopsLost=(a.troopsLost||0)+(a.troops||0);a.troops=0;a.dismounting=false;a.cannonTarget=null}
 }
 // Estimate only a visually confirmed concentration. A bounded sample uses the
 // shared LOS budget; blocked forest samples cannot contribute to its estimate.
 observedDensity(observer,target){
  const g=this.game,candidates=g.apeGrid.near(target.x,target.y,220),count=Math.min(12,candidates.length);let visible=0,checked=0;
  for(let i=0;i<count;i++){const a=candidates[Math.floor(i*candidates.length/count)];if(a.hp<=0)continue;const sight=g.lineVisible(observer,a);if(sight===null)break;checked++;if(sight)visible++}
  return checked&&!(checked<count&&g.performance.losRemaining<=0)?Math.round(candidates.length*visible/checked):null;
 }
 thinkSquad(squad){
  const g=this.game;if(g.time<squad.nextThink||!g.performance.think())return;squad.nextThink=g.time+.5;
  const members=squad.members.map(id=>g.humansById.get(id)).filter(h=>h&&h.hp>0);if(!members.length)return;
  let witness=null,target=null;
  for(const h of members)if(h.state==='combat'&&h.targetId){const a=h.targetId==='king'?g.king:g.apesById.get(h.targetId);if(a?.hp>0&&g.time-(h.lastSeenAt??-100)<2&&distance(h,a)<=this.range(h)+40){const sight=g.lineVisible(h,a);if(sight){witness=h;target=a;break}if(sight===null)break}}
  if(target){squad.objective={x:target.x,y:target.y};squad.reportAt=g.time;const density=this.observedDensity(witness,target);if(density!==null)squad.density=density;squad.observedTargetId=target.id}
  else if(g.time-squad.reportAt>5){squad.density=0;squad.observedTargetId=null}
  const vehicle=g.vehicles.find(v=>v.id===squad.vehicleId&&v.hp>0);squad.vehicle=vehicle;
  const small=members.length<7,density=squad.density;
  if(g.time<squad.cohesionLossUntil)squad.order='Regroup';
  else if(target&&density>=150&&small)squad.order='Retreat';
  else if(target&&density>=80)squad.order='Retreat';
  else if(target&&density>=40)squad.order='Suppress';
  else if(target&&vehicle)squad.order='Protect Vehicle';
  else if(target)squad.order='Advance';
  else if(['Suppress','Retreat','Regroup','Advance','Protect Vehicle'].includes(squad.order))squad.order='Search';
  if(target&&density>=40&&g.time>=(squad.nextReport||0)){const radio=members.find(h=>h.hasRadio);const site=g.world.sites.get(radio?.siteId);if(radio&&!site?.radioDown){squad.nextReport=g.time+24;g.alert(radio,'radio')}}
  const anchor=vehicle||members.find(h=>h.id===squad.leaderId)||members[0];squad.anchor={x:anchor.x,y:anchor.y};
  for(const h of members){h.squadOrder=squad.order;h.squadObjective={...squad.objective};h.squadReportAt=squad.reportAt;h.lastX=squad.objective.x;h.lastY=squad.objective.y;h.cohesionLossUntil=squad.cohesionLossUntil;h.accuracyMultiplier=(ROLES[h.role]?.accuracy||1)*(squad.leaderId?.9:1);h.navCohort=squad.id}
  if(!target&&distance(anchor,squad.objective)>100)g.navigation.setCohortRoute(squad.id,anchor,squad.objective,10,3);
 }
 formationPoint(h,squad){
  const point=squad.objective,anchor=squad.anchor||h,vehicle=squad.vehicle,angle=Math.atan2(point.y-anchor.y,point.x-anchor.x),front={x:Math.cos(angle),y:Math.sin(angle)},side={x:-front.y,y:front.x},slot=h.squadSlot||0;
  let depth=(Math.floor(slot/4)-1)*32,lateral=((slot%4)-1.5)*39;
  if(h.role==='sniper'||h.role==='medic')depth=-115;else if(h.role==='heavy'||h.role==='gunner')depth=-58;else if(h.role==='ranger'||h.role==='flanker'){lateral=h.flankSide*145;depth=30}
  if(vehicle){depth-=50;lateral+=(slot%2?1:-1)*42}
  if(squad.order==='Retreat'){const home=this.game.world.sites.get(squad.siteId),cover=vehicle||home||{x:anchor.x-front.x*230,y:anchor.y-front.y*230};return {x:cover.x-front.x*70+side.x*lateral,y:cover.y-front.y*70+side.y*lateral}}
  if(squad.order==='Regroup')return {x:anchor.x-front.x*55+side.x*lateral,y:anchor.y-front.y*55+side.y*lateral};
  const battle=squad.density>0,base=battle?{x:point.x-front.x*(vehicle?260:200),y:point.y-front.y*(vehicle?260:200)}:point;
  return {x:base.x+front.x*depth+side.x*lateral,y:base.y+front.y*depth+side.y*lateral};
 }
 updateDoctrine(h,dt,perceived=false){
  const g=this.game,squad=this.squads.get(h.squadId);if(!squad)return false;this.thinkSquad(squad);
  // Individual perception, alarms and radio tells retain the existing state
  // machine. Formation steering runs in the intervals between perception ticks.
  if(!perceived&&h.perceptionTimer<=dt||h.state==='radio'||h.state==='alarm')return false;
  if(!perceived){h.perceptionTimer-=dt;h.shootTimer-=dt;h.attackTimer=Math.max(0,(h.attackTimer||0)-dt)}h.moving=false;
  const target=h.targetId==='king'?g.king:g.apesById.get(h.targetId),point=this.formationPoint(h,squad);
  if(h.state==='combat'&&target?.hp>0){
   const gap=distance(h,target);h.dir=Math.atan2(target.y-h.y,target.x-h.x);
   if(gap<45)g.move(h,h.x-target.x,h.y-target.y,64,dt);
   else if(distance(h,point)>30)g.move(h,point.x-h.x,point.y-h.y,squad.order==='Retreat'?83:62,dt);
   const cautious=squad.order==='Retreat'&&squad.members.length<7;
   if(!cautious&&h.shootTimer<=0&&gap<this.range(h)&&g.time>=(h._fireCheck||0)&&g.performance.think()){
    h._fireCheck=g.time+.14;const sight=g.lineVisible(h,target);if(sight){
     if(h.role==='heavy'){if(g.time>=(h.burstUntil||0)&&g.time>=(h.burstRestUntil||0)){h.burstUntil=g.time+(squad.density>=40?3.1:1.8);h.burstRestUntil=h.burstUntil+1.4}if(g.time<h.burstUntil)g.shoot(h,target)}else if(h.role!=='sniper')g.shoot(h,target);
    }else if(sight===false){h.targetId=null;h.state='search';h.searchTime=18}
   }
  }else{
   h.searchTime=Math.max(0,(h.searchTime||0)-dt);if(distance(h,point)>28)g.move(h,point.x-h.x,point.y-h.y,70,dt);else h.dir+=dt*.5;
  }
  return true;
 }
 updateEngineer(h,dt){
  const g=this.game;if(h.role!=='engineer')return false;
  if(h.constructing){
   if(g.apeGrid.near(h.x,h.y,90).length||h.hitTimer>0){h.constructing=null;h.specialAt=g.time+9;return false}
   h.constructing.remaining-=dt;h.moving=false;
   if(h.constructing.remaining<=0){const b=h.constructing,chunk=g.world.chunks.get(Math.floor(b.x/g.world.chunkSize)+','+Math.floor(b.y/g.world.chunkSize));if(chunk&&!g.world.blocked(b.x,b.y,17)){const o=g.world._object(chunk,{id:'field-'+g.nextId++,type:b.type,x:b.x,y:b.y,r:b.type==='wall'?18:9,w:44,h:13,collision:b.type==='wall'?'rect':undefined,hp:130,maxHp:130,height:b.type==='wall'?24:43,siteId:h.siteId,temporary:true,expiresAt:g.time+70});this.hazards.push({type:'fortification',id:o.id,x:o.x,y:o.y,start:g.time,life:70});g.world.navRevision++;g._lightsAt=-1;g.effect('text',o.x,o.y,{text:b.type==='wall'?'BARRICADE':'FIELD LIGHT',color:'#cbbd94',life:1.2})}h.constructing=null;h.specialAt=g.time+35}
   return true;
  }
  if(g.time>h.specialAt&&h.squadId&&['Hold','Suppress','Protect Vehicle','Defend Base'].includes(h.squadOrder)&&!g.apeGrid.near(h.x,h.y,150).length&&g.performance.think()){
   const x=h.x+Math.cos(h.dir)*28,y=h.y+Math.sin(h.dir)*28;if(!g.world.blocked(x,y,20)){h.constructing={x,y,type:ATSUtil.hash(h.id+Math.floor(g.time))%2?'wall':'tower',remaining:3.5};h.animation={kind:'build',start:g.time,duration:3.5};return true}
  }return false;
 }
 update(h,dt){
  const g=this.game;if(!h.role)this.assign(h,g.world.sites.get(h.siteId));h.hitTimer=Math.max(0,(h.hitTimer||0)-dt);if(h.hitTimer>.19){h.aiming=null;return true}
  if(!h.squadId&&h.state==='combat'&&g.tier>=3){const allies=g.humanGrid.near(h.x,h.y,140).filter(a=>a.hp>0&&!a.squadId&&a.siteId===h.siteId).slice(0,6);if(!allies.includes(h))allies.push(h);this.createSquad(allies,{x:h.lastX,y:h.lastY},{order:'Search',siteId:h.siteId})}
  if(h.role==='medic'&&g.time>h.specialAt&&g.time>=(h._roleThink||0)&&g.performance.think()){h._roleThink=g.time+.5;const ally=g.humanGrid.near(h.x,h.y,105).find(a=>a!==h&&a.hp>0&&a.hp<a.maxHp*.7);if(ally){ally.hp=Math.min(ally.maxHp,ally.hp+18);h.specialAt=g.time+6;h.animation={kind:'treat',start:g.time,duration:.5};g.effect('text',ally.x,ally.y,{text:'+18',color:'#92cdbb',life:1})}}
  if(h.role==='tracker'&&h.state==='patrol'&&g.time>=(h._roleThink||0)&&g.performance.think()){h._roleThink=g.time+.35;const sound=g.noiseGrid.near(h.x,h.y,1900).find(n=>distance(h,n)<n.radius*1.4);if(sound){h.state='investigate';h.lastX=sound.x;h.lastY=sound.y;h.searchTime=20}}
  if(this.updateEngineer(h,dt))return true;
  if(h.state==='combat'){
   const target=h.targetId==='king'?g.king:g.apesById.get(h.targetId);
   if(target?.hp>0){
    if(h.role==='sniper'&&distance(h,target)>85&&distance(h,target)<520){const sight=g.lineVisible(h,target);if(sight===null)return true;if(!sight){h.aiming=null;h.state='search';h.searchTime=15;return false}h.dir=Math.atan2(target.y-h.y,target.x-h.x);h.shootTimer-=dt;h.perceptionTimer-=dt;if(!h.aiming&&h.shootTimer<=0){h.aiming={x:target.x,y:target.y,until:g.time+1.35};g.sound('warning',.45,h.x)}if(h.aiming&&g.time>=h.aiming.until){g.shoot(h,{x:h.aiming.x,y:h.aiming.y});h.aiming=null;h.shootTimer=2.8}h.moving=false;return true}
    if(h.role==='grenadier'&&g.time>h.specialAt&&distance(h,target)>105&&distance(h,target)<330&&g.time>=(h._roleThink||0)&&g.performance.think()){h._roleThink=g.time+.3;if(g.lineVisible(h,target)){h.specialAt=g.time+9;h.animation={kind:'throw',start:g.time,duration:.6};this.hazards.push({id:'grenade-'+g.nextId++,type:'grenade',x:target.x,y:target.y,fromX:h.x,fromY:h.y,start:g.time,fuse:1.8,radius:67,life:1.8});g.sound('grenade',.5,h.x)}}
    if(h.role==='officer'&&g.time>h.specialAt&&g.exposure<.4){h.specialAt=g.time+24;this.hazards.push({id:'flare-'+g.nextId++,type:'flare',x:h.lastX,y:h.lastY,start:g.time,life:12,radius:150});g.noise(h.lastX,h.lastY,420,'flare');g.notify('A flare lights the last reported position.','red')}
   }
  }else h.aiming=null;
  return this.updateDoctrine(h,dt);
 }
 initVehicle(v,vehicleClass,prepaidTroops){
  const kind=vehicleClass||v.vehicleClass||v.kind||'jeep',spec=VEHICLES[kind]||VEHICLES.jeep;v.kind=v.vehicleClass=VEHICLES[kind]?kind:'jeep';v.navClass=v.vehicleClass;v.label=spec.label;v.radius=spec.radius;v.weight=spec.weight;
  if(!Number.isFinite(v.maxHp))v.maxHp=spec.hp;if(!Number.isFinite(v.hp))v.hp=v.maxHp;
  v.capacity=clamp(v.capacity??spec.capacity,0,spec.maxCapacity||spec.capacity);v.troops=clamp(prepaidTroops??v.troops??0,0,v.capacity);v.mobilityDamage=clamp(v.mobilityDamage??v.components?.mobility??0,0,100);v.weaponDamage=clamp(v.weaponDamage??v.components?.weapon??0,0,100);v.engineDamage=clamp(v.engineDamage??v.components?.engine??0,0,100);v.components={mobility:v.mobilityDamage,weapon:v.weaponDamage,engine:v.engineDamage};
  v.turretDir=v.turretDir??v.dir??0;v.dir=v.dir||0;v.shootTimer=v.shootTimer??1;v.cannonTimer=v.cannonTimer??3;v.phase=v.phase||0;v.swarmCount=v.swarmCount||0;v.overrun=!!v.overrun;v.rotationMultiplier=v.rotationMultiplier??1;v.turretMultiplier=v.turretMultiplier??1;v.accuracyMultiplier=v.accuracyMultiplier??1;v._militaryInitialized=true;return v;
 }
 vehicleDamage(v,damage,source){
  if(!v._militaryInitialized)this.initVehicle(v);const spec=VEHICLES[v.vehicleClass],bypass=source?.armorPiercing||source?.type==='shell';
  let frontal=0;if(source)frontal=Math.cos(Math.atan2(source.y-v.y,source.x-v.x)-v.dir);const fraction=bypass||!source?1:frontal>.5?spec.front:frontal<-.5?1:spec.side;
  if(!bypass&&source){const component=frontal<-.5?'engineDamage':Math.abs(frontal)<=.5?'mobilityDamage':'weaponDamage',before=v[component];v[component]=clamp(before+damage*fraction*.16,0,100);if(before<65&&v[component]>=65)this.game.effect('text',v.x,v.y,{text:component==='mobilityDamage'?'TRACKS DAMAGED':component==='engineDamage'?'ENGINE DAMAGED':'WEAPON DAMAGED',life:1.1,color:'#e7b981'})}
  v.components={mobility:v.mobilityDamage,weapon:v.weaponDamage,engine:v.engineDamage};return damage*fraction;
 }
 updateSwarm(v,dt){
  const g=this.game;if(g.time>=(v._swarmThink||0)&&g.performance.think()){v._swarmThink=g.time+.22;const apes=g.apeGrid.near(v.x,v.y,v.radius+25);v.swarmCount=apes.length;for(const a of apes)if(v.vehicleClass==='tank'||v.vehicleClass==='ifv'||v.vehicleClass==='apc'){a.climbingVehicleId=v.id;a.climbUntil=g.time+.35}}
  const n=v.swarmCount;v.rotationMultiplier=n>=5?.5:1;v.accuracyMultiplier=(n>=8?1.9:1)*(1+v.weaponDamage*.015);v.turretMultiplier=(n>=12?.16:n>=5?.65:1)*(1-v.weaponDamage*.006);v.overrun=n>=16;
  if(v.overrun){v.cannonTarget=null;v.cannonTimer=Math.max(2.4,v.cannonTimer);v.mobilityDamage=clamp(v.mobilityDamage+dt*4,0,100);v.weaponDamage=clamp(v.weaponDamage+dt*6,0,100);v.engineDamage=clamp(v.engineDamage+dt*4,0,100);g.hurt(v,(12+Math.min(n,35))*dt,{x:v.x,y:v.y,type:'overrun',armorPiercing:true});if(g.time>=(v._sparkAt||0)){v._sparkAt=g.time+.45;g.effect('smash',v.x,v.y,{life:.28,color:'#f7ce89'});g.effect('smoke',v.x,v.y,{life:1,color:'#69716b'})}}
  v.components={mobility:v.mobilityDamage,weapon:v.weaponDamage,engine:v.engineDamage};
 }
 vehicleMove(v,point,dt){
  const g=this.game,spec=VEHICLES[v.vehicleClass];if(v._simTier===2&&g.time>(v._swarmThink||0)+.5){v.swarmCount=0;v.overrun=false;v.rotationMultiplier=1;v.turretMultiplier=1-v.weaponDamage*.006;v.accuracyMultiplier=1+v.weaponDamage*.015}if(v.mobilityDamage>=100||v.engineDamage>=100){v.moving=false;return}
  const waypoint=g.navigation.steer(v,point,v.radius,dt);if(distance(waypoint,v)<1){v.moving=false;return}const angle=Math.atan2(waypoint.y-v.y,waypoint.x-v.x),heading=turn(v.dir,angle,(v.vehicleClass==='tank'?.5:.9)*v.rotationMultiplier*dt),alignment=Math.max(0,Math.cos(angle-heading));
  const speed=spec.speed*(1-v.mobilityDamage*.007)*(1-v.engineDamage*.004)*(v.overrun?.08:1)*alignment;
  if(speed>1)g.navigation.move(v,Math.cos(heading)*64,Math.sin(heading)*64,speed,dt,true);else v.moving=false;v.dir=heading;
 }
 dismount(v,dt){
  const g=this.game;if(!v.troops||v.hp<=0||g.humans.length>=220)return;v.dismounting=true;v.unloadTimer=(v.unloadTimer||0)-dt;if(v.unloadTimer>0)return;
  const roles=['leader','rifleman','rifleman','heavy','ranger','grenadier','medic','engineer','rifleman','sniper','rifleman','rifleman','ranger','rifleman','rifleman','medic'],i=v.deployedTroops||0,rear=v.dir+Math.PI,side=i%2?1:-1,p=g.findOpen(v.x+Math.cos(rear)*42+Math.cos(rear+Math.PI/2)*side*20,v.y+Math.sin(rear)*42+Math.sin(rear+Math.PI/2)*side*20,10);
  const h=g.makeHuman(p.x,p.y,g.world.sites.get(v.siteId));this.assign(h,g.world.sites.get(v.siteId),roles[i%roles.length]);h.responseAllocated=true;h.reported=true;h.state='search';h.searchTime=55;h.lastX=v.target?.x??v.x;h.lastY=v.target?.y??v.y;h.raidTarget=v.target?.id;v.troops--;v.deployedTroops=i+1;v.unloadTimer=.35;
  const squad=this.squads.get(v.dismountSquadId);if(squad){squad.members.push(h.id);h.squadId=squad.id;h.squadSlot=squad.members.length-1;h.squadRole=h.role;h.squadOrder=squad.order;h.squadObjective={...squad.objective};h.squadVehicleId=v.id;h.squadOperationId=squad.operationId;h.squadReportAt=squad.reportAt;h.navCohort=squad.id;if(h.role==='leader')squad.leaderId=h.id}
  else v.dismountSquadId=this.createSquad([h],v.target||v,{order:'Protect Vehicle',vehicleId:v.id,operationId:v.operationId,siteId:v.siteId}).id;
  if(!v.troops){v.dismounting=false;g.effect('text',v.x,v.y,{text:'TROOPS DEPLOYED',life:1.3,color:'#d2c7a3'})}
 }
 acquireVehicleTarget(v){
  const g=this.game,spec=VEHICLES[v.vehicleClass];if(g.time<(v._targetThink||0)||!g.performance.think())return;v._targetThink=g.time+.3;
  const perception=g.perceive(v,{x:v.x,y:v.y,dir:v.turretDir,range:spec.range||310,angle:v._targetId?.9:.6});if(perception.pending)return;const target=perception.seen;
  v._targetId=target?.id;if(target){v.lastSeen={x:target.x,y:target.y,time:g.time};v.state=v.state==='raid'?'raid':'combat';if(g.time>=(v._reportAt||0)){v._reportAt=g.time+8;g.addIntel(target.x,target.y,2)}}
 }
 fireShell(v,target,spec){
  const g=this.game,angle=Math.atan2(target.y-v.y,target.x-v.x),fromX=v.x+Math.cos(angle)*(v.vehicleClass==='tank'?53:37),fromY=v.y+Math.sin(angle)*(v.vehicleClass==='tank'?53:37),travel=Math.max(.18,Math.hypot(target.x-fromX,target.y-fromY)/570);
  this.hazards.push({id:'shell-'+g.nextId++,type:'shell',x:fromX,y:fromY,fromX,fromY,targetX:target.x,targetY:target.y,start:g.time,life:travel,duration:travel,speed:570,radius:spec.cannonRadius,damage:spec.cannonDamage,owner:v.id});
  if(v.vehicleClass==='ifv')v.cannonBurst=Math.max(0,(v.cannonBurst||1)-1);
  v.cannonFlash=.3;v.cannonTimer=(v.vehicleClass==='ifv'&&v.cannonBurst>0?.4:spec.cannonReload)*(1+v.weaponDamage*.018);g.effect('muzzle',fromX,fromY,{life:.3,color:'#ffe9aa'});g.effect('smoke',fromX,fromY,{life:1.1,color:'#b4ac89'});g.sound(v.vehicleClass==='tank'?'cannon':'gun',v.vehicleClass==='tank'?1.4:1,v.x);g.noise(v.x,v.y,900,'cannon');if(distance(v,g.king)<800){g.hitFlash=Math.max(g.hitFlash,.16);g.cannonShake=Math.max(g.cannonShake||0,v.vehicleClass==='tank'?6:3)}
 }
 updateVehicle(v,dt){
  if(!v._militaryInitialized)this.initVehicle(v);const g=this.game,spec=VEHICLES[v.vehicleClass];v.shootTimer-=dt;v.cannonTimer-=dt;v.cannonFlash=Math.max(0,(v.cannonFlash||0)-dt);v.phase+=dt;v.moving=false;if(v._simTier===2)return true;
  this.updateSwarm(v,dt);if(v.hp<=0)return true;this.acquireVehicleTarget(v);
  let target=v._targetId==='king'?g.king:g.apesById.get(v._targetId);if(target?.hp<=0)target=null;
  // A cannon commits to a warned position rather than tracking a dodge.
  const observed=target&&g.time-(v.lastSeen?.time??-100)<.7;if(!observed)target=null;
  const aimPoint=v.cannonTarget||target;if(aimPoint){const aim=Math.atan2(aimPoint.y-v.y,aimPoint.x-v.x);v.turretDir=turn(v.turretDir,aim,(v.vehicleClass==='tank'?.7:1.5)*v.turretMultiplier*dt)}
  if(v.target&&distance(v,v.target)>85){
   if(target&&v.vehicleClass==='tank'&&distance(v,target)<440){if(distance(v,target)<160)this.vehicleMove(v,{x:v.x-(target.x-v.x),y:v.y-(target.y-v.y)},dt)}
   else if(!v.dismounting)this.vehicleMove(v,v.target,dt);
  }else if(v.state==='raid')v.state='combat';
  if(v.troops&&(target&&distance(v,target)<300||v.target&&distance(v,v.target)<170||v.dismounting))this.dismount(v,dt);
  if(spec.weapon&&target&&v.weaponDamage<100&&v.shootTimer<=0){const align=Math.cos(Math.atan2(target.y-v.y,target.x-v.x)-v.turretDir);if(align>.88){const kind=v.kind,dir=v.dir;v.kind=spec.weapon;v.dir=v.turretDir;g.shoot(v,target);v.kind=kind;v.dir=dir;v.shootTimer=spec.reload*(1+v.weaponDamage*.018)}}
  if(spec.cannonReload&&v.weaponDamage<100&&!v.overrun){
   if(v.cannonTarget){const aim=Math.atan2(v.cannonTarget.y-v.y,v.cannonTarget.x-v.x);if(g.time>=v.cannonTarget.until&&Math.cos(aim-v.turretDir)>.96){const sight=g.lineVisible(v,v.cannonTarget);if(sight){this.fireShell(v,v.cannonTarget,spec);v.cannonTarget=null}else if(sight===false){v.cannonTarget=null;v.cannonTimer=1.5}}}
   else if(target&&v.cannonTimer<=0&&distance(v,target)>100){const duration=v.vehicleClass==='tank'?1.8:v.cannonBurst>0?.25:1.1;if(v.vehicleClass==='ifv'&&!v.cannonBurst)v.cannonBurst=2;v.cannonTarget={x:target.x,y:target.y,start:g.time,until:g.time+duration,duration};g.sound('warning',.7,v.x)}
  }else v.cannonTarget=null;
  if(v.engineDamage>55&&g.time>=(v._exhaustAt||0)){v._exhaustAt=g.time+.6;g.effect('smoke',v.x-Math.cos(v.dir)*25,v.y-Math.sin(v.dir)*25,{life:1.6,color:'#555f56'})}
  if(v.engineDamage>=100)g.hurt(v,24*dt,{x:v.x,y:v.y,armorPiercing:true,type:'engine'});
  if(g.time>=(v.nextSound||0)&&distance(v,g.king)<1250){v.nextSound=g.time+2.4;g.sound(v.vehicleClass==='tank'?'tank':v.vehicleClass==='truck'?'truck':['apc','ifv'].includes(v.vehicleClass)?'apc':'rumble',.45,v.x)}
  return true;
 }
 blast(h){
  const g=this.game,tank=h.type==='shell',radius=h.radius;g.effect('wave',h.x,h.y,{life:tank?.8:.5,range:radius,color:'#efbd7d'});g.effect('smoke',h.x,h.y,{life:tank?2.4:1.8,color:'#94705b'});g.effect('smash',h.x,h.y,{life:.55,color:'#f1c68c'});g.sound(tank?'cannon':'smash',tank?1.5:1.6,h.x);
  if(tank)g.effect('tankImpact',h.x,h.y,{life:1.1,radius,range:radius,color:'#f0c789'});
  if(tank&&distance(h,g.king)<600){g.hitFlash=Math.max(g.hitFlash,.35);g.cannonShake=Math.max(g.cannonShake||0,11)}
  for(const a of g.apeGrid.near(h.x,h.y,radius))if(a.hp>0&&g.world.lineClear(h.x,h.y,a.x,a.y)){
   const d=distance(a,h),falloff=1-d/radius*.55;g.hurt(a,(h.damage||72)*falloff,h);
   if(tank&&a.hp>0){const angle=d>.01?Math.atan2(a.y-h.y,a.x-h.x):ATSUtil.hash(a.id)%628/100,push=28+32*falloff,x=a.x+Math.cos(angle)*push,y=a.y+Math.sin(angle)*push,r=a.id==='king'?12:10;if(!g.world.blocked(x,y,r)&&g.navigation.clearSegment(a.x,a.y,x,y,r)){a.x=x;a.y=y}a.knockbackUntil=g.time+.32;a.staggerUntil=g.time+.45;a.animation={kind:'stagger',start:g.time,duration:.4}}
  }
  for(const a of g.humanGrid.near(h.x,h.y,radius))if(a.hp>0&&g.world.lineClear(h.x,h.y,a.x,a.y))g.hurt(a,tank?(h.damage||72)*.65:60,h);
 }
 tick(dt){
  const g=this.game;for(const h of this.hazards){h.life-=dt;
   if(h.type==='shell'){const t=clamp(1-h.life/h.duration,0,1),x=h.fromX+(h.targetX-h.fromX)*t,y=h.fromY+(h.targetY-h.fromY)*t;
    if(!g.navigation.clearSegment(h.x,h.y,x,y,2)){let lo=0,hi=1;for(let i=0;i<5;i++){const mid=(lo+hi)/2;if(g.navigation.clearSegment(h.x,h.y,h.x+(x-h.x)*mid,h.y+(y-h.y)*mid,2))lo=mid;else hi=mid}h.x+=(x-h.x)*lo;h.y+=(y-h.y)*lo;h.life=0}else{h.x=x;h.y=y}if(h.life<=0)this.blast(h);
   }else if(h.type==='grenade'&&h.life<=0)this.blast(h);
   else if(h.type==='fortification'&&h.life<=0){const o=g.world.objects.get(h.id);if(o&&!o.dead){o.dead=true;o.solid=false;o.hp=0;g.world.navRevision++;g._lightsAt=-1}}
  }this.hazards=this.hazards.filter(h=>h.life>0);
  if(g.time>=(this.nextSquadTrim||0)){this.nextSquadTrim=g.time+4;for(const [id,s]of this.squads)if(!s.members.some(id=>g.humansById.has(id))&&!Array.from(g.world.sites.values()).some(site=>(site.sleepingHumans||[]).some(h=>h.hp>0&&h.squadId===id)))this.squads.delete(id)}
 }
}
window.ATSForces=Forces;window.ATSHumanRoles=ROLES;window.ATSVehicleSpecs=VEHICLES;
})();
