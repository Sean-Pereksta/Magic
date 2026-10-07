(function(){
'use strict';
const PERSONALITIES=['bold','guardian','saboteur','rescuer','skirmisher'];
const LIGHT=new Set(['capuchin','gibbon','chimpanzee']);
const FOLLOW=new Set(['follow','charge','hold']);
const d2=(a,b)=>(a.x-b.x)**2+(a.y-b.y)**2;
const lerp=(a,b,t)=>a+(b-a)*t;
class ApeTactics{
 constructor(game){this.g=game}
 ensure(a){
  if(a.id==='king')return a;
  if(!PERSONALITIES.includes(a.personality))a.personality=PERSONALITIES[ATSUtil.hash(this.g.seed+'|personality|'+a.id)%PERSONALITIES.length];
  a.trainingLevel=Math.max(0,Math.min(3,Math.floor(Number.isFinite(a.trainingLevel)?a.trainingLevel:0)));
  return a;
 }
 clearOrder(a){if(a.wallClimb||a.onWallId)this.detach(a);delete a.commandStyle;delete a._tacticTarget;delete a._tacticThink}
 order(style,aim){
  const g=this.g,list=g.apes.filter(a=>a.hp>0&&FOLLOW.has(a.state)),len=Math.hypot(aim.x,aim.y)||1,angle=Math.atan2(aim.y/len,aim.x/len);
  for(let i=0;i<list.length;i++){
   const a=list[i];this.clearOrder(a);a.state='charge';a.commandStyle=style;a.chargeTime=13;a.retreatUntil=0;
   // Stable lanes make a wide front without changing the gameplay random stream.
   const fan=style==='spreadCharge'?((i+.5)/list.length-.5)*1.75:0,heading=angle+fan;
   a.target={x:g.king.x+Math.cos(heading)*570,y:g.king.y+Math.sin(heading)*570};
   if(style==='attackNearest')a.target={x:a.x,y:a.y};
  }
  return list.length;
 }
 preferred(a,t,kind){
  const g=this.g,p=a.personality;
  if(p==='rescuer'&&kind==='object'&&t.type==='cage'&&(t.remaining??t.count??t.prisoners??0)>0)return 150;
  if(p==='saboteur'&&(kind==='vehicle'||kind==='object'&&(['radio','alarm','depot','fuel','fuelDepot','barracks','command','commandHouse','wall','humanBarricade','heavyBarricade','gate'].includes(t.type)||t.commandCenter)))return 115;
  if(p==='skirmisher'&&kind==='human'&&['sniper','officer','medic','spotter'].includes(t.role||t.kind))return 115;
  if(p==='bold'&&(kind==='vehicle'||kind==='human'&&['heavy','gunner','assault'].includes(t.role||t.kind)))return 90;
  if(p==='guardian'&&kind==='human'){
   const home=a.settlementId?g.settlement(a.settlementId):g.king;
   if(t.targetId==='king'||t.raidTarget===a.settlementId||home&&d2(t,home)<160**2)return 130;
  }
  return 0;
 }
 pick(a,range){
  const g=this.g,candidates=g.humanGrid.nearest(a.x,a.y,range,12).map(t=>({t,kind:'human'}));
  if(a.state==='charge'){
   for(const t of g.vehicleGrid.nearest(a.x,a.y,range,4))candidates.push({t,kind:'vehicle'});
   const objects=g.world.getObjects(a.x,a.y,range).filter(t=>!t.dead&&t.hp>0&&!['tree','rock','berry','apeBuilding'].includes(t.type)&&!(t.fortification&&t.team==='ape'));
   objects.sort((x,y)=>g.objectDistance(a,x)-g.objectDistance(a,y));
   for(const t of objects.slice(0,12))candidates.push({t,kind:'object'});
  }
  let best=null,score=Infinity;
  for(const c of candidates){
   const distance=c.kind==='object'?g.objectDistance(a,c.t):Math.sqrt(d2(a,c.t));
   if(distance>range)continue;
   const s=distance-(a.commandStyle==='attackNearest'?0:this.preferred(a,c.t,c.kind));
   if(s<score){score=s;best={id:c.t.id,kind:c.kind}}
  }
  return best;
 }
 resolve(record){const g=this.g;return record?.kind==='human'?g.humansById.get(record.id):record?.kind==='vehicle'?g.vehicles.find(v=>v.id===record.id):record?.kind==='object'?g.world.objects.get(record.id):null}
 canClimb(a,wall){
  if(a.id==='king'||a.state==='young'||a.state==='free'||!wall||wall.dead||wall.hp<=0||wall.type!=='wall'||wall.faction!=='human'||wall.climbable===false)return false;
  const tier=wall.wallTier||1;
  return LIGHT.has(a.species)?tier<=2:a.trainingLevel>0&&tier===1;
 }
 tryClimb(a,dx,dy,wall){
  const g=this.g;if(a.wallClimb||a.onWallId||g.blastActive(a)||g.time<(a._climbRetry||0))return false;
  if(a.id==='king'||a.state==='young'||a.state==='free'||!LIGHT.has(a.species)&&!(a.trainingLevel>0))return false;
  if(!wall){if(g.time<(a._climbCheckAt||0))return false;a._climbCheckAt=g.time+.2;wall=g.world.getObjects(a.x,a.y,65).filter(o=>this.canClimb(a,o)&&g.objectDistance(a,o)<27).sort((x,y)=>g.objectDistance(a,x)-g.objectDistance(a,y))[0]}
  if(!this.canClimb(a,wall)||g.objectDistance(a,wall)>29)return false;
  const vertical=(wall.w||30)<(wall.h||30),delta=vertical?dx:dy,offset=vertical?a.x-wall.x:a.y-wall.y;
  if(Math.abs(delta)<Math.hypot(dx,dy)*.3||Math.abs(delta)<1||offset*delta>=0)return false;
  const side=offset<0?-1:1,half=(vertical?wall.w:wall.h)/2;
  const crest={x:vertical?wall.x:a.x,y:vertical?a.y:wall.y};
  const exit={x:vertical?wall.x-side*(half+24):a.x,y:vertical?a.y:wall.y-side*(half+24)};
  if(g.navigation.blocked(exit.x,exit.y,10,'ape')||!g.navigation.clearSegment(a.x,a.y,exit.x,exit.y,10,'ape',wall.id)){a._climbRetry=g.time+.7;return false}
  a.wallClimb={wallId:wall.id,fromX:a.x,fromY:a.y,crestX:crest.x,crestY:crest.y,exitX:exit.x,exitY:exit.y,elapsed:0,height:Math.min(60,wall.visualHeight||wall.height||28)};
  a.climbingWallId=wall.id;a.wallClimbUntil=g.time+2.5;a.wallClimbHeight=0;a.moving=true;a._nav=null;
  return true;
 }
 detach(a){
  const c=a.wallClimb,g=this.g;
  if(c){
   const points=c.elapsed>1.1?[{x:c.exitX,y:c.exitY},{x:c.fromX,y:c.fromY}]:[{x:c.fromX,y:c.fromY},{x:c.exitX,y:c.exitY}];
   const safe=points.find(p=>Number.isFinite(p.x)&&Number.isFinite(p.y)&&!g.navigation.blocked(p.x,p.y,10,'ape'));
   if(safe){a.x=safe.x;a.y=safe.y}else{const p=g.findOpen(a.x,a.y);a.x=p.x;a.y=p.y}
  }
  delete a.wallClimb;delete a.climbingWallId;delete a.onWallId;delete a.wallClimbUntil;a.wallClimbHeight=0;a._climbRetry=g.time+.8;
 }
 restore(a){
  this.ensure(a);const c=a.wallClimb;
  if(!c){delete a.climbingWallId;delete a.onWallId;a.wallClimbHeight=0;return}
  let valid=['fromX','fromY','crestX','crestY','exitX','exitY','elapsed','height'].every(k=>Number.isFinite(c[k]))&&c.elapsed>=0&&c.elapsed<=2.5&&c.height>=0&&c.height<=60&&Math.hypot(c.exitX-c.fromX,c.exitY-c.fromY)<120;
  const wall=this.g.world.objects.get(c.wallId);
  if(valid&&wall){const vertical=(wall.w||30)<(wall.h||30),half=(vertical?wall.h:wall.w)/2,normal=vertical?'X':'Y',along=vertical?'Y':'X',center=vertical?wall.x:wall.y;
   valid=Math.abs((vertical?c.crestX:c.crestY)-center)<1&&Math.abs((vertical?c.crestY:c.crestX)-(vertical?wall.y:wall.x))<=half+1&&Math.hypot(a.x-c.crestX,a.y-c.crestY)<120&&Math.hypot(c.fromX-c.crestX,c.fromY-c.crestY)<80&&Math.hypot(c.exitX-c.crestX,c.exitY-c.crestY)<80&&(c['from'+normal]-center)*(c['exit'+normal]-center)<0&&Math.abs(c['from'+along]-c['crest'+along])<1&&Math.abs(c['exit'+along]-c['crest'+along])<1;
  }
  if(!wall)valid=false;
  if(!valid){delete a.wallClimb;this.detach(a);if(this.g.navigation.blocked(a.x,a.y,10,'ape')){const safe=this.g.findOpen(a.x,a.y);a.x=safe.x;a.y=safe.y}return}
  if(!this.canClimb(a,wall)||this.g.blastActive(a)||this.g.navigation.blocked(c.fromX,c.fromY,10,'ape')||this.g.navigation.blocked(c.exitX,c.exitY,10,'ape')||!this.g.navigation.clearSegment(c.fromX,c.fromY,c.exitX,c.exitY,10,'ape',wall.id))this.detach(a);
 }
 updateClimb(a,dt){
  const c=a.wallClimb;if(!c)return false;const g=this.g,wall=g.world.objects.get(c.wallId);
  if(!this.canClimb(a,wall)||a.hp<=0){this.detach(a);return false}
  c.elapsed+=dt;const t=c.elapsed;
  let x,y,z;
  if(t<.85){const p=Math.min(1,t/.85);x=lerp(c.fromX,c.crestX,p);y=lerp(c.fromY,c.crestY,p);z=c.height*Math.sin(p*Math.PI/2);a.climbingWallId=wall.id;delete a.onWallId}
  else if(t<1.85){x=c.crestX;y=c.crestY;z=c.height;a.onWallId=wall.id;delete a.climbingWallId;
   if(a.attackCD<=0){a.attackCD=.65;g.animateAttack(a,wall,'overhead');g.damageObject(wall,g.apeDamage(a,20),a)}
  }else{const p=Math.min(1,(t-1.85)/.65);x=lerp(c.crestX,c.exitX,p);y=lerp(c.crestY,c.exitY,p);z=c.height*(1-p);a.climbingWallId=wall.id;delete a.onWallId}
  if(!g.navigation.clearSegment(a.x,a.y,x,y,10,'ape',wall.id)){this.detach(a);return true}
  a.x=x;a.y=y;a.wallClimbHeight=z;a.dir=Math.atan2(c.exitY-c.fromY,c.exitX-c.fromX);a.moving=true;
  if(t>=2.5)this.detach(a);
  return true;
 }
 combat(a,dt,st){
  const g=this.g;
  let range=a.state==='charge'?(a.commandStyle==='attackNearest'?320:220):a.state==='scout'?165:FOLLOW.has(a.state)?90:st?.attack?190:70;
  if(a.state==='follow'&&a.retreatUntil>g.time)range=24;
  if(g.time>=(a._tacticThink||0)&&g.performance.think('ape')){a._tacticThink=g.time+.2+(ATSUtil.hash(a.id)%11)*.008;a._tacticTarget=this.pick(a,range)}
  let target=this.resolve(a._tacticTarget);
  if(target&&(target.dead||target.hp<=0||(a._tacticTarget.kind==='object'?g.objectDistance(a,target):Math.sqrt(d2(a,target)))>range))target=null;
  if(a.state==='free'&&(!target||d2(a,target)>30**2))return false;
  if(a.state==='charge'||target){
   const wall=g.world.objects.get(a.onWallId);
   const barrier=wall&&!wall.dead?wall:g.world.getObjects(a.x,a.y,110).filter(o=>!o.dead&&o.hp>0&&o.solid&&(o.faction==='human'||['humanBarricade','heavyBarricade','fieldBarricade'].includes(o.type))&&g.objectDistance(a,o)<32).sort((x,y)=>g.objectDistance(a,x)-g.objectDistance(a,y))[0];
   if(barrier){
    const goal=target&&target.id!==barrier.id?target:a.target||g.king;
    if(a.state==='charge'&&this.tryClimb(a,goal.x-a.x,goal.y-a.y,barrier))return true;
    if(a.attackCD<=0){a.attackCD=.65;g.animateAttack(a,barrier,'overhead');g.damageObject(barrier,g.apeDamage(a,20+Math.min(20,g.apeGrid.near(a.x,a.y,60).length*2)),a)}return true;
   }
  }
  if(!target)return false;
  const kind=a._tacticTarget.kind,distance=kind==='object'?g.objectDistance(a,target):Math.sqrt(d2(a,target)),reach=kind==='human'?30:kind==='vehicle'?40:28;
  if(distance>reach||kind!=='object'&&!g.world.lineClear(a.x,a.y,target.x,target.y))g.move(a,target.x-a.x,target.y-a.y,a.speed*(a.state==='charge'?1.35:1.12),dt);
  else if(a.attackCD<=0){
   a.attackCD=kind==='human'?.8:.7;g.animateAttack(a,target,kind==='object'?'overhead':kind==='vehicle'?'slam':undefined);
   const damage=g.apeDamage(a,kind==='human'?18+Math.min(12,g.apeGrid.near(target.x,target.y,48).length*2):kind==='vehicle'?24:18);
   if(kind==='object')g.damageObject(target,damage,a);else{g.hurt(target,damage,a);if(kind==='vehicle'&&target.hp<=0){g.stats.structures++;g.sound('smash',2,target.x)}}
  }
  return true;
 }
}
window.ATSApeTactics=ApeTactics;
window.ATSApePersonalities=Object.freeze(PERSONALITIES);
})();
