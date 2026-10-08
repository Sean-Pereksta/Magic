/* Soldiers shoot the nearest visible ape structure, including invasion palisades. */
(() => {
'use strict';
const G=ATSGame.prototype,F=ATSForces.prototype,C=ATSSettlements.prototype;
const defenses=new Set(['spearTower','spearBattery','spearBallista']),duties=new Set(['Fallback','Retreat','Regroup','Shadow']),SIGHT_BUDGET=32;
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),palisade=o=>!!o?.fortification&&(o.team||o.owner)==='ape',alive=o=>(o?.type==='apeBuilding'||palisade(o))&&o.hp>0&&!o.dead&&o.solid;
function budget(g){if(!g._structureBudget||g._structureBudget.time!==g.time)g._structureBudget={time:g.time,acquisitions:0,los:0};return g._structureBudget}
function sight(g,h,o){
 const b=budget(g);if(b.los>=SIGHT_BUDGET||g.performance.losRemaining<=g.performance.heavyLosReserve)return null;
 b.los++;g.performance.losRemaining--;g.performance.counters.losTests++;
 if(g.siege)return g.siege.clearRay(h,o,{ignore:o.id});
 const d=distance(h,o)||1,face=Math.min(35,d*.5);return g.world.lineClear(h.x,h.y,o.x+(h.x-o.x)/d*face,o.y+(h.y-o.y)/d*face);
}
F.structureTarget=function(h){
 const g=this.game,known=g.world.objects.get(h._structureTargetId),range=Math.min(480,this.range(h)),revision=g.world.navRevision||0,changed=h._structureScanRevision!==revision||known&&!alive(known)||Math.hypot(h.x-(h._structureScanX??h.x),h.y-(h._structureScanY??h.y))>35;
 if(!changed&&g.time<(h._structureScanAt||0))return alive(known)&&distance(h,known)<range?known:null;
 const work=budget(g);if(work.acquisitions>=16||!g.performance.think())return null;
 work.acquisitions++;h._structureScanAt=g.time+.6+(ATSUtil.hash(h.id)%11)*.03;h._structureScanRevision=revision;h._structureScanX=h.x;h._structureScanY=h.y;
 const choices=[];
 g.world._queryCollision(h.x-range,h.y-range,h.x+range,h.y+range,o=>{
  if(!alive(o))return;const d=distance(h,o);if(d>range)return;
  const home=o.settlementId?g.settlement(o.settlementId):null;if(!home&&!palisade(o))return;
  const tower=defenses.has(o.kind),building=tower?(home?.facilities||[]).find(f=>f.id===o.structureId):null;
  const recent=tower&&g.time-(building?.lastShot??-100)<8,assigned=!!h.raidTarget&&(h.raidTarget===home?.id||palisade(o)&&!home);
  if(!assigned&&h.state==='patrol'&&!recent)return;
  if(!assigned&&d>85&&Math.cos(Math.atan2(o.y-h.y,o.x-h.x)-(h.dir||0))<.15)return;
  choices.push({o,score:d});choices.sort((a,b)=>a.score-b.score);if(choices.length>6)choices.length=6;
 });
 for(const {o}of choices){const clear=sight(g,h,o);if(clear===null)break;if(clear){h._structureTargetId=o.id;h._structureTargetUntil=g.time+1.2;return o}}
 delete h._structureTargetId;return null;
};
F.updateStructureCombat=function(h,dt){
 const g=this.game;if(!g.settlements.length&&g.navigation.hasFortifications===false||h.hp<=0||h._simTier===2||h.engineerJob||h.hitTimer>.19||h.blastReaction||h.siegeTransition||h.onWallId||['mortar','medic','rotary','bombardier'].includes(h.role)||duties.has(this.squads.get(h.squadId)?.order))return false;
 if(!h.role)this.assign(h,g.world.sites.get(h.siteId));
 const target=this.structureTarget(h);if(!target)return false;
 // Keep ordinary ape combat when a visible ape is closer. A distant remembered
 // ape does not stop an invading rifleman from shooting the palisade in front.
 // One candidate beyond the entire ray budget is an overflow sentinel. If a
 // nearer crowd cannot be ruled out this frame, defer to ordinary perception
 // rather than incorrectly choosing the wall after a small occluded sample.
 const actor=h.targetId==='king'?g.king:g.apesById.get(h.targetId),range=distance(h,target),remaining=Math.max(0,Math.min(SIGHT_BUDGET-budget(g).los,g.performance.losRemaining-g.performance.heavyLosReserve)),nearby=g.apeGrid.nearest(h.x,h.y,range,remaining+1,a=>a.hp>0);
 if(actor?.hp>0&&distance(h,actor)<range&&!nearby.includes(actor))nearby.push(actor);
 nearby.sort((a,b)=>distance(h,a)-distance(h,b));
 for(const a of nearby){if(distance(h,a)>=range)continue;const seen=sight(g,h,a);if(seen===null)return false;if(seen){h.targetId=a.id;h.lastSeenAt=g.time;h.state='combat';delete h.structureTarget;return false}}
 const clear=sight(g,h,target);if(clear!==true){if(clear===false){delete h._structureTargetId;h._structureTargetUntil=0}return false}
 h._structureHandledUntil=g.time+2.1;h.structureTarget=target.structureId||target.id;h.dir=Math.atan2(target.y-h.y,target.x-h.x);h.moving=false;h.shootTimer-=dt;h.perceptionTimer-=dt;h.hitTimer=Math.max(0,(h.hitTimer||0)-dt);h.attackTimer=Math.max(0,(h.attackTimer||0)-dt);
 if(h.shootTimer<=0)g.shoot(h,target);return true;
};
const update=F.update;
F.update=function(h,dt){if(this.updateStructureCombat(h,dt))return true;return update.call(this,h,dt)};
const siege=C.siege;
C.siege=function(s,raiders){return siege.call(this,s,raiders.filter(h=>(h._structureHandledUntil||0)<=this.game.time))};
const shoot=G.shoot;
G.shoot=function(h,target){const first=this.bullets.length,result=shoot.call(this,h,target);if(alive(target))for(let i=first;i<this.bullets.length;i++){const b=this.bullets[i];b.structureShot=true;b.structureTarget=target.id}return result};
function rectEntry(x,y,dx,dy,o){
 const hx=(o.w||o.r*2||40)/2+1,hy=(o.h||o.r*2||40)/2+1;let lo=0,hi=1;
 for(const [start,delta,min,max]of[[x,dx,o.x-hx,o.x+hx],[y,dy,o.y-hy,o.y+hy]]){if(Math.abs(delta)<1e-8){if(start<min||start>max)return null}else{let a=(min-start)/delta,b=(max-start)/delta;if(a>b)[a,b]=[b,a];lo=Math.max(lo,a);hi=Math.min(hi,b);if(lo>hi)return null}}
 return lo;
}
const bullets=G.updateBullets;
G.updateBullets=function(dt){
 for(const b of this.bullets){
  if(!b.structureShot||b.life<=dt)continue;
  const dx=b.vx*dt,dy=b.vy*dt,z=b.z??20,dz=(b.vz||0)*dt,length=Math.hypot(dx,dy);let hit=null,at=1;
  this.world._queryCollision(Math.min(b.x,b.x+dx)-1,Math.min(b.y,b.y+dy)-1,Math.max(b.x,b.x+dx)+1,Math.max(b.y,b.y+dy)+1,o=>{if(!alive(o))return;const t=rectEntry(b.x,b.y,dx,dy,o),height=o.visualHeight||(palisade(o)?Math.max(38,o.height||0):o.height||40);if(t!==null&&t<=at&&z+dz*t<height+2){at=t;hit=o}});
  if(!hit)continue;
  // Let the normal projectile integrator handle any ape standing before the wall.
  let intercepted=false;const l2=length*length;
  for(const a of this.apeGrid.near(b.x+dx*.5,b.y+dy*.5,length*.5+16)){
   if(a.hp<=0||!l2)continue;const projection=((a.x-b.x)*dx+(a.y-b.y)*dy)/l2,cross=(a.x-b.x-dx*projection)**2+(a.y-b.y-dy*projection)**2,entry=Math.max(0,projection-Math.sqrt(Math.max(0,256-cross)/l2));
   if(projection>=0&&projection<=1+16/(length||1)&&cross<=256&&entry<at&&(!this.siege||Math.abs(z+dz*entry-((a.elevation||a.wallClimbHeight||0)+18))<=23)){intercepted=true;break}
  }
  if(intercepted)continue;
  const x=b.x+dx*at,y=b.y+dy*at,clear=this.siege?this.siege.clearRay(b,{x,y},{za:z,zb:z+dz*at,ignore:hit.id,projectile:true}):this.world.lineClear(b.x,b.y,x,y);
  if(!clear)continue;
  this.damageObject(hit,b.damage*.55,{x:b.x,y:b.y,type:'bullet',owner:b.owner});b.x=x;b.y=y;b.z=z+dz*at;b.life=0;this.effect('hit',x,y,{life:.2,color:'#e1be83'});
 }
 return bullets.call(this,dt);
};
})();
