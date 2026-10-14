/* Directional tap/hold charges share a scan range; holds exclude structures. */
(() => {
'use strict';
// Both E gestures scan and retain targets through the full charge distance.
const FOLLOW=new Set(['follow','charge','hold']),RANGE=570;
const HOSTILE=new Set(['wall','gate','cage','gateControl','tower','radio','alarm','barracks','depot','fuel','fuelDepot','house','vehicle','generator','prisonControl','powerRelay','gatlingNest','mortarNest','humanBarricade','heavyBarricade','fieldBarricade','fieldSearchlight','fieldObservation','observationPost']);
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),P=ATSGame.prototype;
const heading=aim=>{const x=Number.isFinite(aim?.x)?aim.x:1,y=Number.isFinite(aim?.y)?aim.y:0,len=Math.hypot(x,y);return len>0?{x:x/len,y:y/len}:{x:1,y:0}};
const setDirection=(a,order,aim)=>{const d=heading(aim);order.dx=d.x;order.dy=d.y;order.goal={x:a.x+d.x*RANGE,y:a.y+d.y*RANGE}};
// Anchor the forward cone to the issued charge, so routing around an obstacle
// does not change which side of the battlefield the player ordered an attack on.
const inDirection=(a,t)=>{const o=a.nearestOrder;if(!o?.goal)return true;const x=t.x-(o.goal.x-o.dx*RANGE),y=t.y-(o.goal.y-o.dy*RANGE);return x*o.dx+y*o.dy>=Math.hypot(x,y)*.5-1e-6};
const hostileObject=o=>!o.dead&&o.hp>0&&o.team!=='ape'&&o.faction!=='ape'&&o.owner!=='ape'&&!o.settlementId&&HOSTILE.has(o.type)&&(o.type!=='cage'||(o.count??o.prisoners??0)>0)&&(o.type!=='gate'||o.solid);
const troopTarget=(t,kind)=>kind==='human'||kind==='vehicle'||kind==='object'&&t.type==='vehicle';
const LIGHT=new Set(['capuchin','gibbon','chimpanzee']),climbPlans=new WeakMap();
// A ground-route failure does not prove that a climber cannot reach its target.
// Inspect only the already loaded charge corridor, with a strict obstacle and
// wall cap. Each crossing ignores its own wall; trees, water and other walls
// still need real clearance. These plans never request A* or stream the world.
ATSApeTactics.prototype.climbApproach=function(a,t){
 const g=this.g,nav=g.navigation,w=g.world,r=a.radius||10,d=distance(a,t);
 if(a.id==='king'||a.state==='young'||a.state==='free'||!LIGHT.has(a.species)&&!(a.trainingLevel>0)||!w._queryCollision||d<1||d>RANGE)return null;
 let cache=climbPlans.get(a);if(!cache)climbPlans.set(a,cache=new Map());
 const old=cache.get(t.id),revision=w.navRevision||0,chunks=w.chunkRevision||0;
 if(old&&old.revision===revision&&old.chunks===chunks&&g.time-old.time<.25&&distance(a,old.from)<12&&distance(t,old.target)<12)return old.plan;
 const remember=plan=>{cache.set(t.id,{revision,chunks,time:g.time,from:{x:a.x,y:a.y},target:{x:t.x,y:t.y},plan});if(cache.size>6)cache.delete(cache.keys().next().value);return plan};
 const dx=t.x-a.x,dy=t.y-a.y,objects=[],walls=[];let overflow=false;
 w._queryCollision(Math.min(a.x,t.x)-r,Math.min(a.y,t.y)-r,Math.max(a.x,t.x)+r,Math.max(a.y,t.y)+r,o=>{if(objects.length>=128){overflow=true;return false}objects.push(o)});
 if(overflow)return remember(null);
 for(const wall of objects){
  if(!wall.solid||!this.canClimb(a,wall))continue;
  const vertical=(wall.w||30)<(wall.h||30),delta=vertical?dx:dy,offset=vertical?a.x-wall.x:a.y-wall.y,end=vertical?t.x-wall.x:t.y-wall.y;
  if(Math.abs(delta)<d*.3||offset*end>=0)continue;
  const u=-offset/delta,along=(vertical?a.y:a.x)+(vertical?dy:dx)*u,center=vertical?wall.y:wall.x,half=(vertical?wall.w:wall.h)/2,span=(vertical?wall.h:wall.w)/2;
  if(u<=0||u>=1||Math.abs(along-center)>span-r||Math.abs(end)<half+24)continue;
  const side=Math.sign(offset),approach={x:vertical?wall.x+side*(half+r+4):along,y:vertical?along:wall.y+side*(half+r+4)},exit={x:vertical?wall.x-side*(half+24):along,y:vertical?along:wall.y-side*(half+24)};
  walls.push({wallId:wall.id,u,approach,exit,vertical,half,span});
 }
 if(!walls.length||walls.length>4)return remember(null);walls.sort((a,b)=>a.u-b.u);
 let from=a,cost=0,walkCost=d;
 const clear=(a,b,ignore)=>nav.clearSegment(a.x,a.y,b.x,b.y,r,'ape',ignore,objects);
 for(const step of walls){
  if(!clear(from,step.approach)||!clear(step.approach,step.exit,step.wallId))return remember(null);
  const wall=w.objects.get(step.wallId),around=Math.min(...[-1,1].map(side=>{const along=(step.vertical?wall.y:wall.x)+side*(step.span+r+4),near={x:step.vertical?step.approach.x:along,y:step.vertical?along:step.approach.y},far={x:step.vertical?step.exit.x:along,y:step.vertical?along:step.exit.y};return distance(a,near)+distance(near,far)+distance(far,t)}));
  walkCost+=Math.max(0,around-d);cost+=distance(from,step.approach)+Math.max(distance(step.approach,step.exit),a.speed*1.3*2.5);from=step.exit;
 }
 if(!clear(from,t,t.id))return remember(null);cost+=distance(from,t);
 return remember({...walls[0],cost,walkCost});
};
const pathLengths=new WeakMap();
function cachedDistance(path,a,t){
 // Smoothed local routes normally have very few points. Never turn target
 // scoring into an unbounded traversal of a long or invalid cached route.
 if(!path?.length||path._navInvalid||path.length>128)return null;
 let length=pathLengths.get(path);
 if(length===undefined){length=0;for(let i=1;i<path.length;i++)length+=distance(path[i-1],path[i]);pathLengths.set(path,length)}
 return distance(a,path[0])+length+distance(path.at(-1),t);
}
function travelCost(g,a,t,d){
 const nav=g.navigation,r=a.radius||10,cell=nav.cell||28;
 const climb=g.tactics?.climbApproach(a,t),climbCost=climb?Math.max(d,climb.cost):Infinity;
 const key=[Math.round(a.x/cell),Math.round(a.y/cell),Math.round(t.x/cell),Math.round(t.y/cell),r,nav.revision,'ape'].join(':');
 const cached=nav.routes.get(key);
 if(cached&&!cached.path._navInvalid&&nav.time-cached.time<(cached.path.length?8:1.1)){
  if(!cached.path.length)return climbCost;
  const length=cachedDistance(cached.path,a,t);
  if(length!==null)return Math.min(climbCost,Math.max(d,length-(distance(a,t)-d)));
 }
 // This graph is already shared by movement, including bridge approaches.
 // Reading it does not enqueue A*, stream chunks or change an actor's route.
 const crossing=g.world.crossingFor?.(a,t,'ape',a._navAvoidCrossings,g.time);
 if(crossing){
  const side=t.y>=a.y?0:1,near=crossing.approaches[side],far=crossing.approaches[1-side];
  const onDeck=a.x>=crossing.minX&&a.x<=crossing.maxX&&a.y>=crossing.minY&&a.y<=crossing.maxY;
  return Math.min(climbCost,Math.max(d,(onDeck?distance(a,far):distance(a,near)+distance(near,far))+distance(far,t)-(distance(a,t)-d)));
 }
 return climb?Math.max(d,Math.min(climb.cost,climb.walkCost)):d;
}
P.nearestTargetFor=function(a,excludeStructures=false){
 const candidates=[];
 const available=t=>{const rejected=a._nearestRejected?.[t.id];return !rejected||rejected.revision!==(this.world.navRevision||0)||rejected.until<=this.time};
 const consider=(t,kind)=>{if(!t||t.hp<=0||t.dead||!inDirection(a,t)||!available(t))return;const d=kind==='object'?this.objectDistance(a,t):distance(a,t);if(d>RANGE)return;const i=candidates.findIndex(c=>d<c.d||d===c.d&&String(t.id)<String(c.t.id));if(i<0){if(candidates.length<6)candidates.push({t,kind,d})}else{candidates.splice(i,0,{t,kind,d});if(candidates.length>6)candidates.pop()}};
 // Keep the same bounded spatial scans, retaining a few alternatives instead
 // of only their first result. At most six candidates receive route scoring.
 for(const h of this.humanGrid.nearest(a.x,a.y,RANGE,4,h=>h.hp>0&&inDirection(a,h)&&available(h)))consider(h,'human');
 for(const v of this.vehicleGrid.nearest(a.x,a.y,RANGE,2,v=>v.hp>0&&inDirection(a,v)&&available(v)))consider(v,'vehicle');
 for(const o of this.world.getObjects(a.x,a.y,RANGE))if(hostileObject(o)&&(!excludeStructures||o.type==='vehicle'))consider(o,'object');
 let best=null,nearest=Infinity;
 for(const {t,kind,d}of candidates){if(d>nearest)break;const cost=travelCost(this,a,t,d);if(cost<nearest||Number.isFinite(cost)&&cost===nearest&&String(t.id)<String(best?.id)){nearest=cost;best={id:t.id,kind}}}
 return best;
};
const command=P.command;
// Retain the existing command/save identifiers. 'human' now means mobile
// human forces (infantry and vehicles); only structural targets are excluded.
P.command=function(cmd,...args){
 if(!['nearestTarget','nearestHuman'].includes(cmd))return command.call(this,cmd,...args);
 if(this.ended||this.blastActive(this.king))return false;const feedback=this.commandCD<=0;
 const selected=!!this.siege?.selected.length,list=selected?this.siege.selectedMembers():this.apes.filter(a=>a.hp>0&&FOLLOW.has(a.state));if(!list.length)return false;
 for(const a of list){this.tactics.clearOrder(a);this.champions?.removeMember(a.id);delete a.throwWindup;a.state='charge';a.chargeTime=13;a.retreatUntil=0;a.nearestOrder={kind:cmd==='nearestHuman'?'human':'all',until:this.time+13};setDirection(a,a.nearestOrder,args[0]||this.aim);a.target={...a.nearestOrder.goal};a._nearestThink=0;delete a._nearestTarget;}
 this._nearestQueue=this.apes.filter(a=>a.hp>0&&a.nearestOrder).map(a=>a.id);this._nearestCursor=0;
 this.mode=cmd;this.commandCD=.5;if(feedback){this.king.attackTimer=.55;this.effect('wave',this.king.x,this.king.y,{color:cmd==='nearestHuman'?'#de947e':'#d3b378',life:.8,range:RANGE});this.sound('charge',.8);this.noise(this.king.x,this.king.y,900,'order')}return true;
};
const clear=ATSApeTactics.prototype.clearOrder;
ATSApeTactics.prototype.clearOrder=function(a){delete a.nearestOrder;delete a._nearestTarget;delete a._nearestThink;delete a._nearestRejected;return clear.call(this,a)};
// The normal navigation interaction can breach barricades automatically. These
// orders climb or navigate around barriers and damage their chosen target.
const interaction=ATSNavigation.prototype.interactFortification;
ATSNavigation.prototype.interactFortification=function(a,...args){if(a.nearestOrder)return null;return interaction.call(this,a,...args)};
const siegeTick=ATSSiege.prototype.tick;
ATSSiege.prototype.tick=function(dt){const g=this.g;g.planNearestOrders();return siegeTick.call(this,dt)};
P.planNearestOrders=function(){
 if(!this._nearestQueue)this._nearestQueue=this.apes.filter(a=>a.hp>0&&a.nearestOrder).map(a=>a.id);
 const queue=this._nearestQueue;let plans=0;
 for(let scanned=0;scanned<Math.min(64,queue.length)&&plans<12;scanned++){
  const index=(this._nearestCursor||0)%queue.length,a=this.apesById.get(queue[index]);this._nearestCursor=(index+1)%queue.length;
  if(!a||a.hp<=0||a.state!=='charge'||!a.nearestOrder||a.nearestOrder.until<=this.time||this.time<(a._nearestThink||0))continue;
  if(!this.performance.think('ape')){this._nearestCursor=index;break}
  if(!a.nearestOrder.goal)setDirection(a,a.nearestOrder,this.aim);
  if(!a.wallClimb&&!a.siegeTransition)a._nearestTarget=this.nearestTargetFor(a,a.nearestOrder.kind==='human');a._nearestThink=this.time+.2+(ATSUtil.hash(a.id)%11)*.008;plans++;
 }
};
const update=P.updateApe;
P.updateApe=function(a,dt){
 const order=a.nearestOrder;if(!order)return update.call(this,a,dt);
 if(a.state!=='charge'||!['human','all'].includes(order.kind)||!Number.isFinite(order.until)){this.tactics.clearOrder(a);return update.call(this,a,dt)}
 if(this.time>=order.until){this.tactics.clearOrder(a);a.state='hold';a.target={x:a.x,y:a.y};a.chargeTime=0;a.moving=false;return}
 if(!order.goal)setDirection(a,order,this.aim);
 a.hitTimer=Math.max(0,(a.hitTimer||0)-dt);a.attackTimer=Math.max(0,(a.attackTimer||0)-dt);a.attackCD=Math.max(0,(a.attackCD||0)-dt);a.age+=dt;a.chargeTime=Math.max(0,order.until-this.time);a.moving=false;if(this.updateBlastReaction(a,dt))return;
 if(a.siegeTransition){this.siege.transition(a,dt);return}
 if(this.tactics.updateClimb(a,dt))return;
 let ref=a._nearestTarget,t=this.tactics.resolve(ref);
 const climb=t&&this.tactics.climbApproach(a,t),unreachable=t&&this.navigation.unreachable?.(a,t);
 if(unreachable&&!climb){
  (a._nearestRejected||(a._nearestRejected={}))[t.id]={until:this.time+8,revision:this.world.navRevision||0};
  a._nearestTarget=null;a._nearestThink=0;delete a.throwWindup;this.navigation.resetActor?.(a);ref=null;t=null;
 }
 if(!t||t.hp<=0||t.dead||!inDirection(a,t)||order.kind==='human'&&!troopTarget(t,ref.kind)||ref.kind==='object'&&!hostileObject(t)||(ref.kind==='object'?this.objectDistance(a,t):distance(a,t))>RANGE){
  a._nearestTarget=null;delete a.throwWindup;a.target={...order.goal};const d=distance(a,order.goal);
  if(d>8)this.move(a,order.goal.x-a.x,order.goal.y-a.y,Math.min(a.speed*1.3,d/Math.max(dt,.001)),dt);
  return;
 }
 this.siege?.releaseThrow(a);a.target={x:t.x,y:t.y};if(this.siege?.attack(a,t,ref.kind,dt))return;
 // Prefer a validated climb when walking needs a long detour, or a ground
 // search has failed. Walk to its near-side approach before starting ascent.
 if(climb&&(unreachable||climb.cost<climb.walkCost)){
  const wall=this.world.objects.get(climb.wallId);
  if(this.objectDistance(a,wall)<=29&&Math.abs((climb.vertical?a.y:a.x)-(climb.vertical?climb.approach.y:climb.approach.x))<4){if(this.tactics.tryClimb(a,climb.exit.x-a.x,climb.exit.y-a.y,wall))return}
  const gap=distance(a,climb.approach);if(gap>2)this.move(a,climb.approach.x-a.x,climb.approach.y-a.y,Math.min(a.speed*1.3,gap/Math.max(dt,.001)),dt);return;
 }
 // Preserve shared terrain, champion and weather movement modifiers.
 this.move(a,t.x-a.x,t.y-a.y,a.speed*1.3,dt);
};
const damage=P.damageObject;
P.damageObject=function(o,n,a){if(a?.nearestOrder?.kind==='human'&&o.type!=='vehicle')return false;return damage.call(this,o,n,a)};
const hurt=P.hurt;
P.hurt=function(a,n,source){if(source?.nearestOrder?.kind==='human'&&!a.id?.startsWith('human')&&!a.id?.startsWith('vehicle'))return false;return hurt.call(this,a,n,source)};
})();
