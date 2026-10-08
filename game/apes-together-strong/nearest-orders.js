/* Tap/hold attack orders: exact local distance, with a strict human-only mode. */
(() => {
'use strict';
const FOLLOW=new Set(['follow','charge','hold']),RANGE=320;
const HOSTILE=new Set(['wall','gate','cage','gateControl','tower','radio','alarm','barracks','depot','fuel','fuelDepot','house','vehicle','generator','prisonControl','powerRelay','gatlingNest','mortarNest','humanBarricade','heavyBarricade','fieldBarricade','fieldSearchlight','fieldObservation','observationPost']);
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),P=ATSGame.prototype;
const hostileObject=o=>!o.dead&&o.hp>0&&o.team!=='ape'&&o.faction!=='ape'&&o.owner!=='ape'&&!o.settlementId&&HOSTILE.has(o.type)&&(o.type!=='cage'||(o.count??o.prisoners??0)>0)&&(o.type!=='gate'||o.solid);
P.nearestTargetFor=function(a,humansOnly=false){
 let best=null,nearest=RANGE;
 const consider=(t,kind)=>{if(!t||t.hp<=0||t.dead)return;const d=kind==='object'?this.objectDistance(a,t):distance(a,t);if(d>RANGE)return;if(!best||d<nearest||(d===nearest&&String(t.id)<String(best.id))){nearest=d;best={id:t.id,kind}}};
 for(const h of this.humanGrid.nearest(a.x,a.y,RANGE,1,h=>h.hp>0))consider(h,'human');
 if(!humansOnly){for(const v of this.vehicleGrid.nearest(a.x,a.y,RANGE,1,v=>v.hp>0))consider(v,'vehicle');for(const o of this.world.getObjects(a.x,a.y,RANGE))if(hostileObject(o))consider(o,'object')}
 return best;
};
const command=P.command;
P.command=function(cmd,...args){
 if(!['nearestTarget','nearestHuman'].includes(cmd))return command.call(this,cmd,...args);
 if(this.ended||this.blastActive(this.king))return false;const feedback=this.commandCD<=0;
 const selected=!!this.siege?.selected.length,list=selected?this.siege.selectedMembers():this.apes.filter(a=>a.hp>0&&FOLLOW.has(a.state));if(!list.length)return false;
 for(const a of list){this.tactics.clearOrder(a);this.champions?.removeMember(a.id);delete a.throwWindup;a.state='charge';a.chargeTime=13;a.retreatUntil=0;a.target={x:a.x,y:a.y};a.nearestOrder={kind:cmd==='nearestHuman'?'human':'all',until:this.time+13};a._nearestThink=0;delete a._nearestTarget;}
 this._nearestQueue=this.apes.filter(a=>a.hp>0&&a.nearestOrder).map(a=>a.id);this._nearestCursor=0;
 this.mode=cmd;this.commandCD=.5;if(feedback){this.king.attackTimer=.55;this.effect('wave',this.king.x,this.king.y,{color:cmd==='nearestHuman'?'#de947e':'#d3b378',life:.8,range:320});this.sound('charge',.8);this.noise(this.king.x,this.king.y,900,'order')}return true;
};
const clear=ATSApeTactics.prototype.clearOrder;
ATSApeTactics.prototype.clearOrder=function(a){delete a.nearestOrder;delete a._nearestTarget;delete a._nearestThink;return clear.call(this,a)};
// The normal navigation interaction can breach barricades automatically. These
// orders navigate around barriers and damage only their explicitly chosen target.
const interaction=ATSNavigation.prototype.interactFortification;
ATSNavigation.prototype.interactFortification=function(a,...args){if(a.nearestOrder)return null;return interaction.call(this,a,...args)};
const climb=ATSApeTactics.prototype.tryClimb;
ATSApeTactics.prototype.tryClimb=function(a,...args){if(a.nearestOrder)return false;return climb.call(this,a,...args)};
const siegeTick=ATSSiege.prototype.tick;
ATSSiege.prototype.tick=function(dt){const g=this.g;g.planNearestOrders();return siegeTick.call(this,dt)};
P.planNearestOrders=function(){
 if(!this._nearestQueue)this._nearestQueue=this.apes.filter(a=>a.hp>0&&a.nearestOrder).map(a=>a.id);
 const queue=this._nearestQueue;let plans=0;
 for(let scanned=0;scanned<Math.min(64,queue.length)&&plans<12;scanned++){
  const index=(this._nearestCursor||0)%queue.length,a=this.apesById.get(queue[index]);this._nearestCursor=(index+1)%queue.length;
  if(!a||a.hp<=0||a.state!=='charge'||!a.nearestOrder||a.nearestOrder.until<=this.time||this.time<(a._nearestThink||0))continue;
  if(!this.performance.think('ape')){this._nearestCursor=index;break}
  a._nearestTarget=this.nearestTargetFor(a,a.nearestOrder.kind==='human');a._nearestThink=this.time+.2+(ATSUtil.hash(a.id)%11)*.008;plans++;
 }
};
const update=P.updateApe;
P.updateApe=function(a,dt){
 const order=a.nearestOrder;if(!order)return update.call(this,a,dt);
 if(a.state!=='charge'||!['human','all'].includes(order.kind)||!Number.isFinite(order.until)){this.tactics.clearOrder(a);return update.call(this,a,dt)}
 if(this.time>=order.until){this.tactics.clearOrder(a);a.state='hold';a.target={x:a.x,y:a.y};a.chargeTime=0;a.moving=false;return}
 a.hitTimer=Math.max(0,(a.hitTimer||0)-dt);a.attackTimer=Math.max(0,(a.attackTimer||0)-dt);a.attackCD=Math.max(0,(a.attackCD||0)-dt);a.age+=dt;a.chargeTime=Math.max(0,order.until-this.time);a.moving=false;if(this.updateBlastReaction(a,dt))return;
 const ref=a._nearestTarget,t=this.tactics.resolve(ref);
 if(!t||t.hp<=0||t.dead||order.kind==='human'&&ref.kind!=='human'||ref.kind==='object'&&!hostileObject(t)||(ref.kind==='object'?this.objectDistance(a,t):distance(a,t))>RANGE){a._nearestTarget=null;delete a.throwWindup;a.target={x:a.x,y:a.y};return}
 this.siege?.releaseThrow(a);a.target={x:t.x,y:t.y};if(this.siege?.attack(a,t,ref.kind,dt))return;
 // Keep the shared terrain, champion and weather movement modifiers while
 // suppressing automatic climbing/breaching through the hooks above.
 this.move(a,t.x-a.x,t.y-a.y,a.speed*1.3,dt);
};
const damage=P.damageObject;
P.damageObject=function(o,n,a){if(a?.nearestOrder?.kind==='human')return false;return damage.call(this,o,n,a)};
const hurt=P.hurt;
P.hurt=function(a,n,source){if(source?.nearestOrder?.kind==='human'&&!a.id?.startsWith('human'))return false;return hurt.call(this,a,n,source)};
})();
