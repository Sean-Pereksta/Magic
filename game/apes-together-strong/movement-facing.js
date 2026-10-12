/* Render direction follows committed translation, not navigation's next wish.
 * Simulation dir remains authoritative for shields, hit arcs and combat. */
(() => {
'use strict';
const P=ATSGame.prototype,wrap=n=>Math.atan2(Math.sin(n),Math.cos(n));
P.updateVisualFacing=function(a,x,y,dt){
 if(!a.species&&a.id!=='king')return;
 if(a.siegeTransition||a.wallClimb||a.palisadeClimb){a.visualDir=a.dir;return}
 if(a.attackTimer>0){a.visualDir=a.dir;return}
 const dx=a.x-x,dy=a.y-y,length=Math.hypot(dx,dy);if(length<.25)return;
 const wanted=Math.atan2(dy,dx);
 if(!Number.isFinite(a.visualDir)){a.visualDir=wanted;return}
 const delta=wrap(wanted-a.visualDir);
 if(Math.abs(delta)<.16)return;
 // Tiny backwards avoidance impulses must persist before reversing the body.
 if(Math.abs(delta)>2.2&&length<1.3){if(!a._facingReverseAt)a._facingReverseAt=this.time;if(this.time-a._facingReverseAt<.14)return}else delete a._facingReverseAt;
 a.visualDir=wrap(a.visualDir+Math.sign(delta)*Math.min(Math.abs(delta),Math.max(.12,dt*10)));
};
const move=P.move;P.move=function(a,dx,dy,speed,dt,...args){const x=a.x,y=a.y,result=move.call(this,a,dx,dy,speed,dt,...args);this.updateVisualFacing(a,x,y,dt);return result};
const abstract=P.abstractActor;P.abstractActor=function(a,dt,kind){const x=a.x,y=a.y,result=abstract.call(this,a,dt,kind);if(kind==='ape')this.updateVisualFacing(a,x,y,dt);return result};
})();
