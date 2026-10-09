/* Friendly palisades have a short, physical climb rather than an invisible gate. */
(() => {
'use strict';
const N=ATSNavigation.prototype,G=ATSGame.prototype,clamp=(x,a,b)=>Math.max(a,Math.min(b,x));
const friendly=b=>!!b?.fortification&&!b.gate&&!b.settlementWall&&(b.team||b.owner)==='ape'&&b.solid&&!b.dead&&b.hp>0;
const now=nav=>nav.game?.time??nav.time;
const radius=a=>a.radius||(a.id==='king'?12:a.state==='young'?7:10);
const pose=a=>{
 const c=a.palisadeClimb;if(!c)return null;
 const t=clamp(c.elapsed,0,.72);
 if(t<.4){const p=t/.4;return{phase:'climb',progress:p,height:c.height*Math.sin(p*Math.PI/2),x:c.fromX+(c.crestX-c.fromX)*p,y:c.fromY+(c.crestY-c.fromY)*p}}
 if(t<.46)return{phase:'crest',progress:(t-.4)/.06,height:c.height,x:c.crestX,y:c.crestY};
 const p=clamp((t-.46)/.26,0,1);return{phase:'jump',progress:p,height:c.height*(1-p)+Math.sin(p*Math.PI)*8,x:c.crestX+(c.exitX-c.crestX)*p,y:c.crestY+(c.exitY-c.crestY)*p};
};
window.ATSPalisadePose=pose;
N.clearPalisadeClimb=function(a){delete a.palisadeClimb;delete a._palisadeStepAt;this._palisadeClimbers?.delete(a)};
N.validPalisadeClimb=function(a){
 const c=a.palisadeClimb,b=this.world.objects.get(c?.wallId);if(!c||!friendly(b)||a.hp<=0)return false;
 if(!['fromX','fromY','crestX','crestY','exitX','exitY','elapsed','height'].every(k=>Number.isFinite(c[k]))||c.elapsed<0||c.elapsed>.73||c.height<12||c.height>45)return false;
 const vertical=(b.w||24)<(b.h||24),normal=vertical?'X':'Y',along=vertical?'Y':'X',center=vertical?b.x:b.y,span=vertical?b.y:b.x,half=(vertical?b.h:b.w)/2;
 return Math.abs(c['crest'+normal]-center)<.01&&Math.abs(c['crest'+along]-span)<=half+radius(a)&&Math.abs(c['from'+along]-c['crest'+along])<.01&&Math.abs(c['exit'+along]-c['crest'+along])<.01&&Math.hypot(c.exitX-c.fromX,c.exitY-c.fromY)<130&&Math.hypot(a.x-c.crestX,a.y-c.crestY)<100&&Math.abs(c['from'+normal]-center)<65&&Math.abs(c['exit'+normal]-center)<65&&(c['from'+normal]-center)*(c['exit'+normal]-center)<=0;
};
N.restorePalisadeClimb=function(a){
 if(!a.palisadeClimb)return;
 if(!this.validPalisadeClimb(a)){this.clearPalisadeClimb(a);return}
 const c=a.palisadeClimb,r=radius(a);
 if(!this.clearSegment(a.x,a.y,c.exitX,c.exitY,r,'ape',c.wallId)){this.clearPalisadeClimb(a);return}
 (this._palisadeClimbers||(this._palisadeClimbers=new Set())).add(a);delete a._palisadeStepAt;
};
N.stepPalisadeClimb=function(a,dt){
 if(!a.palisadeClimb)return false;
 if(a._palisadeStepAt===now(this))return true;
 a._palisadeStepAt=now(this);
 if(!this.validPalisadeClimb(a)||a.blastReaction||a.siegeTransition||a.wallClimb||a.onWallId){this.clearPalisadeClimb(a);return false}
 const c=a.palisadeClimb,previous=c.elapsed;c.elapsed=Math.min(.72,c.elapsed+Math.max(0,Math.min(dt,.1)));const p=pose(a),r=radius(a);
 // Even an interrupted jump may never cross a newly built hut, a tree or water.
 if(!this.clearSegment(a.x,a.y,p.x,p.y,r,'ape',c.wallId)){c.elapsed=previous;this.clearPalisadeClimb(a);a.moving=false;return true}
 a.x=p.x;a.y=p.y;a.dir=Math.atan2(c.exitY-c.fromY,c.exitX-c.fromX);a.moving=true;
 if(c.elapsed>=.72){this.clearPalisadeClimb(a);a._palisadeLandedAt=now(this);a._nav=null}
 return true;
};
N.startPalisadeClimb=function(a,dx,dy,speed,dt){
 if(this.hasFortifications===false||!this.world._queryCollision||a.hp<=0||a.siegeTransition||a.wallClimb||a.onWallId||a.blastReaction||now(this)<(a._palisadeRetryAt||0))return false;
 const d=Math.hypot(dx,dy);if(d<1.5)return false;
 const r=radius(a),travel=Math.min(d,Math.max(3,speed*dt)),ux=dx/d,uy=dy/d,tx=a.x+ux*travel,ty=a.y+uy*travel;
 let best=null,score=Infinity;
 this.world._queryCollision(Math.min(a.x,tx)-r-2,Math.min(a.y,ty)-r-2,Math.max(a.x,tx)+r+2,Math.max(a.y,ty)+r+2,b=>{
  if(!friendly(b)||b.collision!=='rect')return;
  const vertical=(b.w||24)<(b.h||24),normal=vertical?ux:uy,offset=vertical?a.x-b.x:a.y-b.y,along=vertical?a.y:a.x,center=vertical?b.y:b.x,half=(vertical?b.w:b.h)/2,span=(vertical?b.h:b.w)/2;
  if(Math.abs(normal)<.25||offset*normal>=0||Math.abs(along-center)>span+r*.25)return;
  const contact=(Math.abs(offset)-half-r)/Math.abs(normal);if(contact>travel+2||contact>=score)return;
  const sign=normal>0?1:-1,crestX=vertical?b.x:a.x,crestY=vertical?a.y:b.y,exitX=vertical?b.x+sign*(half+r+3):a.x,exitY=vertical?a.y:b.y+sign*(half+r+3);
  if(Math.hypot(exitX-a.x,exitY-a.y)>125)return;
  best={wallId:b.id,fromX:a.x,fromY:a.y,crestX,crestY,exitX,exitY,elapsed:0,height:clamp(b.visualHeight||Math.max(38,b.height||0),20,42)};score=contact;
 });
 if(!best)return false;
 if(!this.clearSegment(a.x,a.y,best.exitX,best.exitY,r,'ape',best.wallId)){a._palisadeRetryAt=now(this)+.15;return false}
 a.palisadeClimb=best;a._palisadeStepAt=now(this);a._nav=null;a.barrierAction=null;a._barrierCrossing=null;a._barrierIgnore=null;a.moving=true;a.dir=Math.atan2(best.exitY-best.fromY,best.exitX-best.fromX);
 (this._palisadeClimbers||(this._palisadeClimbers=new Set())).add(a);return true;
};
const interact=N.interactFortification;
N.interactFortification=function(a,target,r,dt){
 if(this.profile(a)!=='ape'||this.hasFortifications===false)return interact.call(this,a,target,r,dt);
 // The local sweep in move owns friendly climbing; preserve enemy breaching.
 const dx=target.x-a.x,dy=target.y-a.y,d=Math.hypot(dx,dy),step=Math.min(d,r+22),b=d&&this.world.fortificationAt?.(a.x+dx/d*step,a.y+dy/d*step,r);
 if(friendly(b)){a.barrierAction=null;a._barrierCrossing=null;a._barrierIgnore=null;return null}
 return interact.call(this,a,target,r,dt);
};
const move=N.move;
N.move=function(a,dx,dy,speed,dt,controlled=false){
 if(this.profile(a)!=='ape')return move.call(this,a,dx,dy,speed,dt,controlled);
 if(a.palisadeClimb){this.stepPalisadeClimb(a,dt);return}
 if(a._palisadeLandedAt===now(this))return;
 if(this.startPalisadeClimb(a,dx,dy,speed,dt))return;
 return move.call(this,a,dx,dy,speed,dt,controlled);
};
const update=G.update;
G.update=function(dt,input){
 const result=update.call(this,dt,input),nav=this.navigation;
 // Only actors in a finite crossing are visited. A released key or changed order
 // cannot leave an ape hanging, and large distant populations add no scan.
 for(const a of nav._palisadeClimbers||[]){if(this.ended||a.hp<=0)nav.clearPalisadeClimb(a);else nav.stepPalisadeClimb(a,dt)}
 return result;
};
const hurt=G.hurt;
G.hurt=function(a,damage,source){const result=hurt.call(this,a,damage,source);if(a.hp<=0||a.blastReaction)this.navigation.clearPalisadeClimb(a);return result};
const fromJSON=ATSGame.fromJSON;
ATSGame.fromJSON=function(data,hooks){const g=fromJSON.call(this,data,hooks);for(const a of[g.king,...g.apes])g.navigation.restorePalisadeClimb(a);return g};
})();
