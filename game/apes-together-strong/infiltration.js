/* Shared orders and bounded perception. Distractions carry a noise position,
 * never the location or identity of an unseen King or infiltrating squad. */
(() => {
'use strict';
const G=ATSGame.prototype,S=ATSSiege.prototype;
const LIGHT=new Set(['chimpanzee','gibbon','capuchin']),DEVICES=new Set(['gateControl','prisonControl','generator','radio','alarm']);
const STEALTH={gibbon:.55,capuchin:.58,chimpanzee:.73,mandrill:.9,orangutan:1.12,gorilla:1.25};
const dist=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),point=a=>({x:a.x,y:a.y});
G.setInfiltration=function(enabled=true,response='continue',ghost=false){
 if(!['continue','fight','retreat'].includes(response)||ghost&&this.tribe.id!=='shadowhand')return false;
 const members=this.siege.selectedMembers().filter(a=>LIGHT.has(a.species));
 if(!members.length){this.siege.status='Select chimpanzees, gibbons or capuchins';return false}
 for(const a of members){if(enabled)a.infiltration={response,ghost,safe:point(a)};else delete a.infiltration}
 this.siege.status=enabled?(ghost?'Ghost Entry':'Infiltrate')+' · on discovery: '+response:'Infiltration off';return true;
};
const issue=S.issue;S.issue=function(species,orders,append=false){
 const keep=new Map(this.selectedMembers().filter(a=>a.infiltration).map(a=>[a.id,a.infiltration]));
 const ok=issue.call(this,species,orders,append);
 if(ok&&!orders.some(o=>['charge','shield','retreat','follow','assault','attackArea'].includes(o.type)))for(const [id,stance]of keep){const a=this.g.apesById.get(id);if(a?.armyOrder)a.infiltration=stance}
 if(ok&&this.g.tribe.id==='ironback'&&orders.some(o=>o.type==='shield'))for(const a of this.selectedMembers())if(a.armyOrder)a.ironbackShieldwall=true;
 return ok;
};
// Species/behavior differences affect visual detection before the expensive ray.
// The existing light cones, LOS, weather, radio and last-known-position systems
// still decide whether a guard can actually see an eligible target.
G.stealthFactor=function(a){
 let n=STEALTH[a.species]||1;
 if(a.infiltration)n*=a.moving?.84:.7;

 if(a.elite?.type==='ghost')n*=.8;
 if(a.attackTimer>0||this.time-(a.lastAttackAt??-100)<2)n=Math.max(n,1.35);
 if(a.equipment?.torch)n=Math.max(n,1.6);
 if(a.shield?.hp>0||a.equipment?.armorLevel)n*=1.15;
 if(this.time>=(a._stealthAt||0)){
  a._stealthAt=this.time+.6;
  a._stealthCrowd=this.apeGrid.nearest(a.x,a.y,75,9,p=>p.hp>0&&p!==a).length;
  a._stealthCover=this.world.concealment?.(a.x,a.y)||0;
 }
 n*=1+Math.min(.35,(a._stealthCrowd||0)*.045);
 if(!a.equipment?.torch)n*=1-Math.min(.28,(a._stealthCover||0)*.28);
 return Math.max(.28,Math.min(1.8,n));
};
const visible=G.lineVisible;G.lineVisible=function(observer,target,critical=false){
 if(!critical&&observer.id?.startsWith('human')&&target.species){
  const radius=(this.forces.range(observer)||340)*(this.weatherInfo?.().sight||1),alert=observer.state==='combat'||observer.suspicion>=1;
  if(dist(observer,target)>Math.max(38,radius*this.stealthFactor(target)*(alert?1.3:1)))return false;
 }
 return visible.call(this,observer,target,critical);
};
const attack=G.animateAttack;G.animateAttack=function(a,...args){a.lastAttackAt=this.time;return attack.call(this,a,...args)};
const report=S.report;S.report=function(observer,target,...args){if(target.infiltration){target.infiltration.spottedUntil=this.g.time+5;target.infiltration.lastThreat=point(observer)}return report.call(this,observer,target,...args)};
G.distractionRadius=function(a){return (this.tribe.id==='shadowhand'&&this.tribe.unlocks.includes('decoys')?330:240)*(a?.elite?.type==='slinger'?1.15:1)*(this.weatherInfo?.().sound||1)};
G.distract=function(p){
 if(!p||!Number.isFinite(p.x)||!Number.isFinite(p.y)||!this.siege.known({...p,type:'ground'}))return false;
 const a=this.siege.selectedMembers().filter(a=>LIGHT.has(a.species)&&this.time>=(a.distractionReady||0)).sort((a,b)=>dist(a,p)-dist(b,p))[0];
 if(!a){this.siege.status='Select a light ape with a ready distraction';return false}
 if(dist(a,p)>700||this.navigation.blocked(p.x,p.y,9,'ape')){this.siege.status='Choose reachable ground within 700 paces';return false}
 const selected=this.siege.unitIds;this.siege.unitIds=[a.id];
 const ok=this.siege.issue([a.species],[{id:'decoy-'+ ++this.siege.serial,type:'move',...point(p)}]);this.siege.unitIds=selected;
 if(ok){a.distraction={...point(p)};a.infiltration=a.infiltration||{response:'retreat',safe:point(a)};this.siege.status='Approaching distraction location'}return ok;
};
G.emitDistraction=function(a,p){
 const radius=this.distractionRadius(a),prior=this._decoyHistory;
 const repeated=prior&&this.time-prior.at<30&&dist(prior,p)<radius;
 this._decoyHistory={...point(p),at:this.time};
 const guards=this.humanGrid.nearest(p.x,p.y,radius,24,h=>h.hp>0&&!['combat','radio','alarm'].includes(h.state));
 // Never pull the entire garrison. Fixed wall posts stay at their posts.
 const max=Math.min(this.tribe.unlocks.includes('decoys')?4:2,Math.ceil(guards.length/3));let count=0;
 for(const h of guards){if(count>=max)break;if(h.onWallId||h.siegeTransition||h.prisonResponse)continue;
  h.state='investigate';h.lastX=p.x;h.lastY=p.y;h.searchTime=repeated?18:10;h.suspicion=Math.max(h.suspicion||0,repeated?.7:.35);
  h.decoyUntil=this.time+h.searchTime;h.targetId=null;h._sense=null;count++;
 }
 if(repeated)for(const h of guards)h.suspicion=Math.max(h.suspicion||0,.45);
 a.distractionReady=this.time+(a.elite?.type==='slinger'?9:14);delete a.distraction;
 this.effect('wave',p.x,p.y,{range:radius,life:.8,color:'#8fbaa0'});this.sound('hit',.3,p.x);this.siege.status=count+' guards investigating';
};
G.silentRelay=function(){
 if(this.tribe.id!=='shadowhand')return false;const list=this.siege.selectedMembers().filter(a=>a.species==='chimpanzee'&&this.time>=(a.relayReady||0));
 if(!list.length){this.siege.status='Select a chimpanzee saboteur';return false}
 for(const a of list){a.silentRelay=true;a.infiltration=a.infiltration||{response:'retreat',safe:point(a)}}
 this.siege.status='Silent Relay Cut · select a discovered radio or alarm';return true;
};
const update=S.updateApe;S.updateApe=function(a,dt){
 const g=this.g,stance=a.infiltration;
 if(a.siegeTransition)return update.call(this,a,dt);
 if(a.distraction){
  if(!a.armyOrder||a.hp<=0){delete a.distraction}else if(dist(a,a.distraction)<=105){g.emitDistraction(a,a.distraction);this.advance(a);return true}
 }
 if(stance){
  const spotted=stance.spottedUntil>g.time;
  if(!spotted&&g.time>=(a._safePointAt||0)){stance.safe=point(a);a._safePointAt=g.time+1}
  if(spotted&&stance.response==='retreat'){
   const safe=stance.safe;if(dist(a,safe)>18){
    const o={id:'safe-'+a.id,type:'move',...safe};if(a.armyOrder)this.followRoute(a,o,dt);else g.move(a,safe.x-a.x,safe.y-a.y,a.speed,dt);
   }a.routeStatus='Retreating to safety';return true;
  }
  if(spotted&&stance.response==='fight'){
   const h=g.humanGrid.nearest(a.x,a.y,80,1,h=>h.hp>0)[0];if(h){if(!this.attack(a,h,'human',dt))g.move(a,h.x-a.x,h.y-a.y,a.speed,dt);return true}
  }
  const group=a.armyOrder&&this.groups[a.species],order=group?.orders[a.armyOrder.index],target=order?.targetId&&this.resolve(order);
  if(target&&DEVICES.has(target.type)&&target.hp>0&&this.known(target)&&g.objectDistance(a,target)<38){
   if(!this.clearRay(a,target,{ignore:target.id}))return update.call(this,a,dt);
   const silent=a.silentRelay&&['radio','alarm'].includes(target.type)&&g.tribe.id==='shadowhand';
   if(a.sabotageTarget!==target.id)a.sabotageProgress=0;
   a.sabotageTarget=target.id;a.sabotageProgress=(a.sabotageProgress||0)+dt*(g.tribe.id==='shadowhand'?1.2:1)*(a.species==='chimpanzee'?1.25:1)*(a.elite?.type==='shadowbreaker'?1.3:1);
   a.dir=Math.atan2(target.y-a.y,target.x-a.x);a.moving=false;a.routeStatus=silent?'Cutting Silent Relay':target.type==='gateControl'?'Opening Gate':'Sabotaging';
   if(a.sabotageProgress>=3){
    delete a.sabotageProgress;delete a.sabotageTarget;
    if(silent){const s=g.world.sites.get(target.siteId);target.disabledUntil=g.time+30;if(s){const key=target.type==='radio'?'radioDown':'alarmDown';if(target.relayPrior===undefined)target.relayPrior=!!s[key];s[key]=true}(g._silentRelays||(g._silentRelays=new Set())).add(target.id);a.relayReady=g.time+40;delete a.silentRelay}
    else if(target.type==='gateControl'){target.controlDisabled=true;this.openGate(g.world.objects.get(target.gateId))}
    else g.damageObject(target,target.hp+1,a);
    this.advance(a);
   }return true;
  }
  if(target?.id!==a.sabotageTarget){delete a.sabotageTarget;delete a.sabotageProgress}
  if(!a.armyOrder){a.moving=false;return true}
 }
 const result=update.call(this,a,dt);
 if(a.armyOrder){const route=a.siegeRoute,step=route?.steps[route.index];a.routeStatus=a.siegeTransition?'Climbing':this.groups[a.species]?.blocked?'Route Blocked':step?.wallId||route?.steps.some((p,i)=>i>=route.index&&p.wallId)?'Approaching Wall':route?.steps.length>1?'Inside Fortress':'Moving';}
 return result;
};
const move=G.move;G.move=function(a,dx,dy,speed,dt,...args){return move.call(this,a,dx,dy,a.infiltration?speed*.72:speed,dt,...args)};
const capuchin=S.capuchin;S.capuchin=function(a,dt){return a.infiltration?false:capuchin.call(this,a,dt)};
const combat=ATSApeTactics.prototype.combat;ATSApeTactics.prototype.combat=function(a,...args){return a.infiltration?false:combat.call(this,a,...args)};
// Silent outages are refreshed once a second and restored independently.
const tick=S.tick;S.tick=function(dt){
 if(this.g.time>=(this.nextRelayTick||0)){
  this.nextRelayTick=this.g.time+1;const g=this.g;
  for(const id of g._silentRelays||[]){const o=g.world.objects.get(id);if(!o){g._silentRelays.delete(id);continue}if(o.disabledUntil<=g.time){const s=g.world.sites.get(o.siteId);if(s)s[o.type==='radio'?'radioDown':'alarmDown']=!!(o.dead||o.relayPrior);delete o.disabledUntil;delete o.relayPrior;g._silentRelays.delete(id)}}
 }
 return tick.call(this,dt);
};
const restore=ATSGame.fromJSON;ATSGame.fromJSON=function(data,hooks){const g=restore.call(this,data,hooks);g._silentRelays=new Set([...g.world.objects.values()].filter(o=>o.disabledUntil).map(o=>o.id));return g};
const perceive=G.perceive;G.perceive=function(h,light){const result=perceive.call(this,h,light),a=result.seen;if(a?.infiltration&&a.moving&&this.tribe.id==='shadowhand')result.suspicionGain=.24;return result};
G.ghostEntry=function(response='continue'){
 if(!this.setInfiltration(true,response,true))return false;const center=this.siege.center();
 const target=this.world.getObjects(center.x,center.y,600).filter(o=>DEVICES.has(o.type)&&o.hp>0&&!o.dead&&!o.controlDisabled&&!o.disabledUntil&&this.siege.known(o)).sort((a,b)=>dist(a,center)-dist(b,center))[0];
 if(target){const species=this.siege.selected.filter(s=>LIGHT.has(s));this.siege.issue(species,[{id:'ghost-'+ ++this.siege.serial,type:'sabotage',targetId:target.id,targetKind:'object',...point(target)}])}return true;
};
// Human wall posts and prison duties must not override an active investigation.
const human=S.updateHuman;S.updateHuman=function(h,dt){if(h.decoyUntil>this.g.time&&!h.onWallId&&!h.siegeTransition)return false;return human.call(this,h,dt)};
window.ATSStealthProfiles=Object.freeze(STEALTH);
})();
