/* Ownership, settlement mobilization, and one-command tap/hold gestures. */
(() => {
'use strict';
const OWNER='king',NEAR=260,RECRUIT=340,HOLD_MS=600;
const OWNED_STATES=new Set(['follow','charge','hold','settled','scout','young']);
const P=ATSGame.prototype;
function inferOwner(a){if(a.hordeOwner===undefined)a.hordeOwner=OWNED_STATES.has(a.state)||a.settlementId?OWNER:null;return a.hordeOwner===OWNER}
P.ownsApe=function(a){return !!a&&a.id!=='king'&&a.hp>0&&inferOwner(a)};
P.isSettledApe=function(a){return !!a.settlementId};
const make=P.makeApe;
P.makeApe=function(...args){const a=make.apply(this,args);if(a){inferOwner(a);const grid=this.apeGrid,key=Math.floor(a.x/grid.size)+','+Math.floor(a.y/grid.size),cell=grid.cells.get(key);if(cell)cell.push(a);else grid.cells.set(key,[a])}return a};
const restore=ATSGame.fromJSON;
ATSGame.fromJSON=function(...args){const g=restore.apply(this,args);for(const a of g.apes)inferOwner(a);return g};
const clear=ATSApeTactics.prototype.clearOrder;
ATSApeTactics.prototype.clearOrder=function(a){delete a.recallOrder;a.retreatUntil=0;this.g.navigation.resetActor?.(a);return clear.call(this,a)};
P.suspendSettlementAssignment=function(a){
 if(!a.settlementId)return;
 const s=this.settlement(a.settlementId);
 a.previousSettlementAssignment={settlementId:a.settlementId,state:a.state,homeX:a.homeX,homeY:a.homeY,job:a.job,workCohort:a.workCohort,towerId:a.towerId,trainingFacilityId:a.trainingFacilityId,kingdomMission:a.kingdomMission?{...a.kingdomMission}:null,suspendedAt:this.time};
 a.settlementId=null;
 for(const key of ['job','workCohort','workTargetId','towerId','trainingFacilityId','kingdomMission','_activityTarget','_activityAt','activity','carrying'])delete a[key];
 if(s){s._jobsAt=0;s._staffAt=0;s._memberCount=-1;for(const f of s.facilities||[]){if(f.staffedBy===a.id)f.staffedBy=null;if(f.trainingResidents)f.trainingResidents=f.trainingResidents.filter(id=>id!==a.id)}}
};
P.receiveHordeRecall=function(a,kind,serial){
 this.tactics?.clearOrder(a);this.champions?.removeMember(a.id);delete a.throwWindup;
 if(kind==='all')this.suspendSettlementAssignment(a);
 // Children travel without changing their age, health, or settlement growth rules.
 if(a.state!=='young')a.state='follow';
 a.target=null;a.retreatUntil=this.time+5;a.recallOrder={id:serial,kind,issuedAt:this.time};a.commandResponseUntil=this.time+1.1;
 if(this.navigation.resetActor)this.navigation.resetActor(a);else for(const key of ['_nav','_navJourney','_abstractPath','_abstractRequest','navCohort'])delete a[key];
 a._lodDue=this.time+(ATSUtil.hash(a.id)%13)/60;
};
const command=P.command;
P.command=function(cmd,...args){
 if(!['call','recall','recallField','recallAll'].includes(cmd))return command.call(this,cmd,...args);
 if(this.ended||this.blastActive(this.king)||cmd==='call'&&this.commandCD>0)return false;
 const recruit=cmd==='call',kind=cmd==='recallAll'?'all':cmd==='recall'?'nearby':'field';
 // Q and nearby R read only neighboring spatial cells. Global orders scan once
 // here; the movement system shares and staggers subsequent route searches.
 const candidates=recruit||kind==='nearby'?this.apeGrid.near(this.king.x,this.king.y,recruit?RECRUIT:NEAR):this.apes;
 const serial=(this._hordeCommandSerial||0)+1;this._hordeCommandSerial=serial;let count=0,mobilized=0;
 for(const a of candidates){
  if(a.id==='king'||a.hp<=0)continue;
  if(recruit){
   if(inferOwner(a)||a.hordeOwner!=null||a.state!=='free'||a.settlementId||a.captive||a.prisoner||a.prisonEscort)continue;
   const cage=a.cageId&&this.world.objects.get(a.cageId);if(cage&&!cage.dead&&!cage.rescueOpened)continue;
   a.hordeOwner=OWNER;this.receiveHordeRecall(a,'recruit',serial);count++;
  }else if(this.ownsApe(a)&&(kind==='all'||!this.isSettledApe(a))){
   if(a.settlementId)mobilized++;this.receiveHordeRecall(a,kind,serial);count++;
  }
 }
 for(const division of Object.values(this._champions?.divisions||{}))if(!division.memberIds.some(id=>this.apesById.get(id)?.hp>0))division.suspended=true;
 if(mobilized){this.refreshSettlements();for(const s of this.settlements){s._members=(this.settlementMembers.get(s.id)||[]).slice();s._adults=s._members.filter(a=>a.state!=='young'&&a.state!=='scout');s.cohorts=[];s.builders=s.guards=s.lumberWorkers=s.gardeners=s.cooks=s.haulers=0}}
 this.mode='follow';this.commandCD=.5;this.king.attackTimer=.55;
 const color=recruit?'#e5c374':kind==='all'?'#f0bd78':kind==='field'?'#a8bbf0':'#75cabb',range=recruit?170:kind==='all'?410:kind==='field'?300:190;
 this.effect('wave',this.king.x,this.king.y,{color,life:kind==='all'?1.15:.8,range});
 this.noise(this.king.x,this.king.y,recruit?650:kind==='nearby'?500:750,'order');this.sound(recruit?'call':'recall',kind==='all'?1.15:.85);
 const message=recruit?count+' unrecruited apes answer the call.':kind==='nearby'?count+' nearby field apes recalled.':kind==='field'?count+' field apes recalled — settlements remain assigned.':count+' apes mobilized — settlements included ('+mobilized+' residents).';
 this.lastHordeCommand={cmd,kind:recruit?'recruit':kind,count,mobilized,time:this.time};this.notify(message,'gold','command');return true;
};
// A recalled child remains young while traveling; maturation keeps its field
// order. Settlement assignments never reattach themselves in the background.
const update=P.updateApe,abstract=P.abstractActor;
P.updateApe=function(a,dt){
 if(a.state!=='young'||!a.recallOrder||a.settlementId)return update.call(this,a,dt);
 a.age+=dt;
 if(a.age>=35&&!this.blastActive(a)){a.age-=dt;a.state='follow';a.hp=a.maxHp=120;a.speed=90;this.siege?.balance(a,true);return update.call(this,a,dt)}
 a.moving=false;a.attackTimer=0;a.hitTimer=Math.max(0,(a.hitTimer||0)-dt);a.attackCD=Math.max(0,(a.attackCD||0)-dt);if(this.updateBlastReaction(a,dt))return;
 const tx=this.king.x+(a.offsetX||0),ty=this.king.y+(a.offsetY||0),target=this.navigation.followTarget?.(a,this.king,tx,ty)||{x:tx,y:ty};if(a._navCrossing||Math.hypot(target.x-a.x,target.y-a.y)>30)this.move(a,target.x-a.x,target.y-a.y,a.speed,dt);
};
P.abstractActor=function(a,dt,kind){if(kind==='ape'&&a.state==='young'&&a.recallOrder&&!a.settlementId)return this.updateApe(a,dt);return abstract.call(this,a,dt,kind)};
// Wall-clock input is separate from paused simulation time. Releases before the
// threshold tap; a hold commits once, and its later release does nothing.
class HoldCommandInput{
 constructor(commit,progress=()=>{}){this.commit=commit;this.progress=progress;this.active=new Map()}
 press(key,token,now){key=key.toLowerCase();if(!['r','t'].includes(key)||this.active.has(token)||[...this.active.values()].some(g=>g.key===key))return false;this.active.set(token,{key,started:now,held:false});this.render(now);return true}
 fire(g,held){this.commit(held?(g.key==='t'?'recallAll':'recallField'):(g.key==='t'?'recallField':'recall'))}
 release(token,now){const g=this.active.get(token);if(!g)return false;if(!g.held)this.fire(g,now-g.started>=HOLD_MS);this.active.delete(token);this.render(now);return true}
 tick(now){for(const g of this.active.values())if(!g.held&&now-g.started>=HOLD_MS){g.held=true;this.fire(g,true)}this.render(now)}
 cancel(token){if(token===undefined)this.active.clear();else this.active.delete(token);this.render(0)}
 render(now){const g=[...this.active.values()].at(-1);this.progress(g?{key:g.key,held:g.held,progress:g.held?1:Math.max(0,Math.min(1,(now-g.started)/HOLD_MS)),label:g.key==='t'?'Mobilize settlements':'Recall all field apes'}:null)}
}
window.ATSHoldCommandInput=HoldCommandInput;window.ATSHordeCommands=Object.freeze({holdMs:HOLD_MS,nearbyRadius:NEAR,recruitRadius:RECRUIT});
})();
