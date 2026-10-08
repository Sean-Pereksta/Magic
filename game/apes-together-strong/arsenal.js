/* Late-war weapons commit to visible positions. Every burst is finite, saved,
   interruptible, and uses the existing swept projectile / cover collision. */
(() => {
'use strict';
const F=ATSForces.prototype,G=ATSGame.prototype,distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
Object.assign(ATSHumanRoles,{
 breacher:{label:'Breach trooper',hp:135,weapon:'shotgun',range:240,speed:1.12,weight:2.5},
 spotter:{label:'Forward observer',hp:90,weapon:'rifle',range:480,speed:1.08,weight:2},
 bombardier:{label:'Volley grenadier',hp:120,weapon:'rifle',range:380,speed:.85,weight:3},
 rotary:{label:'Rotary gunner',hp:165,weapon:'machine',range:420,speed:.62,weight:3.5}
});
for(const [id,extra]of Object.entries({
 repeater:{label:'Twinfang burst tank',unlockPopulation:550,hp:1650,weight:18,speed:46,range:630,cannonReload:6.8,cannonDamage:68,cannonRadius:65,paint:'#658784',fireMode:'burst'},
 bombard:{label:'Thunderback mortar tank',unlockPopulation:750,hp:2050,weight:23,speed:35,range:780,cannonReload:10,cannonDamage:90,cannonRadius:76,paint:'#a39670',fireMode:'mortar'},
 cyclone:{label:'Cyclone rotary tank',unlockPopulation:900,hp:2250,weight:25,speed:44,range:650,cannonReload:5.5,cannonDamage:12,cannonRadius:60,paint:'#657385',fireMode:'rotary'}
}))ATSVehicleVariants[id]=Object.freeze({...ATSVehicleSpecs.tank,...extra,vehicleClass:'tank',weapon:null,budget:extra.weight,visualScale:1.06});

const role=F.militaryRole;
F.militaryRole=function(i=0){const slot=((Math.floor(i)%20)+20)%20,p=this.game.population;if(p>=450&&slot===16)return 'breacher';if(p>=550&&slot===17)return 'spotter';if(p>=700&&slot===18)return 'bombardier';if(p>=850&&slot===19)return 'rotary';return role.call(this,i)};

// New blueprints only: the save loader keeps damaged objects and finite stocks.
const build=ATSWorld.prototype._buildMilitarySite;
ATSWorld.prototype._buildMilitarySite=function(site,r,add){
 const placed=[];const capture=(type,x,y,extra)=>{const o=add(type,x,y,extra);placed.push(o);return o};build.call(this,site,r,capture);
 if(site.type==='forwardBase'&&ATSUtil.hash(site.id)%3!==0)return;
 const late=site.type!=='forwardBase',n=ATSUtil.hash(site.id)%3,ex=site.extentX||site.radius-74,ey=site.extentY||site.radius-74,mirror=site.layout===1?-1:1;
 site.arsenalVersion=1;site.stronghold=late?['gatlingRedoubt','artilleryBastion','ironCitadel'][n]:'gunOutpost';site.strongholdName={gatlingRedoubt:'Gatling Redoubt',artilleryBastion:'Artillery Bastion',ironCitadel:'Iron Citadel',gunOutpost:'Gun Outpost'}[site.stronghold];site.gunIds=[];
 const free=(x,y,w,h)=>placed.every(o=>!o.solid||o.dead||Math.abs(o.x-(site.x+x*mirror))>((o.w||o.r*2||24)+w)/2+12||Math.abs(o.y-(site.y+y))>((o.h||o.r*2||24)+h)/2+12);
 let relay=null;for(const y of[-ey*.67,-ey*.3,ey*.35])for(const x of[ex*.55,-ex*.55,ex*.3])if(!relay&&free(x,y,32,28))relay=capture('powerRelay',x,y,{w:32,h:28,r:17,height:44,hp:280,maxHp:280,solid:true,collision:'rect',faction:'human'});
 if(!relay)return;
 const count=late?(n===2?4:3):1,outer=ey+96;
 for(let i=0;i<count;i++){
  // Outside the curtain wall, to either side of its wide vehicle opening.
  const rear=i>=2,x=(i%2?1:-1)*154,y=rear?-outer-43:outer+43;
  if(!free(x,y,38,32))continue;
  const gun=capture('gatlingNest',x,y,{w:38,h:32,r:21,height:42,hp:late?480:320,maxHp:late?480:320,solid:true,collision:'rect',faction:'human',relayId:relay.id,mountDir:rear?-Math.PI/2:Math.PI/2,dir:rear?-Math.PI/2:Math.PI/2,range:late?470:390,readyAt:0});site.gunIds.push(gun.id);
 }
 // These are finite static mortar positions, using the same destroyable relay.
 if(late&&n===1)for(const side of[-1,1]){const x=side*ex*.7,y=ey*.6;if(free(x,y,40,36)){const gun=capture('mortarNest',x,y,{w:40,h:36,r:22,height:40,hp:360,maxHp:360,solid:true,collision:'rect',faction:'human',relayId:relay.id,range:740,readyAt:0});site.gunIds.push(gun.id)}}
};

F.rotaryRound=function(owner,point,angle,damage=11){
 const g=this.game;if(g.bullets.length>=320)return false;
 const z=(owner.elevation||0)+32,muzzle=owner.type==='gatlingNest'?34:owner.vehicleClass?42:20,range=distance(owner,point),difficulty=g.difficulty==='wanderer'?.7:g.difficulty==='relentless'?1.15:1;
 // A nest's collision footprint remains solid. Start just beyond its muzzle;
 // cover farther along the ray is still handled by updateBullets.
 const x=owner.x+Math.cos(angle)*muzzle,y=owner.y+Math.sin(angle)*muzzle;
 g.bullets.push(g.bulletPool.take({x,y,px:x,py:y,z,pz:z,vx:Math.cos(angle)*610,vy:Math.sin(angle)*610,vz:((point.elevation||0)+18-z)/Math.max(.1,range/610),damage:damage*difficulty,life:Math.min(1.3,(range+70)/610),owner:owner.id}));
 owner.gunFlashUntil=g.time+.08;g.effect('muzzle',x,y,{life:.075,color:'#ffe1a1'});if(g.time>=(owner.gunSoundAt||0)){owner.gunSoundAt=g.time+.2;g.sound('gun',.42,owner.x)}return true;
};
F.startRotary=function(owner,target,{warmup=.85,duration=1,damage=11,interval=.09,sweep=.12}={}){
 const g=this.game;if(owner.firePlan||g.time<(owner.readyAt||0))return false;
 const angle=Math.atan2(target.y-owner.y,target.x-owner.x);owner.dir=owner.vehicleClass?owner.dir:angle;owner.firePlan={mode:'rotary',x:target.x,y:target.y,elevation:target.elevation||0,angle,start:g.time,release:g.time+warmup,until:g.time+warmup+duration,next:g.time+warmup,interval,damage,sweep,shot:0};owner.readyAt=g.time+warmup+duration+2.8;g.sound('warning',.45,owner.x);return true;
};
F.advanceWeapon=function(owner){
 const g=this.game,p=owner.firePlan;if(!p)return;
 if(owner.hp<=0||owner.dead||owner.overrun||owner.weaponDamage>=100||owner.powered===false||owner.hitTimer>.19&&!owner.vehicleClass||g.time>p.until+.2){owner.firePlan=null;return}
 if(g.time<p.release)return;
 // Never catch up a long dormant/save interval with a storm of delayed shots.
 let emitted=0;while(g.time>=p.next&&p.next<=p.until&&emitted++<3){
  if(g.siege.clearRay(owner,p)){
   if(p.mode==='burst'){const spec=this.vehicleSpec(owner);baseShell.call(this,owner,p,spec);p.shot++;}
   else{const t=Math.max(0,Math.min(1,(p.next-p.release)/Math.max(.1,p.until-p.release))),angle=p.angle+(t-.5)*p.sweep;this.rotaryRound(owner,p,angle,p.damage);p.shot++}
  }p.next+=p.interval;
 }
 if(p.next<g.time)p.next=g.time+p.interval;if(g.time>=p.until)owner.firePlan=null;
};
F.mortarVolley=function(owner,target,damage=76,radius=68){
 const g=this.game;if(this.hazards.length>90)return false;
 const angle=Math.atan2(target.y-owner.y,target.x-owner.x);for(let i=0;i<3;i++){const offset=(i-1)*76,x=target.x+Math.cos(angle+Math.PI/2)*offset,y=target.y+Math.sin(angle+Math.PI/2)*offset,fuse=2.2+i*.28;
  this.hazards.push({id:'mortar-'+g.nextId++,type:'mortar',x,y,fromX:owner.x,fromY:owner.y,start:g.time,fuse,life:fuse,radius,damage,owner:owner.id});
 }owner.cannonFlash=.3;g.sound('mortar',.9,owner.x);g.sound('warning',.65,target.x);g.noise(owner.x,owner.y,800,'mortar');return true;
};
const baseShell=F.fireShell;
F.fireShell=function(v,target,spec){
 if(!spec.fireMode)return baseShell.call(this,v,target,spec);
 const g=this.game;v.cannonTimer=spec.cannonReload*(1+(v.weaponDamage||0)*.018);
 if(spec.fireMode==='mortar'){if(distance(v,target)>=180)this.mortarVolley(v,target,spec.cannonDamage,spec.cannonRadius);return}
 if(spec.fireMode==='rotary'){this.startRotary(v,target,{warmup:.15,duration:1.65,damage:12,interval:.075,sweep:.26});return}
 if(this.hazards.length>90)return;baseShell.call(this,v,target,spec);
 v.firePlan={mode:'burst',x:target.x,y:target.y,release:g.time+.4,next:g.time+.4,until:g.time+.81,interval:.4,shot:1};
};

const shoot=G.shoot;
G.shoot=function(h,target){
 if(h.role==='rotary'){
  if(this.siege.clearRay(h,target))this.forces.startRotary(h,target);h.shootTimer=Math.max(.3,(h.readyAt||this.time)-this.time);return;
 }
 if(h.role==='bombardier'&&this.time>=(h.specialAt||0)&&distance(h,target)>110&&distance(h,target)<380&&this.siege.clearRay(h,target)&&this.forces.hazards.length<90){
  for(const side of[-1,1]){const angle=Math.atan2(target.y-h.y,target.x-h.x)+Math.PI/2;this.forces.hazards.push({id:'grenade-'+this.nextId++,type:'grenade',x:target.x+Math.cos(angle)*side*31,y:target.y+Math.sin(angle)*side*31,fromX:h.x,fromY:h.y,start:this.time,fuse:2,life:2,radius:53,damage:46,owner:h.id})}
  h.specialAt=this.time+12;h.shootTimer=2;h.animation={kind:'throw',start:this.time,duration:.6};this.sound('grenade',.6,h.x);return;
 }return shoot.call(this,h,target);
};
const update=F.update;
F.update=function(h,dt){
 const result=update.call(this,h,dt),g=this.game;
 if(h.role==='spotter'&&h.state==='combat'&&g.time>=(h.specialAt||0)&&g.time-(h.lastSeenAt??-100)<.6){const a=h.targetId==='king'?g.king:g.apesById.get(h.targetId);if(a?.hp>0&&g.lineVisible(h,a)){h.specialAt=g.time+18;g.siege.report(h,a);this.hazards.push({id:'flare-'+g.nextId++,type:'flare',x:a.x,y:a.y,start:g.time,life:8,radius:140});g.noise(h.x,h.y,500,'flare');h.animation={kind:'throw',start:g.time,duration:.5}}}
 return result;
};
const combatMove=F.combatMovement;
F.combatMovement=function(h,target,dt,options){if(h.role==='rotary'&&h.firePlan){h.moving=false;return}if(h.role==='breacher'&&distance(h,target)>100&&distance(h,target)<260&&!h.siegeTransition&&!h.onWallId){this.game.move(h,target.x-h.x,target.y-h.y,88,dt);return}return combatMove.call(this,h,target,dt,options)};

F.updateEmplacement=function(o){
 const g=this.game,site=g.world.sites.get(o.siteId),relay=g.world.objects.get(o.relayId);o.powered=!!relay&&!relay.dead&&relay.hp>0&&!site?.cleared;
 if(o.dead||o.hp<=0||!o.powered){o.firePlan=null;o.aiming=null;return}
 this.advanceWeapon(o);if(o.firePlan)return;
 if(o.aiming){if(g.time>=o.aiming.until){this.mortarVolley(o,o.aiming,72,68);o.aiming=null;o.readyAt=g.time+11}return}
 if(g.time<(o.readyAt||0)||g.time<(o.thinkAt||0)||!g.performance.think('vehicle'))return;o.thinkAt=g.time+.25;
 // Protected mortars can use a recent, delayed report from their own base.
 // Copy the reported position once; never track an unseen moving ape.
 if(o.type==='mortarNest'&&!site?.radioDown){const report=g.siege.reports.filter(r=>r.siteId===o.siteId&&g.time-r.at>=1.2&&g.time-r.at<6&&distance(o,r)>190&&distance(o,r)<o.range).sort((a,b)=>b.at-a.at)[0];if(report){o.aiming={x:report.x,y:report.y,start:g.time,until:g.time+1.3};g.sound('warning',.55,o.x);return}}
 const targets=g.apeGrid.nearest(o.x,o.y,o.range||470,6,a=>a.hp>0);
 for(const a of targets){const angle=Math.atan2(a.y-o.y,a.x-o.x),d=distance(o,a);if(o.type==='gatlingNest'&&Math.cos(angle-o.mountDir)<.35||o.type==='mortarNest'&&d<190)continue;
  const seen=g.lineVisible(o,a);if(seen===null)break;if(!seen)continue;g.siege.report(o,a);
  if(o.type==='mortarNest'){o.aiming={x:a.x,y:a.y,start:g.time,until:g.time+1.3};g.sound('warning',.55,o.x)}else this.startRotary(o,a,{warmup:1,duration:1.2,damage:10,interval:.085,sweep:.16});break;
 }
};
const tick=F.tick;
F.tick=function(dt){
 const g=this.game;for(const v of g.vehicles)this.advanceWeapon(v);for(const h of g.humans)this.advanceWeapon(h);
 // Refresh nearby defenses at a fixed cadence, with a hard cap independent of
 // explored world size. Each gun also shares the game's perception budget.
 if(g.time>=(this.nextGunScan||0)){this.nextGunScan=g.time+.5;this.nearbyGuns=g.world.getObjects(g.king.x,g.king.y,1400).filter(o=>['gatlingNest','mortarNest'].includes(o.type)&&!o.dead).slice(0,12)}
 for(const o of this.nearbyGuns||[])this.updateEmplacement(o);tick.call(this,dt);
};
const hurtObject=G.damageObject;
G.damageObject=function(o,damage,attacker){hurtObject.call(this,o,damage,attacker);if(o.type==='powerRelay'&&o.dead){const site=this.world.sites.get(o.siteId);for(const id of site?.gunIds||[]){const gun=this.world.objects.get(id);if(gun){gun.powered=false;gun.firePlan=null;gun.aiming=null}}}if(o.dead&&o.firePlan)o.firePlan=null};
})();
