/* Finite prison operations. Escaping crowds use one navigation actor per block. */
(() => {
'use strict';
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y), clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
const KINDS={regional:{name:'Regional Prison',blocks:4,reserve:12,reward:90},blacksite:{name:'Blacksite Research Compound',blocks:4,reserve:20,reward:180},transfer:{name:'Transfer Fortress',blocks:6,reserve:24,reward:140},labor:{name:'Quarry Labor Prison',blocks:4,reserve:8,reward:60}};
const basePlan=ATSWorld.prototype._sitePlan;
ATSWorld.prototype._sitePlan=function(cx,cy){
 const p=basePlan.call(this,cx,cy);
 // Use the existing dry, road-safe large parcels; explored saves are untouched.
 if(p&&p.military&&!this.sites.has(p.id)&&!p.prisonKind&&ATSUtil.hash(this.seed+':prison:'+p.id)%5<3){
  p.prisonKind=p.type==='regionalCommand'?'transfer':p.type==='armoredDepot'?'blacksite':ATSUtil.hash(p.id)%2?'regional':'labor';
  p.name=(p.prisonKind==='blacksite'?'Blacksite K-'+String(ATSUtil.hash(p.id)%90+10):p.name.split(' ')[0]+' '+KINDS[p.prisonKind].name);
  p.role='high-security rescue';
 }
 return p;
};
const baseBlueprint=ATSWorld.prototype._buildMilitarySite;
ATSWorld.prototype._buildMilitarySite=function(s,r,add){
 if(!KINDS[s.prisonKind])return baseBlueprint.call(this,s,r,add);
 const kind=s.prisonKind,spec=KINDS[kind],ex=s.extentX||s.radius-74,ey=s.extentY||s.radius-74,mirror=s.layout===1?-1:1,objects=[];
 const put=(type,x,y,data={})=>{const o=add(type,x,y,data);objects.push(o);o.prisonKind=kind;return o};
 s.fortressVersion=1;s.fortressLayout='prison';s.gates=[];s.walkways=[];s.stairs=[];s.climbRoutes=[];s.innerAccess=[];s.garrisonReserve=0;s.reserveCommitted=0;
 s.prison={version:1,kind,stage:'recon',cohorts:[],reserve:spec.reserve,reserveCommitted:0,waves:0,alarmLevel:0,escorted:0,recaptured:0,initialCaptives:s.count,nextResponse:0};
 s.serviceEntrance={x:s.x,y:s.y-ey-44};
 const wall=(x,y,w,h,inner=false)=>{const o=put('wall',x,y,{w,h,r:Math.max(w,h)/2,collision:'rect',wallTier:kind==='labor'?1:kind==='blacksite'?3:2,defenseRing:inner?3:1});o.walkable=true;o.walkHeight=o.visualHeight;s.walkways.push(o.id);return o};
 for(let x=-ex;x<=ex;x+=38){if(Math.abs(x)>83)wall(x,ey,39,22);if(Math.abs(x)>83)wall(x,-ey,39,22)}
 for(const side of[-1,1]){
  let access=null;for(let y=-ey+38;y<ey-16;y+=38){const o=wall(side*ex,y,22,39);if(!access||Math.abs(y)<Math.abs(access.y-s.y))access=o}
  if(access){access.climbAccess=access.climbable=true;access.accessKind=side<0?'vines':'scaffolding';s.climbRoutes.push([access.id]);const stair=put('stairs',side*(ex-52),(access.y-s.y),{r:12,w:70,h:26,solid:false,hp:450,height:access.walkHeight,wallId:access.id,topX:access.x,topY:access.y});s.stairs.push(stair.id)}
 }
 const gate=(x,y,inner=false)=>{const o=put('gate',x,y,{w:150,h:24,r:75,collision:'rect',gateState:inner?'open':'closed',defenseRing:inner?3:1,solid:!inner,riotGate:inner});o.hp=o.maxHp=kind==='labor'?600:kind==='blacksite'?1100:900;s.gates.push(o.id);return o};
 const front=gate(0,ey),rear=gate(0,-ey),innerY=-ey*.18,riot=gate(0,innerY,true);s.prison.riotId=riot.id;
 for(let x=-ex+24;x<ex;x+=38)if(Math.abs(x)>92)wall(x,innerY,39,22,true);
 for(const edge of[front,rear]){const sign=edge===front?-1:1;const control=put('gateControl',-64,edge.y-s.y+sign*65,{r:13,w:24,h:24,height:30,hp:220,gateId:edge.id,discovered:false});edge.controlId=control.id}
 const generator=put('generator',-ex*.59,ey*.18,{r:27,w:54,h:40,height:38,hp:330,collision:'rect',prisonDevice:'power'});
 const controls=put('prisonControl',ex*.53,-ey*.4,{r:20,w:40,h:34,height:34,hp:290,collision:'rect',prisonDevice:'controls'});
 s.prison.powerId=generator.id;s.prison.controlId=controls.id;
 let remaining=s.count;
 for(let i=0;i<spec.blocks;i++){
  const count=Math.ceil(remaining/(spec.blocks-i));remaining-=count;
  const x=(i%2?1:-1)*80,y=-ey*.4-Math.floor(i/2)*63;
  const reward=kind==='blacksite'?(i===0?'champion':i===1?'injuredLegend':i===2?'scoutVeteran':'veteran'):kind==='transfer'?(i===0?'champion':i===1?'warcaller':i===2?'injuredLegend':'veteran'):kind==='labor'?(i===0?'builderElder':i===1?'family':'veteran'):(i===0?'warcaller':i===1?'scoutVeteran':i===2?'family':'veteran');
  const species=reward==='builderElder'?'orangutan':reward==='warcaller'?'mandrill':reward==='scoutVeteran'?'gibbon':reward==='injuredLegend'?'gorilla':reward==='champion'?(kind==='blacksite'?'chimpanzee':'gorilla'):['chimpanzee','orangutan','gibbon','capuchin','mandrill','gorilla'][i%6];
  put('cage',x,y,{r:29,w:64,h:51,height:48,hp:kind==='labor'?230:kind==='blacksite'?520:380,count,prisoners:count,prisonLock:kind==='labor'&&i>1?'chain':i%2?'control':'power',prisonReward:reward,captiveSpecies:species});
 }
 for(const [x,y]of[[-ex+24,-ey+24],[ex-24,-ey+24],[-ex+24,ey-24],[ex-24,ey-24]])put('tower',x,y,{r:16,height:110,hp:310,lightRange:560,scanRange:560,powered:true,staffed:true,angle:r()*Math.PI*2,sweep:.17});
 put('alarm',ex*.59,ey*.18,{r:12,height:48,hp:160,alarmRange:1600,active:false});
 put('radio',-ex*.61,-ey*.65,{r:14,height:86,hp:230,radioRange:2200});
 put('barracks',-ex*.6,ey*.62,{r:34,w:68,h:50,height:48,hp:450,collision:'rect'});
 put('depot',ex*.59,ey*.62,{r:34,w:68,h:54,height:48,hp:450,collision:'rect',armory:kind==='transfer'});
 put('house',ex*.58,-ey*.7,{r:32,w:64,h:49,height:64,hp:400,collision:'rect',laboratory:kind==='blacksite',quarryOffice:kind==='labor'});
 put('vehicle',0,ey*.65,{r:26,w:62,h:36,height:27,hp:230,vehicleType:kind==='labor'?'truck':'armored',solid:false});
 put('berry',-ex*.34,ey*.62,{r:16,solid:false,height:15,food:150+s.tier*30,count:150+s.tier*30,supply:true});
 s.prison.exit={x:s.x,y:s.y+ey+120};s.prison.choke={x:s.x,y:s.y+innerY+50};
 for(const o of objects)if(o.type==='wall'&&o.defenseRing===3&&Math.abs(Math.abs(o.x-s.x)-ex)<60){o.climbAccess=o.climbable=true;s.innerAccess.push(o.id)}
};

class PrisonOperations {
 constructor(g){this.g=g;this.nextTick=0;this.reserved=0;this.active=[];this.restore()}
 restore(){this.reserved=0;for(const s of this.g.world.sites.values())if(s.prison){s.prison.cohorts=s.prison.cohorts||[];for(const c of s.prison.cohorts){c.count=Math.max(0,Math.floor(c.count||0));c.navClass='ape';for(const key of Object.keys(c))if(key.startsWith('_')||key==='navCohort')delete c[key];this.reserved+=c.count}}if(this.reserved+this.g.apes.filter(a=>a.hp>0).length>MAX_APE_POPULATION)throw new Error('Prison survivors exceed the population limit.');}
 site(value){return typeof value==='string'?this.g.world.sites.get(value):value}
 objects(s){return s.objects.map(id=>this.g.world.objects.get(id)).filter(Boolean)}
 unlocked(o){const s=this.site(o.siteId),p=s?.prison;if(!p||!o.prisonLock)return true;return o.prisonLock==='chain'||p.controlsDown||o.prisonLock==='power'&&p.powerDown}
 recon(value){const s=this.site(value),p=s?.prison;if(!p||!s.known)return null;const objects=this.objects(s),cells=objects.filter(o=>o.type==='cage');return{id:s.id,name:s.name,x:s.x,y:s.y,kind:KINDS[p.kind].name,stage:p.stage,power:p.powerDown?'disabled':'online',controls:p.controlsDown?'released':'locked',lockedBlocks:cells.filter(o=>!o.dead&&!this.unlocked(o)).length,captives:s.count||0,escaping:p.cohorts.reduce((n,c)=>n+c.count,0),rescued:p.escorted||0,recaptured:p.recaptured||0,reinforcements:p.reserve,alarm:p.alarmLevel,objectives:objects.filter(o=>['generator','prisonControl','radio','alarm','gateControl','cage'].includes(o.type)).map(o=>({id:o.id,label:o.type==='generator'?'Power relay':o.type==='prisonControl'?'Internal release controls':o.type==='cage'?(o.prisonReward==='champion'?'Champion holding block':o.prisonReward==='injuredLegend'?'Injured legend block':'Detention block')+(this.unlocked(o)?' · breach bars':' · '+o.prisonLock+' locked'):o.type==='gateControl'?'Gate winch':o.type==='radio'?'Reinforcement relay':'Siren station',x:o.x,y:o.y,done:o.dead})),hint:p.cohorts.length?'Keep an escort close and lead survivors beyond the perimeter.':p.controlsDown?'Breach the released blocks.':p.powerDown?'Internal controls still secure the steel-door blocks.':'Cut power for electric cells, or destroy internal controls to release every lock.'}}
 knownSites(){return [...this.g.world.sites.values()].filter(s=>s.prison&&s.known).map(s=>this.recon(s))}
 alarm(s,freed=false){const g=this.g,p=s.prison;if(!p||s.cleared)return;p.stage=freed?'escape':'breach';p.breachAt=p.breachAt??g.time;p.lastAttack=g.time;p.alarmLevel=Math.min(3,Math.max(p.alarmLevel||0,freed?3:1));if(!s.alarmDown&&!p.powerDown){if(!s.alarm)g.sound('alarm',.65,s.x);s.alarm=true;s.alarmUntil=g.time+55;for(const o of this.objects(s))if(o.type==='alarm')o.active=true}if(freed&&!p.escalationAnnounced){p.escalationAnnounced=true;g.addIntel(s.x,s.y,12);g.notify(s.name+': escort the survivors out of the compound.','gold')}}
 damaged(o){const g=this.g,s=this.site(o.siteId),p=s?.prison;if(!p)return;this.alarm(s);if(!o.dead)return;
  if(o.prisonDevice==='power'){p.powerDown=true;s.alarm=false;s.alarmDown=true;for(const x of this.objects(s))if(x.type==='tower')x.powered=false;else if(x.type==='alarm')x.active=false;g._lightsAt=-1;g.notify('Prison power cut. Electric cell locks and floodlights disabled.','green')}
  if(o.prisonDevice==='controls'){p.controlsDown=true;g.notify('Internal release controls disabled. All cell locks released.','green')}
  if(p.controlsDown||p.powerDown){const riot=g.world.objects.get(p.riotId);if(riot&&!riot.dead)g.siege?.openGate(riot)}
  for(const c of this.objects(s))if(c.type==='cage')c.prisonUnlocked=this.unlocked(c);
 }
 release(o){const g=this.g,s=this.site(o.siteId),p=s?.prison;if(!p)return 0;if(!this.unlocked(o)||g.time<(o.recapturedUntil||0))return 0;const num=Math.min(o.count||0,Math.max(0,MAX_APE_POPULATION-g.population));if(!num)return 0;
  // One cohort per holding block, independent of its captive count.
  let c=p.cohorts.find(c=>c.cageId===o.id);if(!c){const exit=g.findOpen(o.x,o.y+40,9);c={id:'prison-cohort-'+g.nextId++,siteId:s.id,cageId:o.id,x:exit.x,y:exit.y,hp:1,radius:9,navClass:'ape',state:'prisonEscort',count:0,exposed:0,safe:0,species:o.captiveSpecies||'chimpanzee',reward:o.prisonReward||'veteran',rewardClaimed:!!o.prisonRewardClaimed,createdAt:g.time};p.cohorts.push(c)}
  c.count+=num;this.reserved+=num;o.count=o.prisoners=(o.count||0)-num;s.count=Math.max(0,(s.count||0)-num);this.alarm(s,true);g.rescueFocus.push({x:c.x,y:c.y,hp:1,until:g.time+20});g.sound('rescue');return num;
 }
 recapture(s,c){const g=this.g,p=s.prison,o=g.world.objects.get(c.cageId);if(!o)return;c.count=Math.max(0,c.count);o.count=o.prisoners=(o.count||0)+c.count;o.recapturedUntil=g.time+12;s.count=(s.count||0)+c.count;p.recaptured=(p.recaptured||0)+c.count;this.reserved-=c.count;c.count=0;p.stage='liberation';g.notify('Unescorted survivors were recaptured. Return to their holding block.','red')}
 reward(s,c){const g=this.g,p=s.prison,o=g.world.objects.get(c.cageId),apes=[];let transferred=0;const amount=c.count;
  for(let i=0;i<amount;i++){c.count--;this.reserved--;const a=g.makeApe(c.x+(i%5-2)*15,c.y+Math.floor(i/5)*12,'follow');if(!a){c.count++;this.reserved++;break}transferred++;a.rescuedFrom=s.id;a.rescueRole=c.reward;a.species=c.species;g.siege?.balance(a,true);apes.push(a);
   if(!c.rewardClaimed){if(c.reward==='champion')g.champions?.promote(a,{archetype:s.prisonKind==='blacksite'?'saboteur':'wallbreaker',sourceId:s.id});else if(c.reward==='injuredLegend'){g.champions?.promote(a,{archetype:'bulwark',injured:true,sourceId:s.id});if(!a.champion){a.hp*=.45;a.recoveryRemaining=60;g.champions?.veteran(a,{injured:true,role:'injuredLegend'})}}else g.champions?.veteran(a,{role:c.reward,injured:false});c.rewardClaimed=true;if(o)o.prisonRewardClaimed=true}
  }
  p.escorted=(p.escorted||0)+transferred;g.stats.freed+=transferred;if(!c.count&&o&&!o.count&&!o.rescueCredited){o.rescueCredited=true;g.stats.prisons++}
  if(apes.length&&['family','builderElder'].includes(c.reward))g.kingdom?.welcomeSurvivors?.(apes,{facilityName:s.name});
  if(!s.count&&!p.cohorts.some(c=>c.count)){s.rescued=true;p.stage='liberated';if(!p.rewardPaid){p.rewardPaid=true;const reward=KINDS[p.kind].reward;g.food+=reward;g.notify(s.name+' survivors safe. +'+reward+' food.','green')}}g.checkSite(s);
 }
 response(s){const g=this.g,p=s.prison;if(!s.alarm||s.radioDown||s.barracksDown||p.powerDown||p.reserve<=0||g.time<(p.nextResponse||0)||p.waves>=3)return;
  const n=Math.min(6,p.reserve,Math.floor((s.strength||0)),Math.max(0,Math.floor((g.responseBudget-g.forceWeight())/3.5)),Math.max(0,g.activeHumanCapacity()-g.humans.length-g.vehicles.reduce((n,v)=>n+(v.troops||0),0)));if(n<=0)return;
  const entrance=s.staging?.[0]||s.prison.exit,members=[];for(let i=0;i<n;i++){const h=g.makeHuman(entrance.x+(i%3)*24,entrance.y+Math.floor(i/3)*22,s);h.prisonResponse=true;members.push(h)}p.reserve-=n;p.reserveCommitted+=n;s.strength-=n;p.waves++;p.nextResponse=g.time+24;g.forces.createSquad?.(members,p.choke,{order:'Defend Base',siteId:s.id});
  if(p.alarmLevel>=3&&!p.regionalResponseCalled){const source=g.world.getSites(s.x,s.y,2400).find(other=>other.id!==s.id&&other.military&&!other.cleared&&!other.radioDown&&(other.strength||0)>=8);if(source&&g.spawnRaid(source,p.exit,false,{peopleCap:12,budgetAllowance:36}))p.regionalResponseCalled=true}
 }
 tick(dt){const g=this.g;if(g.time<this.nextTick)return;const elapsed=clamp(g.time-(this.lastTick??g.time-.25),.01,.5);this.lastTick=g.time;this.nextTick=g.time+.25;
  // Nearby sites plus at most 12 escaping blocks are the entire planning budget.
  const nearby=g.world.getSites(g.king.x,g.king.y,1500).filter(s=>s.prison).sort((a,b)=>distance(a,g.king)-distance(b,g.king)).slice(0,8);this.active=nearby;let cohortBudget=12,guardBudget=18;
  for(const s of nearby){const p=s.prison,objects=this.objects(s);if(distance(s,g.king)<(s.radius||420)+250)s.known=true;
   if(s.alarm&&!s.cleared){p.breachAt=p.breachAt??g.time;p.alarmLevel=Math.max(p.alarmLevel||0,1);const riot=g.world.objects.get(p.riotId);if(!p.powerDown&&!p.controlsDown&&riot&&!riot.dead&&riot.gateState==='open'&&(p.alarmLevel>=3||g.time-p.breachAt>6)){riot.gateState='closed';riot.solid=true;riot.forcedOpen=false;p.lockdown=true;p.alarmLevel=Math.max(2,p.alarmLevel);g.world.navRevision++;g.visibilityCache.clear()}this.response(s)}
   for(const o of objects)if(o.type==='cage'&&o.rescueOpened&&o.count>0&&g.population<MAX_APE_POPULATION)this.release(o);
   if(s.alarm)for(const h of g.humans){if(guardBudget<=0)break;if(h.hp<=0||h.siteId!==s.id||h.elevation||h.siegeTransition)continue;guardBudget--;const target=p.cohorts.find(c=>c.count&&distance(c,h)<280)||p.choke;h.prisonDuty={x:target.x,y:target.y,until:g.time+1};}
   for(const c of p.cohorts){if(!c.count||cohortBudget--<=0)continue;const escort=g.apeGrid.nearest(c.x,c.y,210,3,a=>a.hp>0),enemy=g.humanGrid.nearest(c.x,c.y,85,3,h=>h.hp>0);if(enemy.length&&!escort.length){c.exposed=(c.exposed||0)+elapsed;c.moving=false;if(c.exposed>=5)this.recapture(s,c);continue}c.exposed=Math.max(0,(c.exposed||0)-elapsed*2);
    if(escort.length){let target=distance(c,g.king)<600?g.king:escort[0];const speed=(c.reward==='injuredLegend'?39:57)*(escort.some(a=>a.champion?.archetype==='rescueBearer')?1.3:1);if(distance(c,target)>48)g.navigation.move(c,target.x-c.x,target.y-c.y,speed,elapsed);else c.moving=false;
     const outside=Math.abs(c.x-s.x)>(s.extentX||s.radius)+75||Math.abs(c.y-s.y)>(s.extentY||s.radius)+75;c.safe=outside&&!enemy.length?(c.safe||0)+elapsed:0;if(c.safe>=4)this.reward(s,c)}else c.moving=false;
   }p.cohorts=p.cohorts.filter(c=>c.count>0);if(p.cohorts.length)p.stage='escape';else if(!s.rescued&&p.controlsDown)p.stage='liberation';
  }
  // Cohorts left in a remote compound remain saved and reserved, never teleport.
  g.performance.counters.prisonCohorts=Math.min(12,12-cohortBudget);g.performance.counters.prisonGuardPlans=18-guardBudget;
 }
}
Object.defineProperty(ATSGame.prototype,'prisons',{get(){return this._prisons||(this._prisons=new PrisonOperations(this))}});
const population=Object.getOwnPropertyDescriptor(ATSGame.prototype,'population').get;
Object.defineProperty(ATSGame.prototype,'population',{get(){return population.call(this)+(this._prisons?.reserved||0)}});
const damage=ATSGame.prototype.damageObject;
ATSGame.prototype.damageObject=function(o,n,a){const p=o.siteId&&this.world.sites.get(o.siteId)?.prison;if(p&&o.type==='cage'&&!this.prisons.unlocked(o)){this.prisons.alarm(this.world.sites.get(o.siteId));if(this.time>=(o.nextLockFeedback||0)){o.nextLockFeedback=this.time+1;this.effect('hit',o.x,o.y,{life:.2,color:'#8dd8e8'});this.sound('smash',.25,o.x)}return false}const result=damage.call(this,o,n,a);if(p)this.prisons.damaged(o);return result};
const release=ATSGame.prototype.releaseCaptives;
ATSGame.prototype.releaseCaptives=function(o,announce){return this.world.sites.get(o.siteId)?.prison?this.prisons.release(o):release.call(this,o,announce)};
const checkSite=ATSGame.prototype.checkSite;
ATSGame.prototype.checkSite=function(s){if(s?.prison?.cohorts.some(c=>c.count))return;return checkSite.call(this,s)};
const update=ATSGame.prototype.update;
ATSGame.prototype.update=function(dt,input){const result=update.call(this,dt,input);if(!this.ended)this.prisons.tick(dt);return result};
const lights=ATSGame.prototype.getLights;
ATSGame.prototype.getLights=function(){return lights.call(this).filter(l=>{const o=this.world.objects.get(l.id);return !(o?.prisonKind&&o.type==='tower'&&o.powered===false)})};
// Give nearby guards a real choke-point/recapture destination while retaining
// their normal perception and combat when apes are in weapon range.
const human=ATSSiege.prototype.updateHuman;
ATSSiege.prototype.updateHuman=function(h,dt){const d=h.prisonDuty,g=this.g;if(d?.until>=g.time&&!h.elevation&&!h.siegeTransition&&!g.apeGrid.nearest(h.x,h.y,160,1).length){h.moving=false;h.shootTimer=Math.max(0,h.shootTimer-dt);if(distance(h,d)>24)g.move(h,d.x-h.x,d.y-h.y,65,dt);return true}return human.call(this,h,dt)};
const fromJSON=ATSGame.fromJSON;
ATSGame.fromJSON=function(d,hooks){const g=fromJSON.call(this,d,hooks);g.prisons.restore();return g};
const serialize=ATSGame.prototype.serialize;
ATSGame.prototype.serialize=function(){const saved=serialize.call(this);saved.world.sites=saved.world.sites.map(([id,s])=>s.prison?[id,{...s,prison:{...s.prison,cohorts:s.prison.cohorts.map(c=>this.savedActor(c))}}]:[id,s]);return saved};
window.ATSPrisonKinds=KINDS;window.ATSPrisonOperations=PrisonOperations;
})();
