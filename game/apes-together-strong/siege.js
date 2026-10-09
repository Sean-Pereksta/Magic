/* Persistent species orders, fortress navigation and shared battlefield knowledge. */
(() => {
'use strict';
const SPECIES=['gorilla','orangutan','chimpanzee','gibbon','capuchin','mandrill'];
const HP={gorilla:260,orangutan:205,chimpanzee:120,gibbon:95,capuchin:90,mandrill:140};
const SPEED={gorilla:76,orangutan:88,chimpanzee:96,gibbon:112,capuchin:105,mandrill:96};
const LIGHT=new Set(['gibbon','capuchin','chimpanzee']),FOLLOW=new Set(['follow','charge','hold']);
const TYPES=new Set(['move','attack','sabotage','climb','charge','shield','hold','defend','follow','regroup','retreat','rally','assault','attackArea']);
const dist=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
const point=p=>({x:p.x,y:p.y}),finite=p=>p&&Number.isFinite(p.x)&&Number.isFinite(p.y);
const altitude=a=>a?.elevation||a?.wallClimbHeight||0;
function rectHit(a,b,o,pad=0){
 const hw=(o.w||o.r*2||24)/2+pad,hh=(o.h||o.r*2||24)/2+pad,dx=b.x-a.x,dy=b.y-a.y;
 let lo=0,hi=1;for(const [p,q]of[[-dx,a.x-o.x+hw],[dx,o.x+hw-a.x],[-dy,a.y-o.y+hh],[dy,o.y+hh-a.y]]){if(Math.abs(p)<1e-8){if(q<0)return null;continue}const t=q/p;if(p<0)lo=Math.max(lo,t);else hi=Math.min(hi,t);if(lo>hi)return null}return lo;
}

// This hook runs only while creating a new blueprint. Restoring explored sites
// uses their persisted objects and never replenishes damage or reserves.
const oldMilitary=ATSWorld.prototype._buildMilitarySite;
ATSWorld.prototype._buildMilitarySite=function(site,r,add){
 const objects=[];const capture=(type,x,y,extra)=>{const o=add(type,x,y,extra);objects.push(o);return o};
 oldMilitary.call(this,site,r,capture);
 const ex=site.extentX||site.radius-74,ey=site.extentY||site.radius-74,mirror=site.layout===1?-1:1;
 site.fortressVersion=1;site.fortressLayout=['citadel','militaryTown','armoredFortress','researchPrison','borderFortress'][ATSUtil.hash(site.id)%5];
 site.gates=objects.filter(o=>o.type==='gate').map(o=>o.id);site.climbRoutes=[];site.stairs=[];site.walkways=[];
 const rear=capture('gate',0,-ey,{w:143,h:28,r:68,collision:'rect',gateState:'closed'});site.gates.push(rear.id);
 if(site.type==='regionalCommand'||site.fortressLayout==='citadel'){
  const y=-ey*.43;
  site.innerAccess=[];
  for(let x=-ex+19;x<ex;x+=38)if(Math.abs(x)>106)capture('wall',x,y,{w:39,h:34,collision:'rect',r:20,defenseRing:3});
  for(const side of[-1,1]){const w=objects.filter(o=>o.type==='wall'&&o.defenseRing===3&&Math.sign(o.x-site.x)===side).sort((a,b)=>Math.abs(Math.abs(a.x-site.x)-185)-Math.abs(Math.abs(b.x-site.x)-185))[0];if(w){w.climbAccess=w.walkable=w.climbable=true;w.walkHeight=w.visualHeight;site.innerAccess.push(w.id);const stair=capture('stairs',(w.x-site.x)*mirror,y+60,{r:12,w:26,h:70,solid:false,hp:450,height:w.walkHeight,wallId:w.id,topX:w.x,topY:w.y});site.stairs.push(stair.id)}}
  const inner=capture('gate',0,y,{w:174,h:28,r:87,collision:'rect',defenseRing:3});site.gates.push(inner.id);
 }
 for(const id of site.gates){const gate=objects.find(o=>o.id===id);gate.gateState='closed';gate.maxHp=gate.hp=site.type==='regionalCommand'?1250:site.type==='armoredDepot'?1050:850;
  const dy=gate.y-site.y,sign=gate.defenseRing===3?-1:dy<0?1:-1;
  const control=capture('gateControl',-58,dy+sign*68,{r:13,w:24,h:24,height:30,hp:220,solid:true,gateId:id,discovered:false});gate.controlId=control.id;
 }
 // A narrow maintenance corridor crosses both perimeter rings. Other high
 // walls stay unclimbable. Stairs sit inside each wall, with real top anchors.
 for(const side of[-1,1]){
  const access=objects.filter(o=>o.type==='wall'&&(o.w||0)<(o.h||0)&&Math.sign(o.x-site.x)===side&&o.defenseRing!==3&&objects.every(other=>other===o||!other.solid||other.dead||!this._touches(other,o.x-side*((o.w||20)/2+24),o.y,12))).sort((a,b)=>Math.abs(a.y-site.y)-Math.abs(b.y-site.y));
  const chosen=[];for(const wall of access)if(!chosen.some(w=>Math.abs(w.x-wall.x)<25)){chosen.push(wall);if(chosen.length===2)break}
  chosen.sort((a,b)=>Math.abs(b.x-site.x)-Math.abs(a.x-site.x));site.climbRoutes.push(chosen.map(w=>w.id));
  for(const wall of chosen){wall.climbAccess=true;wall.climbable=true;wall.accessKind=side<0?'vines':'scaffolding';wall.walkable=true;wall.walkHeight=wall.visualHeight||wall.height;
   const sx=(wall.x-site.x)*mirror-side*mirror*55,sy=wall.y-site.y;
   const stair=capture('stairs',sx,sy,{r:12,w:70,h:26,solid:false,hp:450,height:wall.walkHeight,wallId:wall.id,topX:wall.x,topY:wall.y});site.stairs.push(stair.id);
  }
 }
 for(const o of objects){if(o.type==='wall'&&(!o.barricade||o.defenseRing===2)){o.walkable=true;o.walkHeight=o.visualHeight||o.height;site.walkways.push(o.id)}if(o.type==='tower'){o.staffed=true;o.scanRange=o.lightRange||600}}
 site.garrisonReserve=Math.min(24,Math.floor((site.guards||0)*.2));site.reserveCommitted=0;
 // Preserve the existing large footprints, but give the five families a
 // distinct interior. These partitions retain wide roads and open courtyards.
 if(['militaryTown','researchPrison','borderFortress'].includes(site.fortressLayout)){
  const y=site.fortressLayout==='borderFortress'?ey*.36:-ey*.2;
  for(const side of[-1,1])capture('humanBarricade',side*(ex*.6),y,{w:80,h:18,r:40,hp:300,height:24,solid:true,collision:'rect',faction:'human',fortification:false});
 }
 const place=(type,x,y,extra)=>{const wx=site.x+x*mirror,wy=site.y+y;if(objects.some(o=>o.solid&&Math.abs(o.x-wx)<((o.w||o.r*2||24)+extra.w)/2+22&&Math.abs(o.y-wy)<((o.h||o.r*2||24)+extra.h)/2+22))return;capture(type,x,y,{collision:'rect',...extra})};
 if(site.fortressLayout==='militaryTown')for(const x of[110,210])place('house',x,-ey*.6,{w:65,h:54,r:32,height:65,hp:350});
 if(site.fortressLayout==='armoredFortress')place('depot',160,-ey*.4,{w:88,h:64,r:44,height:55,hp:460,repairBay:true});
 if(site.fortressLayout==='researchPrison'){place('house',180,-ey*.62,{w:86,h:64,r:43,height:72,hp:450,laboratory:true});place('tower',170,-ey*.05,{w:22,h:22,r:14,height:110,hp:300,staffed:true,scanRange:520})}
 if(site.fortressLayout==='borderFortress')for(const x of[-170,170])place('tower',x,ey*.52,{w:24,h:24,r:14,height:100,hp:320,staffed:true,scanRange:620,angle:Math.PI/2});
};

class Siege {
 constructor(g){this.g=g;this.selected=[];this.unitIds=null;this.groups={};this.drafts={};this.presets=[[],[],[],[]];this.serial=0;this.reports=[];this.deliveries=[];this.stones=[];this.rallyReady=0;this.nextScan=0;this.scanCursor=0;this.nextGroupTick=0;this.nextGarrison=0;this.status='';this.pathCache=new Map();this.wallCache=new Map();this.stats={towerChecks:0,reports:0,stoneHits:0};}
 balance(a,force=false){if(a.id==='king'||a.state==='young'||!HP[a.species]||a.speciesBalanceVersion&&!force)return;const ratio=clamp(a.hp/Math.max(1,a.maxHp||120),0,1);a.maxHp=HP[a.species]+(a.trainingLevel||0)*12+(a.state==='scout'?25:0);a.hp=ratio*a.maxHp;a.speed=SPEED[a.species];a.speciesBalanceVersion=1;}
 eligible(a){return a.hp>0&&FOLLOW.has(a.state)}
 members(species){return this.g.apes.filter(a=>this.eligible(a)&&species.includes(a.species))}
 select(species,add=false){this.unitIds=null;const ids=(Array.isArray(species)?species:[species]).filter(s=>SPECIES.includes(s));this.selected=add?[...new Set([...this.selected,...ids])]:ids;return this.selected}
 selectClimbers(){const ids=this.members(SPECIES).filter(a=>LIGHT.has(a.species)).map(a=>a.id);this.selectUnits(ids);this.status=ids.length?'Climbers selected · move or attack inside walls to climb':'No climbing apes available';return ids.length}
 selectUnits(ids,add=false){this.unitIds=[...new Set([...(add?this.unitIds||[]:[]),...ids])].filter(id=>this.eligible(this.g.apesById.get(id)||{}));this.selected=[...new Set(this.unitIds.map(id=>this.g.apesById.get(id).species))];return this.selected}
 selectedMembers(){return this.members(this.selected.length?this.selected:SPECIES).filter(a=>!this.unitIds||this.unitIds.includes(a.id))}
 clearActor(a){delete a.areaTarget;delete a._areaThink;delete a.armyOrder;delete a.siegeRoute;delete a._siegeProgress;delete a._orderStall;if(a.siegeTransition)this.detach(a);}
 statusOf(species){const group=this.groups[species];return group?.blocked?'blocked':group?.orders.length?'active':'idle'}
 makeOrder(p,kind){
  if(!finite(p))return null;const g=this.g;
  const visible=o=>this.known(o),objects=g.world.getObjects(p.x,p.y,110).filter(o=>!o.dead&&o.hp>0&&visible(o)&&!['tree','rock','berry','stairs'].includes(o.type));
  let target=objects.filter(o=>rectHit(p,p,o,12)!==null).sort((a,b)=>dist(a,p)-dist(b,p))[0];
  const enemy=g.humanGrid.nearest(p.x,p.y,24,1,h=>this.visibleTarget(h))[0]||g.vehicleGrid.nearest(p.x,p.y,45,1,h=>this.visibleTarget(h))[0];if(enemy)target=enemy;
  if(kind==='move'||kind==='attackArea')target=null;
  if(kind==='climb')target=g.world.getObjects(p.x,p.y,100).filter(o=>o.climbAccess&&!o.dead&&o.hp>0&&this.known(o)).sort((a,b)=>dist(a,p)-dist(b,p))[0];
  if(target){const type=kind|| (target.type==='gateControl'?'sabotage':target.climbAccess?'climb':'attack');return{id:'order-'+ ++this.serial,type,targetId:target.id,targetKind:target.id.startsWith('human')?'human':target.id.startsWith('vehicle')?'vehicle':'object',x:target.x,y:target.y};}
  if(kind==='climb'){this.status='Choose a visible wall with vines or scaffolding';return null}
  const site=g.world.getSites(p.x,p.y,100).find(s=>s.military&&!s.cleared&&Math.abs(p.x-s.x)<(s.extentX||s.radius)&&Math.abs(p.y-s.y)<(s.extentY||s.radius));
  if(!kind&&site&&site.known){const gate=(site.gates||site.objects).map(id=>g.world.objects.get(id)).filter(o=>o?.type==='gate'&&!o.dead&&o.solid&&this.known(o)).sort((a,b)=>dist(a,this.center())-dist(b,this.center()))[0];if(gate)return{id:'order-'+ ++this.serial,type:'assault',targetId:gate.id,targetKind:'object',siteId:site.id,x:gate.x,y:gate.y}}
  if(kind==='attackArea')return{id:'order-'+ ++this.serial,type:'attackArea',...point(p)};
  const q=g.navigation.blocked(p.x,p.y,10,'ape')?g.findOpen(p.x,p.y):p;
  if(g.navigation.blocked(q.x,q.y,10,'ape')){this.status='No reachable staging point';return null}
  return{id:'order-'+ ++this.serial,type:kind||'move',...point(q)};
 }
 center(species=this.selected){const list=this.members(species.length?species:SPECIES).filter(a=>!this.unitIds||this.unitIds.includes(a.id));return list.length?{x:list.reduce((n,a)=>n+a.x,0)/list.length,y:list.reduce((n,a)=>n+a.y,0)/list.length}:point(this.g.king)}
 draft(p,kind){const order=this.makeOrder(p,kind);if(!order)return false;if(order.type==='climb'&&!this.selectClimbers())return false;const ids=this.selected.length?this.selected:SPECIES;if(ids.some(s=>(this.drafts[s]?.length||0)+(this.groups[s]?.orders.length||0)>=16)){this.status='Route full (16)';return false}for(const s of ids)(this.drafts[s]||(this.drafts[s]=[])).push({...order});return true}
 dispatch(append=false){let count=0;const ids=this.selected.length?this.selected:SPECIES,shield=ids.flatMap(s=>this.drafts[s]||[]).find(o=>o.type==='shield');for(const s of ids){const orders=this.drafts[s]||[];if(!orders.length)continue;if(this.issue([s],orders,append)){count++;this.drafts[s]=[]}}if(shield)this.formation(ids,shield);return count>0}
 issue(species,orders,append=false){
  if(orders.some(o=>o.type==='climb'&&(!this.resolve(o)?.climbAccess||this.resolve(o)?.dead||this.resolve(o)?.hp<=0))){this.status='Choose an intact climbable wall';return false}
  if(orders.some(o=>o.type==='climb'))species=species.filter(s=>LIGHT.has(s));if(!species.length)return false;
  if(orders.some((o,i)=>o.type==='attackArea'&&i!==orders.length-1)){this.status='Attack area must be the final stop';return false}
  if(append&&species.some(id=>this.groups[id]?.orders.some(o=>o.type==='attackArea'))){this.status='Final attack already set; Go starts a new route';return false}
  if(this.g.ended||!orders.length||orders.some(o=>!TYPES.has(o.type)||!finite(o)))return false;
  for(const s of species){if(!SPECIES.includes(s))continue;const old=this.groups[s];if((append?old?.orders.length||0:0)+orders.length>16){this.status='Route full (16)';return false}}
  for(const s of species){if(!SPECIES.includes(s))continue;const old=this.groups[s],list=append&&old?old.members.map(id=>this.g.apesById.get(id)).filter(a=>a&&this.eligible(a)):this.members([s]).filter(a=>!this.unitIds||this.unitIds.includes(a.id));if(!list.length)continue;
   if(append&&old?.orders.length){old.orders.push(...orders.map(o=>({...o})));continue}
   const group={id:'army-'+ ++this.serial,species:s,members:list.map(a=>a.id),orders:orders.map(o=>({...o})),started:this.g.time,blocked:false};this.groups[s]=group;
   list.forEach((a,i)=>{this.g.tactics?.clearOrder(a);this.clearActor(a);a.state='hold';a.armyOrder={group:group.id,index:0,slot:i};a.target=point(a)});
  }
  const shield=orders.find(o=>o.type==='shield');if(shield)this.formation(species,shield);
  this.status='';return true;
 }
 formation(species,destination){const units=this.members(species).filter(a=>a.armyOrder&&a.armyOrder.group===this.groups[a.species]?.id).sort((a,b)=>Number(b.shield?.hp>0)-Number(a.shield?.hp>0)),front=units.filter(a=>a.shield?.hp>0).length,columns=Math.min(12,Math.max(3,front)),center=this.center(species),heading=Math.atan2(destination.y-center.y,destination.x-center.x);units.forEach((a,i)=>{const slot=i<front?i:i-front+Math.ceil(front/columns)*columns;a.armyOrder.formation={side:(slot%columns-(columns-1)/2)*25,back:Math.floor(slot/columns)*28,heading}})}
 immediate(type,p=this.center()){const ids=this.selected.length?this.selected:SPECIES;return this.issue(ids,[{id:'order-'+ ++this.serial,type,...point(p)}])}
 shortcut(cmd,aim){if(!this.selected.length)return false;let type={charge:'charge',spreadCharge:'charge',attackNearest:'defend',recall:'retreat',hold:'hold',call:'follow'}[cmd];if(!type)return false;const center=this.center(),length=Math.hypot(aim.x,aim.y)||1,p=['charge'].includes(type)?{x:center.x+aim.x/length*570,y:center.y+aim.y/length*570}:type==='retreat'?point(this.g.king):center;const ok=this.immediate(type,p);if(cmd==='spreadCharge')for(const a of this.selectedMembers())a.armySpread=true;return ok}
 skip(){for(const s of this.selected){const group=this.groups[s];if(!group)continue;for(const id of group.members){const a=this.g.apesById.get(id);if(a?.armyOrder){a.armyOrder.index++;delete a.siegeRoute;delete a._orderStall}}group.blocked=false}}
 retry(){for(const s of this.selected){const group=this.groups[s];if(group){group.blocked=false;for(const id of group.members){const a=this.g.apesById.get(id);if(a){delete a.siegeRoute;delete a._orderStall;a._siegeProgress=null}}}}this.pathCache.clear()}
 resolve(o){return o.targetKind==='human'?this.g.humansById.get(o.targetId):o.targetKind==='vehicle'?this.g.vehicles.find(v=>v.id===o.targetId):this.g.world.objects.get(o.targetId)}
 known(o){if(o.type==='gateControl')return !!o.discovered;const w=this.g.world;return !w.discovered?.size||w.discovered.has(Math.floor(o.x/(w.revealCellSize||192))+','+Math.floor(o.y/(w.revealCellSize||192)))}
 visibleTarget(t){const g=this.g;if(t._siegeSightAt>g.time)return !!t._siegeVisible;for(const a of g.apeGrid.nearest(t.x,t.y,450,6)){const seen=g.lineVisible(a,t);if(seen===null)return !!t._siegeVisible;if(seen){t._siegeSightAt=g.time+.2;t._siegeVisible=true;return true}}t._siegeSightAt=g.time+.2;t._siegeVisible=false;return false}
 rayObjects(a,b){const result=[];this.g.world._queryCollision(Math.min(a.x,b.x)-2,Math.min(a.y,b.y)-2,Math.max(a.x,b.x)+2,Math.max(a.y,b.y)+2,o=>{result.push(o);return true});return result}
 clearRay(a,b,opts={}){
  const za=opts.za??altitude(a)+30,zb=opts.zb??altitude(b)+20;
  for(const o of this.rayObjects(a,b)){if(o.dead||!o.solid||o.id===opts.ignore||o.id===a.id||o.id===b.id||o.type==='berry'||o.type==='stairs'||o.lowCover&&!opts.heightCover||o.type==='gate'&&o.gateState==='open')continue;const t=rectHit(a,b,o,opts.projectile?1:0);if(t===null)continue;const height=o.type==='tree'?(opts.projectile?o.trunkHeight||60:90):o.visualHeight||o.height||40;if(za+(zb-za)*t<height+2)return false}return true;
 }
 meleeAllowed(a,b){return Math.abs(altitude(a)-altitude(b))<26}
 openGate(gate){if(!gate)return;gate.gateState=gate.dead?'destroyed':'open';gate.solid=false;gate.forcedOpen=true;gate.openedAt=this.g.time;this.g.world.navigationChanged(gate);this.g.visibilityCache.clear();this.pathCache.clear();this.g._lightsAt=-1;this.g.effect('wave',gate.x,gate.y,{color:'#acd4ab',range:90,life:.7});for(const group of Object.values(this.groups))if(group.blocked==='gate'){group.blocked=false;for(const id of group.members){const a=this.g.apesById.get(id);if(a){delete a.siegeRoute;delete a._orderStall}}}}
 damagedObject(o){if(o.type==='gateControl'&&o.dead)this.openGate(this.g.world.objects.get(o.gateId));if(o.type==='gate'){if(o.dead)this.openGate(o);else if(o.gateState!=='open'&&o.hp<o.maxHp)o.gateState='damaged'}if(o.siteId){const s=this.g.world.sites.get(o.siteId);if(s){s.siegeStage=4;s.lastHostileAt=this.g.time}}}
 prepareShields(){
  const g=this.g;if(g.humanGrid.near(g.king.x,g.king.y,360).length||g.time-g.lastContact<8){this.status='Prepare logs away from combat';return false}
  const home=g.colonies.lodgeAt(g.king),logs=g.world.getObjects(g.king.x,g.king.y,170).filter(o=>o.type==='tree'&&o.dead&&!o.logCollected);let count=0;
  for(const a of this.selectedMembers()){if(!['gorilla','orangutan'].includes(a.species)||a.shield?.hp>0||dist(a,g.king)>250)continue;if(home&&(home.wood||0)>=4)home.wood-=4;else if(logs.length)logs.pop().logCollected=true;else break;a.shield={hp:a.species==='gorilla'?180:145,maxHp:a.species==='gorilla'?180:145};count++}
  this.status=count?'':'Needs 4 timber per log at a village, or fallen trees';return count>0;
 }
 absorb(a,damage,source){if(a.id==='king'||!a.shield||a.shield.hp<=0||!finite(source))return damage;const explosion=['grenade','shell','mortar','airstrike'].includes(source.type);if(!explosion&&source.type!=='bullet')return damage;const angle=Math.atan2(source.y-a.y,source.x-a.x);if(Math.cos(angle-(a.dir||0))<.5)return damage;const protectedDamage=explosion?damage*.35:damage,blocked=Math.min(a.shield.hp,protectedDamage);a.shield.hp=Math.max(0,a.shield.hp-(explosion?damage:protectedDamage));if(a.shield.hp===0){a.shield.brokenAt=this.g.time;a.staggerUntil=this.g.time+.25;this.g.effect('smash',a.x,a.y,{life:.35,color:'#bf935c'})}return damage-blocked}
 rally(){const g=this.g;if(g.time<this.rallyReady){this.status='Rally cooling down';return false}const list=this.selectedMembers().filter(a=>a.species==='mandrill'&&!g.blastActive(a)&&!a.siegeTransition);if(!list.length){this.status='Select living mandrills';return false}const center=this.center(),a=list.sort((x,y)=>dist(x,center)-dist(y,center))[0];a.rallyCast={at:g.time,release:g.time+.45};a.animation={kind:'rally',start:g.time,duration:1.1};a.attackTimer=1.1;return true}
 updateRally(a){if(!a.rallyCast)return;const g=this.g;if(a.hp<=0||g.blastActive(a)){delete a.rallyCast;return}if(g.time<a.rallyCast.release)return;delete a.rallyCast;if(g.time<this.rallyReady)return;this.rallyReady=g.time+30;for(const p of g.apeGrid.near(a.x,a.y,168))if(p.hp>0)p.rallyUntil=g.time+6;g.effect('wave',a.x,a.y,{color:'#dcb37c',range:168,life:1});g.noise(a.x,a.y,336,'rally');g.sound('call',.8,a.x)}
 throwAt(a,t,kind){
  const g=this.g;if(a.species!=='capuchin'||a.siegeTransition||a.wallClimb||g.blastActive(a))return false;
  if(a.throwWindup)return true;if(g.time<(a.nextThrowAt||0))return true;
  if(!this.clearRay(a,t,{ignore:kind==='object'?t.id:null}))return false;
  a.throwWindup={targetId:t.id,kind,release:g.time+.42};a.nextThrowAt=g.time+1.25;a.animation={kind:'throw',start:g.time,duration:.8};a.attackTimer=.8;a.dir=Math.atan2(t.y-a.y,t.x-a.x);return true;
 }
 releaseThrow(a){const w=a.throwWindup;if(!w)return;const g=this.g;if(a.hp<=0||g.blastActive(a)||a.siegeTransition||a.hitTimer>.15){delete a.throwWindup;return}if(g.time<w.release)return;delete a.throwWindup;const t=this.resolve({targetId:w.targetId,targetKind:w.kind});if(!t||t.hp<=0||!this.clearRay(a,t,{ignore:w.kind==='object'?t.id:null})||dist(a,t)>185||this.stones.length>=256)return;
  this.stones.push({id:'stone-'+ ++this.serial,owner:a.id,from:point(a),to:point(t),x:a.x,y:a.y,age:0,duration:Math.max(.25,dist(a,t)/250),z0:altitude(a)+28,z1:altitude(t)+18,targetId:t.id,kind:w.kind,damage:g.apeDamage(a,6)});
 }
 capuchin(a,dt){if(a.species!=='capuchin'||!this.eligible(a))return false;this.releaseThrow(a);if(a.throwWindup)return true;const g=this.g,t=g.humanGrid.nearest(a.x,a.y,168,4).find(t=>this.clearRay(a,t));if(!t)return false;const d=dist(a,t);if(d<42){if(a.state!=='hold')g.move(a,a.x-t.x,a.y-t.y,a.speed,dt);return this.meleeAllowed(a,t)?false:true}a.dir=Math.atan2(t.y-a.y,t.x-a.x);return this.throwAt(a,t,'human')}
 updateStones(dt){const g=this.g;for(const b of this.stones){const prev=point(b),z=b.z??b.z0;b.age+=dt;const t=Math.min(1,b.age/b.duration);b.x=b.from.x+(b.to.x-b.from.x)*t;b.y=b.from.y+(b.to.y-b.from.y)*t;b.z=b.z0+(b.z1-b.z0)*t+Math.sin(t*Math.PI)*42;
   const target=this.resolve({targetId:b.targetId,targetKind:b.kind});if(!this.clearRay(prev,b,{za:z,zb:b.z,ignore:b.kind==='object'?b.targetId:null,projectile:true})){b.done=true;g.effect('hit',b.x,b.y,{life:.15});continue}
   if(t>=1){b.done=true;if(target&&target.hp>0&&dist(target,b.to)<22){if(b.kind==='object')g.damageObject(target,b.damage*.5,{x:b.from.x,y:b.from.y,type:'stone'});else if(b.kind==='human')g.hurt(target,b.damage,{x:b.from.x,y:b.from.y,type:'stone'});this.stats.stoneHits++}g.effect('hit',b.x,b.y,{life:.16,color:'#bdb895'})}}
  this.stones=this.stones.filter(b=>!b.done&&b.age<3);
 }
 routeFor(a,o){
  const g=this.g,t=this.resolve(o)||o,site=o.siteId?g.world.sites.get(o.siteId):t.siteId?g.world.sites.get(t.siteId):[...g.world.sites.values()].find(s=>s.fortressVersion&&Math.abs(t.x-s.x)<s.extentX&&Math.abs(t.y-s.y)<s.extentY);
  if(!site?.fortressVersion)return[{...point(t),action:o.type==='climb'?'climb':null,wallId:o.type==='climb'?t.id:null}];
  const ex=site.extentX||site.radius-74,ey=site.extentY||site.radius-74,inside=p=>Math.abs(p.x-site.x)<ex-20&&Math.abs(p.y-site.y)<ey-20;
  const finish=route=>{const gate=(site.gates||[]).map(id=>g.world.objects.get(id)).find(w=>w?.defenseRing===3),last=route.at(-1)||a;if(gate&&t.y<gate.y-20&&last.y>gate.y+20){const w=LIGHT.has(a.species)?(site.innerAccess||[]).map(id=>g.world.objects.get(id)).filter(w=>w&&!w.dead).sort((x,y)=>dist(last,x)-dist(last,y))[0]:null;if(w)route.push({x:w.x,y:w.y+44},{x:w.x,y:w.y-44,action:'cross',wallId:w.id});else route.push({x:gate.x,y:gate.y+44},{x:gate.x,y:gate.y-44,gateId:gate.id})}route.push({...point(t),formationBounds:inside(t)?{x:site.x,y:site.y,ex:ex-28,ey:ey-28}:null});return route};
  if(o.type==='climb')return[{...point(t),action:'climb',wallId:t.id}];
  if(inside(a)||!inside(t))return finish([]);
  if(LIGHT.has(a.species)){
   const routes=(site.climbRoutes||[]).map(ids=>ids.map(id=>g.world.objects.get(id)).filter(w=>w&&!w.dead));const viable=routes.filter(r=>r.length).sort((r,s)=>dist(a,r[0])-dist(a,s[0]));
   if(viable.length){const route=[];for(const w of viable[0]){const side=Math.sign(w.x-site.x),half=(w.w||20)/2;route.push({x:w.x+side*(half+24),y:w.y});route.push({x:w.x-side*(half+24),y:w.y,action:'cross',wallId:w.id});}return finish(route)}
  }
  const gates=(site.gates||[]).map(id=>g.world.objects.get(id)).filter(w=>w&&w.defenseRing!==3).sort((x,y)=>dist(a,x)-dist(a,y));const gate=gates[0];if(!gate)return[{...point(a),blocked:true}];const side=Math.sign(gate.y-site.y)||1;return finish([{x:gate.x,y:gate.y+side*45},{x:gate.x,y:gate.y-side*45,gateId:gate.id}]);
 }
 startTransition(a,w,exit,top=false,stairs=null){
  const g=this.g;if(!w||w.dead||!w.walkable&&!w.climbAccess||!stairs&&!LIGHT.has(a.species))return false;
  if(this.occupancyAt!==g.time){this.occupancyAt=g.time;this.climbOccupancy=new Map();for(const p of g.apes)if(p.siegeTransition)this.climbOccupancy.set(p.siegeTransition.wallId,(this.climbOccupancy.get(p.siegeTransition.wallId)||0)+1)}const occupants=this.climbOccupancy.get(w.id)||0;if(!stairs&&occupants>=3)return false;
  if(!stairs&&!g.navigation.clearSegment(a.x,a.y,exit.x,exit.y,9,'ape',w.id)){this.status='Climb exit blocked';return false}
  a.siegeTransition={wallId:w.id,from:point(a),to:point(exit),height:w.walkHeight||w.visualHeight||60,elapsed:0,duration:stairs?2:1.8+(w.visualHeight||60)/70,top,stairs:stairs?.id};if(!stairs)this.climbOccupancy.set(w.id,occupants+1);a.climbingWallId=w.id;a._nav=null;return true;
 }
 detach(a){const t=a.siegeTransition;if(t){const candidates=[t.from,t.to,point(a)];const p=candidates.find(p=>!this.g.navigation.blocked(p.x,p.y,9,a.id.startsWith('human')?'human':'ape'))||this.g.findOpen(a.x,a.y);a.x=p.x;a.y=p.y}delete a.siegeTransition;delete a.climbingWallId;delete a.onWallId;delete a.wallNode;a.elevation=0;a.wallClimbHeight=0;}
 transition(a,dt){const t=a.siegeTransition;if(!t)return false;const w=this.g.world.objects.get(t.wallId);if(!w||w.dead||a.hp<=0){const lift=altitude(a);this.detach(a);if(lift>35&&a.hp>0)this.g.hurt(a,(lift-35)*.25,{x:a.x,y:a.y,type:'fall'});return false}
  t.elapsed+=dt;const u=Math.min(1,t.elapsed/t.duration);a.moving=true;
  if(t.stairs||t.top){a.x=t.from.x+(w.x-t.from.x)*u;a.y=t.from.y+(w.y-t.from.y)*u;a.elevation=t.height*u}
  else{const p=u<.5?u*2:(u-.5)*2;a.x=u<.5?t.from.x+(w.x-t.from.x)*p:w.x+(t.to.x-w.x)*p;a.y=u<.5?t.from.y+(w.y-t.from.y)*p:w.y+(t.to.y-w.y)*p;a.elevation=t.height*(u<.5?p:1-p)}a.wallClimbHeight=a.elevation;
  if(u===1){if(!t.top&&!t.stairs&&this.g.navigation.blocked(t.to.x,t.to.y,9,'ape')){this.detach(a);delete a.siegeRoute;return true}delete a.siegeTransition;delete a.climbingWallId;if(t.top||t.stairs){a.x=w.x;a.y=w.y;a.onWallId=w.id;a.wallNode=w.id;a.elevation=t.height;a.wallClimbHeight=t.height}else{a.x=t.to.x;a.y=t.to.y;a.elevation=0;a.wallClimbHeight=0}}
  return true;
 }
 advance(a){delete a.armyOrder.heading;a.armyOrder.index++;delete a.siegeRoute;delete a._orderStall;delete a._siegeProgress;}
 walkTop(a,target,dt){const g=this.g,start=g.world.objects.get(a.onWallId),end=g.world.objects.get(target.onWallId),site=g.world.sites.get(start?.siteId);if(!start||!end||!site||start.siteId!==end.siteId)return false;
  const key='wall:'+start.id+':'+end.id+':'+g.world.navRevision;let path=this.pathCache.get(key);if(!path){const queue=[[start]],seen=new Set([start.id]);path=[];while(queue.length&&seen.size<=256){const route=queue.shift(),w=route.at(-1);if(w.id===end.id){path=route.map(w=>w.id);break}for(const n of this.wallNeighbors(w,site))if(!seen.has(n.id)){seen.add(n.id);queue.push([...route,n])}}this.pathCache.set(key,path);if(this.pathCache.size>256)this.pathCache.delete(this.pathCache.keys().next().value)}if(!path.length)return false;const next=path.length>1?g.world.objects.get(path[1]):target,d=dist(a,next),step=Math.min(d,a.speed*.6*dt);if(d>1){a.x+=(next.x-a.x)/d*step;a.y+=(next.y-a.y)/d*step;a.dir=Math.atan2(next.y-a.y,next.x-a.x);a.moving=true}if(d<3&&path.length>1){a.onWallId=next.id;a.wallNode=next.id}return true;
 }
 followRoute(a,o,dt){
  const g=this.g,orderKey=o.id+':'+g.world.navRevision+(o.targetKind&&o.targetKind!=='object'?':'+Math.floor(o.x/32)+','+Math.floor(o.y/32):'');if(!a.siegeRoute||a.siegeRoute.key!==orderKey){const key=a.species+':'+Math.floor(a.x/140)+','+Math.floor(a.y/140)+':'+orderKey;let path=this.pathCache.get(key);if(!path){path=this.routeFor(a,o);this.pathCache.set(key,path);if(this.pathCache.size>256)this.pathCache.delete(this.pathCache.keys().next().value)}a.siegeRoute={key:orderKey,steps:path.map(p=>({...p})),index:0}}
  const route=a.siegeRoute,p=route.steps[route.index];if(!p)return true;
  if(p.gateId){const gate=g.world.objects.get(p.gateId);if(gate?.solid&&!gate.dead){this.groups[a.species].blocked='gate';return false}}
  if(p.blocked){this.groups[a.species].blocked='route';return false}
  if(p.action&&!LIGHT.has(a.species)){this.groups[a.species].blocked='climber required';return false}if(p.action){const wall=g.world.objects.get(p.wallId);if(!wall||wall.dead||wall.hp<=0){route.index++;return false}if(dist(a,wall)<Math.max(wall.w||20,wall.h||20)/2+42){if(this.startTransition(a,wall,p,p.action==='climb'))route.index++;else if(this.status==='Climb exit blocked')this.groups[a.species].blocked='climb exit';return false}}
  const last=route.index===route.steps.length-1,slot=a.armyOrder.slot,spacing=a.armySpread?46:34,side=(slot%9-Math.min(8,(this.groups[a.species]?.members.length||1)-1)/2)*spacing,row=Math.floor(slot/9);
  let goal=point(p);if(last&&!o.targetId){const angle=a.armyOrder.heading??(a.armyOrder.heading=Math.atan2(o.y-a.y,o.x-a.x));goal={x:p.x-Math.sin(angle)*side-Math.cos(angle)*row*32,y:p.y+Math.cos(angle)*side-Math.sin(angle)*row*32};const b=p.formationBounds;if(b){goal.x=clamp(goal.x,b.x-b.ex,b.x+b.ex);goal.y=clamp(goal.y,b.y-b.ey,b.y+b.ey)}if(g.navigation.blocked(goal.x,goal.y,10,'ape'))goal=point(p)}
  if(o.type==='shield'&&a.armyOrder.formation&&last){const f=a.armyOrder.formation;goal={x:p.x-Math.sin(f.heading)*f.side-Math.cos(f.heading)*f.back,y:p.y+Math.cos(f.heading)*f.side-Math.sin(f.heading)*f.back};if(g.navigation.blocked(goal.x,goal.y,10,'ape'))goal=point(p)}
  const d=dist(a,goal),arrived=last&&o.targetId?g.objectDistance(a,this.resolve(o)||o)<29:d<(last?26:6);if(arrived){route.index++;delete a._orderStall;return route.index>=route.steps.length}
  const speed=o.type==='shield'?Math.min(a.speed,68)/(a.shield?.hp>0?.9:1):a.speed*(o.type==='charge'?1.35:1);a.target=goal;g.move(a,goal.x-a.x,goal.y-a.y,speed,dt);
  if(!a._siegeProgress||dist(a,a._siegeProgress)>12){a._siegeProgress={...point(a),at:g.time};delete a._orderStall}else if(g.time-a._siegeProgress.at>10){this.groups[a.species].blocked='route';a._orderStall=true}
  return false;
 }
 attack(a,t,kind,dt){const g=this.g,d=kind==='object'?g.objectDistance(a,t):dist(a,t);
  if(a.species==='capuchin'&&d>42&&d<=168&&kind!=='vehicle'&&this.throwAt(a,t,kind))return true;
  if(!this.meleeAllowed(a,t))return false;
  if(d>(kind==='vehicle'?43:30))return false;
  if(!this.clearRay(a,t,{ignore:kind==='object'?t.id:null}))return false;a.dir=Math.atan2(t.y-a.y,t.x-a.x);
  if(a.attackCD<=0){a.attackCD=.75;g.animateAttack(a,t,kind==='object'?'overhead':'slam');let damage=g.apeDamage(a,kind==='vehicle'?24:20);if(a.species==='gorilla')damage*=1.5;else if(a.species==='orangutan')damage*=1.25;else if(a.species==='capuchin')damage*=.5;if(kind==='object')g.damageObject(t,damage,a);else g.hurt(t,damage,a)}return true;
 }
 attackArea(a,o,dt){
  const g=this.g;if(!a.armyOrder.areaArrived){if(this.followRoute(a,o,dt)){a.armyOrder.areaArrived=true;delete a.siegeRoute}return true}
  if(g.time>=(a._areaThink||0)){a._areaThink=g.time+.35;let target=g.humanGrid.nearest(o.x,o.y,190,8,h=>h.hp>0&&this.visibleTarget(h))[0],kind='human';
   if(!target){target=g.vehicleGrid.nearest(o.x,o.y,190,4,v=>v.hp>0&&this.visibleTarget(v))[0];kind='vehicle'}
   if(!target){target=g.world.getObjects(o.x,o.y,190).filter(t=>!t.dead&&t.hp>0&&this.known(t)&&!['tree','rock','berry','stairs','apeBuilding'].includes(t.type)&&t.type!=='gateControl'&&!(t.type==='gate'&&!t.solid)&&dist(t,o)<190).sort((x,y)=>dist(a,x)-dist(a,y))[0];kind='object'}
   a.areaTarget=target?{id:target.id,kind}:null;
  }
  const ref=a.areaTarget,target=ref?this.resolve({targetId:ref.id,targetKind:ref.kind}):null;
  if(target?.hp>0&&dist(target,o)<=210){if(!this.attack(a,target,ref.kind,dt))this.followRoute(a,{id:o.id+':'+target.id,type:'attack',targetId:target.id,targetKind:ref.kind,...point(target)},dt)}
  else{a.areaTarget=null;if(dist(a,o)>70)this.followRoute(a,o,dt);else a.moving=false}return true;
 }
 updateApe(a,dt){
  this.releaseThrow(a);this.updateRally(a);if(this.transition(a,dt))return true;
  if(a.onWallId&&a.armyOrder){const w=this.g.world.objects.get(a.onWallId);if(!w||w.dead)this.detach(a)}
  if(!a.armyOrder)return this.capuchin(a,dt);
  const group=this.groups[a.species];if(!group||group.id!==a.armyOrder.group){this.clearActor(a);return false}
  if(group.blocked||a.armyOrder.index>(group.readyIndex||0)){a.moving=false;return true}let o=group.orders[a.armyOrder.index];if(!o){a.state='hold';return true}
  if(o.type==='rally'){const prior=this.selected;this.selected=[a.species];if(a===group.members.map(id=>this.g.apesById.get(id)).find(p=>p?.hp>0)){if(!this.rally()){group.blocked='rally';this.selected=prior;return true}}this.selected=prior;this.advance(a);return true}
  if(o.type==='follow'){const k=this.g.king;this.g.move(a,k.x+a.offsetX-a.x,k.y+a.offsetY-a.y,a.speed,dt);return true}
  let target=o.targetId?this.resolve(o):null;
  if(o.targetId&&(!target||target.hp<=0||target.type==='gate'&&!target.solid)){if(o.type==='assault'&&target){const s=this.g.world.sites.get(o.siteId);const t=this.g.humanGrid.nearest(s.x,s.y,s.radius,6,h=>h.siteId===s.id&&this.visibleTarget(h))[0];if(t){o={...o,targetId:t.id,targetKind:'human',...point(t)};target=t}else{this.advance(a);return true}}else{this.advance(a);return true}}
  if(target&&(o.targetKind==='human'||o.targetKind==='vehicle')){if(this.visibleTarget(target)){o.x=target.x;o.y=target.y;a._lostTargetAt=this.g.time}else if(this.g.time-(a._lostTargetAt??group.started)>5){group.blocked='target';return true}else target=null}
  if(a.onWallId&&!a.siegeTransition){if(target&&this.meleeAllowed(a,target)&&this.attack(a,target,o.targetKind,dt))return true;if(target?.onWallId&&this.walkTop(a,target,dt))return true;if(o.type==='climb'){this.advance(a);return true}const w=this.g.world.objects.get(a.onWallId),s=this.g.world.sites.get(w?.siteId),side=Math.sign(a.x-(s?.x||0))||1;if(w){const exit={x:w.x-side*((w.w||20)/2+24),y:w.y};a.siegeTransition={wallId:w.id,from:point(a),to:exit,height:altitude(a),elapsed:1.8,duration:3.6,top:false};delete a.onWallId;return true}}
  if(o.type==='attackArea')return this.attackArea(a,o,dt);
  if(target&&['attack','sabotage','assault'].includes(o.type)&&this.attack(a,target,o.targetKind,dt))return true;
  if(['charge','defend','hold'].includes(o.type)){const threat=this.g.humanGrid.nearest(a.x,a.y,o.type==='charge'?90:65,4,h=>this.meleeAllowed(a,h)&&this.clearRay(a,h))[0];if(threat){if(this.attack(a,threat,'human',dt))return true;if(o.type!=='hold'&&(o.type==='charge'||dist(threat,o)<160)){this.g.move(a,threat.x-a.x,threat.y-a.y,a.speed,dt);return true}}}
  if(o.type==='hold'||o.type==='defend'){if(dist(a,o)>60)this.followRoute(a,o,dt);return true}
  if(this.followRoute(a,o,dt)){if(!o.targetId||o.type==='climb'){this.advance(a);a.target=point(a)}else if(target&&!this.attack(a,target,o.targetKind,dt)){delete a.siegeRoute}}
  return true;
 }
 report(observer,target,tower=false){
  const g=this.g;if(!finite(target))return null;const key=(observer.siteId||'field')+':'+Math.floor(target.x/240)+','+Math.floor(target.y/240);let r=this.reports.find(x=>x.key===key);const old=r?point(r):null;
  if(!r){r={id:'sighting-'+ ++this.serial,key,recipients:[],regionalSentAt:-100};this.reports.push(r);if(this.reports.length>64)this.reports.shift()}
  if(g.time-(r.at??-100)<.35)return r;
  Object.assign(r,{x:target.x,y:target.y,elevation:altitude(target),at:g.time,reporter:observer.id,siteId:observer.siteId,estimate:Math.max(1,g.apeGrid.near(target.x,target.y,150).length),confidence:1,visible:true,direction:old?Math.atan2(target.y-old.y,target.x-old.x):target.dir||0});this.stats.reports++;
  for(const h of g.humanGrid.near(observer.x,observer.y,300))this.receive(h,r);
  const site=g.world.sites.get(observer.siteId);if(site){site.lastSightingAt=g.time;site.siegeStage=Math.max(site.siegeStage||0,r.estimate>=25?3:2);if(tower&&!site.alarmDown){const alarm=site.objects.map(id=>g.world.objects.get(id)).find(o=>o?.type==='alarm'&&!o.dead);if(alarm){if(!site.alarm)g.sound('alarm',1,observer.x);site.alarm=true;site.alarmUntil=g.time+35;alarm.active=true}}}
  if((observer.hasRadio||tower)&&!site?.radioDown&&g.time-r.regionalSentAt>3){r.regionalSentAt=g.time;this.deliveries.push({id:r.id,at:g.time+1.5,source:observer.id,siteId:observer.siteId});if(this.deliveries.length>32)this.deliveries.shift()}
  return r;
 }
 receive(h,r){if(h.hp<=0)return;h.lastX=r.x;h.lastY=r.y;h.reportId=r.id;h.reportAt=r.at;h.dir=Math.atan2(r.y-h.y,r.x-h.x);h.searchTime=18;h.suspicion=Math.max(h.suspicion||0,.6);if(h.state!=='combat'&&h.state!=='radio'&&h.state!=='alarm')h.state='investigate';const id=h.squadId||h.id;if(!r.recipients.includes(id)&&r.recipients.length<128)r.recipients.push(id);h.perceptionTimer=0;const squad=this.g.forces.squads?.get(h.squadId);if(squad&&(!squad.reportAt||r.at>=squad.reportAt)){squad.objective={x:r.x,y:r.y};squad.reportAt=r.at;squad.nextThink=this.g.time;}}
 scanTowers(){const g=this.g;if(g.time<this.nextScan)return;this.nextScan=g.time+.15;const towers=g.world.getObjects(g.king.x,g.king.y,Math.max(1300,g.viewRadius||700)).filter(o=>o.type==='tower'&&!o.dead&&o.powered!==false&&o.staffed!==false);let checks=0;
  for(let i=0;i<Math.min(4,towers.length);i++){const t=towers[this.scanCursor++%towers.length];const angle=t.trackUntil>g.time?t.trackAngle:(t.angle||0)+Math.sin(g.time*(t.sweep||.2))*1.2;
   for(const a of g.apeGrid.nearest(t.x,t.y,t.scanRange||t.lightRange||500,6)){if(checks>=12||g.performance.losRemaining<=48)break;const d=dist(a,t),facing=Math.cos(Math.atan2(a.y-t.y,a.x-t.x)-angle);if(facing<Math.cos(.48)&&d>90)continue;checks++;g.performance.losRemaining--;g.performance.counters.losTests++;if(!this.clearRay({...t,elevation:t.height||95},a))continue;
    const conceal=g.world.concealment?.(a.x,a.y)||0;if(d>250&&conceal>.8){const s=g.world.sites.get(t.siteId);if(s)s.siegeStage=Math.max(s.siegeStage||0,1);continue}t.trackAngle=Math.atan2(a.y-t.y,a.x-t.x);t.trackUntil=g.time+2;this.report(t,a,true);break;
   }
  }this.stats.towerChecks=checks;
 }
 tickReports(){const g=this.g;for(const r of this.reports){const age=g.time-r.at;r.visible=age<.5;r.confidence=clamp(1-(age-3)/17,0,1)}this.reports=this.reports.filter(r=>g.time-r.at<30);
  this.deliveries=this.deliveries.filter(d=>{if(d.at>g.time)return true;const s=g.world.sites.get(d.siteId),report=this.reports.find(r=>r.id===d.id);if(!report||s?.radioDown)return false;for(const h of g.humans)if(h.hp>0&&h.hasRadio&&dist(h,report)<1900&&!g.world.sites.get(h.siteId)?.radioDown)this.receive(h,report);if(s&&g.time-(s.lastRaid||0)>30)g.spawnRaid(s,report,false);return false});
 }
 legacySite(s){if(s.fortressVersion||!s.military)return;const objects=s.objects.map(id=>this.g.world.objects.get(id)).filter(Boolean),gates=objects.filter(o=>o.type==='gate');s.gates=gates.map(o=>o.id);for(const gate of gates){gate.gateState=gate.dead?'destroyed':gate.solid?'closed':'open'}s.fortressVersion=0;/* Existing breachable gates and rear approaches are intentionally retained. */}
 garrison(){const g=this.g;if(g.time<this.nextGarrison)return;this.nextGarrison=g.time+1;for(const s of g.world.getSites(g.king.x,g.king.y,1400)){if(!s.fortressVersion){this.legacySite(s);continue}const people=g.humans.filter(h=>h.hp>0&&h.siteId===s.id&&!h.responseAllocated),stairs=s.stairs.map(id=>g.world.objects.get(id)).filter(o=>o&&!o.dead&&g.world.objects.get(o.wallId)?.hp>0);
   for(let i=0;i<Math.min(people.length,stairs.length*4);i++){const h=people[i];if(!h.wallAssignment&&stairs.length)h.wallAssignment={stairId:stairs[i%stairs.length].id,post:Math.floor(i/stairs.length)};}
   if(s.alarm&&g.time-(s.lastReserveAt||-100)>12&&(s.garrisonReserve||0)>0&&!s.barracksDown){const barracks=s.objects.map(id=>g.world.objects.get(id)).find(o=>o?.type==='barracks'&&!o.dead);if(barracks&&g.forceWeight()+4<g.responseBudget){const n=Math.min(4,s.garrisonReserve,s.strength||0);for(let i=0;i<n;i++){g.makeHuman(barracks.x+55,barracks.y+(i-1.5)*18,s);s.garrisonReserve--;s.strength--;s.reserveCommitted++}s.lastReserveAt=g.time}}
   if(g.time-(s.lastSightingAt||0)>30&&g.time-(s.lastHostileAt||0)>30){s.siegeStage=Math.max(0,(s.siegeStage||0)-1);if(!s.siegeStage)s.alarm=false}
  }
 }
 wallNeighbors(w,site){const key=site.id+':'+w.id+':'+this.g.world.navRevision;if(this.wallCache.has(key))return this.wallCache.get(key);const near=site.walkways.map(id=>this.g.world.objects.get(id)).filter(o=>o&&!o.dead&&o.id!==w.id&&Math.abs((o.walkHeight||0)-(w.walkHeight||0))<8&&dist(o,w)<=Math.max(o.w,o.h,w.w,w.h)/2+48);this.wallCache.set(key,near);if(this.wallCache.size>512)this.wallCache.clear();return near}
 updateHuman(h,dt){if(!h.wallAssignment&&!h.siegeTransition&&!h.onWallId)return false;const g=this.g;if(this.transition(h,dt))return true;const stair=g.world.objects.get(h.wallAssignment?.stairId),wall=g.world.objects.get(h.onWallId||stair?.wallId);if(!stair||stair.dead||!wall||wall.dead){this.detach(h);delete h.wallAssignment;return false}
  h.shootTimer-=dt;h.moving=false;
  if(!h.onWallId){if(dist(h,stair)>18)g.move(h,stair.x-h.x,stair.y-h.y,75,dt);else this.startTransition(h,wall,wall,true,stair);return true}
  h.elevation=wall.walkHeight||wall.visualHeight||80;h.wallClimbHeight=h.elevation;
  if(g.time>=(h._wallThink||0)&&g.performance.think()){h._wallThink=g.time+.25;const candidates=g.apeGrid.nearest(h.x,h.y,g.forces.range(h)*1.15,8);let target=null;for(const a of candidates){const visible=g.lineVisible(h,a);if(visible){target=a;break}if(visible===null)break}h.wallTarget=target?.id;if(target){h.lastSeenAt=g.time;h.targetId=target.id;h.state='combat';this.report(h,target)}else{h.state='search';h.targetId=null}}
  const target=h.wallTarget==='king'?g.king:g.apesById.get(h.wallTarget);if(target&&target.hp>0&&this.clearRay(h,target)){h.dir=Math.atan2(target.y-h.y,target.x-h.x);if(h.shootTimer<=0)g.shoot(h,target);return true}
  const site=g.world.sites.get(wall.siteId),neighbors=site?this.wallNeighbors(wall,site):[];if(!neighbors.length)return true;
  if(!h.wallNext||g.world.objects.get(h.wallNext)?.dead){const reference=h.reportAt&&g.time-h.reportAt<20?{x:h.lastX,y:h.lastY}:site;neighbors.sort((a,b)=>dist(a,reference)-dist(b,reference));h.wallNext=neighbors[(h.wallAssignment.post||0)%neighbors.length].id}
  const next=g.world.objects.get(h.wallNext);if(next){const d=dist(h,next),step=Math.min(d,50*dt);if(d>1){h.x+=(next.x-h.x)/d*step;h.y+=(next.y-h.y)/d*step;h.moving=true}else{h.onWallId=next.id;h.wallNext=null}}
  return true;
 }
 tick(dt){this.updateStones(dt);this.scanTowers();if(this.g.time>=this.nextGroupTick){this.nextGroupTick=this.g.time+.3;this.tickReports();for(const s of SPECIES){const group=this.groups[s];if(!group)continue;const live=group.members.map(id=>this.g.apesById.get(id)).filter(a=>a&&this.eligible(a)&&a.armyOrder?.group===group.id);group.members=live.map(a=>a.id);if(!live.length){delete this.groups[s];continue}const min=Math.min(...live.map(a=>a.armyOrder.index));if(min>0){group.orders.splice(0,min);for(const a of live)a.armyOrder.index-=min}group.readyIndex=live.map(a=>a.armyOrder.index).sort((a,b)=>a-b)[Math.floor((live.length-1)*.15)]||0;if(!group.orders.length){for(const a of live){this.clearActor(a);a.state='hold';a.target=point(a)}delete this.groups[s]}}
   // Scouting is bounded and applies to the selected known local objects only.
   for(const o of this.g.world.getObjects(this.g.king.x,this.g.king.y,Math.max(1000,this.g.viewRadius||700))){if(o.type!=='gateControl'||o.discovered||o.dead)continue;if(this.g.apeGrid.nearest(o.x,o.y,230,4).some(a=>this.clearRay(a,o,{ignore:o.id}))){o.discovered=true;const s=this.g.world.sites.get(o.siteId);if(s)s.known=true}}
  }this.garrison();}
 serialize(){return{version:1,selected:this.selected,unitIds:this.unitIds,groups:this.groups,drafts:this.drafts,presets:this.presets,serial:this.serial,reports:this.reports,deliveries:this.deliveries,stones:this.stones,rallyReady:this.rallyReady}}
 restore(d){if(d?.version===1){this.selected=(d.selected||[]).filter(s=>SPECIES.includes(s));this.unitIds=Array.isArray(d.unitIds)?d.unitIds.filter(id=>this.g.apesById.has(id)):null;this.serial=Number.isFinite(d.serial)?d.serial:0;this.rallyReady=Math.max(0,Number(d.rallyReady)||0);this.presets=(d.presets||[]).slice(0,4).map(p=>p.filter(s=>SPECIES.includes(s)));for(const s of SPECIES){const group=d.groups?.[s];if(group&&Array.isArray(group.members)&&Array.isArray(group.orders)){group.members=group.members.filter(id=>this.g.apesById.has(id));group.orders=group.orders.filter(o=>TYPES.has(o.type)&&finite(o)).slice(0,16);this.groups[s]=group}this.drafts[s]=(d.drafts?.[s]||[]).filter(o=>TYPES.has(o.type)&&finite(o)).slice(0,16)}this.reports=(d.reports||[]).filter(r=>finite(r)&&Number.isFinite(r.at)&&this.g.time-r.at<30).slice(-64);this.deliveries=(d.deliveries||[]).filter(r=>Number.isFinite(r.at)).slice(-32);this.stones=(d.stones||[]).filter(b=>finite(b)&&finite(b.from)&&finite(b.to)&&Number.isFinite(b.duration)&&b.duration>0).slice(-256)}
  for(const a of this.g.apes){this.balance(a);if(a.armyOrder&&!this.groups[a.species])this.clearActor(a);delete a.siegeRoute;if(a.siegeTransition){const t=a.siegeTransition;if(!finite(t.from)||!finite(t.to)||!Number.isFinite(t.duration)||t.duration<=0||dist(t.from,t.to)>180||!this.g.world.objects.get(t.wallId)){delete a.siegeTransition;this.detach(a)}}}
  for(const h of this.g.humans){const t=h.siegeTransition;if(t&&(!finite(t.from)||!finite(t.to)||!Number.isFinite(t.duration)||t.duration<=0||dist(t.from,t.to)>180||!this.g.world.objects.get(t.wallId))){delete h.siegeTransition;this.detach(h);delete h.wallAssignment}if(h.onWallId&&!this.g.world.objects.get(h.onWallId)){this.detach(h);delete h.wallAssignment}}
  for(const o of this.g.world.objects.values())if(o.type==='gate'&&(o.gateState==='open'||o.gateState==='destroyed'||o.dead))o.solid=false;this.g.world.navRevision++;
 }
}
window.ATSSiege=Siege;window.ATSSiegeSpecies=SPECIES;window.ATSSpeciesHealth=HP;window.ATSSiegeAltitude=altitude;
})();
