/* Human doctrine and armored combat use observed positions, finite troops and bounded thinking. */
(function(){
'use strict';
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
const turn=(from,to,step)=>from+clamp(Math.atan2(Math.sin(to-from),Math.cos(to-from)),-step,step);
const ROLES={
 guard:{label:'Guard',hp:70,weapon:'pistol',range:230,speed:1},
 tracker:{label:'Tracker',hp:65,weapon:'rifle',range:330,speed:1.15},
 officer:{label:'Radio officer',hp:80,weapon:'pistol',range:300,speed:1},
 shield:{label:'Shield guard',hp:115,weapon:'shotgun',range:230,speed:.82},
 sniper:{label:'Marksman',hp:58,weapon:'sniper',range:510,speed:.8},
 grenadier:{label:'Grenadier',hp:85,weapon:'rifle',range:310,speed:.95},
 medic:{label:'Field medic',hp:65,weapon:'pistol',range:260,speed:1},
 flanker:{label:'Assault scout',hp:70,weapon:'assault',range:290,speed:1.22},
 gunner:{label:'Heavy gunner',hp:100,weapon:'machine',range:330,speed:.75},
 rifleman:{label:'Military rifleman',hp:85,weapon:'rifle',range:365,speed:1,accuracy:.8,weight:1.5},
 ranger:{label:'Ranger',hp:82,weapon:'assault',range:350,speed:1.3,accuracy:.65,weight:1.5},
 heavy:{label:'Heavy assault gunner',hp:112,weapon:'machine',range:380,speed:.78,accuracy:.85,weight:2},
 engineer:{label:'Combat engineer',hp:80,weapon:'rifle',range:320,speed:1,weight:1.5},
 leader:{label:'Squad leader',hp:90,weapon:'assault',range:380,speed:1.03,accuracy:.78,weight:1.5},
 mortar:{label:'Mortar team',hp:75,weapon:'pistol',range:720,speed:.72,weight:3},
 assault:{label:'Armored assault infantry',hp:150,weapon:'assault',range:390,speed:.95,accuracy:.68,weight:2.5},
 commando:{label:'Commando',hp:110,weapon:'rifle',range:420,speed:1.15,accuracy:.48,weight:3},
 juggernaut:{label:'Juggernaut gunner',hp:180,weapon:'machine',range:410,speed:.6,accuracy:.75,weight:3.5,bodyArmor:.8}
};
const VEHICLES={
 jeep:{label:'Scout jeep',hp:230,speed:95,radius:21,weight:4,capacity:0,range:300,weapon:'rifle',reload:1.2,front:1,side:1},
 command:{label:'Command vehicle',hp:260,speed:82,radius:21,weight:4,capacity:0,range:320,weapon:'rifle',reload:1.1,front:.8,side:1},
 armored:{label:'Armored patrol',hp:400,speed:70,radius:23,weight:6,capacity:0,range:330,weapon:'machine',reload:.35,front:.45,side:.75},
 truck:{label:'Troop truck',hp:250,speed:86,radius:24,weight:4,capacity:12,minCapacity:10,maxCapacity:16,range:0,front:1,side:1},
 apc:{label:'Armored personnel carrier',hp:550,speed:76,radius:25,weight:7,capacity:8,minCapacity:6,maxCapacity:10,range:370,weapon:'machine',reload:.25,front:.3,side:.65},
 ifv:{label:'Infantry fighting vehicle',hp:730,speed:64,radius:27,weight:9,capacity:5,minCapacity:4,maxCapacity:6,range:440,weapon:'machine',reload:.3,front:.3,side:.6,cannonReload:3.1,cannonDamage:48,cannonRadius:62},
 tank:{label:'Main battle tank',hp:1100,speed:50,radius:31,weight:12,capacity:0,range:570,weapon:'machine',reload:.22,front:.2,side:.5,cannonReload:6.5,cannonDamage:135,cannonRadius:108}
};
for(const spec of Object.values(VEHICLES))spec.budget=spec.weight;
const VARIANTS={
 veteran:{vehicleClass:'tank',unlockPopulation:450,label:'Veteran battle tank',hp:1500,weight:16,speed:48,range:620,front:.16,side:.42,cannonReload:6,cannonDamage:160,cannonRadius:116,paint:'#56694a',visualScale:1.03,barrelScale:1.08},
 siege:{vehicleClass:'tank',unlockPopulation:650,label:'Heavy siege tank',hp:1900,weight:20,speed:42,range:680,front:.12,side:.35,cannonReload:6.2,cannonDamage:195,cannonRadius:126,paint:'#a39768',visualScale:1.07,barrelScale:1.22},
 ironclad:{vehicleClass:'tank',unlockPopulation:850,label:'Ironclad assault tank',hp:2400,weight:25,speed:39,range:740,front:.1,side:.3,cannonReload:5.8,cannonDamage:225,cannonRadius:138,paint:'#45515b',visualScale:1.1,barrelScale:1.15},
 sentinel:{vehicleClass:'ifv',unlockPopulation:650,label:'Sentinel fighting vehicle',hp:1050,weight:13,speed:60,range:500,front:.23,side:.48,cannonReload:2.8,cannonDamage:66,cannonRadius:74,paint:'#58747e',visualScale:1.05,barrelScale:1.1}
};
// Merged specs are shared, avoiding new objects in every vehicle update.
for(const [id,variant]of Object.entries(VARIANTS))VARIANTS[id]=Object.freeze({...VEHICLES[variant.vehicleClass],...variant,budget:variant.weight});
const MILITARY_ROLES=['leader','rifleman','rifleman','heavy','ranger','grenadier','medic','engineer','rifleman','sniper','rifleman','mortar','ranger','heavy','rifleman','medic'];
class Forces {
 constructor(game){this.game=game;this.hazards=[];this.squads=new Map();this.platoons=new Map();this.engineerJobs=new Map();this.nextMortarAt=0;if(game.navigation)game.navigation.game=game}
 assign(h,site,requested){
  if(!requested&&h.role&&ROLES[h.role])return;
  const tier=Math.max(site?.tier||1,this.game.tier-1),n=ATSUtil.hash(h.id+this.game.seed)%100;let role=requested||'guard';
  if(!requested){if(tier<=1)role=n<20?'tracker':n<32?'officer':'guard';else if(tier===2)role=n<20?'shield':n<40?'tracker':n<55?'officer':n<70?'medic':'guard';else if(tier===3)role=['shield','sniper','grenadier','medic','officer','flanker','gunner','tracker'][n%8];else role=this.game.population>=450?this.militaryRole(n):['rifleman','rifleman','ranger','heavy','grenadier','medic','engineer','leader'][n%8]}
  const spec=ROLES[role]||ROLES.guard;h.role=ROLES[role]?role:'guard';h.kind=spec.weapon;h.hp=h.maxHp=spec.hp;h.hasRadio=role==='officer'||role==='leader'||h.hasRadio;h.specialAt=this.game.time+4+Math.random()*4;h.flankSide=ATSUtil.hash(h.id)%2?1:-1;h.label=spec.label;h.accuracyMultiplier=spec.accuracy||1;
 }
 range(h){return ROLES[h.role]?.range||(h.kind==='pistol'?230:300)}
 militaryRole(index=0){const slot=((Math.floor(index)%16)+16)%16,population=this.game.population;if(population>=850&&(slot===3||slot===14))return 'juggernaut';if(population>=650&&(slot===4||slot===10))return 'commando';if(population>=450&&(slot===2||slot===8))return 'assault';return MILITARY_ROLES[slot]}
 variantFor(kind,index=0){const population=this.game.population,variants=Object.keys(VARIANTS).filter(id=>VARIANTS[id].vehicleClass===kind&&population>=VARIANTS[id].unlockPopulation).sort((a,b)=>VARIANTS[b].unlockPopulation-VARIANTS[a].unlockPopulation);if(!variants.length)return null;variants.push(null);return variants[((Math.floor(index)%variants.length)+variants.length)%variants.length]}
 vehicleSpec(v){const kind=v.vehicleClass||v.kind||'jeep',variant=VARIANTS[v.variant];return variant?.vehicleClass===kind?variant:VEHICLES[kind]||VEHICLES.jeep}
 armor(h,damage,source){
  if(!source||!['shield','juggernaut'].includes(h.role))return damage;const g=this.game,swarm=g.apeGrid.near(h.x,h.y,52).filter(a=>a.hp>0).length;
  if(h.role==='juggernaut')return swarm<6&&!source.armorPiercing&&source.type!=='shell'?damage*ROLES.juggernaut.bodyArmor:damage;
  const angle=Math.atan2(source.y-h.y,source.x-h.x),frontal=Math.cos(angle-h.dir)>.45;if(frontal&&swarm<4){g.effect('text',h.x,h.y,{text:'BLOCK',life:.45,color:'#abc5cf'});return damage*.42}return damage;
 }
 createSquad(members,objective,options={}){
  const g=this.game,id=options.id||'squad-'+g.nextId++,point={x:objective?.x??members[0]?.x??0,y:objective?.y??members[0]?.y??0},order=options.order||'Search';
  const squad={id,members:members.map(h=>h.id),initialSize:members.length,objective:point,order:order==='Retreat'?'Fallback':order,vehicleId:options.vehicleId||null,siteId:options.siteId||members[0]?.siteId,operationId:options.operationId||null,leaderId:members.find(h=>h.role==='leader'||h.role==='officer')?.id,reportAt:options.reportAt??g.time,nextThink:g.time,cohesionLossUntil:0,density:0};
  this.squads.set(id,squad);
  members.forEach((h,i)=>{h.squadId=id;h.squadSlot=i;h.squadRole=h.role;h.squadObjective={...point};h.squadOrder=squad.order;h.squadVehicleId=squad.vehicleId;h.squadOperationId=squad.operationId;h.squadReportAt=squad.reportAt;h.navCohort=id;h.lastX=point.x;h.lastY=point.y});this.attachPlatoon(squad);return squad;
 }
 createPlatoon(squads,objective,options={}){
 const g=this.game,groups=squads.map(s=>typeof s==='string'?this.squads.get(s):s).filter(Boolean).slice(0,4);if(!groups.length)return null;
 const id=options.id||'platoon-'+g.nextId++,p={id,squads:groups.map(s=>s.id),objective:{...(objective||groups[0].objective)},order:options.order||groups[0].order,operationId:options.operationId||groups[0].operationId,reportAt:g.time,nextThink:g.time};this.platoons.set(id,p);
 groups.forEach((s,i)=>{s.platoonId=id;s.platoonSlot=i;for(const member of s.members){const h=g.humansById.get(member);if(h)h.platoonId=id}});return p;
 }
 attachPlatoon(squad){
 const key=squad.operationId||squad.siteId;if(!key)return;const same=Array.from(this.platoons.values()).find(p=>p.groupKey===key&&p.squads.length<4&&(squad.operationId||distance(p.objective,squad.objective)<100));
 if(same){same.squads.push(squad.id);squad.platoonId=same.id;squad.platoonSlot=same.squads.length-1;for(const id of squad.members){const h=this.game.humansById.get(id);if(h)h.platoonId=same.id}}
 else{const p=this.createPlatoon([squad],squad.objective,{operationId:squad.operationId});p.groupKey=key}
 }
 restorePlatoons(saved=[]){
 for(const record of saved||[]){const squads=(record.squads||[]).map(id=>this.squads.get(id)).filter(Boolean);if(!squads.length)continue;for(const [id,p]of this.platoons)if(p.squads.some(s=>squads.some(q=>q.id===s)))this.platoons.delete(id);const p=this.createPlatoon(squads,record.objective,record);for(const key of ['groupKey','reportAt','observedTargetId','assaultPhase','facing','anchor','nextThink'])if(record[key]!==undefined)p[key]=record[key]}
 }
 sharePlatoonObjective(squad,target,witness){
 const p=this.platoons.get(squad.platoonId),g=this.game;if(!p||p.squads.length<2)return;
 if(target&&!squad.assaultPhase&&this.communicationsAvailable(witness)){p.objective={x:target.x,y:target.y};p.reportAt=g.time;p.observedTargetId=target.id;p.order=squad.order}
 else if(p.observedTargetId&&g.time-p.reportAt<5&&!squad.assaultPhase){squad.objective={...p.objective};squad.reportAt=Math.max(squad.reportAt,p.reportAt)}
 }
 restoreSquads(saved=[]){
  this.squads.clear();this.platoons.clear();this.engineerJobs.clear();if(this.game.navigation)this.game.navigation.game=this.game;const groups=new Map(),records=new Map((Array.isArray(saved)?saved:[]).map(s=>[s.id,s])),g=this.game,people=[...g.humans];for(const s of g.world.sites.values())people.push(...(s.sleepingHumans||[]));
  for(const h of people)if(h.hp>0&&h.squadId){if(!groups.has(h.squadId))groups.set(h.squadId,[]);groups.get(h.squadId).push(h)}
  const slots=new Map(people.map(h=>[h.id,h.squadSlot]));
  for(const [id,members]of groups){members.sort((a,b)=>(a.squadSlot||0)-(b.squadSlot||0));const h=members[0],saved=records.get(id),s=this.createSquad(members,saved?.objective||h.squadObjective||{x:h.lastX,y:h.lastY},{id,order:saved?.order||h.squadOrder,vehicleId:saved?.vehicleId||h.squadVehicleId,operationId:saved?.operationId||h.squadOperationId,reportAt:saved?.reportAt??h.squadReportAt});s.cohesionLossUntil=Math.max(saved?.cohesionLossUntil||0,...members.map(h=>h.cohesionLossUntil||0));s.nextReport=saved?.nextReport||0;for(const key of ['initialSize','density','fallbackPoint','fallbackFacing','fallbackUntil','fallbackComplete','regroupPoint','regroupFacing','counterattackUntil','kingIdentifiedUntil','firingLine','firingFacing','fortificationIds','assaultPhase','assaultFacing','lineBuildQueue'])if(saved?.[key]!==undefined)s[key]=saved[key]}
  for(const h of people){const slot=slots.get(h.id);if(Number.isFinite(slot))h.squadSlot=slot;if(h.engineerJob&&h.hp>0){h.constructing=h.engineerJob;this.engineerJobs.set(h.engineerJob.id,h.engineerJob)}}
  this.nextMortarAt=Math.max(g.time,...this.hazards.filter(h=>h.type==='mortar'&&h.life>0).map(h=>(h.start||0)+2.4));
 }
 onDeath(a){
  const g=this.game;if(a.squadId){const squad=this.squads.get(a.squadId);if(squad?.leaderId===a.id){squad.leaderId=null;squad.cohesionLossUntil=g.time+7;for(const id of squad.members){const h=g.humansById.get(id);if(h&&h.hp>0){h.cohesionLossUntil=g.time+7;h.shootTimer=Math.max(h.shootTimer,.8);h.squadOrder='Regroup'}}g.effect('text',a.x,a.y,{text:'LEADER DOWN',life:1.3,color:'#e9bb8a'})}}
  if(a.engineerJob)this.cancelEngineerJob(a,'engineer lost');
  if(a.id?.startsWith('vehicle')){a.troopsLost=(a.troopsLost||0)+(a.troops||0);a.troops=0;a.dismounting=false;a.cannonTarget=null}
 }
 // Estimate only a visually confirmed concentration. A bounded sample uses the
 // shared LOS budget; blocked forest samples cannot contribute to its estimate.
 observedDensity(observer,target){
  const g=this.game,candidates=g.apeGrid.near(target.x,target.y,220),count=Math.min(12,candidates.length);let visible=0,checked=0;
  for(let i=0;i<count;i++){const a=candidates[Math.floor(i*candidates.length/count)];if(a.hp<=0)continue;const sight=g.lineVisible(observer,a);if(sight===null)break;checked++;if(sight)visible++}
  return checked&&!(checked<count&&g.performance.losRemaining<=0)?Math.round(candidates.length*visible/checked):null;
 }
 communicationsAvailable(observer){
  const g=this.game,site=g.world.sites.get(observer.siteId);if(site?.radioDown)return false;
  if(observer.hasRadio)return true;
  const squad=this.squads.get(observer.squadId);if(squad?.members.some(id=>{const h=g.humansById.get(id);return h?.hp>0&&h.hasRadio&&distance(observer,h)<650&&!g.world.sites.get(h.siteId)?.radioDown}))return true;
  return g.humanGrid.near(observer.x,observer.y,480).some(h=>h.hp>0&&h.hasRadio&&!g.world.sites.get(h.siteId)?.radioDown);
 }
 // Examine a few occupied cells around a confirmed contact rather than sorting
 // every ape for every gun. Each selected center must pass the shared LOS budget.
 selectCluster(observer,target,options={}){
  const g=this.game;if(!target)return null;
  const radius=options.radius||90,range=options.range||this.range(observer),minRange=options.minRange||0;
  const key=[radius,range,minRange,options.deconflict||'',options.support?'support':''].join(':');
  const cached=observer._cluster;if(cached?.key===key&&g.time-cached.time<.65&&distance(cached.origin,target)<100)return cached.point;
  if(!options.budgeted&&!g.performance.think(observer._militaryInitialized?'vehicle':observer.id?.startsWith('heli')?'air':'human'))return null;
  const cells=new Map(),size=Math.max(55,radius);
  for(const a of g.apeGrid.near(target.x,target.y,280)){
   const gap=distance(observer,a);if(a.hp<=0||gap>range||gap<minRange)continue;
   const cell=Math.floor(a.x/size)+','+Math.floor(a.y/size);let bucket=cells.get(cell);
   if(!bucket)cells.set(cell,bucket={representative:a,count:0,x:0,y:0});bucket.count++;bucket.x+=a.x;bucket.y+=a.y;
  }
  const candidates=Array.from(cells.values()).sort((a,b)=>b.count-a.count).slice(0,8);let best=null;
  for(const bucket of candidates){
   const center=bucket.representative,sight=g.lineVisible(observer,center);if(sight===null)break;if(!sight)continue;
   const apes=g.apeGrid.near(center.x,center.y,radius).filter(a=>a.hp>0),count=apes.length;
   if(options.deconflict&&this.hazards.some(h=>['grenade','mortar','airstrike'].includes(h.type)&&h.life>0&&distance(h,center)<(h.radius+radius)*.8))continue;
   // Nearby infantry under pressure gives supporting armor a reason to shift fire.
   const pressure=options.support?g.humanGrid.near(center.x,center.y,125).filter(h=>h.hp>0).length:0;
   const allies=g.humanGrid.near(center.x,center.y,radius*.75).filter(h=>h.hp>0).length;
   const score=count+Math.min(pressure,10)*2-allies*3;
   if(!best||score>best.score)best={id:center.id,x:center.x,y:center.y,count,score,confirmedAt:g.time};
  }
  observer._cluster={key,time:g.time,origin:{x:target.x,y:target.y},point:best};return best;
 }
 supportFor(squad,members){
  const g=this.game,anchor=members.find(h=>h.id===squad.leaderId)||members[0];
  const armor=g.vehicles.filter(v=>v.hp>0&&['tank','apc','ifv','armored'].includes(v.vehicleClass||v.kind)&&distance(anchor,v)<520);
  const friends=g.humanGrid.near(anchor.x,anchor.y,430).filter(h=>h.hp>0&&h.squadId&&h.squadId!==squad.id);
  return {anchor,armor,friends,supported:armor.length>0||friends.length>=4,strength:members.length+friends.length+armor.reduce((n,v)=>n+((v.vehicleClass||v.kind)==='tank'?16:9),0)};
 }
 fallbackPosition(squad,support){
  const g=this.game,anchor=support.anchor,point=squad.objective,angle=Math.atan2(point.y-anchor.y,point.x-anchor.x),rear={x:anchor.x-Math.cos(angle)*180,y:anchor.y-Math.sin(angle)*180};
  const home=g.world.sites.get(squad.siteId),positions=[...support.armor,...support.friends];const barricades=g.world.getObjects?.(anchor.x,anchor.y,460)||[];positions.push(...barricades.filter(o=>o.fortification&&o.team==='human'&&o.hp>0&&!o.dead));if(home&&!home.cleared)positions.push(home);
  const cover=positions.filter(p=>distance(anchor,p)>40&&distance(anchor,p)<500&&(p.x-anchor.x)*Math.cos(angle)+(p.y-anchor.y)*Math.sin(angle)<=30).sort((a,b)=>distance(a,rear)-distance(b,rear))[0];
  if(!cover)return rear;return{x:cover.x-Math.cos(angle)*(cover.fortification?42:0),y:cover.y-Math.sin(angle)*(cover.fortification?42:0)};
 }
 thinkSquad(squad){
  const g=this.game;if(g.time<squad.nextThink||!g.performance.think())return;squad.nextThink=g.time+.5;
  const members=squad.members.map(id=>g.humansById.get(id)).filter(h=>h&&h.hp>0);if(!members.length)return;
  let witness=null,target=null;
  for(const h of members)if(h.state==='combat'&&h.targetId){const a=h.targetId==='king'?g.king:g.apesById.get(h.targetId);if(a?.hp>0&&g.time-(h.lastSeenAt??-100)<2&&distance(h,a)<=this.range(h)+40){const sight=g.lineVisible(h,a);if(sight){witness=h;target=a;break}if(sight===null)break}}
  const previousDensity=squad.density;
  if(target){if(!squad.assaultPhase)squad.objective={x:target.x,y:target.y};squad.reportAt=g.time;const density=this.observedDensity(witness,target);if(density!==null){squad.density=density;if(this.communicationsAvailable(witness))g.recordHordeContact?.(witness,target,density);if(previousDensity>=40&&density<previousDensity*.65){squad.counterattackUntil=g.time+8;squad.fallbackPoint=null;squad.fallbackComplete=false}}squad.observedTargetId=target.id}
  else if(g.time-squad.reportAt>5){squad.density=0;squad.observedTargetId=null}
  this.sharePlatoonObjective(squad,target,witness);const phase=String(squad.assaultPhase||'').toLowerCase(),commanded=['deployment','engineers'].includes(phase),assaulting=phase==='assault';
  const vehicle=g.vehicles.find(v=>v.id===squad.vehicleId&&v.hp>0);squad.vehicle=vehicle;
  const support=this.supportFor(squad,members),density=squad.density,oldOrder=squad.order;if(assaulting&&!Number.isFinite(squad.assaultFacing))squad.assaultFacing=Math.atan2(squad.objective.y-support.anchor.y,squad.objective.x-support.anchor.x);else if(!assaulting)squad.assaultFacing=null;
  squad.supported=support.supported;squad.supportStrength=support.strength;
  const badlyDamaged=members.reduce((n,h)=>n+h.hp,0)<members.reduce((n,h)=>n+h.maxHp,0)*.45||members.length<Math.max(2,(squad.initialSize||members.length)*.4);
  const armyReady=support.supported&&support.strength>=Math.min(52,24+density/30);
  if(commanded)squad.order='Hold Line';
  else if(g.time<squad.cohesionLossUntil)squad.order='Regroup';
  else if(oldOrder==='Fallback'&&squad.fallbackPoint&&!squad.fallbackComplete)squad.order='Fallback';
  else if(target&&density>=300&&!armyReady&&!badlyDamaged)squad.order='Shadow';
  else if(target&&(badlyDamaged||density>=80&&!support.supported)&&!squad.fallbackComplete)squad.order='Fallback';
  else if(assaulting&&!badlyDamaged)squad.order='Attack Settlement';
  else if(target&&((oldOrder==='Fallback'||oldOrder==='Shadow')&&support.supported&&!badlyDamaged||g.time<(squad.counterattackUntil||0))){squad.order='Counterattack';if(oldOrder==='Fallback'||oldOrder==='Shadow')squad.counterattackUntil=g.time+8;squad.fallbackPoint=null;squad.fallbackComplete=false}
  else if(squad.fallbackComplete&&badlyDamaged)squad.order='Hold & Suppress';
  else if(target&&density>=80)squad.order=density>=150&&support.friends.length>=8&&ATSUtil.hash(squad.id)%3===0?'Flank':'Hold & Suppress';
  else if(target&&density>=40)squad.order='Suppress';
  else if(target&&vehicle)squad.order='Protect Vehicle';
  else if(target)squad.order='Advance';
  else if(['Suppress','Retreat','Fallback','Regroup','Advance','Protect Vehicle','Hold & Suppress','Hold Line','Shadow','Counterattack','Flank'].includes(squad.order))squad.order='Search';
  if(squad.order==='Regroup'&&(oldOrder!=='Regroup'||!squad.regroupPoint)){squad.regroupPoint={x:support.anchor.x,y:support.anchor.y};squad.regroupFacing=Math.atan2(squad.objective.y-support.anchor.y,squad.objective.x-support.anchor.x)}
  if(squad.order==='Fallback'){
   if(!squad.fallbackPoint){squad.fallbackPoint=this.fallbackPosition(squad,support);squad.fallbackFacing=Math.atan2(squad.objective.y-support.anchor.y,squad.objective.x-support.anchor.x);squad.fallbackUntil=g.time+8}
   if(distance(support.anchor,squad.fallbackPoint)<45||g.time>=squad.fallbackUntil){squad.fallbackComplete=true;squad.order='Hold & Suppress'}
  }
  if(assaulting&&squad.order==='Attack Settlement'){squad.firingLine=null;squad.firingFacing=null}
  const barricade=this.defensiveBarricade(squad,support.anchor);if(!assaulting&&barricade&&!badlyDamaged&&(!target||squad.order!=='Fallback'&&squad.order!=='Shadow')&&squad.order!=='Regroup'&&squad.order!=='Counterattack'){squad.order='Hold Line';const angle=Math.atan2(squad.objective.y-barricade.y,squad.objective.x-barricade.x);squad.firingLine={x:barricade.x-Math.cos(angle)*42,y:barricade.y-Math.sin(angle)*42};squad.firingFacing=angle;}
  if(target&&['Hold & Suppress','Suppress','Hold','Hold Line','Defend Base','Shadow'].includes(squad.order)&&!squad.firingLine){
   const line=members.filter(h=>['rifleman','shield','guard','leader','officer','assault'].includes(h.role)),front=line.length?line:members;
   squad.firingLine={x:front.reduce((n,h)=>n+h.x,0)/front.length,y:front.reduce((n,h)=>n+h.y,0)/front.length};squad.firingFacing=Math.atan2(target.y-squad.firingLine.y,target.x-squad.firingLine.x);
  }else if(squad.order==='Counterattack'||!barricade&&!commanded&&!target&&g.time-squad.reportAt>5){squad.firingLine=null;squad.firingFacing=null}
  if(target&&density>=40&&g.time>=(squad.nextReport||0)){const radio=members.find(h=>h.hasRadio);const site=g.world.sites.get(radio?.siteId);if(radio&&!site?.radioDown){squad.nextReport=g.time+(density>=150?16:24);g.alert(radio,'radio');if(density>=150){g.effect('text',radio.x,radio.y,{text:density>=300?'ARMY CONTACT':'MASS HORDE CONTACT',life:1.5,color:'#e6bb8c'});g.sound('military',.4,radio.x)}}}
  const anchor=vehicle||support.anchor;squad.anchor={x:anchor.x,y:anchor.y};
  for(const h of members){h.squadOrder=squad.order;h.squadObjective={...squad.objective};h.squadReportAt=squad.reportAt;h.lastX=squad.objective.x;h.lastY=squad.objective.y;h.cohesionLossUntil=squad.cohesionLossUntil;h.accuracyMultiplier=(ROLES[h.role]?.accuracy||1)*(squad.leaderId?.9:1)*(g.tier===5?.86:1);h.navCohort=squad.id}
  if(!target&&distance(anchor,squad.objective)>100)g.navigation.setCohortRoute(squad.id,anchor,squad.objective,10,3);
 }
 formationPoint(h,squad){
  const point=squad.objective,anchor=squad.anchor||h,vehicle=squad.vehicle,assaulting=String(squad.assaultPhase||'').toLowerCase()==='assault'&&squad.order==='Attack Settlement',holding=squad.firingLine&&['Hold & Suppress','Suppress','Hold','Hold Line','Defend Base','Shadow'].includes(squad.order),fallingBack=squad.fallbackPoint&&(squad.order==='Fallback'||squad.fallbackComplete),regrouping=squad.order==='Regroup'&&squad.regroupPoint,angle=regrouping&&Number.isFinite(squad.regroupFacing)?squad.regroupFacing:fallingBack&&Number.isFinite(squad.fallbackFacing)?squad.fallbackFacing:assaulting&&Number.isFinite(squad.assaultFacing)?squad.assaultFacing:holding?squad.firingFacing:Math.atan2(point.y-anchor.y,point.x-anchor.x),front={x:Math.cos(angle),y:Math.sin(angle)},side={x:-front.y,y:front.x},slot=h.squadSlot||0;
  let depth=(Math.floor(slot/4)-1)*32,lateral=((slot%4)-1.5)*(assaulting?16:39);
  if(squad.platoonId&&!['Hold','Hold Line','Defend Base','Suppress','Hold & Suppress','Shadow'].includes(squad.order)){const p=this.platoons.get(squad.platoonId);if(p?.squads.length>1)lateral+=(squad.platoonSlot-(p.squads.length-1)/2)*(assaulting?16:190)}
  if(h.role==='mortar')depth=-245;else if(h.role==='sniper'||h.role==='medic')depth=-115;else if(h.role==='heavy'||h.role==='gunner'||h.role==='juggernaut'||h.role==='grenadier')depth=-70;else if(h.role==='engineer')depth=holding?-65:-20;else if(h.role==='ranger'||h.role==='flanker'||h.role==='commando'){lateral=h.flankSide*(assaulting?32:squad.density>=80?200:145);depth=squad.density>=80?-40:30}
  if(vehicle&&!assaulting){depth-=50;lateral+=(slot%2?1:-1)*42}
  if(squad.order==='Fallback'||squad.fallbackComplete&&squad.fallbackPoint&&squad.order!=='Regroup'){const cover=squad.fallbackPoint||anchor;return{x:cover.x+front.x*Math.min(0,depth)+side.x*lateral,y:cover.y+front.y*Math.min(0,depth)+side.y*lateral}}
  if(squad.order==='Regroup'){const rally=squad.regroupPoint||anchor;return {x:rally.x-front.x*55+side.x*lateral,y:rally.y-front.y*55+side.y*lateral}}
  const battle=squad.density>0,standOff=squad.order==='Shadow'?Math.min(180,this.range(h)*.68):squad.order==='Counterattack'?145:vehicle?260:200,base=assaulting?{x:point.x+front.x*110,y:point.y+front.y*110}:holding?squad.firingLine:battle?{x:point.x-front.x*standOff,y:point.y-front.y*standOff}:point;
  if(squad.order==='Flank')lateral+=(ATSUtil.hash(squad.id)%2?1:-1)*190;
  return {x:base.x+front.x*depth+side.x*lateral,y:base.y+front.y*depth+side.y*lateral};
 }
 combatMovement(h,target,dt,options={}){
  const g=this.game;h.moving=false;if(!target||target.hp<=0)return false;
  h.dir=Math.atan2(target.y-h.y,target.x-h.x);
  const gap=distance(h,target),range=options.range??this.range(h),point=options.point,squad=this.squads.get(h.squadId),order=options.order||squad?.order;
  const strategic=point&&squad&&order===squad.order&&['Fallback','Retreat','Regroup'].includes(order);
  let destination=null,speed=62,elapsed=dt,step=null;
  if(h._shieldFlank?.until>g.time&&!strategic){destination=h._shieldFlank;speed=72}
  else if(strategic){if(distance(h,point)>28){destination=point;speed=order==='Fallback'?83:70}}
  else if(gap>=range){destination=point&&distance(point,target)<range*.85?point:target}
  else if(!(h.attackTimer>0)&&!(h.shootTimer<=0&&!['mortar','sniper'].includes(h.role))){
   // A short move cannot become a continuous kite when an ape follows it.
   step=h._combatStep;
   if(!step||g.time>=step.nextAt){
    const close=gap<38,reposition=point&&distance(h,point)>30;
    if(close||reposition){const dx=close?h.x-target.x:point.x-h.x,dy=close?h.y-target.y:point.y-h.y,d=Math.hypot(dx,dy)||1;step=h._combatStep={x:dx/d*64,y:dy/d*64,remaining:16,until:g.time+.28,nextAt:g.time+2.4}}
   }
   if(step&&step.remaining>.01&&g.time<step.until){elapsed=Math.min(dt,step.until-g.time);const bursting=['heavy','gunner','juggernaut'].includes(h.role)&&g.time<(h.burstUntil||0);speed=Math.min(bursting?14:58,step.remaining/(elapsed*Math.max(1,ROLES[h.role]?.speed||1)));destination={x:h.x+step.x,y:h.y+step.y}}
  }
  if(destination){const x=h.x,y=h.y;g.move(h,destination.x-h.x,destination.y-h.y,speed,elapsed);if(step)step.remaining=Math.max(0,step.remaining-Math.hypot(h.x-x,h.y-y))}
  // Navigation faces along travel; perception and weapons must keep facing contact.
  h.dir=Math.atan2(target.y-h.y,target.x-h.x);return h.moving;
 }
 updateDoctrine(h,dt,perceived=false){
  const g=this.game,squad=this.squads.get(h.squadId);if(!squad)return false;this.thinkSquad(squad);
  // Individual perception, alarms and radio tells retain the existing state
  // machine. Formation steering runs in the intervals between perception ticks.
  if(!perceived&&h.perceptionTimer<=dt||h.state==='radio'||h.state==='alarm')return false;
  if(!perceived){h.perceptionTimer-=dt;h.shootTimer-=dt;h.attackTimer=Math.max(0,(h.attackTimer||0)-dt)}h.moving=false;
  const target=h.targetId==='king'?g.king:g.apesById.get(h.targetId),point=this.formationPoint(h,squad);
  if(h.state==='combat'&&target?.hp>0){
   this.combatMovement(h,target,dt,{point,order:squad.order});const gap=distance(h,target);
   if(h.role!=='mortar'&&h.shootTimer<=0&&gap<this.range(h)&&g.time>=(h._fireCheck||0)){
    h._fireCheck=g.time+.14;const sight=g.lineVisible(h,target);if(sight){
     if(['heavy','gunner','juggernaut'].includes(h.role)){if(g.time>=(h.burstUntil||0)&&g.time>=(h.burstRestUntil||0)){h.burstUntil=g.time+(squad.density>=40?(g.tier===5?4.2:3.1):1.8);h.burstRestUntil=h.burstUntil+1.25}if(g.time<h.burstUntil)g.shoot(h,target)}else if(h.role!=='sniper')g.shoot(h,target);
    }else if(sight===false){h.targetId=null;h.state='search';h.searchTime=18}
   }
  }else{
   h.searchTime=Math.max(0,(h.searchTime||0)-dt);if(distance(h,point)>28)g.move(h,point.x-h.x,point.y-h.y,70,dt);else h.dir+=dt*.5;
  }
  return true;
 }
 defensiveBarricade(squad,anchor){
 const world=this.game.world,objects=world.getObjects?.(anchor.x,anchor.y,310)||[];return objects.filter(o=>o.fortification&&o.team==='human'&&o.solid&&o.hp>0&&!o.dead&&distance(anchor,o)<220).sort((a,b)=>distance(a,anchor)-distance(b,anchor))[0]||null;
 }
 startEngineerJob(h,options={}){
 const g=this.game;if(h.role!=='engineer'||h.hp<=0||h.engineerJob)return null;const kind=options.kind||'basic',duration=kind==='heavy'?7:kind==='basic'?4+(ATSUtil.hash(h.id+g.nextId)%3):5;
 const job={...options,id:options.id||'engineer-job-'+g.nextId++,engineerId:h.id,x:options.x??h.x+Math.cos(h.dir)*30,y:options.y??h.y+Math.sin(h.dir)*30,kind,type:kind==='searchlight'?'tower':'wall',team:'human',siteId:options.siteId||h.siteId,startedAt:g.time,remaining:duration,duration,status:'building'};
 h.engineerJob=h.constructing=job;this.engineerJobs.set(job.id,job);h.animation={kind:'build',start:g.time,duration};return job.id;
 }
 cancelEngineerJob(h,reason='under attack'){
 const job=h.engineerJob||h.constructing;if(!job)return;job.status='interrupted';job.reason=reason;h.lastEngineerJob={...job};this.engineerJobs.delete(job.id);h.engineerJob=h.constructing=null;h.specialAt=this.game.time+6;
 }
 prepareLine(squad,options={}){
 const g=this.game,members=squad.members.map(id=>g.humansById.get(id)).filter(h=>h?.hp>0),engineers=members.filter(h=>h.role==='engineer'&&!h.engineerJob);if(!members.length)return [];
 const anchor=options.x!==undefined?options:members.find(h=>h.id===squad.leaderId)||members[0],angle=options.angle??Math.atan2(squad.objective.y-anchor.y,squad.objective.x-anchor.x),front={x:Math.cos(angle),y:Math.sin(angle)},side={x:-front.y,y:front.x};
 // Separate sections leave an infantry passage in the center and usable flanks.
 const placements=[-88,88,-194,194].map(lateral=>({x:anchor.x+front.x*36+side.x*lateral,y:anchor.y+front.y*36+side.y*lateral,angle:angle+Math.PI/2,kind:options.kind||'basic',w:Math.abs(side.x)>.7?82:18,h:Math.abs(side.x)>.7?18:82,siteId:squad.siteId,operationId:squad.operationId,squadId:squad.id,temporary:options.temporary,expiresAt:options.expiresAt})).filter(p=>!g.world.blocked(p.x,p.y,17)&&!g.world.fortificationAt?.(p.x,p.y,40));
 squad.lineBuildQueue=placements;engineers.forEach((h,i)=>{if(placements[i])this.startEngineerJob(h,placements[i])});return placements;
 }
 updateEngineer(h,dt){
 const g=this.game;if(h.role!=='engineer')return false;
 if(h.engineerJob||h.constructing){
  const job=h.engineerJob||h.constructing;h.engineerJob=h.constructing=job;
  const withdrawing=h.state==='retreat'||['Fallback','Retreat'].includes(h.squadOrder)||['Fallback','Retreat'].includes(this.squads.get(h.squadId)?.order),workRadius=Math.max(job.w||82,job.h||18)/2+32;
  if(withdrawing){this.cancelEngineerJob(h,'withdrawal');return false}
  if(h.hitTimer>0||g.apeGrid.near(h.x,h.y,90).some(a=>a.hp>0)||g.apeGrid.near(job.x,job.y,workRadius).some(a=>a.hp>0)){this.cancelEngineerJob(h,'construction area contested');return false}
  if(distance(h,job)>48){g.move(h,job.x-h.x,job.y-h.y,68,dt);return true}
  job.remaining-=dt;h.moving=false;h.animation={kind:'build',start:g.time,duration:.6};
  if(job.remaining<=0){
   let o=null;if(!g.world.blocked(job.x,job.y,17)){if(g.world.createFortification)o=g.world.createFortification({...job,temporary:job.temporary??false,expiresAt:job.expiresAt});else{const chunk=g.world.chunks.get(Math.floor(job.x/g.world.chunkSize)+','+Math.floor(job.y/g.world.chunkSize));if(chunk)o=g.world._object(chunk,{id:'field-'+g.nextId++,type:'wall',fortification:true,team:'human',kind:job.kind,x:job.x,y:job.y,r:18,w:job.w||82,h:job.h||18,collision:'rect',hp:job.kind==='heavy'?600:300,maxHp:job.kind==='heavy'?600:300,siteId:h.siteId})}}
   job.status=o?'completed':'blocked';job.objectId=o?.id;h.lastEngineerJob={...job};if(o){const squad=this.squads.get(h.squadId);if(squad)(squad.fortificationIds||(squad.fortificationIds=[])).push(o.id);g._lightsAt=-1;g.effect('text',o.x,o.y,{text:job.kind==='searchlight'?'FIELD LIGHT':job.kind==='observation'?'OBSERVATION POST':'BARRICADE READY',color:'#cbbd94',life:1.2});if(job.expiresAt)this.hazards.push({type:'fortification',id:o.id,x:o.x,y:o.y,start:g.time,life:Math.max(1,job.expiresAt-g.time)})}this.engineerJobs.delete(job.id);h.engineerJob=h.constructing=null;h.specialAt=g.time+20;
  }return true;
 }
 if(g.time>h.specialAt&&h.squadId&&['Hold','Hold Line','Suppress','Hold & Suppress','Protect Vehicle','Defend Base'].includes(h.squadOrder)&&!g.apeGrid.near(h.x,h.y,150).length&&g.performance.think()){
  const squad=this.squads.get(h.squadId);if(squad){const existing=this.defensiveBarricade(squad,h);if(!existing){const candidates=this.prepareLine(squad);if(h.engineerJob)return true;if(!candidates.length)h.specialAt=g.time+8}}}
 return false;
 }
 tacticalTarget(h){
  const g=this.game;if(!['rifleman','heavy','gunner','ranger','flanker','leader','sniper','assault','commando','juggernaut'].includes(h.role)||g.time<(h._tacticalThink||0))return;
  const current=h.targetId==='king'?g.king:g.apesById.get(h.targetId);if(!current?.hp)return;
  if(current.shield?.hp>0&&Math.cos(Math.atan2(h.y-current.y,h.x-current.x)-(current.dir||0))>.5&&g.performance.think()){
   h._tacticalThink=g.time+.6;
   for(const a of g.apeGrid.nearest(current.x,current.y,220,5,p=>p.hp>0&&!p.shield?.hp)){const sight=g.lineVisible(h,a);if(sight===null)break;if(sight&&distance(h,a)<this.range(h)){h.targetId=a.id;h.lastSeenAt=g.time;return}}
   if(['ranger','flanker','commando'].includes(h.role)){const angle=(current.dir||0)+(h.flankSide||1)*Math.PI/2;h._shieldFlank={x:current.x+Math.cos(angle)*180,y:current.y+Math.sin(angle)*180,until:g.time+2}}
   else h.shootTimer=Math.max(h.shootTimer,.22);
   return;
  }
  const vehicle=['rifleman','heavy','gunner','leader','assault','juggernaut'].includes(h.role)?g.vehicles.find(v=>v.hp>0&&(v.overrun||v.swarmCount>=8)&&distance(h,v)<360):null;
  if(!vehicle&&(h.role==='rifleman'||h.role==='assault'||h.role==='leader'&&g.exposure<.35)){h._tacticalThink=g.time+.55;return}
  if(!g.performance.think())return;h._tacticalThink=g.time+.55;
  if(h.role==='sniper'||h.role==='leader'){
   const king=g.king,gap=distance(h,king),angle=Math.atan2(king.y-h.y,king.x-h.x);
   const exposed=g.exposure>=.35&&gap>95&&gap<this.range(h)&&Math.cos(angle-h.dir)>.65;
   const sight=exposed?g.lineVisible(h,king):false;
   if(sight){h.kingVisibleSince=h.kingVisibleSince??g.time;const squad=this.squads.get(h.squadId);if(squad)squad.kingIdentifiedUntil=g.time+3;if(h.role==='sniper'&&g.time-h.kingVisibleSince>=1.25){h.targetId='king';h.lastSeenAt=g.time}}
   else if(sight===false)h.kingVisibleSince=null;
  }
  if(['rifleman','heavy','gunner','leader','assault','juggernaut'].includes(h.role)){
   if(vehicle){const threats=g.apeGrid.near(vehicle.x,vehicle.y,90).filter(a=>a.hp>0&&distance(h,a)<this.range(h)).slice(0,6);for(const a of threats){const sight=g.lineVisible(h,a);if(sight===null)break;if(sight){h.targetId=a.id;h.lastSeenAt=g.time;h.clearingVehicleId=vehicle.id;return}}}h.clearingVehicleId=null;
  }
  if(h.role==='ranger'||h.role==='flanker'||h.role==='commando'){
   const nearby=g.apeGrid.near(current.x,current.y,240),choices=[];
   for(let i=0;i<Math.min(10,nearby.length);i++){const a=nearby[Math.floor(i*nearby.length/Math.min(10,nearby.length))];if(a.hp>0&&distance(h,a)<this.range(h))choices.push({a,score:g.apeGrid.near(a.x,a.y,60).length+distance(h,a)/130})}
   choices.sort((a,b)=>a.score-b.score);for(const {a}of choices.slice(0,4)){const sight=g.lineVisible(h,a);if(sight===null)break;if(sight){h.targetId=a.id;h.lastSeenAt=g.time;break}}
  }else if(['heavy','gunner','juggernaut'].includes(h.role)){
   const cluster=this.selectCluster(h,current,{radius:75,range:this.range(h),budgeted:true});if(cluster){h.targetId=cluster.id;h.lastSeenAt=g.time}
  }
 }
 updateMortar(h){
  const g=this.game;if(h.role!=='mortar'||g.tier<5||g.time<h.specialAt||g.time<this.nextMortarAt||!this.communicationsAvailable(h))return;
  const squad=this.squads.get(h.squadId);if(squad)this.thinkSquad(squad);
  const id=squad?.observedTargetId||h.targetId,target=id==='king'?g.king:g.apesById.get(id),confirmed=squad?g.time-squad.reportAt<2:g.time-(h.lastSeenAt??-100)<1;
  if(!target?.hp||!confirmed||squad?.order==='Shadow'||distance(h,target)<210||distance(h,target)>720)return;
  const point=this.selectCluster(h,target,{radius:100,range:720,minRange:210,deconflict:'mortar'});if(!point||point.count<8)return;
  this.hazards.push({id:'mortar-'+g.nextId++,type:'mortar',x:point.x,y:point.y,fromX:h.x,fromY:h.y,start:g.time,life:2.1,fuse:2.1,radius:100,damage:90,owner:h.id});
  h.specialAt=g.time+10+ATSUtil.hash(h.id+Math.floor(g.time))%5;this.nextMortarAt=g.time+2.4;h.animation={kind:'throw',start:g.time,duration:.7};g.sound('mortar',.85,h.x);g.sound('warning',.65,point.x);g.noise(h.x,h.y,650,'mortar');
 }
 update(h,dt){
  const g=this.game;if(!h.role)this.assign(h,g.world.sites.get(h.siteId));h.hitTimer=Math.max(0,(h.hitTimer||0)-dt);if(h.engineerJob&&h.hitTimer>0)this.cancelEngineerJob(h);if(h.hitTimer>.19){h.aiming=null;return true}
  if(!h.squadId&&h.state==='combat'&&g.tier>=3){const allies=g.humanGrid.near(h.x,h.y,140).filter(a=>a.hp>0&&!a.squadId&&a.siteId===h.siteId).slice(0,6);if(!allies.includes(h))allies.push(h);this.createSquad(allies,{x:h.lastX,y:h.lastY},{order:'Search',siteId:h.siteId})}
  if(h.role==='medic'&&g.time>h.specialAt&&g.time>=(h._roleThink||0)&&g.performance.think()){h._roleThink=g.time+.5;const ally=g.humanGrid.near(h.x,h.y,105).find(a=>a!==h&&a.hp>0&&a.hp<a.maxHp*.7);if(ally){ally.hp=Math.min(ally.maxHp,ally.hp+18);h.specialAt=g.time+6;h.animation={kind:'treat',start:g.time,duration:.5};g.effect('text',ally.x,ally.y,{text:'+18',color:'#92cdbb',life:1})}}
  if(h.role==='tracker'&&h.state==='patrol'&&g.time>=(h._roleThink||0)&&g.performance.think()){h._roleThink=g.time+.35;const sound=g.noiseGrid.near(h.x,h.y,1900).find(n=>distance(h,n)<n.radius*1.4);if(sound){h.state='investigate';h.lastX=sound.x;h.lastY=sound.y;h.searchTime=20}}
  if(this.updateEngineer(h,dt))return true;
  this.updateMortar(h);
  if(h.state==='combat'){
   this.tacticalTarget(h);
   const target=h.targetId==='king'?g.king:g.apesById.get(h.targetId);
   if(target?.hp>0){
    if(h.role==='sniper'&&distance(h,target)>85&&distance(h,target)<520){const squad=this.squads.get(h.squadId);if(squad)this.thinkSquad(squad);const sight=g.lineVisible(h,target);if(sight===null)return true;if(!sight){h.aiming=null;h.kingVisibleSince=null;h.state='search';h.searchTime=15;return false}h.dir=Math.atan2(target.y-h.y,target.x-h.x);h.shootTimer-=dt;h.perceptionTimer-=dt;if(!h.aiming&&h.shootTimer<=0){h.aiming={x:target.x,y:target.y,targetId:target.id,start:g.time,duration:1.35,until:g.time+1.35};g.sound('warning',.45,h.x)}if(h.aiming&&g.time>=h.aiming.until){g.shoot(h,{x:h.aiming.x,y:h.aiming.y});h.aiming=null;h.shootTimer=2.8}h.moving=false;if(squad&&['Fallback','Retreat','Regroup'].includes(squad.order))this.combatMovement(h,target,dt,{point:this.formationPoint(h,squad),order:squad.order});return true}
    if(h.role==='grenadier'&&g.time>h.specialAt&&g.time>=(h._roleThink||0)){h._roleThink=g.time+.4;const point=this.selectCluster(h,target,{radius:67,range:330,minRange:105,deconflict:'grenade'});if(point){h.specialAt=g.time+(g.tier===5?Math.max(6.6,8-(g.warIntensity||0)*.22):9);h.animation={kind:'throw',start:g.time,duration:.6};this.hazards.push({id:'grenade-'+g.nextId++,type:'grenade',x:point.x,y:point.y,fromX:h.x,fromY:h.y,start:g.time,fuse:1.8,radius:67,life:1.8,owner:h.id});g.sound('grenade',.5,h.x)}}
    if(h.role==='officer'&&g.time>h.specialAt&&g.exposure<.4){h.specialAt=g.time+24;this.hazards.push({id:'flare-'+g.nextId++,type:'flare',x:h.lastX,y:h.lastY,start:g.time,life:12,radius:150});g.noise(h.lastX,h.lastY,420,'flare');g.notify('A flare lights the last reported position.','red')}
   }
  }else h.aiming=null;
  return this.updateDoctrine(h,dt);
 }
 initVehicle(v,vehicleClass,prepaidTroops){
  const kind=vehicleClass||v.vehicleClass||v.kind||'jeep';v.kind=v.vehicleClass=VEHICLES[kind]?kind:'jeep';const spec=this.vehicleSpec(v);v.navClass=v.vehicleClass;v.label=spec.label;v.radius=spec.radius;v.weight=spec.weight;
  if(!Number.isFinite(v.maxHp))v.maxHp=spec.hp;if(!Number.isFinite(v.hp))v.hp=v.maxHp;
  v.capacity=clamp(v.capacity??spec.capacity,0,spec.maxCapacity||spec.capacity);v.troops=clamp(prepaidTroops??v.troops??0,0,v.capacity);v.mobilityDamage=clamp(v.mobilityDamage??v.components?.mobility??0,0,100);v.weaponDamage=clamp(v.weaponDamage??v.components?.weapon??0,0,100);v.engineDamage=clamp(v.engineDamage??v.components?.engine??0,0,100);v.components={mobility:v.mobilityDamage,weapon:v.weaponDamage,engine:v.engineDamage};
  v.turretDir=v.turretDir??v.dir??0;v.dir=v.dir||0;v.shootTimer=v.shootTimer??1;v.cannonTimer=v.cannonTimer??3;v.phase=v.phase||0;v.swarmCount=v.swarmCount||0;v.overrun=!!v.overrun;v.rotationMultiplier=v.rotationMultiplier??1;v.turretMultiplier=v.turretMultiplier??1;v.accuracyMultiplier=v.accuracyMultiplier??1;v._militaryInitialized=true;return v;
 }
 vehicleDamage(v,damage,source){
  if(!v._militaryInitialized)this.initVehicle(v);const spec=this.vehicleSpec(v),bypass=source?.armorPiercing||source?.type==='shell';
  let frontal=0;if(source)frontal=Math.cos(Math.atan2(source.y-v.y,source.x-v.x)-v.dir);const fraction=bypass||!source?1:frontal>.5?spec.front:frontal<-.5?1:spec.side;
  if(!bypass&&source){const component=frontal<-.5?'engineDamage':Math.abs(frontal)<=.5?'mobilityDamage':'weaponDamage',before=v[component];v[component]=clamp(before+damage*fraction*.16,0,100);if(before<65&&v[component]>=65)this.game.effect('text',v.x,v.y,{text:component==='mobilityDamage'?'TRACKS DAMAGED':component==='engineDamage'?'ENGINE DAMAGED':'WEAPON DAMAGED',life:1.1,color:'#e7b981'})}
  v.components={mobility:v.mobilityDamage,weapon:v.weaponDamage,engine:v.engineDamage};return damage*fraction;
 }
 updateSwarm(v,dt){
  const g=this.game;if(g.time>=(v._swarmThink||0)&&g.performance.think('vehicle')){v._swarmThink=g.time+.3;const apes=g.apeGrid.near(v.x,v.y,v.radius+25).filter(a=>a.hp>0);v.swarmCount=apes.length;for(const a of apes)if(v.vehicleClass==='tank'||v.vehicleClass==='ifv'||v.vehicleClass==='apc'){a.climbingVehicleId=v.id;a.climbUntil=g.time+.4}
   if(v.vehicleClass==='tank'){
    const approaching=g.apeGrid.near(v.x,v.y,150).filter(a=>a.hp>0),sample=Math.min(4,approaching.length);let seen=0,checked=0,x=0,y=0;
    for(let i=0;i<sample;i++){const a=approaching[Math.floor(i*approaching.length/sample)],sight=g.lineVisible(v,a);if(sight===null)break;checked++;if(sight){seen++;x+=a.x;y+=a.y}}
    if(checked===sample){v.approachCount=sample?Math.round(approaching.length*seen/sample):0;v.swarmFront=seen?{x:x/seen,y:y/seen}:null}
   }
  }
  const n=v.swarmCount;v.rotationMultiplier=n>=5?.5:1;v.accuracyMultiplier=(n>=8?1.9:1)*(1+v.weaponDamage*.015);v.turretMultiplier=(n>=12?.16:n>=5?.65:1)*(1-v.weaponDamage*.006);v.overrun=n>=16;
  if(v.overrun){v.cannonTarget=null;v.cannonTimer=Math.max(2.4,v.cannonTimer);v.mobilityDamage=clamp(v.mobilityDamage+dt*4,0,100);v.weaponDamage=clamp(v.weaponDamage+dt*6,0,100);v.engineDamage=clamp(v.engineDamage+dt*4,0,100);g.hurt(v,(12+Math.min(n,35))*dt,{x:v.x,y:v.y,type:'overrun',armorPiercing:true});if(g.time>=(v._sparkAt||0)){v._sparkAt=g.time+.45;g.effect('smash',v.x,v.y,{life:.28,color:'#f7ce89'});g.effect('smoke',v.x,v.y,{life:1,color:'#69716b'})}}
  v.components={mobility:v.mobilityDamage,weapon:v.weaponDamage,engine:v.engineDamage};
 }
 vehicleMove(v,point,dt,reverse=false){
  const g=this.game,spec=this.vehicleSpec(v);if(v._simTier===2&&g.time>(v._swarmThink||0)+.5){v.swarmCount=0;v.overrun=false;v.rotationMultiplier=1;v.turretMultiplier=1-v.weaponDamage*.006;v.accuracyMultiplier=1+v.weaponDamage*.015}if(v.mobilityDamage>=100||v.engineDamage>=100){v.moving=false;return}
  if(g.navigation.interactFortification?.(v,point,v.radius,dt)===false){v.moving=false;return}
  const waypoint=g.navigation.steer(v,point,v.radius,dt);if(distance(waypoint,v)<1){v.moving=false;return}const angle=Math.atan2(waypoint.y-v.y,waypoint.x-v.x),heading=turn(v.dir,angle+(reverse?Math.PI:0),(v.vehicleClass==='tank'?.5:.9)*v.rotationMultiplier*dt),travelHeading=heading+(reverse?Math.PI:0),alignment=Math.max(0,Math.cos(angle-travelHeading));
  const speed=spec.speed*(reverse?.72:1)*(1-v.mobilityDamage*.007)*(1-v.engineDamage*.004)*(v.overrun?.08:1)*alignment;
  if(speed>1)g.navigation.move(v,Math.cos(travelHeading)*64,Math.sin(travelHeading)*64,speed,dt,true);else v.moving=false;v.dir=heading;v.reversing=reverse&&v.moving;
 }
 dismount(v,dt){
  const g=this.game;if(!v.troops||v.hp<=0||g.humans.length>=(g.activeHumanCapacity?.()||220))return;v.dismounting=true;v.unloadTimer=(v.unloadTimer||0)-dt;if(v.unloadTimer>0)return;
  const roles=['leader','rifleman','rifleman','heavy','ranger','grenadier','medic','engineer','rifleman','sniper','rifleman','rifleman','ranger','rifleman','rifleman','medic'],i=v.deployedTroops||0,rear=v.dir+Math.PI,side=i%2?1:-1,p=g.findOpen(v.x+Math.cos(rear)*42+Math.cos(rear+Math.PI/2)*side*20,v.y+Math.sin(rear)*42+Math.sin(rear+Math.PI/2)*side*20,10);
  const h=g.makeHuman(p.x,p.y,g.world.sites.get(v.siteId));this.assign(h,g.world.sites.get(v.siteId),v.troopRoles?.[i]||roles[i%roles.length]);h.responseAllocated=true;h.operationId=v.operationId;h.reported=true;h.state='search';h.searchTime=55;h.lastX=v.target?.x??v.x;h.lastY=v.target?.y??v.y;h.raidTarget=v.target?.id;v.troops--;v.deployedTroops=i+1;v.unloadTimer=.35;
  const squad=this.squads.get(v.dismountSquadId);if(squad){squad.members.push(h.id);squad.initialSize=Math.max(squad.initialSize||0,squad.members.length);h.squadId=squad.id;h.squadSlot=squad.members.length-1;h.squadRole=h.role;h.squadOrder=squad.order;h.squadObjective={...squad.objective};h.squadVehicleId=v.id;h.squadOperationId=squad.operationId;h.squadReportAt=squad.reportAt;h.navCohort=squad.id;h.platoonId=squad.platoonId;if(h.role==='leader')squad.leaderId=h.id}
  else v.dismountSquadId=this.createSquad([h],v.target||v,{order:'Protect Vehicle',vehicleId:v.id,operationId:v.operationId,siteId:v.siteId}).id;
  if(!v.troops){v.dismounting=false;g.effect('text',v.x,v.y,{text:'TROOPS DEPLOYED',life:1.3,color:'#d2c7a3'})}
 }
 acquireVehicleTarget(v){
  const g=this.game,spec=this.vehicleSpec(v);if(g.time<(v._targetThink||0)||!g.performance.think('vehicle'))return;v._targetThink=g.time+.3;
  const perception=g.perceive(v,{x:v.x,y:v.y,dir:v.turretDir,range:spec.range||310,angle:v._targetId?.9:.6});if(perception.pending)return;const target=perception.seen;
  v._targetId=target?.id;if(target){v.lastSeen={x:target.x,y:target.y,time:g.time};v.state=v.state==='raid'?'raid':'combat';if(g.time>=(v._reportAt||0)){v._reportAt=g.time+8;g.addIntel(target.x,target.y,2)}}
 }
 fireShell(v,target,spec){
  const g=this.game,angle=Math.atan2(target.y-v.y,target.x-v.x),fromX=v.x+Math.cos(angle)*(v.vehicleClass==='tank'?53:37),fromY=v.y+Math.sin(angle)*(v.vehicleClass==='tank'?53:37),travel=Math.max(.18,Math.hypot(target.x-fromX,target.y-fromY)/570);
  this.hazards.push({id:'shell-'+g.nextId++,type:'shell',x:fromX,y:fromY,fromX,fromY,targetX:target.x,targetY:target.y,start:g.time,life:travel,duration:travel,speed:570,radius:spec.cannonRadius,damage:spec.cannonDamage,owner:v.id});
  if(v.vehicleClass==='ifv'){v.cannonBurst=Math.max(0,(v.cannonBurst||1)-1);if(!v.cannonBurst)v.cannonBurstPoint=null}
  v.cannonFlash=.3;v.cannonTimer=(v.vehicleClass==='ifv'&&v.cannonBurst>0?.4:spec.cannonReload)*(1+v.weaponDamage*.018);g.effect('muzzle',fromX,fromY,{life:.3,color:'#ffe9aa'});g.effect('smoke',fromX,fromY,{life:1.1,color:'#b4ac89'});g.sound(v.vehicleClass==='tank'?'cannon':'gun',v.vehicleClass==='tank'?1.4:1,v.x);g.noise(v.x,v.y,900,'cannon');if(distance(v,g.king)<800){g.hitFlash=Math.max(g.hitFlash,.16);g.cannonShake=Math.max(g.cannonShake||0,v.vehicleClass==='tank'?6:3)}
 }
 positionTank(v,target,dt){
  const g=this.game;if(v.vehicleClass!=='tank')return false;
  const front=v.swarmFront||target,pressured=!!front&&(v.approachCount>=12||target&&distance(v,target)<230);
  if(v.reverseSupport){if(pressured)v.reverseClearAt=null;else if(!v.reverseClearAt)v.reverseClearAt=g.time+5;else if(g.time>=v.reverseClearAt){v.reverseSupport=null;v.reverseUntil=0;v.reverseComplete=false;v.reverseClearAt=null}}
  if(!front)return false;
  const buddies=g.vehicles.filter(a=>a!==v&&a.hp>0&&a.vehicleClass==='tank'&&distance(a,v)<420&&((v.platoonId&&a.platoonId===v.platoonId)||(v.operationId&&a.operationId===v.operationId)));
  v.platoonCover=buddies.some(a=>a.reversing);
  if(pressured){
   if(!v.reverseSupport){
    const angle=Math.atan2(front.y-v.y,front.x-v.x),rear={x:v.x-Math.cos(angle)*180,y:v.y-Math.sin(angle)*180};
    const allies=g.performance.think('vehicle')?g.humanGrid.near(v.x,v.y,340).filter(h=>h.hp>0&&distance(h,rear)<210):[],ally=allies.sort((a,b)=>distance(a,rear)-distance(b,rear))[0];
    v.reverseSupport=ally?{x:rear.x*.7+ally.x*.3,y:rear.y*.7+ally.y*.3}:rear;v.reverseUntil=g.time+8;v.reverseComplete=false;
   }
   if(!Number.isFinite(v.reverseUntil))v.reverseUntil=g.time+8;
   // Withdraw to one supporting position, then fire there while the swarm advances.
   if(distance(v,v.reverseSupport)<=28||g.time>=v.reverseUntil)v.reverseComplete=true;
   if(!v.reverseComplete)this.vehicleMove(v,v.reverseSupport,dt,true);else v.moving=false;return true;
  }
  if(v.platoonCover){v.moving=false;return true}
  if(target){
   const gap=distance(v,target),angle=Math.atan2(target.y-v.y,target.x-v.x);
   if(gap>350){const side=buddies.length?(ATSUtil.hash(v.id)%2?1:-1)*70:0;this.vehicleMove(v,{x:target.x-Math.cos(angle)*300-Math.sin(angle)*side,y:target.y-Math.sin(angle)*300+Math.cos(angle)*side},dt)}
   return true;
  }return false;
 }
 positionOperationArmor(v,target,dt){
 if(!v.settlementTarget||!v.assaultPhase||!v.target)return false;
 // Guns answer nearby confirmed contacts; the independent assault keeps its
 // staging or supporting position when the crown passes through view.
 const point=v.target;if(target&&distance(v,target)<135){const angle=Math.atan2(target.y-v.y,target.x-v.x);this.vehicleMove(v,{x:point.x-Math.cos(angle)*80,y:point.y-Math.sin(angle)*80},dt,true)}else if(distance(v,point)>65)this.vehicleMove(v,point,dt);else v.moving=false;return true;
 }
 positionCarrier(v,target,dt){
 if(!['apc','ifv'].includes(v.vehicleClass)||!target)return false;const g=this.game,infantry=g.humanGrid.near(v.x,v.y,380).filter(h=>h.hp>0&&(h.squadVehicleId===v.id||h.operationId&&h.operationId===v.operationId));
 const angle=Math.atan2(target.y-v.y,target.x-v.x),gap=distance(v,target),line=infantry.length?{x:infantry.reduce((n,h)=>n+h.x,0)/infantry.length,y:infantry.reduce((n,h)=>n+h.y,0)/infantry.length}:v;
 v.supportingSquadId=infantry[0]?.squadId||null;
 if(v.swarmCount>=8||gap<145){const rear={x:line.x-Math.cos(angle)*85,y:line.y-Math.sin(angle)*85};this.vehicleMove(v,rear,dt,true);return true}
 const standoff=v.vehicleClass==='ifv'?280:225;if(gap>standoff+55){const point={x:target.x-Math.cos(angle)*standoff,y:target.y-Math.sin(angle)*standoff};this.vehicleMove(v,point,dt)}else v.moving=false;return true;
 }
 updateVehicle(v,dt){
  if(!v._militaryInitialized)this.initVehicle(v);const g=this.game,spec=this.vehicleSpec(v);v.shootTimer-=dt;v.cannonTimer-=dt;v.cannonFlash=Math.max(0,(v.cannonFlash||0)-dt);v.phase+=dt;v.moving=false;v.reversing=false;if(v._simTier===2)return true;
  this.updateSwarm(v,dt);if(v.hp<=0)return true;this.acquireVehicleTarget(v);
  let target=v._targetId==='king'?g.king:g.apesById.get(v._targetId);if(target?.hp<=0)target=null;
  // A cannon commits to a warned position rather than tracking a dodge.
  const observed=target&&g.time-(v.lastSeen?.time??-100)<.7;if(!observed)target=null;
  const aimPoint=v.cannonTarget||target;if(aimPoint){const aim=Math.atan2(aimPoint.y-v.y,aimPoint.x-v.x);v.turretDir=turn(v.turretDir,aim,(v.vehicleClass==='tank'?.7:1.5)*v.turretMultiplier*dt)}
  const positioned=this.positionOperationArmor(v,target,dt)||this.positionTank(v,target,dt)||this.positionCarrier(v,target,dt);
  if(!positioned&&v.target&&distance(v,v.target)>85){
   if(!v.dismounting)this.vehicleMove(v,v.target,dt);
  }else if(v.state==='raid')v.state='combat';
  if(v.troops&&(target&&distance(v,target)<300||v.target&&distance(v,v.target)<170||v.dismounting))this.dismount(v,dt);
  if(spec.weapon&&target&&v.weaponDamage<100&&v.shootTimer<=0){const align=Math.cos(Math.atan2(target.y-v.y,target.x-v.x)-v.turretDir);if(align>.88){const kind=v.kind,dir=v.dir;v.kind=spec.weapon;v.dir=v.turretDir;g.shoot(v,target);v.kind=kind;v.dir=dir;v.shootTimer=spec.reload*(1+v.weaponDamage*.018)}}
  if(spec.cannonReload&&v.weaponDamage<100&&!v.overrun){
   if(v.cannonTarget){const aim=Math.atan2(v.cannonTarget.y-v.y,v.cannonTarget.x-v.x);if(g.time>=v.cannonTarget.until&&Math.cos(aim-v.turretDir)>.96){const sight=g.lineVisible(v,v.cannonTarget);if(sight){this.fireShell(v,v.cannonTarget,spec);v.cannonTarget=null}else if(sight===false){v.cannonTarget=null;v.cannonTimer=1.5}}}
   else if(target&&v.cannonTimer<=0&&distance(v,target)>100){const point=v.vehicleClass==='ifv'&&v.cannonBurst>0&&v.cannonBurstPoint?v.cannonBurstPoint:this.selectCluster(v,target,{radius:spec.cannonRadius,range:spec.range,minRange:100,support:true});if(point){const duration=v.vehicleClass==='tank'?1.8:v.cannonBurst>0?.25:1.1;if(v.vehicleClass==='ifv'&&!v.cannonBurst){v.cannonBurst=2;v.cannonBurstPoint={x:point.x,y:point.y,count:point.count}}v.cannonTarget={x:point.x,y:point.y,count:point.count,start:g.time,until:g.time+duration,duration};g.sound('warning',.7,v.x)}}
  }else{v.cannonTarget=null;v.cannonBurst=0;v.cannonBurstPoint=null}
  if(v.engineDamage>55&&g.time>=(v._exhaustAt||0)){v._exhaustAt=g.time+.6;g.effect('smoke',v.x-Math.cos(v.dir)*25,v.y-Math.sin(v.dir)*25,{life:1.6,color:'#555f56'})}
  if(v.engineDamage>=100)g.hurt(v,24*dt,{x:v.x,y:v.y,armorPiercing:true,type:'engine'});
  if(g.time>=(v.nextSound||0)&&distance(v,g.king)<1250){v.nextSound=g.time+2.4;g.sound(v.vehicleClass==='tank'?'tank':v.vehicleClass==='truck'?'truck':['apc','ifv'].includes(v.vehicleClass)?'apc':'rumble',.45,v.x)}
  return true;
 }
 blast(h){
  const g=this.game,tank=h.type==='shell',heavy=tank||h.type==='mortar'||h.type==='airstrike',radius=h.radius;g.effect('wave',h.x,h.y,{life:heavy?.8:.5,range:radius,color:'#efbd7d'});g.effect('smoke',h.x,h.y,{life:heavy?2.4:1.8,color:'#94705b'});g.effect('smash',h.x,h.y,{life:.55,color:'#f1c68c'});g.sound(h.type==='mortar'?'mortarImpact':tank?'cannon':'smash',tank?1.5:1.6,h.x);
  const power=heavy?1:.7;g.effect('explosion',h.x,h.y,{life:heavy?1.1:.85,radius,range:radius,blastType:h.type,blastKind:h.type,power});
  if(heavy)g.effect('tankImpact',h.x,h.y,{life:1.1,radius,range:radius,color:'#f0c789'});
  if(tank&&distance(h,g.king)<600){g.hitFlash=Math.max(g.hitFlash,.35);g.cannonShake=Math.max(g.cannonShake||0,11)}
  for(const a of g.apeGrid.near(h.x,h.y,radius))if(a.hp>0&&g.world.lineClear(h.x,h.y,a.x,a.y)){
   const d=distance(a,h),falloff=1-d/radius*.55;g.hurt(a,(h.damage||72)*falloff,h);
   if(a.hp>0&&g.launchBlastReaction)g.launchBlastReaction(a,h,{radius,power});
   else if(tank&&a.hp>0){const angle=d>.01?Math.atan2(a.y-h.y,a.x-h.x):ATSUtil.hash(a.id)%628/100,push=28+32*falloff,x=a.x+Math.cos(angle)*push,y=a.y+Math.sin(angle)*push,r=a.id==='king'?12:10;if(!g.world.blocked(x,y,r)&&g.navigation.clearSegment(a.x,a.y,x,y,r)){a.x=x;a.y=y}a.knockbackUntil=g.time+.32;a.staggerUntil=g.time+.45;a.animation={kind:'stagger',start:g.time,duration:.4}}
  }
  for(const a of g.humanGrid.near(h.x,h.y,radius))if(a.hp>0&&g.world.lineClear(h.x,h.y,a.x,a.y)){g.hurt(a,tank?(h.damage||72)*.65:60,h);if(a.hp>0&&g.launchBlastReaction)g.launchBlastReaction(a,h,{radius,power})}
  g.blastBodies?.(h,radius,{power});
  g.colonies.damageNearby?.(h.x,h.y,radius,h.damage||72,h);
 }
 tick(dt){
  const g=this.game;for(const h of this.hazards){h.life-=dt;
   if(h.type==='shell'){const t=clamp(1-h.life/h.duration,0,1),x=h.fromX+(h.targetX-h.fromX)*t,y=h.fromY+(h.targetY-h.fromY)*t;
    if(!g.navigation.clearSegment(h.x,h.y,x,y,2)){let lo=0,hi=1;for(let i=0;i<5;i++){const mid=(lo+hi)/2;if(g.navigation.clearSegment(h.x,h.y,h.x+(x-h.x)*mid,h.y+(y-h.y)*mid,2))lo=mid;else hi=mid}h.x+=(x-h.x)*lo;h.y+=(y-h.y)*lo;h.life=0}else{h.x=x;h.y=y}if(h.life<=0)this.blast(h);
   }else if(['grenade','mortar','airstrike'].includes(h.type)&&h.life<=0)this.blast(h);
   else if(h.type==='fortification'&&h.life<=0){const o=g.world.objects.get(h.id);if(o&&!o.dead){o.dead=true;o.solid=false;o.hp=0;g.world.navRevision++;g._lightsAt=-1}}
  }this.hazards=this.hazards.filter(h=>h.life>0);
  if(g.time>=(this.nextSquadTrim||0)){this.nextSquadTrim=g.time+4;for(const [id,s]of this.squads)if(!s.members.some(id=>g.humansById.has(id))&&!Array.from(g.world.sites.values()).some(site=>(site.sleepingHumans||[]).some(h=>h.hp>0&&h.squadId===id)))this.squads.delete(id);for(const [id,p]of this.platoons){p.squads=p.squads.filter(s=>this.squads.has(s));if(!p.squads.length)this.platoons.delete(id)}}
 }
}
window.ATSForces=Forces;window.ATSHumanRoles=ROLES;window.ATSVehicleSpecs=VEHICLES;window.ATSVehicleVariants=VARIANTS;
})();
