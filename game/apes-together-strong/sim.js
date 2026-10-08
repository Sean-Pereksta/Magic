(function(){
'use strict';
const TAU=Math.PI*2, clamp=(v,a,b)=>Math.max(a,Math.min(b,v)), dist=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const angleDiff=(a,b)=>Math.atan2(Math.sin(a-b),Math.cos(a-b));
const FOLLOW=new Set(['follow','charge','hold']), tierNames=['Hunters','Containment','Military response','Regional suppression','Full military mobilization'];
const APE_HP=120,SCOUT_HP=150,YOUNG_HP=60,GROW_UP=35;
const PRIMATE_SPECIES=Object.freeze({
 gorilla:Object.freeze({id:'gorilla',label:'Gorilla'}),
 chimpanzee:Object.freeze({id:'chimpanzee',label:'Chimpanzee'}),
 orangutan:Object.freeze({id:'orangutan',label:'Orangutan'}),
 gibbon:Object.freeze({id:'gibbon',label:'Gibbon'}),
 mandrill:Object.freeze({id:'mandrill',label:'Mandrill'}),
 capuchin:Object.freeze({id:'capuchin',label:'Capuchin'})
});
const PRIMATE_IDS=Object.keys(PRIMATE_SPECIES),PRIMATE_NAMES=new Set(PRIMATE_IDS);
const distance2=(a,b)=>(a.x-b.x)**2+(a.y-b.y)**2;
const now=()=>window.performance?.now?.()||0;
class PerformanceMonitor{
 constructor(){this.enabled=false;this.qualityLevel=0;this.samples=new Float32Array(180);this.cursor=0;this.sampleCount=0;this.slowFrames=0;this.ema=16.7;this.slowTime=0;this.recoveryTime=0;this.counters={};this.beginStep()}
 beginStep(g){this.stepStart=now();this.thinkRemaining=32;this.apeThinkRemaining=20;this.losRemaining=96;this.airReserve=g?.helis.some(h=>h.hp>0)?2:0;this.vehicleReserve=g?.vehicles.some(v=>v.hp>0)?6:this.airReserve;this.heavyLosReserve=this.vehicleReserve?18:0;Object.assign(this.counters,{simulatedApes:0,simulatedHumans:0,abstractApes:0,abstractHumans:0,aiThinks:0,losTests:0,losCacheHits:0,separationPairs:0,blastIntegrations:0,blastCollisionSweeps:0})}
 think(kind='human'){const reserve=kind==='air'?0:kind==='vehicle'?this.airReserve:this.vehicleReserve;if(this.thinkRemaining<=reserve||kind==='ape'&&this.apeThinkRemaining<=0)return false;if(kind==='ape')this.apeThinkRemaining--;this.thinkRemaining--;this.counters.aiThinks++;return true}
 finishStep(g){this.simulationMs=now()-this.stepStart;this.navigationMs=g.navigation.stats?.frameMs||0;Object.assign(this.counters,{activeApes:g.apesById.size,activeHumans:g.humansById.size,projectiles:g.bullets.length,effects:g.effects.length,aStarSearches:g.navigation.stats?.searches||g.navigation.searches||0,navigationCacheHits:g.navigation.stats?.cacheHits||0,navigationQueue:g.navigation.stats?.queueLength||0})}
 frame(ms,simulationMs,renderMs,dt,active){this.frameMs=ms;this.simulationMs=simulationMs;this.renderMs=renderMs;if(!active||dt<=0)return;this.samples[this.cursor++%180]=ms;this.sampleCount=Math.min(180,this.sampleCount+1);if(ms>25)this.slowFrames++;this.ema+=.04*(ms-this.ema);this.slowTime=this.ema>23?this.slowTime+dt:Math.max(0,this.slowTime-dt);this.recoveryTime=this.ema<18?this.recoveryTime+dt:0;if(this.slowTime>3&&this.qualityLevel<5){this.qualityLevel++;this.slowTime=0;this.recoveryTime=0}if(this.recoveryTime>8&&this.qualityLevel>0){this.qualityLevel--;this.recoveryTime=0}}
 snapshot(){return{frameMs:this.frameMs||0,simulationMs:this.simulationMs||0,renderMs:this.renderMs||0,navigationMs:this.navigationMs||0,slowFrames:this.slowFrames,qualityLevel:this.qualityLevel,...this.counters}}
}
class Pool{
 constructor(limit){this.limit=limit;this.free=[];this.created=0}
 take(values){let object=this.free.pop();if(!object){object={};this.created++}for(const key in object)delete object[key];return Object.assign(object,values)}
 release(object){if(this.free.length<this.limit)this.free.push(object)}
}
class Spatial{
 constructor(size=120){this.size=size;this.cells=new Map()}
 rebuild(items){this.cells.clear();for(const a of items){if(a.hp<=0)continue;const k=Math.floor(a.x/this.size)+','+Math.floor(a.y/this.size);if(!this.cells.has(k))this.cells.set(k,[]);this.cells.get(k).push(a)}}
 near(x,y,r){const out=[];for(let i=Math.floor((x-r)/this.size);i<=Math.floor((x+r)/this.size);i++)for(let j=Math.floor((y-r)/this.size);j<=Math.floor((y+r)/this.size);j++){const list=this.cells.get(i+','+j);if(list)for(const a of list)if((a.x-x)**2+(a.y-y)**2<=r*r)out.push(a)}return out}
 nearest(x,y,r,limit=8,accept=()=>true){const out=[],ds=[];for(let i=Math.floor((x-r)/this.size);i<=Math.floor((x+r)/this.size);i++)for(let j=Math.floor((y-r)/this.size);j<=Math.floor((y+r)/this.size);j++){const list=this.cells.get(i+','+j);if(!list)continue;for(const a of list){const d=(a.x-x)**2+(a.y-y)**2;if(d>r*r||!accept(a)||out.length===limit&&d>=ds[limit-1])continue;let n=out.length;while(n>0&&ds[n-1]>d)n--;out.splice(n,0,a);ds.splice(n,0,d);if(out.length>limit){out.pop();ds.pop()}}}return out}
}
class Game{
 constructor(seed,difficulty='survival',hooks={}){
 this.version=1;this.seed=String(seed||'LAUREL');this.difficulty=difficulty;this.hooks=hooks;this.world=new ATSWorld(this.seed);this.navigation=new ATSNavigation(this.world);this.colonies=new ATSSettlements(this);this.forces=new ATSForces(this);this.tactics=window.ATSApeTactics?new ATSApeTactics(this):null;this.corpses=[];this.time=0;this.nextId=1;this.king={id:'king',x:0,y:0,hp:160,maxHp:160,dir:-.5,attackTimer:0,moving:false,lastHit:-30};this.ensureApeAppearance(this.king);
 this.apes=[];this.humans=[];this.vehicles=[];this.helis=[];this.settlements=[];this.bullets=[];this.effects=[];this.events=[];this.apeGrid=new Spatial(64);this.humanGrid=new Spatial(96);this.vehicleGrid=new Spatial(128);this.focusGrid=new Spatial(384);this.noiseGrid=new Spatial(384);this.apesById=new Map();this.humansById=new Map();this.settlementsById=new Map();this.settlementMembers=new Map();this.performance=new PerformanceMonitor();this.bulletPool=new Pool(512);this.effectPool=new Pool(400);this.noisePool=new Pool(128);this.visibilityCache=new Map();this.rescueFocus=[];this.nextCohortAt=0;this.nextSpawnAt=0;this.mode='follow';this.food=0;this.exposure=0;this.tier=1;this.viewRadius=700;this.commandCD=0;this.attackCD=0;this.ended=false;this.deathTimer=0;this.secondTimer=0;this.noises=[];this.trail=[{x:0,y:0}];this.raidTimer=45;this.heliTimer=150;this.seenTimer=0;this.lightCache=[];this.lightTimer=0;this.messages=[];this.hitFlash=0;this.lastContact=-20;this.sprintNoiseTime=0;this.day=1;this.aim={x:1,y:0};this.stats={freed:0,born:0,largestHorde:0,humans:0,structures:0,bases:0,prisons:0,settlements:0,largestSettlement:0,lost:0,highestThreat:1,territory:0};
 this.siege=window.ATSSiege?new ATSSiege(this):null;this.world.ensure(0,0,1400);this.world.reveal(0,0,300);this.notify('The crown is yours. A cage rattles nearby.','gold');
 this.responseStage=1;this.responsePeak=1;this.mobilized=false;this.warPhase=false;this.operationSerial=0;this.observedHorde=0;this.roadblockOperations=[];this.militaryOperations=[];this.nextInterceptAt=0;this.nextReconAt=180;
 }
 get followers(){return this.apes.filter(a=>a.hp>0&&FOLLOW.has(a.state))}
 // A civilization still warrants a regional campaign when its king recruits a
 // smaller field army. Visible threat tiers retain the familiar horde triggers.
 get campaignPopulation(){return this.population}
 get warIntensity(){const n=this.campaignPopulation;return n>=850?5:n>=650?4:n>=450?3:n>=300?2:n>=220?1:0}
 get warProgress(){const i=this.warIntensity;if(i<2)return 0;const edges=[0,220,300,450,650,850,1000];return clamp((this.campaignPopulation-edges[i])/(edges[i+1]-edges[i]),0,1)}
 get responseBudget(){return [250,340,500,950,1450,1950][this.warIntensity]+Math.floor(this.warProgress*[0,0,100,180,220,300][this.warIntensity])}
 activeHumanCapacity(){const n=this.campaignPopulation,i=this.warIntensity;return i<2?(n>=250?260:220):[220,260,320,500,700,900][i]+Math.floor(this.warProgress*[0,0,40,80,100,100][i])}
 get vehicleLimits(){const i=this.warIntensity;return {tank:[3,3,6,10,15,22][i],armored:[4,4,9,16,24,34][i],total:[24,24,32,48,68,90][i]}}
 get directorInterval(){return Math.max(3,[28,15,12,9,6.5,4.5][this.warIntensity]-this.warProgress*1.5-Math.min(1.5,(this.stats.bases||0)*.15+(this.stats.territory||0)*.0005))}
 get operationCapacity(){return [1,2,3,5,7,9][this.warIntensity]}
 get reinforcementInterval(){return [45,35,30,23,17,12][this.warIntensity]-this.warProgress*3}
 activeOperations(channel){return this.militaryOperations.filter(op=>op.status==='active'&&(!channel||op.channel===channel))}
 vehicleRoom(kind,pending=[]){const limits=this.vehicleLimits,live=this.vehicles.filter(v=>v.hp>0),armored=['apc','ifv','armored'];return live.length+pending.length<limits.total&&(kind!=='tank'||live.filter(v=>(v.vehicleClass||v.kind)==='tank').length+pending.filter(k=>k==='tank').length<limits.tank)&&(!armored.includes(kind)||live.filter(v=>armored.includes(v.vehicleClass||v.kind)).length+pending.filter(k=>armored.includes(k)).length<limits.armored)}
 humanWeight(role){return window.ATSHumanRoles?.[role]?.weight||(['gunner','heavy'].includes(role)?2:1)}
 cargoWeight(v){let weight=0;const start=v.deployedTroops||0;for(let i=0;i<(v.troops||0);i++)weight+=v.troopRoles?.[start+i]?this.humanWeight(v.troopRoles[start+i]):2;return weight}
 militaryVehicleProfile(kind,index=0){const variant=this.forces.variantFor?.(kind,index)||null;return{variant,spec:this.forces.vehicleSpec?.({vehicleClass:kind,variant})||window.ATSVehicleSpecs?.[kind]||{hp:230,radius:21,budget:4}}}
 forceWeight(){
 let weight=0;for(const h of this.humans)if(h.hp>0)weight+=this.humanWeight(h.role);
 for(const v of this.vehicles)if(v.hp>0)weight+=(this.forces.vehicleSpec?.(v)?.budget||window.ATSVehicleSpecs?.[v.vehicleClass||v.kind]?.budget||4)+this.cargoWeight(v);
 for(const h of this.helis)if(h.hp>0)weight+=10;return weight;
 }
 updateResponseStage(){
 const n=this.followers.length,minimum=n>=220?5:n>=120?4:n>=60?3:n>=25?2:1;
 const threat=this.time/180+this.stats.freed/65+this.stats.bases*.5+this.stats.humans/45+this.population/170+this.settlements.filter(s=>s.known&&s.population).length*.45;
 this.tier=this.responseStage=Math.max(minimum,clamp(1+Math.floor(threat/2),1,5));this.responsePeak=Math.max(this.responsePeak||1,this.tier);this.stats.highestThreat=Math.max(this.tier,this.stats.highestThreat);
 if(n>=200&&!this.mobilized){this.mobilized=true;this.notify('MILITARY MOBILIZATION — Your tribe can no longer be hidden. Armored units are entering the region.','red');this.sound('mobilization',1.3);this.heliTimer=Math.min(this.heliTimer,12)}
 const war=this.campaignPopulation>=300;if(war&&!this.warPhase){this.notify('WAR PHASE — The regional army is assembling.','red');this.sound('military',1.3)}this.warPhase=war;
 const intensity=this.warIntensity;if(intensity>(this.announcedWarIntensity||0)){this.announcedWarIntensity=intensity;if(intensity>=3){this.notify(['','','','REGIONAL WAR','EMERGENCY MOBILIZATION','TOTAL REGIONAL CAMPAIGN'][intensity]+' — Surviving bases commit larger coordinated forces.','red');this.sound('military',1.1)}}
 }
 responsePackage(site,target,settlementRaid=false,options={}){
 const stage=Math.max(this.responseStage||this.tier,this.warIntensity>=2?5:settlementRaid&&this.population>=500?5:settlementRaid&&this.population>=300?4:1),n=this.campaignPopulation,war=n>=300||settlementRaid&&this.population>=500,capacity=this.world.militaryCapacity?.(site)||{vehicleInventory:site.vehicleInventory||{},armorCapacity:site.armorCapacity||0};
 const inventory=capacity.vehicleInventory||capacity.inventory||site.vehicleInventory||{},vehicles=[];
 const major=options.major&&n>=500,column=stage===5&&(war||(this.operationSerial||0)%4===3),tanks=(this.mobilized||settlementRaid&&this.population>=500)&&(stage===5||n>=200),platoon=n>=650&&(this.operationSerial||0)%5===3;
 const add=(kind,count=1)=>{for(let i=0;i<count;i++)if((inventory[kind]||0)>vehicles.filter(x=>x===kind).length)vehicles.push(kind)};
 if(stage>=2&&!site.depotDown&&!site.fuelDown&&(site.tier<3||capacity.depot!==false&&capacity.fuel!==false)){
 if(column){add('jeep');add('apc',major?(n>=850?5:n>=650?4:2):n>=850?4:n>=650?3:war?2:1);add('truck',n>=850?4:n>=650?3:2);if(tanks)add('tank',major||platoon?(n>=850?5:n>=650?4:2):n>=850?4:n>=650?3:n>=450?2:1);if(major||n>=650)add('ifv',n>=850?2:1);add('command')}
 else if(stage>=4){if(tanks)add('tank');add('apc');if(stage===5&&(this.operationSerial||0)%3===2)add('ifv');if(settlementRaid)add('truck')}
 else add('truck');
 }
 const basePeople=major?(n>=850?96+this.warProgress*24:n>=650?68+this.warProgress*20:40):options.intercept?24+this.warIntensity*6:settlementRaid?24+this.warIntensity*12:war?n>=850?105+this.warProgress*24:n>=650?80+this.warProgress*20:n>=450?60+this.warProgress*16:40:[0,5,9,14,20,24][stage],people=Math.min(Math.floor(site.strength||0),Math.max(3,Math.floor(basePeople*(site.barracksDown||site.tier>=2&&capacity.barracks===false?.5:1)*(this.difficulty==='wanderer'?.85:this.difficulty==='relentless'?1.1:1))));
 return {name:settlementRaid?'Settlement Assault':major?'Major Offensive':platoon?'Tank Platoon':options.intercept?'Armored Intercept':column?'Search & Destroy':vehicles.includes('tank')?'Armored Response':vehicles.includes('apc')?'Mechanized Squad':stage>=3?'Military Squad':stage===2?'Tactical Squad':'Search Team',people,vehicles,stage,capacity};
 }
 makeMilitaryVehicle(site,kind,target,troops=0,operationId=null,index=0){
 const {variant,spec}=this.militaryVehicleProfile(kind,index),staging=site.staging||site.approach||[{x:site.x+140,y:site.y+80}];
 const origin=staging[index%staging.length]||staging[0],p=this.world.vehicleStaging?this.world.vehicleStaging(site,kind,index):this.findOpen(origin.x+(index%2)*65,origin.y+Math.floor(index/2)*65,spec.radius||21);
 if(!p||(this.world.vehicleBlocked?this.world.vehicleBlocked(p.x,p.y,spec.radius||21,kind):this.world.blocked(p.x,p.y,spec.radius||21)))return null;
 const v={id:'vehicle-'+this.nextId++,x:p.x,y:p.y,dir:Math.atan2(target.y-p.y,target.x-p.x),kind,vehicleClass:kind,variant,hp:spec.hp,maxHp:spec.hp,siteId:site.id,state:'raid',target:{x:target.x,y:target.y,id:target.id},shootTimer:2,phase:0,operationId,troopRoles:Array.from({length:troops},(_,i)=>this.forces.militaryRole(i))};
 this.forces.initVehicle?.(v,kind,troops);v.troops=troops;if(['tank','apc','ifv'].includes(kind)&&operationId)v.platoonId=operationId;this.vehicles.push(v);return v;
 }
 prepareMilitarySource(site){
 const pad=(site.radius||160)+180;
 if(!this.world.boundsReady||this.world.boundsReady(site.x-pad,site.y-pad,site.x+pad,site.y+pad))return true;
 this.world.requestCorridor?.(site,site,{id:'source-'+site.id,profile:'tank',radius:pad});this.nextDirectorAt=Math.min(this.nextDirectorAt||Infinity,this.time+2);return false;
 }
 recordHordeContact(observer,target,density){
 if(!target||density<1)return;const previous=this.pursuitOperation,elapsed=previous?this.time-previous.lastContact:0;
 let vx=previous?.vx||0,vy=previous?.vy||0,motionX=previous?.motionX??previous?.x??target.x,motionY=previous?.motionY??previous?.y??target.y,motionAt=previous?.motionAt??previous?.lastContact??this.time;const motionElapsed=this.time-motionAt;
 // Frequent squad reports share one motion sample instead of continually
 // resetting the elapsed time before a direction can be estimated.
 if(previous&&motionElapsed>=.2){if(motionElapsed<8){vx=.5*vx+.5*clamp((target.x-motionX)/motionElapsed,-150,150);vy=.5*vy+.5*clamp((target.y-motionY)/motionElapsed,-150,150)}motionX=target.x;motionY=target.y;motionAt=this.time}
 const roads=this.world._roadInfo?.(target.x,target.y);
 this.pursuitOperation={x:target.x,y:target.y,vx,vy,motionX,motionY,motionAt,estimatedSize:density,lastContact:this.time,firstContact:previous&&elapsed<=5?previous.firstContact:this.time,nearbyRoad:roads?{x:roads.x,y:roads.y}:null,settlements:this.settlements.filter(s=>s.known&&s.population>0&&dist(s,target)<2000).map(s=>s.id),sourceId:observer?.siteId};
 this.lastContact=this.time;this.observedHorde=density;
 if(this.tier===5&&density>=150&&this.time-this.pursuitOperation.firstContact>=25&&this.time>=(this.nextReinforcementAt||0)){this.nextReinforcementAt=this.time+this.reinforcementInterval;this.nextFieldOperation=0;this.nextDirectorAt=0;this.sound('radio',.65,observer?.x)}
 }
 pursuitTarget(){const p=this.pursuitOperation;if(this.tier!==5||!p||this.time-p.lastContact>150)return null;const lead=Math.min(8,Math.max(2,this.time-p.lastContact)),goal={x:p.x+p.vx*lead,y:p.y+p.vy*lead};return this.world.terrain(goal.x,goal.y).water?{x:p.x,y:p.y}:goal}
 responseDirector(){
 this.tickMilitaryOperations();
 if(this.time<(this.nextDirectorAt||0))return;this.nextDirectorAt=this.time+this.directorInterval;
 const n=this.campaignPopulation,sites=[...this.world.sites.values()].filter(s=>!s.cleared&&s.strength>0),reports=[...this.world.intel.values()].filter(i=>typeof i==='object'&&i.heat>5&&this.time-i.lastSeen<90).sort((a,b)=>b.lastSeen-a.lastSeen||b.heat-a.heat);
 const field=this.pursuitTarget()||reports[0],settlement=this.tier>=4||n>=300?this.settlements.filter(s=>s.known&&s.population>0&&this.time-(s.lastRaid??-120)>(this.warIntensity>=4?65:95)&&!this.activeOperations('regional').some(op=>op.target.id===s.id)).sort((a,b)=>this.settlementPriority(b)-this.settlementPriority(a))[0]:null;
 if(field)this.redirectFieldForces(field);
 const available=Math.max(0,this.responseBudget-this.forceWeight()),channels=[];
 if(field&&this.time>=(this.nextFieldOperation||0)&&this.activeOperations('field').length<(this.warIntensity>=4?2:1))channels.push({target:field,regional:false,allowance:settlement?available*.58:available*.82});
 if(settlement&&this.time>=(this.nextRegionalOperation||0))channels.push({target:settlement,regional:true,allowance:field?available*.34:available*.82});
 for(const channel of channels){
 if(this.activeOperations().length>=this.operationCapacity)break;
 const {target,regional}=channel,origins=sites.filter(s=>dist(s,target)<(this.warIntensity>=4?4200:3200)&&this.time-(s.lastRaid??-100)>=30).sort((a,b)=>dist(a,target)-dist(b,target));
 const coordinated=(this.tier>=4||this.warIntensity>=2)&&origins.some(s=>this.world.militaryCapacity?.(s)?.radio??!s.radioDown),max=coordinated?[1,3,3,4,5,6][this.warIntensity]:1,used=[],sources=[];
 const major=!regional&&n>=650&&origins.length>=2&&this.time>=(this.nextMajorOffensive||180),peopleLimit=major?(n>=850?300+Math.floor(this.warProgress*80):200+Math.floor(this.warProgress*60)):n>=850?240+Math.floor(this.warProgress*60):n>=650?170+Math.floor(this.warProgress*50):n>=450?110+Math.floor(this.warProgress*40):regional?54:n>=300?60:Infinity;
 const operationId='campaign-'+this.nextId++,staging=regional?this.assaultStaging(target,origins[0]):null,goal=staging?{...staging,id:target.id}:target;let people=0,spent=0;
 for(const site of origins){
 if(used.length>=max||people>=peopleLimit)break;if(used.length&&(this.world.militaryCapacity?.(site)?.radio===false||site.radioDown))continue;
 const bearing=Math.atan2(site.y-target.y,site.x-target.x);if(used.some(a=>Math.abs(angleDiff(a,bearing))<.8))continue;
 if(used.length>=3){const sorted=[...used,bearing].sort((a,b)=>a-b),gaps=sorted.map((a,i)=>(sorted[(i+1)%sorted.length]-a+TAU)%TAU);if(Math.max(...gaps)<1.65)continue}
 const before=this.forceWeight();
 if(this.spawnRaid(site,goal,regional,{major,operationId,settlement:regional?target:null,assaultPhase:regional?'recon':null,peopleCap:Math.min(major?(n>=850?120:90):Infinity,peopleLimit-people),budgetAllowance:channel.allowance-spent})){spent+=this.forceWeight()-before;people+=this.events.at(-1).people;used.push(bearing);sources.push(site)}
 }
 if(!sources.length)continue;
 const name=regional?'SETTLEMENT ASSAULT':major?'MAJOR OFFENSIVE':used.length>1?'SEARCH & DESTROY':'FIELD PURSUIT';
 const op={id:operationId,name,channel:regional?'regional':'field',target:{x:target.x,y:target.y,id:target.id},status:'active',phase:regional?'recon':'pursuit',phaseAt:this.time,createdAt:this.time,expiresAt:this.time+(regional?300:240),origins:sources.map(s=>s.id),people,major,staging,engineerJobs:[],intensity:this.warIntensity};this.militaryOperations.push(op);
 if(regional){target.lastRaid=this.time;this.nextRegionalOperation=this.time+Math.max(35,75-this.warIntensity*6);if(target.scouts>0)this.notify('SCOUT WARNING — '+target.name+' faces a mechanized assault assembling outside its defenses.','red','villageAttack')}
 else{this.nextFieldOperation=this.time+this.reinforcementInterval;if(major){this.nextMajorOffensive=this.time+Math.max(95,210-this.warIntensity*18);this.notify((this.settlements.some(s=>s.scouts>0)?'SCOUT WARNING — ':'')+'MAJOR OFFENSIVE — Multiple bases commit infantry and armor.','red');this.sound('offensive',1,sources[0].x);this.dispatchRecon(sites,target)}}
 if(used.length>1){this.notify('Military columns converge from '+used.length+' directions.','red');this.sound('converge',.9,sources[0].x)}
 this.events.push({type:'operation',operationId,name,channel:op.channel,phase:op.phase,time:this.time,x:target.x,y:target.y,origins:op.origins});if(this.events.length>30)this.events.shift();
 if((this.tier>=4||this.warIntensity>=2)&&this.activeOperations().length<this.operationCapacity)this.deployRoadblock(sources[0],regional?staging:target);
 }
 // These channels keep their own objectives. Moving the crown does not drag
 // an assault, a fortified interception or another region's air search along.
 if(this.warIntensity>=3&&field&&this.time>=(this.nextInterceptAt||0)&&this.activeOperations().length<this.operationCapacity)this.dispatchInterception(sites,field);
 if(this.warIntensity>=2&&this.time>=(this.nextReconAt||0)&&this.activeOperations().length<this.operationCapacity){const region=this.settlements.filter(s=>s.population>0&&s.known).sort((a,b)=>(a.lastRecon??-100)-(b.lastRecon??-100))[0]||reports[1]||field;if(region)this.dispatchRecon(sites,region)}
 if(!field&&!settlement&&(this.tier>=4||this.warIntensity>=2))this.dispatchConvoy(sites);
 }
 settlementPriority(s){return s.population+(s.level||1)*12+(s.developedRadius||s.radius||90)*.2+(s.defense||0)*.06+(s.growthRate||0)*20}
 assaultStaging(target,site){const angle=Math.atan2((site?.y??target.y)-target.y,(site?.x??target.x+1)-target.x),radius=(target.radius||90)+260;return this.findOpen(target.x+Math.cos(angle)*radius,target.y+Math.sin(angle)*radius,35)}
 dispatchInterception(sites,target){
 const point=this.interceptionPoint(target),source=sites.filter(s=>!s.radioDown&&this.time-(s.lastRaid??-100)>=30&&dist(s,point)<3600).sort((a,b)=>dist(a,point)-dist(b,point))[0];if(!source)return false;
 const id='intercept-'+this.nextId++;if(!this.spawnRaid(source,point,false,{intercept:true,operationId:id,peopleCap:36+this.warIntensity*4,budgetAllowance:Math.min(190,this.responseBudget-this.forceWeight())}))return false;
 const op={id,name:'ARMORED INTERCEPTION',channel:'intercept',target:{x:point.x,y:point.y},status:'active',phase:'prepare',phaseAt:this.time,createdAt:this.time,expiresAt:this.time+180,origins:[source.id],intensity:this.warIntensity,engineerJobs:[]};this.militaryOperations.push(op);this.setOperationObjective(op,point,'Hold Line');this.nextInterceptAt=this.time+Math.max(40,100-this.warIntensity*10);this.events.push({type:'operation',operationId:id,name:op.name,channel:'intercept',phase:'prepare',time:this.time,x:point.x,y:point.y,origins:op.origins});if(this.events.length>30)this.events.shift();return true;
 }
 dispatchRecon(sites,target){
 const before=this.helis.length;if(!this.launchHelicopter(target))return false;const h=this.helis.at(-1);if(this.helis.length===before)return false;
 const id='recon-'+this.nextId++;h.operationId=id;this.militaryOperations.push({id,name:'REGIONAL RECON',channel:'recon',target:{x:target.x,y:target.y,id:target.id},status:'active',phase:'search',phaseAt:this.time,createdAt:this.time,expiresAt:this.time+110,origins:[h.siteId],aircraft:h.id,intensity:this.warIntensity});target.lastRecon=this.time;this.nextReconAt=this.time+Math.max(35,115-this.warIntensity*14);return true;
 }
 setOperationObjective(op,point,order){
 for(const squad of this.forces.squads.values())if(squad.operationId===op.id){squad.objective={x:point.x,y:point.y};squad.order=order;squad.assaultPhase=op.channel==='regional'?op.phase:null;squad.reportAt=this.time;for(const id of squad.members){const h=this.humansById.get(id);if(h){h.lastX=point.x;h.lastY=point.y;h.searchTime=Math.max(h.searchTime||0,90);h.squadObjective={...squad.objective};h.squadOrder=order;h.assaultPhase=squad.assaultPhase}}}
 for(const platoon of this.forces.platoons?.values?.()||[])if(platoon.operationId===op.id){platoon.objective={x:point.x,y:point.y};platoon.order=order;platoon.assaultPhase=op.channel==='regional'?op.phase:null}
 for(const v of this.vehicles)if(v.hp>0&&v.operationId===op.id){v.assaultPhase=op.channel==='regional'?op.phase:null;v.settlementTarget=op.channel==='regional'?op.target.id:null;v.target={x:point.x,y:point.y,id:op.target.id};if(v.state==='idle')v.state='raid'}
 }
 prepareOperationLine(op,point){
 const squads=[...this.forces.squads.values()].filter(s=>s.operationId===op.id),angle=Math.atan2(op.target.y-point.y,op.target.x-point.x),kind=this.warIntensity>=4?'heavy':'basic';
 squads.forEach((squad,index)=>{const side=(index-(squads.length-1)/2)*220;this.forces.prepareLine?.(squad,{x:point.x-Math.sin(angle)*side,y:point.y+Math.cos(angle)*side,angle,kind,temporary:true,operationId:op.id,expiresAt:op.expiresAt});for(const id of squad.members){const job=this.humansById.get(id)?.engineerJob;if(job?.operationId===op.id){job.temporary=true;job.expiresAt=op.expiresAt;if(!op.engineerJobs.includes(job.id))op.engineerJobs.push(job.id)}}});
 }
 tickMilitaryOperations(){
 const peopleAll=[...this.humans],armorAll=[...this.vehicles];for(const site of this.world.sites.values()){peopleAll.push(...(site.sleepingHumans||[]));armorAll.push(...(site.sleepingVehicles||[]))}
 for(const op of this.militaryOperations){if(op.status!=='active')continue;const people=peopleAll.filter(h=>h.hp>0&&h.operationId===op.id),armor=armorAll.filter(v=>v.hp>0&&v.operationId===op.id),air=this.helis.find(h=>h.hp>0&&h.operationId===op.id);
 if(this.time>=op.expiresAt||!people.length&&!armor.length&&!air){op.status='ended';op.endedAt=this.time;continue}
 if(op.channel==='recon')continue;
 if(op.channel==='intercept'){if(op.phase==='prepare'&&people.some(h=>dist(h,op.target)<150)){this.prepareOperationLine(op,op.target);op.phase='hold';op.phaseAt=this.time}continue}
 if(op.channel!=='regional')continue;const settlement=this.settlement(op.target.id);if(!settlement||settlement.population<=0){op.status='ended';op.endedAt=this.time;continue}
 const elapsed=this.time-op.phaseAt,atStage=people.some(h=>dist(h,op.staging)<250)||armor.some(v=>dist(v,op.staging)<200);
 let next=null;if(op.phase==='recon'&&elapsed>=8)next='approach';else if(op.phase==='approach'&&(atStage||elapsed>=65))next='deployment';else if(op.phase==='deployment'&&elapsed>=8&&(!armor.some(v=>v.troops>0)||elapsed>=24))next='engineers';else if(op.phase==='engineers'&&elapsed>=12)next='assault';
 if(next){op.phase=next;op.phaseAt=this.time;this.events.push({type:'assault-phase',operationId:op.id,settlementId:settlement.id,phase:next,time:this.time,x:op.staging.x,y:op.staging.y});if(this.events.length>30)this.events.shift();if(next==='engineers')this.prepareOperationLine(op,op.staging);if(next==='assault'){settlement.attack=true;this.notify('ASSAULT — Infantry breaches '+settlement.name+' while armor supports the approaches.','red','villageAttack');this.sound('offensive',.85,op.staging.x)}}
 const assault=op.phase==='assault',entrance=settlement.entrances?.filter(p=>Number.isFinite(p.x)&&Number.isFinite(p.y)).sort((a,b)=>dist(a,op.staging)-dist(b,op.staging))[0]||op.target;
 this.setOperationObjective(op,assault?entrance:op.staging,assault?'Attack Settlement':op.phase==='deployment'||op.phase==='engineers'?'Hold Line':'Approach');
 if(assault)for(const v of armor)if(['tank','apc','ifv'].includes(v.vehicleClass||v.kind)){const angle=Math.atan2(op.staging.y-settlement.y,op.staging.x-settlement.x),radius=(settlement.radius||90)+95;v.target={x:settlement.x+Math.cos(angle)*radius,y:settlement.y+Math.sin(angle)*radius,id:settlement.id};v.assaultPhase='support'}
 }
 this.militaryOperations=this.militaryOperations.filter(op=>op.status==='active'||this.time-(op.endedAt||op.createdAt)<180).slice(-24);
 }
 interceptionPoint(target){const p=this.pursuitOperation,goal={x:target.x+(p?.vx||0)*6,y:target.y+(p?.vy||0)*6},road=this.world._roadInfo?.(goal.x,goal.y);if(road){if(road.dx<road.dy)goal.x=road.x;else goal.y=road.y}return this.world.terrain(goal.x,goal.y).water?target:goal}
 redirectFieldForces(target){
 const independent=new Set(this.militaryOperations.filter(op=>op.channel!=='field').map(op=>op.id));
 for(const op of this.activeOperations('field'))op.target={x:target.x,y:target.y};
 for(const h of this.humans){if(h.hp<=0||!h.responseAllocated||h.raidTarget||!h.operationId||independent.has(h.operationId)||this.world.sites.get(h.siteId)?.radioDown)continue;h.lastX=target.x;h.lastY=target.y;h.searchTime=Math.max(h.searchTime||0,60);if(h.state==='patrol')h.state='search';const squad=this.forces.squads.get(h.squadId);if(squad&&this.time-squad.reportAt>5){squad.objective={x:target.x,y:target.y};if(!squad.density)squad.order='Search'}}
 for(const v of this.vehicles)if(v.hp>0&&v.operationId&&!independent.has(v.operationId)&&!v.logistics&&!v.target?.id&&!this.world.sites.get(v.siteId)?.radioDown){v.target={x:target.x,y:target.y};if(v.state==='idle')v.state='raid'}
 }
 dispatchConvoy(sites){
 if(this.time<(this.nextConvoyAt||180)||this.activeOperations().length>=this.operationCapacity)return false;
 const source=sites.find(s=>s.tier>=3&&!s.depotDown&&!s.fuelDown&&this.world.militaryCapacity?.(s)?.depot!==false&&this.world.militaryCapacity?.(s)?.fuel!==false&&s.strength>=12&&(this.world.militaryCapacity?.(s)?.inventory.truck||0)>0),destination=source&&sites.filter(s=>s!==source&&s.tier>=3&&dist(s,source)<3200&&dist(s,source)>600).sort((a,b)=>dist(a,source)-dist(b,source))[0];
 if(!destination||this.forceWeight()+43>this.responseBudget||this.humans.length+12+this.vehicles.reduce((n,v)=>n+(v.troops||0),0)>this.activeHumanCapacity())return false;
 if(!this.prepareMilitarySource(source)){this.nextConvoyAt=this.time+2;return false}
 const target=destination.approach?.at(-1)||destination,vehicles=[],operationId='convoy-'+this.nextId++;
 for(const kind of ['jeep','truck','apc','command']){const cost=this.militaryVehicleProfile(kind,vehicles.length).spec.budget,cargo=kind==='truck'?Array.from({length:12},(_,i)=>this.forces.militaryRole(i)).reduce((n,role)=>n+this.humanWeight(role),0):0;if(!this.vehicleRoom(kind)||!(source.vehicleInventory[kind]>0)||kind==='apc'&&source.armorCapacity<cost||this.forceWeight()+cost+cargo>this.responseBudget)continue;const v=this.makeMilitaryVehicle(source,kind,target,kind==='truck'?12:0,operationId,vehicles.length);if(!v)continue;v.logistics=true;source.vehicleInventory[kind]--;if(kind==='apc')source.armorCapacity=Math.max(0,source.armorCapacity-cost);if(kind==='truck')source.strength-=12;vehicles.push(v)}
 this.nextConvoyAt=this.time+(vehicles.length?180:10);if(!vehicles.length)return false;this.militaryOperations.push({id:operationId,name:'SUPPLY CONVOY',channel:'logistics',target:{x:target.x,y:target.y},status:'active',phase:'travel',phaseAt:this.time,createdAt:this.time,expiresAt:this.time+240,origins:[source.id]});this.events.push({type:'convoy',operationId,x:source.x,y:source.y,targetX:target.x,targetY:target.y,time:this.time});if(this.events.length>30)this.events.shift();this.sound('rumble',.7,source.x);return true;
 }
 deployRoadblock(site,target){
 const number=this.warIntensity>=3?12:8;
 if(!site||site.radioDown||this.activeOperations().length>=this.operationCapacity||this.time<(site.nextRoadblock||0)||this.humans.length+number+this.vehicles.reduce((n,v)=>n+(v.troops||0),0)>this.activeHumanCapacity()||!this.prepareMilitarySource(site))return false;
 const candidates=site.roadblocks||site.approach||[],point=candidates.filter(p=>dist(p,target)>350&&dist(p,target)<1500&&this.world.terrain(p.x,p.y).road&&!this.world.blocked(p.x,p.y,35)).sort((a,b)=>dist(a,target)-dist(b,target))[0];
 if(!point||site.strength<number||this.forceWeight()+number*2>this.responseBudget)return false;site.nextRoadblock=this.time+Math.max(80,180-this.warIntensity*17);
 const operationId='roadblock-'+this.nextId++,squad=[],roles=['leader','rifleman','heavy','engineer','medic','shield','engineer','rifleman','grenadier','sniper','engineer','rifleman'];for(let i=0;i<number;i++){const h=this.makeHuman(site.x+i%4*24,site.y+Math.floor(i/4)*24,site);this.forces.assign(h,site,roles[i]);h.state='search';h.lastX=point.x;h.lastY=point.y;h.searchTime=180;h.responseAllocated=true;h.operationId=operationId;squad.push(h)}site.strength-=number;
 let parked=null;const inventory=this.world.militaryCapacity?.(site)?.inventory||site.vehicleInventory||{},kind=this.tier===5&&(inventory.tank||0)>0&&this.vehicleRoom('tank')?'tank':(inventory.apc||0)>0?'apc':'jeep',cost=this.militaryVehicleProfile(kind).spec.budget||4;
 if(this.vehicleRoom(kind)&&(inventory[kind]||0)>0&&this.forceWeight()+cost<=this.responseBudget&&!site.depotDown&&!site.fuelDown&&(!['tank','apc'].includes(kind)||site.armorCapacity>=cost)){parked=this.makeMilitaryVehicle(site,kind,point,0,operationId);if(parked){inventory[kind]--;if(['tank','apc'].includes(kind))site.armorCapacity-=cost}}
 const formation=this.forces.createSquad?.(squad,point,{order:'Hold Line',siteId:site.id,roadblock:true,vehicleId:parked?.id,operationId});
 const op={id:operationId,name:'BLOCKADE',channel:'roadblock',siteId:site.id,target:{x:point.x,y:point.y},point:{x:point.x,y:point.y},facing:Math.atan2(target.y-point.y,target.x-point.x),members:squad.map(h=>h.id),engineers:squad.filter(h=>h.role==='engineer').map(h=>h.id),squadId:formation?.id,until:this.time+210,expiresAt:this.time+210,createdAt:this.time,phaseAt:this.time,status:'active',phase:'approach',origins:[site.id],engineerJobs:[],built:false,objects:[]};this.roadblockOperations.push(op);this.militaryOperations.push(op);this.events.push({type:'blockade',operationId,name:'BLOCKADE',time:this.time,x:point.x,y:point.y,siteId:site.id});if(this.events.length>30)this.events.shift();if(this.warIntensity>=3)this.notify('BLOCKADE — Military engineers move to secure a road.','red');return true;
 }
 tickRoadblocks(){
 this.roadblockOperations=(this.roadblockOperations||[]).filter(op=>{
 if(op.until<this.time){op.status='ended';op.endedAt=this.time;for(const id of op.objects||[]){const o=this.world.objects.get(id);if(o&&!o.dead){o.hp=0;o.dead=true;o.solid=false;this.world.navRevision++}}return false}
 const engineers=(op.engineers||op.members.filter(id=>this.humansById.get(id)?.role==='engineer')).map(id=>this.humansById.get(id)).filter(h=>h?.hp>0);
 if(!engineers.length)return true;
 for(let i=0;i<engineers.length;i++){const h=engineers[i],done=h.lastEngineerJob;if(done?.operationId===op.id&&done.status==='completed'&&done.objectId&&!op.objects.includes(done.objectId))op.objects.push(done.objectId);if(h.engineerJob||op.built||done?.operationId===op.id&&done.status==='completed'||done?.operationId===op.id&&this.time<h.specialAt||dist(h,op.point)>170||this.time<(op.nextBuildAt||0)||this.apeGrid.near(op.point.x,op.point.y,140).length)continue;
 const side=[-150,-70,70,150][i%4],angle=op.facing||0,point={x:op.point.x-Math.sin(angle)*side,y:op.point.y+Math.cos(angle)*side};const job=this.forces.startEngineerJob?.(h,{...point,angle:angle+Math.PI/2,kind:this.warIntensity>=4?'heavy':'basic',siteId:op.siteId,operationId:op.id,temporary:true,expiresAt:op.until});if(job){op.engineerJobs.push(job);op.phase='building';op.phaseAt=this.time}
 }
 op.built=op.objects.length>0;if(op.built)op.phase='hold';return true;
 });
 }
 get population(){return this.apes.filter(a=>a.hp>0).length}
 get score(){let s=this.stats;return Math.round(s.freed*250+s.born*300+s.largestHorde*90+s.largestSettlement*60+s.humans*40+s.structures*35+s.bases*1800+s.prisons*500+s.settlements*650+s.territory*15+this.time*2+this.settlements.filter(x=>x.population>0).length*400)}
 get title(){return this.score>90000?'The Unbroken Crown':this.score>45000?'Ape Warlord':this.score>22000?'Lord of the Forest':this.score>10000?'Tribal King':this.stats.freed>5?'Liberator':'Lonely Wanderer'}
 notify(text,color='gold',category='routine'){this.messages.push({text,color,category,time:this.time});if(this.messages.length>25)this.messages.shift();this.hooks.toast?.(text,color,category)}
 sound(name,strength=1,x=this.king.x){this.hooks.sound?.(name,strength,clamp((x-this.king.x)/550,-1,1))}
 effect(type,x,y,opts={}){
 const critical=type==='wave'||type==='charge'||type==='text'||type==='explosion'||distance2({x,y},this.king)<350**2;
 const budget=400-this.performance.qualityLevel*35;
 if(this.effects.length>=budget){
 // Merge redundant impacts away from the crown while retaining command and warning cues.
 if(!critical){for(let i=this.effects.length-1;i>=Math.max(0,this.effects.length-24);i--){const f=this.effects[i];if(f.type===type&&(f.x-x)**2+(f.y-y)**2<30**2){f.life=Math.max(f.life,opts.life||.65);return}}return}
 const index=this.effects.findIndex(f=>!['wave','charge','text','explosion'].includes(f.type)&&distance2(f,this.king)>350**2);
 if(index>=0)this.effectPool.release(this.effects.splice(index,1)[0]);else if(this.effects.length>=budget+48)return;
 }
 const life=opts.life||.65;this.effects.push(this.effectPool.take({type,x,y,life,maxLife:life,...opts}));
 }
 noise(x,y,radius,kind='roar'){
 let event=this.noises.find(n=>n.kind===kind&&(n.x-x)**2+(n.y-y)**2<40**2);
 if(event){event.x=x;event.y=y;event.life=2;event.radius=Math.max(event.radius,radius)}else{event=this.noisePool.take({x,y,radius,life:2,kind,hp:1});this.noises.push(event)}
 if(this.noises.length>128)this.noisePool.release(this.noises.shift());
 // Grid is refreshed before actor work; direct calls before the first step use the small live list.
 const candidates=this._indexesReady?this.humanGrid.near(x,y,radius):this.humans;
 for(const h of candidates){if(h.hp>0&&(x-h.x)**2+(y-h.y)**2<radius**2&&h.state!=='combat'&&h.state!=='radio'&&h.state!=='alarm'){h.state='investigate';h.lastX=x;h.lastY=y;h.searchTime=12;h.suspicion=Math.max(h.suspicion,.35)}}
 }
 command(cmd,aim=this.aim){
 if(this.ended||this.commandCD>0||this.blastActive(this.king))return false;if(this.siege?.selected.length&&['charge','spreadCharge','attackNearest','recall','hold','call'].includes(cmd))return this.siege.shortcut(cmd,aim);this.commandCD=.5;this.king.attackTimer=.55;let commandRadius=cmd==='settle'?105:cmd==='hold'?160:cmd==='call'?340:260;let nearby=this.apes.filter(a=>a.hp>0&&dist(a,this.king)<commandRadius),n=0;
 const wave=(color,range=300)=>this.effect('wave',this.king.x,this.king.y,{color,life:1.1,range});
 if(cmd==='call'){
 for(const a of nearby){if(a.state!=='young'){this.tactics?.clearOrder(a);a.state='follow';a.settlementId=null;a.target=null;n++}}
 this.mode='follow';wave('#e5c374',340);this.noise(this.king.x,this.king.y,650);this.sound('call',1+this.followers.length/90);this.notify(n?'Your roar gathers '+n+' apes.':'Your roar echoes through the woods.');
 }else if((cmd==='spreadCharge'||cmd==='attackNearest')&&this.tactics){
 n=this.tactics.order(cmd,aim);if(!n)return false;this.mode=cmd;wave(cmd==='spreadCharge'?'#f0b56d':'#d387a5',360);this.noise(this.king.x,this.king.y,900);this.sound('charge',1+n/60);
 }else if(cmd==='charge'){
 let len=Math.hypot(aim.x,aim.y)||1;const dx=aim.x/len,dy=aim.y/len;for(const a of this.apes){if(a.hp>0&&FOLLOW.has(a.state)){this.tactics?.clearOrder(a);a.state='charge';a.target={x:this.king.x+dx*570,y:this.king.y+dy*570};a.chargeTime=13;n++}}
 if(!n){this.notify('Free apes and Call them before charging.');return false}this.mode='charge';this.effect('charge',this.king.x,this.king.y,{dx,dy,life:1.2,color:'#e6b05b'});this.noise(this.king.x,this.king.y,900);this.sound('charge',1+n/60);this.notify(n+' apes charge into the dark.');
 }else if(cmd==='recall'){
 for(const a of this.apes){if(a.hp>0&&FOLLOW.has(a.state)){this.tactics?.clearOrder(a);a.state='follow';a.target=null;a.retreatUntil=this.time+5;n++}}this.mode='follow';wave('#75cabb',440);this.noise(this.king.x,this.king.y,750);this.sound('recall');this.notify('Disengage. Return to your king.');
 }else if(cmd==='hold'){
 for(const a of nearby){if(FOLLOW.has(a.state)){this.tactics?.clearOrder(a);a.state='hold';a.target={x:a.x,y:a.y};n++}}this.mode='hold';wave('#99bac3',260);this.sound('hold');this.notify(n?n+' apes hold this ground. Call to regroup.':'No followers close enough to hold.');
 }else if(cmd==='settle'||cmd==='settleAll'){
 if(this.world.terrain(this.king.x,this.king.y).water||this.world.getSites(this.king.x,this.king.y,240).some(s=>!s.cleared&&s.guards>0)){this.notify('Move into safer dry woodland before settling.','red');return false}
 let list=(cmd==='settleAll'?this.apes:nearby).filter(a=>a.hp>0&&FOLLOW.has(a.state));if(!list.length){this.notify('There are no followers here to settle.');return false}
 let s=this.settlements.find(s=>s.population>0&&dist(s,this.king)<240);if(!s){s={id:'settlement-'+this.nextId++,x:this.king.x,y:this.king.y,name:['Redwood','Moonroot','Laurel','Riverbend','Ashgrove','Highbranch'][this.settlements.length%6]+(this.settlements.length>=6?' '+(this.settlements.length+1):''),level:1,radius:90,food:18,age:0,known:false,attack:false,population:0,birthTimer:0,starveTimer:0,lastRaid:-120,nextWarn:0,foragers:0,scouts:0,children:0,livingFounding:true};this.settlements.push(s);this.stats.settlements++;this.notify(s.name+' has been founded.')}
 for(const a of list){this.tactics?.clearOrder(a);a.state='settled';a.settlementId=s.id;a.homeX=s.x;a.homeY=s.y;n++}let transfer=Math.min(this.food,Math.max(12,n*2));this.food-=transfer;s.food+=transfer;wave('#93caa0');this.sound('settle');this.notify(n+' apes settle. Food transferred: '+Math.floor(transfer)+'.');this.refreshSettlements();this.colonies.init(s);
 }else if(cmd==='patrol'){
 for(const a of nearby){if(a.state==='settled'){a.state='scout';a.maxHp=SCOUT_HP+(a.trainingLevel||0)*12;a.hp=Math.max(a.hp,SCOUT_HP);a.speed=104;n++;if(n>=Math.max(2,Math.floor(nearby.length/3)))break}}
 wave('#92c5ec');this.sound('patrol');this.notify(n?n+' scouts guard the settlement approaches.':'Visit settled apes to assign scouts.');this.refreshSettlements();
 }return true;
 }
 findOpen(x,y,r=10){
 if(!this.world.blocked(x,y,r))return{x,y};for(let radius=22;radius<=200;radius+=22)for(let i=0;i<12;i++){let aa=i/12*TAU,xx=x+Math.cos(aa)*radius,yy=y+Math.sin(aa)*radius;if(!this.world.blocked(xx,yy,r))return{x:xx,y:yy}}return{x,y};
 }
 ensureApeAppearance(a){
  // Cosmetics use independent hashes so combat and movement keep their random stream.
  const id=a.id||'body-'+Math.round((a.x||0)*10)+','+Math.round((a.y||0)*10);
  if(a.id==='king')a.species='gorilla';else if(!PRIMATE_NAMES.has(a.species))a.species=PRIMATE_IDS[ATSUtil.hash(this.seed+'|primate|'+id)%PRIMATE_IDS.length];
  if(!Number.isInteger(a.coatVariant)||a.coatVariant<0||a.coatVariant>2)a.coatVariant=ATSUtil.hash(this.seed+'|coat|'+id)%3;
  this.tactics?.ensure(a);return a;
 }
 applyTraining(a,level){
 const desired=clamp(Math.floor(level||0),0,3),applied=clamp(Math.floor(a.trainedAppliedLevel||0),0,3);
 a.maxHp=Math.max(1,(a.maxHp||APE_HP)+(desired-applied)*12);a.hp=Math.min(a.hp,a.maxHp);a.trainingLevel=desired;a.trainedAppliedLevel=desired;
 }
 apeDamage(a,base){return base*(1+clamp(a.trainingLevel||0,0,3)*.08)}
 makeApe(x,y,state='free',settlementId=null,young=false){
 if(this.population>=MAX_APE_POPULATION){if(this.time>=(this.nextPopulationWarn||0)){this.nextPopulationWarn=this.time+10;this.notify('Population limit reached — '+MAX_APE_POPULATION+' living apes.','gold')}return null}
 ({x,y}=this.findOpen(x,y));
 const hp=young?YOUNG_HP:state==='scout'?SCOUT_HP:APE_HP;
 const p={id:'ape-'+this.nextId++,x,y,hp,maxHp:hp,dir:Math.random()*TAU,phase:Math.random()*TAU,fur:Math.random(),bodyScale:.88+Math.random()*.22,state:young?'young':state,settlementId,age:young?0:240,attackTimer:0,attackCD:Math.random()*.4,speed:young?67:84+Math.random()*18,moving:false,offsetX:(Math.random()-.5)*120,offsetY:(Math.random()-.5)*120,wander:Math.random()*TAU,nextThink:0};this.ensureApeAppearance(p);this.siege?.balance(p);this.apes.push(p);this.apesById.set(p.id,p);return p;
 }
 makeHuman(x,y,site,kind=null){
 ({x,y}=this.findOpen(x,y));
 const t=Math.max(site?.tier||1,this.tier-1);kind=kind||(['pistol','rifle','assault','shotgun','machine'][clamp(t-1,0,4)]);if(t>=3&&Math.random()<.25)kind='shotgun';
 const h={id:'human-'+this.nextId++,x,y,hp:kind==='machine'?90:70,maxHp:kind==='machine'?90:70,dir:Math.random()*TAU,kind,state:'patrol',suspicion:0,shootTimer:1.5+Math.random(),radioTimer:0,siteId:site?.id||null,homeX:x,homeY:y,lastX:x,lastY:y,patrolX:x,patrolY:y,searchTime:0,phase:Math.random()*TAU,hasRadio:Math.random()<.35||(t>=3&&Math.random()<.5),perceptionTimer:Math.random()*.25,nextPatrol:0,moving:false,attackTimer:0,reported:false};this.forces.assign(h,site);this.humans.push(h);this._lightsAt=-1;this.humansById.set(h.id,h);return h;
 }
 spawnSites(){
 if(this.time>=(this.nextStreamAt||0)){this.nextStreamAt=this.time+2;this.sleepDistantForces()}
 const sites=this.world.getSites(this.king.x,this.king.y,1050);
 for(const s of sites){
 let budget=this.responseBudget-this.forceWeight();
 while(s.sleepingHumans?.length&&this.humans.length+this.vehicles.reduce((n,v)=>n+(v.troops||0),0)<this.activeHumanCapacity()){const h=s.sleepingHumans[s.sleepingHumans.length-1],cost=this.humanWeight(h.role);if(cost>budget)break;this.humans.push(s.sleepingHumans.pop());budget-=cost}
 while(s.sleepingVehicles?.length){const v=s.sleepingVehicles[s.sleepingVehicles.length-1],cost=this.forces.vehicleSpec(v).budget+this.cargoWeight(v);if(cost>budget||!this.vehicleRoom(v.vehicleClass||v.kind)||this.humans.length+this.vehicles.reduce((n,a)=>n+(a.troops||0),0)+(v.troops||0)>this.activeHumanCapacity())break;this.vehicles.push(s.sleepingVehicles.pop());budget-=cost}
 if(s.spawned)continue;const count=s.guards??Math.max(0,s.tier*2);
 s.spawned=true;s.nextOperation=this.time+55+Math.random()*45;const guards=[];for(let i=0;i<count;i++){let a=i/count*TAU;const h=this.makeHuman(s.x+Math.cos(a)*90,s.y+Math.sin(a)*90,s),cost=this.humanWeight(h.role);if(this.humans.length+this.vehicles.reduce((n,v)=>n+(v.troops||0),0)>this.activeHumanCapacity()||budget<cost){this.humans.pop();(s.sleepingHumans||(s.sleepingHumans=[])).push(h)}else{budget-=cost;guards.push(h)}}
 if(s.tier>=2&&guards.length)this.forces.createSquad?.(guards,{x:s.x,y:s.y},{order:'Defend Base',siteId:s.id});
 this.world.militaryCapacity?.(s);const kind=this.mobilized&&this.tier===5&&s.vehicleInventory?.tank?'tank':this.tier>=4&&s.vehicleInventory?.apc?'apc':s.tier>=4?'armored':'jeep';
 const cost=this.militaryVehicleProfile(kind).spec.budget,heavy=['tank','ifv','apc'].includes(kind);if(s.tier>=3&&this.vehicleRoom(kind)&&budget>=cost&&!s.depotDown&&!s.fuelDown&&(s.vehicleInventory?.[kind]||0)>0&&(!heavy||s.armorCapacity>=cost)){const v=this.makeMilitaryVehicle(s,kind,{x:s.x,y:s.y});if(v){v.state='idle';s.vehicleInventory[kind]--;if(heavy&&Number.isFinite(s.armorCapacity))s.armorCapacity=Math.max(0,s.armorCapacity-cost)}}}
 }
 sleepDistantForces(){
 // Preserve each garrison rather than letting old, inactive guards consume the
 // entire population budget as the player explores new regions.
 const sleep=(a,key)=>{const s=this.world.sites.get(a.siteId);if(!s||a.hp<=0||a.raidTarget||a.state==='raid'||dist(a,this.king)<2300||this.settlements.some(st=>st.population>0&&dist(a,st)<850))return true;for(const key in a)if(key.startsWith('_')||key==='navCohort')delete a[key];a.aiming=null;(s[key]||(s[key]=[])).push(a);return false};
 this.humans=this.humans.filter(a=>sleep(a,'sleepingHumans'));
 this.vehicles=this.vehicles.filter(a=>sleep(a,'sleepingVehicles'));
 }
 move(a,dx,dy,speed,dt){
 if(this.blastActive(a)){a.moving=false;return}
 if(a.siegeTransition)return;if(!a.armyOrder&&(a.wallClimb||(a.state==='charge'||a._nav?.stuck>.2)&&this.tactics?.tryClimb(a,dx,dy)))return;speed*=a.rallyUntil>this.time?1.15:1;speed*=a.shield?.hp>0?.9:1;if(a.staggerUntil>this.time)return;
 const terrain=this.world.terrain(a.x,a.y),roleSpeed=a.role?(ATSHumanRoles[a.role]?.speed||1):1;
 this.navigation.move(a,dx,dy,speed*roleSpeed*(this.terrainPace?.(a,terrain)??1)*(terrain.biome==='wetland'&&!terrain.road?.86:1),dt,a.id==='king');
 }
 spreadApes(dt){
 // Compute all pressures before moving anyone, so array order cannot bias a clump.
 // Local steps bypass route finding but retain wall, trunk and water clearance.
 const pressures=new Map();
 for(const a of this.apes){
 if(a.hp<=0||this.blastActive(a)||a.wallClimb||a.onWallId||a.siegeTransition)continue;
 const st=a.settlementId?this.settlement(a.settlementId):null;
 if(a._simTier===2||a._simTier===1&&a._lodAt!==this.time||st&&dist(a,this.king)>1800&&!st.attack)continue;
 pressures.set(a,{x:0,y:0});
 }
 // Slow actors still participate as neighbors between their own updates.
 this.apeGrid.rebuild([this.king,...this.apes]);const shifts=[];
 for(const [a,push]of pressures){
 for(const p of this.apeGrid.nearest(a.x,a.y,38,8,p=>p!==a&&p.hp>0&&(p.id>a.id||!pressures.has(p)))){
 this.performance.counters.separationPairs++;
 const spacing=p.id==='king'?36:a.state==='young'||p.state==='young'?24:32;
 let dx=a.x-p.x,dy=a.y-p.y,d=Math.hypot(dx,dy);if(d>=spacing)continue;
 if(!this.navigation.clearSegment(a.x,a.y,p.x,p.y,0))continue;
 if(d<.001){
 // A stable, opposite direction for each pair also separates identical save positions.
 const first=a.id<p.id,angle=ATSUtil.hash(first?a.id+':'+p.id:p.id+':'+a.id)/4294967296*TAU;
 dx=Math.cos(angle)*(first?1:-1);dy=Math.sin(angle)*(first?1:-1);d=0;
 }else{dx/=d;dy/=d}
 const pressure=110*(1-d/spacing)**2;push.x+=dx*pressure;push.y+=dy*pressure;
 const other=pressures.get(p);if(other){other.x-=dx*pressure;other.y-=dy*pressure}
 }
 }
 for(const [a,push]of pressures){
 const force=Math.hypot(push.x,push.y);if(force>.5)shifts.push({a,dx:push.x/force,dy:push.y/force,speed:Math.min(70,force)});
 }
 for(const {a,dx,dy,speed}of shifts){
 const moving=a.moving,dir=a.dir;
 this.navigation.move(a,dx*40,dy*40,speed,a._simTier===1?Math.min(.1,a._lodInterval||dt):dt,true);
 a.moving=a.moving||moving;if(a.attackTimer>0)a.dir=dir;
 }
 this.apeGrid.rebuild([this.king,...this.apes]);
 }
 settlement(id){let s=this.settlementsById.get(id);if(!s){s=this.settlements.find(s=>s.id===id);if(s)this.settlementsById.set(id,s)}return s}
 syncIndexes(){
 this.apesById.clear();this.humansById.clear();this.settlementsById.clear();this.settlementMembers.clear();this.followerCount=0;this.followCount=0;
 for(const s of this.settlements){this.settlementsById.set(s.id,s);this.settlementMembers.set(s.id,[])}
 for(const a of this.apes){if(a.hp<=0)continue;this.apesById.set(a.id,a);if(FOLLOW.has(a.state))this.followerCount++;if(a.state==='follow')this.followCount++;const members=this.settlementMembers.get(a.settlementId);if(members)members.push(a)}
 for(const h of this.humans)if(h.hp>0)this.humansById.set(h.id,h);
 this._indexesReady=true;
 }
 buildRelevance(){
 const focus=[];this.rescueFocus=this.rescueFocus.filter(f=>f.until>this.time);
 for(const s of this.settlements){if(!s.attack&&this.humanGrid.near(s.x,s.y,(s.radius||90)+160).some(h=>h.state!=='patrol')){s.attack=true;if(this.time>=(s.attackAlertAt||0)){s.attackAlertAt=this.time+20;this.notify(s.name+' is under attack.','red','villageAttack')}}if(s.attack)focus.push({x:s.x,y:s.y,hp:1,radius:(s.radius||90)+320})}
 for(const s of this.world.sites.values())if(s.alarm)focus.push({x:s.x,y:s.y,hp:1,radius:600});
 for(const h of this.humans)if(h.hp>0&&['combat','alarm','radio'].includes(h.state))focus.push({x:h.x,y:h.y,hp:1,radius:380});
 for(const a of this.apes)if(a.hp>0&&this.time-(a.lastHit??-100)<2)focus.push({x:a.x,y:a.y,hp:1,radius:300});
 for(const n of this.noises)if(['alert','fight','smash'].includes(n.kind))focus.push({x:n.x,y:n.y,hp:1,radius:350});
 for(const f of this.rescueFocus)focus.push({...f,radius:350});
 this.focusGrid.rebuild(focus);this.noiseGrid.rebuild(this.noises);
 }
 actorTier(a){
 const d=distance2(a,this.king);if(a.armyOrder||a.siegeTransition||a.onWallId)return d<1200**2?0:1;if(d<750**2)return 0;
 for(const f of this.focusGrid.near(a.x,a.y,900))if(distance2(a,f)<f.radius**2)return 0;
 if(d<(this.performance.qualityLevel>=5?1300:1650)**2)return 1;return 2;
 }
 simulateActor(a,dt,kind){
 if(this.blastActive(a)||a.wallClimb){a._simTier=0;a._lodElapsed=0;if(kind==='ape'||kind==='human')this.performance.counters[kind==='ape'?'simulatedApes':'simulatedHumans']++;return dt}
 const tier=this.actorTier(a),previousTier=a._simTier;a._simTier=tier;
 a._navPriority=a.id==='king'?0:a.state==='combat'||a.attackTimer>0?1:a._nav?.stuck>.8?2:kind==='ape'?3:4;
 if(tier===0){a._lodInterval=0;a._lodElapsed=0;if(previousTier>0){a.perceptionTimer=0;a._sense=null}if(kind==='ape'||kind==='human')this.performance.counters[kind==='ape'?'simulatedApes':'simulatedHumans']++;return dt}
 const interval=tier===1?1/15:(this.performance.qualityLevel>=5?.8:.5);
 if(a._lodElapsed===undefined)a._lodElapsed=0;
 if(previousTier!==tier||a._lodDue===undefined)a._lodDue=this.time+(ATSUtil.hash(a.id)%997)/997*interval;
 a._lodElapsed+=dt;if(this.time<(a._lodDue||0))return 0;
 const elapsed=Math.min(a._lodElapsed,interval+dt);a._lodElapsed=0;a._lodDue=this.time+interval;a._previousX=a.x;a._previousY=a.y;a._lodAt=this.time;a._lodInterval=interval;
 if(tier===2){this.abstractActor(a,elapsed,kind);if(kind==='ape'||kind==='human')this.performance.counters[kind==='ape'?'abstractApes':'abstractHumans']++;return 0}
 if(kind==='ape'||kind==='human')this.performance.counters[kind==='ape'?'simulatedApes':'simulatedHumans']++;return elapsed;
 }
 abstractActor(a,dt,kind){
 if(this.updateBlastReaction(a,dt))return;
 a.moving=false;a.hitTimer=Math.max(0,(a.hitTimer||0)-dt);a.attackTimer=Math.max(0,(a.attackTimer||0)-dt);
 let target=null,speed=0;
 if(kind==='ape'){
 a.age+=dt;a.attackCD=Math.max(0,a.attackCD-dt);
 if(a.state==='young'&&a.age>=GROW_UP){a.state='settled';a.hp=a.maxHp=APE_HP;a.speed=90;this.siege?.balance(a,true)}
 if(a.state==='follow'){target=this.king;speed=a.speed*1.55}else if(a.state==='charge'){a.chargeTime-=dt;target=a.target;speed=a.speed*1.4;if(a.chargeTime<=0){a.state='hold';a.target={x:a.x,y:a.y}}}
 else if(a.settlementId&&['settled','scout','young'].includes(a.state)){const s=this.settlement(a.settlementId);if(s){if(this.time>=(a._activityAt||0)||!a._activityTarget){a._activityAt=this.time+3+(ATSUtil.hash(a.id)%5)*.3;a._activityTarget=this.colonies.activityTarget?.(a,s)}target=a._activityTarget;speed=target?.speed??a.speed*.55}}
 }else{
 a.shootTimer-=dt;a.perceptionTimer=Math.max(0,a.perceptionTimer-dt);
 if(a.state==='search'||a.state==='investigate'){a.searchTime-=dt;target={x:a.lastX,y:a.lastY};speed=72;if(a.searchTime<=0){a.state='patrol';a.reported=false;a.suspicion=0;a.raidTarget=null}}
 else if(kind==='vehicle'&&a.state==='raid'){this.forces.initVehicle?.(a);target=a.target;speed=this.forces.vehicleSpec(a).speed;if(a.mobilityDamage>=100)speed=0;else speed*=1-(a.mobilityDamage||0)/160;speed*=1-(a.engineDamage||0)/180}
 }
 // Distant journeys advance at a strategic cadence, with one bounded corridor check.
 // Reported response journeys may queue bounded low-priority corridors; they
 // cannot fight or pass through obstacles while abstract.
 if(target){
 if(kind==='vehicle'&&a.state==='raid'&&this.forces.vehicleMove){a._navPriority=5;this.forces.vehicleMove(a,target,dt);return}
 const radius=a.radius||(kind==='vehicle'?21:a.state==='young'?7:10),profile=kind==='vehicle'?a.vehicleClass:kind==='ape'?'ape':'human',step=point=>{const dx=point.x-a.x,dy=point.y-a.y,d=Math.hypot(dx,dy),travel=Math.min(60,speed*dt,d);if(d<.001||!this.navigation.clearSegment(a.x,a.y,a.x+dx/d*travel,a.y+dy/d*travel,radius,profile))return false;a.x+=dx/d*travel;a.y+=dy/d*travel;a.dir=Math.atan2(dy,dx);a.moving=true;return true};
 const moved=distance2(a,target)<=30**2||step(target);
 if(!moved&&kind==='ape'&&a.state==='follow'){
 // A separated follower can exceptionally request a slow recovery corridor.
 // Retain its origin while queued, so coarse ticks do not restart the search.
 const path=a._abstractPath;if(path?.length){let index=a._abstractIndex||0;while(index<path.length-1&&distance2(a,path[index])<22**2&&this.navigation.clearSegment(a.x,a.y,path[index+1].x,path[index+1].y,radius,'ape'))index++;a._abstractIndex=index;if(step(path[index]))return;a._abstractPath=null}
 const ready=!this.world.boundsReady||this.world.boundsReady(Math.min(a.x,target.x)-radius,Math.min(a.y,target.y)-radius,Math.max(a.x,target.x)+radius,Math.max(a.y,target.y)+radius);
 if(ready&&this.time>=(a._abstractRetry||0)){
 const request=a._abstractRequest||(a._abstractRequest={from:{x:a.x,y:a.y},to:{x:target.x,y:target.y}}),route=this.navigation.recoveryPath(request.from,request.to,radius,5,'ape');
 if(route!==null){a._abstractPath=route;a._abstractIndex=0;a._abstractRequest=null;a._abstractRetry=this.time+2}
 }
 }
 if(!moved&&kind==='human'&&a.responseAllocated){a._navPriority=5;const waypoint=this.navigation.steer(a,target,radius,dt);step(waypoint)}
 }
 }
 updateCohorts(){
 if(this.time<this.nextCohortAt)return;this.nextCohortAt=this.time+.35;const groups=new Map();
 for(const a of this.apes){delete a.navCohort;if(a.hp<=0||a._simTier===2||!['follow','charge'].includes(a.state))continue;
 const target=a.state==='follow'?this.king:a.target;if(!target)continue;
 const id=a.state+':'+Math.floor(a.x/240)+','+Math.floor(a.y/240)+':'+Math.floor(target.x/160)+','+Math.floor(target.y/160);a.navCohort=id;
 if(!groups.has(id))groups.set(id,{leader:a,target});
 }
 for(const [id,{leader,target}]of groups)this.navigation.setCohortRoute?.(id,leader,target,10,3);
 }
 lineVisible(a,b,critical=false){
 if(this.siege&&(ATSSiegeAltitude(a)>0||ATSSiegeAltitude(b)>0)){if(!critical&&this.performance.losRemaining<=this.performance.heavyLosReserve)return null;if(!critical){this.performance.losRemaining--;this.performance.counters.losTests++}return this.siege.clearRay(a,b)}
 if(critical)return this.world.lineClear(a.x,a.y,b.x,b.y);
 const key=[Math.round(a.x/3),Math.round(a.y/3),Math.round(b.x/3),Math.round(b.y/3),this.world.navRevision].join(',');
 const cached=this.visibilityCache.get(key);if(cached&&this.time-cached.time<.08){this.performance.counters.losCacheHits++;return cached.clear}
 const heavy=a.id?.startsWith('vehicle')||a.id?.startsWith('heli'),reserve=heavy?(a.id?.startsWith('vehicle')&&this.performance.airReserve?4:0):this.performance.heavyLosReserve;
 if(this.performance.losRemaining<=reserve)return null;
 this.performance.losRemaining--;this.performance.counters.losTests++;const clear=this.world.lineClear(a.x,a.y,b.x,b.y);this.visibilityCache.set(key,{time:this.time,clear});
 if(this.visibilityCache.size>2048)this.visibilityCache.clear();return clear;
 }
 perceive(h,light){
 let scan=h._sense;if(!scan||this.time-scan.time>.4)scan=h._sense={time:this.time,targets:this.apeGrid.near(h.x,h.y,light.range).sort((a,b)=>distance2(a,h)-distance2(b,h)),index:0};
 for(;scan.index<scan.targets.length;scan.index++){
 const a=scan.targets[scan.index];if(a.hp<=0||distance2(a,h)>light.range**2)continue;
 if(distance2(a,h)<32**2){h._sense=null;return{seen:a}}
 let illuminated=false,pending=false;const dx=a.x-h.x,dy=a.y-h.y;
 if(Math.cos(h.dir)*dx+Math.sin(h.dir)*dy>Math.cos(light.angle)*Math.sqrt(dx*dx+dy*dy)){
 const clear=this.lineVisible(h,a);if(clear===null)pending=true;else if(clear)illuminated=true;
 }
 if(!illuminated)for(const l of this.lightCache){if(!['tower','vehicle','heli','flare','fieldlight'].includes(l.kind)||distance2(a,l)>=l.range*l.range)continue;
 if(l.angle<3&&Math.abs(angleDiff(Math.atan2(a.y-l.y,a.x-l.x),l.dir))>=l.angle)continue;
 const lit=this.lineVisible(l,a);if(lit===null){pending=true;break}if(!lit)continue;const clear=this.lineVisible(h,a);if(clear===null)pending=true;else if(clear)illuminated=true;if(illuminated||pending)break;
 }
 if(illuminated){h._sense=null;return{seen:a}}if(pending)return{pending:true};
 }
 h._sense=null;return{seen:null};
 }
 animateAttack(a,target,kind){
 const variants=['hook','slam','backhand','tackle','overhead','uppercut'];a.attackSerial=(a.attackSerial||0)+1;a.animation={kind:kind||variants[(ATSUtil.hash(a.id)+a.attackSerial)%variants.length],start:this.time,duration:a.id==='king'?.42:.5};a.attackTimer=a.animation.duration;if(target)a.dir=Math.atan2(target.y-a.y,target.x-a.x);
 }
 objectDistance(a,o){const x=Math.max(o.x-(o.w||o.r*2)/2,Math.min(a.x,o.x+(o.w||o.r*2)/2)),y=Math.max(o.y-(o.h||o.r*2)/2,Math.min(a.y,o.y+(o.h||o.r*2)/2));return Math.hypot(a.x-x,a.y-y)}
 hurt(a,damage,source){
 if(a.hp<=0)return;if(this.siege&&source?.species&&!this.siege.meleeAllowed(source,a))return;damage=this.siege?.absorb(a,damage,source)??damage;this._lightsAt=-1;if(a.id?.startsWith('human'))damage=this.forces.armor(a,damage,source);else if(a.id?.startsWith('vehicle'))damage=this.forces.vehicleDamage?.(a,damage,source)??damage;else if(a.settlementId)damage=this.colonies.absorb(a,damage);
 if(a.siegeTransition&&damage>0&&a.hp-damage<=0)this.siege?.detach(a);a.hitTimer=.28;a.aiming=null;a.lastHit=this.time;a.hp-=damage;
 if(a.hp<=0&&(a.wallClimb||a.onWallId)){const lift=a.wallClimbHeight||0;this.tactics?.detach(a);if(['shell','mortar','airstrike','grenade'].includes(source?.type))a.blastZ=Math.max(a.blastZ||0,lift)}
 if(a.hp<=0&&a.id!=='king'){this.apesById.delete(a.id);this.humansById.delete(a.id);const type=a.id?.startsWith('human')?'human':a.id?.startsWith('vehicle')?'vehicle':'ape';if(type==='vehicle')this.world.addVehicleWreck?.(a,this.time);this.corpses.push({...a,_nav:undefined,type,actorAge:a.age,actorState:a.state,fallVariant:ATSUtil.hash(a.id+this.time)%5,fallDir:source?Math.atan2(a.y-source.y,a.x-source.x):a.dir,age:0,life:distance2(a,this.king)<900**2?30:12});if(this.corpses.length>320){const distant=this.corpses.findIndex(c=>distance2(c,this.king)>900**2);this.corpses.splice(distant>=0?distant:0,1)}this.sound('fall',type==='human'?.45:.65,a.x)}this.effect('hit',a.x,a.y,{color:a.id==='king'?'#ef947a':'#bda17b',life:.25});
 if(a.id==='king'){a.lastHit=this.time;this.hitFlash=.6;this.sound('hit');if(a.hp<=0){this.end();if(['shell','mortar','airstrike','grenade'].includes(source?.type))this.launchBlastReaction(a,source,{radius:source.radius,power:source.type==='grenade'?.7:1})}}
 else if(a.id?.startsWith('human')){a.state='combat';a.lastX=source?.x??a.lastX;a.lastY=source?.y??a.lastY;a.searchTime=15;if(a.hp<=0){this.stats.humans++;if(a.siteId&&!a.responseAllocated){let s=this.world.sites.get(a.siteId);if(s)s.strength=Math.max(0,s.strength-1)}this.addIntel(a.x,a.y,1.5);this.effect('smoke',a.x,a.y,{life:1.4,color:'#738087'})}}
 else if(a.id?.startsWith('ape')&&a.hp<=0){this.stats.lost++;this.effect('smoke',a.x,a.y,{life:1.5,color:'#746456'})}
 if(a.hp<=0)this.forces.onDeath?.(a);
 }
 blastActive(a){return a?.blastReaction?.stage==='flight'||a?.blastReaction?.stage==='gettingUp'}
 launchBlastReaction(a,source,options={}){
 if(!a||!source||!Number.isFinite(a.x)||!Number.isFinite(a.y))return false;
 const wallLift=a.wallClimbHeight||a.elevation||0;if(a.siegeTransition||a.elevation)this.siege?.detach(a);if(a.wallClimb||a.onWallId)this.tactics?.detach(a);a.blastZ=Math.max(a.blastZ||0,wallLift);
 if(a.role==='engineer'&&a.engineerJob)this.forces.cancelEngineerJob?.(a,'blast');
 const radius=Math.max(1,options.radius||source.radius||100),gap=dist(a,source),falloff=clamp(1-gap/radius*.7,.25,1),power=clamp(options.power??1,.2,2);
 const angle=gap>1?Math.atan2(a.y-source.y,a.x-source.x):(ATSUtil.hash(a.id+this.time)%628)/100,impulse=(100+power*110)*falloff,height0=Math.max(2,a.blastZ||0),vz=(170+power*115)*Math.sqrt(falloff),gravity=640;
 const duration=(vz+Math.sqrt(vz*vz+2*gravity*height0))/gravity,spin=(ATSUtil.hash(a.id+Math.floor(this.time*10))%2?1:-1)*(2.3+power*2)*falloff;
 const previous=a.blastReaction,reaction={start:this.time,duration,elapsed:0,vx:Math.cos(angle)*impulse+(previous?.stage==='flight'?previous.vx*.25:0),vy:Math.sin(angle)*impulse+(previous?.stage==='flight'?previous.vy*.25:0),vz,height0,height:height0,gravity,spin,rotation:previous?.rotation||0,stage:'flight',recoveryDuration:.8+power*.45,origin:{x:a.x,y:a.y},lastSafe:{x:a.x,y:a.y},sourceKind:source.type||source.kind||'blast',recoverAt:this.time+duration+.8+power*.45};
 a.blastReaction=reaction;a.blastStart=this.time;a.blastDuration=duration;a.blastZ=height0;a.blastSpin=reaction.rotation;a.blastRecoveryUntil=reaction.recoverAt;a.knockbackUntil=this.time+duration;a.staggerUntil=reaction.recoverAt;a.moving=false;a.attackTimer=0;a.aiming=null;a.animation={kind:'blastFlight',start:this.time,duration};
 if(a.hp<=0)a.life=Math.max(a.life||0,duration+8);
 // Every affected body receives a flight state immediately. A small kickoff
 // allowance avoids doing a collision sweep for a whole swarm at impact time;
 // the ordinary bounded actor population advances all flights on later ticks.
 if(this.blastKickoffAt!==this.time){this.blastKickoffAt=this.time;this.blastKickoffRemaining=12}const kingPriority=a.id==='king'&&this.blastKingKickoffAt!==this.time;if(kingPriority||this.blastKickoffRemaining>0){if(kingPriority)this.blastKingKickoffAt=this.time;else this.blastKickoffRemaining--;this.updateBlastReaction(a,1/60)}return true;
 }
 blastBodies(source,radius,options={}){
 let count=0;for(const corpse of this.corpses){if(corpse.type==='vehicle'||distance2(corpse,source)>radius*radius||!this.world.lineClear(source.x,source.y,corpse.x,corpse.y))continue;if(this.launchBlastReaction(corpse,source,{...options,radius}))count++}return count;
 }
 blastLanding(a,reaction,profile,radius){
 if(!this.navigation.blocked(a.x,a.y,radius,profile))return;
 const origin={x:a.x,y:a.y};for(let ring=20;ring<=100;ring+=20)for(let i=0;i<8;i++){const angle=i/8*TAU,point={x:origin.x+Math.cos(angle)*ring,y:origin.y+Math.sin(angle)*ring};if(!this.navigation.blocked(point.x,point.y,radius,profile)){a.x=point.x;a.y=point.y;return}}
 const safe=reaction.lastSafe||reaction.origin;if(safe&&!this.navigation.blocked(safe.x,safe.y,radius,profile)){a.x=safe.x;a.y=safe.y}
 }
 blastSegmentClear(ax,ay,bx,by,radius,profile){this.performance.counters.blastCollisionSweeps++;return this.navigation.clearSegment(ax,ay,bx,by,radius,profile)}
 updateBlastReaction(a,dt){
 const reaction=a?.blastReaction;if(!reaction||reaction.stage==='landed')return false;
 a.moving=false;a.attackTimer=0;
 if(reaction.stage==='gettingUp'){reaction.recoveryElapsed=(reaction.recoveryElapsed||0)+dt;if(this.time>=reaction.recoverAt||reaction.recoveryElapsed>=reaction.recoveryDuration){delete a.blastReaction;a.blastZ=0;a.blastSpin=0;a.blastRecoveryUntil=0;a.animation=null;return false}a.animation={kind:'gettingUp',start:reaction.getUpAt,duration:reaction.recoveryDuration};return true}
 if(reaction.stage!=='flight')return false;
 this.performance.counters.blastIntegrations++;
 const profile=a.type==='human'||a.id?.startsWith('human')?'human':'ape',radius=a.id==='king'?12:a.state==='young'?7:10,travelDt=Math.min(dt,Math.max(0,reaction.duration-reaction.elapsed)),dx=reaction.vx*travelDt,dy=reaction.vy*travelDt;
 if(dx||dy){if(this.blastSegmentClear(a.x,a.y,a.x+dx,a.y+dy,radius,profile)){a.x+=dx;a.y+=dy;reaction.lastSafe={x:a.x,y:a.y}}
 else{let moved=false;if(dx&&this.blastSegmentClear(a.x,a.y,a.x+dx,a.y,radius,profile)){a.x+=dx;reaction.vy*=-.15;moved=true}else reaction.vx*=-.15;if(dy&&this.blastSegmentClear(a.x,a.y,a.x,a.y+dy,radius,profile)){a.y+=dy;reaction.vx*=-.15;moved=true}else reaction.vy*=-.15;if(moved)reaction.lastSafe={x:a.x,y:a.y}}}
 reaction.elapsed+=dt;reaction.height=Math.max(0,reaction.height0+reaction.vz*reaction.elapsed-.5*reaction.gravity*reaction.elapsed*reaction.elapsed);reaction.rotation+=reaction.spin*travelDt;a.blastZ=reaction.height;a.blastSpin=reaction.rotation;
 if(reaction.elapsed>=reaction.duration){this.blastLanding(a,reaction,profile,radius);reaction.height=a.blastZ=0;reaction.getUpAt=this.time;reaction.recoveryElapsed=0;reaction.recoverAt=this.time+reaction.recoveryDuration;reaction.vx=reaction.vy=0;
 if(a.hp>0){reaction.stage='gettingUp';reaction.rotation=0;a.blastSpin=0;a.blastRecoveryUntil=reaction.recoverAt;a.staggerUntil=reaction.recoverAt;a.animation={kind:'gettingUp',start:this.time,duration:reaction.recoveryDuration};this.effect('dust',a.x,a.y,{life:.6,color:'#b9b198'})}
 else{reaction.stage='landed';a.blastSpin=0;a.blastRecoveryUntil=0;a.fallDir=(a.fallDir||a.dir||0)+reaction.rotation;a.life=Math.max(a.life||0,8)}}
 return a.hp>0?this.blastActive(a):reaction.stage==='flight';
 }
 attack(aim=this.aim){
 if(this.ended||this.attackCD>0||this.blastActive(this.king))return;this.attackCD=.48;this.animateAttack(this.king);this.king.dir=Math.atan2(aim.y,aim.x);this.sound('attack');
 const wall=this.world.objects.get(this.king.onWallId);if(wall&&!wall.dead&&wall.hp>0){this.damageObject(wall,55,this.king);return}
 let candidates=this.humanGrid.near(this.king.x,this.king.y,75).filter(h=>this.world.lineClear(this.king.x,this.king.y,h.x,h.y)).concat(this.vehicles.filter(v=>v.hp>0&&dist(v,this.king)<85));let nearest=null;
 for(const h of candidates){if(!nearest||dist(h,this.king)<dist(nearest,this.king))nearest=h}
 let objs=this.world.getObjects(this.king.x,this.king.y,95).filter(o=>!o.dead&&o.hp>0&&!['tree','rock','berry','apeBuilding'].includes(o.type)&&!(o.fortification&&o.team==='ape'));
 if(nearest){this.king.dir=Math.atan2(nearest.y-this.king.y,nearest.x-this.king.x);this.hurt(nearest,65,this.king);this.noise(this.king.x,this.king.y,200,'fight')}else{
 let obj=objs.sort((a,b)=>this.objectDistance(this.king,a)-this.objectDistance(this.king,b))[0];if(obj&&this.objectDistance(this.king,obj)<53)this.damageObject(obj,55,this.king);else this.effect('hit',this.king.x+Math.cos(this.king.dir)*38,this.king.y+Math.sin(this.king.dir)*38,{life:.25,color:'#e6ce96'});
 }
 }
 damageObject(o,damage,attacker){
 if(o.type==='apeBuilding'&&o.settlementId){const s=this.settlement(o.settlementId),hut=s?.huts?.find(h=>h.id+':collision'===o.id),building=hut||s?.structures?.find(h=>h.id+':collision'===o.id)||s?.facilities?.find(h=>h.id+':collision'===o.id);if(hut)this.colonies.damageHut?.(s,hut,damage,attacker);else if(building){building.hp=Math.max(0,building.hp-damage);building.lastHit=this.time;this.world.syncSettlementBuildings?.(s,this)}return}
 if(o.fortification&&this.world.damageFortification){if(this.world.damageFortification(o,damage,this.time)){this._lightsAt=-1;this.effect('smash',o.x,o.y,{color:'#c6ac7e',life:.35});this.noise(o.x,o.y,180,'smash');if(o.dead){this.stats.structures++;this.visibilityCache.clear();this.sound('smash',1,o.x);this.effect('smoke',o.x,o.y,{color:'#b29d78',life:1.4})}}return}
 if(o.dead||!(o.hp>0))return;this._lightsAt=-1;o.hp-=damage;this.effect('smash',o.x,o.y,{color:'#c6ac7e',life:.35});
 if(o.type!=='cage'||o.siteId!=='opening-rescue')this.noise(o.x,o.y,180,'smash');
 if(o.hp<=0){o.hp=0;o.dead=true;o.solid=false;this.world.navRevision++;this.visibilityCache.clear();this.stats.structures++;this.sound('smash',1,o.x);this.effect('smoke',o.x,o.y,{color:'#b29d78',life:1.4});
 let s=o.siteId?this.world.sites.get(o.siteId):null;
 if(o.type==='cage'){o.rescueOpened=true;o.count=Math.max(0,o.count??o.prisoners??3);o.prisoners=o.count;this.releaseCaptives(o,true)}
 else if(o.type==='radio'){this.notify('Radio tower down. Regional reinforcements cut.','green');if(s)s.radioDown=true}
 else if(o.type==='alarm'){if(s){s.alarm=false;s.alarmDown=true}this.notify('The alarm station is silent.','green')}
 else if(o.type==='tower'){this.notify('Searchlight destroyed. Darkness returns.','green')}
 else if(o.type==='barracks'){if(s){s.strength=Math.floor(s.strength*.35);s.barracksDown=true}this.notify('Barracks destroyed. Local operations collapse.','green')}
 else if(o.type==='depot'){if(s)s.depotDown=true;this.notify('Vehicle depot destroyed. Mobile raids reduced.','green')}
 else if(o.type==='fuel'){if(s)s.fuelDown=true;for(const h of this.humanGrid.near(o.x,o.y,120))this.hurt(h,80,attacker);this.effect('wave',o.x,o.y,{color:'#e6a465',life:.65,range:120})}
 else if(o.type==='house'){this.food+=35;this.notify('Human supplies seized: +35 food.','green')}
 this.checkSite(s);
 }
 this.siege?.damagedObject(o);
 }
 releaseCaptives(o,announce=false){
 const remaining=Math.max(0,o.count??o.prisoners??0),num=Math.min(remaining,Math.max(0,MAX_APE_POPULATION-this.population));
 for(let i=0;i<num;i++)this.makeApe(o.x+(Math.random()-.5)*55,o.y+(Math.random()-.5)*55);
 o.count=o.prisoners=remaining-num;const site=this.world.sites.get(o.siteId);if(site){site.count=Math.max(0,(site.count||0)-num);site.rescued=site.count===0}
 if(num){this.rescueFocus.push({x:o.x,y:o.y,hp:1,until:this.time+15});this.stats.freed+=num;this.sound('rescue');this.addIntel(o.x,o.y,4)}
 if(!o.count&&!o.rescueCredited){o.rescueCredited=true;this.stats.prisons++}
 if(announce||num)this.notify(num+' apes join the tribe.'+(o.count?' Population limit reached — '+o.count+' captives remain.':this.population===MAX_APE_POPULATION?' Population limit reached — '+MAX_APE_POPULATION+' living apes.':' Roar [Q] to gather them.'),num?'green':'gold');
 return num;
 }
 checkSite(s){if(!s||s.cleared)return;let structural=s.objects.map(id=>this.world.objects.get(id)).filter(Boolean);if(!structural.some(o=>o.type==='cage'&&(!o.dead||o.rescueOpened&&(o.count||0)>0))&&!this.humans.some(h=>h.hp>0&&h.siteId===s.id)&&!(s.sleepingHumans||[]).some(h=>h.hp>0)&&(!structural.some(o=>o.type==='barracks'&&!o.dead)||s.strength<=0)){s.cleared=true;s.alarm=false;s.strength=0;this.stats.bases++;const supplies=s.tier>=2?s.tier*30:0;this.food+=supplies;this.notify(s.name+' liberated.'+(supplies?' +'+supplies+' food seized.':' The forest grows quiet.'),'green')}}
 addIntel(x,y,n){const k=Math.floor(x/768)+','+Math.floor(y/768);const old=this.world.intel.get(k)||{heat:0,lastSeen:0,x,y};if(typeof old==='number'){this.world.intel.set(k,{heat:old+n,lastSeen:this.time,x,y});return}old.heat=Math.min(100,(old.heat||0)+n);old.lastSeen=this.time;old.x=x;old.y=y;this.world.intel.set(k,old)}
 alert(h,kind='shout'){
 const radius=kind==='radio'?1300:kind==='alarm'?900:360;const s=this.world.sites.get(h.siteId);if(kind==='alarm'&&s){s.alarm=true;s.alarmUntil=this.time+45}h.reported=true;this.noise(h.lastX,h.lastY,radius,'alert');this.addIntel(h.lastX,h.lastY,kind==='shout'?3:8);this.lastContact=this.time;
 if(kind==='radio'){this.sound('radio');this.notify('Radio contact confirmed. Reinforcements are moving.','red');if(s&&!s.radioDown)this.spawnRaid(s,{x:h.lastX,y:h.lastY},false)}else if(kind==='alarm'){this.sound('alarm');this.notify('An installation alarm is ringing.','red')}else this.sound('alarm',.4,h.x);
 const density=this.forces.observedDensity?.(h,{x:h.lastX,y:h.lastY});if(density!==null&&density>0&&kind==='radio'&&!s?.radioDown)this.recordHordeContact(h,{x:h.lastX,y:h.lastY},density);
 for(const other of this.humans){if(other.hp>0&&dist(other,h)<radius&&other!==h){other.lastX=h.lastX;other.lastY=h.lastY;other.state='search';other.searchTime=25}}
 for(const st of this.settlements){if(dist(st,h)<500&&!st.known&&st.population>0){st.known=true;this.notify(st.name+' discovered. They know.','red')}}
 }
 spawnRaid(site,target,settlementRaid=false,options={}){
 if(!site||!target||!Number.isFinite(target.x)||!Number.isFinite(target.y)||site.cleared||site.strength<=0||this.humans.length>=this.activeHumanCapacity()||this.time-(site.lastRaid??0)<30)return false;
 if(!options.operationId&&this.activeOperations().length>=this.operationCapacity)return false;
 if(!this.prepareMilitarySource(site))return false;
 const regionalReserve=!options.operationId&&!settlementRaid&&this.population>=300&&this.settlements.some(s=>s.known&&s.population>0)?Math.max(0,this.responseBudget*.7-this.forceWeight()):Infinity;
 const pack=this.responsePackage(site,target,settlementRaid,options),remaining=Math.min(this.responseBudget-this.forceWeight(),options.budgetAllowance??Infinity,regionalReserve);
 const vehicles=[];let cost=0,armor=0;const armorLimit=pack.capacity.armorCapacity??site.armorCapacity??Infinity;
 for(const kind of pack.vehicles){const weight=this.militaryVehicleProfile(kind,vehicles.length).spec.budget||4,heavy=['tank','ifv','apc'].includes(kind)?weight:0;if(!this.vehicleRoom(kind,vehicles)||armor+heavy>armorLimit||cost+weight+6>remaining)continue;vehicles.push(kind);cost+=weight;armor+=heavy}
 const roleList=pack.stage===5?['leader','rifleman','shield','rifleman','heavy','grenadier','medic','engineer','ranger','sniper','mortar','rifleman']:pack.stage>=4?['leader','rifleman','shield','ranger','heavy','grenadier','medic','sniper','engineer','rifleman','heavy','ranger']:pack.stage>=3?['leader','rifleman','rifleman','flanker','gunner','grenadier','medic','sniper','rifleman','rifleman','tracker','rifleman']:pack.stage===2?['officer','shield','tracker','flanker','medic','guard','guard','guard','tracker']:['officer','tracker','guard','guard','guard'];
 const roleAt=i=>pack.stage===5&&this.warIntensity>=3?this.forces.militaryRole(i):roleList[i%roleList.length];
 const allocationWeight=number=>{let weight=cost,cargo=Math.max(0,number-12),carried=0;for(const kind of vehicles){const seats=window.ATSVehicleSpecs?.[kind]?.capacity||0,troops=Math.min(cargo,seats);cargo-=troops;carried+=troops;for(let i=0;i<troops;i++)weight+=this.humanWeight(this.forces.militaryRole(i))}for(let i=0;i<number-carried;i++)weight+=this.humanWeight(roleAt(i));return weight};
 const sleeping=this.vehicles.reduce((sum,v)=>sum+(v.troops||0),0);let number=Math.floor(Math.min(options.peopleCap??Infinity,pack.people,this.activeHumanCapacity()-this.humans.length-sleeping));
 while(number>=3&&allocationWeight(number)>remaining)number--;
 if(number<3)return false;
 const operationId=options.operationId||'operation-'+this.nextId++,snapshot={x:target.x,y:target.y,id:target.id},start={x:site.x,y:site.y},squads=[],created=[];let cargo=Math.max(0,number-12),carried=0;
 for(const [vehicleIndex,kind]of vehicles.entries()){const seats=window.ATSVehicleSpecs?.[kind]?.capacity||0,troops=Math.min(cargo,seats);const v=this.makeMilitaryVehicle(site,kind,snapshot,troops,operationId,vehicleIndex);if(!v)continue;cargo-=troops;carried+=troops;created.push(v);if(site.vehicleInventory)site.vehicleInventory[kind]=Math.max(0,(site.vehicleInventory[kind]||0)-1);if(['tank','ifv','apc'].includes(kind)&&Number.isFinite(site.armorCapacity))site.armorCapacity=Math.max(0,site.armorCapacity-(this.forces.vehicleSpec(v).budget||4))}
 // A blocked staging slot can change the carried/on-foot split. Charge the
 // resulting roles before committing any personnel from the installation.
 let actualCost=created.reduce((n,v)=>n+this.forces.vehicleSpec(v).budget+this.cargoWeight(v),0),foot=0;while(foot<number-carried&&actualCost+this.humanWeight(roleAt(foot))<=remaining){actualCost+=this.humanWeight(roleAt(foot));foot++}number=carried+foot;
 for(let i=0;i<foot;i++){const h=this.makeHuman(start.x+(i%4)*26,start.y+Math.floor(i/4)*26,site);this.forces.assign(h,site,roleAt(i));h.state='search';h.lastX=target.x;h.lastY=target.y;h.searchTime=150;h.raidTarget=settlementRaid?target.id:null;h.assaultPhase=options.assaultPhase;h.reported=true;h.responseAllocated=true;h.operationId=operationId;squads.push(h)}
 const formations=[];for(let i=0;i<squads.length;i+=12){const squad=this.forces.createSquad?.(squads.slice(i,i+12),snapshot,{order:settlementRaid?(options.assaultPhase?'Approach':'Attack Settlement'):options.intercept?'Hold Line':'Search',vehicleId:created[i===0?0:Math.min(created.length-1,1)]?.id,operationId,siteId:site.id});if(squad){squad.assaultPhase=options.assaultPhase;formations.push(squad)}}
 for(const platoon of this.forces.platoons?.values?.()||[])if(platoon.operationId===operationId)platoon.order=settlementRaid?'Attack Settlement':options.intercept?'Intercept Horde':'Encircle';
 for(const v of created){v.assaultPhase=options.assaultPhase;if(settlementRaid)v.settlementTarget=target.id}
 site.lastRaid=this.time;site.strength=Math.max(0,site.strength-number);this.operationSerial=(this.operationSerial||0)+1;
 if(!options.operationId)this.militaryOperations.push({id:operationId,name:settlementRaid?'SETTLEMENT RAID':'RADIO RESPONSE',channel:settlementRaid?'regional':'field',target:snapshot,status:'active',phase:settlementRaid?'assault':'pursuit',phaseAt:this.time,createdAt:this.time,expiresAt:this.time+180,origins:[site.id],people:number,staging:settlementRaid?this.assaultStaging(target,site):null,engineerJobs:[],intensity:this.warIntensity});
 this.events.push({type:'raid',package:pack.name,operationId,x:site.x,y:site.y,targetX:target.x,targetY:target.y,settlementId:settlementRaid?target.id:null,people:number,vehicles:created.map(v=>v.kind),time:this.time});if(this.events.length>30)this.events.shift();
 if(settlementRaid)this.notify('ARMY APPROACHING '+(options.settlement?.name||target.name||'settlement').toUpperCase()+' — '+pack.name+' departing '+(site.name||'a human installation')+'.','red','villageAttack');else if(created.length)this.notify(pack.name+' deploying from '+(site.name||'a human installation')+'.','red');
 this.sound(pack.stage>=4?'military':pack.stage>=3?'radio':'alarm',.65,site.x);return true;
 }
 getLights(){
 const lights=[];for(const h of this.humans){if(h.hp>0&&dist(h,this.king)<1050)lights.push({id:h.id,x:h.x,y:h.y,dir:h.dir,range:this.forces.range(h),angle:h.state==='combat'?.43:.34,kind:'flashlight',active:true})}
 this.observationPosts=[];for(const o of this.world.getObjects(this.king.x,this.king.y,1100)){if(o.dead||o.hp<=0)continue;if(o.type==='tower'){const s=this.world.sites.get(o.siteId);lights.push({id:o.id,x:o.x,y:o.y,dir:o.trackUntil>this.time?o.trackAngle:(o.angle||0)+Math.sin(this.time*(o.sweep||.2))*1.2,range:o.scanRange||480+(s?.tier||1)*25,angle:.48,elevation:o.height||95,kind:'tower',active:true})}else if(o.type==='fieldSearchlight')lights.push({id:o.id,x:o.x,y:o.y,dir:o.dir??o.angle??0,range:390,angle:.65,kind:'fieldlight',active:true});else if(['fieldObservation','observationPost'].includes(o.type))this.observationPosts.push(o);else if(o.type==='alarm'&&this.world.sites.get(o.siteId)?.alarm)lights.push({id:o.id,x:o.x,y:o.y,dir:0,range:75,angle:Math.PI,kind:'alarm',active:true})}
 for(const v of this.vehicles){if(v.hp>0&&dist(v,this.king)<1000){const heavy=['tank','apc','ifv'].includes(v.vehicleClass||v.kind);lights.push({id:v.id,x:v.x,y:v.y,dir:v.dir,range:heavy?540:350,angle:.39,kind:'vehicle',active:true});if(heavy)lights.push({id:v.id+':search',x:v.x,y:v.y,dir:(v.turretDir??v.dir)+Math.sin(this.time*.6)*.24,range:630,angle:.19,kind:'vehicle',active:true})}}
 for(const f of this.forces.hazards)if(f.type==='flare')lights.push({id:f.id,x:f.x,y:f.y,dir:0,range:f.radius,angle:Math.PI,kind:'flare',active:true});
 for(const h of this.helis)if(h.hp>0)lights.push({id:h.id+':spot',x:h.spotX,y:h.spotY,dir:0,range:95,angle:Math.PI,kind:'heli',active:true});this.lightCache=lights;return lights;
 }
 lit(target,light){const d=dist(target,light);return d<light.range&&(light.angle>=3||Math.abs(angleDiff(Math.atan2(target.y-light.y,target.x-light.x),light.dir))<light.angle)&&this.world.lineClear(light.x,light.y,target.x,target.y)}
 update(dt,input={}){
 this.performance.beginStep(this);
 if(this.ended){this.deathTimer+=dt;this.king.deathAge=this.deathTimer;this.updateBlastReaction(this.king,dt);for(const a of this.apes)if(this.blastActive(a))this.updateBlastReaction(a,dt);for(const h of this.humans)if(this.blastActive(h))this.updateBlastReaction(h,dt);for(const c of this.corpses){c.age+=dt;c.life-=dt;this.updateBlastReaction(c,dt)}this.compact(this.corpses,c=>c.life>0);for(const fx of this.effects)fx.life-=dt;this.compact(this.effects,fx=>fx.life>0,this.effectPool);this.performance.finishStep(this);return}
 this.time+=dt;this.day=1+Math.floor(this.time/180);this.commandCD=Math.max(0,this.commandCD-dt);this.attackCD=Math.max(0,this.attackCD-dt);this.king.attackTimer=Math.max(0,this.king.attackTimer-dt);this.hitFlash=Math.max(0,this.hitFlash-dt);this.king.moving=false;
 const mx=input.x||0,my=input.y||0,mag=Math.hypot(mx,my);
 this.navigation.beginFrame(this.time);if(this.world.stream)this.world.stream(this.king.x,this.king.y,1200,{budgetMs:1.5,maxSteps:3,dx:mx,dy:my});else this.world.ensure(this.king.x,this.king.y,1200);
 if(this.time>=this.nextSpawnAt){this.nextSpawnAt=this.time+.25;this.spawnSites()}
 this.syncIndexes();this.apeGrid.rebuild([this.king,...this.apes]);this.humanGrid.rebuild(this.humans);this.vehicleGrid.rebuild(this.vehicles);this.buildRelevance();
 const kingRecovering=this.updateBlastReaction(this.king,dt);if(mag>.04&&!kingRecovering)this.move(this.king,mx*100,my*100,112*(input.sprint?1.24:1)*(this.king.hp<30?.75:1)*(input.sneak?.62:1),dt);
 if(input.sprint&&mag>.04&&this.time>this.sprintNoiseTime){this.sprintNoiseTime=this.time+.9;this.noise(this.king.x,this.king.y,130+Math.min(220,this.followerCount*2),'footsteps')}
 if(input.aim&&Math.hypot(input.aim.x,input.aim.y)>.05)this.aim={...input.aim};if(input.attack)this.attack(this.aim);
 if(this.time-this.king.lastHit>8)this.king.hp=Math.min(this.king.maxHp,this.king.hp+2.4*dt);
 this.followSpread=Math.max(1,Math.sqrt(this.followCount/32));this.siege?.tick(dt);this.updateCohorts();
 for(const a of this.apes)if(a.hp>0){const elapsed=this.simulateActor(a,dt,'ape');if(elapsed)this.updateApe(a,elapsed)}
 this.spreadApes(dt);
 this.humanUpdateOffset=((this.humanUpdateOffset||0)+1)%Math.max(1,this.humans.length);for(let i=0;i<this.humans.length;i++){const h=this.humans[(i+this.humanUpdateOffset)%this.humans.length];if(h.hp>0){const elapsed=this.simulateActor(h,dt,'human');if(elapsed&&!this.updateBlastReaction(h,elapsed)&&!this.siege?.updateHuman(h,elapsed)&&!this.forces.update(h,elapsed))this.updateHuman(h,elapsed)}}
 this.vehicleUpdateOffset=((this.vehicleUpdateOffset||0)+1)%Math.max(1,this.vehicles.length);for(let i=0;i<this.vehicles.length;i++){const v=this.vehicles[(i+this.vehicleUpdateOffset)%this.vehicles.length];if(v.hp>0){const elapsed=this.simulateActor(v,dt,'vehicle');if(elapsed)this.updateVehicle(v,elapsed)}}
 for(const h of this.helis){if(distance2(h,this.king)>1650**2&&!this.focusGrid.near(h.x,h.y,600).length){h._lodElapsed=(h._lodElapsed||0)+dt;if(h._lodElapsed<.25)continue;const elapsed=h._lodElapsed;h._lodElapsed=0;this.updateHeli(h,elapsed)}else this.updateHeli(h,dt)}
 this.compact(this.helis,h=>h.hp>0&&Math.abs(h.x-this.king.x)<3500&&Math.abs(h.y-this.king.y)<3500);
 this.updateBullets(dt);this.colonies.updateDefenses?.(dt);this.forces.tick(dt);for(const c of this.corpses){c.age+=dt;c.life-=dt;this.updateBlastReaction(c,dt)}this.compact(this.corpses,c=>c.life>0);this.king.hitTimer=Math.max(0,(this.king.hitTimer||0)-dt);
 this.secondTimer+=dt;if(this.secondTimer>=1){this.secondTimer-=1;this.tickSecond(true)}this.tickColonies();
 this.lightTimer-=dt;if(this.lightTimer<=0){this.lightTimer=.12;const lights=this.getLights();const exposed=lights.some(l=>this.lit(this.king,l));this.exposure=clamp(this.exposure+(exposed?.34:-.2),0,1)}
 for(const fx of this.effects)fx.life-=dt;this.compact(this.effects,fx=>fx.life>0,this.effectPool);for(const n of this.noises)n.life-=dt;this.compact(this.noises,n=>n.life>0,this.noisePool);
 if(this.time>=(this.nextTrimAt||0)){this.nextTrimAt=this.time+2;const pins=this.settlements.filter(s=>s.population>0).concat(this.rescueFocus);for(const list of [this.apes,this.humans,this.vehicles])for(const a of list)if(a.hp>0&&a._simTier!==2)pins.push(a);for(const s of this.world.sites.values())if(s.alarm)pins.push(s);this.world.trimDistant?.(this.king.x,this.king.y,pins)}
 this.cannonShake=Math.max(0,(this.cannonShake||0)-dt*20);
 this.performance.finishStep(this);
 }
 compact(list,keep,pool){let write=0;for(let read=0;read<list.length;read++){const object=list[read];if(keep(object))list[write++]=object;else pool?.release(object)}list.length=write}
 updateApe(a,dt){
 a.hitTimer=Math.max(0,(a.hitTimer||0)-dt);a.attackTimer=Math.max(0,a.attackTimer-dt);a.attackCD=Math.max(0,a.attackCD-dt);a.moving=false;
 if(this.updateBlastReaction(a,dt)){a.age+=dt;return}
 const st=a.settlementId?this.settlement(a.settlementId):null;
 a.age+=dt;if(a.state==='young'){if(a.age>=GROW_UP){a.state='settled';a.hp=a.maxHp=APE_HP;a.speed=90;this.siege?.balance(a,true);this.effect('text',a.x,a.y,{text:'Grown',color:'#b1d5a0',life:2})}else{if(st){const activity=this.colonies.activityTarget?.(a,st)||{x:st.x+Math.cos(this.time*.28+a.phase)*35,y:st.y+Math.sin(this.time*.28+a.phase)*35,speed:a.speed};this.move(a,activity.x-a.x,activity.y-a.y,activity.speed??a.speed,dt)}return}}
 if(a.rallyUntil>this.time)a.attackCD=Math.max(0,a.attackCD-dt*.15);if(this.siege?.updateApe(a,dt))return;
 if(this.tactics?.updateClimb(a,dt))return;
 if(this.tactics&&a.state==='charge'){a.chargeTime-=dt;if(a.chargeTime<=0){a.state='hold';a.target={x:a.x,y:a.y};this.tactics.clearOrder(a)}}
 if(this.tactics?.combat(a,dt,st))return;
 let enemy=null;let range=a.state==='charge'?185:a.state==='scout'?165:FOLLOW.has(a.state)?90:st?.attack?190:70;
 if(a.state==='follow'&&a.retreatUntil>this.time)range=24;
 if(!this.tactics){if(this.time>=(a._enemyThink||0)&&this.performance.think('ape')){a._enemyThink=this.time+.18+(ATSUtil.hash(a.id)%11)*.008;const found=this.humanGrid.nearest(a.x,a.y,range,1)[0];a._enemyId=found?.id}enemy=this.humansById.get(a._enemyId);if(enemy&&(enemy.hp<=0||distance2(a,enemy)>range**2))enemy=null;}
 if(!this.tactics&&(a.state==='charge'||enemy)){const barrier=this.world.getObjects(a.x,a.y,110).filter(o=>!o.dead&&o.hp>0&&o.solid&&(o.faction==='human'||['humanBarricade','heavyBarricade','fieldBarricade'].includes(o.type))&&this.objectDistance(a,o)<32).sort((x,y)=>this.objectDistance(a,x)-this.objectDistance(a,y))[0];if(barrier){if(a.attackCD<=0){a.attackCD=.65;this.animateAttack(a,barrier,'overhead');this.damageObject(barrier,this.apeDamage(a,20+Math.min(20,this.apeGrid.near(a.x,a.y,60).length*2)),a)}return}}
 if(enemy&&(a.state!=='free'||dist(a,enemy)<30)){
 if(dist(a,enemy)>30||!this.world.lineClear(a.x,a.y,enemy.x,enemy.y))this.move(a,enemy.x-a.x,enemy.y-a.y,a.speed*(a.state==='charge'?1.35:1.12),dt);else if(a.attackCD<=0){a.attackCD=.7+Math.random()*.3;this.animateAttack(a,enemy);const swarm=this.apeGrid.near(enemy.x,enemy.y,48).length;this.hurt(enemy,this.apeDamage(a,18+Math.min(12,swarm*2)),a)}return;
 }
 if(a.state==='charge'){
 if(!this.tactics)a.chargeTime-=dt;let target=a.target||this.king;
 let objs=this.tactics?[]:this.world.getObjects(a.x,a.y,75).filter(o=>!o.dead&&o.hp>0&&!['tree','rock','berry','apeBuilding'].includes(o.type)&&!(o.fortification&&o.team==='ape'));let obj=objs.sort((x,y)=>dist(a,x)-dist(a,y))[0];
 let vehicle=this.tactics?null:this.vehicleGrid.nearest(a.x,a.y,80,1)[0];
 if(vehicle){if(dist(a,vehicle)>40)this.move(a,vehicle.x-a.x,vehicle.y-a.y,a.speed*1.3,dt);else if(a.attackCD<=0){a.attackCD=.65;this.animateAttack(a,vehicle,'slam');this.hurt(vehicle,this.apeDamage(a,24),a);if(vehicle.hp<=0){this.stats.structures++;this.sound('smash',2,vehicle.x)}}return}
 if(obj&&this.objectDistance(a,obj)<26){if(a.attackCD<=0){a.attackCD=.75;this.animateAttack(a,obj,'overhead');this.damageObject(obj,this.apeDamage(a,18),a)}return}
 if(a.chargeTime<=0||dist(a,this.king)>1050||a.commandStyle!=='attackNearest'&&dist(a,target)<30){a.state='hold';a.target={x:a.x,y:a.y};this.tactics?.clearOrder(a)}else if(dist(a,target)>=30)this.move(a,target.x-a.x,target.y-a.y,a.speed*1.4,dt);
 }else if(a.state==='follow'){
 const spread=this.followSpread||1;
 let tx=this.king.x+a.offsetX*spread+Math.sin(this.time*.9+a.phase)*16,ty=this.king.y+a.offsetY*spread+Math.cos(this.time*.8+a.phase)*16;
 const kingGap=Math.hypot(tx-this.king.x,ty-this.king.y);
 if(kingGap<42){const angle=kingGap>.001?Math.atan2(ty-this.king.y,tx-this.king.x):a.phase;tx=this.king.x+Math.cos(angle)*42;ty=this.king.y+Math.sin(angle)*42}
 // Clearance routes own obstacle avoidance. Old trail targets could pull a
 // follower backwards forever when the king had already crossed a clearing.
 let d=Math.hypot(tx-a.x,ty-a.y);if(d>25)this.move(a,tx-a.x,ty-a.y,a.speed*(dist(a,this.king)>180?1.55:1),dt);
 }else if(a.state==='hold'){
 if(a.target&&dist(a,a.target)>28)this.move(a,a.target.x-a.x,a.target.y-a.y,a.speed,dt);
 }else if(st&&(a.state==='settled'||a.state==='scout')){
 const radius=a.state==='scout'?st.radius*1.9:Math.max(32,st.radius*.7),ang=this.time*(a.state==='scout'?.14:.08)+a.phase,activity=this.colonies.activityTarget?.(a,st)||{x:st.x+Math.cos(ang)*radius,y:st.y+Math.sin(ang)*radius,speed:a.speed*.55};
 this.move(a,activity.x-a.x,activity.y-a.y,activity.speed??a.speed*.55,dt);
 if(a.state==='scout'&&this.time>=(a.nextWarn||0)){const threat=this.humanGrid.near(a.x,a.y,280).find(h=>h.state!=='patrol');if(threat){a.nextWarn=this.time+20;this.notify('Distant roars — '+st.name+' scouts sight a search party.','red','villageAttack');st.attack=true;this.sound('recall',.6,a.x)}}
 }else if(a.state==='free'){
 this.move(a,a.x+Math.sin(this.time*.5+a.phase)*20-a.x,a.y+Math.cos(this.time*.5+a.phase)*20-a.y,22,dt);
 }
 }
 updateHuman(h,dt){
 h.shootTimer-=dt;h.perceptionTimer-=dt;h.attackTimer=Math.max(0,(h.attackTimer||0)-dt);h.moving=false;
 if(h._simTier===2)return;
 const site=this.world.sites.get(h.siteId);
 if(h.perceptionTimer<=0&&this.performance.think()){h.perceptionTimer=.2+(ATSUtil.hash(h.id)%100)/1000;const post=this.observationPosts?.some(o=>!o.dead&&distance2(o,h)<280**2),light={x:h.x,y:h.y,dir:h.dir,range:this.forces.range(h)*(post?1.15:1),angle:h.state==='combat'?.65:.34};let seen=null;
 const perception=this.perceive(h,light);seen=perception.seen;if(perception.pending)h.perceptionTimer=0;
 if(seen){this.siege?.report(h,seen);h.lastSeenAt=this.time;h.suspicion=clamp(h.suspicion+.32,0,1);h.lastX=seen.x;h.lastY=seen.y;h.searchTime=14;this.lastContact=this.time;if(h.suspicion>=1){if(!h.reported){this.alert(h,'shout');const alarm=site?.objects.map(id=>this.world.objects.get(id)).find(o=>o?.type==='alarm'&&!o.dead);if(alarm&&dist(h,alarm)<270){h.state='alarm';h.alarmTarget=alarm.id;h.radioTimer=0}else if(h.hasRadio&&!(site?.radioDown)){h.state='radio';h.radioTimer=2.3}else h.state='combat'}else if(h.state!=='radio'&&h.state!=='alarm')h.state='combat';h.targetId=seen.id}}
 else if(!perception.pending){h.suspicion=Math.max(0,h.suspicion-.06);if(h.state==='combat'){h.state='search';h.searchTime=18;h.targetId=null}}
 // A very large moving horde leaves audible evidence, without pinpoint vision.
 if(!seen&&!perception.pending&&this.followerCount>30&&this.king.moving&&dist(h,this.king)<Math.min(430,120+this.followerCount*2)&&h.state==='patrol'){h.state='investigate';h.lastX=this.king.x+(Math.random()-.5)*120;h.lastY=this.king.y+(Math.random()-.5)*120;h.searchTime=12}
 }
 if(this.forces.updateDoctrine?.(h,dt,true))return;
 if(h.state==='radio'){h.radioTimer-=dt;h.dir=Math.atan2(h.lastY-h.y,h.lastX-h.x);if(h.radioTimer<=0){this.alert(h,'radio');h.state='combat'}return}
 if(h.state==='alarm'){
 const alarm=this.world.objects.get(h.alarmTarget);if(!alarm||alarm.dead){h.state='combat';return}if(dist(h,alarm)>35)this.move(h,alarm.x-h.x,alarm.y-h.y,83,dt);else{h.radioTimer+=dt;if(h.radioTimer>.8){this.alert(h,'alarm');h.state='combat'}}return;
 }
 if(h.state==='combat'){
 let target=h.targetId==='king'?this.king:this.apesById.get(h.targetId);if(!target||target.hp<=0){h.state='search';return}
 this.forces.combatMovement(h,target,dt);
 if(h.shootTimer<=0&&dist(h,target)<this.forces.range(h)&&(this.siege?this.siege.clearRay(h,target):this.world.lineClear(h.x,h.y,target.x,target.y)))this.shoot(h,target);
 }else if(h.state==='search'||h.state==='investigate'){
 h.searchTime-=dt;
 if(h.searchTime<=0){h.state='patrol';h.reported=false;h.suspicion=0;h.raidTarget=null}
 else if(Math.hypot(h.lastX-h.x,h.lastY-h.y)>35)this.move(h,h.lastX-h.x,h.lastY-h.y,72,dt);
 else{h.dir+=dt*1.2;if(h.nextPatrol<this.time){h.nextPatrol=this.time+3;h.lastX+=Math.cos(h.phase+this.time)*80;h.lastY+=Math.sin(h.phase+this.time)*80}}
 }else{
 if(h.nextPatrol<this.time||Math.hypot(h.patrolX-h.x,h.patrolY-h.y)<25){h.nextPatrol=this.time+6+Math.random()*5;let aa=Math.random()*TAU;h.patrolX=h.homeX+Math.cos(aa)*150;h.patrolY=h.homeY+Math.sin(aa)*150}
 this.move(h,h.patrolX-h.x,h.patrolY-h.y,36,dt);
 }
 }
 shoot(h,target){
 if(this.siege&&!this.siege.clearRay(h,target))return;
 const difficulty=this.difficulty==='wanderer'?.7:this.difficulty==='relentless'?1.15:1;
 const config={sniper:[2.8,300,1],pistol:[1.25,38,1],rifle:[.9,54,1],assault:[.32,35,1],shotgun:[1.8,22,4],machine:[.2,35,1]}[h.kind]||[1.2,38,1];h.shootTimer=config[0]/difficulty+Math.random()*.12;h.attackTimer=.14;h.animation={kind:'recoil',start:this.time,duration:.2};
 const d=dist(h,target),base=Math.atan2(target.y-h.y,target.x-h.x),error=(Math.random()-.5)*(h.kind==='sniper'?.025+d/6000:.07+d/3800)*(h.accuracyMultiplier||1);
 for(let i=0;i<config[2];i++){const angle=base+error+(i-(config[2]-1)/2)*.06;this.bullets.push(this.bulletPool.take({x:h.x+Math.cos(angle)*18,y:h.y+Math.sin(angle)*18,px:h.x,py:h.y,vx:Math.cos(angle)*550,vy:Math.sin(angle)*550,damage:config[1]*difficulty,life:h.kind==='sniper'?1.2:.7,owner:h.id,z:(h.elevation||h.wallClimbHeight||0)+30,pz:(h.elevation||h.wallClimbHeight||0)+30,vz:((target.elevation||target.wallClimbHeight||0)+18-((h.elevation||h.wallClimbHeight||0)+30))/Math.max(.05,dist(h,target)/550)}))}
 this.sound('gun',.65,h.x);this.effect('muzzle',h.x+Math.cos(base)*23,h.y+Math.sin(base)*23,{life:.1,color:'#f8da9a'});this.noise(h.x,h.y,300,'gun');
 }
 updateBullets(dt){
 for(const b of this.bullets){b.life-=dt;if(b.life<=0)continue;
 const x=b.x,y=b.y,dx=b.vx*dt,dy=b.vy*dt,length2=dx*dx+dy*dy,length=Math.sqrt(length2),z=b.z??20,endZ=z+(b.vz||0)*dt;b.px=x;b.py=y;b.pz=z;
 let target=null,hit=1;
 for(const a of this.apeGrid.near(x+dx*.5,y+dy*.5,length*.5+16)){
 if(a.hp<=0||!length2)continue;const px=a.x-x,py=a.y-y,projection=(px*dx+py*dy)/length2,t=clamp(projection,0,1),distance=(px-dx*t)**2+(py-dy*t)**2;
 if(distance>16**2||this.siege&&Math.abs(z+(endZ-z)*t-((a.elevation||a.wallClimbHeight||0)+18))>23)continue;
 const cross=(px-dx*projection)**2+(py-dy*projection)**2,entry=Math.max(0,projection-Math.sqrt(Math.max(0,16**2-cross)/length2));
 if(entry<=hit){hit=entry;target=a}
 }
 const endX=x+dx*hit,endY=y+dy*hit;
 const clearSegment=(bx,by,f=hit)=>this.siege?this.siege.clearRay({x,y},{x:bx,y:by},{za:z,zb:z+(endZ-z)*f,projectile:true}):this.projectileSegmentClear(x,y,bx,by);
 if(!clearSegment(endX,endY)){
 let lo=0,hi=hit;for(let i=0;i<5;i++){const mid=(lo+hi)/2;if(clearSegment(x+dx*mid,y+dy*mid,mid))lo=mid;else hi=mid}
 b.x=x+dx*lo;b.y=y+dy*lo;b.life=0;this.effect('hit',b.x,b.y,{life:.16,color:'#c7bfa3'});
 }else{b.x=endX;b.y=endY;b.z=z+(endZ-z)*hit;if(target){this.hurt(target,b.damage,{x,y,type:'bullet',owner:b.owner});b.life=0}}
 }
 this.compact(this.bullets,b=>b.life>0,this.bulletPool);
 }
 projectileSegmentClear(ax,ay,bx,by){
 if(!this.world.projectileBlocked)return this.navigation.clearSegment(ax,ay,bx,by,2);
 if(this.world.boundsReady&&!this.world.boundsReady(Math.min(ax,bx)-2,Math.min(ay,by)-2,Math.max(ax,bx)+2,Math.max(ay,by)+2))return false;
 const steps=Math.max(1,Math.ceil(Math.hypot(bx-ax,by-ay)/8));for(let i=0;i<=steps;i++)if(this.world.projectileBlocked(ax+(bx-ax)*i/steps,ay+(by-ay)*i/steps,2))return false;return true;
 }
 updateVehicle(v,dt){
 if(this.forces.updateVehicle?.(v,dt))return;
 v.shootTimer-=dt;v.phase+=dt;if(v._simTier===2)return;
 if(v.state==='raid'&&v.target){if(dist(v,v.target)>85)this.move(v,v.target.x-v.x,v.target.y-v.y,v.kind==='armored'?70:95,dt);else v.state='combat'}
 const targets=this.apeGrid.near(v.x,v.y,290);let target=null;for(const a of targets)if(!target||distance2(a,v)<distance2(target,v))target=a;if(target&&this.world.lineClear(v.x,v.y,target.x,target.y)){v.dir=Math.atan2(target.y-v.y,target.x-v.x);if(v.shootTimer<=0){const oldKind=v.kind;v.kind=v.kind==='armored'?'machine':'rifle';this.shoot(v,target);v.kind=oldKind;v.shootTimer=v.kind==='armored'?.35:1.2}}
 }
 updateHeli(h,dt){
 h.phase=(h.phase||0)+dt;h.shootTimer=(h.shootTimer||0)-dt;h.attackTimer=Math.max(0,(h.attackTimer||0)-dt);h.life=(h.life??110)-dt;
 const center=h.target||{x:h.x+Math.cos(h.dir)*500,y:h.y+Math.sin(h.dir)*500};
 if(h.life<15){const base=this.world.sites.get(h.siteId),goal=base||{x:center.x+1600,y:center.y};h.dir=Math.atan2(goal.y-h.y,goal.x-h.x);if(h.life<=0||dist(h,goal)<80){h.hp=0;return}}
 else if(h.kind==='gunship'||h.kind==='scout'){const angle=h.phase*.23+(h.orbitOffset||0),radius=h.kind==='gunship'?330:410,goal={x:center.x+Math.cos(angle)*radius,y:center.y+Math.sin(angle)*radius};h.dir=Math.atan2(goal.y-h.y,goal.x-h.x)}
 h.x+=Math.cos(h.dir)*115*dt;h.y+=Math.sin(h.dir)*115*dt;h.spotX=h.x+Math.sin(h.phase*.8)*85;h.spotY=h.y+Math.cos(h.phase*.65)*85;
 if(this.time>(h.nextSound||0)&&dist(h,this.king)<1300){h.nextSound=this.time+2;this.sound(h.kind==='gunship'?'gunship':'heli',.55,h.x)}
 if(this.time<(h.nextSense||0)||!this.performance.think('air'))return;h.nextSense=this.time+.4;
 const canopy=this.world.getObjects(h.spotX,h.spotY,90).filter(o=>o.type==='tree'&&!o.dead).length,cover=clamp(canopy/5,0,1);
 let target=this.apeGrid.near(h.spotX,h.spotY,105).find(a=>a.hp>0&&this.lineVisible({id:h.id,x:h.spotX,y:h.spotY},a)===true);
 const settlement=this.settlements.find(s=>s.population>10&&!s.known&&dist({x:h.spotX,y:h.spotY},s)<Math.min(180,s.radius+60));
 if(settlement&&cover<.8){h.confirm=(h.confirm||0)+.4;if(h.confirm>4){settlement.known=true;this.notify('SETTLEMENT DISCOVERED — '+settlement.name+'. They know.','red');this.addIntel(settlement.x,settlement.y,20);h.confirm=0}}else h.confirm=0;
 if(!target||cover>=.8||h.life<15)return;
 this.addIntel(target.x,target.y,.2);this.lastContact=this.time;h.lastX=target.x;h.lastY=target.y;const density=this.forces.observedDensity?.(h,target);if(density>0&&!this.world.sites.get(h.siteId)?.radioDown)this.recordHordeContact(h,target,density);
 if(h.kind==='gunship'){
 if(target.id==='king')h.kingExposure=(h.kingExposure||0)+.4;else h.kingExposure=0;
 if(h.shootTimer<=0&&cover<.55){const cluster=this.forces.selectCluster?.(h,target,{radius:80,range:720,deconflict:'airstrike'});const point=target.id==='king'&&h.kingExposure>=1.6?{x:target.x,y:target.y}:cluster;if(point){this.forces.hazards.push({id:'airstrike-'+this.nextId++,type:'airstrike',x:point.x,y:point.y,start:this.time,fuse:2,life:2,radius:80,damage:85,owner:h.id});h.shootTimer=10;this.sound('gunship',.8,h.x)}}
 }
 if(h.armed&&h.shootTimer<=0&&this.time>=(h.burstRest||0)&&cover<.55){
 const variant=h.kind;h.kind=variant==='gunship'?'machine':'assault';h.accuracyMultiplier=1+cover*6;this.shoot(h,target);h.kind=variant;h.shootTimer=.45;h.burstCount=(h.burstCount||0)+1;if(h.burstCount>=4){h.burstCount=0;h.burstRest=this.time+7;h.orbitOffset=(h.orbitOffset||0)+.8}
 }
 }
 launchHelicopter(objective=null){
 if(this.forceWeight()+10>this.responseBudget||this.helis.filter(h=>h.hp>0).length>=(this.warIntensity>=4?4:2))return false;
 const report=[...this.world.intel.values()].filter(i=>typeof i==='object'&&i.heat>4&&this.time-i.lastSeen<90).sort((a,b)=>b.lastSeen-a.lastSeen)[0];
 const target=objective||report,base=[...this.world.sites.values()].filter(s=>!s.cleared&&s.tier>=3&&!s.fuelDown&&this.world.militaryCapacity?.(s)?.fuel!==false&&dist(s,target||this.king)<(this.warIntensity>=4?4200:3200)&&((this.world.militaryCapacity?.(s)?.vehicleInventory||s.vehicleInventory||{}).heli||0)>0).sort((a,b)=>dist(a,target||this.king)-dist(b,target||this.king))[0];
 if(!base)return false;const destination=target&&!base.radioDown?target:(base.approach?.[base.approach.length-1]||{x:base.x+800,y:base.y}),kind=(this.tier===5||this.warIntensity>=4)&&target&&(this.operationSerial||0)%3===2?'gunship':(this.tier>=4||this.warIntensity>=2)&&target?'scout':'recon';
 this.helis.push({id:'heli-'+this.nextId++,x:base.x,y:base.y,dir:Math.atan2(destination.y-base.y,destination.x-base.x),phase:0,hp:kind==='gunship'?360:250,spotX:base.x,spotY:base.y,maxHp:kind==='gunship'?360:250,kind,armed:kind!=='recon',shootTimer:1,life:110,siteId:base.id,target:{x:destination.x,y:destination.y}});base.vehicleInventory.heli--;
 this.notify('AIR SEARCH — Rotor blades rise from '+(base.name||'a military base')+'. Keep to cover.','red');this.sound('heli',.65,base.x);return true;
 }
 refreshSettlements(){
 this.syncIndexes();this.humanGrid.rebuild(this.humans);
 for(const s of this.settlements){const old=s.population,list=this.settlementMembers.get(s.id)||[];s.population=list.length;s.scouts=0;s.children=0;for(const a of list){if(a.state==='scout')s.scouts++;else if(a.state==='young')s.children++}s.foragers=Math.max(0,Math.floor((s.population-s.scouts-s.children)*.4));s.radius=this.colonies.footprint?.(s)??85+Math.sqrt(s.population)*15;if(old>0&&s.population===0)this.notify(s.name+' is empty. Its shelters stand silent.','red')}
 }
 tickColonies(){let ticks=0;for(const s of this.settlements){if(s._economyAt===undefined)s._economyAt=this.time+1+(ATSUtil.hash(s.id)%997)/997;if(this.time<s._economyAt||ticks>=2)continue;s._economyAt+=1;if(s._economyAt<this.time-1)s._economyAt=this.time+1;this.colonies.tick(s);ticks++}}
 tickSecond(simulating=false){
 for(const o of this.world.getObjects(this.king.x,this.king.y,350))if(o.type==='cage'&&o.rescueOpened&&o.count>0&&this.population<MAX_APE_POPULATION){this.releaseCaptives(o);this.checkSite(this.world.sites.get(o.siteId))}
 this.world.reveal(this.king.x,this.king.y,this.viewRadius*.72);this.refreshSettlements();this.viewRadius=700+Math.min(200,this.followers.length*2);if(this.king.moving&&dist(this.king,this.trail[this.trail.length-1])>35){this.trail.push({x:this.king.x,y:this.king.y});if(this.trail.length>45)this.trail.shift()}
 if(!simulating)for(const s of this.settlements)this.colonies.tick(s);
 // Berries are gathered automatically. Travelling food feeds horde and can stock a new camp.
 for(const o of this.world.getObjects(this.king.x,this.king.y,85)){if(o.type==='berry'&&!o.dead&&(o.food??12)>0){const take=Math.min(o.food??12,4);o.food=(o.food??12)-take;this.food+=take;this.sound('food',.3);this.effect('text',o.x,o.y,{text:'+'+take+' food',color:'#a6d29a',life:1});if(o.food<=0){o.dead=true;o.solid=false}}}
 let consume=this.followers.length*.011;this.food=Math.max(0,this.food-consume);
 const n=this.followers.length;if(n>this.stats.largestHorde)this.stats.largestHorde=n;this.stats.territory=this.world.discovered.size;
 for(const s of this.world.sites.values())if(!s.cleared)this.world.replenishInstallation?.(s,this.time);
 this.updateResponseStage();this.responseDirector();this.tickRoadblocks();
 for(const s of this.world.sites.values()){if(s.alarm&&s.alarmUntil<this.time)s.alarm=false;if(s.spawned&&!s.cleared&&s.strength>0&&this.time>(s.nextOperation||Infinity)){s.nextOperation=this.time+65+Math.random()*40;let intel=this.world.intel.get(Math.floor(s.x/768)+','+Math.floor(s.y/768));if(intel&&typeof intel==='object'&&intel.heat>5&&this.time-intel.lastSeen<90)this.spawnRaid(s,{x:intel.x??s.x,y:intel.y??s.y},false);this.checkSite(s)}}
 for(const [k,v]of this.world.intel){if(typeof v==='object')v.heat=Math.max(0,v.heat-.02)}
 this.heliTimer--;if(this.tier>=3&&this.heliTimer<=0&&this.helis.length<(this.warIntensity>=4?4:2)){this.heliTimer=(this.warIntensity>=3?55:110)+Math.random()*40;this.launchHelicopter()}
 this.apes=this.apes.filter(a=>a.hp>0);this.humans=this.humans.filter(h=>h.hp>0);this.vehicles=this.vehicles.filter(v=>v.hp>0);
 }
 end(){if(this.ended)return;this.ended=true;this.king.hp=0;this.sound('death');this.effect('wave',this.king.x,this.king.y,{color:'#d9be77',range:240,life:2.5});this.notify('The King has fallen.','red');this.hooks.death?.(this)}
 migrateBalance(){
 // Preserve wounds and family progress while bringing an existing run onto the new balance.
 for(const a of this.apes){const hp=a.state==='young'?YOUNG_HP:a.maxHp===65||a.state==='scout'?SCOUT_HP:APE_HP;a.hp=hp*clamp(a.hp/(a.maxHp||48),0,1);a.maxHp=hp;if(a.state==='young')a.age*=GROW_UP/90}
 for(const s of this.settlements){const interval=Math.max(35,110/(1+s.population/22));s.birthTimer=Math.min(29,(s.birthTimer||0)/interval*30)}
 // Captives, supplies and damaged structures are finite saved campaign state.
 // Pacing changes apply to newly generated installations only.
 }
 savedActor(a){const out={};for(const k in a)if(!k.startsWith('_')&&k!=='navCohort')out[k]=a[k];return out}
 serialize(){return {version:1,siege:this.siege?.serialize(),speciesBalanceVersion:1,balanceVersion:2,militaryVersion:3,populationVersion:2,pursuitOperation:this.pursuitOperation,nextReinforcementAt:this.nextReinforcementAt,nextFieldOperation:this.nextFieldOperation,nextRegionalOperation:this.nextRegionalOperation,nextMajorOffensive:this.nextMajorOffensive,nextConvoyAt:this.nextConvoyAt,nextDirectorAt:this.nextDirectorAt,nextInterceptAt:this.nextInterceptAt,nextReconAt:this.nextReconAt,responseStage:this.responseStage,responsePeak:this.responsePeak,mobilized:this.mobilized,warPhase:this.warPhase,announcedWarIntensity:this.announcedWarIntensity,operationSerial:this.operationSerial,observedHorde:this.observedHorde,roadblockOperations:this.roadblockOperations,militaryOperations:this.militaryOperations,
 squads:Array.from(this.forces.squads?.values?.()||[],s=>({id:s.id,initialSize:s.initialSize,density:s.density,fallbackPoint:s.fallbackPoint,fallbackFacing:s.fallbackFacing,fallbackUntil:s.fallbackUntil,fallbackComplete:s.fallbackComplete,regroupPoint:s.regroupPoint,regroupFacing:s.regroupFacing,counterattackUntil:s.counterattackUntil,kingIdentifiedUntil:s.kingIdentifiedUntil,firingLine:s.firingLine,firingFacing:s.firingFacing,members:s.members,objective:s.objective,order:s.order,vehicleId:s.vehicleId,siteId:s.siteId,operationId:s.operationId,leaderId:s.leaderId,reportAt:s.reportAt,cohesionLossUntil:s.cohesionLossUntil,nextReport:s.nextReport,platoonId:s.platoonId,platoonSlot:s.platoonSlot,assaultPhase:s.assaultPhase,assaultFacing:s.assaultFacing,fortificationIds:s.fortificationIds,lineBuildQueue:s.lineBuildQueue})),
 platoons:Array.from(this.forces.platoons?.values?.()||[],p=>this.savedActor(p)),engineerJobs:Array.from(this.forces.engineerJobs?.values?.()||[],j=>this.savedActor(j)),corpseVersion:1,corpses:this.corpses.map(c=>this.savedActor(c)),seed:this.seed,difficulty:this.difficulty,time:this.time,nextId:this.nextId,king:this.king,apes:this.apes.map(a=>this.savedActor(a)),humans:this.humans.map(a=>this.savedActor(a)),vehicles:this.vehicles.map(a=>this.savedActor(a)),helis:this.helis.map(a=>this.savedActor(a)),settlements:this.settlements.map(a=>this.savedActor(a)),food:this.food,tier:this.tier,stats:this.stats,world:this.world.serialize(),hazards:this.forces.hazards,heliTimer:this.heliTimer,events:this.events,messages:this.messages,ended:this.ended}}
 static fromJSON(d,hooks={}){
 if(!d||d.version!==1||!d.king||!Array.isArray(d.apes)||!d.world||!d.stats||d.ended||d.king.hp<=0)throw new Error('This is not a living Apes Together Strong run.');
 if(d.apes.filter(a=>a.hp>0).length>MAX_APE_POPULATION)throw new Error('This save exceeds the '+MAX_APE_POPULATION+'-ape population limit.');
 const g=new Game(d.seed,d.difficulty,hooks);for(const k of ['time','nextId','king','apes','humans','vehicles','helis','settlements','food','tier','stats','heliTimer','events','messages','responseStage','responsePeak','mobilized','warPhase','announcedWarIntensity','operationSerial','observedHorde','roadblockOperations','militaryOperations','nextConvoyAt','nextDirectorAt','nextInterceptAt','nextReconAt','pursuitOperation','nextReinforcementAt','nextFieldOperation','nextRegionalOperation','nextMajorOffensive'])if(d[k]!==undefined)g[k]=d[k];g.world=ATSWorld.fromJSON(d.world);if(!d.balanceVersion||d.balanceVersion<2)g.migrateBalance();g.navigation=new ATSNavigation(g.world);g.forces.hazards=Array.isArray(d.hazards)?d.hazards:[];
 g.corpses=Array.isArray(d.corpses)?d.corpses.filter(c=>c&&c.life>0).slice(-320):[];
 g.ensureApeAppearance(g.king);for(const a of g.apes)g.ensureApeAppearance(a);for(const a of g.corpses)if(a.type==='ape'||a.id==='king'||a.id?.startsWith('ape-'))g.ensureApeAppearance(a);
 for(const a of [...g.apes,...g.humans,...g.vehicles,...g.helis,...g.corpses])for(const key in a)if(key.startsWith('_')||key==='navCohort')delete a[key];for(const site of g.world.sites.values())for(const key of ['sleepingHumans','sleepingVehicles'])for(const a of site[key]||[])for(const field in a)if(field.startsWith('_')||field==='navCohort')delete a[field];
 g.ended=false;g.world.ensure(g.king.x,g.king.y,1200);g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);g.syncIndexes();g.refreshSettlements();for(const s of g.settlements)g.colonies.init(s);for(const a of [g.king,...g.apes]){if(!a.siegeTransition&&!a.elevation)g.tactics?.restore(a);if(a.id!=='king')g.applyTraining(a,a.trainingLevel)}g.apeGrid.rebuild([g.king,...g.apes]);g.trail=[{x:g.king.x,y:g.king.y}];g.responseStage=d.responseStage||g.tier;g.responsePeak=d.responsePeak||g.stats.highestThreat||g.tier;g.mobilized=d.mobilized??(g.stats.largestHorde>=200);for(const v of g.vehicles)g.forces.initVehicle?.(v);g.forces.restoreSquads?.(d.squads);g.forces.restorePlatoons?.(d.platoons);
 g.militaryOperations=Array.isArray(g.militaryOperations)?g.militaryOperations:[];g.roadblockOperations=(Array.isArray(g.roadblockOperations)?g.roadblockOperations:[]).map(op=>{const saved=g.militaryOperations.find(x=>x.id&&x.id===op.id);if(saved)return saved;op.id=op.id||'roadblock-'+g.nextId++;op.channel='roadblock';op.status='active';op.target=op.target||op.point;op.phase=op.phase||(op.built?'hold':'approach');op.engineerJobs=op.engineerJobs||[];op.objects=op.objects||[];op.expiresAt=op.expiresAt||op.until;op.createdAt=op.createdAt??g.time;for(const id of op.members||[]){const h=g.humansById.get(id);if(h)h.operationId=op.id}g.militaryOperations.push(op);return op});
 for(const s of g.settlements)s.birthTimer=Math.min(30,Math.max(0,s.birthTimer||0));if((d.militaryVersion||0)<3){g.nextDirectorAt=Math.min(g.nextDirectorAt||0,g.time+g.directorInterval)}g.siege?.restore(d.siege);return g;
 }
}
window.ATSPerformance=PerformanceMonitor;window.ATSGame=Game;window.ATSTierNames=tierNames;window.ATSPrimateSpecies=PRIMATE_SPECIES;
})();
