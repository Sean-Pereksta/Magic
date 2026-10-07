(function(){
'use strict';
const TAU=Math.PI*2, clamp=(v,a,b)=>Math.max(a,Math.min(b,v)), dist=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const angleDiff=(a,b)=>Math.atan2(Math.sin(a-b),Math.cos(a-b));
const FOLLOW=new Set(['follow','charge','hold']), tierNames=['Hunters','Local security','Military response','Regional suppression','Extermination'];
const APE_HP=120,SCOUT_HP=150,YOUNG_HP=60,GROW_UP=35;
class Spatial{
 constructor(size=120){this.size=size;this.cells=new Map()}
 rebuild(items){this.cells.clear();for(const a of items){if(a.hp<=0)continue;const k=Math.floor(a.x/this.size)+','+Math.floor(a.y/this.size);if(!this.cells.has(k))this.cells.set(k,[]);this.cells.get(k).push(a)}}
 near(x,y,r){const out=[];for(let i=Math.floor((x-r)/this.size);i<=Math.floor((x+r)/this.size);i++)for(let j=Math.floor((y-r)/this.size);j<=Math.floor((y+r)/this.size);j++){const list=this.cells.get(i+','+j);if(list)for(const a of list)if((a.x-x)**2+(a.y-y)**2<=r*r)out.push(a)}return out}
}
class Game{
 constructor(seed,difficulty='survival',hooks={}){
 this.version=1;this.seed=String(seed||'LAUREL');this.difficulty=difficulty;this.hooks=hooks;this.world=new ATSWorld(this.seed);this.navigation=new ATSNavigation(this.world);this.colonies=new ATSSettlements(this);this.forces=new ATSForces(this);this.corpses=[];this.time=0;this.nextId=1;this.king={id:'king',x:0,y:0,hp:160,maxHp:160,dir:-.5,attackTimer:0,moving:false,lastHit:-30};
 this.apes=[];this.humans=[];this.vehicles=[];this.helis=[];this.settlements=[];this.bullets=[];this.effects=[];this.events=[];this.apeGrid=new Spatial();this.humanGrid=new Spatial();this.mode='follow';this.food=0;this.exposure=0;this.tier=1;this.viewRadius=700;this.commandCD=0;this.attackCD=0;this.ended=false;this.deathTimer=0;this.secondTimer=0;this.noises=[];this.trail=[{x:0,y:0}];this.raidTimer=45;this.heliTimer=150;this.seenTimer=0;this.lightCache=[];this.lightTimer=0;this.messages=[];this.hitFlash=0;this.lastContact=-20;this.sprintNoiseTime=0;this.day=1;this.aim={x:1,y:0};this.stats={freed:0,born:0,largestHorde:0,humans:0,structures:0,bases:0,prisons:0,settlements:0,largestSettlement:0,lost:0,highestThreat:1,territory:0};
 this.world.ensure(0,0,1400);this.world.reveal(0,0,300);this.notify('The crown is yours. A cage rattles nearby.','gold');
 }
 get followers(){return this.apes.filter(a=>a.hp>0&&FOLLOW.has(a.state))}
 get population(){return this.apes.filter(a=>a.hp>0).length}
 get score(){let s=this.stats;return Math.round(s.freed*250+s.born*300+s.largestHorde*90+s.largestSettlement*60+s.humans*40+s.structures*35+s.bases*1800+s.prisons*500+s.settlements*650+s.territory*15+this.time*2+this.settlements.filter(x=>x.population>0).length*400)}
 get title(){return this.score>90000?'The Unbroken Crown':this.score>45000?'Ape Warlord':this.score>22000?'Lord of the Forest':this.score>10000?'Tribal King':this.stats.freed>5?'Liberator':'Lonely Wanderer'}
 notify(text,color='gold'){this.messages.push({text,color,time:this.time});if(this.messages.length>25)this.messages.shift();this.hooks.toast?.(text,color)}
 sound(name,strength=1,x=this.king.x){this.hooks.sound?.(name,strength,clamp((x-this.king.x)/550,-1,1))}
 effect(type,x,y,opts={}){this.effects.push({type,x,y,life:opts.life||.65,maxLife:opts.life||.65,...opts});if(this.effects.length>200)this.effects.splice(0,20)}
 noise(x,y,radius,kind='roar'){
 this.noises.push({x,y,radius,life:2,kind});
 for(const h of this.humans){if(h.hp>0&&Math.hypot(x-h.x,y-h.y)<radius&&h.state!=='combat'&&h.state!=='radio'&&h.state!=='alarm'){h.state='investigate';h.lastX=x;h.lastY=y;h.searchTime=12;h.suspicion=Math.max(h.suspicion,.35)}}
 }
 command(cmd,aim=this.aim){
 if(this.ended||this.commandCD>0)return false;this.commandCD=.5;this.king.attackTimer=.55;let commandRadius=cmd==='settle'?105:cmd==='hold'?160:cmd==='call'?340:260;let nearby=this.apes.filter(a=>a.hp>0&&dist(a,this.king)<commandRadius),n=0;
 const wave=(color,range=300)=>this.effect('wave',this.king.x,this.king.y,{color,life:1.1,range});
 if(cmd==='call'){
 for(const a of nearby){if(a.state!=='young'){a.state='follow';a.settlementId=null;a.target=null;n++}}
 this.mode='follow';wave('#e5c374',340);this.noise(this.king.x,this.king.y,650);this.sound('call',1+this.followers.length/90);this.notify(n?'Your roar gathers '+n+' apes.':'Your roar echoes through the woods.');
 }else if(cmd==='charge'){
 let len=Math.hypot(aim.x,aim.y)||1;const dx=aim.x/len,dy=aim.y/len;for(const a of this.apes){if(a.hp>0&&FOLLOW.has(a.state)){a.state='charge';a.target={x:this.king.x+dx*570,y:this.king.y+dy*570};a.chargeTime=13;n++}}
 if(!n){this.notify('Free apes and Call them before charging.');return false}this.mode='charge';this.effect('charge',this.king.x,this.king.y,{dx,dy,life:1.2,color:'#e6b05b'});this.noise(this.king.x,this.king.y,900);this.sound('charge',1+n/60);this.notify(n+' apes charge into the dark.');
 }else if(cmd==='recall'){
 for(const a of this.apes){if(a.hp>0&&FOLLOW.has(a.state)){a.state='follow';a.target=null;a.retreatUntil=this.time+5;n++}}this.mode='follow';wave('#75cabb',440);this.noise(this.king.x,this.king.y,750);this.sound('recall');this.notify('Disengage. Return to your king.');
 }else if(cmd==='hold'){
 for(const a of nearby){if(FOLLOW.has(a.state)){a.state='hold';a.target={x:a.x,y:a.y};n++}}this.mode='hold';wave('#99bac3',260);this.sound('hold');this.notify(n?n+' apes hold this ground. Call to regroup.':'No followers close enough to hold.');
 }else if(cmd==='settle'||cmd==='settleAll'){
 if(this.world.terrain(this.king.x,this.king.y).water||this.world.getSites(this.king.x,this.king.y,240).some(s=>!s.cleared&&s.guards>0)){this.notify('Move into safer dry woodland before settling.','red');return false}
 let list=(cmd==='settleAll'?this.apes:nearby).filter(a=>a.hp>0&&FOLLOW.has(a.state));if(!list.length){this.notify('There are no followers here to settle.');return false}
 let s=this.settlements.find(s=>s.population>0&&dist(s,this.king)<240);if(!s){s={id:'settlement-'+this.nextId++,x:this.king.x,y:this.king.y,name:['Redwood','Moonroot','Laurel','Riverbend','Ashgrove','Highbranch'][this.settlements.length%6]+(this.settlements.length>=6?' '+(this.settlements.length+1):''),level:1,radius:90,food:18,age:0,known:false,attack:false,population:0,birthTimer:0,starveTimer:0,lastRaid:-120,nextWarn:0,foragers:0,scouts:0,children:0};this.settlements.push(s);this.stats.settlements++;this.notify(s.name+' has been founded.')}
 for(const a of list){a.state='settled';a.settlementId=s.id;a.homeX=s.x;a.homeY=s.y;n++}let transfer=Math.min(this.food,Math.max(12,n*2));this.food-=transfer;s.food+=transfer;wave('#93caa0');this.sound('settle');this.notify(n+' apes settle. Food transferred: '+Math.floor(transfer)+'.');this.refreshSettlements();this.colonies.init(s);
 }else if(cmd==='patrol'){
 for(const a of nearby){if(a.state==='settled'){a.state='scout';a.maxHp=SCOUT_HP;a.hp=Math.max(a.hp,SCOUT_HP);a.speed=104;n++;if(n>=Math.max(2,Math.floor(nearby.length/3)))break}}
 wave('#92c5ec');this.sound('patrol');this.notify(n?n+' scouts guard the settlement approaches.':'Visit settled apes to assign scouts.');this.refreshSettlements();
 }return true;
 }
 findOpen(x,y,r=10){
 if(!this.world.blocked(x,y,r))return{x,y};for(let radius=22;radius<=200;radius+=22)for(let i=0;i<12;i++){let aa=i/12*TAU,xx=x+Math.cos(aa)*radius,yy=y+Math.sin(aa)*radius;if(!this.world.blocked(xx,yy,r))return{x:xx,y:yy}}return{x,y};
 }
 makeApe(x,y,state='free',settlementId=null,young=false){
 ({x,y}=this.findOpen(x,y));
 const hp=young?YOUNG_HP:state==='scout'?SCOUT_HP:APE_HP;
 const p={id:'ape-'+this.nextId++,x,y,hp,maxHp:hp,dir:Math.random()*TAU,phase:Math.random()*TAU,fur:Math.random(),bodyScale:.88+Math.random()*.22,state:young?'young':state,settlementId,age:young?0:240,attackTimer:0,attackCD:Math.random()*.4,speed:young?67:84+Math.random()*18,moving:false,offsetX:(Math.random()-.5)*120,offsetY:(Math.random()-.5)*120,wander:Math.random()*TAU,nextThink:0};this.apes.push(p);return p;
 }
 makeHuman(x,y,site,kind=null){
 ({x,y}=this.findOpen(x,y));
 const t=Math.max(site?.tier||1,this.tier-1);kind=kind||(['pistol','rifle','assault','shotgun','machine'][clamp(t-1,0,4)]);if(t>=3&&Math.random()<.25)kind='shotgun';
 const h={id:'human-'+this.nextId++,x,y,hp:kind==='machine'?90:70,maxHp:kind==='machine'?90:70,dir:Math.random()*TAU,kind,state:'patrol',suspicion:0,shootTimer:1.5+Math.random(),radioTimer:0,siteId:site?.id||null,homeX:x,homeY:y,lastX:x,lastY:y,patrolX:x,patrolY:y,searchTime:0,phase:Math.random()*TAU,hasRadio:Math.random()<.35||(t>=3&&Math.random()<.5),perceptionTimer:Math.random()*.25,nextPatrol:0,moving:false,attackTimer:0,reported:false};this.forces.assign(h,site);this.humans.push(h);return h;
 }
 spawnSites(){
 if(this.time>=(this.nextStreamAt||0)){this.nextStreamAt=this.time+2;this.sleepDistantForces()}
 const sites=this.world.getSites(this.king.x,this.king.y,1050);
 for(const s of sites){
 while(s.sleepingHumans?.length&&this.humans.length<220)this.humans.push(s.sleepingHumans.pop());
 while(s.sleepingVehicles?.length&&this.vehicles.length<24)this.vehicles.push(s.sleepingVehicles.pop());
 if(s.spawned)continue;const count=s.guards??Math.max(0,s.tier*2);if(this.humans.length+count>220)continue;
 s.spawned=true;s.nextOperation=this.time+55+Math.random()*45;for(let i=0;i<count;i++){let a=i/count*TAU;this.makeHuman(s.x+Math.cos(a)*90,s.y+Math.sin(a)*90,s)}
 if(s.tier>=3&&this.vehicles.length<20){const p=this.findOpen(s.x+135,s.y+95,21);this.vehicles.push({id:'vehicle-'+this.nextId++,...p,dir:Math.PI,kind:s.tier>=4?'armored':'jeep',hp:s.tier>=4?400:230,maxHp:s.tier>=4?400:230,siteId:s.id,state:'idle',shootTimer:2,phase:0})}}
 }
 sleepDistantForces(){
 // Preserve each garrison rather than letting old, inactive guards consume the
 // entire population budget as the player explores new regions.
 const sleep=(a,key)=>{const s=this.world.sites.get(a.siteId);if(!s||a.hp<=0||a.raidTarget||a.state==='raid'||dist(a,this.king)<2300||this.settlements.some(st=>st.population>0&&dist(a,st)<850))return true;delete a._nav;a.aiming=null;(s[key]||(s[key]=[])).push(a);return false};
 this.humans=this.humans.filter(a=>sleep(a,'sleepingHumans'));
 this.vehicles=this.vehicles.filter(a=>sleep(a,'sleepingVehicles'));
 }
 move(a,dx,dy,speed,dt){
 const terrain=this.world.terrain(a.x,a.y),roleSpeed=a.role?(ATSHumanRoles[a.role]?.speed||1):1;
 this.navigation.move(a,dx,dy,speed*roleSpeed*(terrain.biome==='wetland'&&!terrain.road?.86:1),dt,a.id==='king');
 }
 spreadApes(dt){
 // Compute all pressures before moving anyone, so array order cannot bias a clump.
 // Local steps bypass route finding but retain wall, trunk and water clearance.
 const pressures=new Map();
 for(const a of this.apes){
 if(a.hp<=0)continue;
 const st=a.settlementId?this.settlements.find(s=>s.id===a.settlementId):null;
 if(st&&dist(a,this.king)>1800&&!st.attack)continue;
 pressures.set(a,{x:0,y:0});
 }
 this.apeGrid.rebuild([this.king,...pressures.keys()]);const shifts=[];
 for(const [a,push]of pressures){
 for(const p of this.apeGrid.near(a.x,a.y,38)){
 if(p.id<=a.id||p.hp<=0)continue;
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
 this.navigation.move(a,dx*40,dy*40,speed,dt,true);
 a.moving=a.moving||moving;if(a.attackTimer>0)a.dir=dir;
 }
 this.apeGrid.rebuild([this.king,...this.apes]);
 }
 animateAttack(a,target,kind){
 const variants=['hook','slam','backhand','tackle','overhead','uppercut'];a.attackSerial=(a.attackSerial||0)+1;a.animation={kind:kind||variants[(ATSUtil.hash(a.id)+a.attackSerial)%variants.length],start:this.time,duration:a.id==='king'?.42:.5};a.attackTimer=a.animation.duration;if(target)a.dir=Math.atan2(target.y-a.y,target.x-a.x);
 }
 objectDistance(a,o){const x=Math.max(o.x-(o.w||o.r*2)/2,Math.min(a.x,o.x+(o.w||o.r*2)/2)),y=Math.max(o.y-(o.h||o.r*2)/2,Math.min(a.y,o.y+(o.h||o.r*2)/2));return Math.hypot(a.x-x,a.y-y)}
 hurt(a,damage,source){
 if(a.hp<=0)return;if(a.id?.startsWith('human'))damage=this.forces.armor(a,damage,source);else if(a.settlementId)damage=this.colonies.absorb(a,damage);
 a.hitTimer=.28;a.aiming=null;a.lastHit=this.time;a.hp-=damage;
 if(a.hp<=0&&a.id!=='king'){const type=a.id?.startsWith('human')?'human':a.id?.startsWith('vehicle')?'vehicle':'ape';this.corpses.push({...a,_nav:undefined,type,fallVariant:ATSUtil.hash(a.id+this.time)%5,fallDir:source?Math.atan2(a.y-source.y,a.x-source.x):a.dir,age:0,life:9});if(this.corpses.length>100)this.corpses.shift();this.sound('fall',type==='human'?.45:.65,a.x)}this.effect('hit',a.x,a.y,{color:a.id==='king'?'#ef947a':'#bda17b',life:.25});
 if(a.id==='king'){a.lastHit=this.time;this.hitFlash=.6;this.sound('hit');if(a.hp<=0)this.end()}
 else if(a.id?.startsWith('human')){a.state='combat';a.lastX=source?.x??this.king.x;a.lastY=source?.y??this.king.y;a.searchTime=15;if(a.hp<=0){this.stats.humans++;if(a.siteId){let s=this.world.sites.get(a.siteId);if(s)s.strength=Math.max(0,s.strength-1)}this.addIntel(a.x,a.y,1.5);this.effect('smoke',a.x,a.y,{life:1.4,color:'#738087'})}}
 else if(a.id?.startsWith('ape')&&a.hp<=0){this.stats.lost++;this.effect('smoke',a.x,a.y,{life:1.5,color:'#746456'})}
 }
 attack(aim=this.aim){
 if(this.ended||this.attackCD>0)return;this.attackCD=.48;this.animateAttack(this.king);this.king.dir=Math.atan2(aim.y,aim.x);this.sound('attack');
 let candidates=this.humanGrid.near(this.king.x,this.king.y,75).filter(h=>this.world.lineClear(this.king.x,this.king.y,h.x,h.y)).concat(this.vehicles.filter(v=>v.hp>0&&dist(v,this.king)<85));let nearest=null;
 for(const h of candidates){if(!nearest||dist(h,this.king)<dist(nearest,this.king))nearest=h}
 let objs=this.world.getObjects(this.king.x,this.king.y,95).filter(o=>!o.dead&&o.hp>0&&!['tree','rock','berry'].includes(o.type));
 if(nearest){this.king.dir=Math.atan2(nearest.y-this.king.y,nearest.x-this.king.x);this.hurt(nearest,65,this.king);this.noise(this.king.x,this.king.y,200,'fight')}else{
 let obj=objs.sort((a,b)=>this.objectDistance(this.king,a)-this.objectDistance(this.king,b))[0];if(obj&&this.objectDistance(this.king,obj)<53)this.damageObject(obj,55,this.king);else this.effect('hit',this.king.x+Math.cos(this.king.dir)*38,this.king.y+Math.sin(this.king.dir)*38,{life:.25,color:'#e6ce96'});
 }
 }
 damageObject(o,damage,attacker){
 if(o.dead||!(o.hp>0))return;o.hp-=damage;this.effect('smash',o.x,o.y,{color:'#c6ac7e',life:.35});
 if(o.type!=='cage'||o.siteId!=='opening-rescue')this.noise(o.x,o.y,180,'smash');
 if(o.hp<=0){o.hp=0;o.dead=true;o.solid=false;this.world.navRevision++;this.stats.structures++;this.sound('smash',1,o.x);this.effect('smoke',o.x,o.y,{color:'#b29d78',life:1.4});
 let s=o.siteId?this.world.sites.get(o.siteId):null;
 if(o.type==='cage'){const captives=o.count||o.prisoners||3,num=Math.min(captives,600-this.population);for(let i=0;i<num;i++){const a=this.makeApe(o.x+(Math.random()-.5)*55,o.y+(Math.random()-.5)*55);if(this.world.blocked(a.x,a.y,10)){a.x=o.x;a.y=o.y}}this.stats.freed+=num;this.stats.prisons++;this.sound('rescue');this.notify(num+' apes join the free tribe. Roar [Q] to gather them.','green');this.addIntel(o.x,o.y,4);if(s){s.count=Math.max(0,s.count-captives);s.rescued=s.count===0}}
 else if(o.type==='radio'){this.notify('Radio tower down. Regional reinforcements cut.','green');if(s)s.radioDown=true}
 else if(o.type==='alarm'){if(s){s.alarm=false;s.alarmDown=true}this.notify('The alarm station is silent.','green')}
 else if(o.type==='tower'){this.notify('Searchlight destroyed. Darkness returns.','green')}
 else if(o.type==='barracks'){if(s){s.strength=Math.floor(s.strength*.35);s.barracksDown=true}this.notify('Barracks destroyed. Local operations collapse.','green')}
 else if(o.type==='depot'){if(s)s.depotDown=true;this.notify('Vehicle depot destroyed. Mobile raids reduced.','green')}
 else if(o.type==='fuel'){if(s)s.fuelDown=true;for(const h of this.humanGrid.near(o.x,o.y,120))this.hurt(h,80,attacker);this.effect('wave',o.x,o.y,{color:'#e6a465',life:.65,range:120})}
 else if(o.type==='house'){this.food+=35;this.notify('Human supplies seized: +35 food.','green')}
 this.checkSite(s);
 }
 }
 checkSite(s){if(!s||s.cleared)return;let structural=s.objects.map(id=>this.world.objects.get(id)).filter(Boolean);if(!structural.some(o=>o.type==='cage'&&!o.dead)&&!this.humans.some(h=>h.hp>0&&h.siteId===s.id)&&!(s.sleepingHumans||[]).some(h=>h.hp>0)&&(!structural.some(o=>o.type==='barracks'&&!o.dead)||s.strength<=0)){s.cleared=true;s.alarm=false;s.strength=0;this.stats.bases++;const supplies=s.tier>=2?s.tier*30:0;this.food+=supplies;this.notify(s.name+' liberated.'+(supplies?' +'+supplies+' food seized.':' The forest grows quiet.'),'green')}}
 addIntel(x,y,n){const k=Math.floor(x/768)+','+Math.floor(y/768);const old=this.world.intel.get(k)||{heat:0,lastSeen:0,x,y};if(typeof old==='number'){this.world.intel.set(k,{heat:old+n,lastSeen:this.time,x,y});return}old.heat=Math.min(100,(old.heat||0)+n);old.lastSeen=this.time;old.x=x;old.y=y;this.world.intel.set(k,old)}
 alert(h,kind='shout'){
 const radius=kind==='radio'?1300:kind==='alarm'?900:360;const s=this.world.sites.get(h.siteId);if(kind==='alarm'&&s){s.alarm=true;s.alarmUntil=this.time+45}h.reported=true;this.noise(h.lastX,h.lastY,radius,'alert');this.addIntel(h.lastX,h.lastY,kind==='shout'?3:8);this.lastContact=this.time;
 if(kind==='radio'){this.sound('radio');this.notify('Radio contact confirmed. Reinforcements are moving.','red');if(s&&!s.radioDown)this.spawnRaid(s,{x:h.lastX,y:h.lastY},false)}else if(kind==='alarm'){this.sound('alarm');this.notify('An installation alarm is ringing.','red')}else this.sound('alarm',.4,h.x);
 for(const other of this.humans){if(other.hp>0&&dist(other,h)<radius&&other!==h){other.lastX=h.lastX;other.lastY=h.lastY;other.state='search';other.searchTime=25}}
 for(const st of this.settlements){if(dist(st,h)<500&&!st.known&&st.population>0){st.known=true;this.notify(st.name+' discovered. They know.','red')}}
 }
 spawnRaid(site,target,settlementRaid=false){
 if(!site||site.cleared||site.strength<=0||this.humans.length>=220||this.time-(site.lastRaid||0)<30)return false;
 site.lastRaid=this.time;const number=Math.min(220-this.humans.length,site.strength,2+this.tier+(settlementRaid?2:0));site.strength-=number;
 const start={x:site.x,y:site.y};for(let i=0;i<number;i++){const h=this.makeHuman(start.x+(i%3)*24,start.y+Math.floor(i/3)*24,site);h.state='search';h.lastX=target.x;h.lastY=target.y;h.searchTime=55;h.raidTarget=settlementRaid?target.id:null;h.reported=true}
 if(this.tier>=3&&!site.depotDown&&!site.fuelDown&&this.vehicles.length<24){this.vehicles.push({id:'vehicle-'+this.nextId++,x:site.x+140,y:site.y+80,dir:0,kind:this.tier>=4?'armored':'jeep',hp:this.tier>=4?400:230,maxHp:this.tier>=4?400:230,siteId:site.id,state:'raid',target:{x:target.x,y:target.y,id:target.id},shootTimer:2,phase:0})}
 this.events.push({type:'raid',x:site.x,y:site.y,targetX:target.x,targetY:target.y,time:this.time});if(this.events.length>30)this.events.shift();return true;
 }
 getLights(){
 const lights=[];for(const h of this.humans){if(h.hp>0&&dist(h,this.king)<1050)lights.push({x:h.x,y:h.y,dir:h.dir,range:this.forces.range(h),angle:h.state==='combat'?.43:.34,kind:'flashlight',active:true})}
 for(const o of this.world.getObjects(this.king.x,this.king.y,1100)){if(o.dead)continue;if(o.type==='tower'){const s=this.world.sites.get(o.siteId);lights.push({x:o.x,y:o.y,dir:this.time*.29+(o.phase||0),range:480+(s?.tier||1)*25,angle:.21,kind:'tower',active:true})}else if(o.type==='alarm'&&this.world.sites.get(o.siteId)?.alarm)lights.push({x:o.x,y:o.y,dir:0,range:75,angle:Math.PI,kind:'alarm',active:true})}
 for(const v of this.vehicles){if(v.hp>0&&dist(v,this.king)<1000)lights.push({x:v.x,y:v.y,dir:v.dir,range:350,angle:.39,kind:'vehicle',active:true})}
 for(const f of this.forces.hazards)if(f.type==='flare')lights.push({x:f.x,y:f.y,dir:0,range:f.radius,angle:Math.PI,kind:'flare',active:true});
 for(const h of this.helis)if(h.hp>0)lights.push({x:h.spotX,y:h.spotY,dir:0,range:95,angle:Math.PI,kind:'heli',active:true});this.lightCache=lights;return lights;
 }
 lit(target,light){const d=dist(target,light);return d<light.range&&(light.angle>=3||Math.abs(angleDiff(Math.atan2(target.y-light.y,target.x-light.x),light.dir))<light.angle)&&this.world.lineClear(light.x,light.y,target.x,target.y)}
 update(dt,input={}){
 if(this.ended){this.deathTimer+=dt;this.king.deathAge=this.deathTimer;return}this.time+=dt;this.day=1+Math.floor(this.time/180);this.commandCD=Math.max(0,this.commandCD-dt);this.attackCD=Math.max(0,this.attackCD-dt);this.king.attackTimer=Math.max(0,this.king.attackTimer-dt);this.hitFlash=Math.max(0,this.hitFlash-dt);this.king.moving=false;
 this.navigation.beginFrame(this.time);this.world.ensure(this.king.x,this.king.y,1200);this.spawnSites();
 const mx=input.x||0,my=input.y||0,mag=Math.hypot(mx,my);if(mag>.04){this.move(this.king,mx*100,my*100,112*(input.sprint?1.24:1)*(this.king.hp<30?.75:1)*(input.sneak?.62:1),dt)}
 if(input.sprint&&mag>.04&&this.time>this.sprintNoiseTime){this.sprintNoiseTime=this.time+.9;this.noise(this.king.x,this.king.y,130+Math.min(220,this.followers.length*2),'footsteps')}
 if(input.aim&&Math.hypot(input.aim.x,input.aim.y)>.05)this.aim={...input.aim};if(input.attack)this.attack(this.aim);
 if(this.time-this.king.lastHit>8)this.king.hp=Math.min(this.king.maxHp,this.king.hp+2.4*dt);
 this.apeGrid.rebuild([this.king,...this.apes]);this.humanGrid.rebuild(this.humans);
 this.followSpread=Math.max(1,Math.sqrt(this.apes.filter(a=>a.hp>0&&a.state==='follow').length/32));
 for(const a of this.apes){if(a.hp>0)this.updateApe(a,dt)}
 this.spreadApes(dt);
 for(const h of this.humans){if(h.hp>0&&!this.forces.update(h,dt))this.updateHuman(h,dt)}
 for(const v of this.vehicles){if(v.hp>0)this.updateVehicle(v,dt)}
 for(const h of this.helis)this.updateHeli(h,dt);this.helis=this.helis.filter(h=>h.hp>0&&Math.abs(h.x-this.king.x)<3500&&Math.abs(h.y-this.king.y)<3500);
 this.updateBullets(dt);this.forces.tick(dt);for(const c of this.corpses){c.age+=dt;c.life-=dt}this.corpses=this.corpses.filter(c=>c.life>0);this.king.hitTimer=Math.max(0,(this.king.hitTimer||0)-dt);
 this.secondTimer+=dt;if(this.secondTimer>=1){this.secondTimer-=1;this.tickSecond()}
 this.lightTimer-=dt;if(this.lightTimer<=0){this.lightTimer=.12;const lights=this.getLights();const exposed=lights.some(l=>this.lit(this.king,l));this.exposure=clamp(this.exposure+(exposed?.34:-.2),0,1)}
 for(const fx of this.effects)fx.life-=dt;this.effects=this.effects.filter(fx=>fx.life>0);for(const n of this.noises)n.life-=dt;this.noises=this.noises.filter(n=>n.life>0);
 }
 updateApe(a,dt){
 a.hitTimer=Math.max(0,(a.hitTimer||0)-dt);a.attackTimer=Math.max(0,a.attackTimer-dt);a.attackCD=Math.max(0,a.attackCD-dt);a.moving=false;
 const st=a.settlementId?this.settlements.find(s=>s.id===a.settlementId):null;
 if(st&&dist(a,this.king)>1800&&!st.attack){a.age+=dt;if(a.state==='young'&&a.age>=GROW_UP){a.state='settled';a.hp=a.maxHp=APE_HP;a.speed=90}return}
 a.age+=dt;if(a.state==='young'){if(st?.attack){let h=this.humanGrid.near(a.x,a.y,160)[0];if(h){this.move(a,a.x-h.x,a.y-h.y,90,dt);return}}if(a.age>=GROW_UP){a.state='settled';a.hp=a.maxHp=APE_HP;a.speed=90;this.effect('text',a.x,a.y,{text:'Grown',color:'#b1d5a0',life:2})}else{if(st){let aa=this.time*.28+a.phase;this.move(a,st.x+Math.cos(aa)*35-a.x,st.y+Math.sin(aa)*35-a.y,a.speed,dt)}return}}
 let enemy=null;let range=a.state==='charge'?185:a.state==='scout'?165:FOLLOW.has(a.state)?90:st?.attack?190:70;
 if(a.state==='follow'&&a.retreatUntil>this.time)range=24;
 for(const h of this.humanGrid.near(a.x,a.y,range)){if(!enemy||dist(a,h)<dist(a,enemy))enemy=h}
 if(enemy&&(a.state!=='free'||dist(a,enemy)<30)){
 if(dist(a,enemy)>30||!this.world.lineClear(a.x,a.y,enemy.x,enemy.y))this.move(a,enemy.x-a.x,enemy.y-a.y,a.speed*(a.state==='charge'?1.35:1.12),dt);else if(a.attackCD<=0){a.attackCD=.7+Math.random()*.3;this.animateAttack(a,enemy);const swarm=this.apeGrid.near(enemy.x,enemy.y,48).length;this.hurt(enemy,18+Math.min(12,swarm*2),a)}return;
 }
 if(a.state==='charge'){
 a.chargeTime-=dt;let target=a.target||this.king;
 let objs=this.world.getObjects(a.x,a.y,75).filter(o=>!o.dead&&o.hp>0&&!['tree','rock','berry'].includes(o.type));let obj=objs.sort((x,y)=>dist(a,x)-dist(a,y))[0];
 let vehicle=this.vehicles.find(v=>v.hp>0&&dist(v,a)<80);
 if(vehicle){if(dist(a,vehicle)>40)this.move(a,vehicle.x-a.x,vehicle.y-a.y,a.speed*1.3,dt);else if(a.attackCD<=0){a.attackCD=.65;this.animateAttack(a,vehicle,'slam');this.hurt(vehicle,24,a);if(vehicle.hp<=0){this.stats.structures++;this.sound('smash',2,vehicle.x)}}return}
 if(obj&&this.objectDistance(a,obj)<26){if(a.attackCD<=0){a.attackCD=.75;this.animateAttack(a,obj,'overhead');this.damageObject(obj,18,a)}return}
 if(a.chargeTime<=0||dist(a,this.king)>1050||dist(a,target)<30){a.state='hold';a.target={x:a.x,y:a.y}}else this.move(a,target.x-a.x,target.y-a.y,a.speed*1.4,dt);
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
 let radius=a.state==='scout'?st.radius*1.9:Math.max(32,st.radius*.7),ang=this.time*(a.state==='scout'?.14:.08)+a.phase;let tx=st.x+Math.cos(ang)*radius,ty=st.y+Math.sin(ang)*radius;
 if(a.state==='settled'&&a.job==='forager'){let berry=this.world.getObjects(st.x,st.y,st.radius*2.5).find(o=>o.type==='berry'&&!o.dead&&(o.food??1)>0);if(berry){tx=this.time%12<6?berry.x:st.x;ty=this.time%12<6?berry.y:st.y;a.carrying=this.time%12>=6}}
 this.move(a,tx-a.x,ty-a.y,a.speed*.55,dt);
 if(a.state==='scout'&&this.time>=(a.nextWarn||0)){const threat=this.humanGrid.near(a.x,a.y,280).find(h=>h.state!=='patrol');if(threat){a.nextWarn=this.time+20;this.notify('Distant roars — '+st.name+' scouts sight a search party.','red');st.attack=true;this.sound('recall',.6,a.x)}}
 }else if(a.state==='free'){
 this.move(a,a.x+Math.sin(this.time*.5+a.phase)*20-a.x,a.y+Math.cos(this.time*.5+a.phase)*20-a.y,22,dt);
 }
 }
 updateHuman(h,dt){
 h.shootTimer-=dt;h.perceptionTimer-=dt;h.attackTimer=Math.max(0,(h.attackTimer||0)-dt);h.moving=false;
 if(dist(h,this.king)>1850&&!h.raidTarget){return}
 const site=this.world.sites.get(h.siteId);
 if(h.perceptionTimer<=0){h.perceptionTimer=.2+Math.random()*.1;const light={x:h.x,y:h.y,dir:h.dir,range:this.forces.range(h),angle:h.state==='combat'?.65:.34};let seen=null;
 for(const a of this.apeGrid.near(h.x,h.y,light.range)){if(a.hp<=0)continue;let d=dist(a,h);if(d<32||this.lit(a,light)||this.lightCache.some(l=>['tower','vehicle','heli','flare'].includes(l.kind)&&this.lit(a,l)&&this.world.lineClear(h.x,h.y,a.x,a.y))){if(!seen||d<dist(seen,h))seen=a}}
 if(seen){h.suspicion=clamp(h.suspicion+.32,0,1);h.lastX=seen.x;h.lastY=seen.y;h.searchTime=14;this.lastContact=this.time;if(h.suspicion>=1){if(!h.reported){this.alert(h,'shout');const alarm=site?.objects.map(id=>this.world.objects.get(id)).find(o=>o?.type==='alarm'&&!o.dead);if(alarm&&dist(h,alarm)<270){h.state='alarm';h.alarmTarget=alarm.id;h.radioTimer=0}else if(h.hasRadio&&!(site?.radioDown)){h.state='radio';h.radioTimer=2.3}else h.state='combat'}else if(h.state!=='radio'&&h.state!=='alarm')h.state='combat';h.targetId=seen.id}}
 else{h.suspicion=Math.max(0,h.suspicion-.06);if(h.state==='combat'){h.state='search';h.searchTime=18;h.targetId=null}}
 // A very large moving horde leaves audible evidence, without pinpoint vision.
 if(!seen&&this.followers.length>30&&this.king.moving&&dist(h,this.king)<Math.min(430,120+this.followers.length*2)&&h.state==='patrol'){h.state='investigate';h.lastX=this.king.x+(Math.random()-.5)*120;h.lastY=this.king.y+(Math.random()-.5)*120;h.searchTime=12}
 }
 if(h.state==='radio'){h.radioTimer-=dt;h.dir=Math.atan2(h.lastY-h.y,h.lastX-h.x);if(h.radioTimer<=0){this.alert(h,'radio');h.state='combat'}return}
 if(h.state==='alarm'){
 const alarm=this.world.objects.get(h.alarmTarget);if(!alarm||alarm.dead){h.state='combat';return}if(dist(h,alarm)>35)this.move(h,alarm.x-h.x,alarm.y-h.y,83,dt);else{h.radioTimer+=dt;if(h.radioTimer>.8){this.alert(h,'alarm');h.state='combat'}}return;
 }
 if(h.state==='combat'){
 let target=h.targetId==='king'?this.king:this.apes.find(a=>a.id===h.targetId&&a.hp>0);if(!target){h.state='search';return}
 const d=dist(h,target);h.dir=Math.atan2(target.y-h.y,target.x-h.x);
 if(d<38){this.move(h,h.x-target.x,h.y-target.y,62,dt)}else if(d>210){const flank=h.role==='flanker'?h.flankSide*90:0;this.move(h,target.x-h.x+Math.cos(h.dir+Math.PI/2)*flank,target.y-h.y+Math.sin(h.dir+Math.PI/2)*flank,60,dt);}
 if(h.shootTimer<=0&&d<330&&this.world.lineClear(h.x,h.y,target.x,target.y))this.shoot(h,target);
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
 const difficulty=this.difficulty==='wanderer'?.7:this.difficulty==='relentless'?1.15:1;
 const config={sniper:[2.8,300,1],pistol:[1.25,38,1],rifle:[.9,54,1],assault:[.32,35,1],shotgun:[1.8,22,4],machine:[.2,35,1]}[h.kind]||[1.2,38,1];h.shootTimer=config[0]/difficulty+Math.random()*.12;h.attackTimer=.14;h.animation={kind:'recoil',start:this.time,duration:.2};
 const d=dist(h,target),base=Math.atan2(target.y-h.y,target.x-h.x),error=(Math.random()-.5)*(h.kind==='sniper'?.025+d/6000:.07+d/3800);
 for(let i=0;i<config[2];i++){const angle=base+error+(i-(config[2]-1)/2)*.06;this.bullets.push({x:h.x+Math.cos(angle)*18,y:h.y+Math.sin(angle)*18,px:h.x,py:h.y,vx:Math.cos(angle)*550,vy:Math.sin(angle)*550,damage:config[1]*difficulty,life:h.kind==='sniper'?1.2:.7,owner:h.id})}
 this.sound('gun',.65,h.x);this.effect('muzzle',h.x+Math.cos(base)*23,h.y+Math.sin(base)*23,{life:.1,color:'#f8da9a'});this.noise(h.x,h.y,300,'gun');
 }
 updateBullets(dt){
 for(const b of this.bullets){b.life-=dt;if(b.life<=0)continue;const steps=3,sdt=dt/steps;for(let j=0;j<steps;j++){b.px=b.x;b.py=b.y;b.x+=b.vx*sdt;b.y+=b.vy*sdt;if(this.world.blocked(b.x,b.y,2)){b.life=0;this.effect('hit',b.x,b.y,{life:.16,color:'#c7bfa3'});break}let target=this.apeGrid.near(b.x,b.y,16).find(a=>a.hp>0);if(target){this.hurt(target,b.damage,{x:b.px,y:b.py});b.life=0;break}}
 }this.bullets=this.bullets.filter(b=>b.life>0);
 }
 updateVehicle(v,dt){
 v.shootTimer-=dt;v.phase+=dt;if(dist(v,this.king)>1900)return;
 if(v.state==='raid'&&v.target){if(dist(v,v.target)>85)this.move(v,v.target.x-v.x,v.target.y-v.y,v.kind==='armored'?70:95,dt);else v.state='combat'}
 const targets=this.apeGrid.near(v.x,v.y,290);let target=targets.sort((a,b)=>dist(a,v)-dist(b,v))[0];if(target&&this.world.lineClear(v.x,v.y,target.x,target.y)){v.dir=Math.atan2(target.y-v.y,target.x-v.x);if(v.shootTimer<=0){const oldKind=v.kind;v.kind=v.kind==='armored'?'machine':'rifle';this.shoot(v,target);v.kind=oldKind;v.shootTimer=v.kind==='armored'?.35:1.2}}
 }
 updateHeli(h,dt){
 h.phase+=dt;let target=this.settlements.find(s=>s.population>10&&!s.known&&dist({x:h.spotX,y:h.spotY},s)<Math.min(180,s.radius+60));if(target){h.confirm=(h.confirm||0)+dt;h.dir=Math.atan2(target.y-h.y,target.x-h.x);if(h.confirm>4){target.known=true;this.notify('SETTLEMENT DISCOVERED — '+target.name+'. They know.','red');this.addIntel(target.x,target.y,20);h.confirm=0}}
 h.x+=Math.cos(h.dir)*115*dt;h.y+=Math.sin(h.dir)*115*dt;h.spotX=h.x+Math.sin(h.phase*.8)*85;h.spotY=h.y+Math.cos(h.phase*.65)*85;
 if(this.time>(h.nextSound||0)&&dist(h,this.king)<1000){h.nextSound=this.time+2;this.sound('heli',.55,h.x)}
 if(dist({x:h.spotX,y:h.spotY},this.king)<95){this.addIntel(this.king.x,this.king.y,.03);this.lastContact=this.time;if(h.armed){h.shootTimer=(h.shootTimer||0)-dt;if(h.shootTimer<=0){h.kind='assault';this.shoot(h,this.king)}}}
 }
 refreshSettlements(){for(const s of this.settlements){const old=s.population;const list=this.apes.filter(a=>a.hp>0&&a.settlementId===s.id);s.population=list.length;s.scouts=list.filter(a=>a.state==='scout').length;s.children=list.filter(a=>a.state==='young').length;s.foragers=Math.max(0,Math.floor((s.population-s.scouts-s.children)*.4));s.radius=85+Math.sqrt(s.population)*15;if(old>0&&s.population===0)this.notify(s.name+' is empty. Its shelters stand silent.','red')}}
 tickSecond(){
 this.world.reveal(this.king.x,this.king.y,this.viewRadius*.72);this.refreshSettlements();this.viewRadius=700+Math.min(200,this.followers.length*2);if(this.king.moving&&dist(this.king,this.trail[this.trail.length-1])>35){this.trail.push({x:this.king.x,y:this.king.y});if(this.trail.length>45)this.trail.shift()}
 for(const s of this.settlements)this.colonies.tick(s);
 // Berries are gathered automatically. Travelling food feeds horde and can stock a new camp.
 for(const o of this.world.getObjects(this.king.x,this.king.y,85)){if(o.type==='berry'&&!o.dead&&(o.food??12)>0){const take=Math.min(o.food??12,4);o.food=(o.food??12)-take;this.food+=take;this.sound('food',.3);this.effect('text',o.x,o.y,{text:'+'+take+' food',color:'#a6d29a',life:1});if(o.food<=0){o.dead=true;o.solid=false}}}
 let consume=this.followers.length*.011;this.food=Math.max(0,this.food-consume);
 const n=this.followers.length;if(n>this.stats.largestHorde)this.stats.largestHorde=n;this.stats.territory=this.world.discovered.size;
 let threat=this.time/180+this.stats.freed/65+this.stats.bases*.5+this.stats.humans/45+this.population/170+this.settlements.filter(s=>s.known&&s.population).length*.45;this.tier=clamp(1+Math.floor(threat/2),1,5);this.stats.highestThreat=Math.max(this.tier,this.stats.highestThreat);
 for(const s of this.world.sites.values()){if(s.alarm&&s.alarmUntil<this.time)s.alarm=false;if(s.spawned&&!s.cleared&&s.strength>0&&this.time>(s.nextOperation||Infinity)){s.nextOperation=this.time+65+Math.random()*40;let intel=this.world.intel.get(Math.floor(s.x/768)+','+Math.floor(s.y/768));if(intel&&(intel.heat||intel)>5)this.spawnRaid(s,{x:intel.x||s.x,y:intel.y||s.y},false);this.checkSite(s)}}
 for(const [k,v]of this.world.intel){if(typeof v==='object')v.heat=Math.max(0,v.heat-.02)}
 this.heliTimer--;if(this.tier>=3&&this.heliTimer<=0&&this.helis.length<2){this.heliTimer=110+Math.random()*70;const aa=Math.random()*TAU;this.helis.push({id:'heli-'+this.nextId++,x:this.king.x+Math.cos(aa)*1200,y:this.king.y+Math.sin(aa)*1200,dir:aa+Math.PI,phase:0,hp:250,spotX:0,spotY:0,armed:this.tier>=5,shootTimer:1});this.notify('Rotor blades in the distance. Keep to cover.','red')}
 this.apes=this.apes.filter(a=>a.hp>0);this.humans=this.humans.filter(h=>h.hp>0);this.vehicles=this.vehicles.filter(v=>v.hp>0);
 }
 end(){if(this.ended)return;this.ended=true;this.king.hp=0;this.sound('death');this.effect('wave',this.king.x,this.king.y,{color:'#d9be77',range:240,life:2.5});this.notify('The King has fallen.','red');this.hooks.death?.(this)}
 migrateBalance(){
 // Preserve wounds and family progress while bringing an existing run onto the new balance.
 for(const a of this.apes){const hp=a.state==='young'?YOUNG_HP:a.maxHp===65||a.state==='scout'?SCOUT_HP:APE_HP;a.hp=hp*clamp(a.hp/(a.maxHp||48),0,1);a.maxHp=hp;if(a.state==='young')a.age*=GROW_UP/90}
 for(const s of this.settlements){const interval=Math.max(35,110/(1+s.population/22));s.birthTimer=Math.min(29,(s.birthTimer||0)/interval*30)}
 const multipliers={transport:1.75,hunter:2,research:2.5,checkpoint:3,prison:2.5,detention:2.5,experimental:2.5};
 for(const s of this.world.sites.values()){
 if(s.cleared||s.id==='opening-rescue')continue;let remaining=0;
 for(const id of s.objects){const o=this.world.objects.get(id);if(!o||o.dead)continue;if(o.type==='cage'){o.count=Math.ceil((o.count||o.prisoners||3)*(multipliers[s.type]||2.5));o.prisoners=o.count;remaining+=o.count}else if(o.type==='berry'&&o.supply){const total=120+s.tier*45;o.food=total*clamp((o.food||0)/(o.count||1),0,1);o.count=total}}
 s.count=remaining;
 }
 }
 serialize(){return {version:1,balanceVersion:2,seed:this.seed,difficulty:this.difficulty,time:this.time,nextId:this.nextId,king:this.king,apes:this.apes.map(({_nav,...a})=>a),humans:this.humans.map(({_nav,...a})=>a),vehicles:this.vehicles.map(({_nav,...a})=>a),helis:this.helis,settlements:this.settlements,food:this.food,tier:this.tier,stats:this.stats,world:this.world.serialize(),hazards:this.forces.hazards,heliTimer:this.heliTimer,events:this.events,messages:this.messages,ended:this.ended}}
 static fromJSON(d,hooks={}){
 if(!d||d.version!==1||!d.king||!Array.isArray(d.apes)||!d.world||!d.stats||d.ended||d.king.hp<=0)throw new Error('This is not a living Apes Together Strong run.');
 const g=new Game(d.seed,d.difficulty,hooks);for(const k of ['time','nextId','king','apes','humans','vehicles','helis','settlements','food','tier','stats','heliTimer','events','messages'])if(d[k]!==undefined)g[k]=d[k];g.world=ATSWorld.fromJSON(d.world);if(!d.balanceVersion||d.balanceVersion<2)g.migrateBalance();g.navigation=new ATSNavigation(g.world);g.forces.hazards=Array.isArray(d.hazards)?d.hazards:[];for(const a of [...g.apes,...g.humans,...g.vehicles])delete a._nav;g.ended=false;g.world.ensure(g.king.x,g.king.y,1200);g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);g.refreshSettlements();for(const s of g.settlements)g.colonies.init(s);g.trail=[{x:g.king.x,y:g.king.y}];return g;
 }
}
window.ATSGame=Game;window.ATSTierNames=tierNames;
})();
