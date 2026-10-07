/* Living settlements: persistent development, shared work and tactical defenses. */
(function(){
'use strict';
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v)),distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const hash=value=>{let n=0;for(const c of String(value))n=(Math.imul(n,31)+c.charCodeAt(0))>>>0;return n};
const LABELS={hut:'Shelters',garden:'Gardens',cooking:'Cooking fire',storage:'Food stores',workShelter:'Work shelter',lodge:'Great lodge',barrier:'Palisade',lookout:'Lookout platform',spearTower:'Spear tower',training:'Training ground',repairHut:'Repair huts',rebuildHut:'Rebuild hut',repairBarrier:'Repair defenses',lumber:'Clear woodland'};
const WORK={hut:18,garden:22,cooking:24,storage:35,workShelter:28,lodge:35,barrier:35,lookout:40,repairHut:16,rebuildHut:24,repairBarrier:18,lumber:6};
const TIMBER={hut:6,garden:6,cooking:6,storage:6,workShelter:8,lodge:10,barrier:8,lookout:12,repairHut:4,rebuildHut:6,repairBarrier:4,lumber:0};
const COMMISSIONS={
 spearTower:{kind:'spearTower',label:'Spear tower',description:'A staffed watchtower throws traveling spears at nearby human soldiers and vehicles.',wood:26,food:12,work:54,population:24},
 training:{kind:'training',label:'Training ground',description:'Adults practice in small groups, earning up to three permanent strength levels.',wood:24,food:20,work:48,population:30},
 defense:{kind:'barrier',label:'Palisade section',description:'Build a physical defensive section while leaving village entrances open.',wood:12,food:0,work:35,population:6},
 hut:{kind:'hut',label:'Family hut',description:'A completed shelter houses ten residents.',wood:8,food:0,work:18,population:1},
 garden:{kind:'garden',label:'Garden',description:'Grow a steady local food supply after the planting crew finishes.',wood:10,food:0,work:22,population:6},
 store:{kind:'storage',label:'Food store',description:'Increase the village food and timber storage capacity.',wood:16,food:6,work:35,population:12},
 workshop:{kind:'workShelter',label:'Workshop',description:'Give the village a sheltered place for its growing work crews.',wood:20,food:8,work:28,population:18}
};
class Settlements {
 constructor(game){this.game=game}
 evaluate(x,y){const w=this.game.world,t=w.terrain(x,y),objects=w.getObjects(x,y,390),trees=objects.filter(o=>o.type==='tree'&&!o.dead).length,berries=objects.filter(o=>o.type==='berry').length,region=w.districtAt(x,y);let water=false;for(let i=0;i<8;i++){const a=i*Math.PI/4;if(w.terrain(x+Math.cos(a)*210,y+Math.sin(a)*210).water)water=true}const risk=w.getSites(x,y,700).filter(s=>!s.cleared&&s.guards>0).length;return {fertility:region.fertility,wood:trees,berries,water,cover:clamp(trees/16,.1,1),capacity:Math.round(12+berries*5+(water?12:0)+region.fertility*10),risk,biome:t.biome,region:region.name}}
 init(s){
  if(s.economyVersion!==2){s.economyVersion=2;s.policy=s.policy||'balanced';s.wood=s.wood??8;s.housing=s.livingFounding?6:Math.max(6,Math.ceil((s.population||5)/6)*6);s.gardens=s.gardens||0;s.defense=s.defense||0;s.maxDefense=s.maxDefense||0;s.buildProgress=0;s.safety=s.safety??1;s.suitability=this.evaluate(s.x,s.y);s.foodDelta=0;s.project='Shelters';s.status='Establishing camp';s.supplyTimer=0;s.investedLevel=s.level||1}
  // Migrate actual housing once. Damaged homes and lost housing never return
  // merely because the population index or save is refreshed.
  if(s.structuresVersion!==1){s.structuresVersion=1;s.huts=[];let remaining=Math.max(0,(s.housing||6)-6);while(remaining>0){const capacity=Math.min(12,remaining);this.addHut(s,capacity,true);remaining-=capacity}}
  s.huts=s.huts||[];for(const h of s.huts){h.stage=h.stage??4;h.progress=h.progress??1;h.maxHp=h.maxHp||100;h.variant=h.variant??((h.slot||0)%4)}
  if(s.livingVersion!==1){s.livingVersion=1;s.developedRadius=clamp(Math.max(100,s.radius||100,this.builtExtent(s)),100,650);s.projects=s.projects||[];s.structures=s.structures||[];s.barriers=s.barriers||[];s.lookouts=s.lookouts||[];s.paths=s.paths||[];s.zones=s.zones||[];s.cohorts=s.cohorts||[];s.projectSerial=s.projectSerial||0;s.hutSerial=s.huts.length;s.cooking=s.cooking||0;s.stores=s.stores||0;s.clearedTrees=s.clearedTrees||0}
  s.projects=s.projects||[];s.structures=s.structures||[];s.barriers=s.barriers||[];s.lookouts=s.lookouts||[];s.paths=s.paths||[];s.zones=s.zones||[];s.cohorts=s.cohorts||[];
  s.facilities=Array.isArray(s.facilities)?s.facilities:[];s.commissions=Array.isArray(s.commissions)?s.commissions:[];s.spears=Array.isArray(s.spears)?s.spears.slice(-32):[];
  // Early drafts stored facilities with ordinary props. Preserve their IDs,
  // wounds and occupied plots when moving them to the durable collection.
  for(const f of s.structures.filter(o=>['spearTower','training'].includes(o.kind)))if(!s.facilities.some(o=>o.id===f.id))s.facilities.push(f);
  s.structures=s.structures.filter(o=>!['spearTower','training'].includes(o.kind));s.commissionVersion=1;
  for(const p of s.projects.filter(p=>p.commissioned&&!p.done))if(!s.commissions.some(c=>c.projectId===p.id))s.commissions.push({id:p.id,projectId:p.id,kind:p.commissionKind||p.kind,status:'queued',createdAt:p.createdAt,cost:{...(p.paidCost||{})}});
  for(const c of s.commissions)if(c.status==='queued'&&!s.projects.some(p=>p.id===c.projectId&&!p.done)){const built=s.huts.concat(s.structures,s.facilities,s.barriers).find(o=>o.id===c.structureId||o.id===c.projectId+'-built'||o.id===c.projectId+'-barrier');c.status=built?'complete':'unavailable';if(built)c.structureId=built.id}this.trimCommissions(s);
  s.age=s.age||0;s.food=s.food||0;s.wood=s.wood||0;s.level=s.level||1;s.safety=s.safety??1;s.starveTimer=s.starveTimer||0;s.birthTimer=s.birthTimer||0;s.nextWarn=s.nextWarn||0;s.lastRaid=s.lastRaid??-120;
  s.lodge={id:s.id+'-lodge',kind:'lodge',x:s.x,y:s.y,stage:4,hp:400,maxHp:400,...s.lodge,level:s.level};
  this.updateHousing(s);if(!s.zones.length)this.layout(s);
  // Saved settlements and world objects are separate JSON records. Rebind
  // defenses so breaches persist and repair works on the collision object.
  if(s._barriersBound!==this.game.world){s.barriers=s.barriers.map(b=>this.game.world.objects?.get(b.worldId||b.id)||b);s._barriersBound=this.game.world}
  this.game.world.syncSettlementBuildings?.(s,this.game);
 }
 lodgeAt(king=this.game.king){if(!king||king.hp<=0)return null;return this.game.settlements.filter(s=>s.population>0&&s.lodge?.hp!==0&&distance(king,s)<=90).sort((a,b)=>distance(king,a)-distance(king,b))[0]||null}
 commissioningSlots(s){return{active:(s.projects||[]).filter(p=>p.commissioned&&!p.done).length,limit:clamp(2+Math.ceil((s.population||0)/100),3,12)}}
 facilityLimit(s,kind){const p=s.population||0;return kind==='spearTower'?Math.min(14,2+Math.floor(p/80)):kind==='training'?Math.min(6,1+Math.floor(p/180)):kind==='barrier'?Math.min(56,Math.max(3,Math.ceil((s.developedRadius||100)/14))):kind==='hut'?Math.ceil(Math.max(6,p)*1.2/10)+2:kind==='garden'?Math.ceil(p/8)+2:kind==='storage'?Math.ceil(p/80)+1:Math.ceil(p/100)+1}
 catalog(s){if(typeof s==='string')s=this.game.settlement(s);if(!s)return[];this.init(s);const slots=this.commissioningSlots(s),near=this.lodgeAt()===s;
  return Object.entries(COMMISSIONS).map(([kind,c])=>{const collection=c.kind==='hut'?s.huts:c.kind==='barrier'?s.barriers:['spearTower','training'].includes(c.kind)?s.facilities:s.structures,built=collection.filter(o=>(c.kind==='hut'||c.kind==='barrier'||o.kind===c.kind)&&o.hp>0&&(o.stage??4)===4).length,queued=s.projects.filter(p=>p.kind===c.kind&&!p.done).length,limit=this.facilityLimit(s,c.kind),unlocked=s.population>=c.population;
   const reason=!unlocked?'Needs '+c.population+' village residents.':!near?'Visit the great lodge to commission construction.':s.attack?'Construction orders wait until the village is safe.':built+queued>=limit?'All '+limit+' available plots of this kind are occupied.':slots.active>=slots.limit?'The commission queue is full.':s.wood<c.wood?'Not enough village timber.':s.food<c.food?'Not enough village food.':'';
   return{kind,label:c.label,description:c.description,cost:{wood:c.wood,food:c.food},totalWork:c.work,limit,built,queued,unlocked,available:!reason,reason};
  });
 }
 commission(id,kind){const s=this.game.settlement(id);if(!s)return{ok:false,reason:'This village no longer exists.'};const entry=this.catalog(s).find(c=>c.kind===kind);if(!entry)return{ok:false,reason:'Unknown construction commission.'};if(!entry.available)return{ok:false,reason:entry.reason};const spec=COMMISSIONS[kind],p=this.queue(s,spec.kind,null,{commissioned:true});if(!p)return{ok:false,reason:'No safe, open plot is ready. Try again as the village expands.'};
  s.wood-=spec.wood;s.food-=spec.food;p.commissioned=true;p.commissionKind=kind;p.paidCost={wood:spec.wood,food:spec.food};p.timber=0;p.totalWork=spec.work;p.waiting=true;s.commissions.push({id:p.id,projectId:p.id,kind,status:'queued',createdAt:this.game.time,cost:{...p.paidCost}});this.trimCommissions(s);s._jobsAt=0;return{ok:true,reason:'Commission added to the builders’ work queue.',projectId:p.id};
 }
 trimCommissions(s){const active=s.commissions.filter(c=>c.status==='queued'),history=s.commissions.filter(c=>c.status!=='queued').slice(-24);s.commissions=history.concat(active)}
 trainingBonus(a){const level=clamp(Math.floor(a.trainingLevel||0),0,3);return{maxHp:level*12,damageMultiplier:1+level*.08}}
 builtExtent(s,limit=650){let r=100;for(const h of (s.huts||[]).concat(s.facilities||[]))r=Math.max(r,Math.hypot(h.x-s.x,h.y-s.y)+38);return Math.min(limit,r)}
 targetFootprint(s){const points=[[0,100],[10,100],[50,180],[100,250],[200,340],[400,450],[600,550],[1000,650]],p=Math.max(0,s.population||0);let r=650;for(let i=1;i<points.length;i++)if(p<=points[i][0]){const [a,ra]=points[i-1],[b,rb]=points[i];r=ra+(rb-ra)*(p-a)/(b-a);break}return clamp(r+Math.max(0,(s.level||1)-1)*3+Math.min(16,(s.structures?.length||0)*.8),100,650)}
 footprint(s){this.init(s);return Math.max(s.developedRadius||100,this.builtExtent(s))}
 expand(s){s.targetRadius=Math.max(this.targetFootprint(s),this.builtExtent(s),s.developedRadius);const before=s.developedRadius;s.developedRadius=Math.min(s.targetRadius,s.developedRadius+.35+Math.min(2.45,Math.sqrt(s.builders||0)*.12));s.radius=this.footprint(s);if(Math.abs(s.radius-(s._layoutRadius||0))>=8||!s.entrances?.length)this.layout(s);s.expansionRing={inner:Math.max(65,before-18),outer:s.developedRadius};}
 plot(s,kind,slot=0,legacy=false){
  const w=this.game.world,structures=(s.huts||[]).concat(s.structures||[],s.facilities||[],s.projects||[],s.barriers||[]),radius=s.developedRadius||s.radius||100;
  let housingRing=0,remaining=slot,ringSlots=8;while(remaining>=ringSlots){remaining-=ringSlots;housingRing++;ringSlots=8+housingRing*4}
  const housing=kind==='hut',plotRadius=kind==='training'?40:kind==='spearTower'?30:housing?27:21,separation=kind==='training'?68:kind==='spearTower'?54:housing?54:44,outer=['barrier','lookout','spearTower'].includes(kind),baseAngle=slot*2.399963+.2+(hash(s.id)%71)/71,minimum=legacy?140:housing?Math.min(75,radius*.57)+housingRing*75:kind==='garden'?radius*.68:outer?radius*.9:55+slot%4*28;
  for(let i=0;i<48;i++){const angle=baseAngle+(i%12)*Math.PI/6,r=legacy?minimum+Math.floor((slot+i/12)/8)*70:minimum+Math.floor(i/12)*26+(slot%3)*12,x=s.x+Math.cos(angle)*r,y=s.y+Math.sin(angle)*r;
   if(!legacy&&r>radius-20&&!outer)continue;
   if(outer&&(s.entrances||[]).some(e=>Math.abs(Math.atan2(Math.sin(angle-e.angle),Math.cos(angle-e.angle)))<.2))continue;
   if(kind==='barrier'&&(s.paths||[]).some(path=>{const end=path.points.at(-1);return end&&distance(end,{x,y})<38}))continue;
   if(w.terrain(x,y).water||structures.some(h=>h.kind!=='lumber'&&Math.hypot(h.x-x,h.y-y)<separation))continue;
   const info=w.settlementPlot?.(x,y,plotRadius,{settlementId:s.id});
   if(info){if(!info.valid||!info.trees.length&&w.blocked?.(x,y,plotRadius))continue;return {x,y,treeIds:info.trees.filter(t=>!t.dead).map(t=>t.id)}}
   const objects=w.getObjects(x,y,plotRadius+10),trees=objects.filter(o=>o.type==='tree'&&!o.dead);
   if(objects.some(o=>o.type!=='tree'&&o.solid&&!o.dead&&o.hp!==0))continue;
   if(!trees.length&&w.blocked?.(x,y,24))continue;return {x,y,treeIds:trees.map(t=>t.id)};
  }return null;
 }
 addHut(s,capacity=10,preserveHousing=false){
  s.huts=s.huts||[];const slot=s.hutSerial??s.huts.length,plot=this.plot(s,'hut',slot,true);if(!plot&&!preserveHousing)return null;
  const angle=slot*2.399963+.2,p=plot||{x:s.x+Math.cos(angle)*(140+Math.floor(slot/8)*70),y:s.y+Math.sin(angle)*(140+Math.floor(slot/8)*70)};
  const hut={id:s.id+'-hut-'+slot,slot,x:p.x,y:p.y,maxHp:100,hp:100,capacity,stage:4,progress:1,variant:slot%4};s.hutSerial=slot+1;s.huts.push(hut);this.updateHousing(s);if(s.paths)this.connect(s,hut);return hut;
 }
 updateHousing(s){s.housing=6+(s.huts||[]).reduce((sum,h)=>sum+(h.hp>0&&(h.stage??4)===4?h.capacity||10:0),0)}
 layout(s){const r=s.radius||s.developedRadius||100;s._layoutRadius=r;s.rings={core:r*.2,residential:r*.55,work:r*.76,outer:r};
  const zone=(kind,angle,ratio)=>({id:s.id+'-zone-'+kind,kind,x:s.x+Math.cos(angle)*r*ratio,y:s.y+Math.sin(angle)*r*ratio});
  s.zones=[zone('communal',0,.18),zone('wood',2.5,.27),zone('food',.65,.28),zone('garden',1.15,.67),zone('residential',3.9,.48),zone('guard',Math.PI/2,.88),zone('cook',-.6,.25),zone('work',-2.2,.38)];
  for(const kind of ['wood','communal']){const z=s.zones.find(o=>o.kind===kind),id=s.id+'-activity-'+kind;let prop=s.structures.find(o=>o.id===id);if(!prop){prop={id,kind,stage:4,hp:100,maxHp:100,variant:0,activityZone:true};s.structures.push(prop)}prop.x=z.x;prop.y=z.y;this.connect(s,prop)}
  for(const o of s.structures)if(['garden','cooking','storage','workShelter'].includes(o.kind))s.zones.push({id:o.id+'-zone',kind:{cooking:'cook',storage:'food',workShelter:'work'}[o.kind]||o.kind,x:o.x,y:o.y});
  s.entrances=[0,Math.PI*.68,Math.PI*1.37].map(angle=>({angle,x:s.x+Math.cos(angle)*r*.94,y:s.y+Math.sin(angle)*r*.94}));
 }
 connect(s,object){s.paths=s.paths||[];const id=s.id+'-path-'+object.id,existing=s.paths.find(p=>p.id===id||distance(p.points.at(-1),object)<1),dx=object.x-s.x,dy=object.y-s.y,points=[{x:s.x,y:s.y},{x:s.x+dx*.45-dy*.06,y:s.y+dy*.45+dx*.06},{x:object.x,y:object.y}];if(existing){existing.id=id;existing.points=points;return}s.paths.push({id,width:object.kind==='barrier'?9:7,points})}
 members(s){const g=this.game;return (g.settlementMembers?.get(s.id)||g.apes||[]).filter(a=>a.hp>0&&a.settlementId===s.id)}
 assignJobs(s,members){const adults=members.filter(a=>a.state!=='young'&&a.state!=='scout'),n=adults.length;
  const ratios=s.policy==='forage'?{forager:.56,builder:.17,lumber:.08,gardener:.08,cook:.02,hauler:.04,caretaker:.02}:s.policy==='fortify'?{forager:.28,builder:.31,lumber:.12,gardener:.08,cook:.02,hauler:.05,caretaker:.02}:{forager:.4,builder:.24,lumber:.09,gardener:.09,cook:.03,hauler:.05,caretaker:.02};
  let cursor=0;const roles=[];for(const [job,ratio]of Object.entries(ratios)){let count=Math.floor(n*ratio);if(job==='forager'&&n)count=Math.max(1,count);if(job==='builder'&&n>=4)count=Math.max(1,count);if(job==='lumber'&&n>=6)count=Math.max(1,count);count=Math.min(count,n-cursor);for(let i=0;i<count;i++)roles.push(job);cursor+=count}while(roles.length<n)roles.push('guardian');
  const groups=new Map();adults.forEach((a,i)=>{a.job=roles[i];const list=groups.get(a.job)||[];list.push(a);groups.set(a.job,list)});for(const a of members)if(a.state==='scout')a.job='scout';else if(a.state==='young')a.job='young';
  s.cohorts=[];for(const [job,list]of groups)for(let i=0;i<list.length;i+=20){const cohort={id:s.id+'-'+job+'-'+Math.floor(i/20),job,count:Math.min(20,list.length-i),members:list.slice(i,i+20).map(a=>a.id)};s.cohorts.push(cohort);for(let j=i;j<Math.min(i+20,list.length);j++)list[j].workCohort=cohort.id}
  s.foragers=groups.get('forager')?.length||0;s.builders=groups.get('builder')?.length||0;s.lumberWorkers=groups.get('lumber')?.length||0;s.guards=groups.get('guardian')?.length||0;s.gardeners=groups.get('gardener')?.length||0;s.cooks=groups.get('cook')?.length||0;s.haulers=groups.get('hauler')?.length||0;s._adults=adults;s._members=members;s._jobsAt=this.game.time+ (s.simLOD===2?20:s.simLOD===1?8:4);
 }
 zone(s,kind){return s.zones.find(z=>z.kind===kind)||s.zones[0]||s}
 resources(s){const g=this.game;if(g.time<(s._resourcesAt||0))return;s._resourcesAt=g.time+(s.simLOD===2?30:s.simLOD===1?12:6);
  const objects=g.world.getObjects(s.x,s.y,Math.min(1500,Math.max(520,s.radius*2.4)));s._berries=objects.filter(o=>o.type==='berry');s._trees=objects.filter(o=>o.type==='tree'&&!o.dead&&distance(o,s)<Math.max(430,Math.min(900,s.radius*1.4)));s._foragePoint=s._berries.find(b=>!b.dead&&(b.food??b.count??1)>.5)||this.zone(s,'garden');
 }
 queue(s,kind,target,options={}){if(s.projects.some(p=>p.kind===kind&&(!target||p.structureId===target.id))&&!['hut','garden','barrier'].includes(kind)&&!options.commissioned)return null;
  let plot;if(target)plot={x:target.x,y:target.y,treeIds:[]};else if(kind==='lumber'){const reserved=new Set(s.projects.flatMap(p=>p.treeIds||[]));const tree=(s._trees||[]).find(t=>!t.dead&&!reserved.has(t.id)&&hash(t.id)%5!==0);if(!tree)return null;plot={x:tree.x,y:tree.y,treeIds:[tree.id]}}else if(kind==='lodge')plot={x:s.x,y:s.y,treeIds:[]};else plot=this.plot(s,kind,kind==='hut'?s.huts.length:(s.projectSerial||0));
  if(!plot){s.constructionBlocked=true;return null}const serial=s.projectSerial=(s.projectSerial||0)+1,p={id:s.id+'-project-'+serial,kind,...plot,stage:0,progress:0,work:0,totalWork:WORK[kind]||30,timber:TIMBER[kind]||0,clearProgress:0,createdAt:this.game.time,structureId:target?.id};
  if(kind==='hut'){const slot=s.hutSerial??s.huts.length;const h={id:s.id+'-hut-'+slot,slot,x:p.x,y:p.y,hp:0,maxHp:100,capacity:10,stage:0,progress:0,variant:slot%4};s.hutSerial=slot+1;s.huts.push(h);p.structureId=h.id}
  s.projects.push(p);this.connect(s,p);s.constructionBlocked=false;return p;
 }
 plan(s){const queued=kind=>s.projects.some(p=>p.kind===kind),pending=s.projects.filter(p=>p.kind==='hut').length,housingReserve=s.population<150?Math.max(8,Math.ceil(s.population*.25)):Math.max(10,Math.ceil(s.population*.08)),limit=clamp(Math.ceil(s.builders/15),1,6),ruined=s.huts.find(h=>h.hp<=0&&(h.stage??4)===4&&!s.projects.some(p=>p.structureId===h.id)),wounded=s.huts.find(h=>h.hp>0&&h.hp<h.maxHp&&!s.projects.some(p=>p.structureId===h.id));
  if(s.projects.filter(p=>p.kind!=='lumber').length<limit){
   if(ruined)this.queue(s,'rebuildHut',ruined);
   else if(s.housing+pending*10<s.population+housingReserve)this.queue(s,'hut');
   else if(wounded)this.queue(s,'repairHut',wounded);
   else if(s.level<Math.min(10,1+Math.floor(s.population/12)))this.queue(s,'lodge');
   else if(s.gardens<Math.ceil(s.population/10)&&(!queued('garden'))){this.queue(s,'garden')}
   else if(s.population>=24&&!s.cooking&&!queued('cooking'))this.queue(s,'cooking');
   else if(s.population>=50&&s.stores<Math.ceil(s.population/80)&&!queued('storage'))this.queue(s,'storage');
   else if(s.population>=40&&s.structures.filter(o=>o.kind==='workShelter'&&o.hp>0).length<Math.ceil(s.population/160)&&!queued('workShelter'))this.queue(s,'workShelter');
  }
  // A second cohort supplies communal facilities even while housing expands.
  if(s.population>=24&&s.builders>=5&&s.projects.length<limit+1&&!s.cooking&&!queued('cooking'))this.queue(s,'cooking');
  if(s.gardens<Math.ceil(s.population/10)&&s.builders>=8&&s.projects.length<limit+1&&!queued('garden'))this.queue(s,'garden');
  if((s.wood<24||s.targetRadius>s.developedRadius+4)&&s.lumberWorkers&&!queued('lumber'))this.queue(s,'lumber');
  if((s.level>=6&&s.population>=100)||(s.policy==='fortify'&&s.population>=18)){const target=s.level>=6?this.facilityLimit(s,'barrier'):3;
   const damaged=s.barriers.find(b=>b.hp<b.maxHp&&!queued('repairBarrier'));
   if(damaged&&s.projects.length<limit+2)this.queue(s,'repairBarrier',damaged);
   else if(s.barriers.length+s.projects.filter(p=>p.kind==='barrier').length<target&&!queued('barrier')&&s.projects.length<limit+2)this.queue(s,'barrier');
   if(s.level>=6&&!s.lookouts.length&&!queued('lookout')&&s.projects.length<limit+2)this.queue(s,'lookout');
  }
  s.project=LABELS[s.projects.find(p=>p.kind!=='lumber')?.kind]||'Food stores';s.buildProject=s.project;
 }
 clearWork(s,p,workers){const w=this.game.world,tree=w.objects?.get(p.treeIds?.[0])||(s._trees||[]).find(t=>t.id===p.treeIds?.[0]);if(!tree||tree.dead||tree.hp===0){p.treeIds.shift();p.clearProgress=0;return}
  p.xClear=tree.x;p.yClear=tree.y;let hands=workers;if(s.simLOD===0){const near=(s._members||[]).filter(a=>['lumber','builder'].includes(a.job)&&!this.game.blastActive?.(a)&&distance(a,tree)<52);hands=near.length;for(const a of near){a.activity='chopping';a.workTargetId=tree.id;a.workAnimationAt=this.game.time}}
  if(!hands)return;p.clearProgress+=Math.min(4,hands)*.8;if(p.clearProgress<5)return;
  const wood=w.clearTree?.(tree,{settlementId:s.id,time:this.game.time})??0;if(wood>0){s.wood+=wood;s.clearedTrees++;this.game.effect?.('treeFall',tree.x,tree.y,{life:1.2});}
  if(tree.dead||tree.hp===0){p.treeIds.shift();p.clearProgress=0}else p.clearProgress=5;
 }
 complete(s,p){const g=this.game,target=s.huts.find(h=>h.id===p.structureId);
  if(p.kind==='hut'){target.stage=4;target.progress=1;target.hp=target.maxHp;this.updateHousing(s);this.connect(s,target)}
  else if(p.kind==='rebuildHut'){target.hp=target.maxHp;target.stage=4;delete target.destroyedAt;this.updateHousing(s)}
  else if(p.kind==='repairHut')target.hp=Math.min(target.maxHp,target.hp+45);
  else if(p.kind==='repairBarrier'){const barrier=s.barriers.find(b=>b.id===p.structureId);if(barrier){if(g.world.repairFortification)g.world.repairFortification(barrier,90,g.time);else {barrier.hp=Math.min(barrier.maxHp,barrier.hp+90);barrier.dead=false;barrier.solid=true}}}
  else if(p.kind==='lodge'){s.level=Math.min(10,s.level+1);s.lodge.level=s.level;g.notify?.(s.name+' finishes lodge level '+s.level+'.','green')}
  else if(p.kind==='barrier'){
   const angle=Math.atan2(p.y-s.y,p.x-s.x),weak=s.barriers.length%5===3,width=clamp(62+Math.floor((s.population||0)/100)*7,62,132),o=g.world.addFortification?.({id:p.id+'-barrier',type:'apeBarricade',owner:'ape',team:'ape',settlementId:s.id,x:p.x,y:p.y,width,height:16,dir:angle+Math.PI/2,hp:weak?180:320,maxHp:weak?180:320,weak});
   if(o){o.worldId=o.id;s.barriers.push(o);s.maxDefense+=35;s.defense=Math.min(s.maxDefense,s.defense+35)}
  }else if(p.kind==='lookout'){const o={id:p.id+'-lookout',kind:'lookout',x:p.x,y:p.y,stage:4,hp:150,maxHp:150,warningRadius:500};s.lookouts.push(o);s.structures.push(o);this.connect(s,o)}
  else if(['spearTower','training'].includes(p.kind)){const o={id:p.id+'-built',kind:p.kind,x:p.x,y:p.y,stage:4,progress:1,hp:p.kind==='spearTower'?220:180,maxHp:p.kind==='spearTower'?220:180,variant:s.facilities.length%3};if(p.kind==='spearTower'){o.range=360;o.shotAt=g.time+1;o.station={x:o.x-36,y:o.y+22}}else{o.trainingResidents=[];o.trained=0}s.facilities.push(o);this.connect(s,o);s._staffAt=0;this._defenseRosterAt=0}
  else if(p.kind!=='lumber'){const o={id:p.id+'-built',kind:p.kind,x:p.x,y:p.y,stage:4,hp:100,maxHp:100,variant:s.structures.length%3};s.structures.push(o);if(p.kind==='garden')s.gardens++;else if(p.kind==='cooking')s.cooking++;else if(p.kind==='storage'){s.stores++;s.food+=16}this.connect(s,o);this.layout(s)}
  if(p.commissioned){const entry=s.commissions.find(c=>c.projectId===p.id);if(entry){entry.status='complete';entry.completedAt=g.time;entry.structureId=p.kind==='hut'?p.structureId:p.id+(p.kind==='barrier'?'-barrier':'-built')}this.trimCommissions(s)}
 }
 work(s){const g=this.game,builders=s.builders||0,sites=new Map(s.projects.map(p=>[p.id,p])),nearHands=new Map();
  const concurrency=clamp(Math.ceil(builders/12),1,6),ordered=s.projects.filter(p=>p.kind!=='lumber').sort((a,b)=>(['repairHut','repairBarrier'].includes(b.kind)?1:0)-(['repairHut','repairBarrier'].includes(a.kind)?1:0)||(b.commissioned?1:0)-(a.commissioned?1:0)||(a.createdAt-b.createdAt));const selected=new Set(ordered.slice(0,concurrency).map(p=>p.id));for(const p of ordered)p.waiting=!selected.has(p.id);
  // One shared arrival pass per economy tick, rather than one planner for
  // every builder. Carrying is a visual resource trip, without inventories.
  if(s.simLOD===0)for(const a of s._members||[]){if(a.job!=='builder'||g.blastActive?.(a))continue;const p=sites.get(a.workTargetId);if(!p||p.treeIds.length||distance(a,p)>48)continue;nearHands.set(p.id,(nearHands.get(p.id)||0)+1);if(a.carrying==='wood'){p.deliveries=(p.deliveries||0)+1;p.lastDeliveryAt=g.time;a.carrying=false}a.activity='building';a.workAnimationAt=g.time}
  let active=0;for(const p of s.projects){if(p.kind==='lumber'){if(!s.attack&&p.treeIds.length)this.clearWork(s,p,s.lumberWorkers);if(!p.treeIds.length)p.done=true;continue}
   if(p.waiting||g.time<=p.createdAt||s.attack&&!['repairBarrier','repairHut'].includes(p.kind))continue;
   const assigned=s.simLOD===0?(nearHands.get(p.id)||0):Math.min(20,Math.max(1,builders/Math.max(1,s.projects.filter(q=>q.kind!=='lumber'&&!['repairHut','rebuildHut'].includes(q.kind)).length)));p.cohortId=s.cohorts.find(c=>c.job==='builder')?.id;
   const repair=['repairHut','rebuildHut','repairBarrier'].includes(p.kind),reserved=repair?0:s.projects.filter(q=>!q.done&&['repairHut','rebuildHut','repairBarrier'].includes(q.kind)).reduce((sum,q)=>sum+q.timber,0);
   if(p.treeIds.length){const hut=s.huts.find(h=>h.id===p.structureId);if(hut&&p.kind==='hut'){hut.treeIds=p.treeIds;hut.clearProgress=p.clearProgress/5}this.clearWork(s,p,builders);continue}if(!assigned||!builders||!p.paidCost&&s.wood<p.timber+reserved)continue;
   active++;p.work+=Math.min(p.totalWork*.22,assigned*1.25)*(s.attack?.45:1);p.progress=clamp(p.work/p.totalWork,0,1);p.stage=Math.min(3,1+Math.floor(p.progress*3));const h=s.huts.find(h=>h.id===p.structureId);if(h&&p.kind==='hut'){h.stage=p.stage;h.progress=p.progress}
   if(p.work>=p.totalWork){if(!p.paidCost)s.wood-=p.timber;this.complete(s,p);p.stage=4;p.progress=1;p.completedAt=g.time;p.done=true}
  }s.projects=s.projects.filter(p=>!p.done);s.buildProgress=s.projects.find(p=>p.kind!=='lumber')?.work||0;s.activeConstruction=active;
 }
 activityTarget(a,s){
  if(s.livingVersion!==1)this.init(s);const g=this.game,index=hash(a.id),offset=(index%17-8)*2.2,job=a.state==='young'?'young':a.state==='scout'?'scout':a.job||'guardian';let target,activity,carrying=false,speed=.55;
  const tower=s.facilities.find(f=>f.id===a.towerId&&f.kind==='spearTower'&&f.hp>0),training=s.facilities.find(f=>f.id===a.trainingFacilityId&&f.kind==='training'&&f.hp>0);
  if(tower&&job!=='young'){target=tower.station||{x:tower.x-36,y:tower.y+22};activity='guarding spear tower';speed=.7}
  else if(training&&!s.attack&&job!=='young'){target={x:training.x+62,y:training.y+12};activity='training';speed=.65}
  else if(s.attack){if(job==='young'||job==='forager'||job==='cook'||job==='caretaker'){target=this.zone(s,'communal');activity=job==='young'?'sheltering':'returning home';speed=.9}
   else if(job==='builder'||job==='lumber'){target=s.barriers.find(b=>b.hp<b.maxHp)||this.zone(s,'wood');activity='repairing defenses';carrying='wood'}
   else if(job==='scout'){target={x:s.x+Math.cos(a.phase||index)*s.radius*1.12,y:s.y+Math.sin(a.phase||index)*s.radius*1.12};activity='watching approaches'}
   else {target=s.entrances?.[index%(s.entrances?.length||1)]||this.zone(s,'guard');activity='defending entrance';speed=.85}
  }else if(job==='young'){const phase=g.time*.2+(a.phase||0);target={x:s.x+Math.cos(phase)*Math.min(55,s.radius*.22),y:s.y+Math.sin(phase)*Math.min(55,s.radius*.22)};activity=g.time%24<15?'playing':'rest';speed=.7}
  else if(job==='scout'){const angle=g.time*.04+(a.phase||0);target={x:s.x+Math.cos(angle)*s.radius*1.35,y:s.y+Math.sin(angle)*s.radius*1.35};activity='scouting'}
  else if(job==='forager'){const home=this.zone(s,'food'),field=s._foragePoint||this.zone(s,'garden'),returning=(g.time+index%7)%16>=8;target=returning?home:field;activity=returning?'hauling food':'gathering';carrying=returning?'food':false}
  else if(job==='builder'||job==='lumber'){const candidates=s.projects.filter(p=>!p.waiting&&(job==='lumber'?p.treeIds.length:p.kind!=='lumber')),p=candidates[index%Math.max(1,candidates.length)];const fetching=(g.time+index%5)%12<4;
   if(p?.treeIds.length){const tree=g.world.objects?.get(p.treeIds[0])||(s._trees||[]).find(t=>t.id===p.treeIds[0]);target=tree||p;activity=distance(a,target)<48?'chopping':'clearing woodland';a.workTargetId=p.treeIds[0]}
   else if(p){target=fetching?this.zone(s,'wood'):p;activity=fetching?'collecting timber':distance(a,p)<38?'building':'carrying timber';carrying=activity==='carrying timber'?'wood':false;a.workTargetId=p.id}
   else {target=this.zone(s,'work');activity='rest'}
  }else if(job==='gardener'){target=this.zone(s,'garden');activity='gardening'}
  else if(job==='cook'){target=this.zone(s,'cook');activity='cooking'}
  else if(job==='caretaker'){target=this.zone(s,'residential');activity='tending young'}
  else if(job==='hauler'){const wood=index%2===0,returning=(g.time+index%6)%14>=7;target=this.zone(s,returning?(wood?'work':'cook'):(wood?'wood':'food'));activity='hauling';carrying=returning?(wood?'wood':'food'):false}
  else {const phase=(g.time+index%31)%36;if(phase<22){const entrance=s.entrances?.[index%(s.entrances?.length||1)]||this.zone(s,'guard'),angle=g.time*.035+index;target={x:entrance.x+Math.cos(angle)*18,y:entrance.y+Math.sin(angle)*18};activity='patrolling'}else {target=this.zone(s,index%3===0?'cook':index%3===1?'residential':'communal');activity=['groom','rest','social'][index%3];speed=.35}}
  target=target||s;let x=target.x+(tower?0:offset),y=target.y+(tower?0:Math.sin(index)*12);
  // Residents use the radial paths for long trips without individual planning.
  if(distance(a,target)>90&&job!=='scout'&&job!=='young'){const dot=(a.x-s.x)*(target.x-s.x)+(a.y-s.y)*(target.y-s.y);if(distance(a,s)>40&&dot<0){x=s.x+offset;y=s.y}}
  a.activity=activity;a.carrying=carrying;return {x,y,speed:(a.speed||90)*speed,activity,carrying,projectId:a.workTargetId};
 }
 staffFacilities(s){const g=this.game;if(g.time<(s._staffAt||0))return;s._staffAt=g.time+4;const adults=(s._adults||this.members(s).filter(a=>a.state!=='young'&&a.state!=='scout')).filter(a=>a.hp>0),byId=new Map(adults.map(a=>[a.id,a])),reserved=new Set(),candidates=adults.slice().sort((a,b)=>(b.job==='guardian')-(a.job==='guardian'));
  for(const a of adults){delete a.towerId;delete a.trainingFacilityId}
  for(const tower of s.facilities.filter(f=>f.kind==='spearTower'&&f.hp>0&&f.stage===4).slice(0,14)){let a=byId.get(tower.staffedBy);if(!a||reserved.has(a.id))a=candidates.find(a=>!reserved.has(a.id));tower.staffedBy=a?.id||null;if(a){a.towerId=tower.id;reserved.add(a.id)}tower.station=tower.station||{x:tower.x-36,y:tower.y+22};tower.range=360}
  for(const facility of s.facilities.filter(f=>f.kind==='training'&&f.hp>0&&f.stage===4).slice(0,6)){const keep=(facility.trainingResidents||[]).map(id=>byId.get(id)).filter(a=>a&&a.trainingLevel<3&&!reserved.has(a.id));for(const a of candidates){if(keep.length>=3)break;if(!reserved.has(a.id)&&!keep.includes(a)&&(a.trainingLevel||0)<3)keep.push(a)}facility.trainingResidents=keep.slice(0,3).map(a=>a.id);for(const a of keep.slice(0,3)){a.trainingFacilityId=facility.id;reserved.add(a.id)}}
 }
 train(s){if(s.attack)return;const g=this.game,byId=new Map((s._adults||[]).map(a=>[a.id,a]));let work=0;for(const facility of s.facilities.filter(f=>f.kind==='training'&&f.hp>0&&f.stage===4).slice(0,6))for(const id of (facility.trainingResidents||[]).slice(0,3)){const a=byId.get(id);if(!a||a.hp<=0||a.state==='young'||g.blastActive?.(a)||distance(a,facility)>80)continue;const level=clamp(Math.floor(a.trainingLevel||0),0,3);if(level>=3){a.trainingProgress=0;continue}const needed=45*(level+1),cost=3+level*2;a.trainingProgress=Math.min(needed,(a.trainingProgress||0)+1);a.activity='training';work++;if(a.trainingProgress>=needed&&s.food>=cost){s.food-=cost;a.trainingProgress=0;a.trainingLevel=level+1;g.applyTraining?.(a,a.trainingLevel);facility.trained=(facility.trained||0)+1}}
  s.trainingWorkers=work;
 }
 updateDefenses(dt){if(!(dt>0))return;const g=this.game,counters={projectiles:0,towerChecks:0,losTests:0,collisionChecks:0};this.defenseCounters=counters;const active=[];
  // All saved shafts advance before towers acquire new targets. A shot can
  // therefore never deliver damage on the frame on which it is launched.
  for(const s of g.settlements){s.spears=s.spears||[];for(const spear of s.spears){if(active.length>=128)break;spear.pendingDt=Math.min(.4,(spear.pendingDt||0)+dt);active.push({s,spear})}}
  this._spearCursor=(this._spearCursor||0)%Math.max(1,active.length);for(let i=0;i<Math.min(32,active.length);i++){const {s,spear}=active[this._spearCursor++%active.length];if(spear.life<=0)continue;counters.projectiles++;const step=Math.min(spear.pendingDt,.1);spear.pendingDt-=step;const dx=(spear.vx||0)*step,dy=(spear.vy||0)*step,x=spear.x+dx,y=spear.y+dy;spear.prevX=spear.x;spear.prevY=spear.y;spear.elapsed=(spear.elapsed||0)+step;spear.life-=step;
    let hit=null,best=Infinity;const radius=Math.hypot(dx,dy)*.5+30,candidates=(g.humanGrid?.near((spear.x+x)/2,(spear.y+y)/2,radius)||[]).concat(g.vehicleGrid?.near((spear.x+x)/2,(spear.y+y)/2,radius)||[]).slice(0,24);
    for(const a of candidates){if(a.hp<=0)continue;counters.collisionChecks++;const t=clamp(((a.x-spear.x)*dx+(a.y-spear.y)*dy)/(dx*dx+dy*dy||1),0,1),d=Math.hypot(a.x-spear.x-dx*t,a.y-spear.y-dy*t),touch=a.kind&&['tank','apc','ifv','truck','jeep'].includes(a.vehicleClass||a.kind)?24:12;if(d<touch&&t<best){best=t;hit=a}}
    const endX=hit?spear.x+dx*best:x,endY=hit?spear.y+dy*best:y;
    counters.losTests++;if(!g.world.lineClear(spear.x,spear.y,endX,endY))spear.life=0;
    spear.x=endX;spear.y=endY;if(hit&&spear.life>0){g.hurt(hit,spear.damage,{id:spear.id,x:spear.fromX,y:spear.fromY,team:'ape',kind:'spear',settlementId:s.id});g.effect?.('spearImpact',endX,endY,{life:.25});spear.life=0}
  }
  for(const s of g.settlements)s.spears=s.spears.filter(p=>p.life>0);let count=active.filter(p=>p.spear.life>0).length;
  if(g.time>=(this._defenseRosterAt||0)){this._defenseRosterAt=g.time+1;this._defenseRoster=g.settlements.flatMap(s=>(s.facilities||[]).filter(f=>f.kind==='spearTower'&&f.hp>0&&f.stage===4).slice(0,14).map(tower=>({s,tower})));this._defenseCursor=(this._defenseCursor||0)%Math.max(1,this._defenseRoster.length)}
  const roster=this._defenseRoster||[],actors=g.apesById||new Map((g.apes||[]).map(a=>[a.id,a]));for(let i=0;i<Math.min(16,roster.length);i++){const {s,tower}=roster[this._defenseCursor++%roster.length];counters.towerChecks++;if(tower.hp<=0||s.spears.length>=32||count>=128||g.time<(tower.shotAt||0)||g.time<(tower.acquireAt||0))continue;const a=actors.get(tower.staffedBy),station=tower.station||tower;if(!a||a.hp<=0||a.state==='young'||a.settlementId!==s.id||g.blastActive?.(a)||distance(a,station)>42)continue;
   tower.acquireAt=g.time+.3;const candidates=(g.humanGrid?.near(tower.x,tower.y,360)||[]).concat(g.vehicleGrid?.near(tower.x,tower.y,360)||[]).filter(h=>h.hp>0&&distance(h,tower)<=360).slice(0,24).sort((a,b)=>distance(a,tower)-distance(b,tower));let target=null;
   for(const candidate of candidates.slice(0,2)){if(counters.losTests>=32)break;const d=distance(candidate,tower)||1,point={x:tower.x+(candidate.x-tower.x)/d*32,y:tower.y+(candidate.y-tower.y)/d*32};counters.losTests++;if(g.world.lineClear(point.x,point.y,candidate.x,candidate.y)){target=candidate;break}}
   if(!target)continue;const range=distance(target,tower)||1,launch={x:tower.x+(target.x-tower.x)/range*32,y:tower.y+(target.y-tower.y)/range*32},length=distance(target,launch)||1,speed=360,serial=s.spearSerial=(s.spearSerial||0)+1,training=this.trainingBonus(a),damage=18*training.damageMultiplier;s.spears.push({id:s.id+'-spear-'+serial,towerId:tower.id,sourceId:tower.id,fromZ:90,targetZ:20,team:'ape',x:launch.x,y:launch.y,prevX:launch.x,prevY:launch.y,fromX:tower.x,fromY:tower.y,targetX:target.x,targetY:target.y,targetId:target.id,start:g.time,duration:length/speed,elapsed:0,life:Math.min(1.4,length/speed+.15),speed,vx:(target.x-launch.x)/length*speed,vy:(target.y-launch.y)/length*speed,damage});count++;tower.shotAt=g.time+2.4+(hash(tower.id)%7)*.05;tower.targetId=target.id;a.dir=Math.atan2(target.y-a.y,target.x-a.x);a.attackTimer=.25;
  }
 }
 damageHut(s,h,damage,source){if(h.hp<=0||!(damage>0))return false;h.hp=Math.max(0,h.hp-damage);h.lastHit=this.game.time;this.game.effect?.('hit',h.x,h.y,{life:.3,color:'#c8ac79'});if(h.hp===0){h.destroyedAt=this.game.time;this.updateHousing(s);this.game.sound?.('smash',.65,h.x);this.game.effect?.('smoke',h.x,h.y,{life:2});this.game.notify?.(s.name+' loses a hut — '+(h.capacity||10)+' housing destroyed.','red')}this.game.world.syncSettlementBuildings?.(s,this.game);return true}
 damageNearby(x,y,radius,damage,source){if(!(radius>0&&damage>0))return;for(const s of this.game.settlements){if(Math.hypot(s.x-x,s.y-y)>Math.max(s.radius||100,this.builtExtent(s,Infinity))+radius)continue;this.init(s);for(const h of s.huts){const d=Math.hypot(h.x-x,h.y-y);if(h.hp>0&&d<radius)this.damageHut(s,h,damage*(1-d/radius*.65),source)}for(const f of s.facilities){const d=Math.hypot(f.x-x,f.y-y);if(f.hp>0&&d<radius){f.hp=Math.max(0,f.hp-damage*(1-d/radius*.65));f.lastHit=this.game.time;if(!f.hp){f.destroyedAt=this.game.time;s._staffAt=0;this._defenseRosterAt=0}}}this.game.world.syncSettlementBuildings?.(s,this.game)}}
 siege(s,raiders){const g=this.game;let budget=64;for(const h of raiders){if(!budget||h.hp<=0||h.state==='patrol'||h.raidTarget&&h.raidTarget!==s.id||g.time<(h._hutAttackAt||0))continue;let target=null,distance=175;for(const hut of s.huts){const d=Math.hypot(h.x-hut.x,h.y-hut.y),face=Math.min(28,d*.5),x=hut.x+(h.x-hut.x)/(d||1)*face,y=hut.y+(h.y-hut.y)/(d||1)*face;if(hut.hp>0&&d<distance&&g.world.lineClear(h.x,h.y,x,y)){target=hut;distance=d}}if(!target)continue;budget--;h._hutAttackAt=g.time+1.8;h.structureTarget=target.id;h.dir=Math.atan2(target.y-h.y,target.x-h.x);h.attackTimer=.14;h.animation={kind:'recoil',start:g.time,duration:.2};g.effect?.('muzzle',h.x,h.y,{life:.12});g.effect?.('siegeShot',h.x,h.y,{targetX:target.x,targetY:target.y,life:.15});g.sound?.('gun',.3,h.x);this.damageHut(s,target,h.role==='heavy'?15:h.role==='engineer'?20:10,h)}}
 action(id,action){const g=this.game,s=g.settlement(id);if(!s)return false;this.init(s);if(['balanced','forage','fortify'].includes(action)){s.policy=action;s._jobsAt=0;g.notify(s.name+' prioritizes '+({balanced:'balanced growth',forage:'food security',fortify:'defense'}[action])+'.');return true}if(Math.hypot(g.king.x-s.x,g.king.y-s.y)>s.radius+110){g.notify('Visit this settlement to move supplies or recruit its apes.','red');return false}
  if(action==='supply'){const amount=Math.min(30,g.food);if(amount<1){g.notify('Gather berries or seize human supplies first.');return false}g.food-=amount;s.food+=amount;g.notify(Math.floor(amount)+' food delivered to '+s.name+'.','green');return true}
  if(action==='recruit'){for(const a of g.apes)if(a.settlementId===id&&a.state!=='young'&&a.hp>0){a.state='follow';a.settlementId=null}g.refreshSettlements();g.notify('The adults of '+s.name+' rally to your crown.');return true}return false;
 }
 tick(s){const g=this.game;this.init(s);s.radius=this.footprint(s);s.simLOD=s.attack?0:distance(g.king||s,s)>1800?2:distance(g.king||s,s)>950?1:0;
  const nearby=g.humanGrid.near(s.x,s.y,s.radius+120);s.attack=nearby.some(h=>h.hp>0&&h.state!=='patrol');this.siege(s,nearby);if(!s.population)return;s.age++;
  const members=this.members(s);if(g.time>=(s._jobsAt||0)||s._memberCount!==members.length){this.assignJobs(s,members);s._memberCount=members.length}else{s._members=members;s._adults=members.filter(a=>a.state!=='young'&&a.state!=='scout')}
  const adults=s._adults;s.safety=clamp(s.safety+(s.attack?-.045:.007),0,1);this.expand(s);this.resources(s);
  const before=s.food,nodes=s._berries||[];let available=0;for(const b of nodes){b.food=Math.min(b.count||30,(b.food||0)+.025*s.suitability.fertility);if(b.food>.5)b.dead=false;available+=b.food||0}
  const harvest=Math.min(available,s.foragers*.32*(s.attack?.25:1));let remaining=harvest;for(const b of nodes){const take=Math.min(remaining,b.food||0);b.food-=take;remaining-=take;if(b.food<=0)b.dead=true;if(remaining<=0)break}
  const gardenYield=s.gardens*.6*s.suitability.fertility*(s.suitability.water?1.15:1),foodEfficiency=s.cooking>0?.94:1;s.food=clamp(s.food+harvest+gardenYield-s.population*.06*foodEfficiency,0,Math.max(s.housing,s.population)*10+80+(s.stores||0)*120);s.foodDelta=s.food-before;
  this.plan(s);this.work(s);this.staffFacilities(s);this.train(s);s.wood=Math.min(s.wood,200+s.level*25+(s.stores||0)*80);s.radius=this.footprint(s);g.world.syncSettlementBuildings?.(s,g);
  const capacity=s.suitability.capacity+s.gardens*16+s.level*10,parents=adults.length+(s.scouts||0),space=Math.min(s.housing,capacity)-s.population,canRaise=parents>=4&&space>=1&&s.food>s.population*1.5&&s.safety>.7&&!s.attack,population=g.population,cap=window.MAX_APE_POPULATION||1000;
  if(population>=cap){s.birthTimer=Math.min(30,s.birthTimer);if(canRaise&&!s.capNotified){s.capNotified=true;g.notify('Population limit reached — '+cap.toLocaleString()+' apes. Families wait for space.')}}else{s.capNotified=false;if(canRaise)s.birthTimer+=parents/8}
  if(canRaise&&s.birthTimer>=30&&population<cap){const births=Math.min(Math.floor(s.birthTimer/30),Math.floor(space),cap-population,Math.floor((s.food-s.population*1.5)/6));for(let i=0;i<births;i++){const angle=(g.stats.born+i)*2.399;g.makeApe(s.x+Math.cos(angle)*25,s.y+Math.sin(angle)*25,'young',s.id,true)}if(births>0){s.birthTimer-=births*30;s.food-=births*6;g.stats.born+=births;g.sound?.('birth',.45,s.x);if(g.stats.born===births)g.notify('A new generation is born in '+s.name+'.','green')}}
  if(s.food<1){s.starveTimer++;if(s.starveTimer===15)g.notify(s.name+' needs food. Forage or deliver supplies.','red');if(s.starveTimer>40)for(const a of adults)g.hurt(a,.6,{x:s.x,y:s.y})}else{s.starveTimer=0;for(const a of adults)if(!s.attack)a.hp=Math.min(a.maxHp,a.hp+.16)}
  s.status=s.attack?'Under attack':s.constructionBlocked?'Need open ground':s.food<s.population?'Food shortage':s.population>=s.housing?'Crowded':s.safety<.5?'Recovering':s.foodDelta<0?'Drawing on stores':'Thriving';
  if(s.attack){const raiders=g.humanGrid.near(s.x,s.y,s.radius);s.food=Math.max(0,s.food-raiders.length*.18);s.defense=Math.max(0,s.defense-raiders.length*.15);if(g.time>s.nextWarn){s.nextWarn=g.time+25;g.notify('Distant roars — '+s.name+' is under attack!','red','villageAttack')}}
  if(s.lookouts.length&&g.time>=(s._lookoutAt||0)){s._lookoutAt=g.time+8;const threat=g.humanGrid.near(s.x,s.y,s.radius+500).find(h=>h.hp>0&&h.state!=='patrol');if(threat&&g.time>=(s._lookoutWarn||0)){s._lookoutWarn=g.time+30;const direction=Math.abs(threat.x-s.x)>Math.abs(threat.y-s.y)?threat.x>s.x?'east':'west':threat.y>s.y?'south':'north';g.notify('Scouts at '+s.name+' sight an approaching force from the '+direction+'.','red','villageAttack');s.warning={x:threat.x,y:threat.y,time:g.time,direction}}}
  if((g.campaignPopulation??g.population)<300&&s.known&&g.time-s.lastRaid>95&&Math.random()<.025){const base=[...g.world.sites.values()].filter(x=>!x.cleared&&x.strength>0&&Math.hypot(x.x-s.x,x.y-s.y)<3200).sort((a,b)=>Math.hypot(a.x-s.x,a.y-s.y)-Math.hypot(b.x-s.x,b.y-s.y))[0];if(base&&g.spawnRaid(base,s,true)){s.lastRaid=g.time;g.notify((s.scouts?'Scouts warn: a raid is approaching ':'Human forces march toward ')+s.name+'.','red','villageAttack')}}
  g.stats.largestSettlement=Math.max(g.stats.largestSettlement,s.population);
 }
 absorb(a,damage){const s=this.game.settlement(a.settlementId);if(!s||s.defense<=0||Math.hypot(a.x-s.x,a.y-s.y)>=s.radius)return damage;const absorbed=Math.min(s.defense,damage*.25);s.defense-=absorbed;return damage-absorbed}
}
window.ATSSettlements=Settlements;
})();
