/* Settlement strategy. Economy work is bounded by the existing colony scheduler;
   journeys reuse resident actors and the existing navigation/LOD system. */
(function(){
'use strict';
const clamp=(n,a,b)=>Math.max(a,Math.min(b,n)),distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const EXPEDITION_RANGES={local:0,extended:1600,frontier:2800};
const FOCUS={
 balanced:{label:'General settlement',description:'Follow the selected work priority.'},
 sanctuary:{label:'Sanctuary',description:'35% faster family growth, double recovery and two extra beds per completed hut.',growth:1.35,recovery:2,ratios:{forager:.4,builder:.2,lumber:.08,gardener:.09,cook:.04,hauler:.03,caretaker:.1}},
 warcamp:{label:'War camp',description:'60% faster training, a standing militia and stronger local defense.',training:1.6,recovery:1.35,defense:.86,ratios:{forager:.27,builder:.16,lumber:.08,gardener:.08,cook:.03,hauler:.03,caretaker:.02}},
 supply:{label:'Supply village',description:'35% more food, 50% more timber from felled trees and faster supply carriers.',food:1.35,ratios:{forager:.48,builder:.15,lumber:.13,gardener:.1,cook:.03,hauler:.07,caretaker:.02}},
 scout:{label:'Scout outpost',description:'Reveal nearby terrain and detect approaching troops and vehicles up to 1,200 paces away.',scout:1.8,ratios:{forager:.34,builder:.16,lumber:.08,gardener:.08,cook:.02,hauler:.04,caretaker:.02}},
 workshop:{label:'Forge / workshop',description:'30% faster construction, reinforced new palisades, and equipment crafted from local timber and food.',construction:1.3,ratios:{forager:.3,builder:.34,lumber:.16,gardener:.06,cook:.02,hauler:.04,caretaker:.02}}
};
class Kingdom{
 constructor(game){this.game=game;this.counters={missionMembers:0,warningCandidates:0,trainingCandidates:0}}
 ensure(s){
  if(s.kingdomVersion!==1){s.kingdomVersion=1;s.specialization=FOCUS[s.specialization]?s.specialization:'balanced';s.defensePosture=['balanced','fortified','mobile'].includes(s.defensePosture)?s.defensePosture:'balanced';s.kingdomEvents=[];s.kingdomMissions=[];s.gearKits=0;s.siegeMaterials=0;s.craftProgress=0;s.moraleUntil=0}
  if(!s.expeditionSettings)s.expeditionSettings={foodRange:'local',woodRange:'local',priority:'balanced'};
  for(const key of ['foodRange','woodRange'])if(!(s.expeditionSettings[key] in EXPEDITION_RANGES))s.expeditionSettings[key]='local';
  if(!['balanced','food','wood'].includes(s.expeditionSettings.priority))s.expeditionSettings.priority='balanced';
  return s;
 }
 configureExpeditions(id,settings){
  const s=this.game.settlement(id);if(!s)return{ok:false,reason:'Choose an existing settlement.'};this.ensure(s);
  for(const key of ['foodRange','woodRange'])if(settings[key]!==undefined&&!Object.hasOwn(EXPEDITION_RANGES,settings[key]))return{ok:false,reason:'Choose Local, Extended or Frontier.'};
  if(settings.priority!==undefined&&!['balanced','food','wood'].includes(settings.priority))return{ok:false,reason:'Choose a gathering priority.'};
  for(const key of ['foodRange','woodRange','priority'])if(settings[key]!==undefined)s.expeditionSettings[key]=settings[key];
  s.nextExpeditionAt=0;s.expeditionMessage=s.expeditionSettings.foodRange==='local'&&s.expeditionSettings.woodRange==='local'?'Local gathering; current parties will finish their trips':'Orders ready — parties depart when supplies are needed';return{ok:true};
 }
 expeditionWorkers(s){
  // Keep actual defenders, skilled production crews and deliveries at home.
  const all=this.game.settlementMembers.get(s.id)||[],eligible=[],guards=[];let adults=0,away=0;
  for(const a of all){if(a.hp<=0||a.state==='young')continue;adults++;if(a.kingdomMission){away++;continue}if(a.state!=='settled'||a.champion||a.divisionId||a.recoveryRemaining>0||a.expeditionCargo||a.towerId||a.trainingFacilityId||['gardener','workshop','cook','caretaker'].includes(a.job)||a.carrying)continue;if(a.job==='guardian'){guards.push(a);continue}if(a.job==='builder'&&(s.projects||[]).some(p=>!p.done))continue;eligible.push(a)}
  eligible.push(...guards.slice(Math.max(3,Math.ceil(adults*.2))));
  return eligible.slice(0,Math.max(0,Math.min(Math.floor(adults*.3)-away,adults-away-Math.max(6,Math.ceil(adults*.35)))));
 }
 expeditionStatus(s){
  this.ensure(s);const states=[];
  for(const m of s.kingdomMissions)if(m.kind==='expedition'&&m.status==='traveling')states.push({id:m.id,resource:m.resource,phase:m.phase,members:m.members.filter(id=>{const a=this.game.apesById.get(id);return a?.hp>0&&a.kingdomMission?.id===m.id}).length,cargo:m.cargo?.[m.resource]||0,reason:m.reason||''});
  return{...s.expeditionSettings,parties:states.length,availableWorkers:this.expeditionWorkers(s).length,states,message:states.length?'':s.expeditionMessage||'Local gathering'};
 }
 resourceReserved(id,point,except){
  for(const s of this.game.settlements){for(const m of s.kingdomMissions||[])if(m!==except&&m.kind==='expedition'&&m.status==='traveling'&&!m.returning&&m.target&&(m.target.resourceId===id||distance(m.target,point)<110))return true;
   for(const p of s.projects||[])if(!p.done&&p.treeIds?.includes(id))return true}
  return false;
 }
 resourceQuantity(o,resource){return !o||o.dead||o.hp===0?0:resource==='food'&&o.type==='berry'?Math.max(0,o.food??o.count??0):resource==='wood'&&o.type==='tree'&&!o.woodClaimed?Math.round(5+(o.size||1)*5):0}
 expeditionRouteCost(from,to){
  const g=this.game,nav=g.navigation,cell=nav.cell||28,key=[Math.round(from.x/cell),Math.round(from.y/cell),Math.round(to.x/cell),Math.round(to.y/cell),10,nav.revision,'ape'].join(':'),cached=nav.routes.get(key);
  if(cached&&!cached.path._navInvalid&&g.time-cached.time<(cached.path.length?8:1.1)){if(!cached.path.length)return Infinity;let length=0,last=from;for(const p of cached.path){length+=distance(last,p);last=p}return length+distance(last,to)}
  const crossing=g.world.crossingFor?.(from,to,'ape',null,g.time);if(crossing){const side=to.y>=from.y?0:1;return distance(from,crossing.approaches[side])+distance(crossing.approaches[0],crossing.approaches[1])+distance(crossing.approaches[1-side],to)}
  return distance(from,to);
 }
 discoverResource(s,resource,from=s,except=null){
  const g=this.game,range=EXPEDITION_RANGES[s.expeditionSettings[resource+'Range']];if(!range)return null;
  // Read the spatial resource index once per planning pass. Only eight promising
  // sites get threat and shared-route scoring; no per-worker or full-map A*.
  const reserved=new Set(),areas=[];for(const home of g.settlements){for(const m of home.kingdomMissions||[])if(m!==except&&m.kind==='expedition'&&m.status==='traveling'&&!m.returning&&m.target){reserved.add(m.target.resourceId);areas.push(m.target)}for(const p of home.projects||[])if(!p.done)for(const id of p.treeIds||[])reserved.add(id)}
  const strongholds=g.world.getSites(s.x,s.y,range).filter(site=>!site.cleared&&(site.guards>0||site.sleepingHumans?.length));
  const origin=Math.round(from.x/160)+','+Math.round(from.y/160),prior=s.expeditionSearchAfter?.[resource],after=prior?.origin===origin?prior:null,shortlist=[];
  for(const o of g.world.getObjects(s.x,s.y,range)){const quantity=this.resourceQuantity(o,resource);if(!quantity||distance(s,o)>range||s.expeditionRejected?.[o.id]>g.time||reserved.has(o.id)||areas.some(p=>distance(p,o)<110)||strongholds.some(site=>distance(site,o)<(site.radius||120)+180))continue;const score=distance(from,o)/(Math.min(quantity,48)+8),id=String(o.id);if(after&&(score<after.score||score===after.score&&id<=after.id))continue;const entry={o,score,id};let i=0;while(i<shortlist.length&&(shortlist[i].score<score||shortlist[i].score===score&&shortlist[i].id<=id))i++;if(i<8){shortlist.splice(i,0,entry);if(shortlist.length>8)shortlist.pop()}}
  let best=null,cost=Infinity;for(const {o}of shortlist){if(g.humanGrid.nearest(o.x,o.y,260,1,h=>h.hp>0).length||g.vehicleGrid?.nearest(o.x,o.y,330,1,v=>v.hp>0).length)continue;const length=this.expeditionRouteCost(from,o),score=length/(Math.min(this.resourceQuantity(o,resource),48)+8);if(score<cost){cost=score;best=o}}
  // Continue beyond a rejected shortlist next time, rather than repeatedly
  // reconsidering the same eight guarded or unreachable nodes forever.
  const cursors=s.expeditionSearchAfter||(s.expeditionSearchAfter={});if(!best){const last=shortlist.at(-1);if(last)cursors[resource]={origin,score:last.score,id:last.id};else delete cursors[resource];return null}delete cursors[resource];const d=distance(from,best)||1,r=resource==='wood'?Math.max(42,(best.moveRadius||best.r||16)+20):22;
  return{resourceId:best.id,x:best.x+(from.x-best.x)/d*r,y:best.y+(from.y-best.y)/d*r,resourceX:best.x,resourceY:best.y,routeDistance:this.expeditionRouteCost(from,best)};
 }
 scoutResources(s,resource){
  // Discover unexplored resources incrementally through the normal stream budget.
  const range=EXPEDITION_RANGES[s.expeditionSettings[resource+'Range']],cursor=s.expeditionSearchCursor||0,rings=Math.max(1,Math.ceil(range/600)),ring=cursor%rings+1,angle=Math.floor(cursor/rings)*2.399963,r=Math.min(range-100,ring*600),point={x:s.x+Math.cos(angle)*r,y:s.y+Math.sin(angle)*r};s.expeditionSearchCursor=cursor+1;this.game.world.requestCorridor?.(point,point,{id:'expedition-scout:'+s.id,radius:200});
 }
 planExpeditions(s){
  const g=this.game,settings=s.expeditionSettings;if(settings.foodRange==='local'&&settings.woodRange==='local')return;
  if(g.time<(s.nextExpeditionAt||0))return;s.nextExpeditionAt=g.time+8;
  if(s.attack){s.expeditionMessage='Threatened — workers remain home';return}
  const active=s.kingdomMissions.filter(m=>m.status==='traveling');if(active.length>=4)return;const workers=this.expeditionWorkers(s);if(workers.length<3){s.expeditionMessage='Keeping defenders and essential workers home';return}
  const needs={food:Math.max(0,Math.max(60,s.population*3)-s.food),wood:Math.max(0,Math.max(60,Math.min(180,s.population*2+(s.projects?.length||0)*6))-s.wood)},order=settings.priority==='food'?['food','wood']:settings.priority==='wood'?['wood','food']:needs.food/Math.max(60,s.population*3)>=needs.wood/Math.max(60,s.population*2)?['food','wood']:['wood','food'];
  for(const resource of order){if(!needs[resource]||settings[resource+'Range']==='local'||active.filter(m=>m.kind==='expedition'&&m.resource===resource).length>=Math.max(1,Math.floor(s.population/80)))continue;const target=this.discoverResource(s,resource);if(!target){this.scoutResources(s,resource);s.expeditionMessage='Unable to find safe '+(resource==='wood'?'lumber':'food')+' — scouting selected range';continue}
   const result=this.startMission(s,null,workers.slice(0,Math.min(8,Math.max(3,Math.floor(workers.length/2)))),'expedition');if(!result.ok)return;
   const m=result.mission;Object.assign(m,{resource,target,phase:'traveling',lastTick:g.time,progressAt:g.time,travelUntil:g.time+clamp(target.routeDistance/70*4+30,90,240),progressPoint:{x:workers[0].x,y:workers[0].y},cargo:{},gatherWork:0});for(const id of m.members){const a=g.apesById.get(id);a.job='expedition';a.kingdomMission.targetId=target.resourceId}delete s.expeditionMessage;return;
  }
  if(!active.length&&!needs.food&&!needs.wood)s.expeditionMessage='Stores are sufficient';
 }
 releaseExpeditionActor(a){delete a.kingdomMission;delete a._activityTarget;delete a._activityAt;delete a.workTargetId;delete a.workCohort;a.carrying=!!a.expeditionCargo&&a.expeditionCargo.resource;this.game.navigation.resetActor?.(a)}
 depositExpeditionCargo(a){
  const cargo=a.expeditionCargo;if(!cargo||a.hp<=0)return false;a.carrying=cargo.resource;const s=this.game.settlement(cargo.sourceId);if(!s||distance(a,s)>80)return false;
  const cap=cargo.resource==='food'?Math.max(s.housing||0,s.population||0)*10+80+(s.stores||0)*120+this.game.colonies.expansion(s)*800:200+(s.level||1)*25+(s.stores||0)*80+this.game.colonies.expansion(s)*500,take=Math.min(cargo.amount,Math.max(0,cap-(s[cargo.resource]||0)));
  s[cargo.resource]=(s[cargo.resource]||0)+take;cargo.amount-=take;if(cargo.amount<=.00001){delete a.expeditionCargo;a.carrying=false}return true;
 }
 returnExpedition(s,m,actors,reason='',threat=false){
  m.returning=true;m.phase=threat?'threatened':'returning';m.reason=reason;m.progressAt=this.game.time;m.progressPoint={x:actors[0].x,y:actors[0].y};m.routeFailures=0;delete m.leg;
  for(const a of actors){a.kingdomMission.returning=true;delete a._activityTarget;this.game.navigation.resetActor?.(a)}
 }
 tickExpedition(s,m){
  const g=this.game,actors=[];for(const id of m.members){const a=g.apesById.get(id);if(!a||a.hp<=0)continue;this.depositExpeditionCargo(a);if(a.kingdomMission?.id!==m.id)continue;if(a.settlementId!==s.id||!['settled','scout'].includes(a.state)){this.releaseExpeditionActor(a);continue}actors.push(a)}
  this.counters.missionMembers+=m.members.length;if(!actors.length){m.status=m.returning?'complete':'cancelled';m.cargo={};s._jobsAt=0;return}
  const leader=actors[0],dt=clamp(g.time-(m.lastTick??g.time-1),0,3);m.lastTick=g.time;m.cargo={[m.resource]:actors.reduce((n,a)=>n+(a.expeditionCargo?.amount||0),0)};
  if(!m.returning&&m.target)g.world.requestCorridor?.(m.target,m.target,{id:'expedition-resource:'+m.id});
  const threatened=s.attack||actors.some(a=>g.humanGrid.nearest(a.x,a.y,210,1,h=>h.hp>0).length||g.vehicleGrid?.nearest(a.x,a.y,280,1,v=>v.hp>0).length);if(threatened&&!m.returning)this.returnExpedition(s,m,actors,'Workers recalled from danger',true);
  if(m.phase==='blocked'){if(g.time<(m.retryAt||0))return;m.phase='returning';m.progressAt=g.time;m.progressPoint={x:leader.x,y:leader.y};m.routeFailures=0}
  if(m.returning){let remaining=0;for(const a of actors){if(distance(a,s)<80){this.depositExpeditionCargo(a);this.releaseExpeditionActor(a)}else remaining++}if(!remaining){m.status='complete';m.cargo={};s._jobsAt=0;s.nextExpeditionAt=g.time+5;return}}
  let target=m.returning?s:m.target,object=m.returning?null:g.world.objects.get(target?.resourceId);
  if(!m.returning&&!this.resourceQuantity(object,m.resource)){
   const next=(m.retargets||0)<3?this.discoverResource(s,m.resource,leader,m):null;m.retargets=(m.retargets||0)+1;
   if(next){m.target=target=next;m.phase='traveling';m.gatherWork=0;m.progressAt=g.time;m.progressPoint={x:leader.x,y:leader.y};object=g.world.objects.get(next.resourceId);delete m.leg;for(const a of actors)delete a._activityTarget}else{this.returnExpedition(s,m,actors,'Resource area exhausted');target=s}
  }
  if(!m.returning&&actors.every(a=>distance(a,target)<72)){
   m.phase='gathering';m.progressAt=g.time;delete m.leg;const room=actors.reduce((n,a)=>n+Math.max(0,16-(a.expeditionCargo?.amount||0)),0);let amount=0;
   if(m.resource==='food'){amount=Math.min(this.resourceQuantity(object,'food'),room,actors.length*1.4*dt);object.food=this.resourceQuantity(object,'food')-amount;if(object.food<=0){object.dead=true;object.solid=false}}
   else{m.gatherWork=(m.gatherWork||0)+actors.length*dt;if(m.gatherWork>=8&&room>=this.resourceQuantity(object,'wood')){amount=g.world.clearTree(object,{settlementId:s.id,time:g.time});m.gatherWork=0}}
   for(const a of actors){const take=Math.min(amount,16-(a.expeditionCargo?.amount||0));if(take>0){a.expeditionCargo=a.expeditionCargo||{sourceId:s.id,resource:m.resource,amount:0};a.expeditionCargo.amount+=take;amount-=take}a.activity=m.resource==='wood'?'chopping timber':'gathering food';a.carrying=a.expeditionCargo?.amount>0?m.resource:false;delete a._activityTarget}
   m.cargo={[m.resource]:actors.reduce((n,a)=>n+(a.expeditionCargo?.amount||0),0)};
   if(m.cargo[m.resource]>=actors.length*12||m.resource==='wood'&&room<this.resourceQuantity(object,'wood'))this.returnExpedition(s,m,actors);
   return;
  }
  if(m.phase==='gathering')m.phase='traveling';
  if(!m.progressPoint||distance(leader,m.progressPoint)>45){m.progressPoint={x:leader.x,y:leader.y};m.progressAt=g.time;m.routeFailures=0}
  if(g.time-(m.progressAt||0)>24||!m.returning&&g.time>(m.travelUntil||m.startedAt+240)){
   if(!m.returning){s.expeditionRejected=s.expeditionRejected||{};s.expeditionRejected[m.target.resourceId]=g.time+180;const keys=Object.keys(s.expeditionRejected);if(keys.length>48)delete s.expeditionRejected[keys[0]];this.returnExpedition(s,m,actors,'Unreachable resource abandoned');target=s}
   else{m.phase='blocked';m.reason='Return route blocked — regrouping';m.retryAt=g.time+20;delete m.leg;return}
  }
  // All members use one streamed leg and shared route. The navigation system
  // selects real bridge approaches and bounded recovery paths for stragglers.
  const cohort='kingdom:'+m.id,waypoint=g.navigation.strategicTarget(leader,target,10,'ape');m.leg={x:waypoint.x,y:waypoint.y};g.world.requestCorridor?.(leader,m.leg,{id:cohort});g.navigation.setCohortRoute(cohort,leader,m.leg,10,5);
  m.regroupId=actors.some(a=>distance(a,leader)>240)?leader.id:null;for(const a of actors){a.navCohort=cohort;delete a._activityTarget}
 }
 modifiers(s){
  this.ensure(s);if(s._kingdomModsAt>this.game.time&&s._kingdomMods)return s._kingdomMods;
  const f=FOCUS[s.specialization]||FOCUS.balanced,elite=this.game.champions?.settlementBonus?.(s)||{},celebrating=s.moraleUntil>this.game.time;
  const m={food:(f.food||1)*(elite.food||1),construction:(f.construction||1)*(elite.build||1)*(celebrating?1.1:1),growth:(f.growth||1)*(elite.growth||1)*(celebrating?1.15:1),training:f.training||1,recovery:(f.recovery||1)*(elite.recovery||1),defense:(f.defense||1)/(elite.defense||1)*(s.defensePosture==='fortified'?.88:s.defensePosture==='mobile'?1.06:1),scout:(f.scout||1)*(elite.scout||1)};
  s._kingdomModsAt=this.game.time+1;s._kingdomMods=m;return m;
 }
 jobRatios(s){
  this.ensure(s);const f=FOCUS[s.specialization]||FOCUS.balanced;
  if(!f.ratios&&s.defensePosture==='balanced')return null;
  const r={...(f.ratios||{forager:.4,builder:.24,lumber:.09,gardener:.09,cook:.03,hauler:.05,caretaker:.02})};
  if(s.policy==='forage'){r.forager+=.07;r.builder=Math.max(.08,r.builder-.07)}
  if(s.policy==='fortify'){r.builder+=.06;r.forager-=.06}
  if(s.defensePosture==='fortified'){r.forager-=.08;r.hauler=Math.max(0,r.hauler-.02)}
  if(s.defensePosture==='mobile'){r.forager+=.03;r.builder-=.03}
  return r;
 }
 event(s,kind,text,color='green',cooldown=30){
  this.ensure(s);const last=s.kingdomEvents.findLast(e=>e.kind===kind);
  if(last&&this.game.time-last.time<cooldown)return false;
  s.kingdomEvents.push({kind,text,time:this.game.time});if(s.kingdomEvents.length>8)s.kingdomEvents.shift();
  this.game.notify?.(s.name+' — '+text,color);return true;
 }
 specialize(id,focus){const s=this.game.settlement(id);if(!s||!FOCUS[focus])return{ok:false,reason:'Choose an existing settlement and focus.'};this.ensure(s);s.specialization=focus;s._jobsAt=0;delete s._kingdomModsAt;this.game.colonies.updateHousing(s);this.event(s,'specialization',FOCUS[focus].label+' established.','green',0);return{ok:true}}
 posture(id,value){const s=this.game.settlement(id);if(!s||!['balanced','fortified','mobile'].includes(value))return{ok:false,reason:'Choose a defense posture.'};this.ensure(s);s.defensePosture=value;s._jobsAt=0;delete s._kingdomModsAt;return{ok:true}}
 available(s){return (this.game.settlementMembers.get(s.id)||[]).filter(a=>a.hp>0&&!a.kingdomMission&&a.state!=='young'&&!a.champion&&!a.divisionId&&!(a.recoveryRemaining>0))}
 startMission(source,destination,actors,kind,cargo={}){
  this.ensure(source);if(!actors.length)return{ok:false,reason:'No residents are available for this journey.'};
  if(source.kingdomMissions.filter(m=>m.status==='traveling').length>=4)return{ok:false,reason:'Four groups are already traveling from this settlement.'};
  const m={id:'kingdom-'+this.game.nextId++,kind,sourceId:source.id,targetId:destination?.id||'king',members:actors.slice(0,12).map(a=>a.id),cargo:{...cargo},status:'traveling',startedAt:this.game.time};
  source.kingdomMissions=source.kingdomMissions.filter(x=>x.status==='traveling').concat(m);
  for(const a of actors.slice(0,12)){this.game.tactics?.clearOrder(a);a.kingdomMission={id:m.id,sourceId:source.id,targetId:m.targetId,kind,returning:false};a.job='carrier';delete a._activityAt;delete a._activityTarget;if(a.state!=='young')a.state='settled';a.settlementId=source.id;}
  source._jobsAt=0;return{ok:true,count:m.members.length,mission:m};
 }
 requestReinforcements(id){
  const g=this.game,target=g.settlement(id);if(!target)return{ok:false,reason:'Choose a settlement to reinforce.'};
  const source=g.settlements.filter(s=>s.id!==id&&!s.attack&&s.population>=12&&this.available(s).length>=8).sort((a,b)=>distance(a,target)-distance(b,target))[0];
  if(!source)return{ok:false,reason:'Another safe settlement needs at least eight available adults.'};
  const members=this.available(source).sort((a,b)=>(b.trainingLevel||0)-(a.trainingLevel||0)),count=Math.min(8,Math.max(2,Math.floor(members.length/3))),result=this.startMission(source,target,members.slice(0,count),'reinforcement');
  if(result.ok)this.event(target,'reinforcements',result.count+' defenders are marching from '+source.name+'.','green',0);return result;
 }
 evacuate(id){
  const g=this.game,source=g.settlement(id);if(!source)return{ok:false,reason:'Choose a settlement to evacuate.'};
  const target=g.settlements.filter(s=>s.id!==id&&!s.attack&&s.population>0&&s.housing>s.population).sort((a,b)=>(b.specialization==='sanctuary')-(a.specialization==='sanctuary')||distance(a,source)-distance(b,source))[0];
  if(!target)return{ok:false,reason:'A safe settlement with spare housing is needed.'};
  const members=(g.settlementMembers.get(id)||[]).filter(a=>a.hp>0&&!a.kingdomMission&&(a.state==='young'||a.recoveryRemaining>0||['caretaker','cook','forager'].includes(a.job))).sort((a,b)=>(b.state==='young')-(a.state==='young'));
  const reserved=g.settlements.reduce((n,s)=>n+(s.kingdomMissions||[]).filter(m=>m.status==='traveling'&&m.targetId===target.id&&m.kind!=='supply').reduce((k,m)=>k+m.members.length,0),0),spaces=target.housing-target.population-reserved;
  const result=this.startMission(source,target,members.slice(0,Math.max(0,Math.min(12,spaces))),'evacuation');
  if(result.ok)this.event(source,'evacuation',result.count+' noncombatants are traveling to '+target.name+'.','green',0);return result;
 }
 sendSupplies(id,targetId='king'){
  const g=this.game,source=g.settlement(id),target=targetId==='king'?null:g.settlement(targetId);
  if(!source||targetId!=='king'&&!target||target===source)return{ok:false,reason:'Choose another settlement or the traveling army.'};
  const cargo=Math.min(source.specialization==='supply'?60:40,Math.floor(source.food-source.population*1.5)),actors=this.available(source).slice(0,3);
  if(cargo<10||actors.length<2)return{ok:false,reason:'Keep a food reserve and at least two available carriers before sending supplies.'};
  const result=this.startMission(source,target,actors,'supply',{food:cargo,wood:target?Math.min(12,Math.max(0,Math.floor(source.wood-20))):0});
  if(result.ok){source.food-=cargo;source.wood-=result.mission.cargo.wood;this.event(source,'supply-departure','Carriers depart with '+cargo+' food for '+(target?.name||'the King')+'.','green',0)}return result;
 }
 rally(id){const target=this.game.settlement(id);if(!target)return{ok:false,reason:'Choose a rally settlement.'};for(const s of this.game.settlements)s.rallyPoint=s===target;this.event(target,'rally','This settlement is the kingdom rally point.','green',0);return{ok:true}}
 missionTarget(a){const m=a.kingdomMission;if(!m)return null;if(m.kind==='expedition'){const s=this.game.settlement(m.sourceId),mission=s?.kingdomMissions.find(x=>x.id===m.id);return mission?.returning?s:mission?.target}return m.returning?this.game.settlement(m.sourceId):m.targetId==='king'?this.game.king:this.game.settlement(m.targetId)}
 travelTarget(a){
  const target=this.missionTarget(a);if(!target)return null;const source=this.game.settlement(a.kingdomMission.sourceId),mission=source?.kingdomMissions.find(m=>m.id===a.kingdomMission.id),goal=mission?.leg||target;let speed=(a.speed||75)*(source?.specialization==='supply'?1.25:1)*(a.state==='young'?.9:1);a.navCohort='kingdom:'+a.kingdomMission.id;
  if(a.kingdomMission.kind==='expedition'){a.activity=mission.phase==='gathering'?mission.resource==='wood'?'chopping timber':'gathering food':mission.phase==='blocked'?'regrouping':mission.returning?'returning home':'resource expedition';a.carrying=a.expeditionCargo?.amount>0?mission.resource:false;if(mission.phase==='blocked'||mission.phase==='gathering'||mission.regroupId===a.id)speed=0}
  else{a.activity=a.kingdomMission.kind==='evacuation'?'evacuating':a.kingdomMission.kind==='survivors'?'returning to sanctuary':a.kingdomMission.returning?'returning home':a.kingdomMission.kind==='reinforcement'?'reinforcing settlement':'carrying supplies';a.carrying=a.kingdomMission.kind==='supply'&&!a.kingdomMission.returning?'food':false}
  return{x:goal.x,y:goal.y,speed,activity:a.activity,carrying:a.carrying};
 }
 tickMissions(s){
  for(const m of s.kingdomMissions.slice(0,4)){
   if(m.kind==='expedition'){if(m.status==='traveling')this.tickExpedition(s,m);continue}
   if(m.status!=='traveling')continue;const group=m.members.slice(0,12).map(id=>this.game.apesById.get(id));
   // A direct player order takes precedence over the journey. Do not leave a
   // recalled/recruited actor reserved forever in an obsolete caravan.
   for(const a of group)if(a?.kingdomMission?.id===m.id&&(a.settlementId!==s.id||!['settled','scout','young'].includes(a.state))){delete a.kingdomMission;delete a._activityTarget;a.carrying=false}
   const actors=group.filter(a=>a?.hp>0&&a.kingdomMission?.id===m.id);this.counters.missionMembers+=m.members.length;
   if(!actors.length){const redirected=group.some(a=>a?.hp>0);m.status=redirected?'cancelled':'lost';this.event(s,redirected?'group-recalled':'group-lost',redirected?'A traveling group is following new orders.':'A traveling group was lost.',redirected?'green':'red');continue}
   const target=this.missionTarget(actors[0]);if(!target||target.hp===0){for(const a of actors){delete a.kingdomMission;delete a._activityTarget}m.status='cancelled';continue}
   const leader=actors[0],gap=distance(leader,target);
   if(!m.leg||distance(leader,m.leg)<90||gap<700){const fraction=Math.min(1,700/Math.max(1,gap));m.leg={x:leader.x+(target.x-leader.x)*fraction,y:leader.y+(target.y-leader.y)*fraction};for(const a of actors)delete a._activityTarget}
   // One short shared leg per group keeps distant journeys streamed and routed
   // without generating the whole kingdom or adding per-actor world searches.
   this.game.world.requestCorridor?.(leader,m.leg,{id:'kingdom:'+m.id});
   this.game.navigation.setCohortRoute?.('kingdom:'+m.id,leader,m.leg,10,5);
   if(!actors.every(a=>distance(a,target)<75))continue;
   if(m.returning){for(const a of actors){delete a.kingdomMission;delete a._activityTarget;a.carrying=false}m.status='complete';s._jobsAt=0;continue}
   if(m.kind==='supply'){
    const ratio=actors.length/m.members.length,food=Math.floor((m.cargo.food||0)*ratio),wood=Math.floor((m.cargo.wood||0)*ratio);
    if(m.targetId==='king')this.game.food+=food;else{target.food+=food;target.wood+=wood}
    this.event(s,'supply-arrival',food+' food delivered to '+(target.name||'the King')+'.','green',0);m.cargo={};m.returning=true;delete m.leg;for(const a of actors){a.kingdomMission.returning=true;delete a._activityTarget}continue;
   }
   this.receiveSurvivors(target.id,actors,{facilityName:m.facilityName,celebration:m.kind==='survivors',kind:m.kind});m.status='complete';s._jobsAt=0;
  }
 }
 receiveSurvivors(id,apes,options={}){
  const g=this.game,s=g.settlement(id);if(!s)return{ok:false,reason:'No settlement is available.'};this.ensure(s);let count=0;
  for(const a of apes){if(!a||a.hp<=0)continue;g.tactics?.clearOrder(a);delete a.kingdomMission;delete a._activityTarget;a.carrying=false;a.hordeOwner='king';a.settlementId=s.id;if(a.state!=='young')a.state='settled';a.homeX=s.x;a.homeY=s.y;count++}
  s._jobsAt=0;g.refreshSettlements();if(options.celebration){s.moraleUntil=g.time+90;this.event(s,'survivors',count+' prison survivors arrive'+(options.facilityName?' from '+options.facilityName:'')+'. The village celebrates for 90 seconds.','green',0)}else if(count)this.event(s,'arrivals',count+' '+(options.kind==='reinforcement'?'defenders':'residents')+' arrive safely.','green',0);return{ok:true,count};
 }
 welcomeSurvivors(apes,options={}){
  const living=apes.filter(a=>a?.hp>0);if(!living.length)return{ok:false,reason:'No survivors need a home.'};
  const g=this.game,s=g.settlements.filter(s=>!s.attack&&s.population>0).sort((a,b)=>(b.rallyPoint===true)-(a.rallyPoint===true)||(b.specialization==='sanctuary')-(a.specialization==='sanctuary')||distance(a,living[0])-distance(b,living[0]))[0];
  if(!s)return{ok:false,reason:'Found a settlement to shelter these survivors.'};
  const result=this.startMission(s,s,living.slice(0,12),'survivors');if(result.ok){result.mission.facilityName=options.facilityName;g.refreshSettlements();this.event(s,'survivors-coming','Prison survivors are on their way'+(options.facilityName?' from '+options.facilityName:'')+'.','green',0)}return result;
 }
 refit(id){
  const g=this.game,s=g.settlement(id);if(!s)return{ok:false,reason:'Choose a workshop.'};this.ensure(s);
  if(s.gearKits<1)return{ok:false,reason:'The workshop needs a completed gear kit.'};
  const champions=(g.settlementMembers.get(id)||[]).filter(a=>a.hp>0&&a.champion&&distance(a,s)<s.radius+50);
  for(const a of champions){const result=g.champions?.reforge?.(a.id);if(result===true||result?.ok){s.gearKits--;this.event(s,'refit','Equipment improved for '+(a.champion.name||'a champion')+'.','green',0);return{ok:true}}}
  return{ok:false,reason:'Station an eligible champion here for an equipment upgrade.'};
 }
 issueShields(id){
  const g=this.game,s=g.settlement(id);if(!s)return{ok:false,reason:'Choose a workshop.'};this.ensure(s);
  if(s.siegeMaterials<1)return{ok:false,reason:'The workshop needs crafted siege materials.'};
  const nearby=g.apeGrid.near(s.x,s.y,s.radius+100).filter(a=>a.hp>0&&['gorilla','orangutan'].includes(a.species)&&!a.shield?.reinforced).slice(0,4);
  if(!nearby.length)return{ok:false,reason:'Bring gorillas or orangutans close to this settlement.'};
  for(const a of nearby){const hp=Math.max(a.shield?.maxHp||0,a.species==='gorilla'?225:180);a.shield={...a.shield,hp,maxHp:hp,reinforced:true}}
  s.siegeMaterials--;this.event(s,'shields',nearby.length+' heavy apes receive reinforced log shields.','green',0);return{ok:true};
 }
 tick(s){
  const g=this.game;this.ensure(s);this.counters={missionMembers:0,warningCandidates:0,trainingCandidates:0};this.tickMissions(s);if(!s.population)return;this.planExpeditions(s);
  if(s.food<s.population*.4)this.event(s,'food-shortage','Food stocks are low. Send supplies or switch work priority.','red',60);
  if(s.specialization==='warcamp'&&!s.attack&&s.food>s.population){
   const adults=s._adults||[],start=(s.militiaCursor||0)%Math.max(1,adults.length);let trained=0;
   for(let i=0;i<Math.min(12,adults.length)&&trained<3;i++){const a=adults[(start+i)%adults.length];this.counters.trainingCandidates++;if(a.hp<=0||a.kingdomMission||a.trainingFacilityId||a.state==='young'||(a.trainingLevel||0)>=3||distance(a,s)>s.radius+40)continue;trained++;a.trainingProgress=(a.trainingProgress||0)+.65;const level=a.trainingLevel||0,cost=3+level*2;if(a.trainingProgress>=45*(level+1)&&s.food>=cost){s.food-=cost;a.trainingProgress=0;g.applyTraining(a,level+1)}}
   s.militiaCursor=start+12;
  }
  if(s.specialization==='workshop'&&!s.attack&&s.population>=8&&s.wood>=Math.max(40,(s.essentialWoodReserve||0)+12)&&s.food>=Math.max(8,s.population*2)&&(s.gearKits<6||s.siegeMaterials<6)){
   s.craftProgress++;if(s.craftProgress>=45){s.craftProgress=0;s.wood-=12;s.food-=6;s.gearKits=Math.min(6,s.gearKits+1);s.siegeMaterials=Math.min(6,s.siegeMaterials+1);this.event(s,'craft','A gear kit and siege materials are ready.','green',30)}
  }
  if(s.population>=18&&s.wood>=30&&!s.attack&&!s.projects.some(p=>p.commissioned)&&g.time>=(s.nextOpportunityAt||0)){s.nextOpportunityAt=g.time+150;this.event(s,'construction','Timber is available for a lodge construction commission.','green',150)}
  if(g.time>=(s.nextKingdomScoutAt||0)){
   s.nextKingdomScoutAt=g.time+8;const range=Math.min(1200,(s.radius+380)*this.modifiers(s).scout),scouting=s.specialization==='scout';
   if(scouting)g.world.reveal(s.x,s.y,range);
   if(scouting||s.scouts||s.lookouts.length){const people=g.humanGrid.near(s.x,s.y,range).slice(0,32),vehicles=(g.vehicleGrid?.near(s.x,s.y,range)||[]).slice(0,16);this.counters.warningCandidates=people.length+vehicles.length;const threat=people.concat(vehicles).find(h=>h.hp>0&&h.state!=='patrol'&&distance(h,s)<=range);
    if(threat){const direction=Math.abs(threat.x-s.x)>Math.abs(threat.y-s.y)?threat.x>s.x?'east':'west':threat.y>s.y?'south':'north';s.warning={x:threat.x,y:threat.y,time:g.time,direction};this.event(s,'raid-warning','Scouts report human movement to the '+direction+'.','red',30)}
   }
  }
 }
}
Object.defineProperty(ATSGame.prototype,'kingdom',{configurable:true,get(){return this._kingdom||(this._kingdom=new Kingdom(this))}});
const abstract=ATSGame.prototype.abstractActor;
ATSGame.prototype.abstractActor=function(a,dt,kind){
 if(kind==='ape'&&a.expeditionCargo)this.kingdom.depositExpeditionCargo(a);
 if(kind!=='ape'||!a.kingdomMission)return abstract.call(this,a,dt,kind);
 // Preserve ordinary aging and hit recovery, but let the group's shared route
 // own movement instead of alternating a straight step with a corridor step.
 const home=a.settlementId;let result;a.settlementId=null;try{result=abstract.call(this,a,dt,kind)}finally{a.settlementId=home}
 if(!this.blastActive(a)){const target=this.kingdom.travelTarget(a);if(target&&distance(a,target)>30){a._navPriority=5;for(let remaining=Math.min(1,dt);remaining>0;remaining-=.25)this.move(a,target.x-a.x,target.y-a.y,target.speed,Math.min(.25,remaining))}}
 return result;
};
const updateApe=ATSGame.prototype.updateApe;
ATSGame.prototype.updateApe=function(a,dt){if(a.expeditionCargo)this.kingdom.depositExpeditionCargo(a);return updateApe.call(this,a,dt)};
const p=ATSSettlements.prototype,init=p.init,tick=p.tick,members=p.members,activity=p.activityTarget,housing=p.updateHousing,absorb=p.absorb,clear=p.clearWork,complete=p.complete;
p.init=function(s){this.game.kingdom.ensure(s);return init.call(this,s)};
p.tick=function(s){const result=tick.call(this,s);this.game.kingdom.tick(s);return result};
p.members=function(s){return members.call(this,s).filter(a=>!a.kingdomMission)};
p.activityTarget=function(a,s){if(a.kingdomMission)return this.game.kingdom.travelTarget(a)||activity.call(this,a,s);const target=activity.call(this,a,s);if(s.defensePosture==='mobile'&&(a.job==='guardian'||a.state==='scout'))target.speed*=1.2;return target};
p.updateHousing=function(s){housing.call(this,s);if(s.specialization==='sanctuary')s.housing+=(s.huts||[]).filter(h=>h.hp>0&&(h.stage??4)===4).length*2};
p.absorb=function(a,damage){const s=this.game.settlement(a.settlementId),local=s&&distance(a,s)<s.radius;return absorb.call(this,a,local?damage*this.game.kingdom.modifiers(s).defense:damage)};
p.clearWork=function(s,project,workers){const before=s.wood,result=clear.call(this,s,project,workers);if(s.specialization==='supply'&&s.wood>before)s.wood+=(s.wood-before)*.5;return result};
p.complete=function(s,project){const result=complete.call(this,s,project);if(s.specialization==='workshop'&&project.kind==='barrier'){const b=s.barriers.find(b=>b.id===project.id+'-barrier');if(b&&!b.workshopReinforced){b.maxHp=Math.round(b.maxHp*1.25);b.hp=b.maxHp;b.workshopReinforced=true}}return result};
window.ATSKingdom=Kingdom;window.ATSKingdomFocus=FOCUS;
})();
