// Deterministic strategy and scouting. Enemy choices use current sight or
// explicitly recorded last-seen information; map lanes and camps are public.
import {familyOf,isCombat,canTransport} from './warfare.js';

export const BOT_CONFIGS=Object.freeze({
 easy:Object.freeze({think:2,attack:100,wave:72,group:7,workers:8,expand:150,cap:36,desks:3,printers:2,scoutAt:65,scoutEvery:38,scouts:1,micro:0,counterBias:.5,retreat:.18,workshopAt:65,laboratoryAt:150,dropAt:220,resourceMultiplier:1,initialBonus:0,stipend:0,stipendCap:0}),
 normal:Object.freeze({think:1,attack:72,wave:50,group:9,workers:12,expand:90,cap:60,desks:3,printers:3,scoutAt:35,scoutEvery:26,scouts:1,micro:1,counterBias:1.6,retreat:.27,workshopAt:65,laboratoryAt:150,dropAt:165,resourceMultiplier:1,initialBonus:0,stipend:0,stipendCap:0}),
 hard:Object.freeze({think:.45,attack:48,wave:34,group:10,workers:18,expand:65,cap:90,desks:4,printers:4,scoutAt:18,scoutEvery:17,scouts:2,micro:2,counterBias:2.5,retreat:.36,workshopAt:55,laboratoryAt:130,dropAt:125,resourceMultiplier:1,initialBonus:0,stipend:0,stipendCap:0}),
 impossible:Object.freeze({think:.25,attack:34,wave:25,group:12,workers:24,expand:48,cap:130,desks:5,printers:5,scoutAt:10,scoutEvery:12,scouts:3,micro:3,counterBias:3.2,retreat:.4,workshopAt:40,laboratoryAt:100,dropAt:95,resourceMultiplier:2,initialBonus:300,stipend:2,stipendCap:1200})
});
const FAMILIES=['rock','paper','scissors'],BEATS={rock:'scissors',paper:'rock',scissors:'paper'},COUNTER={rock:'paper',paper:'scissors',scissors:'rock'};
const PERSONALITIES=['raider','flanker','quartermaster','airmarshal','siegebreaker'];
const PLAN_ROTATIONS={
 raider:['raid','flank','mainPush','feint','airDrop'],
 flanker:['flank','feint','raid','mainPush','airDrop'],
 quartermaster:['mainPush','raid','airDrop','flank','feint'],
 airmarshal:['airDrop','raid','flank','feint','mainPush'],
 siegebreaker:['mainPush','feint','flank','airDrop','raid']
};
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
const freeUnit=e=>e.hp>0&&!e.transportedBy&&!e.aim&&!e.pendingAbility&&!e.roll&&!e.shadow&&!e.crash&&!e.divisionPull&&!e.divisionCast;
const centerOf=units=>({x:units.reduce((sum,e)=>sum+e.x,0)/(units.length||1),y:units.reduce((sum,e)=>sum+e.y,0)/(units.length||1)});
const seedFor=(map,team,players)=>[...map].reduce((n,c)=>(n*31+c.charCodeAt(0))>>>0,team*137+players*29);

export function initializeBotState(g,team){
 const cfg=BOT_CONFIGS[g.difficulty],seed=seedFor(g.map.id,team,g.players);
 return {seed,personality:PERSONALITIES[seed%PERSONALITIES.length],nextAttack:cfg.attack+team*2,nextScout:cfg.scoutAt+team,lastExpansion:0,planCursor:0,squadId:0,squads:[],history:[],enemyMemory:new Map(),scouted:new Set(),productionCursor:seed%3,ordersIssued:0,currentPlan:'economy',observedFamilies:{rock:0,paper:0,scissors:0}};
}
export function recordBotPlan(g,team,kind,extra={}){
 const state=g._bots[team];state.currentPlan=kind;
 state.history.push({time:g.time,kind,...extra});if(state.history.length>80)state.history.shift();
}
export function updateBotResources(g,dt){
 const cfg=BOT_CONFIGS[g.difficulty];if(!cfg.stipend)return;
 for(let team=1;team<g.players;team++)if(!g.teams[team].eliminated){
  const amount=Math.min(cfg.stipend*dt,Math.max(0,cfg.stipendCap-g.stats.botStipend[team]));
  g.teams[team].trash+=amount;g.stats.botStipend[team]+=amount;g.stats.botBonus[team]+=amount;
 }
}
function issue(g,team,units,order){
 if(!units.length)return;g.command(units.map(e=>e.id),order);g._bots[team].ordersIssued+=units.length;
}
function refreshSight(g,team){
 const state=g._bots[team],visible=g.entities.filter(e=>e.hp>0&&e.team>=0&&e.team!==team&&!e.transportedBy&&g.isVisible(e,team));
 for(const e of visible)if(e.type==='desk'||e.type==='printer'||e.type==='workshop'||e.type==='laboratory'||e.type==='twig'||e.type==='mouse')state.enemyMemory.set(e.id,{id:e.id,type:e.type,team:e.team,x:e.x,y:e.y,seenAt:g.time});
 const seenIds=new Set(visible.map(e=>e.id));
 for(const [id,memory] of state.enemyMemory)if(g.time-memory.seenAt>(memory.type==='twig'?45:210)||g.canSee(team,memory.x,memory.y)&&!seenIds.has(id))state.enemyMemory.delete(id);
 state.observedFamilies=Object.fromEntries(FAMILIES.map(f=>[f,visible.filter(e=>familyOf(e)===f).length]));
 return visible;
}
export function chooseBotTroop(g,team,army,visible,cfg=BOT_CONFIGS[g.difficulty]){
 const state=g._bots[team],counts=Object.fromEntries(FAMILIES.map(f=>[f,army.filter(e=>familyOf(e)===f).length]));
 const scores=FAMILIES.map((f,i)=>({type:f,score:visible.filter(e=>familyOf(e)===BEATS[f]).length*cfg.counterBias-counts[f]+((i+3-state.productionCursor%3)%3===0?.2:0)}));
 scores.sort((a,b)=>b.score-a.score);state.productionCursor++;return scores[0].type;
}
function cleanAssignments(g,team,own){
 const state=g._bots[team];
 for(const e of own)if(e.aiTask&&e.aiTaskUntil<=g.time){delete e.aiTask;delete e.aiTaskUntil;e.aiTaskGoal=null;}
 const alive=new Set(own.filter(e=>e.hp>0).map(e=>e.id));
 for(const squad of state.squads)squad.ids=squad.ids.filter(id=>alive.has(id)&&g.getEntity(id)?.aiSquad===squad.id);
 state.squads=state.squads.filter(squad=>{
  if(squad.ids.length&&g.time<squad.expires)return true;
  for(const id of squad.ids){const e=g.getEntity(id);if(e?.aiSquad===squad.id)delete e.aiSquad;}
  return false;
 });
}
function publicTarget(g,team,state){
 const points=g.starts.filter((_,i)=>i!==team&&!g.teams[i].eliminated);
 const ordered=points.map((p,i)=>({...p,priority:(i+state.seed)%points.length})).sort((a,b)=>a.priority-b.priority||distance(a,g.starts[team])-distance(b,g.starts[team]));
 return ordered[state.planCursor%Math.max(1,ordered.length)]||g.center;
}
function attackTarget(g,team,state,visible,kind){
 const origin=g.starts[team],memory=[...state.enemyMemory.values()];
 if(kind==='raid'||kind==='airDrop'){
  const workers=visible.filter(e=>e.type==='twig');
  if(workers.length)return {...workers.sort((a,b)=>distance(a,origin)-distance(b,origin))[0]};
  const expansion=memory.filter(e=>e.type==='desk'&&g.starts.every(p=>distance(e,p)>500));
  if(expansion.length)return expansion.sort((a,b)=>distance(a,origin)-distance(b,origin))[0];
 }
 const desks=visible.filter(e=>e.type==='desk');if(desks.length)return {...desks.sort((a,b)=>distance(a,origin)-distance(b,origin))[0]};
 const remembered=memory.filter(e=>e.type==='desk');if(remembered.length)return remembered.sort((a,b)=>b.seenAt-a.seenAt||distance(a,origin)-distance(b,origin))[0];
 return publicTarget(g,team,state);
}
function routeFor(g,origin,target,flank,index=0){
 const options=(g.map.lanes||[]).map((lane,i)=>{
  let points=lane.map(([x,y])=>({x,y}));if(!points.length)return null;
  if(distance(origin,points.at(-1))+distance(target,points[0])<distance(origin,points[0])+distance(target,points.at(-1)))points.reverse();
  const travel=distance(origin,points[0])+points.slice(1).reduce((sum,p,j)=>sum+distance(points[j],p),0)+distance(points.at(-1),target),info=g.map.laneInfo?.[i];
  return {points,travel,role:info?.role,risk:info?.risk,index:i};
 }).filter(Boolean).sort((a,b)=>a.travel-b.travel);
 if(!options.length)return [{x:target.x,y:target.y}];
 const side=options.filter(o=>o.role==='flank'||o.role==='highground');
 const candidates=flank?(side.length?side:options.slice(1).length?options.slice(1):options):options.slice(0,Math.min(2,options.length));
 const chosen=candidates[index%candidates.length];
 const points=chosen.points.filter(p=>distance(p,origin)>250&&distance(p,target)>250);
 return [...points,{x:target.x,y:target.y}];
}
function createSquad(g,team,units,kind,waypoints,target,extra={}){
 if(!units.length)return null;const state=g._bots[team],id=`${team}-${kind}-${++state.squadId}`,origin=centerOf(units);
 const length=waypoints.reduce((n,p,i)=>n+distance(i?waypoints[i-1]:origin,p),0);
 const squad={id,kind,ids:units.map(e=>e.id),waypoints,index:0,target:{x:target.x,y:target.y},created:g.time,expires:g.time+Math.max(60,length/85+70),lastOrder:0,...extra};
 for(const e of units)e.aiSquad=id;
 state.squads.push(squad);recordBotPlan(g,team,kind,{units:units.length,target:squad.target,route:waypoints.map(p=>({...p}))});
 return squad;
}
function runSquads(g,team){
 const state=g._bots[team];
 for(const squad of state.squads){
  const units=squad.ids.map(id=>g.getEntity(id)).filter(e=>e?.aiSquad===squad.id&&freeUnit(e));if(!units.length)continue;
  if(squad.kind==='airDrop'){runDrop(g,team,squad,units);continue;}
  let waypoint=squad.waypoints[squad.index];if(!waypoint){squad.expires=g.time;continue;}
  if(units.filter(e=>distance(e,waypoint)<Math.max(160,Math.sqrt(units.length)*35)).length>=Math.ceil(units.length*.6)){
   squad.index++;waypoint=squad.waypoints[squad.index];squad.lastOrder=0;if(!waypoint){squad.expires=g.time;continue;}
  }
  if(!squad.lastOrder||g.time-squad.lastOrder>9&&units.some(e=>e.order.type==='idle'&&distance(e,waypoint)>250)){
   issue(g,team,units,{type:squad.kind==='feint'&&squad.index===squad.waypoints.length-1?'move':'attackMove',x:waypoint.x,y:waypoint.y});squad.lastOrder=g.time;
  }
 }
}
function runDrop(g,team,squad,units){
 const cd=g.getEntity(squad.carrierId),escorts=squad.escortIds.map(id=>g.getEntity(id)).filter(Boolean);
 if(!cd){for(const e of units)delete e.aiSquad;squad.expires=g.time;return;}
 if(squad.phase==='loading'){
  const cargo=(cd.cargo||[]).map(id=>g.getEntity(id)).filter(Boolean);
  if(cargo.length>=squad.loadGoal||g.time-squad.created>22&&cargo.length>=2){
   const landing={x:clamp(squad.target.x+(team%2?210:-210),60,g.width-60),y:clamp(squad.target.y+160,60,g.height-60)};
   if(g.dropTransports([cd.id],landing.x,landing.y).ok){squad.phase='flight';squad.lastOrder=g.time;recordBotPlan(g,team,'airDropLaunch',{units:cargo.length,target:landing});}
  }else if(g.time-squad.created>28){for(const e of units){delete e.aiSquad;if(e.order.type==='board')issue(g,team,[e],{type:'stop'});}squad.expires=g.time;}
 }
 if(squad.phase==='flight'){
  if(!(cd.cargo||[]).length){
   const passengers=squad.passengerIds.map(id=>g.getEntity(id)).filter(e=>e&&!e.transportedBy);
   issue(g,team,passengers,{type:'attackMove',...squad.target});squad.phase='assault';squad.lastOrder=g.time;
   recordBotPlan(g,team,'dropRaid',{units:passengers.length,target:squad.target});
  }
 }
 if(g.time-(squad.escortOrder||0)>4){
  for(const e of escorts)if(freeUnit(e)&&!e.combatTarget){issue(g,team,[e],{type:'attackMove',x:cd.x,y:cd.y});}
  squad.escortOrder=g.time;
 }
 if(squad.phase==='assault'&&g.time-squad.lastOrder>25){delete cd.aiSquad;issue(g,team,[cd],{type:'move',...g.starts[team]});squad.expires=g.time;}
}
function planDrop(g,team,state,army,own,target){
 const cd=own.find(e=>e.type==='cd'&&freeUnit(e)&&!e.aiSquad&&!e.aiTask&&!(e.cargo||[]).length);
 if(!cd)return false;
 const passengers=army.filter(e=>freeUnit(e)&&canTransport(e.type)&&!e.aiSquad&&!e.aiTask&&distance(e,cd)<650).sort((a,b)=>distance(a,cd)-distance(b,cd)).slice(0,6);
 if(passengers.length<3)return false;
 const escorts=army.filter(e=>e.type==='airplane'&&freeUnit(e)&&!e.aiSquad&&!e.aiTask).slice(0,2);
 const result=g.loadTransport(passengers.map(e=>e.id),[cd.id]);if(!result.ok)return false;
 const squad=createSquad(g,team,[cd,...passengers,...escorts],'airDrop',[],target,{carrierId:cd.id,passengerIds:passengers.map(e=>e.id),escortIds:escorts.map(e=>e.id),phase:'loading',loadGoal:passengers.length});
 squad.expires=g.time+distance(cd,target)/150+120;return true;
}
function scout(g,team,state,cfg,army){
 if(g.time<state.nextScout)return;
 const active=army.filter(e=>e.aiTask==='scout');if(active.length>=cfg.scouts)return;
 const options=army.filter(e=>freeUnit(e)&&!e.aiSquad&&!e.aiTask&&!['divider','rollingrock','mage'].includes(e.type)).sort((a,b)=>(a.type==='airplane'?-1:0)-(b.type==='airplane'?-1:0)||a.id-b.id);
 const unit=options[0];if(!unit)return;
 const points=[...g.starts.filter((_,i)=>i!==team&&!g.teams[i].eliminated),...g.map.expansions.filter(p=>p.role==='outer'||p.role==='highground'||distance(p,g.starts[team])>1000)];
 for(const p of points)if(g.canSee(team,p.x,p.y))state.scouted.add(`${p.x},${p.y}`);
 let choices=points.filter(p=>!state.scouted.has(`${p.x},${p.y}`));if(!choices.length){state.scouted.clear();choices=points;}
 choices.sort((a,b)=>distance(unit,a)-distance(unit,b));const target=choices[(state.seed+state.planCursor)%Math.min(3,choices.length)];if(!target)return;
 unit.aiTask='scout';unit.aiTaskUntil=g.time+Math.max(30,distance(unit,target)/100+12);unit.aiTaskGoal={x:target.x,y:target.y};
 issue(g,team,[unit],{type:'move',x:target.x,y:target.y});recordBotPlan(g,team,'scout',{units:1,target:unit.aiTaskGoal});state.nextScout=g.time+cfg.scoutEvery;
}
function defendAndMicro(g,team,state,cfg,own,army,visible){
 const desks=own.filter(e=>e.type==='desk'&&!e.constructing),threats=visible.filter(e=>isCombat(e.type)||e.type==='cd');
 const intruder=threats.find(e=>desks.some(d=>distance(e,d)<560)||own.some(w=>w.type==='twig'&&distance(e,w)<230));
 if(intruder&&g.time>=(state.nextDefense||0)){
  const defenders=army.filter(e=>freeUnit(e)&&e.type!=='airplane'&&distance(e,intruder)<1300).sort((a,b)=>distance(a,intruder)-distance(b,intruder)).slice(0,cfg.group+cfg.micro*2);
  for(const e of defenders){delete e.aiSquad;e.aiTask='defend';e.aiTaskUntil=g.time+12;}
  issue(g,team,defenders,{type:'attackMove',x:intruder.x,y:intruder.y});if(defenders.length)recordBotPlan(g,team,'defendEconomy',{units:defenders.length,target:{x:intruder.x,y:intruder.y}});state.nextDefense=g.time+(cfg.micro>=2?2:5);
 }
 if(!cfg.micro)return;
 for(const worker of own.filter(e=>e.type==='twig'&&!e.constructing&&e.order.type!=='build')){
  if(worker.aiTask==='evacuate')continue;
  const danger=threats.find(e=>isCombat(e.type)&&distance(e,worker)<(cfg.micro>=2?190:135));if(!danger)continue;
  const home=desks.sort((a,b)=>distance(a,worker)-distance(b,worker))[0];if(!home)continue;
  const angle=Math.atan2(worker.y-danger.y,worker.x-danger.x),p={x:clamp(worker.x+Math.cos(angle)*230,30,g.width-30),y:clamp(worker.y+Math.sin(angle)*230,30,g.height-30)};
  worker.aiTask='evacuate';worker.aiTaskUntil=g.time+7;issue(g,team,[worker],{type:'move',...p});recordBotPlan(g,team,'evacuateWorkers',{units:1,target:p});
 }
 for(const e of army){
  if(!freeUnit(e)||e.aiTask==='retreat'||e.type==='airplane'||e.hp/e.maxHp>=cfg.retreat||g.time-(e.aiLastRetreat||-100)<32)continue;
  const nearby=threats.filter(t=>distance(e,t)<360);if(!nearby.length)continue;
  const danger=nearby.some(t=>familyOf(t)===COUNTER[familyOf(e)])||nearby.length>army.filter(a=>distance(a,e)<320).length;
  if(!danger&&cfg.micro<2)continue;
  const home=desks.sort((a,b)=>distance(a,e)-distance(b,e))[0];if(!home)continue;
  const center=centerOf(nearby),angle=Math.atan2(e.y-center.y,e.x-center.x),p={x:clamp(e.x+Math.cos(angle)*550,40,g.width-40),y:clamp(e.y+Math.sin(angle)*550,40,g.height-40)};
  delete e.aiSquad;e.aiTask='retreat';e.aiTaskUntil=g.time+16;e.aiLastRetreat=g.time;issue(g,team,[e],{type:'move',...p});recordBotPlan(g,team,'retreatRegroup',{units:1,target:p});
 }
 if(cfg.micro>=2)for(const e of army){
  if(!freeUnit(e)||e.aiTask==='retreat'||e.aiTask==='scout'||g.time<(e.aiKiteUntil||0))continue;
  const threat=threats.find(t=>familyOf(t)===COUNTER[familyOf(e)]&&distance(e,t)<145);if(!threat)continue;
  const support=army.filter(a=>a.id!==e.id&&familyOf(a)===COUNTER[familyOf(threat)]&&distance(a,e)<400);
  if(!support.length)continue;const safe=support.sort((a,b)=>distance(a,e)-distance(b,e))[0];
  const angle=Math.atan2(e.y-threat.y,e.x-threat.x);issue(g,team,[e],{type:'move',x:safe.x+Math.cos(angle)*80,y:safe.y+Math.sin(angle)*80});e.aiKiteUntil=g.time+3;
 }
}
export function updateBotStrategy(g,team,cfg=BOT_CONFIGS[g.difficulty]){
 const state=g._bots[team],own=g.entities.filter(e=>e.team===team&&e.hp>0),army=own.filter(e=>isCombat(e.type)&&!e.transportedBy);
 cleanAssignments(g,team,own);const visible=refreshSight(g,team);
 defendAndMicro(g,team,state,cfg,own,army,visible);scout(g,team,state,cfg,army);runSquads(g,team);
 if(g.time<state.nextAttack)return;
 const available=army.filter(e=>freeUnit(e)&&!e.aiSquad&&!e.aiTask&&e.type!=='airplane');
 if(available.length<cfg.group)return;
 const rotation=PLAN_ROTATIONS[state.personality],allowed=cfg.micro>=2?rotation:cfg.micro?rotation.filter(k=>k!=='feint'):['mainPush','flank'];
 let kind=allowed[state.planCursor++%allowed.length];
 const target=attackTarget(g,team,state,visible,kind),origin=centerOf(available),wave=available.slice(0,Math.max(cfg.group,Math.min(30,cfg.group+cfg.micro*5)));
 if(kind==='airDrop'&&g.time>=cfg.dropAt){state.wantTransport=true;if(planDrop(g,team,state,army,own,target)){state.nextAttack=g.time+cfg.wave;return;}}
 if(kind==='airDrop')kind='raid';
 if(kind==='flank'&&wave.length>=8){
  const midpoint=Math.ceil(wave.length/2);createSquad(g,team,wave.slice(0,midpoint),'flank',routeFor(g,origin,target,true,state.seed),target);createSquad(g,team,wave.slice(midpoint),'mainPush',routeFor(g,origin,target,false,state.seed),target);
 }else if(kind==='feint'&&wave.length>=8){
  const diversion=wave.slice(0,3),main=wave.slice(3),enemyAngle=Math.atan2(target.y-origin.y,target.x-origin.x);
  const bait={x:target.x-Math.cos(enemyAngle)*600,y:target.y-Math.sin(enemyAngle)*600},back={x:bait.x-Math.cos(enemyAngle)*700,y:bait.y-Math.sin(enemyAngle)*700};
  createSquad(g,team,diversion,'feint',[...routeFor(g,origin,bait,false,state.seed),back],target);
  createSquad(g,team,main,'flank',routeFor(g,origin,target,true,state.seed+1),target);
 }else{
  const route=routeFor(g,origin,target,kind==='raid',state.seed+state.planCursor);
  if(kind==='mainPush'){
   const angle=Math.atan2(target.y-origin.y,target.x-origin.x),stage={x:origin.x+Math.cos(angle)*Math.min(480,distance(origin,target)*.12),y:origin.y+Math.sin(angle)*Math.min(480,distance(origin,target)*.12)};
   route.unshift(stage);
  }
  createSquad(g,team,wave,kind,route,target);
 }
 state.nextAttack=g.time+cfg.wave;runSquads(g,team);
}
