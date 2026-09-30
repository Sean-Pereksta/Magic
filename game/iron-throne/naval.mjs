import { armyArrivalTurns, projectArmyStep } from './movement-timing.mjs';
import { FORMATIONS, UNITS } from './data.mjs';
import { atWar, distance, neighbors, findPath, moveCost, log, canAfford, canEnter, kingdom, pay, resolveAmphibiousLanding } from './core.mjs';
import { buildingLevel } from './economy.mjs';
import { knowledgeView, refreshKnowledge } from './fog.mjs';
import { SHIPS, initializeNaval, cargoCount, fleetCapacity, fleetSpeed, syncCargo, distributeCargo, troopCount, fleetAttackRange } from './naval-state.mjs';
import { navalGraph, navalNode, navalPath, nodeTile, shoreNodes, adjacentShore } from './naval-graph.mjs';
import { clearShot } from './structures.mjs';
import { coastalTarget, resolveCoastalAttack } from './naval-ranged.mjs';
import { markPlayerOverride } from './command-state.mjs';
import { resolveNavalCombat } from './naval-combat.mjs';
export { SHIPS, fleetCapacity, cargoCount } from './naval-state.mjs';
const fail=error=>({ok:false,error});
const owned=(s,owner,id)=>(s.fleets||[]).find(f=>f.id===id&&f.owner===owner);
function actionError(s,owner) {
  if(s.outcome||s.phase==='founding')return 'Naval orders are unavailable in this campaign phase.';
  if(!kingdom(s,owner))return 'Unknown House.';
  if(s.sequential&&s.sequential.order[s.sequential.index]!==owner)return 'Wait for your House’s activation.';
  return null;
}
export function shipBuildCheck(s,owner,tile,type) {
  const error=actionError(s,owner);if(error)return error;
  const t=s.tiles[tile],k=kingdom(s,owner),spec=SHIPS[type];
  if(!Object.hasOwn(SHIPS,type)||!spec||t?.owner!==owner||!buildingLevel(t,'shipyard'))return 'Build vessels at an owned Shipyard.';
  if(!shoreNodes(s,tile).length)return 'This Shipyard has no navigable water access.';
  if(s.armies.some(a=>a.tile===tile&&atWar(s,owner,a.owner)))return 'Enemy troops occupy this Shipyard.';
  if((s.shipQueues||[]).filter(q=>q.tile===tile).length>=12)return 'The Shipyard queue is full (12 ships).';
  if((s.shipQueues?.length||0)+(s.fleets||[]).reduce((n,f)=>n+f.ships.length,0)>=500)return 'The campaign vessel limit has been reached.';
  if(k.commands<1)return 'No construction orders left this turn.';
  if(k.population<spec.crew+20)return `Need ${spec.crew} population for crew and 20 civilians remaining.`;
  if(!canAfford(k,spec.cost))return `Missing ${Object.entries(spec.cost).filter(([r,n])=>k.resources[r]<n).map(([r,n])=>`${n-k.resources[r]} ${r}`).join(', ')}.`;
  return null;
}
export function queueShip(s,owner,tile,type) {
  const error=shipBuildCheck(s,owner,tile,type);if(error)return fail(error);
  initializeNaval(s);const k=kingdom(s,owner),spec=SHIPS[type];pay(k,spec.cost);k.population-=spec.crew;k.commands--;
  s.shipQueues.push({id:`ship-order-${s.nextId++}`,owner,tile,type,remaining:spec.turns,total:spec.turns,lastTickTurn:0});return {ok:true};
}
export function cancelShip(s,owner,id) {
  const error=actionError(s,owner);if(error)return fail(error);
  const q=(s.shipQueues||[]).find(q=>q.id===id&&q.owner===owner&&s.tiles[q.tile]?.owner===owner);
  if(!q)return fail('This construction order is unavailable.');
  pay(kingdom(s,owner),SHIPS[q.type].cost,1);kingdom(s,owner).population+=SHIPS[q.type].crew;s.shipQueues=s.shipQueues.filter(x=>x!==q);return {ok:true};
}
export function shipLaunchNode(s,owner,tile,graph=navalGraph(s)) {
  let frontier=shoreNodes(s,tile,graph);const seen=new Set(frontier),occupants=new Map();
  for(const f of s.fleets||[]){if(!occupants.has(f.node))occupants.set(f.node,[]);occupants.get(f.node).push(f);}
  while(frontier.length){
    // Prefer a friendly stack among equally near positions; never spawn into
    // another House's fleet. Full friendly stacks can share a new fleet node.
    const open=frontier.filter(node=>(occupants.get(node)||[]).every(f=>f.owner===owner));
    open.sort((a,b)=>Number((occupants.get(b)||[]).some(f=>f.ships.length<100))-Number((occupants.get(a)||[]).some(f=>f.ships.length<100))||a.localeCompare(b));
    if(open.length)return open[0];
    const next=[];
    // A surrounded yard searches beyond occupied nodes, but only along its
    // connected waterway: never across land or into a separate lake.
    for(const node of frontier)for(const neighbor of graph.get(node)||[])if(!seen.has(neighbor)){seen.add(neighbor);next.push(neighbor);}
    frontier=next;
  }
  return null;
}
export function resolveShipConstruction(s) {
  initializeNaval(s);if(s.navalConstructionTurn===s.turn)return;s.navalConstructionTurn=s.turn;
  const graph=navalGraph(s),yards=new Set();
  for(const q of [...s.shipQueues]){
    const t=s.tiles[q.tile];
    if(t?.owner!==q.owner||!buildingLevel(t,'shipyard')){s.shipQueues=s.shipQueues.filter(x=>x!==q);continue;}
    if(yards.has(q.tile))continue;yards.add(q.tile);
    if(q.lastTickTurn===s.turn)continue;q.lastTickTurn=s.turn;q.remaining=Math.max(0,q.remaining-1);
    if(q.remaining)continue;
    const node=shipLaunchNode(s,q.owner,q.tile,graph);
    if(!node)continue; // Only a fully occupied connected waterway delays launch.
    let fleet=s.fleets.find(f=>f.owner===q.owner&&f.node===node&&f.ships.length<100);
    if(!fleet){fleet={id:`fleet-${s.nextId++}`,owner:q.owner,node,tile:nodeTile(node),ships:[],cargo:[],morale:1,path:[],target:null,order:'hold',landing:null,movementTurn:s.turn,movementSpent:0,resolvedTurn:s.turn};s.fleets.push(fleet);}
    const spec=SHIPS[q.type];fleet.ships.push({id:`ship-${s.nextId++}`,type:q.type,hp:spec.hull,crew:spec.crew,cargo:[]});
    s.shipQueues=s.shipQueues.filter(x=>x!==q);syncCargo(fleet);
  }
}
export const boardingStack=(s,f)=>{const group=s.fleets.filter(x=>x.owner===f.owner&&x.node===f.node&&x.resolvedTurn!==s.turn);return group.includes(f)&&group.reduce((n,x)=>n+x.ships.length,0)<=100?group:[f];};
export const boardingFleet=(s,f)=>({...f,ships:boardingStack(s,f).flatMap(x=>x.ships),cargo:boardingStack(s,f).flatMap(x=>x.cargo)});
function combineBoardingStack(s,f){const orders={path:f.path,target:f.target,order:f.order,landing:f.landing,escort:f.escort,attackTile:f.attackTile};for(const other of boardingStack(s,f))if(other!==f)mergeFleets(s,f.owner,f.id,other.id);Object.assign(f,orders);}
export const reservedCargo=(s,f,except=null)=>s.armies.filter(a=>a.id!==except&&a.embarkOrder?.status!=='paused'&&boardingStack(s,f).some(x=>a.embarkOrder?.fleet===x.id)).reduce((n,a)=>n+Math.min(a.embarkOrder.count,troopCount(a)),0);
export const boardingCount=(s,a,f)=>Math.max(0,Math.min(troopCount(a),fleetCapacity(boardingFleet(s,f))-cargoCount(boardingFleet(s,f))-reservedCargo(s,f,a.id)));
export function embarkCheck(s,owner,armyId,fleetId,{resolving=false}={}) {
  const error=actionError(s,owner);if(error)return error;
  const a=s.armies.find(a=>a.id===armyId&&a.owner===owner),f=owned(s,owner,fleetId);
  if(!a||!f)return 'Select your land army and a friendly fleet.';
  if(a.embarkedFleetId)return 'This army is already embarked.';
  if(!adjacentShore(s,f,a.tile)||!canEnter(s,owner,s.tiles[a.tile])||s.tiles[a.tile].owner&&atWar(s,owner,s.tiles[a.tile].owner))return 'Embark from an adjacent friendly or unclaimed shoreline.';
  if(s.armies.some(e=>e.tile===a.tile&&atWar(s,owner,e.owner)))return 'Clear enemy troops from the embarkation shore first.';
  if(!resolving&&(a.resolvedTurn===s.turn||f.resolvedTurn===s.turn))return 'This force has already acted this turn.';
  const count=boardingCount(s,a,f);
  if(!count)return 'No unreserved transport space. Each Transport holds 25 troops.';
  if(count<troopCount(a)&&s.armies.length+s.fleets.reduce((n,x)=>n+x.cargo.length,0)>=500)return 'The campaign army limit prevents splitting this force.';
  return null;
}
// An embark destination is land adjacent to the actual fleet, including river banks.
export function findBoardingRoute(s,a,f) {
  const shore=s.tiles[f.tile];if(!shore)return null;
  const blocked=new Set(s.armies.filter(e=>atWar(s,a.owner,e.owner)).map(e=>e.tile));
  const routes=[shore,...neighbors(s,shore)].filter(t=>adjacentShore(s,f,t.id)&&canEnter(s,a.owner,t)&&(!t.owner||!atWar(s,a.owner,t.owner))&&!blocked.has(t.id)).map(t=>{
    const path=findPath(s,a.tile,t.id,a.owner,false,blocked);
    if(a.tile!==t.id&&!path.length)return null;
    const turns=armyArrivalTurns(s,{...a,target:t.id},path);
    let from=s.tiles[a.tile],cost=0;for(const id of path){cost+=moveCost(from,s.tiles[id]);from=s.tiles[id];}
    return {path,embarkPosition:t.id,turns,cost};
  }).filter(r=>r&&r.turns!==null).sort((a,b)=>a.turns-b.turns||a.cost-b.cost||a.embarkPosition.localeCompare(b.embarkPosition));
  return routes[0]||null;
}
// Forecast accepted, visible orders using the same resolution order: adjacent
// boarding, sailing, land movement, then arrival boarding. No combat is predicted.
export function boardingArrival(s,a,f) {
  if(a.embarkOrder?.status==='paused')return null;
  if(!f.path?.length){
    const position=a.embarkOrder?.embarkPosition,tiles=[a.tile,...a.path||[]];
    // Reuse the authoritative queued route for stationary targets. Panning and
    // selection need only recalculate costs, not run several path searches.
    if(position&&adjacentShore(s,f,position)&&canEnter(s,a.owner,s.tiles[position])&&(!s.tiles[position].owner||!atWar(s,a.owner,s.tiles[position].owner))&&(a.path?.at(-1)||a.tile)===position&&!s.armies.some(e=>atWar(s,a.owner,e.owner)&&tiles.includes(e.tile))){
      const turns=armyArrivalTurns(s,a);if(turns!==null)return {path:a.path||[],embarkPosition:position,turns,target:f.tile};
    }
    const route=findBoardingRoute(s,a,f);return route&&{...route,target:f.tile};
  }
  const army={...a},fleet={...f,path:[...f.path]};let firstRoute=null;
  for(let offset=0;offset<=f.path.length+2;offset++){
    const world={...s,turn:s.turn+offset};
    if(army.resolvedTurn===world.turn)continue;
    if(adjacentShore(world,fleet,army.tile)){
      const route=findBoardingRoute(world,army,fleet);
      if(route?.path.length===0)return {...(firstRoute||route),turns:offset,target:firstRoute?.target||fleet.tile};
    }
    if(fleet.resolvedTurn!==world.turn){
      const budget=fleetSpeed(fleet)+(fleet.sailingCarry||0),spent=fleet.movementTurn===world.turn?fleet.movementSpent||0:0;
      const steps=Math.min(fleet.path.length,Math.floor(budget*(1-spent)+1e-8));
      if(steps){fleet.node=fleet.path[steps-1];fleet.tile=nodeTile(fleet.node);fleet.path=fleet.path.slice(steps);}
      fleet.sailingCarry=fleet.path.length?Math.max(0,budget*(1-spent)-steps):0;
    }
    const route=findBoardingRoute(world,army,fleet);
    if(!route){if(!fleet.path.length)return null;continue;}
    firstRoute??={...route,target:fleet.tile};
    if(!fleet.path.length)return {...firstRoute,turns:offset+route.turns};
    const step=projectArmyStep(world,army,route.path);army.tile=step.tile;
    if(!step.path.length)return {...firstRoute,turns:offset};
  }
  return null;
}
export function boardingPlan(s,owner,armyId,fleetId) {
  const error=actionError(s,owner);if(error)return fail(error);
  const a=s.armies.find(a=>a.id===armyId&&a.owner===owner),f=owned(s,owner,fleetId);
  if(!a||!f||a.embarkedFleetId)return fail('Select your land army and a friendly transport.');
  if(a.resolvedTurn===s.turn||f.resolvedTurn===s.turn)return fail('This force has already acted this turn.');
  const count=boardingCount(s,a,f);
  if(!count)return fail('Transport no longer has enough capacity. No unreserved transport space.');
  if(count<troopCount(a)&&s.armies.length+s.fleets.reduce((n,x)=>n+x.cargo.length,0)>=500)return fail('The campaign army limit prevents splitting this force.');
  const route=findBoardingRoute(knowledgeView(s,owner),a,f);
  if(!route)return fail('No valid embark route remains. Embark from a friendly or unclaimed shoreline.');
  return {ok:true,count,remaining:troopCount(a)-count,...route};
}
export function orderEmbark(s,owner,armyId,fleetId,approvedCount=null) {
  const a=s.armies.find(a=>a.id===armyId&&a.owner===owner);
  if(a?.embarkOrder)return fail('Boarding is already queued; cancel it before issuing another boarding order.');
  const plan=boardingPlan(s,owner,armyId,fleetId);if(!plan.ok)return plan;
  if(approvedCount!==null&&(!Number.isInteger(approvedCount)||approvedCount!==plan.count))return fail('Transport capacity changed. Review the boarding count again.');
  const f=owned(s,owner,fleetId);combineBoardingStack(s,f);
  a.embarkOrder={type:'board-transport',fleet:f.id,count:plan.count,embarkPosition:plan.embarkPosition,turnsUntilCompletion:plan.turns,status:'active'};
  a.path=plan.path;a.target=plan.path.length?plan.embarkPosition:null;a.structureTarget=null;a.order=plan.path.length?'move':'hold';delete a.boardingNotice;markPlayerOverride(s,a);
  return {ok:true,count:plan.count,remaining:plan.remaining};
}
function stopBoarding(s,a,reason,{cancel=false}={}) {
  a.path=[];a.target=null;a.order='hold';a.structureTarget=null;
  if(a.boardingNotice!==reason)log(s,`${a.name||a.id}: ${reason}`,'military',{audience:[a.owner]});
  a.boardingNotice=reason;
  if(cancel)delete a.embarkOrder;
  else Object.assign(a.embarkOrder,{status:'paused',reason,turnsUntilCompletion:null});
  return false;
}
export function revalidateBoardingOrder(s,a) {
  const order=a.embarkOrder;if(!order)return false;
  const f=s.fleets.find(f=>f.id===order.fleet);
  if(!f)return stopBoarding(s,a,'Transport was destroyed or is no longer available.',{cancel:true});
  if(f.owner!==a.owner)return stopBoarding(s,a,'Transport is no longer friendly.',{cancel:true});
  if(boardingCount(s,a,f)<Math.min(order.count,troopCount(a)))return stopBoarding(s,a,'Transport no longer has enough capacity.');
  const route=findBoardingRoute(knowledgeView(s,a.owner),a,f);
  if(!route)return stopBoarding(s,a,'No valid embark route remains. Transport moved out of reachable boarding range or the route is blocked.');
  Object.assign(order,{status:'active',embarkPosition:route.embarkPosition,turnsUntilCompletion:route.turns});delete order.reason;delete a.boardingNotice;
  a.path=route.path;a.target=route.path.length?route.embarkPosition:null;a.order=route.path.length?'move':'hold';a.structureTarget=null;
  return true;
}
// Called only by resolution of a previously accepted order, after land movement.
export function resolveArmyBoarding(s,a) {
  const order=a.embarkOrder;if(!order||order.status==='paused')return;
  const f=s.fleets.find(f=>f.id===order.fleet);
  if(!f||f.owner!==a.owner){revalidateBoardingOrder(s,a);return;}
  if(!adjacentShore(s,f,a.tile))return;
  const result=embarkArmy(s,a.owner,a.id,f.id,order.count,{resolving:true});
  if(!result.ok)stopBoarding(s,a,result.error);
}
export function embarkArmy(s,owner,armyId,fleetId,limit=Infinity,options={}) {
  const error=embarkCheck(s,owner,armyId,fleetId,options);if(error)return fail(error);
  const a=s.armies.find(a=>a.id===armyId),f=owned(s,owner,fleetId),count=Math.min(limit,boardingCount(s,a,f)),total=troopCount(a);
  combineBoardingStack(s,f);let boarded=a;delete a.embarkOrder;
  if(count<total){
    boarded={...a,id:`army-${s.nextId++}`,units:Object.fromEntries(Object.keys(UNITS).map(u=>[u,0]))};
    let left=count;for(const u of Object.keys(UNITS)){const n=Math.min(left,a.units[u]);boarded.units[u]=n;a.units[u]-=n;left-=n;}
    // A partial detachment leaves its general with the original force.
    delete boarded.commandId;delete boarded.commandBonus;delete boarded.commandMove;
    const baseline=a.commandBaseline||total;boarded.commandBaseline=Math.floor(baseline*count/total);a.commandBaseline=baseline-boarded.commandBaseline;
    a.path=[];a.target=null;a.structureTarget=null;a.order='hold';a.resolvedTurn=s.turn;
  }else s.armies=s.armies.filter(x=>x!==a);
  boarded.path=[];boarded.target=null;boarded.structureTarget=null;boarded.order='embarked';boarded.resolvedTurn=s.turn;
  f.cargo.push(boarded);f.loadedTurn=s.turn;syncCargo(f);refreshKnowledge(s);return {ok:true,count,remaining:total-count};
}
export function resolveEmbarkOrders(s,owner=null) {
  for(const a of [...s.armies].sort((a,b)=>a.id.localeCompare(b.id))){
    if(!a.embarkOrder||owner&&a.owner!==owner||a.resolvedTurn===s.turn)continue;
    if(!revalidateBoardingOrder(s,a)||a.path.length)continue;
    resolveArmyBoarding(s,a);
  }
}
export function orderFleet(s,owner,id,target,order='move') {
  const error=actionError(s,owner);if(error)return fail(error);
  const f=owned(s,owner,id);if(!f||!['move','attack','unload','escort','intercept','blockade','hold'].includes(order))return fail('Select one of your fleets and a valid order.');
  if(f.resolvedTurn===s.turn)return fail('This fleet has already acted this turn.');
  if(order==='hold'){Object.assign(f,{path:[],target:null,order,landing:null,escort:null,attackTile:null});return {ok:true};}
  if(order==='blockade'&&!f.ships.some(v=>v.type==='warship'))return fail('A blockade requires a Warship.');
  const view=knowledgeView(s,owner),graph=navalGraph(view);let landing=null,escort=null,attackTile=null,ends=[];
  if(order==='escort'){
    const leader=view.fleets.find(x=>x.id===target&&x.owner===owner&&x.id!==id&&x.order!=='escort');
    if(!leader)return fail('Choose another friendly fleet to escort.');
    escort=leader.id;ends=[leader.node];
  }else if(order==='unload'){
    if(!cargoCount(f))return fail('This fleet has no embarked troops.');
    if(!canEnter(view,owner,view.tiles[target]))return fail('Choose a legal land destination; neutral borders require access or war.');
    landing=target;ends=shoreNodes(view,target,graph);
  }else if(order==='attack'){
    if(view.tiles[target]?.fog!=='visible')return fail('Observe this location before ordering an attack.');
    const enemy=view.fleets.find(e=>e.tile===target&&atWar(view,owner,e.owner));
    const coast=!enemy&&coastalTarget(view,owner,target);
    if(!enemy&&!coast)return fail('Select a visible enemy fleet, army or coastal structure at war with your House.');
    if(coast&&!f.ships.some(v=>v.type==='warship'))return fail('Only Warships can bombard land from the water.');
    attackTile=target;const range=fleetAttackRange(f);
    ends=[...graph.keys()].filter(node=>{const t=view.tiles[nodeTile(node)],d=distance(t,view.tiles[target]);return d<=range&&clearShot(view,t,view.tiles[target])&&(coast||d>1||node===enemy.node||graph.get(node)?.includes(enemy.node));});
  }else{
    const node=navalNode(view,target,graph);if(node)ends=[node];
  }
  const routes=ends.map(end=>({end,path:navalPath(view,f.node,end,{graph})})).filter(x=>x.path!==null).sort((a,b)=>a.path.length-b.path.length||a.end.localeCompare(b.end));
  if(!routes.length)return fail('No connected water route is known. Ships can only sail on ocean or connected rivers; scout farther first.');
  Object.assign(f,{path:routes[0].path,target:routes[0].end,order,landing,escort,attackTile});return {ok:true};
}
export function mergeFleets(s,owner,id,otherId) {
  const error=actionError(s,owner);if(error)return fail(error);
  const f=owned(s,owner,id),other=owned(s,owner,otherId);
  if(!f||!other||f===other||f.node!==other.node||f.ships.length+other.ships.length>100)return fail('Bring two friendly fleets to the same water position (maximum 100 vessels).');
  if(f.resolvedTurn===s.turn||other.resolvedTurn===s.turn)return fail('A fleet has already acted this turn.');
  f.movementSpent=Math.max(f.movementTurn===s.turn?f.movementSpent:0,other.movementTurn===s.turn?other.movementSpent:0);f.movementTurn=s.turn;
  for(const a of s.armies)if(a.embarkOrder?.fleet===other.id)a.embarkOrder.fleet=f.id;
  f.ships.push(...other.ships);f.cargo.push(...other.cargo);s.fleets=s.fleets.filter(x=>x!==other);Object.assign(f,{path:[],target:null,order:'hold',landing:null,escort:null,attackTile:null});syncCargo(f);return {ok:true};
}
export function resolveFleetMovement(s,owner=null) {
  initializeNaval(s);resolveEmbarkOrders(s,owner);const graph=navalGraph(s);
  const fleets=s.fleets.filter(f=>!owner||f.owner===owner).sort((a,b)=>Number(a.order!=='escort')-Number(b.order!=='escort')||a.id.localeCompare(b.id));
  for(const f of fleets){
    if(!s.fleets.includes(f)||f.resolvedTurn===s.turn)continue;f.resolvedTurn=s.turn;
    if(f.movementTurn!==s.turn){f.movementTurn=s.turn;f.movementSpent=0;}
    if(f.order==='escort'){
      const leader=s.fleets.find(x=>x.id===f.escort&&x.owner===f.owner);
      if(leader){const v=knowledgeView(s,f.owner),end=leader.target||leader.node;f.path=navalPath(v,f.node,end)||[];f.target=end;}
      else{f.order='hold';f.path=[];}
    }
    if(f.order==='intercept'&&!f.path.length){
      const view=knowledgeView(s,f.owner),enemy=view.fleets.filter(e=>atWar(s,f.owner,e.owner)).map(e=>({e,path:navalPath(view,f.node,e.node)})).filter(x=>x.path!==null&&x.path.length<=fleetSpeed(f)).sort((a,b)=>a.path.length-b.path.length)[0];
      if(enemy){f.path=enemy.path;f.target=enemy.e.node;}
    }
    const sailingBudget=fleetSpeed(f)+(f.sailingCarry||0);
    let fought=false;
    const fire=()=>{
      if(f.order!=='attack'||!f.attackTile)return false;
      const view=knowledgeView(s,f.owner),enemy=view.fleets.find(e=>e.tile===f.attackTile&&atWar(s,f.owner,e.owner));
      if(enemy){const actual=s.fleets.find(x=>x.id===enemy.id);if(resolveNavalCombat(s,f,actual,{contact:distance(s.tiles[f.tile],s.tiles[actual.tile])<=1}).ok)return true;}
      else if(coastalTarget(view,f.owner,f.attackTile)&&resolveCoastalAttack(s,f,f.attackTile))return true;
      return false;
    };
    if(fire()){fought=true;f.movementSpent=1;f.path=[];}

    while(!fought&&f.path.length&&f.movementSpent<1){
      const next=f.path[0];if(!graph.get(f.node)?.includes(next)){f.path=[];break;}
      const cost=1/sailingBudget;
      if(f.movementSpent+cost>1+1e-8)break;
      // A guarding fleet may intercept at the connected next position, never across land.
      const enemies=s.fleets.filter(e=>e!==f&&atWar(s,f.owner,e.owner)&&(e.node===next||['intercept','blockade','escort'].includes(e.order)&&graph.get(next)?.includes(e.node)&&e.interceptedTurn!==s.turn));
      const enemy=enemies.sort((a,b)=>Number(a.node!==next)-Number(b.node!==next)||a.id.localeCompare(b.id))[0];
      if(enemy){enemy.interceptedTurn=s.turn;
        if(enemy.node!==next&&!graph.get(f.node)?.includes(enemy.node)){f.node=next;f.tile=nodeTile(next);f.path.shift();syncCargo(f);}
        const origin=f.node;resolveNavalCombat(s,f,enemy,{contact:true});f.movementSpent=1;fought=true;
        if(s.fleets.includes(f)&&f.node===origin&&f.node!==next&&!s.fleets.some(e=>e.node===next&&atWar(s,f.owner,e.owner))){f.node=next;f.tile=nodeTile(next);f.path.shift();syncCargo(f);}break;}
      f.node=next;f.tile=nodeTile(next);f.path.shift();f.movementSpent+=cost;syncCargo(f);refreshKnowledge(s);
      if(fire()){fought=true;f.movementSpent=1;f.path=[];break;}
    }
    if(!s.fleets.includes(f))continue;
    f.sailingCarry=!fought&&f.path.length?Math.max(0,sailingBudget*(1-f.movementSpent)):0;
    // Same-position contact can occur on imported games or at a river meeting.
    const enemy=s.fleets.find(e=>e!==f&&e.node===f.node&&atWar(s,e.owner,f.owner));
    if(enemy&&!fought){resolveNavalCombat(s,f,enemy,{contact:true});f.movementSpent=1;fought=true;}
    if(!s.fleets.includes(f))continue;
    if(f.order==='unload'&&f.landing&&!f.path.length&&!fought&&adjacentShore(s,f,f.landing)&&canEnter(s,f.owner,s.tiles[f.landing])){
      for(const a of [...f.cargo]){
        if(resolveAmphibiousLanding(s,a,f.landing)){f.cargo=f.cargo.filter(x=>x!==a);delete a.embarkedFleetId;s.armies.push(a);}
      }
      syncCargo(f);f.movementSpent=1;f.unloadedTurn=s.turn;f.landing=null;f.order='hold';f.target=null;
    }
    if(!f.path.length&&['move','attack'].includes(f.order)){f.order='hold';f.target=null;f.attackTile=null;}
    refreshKnowledge(s);
  }
}
export function blockadeAt(s,tile,owner) {
  if(!(s.fleets||[]).some(f=>f.order==='blockade'&&atWar(s,owner,f.owner)))return false;
  const nodes=shoreNodes(s,tile);return (s.fleets||[]).some(f=>f.order==='blockade'&&!f.path.length&&nodes.includes(f.node)&&atWar(s,owner,f.owner)&&f.ships.some(v=>v.type==='warship'));
}
export function validateNaval(s) {
  initializeNaval(s);const fail=()=>{throw new Error('Damaged naval or embarked army data.');},graph=navalGraph(s);
  const int=(n,min=0,max=100000)=>Number.isInteger(n)&&n>=min&&n<=max,house=id=>s.kingdoms.some(k=>k.id===id);
  if(s.navalVersion!==1||!Array.isArray(s.fleets)||s.fleets.length>500||!Array.isArray(s.shipQueues)||s.shipQueues.length>500)fail();
  if(s.navalConstructionTurn!==undefined&&!int(s.navalConstructionTurn,0,s.turn))fail();
  const ids=new Set(),armies=new Set(s.armies.map(a=>a.id));
  const unique=(id,prefix)=>{if(typeof id!=='string'||!new RegExp(`^${prefix}-[0-9]+$`).test(id)||ids.has(id)||Number(id.split('-').at(-1))>=s.nextId)fail();ids.add(id);};
  for(const f of s.fleets){
    if(!f||!house(f.owner)||!graph.has(f.node)||f.tile!==nodeTile(f.node)||!Array.isArray(f.ships)||!f.ships.length||f.ships.length>100||!Array.isArray(f.cargo)||!Array.isArray(f.path)||f.path.length>graph.size||!['move','attack','unload','escort','intercept','blockade','hold'].includes(f.order)||!Number.isFinite(f.morale)||f.morale<.2||f.morale>1)fail();
    unique(f.id,'fleet');
    if(!Number.isFinite(f.movementSpent)||f.movementSpent<0||f.movementSpent>1.00001||!int(f.movementTurn,0,s.turn))fail();
    for(const field of ['resolvedTurn','interceptedTurn','loadedTurn','unloadedTurn'])if(f[field]!==undefined&&!int(f[field],0,s.turn))fail();
    if(f.target!==null&&!graph.has(f.target)||f.landing!=null&&(!s.tiles[f.landing]||!canEnter({...s,wars:s.kingdoms.filter(k=>k.id!==f.owner).map(k=>[k.id,f.owner].sort().join(':'))},f.owner,s.tiles[f.landing])))fail();
    if(f.sailingCarry!==undefined&&(!Number.isFinite(f.sailingCarry)||f.sailingCarry<0||f.sailingCarry>=1+1e-8))fail();
    if(f.attackTile!=null&&(!s.tiles[f.attackTile]||f.order!=='attack'))fail();
    if(f.escort!=null&&(typeof f.escort!=='string'||f.escort===f.id||f.escort.length>80))fail();
    let previous=f.node;for(const node of f.path){if(!graph.get(previous)?.includes(node))fail();previous=node;}
    if(f.path.length&&f.path.at(-1)!==f.target)fail();
    for(const a of f.cargo){
      if(!a||typeof a.id!=='string'||!/^army-\d+$/.test(a.id)||armies.has(a.id)||a.owner!==f.owner||a.tile!==f.tile||a.embarkedFleetId!==f.id||!Object.hasOwn(FORMATIONS,a.formation)||!int(a.retreats)||!Number.isFinite(a.morale)||a.morale<.1||a.morale>1||!a.units||Object.keys(a.units).length!==Object.keys(UNITS).length||Object.keys(UNITS).some(u=>!int(a.units[u]))||!Array.isArray(a.path)||a.path.length||a.target!==null||a.structureTarget||a.order!=='embarked'||a.embarkOrder||!troopCount(a))fail();
      armies.add(a.id);
    }
    for(const v of f.ships){const spec=SHIPS[v?.type];if(!Object.hasOwn(SHIPS,v?.type)||!spec||!int(v.hp,1,spec.hull)||!int(v.crew,1,spec.crew)||!Array.isArray(v.cargo))fail();unique(v.id,'ship');}
    if(cargoCount(f)>fleetCapacity(f))fail();
    const copy=structuredClone(f);distributeCargo(copy);
    if(JSON.stringify(f.ships.map(v=>v.cargo))!==JSON.stringify(copy.ships.map(v=>v.cargo)))fail();
  }
  for(const a of s.armies)if(a.order==='ranged'&&(!s.tiles[a.target]||a.path.length||a.structureTarget))fail();
  for(const a of s.armies){
    const o=a.embarkOrder;if(!o)continue;
    if(typeof o!=='object'||Array.isArray(o)||typeof o.fleet!=='string'||!/^fleet-\d+$/.test(o.fleet)||!int(o.count,1,100000))fail();
    if(!['hold','move'].includes(a.order)||a.structureTarget||a.path.length&&(a.order!=='move'||a.target!==o.embarkPosition)||!a.path.length&&a.target!==null)fail();
    if(o.type!==undefined&&o.type!=='board-transport'||o.status!==undefined&&!['active','paused'].includes(o.status))fail();
    if(o.embarkPosition!==undefined&&!s.tiles[o.embarkPosition]||o.reason!==undefined&&(typeof o.reason!=='string'||o.reason.length>300))fail();
    if(o.status==='paused'&&a.path.length||o.turnsUntilCompletion!=null&&!int(o.turnsUntilCompletion,0,s.width*s.height*2))fail();
    let previous=s.tiles[a.tile];for(const id of a.path){if(distance(previous,s.tiles[id])!==1)fail();previous=s.tiles[id];}
    if(a.boardingNotice!==undefined&&(typeof a.boardingNotice!=='string'||a.boardingNotice.length>300))fail();
  }
  if(s.armies.some(a=>a.embarkedFleetId||s.tiles[a.tile]?.terrain==='water')||armies.size>500||s.fleets.reduce((n,f)=>n+f.ships.length,0)+s.shipQueues.length>500)fail();
  for(const q of s.shipQueues){if(!q||!Object.hasOwn(SHIPS,q.type)||!house(q.owner)||!s.tiles[q.tile]||!int(q.remaining,0,SHIPS[q.type].turns)||q.total!==SHIPS[q.type].turns||!int(q.lastTickTurn,0,s.turn))fail();unique(q.id,'ship-order');}
}
