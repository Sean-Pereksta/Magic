import { FORMATIONS, UNITS } from './data.mjs';
import { atWar, canAfford, canEnter, kingdom, pay, resolveAmphibiousLanding } from './core.mjs';
import { buildingLevel } from './economy.mjs';
import { knowledgeView, refreshKnowledge } from './fog.mjs';
import { SHIPS, initializeNaval, cargoCount, fleetCapacity, fleetSpeed, syncCargo, distributeCargo, troopCount } from './naval-state.mjs';
import { navalGraph, navalNode, navalPath, nodeTile, shoreNodes, adjacentShore } from './naval-graph.mjs';
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
export function embarkCheck(s,owner,armyId,fleetId) {
  const error=actionError(s,owner);if(error)return error;
  const a=s.armies.find(a=>a.id===armyId&&a.owner===owner),f=owned(s,owner,fleetId);
  if(!a||!f)return 'Select your land army and a friendly fleet.';
  if(a.embarkedFleetId)return 'This army is already embarked.';
  if(!adjacentShore(s,f,a.tile)||!canEnter(s,owner,s.tiles[a.tile])||s.tiles[a.tile].owner&&atWar(s,owner,s.tiles[a.tile].owner))return 'Embark from an adjacent friendly or unclaimed shoreline.';
  if(s.armies.some(e=>e.tile===a.tile&&atWar(s,owner,e.owner)))return 'Clear enemy troops from the embarkation shore first.';
  if(a.resolvedTurn===s.turn||f.resolvedTurn===s.turn)return 'This force has already acted this turn.';
  const need=troopCount(a)+cargoCount(f);
  if(need>fleetCapacity(f))return `${need} troops require ${Math.ceil(need/25)} Transports. Current capacity: ${fleetCapacity(f)}.`;
  return null;
}
export function embarkArmy(s,owner,armyId,fleetId) {
  const error=embarkCheck(s,owner,armyId,fleetId);if(error)return fail(error);
  const a=s.armies.find(a=>a.id===armyId),f=owned(s,owner,fleetId);
  s.armies=s.armies.filter(x=>x!==a);a.path=[];a.target=null;a.structureTarget=null;a.order='embarked';a.resolvedTurn=s.turn;
  f.cargo.push(a);syncCargo(f);refreshKnowledge(s);return {ok:true};
}
export function orderFleet(s,owner,id,target,order='move') {
  const error=actionError(s,owner);if(error)return fail(error);
  const f=owned(s,owner,id);if(!f||!['move','attack','unload','escort','intercept','blockade','hold'].includes(order))return fail('Select one of your fleets and a valid order.');
  if(f.resolvedTurn===s.turn)return fail('This fleet has already acted this turn.');
  if(order==='hold'){Object.assign(f,{path:[],target:null,order,landing:null,escort:null});return {ok:true};}
  if(order==='blockade'&&!f.ships.some(v=>v.type==='warship'))return fail('A blockade requires a Warship.');
  const view=knowledgeView(s,owner),graph=navalGraph(view);let landing=null,escort=null,ends=[];
  if(order==='escort'){
    const leader=view.fleets.find(x=>x.id===target&&x.owner===owner&&x.id!==id&&x.order!=='escort');
    if(!leader)return fail('Choose another friendly fleet to escort.');
    escort=leader.id;ends=[leader.node];
  }else if(order==='unload'){
    if(!cargoCount(f))return fail('This fleet has no embarked troops.');
    if(!canEnter(view,owner,view.tiles[target]))return fail('Choose a legal land destination; neutral borders require access or war.');
    landing=target;ends=shoreNodes(view,target,graph);
  }else{
    const node=navalNode(view,target,graph);if(node)ends=[node];
    if(order==='attack'&&!view.fleets.some(e=>e.node===node&&atWar(view,owner,e.owner)))return fail('Select a visible enemy fleet at war with your House.');
  }
  const routes=ends.map(end=>({end,path:navalPath(view,f.node,end,{graph})})).filter(x=>x.path!==null).sort((a,b)=>a.path.length-b.path.length||a.end.localeCompare(b.end));
  if(!routes.length)return fail('No connected water route is known. Ships can only sail on ocean or connected rivers; scout farther first.');
  Object.assign(f,{path:routes[0].path,target:routes[0].end,order,landing,escort});return {ok:true};
}
export function mergeFleets(s,owner,id,otherId) {
  const error=actionError(s,owner);if(error)return fail(error);
  const f=owned(s,owner,id),other=owned(s,owner,otherId);
  if(!f||!other||f===other||f.node!==other.node||f.ships.length+other.ships.length>100)return fail('Bring two friendly fleets to the same water position (maximum 100 vessels).');
  if(f.resolvedTurn===s.turn||other.resolvedTurn===s.turn)return fail('A fleet has already acted this turn.');
  f.movementSpent=Math.max(f.movementTurn===s.turn?f.movementSpent:0,other.movementTurn===s.turn?other.movementSpent:0);f.movementTurn=s.turn;
  f.ships.push(...other.ships);f.cargo.push(...other.cargo);s.fleets=s.fleets.filter(x=>x!==other);Object.assign(f,{path:[],target:null,order:'hold',landing:null,escort:null});syncCargo(f);return {ok:true};
}
export function resolveFleetMovement(s,owner=null) {
  initializeNaval(s);const graph=navalGraph(s);
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
    let fought=false;
    while(f.path.length&&f.movementSpent<1){
      const next=f.path[0];if(!graph.get(f.node)?.includes(next)){f.path=[];break;}
      const cost=1/fleetSpeed(f,next.startsWith('river:'));
      if(f.movementSpent+cost>1+1e-8)break;
      // A guarding fleet may intercept at the connected next position, never across land.
      const enemies=s.fleets.filter(e=>e!==f&&atWar(s,f.owner,e.owner)&&(e.node===next||['intercept','blockade','escort'].includes(e.order)&&graph.get(next)?.includes(e.node)&&e.interceptedTurn!==s.turn));
      const enemy=enemies.sort((a,b)=>Number(a.node!==next)-Number(b.node!==next)||a.id.localeCompare(b.id))[0];
      if(enemy){enemy.interceptedTurn=s.turn;
        if(enemy.node!==next&&!graph.get(f.node)?.includes(enemy.node)){f.node=next;f.tile=nodeTile(next);f.path.shift();syncCargo(f);}
        const origin=f.node;resolveNavalCombat(s,f,enemy,{contact:true});f.movementSpent=1;fought=true;
        if(s.fleets.includes(f)&&f.node===origin&&f.node!==next&&!s.fleets.some(e=>e.node===next&&atWar(s,f.owner,e.owner))){f.node=next;f.tile=nodeTile(next);f.path.shift();syncCargo(f);}break;}
      f.node=next;f.tile=nodeTile(next);f.path.shift();f.movementSpent+=cost;syncCargo(f);refreshKnowledge(s);
    }
    if(!s.fleets.includes(f))continue;
    // Same-position contact can occur on imported games or at a river meeting.
    const enemy=s.fleets.find(e=>e!==f&&e.node===f.node&&atWar(s,e.owner,f.owner));
    if(enemy&&!fought){resolveNavalCombat(s,f,enemy,{contact:true});f.movementSpent=1;fought=true;}
    if(!s.fleets.includes(f))continue;
    if(f.order==='unload'&&f.landing&&!f.path.length&&!fought&&adjacentShore(s,f,f.landing)&&canEnter(s,f.owner,s.tiles[f.landing])){
      for(const a of [...f.cargo]){
        if(resolveAmphibiousLanding(s,a,f.landing)){f.cargo=f.cargo.filter(x=>x!==a);delete a.embarkedFleetId;s.armies.push(a);}
      }
      syncCargo(f);f.movementSpent=1;f.landing=null;f.order='hold';f.target=null;
    }
    if(!f.path.length&&['move','attack'].includes(f.order)){f.order='hold';f.target=null;}
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
    for(const field of ['resolvedTurn','interceptedTurn'])if(f[field]!==undefined&&!int(f[field],0,s.turn))fail();
    if(f.target!==null&&!graph.has(f.target)||f.landing!=null&&(!s.tiles[f.landing]||!canEnter({...s,wars:s.kingdoms.filter(k=>k.id!==f.owner).map(k=>[k.id,f.owner].sort().join(':'))},f.owner,s.tiles[f.landing])))fail();
    if(f.escort!=null&&(typeof f.escort!=='string'||f.escort===f.id||f.escort.length>80))fail();
    let previous=f.node;for(const node of f.path){if(!graph.get(previous)?.includes(node))fail();previous=node;}
    if(f.path.length&&f.path.at(-1)!==f.target)fail();
    for(const a of f.cargo){
      if(!a||typeof a.id!=='string'||!/^army-\d+$/.test(a.id)||armies.has(a.id)||a.owner!==f.owner||a.tile!==f.tile||a.embarkedFleetId!==f.id||!Object.hasOwn(FORMATIONS,a.formation)||!int(a.retreats)||!Number.isFinite(a.morale)||a.morale<.1||a.morale>1||!a.units||Object.keys(a.units).length!==Object.keys(UNITS).length||Object.keys(UNITS).some(u=>!int(a.units[u]))||!Array.isArray(a.path)||a.path.length||a.target!==null||a.structureTarget||a.order!=='embarked'||!troopCount(a))fail();
      armies.add(a.id);
    }
    for(const v of f.ships){const spec=SHIPS[v?.type];if(!Object.hasOwn(SHIPS,v?.type)||!spec||!int(v.hp,1,spec.hull)||!int(v.crew,1,spec.crew)||!Array.isArray(v.cargo))fail();unique(v.id,'ship');}
    if(cargoCount(f)>fleetCapacity(f))fail();
    const copy=structuredClone(f);distributeCargo(copy);
    if(JSON.stringify(f.ships.map(v=>v.cargo))!==JSON.stringify(copy.ships.map(v=>v.cargo)))fail();
  }
  if(s.armies.some(a=>a.embarkedFleetId||s.tiles[a.tile]?.terrain==='water')||armies.size>500||s.fleets.reduce((n,f)=>n+f.ships.length,0)+s.shipQueues.length>500)fail();
  for(const q of s.shipQueues){if(!q||!Object.hasOwn(SHIPS,q.type)||!house(q.owner)||!s.tiles[q.tile]||!int(q.remaining,0,SHIPS[q.type].turns)||q.total!==SHIPS[q.type].turns||!int(q.lastTickTurn,0,s.turn))fail();unique(q.id,'ship-order');}
}
