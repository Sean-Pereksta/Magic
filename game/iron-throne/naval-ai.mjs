import { planningView } from './ai-knowledge.mjs';
import { difficulty } from './difficulty.mjs';
import { atWar, build, buildCheck, canEnter, distance, orderArmy, settlements, sizeOf, splitArmy } from './core.mjs';
import { buildingLevel } from './economy.mjs';
import { navalGraph, navalPath, shoreNodes, nodeTile } from './naval-graph.mjs';
import { cargoCount, fleetCapacity } from './naval-state.mjs';
import { embarkArmy, orderFleet, queueShip, shipBuildCheck } from './naval.mjs';
import { recordStrategyAction } from './strategy.mjs';

// Every target and route is selected from this House's observation projection.
// Only normal command handlers receive the authoritative state to commit an order.
export function prepareNavalEconomy(world,owner) {
  const view=planningView(world,owner),k=view.kingdoms.find(k=>k.id===owner),d=difficulty(view);
  if(!k||view.turn<Math.ceil(5/d.expansion))return;
  const ports=settlements(view,owner).filter(t=>shoreNodes(view,t.id).length).sort((a,b)=>a.id.localeCompare(b.id));
  if(!ports.length)return;
  const yard=ports.find(t=>buildingLevel(t,'shipyard'));
  const construct=(tile,type)=>{if(buildCheck(view,owner,tile,type))return false;const result=build(world,owner,tile,type);if(result.ok)recordStrategyAction(world,owner,{kind:'build',tile,building:type,level:1});return result.ok;};
  if(!yard){construct(ports.find(t=>!t.project)?.id,'shipyard');return;}
  const fleets=view.fleets.filter(f=>f.owner===owner),queued=view.shipQueues.filter(q=>q.owner===owner),ships=[...fleets.flatMap(f=>f.ships),...queued];
  const army=view.armies.filter(a=>a.owner===owner&&!a.commandId).sort((a,b)=>sizeOf(b)-sizeOf(a))[0];
  const desired=Math.ceil(Math.min(100,army?sizeOf(army):25)/25);
  let type=!ships.some(v=>v.type==='warCanoe')?'warCanoe':ships.filter(v=>v.type==='transport').length<desired?'transport':ships.filter(v=>v.type==='warship').length<Math.ceil(d.expansion)?'warship':null;
  if(type&&queued.filter(q=>q.tile===yard.id).length<4&&!shipBuildCheck(view,owner,yard.id,type)){
    queueShip(world,owner,yard.id,type);return;
  }
  if(!Object.values(view.tiles).some(t=>t.owner===owner&&(t.building==='fishingDock'||t.project?.type==='fishingDock'))){
    const site=Object.values(view.tiles).filter(t=>t.owner===owner&&!t.building&&!t.project&&shoreNodes(view,t.id).length).sort((a,b)=>a.id.localeCompare(b.id)).find(t=>!buildCheck(view,owner,t.id,'fishingDock'));
    if(site)construct(site.id,'fishingDock');
  }
}
function landingPlan(view,f) {
  const graph=navalGraph(view),targets=settlements(view).filter(t=>atWar(view,f.owner,t.owner));
  const choices=Object.values(view.tiles).filter(t=>canEnter(view,f.owner,t)&&targets.some(c=>distance(c,t)<=4))
    .map(t=>({tile:t,score:Math.min(...targets.map(c=>distance(c,t)))*4+distance(view.tiles[f.tile],t)}))
    .sort((a,b)=>a.score-b.score||a.tile.id.localeCompare(b.tile.id));
  for(const {tile} of choices.slice(0,40)){
    const defenders=view.armies.filter(a=>!a.remembered&&a.tile===tile.id&&atWar(view,f.owner,a.owner)).reduce((n,a)=>n+sizeOf(a),0);
    if(defenders>cargoCount(f)/difficulty(view).preparation)continue;
    for(const node of shoreNodes(view,tile.id,graph)){const path=navalPath(view,f.node,node,{graph});if(path!==null)return {tile:tile.id,path};}
  }
  return null;
}
export function navalInvasionReady(view,owner,target) {
  const probe={...view,wars:[...view.wars,[owner,target].sort().join(':')]};
  for(const f of view.fleets.filter(f=>f.owner===owner&&fleetCapacity(f)>=25&&f.ships.some(v=>v.type!=='transport'))){
    const army=view.armies.find(a=>a.owner===owner&&!a.commandId&&sizeOf(a)>=25&&sizeOf(a)<=fleetCapacity(f)&&shoreNodes(view,a.tile).includes(f.node));
    if(army&&landingPlan(probe,{...f,cargo:[army]}))return true;
  }
  return false;
}
export function directNavalForces(world,owner) {
  let view=planningView(world,owner);
  for(const f of view.fleets.filter(f=>f.owner===owner)){
    if(f.path.length||f.resolvedTurn===view.turn)continue;
    if(cargoCount(f)){
      const plan=landingPlan(view,f);
      if(plan){orderFleet(world,owner,f.id,plan.tile,'unload');continue;}
      // Peace or a changed objective returns the army to a safe friendly shore.
      const home=settlements(view,owner).find(t=>shoreNodes(view,t.id).some(node=>navalPath(view,f.node,node)!==null));
      if(home)orderFleet(world,owner,f.id,home.id,'unload');
      continue;
    }
    const enemies=view.fleets.filter(e=>atWar(view,owner,e.owner));
    const fighters=f.ships.filter(v=>v.type!=='transport').length;
    const convoy=view.fleets.find(e=>e.owner===owner&&e.id!==f.id&&cargoCount(e)>0&&navalPath(view,f.node,e.node)!==null);
    if(fighters&&!fleetCapacity(f)&&convoy){orderFleet(world,owner,f.id,convoy.id,'escort');continue;}
    const enemy=enemies.find(e=>e.ships.length<=fighters*(1+difficulty(view).expansion)&&navalPath(view,f.node,e.node)!==null);
    if(enemy&&fighters){orderFleet(world,owner,f.id,enemy.tile,'attack');continue;}
    if(fleetCapacity(f)){
      const ports=settlements(view,owner).filter(t=>shoreNodes(view,t.id).includes(f.node));
      const port=ports[0];
      if(port){
        let army=view.armies.filter(a=>a.owner===owner&&!a.commandId).sort((a,b)=>Number(b.tile===port.id)-Number(a.tile===port.id)||sizeOf(b)-sizeOf(a))[0];
        if(army&&sizeOf(army)>100){const result=splitArmy(world,owner,army.id,'ai');if(result.ok){view=planningView(world,owner);army=view.armies.find(a=>a.id===result.armyId);}}
        const hasObjective=settlements(view).some(t=>atWar(view,owner,t.owner));
        if(army&&hasObjective){
          if(army.tile!==port.id){orderArmy(world,owner,army.id,port.id,'move',null,'ai');continue;}
          if(sizeOf(army)<=fleetCapacity(f)&&fighters){
            const probe={...f,cargo:[army]},plan=landingPlan(view,probe);
            if(plan&&embarkArmy(world,owner,army.id,f.id).ok){orderFleet(world,owner,f.id,plan.tile,'unload');continue;}
          }
          continue; // Wait for the full army's transports; never overload or strand part.
        }
      }
      else{
        const home=settlements(view,owner).find(t=>buildingLevel(t,'shipyard')&&shoreNodes(view,t.id).some(n=>navalPath(view,f.node,n)!==null));
        if(home){const node=shoreNodes(view,home.id).find(n=>navalPath(view,f.node,n)!==null);orderFleet(world,owner,f.id,nodeTile(node));continue;}
      }
    }
    const enemyPort=settlements(view).find(t=>atWar(view,owner,t.owner)&&shoreNodes(view,t.id).some(n=>navalPath(view,f.node,n)!==null));
    if(enemyPort&&f.ships.some(v=>v.type==='warship')){
      const node=shoreNodes(view,enemyPort.id).find(n=>navalPath(view,f.node,n)!==null);orderFleet(world,owner,f.id,nodeTile(node),'blockade');continue;
    }
    if(f.ships.some(v=>v.type==='warship')&&view.kingdoms.some(k=>atWar(view,owner,k.id))){orderFleet(world,owner,f.id,f.tile,'intercept');continue;}
    // Canoes scout connected water at the frontier of actual House knowledge.
    if(f.ships.every(v=>v.type==='warCanoe')){
      const graph=navalGraph(view),targets=[...graph.keys()].filter(n=>n!==f.node&&view.tiles[nodeTile(n)].fog!=='visible')
        .sort((a,b)=>distance(view.tiles[f.tile],view.tiles[nodeTile(a)])-distance(view.tiles[f.tile],view.tiles[nodeTile(b)])||a.localeCompare(b));
      const target=targets.slice(0,40).find(n=>navalPath(view,f.node,n,{graph})!==null);
      if(target)orderFleet(world,owner,f.id,nodeTile(target));
    }
  }
}
