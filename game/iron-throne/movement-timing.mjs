import { armySpeed } from './warfare.mjs';
import { atWar, canEnter, distance, moveCost, zoneOfControl } from './core.mjs';

// Shared with the real resolver, including its one costly first edge allowance.
export const movementBudget = (s,a) => Math.max(0,armySpeed(a)-(a.movementTurn===s.turn?a.movementSpent||0:0));
export const canSpendMovement = (cost,budget,startingBudget) => budget>0&&(cost<=budget||budget===startingBudget);

export function projectArmyStep(s,a,path) {
  const start=movementBudget(s,a);let budget=start,tile=a.tile,index=0;
  while(index<path.length){
    const from=s.tiles[tile],to=s.tiles[path[index]];
    if(!to||!canEnter(s,a.owner,to)||distance(from,to)!==1)break;
    const cost=moveCost(from,to);if(!canSpendMovement(cost,budget,start))break;
    if(s.armies.some(e=>e.tile===to.id&&atWar(s,a.owner,e.owner)))break;
    tile=to.id;index++;budget-=cost;
    if(zoneOfControl(s,a,to))break;
  }
  return {tile,path:path.slice(index)};
}

// Returns the resolution offset: this resolution = 0, the following one = 1.
// Call on the observation view so timing cannot reveal hidden troops or terrain.
export function armyArrivalTurns(s,a,path=a.path||[]) {
  let turns=a.resolvedTurn===s.turn?1:0,from=s.tiles[a.tile];
  let start=a.resolvedTurn===s.turn?armySpeed(a):movementBudget(s,a),budget=start;
  for(const id of path){
    const to=s.tiles[id];
    const exiting=from?.owner&&!canEnter(s,a.owner,from)&&s.tiles[a.target]?.owner!==from.owner&&to?.owner===from.owner;
    if(!from||!to||distance(from,to)!==1||!canEnter(s,a.owner,to)&&!exiting)return null;
    const cost=moveCost(from,to);
    if(!canSpendMovement(cost,budget,start)){turns++;start=budget=armySpeed(a);}
    // Attack indicators mean arrival at first contact, not a forecast battle win.
    if(s.armies.some(e=>e.tile===id&&atWar(s,a.owner,e.owner)))return turns;
    budget-=cost;from=to;
    if(id!==path.at(-1)&&zoneOfControl(s,a,to)){turns++;start=budget=armySpeed(a);}
  }
  return turns;
}
