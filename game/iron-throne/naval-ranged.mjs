import { UNITS } from './data.mjs';
import { atWar, distance, log } from './core.mjs';
import { visionTiles, refreshKnowledge } from './fog.mjs';
import { armySpeed, inflict, protection } from './warfare.mjs';
import { clearShot, damageStructure, structuresAt } from './structures.mjs';
import { navalPower, damageFleet, sinkShips } from './naval-combat.mjs';
import { cargoCount, troopCount } from './naval-state.mjs';
import { markPlayerOverride } from './command-state.mjs';

export const unitNavalRange = u => u.ranged>0?Math.max(2,u.bombardRange||0):u.legacy&&u.bombardRange>1?u.bombardRange:0;
export const landNavalRange = a => Math.max(0,...Object.entries(a.units).filter(([,n])=>n>0).map(([u])=>unitNavalRange(UNITS[u])));
const landPower=(a,range)=>Object.entries(a.units).reduce((n,[id,count])=>n+(unitNavalRange(UNITS[id])>0&&unitNavalRange(UNITS[id])>=range?count*((UNITS[id].ranged||0)*.8+(UNITS[id].breach||0)*1.5):0),0)*a.morale;
export const coastalTarget = (s,owner,tile) => s.armies.some(a=>a.tile===tile&&troopCount(a)>0&&atWar(s,owner,a.owner))||s.tiles[tile]?.owner&&atWar(s,owner,s.tiles[tile].owner)&&structuresAt(s.tiles[tile]).length>0;
export function landNavalAttackCheck(s,owner,a,tile) {
  if(s.outcome||!a||a.owner!==owner)return 'Select your land army.';
  if(a.resolvedTurn===s.turn)return 'This army has already acted this turn.';
  if(!visionTiles(s,owner).has(tile)||!s.fleets.some(f=>f.tile===tile&&atWar(s,owner,f.owner)))return 'Select a visible enemy fleet at war with your House.';
  const range=landNavalRange(a);
  if(!range)return 'Archers or ranged siege troops are required to fire on ships.';
  if(distance(s.tiles[a.tile],s.tiles[tile])>range)return `Outside ranged attack distance (${range} hexes).`;
  if(!clearShot(s,s.tiles[a.tile],s.tiles[tile]))return 'Mountains block the line of fire.';
  return null;
}
export function orderLandNavalAttack(s,owner,a,tile) {
  const error=landNavalAttackCheck(s,owner,a,tile);if(error)return {ok:false,error};
  delete a.embarkOrder;a.path=[];a.target=tile;a.structureTarget=null;a.order='ranged';markPlayerOverride(s,a);return {ok:true};
}
function record(s,f,a,before,sunk,attacker,defender,tile) {
  const after=[cargoCount(f),troopCount(a)],loss=before.map((n,i)=>n-after[i]);
  const navalAttacker=attacker===f.owner,orderedBefore=navalAttacker?before:[...before].reverse(),orderedAfter=navalAttacker?after:[...after].reverse(),orderedLoss=navalAttacker?loss:[...loss].reverse();
  s.militaryEvents.push({id:s.nextId++,turn:s.turn,attacker,defender,tile,action:'naval',ranged:true,before:orderedBefore,after:orderedAfter,troopLosses:orderedLoss,sunk:navalAttacker?[sunk,0]:[0,sunk],boarding:false,phases:[{name:'Coastal ranged fire',loss:orderedLoss,notes:['Ships remain on water. No boarding or land capture.']}]});
  s.militaryEvents=s.militaryEvents.slice(-100);
  log(s,`Coastal ranged fire: ${sunk} vessels sunk, ${loss[0]} embarked troops and ${loss[1]} land troops lost.`,'battle',{audience:[attacker,defender]});
  s.armies=s.armies.filter(a=>troopCount(a)>0);refreshKnowledge(s);
}
export function resolveLandNavalAttack(s,a) {
  // The caller has consumed this army's activation; recheck visibility/range now.
  const error=landNavalAttackCheck(s,a.owner,{...a,resolvedTurn:0},a.target);
  if(error){a.order='hold';a.target=null;return false;}
  const f=s.fleets.find(f=>f.tile===a.target&&atWar(s,a.owner,f.owner)),range=distance(s.tiles[a.tile],s.tiles[f.tile]);
  const before=[cargoCount(f),troopCount(a)],counter=range<=2?navalPower({...f,ships:f.ships.filter(v=>v.type==='warship')},2)*.35/protection(s.tiles[a.tile]):0;
  damageFleet(f,landPower(a,range));inflict(a,counter);const sunk=sinkShips(s,f);
  a.movementSpent=armySpeed(a);a.order='hold';a.target=null;
  record(s,f,a,before,sunk,a.owner,f.owner,f.tile);return true;
}
export function resolveCoastalAttack(s,f,tile) {
  const t=s.tiles[tile];if(!t||!visionTiles(s,f.owner).has(tile)||distance(s.tiles[f.tile],t)>2||!clearShot(s,s.tiles[f.tile],t)||!f.ships.some(v=>v.type==='warship')||!coastalTarget(s,f.owner,tile))return false;
  const a=s.armies.find(a=>a.tile===tile&&atWar(s,f.owner,a.owner)&&troopCount(a)>0);
  const range=distance(s.tiles[f.tile],t),power=navalPower({...f,ships:f.ships.filter(v=>v.type==='warship')},2);
  if(a){const before=[cargoCount(f),troopCount(a)],counter=landPower(a,range);inflict(a,power*.35/protection(t));damageFleet(f,counter);const sunk=sinkShips(s,f);record(s,f,a,before,sunk,f.owner,a.owner,tile);}
  else{
    const type=structuresAt(t).find(id=>id==='wall')||structuresAt(t).find(id=>id!=='road')||'road';
    const proxy={id:f.id,owner:f.owner,tile:f.tile,units:{catapult:Math.max(1,Math.floor(power/19))},morale:f.morale};
    damageStructure(s,proxy,t,type,'bombard');refreshKnowledge(s);
  }
  return true;
}
