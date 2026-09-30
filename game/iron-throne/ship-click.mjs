import { atWar } from './core.mjs';
import { landNavalRange } from './naval-ranged.mjs';
import { boardingCount } from './naval.mjs';

// Resolve clicks from the player's observation view, never hidden fleet state.
export function armyShipClick(view,owner,armyId,tile,mode=null) {
  const army=view.armies.find(a=>a.id===armyId&&a.owner===owner);
  if(!army)return null;
  const fleets=(view.fleets||[]).filter(f=>f.tile===tile);
  if(fleets.some(f=>atWar(view,owner,f.owner))&&(landNavalRange(army)>0||mode))return {type:'order',args:{army:armyId,tile,order:'ranged'}};
  if(mode==='move'){
    const transports=fleets.filter(f=>f.owner===owner&&f.ships.some(v=>v.type==='transport'));
    const fleet=transports.find(f=>boardingCount(view,army,f)>0)||transports[0];
    if(fleet)return {type:'fleetEmbark',args:{army:armyId,fleet:fleet.id}};
  }
  return null;
}
