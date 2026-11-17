import {TYPES} from './engine.js';
import {aircraftTerrainHeight,terrainHeight} from './terrain.js';

export function actorTerrainHeight(map,e){
 return TYPES[e.type]?.flying?aircraftTerrainHeight(map,e.x,e.y,e.r):terrainHeight(map||{},e.x,e.y);
}

function diveProgress(e){
 const remaining=e.crash.remaining??.65,total=e.crash.total??.65;
 return Math.max(0,Math.min(1,remaining/total));
}

export function actorLift(e,time){
 if(e.leap)return Math.sin(Math.PI*(1-e.leap.remaining/e.leap.total))*75;
 if(TYPES[e.type]?.flying){
  const hover=(e.type==='airplane'?32:21)+Math.sin(time*3+(Number(e.id)||0))*2;
  return e.crash?.diving?hover*diveProgress(e):hover;
 }
 if(e.type==='divider')return 8+Math.sin(time*3+(Number(e.id)||0))*2;
 return 0;
}
