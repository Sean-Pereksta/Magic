export const SAILING_SPEED = 3 * 2.5; // Standard infantry: 3 open-ground hexes per turn.
// Ships occupy a separate navigation layer; cargo armies have exactly one owner.
export const SHIPS = Object.freeze({
  warCanoe: {name:'War Canoe',cost:{wood:22,food:12},crew:4,turns:1,capacity:0,hull:45,attack:13,speed:SAILING_SPEED,riverSpeed:SAILING_SPEED,vision:5},
  transport: {name:'Transport',cost:{wood:35},crew:6,turns:1,capacity:25,hull:70,attack:3,speed:SAILING_SPEED,riverSpeed:SAILING_SPEED,vision:2},
  warship: {name:'Warship',cost:{wood:55,iron:24,arms:12},crew:10,turns:2,capacity:0,hull:150,attack:38,speed:SAILING_SPEED,riverSpeed:SAILING_SPEED,vision:5}
});
export const troopCount = a => Object.values(a.units).reduce((n,v)=>n+v,0);
export const cargoCount = f => (f.cargo||[]).reduce((n,a)=>n+troopCount(a),0);
export const fleetCapacity = f => f.ships.reduce((n,v)=>n+(SHIPS[v.type]?.capacity||0),0);
export const fleetVision = f => Math.max(0,...f.ships.map(v=>SHIPS[v.type]?.vision||0));
export const fleetAttackRange = f => f.ships.some(v=>v.type==='warship')?2:1;
export const fleetSpeed = (f,river=false) => Math.min(...f.ships.map(v=>SHIPS[v.type][river?'riverSpeed':'speed']));
export const embarkedArmies = s => (s.fleets||[]).flatMap(f=>f.cargo||[]);
export const allArmies = s => [...s.armies,...embarkedArmies(s)];
export function initializeNaval(s) { s.fleets??=[];s.shipQueues??=[];s.navalVersion??=1; }
export function distributeCargo(f) {
  const pending=(f.cargo||[]).map(a=>({armyId:a.id,count:troopCount(a)}));
  for(const ship of f.ships){
    ship.cargo=[];let free=SHIPS[ship.type].capacity;
    while(free&&pending.length){const a=pending[0],n=Math.min(free,a.count);if(n)ship.cargo.push({armyId:a.armyId,count:n});free-=n;a.count-=n;if(!a.count)pending.shift();}
  }
  return !pending.length;
}
export function syncCargo(f) {
  f.cargo=f.cargo.filter(a=>troopCount(a)>0);
  for(const a of f.cargo){a.tile=f.tile;a.embarkedFleetId=f.id;a.path=[];a.target=null;a.structureTarget=null;a.order='embarked';}
  distributeCargo(f);
}
export function publicFleet(f) {
  return {id:f.id,owner:f.owner,tile:f.tile,node:f.node,ships:f.ships.map(v=>({type:v.type})),cargoUnknown:true,order:'observed'};
}
