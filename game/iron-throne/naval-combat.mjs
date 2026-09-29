import { atWar, random, log, distance } from './core.mjs';
import { inflict } from './warfare.mjs';
import { SHIPS, cargoCount, fleetCapacity, syncCargo, troopCount, fleetAttackRange } from './naval-state.mjs';
import { clearShot } from './structures.mjs';
import { navalGraph, nodeTile } from './naval-graph.mjs';

export function navalPower(f,range=1) {
  const command=Math.max(0,...(f.cargo||[]).map(a=>a.commandBonus||0));
  return f.ships.filter(v=>range<=1||v.type==='warship').reduce((n,v)=>n+SHIPS[v.type].attack*(.35+.65*v.hp/SHIPS[v.type].hull)*(.4+.6*v.crew/SHIPS[v.type].crew)*(f.node.startsWith('river:')&&v.type==='warCanoe'?1.3:1),0)*(f.morale||1)*(1+command);
}
function troopLoss(f,n) {
  for(const a of f.cargo){const loss=Math.min(n,troopCount(a));inflict(a,loss);n-=loss;if(n<=0)break;}
}
export function sinkShips(s,f) {
  // Every troop on a destroyed transport is lost, using its pre-clash manifest.
  const sunk=f.ships.filter(v=>v.hp<=0||v.crew<=0);
  for(const ship of sunk)for(const load of ship.cargo||[]){const a=f.cargo.find(a=>a.id===load.armyId);if(a)inflict(a,Math.min(troopCount(a),load.count));}
  f.ships=f.ships.filter(v=>!sunk.includes(v));
  troopLoss(f,Math.max(0,cargoCount(f)-fleetCapacity(f)));syncCargo(f);
  if(!f.ships.length)s.fleets=s.fleets.filter(x=>x!==f);
  return sunk.length;
}
export function damageFleet(f,amount) {
  // Escorts absorb the first volley, then damage spills through to transports.
  const targets=[...f.ships].sort((a,b)=>Number(a.type==='transport')-Number(b.type==='transport')||a.id.localeCompare(b.id));
  for(const ship of targets){if(amount<=0)break;const hit=Math.min(ship.hp,Math.ceil(amount));ship.hp-=hit;ship.crew=Math.max(0,ship.crew-Math.floor(hit/SHIPS[ship.type].hull*SHIPS[ship.type].crew*.5));amount-=hit;}
}
export function resolveNavalCombat(s,attacker,defender,{contact=true}={}) {
  if(!attacker||!defender||!atWar(s,attacker.owner,defender.owner))return {ok:false,error:'Naval attacks require war.'};
  const range=distance(s.tiles[attacker.tile],s.tiles[defender.tile]);
  if(range>fleetAttackRange(attacker)||!clearShot(s,s.tiles[attacker.tile],s.tiles[defender.tile])||range<=1&&attacker.node!==defender.node&&!navalGraph(s).get(attacker.node)?.includes(defender.node))return {ok:false,error:'Target is outside weapon range or line of fire.'};
  const clashTile=defender.tile;
  const houseTroops=owner=>s.fleets.filter(f=>f.owner===owner).reduce((n,f)=>n+cargoCount(f),0);
  const ownerBefore=[houseTroops(attacker.owner),houseTroops(defender.owner)];
  const before=[cargoCount(attacker),cargoCount(defender)],vessels=[attacker.ships.length,defender.ships.length];
  const powers=[navalPower(attacker,range),range<=fleetAttackRange(defender)?navalPower(defender,range):0];
  damageFleet(defender,powers[0]*(.85+random(s)*.3));damageFleet(attacker,powers[1]*(.85+random(s)*.3));
  const sunk=[sinkShips(s,attacker),sinkShips(s,defender)];
  const boarding=contact&&range<=1&&attacker.ships.length>0&&defender.ships.length>0&&(cargoCount(attacker)>0||cargoCount(defender)>0);
  if(boarding){
    const power=f=>f.ships.reduce((n,v)=>n+v.crew,0)+(f.cargo||[]).reduce((n,a)=>n+troopCount(a)*a.morale*(1+(a.commandBonus||0)),0);
    const hits=[Math.max(1,Math.ceil(power(attacker)*.18)),Math.max(1,Math.ceil(power(defender)*.18))];
    [defender,attacker].forEach((f,i)=>{const troops=Math.min(cargoCount(f),hits[i]);troopLoss(f,troops);let left=hits[i]-troops;for(const ship of f.ships){const n=Math.min(left,ship.crew);ship.crew-=n;left-=n;}syncCargo(f);sunk[1-i]+=sinkShips(s,f);});
  }
  let retreat=null,retreatOwner=null;
  if(contact&&range<=1&&attacker.ships.length&&defender.ships.length){
    const loser=navalPower(attacker)>=navalPower(defender)?defender:attacker;
    loser.morale=Math.max(.2,(loser.morale||1)-.12);
    const graph=navalGraph(s),escape=graph.get(loser.node)?.find(node=>node!==attacker.node&&node!==defender.node&&!s.fleets.some(f=>f.node===node&&atWar(s,f.owner,loser.owner)));
    if(escape){loser.node=escape;loser.tile=nodeTile(escape);syncCargo(loser);retreat=loser.tile;retreatOwner=loser.owner;}
    loser.path=[];loser.target=null;loser.order='hold';loser.landing=null;loser.attackTile=null;loser.resolvedTurn=s.turn;
  }
  const winner=!defender.ships.length||attacker.ships.length&&navalPower(attacker)>=navalPower(defender)?attacker.owner:defender.owner;
  const losses=[ownerBefore[0]-houseTroops(attacker.owner),ownerBefore[1]-houseTroops(defender.owner)];
  const event={id:s.nextId++,turn:s.turn,attacker:attacker.owner,defender:defender.owner,tile:clashTile,action:'naval',ranged:range>1,winner,before,after:before.map((n,i)=>n-losses[i]),troopLosses:losses,vessels,sunk,boarding,retreat,retreatOwner};
  s.militaryEvents.push(event);s.militaryEvents=s.militaryEvents.slice(-100);
  log(s,`Naval clash${boarding?' and boarding battle':''}: ${sunk[0]+sunk[1]} vessels lost, ${event.troopLosses.reduce((a,b)=>a+b,0)} embarked troops lost.`,'battle',{audience:[attacker.owner,defender.owner]});
  return {ok:true,...event};
}
