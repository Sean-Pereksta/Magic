import { generateWorld } from './world-generation.mjs';
import { initializeFounding, validateFoundingSave, STARTING_RADIUS } from './founding.mjs';
import { MAP_PROFILES } from './map-profiles.mjs';
import { calculatePopulationChange, populationCapacity } from './population.mjs';
export { populationCapacity } from './population.mjs';
import { isHumanHouse } from './house-control.mjs';
import { initializePlans, validatePlans } from './plans.mjs';
import { initializeEspionage, spyUpkeep, validateEspionage } from './espionage.mjs';
import { updateAttitudes, validatePolitics } from './politics.mjs';
import { damageStructure, structureAttackCheck, structuresAt, validateStructures } from './structures.mjs';
import { BUILDINGS, HOUSES, INTENT_TYPES, RESOURCES, SAVE_VERSION, TERRAINS, UNITS } from './data.mjs';

import { changeRelation, initializeLiving, recordPoliticalMemory, tradeBlocked, validateLivingSave } from './living.mjs';

import { buildingLevel, buildingSpec, cityOrderBonus, completeConstruction, constructionSpec, emptyUnits, fortMaximum, migrateEconomy, productionPlan, storageCapacity, validateExpansion, wallMaximum } from './economy.mjs';
import { armySpeed, familyCount, inflict, resolveFieldBattle, siegeStep } from './warfare.mjs';

import { initializeStrategy, recordStrategyAction, runStrategyTurn, validateStrategySave } from './strategy.mjs';

export const PLAYER = 'ashen';
import { tileId, distance, neighbors, passable } from './world-hex.mjs';
export { tileId, distance, neighbors, passable } from './world-hex.mjs';
export const getTile = (s, id) => s.tiles[id];
export const kingdom = (s, id) => s.kingdoms.find(k => k.id === id);
export const settlements = (s, owner) => Object.values(s.tiles).filter(t => ['city', 'town'].includes(t.building) && (!owner || t.owner === owner));
export const armiesOf = (s, owner) => s.armies.filter(a => a.owner === owner);
export const sizeOf = a => Object.values(a.units).reduce((sum, n) => sum + n, 0);
export const alive = (s, id) => settlements(s, id).length > 0;
export const pair = (a, b) => [a, b].sort().join(':');
export const atWar = (s, a, b) => a !== b && s.wars.includes(pair(a, b));
export const treaty = (s, a, b, type) => s.treaties.find(t => t.parties.includes(a) && t.parties.includes(b) && (!type || t.type === type) && t.expires > s.turn);
export const canAfford = (k, cost) => Object.entries(cost).every(([r, n]) => Number.isFinite(n) && n >= 0 && k.resources[r] >= n);
export function pay(k, cost, sign = -1) { for (const [r, n] of Object.entries(cost)) k.resources[r] += sign * n; }
export function log(s, message, kind = 'world') { s.events.unshift({ turn: s.turn, message, kind }); s.events = s.events.slice(0, 100); }
export function random(s) { let x = s.rng >>> 0; x ^= x << 13; x ^= x >>> 17; x ^= x << 5; s.rng = x >>> 0; return s.rng / 4294967296; }
export function relation(s, a, b) { return kingdom(s, a).relations[b]; }
export function remember(s, owner, text, importance = 5) {
  const k = kingdom(s, owner);
  k.memories.push({ turn: s.turn, text, importance });
  if (k.memories.length > 30) {
    const old = k.memories.splice(0, 10).sort((a, b) => b.importance - a.importance);
    k.memorySummary = `${k.memorySummary} ${old.slice(0, 3).map(m => `T${m.turn}: ${m.text}`).join(' ')}`.slice(-900);
  }
}
export function shiftRelation(s, a, b, opinion, trust = 0, reason = 'Diplomatic relations changed.') {
  changeRelation(s, a, b, { opinion, trust }, reason);
}
export function declareWar(s, a, b) {
  if (!alive(s, a) || !alive(s, b) || a === b || atWar(s, a, b)) return false;
  const broken = s.treaties.filter(t => t.parties.includes(a) && t.parties.includes(b));
  s.treaties = s.treaties.filter(t => !broken.includes(t));
  if (broken.length) {
    for (const k of s.kingdoms.filter(k => k.id !== a)) changeRelation(s, k.id, a, { opinion: -12, trust: -20, grievance: 20, aggression: 15 }, 'A signed treaty was broken by a declaration of war.');
    remember(s, b, `${kingdom(s, a).name} broke our treaty.`, 10);
  }
  s.diplomacy.warHistory.push({ id: s.nextId++, turn: s.turn, attacker: a, defender: b });
  s.diplomacy.warHistory = s.diplomacy.warHistory.slice(-80);
  s.wars.push(pair(a, b)); changeRelation(s, b, a, { opinion: -30, trust: -20, grievance: 25, aggression: 20 }, 'War declared against our House.');
  recordPoliticalMemory(s, b, a, 'war', `${kingdom(s, a).name} declared war on us.`, 9);
  for (const k of s.kingdoms.filter(k => ![a, b].includes(k.id) && treaty(s, k.id, b, 'alliance'))) changeRelation(s, k.id, a, { opinion: -20, trust: -12, grievance: 20 }, `Attacked our ally, ${kingdom(s, b).name}.`);
  for (const p of s.pledges.filter(p => p.status === 'pending' && p.debtor === a && p.creditor === b)) p.breached = true;
  log(s, `${kingdom(s, a).name} declares war on ${kingdom(s, b).name}.`, 'war');
  return true;
}
export function makePeace(s, a, b) {
  s.wars = s.wars.filter(w => w !== pair(a, b));
  for (const army of s.armies) if ((army.owner === a || army.owner === b) && army.path.length) army.path = [];
}

export function createGame(seed = 8147, preset = 'random') {
  seed = Number(seed) >>> 0 || 8147;
  const s = { version: SAVE_VERSION, width: 40, height: 30, seed, rng: seed, preset, turn: 0, nextId: 1, tiles: {}, kingdoms: [], armies: [], treaties: [], wars: [], pledges: [], events: [], militaryEvents: [], conversations: {}, diplomaticTurns: 0, outcome: null };
  for (const house of HOUSES) {
    const k = { ...house, resources: { food: 140, wood: 110, stone: 90, iron: 50, gold: 200, horses: 12, tools: 0, arms: 0 }, population: 80, happiness: 70, tax: 'medium', commands: 3, relations: {}, memories: [], memorySummary: '', goal: 'ECONOMY', lastGiftTurn: -1 };
    for (const other of HOUSES) if (other.id !== k.id) k.relations[other.id] = { opinion: k.honor > .8 ? 18 : k.aggression > .8 ? -12 : 5, trust: 15 };
    s.kingdoms.push(k);
  }
  initializeFounding(s);
  generateWorld(s, preset);
  s.commerce = {offers:[],lastOfferTurn:0,cooldowns:{},aiTrades:{}};
  log(s, 'FOUND YOUR KINGDOM. Inspect the world, then choose your capital. Starting capitals must be at least 8 hexes apart.', 'council');
  initializeStrategy(s); initializeLiving(s); initializePlans(s); initializeEspionage(s); updateAttitudes(s);
  return s;
}

export function rebuildTerritory(s) {
  if(s.worldGeneration)return rebuildRegionalTerritory(s);
  const anchors = Object.values(s.tiles).filter(t => t.owner && ['city', 'town', 'fort', 'watchtower'].includes(t.building));
  for (const t of Object.values(s.tiles)) {
    if (anchors.includes(t)) continue;
    if (t.project && t.project.owner === t.owner && ['town', 'fort'].includes(t.project.type)) continue;
    const candidates = anchors.map(a => ({ a, d: distance(a, t) })).filter(x => x.d <= (x.a.building === 'fort' ? 2 : x.a.building === 'watchtower' ? 1 : 3));
    candidates.sort((a, b) => a.d - b.d || (a.a.owner === t.owner ? -1 : b.a.owner === t.owner ? 1 : a.a.id.localeCompare(b.a.id)));
    t.owner = candidates[0]?.a.owner || null;
  }
}
// New worlds spread influence along usable land. Legacy saves keep their original rules.
function rebuildRegionalTerritory(s) {
  const tiles=Object.values(s.tiles),anchors=tiles.filter(t=>t.owner&&['city','town','fort','watchtower'].includes(t.building)),claims=new Map();
  for(const a of anchors){
    const radius=a.building==='fort'?2:a.building==='watchtower'?1:a.capital?STARTING_RADIUS:3;
    const queue=[a],seen=new Set([a.id]);
    for(let i=0;i<queue.length;i++){
      const t=queue[i],d=distance(a,t);if(d>radius)continue;
      const before=claims.get(t.id);
      if(!before||d<before.d||d===before.d&&(a.owner===t.owner&&before.a.owner!==t.owner||a.owner!==t.owner&&before.a.owner!==t.owner&&a.id<before.a.id))claims.set(t.id,{a,d});
      for(const n of neighbors(s,t))if(passable(n)&&!seen.has(n.id)&&distance(a,n)<=radius){seen.add(n.id);queue.push(n);}
    }
  }
  for(const t of tiles){
    if(anchors.includes(t)||t.project&&t.project.owner===t.owner&&['town','fort'].includes(t.project.type))continue;
    t.owner=claims.get(t.id)?.a.owner||null;
  }
}
export function commandLimit(s, owner) { return Math.min(8, 3 + cityOrderBonus(s,owner) + Object.values(s.tiles).filter(t=>t.owner===owner).reduce((n,t)=>n+buildingLevel(t,'workshop'),0)); }
export function buildCheck(s, owner, id, type) {
  const t=s.tiles[id],k=kingdom(s,owner),b=BUILDINGS[type];
  if(s.phase==='founding')return 'Found all six kingdoms before construction begins.';
  if(s.outcome)return 'This campaign has ended.';
  if(!b||!k||!t)return 'Unknown construction.';
  const frontier=type==='town'&&!t.owner&&neighbors(s,t).some(n=>n.owner===owner);
  if(t.owner!==owner&&!frontier)return 'Build inside your borders; towns may claim adjacent neutral land.';
  if(!passable(t))return 'This terrain is impassable.';
  if(t.project)return 'Construction is already underway here.';
  if(s.armies.some(a=>a.tile===id&&atWar(s,owner,a.owner)))return 'Enemy troops occupy this tile.';
  if(k.commands<1)return 'No construction orders left this turn.';
  const spec=constructionSpec(t,type);
  if(!spec)return 'Maximum level reached.';
  if(['harbor','envoyOffice','chancery','intelligenceOffice'].includes(type)&&!['city','town'].includes(t.building))return 'Requires a town or city.';
  if(b.settlement&&!['city','town','fort'].includes(t.building))return 'Requires a town, city or fort.';
  if(type==='city'&&!['town','city'].includes(t.building))return 'Select a town or city to upgrade.';
  if(!b.settlement&&!['road','city'].includes(type)&&t.building&&t.building!==type)return 'This tile already has a different building.';
  if(b.terrain&&!b.terrain.includes(t.terrain))return `Requires ${b.terrain.join(' or ')} terrain.`;
  if(b.resource&&t.resource!==b.resource)return `Requires a ${b.resource} deposit.`;
  if(b.coastal&&t.terrain!=='coast'&&!neighbors(s,t).some(n=>n.terrain==='water'))return 'Requires a coastal settlement.';
  if(type==='tradeOutpost'&&!t.road&&!neighbors(s,t).some(n=>n.road))return 'Requires a road on this tile or an adjacent tile.';
  for(const [id,level] of Object.entries(b.requires||{}))if(buildingLevel(t,id)<level)return `Requires ${buildingSpec(id,level).name} here.`;
  if(type==='town'&&settlements(s).some(c=>distance(c,t)<4))return 'Towns must be at least 4 hexes apart.';
  if(type==='town'&&k.population<45)return 'At least 45 population is needed to found another town.';
  if(!canAfford(k,spec.cost))return `Missing ${Object.entries(spec.cost).filter(([r,n])=>k.resources[r]<n).map(([r,n])=>`${n-k.resources[r]} ${r}`).join(', ')}.`;
  return null;
}
export function build(s,owner,id,type) {
  const error=buildCheck(s,owner,id,type);if(error)return {ok:false,error};
  const k=kingdom(s,owner),t=s.tiles[id],spec=constructionSpec(t,type);pay(k,spec.cost);k.commands--;t.owner=owner;
  const efficient=spec.turns>=4&&(buildingLevel(t,'workshop')>=3||buildingLevel(t,'siegeFoundry')&&type==='siegeWorks'||Object.values(s.tiles).some(n=>n.owner===owner&&buildingLevel(n,'lumber')===3));
  const turns=spec.turns-(efficient?1:0);
  t.project={type,remaining:turns,total:turns,level:spec.level,repair:spec.repair,owner};return {ok:true};
}
export function recruitmentCost(t,type) {const u=UNITS[type];return u&&Object.fromEntries(Object.entries(u.cost).map(([r,n])=>[r,buildingLevel(t,'siegeFoundry')&&u.family==='siege'?Math.ceil(n*.8):n]));}
export function buildHighway(s,owner,fromId,toId) {
  const from=s.tiles[fromId],to=s.tiles[toId],k=kingdom(s,owner);
  if(s.outcome||!k||from?.owner!==owner||to?.owner!==owner||fromId===toId||!['city','town'].includes(from.building)||!['city','town'].includes(to.building))return {ok:false,error:'Choose two owned settlements connected by roads.'};
  const path=findPath(s,fromId,toId,owner,true);
  if(!path.length)return {ok:false,error:'Complete a continuous road connection first.'};
  const tiles=[from,...path.map(id=>s.tiles[id])];
  if(tiles.some(t=>t.owner!==owner||t.project||s.armies.some(a=>a.tile===t.id&&atWar(s,owner,a.owner))))return {ok:false,error:'The corridor must be owned, secure and free of other construction.'};
  const projects=tiles.filter(t=>buildingLevel(t,'road')<3).map(t=>{const specs=BUILDINGS.road.levels.slice(buildingLevel(t,'road'));return {tile:t,total:specs.reduce((n,b)=>n+b.turns,0),cost:specs.reduce((all,b)=>{for(const [r,n]of Object.entries(b.cost))all[r]=(all[r]||0)+n;return all;},{})};});
  if(!projects.length)return {ok:false,error:'This corridor is already a Royal Highway.'};
  const cost=projects.reduce((all,p)=>{for(const [r,n]of Object.entries(p.cost))all[r]=(all[r]||0)+n;return all;},{});
  if(k.commands<1||!canAfford(k,cost))return {ok:false,error:`A highway needs one order and ${Object.entries(cost).map(([r,n])=>`${n} ${r}`).join(', ')}.`};
  pay(k,cost);k.commands--;
  for(const p of projects)p.tile.project={type:'road',owner,level:3,total:p.total,remaining:p.total};
  return {ok:true,tiles:projects.length,cost};
}
export function recruitCheck(s,owner,id,type) {
  if(s.phase==='founding')return 'Found all six kingdoms before recruitment begins.';
  const t=s.tiles[id],k=kingdom(s,owner),u=UNITS[type];
  if(s.outcome||!u||!k||t?.owner!==owner||!['city','town','fort'].includes(t.building))return 'Muster at an owned town, city or fort.';
  if(s.armies.some(a=>a.tile===id&&atWar(s,owner,a.owner)))return 'The muster ground is under attack.';
  for(const [building,level] of Object.entries(u.requires||{}))if(buildingLevel(t,building)<level)return `Requires ${buildingSpec(building,level).name} here.`;
  if(k.commands<1||k.population-u.count<20)return 'Need an order and enough population to leave 20 civilians.';
  const cost=recruitmentCost(t,type);
  if(!canAfford(k,cost))return `Missing ${Object.entries(cost).filter(([r,n])=>k.resources[r]<n).map(([r,n])=>`${n-k.resources[r]} ${r}`).join(', ')}.`;
  return null;
}
export function recruit(s,owner,id,type) {
  const error=recruitCheck(s,owner,id,type);if(error)return {ok:false,error};
  const t=s.tiles[id],k=kingdom(s,owner),u=UNITS[type];
  let army=s.armies.find(a=>a.owner===owner&&a.tile===id);
  if(!army){army={id:`army-${s.nextId++}`,owner,tile:id,units:emptyUnits(),formation:'balanced',retreats:0,morale:1,order:'hold',path:[],target:null};s.armies.push(army);}
  const bonus=u.family==='infantry'?Math.min(k.population-u.count-20,Math.max(0,buildingLevel(t,'barracks')-1)):0;
  pay(k,recruitmentCost(t,type));k.population-=u.count+bonus;k.commands--;army.units[type]=(army.units[type]||0)+u.count+bonus;
  army.morale=Math.min(1,army.morale+(buildingLevel(t,'barracks')>=2?.04:0)+(buildingLevel(t,'greatStable')&&u.family==='mounted'?.05:0));
  return {ok:true,armyId:army.id};
}
export function canEnter(s, owner, t) { return !!(passable(t) && (!t.owner || t.owner === owner || atWar(s, owner, t.owner) || treaty(s, owner, t.owner, 'alliance') || treaty(s, owner, t.owner, 'vassalage') || treaty(s, owner, t.owner, 'access'))); }
export function moveCost(a, b) { return (a.road && b.road ? [.5,.5,.4,.3][Math.min(buildingLevel(a,'road'),buildingLevel(b,'road'))] : TERRAINS[b.terrain].cost) + (a.river !== b.river && (a.river || b.river) && !(a.road && b.road) ? 1 : 0); }
export function findPath(s, startId, endId, owner, roadsOnly = false, avoid = null) {
  const start = s.tiles[startId], goal = s.tiles[endId];
  if (!start || !goal || !passable(goal)) return [];
  // Peace or an expired alliance must let stranded armies leave. This grants
  // only a contiguous exit route, never fresh entry into closed borders.
  const exitOwner = !roadsOnly && !canEnter(s, owner, start) && goal.owner !== start.owner ? start.owner : null;
  const open = [{ id: startId, f: 0 }], costs = new Map([[startId, 0]]), came = new Map(), closed = new Set();
  while (open.length) {
    open.sort((a, b) => a.f - b.f || a.id.localeCompare(b.id)); const current = open.shift().id;
    if (closed.has(current)) continue;
    if (current === endId) {
      const path = []; let id = endId;
      while (id !== startId) { path.unshift(id); id = came.get(id); }
      return path;
    }
    closed.add(current);
    for (const n of neighbors(s, s.tiles[current])) {
      if (avoid?.has(n.id)) continue;
      const exiting = exitOwner && n.owner === exitOwner && s.tiles[current].owner === exitOwner;
      if (!passable(n) || (roadsOnly ? !n.road || (n.owner && n.owner !== owner && (tradeBlocked(s, owner, n.owner) || (!treaty(s, owner, n.owner, 'trade') && !treaty(s, owner, n.owner, 'alliance')))) : !canEnter(s, owner, n) && !exiting)) continue;
      const cost = costs.get(current) + moveCost(s.tiles[current], n);
      if (cost >= (costs.get(n.id) ?? Infinity)) continue;
      costs.set(n.id, cost); came.set(n.id, current);
      // Minimum edge is 0.3: the heuristic must not overestimate road travel.
      open.push({ id: n.id, f: cost + distance(n, goal) * .3 });
    }
  }
  return [];
}
export function orderStructureAttack(s, owner, armyId, targetId, type, mode = 'attack', avoid = null) {
  const a = s.armies.find(a => a.id === armyId && a.owner === owner), t = s.tiles[targetId];
  const error = structureAttackCheck(s, a, t, type, mode);
  if (error) return {ok:false,error};
  const path = mode === 'bombard' || a.tile === targetId ? [] : findPath(s,a.tile,targetId,owner,false,avoid);
  if (mode === 'attack' && a.tile !== targetId && !path.length) return {ok:false,error:'No legal route to this structure.'};
  a.path = path; a.target = targetId; a.order = mode; a.structureTarget = type;
  return {ok:true,path};
}
export function orderArmy(s, owner, armyId, targetId, order = 'move', avoid = null) {
  const a = s.armies.find(a => a.id === armyId && a.owner === owner);
  if (s.outcome || !a || !['move', 'attack', 'retreat', 'hold'].includes(order)) return { ok: false, error: 'Select one of your armies.' };
  const t = s.tiles[targetId];
  if (order === 'attack' && targetId === a.tile && t?.owner && atWar(s, owner, t.owner) && structuresAt(t).length)
    return orderStructureAttack(s,owner,armyId,targetId,structuresAt(t).find(type => type !== 'road') || 'road');
  if (order === 'hold' || targetId === a.tile) { a.path = []; a.target = null; a.structureTarget = null; a.order = 'hold'; return { ok: true }; }
  const path = findPath(s, a.tile, targetId, owner, false, avoid);
  if (!path.length) return { ok: false, error: 'No legal route. Neutral borders require an alliance or a declaration of war.' };
  a.path = path; a.target = targetId; a.structureTarget = null; a.order = order;
  return { ok: true, path };
}
export function mergeArmies(s, owner, id) {
  if (s.outcome) return { ok: false, error: 'This campaign has ended.' };
  const group = armiesOf(s, owner).filter(a => a.tile === id);
  if (group.length < 2) return { ok: false, error: 'Bring two armies to the same tile first.' };
  const base = group[0];
  for (const a of group.slice(1)) { for (const u of Object.keys(UNITS)) base.units[u] = (base.units[u]||0) + (a.units[u]||0); s.armies = s.armies.filter(x => x !== a); for (const p of s.intrigue?.plans || []) if (p.assignedArmies.includes(a.id)) p.assignedArmies = [...new Set(p.assignedArmies.map(id => id === a.id ? base.id : id))]; }
  base.path = []; base.order = 'hold'; base.target = null; base.structureTarget = null; return { ok: true };
}
export function splitArmy(s, owner, armyId) {
  if (s.outcome) return { ok: false, error: 'This campaign has ended.' };
  const a = s.armies.find(a => a.id === armyId && a.owner === owner);
  if (!a || sizeOf(a) < 12) return { ok: false, error: 'At least 12 soldiers are needed to split an army.' };
  const b = { ...a, id: `army-${s.nextId++}`, units: {}, path: [], target: null, order: 'hold' };
  for (const u of Object.keys(UNITS)) { b.units[u] = Math.floor((a.units[u]||0) / 2); a.units[u] = (a.units[u]||0) - b.units[u]; }
  a.path = []; a.order = 'hold'; a.target = null; a.structureTarget = null; b.structureTarget = null; s.armies.push(b); return { ok: true, armyId: b.id };
}
export function strength(a, defending = false, t = null) {
  let total = Object.entries(a.units).reduce((n, [type, count]) => n + count * (UNITS[type]?.[defending ? 'defense' : 'attack']||0), 0) * a.morale;
  if (defending && t) total *= TERRAINS[t.terrain].defense * (t.building === 'fort' ? 1.35 + buildingLevel(t,'fort')*.25 : 1) * (t.walls > 0 ? 1.6 : 1) * (t.building === 'watchtower' ? 1 + buildingLevel(t,'watchtower')*.15 : 1) * (['fort', 'city'].includes(t.building) ? 1 + Math.min(.25, familyCount(a,'ranged') / Math.max(1, sizeOf(a))) : 1);
  if (!defending && t?.terrain === 'plains') total *= 1 + Math.min(.25, familyCount(a,'mounted') / Math.max(1, sizeOf(a)));
  return total;
}
function casualties(a, ratio) { inflict(a, Math.ceil(sizeOf(a)*ratio)); }
function retreat(s, a, from) {
  const tiles = neighbors(s, from).filter(t => canEnter(s, a.owner, t) && t.id !== a.tile && !s.armies.some(e => e.tile === t.id && atWar(s, e.owner, a.owner)));
  tiles.sort((x, y) => Number(y.owner === a.owner) - Number(x.owner === a.owner));
  if (tiles[0]) { a.tile = tiles[0].id; a.path = []; a.order = 'hold'; a.structureTarget = null; a.target = null; a.morale = Math.max(.1,a.morale-.04); }
  else casualties(a, .6);
}
function battle(s,attacker,defender,t) {
  const event={id:s.nextId++,turn:s.turn,attacker:attacker.owner,defender:defender.owner,attackerArmyId:attacker.id,defenderArmyId:defender.id,tile:t.id,from:attacker.tile,action:'battle',before:[sizeOf(attacker),sizeOf(defender)]};
  const from=s.tiles[attacker.tile];
  const result=resolveFieldBattle(attacker,defender,t,{roll:()=>random(s),riverCrossing:from.river!==t.river&&(from.river||t.river)&&!(buildingLevel(from,'road')>=2&&buildingLevel(t,'road')>=2),surrounded:[attacker,defender].map(a=>neighbors(s,s.tiles[a.tile]).filter(n=>canEnter(s,a.owner,n)&&!s.armies.some(e=>e.tile===n.id&&atWar(s,e.owner,a.owner))).length===0)});
  const loser=result.loser===0?attacker:defender;
  const retreatFrom=loser.tile;
  if(sizeOf(loser)>0)retreat(s,loser,s.tiles[loser.tile]);
  if(loser===attacker){attacker.path=[];attacker.order='hold';attacker.structureTarget=null;attacker.target=null;}
  Object.assign(event,result,{winner:result.winner===0?attacker.owner:defender.owner,after:[sizeOf(attacker),sizeOf(defender)],retreat:sizeOf(loser)>0&&loser.tile!==retreatFrom?loser.tile:null,retreatOwner:loser.owner});
  event.casualties=[attacker,defender].map((a,i)=>Object.fromEntries(Object.entries(event.composition[i]).map(([id,n])=>[id,n-(a.units[id]||0)])));
  s.militaryEvents.push(event);
  log(s,`${kingdom(s,event.winner).name} wins at ${t.name||t.id}; ${sizeOf(loser)===0?'the opposing army is destroyed':result.routed?'the opposing line routs':'the opposing line withdraws'}.`,'battle');
  s.armies=s.armies.filter(a=>sizeOf(a)>0);
}
function besiege(s,a,t,defender=null) {
  const event={id:s.nextId++,turn:s.turn,attacker:a.owner,defender:t.owner,tile:t.id,from:a.tile,action:'siege',before:[sizeOf(a),t.walls+(t.fortIntegrity??fortMaximum(t))]};
  const defendersBefore=defender?sizeOf(defender):0;
  const result=siegeStep(s,a,t,defender,()=>random(s));
  event.troopLosses=[event.before[0]-sizeOf(a),defendersBefore-(defender?sizeOf(defender):0)];
  event.after=[sizeOf(a),t.walls+(t.fortIntegrity||0)];event.phases=[{name:'Siege',notes:result.notes,loss:[event.before[0]-event.after[0],result.damage]}];event.breach=event.after[1]===0;
  s.militaryEvents.push(event);log(s,`${kingdom(s,a.owner).name} besieges ${t.name||t.id}: ${event.after[1]} fortification strength remains.`,'battle');
  return false;
}
// Read-only estimates use the actual clash + retreat resolver, with isolated
// armies, event lists and RNG. Nothing is read from or written to campaign RNG.
export function projectedBattleLosses(s,armyId,targetId) {
  const attacker=s.armies.find(a=>a.id===armyId&&sizeOf(a)>0),t=s.tiles[targetId];
  if(!attacker||!t||attacker.order==='bombard')return null;
  const enemies=s.armies.filter(a=>a.tile===targetId&&sizeOf(a)>0&&atWar(s,attacker.owner,a.owner));
  const defender=enemies[0];if(!defender)return null;
  const path=attacker.tile===targetId?[]:findPath(s,attacker.tile,targetId,attacker.owner);
  if(attacker.tile!==targetId&&!path.length)return null;
  const approach=path.length>1?path.at(-2):attacker.tile;
  const siege=attacker.tile!==targetId&&t.owner===defender.owner&&(t.walls>0||(t.fortIntegrity??fortMaximum(t))>0);
  const samples=[[],[]],before=[sizeOf(attacker),sizeOf(defender)];
  for(let i=0;i<32;i++){
    const simulation={...s,armies:structuredClone(s.armies),events:[],militaryEvents:[],rng:Math.imul(i+1,0x9e3779b1)>>>0};
    const a=simulation.armies.find(x=>x.id===armyId),d=simulation.armies.find(x=>x.id===defender.id);
    a.tile=approach;
    if(siege)besiege(simulation,a,structuredClone(t),d);else battle(simulation,a,d,t);
    [a,d].forEach((x,side)=>samples[side].push(before[side]-sizeOf(x)));
  }
  const ranges=samples.map((values,side)=>{
    const low=Math.min(...values),high=Math.max(...values);
    // A small envelope avoids implying sampled extrema are guaranteed bounds.
    const padding=high>low?Math.max(1,Math.ceil((high-low)*.15)):0;
    return {low:Math.max(0,low-padding),high:Math.min(before[side],high+padding)};
  });
  return {kind:siege?'siege':'battle',attackerId:armyId,defenderId:defender.id,enemyOwner:defender.owner,
    yours:ranges[0],theirs:ranges[1],multipleDefenders:enemies.length>1};
}
function capture(s, a, t) {
  if (!t.owner || t.owner === a.owner || !atWar(s, a.owner, t.owner)) return true;
  if (t.walls > 0 || (t.fortIntegrity ?? fortMaximum(t)) > 0) return besiege(s,a,t);
  if (['city', 'town', 'fort', 'watchtower'].includes(t.building)) {
    const defense = (t.building === 'city' ? 14 : 8)*(t.building==='fort'?buildingLevel(t,'fort'):1);
    if (strength(a, false, t) < defense) { casualties(a, .12); return false; }
    const before=sizeOf(a);casualties(a, .08); const previous = t.owner;
    s.militaryEvents.push({ id: s.nextId++, turn: s.turn, attacker: a.owner, defender: previous, tile: t.id, from: a.tile, action: 'capture', winner: a.owner, troopLosses:[before-sizeOf(a),0] });
    changeRelation(s, previous, a.owner, { opinion: -15, grievance: 20, aggression: 12 }, `${t.name || 'A settlement'} was captured.`);
    t.owner = a.owner; t.project = null; t.siege = null;
    log(s, `${kingdom(s, a.owner).name} captures ${t.name || 'a fort'} from ${kingdom(s, previous).name}.`, 'war');
    remember(s, previous, `${kingdom(s, a.owner).name} captured ${t.name || t.id}.`, 10);
    rebuildTerritory(s);
  } else { t.owner = a.owner; t.project = null; t.siege = null; }
  return true;
}
function zoneOfControl(s, a, t) {
  return neighbors(s, t).some(n => (['fort','watchtower'].includes(n.building) && n.owner && atWar(s, a.owner, n.owner)) || s.armies.some(e => e.tile === n.id && atWar(s, a.owner, e.owner)));
}
export function resolveMovement(s) {
  // Stable, alternating initiative avoids one house always moving first.
  const armies = [...s.armies].sort((a, b) => a.id.localeCompare(b.id));
  if (s.turn % 2 === 0) armies.reverse();
  for (const a of armies) {
    if (!s.armies.includes(a) || sizeOf(a) === 0) continue;
    if (a.structureTarget) {
      const t = s.tiles[a.target];
      if (structureAttackCheck(s,a,t,a.structureTarget,a.order)) { a.path=[]; a.target=null; a.structureTarget=null; a.order='hold'; }
      else if (a.order === 'bombard' || a.tile === a.target) {
        const enemy = s.armies.find(e => e.tile === t.id && sizeOf(e) > 0 && atWar(s,a.owner,e.owner));
        if (enemy && a.order === 'attack') battle(s,a,enemy,t);
        else damageStructure(s,a,t,a.structureTarget,a.order);
        continue;
      }
    }
    const startingBudget = armySpeed(a);
    let budget = startingBudget;
    while (a.path.length && budget > 0) {
      const t = s.tiles[a.path[0]], from = s.tiles[a.tile];
      const exiting = from.owner && !canEnter(s, a.owner, from) && s.tiles[a.target]?.owner !== from.owner && t?.owner === from.owner;
      if ((!canEnter(s, a.owner, t) && !exiting) || !t || distance(from, t) !== 1) { a.path = []; break; }
      // A slow siege stack can spend its entire turn crossing one costly edge.
      const cost = moveCost(from, t); if (cost > budget && budget !== startingBudget) break;
      const enemy = s.armies.find(e => e.tile === t.id && atWar(s, a.owner, e.owner));
      if (enemy) { if(t.owner===enemy.owner&&(t.walls>0||(t.fortIntegrity??fortMaximum(t))>0))besiege(s,a,t,enemy);else battle(s,a,enemy,t); break; }
      if (!(a.structureTarget && a.target === t.id) && !capture(s, a, t)) break;
      a.tile = t.id; a.path.shift(); budget -= cost;
      if (zoneOfControl(s, a, t)) break;
    }
    if (a.structureTarget && a.tile === a.target) damageStructure(s,a,s.tiles[a.target],a.structureTarget,a.order);
    if (!a.path.length && !a.structureTarget) a.order = 'hold';
    a.morale = Math.min(1, a.morale + .04);
  }
  s.armies = s.armies.filter(a => sizeOf(a) > 0);
  s.militaryEvents = s.militaryEvents.slice(-100);
}

export function economyProjection(s, owner) {
  const k = kingdom(s, owner), {income,gross,stalls}=productionPlan(s,owner), towns=settlements(s,owner);
  for(const t of Object.values(s.tiles).filter(t=>t.owner===owner)) if(t.building)income.gold--;
  income.gold -= spyUpkeep(s, owner);
  income.gold += Math.floor(k.population * ({ low: .08, medium: .17, high: .28 }[k.tax]));
  income.food -= Math.ceil(k.population / 12);
  for (const a of armiesOf(s, owner)) { income.food -= Math.ceil(sizeOf(a) / 6); income.gold -= Math.ceil((sizeOf(a)+familyCount(a,'mounted')+familyCount(a,'siege')*2) / 9); }
  // Each connected pair pays once to each eligible kingdom, never per path tile.
  const partners = settlements(s).filter(t => t.owner === owner || (treaty(s, owner, t.owner, 'trade') && !tradeBlocked(s, owner, t.owner)));
  const seen = new Set(); let routes = 0;
  for (const a of towns) for (const b of partners) {
    const key = pair(a.id, b.id);
    if (a.id === b.id || seen.has(key) || !a.road || !b.road) continue;
    seen.add(key);
    const route=findPath(s,a.id,b.id,owner,true);
    if (route.length) { const quality=Math.min(buildingLevel(a,'road'),...route.map(id=>buildingLevel(s.tiles[id],'road')));income.gold += (quality-1)*4; income.gold += (b.owner === owner ? 6 : 10) + buildingLevel(a,'market')*2 + buildingLevel(a,'merchantGuild')*3; routes++; }
  }
  return { income, gross, stalls, routes };
}
// Forecast completed construction, using the same calculation as resolution.
export function populationProjection(s, owner) {
  let projected=s;
  if(Object.values(s.tiles).some(t=>t.project?.owner===owner&&t.project.remaining===1)){
    projected=structuredClone(s);
    for(const t of Object.values(projected.tiles))if(t.project&&t.project.owner===t.owner&&t.project.remaining===1)completeConstruction(t);
    rebuildTerritory(projected);
  }
  return {...calculatePopulationChange(projected,owner,economyProjection(projected,owner).income),capacity:populationCapacity(s,owner),nextCapacity:populationCapacity(projected,owner)};
}
export function resolveEconomy(s) {
  for (const t of Object.values(s.tiles)) if (t.project) {
    if (t.project.owner !== t.owner) { t.project = null; continue; }
    if (--t.project.remaining <= 0) {
      const name=buildingSpec(t.project.type,t.project.level||1).name;
      recordStrategyAction(s,t.owner,{kind:'complete',tile:t.id,building:t.project.type,level:t.project.level||1});
      completeConstruction(t);
      if(isHumanHouse(s,t.owner))log(s,`${name} completed at ${t.name||t.id}.`,'economy');
    }
  }
  rebuildTerritory(s);
  for (const k of s.kingdoms) {
    if (!alive(s, k.id)) continue;
    const { income } = economyProjection(s, k.id);
    const population=calculatePopulationChange(s,k.id,income);
    for (const r of RESOURCES) k.resources[r] = Math.min(Math.max(k.resources[r],storageCapacity(s,k.id,r)), k.resources[r]+income[r]);
    const deficit = population.shortage;
    if (deficit) {
      const protection=Object.values(s.tiles).some(t=>t.owner===k.id&&buildingLevel(t,'greatGranary'))?.5:1;
      for (const a of armiesOf(s, k.id)) {casualties(a, .06*protection);a.morale=Math.max(.1,a.morale-.12*protection);}
      k.happiness = population.happinessAfter; k.population = population.nextPopulation;
      if (isHumanHouse(s,k.id)) log(s, `${k.name}: Food or gold ran out. Soldiers deserted and population fell.`, 'economy');
    } else {
      k.happiness = population.happinessAfter; k.population = population.nextPopulation;
    }
    for (const r of RESOURCES) k.resources[r] = Math.max(0, Math.min(99999, k.resources[r]));
    k.commands = commandLimit(s, k.id);
  }
  s.armies = s.armies.filter(a => sizeOf(a) > 0);
}

export function strategicThreat(s, owner, t) {
  return s.armies.reduce((value, a) => value + (a.owner === owner ? -1 : atWar(s, a.owner, owner) ? 1 : 0) * strength(a) / (1 + distance(t, s.tiles[a.tile])), 0);
}
export function strategyTurn(s) { return runStrategyTurn(s); }
export function checkVictory(s) {
  if(s.phase==='founding')return;
  if (!s.controllers && !alive(s, PLAYER)) { s.outcome={won:false,reason:'Your last settlement has fallen. Your house survives in the chronicles.'}; return; }
  const houses=s.controllers?s.kingdoms.filter(k=>alive(s,k.id)).map(k=>k.id):[PLAYER];
  s.crownProgress ||= {};
  for(const actor of houses){
    const cities=settlements(s),owned=settlements(s,actor),rivals=s.kingdoms.filter(k=>k.id!==actor&&alive(s,k.id));
    const allies=rivals.filter(k=>treaty(s,actor,k.id,'alliance')||treaty(s,actor,k.id,'vassalage')).map(k=>k.id);
    const progress=s.controllers?(s.crownProgress[actor]||0):s.diplomaticTurns;
    const next=allies.length>rivals.length/2&&allies.length>0?Math.min(3,progress+1):0;
    s.crownProgress[actor]=next;if(actor===PLAYER)s.diplomaticTurns=next;
    const conquest=owned.length>=Math.ceil(cities.length*.6),accord=next>=3;
    if(!s.outcome&&(conquest||accord))s.outcome={won:actor===PLAYER||allies.includes(PLAYER),winnerHouseId:actor,coalition:accord?[actor,...allies]:[actor],reason:s.controllers?`${kingdom(s,actor).name} claims the crown through ${accord?'the Crown Accord':'territorial dominance'}.${accord?` Allied victors: ${allies.map(id=>kingdom(s,id).name).join(', ')}.`:''}`:conquest?'Conquest: your banners fly above at least 60% of all settlements.':'The Crown Accord: a majority of surviving houses recognizes your leadership for three turns.'};
  }
}

export function parseSave(raw) {
  if (typeof raw !== 'string' || raw.length > 2000000) throw new Error('Save is too large or unreadable.');
  const s = JSON.parse(raw);
  const number = (n, min = 0, max = 100000) => Number.isFinite(n) && n >= min && n <= max;
  if (![1, 2, SAVE_VERSION].includes(s?.version) || s.width !== 40 || s.height !== 30 || !Number.isInteger(s.turn) || !number(s.turn, s.phase==='founding'?0:1) || !number(s.rng, 0, 4294967295) || !number(s.nextId, 1) || !s.tiles || Object.keys(s.tiles).length !== 1200 || !Array.isArray(s.kingdoms) || s.kingdoms.length !== 6) throw new Error('Unsupported or damaged campaign save.');
  const oldVersion=s.version;
  if(oldVersion<3) migrateEconomy(s);
  for (const h of HOUSES) {
    const k = kingdom(s, h.id);
    if (!k || !RESOURCES.every(r => number(k.resources?.[r])) || !['low', 'medium', 'high'].includes(k.tax) || !number(k.population) || !number(k.commands, 0, 8) || !number(k.happiness, 0, 100) || !Array.isArray(k.memories) || k.memories.length > 30 || k.memories.some(m => typeof m.text !== 'string' || m.text.length > 1000 || !number(m.turn) || !number(m.importance, 0, 10)) || typeof k.memorySummary !== 'string' || k.memorySummary.length > 900) throw new Error('Damaged kingdom data.');
    for (const other of HOUSES.filter(o => o.id !== h.id)) if (!number(k.relations?.[other.id]?.opinion, -100, 100) || !number(k.relations?.[other.id]?.trust, -100, 100)) throw new Error('Damaged diplomacy data.');
    Object.assign(k, h); // Presentation/personality comes from trusted content, not save strings.
  }
  for (let r = 0; r < 30; r++) for (let q = 0; q < 40; q++) {
    const t = s.tiles[tileId(q, r)];
    if (!t || t.id !== tileId(q, r) || t.q !== q || t.r !== r || !Object.hasOwn(TERRAINS, t.terrain) || (t.owner && !kingdom(s, t.owner)) || (t.building && (!Object.hasOwn(BUILDINGS,t.building)||BUILDINGS[t.building].settlement||['road','wall'].includes(t.building))) || !number(t.walls, 0, 180)) throw new Error('Damaged map data.');
    if (t.project && (!Object.hasOwn(BUILDINGS, t.project.type) || !number(t.project.remaining, 1, 8) || !kingdom(s, t.project.owner))) throw new Error('Damaged construction data.');
  }
  if (!Array.isArray(s.armies) || s.armies.length > 500 || new Set(s.armies.map(a => a.id)).size !== s.armies.length) throw new Error('Damaged army data.');
  for (const a of s.armies) if (!kingdom(s, a.owner) || !s.tiles[a.tile] || !number(a.morale, .1, 1) || !Object.keys(UNITS).every(u => Number.isInteger(a.units?.[u]) && number(a.units[u])) || !Array.isArray(a.path) || a.path.length > 1200 || a.path.some(id => !s.tiles[id])) throw new Error('Damaged army orders.');
  for (const list of ['treaties', 'pledges', 'wars', 'events']) if (!Array.isArray(s[list]) || s[list].length > 1000) throw new Error('Damaged campaign history.');
  for (const t of s.treaties) if (!['alliance', 'peace', 'trade', 'vassalage', 'non-aggression', 'access', 'embargo', 'recurring'].includes(t.type) || t.parties?.length !== 2 || t.parties.some(id => !kingdom(s, id)) || !number(t.expires)) throw new Error('Damaged treaty data.');
  for (const p of s.pledges) if (!kingdom(s, p.debtor) || !kingdom(s, p.creditor) || !p.intent || !INTENT_TYPES.includes(p.intent.type) || !number(p.intent.giveAmount, 0, 1000) || !RESOURCES.includes(p.intent.giveResource) || !number(p.deadline) || !number(p.created) || !number(p.held, 0, 2) || !['pending', 'fulfilled', 'broken', 'released'].includes(p.status)) throw new Error('Damaged pledge data.');
  if (s.outcome && (typeof s.outcome.won !== 'boolean' || typeof s.outcome.reason !== 'string')) throw new Error('Damaged result.');
  if (!Array.isArray(s.militaryEvents) || s.militaryEvents.length > 100 || s.militaryEvents.some(e => !kingdom(s, e.attacker) || !kingdom(s, e.defender) || !number(e.turn) || !s.tiles[e.tile])) throw new Error('Damaged military history.');
  if (!s.conversations || typeof s.conversations !== 'object' || !number(s.diplomaticTurns, 0, 3)) throw new Error('Damaged campaign data.');
  for (const history of Object.values(s.conversations)) if (!Array.isArray(history) || history.length > 60 || history.some(m => typeof m.text !== 'string' || m.text.length > 2000 || !['player', 'ruler', 'council'].includes(m.role))) throw new Error('Damaged conversation data.');
  if (oldVersion === 1) initializeLiving(s);
  s.version=SAVE_VERSION;
  validateLivingSave(s);
  validateFoundingSave(s);
  if(s.worldGeneration&&(!Object.hasOwn(MAP_PROFILES,s.mapProfile)||!Number.isInteger(s.seed)||!Number.isInteger(s.generation?.attempt)||s.generation.attempt<0||s.generation.attempt>=96))throw new Error('Damaged regional world metadata.');
  validateExpansion(s);
  validateStrategySave(s);
  validateStructures(s);
  validatePlans(s); validateEspionage(s); validatePolitics(s);
  return s;
}
