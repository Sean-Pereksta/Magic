import { BUILDINGS, HOUSES, INTENT_TYPES, RESOURCES, SAVE_VERSION, TERRAINS, UNITS } from './data.mjs';

export const PLAYER = 'ashen';
const DIRS = [[1, 0], [1, -1], [0, -1], [-1, 0], [-1, 1], [0, 1]];
export const tileId = (q, r) => `${q},${r}`;
export const distance = (a, b) => (Math.abs(a.q - b.q) + Math.abs(a.r - b.r) + Math.abs(a.q + a.r - b.q - b.r)) / 2;
export const getTile = (s, id) => s.tiles[id];
export const kingdom = (s, id) => s.kingdoms.find(k => k.id === id);
export const settlements = (s, owner) => Object.values(s.tiles).filter(t => ['city', 'town'].includes(t.building) && (!owner || t.owner === owner));
export const armiesOf = (s, owner) => s.armies.filter(a => a.owner === owner);
export const neighbors = (s, t) => DIRS.map(([q, r]) => s.tiles[tileId(t.q + q, t.r + r)]).filter(Boolean);
export const passable = t => !!t && Number.isFinite(TERRAINS[t.terrain]?.cost);
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
export function shiftRelation(s, a, b, opinion, trust = 0) {
  const r = relation(s, a, b); if (!r) return;
  r.opinion = Math.max(-100, Math.min(100, r.opinion + opinion));
  r.trust = Math.max(-100, Math.min(100, r.trust + trust));
}
export function declareWar(s, a, b) {
  if (!alive(s, a) || !alive(s, b) || a === b || atWar(s, a, b)) return false;
  const broken = s.treaties.filter(t => t.parties.includes(a) && t.parties.includes(b));
  s.treaties = s.treaties.filter(t => !broken.includes(t));
  if (broken.length) {
    for (const k of s.kingdoms.filter(k => k.id !== a)) shiftRelation(s, k.id, a, -12, -20);
    remember(s, b, `${kingdom(s, a).name} broke our treaty.`, 10);
  }
  s.wars.push(pair(a, b)); shiftRelation(s, b, a, -30, -20);
  log(s, `${kingdom(s, a).name} declares war on ${kingdom(s, b).name}.`, 'war');
  return true;
}
export function makePeace(s, a, b) {
  s.wars = s.wars.filter(w => w !== pair(a, b));
  for (const army of s.armies) if ((army.owner === a || army.owner === b) && army.path.length) army.path = [];
}

export function createGame(seed = 8147, preset = 'crossroads') {
  seed = Number(seed) >>> 0 || 8147;
  const s = { version: SAVE_VERSION, width: 40, height: 30, seed, rng: seed, preset, turn: 1, nextId: 1, tiles: {}, kingdoms: [], armies: [], treaties: [], wars: [], pledges: [], events: [], militaryEvents: [], conversations: {}, diplomaticTurns: 0, outcome: null };
  for (let r = 0; r < s.height; r++) for (let q = 0; q < s.width; q++) {
    const n = random(s), edge = r === 0 || q === 0 || q === 39 || r === 29;
    let terrain = edge ? 'water' : n < .10 ? 'mountain' : n < .34 ? 'forest' : n < .54 ? 'hills' : 'plains';
    if (!edge && (q === 1 || r === 1 || q === 38 || r === 28)) terrain = 'coast';
    if (preset === 'highlands' && n < .22 && !edge) terrain = 'mountain';
    const resource = terrain === 'hills' ? (random(s) < .45 ? 'iron' : 'stone') : terrain === 'forest' ? 'wood' : terrain === 'plains' && random(s) < .4 ? 'food' : null;
    const t = { id: tileId(q, r), q, r, terrain, resource, owner: null, building: null, road: false, river: q === 19 && r > 1 && r < 28, walls: 0, market: false, workshop: false, project: null, capital: null };
    s.tiles[t.id] = t;
  }
  const starts = [[5, 6], [17, 4], [31, 5], [32, 21], [20, 24], [5, 22]];
  HOUSES.forEach((house, i) => {
    const k = { ...house, resources: { food: 140, wood: 110, stone: 90, iron: 50, gold: 200 }, population: 80, happiness: 70, tax: 'medium', commands: 3, relations: {}, memories: [], memorySummary: '', goal: 'ECONOMY', lastGiftTurn: -1 };
    for (const other of HOUSES) if (other.id !== k.id) k.relations[other.id] = { opinion: k.honor > .8 ? 18 : k.aggression > .8 ? -12 : 5, trust: 15 };
    s.kingdoms.push(k);
    const [q, r] = starts[i], capital = s.tiles[tileId(q, r)];
    Object.assign(capital, { terrain: 'plains', building: 'city', owner: k.id, capital: k.id, road: true, resource: null, name: ['Emberkeep', 'Frostwatch', 'Briarhold', 'Solstice', 'Moonveil', 'Redhaven'][i] });
    const ring = neighbors(s, capital);
    ring.forEach((t, j) => Object.assign(t, { terrain: ['plains', 'forest', 'hills', 'hills', 'plains', 'plains'][j], resource: ['food', 'wood', 'stone', 'iron', 'food', null][j], building: ['farm', 'lumber', 'quarry', 'mine', null, null][j], road: true, owner: k.id }));
    s.armies.push({ id: `army-${s.nextId++}`, owner: k.id, tile: capital.id, units: { levy: 20, archer: 6, cavalry: 2, siege: 0 }, morale: 1, order: 'hold', path: [], target: null });
  });
  // Guaranteed connected corridors retain hills/forests, but clear impassable hexes.
  for (let i = 0; i < starts.length; i++) {
    let t = s.tiles[tileId(...starts[i])]; const goal = s.tiles[tileId(...starts[(i + 1) % starts.length])];
    while (t.id !== goal.id) {
      t = neighbors(s, t).sort((a, b) => distance(a, goal) - distance(b, goal))[0];
      if (!passable(t)) { t.terrain = 'plains'; t.resource = 'food'; }
    }
  }
  rebuildTerritory(s);
  log(s, 'Six houses contest the crown. Unite three rival houses for three turns, or control 60% of settlements.', 'council');
  return s;
}

export function rebuildTerritory(s) {
  const anchors = Object.values(s.tiles).filter(t => t.owner && ['city', 'town', 'fort'].includes(t.building));
  for (const t of Object.values(s.tiles)) {
    if (anchors.includes(t)) continue;
    if (t.project && t.project.owner === t.owner && ['town', 'fort'].includes(t.project.type)) continue;
    const candidates = anchors.map(a => ({ a, d: distance(a, t) })).filter(x => x.d <= (x.a.building === 'fort' ? 2 : 3));
    candidates.sort((a, b) => a.d - b.d || (a.a.owner === t.owner ? -1 : b.a.owner === t.owner ? 1 : a.a.id.localeCompare(b.a.id)));
    t.owner = candidates[0]?.a.owner || null;
  }
}
export function commandLimit(s, owner) { return Math.min(8, 3 + Object.values(s.tiles).filter(t => t.owner === owner && t.workshop).length); }
export function buildCheck(s, owner, id, type) {
  const t = s.tiles[id], k = kingdom(s, owner), b = BUILDINGS[type];
  if (s.outcome) return 'This campaign has ended.';
  if (!b || !k || !t) return 'Unknown construction.';
  const frontierTown = type === 'town' && !t.owner && neighbors(s, t).some(n => n.owner === owner);
  if (t.owner !== owner && !frontierTown) return 'Build inside your borders; towns may also claim an adjacent neutral tile.';
  if (!passable(t)) return 'This terrain is impassable.';
  if (t.project) return 'Construction is already underway here.';
  if (s.armies.some(a => a.tile === id && atWar(s, owner, a.owner))) return 'Enemy troops occupy this tile.';
  if (k.commands < 1) return 'No construction orders left this turn.';
  if (type === 'road' && t.road) return 'This tile already has a road.';
  if (['wall', 'market', 'workshop'].includes(type)) {
    if (!['city', 'town'].includes(t.building)) return 'Select a town or city.';
    if (type === 'wall' ? t.walls >= 60 : t[type]) return 'This improvement is already complete.';
  } else if (type === 'city') { if (t.building !== 'town') return 'Select a town to upgrade.'; }
  else if (type !== 'road' && t.building) return 'This tile already has a building.';
  if (b.terrain && !b.terrain.includes(t.terrain)) return `Requires ${b.terrain.join(' or ')} terrain.`;
  if (b.resource && t.resource !== b.resource) return `Requires a ${b.resource} deposit.`;
  if (type === 'town' && settlements(s).some(c => distance(c, t) < 4)) return 'Towns must be at least 4 hexes apart.';
  if (type === 'town' && k.population < 45) return 'At least 45 population is needed to found another town.';
  if (!canAfford(k, b.cost)) return 'Insufficient resources.';
  return null;
}
export function build(s, owner, id, type) {
  const error = buildCheck(s, owner, id, type); if (error) return { ok: false, error };
  const k = kingdom(s, owner); pay(k, BUILDINGS[type].cost); k.commands--;
  s.tiles[id].owner = owner;
  s.tiles[id].project = { type, remaining: BUILDINGS[type].turns, owner };
  return { ok: true };
}
export function recruit(s, owner, id, type) {
  const t = s.tiles[id], k = kingdom(s, owner), u = UNITS[type];
  if (s.outcome || !u || !k || t?.owner !== owner || !['city', 'town', 'fort'].includes(t.building)) return { ok: false, error: 'Muster at an owned town, city or fort.' };
  if (s.armies.some(a => a.tile === id && atWar(s, owner, a.owner))) return { ok: false, error: 'The muster ground is under attack.' };
  if (k.commands < 1 || k.population - u.count < 20 || !canAfford(k, u.cost)) return { ok: false, error: 'Need an order, resources, and enough population to leave 20 civilians.' };
  let army = s.armies.find(a => a.owner === owner && a.tile === id);
  if (!army) { army = { id: `army-${s.nextId++}`, owner, tile: id, units: { levy: 0, archer: 0, cavalry: 0, siege: 0 }, morale: 1, order: 'hold', path: [], target: null }; s.armies.push(army); }
  pay(k, u.cost); k.population -= u.count; k.commands--; army.units[type] += u.count;
  return { ok: true, armyId: army.id };
}
export function canEnter(s, owner, t) { return !!(passable(t) && (!t.owner || t.owner === owner || atWar(s, owner, t.owner) || treaty(s, owner, t.owner, 'alliance') || treaty(s, owner, t.owner, 'vassalage'))); }
export function moveCost(a, b) { return (a.road && b.road ? .5 : TERRAINS[b.terrain].cost) + (a.river !== b.river && (a.river || b.river) && !(a.road && b.road) ? 1 : 0); }
export function findPath(s, startId, endId, owner, roadsOnly = false) {
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
      const exiting = exitOwner && n.owner === exitOwner && s.tiles[current].owner === exitOwner;
      if (!passable(n) || (roadsOnly ? !n.road || (n.owner && n.owner !== owner && !treaty(s, owner, n.owner, 'trade') && !treaty(s, owner, n.owner, 'alliance')) : !canEnter(s, owner, n) && !exiting)) continue;
      const cost = costs.get(current) + moveCost(s.tiles[current], n);
      if (cost >= (costs.get(n.id) ?? Infinity)) continue;
      costs.set(n.id, cost); came.set(n.id, current);
      // Minimum edge is 0.5: the heuristic must not overestimate road travel.
      open.push({ id: n.id, f: cost + distance(n, goal) * .5 });
    }
  }
  return [];
}
export function orderArmy(s, owner, armyId, targetId, order = 'move') {
  const a = s.armies.find(a => a.id === armyId && a.owner === owner);
  if (s.outcome || !a || !['move', 'attack', 'retreat', 'hold'].includes(order)) return { ok: false, error: 'Select one of your armies.' };
  if (order === 'hold' || targetId === a.tile) { a.path = []; a.target = null; a.order = 'hold'; return { ok: true }; }
  const path = findPath(s, a.tile, targetId, owner);
  if (!path.length) return { ok: false, error: 'No legal route. Neutral borders require an alliance or a declaration of war.' };
  a.path = path; a.target = targetId; a.order = order;
  return { ok: true, path };
}
export function mergeArmies(s, owner, id) {
  if (s.outcome) return { ok: false, error: 'This campaign has ended.' };
  const group = armiesOf(s, owner).filter(a => a.tile === id);
  if (group.length < 2) return { ok: false, error: 'Bring two armies to the same tile first.' };
  const base = group[0];
  for (const a of group.slice(1)) { for (const u of Object.keys(UNITS)) base.units[u] += a.units[u]; s.armies = s.armies.filter(x => x !== a); }
  base.path = []; base.order = 'hold'; return { ok: true };
}
export function splitArmy(s, owner, armyId) {
  if (s.outcome) return { ok: false, error: 'This campaign has ended.' };
  const a = s.armies.find(a => a.id === armyId && a.owner === owner);
  if (!a || sizeOf(a) < 12) return { ok: false, error: 'At least 12 soldiers are needed to split an army.' };
  const b = { ...a, id: `army-${s.nextId++}`, units: {}, path: [], target: null, order: 'hold' };
  for (const u of Object.keys(UNITS)) { b.units[u] = Math.floor(a.units[u] / 2); a.units[u] -= b.units[u]; }
  a.path = []; a.order = 'hold'; s.armies.push(b); return { ok: true, armyId: b.id };
}
export function strength(a, defending = false, t = null) {
  let total = Object.entries(a.units).reduce((n, [type, count]) => n + count * UNITS[type][defending ? 'defense' : 'attack'], 0) * a.morale;
  if (defending && t) total *= TERRAINS[t.terrain].defense * (t.building === 'fort' ? 1.6 : 1) * (t.walls > 0 ? 1.6 : 1) * (['fort', 'city'].includes(t.building) ? 1 + Math.min(.25, a.units.archer / Math.max(1, sizeOf(a))) : 1);
  if (!defending && t?.terrain === 'plains') total *= 1 + Math.min(.25, a.units.cavalry / Math.max(1, sizeOf(a)));
  return total;
}
function casualties(a, ratio) { for (const type of Object.keys(UNITS)) a.units[type] = Math.max(0, a.units[type] - Math.ceil(a.units[type] * ratio)); }
function retreat(s, a, from) {
  const tiles = neighbors(s, from).filter(t => canEnter(s, a.owner, t) && t.id !== a.tile && !s.armies.some(e => e.tile === t.id && atWar(s, e.owner, a.owner)));
  tiles.sort((x, y) => Number(y.owner === a.owner) - Number(x.owner === a.owner));
  if (tiles[0]) { a.tile = tiles[0].id; a.path = []; a.order = 'hold'; a.morale = .75; }
  else casualties(a, .6);
}
function battle(s, attacker, defender, t) {
  s.militaryEvents.push({ turn: s.turn, attacker: attacker.owner, defender: defender.owner, tile: t.id, action: 'battle' });
  const A = strength(attacker, false, t), D = strength(defender, true, t), won = random(s) < A / Math.max(1, A + D);
  const before = sizeOf(attacker) + sizeOf(defender);
  casualties(attacker, won ? .12 + .13 * D / Math.max(1, A + D) : .28 + .2 * D / Math.max(1, A + D));
  casualties(defender, won ? .28 + .2 * A / Math.max(1, A + D) : .12 + .13 * A / Math.max(1, A + D));
  if (won && sizeOf(defender) < sizeOf(attacker) * .55) retreat(s, defender, t);
  log(s, `${kingdom(s, attacker.owner).name} clashes with ${kingdom(s, defender.owner).name} at ${t.name || t.id}; ${before - sizeOf(attacker) - sizeOf(defender)} casualties.`, 'battle');
  s.armies = s.armies.filter(a => sizeOf(a) > 0);
}
function capture(s, a, t) {
  if (!t.owner || t.owner === a.owner || !atWar(s, a.owner, t.owner)) return true;
  if (t.walls > 0) {
    s.militaryEvents.push({ turn: s.turn, attacker: a.owner, defender: t.owner, tile: t.id, action: 'siege' });
    t.walls = Math.max(0, t.walls - Math.max(2, a.units.siege * 9));
    casualties(a, a.units.siege ? .015 : .06);
    log(s, `${kingdom(s, a.owner).name} besieges ${t.name || t.id}: ${t.walls} wall strength remains.`, 'battle');
    return false;
  }
  if (['city', 'town', 'fort'].includes(t.building)) {
    const defense = t.building === 'city' ? 14 : 8;
    if (strength(a, false, t) < defense) { casualties(a, .12); return false; }
    casualties(a, .08); const previous = t.owner;
    s.militaryEvents.push({ turn: s.turn, attacker: a.owner, defender: previous, tile: t.id, action: 'capture' });
    t.owner = a.owner; t.project = null;
    log(s, `${kingdom(s, a.owner).name} captures ${t.name || 'a fort'} from ${kingdom(s, previous).name}.`, 'war');
    remember(s, previous, `${kingdom(s, a.owner).name} captured ${t.name || t.id}.`, 10);
    rebuildTerritory(s);
  } else { t.owner = a.owner; t.project = null; }
  return true;
}
function zoneOfControl(s, a, t) {
  return neighbors(s, t).some(n => (n.building === 'fort' && n.owner && atWar(s, a.owner, n.owner)) || s.armies.some(e => e.tile === n.id && atWar(s, a.owner, e.owner)));
}
export function resolveMovement(s) {
  // Stable, alternating initiative avoids one house always moving first.
  const armies = [...s.armies].sort((a, b) => a.id.localeCompare(b.id));
  if (s.turn % 2 === 0) armies.reverse();
  for (const a of armies) {
    if (!s.armies.includes(a) || sizeOf(a) === 0) continue;
    const startingBudget = a.units.siege ? 2 : a.units.cavalry === sizeOf(a) ? 5 : 3;
    let budget = startingBudget;
    while (a.path.length && budget > 0) {
      const t = s.tiles[a.path[0]], from = s.tiles[a.tile];
      const exiting = from.owner && !canEnter(s, a.owner, from) && s.tiles[a.target]?.owner !== from.owner && t?.owner === from.owner;
      if ((!canEnter(s, a.owner, t) && !exiting) || !t || distance(from, t) !== 1) { a.path = []; break; }
      // A slow siege stack can spend its entire turn crossing one costly edge.
      const cost = moveCost(from, t); if (cost > budget && budget !== startingBudget) break;
      const enemy = s.armies.find(e => e.tile === t.id && atWar(s, a.owner, e.owner));
      if (enemy) { battle(s, a, enemy, t); break; }
      if (!capture(s, a, t)) break;
      a.tile = t.id; a.path.shift(); budget -= cost;
      if (zoneOfControl(s, a, t)) break;
    }
    if (!a.path.length) a.order = 'hold';
    a.morale = Math.min(1, a.morale + .04);
  }
  s.armies = s.armies.filter(a => sizeOf(a) > 0);
  s.militaryEvents = s.militaryEvents.slice(-100);
}

export function economyProjection(s, owner) {
  const k = kingdom(s, owner), income = Object.fromEntries(RESOURCES.map(r => [r, 0])), towns = settlements(s, owner);
  for (const t of Object.values(s.tiles).filter(t => t.owner === owner)) {
    const output = BUILDINGS[t.building]?.yield || {};
    for (const [r, n] of Object.entries(output)) income[r] += n + (t.building === 'farm' && t.resource === 'food' ? 4 : 0);
    if (['town', 'city'].includes(t.building)) { income.food += t.building === 'city' ? 14 : 8; income.gold += t.building === 'city' ? 8 : 5; }
    if (t.market) income.gold += 12;
    if (t.workshop) income.iron += 3;
    if (t.building) income.gold -= 1;
  }
  income.gold += Math.floor(k.population * ({ low: .08, medium: .17, high: .28 }[k.tax]));
  income.food -= Math.ceil(k.population / 12);
  for (const a of armiesOf(s, owner)) { income.food -= Math.ceil(sizeOf(a) / 6); income.gold -= Math.ceil(sizeOf(a) / 9); }
  // Each connected pair pays once to each eligible kingdom, never per path tile.
  const partners = settlements(s).filter(t => t.owner === owner || treaty(s, owner, t.owner, 'trade'));
  const seen = new Set(); let routes = 0;
  for (const a of towns) for (const b of partners) {
    const key = pair(a.id, b.id);
    if (a.id === b.id || seen.has(key) || !a.road || !b.road) continue;
    seen.add(key);
    if (findPath(s, a.id, b.id, owner, true).length) { income.gold += b.owner === owner ? 6 : 10; routes++; }
  }
  return { income, routes };
}
export function resolveEconomy(s) {
  for (const t of Object.values(s.tiles)) if (t.project) {
    if (t.project.owner !== t.owner) { t.project = null; continue; }
    if (--t.project.remaining <= 0) {
      const type = t.project.type;
      if (type === 'wall') t.walls = 60;
      else if (['road', 'market', 'workshop'].includes(type)) t[type] = true;
      else { t.building = type; if (['town', 'city'].includes(type)) { t.road = true; t.name ||= `Outpost ${t.q}.${t.r}`; } }
      if (t.owner === PLAYER) log(s, `${BUILDINGS[type].name} completed at ${t.name || t.id}.`, 'economy');
      t.project = null;
    }
  }
  rebuildTerritory(s);
  for (const k of s.kingdoms) {
    if (!alive(s, k.id)) continue;
    const { income } = economyProjection(s, k.id);
    for (const r of RESOURCES) k.resources[r] += income[r];
    const deficit = k.resources.food < 0 || k.resources.gold < 0;
    if (deficit) {
      for (const a of armiesOf(s, k.id)) casualties(a, .06);
      k.happiness = Math.max(5, k.happiness - 6); k.population = Math.max(20, k.population - 3);
      if (k.id === PLAYER) log(s, 'Food or gold ran out. Soldiers deserted and population fell.', 'economy');
    } else {
      k.happiness = Math.max(5, Math.min(100, k.happiness + (k.tax === 'high' ? -3 : k.tax === 'low' ? 3 : 1)));
      const cap = settlements(s, k.id).reduce((n, t) => n + (t.building === 'city' ? 150 : 80), 0);
      if (k.resources.food > 20 && k.happiness >= 35) k.population = Math.min(cap, k.population + (k.tax === 'low' ? 4 : k.tax === 'high' ? 1 : 2));
    }
    for (const r of RESOURCES) k.resources[r] = Math.max(0, Math.min(99999, k.resources[r]));
    k.commands = commandLimit(s, k.id);
  }
  s.armies = s.armies.filter(a => sizeOf(a) > 0);
}

export function strategicThreat(s, owner, t) {
  return s.armies.reduce((value, a) => value + (a.owner === owner ? -1 : atWar(s, a.owner, owner) ? 1 : 0) * strength(a) / (1 + distance(t, s.tiles[a.tile])), 0);
}
function militaryTarget(s, k, a) {
  const pledge = s.pledges.find(p => p.debtor === k.id && p.status === 'pending' && ['DEFEND', 'POSITION', 'WITHDRAW', 'JOINT_WAR', 'BUILD_DEFENSES'].includes(p.intent.type));
  if (pledge) {
    const intent = pledge.intent;
    if (intent.type === 'JOINT_WAR') return settlements(s, intent.targetId).sort((x, y) => distance(s.tiles[a.tile], x) - distance(s.tiles[a.tile], y))[0];
    if (intent.type === 'WITHDRAW') return settlements(s, k.id)[0];
    return s.tiles[intent.targetId];
  }
  const threatened = settlements(s, k.id).filter(t => strategicThreat(s, k.id, t) > 3).sort((x, y) => strategicThreat(s, k.id, y) - strategicThreat(s, k.id, x));
  if (threatened[0]) { k.goal = 'DEFEND'; return threatened[0]; }
  const enemies = s.armies.filter(e => atWar(s, k.id, e.owner));
  const targets = [...enemies.map(e => s.tiles[e.tile]), ...settlements(s).filter(t => atWar(s, k.id, t.owner))];
  targets.sort((x, y) => distance(s.tiles[a.tile], x) - distance(s.tiles[a.tile], y));
  const target = targets.find(t => strength(a) >= Math.max(10, strategicThreat(s, k.id, t)) * (1.35 - k.aggression * .5));
  if (target) k.goal = 'ATTACK';
  return target;
}
export function strategyTurn(s) {
  for (const k of s.kingdoms.filter(k => k.id !== PLAYER && alive(s, k.id))) {
    const owned = Object.values(s.tiles).filter(t => t.owner === k.id && passable(t));
    k.goal = 'ECONOMY';
    const defensePledge = s.pledges.find(p => p.debtor === k.id && p.status === 'pending' && p.intent.type === 'BUILD_DEFENSES');
    if (defensePledge) build(s, k.id, defensePledge.intent.targetId, 'fort');
    const projection = economyProjection(s, k.id).income;
    const priority = projection.food < 3 ? 'farm' : k.resources.wood < 60 ? 'lumber' : k.resources.stone < 60 ? 'quarry' : k.resources.iron < 30 ? 'mine' : null;
    if (priority) {
      const candidates = owned.filter(t => !buildCheck(s, k.id, t.id, priority)).sort((a, b) => strategicThreat(s, k.id, a) - strategicThreat(s, k.id, b));
      if (candidates[0]) build(s, k.id, candidates[0].id, priority);
    }
    if (s.turn % 5 === 0 && settlements(s, k.id).length < 4) {
      const candidate = Object.values(s.tiles).find(t => !buildCheck(s, k.id, t.id, 'town'));
      if (candidate) { build(s, k.id, candidate.id, 'town'); k.goal = 'EXPAND'; }
    }
    const home = settlements(s, k.id)[0];
    if (s.turn % 3 === 0 && armiesOf(s, k.id).reduce((n, a) => n + sizeOf(a), 0) < 90) recruit(s, k.id, home.id, s.turn % 9 === 0 ? 'siege' : s.turn % 6 === 0 ? 'archer' : 'levy');
    if (s.turn > 9 && s.turn % 8 === HOUSES.findIndex(h => h.id === k.id) && k.aggression > .5) {
      const rival = s.kingdoms.filter(e => e.id !== k.id && alive(s, e.id) && !treaty(s, k.id, e.id) && !atWar(s, k.id, e.id)).sort((a, b) => relation(s, k.id, a.id).opinion - relation(s, k.id, b.id).opinion)[0];
      if (rival && armiesOf(s, k.id).reduce((n, a) => n + strength(a), 0) > armiesOf(s, rival.id).reduce((n, a) => n + strength(a), 0) * .8) declareWar(s, k.id, rival.id);
    }
    for (const a of armiesOf(s, k.id)) {
      const target = militaryTarget(s, k, a);
      if (target) orderArmy(s, k.id, a.id, target.id, 'move');
    }
  }
}
export function checkVictory(s) {
  if (!alive(s, PLAYER)) { s.outcome = { won: false, reason: 'Your last settlement has fallen. Your house survives in the chronicles.' }; return; }
  const cities = settlements(s), owned = settlements(s, PLAYER);
  if (owned.length >= Math.ceil(cities.length * .6)) s.outcome = { won: true, reason: 'Conquest: your banners fly above at least 60% of all settlements.' };
  const rivals = s.kingdoms.filter(k => k.id !== PLAYER && alive(s, k.id));
  const supporters = rivals.filter(k => treaty(s, PLAYER, k.id, 'alliance') || treaty(s, PLAYER, k.id, 'vassalage'));
  if (supporters.length > rivals.length / 2 && supporters.length > 0) s.diplomaticTurns++;
  else s.diplomaticTurns = 0;
  if (s.diplomaticTurns >= 3) s.outcome = { won: true, reason: 'The Crown Accord: a majority of surviving houses recognizes your leadership for three turns.' };
}

export function parseSave(raw) {
  if (typeof raw !== 'string' || raw.length > 2000000) throw new Error('Save is too large or unreadable.');
  const s = JSON.parse(raw);
  const number = (n, min = 0, max = 100000) => Number.isFinite(n) && n >= min && n <= max;
  if (s?.version !== SAVE_VERSION || s.width !== 40 || s.height !== 30 || !Number.isInteger(s.turn) || !number(s.turn, 1) || !number(s.rng, 0, 4294967295) || !number(s.nextId, 1) || !s.tiles || Object.keys(s.tiles).length !== 1200 || !Array.isArray(s.kingdoms) || s.kingdoms.length !== 6) throw new Error('Unsupported or damaged campaign save.');
  for (const h of HOUSES) {
    const k = kingdom(s, h.id);
    if (!k || !RESOURCES.every(r => number(k.resources?.[r])) || !['low', 'medium', 'high'].includes(k.tax) || !number(k.population) || !number(k.commands, 0, 8) || !number(k.happiness, 0, 100) || !Array.isArray(k.memories) || k.memories.length > 30 || k.memories.some(m => typeof m.text !== 'string' || m.text.length > 1000 || !number(m.turn) || !number(m.importance, 0, 10)) || typeof k.memorySummary !== 'string' || k.memorySummary.length > 900) throw new Error('Damaged kingdom data.');
    for (const other of HOUSES.filter(o => o.id !== h.id)) if (!number(k.relations?.[other.id]?.opinion, -100, 100) || !number(k.relations?.[other.id]?.trust, -100, 100)) throw new Error('Damaged diplomacy data.');
    Object.assign(k, h); // Presentation/personality comes from trusted content, not save strings.
  }
  for (let r = 0; r < 30; r++) for (let q = 0; q < 40; q++) {
    const t = s.tiles[tileId(q, r)];
    if (!t || t.id !== tileId(q, r) || t.q !== q || t.r !== r || !Object.hasOwn(TERRAINS, t.terrain) || (t.owner && !kingdom(s, t.owner)) || (t.building && !['city', 'town', 'fort', 'farm', 'lumber', 'quarry', 'mine'].includes(t.building)) || !number(t.walls, 0, 60)) throw new Error('Damaged map data.');
    if (t.project && (!Object.hasOwn(BUILDINGS, t.project.type) || !number(t.project.remaining, 1, 4) || !kingdom(s, t.project.owner))) throw new Error('Damaged construction data.');
  }
  if (!Array.isArray(s.armies) || s.armies.length > 500 || new Set(s.armies.map(a => a.id)).size !== s.armies.length) throw new Error('Damaged army data.');
  for (const a of s.armies) if (!kingdom(s, a.owner) || !s.tiles[a.tile] || !number(a.morale, .1, 1) || !Object.keys(UNITS).every(u => Number.isInteger(a.units?.[u]) && number(a.units[u])) || !Array.isArray(a.path) || a.path.length > 1200 || a.path.some(id => !s.tiles[id])) throw new Error('Damaged army orders.');
  for (const list of ['treaties', 'pledges', 'wars', 'events']) if (!Array.isArray(s[list]) || s[list].length > 1000) throw new Error('Damaged campaign history.');
  for (const t of s.treaties) if (!['alliance', 'peace', 'trade', 'vassalage'].includes(t.type) || t.parties?.length !== 2 || t.parties.some(id => !kingdom(s, id)) || !number(t.expires)) throw new Error('Damaged treaty data.');
  for (const p of s.pledges) if (!kingdom(s, p.debtor) || !kingdom(s, p.creditor) || !p.intent || !INTENT_TYPES.includes(p.intent.type) || !number(p.intent.giveAmount, 0, 1000) || !RESOURCES.includes(p.intent.giveResource) || !number(p.deadline) || !number(p.created) || !number(p.held, 0, 2) || !['pending', 'fulfilled', 'broken'].includes(p.status)) throw new Error('Damaged pledge data.');
  if (s.outcome && (typeof s.outcome.won !== 'boolean' || typeof s.outcome.reason !== 'string')) throw new Error('Damaged result.');
  if (!Array.isArray(s.militaryEvents) || s.militaryEvents.length > 100 || s.militaryEvents.some(e => !kingdom(s, e.attacker) || !kingdom(s, e.defender) || !number(e.turn) || !s.tiles[e.tile])) throw new Error('Damaged military history.');
  if (!s.conversations || typeof s.conversations !== 'object' || !number(s.diplomaticTurns, 0, 3)) throw new Error('Damaged campaign data.');
  for (const history of Object.values(s.conversations)) if (!Array.isArray(history) || history.length > 60 || history.some(m => typeof m.text !== 'string' || m.text.length > 2000 || !['player', 'ruler', 'council'].includes(m.role))) throw new Error('Damaged conversation data.');
  return s;
}
