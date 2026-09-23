import { BUILDINGS, HOUSES, QUALITY, REGIONS, RESOURCES, RESOURCE_VALUES, UNITS } from './data.mjs';
import { buildingLevel, constructionSpec, fortMaximum, tileProduction } from './economy.mjs';
import { chooseFormation, familyCount } from './warfare.mjs';
import { PLAYER, alive, armiesOf, atWar, build, buildCheck, canAfford, canEnter, declareWar, distance, economyProjection, findPath, kingdom, log, makePeace, mergeArmies, neighbors, orderArmy, passable, recruit, recruitCheck, recruitmentCost, relation, settlements, sizeOf, strength, treaty } from './core.mjs';

export const STRATEGY_GOALS = {
  ECONOMY: 'Developing the economy', RECOVER: 'Securing supplies', DEFEND: 'Defending the realm',
  GUARD_FRONTIER: 'Guarding the frontier', ATTACK: 'Conducting a campaign', MUSTER: 'Gathering an army',
  SUPPORT_ALLY: 'Supporting an ally', HONOR_PLEDGE: 'Honoring an agreement', EXPAND: 'Expanding the realm',
  ELIMINATED: 'House defeated'
};
const HISTORY_LIMIT = 6, ACTION_LIMIT = 32;
const troopCount = forces => forces.reduce((n, a) => n + sizeOf(a), 0);
const forcePower = forces => forces.reduce((n, a) => n + strength(a), 0);
const nearby = (s, forces, tile, radius) => forces.filter(a => distance(s.tiles[a.tile], tile) <= radius);
const stableScore = (a, b) => b.score - a.score || a.tile.id.localeCompare(b.tile.id) || a.type.localeCompare(b.type);

export function initializeStrategy(s) { s.strategy ??= { lastTurn: 0, history: [] }; }
function reportFor(s, owner) {
  const round = s.strategy?.history.at(-1);
  return round?.turn === s.turn ? round.houses.find(h => h.owner === owner) : null;
}
export function recordStrategyAction(s, owner, action) {
  const report = reportFor(s, owner);
  if (!report) return;
  if (report.actions.length < ACTION_LIMIT) report.actions.push(action);
  else report.omitted++;
}

function assess(s, k) {
  const tiles = Object.values(s.tiles).filter(t => t.owner === k.id && passable(t));
  const towns = settlements(s, k.id), forces = armiesOf(s, k.id);
  const enemies = s.armies.filter(a => atWar(s, k.id, a.owner));
  const enemyTowns = settlements(s).filter(t => atWar(s, k.id, t.owner));
  const threats = towns.map(tile => ({ tile, enemy: forcePower(nearby(s, enemies, tile, 5)), own: forcePower(nearby(s, forces, tile, 3)) }))
    .filter(t => t.enemy > Math.max(8, t.own * .75)).sort((a, b) => (b.enemy - b.own) - (a.enemy - a.own) || a.tile.id.localeCompare(b.tile.id));
  const wary = Object.entries(k.relations).filter(([id, r]) => alive(s, id) && r.wariness >= 30 && r.trust < 35)
    .sort((a, b) => b[1].wariness - a[1].wariness)[0]?.[0];
  const pledges = s.pledges.filter(p => p.debtor === k.id && p.status === 'pending' && !p.breached &&
    ['DEFEND', 'POSITION', 'WITHDRAW', 'JOINT_WAR', 'BUILD_DEFENSES'].includes(p.intent.type)).sort((a, b) => a.deadline - b.deadline);
  const { income } = economyProjection(s, k.id);
  // Reserve two deliveries of ratified supply obligations as well as upkeep.
  const reserves = Object.fromEntries(RESOURCES.map(r => [r, 0]));
  for (const t of s.treaties.filter(t => t.type === 'recurring' && t.expires > s.turn && t.parties.includes(k.id))) {
    const payer = t.payer === k.id;
    reserves[payer ? t.intent.giveResource : t.intent.receiveResource] += 2 * (payer ? t.intent.giveAmount : t.intent.receiveAmount);
  }
  reserves.food += Math.max(24, -income.food * 3);
  reserves.gold += Math.max(18, -income.gold * 3);
  const crisis = k.resources.food + income.food * 3 < 30 || k.resources.gold + income.gold * 3 < 20;
  return { tiles, towns, home: towns.find(t => t.capital === k.id) || towns[0], forces, enemies, enemyTowns, threats, wary, pledges, income, reserves, crisis, war: enemyTowns.length > 0 };
}

function chooseGoal(c) {
  if (c.threats.length) return ['DEFEND', 'Enemy forces threaten a settlement.'];
  if (c.pledges.length) return ['HONOR_PLEDGE', 'A ratified agreement takes priority.'];
  if (c.crisis) return ['RECOVER', 'Food and treasury reserves need rebuilding.'];
  if (c.war) return ['ATTACK', 'Gathering forces and seeking a favorable enemy target.'];
  if (c.wary) return ['GUARD_FRONTIER', 'Distrust of a neighboring House calls for stronger defenses.'];
  return ['ECONOMY', 'Developing production, trade and a sustainable army.'];
}

function protectedPeace(s, a, b) {
  return ['peace', 'non-aggression', 'alliance', 'vassalage'].some(type => treaty(s, a, b, type));
}
function considerWar(s, k, c) {
  if (s.turn < 10 || c.war || c.crisis || c.threats.length || troopCount(c.forces) < 30) return;
  const ours = forcePower(c.forces);
  const candidates = s.kingdoms.filter(o => o.id !== k.id && alive(s, o.id) && !protectedPeace(s, k.id, o.id)).map(o => {
    const r = relation(s, k.id, o.id), theirForces = armiesOf(s, o.id);
    const allyAttacked = s.kingdoms.some(ally => ally.id !== k.id && treaty(s, k.id, ally.id, 'alliance') &&
      relation(s, k.id, ally.id).trust >= 45 && atWar(s, o.id, ally.id));
    const motive = r.grievance >= 40 || r.opinion < -20 || k.aggression >= .75 && r.opinion < 10 || allyAttacked;
    if (!motive || r.dependency >= 30 || r.trust >= 45 || ours < forcePower(theirForces) * (1.45 - k.aggression * .4)) return null;
    const tile = settlements(s, o.id).sort((a, b) => distance(c.home, a) - distance(c.home, b))[0];
    return { type: o.id, tile, score: r.grievance - r.opinion + (allyAttacked ? 35 : 0) - distance(c.home, tile) * 3 };
  }).filter(Boolean).sort(stableScore);
  for (const candidate of candidates.slice(0, 2)) {
    // Probe geography without crossing a third party's closed borders. The
    // hypothetical war is read-only; real declarations use the shared rules.
    const probe = { ...s, wars: [...s.wars, [k.id, candidate.type].sort().join(':')] };
    if (!findPath(probe, c.home.id, candidate.tile.id, k.id).length) continue;
    if (declareWar(s, k.id, candidate.type)) recordStrategyAction(s, k.id, { kind: 'war', house: candidate.type });
    break;
  }
}

function canSpend(k, c, cost, essential = false) {
  return canAfford(k, cost) && Object.entries(cost).every(([r, n]) =>
    k.resources[r] - n >= (essential && r === 'gold' ? 0 : c.reserves[r]));
}
function buildingCandidates(s, k, c) {
  const pending = type => c.tiles.filter(t => t.project?.type === type).length;
  const totalLevel = type => c.tiles.reduce((n, t) => n + buildingLevel(t, type), 0);
  const highestLevel = type => Math.max(0, ...c.tiles.map(t => buildingLevel(t, type)));
  const tier = s.turn < 8 ? 1 : s.turn < 20 ? 2 : 3;
  const specialty = REGIONS[k.id].troops;
  const fortifications = c.enemyTowns.some(t => t.walls > 0 || fortMaximum(t));
  const need = (resource, stock, flow) => c.income[resource] < 0 ? 105 + Math.min(35, -c.income[resource] * 3) :
    k.resources[resource] + c.income[resource] * 2 < stock ? 85 : c.income[resource] < flow ? 48 : 0;
  const weights = {
    farm: need('food', 70, 5), lumber: need('wood', 60, 6), quarry: need('stone', 50, 5), mine: need('iron', 35, 4),
    ranch: specialty === 'cavalry' ? need('horses', 20, 3) : 0,
    workshop: !totalLevel('workshop') ? 98 : need('tools', 22, 3),
    armory: !totalLevel('armory') ? 60 : need('arms', 16, 3),
    market: need('gold', 80, 6) || (k.greed > .7 ? 35 : 15),
    tradeOutpost: totalLevel('tradeOutpost') < (s.turn > 16 ? 3 : 1) ? 35 + k.greed * 15 : 0,
    harbor: !totalLevel('harbor') ? 35 + k.greed * 15 : 0,
    barracks: highestLevel('barracks') < tier ? (c.war ? 90 : 50) : 0,
    range: specialty === 'archer' && highestLevel('range') < tier ? (c.war ? 95 : 65) : 0,
    stable: specialty === 'cavalry' && highestLevel('stable') < tier ? (c.war ? 95 : 65) : 0,
    siegeWorks: c.war && fortifications && highestLevel('siegeWorks') < (c.enemyTowns.some(t => t.walls >= 120) ? 3 : 1) ? 140 : 0,
    wall: c.threats.length || c.wary ? 220 : c.war ? 75 : 0,
    watchtower: (c.war || c.wary) && totalLevel('watchtower') < c.towns.length ? 42 : 0,
    envoyOffice: s.turn >= 4 && !totalLevel('envoyOffice') ? 28 + k.honor * 10 : 0,
    chancery: s.turn >= 16 && !totalLevel('chancery') ? 22 : 0,
    city: s.turn >= 12 && !c.crisis ? 32 : 0,
    storehouse: RESOURCES.some(r => r !== 'gold' && k.resources[r] > 540) && totalLevel('storehouse') < c.towns.length ? 28 : 0,
    town: !c.crisis && !c.threats.length && k.population >= 70 && c.towns.length + pending('town') < Math.min(6, 2 + Math.floor(s.turn / 12)) ? 62 + k.ambition * 15 : 0
  };
  const defensePledge = c.pledges.find(p => p.intent.type === 'BUILD_DEFENSES');
  const candidates = [];
  const sites = weights.town ? [...c.tiles, ...Object.values(s.tiles).filter(t => !t.owner && passable(t) && neighbors(s, t).some(n => n.owner === k.id))] : c.tiles;
  for (const [type, weight] of Object.entries(weights)) {
    if (!weight || pending(type)) continue;
    for (const tile of sites) {
      if (tile.owner !== k.id && type !== 'town') continue;
      if (type === 'wall' && (buildingLevel(tile, type) >= (c.threats.length ? 3 : 1) && tile.walls >= buildingLevel(tile, type) * 60)) continue;
      if (['barracks', 'range', 'stable'].includes(type) && buildingLevel(tile, type) >= tier) continue;
      const error = buildCheck(s, k.id, tile.id, type);
      if (error && !error.startsWith('Missing ')) continue;
      const spec = constructionSpec(tile, type);
      const costWeight = Object.entries(spec.cost).reduce((n, [r, v]) => n + v * RESOURCE_VALUES[r], 0);
      const quality = QUALITY[tile.quality] || 1;
      let score = weight - costWeight / 35 - distance(c.home, tile) * .2;
      if (['barracks','range','stable','siegeWorks'].includes(type) && buildingLevel(tile, type)) score += 20;
      if (['farm','lumber','quarry','mine','ranch'].includes(type)) {
        const copy = { ...tile, building: type, levels: { ...tile.levels, [type]: spec.level } };
        const r = Object.keys(BUILDINGS[type].yield)[0];
        score += (tileProduction(copy, k.id)[r] - tileProduction(tile, k.id)[r]) * .7;
      }
      if (type === 'town') score += neighbors(s, tile).reduce((n, t) => n + (t.resource ? QUALITY[t.quality] || 1 : 0), 0) + quality;
      if (type === 'wall') score += c.threats.find(t => t.tile.id === tile.id)?.enemy || (tile.id === c.home.id ? 5 : 0);
      if (nearby(s, c.enemies, tile, 2).length && !['wall','watchtower'].includes(type)) score -= 90;
      candidates.push({ type, tile, score, cost: spec.cost, level: spec.level, kind: 'build', essential: type === 'farm' && c.crisis || type === 'market' && c.income.gold < 0 });
    }
  }
  if (defensePledge) {
    const tile = s.tiles[defensePledge.intent.targetId], spec = constructionSpec(tile, 'fort');
    const error = buildCheck(s, k.id, tile?.id, 'fort');
    if (spec && (!error || error.startsWith('Missing '))) candidates.push({ type: 'fort', tile, score: 260, cost: spec.cost, level: spec.level, kind: 'build', essential: true });
  }
  // Fill gaps in owned settlement connections; never spend orders paving a
  // disconnected foreign corridor or endlessly upgrading existing roads.
  if (!pending('road') && c.towns.length > 1) {
    const target = c.towns.find(t => t.id !== c.home.id && !findPath(s, c.home.id, t.id, k.id, true).length);
    if (target) for (const id of findPath(s, c.home.id, target.id, k.id)) {
      const tile = s.tiles[id];
      if (tile.road || buildCheck(s, k.id, id, 'road')) continue;
      const spec = constructionSpec(tile, 'road');
      candidates.push({ type: 'road', tile, score: 58, cost: spec.cost, level: spec.level, kind: 'build' });
      break;
    }
  }
  return candidates.sort(stableScore);
}

function recruitmentCandidates(s, k, c, recruited) {
  if (recruited >= (c.war || c.threats.length ? 2 : 1) || k.population < (c.threats.length ? 28 : 42)) return [];
  const count = troopCount(c.forces);
  const weakestDefense = c.enemyTowns.length ? Math.min(...c.enemyTowns.map(t => c.enemies.filter(e => e.tile === t.id).reduce((n, e) => n + strength(e, true, t), 0) + 14)) : 0;
  const desired = c.war ? Math.min(200, Math.max(60, troopCount(c.enemies) * 1.1, weakestDefense * 1.3 / Math.max(.8, forcePower(c.forces) / Math.max(1, count)))) : 24 + c.towns.length * 12;
  if (!c.threats.length && (c.crisis || c.income.food < 0 && k.resources.food < 100)) return [];
  const totals = Object.fromEntries(['infantry','ranged','mounted','siege'].map(f => [f, c.forces.reduce((n, a) => n + familyCount(a, f), 0)]));
  const specialty = REGIONS[k.id].troops, mountedEnemy = c.enemies.reduce((n, a) => n + familyCount(a, 'mounted'), 0) > troopCount(c.enemies) * .25;
  const proportions = specialty === 'cavalry' ? {infantry:.45,ranged:.15,mounted:.4} : specialty === 'archer' ? {infantry:.45,ranged:.45,mounted:.1} : {infantry:.65,ranged:.25,mounted:.1};
  const siegeNeeded = c.war && c.enemyTowns.some(t => t.walls > 0 || fortMaximum(t)) && totals.siege < 4 && totals.infantry >= 12;
  const candidates = [];
  for (const tile of c.tiles.filter(t => ['town','city','fort'].includes(t.building))) for (const [type, unit] of Object.entries(UNITS)) {
    if (unit.legacy || recruitCheck(s, k.id, tile.id, type) || unit.family === 'siege' && !siegeNeeded || unit.family !== 'siege' && count >= desired) continue;
    const shortage = (proportions[unit.family] || 0) - totals[unit.family] / Math.max(1, count);
    const score = (c.threats.length ? 145 : c.war ? 90 : 52) + shortage * 60 + unit.attack * 2 +
      (unit.family === 'siege' && siegeNeeded ? 45 + (unit.breach || 0) : 0) + (type === 'spearman' && mountedEnemy ? 25 : 0);
    candidates.push({ kind: 'recruit', type, tile, cost: recruitmentCost(tile, type), score, essential: c.threats.length > 0 });
  }
  return candidates.sort(stableScore);
}

function develop(s, k) {
  let recruited = 0;
  const available = k.commands;
  for (let n = 0; n < available && k.commands > 0; n++) {
    const c = assess(s, k), buildings = buildingCandidates(s, k, c);
    // Keep the best future project for the trade planner even when an
    // affordable alternative is the right use of this round's resources.
    k.economicPlan = buildings[0] ? { type: buildings[0].type, tile: buildings[0].tile.id } : null;
    const options = [...buildings, ...recruitmentCandidates(s, k, c, recruited)].sort(stableScore);
    const choice = options.find(o => canSpend(k, c, o.cost, o.essential) && (o.kind !== 'build' || !buildCheck(s, k.id, o.tile.id, o.type)));
    if (!choice) break;
    const before = troopCount(armiesOf(s, k.id));
    const result = choice.kind === 'build' ? build(s, k.id, choice.tile.id, choice.type) : recruit(s, k.id, choice.tile.id, choice.type);
    if (!result.ok) break;
    if (choice.kind === 'recruit') recruited++;
    recordStrategyAction(s, k.id, choice.kind === 'build' ? { kind: 'build', tile: choice.tile.id, building: choice.type, level: choice.level } :
      { kind: 'recruit', tile: choice.tile.id, unit: choice.type, count: troopCount(armiesOf(s, k.id)) - before });
    if (choice.type === 'town' && k.goal === 'ECONOMY') k.goal = 'EXPAND';
  }
  const report = reportFor(s, k.id);
  report.orders = available - k.commands;
}

function command(s, k, a, target, order = 'move', avoid = null) {
  const result = orderArmy(s, k.id, a.id, target.id, order, avoid);
  if (!result.ok) return false;
  a.formation = chooseFormation(a, s.armies.find(e => e.tile === target.id && atWar(s, k.id, e.owner)), target);
  recordStrategyAction(s, k.id, { kind: target.id === a.tile ? 'hold' : 'march', army: a.id, from: a.tile, tile: target.id });
  return true;
}
function directArmies(s, k, c) {
  for (const tile of new Set(c.forces.map(a => a.tile))) if (armiesOf(s, k.id).filter(a => a.tile === tile).length > 1) {
    mergeArmies(s, k.id, tile); recordStrategyAction(s, k.id, { kind: 'merge', tile });
  }
  c.forces = armiesOf(s, k.id);
  const assignedPledges = new Set();
  for (const a of [...c.forces].sort((a, b) => strength(b) - strength(a) || a.id.localeCompare(b.id))) {
    const location = s.tiles[a.tile], nearestEnemy = [...c.enemies].sort((x, y) => distance(location, s.tiles[x.tile]) - distance(location, s.tiles[y.tile]))[0];
    a.formation = chooseFormation(a, nearestEnemy, location);
    const safeTowns = c.towns.filter(t => forcePower(nearby(s, c.enemies, t, 1)) < strength(a, true, t)).sort((x, y) => distance(location, x) - distance(location, y));
    const refuge = safeTowns[0] || c.home;
    if (a.morale < .55 || !canEnter(s, k.id, location)) {
      if (!command(s, k, a, refuge, 'retreat')) command(s, k, a, location, 'hold');
      continue;
    }
    const pledge = c.pledges.find(p => p.intent.type !== 'BUILD_DEFENSES' && (!assignedPledges.has(p.id) || p.intent.type === 'WITHDRAW'));
    if (pledge) {
      const target = pledge.intent.type === 'WITHDRAW' ? refuge : pledge.intent.type === 'JOINT_WAR' ?
        settlements(s, pledge.intent.targetId).sort((x, y) => distance(location, x) - distance(location, y))[0] : s.tiles[pledge.intent.targetId];
      if (target && command(s, k, a, target)) { assignedPledges.add(pledge.id); continue; }
    }
    if (c.threats.length) {
      const danger = c.threats[0], invader = nearby(s, c.enemies, danger.tile, 5).sort((x, y) => distance(location, s.tiles[x.tile]) - distance(location, s.tiles[y.tile]))[0];
      const intercept = invader && !s.tiles[invader.tile].walls && strength(a) > strength(invader, true, s.tiles[invader.tile]) * 1.2;
      if (command(s, k, a, intercept ? s.tiles[invader.tile] : danger.tile, intercept ? 'attack' : 'move')) continue;
    }
    if (c.war) {
      const avoid = new Set(Object.values(s.tiles).filter(t => atWar(s, k.id, t.owner) &&
        (t.walls > 0 || (t.fortIntegrity ?? fortMaximum(t)) > 0) && familyCount(a, 'siege') < 2).map(t => t.id));
      for (const enemy of c.enemies) if (strength(enemy, true, s.tiles[enemy.tile]) > strength(a)) avoid.add(enemy.tile);
      const candidates = [...new Set([...c.enemyTowns, ...c.enemies.map(e => s.tiles[e.tile])])].map(tile => {
        const defenders = c.enemies.filter(e => e.tile === tile.id);
        const defense = defenders.reduce((n, e) => n + strength(e, true, tile), 0) + (tile.building === 'city' ? 14 : tile.building ? 8 : 0);
        return { tile, defense, score: distance(location, tile) + defense / Math.max(1, strength(a)) * 8 - (a.target === tile.id ? 3 : 0) };
      }).filter(x => strength(a) >= Math.max(16, x.defense * (1.35 - k.aggression * .3)) &&
        (!(x.tile.walls > 0 || (x.tile.fortIntegrity ?? fortMaximum(x.tile)) > 0) || familyCount(a, 'siege') >= 2))
        .sort((x, y) => x.score - y.score || x.tile.id.localeCompare(y.tile.id));
      let moved = false;
      for (const { tile } of candidates.slice(0, 3)) {
        if (!findPath(s, a.tile, tile.id, k.id, false, avoid).length) continue;
        if (command(s, k, a, tile, 'attack', avoid)) { moved = true; break; }
      }
      if (moved) continue;
      // Reinforcements muster together at a safe forward settlement. A weak
      // detachment never receives a suicide attack just to appear active.
      const rally = [...(safeTowns.length ? safeTowns : c.towns)].sort((x, y) =>
        Math.min(...c.enemyTowns.map(t => distance(x, t))) - Math.min(...c.enemyTowns.map(t => distance(y, t))))[0];
      if (k.goal === 'ATTACK') k.goal = 'MUSTER';
      if (command(s, k, a, rally)) continue;
    }
    const ally = settlements(s).filter(t => t.owner !== k.id && treaty(s, k.id, t.owner, 'alliance') &&
      relation(s, k.id, t.owner).trust >= 40 && nearby(s, c.enemies, t, 4).length).sort((x, y) => distance(location, x) - distance(location, y))[0];
    if (ally && command(s, k, a, ally)) { k.goal = 'SUPPORT_ALLY'; continue; }
    if (!command(s, k, a, refuge)) command(s, k, a, location, 'hold');
  }
}

function considerRivalPeace(s) {
  for (const war of [...s.wars]) {
    const [a, b] = war.split(':');
    if ([a, b].includes(PLAYER) || !alive(s, a) || !alive(s, b)) continue; // The player ratifies their own peace.
    const start = s.diplomacy.warHistory.filter(w => [w.attacker, w.defender].includes(a) && [w.attacker, w.defender].includes(b)).at(-1)?.turn ?? s.turn;
    const recent = s.militaryEvents.some(e => e.turn > s.turn - 6 && [e.attacker, e.defender].includes(a) && [e.attacker, e.defender].includes(b));
    const exhausted = [a, b].every(id => troopCount(armiesOf(s, id)) < 18);
    if (s.turn - start < 12 || !exhausted && (recent || s.turn - start < 20)) continue;
    makePeace(s, a, b);
    s.treaties.push({ id: `treaty-${s.nextId++}`, type: 'peace', parties: [a, b], expires: s.turn + 8 });
    for (const [owner, house] of [[a, b], [b, a]]) {
      recordStrategyAction(s, owner, { kind: 'peace', house });
      if (!s.wars.some(w => w.split(':').includes(owner))) {
        kingdom(s, owner).goal = 'ECONOMY';
        const report = reportFor(s, owner);
        if (report) { report.goal = 'ECONOMY'; report.reason = 'A peace agreement allows the realm to rebuild.'; }
      }
    }
    log(s, `${kingdom(s, a).name} and ${kingdom(s, b).name} end their exhausted campaign with an eight-turn peace.`, 'diplomacy');
  }
}

export function runStrategyTurn(s) {
  if (s.outcome) return;
  initializeStrategy(s);
  if (s.strategy.lastTurn >= s.turn) return;
  const rivals = s.kingdoms.filter(k => k.id !== PLAYER);
  const round = { turn: s.turn, houses: rivals.map(k => ({ owner: k.id, goal: 'ECONOMY', reason: '', orders: 0, actions: [], omitted: 0 })) };
  s.strategy.history.push(round); s.strategy.history = s.strategy.history.slice(-HISTORY_LIMIT);
  // Every living House acts exactly once; the starting House rotates so
  // contested decisions do not always favor the same kingdom.
  const offset = (s.turn - 1) % rivals.length;
  for (const k of [...rivals.slice(offset), ...rivals.slice(0, offset)]) {
    const report = reportFor(s, k.id);
    if (!alive(s, k.id)) { report.goal = 'ELIMINATED'; report.reason = 'No settlements remain.'; continue; }
    let c = assess(s, k);
    considerWar(s, k, c); c = assess(s, k);
    [k.goal, report.reason] = chooseGoal(c);
    const tax = k.happiness < 40 ? 'low' : c.income.gold < 2 && k.resources.gold < 70 && k.happiness >= 60 ? 'high' : 'medium';
    if (tax !== k.tax) { k.tax = tax; recordStrategyAction(s, k.id, { kind: 'tax', policy: tax }); }
    develop(s, k);
    directArmies(s, k, assess(s, k));
    report.goal = k.goal;
    if (!report.orders && !report.actions.some(a => ['march','war'].includes(a.kind))) report.reason =
      c.tiles.some(t => t.project) ? 'Existing projects are underway; holding positions and rebuilding reserves.' :
        k.economicPlan ? 'Saving for the next project while maintaining the army.' : 'Holding secure positions and rebuilding population and reserves.';
  }
  considerRivalPeace(s);
  s.strategy.lastTurn = s.turn;
}

export function finishStrategyRound(s) {
  const round = s.strategy?.history.at(-1);
  if (round?.turn !== s.turn) return;
  for (const report of round.houses) for (const action of report.actions) if (action.kind === 'march') {
    action.arrivedAt = s.armies.find(a => a.id === action.army)?.tile || null;
  }
  log(s, `${round.houses.filter(h => h.goal !== 'ELIMINATED').length} rival Houses completed round ${s.turn}. Their actions are recorded under Realm → Rival Turns.`, 'council');
}

export function validateStrategySave(s) {
  initializeStrategy(s);
  const fail = () => { throw new Error('Damaged rival strategy data.'); };
  const integer = (n, min, max) => Number.isInteger(n) && n >= min && n <= max;
  const state = s.strategy;
  if (!integer(state.lastTurn, 0, s.turn) || !Array.isArray(state.history) || state.history.length > HISTORY_LIMIT) fail();
  let previous = 0;
  for (const round of state.history) {
    if (!round || !integer(round.turn, previous + 1, state.lastTurn) || !Array.isArray(round.houses) || round.houses.length !== HOUSES.length - 1 || new Set(round.houses.map(h => h?.owner)).size !== HOUSES.length - 1) fail();
    previous = round.turn;
    for (const h of round.houses) {
      if (!h || h.owner === PLAYER || !HOUSES.some(k => k.id === h.owner) || !Object.hasOwn(STRATEGY_GOALS, h.goal) || typeof h.reason !== 'string' || h.reason.length > 240 ||
        !integer(h.orders, 0, 8) || !integer(h.omitted, 0, 1000) || !Array.isArray(h.actions) || h.actions.length > ACTION_LIMIT) fail();
      for (const a of h.actions) {
        if (!a || !['build','complete','recruit','march','hold','merge','war','peace','tax'].includes(a.kind)) fail();
        if (['build','complete','recruit','march','hold','merge'].includes(a.kind) && !s.tiles[a.tile]) fail();
        if (['build','complete'].includes(a.kind) && (!Object.hasOwn(BUILDINGS, a.building) || !integer(a.level, 1, BUILDINGS[a.building].maxLevel))) fail();
        if (a.kind === 'recruit' && (!Object.hasOwn(UNITS, a.unit) || !integer(a.count, 1, 12))) fail();
        if (['march','hold'].includes(a.kind) && (typeof a.army !== 'string' || a.army.length > 80 || !s.tiles[a.from] || a.arrivedAt != null && !s.tiles[a.arrivedAt])) fail();
        if (['war','peace'].includes(a.kind) && (!HOUSES.some(k => k.id === a.house) || a.house === h.owner)) fail();
        if (a.kind === 'tax' && !['low','medium','high'].includes(a.policy)) fail();
      }
    }
  }
  if (state.history.length && previous !== state.lastTurn) fail();
}
