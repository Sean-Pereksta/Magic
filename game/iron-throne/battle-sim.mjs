import { UNITS } from './data.mjs';

// This module has no DOM, clock or random global dependencies. The entire live
// simulation is JSON data, including attack phases, missiles and delayed armies.
export const BATTLE_VERSION = 1;
const TAU = Math.PI * 2, HALF = 80, STEP = 1 / 20;
const clamp = (n, a, z) => Math.max(a, Math.min(z, n));
const distance = (a, c) => Math.hypot(a.x - c.x, a.z - c.z);
const facing = (a, c) => Math.atan2(c.x - a.x, c.z - a.z);
const angle = (a, c) => Math.abs(Math.atan2(Math.sin(a - c), Math.cos(a - c)));
const alive = u => u.status === 'active';
const fighting = u => alive(u) && !u.routing;
const count = units => Object.values(units || {}).reduce((a, n) => a + n, 0);
const clone = value => JSON.parse(JSON.stringify(value));
const countTypes = list => { const out = {}; for (const u of list) out[u.type] = (out[u.type] || 0) + 1; return out; };
const PROFILE = {
  levy: { hp: 70, damage: 16, reach: 1.9, speed: 3.5, weapon: 'axe', shield: false, windup: .45, recovery: .7 },
  spearman: { hp: 85, damage: 18, reach: 3.7, speed: 3.3, weapon: 'spear', shield: true, windup: .52, recovery: .7 },
  menAtArms: { hp: 100, damage: 25, reach: 2.1, speed: 3.3, weapon: 'sword', shield: true, windup: .3, recovery: .45 },
  heavyInfantry: { hp: 135, damage: 35, reach: 2.3, speed: 2.6, weapon: 'mace', shield: true, windup: .65, recovery: .9 },
  archer: { hp: 62, damage: 21, reach: 40, speed: 3.6, weapon: 'bow', ammo: 24, windup: .75, recovery: 1.4 },
  veteranArcher: { hp: 78, damage: 26, reach: 48, speed: 3.7, weapon: 'bow', ammo: 30, windup: .6, recovery: 1.1 },
  crossbow: { hp: 90, damage: 44, reach: 46, speed: 3, weapon: 'crossbow', ammo: 18, windup: .6, recovery: 3.1 },
  scout: { hp: 95, damage: 16, reach: 2.6, speed: 8, weapon: 'sword', windup: .32, recovery: .65 },
  lightCavalry: { hp: 110, damage: 23, reach: 2.8, speed: 8.2, weapon: 'sword', shield: true, windup: .35, recovery: .7 },
  cavalry: { hp: 145, damage: 31, reach: 4.1, speed: 7, weapon: 'lance', shield: true, windup: .4, recovery: .9 },
  knight: { hp: 180, damage: 40, reach: 4.4, speed: 6.5, weapon: 'lance', shield: true, windup: .4, recovery: .85 },
  ram: { hp: 280, damage: 6, reach: 4, speed: 1.8, weapon: 'ram', windup: 1.7, recovery: 2.3 },
  catapult: { hp: 160, damage: 54, reach: 65, speed: 1.3, weapon: 'catapult', ammo: 16, windup: 2.1, recovery: 5.2 },
  trebuchet: { hp: 190, damage: 80, reach: 100, speed: .9, weapon: 'trebuchet', ammo: 12, windup: 3.2, recovery: 7.5 },
  siege: { hp: 160, damage: 48, reach: 60, speed: 1.3, weapon: 'catapult', ammo: 16, windup: 2.1, recovery: 5.5 }
};
export const TACTICAL_PROFILES = PROFILE;
const WEAPONS = {
  sword: { damage: 24, reach: 2.1, windup: .3, recovery: .45 },
  spear: { damage: 22, reach: 3.7, windup: .52, recovery: .7 },
  axe: { damage: 30, reach: 2.3, windup: .6, recovery: .85 },
  mace: { damage: 32, reach: 2, windup: .65, recovery: .9 },
  bow: { damage: 23, reach: 44, windup: .7, recovery: 1.4 },
  crossbow: { damage: 44, reach: 46, windup: .6, recovery: 3.1 },
  lance: { damage: 31, reach: 4.1, windup: .4, recovery: .9 }
};
function profileFor(u) { const p = PROFILE[u.type]; return u.weapon === p.weapon || !WEAPONS[u.weapon] ? p : { ...p, ...WEAPONS[u.weapon] }; }

function hash(value) { let n = 2166136261; for (const c of String(value)) n = Math.imul(n ^ c.charCodeAt(0), 16777619); return n >>> 0 || 1; }
function random(b) { let x = b.rng; x ^= x << 13; x ^= x >>> 17; x ^= x << 5; b.rng = x >>> 0; return b.rng / 4294967296; }
function event(b, type, message, extra = {}) { b.events.push({ time: b.time, type, message, ...extra }); if (b.events.length > 100) b.events.shift(); }
function entryPoint(p) {
  let e = p.entry || p.entryDirection;
  if (typeof e === 'string') e = ({ north: { x: 0, z: -1 }, south: { x: 0, z: 1 }, east: { x: 1, z: 0 }, west: { x: -1, z: 0 }, northeast: { x: .7, z: -.7 }, northwest: { x: -.7, z: -.7 }, southeast: { x: .7, z: .7 }, southwest: { x: -.7, z: .7 } })[e.toLowerCase()];
  if (Number.isFinite(e)) e = { x: Math.sin(e), z: Math.cos(e) };
  const x = Number(e?.x ?? e?.dx) || 0;
  const rawZ = e?.z ?? e?.y ?? e?.dy;
  const z = rawZ !== undefined && Number.isFinite(Number(rawZ)) ? Number(rawZ) : (p.side ? 1 : -1);
  const norm = Math.max(Math.abs(x), Math.abs(z), .001);
  return { x: x / norm * 75, z: z / norm * 75 };
}
function layoutFor(b, tile) {
  const terrain = tile.terrain || 'plains';
  const layout = { width: 160, depth: 160, terrain, obstacles: [], walls: [], gate: null, ladders: [], stairs: [], objective: { x: 0, z: 38 }, bridges: [], navRevision: 0 };
  const initialWalls = Math.max(0, Number(tile.walls) || 0);
  const fortMax = tile.building === 'fort' ? [0, 45, 100, 180][clamp(tile.levels?.fort || tile.buildings?.fort || tile.level || tile.buildingLevel || 1, 1, 3)] : 0;
  const initialFortIntegrity = Math.max(0, Number(tile.fortIntegrity ?? fortMax) || 0);
  const baseline = tile.battleDamageIntegrity;
  const repaired = !!baseline && (initialWalls > baseline.walls + 1e-6 || initialFortIntegrity > baseline.fortIntegrity + 1e-6);
  const damageScale = baseline && !repaired ? clamp((initialWalls + initialFortIntegrity) / Math.max(.00001, baseline.walls + baseline.fortIntegrity), 0, 1) : 1;
  b.fortifications = { walls: initialWalls, fortIntegrity: initialFortIntegrity, initialWalls, initialFortIntegrity };
  const fortified = initialWalls > 0 || tile.building === 'fort' || tile.wallBreaches?.length;
  layout.fortified = !!fortified;
  if (fortified) {
    const maximum = Math.max(initialWalls, (tile.levels?.wall || tile.buildings?.wall || 1) * 60, fortMax);
    const condition = clamp((initialWalls || initialFortIntegrity) / maximum, 0, 1);
    const specifications = [ ['wall-west-front', -19, 12, 30, 2], ['wall-east-front', 19, 12, 30, 2], ['wall-west', -35, 35, 2, 48], ['wall-east', 35, 35, 2, 48], ['wall-rear', 0, 59, 72, 2] ];
    for (const [id, x, z, width, depth] of specifications) layout.walls.push({ id, x, z, width, depth, height: 6, maxHp: 220, hp: 220 * condition, state: condition < .7 ? 'damaged' : 'intact' });
    layout.gate = { id: 'gate', x: 0, z: 12, width: 8, depth: 2, height: 5.7, hp: 150 * condition, maxHp: 150, state: condition < .7 ? 'damaged' : 'intact' };
    if (condition < .3) Object.assign(layout.walls[0], { hp: 0, state: 'breached' });
    if (!condition) for (const w of [...layout.walls, layout.gate]) Object.assign(w, { hp: 0, state: 'breached' });
    for (const w of [...layout.walls, layout.gate]) {
      const saved = !repaired && tile.wallSections?.find(s => s.id === w.id);
      if (saved && Number.isFinite(saved.hp) && Number.isFinite(saved.maxHp) && saved.maxHp > 0) w.hp = w.maxHp * clamp(saved.hp / saved.maxHp, 0, 1) * damageScale;
      if (!repaired && tile.wallBreaches?.includes(w.id)) w.hp = 0;
      w.state = w.hp <= 0 ? 'breached' : w.hp < w.maxHp * .7 ? 'damaged' : 'intact';
    }
    layout.ladders = [-20, 20].map((x, i) => ({ id: `ladder-${i}`, x, z: 12, height: 6, active: false, progress: 0, climberId: null }));
    layout.stairs = layout.walls.map(w => ({ id: `${w.id}-stairs`, wallId: w.id, x: w.x + (w.id === 'wall-west' ? 6 : w.id === 'wall-east' ? -6 : 0), z: w.z + (w.id.includes('front') ? 6 : w.id === 'wall-rear' ? -6 : 0), topX: w.x, topZ: w.z, height: w.height, width: 3, depth: 6 }));
    b.fortifications.initialTacticalHp = [...layout.walls, layout.gate].reduce((sum, w) => sum + w.hp, 0);
  }
  if (terrain === 'forest') {
    for (let i = 0; i < 42; i++) {
      const x = (random(b) - .5) * 140, z = (random(b) - .5) * 130;
      if (Math.abs(x) < 10 || fortified && Math.abs(x) < 40 && z > 0) continue;
      layout.obstacles.push({ id: `tree-${i}`, kind: 'tree', x, z, radius: .9 + random(b) * .6, height: 7 + random(b) * 4 });
    }
  }
  if (terrain === 'mountain' || terrain === 'pass') {
    layout.obstacles.push({ id: 'mountain-west', kind: 'rock', x: -52, z: 0, width: 68, depth: 24, height: 13 }, { id: 'mountain-east', kind: 'rock', x: 52, z: 0, width: 68, depth: 24, height: 13 });
  }
  if (tile.river || tile.crossing || terrain === 'river') {
    layout.bridges = [{ x: -20, z: 0, width: 12, depth: 12 }, { x: 28, z: 0, width: 10, depth: 12 }];
    for (const [x, width] of [[-53, 42], [5, 34], [56, 46]]) layout.obstacles.push({ id: `water-${x}`, kind: 'water', x, z: 0, width, depth: 10, height: .1 });
  }
  if (['town', 'city'].includes(tile.building)) {
    for (const [x, z] of [[-23, 30], [23, 30], [-23, 49], [23, 49]]) layout.obstacles.push({ id: `house-${x}-${z}`, kind: 'building', x, z, width: 11, depth: 10, height: 7 });
  }
  return layout;
}
export function terrainHeight(b, x, z) { return b.layout.terrain === 'hills' ? 2.8 * Math.sin(x / 25) + 2.3 * Math.cos(z / 23) : 0; }
function inside(o, x, z, margin = 0) { return o.radius ? Math.hypot(o.x - x, o.z - z) < o.radius + margin : Math.abs(o.x - x) < o.width / 2 + margin && Math.abs(o.z - z) < o.depth / 2 + margin; }
function obstruction(b, x, z, u, options = {}) {
  if (Math.abs(x) > 78.5 || Math.abs(z) > 78.5) return { id: 'boundary', kind: 'boundary' };
  for (const o of b.layout.obstacles) if (inside(o, x, z, .35)) return o;
  for (const w of b.layout.walls) {
    if (w.hp <= 0 || !inside(w, x, z, .35)) continue;
    if (u?.wallId === w.id && u.y >= terrainHeight(b, x, z) + w.height - .3) continue;
    if (options.ladders && u?.family === 'infantry' && b.layout.ladders.some(l => Math.abs(l.x - x) < 2.3 && Math.abs(l.z - z) < 3)) continue;
    return w;
  }
  const g = b.layout.gate;
  if (g?.hp > 0 && u?.side !== 1 && inside(g, x, z, .35)) return g;
  return null;
}
export function nearestBattlePosition(b, position, unit = null) {
  const p = { x: clamp(Number(position.x) || 0, -76, 76), z: clamp(Number(position.z) || 0, -76, 76) };
  if (!obstruction(b, p.x, p.z, unit)) return { ...p, reachable: true };
  for (let r = 1; r <= 80; r += 1.5) for (let i = 0; i < 20; i++) {
    const x = clamp(p.x + Math.sin(i * TAU / 20) * r, -76, 76), z = clamp(p.z + Math.cos(i * TAU / 20) * r, -76, 76);
    if (!obstruction(b, x, z, unit)) return { x, z, reachable: false, reason: 'That position is obstructed; the nearest open position is marked.' };
  }
  return { ...p, reachable: false, reason: 'No open position is available.' };
}
function lineClear(b, a, c, u, projectile = false) {
  const length = distance(a, c), steps = Math.ceil(length / 1.4);
  for (let i = 1; i < steps; i++) {
    const t = i / steps, x = a.x + (c.x - a.x) * t, z = a.z + (c.z - a.z) * t;
    const o = obstruction(b, x, z, u);
    if (o && projectile) {
      const rayY = (a.y || 0) + 1.6 + (((c.y || 0) + 1.2) - ((a.y || 0) + 1.6)) * t + Math.sin(t * Math.PI) * .22;
      if (rayY > terrainHeight(b, x, z) + (o.height || 6) + .1) continue;
    }
    if (o && (!projectile || o.kind !== 'water') && !(projectile && o.kind === 'tree' && random(b) > .8)) return false;
  }
  return true;
}
function pathTo(b, u, target) {
  if (lineClear(b, u, target, u)) return [{ x: target.x, z: target.z }];
  const grid = 40, size = 4, cell = p => ({ x: clamp(Math.floor((p.x + HALF) / size), 0, grid - 1), z: clamp(Math.floor((p.z + HALF) / size), 0, grid - 1) });
  const start = cell(u), goal = cell(target), key = p => p.z * grid + p.x, center = p => ({ x: p.x * size - HALF + 2, z: p.z * size - HALF + 2 });
  const queue = [{ ...start, g: 0, h: Math.hypot(start.x - goal.x, start.z - goal.z) }], parents = new Map(), costs = new Map([[key(start), 0]]), done = new Set();
  let end = null, closest = queue[0];
  while (queue.length && done.size < 1600) {
    queue.sort((a, c) => (a.g + a.h) - (c.g + c.h));
    const n = queue.shift(), nk = key(n);
    if (done.has(nk)) continue;
    done.add(nk);
    if (n.h < closest.h) closest = n;
    if (n.x === goal.x && n.z === goal.z) { end = n; break; }
    for (const [dx, dz] of [[1, 0], [-1, 0], [0, 1], [0, -1]]) {
      const next = { x: n.x + dx, z: n.z + dz };
      if (next.x < 0 || next.z < 0 || next.x >= grid || next.z >= grid) continue;
      const p = center(next), q = center(n), midpoint = { x: (p.x + q.x) / 2, z: (p.z + q.z) / 2 };
      if (obstruction(b, p.x, p.z, u, { ladders: true }) || obstruction(b, midpoint.x, midpoint.z, u, { ladders: true })) continue;
      const g = n.g + 1, k = key(next);
      if (costs.has(k) && costs.get(k) <= g) continue;
      costs.set(k, g); parents.set(k, n); queue.push({ ...next, g, h: Math.abs(next.x - goal.x) + Math.abs(next.z - goal.z) });
    }
  }
  end ||= closest;
  const path = []; let at = end;
  while (at && key(at) !== key(start)) { path.push(center(at)); at = parents.get(key(at)); }
  path.reverse();
  if (end.x === goal.x && end.z === goal.z) path.push({ x: target.x, z: target.z });
  return path;
}

function allocateCaps(b) {
  const totals = [0, 0]; for (const a of b.armies) totals[a.side] += count(a.starting);
  const total = totals[0] + totals[1];
  const cap0 = Math.min(totals[0], total ? Math.max(totals[0] ? 1 : 0, Math.round(b.maxActive * totals[0] / total)) : 0);
  b.activeCaps = [cap0, Math.min(totals[1], Math.max(totals[1] ? 1 : 0, b.maxActive - cap0))];
}
function initialPosition(b, u, a, index) {
  const e = a.entry, early = !a.arrivalSeconds;
  const direction = Math.atan2(-e.x, -e.z), columns = 12;
  let center = early ? { x: e.x * .69, z: e.z * .69 } : e;
  if (early && a.side === 1 && b.layout.fortified) center = { x: 0, z: 33 };
  const sideways = ((index % columns) - (columns - 1) / 2) * 2.05;
  const row = Math.floor(index / columns) * 2.2 + (u.family === 'ranged' ? 4 : u.family === 'siege' ? 9 : 0);
  const p = nearestBattlePosition(b, { x: center.x + Math.cos(direction) * sideways - Math.sin(direction) * row, z: center.z - Math.sin(direction) * sideways - Math.cos(direction) * row }, u);
  return { ...p, facing: direction };
}
function deployReserves(b) {
  for (const a of b.armies) if (!a.arrived && b.time >= a.arrivalSeconds) {
    a.arrived = true;
    for (const u of b.units) if (u.side === a.side && alive(u)) u.morale = clamp(u.morale + 5, 0, 100);
    if (a.arrivalSeconds > 0) event(b, 'reinforcements', `${a.name || a.owner} reinforcements have arrived.`, { armyId: a.armyId, owner: a.owner, x: a.entry.x, z: a.entry.z });
  }
  for (const side of [0, 1]) {
    const active = b.units.filter(u => u.side === side && alive(u));
    let remaining = b.activeCaps[side] - active.length;
    const eligible = b.armies.filter(a => a.side === side && a.arrived && !a.retreating);
    const groups = [];
    for (const a of eligible) for (const [type, amount] of Object.entries(a.starting)) {
      const reserves = b.units.filter(u => u.armyId === a.armyId && u.type === type && u.status === 'reserve' && !u.routing);
      if (reserves.length) groups.push({ a, type, amount, reserves, deployed: b.units.filter(u => u.armyId === a.armyId && u.type === type && u.status !== 'reserve').length });
    }
    while (remaining > 0 && groups.some(g => g.reserves.length && !g.blocked)) {
      groups.sort((a, c) => ((a.deployed + 1) / a.amount) - ((c.deployed + 1) / c.amount));
      const commanderGroup = groups.find(g => !g.blocked && g.reserves.some(u => u.id === b.commanderId));
      // Keep rare but important roster roles (especially the player's actual
      // ram) available when there is room, then fill remaining slots in
      // proportion to the original formations. Both sides use this rule.
      const missingRole = b.activeCaps[side] >= groups.length ? groups.find(g => !g.blocked && g.reserves.length && !active.some(u => u.armyId === g.a.armyId && u.type === g.type) && !b.units.some(u => alive(u) && u.armyId === g.a.armyId && u.type === g.type)) : null;
      const group = commanderGroup || missingRole || groups.find(g => g.reserves.length && !g.blocked);
      const index = group.reserves.findIndex(u => u.id === b.commanderId), u = group.reserves.splice(index >= 0 ? index : 0, 1)[0];
      const deployedCount = b.units.filter(v => v.armyId === u.armyId && alive(v)).length;
      let p = initialPosition(b, u, group.a, deployedCount);
      // Arrivals use the edge; an occupied edge is shifted along its boundary.
      const available = point => !obstruction(b, point.x, point.z, u) && !b.units.some(v => alive(v) && distance(v, point) < (v.side !== side ? 6 : v.family === 'siege' || u.family === 'siege' ? 2.4 : 1.6));
      const origin = { ...p };
      for (let attempts = 1; !available(p) && attempts < 100; attempts++) {
        const offset = Math.ceil(attempts / 2) * 2.5 * (attempts % 2 ? 1 : -1);
        if (Math.abs(group.a.entry.x) > Math.abs(group.a.entry.z)) p = { ...origin, z: clamp(origin.z + offset, -74, 74) };
        else p = { ...origin, x: clamp(origin.x + offset, -74, 74) };
      }
      if (!available(p)) { group.reserves.unshift(u); group.blocked = true; continue; }
      Object.assign(u, { status: 'active', x: p.x, z: p.z, y: terrainHeight(b, p.x, p.z), facing: p.facing, arrived: true, deployedAt: b.time });
      if (!u.explicitOrder) u.order = { kind: b.phase === 'deployment' ? 'hold' : 'advance', x: p.x, z: p.z, facing: p.facing };
      group.deployed++; remaining--;
    }
  }
}
export function createBattle(encounter, options = {}) {
  if (!encounter?.id || !Array.isArray(encounter.participants) || !encounter.participants.length) throw new Error('A recorded encounter with participating armies is required.');
  const ids = new Set();
  const b = { version: BATTLE_VERSION, encounterId: encounter.id, battleId: encounter.id, seed: encounter.seed ?? encounter.id, rng: hash(encounter.seed ?? encounter.id), time: 0, accumulator: 0, phase: 'deployment', playerOwner: encounter.playerOwner, playerSide: encounter.playerSide ?? 0, commanderId: null, units: [], armies: [], projectiles: [], events: [], winner: null, maxActive: clamp(Math.floor(options.maxActive || 120), 4, 240), nextProjectile: 0, nextArmyPlan: 0, nextMorale: 1, nextReserves: 1, occupation: 0, auto: false };
  b.layout = layoutFor(b, encounter.tile || {});
  for (const p of encounter.participants) {
    if (ids.has(p.armyId) || p.armyId == null || ![0, 1].includes(p.side)) throw new Error('Participants must have unique army identities and a battle side.');
    ids.add(p.armyId);
    const starting = {};
    for (const [type, amount] of Object.entries(p.units || {})) {
      if (!UNITS[type] || !Number.isSafeInteger(amount) || amount < 0) throw new Error(`Invalid roster: ${p.armyId} / ${type}.`);
      if (amount) starting[type] = amount;
    }
    const suppliedMorale = Number(p.morale ?? .75);
    const a = { ...clone(p), starting, units: { ...starting }, general: clone(p.general || p.commander || null), entry: entryPoint(p), arrivalSeconds: Math.max(0, Number(p.arrivalSeconds) || (p.distance ? p.distance * 25 : 0)), arrived: false, retreating: false, morale: clamp(suppliedMorale <= 1 ? suppliedMorale * 100 : suppliedMorale, 0, 100) };
    b.armies.push(a);
    for (const [type, amount] of Object.entries(starting)) for (let n = 0; n < amount; n++) {
      const profile = PROFILE[type];
      b.units.push({ id: `${p.armyId}:${type}:${n}`, armyId: p.armyId, owner: p.owner, side: p.side, type, family: UNITS[type].family, mounted: UNITS[type].family === 'mounted', status: 'reserve', arrived: false, hp: profile.hp, maxHp: profile.hp, x: a.entry.x, z: a.entry.z, y: 0, facing: Math.atan2(-a.entry.x, -a.entry.z), stamina: 100, ammo: profile.ammo || 0, morale: a.morale, speed: 0, action: null, targetId: null, order: { kind: 'hold' }, formation: p.formation || 'balanced', weapon: profile.weapon, shield: !!profile.shield, blocking: false, braced: false, routing: false, holdFire: false, path: [], pathAt: -10, chargeDistance: 0, attackCooldown: 0, blockedAt: -10, stagger: 0 });
    }
  }
  const commanderArmy = b.armies.find(a => a.owner === b.playerOwner && !a.arrivalSeconds) || b.armies.find(a => a.owner === b.playerOwner) || b.armies.find(a => a.playerControlled);
  if (commanderArmy) {
    const roster = b.units.filter(u => u.armyId === commanderArmy.armyId);
    const u = roster.find(u => u.family !== 'siege') || roster[0];
    if (u) { b.commanderId = u.id; b.playerSide = u.side; b.commanderName = commanderArmy.general?.name || commanderArmy.general || `${commanderArmy.name || commanderArmy.owner} Captain`; u.commander = true; }
  }
  allocateCaps(b); deployReserves(b);
  event(b, 'deployment', 'Place your formations, then begin the battle.');
  return b;
}
export function beginBattle(b) {
  if (b.phase !== 'deployment') return b;
  b.phase = 'combat';
  for (const u of b.units) if (alive(u) && !u.explicitOrder) u.order = { kind: 'advance' };
  event(b, 'begin', 'The battle has begun.');
  checkVictory(b);
  return b;
}

function selected(b, selection) {
  let candidates = b.units.filter(u => u.owner === b.playerOwner && u.status !== 'dead' && u.status !== 'escaped');
  if (Array.isArray(selection)) return candidates.filter(u => selection.includes(u.id) || selection.includes(u.armyId));
  if (selection && typeof selection === 'object') {
    if (selection.armyId) candidates = candidates.filter(u => u.armyId === selection.armyId);
    if (selection.ids) candidates = candidates.filter(u => selection.ids.includes(u.id));
    selection = selection.group || 'all';
  }
  if (!selection || selection === 'all') return candidates;
  if (selection === 'spearmen') return candidates.filter(u => u.type === 'spearman');
  if (selection === 'cavalry') return candidates.filter(u => u.family === 'mounted');
  if (['infantry', 'ranged', 'siege'].includes(selection)) return candidates.filter(u => u.family === selection);
  return candidates.filter(u => u.armyId === selection || u.id === selection);
}
const ORDER_ALIASES = { moveAndFace: 'move', holdPosition: 'hold', followMe: 'follow', attackFormation: 'attack', attackNearest: 'nearest', holdfire: 'holdFire' };
export function issueOrder(b, selection, order, params = {}) {
  if (b.phase === 'complete') return { accepted: false, reason: 'The battle has ended.' };
  order = ORDER_ALIASES[order] || order;
  if (!['move', 'hold', 'advance', 'charge', 'follow', 'attack', 'nearest', 'fire', 'holdFire', 'withdraw', 'retreat', 'formation', 'ladder', 'walls'].includes(order)) return { accepted: false, reason: 'Unknown battle order.' };
  const units = selected(b, selection).filter(u => order !== 'walls' || ['infantry', 'ranged'].includes(u.family) && (b.phase !== 'deployment' || u.side === 1));
  if (!units.length) return { accepted: false, reason: 'No available troops match the selection.' };
  if (order === 'walls') return issueWallOrder(b, units, params);
  if (['move', 'withdraw'].includes(order) && (!Number.isFinite(params.x) || !Number.isFinite(params.z))) return { accepted: false, reason: 'Choose a destination on the battlefield.' };
  const pos = nearestBattlePosition(b, { x: params.x ?? units[0].x, z: params.z ?? units[0].z }, units[0]);
  const f = Number.isFinite(params.facing) ? params.facing : units[0].facing;
  units.forEach((u, index) => {
    if (order === 'fire' || order === 'holdFire') { u.holdFire = order === 'holdFire'; return; }
    if (params.formation) u.formation = params.formation;
    if (order === 'formation') return;
    const spacing = u.formation === 'skirmish' ? 3.6 : u.formation === 'defensive' || u.formation === 'spearWall' ? 1.8 : 2.2;
    const width = Math.max(3, Math.ceil(Math.sqrt(units.length) * (u.formation === 'defensive' ? 2 : 1.4)));
    const x = ((index % width) - (Math.min(width, units.length) - 1) / 2) * spacing, z = -Math.floor(index / width) * spacing;
    const destination = nearestBattlePosition(b, { x: pos.x + x * Math.cos(f) + z * Math.sin(f), z: pos.z - x * Math.sin(f) + z * Math.cos(f) }, u);
    u.order = { kind: order, x: destination.x, z: destination.z, facing: f, targetArmyId: params.targetArmyId || null, targetId: params.targetId || null, offsetX: x, offsetZ: z };
    u.explicitOrder = true; u.path = []; u.pathAt = -10;
    if (order === 'retreat') u.routing = true;
    if (b.phase === 'deployment' && ['move', 'hold'].includes(order) && alive(u)) {
      const a = b.armies.find(a => a.armyId === u.armyId), origin = initialPosition(b, u, a, 0);
      const dx = destination.x - origin.x, dz = destination.z - origin.z, d = Math.hypot(dx, dz), ratio = d > 25 ? 25 / d : 1;
      const safe = nearestBattlePosition(b, { x: origin.x + dx * ratio, z: origin.z + dz * ratio }, u);
      u.x = safe.x; u.z = safe.z; u.y = terrainHeight(b, u.x, u.z); u.facing = f;
      u.order.x = u.x; u.order.z = u.z;
    }
  });
  if (order === 'retreat') for (const a of b.armies) {
    if (b.units.filter(u => u.armyId === a.armyId && u.status !== 'dead' && u.status !== 'escaped').every(u => u.routing)) a.retreating = true;
  }
  event(b, 'order', `${order === 'holdFire' ? 'Hold fire' : order}: ${units.length} troops.`, { x: pos.x, z: pos.z });
  return { accepted: true, count: units.length, destination: pos, reason: pos.reason || null };
}
function issueWallOrder(b, units, params) {
  const walls = b.layout.walls.filter(w => w.hp > 0 && (!params.wallId || w.id === params.wallId));
  if (!walls.length) return { accepted: false, reason: 'No intact wall section is available to defend.' };
  const selectedIds = new Set(units.map(u => u.id));
  const occupied = new Set(b.units.filter(u => u.wallPosition && u.status !== 'dead' && !selectedIds.has(u.id)).map(u => u.wallPosition.slot));
  const slots = [];
  for (const w of walls) {
    const horizontal = w.width > w.depth, length = horizontal ? w.width : w.depth;
    const n = Math.max(1, Math.floor((length - 4) / 2.4));
    const f = w.id.includes('front') ? Math.PI : w.id === 'wall-rear' ? 0 : w.id === 'wall-west' ? -Math.PI / 2 : Math.PI / 2;
    for (let i = 0; i < n; i++) {
      const slot = `${w.id}:${i}`, offset = (i - (n - 1) / 2) * 2.4;
      if (!occupied.has(slot)) slots.push({ slot, wallId: w.id, x: w.x + (horizontal ? offset : 0), z: w.z + (horizontal ? 0 : offset), facing: f });
    }
  }
  let assigned = 0;
  for (const u of units) {
    const slot = slots.shift(); if (!slot) break;
    u.wallPosition = slot; u.order = { kind: 'walls', ...slot }; u.explicitOrder = true;
    u.path = []; u.pathAt = -10; u.routing = false;
    if (b.phase === 'deployment' && alive(u)) {
      const wall = walls.find(w => w.id === slot.wallId);
      Object.assign(u, { wallId: wall.id, x: slot.x, z: slot.z, y: terrainHeight(b, slot.x, slot.z) + wall.height, facing: slot.facing, speed: 0 });
    }
    assigned++;
  }
  event(b, 'order', `${assigned} troops assigned to defend the parapets.`);
  return { accepted: assigned > 0, count: assigned, reason: assigned < units.length ? 'The available parapet positions are full; remaining troops keep their orders.' : null };
}

function moveOnWall(b, u, target, dt, keepFacing = false) {
  const w = b.layout.walls.find(w => w.id === u.wallId);
  if (!w || w.hp <= 0) return;
  const x = clamp(target.x, w.x - w.width / 2 + .3, w.x + w.width / 2 - .3), z = clamp(target.z, w.z - w.depth / 2 + .3, w.z + w.depth / 2 - .3);
  const d = Math.hypot(x - u.x, z - u.z), step = Math.min(d, profileFor(u).speed * dt * .65);
  if (d > .05) { if (!keepFacing) u.facing = Math.atan2(x - u.x, z - u.z); u.x += (x - u.x) / d * step; u.z += (z - u.z) / d * step; }
  u.y = terrainHeight(b, u.x, u.z) + w.height; u.speed = d > .05 ? step / dt : 0;
}
function startStairs(b, u, stair, ascending) {
  if (u.action || b.units.some(v => v.id !== u.id && alive(v) && v.action?.kind === 'stairs' && v.action.stairId === stair.id)) return false;
  u.action = { kind: 'stairs', stairId: stair.id, wallId: stair.wallId, ascending, elapsed: 0, duration: 4, windup: 4, recovery: 0, fromX: u.x, fromZ: u.z, fromY: u.y, targetX: ascending ? stair.topX : stair.x, targetZ: ascending ? stair.topZ : stair.z };
  u.speed = 0; u.blocking = false; u.braced = false;
  return true;
}
function takeWallPosition(b, u, dt) {
  const slot = u.wallPosition, wall = slot && b.layout.walls.find(w => w.id === slot.wallId);
  if (!wall || wall.hp <= 0) { u.order = { kind: 'hold', x: u.x, z: u.z, facing: u.facing }; return false; }
  if (u.action?.kind === 'stairs' || u.action?.kind === 'climb') return true;
  if (u.wallId === wall.id) {
    if (distance(u, slot) > .3) { moveOnWall(b, u, slot, dt); return true; }
    u.facing = slot.facing; u.speed = 0; return false;
  }
  const stair = b.layout.stairs?.find(s => s.wallId === wall.id);
  if (!stair) return false;
  if (u.wallId) moveUnit(b, u, stair, dt);
  else if (distance(u, stair) > 1) moveUnit(b, u, stair, dt);
  else startStairs(b, u, stair, true);
  return true;
}
export function requestAlliedOrder(b, armyId, order) {
  const a = b.armies.find(a => a.armyId === armyId && a.side === b.playerSide && a.owner !== b.playerOwner);
  if (!a) return { accepted: false, reason: 'That army is not allied support.' };
  if (a.morale < 20 && order !== 'withdraw') return { accepted: false, reason: 'The allied commander is preserving a broken force.' };
  const kind = { flank: 'flanking', assault: 'charge', cover: 'hold', withdraw: 'withdraw' }[order] || order;
  for (const u of b.units.filter(u => u.armyId === armyId)) {
    if (kind === 'flanking') u.formation = 'flanking';
    else u.order = { kind, x: a.entry.x * .6, z: a.entry.z * .6, facing: u.facing };
    u.allyRequestUntil = b.time + 30;
  }
  event(b, 'ally', `${a.name || a.owner} accepts the ${order} request.`);
  return { accepted: true };
}

function moveUnit(b, u, destination, dt, options = {}) {
  const profile = profileFor(u);
  if (u.stagger > 0 || ['climb', 'stairs'].includes(u.action?.kind)) return;
  if (u.wallId) {
    const wall = b.layout.walls.find(w => w.id === u.wallId);
    if (!wall || wall.hp <= 0) {
      u.wallId = null; u.y = terrainHeight(b, u.x, u.z); u.hp -= 18; u.stagger = .8;
      if (u.hp <= 0) kill(b, u, null);
      return;
    }
    if (destination.wallId === u.wallId || options.direct && inside(wall, destination.x, destination.z, -.15)) {
      moveOnWall(b, u, destination, dt, options.keepFacing); return;
    }
    const stair = b.layout.stairs?.find(s => s.wallId === wall.id);
    if (!stair) return;
    const top = { x: stair.topX, z: stair.topZ };
    if (distance(u, top) > .65) moveOnWall(b, u, top, dt, options.keepFacing);
    else startStairs(b, u, stair, false);
    return;
  }
  let target = destination;
  if (options.direct) u.path = [];
  else {
    if (b.time - u.pathAt > 3 || !u.pathTarget || distance(u.pathTarget, destination) > 6 || u.navRevision !== b.layout.navRevision) {
      u.path = pathTo(b, u, destination); u.pathAt = b.time; u.pathTarget = { x: destination.x, z: destination.z }; u.navRevision = b.layout.navRevision;
    }
    while (u.path.length && distance(u, u.path[0]) < 1.3) u.path.shift();
    target = u.path[0] || destination;
  }
  const dx = target.x - u.x, dz = target.z - u.z, d = Math.hypot(dx, dz);
  if (d < .3) { u.speed *= .5; return; }
  let speed = profile.speed * (options.sprint && u.stamina > 5 ? 1.45 : 1);
  if (u.family === 'mounted' && !u.mounted) speed = 3.3 * (options.sprint ? 1.45 : 1);
  if (u.family === 'mounted' && b.layout.terrain === 'forest') speed *= .55;
  if (u.formation === 'defensive' || u.formation === 'spearWall') speed *= .78;
  if (u.blocking) speed *= .58;
  if (u.action) speed *= .4;
  const slope = terrainHeight(b, target.x, target.z) - u.y;
  if (slope > 0) speed *= clamp(1 - slope * .06, .6, 1);
  u.speed = Math.min(speed, u.speed + dt * (u.family === 'mounted' ? 2.8 : 10));
  const step = Math.min(d, u.speed * dt), nx = u.x + dx / d * step, nz = u.z + dz / d * step;
  const blocked = obstruction(b, nx, nz, u);
  if (blocked) {
    u.chargeDistance = 0; u.speed = 0;
    const ladder = u.family === 'infantry' && b.layout.ladders.find(l => Math.abs(u.x - l.x) < 3 && Math.abs(u.z - l.z) < 4 && Math.sign(destination.z - l.z) !== Math.sign(u.z - l.z));
    if (ladder && !u.action && (!ladder.climberId || !b.units.some(v => v.id === ladder.climberId && alive(v) && v.action?.kind === 'climb'))) {
      ladder.active = true; ladder.climberId = u.id;
      const destinationWall = b.layout.walls.find(w => w.hp > 0 && inside(w, destination.x, destination.z, .5));
      u.action = { kind: 'climb', elapsed: 0, windup: 3.5, duration: 5, recovery: 1.5, ladderId: ladder.id, targetX: ladder.x, targetZ: destinationWall ? ladder.z : ladder.z + Math.sign(destination.z - ladder.z) * 3, wallId: destinationWall?.id || null, fromZ: u.z };
      event(b, 'ladder', 'Infantry are climbing the assault ladder.', { x: ladder.x, z: ladder.z });
    }
    return;
  }
  if (u.mounted && u.speed > 4) u.chargeDistance += step; else u.chargeDistance = Math.max(0, u.chargeDistance - dt * 3);
  if (!options.keepFacing) u.facing = Math.atan2(dx, dz);
  u.x = nx; u.z = nz; u.y = terrainHeight(b, nx, nz);
  if (options.sprint) u.stamina = Math.max(0, u.stamina - dt * 13);
}
function nearestEnemy(b, u, candidates, max = Infinity) {
  let enemy = null, best = max;
  for (const v of candidates) {
    if (!alive(v) || v.side === u.side || u.order.targetArmyId && v.armyId !== u.order.targetArmyId) continue;
    const d = distance(u, v);
    if (d < best && (b.layout.terrain !== 'forest' || d < 32 || u.family === 'mounted')) { enemy = v; best = d; }
  }
  return enemy;
}
function startAttack(b, u, target, heavy = false, point = null) {
  if (u.action || u.stagger > 0 || u.attackCooldown > 0 || u.stamina < (heavy ? 24 : 8)) return false;
  if ((['bow', 'crossbow'].includes(u.weapon) || u.family === 'siege' && u.type !== 'ram') && u.ammo <= 0) u.sidearm = true;
  const p = profileFor(u), ranged = !u.sidearm && (['bow', 'crossbow'].includes(u.weapon) || u.family === 'siege' && u.type !== 'ram') && u.ammo > 0;
  if (ranged && u.holdFire) return false;
  const kind = u.family === 'siege' ? 'siege' : ranged ? 'shoot' : 'attack';
  const targetPoint = point || target;
  if (!targetPoint) return false;
  if (ranged && u.family === 'ranged' && !lineClear(b, u, targetPoint, u, true)) return false;
  if (ranged && u.family === 'ranged') {
    const dx = targetPoint.x - u.x, dz = targetPoint.z - u.z, lengthSq = dx * dx + dz * dz;
    const blocked = b.units.some(friend => {
      if (!alive(friend) || friend.id === u.id || friend.side !== u.side || Math.abs(friend.y - u.y) > 1.5) return false;
      const t = lengthSq ? ((friend.x - u.x) * dx + (friend.z - u.z) * dz) / lengthSq : 0;
      return t > .03 && t < .97 && Math.hypot(friend.x - u.x - dx * t, friend.z - u.z - dz * t) < .75;
    });
    if (blocked) return false;
  }
  u.stamina -= heavy ? 24 : ranged ? 8 : 11;
  if (target) u.targetId = target.id;
  u.action = { kind, elapsed: 0, windup: ranged || u.family === 'siege' ? p.windup : u.sidearm ? .35 : p.windup * (heavy ? 1.25 : 1), recovery: u.sidearm ? .65 : p.recovery, duration: 0, heavy, targetId: target?.id || null, targetX: targetPoint.x, targetZ: targetPoint.z, targetY: Number.isFinite(targetPoint.y) ? targetPoint.y : terrainHeight(b, targetPoint.x, targetPoint.z), ranged, impacted: false, facing: u.facing };
  u.action.duration = u.action.windup + u.action.recovery;
  return true;
}
function kill(b, u, attacker) {
  if (!alive(u)) return;
  u.hp = 0; u.status = 'dead'; u.action = null; u.speed = 0; u.blocking = false;
  event(b, 'casualty', `${UNITS[u.type].name} fell.`, { unitId: u.id, armyId: u.armyId, x: u.x, z: u.z });
  if (attacker) attacker.morale = clamp(attacker.morale + 2, 0, 100);
  for (const v of b.units) if (alive(v) && v.side === u.side && distance(v, u) < 12) v.morale = Math.max(0, v.morale - (u.commander ? 18 : 2.5));
  if (u.commander) event(b, 'commander', 'Your commander is incapacitated. You can continue commanding the surviving army.', { unitId: u.id });
}
function hit(b, attacker, target, raw, options = {}) {
  if (!alive(target)) return;
  let damage = raw;
  const incoming = facing(target, attacker), frontal = angle(target.facing, incoming) < 1.08;
  const charge = attacker.mounted && attacker.speed > 4.5 && attacker.chargeDistance > 8 && !options.projectile;
  if (charge && target.braced && frontal && target.weapon === 'spear') {
    attacker.hp -= 25 + (UNITS[target.type].antiCavalry || 1) * 14;
    attacker.speed = 0; attacker.chargeDistance = 0; attacker.stagger = 1.5; attacker.morale = Math.max(0, attacker.morale - 12);
    event(b, 'brace', 'A braced spear stops the mounted charge.', { x: target.x, z: target.z });
    if (attacker.hp <= 0) kill(b, attacker, target);
    damage *= .3;
  } else if (charge) {
    damage += (UNITS[attacker.type].charge || 1) * 6;
    target.stagger = .8; target.morale = Math.max(0, target.morale - (frontal ? 5 : 13)); attacker.chargeDistance = 0; attacker.speed *= .45;
    event(b, 'charge', 'A mounted charge strikes home.', { x: target.x, z: target.z });
  }
  if (target.blocking && frontal && target.stamina >= 7 && (target.shield || !options.projectile)) {
    const axe = attacker.weapon === 'axe' || options.heavy;
    target.stamina = Math.max(0, target.stamina - (axe ? 25 : 10));
    damage *= axe ? .48 : .14;
    target.blockedAt = b.time;
    if (b.time - target.blockStarted < .3) { attacker.stagger = .5; target.counterUntil = b.time + 1.2; }
    event(b, 'block', 'An attack is blocked.', { x: target.x, z: target.z, unitId: target.id });
  }
  const armor = UNITS[target.type].armor || (target.family === 'siege' ? .1 : 0);
  damage *= 1 - armor * (options.piercing || attacker.weapon === 'mace' ? .35 : 1);
  if (options.projectile && target.wallId && target.y > attacker.y + 2 && attacker.family !== 'siege' && b.layout.walls.some(w => w.id === target.wallId && w.hp > 0)) damage *= .6;
  if (attacker.counterUntil > b.time) damage *= 1.25;
  if (!frontal && !options.projectile) target.morale = Math.max(0, target.morale - 2.5);
  if (['climb', 'stairs'].includes(target.action?.kind)) damage *= 1.4;
  target.hp -= Math.max(1, damage);
  event(b, 'hit', 'A weapon hits.', { x: target.x, z: target.z, damage: Math.round(damage), unitId: target.id });
  if (target.hp <= 0) kill(b, target, attacker);
}
function updateFortifications(b) {
  const f = b.fortifications, remaining = [...b.layout.walls, ...(b.layout.gate ? [b.layout.gate] : [])].reduce((sum, w) => sum + Math.max(0, w.hp), 0);
  const ratio = f.initialTacticalHp ? clamp(remaining / f.initialTacticalHp, 0, 1) : 0;
  f.walls = Math.min(f.walls, f.initialWalls * ratio); f.fortIntegrity = Math.min(f.fortIntegrity, f.initialFortIntegrity * ratio);
}
function hitWall(b, u, w, damage) {
  if (!w || w.hp <= 0) return;
  w.hp = Math.max(0, w.hp - damage);
  const before = w.state;
  w.state = w.hp === 0 ? 'breached' : w.hp < w.maxHp * .7 ? 'damaged' : 'intact';
  if (w.state === 'breached' && before !== 'breached') { b.layout.navRevision++; event(b, 'breach', `${w.id === 'gate' ? 'The gate has fallen' : 'A wall is breached'}!`, { x: w.x, z: w.z, wallId: w.id }); }
  updateFortifications(b);
}
function actionImpact(b, u, action) {
  const p = profileFor(u), target = b.units.find(v => v.id === action.targetId), wall = [...b.layout.walls, ...(b.layout.gate ? [b.layout.gate] : [])].find(w => w.id === action.targetId);
  if (action.ranged) {
    if (u.ammo <= 0) return;
    u.ammo--;
    const stone = u.family === 'siege', spread = stone ? 1.8 : u.type === 'veteranArcher' ? .3 : .75;
    const tx = action.targetX + (random(b) - .5) * spread, tz = action.targetZ + (random(b) - .5) * spread;
    b.projectiles.push({ id: `missile-${b.nextProjectile++}`, sourceId: u.id, side: u.side, kind: stone ? 'stone' : u.weapon === 'crossbow' ? 'bolt' : 'arrow', x: u.x, z: u.z, y: u.y + 1.6, startX: u.x, startZ: u.z, startY: u.y + 1.6, targetX: tx, targetZ: tz, targetY: wall ? terrainHeight(b, tx, tz) + 3 : action.targetY + 1, elapsed: 0, duration: Math.max(.15, distance(u, { x: tx, z: tz }) / (stone ? 20 : 45)), damage: p.damage, wallId: wall?.id || null, splash: stone ? 4.8 : 0 });
    return;
  }
  if (wall && distance(u, wall) < p.reach + Math.max(wall.width, wall.depth) / 2) { hitWall(b, u, wall, u.type === 'ram' ? 38 : 3); return; }
  const reach = u.sidearm ? 1.9 : p.reach, maxTargets = action.heavy ? 3 : 1;
  const targets = b.units.filter(v => alive(v) && v.side !== u.side && Math.abs(v.y - u.y) < 2.7 && distance(u, v) < reach + .6 && angle(action.facing, facing(u, v)) < (action.heavy ? 1.1 : .65) && lineClear(b, u, v, u)).sort((a, c) => distance(u, a) - distance(u, c));
  for (let i = 0; i < Math.min(maxTargets, targets.length); i++) hit(b, u, targets[i], (u.sidearm ? 14 : p.damage) * (action.heavy ? 1.5 : 1) * Math.pow(.65, i), { heavy: action.heavy });
}
function updateAction(b, u, dt) {
  const a = u.action;
  if (!a) return;
  a.elapsed += dt;
  if (a.kind === 'stairs') {
    const wall = b.layout.walls.find(w => w.id === a.wallId);
    if (!wall || wall.hp <= 0) { u.wallId = null; u.action = null; u.y = terrainHeight(b, u.x, u.z); u.hp -= 18; if (u.hp <= 0) kill(b, u, null); return; }
    const t = clamp(a.elapsed / a.duration, 0, 1), targetY = terrainHeight(b, a.targetX, a.targetZ) + (a.ascending ? wall.height : 0);
    u.x = a.fromX + (a.targetX - a.fromX) * t; u.z = a.fromZ + (a.targetZ - a.fromZ) * t; u.y = a.fromY + (targetY - a.fromY) * t;
    if (t >= 1) { u.wallId = a.ascending ? a.wallId : null; u.action = null; u.path = []; u.pathAt = -10; }
    return;
  }
  if (a.kind === 'climb') {
    const ladder = b.layout.ladders.find(l => l.id === a.ladderId);
    if (ladder) ladder.progress = clamp(a.elapsed / a.duration, 0, 1);
    u.y = terrainHeight(b, u.x, u.z) + (a.wallId ? clamp(a.elapsed / a.duration, 0, 1) : Math.sin(Math.PI * clamp(a.elapsed / a.duration, 0, 1))) * 6;
    if (a.elapsed >= a.duration) { u.x = a.targetX; u.z = a.targetZ; u.wallId = a.wallId || null; u.y = terrainHeight(b, u.x, u.z) + (u.wallId ? 6 : 0); if (ladder) ladder.climberId = null; u.action = null; u.path = []; u.pathAt = -10; }
    return;
  }
  if (!a.impacted && a.elapsed >= a.windup) { a.impacted = true; actionImpact(b, u, a); }
  if (a.elapsed >= a.duration) u.action = null;
}
function updateProjectiles(b, dt) {
  for (const p of b.projectiles) {
    const ox = p.x, oz = p.z;
    p.elapsed += dt; const t = clamp(p.elapsed / p.duration, 0, 1);
    p.x = p.startX + (p.targetX - p.startX) * t; p.z = p.startZ + (p.targetZ - p.startZ) * t;
    p.y = p.startY + (p.targetY - p.startY) * t + Math.sin(t * Math.PI) * (p.kind === 'stone' ? 16 : .22);
    const source = b.units.find(u => u.id === p.sourceId);
    if (!source) { p.done = true; continue; }
    if (p.kind !== 'stone') {
      const obstacle = obstruction(b, p.x, p.z, source);
      if (obstacle && obstacle.kind !== 'water' && p.y < (obstacle.height || 6) + terrainHeight(b, p.x, p.z)) { p.done = true; event(b, 'missileBlocked', 'A missile strikes cover.', { x: p.x, z: p.z }); continue; }
      for (const u of b.units) {
        if (!alive(u) || u.id === source.id || Math.abs(p.y - (u.y + 1.2)) > 1.3) continue;
        const vx = p.x - ox, vz = p.z - oz, lengthSq = vx * vx + vz * vz;
        const along = lengthSq ? clamp(((u.x - ox) * vx + (u.z - oz) * vz) / lengthSq, 0, 1) : 0;
        if (Math.hypot(u.x - ox - along * vx, u.z - oz - along * vz) < .85) {
          if (u.side !== source.side) hit(b, source, u, p.damage, { projectile: true, piercing: p.kind === 'bolt' });
          else event(b, 'missileBlocked', 'An allied formation blocks the firing lane.', { x: p.x, z: p.z });
          p.done = true; break;
        }
      }
    }
    if (t >= 1) {
      if (p.kind === 'stone') {
        for (const u of b.units) if (alive(u) && distance(p, u) < p.splash) hit(b, source, u, p.damage * (1 - distance(p, u) / (p.splash + 1)), { projectile: true, piercing: true });
        const wall = [...b.layout.walls, ...(b.layout.gate ? [b.layout.gate] : [])].find(w => w.id === p.wallId);
        if (wall) hitWall(b, source, wall, (UNITS[source.type].breach || 8) * 3);
        event(b, 'siegeImpact', 'A siege projectile impacts.', { x: p.x, z: p.z });
      }
      p.done = true;
    }
  }
  b.projectiles = b.projectiles.filter(p => !p.done);
}

function planArmies(b) {
  const strengths = [0, 0]; for (const u of b.units) if (u.status === 'active' || u.status === 'reserve') strengths[u.side] += (UNITS[u.type].attack + UNITS[u.type].defense) * (u.hp / u.maxHp);
  for (const a of b.armies) {
    const troops = b.units.filter(u => u.armyId === a.armyId && fighting(u));
    if (!troops.length) continue;
    a.morale = troops.reduce((sum, u) => sum + u.morale, 0) / troops.length;
    const commander = a.general || {}, aggression = Number(commander.aggression ?? .5), defensive = strengths[a.side] < strengths[1 - a.side] * .7 || a.side === 1 && b.layout.fortified;
    for (const u of troops) {
      if (u.owner === b.playerOwner && !b.auto || u.explicitOrder || u.allyRequestUntil > b.time) continue;
      if (a.morale < 18 && strengths[a.side] < strengths[1 - a.side] * .6) { u.routing = true; u.order = { kind: 'retreat' }; continue; }
      if (u.family === 'mounted') u.formation = 'flanking';
      if (u.type === 'spearman' && defensive) u.formation = 'spearWall';
      const hold = defensive && aggression < .8 && b.time < 90;
      u.order = { kind: hold ? 'hold' : 'advance', x: u.x, z: u.z, facing: u.facing };
    }
  }
}
function targetWall(b, u) {
  if (u.side !== 0 || !b.layout.fortified) return null;
  if (u.family === 'siege') {
    if (u.type === 'ram') return b.layout.gate?.hp > 0 ? b.layout.gate : null;
    return b.layout.walls.filter(w => w.hp > 0).sort((a, c) => distance(u, a) - distance(u, c))[0] || (b.layout.gate?.hp > 0 ? b.layout.gate : null);
  }
  return null;
}
function aiUnit(b, u, active, dt) {
  const a = b.armies.find(a => a.armyId === u.armyId), p = profileFor(u);
  if (['stairs', 'climb'].includes(u.action?.kind)) return;
  if (u.routing || u.order.kind === 'retreat') {
    u.routing = true; u.blocking = false; u.braced = false;
    const e = a.entry;
    moveUnit(b, u, e, dt, { sprint: u.family !== 'siege' });
    if (distance(u, e) < 5 || Math.abs(u.x) > 76 || Math.abs(u.z) > 76) { u.status = 'escaped'; u.action = null; event(b, 'escape', 'Routing troops reach an escape route.', { armyId: u.armyId }); }
    return;
  }
  const enemy = nearestEnemy(b, u, active), d = enemy ? distance(u, enemy) : Infinity;
  if (u.order.kind === 'walls' && takeWallPosition(b, u, dt)) return;
  u.targetId = enemy?.id || null;
  const oldBlock = u.blocking;
  u.blocking = !!enemy && d < 6 && u.shield && u.stamina > 14 && !u.action && (u.formation === 'defensive' || u.formation === 'spearWall' || enemy.action?.elapsed < enemy.action?.windup);
  if (u.blocking && !oldBlock) u.blockStarted = b.time;
  u.braced = u.type === 'spearman' && u.speed < .7 && !u.action && ['spearWall', 'defensive'].includes(u.formation) && u.stamina > 15;
  const wall = targetWall(b, u);
  if (wall && (u.type === 'ram' || u.ammo > 0)) {
    const approach = { x: wall.x, z: wall.z - (u.type === 'ram' ? 3.5 : p.reach * .75) };
    if (distance(u, wall) <= p.reach + 2) { u.facing = facing(u, wall); startAttack(b, u, wall); u.speed *= .8; }
    else moveUnit(b, u, approach, dt);
    return;
  }
  if (u.order.kind === 'ladder' && b.layout.ladders.length) {
    const ladder = b.layout.ladders.slice().sort((a, c) => distance(u, a) - distance(u, c))[0];
    moveUnit(b, u, { x: ladder.x, z: ladder.z + 6 }, dt); return;
  }
  if (u.order.kind === 'move' || u.order.kind === 'withdraw') {
    moveUnit(b, u, u.order, dt, { keepFacing: u.order.kind === 'withdraw' });
    if (u.order.kind === 'withdraw' && enemy) u.facing = facing(u, enemy);
    if (distance(u, u.order) < 2) { u.order.kind = 'hold'; u.facing = u.order.facing; }
    if (d > 2.2) return;
  }
  if (u.order.kind === 'follow') {
    const commander = b.units.find(v => v.id === b.commanderId);
    if (commander && alive(commander)) {
      const dest = { x: commander.x + (u.order.offsetX || 0), z: commander.z + (u.order.offsetZ || -3) };
      if (distance(u, dest) > 4) moveUnit(b, u, dest, dt);
      if (d > 3) return;
    }
  }
  if (!enemy) {
    if (b.securing && u.side === 0 && u.family !== 'siege') moveUnit(b, u, b.layout.objective, dt);
    else u.speed *= .8;
    return;
  }
  const ranged = ['bow', 'crossbow'].includes(u.weapon) && u.ammo > 0 || u.family === 'siege' && u.type !== 'ram' && u.ammo > 0;
  u.sidearm = u.family === 'ranged' && (u.ammo <= 0 || d < 3) || u.family === 'siege' && u.type !== 'ram' && u.ammo <= 0;
  if (ranged && !u.sidearm) {
    if (d < 10 && !['hold', 'walls'].includes(u.order.kind)) { moveUnit(b, u, { x: u.x + (u.x - enemy.x), z: u.z + (u.z - enemy.z) }, dt, { keepFacing: true }); u.facing = facing(u, enemy); }
    else if (d < p.reach && !u.holdFire && lineClear(b, u, enemy, u, u.family === 'ranged')) {
      u.facing = facing(u, enemy);
      const fired = startAttack(b, u, enemy);
      if (!fired && !u.action && u.stamina > 10 && !u.wallId) {
        const direction = hash(u.id) % 2 ? 1 : -1;
        moveUnit(b, u, { x: u.x + Math.cos(u.facing) * direction * 3, z: u.z - Math.sin(u.facing) * direction * 3 }, dt, { keepFacing: true });
      } else u.speed *= .75;
    }
    else if (!['hold', 'walls', 'follow'].includes(u.order.kind)) moveUnit(b, u, enemy, dt);
    return;
  }
  const reach = u.sidearm ? 1.9 : p.reach;
  if (d < reach + .3 && Math.abs(u.y - enemy.y) < 2.7 && lineClear(b, u, enemy, u)) {
    u.facing = facing(u, enemy); startAttack(b, u, enemy, u.type === 'heavyInfantry' && random(b) < .2); u.speed *= .8;
    return;
  }
  if (u.order.kind === 'walls') { u.speed = 0; return; }
  if (u.order.kind === 'hold' && distance(enemy, u.order) > 8) { moveUnit(b, u, u.order, dt); u.facing = u.order.facing ?? u.facing; return; }
  if (enemy.wallId && enemy.wallId !== u.wallId && u.family !== 'siege') {
    const wall = b.layout.walls.find(w => w.id === enemy.wallId), stair = b.layout.stairs?.find(s => s.wallId === enemy.wallId);
    const ladder = b.layout.ladders.find(l => wall && inside(wall, l.x, l.z, .5));
    if (ladder && u.z < 12 && u.family === 'infantry') moveUnit(b, u, { x: ladder.x, z: ladder.z, wallId: wall.id }, dt);
    else if (stair) { if (!u.wallId && distance(u, stair) < 1) startStairs(b, u, stair, true); else moveUnit(b, u, stair, dt); }
    return;
  }
  let destination = enemy;
  if (u.family === 'mounted' && u.formation === 'flanking' && d > 13 && enemy.braced) {
    const flank = Math.sin(enemy.facing + Math.PI / 2) * 14;
    destination = { x: enemy.x + flank, z: enemy.z + Math.cos(enemy.facing + Math.PI / 2) * 14 };
    if (distance(u, destination) < 5) destination = enemy;
  }
  if (u.order.kind === 'advance' && d > 10 && u.family === 'infantry') {
    const peers = active.filter(v => v.armyId === u.armyId && v.family === 'infantry' && fighting(v));
    const rank = peers.indexOf(u), width = Math.ceil(Math.sqrt(peers.length) * 1.6);
    const centerX = peers.reduce((sum, v) => sum + v.x, 0) / Math.max(1, peers.length);
    destination = { x: centerX * .4 + enemy.x * .6 + ((rank % width) - width / 2) * 1.6, z: enemy.z + (u.side ? 1 : -1) * Math.floor(rank / width) * 1.7 };
  }
  moveUnit(b, u, destination, dt, { sprint: u.order.kind === 'charge' && d > 5 });
}
function playerUnit(b, u, input, dt) {
  if (['stairs', 'climb'].includes(u.action?.kind)) return;
  if (input.weapon && !u.action && WEAPONS[input.weapon] && u.family !== 'siege') {
    const canEquip = input.weapon !== 'lance' || u.family === 'mounted';
    const hasQuiver = !['bow', 'crossbow'].includes(input.weapon) || u.family === 'ranged';
    if (canEquip && hasQuiver && u.weapon !== input.weapon) { u.weapon = input.weapon; u.sidearm = false; }
  }
  const oldBlock = u.blocking;
  u.blocking = !!input.block && u.stamina > 3 && !u.action;
  if (u.blocking && !oldBlock) u.blockStarted = b.time;
  if (u.blocking) u.stamina = Math.max(0, u.stamina - dt * 5);
  if (Number.isFinite(input.facing)) u.facing = input.facing;
  if (input.sidearm !== undefined) u.sidearm = !!input.sidearm;
  const mx = clamp(Number(input.moveX ?? input.move?.x) || 0, -1, 1), mz = clamp(Number(input.moveZ ?? input.move?.z) || 0, -1, 1), norm = Math.hypot(mx, mz);
  if (norm > .05) moveUnit(b, u, { x: u.x + mx / Math.max(1, norm) * 5, z: u.z + mz / Math.max(1, norm) * 5 }, dt, { direct: true, sprint: !!input.sprint, keepFacing: true });
  else if (u.order.kind === 'walls') takeWallPosition(b, u, dt);
  else u.speed = Math.max(0, u.speed - dt * 8);
  u.braced = u.weapon === 'spear' && u.blocking && u.speed < .5;
  if (input.attack) {
    const enemy = nearestEnemy(b, u, b.units.filter(v => angle(u.facing, facing(u, v)) < 1));
    const reach = profileFor(u).reach;
    const point = { x: Number.isFinite(input.aimX) ? input.aimX : u.x + Math.sin(u.facing) * reach, z: Number.isFinite(input.aimZ) ? input.aimZ : u.z + Math.cos(u.facing) * reach };
    const aimDistance = distance(u, point);
    if (aimDistance > reach) { point.x = u.x + (point.x - u.x) / aimDistance * reach; point.z = u.z + (point.z - u.z) / aimDistance * reach; }
    if (enemy && distance(u, enemy) <= reach && angle(u.facing, facing(u, enemy)) < .12 && ['bow', 'crossbow'].includes(u.weapon)) Object.assign(point, { x: enemy.x, z: enemy.z, y: enemy.y });
    startAttack(b, u, enemy && distance(u, enemy) <= reach ? enemy : null, !!input.heavy, point);
  }
  if (input.interact && !u.interactHeld) {
    const ladder = b.layout.ladders.find(l => distance(u, l) < 5);
    const stair = b.layout.stairs?.find(s => u.wallId === s.wallId ? Math.hypot(u.x - s.topX, u.z - s.topZ) < 3 : distance(u, s) < 3);
    if (stair) startStairs(b, u, stair, !u.wallId);
    else if (ladder) moveUnit(b, u, { x: ladder.x, z: ladder.z + (u.z < ladder.z ? 5 : -5) }, dt);
    else if (u.family === 'mounted' && !u.action) { u.mounted = !u.mounted; u.speed = 0; u.chargeDistance = 0; event(b, 'mount', u.mounted ? 'You mount your horse.' : 'You dismount. Your mount stays with you.'); }
  }
  u.interactHeld = !!input.interact;
}
function updateMorale(b) {
  for (const a of b.armies) {
    const units = b.units.filter(u => u.armyId === a.armyId), losses = units.filter(u => u.status === 'dead').length / Math.max(1, units.length), routing = units.filter(u => alive(u) && u.routing).length;
    for (const u of units.filter(alive)) {
      const discipline = UNITS[u.type].discipline || 1;
      const pressure = Math.max(0, losses - .15) * 2.5 / discipline + routing / Math.max(1, units.length) * 3;
      u.morale = clamp(u.morale - pressure + (losses < .1 ? .1 : 0), 0, 100);
      if (u.morale < 14 / discipline && !u.commander && !u.routing) { u.routing = true; event(b, 'route', `${a.name || a.owner} troops are routing.`, { armyId: a.armyId, x: u.x, z: u.z }); }
    }
  }
}
function separateUnits(b, active, dt) {
  for (let i = 0; i < active.length; i++) for (let j = i + 1; j < active.length; j++) {
    const u = active[i], v = active[j];
    if (u.wallId || v.wallId || ['climb', 'stairs'].includes(u.action?.kind) || ['climb', 'stairs'].includes(v.action?.kind) || Math.abs(u.y - v.y) > 2.7) continue;
    const dx = v.x - u.x, dz = v.z - u.z, d = Math.hypot(dx, dz), radius = u.family === 'siege' || v.family === 'siege' ? 2.2 : u.family === 'mounted' || v.family === 'mounted' ? 1.7 : 1.15;
    if (d >= radius) continue;
    if (u.family === 'mounted' && u.speed > 4 && u.side === v.side) { u.speed *= .7; u.chargeDistance = 0; }
    const n = d || 1, push = Math.min((radius - d) * .5, dt * 2), x = (dx || .1) / n * push, z = dz / n * push;
    if (!obstruction(b, u.x - x, u.z - z, u)) { u.x -= x; u.z -= z; }
    if (!obstruction(b, v.x + x, v.z + z, v)) { v.x += x; v.z += z; }
  }
}
function checkVictory(b) {
  if (b.phase !== 'combat') return;
  const resistance = [0, 0];
  for (const u of b.units) if (fighting(u) || u.status === 'reserve' && !u.routing && !b.armies.find(a => a.armyId === u.armyId)?.retreating) resistance[u.side]++;
  if (resistance[0] && resistance[1]) return;
  const winner = resistance[0] ? 0 : resistance[1] ? 1 : null;
  if (winner === 0 && b.layout.fortified && !b.units.some(u => fighting(u) && u.side === 0 && u.family !== 'siege' && distance(u, b.layout.objective) < 14)) {
    b.securing = true;
    // Siege-only survivors cannot occupy; end their assault without capture.
    if (b.units.some(u => fighting(u) && u.side === 0 && u.family !== 'siege')) return;
  }
  // A field defeat leaves unarrived supporters and routed survivors intact.
  // All committed reserves count as resistance until they arrive or withdraw.
  b.winner = winner; b.phase = 'complete'; b.completedAt = b.time;
  for (const u of b.units) if (alive(u) && u.routing) { u.status = 'escaped'; u.action = null; }
  event(b, 'complete', winner === null ? 'Both armies have withdrawn.' : `${winner === b.playerSide ? 'Your side' : 'The opposing side'} wins the battle.`);
}
function tick(b, input) {
  const dt = STEP;
  b.time += dt;
  if (b.time >= b.nextReserves) { deployReserves(b); b.nextReserves = b.time + 1; }
  if (b.time >= b.nextArmyPlan) { planArmies(b); b.nextArmyPlan = b.time + 4; }
  if (b.time >= b.nextMorale) { updateMorale(b); b.nextMorale = b.time + 1; }
  const active = b.units.filter(alive);
  for (const u of active) {
    if (u.wallId && !b.layout.walls.some(w => w.id === u.wallId && w.hp > 0)) {
      u.wallId = null; u.y = terrainHeight(b, u.x, u.z); u.hp -= 18; u.stagger = .8; u.action = null;
      if (u.hp <= 0) { kill(b, u, null); continue; }
    }
    u.stagger = Math.max(0, u.stagger - dt); u.attackCooldown = Math.max(0, u.attackCooldown - dt);
    if (!u.action && !u.blocking) u.stamina = Math.min(100, u.stamina + dt * 11);
    updateAction(b, u, dt);
    if (!alive(u)) continue;
    if (u.id === b.commanderId && !b.auto && !input.commanderAI && !u.routing) playerUnit(b, u, input, dt);
    else aiUnit(b, u, active, dt);
  }
  separateUnits(b, active.filter(alive), dt); updateProjectiles(b, dt); checkVictory(b);
}
export function stepBattle(b, dt, input = {}) {
  if (b.phase !== 'combat' || input.paused || !Number.isFinite(dt) || dt <= 0) return b;
  b.accumulator += Math.min(dt, 2);
  b.pendingInput ||= {};
  for (const key of ['attack', 'interact', 'heavy']) if (input[key]) b.pendingInput[key] = true;
  while (b.accumulator + 1e-9 >= STEP && b.phase === 'combat') {
    b.accumulator -= STEP;
    tick(b, { ...input, attack: input.attack || b.pendingInput.attack, interact: input.interact || b.pendingInput.interact, heavy: input.heavy || b.pendingInput.heavy });
    b.pendingInput = {};
  }
  return b;
}

export function battleResult(b) {
  const armies = b.armies.map(a => {
    const units = b.units.filter(u => u.armyId === a.armyId), survivors = countTypes(units.filter(u => u.status !== 'dead')), losses = {}, escaped = countTypes(units.filter(u => u.status === 'escaped' || u.routing && alive(u))), undeployed = countTypes(units.filter(u => u.status === 'reserve'));
    for (const [type, n] of Object.entries(a.starting)) { survivors[type] ||= 0; losses[type] = n - survivors[type]; }
    const living = units.filter(u => u.status !== 'dead');
    return { armyId: a.armyId, owner: a.owner, side: a.side, starting: { ...a.starting }, units: survivors, survivors, losses, escaped, undeployed, morale: living.length ? Math.max(.1, living.reduce((sum, u) => sum + u.morale, 0) / living.length / 100) : .1, commanderKnockedOut: units.some(u => u.commander && u.status === 'dead'), destroyedEngines: countTypes(units.filter(u => u.family === 'siege' && u.status === 'dead')), sourceTile: a.sourceTile, arrived: a.arrived, retreat: b.winner !== a.side };
  });
  const occupying = b.units.filter(u => fighting(u) && u.side === b.winner && u.family !== 'siege' && (!b.layout.fortified || b.winner === 1 || distance(u, b.layout.objective) < 14));
  const sections = [...b.layout.walls, ...(b.layout.gate ? [b.layout.gate] : [])];
  return { battleId: b.encounterId, encounterId: b.encounterId, complete: b.phase === 'complete', winner: b.winner, armies, walls: Math.max(0, b.fortifications.walls), fortIntegrity: Math.max(0, b.fortifications.fortIntegrity), fortifications: clone(b.fortifications), wallBreaches: sections.filter(w => w.hp <= 0).map(w => w.id), wallSections: sections.map(w => ({ id: w.id, hp: w.hp, maxHp: w.maxHp })), occupationSide: occupying.length ? b.winner : null, duration: b.time, commanderId: b.commanderId };
}

export function finishAutomatically(b) {
  if (b.phase === 'complete') return battleResult(b);
  beginBattle(b); b.auto = true;
  for (const u of b.units) { u.explicitOrder = false; if (u.order.kind !== 'retreat') u.order = { kind: 'advance' }; }
  // Execute the same simulation from this checkpoint. Existing casualties,
  // missiles, depleted quivers, fortification damage and delayed troops remain.
  const stop = b.time + 240;
  while (b.phase === 'combat' && b.time < stop) stepBattle(b, 2, { commanderAI: true });
  if (b.phase === 'combat') {
    // Bounded strategic continuation for large reserve armies and stalemates.
    // Arrival and casualty accounting still operate on the original identities.
    let rounds = 0;
    while (b.phase === 'combat' && rounds++ < 2400) {
      b.time += 5; deployReserves(b);
      const forces = [0, 0], living = [[], []];
      for (const u of b.units) if (fighting(u)) {
        const ammunition = u.family === 'ranged' ? u.ammo > 0 ? 1 : .45 : u.family === 'siege' ? u.type === 'ram' || u.ammo > 0 ? .8 : .2 : 1;
        const health = .35 + .65 * u.hp / u.maxHp, morale = .3 + .7 * u.morale / 100;
        forces[u.side] += (UNITS[u.type].attack + UNITS[u.type].defense) * ammunition * health * morale;
        living[u.side].push(u);
      }
      if (!living[0].length || !living[1].length) {
        if (living[0].length && !living[1].length && b.layout.fortified && !b.units.some(u => u.side === 1 && u.status === 'reserve' && !u.routing)) {
          // With resistance gone, the surviving infantry can secure an empty
          // courtyard without inventing a casualty or replacing the roster.
          const occupier = living[0].find(u => u.family !== 'siege');
          if (occupier) { occupier.x = b.layout.objective.x; occupier.z = b.layout.objective.z; occupier.y = terrainHeight(b, occupier.x, occupier.z); }
        }
        checkVictory(b); continue;
      }
      for (const side of [0, 1]) {
        const cover = side === 1 ? 1 + Math.min(.35, (b.fortifications.walls + b.fortifications.fortIntegrity) / 400) : 1;
        const damage = forces[1 - side] * .9 / cover;
        let budget = damage;
        const targets = living[side].slice().sort((a, c) => a.hp - c.hp);
        for (const u of targets) {
          if (budget <= 0) break;
          const hitPoints = Math.min(u.hp, budget); u.hp -= hitPoints; budget -= hitPoints; u.morale = Math.max(0, u.morale - 2);
          if (u.hp <= 0) kill(b, u, null);
        }
        for (const u of living[side]) {
          if (u.ammo > 0) u.ammo--;
          if (u.family === 'siege' && side === 0) { const w = targetWall(b, u); if (w && (u.ammo > 0 || u.type === 'ram')) hitWall(b, u, w, UNITS[u.type].breach || 5); }
        }
      }
      updateMorale(b); checkVictory(b);
    }
    if (b.phase === 'combat') {
      // A force unable to close after prolonged fighting disengages. No living
      // or not-yet-arrived troop is silently converted into a casualty.
      const scores = [0, 0]; for (const u of b.units) if (u.status !== 'dead' && u.status !== 'escaped') scores[u.side] += u.hp * (u.morale + 20);
      const losing = scores[0] >= scores[1] ? 1 : 0;
      for (const a of b.armies) if (a.side === losing) a.retreating = true;
      for (const u of b.units) if (u.side === losing && alive(u)) { u.routing = true; u.status = 'escaped'; }
      checkVictory(b);
    }
  }
  return battleResult(b);
}

export function validateBattleCheckpoint(b, encounter) {
  if (!b || b.version !== BATTLE_VERSION || b.encounterId !== encounter?.id || !['deployment', 'combat', 'complete'].includes(b.phase) || !Array.isArray(b.units) || !Array.isArray(b.armies)) return false;
  if (!Number.isFinite(b.time) || b.time < 0 || !Number.isFinite(b.rng) || !Number.isFinite(b.accumulator) || !b.layout || !Array.isArray(b.projectiles) || !b.fortifications || !Array.isArray(b.events)) return false;
  if (b.seed !== (encounter.seed ?? encounter.id) || b.playerOwner !== encounter.playerOwner || !Number.isInteger(b.maxActive) || b.maxActive < 4 || b.maxActive > 240 || !Array.isArray(b.activeCaps)) return false;
  const seen = new Set(), expected = new Map();
  for (const p of encounter.participants || []) {
    if (expected.has(p.armyId)) return false;
    expected.set(p.armyId, p);
    const recorded = b.armies.find(a => a.armyId === p.armyId);
    if (!recorded || recorded.owner !== p.owner || recorded.side !== p.side || !recorded.starting || !Number.isFinite(recorded.arrivalSeconds) || recorded.arrivalSeconds < 0 || !recorded.entry || ![recorded.entry.x, recorded.entry.z].every(Number.isFinite)) return false;
    for (const [type, amount] of Object.entries(p.units || {})) if (amount !== (recorded.starting[type] || 0)) return false;
  }
  if (b.armies.length !== expected.size) return false;
  const totals = new Map();
  for (const u of b.units) {
    const p = expected.get(u.armyId);
    if (!p || seen.has(u.id) || !PROFILE[u.type] || p.owner !== u.owner || p.side !== u.side || !['active', 'reserve', 'dead', 'escaped'].includes(u.status)) return false;
    seen.add(u.id);
    const prefix = `${u.armyId}:${u.type}:`, index = Number(String(u.id).slice(prefix.length));
    if (!String(u.id).startsWith(prefix) || !Number.isInteger(index) || index < 0 || index >= (p.units?.[u.type] || 0)) return false;
    if (![u.x, u.z, u.y, u.hp, u.maxHp, u.morale, u.stamina, u.ammo, u.facing].every(Number.isFinite) || u.hp < 0 || u.hp > u.maxHp || u.maxHp !== PROFILE[u.type].hp || u.ammo < 0 || u.ammo > (PROFILE[u.type].ammo || 0) || u.morale < 0 || u.morale > 100 || u.stamina < 0 || u.stamina > 100) return false;
    if (u.status === 'dead' && u.hp !== 0 || u.status !== 'dead' && u.hp <= 0) return false;
    const key = `${u.armyId}:${u.type}`; totals.set(key, (totals.get(key) || 0) + 1);
    if (!u.order || !Array.isArray(u.path) || u.action && ![u.action.elapsed, u.action.duration, u.action.windup].every(Number.isFinite)) return false;
    if (u.wallId && !b.layout.walls?.some(w => w.id === u.wallId)) return false;
    if (u.wallPosition && (![u.wallPosition.x, u.wallPosition.z, u.wallPosition.facing].every(Number.isFinite) || !b.layout.walls?.some(w => w.id === u.wallPosition.wallId))) return false;
    if (u.action?.kind === 'stairs' && (![u.action.fromX, u.action.fromZ, u.action.fromY, u.action.targetX, u.action.targetZ].every(Number.isFinite) || !b.layout.stairs?.some(s => s.id === u.action.stairId && s.wallId === u.action.wallId))) return false;
  }
  for (const p of encounter.participants) for (const type of Object.keys(UNITS)) if ((p.units?.[type] || 0) !== (totals.get(`${p.armyId}:${type}`) || 0)) return false;
  if (b.commanderId && !b.units.some(u => u.id === b.commanderId && u.owner === encounter.playerOwner && u.commander)) return false;
  if (!Array.isArray(b.layout.walls) || !Array.isArray(b.layout.obstacles) || !Array.isArray(b.layout.ladders) || ![0, 1, null].includes(b.winner)) return false;
  const tile = encounter.tile || {}, fortMax = tile.building === 'fort' ? [0, 45, 100, 180][clamp(tile.levels?.fort || tile.buildings?.fort || tile.level || tile.buildingLevel || 1, 1, 3)] : 0;
  const f = b.fortifications;
  if (![f.walls, f.initialWalls, f.fortIntegrity, f.initialFortIntegrity].every(Number.isFinite) || f.initialWalls !== Math.max(0, Number(tile.walls) || 0) || f.initialFortIntegrity !== Math.max(0, Number(tile.fortIntegrity ?? fortMax) || 0)) return false;
  if (f.walls < 0 || f.walls > f.initialWalls || f.fortIntegrity < 0 || f.fortIntegrity > f.initialFortIntegrity) return false;
  for (const w of [...b.layout.walls, ...(b.layout.gate ? [b.layout.gate] : [])]) if (![w.x, w.z, w.width, w.depth, w.hp, w.maxHp].every(Number.isFinite) || w.hp < 0 || w.hp > w.maxHp) return false;
  if (b.layout.stairs !== undefined && !Array.isArray(b.layout.stairs)) return false;
  for (const s of b.layout.stairs || []) if (![s.x, s.z, s.topX, s.topZ, s.height].every(Number.isFinite) || !b.layout.walls.some(w => w.id === s.wallId)) return false;
  for (const p of b.projectiles) if (![p.x, p.z, p.y, p.startX, p.startZ, p.startY, p.targetX, p.targetZ, p.targetY, p.elapsed, p.duration, p.damage].every(Number.isFinite) || p.duration <= 0 || p.damage < 0) return false;
  return true;
}
